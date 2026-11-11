//! Owned native packet-controller lifecycle. This initial constructor creates
//! a fresh isolated namespace, optionally on its own inspected uplink.
//! Isolation/credits are NOT VPN health,
//! a public-worker capability, installation permission or billing eligibility.
use super::{
    accounting::CommittedTransferReservation, lease::ControllerLease,
    observation_time::ObservationTime, packet_client::PacketClient, runtime::image_is_pinned,
};
use crate::runtime::{
    RuntimeManager,
    model::{
        ContainerHandle, ContainerLogOptions, ContainerSecurityOptions, ContainerSpec,
        ELIXIR_DEPLOYMENT_ID_LABEL, ELIXIR_EXTENSION_ID_LABEL, ELIXIR_INSTANCE_ID_LABEL,
        ELIXIR_MANAGED_LABEL, EnvVar, PrivateActorInspection, apply_container_spec_fingerprint,
    },
    packet_network::{PacketNetworkInspection, PacketNetworkSpec, immutable_id},
};
use anyhow::{Context, Result, bail};
use elixir_marketplace::{
    canonical,
    guard_control::{GuardCommand, MAX_HOST_RESOURCES_BYTES},
    identity::Digest,
    network_probe::{NetworkObservation, WireguardIdentity, WireguardSnapshot, check_snapshot},
    packet_guard::PacketPolicy,
    packet_usage::PacketUsage,
    policy::OrdinaryLimits,
    wireguard_setup::{WireguardSecrets, WireguardSetup},
};
use std::{
    collections::HashMap,
    path::Path,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::time::timeout;

pub(super) fn packet_image_protocol_supported(labels: &HashMap<String, String>) -> bool {
    [
        "schema",
        "kernel_report",
        "firewall_integrity",
        "transport_burst",
    ]
    .into_iter()
    .all(|feature| {
        labels
            .get(&format!("elixir.marketplace.guard.{feature}"))
            .map(String::as_str)
            == Some("1")
    })
}
#[cfg(unix)]
mod provider;
#[cfg(unix)]
pub(in crate::marketplace) use provider::ProviderConnectivity;

const MIB: u64 = 1024 * 1024;
pub const GUARD_MEMORY_BYTES: u64 = 32 * MIB;
// Reserve control-plane headroom without increasing the combined allowance:
// PacketWorkloadBudget subtracts this same share from the ordinary worker.
pub const GUARD_CPU_MILLICORES: u32 = 20;
pub const GUARD_PIDS: u64 = 16;
const ENTRYPOINT: &str = "/opt/elixir-marketplace/bin/elixir-marketplace-netguard";
const PATH_VALUE: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";
const EPOCH_LABEL: &str = "elixir.marketplace.packet.epoch";
const POLICY_LABEL: &str = "elixir.marketplace.packet.policy";
const UPLINK_LABEL: &str = "elixir.marketplace.packet.uplink";

#[derive(Debug)]
struct PacketUplink {
    id: String,
    spec: PacketNetworkSpec,
}

/// Fixed split, not an increase in ordinary-server resource requirements.
#[derive(Debug, Clone)]
pub struct PacketWorkloadBudget {
    worker: OrdinaryLimits,
}
impl PacketWorkloadBudget {
    pub fn split(limits: &OrdinaryLimits) -> Result<Self> {
        limits.validate()?;
        let mut worker = limits.clone();
        worker.memory_bytes = worker
            .memory_bytes
            .checked_sub(GUARD_MEMORY_BYTES)
            .context("insufficient packet actor memory budget")?;
        worker.cpu_millicores = worker
            .cpu_millicores
            .checked_sub(GUARD_CPU_MILLICORES)
            .context("insufficient packet actor CPU budget")?;
        if worker.memory_bytes < 64 * MIB
            || !worker.memory_bytes.is_multiple_of(MIB)
            || worker.cpu_millicores < 10
        {
            bail!("configured limits cannot fit the guarded worker profile");
        }
        worker.validate()?;
        Ok(Self { worker })
    }
    pub fn worker_limits(&self) -> &OrdinaryLimits {
        &self.worker
    }
}

/// One-use startup state, not deserializable/Clone or a protection capability.
/// Every new controller must construct a fresh profile and epoch.
#[derive(Debug)]
pub struct PacketGuardProfile {
    spec: ContainerSpec,
    policy: PacketPolicy,
    budget: PacketWorkloadBudget,
    uplink: Option<PacketUplink>,
    provider: Option<String>,
}
impl PacketGuardProfile {
    pub fn isolated(
        deployment: &str,
        image: &str,
        policy: PacketPolicy,
        limits: &OrdinaryLimits,
    ) -> Result<Self> {
        Self::compile(
            deployment,
            image,
            policy,
            limits,
            Digest::of(uuid::Uuid::new_v4().as_bytes()),
        )
    }
    fn compile(
        deployment: &str,
        image: &str,
        mut policy: PacketPolicy,
        limits: &OrdinaryLimits,
        epoch: Digest,
    ) -> Result<Self> {
        if deployment.is_empty()
            || deployment.len() > 64
            || !deployment
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            || !image_is_pinned(image)
        {
            bail!("packet actor requires a stable deployment and immutable image");
        }
        let budget = PacketWorkloadBudget::split(limits)?;
        if policy.upload_bytes_per_second > limits.upload_bytes_per_second
            || policy.download_bytes_per_second > limits.download_bytes_per_second
        {
            bail!("packet policy exceeds the configured network budget");
        }
        // Caller/restart cannot recycle an established namespace's epoch.
        policy.epoch = epoch;
        policy.validate()?;
        let mut spec = ContainerSpec {
            name: format!("elixir-marketplace-guard-{deployment}"),
            image: image.to_owned(),
            network: String::new(),
            network_mode: Some("none".to_owned()),
            aliases: vec![],
            env: vec![EnvVar {
                name: "PATH".to_owned(),
                value: PATH_VALUE.to_owned(),
            }],
            volumes: vec![],
            ports: vec![],
            labels: HashMap::from([
                (ELIXIR_MANAGED_LABEL.to_owned(), "true".to_owned()),
                (ELIXIR_DEPLOYMENT_ID_LABEL.to_owned(), deployment.to_owned()),
                (
                    ELIXIR_EXTENSION_ID_LABEL.to_owned(),
                    "elixir-marketplace-guard".to_owned(),
                ),
                (
                    ELIXIR_INSTANCE_ID_LABEL.to_owned(),
                    format!("marketplace-guard-{deployment}"),
                ),
                (
                    "elixir.marketplace.isolation".to_owned(),
                    "packet-v1".to_owned(),
                ),
                (EPOCH_LABEL.to_owned(), policy.epoch.to_string()),
                (
                    POLICY_LABEL.to_owned(),
                    Digest::of(canonical::to_vec(&policy)?).to_string(),
                ),
            ]),
            command: vec!["--epoch".to_owned(), policy.epoch.to_string()],
            cap_add: vec!["NET_ADMIN".to_owned()],
            cap_drop: vec!["ALL".to_owned()],
            devices: vec![],
            sysctls: HashMap::new(),
            security: ContainerSecurityOptions {
                user: Some("0:0".to_owned()),
                read_only_rootfs: true,
                no_new_privileges: true,
                memory_limit_mb: Some(32),
                memory_swap_limit_mb: Some(32),
                pids_limit: Some(GUARD_PIDS),
                cpu_quota: Some(format!("0.{GUARD_CPU_MILLICORES:03}")),
                shared_memory_mb: Some(1),
                disable_core_dumps: true,
                open_files_limit: Some(32),
                file_size_limit_bytes: Some(0),
                stdin_open: true,
                log_options: Some(ContainerLogOptions::Disabled),
                ..Default::default()
            },
        };
        apply_container_spec_fingerprint(&mut spec);
        Ok(Self {
            spec,
            policy,
            budget,
            uplink: None,
            provider: None,
        })
    }
    #[cfg(unix)]
    pub(in crate::marketplace) fn provider(
        deployment: &str,
        image: &str,
        policy: PacketPolicy,
        limits: &OrdinaryLimits,
        parent: &str,
        namespace: u64,
    ) -> Result<Self> {
        if !immutable_id(parent)
            || !(1..(1u64 << 53)).contains(&namespace)
            || policy.tunnel_interface != elixir_marketplace::packet_guard::PROVIDER_TUN_INTERFACE
        {
            bail!("provider guard requires its exact parent and provider-TUN policy");
        }
        let epoch = policy.epoch;
        let mut profile = Self::compile(deployment, image, policy, limits, epoch)?;
        profile.spec.name = format!(
            "elixir-marketplace-provider-guard-{}",
            &epoch.to_string()[7..]
        );
        profile
            .spec
            .labels
            .insert(ELIXIR_INSTANCE_ID_LABEL.into(), profile.spec.name.clone());
        profile.spec.network_mode = Some(format!("container:{parent}"));
        profile
            .spec
            .labels
            .insert("elixir.marketplace.packet.provider".into(), parent.into());
        profile.spec.labels.insert(
            "elixir.marketplace.packet.provider_namespace".into(),
            namespace.to_string(),
        );
        profile.provider = Some(parent.into());
        apply_container_spec_fingerprint(&mut profile.spec);
        Ok(profile)
    }
    fn bind_uplink(
        &mut self,
        spec: PacketNetworkSpec,
        actual: &PacketNetworkInspection,
    ) -> Result<()> {
        spec.verify(actual)?;
        if self.uplink.is_some() {
            bail!("packet profile already has an uplink");
        }
        self.spec.network_mode = Some(actual.id.clone());
        self.spec
            .labels
            .insert(UPLINK_LABEL.to_owned(), actual.id.clone());
        self.uplink = Some(PacketUplink {
            id: actual.id.clone(),
            spec,
        });
        apply_container_spec_fingerprint(&mut self.spec);
        Ok(())
    }
    async fn inspect_uplink(
        &self,
        runtime: &dyn RuntimeManager,
        actor: Option<(&ContainerHandle, &PrivateActorInspection)>,
    ) -> Result<()> {
        let Some(uplink) = &self.uplink else {
            return Ok(());
        };
        let actual = timeout(
            Duration::from_secs(10),
            runtime.inspect_packet_uplink(&uplink.id),
        )
        .await
        .context("uplink inspection deadline")??
        .context("owned packet uplink missing")?;
        uplink.spec.verify(&actual)?;
        if actual.id != uplink.id || actual.members.len() > 1 {
            bail!("packet uplink identity or membership differs");
        }
        match actor {
            None => {
                if !actual.members.is_empty() {
                    bail!("packet uplink has a foreign attachment");
                }
            }
            Some((handle, state)) => {
                for (id, member) in &actual.members {
                    if id != &handle.id
                        || member.name != handle.name
                        || state.endpoint_ids.get(uplink.spec.name()) != Some(&member.endpoint_id)
                    {
                        bail!("packet uplink endpoint is not the owned actor");
                    }
                }
                if state.running && !actual.members.contains_key(&handle.id) {
                    bail!("running packet actor is detached from its uplink");
                }
            }
        }
        Ok(())
    }
    pub fn container_spec(&self) -> &ContainerSpec {
        &self.spec
    }
    pub fn policy(&self) -> &PacketPolicy {
        &self.policy
    }
    pub fn budget(&self) -> &PacketWorkloadBudget {
        &self.budget
    }

    pub(crate) fn verify(
        &self,
        handle: &ContainerHandle,
        actual: &PrivateActorInspection,
        stale: bool,
    ) -> Result<()> {
        let state = &actual.runtime;
        let security = &state.security;
        let mut expected = self.spec.clone();
        if stale {
            let epoch: Digest = state
                .labels
                .get(EPOCH_LABEL)
                .context("missing old packet epoch")?
                .parse()?;
            let policy: Digest = state
                .labels
                .get(POLICY_LABEL)
                .context("missing old packet policy digest")?
                .parse()?;
            expected
                .labels
                .insert(EPOCH_LABEL.to_owned(), epoch.to_string());
            expected
                .labels
                .insert(POLICY_LABEL.to_owned(), policy.to_string());
            expected.command = vec!["--epoch".to_owned(), epoch.to_string()];
            apply_container_spec_fingerprint(&mut expected);
        }
        let cap = |value: &str| {
            value
                .strip_prefix("CAP_")
                .unwrap_or(value)
                .to_ascii_uppercase()
        };
        let network_name = self
            .uplink
            .as_ref()
            .map(|uplink| uplink.spec.name())
            .unwrap_or("none");
        if let Some(uplink) = &self.uplink {
            let id = actual
                .network_ids
                .get(network_name)
                .context("missing packet actor network ID")?;
            if ((actual.running || !id.is_empty()) && id != &uplink.id)
                || (actual.running
                    && actual
                        .endpoint_ids
                        .get(network_name)
                        .is_none_or(|id| !immutable_id(id)))
            {
                bail!("packet actor uplink binding differs");
            }
        }
        if actual.id != handle.id
            || handle.id.len() != 64
            || !handle
                .id
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || handle.name != expected.name
            || state.name != expected.name
            || state.network_mode != expected.network_mode
            || expected
                .labels
                .iter()
                .any(|(key, value)| state.labels.get(key) != Some(value))
            || !state.mounts.is_empty()
            || !state.published_ports.is_empty()
            || security.image_reference.as_deref() != Some(expected.image.as_str())
            || !security.image_id.as_deref().is_some_and(image_is_pinned)
            || (expected.image.starts_with("sha256:")
                && security.image_id.as_deref() != Some(expected.image.as_str()))
            || security.user.as_deref() != Some("0:0")
            || !security.read_only_rootfs
            || !security.no_new_privileges
            || security.privileged != Some(false)
            || security.cap_add.len() != 1
            || security
                .cap_add
                .iter()
                .any(|value| cap(value) != "NET_ADMIN")
            || security.cap_drop.len() != 1
            || security.cap_drop.iter().any(|value| cap(value) != "ALL")
            || !security.tmpfs.is_empty()
            || security.memory_limit_bytes != Some(GUARD_MEMORY_BYTES as i64)
            || security.memory_swap_limit_bytes != Some(GUARD_MEMORY_BYTES as i64)
            || security.nano_cpus != Some(i64::from(GUARD_CPU_MILLICORES) * 1_000_000)
            || security.pids_limit != Some(GUARD_PIDS as i64)
            || security.shared_memory_bytes != Some(MIB as i64)
            || !security
                .core_dump_limit
                .as_ref()
                .is_some_and(|value| value.soft == 0 && value.hard == 0)
            || !security
                .open_files_limit
                .as_ref()
                .is_some_and(|value| value.soft == 32 && value.hard == 32)
            || !security
                .file_size_limit
                .as_ref()
                .is_some_and(|value| value.soft == 0 && value.hard == 0)
            || !security
                .log_config
                .as_ref()
                .is_some_and(|value| value.driver == "none" && value.options.is_empty())
            || security.stdin_open != Some(true)
            || security.tty != Some(false)
            || security.seccomp_profile.is_some()
            || security.apparmor_profile.is_some()
            || actual.command != expected.command
            || actual.entrypoint != [ENTRYPOINT.to_owned()]
            || actual.environment != [format!("PATH={PATH_VALUE}")]
            || !actual.pid_mode.is_empty()
            || actual.ipc_mode != "private"
            || !actual.userns_mode.is_empty()
            || actual.device_count != 0
            || actual.device_request_count != 0
            || actual.bind_count != 0
            || !actual.sysctls.is_empty()
            || if self.provider.is_some() {
                !actual.networks.is_empty()
                    || !actual.network_ids.is_empty()
                    || !actual.endpoint_ids.is_empty()
            } else {
                actual.networks != [network_name.to_owned()]
            }
            || actual.restart_policy != "no"
        {
            bail!("packet actor identity or isolation differs from its frozen profile");
        }
        Ok(())
    }
}

pub struct PacketGuardController {
    handle: ContainerHandle,
    profile: PacketGuardProfile,
    client: PacketClient,
    _lease: Arc<ControllerLease>,
    #[cfg(unix)]
    provider: Option<super::provider::Owner>,
    #[cfg(unix)]
    provider_connectivity_nonce: Option<Digest>,
    network_probe_available: bool,
    provider_tun_probe_available: bool,
    wireguard_setup_available: bool,
    credit_usage_available: bool,
    network_observation_sequence: Option<u64>,
}

/// A fresh observation obtained from the owned actor and checked against VPN
/// identity anchors. Not DNS/exit/firewall proof or public-worker permission.
/// Deliberately not Clone/Deserialize: later health stages must preserve its
/// live container/epoch/namespace binding and monotonic age.
pub struct KernelTunnelObservation {
    container_id: String,
    epoch: Digest,
    namespace_inode: u64,
    sequence: u64,
    started: ObservationTime,
    snapshot: WireguardSnapshot,
}
impl KernelTunnelObservation {
    pub fn snapshot(&self) -> &WireguardSnapshot {
        &self.snapshot
    }
    /// Context/age only. This does not re-inspect the OS or prove VPN readiness.
    pub fn is_recent_for(&self, controller: &PacketGuardController) -> bool {
        self.container_id == controller.handle.id
            && self.epoch == controller.profile.policy.epoch
            && self.namespace_inode == controller.namespace_inode()
            && controller.network_observation_sequence == Some(self.sequence)
            && self.started.is_current(Duration::from_secs(3))
            && !controller.client.is_poisoned()
    }
}
impl PacketGuardController {
    /// Dedicated underlay only, not protected-worker authority. Network state
    /// is reused only after complete ownership/configuration inspection.
    pub async fn start_with_uplink(
        runtime: &dyn RuntimeManager,
        directory: &Path,
        deployment: &str,
        image: &str,
        policy: PacketPolicy,
        limits: &OrdinaryLimits,
    ) -> Result<Self> {
        Self::start_with_uplink_owned(runtime, directory, deployment, image, policy, limits, None)
            .await
    }
    pub(in crate::marketplace) async fn start_with_uplink_leased(
        runtime: &dyn RuntimeManager,
        directory: &Path,
        deployment: &str,
        image: &str,
        policy: PacketPolicy,
        limits: &OrdinaryLimits,
        lease: ControllerLease,
    ) -> Result<Self> {
        Self::start_with_uplink_owned(
            runtime,
            directory,
            deployment,
            image,
            policy,
            limits,
            Some(lease),
        )
        .await
    }
    async fn start_with_uplink_owned(
        runtime: &dyn RuntimeManager,
        directory: &Path,
        deployment: &str,
        image: &str,
        policy: PacketPolicy,
        limits: &OrdinaryLimits,
        lease: Option<ControllerLease>,
    ) -> Result<Self> {
        let ipv6 = policy
            .endpoints
            .iter()
            .any(|endpoint| endpoint.address.is_ipv6());
        let profile = PacketGuardProfile::isolated(deployment, image, policy, limits)?;
        let network = PacketNetworkSpec::internet(deployment, ipv6)?;
        let lease = match lease {
            Some(lease) if lease.matches_directory(directory) => lease,
            Some(_) => bail!("packet controller lease does not match startup directory"),
            None => return Self::start_networked(runtime, directory, profile, network).await,
        };
        Self::start_networked_owned(runtime, profile, network, lease).await
    }
    async fn start_networked(
        runtime: &dyn RuntimeManager,
        directory: &Path,
        profile: PacketGuardProfile,
        network: PacketNetworkSpec,
    ) -> Result<Self> {
        let lease = ControllerLease::acquire(directory)?;
        Self::start_networked_owned(runtime, profile, network, lease).await
    }
    async fn start_networked_owned(
        runtime: &dyn RuntimeManager,
        mut profile: PacketGuardProfile,
        network: PacketNetworkSpec,
        lease: ControllerLease,
    ) -> Result<Self> {
        timeout(Duration::from_secs(90), async {
            let actual = runtime.ensure_packet_uplink(&network).await?;
            profile.bind_uplink(network, &actual)?;
            Self::start_owned(runtime, lease, profile).await
        })
        .await
        .context("networked packet actor startup deadline")?
    }
    /// Bootstrap only. Fresh network-none namespace, no sockets or public worker
    /// enabled; successful initialization is NOT protected-connectivity proof.
    pub async fn start_isolated(
        runtime: &dyn RuntimeManager,
        directory: &Path,
        profile: PacketGuardProfile,
    ) -> Result<Self> {
        let lease = ControllerLease::acquire(directory)?;
        timeout(
            Duration::from_secs(90),
            Self::start_owned(runtime, lease, profile),
        )
        .await
        .context("packet actor startup timed out")?
    }
    async fn start_owned(
        runtime: &dyn RuntimeManager,
        lease: ControllerLease,
        profile: PacketGuardProfile,
    ) -> Result<Self> {
        let labels = runtime.image_labels(&profile.spec.image).await?;
        let network_probe_available = labels
            .get("elixir.marketplace.guard.network_probe")
            .map(String::as_str)
            == Some("1");
        let wireguard_setup_available = labels
            .get("elixir.marketplace.guard.wireguard_setup")
            .map(String::as_str)
            == Some("1");
        let provider_tun_probe_available = labels
            .get("elixir.marketplace.guard.provider_tun_probe")
            .map(String::as_str)
            == Some("1");
        let credit_usage_available = labels
            .get("elixir.marketplace.guard.credit_usage")
            .map(String::as_str)
            == Some("1");
        if !packet_image_protocol_supported(&labels) {
            bail!("packet actor image protocol is unsupported");
        }
        if let Some(old) = runtime.get_container_handle(&profile.spec.name).await? {
            let actual = runtime.inspect_private_actor(&old).await?;
            profile.verify(&old, &actual, true)?;
            profile
                .inspect_uplink(runtime, Some((&old, &actual)))
                .await?;
            runtime.stop_container(&old).await?;
            let stopped = runtime.inspect_private_actor(&old).await?;
            profile.verify(&old, &stopped, true)?;
            if stopped.running {
                bail!("old packet actor is still running");
            }
            // Never reset an existing table. Removing the owned holder permits
            // a fresh namespace; its old packet credits are never reissued.
            runtime.remove_container(&old).await?;
        }
        profile.inspect_uplink(runtime, None).await?;
        let handle = runtime.create_isolated_packet_actor(&profile.spec).await?;
        let ready = async {
            let state = runtime.inspect_private_actor(&handle).await?;
            profile.verify(&handle, &state, false)?;
            if !state.running {
                bail!("packet actor did not start");
            }
            profile
                .inspect_uplink(runtime, Some((&handle, &state)))
                .await?;
            let stream = runtime
                .attach_private_stdio(&handle, &profile.spec.labels)
                .await?;
            let mut client = PacketClient::new(
                stream,
                profile.policy.epoch,
                GUARD_MEMORY_BYTES,
                GUARD_CPU_MILLICORES,
                GUARD_PIDS,
            )?;
            client
                .request(GuardCommand::Initialize {
                    policy: profile.policy.clone(),
                })
                .await?;
            Ok(client)
        }
        .await;
        let client = match ready {
            Ok(client) => client,
            Err(error) => {
                // Only a positively identified current instance may be stopped.
                if let Ok(actual) = runtime.inspect_private_actor(&handle).await
                    && actual.id == handle.id
                    && actual.runtime.name == profile.spec.name
                    && profile
                        .spec
                        .labels
                        .iter()
                        .all(|(key, value)| actual.runtime.labels.get(key) == Some(value))
                {
                    let _ = timeout(Duration::from_secs(20), runtime.stop_container(&handle)).await;
                }
                return Err(error);
            }
        };
        Ok(Self {
            handle,
            profile,
            client,
            _lease: Arc::new(lease),
            #[cfg(unix)]
            provider: None,
            #[cfg(unix)]
            provider_connectivity_nonce: None,
            network_probe_available,
            provider_tun_probe_available,
            wireguard_setup_available,
            credit_usage_available,
            network_observation_sequence: None,
        })
    }
    pub fn handle(&self) -> &ContainerHandle {
        &self.handle
    }
    pub fn profile(&self) -> &PacketGuardProfile {
        &self.profile
    }
    pub fn namespace_inode(&self) -> u64 {
        self.client.namespace_inode().unwrap()
    }
    pub(super) fn channel_usable(&self) -> bool {
        !self.client.is_poisoned()
    }
    pub(super) async fn inspect_owned(&self, runtime: &dyn RuntimeManager) -> Result<()> {
        #[cfg(unix)]
        if let Some(provider) = &self.provider {
            provider.inspect().await?;
        }
        let actual = timeout(
            Duration::from_secs(10),
            runtime.inspect_private_actor(&self.handle),
        )
        .await
        .context("packet actor inspection timed out")??;
        self.profile.verify(&self.handle, &actual, false)?;
        if !actual.running {
            bail!("packet actor stopped");
        }
        self.profile
            .inspect_uplink(runtime, Some((&self.handle, &actual)))
            .await?;
        Ok(())
    }
    /// Consume the real server debit exactly once. Never accept worker-supplied
    /// PacketCredit JSON, refund unknown results, or replay into another epoch.
    pub async fn apply_credit(
        &mut self,
        runtime: &dyn RuntimeManager,
        debit: CommittedTransferReservation,
    ) -> Result<()> {
        self.network_observation_sequence = None;
        let credit = debit.into_packet_credit()?;
        self.inspect_owned(runtime).await?;
        self.client.request(GuardCommand::Credit { credit }).await?;
        Ok(())
    }
    /// Fixed startup-only sample from this original inspected private actor.
    /// Native session enforces one use before any credit. No packet grant or
    /// generic process/command can be obtained from the returned measurements.
    pub(in crate::marketplace) async fn observe_host_resources(
        &mut self,
        runtime: &dyn RuntimeManager,
    ) -> Result<String> {
        self.network_observation_sequence = None;
        self.inspect_owned(runtime).await?;
        if runtime
            .image_labels(&self.profile.spec.image)
            .await?
            .get("elixir.marketplace.guard.host_resources")
            .map(String::as_str)
            != Some("2")
        {
            bail!("guard image lacks fixed private host-resource observation");
        }
        let report = self
            .client
            .request(GuardCommand::HostResources)
            .await?
            .host_resources
            .context("guard omitted native host-resource observation")?;
        if report.is_empty() || report.len() > MAX_HOST_RESOURCES_BYTES || report.contains('\0') {
            bail!("guard host-resource observation bounds");
        }
        self.inspect_owned(runtime).await?;
        Ok(report)
    }
    /// Read the current credit's actual outer-packet charge from the owned
    /// actor. This is not an application byte count, refund, lease renewal or
    /// permission to reuse a previous tunnel observation.
    pub(super) async fn observe_credit(
        &mut self,
        runtime: &dyn RuntimeManager,
    ) -> Result<PacketUsage> {
        self.network_observation_sequence = None;
        let result = async {
            self.inspect_owned(runtime).await?;
            self.require_credit_observation()?;
            let started = Instant::now();
            let mut usage = self
                .client
                .request(GuardCommand::CreditStatus)
                .await?
                .usage
                .context("packet actor omitted usage observation")?;
            // Charge the complete round trip against the reported lifetime;
            // local processing delay can never extend native credit expiry.
            usage.valid_for_millis = usage
                .valid_for_millis
                .saturating_sub(u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX));
            Ok(usage)
        }
        .await;
        if result.is_err() {
            let _ = self.client.request(GuardCommand::Pause).await;
        }
        result
    }
    pub(super) fn require_credit_observation(&self) -> Result<()> {
        if !self.credit_usage_available {
            bail!("packet actor lacks native credit observation");
        }
        Ok(())
    }
    pub async fn pause(&mut self, runtime: &dyn RuntimeManager) -> Result<()> {
        self.network_observation_sequence = None;
        if let Err(error) = self.inspect_owned(runtime).await {
            let _ = self.client.request(GuardCommand::Pause).await;
            return Err(error);
        }
        self.client.request(GuardCommand::Pause).await?;
        Ok(())
    }
    /// One-time private kernel setup before any packet debit. A separately
    /// authorized marketplace enrollment supplies its own device identity.
    /// This consumes secrets, not a config path, and grants no lease/health.
    pub async fn configure_wireguard(
        &mut self,
        runtime: &dyn RuntimeManager,
        setup: WireguardSetup,
        secrets: WireguardSecrets,
    ) -> Result<()> {
        self.network_observation_sequence = None;
        self.inspect_owned(runtime).await?;
        if !self.wireguard_setup_available {
            bail!("packet actor lacks native WireGuard setup");
        }
        setup.validate(&self.profile.policy)?;
        secrets.check_device(&setup)?;
        self.client.configure_wireguard(setup, &secrets).await?;
        Ok(())
    }
    /// Read-only kernel stage only. A failed/missing/stale observation also
    /// stops the lease through the original epoch-bound private channel.
    /// Success does NOT renew a lease or start an online worker.
    pub async fn observe_wireguard(
        &mut self,
        runtime: &dyn RuntimeManager,
        identity: &WireguardIdentity,
    ) -> Result<KernelTunnelObservation> {
        // Invalidate before the first await, including cancellation and failed
        // reinspection. An older successful sample cannot mask a newer failure.
        self.network_observation_sequence = None;
        let result = async {
            self.inspect_owned(runtime).await?;
            if !self.network_probe_available {
                bail!("packet actor lacks kernel network observation support");
            }
            let started = ObservationTime::now();
            let reply = self.client.request(GuardCommand::Observe).await?;
            let snapshot = match reply
                .network
                .context("missing kernel network observation")?
            {
                NetworkObservation::Observed { snapshot } => *snapshot,
                NetworkObservation::ProviderTunObserved { .. } => {
                    bail!("userspace TUN cannot establish kernel WireGuard identity")
                }
                NetworkObservation::Unavailable { reason } => {
                    bail!("kernel tunnel observation unavailable: {reason:?}")
                }
            };
            if !started.is_current(Duration::from_secs(3)) {
                bail!("kernel tunnel observation is stale");
            }
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("invalid kernel observation clock")?
                .as_secs();
            check_snapshot(&self.profile.policy, identity, &snapshot, now)?;
            self.network_observation_sequence = Some(reply.sequence);
            Ok(KernelTunnelObservation {
                container_id: self.handle.id.clone(),
                epoch: self.profile.policy.epoch,
                namespace_inode: self.namespace_inode(),
                sequence: reply.sequence,
                started,
                snapshot,
            })
        }
        .await;
        if result.is_err() {
            let _ = self.client.request(GuardCommand::Pause).await;
        }
        result
    }
    /// Read-only metadata from the exact owned actor. Deliberately returns
    /// DATA, not KernelTunnelObservation: no WireGuard identity/handshake is
    /// fabricated and this cannot satisfy renew_observed_lease. A future owned
    /// provider lifecycle + independent DNS/TLS stage must compose full health.
    pub(super) async fn observe_provider_tun(
        &mut self,
        runtime: &dyn RuntimeManager,
    ) -> Result<elixir_marketplace::network_probe::provider_tun::ProviderTunSnapshot> {
        use elixir_marketplace::{
            network_probe::provider_tun, packet_guard::PROVIDER_TUN_INTERFACE,
        };
        self.network_observation_sequence = None;
        let result = async {
            self.inspect_owned(runtime).await?;
            if !self.provider_tun_probe_available
                || self.profile.policy.tunnel_interface != PROVIDER_TUN_INTERFACE
            {
                bail!("packet actor lacks the selected provider TUN observation contract");
            }
            let started = Instant::now();
            let reply = self.client.request(GuardCommand::Observe).await?;
            let snapshot = match reply.network.context("missing provider TUN observation")? {
                NetworkObservation::ProviderTunObserved { snapshot } => *snapshot,
                _ => bail!("packet actor did not observe the selected provider TUN"),
            };
            if started.elapsed() > Duration::from_secs(3) {
                bail!("provider TUN observation is stale");
            }
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("invalid provider TUN observation clock")?
                .as_secs();
            provider_tun::check_snapshot(&self.profile.policy, &snapshot, now)?;
            Ok(snapshot)
        }
        .await;
        if result.is_err() {
            let _ = self.client.request(GuardCommand::Pause).await;
        }
        result
    }
    /// Trusted bootstrap only, before any public worker joins. This has no
    /// public protection token. VPN/interface/route/health evidence and ongoing
    /// supervision must gate the later online worker constructor.
    pub async fn renew_bootstrap_lease(&mut self, runtime: &dyn RuntimeManager) -> Result<()> {
        if let Err(error) = self.inspect_owned(runtime).await {
            self.network_observation_sequence = None;
            let _ = self.client.request(GuardCommand::Pause).await;
            return Err(error);
        }
        self.client.request(GuardCommand::Heartbeat).await?;
        Ok(())
    }
    /// Supervisor-only renewal after a live kernel observation. The caller
    /// must also hold recent independent DNS/TLS evidence. An old, paused,
    /// credited or replacement-namespace observation cannot authorize renewal.
    pub(super) async fn renew_observed_lease(
        &mut self,
        runtime: &dyn RuntimeManager,
        observation: &KernelTunnelObservation,
    ) -> Result<()> {
        let result = async {
            if !observation.is_recent_for(self) {
                bail!("packet lease requires a current kernel tunnel observation");
            }
            self.inspect_owned(runtime).await?;
            // Inspection itself may stall; do not turn a previously fresh
            // sample into permission to renew after either clock's deadline.
            if !observation.is_recent_for(self) {
                bail!("kernel tunnel observation expired before lease renewal");
            }
            self.client.request(GuardCommand::Heartbeat).await?;
            Ok(())
        }
        .await;
        if result.is_err() {
            self.network_observation_sequence = None;
            let _ = self.client.request(GuardCommand::Pause).await;
        }
        result
    }
    pub async fn stop(mut self, runtime: &dyn RuntimeManager) -> Result<()> {
        let _ = self.client.request(GuardCommand::Pause).await;
        #[cfg(unix)]
        if let Some(provider) = self.provider.take() {
            self.client.close_joined().await?;
            return provider.close(false).await;
        }
        let actual = timeout(
            Duration::from_secs(10),
            runtime.inspect_private_actor(&self.handle),
        )
        .await
        .context("packet actor stop inspection timed out")??;
        self.profile.verify(&self.handle, &actual, false)?;
        timeout(
            Duration::from_secs(20),
            runtime.stop_container(&self.handle),
        )
        .await
        .context("packet actor stop timed out")??;
        if timeout(Duration::from_secs(10), runtime.inspect(&self.handle))
            .await
            .context("packet actor stop confirmation deadline")??
            .running
        {
            bail!("packet actor did not stop");
        }
        self.client.close().await;
        Ok(())
    }
}

#[cfg(unix)]
impl PacketGuardController {
    /// Only the original provider boot task can hand over this owned parent.
    /// This initializes a closed firewall; it grants no credit or live lease.
    pub(in crate::marketplace) fn from_provider(
        handle: ContainerHandle,
        profile: PacketGuardProfile,
        client: PacketClient,
        lease: Arc<ControllerLease>,
        provider: super::provider::Owner,
    ) -> Result<Self> {
        if profile.provider.as_deref() != Some(provider.handle().id.as_str())
            || client.namespace_inode() != Some(provider.guard_namespace()?)
        {
            bail!("provider guard namespace differs from its original owner");
        }
        provider.require_guard_binding(
            &lease,
            &handle,
            &profile,
            client
                .namespace_inode()
                .context("guard namespace missing")?,
        )?;
        Ok(Self {
            handle,
            profile,
            client,
            _lease: lease,
            provider: Some(provider),
            provider_connectivity_nonce: None,
            network_probe_available: true,
            provider_tun_probe_available: true,
            wireguard_setup_available: false,
            credit_usage_available: true,
            network_observation_sequence: None,
        })
    }
}

#[cfg(test)]
pub(in crate::marketplace) mod tests {
    use super::*;
    #[test]
    fn packet_image_requires_finite_transport_burst_before_any_native_effect() {
        let good: HashMap<_, _> = [
            "schema",
            "kernel_report",
            "firewall_integrity",
            "transport_burst",
        ]
        .into_iter()
        .map(|name| (format!("elixir.marketplace.guard.{name}"), "1".to_owned()))
        .collect();
        assert!(packet_image_protocol_supported(&good));
        for feature in [
            "schema",
            "kernel_report",
            "firewall_integrity",
            "transport_burst",
        ] {
            let key = format!("elixir.marketplace.guard.{feature}");
            let mut missing = good.clone();
            missing.remove(&key);
            assert!(!packet_image_protocol_supported(&missing));
            for value in ["0", "2", ""] {
                let mut changed = good.clone();
                changed.insert(key.clone(), value.into());
                assert!(!packet_image_protocol_supported(&changed));
            }
        }
    }
    use crate::{
        config::DatabaseConfig,
        db::Database,
        runtime::{
            docker::DockerRuntimeManager,
            model::{
                ContainerRuntimeLogConfig, ContainerRuntimeSecurityState, ContainerRuntimeState,
                ContainerRuntimeUlimit,
            },
        },
    };
    use elixir_marketplace::packet_guard::VpnEndpoint;
    use std::collections::BTreeMap;

    fn policy() -> PacketPolicy {
        PacketPolicy {
            schema_version: 1,
            epoch: Digest::of(b"untrusted-reused-epoch"),
            tunnel_interface: "wg0".to_owned(),
            tunnel_addresses: vec!["10.66.0.2".parse().unwrap()],
            endpoints: vec![VpnEndpoint {
                address: "8.8.8.8".parse().unwrap(),
                port: 51820,
                local_port: 51821,
            }],
            lease_seconds: 15,
            upload_bytes_per_second: 32768,
            download_bytes_per_second: 131072,
        }
    }
    fn profile() -> PacketGuardProfile {
        PacketGuardProfile::isolated(
            "unit",
            &format!("sha256:{}", "1".repeat(64)),
            policy(),
            &OrdinaryLimits::default(),
        )
        .unwrap()
    }
    pub(in crate::marketplace) fn inspection(
        profile: &PacketGuardProfile,
    ) -> (ContainerHandle, PrivateActorInspection) {
        let spec = &profile.spec;
        let id = "2".repeat(64);
        (
            ContainerHandle {
                id: id.clone(),
                name: spec.name.clone(),
            },
            PrivateActorInspection {
                id,
                running: true,
                runtime: ContainerRuntimeState {
                    name: spec.name.clone(),
                    network_mode: Some("none".to_owned()),
                    labels: spec.labels.clone(),
                    mounts: vec![],
                    published_ports: vec![],
                    security: ContainerRuntimeSecurityState {
                        image_reference: Some(spec.image.clone()),
                        image_id: Some(spec.image.clone()),
                        user: Some("0:0".to_owned()),
                        read_only_rootfs: true,
                        no_new_privileges: true,
                        cap_drop: vec!["ALL".to_owned()],
                        cap_add: vec!["NET_ADMIN".to_owned()],
                        memory_limit_bytes: Some(GUARD_MEMORY_BYTES as i64),
                        memory_swap_limit_bytes: Some(GUARD_MEMORY_BYTES as i64),
                        nano_cpus: Some(i64::from(GUARD_CPU_MILLICORES) * 1_000_000),
                        pids_limit: Some(16),
                        shared_memory_bytes: Some(MIB as i64),
                        privileged: Some(false),
                        stdin_open: Some(true),
                        tty: Some(false),
                        log_config: Some(ContainerRuntimeLogConfig {
                            driver: "none".to_owned(),
                            options: BTreeMap::new(),
                        }),
                        core_dump_limit: Some(ContainerRuntimeUlimit { soft: 0, hard: 0 }),
                        open_files_limit: Some(ContainerRuntimeUlimit { soft: 32, hard: 32 }),
                        file_size_limit: Some(ContainerRuntimeUlimit { soft: 0, hard: 0 }),
                        ..Default::default()
                    },
                },
                command: spec.command.clone(),
                entrypoint: vec![ENTRYPOINT.to_owned()],
                environment: vec![format!("PATH={PATH_VALUE}")],
                pid_mode: String::new(),
                ipc_mode: "private".to_owned(),
                userns_mode: String::new(),
                device_count: 0,
                device_request_count: 0,
                bind_count: 0,
                sysctls: BTreeMap::new(),
                networks: vec!["none".to_owned()],
                network_ids: BTreeMap::new(),
                endpoint_ids: BTreeMap::new(),
                restart_policy: "no".to_owned(),
            },
        )
    }
    #[test]
    fn guard_and_worker_share_the_existing_budget_and_epochs_never_recycle() -> Result<()> {
        let limits = OrdinaryLimits::default();
        let profile = profile();
        assert_eq!(
            profile.budget.worker.memory_bytes + GUARD_MEMORY_BYTES,
            limits.memory_bytes
        );
        assert_eq!(
            profile.budget.worker.cpu_millicores + GUARD_CPU_MILLICORES,
            limits.cpu_millicores
        );
        assert_eq!(profile.budget.worker.memory_bytes, 224 * MIB);
        assert_eq!(profile.budget.worker.cpu_millicores, 80);
        assert_eq!(profile.spec.security.cpu_quota.as_deref(), Some("0.020"));
        let minimum = PacketWorkloadBudget::split(&OrdinaryLimits {
            cpu_millicores: 30,
            ..limits.clone()
        })?;
        assert_eq!(minimum.worker.cpu_millicores, 10);
        assert_eq!(minimum.worker.cpu_millicores + GUARD_CPU_MILLICORES, 30);
        assert_ne!(profile.policy.epoch, policy().epoch);
        assert_ne!(profile.policy.epoch, self::profile().policy.epoch);
        assert!(
            PacketWorkloadBudget::split(&OrdinaryLimits {
                memory_bytes: 64 * MIB,
                ..limits.clone()
            })
            .is_err()
        );
        assert!(
            PacketWorkloadBudget::split(&OrdinaryLimits {
                cpu_millicores: 29,
                ..limits.clone()
            })
            .is_err()
        );
        assert!(PacketGuardProfile::isolated("unit", "latest", policy(), &limits).is_err());
        assert!(
            PacketGuardProfile::isolated("../other", &profile.spec.image, policy(), &limits)
                .is_err()
        );
        Ok(())
    }
    #[test]
    fn uplink_profile_binds_immutable_network_and_endpoint_ids() -> Result<()> {
        let spec = PacketNetworkSpec::internet("unit", false)?;
        let raw = PacketNetworkInspection::parse(
            &serde_json::json!({"Id":"a".repeat(64),"Name":spec.name(),"Driver":"bridge","Scope":"local","Internal":false,"EnableIPv4":true,"EnableIPv6":false,"Attachable":false,"Ingress":false,"ConfigOnly":false,"ConfigFrom":{"Network":""},"Labels":spec.labels(),"Options":spec.options(),"IPAM":{"Driver":"default","Options":null,"Config":[{"Subnet":"172.29.0.0/16","Gateway":"172.29.0.1"}]},"Containers":{}}),
        )?;
        let mut profile = PacketGuardProfile::isolated(
            "unit",
            &profile().spec.image,
            policy(),
            &OrdinaryLimits::default(),
        )?;
        profile.bind_uplink(spec, &raw)?;
        let (handle, mut actual) = inspection(&profile);
        actual.runtime.network_mode = Some(raw.id.clone());
        actual.networks = vec![raw.name.clone()];
        actual.network_ids.insert(raw.name.clone(), raw.id.clone());
        actual.endpoint_ids.insert(raw.name.clone(), "b".repeat(64));
        profile.verify(&handle, &actual, false)?;
        let mut bad = actual.clone();
        bad.network_ids.insert(raw.name.clone(), "c".repeat(64));
        assert!(profile.verify(&handle, &bad, false).is_err());
        let mut bad = actual.clone();
        bad.endpoint_ids.clear();
        assert!(profile.verify(&handle, &bad, false).is_err());
        let mut bad = actual.clone();
        bad.runtime.network_mode = Some(raw.name.clone());
        assert!(profile.verify(&handle, &bad, false).is_err());
        let mut bad = actual;
        bad.networks.push("foreign".to_owned());
        assert!(profile.verify(&handle, &bad, false).is_err());
        Ok(())
    }
    #[test]
    fn actual_identity_privileges_namespaces_and_resources_are_required_not_just_labels()
    -> Result<()> {
        let profile = profile();
        let (handle, good) = inspection(&profile);
        profile.verify(&handle, &good, false)?;
        for field in [
            "id",
            "name",
            "label",
            "image",
            "network",
            "env",
            "entrypoint",
            "command",
            "cap",
            "cap_missing",
            "privileged",
            "writable",
            "memory",
            "swap",
            "cpu",
            "pids",
            "pidns",
            "ipcns",
            "userns",
            "device",
            "gpu",
            "bind",
            "sysctl",
            "restart",
            "port",
            "tty",
            "seccomp",
            "log",
        ] {
            let mut bad = good.clone();
            match field {
                "id" => bad.id = "3".repeat(64),
                "name" => bad.runtime.name = "foreign".to_owned(),
                "label" => {
                    bad.runtime.labels.remove(ELIXIR_MANAGED_LABEL);
                }
                "image" => {
                    bad.runtime.security.image_id = Some(format!("sha256:{}", "3".repeat(64)))
                }
                "network" => bad.networks.push("bridge".to_owned()),
                "env" => bad.environment.push("LD_PRELOAD=/tmp/evil".to_owned()),
                "entrypoint" => bad.entrypoint = vec!["/bin/sh".to_owned()],
                "command" => bad.command.push("extra".to_owned()),
                "cap" => bad.runtime.security.cap_add.push("NET_RAW".to_owned()),
                "cap_missing" => bad.runtime.security.cap_drop.clear(),
                "privileged" => bad.runtime.security.privileged = None,
                "writable" => bad.runtime.security.read_only_rootfs = false,
                "memory" => bad.runtime.security.memory_limit_bytes = None,
                "swap" => bad.runtime.security.memory_swap_limit_bytes = Some(64 * MIB as i64),
                "cpu" => bad.runtime.security.nano_cpus = Some(100_000_000),
                "pids" => bad.runtime.security.pids_limit = Some(64),
                "pidns" => bad.pid_mode = "host".to_owned(),
                "ipcns" => bad.ipc_mode = "host".to_owned(),
                "userns" => bad.userns_mode = "host".to_owned(),
                "device" => bad.device_count = 1,
                "gpu" => bad.device_request_count = 1,
                "bind" => bad.bind_count = 1,
                "sysctl" => {
                    bad.sysctls
                        .insert("net.ipv4.ip_forward".to_owned(), "1".to_owned());
                }
                "restart" => bad.restart_policy = "always".to_owned(),
                "port" => bad.runtime.published_ports.push(
                    crate::runtime::model::ContainerPublishedPort {
                        container_port: 1,
                        host_port: 1,
                        protocol: "tcp".to_owned(),
                        host_ip: None,
                    },
                ),
                "tty" => bad.runtime.security.tty = None,
                "seccomp" => bad.runtime.security.seccomp_profile = Some("unconfined".to_owned()),
                "log" => bad.runtime.security.log_config = None,
                _ => unreachable!(),
            }
            assert!(profile.verify(&handle, &bad, false).is_err(), "{field}");
        }
        Ok(())
    }
    #[tokio::test]
    async fn controller_handoff_rejects_a_different_live_lease_before_runtime_access() -> Result<()>
    {
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?.join("expected");
        drop(ControllerLease::acquire(&directory)?);
        let wrong = ControllerLease::acquire(&root.path().canonicalize()?.join("wrong"))?;
        // This executable must never be reached: validation precedes all
        // runtime access and cannot create an uplink or contact a provider.
        let runtime = DockerRuntimeManager::new(Some(
            root.path()
                .join("must-not-execute")
                .to_string_lossy()
                .into_owned(),
        ));
        let result = PacketGuardController::start_with_uplink_leased(
            &runtime,
            &directory,
            "lease-test",
            &format!("sha256:{}", "a".repeat(64)),
            policy(),
            &OrdinaryLimits::default(),
            wrong,
        )
        .await;
        assert!(matches!(result,Err(error) if error.to_string().contains("lease does not match")));
        Ok(())
    }
    #[test]
    fn stale_recovery_accepts_only_the_same_frozen_owned_actor_class() -> Result<()> {
        let current = profile();
        let old = profile();
        let (handle, actual) = inspection(&old);
        current.verify(&handle, &actual, true)?;
        assert!(current.verify(&handle, &actual, false).is_err());
        let mut bad = actual;
        bad.runtime
            .labels
            .insert(ELIXIR_DEPLOYMENT_ID_LABEL.to_owned(), "other".to_owned());
        assert!(current.verify(&handle, &bad, true).is_err());
        Ok(())
    }

    async fn docker(args: &[&str]) -> Result<String> {
        let output = timeout(
            Duration::from_secs(20),
            tokio::process::Command::new("docker")
                .args(args)
                .kill_on_drop(true)
                .output(),
        )
        .await??;
        if !output.status.success() {
            bail!(
                "packet fixture Docker operation failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(String::from_utf8(output.stdout)?.trim().to_owned())
    }
    async fn docker_stdin(args: &[&str], bytes: &[u8]) -> Result<String> {
        use tokio::io::AsyncWriteExt;
        if bytes.len() > 4096 {
            bail!("fixture input is oversized");
        }
        let mut child = tokio::process::Command::new("docker")
            .args(args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        timeout(Duration::from_secs(20), async {
            let mut input = child.stdin.take().context("fixture stdin missing")?;
            input.write_all(bytes).await?;
            input.shutdown().await?;
            drop(input);
            let output = child.wait_with_output().await?;
            if !output.status.success() || output.stdout.len() > 4096 || output.stderr.len() > 4096
            {
                bail!("bounded fixture input command failed");
            }
            Ok(String::from_utf8(output.stdout)?.trim().to_owned())
        })
        .await?
    }
    struct FixtureCleanup {
        name: String,
        owner: String,
    }
    struct FixtureNetworkCleanup {
        name: String,
        owner: String,
    }
    impl Drop for FixtureNetworkCleanup {
        fn drop(&mut self) {
            if let Some(output) = cleanup_docker(&[
                "network",
                "inspect",
                "--format",
                "{{.Id}}|{{index .Labels \"elixir.deployment_id\"}}|{{index .Labels \"elixir.marketplace.uplink\"}}|{{index .Labels \"elixir.marketplace.packet_fixture\"}}|{{len .Containers}}",
                &self.name,
            ]) {
                let text = String::from_utf8_lossy(&output);
                let fields = text.trim().split('|').collect::<Vec<_>>();
                if fields.len() == 5
                    && immutable_id(fields[0])
                    && fields[4] == "0"
                    && ((fields[1] == self.owner
                        && fields[2] == crate::runtime::packet_network::PACKET_NETWORK_SCHEMA)
                        || fields[3] == self.owner)
                {
                    let _ = cleanup_docker(&["network", "rm", fields[0]]);
                }
            }
        }
    }
    fn cleanup_docker(args: &[&str]) -> Option<Vec<u8>> {
        use std::io::Read;
        let mut child = std::process::Command::new("docker")
            .args(args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()
            .ok()?;
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        loop {
            match child.try_wait() {
                Ok(Some(status)) if status.success() => break,
                Ok(Some(_)) => return None,
                Ok(None) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                _ => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return None;
                }
            }
        }
        let mut bytes = vec![];
        child
            .stdout
            .take()?
            .take(1025)
            .read_to_end(&mut bytes)
            .ok()?;
        (bytes.len() <= 1024).then_some(bytes)
    }
    impl Drop for FixtureCleanup {
        fn drop(&mut self) {
            if let Some(output) = cleanup_docker(&[
                "inspect",
                "--format",
                "{{.Id}} {{index .Config.Labels \"elixir.marketplace.packet_fixture\"}}",
                &self.name,
            ]) {
                let text = String::from_utf8_lossy(&output);
                let fields = text.split_whitespace().collect::<Vec<_>>();
                if fields.len() == 2
                    && fields[1] == self.owner
                    && fields[0].len() == 64
                    && fields[0]
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                {
                    // Never resolve the mutable name again for removal.
                    let _ = cleanup_docker(&["rm", "-f", fields[0]]);
                }
            }
        }
    }
    fn fixture_profile(
        owner: &str,
        image: &str,
        limits: &OrdinaryLimits,
    ) -> Result<PacketGuardProfile> {
        let mut profile = PacketGuardProfile::isolated(owner, image, policy(), limits)?;
        profile.spec.labels.insert(
            "elixir.marketplace.packet_fixture".to_owned(),
            owner.to_owned(),
        );
        apply_container_spec_fingerprint(&mut profile.spec);
        Ok(profile)
    }
    #[tokio::test]
    #[ignore = "explicit isolated owned-uplink lifecycle; no internet or provider credentials"]
    async fn server_owns_uplink_reuses_only_verified_network_and_refuses_foreign_members_and_names()
    -> Result<()> {
        let runtime = DockerRuntimeManager::new(None);
        let image = docker(&[
            "image",
            "inspect",
            "--format",
            "{{.Id}}",
            "elixir-marketplace-netguard:development",
        ])
        .await?;
        let owner = format!("uplink-{}", uuid::Uuid::new_v4().simple());
        let spec = PacketNetworkSpec::isolated_fixture(&owner, true)?;
        let _network_cleanup = FixtureNetworkCleanup {
            name: spec.name().to_owned(),
            owner: owner.clone(),
        };
        let limits = OrdinaryLimits {
            monthly_transfer_bytes: 32768,
            ..Default::default()
        };
        let directory = tempfile::tempdir()?;
        let root = directory.path().canonicalize()?;
        let controller = root.join("control");
        let profile = fixture_profile(&owner, &image, &limits)?;
        let _guard_cleanup = FixtureCleanup {
            name: profile.spec.name.clone(),
            owner: owner.clone(),
        };
        let mut guard =
            PacketGuardController::start_networked(&runtime, &controller, profile, spec.clone())
                .await?;
        let first = runtime
            .inspect_packet_uplink(spec.name())
            .await?
            .context("fixture uplink missing")?;
        spec.verify(&first)?;
        assert!(first.internal && first.ipv4 && first.ipv6);
        assert_eq!(first.members.len(), 1);
        assert!(
            runtime
                .remove_packet_uplink(&spec, &first.id)
                .await
                .is_err(),
            "occupied uplinks cannot be removed"
        );
        let actual = runtime.inspect_private_actor(guard.handle()).await?;
        assert_eq!(actual.network_ids.get(spec.name()), Some(&first.id));
        assert_eq!(
            actual.runtime.network_mode.as_deref(),
            Some(first.id.as_str())
        );
        let probe_name = format!("elixir-marketplace-uplink-probe-{owner}");
        let _probe_cleanup = FixtureCleanup {
            name: probe_name.clone(),
            owner: owner.clone(),
        };
        let tools = "sha256:0b11c8bc24df09f988781122d1b5f22604d3152b1342545a9369387a39e23403";
        let label = format!("elixir.marketplace.packet_fixture={owner}");
        let probe = docker(&[
            "run",
            "-d",
            "--pull=never",
            "--network",
            &format!("container:{}", guard.handle.id),
            "--name",
            &probe_name,
            "--label",
            &label,
            "--read-only",
            "--cap-drop=ALL",
            "--cap-add=NET_ADMIN",
            "--security-opt=no-new-privileges",
            "--memory=32m",
            "--memory-swap=32m",
            "--cpus=0.1",
            "--pids-limit=16",
            "--log-driver=none",
            "--entrypoint=/bin/sleep",
            tools,
            "180",
        ])
        .await?;
        // Only this disconnected fixture's namespace is changed. An internal
        // Docker bridge supplies no internet default route; add local defaults
        // so attempted plaintext packets actually reach the native filter.
        for subnet in &first.subnets {
            let gateway = subnet.gateway.to_string();
            let mut args = vec!["exec", probe.as_str(), "ip"];
            if subnet.gateway.is_ipv6() {
                args.push("-6");
            }
            args.extend([
                "route", "replace", "default", "via", &gateway, "dev", "eth0",
            ]);
            docker(&args).await?;
        }
        let database = Database::connect(&DatabaseConfig {
            url: format!("sqlite://{}", root.join("server.sqlite").display()),
            max_connections: 1,
            connect_timeout_seconds: 5,
        })
        .await?;
        database.run_migrations().await?;
        guard
            .apply_credit(
                &runtime,
                super::super::accounting::reserve_transfer(&database, 1_790_000_000, 8192, &limits)
                    .await?,
            )
            .await?;
        guard.renew_bootstrap_lease(&runtime).await?;
        let counter = |state: &serde_json::Value| -> u64 {
            state["nftables"]
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|row| row.get("rule"))
                .find(|rule| {
                    rule["chain"] == "outgoing"
                        && rule["expr"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .any(|value| value.get("drop").is_some())
                })
                .unwrap()["expr"]
                .as_array()
                .unwrap()
                .iter()
                .find_map(|value| value.get("counter"))
                .unwrap()["packets"]
                .as_u64()
                .unwrap()
        };
        let before: serde_json::Value = serde_json::from_str(
            &docker(&[
                "exec",
                &probe,
                "nft",
                "-j",
                "list",
                "table",
                "inet",
                "elx_marketplace",
            ])
            .await?,
        )?;
        let code = r#"import socket,os,json,errno
assert os.getuid() in (10004,10005)
status=dict(line.split(':',1) for line in open('/proc/self/status') if ':' in line)
assert int(status['CapEff'],16)==0
for family,host in [(socket.AF_INET,'8.8.8.8'),(socket.AF_INET,'169.254.169.254'),(socket.AF_INET6,'2606:4700:4700::1111'),(socket.AF_INET6,'fd00:ec2::254')]:
    s=socket.socket(family,socket.SOCK_DGRAM)
    try: s.sendto(b'owned-plaintext-denial-fixture',(host,59999))
    except OSError as error: assert error.errno in (errno.EPERM,errno.EACCES)
    finally: s.close()
print(json.dumps({'attempted':4,'uid':os.getuid()}))"#;
        for uid in ["10004:10004", "10005:10005"] {
            let output: serde_json::Value = serde_json::from_str(
                &docker(&["exec", "--user", uid, &probe, "python3", "-c", code]).await?,
            )?;
            assert_eq!(output["attempted"], 4);
        }
        let after: serde_json::Value = serde_json::from_str(
            &docker(&[
                "exec",
                &probe,
                "nft",
                "-j",
                "list",
                "table",
                "inet",
                "elx_marketplace",
            ])
            .await?,
        )?;
        assert!(
            counter(&after) >= counter(&before) + 8,
            "both unprivileged roles' plaintext must reach and be dropped by the actual filter"
        );
        docker(&["rm", "-f", &probe]).await?;
        let old_id = guard.handle.id.clone();
        let old_epoch = guard.profile.policy.epoch;
        drop(guard);
        let mut recovered = PacketGuardController::start_networked(
            &runtime,
            &controller,
            fixture_profile(&owner, &image, &limits)?,
            spec.clone(),
        )
        .await?;
        assert_ne!(recovered.handle.id, old_id);
        assert_ne!(recovered.profile.policy.epoch, old_epoch);
        let network = runtime
            .inspect_packet_uplink(&first.id)
            .await?
            .context("reused uplink missing")?;
        assert_eq!(network.id, first.id);
        assert_eq!(network.members.len(), 1);
        assert!(network.members.contains_key(&recovered.handle.id));
        recovered
            .apply_credit(
                &runtime,
                super::super::accounting::reserve_transfer(&database, 1_790_000_000, 8192, &limits)
                    .await?,
            )
            .await?;
        recovered.renew_bootstrap_lease(&runtime).await?;
        let lease_elements = |state: &serde_json::Value| -> usize {
            state["nftables"]
                .as_array()
                .unwrap()
                .iter()
                .find_map(|row| row.get("set"))
                .unwrap()
                .get("elem")
                .and_then(serde_json::Value::as_array)
                .map_or(0, Vec::len)
        };
        let active: serde_json::Value = serde_json::from_str(
            &docker(&[
                "exec",
                &recovered.handle.id,
                "/usr/sbin/nft",
                "-j",
                "list",
                "set",
                "inet",
                "elx_marketplace",
                "alive",
            ])
            .await?,
        )?;
        assert_eq!(
            lease_elements(&active),
            2,
            "membership-drift check starts from an actual live lease"
        );
        let foreign_name = format!("elixir-marketplace-uplink-foreign-{owner}");
        let _foreign_cleanup = FixtureCleanup {
            name: foreign_name.clone(),
            owner: owner.clone(),
        };
        let foreign = docker(&[
            "run",
            "-d",
            "--pull=never",
            "--network",
            &network.id,
            "--name",
            &foreign_name,
            "--label",
            &label,
            "--read-only",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges",
            "--memory=32m",
            "--memory-swap=32m",
            "--pids-limit=16",
            "--log-driver=none",
            "--entrypoint=/bin/sleep",
            tools,
            "180",
        ])
        .await?;
        assert!(recovered.renew_bootstrap_lease(&runtime).await.is_err());
        let paused: serde_json::Value = serde_json::from_str(
            &docker(&[
                "exec",
                &recovered.handle.id,
                "/usr/sbin/nft",
                "-j",
                "list",
                "set",
                "inet",
                "elx_marketplace",
                "alive",
            ])
            .await?,
        )?;
        assert_eq!(
            lease_elements(&paused),
            0,
            "foreign membership must stop the original kernel lease"
        );
        assert!(
            runtime
                .remove_packet_uplink(&spec, &network.id)
                .await
                .is_err()
        );
        assert_eq!(
            docker(&["inspect", "--format", "{{.Id}}", &foreign]).await?,
            foreign
        );
        docker(&["rm", "-f", &foreign]).await?;
        let handle = recovered.handle.clone();
        recovered.stop(&runtime).await?;
        runtime.remove_container(&handle).await?;
        runtime.remove_packet_uplink(&spec, &network.id).await?;
        assert!(runtime.inspect_packet_uplink(&network.id).await?.is_none());
        // A fixture-owned foreign network deliberately occupies the name.
        // Product startup must not alter or replace it, even when empty.
        let foreign_network = docker(&[
            "network",
            "create",
            "--internal",
            "--label",
            &label,
            spec.name(),
        ])
        .await?;
        assert!(
            PacketGuardController::start_networked(
                &runtime,
                &controller,
                fixture_profile(&owner, &image, &limits)?,
                spec.clone()
            )
            .await
            .is_err()
        );
        assert_eq!(
            docker(&["network", "inspect", "--format", "{{.Id}}", spec.name()]).await?,
            foreign_network
        );
        eprintln!(
            "{}",
            serde_json::json!({"image":image,"owned_uplink":true,"internal_fixture":true,"ipv4_ipv6_plaintext_kernel_drops":8,"network_reused_by_immutable_id":true,"foreign_member_and_name_preserved":true,"excludes":"real provider/WAN/NAT traffic, continuous supervision and aggregate hardware qualification"})
        );
        Ok(())
    }
    #[tokio::test]
    #[ignore = "explicit isolated native packet-controller/server durability integration"]
    async fn server_owns_guard_consumes_durable_credits_and_recovers_without_resetting_or_touching_foreign_names()
    -> Result<()> {
        let runtime = DockerRuntimeManager::new(None);
        let image = docker(&[
            "image",
            "inspect",
            "--format",
            "{{.Id}}",
            "elixir-marketplace-netguard:development",
        ])
        .await?;
        let directory = tempfile::tempdir()?;
        let root = directory.path().canonicalize()?;
        let controller = root.join("controller");
        let url = if let Ok(url) = std::env::var("ELIXIR_MARKETPLACE_PACKET_POSTGRES_URL") {
            let parsed = reqwest::Url::parse(&url)?;
            if parsed.host_str() != Some("127.0.0.1")
                || parsed.path() != "/elixir_marketplace_packet_test"
                || !matches!(parsed.scheme(), "postgres" | "postgresql")
            {
                bail!("packet test requires a dedicated loopback PostgreSQL database");
            }
            url
        } else {
            format!("sqlite://{}", root.join("server.sqlite").display())
        };
        let config = DatabaseConfig {
            url,
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        if database.driver == crate::db::DatabaseDriver::Postgres {
            use sqlx::Row;
            let tables: i64 =
                sqlx::query("SELECT COUNT(*) FROM pg_catalog.pg_tables WHERE schemaname='public'")
                    .fetch_one(&database.pool)
                    .await?
                    .try_get(0)?;
            if tables != 0 {
                bail!("packet PostgreSQL fixture must start empty");
            }
        }
        database.run_migrations().await?;
        let owner = format!("packet-{}", uuid::Uuid::new_v4().simple());
        let limits = OrdinaryLimits {
            monthly_transfer_bytes: 32768,
            ..Default::default()
        };
        let profile = fixture_profile(&owner, &image, &limits)?;
        let cleanup = FixtureCleanup {
            name: profile.spec.name.clone(),
            owner: owner.clone(),
        };
        let mut guard =
            PacketGuardController::start_isolated(&runtime, &controller, profile).await?;
        let old_id = guard.handle.id.clone();
        let old_epoch = guard.profile.policy.epoch;
        let old_ns = guard.namespace_inode();
        let identity = WireguardIdentity {
            device: elixir_marketplace::network_probe::key(&[17; 32])?,
            peer: elixir_marketplace::network_probe::key(&[34; 32])?,
        };
        let absent = guard
            .observe_wireguard(&runtime, &identity)
            .await
            .err()
            .context("an isolated namespace must not claim a tunnel")?;
        assert!(absent.to_string().contains("MissingTunnel"), "{absent:#}");
        assert!(
            PacketGuardController::start_isolated(
                &runtime,
                &controller,
                fixture_profile(&owner, &image, &limits)?
            )
            .await
            .is_err()
        );
        let now = 1_790_000_000;
        guard
            .apply_credit(
                &runtime,
                super::super::accounting::reserve_transfer(&database, now, 8192, &limits).await?,
            )
            .await?;
        guard.renew_bootstrap_lease(&runtime).await?;
        guard.pause(&runtime).await?;
        drop(guard);
        database.pool.close().await;
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        let mut recovered = PacketGuardController::start_isolated(
            &runtime,
            &controller,
            fixture_profile(&owner, &image, &limits)?,
        )
        .await?;
        assert_ne!(recovered.handle.id, old_id);
        assert_ne!(recovered.profile.policy.epoch, old_epoch);
        assert!(recovered.namespace_inode() > 0);
        // nsfs inode numbers may be recycled after destruction; they bind one
        // live actor, not all past incarnations. A new immutable container ID,
        // fresh epoch and successful exclusive CREATE of the policy table
        // establish recovery. Initialize would fail in the still-existing old
        // namespace rather than resetting its table or credit.
        let namespace_inode_recycled = recovered.namespace_inode() == old_ns;
        assert!(
            runtime.get_container_handle(&old_id).await?.is_none(),
            "the old container ID must no longer exist"
        );
        recovered
            .apply_credit(
                &runtime,
                super::super::accounting::reserve_transfer(&database, now + 1, 8192, &limits)
                    .await?,
            )
            .await?;
        recovered.renew_bootstrap_lease(&runtime).await?;
        recovered.stop(&runtime).await?;
        let last = runtime.get_container_handle(&cleanup.name).await?.unwrap();
        assert!(!runtime.inspect(&last).await?.running);
        runtime.remove_container(&last).await?;
        assert!(
            super::super::accounting::reserve_transfer(&database, now + 1, 16385, &limits)
                .await
                .is_err(),
            "spent credits survive actor replacement"
        );
        let fixture_image =
            "sha256:2298d942ce5f8e8083c036b6d62e129379998712ef78343f88d41d782d507141";
        docker(&[
            "run",
            "-d",
            "--pull=never",
            "--network=none",
            "--name",
            &cleanup.name,
            "--label",
            &format!("elixir.marketplace.packet_fixture={owner}"),
            "--cap-drop=ALL",
            "--read-only",
            "--memory=32m",
            "--memory-swap=32m",
            "--pids-limit=8",
            "--log-driver=none",
            "--entrypoint=/bin/sleep",
            fixture_image,
            "120",
        ])
        .await?;
        let foreign = runtime.get_container_handle(&cleanup.name).await?.unwrap();
        assert!(
            PacketGuardController::start_isolated(
                &runtime,
                &controller,
                fixture_profile(&owner, &image, &limits)?
            )
            .await
            .is_err()
        );
        assert!(
            runtime.inspect_private_actor(&foreign).await?.running,
            "foreign actor was not stopped or replaced"
        );
        eprintln!(
            "{}",
            serde_json::json!({
                "image":image,
                "database":if database.driver==crate::db::DatabaseDriver::Postgres {"postgres"} else {"sqlite"},
                "guard_memory_bytes":GUARD_MEMORY_BYTES,
                "guard_cpu_millicores":GUARD_CPU_MILLICORES,
                "guard_pids":GUARD_PIDS,
                "committed_bytes_after_recovery":16384,
            "fresh_container_epoch_and_empty_policy":true,
            "namespace_inode_recycled":namespace_inode_recycled,
                "foreign_name_preserved":true,
                "excludes":"VPN health, public workers, packet consumption and aggregate hardware qualification"
            })
        );
        drop(cleanup);
        Ok(())
    }

    #[tokio::test]
    #[ignore = "explicit isolated server-owned WireGuard kernel observation; no public provider"]
    async fn server_reads_owned_kernel_observation_and_pauses_on_changed_tunnel_configuration()
    -> Result<()> {
        use base64::{
            Engine as _,
            engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
        };
        use rand::RngCore;
        use zeroize::Zeroizing;
        let runtime = DockerRuntimeManager::new(None);
        let image = docker(&[
            "image",
            "inspect",
            "--format",
            "{{.Id}}",
            "elixir-marketplace-netguard:development",
        ])
        .await?;
        let owner = format!("probe-{}", uuid::Uuid::new_v4().simple());
        let limits = OrdinaryLimits {
            monthly_transfer_bytes: 1024 * 1024,
            ..Default::default()
        };
        let directory = tempfile::tempdir()?;
        let root = directory.path().canonicalize()?;
        let profile = fixture_profile(&owner, &image, &limits)?;
        let _guard_cleanup = FixtureCleanup {
            name: profile.spec.name.clone(),
            owner: owner.clone(),
        };
        let mut guard =
            PacketGuardController::start_isolated(&runtime, &root.join("control"), profile).await?;
        let tools = "sha256:0b11c8bc24df09f988781122d1b5f22604d3152b1342545a9369387a39e23403";
        let vpn_name = format!("elixir-marketplace-probe-vpn-{owner}");
        let config_name = format!("elixir-marketplace-probe-config-{owner}");
        let _vpn_cleanup = FixtureCleanup {
            name: vpn_name.clone(),
            owner: owner.clone(),
        };
        let _config_cleanup = FixtureCleanup {
            name: config_name.clone(),
            owner: owner.clone(),
        };
        let label = format!("elixir.marketplace.packet_fixture={owner}");
        let vpn = docker(&[
            "run",
            "-d",
            "--pull=never",
            "--network=none",
            "--name",
            &vpn_name,
            "--label",
            &label,
            "--read-only",
            "--cap-drop=ALL",
            "--cap-add=NET_ADMIN",
            "--security-opt=no-new-privileges",
            "--memory=32m",
            "--memory-swap=32m",
            "--pids-limit=16",
            "--log-driver=none",
            "--entrypoint=/bin/sleep",
            tools,
            "180",
        ])
        .await?;
        let config = docker(&[
            "run",
            "-d",
            "--pull=never",
            "--network",
            &format!("container:{}", guard.handle.id),
            "--pid",
            &format!("container:{vpn}"),
            "--name",
            &config_name,
            "--label",
            &label,
            "--read-only",
            "--cap-drop=ALL",
            "--cap-add=NET_ADMIN",
            "--security-opt=no-new-privileges",
            "--memory=32m",
            "--memory-swap=32m",
            "--pids-limit=16",
            "--log-driver=none",
            "--entrypoint=/bin/sleep",
            tools,
            "180",
        ])
        .await?;
        for (id, args) in [
            (
                &config,
                vec![
                    "ip",
                    "link",
                    "add",
                    "eth0",
                    "type",
                    "veth",
                    "peer",
                    "name",
                    "probe-vpn",
                ],
            ),
            (
                &config,
                vec!["ip", "link", "set", "probe-vpn", "netns", "1"],
            ),
            (
                &config,
                vec!["ip", "addr", "add", "172.31.72.2/30", "dev", "eth0"],
            ),
            (&config, vec!["ip", "link", "set", "eth0", "up"]),
            (
                &config,
                vec![
                    "ip",
                    "route",
                    "add",
                    "default",
                    "via",
                    "172.31.72.1",
                    "dev",
                    "eth0",
                ],
            ),
            (
                &vpn,
                vec!["ip", "addr", "add", "172.31.72.1/30", "dev", "probe-vpn"],
            ),
            (&vpn, vec!["ip", "link", "set", "probe-vpn", "up"]),
            (&vpn, vec!["ip", "addr", "add", "8.8.8.8/32", "dev", "lo"]),
            (&vpn, vec!["ip", "link", "add", "wg0", "type", "wireguard"]),
        ] {
            let mut command = vec!["exec", id.as_str()];
            command.extend(args);
            docker(&command).await?;
        }
        // Fresh throwaway keys only. No file, real profile or provider account
        // is read. Key material travels through a bounded stdin pipe, not argv.
        let mut private_gw = Zeroizing::new([0u8; 32]);
        let mut private_vpn = Zeroizing::new([0u8; 32]);
        rand::rngs::OsRng.fill_bytes(private_gw.as_mut());
        rand::rngs::OsRng.fill_bytes(private_vpn.as_mut());
        let secrets = WireguardSecrets::from_keys(private_gw, None)?;
        let mut private_vpn = Zeroizing::new(STANDARD.encode(private_vpn.as_ref()));
        private_vpn.push('\n');
        let device_key = secrets.device_public_key()?;
        let public_gw = STANDARD.encode(URL_SAFE_NO_PAD.decode(&device_key)?);
        let public_vpn = docker_stdin(
            &["exec", "-i", &vpn, "wg", "pubkey"],
            private_vpn.as_bytes(),
        )
        .await
        .context("derive fresh fixture provider public key")?;
        // Keep the actor alive while preparing the isolated namespace without
        // enabling network traffic; incomplete observations only pause it.
        let identity = WireguardIdentity {
            device: device_key.clone(),
            peer: elixir_marketplace::network_probe::key(&STANDARD.decode(&public_vpn)?)?,
        };
        assert!(guard.observe_wireguard(&runtime, &identity).await.is_err());
        guard
            .configure_wireguard(
                &runtime,
                WireguardSetup {
                    device_public_key: device_key,
                    peer_public_key: identity.peer.clone(),
                    mtu: 1280,
                    keepalive_seconds: 1,
                },
                secrets,
            )
            .await
            .context("configure fixture gateway through owned native guard")?;
        docker_stdin(
            &[
                "exec",
                "-i",
                &vpn,
                "wg",
                "set",
                "wg0",
                "private-key",
                "/dev/stdin",
                "listen-port",
                "51820",
                "peer",
                &public_gw,
                "allowed-ips",
                "10.66.0.2/32",
            ],
            private_vpn.as_bytes(),
        )
        .await
        .context("configure fresh fixture provider tunnel")?;
        for (id, args) in [
            (
                &vpn,
                vec!["ip", "addr", "add", "10.66.0.1/32", "dev", "wg0"],
            ),
            (&vpn, vec!["ip", "link", "set", "wg0", "mtu", "1280", "up"]),
            (
                &vpn,
                vec!["ip", "route", "add", "10.66.0.2/32", "dev", "wg0"],
            ),
        ] {
            let mut command = vec!["exec", id.as_str()];
            command.extend(args);
            docker(&command).await?;
        }
        let database = Database::connect(&DatabaseConfig {
            url: format!("sqlite://{}", root.join("server.sqlite").display()),
            max_connections: 1,
            connect_timeout_seconds: 5,
        })
        .await?;
        database.run_migrations().await?;
        guard
            .apply_credit(
                &runtime,
                super::super::accounting::reserve_transfer(&database, 1_790_000_000, 8192, &limits)
                    .await?,
            )
            .await?;
        guard.renew_bootstrap_lease(&runtime).await?;
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let handshake =
                docker(&["exec", &config, "wg", "show", "wg0", "latest-handshakes"]).await?;
            if handshake
                .split_whitespace()
                .nth(1)
                .is_some_and(|value| value.parse::<u64>().is_ok_and(|value| value > 0))
            {
                break;
            }
            if Instant::now() > deadline {
                bail!("fresh fixture handshake missing");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        docker(&[
            "exec",
            &config,
            "wg",
            "set",
            "wg0",
            "peer",
            &public_vpn,
            "persistent-keepalive",
            "0",
        ])
        .await?;
        let observed = guard.observe_wireguard(&runtime, &identity).await?;
        assert!(observed.is_recent_for(&guard));
        assert_eq!(observed.snapshot().routes.len(), 5);
        guard.renew_observed_lease(&runtime, &observed).await?;
        let active: serde_json::Value = serde_json::from_str(
            &docker(&[
                "exec",
                &config,
                "nft",
                "-j",
                "list",
                "set",
                "inet",
                "elx_marketplace",
                "alive",
            ])
            .await?,
        )?;
        let active = active["nftables"]
            .as_array()
            .context("missing active lease set")?
            .iter()
            .find_map(|value| value.get("set"))
            .context("missing active lease set")?;
        assert_eq!(
            active
                .get("elem")
                .and_then(serde_json::Value::as_array)
                .map(Vec::len),
            Some(2),
            "the negative health check must start from a real live kernel lease"
        );
        docker(&["exec", &config, "wg", "set", "wg0", "fwmark", "0"]).await?;
        assert!(guard.observe_wireguard(&runtime, &identity).await.is_err());
        assert!(guard.network_observation_sequence.is_none());
        assert!(!observed.is_recent_for(&guard));
        assert!(
            guard
                .renew_observed_lease(&runtime, &observed)
                .await
                .is_err()
        );
        let state: serde_json::Value = serde_json::from_str(
            &docker(&[
                "exec",
                &config,
                "nft",
                "-j",
                "list",
                "set",
                "inet",
                "elx_marketplace",
                "alive",
            ])
            .await?,
        )?;
        let alive = state["nftables"]
            .as_array()
            .context("missing lease set")?
            .iter()
            .find_map(|value| value.get("set"))
            .context("missing lease set")?;
        assert!(
            alive
                .get("elem")
                .and_then(serde_json::Value::as_array)
                .is_none_or(|values| values.is_empty()),
            "failed observation must pause the actual kernel lease"
        );
        guard.stop(&runtime).await?;
        eprintln!(
            "{}",
            serde_json::json!({"image":image,"native_owned_tunnel_setup":true,"owned_kernel_observation":true,"real_fixture_handshake":true,"changed_mark_pauses_kernel_lease":true,"guard_memory_mib":32,"guard_cpu_millicores":10,"excludes":"production provider, DNS/exit/firewall integrity checks, continuous supervision, public worker and hardware qualification"})
        );
        Ok(())
    }
}
