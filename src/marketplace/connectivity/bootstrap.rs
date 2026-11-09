//! Temporary initial health process: it replaces the worker's resource slot,
//! uses only prepaid protected traffic, and never enables a public worker.
use super::{ConnectivityMonitor, ConnectivityObservation, ProbePolicy, exchange};
use crate::marketplace::{
    accounting::CommittedTransferReservation, packet::PacketGuardController,
    runtime::image_is_pinned,
};
use crate::runtime::{
    RuntimeManager,
    model::{
        ContainerHandle, ContainerLogOptions, ContainerSecurityOptions, ContainerSpec,
        ELIXIR_DEPLOYMENT_ID_LABEL, ELIXIR_EXTENSION_ID_LABEL, ELIXIR_INSTANCE_ID_LABEL,
        ELIXIR_MANAGED_LABEL, PrivateActorInspection, apply_container_spec_fingerprint,
    },
};
use anyhow::{Context, Result, bail};
use elixir_marketplace::{
    connectivity::{ConnectivityRequest, ProbeContext},
    identity::Digest,
    network_probe::WireguardIdentity,
    policy::OrdinaryLimits,
};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
use tokio::time::timeout;

pub const BOOTSTRAP_PROBE_BYTES: u64 = 64 * 1024;
const ENTRYPOINT: &str = "/opt/elixir-marketplace/bin/elixir-marketplace-launcher";

struct BootstrapProfile {
    spec: ContainerSpec,
    memory: u64,
    cpu: u32,
    guard_id: String,
}
impl BootstrapProfile {
    fn new(deployment: &str, guard_id: &str, image: &str, limits: &OrdinaryLimits) -> Result<Self> {
        limits.validate()?;
        if !image_is_pinned(image)
            || !crate::runtime::packet_network::immutable_id(guard_id)
            || deployment.is_empty()
            || deployment.len() > 64
            || !deployment
                .bytes()
                .all(|value| value.is_ascii_alphanumeric() || value == b'-')
            || !limits.memory_bytes.is_multiple_of(1024 * 1024)
        {
            bail!("bootstrap probe identity or budget");
        }
        let mut spec = ContainerSpec {
            name: format!("elixir-marketplace-connectivity-{deployment}"),
            image: image.to_owned(),
            network: String::new(),
            network_mode: Some(format!("container:{guard_id}")),
            aliases: vec![],
            env: vec![],
            volumes: vec![],
            ports: vec![],
            labels: HashMap::from([
                (ELIXIR_MANAGED_LABEL.to_owned(), "true".to_owned()),
                (ELIXIR_DEPLOYMENT_ID_LABEL.to_owned(), deployment.to_owned()),
                (
                    ELIXIR_EXTENSION_ID_LABEL.to_owned(),
                    "elixir-marketplace-connectivity".to_owned(),
                ),
                (
                    ELIXIR_INSTANCE_ID_LABEL.to_owned(),
                    format!("marketplace-connectivity-{deployment}"),
                ),
                (
                    "elixir.marketplace.isolation".to_owned(),
                    "connectivity-v1".to_owned(),
                ),
                (
                    "elixir.marketplace.probe.guard".to_owned(),
                    guard_id.to_owned(),
                ),
            ]),
            command: vec!["--connectivity-probe".to_owned()],
            cap_add: vec![],
            cap_drop: vec!["ALL".to_owned()],
            devices: vec![],
            sysctls: HashMap::new(),
            security: ContainerSecurityOptions {
                user: Some("10005:10005".to_owned()),
                read_only_rootfs: true,
                no_new_privileges: true,
                memory_limit_mb: Some(limits.memory_bytes / (1024 * 1024)),
                memory_swap_limit_mb: Some(limits.memory_bytes / (1024 * 1024)),
                pids_limit: Some(64),
                cpu_quota: Some(format!(
                    "{}.{:03}",
                    limits.cpu_millicores / 1000,
                    limits.cpu_millicores % 1000
                )),
                shared_memory_mb: Some(1),
                disable_core_dumps: true,
                open_files_limit: Some(64),
                file_size_limit_bytes: Some(1),
                stdin_open: true,
                log_options: Some(ContainerLogOptions::Disabled),
                ..Default::default()
            },
        };
        apply_container_spec_fingerprint(&mut spec);
        Ok(Self {
            spec,
            memory: limits.memory_bytes,
            cpu: limits.cpu_millicores,
            guard_id: guard_id.to_owned(),
        })
    }
    fn owned(&self, handle: &ContainerHandle, actual: &PrivateActorInspection) -> Result<()> {
        let s = &actual.runtime.security;
        if actual.id != handle.id
            || actual.runtime.name != self.spec.name
            || handle.name != self.spec.name
            || self
                .spec
                .labels
                .iter()
                .any(|(key, value)| actual.runtime.labels.get(key) != Some(value))
            || actual.runtime.network_mode != self.spec.network_mode
            || !actual.runtime.mounts.is_empty()
            || !actual.runtime.published_ports.is_empty()
            || actual.command != self.spec.command
            || actual.entrypoint != [ENTRYPOINT.to_owned()]
            || !actual.pid_mode.is_empty()
            || actual.ipc_mode != "private"
            || !actual.userns_mode.is_empty()
            || actual.device_count != 0
            || actual.device_request_count != 0
            || actual.bind_count != 0
            || !actual.sysctls.is_empty()
            || actual.restart_policy != "no"
            || s.image_reference.as_deref() != Some(self.spec.image.as_str())
            || !s.image_id.as_deref().is_some_and(image_is_pinned)
            || (self.spec.image.starts_with("sha256:")
                && s.image_id.as_deref() != Some(self.spec.image.as_str()))
            || s.user.as_deref() != Some("10005:10005")
            || !s.read_only_rootfs
            || !s.no_new_privileges
            || s.privileged != Some(false)
            || !s.cap_add.is_empty()
            || !s.cap_drop.iter().any(|cap| cap.eq_ignore_ascii_case("ALL"))
            || !s.tmpfs.is_empty()
            || s.memory_limit_bytes != Some(self.memory as i64)
            || s.memory_swap_limit_bytes != Some(self.memory as i64)
            || s.nano_cpus != Some(i64::from(self.cpu) * 1_000_000)
            || s.pids_limit != Some(64)
            || s.shared_memory_bytes != Some(1024 * 1024)
            || s.stdin_open != Some(true)
            || s.tty != Some(false)
            || s.seccomp_profile.is_some()
            || s.apparmor_profile.is_some()
            || !s
                .core_dump_limit
                .as_ref()
                .is_some_and(|v| v.soft == 0 && v.hard == 0)
            || !s
                .open_files_limit
                .as_ref()
                .is_some_and(|v| v.soft == 64 && v.hard == 64)
            || !s
                .file_size_limit
                .as_ref()
                .is_some_and(|v| v.soft == 1 && v.hard == 1)
            || !s
                .log_config
                .as_ref()
                .is_some_and(|v| v.driver == "none" && v.options.is_empty())
            || actual.environment.iter().any(|value| {
                !value.starts_with("PATH=")
                    && !value.starts_with("NODE_VERSION=")
                    && !value.starts_with("YARN_VERSION=")
            })
        {
            bail!("bootstrap connectivity process differs from its owned profile");
        }
        Ok(())
    }
    async fn inspect(
        &self,
        runtime: &dyn RuntimeManager,
        handle: &ContainerHandle,
    ) -> Result<PrivateActorInspection> {
        let actual = timeout(
            Duration::from_secs(10),
            runtime.inspect_private_actor(handle),
        )
        .await
        .context("bootstrap probe inspection deadline")??;
        self.owned(handle, &actual)?;
        Ok(actual)
    }
    async fn cleanup(&self, runtime: &dyn RuntimeManager, handle: &ContainerHandle) -> Result<()> {
        self.inspect(runtime, handle).await?;
        timeout(Duration::from_secs(20), runtime.stop_container(handle))
            .await
            .context("bootstrap probe stop deadline")??;
        if self.inspect(runtime, handle).await?.running {
            bail!("bootstrap probe did not stop");
        }
        timeout(Duration::from_secs(20), runtime.remove_container(handle))
            .await
            .context("bootstrap probe removal deadline")??;
        if timeout(
            Duration::from_secs(10),
            runtime.get_container_handle(&handle.id),
        )
        .await
        .context("bootstrap probe removal confirmation deadline")??
        .is_some()
        {
            bail!("bootstrap probe removal is unconfirmed");
        }
        Ok(())
    }
}

impl ConnectivityMonitor {
    /// Startup holds the stable packet AND worker controller leases before
    /// invoking this. A prior probe is reconstructed from its immutable guard
    /// reference and must match the entire frozen profile before any stop.
    pub(in crate::marketplace) async fn recover_bootstrap_probe(
        runtime: &dyn RuntimeManager,
        deployment: &str,
        image: &str,
        limits: &OrdinaryLimits,
    ) -> Result<()> {
        let name = format!("elixir-marketplace-connectivity-{deployment}");
        let Some(handle) = timeout(Duration::from_secs(10), runtime.get_container_handle(&name))
            .await
            .context("stale bootstrap probe lookup deadline")??
        else {
            return Ok(());
        };
        let actual = timeout(
            Duration::from_secs(10),
            runtime.inspect_private_actor(&handle),
        )
        .await
        .context("stale bootstrap probe inspection deadline")??;
        let guard = actual
            .runtime
            .labels
            .get("elixir.marketplace.probe.guard")
            .context("stale bootstrap guard identity missing")?;
        let profile = BootstrapProfile::new(deployment, guard, image, limits)?;
        profile.owned(&handle, &actual)?;
        profile.cleanup(runtime, &handle).await
    }
    /// Initial probe consumes an actual one-use transfer debit. The native
    /// watchdog bounds abandoned helpers; the packet lease remains <=15 s.
    /// Success closes the lease and checks WireGuard, not public-worker readiness.
    pub async fn observe_bootstrap(
        &mut self,
        runtime: &dyn RuntimeManager,
        guard: &mut PacketGuardController,
        image: &str,
        identity: &WireguardIdentity,
        debit: CommittedTransferReservation,
        policy: ProbePolicy,
    ) -> Result<()> {
        self.current = None;
        guard.pause(runtime).await?;
        let deployment = guard
            .profile()
            .container_spec()
            .labels
            .get(ELIXIR_DEPLOYMENT_ID_LABEL)
            .context("missing bootstrap deployment")?;
        let profile = BootstrapProfile::new(
            deployment,
            &guard.handle().id,
            image,
            guard.profile().budget().worker_limits(),
        )?;
        if let Some(worker) = timeout(
            Duration::from_secs(10),
            runtime.get_container_handle(&format!("elixir-marketplace-worker-{deployment}")),
        )
        .await
        .context("bootstrap worker inspection deadline")??
            && timeout(Duration::from_secs(10), runtime.inspect(&worker))
                .await
                .context("bootstrap worker status deadline")??
                .running
        {
            bail!("bootstrap health cannot run alongside a marketplace worker");
        }
        if debit.bytes() != BOOTSTRAP_PROBE_BYTES {
            bail!("bootstrap health requires its fixed committed allowance");
        }
        let features = timeout(Duration::from_secs(10), runtime.image_labels(image))
            .await
            .context("bootstrap image inspection deadline")??;
        if features
            .get("elixir.marketplace.worker.connectivity_probe")
            .map(String::as_str)
            != Some("2")
        {
            bail!("bootstrap image lacks independent connectivity support");
        }
        // Stable name bounds abandoned container metadata. No persistent state
        // or volume belongs to this helper. Foreign same-name containers fail.
        Self::recover_bootstrap_probe(
            runtime,
            deployment,
            image,
            guard.profile().budget().worker_limits(),
        )
        .await?;
        guard.apply_credit(runtime, debit).await?;
        guard.renew_bootstrap_lease(runtime).await?;
        let started = Instant::now();
        let handle = match timeout(
            Duration::from_secs(20),
            runtime.ensure_container(&profile.spec),
        )
        .await
        {
            Ok(Ok(handle)) => handle,
            outcome => {
                let _ = guard.pause(runtime).await;
                return match outcome {
                    Ok(Err(error)) => Err(error),
                    Err(_) => Err(anyhow::anyhow!("bootstrap process startup deadline")),
                    _ => unreachable!(),
                };
            }
        };
        let result = async {
            let actual = profile.inspect(runtime, &handle).await?;
            if !actual.running {
                bail!("bootstrap helper did not start");
            }
            let process = timeout(
                Duration::from_secs(10),
                runtime.attach_private_stdio(&handle, &profile.spec.labels),
            )
            .await
            .context("bootstrap private attachment deadline")??;
            let request = ConnectivityRequest {
                schema_version: 1,
                nonce: Digest::of(uuid::Uuid::new_v4().as_bytes()),
                family: policy.family,
                require_warp: policy.require_warp,
            };
            let context = ProbeContext {
                namespace_inode: guard.namespace_inode(),
                memory_bytes: profile.memory,
                cpu_millicores: profile.cpu,
                pids: 64,
                tunnel_addresses: &guard.profile().policy().tunnel_addresses,
            };
            let report = exchange(process, &request, &context, Duration::from_secs(18)).await?;
            profile.inspect(runtime, &handle).await?;
            Ok::<_, anyhow::Error>(report)
        }
        .await;
        let paused = guard.pause(runtime).await;
        let cleanup = profile.cleanup(runtime, &handle).await;
        let report = result?;
        paused?;
        cleanup?;
        guard.observe_wireguard(runtime, identity).await?;
        self.current = Some(ConnectivityObservation {
            guard_id: profile.guard_id,
            epoch: guard.profile().policy().epoch,
            started,
            report,
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::DatabaseConfig,
        db::Database,
        marketplace::{accounting::reserve_transfer, packet::PacketGuardProfile},
        runtime::docker::DockerRuntimeManager,
    };
    use elixir_marketplace::{
        connectivity::ProbeFamily,
        packet_guard::{PacketPolicy, VpnEndpoint},
        policy::TransferLedger,
    };
    use std::process::Command;

    fn docker(args: &[&str]) -> Result<String> {
        let output = Command::new("docker").args(args).output()?;
        if !output.status.success() {
            bail!("isolated bootstrap fixture Docker operation failed");
        }
        Ok(String::from_utf8(output.stdout)?.trim().to_owned())
    }
    struct Cleanup {
        names: Vec<String>,
        owner: String,
    }
    impl Drop for Cleanup {
        fn drop(&mut self) {
            for name in &self.names {
                let deployment = docker(&[
                    "inspect",
                    "--format",
                    "{{index .Config.Labels \"elixir.deployment_id\"}}",
                    name,
                ]);
                let fixture = docker(&[
                    "inspect",
                    "--format",
                    "{{index .Config.Labels \"elixir.marketplace.connectivity_test\"}}",
                    name,
                ]);
                if deployment.as_ref().is_ok_and(|value| value == &self.owner)
                    || fixture.as_ref().is_ok_and(|value| value == &self.owner)
                {
                    let _ = docker(&["rm", "-f", name]);
                }
            }
        }
    }

    #[tokio::test]
    #[ignore = "explicit isolated native guard/bootstrap probe lifecycle; no real provider"]
    async fn bootstrap_failure_closes_the_kernel_lease_keeps_the_debit_and_preserves_foreign_names()
    -> Result<()> {
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))?;
        }
        let config = DatabaseConfig {
            url: format!("sqlite://{}", directory.join("server.sqlite").display()),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        sqlx::raw_sql(include_str!(
            "../../../migrations/0060_marketplace_transfer_accounting.sql"
        ))
        .execute(&database.pool)
        .await?;
        let runtime = DockerRuntimeManager::new(Some("docker".to_owned()));
        let guard_image = std::env::var("ELIXIR_MARKETPLACE_PACKET_GUARD_IMAGE")
            .unwrap_or_else(|_| "elixir-marketplace-netguard:development".to_owned());
        let probe_image = std::env::var("ELIXIR_MARKETPLACE_CONNECTIVITY_IMAGE")
            .unwrap_or_else(|_| "elixir-marketplace-worker:connectivity-probe".to_owned());
        let guard_image = docker(&["image", "inspect", "--format", "{{.Id}}", &guard_image])?;
        let probe_image = docker(&["image", "inspect", "--format", "{{.Id}}", &probe_image])?;
        let owner = format!("health-{}", uuid::Uuid::new_v4().simple());
        let limits = OrdinaryLimits {
            monthly_transfer_bytes: 2 * BOOTSTRAP_PROBE_BYTES,
            ..Default::default()
        };
        let profile = PacketGuardProfile::isolated(
            &owner,
            &guard_image,
            PacketPolicy {
                schema_version: 1,
                epoch: Digest::of(b"discarded-fixture-epoch"),
                tunnel_interface: "wg0".to_owned(),
                tunnel_addresses: vec!["10.66.0.2".parse()?],
                endpoints: vec![VpnEndpoint {
                    address: "8.8.8.8".parse()?,
                    port: 51820,
                    local_port: 51821,
                }],
                lease_seconds: 15,
                upload_bytes_per_second: 32768,
                download_bytes_per_second: 131072,
            },
            &limits,
        )?;
        let helper = format!("elixir-marketplace-connectivity-{owner}");
        let _cleanup = Cleanup {
            names: vec![profile.container_spec().name.clone(), helper.clone()],
            owner: owner.clone(),
        };
        let mut guard =
            PacketGuardController::start_isolated(&runtime, &directory, profile).await?;
        let identity = WireguardIdentity {
            device: elixir_marketplace::network_probe::key(&[17; 32])?,
            peer: elixir_marketplace::network_probe::key(&[34; 32])?,
        };
        let now = 1_790_000_000;
        let mut monitor = ConnectivityMonitor::default();
        assert!(
            monitor
                .observe_bootstrap(
                    &runtime,
                    &mut guard,
                    &probe_image,
                    &identity,
                    reserve_transfer(&database, now, BOOTSTRAP_PROBE_BYTES, &limits).await?,
                    ProbePolicy {
                        family: ProbeFamily::Ipv4,
                        require_warp: false
                    }
                )
                .await
                .is_err(),
            "network-none must never produce a successful connectivity observation"
        );
        assert!(monitor.current(&guard).is_none());
        assert!(
            runtime.get_container_handle(&helper).await?.is_none(),
            "failed helper is removed, not retained as infrastructure"
        );
        let command = [
            "/usr/sbin/nft",
            "-j",
            "list",
            "set",
            "inet",
            "elx_marketplace",
            "alive",
        ]
        .map(str::to_owned);
        // Independent, read-only fixture observation. The generic five-second
        // Docker exec probe can be shorter than cold nft startup at 0.01 CPU.
        // Keep the actual actor resource limits and 15-second lease unchanged.
        let snapshot = timeout(
            Duration::from_secs(15),
            tokio::process::Command::new("docker")
                .args(["exec", "--user", "0:0", guard.handle().id.as_str()])
                .args(&command)
                .kill_on_drop(true)
                .output(),
        )
        .await
        .context("isolated kernel lease inspection deadline")??;
        if !snapshot.status.success() {
            bail!("isolated kernel lease inspection failed");
        }
        let snapshot = String::from_utf8(snapshot.stdout)?;
        let value: serde_json::Value = serde_json::from_str(&snapshot)?;
        let set = value["nftables"]
            .as_array()
            .context("missing kernel set")?
            .iter()
            .find_map(|item| item.get("set"))
            .context("missing lease set")?;
        assert!(
            set.get("elem")
                .and_then(serde_json::Value::as_array)
                .is_none_or(Vec::is_empty)
        );
        let state: String = sqlx::query_scalar(
            "SELECT state_json FROM marketplace_transfer_accounting WHERE singleton=1",
        )
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(
            TransferLedger::restore(state.as_bytes())?.used_bytes(),
            BOOTSTRAP_PROBE_BYTES
        );
        // A crashed parent can leave its owned helper attached to an old
        // guard. Recover the frozen class without any new debit or health.
        let abandoned = BootstrapProfile::new(
            &owner,
            &guard.handle().id,
            &probe_image,
            guard.profile().budget().worker_limits(),
        )?;
        let orphan = runtime.ensure_container(&abandoned.spec).await?;
        abandoned.inspect(&runtime, &orphan).await?;
        ConnectivityMonitor::recover_bootstrap_probe(
            &runtime,
            &owner,
            &probe_image,
            guard.profile().budget().worker_limits(),
        )
        .await?;
        assert!(runtime.get_container_handle(&helper).await?.is_none());
        let unchanged: String = sqlx::query_scalar(
            "SELECT state_json FROM marketplace_transfer_accounting WHERE singleton=1",
        )
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(
            unchanged, state,
            "orphan cleanup is not a transfer debit or refund"
        );
        let foreign = docker(&[
            "create",
            "--name",
            &helper,
            "--label",
            &format!("elixir.marketplace.connectivity_test={owner}"),
            "--network=none",
            &probe_image,
            "--connectivity-probe",
        ])?;
        assert!(
            ConnectivityMonitor::recover_bootstrap_probe(
                &runtime,
                &owner,
                &probe_image,
                guard.profile().budget().worker_limits()
            )
            .await
            .is_err()
        );
        assert!(
            monitor
                .observe_bootstrap(
                    &runtime,
                    &mut guard,
                    &probe_image,
                    &identity,
                    reserve_transfer(&database, now + 1, BOOTSTRAP_PROBE_BYTES, &limits).await?,
                    ProbePolicy {
                        family: ProbeFamily::Ipv4,
                        require_warp: false
                    }
                )
                .await
                .is_err()
        );
        assert_eq!(
            runtime
                .get_container_handle(&helper)
                .await?
                .context("foreign helper disappeared")?
                .id,
            foreign
        );
        assert!(
            !runtime
                .inspect(&runtime.get_container_handle(&helper).await?.unwrap())
                .await?
                .running
        );
        guard.stop(&runtime).await?;
        database.pool.close().await;
        eprintln!(
            "{}",
            serde_json::json!({"guard":guard_image,"probe":probe_image,"isolated_network_none":true,"failed_health_closes_lease":true,
            "spent_bytes_before_foreign_test":BOOTSTRAP_PROBE_BYTES,"helper_removed":true,"foreign_preserved":true,
            "excludes":"positive provider/DNS/TLS health, public worker, full-catalog concurrency and aggregate playback"})
        );
        Ok(())
    }
}
