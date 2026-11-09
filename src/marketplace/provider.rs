//! Exact native provider ownership. No constructor here grants VPN health,
//! packet allowance, public participation or permission to borrow downloader
//! identity. Only an accepted local owner setup may choose fresh enrollment.
mod enrollment;
mod fence;
mod guard;
mod owner;
mod worker;
use crate::runtime::{
    model::{
        ContainerLogOptions, ContainerSecurityOptions, ContainerSpec, ContainerTmpfsMount,
        ELIXIR_DEPLOYMENT_ID_LABEL, ELIXIR_EXTENSION_ID_LABEL, ELIXIR_INSTANCE_ID_LABEL,
        ELIXIR_MANAGED_LABEL, EnvVar, apply_container_spec_fingerprint,
    },
    packet_network::{PacketNetworkSpec, immutable_id},
};
use anyhow::{Result, ensure};
use elixir_marketplace::{
    identity::Digest,
    warp_provider::{CONSENT_VERSION, Scope, Start},
};
pub use fence::CreateAttempt;
pub use fence::StartAttempt;
pub use guard::{GuardCreateAttempt, GuardStartAttempt, Profile as GuardProfile};
pub(super) use owner::{Owner, Pending};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
pub use worker::{Profile as WorkerProfile, WorkerCreateAttempt, WorkerStartAttempt};

pub(crate) const ENTRYPOINT: &str = "/opt/elixir-marketplace/bin/elixir-marketplace-warp-provider";
pub(super) const FENCE: &str = "provider-native.pending";
pub(super) const NEXT: &str = "provider-native.next";
pub(in crate::marketplace) const TRANSPORT_BOOTSTRAP_BYTES: u64 = 64 * 1024;

/// Frozen DATA for native runtime implementations. No public factory, Clone,
/// Deserialize, mutable spec, secret values or runtime/readiness capability.
pub struct Profile {
    spec: ContainerSpec,
    intent: Intent,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Intent {
    deployment: String,
    image: String,
    network: String,
    scope: Scope,
    session: Digest,
    resume: bool,
    fingerprint: String,
}
impl Intent {
    pub(crate) fn profile(&self) -> Result<Profile> {
        let profile = Profile::compile(
            &self.deployment,
            &self.image,
            &self.network,
            self.scope,
            self.session,
            self.resume,
        )?;
        ensure!(
            profile.intent == *self,
            "provider intent differs from frozen profile"
        );
        Ok(profile)
    }
}
impl Profile {
    #[cfg(test)]
    pub(crate) fn fixture(deployment: &str, image: &str, network: &str) -> Result<Self> {
        Self::compile(
            deployment,
            image,
            network,
            Scope {
                deployment: Self::deployment_scope(deployment),
                enrollment: Digest::of(b"fixture enrollment"),
            },
            Digest::of(uuid::Uuid::new_v4().as_bytes()),
            false,
        )
    }
    pub(super) fn network_owner(deployment: &str) -> String {
        format!("provider-{}", &Digest::of(deployment).to_string()[7..39])
    }
    pub(super) fn deployment_scope(deployment: &str) -> Digest {
        Digest::of(format!(
            "elixir/marketplace/provider/deployment/v1\0{deployment}"
        ))
    }
    pub(super) fn network_spec(deployment: &str) -> Result<PacketNetworkSpec> {
        ensure!(
            !deployment.is_empty()
                && deployment.len() <= 64
                && deployment
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-'),
            "invalid provider deployment"
        );
        PacketNetworkSpec::internet(&Self::network_owner(deployment), false)
    }
    pub(super) fn compile(
        deployment: &str,
        image: &str,
        network: &str,
        scope: Scope,
        session: Digest,
        resume: bool,
    ) -> Result<Self> {
        ensure!(
            !deployment.is_empty()
                && deployment.len() <= 64
                && deployment
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
                && image.strip_prefix("sha256:").is_some_and(immutable_id)
                && immutable_id(network)
                && session.as_bytes() != &[0; 32],
            "provider requires exact local deployment/image/network/session"
        );
        scope.validate()?;
        ensure!(
            scope.deployment == Self::deployment_scope(deployment),
            "provider scope belongs to another deployment"
        );
        let name = format!("elixir-marketplace-provider-{}", &session.to_string()[7..]);
        let tmpfs = |path: &str, size, mode| ContainerTmpfsMount {
            path: path.into(),
            size_mb: Some(size),
            owner_uid: Some(0),
            owner_gid: Some(0),
            mode: Some(mode),
            no_exec: true,
            no_suid: true,
            no_dev: true,
        };
        let mut spec = ContainerSpec {
            name: name.clone(),
            image: image.into(),
            network: network.into(),
            network_mode: None,
            aliases: vec![],
            env: vec![
                EnvVar {
                    name: "WARP_LICENSE_KEY".into(),
                    value: String::new(),
                },
                EnvVar {
                    name: "WARP_ENABLE_NAT".into(),
                    value: String::new(),
                },
            ],
            volumes: vec![],
            ports: vec![],
            labels: HashMap::from([
                (ELIXIR_MANAGED_LABEL.into(), "true".into()),
                (ELIXIR_DEPLOYMENT_ID_LABEL.into(), deployment.into()),
                (
                    ELIXIR_EXTENSION_ID_LABEL.into(),
                    "elixir-marketplace-provider".into(),
                ),
                (ELIXIR_INSTANCE_ID_LABEL.into(), name),
                ("elixir.marketplace.isolation".into(), "provider-v1".into()),
                (
                    "elixir.marketplace.provider.session".into(),
                    session.to_string(),
                ),
                (
                    "elixir.marketplace.provider.scope".into(),
                    scope.deployment.to_string(),
                ),
                (
                    "elixir.marketplace.provider.enrollment".into(),
                    scope.enrollment.to_string(),
                ),
                (
                    "elixir.marketplace.provider.resume".into(),
                    resume.to_string(),
                ),
            ]),
            command: vec![],
            cap_add: ["NET_ADMIN", "SETUID", "SETGID", "CHOWN", "KILL"]
                .into_iter()
                .map(str::to_owned)
                .collect(),
            cap_drop: vec!["ALL".into()],
            devices: vec!["/dev/net/tun:/dev/net/tun".into()],
            sysctls: HashMap::from([
                ("net.ipv6.conf.all.disable_ipv6".into(), "0".into()),
                ("net.ipv4.conf.all.src_valid_mark".into(), "1".into()),
            ]),
            security: ContainerSecurityOptions {
                user: Some("0:0".into()),
                read_only_rootfs: true,
                no_new_privileges: true,
                tmpfs: vec![
                    tmpfs("/var/lib/cloudflare-warp", 32, 0o700),
                    tmpfs("/run", 8, 0o755),
                    tmpfs("/tmp", 8, 0o1777),
                    tmpfs("/var/log", 8, 0o700),
                ],
                memory_limit_mb: Some(256),
                memory_swap_limit_mb: Some(256),
                cpu_quota: Some("0.200".into()),
                pids_limit: Some(64),
                log_options: Some(ContainerLogOptions::Disabled),
                shared_memory_mb: Some(1),
                disable_core_dumps: true,
                open_files_limit: Some(256),
                file_size_limit_bytes: Some(32 * 1024 * 1024),
                stdin_open: true,
                init_process: true,
                ..Default::default()
            },
        };
        apply_container_spec_fingerprint(&mut spec);
        let fingerprint = spec.labels[crate::runtime::model::CONTAINER_SPEC_HASH_LABEL].clone();
        let intent = Intent {
            deployment: deployment.into(),
            image: image.into(),
            network: network.into(),
            scope,
            session,
            resume,
            fingerprint,
        };
        Ok(Self { spec, intent })
    }
    pub fn spec(&self) -> &ContainerSpec {
        &self.spec
    }
    pub(crate) fn intent(&self) -> Intent {
        self.intent.clone()
    }
    pub(crate) fn expected_network_owner(&self) -> String {
        Self::network_owner(&self.intent.deployment)
    }
    pub(super) fn start_data(&self) -> Start {
        Start {
            schema_version: 1,
            session: self.intent.session,
            scope: self.intent.scope,
            consent_version: CONSENT_VERSION.into(),
            resume: self.intent.resume,
        }
    }
}
