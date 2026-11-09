//! Closed foreground HTTP configuration, independent of the catalog worker.
//! This is NOT admission, VPN health, an installed grant, or permission to start
//! sockets. The application owner must supply those separately before input.
use crate::runtime::{
    model::{
        CONTAINER_SPEC_HASH_LABEL, ContainerHandle, ContainerLogOptions, ContainerRuntimeUlimit,
        ContainerSecurityOptions, ContainerSpec, ELIXIR_DEPLOYMENT_ID_LABEL,
        ELIXIR_EXTENSION_ID_LABEL, ELIXIR_INSTANCE_ID_LABEL, ELIXIR_MANAGED_LABEL,
        PrivateActorInspection, apply_container_spec_fingerprint,
    },
    packet_network::immutable_id,
};
use anyhow::{Result, bail};
use elixir_marketplace::{
    identity::Digest,
    sdk_http::{HTTP_DEADLINE_MILLIS, stream::LARGE_STREAM_NATIVE_MILLIS},
};
use std::{collections::HashMap, time::Duration};

pub use crate::marketplace::lease::activity::http::HttpCreateAttempt;

const MIB: u64 = 1024 * 1024;
pub(crate) const MEMORY_BYTES: u64 = 96 * MIB;
pub(crate) const CPU_MILLICORES: u32 = 90;
pub(crate) const ISOLATION: &str = "application-http-v1";
pub(crate) const ENTRYPOINT: &str = "/opt/elixir-marketplace/bin/elixir-marketplace-launcher";
const SCOPE_LABEL: &str = "elixir.marketplace.application_http.scope";
const INVOCATION_LABEL: &str = "elixir.marketplace.application_http.invocation";
const GATEWAY_LABEL: &str = "elixir.marketplace.application_http.gateway";
const OWNER_LABEL: &str = "elixir.marketplace.application_http.owner_deployment";

/// Closed trusted process selection, never a publisher-selected executable or
/// resource override. Omitted StreamLarge preserves existing recovery bytes.
#[derive(Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(in crate::marketplace) enum HttpMode {
    Buffered,
    #[default]
    StreamLarge,
}
fn legacy_mode(mode: &HttpMode) -> bool {
    *mode == HttpMode::StreamLarge
}

/// One original invocation's immutable desired configuration. No mutable spec,
/// Deserialize, Default or unbounded resource selector. Keeping this profile
/// separate prevents application data from inheriting catalog state/allowances.
pub struct ApplicationHttpProfile {
    spec: ContainerSpec,
    intent: ApplicationHttpIntent,
}
/// Bounded recovery DATA only. Decoding it cannot authorize startup, an SDK
/// effect or networking. Reconstruction must match the original frozen profile.
#[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ApplicationHttpIntent {
    deployment: String,
    image: String,
    gateway_id: String,
    scope: Digest,
    invocation: Digest,
    fingerprint: String,
    #[serde(default, skip_serializing_if = "legacy_mode")]
    mode: HttpMode,
}
impl ApplicationHttpIntent {
    pub(crate) fn profile(&self) -> Result<ApplicationHttpProfile> {
        let profile = ApplicationHttpProfile::for_mode(
            &self.deployment,
            &self.image,
            &self.gateway_id,
            self.scope,
            self.invocation,
            self.mode,
        )?;
        if profile.spec.labels[CONTAINER_SPEC_HASH_LABEL] != self.fingerprint {
            bail!("application HTTP recovery differs from its original frozen profile");
        }
        Ok(profile)
    }
}
impl ApplicationHttpProfile {
    pub(crate) fn new(
        deployment: &str,
        image: &str,
        gateway_id: &str,
        scope: Digest,
        invocation: Digest,
    ) -> Result<Self> {
        Self::for_mode(
            deployment,
            image,
            gateway_id,
            scope,
            invocation,
            HttpMode::StreamLarge,
        )
    }
    pub(in crate::marketplace) fn for_mode(
        deployment: &str,
        image: &str,
        gateway_id: &str,
        scope: Digest,
        invocation: Digest,
        mode: HttpMode,
    ) -> Result<Self> {
        if deployment.is_empty()
            || deployment.len() > 64
            || !deployment
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            || !image.strip_prefix("sha256:").is_some_and(immutable_id)
            || !immutable_id(gateway_id)
            || scope.as_bytes() == &[0; 32]
            || invocation.as_bytes() == &[0; 32]
        {
            bail!(
                "application HTTP requires exact owner, image, gateway and invocation identities"
            );
        }
        let deployment_digest = Digest::of(format!(
            "elixir/application-http/deployment/v1\0{deployment}"
        ))
        .to_string();
        // Main extension reconciliation owns storage-* deployments. Never
        // label this independent process as one of its extension instances.
        let helper_deployment = format!(
            "application-http-{}",
            &deployment_digest.trim_start_matches("sha256:")[..32]
        );
        let identity = Digest::of(format!(
            "elixir/application-http/invocation/v1\0{deployment}\0{scope}\0{invocation}"
        ));
        let instance = format!(
            "extension-http-{}",
            identity.to_string().trim_start_matches("sha256:")
        );
        let mut spec = ContainerSpec {
            name: format!("elixir-{instance}"),
            image: image.to_owned(),
            network: String::new(),
            network_mode: Some(format!("container:{gateway_id}")),
            aliases: vec![],
            env: vec![],
            volumes: vec![],
            ports: vec![],
            labels: HashMap::from([
                (ELIXIR_MANAGED_LABEL.into(), "true".into()),
                (ELIXIR_DEPLOYMENT_ID_LABEL.into(), helper_deployment),
                (OWNER_LABEL.into(), deployment.into()),
                (
                    ELIXIR_EXTENSION_ID_LABEL.into(),
                    "elixir-extension-http".into(),
                ),
                (ELIXIR_INSTANCE_ID_LABEL.into(), instance),
                ("elixir.marketplace.isolation".into(), ISOLATION.into()),
                (SCOPE_LABEL.into(), scope.to_string()),
                (INVOCATION_LABEL.into(), invocation.to_string()),
                (GATEWAY_LABEL.into(), gateway_id.into()),
            ]),
            command: vec![
                match mode {
                    HttpMode::Buffered => "--sdk-http",
                    HttpMode::StreamLarge => "--sdk-http-stream-large",
                }
                .into(),
            ],
            cap_add: vec![],
            cap_drop: vec!["ALL".into()],
            devices: vec![],
            sysctls: HashMap::new(),
            security: ContainerSecurityOptions {
                user: Some("10005:10005".into()),
                read_only_rootfs: true,
                no_new_privileges: true,
                memory_limit_mb: Some(MEMORY_BYTES / MIB),
                memory_swap_limit_mb: Some(MEMORY_BYTES / MIB),
                pids_limit: Some(64),
                cpu_quota: Some(format!("0.{CPU_MILLICORES:03}")),
                shared_memory_mb: Some(1),
                disable_core_dumps: true,
                open_files_limit: Some(64),
                file_size_limit_bytes: Some(1),
                log_options: Some(ContainerLogOptions::Disabled),
                stdin_open: true,
                ..Default::default()
            },
        };
        apply_container_spec_fingerprint(&mut spec);
        let intent = ApplicationHttpIntent {
            deployment: deployment.to_owned(),
            image: image.to_owned(),
            gateway_id: gateway_id.to_owned(),
            scope,
            invocation,
            fingerprint: spec.labels[CONTAINER_SPEC_HASH_LABEL].clone(),
            mode,
        };
        Ok(Self { spec, intent })
    }
    pub(crate) fn intent(&self) -> ApplicationHttpIntent {
        self.intent.clone()
    }
    /// Bind recovery/configuration data to the actual source executor. A valid
    /// profile for a different invocation is not interchangeable authority.
    pub(crate) fn require_invocation(&self, scope: Digest, invocation: Digest) -> Result<()> {
        if self.intent.scope != scope || self.intent.invocation != invocation {
            bail!("application HTTP profile belongs to another source invocation");
        }
        Ok(())
    }
    pub(crate) fn spec(&self) -> &ContainerSpec {
        &self.spec
    }
    pub(crate) fn native_deadline(&self) -> Duration {
        Duration::from_millis(match self.intent.mode {
            HttpMode::Buffered => HTTP_DEADLINE_MILLIS,
            HttpMode::StreamLarge => LARGE_STREAM_NATIVE_MILLIS,
        })
    }
    /// Check actual immutable OCI state, never a peer's resource/health flag.
    /// The Docker start boundary must additionally check Created/no Init before
    /// starting. A matching gateway ID here does NOT attest its protection.
    pub(crate) fn verify(
        &self,
        handle: &ContainerHandle,
        actual: &PrivateActorInspection,
    ) -> Result<()> {
        let state = &actual.runtime;
        let security = &state.security;
        let exact_limit = |value: &Option<ContainerRuntimeUlimit>, expected| {
            value
                .as_ref()
                .is_some_and(|v| v.soft == expected && v.hard == expected)
        };
        let mut keys = std::collections::HashSet::new();
        let ambient = actual.environment.iter().all(|entry| {
            entry.split_once('=').is_some_and(|(key, value)| {
                matches!(key, "PATH" | "NODE_VERSION" | "YARN_VERSION")
                    && !value.is_empty()
                    && keys.insert(key)
            })
        });
        if !immutable_id(&handle.id)
            || actual.id != handle.id
            || handle.name != self.spec.name
            || state.name != self.spec.name
            || state.network_mode != self.spec.network_mode
            || self
                .spec
                .labels
                .iter()
                .any(|(k, v)| state.labels.get(k) != Some(v))
            || !state.mounts.is_empty()
            || !state.published_ports.is_empty()
            || security.image_reference.as_deref() != Some(self.spec.image.as_str())
            || security.image_id.as_deref() != Some(self.spec.image.as_str())
            || security.user.as_deref() != Some("10005:10005")
            || !security.read_only_rootfs
            || !security.no_new_privileges
            || security.privileged != Some(false)
            || !security.cap_add.is_empty()
            || security.cap_drop.len() != 1
            || !security.cap_drop[0].eq_ignore_ascii_case("ALL")
            || !security.tmpfs.is_empty()
            || security.memory_limit_bytes != Some(MEMORY_BYTES as i64)
            || security.memory_swap_limit_bytes != Some(MEMORY_BYTES as i64)
            || security.nano_cpus != Some(i64::from(CPU_MILLICORES) * 1_000_000)
            || security.pids_limit != Some(64)
            || security.shared_memory_bytes != Some(MIB as i64)
            || !exact_limit(&security.core_dump_limit, 0)
            || !exact_limit(&security.open_files_limit, 64)
            || !exact_limit(&security.file_size_limit, 1)
            || !security
                .log_config
                .as_ref()
                .is_some_and(|v| v.driver == "none" && v.options.is_empty())
            || security.stdin_open != Some(true)
            || security.tty != Some(false)
            || security.seccomp_profile.is_some()
            || security.apparmor_profile.is_some()
            || actual.command != self.spec.command
            || actual.entrypoint != [ENTRYPOINT.to_owned()]
            || !ambient
            || !actual.pid_mode.is_empty()
            || actual.ipc_mode != "private"
            || !actual.userns_mode.is_empty()
            || actual.device_count != 0
            || actual.device_request_count != 0
            || actual.bind_count != 0
            || !actual.sysctls.is_empty()
            || !actual.networks.is_empty()
            || !actual.network_ids.is_empty()
            || !actual.endpoint_ids.is_empty()
            || actual.restart_policy != "no"
        {
            bail!("application HTTP identity or isolation differs from its original profile");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
