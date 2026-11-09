use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

pub const CONTAINER_SPEC_HASH_LABEL: &str = "elixir.spec_hash";
pub const ELIXIR_DEPLOYMENT_ID_LABEL: &str = "elixir.deployment_id";
pub const ELIXIR_EXTENSION_ID_LABEL: &str = "elixir.extension_id";
pub const ELIXIR_INSTANCE_ID_LABEL: &str = "elixir.instance_id";
pub const ELIXIR_MANAGED_LABEL: &str = "elixir.managed";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerSpec {
    pub name: String,
    pub image: String,
    pub network: String,
    #[serde(default)]
    pub network_mode: Option<String>,
    #[serde(default)]
    pub aliases: Vec<String>,
    #[serde(default)]
    pub env: Vec<EnvVar>,
    #[serde(default)]
    pub volumes: Vec<VolumeMount>,
    #[serde(default)]
    pub ports: Vec<PortMapping>,
    #[serde(default)]
    pub labels: HashMap<String, String>,
    #[serde(default)]
    pub command: Vec<String>,
    #[serde(default)]
    pub cap_add: Vec<String>,
    #[serde(default)]
    pub cap_drop: Vec<String>,
    #[serde(default)]
    pub devices: Vec<String>,
    #[serde(default)]
    pub sysctls: HashMap<String, String>,
    #[serde(default)]
    pub security: ContainerSecurityOptions,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ContainerSecurityOptions {
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub read_only_rootfs: bool,
    #[serde(default)]
    pub no_new_privileges: bool,
    #[serde(default)]
    pub tmpfs: Vec<ContainerTmpfsMount>,
    #[serde(default)]
    pub memory_limit_mb: Option<u64>,
    #[serde(default)]
    pub pids_limit: Option<u64>,
    #[serde(default)]
    pub cpu_quota: Option<String>,
    #[serde(default)]
    pub seccomp_profile: Option<String>,
    #[serde(default)]
    pub apparmor_profile: Option<String>,
    /// Total RAM plus swap. Equal to memory_limit_mb disables container swap.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_swap_limit_mb: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_options: Option<ContainerLogOptions>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shared_memory_mb: Option<u64>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub disable_core_dumps: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub open_files_limit: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_size_limit_bytes: Option<u64>,
    /// Keep the private process input pipe open until a trusted controller
    /// attaches. No TTY is allocated. Existing service specs remain unchanged.
    #[serde(default, skip_serializing_if = "is_false")]
    pub stdin_open: bool,
    /// Reap orphaned native watchdogs. Omission preserves existing frozen
    /// specifications; the caller must explicitly select this PID1 topology.
    #[serde(default, skip_serializing_if = "is_false")]
    pub init_process: bool,
}

fn is_false(value: &bool) -> bool {
    !value
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContainerLogOptions {
    Disabled,
    BoundedJson { max_size_mb: u32, max_files: u16 },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContainerTmpfsMount {
    pub path: String,
    #[serde(default)]
    pub size_mb: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_uid: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_gid: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<u16>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub no_exec: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub no_suid: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub no_dev: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvVar {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeMount {
    #[serde(default)]
    pub source_kind: VolumeMountSourceKind,
    pub host_path: String,
    pub container_path: String,
    #[serde(default)]
    pub read_only: bool,
}

#[derive(Debug, Clone)]
pub struct PrivateFileVolumeSpec {
    pub name: String,
    pub image: String,
    pub source_path: String,
    pub file_name: String,
    pub owner_uid: u32,
    pub owner_gid: u32,
    pub labels: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct OwnedDirectoryVolumeSpec {
    pub name: String,
    pub image: String,
    pub owner_uid: u32,
    pub owner_gid: u32,
    pub labels: HashMap<String, String>,
}

/// Controller-owned fixed catalog storage; no peer paths, scripts or UID
/// overrides. Only the immutable worker image's bundled initializer may run.
#[derive(Debug, Clone)]
pub struct FixedMarketplaceVolumeSpec {
    pub name: String,
    pub image: String,
    pub memory_limit_mb: u64,
    pub cpu_millicores: u32,
    pub labels: HashMap<String, String>,
}

/// One controller-owned foreground source workspace. The closed descriptor
/// supplies sizes/digests only, never paths, scripts, mounts or reset authority.
#[derive(Debug, Clone)]
pub struct QuarantineMarketplaceVolumeSpec {
    pub name: String,
    pub image: String,
    pub memory_limit_mb: u64,
    pub cpu_millicores: u32,
    pub workspace: elixir_marketplace::quarantine::QuarantineWorkspace,
    pub labels: HashMap<String, String>,
}

/// Untrusted cache metadata, never a byte-verification or execution receipt.
#[derive(Debug, PartialEq, Eq)]
pub enum SourceCacheState {
    Unsupported,
    Absent,
    Present(Option<String>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum VolumeMountSourceKind {
    #[default]
    Bind,
    NamedVolume,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortMapping {
    pub container_port: u16,
    pub host_port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_ip: Option<String>,
    #[serde(default)]
    pub protocol: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerHandle {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerState {
    pub id: String,
    pub name: String,
    pub status: String,
    pub running: bool,
    pub health: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContainerRuntimeMount {
    pub mount_type: String,
    pub source: Option<String>,
    pub name: Option<String>,
    pub destination: String,
    pub read_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContainerPublishedPort {
    pub container_port: u16,
    pub host_port: u16,
    pub protocol: String,
    pub host_ip: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContainerRuntimeState {
    pub name: String,
    pub network_mode: Option<String>,
    #[serde(default)]
    pub labels: HashMap<String, String>,
    #[serde(default)]
    pub mounts: Vec<ContainerRuntimeMount>,
    #[serde(default)]
    pub published_ports: Vec<ContainerPublishedPort>,
    #[serde(default)]
    pub security: ContainerRuntimeSecurityState,
}

/// Raw runtime observations, not a protection/ownership capability. The packet
/// controller validates these against its immutable desired profile.
#[derive(Debug, Clone)]
pub struct PrivateActorInspection {
    pub id: String,
    pub running: bool,
    pub runtime: ContainerRuntimeState,
    pub command: Vec<String>,
    pub entrypoint: Vec<String>,
    pub environment: Vec<String>,
    pub pid_mode: String,
    pub ipc_mode: String,
    pub userns_mode: String,
    pub device_count: usize,
    pub device_request_count: usize,
    pub bind_count: usize,
    pub sysctls: BTreeMap<String, String>,
    pub networks: Vec<String>,
    pub network_ids: BTreeMap<String, String>,
    pub endpoint_ids: BTreeMap<String, String>,
    pub restart_policy: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContainerRuntimeSecurityState {
    #[serde(default)]
    pub image_reference: Option<String>,
    #[serde(default)]
    pub image_id: Option<String>,
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub read_only_rootfs: bool,
    #[serde(default)]
    pub no_new_privileges: bool,
    #[serde(default)]
    pub cap_drop: Vec<String>,
    #[serde(default)]
    pub tmpfs: Vec<ContainerRuntimeTmpfsMount>,
    #[serde(default)]
    pub memory_limit_bytes: Option<i64>,
    #[serde(default)]
    pub pids_limit: Option<i64>,
    #[serde(default)]
    pub nano_cpus: Option<i64>,
    #[serde(default)]
    pub seccomp_profile: Option<String>,
    #[serde(default)]
    pub apparmor_profile: Option<String>,
    #[serde(default)]
    pub memory_swap_limit_bytes: Option<i64>,
    #[serde(default)]
    pub log_config: Option<ContainerRuntimeLogConfig>,
    #[serde(default)]
    pub shared_memory_bytes: Option<i64>,
    #[serde(default)]
    pub core_dump_limit: Option<ContainerRuntimeUlimit>,
    #[serde(default)]
    pub open_files_limit: Option<ContainerRuntimeUlimit>,
    #[serde(default)]
    pub file_size_limit: Option<ContainerRuntimeUlimit>,
    #[serde(default)]
    pub stdin_open: Option<bool>,
    #[serde(default)]
    pub tty: Option<bool>,
    #[serde(default)]
    pub privileged: Option<bool>,
    #[serde(default)]
    pub cap_add: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContainerRuntimeLogConfig {
    pub driver: String,
    pub options: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContainerRuntimeUlimit {
    pub soft: i64,
    pub hard: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContainerRuntimeTmpfsMount {
    pub path: String,
    #[serde(default)]
    pub options: Option<String>,
}

pub fn apply_container_spec_fingerprint(spec: &mut ContainerSpec) {
    let hash = container_spec_fingerprint(spec);
    spec.labels
        .insert(CONTAINER_SPEC_HASH_LABEL.to_string(), hash);
}

pub fn container_spec_fingerprint(spec: &ContainerSpec) -> String {
    let mut labels = BTreeMap::new();
    for (key, value) in &spec.labels {
        if key != CONTAINER_SPEC_HASH_LABEL {
            labels.insert(key.clone(), value.clone());
        }
    }

    let mut env = spec.env.clone();
    env.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.value.cmp(&right.value))
    });

    let mut volumes = spec.volumes.clone();
    volumes.sort_by(|left, right| {
        left.container_path
            .cmp(&right.container_path)
            .then_with(|| left.host_path.cmp(&right.host_path))
            .then_with(|| {
                format!("{:?}", left.source_kind).cmp(&format!("{:?}", right.source_kind))
            })
            .then_with(|| left.read_only.cmp(&right.read_only))
    });

    let mut ports = spec.ports.clone();
    ports.sort_by(|left, right| {
        left.container_port
            .cmp(&right.container_port)
            .then_with(|| left.host_port.cmp(&right.host_port))
            .then_with(|| left.host_ip.cmp(&right.host_ip))
            .then_with(|| left.protocol.cmp(&right.protocol))
    });

    let mut aliases = spec.aliases.clone();
    aliases.sort();
    let mut cap_add = spec.cap_add.clone();
    cap_add.sort();
    let mut cap_drop = spec.cap_drop.clone();
    cap_drop.sort();
    let mut devices = spec.devices.clone();
    devices.sort();
    let sysctls = spec
        .sysctls
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();

    let canonical = serde_json::json!({
        "name": &spec.name,
        "image": &spec.image,
        "network": &spec.network,
        "network_mode": &spec.network_mode,
        "aliases": aliases,
        "env": env,
        "volumes": volumes,
        "ports": ports,
        "labels": labels,
        "command": &spec.command,
        "cap_add": cap_add,
        "cap_drop": cap_drop,
        "devices": devices,
        "sysctls": sysctls,
        "security": &spec.security,
    });
    blake3::hash(canonical.to_string().as_bytes())
        .to_hex()
        .to_string()
}

#[cfg(test)]
mod serialization_tests {
    use super::*;
    #[test]
    fn absent_new_limits_preserve_legacy_security_fingerprint_shape() {
        assert_eq!(
            serde_json::to_value(ContainerSecurityOptions::default()).unwrap(),
            serde_json::json!({
                "user":null,"read_only_rootfs":false,"no_new_privileges":false,"tmpfs":[],"memory_limit_mb":null,
                "pids_limit":null,"cpu_quota":null,"seccomp_profile":null,"apparmor_profile":null
            })
        );
        assert_eq!(
            serde_json::to_value(ContainerTmpfsMount {
                path: "/tmp".to_owned(),
                size_mb: Some(64),
                ..Default::default()
            })
            .unwrap(),
            serde_json::json!({"path":"/tmp","size_mb":64})
        );
    }
}
