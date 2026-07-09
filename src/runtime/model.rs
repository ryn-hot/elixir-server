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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContainerTmpfsMount {
    pub path: String,
    #[serde(default)]
    pub size_mb: Option<u64>,
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

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContainerRuntimeSecurityState {
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
