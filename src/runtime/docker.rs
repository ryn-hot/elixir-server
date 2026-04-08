use std::collections::{HashMap, HashSet};
use std::time::Duration;

#[cfg(target_os = "windows")]
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio::process::Command;
use tokio::time::{Instant, sleep};

use crate::runtime::RuntimeManager;
use crate::runtime::model::{
    ContainerHandle, ContainerSpec, ContainerState, PortMapping, VolumeMount,
};

const REQUIRED_LABELS: [&str; 2] = ["elixir.instance_id", "elixir.extension_id"];

pub struct DockerRuntimeManager {
    docker_bin: String,
}

#[derive(Debug, Clone)]
pub struct DockerStartupConfig {
    pub auto_start_runtime: bool,
    pub startup_timeout: Duration,
    pub startup_poll_interval: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DockerDaemonStatus {
    Ready,
    StartedByElixir,
}

impl DockerRuntimeManager {
    pub fn new(docker_bin: Option<String>) -> Self {
        Self {
            docker_bin: docker_bin.unwrap_or_else(|| "docker".to_string()),
        }
    }

    async fn run_capture(&self, args: &[String]) -> Result<CommandOutput> {
        let output = Command::new(&self.docker_bin)
            .args(args)
            .output()
            .await
            .with_context(|| format!("running docker {}", args.join(" ")))?;

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        if !output.status.success() {
            bail!(
                "docker {} failed (status {:?}): {}",
                args.join(" "),
                output.status.code(),
                stderr.trim()
            );
        }

        Ok(CommandOutput { stdout, stderr })
    }

    async fn run_stdout(&self, args: &[String]) -> Result<String> {
        Ok(self.run_capture(args).await?.stdout)
    }

    pub async fn server_version(&self) -> Result<String> {
        let args = vec![
            "version".to_string(),
            "--format".to_string(),
            "{{.Server.Version}}".to_string(),
        ];
        let stdout = self.run_stdout(&args).await?;
        let version = stdout.trim();
        if version.is_empty() {
            bail!("docker daemon returned an empty server version");
        }
        Ok(version.to_string())
    }

    pub async fn ensure_daemon_available(
        &self,
        config: &DockerStartupConfig,
    ) -> Result<DockerDaemonStatus> {
        match self.server_version().await {
            Ok(_) => return Ok(DockerDaemonStatus::Ready),
            Err(err) if !is_docker_daemon_unavailable(&err) => return Err(err),
            Err(initial_err) => {
                let mut started_by_elixir = false;
                let mut last_err = initial_err;

                if config.auto_start_runtime {
                    match self.start_docker_runtime().await {
                        Ok(()) => {
                            started_by_elixir = true;
                            tracing::info!("docker daemon unavailable; launched Docker runtime");
                        }
                        Err(err) => {
                            tracing::warn!(
                                "docker daemon unavailable and runtime auto-start failed: {}",
                                err
                            );
                        }
                    }
                }

                let deadline = Instant::now() + config.startup_timeout;
                while Instant::now() < deadline {
                    sleep(config.startup_poll_interval).await;
                    match self.server_version().await {
                        Ok(_) => {
                            return Ok(if started_by_elixir {
                                DockerDaemonStatus::StartedByElixir
                            } else {
                                DockerDaemonStatus::Ready
                            });
                        }
                        Err(err) if is_docker_daemon_unavailable(&err) => {
                            last_err = err;
                        }
                        Err(err) => return Err(err),
                    }
                }

                bail!(
                    "docker daemon unavailable after waiting {:?}: {}",
                    config.startup_timeout,
                    last_err
                );
            }
        }
    }

    async fn start_docker_runtime(&self) -> Result<()> {
        let attempts = docker_start_attempts();
        if attempts.is_empty() {
            bail!("no docker auto-start strategy is available for this platform");
        }

        let mut errors = Vec::new();
        for attempt in attempts {
            match self.run_start_attempt(&attempt).await {
                Ok(()) => return Ok(()),
                Err(err) => errors.push(format!("{}: {}", attempt.label, err)),
            }
        }

        bail!("docker auto-start failed: {}", errors.join(" | "))
    }

    async fn run_start_attempt(&self, attempt: &DockerStartAttempt) -> Result<()> {
        let output = Command::new(&attempt.program)
            .args(&attempt.args)
            .output()
            .await
            .with_context(|| format!("running {}", attempt.label))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            let detail = if !stderr.trim().is_empty() {
                stderr.trim()
            } else {
                stdout.trim()
            };
            bail!(
                "{} failed (status {:?}): {}",
                attempt.label,
                output.status.code(),
                detail
            );
        }
        Ok(())
    }

    async fn find_container_id(&self, name: &str) -> Result<Option<String>> {
        let args = vec![
            "ps".to_string(),
            "-a".to_string(),
            "--filter".to_string(),
            format!("name=^/{}$", name),
            "--format".to_string(),
            "{{.ID}}".to_string(),
        ];
        let stdout = self.run_stdout(&args).await?;
        let id = stdout.lines().next().map(|s| s.trim().to_string());
        Ok(id.filter(|s| !s.is_empty()))
    }

    async fn inspect_container(&self, name: &str) -> Result<Value> {
        let args = vec![
            "inspect".to_string(),
            "--format".to_string(),
            "{{json .}}".to_string(),
            name.to_string(),
        ];
        let stdout = self.run_stdout(&args).await?;
        serde_json::from_str(&stdout).context("parsing docker inspect output")
    }

    fn ensure_required_labels(labels: &HashMap<String, String>) -> Result<()> {
        for label in REQUIRED_LABELS {
            if !labels.contains_key(label) {
                bail!("container label '{}' is required", label);
            }
        }
        Ok(())
    }

    fn extract_state(value: &Value) -> Result<ContainerState> {
        let id = value
            .get("Id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let name = value
            .get("Name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim_start_matches('/')
            .to_string();
        let status = value
            .pointer("/State/Status")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let running = value
            .pointer("/State/Running")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let health = value
            .pointer("/State/Health/Status")
            .and_then(Value::as_str)
            .map(|s| s.to_string());

        Ok(ContainerState {
            id,
            name,
            status,
            running,
            health,
        })
    }

    fn extract_aliases(value: &Value, network: &str) -> Vec<String> {
        value
            .pointer(&format!("/NetworkSettings/Networks/{network}/Aliases"))
            .and_then(Value::as_array)
            .map(|aliases| {
                aliases
                    .iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    }

    fn has_network(value: &Value, network: &str) -> bool {
        value
            .pointer(&format!("/NetworkSettings/Networks/{network}"))
            .is_some()
    }

    fn extract_labels(value: &Value) -> HashMap<String, String> {
        value
            .pointer("/Config/Labels")
            .and_then(Value::as_object)
            .map(|labels| {
                labels
                    .iter()
                    .filter_map(|(key, value)| {
                        value.as_str().map(|value| (key.clone(), value.to_string()))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn aliases_conflict(spec: &ContainerSpec, value: &Value) -> bool {
        if spec.network_mode.is_some() || spec.aliases.is_empty() {
            return false;
        }
        if !Self::has_network(value, &spec.network) {
            return false;
        }
        let aliases = Self::extract_aliases(value, &spec.network);
        spec.aliases.iter().any(|alias| aliases.contains(alias))
    }

    async fn list_managed_container_names(&self) -> Result<Vec<String>> {
        let args = vec![
            "ps".to_string(),
            "-a".to_string(),
            "--filter".to_string(),
            "label=elixir.managed=true".to_string(),
            "--format".to_string(),
            "{{.Names}}".to_string(),
        ];
        let stdout = self.run_stdout(&args).await?;
        Ok(stdout
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(ToString::to_string)
            .collect())
    }

    async fn remove_conflicting_managed_containers(&self, spec: &ContainerSpec) -> Result<usize> {
        if spec.network_mode.is_some() || spec.aliases.is_empty() {
            return Ok(0);
        }

        let Some(expected_instance_id) = spec.labels.get("elixir.instance_id") else {
            return Ok(0);
        };

        let mut removed = 0usize;
        for name in self.list_managed_container_names().await? {
            if name == spec.name {
                continue;
            }

            let inspect = self.inspect_container(&name).await?;
            if !Self::aliases_conflict(spec, &inspect) {
                continue;
            }

            let labels = Self::extract_labels(&inspect);
            if labels.get("elixir.instance_id") == Some(expected_instance_id) {
                continue;
            }

            tracing::warn!(
                "docker runtime: removing stale/conflicting managed container '{}' before ensuring '{}'",
                name,
                spec.name
            );
            self.remove_container(&ContainerHandle {
                id: name.clone(),
                name,
            })
            .await?;
            removed += 1;
        }

        Ok(removed)
    }

    pub async fn prune_orphaned_managed_containers(
        &self,
        active_instance_ids: &HashSet<String>,
    ) -> Result<Vec<String>> {
        let mut removed = Vec::new();
        for name in self.list_managed_container_names().await? {
            let inspect = self.inspect_container(&name).await?;
            let labels = Self::extract_labels(&inspect);
            let instance_id = labels.get("elixir.instance_id");
            if instance_id.is_some_and(|value| active_instance_ids.contains(value)) {
                continue;
            }

            tracing::warn!(
                "docker runtime: removing orphaned managed container '{}' with missing/stale instance id {:?}",
                name,
                instance_id
            );
            self.remove_container(&ContainerHandle {
                id: name.clone(),
                name: name.clone(),
            })
            .await?;
            removed.push(name);
        }
        Ok(removed)
    }

    async fn create_container(&self, spec: &ContainerSpec) -> Result<ContainerHandle> {
        Self::ensure_required_labels(&spec.labels)?;

        let mut args = vec![
            "run".to_string(),
            "-d".to_string(),
            "--name".to_string(),
            spec.name.clone(),
        ];

        if let Some(network_mode) = spec.network_mode.as_ref() {
            args.push("--network".to_string());
            args.push(network_mode.clone());
        } else {
            args.push("--network".to_string());
            args.push(spec.network.clone());
        }

        if spec.network_mode.is_none() {
            for alias in &spec.aliases {
                args.push("--network-alias".to_string());
                args.push(alias.clone());
            }
        }

        for (key, value) in &spec.labels {
            args.push("--label".to_string());
            args.push(format!("{}={}", key, value));
        }

        if !spec.labels.contains_key("elixir.managed") {
            args.push("--label".to_string());
            args.push("elixir.managed=true".to_string());
        }

        for env in &spec.env {
            args.push("-e".to_string());
            args.push(format!("{}={}", env.name, env.value));
        }

        for capability in &spec.cap_add {
            args.push("--cap-add".to_string());
            args.push(capability.clone());
        }

        for device in &spec.devices {
            args.push("--device".to_string());
            args.push(device.clone());
        }

        for (key, value) in &spec.sysctls {
            args.push("--sysctl".to_string());
            args.push(format!("{key}={value}"));
        }

        for volume in &spec.volumes {
            args.push("-v".to_string());
            args.push(format_volume(volume));
        }

        for port in &spec.ports {
            if let Some(mapping) = format_port(port) {
                args.push("-p".to_string());
                args.push(mapping);
            }
        }

        args.push(spec.image.clone());
        if !spec.command.is_empty() {
            args.extend(spec.command.iter().cloned());
        }

        let stdout = self.run_stdout(&args).await?;
        let id = stdout.lines().next().unwrap_or_default().to_string();
        if id.trim().is_empty() {
            bail!("docker run did not return a container id");
        }

        Ok(ContainerHandle {
            id,
            name: spec.name.clone(),
        })
    }

    async fn ensure_container_attached(&self, spec: &ContainerSpec, value: &Value) -> Result<()> {
        if let Some(network_mode) = spec.network_mode.as_ref() {
            let current = value
                .pointer("/HostConfig/NetworkMode")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if current != network_mode {
                bail!(
                    "container '{}' has network mode '{}' but expected '{}'; recreate to fix",
                    spec.name,
                    current,
                    network_mode
                );
            }
            return Ok(());
        }

        if !Self::has_network(value, &spec.network) {
            bail!(
                "container '{}' is not attached to network '{}'",
                spec.name,
                spec.network
            );
        }

        let aliases = Self::extract_aliases(value, &spec.network);
        let missing: Vec<&String> = spec
            .aliases
            .iter()
            .filter(|alias| !aliases.contains(alias))
            .collect();
        if !missing.is_empty() {
            bail!(
                "container '{}' is missing network aliases {:?}; recreate to fix",
                spec.name,
                missing
            );
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl RuntimeManager for DockerRuntimeManager {
    async fn ensure_network(&self, name: &str) -> Result<()> {
        let inspect_args = vec![
            "network".to_string(),
            "inspect".to_string(),
            name.to_string(),
        ];
        if self.run_capture(&inspect_args).await.is_ok() {
            return Ok(());
        }

        let args = vec![
            "network".to_string(),
            "create".to_string(),
            "--driver".to_string(),
            "bridge".to_string(),
            "--label".to_string(),
            "elixir.managed=true".to_string(),
            "--label".to_string(),
            "elixir.network.schema_version=1".to_string(),
            name.to_string(),
        ];
        self.run_capture(&args).await?;
        Ok(())
    }

    async fn ensure_container(&self, spec: &ContainerSpec) -> Result<ContainerHandle> {
        if spec.name.trim().is_empty() {
            bail!("container name is required");
        }
        if spec.network_mode.is_none() && spec.network.trim().is_empty() {
            bail!("container network is required");
        }
        if spec.image.trim().is_empty() {
            bail!("container image is required");
        }

        if spec.network_mode.is_none() {
            self.ensure_network(&spec.network).await?;
        }

        self.remove_conflicting_managed_containers(spec).await?;

        if let Some(_) = self.find_container_id(&spec.name).await? {
            let inspect = self.inspect_container(&spec.name).await?;
            self.ensure_container_attached(spec, &inspect).await?;
            let state = Self::extract_state(&inspect)?;
            if !state.running {
                let args = vec!["start".to_string(), spec.name.clone()];
                self.run_capture(&args).await?;
            }
            return Ok(ContainerHandle {
                id: state.id,
                name: state.name,
            });
        }

        self.create_container(spec).await
    }

    async fn get_container_handle(&self, name: &str) -> Result<Option<ContainerHandle>> {
        if let Some(id) = self.find_container_id(name).await? {
            return Ok(Some(ContainerHandle {
                id,
                name: name.to_string(),
            }));
        }
        Ok(None)
    }

    async fn start_container(&self, handle: &ContainerHandle) -> Result<()> {
        let args = vec!["start".to_string(), handle.name.clone()];
        self.run_capture(&args).await?;
        Ok(())
    }

    async fn stop_container(&self, handle: &ContainerHandle) -> Result<()> {
        let args = vec!["stop".to_string(), handle.name.clone()];
        self.run_capture(&args).await?;
        Ok(())
    }

    async fn rename_container(
        &self,
        handle: &ContainerHandle,
        new_name: &str,
    ) -> Result<ContainerHandle> {
        let args = vec![
            "rename".to_string(),
            handle.name.clone(),
            new_name.to_string(),
        ];
        self.run_capture(&args).await?;
        Ok(ContainerHandle {
            id: handle.id.clone(),
            name: new_name.to_string(),
        })
    }

    async fn remove_container(&self, handle: &ContainerHandle) -> Result<()> {
        let args = vec!["rm".to_string(), "-f".to_string(), handle.name.clone()];
        self.run_capture(&args).await?;
        Ok(())
    }

    async fn container_logs(
        &self,
        handle: &ContainerHandle,
        since: Option<DateTime<Utc>>,
    ) -> Result<String> {
        let mut args = vec!["logs".to_string()];
        if let Some(since) = since {
            args.push("--since".to_string());
            args.push(since.to_rfc3339());
        }
        args.push(handle.name.clone());
        Ok(self.run_stdout(&args).await?)
    }

    async fn inspect(&self, handle: &ContainerHandle) -> Result<ContainerState> {
        let inspect = self.inspect_container(&handle.name).await?;
        Self::extract_state(&inspect)
    }
}

struct CommandOutput {
    stdout: String,
    stderr: String,
}

#[derive(Debug, Clone)]
struct DockerStartAttempt {
    program: String,
    args: Vec<String>,
    label: String,
}

impl DockerStartAttempt {
    fn new(program: impl Into<String>, args: Vec<String>, label: impl Into<String>) -> Self {
        Self {
            program: program.into(),
            args,
            label: label.into(),
        }
    }
}

fn is_docker_daemon_unavailable(err: &anyhow::Error) -> bool {
    let message = err.to_string().to_ascii_lowercase();
    message.contains("cannot connect to the docker daemon")
        || message.contains("is the docker daemon running")
        || message.contains("error during connect")
        || message.contains("docker daemon unavailable")
        || (message.contains("dial unix")
            && (message.contains("docker.sock") || message.contains("docker.raw.sock"))
            && (message.contains("connection refused")
                || message.contains("connect: no such file or directory")
                || message.contains("no such file or directory")))
}

fn docker_start_attempts() -> Vec<DockerStartAttempt> {
    #[cfg(target_os = "macos")]
    {
        return vec![DockerStartAttempt::new(
            "open",
            vec!["-a".to_string(), "Docker".to_string()],
            "open -a Docker",
        )];
    }

    #[cfg(target_os = "windows")]
    {
        let mut attempts = Vec::new();
        for base in windows_program_files_roots() {
            let desktop_path = base
                .join("Docker")
                .join("Docker")
                .join("Docker Desktop.exe");
            attempts.push(DockerStartAttempt::new(
                "cmd",
                vec![
                    "/C".to_string(),
                    "start".to_string(),
                    "".to_string(),
                    desktop_path.to_string_lossy().to_string(),
                ],
                format!("cmd /C start {}", desktop_path.display()),
            ));
        }
        attempts.push(DockerStartAttempt::new(
            "cmd",
            vec![
                "/C".to_string(),
                "start".to_string(),
                "".to_string(),
                "Docker Desktop".to_string(),
            ],
            "cmd /C start Docker Desktop",
        ));
        return attempts;
    }

    #[cfg(target_os = "linux")]
    {
        return vec![
            DockerStartAttempt::new(
                "systemctl",
                vec![
                    "--user".to_string(),
                    "start".to_string(),
                    "docker-desktop".to_string(),
                ],
                "systemctl --user start docker-desktop",
            ),
            DockerStartAttempt::new(
                "systemctl",
                vec![
                    "--user".to_string(),
                    "start".to_string(),
                    "docker".to_string(),
                ],
                "systemctl --user start docker",
            ),
            DockerStartAttempt::new(
                "systemctl",
                vec!["start".to_string(), "docker".to_string()],
                "systemctl start docker",
            ),
            DockerStartAttempt::new(
                "service",
                vec!["docker".to_string(), "start".to_string()],
                "service docker start",
            ),
        ];
    }

    #[allow(unreachable_code)]
    Vec::new()
}

#[cfg(target_os = "windows")]
fn windows_program_files_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    for key in ["ProgramFiles", "ProgramW6432", "ProgramFiles(x86)"] {
        if let Some(value) = std::env::var_os(key) {
            let path = PathBuf::from(value);
            if !roots.contains(&path) {
                roots.push(path);
            }
        }
    }
    roots
}

fn format_volume(volume: &VolumeMount) -> String {
    if volume.read_only {
        format!("{}:{}:ro", volume.host_path, volume.container_path)
    } else {
        format!("{}:{}", volume.host_path, volume.container_path)
    }
}

fn format_port(port: &PortMapping) -> Option<String> {
    let host = port.host_port?;
    let container = port.container_port;
    let proto = port
        .protocol
        .as_deref()
        .map(|p| format!("/{p}"))
        .unwrap_or_default();
    Some(format!("{}:{}{}", host, container, proto))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn port_mapping_formats() {
        let mapping = PortMapping {
            container_port: 8080,
            host_port: Some(0),
            protocol: Some("tcp".to_string()),
        };
        assert_eq!(format_port(&mapping).as_deref(), Some("0:8080/tcp"));
    }

    #[test]
    fn volume_mapping_formats() {
        let volume = VolumeMount {
            host_path: "/data".to_string(),
            container_path: "/config".to_string(),
            read_only: true,
        };
        assert_eq!(format_volume(&volume), "/data:/config:ro");
    }

    #[test]
    fn required_labels_enforced() {
        let mut labels = HashMap::new();
        labels.insert("elixir.instance_id".to_string(), "id".to_string());
        labels.insert("elixir.extension_id".to_string(), "ext".to_string());
        assert!(DockerRuntimeManager::ensure_required_labels(&labels).is_ok());
    }

    #[test]
    fn docker_daemon_unavailable_errors_are_detected() {
        let err = anyhow::anyhow!(
            "docker version --format {{.Server.Version}} failed: Cannot connect to the Docker daemon at unix:///var/run/docker.sock. Is the docker daemon running?"
        );
        assert!(is_docker_daemon_unavailable(&err));
    }

    #[test]
    fn docker_desktop_transitional_socket_errors_are_detected() {
        let refused = anyhow::anyhow!(
            "docker version --format {{.Server.Version}} failed (status Some(1)): Error response from daemon: dial unix docker.raw.sock: connect: connection refused"
        );
        assert!(is_docker_daemon_unavailable(&refused));

        let missing = anyhow::anyhow!(
            "docker info failed (status Some(1)): Error response from daemon: dial unix docker.raw.sock: connect: no such file or directory"
        );
        assert!(is_docker_daemon_unavailable(&missing));
    }

    #[test]
    fn unrelated_docker_errors_are_not_classified_as_daemon_unavailable() {
        let err = anyhow::anyhow!("docker inspect foo failed: No such container: foo");
        assert!(!is_docker_daemon_unavailable(&err));
    }

    #[test]
    fn alias_conflicts_are_detected_on_same_network() {
        let spec = ContainerSpec {
            name: "elx-new".to_string(),
            image: "example:latest".to_string(),
            network: "elixir_net".to_string(),
            network_mode: None,
            aliases: vec![
                "svc-elixir-modules-prowlarr-default".to_string(),
                "elx-prowlarr".to_string(),
            ],
            env: Vec::new(),
            volumes: Vec::new(),
            ports: Vec::new(),
            labels: HashMap::new(),
            command: Vec::new(),
            cap_add: Vec::new(),
            devices: Vec::new(),
            sysctls: HashMap::new(),
        };
        let inspect = json!({
            "NetworkSettings": {
                "Networks": {
                    "elixir_net": {
                        "Aliases": ["svc-elixir-modules-prowlarr-default", "deadbeef"]
                    }
                }
            }
        });
        assert!(DockerRuntimeManager::aliases_conflict(&spec, &inspect));
    }

    #[test]
    fn alias_conflicts_ignore_other_networks() {
        let spec = ContainerSpec {
            name: "elx-new".to_string(),
            image: "example:latest".to_string(),
            network: "elixir_net".to_string(),
            network_mode: None,
            aliases: vec!["svc-elixir-modules-prowlarr-default".to_string()],
            env: Vec::new(),
            volumes: Vec::new(),
            ports: Vec::new(),
            labels: HashMap::new(),
            command: Vec::new(),
            cap_add: Vec::new(),
            devices: Vec::new(),
            sysctls: HashMap::new(),
        };
        let inspect = json!({
            "NetworkSettings": {
                "Networks": {
                    "other_net": {
                        "Aliases": ["svc-elixir-modules-prowlarr-default"]
                    }
                }
            }
        });
        assert!(!DockerRuntimeManager::aliases_conflict(&spec, &inspect));
    }
}
