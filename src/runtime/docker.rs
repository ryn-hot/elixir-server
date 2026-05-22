use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio::fs;
use tokio::process::Command;
use tokio::time::{Duration as TokioDuration, Instant, sleep, timeout};
use uuid::Uuid;

use crate::runtime::RuntimeManager;
use crate::runtime::model::{
    CONTAINER_SPEC_HASH_LABEL, ContainerHandle, ContainerPublishedPort, ContainerRuntimeMount,
    ContainerRuntimeState, ContainerSpec, ContainerState, PortMapping, VolumeMount,
    VolumeMountSourceKind,
};

const REQUIRED_LABELS: [&str; 2] = ["elixir.instance_id", "elixir.extension_id"];
const DOCKER_PROBE_COMMAND_TIMEOUT: Duration = Duration::from_secs(5);

pub struct DockerRuntimeManager {
    docker_bin: String,
}

#[derive(Debug, Clone, Default)]
pub struct DockerImageMetadata {
    pub repo_digests: Vec<String>,
    pub labels: HashMap<String, String>,
}

fn bind_mount_source_matches(desired_source: &str, actual_source: &str) -> bool {
    let desired = DockerRuntimeManager::normalized_bind_mount_sources(desired_source);
    let actual = DockerRuntimeManager::normalized_bind_mount_sources(actual_source);
    !desired.is_disjoint(&actual)
}

#[derive(Debug)]
struct ContainerMount {
    mount_type: String,
    source: Option<String>,
    name: Option<String>,
    destination: String,
    read_only: bool,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DockerRuntimeFailureKind {
    DaemonUnavailable,
    DesktopGuestSocketMissing,
    EngineKillStuck,
    EngineDeadlineExceeded,
    OciRuntimeFailure,
}

impl DockerRuntimeFailureKind {
    pub fn code(self) -> &'static str {
        match self {
            Self::DaemonUnavailable => "docker_runtime_unavailable",
            Self::DesktopGuestSocketMissing => "docker_desktop_guest_socket_missing",
            Self::EngineKillStuck => "docker_engine_kill_stuck",
            Self::EngineDeadlineExceeded => "docker_engine_deadline_exceeded",
            Self::OciRuntimeFailure => "docker_oci_runtime_failure",
        }
    }
}

pub fn describe_docker_runtime_failure(
    kind: DockerRuntimeFailureKind,
    err: &anyhow::Error,
) -> String {
    match kind {
        DockerRuntimeFailureKind::DaemonUnavailable => format!(
            "Docker daemon is unavailable from Elixir. Docker may still be starting, stopped, or unreachable from the current host session. {}",
            err
        ),
        DockerRuntimeFailureKind::DesktopGuestSocketMissing => format!(
            "Docker Desktop is missing required guest sockets, which usually means its VM is only partially started or internally unhealthy. {}",
            err
        ),
        DockerRuntimeFailureKind::EngineKillStuck => format!(
            "Docker could not stop or remove an Elixir-managed container because the engine never delivered an exit event. {}",
            err
        ),
        DockerRuntimeFailureKind::EngineDeadlineExceeded => format!(
            "Docker timed out while handling a managed container lifecycle operation. {}",
            err
        ),
        DockerRuntimeFailureKind::OciRuntimeFailure => format!(
            "Docker reported an OCI runtime failure while starting or replacing a managed container. {}",
            err
        ),
    }
}

impl DockerRuntimeManager {
    pub fn new(docker_bin: Option<String>) -> Self {
        Self {
            docker_bin: docker_bin.unwrap_or_else(|| "docker".to_string()),
        }
    }

    async fn run_capture(&self, args: &[String]) -> Result<CommandOutput> {
        self.run_capture_with_timeout(args, None).await
    }

    async fn run_capture_with_timeout(
        &self,
        args: &[String],
        timeout_duration: Option<TokioDuration>,
    ) -> Result<CommandOutput> {
        let mut command = Command::new(&self.docker_bin);
        command.args(args).kill_on_drop(true);
        let command_label = format!("docker {}", args.join(" "));
        let output_future = command.output();
        let output = match timeout_duration {
            Some(duration) => timeout(duration, output_future)
                .await
                .with_context(|| format!("{command_label} timed out after {duration:?}"))?
                .with_context(|| format!("running {command_label}"))?,
            None => output_future
                .await
                .with_context(|| format!("running {command_label}"))?,
        };

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

    async fn run_stdout_with_timeout(
        &self,
        args: &[String],
        timeout_duration: TokioDuration,
    ) -> Result<String> {
        Ok(self
            .run_capture_with_timeout(args, Some(timeout_duration))
            .await?
            .stdout)
    }

    pub async fn server_version(&self) -> Result<String> {
        let args = vec![
            "version".to_string(),
            "--format".to_string(),
            "{{.Server.Version}}".to_string(),
        ];
        let stdout = self
            .run_stdout_with_timeout(&args, DOCKER_PROBE_COMMAND_TIMEOUT)
            .await?;
        let version = stdout.trim();
        if version.is_empty() {
            bail!("docker daemon returned an empty server version");
        }
        Ok(version.to_string())
    }

    pub async fn pull_image(&self, image: &str) -> Result<()> {
        self.run_capture(&["pull".to_string(), image.to_string()])
            .await?;
        Ok(())
    }

    pub async fn inspect_image_metadata(&self, image: &str) -> Result<DockerImageMetadata> {
        let stdout = self
            .run_stdout(&[
                "image".to_string(),
                "inspect".to_string(),
                image.to_string(),
            ])
            .await?;
        let values: Vec<Value> =
            serde_json::from_str(&stdout).context("parsing docker image inspect output")?;
        let value = values
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("docker image inspect returned no entries"))?;
        let repo_digests = value
            .get("RepoDigests")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let labels = value
            .get("Config")
            .and_then(|config| config.get("Labels"))
            .and_then(Value::as_object)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|(key, value)| {
                        value.as_str().map(|value| (key.clone(), value.to_string()))
                    })
                    .collect::<HashMap<_, _>>()
            })
            .unwrap_or_default();
        Ok(DockerImageMetadata {
            repo_digests,
            labels,
        })
    }

    pub async fn copy_named_volume_path_to_host(
        &self,
        helper_image: &str,
        volume_name: &str,
        volume_path: &str,
        destination_path: &Path,
    ) -> Result<()> {
        fs::create_dir_all(destination_path)
            .await
            .with_context(|| {
                format!(
                    "creating backup destination directory {}",
                    destination_path.display()
                )
            })?;
        let helper = self
            .create_named_volume_helper(helper_image, volume_name, volume_path)
            .await?;
        let result = self
            .copy_container_path_to_host(&helper, volume_path, destination_path)
            .await;
        let cleanup = self.remove_container(&helper).await;
        result?;
        cleanup?;
        Ok(())
    }

    pub async fn replace_named_volume_path_from_host(
        &self,
        helper_image: &str,
        volume_name: &str,
        volume_path: &str,
        source_path: &Path,
    ) -> Result<()> {
        let helper = self
            .create_named_volume_helper(helper_image, volume_name, volume_path)
            .await?;
        let result = async {
            self.clear_helper_path(&helper, volume_path).await?;
            self.copy_host_path_to_container(&helper, source_path, volume_path)
                .await
        }
        .await;
        let cleanup = self.remove_container(&helper).await;
        result?;
        cleanup?;
        Ok(())
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

    async fn create_named_volume_helper(
        &self,
        helper_image: &str,
        volume_name: &str,
        volume_path: &str,
    ) -> Result<ContainerHandle> {
        let helper_name = format!("elixir-volhelper-{}", Uuid::new_v4().simple());
        let args = vec![
            "run".to_string(),
            "-d".to_string(),
            "--rm".to_string(),
            "--name".to_string(),
            helper_name.clone(),
            "-v".to_string(),
            format!("{volume_name}:{volume_path}"),
            helper_image.to_string(),
            "sh".to_string(),
            "-lc".to_string(),
            "trap 'exit 0' TERM INT; while true; do sleep 3600; done".to_string(),
        ];
        let stdout = self.run_stdout(&args).await?;
        let id = stdout.lines().next().unwrap_or_default().trim().to_string();
        if id.is_empty() {
            bail!("docker run did not return a helper container id");
        }
        Ok(ContainerHandle {
            id,
            name: helper_name,
        })
    }

    async fn copy_container_path_to_host(
        &self,
        handle: &ContainerHandle,
        source_path: &str,
        destination_path: &Path,
    ) -> Result<()> {
        let args = vec![
            "cp".to_string(),
            format!("{}:{}/.", handle.name, source_path.trim_end_matches('/')),
            destination_path.to_string_lossy().to_string(),
        ];
        self.run_capture(&args).await?;
        Ok(())
    }

    async fn clear_helper_path(&self, handle: &ContainerHandle, path: &str) -> Result<()> {
        let args = vec![
            "exec".to_string(),
            handle.name.clone(),
            "sh".to_string(),
            "-lc".to_string(),
            "set -e; target=\"$1\"; mkdir -p \"$target\"; find \"$target\" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +".to_string(),
            "elixir-volume-helper".to_string(),
            path.to_string(),
        ];
        self.run_capture(&args).await?;
        Ok(())
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

    async fn container_top_output(&self, name: &str) -> Result<String> {
        let args = vec!["top".to_string(), name.to_string()];
        self.run_stdout(&args).await
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

    fn extract_mounts(value: &Value) -> Vec<ContainerMount> {
        value
            .get("Mounts")
            .and_then(Value::as_array)
            .map(|mounts| {
                mounts
                    .iter()
                    .filter_map(|mount| {
                        let destination = mount
                            .get("Destination")
                            .and_then(Value::as_str)?
                            .to_string();
                        let mount_type = mount
                            .get("Type")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string();
                        let source = mount
                            .get("Source")
                            .and_then(Value::as_str)
                            .map(ToString::to_string);
                        let name = mount
                            .get("Name")
                            .and_then(Value::as_str)
                            .map(ToString::to_string);
                        let read_only = mount
                            .get("RW")
                            .and_then(Value::as_bool)
                            .map(|rw| !rw)
                            .unwrap_or(false);
                        Some(ContainerMount {
                            mount_type,
                            source,
                            name,
                            destination,
                            read_only,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn extract_published_ports(value: &Value) -> Vec<ContainerPublishedPort> {
        let mut published = value
            .pointer("/NetworkSettings/Ports")
            .and_then(Value::as_object)
            .map(|ports| {
                ports
                    .iter()
                    .flat_map(|(key, bindings)| {
                        let Some((container_port, protocol)) = key.split_once('/') else {
                            return Vec::new();
                        };
                        let Ok(container_port) = container_port.parse::<u16>() else {
                            return Vec::new();
                        };
                        bindings
                            .as_array()
                            .into_iter()
                            .flatten()
                            .filter_map(move |binding| {
                                let host_port = binding
                                    .get("HostPort")
                                    .and_then(Value::as_str)
                                    .unwrap_or_default()
                                    .trim()
                                    .parse::<u16>()
                                    .ok()
                                    .filter(|port| *port > 0)?;
                                let host_ip = binding
                                    .get("HostIp")
                                    .and_then(Value::as_str)
                                    .map(str::trim)
                                    .filter(|value| !value.is_empty())
                                    .map(str::to_string);
                                Some(ContainerPublishedPort {
                                    container_port,
                                    host_port,
                                    protocol: protocol.to_string(),
                                    host_ip,
                                })
                            })
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        published.sort_by(|left, right| {
            left.container_port
                .cmp(&right.container_port)
                .then_with(|| left.protocol.cmp(&right.protocol))
                .then_with(|| left.host_port.cmp(&right.host_port))
        });
        published
    }

    fn extract_runtime_state(value: &Value) -> ContainerRuntimeState {
        let name = value
            .get("Name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim_start_matches('/')
            .to_string();
        let network_mode = value
            .pointer("/HostConfig/NetworkMode")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        let mounts = Self::extract_mounts(value)
            .into_iter()
            .map(|mount| ContainerRuntimeMount {
                mount_type: mount.mount_type,
                source: mount.source,
                name: mount.name,
                destination: mount.destination,
                read_only: mount.read_only,
            })
            .collect();

        ContainerRuntimeState {
            name,
            network_mode,
            labels: Self::extract_labels(value),
            mounts,
            published_ports: Self::extract_published_ports(value),
        }
    }

    fn mount_matches(desired: &VolumeMount, actual: &ContainerMount) -> bool {
        if actual.destination != desired.container_path || actual.read_only != desired.read_only {
            return false;
        }

        match desired.source_kind {
            VolumeMountSourceKind::Bind => {
                actual.mount_type == "bind"
                    && actual.source.as_deref().is_some_and(|actual_source| {
                        bind_mount_source_matches(desired.host_path.as_str(), actual_source)
                    })
            }
            VolumeMountSourceKind::NamedVolume => {
                actual.mount_type == "volume"
                    && actual.name.as_deref() == Some(desired.host_path.as_str())
            }
        }
    }

    fn normalized_bind_mount_sources(path: &str) -> HashSet<String> {
        let mut candidates = HashSet::new();
        let trimmed = path.trim();
        if trimmed.is_empty() {
            return candidates;
        }

        candidates.insert(trimmed.to_string());
        let slash_normalized = trimmed.replace('\\', "/");
        candidates.insert(slash_normalized.clone());

        if let Some(stripped) = trimmed.strip_prefix("/host_mnt") {
            let normalized = if stripped.is_empty() { "/" } else { stripped };
            candidates.insert(normalized.to_string());
        }
        if let Some(stripped) = slash_normalized.strip_prefix("/host_mnt") {
            let normalized = if stripped.is_empty() { "/" } else { stripped };
            candidates.insert(normalized.to_string());
        }
        add_windows_docker_desktop_bind_mount_candidates(&mut candidates, &slash_normalized);

        if let Some(stripped) = trimmed.strip_prefix("/private") {
            let normalized = if stripped.is_empty() { "/" } else { stripped };
            candidates.insert(normalized.to_string());
        }

        if !trimmed.starts_with("/private/")
            && (trimmed == "/tmp"
                || trimmed.starts_with("/tmp/")
                || trimmed == "/var"
                || trimmed.starts_with("/var/"))
        {
            candidates.insert(format!("/private{trimmed}"));
        }

        let current: Vec<String> = candidates.iter().cloned().collect();
        for candidate in current {
            if let Ok(canonical) = std::fs::canonicalize(&candidate) {
                candidates.insert(canonical.to_string_lossy().to_string());
            }
        }

        candidates
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

    fn top_output_has_defunct_processes(output: &str) -> bool {
        output
            .lines()
            .skip(1)
            .map(str::trim)
            .any(|line| !line.is_empty() && line.to_ascii_lowercase().contains("<defunct>"))
    }

    fn container_file_missing_error(err: &anyhow::Error) -> bool {
        let lower = err.to_string().to_ascii_lowercase();
        lower.contains("could not find the file")
            || lower.contains("no such file or directory")
            || lower.contains("file does not exist")
    }

    fn temp_copy_path(path: &str) -> PathBuf {
        let filename = Path::new(path)
            .file_name()
            .and_then(|value| value.to_str())
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("copied");
        std::env::temp_dir()
            .join(format!("elixir-docker-copy-{}", Uuid::new_v4()))
            .join(filename)
    }

    pub async fn list_managed_container_names(&self) -> Result<Vec<String>> {
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
            .collect::<Vec<_>>())
    }

    pub async fn lookup_published_host_port(
        &self,
        container_name: &str,
        container_port: u16,
    ) -> Result<Option<u16>> {
        let args = vec![
            "inspect".to_string(),
            "--format".to_string(),
            "{{json .NetworkSettings.Ports}}".to_string(),
            container_name.to_string(),
        ];
        let stdout = self.run_stdout(&args).await?;
        Ok(parse_published_host_port(&stdout, container_port))
    }

    pub async fn describe_container_runtime_state(
        &self,
        container_name: &str,
    ) -> Result<Option<ContainerRuntimeState>> {
        if self.find_container_id(container_name).await?.is_none() {
            return Ok(None);
        }

        let inspect = self.inspect_container(container_name).await?;
        Ok(Some(Self::extract_runtime_state(&inspect)))
    }

    pub async fn stop_and_remove_managed_containers(&self) -> Result<Vec<String>> {
        let mut names = self.list_managed_container_names().await?;
        names.sort();

        let mut removed = Vec::new();
        for name in names {
            let handle = ContainerHandle {
                id: name.clone(),
                name: name.clone(),
            };
            let stop_result = self.stop_container(&handle).await;
            let existing = self.get_container_handle(&name).await?;
            match existing {
                Some(existing) => {
                    self.remove_container(&existing).await?;
                    removed.push(name);
                }
                None => {
                    if stop_result.is_ok() {
                        removed.push(name);
                    }
                }
            }
            if let Err(err) = stop_result {
                tracing::debug!(
                    "docker runtime: managed container '{}' did not stop cleanly before removal: {}",
                    handle.name,
                    err
                );
            }
        }

        Ok(removed)
    }

    pub async fn recreate_managed_network(&self, name: &str) -> Result<bool> {
        let inspect_args = vec![
            "network".to_string(),
            "inspect".to_string(),
            name.to_string(),
        ];
        let existed = self.run_capture(&inspect_args).await.is_ok();
        let mut recreated = false;
        if existed {
            let remove_args = vec!["network".to_string(), "rm".to_string(), name.to_string()];
            match self.run_capture(&remove_args).await {
                Ok(_) => {
                    recreated = true;
                }
                Err(err) if network_has_attached_endpoints_error(&err) => {
                    tracing::warn!(
                        "docker runtime: leaving managed network '{}' in place because Docker still reports attached endpoints: {}",
                        name,
                        err
                    );
                }
                Err(err) => return Err(err),
            }
        }
        self.ensure_network(name).await?;
        Ok(recreated)
    }

    pub async fn restart_docker_runtime(
        &self,
        config: &DockerStartupConfig,
    ) -> Result<DockerDaemonStatus> {
        #[cfg(target_os = "macos")]
        {
            let _ = self
                .run_start_attempt(&DockerStartAttempt::new(
                    "osascript",
                    vec![
                        "-e".to_string(),
                        "tell application \"Docker\" to quit".to_string(),
                    ],
                    "osascript quit Docker",
                ))
                .await;
            sleep(Duration::from_secs(3)).await;
            self.start_docker_runtime().await?;
            let wait_config = DockerStartupConfig {
                auto_start_runtime: false,
                startup_timeout: config.startup_timeout,
                startup_poll_interval: config.startup_poll_interval,
            };
            return self.ensure_daemon_available(&wait_config).await;
        }

        #[cfg(target_os = "windows")]
        {
            let _ = self
                .run_start_attempt(&DockerStartAttempt::new(
                    "cmd",
                    vec![
                        "/C".to_string(),
                        "taskkill".to_string(),
                        "/IM".to_string(),
                        "Docker Desktop.exe".to_string(),
                        "/F".to_string(),
                    ],
                    "taskkill Docker Desktop",
                ))
                .await;
            let _ = self
                .run_start_attempt(&DockerStartAttempt::new(
                    "wsl",
                    vec!["--shutdown".to_string()],
                    "wsl --shutdown",
                ))
                .await;
            sleep(Duration::from_secs(2)).await;
            self.start_docker_runtime().await?;
            let wait_config = DockerStartupConfig {
                auto_start_runtime: false,
                startup_timeout: config.startup_timeout,
                startup_poll_interval: config.startup_poll_interval,
            };
            return self.ensure_daemon_available(&wait_config).await;
        }

        #[cfg(target_os = "linux")]
        {
            let attempts = vec![
                DockerStartAttempt::new(
                    "systemctl",
                    vec![
                        "--user".to_string(),
                        "restart".to_string(),
                        "docker-desktop".to_string(),
                    ],
                    "systemctl --user restart docker-desktop",
                ),
                DockerStartAttempt::new(
                    "systemctl",
                    vec![
                        "--user".to_string(),
                        "restart".to_string(),
                        "docker".to_string(),
                    ],
                    "systemctl --user restart docker",
                ),
                DockerStartAttempt::new(
                    "systemctl",
                    vec!["restart".to_string(), "docker".to_string()],
                    "systemctl restart docker",
                ),
                DockerStartAttempt::new(
                    "service",
                    vec!["docker".to_string(), "restart".to_string()],
                    "service docker restart",
                ),
            ];

            let mut errors = Vec::new();
            for attempt in attempts {
                match self.run_start_attempt(&attempt).await {
                    Ok(()) => {
                        let wait_config = DockerStartupConfig {
                            auto_start_runtime: false,
                            startup_timeout: config.startup_timeout,
                            startup_poll_interval: config.startup_poll_interval,
                        };
                        return self.ensure_daemon_available(&wait_config).await;
                    }
                    Err(err) => errors.push(format!("{}: {}", attempt.label, err)),
                }
            }

            bail!("docker runtime restart failed: {}", errors.join(" | "));
        }

        #[cfg(not(any(target_os = "macos", target_os = "windows", target_os = "linux")))]
        bail!("docker runtime restart is not implemented for this platform")
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
        if let Some((actual, desired)) = Self::container_spec_hash_mismatch(spec, value) {
            bail!(
                "container '{}' has spec fingerprint '{}' but expected '{}'; recreate to fix",
                spec.name,
                actual,
                desired
            );
        }

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

        let mounts = Self::extract_mounts(value);
        for desired in &spec.volumes {
            let Some(actual) = mounts
                .iter()
                .find(|mount| mount.destination == desired.container_path)
            else {
                bail!(
                    "container '{}' is missing volume mount '{}'; recreate to fix",
                    spec.name,
                    desired.container_path
                );
            };
            if !Self::mount_matches(desired, actual) {
                bail!(
                    "container '{}' has mismatched volume mount '{}' (actual type='{}' source='{}' name='{}'); recreate to fix",
                    spec.name,
                    desired.container_path,
                    actual.mount_type,
                    actual.source.as_deref().unwrap_or_default(),
                    actual.name.as_deref().unwrap_or_default()
                );
            }
        }

        Ok(())
    }

    fn container_spec_hash_mismatch(
        spec: &ContainerSpec,
        value: &Value,
    ) -> Option<(String, String)> {
        let desired = spec.labels.get(CONTAINER_SPEC_HASH_LABEL)?;
        let labels = Self::extract_labels(value);
        let actual = labels.get(CONTAINER_SPEC_HASH_LABEL)?;
        (actual != desired).then(|| (actual.clone(), desired.clone()))
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
            if let Err(err) = self.ensure_container_attached(spec, &inspect).await {
                let state = Self::extract_state(&inspect)?;
                tracing::warn!(
                    "docker runtime: container '{}' no longer matches desired runtime spec; removing and recreating it: {}",
                    spec.name,
                    err
                );
                self.remove_container(&ContainerHandle {
                    id: state.id,
                    name: spec.name.clone(),
                })
                .await?;
                return self.create_container(spec).await;
            }
            let state = Self::extract_state(&inspect)?;
            if state.running {
                match self.container_top_output(&spec.name).await {
                    Ok(output) if Self::top_output_has_defunct_processes(&output) => {
                        tracing::warn!(
                            "docker runtime: container '{}' has defunct processes; removing and recreating it",
                            spec.name
                        );
                        self.remove_container(&ContainerHandle {
                            id: state.id.clone(),
                            name: spec.name.clone(),
                        })
                        .await?;
                        return self.create_container(spec).await;
                    }
                    Ok(_) => {}
                    Err(err) => {
                        tracing::debug!(
                            "docker runtime: unable to inspect process list for '{}': {}",
                            spec.name,
                            err
                        );
                    }
                }
            } else {
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

    async fn describe_container_runtime_state(
        &self,
        container_name: &str,
    ) -> Result<Option<ContainerRuntimeState>> {
        DockerRuntimeManager::describe_container_runtime_state(self, container_name).await
    }

    async fn read_container_file(
        &self,
        handle: &ContainerHandle,
        path: &str,
    ) -> Result<Option<Vec<u8>>> {
        let temp_path = Self::temp_copy_path(path);
        if let Some(parent) = temp_path.parent() {
            fs::create_dir_all(parent).await.with_context(|| {
                format!("creating temp container copy dir {}", parent.display())
            })?;
        }

        let args = vec![
            "cp".to_string(),
            format!("{}:{}", handle.name, path),
            temp_path.to_string_lossy().to_string(),
        ];
        let copy_result = self.run_capture(&args).await;
        match copy_result {
            Ok(_) => {
                let bytes = fs::read(&temp_path).await.with_context(|| {
                    format!("reading copied container file {}", temp_path.display())
                })?;
                let _ = fs::remove_dir_all(
                    temp_path
                        .parent()
                        .map(Path::to_path_buf)
                        .unwrap_or_else(std::env::temp_dir),
                )
                .await;
                Ok(Some(bytes))
            }
            Err(err) if Self::container_file_missing_error(&err) => {
                if let Some(parent) = temp_path.parent() {
                    let _ = fs::remove_dir_all(parent).await;
                }
                Ok(None)
            }
            Err(err) => {
                if let Some(parent) = temp_path.parent() {
                    let _ = fs::remove_dir_all(parent).await;
                }
                Err(err)
            }
        }
    }

    async fn copy_host_path_to_container(
        &self,
        handle: &ContainerHandle,
        source_path: &Path,
        destination_path: &str,
    ) -> Result<()> {
        let source = if source_path.is_dir() {
            format!("{}/.", source_path.to_string_lossy())
        } else {
            source_path.to_string_lossy().to_string()
        };
        let args = vec![
            "cp".to_string(),
            source,
            format!("{}:{}", handle.name, destination_path),
        ];
        self.run_capture(&args).await?;
        Ok(())
    }

    async fn ensure_container_directories(
        &self,
        handle: &ContainerHandle,
        paths: &[String],
    ) -> Result<()> {
        if paths.is_empty() {
            return Ok(());
        }
        let mut args = vec![
            "exec".to_string(),
            handle.name.clone(),
            "mkdir".to_string(),
            "-p".to_string(),
        ];
        args.extend(paths.iter().cloned());
        self.run_capture(&args).await?;
        Ok(())
    }

    async fn ensure_container_directories_owned_like(
        &self,
        handle: &ContainerHandle,
        reference_path: &str,
        paths: &[String],
    ) -> Result<bool> {
        if paths.is_empty() {
            return Ok(false);
        }

        let mut args = vec![
            "exec".to_string(),
            handle.name.clone(),
            "sh".to_string(),
            "-lc".to_string(),
            "set -e; owner=\"$(stat -c '%u:%g' \"$1\")\"; shift; changed=0; for path in \"$@\"; do mkdir -p \"$path\"; current=\"$(stat -c '%u:%g' \"$path\")\"; if [ \"$current\" != \"$owner\" ]; then chown -R \"$owner\" \"$path\"; changed=1; fi; done; printf '%s' \"$changed\"".to_string(),
            "elixir-runtime-init".to_string(),
            reference_path.to_string(),
        ];
        args.extend(paths.iter().cloned());
        let output = self.run_capture(&args).await?;
        Ok(output.stdout.trim() == "1")
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
        || (message.contains("docker version") && message.contains("timed out"))
        || (message.contains("dial unix")
            && (message.contains("docker.sock") || message.contains("docker.raw.sock"))
            && (message.contains("connection refused")
                || message.contains("connect: no such file or directory")
                || message.contains("no such file or directory")))
}

fn network_has_attached_endpoints_error(err: &anyhow::Error) -> bool {
    let message = err.to_string().to_ascii_lowercase();
    message.contains("active endpoints")
        || message.contains("has endpoints")
        || (message.contains("error while removing network") && message.contains("endpoint"))
}

pub fn classify_docker_runtime_failure(err: &anyhow::Error) -> Option<DockerRuntimeFailureKind> {
    let message = err.to_string().to_ascii_lowercase();
    if message.contains("docker.raw.sock")
        || message.contains("lifecycle-server.sock")
        || message.contains("diagnosticd.sock")
        || message.contains("dns-forwarder.sock")
    {
        return Some(DockerRuntimeFailureKind::DesktopGuestSocketMissing);
    }
    if message.contains("did not receive an exit event")
        || message.contains("tried to kill container")
        || message.contains("could not kill running container")
    {
        return Some(DockerRuntimeFailureKind::EngineKillStuck);
    }
    if message.contains("context deadline exceeded") {
        return Some(DockerRuntimeFailureKind::EngineDeadlineExceeded);
    }
    if message.contains("oci runtime")
        || message.contains("setns")
        || message.contains("failed to create shim task")
    {
        return Some(DockerRuntimeFailureKind::OciRuntimeFailure);
    }
    if is_docker_daemon_unavailable(err) {
        return Some(DockerRuntimeFailureKind::DaemonUnavailable);
    }
    None
}

fn parse_published_host_port(ports_json: &str, container_port: u16) -> Option<u16> {
    let value: Value = serde_json::from_str(ports_json).ok()?;
    let key = format!("{container_port}/tcp");
    let bindings = value.get(&key)?.as_array()?;
    let binding = bindings.first()?;
    binding
        .get("HostPort")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .parse::<u16>()
        .ok()
        .filter(|port| *port > 0)
}

fn add_windows_docker_desktop_bind_mount_candidates(
    candidates: &mut HashSet<String>,
    slash_path: &str,
) {
    if let Some((drive, rest)) = windows_drive_path_parts(slash_path) {
        insert_windows_bind_mount_candidates(candidates, drive, rest);
    }
    if let Some((drive, rest)) = docker_desktop_linux_windows_path_parts(slash_path) {
        insert_windows_bind_mount_candidates(candidates, drive, rest);
    }
}

fn windows_drive_path_parts(path: &str) -> Option<(char, &str)> {
    let mut chars = path.chars();
    let drive = chars.next()?;
    if !drive.is_ascii_alphabetic() || chars.next()? != ':' {
        return None;
    }
    let rest = chars.as_str().trim_start_matches('/');
    Some((drive.to_ascii_lowercase(), rest))
}

fn docker_desktop_linux_windows_path_parts(path: &str) -> Option<(char, &str)> {
    for prefix in ["/host_mnt/", "/run/desktop/mnt/host/", "/"] {
        let Some(remainder) = path.strip_prefix(prefix) else {
            continue;
        };
        let mut chars = remainder.chars();
        let drive = chars.next()?;
        if !drive.is_ascii_alphabetic() {
            continue;
        }
        let rest = chars.as_str();
        if !rest.is_empty() && !rest.starts_with('/') {
            continue;
        }
        return Some((drive.to_ascii_lowercase(), rest.trim_start_matches('/')));
    }
    None
}

fn insert_windows_bind_mount_candidates(candidates: &mut HashSet<String>, drive: char, rest: &str) {
    let drive_lower = drive.to_ascii_lowercase();
    let drive_upper = drive.to_ascii_uppercase();
    let rest = rest.trim_start_matches('/');
    let rest_slashes = rest.replace('\\', "/");
    let rest_backslashes = rest_slashes.replace('/', "\\");

    let suffix = if rest_slashes.is_empty() {
        String::new()
    } else {
        format!("/{rest_slashes}")
    };
    candidates.insert(format!("{drive_upper}:{suffix}"));
    candidates.insert(format!("{drive_lower}:{suffix}"));
    if rest_backslashes.is_empty() {
        candidates.insert(format!("{drive_upper}:\\"));
        candidates.insert(format!("{drive_lower}:\\"));
    } else {
        candidates.insert(format!("{drive_upper}:\\{rest_backslashes}"));
        candidates.insert(format!("{drive_lower}:\\{rest_backslashes}"));
    }
    candidates.insert(format!("/{drive_lower}{suffix}"));
    candidates.insert(format!("/host_mnt/{drive_lower}{suffix}"));
    candidates.insert(format!("/run/desktop/mnt/host/{drive_lower}{suffix}"));
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
            source_kind: VolumeMountSourceKind::Bind,
            host_path: "/data".to_string(),
            container_path: "/config".to_string(),
            read_only: true,
        };
        assert_eq!(format_volume(&volume), "/data:/config:ro");
    }

    #[test]
    fn named_volume_mapping_formats() {
        let volume = VolumeMount {
            source_kind: VolumeMountSourceKind::NamedVolume,
            host_path: "elixir_cfg_test".to_string(),
            container_path: "/config".to_string(),
            read_only: false,
        };
        assert_eq!(format_volume(&volume), "elixir_cfg_test:/config");
    }

    #[test]
    fn named_volume_mount_matching_uses_volume_name() {
        let desired = VolumeMount {
            source_kind: VolumeMountSourceKind::NamedVolume,
            host_path: "elixir_cfg_test".to_string(),
            container_path: "/config".to_string(),
            read_only: false,
        };
        let actual = ContainerMount {
            mount_type: "volume".to_string(),
            source: Some("/var/lib/docker/volumes/elixir_cfg_test/_data".to_string()),
            name: Some("elixir_cfg_test".to_string()),
            destination: "/config".to_string(),
            read_only: false,
        };
        assert!(DockerRuntimeManager::mount_matches(&desired, &actual));
    }

    #[test]
    fn bind_mount_matching_normalizes_docker_desktop_host_mnt_prefix() {
        let desired = VolumeMount {
            source_kind: VolumeMountSourceKind::Bind,
            host_path: "/Users/tester/elixir/media/tv".to_string(),
            container_path: "/tv".to_string(),
            read_only: false,
        };
        let actual = ContainerMount {
            mount_type: "bind".to_string(),
            source: Some("/host_mnt/Users/tester/elixir/media/tv".to_string()),
            name: None,
            destination: "/tv".to_string(),
            read_only: false,
        };
        assert!(DockerRuntimeManager::mount_matches(&desired, &actual));
    }

    #[test]
    fn bind_mount_matching_normalizes_windows_drive_to_docker_desktop_prefixes() {
        let desired = VolumeMount {
            source_kind: VolumeMountSourceKind::Bind,
            host_path: r"C:\Users\tester\elixir\data\downloads".to_string(),
            container_path: "/downloads".to_string(),
            read_only: false,
        };
        for source in [
            "/host_mnt/c/Users/tester/elixir/data/downloads",
            "/run/desktop/mnt/host/c/Users/tester/elixir/data/downloads",
            "/c/Users/tester/elixir/data/downloads",
        ] {
            let actual = ContainerMount {
                mount_type: "bind".to_string(),
                source: Some(source.to_string()),
                name: None,
                destination: "/downloads".to_string(),
                read_only: false,
            };
            assert!(
                DockerRuntimeManager::mount_matches(&desired, &actual),
                "expected Windows path to match Docker Desktop source {source}"
            );
        }
    }

    #[test]
    fn runtime_state_extracts_mounts_network_mode_labels_and_published_ports() {
        let inspect = json!({
            "Name": "/elx-qbittorrent",
            "Config": {
                "Labels": {
                    "elixir.instance_id": "abc",
                    "elixir.extension_id": "elixir.modules.qbittorrent"
                }
            },
            "HostConfig": {
                "NetworkMode": "container:elx-qbittorrent-vpn"
            },
            "Mounts": [
                {
                    "Type": "volume",
                    "Name": "elixir_cfg_abc",
                    "Source": "/var/lib/docker/volumes/elixir_cfg_abc/_data",
                    "Destination": "/config",
                    "RW": true
                },
                {
                    "Type": "bind",
                    "Source": "/host_mnt/Users/tester/elixir/data/downloads",
                    "Destination": "/downloads",
                    "RW": true
                }
            ],
            "NetworkSettings": {
                "Ports": {
                    "8080/tcp": [
                        { "HostIp": "127.0.0.1", "HostPort": "49152" }
                    ],
                    "6881/udp": null
                }
            }
        });

        let state = DockerRuntimeManager::extract_runtime_state(&inspect);
        assert_eq!(state.name, "elx-qbittorrent");
        assert_eq!(
            state.network_mode.as_deref(),
            Some("container:elx-qbittorrent-vpn")
        );
        assert_eq!(
            state.labels.get("elixir.extension_id").map(String::as_str),
            Some("elixir.modules.qbittorrent")
        );
        assert!(state.mounts.iter().any(|mount| {
            mount.mount_type == "volume"
                && mount.name.as_deref() == Some("elixir_cfg_abc")
                && mount.destination == "/config"
        }));
        assert!(state.mounts.iter().any(|mount| {
            mount.mount_type == "bind"
                && mount.source.as_deref() == Some("/host_mnt/Users/tester/elixir/data/downloads")
                && mount.destination == "/downloads"
        }));
        assert_eq!(
            state.published_ports,
            vec![ContainerPublishedPort {
                container_port: 8080,
                host_port: 49152,
                protocol: "tcp".to_string(),
                host_ip: Some("127.0.0.1".to_string()),
            }]
        );
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
    fn docker_probe_timeouts_are_treated_as_unavailable() {
        let err = anyhow::anyhow!("docker version --format {{.Server.Version}} timed out after 5s");
        assert!(is_docker_daemon_unavailable(&err));
        assert_eq!(
            classify_docker_runtime_failure(&err),
            Some(DockerRuntimeFailureKind::DaemonUnavailable)
        );
    }

    #[tokio::test]
    async fn docker_probe_command_timeout_returns_promptly() {
        let runtime = DockerRuntimeManager::new(Some("sleep".to_string()));
        let start = std::time::Instant::now();

        let result = runtime
            .run_capture_with_timeout(&["5".to_string()], Some(Duration::from_millis(50)))
            .await;
        let err = match result {
            Ok(_) => panic!("expected timed out command to fail"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("timed out"));
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "timeout path should return promptly"
        );
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
        assert_eq!(
            classify_docker_runtime_failure(&missing),
            Some(DockerRuntimeFailureKind::DesktopGuestSocketMissing)
        );
    }

    #[test]
    fn active_endpoint_network_errors_are_detected() {
        let err = anyhow::anyhow!(
            "docker network rm elixir_net failed (status Some(1)): Error response from daemon: error while removing network: network elixir_net id 123 has active endpoints"
        );
        assert!(network_has_attached_endpoints_error(&err));
    }

    #[test]
    fn unrelated_docker_errors_are_not_classified_as_daemon_unavailable() {
        let err = anyhow::anyhow!("docker inspect foo failed: No such container: foo");
        assert!(!is_docker_daemon_unavailable(&err));
    }

    #[test]
    fn kill_stuck_errors_are_classified() {
        let err = anyhow::anyhow!(
            "docker rm -f elx-b44181 failed (status Some(1)): Error response from daemon: Could not kill running container e12eaf..., cannot remove - tried to kill container, but did not receive an exit event"
        );
        assert_eq!(
            classify_docker_runtime_failure(&err),
            Some(DockerRuntimeFailureKind::EngineKillStuck)
        );
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

    #[test]
    fn spec_fingerprint_mismatch_is_detected_when_actual_carries_hash() {
        let mut spec = ContainerSpec {
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
        crate::runtime::model::apply_container_spec_fingerprint(&mut spec);
        let desired = spec
            .labels
            .get(CONTAINER_SPEC_HASH_LABEL)
            .expect("desired spec hash")
            .clone();
        let inspect = json!({
            "Config": {
                "Labels": {
                    "elixir.spec_hash": "old-hash"
                }
            }
        });

        assert_eq!(
            DockerRuntimeManager::container_spec_hash_mismatch(&spec, &inspect),
            Some(("old-hash".to_string(), desired))
        );
    }

    #[test]
    fn missing_actual_spec_fingerprint_is_legacy_compatible() {
        let mut spec = ContainerSpec {
            name: "elx-new".to_string(),
            image: "example:latest".to_string(),
            network: "elixir_net".to_string(),
            network_mode: None,
            aliases: Vec::new(),
            env: Vec::new(),
            volumes: Vec::new(),
            ports: Vec::new(),
            labels: HashMap::new(),
            command: Vec::new(),
            cap_add: Vec::new(),
            devices: Vec::new(),
            sysctls: HashMap::new(),
        };
        crate::runtime::model::apply_container_spec_fingerprint(&mut spec);
        let inspect = json!({ "Config": { "Labels": {} } });

        assert_eq!(
            DockerRuntimeManager::container_spec_hash_mismatch(&spec, &inspect),
            None
        );
    }

    #[test]
    fn top_output_detects_defunct_processes() {
        let output = "\
UID                 PID                 PPID                C                   STIME               TTY                 TIME                CMD
911                 7608                7308                0                   12:27               ?                   00:03:36            [Sonarr] <defunct>";
        assert!(DockerRuntimeManager::top_output_has_defunct_processes(
            output
        ));
    }

    #[test]
    fn top_output_ignores_healthy_processes() {
        let output = "\
UID                 PID                 PPID                C                   STIME               TTY                 TIME                CMD
911                 7608                7308                0                   12:27               ?                   00:03:36            /app/sonarr/bin/Sonarr -nobrowser";
        assert!(!DockerRuntimeManager::top_output_has_defunct_processes(
            output
        ));
    }
}
