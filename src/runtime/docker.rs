use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio::fs;
use tokio::process::Command;
use tokio::time::{Duration as TokioDuration, Instant, sleep, timeout};
use uuid::Uuid;

use crate::runtime::model::{
    CONTAINER_SPEC_HASH_LABEL, ContainerHandle, ContainerLogOptions, ContainerPublishedPort,
    ContainerRuntimeLogConfig, ContainerRuntimeMount, ContainerRuntimeSecurityState,
    ContainerRuntimeState, ContainerRuntimeTmpfsMount, ContainerRuntimeUlimit,
    ContainerSecurityOptions, ContainerSpec, ContainerState, ContainerTmpfsMount,
    ELIXIR_DEPLOYMENT_ID_LABEL, ELIXIR_EXTENSION_ID_LABEL, ELIXIR_INSTANCE_ID_LABEL,
    ELIXIR_MANAGED_LABEL, FixedMarketplaceVolumeSpec, OwnedDirectoryVolumeSpec, PortMapping,
    PrivateActorInspection, PrivateFileVolumeSpec, QuarantineMarketplaceVolumeSpec, VolumeMount,
    VolumeMountSourceKind,
};
use crate::runtime::packet_network::{PacketNetworkInspection, PacketNetworkSpec, immutable_id};
use crate::runtime::{PrivateProcessStdio, RuntimeManager};
#[cfg(unix)]
mod application_gateway;
mod application_http;
mod build_output;
mod image_labels;
mod image_import;
#[cfg(unix)]
mod local_observation;
mod measurement;
#[cfg(unix)]
mod provider;
#[cfg(unix)]
mod provider_guard;
#[cfg(unix)]
mod provider_observation;
#[cfg(unix)]
mod provider_worker;
mod source_recovery;

const REQUIRED_LABELS: [&str; 2] = [ELIXIR_INSTANCE_ID_LABEL, ELIXIR_EXTENSION_ID_LABEL];
const DOCKER_PROBE_COMMAND_TIMEOUT: Duration = Duration::from_secs(5);

pub struct DockerRuntimeManager {
    docker_bin: String,
    #[cfg(unix)]
    import_profile:Option<std::sync::Arc<crate::runtime::image_import::Profile>>,
    #[cfg(unix)]
    local_observer: tokio::sync::OnceCell<local_observation::Observer>,
    #[cfg(unix)]
    local_cli_binding: tokio::sync::OnceCell<()>,
    #[cfg(unix)]
    container_guardian: Option<std::sync::Arc<crate::runtime::container_guardian::Profile>>,
    immutable_image_labels: tokio::sync::Mutex<image_labels::Cache>,
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
    fn require_quarantine_exit_owner(
        handle: &ContainerHandle,
        required: &HashMap<String, String>,
    ) -> Result<()> {
        if !immutable_id(&handle.id)
            || required.get(ELIXIR_MANAGED_LABEL).map(String::as_str) != Some("true")
            || required
                .get("elixir.marketplace.isolation")
                .map(String::as_str)
                != Some("quarantine-v1")
            || required
                .get(CONTAINER_SPEC_HASH_LABEL)
                .is_none_or(String::is_empty)
        {
            bail!("native exit verification requires a specific owned quarantine");
        }
        Ok(())
    }
    fn verify_quarantine_exit(
        value: &Value,
        handle: &ContainerHandle,
        required: &HashMap<String, String>,
    ) -> Result<()> {
        Self::require_quarantine_exit_owner(handle, required)?;
        Self::verify_private_native_exit(value, handle, required)
    }
    fn verify_private_native_exit(
        value: &Value,
        handle: &ContainerHandle,
        required: &HashMap<String, String>,
    ) -> Result<()> {
        let state = Self::extract_state(value)?;
        let labels = Self::extract_labels(value);
        if state.id != handle.id
            || state.name != handle.name
            || state.running
            || required
                .iter()
                .any(|(key, expected)| labels.get(key) != Some(expected))
            || value.pointer("/Config/OpenStdin").and_then(Value::as_bool) != Some(true)
            || value.pointer("/Config/Tty").and_then(Value::as_bool) != Some(false)
            || value.pointer("/State/Status").and_then(Value::as_str) != Some("exited")
            || value.pointer("/State/ExitCode").and_then(Value::as_i64) != Some(0)
            || value.pointer("/State/Pid").and_then(Value::as_i64) != Some(0)
            || value.pointer("/State/Error").and_then(Value::as_str) != Some("")
            || ["Running", "Paused", "Restarting", "OOMKilled", "Dead"]
                .iter()
                .any(|field| {
                    value
                        .pointer(&format!("/State/{field}"))
                        .and_then(Value::as_bool)
                        != Some(false)
                })
        {
            bail!("owned native quarantine did not exit successfully");
        }
        Ok(())
    }
    /// Fixed local network operations only. Bound both streams before parsing
    /// even if a collision/foreign network carries huge labels or membership.
    async fn packet_network_capture(
        &self,
        args: &[String],
    ) -> Result<(std::process::ExitStatus, String, String)> {
        use tokio::io::AsyncReadExt;
        async fn read(
            mut stream: impl tokio::io::AsyncRead + Unpin,
            limit: u64,
        ) -> Result<Vec<u8>> {
            let mut bytes = vec![];
            (&mut stream)
                .take(limit + 1)
                .read_to_end(&mut bytes)
                .await?;
            if bytes.len() as u64 > limit {
                bail!("marketplace network output exceeds its bound");
            }
            Ok(bytes)
        }
        let mut child = self.docker_command().await?
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let stdout = child.stdout.take().context("missing network stdout")?;
        let stderr = child.stderr.take().context("missing network stderr")?;
        let (out, err, status) = timeout(Duration::from_secs(10), async {
            tokio::try_join!(read(stdout, 65536), read(stderr, 4096), async {
                child.wait().await.map_err(anyhow::Error::from)
            })
        })
        .await
        .context("marketplace network command deadline")??;
        Ok((status, String::from_utf8(out)?, String::from_utf8(err)?))
    }
    pub fn new(docker_bin: Option<String>) -> Self {
        Self {
            docker_bin: docker_bin.unwrap_or_else(|| "docker".to_string()),
            #[cfg(unix)]
            import_profile:None,
            #[cfg(unix)]
            local_observer: Default::default(),
            #[cfg(unix)]
            local_cli_binding: Default::default(),
            #[cfg(unix)]
            container_guardian: None,
            immutable_image_labels: Default::default(),
        }
    }

    async fn run_capture(&self, args: &[String]) -> Result<CommandOutput> {
        self.run_capture_with_timeout(args, None).await
    }

    async fn docker_command(&self) -> Result<Command> {
        #[cfg(unix)]
        if self.import_profile.is_some() {
            // Keep lazy endpoint validation off every caller's native stack.
            return Box::pin(self.local_docker_command()).await;
        }
        Ok(Command::new(&self.docker_bin))
    }

    async fn run_capture_with_timeout(
        &self,
        args: &[String],
        timeout_duration: Option<TokioDuration>,
    ) -> Result<CommandOutput> {
        let mut command = self.docker_command().await?;
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

    async fn inspect_named_volume(&self, name: &str) -> Result<Option<Value>> {
        let output = self.docker_command().await?
            .args(["volume", "inspect", name])
            .kill_on_drop(true)
            .output()
            .await
            .with_context(|| format!("inspecting Docker volume {name}"))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stderr.to_ascii_lowercase().contains("no such volume") {
                return Ok(None);
            }
            bail!(
                "docker volume inspect failed (status {:?}): {}",
                output.status.code(),
                stderr.trim()
            );
        }
        let payload: Value = serde_json::from_slice(&output.stdout)
            .context("Docker volume inspect returned invalid JSON")?;
        let values = payload
            .as_array()
            .filter(|values| values.len() == 1)
            .context("Docker volume inspect returned an unexpected result")?;
        Ok(values.first().cloned())
    }

    fn private_volume_labels(value: &Value) -> HashMap<String, String> {
        value
            .get("Labels")
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

    fn private_volume_labels_match(
        value: &Value,
        required_labels: &HashMap<String, String>,
    ) -> bool {
        let labels = Self::private_volume_labels(value);
        required_labels
            .iter()
            .all(|(key, expected)| labels.get(key) == Some(expected))
    }

    fn validate_private_file_volume_spec(spec: &PrivateFileVolumeSpec) -> Result<()> {
        fn safe_name(value: &str) -> bool {
            !value.is_empty()
                && value.len() <= 128
                && value
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric() || "_.-".contains(character))
        }

        if !safe_name(&spec.name)
            || !safe_name(&spec.file_name)
            || spec.image.trim().is_empty()
            || spec.image.len() > 512
            || spec.source_path.trim().is_empty()
            || !Path::new(&spec.source_path).is_absolute()
            || spec.owner_uid == 0
            || spec.owner_gid == 0
            || spec.labels.is_empty()
            || spec.labels.iter().any(|(key, value)| {
                key.is_empty()
                    || key.len() > 128
                    || value.is_empty()
                    || value.len() > 256
                    || key.contains(['\r', '\n', '='])
                    || value.contains(['\r', '\n'])
            })
        {
            bail!("private file volume specification is invalid");
        }
        let metadata = std::fs::symlink_metadata(&spec.source_path)
            .context("reading private volume source metadata")?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            bail!("private file volume source must be a regular non-symbolic file");
        }
        Ok(())
    }

    fn validate_owned_directory_volume_spec(spec: &OwnedDirectoryVolumeSpec) -> Result<()> {
        fn safe_name(value: &str) -> bool {
            !value.is_empty()
                && value.len() <= 128
                && value
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric() || "_.-".contains(character))
        }

        if !safe_name(&spec.name)
            || spec.image.trim().is_empty()
            || spec.image.len() > 512
            || spec.owner_uid == 0
            || spec.owner_gid == 0
            || spec.labels.is_empty()
            || spec.labels.iter().any(|(key, value)| {
                key.is_empty()
                    || key.len() > 128
                    || value.is_empty()
                    || value.len() > 256
                    || key.contains(['\r', '\n', '='])
                    || value.contains(['\r', '\n'])
            })
        {
            bail!("owned directory volume specification is invalid");
        }
        Ok(())
    }

    fn validate_fixed_marketplace_volume_spec(spec: &FixedMarketplaceVolumeSpec) -> Result<()> {
        Self::validate_owned_directory_volume_spec(&OwnedDirectoryVolumeSpec {
            name: spec.name.clone(),
            image: spec.image.clone(),
            owner_uid: 10004,
            owner_gid: 10004,
            labels: spec.labels.clone(),
        })?;
        if !spec.name.starts_with("elixir-marketplace-")
            || !crate::marketplace::runtime::image_is_pinned(&spec.image)
            || !(64..=256).contains(&spec.memory_limit_mb)
            || !(1..=100).contains(&spec.cpu_millicores)
            || spec.labels.get(ELIXIR_MANAGED_LABEL).map(String::as_str) != Some("true")
            || spec
                .labels
                .get("elixir.marketplace.volume")
                .map(String::as_str)
                != Some("fixed-v1")
            || !spec
                .labels
                .get(ELIXIR_DEPLOYMENT_ID_LABEL)
                .is_some_and(|id| !id.is_empty())
        {
            bail!("invalid fixed marketplace volume identity");
        }
        Ok(())
    }

    fn verify_fixed_marketplace_volume(
        value: &Value,
        spec: &FixedMarketplaceVolumeSpec,
    ) -> Result<()> {
        Self::verify_owned_local_volume(value, &spec.name, &spec.labels)
    }

    fn verify_owned_local_volume(
        value: &Value,
        name: &str,
        labels: &HashMap<String, String>,
    ) -> Result<()> {
        let options = value.get("Options");
        if value.get("Name").and_then(Value::as_str) != Some(name)
            || value.get("Driver").and_then(Value::as_str) != Some("local")
            || value.get("Scope").and_then(Value::as_str) != Some("local")
            || !options.is_some_and(|options| {
                options.is_null()
                    || options
                        .as_object()
                        .is_some_and(|options| options.is_empty())
            })
            || !Self::private_volume_labels_match(value, labels)
        {
            bail!("marketplace volume backend or ownership differs from its specification");
        }
        Ok(())
    }

    fn validate_quarantine_marketplace_volume_spec(
        spec: &QuarantineMarketplaceVolumeSpec,
    ) -> Result<()> {
        Self::validate_owned_directory_volume_spec(&OwnedDirectoryVolumeSpec {
            name: spec.name.clone(),
            image: spec.image.clone(),
            owner_uid: 10004,
            owner_gid: 10004,
            labels: spec.labels.clone(),
        })?;
        spec.workspace.validate()?;
        crate::marketplace::runtime::SourceJobLimits::new(
            spec.memory_limit_mb
                .checked_mul(1024 * 1024)
                .context("source quarantine memory overflow")?,
            spec.cpu_millicores,
        )?;
        if !spec.name.starts_with("elixir-marketplace-source-")
            || !crate::marketplace::runtime::image_is_pinned(&spec.image)
            || spec.labels.get(ELIXIR_MANAGED_LABEL).map(String::as_str) != Some("true")
            || spec
                .labels
                .get("elixir.marketplace.volume")
                .map(String::as_str)
                != Some("quarantine-v1")
            || !spec
                .labels
                .get(ELIXIR_DEPLOYMENT_ID_LABEL)
                .is_some_and(|id| !id.is_empty())
        {
            bail!("invalid source quarantine volume identity");
        }
        Ok(())
    }

    /// Stable helper name + ownership checks prevent abandoned initialization
    /// jobs accumulating or writing concurrently after a controller crash.
    async fn recover_quarantine_provisioner(
        &self,
        spec: &QuarantineMarketplaceVolumeSpec,
    ) -> Result<()> {
        let name = format!("{}-provision", spec.name);
        if let Some(handle) = self.get_container_handle(&name).await? {
            let value = self.inspect_container(&handle.id).await?;
            let labels = Self::extract_labels(&value);
            if labels.get(ELIXIR_MANAGED_LABEL).map(String::as_str) != Some("true")
                || labels.get(ELIXIR_DEPLOYMENT_ID_LABEL)
                    != spec.labels.get(ELIXIR_DEPLOYMENT_ID_LABEL)
                || labels
                    .get("elixir.marketplace.storage_job")
                    .map(String::as_str)
                    != Some("quarantine-v1")
                || labels.get("elixir.marketplace.storage_name") != Some(&spec.name)
            {
                bail!("refusing to stop an unowned source quarantine provisioner");
            }
            self.run_capture_with_timeout(
                &["rm".to_owned(), "-f".to_owned(), handle.id],
                Some(Duration::from_secs(15)),
            )
            .await?;
        }
        Ok(())
    }

    async fn quarantine_marketplace_volume_job(
        &self,
        spec: &QuarantineMarketplaceVolumeSpec,
        initialize: bool,
    ) -> Result<()> {
        let name = format!("{}-provision", spec.name);
        let mut args = vec![
            "run".to_owned(),
            "--rm".to_owned(),
            "--name".to_owned(),
            name,
            "--pull=never".to_owned(),
            "--network=none".to_owned(),
            "--read-only".to_owned(),
            "--cap-drop=ALL".to_owned(),
            "--security-opt=no-new-privileges".to_owned(),
            format!("--memory={}m", spec.memory_limit_mb),
            format!("--memory-swap={}m", spec.memory_limit_mb),
            format!(
                "--cpus={}.{:03}",
                spec.cpu_millicores / 1000,
                spec.cpu_millicores % 1000
            ),
            "--pids-limit=32".to_owned(),
            "--log-driver=none".to_owned(),
            "--shm-size=1m".to_owned(),
            "--ulimit=core=0:0".to_owned(),
            "--ulimit=nofile=256:256".to_owned(),
            "--tmpfs=/tmp:size=8m,noexec,nosuid,nodev".to_owned(),
            format!("--user={}", if initialize { "0:0" } else { "10004:10004" }),
        ];
        let mut labels = spec.labels.clone();
        labels.insert(
            "elixir.marketplace.storage_job".to_owned(),
            "quarantine-v1".to_owned(),
        );
        labels.insert(
            "elixir.marketplace.storage_name".to_owned(),
            spec.name.clone(),
        );
        let mut labels = labels.iter().collect::<Vec<_>>();
        labels.sort_by_key(|(key, _)| *key);
        for (key, value) in labels {
            args.extend(["--label".to_owned(), format!("{key}={value}")]);
        }
        if initialize {
            args.push("--cap-add=CHOWN".to_owned());
        }
        args.extend([
            "--mount".to_owned(),
            format!(
                "type=volume,source={},target=/quarantine,volume-nocopy{}",
                spec.name,
                if initialize { "" } else { ",readonly" }
            ),
            "--entrypoint=/usr/bin/timeout".to_owned(),
            spec.image.clone(),
            "--signal=KILL".to_owned(),
            "--".to_owned(),
            format!("{}s", spec.workspace.timeout_seconds.min(180)),
            "/usr/local/bin/node".to_owned(),
            "--max-old-space-size=16".to_owned(),
            "/opt/elixir-marketplace/src/provision-quarantine.mjs".to_owned(),
            if initialize { "initialize" } else { "inspect" }.to_owned(),
            String::from_utf8(elixir_marketplace::canonical::to_vec(&spec.workspace)?)?,
        ]);
        let result = self
            .run_capture_with_timeout(
                &args,
                Some(Duration::from_secs(spec.workspace.timeout_seconds.min(180))),
            )
            .await;
        if result.is_err() {
            self.recover_quarantine_provisioner(spec).await?;
        }
        let output =
            result.context("source quarantine provisioning failed; existing data was not reset")?;
        let report: Value = serde_json::from_str(&output.stdout)?;
        let reservation = spec.workspace.volume_reservation_bytes()?;
        let slots = spec.workspace.descriptors()?;
        if report.get("reservationBytes").and_then(Value::as_u64) != Some(reservation)
            || !report
                .get("allocatedBytes")
                .and_then(Value::as_u64)
                .is_some_and(|bytes| bytes > 0 && bytes <= reservation)
            || report
                .get("files")
                .and_then(Value::as_object)
                .map(|files| files.len())
                != Some(slots.len())
            || slots.iter().any(|slot| {
                report
                    .get("files")
                    .and_then(|files| files.get(slot.purpose.name()))
                    .and_then(Value::as_u64)
                    != Some(slot.blob.size)
            })
        {
            bail!("source quarantine allocation differs from its reservation");
        }
        Ok(())
    }

    async fn fixed_marketplace_volume_job(
        &self,
        spec: &FixedMarketplaceVolumeSpec,
        initialize: bool,
    ) -> Result<()> {
        let token = Uuid::new_v4().to_string();
        let name = format!("elixir-marketplace-storage-{token}");
        let memory_mb = if initialize { spec.memory_limit_mb } else { 64 };
        let memory = format!("--memory={memory_mb}m");
        let swap = format!("--memory-swap={memory_mb}m");
        let cpu = format!("--cpus=0.{:03}", spec.cpu_millicores);
        let mut args = [
            "run",
            "--rm",
            "--name",
            &name,
            "--pull=never",
            "--network=none",
            "--read-only",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges",
            &memory,
            &swap,
            &cpu,
            if initialize {
                "--pids-limit=32"
            } else {
                "--pids-limit=16"
            },
            "--log-driver=none",
            "--shm-size=1m",
            "--ulimit=core=0:0",
            "--ulimit=nofile=256:256",
            "--tmpfs=/tmp:size=8m,noexec,nosuid,nodev",
            "--label",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
        args.push(format!("elixir.marketplace.storage_job={token}"));
        args.extend([
            "--label".to_owned(),
            format!("{ELIXIR_MANAGED_LABEL}=true"),
            "--user".to_owned(),
            if initialize { "0:0" } else { "10004:10004" }.to_owned(),
        ]);
        if initialize {
            args.extend(["--cap-add=CHOWN".to_owned()]);
        }
        args.extend([
            "--mount".to_owned(),
            format!(
                "type=volume,source={},target=/state,volume-nocopy{}",
                spec.name,
                if initialize { "" } else { ",readonly" }
            ),
            "--entrypoint=/usr/local/bin/node".to_owned(),
            spec.image.clone(),
            format!(
                "--max-old-space-size={}",
                if initialize {
                    (memory_mb / 4).min(48)
                } else {
                    16
                }
            ),
            format!(
                "/opt/elixir-marketplace/src/{}-fixed-state.mjs",
                if initialize { "initialize" } else { "inspect" }
            ),
        ]);
        let result = self
            .run_capture_with_timeout(
                &args,
                Some(Duration::from_secs(if initialize { 120 } else { 30 })),
            )
            .await;
        if result.is_err() {
            // A CLI timeout does not prove the container stopped. Remove only
            // this exact freshly labelled helper, never the persistent volume.
            if let Ok(value) = self.inspect_container(&name).await {
                if Self::extract_labels(&value).get("elixir.marketplace.storage_job")
                    == Some(&token)
                {
                    self.run_capture_with_timeout(
                        &["rm".to_owned(), "-f".to_owned(), name],
                        Some(Duration::from_secs(10)),
                    )
                    .await
                    .context("stopping failed marketplace storage helper")?;
                }
            }
        }
        let output = result
            .context("marketplace storage verification failed; existing data was not reset")?;
        let report: Value = serde_json::from_str(&output.stdout)
            .context("invalid marketplace allocation report")?;
        if report.get("fileBytes").and_then(Value::as_u64) != Some(192 * 1024 * 1024)
            || !report
                .get("allocatedBytes")
                .and_then(Value::as_u64)
                .is_some_and(|bytes| (384 * 1024 * 1024..=416 * 1024 * 1024).contains(&bytes))
        {
            bail!("marketplace allocation exceeds its fixed reservation");
        }
        Ok(())
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
        Ok(Self::image_metadata(
            &self.inspect_image_value(image).await?,
        ))
    }

    async fn inspect_image_value(&self, image: &str) -> Result<Value> {
        #[cfg(unix)]
        if self.import_profile.is_some() && image_labels::immutable_reference(image) {
            let bytes = self.image_import_inspect(image.parse()?).await?;
            return Ok(elixir_marketplace::canonical::parse(
                &bytes,
                elixir_marketplace::container_image::import::MAX_INSPECTION_BYTES,
            )?);
        }
        let stdout = self
            .run_stdout(&[
                "image".to_string(),
                "inspect".to_string(),
                image.to_string(),
            ])
            .await?;
        let values: Vec<Value> =
            serde_json::from_str(&stdout).context("parsing docker image inspect output")?;
        if values.len() != 1 {
            bail!("docker image inspect did not return exactly one image");
        }
        values
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("docker image inspect returned no entries"))
    }

    fn image_metadata(value: &Value) -> DockerImageMetadata {
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
        DockerImageMetadata {
            repo_digests,
            labels,
        }
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
        #[cfg(unix)]
        if self.use_local_observation(name) {
            return Ok(self
                .observe_local_container(name)
                .await?
                .and_then(|value| value["Id"].as_str().map(str::to_owned)));
        }
        let args = vec![
            "ps".to_string(),
            "-a".to_string(),
            "--no-trunc".to_string(),
            "--filter".to_string(),
            Self::container_identity_filter(name),
            "--format".to_string(),
            "{{.ID}}".to_string(),
        ];
        let stdout = self.run_stdout(&args).await?;
        Self::parse_container_identity(name, &stdout)
    }

    fn container_identity_filter(reference: &str) -> String {
        if immutable_id(reference) {
            format!("id={reference}")
        } else {
            format!("name=^/{reference}$")
        }
    }

    fn parse_container_identity(reference: &str, output: &str) -> Result<Option<String>> {
        let mut rows = output.lines().map(str::trim).filter(|row| !row.is_empty());
        let id = rows.next();
        if rows.next().is_some()
            || id
                .is_some_and(|id| !immutable_id(id) || (immutable_id(reference) && reference != id))
        {
            bail!("container identity lookup did not return one exact immutable identity");
        }
        Ok(id.map(str::to_owned))
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
        #[cfg(unix)]
        if self.use_local_observation(name) {
            return self
                .observe_local_container(name)
                .await?
                .context("exact local container is absent");
        }
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

    fn require_quarantine_writer(
        value: &Value,
        handle: &ContainerHandle,
    ) -> Result<ContainerState> {
        let state = Self::extract_state(value)?;
        let labels = Self::extract_labels(value);
        let deployment = labels
            .get(ELIXIR_DEPLOYMENT_ID_LABEL)
            .filter(|id| {
                !id.is_empty()
                    && id.len() <= 64
                    && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
            })
            .context("native quarantine deployment is unavailable")?;
        if !immutable_id(&handle.id)
            || handle.name != handle.id
            || state.id != handle.id
            || state.name != format!("elixir-marketplace-source-writer-{deployment}")
            || labels.get(ELIXIR_MANAGED_LABEL).map(String::as_str) != Some("true")
            || labels
                .get("elixir.marketplace.isolation")
                .map(String::as_str)
                != Some("quarantine-v1")
            || labels.get(ELIXIR_EXTENSION_ID_LABEL).map(String::as_str)
                != Some("elixir-marketplace-source")
            || labels.get(ELIXIR_INSTANCE_ID_LABEL)
                != Some(&format!("marketplace-source-{deployment}"))
            || value
                .pointer("/State/Running")
                .and_then(Value::as_bool)
                .is_none()
            || !matches!(
                state.status.as_str(),
                "created" | "running" | "paused" | "exited" | "dead"
            )
            || matches!(state.status.as_str(), "running" | "paused") != state.running
        {
            bail!("refusing immediate stop of an unowned or ambiguous native quarantine writer");
        }
        Ok(state)
    }

    fn require_source_writer_spec(spec: &ContainerSpec) -> Result<()> {
        #[cfg(unix)]
        if crate::marketplace::runtime::container_execution::is_command(&spec.command) {
            crate::marketplace::runtime::container_execution::validate_spec(spec)?;
            container_resource_args(&spec.security)?;
            return Ok(());
        }
        if crate::marketplace::runtime::build::is_command(&spec.command) {
            crate::marketplace::runtime::build::validate_spec(spec)?;
            crate::marketplace::runtime::source_writer::SourceWriterIntent::capture(spec)?;
            container_resource_args(&spec.security)?;
            return Ok(());
        }
        let deployment = spec
            .labels
            .get(ELIXIR_DEPLOYMENT_ID_LABEL)
            .filter(|v| {
                !v.is_empty()
                    && v.len() <= 64
                    && v.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
            })
            .context("source writer deployment missing")?;
        if spec.name != format!("elixir-marketplace-source-writer-{deployment}")
            || spec.labels.get(ELIXIR_MANAGED_LABEL).map(String::as_str) != Some("true")
            || spec
                .labels
                .get("elixir.marketplace.isolation")
                .map(String::as_str)
                != Some("quarantine-v1")
            || spec
                .labels
                .get(ELIXIR_EXTENSION_ID_LABEL)
                .map(String::as_str)
                != Some("elixir-marketplace-source")
            || spec.labels.get(ELIXIR_INSTANCE_ID_LABEL)
                != Some(&format!("marketplace-source-{deployment}"))
            || !crate::marketplace::runtime::image_is_pinned(&spec.image)
            || spec.network_mode.as_deref() != Some("none")
            || !spec.aliases.is_empty()
            || !spec.ports.is_empty()
            || !spec.env.is_empty()
            || !spec.cap_add.is_empty()
            || !spec.devices.is_empty()
            || !spec.sysctls.is_empty()
            || spec.volumes.len() != 1
            || spec.volumes[0].source_kind != VolumeMountSourceKind::NamedVolume
            || spec.volumes[0].host_path != format!("elixir-marketplace-source-{deployment}")
            || spec.volumes[0].container_path != "/quarantine"
            || !spec.security.read_only_rootfs
            || !spec.security.no_new_privileges
            || spec.security.user.as_deref() != Some("10004:10004")
            || !crate::marketplace::runtime::source_writer::supported_command(&spec.command)
        {
            bail!("create-only source writer requires its closed offline profile");
        }
        container_resource_args(&spec.security)?;
        Ok(())
    }
    fn verify_unstarted_source_writer(
        value: &Value,
        handle: &ContainerHandle,
        spec: &ContainerSpec,
        limits: &crate::marketplace::runtime::SourceJobLimits,
    ) -> Result<()> {
        let state = Self::require_quarantine_writer(value, handle)?;
        if state.name != spec.name || state.status != "created" || state.running {
            bail!("source execution requires a never-started immutable container");
        }
        Self::verify_source_start_intent(value, spec)?;
        let actual = Self::extract_runtime_state(value);
        #[cfg(unix)]
        if crate::marketplace::runtime::container_execution::is_command(&spec.command) {
            return crate::marketplace::runtime::container_execution::verify_configuration(spec, &actual, limits);
        }
        if crate::marketplace::runtime::build::is_command(&spec.command) {
            return crate::marketplace::runtime::build::verify_configuration(spec, &actual, limits);
        }
        let volume = &spec.volumes[0];
        crate::marketplace::runtime::verify_quarantine_configuration(
            spec,
            &actual,
            limits,
            &volume.host_path,
            spec.security
                .file_size_limit_bytes
                .context("source file limit missing")?,
            volume.read_only,
        )
    }

    fn verify_source_start_intent(value: &Value, spec: &ContainerSpec) -> Result<()> {
        #[cfg(unix)]
        if crate::marketplace::runtime::container_execution::is_command(&spec.command) {
            return Self::verify_private_start_intent(value, spec, elixir_marketplace::container_execution::GUARDIAN_PATH);
        }
        Self::verify_private_start_intent(
            value,
            spec,
            "/opt/elixir-marketplace/bin/elixir-marketplace-launcher",
        )
    }
    fn verify_private_start_intent(
        value: &Value,
        spec: &ContainerSpec,
        entrypoint: &str,
    ) -> Result<()> {
        if value.pointer("/Config/Cmd") != Some(&serde_json::to_value(&spec.command)?)
            || value.pointer("/Config/Entrypoint") != Some(&serde_json::json!([entrypoint]))
            || value
                .pointer("/HostConfig/RestartPolicy/Name")
                .and_then(Value::as_str)
                != Some("no")
            || value.pointer("/HostConfig/Init").and_then(Value::as_bool) != Some(false)
            || value.pointer("/HostConfig/PidMode").and_then(Value::as_str) != Some("")
            || value.pointer("/HostConfig/IpcMode").and_then(Value::as_str) != Some("private")
            || !matches!(
                value
                    .pointer("/HostConfig/UsernsMode")
                    .and_then(Value::as_str),
                Some("" | "private")
            )
        {
            bail!("source start command, launcher or namespace policy differs");
        }
        // No image/daemon environment may add process hooks or preload code.
        // These are the only ambient variables in the pinned worker contract;
        // extension settings/secrets travel through the private SDK, not OCI.
        let env = value
            .pointer("/Config/Env")
            .and_then(Value::as_array)
            .context("source environment is unavailable")?;
        let mut keys = HashSet::new();
        #[cfg(unix)]
        let compiled = crate::marketplace::runtime::container_execution::is_command(&spec.command);
        #[cfg(not(unix))]
        let compiled = false;
        #[cfg(unix)]
        if compiled {
            if entrypoint != elixir_marketplace::container_execution::GUARDIAN_PATH {
                bail!("compiled command requires its independent guardian entrypoint");
            }
            crate::marketplace::runtime::container_execution::verify_environment(spec, value.pointer("/Config/Env").context("compiled environment absent")?)?;
        }
        for item in env.iter().filter(|_| !compiled) {
            let (key, _) = item
                .as_str()
                .and_then(|v| v.split_once('='))
                .context("source environment is malformed")?;
            let rust_build = spec
                .command
                .first()
                .is_some_and(|mode| mode == "--build-run")
                && matches!(key, "RUSTUP_HOME" | "CARGO_HOME" | "RUST_VERSION");
            if (!matches!(key, "PATH" | "NODE_VERSION" | "YARN_VERSION") && !rust_build)
                || !keys.insert(key)
            {
                bail!("source environment contains an unexpected or duplicate setting");
            }
        }
        for pointer in ["/HostConfig/Devices", "/HostConfig/DeviceRequests"] {
            if !matches!(value.pointer(pointer), Some(Value::Null))
                && !value
                    .pointer(pointer)
                    .and_then(Value::as_array)
                    .is_some_and(Vec::is_empty)
            {
                bail!("source start contains unapproved host devices");
            }
        }
        // Docker's `Binds` also encodes named volumes supplied with -v. Match
        // the exact desired mapping, not merely an empty field. The independent
        // actual-Mounts check still requires type=volume and this volume name;
        // a host bind at the same destination is never accepted.
        let binds = match value.pointer("/HostConfig/Binds") {
            Some(Value::Null) => Vec::new(),
            Some(Value::Array(values)) => values
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .map(str::to_owned)
                        .context("invalid source mount mapping")
                })
                .collect::<Result<Vec<_>>>()?,
            _ => bail!("source mount mapping is unavailable"),
        };
        if binds != spec.volumes.iter().map(format_volume).collect::<Vec<_>>() {
            bail!("source mount mapping differs from its exact approved volume");
        }
        match value.pointer("/HostConfig/Sysctls") {
            // Docker omits its empty optional sysctls map. This is the OCI
            // empty configuration, not a missing kernel/state measurement.
            None | Some(Value::Null) => {}
            Some(Value::Object(values)) if values.is_empty() => {}
            _ => bail!("source start contains unexpected sysctls"),
        }
        Ok(())
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

    fn extract_runtime_security(value: &Value) -> ContainerRuntimeSecurityState {
        let user = value
            .pointer("/Config/User")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        let read_only_rootfs = value
            .pointer("/HostConfig/ReadonlyRootfs")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let security_opts = value
            .pointer("/HostConfig/SecurityOpt")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let no_new_privileges = security_opts
            .iter()
            .any(|value| value == "no-new-privileges" || value == "no-new-privileges:true");
        let seccomp_profile = Self::security_opt_value(&security_opts, "seccomp");
        let apparmor_profile = Self::security_opt_value(&security_opts, "apparmor");
        let mut cap_drop = value
            .pointer("/HostConfig/CapDrop")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        cap_drop.sort();
        let mut tmpfs = value
            .pointer("/HostConfig/Tmpfs")
            .and_then(Value::as_object)
            .map(|items| {
                items
                    .iter()
                    .map(|(path, options)| ContainerRuntimeTmpfsMount {
                        path: path.clone(),
                        options: options.as_str().map(str::to_string),
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        tmpfs.sort_by(|left, right| left.path.cmp(&right.path));
        let memory_limit_bytes = value
            .pointer("/HostConfig/Memory")
            .and_then(Value::as_i64)
            .filter(|value| *value > 0);
        let pids_limit = value
            .pointer("/HostConfig/PidsLimit")
            .and_then(Value::as_i64)
            .filter(|value| *value > 0);
        let nano_cpus = value
            .pointer("/HostConfig/NanoCpus")
            .and_then(Value::as_i64)
            .filter(|value| *value > 0);
        let memory_swap_limit_bytes = value
            .pointer("/HostConfig/MemorySwap")
            .and_then(Value::as_i64);
        let shared_memory_bytes = value.pointer("/HostConfig/ShmSize").and_then(Value::as_i64);
        let privileged = value
            .pointer("/HostConfig/Privileged")
            .and_then(Value::as_bool);
        let cap_add = value
            .pointer("/HostConfig/CapAdd")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default();
        let log_config = value.pointer("/HostConfig/LogConfig").and_then(|config| {
            let driver = config.get("Type")?.as_str()?.to_owned();
            let options = config
                .get("Config")?
                .as_object()?
                .iter()
                .map(|(key, value)| Some((key.clone(), value.as_str()?.to_owned())))
                .collect::<Option<BTreeMap<_, _>>>()?;
            Some(ContainerRuntimeLogConfig { driver, options })
        });
        let core_limits = value
            .pointer("/HostConfig/Ulimits")
            .and_then(Value::as_array)
            .map(|limits| {
                limits
                    .iter()
                    .filter(|limit| limit.get("Name").and_then(Value::as_str) == Some("core"))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let core_dump_limit = if core_limits.len() == 1 {
            core_limits[0]
                .get("Soft")
                .and_then(Value::as_i64)
                .zip(core_limits[0].get("Hard").and_then(Value::as_i64))
                .map(|(soft, hard)| ContainerRuntimeUlimit { soft, hard })
        } else {
            None
        };
        let file_limits = value
            .pointer("/HostConfig/Ulimits")
            .and_then(Value::as_array)
            .map(|limits| {
                limits
                    .iter()
                    .filter(|limit| limit.get("Name").and_then(Value::as_str) == Some("nofile"))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let open_files_limit = if file_limits.len() == 1 {
            file_limits[0]
                .get("Soft")
                .and_then(Value::as_i64)
                .zip(file_limits[0].get("Hard").and_then(Value::as_i64))
                .map(|(soft, hard)| ContainerRuntimeUlimit { soft, hard })
        } else {
            None
        };
        let size_limits = value
            .pointer("/HostConfig/Ulimits")
            .and_then(Value::as_array)
            .map(|limits| {
                limits
                    .iter()
                    .filter(|limit| limit.get("Name").and_then(Value::as_str) == Some("fsize"))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let file_size_limit = if size_limits.len() == 1 {
            size_limits[0]
                .get("Soft")
                .and_then(Value::as_i64)
                .zip(size_limits[0].get("Hard").and_then(Value::as_i64))
                .map(|(soft, hard)| ContainerRuntimeUlimit { soft, hard })
        } else {
            None
        };

        ContainerRuntimeSecurityState {
            image_reference: value
                .pointer("/Config/Image")
                .and_then(Value::as_str)
                .map(str::to_owned),
            image_id: value
                .get("Image")
                .and_then(Value::as_str)
                .map(str::to_owned),
            user,
            read_only_rootfs,
            no_new_privileges,
            cap_drop,
            tmpfs,
            memory_limit_bytes,
            pids_limit,
            nano_cpus,
            seccomp_profile,
            apparmor_profile,
            memory_swap_limit_bytes,
            log_config,
            shared_memory_bytes,
            core_dump_limit,
            open_files_limit,
            file_size_limit,
            stdin_open: value.pointer("/Config/OpenStdin").and_then(Value::as_bool),
            tty: value.pointer("/Config/Tty").and_then(Value::as_bool),
            privileged,
            cap_add,
        }
    }

    fn security_opt_value(options: &[String], key: &str) -> Option<String> {
        let prefix = format!("{key}=");
        options
            .iter()
            .find_map(|value| value.strip_prefix(&prefix))
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
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
            security: Self::extract_runtime_security(value),
        }
    }

    fn extract_private_actor(value: &Value) -> Result<PrivateActorInspection> {
        fn strings(value: &Value, path: &str) -> Result<Vec<String>> {
            match value.pointer(path).context("missing private actor array")? {
                Value::Null => Ok(vec![]),
                Value::Array(values) => values
                    .iter()
                    .map(|value| {
                        value
                            .as_str()
                            .map(str::to_owned)
                            .context("invalid private actor array")
                    })
                    .collect(),
                _ => bail!("invalid private actor array"),
            }
        }
        fn count(value: &Value, path: &str) -> Result<usize> {
            match value.pointer(path).unwrap_or(&Value::Null) {
                Value::Null => Ok(0),
                Value::Array(values) => Ok(values.len()),
                _ => bail!("invalid private actor collection"),
            }
        }
        let text = |path: &str| {
            value
                .pointer(path)
                .and_then(Value::as_str)
                .map(str::to_owned)
                .context("missing private actor setting")
        };
        let state = Self::extract_state(value)?;
        // Do not let the general compatibility parser discard malformed caps.
        strings(value, "/HostConfig/CapAdd")?;
        strings(value, "/HostConfig/CapDrop")?;
        if strings(value, "/HostConfig/SecurityOpt")?
            .iter()
            .any(|option| {
                !matches!(
                    option.as_str(),
                    "no-new-privileges" | "no-new-privileges:true"
                )
            })
        {
            bail!("private actor has unexpected security options");
        }
        match value.pointer("/HostConfig/Tmpfs").unwrap_or(&Value::Null) {
            Value::Null => {}
            Value::Object(values) if values.is_empty() => {}
            _ => bail!("private actor may not have tmpfs mounts"),
        }
        value
            .pointer("/State/Running")
            .and_then(Value::as_bool)
            .context("missing private actor running state")?;
        let sysctls = match value.pointer("/HostConfig/Sysctls").unwrap_or(&Value::Null) {
            Value::Null => BTreeMap::new(),
            Value::Object(values) => values
                .iter()
                .map(|(key, value)| {
                    Ok((
                        key.clone(),
                        value
                            .as_str()
                            .context("invalid private actor sysctl")?
                            .to_owned(),
                    ))
                })
                .collect::<Result<_>>()?,
            _ => bail!("invalid private actor sysctls"),
        };
        let network_rows = value
            .pointer("/NetworkSettings/Networks")
            .and_then(Value::as_object)
            .context("missing private actor networks")?;
        if network_rows.len() > 2 {
            bail!("unexpected private actor network count");
        }
        let attachment_ids = |field: &str| -> Result<BTreeMap<String, String>> {
            network_rows
                .iter()
                .map(|(name, row)| {
                    let row = row
                        .as_object()
                        .context("invalid private actor network attachment")?;
                    let id = match row.get(field) {
                        None | Some(Value::Null) => String::new(),
                        Some(Value::String(value)) => value.clone(),
                        _ => bail!("invalid private actor network identity"),
                    };
                    Ok((name.clone(), id))
                })
                .collect()
        };
        let network_ids = attachment_ids("NetworkID")?;
        let endpoint_ids = attachment_ids("EndpointID")?;
        let mut networks = network_rows.keys().cloned().collect::<Vec<_>>();
        networks.sort();
        if !value
            .get("Mounts")
            .and_then(Value::as_array)
            .context("missing private actor mounts")?
            .is_empty()
        {
            bail!("private actor may not have mounts");
        }
        Ok(PrivateActorInspection {
            id: state.id,
            running: state.running,
            runtime: Self::extract_runtime_state(value),
            command: strings(value, "/Config/Cmd")?,
            entrypoint: strings(value, "/Config/Entrypoint")?,
            environment: strings(value, "/Config/Env")?,
            pid_mode: text("/HostConfig/PidMode")?,
            ipc_mode: text("/HostConfig/IpcMode")?,
            userns_mode: text("/HostConfig/UsernsMode")?,
            device_count: count(value, "/HostConfig/Devices")?,
            device_request_count: count(value, "/HostConfig/DeviceRequests")?,
            bind_count: count(value, "/HostConfig/Binds")?,
            sysctls,
            networks,
            network_ids,
            endpoint_ids,
            restart_policy: text("/HostConfig/RestartPolicy/Name")?,
        })
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
            Self::managed_container_filter(),
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
        // Immutable handles must not be resolved through Docker's *name*
        // filter, nor inspected through a reusable name after a separate check.
        // Preserve the existing optional-name API for ordinary callers.
        if container_name.len() == 64
            && container_name
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            let inspect = self.inspect_container(container_name).await?;
            if Self::extract_state(&inspect)?.id != container_name {
                bail!("immutable container inspection identity differs");
            }
            return Ok(Some(Self::extract_runtime_state(&inspect)));
        }
        if self.find_container_id(container_name).await?.is_none() {
            return Ok(None);
        }

        let inspect = self.inspect_container(container_name).await?;
        Ok(Some(Self::extract_runtime_state(&inspect)))
    }

    pub async fn stop_and_remove_managed_containers(
        &self,
        active_instance_ids: &HashSet<String>,
        deployment_id: &str,
    ) -> Result<Vec<String>> {
        let mut names = self.list_managed_container_names().await?;
        names.sort();

        let mut removed = Vec::new();
        for name in names {
            let inspect = self.inspect_container(&name).await?;
            let labels = Self::extract_labels(&inspect);
            if !Self::should_reset_managed_container(&labels, active_instance_ids, deployment_id) {
                tracing::info!(
                    "docker runtime: skipping managed container '{}' during reset because it is outside deployment scope '{}'",
                    name,
                    deployment_id
                );
                continue;
            }

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

    fn has_matching_deployment_label(
        labels: &HashMap<String, String>,
        deployment_id: &str,
    ) -> bool {
        labels
            .get(ELIXIR_DEPLOYMENT_ID_LABEL)
            .is_some_and(|value| value == deployment_id)
    }

    fn has_active_legacy_instance_label(
        labels: &HashMap<String, String>,
        active_instance_ids: &HashSet<String>,
    ) -> bool {
        !labels.contains_key(ELIXIR_DEPLOYMENT_ID_LABEL)
            && labels
                .get(ELIXIR_INSTANCE_ID_LABEL)
                .is_some_and(|value| active_instance_ids.contains(value))
    }

    fn managed_container_filter() -> String {
        format!("label={ELIXIR_MANAGED_LABEL}=true")
    }

    fn should_prune_orphaned_managed_container(
        labels: &HashMap<String, String>,
        active_instance_ids: &HashSet<String>,
        deployment_id: &str,
    ) -> bool {
        if labels
            .get(ELIXIR_INSTANCE_ID_LABEL)
            .is_some_and(|value| active_instance_ids.contains(value))
        {
            return false;
        }
        Self::has_matching_deployment_label(labels, deployment_id)
    }

    fn should_reset_managed_container(
        labels: &HashMap<String, String>,
        active_instance_ids: &HashSet<String>,
        deployment_id: &str,
    ) -> bool {
        Self::has_matching_deployment_label(labels, deployment_id)
            || Self::has_active_legacy_instance_label(labels, active_instance_ids)
    }

    fn should_remove_conflicting_managed_container(
        labels: &HashMap<String, String>,
        spec: &ContainerSpec,
    ) -> bool {
        if labels.get(ELIXIR_INSTANCE_ID_LABEL) == spec.labels.get(ELIXIR_INSTANCE_ID_LABEL) {
            return false;
        }

        let Some(deployment_id) = spec.labels.get(ELIXIR_DEPLOYMENT_ID_LABEL) else {
            return true;
        };
        Self::has_matching_deployment_label(labels, deployment_id)
    }

    fn ensure_existing_container_is_mutable_by_spec(
        name: &str,
        labels: &HashMap<String, String>,
        spec: &ContainerSpec,
    ) -> Result<()> {
        if labels.get(ELIXIR_INSTANCE_ID_LABEL) == spec.labels.get(ELIXIR_INSTANCE_ID_LABEL) {
            return Ok(());
        }

        let Some(expected_deployment_id) = spec.labels.get(ELIXIR_DEPLOYMENT_ID_LABEL) else {
            return Ok(());
        };

        match labels.get(ELIXIR_DEPLOYMENT_ID_LABEL) {
            Some(actual_deployment_id) if actual_deployment_id == expected_deployment_id => Ok(()),
            Some(actual_deployment_id) => bail!(
                "container '{}' belongs to Elixir deployment '{}' but current deployment is '{}'; refusing to mutate it",
                name,
                actual_deployment_id,
                expected_deployment_id
            ),
            None => bail!(
                "container '{}' is Elixir-managed but has no deployment label and does not match instance id '{}'; refusing to mutate it",
                name,
                spec.labels
                    .get(ELIXIR_INSTANCE_ID_LABEL)
                    .map(String::as_str)
                    .unwrap_or("<missing>")
            ),
        }
    }

    async fn remove_conflicting_managed_containers(&self, spec: &ContainerSpec) -> Result<usize> {
        if spec.network_mode.is_some() || spec.aliases.is_empty() {
            return Ok(0);
        }

        let Some(expected_instance_id) = spec.labels.get(ELIXIR_INSTANCE_ID_LABEL) else {
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
            if labels.get(ELIXIR_INSTANCE_ID_LABEL) == Some(expected_instance_id) {
                continue;
            }
            if !Self::should_remove_conflicting_managed_container(&labels, spec) {
                tracing::warn!(
                    "docker runtime: leaving conflicting managed container '{}' in place because it is outside the current deployment scope",
                    name
                );
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
        deployment_id: &str,
    ) -> Result<Vec<String>> {
        let mut removed = Vec::new();
        for name in self.list_managed_container_names().await? {
            let inspect = self.inspect_container(&name).await?;
            let labels = Self::extract_labels(&inspect);
            if !Self::should_prune_orphaned_managed_container(
                &labels,
                active_instance_ids,
                deployment_id,
            ) {
                tracing::info!(
                    "docker runtime: leaving managed container '{}' in place during orphan cleanup because it is active, foreign, or legacy-unscoped",
                    name
                );
                continue;
            }
            let instance_id = labels.get(ELIXIR_INSTANCE_ID_LABEL);

            tracing::warn!(
                "docker runtime: removing deployment-scoped orphaned managed container '{}' with missing/stale instance id {:?}",
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
        self.create_container_mode(spec, true).await
    }
    async fn create_container_mode(
        &self,
        spec: &ContainerSpec,
        start: bool,
    ) -> Result<ContainerHandle> {
        self.create_container_entrypoint(spec, start, None).await
    }
    async fn create_container_entrypoint(
        &self,
        spec: &ContainerSpec,
        start: bool,
        entrypoint: Option<&str>,
    ) -> Result<ContainerHandle> {
        Self::ensure_required_labels(&spec.labels)?;
        let resource_args = container_resource_args(&spec.security)?;

        let mut args = if start {
            vec!["run".to_owned(), "-d".to_owned()]
        } else {
            vec!["create".to_owned()]
        };
        args.extend(["--name".to_owned(), spec.name.clone()]);
        if matches!(
            spec.labels
                .get("elixir.marketplace.isolation")
                .map(String::as_str),
            Some(
                "fixed-v1"
                    | "quarantine-v1"
                    | "packet-v1"
                    | "measurement-v1"
                    | "application-http-v1"
                    | "application-gateway-v1"
                    | "provider-v1"
            )
        ) {
            args.push("--pull=never".to_owned());
        }
        if !spec.security.init_process
            && matches!(
                spec.labels
                    .get("elixir.marketplace.isolation")
                    .map(String::as_str),
                Some(
                    "quarantine-v1"
                        | "packet-v1"
                        | "measurement-v1"
                        | "application-http-v1"
                        | "application-gateway-v1"
                )
            )
        {
            // Preserve legacy native topology, never a daemon-global default.
            // An explicitly fingerprinted reaper is selected via resource_args.
            args.push("--init=false".to_owned());
        }
        if let Some(entrypoint) = entrypoint {
            args.extend(["--entrypoint".to_owned(), entrypoint.to_owned()]);
        }

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

        if !spec.labels.contains_key(ELIXIR_MANAGED_LABEL) {
            args.push("--label".to_string());
            args.push(format!("{ELIXIR_MANAGED_LABEL}=true"));
        }

        for env in &spec.env {
            args.push("-e".to_string());
            args.push(format!("{}={}", env.name, env.value));
        }

        if let Some(user) = spec
            .security
            .user
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            args.push("--user".to_string());
            args.push(user.to_string());
        }

        if spec.security.read_only_rootfs {
            args.push("--read-only".to_string());
        }

        if spec.security.no_new_privileges {
            args.push("--security-opt".to_string());
            args.push("no-new-privileges".to_string());
        }

        if let Some(seccomp_profile) = spec
            .security
            .seccomp_profile
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            args.push("--security-opt".to_string());
            args.push(format!("seccomp={seccomp_profile}"));
        }

        if let Some(apparmor_profile) = spec
            .security
            .apparmor_profile
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            args.push("--security-opt".to_string());
            args.push(format!("apparmor={apparmor_profile}"));
        }

        for capability in &spec.cap_add {
            args.push("--cap-add".to_string());
            args.push(capability.clone());
        }

        for capability in &spec.cap_drop {
            args.push("--cap-drop".to_string());
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

        for tmpfs in &spec.security.tmpfs {
            args.push("--tmpfs".to_string());
            args.push(format_tmpfs(tmpfs));
        }

        if let Some(memory_limit_mb) = spec.security.memory_limit_mb {
            args.push("--memory".to_string());
            args.push(format!("{memory_limit_mb}m"));
        }
        args.extend(resource_args);

        if let Some(pids_limit) = spec.security.pids_limit {
            args.push("--pids-limit".to_string());
            args.push(pids_limit.to_string());
        }

        if let Some(cpu_quota) = spec
            .security
            .cpu_quota
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            args.push("--cpus".to_string());
            args.push(cpu_quota.to_string());
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
        if id.trim().is_empty() || (!start && !immutable_id(&id)) {
            bail!("native container creation omitted its required identity");
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

/// Only these fixed entry points may run as the independent helper UID.
/// No peer/request can supply a command, environment, UID or image feature.
enum IndependentMarketplaceFetch<'a> {
    Connectivity,
    SourcePreflight,
    RadicleSource,
    SdkHttp,
    SdkHttpStream,
    Metadata(&'a [elixir_marketplace::identity::PublicKey]),
    Security(&'a [elixir_marketplace::identity::PublicKey]),
    Evidence(&'a [elixir_marketplace::identity::PublicKey]),
}
impl IndependentMarketplaceFetch<'_> {
    fn supports_feature(&self, actual: Option<&str>, exact: &str) -> bool {
        match self {
            Self::Metadata(_) => matches!(actual, Some("1" | "2" | "3")),
            Self::Security(_) => matches!(actual, Some("2" | "3")),
            Self::Evidence(_) => actual == Some("3"),
            _ => actual == Some(exact),
        }
    }
}
impl DockerRuntimeManager {
    async fn exec_independent_marketplace_fetch(
        &self,
        handle: &ContainerHandle,
        required: &HashMap<String, String>,
        purpose: IndependentMarketplaceFetch<'_>,
    ) -> Result<PrivateProcessStdio> {
        if !immutable_id(&handle.id)
            || required.get(ELIXIR_MANAGED_LABEL).map(String::as_str) != Some("true")
            || required
                .get("elixir.marketplace.isolation")
                .map(String::as_str)
                != Some("fixed-v1")
            || !required.contains_key(CONTAINER_SPEC_HASH_LABEL)
        {
            bail!("independent fetch requires a specific managed marketplace worker");
        }
        let value = self.inspect_container(&handle.id).await?;
        let state = Self::extract_state(&value)?;
        let labels = Self::extract_labels(&value);
        let security = Self::extract_runtime_security(&value);
        let network = value
            .pointer("/HostConfig/NetworkMode")
            .and_then(Value::as_str);
        let isolated = network == Some("none")
            || network
                .and_then(|mode| mode.strip_prefix("container:"))
                .is_some_and(immutable_id);
        if state.id != handle.id
            || state.name != handle.name
            || !state.running
            || required
                .iter()
                .any(|(key, value)| labels.get(key) != Some(value))
            || !isolated
            || value.pointer("/HostConfig/PidMode").and_then(Value::as_str) != Some("")
            || security.user.as_deref() != Some("10004:10004")
            || !security.read_only_rootfs
            || !security.no_new_privileges
            || security.privileged != Some(false)
            || !security.cap_add.is_empty()
            || !security
                .cap_drop
                .iter()
                .any(|cap| cap.eq_ignore_ascii_case("ALL"))
            || security.tty != Some(false)
        {
            bail!("independent fetch worker identity or isolation differs");
        }
        let image = value
            .get("Image")
            .and_then(Value::as_str)
            .context("missing fetch image identity")?;
        if !image.strip_prefix("sha256:").is_some_and(immutable_id) {
            bail!("independent fetch image is not immutable");
        }
        let (feature, version, argument) = match purpose {
            IndependentMarketplaceFetch::Connectivity => (
                "elixir.marketplace.worker.connectivity_probe",
                "2",
                "--connectivity-probe",
            ),
            IndependentMarketplaceFetch::SourcePreflight => (
                "elixir.marketplace.worker.source_preflight",
                "2",
                "--source-preflight",
            ),
            IndependentMarketplaceFetch::RadicleSource => (
                "elixir.marketplace.worker.radicle_source",
                "2",
                "--radicle-source",
            ),
            IndependentMarketplaceFetch::SdkHttp => {
                ("elixir.marketplace.worker.sdk_http", "1", "--sdk-http")
            }
            IndependentMarketplaceFetch::SdkHttpStream => (
                "elixir.marketplace.worker.sdk_http_stream",
                "1",
                "--sdk-http-stream",
            ),
            IndependentMarketplaceFetch::Metadata(keys)
            | IndependentMarketplaceFetch::Security(keys)
            | IndependentMarketplaceFetch::Evidence(keys) => {
                if !(1..=4).contains(&keys.len()) {
                    bail!("invalid independent metadata issuer keys");
                }
                // Bind the independent helper to this worker's actual configured
                // issuer scope, never keys from a candidate or peer response.
                let command = value
                    .pointer("/Config/Cmd")
                    .and_then(Value::as_array)
                    .context("missing metadata worker configuration")?;
                if command.len() > 12 || command.len() % 2 != 0 {
                    bail!("invalid metadata worker configuration");
                }
                let configured = command
                    .chunks_exact(2)
                    .filter(|pair| pair[0].as_str() == Some("--issuer-key"))
                    .map(|pair| {
                        pair[1]
                            .as_str()
                            .context("invalid configured issuer key")?
                            .parse::<elixir_marketplace::identity::PublicKey>()
                            .map_err(Into::into)
                    })
                    .collect::<Result<Vec<_>>>()?;
                if configured != keys {
                    bail!("independent metadata issuer keys differ from worker");
                }
                elixir_marketplace::evidence::IssuerPolicy::new(keys.iter().copied())?;
                (
                    "elixir.marketplace.worker.metadata_verifier",
                    if matches!(purpose, IndependentMarketplaceFetch::Evidence(_)) {
                        "3"
                    } else if matches!(purpose, IndependentMarketplaceFetch::Security(_)) {
                        "2"
                    } else {
                        "1"
                    },
                    "--metadata-verifier",
                )
            }
        };
        let features = self.image_labels(image).await?;
        let actual = features.get(feature).map(String::as_str);
        if !purpose.supports_feature(actual, version) {
            bail!("marketplace image lacks the independent fetch feature");
        }
        let mut command = self.docker_command().await?;
        command.args([
            "exec",
            "-i",
            "--user",
            "10005:10005",
            &handle.id,
            "/opt/elixir-marketplace/bin/elixir-marketplace-launcher",
            argument,
        ]);
        if let IndependentMarketplaceFetch::Metadata(keys)
        | IndependentMarketplaceFetch::Security(keys)
        | IndependentMarketplaceFetch::Evidence(keys) = purpose
        {
            for key in keys {
                command.args(["--issuer-key", &key.to_string()]);
            }
        }
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        Ok(PrivateProcessStdio {
            input: child.stdin.take().context("missing fetch stdin")?,
            output: child.stdout.take().context("missing fetch stdout")?,
            diagnostics: Some(child.stderr.take().context("missing fetch stderr")?),
            child,
        })
    }
}

#[async_trait::async_trait]
impl RuntimeManager for DockerRuntimeManager {
    #[cfg(unix)]
    fn marketplace_container_guardian(&self) -> Option<std::sync::Arc<crate::runtime::container_guardian::Profile>> {
        self.container_guardian.clone()
    }
    #[cfg(unix)]
    fn marketplace_import_profile(&self)->Option<std::sync::Arc<crate::runtime::image_import::Profile>> {self.import_profile.clone()}
    async fn marketplace_daemon_id(&self)->Result<String> {self.image_import_daemon_id().await}
    async fn inspect_marketplace_import_image(&self,image:elixir_marketplace::identity::Digest)->Result<Vec<u8>> {self.image_import_inspect(image).await}
    #[cfg(unix)]
    async fn provider_transport(
        &self,
        handle: &ContainerHandle,
        profile: &crate::marketplace::provider::Profile,
    ) -> Result<crate::runtime::provider_observation::ProviderTransport> {
        self.observe_provider_transport(handle, profile).await
    }
    #[cfg(unix)]
    async fn probe_provider_exit(
        &self,
        handle: &ContainerHandle,
        profile: &crate::marketplace::provider::Profile,
        ipv6: bool,
    ) -> Result<()> {
        self.observe_provider_exit(handle, profile, ipv6).await
    }
    #[cfg(unix)]
    async fn provider_namespace(
        &self,
        handle: &ContainerHandle,
        profile: &crate::marketplace::provider::Profile,
    ) -> Result<u64> {
        self.provider_native_namespace(handle, profile).await
    }
    #[cfg(unix)]
    async fn create_provider_worker(
        &self,
        profile: &crate::marketplace::provider::WorkerProfile,
        attempt: crate::marketplace::provider::WorkerCreateAttempt,
    ) -> Result<crate::runtime::provider_creation::WorkerCreation> {
        self.create_provider_worker_actor(profile, attempt).await
    }
    #[cfg(unix)]
    async fn launch_provider_worker(
        &self,
        handle: &ContainerHandle,
        profile: &crate::marketplace::provider::WorkerProfile,
        attempt: crate::marketplace::provider::WorkerStartAttempt,
    ) -> Result<PrivateProcessStdio> {
        self.launch_provider_worker_actor(handle, profile, attempt)
            .await
    }
    #[cfg(unix)]
    async fn inspect_unstarted_provider_worker(
        &self,
        profile: &crate::marketplace::provider::WorkerProfile,
    ) -> Result<ContainerHandle> {
        self.inspect_unstarted_provider_worker_actor(profile).await
    }
    #[cfg(unix)]
    async fn stop_provider_worker(
        &self,
        handle: &ContainerHandle,
        profile: &crate::marketplace::provider::WorkerProfile,
    ) -> Result<()> {
        self.stop_provider_worker_actor(handle, profile).await
    }
    #[cfg(unix)]
    async fn remove_provider_worker(
        &self,
        handle: &ContainerHandle,
        profile: &crate::marketplace::provider::WorkerProfile,
    ) -> Result<()> {
        self.remove_provider_worker_actor(handle, profile).await
    }
    #[cfg(unix)]
    async fn create_provider_guard(
        &self,
        profile: &crate::marketplace::provider::GuardProfile,
        attempt: crate::marketplace::provider::GuardCreateAttempt,
    ) -> Result<crate::runtime::provider_creation::GuardCreation> {
        self.create_provider_guard_actor(profile, attempt).await
    }
    #[cfg(unix)]
    async fn launch_provider_guard(
        &self,
        handle: &ContainerHandle,
        profile: &crate::marketplace::provider::GuardProfile,
        attempt: crate::marketplace::provider::GuardStartAttempt,
    ) -> Result<PrivateProcessStdio> {
        self.launch_provider_guard_actor(handle, profile, attempt)
            .await
    }
    #[cfg(unix)]
    async fn inspect_provider_guard(
        &self,
        handle: &ContainerHandle,
        profile: &crate::marketplace::provider::GuardProfile,
    ) -> Result<PrivateActorInspection> {
        self.inspect_provider_guard_actor(handle, profile).await
    }
    #[cfg(unix)]
    async fn inspect_unstarted_provider_guard(
        &self,
        profile: &crate::marketplace::provider::GuardProfile,
    ) -> Result<ContainerHandle> {
        self.inspect_unstarted_provider_guard_actor(profile).await
    }
    #[cfg(unix)]
    async fn stop_provider_guard(
        &self,
        handle: &ContainerHandle,
        profile: &crate::marketplace::provider::GuardProfile,
    ) -> Result<()> {
        self.stop_provider_guard_actor(handle, profile).await
    }
    #[cfg(unix)]
    async fn remove_provider_guard(
        &self,
        handle: &ContainerHandle,
        profile: &crate::marketplace::provider::GuardProfile,
    ) -> Result<()> {
        self.remove_provider_guard_actor(handle, profile).await
    }
    #[cfg(unix)]
    async fn create_provider(
        &self,
        profile: &crate::marketplace::provider::Profile,
        attempt: crate::marketplace::provider::CreateAttempt,
    ) -> Result<crate::runtime::provider_creation::ProviderCreation> {
        self.create_provider_actor(profile, attempt).await
    }
    #[cfg(unix)]
    async fn launch_provider(
        &self,
        handle: &ContainerHandle,
        profile: &crate::marketplace::provider::Profile,
        attempt: crate::marketplace::provider::StartAttempt,
    ) -> Result<PrivateProcessStdio> {
        self.launch_provider_actor(handle, profile, attempt).await
    }
    #[cfg(unix)]
    async fn inspect_provider(
        &self,
        handle: &ContainerHandle,
        profile: &crate::marketplace::provider::Profile,
    ) -> Result<ContainerState> {
        self.inspect_provider_actor(handle, profile).await
    }
    #[cfg(unix)]
    async fn inspect_unstarted_provider(
        &self,
        profile: &crate::marketplace::provider::Profile,
    ) -> Result<ContainerHandle> {
        self.inspect_unstarted_provider_actor(profile).await
    }
    #[cfg(unix)]
    async fn stop_provider(
        &self,
        handle: &ContainerHandle,
        profile: &crate::marketplace::provider::Profile,
    ) -> Result<()> {
        self.stop_provider_actor(handle, profile).await
    }
    #[cfg(unix)]
    async fn remove_provider(
        &self,
        handle: &ContainerHandle,
        profile: &crate::marketplace::provider::Profile,
    ) -> Result<()> {
        self.remove_provider_actor(handle, profile).await
    }
    #[cfg(unix)]
    async fn create_application_gateway(
        &self,
        profile: &crate::marketplace::application_egress::ApplicationGatewayProfile,
        attempt: crate::marketplace::application_egress::GatewayCreateAttempt,
    ) -> Result<crate::runtime::gateway_creation::GatewayCreation> {
        self.create_application_gateway_actor(profile, attempt)
            .await
    }
    #[cfg(unix)]
    async fn start_application_gateway(
        &self,
        handle: &ContainerHandle,
        profile: &crate::marketplace::application_egress::ApplicationGatewayProfile,
    ) -> Result<()> {
        self.start_application_gateway_actor(handle, profile).await
    }
    #[cfg(unix)]
    async fn inspect_unstarted_application_gateway(
        &self,
        profile: &crate::marketplace::application_egress::ApplicationGatewayProfile,
    ) -> Result<ContainerHandle> {
        self.inspect_unstarted_application_gateway_actor(profile)
            .await
    }
    #[cfg(unix)]
    async fn attach_application_gateway(
        &self,
        handle: &ContainerHandle,
        profile: &crate::marketplace::application_egress::ApplicationGatewayProfile,
    ) -> Result<PrivateProcessStdio> {
        self.attach_application_gateway_actor(handle, profile).await
    }
    #[cfg(unix)]
    async fn stop_application_gateway(
        &self,
        handle: &ContainerHandle,
        profile: &crate::marketplace::application_egress::ApplicationGatewayProfile,
    ) -> Result<()> {
        self.stop_application_gateway_actor(handle, profile).await
    }
    #[cfg(unix)]
    async fn observe_application_gateway(
        &self,
        handle: &ContainerHandle,
        profile: &crate::marketplace::application_egress::ApplicationGatewayProfile,
    ) -> Result<crate::marketplace::application_egress::ApplicationGatewayObservation> {
        self.observe_application_gateway_actor(handle, profile, None)
            .await
    }
    #[cfg(unix)]
    async fn observe_unchanged_application_gateway(
        &self,
        handle: &ContainerHandle,
        profile: &crate::marketplace::application_egress::ApplicationGatewayProfile,
        original: &crate::marketplace::application_egress::ApplicationGatewayObservation,
    ) -> Result<crate::marketplace::application_egress::ApplicationGatewayObservation> {
        self.observe_application_gateway_actor(handle, profile, Some(original))
            .await
    }
    #[cfg(unix)]
    async fn remove_application_gateway(
        &self,
        handle: &ContainerHandle,
        profile: &crate::marketplace::application_egress::ApplicationGatewayProfile,
    ) -> Result<()> {
        self.remove_application_gateway_actor(handle, profile).await
    }
    async fn create_application_http(
        &self,
        profile: &crate::marketplace::runtime::application_http::ApplicationHttpProfile,
        attempt: crate::marketplace::runtime::application_http::HttpCreateAttempt,
    ) -> Result<crate::runtime::http_creation::HttpCreation> {
        self.create_application_http_actor(profile, attempt).await
    }
    async fn start_application_http(
        &self,
        handle: &ContainerHandle,
        profile: &crate::marketplace::runtime::application_http::ApplicationHttpProfile,
    ) -> Result<()> {
        self.start_application_http_actor(handle, profile).await
    }
    async fn inspect_unstarted_application_http(
        &self,
        profile: &crate::marketplace::runtime::application_http::ApplicationHttpProfile,
    ) -> Result<ContainerHandle> {
        self.inspect_unstarted_application_http_actor(profile).await
    }
    async fn attach_application_http(
        &self,
        handle: &ContainerHandle,
        profile: &crate::marketplace::runtime::application_http::ApplicationHttpProfile,
    ) -> Result<PrivateProcessStdio> {
        self.attach_application_http_actor(handle, profile).await
    }
    async fn confirm_application_http_success(
        &self,
        handle: &ContainerHandle,
        profile: &crate::marketplace::runtime::application_http::ApplicationHttpProfile,
    ) -> Result<()> {
        self.confirm_application_http_actor(handle, profile).await
    }
    async fn stop_application_http(
        &self,
        handle: &ContainerHandle,
        profile: &crate::marketplace::runtime::application_http::ApplicationHttpProfile,
    ) -> Result<()> {
        self.stop_application_http_actor(handle, profile).await
    }
    async fn remove_application_http(
        &self,
        handle: &ContainerHandle,
        profile: &crate::marketplace::runtime::application_http::ApplicationHttpProfile,
    ) -> Result<()> {
        self.remove_application_http_actor(handle, profile).await
    }
    async fn create_host_measurement(
        &self,
        spec: &crate::runtime::measurement::MeasurementSpec,
    ) -> Result<ContainerHandle> {
        self.create_measurement_actor(spec).await
    }
    async fn start_host_measurement(
        &self,
        handle: &ContainerHandle,
        spec: &crate::runtime::measurement::MeasurementSpec,
    ) -> Result<()> {
        self.start_measurement_actor(handle, spec).await
    }
    async fn confirm_host_measurement_success(
        &self,
        handle: &ContainerHandle,
        spec: &crate::runtime::measurement::MeasurementSpec,
    ) -> Result<()> {
        let value = self.inspect_container(&handle.id).await?;
        Self::require_measurement_actor(
            &value,
            &ContainerHandle {
                id: handle.id.clone(),
                name: handle.id.clone(),
            },
        )?;
        Self::verify_private_native_exit(&value, handle, &spec.spec().labels)
    }
    async fn stop_host_measurement(&self, handle: &ContainerHandle) -> Result<()> {
        self.stop_measurement_actor(handle).await
    }
    async fn inspect_packet_uplink(
        &self,
        reference: &str,
    ) -> Result<Option<PacketNetworkInspection>> {
        if !immutable_id(reference)
            && (!reference.starts_with("elixir-marketplace-uplink-")
                || reference.len() > 100
                || !reference
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'))
        {
            bail!("invalid marketplace network reference");
        }
        let args = vec![
            "network".to_owned(),
            "inspect".to_owned(),
            "--format".to_owned(),
            "{{json .}}".to_owned(),
            reference.to_owned(),
        ];
        let (status, out, err) = self.packet_network_capture(&args).await?;
        if !status.success() {
            if err.contains("No such network")
                || err.contains(&format!("network {reference} not found"))
            {
                return Ok(None);
            }
            bail!("marketplace network inspection failed");
        }
        let actual = PacketNetworkInspection::parse(&serde_json::from_str(&out)?)?;
        if (immutable_id(reference) && actual.id != reference)
            || (!immutable_id(reference) && actual.name != reference)
        {
            bail!("marketplace network identity differs");
        }
        Ok(Some(actual))
    }
    async fn ensure_packet_uplink(
        &self,
        spec: &PacketNetworkSpec,
    ) -> Result<PacketNetworkInspection> {
        if let Some(actual) = self.inspect_packet_uplink(spec.name()).await? {
            spec.verify(&actual)?;
            return Ok(actual);
        }
        let mut args = vec![
            "network".to_owned(),
            "create".to_owned(),
            "--driver=bridge".to_owned(),
            "--ipv4=true".to_owned(),
            format!("--ipv6={}", spec.ipv6()),
        ];
        if spec.internal() {
            args.push("--internal".to_owned());
        }
        for (key, value) in spec.options() {
            args.extend(["--opt".to_owned(), format!("{key}={value}")]);
        }
        let mut labels = spec.labels().into_iter().collect::<Vec<_>>();
        labels.sort();
        for (key, value) in labels {
            args.extend(["--label".to_owned(), format!("{key}={value}")]);
        }
        args.push(spec.name().to_owned());
        let (status, out, _) = self.packet_network_capture(&args).await?;
        if !status.success() {
            bail!("marketplace network creation failed; no replacement attempted");
        }
        let id = out.trim();
        if !immutable_id(id) {
            bail!("marketplace network creation omitted immutable ID");
        }
        let actual = self
            .inspect_packet_uplink(id)
            .await?
            .context("created marketplace network missing")?;
        spec.verify(&actual)?;
        if !actual.members.is_empty() {
            bail!("created marketplace network has unexpected members");
        }
        Ok(actual)
    }
    async fn remove_packet_uplink(&self, spec: &PacketNetworkSpec, id: &str) -> Result<()> {
        if !immutable_id(id) {
            bail!("marketplace network removal requires immutable ID");
        }
        let Some(actual) = self.inspect_packet_uplink(id).await? else {
            return Ok(());
        };
        spec.verify(&actual)?;
        if !actual.members.is_empty() {
            bail!("cannot remove an occupied marketplace uplink");
        }
        let (status, _, _) = self
            .packet_network_capture(&["network".to_owned(), "rm".to_owned(), id.to_owned()])
            .await?;
        if !status.success() {
            bail!("marketplace network removal failed");
        }
        Ok(())
    }
    async fn create_isolated_packet_actor(&self, spec: &ContainerSpec) -> Result<ContainerHandle> {
        let mode = spec
            .network_mode
            .as_deref()
            .context("packet actor requires explicit namespace")?;
        if mode != "none" {
            if !immutable_id(mode)
                || spec
                    .labels
                    .get("elixir.marketplace.packet.uplink")
                    .map(String::as_str)
                    != Some(mode)
            {
                bail!("packet actor requires its immutable owned uplink");
            }
            let actual = self
                .inspect_packet_uplink(mode)
                .await?
                .context("packet uplink missing")?;
            let deployment = spec
                .labels
                .get(ELIXIR_DEPLOYMENT_ID_LABEL)
                .context("missing uplink deployment")?;
            PacketNetworkSpec::new(deployment, actual.ipv6, actual.internal)?.verify(&actual)?;
            if !actual.members.is_empty() {
                bail!("packet actor uplink is occupied");
            }
        }
        if spec
            .labels
            .get("elixir.marketplace.isolation")
            .map(String::as_str)
            != Some("packet-v1")
            || spec.labels.get(ELIXIR_MANAGED_LABEL).map(String::as_str) != Some("true")
        {
            bail!("packet actor creation requires the isolated managed profile");
        }
        // Never perform general conflicting-container cleanup or replacement.
        // A racing create/name collision fails; the caller owns recovery.
        self.create_container(spec).await
    }

    async fn create_quarantine_marketplace_writer(
        &self,
        spec: &ContainerSpec,
    ) -> Result<ContainerHandle> {
        Self::require_source_writer_spec(spec)?;
        #[cfg(unix)]
        if crate::marketplace::runtime::container_execution::is_command(&spec.command) {
            crate::marketplace::runtime::container_execution::require_guardian(spec,
                self.container_guardian.as_deref().context("compiled guardian is not independently configured")?)?;
        }
        // No general conflict cleanup, implicit restart, image pull or network
        // creation. Reuse this attempt's name only after positive stopped and
        // managed-role inspection of the previous immutable writer.
        if let Some(old) = self.get_container_handle(&spec.name).await? {
            let value = self.inspect_container(&old.id).await?;
            let identity = ContainerHandle {
                id: old.id.clone(),
                name: old.id.clone(),
            };
            let state = Self::require_quarantine_writer(&value, &identity)?;
            let labels = Self::extract_labels(&value);
            if state.name != spec.name
                || state.running
                || labels.get(ELIXIR_DEPLOYMENT_ID_LABEL)
                    != spec.labels.get(ELIXIR_DEPLOYMENT_ID_LABEL)
            {
                bail!("source create refuses a running or different owned writer");
            }
            self.remove_container(&identity).await?;
        }
        // Return the CID immediately, before further inspection can fail.
        #[cfg(unix)]
        if crate::marketplace::runtime::container_execution::is_command(&spec.command) {
            return self.create_container_entrypoint(spec, false, Some(elixir_marketplace::container_execution::GUARDIAN_PATH)).await;
        }
        self.create_container_mode(spec, false).await
    }

    async fn start_quarantine_marketplace_writer(
        &self,
        handle: &ContainerHandle,
        spec: &ContainerSpec,
        limits: &crate::marketplace::runtime::SourceJobLimits,
    ) -> Result<()> {
        Self::require_source_writer_spec(spec)?;
        #[cfg(unix)]
        if crate::marketplace::runtime::container_execution::is_command(&spec.command) {
            crate::marketplace::runtime::container_execution::require_guardian(spec,
                self.container_guardian.as_deref().context("compiled guardian is not independently configured")?)?;
        }
        limits.validate()?;
        let value = self.inspect_container(&handle.id).await?;
        Self::verify_unstarted_source_writer(&value, handle, spec, limits)?;
        self.run_capture(&["start".to_owned(), handle.id.clone()])
            .await?;
        Ok(())
    }
    async fn inspect_unstarted_quarantine_marketplace_writer(
        &self,
        intent: &crate::marketplace::runtime::source_writer::SourceWriterIntent,
    ) -> Result<ContainerHandle> {
        self.inspect_unstarted_source_creation(intent).await
    }
    async fn inspect_private_actor(
        &self,
        handle: &ContainerHandle,
    ) -> Result<PrivateActorInspection> {
        if handle.id.len() != 64
            || !handle
                .id
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            bail!("private actor inspection requires an immutable container ID");
        }
        let value = self.inspect_container(&handle.id).await?;
        let result = Self::extract_private_actor(&value)?;
        if result.id != handle.id || result.runtime.name != handle.name {
            bail!("private actor identity changed");
        }
        Ok(result)
    }
    async fn image_labels(&self, image: &str) -> Result<HashMap<String, String>> {
        if !image_labels::immutable_reference(image) {
            return Ok(self.inspect_image_metadata(image).await?.labels);
        }
        // Only image configuration is immutable. This is deliberately not a
        // cache of running containers, native profiles, ownership or authority.
        // Serialize a miss so concurrent source/HTTP owners share one query.
        let mut cache = self.immutable_image_labels.lock().await;
        if let Some(labels) = cache.get(image) {
            return Ok(labels);
        }
        let actual = self.inspect_image_value(image).await?;
        image_labels::require_identity(image, &actual)?;
        let labels = Self::image_metadata(&actual).labels;
        cache.insert(image, &labels);
        Ok(labels)
    }
    async fn local_image_id(&self, reference: &str) -> Result<String> {
        if reference.is_empty()
            || reference.len() > 512
            || reference.starts_with('-')
            || !reference.is_ascii()
            || reference
                .bytes()
                .any(|b| b.is_ascii_control() || b.is_ascii_whitespace())
            || (reference.starts_with("sha256:")
                && !reference.strip_prefix("sha256:").is_some_and(immutable_id))
        {
            bail!("invalid local platform image reference");
        }
        let output = self
            .run_stdout_with_timeout(
                &[
                    "image".into(),
                    "inspect".into(),
                    "--format".into(),
                    "{{.Id}}".into(),
                    "--".into(),
                    reference.into(),
                ],
                DOCKER_PROBE_COMMAND_TIMEOUT,
            )
            .await?;
        let id = output.trim();
        if !id
            .strip_prefix("sha256:")
            .is_some_and(crate::runtime::packet_network::immutable_id)
            || (reference.starts_with("sha256:") && reference != id)
        {
            bail!("local platform image identity is not exact");
        }
        Ok(id.to_owned())
    }

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
        // Validate before any network creation or managed-container replacement.
        container_resource_args(&spec.security)?;
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
            let labels = Self::extract_labels(&inspect);
            Self::ensure_existing_container_is_mutable_by_spec(&spec.name, &labels, spec)?;
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

    async fn create_private_file_volume(&self, spec: &PrivateFileVolumeSpec) -> Result<()> {
        Self::validate_private_file_volume_spec(spec)?;
        if self.inspect_named_volume(&spec.name).await?.is_some() {
            bail!("private file volume '{}' already exists", spec.name);
        }

        let mut create_args = vec!["volume".to_string(), "create".to_string()];
        let mut labels = spec.labels.iter().collect::<Vec<_>>();
        labels.sort_by(|left, right| left.0.cmp(right.0));
        for (key, value) in labels {
            create_args.push("--label".to_string());
            create_args.push(format!("{key}={value}"));
        }
        create_args.push(spec.name.clone());
        let created = self.run_stdout(&create_args).await?;
        if created.trim() != spec.name {
            bail!("Docker created an unexpected private volume identity");
        }
        let Some(created_volume) = self.inspect_named_volume(&spec.name).await? else {
            bail!("private file volume disappeared after creation");
        };
        if !Self::private_volume_labels_match(&created_volume, &spec.labels) {
            bail!("private file volume ownership labels do not match");
        }

        let source_mount = format!("{}:/run/elixir-seed/input:ro", spec.source_path);
        let volume_mount = format!("{}:/run/elixir-seed/output", spec.name);
        let seed_script = concat!(
            "set -eu; ",
            "destination=\"/run/elixir-seed/output/$1\"; ",
            "test ! -e \"$destination\"; ",
            "umask 077; ",
            "cp /run/elixir-seed/input \"$destination.tmp\"; ",
            "chmod 0600 \"$destination.tmp\"; ",
            "mv \"$destination.tmp\" \"$destination\"; ",
            "sync -f \"$destination\"; ",
            "chmod 0700 /run/elixir-seed/output; ",
            "chown \"$2:$3\" \"$destination\" /run/elixir-seed/output"
        );
        let seed_args = vec![
            "run".to_string(),
            "--rm".to_string(),
            "--pull".to_string(),
            "never".to_string(),
            "--network".to_string(),
            "none".to_string(),
            "--read-only".to_string(),
            "--cap-drop".to_string(),
            "ALL".to_string(),
            "--cap-add".to_string(),
            "CHOWN".to_string(),
            "--security-opt".to_string(),
            "no-new-privileges".to_string(),
            "--pids-limit".to_string(),
            "32".to_string(),
            "--memory".to_string(),
            "64m".to_string(),
            "--user".to_string(),
            "0:0".to_string(),
            "--tmpfs".to_string(),
            "/tmp:size=16m".to_string(),
            "--label".to_string(),
            format!("{ELIXIR_MANAGED_LABEL}=true"),
            "--label".to_string(),
            "elixir.runtime.role=private_file_volume_seed".to_string(),
            "-v".to_string(),
            source_mount,
            "-v".to_string(),
            volume_mount,
            "--entrypoint".to_string(),
            "/bin/sh".to_string(),
            spec.image.clone(),
            "-c".to_string(),
            seed_script.to_string(),
            "--".to_string(),
            spec.file_name.clone(),
            spec.owner_uid.to_string(),
            spec.owner_gid.to_string(),
        ];
        if let Err(seed_error) = self.run_capture(&seed_args).await {
            let cleanup = self
                .remove_private_file_volume(&spec.name, &spec.labels)
                .await;
            return match cleanup {
                Ok(()) => Err(seed_error.context("seeding private file volume")),
                Err(cleanup_error) => Err(anyhow::anyhow!(
                    "seeding private file volume failed: {seed_error}; cleanup failed: {cleanup_error}"
                )),
            };
        }
        Ok(())
    }

    async fn ensure_owned_directory_volume(&self, spec: &OwnedDirectoryVolumeSpec) -> Result<()> {
        Self::validate_owned_directory_volume_spec(spec)?;
        let existing = self.inspect_named_volume(&spec.name).await?;
        let created = existing.is_none();
        if let Some(volume) = existing {
            if !Self::private_volume_labels_match(&volume, &spec.labels) {
                bail!("owned directory volume ownership labels do not match");
            }
        } else {
            let mut create_args = vec!["volume".to_string(), "create".to_string()];
            let mut labels = spec.labels.iter().collect::<Vec<_>>();
            labels.sort_by(|left, right| left.0.cmp(right.0));
            for (key, value) in labels {
                create_args.push("--label".to_string());
                create_args.push(format!("{key}={value}"));
            }
            create_args.push(spec.name.clone());
            let created_name = self.run_stdout(&create_args).await?;
            if created_name.trim() != spec.name {
                bail!("Docker created an unexpected owned volume identity");
            }
            let Some(volume) = self.inspect_named_volume(&spec.name).await? else {
                bail!("owned directory volume disappeared after creation");
            };
            if !Self::private_volume_labels_match(&volume, &spec.labels) {
                bail!("owned directory volume ownership labels do not match");
            }
        }

        let initialize_script = concat!(
            "set -eu; ",
            "root=/run/elixir-owned-volume; ",
            "entry=\"$(find \"$root\" -mindepth 1 -maxdepth 1 -print -quit)\"; ",
            "if [ -z \"$entry\" ]; then ",
            "chmod 0700 \"$root\"; chown \"$1:$2\" \"$root\"; ",
            "else test \"$(stat -c '%u:%g:%a' \"$root\")\" = \"$1:$2:700\"; fi; ",
            "test \"$(stat -c '%u:%g:%a' \"$root\")\" = \"$1:$2:700\""
        );
        let initialize_args = vec![
            "run".to_string(),
            "--rm".to_string(),
            "--pull".to_string(),
            "never".to_string(),
            "--network".to_string(),
            "none".to_string(),
            "--read-only".to_string(),
            "--cap-drop".to_string(),
            "ALL".to_string(),
            "--cap-add".to_string(),
            "CHOWN".to_string(),
            "--cap-add".to_string(),
            "DAC_OVERRIDE".to_string(),
            "--security-opt".to_string(),
            "no-new-privileges".to_string(),
            "--pids-limit".to_string(),
            "32".to_string(),
            "--memory".to_string(),
            "64m".to_string(),
            "--user".to_string(),
            "0:0".to_string(),
            "--tmpfs".to_string(),
            "/tmp:size=16m".to_string(),
            "--label".to_string(),
            format!("{ELIXIR_MANAGED_LABEL}=true"),
            "--label".to_string(),
            "elixir.runtime.role=owned_directory_volume_initialize".to_string(),
            "-v".to_string(),
            format!("{}:/run/elixir-owned-volume", spec.name),
            "--entrypoint".to_string(),
            "/bin/sh".to_string(),
            spec.image.clone(),
            "-c".to_string(),
            initialize_script.to_string(),
            "--".to_string(),
            spec.owner_uid.to_string(),
            spec.owner_gid.to_string(),
        ];
        if let Err(initialize_error) = self.run_capture(&initialize_args).await {
            if created {
                let cleanup = self
                    .remove_private_file_volume(&spec.name, &spec.labels)
                    .await;
                return match cleanup {
                    Ok(()) => Err(initialize_error.context("initializing owned directory volume")),
                    Err(cleanup_error) => Err(anyhow::anyhow!(
                        "initializing owned directory volume failed: {initialize_error}; cleanup failed: {cleanup_error}"
                    )),
                };
            }
            return Err(initialize_error.context("verifying owned directory volume"));
        }
        Ok(())
    }

    async fn ensure_fixed_marketplace_volume(
        &self,
        spec: &FixedMarketplaceVolumeSpec,
    ) -> Result<()> {
        Self::validate_fixed_marketplace_volume_spec(spec)?;
        let image = self.inspect_image_metadata(&spec.image).await?;
        if image
            .labels
            .get("elixir.marketplace.worker.schema")
            .map(String::as_str)
            != Some("1")
            || image
                .labels
                .get("elixir.marketplace.worker.fixed_state")
                .map(String::as_str)
                != Some("1")
        {
            bail!("immutable image does not declare the fixed marketplace storage contract");
        }
        if let Some(existing) = self.inspect_named_volume(&spec.name).await? {
            Self::verify_fixed_marketplace_volume(&existing, spec)?;
            return self.fixed_marketplace_volume_job(spec, false).await;
        }
        let mut args = vec![
            "volume".to_owned(),
            "create".to_owned(),
            "--driver=local".to_owned(),
        ];
        let mut labels = spec.labels.iter().collect::<Vec<_>>();
        labels.sort_by_key(|(key, _)| *key);
        for (key, value) in labels {
            args.extend(["--label".to_owned(), format!("{key}={value}")]);
        }
        args.push(spec.name.clone());
        if self.run_stdout(&args).await?.trim() != spec.name {
            bail!("unexpected marketplace volume identity");
        }
        let value = self
            .inspect_named_volume(&spec.name)
            .await?
            .context("marketplace volume disappeared after creation")?;
        Self::verify_fixed_marketplace_volume(&value, spec)?;
        // The initializer has an exclusive new-file claim. A concurrent creator
        // cannot overwrite another initializer's state. Failed/partial volumes
        // remain bounded and unavailable; never delete established state here.
        self.fixed_marketplace_volume_job(spec, true).await
    }

    async fn ensure_quarantine_marketplace_volume(
        &self,
        spec: &QuarantineMarketplaceVolumeSpec,
    ) -> Result<()> {
        Self::validate_quarantine_marketplace_volume_spec(spec)?;
        let image = self.inspect_image_metadata(&spec.image).await?;
        if image
            .labels
            .get("elixir.marketplace.worker.schema")
            .map(String::as_str)
            != Some("1")
            || image
                .labels
                .get("elixir.marketplace.worker.quarantine")
                .map(String::as_str)
                != Some("1")
        {
            bail!("immutable image does not declare the source quarantine contract");
        }
        // The caller holds the shared expensive-job lease and has stopped any
        // old writer before this method. Recover an interrupted root helper too.
        self.recover_quarantine_provisioner(spec).await?;
        if let Some(existing) = self.inspect_named_volume(&spec.name).await? {
            Self::verify_owned_local_volume(&existing, &spec.name, &spec.labels)?;
            return self.quarantine_marketplace_volume_job(spec, false).await;
        }
        let mut args = vec![
            "volume".to_owned(),
            "create".to_owned(),
            "--driver=local".to_owned(),
        ];
        let mut labels = spec.labels.iter().collect::<Vec<_>>();
        labels.sort_by_key(|(key, _)| *key);
        for (key, value) in labels {
            args.extend(["--label".to_owned(), format!("{key}={value}")]);
        }
        args.push(spec.name.clone());
        if self.run_stdout(&args).await?.trim() != spec.name {
            bail!("unexpected source quarantine volume identity");
        }
        let volume = self
            .inspect_named_volume(&spec.name)
            .await?
            .context("source quarantine volume disappeared")?;
        Self::verify_owned_local_volume(&volume, &spec.name, &spec.labels)?;
        self.quarantine_marketplace_volume_job(spec, true).await
    }

    async fn create_marketplace_build_output_volume(&self, slot: &str) -> Result<()> {
        self.create_build_output_volume(slot).await
    }
    async fn remove_marketplace_build_output_volume(&self, slot: &str) -> Result<()> {
        self.remove_build_output_volume(slot).await
    }
    async fn cleanup_idle_marketplace_build_output(&self, slot: &str) -> Result<()> {
        self.remove_build_output_volume(slot).await
    }

    async fn attach_private_stdio(
        &self,
        handle: &ContainerHandle,
        required: &HashMap<String, String>,
    ) -> Result<PrivateProcessStdio> {
        if handle.id.len() != 64
            || !handle.id.bytes().all(|b| b.is_ascii_hexdigit())
            || required.get(ELIXIR_MANAGED_LABEL).map(String::as_str) != Some("true")
            || !matches!(
                required
                    .get("elixir.marketplace.isolation")
                    .map(String::as_str),
                Some(
                    "fixed-v1"
                        | "quarantine-v1"
                        | "packet-v1"
                        | "connectivity-v1"
                        | "measurement-v1"
                        | "application-http-v1"
                        | "application-gateway-v1"
                )
            )
            || !required.contains_key(CONTAINER_SPEC_HASH_LABEL)
        {
            bail!("private streams require a specific managed marketplace worker");
        }
        let value = self.inspect_container(&handle.id).await?;
        let state = Self::extract_state(&value)?;
        let labels = Self::extract_labels(&value);
        if state.id != handle.id
            || state.name != handle.name
            || !state.running
            || required
                .iter()
                .any(|(key, value)| labels.get(key) != Some(value))
            || value.pointer("/Config/OpenStdin").and_then(Value::as_bool) != Some(true)
            || value.pointer("/Config/Tty").and_then(Value::as_bool) != Some(false)
        {
            bail!("private process ownership or stream configuration differs");
        }
        let mut child = self.docker_command().await?
            .args(["attach", "--sig-proxy=false", &handle.id])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        Ok(PrivateProcessStdio {
            input: child.stdin.take().context("missing private stdin")?,
            output: child.stdout.take().context("missing private stdout")?,
            diagnostics: Some(child.stderr.take().context("missing private diagnostics")?),
            child,
        })
    }

    async fn confirm_quarantine_marketplace_success(
        &self,
        handle: &ContainerHandle,
        required: &HashMap<String, String>,
    ) -> Result<()> {
        Self::require_quarantine_exit_owner(handle, required)?;
        let value = self.inspect_container(&handle.id).await?;
        Self::verify_quarantine_exit(&value, handle, required)
    }

    async fn exec_marketplace_connectivity_probe(
        &self,
        handle: &ContainerHandle,
        required: &HashMap<String, String>,
    ) -> Result<PrivateProcessStdio> {
        self.exec_independent_marketplace_fetch(
            handle,
            required,
            IndependentMarketplaceFetch::Connectivity,
        )
        .await
    }

    async fn exec_marketplace_source_preflight(
        &self,
        handle: &ContainerHandle,
        required: &HashMap<String, String>,
    ) -> Result<PrivateProcessStdio> {
        self.exec_independent_marketplace_fetch(
            handle,
            required,
            IndependentMarketplaceFetch::SourcePreflight,
        )
        .await
    }
    async fn exec_marketplace_radicle_source(
        &self,
        handle: &ContainerHandle,
        required: &HashMap<String, String>,
    ) -> Result<PrivateProcessStdio> {
        self.exec_independent_marketplace_fetch(
            handle,
            required,
            IndependentMarketplaceFetch::RadicleSource,
        )
        .await
    }

    async fn exec_marketplace_sdk_http(
        &self,
        handle: &ContainerHandle,
        required: &HashMap<String, String>,
    ) -> Result<PrivateProcessStdio> {
        self.exec_independent_marketplace_fetch(
            handle,
            required,
            IndependentMarketplaceFetch::SdkHttp,
        )
        .await
    }

    async fn exec_marketplace_sdk_http_stream(
        &self,
        handle: &ContainerHandle,
        required: &HashMap<String, String>,
    ) -> Result<PrivateProcessStdio> {
        self.exec_independent_marketplace_fetch(
            handle,
            required,
            IndependentMarketplaceFetch::SdkHttpStream,
        )
        .await
    }

    async fn exec_marketplace_metadata_verifier(
        &self,
        handle: &ContainerHandle,
        required: &HashMap<String, String>,
        issuer_keys: &[elixir_marketplace::identity::PublicKey],
    ) -> Result<PrivateProcessStdio> {
        self.exec_independent_marketplace_fetch(
            handle,
            required,
            IndependentMarketplaceFetch::Metadata(issuer_keys),
        )
        .await
    }
    async fn exec_marketplace_security_verifier(
        &self,
        handle: &ContainerHandle,
        required: &HashMap<String, String>,
        issuer_keys: &[elixir_marketplace::identity::PublicKey],
    ) -> Result<PrivateProcessStdio> {
        self.exec_independent_marketplace_fetch(
            handle,
            required,
            IndependentMarketplaceFetch::Security(issuer_keys),
        )
        .await
    }

    async fn exec_marketplace_evidence_verifier(
        &self,
        handle: &ContainerHandle,
        required: &HashMap<String, String>,
        issuer_keys: &[elixir_marketplace::identity::PublicKey],
    ) -> Result<PrivateProcessStdio> {
        self.exec_independent_marketplace_fetch(
            handle,
            required,
            IndependentMarketplaceFetch::Evidence(issuer_keys),
        )
        .await
    }

    async fn source_cache_state(
        &self,
        name: &str,
        required_labels: &HashMap<String, String>,
    ) -> Result<crate::runtime::model::SourceCacheState> {
        use crate::runtime::model::SourceCacheState;
        let Some(volume) = self.inspect_named_volume(name).await? else {
            return Ok(SourceCacheState::Absent);
        };
        Self::verify_owned_local_volume(&volume, name, required_labels)?;
        let label = volume
            .get("Labels")
            .and_then(|labels| labels.get("elixir.marketplace.source_cache"))
            .and_then(Value::as_str)
            .filter(|value| value.len() == 71)
            .filter(|value| {
                value
                    .parse::<elixir_marketplace::identity::Digest>()
                    .is_ok()
            })
            .map(str::to_owned);
        Ok(SourceCacheState::Present(label))
    }

    async fn private_file_volume_owned(
        &self,
        name: &str,
        required_labels: &HashMap<String, String>,
    ) -> Result<bool> {
        let Some(volume) = self.inspect_named_volume(name).await? else {
            return Ok(false);
        };
        if !Self::private_volume_labels_match(&volume, required_labels) {
            bail!("private file volume ownership labels do not match");
        }
        Ok(true)
    }

    async fn remove_private_file_volume(
        &self,
        name: &str,
        required_labels: &HashMap<String, String>,
    ) -> Result<()> {
        if !self
            .private_file_volume_owned(name, required_labels)
            .await?
        {
            return Ok(());
        }
        self.run_capture(&["volume".to_string(), "rm".to_string(), name.to_string()])
            .await?;
        if self.inspect_named_volume(name).await?.is_some() {
            bail!("private file volume still exists after removal");
        }
        Ok(())
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

    async fn exec_container_command(
        &self,
        handle: &ContainerHandle,
        user: &str,
        command: &[String],
    ) -> Result<String> {
        if user.trim().is_empty() {
            bail!("container command user is required");
        }
        if command.is_empty() || command.iter().any(|value| value.is_empty()) {
            bail!("container command is required");
        }
        let mut args = vec![
            "exec".to_string(),
            "--user".to_string(),
            user.to_string(),
            handle.id.clone(),
        ];
        args.extend(command.iter().cloned());
        Ok(self
            .run_capture_with_timeout(&args, Some(DOCKER_PROBE_COMMAND_TIMEOUT))
            .await?
            .stdout)
    }

    async fn stop_container(&self, handle: &ContainerHandle) -> Result<()> {
        let args = vec!["stop".to_string(), handle.id.clone()];
        self.run_capture(&args).await?;
        Ok(())
    }

    async fn stop_quarantine_marketplace_writer(&self, handle: &ContainerHandle) -> Result<()> {
        if !immutable_id(&handle.id) || handle.name != handle.id {
            bail!("immediate native stop requires an immutable container identity");
        }
        timeout(Duration::from_secs(15), async {
            let value = self.inspect_container(&handle.id).await?;
            if !Self::require_quarantine_writer(&value, handle)?.running {
                return Ok(());
            }
            // Offline verification has no approved application state to flush.
            // Partial quarantine bytes remain unapproved and must be rehashed.
            // Do not rely on TERM handling by a container's unattached PID 1.
            let attempt = self
                .run_capture_with_timeout(
                    &[
                        "kill".to_owned(),
                        "--signal=KILL".to_owned(),
                        handle.id.clone(),
                    ],
                    Some(DOCKER_PROBE_COMMAND_TIMEOUT),
                )
                .await;
            let value = self.inspect_container(&handle.id).await?;
            if Self::require_quarantine_writer(&value, handle)?.running {
                attempt?;
                bail!("native quarantine writer remains running after immediate stop");
            }
            // A racing natural exit is also success, but only after positively
            // inspecting this exact owned identity as stopped, never by text.
            Ok(())
        })
        .await
        .context("immediate native quarantine stop deadline exceeded")?
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
        let args = vec!["rm".to_string(), "-f".to_string(), handle.id.clone()];
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

fn format_tmpfs(tmpfs: &ContainerTmpfsMount) -> String {
    let path = tmpfs.path.trim();
    let mut options = Vec::new();
    if let Some(size) = tmpfs.size_mb {
        options.push(format!("size={size}m"));
    }
    if let Some(uid) = tmpfs.owner_uid {
        options.push(format!("uid={uid}"));
    }
    if let Some(gid) = tmpfs.owner_gid {
        options.push(format!("gid={gid}"));
    }
    if let Some(mode) = tmpfs.mode {
        options.push(format!("mode={mode:o}"));
    }
    if tmpfs.no_exec {
        options.push("noexec".to_string());
    } else if matches!(path, "/build-source" | "/scratch") {
        // Builder profiles require an explicit kernel exec option; omission
        // may retain Docker's noexec tmpfs default. Other profiles are unchanged.
        options.push("exec".to_string());
    }
    if tmpfs.no_suid {
        options.push("nosuid".to_string());
    }
    if tmpfs.no_dev {
        options.push("nodev".to_string());
    }
    if options.is_empty() {
        path.to_string()
    } else {
        format!("{path}:{}", options.join(","))
    }
}

fn container_resource_args(security: &ContainerSecurityOptions) -> Result<Vec<String>> {
    let mut args = Vec::new();
    if security.init_process {
        args.push("--init=true".to_owned());
    }
    if let Some(swap) = security.memory_swap_limit_mb {
        let memory = security
            .memory_limit_mb
            .filter(|value| *value > 0)
            .context("memory-swap requires a positive memory limit")?;
        if swap < memory || swap > (i64::MAX as u64) / (1024 * 1024) {
            bail!("invalid total memory-plus-swap limit");
        }
        args.extend(["--memory-swap".to_string(), format!("{swap}m")]);
    }
    if let Some(size) = security.shared_memory_mb {
        if size == 0 || size > (i64::MAX as u64) / (1024 * 1024) {
            bail!("invalid shared-memory limit");
        }
        args.extend(["--shm-size".to_string(), format!("{size}m")]);
    }
    if security.disable_core_dumps {
        args.extend(["--ulimit".to_string(), "core=0:0".to_string()]);
    }
    if let Some(limit) = security.open_files_limit {
        if !(16..=1_048_576).contains(&limit) {
            bail!("invalid container open-file limit");
        }
        args.extend(["--ulimit".to_owned(), format!("nofile={limit}:{limit}")]);
    }
    if let Some(limit) = security.file_size_limit_bytes {
        // RLIMIT_FSIZE=0 is the strictest valid limit: deny regular-file writes.
        if limit > i64::MAX as u64 {
            bail!("invalid container file-size limit");
        }
        args.extend(["--ulimit".to_owned(), format!("fsize={limit}:{limit}")]);
    }
    if security.stdin_open {
        args.push("--interactive".to_owned());
    }
    match &security.log_options {
        Some(ContainerLogOptions::Disabled) => {
            args.extend(["--log-driver".to_string(), "none".to_string()])
        }
        Some(ContainerLogOptions::BoundedJson {
            max_size_mb,
            max_files,
        }) => {
            if *max_size_mb == 0 || *max_files == 0 {
                bail!("bounded container logs require positive size and file limits");
            }
            args.extend([
                "--log-driver".to_string(),
                "json-file".to_string(),
                "--log-opt".to_string(),
                format!("max-size={max_size_mb}m"),
                "--log-opt".to_string(),
                format!("max-file={max_files}"),
            ]);
        }
        None => {}
    }
    for tmpfs in &security.tmpfs {
        // A sticky temporary directory is valid (for example /tmp=1777).
        // Never accept setuid, setgid or bits outside the permission/sticky mask.
        if tmpfs.mode.is_some_and(|mode| mode & !0o1777 != 0) {
            bail!("invalid tmpfs permission mode");
        }
        if tmpfs.size_mb == Some(0) {
            bail!("tmpfs size must be positive");
        }
        if (tmpfs.owner_uid.is_some()
            || tmpfs.owner_gid.is_some()
            || tmpfs.mode.is_some()
            || tmpfs.no_exec
            || tmpfs.no_suid
            || tmpfs.no_dev)
            && (tmpfs.path.is_empty()
                || !tmpfs.path.starts_with('/')
                || tmpfs.path.contains([':', ',', '\n', '\r']))
        {
            bail!("invalid hardened tmpfs path");
        }
    }
    Ok(args)
}

fn format_port(port: &PortMapping) -> Option<String> {
    let host = port.host_port?;
    let container = port.container_port;
    let proto = port
        .protocol
        .as_deref()
        .map(|p| format!("/{p}"))
        .unwrap_or_default();
    match port.host_ip.as_deref() {
        Some(host_ip) => Some(format!("{host_ip}:{host}:{container}{proto}")),
        None => Some(format!("{host}:{container}{proto}")),
    }
}

#[cfg(all(test, unix))]
mod application_gateway_tests;
#[cfg(test)]
mod local_image_tests;
#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::model::ContainerSecurityOptions;
    use serde_json::json;
    use sha2::{Digest as _, Sha256};

    #[test]
    fn native_quarantine_success_requires_owned_immutable_identity_and_natural_exit() -> Result<()>
    {
        let handle = ContainerHandle {
            id: "a".repeat(64),
            name: "native-exit-test".into(),
        };
        let labels = HashMap::from([
            (ELIXIR_MANAGED_LABEL.into(), "true".into()),
            (
                "elixir.marketplace.isolation".into(),
                "quarantine-v1".into(),
            ),
            (CONTAINER_SPEC_HASH_LABEL.into(), "test-only".into()),
        ]);
        let good = json!({"Id":handle.id,"Name":format!("/{}",handle.name),"Config":{"Labels":labels,"OpenStdin":true,"Tty":false},
            "State":{"Status":"exited","Running":false,"Paused":false,"Restarting":false,"OOMKilled":false,"Dead":false,"Pid":0,"ExitCode":0,"Error":""}});
        DockerRuntimeManager::verify_quarantine_exit(&good, &handle, &labels)?;
        for (path, bad) in [
            ("/Id", json!("b".repeat(64))),
            ("/Name", json!("/foreign")),
            ("/Config/OpenStdin", json!(false)),
            ("/Config/Tty", json!(true)),
            ("/Config/Labels/elixir.managed", json!("false")),
            ("/State/Status", json!("created")),
            ("/State/ExitCode", json!(1)),
            ("/State/ExitCode", Value::Null),
            ("/State/Pid", json!(42)),
            ("/State/Error", json!("launch failed")),
            ("/State/Running", json!(true)),
            ("/State/Paused", json!(true)),
            ("/State/Restarting", json!(true)),
            ("/State/OOMKilled", json!(true)),
            ("/State/Dead", json!(true)),
        ] {
            let mut value = good.clone();
            *value
                .pointer_mut(path)
                .context("exit fixture field missing")? = bad;
            assert!(
                DockerRuntimeManager::verify_quarantine_exit(&value, &handle, &labels).is_err(),
                "{path}"
            );
        }
        assert!(
            DockerRuntimeManager::verify_quarantine_exit(&good, &handle, &HashMap::new()).is_err()
        );
        Ok(())
    }

    #[test]
    fn independent_marketplace_evidence_requires_v3_and_future_features_are_not_assumed_compatible()
    {
        for version in [
            None,
            Some("0"),
            Some("1"),
            Some("2"),
            Some("3"),
            Some("4"),
            Some("03"),
        ] {
            assert_eq!(
                IndependentMarketplaceFetch::Metadata(&[]).supports_feature(version, "1"),
                matches!(version, Some("1" | "2" | "3"))
            );
            assert_eq!(
                IndependentMarketplaceFetch::Security(&[]).supports_feature(version, "2"),
                matches!(version, Some("2" | "3"))
            );
            assert_eq!(
                IndependentMarketplaceFetch::Evidence(&[]).supports_feature(version, "3"),
                version == Some("3")
            );
            assert_eq!(
                IndependentMarketplaceFetch::SdkHttp.supports_feature(version, "1"),
                version == Some("1")
            );
            assert_eq!(
                IndependentMarketplaceFetch::SdkHttpStream.supports_feature(version, "1"),
                version == Some("1")
            );
            assert_eq!(
                IndependentMarketplaceFetch::Connectivity.supports_feature(version, "2"),
                version == Some("2")
            );
        }
    }

    #[tokio::test]
    async fn independent_marketplace_fetch_entry_points_reject_unowned_handles_before_cli() {
        let runtime = DockerRuntimeManager::new(Some("/must-not-run-marketplace-fixture".into()));
        let handle = ContainerHandle {
            id: "a".repeat(64),
            name: "marketplace-test".into(),
        };
        let good = HashMap::from([
            (ELIXIR_MANAGED_LABEL.to_owned(), "true".to_owned()),
            (
                "elixir.marketplace.isolation".to_owned(),
                "fixed-v1".to_owned(),
            ),
            (CONTAINER_SPEC_HASH_LABEL.to_owned(), "test-only".to_owned()),
        ]);
        for key in [
            ELIXIR_MANAGED_LABEL,
            "elixir.marketplace.isolation",
            CONTAINER_SPEC_HASH_LABEL,
        ] {
            let mut labels = good.clone();
            labels.remove(key);
            for purpose in [
                IndependentMarketplaceFetch::Connectivity,
                IndependentMarketplaceFetch::SourcePreflight,
                IndependentMarketplaceFetch::RadicleSource,
                IndependentMarketplaceFetch::SdkHttp,
                IndependentMarketplaceFetch::SdkHttpStream,
                IndependentMarketplaceFetch::Metadata(&[]),
                IndependentMarketplaceFetch::Security(&[]),
                IndependentMarketplaceFetch::Evidence(&[]),
            ] {
                let error = runtime
                    .exec_independent_marketplace_fetch(&handle, &labels, purpose)
                    .await
                    .err()
                    .unwrap();
                assert!(
                    error
                        .to_string()
                        .contains("specific managed marketplace worker")
                );
            }
        }
        let mut mutable_handle = handle;
        mutable_handle.id = "mutable-container-name".into();
        assert!(
            runtime
                .exec_marketplace_sdk_http(&mutable_handle, &good)
                .await
                .err()
                .unwrap()
                .to_string()
                .contains("specific managed marketplace worker")
        );
        assert!(
            runtime
                .exec_marketplace_sdk_http_stream(&mutable_handle, &good)
                .await
                .err()
                .unwrap()
                .to_string()
                .contains("specific managed marketplace worker")
        );
        assert!(
            runtime
                .exec_marketplace_metadata_verifier(&mutable_handle, &good, &[])
                .await
                .err()
                .unwrap()
                .to_string()
                .contains("specific managed marketplace worker")
        );
        assert!(
            runtime
                .exec_marketplace_source_preflight(&mutable_handle, &good)
                .await
                .err()
                .unwrap()
                .to_string()
                .contains("specific managed marketplace worker")
        );
        assert!(
            runtime
                .exec_marketplace_connectivity_probe(&mutable_handle, &good)
                .await
                .err()
                .unwrap()
                .to_string()
                .contains("specific managed marketplace worker")
        );
    }

    #[tokio::test]
    async fn packet_network_capture_bounds_output_and_does_not_reflect_diagnostics() -> Result<()> {
        let runtime = DockerRuntimeManager::new(Some("node".to_owned()));
        let started = Instant::now();
        for script in [
            "process.stdout.write('x'.repeat(70000));setInterval(()=>{},1000)",
            "process.stderr.write('x'.repeat(5000));setInterval(()=>{},1000)",
        ] {
            assert!(
                runtime
                    .packet_network_capture(&["--eval".to_owned(), script.to_owned()])
                    .await
                    .is_err()
            );
        }
        assert!(started.elapsed() < Duration::from_secs(5));
        let (status, out, err) = runtime
            .packet_network_capture(&["--eval".to_owned(), "process.stdout.write('{}')".to_owned()])
            .await?;
        assert!(status.success());
        assert_eq!(out, "{}");
        assert!(err.is_empty());
        Ok(())
    }

    #[test]
    fn private_actor_inspection_rejects_malformed_or_hidden_isolation_settings() -> Result<()> {
        let good = json!({
            "Id":"a".repeat(64), "Name":"/private-packet-test",
            "State":{"Running":true, "Status":"running"},
            "Config":{"Cmd":["--epoch","test"], "Entrypoint":["/actor"],
                "Env":["PATH=/usr/bin"]},
            "HostConfig":{"CapAdd":["NET_ADMIN"], "CapDrop":["ALL"],
                "SecurityOpt":["no-new-privileges:true"],
                "PidMode":"", "IpcMode":"private", "UsernsMode":"",
                "Devices":null, "DeviceRequests":null, "Binds":null,
                "Sysctls":null, "Tmpfs":null,
                "RestartPolicy":{"Name":"no"}},
            "Mounts":[], "NetworkSettings":{"Networks":{"none":{}}}
        });
        let actual = DockerRuntimeManager::extract_private_actor(&good)?;
        assert!(actual.running);
        assert_eq!(actual.networks, ["none"]);
        assert_eq!(
            actual.device_count + actual.device_request_count + actual.bind_count,
            0
        );
        assert!(actual.sysctls.is_empty());
        let mut omitted = good.clone();
        for field in ["Devices", "DeviceRequests", "Binds", "Sysctls", "Tmpfs"] {
            omitted["HostConfig"].as_object_mut().unwrap().remove(field);
        }
        DockerRuntimeManager::extract_private_actor(&omitted)?;
        // Moby omits these optional fields when empty; explicit null/empty
        // values must be equivalent, while malformed values must never vanish.
        for path in ["Devices", "DeviceRequests", "Binds"] {
            for value in [Value::Null, json!([])] {
                let mut optional = good.clone();
                optional["HostConfig"][path] = value;
                DockerRuntimeManager::extract_private_actor(&optional)?;
            }
        }
        for path in ["Sysctls", "Tmpfs"] {
            for value in [Value::Null, json!({})] {
                let mut optional = good.clone();
                optional["HostConfig"][path] = value;
                DockerRuntimeManager::extract_private_actor(&optional)?;
            }
        }
        for (path, malformed) in [
            ("/HostConfig/CapAdd", json!(["NET_ADMIN", 123])),
            ("/HostConfig/CapDrop", json!(["ALL", {}])),
            ("/HostConfig/SecurityOpt", json!(["no-new-privileges", 123])),
            (
                "/HostConfig/SecurityOpt",
                json!(["no-new-privileges", "seccomp:unconfined"]),
            ),
            ("/HostConfig/Devices", json!({})),
            ("/HostConfig/DeviceRequests", json!(true)),
            ("/HostConfig/Binds", json!("/host:/guest")),
            ("/HostConfig/Sysctls", json!({"net.ipv4.ip_forward":true})),
            ("/HostConfig/Sysctls", json!([])),
            ("/Config/Cmd", json!([false])),
            ("/Config/Entrypoint", json!("/bin/sh")),
            ("/Config/Env", json!([null])),
            ("/HostConfig/PidMode", Value::Null),
            ("/HostConfig/IpcMode", json!(false)),
            ("/HostConfig/UsernsMode", json!([])),
            ("/HostConfig/RestartPolicy/Name", Value::Null),
            ("/NetworkSettings/Networks", Value::Null),
            ("/Mounts", Value::Null),
            ("/Mounts", json!([{"unexpected":true}])),
            ("/State/Running", json!("true")),
        ] {
            let mut bad = good.clone();
            *bad.pointer_mut(path).unwrap() = malformed;
            assert!(
                DockerRuntimeManager::extract_private_actor(&bad).is_err(),
                "{path}"
            );
        }
        for value in [json!([]), json!({"/tmp":"size=1m"}), json!(true)] {
            let mut bad = good.clone();
            bad["HostConfig"]["Tmpfs"] = value;
            assert!(DockerRuntimeManager::extract_private_actor(&bad).is_err());
        }
        Ok(())
    }

    #[test]
    fn port_mapping_formats() {
        let mapping = PortMapping {
            container_port: 8080,
            host_port: Some(0),
            host_ip: None,
            protocol: Some("tcp".to_string()),
        };
        assert_eq!(format_port(&mapping).as_deref(), Some("0:8080/tcp"));

        let loopback = PortMapping {
            host_ip: Some("127.0.0.1".to_string()),
            ..mapping
        };
        assert_eq!(
            format_port(&loopback).as_deref(),
            Some("127.0.0.1:0:8080/tcp")
        );
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
    fn private_file_volume_requires_safe_owned_input() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let source = temporary.path().join("control.json");
        std::fs::write(&source, b"private-volume-test").expect("test input");
        let spec = PrivateFileVolumeSpec {
            name: "elixir_live_private_test".to_string(),
            image: "elixir-live-egress-worker:test".to_string(),
            source_path: source.to_string_lossy().into_owned(),
            file_name: "control.json".to_string(),
            owner_uid: 65_532,
            owner_gid: 65_532,
            labels: HashMap::from([("elixir.managed".to_string(), "true".to_string())]),
        };
        DockerRuntimeManager::validate_private_file_volume_spec(&spec)
            .expect("valid private volume specification");

        for invalid in [
            PrivateFileVolumeSpec {
                name: "../unsafe".to_string(),
                ..spec.clone()
            },
            PrivateFileVolumeSpec {
                file_name: "nested/control.json".to_string(),
                ..spec.clone()
            },
            PrivateFileVolumeSpec {
                owner_uid: 0,
                ..spec.clone()
            },
        ] {
            assert!(DockerRuntimeManager::validate_private_file_volume_spec(&invalid).is_err());
        }
    }

    #[test]
    fn private_file_volume_labels_require_the_complete_binding_tuple() {
        let volume = json!({
            "Labels": {
                "elixir.managed": "true",
                "elixir.live.egress.binding_id": "binding-a"
            }
        });
        let required = HashMap::from([
            ("elixir.managed".to_string(), "true".to_string()),
            (
                "elixir.live.egress.binding_id".to_string(),
                "binding-a".to_string(),
            ),
        ]);
        assert!(DockerRuntimeManager::private_volume_labels_match(
            &volume, &required
        ));
        let foreign = HashMap::from([(
            "elixir.live.egress.binding_id".to_string(),
            "binding-b".to_string(),
        )]);
        assert!(!DockerRuntimeManager::private_volume_labels_match(
            &volume, &foreign
        ));
    }

    #[test]
    fn owned_directory_volume_requires_non_root_identity_and_labels() {
        let spec = OwnedDirectoryVolumeSpec {
            name: "elixir_live_warp_state".to_string(),
            image: "elixir-live-egress-worker:test".to_string(),
            owner_uid: 1_000,
            owner_gid: 1_000,
            labels: HashMap::from([("elixir.managed".to_string(), "true".to_string())]),
        };
        DockerRuntimeManager::validate_owned_directory_volume_spec(&spec)
            .expect("valid owned directory volume");
        for invalid in [
            OwnedDirectoryVolumeSpec {
                name: "../unsafe".to_string(),
                ..spec.clone()
            },
            OwnedDirectoryVolumeSpec {
                owner_uid: 0,
                ..spec.clone()
            },
            OwnedDirectoryVolumeSpec {
                labels: HashMap::new(),
                ..spec.clone()
            },
        ] {
            assert!(DockerRuntimeManager::validate_owned_directory_volume_spec(&invalid).is_err());
        }
    }

    #[tokio::test]
    #[ignore = "live Docker private-volume test; set ELIXIR_LIVE_EGRESS_DOCKER_TESTS=1 to run"]
    async fn n11_private_file_volume_is_portable_when_configured() -> Result<()> {
        if std::env::var("ELIXIR_LIVE_EGRESS_DOCKER_TESTS").as_deref() != Ok("1") {
            eprintln!(
                "skipping Live private-volume test: ELIXIR_LIVE_EGRESS_DOCKER_TESTS=1 is not set"
            );
            return Ok(());
        }
        let image = std::env::var("ELIXIR_LIVE_EGRESS_WORKER_IMAGE")
            .context("ELIXIR_LIVE_EGRESS_WORKER_IMAGE is required")?;
        let runtime = DockerRuntimeManager::new(None);
        runtime.server_version().await?;
        let temporary = tempfile::tempdir()?;
        let source = temporary.path().join("control.json");
        let body = b"elixir-live-private-volume-certification";
        std::fs::write(&source, body)?;
        let volume_name = format!("elixir_live_private_test_{}", Uuid::new_v4().simple());
        let mut labels = HashMap::from([
            (ELIXIR_MANAGED_LABEL.to_string(), "true".to_string()),
            (
                "elixir.live.egress.binding_id".to_string(),
                Uuid::new_v4().to_string(),
            ),
        ]);
        if let Ok(run_id) = std::env::var("ELIXIR_LIVE_EGRESS_TEST_RUN_ID") {
            if !run_id.is_empty()
                && run_id.len() <= 64
                && run_id.chars().all(|character| character.is_ascii_digit())
            {
                labels.insert("elixir.live.egress.test_run_id".to_string(), run_id);
            }
        }
        let spec = PrivateFileVolumeSpec {
            name: volume_name.clone(),
            image: image.clone(),
            source_path: source.to_string_lossy().into_owned(),
            file_name: "control.json".to_string(),
            owner_uid: 65_532,
            owner_gid: 65_532,
            labels: labels.clone(),
        };
        runtime.create_private_file_volume(&spec).await?;
        assert!(
            runtime
                .private_file_volume_owned(&volume_name, &labels)
                .await?
        );

        let expected_hash = format!("{:x}", Sha256::digest(body));
        let check = runtime
            .run_capture(&[
                "run".to_string(),
                "--rm".to_string(),
                "--pull".to_string(),
                "never".to_string(),
                "--network".to_string(),
                "none".to_string(),
                "--read-only".to_string(),
                "--cap-drop".to_string(),
                "ALL".to_string(),
                "--security-opt".to_string(),
                "no-new-privileges".to_string(),
                "--user".to_string(),
                "65532:65532".to_string(),
                "-v".to_string(),
                format!("{volume_name}:/run/elixir-live-egress:ro"),
                "--entrypoint".to_string(),
                "/bin/sh".to_string(),
                image,
                "-c".to_string(),
                concat!(
                    "set -eu; ",
                    "test \"$(stat -c '%u:%g:%a' /run/elixir-live-egress)\" = '65532:65532:700'; ",
                    "test \"$(stat -c '%u:%g:%a' /run/elixir-live-egress/control.json)\" = '65532:65532:600'; ",
                    "test \"$(sha256sum /run/elixir-live-egress/control.json | cut -d ' ' -f 1)\" = \"$1\""
                )
                .to_string(),
                "--".to_string(),
                expected_hash,
            ])
            .await;
        let cleanup = runtime
            .remove_private_file_volume(&volume_name, &labels)
            .await;
        check?;
        cleanup?;
        assert!(
            !runtime
                .private_file_volume_owned(&volume_name, &labels)
                .await?
        );
        Ok(())
    }

    #[tokio::test]
    #[ignore = "live Docker owned-volume test; set ELIXIR_LIVE_EGRESS_DOCKER_TESTS=1 to run"]
    async fn n11_owned_directory_volume_is_portable_when_configured() -> Result<()> {
        if std::env::var("ELIXIR_LIVE_EGRESS_DOCKER_TESTS").as_deref() != Ok("1") {
            eprintln!(
                "skipping Live owned-volume test: ELIXIR_LIVE_EGRESS_DOCKER_TESTS=1 is not set"
            );
            return Ok(());
        }
        let image = std::env::var("ELIXIR_LIVE_EGRESS_WORKER_IMAGE")
            .context("ELIXIR_LIVE_EGRESS_WORKER_IMAGE is required")?;
        let runtime = DockerRuntimeManager::new(None);
        runtime.server_version().await?;
        let volume_name = format!("elixir_live_owned_test_{}", Uuid::new_v4().simple());
        let labels = HashMap::from([
            (ELIXIR_MANAGED_LABEL.to_string(), "true".to_string()),
            (
                "elixir.live.egress.role".to_string(),
                "warp_state".to_string(),
            ),
            (
                "elixir.live.egress.policy_id".to_string(),
                Uuid::new_v4().to_string(),
            ),
        ]);
        let spec = OwnedDirectoryVolumeSpec {
            name: volume_name.clone(),
            image: image.clone(),
            owner_uid: 1_000,
            owner_gid: 1_000,
            labels: labels.clone(),
        };

        let verification = async {
            runtime.ensure_owned_directory_volume(&spec).await?;
            runtime
                .run_capture(&[
                    "run".to_string(),
                    "--rm".to_string(),
                    "--pull".to_string(),
                    "never".to_string(),
                    "--network".to_string(),
                    "none".to_string(),
                    "--read-only".to_string(),
                    "--cap-drop".to_string(),
                    "ALL".to_string(),
                    "--security-opt".to_string(),
                    "no-new-privileges".to_string(),
                    "--user".to_string(),
                    "1000:1000".to_string(),
                    "-v".to_string(),
                    format!("{volume_name}:/var/lib/cloudflare-warp"),
                    "--entrypoint".to_string(),
                    "/bin/sh".to_string(),
                    image.clone(),
                    "-c".to_string(),
                    concat!(
                        "set -eu; umask 077; ",
                        "test \"$(stat -c '%u:%g:%a' /var/lib/cloudflare-warp)\" = '1000:1000:700'; ",
                        "printf state > /var/lib/cloudflare-warp/registration; ",
                        "test \"$(stat -c '%u:%g:%a' /var/lib/cloudflare-warp/registration)\" = '1000:1000:600'"
                    )
                    .to_string(),
                ])
                .await?;

            // Re-admission verifies an existing nonempty state volume without mutating it.
            runtime.ensure_owned_directory_volume(&spec).await?;
            runtime
                .run_capture(&[
                    "run".to_string(),
                    "--rm".to_string(),
                    "--pull".to_string(),
                    "never".to_string(),
                    "--network".to_string(),
                    "none".to_string(),
                    "--read-only".to_string(),
                    "--cap-drop".to_string(),
                    "ALL".to_string(),
                    "--security-opt".to_string(),
                    "no-new-privileges".to_string(),
                    "--user".to_string(),
                    "1000:1000".to_string(),
                    "-v".to_string(),
                    format!("{volume_name}:/var/lib/cloudflare-warp:ro"),
                    "--entrypoint".to_string(),
                    "/bin/sh".to_string(),
                    image,
                    "-c".to_string(),
                    concat!(
                        "set -eu; ",
                        "test \"$(stat -c '%u:%g:%a' /var/lib/cloudflare-warp)\" = '1000:1000:700'; ",
                        "test \"$(stat -c '%u:%g:%a' /var/lib/cloudflare-warp/registration)\" = '1000:1000:600'; ",
                        "test \"$(cat /var/lib/cloudflare-warp/registration)\" = state"
                    )
                    .to_string(),
                ])
                .await?;
            Result::<()>::Ok(())
        }
        .await;
        let cleanup = runtime
            .remove_private_file_volume(&volume_name, &labels)
            .await;
        verification?;
        cleanup?;
        assert!(
            !runtime
                .private_file_volume_owned(&volume_name, &labels)
                .await?
        );
        Ok(())
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
            "Image": "sha256:resolved-local-id",
            "HostConfig": {
                "NetworkMode": "container:elx-qbittorrent-vpn",
                "ReadonlyRootfs": true,
                "SecurityOpt": ["no-new-privileges", "seccomp=/etc/elixir/seccomp/prism.json", "apparmor=prism-default"],
                "CapDrop": ["ALL"],
                "Tmpfs": {
                    "/tmp": "size=64m"
                },
                "Memory": 536870912,
                "MemorySwap": 536870912,
                "ShmSize": 1048576,
                "Privileged": false,
                "CapAdd": [],
                "LogConfig": {"Type":"none","Config":{}},
                "Ulimits": [{"Name":"core","Soft":0,"Hard":0}],
                "PidsLimit": 128,
                "NanoCpus": 1000000000
            },
            "Config": {
                "Image": "example@sha256:pinned-registry-digest",
                "User": "1000:1000",
                "Labels": {
                    "elixir.instance_id": "abc",
                    "elixir.extension_id": "elixir.modules.qbittorrent"
                }
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
        assert_eq!(state.security.user.as_deref(), Some("1000:1000"));
        assert_eq!(
            state.security.image_reference.as_deref(),
            Some("example@sha256:pinned-registry-digest")
        );
        assert_eq!(
            state.security.image_id.as_deref(),
            Some("sha256:resolved-local-id")
        );
        assert!(state.security.read_only_rootfs);
        assert!(state.security.no_new_privileges);
        assert_eq!(state.security.cap_drop, vec!["ALL".to_string()]);
        assert_eq!(state.security.tmpfs.len(), 1);
        assert_eq!(state.security.tmpfs[0].path, "/tmp");
        assert_eq!(state.security.tmpfs[0].options.as_deref(), Some("size=64m"));
        assert_eq!(state.security.memory_limit_bytes, Some(536870912));
        assert_eq!(state.security.memory_swap_limit_bytes, Some(536870912));
        assert_eq!(state.security.shared_memory_bytes, Some(1048576));
        assert_eq!(state.security.privileged, Some(false));
        assert!(state.security.cap_add.is_empty());
        assert_eq!(state.security.log_config.as_ref().unwrap().driver, "none");
        assert_eq!(
            state.security.core_dump_limit,
            Some(ContainerRuntimeUlimit { soft: 0, hard: 0 })
        );
        assert_eq!(state.security.pids_limit, Some(128));
        assert_eq!(state.security.nano_cpus, Some(1000000000));
        assert_eq!(
            state.security.seccomp_profile.as_deref(),
            Some("/etc/elixir/seccomp/prism.json")
        );
        assert_eq!(
            state.security.apparmor_profile.as_deref(),
            Some("prism-default")
        );
    }

    #[test]
    fn required_labels_enforced() {
        let mut labels = HashMap::new();
        labels.insert(ELIXIR_INSTANCE_ID_LABEL.to_string(), "id".to_string());
        labels.insert(ELIXIR_EXTENSION_ID_LABEL.to_string(), "ext".to_string());
        assert!(DockerRuntimeManager::ensure_required_labels(&labels).is_ok());
    }

    #[test]
    fn managed_container_filter_uses_docker_label_filter_syntax() {
        assert_eq!(
            DockerRuntimeManager::managed_container_filter(),
            "label=elixir.managed=true"
        );
    }

    #[test]
    fn marketplace_immutable_container_lookup_uses_id_filter_and_rejects_substitutions()
    -> Result<()> {
        let id = "a".repeat(64);
        assert_eq!(
            DockerRuntimeManager::container_identity_filter(&id),
            format!("id={id}")
        );
        assert_eq!(
            DockerRuntimeManager::container_identity_filter("owned-source"),
            "name=^/owned-source$"
        );
        assert_eq!(
            DockerRuntimeManager::parse_container_identity(&id, &format!("{id}\n"))?,
            Some(id.clone())
        );
        assert!(DockerRuntimeManager::parse_container_identity(&id, "")?.is_none());
        assert!(DockerRuntimeManager::parse_container_identity(&id, &"b".repeat(64)).is_err());
        assert!(DockerRuntimeManager::parse_container_identity(&id, "short-id").is_err());
        assert!(
            DockerRuntimeManager::parse_container_identity(
                "owned-source",
                &format!("{id}\n{}\n", "b".repeat(64))
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn marketplace_immediate_stop_requires_exact_native_role_and_positive_state() -> Result<()> {
        let id = "a".repeat(64);
        let handle = ContainerHandle {
            id: id.clone(),
            name: id.clone(),
        };
        let fixture = serde_json::json!({
            "Id":id,"Name":"/elixir-marketplace-source-writer-owned",
            "State":{"Status":"running","Running":true},
            "Config":{"Labels":{
                "elixir.managed":"true","elixir.deployment_id":"owned",
                "elixir.extension_id":"elixir-marketplace-source","elixir.instance_id":"marketplace-source-owned",
                "elixir.marketplace.isolation":"quarantine-v1"
            }}
        });
        assert!(DockerRuntimeManager::require_quarantine_writer(&fixture, &handle)?.running);
        let mut stopped = fixture.clone();
        stopped["State"]["Status"] = "exited".into();
        stopped["State"]["Running"] = false.into();
        assert!(!DockerRuntimeManager::require_quarantine_writer(&stopped, &handle)?.running);
        for pointer in [
            "/Id",
            "/Name",
            "/Config/Labels/elixir.managed",
            "/Config/Labels/elixir.deployment_id",
            "/Config/Labels/elixir.extension_id",
            "/Config/Labels/elixir.instance_id",
            "/Config/Labels/elixir.marketplace.isolation",
            "/State/Status",
        ] {
            let mut wrong = fixture.clone();
            *wrong.pointer_mut(pointer).unwrap() = "foreign".into();
            assert!(
                DockerRuntimeManager::require_quarantine_writer(&wrong, &handle).is_err(),
                "{pointer}"
            );
        }
        let mut unknown = fixture.clone();
        unknown["State"].as_object_mut().unwrap().remove("Running");
        assert!(DockerRuntimeManager::require_quarantine_writer(&unknown, &handle).is_err());
        let mut contradictory = fixture.clone();
        contradictory["State"]["Running"] = false.into();
        assert!(DockerRuntimeManager::require_quarantine_writer(&contradictory, &handle).is_err());
        let named = ContainerHandle {
            id: handle.id.clone(),
            name: "elixir-marketplace-source-writer-owned".into(),
        };
        assert!(DockerRuntimeManager::require_quarantine_writer(&fixture, &named).is_err());
        Ok(())
    }

    #[test]
    fn source_start_intent_rejects_hooks_restart_and_host_namespace_drift() -> Result<()> {
        let mut spec = scoped_spec("source", "owned");
        spec.command = vec!["--source-eval".into(), "{}".into()];
        let fixture = serde_json::json!({
            "Config": {
                "Cmd":spec.command,
                "Entrypoint":["/opt/elixir-marketplace/bin/elixir-marketplace-launcher"],
                "Env":["PATH=/usr/local/bin:/usr/bin:/bin","NODE_VERSION=24.16.0","YARN_VERSION=1.22.22"]
            },
            "HostConfig": {
                "RestartPolicy":{"Name":"no"},"Init":false,"PidMode":"","IpcMode":"private","UsernsMode":"",
                "Devices":[],"DeviceRequests":null,"Binds":null,"Sysctls":null
            }
        });
        DockerRuntimeManager::verify_source_start_intent(&fixture, &spec)?;
        let mut omitted_empty_sysctls = fixture.clone();
        omitted_empty_sysctls["HostConfig"]
            .as_object_mut()
            .unwrap()
            .remove("Sysctls");
        DockerRuntimeManager::verify_source_start_intent(&omitted_empty_sysctls, &spec)?;
        for (pointer, wrong) in [
            (
                "/Config/Cmd",
                serde_json::json!(["--source-eval", "{\"wrong\":true}"]),
            ),
            ("/Config/Entrypoint", serde_json::json!(["/bin/sh"])),
            (
                "/Config/Env",
                serde_json::json!(["NODE_OPTIONS=--require=/quarantine/package.blob"]),
            ),
            (
                "/Config/Env",
                serde_json::json!(["PATH=/bin", "PATH=/quarantine"]),
            ),
            (
                "/HostConfig/RestartPolicy/Name",
                serde_json::json!("always"),
            ),
            ("/HostConfig/Init", serde_json::json!(true)),
            ("/HostConfig/PidMode", serde_json::json!("host")),
            ("/HostConfig/IpcMode", serde_json::json!("host")),
            ("/HostConfig/UsernsMode", serde_json::json!("host")),
            ("/HostConfig/Devices", serde_json::json!([{}])),
            ("/HostConfig/DeviceRequests", serde_json::json!([{}])),
            ("/HostConfig/Binds", serde_json::json!(["/:/host"])),
            (
                "/HostConfig/Sysctls",
                serde_json::json!({"net.ipv4.ip_forward":"1"}),
            ),
        ] {
            let mut actual = fixture.clone();
            *actual.pointer_mut(pointer).unwrap() = wrong;
            assert!(
                DockerRuntimeManager::verify_source_start_intent(&actual, &spec).is_err(),
                "{pointer}"
            );
        }
        for pointer in ["/Config/Env", "/HostConfig/Devices", "/HostConfig/Init"] {
            let mut actual = fixture.clone();
            *actual.pointer_mut(pointer).unwrap() = serde_json::json!("unknown");
            assert!(DockerRuntimeManager::verify_source_start_intent(&actual, &spec).is_err());
        }
        // The generic Docker argument builder uses -v for named volumes too.
        // Accept only that exact closed mapping, including its read-only mode.
        spec.volumes = vec![VolumeMount {
            source_kind: VolumeMountSourceKind::NamedVolume,
            host_path: "elixir-marketplace-source-owned".into(),
            container_path: "/quarantine".into(),
            read_only: true,
        }];
        let mut mounted = fixture.clone();
        mounted["HostConfig"]["Binds"] =
            serde_json::json!(["elixir-marketplace-source-owned:/quarantine:ro"]);
        DockerRuntimeManager::verify_source_start_intent(&mounted, &spec)?;
        for binds in [
            serde_json::json!(null),
            serde_json::json!(["/tmp:/quarantine:ro"]),
            serde_json::json!(["foreign:/quarantine:ro"]),
            serde_json::json!(["elixir-marketplace-source-owned:/quarantine"]),
            serde_json::json!([
                "elixir-marketplace-source-owned:/quarantine:ro",
                "elixir-marketplace-source-owned:/quarantine:ro"
            ]),
        ] {
            mounted["HostConfig"]["Binds"] = binds;
            assert!(DockerRuntimeManager::verify_source_start_intent(&mounted, &spec).is_err());
        }
        Ok(())
    }

    #[test]
    fn create_only_source_writer_accepts_verified_executor_but_not_arbitrary_commands() -> Result<()>
    {
        let mut spec = scoped_spec("marketplace-source-owned", "owned");
        spec.name = "elixir-marketplace-source-writer-owned".into();
        spec.image = format!("sha256:{}", "a".repeat(64));
        spec.network_mode = Some("none".into());
        spec.aliases.clear();
        spec.labels
            .insert(ELIXIR_MANAGED_LABEL.into(), "true".into());
        spec.labels.insert(
            ELIXIR_EXTENSION_ID_LABEL.into(),
            "elixir-marketplace-source".into(),
        );
        spec.labels.insert(
            "elixir.marketplace.isolation".into(),
            "quarantine-v1".into(),
        );
        spec.volumes = vec![VolumeMount {
            source_kind: VolumeMountSourceKind::NamedVolume,
            host_path: "elixir-marketplace-source-owned".into(),
            container_path: "/quarantine".into(),
            read_only: true,
        }];
        spec.security.user = Some("10004:10004".into());
        spec.security.read_only_rootfs = true;
        spec.security.no_new_privileges = true;
        for mode in [
            "--source-match",
            "--build-inputs-check",
            "--source-eval",
            "--source-eval-verified",
        ] {
            spec.command = vec![mode.into(), "{}".into()];
            DockerRuntimeManager::require_source_writer_spec(&spec)?;
        }
        for mode in [
            "/bin/sh",
            "--self-test-source-eval-verified",
            "--unknown-source",
        ] {
            spec.command = vec![mode.into(), "{}".into()];
            assert!(DockerRuntimeManager::require_source_writer_spec(&spec).is_err());
        }
        Ok(())
    }

    #[test]
    fn orphan_cleanup_only_prunes_current_deployment_containers() {
        let active = HashSet::from(["active".to_string()]);
        let current_orphan = labels_for("orphan", Some("current"));
        let current_active = labels_for("active", Some("current"));
        let foreign_orphan = labels_for("orphan", Some("foreign"));
        let legacy_orphan = labels_for("orphan", None);

        assert!(
            DockerRuntimeManager::should_prune_orphaned_managed_container(
                &current_orphan,
                &active,
                "current"
            )
        );
        assert!(
            !DockerRuntimeManager::should_prune_orphaned_managed_container(
                &current_active,
                &active,
                "current"
            )
        );
        assert!(
            !DockerRuntimeManager::should_prune_orphaned_managed_container(
                &foreign_orphan,
                &active,
                "current"
            )
        );
        assert!(
            !DockerRuntimeManager::should_prune_orphaned_managed_container(
                &legacy_orphan,
                &active,
                "current"
            )
        );
    }

    #[test]
    fn runtime_reset_keeps_foreign_and_unknown_legacy_containers() {
        let active = HashSet::from(["active".to_string()]);
        let current_orphan = labels_for("orphan", Some("current"));
        let legacy_active = labels_for("active", None);
        let legacy_orphan = labels_for("orphan", None);
        let foreign_active = labels_for("active", Some("foreign"));

        assert!(DockerRuntimeManager::should_reset_managed_container(
            &current_orphan,
            &active,
            "current"
        ));
        assert!(DockerRuntimeManager::should_reset_managed_container(
            &legacy_active,
            &active,
            "current"
        ));
        assert!(!DockerRuntimeManager::should_reset_managed_container(
            &legacy_orphan,
            &active,
            "current"
        ));
        assert!(!DockerRuntimeManager::should_reset_managed_container(
            &foreign_active,
            &active,
            "current"
        ));
    }

    #[test]
    fn alias_conflict_cleanup_only_removes_current_deployment_conflicts() {
        let spec = scoped_spec("desired-instance", "current");
        let same_instance = labels_for("desired-instance", Some("foreign"));
        let current_conflict = labels_for("other-instance", Some("current"));
        let foreign_conflict = labels_for("other-instance", Some("foreign"));
        let legacy_conflict = labels_for("other-instance", None);

        assert!(
            !DockerRuntimeManager::should_remove_conflicting_managed_container(
                &same_instance,
                &spec
            )
        );
        assert!(
            DockerRuntimeManager::should_remove_conflicting_managed_container(
                &current_conflict,
                &spec
            )
        );
        assert!(
            !DockerRuntimeManager::should_remove_conflicting_managed_container(
                &foreign_conflict,
                &spec
            )
        );
        assert!(
            !DockerRuntimeManager::should_remove_conflicting_managed_container(
                &legacy_conflict,
                &spec
            )
        );
    }

    #[test]
    fn exact_name_foreign_container_refuses_mutation() {
        let spec = scoped_spec("desired-instance", "current");
        let foreign = labels_for("other-instance", Some("foreign"));
        let legacy = labels_for("other-instance", None);
        let same_instance_legacy = labels_for("desired-instance", None);

        let foreign_err = DockerRuntimeManager::ensure_existing_container_is_mutable_by_spec(
            "elx-abc123",
            &foreign,
            &spec,
        )
        .expect_err("foreign deployment should be rejected");
        assert!(
            foreign_err.to_string().contains("refusing to mutate it"),
            "unexpected error: {foreign_err}"
        );

        let legacy_err = DockerRuntimeManager::ensure_existing_container_is_mutable_by_spec(
            "elx-abc123",
            &legacy,
            &spec,
        )
        .expect_err("unknown legacy container should be rejected");
        assert!(
            legacy_err.to_string().contains("has no deployment label"),
            "unexpected error: {legacy_err}"
        );

        DockerRuntimeManager::ensure_existing_container_is_mutable_by_spec(
            "elx-abc123",
            &same_instance_legacy,
            &spec,
        )
        .expect("legacy container with the same instance id is owned by this spec");
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

    fn labels_for(instance_id: &str, deployment_id: Option<&str>) -> HashMap<String, String> {
        let mut labels = HashMap::new();
        labels.insert(ELIXIR_MANAGED_LABEL.to_string(), "true".to_string());
        labels.insert(
            ELIXIR_INSTANCE_ID_LABEL.to_string(),
            instance_id.to_string(),
        );
        labels.insert(
            ELIXIR_EXTENSION_ID_LABEL.to_string(),
            "elixir.test".to_string(),
        );
        if let Some(deployment_id) = deployment_id {
            labels.insert(
                ELIXIR_DEPLOYMENT_ID_LABEL.to_string(),
                deployment_id.to_string(),
            );
        }
        labels
    }

    fn scoped_spec(instance_id: &str, deployment_id: &str) -> ContainerSpec {
        ContainerSpec {
            name: "elx-abc123".to_string(),
            image: "example/test:latest".to_string(),
            network: "elixir_net".to_string(),
            network_mode: None,
            aliases: vec!["svc-test".to_string()],
            env: Vec::new(),
            volumes: Vec::new(),
            ports: Vec::new(),
            labels: labels_for(instance_id, Some(deployment_id)),
            command: Vec::new(),
            cap_add: Vec::new(),
            cap_drop: Vec::new(),
            devices: Vec::new(),
            sysctls: HashMap::new(),
            security: ContainerSecurityOptions::default(),
        }
    }

    #[test]
    fn fixed_marketplace_volumes_reject_mutable_images_foreign_owners_and_nonlocal_options() {
        let spec = FixedMarketplaceVolumeSpec {
            name: "elixir-marketplace-fixture".to_owned(),
            image: format!("sha256:{}", "1".repeat(64)),
            memory_limit_mb: 256,
            cpu_millicores: 100,
            labels: HashMap::from([
                (ELIXIR_MANAGED_LABEL.to_owned(), "true".to_owned()),
                (ELIXIR_DEPLOYMENT_ID_LABEL.to_owned(), "fixture".to_owned()),
                (
                    "elixir.marketplace.volume".to_owned(),
                    "fixed-v1".to_owned(),
                ),
            ]),
        };
        DockerRuntimeManager::validate_fixed_marketplace_volume_spec(&spec).unwrap();
        for (memory_limit_mb, cpu_millicores) in
            [(0, 100), (63, 100), (257, 100), (256, 0), (256, 101)]
        {
            assert!(
                DockerRuntimeManager::validate_fixed_marketplace_volume_spec(
                    &FixedMarketplaceVolumeSpec {
                        memory_limit_mb,
                        cpu_millicores,
                        ..spec.clone()
                    }
                )
                .is_err()
            );
        }
        DockerRuntimeManager::validate_fixed_marketplace_volume_spec(&FixedMarketplaceVolumeSpec {
            memory_limit_mb: 128,
            cpu_millicores: 50,
            ..spec.clone()
        })
        .unwrap();
        let valid = serde_json::json!({"Name":spec.name,"Driver":"local","Scope":"local","Options":null,"Labels":spec.labels});
        DockerRuntimeManager::verify_fixed_marketplace_volume(&valid, &spec).unwrap();
        for (field, replacement) in [
            ("Name", serde_json::json!("other")),
            ("Driver", serde_json::json!("plugin")),
            ("Scope", serde_json::json!("global")),
            ("Options", serde_json::json!({"device":"/unowned"})),
            ("Labels", serde_json::json!({})),
        ] {
            let mut changed = valid.clone();
            changed[field] = replacement;
            assert!(
                DockerRuntimeManager::verify_fixed_marketplace_volume(&changed, &spec).is_err()
            );
        }
        assert!(
            DockerRuntimeManager::validate_fixed_marketplace_volume_spec(
                &FixedMarketplaceVolumeSpec {
                    image: "mutable:latest".to_owned(),
                    ..spec
                }
            )
            .is_err()
        );
    }

    #[test]
    fn quarantine_volume_requires_its_own_closed_profile_and_immutable_image() {
        use elixir_marketplace::{
            identity::Digest, quarantine::QuarantineWorkspace, records::Blob,
        };
        let blob = Blob {
            sha256: Digest::of(b"fixture"),
            size: 7,
        };
        let spec = QuarantineMarketplaceVolumeSpec {
            name: "elixir-marketplace-source-fixture".to_owned(),
            image: format!("sha256:{}", "1".repeat(64)),
            memory_limit_mb: 64,
            cpu_millicores: 100,
            workspace: QuarantineWorkspace {
                schema_version: 1,
                release_id: Digest::of(b"release"),
                package: blob.clone(),
                proof: blob,
                artifact: None,
                timeout_seconds: 60,
            },
            labels: HashMap::from([
                (ELIXIR_MANAGED_LABEL.to_owned(), "true".to_owned()),
                (ELIXIR_DEPLOYMENT_ID_LABEL.to_owned(), "fixture".to_owned()),
                (
                    "elixir.marketplace.volume".to_owned(),
                    "quarantine-v1".to_owned(),
                ),
            ]),
        };
        DockerRuntimeManager::validate_quarantine_marketplace_volume_spec(&spec).unwrap();
        for cpu_millicores in [500, 1000] {
            DockerRuntimeManager::validate_quarantine_marketplace_volume_spec(
                &QuarantineMarketplaceVolumeSpec {
                    cpu_millicores,
                    ..spec.clone()
                },
            )
            .unwrap();
        }
        for bad in [
            QuarantineMarketplaceVolumeSpec {
                image: "elixir-marketplace:latest".to_owned(),
                ..spec.clone()
            },
            QuarantineMarketplaceVolumeSpec {
                memory_limit_mb: 0,
                ..spec.clone()
            },
            QuarantineMarketplaceVolumeSpec {
                cpu_millicores: 0,
                ..spec.clone()
            },
            QuarantineMarketplaceVolumeSpec {
                cpu_millicores: 1001,
                ..spec.clone()
            },
            QuarantineMarketplaceVolumeSpec {
                memory_limit_mb: u64::MAX,
                ..spec.clone()
            },
            QuarantineMarketplaceVolumeSpec {
                memory_limit_mb: 257,
                ..spec.clone()
            },
            QuarantineMarketplaceVolumeSpec {
                name: "unrelated-volume".to_owned(),
                ..spec.clone()
            },
        ] {
            assert!(
                DockerRuntimeManager::validate_quarantine_marketplace_volume_spec(&bad).is_err()
            );
        }
        let mut bad = spec.clone();
        bad.workspace.package.size = 0;
        assert!(DockerRuntimeManager::validate_quarantine_marketplace_volume_spec(&bad).is_err());
        let mut bad = spec;
        bad.labels.insert(
            "elixir.marketplace.volume".to_owned(),
            "fixed-v1".to_owned(),
        );
        assert!(DockerRuntimeManager::validate_quarantine_marketplace_volume_spec(&bad).is_err());
    }

    #[test]
    fn private_stream_and_file_limits_are_explicit_and_duplicate_limits_remain_unknown() {
        let security = ContainerSecurityOptions {
            stdin_open: true,
            file_size_limit_bytes: Some(201326592),
            ..Default::default()
        };
        assert_eq!(
            container_resource_args(&security).unwrap(),
            ["--ulimit", "fsize=201326592:201326592", "--interactive"]
        );
        assert_eq!(
            container_resource_args(&ContainerSecurityOptions {
                file_size_limit_bytes: Some(0),
                ..Default::default()
            })
            .unwrap(),
            ["--ulimit", "fsize=0:0"]
        );
        assert!(
            container_resource_args(&ContainerSecurityOptions {
                file_size_limit_bytes: Some(i64::MAX as u64 + 1),
                ..Default::default()
            })
            .is_err()
        );
        let state = DockerRuntimeManager::extract_runtime_security(
            &serde_json::json!({"Config":{"OpenStdin":true,"Tty":false},
            "HostConfig":{"Ulimits":[{"Name":"fsize","Soft":1,"Hard":1},{"Name":"fsize","Soft":2,"Hard":2}]}}),
        );
        assert!(state.file_size_limit.is_none());
        assert_eq!(state.stdin_open, Some(true));
        assert_eq!(state.tty, Some(false));
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
            cap_drop: Vec::new(),
            sysctls: HashMap::new(),
            security: Default::default(),
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
            cap_drop: Vec::new(),
            sysctls: HashMap::new(),
            security: Default::default(),
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
            cap_drop: Vec::new(),
            sysctls: HashMap::new(),
            security: Default::default(),
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
            cap_drop: Vec::new(),
            sysctls: HashMap::new(),
            security: Default::default(),
        };
        crate::runtime::model::apply_container_spec_fingerprint(&mut spec);
        let inspect = json!({ "Config": { "Labels": {} } });

        assert_eq!(
            DockerRuntimeManager::container_spec_hash_mismatch(&spec, &inspect),
            None
        );
    }

    #[test]
    fn security_options_change_spec_fingerprint() {
        let mut baseline = ContainerSpec {
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
            cap_drop: Vec::new(),
            sysctls: HashMap::new(),
            security: Default::default(),
        };
        let mut hardened = baseline.clone();
        hardened.cap_drop = vec!["ALL".to_string()];
        hardened.security.read_only_rootfs = true;
        hardened.security.no_new_privileges = true;
        hardened.security.tmpfs.push(ContainerTmpfsMount {
            path: "/tmp".to_string(),
            size_mb: Some(64),
            ..Default::default()
        });
        hardened.security.apparmor_profile = Some("prism-default".to_string());
        crate::runtime::model::apply_container_spec_fingerprint(&mut baseline);
        crate::runtime::model::apply_container_spec_fingerprint(&mut hardened);

        assert_ne!(
            baseline.labels.get(CONTAINER_SPEC_HASH_LABEL),
            hardened.labels.get(CONTAINER_SPEC_HASH_LABEL)
        );
    }

    #[test]
    fn explicit_resource_controls_compile_to_bounded_docker_flags() {
        let reaper = ContainerSecurityOptions {
            init_process: true,
            ..Default::default()
        };
        assert_eq!(container_resource_args(&reaper).unwrap(), ["--init=true"]);
        assert!(
            !serde_json::to_string(&ContainerSecurityOptions::default())
                .unwrap()
                .contains("init_process")
        );
        let security = ContainerSecurityOptions {
            memory_limit_mb: Some(256),
            memory_swap_limit_mb: Some(256),
            shared_memory_mb: Some(1),
            disable_core_dumps: true,
            log_options: Some(ContainerLogOptions::Disabled),
            ..Default::default()
        };
        assert_eq!(
            container_resource_args(&security).unwrap(),
            [
                "--memory-swap",
                "256m",
                "--shm-size",
                "1m",
                "--ulimit",
                "core=0:0",
                "--log-driver",
                "none"
            ]
        );
        let bounded = ContainerSecurityOptions {
            log_options: Some(ContainerLogOptions::BoundedJson {
                max_size_mb: 1,
                max_files: 2,
            }),
            ..Default::default()
        };
        assert_eq!(
            container_resource_args(&bounded).unwrap(),
            [
                "--log-driver",
                "json-file",
                "--log-opt",
                "max-size=1m",
                "--log-opt",
                "max-file=2"
            ]
        );
        let tmpfs = ContainerTmpfsMount {
            path: "/state".to_owned(),
            size_mb: Some(192),
            owner_uid: Some(10004),
            owner_gid: Some(10004),
            mode: Some(0o700),
            no_exec: true,
            no_suid: true,
            no_dev: true,
        };
        assert_eq!(
            format_tmpfs(&tmpfs),
            "/state:size=192m,uid=10004,gid=10004,mode=700,noexec,nosuid,nodev"
        );
        let sticky = ContainerTmpfsMount {
            path: "/tmp".into(),
            mode: Some(0o1777),
            ..tmpfs
        };
        assert_eq!(
            format_tmpfs(&sticky),
            "/tmp:size=192m,uid=10004,gid=10004,mode=1777,noexec,nosuid,nodev"
        );
        container_resource_args(&ContainerSecurityOptions {
            tmpfs: vec![sticky.clone()],
            ..Default::default()
        })
        .unwrap();
        for mode in [0o2777, 0o4777, 0o10000, u16::MAX] {
            assert!(
                container_resource_args(&ContainerSecurityOptions {
                    tmpfs: vec![ContainerTmpfsMount {
                        mode: Some(mode),
                        ..sticky.clone()
                    }],
                    ..Default::default()
                })
                .is_err()
            );
        }
        assert!(
            container_resource_args(&ContainerSecurityOptions::default())
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn invalid_resource_controls_fail_before_docker_side_effects() {
        for security in [
            ContainerSecurityOptions {
                memory_swap_limit_mb: Some(256),
                ..Default::default()
            },
            ContainerSecurityOptions {
                memory_limit_mb: Some(256),
                memory_swap_limit_mb: Some(128),
                ..Default::default()
            },
            ContainerSecurityOptions {
                shared_memory_mb: Some(0),
                ..Default::default()
            },
            ContainerSecurityOptions {
                log_options: Some(ContainerLogOptions::BoundedJson {
                    max_size_mb: 0,
                    max_files: 2,
                }),
                ..Default::default()
            },
            ContainerSecurityOptions {
                log_options: Some(ContainerLogOptions::BoundedJson {
                    max_size_mb: 1,
                    max_files: 0,
                }),
                ..Default::default()
            },
            ContainerSecurityOptions {
                tmpfs: vec![ContainerTmpfsMount {
                    path: "/bad:exec".to_owned(),
                    no_exec: true,
                    ..Default::default()
                }],
                ..Default::default()
            },
            ContainerSecurityOptions {
                tmpfs: vec![ContainerTmpfsMount {
                    mode: Some(0o7777),
                    ..Default::default()
                }],
                ..Default::default()
            },
        ] {
            assert!(container_resource_args(&security).is_err());
        }
    }

    #[test]
    fn unknown_or_unlimited_inspect_controls_are_not_normalized_into_safe_values() {
        let unknown = DockerRuntimeManager::extract_runtime_security(&json!({"HostConfig":{
            "MemorySwap":-1,"LogConfig":{"Type":"none","Config":{"malformed":1}},
            "Ulimits":[{"Name":"core","Soft":0,"Hard":0},{"Name":"core","Soft":-1,"Hard":-1}]
        }}));
        assert_eq!(unknown.memory_swap_limit_bytes, Some(-1));
        assert!(unknown.log_config.is_none());
        assert!(unknown.core_dump_limit.is_none());
        assert_eq!(unknown.privileged, None);
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
