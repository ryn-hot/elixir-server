use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::Deserialize;
use tokio::process::Command;
use tokio::time::timeout;

#[derive(Debug, Clone)]
pub struct ProbeConfig {
    pub docker_bin: String,
    pub network: String,
    pub image: String,
    pub builder_image: String,
    pub probe_binary_path: PathBuf,
    pub bundled_probe_binary_path: Option<PathBuf>,
    pub probe_workspace_root: PathBuf,
    pub probe_manifest_path: PathBuf,
    pub target_triple: Option<String>,
    pub allow_utility_fallback: bool,
}

impl ProbeConfig {
    pub fn with_storage_root(storage_root: &str) -> Self {
        let bundled_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("extensions/bundled");
        Self::with_storage_and_bundled_dirs(storage_root, bundled_dir.to_string_lossy().as_ref())
    }

    pub fn with_storage_and_bundled_dirs(storage_root: &str, bundled_dir: &str) -> Self {
        let storage_root = absolutize_path(storage_root);
        let probe_workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap_or_else(|| Path::new(env!("CARGO_MANIFEST_DIR")))
            .to_path_buf();
        let bundled_probe_binary_path =
            bundled_probe_binary_path(bundled_dir, std::env::consts::ARCH);
        Self {
            docker_bin: "docker".to_string(),
            network: "elixir_net".to_string(),
            image: "alpine:3.19".to_string(),
            builder_image: "rust:1.88".to_string(),
            probe_binary_path: storage_root.join("probe").join("elixir-probe"),
            bundled_probe_binary_path,
            probe_manifest_path: probe_workspace_root
                .join("crates")
                .join("elixir_probe")
                .join("Cargo.toml"),
            probe_workspace_root,
            target_triple: linux_musl_target_for_arch(std::env::consts::ARCH)
                .map(ToOwned::to_owned),
            allow_utility_fallback: true,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ProbeResult {
    pub ok: bool,
    pub latency_ms: Option<u64>,
    #[serde(default)]
    pub details: Option<serde_json::Value>,
}

#[async_trait]
pub trait ProbeRunner: Send + Sync {
    async fn probe_dns(&self, name: &str) -> Result<ProbeResult>;
    async fn probe_tcp(&self, host: &str, port: u16) -> Result<ProbeResult>;
    async fn probe_http(&self, url: &str) -> Result<ProbeResult>;

    async fn assert_reachable(&self, host: &str, port: u16, url: Option<&str>) -> Result<()> {
        ensure_ok(self.probe_dns(host).await?, "dns")?;
        ensure_ok(self.probe_tcp(host, port).await?, "tcp")?;
        if let Some(url) = url {
            ensure_ok(self.probe_http(url).await?, "http")?;
        }
        Ok(())
    }
}

pub struct NetworkProbe {
    config: ProbeConfig,
    binary_disabled: AtomicBool,
    build_attempted: AtomicBool,
}

impl NetworkProbe {
    pub fn new(config: ProbeConfig) -> Self {
        Self {
            config,
            binary_disabled: AtomicBool::new(false),
            build_attempted: AtomicBool::new(false),
        }
    }

    pub async fn probe_dns(&self, name: &str) -> Result<ProbeResult> {
        self.run_probe(&["dns", name]).await
    }

    pub async fn probe_tcp(&self, host: &str, port: u16) -> Result<ProbeResult> {
        self.run_probe(&["tcp", host, &port.to_string()]).await
    }

    pub async fn probe_http(&self, url: &str) -> Result<ProbeResult> {
        self.run_probe(&["http", url]).await
    }

    pub async fn assert_reachable(&self, host: &str, port: u16, url: Option<&str>) -> Result<()> {
        <Self as ProbeRunner>::assert_reachable(self, host, port, url).await
    }

    pub async fn prepare_binary(&self) -> Result<()> {
        self.ensure_probe_binary_ready().await
    }

    async fn run_probe(&self, args: &[&str]) -> Result<ProbeResult> {
        if !self.binary_disabled.load(Ordering::Relaxed) {
            match self.ensure_probe_binary_ready().await {
                Ok(()) => return self.run_binary_probe(args).await,
                Err(err) if self.config.allow_utility_fallback => {
                    self.binary_disabled.store(true, Ordering::Relaxed);
                    tracing::warn!(
                        "probe binary at '{}' is not usable in the probe container; switching to utility fallback: {}",
                        self.config.probe_binary_path.display(),
                        err
                    );
                }
                Err(err) => return Err(err),
            }
        }
        if self.config.allow_utility_fallback {
            return self.run_utility_probe(args).await;
        }
        self.ensure_probe_binary()?;
        unreachable!("ensure_probe_binary always returns an error when binary is missing")
    }

    async fn ensure_probe_binary_ready(&self) -> Result<()> {
        match self.probe_binary_status() {
            ProbeBinaryStatus::Usable => Ok(()),
            ProbeBinaryStatus::Missing | ProbeBinaryStatus::Invalid(_) => {
                self.stage_bundled_probe_binary()?;

                if matches!(self.probe_binary_status(), ProbeBinaryStatus::Usable) {
                    return Ok(());
                }

                if !self.build_attempted.swap(true, Ordering::Relaxed) {
                    self.build_linux_probe_binary().await?;
                }

                match self.probe_binary_status() {
                    ProbeBinaryStatus::Usable => Ok(()),
                    ProbeBinaryStatus::Missing => self.ensure_probe_binary(),
                    ProbeBinaryStatus::Invalid(reason) => bail!(
                        "probe binary at '{}' is not usable: {}",
                        self.config.probe_binary_path.display(),
                        reason
                    ),
                }
            }
        }
    }

    async fn run_binary_probe(&self, args: &[&str]) -> Result<ProbeResult> {
        let mut cmd = Command::new(&self.config.docker_bin);
        cmd.arg("run")
            .arg("--rm")
            .arg("--network")
            .arg(&self.config.network)
            .arg("-v")
            .arg(format!(
                "{}:/probe:ro",
                self.config.probe_binary_path.display()
            ))
            .arg("--entrypoint")
            .arg("/probe")
            .arg(&self.config.image);

        for arg in args {
            cmd.arg(arg);
        }

        let output = timeout(Duration::from_secs(20), cmd.output())
            .await
            .context("probe command timed out")?
            .context("running probe container")?;
        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        let stderr_trimmed = stderr.trim();

        if !output.status.success()
            && self.config.allow_utility_fallback
            && should_fallback_from_binary_error(stderr_trimmed)
        {
            self.binary_disabled.store(true, Ordering::Relaxed);
            tracing::warn!(
                "probe binary failed compatibility checks; switching to utility fallback: {}",
                stderr_trimmed
            );
            return self.run_utility_probe(args).await;
        }

        if stdout.trim().is_empty() {
            bail!("probe returned no output: {}", stderr.trim());
        }

        let parsed: ProbeResult = serde_json::from_str(&stdout).context("parsing probe output")?;

        if !output.status.success() && parsed.ok {
            bail!("probe exited with error: {}", stderr.trim());
        }

        Ok(parsed)
    }

    async fn build_linux_probe_binary(&self) -> Result<()> {
        let target_triple = self.config.target_triple.as_deref().ok_or_else(|| {
            anyhow::anyhow!("unsupported host architecture '{}'", std::env::consts::ARCH)
        })?;
        if !self.config.probe_manifest_path.is_file() {
            bail!(
                "probe source manifest not found at {}; packaged builds should ship a prebuilt Linux probe binary",
                self.config.probe_manifest_path.display()
            );
        }

        let output_dir = self
            .config
            .probe_binary_path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("probe binary path has no parent"))?;
        fs::create_dir_all(output_dir)
            .with_context(|| format!("creating probe output dir {}", output_dir.display()))?;

        let manifest_rel = self
            .config
            .probe_manifest_path
            .strip_prefix(&self.config.probe_workspace_root)
            .with_context(|| {
                format!(
                    "probe manifest '{}' is not inside workspace '{}'",
                    self.config.probe_manifest_path.display(),
                    self.config.probe_workspace_root.display()
                )
            })?;
        let manifest_rel = posix_path(manifest_rel);

        let script = format!(
            "set -e; \
             export PATH=/usr/local/cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin; \
             export DEBIAN_FRONTEND=noninteractive; \
             apt-get update >/dev/null; \
             apt-get install -y musl-tools pkg-config build-essential >/dev/null; \
             rustup target add {target_triple} >/dev/null; \
             cargo build --release --quiet --manifest-path /workspace/{manifest_rel} --target {target_triple}; \
             cp /tmp/target/{target_triple}/release/elixir-probe /out/elixir-probe; \
             chmod 755 /out/elixir-probe"
        );

        tracing::info!(
            "building Linux probe binary with Docker for target {}",
            target_triple
        );

        let mut cmd = Command::new(&self.config.docker_bin);
        cmd.arg("run")
            .arg("--rm")
            .arg("--entrypoint")
            .arg("/bin/sh")
            .arg("-v")
            .arg(format!(
                "{}:/workspace:ro",
                self.config.probe_workspace_root.display()
            ))
            .arg("-v")
            .arg(format!("{}:/out", output_dir.display()))
            .arg("-e")
            .arg("CARGO_TARGET_DIR=/tmp/target")
            .arg("-w")
            .arg("/workspace")
            .arg(&self.config.builder_image)
            .arg("-lc")
            .arg(script);

        let output = timeout(Duration::from_secs(600), cmd.output())
            .await
            .context("probe build command timed out")?
            .context("running probe builder container")?;
        if !output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            let diagnostics = if !stderr.is_empty() {
                stderr
            } else if !stdout.is_empty() {
                stdout
            } else {
                "no output".to_string()
            };
            bail!(
                "building Linux probe binary failed (status {:?}): {}",
                output.status.code(),
                diagnostics
            );
        }

        set_probe_permissions(&self.config.probe_binary_path)?;

        Ok(())
    }

    async fn run_utility_probe(&self, args: &[&str]) -> Result<ProbeResult> {
        let (entrypoint, utility_args): (&str, Vec<String>) = match args {
            ["dns", name] => ("nslookup", vec![(*name).to_string()]),
            ["tcp", host, port] => (
                "nc",
                vec![
                    "-z".to_string(),
                    "-w".to_string(),
                    "3".to_string(),
                    (*host).to_string(),
                    (*port).to_string(),
                ],
            ),
            ["http", url] => (
                "wget",
                vec![
                    "-q".to_string(),
                    "-T".to_string(),
                    "5".to_string(),
                    "-O".to_string(),
                    "/dev/null".to_string(),
                    (*url).to_string(),
                ],
            ),
            _ => bail!("invalid probe invocation arguments: {:?}", args),
        };

        let mut cmd = Command::new(&self.config.docker_bin);
        cmd.arg("run")
            .arg("--rm")
            .arg("--network")
            .arg(&self.config.network)
            .arg("--entrypoint")
            .arg(entrypoint)
            .arg(&self.config.image)
            .args(&utility_args);

        let output = cmd.output();
        let output = timeout(Duration::from_secs(20), output)
            .await
            .context("utility probe command timed out")?
            .with_context(|| format!("running fallback probe utility '{entrypoint}'"))?;
        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();

        if output.status.success() {
            return Ok(ProbeResult {
                ok: true,
                latency_ms: None,
                details: Some(serde_json::json!({
                    "mode": "utility_fallback",
                    "entrypoint": entrypoint
                })),
            });
        }

        Ok(ProbeResult {
            ok: false,
            latency_ms: None,
            details: Some(serde_json::json!({
                "mode": "utility_fallback",
                "entrypoint": entrypoint,
                "stdout": stdout.trim(),
                "stderr": stderr.trim(),
                "exit_code": output.status.code()
            })),
        })
    }

    fn probe_binary_exists(&self) -> bool {
        Path::new(&self.config.probe_binary_path).is_file()
    }

    fn probe_binary_status(&self) -> ProbeBinaryStatus {
        Self::probe_binary_status_for(Path::new(&self.config.probe_binary_path))
    }

    fn probe_binary_status_for(path: &Path) -> ProbeBinaryStatus {
        if !path.is_file() {
            return ProbeBinaryStatus::Missing;
        }

        match read_probe_magic(path) {
            Ok(ProbeBinaryMagic::Elf) => ProbeBinaryStatus::Usable,
            Ok(ProbeBinaryMagic::MachO) => ProbeBinaryStatus::Invalid(
                "found a Mach-O host binary; the probe container requires a Linux ELF binary"
                    .to_string(),
            ),
            Ok(ProbeBinaryMagic::Unknown(bytes)) => ProbeBinaryStatus::Invalid(format!(
                "unexpected binary format (magic bytes: {})",
                bytes
            )),
            Err(err) => ProbeBinaryStatus::Invalid(format!("failed to inspect binary: {err}")),
        }
    }

    fn stage_bundled_probe_binary(&self) -> Result<()> {
        let Some(source_path) = self.config.bundled_probe_binary_path.as_ref() else {
            return Ok(());
        };

        match Self::probe_binary_status_for(source_path) {
            ProbeBinaryStatus::Missing => Ok(()),
            ProbeBinaryStatus::Usable => {
                let output_dir = self
                    .config
                    .probe_binary_path
                    .parent()
                    .ok_or_else(|| anyhow::anyhow!("probe binary path has no parent"))?;
                fs::create_dir_all(output_dir).with_context(|| {
                    format!("creating bundled probe output dir {}", output_dir.display())
                })?;
                fs::copy(source_path, &self.config.probe_binary_path).with_context(|| {
                    format!(
                        "copying bundled probe binary from '{}' to '{}'",
                        source_path.display(),
                        self.config.probe_binary_path.display()
                    )
                })?;
                set_probe_permissions(&self.config.probe_binary_path)?;
                Ok(())
            }
            ProbeBinaryStatus::Invalid(reason) => {
                tracing::warn!(
                    "bundled probe binary at '{}' is not usable: {}",
                    source_path.display(),
                    reason
                );
                Ok(())
            }
        }
    }

    fn ensure_probe_binary(&self) -> Result<()> {
        if self.probe_binary_exists() {
            return Ok(());
        }
        bail!(
            "probe binary not found at {}; build and place it before running probes",
            self.config.probe_binary_path.display()
        );
    }
}

#[async_trait]
impl ProbeRunner for NetworkProbe {
    async fn probe_dns(&self, name: &str) -> Result<ProbeResult> {
        NetworkProbe::probe_dns(self, name).await
    }

    async fn probe_tcp(&self, host: &str, port: u16) -> Result<ProbeResult> {
        NetworkProbe::probe_tcp(self, host, port).await
    }

    async fn probe_http(&self, url: &str) -> Result<ProbeResult> {
        NetworkProbe::probe_http(self, url).await
    }
}

fn ensure_ok(result: ProbeResult, stage: &str) -> Result<()> {
    if result.ok {
        return Ok(());
    }
    let details = result
        .details
        .unwrap_or_else(|| serde_json::json!({ "stage": stage }));
    bail!("probe {stage} failed: {details}");
}

fn should_fallback_from_binary_error(stderr: &str) -> bool {
    let lowered = stderr.to_ascii_lowercase();
    lowered.contains("exec format error")
        || lowered.contains("cannot execute binary file")
        || lowered.contains("no such file or directory")
        || lowered.contains("invalid mount config for type")
        || lowered.contains("mount path must be absolute")
        || lowered.contains("includes invalid characters for a local volume name")
}

fn absolutize_path(raw: &str) -> PathBuf {
    let path = PathBuf::from(raw);
    if path.is_absolute() {
        return path;
    }
    match std::env::current_dir() {
        Ok(cwd) => cwd.join(path),
        Err(_) => path,
    }
}

fn linux_musl_target_for_arch(arch: &str) -> Option<&'static str> {
    match arch {
        "x86_64" => Some("x86_64-unknown-linux-musl"),
        "aarch64" => Some("aarch64-unknown-linux-musl"),
        _ => None,
    }
}

fn bundled_probe_binary_path(bundled_dir: &str, arch: &str) -> Option<PathBuf> {
    linux_musl_target_for_arch(arch)?;
    let bundled_dir = absolutize_path(bundled_dir);
    let extension_root = bundled_dir
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or(bundled_dir);
    Some(
        extension_root
            .join("probe")
            .join("linux")
            .join(arch)
            .join("elixir-probe"),
    )
}

fn posix_path(path: &Path) -> String {
    path.components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

fn set_probe_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(path)
            .with_context(|| format!("reading probe binary metadata at {}", path.display()))?
            .permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms)
            .with_context(|| format!("setting probe binary permissions at {}", path.display()))?;
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ProbeBinaryStatus {
    Missing,
    Usable,
    Invalid(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ProbeBinaryMagic {
    Elf,
    MachO,
    Unknown(String),
}

fn read_probe_magic(path: &Path) -> Result<ProbeBinaryMagic> {
    let bytes = fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    if bytes.len() < 4 {
        bail!("binary is shorter than 4 bytes");
    }
    let magic = &bytes[..4];
    if magic == [0x7f, b'E', b'L', b'F'] {
        return Ok(ProbeBinaryMagic::Elf);
    }

    let mach_o_magics = [
        [0xfe, 0xed, 0xfa, 0xce],
        [0xce, 0xfa, 0xed, 0xfe],
        [0xfe, 0xed, 0xfa, 0xcf],
        [0xcf, 0xfa, 0xed, 0xfe],
        [0xca, 0xfe, 0xba, 0xbe],
        [0xbe, 0xba, 0xfe, 0xca],
        [0xca, 0xfe, 0xba, 0xbf],
        [0xbf, 0xba, 0xfe, 0xca],
    ];
    if mach_o_magics.iter().any(|candidate| magic == candidate) {
        return Ok(ProbeBinaryMagic::MachO);
    }

    Ok(ProbeBinaryMagic::Unknown(format!(
        "{:02x}{:02x}{:02x}{:02x}",
        magic[0], magic[1], magic[2], magic[3]
    )))
}

#[cfg(test)]
mod tests {
    use super::{
        NetworkProbe, ProbeBinaryMagic, ProbeConfig, bundled_probe_binary_path,
        linux_musl_target_for_arch, read_probe_magic, should_fallback_from_binary_error,
    };
    use std::fs;
    use std::path::PathBuf;
    use uuid::Uuid;

    #[test]
    fn detects_exec_format_error_for_binary_fallback() {
        assert!(should_fallback_from_binary_error(
            "exec /probe: exec format error"
        ));
        assert!(should_fallback_from_binary_error(
            "cannot execute binary file"
        ));
        assert!(should_fallback_from_binary_error(
            "no such file or directory"
        ));
        assert!(should_fallback_from_binary_error(
            "invalid mount config for type \"bind\": bind source path does not exist"
        ));
        assert!(should_fallback_from_binary_error(
            "create data/extensions/probe/elixir-probe: \"data/extensions/probe/elixir-probe\" includes invalid characters for a local volume name"
        ));
        assert!(!should_fallback_from_binary_error(
            "http probe failed with status 401"
        ));
    }

    #[test]
    fn detects_probe_binary_magic() {
        let temp_root =
            std::env::temp_dir().join(format!("elixir-probe-magic-{}", Uuid::new_v4().simple()));
        fs::create_dir_all(&temp_root).expect("create temp dir");

        let elf_path = temp_root.join("elf-probe");
        fs::write(&elf_path, [0x7f, b'E', b'L', b'F', 0, 0, 0, 0]).expect("write elf probe");
        assert_eq!(
            read_probe_magic(&elf_path).expect("read elf magic"),
            ProbeBinaryMagic::Elf
        );

        let macho_path = temp_root.join("macho-probe");
        fs::write(&macho_path, [0xcf, 0xfa, 0xed, 0xfe, 0, 0, 0, 0]).expect("write macho probe");
        assert_eq!(
            read_probe_magic(&macho_path).expect("read mach-o magic"),
            ProbeBinaryMagic::MachO
        );

        let unknown_path = temp_root.join("unknown-probe");
        fs::write(&unknown_path, [0x00, 0x01, 0x02, 0x03, 0, 0, 0, 0])
            .expect("write unknown probe");
        assert_eq!(
            read_probe_magic(&unknown_path).expect("read unknown magic"),
            ProbeBinaryMagic::Unknown("00010203".to_string())
        );

        fs::remove_dir_all(PathBuf::from(&temp_root)).expect("cleanup temp dir");
    }

    #[test]
    fn maps_linux_musl_target_for_supported_arches() {
        assert_eq!(
            linux_musl_target_for_arch("x86_64"),
            Some("x86_64-unknown-linux-musl")
        );
        assert_eq!(
            linux_musl_target_for_arch("aarch64"),
            Some("aarch64-unknown-linux-musl")
        );
        assert_eq!(linux_musl_target_for_arch("mips64"), None);
    }

    #[test]
    fn resolves_bundled_probe_path_from_bundled_dir() {
        let path = bundled_probe_binary_path("/opt/elixir/extensions/bundled", "x86_64")
            .expect("bundled probe path");
        assert_eq!(
            path,
            PathBuf::from("/opt/elixir/extensions/probe/linux/x86_64/elixir-probe")
        );
        assert!(bundled_probe_binary_path("/opt/elixir/extensions/bundled", "mips64").is_none());
    }

    #[tokio::test]
    async fn prepare_binary_stages_bundled_probe_without_source_build() {
        let temp_root =
            std::env::temp_dir().join(format!("elixir-probe-stage-{}", Uuid::new_v4().simple()));
        let storage_root = temp_root.join("storage");
        let staged_path = storage_root.join("probe").join("elixir-probe");
        let bundled_path = temp_root
            .join("extensions")
            .join("probe")
            .join("linux")
            .join("x86_64")
            .join("elixir-probe");
        fs::create_dir_all(bundled_path.parent().expect("bundled probe dir"))
            .expect("create bundled probe dir");
        fs::write(&bundled_path, [0x7f, b'E', b'L', b'F', 0, 0, 0, 0])
            .expect("write bundled probe");

        let probe = NetworkProbe::new(ProbeConfig {
            docker_bin: "docker".to_string(),
            network: "elixir_net".to_string(),
            image: "alpine:3.19".to_string(),
            builder_image: "rust:1.88".to_string(),
            probe_binary_path: staged_path.clone(),
            bundled_probe_binary_path: Some(bundled_path.clone()),
            probe_workspace_root: temp_root.clone(),
            probe_manifest_path: temp_root.join("missing").join("Cargo.toml"),
            target_triple: Some("x86_64-unknown-linux-musl".to_string()),
            allow_utility_fallback: true,
        });

        probe.prepare_binary().await.expect("prepare bundled probe");

        assert_eq!(
            read_probe_magic(&staged_path).expect("read staged probe magic"),
            ProbeBinaryMagic::Elf
        );

        fs::remove_dir_all(PathBuf::from(&temp_root)).expect("cleanup temp dir");
    }
}

#[cfg(all(test, feature = "docker-probe-tests"))]
mod docker_tests {
    use super::*;

    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::process::Command;
    use std::thread;
    use std::time::Duration;

    use anyhow::{Context, Result, bail};
    use uuid::Uuid;

    use crate::runtime::RuntimeManager;
    use crate::runtime::docker::DockerRuntimeManager;
    use crate::runtime::model::ContainerSpec;

    struct ContainerCleanup {
        name: String,
    }

    impl ContainerCleanup {
        fn new(name: String) -> Self {
            Self { name }
        }
    }

    impl Drop for ContainerCleanup {
        fn drop(&mut self) {
            let _ = Command::new("docker")
                .arg("rm")
                .arg("-f")
                .arg(&self.name)
                .status();
        }
    }

    #[tokio::test]
    async fn network_probe_runs_via_docker() -> Result<()> {
        ensure_docker_available()?;

        let workspace_root = workspace_root();
        let storage_root = workspace_root.join("data").join("extensions");

        let runtime = DockerRuntimeManager::new(None);
        runtime.ensure_network("elixir_net").await?;

        let suffix = short_id();
        let alias = format!("svc-probe-test-{suffix}");
        let name = format!("elixir-probe-test-{suffix}");

        let mut labels = HashMap::new();
        labels.insert("elixir.instance_id".to_string(), Uuid::new_v4().to_string());
        labels.insert("elixir.extension_id".to_string(), "elixir.test".to_string());
        labels.insert("elixir.managed".to_string(), "true".to_string());

        let spec = ContainerSpec {
            name: name.clone(),
            image: "hashicorp/http-echo:0.2.3".to_string(),
            network: "elixir_net".to_string(),
            network_mode: None,
            aliases: vec![alias.clone()],
            env: Vec::new(),
            volumes: Vec::new(),
            ports: Vec::new(),
            labels,
            command: vec![
                "-listen".to_string(),
                ":8080".to_string(),
                "-text".to_string(),
                "ok".to_string(),
            ],
            cap_add: Vec::new(),
            devices: Vec::new(),
            sysctls: HashMap::new(),
        };

        let _handle = runtime.ensure_container(&spec).await?;
        let _cleanup = ContainerCleanup::new(name);

        let probe = NetworkProbe::new(ProbeConfig::with_storage_root(
            storage_root.to_string_lossy().as_ref(),
        ));
        probe.prepare_binary().await?;
        let url = format!("http://{}:{}/", alias, 8080);
        let mut last_err = None;

        for _ in 0..10 {
            match probe.assert_reachable(&alias, 8080, Some(&url)).await {
                Ok(()) => {
                    last_err = None;
                    break;
                }
                Err(err) => {
                    last_err = Some(err);
                    thread::sleep(Duration::from_millis(500));
                }
            }
        }

        if let Some(err) = last_err {
            return Err(err);
        }

        Ok(())
    }

    fn workspace_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("workspace root")
            .to_path_buf()
    }

    fn ensure_docker_available() -> Result<()> {
        let output = Command::new("docker")
            .arg("version")
            .arg("--format")
            .arg("{{.Server.Version}}")
            .output()
            .context("checking docker availability")?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!("docker is not available: {}", stderr.trim());
        }
        Ok(())
    }

    fn short_id() -> String {
        let raw = Uuid::new_v4().simple().to_string();
        raw.chars().take(8).collect()
    }
}
