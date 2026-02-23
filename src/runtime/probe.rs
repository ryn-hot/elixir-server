use std::{
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
    pub probe_binary_path: PathBuf,
    pub allow_utility_fallback: bool,
}

impl ProbeConfig {
    pub fn with_storage_root(storage_root: &str) -> Self {
        let storage_root = absolutize_path(storage_root);
        Self {
            docker_bin: "docker".to_string(),
            network: "elixir_net".to_string(),
            image: "alpine:3.19".to_string(),
            probe_binary_path: storage_root.join("probe").join("elixir-probe"),
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
}

impl NetworkProbe {
    pub fn new(config: ProbeConfig) -> Self {
        Self {
            config,
            binary_disabled: AtomicBool::new(false),
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

    async fn run_probe(&self, args: &[&str]) -> Result<ProbeResult> {
        if !self.binary_disabled.load(Ordering::Relaxed) && self.probe_binary_exists() {
            return self.run_binary_probe(args).await;
        }
        if self.config.allow_utility_fallback {
            return self.run_utility_probe(args).await;
        }
        self.ensure_probe_binary()?;
        unreachable!("ensure_probe_binary always returns an error when binary is missing")
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

#[cfg(test)]
mod tests {
    use super::should_fallback_from_binary_error;

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
}

#[cfg(all(test, feature = "docker-probe-tests"))]
mod docker_tests {
    use super::*;

    use std::collections::HashMap;
    use std::fs;
    use std::path::{Path, PathBuf};
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
        let _probe_path = build_probe_binary(&workspace_root, &storage_root)?;

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
        };

        let _handle = runtime.ensure_container(&spec).await?;
        let _cleanup = ContainerCleanup::new(name);

        let probe = NetworkProbe::new(ProbeConfig::with_storage_root(
            storage_root.to_string_lossy().as_ref(),
        ));
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

    fn build_probe_binary(workspace_root: &Path, storage_root: &Path) -> Result<PathBuf> {
        let status = Command::new("cargo")
            .current_dir(workspace_root)
            .args(["build", "-p", "elixir-probe"])
            .status()
            .context("building elixir_probe")?;
        if !status.success() {
            bail!(
                "cargo build elixir_probe failed with status {:?}",
                status.code()
            );
        }

        let exe_suffix = std::env::consts::EXE_SUFFIX;
        let built_path = workspace_root
            .join("target")
            .join("debug")
            .join(format!("elixir-probe{exe_suffix}"));
        if !built_path.is_file() {
            bail!("probe binary not found at {}", built_path.display());
        }

        let dest_dir = storage_root.join("probe");
        fs::create_dir_all(&dest_dir).context("creating probe dir")?;
        let dest_path = dest_dir.join("elixir-probe");
        fs::copy(&built_path, &dest_path).context("copying probe binary")?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(&dest_path)?.permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&dest_path, perms)?;
        }

        Ok(dest_path)
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
