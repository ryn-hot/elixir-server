use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio::process::Command;

use crate::runtime::RuntimeManager;
use crate::runtime::model::{
    ContainerHandle, ContainerSpec, ContainerState, PortMapping, VolumeMount,
};

const REQUIRED_LABELS: [&str; 2] = ["elixir.instance_id", "elixir.extension_id"];

pub struct DockerRuntimeManager {
    docker_bin: String,
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

    async fn create_container(&self, spec: &ContainerSpec) -> Result<ContainerHandle> {
        Self::ensure_required_labels(&spec.labels)?;

        let mut args = vec![
            "run".to_string(),
            "-d".to_string(),
            "--name".to_string(),
            spec.name.clone(),
            "--network".to_string(),
            spec.network.clone(),
        ];

        for alias in &spec.aliases {
            args.push("--network-alias".to_string());
            args.push(alias.clone());
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
        if spec.network.trim().is_empty() {
            bail!("container network is required");
        }
        if spec.image.trim().is_empty() {
            bail!("container image is required");
        }

        self.ensure_network(&spec.network).await?;

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
}
