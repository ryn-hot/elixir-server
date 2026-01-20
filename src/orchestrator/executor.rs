use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use quick_xml::Reader;
use quick_xml::events::Event;
use serde::Deserialize;
use sqlx::AnyPool;
use tokio::fs;
use tokio::time::sleep;
use uuid::Uuid;

use crate::db::models::{BindingStatus, Provider, ProviderHealthState, SecretScope, SlotCardinality};
use crate::drivers::{ApplyStatus, DriverCtx, DriverPatch, DriverRegistry};
use crate::extensions::manifest::{ManifestNetworking, ManifestRuntime};
use crate::extensions::store::{
    ExtensionStore, NewBinding, NewExtensionInstance, NewProvider, NewSecret,
};
use crate::orchestrator::bindings::ensure_binding_connectivity;
use crate::orchestrator::model::ProviderEndpoint;
use crate::runtime::model::{ContainerSpec, EnvVar, PortMapping, VolumeMount};
use crate::runtime::probe::ProbeRunner;
use crate::runtime::{RuntimeManager, RuntimePaths};

pub enum ExecutorAction {
    EnsureInstanceInstalled {
        instance_id: Uuid,
        extension_id: String,
        instance_name: String,
        config_json: Option<serde_json::Value>,
        enabled: bool,
    },
    EnsureRuntimeRunning {
        instance_id: Uuid,
        extension_id: String,
        instance_name: String,
        runtime: ManifestRuntime,
        networking: Option<ManifestNetworking>,
        aliases: Vec<String>,
    },
    HealthGate {
        provider_id: Uuid,
        timeout_seconds: u64,
    },
    CreateOrUpdateProvider {
        provider_id: Uuid,
        instance_id: Uuid,
        capability: String,
        slot_id: String,
        cardinality: SlotCardinality,
        implementation: Option<String>,
        endpoint: ProviderEndpoint,
    },
    ApplyDriverPatch {
        connector_extension_id: String,
        target_provider_id: Uuid,
        patch: serde_json::Value,
    },
    ApplyBinding {
        binding: NewBinding,
        consumer_endpoint: ProviderEndpoint,
        provider_endpoint: ProviderEndpoint,
        reverse_probe: bool,
    },
}

pub struct Executor<'a> {
    store: ExtensionStore<'a>,
    probe: &'a dyn ProbeRunner,
    drivers: &'a DriverRegistry,
    runtime: &'a dyn RuntimeManager,
    runtime_paths: RuntimePaths,
}

impl<'a> Executor<'a> {
    pub fn new(
        pool: &'a AnyPool,
        probe: &'a dyn ProbeRunner,
        drivers: &'a DriverRegistry,
        runtime: &'a dyn RuntimeManager,
        runtime_paths: RuntimePaths,
    ) -> Self {
        Self {
            store: ExtensionStore::new(pool),
            probe,
            drivers,
            runtime,
            runtime_paths,
        }
    }

    pub async fn apply(&self, action: ExecutorAction) -> Result<()> {
        match action {
            ExecutorAction::EnsureInstanceInstalled {
                instance_id,
                extension_id,
                instance_name,
                config_json,
                enabled,
            } => {
                self.ensure_instance_installed(
                    instance_id,
                    extension_id,
                    instance_name,
                    config_json,
                    enabled,
                )
                .await
            }
            ExecutorAction::EnsureRuntimeRunning {
                instance_id,
                extension_id,
                instance_name,
                runtime,
                networking,
                aliases,
            } => {
                self.ensure_runtime_running(
                    instance_id,
                    extension_id,
                    instance_name,
                    runtime,
                    networking,
                    aliases,
                )
                .await
            }
            ExecutorAction::HealthGate {
                provider_id,
                timeout_seconds,
            } => self.health_gate(provider_id, timeout_seconds).await,
            ExecutorAction::CreateOrUpdateProvider {
                provider_id,
                instance_id,
                capability,
                slot_id,
                cardinality,
                implementation,
                endpoint,
            } => {
                self.create_or_update_provider(
                    provider_id,
                    instance_id,
                    capability,
                    slot_id,
                    cardinality,
                    implementation,
                    endpoint,
                )
                .await
            }
            ExecutorAction::ApplyDriverPatch {
                connector_extension_id,
                target_provider_id,
                patch,
            } => self
                .apply_driver_patch(connector_extension_id, target_provider_id, patch)
                .await,
            ExecutorAction::ApplyBinding {
                binding,
                consumer_endpoint,
                provider_endpoint,
                reverse_probe,
            } => {
                self.apply_binding(binding, consumer_endpoint, provider_endpoint, reverse_probe)
                    .await
            }
        }
    }

    async fn ensure_instance_installed(
        &self,
        instance_id: Uuid,
        extension_id: String,
        instance_name: String,
        config_json: Option<serde_json::Value>,
        enabled: bool,
    ) -> Result<()> {
        if self.store.get_instance(instance_id).await?.is_some() {
            return Ok(());
        }
        self.store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id,
                instance_name,
                config_json,
                enabled,
            })
            .await?;
        Ok(())
    }

    async fn ensure_runtime_running(
        &self,
        instance_id: Uuid,
        extension_id: String,
        _instance_name: String,
        runtime: ManifestRuntime,
        _networking: Option<ManifestNetworking>,
        aliases: Vec<String>,
    ) -> Result<()> {
        if runtime.r#type != "container" {
            bail!("runtime type '{}' is not supported yet", runtime.r#type);
        }
        let image = runtime
            .image
            .clone()
            .ok_or_else(|| anyhow::anyhow!("runtime.image is required"))?;
        let network = runtime
            .network
            .clone()
            .unwrap_or_else(|| "elixir_net".to_string());
        if network != "elixir_net" {
            bail!("runtime network must be 'elixir_net'");
        }

        let name = format!("elx-{}", short_instance_id(instance_id));
        let mut alias_list = aliases;
        if let Some(service_name) = runtime.service_name.clone() {
            if !alias_list.contains(&service_name) {
                alias_list.push(service_name);
            }
        }

        let mut labels = HashMap::new();
        labels.insert("elixir.instance_id".to_string(), instance_id.to_string());
        labels.insert("elixir.extension_id".to_string(), extension_id.clone());
        labels.insert("elixir.managed".to_string(), "true".to_string());

        let env = runtime
            .env
            .into_iter()
            .map(|env| match env.value {
                Some(value) => Ok(EnvVar {
                    name: env.name,
                    value,
                }),
                None => bail!("runtime.env '{}' requires secrets support", env.name),
            })
            .collect::<Result<Vec<_>>>()?;

        let volumes = runtime
            .volumes
            .iter()
            .map(|volume| resolve_volume_mount(volume, &self.runtime_paths))
            .collect::<Result<Vec<_>>>()?;
        let runtime_volumes = volumes.clone();

        let ports = runtime
            .ports
            .iter()
            .map(|port| PortMapping {
                container_port: port.container,
                host_port: port.host,
                protocol: None,
            })
            .collect();

        let spec = ContainerSpec {
            name,
            image,
            network,
            aliases: alias_list,
            env,
            volumes,
            ports,
            labels,
            command: Vec::new(),
        };

        self.runtime.ensure_network(&spec.network).await?;
        self.runtime.ensure_container(&spec).await?;
        persist_runtime_config(&self.store, instance_id, &runtime_volumes).await?;
        Ok(())
    }

    async fn create_or_update_provider(
        &self,
        provider_id: Uuid,
        instance_id: Uuid,
        capability: String,
        slot_id: String,
        cardinality: SlotCardinality,
        implementation: Option<String>,
        endpoint: ProviderEndpoint,
    ) -> Result<()> {
        let endpoint_json = serde_json::to_value(endpoint).context("serializing provider endpoint")?;
        self.store
            .upsert_provider(&NewProvider {
                provider_id,
                instance_id,
                capability,
                slot_id,
                cardinality,
                implementation,
                endpoint_json: Some(endpoint_json),
                health_state: ProviderHealthState::Unknown,
            })
            .await?;
        Ok(())
    }

    async fn apply_driver_patch(
        &self,
        connector_extension_id: String,
        target_provider_id: Uuid,
        patch: serde_json::Value,
    ) -> Result<()> {
        let provider = self
            .store
            .get_provider(target_provider_id)
            .await?
            .ok_or_else(|| {
                anyhow!(
                    "provider {} not found for connector {}",
                    target_provider_id,
                    connector_extension_id
                )
            })?;

        ensure_provider_healthy(&provider)?;

        let endpoint_json = provider
            .endpoint_json
            .ok_or_else(|| anyhow!("provider {} has no endpoint", provider.provider_id))?;
        let endpoint: ProviderEndpoint =
            serde_json::from_value(endpoint_json).context("parsing provider endpoint")?;
        endpoint.validate()?;

        let instance = self
            .store
            .get_instance(provider.instance_id)
            .await?
            .ok_or_else(|| {
                anyhow!(
                    "instance {} not found for provider {}",
                    provider.instance_id,
                    provider.provider_id
                )
            })?;

        let driver = self.drivers.get(&provider.capability).ok_or_else(|| {
            anyhow!(
                "no driver registered for capability '{}' (connector {})",
                provider.capability,
                connector_extension_id
            )
        })?;

        let patch =
            DriverPatch::from_manifest(&provider.capability, patch).context("parsing driver patch")?;
        patch.validate().context("validating driver patch")?;

        let mut secrets = HashMap::new();
        if provider.capability == "media.manager.tv"
            && provider.implementation.as_deref() == Some("sonarr")
        {
            let api_key = resolve_sonarr_api_key(&self.store, &instance).await?;
            secrets.insert("sonarr_api_key".to_string(), api_key);
        }

        let ctx = DriverCtx::new(
            provider.provider_id,
            provider.instance_id,
            provider.capability.clone(),
            endpoint,
            provider.implementation.clone(),
            instance.config_json.clone(),
            secrets,
        );
        let result = driver.apply_patch(ctx, patch).await?;
        if result.status == ApplyStatus::Deferred {
            let detail = result
                .message
                .unwrap_or_else(|| "driver patch deferred".to_string());
            bail!(
                "driver patch deferred for connector {}: {}",
                connector_extension_id,
                detail
            );
        }
        Ok(())
    }

    async fn apply_binding(
        &self,
        binding: NewBinding,
        consumer_endpoint: ProviderEndpoint,
        provider_endpoint: ProviderEndpoint,
        reverse_probe: bool,
    ) -> Result<()> {
        if let Err(err) = ensure_binding_connectivity(
            self.probe,
            &consumer_endpoint,
            &provider_endpoint,
            reverse_probe,
        )
        .await
        {
            let _ = self
                .store
                .update_binding_status(
                    binding.binding_id,
                    BindingStatus::Failed,
                    Some(&err.to_string()),
                )
                .await;
            return Err(err);
        }

        let mut applied = binding;
        applied.status = BindingStatus::Applied;
        self.store.upsert_binding(&applied).await?;
        self.store
            .update_binding_status(applied.binding_id, BindingStatus::Applied, None)
            .await?;
        Ok(())
    }

    async fn health_gate(&self, provider_id: Uuid, timeout_seconds: u64) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(timeout_seconds.max(1));
        loop {
            match self.health_gate_once(provider_id).await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    if Instant::now() >= deadline {
                        let _ = self
                            .store
                            .update_provider_health(
                                provider_id,
                                ProviderHealthState::Unhealthy,
                            )
                            .await;
                        return Err(err);
                    }
                }
            }
            sleep(Duration::from_secs(2)).await;
        }
    }

    async fn health_gate_once(&self, provider_id: Uuid) -> Result<()> {
        let provider = self
            .store
            .get_provider(provider_id)
            .await?
            .ok_or_else(|| anyhow!("provider {} not found", provider_id))?;

        let endpoint_json = provider
            .endpoint_json
            .ok_or_else(|| anyhow!("provider {} has no endpoint", provider.provider_id))?;
        let endpoint: ProviderEndpoint =
            serde_json::from_value(endpoint_json).context("parsing provider endpoint")?;
        endpoint.validate()?;

        let instance = self
            .store
            .get_instance(provider.instance_id)
            .await?
            .ok_or_else(|| {
                anyhow!(
                    "instance {} not found for provider {}",
                    provider.instance_id,
                    provider.provider_id
                )
            })?;

        if provider.capability == "media.manager.tv"
            && provider.implementation.as_deref() == Some("sonarr")
        {
            let config = parse_sonarr_instance_config(instance.config_json.as_ref())?;
            if let Some(config_dir) = config.config_dir {
                if let Some(key) = read_sonarr_api_key_from_config(&config_dir).await? {
                    upsert_sonarr_secret(&self.store, provider.instance_id, &key).await?;
                    self.probe
                        .probe_dns(&endpoint.host)
                        .await
                        .and_then(|result| ensure_probe_ok(result, "dns"))?;
                    self.probe
                        .probe_tcp(&endpoint.host, endpoint.port)
                        .await
                        .and_then(|result| ensure_probe_ok(result, "tcp"))?;
                    self.store
                        .update_provider_health(provider.provider_id, ProviderHealthState::Healthy)
                        .await?;
                    return Ok(());
                }
            }
            bail!("sonarr config.xml not ready");
        }

        self.probe
            .probe_dns(&endpoint.host)
            .await
            .and_then(|result| ensure_probe_ok(result, "dns"))?;
        self.probe
            .probe_tcp(&endpoint.host, endpoint.port)
            .await
            .and_then(|result| ensure_probe_ok(result, "tcp"))?;
        self.store
            .update_provider_health(provider.provider_id, ProviderHealthState::Healthy)
            .await?;
        Ok(())
    }
}

async fn resolve_sonarr_api_key(
    store: &ExtensionStore<'_>,
    instance: &crate::db::models::ExtensionInstance,
) -> Result<String> {
    let config = parse_sonarr_instance_config(instance.config_json.as_ref())?;
    if let Some(key) = config.api_key {
        upsert_sonarr_secret(store, instance.instance_id, &key).await?;
        return Ok(key);
    }

    if let Some(secret) = store
        .get_secret(SecretScope::Instance, Some(instance.instance_id), "sonarr_api_key")
        .await?
    {
        return Ok(secret.value_encrypted);
    }

    if let Some(config_dir) = config.config_dir {
        if let Some(key) = read_sonarr_api_key_from_config(&config_dir).await? {
            upsert_sonarr_secret(store, instance.instance_id, &key).await?;
            return Ok(key);
        }
    }

    bail!("sonarr api key is not available yet");
}

async fn upsert_sonarr_secret(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    api_key: &str,
) -> Result<()> {
    store
        .upsert_secret(&NewSecret {
            secret_id: Uuid::new_v4(),
            scope: SecretScope::Instance,
            scope_id: Some(instance_id),
            key: "sonarr_api_key".to_string(),
            value_encrypted: api_key.to_string(),
            rotatable: false,
        })
        .await
}

#[derive(Default, Deserialize)]
struct SonarrInstanceConfig {
    #[serde(default)]
    api_key: Option<String>,
    #[serde(default)]
    runtime: Option<SonarrRuntimeConfig>,
}

#[derive(Default, Deserialize)]
struct SonarrRuntimeConfig {
    #[serde(default)]
    config_dir: Option<String>,
}

struct ParsedSonarrConfig {
    api_key: Option<String>,
    config_dir: Option<String>,
}

fn parse_sonarr_instance_config(value: Option<&serde_json::Value>) -> Result<ParsedSonarrConfig> {
    let Some(value) = value else {
        return Ok(ParsedSonarrConfig {
            api_key: None,
            config_dir: None,
        });
    };
    let config: SonarrInstanceConfig =
        serde_json::from_value(value.clone()).context("parsing sonarr instance config")?;
    Ok(ParsedSonarrConfig {
        api_key: config.api_key,
        config_dir: config.runtime.and_then(|runtime| runtime.config_dir),
    })
}

async fn read_sonarr_api_key_from_config(config_dir: &str) -> Result<Option<String>> {
    let path = Path::new(config_dir).join("config.xml");
    let content = match fs::read_to_string(&path).await {
        Ok(content) => content,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    parse_sonarr_api_key(&content)
}

fn parse_sonarr_api_key(xml: &str) -> Result<Option<String>> {
    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);
    let mut buf = Vec::new();
    let mut in_key = false;
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(event)) => {
                if event.name().as_ref() == b"ApiKey" {
                    in_key = true;
                }
            }
            Ok(Event::End(event)) => {
                if event.name().as_ref() == b"ApiKey" {
                    in_key = false;
                }
            }
            Ok(Event::Text(event)) if in_key => {
                let value = event.unescape().context("decoding ApiKey")?;
                return Ok(Some(value.to_string()));
            }
            Ok(Event::Eof) => break,
            Err(err) => return Err(err.into()),
            _ => {}
        }
        buf.clear();
    }
    Ok(None)
}

fn ensure_probe_ok(result: crate::runtime::probe::ProbeResult, stage: &str) -> Result<()> {
    if result.ok {
        return Ok(());
    }
    let details = result
        .details
        .unwrap_or_else(|| serde_json::json!({ "stage": stage }));
    bail!("probe {stage} failed: {details}");
}

async fn persist_runtime_config(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    volumes: &[VolumeMount],
) -> Result<()> {
    let config_dir = volumes
        .iter()
        .find(|volume| volume.container_path == "/config")
        .map(|volume| volume.host_path.clone());
    if config_dir.is_none() {
        return Ok(());
    }
    let instance = store
        .get_instance(instance_id)
        .await?
        .ok_or_else(|| anyhow!("instance {} not found", instance_id))?;
    let updated =
        merge_runtime_config(instance.config_json, config_dir, volumes).context("runtime config")?;
    if let Some(updated) = updated {
        store
            .update_instance_config(instance_id, Some(&updated))
            .await?;
    }
    Ok(())
}

fn merge_runtime_config(
    existing: Option<serde_json::Value>,
    config_dir: Option<String>,
    volumes: &[VolumeMount],
) -> Result<Option<serde_json::Value>> {
    let mut changed = false;
    let mut root = match existing {
        Some(serde_json::Value::Object(map)) => map,
        Some(_) => {
            tracing::warn!("instance config is not an object; skipping runtime config merge");
            return Ok(None);
        }
        None => serde_json::Map::new(),
    };

    let runtime_entry = root
        .entry("runtime".to_string())
        .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
    let runtime = runtime_entry
        .as_object_mut()
        .ok_or_else(|| anyhow!("runtime config is not an object"))?;

    if let Some(config_dir) = config_dir {
        if !runtime.contains_key("config_dir") {
            runtime.insert("config_dir".to_string(), serde_json::Value::String(config_dir));
            changed = true;
        }
    }

    if !runtime.contains_key("volumes") {
        let volume_values = volumes
            .iter()
            .map(|volume| {
                serde_json::json!({
                    "host_path": volume.host_path,
                    "container_path": volume.container_path,
                    "read_only": volume.read_only,
                })
            })
            .collect::<Vec<_>>();
        runtime.insert("volumes".to_string(), serde_json::Value::Array(volume_values));
        changed = true;
    }

    if changed {
        Ok(Some(serde_json::Value::Object(root)))
    } else {
        Ok(None)
    }
}

fn ensure_provider_healthy(provider: &Provider) -> Result<()> {
    match provider.health_state {
        ProviderHealthState::Healthy | ProviderHealthState::Degraded => Ok(()),
        ProviderHealthState::Unknown => bail!(
            "provider {} health is unknown; cannot apply driver patches",
            provider.provider_id
        ),
        ProviderHealthState::Unhealthy => bail!(
            "provider {} is unhealthy; cannot apply driver patches",
            provider.provider_id
        ),
    }
}

fn resolve_volume_mount(raw: &str, paths: &RuntimePaths) -> Result<VolumeMount> {
    let parts: Vec<&str> = raw.split(':').collect();
    if parts.len() < 2 || parts.len() > 3 {
        bail!("invalid volume specification '{}'", raw);
    }
    let host_raw = parts[0];
    let container_path = parts[1];
    let read_only = parts.get(2).map(|v| *v == "ro").unwrap_or(false);
    if parts.len() == 3 && !read_only {
        bail!("volume spec '{}' has invalid mode '{}'", raw, parts[2]);
    }

    let host_path = resolve_placeholders(host_raw, paths)?;
    Ok(VolumeMount {
        host_path,
        container_path: container_path.to_string(),
        read_only,
    })
}

fn resolve_placeholders(raw: &str, paths: &RuntimePaths) -> Result<String> {
    let mut resolved = raw.to_string();
    resolved = resolved.replace("{data}", &paths.data_root);
    resolved = resolved.replace("{downloads}", &paths.downloads_root);
    resolved = resolved.replace("{media}", &paths.media_root);
    if resolved.contains('{') {
        bail!("unknown placeholder in path '{}'", raw);
    }
    Ok(resolved)
}

fn short_instance_id(instance_id: Uuid) -> String {
    let raw = instance_id.simple().to_string();
    raw.chars().take(6).collect()
}

pub fn new_binding_id() -> Uuid {
    Uuid::new_v4()
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Mutex;

    use async_trait::async_trait;
    use serde_json::json;
    use tempfile::TempDir;

    use crate::config::DatabaseConfig;
    use crate::db::Database;
    use crate::db::models::{ExtensionKind, ExtensionTrustLevel, ProviderHealthState, SlotCardinality};
    use crate::extensions::store::{ExtensionStore, NewExtension, NewExtensionInstance, NewProvider};
    use crate::runtime::model::{ContainerHandle, ContainerSpec, ContainerState};

    #[derive(Default)]
    struct StubProbe {
        calls: Mutex<Vec<String>>,
    }

    impl StubProbe {
        fn calls(&self) -> Vec<String> {
            self.calls.lock().expect("probe calls lock").clone()
        }
    }

    #[async_trait]
    impl ProbeRunner for StubProbe {
        async fn probe_dns(&self, name: &str) -> Result<crate::runtime::probe::ProbeResult> {
            self.calls
                .lock()
                .expect("probe calls lock")
                .push(format!("dns:{name}"));
            Ok(crate::runtime::probe::ProbeResult {
                ok: true,
                latency_ms: Some(1),
                details: None,
            })
        }

        async fn probe_tcp(
            &self,
            host: &str,
            port: u16,
        ) -> Result<crate::runtime::probe::ProbeResult> {
            self.calls
                .lock()
                .expect("probe calls lock")
                .push(format!("tcp:{host}:{port}"));
            Ok(crate::runtime::probe::ProbeResult {
                ok: true,
                latency_ms: Some(1),
                details: None,
            })
        }

        async fn probe_http(&self, url: &str) -> Result<crate::runtime::probe::ProbeResult> {
            self.calls
                .lock()
                .expect("probe calls lock")
                .push(format!("http:{url}"));
            Ok(crate::runtime::probe::ProbeResult {
                ok: true,
                latency_ms: Some(1),
                details: None,
            })
        }
    }

    struct StubRuntime;

    #[async_trait]
    impl RuntimeManager for StubRuntime {
        async fn ensure_network(&self, _name: &str) -> Result<()> {
            Ok(())
        }

        async fn ensure_container(&self, _spec: &ContainerSpec) -> Result<ContainerHandle> {
            bail!("unexpected runtime call")
        }

        async fn stop_container(&self, _handle: &ContainerHandle) -> Result<()> {
            bail!("unexpected runtime call")
        }

        async fn container_logs(
            &self,
            _handle: &ContainerHandle,
            _since: Option<chrono::DateTime<chrono::Utc>>,
        ) -> Result<String> {
            bail!("unexpected runtime call")
        }

        async fn inspect(&self, _handle: &ContainerHandle) -> Result<ContainerState> {
            bail!("unexpected runtime call")
        }
    }

    async fn setup_db() -> Result<Database> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        Ok(database)
    }

    async fn insert_extension(
        store: &ExtensionStore<'_>,
        extension_id: &str,
        manifest_json: serde_json::Value,
    ) -> Result<()> {
        store
            .upsert_extension(&NewExtension {
                extension_id: extension_id.to_string(),
                name: extension_id.to_string(),
                version: "1.0.0".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: None,
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Verified,
                manifest_json,
                package_hash: None,
                enabled: true,
            })
            .await
    }

    fn sonarr_manifest(id: &str) -> serde_json::Value {
        json!({
            "id": id,
            "version": "1.0.0",
            "kind": "module",
            "name": id,
            "provides": [
                {
                    "capability": "media.manager.tv",
                    "slot": "default",
                    "cardinality": "one",
                    "implementation": "sonarr"
                }
            ],
            "runtime": {
                "type": "container",
                "image": "example/sonarr:latest"
            }
        })
    }

    #[tokio::test]
    async fn sonarr_health_gate_reads_config_xml() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        insert_extension(&store, "ext.sonarr", sonarr_manifest("ext.sonarr")).await?;

        let temp_dir = TempDir::new()?;
        let config_dir = temp_dir.path().join("sonarr-config");
        std::fs::create_dir_all(&config_dir)?;
        let config_path = config_dir.join("config.xml");
        std::fs::write(
            &config_path,
            "<Config><ApiKey>test-api-key</ApiKey></Config>",
        )?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "ext.sonarr".to_string(),
                instance_name: "default".to_string(),
                config_json: Some(json!({
                    "runtime": {
                        "config_dir": config_dir.to_string_lossy()
                    }
                })),
                enabled: true,
            })
            .await?;

        let provider_id = Uuid::new_v4();
        let endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "svc-sonarr".to_string(),
            8989,
            None,
            Some("elixir_net".to_string()),
        )?;
        store
            .upsert_provider(&NewProvider {
                provider_id,
                instance_id,
                capability: "media.manager.tv".to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: Some("sonarr".to_string()),
                endpoint_json: Some(serde_json::to_value(endpoint)?),
                health_state: ProviderHealthState::Unknown,
            })
            .await?;

        let probe = StubProbe::default();
        let drivers = DriverRegistry::new();
        let runtime = StubRuntime;
        let runtime_paths = RuntimePaths::from_roots(
            temp_dir.path().join("data").join("extensions").to_string_lossy().as_ref(),
            temp_dir.path().to_string_lossy().as_ref(),
        );

        let executor = Executor::new(
            &database.pool,
            &probe,
            &drivers,
            &runtime,
            runtime_paths,
        );

        executor.health_gate(provider_id, 5).await?;

        let provider = store
            .get_provider(provider_id)
            .await?
            .expect("provider");
        assert_eq!(provider.health_state, ProviderHealthState::Healthy);

        let secret = store
            .get_secret(SecretScope::Instance, Some(instance_id), "sonarr_api_key")
            .await?
            .expect("sonarr api key secret");
        assert_eq!(secret.value_encrypted, "test-api-key");

        let calls = probe.calls();
        assert!(calls.iter().any(|call| call == "dns:svc-sonarr"));
        assert!(calls.iter().any(|call| call == "tcp:svc-sonarr:8989"));
        Ok(())
    }
}
