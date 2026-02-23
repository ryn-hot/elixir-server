use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use quick_xml::Reader;
use quick_xml::events::Event;
use rand::{Rng, distributions::Alphanumeric};
use reqwest::Url;
use serde::Deserialize;
use sqlx::AnyPool;
use tokio::fs;
use tokio::time::sleep;
use uuid::Uuid;

use crate::db::models::{
    BindingStatus, Provider, ProviderHealthState, SecretScope, SlotCardinality,
};
use crate::drivers::{ApplyStatus, DriverCtx, DriverPatch, DriverRegistry};
use crate::drivers::{
    DownloaderSpec, IndexerCredentialField, IndexerRegistryPatch, MediaManagerMoviesPatch,
    MediaManagerTvPatch,
};
use crate::extensions::manifest::{ManifestNetworking, ManifestRuntime, ManifestRuntimeEnv};
use crate::extensions::required_secrets::{
    missing_required_secrets_for_instance, required_secrets_from_runtime,
};
use crate::extensions::store::{
    ExtensionStore, NewBinding, NewExtensionInstance, NewProvider, NewSecret,
};
use crate::orchestrator::bindings::ensure_binding_connectivity;
use crate::orchestrator::model::ProviderEndpoint;
use crate::orchestrator::naming::container_name;
use crate::runtime::model::{ContainerHandle, ContainerSpec, EnvVar, PortMapping, VolumeMount};
use crate::runtime::probe::ProbeRunner;
use crate::runtime::{RuntimeManager, RuntimePaths};
use crate::secrets::SecretsManager;

pub enum ExecutorAction {
    EnsureInstanceInstalled {
        instance_id: Uuid,
        extension_id: String,
        instance_name: String,
        config_json: Option<serde_json::Value>,
        enabled: bool,
    },
    DeleteProvider {
        provider_id: Uuid,
    },
    EnsureRuntimeRunning {
        instance_id: Uuid,
        extension_id: String,
        instance_name: String,
        runtime: ManifestRuntime,
        networking: Option<ManifestNetworking>,
        aliases: Vec<String>,
    },
    RollbackRuntime {
        instance_id: Uuid,
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
        scope_json: Option<serde_json::Value>,
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
    secrets: &'a SecretsManager,
}

impl<'a> Executor<'a> {
    pub fn new(
        pool: &'a AnyPool,
        probe: &'a dyn ProbeRunner,
        drivers: &'a DriverRegistry,
        runtime: &'a dyn RuntimeManager,
        runtime_paths: RuntimePaths,
        secrets: &'a SecretsManager,
    ) -> Self {
        Self {
            store: ExtensionStore::new(pool),
            probe,
            drivers,
            runtime,
            runtime_paths,
            secrets,
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
            ExecutorAction::DeleteProvider { provider_id } => {
                self.delete_provider(provider_id).await
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
            ExecutorAction::RollbackRuntime { instance_id } => {
                self.rollback_runtime(instance_id).await
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
                scope_json,
                endpoint,
            } => {
                self.create_or_update_provider(
                    provider_id,
                    instance_id,
                    capability,
                    slot_id,
                    cardinality,
                    implementation,
                    scope_json,
                    endpoint,
                )
                .await
            }
            ExecutorAction::ApplyDriverPatch {
                connector_extension_id,
                target_provider_id,
                patch,
            } => {
                self.apply_driver_patch(connector_extension_id, target_provider_id, patch)
                    .await
            }
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

    pub async fn check_provider_health(&self, provider_id: Uuid) -> Result<()> {
        self.health_gate_once(provider_id).await
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

    async fn delete_provider(&self, provider_id: Uuid) -> Result<()> {
        self.store.delete_provider(provider_id).await?;
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
        let instance = self
            .store
            .get_instance(instance_id)
            .await?
            .ok_or_else(|| anyhow!("instance {} not found", instance_id))?;
        let extension = self
            .store
            .get_extension(&extension_id)
            .await?
            .ok_or_else(|| anyhow!("extension {} not found", extension_id))?;
        let desired_version = extension.version.clone();
        let current_version = instance.runtime_version.clone();
        let rollback_version = instance.rollback_version.clone();

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

        if is_qbittorrent_extension_id(&extension_id) {
            ensure_qbittorrent_secrets(&self.store, self.secrets, instance_id, &runtime.env)
                .await?;
        }
        ensure_runtime_secrets_present(&self.store, instance_id, &runtime.env).await?;

        let name = container_name(instance_id);
        let mut alias_list = aliases;
        if let Some(service_name) = runtime.service_name.clone() {
            if !alias_list.contains(&service_name) {
                alias_list.push(service_name);
            }
        }

        let mut labels = HashMap::new();
        labels.insert("elixir.instance_id".to_string(), instance_id.to_string());
        labels.insert("elixir.extension_id".to_string(), extension_id.clone());
        labels.insert(
            "elixir.extension_version".to_string(),
            desired_version.clone(),
        );
        labels.insert("elixir.managed".to_string(), "true".to_string());

        let env = resolve_runtime_env(&self.store, self.secrets, instance_id, runtime.env).await?;

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
            name: name.clone(),
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

        let needs_upgrade = current_version.as_deref() != Some(desired_version.as_str());
        if needs_upgrade {
            let rollback_name = format!("{name}-rollback");
            if let Some(handle) = self.runtime.get_container_handle(&rollback_name).await? {
                let _ = self.runtime.stop_container(&handle).await;
                let _ = self.runtime.remove_container(&handle).await;
            }

            let mut backup_created = false;
            if let Some(handle) = self.runtime.get_container_handle(&name).await? {
                let _ = self.runtime.stop_container(&handle).await;
                if let Err(err) = self.runtime.rename_container(&handle, &rollback_name).await {
                    tracing::warn!(
                        "upgrade: failed to rename container {} -> {}: {}",
                        handle.name,
                        rollback_name,
                        err
                    );
                } else {
                    backup_created = true;
                }
            }

            if let Err(err) = self.runtime.ensure_container(&spec).await {
                if let Some(handle) = self.runtime.get_container_handle(&name).await? {
                    let _ = self.runtime.remove_container(&handle).await;
                }
                if backup_created {
                    let rollback_handle = ContainerHandle {
                        id: rollback_name.clone(),
                        name: rollback_name.clone(),
                    };
                    if let Err(rename_err) =
                        self.runtime.rename_container(&rollback_handle, &name).await
                    {
                        tracing::warn!(
                            "upgrade: failed to restore rollback container {}: {}",
                            rollback_name,
                            rename_err
                        );
                    } else {
                        let _ = self
                            .runtime
                            .start_container(&ContainerHandle {
                                id: name.clone(),
                                name: name.clone(),
                            })
                            .await;
                    }
                }
                return Err(err);
            }

            persist_runtime_config(&self.store, instance_id, &runtime_volumes).await?;
            let next_rollback = if backup_created {
                current_version.as_deref().or(rollback_version.as_deref())
            } else {
                rollback_version.as_deref()
            };
            self.store
                .update_instance_runtime_version(instance_id, &desired_version, next_rollback)
                .await?;
            return Ok(());
        }

        self.runtime.ensure_container(&spec).await?;
        persist_runtime_config(&self.store, instance_id, &runtime_volumes).await?;
        if current_version.is_none() {
            self.store
                .update_instance_runtime_version(
                    instance_id,
                    &desired_version,
                    rollback_version.as_deref(),
                )
                .await?;
        }
        Ok(())
    }

    async fn rollback_runtime(&self, instance_id: Uuid) -> Result<()> {
        let instance = self
            .store
            .get_instance(instance_id)
            .await?
            .ok_or_else(|| anyhow!("instance {} not found", instance_id))?;
        let rollback_version = instance
            .rollback_version
            .clone()
            .ok_or_else(|| anyhow!("instance {} has no rollback version", instance_id))?;

        let name = container_name(instance_id);
        let rollback_name = format!("{name}-rollback");
        let rollback_handle = self
            .runtime
            .get_container_handle(&rollback_name)
            .await?
            .ok_or_else(|| anyhow!("rollback container '{}' not found", rollback_name))?;

        if let Some(handle) = self.runtime.get_container_handle(&name).await? {
            let _ = self.runtime.stop_container(&handle).await;
            let _ = self.runtime.remove_container(&handle).await;
        }

        let renamed = self
            .runtime
            .rename_container(&rollback_handle, &name)
            .await?;
        self.runtime.start_container(&renamed).await?;

        self.store
            .update_instance_runtime_version(instance_id, &rollback_version, None)
            .await?;
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
        scope_json: Option<serde_json::Value>,
        endpoint: ProviderEndpoint,
    ) -> Result<()> {
        let endpoint_json =
            serde_json::to_value(endpoint).context("serializing provider endpoint")?;
        self.store
            .upsert_provider(&NewProvider {
                provider_id,
                instance_id,
                capability,
                slot_id,
                cardinality,
                implementation,
                scope_json,
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

        let mut patch = DriverPatch::from_manifest(&provider.capability, patch)
            .context("parsing driver patch")?;
        patch.validate().context("validating driver patch")?;

        let mut secrets = HashMap::new();
        if provider.capability == "media.manager.tv"
            && provider.implementation.as_deref() == Some("sonarr")
        {
            let api_key = resolve_sonarr_api_key(&self.store, self.secrets, &instance).await?;
            secrets.insert("sonarr_api_key".to_string(), api_key);
        }
        if provider.capability == "media.manager.movies"
            && provider.implementation.as_deref() == Some("radarr")
        {
            let api_key = resolve_radarr_api_key(&self.store, self.secrets, &instance).await?;
            secrets.insert("radarr_api_key".to_string(), api_key.clone());
            secrets.insert("api_key".to_string(), api_key);
        }
        if provider.capability == "indexer.registry"
            && provider.implementation.as_deref() == Some("prowlarr")
        {
            let api_key = resolve_prowlarr_api_key(&self.store, self.secrets, &instance).await?;
            secrets.insert("prowlarr_api_key".to_string(), api_key);
        }
        if provider.capability == "downloader.torrent"
            && provider.implementation.as_deref() == Some("qbittorrent")
        {
            let (username, password) =
                resolve_qbittorrent_credentials(&self.store, self.secrets, &instance).await?;
            secrets.insert("qbittorrent_username".to_string(), username);
            secrets.insert("qbittorrent_password".to_string(), password);
        }

        resolve_indexer_credentials(&self.store, self.secrets, provider.instance_id, &mut patch)
            .await?;
        resolve_indexer_apps(&self.store, self.secrets, &mut patch).await?;
        resolve_downloader_credentials(&self.store, self.secrets, &mut patch).await?;

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
                            .update_provider_health(provider_id, ProviderHealthState::Unhealthy)
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
                    upsert_sonarr_secret(&self.store, self.secrets, provider.instance_id, &key)
                        .await?;
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
        if provider.capability == "media.manager.movies"
            && provider.implementation.as_deref() == Some("radarr")
        {
            let config = parse_radarr_instance_config(instance.config_json.as_ref())?;
            if let Some(config_dir) = config.config_dir {
                if let Some(key) = read_radarr_api_key_from_config(&config_dir).await? {
                    upsert_radarr_secret(&self.store, self.secrets, provider.instance_id, &key)
                        .await?;
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
            bail!("radarr config.xml not ready");
        }
        if provider.capability == "indexer.registry"
            && provider.implementation.as_deref() == Some("prowlarr")
        {
            let config = parse_prowlarr_instance_config(instance.config_json.as_ref())?;
            if let Some(config_dir) = config.config_dir {
                if let Some(key) = read_prowlarr_api_key_from_config(&config_dir).await? {
                    upsert_prowlarr_secret(&self.store, self.secrets, provider.instance_id, &key)
                        .await?;
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
            bail!("prowlarr config.xml not ready");
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
    secrets: &SecretsManager,
    instance: &crate::db::models::ExtensionInstance,
) -> Result<String> {
    let config = parse_sonarr_instance_config(instance.config_json.as_ref())?;
    if let Some(key) = config.api_key {
        upsert_sonarr_secret(store, secrets, instance.instance_id, &key).await?;
        return Ok(key);
    }

    if let Some(secret) = store
        .get_secret(
            SecretScope::Instance,
            Some(instance.instance_id),
            "sonarr_api_key",
        )
        .await?
    {
        return secrets.decrypt(&secret.value_encrypted);
    }

    if let Some(config_dir) = config.config_dir {
        if let Some(key) = read_sonarr_api_key_from_config(&config_dir).await? {
            upsert_sonarr_secret(store, secrets, instance.instance_id, &key).await?;
            return Ok(key);
        }
    }

    bail!("sonarr api key is not available yet");
}

async fn resolve_radarr_api_key(
    store: &ExtensionStore<'_>,
    secrets: &SecretsManager,
    instance: &crate::db::models::ExtensionInstance,
) -> Result<String> {
    let config = parse_radarr_instance_config(instance.config_json.as_ref())?;
    if let Some(key) = config.api_key {
        upsert_radarr_secret(store, secrets, instance.instance_id, &key).await?;
        return Ok(key);
    }

    if let Some(secret) = store
        .get_secret(
            SecretScope::Instance,
            Some(instance.instance_id),
            "radarr_api_key",
        )
        .await?
    {
        return secrets.decrypt(&secret.value_encrypted);
    }

    if let Some(config_dir) = config.config_dir {
        if let Some(key) = read_radarr_api_key_from_config(&config_dir).await? {
            upsert_radarr_secret(store, secrets, instance.instance_id, &key).await?;
            return Ok(key);
        }
    }

    bail!("radarr api key is not available yet");
}

async fn upsert_sonarr_secret(
    store: &ExtensionStore<'_>,
    secrets: &SecretsManager,
    instance_id: Uuid,
    api_key: &str,
) -> Result<()> {
    let encrypted = secrets.encrypt(api_key)?;
    store
        .upsert_secret(&NewSecret {
            secret_id: Uuid::new_v4(),
            scope: SecretScope::Instance,
            scope_id: Some(instance_id),
            key: "sonarr_api_key".to_string(),
            value_encrypted: encrypted,
            rotatable: false,
        })
        .await
}

async fn upsert_radarr_secret(
    store: &ExtensionStore<'_>,
    secrets: &SecretsManager,
    instance_id: Uuid,
    api_key: &str,
) -> Result<()> {
    let encrypted = secrets.encrypt(api_key)?;
    store
        .upsert_secret(&NewSecret {
            secret_id: Uuid::new_v4(),
            scope: SecretScope::Instance,
            scope_id: Some(instance_id),
            key: "radarr_api_key".to_string(),
            value_encrypted: encrypted,
            rotatable: false,
        })
        .await
}

async fn resolve_prowlarr_api_key(
    store: &ExtensionStore<'_>,
    secrets: &SecretsManager,
    instance: &crate::db::models::ExtensionInstance,
) -> Result<String> {
    let config = parse_prowlarr_instance_config(instance.config_json.as_ref())?;
    if let Some(key) = config.api_key {
        upsert_prowlarr_secret(store, secrets, instance.instance_id, &key).await?;
        return Ok(key);
    }

    if let Some(secret) = store
        .get_secret(
            SecretScope::Instance,
            Some(instance.instance_id),
            "prowlarr_api_key",
        )
        .await?
    {
        return secrets.decrypt(&secret.value_encrypted);
    }

    if let Some(config_dir) = config.config_dir {
        if let Some(key) = read_prowlarr_api_key_from_config(&config_dir).await? {
            upsert_prowlarr_secret(store, secrets, instance.instance_id, &key).await?;
            return Ok(key);
        }
    }

    bail!("prowlarr api key is not available yet");
}

async fn upsert_prowlarr_secret(
    store: &ExtensionStore<'_>,
    secrets: &SecretsManager,
    instance_id: Uuid,
    api_key: &str,
) -> Result<()> {
    let encrypted = secrets.encrypt(api_key)?;
    store
        .upsert_secret(&NewSecret {
            secret_id: Uuid::new_v4(),
            scope: SecretScope::Instance,
            scope_id: Some(instance_id),
            key: "prowlarr_api_key".to_string(),
            value_encrypted: encrypted,
            rotatable: false,
        })
        .await
}

async fn resolve_qbittorrent_credentials(
    store: &ExtensionStore<'_>,
    secrets: &SecretsManager,
    instance: &crate::db::models::ExtensionInstance,
) -> Result<(String, String)> {
    let config = parse_qbittorrent_instance_config(instance.config_json.as_ref())?;
    if let (Some(username), Some(password)) = (config.username, config.password) {
        upsert_qbittorrent_secrets(store, secrets, instance.instance_id, &username, &password)
            .await?;
        return Ok((username, password));
    }

    ensure_qbittorrent_credentials(store, secrets, instance.instance_id).await
}

async fn resolve_indexer_credentials(
    store: &ExtensionStore<'_>,
    secrets: &SecretsManager,
    instance_id: Uuid,
    patch: &mut DriverPatch,
) -> Result<()> {
    let indexers = match patch {
        DriverPatch::IndexerRegistry(IndexerRegistryPatch::RegisterIndexers { indexers }) => {
            indexers
        }
        DriverPatch::MediaManagerTv(MediaManagerTvPatch::SetIndexerRegistry { indexers }) => {
            indexers
        }
        _ => return Ok(()),
    };

    for indexer in indexers {
        let fields = indexer.credential_fields()?;
        for field in fields {
            let key = indexer.credential_secret_key(field);
            let secret = store
                .get_secret(SecretScope::Instance, Some(instance_id), &key)
                .await?
                .ok_or_else(|| {
                    anyhow!("missing required secret instance:{}:{}", instance_id, key)
                })?;
            let value = secrets.decrypt(&secret.value_encrypted)?;
            match field {
                IndexerCredentialField::ApiKey => {
                    indexer.api_key = Some(value);
                }
                IndexerCredentialField::Username => {
                    indexer
                        .settings
                        .insert("username".to_string(), serde_json::Value::String(value));
                }
                IndexerCredentialField::Password => {
                    indexer
                        .settings
                        .insert("password".to_string(), serde_json::Value::String(value));
                }
            }
        }
    }
    Ok(())
}

async fn ensure_qbittorrent_secrets(
    store: &ExtensionStore<'_>,
    secrets: &SecretsManager,
    instance_id: Uuid,
    env: &[ManifestRuntimeEnv],
) -> Result<()> {
    let mut requires_qbittorrent = false;
    for entry in env {
        if let Some(from_secret) = entry.from_secret.as_ref() {
            let trimmed = from_secret.trim();
            if trimmed == "instance:qbittorrent_username"
                || trimmed == "instance:qbittorrent_password"
            {
                requires_qbittorrent = true;
                break;
            }
        }
    }
    if !requires_qbittorrent {
        return Ok(());
    }
    let _ = ensure_qbittorrent_credentials(store, secrets, instance_id).await?;
    Ok(())
}

async fn ensure_qbittorrent_credentials(
    store: &ExtensionStore<'_>,
    secrets: &SecretsManager,
    instance_id: Uuid,
) -> Result<(String, String)> {
    let username = store
        .get_secret(
            SecretScope::Instance,
            Some(instance_id),
            "qbittorrent_username",
        )
        .await?
        .map(|secret| secrets.decrypt(&secret.value_encrypted))
        .transpose()?
        .filter(|value| !value.trim().is_empty());
    let password = store
        .get_secret(
            SecretScope::Instance,
            Some(instance_id),
            "qbittorrent_password",
        )
        .await?
        .map(|secret| secrets.decrypt(&secret.value_encrypted))
        .transpose()?
        .filter(|value| !value.trim().is_empty());

    match (username, password) {
        (Some(username), Some(password)) => Ok((username, password)),
        (None, None) => {
            let (username, password) = generate_qbittorrent_credentials();
            upsert_qbittorrent_secrets(store, secrets, instance_id, &username, &password).await?;
            Ok((username, password))
        }
        _ => bail!("qbittorrent credentials are partially configured"),
    }
}

fn generate_qbittorrent_credentials() -> (String, String) {
    let suffix: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(6)
        .map(char::from)
        .collect();
    let password: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(28)
        .map(char::from)
        .collect();
    (format!("elixir_{suffix}"), password)
}

fn is_qbittorrent_extension_id(extension_id: &str) -> bool {
    extension_id.to_ascii_lowercase().contains("qbittorrent")
}

async fn resolve_indexer_apps(
    store: &ExtensionStore<'_>,
    secrets: &SecretsManager,
    patch: &mut DriverPatch,
) -> Result<()> {
    let apps = match patch {
        DriverPatch::IndexerRegistry(IndexerRegistryPatch::RegisterApps { apps }) => apps,
        _ => return Ok(()),
    };
    for app in apps {
        if app
            .api_key
            .as_ref()
            .is_some_and(|value| !value.trim().is_empty())
        {
            continue;
        }
        let host_hint = url_host(&app.url);
        if is_sonarr_app(&app.implementation) {
            let instance = resolve_sonarr_instance_for_app(store, host_hint.as_deref()).await?;
            let api_key = resolve_sonarr_api_key(store, secrets, &instance).await?;
            app.api_key = Some(api_key);
        } else if is_radarr_app(&app.implementation) {
            let instance = resolve_radarr_instance_for_app(store, host_hint.as_deref()).await?;
            let api_key = resolve_radarr_api_key(store, secrets, &instance).await?;
            app.api_key = Some(api_key);
        }
    }
    Ok(())
}

async fn resolve_downloader_credentials(
    store: &ExtensionStore<'_>,
    secrets: &SecretsManager,
    patch: &mut DriverPatch,
) -> Result<()> {
    let downloaders = match patch {
        DriverPatch::MediaManagerTv(MediaManagerTvPatch::SetDownloaders { downloaders }) => {
            downloaders
        }
        DriverPatch::MediaManagerMovies(MediaManagerMoviesPatch::SetDownloaders {
            downloaders,
        }) => downloaders,
        _ => return Ok(()),
    };

    for downloader in downloaders {
        if !is_qbittorrent_downloader(&downloader.r#type) {
            continue;
        }
        if downloader_has_credentials(downloader) {
            continue;
        }
        let host_hint = url_host(&downloader.url);
        let instance =
            resolve_qbittorrent_instance_for_downloader(store, host_hint.as_deref()).await?;
        let (username, password) =
            resolve_qbittorrent_credentials(store, secrets, &instance).await?;
        if downloader_setting_missing(&downloader.settings, "username") {
            downloader
                .settings
                .insert("username".to_string(), serde_json::Value::String(username));
        }
        if downloader_setting_missing(&downloader.settings, "password") {
            downloader
                .settings
                .insert("password".to_string(), serde_json::Value::String(password));
        }
    }
    Ok(())
}

fn is_sonarr_app(implementation: &str) -> bool {
    implementation
        .trim()
        .to_ascii_lowercase()
        .starts_with("sonarr")
}

fn is_radarr_app(implementation: &str) -> bool {
    implementation
        .trim()
        .to_ascii_lowercase()
        .starts_with("radarr")
}

fn url_host(url: &str) -> Option<String> {
    Url::parse(url)
        .ok()
        .and_then(|parsed| parsed.host_str().map(|host| host.to_string()))
}

fn is_qbittorrent_downloader(implementation: &str) -> bool {
    implementation
        .trim()
        .to_ascii_lowercase()
        .starts_with("qbittorrent")
}

fn downloader_has_credentials(downloader: &DownloaderSpec) -> bool {
    !downloader_setting_missing(&downloader.settings, "username")
        && !downloader_setting_missing(&downloader.settings, "password")
}

fn downloader_setting_missing(settings: &HashMap<String, serde_json::Value>, key: &str) -> bool {
    match settings.get(key) {
        None => true,
        Some(serde_json::Value::Null) => true,
        Some(serde_json::Value::String(value)) => value.trim().is_empty(),
        Some(_) => false,
    }
}

async fn resolve_sonarr_instance_for_app(
    store: &ExtensionStore<'_>,
    host_hint: Option<&str>,
) -> Result<crate::db::models::ExtensionInstance> {
    let providers = store.list_provider_details().await?;
    let mut candidates = Vec::new();
    for detail in providers {
        if detail.provider.capability != "media.manager.tv" {
            continue;
        }
        if let Some(implementation) = detail.provider.implementation.as_deref() {
            if !implementation.eq_ignore_ascii_case("sonarr") {
                continue;
            }
        }
        candidates.push(detail);
    }

    if candidates.is_empty() {
        bail!("sonarr provider not found for prowlarr app registration");
    }

    let mut selected = None;
    if let Some(host) = host_hint {
        for detail in &candidates {
            let endpoint_json = detail.provider.endpoint_json.as_ref();
            let endpoint_json = match endpoint_json {
                Some(value) => value,
                None => continue,
            };
            let endpoint: ProviderEndpoint = match serde_json::from_value(endpoint_json.clone()) {
                Ok(endpoint) => endpoint,
                Err(_) => continue,
            };
            if endpoint.host == host {
                selected = Some(detail.clone());
                break;
            }
        }
    }

    let selected = if let Some(selected) = selected {
        selected
    } else if candidates.len() == 1 {
        candidates.remove(0)
    } else {
        bail!("multiple sonarr providers found; specify the elx-sonarr host in the app url");
    };

    store
        .get_instance(selected.provider.instance_id)
        .await?
        .ok_or_else(|| {
            anyhow!(
                "sonarr instance {} not found",
                selected.provider.instance_id
            )
        })
}

async fn resolve_radarr_instance_for_app(
    store: &ExtensionStore<'_>,
    host_hint: Option<&str>,
) -> Result<crate::db::models::ExtensionInstance> {
    let providers = store.list_provider_details().await?;
    let mut candidates = Vec::new();
    for detail in providers {
        if detail.provider.capability != "media.manager.movies" {
            continue;
        }
        if let Some(implementation) = detail.provider.implementation.as_deref() {
            if !implementation.eq_ignore_ascii_case("radarr") {
                continue;
            }
        }
        candidates.push(detail);
    }

    if candidates.is_empty() {
        bail!("radarr provider not found for prowlarr app registration");
    }

    let mut selected = None;
    if let Some(host) = host_hint {
        for detail in &candidates {
            let endpoint_json = detail.provider.endpoint_json.as_ref();
            let endpoint_json = match endpoint_json {
                Some(value) => value,
                None => continue,
            };
            let endpoint: ProviderEndpoint = match serde_json::from_value(endpoint_json.clone()) {
                Ok(endpoint) => endpoint,
                Err(_) => continue,
            };
            if endpoint.host == host {
                selected = Some(detail.clone());
                break;
            }
        }
    }

    let selected = if let Some(selected) = selected {
        selected
    } else if candidates.len() == 1 {
        candidates.remove(0)
    } else {
        bail!("multiple radarr providers found; specify the elx-radarr host in the app url");
    };

    store
        .get_instance(selected.provider.instance_id)
        .await?
        .ok_or_else(|| {
            anyhow!(
                "radarr instance {} not found",
                selected.provider.instance_id
            )
        })
}

async fn resolve_qbittorrent_instance_for_downloader(
    store: &ExtensionStore<'_>,
    host_hint: Option<&str>,
) -> Result<crate::db::models::ExtensionInstance> {
    let providers = store.list_provider_details().await?;
    let mut candidates = Vec::new();
    for detail in providers {
        if detail.provider.capability != "downloader.torrent" {
            continue;
        }
        if let Some(implementation) = detail.provider.implementation.as_deref() {
            if !implementation.eq_ignore_ascii_case("qbittorrent") {
                continue;
            }
        }
        candidates.push(detail);
    }

    if candidates.is_empty() {
        bail!("qbittorrent provider not found for downloader credentials");
    }

    let mut selected = None;
    if let Some(host) = host_hint {
        for detail in &candidates {
            let endpoint_json = detail.provider.endpoint_json.as_ref();
            let endpoint_json = match endpoint_json {
                Some(value) => value,
                None => continue,
            };
            let endpoint: ProviderEndpoint = match serde_json::from_value(endpoint_json.clone()) {
                Ok(endpoint) => endpoint,
                Err(_) => continue,
            };
            if endpoint.host == host {
                selected = Some(detail.clone());
                break;
            }
        }
    }

    let selected = if let Some(selected) = selected {
        selected
    } else if candidates.len() == 1 {
        candidates.remove(0)
    } else {
        bail!(
            "multiple qbittorrent providers found; specify the elx-qbittorrent host in the downloader url"
        );
    };

    store
        .get_instance(selected.provider.instance_id)
        .await?
        .ok_or_else(|| {
            anyhow!(
                "qbittorrent instance {} not found",
                selected.provider.instance_id
            )
        })
}

async fn upsert_qbittorrent_secrets(
    store: &ExtensionStore<'_>,
    secrets: &SecretsManager,
    instance_id: Uuid,
    username: &str,
    password: &str,
) -> Result<()> {
    let encrypted_username = secrets.encrypt(username)?;
    store
        .upsert_secret(&NewSecret {
            secret_id: Uuid::new_v4(),
            scope: SecretScope::Instance,
            scope_id: Some(instance_id),
            key: "qbittorrent_username".to_string(),
            value_encrypted: encrypted_username,
            rotatable: false,
        })
        .await?;
    let encrypted_password = secrets.encrypt(password)?;
    store
        .upsert_secret(&NewSecret {
            secret_id: Uuid::new_v4(),
            scope: SecretScope::Instance,
            scope_id: Some(instance_id),
            key: "qbittorrent_password".to_string(),
            value_encrypted: encrypted_password,
            rotatable: false,
        })
        .await?;
    Ok(())
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

#[derive(Default, Deserialize)]
struct RadarrInstanceConfig {
    #[serde(default)]
    api_key: Option<String>,
    #[serde(default)]
    runtime: Option<RadarrRuntimeConfig>,
}

#[derive(Default, Deserialize)]
struct RadarrRuntimeConfig {
    #[serde(default)]
    config_dir: Option<String>,
}

#[derive(Default, Deserialize)]
struct ProwlarrInstanceConfig {
    #[serde(default)]
    api_key: Option<String>,
    #[serde(default)]
    runtime: Option<ProwlarrRuntimeConfig>,
}

#[derive(Default, Deserialize)]
struct ProwlarrRuntimeConfig {
    #[serde(default)]
    config_dir: Option<String>,
}

#[derive(Default, Deserialize)]
struct QbittorrentInstanceConfig {
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    password: Option<String>,
}

struct ParsedSonarrConfig {
    api_key: Option<String>,
    config_dir: Option<String>,
}

struct ParsedRadarrConfig {
    api_key: Option<String>,
    config_dir: Option<String>,
}

struct ParsedProwlarrConfig {
    api_key: Option<String>,
    config_dir: Option<String>,
}

struct ParsedQbittorrentConfig {
    username: Option<String>,
    password: Option<String>,
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

fn parse_radarr_instance_config(value: Option<&serde_json::Value>) -> Result<ParsedRadarrConfig> {
    let Some(value) = value else {
        return Ok(ParsedRadarrConfig {
            api_key: None,
            config_dir: None,
        });
    };
    let config: RadarrInstanceConfig =
        serde_json::from_value(value.clone()).context("parsing radarr instance config")?;
    Ok(ParsedRadarrConfig {
        api_key: config.api_key,
        config_dir: config.runtime.and_then(|runtime| runtime.config_dir),
    })
}

fn parse_prowlarr_instance_config(
    value: Option<&serde_json::Value>,
) -> Result<ParsedProwlarrConfig> {
    let Some(value) = value else {
        return Ok(ParsedProwlarrConfig {
            api_key: None,
            config_dir: None,
        });
    };
    let config: ProwlarrInstanceConfig =
        serde_json::from_value(value.clone()).context("parsing prowlarr instance config")?;
    Ok(ParsedProwlarrConfig {
        api_key: config.api_key,
        config_dir: config.runtime.and_then(|runtime| runtime.config_dir),
    })
}

fn parse_qbittorrent_instance_config(
    value: Option<&serde_json::Value>,
) -> Result<ParsedQbittorrentConfig> {
    let Some(value) = value else {
        return Ok(ParsedQbittorrentConfig {
            username: None,
            password: None,
        });
    };
    let config: QbittorrentInstanceConfig =
        serde_json::from_value(value.clone()).context("parsing qbittorrent instance config")?;
    Ok(ParsedQbittorrentConfig {
        username: config.username,
        password: config.password,
    })
}

async fn read_sonarr_api_key_from_config(config_dir: &str) -> Result<Option<String>> {
    read_arr_api_key_from_config(config_dir).await
}

async fn read_radarr_api_key_from_config(config_dir: &str) -> Result<Option<String>> {
    read_arr_api_key_from_config(config_dir).await
}

async fn read_prowlarr_api_key_from_config(config_dir: &str) -> Result<Option<String>> {
    read_arr_api_key_from_config(config_dir).await
}

async fn read_arr_api_key_from_config(config_dir: &str) -> Result<Option<String>> {
    let path = Path::new(config_dir).join("config.xml");
    let content = match fs::read_to_string(&path).await {
        Ok(content) => content,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    parse_arr_api_key(&content)
}

fn parse_arr_api_key(xml: &str) -> Result<Option<String>> {
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
    let updated = merge_runtime_config(instance.config_json, config_dir, volumes)
        .context("runtime config")?;
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
            runtime.insert(
                "config_dir".to_string(),
                serde_json::Value::String(config_dir),
            );
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
        runtime.insert(
            "volumes".to_string(),
            serde_json::Value::Array(volume_values),
        );
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

async fn ensure_runtime_secrets_present(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    env: &[ManifestRuntimeEnv],
) -> Result<()> {
    let required = required_secrets_from_runtime(env)?;
    if required.is_empty() {
        return Ok(());
    }
    let missing = missing_required_secrets_for_instance(store, instance_id, &required).await?;
    if missing.is_empty() {
        Ok(())
    } else {
        bail!("missing required secrets: {}", missing.join(", "));
    }
}

struct SecretReference {
    scope: SecretScope,
    scope_id: Option<Uuid>,
    key: String,
}

fn parse_secret_reference(raw: &str, instance_id: Uuid) -> Result<SecretReference> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("from_secret value must not be empty");
    }
    let parts: Vec<&str> = trimmed.split(':').collect();
    match parts.as_slice() {
        ["instance", key] => {
            if key.trim().is_empty() {
                bail!("from_secret instance key must not be empty");
            }
            Ok(SecretReference {
                scope: SecretScope::Instance,
                scope_id: Some(instance_id),
                key: (*key).to_string(),
            })
        }
        ["global", key] => {
            if key.trim().is_empty() {
                bail!("from_secret global key must not be empty");
            }
            Ok(SecretReference {
                scope: SecretScope::Global,
                scope_id: None,
                key: (*key).to_string(),
            })
        }
        ["provider", provider_id, key] => {
            if key.trim().is_empty() {
                bail!("from_secret provider key must not be empty");
            }
            let scope_id = Uuid::parse_str(provider_id)
                .map_err(|_| anyhow!("from_secret provider id is invalid"))?;
            Ok(SecretReference {
                scope: SecretScope::Provider,
                scope_id: Some(scope_id),
                key: (*key).to_string(),
            })
        }
        _ => bail!("from_secret must be instance:<key>, global:<key>, or provider:<uuid>:<key>"),
    }
}

async fn resolve_runtime_env(
    store: &ExtensionStore<'_>,
    secrets: &SecretsManager,
    instance_id: Uuid,
    env: Vec<ManifestRuntimeEnv>,
) -> Result<Vec<EnvVar>> {
    let mut resolved = Vec::with_capacity(env.len());
    for env in env {
        let name = env.name.clone();
        let value = match (env.value, env.from_secret) {
            (Some(value), None) => value,
            (None, Some(from_secret)) => {
                let reference = parse_secret_reference(&from_secret, instance_id)?;
                let secret = store
                    .get_secret(reference.scope, reference.scope_id, &reference.key)
                    .await?
                    .ok_or_else(|| {
                        anyhow!(
                            "secret '{}' not found for runtime.env '{}'",
                            reference.key,
                            name
                        )
                    })?;
                secrets
                    .decrypt(&secret.value_encrypted)
                    .with_context(|| format!("decrypting secret for runtime.env '{name}'"))?
            }
            (Some(_), Some(_)) => bail!(
                "runtime.env '{}' must not define both value and from_secret",
                name
            ),
            (None, None) => bail!("runtime.env '{}' requires value or from_secret", name),
        };
        resolved.push(EnvVar { name, value });
    }
    Ok(resolved)
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
    // Docker rejects host paths like `data/foo` as local volume names.
    // Normalize unresolved relative host paths to absolute host directories.
    if !resolved.contains("://") {
        let path = Path::new(&resolved);
        if !path.is_absolute() && (resolved.contains('/') || resolved.starts_with('.')) {
            if let Ok(cwd) = std::env::current_dir() {
                resolved = cwd.join(path).to_string_lossy().to_string();
            }
        }
    }
    Ok(resolved)
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
    use crate::db::models::{
        ExtensionKind, ExtensionTrustLevel, ProviderHealthState, SlotCardinality,
    };
    use crate::extensions::store::{
        ExtensionStore, NewExtension, NewExtensionInstance, NewProvider,
    };
    use crate::orchestrator::naming::container_name;
    use crate::runtime::model::{ContainerHandle, ContainerSpec, ContainerState};
    use crate::secrets::SecretsManager;

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

        async fn get_container_handle(&self, _name: &str) -> Result<Option<ContainerHandle>> {
            Ok(None)
        }

        async fn start_container(&self, _handle: &ContainerHandle) -> Result<()> {
            bail!("unexpected runtime call")
        }

        async fn stop_container(&self, _handle: &ContainerHandle) -> Result<()> {
            bail!("unexpected runtime call")
        }

        async fn rename_container(
            &self,
            _handle: &ContainerHandle,
            _new_name: &str,
        ) -> Result<ContainerHandle> {
            bail!("unexpected runtime call")
        }

        async fn remove_container(&self, _handle: &ContainerHandle) -> Result<()> {
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

    #[derive(Default)]
    struct CaptureRuntime {
        spec: Mutex<Option<ContainerSpec>>,
    }

    impl CaptureRuntime {
        fn last_spec(&self) -> Option<ContainerSpec> {
            self.spec.lock().expect("capture runtime lock").clone()
        }
    }

    #[async_trait]
    impl RuntimeManager for CaptureRuntime {
        async fn ensure_network(&self, _name: &str) -> Result<()> {
            Ok(())
        }

        async fn ensure_container(&self, spec: &ContainerSpec) -> Result<ContainerHandle> {
            *self.spec.lock().expect("capture runtime lock") = Some(spec.clone());
            Ok(ContainerHandle {
                id: "capture".to_string(),
                name: spec.name.clone(),
            })
        }

        async fn get_container_handle(&self, _name: &str) -> Result<Option<ContainerHandle>> {
            Ok(None)
        }

        async fn start_container(&self, _handle: &ContainerHandle) -> Result<()> {
            bail!("unexpected runtime call")
        }

        async fn stop_container(&self, _handle: &ContainerHandle) -> Result<()> {
            bail!("unexpected runtime call")
        }

        async fn rename_container(
            &self,
            _handle: &ContainerHandle,
            _new_name: &str,
        ) -> Result<ContainerHandle> {
            bail!("unexpected runtime call")
        }

        async fn remove_container(&self, _handle: &ContainerHandle) -> Result<()> {
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

    #[derive(Default)]
    struct UpgradeState {
        base_exists: bool,
        rollback_exists: bool,
    }

    struct UpgradeRuntime {
        calls: Mutex<Vec<String>>,
        base_name: String,
        state: Mutex<UpgradeState>,
        fail_create: bool,
    }

    impl UpgradeRuntime {
        fn new(base_name: String) -> Self {
            Self::new_with_failure(base_name, true)
        }

        fn new_with_failure(base_name: String, fail_create: bool) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                base_name,
                state: Mutex::new(UpgradeState {
                    base_exists: true,
                    rollback_exists: false,
                }),
                fail_create,
            }
        }

        fn calls(&self) -> Vec<String> {
            self.calls
                .lock()
                .expect("upgrade runtime calls lock")
                .clone()
        }
    }

    #[async_trait]
    impl RuntimeManager for UpgradeRuntime {
        async fn ensure_network(&self, _name: &str) -> Result<()> {
            self.calls
                .lock()
                .expect("upgrade runtime calls lock")
                .push("ensure_network".to_string());
            Ok(())
        }

        async fn ensure_container(&self, _spec: &ContainerSpec) -> Result<ContainerHandle> {
            self.calls
                .lock()
                .expect("upgrade runtime calls lock")
                .push("ensure_container".to_string());
            if self.fail_create {
                bail!("create failed");
            }
            let mut state = self.state.lock().expect("upgrade runtime state lock");
            state.base_exists = true;
            Ok(ContainerHandle {
                id: self.base_name.clone(),
                name: self.base_name.clone(),
            })
        }

        async fn get_container_handle(&self, name: &str) -> Result<Option<ContainerHandle>> {
            self.calls
                .lock()
                .expect("upgrade runtime calls lock")
                .push(format!("get:{name}"));
            let state = self.state.lock().expect("upgrade runtime state lock");
            if name == self.base_name && state.base_exists {
                return Ok(Some(ContainerHandle {
                    id: name.to_string(),
                    name: name.to_string(),
                }));
            }
            let rollback_name = format!("{}-rollback", self.base_name);
            if name == rollback_name && state.rollback_exists {
                return Ok(Some(ContainerHandle {
                    id: name.to_string(),
                    name: name.to_string(),
                }));
            }
            Ok(None)
        }

        async fn start_container(&self, handle: &ContainerHandle) -> Result<()> {
            self.calls
                .lock()
                .expect("upgrade runtime calls lock")
                .push(format!("start:{}", handle.name));
            Ok(())
        }

        async fn stop_container(&self, handle: &ContainerHandle) -> Result<()> {
            self.calls
                .lock()
                .expect("upgrade runtime calls lock")
                .push(format!("stop:{}", handle.name));
            Ok(())
        }

        async fn rename_container(
            &self,
            handle: &ContainerHandle,
            new_name: &str,
        ) -> Result<ContainerHandle> {
            self.calls
                .lock()
                .expect("upgrade runtime calls lock")
                .push(format!("rename:{}->{new_name}", handle.name));
            let mut state = self.state.lock().expect("upgrade runtime state lock");
            let rollback_name = format!("{}-rollback", self.base_name);
            if handle.name == self.base_name && new_name == rollback_name {
                state.base_exists = false;
                state.rollback_exists = true;
            } else if handle.name == rollback_name && new_name == self.base_name {
                state.rollback_exists = false;
                state.base_exists = true;
            }
            Ok(ContainerHandle {
                id: handle.id.clone(),
                name: new_name.to_string(),
            })
        }

        async fn remove_container(&self, handle: &ContainerHandle) -> Result<()> {
            self.calls
                .lock()
                .expect("upgrade runtime calls lock")
                .push(format!("remove:{}", handle.name));
            Ok(())
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

    #[derive(Default)]
    struct RollbackState {
        base_exists: bool,
        rollback_exists: bool,
    }

    struct RollbackRuntime {
        calls: Mutex<Vec<String>>,
        base_name: String,
        state: Mutex<RollbackState>,
    }

    impl RollbackRuntime {
        fn new(base_name: String) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                base_name,
                state: Mutex::new(RollbackState {
                    base_exists: true,
                    rollback_exists: true,
                }),
            }
        }

        fn calls(&self) -> Vec<String> {
            self.calls
                .lock()
                .expect("rollback runtime calls lock")
                .clone()
        }
    }

    #[async_trait]
    impl RuntimeManager for RollbackRuntime {
        async fn ensure_network(&self, _name: &str) -> Result<()> {
            self.calls
                .lock()
                .expect("rollback runtime calls lock")
                .push("ensure_network".to_string());
            Ok(())
        }

        async fn ensure_container(&self, _spec: &ContainerSpec) -> Result<ContainerHandle> {
            bail!("unexpected runtime call")
        }

        async fn get_container_handle(&self, name: &str) -> Result<Option<ContainerHandle>> {
            self.calls
                .lock()
                .expect("rollback runtime calls lock")
                .push(format!("get:{name}"));
            let state = self.state.lock().expect("rollback runtime state lock");
            if name == self.base_name && state.base_exists {
                return Ok(Some(ContainerHandle {
                    id: name.to_string(),
                    name: name.to_string(),
                }));
            }
            let rollback_name = format!("{}-rollback", self.base_name);
            if name == rollback_name && state.rollback_exists {
                return Ok(Some(ContainerHandle {
                    id: name.to_string(),
                    name: name.to_string(),
                }));
            }
            Ok(None)
        }

        async fn start_container(&self, handle: &ContainerHandle) -> Result<()> {
            self.calls
                .lock()
                .expect("rollback runtime calls lock")
                .push(format!("start:{}", handle.name));
            Ok(())
        }

        async fn stop_container(&self, handle: &ContainerHandle) -> Result<()> {
            self.calls
                .lock()
                .expect("rollback runtime calls lock")
                .push(format!("stop:{}", handle.name));
            Ok(())
        }

        async fn rename_container(
            &self,
            handle: &ContainerHandle,
            new_name: &str,
        ) -> Result<ContainerHandle> {
            self.calls
                .lock()
                .expect("rollback runtime calls lock")
                .push(format!("rename:{}->{new_name}", handle.name));
            let mut state = self.state.lock().expect("rollback runtime state lock");
            let rollback_name = format!("{}-rollback", self.base_name);
            if handle.name == rollback_name && new_name == self.base_name {
                state.rollback_exists = false;
                state.base_exists = true;
            }
            Ok(ContainerHandle {
                id: handle.id.clone(),
                name: new_name.to_string(),
            })
        }

        async fn remove_container(&self, handle: &ContainerHandle) -> Result<()> {
            self.calls
                .lock()
                .expect("rollback runtime calls lock")
                .push(format!("remove:{}", handle.name));
            let mut state = self.state.lock().expect("rollback runtime state lock");
            if handle.name == self.base_name {
                state.base_exists = false;
            }
            Ok(())
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

    fn runtime_env_manifest(id: &str) -> serde_json::Value {
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
                "image": "example/runtime:latest",
                "env": [
                    {
                        "name": "API_KEY",
                        "from_secret": "instance:api_key"
                    }
                ]
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
                scope_json: None,
                endpoint_json: Some(serde_json::to_value(endpoint)?),
                health_state: ProviderHealthState::Unknown,
            })
            .await?;

        let probe = StubProbe::default();
        let drivers = DriverRegistry::new();
        let runtime = StubRuntime;
        let runtime_paths = RuntimePaths::from_roots(
            temp_dir
                .path()
                .join("data")
                .join("extensions")
                .to_string_lossy()
                .as_ref(),
            temp_dir.path().to_string_lossy().as_ref(),
        );

        let secrets = SecretsManager::from_key_bytes([7u8; 32], true);
        let executor = Executor::new(
            &database.pool,
            &probe,
            &drivers,
            &runtime,
            runtime_paths,
            &secrets,
        );

        executor.health_gate(provider_id, 5).await?;

        let provider = store.get_provider(provider_id).await?.expect("provider");
        assert_eq!(provider.health_state, ProviderHealthState::Healthy);

        let secret = store
            .get_secret(SecretScope::Instance, Some(instance_id), "sonarr_api_key")
            .await?
            .expect("sonarr api key secret");
        let decrypted = secrets.decrypt(&secret.value_encrypted)?;
        assert_eq!(decrypted, "test-api-key");

        let calls = probe.calls();
        assert!(calls.iter().any(|call| call == "dns:svc-sonarr"));
        assert!(calls.iter().any(|call| call == "tcp:svc-sonarr:8989"));
        Ok(())
    }

    #[tokio::test]
    async fn runtime_env_resolves_instance_secret() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        insert_extension(&store, "ext.secret", runtime_env_manifest("ext.secret")).await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "ext.secret".to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;

        let secrets = SecretsManager::from_key_bytes([3u8; 32], false);
        let encrypted = secrets.encrypt("super-secret")?;
        store
            .upsert_secret(&NewSecret {
                secret_id: Uuid::new_v4(),
                scope: SecretScope::Instance,
                scope_id: Some(instance_id),
                key: "api_key".to_string(),
                value_encrypted: encrypted,
                rotatable: false,
            })
            .await?;

        let probe = StubProbe::default();
        let drivers = DriverRegistry::new();
        let runtime = CaptureRuntime::default();
        let temp_dir = TempDir::new()?;
        let runtime_paths = RuntimePaths::from_roots(
            temp_dir
                .path()
                .join("data")
                .join("extensions")
                .to_string_lossy()
                .as_ref(),
            temp_dir.path().to_string_lossy().as_ref(),
        );

        let executor = Executor::new(
            &database.pool,
            &probe,
            &drivers,
            &runtime,
            runtime_paths,
            &secrets,
        );

        let runtime_spec = ManifestRuntime {
            r#type: "container".to_string(),
            image: Some("example/runtime:latest".to_string()),
            network: None,
            service_name: None,
            ports: Vec::new(),
            volumes: Vec::new(),
            env: vec![ManifestRuntimeEnv {
                name: "API_KEY".to_string(),
                value: None,
                from_secret: Some("instance:api_key".to_string()),
            }],
        };

        executor
            .ensure_runtime_running(
                instance_id,
                "ext.secret".to_string(),
                "default".to_string(),
                runtime_spec,
                None,
                Vec::new(),
            )
            .await?;

        let spec = runtime.last_spec().expect("container spec captured");
        let env_value = spec
            .env
            .iter()
            .find(|env| env.name == "API_KEY")
            .map(|env| env.value.clone());
        assert_eq!(env_value.as_deref(), Some("super-secret"));
        Ok(())
    }

    #[tokio::test]
    async fn runtime_env_rejects_plaintext_secret_in_prod() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        insert_extension(&store, "ext.secret", runtime_env_manifest("ext.secret")).await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "ext.secret".to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;

        store
            .upsert_secret(&NewSecret {
                secret_id: Uuid::new_v4(),
                scope: SecretScope::Instance,
                scope_id: Some(instance_id),
                key: "api_key".to_string(),
                value_encrypted: "plaintext".to_string(),
                rotatable: false,
            })
            .await?;

        let probe = StubProbe::default();
        let drivers = DriverRegistry::new();
        let runtime = StubRuntime;
        let temp_dir = TempDir::new()?;
        let runtime_paths = RuntimePaths::from_roots(
            temp_dir
                .path()
                .join("data")
                .join("extensions")
                .to_string_lossy()
                .as_ref(),
            temp_dir.path().to_string_lossy().as_ref(),
        );

        let secrets = SecretsManager::from_key_bytes([9u8; 32], false);
        let executor = Executor::new(
            &database.pool,
            &probe,
            &drivers,
            &runtime,
            runtime_paths,
            &secrets,
        );

        let runtime_spec = ManifestRuntime {
            r#type: "container".to_string(),
            image: Some("example/runtime:latest".to_string()),
            network: None,
            service_name: None,
            ports: Vec::new(),
            volumes: Vec::new(),
            env: vec![ManifestRuntimeEnv {
                name: "API_KEY".to_string(),
                value: None,
                from_secret: Some("instance:api_key".to_string()),
            }],
        };

        let err = executor
            .ensure_runtime_running(
                instance_id,
                "ext.secret".to_string(),
                "default".to_string(),
                runtime_spec,
                None,
                Vec::new(),
            )
            .await
            .unwrap_err();
        let root = err.root_cause().to_string();
        assert!(root.contains("not encrypted"), "unexpected error: {root}");
        Ok(())
    }

    #[tokio::test]
    async fn runtime_env_missing_instance_secret_fails_fast() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        insert_extension(&store, "ext.secret", runtime_env_manifest("ext.secret")).await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "ext.secret".to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;

        let probe = StubProbe::default();
        let drivers = DriverRegistry::new();
        let runtime = StubRuntime;
        let temp_dir = TempDir::new()?;
        let runtime_paths = RuntimePaths::from_roots(
            temp_dir
                .path()
                .join("data")
                .join("extensions")
                .to_string_lossy()
                .as_ref(),
            temp_dir.path().to_string_lossy().as_ref(),
        );

        let secrets = SecretsManager::from_key_bytes([7u8; 32], false);
        let executor = Executor::new(
            &database.pool,
            &probe,
            &drivers,
            &runtime,
            runtime_paths,
            &secrets,
        );

        let runtime_spec = ManifestRuntime {
            r#type: "container".to_string(),
            image: Some("example/runtime:latest".to_string()),
            network: None,
            service_name: None,
            ports: Vec::new(),
            volumes: Vec::new(),
            env: vec![ManifestRuntimeEnv {
                name: "API_KEY".to_string(),
                value: None,
                from_secret: Some("instance:api_key".to_string()),
            }],
        };

        let err = executor
            .ensure_runtime_running(
                instance_id,
                "ext.secret".to_string(),
                "default".to_string(),
                runtime_spec,
                None,
                Vec::new(),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("missing required secrets"));
        Ok(())
    }

    #[tokio::test]
    async fn runtime_upgrade_rolls_back_on_failed_create() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        insert_extension(&store, "ext.upgrade", json!({})).await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "ext.upgrade".to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;
        store
            .update_instance_runtime_version(instance_id, "0.9.0", None)
            .await?;

        let runtime_spec = ManifestRuntime {
            r#type: "container".to_string(),
            image: Some("example/runtime:latest".to_string()),
            network: None,
            service_name: None,
            ports: Vec::new(),
            volumes: Vec::new(),
            env: Vec::new(),
        };

        let probe = StubProbe::default();
        let drivers = DriverRegistry::new();
        let runtime = UpgradeRuntime::new(container_name(instance_id));
        let temp_dir = TempDir::new()?;
        let runtime_paths = RuntimePaths::from_roots(
            temp_dir
                .path()
                .join("data")
                .join("extensions")
                .to_string_lossy()
                .as_ref(),
            temp_dir.path().to_string_lossy().as_ref(),
        );

        let secrets = SecretsManager::from_key_bytes([3u8; 32], true);
        let executor = Executor::new(
            &database.pool,
            &probe,
            &drivers,
            &runtime,
            runtime_paths,
            &secrets,
        );

        let err = executor
            .ensure_runtime_running(
                instance_id,
                "ext.upgrade".to_string(),
                "default".to_string(),
                runtime_spec,
                None,
                Vec::new(),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("create failed"));

        let instance = store.get_instance(instance_id).await?.expect("instance");
        assert_eq!(instance.runtime_version.as_deref(), Some("0.9.0"));
        assert!(instance.rollback_version.is_none());

        let base_name = container_name(instance_id);
        let rollback_name = format!("{base_name}-rollback");
        let expected = vec![
            "ensure_network".to_string(),
            format!("get:{rollback_name}"),
            format!("get:{base_name}"),
            format!("stop:{base_name}"),
            format!("rename:{base_name}->{rollback_name}"),
            "ensure_container".to_string(),
            format!("get:{base_name}"),
            format!("rename:{rollback_name}->{base_name}"),
            format!("start:{base_name}"),
        ];
        assert_eq!(runtime.calls(), expected);
        Ok(())
    }

    #[tokio::test]
    async fn runtime_upgrade_updates_versions_and_sets_rollback() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        insert_extension(&store, "ext.upgrade", json!({})).await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "ext.upgrade".to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;
        store
            .update_instance_runtime_version(instance_id, "0.9.0", Some("0.8.0"))
            .await?;

        let runtime_spec = ManifestRuntime {
            r#type: "container".to_string(),
            image: Some("example/runtime:latest".to_string()),
            network: None,
            service_name: None,
            ports: Vec::new(),
            volumes: Vec::new(),
            env: Vec::new(),
        };

        let probe = StubProbe::default();
        let drivers = DriverRegistry::new();
        let runtime = UpgradeRuntime::new_with_failure(container_name(instance_id), false);
        let temp_dir = TempDir::new()?;
        let runtime_paths = RuntimePaths::from_roots(
            temp_dir
                .path()
                .join("data")
                .join("extensions")
                .to_string_lossy()
                .as_ref(),
            temp_dir.path().to_string_lossy().as_ref(),
        );
        let secrets = SecretsManager::from_key_bytes([5u8; 32], true);
        let executor = Executor::new(
            &database.pool,
            &probe,
            &drivers,
            &runtime,
            runtime_paths,
            &secrets,
        );

        executor
            .ensure_runtime_running(
                instance_id,
                "ext.upgrade".to_string(),
                "default".to_string(),
                runtime_spec,
                None,
                Vec::new(),
            )
            .await?;

        let instance = store.get_instance(instance_id).await?.expect("instance");
        assert_eq!(instance.runtime_version.as_deref(), Some("1.0.0"));
        assert_eq!(instance.rollback_version.as_deref(), Some("0.9.0"));

        let base_name = container_name(instance_id);
        let rollback_name = format!("{base_name}-rollback");
        let expected = vec![
            "ensure_network".to_string(),
            format!("get:{rollback_name}"),
            format!("get:{base_name}"),
            format!("stop:{base_name}"),
            format!("rename:{base_name}->{rollback_name}"),
            "ensure_container".to_string(),
        ];
        assert_eq!(runtime.calls(), expected);
        Ok(())
    }

    #[tokio::test]
    async fn rollback_runtime_restores_previous_version() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        insert_extension(&store, "ext.rollback", json!({})).await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "ext.rollback".to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;
        store
            .update_instance_runtime_version(instance_id, "1.1.0", Some("1.0.0"))
            .await?;

        let probe = StubProbe::default();
        let drivers = DriverRegistry::new();
        let runtime = RollbackRuntime::new(container_name(instance_id));
        let temp_dir = TempDir::new()?;
        let runtime_paths = RuntimePaths::from_roots(
            temp_dir
                .path()
                .join("data")
                .join("extensions")
                .to_string_lossy()
                .as_ref(),
            temp_dir.path().to_string_lossy().as_ref(),
        );
        let secrets = SecretsManager::from_key_bytes([4u8; 32], true);
        let executor = Executor::new(
            &database.pool,
            &probe,
            &drivers,
            &runtime,
            runtime_paths,
            &secrets,
        );

        executor
            .apply(ExecutorAction::RollbackRuntime { instance_id })
            .await?;

        let instance = store.get_instance(instance_id).await?.expect("instance");
        assert_eq!(instance.runtime_version.as_deref(), Some("1.0.0"));
        assert!(instance.rollback_version.is_none());

        let base_name = container_name(instance_id);
        let rollback_name = format!("{base_name}-rollback");
        let expected = vec![
            format!("get:{rollback_name}"),
            format!("get:{base_name}"),
            format!("stop:{base_name}"),
            format!("remove:{base_name}"),
            format!("rename:{rollback_name}->{base_name}"),
            format!("start:{base_name}"),
        ];
        assert_eq!(runtime.calls(), expected);
        Ok(())
    }

    #[test]
    fn resolve_volume_mount_makes_relative_placeholder_absolute() -> Result<()> {
        let paths = RuntimePaths {
            data_root: "data".to_string(),
            downloads_root: "data/downloads".to_string(),
            media_root: "media".to_string(),
        };
        let mount = resolve_volume_mount("{data}/bazarr:/config", &paths)?;
        assert!(
            Path::new(&mount.host_path).is_absolute(),
            "expected absolute host path, got {}",
            mount.host_path
        );
        assert!(mount.host_path.ends_with("/data/bazarr"));
        Ok(())
    }
}
