use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use quick_xml::Reader;
use quick_xml::events::Event;
use rand::{Rng, distributions::Alphanumeric};
use reqwest::Url;
use serde::Deserialize;
use serde_json::Value;
use sqlx::AnyPool;
use tokio::fs;
use tokio::net::lookup_host;
use tokio::process::Command;
use tokio::time::sleep;
use uuid::Uuid;

use crate::config::DownloaderPerformanceProfile;
use crate::db::models::{
    BindingStatus, Provider, ProviderHealthState, ProviderReadinessPhase, SecretScope,
    SlotCardinality,
};
use crate::drivers::{
    ApplyStatus, DriftStatus, DriverCtx, DriverPatch, DriverRegistry, NzbgetPauseSnapshot,
    PatchApplyPolicy, QbittorrentPauseSnapshot, bootstrap_qbittorrent_session_cookie,
    pause_nzbget_for_rehome, pause_qbittorrent_for_rehome, render_nzbget_config_patch,
    resume_nzbget_after_rehome, resume_qbittorrent_after_rehome,
};
use crate::drivers::{
    DownloaderNzbPatch, DownloaderSpec, DownloaderTorrentPatch, IndexerCredentialField,
    IndexerRegistryPatch, MediaManagerMoviesPatch, MediaManagerTvPatch,
};
use crate::extensions::auto_managed::{is_nzbget_extension_id, is_qbittorrent_extension_id};
use crate::extensions::managed_paths::{
    DOWNLOADS_ROOT, NZBGET_CONFIG_TEMPLATE, NZBGET_INCOMPLETE_DIR, NZBGET_LOG_FILE,
    NZBGET_MAIN_DIR, NZBGET_NZB_DIR, NZBGET_QUEUE_DIR, NZBGET_REQUIRED_MANAGED_PATHS,
    NZBGET_SCRIPT_DIR, NZBGET_TEMP_DIR, NZBGET_WEB_DIR, QBITTORRENT_INCOMPLETE_DIR,
};
use crate::extensions::manifest::{
    ExtensionManifest, ManifestNetworking, ManifestRuntime, ManifestRuntimeEgress,
    ManifestRuntimeEnv,
};
use crate::extensions::required_secrets::{
    missing_required_secrets_for_instance, required_secrets_from_runtime,
};
use crate::extensions::store::{
    ExtensionStore, NewBinding, NewExtensionInstance, NewProvider, NewSecret, ProviderDetails,
};
use crate::network::protection::{
    ActiveManagedDownloaderRuntime, CloudflareWarpGatewayRuntime,
    CompiledDownloadProtectionProfile, DOWNLOAD_NETWORK_EXPOSED_PORTS_LABEL,
    DOWNLOAD_NETWORK_PROFILE_ID_LABEL, DOWNLOAD_NETWORK_PROFILE_KIND_LABEL,
    DOWNLOAD_NETWORK_RUNTIME_KIND_LABEL, DownloadNetworkProfileKind,
    DownloadProtectionCompileInput, DownloadProtectionProfile, GluetunOpenvpnGatewayRuntime,
    GluetunWireguardGatewayRuntime, active_download_network_profile_identity,
    active_managed_downloader_runtime, exposed_container_ports_label,
    mark_cloudflare_warp_runtime_ready, mark_cloudflare_warp_runtime_unavailable,
    profile_kind_as_str,
};
use crate::orchestrator::model::ProviderEndpoint;
use crate::orchestrator::naming::{build_aliases, container_name};
use crate::runtime::model::{
    CONTAINER_SPEC_HASH_LABEL, ContainerHandle, ContainerRuntimeMount, ContainerRuntimeState,
    ContainerSpec, EnvVar, PortMapping, VolumeMount, VolumeMountSourceKind,
    apply_container_spec_fingerprint,
};
use crate::runtime::probe::ProbeRunner;
use crate::runtime::{RuntimeManager, RuntimePaths};
use crate::secrets::SecretsManager;

const ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY: &str = "acquisition.candidate_provider";

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
    InstallRuntimeAsset {
        target_instance_id: Uuid,
        source_extension_id: String,
        source_extension_version: String,
        source_path: String,
        destination_path: String,
    },
    RestartRuntime {
        instance_id: Uuid,
    },
    RollbackRuntime {
        instance_id: Uuid,
    },
    TransportGate {
        provider_id: Uuid,
        timeout_seconds: u64,
    },
    BootstrapGate {
        provider_id: Uuid,
        timeout_seconds: u64,
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
    },
}

pub struct Executor<'a> {
    pool: &'a AnyPool,
    store: ExtensionStore<'a>,
    probe: &'a dyn ProbeRunner,
    drivers: &'a DriverRegistry,
    runtime: &'a dyn RuntimeManager,
    runtime_paths: RuntimePaths,
    secrets: &'a SecretsManager,
    wireguard_gateway_image: String,
    default_wireguard_config_secret: Option<String>,
    default_downloader_profile: DownloaderPerformanceProfile,
}

struct PreparedRuntimeVolumes {
    volumes: Vec<VolumeMount>,
}

#[derive(Debug, Clone)]
struct DownloaderRehomePreflight {
    requires_rehome: bool,
    previous_network_mode: Option<String>,
    desired_network_mode: Option<String>,
}

impl DownloaderRehomePreflight {
    fn unchanged() -> Self {
        Self {
            requires_rehome: false,
            previous_network_mode: None,
            desired_network_mode: None,
        }
    }
}

#[derive(Debug, Clone)]
enum DownloaderRehomePause {
    Qbittorrent {
        provider_id: Uuid,
        snapshot: QbittorrentPauseSnapshot,
    },
    Nzbget {
        provider_id: Uuid,
        snapshot: NzbgetPauseSnapshot,
    },
}

#[derive(Debug, Clone)]
enum ResolvedDownloadEgress {
    Wireguard(ManifestRuntimeEgress),
    Openvpn {
        profile_id: String,
        config_secret_ref: String,
        username_secret_ref: Option<String>,
        password_secret_ref: Option<String>,
        gateway_image: Option<String>,
    },
    CloudflareWarp {
        profile_id: String,
        runtime: CloudflareWarpGatewayRuntime,
    },
}

#[derive(Debug)]
pub(crate) struct DeferredDependencyActionError {
    message: String,
}

impl std::fmt::Display for DeferredDependencyActionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for DeferredDependencyActionError {}

pub(crate) fn deferred_dependency_error(message: impl Into<String>) -> anyhow::Error {
    DeferredDependencyActionError {
        message: message.into(),
    }
    .into()
}

pub(crate) fn deferred_dependency_message(err: &anyhow::Error) -> Option<String> {
    err.downcast_ref::<DeferredDependencyActionError>()
        .map(|value| value.message.clone())
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
            pool,
            store: ExtensionStore::new(pool),
            probe,
            drivers,
            runtime,
            runtime_paths,
            secrets,
            wireguard_gateway_image: "qmcgaw/gluetun:v3.39.0".to_string(),
            default_wireguard_config_secret: None,
            default_downloader_profile: DownloaderPerformanceProfile::Balanced,
        }
    }

    pub fn with_wireguard_gateway_image(mut self, image: impl Into<String>) -> Self {
        let image = image.into();
        if !image.trim().is_empty() {
            self.wireguard_gateway_image = image;
        }
        self
    }

    pub fn with_default_wireguard_config_secret(mut self, secret: Option<String>) -> Self {
        self.default_wireguard_config_secret =
            secret.and_then(|value| (!value.trim().is_empty()).then_some(value));
        self
    }

    pub fn with_default_downloader_profile(
        mut self,
        profile: DownloaderPerformanceProfile,
    ) -> Self {
        self.default_downloader_profile = profile;
        self
    }

    pub async fn apply(&self, action: ExecutorAction) -> Result<()> {
        self.apply_with_note(action).await.map(|_| ())
    }

    pub async fn apply_with_note(&self, action: ExecutorAction) -> Result<Option<String>> {
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
                .await?;
                Ok(None)
            }
            ExecutorAction::DeleteProvider { provider_id } => {
                self.delete_provider(provider_id).await?;
                Ok(None)
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
                .await?;
                Ok(None)
            }
            ExecutorAction::InstallRuntimeAsset {
                target_instance_id,
                source_extension_id,
                source_extension_version,
                source_path,
                destination_path,
            } => {
                self.install_runtime_asset(
                    target_instance_id,
                    source_extension_id,
                    source_extension_version,
                    source_path,
                    destination_path,
                )
                .await?;
                Ok(None)
            }
            ExecutorAction::RestartRuntime { instance_id } => {
                self.restart_instance_runtime(instance_id).await?;
                Ok(None)
            }
            ExecutorAction::RollbackRuntime { instance_id } => {
                self.rollback_runtime(instance_id).await?;
                Ok(None)
            }
            ExecutorAction::TransportGate {
                provider_id,
                timeout_seconds,
            } => {
                self.transport_gate(provider_id, timeout_seconds).await?;
                Ok(None)
            }
            ExecutorAction::BootstrapGate {
                provider_id,
                timeout_seconds,
            } => {
                self.bootstrap_gate(provider_id, timeout_seconds).await?;
                Ok(None)
            }
            ExecutorAction::HealthGate {
                provider_id,
                timeout_seconds,
            } => {
                self.health_gate(provider_id, timeout_seconds).await?;
                Ok(None)
            }
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
                .await?;
                Ok(None)
            }
            ExecutorAction::ApplyDriverPatch {
                connector_extension_id,
                target_provider_id,
                patch,
            } => {
                self.apply_driver_patch(connector_extension_id, target_provider_id, patch)
                    .await
            }
            ExecutorAction::ApplyBinding { binding } => {
                self.apply_binding(binding).await?;
                Ok(None)
            }
        }
    }

    pub async fn check_provider_health(&self, provider_id: Uuid) -> Result<()> {
        self.transport_gate(provider_id, 60).await?;
        self.bootstrap_gate(provider_id, 60).await?;
        self.health_gate(provider_id, 60).await
    }

    pub async fn apply_builtin_downloader_profiles_now(&self) -> Result<()> {
        let providers = self.store.list_providers(None).await?;
        for provider in providers {
            let Some(implementation) = provider.implementation.as_deref() else {
                continue;
            };
            if !matches!(
                (provider.capability.as_str(), implementation),
                ("downloader.torrent", "qbittorrent") | ("downloader.nzb", "nzbget")
            ) {
                continue;
            }

            let endpoint_json = provider
                .endpoint_json
                .as_ref()
                .cloned()
                .ok_or_else(|| anyhow!("provider {} has no endpoint", provider.provider_id))?;
            let endpoint: ProviderEndpoint =
                serde_json::from_value(endpoint_json).context("parsing provider endpoint")?;
            endpoint.validate()?;

            self.transport_gate(provider.provider_id, 60).await?;
            self.bootstrap_gate(provider.provider_id, 60).await?;

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

            self.apply_builtin_downloader_profile_if_needed(&provider, &instance, &endpoint)
                .await?;
            self.health_gate(provider.provider_id, 60).await?;
        }

        Ok(())
    }

    async fn resolve_default_download_egress(
        &self,
        extension_id: &str,
        explicit_egress: Option<ManifestRuntimeEgress>,
    ) -> Result<Option<ResolvedDownloadEgress>> {
        if let Some(explicit_egress) = explicit_egress {
            return if explicit_egress.mode_is_wireguard() {
                Ok(Some(ResolvedDownloadEgress::Wireguard(explicit_egress)))
            } else {
                Ok(None)
            };
        }
        if !is_default_wireguard_downloader_extension_id(extension_id) {
            return Ok(None);
        }

        match active_managed_downloader_runtime(self.pool).await? {
            ActiveManagedDownloaderRuntime::NoStoredProfile => Ok(self
                .default_wireguard_config_secret
                .as_ref()
                .map(|secret| ManifestRuntimeEgress {
                    mode: "wireguard".to_string(),
                    strict: true,
                    wireguard_config_secret: Some(secret.clone()),
                    wireguard_gateway_image: None,
                })
                .map(ResolvedDownloadEgress::Wireguard)),
            ActiveManagedDownloaderRuntime::Direct => Ok(None),
            ActiveManagedDownloaderRuntime::WireguardConfig {
                profile_id: _,
                secret_ref,
                gateway_image,
            } => Ok(Some(ResolvedDownloadEgress::Wireguard(
                ManifestRuntimeEgress {
                    mode: "wireguard".to_string(),
                    strict: true,
                    wireguard_config_secret: Some(secret_ref),
                    wireguard_gateway_image: gateway_image,
                },
            ))),
            ActiveManagedDownloaderRuntime::OpenvpnConfig {
                profile_id,
                config_secret_ref,
                username_secret_ref,
                password_secret_ref,
                gateway_image,
            } => Ok(Some(ResolvedDownloadEgress::Openvpn {
                profile_id,
                config_secret_ref,
                username_secret_ref,
                password_secret_ref,
                gateway_image,
            })),
            ActiveManagedDownloaderRuntime::CloudflareWarp {
                profile_id,
                enrollment_id,
                identity_secret_ref,
                gateway_image,
                state_volume_name,
            } => Ok(Some(ResolvedDownloadEgress::CloudflareWarp {
                profile_id: profile_id.clone(),
                runtime: CloudflareWarpGatewayRuntime {
                    image: gateway_image,
                    state_volume_name,
                    enrollment_id,
                    identity_secret_ref,
                },
            })),
            ActiveManagedDownloaderRuntime::UnsupportedProtected { profile_id, kind } => {
                bail!(
                    "active download network profile '{}' uses unsupported protected runtime '{:?}' for managed downloader '{}'",
                    profile_id,
                    kind,
                    extension_id
                )
            }
        }
    }

    fn stamp_direct_downloader_network_labels(
        spec: &mut ContainerSpec,
        profile_id: &str,
        profile_kind: &DownloadNetworkProfileKind,
    ) {
        let exposed_ports = exposed_container_ports_label(spec);
        spec.labels.insert(
            DOWNLOAD_NETWORK_PROFILE_ID_LABEL.to_string(),
            profile_id.to_string(),
        );
        spec.labels.insert(
            DOWNLOAD_NETWORK_PROFILE_KIND_LABEL.to_string(),
            profile_kind_as_str(profile_kind).to_string(),
        );
        spec.labels.insert(
            DOWNLOAD_NETWORK_RUNTIME_KIND_LABEL.to_string(),
            "direct".to_string(),
        );
        spec.labels.insert(
            DOWNLOAD_NETWORK_EXPOSED_PORTS_LABEL.to_string(),
            exposed_ports,
        );
        apply_container_spec_fingerprint(spec);
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
        instance_name: String,
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
        if is_nzbget_extension_id(&extension_id) {
            ensure_nzbget_secrets(&self.store, self.secrets, instance_id, &runtime.env).await?;
        }
        ensure_runtime_secrets_present(&self.store, instance_id, &runtime).await?;

        let name = container_name(instance_id);
        let mut alias_list = aliases;
        if let Some(service_name) = runtime.service_name.clone() {
            if !alias_list.contains(&service_name) {
                alias_list.push(service_name);
            }
        }

        let mut labels = HashMap::new();
        labels.insert("elixir.instance_id".to_string(), instance_id.to_string());
        labels.insert("elixir.instance_name".to_string(), instance_name);
        labels.insert("elixir.extension_id".to_string(), extension_id.clone());
        labels.insert(
            "elixir.extension_version".to_string(),
            desired_version.clone(),
        );
        labels.insert("elixir.managed".to_string(), "true".to_string());

        let env = resolve_runtime_env(&self.store, self.secrets, instance_id, runtime.env).await?;

        let prepared_volumes = prepare_runtime_volumes(
            &extension_id,
            instance_id,
            &runtime.volumes,
            &self.runtime_paths,
        )?;
        let volumes = prepared_volumes.volumes.clone();
        let runtime_volumes = prepared_volumes.volumes.clone();
        if is_nzbget_extension_id(&extension_id) {
            prepare_nzbget_runtime_dirs(&runtime_volumes).await?;
        }

        let ports = runtime
            .ports
            .iter()
            .map(|port| PortMapping {
                container_port: port.container,
                host_port: port.host,
                protocol: None,
            })
            .collect();

        let mut spec = ContainerSpec {
            name: name.clone(),
            image,
            network,
            network_mode: None,
            aliases: alias_list,
            env,
            volumes,
            ports,
            labels: labels.clone(),
            command: Vec::new(),
            cap_add: Vec::new(),
            devices: Vec::new(),
            sysctls: HashMap::new(),
        };

        let resolved_egress = self
            .resolve_default_download_egress(&extension_id, runtime.egress.clone())
            .await?;

        if let Some(egress) = resolved_egress {
            match egress {
                ResolvedDownloadEgress::Wireguard(egress) => {
                    match self
                        .apply_download_protection_profile(
                            instance_id,
                            &extension_id,
                            &name,
                            &spec,
                            &egress,
                            &labels,
                        )
                        .await
                    {
                        Ok(protected_spec) => spec = protected_spec,
                        Err(err) if !egress.strict => {
                            tracing::warn!(
                                "download protection gateway setup failed for extension {} instance {} (strict=false), falling back to direct egress: {}",
                                extension_id,
                                instance_id,
                                err
                            );
                        }
                        Err(err) => return Err(err),
                    }
                }
                ResolvedDownloadEgress::Openvpn {
                    profile_id,
                    config_secret_ref,
                    username_secret_ref,
                    password_secret_ref,
                    gateway_image,
                } => {
                    spec = self
                        .apply_openvpn_profile(
                            instance_id,
                            &extension_id,
                            &name,
                            &spec,
                            &profile_id,
                            &config_secret_ref,
                            username_secret_ref.as_deref(),
                            password_secret_ref.as_deref(),
                            gateway_image.as_deref(),
                            &labels,
                        )
                        .await?;
                }
                ResolvedDownloadEgress::CloudflareWarp {
                    profile_id,
                    runtime,
                } => {
                    spec = self
                        .apply_cloudflare_warp_profile(
                            instance_id,
                            &extension_id,
                            &name,
                            &spec,
                            &profile_id,
                            runtime,
                            &labels,
                        )
                        .await?;
                }
            }
        }
        if spec.network_mode.is_none()
            && (is_qbittorrent_extension_id(&extension_id) || is_nzbget_extension_id(&extension_id))
        {
            let (profile_id, profile_kind) = active_download_network_profile_identity(self.pool)
                .await?
                .unwrap_or_else(|| {
                    (
                        "legacy-direct".to_string(),
                        DownloadNetworkProfileKind::Direct,
                    )
                });
            Self::stamp_direct_downloader_network_labels(&mut spec, &profile_id, &profile_kind);
        }

        let rehome_preflight = self
            .preflight_downloader_rehome(
                instance_id,
                &extension_id,
                &name,
                &spec,
                &runtime_volumes,
                instance.config_json.as_ref(),
            )
            .await?;
        if rehome_preflight.requires_rehome {
            tracing::info!(
                "downloader rehome preflight passed for extension {} instance {}: {:?} -> {:?}",
                extension_id,
                instance_id,
                rehome_preflight.previous_network_mode,
                rehome_preflight.desired_network_mode
            );
        }
        let keep_paused_after_rehome =
            keep_downloader_paused_after_rehome(instance.config_json.as_ref());
        let rehome_pause = if rehome_preflight.requires_rehome {
            self.pause_downloader_for_rehome(instance_id, &extension_id, &instance)
                .await?
        } else {
            None
        };

        if spec.network_mode.is_none() {
            self.runtime.ensure_network(&spec.network).await?;
        }

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

            let handle = if let Err(err) = self.runtime.ensure_container(&spec).await {
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
                self.try_resume_downloader_after_rehome(
                    &rehome_pause,
                    keep_paused_after_rehome,
                    "container ensure failed",
                )
                .await;
                return Err(err);
            } else {
                self.runtime
                    .get_container_handle(&name)
                    .await?
                    .unwrap_or(ContainerHandle {
                        id: name.clone(),
                        name: name.clone(),
                    })
            };

            if let Err(err) = self
                .finalize_runtime_storage(instance_id, &extension_id, &handle, &runtime_volumes)
                .await
            {
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
                            "upgrade: failed to restore rollback container {} after storage finalize error: {}",
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
                self.try_resume_downloader_after_rehome(
                    &rehome_pause,
                    keep_paused_after_rehome,
                    "storage finalize failed",
                )
                .await;
                return Err(err);
            }
            if rehome_preflight.requires_rehome {
                if let Err(err) = self
                    .verify_downloader_rehome(instance_id, &extension_id)
                    .await
                {
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
                                "upgrade: failed to restore rollback container {} after downloader rehome verification error: {}",
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
                    self.try_resume_downloader_after_rehome(
                        &rehome_pause,
                        keep_paused_after_rehome,
                        "downloader verification failed",
                    )
                    .await;
                    return Err(err);
                }
                self.resume_downloader_after_rehome(&rehome_pause, keep_paused_after_rehome)
                    .await?;
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
            self.cleanup_stale_downloader_gateway_if_unneeded(
                &extension_id,
                &name,
                spec.network_mode.as_deref(),
            )
            .await?;
            return Ok(());
        }

        if rehome_preflight.requires_rehome {
            self.ensure_rehomed_runtime_with_rollback(
                instance_id,
                &extension_id,
                &name,
                &spec,
                &runtime_volumes,
                rehome_pause,
                keep_paused_after_rehome,
            )
            .await?;
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
            self.cleanup_stale_downloader_gateway_if_unneeded(
                &extension_id,
                &name,
                spec.network_mode.as_deref(),
            )
            .await?;
            return Ok(());
        }

        let handle = self.runtime.ensure_container(&spec).await?;
        self.finalize_runtime_storage(instance_id, &extension_id, &handle, &runtime_volumes)
            .await?;
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
        self.cleanup_stale_downloader_gateway_if_unneeded(
            &extension_id,
            &name,
            spec.network_mode.as_deref(),
        )
        .await?;
        Ok(())
    }

    async fn apply_download_protection_profile(
        &self,
        instance_id: Uuid,
        extension_id: &str,
        container_name: &str,
        app_spec: &ContainerSpec,
        egress: &ManifestRuntimeEgress,
        base_labels: &HashMap<String, String>,
    ) -> Result<ContainerSpec> {
        let compiled = self
            .compile_download_protection_profile(
                instance_id,
                container_name,
                app_spec,
                egress,
                base_labels,
            )
            .await?;
        if let Some(gateway_spec) = compiled.gateway_spec.as_ref() {
            self.runtime.ensure_network(&gateway_spec.network).await?;
            self.runtime
                .ensure_container(gateway_spec)
                .await
                .with_context(|| {
                    format!(
                        "ensuring download protection gateway container '{}' for extension '{}'",
                        gateway_spec.name, extension_id
                    )
                })?;
        }
        Ok(compiled.protected_app_spec)
    }

    async fn apply_cloudflare_warp_profile(
        &self,
        instance_id: Uuid,
        extension_id: &str,
        container_name: &str,
        app_spec: &ContainerSpec,
        profile_id: &str,
        runtime: CloudflareWarpGatewayRuntime,
        base_labels: &HashMap<String, String>,
    ) -> Result<ContainerSpec> {
        let profile = DownloadProtectionProfile::cloudflare_warp(
            profile_id,
            "Cloudflare WARP",
            true,
            runtime,
        );
        let compiled = profile.compile(DownloadProtectionCompileInput {
            app_container_name: container_name,
            app_spec,
            base_labels,
        })?;
        let result = async {
            if let Some(gateway_spec) = compiled.gateway_spec.as_ref() {
                self.runtime.ensure_network(&gateway_spec.network).await?;
                self.runtime
                    .ensure_container(gateway_spec)
                    .await
                    .with_context(|| {
                        format!(
                            "ensuring Cloudflare WARP gateway container '{}' for extension '{}'",
                            gateway_spec.name, extension_id
                        )
                    })?;
            }
            mark_cloudflare_warp_runtime_ready(self.pool, profile_id).await?;
            Ok::<ContainerSpec, anyhow::Error>(compiled.protected_app_spec)
        }
        .await;

        if let Err(err) = result.as_ref() {
            let detail = format!(
                "Cloudflare WARP gateway apply failed for extension '{}' instance {}: {}",
                extension_id, instance_id, err
            );
            let _ = mark_cloudflare_warp_runtime_unavailable(self.pool, profile_id, &detail).await;
        }

        result
    }

    #[allow(clippy::too_many_arguments)]
    async fn apply_openvpn_profile(
        &self,
        instance_id: Uuid,
        extension_id: &str,
        container_name: &str,
        app_spec: &ContainerSpec,
        profile_id: &str,
        config_secret_ref: &str,
        username_secret_ref: Option<&str>,
        password_secret_ref: Option<&str>,
        gateway_image: Option<&str>,
        base_labels: &HashMap<String, String>,
    ) -> Result<ContainerSpec> {
        let config_value =
            resolve_secret_value(&self.store, self.secrets, instance_id, config_secret_ref)
                .await
                .with_context(|| {
                    format!(
                        "resolving OpenVPN config secret '{}' for instance {}",
                        config_secret_ref, instance_id
                    )
                })?;
        if config_value.trim().is_empty() {
            bail!(
                "OpenVPN config secret '{}' resolved to empty value",
                config_secret_ref
            );
        }

        let username = match username_secret_ref {
            Some(secret_ref) => Some(
                resolve_secret_value(&self.store, self.secrets, instance_id, secret_ref)
                    .await
                    .with_context(|| {
                        format!(
                            "resolving OpenVPN username secret '{}' for instance {}",
                            secret_ref, instance_id
                        )
                    })?,
            ),
            None => None,
        };
        let password = match password_secret_ref {
            Some(secret_ref) => Some(
                resolve_secret_value(&self.store, self.secrets, instance_id, secret_ref)
                    .await
                    .with_context(|| {
                        format!(
                            "resolving OpenVPN password secret '{}' for instance {}",
                            secret_ref, instance_id
                        )
                    })?,
            ),
            None => None,
        };
        if username.is_some() != password.is_some() {
            bail!(
                "OpenVPN profile '{}' has incomplete credentials",
                profile_id
            );
        }

        let (config_path, auth_path) = self
            .write_openvpn_config(
                instance_id,
                &config_value,
                username.as_deref(),
                password.as_deref(),
            )
            .await
            .context("writing OpenVPN config")?;
        let image = gateway_image
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or(self.wireguard_gateway_image.as_str())
            .to_string();
        let profile = DownloadProtectionProfile::openvpn_config(
            profile_id,
            "Imported OpenVPN",
            true,
            GluetunOpenvpnGatewayRuntime {
                image,
                config_host_path: config_path,
                auth_host_path: auth_path,
            },
        );
        let compiled = profile.compile(DownloadProtectionCompileInput {
            app_container_name: container_name,
            app_spec,
            base_labels,
        })?;
        if let Some(gateway_spec) = compiled.gateway_spec.as_ref() {
            self.runtime.ensure_network(&gateway_spec.network).await?;
            self.runtime
                .ensure_container(gateway_spec)
                .await
                .with_context(|| {
                    format!(
                        "ensuring OpenVPN gateway container '{}' for extension '{}'",
                        gateway_spec.name, extension_id
                    )
                })?;
        }
        Ok(compiled.protected_app_spec)
    }

    async fn preflight_downloader_rehome(
        &self,
        instance_id: Uuid,
        extension_id: &str,
        container_name: &str,
        desired_spec: &ContainerSpec,
        desired_volumes: &[VolumeMount],
        instance_config: Option<&serde_json::Value>,
    ) -> Result<DownloaderRehomePreflight> {
        if !is_qbittorrent_extension_id(extension_id) && !is_nzbget_extension_id(extension_id) {
            return Ok(DownloaderRehomePreflight::unchanged());
        }

        if self
            .runtime
            .get_container_handle(container_name)
            .await?
            .is_none()
        {
            return Ok(DownloaderRehomePreflight::unchanged());
        }

        let runtime_state = self
            .runtime
            .describe_container_runtime_state(container_name)
            .await
            .with_context(|| {
                format!(
                    "inspecting current downloader runtime state for instance {}",
                    instance_id
                )
            })?;
        let current_network_mode = runtime_state
            .as_ref()
            .and_then(|state| normalized_network_mode(state.network_mode.as_deref()));
        let desired_network_mode = normalized_network_mode(desired_spec.network_mode.as_deref());
        let desired_gateway_id =
            match container_network_namespace_target(desired_network_mode.as_deref()) {
                Some(gateway_name) => self
                    .runtime
                    .get_container_handle(gateway_name)
                    .await?
                    .map(|handle| handle.id),
                None => None,
            };

        let network_mode_requires_rehome =
            downloader_network_mode_requires_rehome_with_gateway_identity(
                current_network_mode.as_deref(),
                desired_network_mode.as_deref(),
                desired_gateway_id.as_deref(),
            );
        let spec_fingerprint_requires_rehome = runtime_state.as_ref().is_some_and(|state| {
            desired_spec
                .labels
                .get(CONTAINER_SPEC_HASH_LABEL)
                .zip(state.labels.get(CONTAINER_SPEC_HASH_LABEL))
                .is_some_and(|(desired, actual)| desired != actual)
        });

        if !network_mode_requires_rehome && !spec_fingerprint_requires_rehome {
            return Ok(DownloaderRehomePreflight::unchanged());
        }

        let existing_volumes = if let Some(state) = runtime_state.as_ref() {
            volume_mounts_from_runtime_state(state)
        } else {
            persisted_runtime_volumes(instance_config)?.ok_or_else(|| {
                anyhow!(
                    "downloader migration preflight failed: current runtime state for instance {} is unavailable and no persisted runtime volumes exist",
                    instance_id
                )
            })?
        };

        let rehome_context =
            format!("validating downloader rehome volume preservation for instance {instance_id}");
        validate_downloader_volume_preservation(extension_id, &existing_volumes, desired_volumes)
            .context(rehome_context)?;

        Ok(DownloaderRehomePreflight {
            requires_rehome: true,
            previous_network_mode: current_network_mode,
            desired_network_mode,
        })
    }

    async fn ensure_rehomed_runtime_with_rollback(
        &self,
        instance_id: Uuid,
        extension_id: &str,
        container_name: &str,
        spec: &ContainerSpec,
        runtime_volumes: &[VolumeMount],
        rehome_pause: Option<DownloaderRehomePause>,
        keep_paused_after_rehome: bool,
    ) -> Result<()> {
        let rollback_name = format!("{container_name}-network-rollback");
        if let Some(handle) = self.runtime.get_container_handle(&rollback_name).await? {
            let _ = self.runtime.stop_container(&handle).await;
            let _ = self.runtime.remove_container(&handle).await;
        }

        let mut backup_created = false;
        if let Some(handle) = self.runtime.get_container_handle(container_name).await? {
            let _ = self.runtime.stop_container(&handle).await;
            match self.runtime.rename_container(&handle, &rollback_name).await {
                Ok(_) => backup_created = true,
                Err(err) => {
                    tracing::warn!(
                        "downloader rehome: failed to rename container {} -> {}: {}",
                        handle.name,
                        rollback_name,
                        err
                    );
                }
            }
        }

        let handle = match self.runtime.ensure_container(spec).await {
            Ok(_) => self
                .runtime
                .get_container_handle(container_name)
                .await?
                .unwrap_or(ContainerHandle {
                    id: container_name.to_string(),
                    name: container_name.to_string(),
                }),
            Err(err) => {
                self.restore_rehome_rollback(container_name, &rollback_name, backup_created)
                    .await;
                self.try_resume_downloader_after_rehome(
                    &rehome_pause,
                    keep_paused_after_rehome,
                    "container ensure failed",
                )
                .await;
                return Err(err);
            }
        };

        if let Err(err) = self
            .finalize_runtime_storage(instance_id, extension_id, &handle, runtime_volumes)
            .await
        {
            self.restore_rehome_rollback(container_name, &rollback_name, backup_created)
                .await;
            self.try_resume_downloader_after_rehome(
                &rehome_pause,
                keep_paused_after_rehome,
                "storage finalize failed",
            )
            .await;
            return Err(err);
        }

        if let Err(err) = self
            .verify_downloader_rehome(instance_id, extension_id)
            .await
        {
            self.restore_rehome_rollback(container_name, &rollback_name, backup_created)
                .await;
            self.try_resume_downloader_after_rehome(
                &rehome_pause,
                keep_paused_after_rehome,
                "downloader verification failed",
            )
            .await;
            return Err(err);
        }

        self.resume_downloader_after_rehome(&rehome_pause, keep_paused_after_rehome)
            .await?;

        if backup_created {
            let rollback_handle = ContainerHandle {
                id: rollback_name.clone(),
                name: rollback_name,
            };
            if let Err(err) = self.runtime.remove_container(&rollback_handle).await {
                tracing::warn!(
                    "downloader rehome: failed to remove successful rollback backup {}: {}",
                    rollback_handle.name,
                    err
                );
            }
        }

        Ok(())
    }

    async fn cleanup_stale_downloader_gateway_if_unneeded(
        &self,
        extension_id: &str,
        container_name: &str,
        desired_network_mode: Option<&str>,
    ) -> Result<()> {
        if !is_qbittorrent_extension_id(extension_id) && !is_nzbget_extension_id(extension_id) {
            return Ok(());
        }

        let gateway_name = format!("{container_name}-vpn");
        let desired_gateway_mode = format!("container:{gateway_name}");
        if desired_network_mode == Some(desired_gateway_mode.as_str()) {
            return Ok(());
        }

        let Some(gateway_handle) = self.runtime.get_container_handle(&gateway_name).await? else {
            return Ok(());
        };

        if let Some(app_state) = self
            .runtime
            .describe_container_runtime_state(container_name)
            .await?
        {
            if app_state.network_mode.as_deref().is_some_and(|mode| {
                downloader_uses_gateway_namespace(mode, &gateway_name, &gateway_handle.id)
            }) {
                bail!(
                    "refusing to remove stale downloader gateway '{}' because app container '{}' still uses its network namespace",
                    gateway_name,
                    container_name
                );
            }
        }

        tracing::info!(
            "removing stale downloader gateway '{}' after '{}' was rehomed away from it",
            gateway_name,
            container_name
        );
        let _ = self.runtime.stop_container(&gateway_handle).await;
        self.runtime.remove_container(&gateway_handle).await?;
        Ok(())
    }

    async fn pause_downloader_for_rehome(
        &self,
        instance_id: Uuid,
        extension_id: &str,
        instance: &crate::db::models::ExtensionInstance,
    ) -> Result<Option<DownloaderRehomePause>> {
        let Some(provider) = self
            .rehome_downloader_provider(instance_id, extension_id)
            .await?
        else {
            tracing::debug!(
                "downloader rehome: no downloader provider exists for extension {} instance {}; skipping API pause",
                extension_id,
                instance_id
            );
            return Ok(None);
        };
        let app_container_name = container_name(instance_id);
        if self
            .runtime
            .get_container_handle(&app_container_name)
            .await?
            .is_none()
        {
            tracing::info!(
                "downloader rehome: app container {} is not present for extension {} instance {}; skipping API pause and recreating from persisted storage",
                app_container_name,
                extension_id,
                instance_id
            );
            return Ok(None);
        }
        let ctx = build_driver_ctx_for_provider(
            &self.store,
            self.secrets,
            self.runtime,
            &provider,
            instance,
        )
        .await?;

        if is_qbittorrent_extension_id(extension_id) {
            let snapshot = pause_qbittorrent_for_rehome(ctx).await.with_context(|| {
                format!("pausing qBittorrent before network rehome for instance {instance_id}")
            })?;
            return Ok(Some(DownloaderRehomePause::Qbittorrent {
                provider_id: provider.provider_id,
                snapshot,
            }));
        }

        if is_nzbget_extension_id(extension_id) {
            let snapshot = pause_nzbget_for_rehome(ctx).await.with_context(|| {
                format!("pausing NZBGet before network rehome for instance {instance_id}")
            })?;
            return Ok(Some(DownloaderRehomePause::Nzbget {
                provider_id: provider.provider_id,
                snapshot,
            }));
        }

        Ok(None)
    }

    async fn resume_downloader_after_rehome(
        &self,
        rehome_pause: &Option<DownloaderRehomePause>,
        keep_paused_after_rehome: bool,
    ) -> Result<()> {
        let Some(rehome_pause) = rehome_pause else {
            return Ok(());
        };
        if keep_paused_after_rehome {
            tracing::info!(
                "downloader rehome: leaving downloader paused because instance config requested it"
            );
            return Ok(());
        }

        match rehome_pause {
            DownloaderRehomePause::Qbittorrent {
                provider_id,
                snapshot,
            } => {
                let (provider, instance) = self.provider_and_instance(*provider_id).await?;
                let ctx = build_driver_ctx_for_provider(
                    &self.store,
                    self.secrets,
                    self.runtime,
                    &provider,
                    &instance,
                )
                .await?;
                resume_qbittorrent_after_rehome(ctx, snapshot)
                    .await
                    .context("resuming qBittorrent after network rehome")?;
            }
            DownloaderRehomePause::Nzbget {
                provider_id,
                snapshot,
            } => {
                let (provider, instance) = self.provider_and_instance(*provider_id).await?;
                let ctx = build_driver_ctx_for_provider(
                    &self.store,
                    self.secrets,
                    self.runtime,
                    &provider,
                    &instance,
                )
                .await?;
                resume_nzbget_after_rehome(ctx, snapshot)
                    .await
                    .context("resuming NZBGet after network rehome")?;
            }
        }

        Ok(())
    }

    async fn try_resume_downloader_after_rehome(
        &self,
        rehome_pause: &Option<DownloaderRehomePause>,
        keep_paused_after_rehome: bool,
        reason: &str,
    ) {
        if let Err(err) = self
            .resume_downloader_after_rehome(rehome_pause, keep_paused_after_rehome)
            .await
        {
            tracing::warn!(
                "downloader rehome: failed to resume downloader after rollback ({reason}): {err}"
            );
        }
    }

    async fn rehome_downloader_provider(
        &self,
        instance_id: Uuid,
        extension_id: &str,
    ) -> Result<Option<Provider>> {
        let providers = self.store.list_providers(Some(instance_id)).await?;
        let provider = providers.into_iter().find(|provider| {
            provider.capability == "downloader.torrent"
                && provider.implementation.as_deref() == Some("qbittorrent")
                && is_qbittorrent_extension_id(extension_id)
                || provider.capability == "downloader.nzb"
                    && provider.implementation.as_deref() == Some("nzbget")
                    && is_nzbget_extension_id(extension_id)
        });
        Ok(provider)
    }

    async fn provider_and_instance(
        &self,
        provider_id: Uuid,
    ) -> Result<(Provider, crate::db::models::ExtensionInstance)> {
        let provider = self
            .store
            .get_provider(provider_id)
            .await?
            .ok_or_else(|| anyhow!("provider {} not found", provider_id))?;
        let instance = self
            .store
            .get_instance(provider.instance_id)
            .await?
            .ok_or_else(|| anyhow!("instance {} not found", provider.instance_id))?;
        Ok((provider, instance))
    }

    async fn restore_rehome_rollback(
        &self,
        container_name: &str,
        rollback_name: &str,
        backup_created: bool,
    ) {
        if let Ok(Some(handle)) = self.runtime.get_container_handle(container_name).await {
            let _ = self.runtime.stop_container(&handle).await;
            let _ = self.runtime.remove_container(&handle).await;
        }
        if !backup_created {
            return;
        }

        let rollback_handle = ContainerHandle {
            id: rollback_name.to_string(),
            name: rollback_name.to_string(),
        };
        match self
            .runtime
            .rename_container(&rollback_handle, container_name)
            .await
        {
            Ok(restored) => {
                let _ = self.runtime.start_container(&restored).await;
            }
            Err(err) => {
                tracing::warn!(
                    "downloader rehome: failed to restore rollback container {}: {}",
                    rollback_name,
                    err
                );
            }
        }
    }

    async fn verify_downloader_rehome(&self, instance_id: Uuid, extension_id: &str) -> Result<()> {
        if !is_qbittorrent_extension_id(extension_id) && !is_nzbget_extension_id(extension_id) {
            return Ok(());
        }

        let instance = self
            .store
            .get_instance(instance_id)
            .await?
            .ok_or_else(|| anyhow!("instance {} not found", instance_id))?;
        let providers = self.store.list_providers(Some(instance_id)).await?;
        for provider in providers {
            let is_matching_downloader = provider.capability == "downloader.torrent"
                && provider.implementation.as_deref() == Some("qbittorrent")
                && is_qbittorrent_extension_id(extension_id)
                || provider.capability == "downloader.nzb"
                    && provider.implementation.as_deref() == Some("nzbget")
                    && is_nzbget_extension_id(extension_id);
            if !is_matching_downloader {
                continue;
            }

            self.ensure_provider_driver_ready(&provider, &instance)
                .await
                .with_context(|| {
                    format!(
                        "verifying downloader API after network rehome for provider {}",
                        provider.provider_id
                    )
                })?;
        }

        Ok(())
    }

    async fn compile_download_protection_profile(
        &self,
        instance_id: Uuid,
        container_name: &str,
        app_spec: &ContainerSpec,
        egress: &ManifestRuntimeEgress,
        base_labels: &HashMap<String, String>,
    ) -> Result<CompiledDownloadProtectionProfile> {
        let config_secret = egress
            .wireguard_config_secret
            .as_deref()
            .ok_or_else(|| anyhow!("wireguard egress requires wireguard_config_secret"))?;
        let config_value =
            resolve_secret_value(&self.store, self.secrets, instance_id, config_secret)
                .await
                .with_context(|| {
                    format!(
                        "resolving wireguard config secret '{}' for instance {}",
                        config_secret, instance_id
                    )
                })?;
        if config_value.trim().is_empty() {
            bail!(
                "wireguard config secret '{}' resolved to empty value",
                config_secret
            );
        }

        let config_path = self
            .write_wireguard_config(instance_id, &config_value)
            .await
            .context("writing wireguard config")?;

        let gateway_image = egress
            .wireguard_gateway_image
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(self.wireguard_gateway_image.as_str())
            .to_string();

        let profile = DownloadProtectionProfile::wireguard_config(
            "legacy-wireguard",
            "Legacy WireGuard",
            egress.strict,
            GluetunWireguardGatewayRuntime {
                image: gateway_image,
                config_host_path: config_path,
            },
        );
        profile.compile(DownloadProtectionCompileInput {
            app_container_name: container_name,
            app_spec,
            base_labels,
        })
    }

    async fn write_wireguard_config(&self, instance_id: Uuid, config: &str) -> Result<String> {
        let root = Path::new(&self.runtime_paths.data_root)
            .join("extensions")
            .join("wireguard")
            .join(instance_id.to_string());
        fs::create_dir_all(&root).await?;
        let path = root.join("wg0.conf");
        fs::write(&path, config).await?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let permissions = std::fs::Permissions::from_mode(0o600);
            fs::set_permissions(&path, permissions).await?;
        }
        Ok(path.to_string_lossy().to_string())
    }

    async fn write_openvpn_config(
        &self,
        instance_id: Uuid,
        config: &str,
        username: Option<&str>,
        password: Option<&str>,
    ) -> Result<(String, Option<String>)> {
        let root = Path::new(&self.runtime_paths.data_root)
            .join("extensions")
            .join("openvpn")
            .join(instance_id.to_string());
        fs::create_dir_all(&root).await?;

        let auth_path = if let (Some(username), Some(password)) = (username, password) {
            let path = root.join("auth.txt");
            fs::write(&path, format!("{}\n{}\n", username.trim(), password.trim())).await?;
            set_private_file_permissions(&path).await?;
            Some(path)
        } else {
            None
        };

        let rendered_config = render_openvpn_config(config, auth_path.is_some());
        let config_path = root.join("custom.conf");
        fs::write(&config_path, rendered_config).await?;
        set_private_file_permissions(&config_path).await?;
        Ok((
            config_path.to_string_lossy().to_string(),
            auth_path.map(|path| path.to_string_lossy().to_string()),
        ))
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

    async fn restart_instance_runtime(&self, instance_id: Uuid) -> Result<()> {
        let name = container_name(instance_id);
        let handle = self
            .runtime
            .get_container_handle(&name)
            .await?
            .ok_or_else(|| anyhow!("runtime container '{}' not found", name))?;
        self.runtime.stop_container(&handle).await?;
        self.runtime.start_container(&handle).await?;
        Ok(())
    }

    async fn install_runtime_asset(
        &self,
        target_instance_id: Uuid,
        source_extension_id: String,
        source_extension_version: String,
        source_path: String,
        destination_path: String,
    ) -> Result<()> {
        let name = container_name(target_instance_id);
        let handle = self
            .runtime
            .get_container_handle(&name)
            .await?
            .ok_or_else(|| anyhow!("runtime container '{}' not found", name))?;

        let resolved_source = self.resolve_runtime_asset_source_path(
            &source_extension_id,
            &source_extension_version,
            &source_path,
        )?;
        let new_bytes = fs::read(&resolved_source).await.with_context(|| {
            format!(
                "reading runtime asset '{}' from extension {}@{}",
                resolved_source.display(),
                source_extension_id,
                source_extension_version
            )
        })?;
        if let Some(existing) = self
            .runtime
            .read_container_file(&handle, &destination_path)
            .await?
        {
            if existing == new_bytes {
                return Ok(());
            }
        }

        let destination_parent = Path::new(&destination_path)
            .parent()
            .filter(|parent| *parent != Path::new("/"))
            .map(|parent| parent.to_string_lossy().to_string());
        if let Some(parent) = destination_parent {
            self.runtime
                .ensure_container_directories(&handle, &[parent])
                .await?;
        }

        self.runtime
            .copy_host_path_to_container(&handle, &resolved_source, &destination_path)
            .await
            .with_context(|| {
                format!(
                    "copying runtime asset '{}' to '{}' for instance {}",
                    resolved_source.display(),
                    destination_path,
                    target_instance_id
                )
            })?;
        Ok(())
    }

    fn resolve_runtime_asset_source_path(
        &self,
        source_extension_id: &str,
        source_extension_version: &str,
        source_path: &str,
    ) -> Result<PathBuf> {
        let relative = Path::new(source_path);
        if relative.is_absolute() {
            bail!("runtime asset source_path must be relative");
        }
        if relative.components().any(|component| {
            matches!(
                component,
                std::path::Component::ParentDir | std::path::Component::RootDir
            )
        }) {
            bail!("runtime asset source_path must not escape the package root");
        }

        Ok(PathBuf::from(&self.runtime_paths.extensions_root)
            .join("unpacked")
            .join(source_extension_id)
            .join(source_extension_version)
            .join(relative))
    }

    async fn finalize_runtime_storage(
        &self,
        instance_id: Uuid,
        extension_id: &str,
        handle: &ContainerHandle,
        volumes: &[VolumeMount],
    ) -> Result<()> {
        let uses_named_config = volumes.iter().any(|volume| {
            volume.container_path == "/config"
                && volume.source_kind == VolumeMountSourceKind::NamedVolume
        });
        if !uses_named_config {
            return Ok(());
        }

        let directories = required_named_runtime_directories(extension_id, volumes);
        if !directories.is_empty() {
            let ownership_corrected = self
                .runtime
                .ensure_container_directories_owned_like(handle, "/config", &directories)
                .await
                .with_context(|| {
                    format!(
                        "ensuring owned named-volume runtime directories for instance {}",
                        instance_id
                    )
                })?;
            if ownership_corrected {
                self.restart_instance_runtime(instance_id).await?;
            }
        }

        self.compact_nzbget_named_config_if_needed(instance_id, extension_id, handle, volumes)
            .await?;

        Ok(())
    }

    async fn compact_nzbget_named_config_if_needed(
        &self,
        instance_id: Uuid,
        extension_id: &str,
        handle: &ContainerHandle,
        volumes: &[VolumeMount],
    ) -> Result<()> {
        let uses_named_config = volumes.iter().any(|volume| {
            volume.container_path == "/config"
                && volume.source_kind == VolumeMountSourceKind::NamedVolume
        });
        if !uses_named_config || !is_nzbget_extension_id(extension_id) {
            return Ok(());
        }

        let Some(bytes) = self
            .runtime
            .read_container_file(handle, "/config/nzbget.conf")
            .await?
        else {
            return Ok(());
        };
        let text = String::from_utf8(bytes).context("decoding nzbget named-volume config")?;
        let compacted = compact_nzbget_config_text(&text);
        if compacted == text {
            return Ok(());
        }
        self.write_nzbget_named_config(instance_id, handle, &compacted)
            .await
    }

    async fn apply_nzbget_named_volume_patch(
        &self,
        provider: &Provider,
        instance: &crate::db::models::ExtensionInstance,
        ctx: &DriverCtx,
        patch: &DriverPatch,
        provider_id: Uuid,
    ) -> Result<bool> {
        if !should_apply_nzbget_named_volume_patch(provider, instance.config_json.as_ref(), patch) {
            return Ok(false);
        }
        let DriverPatch::DownloaderNzb(nzb_patch) = patch else {
            return Ok(false);
        };

        let handle = self
            .runtime
            .get_container_handle(&container_name(instance.instance_id))
            .await?
            .ok_or_else(|| {
                anyhow!(
                    "runtime container '{}' not found",
                    container_name(instance.instance_id)
                )
            })?;
        let Some(bytes) = self
            .runtime
            .read_container_file(&handle, "/config/nzbget.conf")
            .await?
        else {
            bail!(
                "nzbget named-volume config file missing for instance {}",
                instance.instance_id
            );
        };
        let text = String::from_utf8(bytes).context("decoding nzbget named-volume config")?;
        let Some(rendered) = render_nzbget_config_patch(ctx, &text, nzb_patch)? else {
            return Ok(true);
        };
        self.write_nzbget_named_config(instance.instance_id, &handle, &rendered)
            .await?;
        let Some(contents) = read_config_text(
            self.runtime,
            instance.instance_id,
            None,
            "nzbget.conf",
            "/config/nzbget.conf",
        )
        .await?
        else {
            bail!(
                "nzbget named-volume config missing after patch for instance {}",
                instance.instance_id
            );
        };
        let missing = missing_nzbget_managed_paths(&contents);
        if !missing.is_empty() {
            bail!(
                "nzbget named-volume config did not converge for keys: {}",
                missing.join(", ")
            );
        }
        self.health_gate(provider_id, 30).await?;
        Ok(true)
    }

    async fn write_nzbget_named_config(
        &self,
        instance_id: Uuid,
        handle: &ContainerHandle,
        text: &str,
    ) -> Result<()> {
        let temp_dir =
            std::env::temp_dir().join(format!("elixir-nzbget-config-{}", instance_id.simple()));
        fs::create_dir_all(&temp_dir)
            .await
            .with_context(|| format!("creating temp nzbget config dir {}", temp_dir.display()))?;
        let temp_file = temp_dir.join("nzbget.conf");
        fs::write(&temp_file, text.as_bytes())
            .await
            .with_context(|| format!("writing temp nzbget config {}", temp_file.display()))?;

        self.runtime.stop_container(handle).await?;
        let copy_result = self
            .runtime
            .copy_host_path_to_container(handle, &temp_file, "/config/nzbget.conf")
            .await;
        let start_result = self.runtime.start_container(handle).await;
        let _ = fs::remove_dir_all(&temp_dir).await;

        copy_result?;
        start_result?;
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
    ) -> Result<Option<String>> {
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
            .as_ref()
            .cloned()
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

        resolve_indexer_credentials(&self.store, self.secrets, provider.instance_id, &mut patch)
            .await?;
        resolve_indexer_apps(
            &self.store,
            self.secrets,
            self.runtime,
            self.probe,
            &mut patch,
        )
        .await?;
        resolve_downloader_credentials(&self.store, self.secrets, self.probe, &mut patch).await?;

        let ctx = build_driver_ctx_for_provider(
            &self.store,
            self.secrets,
            self.runtime,
            &provider,
            &instance,
        )
        .await?;
        let semantics = driver.patch_semantics(&patch);
        let evaluation = driver
            .evaluate_patch(ctx.clone(), patch.clone())
            .await
            .context("evaluating driver patch drift")?;
        match evaluation.status {
            DriftStatus::InSync => return Ok(None),
            DriftStatus::Unknown
                if semantics.side_effect.is_service_disruptive()
                    || semantics.apply_policy != PatchApplyPolicy::PeriodicSafe =>
            {
                let mut detail = evaluation.message.unwrap_or_else(|| {
                    "driver patch drift could not be safely evaluated".to_string()
                });
                if !evaluation.non_comparable_fields.is_empty() {
                    let fields = evaluation
                        .non_comparable_fields
                        .iter()
                        .map(|field| field.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ");
                    detail.push_str(&format!(" (non-comparable fields: {fields})"));
                }
                bail!(
                    "driver patch requires explicit repair for connector {}: {}",
                    connector_extension_id,
                    detail
                );
            }
            DriftStatus::Drifted | DriftStatus::Unknown => {}
        }
        let restart_nzbget_runtime =
            should_restart_nzbget_after_patch(&provider, instance.config_json.as_ref(), &patch);
        if self
            .apply_nzbget_named_volume_patch(&provider, &instance, &ctx, &patch, target_provider_id)
            .await?
        {
            return Ok(None);
        }
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
        if restart_nzbget_runtime {
            self.restart_instance_runtime(provider.instance_id).await?;
            self.health_gate(provider.provider_id, 30).await?;
        }
        if let Some(detail) = result.message.as_ref() {
            tracing::warn!(
                connector_extension_id = %connector_extension_id,
                provider_id = %provider.provider_id,
                detail = %detail,
                "driver patch completed with warnings"
            );
        }
        Ok(result.message)
    }

    async fn apply_binding(&self, binding: NewBinding) -> Result<()> {
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
                        let detail = err.source.to_string();
                        let _ = self
                            .mark_provider_status(
                                provider_id,
                                ProviderHealthState::Unhealthy,
                                err.phase,
                                Some(detail.as_str()),
                            )
                            .await;
                        return Err(err.source);
                    }
                }
            }
            sleep(Duration::from_secs(2)).await;
        }
    }

    async fn transport_gate(&self, provider_id: Uuid, timeout_seconds: u64) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(timeout_seconds.max(1));
        loop {
            match self.transport_gate_once(provider_id).await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    if Instant::now() >= deadline {
                        let detail = err.source.to_string();
                        let _ = self
                            .mark_provider_status(
                                provider_id,
                                ProviderHealthState::Unhealthy,
                                err.phase,
                                Some(detail.as_str()),
                            )
                            .await;
                        return Err(err.source);
                    }
                }
            }
            sleep(Duration::from_secs(2)).await;
        }
    }

    async fn transport_gate_once(
        &self,
        provider_id: Uuid,
    ) -> std::result::Result<(), ReadinessCheckError> {
        let provider = self
            .store
            .get_provider(provider_id)
            .await
            .map_err(|err| ReadinessCheckError::new(ProviderReadinessPhase::Unknown, err))?
            .ok_or_else(|| {
                ReadinessCheckError::new(
                    ProviderReadinessPhase::Unknown,
                    anyhow!("provider {} not found", provider_id),
                )
            })?;

        let endpoint_json = provider.endpoint_json.as_ref().cloned().ok_or_else(|| {
            ReadinessCheckError::new(
                ProviderReadinessPhase::Unknown,
                anyhow!("provider {} has no endpoint", provider.provider_id),
            )
        })?;
        let endpoint: ProviderEndpoint = serde_json::from_value(endpoint_json)
            .context("parsing provider endpoint")
            .map_err(|err| ReadinessCheckError::new(ProviderReadinessPhase::Unknown, err))?;
        endpoint
            .validate()
            .map_err(|err| ReadinessCheckError::new(ProviderReadinessPhase::Unknown, err))?;

        probe_provider_transport(self.probe, provider.instance_id, &endpoint)
            .await
            .map_err(|err| ReadinessCheckError::new(ProviderReadinessPhase::Unknown, err))?;
        self.record_provider_readiness(
            provider.provider_id,
            ProviderReadinessPhase::TransportReady,
            None,
        )
        .await
        .map_err(|err| ReadinessCheckError::new(ProviderReadinessPhase::TransportReady, err))?;
        Ok(())
    }

    async fn bootstrap_gate(&self, provider_id: Uuid, timeout_seconds: u64) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(timeout_seconds.max(1));
        loop {
            match self.bootstrap_gate_once(provider_id).await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    if Instant::now() >= deadline {
                        let detail = err.source.to_string();
                        let _ = self
                            .mark_provider_status(
                                provider_id,
                                ProviderHealthState::Unhealthy,
                                err.phase,
                                Some(detail.as_str()),
                            )
                            .await;
                        return Err(err.source);
                    }
                }
            }
            sleep(Duration::from_secs(2)).await;
        }
    }

    async fn bootstrap_gate_once(
        &self,
        provider_id: Uuid,
    ) -> std::result::Result<(), ReadinessCheckError> {
        let provider = self
            .store
            .get_provider(provider_id)
            .await
            .map_err(|err| ReadinessCheckError::new(ProviderReadinessPhase::Unknown, err))?
            .ok_or_else(|| {
                ReadinessCheckError::new(
                    ProviderReadinessPhase::Unknown,
                    anyhow!("provider {} not found", provider_id),
                )
            })?;

        let endpoint_json = provider.endpoint_json.as_ref().cloned().ok_or_else(|| {
            ReadinessCheckError::new(
                ProviderReadinessPhase::Unknown,
                anyhow!("provider {} has no endpoint", provider.provider_id),
            )
        })?;
        let endpoint: ProviderEndpoint = serde_json::from_value(endpoint_json)
            .context("parsing provider endpoint")
            .map_err(|err| ReadinessCheckError::new(ProviderReadinessPhase::Unknown, err))?;
        endpoint
            .validate()
            .map_err(|err| ReadinessCheckError::new(ProviderReadinessPhase::Unknown, err))?;

        let instance = self
            .store
            .get_instance(provider.instance_id)
            .await
            .map_err(|err| ReadinessCheckError::new(ProviderReadinessPhase::Unknown, err))?
            .ok_or_else(|| {
                ReadinessCheckError::new(
                    ProviderReadinessPhase::Unknown,
                    anyhow!(
                        "instance {} not found for provider {}",
                        provider.instance_id,
                        provider.provider_id
                    ),
                )
            })?;

        probe_provider_transport(self.probe, provider.instance_id, &endpoint)
            .await
            .map_err(|err| ReadinessCheckError::new(ProviderReadinessPhase::Unknown, err))?;
        self.record_provider_readiness(
            provider.provider_id,
            ProviderReadinessPhase::TransportReady,
            None,
        )
        .await
        .map_err(|err| ReadinessCheckError::new(ProviderReadinessPhase::TransportReady, err))?;

        self.ensure_provider_bootstrap_ready(&provider, &instance, &endpoint)
            .await
            .map_err(|err| ReadinessCheckError::new(ProviderReadinessPhase::TransportReady, err))?;
        Ok(())
    }

    async fn health_gate_once(
        &self,
        provider_id: Uuid,
    ) -> std::result::Result<(), ReadinessCheckError> {
        let provider = self
            .store
            .get_provider(provider_id)
            .await
            .map_err(|err| ReadinessCheckError::new(ProviderReadinessPhase::Unknown, err))?
            .ok_or_else(|| {
                ReadinessCheckError::new(
                    ProviderReadinessPhase::Unknown,
                    anyhow!("provider {} not found", provider_id),
                )
            })?;

        let instance = self
            .store
            .get_instance(provider.instance_id)
            .await
            .map_err(|err| ReadinessCheckError::new(ProviderReadinessPhase::Unknown, err))?
            .ok_or_else(|| {
                ReadinessCheckError::new(
                    ProviderReadinessPhase::Unknown,
                    anyhow!(
                        "instance {} not found for provider {}",
                        provider.instance_id,
                        provider.provider_id
                    ),
                )
            })?;

        let recorded_phase = self
            .store
            .get_provider_readiness(provider.provider_id)
            .await
            .map_err(|err| ReadinessCheckError::new(ProviderReadinessPhase::Unknown, err))?
            .map(|readiness| readiness.readiness_phase)
            .unwrap_or(ProviderReadinessPhase::Unknown);
        let required_phase = health_gate_prerequisite_phase(&provider);
        if !readiness_satisfies(recorded_phase, required_phase) {
            return Err(ReadinessCheckError::new(
                recorded_phase,
                anyhow!(
                    "driver readiness requires prior {} gate",
                    required_phase.as_str()
                ),
            ));
        }

        let failure_phase = if matches!(recorded_phase, ProviderReadinessPhase::DriverReady) {
            required_phase
        } else {
            recorded_phase
        };
        self.ensure_provider_driver_ready(&provider, &instance)
            .await
            .map_err(|err| ReadinessCheckError::new(failure_phase, err))?;
        self.mark_provider_status(
            provider.provider_id,
            ProviderHealthState::Healthy,
            ProviderReadinessPhase::DriverReady,
            None,
        )
        .await
        .map_err(|err| ReadinessCheckError::new(failure_phase, err))?;
        Ok(())
    }

    async fn ensure_provider_bootstrap_ready(
        &self,
        provider: &Provider,
        instance: &crate::db::models::ExtensionInstance,
        endpoint: &ProviderEndpoint,
    ) -> Result<Option<ProviderReadinessPhase>> {
        if provider.capability == "media.manager.tv"
            && provider.implementation.as_deref() == Some("sonarr")
        {
            let config = parse_sonarr_instance_config(instance.config_json.as_ref())?;
            let key = read_sonarr_api_key(
                self.runtime,
                provider.instance_id,
                config.config_dir.as_deref(),
            )
            .await?
            .ok_or_else(|| anyhow!("sonarr config.xml not ready"))?;
            upsert_sonarr_secret(&self.store, self.secrets, provider.instance_id, &key).await?;
            self.normalize_arr_auth_config_if_needed(
                provider.instance_id,
                &key,
                endpoint,
                &["/api/v3/config/host", "/api/v4/config/host"],
            )
            .await?;
            self.record_provider_readiness(
                provider.provider_id,
                ProviderReadinessPhase::BootstrapReady,
                None,
            )
            .await?;
            return Ok(Some(ProviderReadinessPhase::BootstrapReady));
        }
        if provider.capability == "media.manager.movies"
            && provider.implementation.as_deref() == Some("radarr")
        {
            let config = parse_radarr_instance_config(instance.config_json.as_ref())?;
            let key = read_radarr_api_key(
                self.runtime,
                provider.instance_id,
                config.config_dir.as_deref(),
            )
            .await?
            .ok_or_else(|| anyhow!("radarr config.xml not ready"))?;
            upsert_radarr_secret(&self.store, self.secrets, provider.instance_id, &key).await?;
            self.normalize_arr_auth_config_if_needed(
                provider.instance_id,
                &key,
                endpoint,
                &["/api/v3/config/host", "/api/v4/config/host"],
            )
            .await?;
            self.record_provider_readiness(
                provider.provider_id,
                ProviderReadinessPhase::BootstrapReady,
                None,
            )
            .await?;
            return Ok(Some(ProviderReadinessPhase::BootstrapReady));
        }
        if provider.capability == "indexer.registry"
            && provider.implementation.as_deref() == Some("prowlarr")
        {
            let config = parse_prowlarr_instance_config(instance.config_json.as_ref())?;
            let key = read_prowlarr_api_key(
                self.runtime,
                provider.instance_id,
                config.config_dir.as_deref(),
            )
            .await?
            .ok_or_else(|| anyhow!("prowlarr config.xml not ready"))?;
            upsert_prowlarr_secret(&self.store, self.secrets, provider.instance_id, &key).await?;
            self.normalize_arr_auth_config_if_needed(
                provider.instance_id,
                &key,
                endpoint,
                &["/api/v1/config/host"],
            )
            .await?;
            self.record_provider_readiness(
                provider.provider_id,
                ProviderReadinessPhase::BootstrapReady,
                None,
            )
            .await?;
            return Ok(Some(ProviderReadinessPhase::BootstrapReady));
        }
        if provider.capability == "downloader.torrent"
            && provider.implementation.as_deref() == Some("qbittorrent")
        {
            let (username, password) =
                resolve_qbittorrent_credentials(&self.store, self.secrets, instance).await?;
            let endpoint_url = endpoint.canonical_url()?;
            let transport_base_url =
                resolve_driver_transport_base_url(provider.instance_id, endpoint)
                    .await?
                    .ok_or_else(|| {
                        anyhow!(
                            "driver transport is not ready for instance {} endpoint {}:{}",
                            provider.instance_id,
                            endpoint.host,
                            endpoint.port
                        )
                    })?;
            bootstrap_qbittorrent_session_cookie(
                &endpoint_url,
                Some(transport_base_url.as_str()),
                provider.instance_id,
                &username,
                &password,
            )
            .await?;
            self.record_provider_readiness(
                provider.provider_id,
                ProviderReadinessPhase::BootstrapReady,
                None,
            )
            .await?;
            return Ok(Some(ProviderReadinessPhase::BootstrapReady));
        }

        Ok(None)
    }

    async fn ensure_provider_driver_ready(
        &self,
        provider: &Provider,
        instance: &crate::db::models::ExtensionInstance,
    ) -> Result<()> {
        if provider.capability == ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY {
            ensure_candidate_provider_runtime_ready(provider).await?;
            return Ok(());
        }
        let Some(driver) = self.drivers.get(&provider.capability) else {
            return Ok(());
        };
        let ctx = build_driver_ctx_for_provider(
            &self.store,
            self.secrets,
            self.runtime,
            provider,
            instance,
        )
        .await?;
        driver.read_state(ctx).await?;
        Ok(())
    }

    async fn record_provider_readiness(
        &self,
        provider_id: Uuid,
        readiness_phase: ProviderReadinessPhase,
        readiness_detail: Option<&str>,
    ) -> Result<()> {
        self.store
            .upsert_provider_readiness(provider_id, readiness_phase, readiness_detail)
            .await
    }

    async fn mark_provider_status(
        &self,
        provider_id: Uuid,
        health_state: ProviderHealthState,
        readiness_phase: ProviderReadinessPhase,
        readiness_detail: Option<&str>,
    ) -> Result<()> {
        self.record_provider_readiness(provider_id, readiness_phase, readiness_detail)
            .await?;
        self.store
            .update_provider_health(provider_id, health_state)
            .await?;
        Ok(())
    }

    async fn apply_builtin_downloader_profile_if_needed(
        &self,
        provider: &Provider,
        instance: &crate::db::models::ExtensionInstance,
        endpoint: &ProviderEndpoint,
    ) -> Result<()> {
        let Some(implementation) = provider.implementation.as_deref() else {
            return Ok(());
        };
        let selected_profile = self.selected_downloader_profile().await?;

        match (provider.capability.as_str(), implementation) {
            ("downloader.torrent", "qbittorrent") => {
                let config = parse_qbittorrent_instance_config(instance.config_json.as_ref())?;
                let desired_version = qbittorrent_performance_profile_version(selected_profile);
                if qbittorrent_profile_version_matches(
                    config.performance_profile_version.as_deref(),
                    selected_profile,
                ) {
                    return Ok(());
                }
                let (username, password) =
                    resolve_qbittorrent_credentials(&self.store, self.secrets, instance).await?;
                let mut secrets = HashMap::new();
                secrets.insert("qbittorrent_username".to_string(), username);
                secrets.insert("qbittorrent_password".to_string(), password);

                let driver = self.drivers.get(&provider.capability).ok_or_else(|| {
                    anyhow!(
                        "no driver registered for capability '{}'",
                        provider.capability
                    )
                })?;
                let transport_base_url =
                    resolve_driver_transport_base_url(provider.instance_id, endpoint)
                        .await?
                        .ok_or_else(|| {
                            anyhow!(
                                "driver transport is not ready for instance {} endpoint {}:{}",
                                provider.instance_id,
                                endpoint.host,
                                endpoint.port
                            )
                        })?;
                let ctx = DriverCtx::new(
                    provider.provider_id,
                    provider.instance_id,
                    provider.capability.clone(),
                    endpoint.clone(),
                    Some(transport_base_url),
                    provider.implementation.clone(),
                    instance.config_json.clone(),
                    secrets,
                );
                let result = driver
                    .apply_patch(ctx, qbittorrent_performance_profile_patch(selected_profile))
                    .await?;
                if result.status == ApplyStatus::Deferred {
                    bail!(
                        "qbittorrent performance profile deferred: {}",
                        result
                            .message
                            .unwrap_or_else(|| "driver deferred".to_string())
                    );
                }
                persist_managed_defaults_profile_version(
                    &self.store,
                    instance.instance_id,
                    instance.config_json.clone(),
                    "qbittorrent_performance_profile_version",
                    desired_version,
                )
                .await?;
            }
            ("downloader.nzb", "nzbget") => {
                let config = parse_nzbget_instance_config(instance.config_json.as_ref())?;
                let desired_version = nzbget_performance_profile_version(selected_profile);
                if nzbget_profile_version_matches(
                    config.performance_profile_version.as_deref(),
                    selected_profile,
                ) && nzbget_managed_paths_are_current(
                    self.runtime,
                    instance.instance_id,
                    instance.config_json.as_ref(),
                )
                .await?
                {
                    return Ok(());
                }
                let patch = nzbget_performance_profile_patch(selected_profile);
                let ctx = build_driver_ctx_for_provider(
                    &self.store,
                    self.secrets,
                    self.runtime,
                    provider,
                    instance,
                )
                .await?;
                if !self
                    .apply_nzbget_named_volume_patch(
                        provider,
                        instance,
                        &ctx,
                        &patch,
                        provider.provider_id,
                    )
                    .await?
                {
                    let driver = self.drivers.get(&provider.capability).ok_or_else(|| {
                        anyhow!(
                            "no driver registered for capability '{}'",
                            provider.capability
                        )
                    })?;
                    let result = driver.apply_patch(ctx, patch).await?;
                    if result.status == ApplyStatus::Deferred {
                        bail!(
                            "nzbget performance profile deferred: {}",
                            result
                                .message
                                .unwrap_or_else(|| "driver deferred".to_string())
                        );
                    }
                }
                self.record_provider_readiness(
                    provider.provider_id,
                    ProviderReadinessPhase::BootstrapReady,
                    None,
                )
                .await?;
                let restart_required = runtime_has_bind_config_dir(instance.config_json.as_ref());
                persist_managed_defaults_profile_version(
                    &self.store,
                    instance.instance_id,
                    instance.config_json.clone(),
                    "nzbget_performance_profile_version",
                    desired_version,
                )
                .await?;
                if restart_required {
                    self.restart_instance_runtime(instance.instance_id).await?;
                    self.health_gate(provider.provider_id, 30).await?;
                }
            }
            _ => {}
        }

        Ok(())
    }

    async fn normalize_arr_auth_config_if_needed(
        &self,
        instance_id: Uuid,
        api_key: &str,
        endpoint: &ProviderEndpoint,
        config_paths: &[&str],
    ) -> Result<()> {
        let Some(base_url) = resolve_driver_transport_base_url(instance_id, endpoint).await? else {
            return Ok(());
        };
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .context("building arr host config client")?;

        let mut config = None;
        let mut selected_url = None;
        for path in config_paths {
            let url = Url::parse(&base_url)
                .context("parsing arr transport base url")?
                .join(path)
                .with_context(|| format!("building arr host config url for {path}"))?;
            let response = client
                .get(url.clone())
                .header("X-Api-Key", api_key)
                .send()
                .await
                .with_context(|| format!("fetching arr host config {path}"))?;
            if response.status() == reqwest::StatusCode::NOT_FOUND {
                continue;
            }
            let response = response
                .error_for_status()
                .with_context(|| format!("arr host config request failed for {path}"))?;
            let value: Value = response
                .json()
                .await
                .with_context(|| format!("decoding arr host config for {path}"))?;
            config = Some(value);
            selected_url = Some(url);
            break;
        }

        let Some(mut config) = config else {
            return Ok(());
        };
        let Some(url) = selected_url else {
            return Ok(());
        };

        if !arr_host_auth_config_requires_normalization(&config) {
            return Ok(());
        }

        let object = config
            .as_object_mut()
            .ok_or_else(|| anyhow!("arr host config must be a JSON object"))?;
        let authentication_method = object
            .get("authenticationMethod")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();

        if authentication_method == "none" {
            let (username, password) =
                ensure_arr_ui_credentials(&self.store, self.secrets, instance_id).await?;
            object.insert(
                "authenticationMethod".to_string(),
                Value::String("forms".to_string()),
            );
            object.insert("username".to_string(), Value::String(username));
            object.insert("password".to_string(), Value::String(password.clone()));
            object.insert("passwordConfirmation".to_string(), Value::String(password));
        }
        object.insert(
            "authenticationRequired".to_string(),
            Value::String("disabledForLocalAddresses".to_string()),
        );

        client
            .put(url)
            .header("X-Api-Key", api_key)
            .json(&config)
            .send()
            .await
            .context("updating arr host config")?
            .error_for_status()
            .context("arr host config update failed")?;

        Ok(())
    }

    async fn selected_downloader_profile(&self) -> Result<DownloaderPerformanceProfile> {
        let override_value = self
            .store
            .get_extension_setting("downloader_profile")
            .await?;
        Ok(DownloaderPerformanceProfile::from_setting_value(
            override_value.as_ref(),
            self.default_downloader_profile,
        ))
    }
}

const BALANCED_PERFORMANCE_PROFILE_VERSION: &str = "balanced-v2";
const AGGRESSIVE_PERFORMANCE_PROFILE_VERSION: &str = "aggressive-v2";
const NZBGET_BALANCED_PERFORMANCE_PROFILE_VERSION: &str = "balanced-v4";
const NZBGET_AGGRESSIVE_PERFORMANCE_PROFILE_VERSION: &str = "aggressive-v4";

async fn resolve_sonarr_api_key(
    store: &ExtensionStore<'_>,
    secrets: &SecretsManager,
    runtime: &dyn RuntimeManager,
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

    if let Some(key) =
        read_sonarr_api_key(runtime, instance.instance_id, config.config_dir.as_deref()).await?
    {
        upsert_sonarr_secret(store, secrets, instance.instance_id, &key).await?;
        return Ok(key);
    }

    bail!("sonarr api key is not available yet");
}

pub(crate) async fn build_driver_ctx_for_provider(
    store: &ExtensionStore<'_>,
    secrets_manager: &SecretsManager,
    runtime: &dyn RuntimeManager,
    provider: &Provider,
    instance: &crate::db::models::ExtensionInstance,
) -> Result<DriverCtx> {
    let endpoint_json = provider
        .endpoint_json
        .as_ref()
        .cloned()
        .ok_or_else(|| anyhow!("provider {} has no endpoint", provider.provider_id))?;
    let endpoint: ProviderEndpoint =
        serde_json::from_value(endpoint_json).context("parsing provider endpoint")?;
    endpoint.validate()?;

    let mut secrets = HashMap::new();
    if provider.capability == "media.manager.tv"
        && provider.implementation.as_deref() == Some("sonarr")
    {
        let api_key = resolve_sonarr_api_key(store, secrets_manager, runtime, instance).await?;
        secrets.insert("sonarr_api_key".to_string(), api_key);
    }
    if provider.capability == "media.manager.movies"
        && provider.implementation.as_deref() == Some("radarr")
    {
        let api_key = resolve_radarr_api_key(store, secrets_manager, runtime, instance).await?;
        secrets.insert("radarr_api_key".to_string(), api_key.clone());
        secrets.insert("api_key".to_string(), api_key);
    }
    if provider.capability == "indexer.registry"
        && provider.implementation.as_deref() == Some("prowlarr")
    {
        let api_key = resolve_prowlarr_api_key(store, secrets_manager, runtime, instance).await?;
        secrets.insert("prowlarr_api_key".to_string(), api_key);
    }
    if provider.capability == "downloader.torrent"
        && provider.implementation.as_deref() == Some("qbittorrent")
    {
        let (username, password) =
            resolve_qbittorrent_credentials(store, secrets_manager, instance).await?;
        secrets.insert("qbittorrent_username".to_string(), username);
        secrets.insert("qbittorrent_password".to_string(), password);
    }
    if provider.capability == "downloader.nzb"
        && provider.implementation.as_deref() == Some("nzbget")
    {
        let (username, password) =
            resolve_nzbget_credentials(store, secrets_manager, instance).await?;
        secrets.insert("nzbget_username".to_string(), username);
        secrets.insert("nzbget_password".to_string(), password);
        secrets.extend(resolve_nzbget_server_slot_secrets(store, secrets_manager, instance).await?);
    }

    let transport_base_url = resolve_driver_transport_base_url(provider.instance_id, &endpoint)
        .await?
        .ok_or_else(|| {
            anyhow!(
                "driver transport is not ready for instance {} endpoint {}:{}",
                provider.instance_id,
                endpoint.host,
                endpoint.port
            )
        })?;
    Ok(DriverCtx::new(
        provider.provider_id,
        provider.instance_id,
        provider.capability.clone(),
        endpoint,
        Some(transport_base_url),
        provider.implementation.clone(),
        instance.config_json.clone(),
        secrets,
    ))
}

async fn resolve_radarr_api_key(
    store: &ExtensionStore<'_>,
    secrets: &SecretsManager,
    runtime: &dyn RuntimeManager,
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

    if let Some(key) =
        read_radarr_api_key(runtime, instance.instance_id, config.config_dir.as_deref()).await?
    {
        upsert_radarr_secret(store, secrets, instance.instance_id, &key).await?;
        return Ok(key);
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
    runtime: &dyn RuntimeManager,
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

    if let Some(key) =
        read_prowlarr_api_key(runtime, instance.instance_id, config.config_dir.as_deref()).await?
    {
        upsert_prowlarr_secret(store, secrets, instance.instance_id, &key).await?;
        return Ok(key);
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

async fn upsert_arr_ui_secrets(
    store: &ExtensionStore<'_>,
    secrets: &SecretsManager,
    instance_id: Uuid,
    username: &str,
    password: &str,
) -> Result<()> {
    let encrypted_username = secrets.encrypt(username)?;
    let encrypted_password = secrets.encrypt(password)?;
    for (key, value_encrypted) in [
        ("arr_ui_username", encrypted_username),
        ("arr_ui_password", encrypted_password),
    ] {
        store
            .upsert_secret(&NewSecret {
                secret_id: Uuid::new_v4(),
                scope: SecretScope::Instance,
                scope_id: Some(instance_id),
                key: key.to_string(),
                value_encrypted,
                rotatable: false,
            })
            .await?;
    }
    Ok(())
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

async fn resolve_nzbget_credentials(
    store: &ExtensionStore<'_>,
    secrets: &SecretsManager,
    instance: &crate::db::models::ExtensionInstance,
) -> Result<(String, String)> {
    let config = parse_nzbget_instance_config(instance.config_json.as_ref())?;
    if let (Some(username), Some(password)) = (config.username, config.password) {
        upsert_nzbget_secrets(store, secrets, instance.instance_id, &username, &password).await?;
        return Ok((username, password));
    }

    ensure_nzbget_credentials(store, secrets, instance.instance_id).await
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

async fn ensure_nzbget_secrets(
    store: &ExtensionStore<'_>,
    secrets: &SecretsManager,
    instance_id: Uuid,
    env: &[ManifestRuntimeEnv],
) -> Result<()> {
    let mut requires_nzbget = false;
    for entry in env {
        if let Some(from_secret) = entry.from_secret.as_ref() {
            let trimmed = from_secret.trim();
            if trimmed == "instance:nzbget_username" || trimmed == "instance:nzbget_password" {
                requires_nzbget = true;
                break;
            }
        }
    }
    if !requires_nzbget {
        return Ok(());
    }
    let _ = ensure_nzbget_credentials(store, secrets, instance_id).await?;
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

async fn ensure_nzbget_credentials(
    store: &ExtensionStore<'_>,
    secrets: &SecretsManager,
    instance_id: Uuid,
) -> Result<(String, String)> {
    let username = store
        .get_secret(SecretScope::Instance, Some(instance_id), "nzbget_username")
        .await?
        .map(|secret| secrets.decrypt(&secret.value_encrypted))
        .transpose()?
        .filter(|value| !value.trim().is_empty());
    let password = store
        .get_secret(SecretScope::Instance, Some(instance_id), "nzbget_password")
        .await?
        .map(|secret| secrets.decrypt(&secret.value_encrypted))
        .transpose()?
        .filter(|value| !value.trim().is_empty());

    match (username, password) {
        (Some(username), Some(password)) => Ok((username, password)),
        (None, None) => {
            let (username, password) = generate_nzbget_credentials();
            upsert_nzbget_secrets(store, secrets, instance_id, &username, &password).await?;
            Ok((username, password))
        }
        _ => bail!("nzbget credentials are partially configured"),
    }
}

async fn ensure_arr_ui_credentials(
    store: &ExtensionStore<'_>,
    secrets: &SecretsManager,
    instance_id: Uuid,
) -> Result<(String, String)> {
    let username = store
        .get_secret(SecretScope::Instance, Some(instance_id), "arr_ui_username")
        .await?
        .map(|secret| secrets.decrypt(&secret.value_encrypted))
        .transpose()?
        .filter(|value| !value.trim().is_empty());
    let password = store
        .get_secret(SecretScope::Instance, Some(instance_id), "arr_ui_password")
        .await?
        .map(|secret| secrets.decrypt(&secret.value_encrypted))
        .transpose()?
        .filter(|value| !value.trim().is_empty());

    match (username, password) {
        (Some(username), Some(password)) => Ok((username, password)),
        (None, None) => {
            let (username, password) = generate_arr_ui_credentials();
            upsert_arr_ui_secrets(store, secrets, instance_id, &username, &password).await?;
            Ok((username, password))
        }
        _ => bail!("arr ui credentials are partially configured"),
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

fn generate_nzbget_credentials() -> (String, String) {
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

fn generate_arr_ui_credentials() -> (String, String) {
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

fn compact_nzbget_config_text(text: &str) -> String {
    let lines = text.lines().collect::<Vec<_>>();
    let mut last_assignment = HashMap::new();
    for (index, line) in lines.iter().enumerate() {
        if let Some(key) = nzbget_config_key(line) {
            last_assignment.insert(key, index);
        }
    }

    let mut changed = false;
    let mut rendered = Vec::with_capacity(lines.len());
    for (index, line) in lines.iter().enumerate() {
        let Some(key) = nzbget_config_key(line) else {
            rendered.push((*line).to_string());
            continue;
        };
        if last_assignment.get(key).copied() != Some(index) {
            changed = true;
            continue;
        }
        rendered.push((*line).to_string());
    }

    if !changed {
        return text.to_string();
    }

    let mut output = rendered.join("\n");
    output.push('\n');
    output
}

fn nzbget_config_key(line: &str) -> Option<&str> {
    let trimmed = line.trim_start();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return None;
    }
    let (name, _) = line.split_once('=')?;
    Some(name.trim())
}

fn is_default_wireguard_downloader_extension_id(extension_id: &str) -> bool {
    is_qbittorrent_extension_id(extension_id) || is_nzbget_extension_id(extension_id)
}

fn downloader_uses_gateway_namespace(
    actual_network_mode: &str,
    gateway_name: &str,
    gateway_id: &str,
) -> bool {
    container_network_namespace_matches(actual_network_mode, gateway_name, Some(gateway_id))
}

async fn resolve_indexer_apps(
    store: &ExtensionStore<'_>,
    secrets: &SecretsManager,
    runtime: &dyn RuntimeManager,
    probe: &dyn ProbeRunner,
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
            let target = resolve_sonarr_target_for_app(store, host_hint.as_deref()).await?;
            ensure_dependency_provider_reachable(
                probe,
                &target,
                "Sonarr",
                "Prowlarr application registration",
            )
            .await?;
            let api_key = resolve_sonarr_api_key(store, secrets, runtime, &target.instance).await?;
            app.url = target.endpoint.canonical_url()?;
            app.api_key = Some(api_key);
        } else if is_radarr_app(&app.implementation) {
            let target = resolve_radarr_target_for_app(store, host_hint.as_deref()).await?;
            ensure_dependency_provider_reachable(
                probe,
                &target,
                "Radarr",
                "Prowlarr application registration",
            )
            .await?;
            let api_key = resolve_radarr_api_key(store, secrets, runtime, &target.instance).await?;
            app.url = target.endpoint.canonical_url()?;
            app.api_key = Some(api_key);
        }
    }
    Ok(())
}

async fn resolve_downloader_credentials(
    store: &ExtensionStore<'_>,
    secrets: &SecretsManager,
    probe: &dyn ProbeRunner,
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
        let host_hint = url_host(&downloader.url);
        if is_qbittorrent_downloader(&downloader.r#type) {
            let target =
                resolve_qbittorrent_target_for_downloader(store, host_hint.as_deref()).await?;
            ensure_dependency_provider_reachable(
                probe,
                &target,
                "qBittorrent",
                "download client registration",
            )
            .await?;
            downloader.url = target.endpoint.canonical_url()?;
            if downloader_has_credentials(downloader) {
                continue;
            }
            let (username, password) =
                resolve_qbittorrent_credentials(store, secrets, &target.instance).await?;
            apply_downloader_credentials(downloader, username, password);
            continue;
        }
        if is_nzbget_downloader(&downloader.r#type) {
            let target = resolve_nzbget_target_for_downloader(store, host_hint.as_deref()).await?;
            ensure_dependency_provider_reachable(
                probe,
                &target,
                "NZBGet",
                "download client registration",
            )
            .await?;
            downloader.url = target.endpoint.canonical_url()?;
            if downloader_has_credentials(downloader) {
                continue;
            }
            let (username, password) =
                resolve_nzbget_credentials(store, secrets, &target.instance).await?;
            apply_downloader_credentials(downloader, username, password);
            continue;
        }
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct ManagedProviderTarget {
    provider: Provider,
    instance: crate::db::models::ExtensionInstance,
    endpoint: ProviderEndpoint,
    aliases: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HostProbeTarget {
    base_url: String,
    host: String,
    port: u16,
}

#[derive(Debug)]
struct ReadinessCheckError {
    phase: ProviderReadinessPhase,
    source: anyhow::Error,
}

impl ReadinessCheckError {
    fn new(phase: ProviderReadinessPhase, source: anyhow::Error) -> Self {
        Self { phase, source }
    }
}

fn readiness_satisfies(current: ProviderReadinessPhase, required: ProviderReadinessPhase) -> bool {
    readiness_rank(current) >= readiness_rank(required)
}

fn readiness_rank(phase: ProviderReadinessPhase) -> u8 {
    match phase {
        ProviderReadinessPhase::Unknown => 0,
        ProviderReadinessPhase::TransportReady => 1,
        ProviderReadinessPhase::BootstrapReady => 2,
        ProviderReadinessPhase::DriverReady => 3,
    }
}

fn health_gate_prerequisite_phase(provider: &Provider) -> ProviderReadinessPhase {
    if provider_requires_bootstrap(provider) {
        ProviderReadinessPhase::BootstrapReady
    } else {
        ProviderReadinessPhase::TransportReady
    }
}

fn provider_requires_bootstrap(provider: &Provider) -> bool {
    matches!(
        (
            provider.capability.as_str(),
            provider.implementation.as_deref(),
        ),
        ("media.manager.tv", Some("sonarr"))
            | ("media.manager.movies", Some("radarr"))
            | ("indexer.registry", Some("prowlarr"))
            | ("downloader.torrent", Some("qbittorrent"))
    )
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

fn is_nzbget_downloader(implementation: &str) -> bool {
    implementation
        .trim()
        .to_ascii_lowercase()
        .starts_with("nzbget")
}

fn apply_downloader_credentials(
    downloader: &mut DownloaderSpec,
    username: String,
    password: String,
) {
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

async fn resolve_sonarr_target_for_app(
    store: &ExtensionStore<'_>,
    host_hint: Option<&str>,
) -> Result<ManagedProviderTarget> {
    resolve_provider_target(
        store,
        "media.manager.tv",
        "sonarr",
        host_hint,
        "sonarr provider not found for prowlarr app registration",
        "multiple sonarr providers found; specify a managed Sonarr host alias in the app url",
    )
    .await
}

async fn resolve_radarr_target_for_app(
    store: &ExtensionStore<'_>,
    host_hint: Option<&str>,
) -> Result<ManagedProviderTarget> {
    resolve_provider_target(
        store,
        "media.manager.movies",
        "radarr",
        host_hint,
        "radarr provider not found for prowlarr app registration",
        "multiple radarr providers found; specify a managed Radarr host alias in the app url",
    )
    .await
}

async fn resolve_qbittorrent_target_for_downloader(
    store: &ExtensionStore<'_>,
    host_hint: Option<&str>,
) -> Result<ManagedProviderTarget> {
    resolve_provider_target(
        store,
        "downloader.torrent",
        "qbittorrent",
        host_hint,
        "qbittorrent provider not found for downloader credentials",
        "multiple qbittorrent providers found; specify a managed qBittorrent host alias in the downloader url",
    )
    .await
}

async fn resolve_nzbget_target_for_downloader(
    store: &ExtensionStore<'_>,
    host_hint: Option<&str>,
) -> Result<ManagedProviderTarget> {
    resolve_provider_target(
        store,
        "downloader.nzb",
        "nzbget",
        host_hint,
        "nzbget provider not found for downloader credentials",
        "multiple nzbget providers found; specify a managed NZBGet host alias in the downloader url",
    )
    .await
}

async fn resolve_provider_target(
    store: &ExtensionStore<'_>,
    capability: &str,
    implementation: &str,
    host_hint: Option<&str>,
    not_found_message: &str,
    multiple_message: &str,
) -> Result<ManagedProviderTarget> {
    let candidates = list_managed_provider_targets(store, capability, implementation).await?;
    if candidates.is_empty() {
        bail!("{}", not_found_message);
    }

    if let Some(host) = host_hint {
        if let Some(candidate) = candidates
            .iter()
            .find(|candidate| provider_host_matches_alias(host, &candidate.aliases))
            .cloned()
        {
            return Ok(candidate);
        }
    }

    if candidates.len() == 1 {
        return Ok(candidates[0].clone());
    }

    bail!("{}", multiple_message);
}

async fn list_managed_provider_targets(
    store: &ExtensionStore<'_>,
    capability: &str,
    implementation: &str,
) -> Result<Vec<ManagedProviderTarget>> {
    let providers = store.list_provider_details().await?;
    let mut candidates = Vec::new();
    for detail in providers {
        if detail.provider.capability != capability {
            continue;
        }
        if let Some(value) = detail.provider.implementation.as_deref() {
            if !value.eq_ignore_ascii_case(implementation) {
                continue;
            }
        }
        let Some(endpoint_json) = detail.provider.endpoint_json.as_ref() else {
            continue;
        };
        let endpoint: ProviderEndpoint =
            serde_json::from_value(endpoint_json.clone()).context("parsing provider endpoint")?;
        let instance = store
            .get_instance(detail.provider.instance_id)
            .await?
            .ok_or_else(|| {
                anyhow!(
                    "{} instance {} not found",
                    implementation,
                    detail.provider.instance_id
                )
            })?;
        let aliases = provider_host_aliases(store, &detail, &instance, &endpoint).await?;
        candidates.push(ManagedProviderTarget {
            provider: detail.provider,
            instance,
            endpoint,
            aliases,
        });
    }
    Ok(candidates)
}

async fn ensure_dependency_provider_reachable(
    probe: &dyn ProbeRunner,
    target: &ManagedProviderTarget,
    dependency_name: &str,
    action_name: &str,
) -> Result<()> {
    if let Err(err) =
        probe_provider_transport(probe, target.provider.instance_id, &target.endpoint).await
    {
        return Err(deferred_dependency_error(format!(
            "{dependency_name} is not reachable yet; deferring {action_name}: {err}"
        )));
    }
    Ok(())
}

async fn provider_host_aliases(
    store: &ExtensionStore<'_>,
    detail: &ProviderDetails,
    instance: &crate::db::models::ExtensionInstance,
    endpoint: &ProviderEndpoint,
) -> Result<Vec<String>> {
    let extension = store.get_extension(&detail.extension_id).await?;
    let service_name = extension.and_then(|extension| {
        serde_json::from_value::<ExtensionManifest>(extension.manifest_json)
            .ok()
            .and_then(|manifest| {
                manifest
                    .runtime
                    .and_then(|runtime| runtime.service_name)
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
            })
    });
    let (computed_aliases, _) = build_aliases(
        &detail.extension_id,
        &instance.instance_name,
        instance.instance_id,
        service_name,
    );
    let mut aliases = vec![endpoint.host.clone()];
    for alias in computed_aliases {
        if !aliases
            .iter()
            .any(|existing| existing.eq_ignore_ascii_case(&alias))
        {
            aliases.push(alias);
        }
    }
    Ok(aliases)
}

fn provider_host_matches_alias(host: &str, aliases: &[String]) -> bool {
    aliases
        .iter()
        .any(|alias| alias.eq_ignore_ascii_case(host.trim()))
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

async fn upsert_nzbget_secrets(
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
            key: "nzbget_username".to_string(),
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
            key: "nzbget_password".to_string(),
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
    #[serde(default)]
    managed_defaults: Option<ManagedDefaultsConfig>,
}

#[derive(Default, Deserialize)]
struct NzbgetInstanceConfig {
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    password: Option<String>,
    #[serde(default)]
    managed_defaults: Option<ManagedDefaultsConfig>,
}

#[derive(Default, Deserialize)]
struct ManagedDefaultsConfig {
    #[serde(default)]
    qbittorrent_performance_profile_version: Option<String>,
    #[serde(default)]
    nzbget_performance_profile_version: Option<String>,
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
    performance_profile_version: Option<String>,
}

struct ParsedNzbgetConfig {
    username: Option<String>,
    password: Option<String>,
    performance_profile_version: Option<String>,
}

#[derive(Default, Deserialize)]
struct PersistedNzbgetServerSecretSlot {
    slot: u32,
}

#[derive(Default, Deserialize)]
struct PersistedNzbgetServerInventoryConfig {
    #[serde(default)]
    server_inventory: Vec<PersistedNzbgetServerSecretSlot>,
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
            performance_profile_version: None,
        });
    };
    let config: QbittorrentInstanceConfig =
        serde_json::from_value(value.clone()).context("parsing qbittorrent instance config")?;
    Ok(ParsedQbittorrentConfig {
        username: config.username,
        password: config.password,
        performance_profile_version: config
            .managed_defaults
            .and_then(|defaults| defaults.qbittorrent_performance_profile_version),
    })
}

fn parse_nzbget_instance_config(value: Option<&serde_json::Value>) -> Result<ParsedNzbgetConfig> {
    let Some(value) = value else {
        return Ok(ParsedNzbgetConfig {
            username: None,
            password: None,
            performance_profile_version: None,
        });
    };
    let config: NzbgetInstanceConfig =
        serde_json::from_value(value.clone()).context("parsing nzbget instance config")?;
    Ok(ParsedNzbgetConfig {
        username: config.username,
        password: config.password,
        performance_profile_version: config
            .managed_defaults
            .and_then(|defaults| defaults.nzbget_performance_profile_version),
    })
}

fn parse_nzbget_server_secret_slots(value: Option<&serde_json::Value>) -> Result<Vec<u32>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let config: PersistedNzbgetServerInventoryConfig =
        serde_json::from_value(value.clone()).context("parsing nzbget server inventory config")?;
    Ok(config
        .server_inventory
        .into_iter()
        .map(|entry| entry.slot)
        .filter(|slot| *slot > 0)
        .collect())
}

async fn resolve_nzbget_server_slot_secrets(
    store: &ExtensionStore<'_>,
    secrets: &SecretsManager,
    instance: &crate::db::models::ExtensionInstance,
) -> Result<HashMap<String, String>> {
    let mut resolved = HashMap::new();
    for slot in parse_nzbget_server_secret_slots(instance.config_json.as_ref())? {
        for key in ["username", "password"] {
            let secret_key = format!("nzbget.server.{slot}.{key}");
            let Some(secret) = store
                .get_secret(
                    SecretScope::Instance,
                    Some(instance.instance_id),
                    &secret_key,
                )
                .await?
            else {
                continue;
            };
            let value = secrets.decrypt(&secret.value_encrypted)?;
            if !value.trim().is_empty() {
                resolved.insert(secret_key, value);
            }
        }
    }
    Ok(resolved)
}

async fn read_sonarr_api_key(
    runtime: &dyn RuntimeManager,
    instance_id: Uuid,
    config_dir: Option<&str>,
) -> Result<Option<String>> {
    read_arr_api_key(runtime, instance_id, config_dir).await
}

async fn read_radarr_api_key(
    runtime: &dyn RuntimeManager,
    instance_id: Uuid,
    config_dir: Option<&str>,
) -> Result<Option<String>> {
    read_arr_api_key(runtime, instance_id, config_dir).await
}

async fn read_prowlarr_api_key(
    runtime: &dyn RuntimeManager,
    instance_id: Uuid,
    config_dir: Option<&str>,
) -> Result<Option<String>> {
    read_arr_api_key(runtime, instance_id, config_dir).await
}

fn arr_host_auth_config_requires_normalization(config: &Value) -> bool {
    let Some(object) = config.as_object() else {
        return false;
    };
    let authentication_method = object
        .get("authenticationMethod")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let authentication_required = object
        .get("authenticationRequired")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();

    authentication_method == "none" || authentication_required == "enabled"
}

async fn read_arr_api_key(
    runtime: &dyn RuntimeManager,
    instance_id: Uuid,
    config_dir: Option<&str>,
) -> Result<Option<String>> {
    let Some(content) = read_config_text(
        runtime,
        instance_id,
        config_dir,
        "config.xml",
        "/config/config.xml",
    )
    .await?
    else {
        return Ok(None);
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

async fn resolve_driver_transport_base_url(
    instance_id: Uuid,
    endpoint: &ProviderEndpoint,
) -> Result<Option<String>> {
    const TRANSPORT_RESOLUTION_ATTEMPTS: usize = 6;
    const TRANSPORT_RESOLUTION_RETRY_DELAY_MS: u64 = 500;

    let canonical = endpoint.canonical_url()?;
    if endpoint_host_resolves(&endpoint.host, endpoint.port).await
        || !endpoint_uses_container_network(endpoint)
    {
        return Ok(Some(canonical));
    }

    let mut last_lookup_error = None;
    for attempt in 0..TRANSPORT_RESOLUTION_ATTEMPTS {
        match lookup_docker_published_port(instance_id, endpoint.port).await {
            Ok(Some(host_port)) => {
                return Ok(Some(driver_transport_base_url(endpoint, host_port)));
            }
            Ok(None) => {}
            Err(err) => {
                last_lookup_error = Some(err);
            }
        }

        if endpoint_host_resolves(&endpoint.host, endpoint.port).await {
            return Ok(Some(canonical.clone()));
        }

        if attempt + 1 < TRANSPORT_RESOLUTION_ATTEMPTS {
            sleep(Duration::from_millis(TRANSPORT_RESOLUTION_RETRY_DELAY_MS)).await;
        }
    }

    if let Some(err) = last_lookup_error {
        tracing::warn!(
            "driver transport fallback failed for instance {} endpoint {}:{}: {}",
            instance_id,
            endpoint.host,
            endpoint.port,
            err
        );
    } else {
        tracing::warn!(
            "driver transport fallback unavailable for instance {} endpoint {}:{}",
            instance_id,
            endpoint.host,
            endpoint.port
        );
    }

    Ok(None)
}

async fn ensure_candidate_provider_runtime_ready(provider: &Provider) -> Result<()> {
    let endpoint_json = provider.endpoint_json.as_ref().cloned().ok_or_else(|| {
        anyhow!(
            "candidate provider {} has no endpoint",
            provider.provider_id
        )
    })?;
    let endpoint: ProviderEndpoint =
        serde_json::from_value(endpoint_json).context("parsing candidate provider endpoint")?;
    endpoint.validate()?;
    let base_url = resolve_driver_transport_base_url(provider.instance_id, &endpoint)
        .await?
        .ok_or_else(|| {
            anyhow!(
                "candidate provider transport is not ready for instance {} endpoint {}:{}",
                provider.instance_id,
                endpoint.host,
                endpoint.port
            )
        })?;
    let mut base = Url::parse(&base_url).context("parsing candidate provider base URL")?;
    let mut path = base.path().trim_end_matches('/').to_string();
    if path.is_empty() {
        path.push('/');
    } else {
        path.push('/');
    }
    base.set_path(&path);
    let health_url = base
        .join("health")
        .context("building candidate provider health URL")?;
    let response = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .context("building candidate provider health client")?
        .get(health_url.clone())
        .send()
        .await
        .with_context(|| format!("calling candidate provider health endpoint at {health_url}"))?;
    let status = response.status();
    if !status.is_success() {
        bail!("candidate provider health endpoint returned {status}");
    }
    let payload: serde_json::Value = response
        .json()
        .await
        .context("parsing candidate provider health response")?;
    if payload.get("ok").and_then(serde_json::Value::as_bool) == Some(false) {
        bail!("candidate provider health endpoint reported not ok");
    }
    if payload.get("ready").and_then(serde_json::Value::as_bool) == Some(false) {
        bail!("candidate provider health endpoint reported not ready");
    }
    Ok(())
}

fn probe_container_host(host: &str) -> String {
    if host.eq_ignore_ascii_case("localhost") || host == "127.0.0.1" {
        "host.docker.internal".to_string()
    } else {
        host.to_string()
    }
}

fn probe_target_from_endpoint(endpoint: &ProviderEndpoint) -> Result<HostProbeTarget> {
    Ok(HostProbeTarget {
        base_url: endpoint.canonical_url()?,
        host: probe_container_host(&endpoint.host),
        port: endpoint.port,
    })
}

fn host_probe_target_from_base_url(base_url: &str) -> Result<HostProbeTarget> {
    let parsed = Url::parse(base_url).context("parsing host probe transport base url")?;
    let host = parsed
        .host_str()
        .map(str::to_string)
        .ok_or_else(|| anyhow!("host probe transport URL has no host: {base_url}"))?;
    let port = parsed
        .port_or_known_default()
        .ok_or_else(|| anyhow!("host probe transport URL has no port: {base_url}"))?;
    Ok(HostProbeTarget {
        base_url: base_url.to_string(),
        host: probe_container_host(&host),
        port,
    })
}

async fn resolve_probe_target(
    instance_id: Uuid,
    endpoint: &ProviderEndpoint,
) -> Result<HostProbeTarget> {
    if endpoint_uses_container_network(endpoint) {
        return probe_target_from_endpoint(endpoint);
    }

    let canonical = probe_target_from_endpoint(endpoint)?;
    if endpoint_host_resolves(&endpoint.host, endpoint.port).await {
        return Ok(canonical);
    }

    let Some(base_url) = resolve_driver_transport_base_url(instance_id, endpoint).await? else {
        bail!(
            "host probe transport is not ready for instance {} endpoint {}:{}",
            instance_id,
            endpoint.host,
            endpoint.port
        );
    };
    host_probe_target_from_base_url(&base_url)
}

async fn probe_host_target(probe: &dyn ProbeRunner, target: &HostProbeTarget) -> Result<()> {
    probe
        .probe_dns(&target.host)
        .await
        .and_then(|result| ensure_probe_ok(result, "dns"))?;
    probe
        .probe_tcp(&target.host, target.port)
        .await
        .and_then(|result| ensure_probe_ok(result, "tcp"))?;
    Ok(())
}

async fn probe_provider_transport(
    probe: &dyn ProbeRunner,
    instance_id: Uuid,
    endpoint: &ProviderEndpoint,
) -> Result<HostProbeTarget> {
    let target = resolve_probe_target(instance_id, endpoint).await?;
    probe_host_target(probe, &target).await?;
    Ok(target)
}

async fn endpoint_host_resolves(host: &str, port: u16) -> bool {
    lookup_host((host, port))
        .await
        .map(|mut addrs| addrs.next().is_some())
        .unwrap_or(false)
}

async fn lookup_docker_published_port(
    instance_id: Uuid,
    container_port: u16,
) -> Result<Option<u16>> {
    let app_container_name = container_name(instance_id);
    if !docker_container_exists(&app_container_name).await? {
        return Ok(None);
    }

    let mut candidates = vec![app_container_name];
    candidates.extend(list_docker_container_names(instance_id, true).await?);
    candidates.extend(list_docker_container_names(instance_id, false).await?);

    let mut seen = HashSet::new();
    for candidate in candidates {
        if candidate.trim().is_empty() || !seen.insert(candidate.clone()) {
            continue;
        }
        if let Some(host_port) = inspect_docker_published_port(&candidate, container_port).await? {
            return Ok(Some(host_port));
        }
    }

    Ok(None)
}

async fn docker_container_exists(container_name: &str) -> Result<bool> {
    let inspect_args = vec![
        "inspect".to_string(),
        "--format".to_string(),
        "{{.Name}}".to_string(),
        container_name.to_string(),
    ];
    match run_docker_stdout(&inspect_args).await {
        Ok(_) => Ok(true),
        Err(err) if docker_container_missing_error(&err) => Ok(false),
        Err(err) => Err(err),
    }
}

async fn run_docker_stdout(args: &[String]) -> Result<String> {
    let output = Command::new("docker")
        .args(args)
        .output()
        .await
        .with_context(|| format!("running docker {}", args.join(" ")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "docker {} failed (status {:?}): {}",
            args.join(" "),
            output.status.code(),
            stderr.trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

fn endpoint_uses_container_network(endpoint: &ProviderEndpoint) -> bool {
    endpoint
        .network
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
}

fn driver_transport_base_url(endpoint: &ProviderEndpoint, host_port: u16) -> String {
    format!(
        "{}://127.0.0.1:{host_port}{}",
        endpoint.scheme, endpoint.base_path
    )
}

async fn list_docker_container_names(instance_id: Uuid, running_only: bool) -> Result<Vec<String>> {
    let mut ps_args = vec!["ps".to_string()];
    if !running_only {
        ps_args.push("-a".to_string());
    }
    ps_args.extend([
        "--filter".to_string(),
        format!("label=elixir.instance_id={instance_id}"),
        "--format".to_string(),
        "{{.Names}}".to_string(),
    ]);
    let containers = run_docker_stdout(&ps_args).await?;
    Ok(parse_docker_container_names(&containers))
}

fn parse_docker_container_names(output: &str) -> Vec<String> {
    output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect()
}

async fn inspect_docker_published_port(
    container_name: &str,
    container_port: u16,
) -> Result<Option<u16>> {
    let inspect_args = vec![
        "inspect".to_string(),
        "--format".to_string(),
        "{{json .NetworkSettings.Ports}}".to_string(),
        container_name.to_string(),
    ];
    let ports_json = match run_docker_stdout(&inspect_args).await {
        Ok(ports_json) => ports_json,
        Err(err) if docker_container_missing_error(&err) => return Ok(None),
        Err(err) => return Err(err),
    };
    parse_docker_published_port(&ports_json, container_port)
}

fn parse_docker_published_port(ports_json: &str, container_port: u16) -> Result<Option<u16>> {
    let ports: serde_json::Value =
        serde_json::from_str(ports_json.trim()).context("parsing docker ports inspect output")?;
    let key = format!("{container_port}/tcp");
    let bindings = ports.get(&key).and_then(serde_json::Value::as_array);
    let Some(binding) = bindings.and_then(|values| values.first()) else {
        return Ok(None);
    };
    let host_port = binding
        .get("HostPort")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .trim()
        .parse::<u16>()
        .ok();
    Ok(host_port.filter(|port| *port > 0))
}

fn docker_container_missing_error(err: &anyhow::Error) -> bool {
    let lower = err.to_string().to_ascii_lowercase();
    lower.contains("no such object") || lower.contains("no such container")
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
    let instance = store
        .get_instance(instance_id)
        .await?
        .ok_or_else(|| anyhow!("instance {} not found", instance_id))?;
    let updated = merge_runtime_config(instance.config_json, volumes).context("runtime config")?;
    if let Some(updated) = updated {
        store
            .update_instance_config(instance_id, Some(&updated))
            .await?;
    }
    Ok(())
}

async fn prepare_nzbget_runtime_dirs(volumes: &[VolumeMount]) -> Result<()> {
    let downloads_root = volumes
        .iter()
        .find(|volume| {
            volume.container_path == "/downloads"
                && volume.source_kind == VolumeMountSourceKind::Bind
        })
        .map(|volume| volume.host_path.clone());
    let config_root = volumes
        .iter()
        .find(|volume| {
            volume.container_path == "/config" && volume.source_kind == VolumeMountSourceKind::Bind
        })
        .map(|volume| volume.host_path.clone());

    if let Some(downloads_root) = downloads_root {
        for relative in [
            "",
            ".incomplete",
            ".nzb",
            ".queue",
            ".tmp",
            "movies",
            "tv",
            "anime",
        ] {
            let path = if relative.is_empty() {
                Path::new(&downloads_root).to_path_buf()
            } else {
                Path::new(&downloads_root).join(relative)
            };
            fs::create_dir_all(&path)
                .await
                .with_context(|| format!("creating nzbget runtime directory {}", path.display()))?;
        }
    }

    if let Some(config_root) = config_root {
        for relative in ["", "scripts"] {
            let path = if relative.is_empty() {
                Path::new(&config_root).to_path_buf()
            } else {
                Path::new(&config_root).join(relative)
            };
            fs::create_dir_all(&path)
                .await
                .with_context(|| format!("creating nzbget config directory {}", path.display()))?;
        }
    }

    Ok(())
}

async fn persist_managed_defaults_profile_version(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    existing: Option<serde_json::Value>,
    key: &str,
    version: &str,
) -> Result<()> {
    let updated = merge_managed_defaults_profile(existing, key, version)?;
    store
        .update_instance_config(instance_id, Some(&updated))
        .await?;
    Ok(())
}

fn merge_runtime_config(
    existing: Option<serde_json::Value>,
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

    let config_mount = volumes
        .iter()
        .find(|volume| volume.container_path == "/config");
    let config_dir = config_mount
        .filter(|volume| volume.source_kind == VolumeMountSourceKind::Bind)
        .map(|volume| volume.host_path.clone());
    match config_dir {
        Some(config_dir) => {
            if runtime
                .get("config_dir")
                .and_then(serde_json::Value::as_str)
                != Some(config_dir.as_str())
            {
                runtime.insert(
                    "config_dir".to_string(),
                    serde_json::Value::String(config_dir),
                );
                changed = true;
            }
        }
        None => {
            if runtime.remove("config_dir").is_some() {
                changed = true;
            }
        }
    }

    if let Some(config_mount) = config_mount {
        let config_storage = serde_json::json!({
            "source_kind": config_mount.source_kind,
            "source": config_mount.host_path,
            "container_path": config_mount.container_path,
        });
        if runtime.get("config_storage") != Some(&config_storage) {
            runtime.insert("config_storage".to_string(), config_storage);
            changed = true;
        }
    }

    let volume_values = volumes
        .iter()
        .map(|volume| {
            serde_json::json!({
                "source_kind": volume.source_kind,
                "host_path": volume.host_path,
                "container_path": volume.container_path,
                "read_only": volume.read_only,
            })
        })
        .collect::<Vec<_>>();
    let volume_value = serde_json::Value::Array(volume_values);
    if runtime.get("volumes") != Some(&volume_value) {
        runtime.insert("volumes".to_string(), volume_value);
        changed = true;
    }

    if changed {
        Ok(Some(serde_json::Value::Object(root)))
    } else {
        Ok(None)
    }
}

fn merge_managed_defaults_profile(
    existing: Option<serde_json::Value>,
    key: &str,
    version: &str,
) -> Result<serde_json::Value> {
    let mut root = match existing {
        Some(serde_json::Value::Object(map)) => map,
        Some(_) => bail!("instance config is not an object"),
        None => serde_json::Map::new(),
    };

    let defaults_entry = root
        .entry("managed_defaults".to_string())
        .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
    let defaults = defaults_entry
        .as_object_mut()
        .ok_or_else(|| anyhow!("managed_defaults config is not an object"))?;
    defaults.insert(
        key.to_string(),
        serde_json::Value::String(version.to_string()),
    );
    Ok(serde_json::Value::Object(root))
}

fn should_restart_nzbget_after_patch(
    provider: &Provider,
    instance_config: Option<&serde_json::Value>,
    patch: &DriverPatch,
) -> bool {
    if provider.capability != "downloader.nzb"
        || provider.implementation.as_deref() != Some("nzbget")
        || !runtime_has_bind_config_dir(instance_config)
    {
        return false;
    }

    matches!(
        patch,
        DriverPatch::DownloaderNzb(DownloaderNzbPatch::SetCategories { .. })
            | DriverPatch::DownloaderNzb(DownloaderNzbPatch::SetPreferences { .. })
    )
}

fn should_apply_nzbget_named_volume_patch(
    provider: &Provider,
    instance_config: Option<&serde_json::Value>,
    patch: &DriverPatch,
) -> bool {
    if provider.capability != "downloader.nzb"
        || provider.implementation.as_deref() != Some("nzbget")
        || !runtime_uses_named_config_storage(instance_config)
    {
        return false;
    }

    matches!(
        patch,
        DriverPatch::DownloaderNzb(DownloaderNzbPatch::SetCategories { .. })
            | DriverPatch::DownloaderNzb(DownloaderNzbPatch::SetPreferences { .. })
    )
}

fn runtime_has_bind_config_dir(instance_config: Option<&serde_json::Value>) -> bool {
    instance_config
        .and_then(|value| value.get("runtime"))
        .and_then(|value| value.get("config_dir"))
        .and_then(serde_json::Value::as_str)
        .is_some_and(|value| !value.trim().is_empty())
}

fn runtime_uses_named_config_storage(instance_config: Option<&serde_json::Value>) -> bool {
    instance_config
        .and_then(|value| value.get("runtime"))
        .and_then(|value| value.get("config_storage"))
        .and_then(|value| value.get("source_kind"))
        .and_then(serde_json::Value::as_str)
        .is_some_and(|value| value.eq_ignore_ascii_case("named_volume"))
}

async fn nzbget_managed_paths_are_current(
    runtime: &dyn RuntimeManager,
    instance_id: Uuid,
    instance_config: Option<&serde_json::Value>,
) -> Result<bool> {
    let host_config_dir = instance_config
        .and_then(|value| value.get("runtime"))
        .and_then(|value| value.get("config_dir"))
        .and_then(serde_json::Value::as_str);
    let Some(contents) = read_config_text(
        runtime,
        instance_id,
        host_config_dir,
        "nzbget.conf",
        "/config/nzbget.conf",
    )
    .await?
    else {
        return Ok(false);
    };

    Ok(missing_nzbget_managed_paths(&contents).is_empty())
}

fn missing_nzbget_managed_paths(contents: &str) -> Vec<&'static str> {
    NZBGET_REQUIRED_MANAGED_PATHS
        .iter()
        .filter_map(
            |(key, expected)| match find_nzbget_config_value(contents, key) {
                Some(value) if value == *expected => None,
                _ => Some(*key),
            },
        )
        .collect()
}

fn find_nzbget_config_value<'a>(contents: &'a str, key: &str) -> Option<&'a str> {
    contents.lines().find_map(|line| {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            return None;
        }
        let (name, value) = trimmed.split_once('=')?;
        (name.trim() == key).then_some(value.trim())
    })
}

async fn read_config_text(
    runtime: &dyn RuntimeManager,
    instance_id: Uuid,
    host_config_dir: Option<&str>,
    relative_name: &str,
    container_path: &str,
) -> Result<Option<String>> {
    if let Some(config_dir) = host_config_dir.filter(|value| !value.trim().is_empty()) {
        let path = Path::new(config_dir).join(relative_name);
        let content = match fs::read_to_string(&path).await {
            Ok(content) => Some(content),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
            Err(err) => return Err(err).with_context(|| format!("reading {}", path.display())),
        };
        if content.is_some() {
            return Ok(content);
        }
    }

    let Some(handle) = runtime
        .get_container_handle(&container_name(instance_id))
        .await?
    else {
        return Ok(None);
    };
    let Some(bytes) = runtime.read_container_file(&handle, container_path).await? else {
        return Ok(None);
    };
    let text = String::from_utf8(bytes)
        .with_context(|| format!("decoding container config file {}", container_path))?;
    Ok(Some(text))
}

fn qbittorrent_incomplete_path() -> &'static str {
    QBITTORRENT_INCOMPLETE_DIR
}

fn nzbget_incomplete_path() -> &'static str {
    NZBGET_INCOMPLETE_DIR
}

fn nzbget_nzb_dir() -> &'static str {
    NZBGET_NZB_DIR
}

fn nzbget_queue_dir() -> &'static str {
    NZBGET_QUEUE_DIR
}

fn nzbget_temp_dir() -> &'static str {
    NZBGET_TEMP_DIR
}

fn qbittorrent_performance_profile_patch(profile: DownloaderPerformanceProfile) -> DriverPatch {
    let (
        max_connections,
        max_connections_per_torrent,
        max_upload_slots,
        max_upload_slots_per_torrent,
        disk_cache_mb,
        disk_cache_ttl_seconds,
        max_active_downloads,
        max_active_torrents,
        max_active_uploads,
    ) = match profile {
        DownloaderPerformanceProfile::Balanced => (500, 100, 20, 8, 512, 60, 50, 100, 20),
        DownloaderPerformanceProfile::Aggressive => (800, 150, 30, 10, 768, 120, 80, 160, 30),
    };
    DriverPatch::DownloaderTorrent(DownloaderTorrentPatch::SetPreferences {
        default_save_path: Some("/downloads".to_string()),
        incomplete_path: Some(qbittorrent_incomplete_path().to_string()),
        use_incomplete: Some(true),
        max_connections: Some(max_connections),
        max_connections_per_torrent: Some(max_connections_per_torrent),
        max_upload_slots: Some(max_upload_slots),
        max_upload_slots_per_torrent: Some(max_upload_slots_per_torrent),
        disk_cache_mb: Some(disk_cache_mb),
        disk_cache_ttl_seconds: Some(disk_cache_ttl_seconds),
        queueing_enabled: Some(false),
        max_active_downloads: Some(max_active_downloads),
        max_active_torrents: Some(max_active_torrents),
        max_active_uploads: Some(max_active_uploads),
        random_port: Some(false),
        listen_port: Some(51413),
        upnp: Some(false),
        preallocate_all: Some(false),
    })
}

fn nzbget_performance_profile_patch(profile: DownloaderPerformanceProfile) -> DriverPatch {
    let (article_retries, article_cache_mb, write_buffer_kb, par_threads) = match profile {
        DownloaderPerformanceProfile::Balanced => {
            (3, 200, 1024, recommended_nzbget_par_threads(false))
        }
        DownloaderPerformanceProfile::Aggressive => {
            (4, 384, 2048, recommended_nzbget_par_threads(true))
        }
    };
    DriverPatch::DownloaderNzb(DownloaderNzbPatch::SetPreferences {
        main_dir: Some(NZBGET_MAIN_DIR.to_string()),
        default_save_path: Some(DOWNLOADS_ROOT.to_string()),
        incomplete_path: Some(nzbget_incomplete_path().to_string()),
        nzb_dir: Some(nzbget_nzb_dir().to_string()),
        queue_dir: Some(nzbget_queue_dir().to_string()),
        temp_dir: Some(nzbget_temp_dir().to_string()),
        script_dir: Some(NZBGET_SCRIPT_DIR.to_string()),
        log_file: Some(NZBGET_LOG_FILE.to_string()),
        web_dir: Some(NZBGET_WEB_DIR.to_string()),
        config_template: Some(NZBGET_CONFIG_TEMPLATE.to_string()),
        use_incomplete: Some(true),
        server_connections: None,
        article_retries: Some(article_retries),
        article_timeout_seconds: Some(60),
        article_cache_mb: Some(article_cache_mb),
        direct_write: Some(true),
        write_buffer_kb: Some(write_buffer_kb),
        continue_partial: Some(true),
        par_check: Some("auto".to_string()),
        par_scan: Some("auto".to_string()),
        par_quick: Some(true),
        par_repair: Some(true),
        par_rename: Some(true),
        par_pause_queue: Some(true),
        par_threads: Some(par_threads),
        unpack: Some(true),
        unpack_pause_queue: Some(true),
        download_rate_kib: Some(0),
    })
}

fn recommended_nzbget_par_threads(aggressive: bool) -> u64 {
    std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(if aggressive { 4 } else { 2 })
        .clamp(
            if aggressive { 3 } else { 2 },
            if aggressive { 6 } else { 4 },
        ) as u64
}

fn qbittorrent_performance_profile_version(profile: DownloaderPerformanceProfile) -> &'static str {
    match profile {
        DownloaderPerformanceProfile::Balanced => BALANCED_PERFORMANCE_PROFILE_VERSION,
        DownloaderPerformanceProfile::Aggressive => AGGRESSIVE_PERFORMANCE_PROFILE_VERSION,
    }
}

fn nzbget_performance_profile_version(profile: DownloaderPerformanceProfile) -> &'static str {
    match profile {
        DownloaderPerformanceProfile::Balanced => NZBGET_BALANCED_PERFORMANCE_PROFILE_VERSION,
        DownloaderPerformanceProfile::Aggressive => NZBGET_AGGRESSIVE_PERFORMANCE_PROFILE_VERSION,
    }
}

fn qbittorrent_profile_version_matches(
    current: Option<&str>,
    selected: DownloaderPerformanceProfile,
) -> bool {
    matches_profile_version(current, selected)
}

fn nzbget_profile_version_matches(
    current: Option<&str>,
    selected: DownloaderPerformanceProfile,
) -> bool {
    match selected {
        DownloaderPerformanceProfile::Balanced => {
            matches!(current, Some(NZBGET_BALANCED_PERFORMANCE_PROFILE_VERSION))
        }
        DownloaderPerformanceProfile::Aggressive => {
            matches!(current, Some(NZBGET_AGGRESSIVE_PERFORMANCE_PROFILE_VERSION))
        }
    }
}

fn matches_profile_version(current: Option<&str>, selected: DownloaderPerformanceProfile) -> bool {
    match selected {
        DownloaderPerformanceProfile::Balanced => {
            matches!(current, Some(BALANCED_PERFORMANCE_PROFILE_VERSION))
        }
        DownloaderPerformanceProfile::Aggressive => {
            matches!(current, Some(AGGRESSIVE_PERFORMANCE_PROFILE_VERSION))
        }
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
    runtime: &ManifestRuntime,
) -> Result<()> {
    let required = required_secrets_from_runtime(runtime)?;
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

async fn resolve_secret_value(
    store: &ExtensionStore<'_>,
    secrets: &SecretsManager,
    instance_id: Uuid,
    from_secret: &str,
) -> Result<String> {
    let reference = parse_secret_reference(from_secret, instance_id)?;
    let secret = store
        .get_secret(reference.scope, reference.scope_id, &reference.key)
        .await?
        .ok_or_else(|| anyhow!("secret '{}' not found", reference.key))?;
    secrets
        .decrypt(&secret.value_encrypted)
        .with_context(|| format!("decrypting secret '{}'", reference.key))
}

fn render_openvpn_config(config: &str, has_auth_file: bool) -> String {
    if !has_auth_file {
        return config.to_string();
    }

    let mut replaced_auth = false;
    let mut rendered = Vec::new();
    for line in config.lines() {
        if line.trim_start().starts_with("auth-user-pass") {
            rendered.push("auth-user-pass /gluetun/auth.txt".to_string());
            replaced_auth = true;
        } else {
            rendered.push(line.to_string());
        }
    }
    if !replaced_auth {
        rendered.push("auth-user-pass /gluetun/auth.txt".to_string());
    }
    rendered.join("\n") + "\n"
}

async fn set_private_file_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let permissions = std::fs::Permissions::from_mode(0o600);
        fs::set_permissions(path, permissions).await?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
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
                resolve_secret_value(store, secrets, instance_id, &from_secret)
                    .await
                    .with_context(|| format!("resolving runtime.env '{name}'"))?
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

pub(crate) fn resolve_runtime_volume_mounts(
    extension_id: &str,
    instance_id: Uuid,
    raw_volumes: &[String],
    paths: &RuntimePaths,
) -> Result<Vec<VolumeMount>> {
    Ok(prepare_runtime_volumes(extension_id, instance_id, raw_volumes, paths)?.volumes)
}

fn prepare_runtime_volumes(
    extension_id: &str,
    instance_id: Uuid,
    raw_volumes: &[String],
    paths: &RuntimePaths,
) -> Result<PreparedRuntimeVolumes> {
    let mut volumes = raw_volumes
        .iter()
        .map(|volume| resolve_volume_mount(volume, paths))
        .collect::<Result<Vec<_>>>()?;

    if extension_uses_managed_config_volume(extension_id) {
        if let Some(config_mount) = volumes
            .iter_mut()
            .find(|volume| volume.container_path == "/config")
        {
            config_mount.source_kind = VolumeMountSourceKind::NamedVolume;
            config_mount.host_path = config_volume_name(instance_id);
        } else {
            volumes.push(VolumeMount {
                source_kind: VolumeMountSourceKind::NamedVolume,
                host_path: config_volume_name(instance_id),
                container_path: "/config".to_string(),
                read_only: false,
            });
        }
    }

    if extension_requires_runtime_volume(extension_id)
        && !volumes
            .iter()
            .any(|volume| volume.container_path == "/runtime")
    {
        volumes.push(VolumeMount {
            source_kind: VolumeMountSourceKind::NamedVolume,
            host_path: runtime_volume_name(instance_id),
            container_path: "/runtime".to_string(),
            read_only: false,
        });
    }

    Ok(PreparedRuntimeVolumes { volumes })
}

pub(crate) fn normalized_network_mode(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

pub(crate) fn downloader_network_mode_requires_rehome_with_gateway_identity(
    current: Option<&str>,
    desired: Option<&str>,
    desired_gateway_id: Option<&str>,
) -> bool {
    if current == desired {
        return false;
    }

    if let Some(desired_gateway_name) = container_network_namespace_target(desired) {
        if current.is_some_and(|mode| {
            container_network_namespace_matches(mode, desired_gateway_name, desired_gateway_id)
        }) {
            return false;
        }
    }

    is_explicit_container_network_namespace(current)
        || is_explicit_container_network_namespace(desired)
        || desired.is_some()
}

pub(crate) fn container_network_namespace_target(value: Option<&str>) -> Option<&str> {
    value
        .map(str::trim)
        .and_then(|mode| mode.strip_prefix("container:"))
        .map(str::trim)
        .filter(|target| !target.is_empty())
}

pub(crate) fn container_network_namespace_matches(
    actual_network_mode: &str,
    expected_gateway_name: &str,
    expected_gateway_id: Option<&str>,
) -> bool {
    let Some(actual_container) = container_network_namespace_target(Some(actual_network_mode))
    else {
        return false;
    };
    if actual_container == expected_gateway_name {
        return true;
    }
    if let Some(expected_gateway_id) = expected_gateway_id {
        actual_container == expected_gateway_id
            || actual_container.starts_with(expected_gateway_id)
            || expected_gateway_id.starts_with(actual_container)
    } else {
        false
    }
}

fn is_explicit_container_network_namespace(value: Option<&str>) -> bool {
    value.is_some_and(|mode| mode.trim().starts_with("container:"))
}

pub(crate) fn volume_mounts_from_runtime_state(state: &ContainerRuntimeState) -> Vec<VolumeMount> {
    state
        .mounts
        .iter()
        .filter_map(volume_mount_from_runtime_mount)
        .collect()
}

fn volume_mount_from_runtime_mount(mount: &ContainerRuntimeMount) -> Option<VolumeMount> {
    let source_kind = match mount.mount_type.as_str() {
        "bind" => VolumeMountSourceKind::Bind,
        "volume" => VolumeMountSourceKind::NamedVolume,
        _ => return None,
    };
    let host_path = match source_kind {
        VolumeMountSourceKind::Bind => mount.source.clone(),
        VolumeMountSourceKind::NamedVolume => mount.name.clone().or_else(|| mount.source.clone()),
    }?;

    Some(VolumeMount {
        source_kind,
        host_path,
        container_path: mount.destination.clone(),
        read_only: mount.read_only,
    })
}

fn persisted_runtime_volumes(
    instance_config: Option<&serde_json::Value>,
) -> Result<Option<Vec<VolumeMount>>> {
    let Some(volumes) = instance_config
        .and_then(|config| config.get("runtime"))
        .and_then(|runtime| runtime.get("volumes"))
    else {
        return Ok(None);
    };

    let parsed = serde_json::from_value(volumes.clone())
        .context("parsing persisted runtime volume metadata")?;
    Ok(Some(parsed))
}

fn keep_downloader_paused_after_rehome(instance_config: Option<&serde_json::Value>) -> bool {
    let Some(config) = instance_config else {
        return false;
    };
    config
        .get("network")
        .and_then(|network| network.get("keep_downloads_paused_after_rehome"))
        .or_else(|| {
            config
                .get("runtime")
                .and_then(|runtime| runtime.get("keep_downloads_paused_after_rehome"))
        })
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

// Downloader containers may need to be recreated to change image, labels, ports,
// or network namespace. Their durable state must remain on the same config,
// runtime, and downloads mounts so credentials, queues, fastresume files, and
// paused transfers survive that recreation.
pub(crate) fn validate_downloader_volume_preservation(
    extension_id: &str,
    existing: &[VolumeMount],
    next: &[VolumeMount],
) -> Result<()> {
    for container_path in downloader_preserved_mounts(extension_id) {
        let existing_mount = existing
            .iter()
            .find(|volume| volume.container_path == *container_path)
            .ok_or_else(|| {
                anyhow!(
                    "downloader migration preflight failed: existing {} mount is missing",
                    container_path
                )
            })?;
        let next_mount = next
            .iter()
            .find(|volume| volume.container_path == *container_path)
            .ok_or_else(|| {
                anyhow!(
                    "downloader migration preflight failed: desired {} mount is missing",
                    container_path
                )
            })?;

        if !volume_mount_identity_matches(existing_mount, next_mount) {
            bail!(
                "downloader migration preflight failed: {} mount would change from {} to {}",
                container_path,
                describe_volume_mount_identity(existing_mount),
                describe_volume_mount_identity(next_mount)
            );
        }
    }

    Ok(())
}

#[allow(dead_code)]
pub(crate) fn downloader_preserved_mounts(extension_id: &str) -> &'static [&'static str] {
    if is_qbittorrent_extension_id(extension_id) {
        &["/config", DOWNLOADS_ROOT]
    } else if is_nzbget_extension_id(extension_id) {
        &["/config", "/runtime", DOWNLOADS_ROOT]
    } else {
        &[]
    }
}

#[allow(dead_code)]
fn volume_mount_identity_matches(left: &VolumeMount, right: &VolumeMount) -> bool {
    left.source_kind == right.source_kind
        && left.container_path == right.container_path
        && left.read_only == right.read_only
        && match left.source_kind {
            VolumeMountSourceKind::Bind => {
                bind_mount_source_identity_matches(&left.host_path, &right.host_path)
            }
            VolumeMountSourceKind::NamedVolume => left.host_path == right.host_path,
        }
}

fn bind_mount_source_identity_matches(left: &str, right: &str) -> bool {
    let left_sources = normalized_bind_mount_sources(left);
    let right_sources = normalized_bind_mount_sources(right);
    !left_sources.is_empty()
        && !right_sources.is_empty()
        && left_sources
            .iter()
            .any(|candidate| right_sources.contains(candidate))
}

fn normalized_bind_mount_sources(path: &str) -> HashSet<String> {
    let mut candidates = HashSet::new();
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return candidates;
    }

    candidates.insert(trimmed.to_string());

    if let Some(stripped) = trimmed.strip_prefix("/host_mnt") {
        let normalized = if stripped.is_empty() { "/" } else { stripped };
        candidates.insert(normalized.to_string());
    }

    if !trimmed.starts_with("/host_mnt") && trimmed.starts_with('/') {
        candidates.insert(format!("/host_mnt{trimmed}"));
    }

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

    let current = candidates.iter().cloned().collect::<Vec<_>>();
    for candidate in current {
        if let Ok(canonical) = std::fs::canonicalize(&candidate) {
            candidates.insert(canonical.to_string_lossy().to_string());
        }
    }

    candidates
}

#[allow(dead_code)]
pub(crate) fn describe_volume_mount_identity(volume: &VolumeMount) -> String {
    format!(
        "{:?}:{}:{}{}",
        volume.source_kind,
        volume.host_path,
        volume.container_path,
        if volume.read_only { ":ro" } else { "" }
    )
}

fn extension_uses_managed_config_volume(extension_id: &str) -> bool {
    matches!(
        extension_id,
        "elixir.modules.sonarr"
            | "elixir.modules.radarr"
            | "elixir.modules.prowlarr"
            | "elixir.modules.bazarr"
            | "elixir.modules.nzbget"
            | "elixir.modules.qbittorrent"
    )
}

fn extension_requires_runtime_volume(extension_id: &str) -> bool {
    is_nzbget_extension_id(extension_id)
}

fn config_volume_name(instance_id: Uuid) -> String {
    format!("elixir_cfg_{}", instance_id.simple())
}

fn runtime_volume_name(instance_id: Uuid) -> String {
    format!("elixir_rt_{}", instance_id.simple())
}

fn required_named_runtime_directories(extension_id: &str, volumes: &[VolumeMount]) -> Vec<String> {
    let mut directories = Vec::new();
    let has_named_config = volumes.iter().any(|volume| {
        volume.container_path == "/config"
            && volume.source_kind == VolumeMountSourceKind::NamedVolume
    });
    let has_named_runtime = volumes.iter().any(|volume| {
        volume.container_path == "/runtime"
            && volume.source_kind == VolumeMountSourceKind::NamedVolume
    });
    let has_downloads = volumes
        .iter()
        .any(|volume| volume.container_path == DOWNLOADS_ROOT);

    if is_nzbget_extension_id(extension_id) {
        if has_named_config {
            directories.push(NZBGET_SCRIPT_DIR.to_string());
        }
        if has_named_runtime {
            directories.extend([
                NZBGET_INCOMPLETE_DIR.to_string(),
                NZBGET_NZB_DIR.to_string(),
                NZBGET_QUEUE_DIR.to_string(),
                NZBGET_TEMP_DIR.to_string(),
            ]);
        }
    }

    if is_qbittorrent_extension_id(extension_id) && has_downloads {
        directories.push(QBITTORRENT_INCOMPLETE_DIR.to_string());
    }

    directories
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
        source_kind: VolumeMountSourceKind::Bind,
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

    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use serde_json::json;
    use tempfile::TempDir;

    use crate::config::DatabaseConfig;
    use crate::db::Database;
    use crate::db::models::{
        ExtensionKind, ExtensionTrustLevel, ProviderHealthState, SlotCardinality,
    };
    use crate::download_broker::{TORRENT_DEFAULT_LOGICAL_ID, list_logical_downloaders};
    use crate::extensions::managed_paths::NZBGET_LOCK_FILE;
    use crate::extensions::manifest::ManifestRuntimePort;
    use crate::extensions::store::{
        ExtensionStore, NewExtension, NewExtensionInstance, NewProvider,
    };
    use crate::orchestrator::naming::container_name;
    use crate::runtime::model::{
        ContainerHandle, ContainerRuntimeMount, ContainerRuntimeState, ContainerSpec,
        ContainerState,
    };
    use crate::secrets::SecretsManager;

    #[test]
    fn detects_invalid_arr_none_plus_enabled_auth_state() {
        assert!(arr_host_auth_config_requires_normalization(&json!({
            "authenticationMethod": "none",
            "authenticationRequired": "enabled"
        })));
        assert!(arr_host_auth_config_requires_normalization(&json!({
            "authenticationMethod": "none",
            "authenticationRequired": "disabledForLocalAddresses"
        })));
        assert!(!arr_host_auth_config_requires_normalization(&json!({
            "authenticationMethod": "forms",
            "authenticationRequired": "disabledForLocalAddresses"
        })));
    }

    #[test]
    fn parse_docker_published_port_reads_bound_tcp_port() -> Result<()> {
        let ports_json = r#"{"6789/tcp":[{"HostIp":"0.0.0.0","HostPort":"32932"}]}"#;
        let host_port = parse_docker_published_port(ports_json, 6789)?;
        assert_eq!(host_port, Some(32932));
        Ok(())
    }

    #[test]
    fn host_probe_target_from_base_url_uses_resolved_transport_host_and_port() -> Result<()> {
        let target = host_probe_target_from_base_url("http://127.0.0.1:33042/base/")?;
        assert_eq!(target.base_url, "http://127.0.0.1:33042/base/");
        assert_eq!(target.host, "host.docker.internal");
        assert_eq!(target.port, 33042);
        Ok(())
    }

    #[test]
    fn probe_target_from_endpoint_preserves_container_network_service_host() -> Result<()> {
        let endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "svc-sonarr".to_string(),
            8989,
            None,
            Some("elixir_net".to_string()),
        )?;
        let target = probe_target_from_endpoint(&endpoint)?;
        assert_eq!(target.base_url, "http://svc-sonarr:8989/");
        assert_eq!(target.host, "svc-sonarr");
        assert_eq!(target.port, 8989);
        Ok(())
    }

    #[tokio::test]
    async fn nzbget_managed_paths_report_missing_when_required_keys_absent() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let config_path = temp_dir.path().join("nzbget.conf");
        fs::write(
            &config_path,
            "MainDir=/config\nDestDir=/downloads\n# WebDir intentionally missing\n",
        )
        .await?;

        let config = json!({
            "runtime": {
                "config_dir": temp_dir.path().to_string_lossy().to_string()
            }
        });

        assert!(
            !nzbget_managed_paths_are_current(&StubRuntime, Uuid::new_v4(), Some(&config)).await?
        );
        Ok(())
    }

    #[tokio::test]
    async fn nzbget_managed_paths_accept_expected_values() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let config_path = temp_dir.path().join("nzbget.conf");
        fs::write(
            &config_path,
            format!(
                "MainDir={}\nDestDir={}\nInterDir={}\nNzbDir={}\nQueueDir={}\nTempDir={}\nScriptDir={}\nLogFile={}\nWebDir={}\nConfigTemplate={}\nLockFile={}\n",
                NZBGET_MAIN_DIR,
                DOWNLOADS_ROOT,
                NZBGET_INCOMPLETE_DIR,
                NZBGET_NZB_DIR,
                NZBGET_QUEUE_DIR,
                NZBGET_TEMP_DIR,
                NZBGET_SCRIPT_DIR,
                NZBGET_LOG_FILE,
                NZBGET_WEB_DIR,
                NZBGET_CONFIG_TEMPLATE,
                NZBGET_LOCK_FILE
            ),
        )
        .await?;

        let config = json!({
            "runtime": {
                "config_dir": temp_dir.path().to_string_lossy().to_string()
            }
        });

        assert!(
            nzbget_managed_paths_are_current(&StubRuntime, Uuid::new_v4(), Some(&config)).await?
        );
        Ok(())
    }

    struct RecordingDriver {
        capability: &'static str,
        calls: Arc<Mutex<Vec<crate::drivers::DriverPatch>>>,
        semantics: crate::drivers::PatchSemantics,
        evaluation: crate::drivers::DriftEvaluation,
        apply_result: crate::drivers::ApplyResult,
    }

    impl RecordingDriver {
        fn new(
            capability: &'static str,
            calls: Arc<Mutex<Vec<crate::drivers::DriverPatch>>>,
        ) -> Self {
            Self {
                capability,
                calls,
                semantics: crate::drivers::PatchSemantics::periodic_safe(
                    crate::drivers::PatchSideEffect::LiveApiWrite,
                ),
                evaluation: crate::drivers::DriftEvaluation::unknown(
                    "recording driver does not model drift",
                ),
                apply_result: crate::drivers::ApplyResult::applied(),
            }
        }

        fn with_semantics(mut self, semantics: crate::drivers::PatchSemantics) -> Self {
            self.semantics = semantics;
            self
        }

        fn with_evaluation(mut self, evaluation: crate::drivers::DriftEvaluation) -> Self {
            self.evaluation = evaluation;
            self
        }

        fn with_apply_result(mut self, apply_result: crate::drivers::ApplyResult) -> Self {
            self.apply_result = apply_result;
            self
        }
    }

    #[async_trait]
    impl crate::drivers::CapabilityDriver for RecordingDriver {
        fn capability(&self) -> &'static str {
            self.capability
        }

        async fn read_state(&self, _ctx: DriverCtx) -> Result<crate::drivers::StateSnapshot> {
            Ok(crate::drivers::StateSnapshot {
                summary: None,
                activity: None,
            })
        }

        fn patch_semantics(
            &self,
            _patch: &crate::drivers::DriverPatch,
        ) -> crate::drivers::PatchSemantics {
            self.semantics
        }

        async fn evaluate_patch(
            &self,
            _ctx: DriverCtx,
            _patch: crate::drivers::DriverPatch,
        ) -> Result<crate::drivers::DriftEvaluation> {
            Ok(self.evaluation.clone())
        }

        async fn apply_patch(
            &self,
            _ctx: DriverCtx,
            patch: crate::drivers::DriverPatch,
        ) -> Result<crate::drivers::ApplyResult> {
            self.calls
                .lock()
                .expect("recording driver lock")
                .push(patch);
            Ok(self.apply_result.clone())
        }
    }

    #[derive(Default)]
    struct StubProbe {
        calls: Mutex<Vec<String>>,
        fail_dns: HashSet<String>,
        fail_tcp: HashSet<String>,
    }

    impl StubProbe {
        fn calls(&self) -> Vec<String> {
            self.calls.lock().expect("probe calls lock").clone()
        }

        fn fail_dns_for(mut self, host: &str) -> Self {
            self.fail_dns.insert(host.to_string());
            self
        }

        fn fail_tcp_for(mut self, host: &str, port: u16) -> Self {
            self.fail_tcp.insert(format!("{host}:{port}"));
            self
        }
    }

    #[async_trait]
    impl ProbeRunner for StubProbe {
        async fn probe_dns(&self, name: &str) -> Result<crate::runtime::probe::ProbeResult> {
            if self.fail_dns.contains(name) {
                bail!("dns unavailable for {name}");
            }
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
            if self.fail_tcp.contains(&format!("{host}:{port}")) {
                bail!("tcp unavailable for {host}:{port}");
            }
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

        async fn read_container_file(
            &self,
            _handle: &ContainerHandle,
            _path: &str,
        ) -> Result<Option<Vec<u8>>> {
            Ok(None)
        }

        async fn copy_host_path_to_container(
            &self,
            _handle: &ContainerHandle,
            _source_path: &std::path::Path,
            _destination_path: &str,
        ) -> Result<()> {
            bail!("unexpected runtime call")
        }

        async fn ensure_container_directories(
            &self,
            _handle: &ContainerHandle,
            _paths: &[String],
        ) -> Result<()> {
            bail!("unexpected runtime call")
        }

        async fn ensure_container_directories_owned_like(
            &self,
            _handle: &ContainerHandle,
            _reference_path: &str,
            _paths: &[String],
        ) -> Result<bool> {
            Ok(false)
        }
    }

    async fn start_mock_qbittorrent_auth_server() -> Result<(String, u16)> {
        async fn login() -> impl axum::response::IntoResponse {
            (
                [(
                    axum::http::header::SET_COOKIE,
                    axum::http::HeaderValue::from_static("SID=test; HttpOnly"),
                )],
                "Ok.",
            )
        }

        let app = axum::Router::new().route("/api/v2/auth/login", axum::routing::post(login));
        let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await?;
        let port = listener.local_addr()?.port();
        let host = match local_ip_address::local_ip()? {
            std::net::IpAddr::V4(ip) if !ip.is_loopback() => ip.to_string(),
            ip => bail!("expected non-loopback IPv4 address for qBittorrent test server, got {ip}"),
        };
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("mock qbittorrent auth server");
        });
        Ok((host, port))
    }

    async fn start_mock_qbittorrent_rehome_server() -> Result<(String, u16, Arc<Mutex<Vec<String>>>)>
    {
        type Calls = Arc<Mutex<Vec<String>>>;

        async fn login(
            axum::extract::State(calls): axum::extract::State<Calls>,
        ) -> impl axum::response::IntoResponse {
            calls
                .lock()
                .expect("qbittorrent rehome mock calls lock")
                .push("login".to_string());
            (
                [(
                    axum::http::header::SET_COOKIE,
                    axum::http::HeaderValue::from_static("SID=test; HttpOnly"),
                )],
                "Ok.",
            )
        }

        async fn transfer_info(
            axum::extract::State(calls): axum::extract::State<Calls>,
        ) -> impl axum::response::IntoResponse {
            calls
                .lock()
                .expect("qbittorrent rehome mock calls lock")
                .push("transfer_info".to_string());
            axum::Json(json!({
                "connection_status": "connected",
                "dl_info_speed": 0,
                "up_info_speed": 0,
                "dl_info_data": 0,
                "up_info_data": 0
            }))
        }

        async fn torrents_info(
            axum::extract::State(calls): axum::extract::State<Calls>,
        ) -> impl axum::response::IntoResponse {
            calls
                .lock()
                .expect("qbittorrent rehome mock calls lock")
                .push("torrents_info".to_string());
            axum::Json(json!([
                { "hash": "activehash", "state": "downloading" },
                { "hash": "pausedhash", "state": "pausedDL" }
            ]))
        }

        async fn pause_all(
            axum::extract::State(calls): axum::extract::State<Calls>,
        ) -> impl axum::response::IntoResponse {
            calls
                .lock()
                .expect("qbittorrent rehome mock calls lock")
                .push("pause_all".to_string());
            axum::http::StatusCode::OK
        }

        async fn resume(
            axum::extract::State(calls): axum::extract::State<Calls>,
            axum::extract::Form(fields): axum::extract::Form<HashMap<String, String>>,
        ) -> impl axum::response::IntoResponse {
            let hashes = fields.get("hashes").cloned().unwrap_or_default();
            calls
                .lock()
                .expect("qbittorrent rehome mock calls lock")
                .push(format!("resume:{hashes}"));
            axum::http::StatusCode::OK
        }

        let calls = Arc::new(Mutex::new(Vec::new()));
        let app = axum::Router::new()
            .route("/api/v2/auth/login", axum::routing::post(login))
            .route("/api/v2/transfer/info", axum::routing::get(transfer_info))
            .route("/api/v2/torrents/info", axum::routing::get(torrents_info))
            .route("/api/v2/transfer/pauseAll", axum::routing::post(pause_all))
            .route("/api/v2/torrents/resume", axum::routing::post(resume))
            .with_state(calls.clone());
        let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await?;
        let port = listener.local_addr()?.port();
        let host = match local_ip_address::local_ip()? {
            std::net::IpAddr::V4(ip) if !ip.is_loopback() => ip.to_string(),
            ip => bail!("expected non-loopback IPv4 address for qBittorrent test server, got {ip}"),
        };
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("mock qbittorrent rehome server");
        });
        Ok((host, port, calls))
    }

    #[derive(Default)]
    struct CaptureRuntime {
        specs: Mutex<Vec<ContainerSpec>>,
        networks: Mutex<Vec<String>>,
        runtime_states: Mutex<HashMap<String, ContainerRuntimeState>>,
        renames: Mutex<Vec<(String, String)>>,
        removals: Mutex<Vec<String>>,
    }

    impl CaptureRuntime {
        fn set_runtime_state(&self, state: ContainerRuntimeState) {
            self.runtime_states
                .lock()
                .expect("capture runtime lock")
                .insert(state.name.clone(), state);
        }

        fn last_spec(&self) -> Option<ContainerSpec> {
            self.specs
                .lock()
                .expect("capture runtime lock")
                .last()
                .cloned()
        }

        fn specs(&self) -> Vec<ContainerSpec> {
            self.specs.lock().expect("capture runtime lock").clone()
        }

        fn networks(&self) -> Vec<String> {
            self.networks.lock().expect("capture runtime lock").clone()
        }

        fn renames(&self) -> Vec<(String, String)> {
            self.renames.lock().expect("capture runtime lock").clone()
        }

        fn removals(&self) -> Vec<String> {
            self.removals.lock().expect("capture runtime lock").clone()
        }
    }

    fn runtime_state_from_container_spec(
        spec: &ContainerSpec,
        network_mode: Option<String>,
    ) -> ContainerRuntimeState {
        runtime_state_from_volumes(&spec.name, network_mode, &spec.labels, &spec.volumes)
    }

    fn runtime_state_from_volumes(
        name: &str,
        network_mode: Option<String>,
        labels: &HashMap<String, String>,
        volumes: &[VolumeMount],
    ) -> ContainerRuntimeState {
        ContainerRuntimeState {
            name: name.to_string(),
            network_mode,
            labels: labels.clone(),
            mounts: volumes
                .iter()
                .map(|volume| ContainerRuntimeMount {
                    mount_type: match volume.source_kind {
                        VolumeMountSourceKind::Bind => "bind".to_string(),
                        VolumeMountSourceKind::NamedVolume => "volume".to_string(),
                    },
                    source: Some(volume.host_path.clone()),
                    name: (volume.source_kind == VolumeMountSourceKind::NamedVolume)
                        .then(|| volume.host_path.clone()),
                    destination: volume.container_path.clone(),
                    read_only: volume.read_only,
                })
                .collect(),
            published_ports: Vec::new(),
        }
    }

    #[async_trait]
    impl RuntimeManager for CaptureRuntime {
        async fn ensure_network(&self, name: &str) -> Result<()> {
            self.networks
                .lock()
                .expect("capture runtime lock")
                .push(name.to_string());
            Ok(())
        }

        async fn ensure_container(&self, spec: &ContainerSpec) -> Result<ContainerHandle> {
            self.specs
                .lock()
                .expect("capture runtime lock")
                .push(spec.clone());
            self.runtime_states
                .lock()
                .expect("capture runtime lock")
                .insert(
                    spec.name.clone(),
                    runtime_state_from_container_spec(spec, spec.network_mode.clone()),
                );
            Ok(ContainerHandle {
                id: "capture".to_string(),
                name: spec.name.clone(),
            })
        }

        async fn get_container_handle(&self, name: &str) -> Result<Option<ContainerHandle>> {
            if self
                .runtime_states
                .lock()
                .expect("capture runtime lock")
                .contains_key(name)
            {
                Ok(Some(ContainerHandle {
                    id: name.to_string(),
                    name: name.to_string(),
                }))
            } else {
                Ok(None)
            }
        }

        async fn start_container(&self, _handle: &ContainerHandle) -> Result<()> {
            Ok(())
        }

        async fn stop_container(&self, _handle: &ContainerHandle) -> Result<()> {
            Ok(())
        }

        async fn rename_container(
            &self,
            handle: &ContainerHandle,
            new_name: &str,
        ) -> Result<ContainerHandle> {
            let mut states = self.runtime_states.lock().expect("capture runtime lock");
            if let Some(mut state) = states.remove(&handle.name) {
                state.name = new_name.to_string();
                states.insert(new_name.to_string(), state);
            }
            drop(states);
            self.renames
                .lock()
                .expect("capture runtime lock")
                .push((handle.name.clone(), new_name.to_string()));
            Ok(ContainerHandle {
                id: new_name.to_string(),
                name: new_name.to_string(),
            })
        }

        async fn remove_container(&self, handle: &ContainerHandle) -> Result<()> {
            self.runtime_states
                .lock()
                .expect("capture runtime lock")
                .remove(&handle.name);
            self.removals
                .lock()
                .expect("capture runtime lock")
                .push(handle.name.clone());
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

        async fn describe_container_runtime_state(
            &self,
            container_name: &str,
        ) -> Result<Option<ContainerRuntimeState>> {
            Ok(self
                .runtime_states
                .lock()
                .expect("capture runtime lock")
                .get(container_name)
                .cloned())
        }

        async fn read_container_file(
            &self,
            _handle: &ContainerHandle,
            _path: &str,
        ) -> Result<Option<Vec<u8>>> {
            Ok(None)
        }

        async fn copy_host_path_to_container(
            &self,
            _handle: &ContainerHandle,
            _source_path: &std::path::Path,
            _destination_path: &str,
        ) -> Result<()> {
            bail!("unexpected runtime call")
        }

        async fn ensure_container_directories(
            &self,
            _handle: &ContainerHandle,
            _paths: &[String],
        ) -> Result<()> {
            bail!("unexpected runtime call")
        }

        async fn ensure_container_directories_owned_like(
            &self,
            _handle: &ContainerHandle,
            _reference_path: &str,
            _paths: &[String],
        ) -> Result<bool> {
            Ok(false)
        }
    }

    #[derive(Default)]
    struct AssetInstallRuntime {
        files: Mutex<HashMap<String, Vec<u8>>>,
        copied_destinations: Mutex<Vec<String>>,
        ensured_directories: Mutex<Vec<Vec<String>>>,
        stop_count: Mutex<usize>,
        start_count: Mutex<usize>,
    }

    impl AssetInstallRuntime {
        fn copied_destinations(&self) -> Vec<String> {
            self.copied_destinations
                .lock()
                .expect("asset runtime lock")
                .clone()
        }

        fn ensured_directories(&self) -> Vec<Vec<String>> {
            self.ensured_directories
                .lock()
                .expect("asset runtime lock")
                .clone()
        }

        fn file_contents(&self, path: &str) -> Option<Vec<u8>> {
            self.files
                .lock()
                .expect("asset runtime lock")
                .get(path)
                .cloned()
        }

        fn stop_count(&self) -> usize {
            *self.stop_count.lock().expect("asset runtime lock")
        }

        fn start_count(&self) -> usize {
            *self.start_count.lock().expect("asset runtime lock")
        }
    }

    #[async_trait]
    impl RuntimeManager for AssetInstallRuntime {
        async fn ensure_network(&self, _name: &str) -> Result<()> {
            bail!("unexpected runtime call")
        }

        async fn ensure_container(&self, _spec: &ContainerSpec) -> Result<ContainerHandle> {
            bail!("unexpected runtime call")
        }

        async fn get_container_handle(&self, name: &str) -> Result<Option<ContainerHandle>> {
            Ok(Some(ContainerHandle {
                id: "asset".to_string(),
                name: name.to_string(),
            }))
        }

        async fn start_container(&self, _handle: &ContainerHandle) -> Result<()> {
            *self.start_count.lock().expect("asset runtime lock") += 1;
            Ok(())
        }

        async fn stop_container(&self, _handle: &ContainerHandle) -> Result<()> {
            *self.stop_count.lock().expect("asset runtime lock") += 1;
            Ok(())
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

        async fn read_container_file(
            &self,
            _handle: &ContainerHandle,
            path: &str,
        ) -> Result<Option<Vec<u8>>> {
            Ok(self
                .files
                .lock()
                .expect("asset runtime lock")
                .get(path)
                .cloned())
        }

        async fn copy_host_path_to_container(
            &self,
            _handle: &ContainerHandle,
            source_path: &std::path::Path,
            destination_path: &str,
        ) -> Result<()> {
            let bytes = std::fs::read(source_path)
                .with_context(|| format!("reading test asset {}", source_path.display()))?;
            self.files
                .lock()
                .expect("asset runtime lock")
                .insert(destination_path.to_string(), bytes);
            self.copied_destinations
                .lock()
                .expect("asset runtime lock")
                .push(destination_path.to_string());
            Ok(())
        }

        async fn ensure_container_directories(
            &self,
            _handle: &ContainerHandle,
            paths: &[String],
        ) -> Result<()> {
            self.ensured_directories
                .lock()
                .expect("asset runtime lock")
                .push(paths.to_vec());
            Ok(())
        }

        async fn ensure_container_directories_owned_like(
            &self,
            _handle: &ContainerHandle,
            _reference_path: &str,
            _paths: &[String],
        ) -> Result<bool> {
            bail!("unexpected runtime call")
        }
    }

    struct NzbgetNamedVolumeRuntime {
        config_text: Mutex<String>,
        start_count: Mutex<usize>,
        stop_count: Mutex<usize>,
        copy_count: Mutex<usize>,
    }

    impl NzbgetNamedVolumeRuntime {
        fn new(initial_config: &str) -> Self {
            Self {
                config_text: Mutex::new(initial_config.to_string()),
                start_count: Mutex::new(0),
                stop_count: Mutex::new(0),
                copy_count: Mutex::new(0),
            }
        }

        fn config_text(&self) -> String {
            self.config_text
                .lock()
                .expect("nzbget runtime config lock")
                .clone()
        }

        fn start_count(&self) -> usize {
            *self
                .start_count
                .lock()
                .expect("nzbget runtime start count lock")
        }

        fn stop_count(&self) -> usize {
            *self
                .stop_count
                .lock()
                .expect("nzbget runtime stop count lock")
        }

        fn copy_count(&self) -> usize {
            *self
                .copy_count
                .lock()
                .expect("nzbget runtime copy count lock")
        }
    }

    #[async_trait]
    impl RuntimeManager for NzbgetNamedVolumeRuntime {
        async fn ensure_network(&self, _name: &str) -> Result<()> {
            bail!("unexpected runtime call")
        }

        async fn ensure_container(&self, _spec: &ContainerSpec) -> Result<ContainerHandle> {
            bail!("unexpected runtime call")
        }

        async fn get_container_handle(&self, name: &str) -> Result<Option<ContainerHandle>> {
            Ok(Some(ContainerHandle {
                id: name.to_string(),
                name: name.to_string(),
            }))
        }

        async fn start_container(&self, _handle: &ContainerHandle) -> Result<()> {
            *self
                .start_count
                .lock()
                .expect("nzbget runtime start count lock") += 1;
            Ok(())
        }

        async fn stop_container(&self, _handle: &ContainerHandle) -> Result<()> {
            *self
                .stop_count
                .lock()
                .expect("nzbget runtime stop count lock") += 1;
            Ok(())
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

        async fn read_container_file(
            &self,
            _handle: &ContainerHandle,
            path: &str,
        ) -> Result<Option<Vec<u8>>> {
            if path != "/config/nzbget.conf" {
                bail!("unexpected container file read {path}");
            }
            Ok(Some(self.config_text().into_bytes()))
        }

        async fn copy_host_path_to_container(
            &self,
            _handle: &ContainerHandle,
            source_path: &std::path::Path,
            destination_path: &str,
        ) -> Result<()> {
            if destination_path != "/config/nzbget.conf" {
                bail!("unexpected container copy destination {destination_path}");
            }
            *self
                .copy_count
                .lock()
                .expect("nzbget runtime copy count lock") += 1;
            let text = fs::read_to_string(source_path).await?;
            *self.config_text.lock().expect("nzbget runtime config lock") = text;
            Ok(())
        }

        async fn ensure_container_directories(
            &self,
            _handle: &ContainerHandle,
            _paths: &[String],
        ) -> Result<()> {
            bail!("unexpected runtime call")
        }

        async fn ensure_container_directories_owned_like(
            &self,
            _handle: &ContainerHandle,
            _reference_path: &str,
            _paths: &[String],
        ) -> Result<bool> {
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

        async fn read_container_file(
            &self,
            _handle: &ContainerHandle,
            _path: &str,
        ) -> Result<Option<Vec<u8>>> {
            bail!("unexpected runtime call")
        }

        async fn copy_host_path_to_container(
            &self,
            _handle: &ContainerHandle,
            _source_path: &std::path::Path,
            _destination_path: &str,
        ) -> Result<()> {
            bail!("unexpected runtime call")
        }

        async fn ensure_container_directories(
            &self,
            _handle: &ContainerHandle,
            _paths: &[String],
        ) -> Result<()> {
            bail!("unexpected runtime call")
        }

        async fn ensure_container_directories_owned_like(
            &self,
            _handle: &ContainerHandle,
            _reference_path: &str,
            _paths: &[String],
        ) -> Result<bool> {
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

        async fn read_container_file(
            &self,
            _handle: &ContainerHandle,
            _path: &str,
        ) -> Result<Option<Vec<u8>>> {
            bail!("unexpected runtime call")
        }

        async fn copy_host_path_to_container(
            &self,
            _handle: &ContainerHandle,
            _source_path: &std::path::Path,
            _destination_path: &str,
        ) -> Result<()> {
            bail!("unexpected runtime call")
        }

        async fn ensure_container_directories(
            &self,
            _handle: &ContainerHandle,
            _paths: &[String],
        ) -> Result<()> {
            bail!("unexpected runtime call")
        }

        async fn ensure_container_directories_owned_like(
            &self,
            _handle: &ContainerHandle,
            _reference_path: &str,
            _paths: &[String],
        ) -> Result<bool> {
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
                endpoint_json: Some(serde_json::to_value(&endpoint)?),
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
            egress: None,
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
            egress: None,
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
            egress: None,
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
    async fn wireguard_egress_creates_gateway_and_container_namespace_pair() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_extension(&store, "ext.wg", json!({})).await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "ext.wg".to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;

        let secrets = SecretsManager::from_key_bytes([4u8; 32], true);
        let encrypted =
            secrets.encrypt("[Interface]\nPrivateKey = test\nAddress = 10.64.0.2/32\n")?;
        store
            .upsert_secret(&NewSecret {
                secret_id: Uuid::new_v4(),
                scope: SecretScope::Instance,
                scope_id: Some(instance_id),
                key: "wg_config".to_string(),
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
        )
        .with_wireguard_gateway_image("example/wireguard-gateway:1");

        let runtime_spec = ManifestRuntime {
            r#type: "container".to_string(),
            image: Some("example/downloader:latest".to_string()),
            network: None,
            service_name: Some("elx-downloader".to_string()),
            ports: vec![ManifestRuntimePort {
                container: 8080,
                host: None,
            }],
            volumes: Vec::new(),
            env: Vec::new(),
            egress: Some(ManifestRuntimeEgress {
                mode: "wireguard".to_string(),
                strict: true,
                wireguard_config_secret: Some("instance:wg_config".to_string()),
                wireguard_gateway_image: None,
            }),
        };

        executor
            .ensure_runtime_running(
                instance_id,
                "ext.wg".to_string(),
                "default".to_string(),
                runtime_spec,
                None,
                vec!["svc-test".to_string()],
            )
            .await?;

        let specs = runtime.specs();
        assert_eq!(specs.len(), 2, "expected gateway + app container specs");
        let gateway = &specs[0];
        let app = &specs[1];
        assert_eq!(gateway.image, "example/wireguard-gateway:1");
        assert!(gateway.aliases.iter().any(|alias| alias == "svc-test"));
        assert!(
            gateway
                .aliases
                .iter()
                .any(|alias| alias == "elx-downloader"),
            "service alias should be attached to gateway container"
        );
        assert_eq!(gateway.cap_add, vec!["NET_ADMIN".to_string()]);
        assert!(
            gateway
                .devices
                .iter()
                .any(|d| d == "/dev/net/tun:/dev/net/tun")
        );
        assert_eq!(gateway.network_mode, None);
        assert_eq!(gateway.ports.len(), 1);

        let gateway_name = gateway.name.clone();
        assert_eq!(
            app.network_mode.as_deref(),
            Some(format!("container:{gateway_name}").as_str())
        );
        assert!(app.aliases.is_empty(), "app aliases belong on gateway");
        assert!(app.ports.is_empty(), "app ports belong on gateway");

        let wg_path = gateway
            .volumes
            .first()
            .expect("gateway config mount")
            .host_path
            .clone();
        assert!(
            Path::new(&wg_path).exists(),
            "wireguard config file should exist"
        );
        let content = fs::read_to_string(&wg_path).await?;
        assert!(content.contains("PrivateKey = test"));

        let networks = runtime.networks();
        assert_eq!(networks, vec!["elixir_net".to_string()]);
        Ok(())
    }

    #[tokio::test]
    async fn wireguard_egress_strict_false_falls_back_to_direct_when_secret_missing() -> Result<()>
    {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_extension(&store, "ext.wg", json!({})).await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "ext.wg".to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;

        let secrets = SecretsManager::from_key_bytes([6u8; 32], true);
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
            image: Some("example/downloader:latest".to_string()),
            network: None,
            service_name: None,
            ports: vec![ManifestRuntimePort {
                container: 8080,
                host: None,
            }],
            volumes: Vec::new(),
            env: Vec::new(),
            egress: Some(ManifestRuntimeEgress {
                mode: "wireguard".to_string(),
                strict: false,
                wireguard_config_secret: Some("instance:wg_config".to_string()),
                wireguard_gateway_image: None,
            }),
        };

        executor
            .ensure_runtime_running(
                instance_id,
                "ext.wg".to_string(),
                "default".to_string(),
                runtime_spec,
                None,
                vec!["svc-test".to_string()],
            )
            .await?;

        let specs = runtime.specs();
        assert_eq!(
            specs.len(),
            1,
            "strict=false should fall back to direct runtime"
        );
        assert_eq!(specs[0].network_mode, None);
        assert!(!specs[0].aliases.is_empty());
        assert_eq!(specs[0].ports.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn wireguard_egress_strict_true_fails_when_secret_missing() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_extension(&store, "ext.wg", json!({})).await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "ext.wg".to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;

        let secrets = SecretsManager::from_key_bytes([8u8; 32], true);
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
            image: Some("example/downloader:latest".to_string()),
            network: None,
            service_name: None,
            ports: vec![ManifestRuntimePort {
                container: 8080,
                host: None,
            }],
            volumes: Vec::new(),
            env: Vec::new(),
            egress: Some(ManifestRuntimeEgress {
                mode: "wireguard".to_string(),
                strict: true,
                wireguard_config_secret: Some("instance:wg_config".to_string()),
                wireguard_gateway_image: None,
            }),
        };

        let err = executor
            .ensure_runtime_running(
                instance_id,
                "ext.wg".to_string(),
                "default".to_string(),
                runtime_spec,
                None,
                vec!["svc-test".to_string()],
            )
            .await
            .unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("missing required secrets") || message.contains("secret"),
            "unexpected error: {message}"
        );
        assert!(runtime.specs().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn qbittorrent_uses_default_wireguard_secret_when_runtime_egress_not_declared()
    -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_extension(&store, "elixir.modules.qbittorrent", json!({})).await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "elixir.modules.qbittorrent".to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;

        let secrets = SecretsManager::from_key_bytes([9u8; 32], true);
        let encrypted =
            secrets.encrypt("[Interface]\nPrivateKey = test\nAddress = 10.64.0.2/32\n")?;
        store
            .upsert_secret(&NewSecret {
                secret_id: Uuid::new_v4(),
                scope: SecretScope::Global,
                scope_id: None,
                key: "wireguard_config".to_string(),
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
        )
        .with_default_wireguard_config_secret(Some("global:wireguard_config".to_string()));

        let runtime_spec = ManifestRuntime {
            r#type: "container".to_string(),
            image: Some("example/qbittorrent:latest".to_string()),
            network: None,
            service_name: Some("elx-qbittorrent".to_string()),
            ports: vec![ManifestRuntimePort {
                container: 8080,
                host: None,
            }],
            volumes: Vec::new(),
            env: Vec::new(),
            egress: None,
        };

        executor
            .ensure_runtime_running(
                instance_id,
                "elixir.modules.qbittorrent".to_string(),
                "default".to_string(),
                runtime_spec,
                None,
                vec!["svc-qbit".to_string()],
            )
            .await?;

        let specs = runtime.specs();
        assert_eq!(
            specs.len(),
            2,
            "expected default wireguard wrapping for qbittorrent"
        );
        assert!(specs[1].network_mode.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn downloader_rehome_preserves_qbittorrent_mounts_when_enabling_wireguard() -> Result<()>
    {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_extension(&store, "elixir.modules.qbittorrent", json!({})).await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "elixir.modules.qbittorrent".to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;
        store
            .update_instance_runtime_version(instance_id, "1.0.0", None)
            .await?;

        let secrets = SecretsManager::from_key_bytes([1u8; 32], true);
        let encrypted =
            secrets.encrypt("[Interface]\nPrivateKey = test\nAddress = 10.64.0.2/32\n")?;
        store
            .upsert_secret(&NewSecret {
                secret_id: Uuid::new_v4(),
                scope: SecretScope::Global,
                scope_id: None,
                key: "wireguard_config".to_string(),
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
        let prepared_volumes = prepare_runtime_volumes(
            "elixir.modules.qbittorrent",
            instance_id,
            &["{downloads}:/downloads".to_string()],
            &runtime_paths,
        )?
        .volumes;
        let app_name = container_name(instance_id);
        runtime.set_runtime_state(runtime_state_from_volumes(
            &app_name,
            Some("elixir_net".to_string()),
            &HashMap::new(),
            &prepared_volumes,
        ));

        let executor = Executor::new(
            &database.pool,
            &probe,
            &drivers,
            &runtime,
            runtime_paths,
            &secrets,
        )
        .with_default_wireguard_config_secret(Some("global:wireguard_config".to_string()));

        let runtime_spec = ManifestRuntime {
            r#type: "container".to_string(),
            image: Some("example/qbittorrent:latest".to_string()),
            network: None,
            service_name: Some("elx-qbittorrent".to_string()),
            ports: vec![ManifestRuntimePort {
                container: 8080,
                host: None,
            }],
            volumes: vec!["{downloads}:/downloads".to_string()],
            env: Vec::new(),
            egress: None,
        };

        executor
            .ensure_runtime_running(
                instance_id,
                "elixir.modules.qbittorrent".to_string(),
                "default".to_string(),
                runtime_spec,
                None,
                vec!["svc-qbit".to_string()],
            )
            .await?;

        let specs = runtime.specs();
        assert_eq!(specs.len(), 2, "expected gateway + rehomed app specs");
        let app = specs
            .iter()
            .find(|spec| spec.name == app_name)
            .expect("app spec");
        assert!(
            app.network_mode
                .as_deref()
                .is_some_and(|mode| { mode.starts_with("container:") })
        );
        validate_downloader_volume_preservation(
            "elixir.modules.qbittorrent",
            &prepared_volumes,
            &app.volumes,
        )?;

        let rollback_name = format!("{app_name}-network-rollback");
        assert!(
            runtime
                .renames()
                .iter()
                .any(|(from, to)| from == &app_name && to == &rollback_name),
            "expected existing downloader container to be renamed before rehome"
        );
        assert!(
            runtime.removals().iter().any(|name| name == &rollback_name),
            "successful rehome should remove the stopped rollback container"
        );
        Ok(())
    }

    #[tokio::test]
    async fn downloader_rehome_to_direct_removes_stale_gateway_after_app_moves_off_namespace()
    -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_extension(&store, "elixir.modules.qbittorrent", json!({})).await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "elixir.modules.qbittorrent".to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;
        store
            .update_instance_runtime_version(instance_id, "1.0.0", None)
            .await?;

        let secrets = SecretsManager::from_key_bytes([33u8; 32], true);
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
        let prepared_volumes = prepare_runtime_volumes(
            "elixir.modules.qbittorrent",
            instance_id,
            &["{downloads}:/downloads".to_string()],
            &runtime_paths,
        )?
        .volumes;
        let app_name = container_name(instance_id);
        let gateway_name = format!("{app_name}-vpn");
        runtime.set_runtime_state(runtime_state_from_volumes(
            &app_name,
            Some(format!("container:{gateway_name}")),
            &HashMap::new(),
            &prepared_volumes,
        ));
        runtime.set_runtime_state(runtime_state_from_volumes(
            &gateway_name,
            None,
            &HashMap::new(),
            &[],
        ));

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
            image: Some("example/qbittorrent:latest".to_string()),
            network: None,
            service_name: Some("elx-qbittorrent".to_string()),
            ports: vec![ManifestRuntimePort {
                container: 8080,
                host: None,
            }],
            volumes: vec!["{downloads}:/downloads".to_string()],
            env: Vec::new(),
            egress: None,
        };

        executor
            .ensure_runtime_running(
                instance_id,
                "elixir.modules.qbittorrent".to_string(),
                "default".to_string(),
                runtime_spec,
                None,
                vec!["svc-qbit".to_string()],
            )
            .await?;

        assert!(
            runtime.removals().iter().any(|name| name == &gateway_name),
            "stale gateway should be removed after direct rehome"
        );
        let app_state = runtime
            .describe_container_runtime_state(&app_name)
            .await?
            .expect("app runtime state");
        assert_eq!(app_state.network_mode, None);
        assert!(runtime.get_container_handle(&gateway_name).await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn downloader_spec_fingerprint_mismatch_uses_rehome_rollback_path() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_extension(&store, "elixir.modules.qbittorrent", json!({})).await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "elixir.modules.qbittorrent".to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;
        store
            .update_instance_runtime_version(instance_id, "1.0.0", None)
            .await?;

        let secrets = SecretsManager::from_key_bytes([46u8; 32], true);
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
        let prepared_volumes = prepare_runtime_volumes(
            "elixir.modules.qbittorrent",
            instance_id,
            &["{downloads}:/downloads".to_string()],
            &runtime_paths,
        )?
        .volumes;
        let app_name = container_name(instance_id);
        let rollback_name = format!("{app_name}-network-rollback");
        let mut labels = HashMap::new();
        labels.insert(
            crate::runtime::model::CONTAINER_SPEC_HASH_LABEL.to_string(),
            "old-spec-hash".to_string(),
        );
        runtime.set_runtime_state(runtime_state_from_volumes(
            &app_name,
            None,
            &labels,
            &prepared_volumes,
        ));

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
            image: Some("example/qbittorrent:latest".to_string()),
            network: None,
            service_name: Some("elx-qbittorrent".to_string()),
            ports: vec![ManifestRuntimePort {
                container: 8080,
                host: None,
            }],
            volumes: vec!["{downloads}:/downloads".to_string()],
            env: Vec::new(),
            egress: None,
        };

        executor
            .ensure_runtime_running(
                instance_id,
                "elixir.modules.qbittorrent".to_string(),
                "default".to_string(),
                runtime_spec,
                None,
                vec!["svc-qbit".to_string()],
            )
            .await?;

        assert!(
            runtime
                .renames()
                .iter()
                .any(|(from, to)| from == &app_name && to == &rollback_name),
            "spec fingerprint drift should use the rollback rehome path"
        );
        assert!(
            runtime.removals().iter().any(|name| name == &rollback_name),
            "successful rehome should remove the rollback backup"
        );
        Ok(())
    }

    #[tokio::test]
    async fn downloader_rehome_pauses_and_resumes_only_active_qbittorrent_torrents() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_extension(&store, "elixir.modules.qbittorrent", json!({})).await?;
        let (host, port, calls) = start_mock_qbittorrent_rehome_server().await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "elixir.modules.qbittorrent".to_string(),
                instance_name: "default".to_string(),
                config_json: Some(json!({
                    "username": "admin",
                    "password": "adminadmin"
                })),
                enabled: true,
            })
            .await?;
        store
            .update_instance_runtime_version(instance_id, "1.0.0", None)
            .await?;

        let endpoint = ProviderEndpoint::new("http".to_string(), host, port, None, None)?;
        let provider_id = Uuid::new_v4();
        store
            .upsert_provider(&NewProvider {
                provider_id,
                instance_id,
                capability: "downloader.torrent".to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: Some("qbittorrent".to_string()),
                scope_json: None,
                endpoint_json: Some(serde_json::to_value(endpoint)?),
                health_state: ProviderHealthState::Unknown,
            })
            .await?;

        let secrets = SecretsManager::from_key_bytes([6u8; 32], true);
        let encrypted =
            secrets.encrypt("[Interface]\nPrivateKey = test\nAddress = 10.64.0.2/32\n")?;
        store
            .upsert_secret(&NewSecret {
                secret_id: Uuid::new_v4(),
                scope: SecretScope::Global,
                scope_id: None,
                key: "wireguard_config".to_string(),
                value_encrypted: encrypted,
                rotatable: false,
            })
            .await?;

        let probe = StubProbe::default();
        let drivers = DriverRegistry::with_defaults();
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
        let prepared_volumes = prepare_runtime_volumes(
            "elixir.modules.qbittorrent",
            instance_id,
            &["{downloads}:/downloads".to_string()],
            &runtime_paths,
        )?
        .volumes;
        let app_name = container_name(instance_id);
        runtime.set_runtime_state(runtime_state_from_volumes(
            &app_name,
            Some("elixir_net".to_string()),
            &HashMap::new(),
            &prepared_volumes,
        ));

        let executor = Executor::new(
            &database.pool,
            &probe,
            &drivers,
            &runtime,
            runtime_paths,
            &secrets,
        )
        .with_default_wireguard_config_secret(Some("global:wireguard_config".to_string()));

        let runtime_spec = ManifestRuntime {
            r#type: "container".to_string(),
            image: Some("example/qbittorrent:latest".to_string()),
            network: None,
            service_name: Some("elx-qbittorrent".to_string()),
            ports: vec![ManifestRuntimePort {
                container: 8080,
                host: None,
            }],
            volumes: vec!["{downloads}:/downloads".to_string()],
            env: Vec::new(),
            egress: None,
        };

        executor
            .ensure_runtime_running(
                instance_id,
                "elixir.modules.qbittorrent".to_string(),
                "default".to_string(),
                runtime_spec,
                None,
                vec!["svc-qbit".to_string()],
            )
            .await?;

        let inventory = list_logical_downloaders(&store).await?;
        let broker_record = inventory
            .downloaders
            .iter()
            .find(|record| {
                record.logical_id == TORRENT_DEFAULT_LOGICAL_ID && record.provider_id == provider_id
            })
            .expect("managed qBittorrent broker record after rehome");
        assert_eq!(broker_record.instance_id, instance_id);
        assert_eq!(
            broker_record.endpoints.progress_path,
            "/api/v1/download-broker/downloaders.torrent.default/progress"
        );

        let calls = calls
            .lock()
            .expect("qbittorrent rehome mock calls lock")
            .clone();
        let pause_index = calls
            .iter()
            .position(|call| call == "pause_all")
            .expect("pause_all call");
        let resume_index = calls
            .iter()
            .position(|call| call.starts_with("resume:"))
            .expect("resume call");
        assert!(
            pause_index < resume_index,
            "downloads must be paused before resume; calls={calls:?}"
        );
        let verify_index = calls
            .iter()
            .rposition(|call| call == "transfer_info")
            .expect("post-rehome driver API verification call");
        assert!(
            pause_index < verify_index && verify_index < resume_index,
            "downloader API must be reachable after rehome before resume; calls={calls:?}"
        );
        assert_eq!(calls[resume_index], "resume:activehash");
        assert!(
            !calls[resume_index].contains("pausedhash"),
            "pre-paused torrents must remain paused; calls={calls:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn downloader_rehome_rejects_qbittorrent_config_volume_change() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_extension(&store, "elixir.modules.qbittorrent", json!({})).await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "elixir.modules.qbittorrent".to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;
        store
            .update_instance_runtime_version(instance_id, "1.0.0", None)
            .await?;

        let secrets = SecretsManager::from_key_bytes([2u8; 32], true);
        let encrypted =
            secrets.encrypt("[Interface]\nPrivateKey = test\nAddress = 10.64.0.2/32\n")?;
        store
            .upsert_secret(&NewSecret {
                secret_id: Uuid::new_v4(),
                scope: SecretScope::Global,
                scope_id: None,
                key: "wireguard_config".to_string(),
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
        let mut existing_volumes = prepare_runtime_volumes(
            "elixir.modules.qbittorrent",
            instance_id,
            &["{downloads}:/downloads".to_string()],
            &runtime_paths,
        )?
        .volumes;
        existing_volumes
            .iter_mut()
            .find(|volume| volume.container_path == "/config")
            .expect("config mount")
            .host_path = "elixir_cfg_recreated".to_string();
        let app_name = container_name(instance_id);
        runtime.set_runtime_state(runtime_state_from_volumes(
            &app_name,
            Some("elixir_net".to_string()),
            &HashMap::new(),
            &existing_volumes,
        ));

        let executor = Executor::new(
            &database.pool,
            &probe,
            &drivers,
            &runtime,
            runtime_paths,
            &secrets,
        )
        .with_default_wireguard_config_secret(Some("global:wireguard_config".to_string()));

        let runtime_spec = ManifestRuntime {
            r#type: "container".to_string(),
            image: Some("example/qbittorrent:latest".to_string()),
            network: None,
            service_name: Some("elx-qbittorrent".to_string()),
            ports: vec![ManifestRuntimePort {
                container: 8080,
                host: None,
            }],
            volumes: vec!["{downloads}:/downloads".to_string()],
            env: Vec::new(),
            egress: None,
        };

        let err = executor
            .ensure_runtime_running(
                instance_id,
                "elixir.modules.qbittorrent".to_string(),
                "default".to_string(),
                runtime_spec,
                None,
                vec!["svc-qbit".to_string()],
            )
            .await
            .unwrap_err();
        let message = err.root_cause().to_string();
        assert!(
            message.contains("/config mount would change"),
            "unexpected error: {message}"
        );
        assert!(
            runtime.specs().iter().all(|spec| spec.name != app_name),
            "unsafe rehome must not recreate the downloader app container"
        );
        assert!(
            runtime.renames().is_empty(),
            "unsafe rehome must not rename the current downloader container"
        );
        Ok(())
    }

    #[tokio::test]
    async fn explicit_direct_egress_bypasses_default_wireguard_wrapping() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_extension(&store, "elixir.modules.qbittorrent", json!({})).await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "elixir.modules.qbittorrent".to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;

        let secrets = SecretsManager::from_key_bytes([5u8; 32], true);
        let encrypted =
            secrets.encrypt("[Interface]\nPrivateKey = test\nAddress = 10.64.0.2/32\n")?;
        store
            .upsert_secret(&NewSecret {
                secret_id: Uuid::new_v4(),
                scope: SecretScope::Global,
                scope_id: None,
                key: "wireguard_config".to_string(),
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
        )
        .with_default_wireguard_config_secret(Some("global:wireguard_config".to_string()));

        let runtime_spec = ManifestRuntime {
            r#type: "container".to_string(),
            image: Some("example/qbittorrent:latest".to_string()),
            network: None,
            service_name: Some("elx-qbittorrent".to_string()),
            ports: vec![ManifestRuntimePort {
                container: 8080,
                host: None,
            }],
            volumes: Vec::new(),
            env: Vec::new(),
            egress: Some(ManifestRuntimeEgress {
                mode: "direct".to_string(),
                strict: true,
                wireguard_config_secret: None,
                wireguard_gateway_image: None,
            }),
        };

        executor
            .ensure_runtime_running(
                instance_id,
                "elixir.modules.qbittorrent".to_string(),
                "default".to_string(),
                runtime_spec,
                None,
                vec!["svc-qbit".to_string()],
            )
            .await?;

        let specs = runtime.specs();
        assert_eq!(specs.len(), 1, "explicit direct should not create gateway");
        assert_eq!(specs[0].network_mode, None);
        assert!(specs[0].aliases.iter().any(|alias| alias == "svc-qbit"));
        assert_eq!(specs[0].ports.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn active_warp_profile_wraps_managed_qbittorrent_without_user_vpn_config() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_extension(&store, "elixir.modules.qbittorrent", json!({})).await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "elixir.modules.qbittorrent".to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO download_network_profiles (id, name, kind, enabled, strict, scope, provider, gateway_runtime, config_json, status, active) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind("cloudflare-warp")
        .bind("Cloudflare WARP")
        .bind("cloudflare_warp")
        .bind(true)
        .bind(true)
        .bind("managed_downloaders")
        .bind("cloudflare")
        .bind("warp_gateway")
        .bind(serde_json::to_string(&json!({
            "cloudflareWarp": {
                "gatewayImage": "example/warp-gateway:1",
                "sharedCredentials": false
            }
        }))?)
        .bind("unknown")
        .bind(true)
        .execute(&database.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO download_warp_enrollments (id, profile_id, enrollment_id, identity_secret_ref, status, disclosure_version) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind("cloudflare-warp")
        .bind("enrollment-1")
        .bind("global:cloudflare_warp_identity")
        .bind("pending_runtime")
        .bind("2026-04-29")
        .execute(&database.pool)
        .await?;

        let secrets = SecretsManager::from_key_bytes([5u8; 32], true);
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
            image: Some("example/qbittorrent:latest".to_string()),
            network: None,
            service_name: Some("elx-qbittorrent".to_string()),
            ports: vec![ManifestRuntimePort {
                container: 8080,
                host: None,
            }],
            volumes: Vec::new(),
            env: Vec::new(),
            egress: None,
        };

        executor
            .ensure_runtime_running(
                instance_id,
                "elixir.modules.qbittorrent".to_string(),
                "default".to_string(),
                runtime_spec,
                None,
                vec!["svc-qbit".to_string()],
            )
            .await?;

        let specs = runtime.specs();
        assert_eq!(specs.len(), 2, "expected WARP gateway + app spec");
        let gateway = specs
            .iter()
            .find(|spec| spec.name.ends_with("-vpn"))
            .expect("warp gateway spec");
        assert_eq!(gateway.image, "example/warp-gateway:1");
        assert_eq!(
            gateway
                .labels
                .get("elixir.network_role")
                .map(String::as_str),
            Some("warp_gateway")
        );
        assert!(
            gateway
                .env
                .iter()
                .any(|env| { env.name == "WARP_ENABLE_NAT" && env.value == "1" })
        );
        assert!(gateway.env.iter().all(|env| env.name != "WARP_LICENSE_KEY"));
        assert_eq!(
            gateway
                .volumes
                .iter()
                .find(|volume| volume.container_path == "/var/lib/cloudflare-warp")
                .map(|volume| (&volume.source_kind, volume.host_path.as_str())),
            Some((
                &VolumeMountSourceKind::NamedVolume,
                "elixir_warp_state_cloudflare_warp"
            ))
        );

        let app = specs
            .iter()
            .find(|spec| spec.name == container_name(instance_id))
            .expect("qbittorrent app spec");
        assert_eq!(
            app.network_mode.as_deref(),
            Some(format!("container:{}", gateway.name).as_str())
        );
        assert!(app.aliases.is_empty());
        assert!(app.ports.is_empty());

        let status: String = sqlx::query_scalar::<sqlx::Any, String>(
            "SELECT status FROM download_warp_enrollments WHERE profile_id = ?",
        )
        .bind("cloudflare-warp")
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(status, "ready");
        Ok(())
    }

    #[tokio::test]
    async fn nzbget_credentials_are_auto_generated_for_runtime_env() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_extension(&store, "elixir.modules.nzbget", json!({})).await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "elixir.modules.nzbget".to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;

        let secrets = SecretsManager::from_key_bytes([8u8; 32], true);
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
            image: Some("example/nzbget:latest".to_string()),
            network: None,
            service_name: Some("elx-nzbget".to_string()),
            ports: vec![ManifestRuntimePort {
                container: 6789,
                host: None,
            }],
            volumes: Vec::new(),
            env: vec![
                ManifestRuntimeEnv {
                    name: "NZBGET_USER".to_string(),
                    value: None,
                    from_secret: Some("instance:nzbget_username".to_string()),
                },
                ManifestRuntimeEnv {
                    name: "NZBGET_PASS".to_string(),
                    value: None,
                    from_secret: Some("instance:nzbget_password".to_string()),
                },
            ],
            egress: None,
        };

        executor
            .ensure_runtime_running(
                instance_id,
                "elixir.modules.nzbget".to_string(),
                "default".to_string(),
                runtime_spec,
                None,
                vec!["svc-nzb".to_string()],
            )
            .await?;

        let spec = runtime.last_spec().expect("captured nzbget spec");
        let env_map: std::collections::HashMap<_, _> = spec
            .env
            .iter()
            .map(|entry| (entry.name.clone(), entry.value.clone()))
            .collect();
        let username_secret = store
            .get_secret(SecretScope::Instance, Some(instance_id), "nzbget_username")
            .await?
            .expect("nzbget username secret");
        let password_secret = store
            .get_secret(SecretScope::Instance, Some(instance_id), "nzbget_password")
            .await?
            .expect("nzbget password secret");
        let username = secrets.decrypt(&username_secret.value_encrypted)?;
        let password = secrets.decrypt(&password_secret.value_encrypted)?;
        assert_eq!(env_map.get("NZBGET_USER"), Some(&username));
        assert_eq!(env_map.get("NZBGET_PASS"), Some(&password));
        Ok(())
    }

    #[tokio::test]
    async fn nzbget_uses_default_wireguard_secret_when_runtime_egress_not_declared() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_extension(&store, "elixir.modules.nzbget", json!({})).await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "elixir.modules.nzbget".to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;

        let secrets = SecretsManager::from_key_bytes([7u8; 32], true);
        let encrypted =
            secrets.encrypt("[Interface]\nPrivateKey = test\nAddress = 10.64.0.2/32\n")?;
        store
            .upsert_secret(&NewSecret {
                secret_id: Uuid::new_v4(),
                scope: SecretScope::Global,
                scope_id: None,
                key: "wireguard_config".to_string(),
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
        )
        .with_default_wireguard_config_secret(Some("global:wireguard_config".to_string()));

        let runtime_spec = ManifestRuntime {
            r#type: "container".to_string(),
            image: Some("example/nzbget:latest".to_string()),
            network: None,
            service_name: Some("elx-nzbget".to_string()),
            ports: vec![ManifestRuntimePort {
                container: 6789,
                host: None,
            }],
            volumes: Vec::new(),
            env: Vec::new(),
            egress: None,
        };

        executor
            .ensure_runtime_running(
                instance_id,
                "elixir.modules.nzbget".to_string(),
                "default".to_string(),
                runtime_spec,
                None,
                vec!["svc-nzb".to_string()],
            )
            .await?;

        let specs = runtime.specs();
        assert_eq!(
            specs.len(),
            2,
            "expected default wireguard wrapping for nzbget"
        );
        assert!(specs[1].network_mode.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn resolve_downloader_credentials_injects_nzbget_settings() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_extension(
            &store,
            "elixir.modules.nzbget",
            json!({
                "id": "elixir.modules.nzbget",
                "version": "1.0.0",
                "kind": "module",
                "name": "NZBGet",
                "provides": [{
                    "capability": "downloader.nzb",
                    "slot": "default",
                    "implementation": "nzbget"
                }],
                "runtime": {
                    "type": "container",
                    "image": "example/nzbget:latest",
                    "service_name": "elx-nzbget"
                }
            }),
        )
        .await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "elixir.modules.nzbget".to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;

        let endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "svc-elixir-modules-nzbget-default".to_string(),
            6789,
            None,
            Some("elixir_net".to_string()),
        )?;
        store
            .upsert_provider(&NewProvider {
                provider_id: Uuid::new_v4(),
                instance_id,
                capability: "downloader.nzb".to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: Some("nzbget".to_string()),
                scope_json: None,
                endpoint_json: Some(serde_json::to_value(&endpoint)?),
                health_state: ProviderHealthState::Healthy,
            })
            .await?;

        let secrets = SecretsManager::from_key_bytes([6u8; 32], true);
        let mut patch = DriverPatch::MediaManagerTv(MediaManagerTvPatch::SetDownloaders {
            downloaders: vec![DownloaderSpec {
                name: "NZBGet".to_string(),
                r#type: "nzbget".to_string(),
                url: "http://elx-nzbget:6789".to_string(),
                api_key: None,
                category: Some("movies".to_string()),
                tags: Vec::new(),
                enabled: Some(true),
                settings: HashMap::new(),
            }],
        });

        let probe = StubProbe::default();
        resolve_downloader_credentials(&store, &secrets, &probe, &mut patch).await?;

        let DriverPatch::MediaManagerTv(MediaManagerTvPatch::SetDownloaders { downloaders }) =
            patch
        else {
            panic!("expected media manager tv patch");
        };
        let downloader = downloaders.first().expect("nzbget downloader");
        assert_eq!(
            downloader.url,
            endpoint.canonical_url()?,
            "expected legacy downloader host to be rewritten to canonical provider url"
        );
        let username = downloader
            .settings
            .get("username")
            .and_then(serde_json::Value::as_str)
            .expect("username setting");
        let password = downloader
            .settings
            .get("password")
            .and_then(serde_json::Value::as_str)
            .expect("password setting");
        assert!(!username.trim().is_empty());
        assert!(!password.trim().is_empty());

        let username_secret = store
            .get_secret(SecretScope::Instance, Some(instance_id), "nzbget_username")
            .await?
            .expect("nzbget username secret");
        let password_secret = store
            .get_secret(SecretScope::Instance, Some(instance_id), "nzbget_password")
            .await?
            .expect("nzbget password secret");
        assert_eq!(username, secrets.decrypt(&username_secret.value_encrypted)?);
        assert_eq!(password, secrets.decrypt(&password_secret.value_encrypted)?);
        Ok(())
    }

    #[tokio::test]
    async fn resolve_indexer_apps_defers_when_sonarr_dependency_is_unreachable() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_extension(
            &store,
            "elixir.modules.sonarr",
            json!({
                "id": "elixir.modules.sonarr",
                "version": "1.0.0",
                "kind": "module",
                "name": "Sonarr",
                "provides": [{
                    "capability": "media.manager.tv",
                    "slot": "default",
                    "implementation": "sonarr"
                }],
                "runtime": {
                    "type": "container",
                    "image": "example/sonarr:latest",
                    "service_name": "elx-sonarr"
                }
            }),
        )
        .await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "elixir.modules.sonarr".to_string(),
                instance_name: "default".to_string(),
                config_json: Some(json!({ "api_key": "sonarr-test-key" })),
                enabled: true,
            })
            .await?;

        let endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "sonarr.example".to_string(),
            8989,
            None,
            None,
        )?;
        store
            .upsert_provider(&NewProvider {
                provider_id: Uuid::new_v4(),
                instance_id,
                capability: "media.manager.tv".to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: Some("sonarr".to_string()),
                scope_json: None,
                endpoint_json: Some(serde_json::to_value(&endpoint)?),
                health_state: ProviderHealthState::Healthy,
            })
            .await?;

        let secrets = SecretsManager::from_key_bytes([9u8; 32], true);
        let runtime = StubRuntime;
        let probe = StubProbe::default().fail_tcp_for("sonarr.example", 8989);
        let mut patch = DriverPatch::IndexerRegistry(IndexerRegistryPatch::RegisterApps {
            apps: vec![crate::drivers::AppSpec {
                name: "Sonarr".to_string(),
                implementation: "Sonarr".to_string(),
                url: "http://sonarr.example:8989".to_string(),
                api_key: None,
                tags: Vec::new(),
                categories: vec!["5000".to_string()],
                enabled: Some(true),
                settings: HashMap::new(),
            }],
        });

        let err = resolve_indexer_apps(&store, &secrets, &runtime, &probe, &mut patch)
            .await
            .expect_err("expected deferred dependency error");
        let message =
            deferred_dependency_message(&err).expect("deferred dependency message should exist");
        assert!(message.contains("Sonarr is not reachable yet"));
        assert!(message.contains("Prowlarr application registration"));

        Ok(())
    }

    #[tokio::test]
    async fn apply_builtin_downloader_profiles_updates_qbittorrent_once() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_extension(&store, "elixir.modules.qbittorrent", json!({})).await?;
        let (host, port) = start_mock_qbittorrent_auth_server().await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "elixir.modules.qbittorrent".to_string(),
                instance_name: "default".to_string(),
                config_json: Some(json!({
                    "username": "admin",
                    "password": "adminadmin"
                })),
                enabled: true,
            })
            .await?;

        let provider_id = Uuid::new_v4();
        let endpoint = ProviderEndpoint::new("http".to_string(), host, port, None, None)?;
        store
            .upsert_provider(&NewProvider {
                provider_id,
                instance_id,
                capability: "downloader.torrent".to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: Some("qbittorrent".to_string()),
                scope_json: None,
                endpoint_json: Some(serde_json::to_value(endpoint)?),
                health_state: ProviderHealthState::Unknown,
            })
            .await?;

        let probe = StubProbe::default();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut drivers = DriverRegistry::new();
        drivers.register(RecordingDriver::new("downloader.torrent", calls.clone()));
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
        let secrets = SecretsManager::from_key_bytes([5u8; 32], true);
        let executor = Executor::new(
            &database.pool,
            &probe,
            &drivers,
            &runtime,
            runtime_paths,
            &secrets,
        );

        executor.apply_builtin_downloader_profiles_now().await?;
        executor.apply_builtin_downloader_profiles_now().await?;

        let calls = calls.lock().expect("recording driver lock");
        assert_eq!(calls.len(), 1, "profile should only apply once");
        let crate::drivers::DriverPatch::DownloaderTorrent(
            DownloaderTorrentPatch::SetPreferences {
                max_connections,
                disk_cache_mb,
                queueing_enabled,
                listen_port,
                preallocate_all,
                ..
            },
        ) = &calls[0]
        else {
            panic!("expected qbittorrent preferences patch");
        };
        assert_eq!(*max_connections, Some(500));
        assert_eq!(*disk_cache_mb, Some(512));
        assert_eq!(*queueing_enabled, Some(false));
        assert_eq!(*listen_port, Some(51413));
        assert_eq!(*preallocate_all, Some(false));
        drop(calls);

        let instance = store.get_instance(instance_id).await?.expect("instance");
        let parsed = parse_qbittorrent_instance_config(instance.config_json.as_ref())?;
        assert_eq!(
            parsed.performance_profile_version.as_deref(),
            Some(BALANCED_PERFORMANCE_PROFILE_VERSION)
        );
        Ok(())
    }

    #[tokio::test]
    async fn health_gate_does_not_apply_qbittorrent_profile_implicitly() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_extension(&store, "elixir.modules.qbittorrent", json!({})).await?;
        let (host, port) = start_mock_qbittorrent_auth_server().await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "elixir.modules.qbittorrent".to_string(),
                instance_name: "default".to_string(),
                config_json: Some(json!({
                    "username": "admin",
                    "password": "adminadmin"
                })),
                enabled: true,
            })
            .await?;

        let provider_id = Uuid::new_v4();
        let endpoint = ProviderEndpoint::new("http".to_string(), host, port, None, None)?;
        store
            .upsert_provider(&NewProvider {
                provider_id,
                instance_id,
                capability: "downloader.torrent".to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: Some("qbittorrent".to_string()),
                scope_json: None,
                endpoint_json: Some(serde_json::to_value(endpoint)?),
                health_state: ProviderHealthState::Unknown,
            })
            .await?;

        let probe = StubProbe::default();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut drivers = DriverRegistry::new();
        drivers.register(RecordingDriver::new("downloader.torrent", calls.clone()));
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
            .transport_gate_once(provider_id)
            .await
            .map_err(|err| err.source)?;
        executor
            .bootstrap_gate_once(provider_id)
            .await
            .map_err(|err| err.source)?;
        executor
            .health_gate_once(provider_id)
            .await
            .map_err(|err| err.source)?;
        executor
            .health_gate_once(provider_id)
            .await
            .map_err(|err| err.source)?;

        assert!(
            calls.lock().expect("recording driver lock").is_empty(),
            "steady-state health checks must not rewrite downloader profiles"
        );
        let instance = store.get_instance(instance_id).await?.expect("instance");
        let parsed = parse_qbittorrent_instance_config(instance.config_json.as_ref())?;
        assert_eq!(parsed.performance_profile_version.as_deref(), None);
        Ok(())
    }

    #[tokio::test]
    async fn apply_builtin_downloader_profiles_uses_aggressive_qbittorrent_profile_when_selected()
    -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_extension(&store, "elixir.modules.qbittorrent", json!({})).await?;
        store
            .upsert_extension_setting("downloader_profile", &json!("aggressive"))
            .await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "elixir.modules.qbittorrent".to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;

        let provider_id = Uuid::new_v4();
        let endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "qbittorrent.example".to_string(),
            8080,
            None,
            None,
        )?;
        store
            .upsert_provider(&NewProvider {
                provider_id,
                instance_id,
                capability: "downloader.torrent".to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: Some("qbittorrent".to_string()),
                scope_json: None,
                endpoint_json: Some(serde_json::to_value(endpoint)?),
                health_state: ProviderHealthState::Unknown,
            })
            .await?;

        let probe = StubProbe::default();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut drivers = DriverRegistry::new();
        drivers.register(RecordingDriver::new("downloader.torrent", calls.clone()));
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
        let secrets = SecretsManager::from_key_bytes([5u8; 32], true);
        let executor = Executor::new(
            &database.pool,
            &probe,
            &drivers,
            &runtime,
            runtime_paths,
            &secrets,
        );

        executor.apply_builtin_downloader_profiles_now().await?;

        let calls = calls.lock().expect("recording driver lock");
        assert_eq!(calls.len(), 1, "profile should apply once");
        let crate::drivers::DriverPatch::DownloaderTorrent(
            DownloaderTorrentPatch::SetPreferences {
                max_connections,
                disk_cache_mb,
                max_active_downloads,
                ..
            },
        ) = &calls[0]
        else {
            panic!("expected qbittorrent preferences patch");
        };
        assert_eq!(*max_connections, Some(800));
        assert_eq!(*disk_cache_mb, Some(768));
        assert_eq!(*max_active_downloads, Some(80));
        drop(calls);

        let instance = store.get_instance(instance_id).await?.expect("instance");
        let parsed = parse_qbittorrent_instance_config(instance.config_json.as_ref())?;
        assert_eq!(
            parsed.performance_profile_version.as_deref(),
            Some(AGGRESSIVE_PERFORMANCE_PROFILE_VERSION)
        );
        Ok(())
    }

    #[tokio::test]
    async fn apply_builtin_downloader_profiles_updates_nzbget_once() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_extension(&store, "elixir.modules.nzbget", json!({})).await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "elixir.modules.nzbget".to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;

        let provider_id = Uuid::new_v4();
        let endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "example.com".to_string(),
            6789,
            None,
            None,
        )?;
        store
            .upsert_provider(&NewProvider {
                provider_id,
                instance_id,
                capability: "downloader.nzb".to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: Some("nzbget".to_string()),
                scope_json: None,
                endpoint_json: Some(serde_json::to_value(endpoint)?),
                health_state: ProviderHealthState::Unknown,
            })
            .await?;

        let probe = StubProbe::default();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut drivers = DriverRegistry::new();
        drivers.register(RecordingDriver::new("downloader.nzb", calls.clone()));
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
        let secrets = SecretsManager::from_key_bytes([6u8; 32], true);
        let executor = Executor::new(
            &database.pool,
            &probe,
            &drivers,
            &runtime,
            runtime_paths,
            &secrets,
        );

        executor.apply_builtin_downloader_profiles_now().await?;
        executor.apply_builtin_downloader_profiles_now().await?;

        let calls = calls.lock().expect("recording driver lock");
        assert_eq!(calls.len(), 1, "profile should only apply once");
        let crate::drivers::DriverPatch::DownloaderNzb(DownloaderNzbPatch::SetPreferences {
            main_dir,
            server_connections,
            write_buffer_kb,
            direct_write,
            par_check,
            unpack_pause_queue,
            script_dir,
            web_dir,
            config_template,
            ..
        }) = &calls[0]
        else {
            panic!("expected nzbget preferences patch");
        };
        assert_eq!(main_dir.as_deref(), Some("/config"));
        assert_eq!(*server_connections, None);
        assert_eq!(*write_buffer_kb, Some(1024));
        assert_eq!(*direct_write, Some(true));
        assert_eq!(par_check.as_deref(), Some("auto"));
        assert_eq!(*unpack_pause_queue, Some(true));
        assert_eq!(script_dir.as_deref(), Some("/config/scripts"));
        assert_eq!(web_dir.as_deref(), Some("/app/nzbget/webui"));
        assert_eq!(
            config_template.as_deref(),
            Some("/app/nzbget/webui/nzbget.conf.template")
        );
        drop(calls);

        let instance = store.get_instance(instance_id).await?.expect("instance");
        let parsed = parse_nzbget_instance_config(instance.config_json.as_ref())?;
        assert_eq!(
            parsed.performance_profile_version.as_deref(),
            Some(NZBGET_BALANCED_PERFORMANCE_PROFILE_VERSION)
        );
        Ok(())
    }

    #[tokio::test]
    async fn apply_builtin_downloader_profiles_repairs_named_volume_nzbget_config() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_extension(&store, "elixir.modules.nzbget", json!({})).await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "elixir.modules.nzbget".to_string(),
                instance_name: "default".to_string(),
                config_json: Some(json!({
                    "username": "elixir",
                    "password": "secret",
                    "runtime": {
                        "config_storage": {
                            "source_kind": "named_volume"
                        }
                    }
                })),
                enabled: true,
            })
            .await?;

        let provider_id = Uuid::new_v4();
        let endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "example.com".to_string(),
            6789,
            None,
            None,
        )?;
        store
            .upsert_provider(&NewProvider {
                provider_id,
                instance_id,
                capability: "downloader.nzb".to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: Some("nzbget".to_string()),
                scope_json: None,
                endpoint_json: Some(serde_json::to_value(endpoint)?),
                health_state: ProviderHealthState::Unknown,
            })
            .await?;

        let probe = StubProbe::default();
        let drivers = DriverRegistry::new();
        let runtime = NzbgetNamedVolumeRuntime::new(
            "InterDir=/runtime/incomplete\nNzbDir=/runtime/nzb\nQueueDir=/runtime/queue\nTempDir=/runtime/tmp\n",
        );
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
        let secrets = SecretsManager::from_key_bytes([7u8; 32], true);
        let executor = Executor::new(
            &database.pool,
            &probe,
            &drivers,
            &runtime,
            runtime_paths,
            &secrets,
        );

        executor.apply_builtin_downloader_profiles_now().await?;

        let rendered = runtime.config_text();
        for (key, value) in NZBGET_REQUIRED_MANAGED_PATHS {
            assert!(
                rendered.contains(&format!("{key}={value}")),
                "expected rendered config to contain {key}={value}, got:\n{rendered}"
            );
        }
        assert_eq!(runtime.stop_count(), 1);
        assert_eq!(runtime.copy_count(), 1);
        assert_eq!(runtime.start_count(), 1);

        let instance = store.get_instance(instance_id).await?.expect("instance");
        let parsed = parse_nzbget_instance_config(instance.config_json.as_ref())?;
        assert_eq!(
            parsed.performance_profile_version.as_deref(),
            Some(NZBGET_BALANCED_PERFORMANCE_PROFILE_VERSION)
        );
        Ok(())
    }

    #[tokio::test]
    async fn apply_driver_patch_skips_in_sync_disruptive_patch() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_extension(&store, "elixir.modules.qbittorrent", json!({})).await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "elixir.modules.qbittorrent".to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;

        let provider_id = Uuid::new_v4();
        let endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "svc-qbit".to_string(),
            8080,
            None,
            Some("elixir_net".to_string()),
        )?;
        store
            .upsert_provider(&NewProvider {
                provider_id,
                instance_id,
                capability: "downloader.torrent".to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: Some("qbittorrent".to_string()),
                scope_json: None,
                endpoint_json: Some(serde_json::to_value(endpoint)?),
                health_state: ProviderHealthState::Healthy,
            })
            .await?;

        let probe = StubProbe::default();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut drivers = DriverRegistry::new();
        drivers.register(
            RecordingDriver::new("downloader.torrent", calls.clone())
                .with_semantics(crate::drivers::PatchSemantics::desired_change_only(
                    crate::drivers::PatchSideEffect::ReloadService,
                ))
                .with_evaluation(crate::drivers::DriftEvaluation::in_sync()),
        );
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
        let secrets = SecretsManager::from_key_bytes([7u8; 32], true);
        let executor = Executor::new(
            &database.pool,
            &probe,
            &drivers,
            &runtime,
            runtime_paths,
            &secrets,
        );

        executor
            .apply_driver_patch(
                "test.connector".to_string(),
                provider_id,
                json!({
                    "op": "set_preferences",
                    "queueing_enabled": false
                }),
            )
            .await?;

        assert!(
            calls.lock().expect("recording driver lock").is_empty(),
            "in-sync disruptive patch should not apply"
        );
        Ok(())
    }

    #[tokio::test]
    async fn apply_driver_patch_requires_explicit_repair_for_unknown_disruptive_drift() -> Result<()>
    {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_extension(&store, "elixir.modules.qbittorrent", json!({})).await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "elixir.modules.qbittorrent".to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;

        let provider_id = Uuid::new_v4();
        let endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "svc-qbit".to_string(),
            8080,
            None,
            Some("elixir_net".to_string()),
        )?;
        store
            .upsert_provider(&NewProvider {
                provider_id,
                instance_id,
                capability: "downloader.torrent".to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: Some("qbittorrent".to_string()),
                scope_json: None,
                endpoint_json: Some(serde_json::to_value(endpoint)?),
                health_state: ProviderHealthState::Healthy,
            })
            .await?;

        let probe = StubProbe::default();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut drivers = DriverRegistry::new();
        drivers.register(
            RecordingDriver::new("downloader.torrent", calls.clone())
                .with_semantics(crate::drivers::PatchSemantics::desired_change_only(
                    crate::drivers::PatchSideEffect::ReloadService,
                ))
                .with_evaluation(
                    crate::drivers::DriftEvaluation::unknown(
                        "opaque secret fields prevent safe live comparison",
                    )
                    .with_non_comparable_fields(vec![
                        crate::drivers::DriftField::new(
                            "Server1.Password",
                            crate::drivers::FieldSemantics::OpaqueSecret,
                        ),
                    ]),
                ),
        );
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
        let secrets = SecretsManager::from_key_bytes([8u8; 32], true);
        let executor = Executor::new(
            &database.pool,
            &probe,
            &drivers,
            &runtime,
            runtime_paths,
            &secrets,
        );

        let err = executor
            .apply_driver_patch(
                "test.connector".to_string(),
                provider_id,
                json!({
                    "op": "set_preferences",
                    "queueing_enabled": false
                }),
            )
            .await
            .expect_err("unknown disruptive drift should require explicit repair");
        let message = err.to_string();
        assert!(
            message.contains("requires explicit repair"),
            "unexpected error: {message}"
        );
        assert!(
            message.contains("Server1.Password"),
            "opaque field should be surfaced: {message}"
        );
        assert!(
            calls.lock().expect("recording driver lock").is_empty(),
            "unknown disruptive patch should not apply"
        );
        Ok(())
    }

    #[tokio::test]
    async fn apply_driver_patch_allows_unknown_periodic_safe_patch() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_extension(&store, "elixir.modules.qbittorrent", json!({})).await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "elixir.modules.qbittorrent".to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;

        let provider_id = Uuid::new_v4();
        let endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "svc-qbit".to_string(),
            8080,
            None,
            Some("elixir_net".to_string()),
        )?;
        store
            .upsert_provider(&NewProvider {
                provider_id,
                instance_id,
                capability: "downloader.torrent".to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: Some("qbittorrent".to_string()),
                scope_json: None,
                endpoint_json: Some(serde_json::to_value(endpoint)?),
                health_state: ProviderHealthState::Healthy,
            })
            .await?;

        let probe = StubProbe::default();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut drivers = DriverRegistry::new();
        drivers.register(
            RecordingDriver::new("downloader.torrent", calls.clone()).with_evaluation(
                crate::drivers::DriftEvaluation::unknown(
                    "periodic-safe patch did not provide semantic drift evaluation",
                ),
            ),
        );
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
        let secrets = SecretsManager::from_key_bytes([9u8; 32], true);
        let executor = Executor::new(
            &database.pool,
            &probe,
            &drivers,
            &runtime,
            runtime_paths,
            &secrets,
        );

        executor
            .apply_driver_patch(
                "test.connector".to_string(),
                provider_id,
                json!({
                    "op": "set_preferences",
                    "queueing_enabled": false
                }),
            )
            .await?;

        assert_eq!(
            calls.lock().expect("recording driver lock").len(),
            1,
            "periodic-safe unknown patch should still apply"
        );
        Ok(())
    }

    #[tokio::test]
    async fn apply_driver_patch_returns_partial_warning_note() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_extension(&store, "elixir.modules.qbittorrent", json!({})).await?;

        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "elixir.modules.qbittorrent".to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;

        let provider_id = Uuid::new_v4();
        let endpoint = ProviderEndpoint::new(
            "http".to_string(),
            "svc-qbit".to_string(),
            8080,
            None,
            Some("elixir_net".to_string()),
        )?;
        store
            .upsert_provider(&NewProvider {
                provider_id,
                instance_id,
                capability: "downloader.torrent".to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: Some("qbittorrent".to_string()),
                scope_json: None,
                endpoint_json: Some(serde_json::to_value(endpoint)?),
                health_state: ProviderHealthState::Healthy,
            })
            .await?;

        let probe = StubProbe::default();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut drivers = DriverRegistry::new();
        drivers.register(
            RecordingDriver::new("downloader.torrent", calls.clone()).with_apply_result(
                crate::drivers::ApplyResult::applied_with_message("partial apply: one item failed"),
            ),
        );
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
        let secrets = SecretsManager::from_key_bytes([10u8; 32], true);
        let executor = Executor::new(
            &database.pool,
            &probe,
            &drivers,
            &runtime,
            runtime_paths,
            &secrets,
        );

        let note = executor
            .apply_driver_patch(
                "test.connector".to_string(),
                provider_id,
                json!({
                    "op": "set_preferences",
                    "queueing_enabled": false
                }),
            )
            .await?;

        assert_eq!(note.as_deref(), Some("partial apply: one item failed"));
        assert_eq!(calls.lock().expect("recording driver lock").len(), 1);
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
            egress: None,
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
            egress: None,
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

    #[tokio::test]
    async fn install_runtime_asset_copies_extension_file_once_and_reuses_existing_bytes()
    -> Result<()> {
        let database = setup_db().await?;
        let probe = StubProbe::default();
        let drivers = DriverRegistry::new();
        let runtime = AssetInstallRuntime::default();
        let temp_dir = TempDir::new()?;
        let extensions_root = temp_dir.path().join("data").join("extensions");
        let asset_dir = extensions_root
            .join("unpacked")
            .join("elixir.connectors.prowlarr_public_indexers")
            .join("1.0.0")
            .join("assets")
            .join("config");
        std::fs::create_dir_all(&asset_dir)?;
        let asset_path = asset_dir.join("custom-indexer.yml");
        std::fs::write(&asset_path, "id: customindexer\nname: Custom Indexer\n")?;

        let runtime_paths = RuntimePaths::from_roots(
            extensions_root.to_string_lossy().as_ref(),
            temp_dir.path().to_string_lossy().as_ref(),
        );
        let secrets = SecretsManager::from_key_bytes([21u8; 32], true);
        let executor = Executor::new(
            &database.pool,
            &probe,
            &drivers,
            &runtime,
            runtime_paths,
            &secrets,
        );

        let instance_id = Uuid::new_v4();
        let action = ExecutorAction::InstallRuntimeAsset {
            target_instance_id: instance_id,
            source_extension_id: "elixir.connectors.prowlarr_public_indexers".to_string(),
            source_extension_version: "1.0.0".to_string(),
            source_path: "assets/config/custom-indexer.yml".to_string(),
            destination_path: "/config/Definitions/Custom/custom-indexer.yml".to_string(),
        };

        executor.apply(action).await?;
        assert_eq!(
            runtime.file_contents("/config/Definitions/Custom/custom-indexer.yml"),
            Some(b"id: customindexer\nname: Custom Indexer\n".to_vec())
        );
        assert_eq!(
            runtime.copied_destinations(),
            vec!["/config/Definitions/Custom/custom-indexer.yml".to_string()]
        );
        assert_eq!(
            runtime.ensured_directories(),
            vec![vec!["/config/Definitions/Custom".to_string()]]
        );

        executor
            .apply(ExecutorAction::InstallRuntimeAsset {
                target_instance_id: instance_id,
                source_extension_id: "elixir.connectors.prowlarr_public_indexers".to_string(),
                source_extension_version: "1.0.0".to_string(),
                source_path: "assets/config/custom-indexer.yml".to_string(),
                destination_path: "/config/Definitions/Custom/custom-indexer.yml".to_string(),
            })
            .await?;
        assert_eq!(
            runtime.copied_destinations(),
            vec!["/config/Definitions/Custom/custom-indexer.yml".to_string()]
        );

        Ok(())
    }

    #[tokio::test]
    async fn restart_runtime_action_stops_then_starts_container() -> Result<()> {
        let database = setup_db().await?;
        let probe = StubProbe::default();
        let drivers = DriverRegistry::new();
        let runtime = AssetInstallRuntime::default();
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
        let secrets = SecretsManager::from_key_bytes([22u8; 32], true);
        let executor = Executor::new(
            &database.pool,
            &probe,
            &drivers,
            &runtime,
            runtime_paths,
            &secrets,
        );

        executor
            .apply(ExecutorAction::RestartRuntime {
                instance_id: Uuid::new_v4(),
            })
            .await?;

        assert_eq!(runtime.stop_count(), 1);
        assert_eq!(runtime.start_count(), 1);
        Ok(())
    }

    #[cfg(feature = "docker-wireguard-tests")]
    mod docker_wireguard_tests {
        use super::*;

        use std::collections::HashMap;
        use std::process::{Command, Stdio};
        use std::thread;
        use std::time::Duration;

        use anyhow::{Context, bail};

        use crate::runtime::RuntimeManager;
        use crate::runtime::docker::DockerRuntimeManager;

        struct ContainerCleanup {
            names: Vec<String>,
        }

        impl ContainerCleanup {
            fn new(names: Vec<String>) -> Self {
                Self { names }
            }
        }

        impl Drop for ContainerCleanup {
            fn drop(&mut self) {
                for name in &self.names {
                    let _ = Command::new("docker")
                        .arg("rm")
                        .arg("-f")
                        .arg(name)
                        .stdout(Stdio::null())
                        .stderr(Stdio::null())
                        .status();
                }
            }
        }

        #[tokio::test]
        async fn wireguard_egress_docker_namespace_routing() -> Result<()> {
            ensure_docker_available()?;
            if !Path::new("/dev/net/tun").exists() {
                eprintln!("skipping docker wireguard test: /dev/net/tun is not available");
                return Ok(());
            }

            let database = setup_db().await?;
            let store = ExtensionStore::new(&database.pool);
            insert_extension(&store, "ext.wg", json!({})).await?;

            let instance_id = Uuid::new_v4();
            store
                .create_instance(&NewExtensionInstance {
                    instance_id,
                    extension_id: "ext.wg".to_string(),
                    instance_name: "default".to_string(),
                    config_json: None,
                    enabled: true,
                })
                .await?;

            let wireguard_config = r#"[Interface]
PrivateKey = yAnzdtF2rM8Nl1N8MPm+2MvmFo0xSg6u40qCMgfHdC0=
Address = 10.64.0.2/32
DNS = 1.1.1.1

[Peer]
PublicKey = fNBdb9h9NP7VDaRao7IhiHBpjz2uVH54camzato3tr0=
AllowedIPs = 0.0.0.0/0,::/0
Endpoint = 127.0.0.1:51820
PersistentKeepalive = 25
"#;

            let secrets = SecretsManager::from_key_bytes([11u8; 32], true);
            let encrypted = secrets.encrypt(wireguard_config)?;
            store
                .upsert_secret(&NewSecret {
                    secret_id: Uuid::new_v4(),
                    scope: SecretScope::Instance,
                    scope_id: Some(instance_id),
                    key: "wg_config".to_string(),
                    value_encrypted: encrypted,
                    rotatable: false,
                })
                .await?;

            let runtime = DockerRuntimeManager::new(None);
            runtime.ensure_network("elixir_net").await?;

            let suffix = short_id();
            let target_name = format!("elixir-wg-target-{suffix}");
            let target_alias = format!("svc-wg-target-{suffix}");
            let app_name = container_name(instance_id);
            let gateway_name = format!("{app_name}-vpn");
            let _cleanup = ContainerCleanup::new(vec![
                app_name.clone(),
                gateway_name.clone(),
                target_name.clone(),
            ]);

            let mut target_labels = HashMap::new();
            target_labels.insert("elixir.managed".to_string(), "true".to_string());
            target_labels.insert("elixir.instance_id".to_string(), Uuid::new_v4().to_string());
            target_labels.insert("elixir.extension_id".to_string(), "elixir.test".to_string());
            let target_spec = ContainerSpec {
                name: target_name.clone(),
                image: "hashicorp/http-echo:0.2.3".to_string(),
                network: "elixir_net".to_string(),
                network_mode: None,
                aliases: vec![target_alias.clone()],
                env: Vec::new(),
                volumes: Vec::new(),
                ports: Vec::new(),
                labels: target_labels,
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
            runtime.ensure_container(&target_spec).await?;

            let probe = StubProbe::default();
            let drivers = DriverRegistry::new();
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
            )
            .with_wireguard_gateway_image("qmcgaw/gluetun:v3.39.0");

            let runtime_spec = ManifestRuntime {
                r#type: "container".to_string(),
                image: Some("nginx:1.27-alpine".to_string()),
                network: None,
                service_name: Some(format!("elx-wg-app-{suffix}")),
                ports: vec![],
                volumes: Vec::new(),
                env: Vec::new(),
                egress: Some(ManifestRuntimeEgress {
                    mode: "wireguard".to_string(),
                    strict: true,
                    wireguard_config_secret: Some("instance:wg_config".to_string()),
                    wireguard_gateway_image: None,
                }),
            };

            executor
                .ensure_runtime_running(
                    instance_id,
                    "ext.wg".to_string(),
                    "default".to_string(),
                    runtime_spec,
                    None,
                    vec![format!("svc-wg-app-{suffix}")],
                )
                .await?;

            wait_until_running(&runtime, &gateway_name).await?;
            wait_until_running(&runtime, &app_name).await?;

            let app_mode = inspect_network_mode(&app_name)?;
            let gateway = runtime
                .get_container_handle(&gateway_name)
                .await?
                .ok_or_else(|| anyhow::anyhow!("gateway container missing"))?;
            let app_expected_by_name = format!("container:{gateway_name}");
            let app_expected_by_id = format!("container:{}", gateway.id);
            assert!(
                app_mode == app_expected_by_name || app_mode == app_expected_by_id,
                "unexpected app network mode '{}'; expected '{}' or '{}'",
                app_mode,
                app_expected_by_name,
                app_expected_by_id
            );

            let body = wait_for_gateway_http_probe(&gateway_name, &target_alias)?;
            assert!(
                body.contains("ok"),
                "unexpected response body from target via gateway namespace: {body}"
            );

            Ok(())
        }

        #[tokio::test]
        #[ignore = "live Docker downloader test; set ELIXIR_LIVE_DOWNLOAD_NETWORK_TESTS=1 to run"]
        async fn live_qbittorrent_rehome_preserves_config_and_download_state() -> Result<()> {
            if !live_download_network_tests_enabled() {
                eprintln!(
                    "skipping live qBittorrent rehome test: ELIXIR_LIVE_DOWNLOAD_NETWORK_TESTS=1 is not set"
                );
                return Ok(());
            }
            ensure_docker_available()?;

            let suffix = short_id();
            let network_name = format!("elixir_live_net_{suffix}");
            let _network = NetworkCleanup::new(network_name.clone());
            let runtime = DockerRuntimeManager::new(None);
            runtime.ensure_network(&network_name).await?;

            let instance_id = Uuid::new_v4();
            let app_name = format!("elixir-live-qb-{suffix}");
            let gateway_name = format!("elixir-live-qb-gateway-{suffix}");
            let config_volume = format!("elixir_live_qb_config_{suffix}");
            let host_root = docker_shared_tempdir(&suffix)?;
            let downloads_dir = host_root.path().join("downloads");
            std::fs::create_dir_all(&downloads_dir)?;

            let _volumes = VolumeCleanup::new(vec![config_volume.clone()]);
            let _containers = ContainerCleanup::new(vec![app_name.clone(), gateway_name.clone()]);
            docker_volume_create(&config_volume)?;
            seed_qbittorrent_state(&config_volume, &downloads_dir)?;

            let gateway_spec =
                live_sleeping_container_spec(&gateway_name, vec![], None, &network_name);
            runtime.ensure_container(&gateway_spec).await?;
            wait_until_running(&runtime, &gateway_name).await?;

            let direct_spec = live_qbittorrent_spec(
                &app_name,
                &config_volume,
                &downloads_dir,
                None,
                instance_id,
                &network_name,
            );
            runtime.ensure_container(&direct_spec).await?;
            wait_until_running(&runtime, &app_name).await?;
            let direct_state = runtime
                .describe_container_runtime_state(&app_name)
                .await?
                .ok_or_else(|| anyhow::anyhow!("direct qBittorrent state missing"))?;
            let direct_volumes = volume_mounts_from_runtime_state(&direct_state);

            let protected_spec = live_qbittorrent_spec(
                &app_name,
                &config_volume,
                &downloads_dir,
                Some(&format!("container:{gateway_name}")),
                instance_id,
                &network_name,
            );
            validate_downloader_volume_preservation(
                "elixir.modules.qbittorrent",
                &direct_volumes,
                &protected_spec.volumes,
            )?;
            runtime.ensure_container(&protected_spec).await?;
            wait_until_running(&runtime, &app_name).await?;
            let protected_state = runtime
                .describe_container_runtime_state(&app_name)
                .await?
                .ok_or_else(|| anyhow::anyhow!("protected qBittorrent state missing"))?;
            let protected_volumes = volume_mounts_from_runtime_state(&protected_state);
            validate_downloader_volume_preservation(
                "elixir.modules.qbittorrent",
                &direct_volumes,
                &protected_volumes,
            )?;
            assert_container_namespace_mode(&runtime, &protected_state, &gateway_name).await?;
            assert_qbittorrent_state_present(&config_volume, &downloads_dir)?;

            Ok(())
        }

        #[tokio::test]
        #[ignore = "live Docker downloader test; set ELIXIR_LIVE_DOWNLOAD_NETWORK_TESTS=1 to run"]
        async fn live_nzbget_rehome_preserves_config_runtime_and_download_state() -> Result<()> {
            if !live_download_network_tests_enabled() {
                eprintln!(
                    "skipping live NZBGet rehome test: ELIXIR_LIVE_DOWNLOAD_NETWORK_TESTS=1 is not set"
                );
                return Ok(());
            }
            ensure_docker_available()?;

            let suffix = short_id();
            let network_name = format!("elixir_live_net_{suffix}");
            let _network = NetworkCleanup::new(network_name.clone());
            let runtime = DockerRuntimeManager::new(None);
            runtime.ensure_network(&network_name).await?;

            let instance_id = Uuid::new_v4();
            let app_name = format!("elixir-live-nzb-{suffix}");
            let gateway_name = format!("elixir-live-nzb-gateway-{suffix}");
            let config_volume = format!("elixir_live_nzb_config_{suffix}");
            let runtime_volume = format!("elixir_live_nzb_runtime_{suffix}");
            let host_root = docker_shared_tempdir(&suffix)?;
            let downloads_dir = host_root.path().join("downloads");
            std::fs::create_dir_all(&downloads_dir)?;

            let _volumes = VolumeCleanup::new(vec![config_volume.clone(), runtime_volume.clone()]);
            let _containers = ContainerCleanup::new(vec![app_name.clone(), gateway_name.clone()]);
            docker_volume_create(&config_volume)?;
            docker_volume_create(&runtime_volume)?;
            seed_nzbget_state(&config_volume, &runtime_volume, &downloads_dir)?;

            let gateway_spec =
                live_sleeping_container_spec(&gateway_name, vec![], None, &network_name);
            runtime.ensure_container(&gateway_spec).await?;
            wait_until_running(&runtime, &gateway_name).await?;

            let direct_spec = live_nzbget_spec(
                &app_name,
                &config_volume,
                &runtime_volume,
                &downloads_dir,
                None,
                instance_id,
                &network_name,
            );
            runtime.ensure_container(&direct_spec).await?;
            wait_until_running(&runtime, &app_name).await?;
            let direct_state = runtime
                .describe_container_runtime_state(&app_name)
                .await?
                .ok_or_else(|| anyhow::anyhow!("direct NZBGet state missing"))?;
            let direct_volumes = volume_mounts_from_runtime_state(&direct_state);

            let protected_spec = live_nzbget_spec(
                &app_name,
                &config_volume,
                &runtime_volume,
                &downloads_dir,
                Some(&format!("container:{gateway_name}")),
                instance_id,
                &network_name,
            );
            validate_downloader_volume_preservation(
                "elixir.modules.nzbget",
                &direct_volumes,
                &protected_spec.volumes,
            )?;
            runtime.ensure_container(&protected_spec).await?;
            wait_until_running(&runtime, &app_name).await?;
            let protected_state = runtime
                .describe_container_runtime_state(&app_name)
                .await?
                .ok_or_else(|| anyhow::anyhow!("protected NZBGet state missing"))?;
            let protected_volumes = volume_mounts_from_runtime_state(&protected_state);
            validate_downloader_volume_preservation(
                "elixir.modules.nzbget",
                &direct_volumes,
                &protected_volumes,
            )?;
            assert_container_namespace_mode(&runtime, &protected_state, &gateway_name).await?;
            assert_nzbget_state_present(&config_volume, &runtime_volume, &downloads_dir)?;

            Ok(())
        }

        #[tokio::test]
        #[ignore = "live Docker gateway test; set ELIXIR_LIVE_DOWNLOAD_NETWORK_TESTS=1 to run"]
        async fn live_gateway_namespace_blocks_app_egress_when_gateway_stops() -> Result<()> {
            if !live_download_network_tests_enabled() {
                eprintln!(
                    "skipping live gateway egress-block test: ELIXIR_LIVE_DOWNLOAD_NETWORK_TESTS=1 is not set"
                );
                return Ok(());
            }
            ensure_docker_available()?;

            let suffix = short_id();
            let network_name = format!("elixir_live_net_{suffix}");
            let _network = NetworkCleanup::new(network_name.clone());
            let runtime = DockerRuntimeManager::new(None);
            runtime.ensure_network(&network_name).await?;

            let target_name = format!("elixir-live-target-{suffix}");
            let target_alias = format!("svc-live-target-{suffix}");
            let gateway_name = format!("elixir-live-gateway-{suffix}");
            let app_name = format!("elixir-live-app-{suffix}");
            let _cleanup = ContainerCleanup::new(vec![
                app_name.clone(),
                gateway_name.clone(),
                target_name.clone(),
            ]);

            let target_spec = live_http_echo_spec(&target_name, &target_alias, &network_name);
            runtime.ensure_container(&target_spec).await?;
            wait_until_running(&runtime, &target_name).await?;

            let gateway_spec =
                live_sleeping_container_spec(&gateway_name, vec![], None, &network_name);
            runtime.ensure_container(&gateway_spec).await?;
            wait_until_running(&runtime, &gateway_name).await?;

            let app_spec = live_sleeping_container_spec(
                &app_name,
                vec![],
                Some(&format!("container:{gateway_name}")),
                &network_name,
            );
            runtime.ensure_container(&app_spec).await?;
            wait_until_running(&runtime, &app_name).await?;

            let body = docker_exec_stdout(
                &app_name,
                &["wget", "-qO-", &format!("http://{target_alias}:8080/")],
            )?;
            assert!(
                body.contains("ok"),
                "unexpected response before gateway stop: {body}"
            );

            let gateway = runtime
                .get_container_handle(&gateway_name)
                .await?
                .ok_or_else(|| anyhow::anyhow!("gateway container missing"))?;
            runtime.stop_container(&gateway).await?;

            let blocked = Command::new("docker")
                .args([
                    "exec",
                    &app_name,
                    "wget",
                    "-qO-",
                    &format!("http://{target_alias}:8080/"),
                ])
                .output()
                .context("probing app egress after gateway stop")?;
            assert!(
                !blocked.status.success(),
                "app egress unexpectedly succeeded after gateway stop: {}",
                String::from_utf8_lossy(&blocked.stdout)
            );

            Ok(())
        }

        #[tokio::test]
        #[ignore = "live Windows Docker Desktop host validation; set ELIXIR_LIVE_DOWNLOAD_NETWORK_TESTS=1 to run"]
        async fn live_windows_docker_desktop_host_validation() -> Result<()> {
            if !live_download_network_tests_enabled() {
                eprintln!(
                    "skipping live Windows Docker Desktop host validation: ELIXIR_LIVE_DOWNLOAD_NETWORK_TESTS=1 is not set"
                );
                return Ok(());
            }
            if std::env::consts::OS != "windows" {
                eprintln!(
                    "skipping live Windows Docker Desktop host validation: host OS is {}",
                    std::env::consts::OS
                );
                return Ok(());
            }
            ensure_docker_available()?;

            let os_type = docker_info_value("{{.OSType}}")?;
            assert_eq!(
                os_type, "linux",
                "Elixir downloader networking requires Docker Desktop Linux-container mode on Windows; docker OSType was '{os_type}'"
            );
            let operating_system = docker_info_value("{{.OperatingSystem}}")?;
            eprintln!("Windows Docker host reports: {operating_system}");

            let suffix = short_id();
            let host_root = docker_shared_tempdir(&suffix)?;
            assert_windows_bind_mount_roundtrip(host_root.path())?;
            assert_host_docker_internal_resolves()?;

            let network_name = format!("elixir_live_net_win_{suffix}");
            let bind_name = format!("elixir-live-win-bind-{suffix}");
            let target_name = format!("elixir-live-win-target-{suffix}");
            let target_alias = format!("svc-live-win-target-{suffix}");
            let gateway_name = format!("elixir-live-win-gateway-{suffix}");
            let app_name = format!("elixir-live-win-app-{suffix}");
            let _network = NetworkCleanup::new(network_name.clone());
            let _cleanup = ContainerCleanup::new(vec![
                bind_name.clone(),
                app_name.clone(),
                gateway_name.clone(),
                target_name.clone(),
            ]);
            let runtime = DockerRuntimeManager::new(None);
            runtime.ensure_network(&network_name).await?;

            assert_windows_runtime_bind_mount_roundtrip(
                &runtime,
                &network_name,
                &bind_name,
                host_root.path(),
            )
            .await?;

            let target_spec = live_http_echo_spec(&target_name, &target_alias, &network_name);
            runtime.ensure_container(&target_spec).await?;
            wait_until_running(&runtime, &target_name).await?;

            let gateway_spec =
                live_sleeping_container_spec(&gateway_name, vec![], None, &network_name);
            runtime.ensure_container(&gateway_spec).await?;
            wait_until_running(&runtime, &gateway_name).await?;

            let app_spec = live_sleeping_container_spec(
                &app_name,
                vec![],
                Some(&format!("container:{gateway_name}")),
                &network_name,
            );
            runtime.ensure_container(&app_spec).await?;
            wait_until_running(&runtime, &app_name).await?;

            let body = docker_exec_stdout(
                &app_name,
                &["wget", "-qO-", &format!("http://{target_alias}:8080/")],
            )?;
            assert!(
                body.contains("ok"),
                "unexpected response before gateway stop: {body}"
            );

            let gateway = runtime
                .get_container_handle(&gateway_name)
                .await?
                .ok_or_else(|| anyhow::anyhow!("gateway container missing"))?;
            runtime.stop_container(&gateway).await?;

            let blocked = Command::new("docker")
                .args([
                    "exec",
                    &app_name,
                    "wget",
                    "-qO-",
                    &format!("http://{target_alias}:8080/"),
                ])
                .output()
                .context("probing Windows app egress after gateway stop")?;
            assert!(
                !blocked.status.success(),
                "Windows app egress unexpectedly succeeded after gateway stop: {}",
                String::from_utf8_lossy(&blocked.stdout)
            );

            Ok(())
        }

        async fn wait_until_running(runtime: &DockerRuntimeManager, name: &str) -> Result<()> {
            let mut last = None;
            for _ in 0..30 {
                if let Some(handle) = runtime.get_container_handle(name).await? {
                    let state = runtime.inspect(&handle).await?;
                    if state.running {
                        return Ok(());
                    }
                    last = Some(format!(
                        "container exists but not running (status={})",
                        state.status
                    ));
                } else {
                    last = Some("container not found yet".to_string());
                }
                thread::sleep(Duration::from_millis(500));
            }
            bail!(
                "container '{}' did not become running: {}",
                name,
                last.unwrap_or_else(|| "unknown state".to_string())
            );
        }

        async fn assert_container_namespace_mode(
            runtime: &DockerRuntimeManager,
            state: &ContainerRuntimeState,
            gateway_name: &str,
        ) -> Result<()> {
            let gateway = runtime
                .get_container_handle(gateway_name)
                .await?
                .ok_or_else(|| anyhow::anyhow!("gateway container missing"))?;
            let actual = normalized_network_mode(state.network_mode.as_deref());
            let expected_by_name = format!("container:{gateway_name}");
            let expected_by_id = format!("container:{}", gateway.id);
            let actual_container_ref = actual
                .as_deref()
                .and_then(|mode| mode.strip_prefix("container:"));
            let id_matches = actual_container_ref.is_some_and(|value| {
                value == gateway.id
                    || value.starts_with(&gateway.id)
                    || gateway.id.starts_with(value)
            });
            assert!(
                actual.as_deref() == Some(expected_by_name.as_str()) || id_matches,
                "unexpected network mode {:?}; expected '{}' or '{}'",
                actual,
                expected_by_name,
                expected_by_id
            );
            Ok(())
        }

        fn wait_for_gateway_http_probe(gateway_name: &str, target_alias: &str) -> Result<String> {
            let mut last_err = None;
            for _ in 0..20 {
                match gateway_http_probe(gateway_name, target_alias) {
                    Ok(body) => return Ok(body),
                    Err(err) => {
                        last_err = Some(err);
                        thread::sleep(Duration::from_millis(750));
                    }
                }
            }
            if let Some(err) = last_err {
                let gateway_logs = docker_logs(gateway_name).unwrap_or_else(|_| String::new());
                bail!(
                    "failed to reach target '{}' from gateway namespace: {}\n{}",
                    target_alias,
                    err,
                    gateway_logs
                );
            }
            bail!("gateway probe failed without an error");
        }

        fn gateway_http_probe(gateway_name: &str, target_alias: &str) -> Result<String> {
            let output = Command::new("docker")
                .args([
                    "run",
                    "--rm",
                    "--network",
                    &format!("container:{gateway_name}"),
                    "curlimages/curl:8.10.1",
                    "-fsS",
                    &format!("http://{target_alias}:8080/"),
                ])
                .output()
                .context("running gateway namespace http probe")?;
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
                bail!("gateway probe command failed: {stderr}");
            }
            Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
        }

        fn docker_info_value(format: &str) -> Result<String> {
            let output = Command::new("docker")
                .args(["info", "--format", format])
                .output()
                .with_context(|| format!("reading docker info value '{format}'"))?;
            if !output.status.success() {
                bail!(
                    "docker info failed: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                );
            }
            Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
        }

        fn assert_windows_bind_mount_roundtrip(host_dir: &Path) -> Result<()> {
            std::fs::write(host_dir.join("host-marker.txt"), "host")
                .context("writing Windows bind-mount marker")?;
            let mount = format!(
                "type=bind,source={},target=/hostcheck",
                host_dir.to_string_lossy()
            );
            let output = Command::new("docker")
                .args([
                    "run",
                    "--rm",
                    "--mount",
                    &mount,
                    "alpine:3.20",
                    "sh",
                    "-lc",
                    "set -e; test -f /hostcheck/host-marker.txt; printf container > /hostcheck/container-marker.txt",
                ])
                .output()
                .context("running Windows bind-mount roundtrip helper")?;
            if !output.status.success() {
                bail!(
                    "Windows bind-mount roundtrip helper failed: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                );
            }
            let marker = std::fs::read_to_string(host_dir.join("container-marker.txt"))
                .context("reading Windows bind-mount roundtrip marker")?;
            assert_eq!(marker, "container");
            Ok(())
        }

        async fn assert_windows_runtime_bind_mount_roundtrip(
            runtime: &DockerRuntimeManager,
            network_name: &str,
            container_name: &str,
            host_dir: &Path,
        ) -> Result<()> {
            std::fs::write(host_dir.join("runtime-host-marker.txt"), "host")
                .context("writing Windows runtime bind-mount marker")?;
            let spec = ContainerSpec {
                name: container_name.to_string(),
                image: "alpine:3.20".to_string(),
                network: network_name.to_string(),
                network_mode: None,
                aliases: Vec::new(),
                env: Vec::new(),
                volumes: vec![VolumeMount {
                    source_kind: VolumeMountSourceKind::Bind,
                    host_path: host_dir.to_string_lossy().to_string(),
                    container_path: "/hostcheck".to_string(),
                    read_only: false,
                }],
                ports: Vec::new(),
                labels: live_labels(
                    Uuid::new_v4(),
                    container_name,
                    "elixir.live.windows",
                    "1.0.0",
                ),
                command: vec![
                    "sh".to_string(),
                    "-lc".to_string(),
                    "set -e; test -f /hostcheck/runtime-host-marker.txt; printf runtime > /hostcheck/runtime-container-marker.txt".to_string(),
                ],
                cap_add: Vec::new(),
                devices: Vec::new(),
                sysctls: HashMap::new(),
            };

            runtime.ensure_container(&spec).await?;
            let exit_code = docker_wait_exit_code(container_name)?;
            if exit_code != 0 {
                bail!(
                    "Windows runtime bind-mount helper exited with {exit_code}: {}",
                    docker_logs(container_name).unwrap_or_else(|_| String::new())
                );
            }

            let marker = std::fs::read_to_string(host_dir.join("runtime-container-marker.txt"))
                .context("reading Windows runtime bind-mount marker")?;
            assert_eq!(marker, "runtime");

            let state = runtime
                .describe_container_runtime_state(container_name)
                .await?
                .ok_or_else(|| anyhow::anyhow!("runtime bind-mount helper state missing"))?;
            assert!(
                state.mounts.iter().any(|mount| {
                    mount.mount_type == "bind"
                        && mount.destination == "/hostcheck"
                        && !mount.read_only
                }),
                "runtime bind-mount helper did not report the expected writable /hostcheck bind mount: {:?}",
                state.mounts
            );
            Ok(())
        }

        fn assert_host_docker_internal_resolves() -> Result<()> {
            let output = Command::new("docker")
                .args([
                    "run",
                    "--rm",
                    "alpine:3.20",
                    "sh",
                    "-lc",
                    "getent hosts host.docker.internal >/dev/null 2>&1 || nslookup host.docker.internal >/dev/null 2>&1",
                ])
                .output()
                .context("checking host.docker.internal from a Linux container")?;
            if !output.status.success() {
                bail!(
                    "host.docker.internal did not resolve from a Linux container: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                );
            }
            Ok(())
        }

        fn live_download_network_tests_enabled() -> bool {
            std::env::var("ELIXIR_LIVE_DOWNLOAD_NETWORK_TESTS")
                .ok()
                .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        }

        fn docker_shared_tempdir(suffix: &str) -> Result<TempDir> {
            let root = std::env::current_dir()?
                .join("target")
                .join("live-docker-tests");
            std::fs::create_dir_all(&root)?;
            tempfile::Builder::new()
                .prefix(&format!("elixir-{suffix}-"))
                .tempdir_in(root)
                .context("creating Docker-shared tempdir")
        }

        struct VolumeCleanup {
            names: Vec<String>,
        }

        impl VolumeCleanup {
            fn new(names: Vec<String>) -> Self {
                Self { names }
            }
        }

        impl Drop for VolumeCleanup {
            fn drop(&mut self) {
                for name in &self.names {
                    let _ = Command::new("docker")
                        .arg("volume")
                        .arg("rm")
                        .arg("-f")
                        .arg(name)
                        .stdout(Stdio::null())
                        .stderr(Stdio::null())
                        .status();
                }
            }
        }

        struct NetworkCleanup {
            name: String,
        }

        impl NetworkCleanup {
            fn new(name: String) -> Self {
                Self { name }
            }
        }

        impl Drop for NetworkCleanup {
            fn drop(&mut self) {
                let _ = Command::new("docker")
                    .arg("network")
                    .arg("rm")
                    .arg(&self.name)
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
            }
        }

        fn docker_volume_create(name: &str) -> Result<()> {
            let output = Command::new("docker")
                .args(["volume", "create", name])
                .output()
                .with_context(|| format!("creating Docker volume '{name}'"))?;
            if !output.status.success() {
                bail!(
                    "docker volume create '{}' failed: {}",
                    name,
                    String::from_utf8_lossy(&output.stderr).trim()
                );
            }
            Ok(())
        }

        fn docker_exec_stdout(container: &str, args: &[&str]) -> Result<String> {
            let mut command = Command::new("docker");
            command.arg("exec").arg(container).args(args);
            let output = command
                .output()
                .with_context(|| format!("running docker exec in '{container}'"))?;
            if !output.status.success() {
                bail!(
                    "docker exec in '{}' failed: {}",
                    container,
                    String::from_utf8_lossy(&output.stderr).trim()
                );
            }
            Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
        }

        fn docker_wait_exit_code(container: &str) -> Result<i32> {
            let output = Command::new("docker")
                .args(["wait", container])
                .output()
                .with_context(|| format!("waiting for container '{container}'"))?;
            if !output.status.success() {
                bail!(
                    "docker wait failed for '{}': {}",
                    container,
                    String::from_utf8_lossy(&output.stderr).trim()
                );
            }
            String::from_utf8_lossy(&output.stdout)
                .trim()
                .parse::<i32>()
                .with_context(|| format!("parsing docker wait exit code for '{container}'"))
        }

        fn docker_run_alpine_with_mounts(mounts: &[String], script: &str) -> Result<()> {
            let mut args = vec!["run", "--rm"];
            for mount in mounts {
                args.push("-v");
                args.push(mount.as_str());
            }
            args.extend(["alpine:3.20", "sh", "-lc", script]);
            let output = Command::new("docker")
                .args(args)
                .output()
                .context("running disposable alpine helper")?;
            if !output.status.success() {
                bail!(
                    "disposable alpine helper failed: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                );
            }
            Ok(())
        }

        fn seed_qbittorrent_state(config_volume: &str, downloads_dir: &Path) -> Result<()> {
            docker_run_alpine_with_mounts(
                &[
                    format!("{config_volume}:/config"),
                    format!("{}:/downloads", downloads_dir.display()),
                ],
                "set -e; \
                 mkdir -p /config/qBittorrent/BT_backup /downloads/.incomplete; \
                 printf paused-torrent > /config/qBittorrent/BT_backup/paused.torrent; \
                 printf paused-fastresume > /config/qBittorrent/BT_backup/paused.fastresume; \
                 printf partial > /downloads/.incomplete/partial.data; \
                 printf complete > /downloads/completed.data",
            )
        }

        fn assert_qbittorrent_state_present(
            config_volume: &str,
            downloads_dir: &Path,
        ) -> Result<()> {
            docker_run_alpine_with_mounts(
                &[
                    format!("{config_volume}:/config:ro"),
                    format!("{}:/downloads:ro", downloads_dir.display()),
                ],
                "set -e; \
                 test -f /config/qBittorrent/BT_backup/paused.torrent; \
                 test -f /config/qBittorrent/BT_backup/paused.fastresume; \
                 test -f /downloads/.incomplete/partial.data; \
                 test -f /downloads/completed.data",
            )
        }

        fn seed_nzbget_state(
            config_volume: &str,
            runtime_volume: &str,
            downloads_dir: &Path,
        ) -> Result<()> {
            docker_run_alpine_with_mounts(
                &[
                    format!("{config_volume}:/config"),
                    format!("{runtime_volume}:/runtime"),
                    format!("{}:/downloads", downloads_dir.display()),
                ],
                "set -e; \
                 mkdir -p /config /runtime/queue /runtime/tmp /downloads/.incomplete; \
                 printf 'ControlUsername=elixir\\nControlPassword=secret\\n' > /config/nzbget.conf; \
                 printf queued > /runtime/queue/queue-state; \
                 printf runtime > /runtime/tmp/runtime-state; \
                 printf partial > /downloads/.incomplete/partial.nzb; \
                 printf complete > /downloads/completed.nzb",
            )
        }

        fn assert_nzbget_state_present(
            config_volume: &str,
            runtime_volume: &str,
            downloads_dir: &Path,
        ) -> Result<()> {
            docker_run_alpine_with_mounts(
                &[
                    format!("{config_volume}:/config:ro"),
                    format!("{runtime_volume}:/runtime:ro"),
                    format!("{}:/downloads:ro", downloads_dir.display()),
                ],
                "set -e; \
                 grep -q ControlUsername /config/nzbget.conf; \
                 test -f /runtime/queue/queue-state; \
                 test -f /runtime/tmp/runtime-state; \
                 test -f /downloads/.incomplete/partial.nzb; \
                 test -f /downloads/completed.nzb",
            )
        }

        fn live_qbittorrent_spec(
            name: &str,
            config_volume: &str,
            downloads_dir: &Path,
            network_mode: Option<&str>,
            instance_id: Uuid,
            network: &str,
        ) -> ContainerSpec {
            let image = std::env::var("ELIXIR_LIVE_QBITTORRENT_IMAGE")
                .unwrap_or_else(|_| "lscr.io/linuxserver/qbittorrent:latest".to_string());
            live_downloader_spec(
                name,
                image,
                vec![
                    VolumeMount {
                        source_kind: VolumeMountSourceKind::NamedVolume,
                        host_path: config_volume.to_string(),
                        container_path: "/config".to_string(),
                        read_only: false,
                    },
                    VolumeMount {
                        source_kind: VolumeMountSourceKind::Bind,
                        host_path: downloads_dir.to_string_lossy().to_string(),
                        container_path: "/downloads".to_string(),
                        read_only: false,
                    },
                ],
                network_mode,
                instance_id,
                "elixir.modules.qbittorrent",
                network,
            )
        }

        fn live_nzbget_spec(
            name: &str,
            config_volume: &str,
            runtime_volume: &str,
            downloads_dir: &Path,
            network_mode: Option<&str>,
            instance_id: Uuid,
            network: &str,
        ) -> ContainerSpec {
            let image = std::env::var("ELIXIR_LIVE_NZBGET_IMAGE")
                .unwrap_or_else(|_| "lscr.io/linuxserver/nzbget:latest".to_string());
            live_downloader_spec(
                name,
                image,
                vec![
                    VolumeMount {
                        source_kind: VolumeMountSourceKind::NamedVolume,
                        host_path: config_volume.to_string(),
                        container_path: "/config".to_string(),
                        read_only: false,
                    },
                    VolumeMount {
                        source_kind: VolumeMountSourceKind::NamedVolume,
                        host_path: runtime_volume.to_string(),
                        container_path: "/runtime".to_string(),
                        read_only: false,
                    },
                    VolumeMount {
                        source_kind: VolumeMountSourceKind::Bind,
                        host_path: downloads_dir.to_string_lossy().to_string(),
                        container_path: "/downloads".to_string(),
                        read_only: false,
                    },
                ],
                network_mode,
                instance_id,
                "elixir.modules.nzbget",
                network,
            )
        }

        fn live_downloader_spec(
            name: &str,
            image: String,
            volumes: Vec<VolumeMount>,
            network_mode: Option<&str>,
            instance_id: Uuid,
            extension_id: &str,
            network: &str,
        ) -> ContainerSpec {
            ContainerSpec {
                name: name.to_string(),
                image,
                network: network.to_string(),
                network_mode: network_mode.map(str::to_string),
                aliases: if network_mode.is_some() {
                    Vec::new()
                } else {
                    vec![format!("svc-{name}")]
                },
                env: vec![
                    EnvVar {
                        name: "PUID".to_string(),
                        value: "1000".to_string(),
                    },
                    EnvVar {
                        name: "PGID".to_string(),
                        value: "1000".to_string(),
                    },
                ],
                volumes,
                ports: Vec::new(),
                labels: live_labels(instance_id, name, extension_id, "1.0.0"),
                command: Vec::new(),
                cap_add: Vec::new(),
                devices: Vec::new(),
                sysctls: HashMap::new(),
            }
        }

        fn live_sleeping_container_spec(
            name: &str,
            aliases: Vec<String>,
            network_mode: Option<&str>,
            network: &str,
        ) -> ContainerSpec {
            ContainerSpec {
                name: name.to_string(),
                image: "alpine:3.20".to_string(),
                network: network.to_string(),
                network_mode: network_mode.map(str::to_string),
                aliases,
                env: Vec::new(),
                volumes: Vec::new(),
                ports: Vec::new(),
                labels: live_labels(Uuid::new_v4(), name, "elixir.live.test", "1.0.0"),
                command: vec![
                    "sh".to_string(),
                    "-lc".to_string(),
                    "trap 'exit 0' TERM INT; while true; do sleep 3600; done".to_string(),
                ],
                cap_add: Vec::new(),
                devices: Vec::new(),
                sysctls: HashMap::new(),
            }
        }

        fn live_http_echo_spec(name: &str, alias: &str, network: &str) -> ContainerSpec {
            ContainerSpec {
                name: name.to_string(),
                image: "hashicorp/http-echo:0.2.3".to_string(),
                network: network.to_string(),
                network_mode: None,
                aliases: vec![alias.to_string()],
                env: Vec::new(),
                volumes: Vec::new(),
                ports: Vec::new(),
                labels: live_labels(Uuid::new_v4(), name, "elixir.live.target", "1.0.0"),
                command: vec![
                    "-listen".to_string(),
                    ":8080".to_string(),
                    "-text".to_string(),
                    "ok".to_string(),
                ],
                cap_add: Vec::new(),
                devices: Vec::new(),
                sysctls: HashMap::new(),
            }
        }

        fn live_labels(
            instance_id: Uuid,
            instance_name: &str,
            extension_id: &str,
            version: &str,
        ) -> HashMap<String, String> {
            let mut labels = HashMap::new();
            labels.insert("elixir.managed".to_string(), "true".to_string());
            labels.insert("elixir.instance_id".to_string(), instance_id.to_string());
            labels.insert(
                "elixir.instance_name".to_string(),
                instance_name.to_string(),
            );
            labels.insert("elixir.extension_id".to_string(), extension_id.to_string());
            labels.insert("elixir.extension_version".to_string(), version.to_string());
            labels
        }

        fn inspect_network_mode(name: &str) -> Result<String> {
            let output = Command::new("docker")
                .args(["inspect", "--format", "{{.HostConfig.NetworkMode}}", name])
                .output()
                .with_context(|| format!("inspecting container '{}'", name))?;
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
                bail!("docker inspect failed for '{}': {}", name, stderr);
            }
            Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
        }

        fn docker_logs(name: &str) -> Result<String> {
            let output = Command::new("docker")
                .args(["logs", name])
                .output()
                .with_context(|| format!("reading logs for container '{}'", name))?;
            Ok(String::from_utf8_lossy(&output.stderr).to_string())
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

    #[test]
    fn resolve_volume_mount_makes_relative_placeholder_absolute() -> Result<()> {
        let paths = RuntimePaths {
            data_root: "data".to_string(),
            extensions_root: "data/extensions".to_string(),
            downloads_root: "data/downloads".to_string(),
            media_root: "media".to_string(),
        };
        let mount = resolve_volume_mount("{data}/community-app:/config", &paths)?;
        assert!(
            Path::new(&mount.host_path).is_absolute(),
            "expected absolute host path, got {}",
            mount.host_path
        );
        assert!(mount.host_path.ends_with("/data/community-app"));
        Ok(())
    }

    fn downloader_volume_test_paths() -> RuntimePaths {
        RuntimePaths {
            data_root: "/tmp/elixir/data".to_string(),
            extensions_root: "/tmp/elixir/data/extensions".to_string(),
            downloads_root: "/tmp/elixir/downloads".to_string(),
            media_root: "/tmp/elixir/media".to_string(),
        }
    }

    fn downloader_rehome_test_spec(
        name: &str,
        volumes: Vec<VolumeMount>,
        network_mode: Option<&str>,
    ) -> ContainerSpec {
        ContainerSpec {
            name: name.to_string(),
            image: "example/downloader:latest".to_string(),
            network: "elixir_net".to_string(),
            network_mode: network_mode.map(str::to_string),
            aliases: if network_mode.is_some() {
                Vec::new()
            } else {
                vec![format!("svc-{name}")]
            },
            env: Vec::new(),
            volumes,
            ports: if network_mode.is_some() {
                Vec::new()
            } else {
                vec![PortMapping {
                    container_port: 8080,
                    host_port: Some(0),
                    protocol: None,
                }]
            },
            labels: HashMap::new(),
            command: Vec::new(),
            cap_add: Vec::new(),
            devices: Vec::new(),
            sysctls: HashMap::new(),
        }
    }

    #[test]
    fn keep_downloader_paused_after_rehome_reads_network_or_runtime_config() {
        assert!(!keep_downloader_paused_after_rehome(None));
        assert!(keep_downloader_paused_after_rehome(Some(&json!({
            "network": {
                "keep_downloads_paused_after_rehome": true
            }
        }))));
        assert!(keep_downloader_paused_after_rehome(Some(&json!({
            "runtime": {
                "keep_downloads_paused_after_rehome": true
            }
        }))));
        assert!(!keep_downloader_paused_after_rehome(Some(&json!({
            "network": {
                "keep_downloads_paused_after_rehome": false
            },
            "runtime": {
                "keep_downloads_paused_after_rehome": true
            }
        }))));
    }

    #[test]
    fn prepare_runtime_volumes_rewrites_first_party_config_storage_to_named_volume() -> Result<()> {
        let paths = downloader_volume_test_paths();
        let instance_id =
            Uuid::parse_str("c1eaaec2-3dcf-40e4-85aa-adea48e4b221").expect("instance id");
        let prepared = prepare_runtime_volumes(
            "elixir.modules.nzbget",
            instance_id,
            &[
                "{data}/legacy-nzbget-config:/config".to_string(),
                "{downloads}:/downloads".to_string(),
            ],
            &paths,
        )?;

        let config_mount = prepared
            .volumes
            .iter()
            .find(|volume| volume.container_path == "/config")
            .expect("config mount");
        assert_eq!(config_mount.source_kind, VolumeMountSourceKind::NamedVolume);
        assert_eq!(config_mount.host_path, config_volume_name(instance_id));
        assert!(
            prepared
                .volumes
                .iter()
                .any(|volume| volume.container_path == "/runtime"
                    && volume.source_kind == VolumeMountSourceKind::NamedVolume)
        );

        Ok(())
    }

    #[test]
    fn prepare_runtime_volumes_injects_managed_config_mount_when_manifest_omits_config()
    -> Result<()> {
        let paths = downloader_volume_test_paths();
        let instance_id =
            Uuid::parse_str("b71d9e69-0290-5e73-b6c4-c9e771b993a6").expect("instance id");
        let prepared = prepare_runtime_volumes(
            "elixir.modules.sonarr",
            instance_id,
            &[
                "{downloads}:/downloads".to_string(),
                "{media}/tv:/tv".to_string(),
            ],
            &paths,
        )?;

        let config_mount = prepared
            .volumes
            .iter()
            .find(|volume| volume.container_path == "/config")
            .expect("config mount");
        assert_eq!(config_mount.source_kind, VolumeMountSourceKind::NamedVolume);
        assert_eq!(config_mount.host_path, config_volume_name(instance_id));
        Ok(())
    }

    #[test]
    fn prepare_runtime_volumes_keeps_qbittorrent_incomplete_on_downloads_mount() -> Result<()> {
        let paths = downloader_volume_test_paths();
        let instance_id =
            Uuid::parse_str("e88bc8bb-cd98-4742-8d21-c1c13e28f1e5").expect("instance id");
        let prepared = prepare_runtime_volumes(
            "elixir.modules.qbittorrent",
            instance_id,
            &["{downloads}:/downloads".to_string()],
            &paths,
        )?;

        assert!(
            prepared
                .volumes
                .iter()
                .any(|volume| volume.container_path == "/config"
                    && volume.source_kind == VolumeMountSourceKind::NamedVolume)
        );
        assert!(
            !prepared
                .volumes
                .iter()
                .any(|volume| volume.container_path == "/runtime"),
            "qBittorrent should complete from an incomplete directory on the downloads mount"
        );
        assert_eq!(
            required_named_runtime_directories("elixir.modules.qbittorrent", &prepared.volumes),
            vec![QBITTORRENT_INCOMPLETE_DIR.to_string()]
        );
        Ok(())
    }

    #[test]
    fn downloader_volume_preflight_allows_qbittorrent_network_mode_rehome() -> Result<()> {
        let paths = downloader_volume_test_paths();
        let instance_id =
            Uuid::parse_str("d78f4a2c-95f5-40a9-bf93-e0864b69a401").expect("instance id");
        let prepared = prepare_runtime_volumes(
            "elixir.modules.qbittorrent",
            instance_id,
            &["{downloads}:/downloads".to_string()],
            &paths,
        )?;

        let direct = downloader_rehome_test_spec("qbit", prepared.volumes.clone(), None);
        let rehomed = downloader_rehome_test_spec(
            "qbit",
            prepared.volumes.clone(),
            Some("container:qbit-vpn"),
        );

        assert_ne!(direct.network_mode, rehomed.network_mode);
        validate_downloader_volume_preservation(
            "elixir.modules.qbittorrent",
            &direct.volumes,
            &rehomed.volumes,
        )?;
        let config_volume = config_volume_name(instance_id);
        assert_eq!(
            rehomed
                .volumes
                .iter()
                .find(|volume| volume.container_path == "/config")
                .map(|volume| (&volume.source_kind, volume.host_path.as_str())),
            Some((&VolumeMountSourceKind::NamedVolume, config_volume.as_str()))
        );
        assert_eq!(
            rehomed
                .volumes
                .iter()
                .find(|volume| volume.container_path == DOWNLOADS_ROOT)
                .map(|volume| (&volume.source_kind, volume.host_path.as_str())),
            Some((&VolumeMountSourceKind::Bind, paths.downloads_root.as_str()))
        );
        Ok(())
    }

    #[test]
    fn downloader_volume_preflight_allows_nzbget_network_mode_rehome() -> Result<()> {
        let paths = downloader_volume_test_paths();
        let instance_id =
            Uuid::parse_str("4c5565b5-91d0-4ec4-b4a5-e1471c5342eb").expect("instance id");
        let prepared = prepare_runtime_volumes(
            "elixir.modules.nzbget",
            instance_id,
            &["{downloads}:/downloads".to_string()],
            &paths,
        )?;

        let direct = downloader_rehome_test_spec("nzbget", prepared.volumes.clone(), None);
        let rehomed = downloader_rehome_test_spec(
            "nzbget",
            prepared.volumes.clone(),
            Some("container:nzbget-vpn"),
        );

        assert_ne!(direct.network_mode, rehomed.network_mode);
        validate_downloader_volume_preservation(
            "elixir.modules.nzbget",
            &direct.volumes,
            &rehomed.volumes,
        )?;
        let config_volume = config_volume_name(instance_id);
        let runtime_volume = runtime_volume_name(instance_id);
        assert_eq!(
            rehomed
                .volumes
                .iter()
                .find(|volume| volume.container_path == "/config")
                .map(|volume| (&volume.source_kind, volume.host_path.as_str())),
            Some((&VolumeMountSourceKind::NamedVolume, config_volume.as_str()))
        );
        assert_eq!(
            rehomed
                .volumes
                .iter()
                .find(|volume| volume.container_path == "/runtime")
                .map(|volume| (&volume.source_kind, volume.host_path.as_str())),
            Some((&VolumeMountSourceKind::NamedVolume, runtime_volume.as_str()))
        );
        assert_eq!(
            rehomed
                .volumes
                .iter()
                .find(|volume| volume.container_path == DOWNLOADS_ROOT)
                .map(|volume| (&volume.source_kind, volume.host_path.as_str())),
            Some((&VolumeMountSourceKind::Bind, paths.downloads_root.as_str()))
        );
        Ok(())
    }

    #[test]
    fn downloader_network_mode_accepts_docker_resolved_gateway_id() {
        let gateway_id = "bcb7b296654532afba9fb825f44689f707bd5bd30bb6977404008072242cedde";

        assert!(
            !downloader_network_mode_requires_rehome_with_gateway_identity(
                Some("container:bcb7b296654532afba9fb825f44689f707bd5bd30bb6977404008072242cedde"),
                Some("container:elx-ba4bf0-vpn"),
                Some(gateway_id),
            )
        );
        assert!(
            !downloader_network_mode_requires_rehome_with_gateway_identity(
                Some("container:bcb7b2966545"),
                Some("container:elx-ba4bf0-vpn"),
                Some(gateway_id),
            )
        );
        assert!(
            downloader_network_mode_requires_rehome_with_gateway_identity(
                Some("container:other-vpn"),
                Some("container:elx-ba4bf0-vpn"),
                Some(gateway_id),
            )
        );
    }

    #[test]
    fn downloader_volume_preflight_allows_docker_desktop_bind_mount_aliases() -> Result<()> {
        let existing = vec![
            VolumeMount {
                source_kind: VolumeMountSourceKind::NamedVolume,
                host_path: "elixir_cfg_existing".to_string(),
                container_path: "/config".to_string(),
                read_only: false,
            },
            VolumeMount {
                source_kind: VolumeMountSourceKind::Bind,
                host_path: "/host_mnt/Users/ryanhotard/elixir/data/downloads".to_string(),
                container_path: DOWNLOADS_ROOT.to_string(),
                read_only: false,
            },
        ];
        let next = vec![
            VolumeMount {
                source_kind: VolumeMountSourceKind::NamedVolume,
                host_path: "elixir_cfg_existing".to_string(),
                container_path: "/config".to_string(),
                read_only: false,
            },
            VolumeMount {
                source_kind: VolumeMountSourceKind::Bind,
                host_path: "/Users/ryanhotard/elixir/data/downloads".to_string(),
                container_path: DOWNLOADS_ROOT.to_string(),
                read_only: false,
            },
        ];

        validate_downloader_volume_preservation("elixir.modules.qbittorrent", &existing, &next)
    }

    #[test]
    fn downloader_volume_preflight_rejects_changed_qbittorrent_config_volume() -> Result<()> {
        let paths = downloader_volume_test_paths();
        let instance_id =
            Uuid::parse_str("e95ee36e-a50a-4783-a45f-c88bb212ce50").expect("instance id");
        let prepared = prepare_runtime_volumes(
            "elixir.modules.qbittorrent",
            instance_id,
            &["{downloads}:/downloads".to_string()],
            &paths,
        )?;
        let mut changed = prepared.volumes.clone();
        let config_mount = changed
            .iter_mut()
            .find(|volume| volume.container_path == "/config")
            .expect("config mount");
        config_mount.host_path = "elixir_cfg_recreated".to_string();

        let err = validate_downloader_volume_preservation(
            "elixir.modules.qbittorrent",
            &prepared.volumes,
            &changed,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("/config mount would change"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[test]
    fn downloader_volume_preflight_rejects_changed_nzbget_runtime_volume() -> Result<()> {
        let paths = downloader_volume_test_paths();
        let instance_id =
            Uuid::parse_str("4f7d38c3-d77f-492f-90af-5cc2f672ef71").expect("instance id");
        let prepared = prepare_runtime_volumes(
            "elixir.modules.nzbget",
            instance_id,
            &["{downloads}:/downloads".to_string()],
            &paths,
        )?;
        let mut changed = prepared.volumes.clone();
        let runtime_mount = changed
            .iter_mut()
            .find(|volume| volume.container_path == "/runtime")
            .expect("runtime mount");
        runtime_mount.host_path = "elixir_rt_recreated".to_string();

        let err = validate_downloader_volume_preservation(
            "elixir.modules.nzbget",
            &prepared.volumes,
            &changed,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("/runtime mount would change"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[test]
    fn downloader_volume_preflight_rejects_changed_downloads_mount() -> Result<()> {
        let paths = downloader_volume_test_paths();
        let instance_id =
            Uuid::parse_str("92a87c6a-2871-4c3e-8318-0a74d07ecf33").expect("instance id");
        let prepared = prepare_runtime_volumes(
            "elixir.modules.qbittorrent",
            instance_id,
            &["{downloads}:/downloads".to_string()],
            &paths,
        )?;
        let mut changed = prepared.volumes.clone();
        let downloads_mount = changed
            .iter_mut()
            .find(|volume| volume.container_path == DOWNLOADS_ROOT)
            .expect("downloads mount");
        downloads_mount.host_path = "/tmp/elixir/new-downloads".to_string();

        let err = validate_downloader_volume_preservation(
            "elixir.modules.qbittorrent",
            &prepared.volumes,
            &changed,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("/downloads mount would change"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[test]
    fn merge_runtime_config_rewrites_named_config_storage_metadata() -> Result<()> {
        let updated = merge_runtime_config(
            Some(json!({
                "runtime": {
                    "config_dir": "/tmp/legacy",
                    "volumes": [{
                        "source_kind": "bind",
                        "host_path": "/tmp/legacy",
                        "container_path": "/config",
                        "read_only": false
                    }]
                }
            })),
            &[VolumeMount {
                source_kind: VolumeMountSourceKind::NamedVolume,
                host_path: "elixir_cfg_test".to_string(),
                container_path: "/config".to_string(),
                read_only: false,
            }],
        )?
        .expect("updated config");

        let runtime = updated
            .get("runtime")
            .and_then(serde_json::Value::as_object)
            .expect("runtime object");
        assert!(runtime.get("config_dir").is_none());
        assert_eq!(
            runtime
                .get("config_storage")
                .and_then(|value| value.get("source_kind"))
                .and_then(serde_json::Value::as_str),
            Some("named_volume")
        );
        assert_eq!(
            runtime
                .get("volumes")
                .and_then(serde_json::Value::as_array)
                .and_then(|volumes| volumes.first())
                .and_then(|volume| volume.get("source_kind"))
                .and_then(serde_json::Value::as_str),
            Some("named_volume")
        );
        Ok(())
    }

    #[test]
    fn downloader_managed_paths_keep_qbittorrent_completion_on_downloads_mount() {
        assert_eq!(qbittorrent_incomplete_path(), "/downloads/.incomplete");
        assert_eq!(nzbget_incomplete_path(), "/runtime/incomplete");
        assert_eq!(nzbget_nzb_dir(), "/runtime/nzb");
        assert_eq!(nzbget_queue_dir(), "/runtime/queue");
        assert_eq!(nzbget_temp_dir(), "/runtime/tmp");
    }

    #[test]
    fn compact_nzbget_config_text_deduplicates_keys_and_keeps_last_assignment() {
        let text = "\
# comment
DestDir=/downloads/old
InterDir=/runtime/incomplete
DestDir=/downloads
Category1.Name=tv
Category1.Name=tv
TempDir=/runtime/tmp
";
        let compacted = compact_nzbget_config_text(text);
        assert_eq!(
            compacted,
            "\
# comment
InterDir=/runtime/incomplete
DestDir=/downloads
Category1.Name=tv
TempDir=/runtime/tmp
"
        );
    }

    #[test]
    fn compact_nzbget_config_text_is_noop_when_config_is_unique() {
        let text = "\
DestDir=/downloads
InterDir=/runtime/incomplete
Category1.Name=tv
";
        assert_eq!(compact_nzbget_config_text(text), text);
    }

    #[test]
    fn render_openvpn_config_points_auth_user_pass_at_managed_secret_file() {
        let rendered = render_openvpn_config(
            "client\nauth-user-pass old-auth.txt\nremote 203.0.113.10 1194\n",
            true,
        );
        assert!(rendered.contains("auth-user-pass /gluetun/auth.txt"));
        assert!(!rendered.contains("old-auth.txt"));

        let appended = render_openvpn_config("client\nremote 203.0.113.10 1194\n", true);
        assert!(appended.ends_with("auth-user-pass /gluetun/auth.txt\n"));
    }
}
