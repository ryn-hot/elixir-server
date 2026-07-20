use std::{collections::BTreeMap, error::Error, fmt, sync::Arc, time::Duration};

use base64::{Engine as _, engine::general_purpose};
use rand_core::{OsRng, RngCore};
use serde::Serialize;
use sqlx::Row;
use tokio::sync::{Mutex, OnceCell, RwLock};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::{
    auth::revocation::{AuthorizationRevocationEvent, AuthorizationRevocationNotifier},
    config::RunEnvironment,
    db::models::SecretScope,
    extensions::store::ExtensionStore,
    secrets::SecretsManager,
};

use super::{
    admin::{DestinationRulePolicy, LiveAuditChain, LiveAuditKey, LiveDestinationRuleRepository},
    artwork::{LiveArtworkError, LiveArtworkService},
    catalog::LiveCatalogService,
    config::{LiveConfig, LiveConfigError},
    crypto::{LiveCrypto, LiveCryptoError, LiveMasterKey},
    diagnostics::LiveRedactor,
    egress::{LiveEgressError, LiveEgressService},
    lease::{ControlLease, ControlLeaseError, ControlLeaseRepository},
    provider::{LiveProviderClient, ProviderClientBuildError},
    relay::{LiveRelayBuildError, LiveRelayService},
    remux::{LiveRemuxBuildError, LiveRemuxError, LiveRemuxService},
    session::{LiveSessionLifecycle, LiveSessionRepository, SessionLifecycleError},
};

const ENVELOPE_SECRET_PREFIX: &str = "live.crypto.envelope.";
const TOKEN_HASH_SECRET_PREFIX: &str = "live.crypto.token_hash.";
const AUDIT_SECRET_PREFIX: &str = "live.crypto.audit.";

#[derive(Debug)]
pub enum LiveServiceError {
    InvalidConfig(LiveConfigError),
    SecretStore(anyhow::Error),
    Crypto(LiveCryptoError),
    Lease(ControlLeaseError),
    Provider(ProviderClientBuildError),
    Artwork(LiveArtworkError),
    Relay(LiveRelayBuildError),
    Remux(LiveRemuxBuildError),
    RemuxRuntime(LiveRemuxError),
    Lifecycle(SessionLifecycleError),
    ReconciliationTimeout,
    RemuxReconciliationTimeout,
    Egress(LiveEgressError),
}

impl fmt::Display for LiveServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(error) => write!(formatter, "invalid Live configuration: {error}"),
            Self::SecretStore(_) => {
                formatter.write_str("Live cryptographic key storage is unavailable")
            }
            Self::Crypto(error) => write!(
                formatter,
                "Live cryptography initialization failed: {error}"
            ),
            Self::Lease(error) => write!(
                formatter,
                "Live control lease initialization failed: {error}"
            ),
            Self::Provider(error) => write!(
                formatter,
                "Live provider subsystem initialization failed: {error}"
            ),
            Self::Artwork(error) => write!(
                formatter,
                "Live artwork subsystem initialization failed: {error}"
            ),
            Self::Relay(_) => {
                formatter.write_str("Live relay subsystem initialization failed closed")
            }
            Self::Remux(_) => {
                formatter.write_str("Live remux subsystem initialization failed closed")
            }
            Self::RemuxRuntime(_) => {
                formatter.write_str("Live remux startup reconciliation failed closed")
            }
            Self::Lifecycle(error) => write!(
                formatter,
                "Live session lifecycle initialization failed: {error}"
            ),
            Self::ReconciliationTimeout => {
                formatter.write_str("Live startup reconciliation exceeded its deadline")
            }
            Self::RemuxReconciliationTimeout => {
                formatter.write_str("Live remux startup reconciliation exceeded its deadline")
            }
            Self::Egress(_) => {
                formatter.write_str("Live protected-egress initialization failed closed")
            }
        }
    }
}

impl Error for LiveServiceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidConfig(error) => Some(error),
            Self::SecretStore(error) => Some(error.as_ref()),
            Self::Crypto(error) => Some(error),
            Self::Lease(error) => Some(error),
            Self::Provider(error) => Some(error),
            Self::Artwork(error) => Some(error),
            Self::Relay(_) => None,
            Self::Remux(_) | Self::RemuxRuntime(_) => None,
            Self::Lifecycle(error) => Some(error),
            Self::Egress(error) => Some(error),
            Self::ReconciliationTimeout | Self::RemuxReconciliationTimeout => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LiveComponent {
    Catalog,
    Playback,
    ClientDirect,
    Relay,
    Remux,
    ProtectedEgress,
    StremioCompat,
    NativeDashRelay,
    LowLatencyHls,
    RtmpRemux,
    SrtRemux,
    PrivateLanSources,
}

impl LiveComponent {
    pub const ALL: [Self; 12] = [
        Self::Catalog,
        Self::Playback,
        Self::ClientDirect,
        Self::Relay,
        Self::Remux,
        Self::ProtectedEgress,
        Self::StremioCompat,
        Self::NativeDashRelay,
        Self::LowLatencyHls,
        Self::RtmpRemux,
        Self::SrtRemux,
        Self::PrivateLanSources,
    ];

    pub const fn flag_name(self) -> &'static str {
        match self {
            Self::Catalog => "catalog_enabled",
            Self::Playback => "playback_enabled",
            Self::ClientDirect => "client_direct_enabled",
            Self::Relay => "relay_enabled",
            Self::Remux => "remux_enabled",
            Self::ProtectedEgress => "protected_egress_enabled",
            Self::StremioCompat => "stremio_compat_enabled",
            Self::NativeDashRelay => "native_dash_relay_enabled",
            Self::LowLatencyHls => "low_latency_hls_enabled",
            Self::RtmpRemux => "rtmp_remux_enabled",
            Self::SrtRemux => "srt_remux_enabled",
            Self::PrivateLanSources => "allow_private_lan_sources",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveLifecycle {
    Uninitialized,
    Disabled,
    LeaseHeld,
    ControlReady,
    LeaseLost,
    Blocked,
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LiveFeatureStatus {
    pub flag: &'static str,
    pub raw_enabled: bool,
    pub effective_enabled: bool,
    pub dependency_ready: bool,
    pub disabled_reason: Option<&'static str>,
    pub certification_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LiveServiceSnapshot {
    pub raw_enabled: bool,
    pub effective_enabled: bool,
    pub ready: bool,
    pub lifecycle: LiveLifecycle,
    pub disabled_reason: Option<&'static str>,
    pub lease_generation: Option<i64>,
    pub features: Vec<LiveFeatureStatus>,
}

#[derive(Debug, Clone)]
struct ComponentState {
    ready: bool,
    disabled_reason: Option<&'static str>,
    certification_id: Option<String>,
}

impl Default for ComponentState {
    fn default() -> Self {
        Self {
            ready: false,
            disabled_reason: Some("runtime_not_initialized"),
            certification_id: None,
        }
    }
}

struct RuntimeState {
    lifecycle: LiveLifecycle,
    blocked_reason: Option<&'static str>,
    lease: Option<ControlLease>,
    crypto: Option<Arc<LiveCrypto>>,
    protected_egress_configured: bool,
    components: BTreeMap<LiveComponent, ComponentState>,
}

struct LoadedLiveCrypto {
    crypto: LiveCrypto,
    audit_key: LiveAuditKey,
}

struct StoredLiveKeyState {
    envelope_primary_key_id: String,
    token_hash_primary_key_id: String,
    audit_primary_key_id: String,
}

impl Default for RuntimeState {
    fn default() -> Self {
        Self {
            lifecycle: LiveLifecycle::Uninitialized,
            blocked_reason: Some("not_initialized"),
            lease: None,
            crypto: None,
            protected_egress_configured: false,
            components: LiveComponent::ALL
                .into_iter()
                .map(|component| (component, ComponentState::default()))
                .collect(),
        }
    }
}

pub struct LiveService {
    config: LiveConfig,
    environment: RunEnvironment,
    owner_instance_id: Uuid,
    pool: sqlx::AnyPool,
    secrets: Arc<SecretsManager>,
    runtime_manager: Option<Arc<dyn crate::runtime::RuntimeManager>>,
    lease_repository: ControlLeaseRepository,
    redactor: Arc<LiveRedactor>,
    allow_loopback_providers: bool,
    provider_client: OnceCell<Arc<LiveProviderClient>>,
    catalog_service: OnceCell<Arc<LiveCatalogService>>,
    artwork_service: OnceCell<Arc<LiveArtworkService>>,
    relay_service: OnceCell<Arc<LiveRelayService>>,
    remux_service: OnceCell<Arc<LiveRemuxService>>,
    egress_service: OnceCell<Arc<LiveEgressService>>,
    session_repository: OnceCell<Arc<LiveSessionRepository>>,
    session_lifecycle: OnceCell<Arc<LiveSessionLifecycle>>,
    admin_audit: OnceCell<Arc<LiveAuditChain>>,
    destination_rule_repository: OnceCell<Arc<LiveDestinationRuleRepository>>,
    initialize_lock: Mutex<()>,
    key_rotation_lock: Mutex<()>,
    runtime: RwLock<RuntimeState>,
}

impl LiveService {
    pub fn new(
        config: LiveConfig,
        environment: RunEnvironment,
        pool: sqlx::AnyPool,
        secrets: Arc<SecretsManager>,
    ) -> Self {
        Self::new_with_provider_policy(config, environment, pool, secrets, false, None)
    }

    pub fn new_with_runtime(
        config: LiveConfig,
        environment: RunEnvironment,
        pool: sqlx::AnyPool,
        secrets: Arc<SecretsManager>,
        runtime_manager: Arc<dyn crate::runtime::RuntimeManager>,
    ) -> Self {
        Self::new_with_provider_policy(
            config,
            environment,
            pool,
            secrets,
            false,
            Some(runtime_manager),
        )
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(
        config: LiveConfig,
        environment: RunEnvironment,
        pool: sqlx::AnyPool,
        secrets: Arc<SecretsManager>,
    ) -> Self {
        Self::new_with_provider_policy(config, environment, pool, secrets, true, None)
    }

    fn new_with_provider_policy(
        config: LiveConfig,
        environment: RunEnvironment,
        pool: sqlx::AnyPool,
        secrets: Arc<SecretsManager>,
        allow_loopback_providers: bool,
        runtime_manager: Option<Arc<dyn crate::runtime::RuntimeManager>>,
    ) -> Self {
        let lease_repository = ControlLeaseRepository::new(
            pool.clone(),
            Duration::from_secs(config.sessions.lease_seconds),
        );
        Self {
            config,
            environment,
            owner_instance_id: Uuid::new_v4(),
            pool,
            secrets,
            runtime_manager,
            lease_repository,
            redactor: Arc::new(LiveRedactor::default()),
            allow_loopback_providers,
            provider_client: OnceCell::new(),
            catalog_service: OnceCell::new(),
            artwork_service: OnceCell::new(),
            relay_service: OnceCell::new(),
            remux_service: OnceCell::new(),
            egress_service: OnceCell::new(),
            session_repository: OnceCell::new(),
            session_lifecycle: OnceCell::new(),
            admin_audit: OnceCell::new(),
            destination_rule_repository: OnceCell::new(),
            initialize_lock: Mutex::new(()),
            key_rotation_lock: Mutex::new(()),
            runtime: RwLock::new(RuntimeState::default()),
        }
    }

    pub fn config(&self) -> &LiveConfig {
        &self.config
    }

    pub fn redactor(&self) -> Arc<LiveRedactor> {
        self.redactor.clone()
    }

    pub fn provider_client(&self) -> Option<Arc<LiveProviderClient>> {
        self.provider_client.get().cloned()
    }

    pub fn catalog_service(&self) -> Option<Arc<LiveCatalogService>> {
        self.catalog_service.get().cloned()
    }

    pub fn artwork_service(&self) -> Option<Arc<LiveArtworkService>> {
        self.artwork_service.get().cloned()
    }

    pub fn session_repository(&self) -> Option<Arc<LiveSessionRepository>> {
        self.session_repository.get().cloned()
    }

    pub fn session_lifecycle(&self) -> Option<Arc<LiveSessionLifecycle>> {
        self.session_lifecycle.get().cloned()
    }

    pub fn relay_service(&self) -> Option<Arc<LiveRelayService>> {
        self.relay_service.get().cloned()
    }

    pub fn remux_service(&self) -> Option<Arc<LiveRemuxService>> {
        self.remux_service.get().cloned()
    }

    pub fn egress_service(&self) -> Option<Arc<LiveEgressService>> {
        self.egress_service.get().cloned()
    }

    pub(crate) async fn refresh_builtin_egress(
        &self,
    ) -> Result<Option<Arc<LiveEgressService>>, LiveServiceError> {
        let _guard = self.initialize_lock.lock().await;
        let Some(egress) = self.egress_service() else {
            return Ok(None);
        };
        let enabled = egress.refresh_builtin_profile().await;
        let (fencing_token, already_ready) = {
            let mut runtime = self.runtime.write().await;
            runtime.protected_egress_configured = enabled;
            let already_ready = runtime
                .components
                .get(&LiveComponent::ProtectedEgress)
                .is_some_and(|component| component.ready);
            let fencing_token = (runtime.lifecycle == LiveLifecycle::ControlReady)
                .then(|| runtime.lease.as_ref().map(|lease| lease.fencing_token))
                .flatten();
            (fencing_token, already_ready)
        };
        if !enabled || already_ready {
            return Ok(Some(egress));
        }
        let Some(fencing_token) = fencing_token else {
            return Ok(Some(egress));
        };
        match tokio::time::timeout(
            Duration::from_secs(self.config.sessions.startup_queue_seconds),
            egress.reconcile_startup(fencing_token),
        )
        .await
        {
            Ok(Ok(_)) => {
                self.set_component_readiness(
                    LiveComponent::ProtectedEgress,
                    true,
                    None,
                    Some("live-egress-worker-v1".to_string()),
                )
                .await;
                Ok(Some(egress))
            }
            Ok(Err(error)) => Err(LiveServiceError::Egress(error)),
            Err(_) => Err(LiveServiceError::ReconciliationTimeout),
        }
    }

    pub fn admin_audit(&self) -> Option<Arc<LiveAuditChain>> {
        self.admin_audit.get().cloned()
    }

    pub fn destination_rule_repository(&self) -> Option<Arc<LiveDestinationRuleRepository>> {
        self.destination_rule_repository.get().cloned()
    }

    #[cfg(test)]
    pub(crate) fn allows_test_live_sources(&self) -> bool {
        self.allow_loopback_providers
    }

    pub async fn control_fencing_token(&self) -> Option<i64> {
        let runtime = self.runtime.read().await;
        (runtime.lifecycle == LiveLifecycle::ControlReady)
            .then(|| runtime.lease.as_ref().map(|lease| lease.fencing_token))
            .flatten()
    }

    pub async fn crypto(&self) -> Option<Arc<LiveCrypto>> {
        self.runtime.read().await.crypto.clone()
    }

    pub(crate) async fn key_rotation_guard(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.key_rotation_lock.lock().await
    }

    pub async fn initialize(&self) -> Result<LiveServiceSnapshot, LiveServiceError> {
        let _guard = self.initialize_lock.lock().await;
        self.config
            .validate()
            .map_err(LiveServiceError::InvalidConfig)?;
        if !self.config.enabled {
            let mut runtime = self.runtime.write().await;
            runtime.lifecycle = LiveLifecycle::Disabled;
            runtime.blocked_reason = Some("live_disabled");
            runtime.lease = None;
            runtime.crypto = None;
            return Ok(snapshot(&self.config, &runtime));
        }

        let provider_client = if self.config.catalog_enabled {
            Some(
                self.provider_client
                    .get_or_try_init(|| async {
                        #[cfg(test)]
                        {
                            if self.allow_loopback_providers {
                                return LiveProviderClient::new_for_test(
                                    self.pool.clone(),
                                    self.config.providers.clone(),
                                    self.redactor.clone(),
                                )
                                .map(Arc::new);
                            }
                        }
                        #[cfg(not(test))]
                        debug_assert!(!self.allow_loopback_providers);
                        LiveProviderClient::new(
                            self.pool.clone(),
                            self.config.providers.clone(),
                            self.redactor.clone(),
                        )
                        .map(Arc::new)
                    })
                    .await
                    .map_err(LiveServiceError::Provider)?
                    .clone(),
            )
        } else {
            None
        };

        let loaded_crypto = self.load_crypto().await?;
        let crypto = Arc::new(loaded_crypto.crypto);
        let audit = self
            .admin_audit
            .get_or_init(|| async move { Arc::new(LiveAuditChain::new(loaded_crypto.audit_key)) })
            .await
            .clone();
        self.destination_rule_repository
            .get_or_init(|| async move {
                Arc::new(LiveDestinationRuleRepository::new(
                    self.pool.clone(),
                    audit,
                    DestinationRulePolicy {
                        private_lan_enabled: self.config.allow_private_lan_sources,
                        rtmp_certified: self.config.rtmp_remux_enabled,
                        srt_certified: self.config.srt_remux_enabled,
                    },
                ))
            })
            .await;
        if self.config.playback_enabled {
            self.session_repository
                .get_or_init(|| async {
                    Arc::new(LiveSessionRepository::new(
                        self.pool.clone(),
                        crypto.clone(),
                        self.config.sessions.clone(),
                    ))
                })
                .await;
        }
        if self.egress_service.get().is_none() {
            if let Some(runtime) = self.runtime_manager.clone() {
                if let Some(service) = LiveEgressService::new_with_builtin_fallback(
                    self.pool.clone(),
                    runtime,
                    self.config.egress.clone(),
                    self.admin_audit()
                        .expect("Live audit initialized before protected egress"),
                    self.secrets.clone(),
                )
                .await
                .map_err(LiveServiceError::Egress)?
                {
                    self.egress_service
                        .get_or_init(|| async move { Arc::new(service) })
                        .await;
                }
            } else if self.config.protected_egress_enabled {
                return Err(LiveServiceError::Egress(LiveEgressError::Runtime));
            }
        }
        self.runtime.write().await.protected_egress_configured = self
            .egress_service()
            .is_some_and(|egress| egress.status().enabled);
        if self.config.relay_enabled {
            let repository = self
                .session_repository()
                .expect("playback repository initialized before relay service");
            let provider_client = provider_client
                .as_ref()
                .expect("provider client initialized before relay service")
                .clone();
            self.relay_service
                .get_or_try_init(|| async {
                    #[cfg(test)]
                    {
                        if self.allow_loopback_providers {
                            return LiveRelayService::new_for_test(
                                self.pool.clone(),
                                repository.clone(),
                                provider_client.clone(),
                                self.config.relay.clone(),
                            )
                            .map(Arc::new);
                        }
                    }
                    LiveRelayService::new(
                        self.pool.clone(),
                        repository,
                        provider_client,
                        self.config.relay.clone(),
                        self.config.allow_private_lan_sources,
                        self.egress_service(),
                    )
                    .map(Arc::new)
                })
                .await
                .map_err(LiveServiceError::Relay)?;
        }
        if self.config.remux_enabled {
            let repository = self
                .session_repository()
                .expect("playback repository initialized before remux service");
            let relay = self
                .relay_service()
                .expect("relay service initialized before remux service");
            self.remux_service
                .get_or_try_init(|| async {
                    LiveRemuxService::new(
                        repository,
                        relay,
                        self.redactor.clone(),
                        self.config.remux.clone(),
                        Duration::from_secs(self.config.sessions.startup_queue_seconds),
                    )
                    .map(Arc::new)
                })
                .await
                .map_err(LiveServiceError::Remux)?;
        }
        if self.config.catalog_enabled {
            let provider_client = provider_client
                .expect("catalog provider client initialized before catalog service");
            self.catalog_service
                .get_or_init(|| async {
                    Arc::new(LiveCatalogService::new(
                        self.pool.clone(),
                        crypto.clone(),
                        provider_client,
                    ))
                })
                .await;
            self.artwork_service
                .get_or_try_init(|| async {
                    #[cfg(test)]
                    {
                        if self.allow_loopback_providers {
                            let resolver =
                                super::upstream::SystemDnsResolver::new(Duration::from_secs(5))
                                    .map_err(|_| {
                                        LiveArtworkError::new_for_service_initialization()
                                    })?;
                            return LiveArtworkService::new_for_test(
                                self.pool.clone(),
                                Arc::new(resolver),
                                super::artwork::LiveArtworkLimits::default(),
                            )
                            .map(Arc::new);
                        }
                    }
                    LiveArtworkService::new(self.pool.clone()).map(Arc::new)
                })
                .await
                .map_err(LiveServiceError::Artwork)?;
        }
        match self.lease_repository.acquire(self.owner_instance_id).await {
            Ok(lease) => {
                let fencing_token = lease.fencing_token;
                let mut runtime = self.runtime.write().await;
                runtime.lifecycle = LiveLifecycle::ControlReady;
                runtime.blocked_reason = None;
                runtime.lease = Some(lease);
                runtime.crypto = Some(crypto);
                drop(runtime);

                if let Some(egress) = self
                    .egress_service()
                    .filter(|egress| egress.status().enabled)
                {
                    match tokio::time::timeout(
                        Duration::from_secs(self.config.sessions.startup_queue_seconds),
                        egress.reconcile_startup(fencing_token),
                    )
                    .await
                    {
                        Ok(Ok(released)) => {
                            tracing::info!(
                                released,
                                fencing_token,
                                "Live protected-egress startup reconciliation completed"
                            );
                            self.set_component_readiness(
                                LiveComponent::ProtectedEgress,
                                true,
                                None,
                                Some("live-egress-worker-v1".to_string()),
                            )
                            .await;
                        }
                        Ok(Err(error)) => {
                            self.block_and_release("egress_startup_reconciliation_failed")
                                .await;
                            return Err(LiveServiceError::Egress(error));
                        }
                        Err(_) => {
                            self.block_and_release("egress_startup_reconciliation_timeout")
                                .await;
                            return Err(LiveServiceError::ReconciliationTimeout);
                        }
                    }
                }

                if self.config.playback_enabled {
                    let repository = self
                        .session_repository()
                        .expect("playback repository initialized before lease acquisition");
                    let provider_client = self
                        .provider_client()
                        .expect("playback provider client initialized before reconciliation");
                    let lifecycle = self
                        .session_lifecycle
                        .get_or_init(|| async {
                            Arc::new(LiveSessionLifecycle::new(
                                self.pool.clone(),
                                repository,
                                provider_client,
                                self.owner_instance_id,
                            ))
                        })
                        .await
                        .clone();
                    let reconciliation = tokio::time::timeout(
                        Duration::from_secs(self.config.sessions.startup_queue_seconds),
                        lifecycle.reconcile_startup(fencing_token),
                    )
                    .await;
                    match reconciliation {
                        Ok(Ok(report)) => {
                            tracing::info!(
                                inspected = report.inspected,
                                adopted = report.adopted,
                                terminated = report.terminated,
                                revocations_consumed = report.revocations_consumed,
                                fencing_token,
                                "Live startup reconciliation completed"
                            );
                            self.set_component_readiness(LiveComponent::Playback, true, None, None)
                                .await;
                            if self.config.client_direct_enabled {
                                self.set_component_readiness(
                                    LiveComponent::ClientDirect,
                                    true,
                                    None,
                                    None,
                                )
                                .await;
                            }
                            if self.config.relay_enabled {
                                self.set_component_readiness(
                                    LiveComponent::Relay,
                                    true,
                                    None,
                                    None,
                                )
                                .await;
                                if self.config.allow_private_lan_sources {
                                    self.set_component_readiness(
                                        LiveComponent::PrivateLanSources,
                                        true,
                                        None,
                                        None,
                                    )
                                    .await;
                                }
                            }
                            if self.config.remux_enabled {
                                let remux = self
                                    .remux_service()
                                    .expect("remux service initialized before reconciliation");
                                if let Err(error) = remux.initialize().await {
                                    self.block_and_release("remux_runtime_initialization_failed")
                                        .await;
                                    return Err(LiveServiceError::Remux(error));
                                }
                                match tokio::time::timeout(
                                    Duration::from_secs(self.config.sessions.startup_queue_seconds),
                                    remux.reconcile_startup(fencing_token),
                                )
                                .await
                                {
                                    Ok(Ok(report)) => {
                                        tracing::info!(
                                            inspected = report.inspected,
                                            restarted = report.restarted,
                                            terminated = report.terminated,
                                            fencing_token,
                                            "Live remux startup reconciliation completed"
                                        );
                                        self.set_component_readiness(
                                            LiveComponent::Remux,
                                            true,
                                            None,
                                            Some("mpeg-ts-dash-copy-v1".to_string()),
                                        )
                                        .await;
                                    }
                                    Ok(Err(error)) => {
                                        self.block_and_release(
                                            "remux_startup_reconciliation_failed",
                                        )
                                        .await;
                                        return Err(LiveServiceError::RemuxRuntime(error));
                                    }
                                    Err(_) => {
                                        self.block_and_release(
                                            "remux_startup_reconciliation_timeout",
                                        )
                                        .await;
                                        return Err(LiveServiceError::RemuxReconciliationTimeout);
                                    }
                                }
                            }
                        }
                        Ok(Err(error)) => {
                            self.block_and_release("startup_reconciliation_failed")
                                .await;
                            return Err(LiveServiceError::Lifecycle(error));
                        }
                        Err(_) => {
                            self.block_and_release("startup_reconciliation_timeout")
                                .await;
                            return Err(LiveServiceError::ReconciliationTimeout);
                        }
                    }
                }
            }
            Err(ControlLeaseError::Held) => {
                let mut runtime = self.runtime.write().await;
                runtime.lifecycle = LiveLifecycle::LeaseHeld;
                runtime.blocked_reason = Some("control_lease_held");
                runtime.lease = None;
                runtime.crypto = Some(crypto);
            }
            Err(ControlLeaseError::FenceExhausted) => {
                let mut runtime = self.runtime.write().await;
                runtime.lifecycle = LiveLifecycle::Blocked;
                runtime.blocked_reason = Some("control_fence_exhausted");
                runtime.lease = None;
                runtime.crypto = Some(crypto);
            }
            Err(error) => {
                let mut runtime = self.runtime.write().await;
                runtime.lifecycle = LiveLifecycle::Blocked;
                runtime.blocked_reason = Some("control_lease_error");
                runtime.lease = None;
                runtime.crypto = Some(crypto);
                return Err(LiveServiceError::Lease(error));
            }
        }
        if self.config.catalog_enabled {
            self.set_component_readiness(LiveComponent::Catalog, true, None, None)
                .await;
        }
        Ok(self.snapshot().await)
    }

    pub async fn snapshot(&self) -> LiveServiceSnapshot {
        let runtime = self.runtime.read().await;
        snapshot(&self.config, &*runtime)
    }

    pub async fn set_component_readiness(
        &self,
        component: LiveComponent,
        ready: bool,
        disabled_reason: Option<&'static str>,
        certification_id: Option<String>,
    ) {
        let certification_id = certification_id.filter(|value| {
            !value.is_empty()
                && value.len() <= 128
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        });
        self.runtime.write().await.components.insert(
            component,
            ComponentState {
                ready,
                disabled_reason: if ready { None } else { disabled_reason },
                certification_id,
            },
        );
    }

    pub async fn run_lease_heartbeat(self: Arc<Self>, shutdown: CancellationToken) {
        if !self.config.enabled {
            return;
        }
        let interval = self.lease_repository.heartbeat_interval();
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    self.release_on_shutdown().await;
                    return;
                }
                _ = tokio::time::sleep(interval) => {
                    let lease = self.runtime.read().await.lease.clone();
                    let Some(lease) = lease else {
                        return;
                    };
                    match self.lease_repository.renew(&lease).await {
                        Ok(Some(renewed)) => {
                            let renewed_fencing_token = renewed.fencing_token;
                            self.runtime.write().await.lease = Some(renewed);
                            if let Some(artwork) = self.artwork_service() {
                                if let Err(error) = artwork.expire_batch(256).await {
                                    tracing::warn!(error = %error, "Live artwork expiry maintenance failed");
                                }
                                if let Err(error) = artwork.reconcile_removed_providers_batch(64).await {
                                    tracing::warn!(error = %error, "Live artwork provider cleanup failed");
                                }
                            }
                            if let Some(relay) = self.relay_service() {
                                relay.reap_stale().await;
                            }
                            if let Some(remux) = self.remux_service() {
                                remux.reap_stale().await;
                            }
                            if let Some(egress) = self.egress_service()
                                && let Err(error) = egress.reap_stale(renewed_fencing_token).await
                            {
                                tracing::error!(error = %error, "Live egress maintenance failed closed");
                                self.mark_lease_lost("egress_maintenance_failed").await;
                                return;
                            }
                            if let Err(error) = crate::live::metrics::refresh_database_gauges(&self.pool).await {
                                tracing::warn!(error = %error, "Live metric gauge reconciliation failed");
                            }
                        }
                        Ok(None) => {
                            self.mark_lease_lost("control_lease_lost").await;
                            return;
                        }
                        Err(error) => {
                            tracing::error!(error = %error, "Live control lease heartbeat failed closed");
                            self.mark_lease_lost("control_lease_error").await;
                            return;
                        }
                    }
                }
            }
        }
    }

    pub async fn run_session_lifecycle(
        self: Arc<Self>,
        notifier: AuthorizationRevocationNotifier,
        shutdown: CancellationToken,
    ) {
        let Some(lifecycle) = self.session_lifecycle() else {
            return;
        };
        let Some(fencing_token) = self.control_fencing_token().await else {
            return;
        };
        let shutdown_observer = shutdown.clone();
        if let Err(error) = lifecycle.run(fencing_token, notifier, shutdown).await {
            if shutdown_observer.is_cancelled() {
                return;
            }
            tracing::error!(error = %error, "Live session lifecycle failed closed");
            self.block_and_release("session_lifecycle_failed").await;
        }
    }

    pub async fn apply_authorization_revocation(
        &self,
        event: &AuthorizationRevocationEvent,
    ) -> Result<(), LiveServiceError> {
        let (Some(lifecycle), Some(fencing_token)) =
            (self.session_lifecycle(), self.control_fencing_token().await)
        else {
            return Ok(());
        };
        lifecycle
            .apply_revocation(event, fencing_token)
            .await
            .map_err(LiveServiceError::Lifecycle)?;
        if let Some(relay) = self.relay_service() {
            relay.reap_stale().await;
        }
        if let Some(remux) = self.remux_service() {
            remux.reap_stale().await;
        }
        if let Some(egress) = self.egress_service() {
            egress
                .reap_stale(fencing_token)
                .await
                .map_err(LiveServiceError::Egress)?;
        }
        Ok(())
    }

    pub async fn drain_authorization_revocations(&self) -> Result<u64, LiveServiceError> {
        let (Some(lifecycle), Some(fencing_token)) =
            (self.session_lifecycle(), self.control_fencing_token().await)
        else {
            return Ok(0);
        };
        let consumed = lifecycle
            .drain_revocations(fencing_token, None)
            .await
            .map_err(LiveServiceError::Lifecycle)?;
        if let Some(relay) = self.relay_service() {
            relay.reap_stale().await;
        }
        if let Some(remux) = self.remux_service() {
            remux.reap_stale().await;
        }
        if let Some(egress) = self.egress_service() {
            egress
                .reap_stale(fencing_token)
                .await
                .map_err(LiveServiceError::Egress)?;
        }
        Ok(consumed)
    }

    async fn mark_lease_lost(&self, reason: &'static str) {
        if let Some(relay) = self.relay_service() {
            relay.cancel_all();
        }
        if let Some(remux) = self.remux_service() {
            remux.cancel_all().await;
        }
        if let (Some(egress), Some(lease)) = (
            self.egress_service(),
            self.runtime.read().await.lease.clone(),
        ) {
            egress.cancel_all(lease.fencing_token).await;
        }
        let lease = self.runtime.read().await.lease.clone();
        if let Some(lease) = lease {
            if let Err(error) = self.lease_repository.release(&lease).await {
                tracing::error!(error = %error, "Live lost-lease release failed");
            }
        }
        let mut runtime = self.runtime.write().await;
        runtime.lifecycle = LiveLifecycle::LeaseLost;
        runtime.blocked_reason = Some(reason);
        runtime.lease = None;
    }

    async fn block_and_release(&self, reason: &'static str) {
        if let Some(relay) = self.relay_service() {
            relay.cancel_all();
        }
        if let Some(remux) = self.remux_service() {
            remux.cancel_all().await;
        }
        if let (Some(egress), Some(lease)) = (
            self.egress_service(),
            self.runtime.read().await.lease.clone(),
        ) {
            egress.cancel_all(lease.fencing_token).await;
        }
        let lease = self.runtime.read().await.lease.clone();
        if let Some(lease) = lease {
            if let Err(error) = self.lease_repository.release(&lease).await {
                tracing::error!(error = %error, "Live control lease fail-closed release failed");
            }
        }
        let mut runtime = self.runtime.write().await;
        runtime.lifecycle = LiveLifecycle::Blocked;
        runtime.blocked_reason = Some(reason);
        runtime.lease = None;
    }

    async fn release_on_shutdown(&self) {
        if let Some(relay) = self.relay_service() {
            relay.cancel_all();
        }
        if let Some(remux) = self.remux_service() {
            remux.cancel_all().await;
        }
        if let (Some(egress), Some(lease)) = (
            self.egress_service(),
            self.runtime.read().await.lease.clone(),
        ) {
            egress.cancel_all(lease.fencing_token).await;
        }
        let lease = self.runtime.read().await.lease.clone();
        if let Some(lease) = lease {
            if let Err(error) = self.lease_repository.release(&lease).await {
                tracing::error!(error = %error, "Live control lease release failed");
            }
        }
        let mut runtime = self.runtime.write().await;
        runtime.lifecycle = LiveLifecycle::Stopped;
        runtime.blocked_reason = Some("server_stopped");
        runtime.lease = None;
    }

    async fn load_crypto(&self) -> Result<LoadedLiveCrypto, LiveServiceError> {
        if self.environment == RunEnvironment::Development {
            for key in [
                format!(
                    "{ENVELOPE_SECRET_PREFIX}{}",
                    self.config.crypto.primary_envelope_key_id
                ),
                format!(
                    "{TOKEN_HASH_SECRET_PREFIX}{}",
                    self.config.crypto.primary_token_hash_key_id
                ),
                format!(
                    "{AUDIT_SECRET_PREFIX}{}",
                    self.config.crypto.primary_audit_key_id
                ),
            ] {
                self.ensure_development_key(&key).await?;
            }
        }

        let key_state = self.load_key_state().await?;

        let store = ExtensionStore::new(&self.pool);
        let secrets = store
            .list_secrets(Some(SecretScope::Global), None, None)
            .await
            .map_err(LiveServiceError::SecretStore)?;
        let mut envelope_keys = Vec::new();
        let mut token_hash_keys = Vec::new();
        let mut audit_primary = None;
        for secret in secrets {
            if let Some(key_id) = secret.key.strip_prefix(ENVELOPE_SECRET_PREFIX) {
                envelope_keys.push(self.decrypt_master_key(key_id, &secret.value_encrypted)?);
            } else if let Some(key_id) = secret.key.strip_prefix(TOKEN_HASH_SECRET_PREFIX) {
                token_hash_keys.push(self.decrypt_master_key(key_id, &secret.value_encrypted)?);
            } else if let Some(key_id) = secret.key.strip_prefix(AUDIT_SECRET_PREFIX) {
                let key = self.decrypt_audit_key(key_id, &secret.value_encrypted)?;
                if key.key_id() == key_state.audit_primary_key_id {
                    audit_primary = Some(key);
                }
            }
        }
        let audit_key = audit_primary.ok_or(LiveServiceError::Crypto(
            LiveCryptoError::InvalidConfiguration("primary audit key is not configured"),
        ))?;
        let crypto = LiveCrypto::new_with_domain_keys(
            key_state.envelope_primary_key_id,
            envelope_keys,
            key_state.token_hash_primary_key_id,
            token_hash_keys,
        )
        .map_err(LiveServiceError::Crypto)?;
        Ok(LoadedLiveCrypto { crypto, audit_key })
    }

    async fn load_key_state(&self) -> Result<StoredLiveKeyState, LiveServiceError> {
        sqlx::query(
            "INSERT INTO live_key_rotation_state (
                state_id, envelope_primary_key_id, token_hash_primary_key_id,
                audit_primary_key_id
             ) VALUES ('live-crypto-v1', $1, $2, $3)
             ON CONFLICT(state_id) DO NOTHING",
        )
        .bind(&self.config.crypto.primary_envelope_key_id)
        .bind(&self.config.crypto.primary_token_hash_key_id)
        .bind(&self.config.crypto.primary_audit_key_id)
        .execute(&self.pool)
        .await
        .map_err(|error| LiveServiceError::SecretStore(error.into()))?;
        let row = sqlx::query(
            "SELECT envelope_primary_key_id, token_hash_primary_key_id,
                    audit_primary_key_id, revision
             FROM live_key_rotation_state WHERE state_id = 'live-crypto-v1'",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| LiveServiceError::SecretStore(error.into()))?
        .ok_or_else(|| {
            LiveServiceError::Crypto(LiveCryptoError::InvalidConfiguration(
                "Live key rotation state is missing",
            ))
        })?;
        let revision: i64 = row
            .try_get("revision")
            .map_err(|error| LiveServiceError::SecretStore(error.into()))?;
        if revision < 1 {
            return Err(LiveServiceError::Crypto(
                LiveCryptoError::InvalidConfiguration("Live key rotation revision is invalid"),
            ));
        }
        Ok(StoredLiveKeyState {
            envelope_primary_key_id: row
                .try_get("envelope_primary_key_id")
                .map_err(|error| LiveServiceError::SecretStore(error.into()))?,
            token_hash_primary_key_id: row
                .try_get("token_hash_primary_key_id")
                .map_err(|error| LiveServiceError::SecretStore(error.into()))?,
            audit_primary_key_id: row
                .try_get("audit_primary_key_id")
                .map_err(|error| LiveServiceError::SecretStore(error.into()))?,
        })
    }

    fn decrypt_master_key(
        &self,
        key_id: &str,
        encrypted: &str,
    ) -> Result<LiveMasterKey, LiveServiceError> {
        if !SecretsManager::is_encrypted(encrypted) {
            return Err(LiveServiceError::Crypto(
                LiveCryptoError::InvalidConfiguration("Live master key is not encrypted"),
            ));
        }
        let plaintext = self
            .secrets
            .decrypt(encrypted)
            .map_err(LiveServiceError::SecretStore)?;
        let plaintext = Zeroizing::new(plaintext);
        LiveMasterKey::from_base64(key_id, plaintext.as_str()).map_err(LiveServiceError::Crypto)
    }

    fn decrypt_audit_key(
        &self,
        key_id: &str,
        encrypted: &str,
    ) -> Result<LiveAuditKey, LiveServiceError> {
        if !SecretsManager::is_encrypted(encrypted) {
            return Err(LiveServiceError::Crypto(
                LiveCryptoError::InvalidConfiguration("Live audit key is not encrypted"),
            ));
        }
        let plaintext = self
            .secrets
            .decrypt(encrypted)
            .map_err(LiveServiceError::SecretStore)?;
        let plaintext = Zeroizing::new(plaintext);
        let decoded = general_purpose::STANDARD
            .decode(plaintext.trim())
            .map(Zeroizing::new)
            .map_err(|_| {
                LiveServiceError::Crypto(LiveCryptoError::InvalidConfiguration(
                    "audit key is not base64",
                ))
            })?;
        if decoded.len() != 32 {
            return Err(LiveServiceError::Crypto(
                LiveCryptoError::InvalidConfiguration("audit key must contain exactly 32 bytes"),
            ));
        }
        let mut material = [0u8; 32];
        material.copy_from_slice(decoded.as_slice());
        LiveAuditKey::new(key_id, material).map_err(|_| {
            LiveServiceError::Crypto(LiveCryptoError::InvalidConfiguration(
                "audit key identifier is invalid",
            ))
        })
    }

    async fn ensure_development_key(&self, key: &str) -> Result<(), LiveServiceError> {
        let mut material = Zeroizing::new([0u8; 32]);
        OsRng
            .try_fill_bytes(material.as_mut())
            .map_err(|_| LiveServiceError::Crypto(LiveCryptoError::EncryptionFailed))?;
        let encoded = Zeroizing::new(general_purpose::STANDARD.encode(material.as_ref()));
        let encrypted = self
            .secrets
            .encrypt(encoded.as_str())
            .map_err(LiveServiceError::SecretStore)?;
        for attempt in 0..8u64 {
            let result = sqlx::query(
                "INSERT INTO secrets
                    (secret_id, scope, scope_id, key, value_encrypted, rotatable)
                 VALUES ($1, 'global', NULL, $2, $3, TRUE)
                 ON CONFLICT DO NOTHING",
            )
            .bind(Uuid::new_v4().to_string())
            .bind(key)
            .bind(&encrypted)
            .execute(&self.pool)
            .await;
            match result {
                Ok(_) => return Ok(()),
                Err(error) if attempt < 7 && transient_database_lock(&error) => {
                    tokio::time::sleep(Duration::from_millis(5 * (attempt + 1))).await;
                }
                Err(error) => return Err(LiveServiceError::SecretStore(error.into())),
            }
        }
        Err(LiveServiceError::Crypto(LiveCryptoError::EncryptionFailed))
    }
}

fn transient_database_lock(error: &sqlx::Error) -> bool {
    let sqlx::Error::Database(database) = error else {
        return false;
    };
    matches!(
        database.code().as_deref(),
        Some("5" | "6" | "40001" | "40P01" | "SQLITE_BUSY" | "SQLITE_LOCKED")
    )
}

fn snapshot(config: &LiveConfig, runtime: &RuntimeState) -> LiveServiceSnapshot {
    let effective_enabled = config.enabled && runtime.lifecycle == LiveLifecycle::ControlReady;
    let features = LiveComponent::ALL
        .into_iter()
        .map(|component| {
            let raw_enabled = component_raw_enabled(config, runtime, component);
            let state = runtime
                .components
                .get(&component)
                .cloned()
                .unwrap_or_default();
            let effective = effective_enabled && raw_enabled && state.ready;
            LiveFeatureStatus {
                flag: component.flag_name(),
                raw_enabled,
                effective_enabled: effective,
                dependency_ready: state.ready,
                disabled_reason: if !raw_enabled {
                    Some("flag_disabled")
                } else if !effective_enabled {
                    runtime.blocked_reason
                } else if !state.ready {
                    state.disabled_reason.or(Some("runtime_not_ready"))
                } else {
                    None
                },
                certification_id: state.certification_id,
            }
        })
        .collect::<Vec<_>>();
    let selected_count = features
        .iter()
        .filter(|feature| feature.raw_enabled)
        .count();
    let ready = selected_count > 0
        && features
            .iter()
            .filter(|feature| feature.raw_enabled)
            .all(|feature| feature.effective_enabled);
    let disabled_reason = if !config.enabled {
        Some("live_disabled")
    } else if !effective_enabled {
        runtime.blocked_reason
    } else if selected_count == 0 {
        Some("no_live_surface_enabled")
    } else if !ready {
        Some("selected_surface_not_ready")
    } else {
        None
    };
    LiveServiceSnapshot {
        raw_enabled: config.enabled,
        effective_enabled,
        ready,
        lifecycle: runtime.lifecycle,
        disabled_reason,
        lease_generation: runtime.lease.as_ref().map(|lease| lease.fencing_token),
        features,
    }
}

fn component_raw_enabled(
    config: &LiveConfig,
    runtime: &RuntimeState,
    component: LiveComponent,
) -> bool {
    match component {
        LiveComponent::Catalog => config.catalog_enabled,
        LiveComponent::Playback => config.playback_enabled,
        LiveComponent::ClientDirect => config.client_direct_enabled,
        LiveComponent::Relay => config.relay_enabled,
        LiveComponent::Remux => config.remux_enabled,
        LiveComponent::ProtectedEgress => {
            config.protected_egress_enabled || runtime.protected_egress_configured
        }
        LiveComponent::StremioCompat => config.stremio_compat_enabled,
        LiveComponent::NativeDashRelay => config.native_dash_relay_enabled,
        LiveComponent::LowLatencyHls => config.low_latency_hls_enabled,
        LiveComponent::RtmpRemux => config.rtmp_remux_enabled,
        LiveComponent::SrtRemux => config.srt_remux_enabled,
        LiveComponent::PrivateLanSources => config.allow_private_lan_sources,
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use serde_json::json;

    use crate::{config::DatabaseConfig, db::Database, live::provider::tests::seed_provider};

    use super::*;

    async fn test_database() -> Result<Database> {
        let database = Database::connect(&DatabaseConfig {
            url: format!(
                "sqlite:file:s10-live-service-{}?mode=memory&cache=shared",
                Uuid::new_v4()
            ),
            max_connections: 4,
            connect_timeout_seconds: 5,
        })
        .await?;
        database.run_migrations().await?;
        Ok(database)
    }

    #[test]
    fn auto_created_egress_is_visible_to_readiness_and_planner_snapshots() {
        let config = LiveConfig {
            enabled: true,
            ..LiveConfig::default()
        };
        let mut runtime = RuntimeState::default();
        runtime.lifecycle = LiveLifecycle::ControlReady;
        runtime.blocked_reason = None;
        let dormant = snapshot(&config, &runtime);
        let dormant_protected = dormant
            .features
            .iter()
            .find(|feature| feature.flag == "protected_egress_enabled")
            .expect("dormant protected egress feature");
        assert!(!dormant_protected.raw_enabled);
        assert!(!dormant_protected.effective_enabled);

        runtime.protected_egress_configured = true;
        runtime.components.insert(
            LiveComponent::ProtectedEgress,
            ComponentState {
                ready: true,
                disabled_reason: None,
                certification_id: Some("live-egress-worker-v1".to_string()),
            },
        );

        let snapshot = snapshot(&config, &runtime);
        let protected = snapshot
            .features
            .iter()
            .find(|feature| feature.flag == "protected_egress_enabled")
            .expect("protected egress feature");
        assert!(protected.raw_enabled);
        assert!(protected.effective_enabled);
        assert!(protected.dependency_ready);
    }

    #[tokio::test]
    async fn s10_disabled_service_is_inert_and_does_not_create_keys_or_take_lease() -> Result<()> {
        let database = test_database().await?;
        let service = LiveService::new(
            LiveConfig::default(),
            RunEnvironment::Development,
            database.pool.clone(),
            Arc::new(SecretsManager::from_key_bytes([4u8; 32], true)),
        );
        let snapshot = service.initialize().await?;
        assert_eq!(snapshot.lifecycle, LiveLifecycle::Disabled);
        assert!(!snapshot.effective_enabled);
        assert!(!snapshot.ready);
        assert!(service.crypto().await.is_none());
        let key_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM secrets WHERE scope = 'global' AND key LIKE 'live.crypto.%'",
        )
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(key_count, 0);
        assert!(
            ControlLeaseRepository::new(database.pool.clone(), Duration::from_secs(30))
                .current()
                .await?
                .owner_instance_id
                .is_none()
        );
        Ok(())
    }

    #[tokio::test]
    async fn s10_enabled_development_service_generates_encrypted_keys_and_fences_singleton()
    -> Result<()> {
        let database = test_database().await?;
        let config = LiveConfig {
            enabled: true,
            ..LiveConfig::default()
        };
        let secrets = Arc::new(SecretsManager::from_key_bytes([5u8; 32], true));
        let first = Arc::new(LiveService::new(
            config.clone(),
            RunEnvironment::Development,
            database.pool.clone(),
            secrets.clone(),
        ));
        let second = Arc::new(LiveService::new(
            config,
            RunEnvironment::Development,
            database.pool.clone(),
            secrets,
        ));
        let (first_snapshot, second_snapshot) =
            tokio::join!(first.initialize(), second.initialize());
        let first_snapshot = first_snapshot?;
        let second_snapshot = second_snapshot?;
        let snapshots = [&first_snapshot, &second_snapshot];
        assert_eq!(
            snapshots
                .iter()
                .filter(|snapshot| snapshot.lifecycle == LiveLifecycle::ControlReady)
                .count(),
            1
        );
        assert_eq!(
            snapshots
                .iter()
                .filter(|snapshot| snapshot.lifecycle == LiveLifecycle::LeaseHeld)
                .count(),
            1
        );
        assert!(snapshots.iter().all(|snapshot| !snapshot.ready));
        assert_eq!(
            snapshots
                .iter()
                .find_map(|snapshot| snapshot.lease_generation),
            Some(1)
        );
        assert!(first.crypto().await.is_some());
        assert!(second.crypto().await.is_some());
        assert!(first.admin_audit().is_some());
        assert!(second.admin_audit().is_some());
        assert!(first.destination_rule_repository().is_some());
        assert!(second.destination_rule_repository().is_some());

        let store = ExtensionStore::new(&database.pool);
        let stored = store
            .list_secrets(Some(SecretScope::Global), None, None)
            .await?;
        let live_keys = stored
            .iter()
            .filter(|secret| secret.key.starts_with("live.crypto."))
            .collect::<Vec<_>>();
        assert_eq!(live_keys.len(), 3);
        assert!(live_keys.iter().all(|secret| {
            secret.rotatable && SecretsManager::is_encrypted(&secret.value_encrypted)
        }));

        let shutdown = CancellationToken::new();
        shutdown.cancel();
        if first_snapshot.lifecycle == LiveLifecycle::ControlReady {
            first.clone().run_lease_heartbeat(shutdown).await;
        } else {
            second.clone().run_lease_heartbeat(shutdown).await;
        }
        assert!(
            ControlLeaseRepository::new(database.pool.clone(), Duration::from_secs(30))
                .current()
                .await?
                .owner_instance_id
                .is_none()
        );
        Ok(())
    }

    #[tokio::test]
    async fn s10_production_service_fails_closed_before_lease_when_keys_are_missing() -> Result<()>
    {
        let database = test_database().await?;
        let service = LiveService::new(
            LiveConfig {
                enabled: true,
                ..LiveConfig::default()
            },
            RunEnvironment::Production,
            database.pool.clone(),
            Arc::new(SecretsManager::from_key_bytes([6u8; 32], false)),
        );
        assert!(matches!(
            service.initialize().await,
            Err(LiveServiceError::Crypto(_))
        ));
        assert!(
            ControlLeaseRepository::new(database.pool.clone(), Duration::from_secs(30))
                .current()
                .await?
                .owner_instance_id
                .is_none()
        );
        Ok(())
    }

    #[tokio::test]
    async fn o11_persisted_key_rotation_state_controls_restart_primaries() -> Result<()> {
        let database = Database::connect(&DatabaseConfig {
            url: format!(
                "sqlite:file:/tmp/o11-live-key-restart-{}?mode=memory&cache=shared",
                Uuid::new_v4()
            ),
            max_connections: 4,
            connect_timeout_seconds: 5,
        })
        .await?;
        database.run_migrations().await?;
        let secrets = Arc::new(SecretsManager::from_key_bytes([46_u8; 32], true));
        for (key, material) in [
            ("live.crypto.envelope.live-envelope-2", [47_u8; 32]),
            ("live.crypto.token_hash.live-token-hash-2", [48_u8; 32]),
            ("live.crypto.audit.live-audit-2", [49_u8; 32]),
        ] {
            let encoded = general_purpose::STANDARD.encode(material);
            sqlx::query(
                "INSERT INTO secrets
                    (secret_id, scope, scope_id, key, value_encrypted, rotatable)
                 VALUES ($1, 'global', NULL, $2, $3, TRUE)",
            )
            .bind(Uuid::new_v4().to_string())
            .bind(key)
            .bind(secrets.encrypt(&encoded)?)
            .execute(&database.pool)
            .await?;
        }
        sqlx::query(
            "INSERT INTO live_key_rotation_state (
                state_id, envelope_primary_key_id, token_hash_primary_key_id,
                audit_primary_key_id, revision
             ) VALUES ('live-crypto-v1', 'live-envelope-2', 'live-token-hash-2',
                       'live-audit-2', 4)",
        )
        .execute(&database.pool)
        .await?;
        let service = LiveService::new(
            LiveConfig {
                enabled: true,
                ..LiveConfig::default()
            },
            RunEnvironment::Development,
            database.pool.clone(),
            secrets,
        );
        let loaded = service.load_crypto().await?;
        assert_eq!(loaded.crypto.primary_key_id()?, "live-envelope-2");
        assert_eq!(
            loaded.crypto.token_hash_primary_key_id()?,
            "live-token-hash-2"
        );
        assert_eq!(loaded.audit_key.key_id(), "live-audit-2");
        Ok(())
    }

    #[tokio::test]
    async fn s11_catalog_component_is_ready_only_after_provider_client_initializes() -> Result<()> {
        let database = test_database().await?;
        let service = Arc::new(LiveService::new(
            LiveConfig {
                enabled: true,
                catalog_enabled: true,
                ..LiveConfig::default()
            },
            RunEnvironment::Development,
            database.pool.clone(),
            Arc::new(SecretsManager::from_key_bytes([7u8; 32], true)),
        ));
        assert!(service.provider_client().is_none());
        let snapshot = service.initialize().await?;
        assert_eq!(snapshot.lifecycle, LiveLifecycle::ControlReady);
        assert!(snapshot.ready);
        assert!(service.provider_client().is_some());
        assert!(service.catalog_service().is_some());
        let catalog = snapshot
            .features
            .iter()
            .find(|feature| feature.flag == "catalog_enabled")
            .expect("catalog feature status");
        assert!(catalog.dependency_ready);
        assert!(catalog.effective_enabled);

        let shutdown = CancellationToken::new();
        shutdown.cancel();
        service.run_lease_heartbeat(shutdown).await;
        Ok(())
    }

    #[tokio::test]
    async fn p12_playback_readiness_waits_for_registered_startup_reconciliation() -> Result<()> {
        let database = Database::connect(&DatabaseConfig {
            url: format!(
                "sqlite:file:/tmp/p12-live-service-{}?mode=memory&cache=shared",
                Uuid::new_v4()
            ),
            max_connections: 4,
            connect_timeout_seconds: 5,
        })
        .await?;
        database.run_migrations().await?;
        seed_provider(&database, 45_679, json!({})).await?;
        let service = Arc::new(LiveService::new_for_test(
            LiveConfig {
                enabled: true,
                catalog_enabled: true,
                playback_enabled: true,
                client_direct_enabled: true,
                relay_enabled: true,
                allow_private_lan_sources: true,
                ..LiveConfig::default()
            },
            RunEnvironment::Development,
            database.pool.clone(),
            Arc::new(SecretsManager::from_key_bytes([13u8; 32], true)),
        ));
        let before = service.snapshot().await;
        assert!(
            before
                .features
                .iter()
                .all(|feature| !feature.effective_enabled)
        );

        let after = service.initialize().await?;
        for component in [
            LiveComponent::Playback,
            LiveComponent::ClientDirect,
            LiveComponent::Relay,
            LiveComponent::PrivateLanSources,
        ] {
            let feature = after
                .features
                .iter()
                .find(|feature| feature.flag == component.flag_name())
                .expect("configured Live feature");
            assert!(feature.dependency_ready);
            assert!(feature.effective_enabled);
        }
        let consumer_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM authorization_revocation_consumers
             WHERE consumer_name = 'live-session-revoker-v1'",
        )
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(consumer_count, 1);

        let shutdown = CancellationToken::new();
        shutdown.cancel();
        service.run_lease_heartbeat(shutdown).await;
        Ok(())
    }
}
