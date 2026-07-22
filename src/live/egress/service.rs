use std::{
    collections::HashMap,
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
    time::Duration,
};

use chrono::Utc;
use dashmap::DashMap;
use reqwest::Url;
use sqlx::Row;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::{
    live::{
        admin::{ActorSnapshot, LiveAuditChain},
        config::{
            LiveEgressConfig, LiveEgressDefaultMode, LiveEgressProfileConfig, LiveEgressProfileKind,
        },
        session::SessionRecord,
        upstream::{UpstreamFetcher, UpstreamLimits},
    },
    network::gateway::{
        CloudflareWarpGatewayRuntime, GatewayRuntime, GatewayTopologyCompileInput,
        GatewayTopologyLabels, GatewayTopologyProfile, GluetunOpenvpnGatewayRuntime,
        GluetunWireguardGatewayRuntime, compile_gateway_topology,
    },
    network::protection::{
        ActiveLiveEgressProfile, DownloadNetworkProfileKind, active_live_egress_profile,
        resolve_live_egress_secret,
    },
    runtime::{
        RuntimeManager,
        model::{
            ContainerHandle, ContainerSecurityOptions, ContainerSpec, ContainerTmpfsMount,
            ELIXIR_MANAGED_LABEL, EnvVar, OwnedDirectoryVolumeSpec, PortMapping,
            PrivateFileVolumeSpec, VolumeMount, VolumeMountSourceKind,
            apply_container_spec_fingerprint,
        },
    },
    secrets::SecretsManager,
};

use super::{
    ProtectedEgressTransport,
    control::{ControlKeys, ControlSecretDocument, WorkerReadinessConfig, readiness_ip_matches},
    material::{
        GATEWAY_CONFIG_ROLE, OPENVPN_CONFIG_PATH, OPENVPN_CONFIG_ROOT, OPENVPN_PASSWORD_PATH,
        OPENVPN_PASSWORD_ROLE, OPENVPN_PASSWORD_ROOT, OPENVPN_USERNAME_PATH, OPENVPN_USERNAME_ROLE,
        OPENVPN_USERNAME_ROOT, WIREGUARD_CONFIG_ROOT, prepare_gateway_material,
        prepare_gateway_material_from_secret_values,
    },
    policy::{
        EffectiveEgressPolicy, EgressPolicyMode, EgressPolicySelectionError, EgressPolicySource,
        PolicyCandidate, SessionEgressPolicyRequest, select_effective_policy,
        validate_effective_policy,
    },
    repository::{EgressPolicyRepository, EgressPolicyRepositoryError},
};

const ROLE_LABEL: &str = "elixir.live.egress.role";
const SESSION_LABEL: &str = "elixir.live.session_id";
const BINDING_LABEL: &str = "elixir.live.egress.binding_id";
const POLICY_LABEL: &str = "elixir.live.egress.policy_id";
const POLICY_KIND_LABEL: &str = "elixir.live.egress.policy_kind";
const RUNTIME_KIND_LABEL: &str = "elixir.live.egress.runtime_kind";
const PORTS_LABEL: &str = "elixir.live.egress.exposed_ports";
const FENCE_LABEL: &str = "elixir.live.control_fencing_token";
const WORKER_SECRET_PATH: &str = "/run/elixir-live-egress/control.json";
const WORKER_SECRET_ROOT: &str = "/run/elixir-live-egress";
const WORKER_UID: u32 = 65_532;
const WORKER_GID: u32 = 65_532;
const WARP_UID: u32 = 1_000;
const WARP_GID: u32 = 1_000;
const PROJECTED_WIREGUARD_CONFIG_PATH: &str = "/run/elixir-live-egress/projected/wg0.conf";
const PROJECTED_OPENVPN_CONFIG_PATH: &str = "/run/elixir-live-egress/projected/custom.conf";
const PROJECTED_OPENVPN_AUTH_PATH: &str = "/run/elixir-live-egress/projected/auth.txt";

#[derive(Debug, thiserror::Error)]
pub enum LiveEgressError {
    #[error("protected Live egress policy is invalid")]
    InvalidPolicy,
    #[error("protected Live egress profile is unavailable")]
    ProfileUnavailable,
    #[error("protected Live egress capacity is exhausted")]
    CapacityExhausted,
    #[error("protected Live egress ownership is stale")]
    StaleFence,
    #[error("protected Live egress persistence failed")]
    Database(#[from] sqlx::Error),
    #[error("protected Live egress policy persistence failed")]
    PolicyRepository(#[from] EgressPolicyRepositoryError),
    #[error("protected Live egress runtime failed closed")]
    Runtime,
    #[error("protected Live egress readiness proof failed")]
    Readiness,
    #[error("protected Live egress cleanup is incomplete")]
    Cleanup,
}

impl From<EgressPolicySelectionError> for LiveEgressError {
    fn from(_: EgressPolicySelectionError) -> Self {
        Self::InvalidPolicy
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveEgressProfileStatus {
    pub id: String,
    pub name: String,
    pub kind: LiveEgressProfileKind,
    pub selectable_by_profiles: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveEgressStatus {
    pub enabled: bool,
    pub ready: bool,
    pub default_mode: LiveEgressDefaultMode,
    pub default_policy_id: Option<String>,
    pub default_allow_fallback: bool,
    pub active_bindings: usize,
    pub available_capacity: usize,
    pub profiles: Vec<LiveEgressProfileStatus>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveEgressOutcome {
    ServerDefault,
    Protected,
    DirectFallback,
}

impl LiveEgressOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ServerDefault => "server_default",
            Self::Protected => "protected",
            Self::DirectFallback => "direct_fallback",
        }
    }
}

struct ActiveBinding {
    binding_id: Uuid,
    session_id: Uuid,
    control_fencing_token: i64,
    policy_id: String,
    policy_revision: i64,
    gateway_role: &'static str,
    gateway_name: String,
    worker_name: String,
    fetcher: Arc<UpstreamFetcher>,
    _permit: OwnedSemaphorePermit,
}

#[derive(Debug, Clone)]
struct DirectFallback {
    control_fencing_token: i64,
    policy_id: String,
    policy_revision: i64,
}

impl DirectFallback {
    fn matches(&self, session: &SessionRecord, policy: &EffectiveEgressPolicy) -> bool {
        self.control_fencing_token == session.control_fencing_token
            && self.policy_id == policy.policy_id.as_deref().unwrap_or_default()
            && self.policy_revision == policy.revision
    }
}

impl std::fmt::Debug for ActiveBinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ActiveBinding")
            .field("binding_id", &self.binding_id)
            .field("session_id", &self.session_id)
            .field("control_fencing_token", &self.control_fencing_token)
            .field("policy_id", &self.policy_id)
            .field("policy_revision", &self.policy_revision)
            .field("gateway_role", &self.gateway_role)
            .field("gateway_name", &self.gateway_name)
            .field("worker_name", &self.worker_name)
            .finish_non_exhaustive()
    }
}

impl ActiveBinding {
    fn matches(&self, session: &SessionRecord, policy: &EffectiveEgressPolicy) -> bool {
        self.session_id == session.id
            && self.control_fencing_token == session.control_fencing_token
            && self.policy_id == policy.policy_id.as_deref().unwrap_or_default()
            && self.policy_revision == policy.revision
    }
}

pub struct LiveEgressService {
    pool: sqlx::AnyPool,
    runtime: Arc<dyn RuntimeManager>,
    config: LiveEgressConfig,
    audit: Arc<LiveAuditChain>,
    policy_repository: EgressPolicyRepository,
    capacity: Arc<Semaphore>,
    bindings: DashMap<Uuid, Arc<ActiveBinding>>,
    fallbacks: DashMap<Uuid, DirectFallback>,
    mutation_lock: Mutex<()>,
    control_root: PathBuf,
    projected: OnceLock<ProjectedLiveEgress>,
    secrets: Option<Arc<SecretsManager>>,
}

struct ProjectedLiveEgress {
    config: LiveEgressConfig,
    profile: ActiveLiveEgressProfile,
}

impl std::fmt::Debug for LiveEgressService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LiveEgressService")
            .field("active_bindings", &self.bindings.len())
            .field("available_capacity", &self.capacity.available_permits())
            .field("control_root", &self.control_root)
            .finish_non_exhaustive()
    }
}

impl LiveEgressService {
    pub fn new(
        pool: sqlx::AnyPool,
        runtime: Arc<dyn RuntimeManager>,
        config: LiveEgressConfig,
        audit: Arc<LiveAuditChain>,
    ) -> Result<Self, LiveEgressError> {
        Self::build(pool, runtime, config, audit, None)
    }

    pub async fn new_with_builtin_fallback(
        pool: sqlx::AnyPool,
        runtime: Arc<dyn RuntimeManager>,
        config: LiveEgressConfig,
        audit: Arc<LiveAuditChain>,
        secrets: Arc<SecretsManager>,
    ) -> Result<Option<Self>, LiveEgressError> {
        if !config.profiles.is_empty() {
            return Self::new(pool, runtime, config, audit).map(Some);
        }
        let service = Self::build(pool, runtime, config, audit, Some(secrets))?;
        service.refresh_builtin_profile().await;
        Ok(Some(service))
    }

    fn build(
        pool: sqlx::AnyPool,
        runtime: Arc<dyn RuntimeManager>,
        config: LiveEgressConfig,
        audit: Arc<LiveAuditChain>,
        secrets: Option<Arc<SecretsManager>>,
    ) -> Result<Self, LiveEgressError> {
        let capacity = usize::try_from(config.max_concurrent)
            .ok()
            .filter(|value| *value > 0)
            .ok_or(LiveEgressError::InvalidPolicy)?;
        let control_root = absolute_path(&config.control_root)?;
        prepare_control_root(&control_root)?;
        Ok(Self {
            policy_repository: EgressPolicyRepository::new(pool.clone()),
            pool,
            runtime,
            config,
            audit,
            capacity: Arc::new(Semaphore::new(capacity)),
            bindings: DashMap::new(),
            fallbacks: DashMap::new(),
            mutation_lock: Mutex::new(()),
            control_root,
            projected: OnceLock::new(),
            secrets,
        })
    }

    pub fn policy_repository(&self) -> &EgressPolicyRepository {
        &self.policy_repository
    }

    pub fn status(&self) -> LiveEgressStatus {
        let config = self.effective_config();
        LiveEgressStatus {
            enabled: !config.profiles.is_empty(),
            ready: !config.profiles.is_empty(),
            default_mode: config.default_mode,
            default_policy_id: config.default_policy_id.clone(),
            default_allow_fallback: config.default_allow_fallback,
            active_bindings: self.bindings.len(),
            available_capacity: self.capacity.available_permits(),
            profiles: self
                .effective_config()
                .profiles
                .iter()
                .map(|profile| LiveEgressProfileStatus {
                    id: profile.id.clone(),
                    name: profile.name.clone(),
                    kind: profile.kind,
                    selectable_by_profiles: profile.selectable_by_profiles,
                })
                .collect(),
        }
    }

    pub fn profile(&self, id: &str) -> Option<&LiveEgressProfileConfig> {
        self.effective_config()
            .profiles
            .iter()
            .find(|profile| profile.id == id)
    }

    pub(crate) async fn refresh_builtin_profile(&self) -> bool {
        if !self.config.profiles.is_empty() || self.projected.get().is_some() {
            return self.status().enabled;
        }
        let Some(secrets) = self.secrets.as_deref() else {
            return false;
        };
        let Some(projected) = active_live_egress_profile(&self.pool, secrets)
            .await
            .ok()
            .flatten()
        else {
            return false;
        };
        let mut config = self.config.clone();
        let profile = install_projected_profile(&mut config, &projected);
        if profile.kind != LiveEgressProfileKind::Warp
            && self
                .prepare_projected_material(&projected, &profile)
                .await
                .is_err()
        {
            return false;
        }
        let _ = self.projected.set(ProjectedLiveEgress {
            config,
            profile: projected,
        });
        self.projected.get().is_some()
    }

    fn effective_config(&self) -> &LiveEgressConfig {
        self.projected
            .get()
            .map(|projected| &projected.config)
            .unwrap_or(&self.config)
    }

    async fn prepare_profile_material(
        &self,
        profile: &LiveEgressProfileConfig,
    ) -> Result<Vec<super::material::PreparedMaterialFile>, LiveEgressError> {
        if profile.kind == LiveEgressProfileKind::Warp {
            return prepare_gateway_material(profile)
                .await
                .map_err(|_| LiveEgressError::InvalidPolicy);
        }
        let Some(projected) = self
            .projected
            .get()
            .map(|projected| &projected.profile)
            .filter(|projected| projected.profile_id == profile.id)
        else {
            return prepare_gateway_material(profile)
                .await
                .map_err(|_| LiveEgressError::InvalidPolicy);
        };
        self.prepare_projected_material(projected, profile).await
    }

    async fn prepare_projected_material(
        &self,
        projected: &ActiveLiveEgressProfile,
        profile: &LiveEgressProfileConfig,
    ) -> Result<Vec<super::material::PreparedMaterialFile>, LiveEgressError> {
        let secrets = self
            .secrets
            .as_deref()
            .ok_or(LiveEgressError::InvalidPolicy)?;
        let config_secret_ref = projected
            .config_secret_ref
            .as_deref()
            .ok_or(LiveEgressError::InvalidPolicy)?;
        let config = resolve_live_egress_secret(&self.pool, secrets, config_secret_ref)
            .await
            .map_err(|_| LiveEgressError::InvalidPolicy)?;
        let username = match projected.username_secret_ref.as_deref() {
            Some(secret_ref) => Some(
                resolve_live_egress_secret(&self.pool, secrets, secret_ref)
                    .await
                    .map_err(|_| LiveEgressError::InvalidPolicy)?,
            ),
            None => None,
        };
        let password = match projected.password_secret_ref.as_deref() {
            Some(secret_ref) => Some(
                resolve_live_egress_secret(&self.pool, secrets, secret_ref)
                    .await
                    .map_err(|_| LiveEgressError::InvalidPolicy)?,
            ),
            None => None,
        };
        prepare_gateway_material_from_secret_values(
            profile.kind,
            config.as_bytes(),
            username.as_ref().map(|value| value.as_bytes()),
            password.as_ref().map(|value| value.as_bytes()),
        )
        .await
        .map_err(|_| LiveEgressError::InvalidPolicy)
    }

    pub async fn select_policy(
        &self,
        home_id: Uuid,
        profile_id: Uuid,
        provider_id: Uuid,
        request: Option<SessionEgressPolicyRequest>,
        explicit_policy: bool,
    ) -> Result<EffectiveEgressPolicy, LiveEgressError> {
        let mut candidates = vec![self.config_candidate()?];
        candidates.extend(
            self.policy_repository
                .assignments_for(home_id, profile_id, provider_id)
                .await?
                .into_iter()
                .map(|assignment| assignment.candidate()),
        );
        let inherited = select_effective_policy(candidates.clone())?;
        if let Some(mut request) = request {
            if request.mode != EgressPolicyMode::Off && request.policy_id.is_none() {
                request.policy_id = inherited
                    .policy_id
                    .clone()
                    .or_else(|| self.effective_config().default_policy_id.clone());
            }
            if request.mode == EgressPolicyMode::PreferProtected {
                request.allow_fallback = inherited.mode == EgressPolicyMode::PreferProtected
                    && inherited.policy_id == request.policy_id
                    && inherited.allow_fallback;
            } else {
                request.allow_fallback = false;
            }
            candidates.push(PolicyCandidate {
                mode: request.mode,
                policy_id: request.policy_id,
                allow_fallback: request.allow_fallback,
                revision: inherited.revision,
                source: EgressPolicySource::Session,
            });
        }
        let selected = select_effective_policy(candidates)?;
        validate_effective_policy(&selected)?;
        if let Some(policy_id) = selected.policy_id.as_deref() {
            let profile = self
                .profile(policy_id)
                .ok_or(LiveEgressError::ProfileUnavailable)?;
            if explicit_policy && !profile.selectable_by_profiles {
                return Err(LiveEgressError::ProfileUnavailable);
            }
        }
        Ok(selected)
    }

    pub async fn ensure_session(
        &self,
        session: &SessionRecord,
        policy: &EffectiveEgressPolicy,
        actor: &ActorSnapshot,
    ) -> Result<Option<Arc<UpstreamFetcher>>, LiveEgressError> {
        validate_effective_policy(policy)?;
        if !policy.protected() {
            return Ok(None);
        }
        if session.control_fencing_token < 1 || session.state.is_terminal() {
            return Err(LiveEgressError::StaleFence);
        }
        if let Some(existing) = self.bindings.get(&session.id) {
            if existing.matches(session, policy) {
                return Ok(Some(existing.fetcher.clone()));
            }
            if existing.control_fencing_token >= session.control_fencing_token {
                return Err(LiveEgressError::StaleFence);
            }
        }
        if let Some(fallback) = self.fallbacks.get(&session.id)
            && fallback.matches(session, policy)
            && policy.mode == EgressPolicyMode::PreferProtected
            && policy.allow_fallback
        {
            return Ok(None);
        }

        let _guard = self.mutation_lock.lock().await;
        if let Some(existing) = self.bindings.get(&session.id) {
            if existing.matches(session, policy) {
                return Ok(Some(existing.fetcher.clone()));
            }
        }
        if let Some(fallback) = self.fallbacks.get(&session.id)
            && fallback.matches(session, policy)
            && policy.mode == EgressPolicyMode::PreferProtected
            && policy.allow_fallback
        {
            return Ok(None);
        }
        self.release_locked(session.id, session.control_fencing_token)
            .await?;
        match self.provision_locked(session, policy, actor).await {
            Ok(Some(binding)) => {
                let fetcher = binding.fetcher.clone();
                self.bindings.insert(session.id, Arc::new(binding));
                Ok(Some(fetcher))
            }
            Ok(None) => {
                self.fallbacks.insert(
                    session.id,
                    DirectFallback {
                        control_fencing_token: session.control_fencing_token,
                        policy_id: policy.policy_id.clone().unwrap_or_default(),
                        policy_revision: policy.revision,
                    },
                );
                tracing::warn!(
                    session_id = %session.id,
                    policy_id = policy.policy_id.as_deref().unwrap_or(""),
                    "preferred Live egress was unavailable; using the explicitly permitted direct fallback"
                );
                Ok(None)
            }
            Err(error) => {
                crate::live::metrics::ADMISSION_REJECTIONS
                    .with_label_values(&["egress", egress_error_label(&error)])
                    .inc();
                Err(error)
            }
        }
    }

    pub fn fetcher_for(
        &self,
        session: &SessionRecord,
        policy: &EffectiveEgressPolicy,
    ) -> Result<Option<Arc<UpstreamFetcher>>, LiveEgressError> {
        if !policy.protected() {
            return Ok(None);
        }
        let Some(binding) = self.bindings.get(&session.id) else {
            if policy.mode == EgressPolicyMode::PreferProtected
                && policy.allow_fallback
                && self
                    .fallbacks
                    .get(&session.id)
                    .is_some_and(|fallback| fallback.matches(session, policy))
            {
                return Ok(None);
            }
            return Err(LiveEgressError::Readiness);
        };
        if !binding.matches(session, policy) {
            return Err(LiveEgressError::StaleFence);
        }
        Ok(Some(binding.fetcher.clone()))
    }

    pub fn outcome_for(
        &self,
        session: &SessionRecord,
        policy: &EffectiveEgressPolicy,
    ) -> Result<LiveEgressOutcome, LiveEgressError> {
        validate_effective_policy(policy)?;
        if !policy.protected() {
            return Ok(LiveEgressOutcome::ServerDefault);
        }
        if self
            .bindings
            .get(&session.id)
            .is_some_and(|binding| binding.matches(session, policy))
        {
            return Ok(LiveEgressOutcome::Protected);
        }
        if policy.mode == EgressPolicyMode::PreferProtected
            && policy.allow_fallback
            && self
                .fallbacks
                .get(&session.id)
                .is_some_and(|fallback| fallback.matches(session, policy))
        {
            return Ok(LiveEgressOutcome::DirectFallback);
        }
        Err(LiveEgressError::Readiness)
    }

    pub async fn end_session(
        &self,
        session_id: Uuid,
        control_fencing_token: i64,
    ) -> Result<(), LiveEgressError> {
        let _guard = self.mutation_lock.lock().await;
        self.release_locked(session_id, control_fencing_token).await
    }

    pub async fn reconcile_startup(
        &self,
        control_fencing_token: i64,
    ) -> Result<u64, LiveEgressError> {
        if control_fencing_token < 1 {
            return Err(LiveEgressError::StaleFence);
        }
        let _guard = self.mutation_lock.lock().await;
        let rows = sqlx::query(
            "SELECT session_id FROM live_egress_bindings
             WHERE state IN ('provisioning', 'ready', 'releasing')
               AND control_fencing_token <= $1
             ORDER BY created_at, id",
        )
        .bind(control_fencing_token)
        .fetch_all(&self.pool)
        .await?;
        let mut released = 0_u64;
        for row in rows {
            let session_id = row
                .try_get::<String, _>("session_id")?
                .parse::<Uuid>()
                .map_err(|_| LiveEgressError::Database(sqlx::Error::RowNotFound))?;
            self.release_locked(session_id, control_fencing_token)
                .await?;
            released = released.saturating_add(1);
        }
        Ok(released)
    }

    pub async fn cancel_all(&self, control_fencing_token: i64) {
        let session_ids = self
            .bindings
            .iter()
            .map(|entry| *entry.key())
            .chain(self.fallbacks.iter().map(|entry| *entry.key()))
            .collect::<Vec<_>>();
        for session_id in session_ids {
            if let Err(error) = self.end_session(session_id, control_fencing_token).await {
                tracing::error!(
                    session_id = %session_id,
                    error = %error,
                    "Live egress cleanup failed closed"
                );
            }
        }
    }

    pub async fn reap_stale(&self, control_fencing_token: i64) -> Result<(), LiveEgressError> {
        let session_ids = self
            .bindings
            .iter()
            .map(|entry| *entry.key())
            .chain(self.fallbacks.iter().map(|entry| *entry.key()))
            .collect::<Vec<_>>();
        for session_id in session_ids {
            let row = sqlx::query(
                "SELECT state, control_fencing_token FROM live_playback_sessions WHERE id = $1",
            )
            .bind(session_id.to_string())
            .fetch_optional(&self.pool)
            .await?;
            let stale = match row {
                Some(row) => {
                    let state: String = row.try_get("state")?;
                    let session_fence: i64 = row.try_get("control_fencing_token")?;
                    matches!(state.as_str(), "ended" | "expired" | "failed")
                        || session_fence != control_fencing_token
                }
                None => true,
            };
            if stale {
                self.end_session(session_id, control_fencing_token).await?;
            }
        }
        Ok(())
    }

    fn config_candidate(&self) -> Result<PolicyCandidate, LiveEgressError> {
        let config = self.effective_config();
        let mode = match config.default_mode {
            LiveEgressDefaultMode::Off => EgressPolicyMode::Off,
            LiveEgressDefaultMode::PreferProtected => EgressPolicyMode::PreferProtected,
            LiveEgressDefaultMode::RequireProtected => EgressPolicyMode::RequireProtected,
        };
        let candidate = PolicyCandidate {
            mode,
            policy_id: config.default_policy_id.clone(),
            allow_fallback: config.default_allow_fallback,
            revision: 1,
            source: EgressPolicySource::ServerConfig,
        };
        select_effective_policy([candidate.clone()])?;
        Ok(candidate)
    }

    async fn provision_locked(
        &self,
        session: &SessionRecord,
        policy: &EffectiveEgressPolicy,
        actor: &ActorSnapshot,
    ) -> Result<Option<ActiveBinding>, LiveEgressError> {
        let policy_id = policy
            .policy_id
            .as_deref()
            .ok_or(LiveEgressError::InvalidPolicy)?;
        let profile = self
            .profile(policy_id)
            .cloned()
            .ok_or(LiveEgressError::ProfileUnavailable)?;
        let binding_id = self
            .upsert_provisioning_binding(session, policy, &profile)
            .await?;
        let secret_dir = self.control_root.join(binding_id.to_string());
        let secret_path = secret_dir.join("control.json");
        let control_volume_name = control_volume_name(binding_id);
        let control_volume_labels = control_volume_labels(
            session.id,
            binding_id,
            session.control_fencing_token,
            policy_id,
        );
        let worker_name = worker_name(session.id);
        let gateway_name = format!("{worker_name}-vpn");
        let result = async {
            let gateway_material = self.prepare_profile_material(&profile).await?;
            if profile.kind == LiveEgressProfileKind::Warp {
                self.runtime
                    .ensure_owned_directory_volume(&OwnedDirectoryVolumeSpec {
                        name: profile
                            .state_volume_name
                            .clone()
                            .ok_or(LiveEgressError::InvalidPolicy)?,
                        image: profile.gateway_image.clone(),
                        owner_uid: WARP_UID,
                        owner_gid: WARP_GID,
                        labels: warp_state_volume_labels(&profile),
                    })
                    .await
                    .map_err(|_| LiveEgressError::Runtime)?;
            }
            let permit = self
                .capacity
                .clone()
                .try_acquire_owned()
                .map_err(|_| LiveEgressError::CapacityExhausted)?;
            let keys = ControlKeys::generate();
            let secret = ControlSecretDocument::new(
                session.id,
                session.control_fencing_token,
                session.hard_expires_at,
                &keys,
                WorkerReadinessConfig {
                    external_ip_url: profile.external_ip_url.clone(),
                    dns_probe_host: profile.dns_probe_host.clone(),
                    expected_egress_ips: profile.expected_egress_ips.clone(),
                },
            )
            .map_err(|_| LiveEgressError::InvalidPolicy)?;
            write_control_secret(&secret_dir, &secret_path, &secret)?;
            self.runtime
                .create_private_file_volume(&PrivateFileVolumeSpec {
                    name: control_volume_name.clone(),
                    image: profile.gateway_image.clone(),
                    source_path: secret_path.to_string_lossy().into_owned(),
                    file_name: "control.json".to_string(),
                    owner_uid: WORKER_UID,
                    owner_gid: WORKER_GID,
                    labels: control_volume_labels.clone(),
                })
                .await
                .map_err(|_| LiveEgressError::Runtime)?;
            for material in &gateway_material {
                let staged_path = secret_dir.join(material.file_name);
                write_private_file(&staged_path, material.contents())?;
                self.runtime
                    .create_private_file_volume(&PrivateFileVolumeSpec {
                        name: material_volume_name(binding_id, material.role),
                        image: profile.gateway_image.clone(),
                        source_path: staged_path.to_string_lossy().into_owned(),
                        file_name: material.file_name.to_string(),
                        owner_uid: WORKER_UID,
                        owner_gid: WORKER_GID,
                        labels: private_volume_labels(
                            session.id,
                            binding_id,
                            session.control_fencing_token,
                            policy_id,
                            material.role,
                        ),
                    })
                    .await
                    .map_err(|_| LiveEgressError::Runtime)?;
            }
            remove_secret_dir(&secret_dir).await?;
            let topology = self.compile_topology(
                session,
                binding_id,
                &profile,
                &worker_name,
                &control_volume_name,
            )?;
            let (gateway, worker, fetcher, readiness_json) = self
                .start_and_verify(
                    session,
                    binding_id,
                    &profile,
                    topology.gateway_spec,
                    topology.protected_app_spec,
                    keys,
                )
                .await?;
            self.mark_ready(binding_id, session, &gateway, &worker, &readiness_json)
                .await?;
            Ok(ActiveBinding {
                binding_id,
                session_id: session.id,
                control_fencing_token: session.control_fencing_token,
                policy_id: policy_id.to_string(),
                policy_revision: policy.revision,
                gateway_role: gateway_role(profile.kind),
                gateway_name: gateway.name,
                worker_name: worker.name,
                fetcher,
                _permit: permit,
            })
        }
        .await;
        match result {
            Ok(binding) => Ok(Some(binding)),
            Err(error) => {
                let runtime_cleanup = self
                    .cleanup_runtime(
                        session.id,
                        &binding_id.to_string(),
                        session.control_fencing_token,
                        policy_id,
                        &worker_name,
                        &gateway_name,
                        gateway_role(profile.kind),
                    )
                    .await;
                let secret_cleanup = remove_secret_dir(&secret_dir).await;
                if runtime_cleanup.is_err() || secret_cleanup.is_err() {
                    crate::live::metrics::CLEANUP
                        .with_label_values(&["egress_binding", "failed"])
                        .inc();
                    return Err(LiveEgressError::Cleanup);
                }
                self.policy_repository
                    .mark_binding_failed(
                        binding_id,
                        session,
                        policy,
                        actor,
                        &self.audit,
                        egress_error_label(&error),
                        Utc::now(),
                    )
                    .await?;
                if policy.mode == EgressPolicyMode::PreferProtected && policy.allow_fallback {
                    Ok(None)
                } else {
                    Err(error)
                }
            }
        }
    }

    fn compile_topology(
        &self,
        session: &SessionRecord,
        binding_id: Uuid,
        profile: &LiveEgressProfileConfig,
        worker_name: &str,
        control_volume_name: &str,
    ) -> Result<crate::network::gateway::CompiledGatewayTopology, LiveEgressError> {
        compile_live_topology(
            &self.config,
            session,
            binding_id,
            profile,
            worker_name,
            control_volume_name,
        )
    }

    async fn start_and_verify(
        &self,
        session: &SessionRecord,
        binding_id: Uuid,
        profile: &LiveEgressProfileConfig,
        gateway_spec: Option<ContainerSpec>,
        worker_spec: ContainerSpec,
        keys: ControlKeys,
    ) -> Result<StartedBinding, LiveEgressError> {
        let gateway_spec = gateway_spec.ok_or(LiveEgressError::InvalidPolicy)?;
        self.runtime
            .ensure_network(&gateway_spec.network)
            .await
            .map_err(|_| LiveEgressError::Runtime)?;
        let gateway = self
            .runtime
            .ensure_container(&gateway_spec)
            .await
            .map_err(|_| LiveEgressError::Runtime)?;
        self.runtime
            .start_container(&gateway)
            .await
            .map_err(|_| LiveEgressError::Runtime)?;
        wait_running(
            self.runtime.as_ref(),
            &gateway,
            Duration::from_secs(self.config.startup_timeout_seconds),
        )
        .await?;
        let worker = self
            .runtime
            .ensure_container(&worker_spec)
            .await
            .map_err(|_| LiveEgressError::Runtime)?;
        self.runtime
            .start_container(&worker)
            .await
            .map_err(|_| LiveEgressError::Runtime)?;
        wait_running(
            self.runtime.as_ref(),
            &worker,
            Duration::from_secs(self.config.startup_timeout_seconds),
        )
        .await?;
        let worker_state = self
            .runtime
            .describe_container_runtime_state(&worker.name)
            .await
            .map_err(|_| LiveEgressError::Runtime)?
            .ok_or(LiveEgressError::Runtime)?;
        let gateway_state = self
            .runtime
            .describe_container_runtime_state(&gateway.name)
            .await
            .map_err(|_| LiveEgressError::Runtime)?
            .ok_or(LiveEgressError::Runtime)?;
        verify_runtime_state(
            session,
            binding_id,
            profile,
            &worker_state,
            &gateway_state,
            &gateway,
            &worker_spec.security,
            self.config.control_port,
        )?;
        let host_port = gateway_state
            .published_ports
            .iter()
            .find(|port| {
                port.container_port == self.config.control_port
                    && port.protocol == "tcp"
                    && port.host_ip.as_deref() == Some("127.0.0.1")
            })
            .map(|port| port.host_port)
            .ok_or(LiveEgressError::Runtime)?;
        let endpoint = Url::parse(&format!("http://127.0.0.1:{host_port}/"))
            .map_err(|_| LiveEgressError::Runtime)?;
        let transport = ProtectedEgressTransport::new(
            endpoint,
            keys,
            session.control_fencing_token,
            Duration::from_secs(self.config.health_timeout_seconds),
        )
        .map_err(|_| LiveEgressError::Runtime)?;
        let deadline =
            tokio::time::Instant::now() + Duration::from_secs(self.config.startup_timeout_seconds);
        let readiness = loop {
            let cancellation = CancellationToken::new();
            if let Ok(readiness) = transport.readiness(&cancellation).await
                && readiness.ready()
                && readiness_ip_matches(&profile.expected_egress_ips, readiness.observed_egress_ip)
            {
                break readiness;
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(LiveEgressError::Readiness);
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        };
        let readiness_json =
            serde_json::to_string(&readiness).map_err(|_| LiveEgressError::Readiness)?;
        let fetcher = transport
            .fetcher(protected_limits())
            .map(Arc::new)
            .map_err(|_| LiveEgressError::Runtime)?;
        Ok((gateway, worker, fetcher, readiness_json))
    }

    async fn upsert_provisioning_binding(
        &self,
        session: &SessionRecord,
        policy: &EffectiveEgressPolicy,
        profile: &LiveEgressProfileConfig,
    ) -> Result<Uuid, LiveEgressError> {
        let mut transaction = self.pool.begin().await?;
        let authoritative = sqlx::query(
            "SELECT control_fencing_token, state FROM live_playback_sessions WHERE id = $1",
        )
        .bind(session.id.to_string())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(LiveEgressError::StaleFence)?;
        let fence: i64 = authoritative.try_get("control_fencing_token")?;
        let state: String = authoritative.try_get("state")?;
        if fence != session.control_fencing_token
            || matches!(state.as_str(), "ended" | "expired" | "failed")
        {
            return Err(LiveEgressError::StaleFence);
        }
        let existing = sqlx::query(
            "SELECT id, control_fencing_token FROM live_egress_bindings WHERE session_id = $1",
        )
        .bind(session.id.to_string())
        .fetch_optional(&mut *transaction)
        .await?;
        let binding_id = if let Some(row) = existing {
            let existing_fence: i64 = row.try_get("control_fencing_token")?;
            if existing_fence > session.control_fencing_token {
                return Err(LiveEgressError::StaleFence);
            }
            let id = row
                .try_get::<String, _>("id")?
                .parse::<Uuid>()
                .map_err(|_| LiveEgressError::StaleFence)?;
            sqlx::query(
                "UPDATE live_egress_bindings
                 SET policy_id = $1, mode = $2, gateway_instance_id = NULL,
                     worker_instance_id = NULL, gateway_container_name = $3,
                     worker_container_name = $4, state = 'provisioning',
                     control_fencing_token = $5, policy_revision = $6,
                     failure_reason_redacted = NULL, readiness_json = NULL,
                     ready_at = NULL, last_health_at = NULL, released_at = NULL
                 WHERE id = $7 AND control_fencing_token <= $5",
            )
            .bind(policy.policy_id.as_deref())
            .bind(profile_kind(profile.kind))
            .bind(format!("{}-vpn", worker_name(session.id)))
            .bind(worker_name(session.id))
            .bind(session.control_fencing_token)
            .bind(policy.revision)
            .bind(id.to_string())
            .execute(&mut *transaction)
            .await?;
            id
        } else {
            let id = Uuid::new_v4();
            sqlx::query(
                "INSERT INTO live_egress_bindings
                 (id, session_id, policy_id, mode, gateway_container_name,
                  worker_container_name, state, control_fencing_token, policy_revision)
                 VALUES ($1, $2, $3, $4, $5, $6, 'provisioning', $7, $8)",
            )
            .bind(id.to_string())
            .bind(session.id.to_string())
            .bind(policy.policy_id.as_deref())
            .bind(profile_kind(profile.kind))
            .bind(format!("{}-vpn", worker_name(session.id)))
            .bind(worker_name(session.id))
            .bind(session.control_fencing_token)
            .bind(policy.revision)
            .execute(&mut *transaction)
            .await?;
            id
        };
        let updated = sqlx::query(
            "UPDATE live_playback_sessions SET egress_binding_id = $1
             WHERE id = $2 AND control_fencing_token = $3
               AND state NOT IN ('ended', 'expired', 'failed')",
        )
        .bind(binding_id.to_string())
        .bind(session.id.to_string())
        .bind(session.control_fencing_token)
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(LiveEgressError::StaleFence);
        }
        transaction.commit().await?;
        Ok(binding_id)
    }

    async fn mark_ready(
        &self,
        binding_id: Uuid,
        session: &SessionRecord,
        gateway: &ContainerHandle,
        worker: &ContainerHandle,
        readiness_json: &str,
    ) -> Result<(), LiveEgressError> {
        let now = Utc::now().to_rfc3339();
        let result = sqlx::query(
            "UPDATE live_egress_bindings
             SET gateway_instance_id = $1, worker_instance_id = $2, state = 'ready',
                 readiness_json = $3, ready_at = $4, last_health_at = $4
             WHERE id = $5 AND session_id = $6 AND state = 'provisioning'
               AND control_fencing_token = $7",
        )
        .bind(&gateway.id)
        .bind(&worker.id)
        .bind(readiness_json)
        .bind(now)
        .bind(binding_id.to_string())
        .bind(session.id.to_string())
        .bind(session.control_fencing_token)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() != 1 {
            return Err(LiveEgressError::StaleFence);
        }
        Ok(())
    }

    async fn release_locked(
        &self,
        session_id: Uuid,
        control_fencing_token: i64,
    ) -> Result<(), LiveEgressError> {
        if control_fencing_token < 1 {
            return Err(LiveEgressError::StaleFence);
        }
        if let Some(binding) = self.bindings.get(&session_id)
            && binding.control_fencing_token > control_fencing_token
        {
            return Err(LiveEgressError::StaleFence);
        }
        if let Some(fallback) = self.fallbacks.get(&session_id)
            && fallback.control_fencing_token > control_fencing_token
        {
            return Err(LiveEgressError::StaleFence);
        }
        let row = sqlx::query(
            "SELECT id, policy_id, mode, gateway_container_name, worker_container_name,
                    control_fencing_token
             FROM live_egress_bindings WHERE session_id = $1",
        )
        .bind(session_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            if let Some(binding) = self.bindings.get(&session_id) {
                let worker_name = binding.worker_name.clone();
                let gateway_name = binding.gateway_name.clone();
                let binding_id = binding.binding_id.to_string();
                let binding_fence = binding.control_fencing_token;
                let policy_id = binding.policy_id.clone();
                let gateway_role = binding.gateway_role;
                drop(binding);
                self.cleanup_runtime(
                    session_id,
                    &binding_id,
                    binding_fence,
                    &policy_id,
                    &worker_name,
                    &gateway_name,
                    gateway_role,
                )
                .await?;
                remove_secret_dir(&self.control_root.join(&binding_id)).await?;
            }
            self.bindings.remove(&session_id);
            self.fallbacks.remove(&session_id);
            return Ok(());
        };
        let binding_fence: i64 = row.try_get("control_fencing_token")?;
        if binding_fence > control_fencing_token {
            return Err(LiveEgressError::StaleFence);
        }
        let binding_id: String = row.try_get("id")?;
        let policy_id = row
            .try_get::<Option<String>, _>("policy_id")?
            .ok_or(LiveEgressError::Cleanup)?;
        let mode: String = row.try_get("mode")?;
        let expected_gateway_role = gateway_role_from_mode(&mode).ok_or_else(|| {
            tracing::error!(
                binding_id,
                "Live egress binding has an invalid persisted mode"
            );
            LiveEgressError::Cleanup
        })?;
        let gateway_name: String = row.try_get("gateway_container_name")?;
        let worker_name: String = row.try_get("worker_container_name")?;
        sqlx::query(
            "UPDATE live_egress_bindings SET state = 'releasing'
             WHERE id = $1 AND state NOT IN ('released', 'failed')
               AND control_fencing_token <= $2",
        )
        .bind(&binding_id)
        .bind(control_fencing_token)
        .execute(&self.pool)
        .await?;
        let secret_dir = self.control_root.join(&binding_id);
        if self
            .cleanup_runtime(
                session_id,
                &binding_id,
                binding_fence,
                &policy_id,
                &worker_name,
                &gateway_name,
                expected_gateway_role,
            )
            .await
            .is_err()
            || remove_secret_dir(&secret_dir).await.is_err()
        {
            crate::live::metrics::CLEANUP
                .with_label_values(&["egress_binding", "failed"])
                .inc();
            return Err(LiveEgressError::Cleanup);
        }
        let now = Utc::now().to_rfc3339();
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "UPDATE live_egress_bindings SET state = 'released', released_at = $1
             WHERE id = $2 AND control_fencing_token <= $3 AND state != 'failed'",
        )
        .bind(now)
        .bind(&binding_id)
        .bind(control_fencing_token)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE live_playback_sessions SET egress_binding_id = NULL
             WHERE id = $1 AND egress_binding_id = $2",
        )
        .bind(session_id.to_string())
        .bind(&binding_id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        self.bindings.remove(&session_id);
        self.fallbacks.remove(&session_id);
        crate::live::metrics::CLEANUP
            .with_label_values(&["egress_binding", "completed"])
            .inc();
        Ok(())
    }

    async fn cleanup_runtime(
        &self,
        session_id: Uuid,
        binding_id: &str,
        control_fencing_token: i64,
        policy_id: &str,
        worker_name: &str,
        gateway_name: &str,
        expected_gateway_role: &str,
    ) -> Result<(), LiveEgressError> {
        if binding_id.is_empty()
            || control_fencing_token < 1
            || policy_id.is_empty()
            || !is_gateway_role(expected_gateway_role)
        {
            return Err(LiveEgressError::Cleanup);
        }
        let binding_uuid = binding_id
            .parse::<Uuid>()
            .map_err(|_| LiveEgressError::Cleanup)?;
        let mut owned_private_volumes = Vec::new();
        for role in private_volume_roles(expected_gateway_role) {
            let name = if *role == "control_secret" {
                control_volume_name(binding_uuid)
            } else {
                material_volume_name(binding_uuid, role)
            };
            let labels = private_volume_labels(
                session_id,
                binding_uuid,
                control_fencing_token,
                policy_id,
                role,
            );
            let present = self
                .runtime
                .private_file_volume_owned(&name, &labels)
                .await
                .map_err(|error| {
                    tracing::error!(
                        volume = name,
                        error = %error,
                        "refusing cleanup without exact Live egress private-volume ownership"
                    );
                    LiveEgressError::Cleanup
                })?;
            if present {
                owned_private_volumes.push((name, labels));
            }
        }
        let expected = [
            (worker_name, "worker"),
            (gateway_name, expected_gateway_role),
        ];
        let mut handles = Vec::with_capacity(expected.len());
        let mut ownership_failed = false;
        for (name, role) in expected {
            let handle = match self.runtime.get_container_handle(name).await {
                Ok(value) => value,
                Err(error) => {
                    tracing::error!(container = name, error = %error, "Live egress container lookup failed");
                    ownership_failed = true;
                    continue;
                }
            };
            let Some(handle) = handle else {
                continue;
            };
            let state = match self.runtime.describe_container_runtime_state(name).await {
                Ok(Some(state)) => state,
                Ok(None) => {
                    tracing::error!(
                        container = name,
                        "Live egress container ownership state is unavailable"
                    );
                    ownership_failed = true;
                    continue;
                }
                Err(error) => {
                    tracing::error!(container = name, error = %error, "Live egress container ownership inspection failed");
                    ownership_failed = true;
                    continue;
                }
            };
            if !runtime_owned_by_binding(
                &state,
                name,
                role,
                session_id,
                binding_id,
                control_fencing_token,
                policy_id,
            ) {
                tracing::error!(
                    container = name,
                    "refusing to clean a container without exact Live egress binding ownership"
                );
                ownership_failed = true;
                continue;
            }
            handles.push(handle);
        }
        if ownership_failed {
            return Err(LiveEgressError::Cleanup);
        }

        let mut cleanup_failed = false;
        for handle in handles {
            let name = handle.name.as_str();
            match self.runtime.get_container_handle(name).await {
                Ok(Some(current)) if current.id == handle.id => {}
                Ok(None) => continue,
                Ok(Some(_)) => {
                    tracing::error!(
                        container = name,
                        "Live egress container identity changed before cleanup"
                    );
                    cleanup_failed = true;
                    continue;
                }
                Err(error) => {
                    tracing::error!(container = name, error = %error, "Live egress container identity recheck failed");
                    cleanup_failed = true;
                    continue;
                }
            }
            if let Err(error) = self.runtime.stop_container(&handle).await {
                tracing::warn!(container = name, error = %error, "Live egress container stop failed; forcing removal");
            }
            if let Err(error) = self.runtime.remove_container(&handle).await {
                tracing::error!(container = name, error = %error, "Live egress container removal failed");
            }
            match self.runtime.get_container_handle(name).await {
                Ok(None) => {}
                Ok(Some(_)) => {
                    cleanup_failed = true;
                    tracing::error!(
                        container = name,
                        "Live egress container remains after removal"
                    );
                }
                Err(error) => {
                    cleanup_failed = true;
                    tracing::error!(container = name, error = %error, "Live egress container cleanup verification failed");
                }
            }
        }
        if cleanup_failed {
            return Err(LiveEgressError::Cleanup);
        }
        let mut volume_cleanup_failed = false;
        for (name, labels) in owned_private_volumes {
            if let Err(error) = self
                .runtime
                .remove_private_file_volume(&name, &labels)
                .await
            {
                tracing::error!(
                    volume = name,
                    error = %error,
                    "Live egress private-volume cleanup failed"
                );
                volume_cleanup_failed = true;
            }
        }
        if volume_cleanup_failed {
            return Err(LiveEgressError::Cleanup);
        }
        Ok(())
    }
}

type StartedBinding = (
    ContainerHandle,
    ContainerHandle,
    Arc<UpstreamFetcher>,
    String,
);

fn worker_name(session_id: Uuid) -> String {
    format!("elixir-live-egress-{}", session_id.simple())
}

fn control_volume_name(binding_id: Uuid) -> String {
    format!("elixir_live_egress_secret_{}", binding_id.simple())
}

fn control_volume_labels(
    session_id: Uuid,
    binding_id: Uuid,
    control_fencing_token: i64,
    policy_id: &str,
) -> HashMap<String, String> {
    private_volume_labels(
        session_id,
        binding_id,
        control_fencing_token,
        policy_id,
        "control_secret",
    )
}

fn material_volume_name(binding_id: Uuid, role: &str) -> String {
    format!("elixir_live_egress_{role}_{}", binding_id.simple())
}

fn private_volume_labels(
    session_id: Uuid,
    binding_id: Uuid,
    control_fencing_token: i64,
    policy_id: &str,
    role: &str,
) -> HashMap<String, String> {
    HashMap::from([
        (ELIXIR_MANAGED_LABEL.to_string(), "true".to_string()),
        (ROLE_LABEL.to_string(), role.to_string()),
        (SESSION_LABEL.to_string(), session_id.to_string()),
        (BINDING_LABEL.to_string(), binding_id.to_string()),
        (POLICY_LABEL.to_string(), policy_id.to_string()),
        (FENCE_LABEL.to_string(), control_fencing_token.to_string()),
    ])
}

fn warp_state_volume_labels(profile: &LiveEgressProfileConfig) -> HashMap<String, String> {
    HashMap::from([
        (ELIXIR_MANAGED_LABEL.to_string(), "true".to_string()),
        (ROLE_LABEL.to_string(), "warp_state".to_string()),
        (POLICY_LABEL.to_string(), profile.id.clone()),
        (POLICY_KIND_LABEL.to_string(), "warp".to_string()),
    ])
}

fn private_volume_roles(expected_gateway_role: &str) -> &'static [&'static str] {
    match expected_gateway_role {
        "wireguard_gateway" => &["control_secret", GATEWAY_CONFIG_ROLE],
        "openvpn_gateway" => &[
            "control_secret",
            GATEWAY_CONFIG_ROLE,
            OPENVPN_USERNAME_ROLE,
            OPENVPN_PASSWORD_ROLE,
        ],
        "warp_gateway" => &["control_secret"],
        _ => &[],
    }
}

fn profile_kind(kind: LiveEgressProfileKind) -> &'static str {
    match kind {
        LiveEgressProfileKind::Warp => "warp",
        LiveEgressProfileKind::Wireguard => "wireguard",
        LiveEgressProfileKind::Openvpn => "openvpn",
    }
}

fn gateway_role(kind: LiveEgressProfileKind) -> &'static str {
    match kind {
        LiveEgressProfileKind::Warp => "warp_gateway",
        LiveEgressProfileKind::Wireguard => "wireguard_gateway",
        LiveEgressProfileKind::Openvpn => "openvpn_gateway",
    }
}

fn gateway_role_from_mode(mode: &str) -> Option<&'static str> {
    match mode {
        "warp" => Some("warp_gateway"),
        "wireguard" => Some("wireguard_gateway"),
        "openvpn" => Some("openvpn_gateway"),
        _ => None,
    }
}

fn is_gateway_role(role: &str) -> bool {
    matches!(
        role,
        "warp_gateway" | "wireguard_gateway" | "openvpn_gateway"
    )
}

fn egress_error_label(error: &LiveEgressError) -> &'static str {
    match error {
        LiveEgressError::InvalidPolicy => "invalid_policy",
        LiveEgressError::ProfileUnavailable => "profile_unavailable",
        LiveEgressError::CapacityExhausted => "capacity_exhausted",
        LiveEgressError::StaleFence => "stale_fence",
        LiveEgressError::Database(_) => "database",
        LiveEgressError::PolicyRepository(_) => "policy_repository",
        LiveEgressError::Runtime => "runtime",
        LiveEgressError::Readiness => "readiness",
        LiveEgressError::Cleanup => "cleanup",
    }
}

fn project_builtin_profile(projected: &ActiveLiveEgressProfile) -> LiveEgressProfileConfig {
    let mut profile = LiveEgressProfileConfig {
        id: projected.profile_id.clone(),
        name: projected.name.clone(),
        gateway_image: projected.gateway_image.clone(),
        ..LiveEgressProfileConfig::default()
    };
    match &projected.kind {
        DownloadNetworkProfileKind::WireguardConfig => {
            profile.kind = LiveEgressProfileKind::Wireguard;
            profile.config_host_path = Some(PROJECTED_WIREGUARD_CONFIG_PATH.to_string());
        }
        DownloadNetworkProfileKind::OpenvpnConfig => {
            profile.kind = LiveEgressProfileKind::Openvpn;
            profile.config_host_path = Some(PROJECTED_OPENVPN_CONFIG_PATH.to_string());
            if projected.username_secret_ref.is_some() {
                profile.auth_host_path = Some(PROJECTED_OPENVPN_AUTH_PATH.to_string());
            }
        }
        DownloadNetworkProfileKind::CloudflareWarp => {
            profile.kind = LiveEgressProfileKind::Warp;
            profile.state_volume_name = Some(live_warp_state_volume_name(&projected.profile_id));
            profile.enrollment_id = projected.enrollment_id.clone();
            profile.identity_secret_ref = projected.identity_secret_ref.clone();
        }
        DownloadNetworkProfileKind::ExternalOnly
        | DownloadNetworkProfileKind::Direct
        | DownloadNetworkProfileKind::ProviderPreset
        | DownloadNetworkProfileKind::DebridOnly => {}
    }
    profile
}

fn install_projected_profile(
    config: &mut LiveEgressConfig,
    projected: &ActiveLiveEgressProfile,
) -> LiveEgressProfileConfig {
    let profile = project_builtin_profile(projected);
    config.default_mode = LiveEgressDefaultMode::PreferProtected;
    config.default_policy_id = Some(profile.id.clone());
    config.default_allow_fallback = true;
    config.profiles.push(profile.clone());
    profile
}

fn live_warp_state_volume_name(profile_id: &str) -> String {
    let digest = blake3::hash(profile_id.as_bytes()).to_hex().to_string();
    format!("elixir_live_warp_state_{}", &digest[..32])
}

fn gateway_runtime(profile: &LiveEgressProfileConfig) -> Result<GatewayRuntime, LiveEgressError> {
    match profile.kind {
        LiveEgressProfileKind::Wireguard => Ok(GatewayRuntime::GluetunWireguard(
            GluetunWireguardGatewayRuntime {
                image: profile.gateway_image.clone(),
                config_host_path: profile
                    .config_host_path
                    .clone()
                    .ok_or(LiveEgressError::InvalidPolicy)?,
            },
        )),
        LiveEgressProfileKind::Openvpn => Ok(GatewayRuntime::GluetunOpenvpn(
            GluetunOpenvpnGatewayRuntime {
                image: profile.gateway_image.clone(),
                config_host_path: profile
                    .config_host_path
                    .clone()
                    .ok_or(LiveEgressError::InvalidPolicy)?,
                auth_host_path: profile.auth_host_path.clone(),
            },
        )),
        LiveEgressProfileKind::Warp => Ok(GatewayRuntime::CloudflareWarp(
            CloudflareWarpGatewayRuntime {
                image: profile.gateway_image.clone(),
                state_volume_name: profile
                    .state_volume_name
                    .clone()
                    .ok_or(LiveEgressError::InvalidPolicy)?,
                enrollment_id: profile
                    .enrollment_id
                    .clone()
                    .ok_or(LiveEgressError::InvalidPolicy)?,
                identity_secret_ref: profile
                    .identity_secret_ref
                    .clone()
                    .ok_or(LiveEgressError::InvalidPolicy)?,
            },
        )),
    }
}

fn compile_live_topology(
    config: &LiveEgressConfig,
    session: &SessionRecord,
    binding_id: Uuid,
    profile: &LiveEgressProfileConfig,
    worker_name: &str,
    control_volume_name: &str,
) -> Result<crate::network::gateway::CompiledGatewayTopology, LiveEgressError> {
    let mut labels = HashMap::new();
    labels.insert(ELIXIR_MANAGED_LABEL.to_string(), "true".to_string());
    labels.insert(ROLE_LABEL.to_string(), "worker".to_string());
    labels.insert(SESSION_LABEL.to_string(), session.id.to_string());
    labels.insert(BINDING_LABEL.to_string(), binding_id.to_string());
    labels.insert(POLICY_LABEL.to_string(), profile.id.clone());
    labels.insert(
        FENCE_LABEL.to_string(),
        session.control_fencing_token.to_string(),
    );
    let app_spec = ContainerSpec {
        name: worker_name.to_string(),
        image: config.worker_image.clone(),
        network: config.network.clone(),
        network_mode: None,
        aliases: vec![worker_name.to_string()],
        env: vec![
            EnvVar {
                name: "ELIXIR_LIVE_EGRESS_SECRET_FILE".to_string(),
                value: WORKER_SECRET_PATH.to_string(),
            },
            EnvVar {
                name: "ELIXIR_LIVE_EGRESS_CONTROL_PORT".to_string(),
                value: config.control_port.to_string(),
            },
        ],
        volumes: vec![VolumeMount {
            source_kind: VolumeMountSourceKind::NamedVolume,
            host_path: control_volume_name.to_string(),
            container_path: WORKER_SECRET_ROOT.to_string(),
            read_only: true,
        }],
        ports: vec![PortMapping {
            container_port: config.control_port,
            host_port: Some(0),
            host_ip: Some("127.0.0.1".to_string()),
            protocol: Some("tcp".to_string()),
        }],
        labels: labels.clone(),
        command: Vec::new(),
        cap_add: Vec::new(),
        cap_drop: vec!["ALL".to_string()],
        devices: Vec::new(),
        sysctls: HashMap::new(),
        security: ContainerSecurityOptions {
            user: Some(format!("{WORKER_UID}:{WORKER_GID}")),
            read_only_rootfs: true,
            no_new_privileges: true,
            tmpfs: vec![ContainerTmpfsMount {
                path: "/tmp".to_string(),
                size_mb: Some(16),
            }],
            memory_limit_mb: Some(config.worker_memory_mb),
            pids_limit: Some(config.worker_pids_limit),
            ..Default::default()
        },
    };
    let runtime = gateway_runtime(profile)?;
    let mut topology = compile_gateway_topology(
        GatewayTopologyProfile {
            id: &profile.id,
            kind: profile_kind(profile.kind),
            runtime: &runtime,
        },
        GatewayTopologyCompileInput {
            app_container_name: worker_name,
            app_spec: &app_spec,
            base_labels: &labels,
            labels: GatewayTopologyLabels {
                role: ROLE_LABEL,
                profile_id: POLICY_LABEL,
                profile_kind: POLICY_KIND_LABEL,
                runtime_kind: RUNTIME_KIND_LABEL,
                exposed_ports: PORTS_LABEL,
            },
            error_subject: "Live egress profile",
        },
    )
    .map_err(|_| LiveEgressError::InvalidPolicy)?;
    let gateway = topology
        .gateway_spec
        .as_mut()
        .ok_or(LiveEgressError::InvalidPolicy)?;
    gateway
        .env
        .retain(|value| value.name != "FIREWALL_OUTBOUND_SUBNETS");
    if profile.kind == LiveEgressProfileKind::Wireguard {
        gateway.sysctls.insert(
            "net.ipv6.conf.all.disable_ipv6".to_string(),
            "0".to_string(),
        );
    }
    apply_live_material_mounts(gateway, profile, binding_id)?;
    apply_container_spec_fingerprint(gateway);
    Ok(topology)
}

fn apply_live_material_mounts(
    gateway: &mut ContainerSpec,
    profile: &LiveEgressProfileConfig,
    binding_id: Uuid,
) -> Result<(), LiveEgressError> {
    match profile.kind {
        LiveEgressProfileKind::Warp => {}
        LiveEgressProfileKind::Wireguard => {
            let mount = gateway
                .volumes
                .iter_mut()
                .find(|mount| mount.container_path == "/gluetun/wireguard/wg0.conf")
                .ok_or(LiveEgressError::InvalidPolicy)?;
            mount.source_kind = VolumeMountSourceKind::NamedVolume;
            mount.host_path = material_volume_name(binding_id, GATEWAY_CONFIG_ROLE);
            mount.container_path = WIREGUARD_CONFIG_ROOT.to_string();
            mount.read_only = true;
        }
        LiveEgressProfileKind::Openvpn => {
            gateway.volumes.retain(|mount| {
                mount.container_path != "/gluetun/custom.conf"
                    && mount.container_path != "/gluetun/auth.txt"
            });
            gateway.volumes.push(VolumeMount {
                source_kind: VolumeMountSourceKind::NamedVolume,
                host_path: material_volume_name(binding_id, GATEWAY_CONFIG_ROLE),
                container_path: OPENVPN_CONFIG_ROOT.to_string(),
                read_only: true,
            });
            let config = gateway
                .env
                .iter_mut()
                .find(|value| value.name == "OPENVPN_CUSTOM_CONFIG")
                .ok_or(LiveEgressError::InvalidPolicy)?;
            config.value = OPENVPN_CONFIG_PATH.to_string();
            if profile.auth_host_path.is_some() {
                gateway.volumes.extend([
                    VolumeMount {
                        source_kind: VolumeMountSourceKind::NamedVolume,
                        host_path: material_volume_name(binding_id, OPENVPN_USERNAME_ROLE),
                        container_path: OPENVPN_USERNAME_ROOT.to_string(),
                        read_only: true,
                    },
                    VolumeMount {
                        source_kind: VolumeMountSourceKind::NamedVolume,
                        host_path: material_volume_name(binding_id, OPENVPN_PASSWORD_ROLE),
                        container_path: OPENVPN_PASSWORD_ROOT.to_string(),
                        read_only: true,
                    },
                ]);
                gateway.env.extend([
                    EnvVar {
                        name: "OPENVPN_USER_SECRETFILE".to_string(),
                        value: OPENVPN_USERNAME_PATH.to_string(),
                    },
                    EnvVar {
                        name: "OPENVPN_PASSWORD_SECRETFILE".to_string(),
                        value: OPENVPN_PASSWORD_PATH.to_string(),
                    },
                ]);
            }
        }
    }
    Ok(())
}

fn verify_runtime_state(
    session: &SessionRecord,
    binding_id: Uuid,
    profile: &LiveEgressProfileConfig,
    worker: &crate::runtime::model::ContainerRuntimeState,
    gateway: &crate::runtime::model::ContainerRuntimeState,
    gateway_handle: &ContainerHandle,
    expected_security: &ContainerSecurityOptions,
    control_port: u16,
) -> Result<(), LiveEgressError> {
    let gateway_name = gateway_handle.name.as_str();
    let binding_id = binding_id.to_string();
    let expected_worker_name = worker_name(session.id);
    let worker_labels_match = runtime_owned_by_binding(
        worker,
        &expected_worker_name,
        "worker",
        session.id,
        &binding_id,
        session.control_fencing_token,
        &profile.id,
    );
    let gateway_labels_match = runtime_owned_by_binding(
        gateway,
        gateway_name,
        gateway_role(profile.kind),
        session.id,
        &binding_id,
        session.control_fencing_token,
        &profile.id,
    );
    let security = &worker.security;
    let expected_memory = expected_security
        .memory_limit_mb
        .and_then(|value| i64::try_from(value.saturating_mul(1024 * 1024)).ok());
    let expected_pids = expected_security
        .pids_limit
        .and_then(|value| i64::try_from(value).ok());
    let security_match = security.user == expected_security.user
        && security.read_only_rootfs
        && security.no_new_privileges
        && security.cap_drop.iter().any(|value| value == "ALL")
        && security.memory_limit_bytes == expected_memory
        && security.pids_limit == expected_pids;
    let port_match = gateway.published_ports.iter().any(|port| {
        port.container_port == control_port
            && port.protocol == "tcp"
            && port.host_ip.as_deref() == Some("127.0.0.1")
    });
    let expected_control_volume =
        control_volume_name(binding_id.parse().map_err(|_| LiveEgressError::Runtime)?);
    let control_volume_match = worker.mounts.iter().any(|mount| {
        mount.mount_type == "volume"
            && mount.name.as_deref() == Some(expected_control_volume.as_str())
            && mount.destination == WORKER_SECRET_ROOT
            && mount.read_only
    });
    let gateway_material_match = gateway_material_mounts_match(profile, &binding_id, gateway);
    if !shares_container_network_namespace(worker.network_mode.as_deref(), gateway_handle)
        || !worker_labels_match
        || !gateway_labels_match
        || !security_match
        || !control_volume_match
        || !gateway_material_match
        || !port_match
    {
        return Err(LiveEgressError::Runtime);
    }
    Ok(())
}

fn shares_container_network_namespace(
    network_mode: Option<&str>,
    gateway: &ContainerHandle,
) -> bool {
    let Some(target) = network_mode.and_then(|mode| mode.strip_prefix("container:")) else {
        return false;
    };

    // Docker inspect resolves a name-based namespace target to the container's full ID.
    !target.is_empty() && (target == gateway.id || target == gateway.name)
}

fn gateway_material_mounts_match(
    profile: &LiveEgressProfileConfig,
    binding_id: &str,
    gateway: &crate::runtime::model::ContainerRuntimeState,
) -> bool {
    let Ok(binding_id) = binding_id.parse::<Uuid>() else {
        return false;
    };
    let expected = match profile.kind {
        LiveEgressProfileKind::Warp => return true,
        LiveEgressProfileKind::Wireguard => vec![(
            material_volume_name(binding_id, GATEWAY_CONFIG_ROLE),
            WIREGUARD_CONFIG_ROOT,
        )],
        LiveEgressProfileKind::Openvpn => {
            let mut expected = vec![(
                material_volume_name(binding_id, GATEWAY_CONFIG_ROLE),
                OPENVPN_CONFIG_ROOT,
            )];
            if profile.auth_host_path.is_some() {
                expected.extend([
                    (
                        material_volume_name(binding_id, OPENVPN_USERNAME_ROLE),
                        OPENVPN_USERNAME_ROOT,
                    ),
                    (
                        material_volume_name(binding_id, OPENVPN_PASSWORD_ROLE),
                        OPENVPN_PASSWORD_ROOT,
                    ),
                ]);
            }
            expected
        }
    };
    expected.iter().all(|(name, destination)| {
        gateway.mounts.iter().any(|mount| {
            mount.mount_type == "volume"
                && mount.name.as_deref() == Some(name.as_str())
                && mount.destination == *destination
                && mount.read_only
        })
    })
}

#[allow(clippy::too_many_arguments)]
fn runtime_owned_by_binding(
    state: &crate::runtime::model::ContainerRuntimeState,
    expected_name: &str,
    expected_role: &str,
    session_id: Uuid,
    binding_id: &str,
    control_fencing_token: i64,
    policy_id: &str,
) -> bool {
    let session_id = session_id.to_string();
    let control_fencing_token = control_fencing_token.to_string();
    state.name == expected_name
        && state.labels.get(ELIXIR_MANAGED_LABEL).map(String::as_str) == Some("true")
        && state.labels.get(ROLE_LABEL).map(String::as_str) == Some(expected_role)
        && state.labels.get(SESSION_LABEL).map(String::as_str) == Some(session_id.as_str())
        && state.labels.get(BINDING_LABEL).map(String::as_str) == Some(binding_id)
        && state.labels.get(POLICY_LABEL).map(String::as_str) == Some(policy_id)
        && state.labels.get(FENCE_LABEL).map(String::as_str) == Some(control_fencing_token.as_str())
}

async fn wait_running(
    runtime: &dyn RuntimeManager,
    handle: &ContainerHandle,
    timeout: Duration,
) -> Result<(), LiveEgressError> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if runtime
            .inspect(handle)
            .await
            .map_err(|_| LiveEgressError::Runtime)?
            .running
        {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(LiveEgressError::Runtime);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn protected_limits() -> UpstreamLimits {
    UpstreamLimits {
        connect_timeout: Duration::from_secs(5),
        header_timeout: Duration::from_secs(10),
        idle_timeout: Duration::from_secs(15),
        total_timeout: Duration::from_secs(120),
        max_response_bytes: 4_u64 * 1024 * 1024 * 1024,
        max_response_headers: 64,
        max_response_header_bytes: 32 * 1024,
        max_redirects: 5,
    }
}

fn absolute_path(value: &str) -> Result<PathBuf, LiveEgressError> {
    let path = PathBuf::from(value);
    if path.is_absolute() {
        Ok(path)
    } else {
        std::env::current_dir()
            .map(|current| current.join(path))
            .map_err(|_| LiveEgressError::Runtime)
    }
}

fn prepare_control_root(path: &Path) -> Result<(), LiveEgressError> {
    if !path.is_absolute() {
        return Err(LiveEgressError::Runtime);
    }
    if !path.exists() {
        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt as _;
            builder.mode(0o700);
        }
        builder.create(path).map_err(|_| LiveEgressError::Runtime)?;
    }
    let metadata = std::fs::symlink_metadata(path).map_err(|_| LiveEgressError::Runtime)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(LiveEgressError::Runtime);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(LiveEgressError::Runtime);
        }
    }
    Ok(())
}

fn write_control_secret(
    directory: &Path,
    path: &Path,
    secret: &ControlSecretDocument,
) -> Result<(), LiveEgressError> {
    let mut directory_builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt as _;
        directory_builder.mode(0o700);
    }
    directory_builder
        .create(directory)
        .map_err(|_| LiveEgressError::Runtime)?;
    let body =
        Zeroizing::new(serde_json::to_vec(secret).map_err(|_| LiveEgressError::InvalidPolicy)?);
    write_private_file(path, &body)
}

fn write_private_file(path: &Path, body: &[u8]) -> Result<(), LiveEgressError> {
    if body.is_empty() {
        return Err(LiveEgressError::InvalidPolicy);
    }
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path).map_err(|_| LiveEgressError::Runtime)?;
    file.write_all(&body)
        .map_err(|_| LiveEgressError::Runtime)?;
    file.sync_all().map_err(|_| LiveEgressError::Runtime)?;
    #[cfg(unix)]
    std::fs::File::open(path.parent().ok_or(LiveEgressError::Runtime)?)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| LiveEgressError::Runtime)?;
    container_identity(path).map(|_| ())
}

#[cfg(unix)]
fn container_identity(path: &Path) -> Result<String, LiveEgressError> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
    let metadata = std::fs::symlink_metadata(path).map_err(|_| LiveEgressError::Runtime)?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.permissions().mode() & 0o777 != 0o600
        || metadata.nlink() != 1
    {
        return Err(LiveEgressError::Runtime);
    }
    Ok(format!("{}:{}", metadata.uid(), metadata.gid()))
}

#[cfg(not(unix))]
fn container_identity(_path: &Path) -> Result<String, LiveEgressError> {
    Ok("65532:65532".to_string())
}

async fn remove_secret_dir(path: &Path) -> Result<(), LiveEgressError> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
        Ok(_) => return Err(LiveEgressError::Cleanup),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(_) => return Err(LiveEgressError::Cleanup),
    }
    tokio::fs::remove_dir_all(path)
        .await
        .map_err(|_| LiveEgressError::Cleanup)?;
    match tokio::fs::symlink_metadata(path).await {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        _ => Err(LiveEgressError::Cleanup),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool as StdAtomicBool, Ordering as StdOrdering};

    use async_trait::async_trait;
    use chrono::Duration as ChronoDuration;

    use crate::live::session::{DeliveryMode, SessionOwner, SessionProtocol, SessionState};

    use super::*;

    #[test]
    fn live_runtime_namespace_accepts_exact_gateway_identity() {
        let gateway = ContainerHandle {
            id: "66294ecf8051959686cf9c5b06970c85189a1850b40b068dd30405fa74f03761".to_string(),
            name: "elixir-live-egress-gateway".to_string(),
        };

        assert!(shares_container_network_namespace(
            Some("container:66294ecf8051959686cf9c5b06970c85189a1850b40b068dd30405fa74f03761"),
            &gateway,
        ));
        assert!(shares_container_network_namespace(
            Some("container:elixir-live-egress-gateway"),
            &gateway,
        ));
    }

    #[test]
    fn live_runtime_namespace_rejects_any_other_target() {
        let gateway = ContainerHandle {
            id: "66294ecf8051959686cf9c5b06970c85189a1850b40b068dd30405fa74f03761".to_string(),
            name: "elixir-live-egress-gateway".to_string(),
        };

        for network_mode in [
            None,
            Some("bridge"),
            Some("host"),
            Some("container:"),
            Some("container:66294ecf8051"),
            Some("container:unrelated-gateway"),
            Some("container:elixir-live-egress-gateway-other"),
        ] {
            assert!(!shares_container_network_namespace(network_mode, &gateway));
        }
    }

    #[test]
    fn live_builtin_projection_prefers_privacy_without_blocking_playback() {
        let projected = ActiveLiveEgressProfile {
            profile_id: "builtin-wg".to_string(),
            name: "Built-in WireGuard".to_string(),
            kind: DownloadNetworkProfileKind::WireguardConfig,
            gateway_image: "gluetun:test".to_string(),
            config_secret_ref: Some("global:builtin_wg".to_string()),
            username_secret_ref: None,
            password_secret_ref: None,
            enrollment_id: None,
            identity_secret_ref: None,
        };
        let mut config = LiveEgressConfig::default();
        let profile = install_projected_profile(&mut config, &projected);
        assert_eq!(config.default_mode, LiveEgressDefaultMode::PreferProtected);
        assert_eq!(config.default_policy_id.as_deref(), Some("builtin-wg"));
        assert!(config.default_allow_fallback);
        assert!(!profile.selectable_by_profiles);
        assert!(profile.expected_egress_ips.is_empty());
        assert_eq!(
            profile.config_host_path.as_deref(),
            Some(PROJECTED_WIREGUARD_CONFIG_PATH)
        );

        let warp = project_builtin_profile(&ActiveLiveEgressProfile {
            profile_id: "builtin-warp".to_string(),
            name: "Built-in WARP".to_string(),
            kind: DownloadNetworkProfileKind::CloudflareWarp,
            gateway_image: "warp:test".to_string(),
            config_secret_ref: None,
            username_secret_ref: None,
            password_secret_ref: None,
            enrollment_id: Some("live-enrollment".to_string()),
            identity_secret_ref: Some("global:warp_identity".to_string()),
        });
        let state_volume = warp.state_volume_name.expect("Live WARP state volume");
        assert!(state_volume.starts_with("elixir_live_warp_state_"));
        assert!(!state_volume.starts_with("elixir_warp_state_"));
    }

    #[tokio::test]
    async fn explicit_live_profiles_take_precedence_over_builtin_projection() {
        sqlx::any::install_default_drivers();
        let pool = sqlx::AnyPool::connect_lazy("sqlite::memory:").expect("lazy database pool");
        let temporary = tempfile::tempdir().expect("temporary directory");
        let config = LiveEgressConfig {
            control_root: temporary
                .path()
                .join("control")
                .to_string_lossy()
                .into_owned(),
            default_mode: LiveEgressDefaultMode::RequireProtected,
            default_policy_id: Some("explicit-wg".to_string()),
            profiles: vec![LiveEgressProfileConfig {
                id: "explicit-wg".to_string(),
                name: "Explicit WireGuard".to_string(),
                kind: LiveEgressProfileKind::Wireguard,
                gateway_image: "gluetun:test".to_string(),
                config_host_path: Some("/private/explicit-wg.conf".to_string()),
                expected_egress_ips: vec!["1.1.1.1".parse().expect("public IP")],
                ..LiveEgressProfileConfig::default()
            }],
            ..LiveEgressConfig::default()
        };
        let audit = Arc::new(LiveAuditChain::new(
            crate::live::admin::LiveAuditKey::new("explicit-egress", [41_u8; 32])
                .expect("audit key"),
        ));
        let service = LiveEgressService::new_with_builtin_fallback(
            pool,
            Arc::new(CleanupRuntime::default()),
            config,
            audit,
            Arc::new(SecretsManager::from_key_bytes([42_u8; 32], true)),
        )
        .await
        .expect("egress constructor")
        .expect("explicit egress service");

        assert!(service.projected.get().is_none());
        let status = service.status();
        assert_eq!(status.default_policy_id.as_deref(), Some("explicit-wg"));
        assert_eq!(status.default_mode, LiveEgressDefaultMode::RequireProtected);
        assert_eq!(status.profiles.len(), 1);
    }

    #[tokio::test]
    async fn missing_builtin_profile_keeps_a_dormant_service_for_lazy_refresh() {
        sqlx::any::install_default_drivers();
        let pool = sqlx::AnyPool::connect_lazy("sqlite::memory:").expect("lazy database pool");
        let temporary = tempfile::tempdir().expect("temporary directory");
        let config = LiveEgressConfig {
            control_root: temporary
                .path()
                .join("control")
                .to_string_lossy()
                .into_owned(),
            ..LiveEgressConfig::default()
        };
        let audit = Arc::new(LiveAuditChain::new(
            crate::live::admin::LiveAuditKey::new("dormant-egress", [43_u8; 32])
                .expect("audit key"),
        ));
        let service = LiveEgressService::new_with_builtin_fallback(
            pool,
            Arc::new(CleanupRuntime::default()),
            config,
            audit,
            Arc::new(SecretsManager::from_key_bytes([44_u8; 32], true)),
        )
        .await
        .expect("egress constructor")
        .expect("dormant egress service");

        let status = service.status();
        assert!(!status.enabled);
        assert!(!status.ready);
        assert_eq!(status.default_mode, LiveEgressDefaultMode::Off);
        assert!(status.profiles.is_empty());
    }

    #[derive(Default)]
    struct CleanupRuntime {
        containers: std::sync::Mutex<HashMap<String, ContainerHandle>>,
        states: std::sync::Mutex<HashMap<String, crate::runtime::model::ContainerRuntimeState>>,
        private_volumes: std::sync::Mutex<HashMap<String, HashMap<String, String>>>,
        stop_calls: std::sync::Mutex<Vec<String>>,
        remove_calls: std::sync::Mutex<Vec<String>>,
        volume_remove_calls: std::sync::Mutex<Vec<String>>,
        remove_fails: StdAtomicBool,
    }

    impl CleanupRuntime {
        fn insert(&self, name: &str, labels: HashMap<String, String>) {
            self.containers.lock().expect("container lock").insert(
                name.to_string(),
                ContainerHandle {
                    id: format!("id-{name}"),
                    name: name.to_string(),
                },
            );
            self.states.lock().expect("state lock").insert(
                name.to_string(),
                crate::runtime::model::ContainerRuntimeState {
                    name: name.to_string(),
                    network_mode: None,
                    labels,
                    mounts: Vec::new(),
                    published_ports: Vec::new(),
                    security: Default::default(),
                },
            );
        }
    }

    #[async_trait]
    impl RuntimeManager for CleanupRuntime {
        async fn ensure_network(&self, _name: &str) -> anyhow::Result<()> {
            Ok(())
        }

        async fn ensure_container(&self, spec: &ContainerSpec) -> anyhow::Result<ContainerHandle> {
            let handle = ContainerHandle {
                id: format!("id-{}", spec.name),
                name: spec.name.clone(),
            };
            self.containers
                .lock()
                .expect("container lock")
                .insert(handle.name.clone(), handle.clone());
            self.states.lock().expect("state lock").insert(
                handle.name.clone(),
                crate::runtime::model::ContainerRuntimeState {
                    name: handle.name.clone(),
                    network_mode: spec.network_mode.clone(),
                    labels: spec.labels.clone(),
                    mounts: Vec::new(),
                    published_ports: Vec::new(),
                    security: Default::default(),
                },
            );
            Ok(handle)
        }

        async fn create_private_file_volume(
            &self,
            spec: &PrivateFileVolumeSpec,
        ) -> anyhow::Result<()> {
            let mut volumes = self.private_volumes.lock().expect("private volume lock");
            if volumes.contains_key(&spec.name) {
                anyhow::bail!("private volume already exists");
            }
            volumes.insert(spec.name.clone(), spec.labels.clone());
            Ok(())
        }

        async fn private_file_volume_owned(
            &self,
            name: &str,
            required_labels: &HashMap<String, String>,
        ) -> anyhow::Result<bool> {
            let volumes = self.private_volumes.lock().expect("private volume lock");
            let Some(labels) = volumes.get(name) else {
                return Ok(false);
            };
            if required_labels
                .iter()
                .any(|(key, value)| labels.get(key) != Some(value))
            {
                anyhow::bail!("private volume ownership mismatch");
            }
            Ok(true)
        }

        async fn remove_private_file_volume(
            &self,
            name: &str,
            required_labels: &HashMap<String, String>,
        ) -> anyhow::Result<()> {
            if !self
                .private_file_volume_owned(name, required_labels)
                .await?
            {
                return Ok(());
            }
            self.volume_remove_calls
                .lock()
                .expect("private volume remove call lock")
                .push(name.to_string());
            self.private_volumes
                .lock()
                .expect("private volume lock")
                .remove(name);
            Ok(())
        }

        async fn get_container_handle(
            &self,
            name: &str,
        ) -> anyhow::Result<Option<ContainerHandle>> {
            Ok(self
                .containers
                .lock()
                .expect("container lock")
                .get(name)
                .cloned())
        }

        async fn start_container(&self, _handle: &ContainerHandle) -> anyhow::Result<()> {
            Ok(())
        }

        async fn stop_container(&self, handle: &ContainerHandle) -> anyhow::Result<()> {
            self.stop_calls
                .lock()
                .expect("stop call lock")
                .push(handle.name.clone());
            Ok(())
        }

        async fn rename_container(
            &self,
            handle: &ContainerHandle,
            new_name: &str,
        ) -> anyhow::Result<ContainerHandle> {
            self.containers
                .lock()
                .expect("container lock")
                .remove(&handle.name);
            self.states.lock().expect("state lock").remove(&handle.name);
            let renamed = ContainerHandle {
                id: handle.id.clone(),
                name: new_name.to_string(),
            };
            self.containers
                .lock()
                .expect("container lock")
                .insert(new_name.to_string(), renamed.clone());
            Ok(renamed)
        }

        async fn remove_container(&self, handle: &ContainerHandle) -> anyhow::Result<()> {
            self.remove_calls
                .lock()
                .expect("remove call lock")
                .push(handle.name.clone());
            if self.remove_fails.load(StdOrdering::Acquire) {
                anyhow::bail!("injected container removal failure");
            }
            self.containers
                .lock()
                .expect("container lock")
                .remove(&handle.name);
            self.states.lock().expect("state lock").remove(&handle.name);
            Ok(())
        }

        async fn container_logs(
            &self,
            _handle: &ContainerHandle,
            _since: Option<chrono::DateTime<Utc>>,
        ) -> anyhow::Result<String> {
            Ok(String::new())
        }

        async fn inspect(
            &self,
            handle: &ContainerHandle,
        ) -> anyhow::Result<crate::runtime::model::ContainerState> {
            Ok(crate::runtime::model::ContainerState {
                id: handle.id.clone(),
                name: handle.name.clone(),
                status: "running".to_string(),
                running: true,
                health: Some("healthy".to_string()),
            })
        }

        async fn describe_container_runtime_state(
            &self,
            container_name: &str,
        ) -> anyhow::Result<Option<crate::runtime::model::ContainerRuntimeState>> {
            Ok(self
                .states
                .lock()
                .expect("state lock")
                .get(container_name)
                .cloned())
        }

        async fn read_container_file(
            &self,
            _handle: &ContainerHandle,
            _path: &str,
        ) -> anyhow::Result<Option<Vec<u8>>> {
            Ok(None)
        }

        async fn copy_host_path_to_container(
            &self,
            _handle: &ContainerHandle,
            _source_path: &Path,
            _destination_path: &str,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        async fn ensure_container_directories(
            &self,
            _handle: &ContainerHandle,
            _paths: &[String],
        ) -> anyhow::Result<()> {
            Ok(())
        }

        async fn ensure_container_directories_owned_like(
            &self,
            _handle: &ContainerHandle,
            _reference_path: &str,
            _paths: &[String],
        ) -> anyhow::Result<bool> {
            Ok(true)
        }
    }

    fn cleanup_labels(
        session_id: Uuid,
        binding_id: &str,
        control_fencing_token: i64,
        policy_id: &str,
        role: &str,
    ) -> HashMap<String, String> {
        HashMap::from([
            (ELIXIR_MANAGED_LABEL.to_string(), "true".to_string()),
            (ROLE_LABEL.to_string(), role.to_string()),
            (SESSION_LABEL.to_string(), session_id.to_string()),
            (BINDING_LABEL.to_string(), binding_id.to_string()),
            (POLICY_LABEL.to_string(), policy_id.to_string()),
            (FENCE_LABEL.to_string(), control_fencing_token.to_string()),
        ])
    }

    #[tokio::test]
    async fn n11_cleanup_runtime_requires_verified_container_absence() {
        sqlx::any::install_default_drivers();
        let pool = sqlx::AnyPool::connect_lazy("sqlite::memory:").expect("lazy database pool");
        let runtime = Arc::new(CleanupRuntime::default());
        let session_id = Uuid::new_v4();
        let binding_id = Uuid::new_v4().to_string();
        let binding_uuid = binding_id.parse::<Uuid>().expect("binding UUID");
        let policy_id = "protected-policy";
        let fence = 7;
        let mut volumes = runtime.private_volumes.lock().expect("private volume lock");
        volumes.insert(
            control_volume_name(binding_uuid),
            control_volume_labels(session_id, binding_uuid, fence, policy_id),
        );
        volumes.insert(
            material_volume_name(binding_uuid, GATEWAY_CONFIG_ROLE),
            private_volume_labels(
                session_id,
                binding_uuid,
                fence,
                policy_id,
                GATEWAY_CONFIG_ROLE,
            ),
        );
        drop(volumes);
        runtime.insert(
            "worker",
            cleanup_labels(session_id, &binding_id, fence, policy_id, "worker"),
        );
        runtime.insert(
            "gateway",
            cleanup_labels(
                session_id,
                &binding_id,
                fence,
                policy_id,
                "wireguard_gateway",
            ),
        );
        runtime.remove_fails.store(true, StdOrdering::Release);
        let temporary = tempfile::tempdir().expect("temporary directory");
        let config = LiveEgressConfig {
            control_root: temporary
                .path()
                .join("control")
                .to_string_lossy()
                .into_owned(),
            ..LiveEgressConfig::default()
        };
        let audit = Arc::new(LiveAuditChain::new(
            crate::live::admin::LiveAuditKey::new("n11-cleanup", [19_u8; 32]).expect("audit key"),
        ));
        let service =
            LiveEgressService::new(pool, runtime.clone(), config, audit).expect("egress service");

        assert!(
            service
                .cleanup_runtime(
                    session_id,
                    &binding_id,
                    fence,
                    policy_id,
                    "worker",
                    "gateway",
                    "wireguard_gateway",
                )
                .await
                .is_err()
        );
        assert_eq!(runtime.containers.lock().expect("container lock").len(), 2);
        assert_eq!(
            runtime
                .private_volumes
                .lock()
                .expect("private volume lock")
                .len(),
            2
        );

        runtime.remove_fails.store(false, StdOrdering::Release);
        service
            .cleanup_runtime(
                session_id,
                &binding_id,
                fence,
                policy_id,
                "worker",
                "gateway",
                "wireguard_gateway",
            )
            .await
            .expect("verified cleanup");
        assert!(
            runtime
                .containers
                .lock()
                .expect("container lock")
                .is_empty()
        );
        assert!(
            runtime
                .private_volumes
                .lock()
                .expect("private volume lock")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn n11_cleanup_runtime_never_mutates_a_foreign_name_collision() {
        sqlx::any::install_default_drivers();
        let pool = sqlx::AnyPool::connect_lazy("sqlite::memory:").expect("lazy database pool");
        let runtime = Arc::new(CleanupRuntime::default());
        let session_id = Uuid::new_v4();
        let binding_id = Uuid::new_v4().to_string();
        let policy_id = "protected-policy";
        let fence = 11;
        runtime.insert(
            "worker",
            cleanup_labels(
                session_id,
                &Uuid::new_v4().to_string(),
                fence,
                policy_id,
                "worker",
            ),
        );
        runtime.insert(
            "gateway",
            cleanup_labels(
                session_id,
                &binding_id,
                fence,
                policy_id,
                "wireguard_gateway",
            ),
        );
        let temporary = tempfile::tempdir().expect("temporary directory");
        let config = LiveEgressConfig {
            control_root: temporary
                .path()
                .join("control")
                .to_string_lossy()
                .into_owned(),
            ..LiveEgressConfig::default()
        };
        let audit = Arc::new(LiveAuditChain::new(
            crate::live::admin::LiveAuditKey::new("n11-ownership", [23_u8; 32]).expect("audit key"),
        ));
        let service =
            LiveEgressService::new(pool, runtime.clone(), config, audit).expect("egress service");

        assert!(
            service
                .cleanup_runtime(
                    session_id,
                    &binding_id,
                    fence,
                    policy_id,
                    "worker",
                    "gateway",
                    "wireguard_gateway",
                )
                .await
                .is_err()
        );
        assert_eq!(runtime.containers.lock().expect("container lock").len(), 2);
        assert!(
            runtime
                .stop_calls
                .lock()
                .expect("stop call lock")
                .is_empty()
        );
        assert!(
            runtime
                .remove_calls
                .lock()
                .expect("remove call lock")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn n11_cleanup_runtime_never_mutates_with_a_foreign_control_volume() {
        sqlx::any::install_default_drivers();
        let pool = sqlx::AnyPool::connect_lazy("sqlite::memory:").expect("lazy database pool");
        let runtime = Arc::new(CleanupRuntime::default());
        let session_id = Uuid::new_v4();
        let binding_uuid = Uuid::new_v4();
        let binding_id = binding_uuid.to_string();
        let policy_id = "protected-policy";
        let fence = 13;
        runtime.insert(
            "worker",
            cleanup_labels(session_id, &binding_id, fence, policy_id, "worker"),
        );
        runtime.insert(
            "gateway",
            cleanup_labels(
                session_id,
                &binding_id,
                fence,
                policy_id,
                "wireguard_gateway",
            ),
        );
        runtime
            .private_volumes
            .lock()
            .expect("private volume lock")
            .insert(
                control_volume_name(binding_uuid),
                control_volume_labels(session_id, Uuid::new_v4(), fence, policy_id),
            );
        let temporary = tempfile::tempdir().expect("temporary directory");
        let config = LiveEgressConfig {
            control_root: temporary
                .path()
                .join("control")
                .to_string_lossy()
                .into_owned(),
            ..LiveEgressConfig::default()
        };
        let audit = Arc::new(LiveAuditChain::new(
            crate::live::admin::LiveAuditKey::new("n11-volume-ownership", [29_u8; 32])
                .expect("audit key"),
        ));
        let service =
            LiveEgressService::new(pool, runtime.clone(), config, audit).expect("egress service");

        assert!(
            service
                .cleanup_runtime(
                    session_id,
                    &binding_id,
                    fence,
                    policy_id,
                    "worker",
                    "gateway",
                    "wireguard_gateway",
                )
                .await
                .is_err()
        );
        assert!(
            runtime
                .stop_calls
                .lock()
                .expect("stop call lock")
                .is_empty()
        );
        assert!(
            runtime
                .remove_calls
                .lock()
                .expect("remove call lock")
                .is_empty()
        );
        assert!(
            runtime
                .volume_remove_calls
                .lock()
                .expect("private volume remove call lock")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn n11_cleanup_runtime_never_mutates_with_a_foreign_gateway_material_volume() {
        sqlx::any::install_default_drivers();
        let pool = sqlx::AnyPool::connect_lazy("sqlite::memory:").expect("lazy database pool");
        let runtime = Arc::new(CleanupRuntime::default());
        let session_id = Uuid::new_v4();
        let binding_uuid = Uuid::new_v4();
        let binding_id = binding_uuid.to_string();
        let policy_id = "protected-policy";
        let fence = 17;
        runtime.insert(
            "worker",
            cleanup_labels(session_id, &binding_id, fence, policy_id, "worker"),
        );
        runtime.insert(
            "gateway",
            cleanup_labels(session_id, &binding_id, fence, policy_id, "openvpn_gateway"),
        );
        let mut volumes = runtime.private_volumes.lock().expect("private volume lock");
        volumes.insert(
            control_volume_name(binding_uuid),
            control_volume_labels(session_id, binding_uuid, fence, policy_id),
        );
        volumes.insert(
            material_volume_name(binding_uuid, GATEWAY_CONFIG_ROLE),
            private_volume_labels(
                session_id,
                Uuid::new_v4(),
                fence,
                policy_id,
                GATEWAY_CONFIG_ROLE,
            ),
        );
        drop(volumes);
        let temporary = tempfile::tempdir().expect("temporary directory");
        let config = LiveEgressConfig {
            control_root: temporary
                .path()
                .join("control")
                .to_string_lossy()
                .into_owned(),
            ..LiveEgressConfig::default()
        };
        let audit = Arc::new(LiveAuditChain::new(
            crate::live::admin::LiveAuditKey::new("n11-material-ownership", [31_u8; 32])
                .expect("audit key"),
        ));
        let service =
            LiveEgressService::new(pool, runtime.clone(), config, audit).expect("egress service");

        assert!(
            service
                .cleanup_runtime(
                    session_id,
                    &binding_id,
                    fence,
                    policy_id,
                    "worker",
                    "gateway",
                    "openvpn_gateway",
                )
                .await
                .is_err()
        );
        assert!(
            runtime
                .stop_calls
                .lock()
                .expect("stop call lock")
                .is_empty()
        );
        assert!(
            runtime
                .remove_calls
                .lock()
                .expect("remove call lock")
                .is_empty()
        );
        assert!(
            runtime
                .volume_remove_calls
                .lock()
                .expect("private volume remove call lock")
                .is_empty()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn n11_control_root_and_secret_cleanup_reject_public_or_symbolic_paths() {
        use std::os::unix::fs::{PermissionsExt as _, symlink};

        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("control-root");
        prepare_control_root(&root).expect("private control root");
        assert_eq!(
            std::fs::metadata(&root)
                .expect("root metadata")
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755))
            .expect("public root");
        assert!(prepare_control_root(&root).is_err());

        let secret_dir = temporary.path().join("secret-dir");
        std::fs::create_dir(&secret_dir).expect("secret directory");
        std::fs::write(secret_dir.join("control.json"), b"secret").expect("secret file");
        remove_secret_dir(&secret_dir)
            .await
            .expect("verified secret cleanup");
        assert!(!secret_dir.exists());

        let target = temporary.path().join("target");
        std::fs::create_dir(&target).expect("target directory");
        let symbolic = temporary.path().join("symbolic-secret-dir");
        symlink(&target, &symbolic).expect("symbolic directory");
        assert!(remove_secret_dir(&symbolic).await.is_err());
        assert!(target.is_dir());
    }

    fn session() -> SessionRecord {
        let now = Utc::now();
        SessionRecord {
            id: Uuid::new_v4(),
            owner: SessionOwner {
                user_id: Uuid::new_v4(),
                home_id: Uuid::new_v4(),
                profile_id: Uuid::new_v4(),
                account_session_id: Uuid::new_v4(),
                provider_id: Uuid::new_v4(),
            },
            delivery_mode: DeliveryMode::ServerRelay,
            protocol: SessionProtocol::Hls,
            state: SessionState::Planning,
            revision: 2,
            token_revision: 1,
            control_fencing_token: 7,
            source_index: 0,
            failover_count: 0,
            refresh_count: 0,
            egress_binding_id: None,
            remux_job_id: None,
            created_at: now,
            last_heartbeat_at: now,
            expires_at: now + ChronoDuration::minutes(2),
            hard_expires_at: now + ChronoDuration::hours(1),
            ended_at: None,
            error_code: None,
            error_detail_redacted: None,
        }
    }

    #[test]
    fn n11_live_topology_is_session_isolated_loopback_only_and_least_privilege() {
        let session = session();
        let config = LiveEgressConfig {
            worker_image: "elixir-live-egress-worker:test".to_string(),
            network: "elixir_live_egress_test".to_string(),
            control_port: 18_080,
            worker_memory_mb: 192,
            worker_pids_limit: 48,
            ..LiveEgressConfig::default()
        };
        let profile = LiveEgressProfileConfig {
            id: "wireguard-live-test".to_string(),
            name: "WireGuard Live test".to_string(),
            kind: LiveEgressProfileKind::Wireguard,
            gateway_image: "gluetun:test".to_string(),
            config_host_path: Some("/run/elixir/live/wg0.conf".to_string()),
            expected_egress_ips: vec!["1.1.1.1".parse().unwrap()],
            ..LiveEgressProfileConfig::default()
        };
        let worker = worker_name(session.id);
        let binding_id = Uuid::new_v4();
        let secret_volume = control_volume_name(binding_id);
        let topology = compile_live_topology(
            &config,
            &session,
            binding_id,
            &profile,
            &worker,
            &secret_volume,
        )
        .expect("compile isolated Live topology");

        let gateway = topology.gateway_spec.expect("protected gateway");
        let app = topology.protected_app_spec;
        let expected_namespace = format!("container:{}", gateway.name);
        let expected_session_id = session.id.to_string();
        let expected_binding_id = binding_id.to_string();
        assert_eq!(gateway.name, format!("{worker}-vpn"));
        assert_eq!(app.name, worker);
        assert_eq!(
            app.network_mode.as_deref(),
            Some(expected_namespace.as_str())
        );
        assert!(app.ports.is_empty());
        assert!(app.aliases.is_empty());
        assert!(app.volumes.iter().any(|volume| {
            volume.source_kind == VolumeMountSourceKind::NamedVolume
                && volume.host_path == secret_volume
                && volume.container_path == WORKER_SECRET_ROOT
                && volume.read_only
        }));
        assert_eq!(gateway.ports.len(), 1);
        assert_eq!(gateway.ports[0].container_port, 18_080);
        assert_eq!(gateway.ports[0].host_port, Some(0));
        assert_eq!(gateway.ports[0].host_ip.as_deref(), Some("127.0.0.1"));
        assert!(
            gateway
                .env
                .iter()
                .all(|value| value.name != "FIREWALL_OUTBOUND_SUBNETS")
        );
        assert!(gateway.volumes.iter().any(|volume| {
            volume.source_kind == VolumeMountSourceKind::NamedVolume
                && volume.host_path == material_volume_name(binding_id, GATEWAY_CONFIG_ROLE)
                && volume.container_path == WIREGUARD_CONFIG_ROOT
                && volume.read_only
        }));
        assert!(
            gateway
                .volumes
                .iter()
                .all(|volume| volume.source_kind != VolumeMountSourceKind::Bind)
        );
        assert_eq!(
            gateway
                .sysctls
                .get("net.ipv6.conf.all.disable_ipv6")
                .map(String::as_str),
            Some("0")
        );
        assert_eq!(app.image, "elixir-live-egress-worker:test");
        assert_eq!(app.volumes.len(), 1);
        assert_eq!(app.volumes[0].container_path, WORKER_SECRET_ROOT);
        assert!(app.volumes[0].read_only);
        assert_eq!(app.security.user.as_deref(), Some("65532:65532"));
        assert!(app.security.read_only_rootfs);
        assert!(app.security.no_new_privileges);
        assert_eq!(app.security.memory_limit_mb, Some(192));
        assert_eq!(app.security.pids_limit, Some(48));
        assert_eq!(app.cap_drop, vec!["ALL"]);
        assert!(app.cap_add.is_empty());
        assert!(app.devices.is_empty());
        assert_eq!(
            app.labels.get(ROLE_LABEL).map(String::as_str),
            Some("worker")
        );
        assert_eq!(
            app.labels.get(SESSION_LABEL).map(String::as_str),
            Some(expected_session_id.as_str())
        );
        assert_eq!(
            app.labels.get(BINDING_LABEL).map(String::as_str),
            Some(expected_binding_id.as_str())
        );
        assert_eq!(
            gateway.labels.get(BINDING_LABEL).map(String::as_str),
            Some(expected_binding_id.as_str())
        );
        for spec in [&gateway, &app] {
            assert!(
                spec.labels
                    .keys()
                    .all(|key| !key.starts_with("elixir.download"))
            );
            assert!(spec.volumes.iter().all(|mount| {
                !mount.host_path.contains("media")
                    && !mount.host_path.contains("download")
                    && !mount.container_path.contains("media")
                    && !mount.container_path.contains("download")
                    && !mount.container_path.contains("docker.sock")
            }));
        }
    }

    #[test]
    fn n11_warp_state_volume_has_persistent_profile_ownership() {
        let profile = LiveEgressProfileConfig {
            id: "warp-live-test".to_string(),
            kind: LiveEgressProfileKind::Warp,
            ..LiveEgressProfileConfig::default()
        };
        let labels = warp_state_volume_labels(&profile);
        assert_eq!(
            labels.get(ELIXIR_MANAGED_LABEL).map(String::as_str),
            Some("true")
        );
        assert_eq!(
            labels.get(ROLE_LABEL).map(String::as_str),
            Some("warp_state")
        );
        assert_eq!(
            labels.get(POLICY_LABEL).map(String::as_str),
            Some("warp-live-test")
        );
        assert_eq!(
            labels.get(POLICY_KIND_LABEL).map(String::as_str),
            Some("warp")
        );
        assert!(!labels.contains_key(SESSION_LABEL));
        assert!(!labels.contains_key(BINDING_LABEL));
        assert!(!labels.contains_key(FENCE_LABEL));
    }

    #[test]
    fn n11_openvpn_topology_uses_secret_files_without_host_binds_or_credential_values() {
        let session = session();
        let config = LiveEgressConfig {
            worker_image: "elixir-live-egress-worker:test".to_string(),
            network: "elixir_live_egress_test".to_string(),
            ..LiveEgressConfig::default()
        };
        let profile = LiveEgressProfileConfig {
            id: "openvpn-live-test".to_string(),
            name: "OpenVPN Live test".to_string(),
            kind: LiveEgressProfileKind::Openvpn,
            gateway_image: "gluetun:test".to_string(),
            config_host_path: Some("/private/provider.ovpn".to_string()),
            auth_host_path: Some("/private/provider.auth".to_string()),
            expected_egress_ips: vec!["1.1.1.1".parse().unwrap()],
            ..LiveEgressProfileConfig::default()
        };
        let worker = worker_name(session.id);
        let binding_id = Uuid::new_v4();
        let topology = compile_live_topology(
            &config,
            &session,
            binding_id,
            &profile,
            &worker,
            &control_volume_name(binding_id),
        )
        .expect("compile OpenVPN Live topology");
        let gateway = topology.gateway_spec.expect("OpenVPN gateway");

        assert!(
            gateway
                .volumes
                .iter()
                .all(|volume| volume.source_kind == VolumeMountSourceKind::NamedVolume)
        );
        for (role, root) in [
            (GATEWAY_CONFIG_ROLE, OPENVPN_CONFIG_ROOT),
            (OPENVPN_USERNAME_ROLE, OPENVPN_USERNAME_ROOT),
            (OPENVPN_PASSWORD_ROLE, OPENVPN_PASSWORD_ROOT),
        ] {
            assert!(gateway.volumes.iter().any(|volume| {
                volume.host_path == material_volume_name(binding_id, role)
                    && volume.container_path == root
                    && volume.read_only
            }));
        }
        for (name, path) in [
            ("OPENVPN_CUSTOM_CONFIG", OPENVPN_CONFIG_PATH),
            ("OPENVPN_USER_SECRETFILE", OPENVPN_USERNAME_PATH),
            ("OPENVPN_PASSWORD_SECRETFILE", OPENVPN_PASSWORD_PATH),
        ] {
            assert!(
                gateway
                    .env
                    .iter()
                    .any(|value| { value.name == name && value.value == path })
            );
        }
        assert!(gateway.volumes.iter().all(|volume| {
            volume.host_path != "/private/provider.ovpn"
                && volume.host_path != "/private/provider.auth"
                && volume.container_path != "/gluetun/auth.txt"
        }));
        assert!(
            gateway.env.iter().all(|value| {
                value.value != "service-user" && value.value != "service-password"
            })
        );

        let mut runtime_state = crate::runtime::model::ContainerRuntimeState {
            name: gateway.name,
            network_mode: None,
            labels: gateway.labels,
            mounts: gateway
                .volumes
                .iter()
                .map(|mount| crate::runtime::model::ContainerRuntimeMount {
                    mount_type: "volume".to_string(),
                    source: None,
                    name: Some(mount.host_path.clone()),
                    destination: mount.container_path.clone(),
                    read_only: mount.read_only,
                })
                .collect(),
            published_ports: Vec::new(),
            security: Default::default(),
        };
        assert!(gateway_material_mounts_match(
            &profile,
            &binding_id.to_string(),
            &runtime_state,
        ));
        runtime_state
            .mounts
            .retain(|mount| mount.destination != OPENVPN_PASSWORD_ROOT);
        assert!(!gateway_material_mounts_match(
            &profile,
            &binding_id.to_string(),
            &runtime_state,
        ));
    }
}
