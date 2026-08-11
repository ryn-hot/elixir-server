//! Application lifecycle for the single managed anime-matching model.
//!
//! Construction is deliberately I/O-free. The server can bind and serve the
//! deterministic resolver before this service creates storage, inventories the
//! host, downloads a bundle, probes a worker, or loads a model.

use std::{
    collections::BTreeSet,
    path::PathBuf,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::{Client, Url};
use semver::Version;
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::{
    sync::{Notify, RwLock},
    time::MissedTickBehavior,
};
use tokio_util::sync::CancellationToken;

use crate::{
    config::RunEnvironment,
    metrics::{ANIME_INFERENCE_EVENTS, ANIME_INFERENCE_OPERATION_DURATION},
    playback::{
        PlaybackJobManager,
        hardware::{HostHardwareInventory, SharedHostHardwareInventory},
    },
};

use super::{
    ANIME_MATCH_PROMPT_REVISION, ActiveAnimeBundleDescriptor, AnimeBundleCompatibilityPolicy,
    AnimeBundleStore, AnimeExecutionBackend, AnimeInferenceBundleManifest, AnimeKvCacheType,
    AnimeMatchEngine, AnimeMatchingService, AnimeRuntimeSelection, InferenceBackend,
    InferenceEnvelopeProbe, InferenceHardwareInventory, InferenceModelEnvelope,
    InferenceProbeError, InferenceProbeLimits, InferenceProbeMeasurement,
    InferenceRuntimeCandidate, LocalModelAdmission, LocalModelAdmissionPhase, LocalModelEngine,
    LocalModelRuntimeProfile, LocalModelSamplingProfile, LocalModelSnapshot, LocalModelWorkerState,
    QualifiedAnimeBundleApproval, ResolvedAnimeRuntime, RuntimeProfileIdentity,
    RuntimeProfilePolicy, SignedAnimeBundleEnvelope, StagedAnimeBundle, ValidatedAnimeBundle,
    VerifiedAnimeBundleEnvelope, assess_inference_memory_pressure, bundle_inference_host,
    bundle_runtime_profile_from_probe, cached_profile_driver_evidence_is_reusable,
    collect_current_inference_memory, collect_inference_hardware_inventory,
    commit_accepted_anime_update, ensure_monotonic_anime_update, inference_hardware_fingerprint,
    load_accepted_anime_update, resolve_anime_runtime, runtime_device_memory,
    runtime_profile_candidates, select_runtime_profile, smoke_requests,
    verify_anime_bundle_envelope,
};

const FIRST_PARTY_MANIFEST_URL: &str =
    "https://releases.elixir-media.com/anime/stable/anime-inference-channel.json";
const MANIFEST_URL_OVERRIDE: &str = "ELIXIR_ANIME_INFERENCE_BUNDLE_MANIFEST_URL";
const MAX_MANIFEST_BYTES: usize = 2 * 1024 * 1024;
const MANIFEST_FETCH_DEADLINE: Duration = Duration::from_secs(30);
const UPDATE_SUCCESS_INTERVAL: Duration = Duration::from_secs(6 * 60 * 60);
const UPDATE_RETRY_INTERVAL: Duration = Duration::from_secs(15 * 60);
const RESOURCE_POLL_INTERVAL: Duration = Duration::from_secs(2);
const UNKNOWN_AVAILABLE_MEMORY: u64 = u64::MAX;

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AnimeInferenceLifecycleState {
    Inactive,
    Bootstrapping,
    Downloading,
    Probing,
    Active,
    DeterministicOnly,
    ShuttingDown,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AnimeInferenceSnapshot {
    pub state: AnimeInferenceLifecycleState,
    pub deterministic_fallback_available: bool,
    pub bundle_version: Option<String>,
    pub model_id: Option<String>,
    pub backend: Option<String>,
    pub profile_fingerprint: Option<String>,
    pub update_channel_sequence: Option<u64>,
    pub update_key_id: Option<String>,
    pub update_envelope_fingerprint: Option<String>,
    pub update_bundle_closure_fingerprint: Option<String>,
    pub last_successful_update_at: Option<DateTime<Utc>>,
    pub worker_state: LocalModelWorkerState,
    pub worker_resident_rss_bytes: Option<u64>,
    pub available_system_memory_bytes: Option<u64>,
    pub playback_priority_active: bool,
    pub resource_suspended: bool,
    pub last_error: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct InferenceServiceState {
    lifecycle: AnimeInferenceLifecycleState,
    bundle_version: Option<String>,
    model_id: Option<String>,
    backend: Option<String>,
    profile_fingerprint: Option<String>,
    update_channel_sequence: Option<u64>,
    update_key_id: Option<String>,
    update_envelope_fingerprint: Option<String>,
    update_bundle_closure_fingerprint: Option<String>,
    last_successful_update_at: Option<DateTime<Utc>>,
    last_error: Option<String>,
    updated_at: DateTime<Utc>,
}

impl Default for InferenceServiceState {
    fn default() -> Self {
        Self {
            lifecycle: AnimeInferenceLifecycleState::Inactive,
            bundle_version: None,
            model_id: None,
            backend: None,
            profile_fingerprint: None,
            update_channel_sequence: None,
            update_key_id: None,
            update_envelope_fingerprint: None,
            update_bundle_closure_fingerprint: None,
            last_successful_update_at: None,
            last_error: None,
            updated_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RepairScheduleState {
    Idle,
    Requested,
    Running { followup_requested: bool },
    Backoff,
}

struct AnimeResourceAdmission {
    playback: Arc<PlaybackJobManager>,
    available_memory_bytes: AtomicU64,
    resource_suspended: AtomicBool,
    maintenance_suspended: AtomicBool,
}

impl AnimeResourceAdmission {
    fn new(playback: Arc<PlaybackJobManager>) -> Self {
        Self {
            playback,
            available_memory_bytes: AtomicU64::new(UNKNOWN_AVAILABLE_MEMORY),
            resource_suspended: AtomicBool::new(false),
            maintenance_suspended: AtomicBool::new(false),
        }
    }

    fn update_available_memory(&self, available: Option<u64>) {
        self.available_memory_bytes.store(
            available.unwrap_or(UNKNOWN_AVAILABLE_MEMORY),
            Ordering::Release,
        );
    }

    fn available_memory(&self) -> Option<u64> {
        match self.available_memory_bytes.load(Ordering::Acquire) {
            UNKNOWN_AVAILABLE_MEMORY => None,
            value => Some(value),
        }
    }

    fn can_start(&self, peak_rss_bytes: u64) -> bool {
        !self.maintenance_suspended.load(Ordering::Acquire)
            && !self.playback.has_latency_sensitive_work()
            && self.available_memory().is_some_and(|available| {
                let required =
                    super::MIN_AVAILABLE_SYSTEM_MEMORY_BYTES.saturating_add(peak_rss_bytes);
                available >= required
            })
    }

    fn resident_under_pressure(&self) -> bool {
        self.maintenance_suspended.load(Ordering::Acquire)
            || self.playback.has_latency_sensitive_work()
            || self
                .available_memory()
                .is_none_or(|available| available < super::MIN_AVAILABLE_SYSTEM_MEMORY_BYTES)
    }

    fn enter_maintenance(&self) -> AnimeInferenceMaintenanceGuard<'_> {
        self.maintenance_suspended.store(true, Ordering::Release);
        AnimeInferenceMaintenanceGuard {
            flag: &self.maintenance_suspended,
            active: true,
        }
    }
}

impl LocalModelAdmission for AnimeResourceAdmission {
    fn admit(
        &self,
        phase: LocalModelAdmissionPhase,
        profile: &LocalModelRuntimeProfile,
    ) -> Result<()> {
        let manager_owned = matches!(
            phase,
            LocalModelAdmissionPhase::ActivationWorkerStart
                | LocalModelAdmissionPhase::ActivationInference
                | LocalModelAdmissionPhase::ProbeWorkerStart
                | LocalModelAdmissionPhase::ProbeInference
        );
        ensure!(
            manager_owned || !self.maintenance_suspended.load(Ordering::Acquire),
            "local inference is temporarily suspended for runtime maintenance"
        );
        ensure!(
            !self.playback.has_latency_sensitive_work(),
            "playback has latency-sensitive work"
        );
        let peak = matches!(
            phase,
            LocalModelAdmissionPhase::WorkerStart
                | LocalModelAdmissionPhase::ActivationWorkerStart
                | LocalModelAdmissionPhase::ProbeWorkerStart
        )
        .then_some(profile.peak_rss_bytes);
        let memory = super::InferenceSystemMemory {
            total_bytes: None,
            available_bytes: self.available_memory(),
            source: "admission_cache".to_string(),
            container_limit_bytes: None,
        };
        let pressure = assess_inference_memory_pressure(&memory, peak);
        ensure!(
            !pressure.under_pressure,
            "local inference resource envelope is under memory pressure"
        );
        Ok(())
    }
}

struct AnimeInferenceMaintenanceGuard<'a> {
    flag: &'a AtomicBool,
    active: bool,
}

impl AnimeInferenceMaintenanceGuard<'_> {
    fn release(&mut self) {
        if self.active {
            self.flag.store(false, Ordering::Release);
            self.active = false;
        }
    }
}

impl Drop for AnimeInferenceMaintenanceGuard<'_> {
    fn drop(&mut self) {
        self.release();
    }
}

#[async_trait]
trait AnimeManifestSource: Send + Sync {
    async fn fetch(&self) -> Result<FetchedAnimeManifest>;
}

struct HttpAnimeManifestSource {
    client: Client,
    url: Url,
    require_signed_channel: bool,
}

struct FetchedAnimeManifest {
    manifest: AnimeInferenceBundleManifest,
    channel: Option<VerifiedAnimeBundleEnvelope>,
}

struct FetchedValidatedAnimeBundle {
    bundle: ValidatedAnimeBundle,
    channel: Option<VerifiedAnimeBundleEnvelope>,
}

#[async_trait]
impl AnimeManifestSource for HttpAnimeManifestSource {
    async fn fetch(&self) -> Result<FetchedAnimeManifest> {
        tokio::time::timeout(MANIFEST_FETCH_DEADLINE, self.fetch_inner())
            .await
            .context("anime inference manifest request timed out")?
    }
}

impl HttpAnimeManifestSource {
    async fn fetch_inner(&self) -> Result<FetchedAnimeManifest> {
        let response = self.client.get(self.url.clone()).send().await?;
        let status = response.status();
        ensure!(status.is_success(), "manifest endpoint returned {status}");
        if let Some(length) = response.content_length() {
            ensure!(
                length <= MAX_MANIFEST_BYTES as u64,
                "anime inference manifest is too large"
            );
        }
        let mut response = response;
        let mut bytes = Vec::with_capacity(
            response
                .content_length()
                .and_then(|length| usize::try_from(length).ok())
                .unwrap_or(0)
                .min(MAX_MANIFEST_BYTES),
        );
        while let Some(chunk) = response.chunk().await.context("reading bundle manifest")? {
            ensure!(
                bytes.len().saturating_add(chunk.len()) <= MAX_MANIFEST_BYTES,
                "anime inference manifest is too large"
            );
            bytes.extend_from_slice(&chunk);
        }
        if self.require_signed_channel {
            let envelope: SignedAnimeBundleEnvelope = serde_json::from_slice(&bytes)
                .context("decoding strict signed anime update envelope")?;
            let channel = verify_anime_bundle_envelope(envelope, Utc::now(), true)?;
            return Ok(FetchedAnimeManifest {
                manifest: channel.manifest().clone(),
                channel: Some(channel),
            });
        }
        let manifest = serde_json::from_slice(&bytes)
            .context("decoding strict development anime inference manifest")?;
        Ok(FetchedAnimeManifest {
            manifest,
            channel: None,
        })
    }
}

/// Owns the single local anime-matching engine, its background artifacts, and
/// its automatic hardware execution profile. No normal user setting is
/// exposed by this type.
pub struct AnimeInferenceService {
    engine: Option<Arc<LocalModelEngine>>,
    store: AnimeBundleStore,
    manifest_source: Option<Arc<dyn AnimeManifestSource>>,
    policy: AnimeBundleCompatibilityPolicy,
    admission: Arc<AnimeResourceAdmission>,
    host_inventory: Arc<SharedHostHardwareInventory>,
    state: RwLock<InferenceServiceState>,
    active_profile: RwLock<Option<LocalModelRuntimeProfile>>,
    activation_generation: AtomicU64,
    activation_notify: Notify,
    construction_error: Option<String>,
    started: AtomicBool,
    stopped: AtomicBool,
    stopped_notify: Notify,
    repair_schedule: StdMutex<RepairScheduleState>,
    repair_notify: Notify,
    shutdown: CancellationToken,
}

impl AnimeInferenceService {
    /// I/O-free constructor. The inference directory is created only from
    /// `run_background`, after the API listener is available.
    pub fn new(
        inference_root: PathBuf,
        environment: RunEnvironment,
        playback: Arc<PlaybackJobManager>,
        host_inventory: Arc<SharedHostHardwareInventory>,
    ) -> Self {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .read_timeout(Duration::from_secs(60))
            .build();
        let admission = Arc::new(AnimeResourceAdmission::new(playback));
        let (client, client_error) = match client {
            Ok(client) => (client, None),
            Err(error) => (
                Client::new(),
                Some(format!("building inference HTTP client: {error}")),
            ),
        };
        let manifest_client_available = client_error.is_none();
        let engine = LocalModelEngine::new(admission.clone()).map(Arc::new);
        let engine_error = engine.as_ref().err().map(ToString::to_string);
        let source = if manifest_client_available {
            manifest_url(&environment).map(|url| {
                Arc::new(HttpAnimeManifestSource {
                    client: client.clone(),
                    url,
                    require_signed_channel: matches!(environment, RunEnvironment::Production),
                }) as Arc<dyn AnimeManifestSource>
            })
        } else {
            Err(anyhow!("anime inference HTTP client is unavailable"))
        };
        let source_error = source.as_ref().err().map(ToString::to_string);
        let policy = bundle_policy(&environment).unwrap_or_else(|error| {
            tracing::error!(error = %error, "failed to build anime inference qualification policy");
            AnimeBundleCompatibilityPolicy::production(Version::new(0, 0, 0), Vec::new())
        });
        let construction_error = client_error.or(engine_error).or(source_error);
        Self {
            engine: engine.ok(),
            store: AnimeBundleStore::new(inference_root, client),
            manifest_source: source.ok(),
            policy,
            admission,
            host_inventory,
            state: RwLock::new(InferenceServiceState::default()),
            active_profile: RwLock::new(None),
            activation_generation: AtomicU64::new(0),
            activation_notify: Notify::new(),
            construction_error,
            started: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            stopped_notify: Notify::new(),
            repair_schedule: StdMutex::new(RepairScheduleState::Idle),
            repair_notify: Notify::new(),
            shutdown: CancellationToken::new(),
        }
    }

    pub fn matching_service(&self) -> AnimeMatchingService {
        self.engine
            .as_ref()
            .map_or_else(AnimeMatchingService::disabled, |engine| {
                AnimeMatchingService::with_engine(engine.clone() as Arc<dyn AnimeMatchEngine>)
            })
    }

    /// Returns the number of local profiles successfully published as Active.
    ///
    /// Library schedulers can retain this value and pass it to
    /// [`Self::wait_for_activation_after`] to retry pending deterministic-only
    /// matches after an initial activation or a later model update.
    pub fn activation_generation(&self) -> u64 {
        self.activation_generation.load(Ordering::Acquire)
    }

    /// Waits until a local profile is published after `observed_generation`.
    ///
    /// Multiple publications coalesce into the latest generation. Registration
    /// happens before the generation is rechecked, so a publication concurrent
    /// with this call cannot be missed. Returns `None` when the service shuts
    /// down before another profile is published.
    pub async fn wait_for_activation_after(&self, observed_generation: u64) -> Option<u64> {
        loop {
            let notified = self.activation_notify.notified();
            tokio::pin!(notified);
            // `notify_waiters` stores no permit, so join its waiter set before
            // checking the generation that makes a missed wake harmless.
            notified.as_mut().enable();

            let current_generation = self.activation_generation();
            if current_generation > observed_generation {
                return Some(current_generation);
            }

            tokio::select! {
                _ = self.shutdown.cancelled() => {
                    let current_generation = self.activation_generation();
                    return (current_generation > observed_generation)
                        .then_some(current_generation);
                }
                _ = &mut notified => {},
            }
        }
    }

    /// Schedules one automatic runtime/profile repair. Signals coalesce so a
    /// failed backend cannot create a retry storm or a user-facing decision.
    pub fn request_runtime_repair(&self) {
        if self.shutdown.is_cancelled() {
            return;
        }
        let mut schedule = self
            .repair_schedule
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let notify = match *schedule {
            RepairScheduleState::Idle => {
                *schedule = RepairScheduleState::Requested;
                true
            }
            RepairScheduleState::Running {
                followup_requested: false,
            } => {
                *schedule = RepairScheduleState::Running {
                    followup_requested: true,
                };
                false
            }
            RepairScheduleState::Requested
            | RepairScheduleState::Running {
                followup_requested: true,
            }
            | RepairScheduleState::Backoff => false,
        };
        drop(schedule);
        if notify {
            self.repair_notify.notify_one();
        }
    }

    fn claim_scheduled_repair(&self) -> bool {
        let mut schedule = self
            .repair_schedule
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if matches!(
            *schedule,
            RepairScheduleState::Requested | RepairScheduleState::Backoff
        ) {
            *schedule = RepairScheduleState::Running {
                followup_requested: false,
            };
            true
        } else {
            false
        }
    }

    fn finish_scheduled_repair(&self, succeeded: bool) {
        let mut schedule = self
            .repair_schedule
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let followup_requested = match *schedule {
            RepairScheduleState::Running { followup_requested } => followup_requested,
            _ => return,
        };
        let notify = succeeded && followup_requested;
        *schedule = if !succeeded {
            RepairScheduleState::Backoff
        } else if followup_requested {
            RepairScheduleState::Requested
        } else {
            RepairScheduleState::Idle
        };
        drop(schedule);
        if notify {
            self.repair_notify.notify_one();
        }
    }

    fn repair_is_running(&self) -> bool {
        matches!(
            *self
                .repair_schedule
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            RepairScheduleState::Running { .. }
        )
    }

    pub async fn snapshot(&self) -> AnimeInferenceSnapshot {
        let state = self.state.read().await.clone();
        let worker = match self.engine.as_ref() {
            Some(engine) => engine.snapshot().await,
            None => LocalModelSnapshot {
                state: LocalModelWorkerState::Unavailable,
                profile_fingerprint: None,
                backend: None,
                process_id: None,
                loopback_port: None,
                resident_rss_bytes: None,
                last_error: self.construction_error.clone(),
            },
        };
        AnimeInferenceSnapshot {
            state: state.lifecycle,
            deterministic_fallback_available: true,
            bundle_version: state.bundle_version,
            model_id: state.model_id,
            backend: worker.backend.or(state.backend),
            profile_fingerprint: worker.profile_fingerprint.or(state.profile_fingerprint),
            update_channel_sequence: state.update_channel_sequence,
            update_key_id: state.update_key_id,
            update_envelope_fingerprint: state.update_envelope_fingerprint,
            update_bundle_closure_fingerprint: state.update_bundle_closure_fingerprint,
            last_successful_update_at: state.last_successful_update_at,
            worker_state: worker.state,
            worker_resident_rss_bytes: worker.resident_rss_bytes,
            available_system_memory_bytes: self.admission.available_memory(),
            playback_priority_active: self.admission.playback.has_latency_sensitive_work(),
            resource_suspended: self.admission.resource_suspended.load(Ordering::Acquire),
            last_error: worker.last_error.or(state.last_error),
            updated_at: state.updated_at,
        }
    }

    pub async fn run_background(self: Arc<Self>, external_shutdown: CancellationToken) {
        if self
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let _completion = InferenceRunCompletion(self.as_ref());
        if self.shutdown.is_cancelled() || external_shutdown.is_cancelled() {
            return;
        }
        if let Some(error) = self.construction_error.as_deref() {
            self.record_failure(error).await;
        }
        let Some(engine) = self.engine.clone() else {
            tokio::select! {
                _ = external_shutdown.cancelled() => {},
                _ = self.shutdown.cancelled() => {},
            }
            return;
        };

        // Link the application token into the service token without dropping
        // an in-progress staging future. Artifact hashing/extraction observes
        // the service token and is then awaited to a cooperative stop.
        let shutdown_bridge = tokio::spawn({
            let external_shutdown = external_shutdown.clone();
            let service_shutdown = self.shutdown.clone();
            async move {
                tokio::select! {
                    _ = external_shutdown.cancelled() => service_shutdown.cancel(),
                    _ = service_shutdown.cancelled() => {},
                }
            }
        });
        let engine_shutdown = self.shutdown.clone();
        let engine_task = tokio::spawn({
            let engine = engine.clone();
            async move { engine.run_background(engine_shutdown).await }
        });
        let resource_task = tokio::spawn({
            let service = self.clone();
            let external_shutdown = external_shutdown.clone();
            async move { service.run_resource_monitor(external_shutdown).await }
        });

        let bootstrap = self.bootstrap_once().await;
        let mut next_delay = if self.shutdown.is_cancelled() {
            Duration::ZERO
        } else {
            match bootstrap {
                Ok(()) => UPDATE_SUCCESS_INTERVAL,
                Err(error) => {
                    self.record_failure(&error.to_string()).await;
                    UPDATE_RETRY_INTERVAL
                }
            }
        };

        while !next_delay.is_zero() {
            tokio::select! {
                _ = self.shutdown.cancelled() => break,
                _ = tokio::time::sleep(next_delay) => {
                    // Backoff retries remain offline: they consume the exact
                    // active manifest instead of depending on an update fetch.
                    let run_cached_repair = self.claim_scheduled_repair();
                    let maintenance = if run_cached_repair {
                        self.repair_active_bundle().await
                    } else {
                        self.check_for_update().await
                    };
                    next_delay = match maintenance {
                        Ok(()) => {
                            if run_cached_repair {
                                self.finish_scheduled_repair(true);
                            }
                            UPDATE_SUCCESS_INTERVAL
                        },
                        Err(error) => {
                            if run_cached_repair {
                                self.finish_scheduled_repair(false);
                            }
                            if self.shutdown.is_cancelled() {
                                Duration::ZERO
                            } else {
                                self.record_failure(&error.to_string()).await;
                                UPDATE_RETRY_INTERVAL
                            }
                        },
                    };
                }
                _ = self.repair_notify.notified() => {
                    if !self.claim_scheduled_repair() {
                        continue;
                    }
                    let repair = self.repair_active_bundle().await;
                    next_delay = match repair {
                        Ok(()) => {
                            self.finish_scheduled_repair(true);
                            UPDATE_SUCCESS_INTERVAL
                        }
                        Err(error) => {
                            self.finish_scheduled_repair(false);
                            if self.shutdown.is_cancelled() {
                                Duration::ZERO
                            } else {
                                self.record_failure(&error.to_string()).await;
                                UPDATE_RETRY_INTERVAL
                            }
                        },
                    };
                }
            }
        }

        self.shutdown.cancel();
        self.transition(
            AnimeInferenceLifecycleState::ShuttingDown,
            None,
            None,
            None,
            None,
        )
        .await;
        engine.shutdown().await;
        await_task(shutdown_bridge, "anime inference shutdown bridge").await;
        await_task(engine_task, "anime inference engine").await;
        await_task(resource_task, "anime inference resource monitor").await;
        self.rollback_pending_activation().await;
        if let Err(error) = self.store.cleanup_staging().await {
            tracing::warn!(error = %error, "failed to clean interrupted inference staging on shutdown");
        }
        if let Err(error) = self.store.cleanup_unreferenced_installs().await {
            tracing::warn!(error = %error, "failed to clean interrupted inference installs on shutdown");
        }
    }

    pub async fn shutdown(&self) {
        self.shutdown.cancel();
        if let Some(engine) = self.engine.as_ref() {
            engine.shutdown().await;
        }
        if self.started.load(Ordering::Acquire) && !self.stopped.load(Ordering::Acquire) {
            let stopped = self.stopped_notify.notified();
            if !self.stopped.load(Ordering::Acquire) {
                stopped.await;
            }
        }
    }

    async fn bootstrap_once(&self) -> Result<()> {
        let mut operation = InferenceOperation::new("bootstrap");
        self.transition(
            AnimeInferenceLifecycleState::Bootstrapping,
            None,
            None,
            None,
            None,
        )
        .await;
        self.store.ensure_layout().await?;
        // A durable marker means the prior process committed a new Active
        // pointer but did not finish live verification. Restore the exact
        // previous pointer before pruning or attempting any cached startup.
        self.store.recover_pending_activation().await?;
        self.store.cleanup_staging().await?;
        self.store.cleanup_unreferenced_installs().await?;
        self.refresh_memory().await;

        let mut active_loaded = false;
        let accepted_channel = load_accepted_anime_update(self.store.paths().root(), Utc::now())?;
        if let Some(channel) = accepted_channel.as_ref() {
            self.record_update_channel(channel, None).await;
        }
        let cached_policy = self.policy_with_channel(accepted_channel.as_ref());
        if let Some(active) = self.store.load_active()? {
            match self
                .store
                .load_manifest_for_descriptor(&active, &cached_policy)
            {
                Ok(bundle) => match self.activate_cached_bundle(&bundle).await {
                    Ok(true) => active_loaded = true,
                    Ok(false) => {}
                    Err(error) => {
                        tracing::warn!(error = %error, "cached anime inference bundle is not reusable");
                    }
                },
                Err(error) => {
                    tracing::warn!(error = %error, "active anime inference manifest is unavailable or invalid");
                }
            }
        }

        match self.fetch_validated_manifest().await {
            Ok(fetched) => {
                self.reconcile_bundle(&fetched.bundle).await?;
                self.store.cache_validated_manifest(&fetched.bundle)?;
                if let Some(channel) = fetched.channel.as_ref() {
                    let accepted_at = Utc::now();
                    commit_accepted_anime_update(self.store.paths().root(), channel, accepted_at)?;
                    self.record_update_channel(channel, Some(accepted_at)).await;
                }
            }
            Err(error) if active_loaded => {
                tracing::warn!(error = %error, "anime inference update check failed; cached bundle remains active");
            }
            Err(error) => return Err(error),
        }
        operation.succeed();
        Ok(())
    }

    async fn check_for_update(&self) -> Result<()> {
        let mut operation = InferenceOperation::new("update_check");
        let fetched = self.fetch_validated_manifest().await?;
        self.reconcile_bundle(&fetched.bundle).await?;
        self.store.cache_validated_manifest(&fetched.bundle)?;
        if let Some(channel) = fetched.channel.as_ref() {
            let accepted_at = Utc::now();
            commit_accepted_anime_update(self.store.paths().root(), channel, accepted_at)?;
            self.record_update_channel(channel, Some(accepted_at)).await;
        }
        operation.succeed();
        Ok(())
    }

    async fn repair_active_bundle(&self) -> Result<()> {
        let active = self
            .store
            .load_active()?
            .ok_or_else(|| anyhow!("no active anime bundle is available for repair"))?;
        let accepted_channel = load_accepted_anime_update(self.store.paths().root(), Utc::now())?;
        let policy = self.policy_with_channel(accepted_channel.as_ref());
        let bundle = self
            .store
            .load_manifest_for_descriptor(&active, &policy)
            .context("loading the exact active anime bundle manifest for repair")?;
        self.reconcile_bundle(&bundle).await
    }

    async fn fetch_validated_manifest(&self) -> Result<FetchedValidatedAnimeBundle> {
        let source = self
            .manifest_source
            .as_ref()
            .ok_or_else(|| anyhow!("anime inference manifest source is unavailable"))?;
        let fetched = tokio::select! {
            biased;
            _ = self.shutdown.cancelled() => bail!("anime inference service is shutting down"),
            manifest = source.fetch() => manifest?,
        };
        if let Some(incoming) = fetched.channel.as_ref() {
            let accepted = load_accepted_anime_update(self.store.paths().root(), Utc::now())?;
            ensure_monotonic_anime_update(accepted.as_ref(), incoming)?;
        }
        let policy = self.policy_with_channel(fetched.channel.as_ref());
        let bundle = super::validate_anime_bundle(fetched.manifest, &policy)?;
        Ok(FetchedValidatedAnimeBundle {
            bundle,
            channel: fetched.channel,
        })
    }

    fn policy_with_channel(
        &self,
        channel: Option<&VerifiedAnimeBundleEnvelope>,
    ) -> AnimeBundleCompatibilityPolicy {
        match &self.policy.qualification_gate {
            super::AnimeBundleQualificationGate::DevelopmentAllowUnqualified => self.policy.clone(),
            super::AnimeBundleQualificationGate::Production { approvals } => {
                let mut approvals = approvals.clone();
                if let Some(channel) = channel {
                    approvals.push(channel.approval.clone());
                }
                AnimeBundleCompatibilityPolicy::production(
                    self.policy.server_version.clone(),
                    approvals,
                )
            }
        }
    }

    async fn record_update_channel(
        &self,
        channel: &VerifiedAnimeBundleEnvelope,
        accepted_at: Option<DateTime<Utc>>,
    ) {
        let mut state = self.state.write().await;
        state.update_channel_sequence = Some(channel.sequence());
        state.update_key_id = Some(channel.key_id().to_string());
        state.update_envelope_fingerprint = Some(channel.envelope_fingerprint.clone());
        state.update_bundle_closure_fingerprint =
            Some(channel.bundle_closure_fingerprint().to_string());
        if accepted_at.is_some() {
            state.last_successful_update_at = accepted_at;
        }
        state.updated_at = Utc::now();
    }

    async fn activate_cached_bundle(&self, bundle: &ValidatedAnimeBundle) -> Result<bool> {
        let Some(active) = self.store.load_active()? else {
            return Ok(false);
        };
        let (host, inventory) = self.collect_host_and_inference_inventory().await;
        let selection = resolve_anime_runtime(bundle, &bundle_inference_host(&host, &inventory)?)?;
        let Some(selection) = bundle
            .certified_runtime_selection(&inference_hardware_fingerprint(&inventory), &selection)
        else {
            return Ok(false);
        };
        if !active_descriptor_compatible(bundle, &active, &host, &inventory, &selection) {
            return Ok(false);
        }
        let _maintenance = self.admission.enter_maintenance();
        let profile = local_profile_from_active(&self.store, bundle, &active)?;
        if let Err(error) = self.activate_local_profile(profile).await {
            if let Some(engine) = self.engine.as_ref() {
                engine.clear_profile().await;
            }
            *self.active_profile.write().await = None;
            return Err(error);
        }
        Ok(true)
    }

    async fn reconcile_bundle(&self, bundle: &ValidatedAnimeBundle) -> Result<()> {
        let (host, inventory) = self.collect_host_and_inference_inventory().await;
        let bundle_host = bundle_inference_host(&host, &inventory)?;
        let selection = resolve_anime_runtime(bundle, &bundle_host)?;
        let Some(selection) = bundle
            .certified_runtime_selection(&inference_hardware_fingerprint(&inventory), &selection)
        else {
            self.remain_deterministic_only().await;
            return Ok(());
        };
        if let Some(active) = self.store.load_active()?
            && active_descriptor_compatible(bundle, &active, &host, &inventory, &selection)
        {
            let _maintenance = self.admission.enter_maintenance();
            let had_active_profile = self.active_profile.read().await.is_some();
            let profile = local_profile_from_active(&self.store, bundle, &active)?;
            match self.activate_local_profile(profile).await {
                Ok(()) => return Ok(()),
                Err(error) => {
                    if !had_active_profile {
                        if let Some(engine) = self.engine.as_ref() {
                            engine.clear_profile().await;
                        }
                    }
                    tracing::warn!(error = %error, "active inference profile failed its live health check; rebuilding automatically");
                }
            }
        }

        // The initial transaction is rooted in the mandatory CPU worker so a
        // missing preferred accelerator can never strand a usable model/CPU
        // download. The preferred runtime is then added to that same
        // transaction before any probe. Later accelerators remain lazy.
        let initial_selection = selection.preferred_with_cpu_fallback();
        let cpu_selection = AnimeRuntimeSelection {
            candidates: vec![selection.cpu_fallback().clone()],
        };

        let probe_policy = runtime_probe_policy(bundle, &selection);
        let reclaimable = self
            .active_profile
            .read()
            .await
            .as_ref()
            .map(|profile| profile.peak_rss_bytes)
            .unwrap_or(0);
        let mut candidate_inventory = inventory.clone();
        candidate_inventory.memory.available_bytes = candidate_inventory
            .memory
            .available_bytes
            .map(|available| available.saturating_add(reclaimable));
        let candidates = runtime_profile_candidates(&candidate_inventory, &probe_policy);
        ensure!(
            !candidates.is_empty(),
            "host has no viable inference profile candidates"
        );
        let required = super::MIN_AVAILABLE_SYSTEM_MEMORY_BYTES
            .saturating_add(bundle.manifest().model.size_bytes);
        ensure!(
            inventory
                .memory
                .available_bytes
                .map(|available| available.saturating_add(reclaimable))
                .is_some_and(|available| available >= required),
            "host lacks the cold-start memory envelope for local inference"
        );
        ensure!(
            !self.admission.playback.has_latency_sensitive_work(),
            "deferring inference update while playback is active"
        );
        let engine = self
            .engine
            .as_ref()
            .ok_or_else(|| anyhow!("local model engine is unavailable"))?;
        self.transition(
            AnimeInferenceLifecycleState::Downloading,
            Some(bundle.manifest().bundle_version.clone()),
            Some(bundle.manifest().model.id.clone()),
            None,
            None,
        )
        .await;
        let mut stage_operation = InferenceOperation::new("bundle_stage");
        let mut staged = self
            .store
            .stage_bundle_with_cancellation(bundle, &cpu_selection, &self.shutdown)
            .await?;
        let mut initially_unavailable_runtimes = BTreeSet::new();
        if initial_selection.preferred() != initial_selection.cpu_fallback()
            && let Err(error) = self
                .store
                .stage_additional_runtimes_with_cancellation(
                    bundle,
                    &initial_selection,
                    &mut staged,
                    &self.shutdown,
                )
                .await
        {
            ensure!(
                !self.shutdown.is_cancelled(),
                "anime inference service is shutting down"
            );
            ensure!(
                !self.admission.playback.has_latency_sensitive_work(),
                "deferring inference update because playback became active"
            );
            let preferred = initial_selection.preferred();
            initially_unavailable_runtimes.insert(preferred.artifact.artifact_key());
            tracing::warn!(
                backend = preferred.execution_backend.as_str(),
                error = %error,
                "preferred anime inference runtime is unavailable; preserving the staged CPU fallback"
            );
        }
        stage_operation.succeed();
        let old_profile = self.active_profile.read().await.clone();
        let _maintenance = self.admission.enter_maintenance();
        let result = async {
            engine.suspend().await?;
            self.refresh_memory().await;
            ensure!(
                !self.admission.playback.has_latency_sensitive_work(),
                "deferring inference probe because playback became active"
            );
            ensure!(
                self.admission
                    .available_memory()
                    .is_some_and(|available| available >= required),
                "host lost the cold-start memory envelope after unloading the previous worker"
            );
            self.probe_and_activate(
                bundle,
                &host,
                &inventory,
                &candidate_inventory,
                &selection,
                staged,
                required,
                &initially_unavailable_runtimes,
            )
            .await
        }
        .await;
        if let Err(error) = result {
            match old_profile {
                Some(profile) => {
                    let disk_matches = self
                        .store
                        .load_active()
                        .ok()
                        .flatten()
                        .is_some_and(|active| descriptor_matches_local_profile(&active, &profile));
                    if disk_matches {
                        if let Err(restore_error) = self.activate_local_profile(profile).await {
                            tracing::error!(error = %restore_error, "failed to restore previous inference worker");
                            engine.clear_profile().await;
                            *self.active_profile.write().await = None;
                        }
                    } else {
                        tracing::error!(
                            "refusing to restore previous inference worker because the disk descriptor was not rolled back"
                        );
                        engine.clear_profile().await;
                        *self.active_profile.write().await = None;
                    }
                }
                None => {
                    engine.clear_profile().await;
                    *self.active_profile.write().await = None;
                }
            }
            if self.active_profile.read().await.is_none() {
                self.transition(
                    AnimeInferenceLifecycleState::DeterministicOnly,
                    None,
                    None,
                    None,
                    None,
                )
                .await;
            }
            self.record_failure(&error.to_string()).await;
            return Err(error);
        }
        Ok(())
    }

    async fn probe_and_activate(
        &self,
        bundle: &ValidatedAnimeBundle,
        host: &crate::playback::hardware::HostHardwareInventory,
        inventory: &InferenceHardwareInventory,
        candidate_inventory: &InferenceHardwareInventory,
        selection: &AnimeRuntimeSelection,
        mut staged: StagedAnimeBundle,
        cold_start_memory_required: u64,
        initially_unavailable_runtimes: &BTreeSet<String>,
    ) -> Result<()> {
        self.transition(
            AnimeInferenceLifecycleState::Probing,
            Some(bundle.manifest().bundle_version.clone()),
            Some(bundle.manifest().model.id.clone()),
            None,
            None,
        )
        .await;

        let mut last_probe_error = None;
        let mut persisted = None;
        for attempt_selection in selection.ordered_probe_attempts() {
            let runtime = attempt_selection.preferred();
            let runtime_is_staged = staged.runtimes().iter().any(|staged_runtime| {
                staged_runtime.manifest().artifact_key() == runtime.artifact.artifact_key()
            });
            if !runtime_is_staged {
                if initially_unavailable_runtimes.contains(&runtime.artifact.artifact_key()) {
                    last_probe_error = Some(anyhow!(
                        "{} runtime artifact was unavailable during initial staging",
                        runtime.execution_backend.as_str()
                    ));
                    continue;
                }
                ensure!(
                    runtime.execution_backend != AnimeExecutionBackend::Cpu,
                    "mandatory CPU fallback was absent from initial staging"
                );
                ensure!(
                    !self.shutdown.is_cancelled(),
                    "anime inference service is shutting down"
                );
                ensure!(
                    !self.admission.playback.has_latency_sensitive_work(),
                    "deferring inference fallback download while playback is active"
                );
                self.refresh_memory().await;
                ensure!(
                    self.admission
                        .available_memory()
                        .is_some_and(|available| available >= cold_start_memory_required),
                    "host lost the cold-start memory envelope before accelerator fallback"
                );
                self.transition(
                    AnimeInferenceLifecycleState::Downloading,
                    Some(bundle.manifest().bundle_version.clone()),
                    Some(bundle.manifest().model.id.clone()),
                    None,
                    None,
                )
                .await;
                let fallback_selection = AnimeRuntimeSelection {
                    candidates: vec![runtime.clone(), selection.cpu_fallback().clone()],
                };
                let mut stage_operation = InferenceOperation::new("bundle_stage");
                let stage_result = self
                    .store
                    .stage_additional_runtimes_with_cancellation(
                        bundle,
                        &fallback_selection,
                        &mut staged,
                        &self.shutdown,
                    )
                    .await;
                self.transition(
                    AnimeInferenceLifecycleState::Probing,
                    Some(bundle.manifest().bundle_version.clone()),
                    Some(bundle.manifest().model.id.clone()),
                    None,
                    None,
                )
                .await;
                match stage_result {
                    Ok(()) => stage_operation.succeed(),
                    Err(error) => {
                        ensure!(
                            !self.shutdown.is_cancelled(),
                            "anime inference service is shutting down"
                        );
                        ensure!(
                            !self.admission.playback.has_latency_sensitive_work(),
                            "deferring inference fallback because playback became active"
                        );
                        let error = error.context(format!(
                            "staging {} inference fallback runtime",
                            runtime.execution_backend.as_str()
                        ));
                        tracing::warn!(
                            backend = runtime.execution_backend.as_str(),
                            error = %error,
                            "anime inference fallback runtime is unavailable; advancing the automatic fallback chain"
                        );
                        last_probe_error = Some(error);
                        continue;
                    }
                }
            }

            let candidates = runtime_profile_candidates(
                candidate_inventory,
                &runtime_probe_policy(bundle, &attempt_selection),
            );
            if candidates.is_empty() {
                last_probe_error = Some(anyhow!(
                    "{} runtime has no viable hardware-envelope candidates",
                    runtime.execution_backend.as_str()
                ));
                continue;
            }
            match self
                .probe_staged_profile(
                    bundle,
                    host,
                    inventory,
                    &attempt_selection,
                    &candidates,
                    &staged,
                )
                .await
            {
                Ok(profile) => {
                    persisted = Some(profile);
                    break;
                }
                Err(error) => {
                    tracing::warn!(
                        backend = runtime.execution_backend.as_str(),
                        error = %error,
                        "anime inference runtime probe failed; trying the next automatic fallback"
                    );
                    last_probe_error = Some(error);
                    ensure!(
                        !self.shutdown.is_cancelled(),
                        "anime inference service is shutting down"
                    );
                    ensure!(
                        !self.admission.playback.has_latency_sensitive_work(),
                        "deferring inference fallback because playback became active"
                    );
                }
            }
        }
        let persisted = match persisted {
            Some(profile) => profile,
            None => {
                let error = last_probe_error.unwrap_or_else(|| {
                    anyhow!("no local inference profile passed the hardware envelope")
                });
                if let Err(cleanup_error) = self.store.discard_staged(staged).await {
                    return Err(error).context(format!(
                        "discarding failed inference staging directory also failed: {cleanup_error}"
                    ));
                }
                return Err(error);
            }
        };
        self.refresh_memory().await;
        ensure!(
            !self.admission.playback.has_latency_sensitive_work(),
            "deferring inference activation because playback became active"
        );
        let activation_memory_required =
            super::MIN_AVAILABLE_SYSTEM_MEMORY_BYTES.saturating_add(persisted.peak_rss_bytes);
        ensure!(
            self.admission
                .available_memory()
                .is_some_and(|available| available >= activation_memory_required),
            "host lost the measured inference memory envelope before activation"
        );
        let mut activation_operation = InferenceOperation::new("bundle_activation");
        let descriptor = self
            .store
            .activate_with_cancellation(staged, persisted, &self.shutdown)
            .await?;
        activation_operation.succeed();
        // Ordinary model calls remain maintenance-gated through the managed
        // worker's awaited live health warm and durable manifest commit.
        let activation = async {
            let pending = self.store.pending_activation_token(&descriptor)?;
            let profile = local_profile_from_active(&self.store, bundle, &descriptor)?;
            self.warm_local_profile(&profile).await?;
            // A new active pointer is not considered healthy until its exact
            // validated manifest is also durable for an offline restart.
            self.store.cache_validated_manifest(bundle)?;
            self.store.complete_pending_activation(&pending).await?;
            self.publish_local_profile(profile).await;
            Ok::<(), anyhow::Error>(())
        }
        .await;
        if let Err(error) = activation {
            let mut rollback_operation = InferenceOperation::new("bundle_rollback");
            return match self.store.rollback_failed_activation(&descriptor).await {
                Ok(_) => {
                    rollback_operation.succeed();
                    Err(error)
                        .context("activating newly installed inference profile; bundle rolled back")
                }
                Err(rollback_error) => Err(error).context(format!(
                    "activating newly installed inference profile failed and bundle rollback also failed: {rollback_error}"
                )),
            };
        }
        Ok(())
    }

    async fn probe_staged_profile(
        &self,
        bundle: &ValidatedAnimeBundle,
        host: &crate::playback::hardware::HostHardwareInventory,
        inventory: &InferenceHardwareInventory,
        selection: &AnimeRuntimeSelection,
        candidates: &[InferenceRuntimeCandidate],
        staged: &StagedAnimeBundle,
    ) -> Result<super::AnimeRuntimeProfile> {
        ensure!(
            !self.admission.playback.has_latency_sensitive_work(),
            "deferring inference profile probe while playback is active"
        );
        let probe = LocalEnvelopeProbe {
            bundle,
            staged,
            selection,
            host,
            inventory,
            admission: self.admission.clone(),
            cancellation: self.shutdown.clone(),
            active_engine: tokio::sync::Mutex::new(None),
        };
        let identity = RuntimeProfileIdentity {
            bundle_version: bundle.manifest().bundle_version.clone(),
            model_revision: bundle.manifest().model.revision.clone(),
            worker_revision: bundle.manifest().worker_revision.clone(),
            runtime_policy_revision: bundle
                .manifest()
                .runtime_policy
                .sampling_profile_revision
                .clone(),
            kv_cache_type: bundle.manifest().runtime_policy.kv_cache_type,
        };
        let selected = select_runtime_profile(
            inventory,
            &identity,
            candidates,
            &InferenceProbeLimits::default(),
            &probe,
        )
        .await;
        // `select_runtime_profile` owns an outer candidate deadline. If that
        // deadline cancels a probe future, explicitly stop and reap the
        // disposable worker before a fallback runtime can be downloaded.
        probe.shutdown_active_engine().await;
        let Some(probe_profile) = selected.profile else {
            bail!("no local inference profile passed the hardware envelope");
        };
        let runtime = resolved_runtime_for_profile(selection, &probe_profile)?;
        Ok(bundle_runtime_profile_from_probe(
            bundle,
            runtime,
            &probe_profile,
        )?)
    }

    async fn warm_local_profile(&self, profile: &LocalModelRuntimeProfile) -> Result<()> {
        let engine = self
            .engine
            .as_ref()
            .ok_or_else(|| anyhow!("local model engine is unavailable"))?;
        let activation = tokio::select! {
            biased;
            _ = self.shutdown.cancelled() => {
                let _ = engine.suspend().await;
                bail!("anime inference service is shutting down");
            }
            result = async {
                engine.activate_profile_cold(profile.clone()).await?;
                engine.prime_for_activation().await
            } => result,
        };
        if let Err(error) = activation {
            return Err(error).context("live inference worker health verification failed");
        }
        Ok(())
    }

    async fn publish_local_profile(&self, profile: LocalModelRuntimeProfile) {
        *self.active_profile.write().await = Some(profile.clone());
        self.transition(
            AnimeInferenceLifecycleState::Active,
            Some(profile.bundle_version),
            Some(profile.model_id),
            Some(profile.backend),
            Some(profile.profile_fingerprint),
        )
        .await;
        self.activation_generation.fetch_add(1, Ordering::Release);
        self.activation_notify.notify_waiters();
    }

    async fn activate_local_profile(&self, profile: LocalModelRuntimeProfile) -> Result<()> {
        self.warm_local_profile(&profile).await?;
        self.publish_local_profile(profile).await;
        Ok(())
    }

    /// A compatible artifact is insufficient in production: the exact host,
    /// runtime artifact, and execution backend must also be present in the
    /// qualification approval. Missing certification is an ordinary internal
    /// capability state, so it clears model state without surfacing an error or
    /// asking the user to choose a fallback.
    async fn remain_deterministic_only(&self) {
        if let Some(engine) = self.engine.as_ref() {
            engine.clear_profile().await;
        }
        *self.active_profile.write().await = None;
        self.admission
            .resource_suspended
            .store(false, Ordering::Release);
        self.transition(
            AnimeInferenceLifecycleState::DeterministicOnly,
            None,
            None,
            None,
            None,
        )
        .await;
    }

    async fn collect_host_and_inference_inventory(
        &self,
    ) -> (HostHardwareInventory, InferenceHardwareInventory) {
        let host = self.host_inventory.get_or_collect().await;
        let inventory = collect_inference_hardware_inventory(host.clone()).await;
        (host, inventory)
    }

    async fn rollback_pending_activation(&self) {
        let before = self.store.load_active().ok().flatten();
        match self.store.recover_pending_activation().await {
            Ok(restored) => {
                if before != restored {
                    inference_event("bundle_rollback", "shutdown");
                }
            }
            Err(error) => {
                tracing::error!(
                    error = %error,
                    "failed to roll back an inference bundle interrupted after commit"
                );
            }
        }
    }

    async fn run_resource_monitor(self: Arc<Self>, external_shutdown: CancellationToken) {
        let mut ticker = tokio::time::interval(RESOURCE_POLL_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = external_shutdown.cancelled() => return,
                _ = self.shutdown.cancelled() => return,
                _ = ticker.tick() => self.refresh_resources_and_worker().await,
            }
        }
    }

    async fn refresh_resources_and_worker(&self) {
        self.refresh_memory().await;
        if self.admission.maintenance_suspended.load(Ordering::Acquire) {
            return;
        }
        let Some(engine) = self.engine.as_ref() else {
            return;
        };
        let snapshot = engine.snapshot().await;
        if self.admission.resident_under_pressure() {
            if matches!(
                snapshot.state,
                LocalModelWorkerState::Ready | LocalModelWorkerState::Starting
            ) {
                if let Err(error) = engine.suspend().await {
                    tracing::warn!(error = %error, "failed to suspend inference worker for resource pressure");
                } else {
                    self.admission
                        .resource_suspended
                        .store(true, Ordering::Release);
                    inference_event("resource_suspend", "success");
                }
            }
            return;
        }
        if snapshot.state == LocalModelWorkerState::Unavailable
            && self.active_profile.read().await.is_some()
            && !self.repair_is_running()
        {
            self.request_runtime_repair();
        }
        if self
            .admission
            .resource_suspended
            .swap(false, Ordering::AcqRel)
        {
            let peak = self
                .active_profile
                .read()
                .await
                .as_ref()
                .map(|profile| profile.peak_rss_bytes)
                .unwrap_or(u64::MAX);
            if self.admission.can_start(peak) {
                engine.request_background_prime();
            } else {
                self.admission
                    .resource_suspended
                    .store(true, Ordering::Release);
            }
        }
    }

    async fn refresh_memory(&self) {
        let memory = collect_current_inference_memory().await;
        self.admission
            .update_available_memory(memory.available_bytes);
    }

    async fn record_failure(&self, error: &str) {
        let active = self.active_profile.read().await.clone();
        let mut state = self.state.write().await;
        match active {
            Some(profile) => {
                state.lifecycle = AnimeInferenceLifecycleState::Active;
                state.bundle_version = Some(profile.bundle_version);
                state.model_id = Some(profile.model_id);
                state.backend = Some(profile.backend);
                state.profile_fingerprint = Some(profile.profile_fingerprint);
            }
            None => {
                state.lifecycle = AnimeInferenceLifecycleState::DeterministicOnly;
                state.bundle_version = None;
                state.model_id = None;
                state.backend = None;
                state.profile_fingerprint = None;
            }
        }
        state.last_error = Some(bounded_text(error));
        state.updated_at = Utc::now();
        inference_event("service", "fallback");
    }

    async fn transition(
        &self,
        lifecycle: AnimeInferenceLifecycleState,
        bundle_version: Option<String>,
        model_id: Option<String>,
        backend: Option<String>,
        profile_fingerprint: Option<String>,
    ) {
        let mut state = self.state.write().await;
        state.lifecycle = lifecycle;
        if lifecycle == AnimeInferenceLifecycleState::DeterministicOnly {
            state.bundle_version = None;
            state.model_id = None;
            state.backend = None;
            state.profile_fingerprint = None;
        }
        if bundle_version.is_some() {
            state.bundle_version = bundle_version;
        }
        if model_id.is_some() {
            state.model_id = model_id;
        }
        if backend.is_some() {
            state.backend = backend;
        }
        if profile_fingerprint.is_some() {
            state.profile_fingerprint = profile_fingerprint;
        }
        state.last_error = None;
        state.updated_at = Utc::now();
    }
}

struct LocalEnvelopeProbe<'a> {
    bundle: &'a ValidatedAnimeBundle,
    staged: &'a StagedAnimeBundle,
    selection: &'a AnimeRuntimeSelection,
    host: &'a crate::playback::hardware::HostHardwareInventory,
    inventory: &'a InferenceHardwareInventory,
    admission: Arc<AnimeResourceAdmission>,
    cancellation: CancellationToken,
    active_engine: tokio::sync::Mutex<Option<LocalModelEngine>>,
}

#[async_trait]
impl InferenceEnvelopeProbe for LocalEnvelopeProbe<'_> {
    async fn probe(
        &self,
        candidate: &InferenceRuntimeCandidate,
    ) -> std::result::Result<InferenceProbeMeasurement, InferenceProbeError> {
        self.probe_inner(candidate)
            .await
            .map_err(|error| InferenceProbeError::Runner(bounded_error(&error)))
    }
}

impl LocalEnvelopeProbe<'_> {
    async fn shutdown_active_engine(&self) {
        let engine = self.active_engine.lock().await.take();
        if let Some(engine) = engine {
            engine.shutdown().await;
        }
    }

    async fn probe_inner(
        &self,
        candidate: &InferenceRuntimeCandidate,
    ) -> Result<InferenceProbeMeasurement> {
        // A generic envelope deadline can cancel the preceding probe future.
        // Never spawn the next disposable candidate until that worker has
        // been explicitly stopped and reaped.
        self.shutdown_active_engine().await;
        let runtime = resolved_runtime_for_candidate(self.selection, candidate)?;
        let staged_runtime = self
            .staged
            .runtimes()
            .iter()
            .find(|staged| staged.manifest().artifact_key() == runtime.artifact.artifact_key())
            .ok_or_else(|| anyhow!("staged runtime for probe candidate is missing"))?;
        let profile = local_profile_for_probe(
            self.bundle,
            self.staged,
            staged_runtime.entrypoint().to_path_buf(),
            candidate,
        )?;
        let [priming_request, request] = smoke_requests()?;
        let engine = LocalModelEngine::new_for_probe(self.admission.clone())?;
        engine.activate_profile_for_probe(profile).await?;
        *self.active_engine.lock().await = Some(engine.clone());
        let measured = match tokio::select! {
            biased;
            _ = self.cancellation.cancelled() => {
                Err(anyhow!("anime inference service is shutting down"))
            }
            result = engine.probe(priming_request, request) => result,
        } {
            Ok(measured) => measured,
            Err(error) => {
                engine.shutdown().await;
                self.active_engine.lock().await.take();
                return Err(error);
            }
        };
        let post_inventory = collect_inference_hardware_inventory(self.host.clone()).await;
        engine.shutdown().await;
        self.active_engine.lock().await.take();
        let memory_pressure = assess_inference_memory_pressure(&post_inventory.memory, None);
        let before_device = runtime_device_memory(
            self.inventory,
            candidate.backend,
            candidate.device_key.as_deref(),
        )
        .and_then(|device| {
            device
                .available_bytes
                .map(|bytes| (bytes, device.available_is_estimate))
        });
        let after_device = runtime_device_memory(
            &post_inventory,
            candidate.backend,
            candidate.device_key.as_deref(),
        )
        .and_then(|device| {
            device
                .available_bytes
                .map(|bytes| (bytes, device.available_is_estimate))
        });
        Ok(InferenceProbeMeasurement {
            worker_ready: measured.worker_ready,
            smoke_match_passed: measured.smoke_match_passed,
            load_time_ms: measured.load_time_ms,
            warm_latency_ms: measured.warm_latency_ms,
            peak_rss_bytes: measured.peak_rss_bytes.unwrap_or(0),
            peak_device_memory_bytes: before_device.zip(after_device).and_then(
                |((before, before_estimated), (after, after_estimated))| {
                    (!before_estimated && !after_estimated).then(|| before.saturating_sub(after))
                },
            ),
            system_available_bytes: post_inventory.memory.available_bytes,
            device_available_bytes: after_device.map(|(bytes, _)| bytes),
            memory_pressure: memory_pressure.under_pressure,
        })
    }
}

fn local_profile_for_probe(
    bundle: &ValidatedAnimeBundle,
    staged: &StagedAnimeBundle,
    worker_path: PathBuf,
    candidate: &InferenceRuntimeCandidate,
) -> Result<LocalModelRuntimeProfile> {
    let manifest = bundle.manifest();
    let sampling = fixed_sampling_profile(&manifest.runtime_policy.sampling_profile_revision)?;
    let fingerprint = format!(
        "sha256:{:x}",
        Sha256::digest(
            format!(
                "{}:{}:{}:{}:{}:{}",
                manifest.bundle_version,
                candidate.backend.as_str(),
                candidate.device_key.as_deref().unwrap_or("cpu"),
                candidate.gpu_layers,
                candidate.cpu_threads,
                candidate.batch_threads
            )
            .as_bytes()
        )
    );
    Ok(LocalModelRuntimeProfile {
        bundle_version: manifest.bundle_version.clone(),
        model_id: manifest.model.id.clone(),
        model_revision: manifest.model.revision.clone(),
        worker_revision: manifest.worker_revision.clone(),
        backend: candidate.backend.as_str().to_string(),
        profile_fingerprint: fingerprint,
        protocol_version: manifest.protocol_version,
        matcher_schema_version: manifest.matcher_schema_version,
        prompt_revision: ANIME_MATCH_PROMPT_REVISION.to_string(),
        worker_path,
        model_path: staged.model_path().to_path_buf(),
        context_tokens: manifest.model.context_tokens,
        max_output_tokens: manifest.model.max_output_tokens,
        threads: candidate.cpu_threads,
        batch_threads: candidate.batch_threads,
        gpu_layers: candidate.gpu_layers,
        kv_cache_type: kv_cache_name(manifest.runtime_policy.kv_cache_type).to_string(),
        peak_rss_bytes: manifest.model.size_bytes,
        idle_unload_seconds: manifest.runtime_policy.idle_unload_seconds,
        sampling,
    })
}

fn local_profile_from_active(
    store: &AnimeBundleStore,
    bundle: &ValidatedAnimeBundle,
    active: &ActiveAnimeBundleDescriptor,
) -> Result<LocalModelRuntimeProfile> {
    ensure!(
        active.manifest_fingerprint == bundle.manifest_fingerprint(),
        "active descriptor and bundle manifest differ"
    );
    let manifest = bundle.manifest();
    let runtime_manifest = manifest
        .runtimes
        .iter()
        .find(|runtime| runtime.artifact_key() == active.profile.runtime_artifact_key)
        .ok_or_else(|| anyhow!("active runtime is absent from cached manifest"))?;
    let installed = active
        .runtimes
        .iter()
        .find(|runtime| runtime.artifact_key == active.profile.runtime_artifact_key)
        .ok_or_else(|| anyhow!("active runtime installation is absent"))?;
    ensure!(
        installed.revision == runtime_manifest.revision
            && installed
                .sha256
                .eq_ignore_ascii_case(&runtime_manifest.sha256),
        "active runtime installation identity changed"
    );
    ensure!(
        active.model.id == manifest.model.id
            && active.model.revision == manifest.model.revision
            && active
                .model
                .sha256
                .eq_ignore_ascii_case(&manifest.model.sha256),
        "active model installation identity changed"
    );
    ensure!(
        active.profile.worker_revision == manifest.worker_revision,
        "active worker generation changed"
    );
    let runtime_root = store.resolve_relative(&installed.relative_root)?;
    let worker_path = runtime_root.join(&installed.relative_entrypoint);
    let model_path = store.resolve_relative(&active.model.relative_file)?;
    Ok(LocalModelRuntimeProfile {
        bundle_version: manifest.bundle_version.clone(),
        model_id: manifest.model.id.clone(),
        model_revision: manifest.model.revision.clone(),
        worker_revision: manifest.worker_revision.clone(),
        backend: active.profile.execution_backend.as_str().to_string(),
        profile_fingerprint: active.profile.profile_fingerprint.clone(),
        protocol_version: manifest.protocol_version,
        matcher_schema_version: manifest.matcher_schema_version,
        prompt_revision: ANIME_MATCH_PROMPT_REVISION.to_string(),
        worker_path,
        model_path,
        context_tokens: manifest.model.context_tokens,
        max_output_tokens: manifest.model.max_output_tokens,
        threads: u32::from(active.profile.cpu_thread_count),
        batch_threads: u32::from(active.profile.batch_thread_count),
        gpu_layers: active.profile.gpu_layer_count,
        kv_cache_type: kv_cache_name(active.profile.kv_cache_type).to_string(),
        peak_rss_bytes: active.profile.peak_rss_bytes,
        idle_unload_seconds: manifest.runtime_policy.idle_unload_seconds,
        sampling: fixed_sampling_profile(&manifest.runtime_policy.sampling_profile_revision)?,
    })
}

fn fixed_sampling_profile(revision: &str) -> Result<LocalModelSamplingProfile> {
    let sampling = LocalModelSamplingProfile::default();
    ensure!(
        sampling.revision == revision,
        "bundle sampling profile '{}' is unsupported by this server",
        revision
    );
    Ok(sampling)
}

fn active_descriptor_compatible(
    bundle: &ValidatedAnimeBundle,
    active: &ActiveAnimeBundleDescriptor,
    host: &HostHardwareInventory,
    inventory: &InferenceHardwareInventory,
    selection: &AnimeRuntimeSelection,
) -> bool {
    active.manifest_fingerprint == bundle.manifest_fingerprint()
        && active.bundle_version == bundle.manifest().bundle_version
        && active.profile.host_fingerprint == inference_hardware_fingerprint(inventory)
        && active.profile.model_id == bundle.manifest().model.id
        && active.profile.model_revision == bundle.manifest().model.revision
        && active.profile.worker_revision == bundle.manifest().worker_revision
        && cached_profile_driver_evidence_is_reusable(
            host,
            inventory,
            active.profile.execution_backend,
            active.profile.device_id.as_deref(),
        )
        && selection.candidates.iter().any(|runtime| {
            runtime.artifact.artifact_key() == active.profile.runtime_artifact_key
                && runtime.execution_backend == active.profile.execution_backend
                && runtime.device_id == active.profile.device_id
        })
}

fn descriptor_matches_local_profile(
    active: &ActiveAnimeBundleDescriptor,
    profile: &LocalModelRuntimeProfile,
) -> bool {
    active.bundle_version == profile.bundle_version
        && active.model.id == profile.model_id
        && active.model.revision == profile.model_revision
        && active.profile.worker_revision == profile.worker_revision
        && active.profile.profile_fingerprint == profile.profile_fingerprint
        && active.profile.execution_backend.as_str() == profile.backend
        && u32::from(active.profile.cpu_thread_count) == profile.threads
        && u32::from(active.profile.batch_thread_count) == profile.batch_threads
        && active.profile.gpu_layer_count == profile.gpu_layers
}

fn runtime_probe_policy(
    bundle: &ValidatedAnimeBundle,
    selection: &AnimeRuntimeSelection,
) -> RuntimeProfilePolicy {
    RuntimeProfilePolicy {
        certified_backends: selection
            .candidates
            .iter()
            .map(|runtime| inference_backend(runtime.execution_backend))
            .collect::<BTreeSet<_>>(),
        model: InferenceModelEnvelope {
            model_size_bytes: bundle.manifest().model.size_bytes,
            transformer_layers: bundle.manifest().model.transformer_layers,
        },
    }
}

fn resolved_runtime_for_candidate<'a>(
    selection: &'a AnimeRuntimeSelection,
    candidate: &InferenceRuntimeCandidate,
) -> Result<&'a ResolvedAnimeRuntime> {
    let backend = anime_execution_backend(candidate.backend);
    selection
        .candidates
        .iter()
        .find(|runtime| {
            runtime.execution_backend == backend && runtime.device_id == candidate.device_key
        })
        .ok_or_else(|| anyhow!("hardware probe candidate has no staged bundle runtime"))
}

fn resolved_runtime_for_profile<'a>(
    selection: &'a AnimeRuntimeSelection,
    profile: &super::InferenceRuntimeProfile,
) -> Result<&'a ResolvedAnimeRuntime> {
    selection
        .candidates
        .iter()
        .find(|runtime| {
            runtime.execution_backend == anime_execution_backend(profile.backend)
                && runtime.device_id == profile.device_key
        })
        .ok_or_else(|| anyhow!("selected profile has no exact bundle runtime"))
}

fn anime_execution_backend(backend: InferenceBackend) -> AnimeExecutionBackend {
    match backend {
        InferenceBackend::Metal => AnimeExecutionBackend::Metal,
        InferenceBackend::Cuda => AnimeExecutionBackend::Cuda,
        InferenceBackend::Hip => AnimeExecutionBackend::Hip,
        InferenceBackend::Vulkan => AnimeExecutionBackend::Vulkan,
        InferenceBackend::Cpu => AnimeExecutionBackend::Cpu,
    }
}

fn inference_backend(backend: AnimeExecutionBackend) -> InferenceBackend {
    match backend {
        AnimeExecutionBackend::Metal => InferenceBackend::Metal,
        AnimeExecutionBackend::Cuda => InferenceBackend::Cuda,
        AnimeExecutionBackend::Hip => InferenceBackend::Hip,
        AnimeExecutionBackend::Vulkan => InferenceBackend::Vulkan,
        AnimeExecutionBackend::Cpu => InferenceBackend::Cpu,
    }
}

fn kv_cache_name(cache: AnimeKvCacheType) -> &'static str {
    match cache {
        AnimeKvCacheType::F16 => "f16",
        AnimeKvCacheType::Q8_0 => "q8_0",
    }
}

fn bundle_policy(environment: &RunEnvironment) -> Result<AnimeBundleCompatibilityPolicy> {
    let server_version = Version::parse(env!("CARGO_PKG_VERSION"))
        .context("server package version is not semantic versioning")?;
    match environment {
        RunEnvironment::Development => {
            Ok(AnimeBundleCompatibilityPolicy::development(server_version))
        }
        RunEnvironment::Production => {
            let approvals: Vec<QualifiedAnimeBundleApproval> =
                serde_json::from_str(include_str!("qualified-anime-bundles.json"))
                    .context("decoding compiled qualified anime bundle approvals")?;
            Ok(AnimeBundleCompatibilityPolicy::production(
                server_version,
                approvals,
            ))
        }
    }
}

fn manifest_url(environment: &RunEnvironment) -> Result<Url> {
    let raw = std::env::var(MANIFEST_URL_OVERRIDE)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| FIRST_PARTY_MANIFEST_URL.to_string());
    let url = Url::parse(&raw).context("anime inference manifest URL is invalid")?;
    ensure!(
        url.scheme() == "https"
            || (matches!(environment, RunEnvironment::Development) && url.scheme() == "http"),
        "anime inference manifest URL must use HTTPS"
    );
    Ok(url)
}

fn bounded_error(error: &anyhow::Error) -> String {
    bounded_text(&error.to_string())
}

fn bounded_text(value: &str) -> String {
    value.chars().take(1_024).collect()
}

fn inference_event(event: &'static str, result: &'static str) {
    ANIME_INFERENCE_EVENTS
        .with_label_values(&[event, result])
        .inc();
}

struct InferenceOperation {
    name: &'static str,
    result: &'static str,
    started: std::time::Instant,
}

struct InferenceRunCompletion<'a>(&'a AnimeInferenceService);

impl Drop for InferenceRunCompletion<'_> {
    fn drop(&mut self) {
        self.0.stopped.store(true, Ordering::Release);
        self.0.stopped_notify.notify_waiters();
    }
}

impl InferenceOperation {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            result: "error",
            started: std::time::Instant::now(),
        }
    }

    fn succeed(&mut self) {
        self.result = "success";
    }
}

impl Drop for InferenceOperation {
    fn drop(&mut self) {
        inference_event(self.name, self.result);
        ANIME_INFERENCE_OPERATION_DURATION
            .with_label_values(&[self.name, self.result])
            .observe(self.started.elapsed().as_secs_f64());
    }
}

async fn await_task(task: tokio::task::JoinHandle<()>, name: &str) {
    if let Err(error) = task.await {
        tracing::warn!(task = name, error = %error, "inference task failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::anime_matching::ANIME_MATCH_SCHEMA_VERSION;
    fn test_service(root: PathBuf) -> AnimeInferenceService {
        sqlx::any::install_default_drivers();
        let pool = sqlx::AnyPool::connect_lazy("sqlite::memory:").expect("lazy test database");
        AnimeInferenceService::new(
            root,
            RunEnvironment::Development,
            Arc::new(PlaybackJobManager::new(pool, None)),
            Arc::new(SharedHostHardwareInventory::new()),
        )
    }

    fn test_runtime_profile() -> LocalModelRuntimeProfile {
        LocalModelRuntimeProfile {
            bundle_version: "2026.08.1".to_string(),
            model_id: "qwen-anime".to_string(),
            model_revision: "model-r1".to_string(),
            worker_revision: "worker-r1".to_string(),
            backend: "cpu".to_string(),
            profile_fingerprint: format!("sha256:{}", "1".repeat(64)),
            protocol_version: 1,
            matcher_schema_version: ANIME_MATCH_SCHEMA_VERSION,
            prompt_revision: ANIME_MATCH_PROMPT_REVISION.to_string(),
            worker_path: PathBuf::from("/tmp/llama-server"),
            model_path: PathBuf::from("/tmp/model.gguf"),
            context_tokens: 4_096,
            max_output_tokens: 256,
            threads: 1,
            batch_threads: 1,
            gpu_layers: 0,
            kv_cache_type: "f16".to_string(),
            peak_rss_bytes: 1,
            idle_unload_seconds: 300,
            sampling: LocalModelSamplingProfile::default(),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn alm6_construction_does_not_create_inference_storage() {
        let root = std::env::temp_dir().join(format!("elixir-alm6-no-io-{}", uuid::Uuid::new_v4()));
        assert!(!root.exists());
        let _service = test_service(root.clone());
        assert!(!root.exists());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn alm8_activation_signal_waits_for_first_active_publication() {
        let root = std::env::temp_dir().join(format!(
            "elixir-alm8-initial-activation-{}",
            uuid::Uuid::new_v4()
        ));
        let service = Arc::new(test_service(root));

        service
            .transition(
                AnimeInferenceLifecycleState::Bootstrapping,
                None,
                None,
                None,
                None,
            )
            .await;
        assert_eq!(service.activation_generation(), 0);

        let mut waiter = tokio::spawn({
            let service = service.clone();
            async move { service.wait_for_activation_after(0).await }
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut waiter)
                .await
                .is_err(),
            "non-Active lifecycle transitions must not notify activation waiters"
        );

        service.publish_local_profile(test_runtime_profile()).await;
        assert_eq!(service.activation_generation(), 1);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), waiter)
                .await
                .expect("activation waiter timed out")
                .expect("activation waiter task panicked"),
            Some(1)
        );

        service.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn alm8_activation_signal_reports_updates_and_coalesces_publications() {
        let root = std::env::temp_dir().join(format!(
            "elixir-alm8-profile-update-{}",
            uuid::Uuid::new_v4()
        ));
        let service = Arc::new(test_service(root));

        service.publish_local_profile(test_runtime_profile()).await;
        assert_eq!(service.activation_generation(), 1);

        let update_waiter = tokio::spawn({
            let service = service.clone();
            async move { service.wait_for_activation_after(1).await }
        });
        tokio::task::yield_now().await;

        let mut updated_profile = test_runtime_profile();
        updated_profile.bundle_version = "2026.08.2".to_string();
        updated_profile.profile_fingerprint = format!("sha256:{}", "2".repeat(64));
        service.publish_local_profile(updated_profile).await;

        assert_eq!(service.activation_generation(), 2);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), update_waiter)
                .await
                .expect("update waiter timed out")
                .expect("update waiter task panicked"),
            Some(2)
        );
        assert_eq!(
            service.wait_for_activation_after(0).await,
            Some(2),
            "a late waiter must observe the latest coalesced generation"
        );

        service.shutdown().await;
    }

    #[tokio::test]
    async fn alm9_certified_profile_uncertified_host_silently_returns_to_deterministic_only() {
        let root = std::env::temp_dir().join(format!(
            "elixir-alm9-uncertified-host-{}",
            uuid::Uuid::new_v4()
        ));
        let service = test_service(root);
        service.publish_local_profile(test_runtime_profile()).await;
        service
            .admission
            .resource_suspended
            .store(true, Ordering::Release);

        service.remain_deterministic_only().await;

        let snapshot = service.snapshot().await;
        assert_eq!(
            snapshot.state,
            AnimeInferenceLifecycleState::DeterministicOnly
        );
        assert!(snapshot.deterministic_fallback_available);
        assert!(snapshot.bundle_version.is_none());
        assert!(snapshot.model_id.is_none());
        assert!(snapshot.backend.is_none());
        assert!(snapshot.profile_fingerprint.is_none());
        assert!(snapshot.last_error.is_none());
        assert!(!snapshot.resource_suspended);
        assert!(service.active_profile.read().await.is_none());

        service.shutdown().await;
    }

    #[tokio::test]
    async fn alm6_failure_health_preserves_a_real_profile_and_clears_stale_identity_without_one() {
        let root =
            std::env::temp_dir().join(format!("elixir-alm6-health-{}", uuid::Uuid::new_v4()));
        let service = test_service(root);
        service
            .transition(
                AnimeInferenceLifecycleState::Downloading,
                Some("stale-bundle".to_string()),
                Some("stale-model".to_string()),
                Some("cuda".to_string()),
                Some(format!("sha256:{}", "2".repeat(64))),
            )
            .await;
        service.record_failure("download failed").await;
        let snapshot = service.snapshot().await;
        assert_eq!(
            snapshot.state,
            AnimeInferenceLifecycleState::DeterministicOnly
        );
        assert!(snapshot.bundle_version.is_none());
        assert!(snapshot.backend.is_none());
        assert!(snapshot.profile_fingerprint.is_none());

        let profile = test_runtime_profile();
        *service.active_profile.write().await = Some(profile.clone());
        service
            .transition(
                AnimeInferenceLifecycleState::Downloading,
                Some("new-unhealthy-bundle".to_string()),
                Some("new-unhealthy-model".to_string()),
                None,
                None,
            )
            .await;
        service.record_failure("replacement failed").await;
        let snapshot = service.snapshot().await;
        assert_eq!(snapshot.state, AnimeInferenceLifecycleState::Active);
        assert_eq!(snapshot.bundle_version.as_deref(), Some("2026.08.1"));
        assert_eq!(snapshot.backend.as_deref(), Some("cpu"));
        assert_eq!(
            snapshot.profile_fingerprint.as_deref(),
            Some(profile.profile_fingerprint.as_str())
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn alm6_maintenance_gate_allows_only_manager_owned_probe_and_activation_phases() {
        let root = std::env::temp_dir().join(format!("elixir-alm6-gate-{}", uuid::Uuid::new_v4()));
        let service = test_service(root);
        // `u64::MAX` is the internal unknown-memory sentinel; use the largest
        // representable known value so this test isolates maintenance gating.
        service
            .admission
            .update_available_memory(Some(u64::MAX - 1));
        let _maintenance = service.admission.enter_maintenance();
        let profile = test_runtime_profile();
        assert!(
            service
                .admission
                .admit(LocalModelAdmissionPhase::Inference, &profile)
                .is_err()
        );
        assert!(
            service
                .admission
                .admit(LocalModelAdmissionPhase::WorkerStart, &profile)
                .is_err()
        );
        for phase in [
            LocalModelAdmissionPhase::ActivationWorkerStart,
            LocalModelAdmissionPhase::ProbeWorkerStart,
            LocalModelAdmissionPhase::ProbeInference,
        ] {
            service
                .admission
                .admit(phase, &profile)
                .expect("manager-owned phase bypasses maintenance only");
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn alm6_repair_scheduler_preserves_inflight_signals_and_offline_backoff() {
        let root = std::env::temp_dir().join(format!(
            "elixir-alm6-repair-schedule-{}",
            uuid::Uuid::new_v4()
        ));
        let service = test_service(root);

        service.request_runtime_repair();
        assert_eq!(
            *service.repair_schedule.lock().unwrap(),
            RepairScheduleState::Requested
        );
        assert!(service.claim_scheduled_repair());
        service.request_runtime_repair();
        assert_eq!(
            *service.repair_schedule.lock().unwrap(),
            RepairScheduleState::Running {
                followup_requested: true
            }
        );
        service.finish_scheduled_repair(true);
        assert_eq!(
            *service.repair_schedule.lock().unwrap(),
            RepairScheduleState::Requested
        );

        assert!(service.claim_scheduled_repair());
        service.finish_scheduled_repair(false);
        assert_eq!(
            *service.repair_schedule.lock().unwrap(),
            RepairScheduleState::Backoff
        );
        service.request_runtime_repair();
        assert_eq!(
            *service.repair_schedule.lock().unwrap(),
            RepairScheduleState::Backoff,
            "polling requests must coalesce during offline retry backoff"
        );
        assert!(service.claim_scheduled_repair());
    }

    #[test]
    fn alm6_production_qualification_list_is_strict_json() {
        let approvals: Vec<QualifiedAnimeBundleApproval> =
            serde_json::from_str(include_str!("qualified-anime-bundles.json")).unwrap();
        assert!(approvals.is_empty(), "ALM-9 owns production approvals");
    }

    #[test]
    fn alm6_manifest_url_is_first_party_and_https() {
        let url = Url::parse(FIRST_PARTY_MANIFEST_URL).unwrap();
        assert_eq!(url.scheme(), "https");
        assert_eq!(url.host_str(), Some("releases.elixir-media.com"));
        assert_eq!(url.path(), "/anime/stable/anime-inference-channel.json");
        assert!(url.query().is_none());
        assert!(url.fragment().is_none());
    }
}
