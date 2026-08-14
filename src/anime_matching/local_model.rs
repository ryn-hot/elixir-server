//! Managed `llama-server` implementation of the anime matching engine.
//!
//! Bundle acquisition and host-profile selection intentionally live outside
//! this module. This module owns only the active worker: its process lifetime,
//! loopback protocol, fixed prompt/schema, queue bound, deadlines, and runtime
//! provenance.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    ffi::OsString,
    future::Future,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener as StdTcpListener},
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    sync::{
        Arc, Mutex as StdMutex, RwLock as StdRwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use async_trait::async_trait;
use reqwest::{Client, StatusCode, header::CONTENT_TYPE, redirect::Policy};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{
    io::AsyncReadExt,
    process::{Child, Command},
    sync::{Mutex, RwLock, Semaphore, TryAcquireError},
    time::{MissedTickBehavior, interval, sleep, timeout_at},
};
use tokio_util::sync::CancellationToken;

use crate::metrics::{
    ANIME_INFERENCE_EVENTS, ANIME_INFERENCE_OPERATION_DURATION, ANIME_INFERENCE_QUEUE_DEPTH,
    ANIME_INFERENCE_RUNTIME_STATE, ANIME_INFERENCE_WORKER_RSS_BYTES,
};

use super::{
    ANIME_MATCH_SCHEMA_VERSION, AnimeCandidateMatch, AnimeMatchAudioProfile,
    AnimeMatchContextTarget, AnimeMatchEngine, AnimeMatchEngineOutput, AnimeMatchRequest,
    AnimeMatchResponse, AnimeMatchRuntimeProvenance, AnimeMatchSeasonContext,
    AnimeSemanticEvidenceEngine, AnimeSemanticEvidenceEngineOutput, AnimeSemanticEvidenceRequest,
    AnimeSemanticEvidenceResponse, anime_match_alias_equivalence_key, hardware::InferenceBackend,
    prime_request,
};

const LOOPBACK_HOST: &str = "127.0.0.1";
const V1_CONTEXT_TOKENS: u32 = 4_096;
const V1_PARALLEL: u32 = 1;
// Difficult anime resolution is background work where correctness dominates
// latency. This is a failure boundary, not a performance target: playback can
// defer/restart the work within this envelope and deterministic fallback is
// used only after the model genuinely cannot complete.
const REQUEST_DEADLINE: Duration = Duration::from_secs(30 * 60);
// Priming is an internal cold-start operation. It remains separately bounded
// so a cold worker cannot consume the full correctness envelope before the
// actual match begins.
const PRIME_DEADLINE: Duration = Duration::from_secs(5 * 60);
const COLD_READINESS_DEADLINE: Duration = Duration::from_secs(2 * 60);
const READINESS_POLL_INTERVAL: Duration = Duration::from_millis(100);
const ADMISSION_POLL_INTERVAL: Duration = Duration::from_millis(100);
const BACKGROUND_TICK: Duration = Duration::from_secs(1);
const PROCESS_STOP_GRACE: Duration = Duration::from_millis(500);
const HTTP_CONNECT_TIMEOUT: Duration = Duration::from_millis(500);
const MAX_HTTP_RESPONSE_BYTES: usize = 256 * 1024;
const MAX_WORKER_DIAGNOSTIC_BYTES: usize = 4 * 1024;
const QUEUE_AND_ACTIVE_CAPACITY: usize = 2;

/// Prompt behavior is owned by the server release, not by a downloadable
/// bundle. A bundle can name this revision but cannot replace its semantics.
pub const ANIME_MATCH_PROMPT_REVISION: &str = "anime-semantic-evidence-v2";
pub const ANIME_MATCH_RESPONSE_SCHEMA_REVISION: &str = "anime-semantic-evidence-response-v1";
pub const ANIME_MATCH_SAMPLING_REVISION: &str = "anime-match-v1";
pub const LLAMA_SERVER_PROTOCOL_VERSION: u32 = 1;

const DIRECT_MATCH_PROMPT: &str = r#"Check every anime candidate against the exact wanted target.

`target` is the requested movie, special, season, range, or episode. Each `seasons` item owns its aliases and episode coordinates; `episodes[].wanted` is the output target index. `release` and file `name` are raw names. Candidate and file `index` values are output indexes.

Use English, romaji, Japanese, and alternate titles plus seasonal and absolute numbering. Different sequel names can be seasons of one franchise: for example Tokyo Ghoul Root A is season 2, Tokyo Ghoul:re is season 3, and Tokyo Ghoul:re 2 is season 4. Do not require literal SxxExx when an alias or absolute number resolves the target. Ranges cover every episode in the range. Movies, specials, OVAs, samples, NCOP, and NCED do not cover normal episodes.

First output one `d` decision for every candidate in input order: 0 means contradicted or wrong, 1 means ambiguous or missing affirmative evidence, and 2 means the raw release/files affirmatively identify the wanted title or owned alias, season, episode/range, and media kind. The target metadata is reference context, not evidence that a candidate matches. An unrelated title is always 0. The same episode number with the wrong title, the same franchise with the wrong season, and an off-by-one episode are always 0. An exact match can occur at any candidate index. Only candidates rated 2 may appear in `m`, and every candidate rated 2 must appear once.

For `require`, audio evidence must match `audio.accepted`; `require_dub` accepts dubbed or dual-audio. `any`, `prefer`, and `prefer_dub` never reject an otherwise correct title match. For packs, select only the file indexes that cover the returned wanted indexes.

Example: target Example Show season 2 episode 3; candidates are `Other Show S02E03`, `Example Show S01E03`, and `Example Show: Return - 03`, where Return is the season-2 alias. The result decisions are `[0,0,2]` and only candidate 2 is mapped. If every candidate is an unrelated title, wrong season, or wrong episode, all decisions are 0 and mappings are empty.

JSON only: {\"d\":[decision per candidate],\"m\":[[candidate index,[wanted indexes],[file indexes]],...]}. Fileless candidates use an empty file list."#;

const SEMANTIC_EVIDENCE_PROMPT: &str = r#"Interpret one raw anime release or filename using only the supplied title candidates, entities, and server-authored hypotheses.

Choose identity from title evidence first. Episode-number coincidence never proves identity. Each entity owns its aliases; a named sequel, part, cour, arc, movie, special, or OVA may differ from the franchise title. `releaseSeasonNumbers` lists the explicit season labels valid for that entity even when Elixir's canonical season differs. Seasonal and absolute numbering are different interpretations. Entity-only means the title/media entity is clear and Elixir should retain its deterministic number parsing. Select a numbered hypothesis only when entity, numbering, and media kind all agree. Return null for an unrelated title, conflicting episode, insufficient title evidence, sample, opening, ending, or extra.

Do not invent a title, entity, number, media kind, or hypothesis. Output JSON only: {\"schemaVersion\":1,\"hypothesisIndex\":<supplied integer or null>}."#;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LocalModelSamplingProfile {
    pub revision: String,
    pub temperature: f32,
    pub top_p: f32,
    pub top_k: u32,
    pub min_p: f32,
    pub seed: i64,
}

impl Default for LocalModelSamplingProfile {
    fn default() -> Self {
        Self {
            revision: ANIME_MATCH_SAMPLING_REVISION.to_string(),
            temperature: 0.0,
            top_p: 1.0,
            top_k: 0,
            min_p: 0.0,
            seed: 0,
        }
    }
}

impl LocalModelSamplingProfile {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.revision == ANIME_MATCH_SAMPLING_REVISION,
            "unsupported sampling revision '{}'",
            self.revision
        );
        ensure!(
            self.temperature.is_finite(),
            "sampling temperature is not finite"
        );
        ensure!(
            (0.0..=2.0).contains(&self.temperature),
            "sampling temperature is outside 0..=2"
        );
        ensure!(self.top_p.is_finite(), "sampling top_p is not finite");
        ensure!(
            (0.0..=1.0).contains(&self.top_p),
            "sampling top_p is outside 0..=1"
        );
        ensure!(self.min_p.is_finite(), "sampling min_p is not finite");
        ensure!(
            (0.0..=1.0).contains(&self.min_p),
            "sampling min_p is outside 0..=1"
        );
        ensure!(self.top_k <= 10_000, "sampling top_k is unreasonably large");
        Ok(())
    }
}

/// Fully resolved, already verified execution profile supplied by the bundle
/// and hardware-envelope layer.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LocalModelRuntimeProfile {
    pub bundle_version: String,
    pub model_id: String,
    pub model_revision: String,
    pub worker_revision: String,
    pub backend: String,
    pub profile_fingerprint: String,
    pub protocol_version: u32,
    pub matcher_schema_version: u32,
    pub prompt_revision: String,
    pub worker_path: PathBuf,
    pub model_path: PathBuf,
    pub context_tokens: u32,
    pub max_output_tokens: u32,
    pub threads: u32,
    pub batch_threads: u32,
    pub gpu_layers: u32,
    pub kv_cache_type: String,
    pub peak_rss_bytes: u64,
    pub idle_unload_seconds: u64,
    pub sampling: LocalModelSamplingProfile,
}

impl LocalModelRuntimeProfile {
    pub fn validate_contract(&self) -> Result<()> {
        for (label, value) in [
            ("bundle version", self.bundle_version.as_str()),
            ("model id", self.model_id.as_str()),
            ("model revision", self.model_revision.as_str()),
            ("worker revision", self.worker_revision.as_str()),
            ("backend", self.backend.as_str()),
            ("profile fingerprint", self.profile_fingerprint.as_str()),
        ] {
            ensure!(!value.trim().is_empty(), "{label} is empty");
        }
        ensure!(
            self.protocol_version == LLAMA_SERVER_PROTOCOL_VERSION,
            "unsupported llama worker protocol version {}",
            self.protocol_version
        );
        ensure!(
            self.matcher_schema_version == ANIME_MATCH_SCHEMA_VERSION,
            "unsupported anime matcher schema version {}",
            self.matcher_schema_version
        );
        ensure!(
            self.prompt_revision == ANIME_MATCH_PROMPT_REVISION,
            "bundle prompt revision '{}' is incompatible with server prompt '{}'",
            self.prompt_revision,
            ANIME_MATCH_PROMPT_REVISION
        );
        ensure!(
            self.context_tokens == V1_CONTEXT_TOKENS,
            "V1 context must be {V1_CONTEXT_TOKENS} tokens"
        );
        ensure!(
            (1..self.context_tokens).contains(&self.max_output_tokens),
            "max output tokens must be below the context size"
        );
        ensure!(
            (1..=4).contains(&self.threads),
            "V1 worker thread count is outside 1..=4"
        );
        ensure!(
            self.threads <= self.batch_threads && self.batch_threads <= 8,
            "V1 worker batch thread count must be between generation threads and 8"
        );
        ensure!(
            self.gpu_layers <= 512,
            "GPU layer count is unreasonably large"
        );
        ensure!(
            matches!(
                self.backend.as_str(),
                "cpu" | "metal" | "cuda" | "hip" | "vulkan"
            ),
            "unsupported inference backend '{}'",
            self.backend
        );
        ensure!(
            (self.backend == "cpu" && self.gpu_layers == 0)
                || (self.backend != "cpu" && self.gpu_layers > 0),
            "GPU layer count is incompatible with backend '{}'",
            self.backend
        );
        ensure!(
            matches!(self.kv_cache_type.as_str(), "f16" | "q8_0"),
            "unsupported KV cache type '{}'",
            self.kv_cache_type
        );
        ensure!(
            self.peak_rss_bytes > 0,
            "qualified peak worker RSS must be positive"
        );
        ensure!(
            self.idle_unload_seconds > 0,
            "idle unload deadline must be positive"
        );
        ensure!(
            self.worker_path.is_absolute(),
            "worker path must be absolute"
        );
        ensure!(self.model_path.is_absolute(), "model path must be absolute");
        self.sampling.validate()
    }

    async fn validate_installed_files(&self) -> Result<()> {
        validate_regular_file(&self.worker_path, "worker").await?;
        validate_regular_file(&self.model_path, "model").await
    }

    fn provenance(&self) -> AnimeMatchRuntimeProvenance {
        AnimeMatchRuntimeProvenance {
            bundle_version: self.bundle_version.clone(),
            model_id: self.model_id.clone(),
            model_revision: self.model_revision.clone(),
            worker_revision: self.worker_revision.clone(),
            backend: self.backend.clone(),
            profile_fingerprint: self.profile_fingerprint.clone(),
            prompt_revision: self.prompt_revision.clone(),
            protocol_version: self.protocol_version,
        }
    }
}

async fn validate_regular_file(path: &Path, label: &str) -> Result<()> {
    let metadata = tokio::fs::symlink_metadata(path)
        .await
        .with_context(|| format!("reading installed {label} at {}", path.display()))?;
    ensure!(
        metadata.is_file(),
        "installed {label} is not a regular file"
    );
    ensure!(
        !metadata.file_type().is_symlink(),
        "installed {label} must not be a symbolic link"
    );
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalModelAdmissionPhase {
    WorkerStart,
    ActivationWorkerStart,
    ActivationInference,
    ProbeWorkerStart,
    ProbeInference,
    Inference,
}

/// Hook used by the host hardware/resource service. Returning an error causes
/// work to defer within the production correctness deadline. An in-flight
/// inference rejection unloads the worker so streaming and transcoding keep
/// priority; production matching resumes automatically when resources return.
pub trait LocalModelAdmission: Send + Sync {
    fn admit(
        &self,
        phase: LocalModelAdmissionPhase,
        profile: &LocalModelRuntimeProfile,
    ) -> Result<()>;
}

#[derive(Debug, thiserror::Error)]
#[error("local model resource admission was revoked: {0}")]
struct InFlightAdmissionRejected(String);

enum AdmissionMonitored<T> {
    Completed(Result<T>),
    Rejected(anyhow::Error),
    Shutdown,
}

async fn monitor_inference_admission<F, T>(
    admission: &dyn LocalModelAdmission,
    phase: LocalModelAdmissionPhase,
    profile: &LocalModelRuntimeProfile,
    shutdown: &CancellationToken,
    future: F,
) -> AdmissionMonitored<T>
where
    F: Future<Output = Result<T>>,
{
    let mut poll = interval(ADMISSION_POLL_INTERVAL);
    poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // Tokio intervals tick immediately once. Consume that tick because the
    // caller performs an admission check immediately before starting.
    poll.tick().await;
    tokio::pin!(future);
    loop {
        tokio::select! {
            biased;
            result = &mut future => {
                return match result {
                    Err(error) => AdmissionMonitored::Completed(Err(error)),
                    Ok(value) => match admission.admit(phase, profile) {
                        Ok(()) => AdmissionMonitored::Completed(Ok(value)),
                        Err(error) => AdmissionMonitored::Rejected(error),
                    },
                };
            }
            _ = shutdown.cancelled() => return AdmissionMonitored::Shutdown,
            _ = poll.tick() => {
                if let Err(error) = admission.admit(phase, profile) {
                    return AdmissionMonitored::Rejected(error);
                }
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct AllowLocalModelAdmission;

impl LocalModelAdmission for AllowLocalModelAdmission {
    fn admit(
        &self,
        _phase: LocalModelAdmissionPhase,
        _profile: &LocalModelRuntimeProfile,
    ) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LocalModelWorkerState {
    Inactive,
    Starting,
    Ready,
    Unavailable,
    ShuttingDown,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalModelSnapshot {
    pub state: LocalModelWorkerState,
    pub profile_fingerprint: Option<String>,
    pub backend: Option<String>,
    pub process_id: Option<u32>,
    pub loopback_port: Option<u16>,
    pub resident_rss_bytes: Option<u64>,
    pub last_error: Option<String>,
}

impl LocalModelSnapshot {
    fn inactive() -> Self {
        Self {
            state: LocalModelWorkerState::Inactive,
            profile_fingerprint: None,
            backend: None,
            process_id: None,
            loopback_port: None,
            resident_rss_bytes: None,
            last_error: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalModelProbeMeasurement {
    pub worker_ready: bool,
    pub smoke_match_passed: bool,
    pub load_time_ms: u64,
    pub warm_latency_ms: u64,
    pub current_rss_bytes: Option<u64>,
    pub peak_rss_bytes: Option<u64>,
    pub priming_response: AnimeMatchResponse,
    pub response: AnimeMatchResponse,
}

/// Release-certification measurement from the exact production worker path.
/// This is intentionally not exposed through HTTP or product configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalModelBenchmarkMeasurement {
    pub output: AnimeMatchEngineOutput,
    pub prompt_tokens: u64,
    pub generated_tokens: u64,
    pub prompt_time_ms: u64,
    pub generation_time_ms: u64,
    pub elapsed_ms: u64,
}

/// Release-only observation from the exact managed worker. These values are
/// compared with source-tokenizer fixtures by the model-smoke producer; the
/// worker address never leaves the engine and no product API exposes these
/// llama.cpp maintenance endpoints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalModelContractSmokeMeasurement {
    pub tokenizations: Vec<Vec<i64>>,
    pub rendered_templates: Vec<String>,
}

#[derive(Debug)]
struct LocalModelCompletion {
    response: LocalModelResponse,
    prompt_tokens: Option<u64>,
    generated_tokens: Option<u64>,
    prompt_time_ms: Option<u64>,
    generation_time_ms: Option<u64>,
}

#[derive(Debug, Clone)]
enum LocalModelRequest {
    Match(AnimeMatchRequest),
    Semantic(AnimeSemanticEvidenceRequest),
}

#[derive(Debug, Clone)]
enum LocalModelResponse {
    Match(AnimeMatchResponse),
    Semantic(AnimeSemanticEvidenceResponse),
}

impl LocalModelCompletion {
    fn match_response(&self) -> Result<&AnimeMatchResponse> {
        match &self.response {
            LocalModelResponse::Match(response) => Ok(response),
            LocalModelResponse::Semantic(_) => {
                bail!("local model returned semantic evidence to a match request")
            }
        }
    }

    fn semantic_response(&self) -> Result<&AnimeSemanticEvidenceResponse> {
        match &self.response {
            LocalModelResponse::Semantic(response) => Ok(response),
            LocalModelResponse::Match(_) => {
                bail!("local model returned a match plan to a semantic request")
            }
        }
    }
}

struct WorkerSlot {
    worker: Option<ManagedWorker>,
    state: LocalModelWorkerState,
    metric_backend: &'static str,
    publish_runtime_metrics: bool,
    last_activity: Instant,
    resident_rss_bytes: Option<u64>,
    last_error: Option<String>,
    snapshot_cache: Arc<StdRwLock<LocalModelSnapshot>>,
}

impl WorkerSlot {
    fn new(
        publish_runtime_metrics: bool,
        snapshot_cache: Arc<StdRwLock<LocalModelSnapshot>>,
    ) -> Self {
        Self {
            worker: None,
            state: LocalModelWorkerState::Inactive,
            metric_backend: "none",
            publish_runtime_metrics,
            last_activity: Instant::now(),
            resident_rss_bytes: None,
            last_error: None,
            snapshot_cache,
        }
    }
}

struct ManagedWorker {
    child: Child,
    _isolation: ProcessIsolation,
    address: SocketAddr,
    profile_fingerprint: String,
    primed: bool,
    diagnostic_tail: Arc<WorkerDiagnosticTail>,
    diagnostic_task: tokio::task::JoinHandle<()>,
    #[cfg(unix)]
    process_group_id: i32,
    #[cfg(unix)]
    kill_group_on_drop: bool,
}

impl Drop for ManagedWorker {
    fn drop(&mut self) {
        self.diagnostic_task.abort();
        #[cfg(unix)]
        if self.kill_group_on_drop {
            let _ = unsafe { libc::kill(-self.process_group_id, libc::SIGKILL) };
        }
        // Windows descendants are terminated when `_isolation` closes its
        // kill-on-close Job Object; `Child::kill_on_drop` covers the primary
        // process on every platform.
    }
}

#[derive(Debug, Default)]
struct WorkerDiagnosticTail {
    bytes: StdMutex<VecDeque<u8>>,
}

impl WorkerDiagnosticTail {
    fn push(&self, bytes: &[u8]) {
        let mut tail = self
            .bytes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let excess = tail
            .len()
            .saturating_add(bytes.len())
            .saturating_sub(MAX_WORKER_DIAGNOSTIC_BYTES);
        let drain = excess.min(tail.len());
        tail.drain(..drain);
        let keep_from = bytes.len().saturating_sub(MAX_WORKER_DIAGNOSTIC_BYTES);
        tail.extend(&bytes[keep_from..]);
    }

    fn excerpt(&self) -> Option<String> {
        let tail = self
            .bytes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let bytes = tail.iter().copied().collect::<Vec<_>>();
        let text = String::from_utf8_lossy(&bytes);
        let text = text.trim();
        (!text.is_empty()).then(|| {
            let character_count = text.chars().count();
            text.chars()
                .skip(character_count.saturating_sub(1_024))
                .collect()
        })
    }
}

fn unexpected_worker_exit(worker: &ManagedWorker, status: &ExitStatus) -> String {
    match worker.diagnostic_tail.excerpt() {
        Some(diagnostic) => {
            format!("llama-server exited unexpectedly: {status}; stderr: {diagnostic}")
        }
        None => format!("llama-server exited unexpectedly: {status}"),
    }
}

struct LocalModelInner {
    profile: RwLock<Option<Arc<LocalModelRuntimeProfile>>>,
    worker: Mutex<WorkerSlot>,
    http: Client,
    admission: Arc<dyn LocalModelAdmission>,
    total_slots: Arc<Semaphore>,
    execution_slot: Arc<Semaphore>,
    background_warm_active: AtomicBool,
    background_prime_requested: AtomicU64,
    background_prime_completed: AtomicU64,
    restart_used: AtomicBool,
    publish_runtime_metrics: bool,
    snapshot_cache: Arc<StdRwLock<LocalModelSnapshot>>,
    shutdown: CancellationToken,
}

/// Cloneable native worker engine. Construction performs no file, process, or
/// network I/O, so it is safe to place in `AppState` before the API binds.
#[derive(Clone)]
pub struct LocalModelEngine {
    inner: Arc<LocalModelInner>,
}

impl LocalModelEngine {
    pub fn new(admission: Arc<dyn LocalModelAdmission>) -> Result<Self> {
        Self::new_inner(admission, true)
    }

    /// Constructs an isolated hardware-probe engine without replacing the
    /// active service's process-state and RSS gauges.
    pub fn new_for_probe(admission: Arc<dyn LocalModelAdmission>) -> Result<Self> {
        Self::new_inner(admission, false)
    }

    fn new_inner(
        admission: Arc<dyn LocalModelAdmission>,
        publish_runtime_metrics: bool,
    ) -> Result<Self> {
        let http = Client::builder()
            .no_proxy()
            .redirect(Policy::none())
            .connect_timeout(HTTP_CONNECT_TIMEOUT)
            .build()
            .context("building loopback llama-server HTTP client")?;
        if publish_runtime_metrics {
            set_runtime_state_metric(LocalModelWorkerState::Inactive, "none");
        }
        let snapshot_cache = Arc::new(StdRwLock::new(LocalModelSnapshot::inactive()));
        Ok(Self {
            inner: Arc::new(LocalModelInner {
                profile: RwLock::new(None),
                worker: Mutex::new(WorkerSlot::new(
                    publish_runtime_metrics,
                    snapshot_cache.clone(),
                )),
                http,
                admission,
                total_slots: Arc::new(Semaphore::new(QUEUE_AND_ACTIVE_CAPACITY)),
                execution_slot: Arc::new(Semaphore::new(1)),
                background_warm_active: AtomicBool::new(false),
                background_prime_requested: AtomicU64::new(0),
                background_prime_completed: AtomicU64::new(0),
                restart_used: AtomicBool::new(false),
                publish_runtime_metrics,
                snapshot_cache,
                shutdown: CancellationToken::new(),
            }),
        })
    }

    pub fn allow_all() -> Result<Self> {
        Self::new(Arc::new(AllowLocalModelAdmission))
    }

    pub fn allow_all_for_probe() -> Result<Self> {
        Self::new_for_probe(Arc::new(AllowLocalModelAdmission))
    }

    /// Replace the active execution profile atomically with respect to model
    /// calls. Bundle smoke/verification is expected to have completed first.
    pub async fn activate_profile(&self, profile: LocalModelRuntimeProfile) -> Result<()> {
        self.activate_profile_inner(profile, true).await
    }

    /// Installs a production profile without scheduling a detached warm. The
    /// lifecycle manager can then await exactly one bounded `warm()` call and
    /// publish Active only after that health verification succeeds.
    pub async fn activate_profile_cold(&self, profile: LocalModelRuntimeProfile) -> Result<()> {
        ensure!(
            self.inner.publish_runtime_metrics,
            "cold manager activation requires the production local model engine"
        );
        self.activate_profile_inner(profile, false).await
    }

    /// Probe-only activation for a disposable engine. It deliberately avoids
    /// scheduling the normal warm task so `probe` owns exactly one cold spawn.
    pub async fn activate_profile_for_probe(
        &self,
        profile: LocalModelRuntimeProfile,
    ) -> Result<()> {
        ensure!(
            !self.inner.publish_runtime_metrics,
            "probe activation requires a probe-only local model engine"
        );
        self.activate_profile_inner(profile, false).await
    }

    async fn activate_profile_inner(
        &self,
        profile: LocalModelRuntimeProfile,
        schedule_warm: bool,
    ) -> Result<()> {
        let mut operation = MetricOperation::new("profile_activation");
        profile.validate_contract()?;
        profile.validate_installed_files().await?;
        ensure!(
            !self.inner.shutdown.is_cancelled(),
            "local model engine is shut down"
        );

        let _total = self
            .inner
            .total_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| anyhow!("local model engine is shut down"))?;
        let _execution = self
            .inner
            .execution_slot
            .acquire()
            .await
            .map_err(|_| anyhow!("local model engine is shut down"))?;
        let mut current = self.inner.profile.write().await;
        if let Some(active) = current.as_ref() {
            ensure!(
                active.profile_fingerprint != profile.profile_fingerprint
                    || active.as_ref() == &profile,
                "runtime profile changed without changing its fingerprint"
            );
        }
        let changed = current
            .as_ref()
            .is_none_or(|active| active.profile_fingerprint != profile.profile_fingerprint);
        if changed {
            let mut slot = self.inner.worker.lock().await;
            stop_worker(&mut slot).await;
            slot.metric_backend = metric_backend(&profile.backend);
            update_snapshot_profile(&self.inner.snapshot_cache, Some(&profile));
            slot.last_error = None;
            transition_worker_state(&mut slot, LocalModelWorkerState::Inactive);
            self.inner.restart_used.store(false, Ordering::Release);
        }
        *current = Some(Arc::new(profile));
        drop(current);
        if changed && schedule_warm {
            self.schedule_background_warm();
        }
        operation.succeed();
        Ok(())
    }

    pub async fn clear_profile(&self) {
        let _total = self.inner.total_slots.clone().acquire_owned().await.ok();
        let _execution = self.inner.execution_slot.acquire().await.ok();
        *self.inner.profile.write().await = None;
        update_snapshot_profile(&self.inner.snapshot_cache, None);
        let mut slot = self.inner.worker.lock().await;
        stop_worker(&mut slot).await;
        slot.metric_backend = "none";
        transition_worker_state(&mut slot, LocalModelWorkerState::Inactive);
        inference_event("profile_clear", "success");
    }

    /// Stops the resident worker while retaining the selected profile. The
    /// admission hook remains authoritative, so later automatic work either
    /// warms transparently or continues to return deterministic fallback.
    pub async fn suspend(&self) -> Result<()> {
        ensure!(
            !self.inner.shutdown.is_cancelled(),
            "local model engine is shut down"
        );
        let mut operation = MetricOperation::new("worker_suspend");
        let _total = self
            .inner
            .total_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| anyhow!("local model engine is shut down"))?;
        let _execution = self
            .inner
            .execution_slot
            .acquire()
            .await
            .map_err(|_| anyhow!("local model engine is shut down"))?;
        let mut slot = self.inner.worker.lock().await;
        stop_worker(&mut slot).await;
        inference_event("worker_unload", "resource_pressure");
        operation.succeed();
        Ok(())
    }

    /// Start the selected worker and run one exact, production-shaped model
    /// request. This is idempotent for the lifetime of a healthy worker.
    pub async fn prime(&self) -> Result<()> {
        let phases = if self.inner.publish_runtime_metrics {
            (
                LocalModelAdmissionPhase::WorkerStart,
                LocalModelAdmissionPhase::Inference,
            )
        } else {
            (
                LocalModelAdmissionPhase::ProbeWorkerStart,
                LocalModelAdmissionPhase::ProbeInference,
            )
        };
        self.prime_with_phases(phases.0, phases.1).await
    }

    /// Manager-owned prime used while ordinary inference is maintenance-gated
    /// during an atomic bundle activation. It bypasses only that gate; the
    /// admission implementation still enforces playback and live memory.
    pub async fn prime_for_activation(&self) -> Result<()> {
        ensure!(
            self.inner.publish_runtime_metrics,
            "activation prime requires the production local model engine"
        );
        self.prime_with_phases(
            LocalModelAdmissionPhase::ActivationWorkerStart,
            LocalModelAdmissionPhase::ActivationInference,
        )
        .await
    }

    /// Compatibility alias retained for internal release tooling. A warm is a
    /// real model prime, never a readiness-only health check.
    pub async fn warm(&self) -> Result<()> {
        self.prime().await
    }

    /// Compatibility alias retained for tests and older manager call sites.
    pub async fn warm_for_activation(&self) -> Result<()> {
        self.prime_for_activation().await
    }

    async fn prime_with_phases(
        &self,
        worker_phase: LocalModelAdmissionPhase,
        inference_phase: LocalModelAdmissionPhase,
    ) -> Result<()> {
        let mut operation = MetricOperation::new("worker_prime");
        ensure!(
            !self.inner.shutdown.is_cancelled(),
            "local model engine is shut down"
        );
        let _total = self
            .inner
            .total_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| anyhow!("local model engine is shut down"))?;
        let _execution = self
            .inner
            .execution_slot
            .acquire()
            .await
            .map_err(|_| anyhow!("local model engine is shut down"))?;
        let profile = self.active_profile().await?;
        let request = prime_request()?;
        let mut slot = self.inner.worker.lock().await;
        self.ensure_primed_locked(&profile, &mut slot, worker_phase, inference_phase, &request)
            .await?;
        operation.succeed();
        Ok(())
    }

    async fn ensure_primed_locked(
        &self,
        profile: &LocalModelRuntimeProfile,
        slot: &mut WorkerSlot,
        worker_phase: LocalModelAdmissionPhase,
        inference_phase: LocalModelAdmissionPhase,
        request: &AnimeMatchRequest,
    ) -> Result<SocketAddr> {
        if slot.worker.as_ref().is_some_and(|worker| {
            worker.profile_fingerprint == profile.profile_fingerprint && worker.primed
        }) {
            let deadline = tokio::time::Instant::now() + COLD_READINESS_DEADLINE;
            return match timeout_at(deadline, self.ensure_ready(profile, slot, worker_phase)).await
            {
                Ok(result) => result.context("checking primed llama-server readiness"),
                Err(_) => {
                    let error = anyhow!("primed llama-server readiness deadline exceeded");
                    abort_worker(slot).await;
                    slot.last_error = Some(bounded_error(&error));
                    transition_worker_state(slot, LocalModelWorkerState::Unavailable);
                    Err(error)
                }
            };
        }

        let readiness_deadline = tokio::time::Instant::now() + COLD_READINESS_DEADLINE;
        let address = match timeout_at(
            readiness_deadline,
            self.ensure_ready(profile, slot, worker_phase),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => {
                let error = anyhow!("llama-server cold readiness deadline exceeded");
                abort_worker(slot).await;
                slot.last_error = Some(bounded_error(&error));
                transition_worker_state(slot, LocalModelWorkerState::Unavailable);
                return Err(error);
            }
        };

        if let Err(error) = self.inner.admission.admit(inference_phase, profile) {
            abort_worker(slot).await;
            slot.last_error = Some(bounded_error(&error));
            transition_worker_state(slot, LocalModelWorkerState::Inactive);
            return Err(error).context("local model prime deferred by resource admission");
        }
        let prime_deadline = tokio::time::Instant::now() + PRIME_DEADLINE;
        let completion = monitor_inference_admission(
            self.inner.admission.as_ref(),
            inference_phase,
            profile,
            &self.inner.shutdown,
            resolve_direct_request(&self.inner.http, address, request, profile),
        );
        let completion = match timeout_at(prime_deadline, completion).await {
            Ok(AdmissionMonitored::Completed(result)) => result,
            Ok(AdmissionMonitored::Rejected(error)) => {
                abort_worker(slot).await;
                slot.last_error = Some(bounded_error(&error));
                transition_worker_state(slot, LocalModelWorkerState::Inactive);
                return Err(error).context("local model prime admission was revoked");
            }
            Ok(AdmissionMonitored::Shutdown) => {
                abort_worker(slot).await;
                transition_worker_state(slot, LocalModelWorkerState::Inactive);
                return Err(anyhow!("local model engine is shutting down"));
            }
            Err(_) => Err(anyhow!("llama-server prime deadline exceeded")),
        };
        let _completion = match completion {
            Ok(completion) => completion,
            Err(error) => {
                // Dropping an HTTP future is not a protocol cancellation
                // guarantee. Never leave a failed prime consuming resources.
                abort_worker(slot).await;
                slot.last_error = Some(bounded_error(&error));
                transition_worker_state(slot, LocalModelWorkerState::Unavailable);
                return Err(error);
            }
        };
        // `request_completion` has already proved model load, tokenization,
        // grammar-constrained decoding, and request-local reference integrity.
        // Semantic accuracy belongs to the frozen corpus, not worker warm-up.
        let worker = slot
            .worker
            .as_mut()
            .ok_or_else(|| anyhow!("llama-server worker disappeared after prime"))?;
        ensure!(
            worker.profile_fingerprint == profile.profile_fingerprint,
            "llama-server profile changed during prime"
        );
        worker.primed = true;
        slot.resident_rss_bytes = worker_rss_bytes(&worker.child).await;
        slot.last_activity = Instant::now();
        slot.last_error = None;
        transition_worker_state(slot, LocalModelWorkerState::Ready);
        inference_event("worker_prime", "success");
        Ok(address)
    }

    /// Runs the real cold-load, template-aware token preflight, and constrained
    /// completion protocol for two distinct hardware-envelope smoke fixtures.
    /// The first request populates the newly loaded worker's cache.
    /// `warm_latency_ms` measures the second, distinct request, so only their
    /// shared production prefix is reusable and an exact cached user payload
    /// cannot make an incapable profile pass.
    /// Semantic correctness is deliberately not decided here; the frozen
    /// production-shaped corpus is the model-intelligence gate.
    pub async fn probe(
        &self,
        priming_request: AnimeMatchRequest,
        request: AnimeMatchRequest,
    ) -> Result<LocalModelProbeMeasurement> {
        let mut operation = MetricOperation::new("profile_probe");
        ensure!(
            !self.inner.publish_runtime_metrics,
            "hardware-envelope probe requires a probe-only local model engine"
        );
        ensure!(
            !self.inner.shutdown.is_cancelled(),
            "local model engine is shut down"
        );
        let _total = self
            .inner
            .total_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| anyhow!("local model engine is shut down"))?;
        let _execution = self
            .inner
            .execution_slot
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| anyhow!("local model engine is shut down"))?;
        ensure!(
            priming_request.request_id != request.request_id,
            "hardware-envelope priming and measured requests must be distinct"
        );
        let profile = self.active_profile().await?;
        let mut slot = self.inner.worker.lock().await;
        stop_worker(&mut slot).await;

        let load_started = Instant::now();
        let load_deadline = tokio::time::Instant::now() + COLD_READINESS_DEADLINE;
        let address = match timeout_at(
            load_deadline,
            self.ensure_ready(
                &profile,
                &mut slot,
                LocalModelAdmissionPhase::ProbeWorkerStart,
            ),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => {
                stop_worker(&mut slot).await;
                bail!("llama-server probe readiness deadline exceeded");
            }
        };
        let load_time_ms = duration_millis(load_started.elapsed());
        let rss_after_load = match slot.worker.as_ref() {
            Some(worker) => worker_rss_bytes(&worker.child).await,
            None => None,
        };

        let priming_completion = self
            .probe_completion(
                &profile,
                &mut slot,
                address,
                &priming_request,
                PRIME_DEADLINE,
                "llama-server probe priming completion deadline exceeded",
            )
            .await?;
        let primed_worker = slot
            .worker
            .as_mut()
            .ok_or_else(|| anyhow!("llama-server worker disappeared after probe prime"))?;
        primed_worker.primed = true;
        transition_worker_state(&mut slot, LocalModelWorkerState::Ready);
        let rss_after_priming = match slot.worker.as_ref() {
            Some(worker) => worker_rss_bytes(&worker.child).await,
            None => None,
        };

        let warm_started = Instant::now();
        let completion = self
            .probe_completion(
                &profile,
                &mut slot,
                address,
                &request,
                REQUEST_DEADLINE,
                "llama-server probe completion deadline exceeded",
            )
            .await?;
        let warm_latency_ms = duration_millis(warm_started.elapsed());
        let current_rss_bytes = match slot.worker.as_ref() {
            Some(worker) => worker_rss_bytes(&worker.child).await,
            None => None,
        };
        slot.resident_rss_bytes = current_rss_bytes;
        let peak_rss_bytes = rss_after_load
            .into_iter()
            .chain(rss_after_priming)
            .chain(current_rss_bytes)
            .max();
        slot.last_activity = Instant::now();
        transition_worker_state(&mut slot, LocalModelWorkerState::Ready);
        operation.succeed();
        // Both completions passed the production decoder and request-local
        // reference checks. Hardware probing must not duplicate the corpus's
        // semantic-accuracy decision.
        let smoke_match_passed = true;
        Ok(LocalModelProbeMeasurement {
            worker_ready: true,
            smoke_match_passed,
            load_time_ms,
            warm_latency_ms,
            current_rss_bytes,
            peak_rss_bytes,
            priming_response: priming_completion.match_response()?.clone(),
            response: completion.match_response()?.clone(),
        })
    }

    async fn probe_completion(
        &self,
        profile: &LocalModelRuntimeProfile,
        slot: &mut WorkerSlot,
        address: SocketAddr,
        request: &AnimeMatchRequest,
        completion_deadline: Duration,
        deadline_error: &'static str,
    ) -> Result<LocalModelCompletion> {
        if let Err(error) = self
            .inner
            .admission
            .admit(LocalModelAdmissionPhase::ProbeInference, profile)
        {
            inference_event("resource_admission", "deferred");
            stop_worker(slot).await;
            return Err(error).context("local model probe deferred by resource admission");
        }
        let request_deadline = tokio::time::Instant::now() + completion_deadline;
        let completion = monitor_inference_admission(
            self.inner.admission.as_ref(),
            LocalModelAdmissionPhase::ProbeInference,
            profile,
            &self.inner.shutdown,
            resolve_direct_request(&self.inner.http, address, request, profile),
        );
        let monitored = match timeout_at(request_deadline, completion).await {
            Ok(monitored) => monitored,
            Err(_) => {
                stop_worker(slot).await;
                bail!(deadline_error);
            }
        };
        match monitored {
            AdmissionMonitored::Completed(result) => result,
            AdmissionMonitored::Rejected(error) => {
                stop_worker(slot).await;
                Err(error).context("local model probe admission was revoked")
            }
            AdmissionMonitored::Shutdown => {
                stop_worker(slot).await;
                bail!("local model engine is shutting down");
            }
        }
    }

    /// Execute one request through the same queue, admission, deadline,
    /// tokenizer, constrained-decoding, and worker lifecycle used in
    /// production, while requiring llama.cpp's token accounting for the
    /// physical ALM-9 throughput gate.
    pub async fn benchmark_match(
        &self,
        request: AnimeMatchRequest,
    ) -> Result<LocalModelBenchmarkMeasurement> {
        let started = Instant::now();
        let (output, completion) = self.infer_measured(request).await?;
        let prompt_tokens = completion
            .prompt_tokens
            .ok_or_else(|| anyhow!("llama-server omitted prompt token accounting"))?;
        let generated_tokens = completion
            .generated_tokens
            .ok_or_else(|| anyhow!("llama-server omitted generated token accounting"))?;
        ensure!(
            prompt_tokens > 0,
            "llama-server reported zero prompt tokens"
        );
        ensure!(
            generated_tokens > 0,
            "llama-server reported zero generated tokens"
        );
        let prompt_time_ms = completion
            .prompt_time_ms
            .ok_or_else(|| anyhow!("llama-server omitted prompt timing"))?;
        let generation_time_ms = completion
            .generation_time_ms
            .ok_or_else(|| anyhow!("llama-server omitted generation timing"))?;
        ensure!(prompt_time_ms > 0, "llama-server reported zero prompt time");
        ensure!(
            generation_time_ms > 0,
            "llama-server reported zero generation time"
        );
        Ok(LocalModelBenchmarkMeasurement {
            output,
            prompt_tokens,
            generated_tokens,
            prompt_time_ms,
            generation_time_ms,
            elapsed_ms: duration_millis(started.elapsed()),
        })
    }

    /// Force the exact active packaged worker to exit for physical release
    /// certification. The exited child deliberately remains in the managed
    /// slot so the next ordinary production request must detect the crash and
    /// exercise the normal single-restart path. This is crate-private and has
    /// no product API or configuration surface.
    pub(crate) async fn crash_active_worker_for_certification(&self) -> Result<u32> {
        ensure!(
            !self.inner.publish_runtime_metrics,
            "worker crash certification requires a probe-only engine"
        );
        ensure!(
            !self.inner.shutdown.is_cancelled(),
            "local model engine is shut down"
        );
        let _total = self
            .inner
            .total_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| anyhow!("local model engine is shut down"))?;
        let _execution = self
            .inner
            .execution_slot
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| anyhow!("local model engine is shut down"))?;
        let mut slot = self.inner.worker.lock().await;
        let worker = slot
            .worker
            .as_mut()
            .ok_or_else(|| anyhow!("no active packaged worker to crash"))?;
        let process_id = worker
            .child
            .id()
            .ok_or_else(|| anyhow!("active packaged worker has no process ID"))?;
        force_terminate_managed_worker(worker)
            .await
            .context("forcing packaged worker crash for release certification")?;
        ensure!(
            worker.child.try_wait()?.is_some(),
            "forced packaged worker crash was not reaped"
        );
        Ok(process_id)
    }

    /// Run the ordinary production inference path with a deliberately short
    /// deadline so release certification can prove that an over-deadline real
    /// packaged worker is terminated before deterministic fallback returns.
    pub(crate) async fn match_with_deadline_for_certification(
        &self,
        request: AnimeMatchRequest,
        deadline: Duration,
    ) -> Result<AnimeMatchEngineOutput> {
        ensure!(
            !self.inner.publish_runtime_metrics,
            "worker deadline certification requires a probe-only engine"
        );
        ensure!(
            !deadline.is_zero() && deadline < REQUEST_DEADLINE,
            "certification deadline must be positive and shorter than production"
        );
        self.infer_measured_with_deadline(request, deadline)
            .await
            .map(|(output, _)| output)
    }

    /// Exercises the tokenizer and embedded chat template exposed by the
    /// exact managed worker. This is release-certification plumbing only: the
    /// worker remains private and callers receive only the bounded results
    /// required to compare a frozen source-model contract.
    pub async fn contract_smoke(
        &self,
        tokenizer_inputs: &[String],
        template_messages: &[Value],
    ) -> Result<LocalModelContractSmokeMeasurement> {
        ensure!(
            !self.inner.publish_runtime_metrics,
            "model contract smoke requires a probe-only local model engine"
        );
        ensure!(
            !self.inner.shutdown.is_cancelled(),
            "local model engine is shut down"
        );
        ensure!(
            (1..=64).contains(&tokenizer_inputs.len()),
            "model contract smoke requires 1..=64 tokenizer cases"
        );
        ensure!(
            (1..=32).contains(&template_messages.len()),
            "model contract smoke requires 1..=32 chat-template cases"
        );
        ensure!(
            tokenizer_inputs
                .iter()
                .all(|input| !input.is_empty() && input.len() <= 16 * 1024),
            "model contract smoke tokenizer input is empty or too large"
        );
        ensure!(
            template_messages.iter().all(Value::is_array),
            "model contract smoke messages must be arrays"
        );

        let _total = self
            .inner
            .total_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| anyhow!("local model engine is shut down"))?;
        let _execution = self
            .inner
            .execution_slot
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| anyhow!("local model engine is shut down"))?;
        let profile = self.active_profile().await?;
        self.inner
            .admission
            .admit(LocalModelAdmissionPhase::ProbeInference, &profile)
            .context("local model contract smoke deferred by resource admission")?;
        let mut slot = self.inner.worker.lock().await;
        let readiness_deadline = tokio::time::Instant::now() + COLD_READINESS_DEADLINE;
        let address = match timeout_at(
            readiness_deadline,
            self.ensure_ready(
                &profile,
                &mut slot,
                LocalModelAdmissionPhase::ProbeWorkerStart,
            ),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => {
                let error = anyhow!("llama-server contract-smoke readiness deadline exceeded");
                abort_worker(&mut slot).await;
                slot.last_error = Some(bounded_error(&error));
                transition_worker_state(&mut slot, LocalModelWorkerState::Unavailable);
                return Err(error);
            }
        };
        let deadline = tokio::time::Instant::now() + COLD_READINESS_DEADLINE;
        let smoke = timeout_at(deadline, async {
            let mut tokenizations = Vec::with_capacity(tokenizer_inputs.len());
            for input in tokenizer_inputs {
                tokenizations.push(request_tokenization(&self.inner.http, address, input).await?);
            }
            let mut rendered_templates = Vec::with_capacity(template_messages.len());
            for messages in template_messages {
                rendered_templates.push(
                    request_applied_template(
                        &self.inner.http,
                        address,
                        &profile.model_id,
                        messages,
                    )
                    .await?,
                );
            }
            Result::<_>::Ok(LocalModelContractSmokeMeasurement {
                tokenizations,
                rendered_templates,
            })
        })
        .await;
        let measurement = match smoke {
            Ok(result) => result?,
            Err(_) => {
                stop_worker(&mut slot).await;
                bail!("llama-server model contract smoke deadline exceeded");
            }
        };
        self.inner
            .admission
            .admit(LocalModelAdmissionPhase::ProbeInference, &profile)
            .context("local model contract smoke admission was revoked")?;
        slot.resident_rss_bytes = match slot.worker.as_ref() {
            Some(worker) => worker_rss_bytes(&worker.child).await,
            None => None,
        };
        slot.last_activity = Instant::now();
        let state = if slot.worker.as_ref().is_some_and(|worker| worker.primed) {
            LocalModelWorkerState::Ready
        } else {
            LocalModelWorkerState::Starting
        };
        transition_worker_state(&mut slot, state);
        Ok(measurement)
    }

    pub async fn snapshot(&self) -> LocalModelSnapshot {
        // Health must never queue behind a long-running completion while the
        // worker mutex is held. Refresh opportunistically and otherwise return
        // the last observation maintained by lifecycle transitions.
        if let Ok(slot) = self.inner.worker.try_lock() {
            refresh_worker_snapshot_cache(&slot);
        }
        self.inner
            .snapshot_cache
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Background lifecycle driver. The owning inference service should run it
    /// after the API binds and pass its server shutdown token.
    pub async fn run_background(&self, external_shutdown: CancellationToken) {
        let mut ticker = interval(BACKGROUND_TICK);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = external_shutdown.cancelled() => break,
                _ = self.inner.shutdown.cancelled() => break,
                _ = ticker.tick() => self.background_tick().await,
            }
        }
        self.shutdown().await;
    }

    pub async fn shutdown(&self) {
        let mut operation = MetricOperation::new("shutdown");
        self.inner.shutdown.cancel();
        self.inner.total_slots.close();
        self.inner.execution_slot.close();
        let mut slot = self.inner.worker.lock().await;
        transition_worker_state(&mut slot, LocalModelWorkerState::ShuttingDown);
        stop_worker(&mut slot).await;
        operation.succeed();
    }

    /// Request a coalesced background prime. Repeated triggers never spawn
    /// competing workers, and a profile change that races an older task is
    /// observed before that task exits.
    pub(crate) fn request_background_prime(&self) {
        self.schedule_background_warm();
    }

    fn schedule_background_warm(&self) {
        let _ = self.inner.background_prime_requested.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |generation| Some(generation.saturating_add(1)),
        );
        self.start_background_prime_if_idle();
    }

    fn start_background_prime_if_idle(&self) {
        if self.inner.shutdown.is_cancelled()
            || self
                .inner
                .background_warm_active
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            return;
        }
        inference_event("background_warm", "scheduled");
        let engine = self.clone();
        tokio::spawn(async move {
            loop {
                let requested = engine
                    .inner
                    .background_prime_requested
                    .load(Ordering::Acquire);
                if let Err(error) = engine.prime().await
                    && !engine.inner.shutdown.is_cancelled()
                {
                    tracing::debug!(error = %error, "background inference prime deferred");
                }
                engine
                    .inner
                    .background_prime_completed
                    .store(requested, Ordering::Release);
                if engine.inner.shutdown.is_cancelled()
                    || engine
                        .inner
                        .background_prime_requested
                        .load(Ordering::Acquire)
                        == requested
                {
                    break;
                }
            }
            engine
                .inner
                .background_warm_active
                .store(false, Ordering::Release);
            if !engine.inner.shutdown.is_cancelled()
                && engine
                    .inner
                    .background_prime_completed
                    .load(Ordering::Acquire)
                    < engine
                        .inner
                        .background_prime_requested
                        .load(Ordering::Acquire)
            {
                engine.start_background_prime_if_idle();
            }
        });
    }

    async fn active_profile(&self) -> Result<Arc<LocalModelRuntimeProfile>> {
        self.inner
            .profile
            .read()
            .await
            .clone()
            .ok_or_else(|| anyhow!("no active local-model runtime profile"))
    }

    fn claim_restart(&self) -> bool {
        !self.inner.restart_used.swap(true, Ordering::AcqRel)
    }

    fn mark_successful_completion(&self) {
        self.inner.restart_used.store(false, Ordering::Release);
    }

    async fn background_tick(&self) {
        let profile = self.inner.profile.read().await.clone();
        let Some(profile) = profile else {
            return;
        };
        let mut restart = false;
        let mut restart_exhausted = false;
        {
            let mut slot = self.inner.worker.lock().await;
            let status = slot.worker.as_mut().map(|worker| worker.child.try_wait());
            match status {
                Some(Ok(Some(status))) => {
                    let detail = slot
                        .worker
                        .as_ref()
                        .map(|worker| unexpected_worker_exit(worker, &status))
                        .unwrap_or_else(|| format!("llama-server exited unexpectedly: {status}"));
                    slot.worker = None;
                    slot.last_error = Some(detail);
                    transition_worker_state(&mut slot, LocalModelWorkerState::Unavailable);
                    inference_event("worker_exit", "unexpected");
                    restart = self.claim_restart();
                    restart_exhausted = !restart;
                }
                Some(Ok(None)) => {
                    let rss_bytes = match slot.worker.as_ref() {
                        Some(worker) => worker_rss_bytes(&worker.child).await,
                        None => None,
                    };
                    if slot.publish_runtime_metrics
                        && let Some(rss_bytes) = rss_bytes
                    {
                        slot.resident_rss_bytes = Some(rss_bytes);
                        ANIME_INFERENCE_WORKER_RSS_BYTES
                            .with_label_values(&[slot.metric_backend])
                            .set(i64::try_from(rss_bytes).unwrap_or(i64::MAX));
                    }
                    if slot.last_activity.elapsed()
                        >= Duration::from_secs(profile.idle_unload_seconds)
                    {
                        inference_event("worker_unload", "idle");
                        stop_worker(&mut slot).await;
                    }
                    refresh_worker_snapshot_cache(&slot);
                }
                Some(Err(error)) => {
                    slot.last_error = Some(format!("checking llama-server status: {error}"));
                    slot.worker = None;
                    transition_worker_state(&mut slot, LocalModelWorkerState::Unavailable);
                    inference_event("worker_status", "error");
                    restart = self.claim_restart();
                    restart_exhausted = !restart;
                }
                None => {}
            }
        }
        if restart {
            inference_event("worker_restart", "scheduled");
            self.schedule_background_warm();
        } else if restart_exhausted {
            inference_event("worker_restart", "exhausted");
        }
    }

    async fn ensure_ready(
        &self,
        profile: &LocalModelRuntimeProfile,
        slot: &mut WorkerSlot,
        admission_phase: LocalModelAdmissionPhase,
    ) -> Result<SocketAddr> {
        slot.metric_backend = metric_backend(&profile.backend);
        let stale = slot
            .worker
            .as_ref()
            .is_some_and(|worker| worker.profile_fingerprint != profile.profile_fingerprint);
        if stale {
            stop_worker(slot).await;
        }
        if let Err(error) = self.inner.admission.admit(admission_phase, profile) {
            stop_worker(slot).await;
            slot.last_error = Some(bounded_error(&error));
            transition_worker_state(slot, LocalModelWorkerState::Inactive);
            inference_event("resource_admission", "deferred");
            return Err(error).context("local model worker start deferred by resource admission");
        }
        if let Some(worker) = slot.worker.as_mut() {
            match worker.child.try_wait() {
                Ok(None) => {
                    let readiness = monitor_inference_admission(
                        self.inner.admission.as_ref(),
                        admission_phase,
                        profile,
                        &self.inner.shutdown,
                        wait_until_ready(&self.inner.http, worker, &self.inner.shutdown),
                    )
                    .await;
                    match readiness {
                        AdmissionMonitored::Completed(Ok(address)) => {
                            slot.last_activity = Instant::now();
                            let state = if slot.worker.as_ref().is_some_and(|worker| worker.primed)
                            {
                                LocalModelWorkerState::Ready
                            } else {
                                LocalModelWorkerState::Starting
                            };
                            transition_worker_state(slot, state);
                            return Ok(address);
                        }
                        AdmissionMonitored::Completed(Err(error)) => {
                            stop_worker(slot).await;
                            slot.last_error = Some(bounded_error(&error));
                            transition_worker_state(slot, LocalModelWorkerState::Unavailable);
                            return Err(error);
                        }
                        AdmissionMonitored::Rejected(error) => {
                            stop_worker(slot).await;
                            slot.last_error = Some(bounded_error(&error));
                            transition_worker_state(slot, LocalModelWorkerState::Inactive);
                            return Err(error)
                                .context("local model worker readiness admission was revoked");
                        }
                        AdmissionMonitored::Shutdown => {
                            stop_worker(slot).await;
                            transition_worker_state(slot, LocalModelWorkerState::Inactive);
                            bail!("local model engine is shutting down");
                        }
                    }
                }
                Ok(Some(status)) => {
                    slot.last_error = Some(unexpected_worker_exit(worker, &status));
                    slot.worker = None;
                    transition_worker_state(slot, LocalModelWorkerState::Unavailable);
                    inference_event("worker_exit", "unexpected");
                    bail!("llama-server exited unexpectedly: {status}");
                }
                Err(error) => {
                    slot.last_error = Some(format!("checking llama-server status: {error}"));
                    slot.worker = None;
                    transition_worker_state(slot, LocalModelWorkerState::Unavailable);
                    inference_event("worker_status", "error");
                    return Err(error).context("checking llama-server status");
                }
            }
        }

        if let Err(error) = self.inner.admission.admit(admission_phase, profile) {
            slot.last_error = Some(bounded_error(&error));
            transition_worker_state(slot, LocalModelWorkerState::Inactive);
            inference_event("resource_admission", "deferred");
            return Err(error).context("local model worker start deferred by resource admission");
        }
        transition_worker_state(slot, LocalModelWorkerState::Starting);
        let mut load_operation = MetricOperation::new("worker_load");
        let worker = match spawn_worker(profile).await {
            Ok(worker) => worker,
            Err(error) => {
                slot.last_error = Some(bounded_error(&error));
                transition_worker_state(slot, LocalModelWorkerState::Unavailable);
                return Err(error);
            }
        };
        slot.worker = Some(worker);
        refresh_worker_snapshot_cache(slot);
        let readiness = {
            let worker = slot
                .worker
                .as_mut()
                .ok_or_else(|| anyhow!("llama-server worker disappeared during startup"))?;
            monitor_inference_admission(
                self.inner.admission.as_ref(),
                admission_phase,
                profile,
                &self.inner.shutdown,
                wait_until_ready(&self.inner.http, worker, &self.inner.shutdown),
            )
            .await
        };
        match readiness {
            AdmissionMonitored::Completed(Ok(address)) => {
                slot.last_activity = Instant::now();
                slot.last_error = None;
                let state = if slot.worker.as_ref().is_some_and(|worker| worker.primed) {
                    LocalModelWorkerState::Ready
                } else {
                    LocalModelWorkerState::Starting
                };
                transition_worker_state(slot, state);
                load_operation.succeed();
                Ok(address)
            }
            AdmissionMonitored::Completed(Err(error)) => {
                stop_worker(slot).await;
                slot.last_error = Some(bounded_error(&error));
                transition_worker_state(slot, LocalModelWorkerState::Unavailable);
                Err(error)
            }
            AdmissionMonitored::Rejected(error) => {
                stop_worker(slot).await;
                slot.last_error = Some(bounded_error(&error));
                transition_worker_state(slot, LocalModelWorkerState::Inactive);
                Err(error).context("local model worker readiness admission was revoked")
            }
            AdmissionMonitored::Shutdown => {
                stop_worker(slot).await;
                transition_worker_state(slot, LocalModelWorkerState::Inactive);
                bail!("local model engine is shutting down");
            }
        }
    }

    async fn infer(&self, request: AnimeMatchRequest) -> Result<AnimeMatchEngineOutput> {
        self.infer_measured(request).await.map(|(output, _)| output)
    }

    async fn infer_measured(
        &self,
        request: AnimeMatchRequest,
    ) -> Result<(AnimeMatchEngineOutput, LocalModelCompletion)> {
        let (completion, runtime) = self
            .infer_completion_measured(LocalModelRequest::Match(request))
            .await?;
        let response = completion.match_response()?.clone();
        Ok((
            AnimeMatchEngineOutput {
                response,
                runtime: Some(runtime),
            },
            completion,
        ))
    }

    async fn infer_semantic(
        &self,
        request: AnimeSemanticEvidenceRequest,
    ) -> Result<AnimeSemanticEvidenceEngineOutput> {
        let (completion, runtime) = self
            .infer_completion_measured(LocalModelRequest::Semantic(request))
            .await?;
        Ok(AnimeSemanticEvidenceEngineOutput {
            response: completion.semantic_response()?.clone(),
            runtime: Some(runtime),
        })
    }

    async fn infer_completion_measured(
        &self,
        request: LocalModelRequest,
    ) -> Result<(LocalModelCompletion, AnimeMatchRuntimeProvenance)> {
        let final_deadline = tokio::time::Instant::now() + REQUEST_DEADLINE;
        let mut resuming_after_resource_preemption = false;
        loop {
            ensure!(
                !self.inner.shutdown.is_cancelled(),
                "local model engine is shut down"
            );
            let profile = self.active_profile().await?;
            self.wait_for_inference_admission(&profile, final_deadline)
                .await?;

            let (ready, crashed) = self.primed_worker_status(&profile).await?;
            if !ready {
                if crashed {
                    if self.claim_restart() {
                        inference_event("worker_restart", "scheduled");
                        self.schedule_background_warm();
                    } else {
                        inference_event("worker_restart", "exhausted");
                    }
                    bail!("local model worker is not primed; deterministic fallback required");
                }
                if self.inner.restart_used.load(Ordering::Acquire)
                    && !resuming_after_resource_preemption
                {
                    let remaining =
                        final_deadline.saturating_duration_since(tokio::time::Instant::now());
                    return self
                        .infer_completion_with_deadline(request, remaining)
                        .await;
                }
                match timeout_at(final_deadline, self.prime()).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) if resource_admission_deferred(&error) => continue,
                    Ok(Err(error)) => return Err(error),
                    Err(_) => bail!("local model request deadline exceeded"),
                }
            }

            let remaining = final_deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                bail!("local model request deadline exceeded");
            }
            match self
                .infer_completion_with_deadline(request.clone(), remaining)
                .await
            {
                Err(error) if resource_admission_deferred(&error) => {
                    resuming_after_resource_preemption = true;
                    continue;
                }
                result => return result,
            }
        }
    }

    async fn wait_for_inference_admission(
        &self,
        profile: &LocalModelRuntimeProfile,
        deadline: tokio::time::Instant,
    ) -> Result<()> {
        loop {
            if self
                .inner
                .admission
                .admit(LocalModelAdmissionPhase::Inference, profile)
                .is_ok()
            {
                return Ok(());
            }
            inference_event("resource_admission", "waiting");
            tokio::select! {
                _ = self.inner.shutdown.cancelled() => {
                    bail!("local model engine is shutting down");
                }
                _ = sleep(ADMISSION_POLL_INTERVAL) => {}
                _ = tokio::time::sleep_until(deadline) => {
                    bail!("local model request deadline exceeded while waiting for resources");
                }
            }
        }
    }

    async fn infer_measured_with_deadline(
        &self,
        request: AnimeMatchRequest,
        request_deadline: Duration,
    ) -> Result<(AnimeMatchEngineOutput, LocalModelCompletion)> {
        let (completion, runtime) = self
            .infer_completion_with_deadline(LocalModelRequest::Match(request), request_deadline)
            .await?;
        let response = completion.match_response()?.clone();
        Ok((
            AnimeMatchEngineOutput {
                response,
                runtime: Some(runtime),
            },
            completion,
        ))
    }

    async fn infer_completion_with_deadline(
        &self,
        request: LocalModelRequest,
        request_deadline: Duration,
    ) -> Result<(LocalModelCompletion, AnimeMatchRuntimeProvenance)> {
        let mut operation = MetricOperation::new("inference");
        ensure!(
            !self.inner.shutdown.is_cancelled(),
            "local model engine is shut down"
        );
        let initial_profile = self.active_profile().await?;
        self.inner
            .admission
            .admit(LocalModelAdmissionPhase::Inference, &initial_profile)
            .context("local model inference deferred by resource admission")?;
        let queue_started = tokio::time::Instant::now();
        let queue_deadline = queue_started + request_deadline;
        let _total = self
            .inner
            .total_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                inference_event("queue_admission", "rejected");
                anyhow!("local model queue is full")
            })?;
        let _execution = match self.inner.execution_slot.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(TryAcquireError::Closed) => bail!("local model engine is shut down"),
            Err(TryAcquireError::NoPermits) => {
                let _queued = QueueDepthGuard::new();
                timeout_at(
                    queue_deadline,
                    self.inner.execution_slot.clone().acquire_owned(),
                )
                .await
                .map_err(|_| anyhow!("local model queue deadline exceeded"))?
                .map_err(|_| anyhow!("local model engine is shut down"))?
            }
        };
        let profile = self.active_profile().await?;

        if let Err(error) = self
            .inner
            .admission
            .admit(LocalModelAdmissionPhase::Inference, &profile)
        {
            inference_event("resource_admission", "deferred");
            return Err(error).context("local model inference deferred by resource admission");
        }

        let (ready, crashed) = self.primed_worker_status(&profile).await?;
        if !ready {
            if crashed {
                if self.claim_restart() {
                    inference_event("worker_restart", "scheduled");
                    self.schedule_background_warm();
                } else {
                    inference_event("worker_restart", "exhausted");
                }
            } else if !self.inner.restart_used.load(Ordering::Acquire) {
                self.schedule_background_warm();
            } else {
                inference_event("worker_restart", "exhausted");
            }
            bail!("local model worker is not primed; deterministic fallback required");
        }

        let attempt = tokio::select! {
            _ = self.inner.shutdown.cancelled() => {
                return Err(anyhow!("local model engine is shutting down"));
            }
            attempt = timeout_at(
                queue_deadline,
                self.infer_once_with_admission_monitor(&profile, &request),
            ) => attempt,
        };
        match attempt {
            Ok(Ok(completion)) => {
                self.mark_successful_completion();
                operation.succeed();
                Ok((completion, profile.provenance()))
            }
            Ok(Err(error)) => {
                if error.is::<InFlightAdmissionRejected>() {
                    return Err(error)
                        .context("local model inference cancelled for playback priority");
                }
                if self.worker_exited().await {
                    if self.claim_restart() {
                        inference_event("worker_restart", "scheduled");
                        self.schedule_background_warm();
                    } else {
                        inference_event("worker_restart", "exhausted");
                    }
                }
                Err(error)
            }
            Err(_) => {
                // Cancelling the HTTP request is not a protocol-level
                // cancellation guarantee. Kill the worker before making a
                // replacement eligible so an over-deadline generation cannot
                // keep consuming playback resources.
                let mut slot = self.inner.worker.lock().await;
                abort_worker(&mut slot).await;
                drop(slot);
                if self.claim_restart() {
                    self.schedule_background_warm();
                } else {
                    inference_event("worker_restart", "exhausted");
                }
                Err(anyhow!("local model request deadline exceeded"))
            }
        }
    }

    async fn primed_worker_status(
        &self,
        profile: &LocalModelRuntimeProfile,
    ) -> Result<(bool, bool)> {
        let mut slot = self.inner.worker.lock().await;
        let Some(worker) = slot.worker.as_mut() else {
            return Ok((false, false));
        };
        if worker.profile_fingerprint != profile.profile_fingerprint {
            stop_worker(&mut slot).await;
            return Ok((false, false));
        }
        match worker.child.try_wait() {
            Ok(None) => Ok((worker.primed, false)),
            Ok(Some(status)) => {
                slot.last_error = Some(unexpected_worker_exit(worker, &status));
                slot.worker = None;
                transition_worker_state(&mut slot, LocalModelWorkerState::Unavailable);
                inference_event("worker_exit", "unexpected");
                Ok((false, true))
            }
            Err(error) => {
                slot.last_error = Some(format!("checking llama-server status: {error}"));
                slot.worker = None;
                transition_worker_state(&mut slot, LocalModelWorkerState::Unavailable);
                inference_event("worker_status", "error");
                Ok((false, true))
            }
        }
    }

    async fn infer_once_with_admission_monitor(
        &self,
        profile: &LocalModelRuntimeProfile,
        request: &LocalModelRequest,
    ) -> Result<LocalModelCompletion> {
        let outcome = monitor_inference_admission(
            self.inner.admission.as_ref(),
            LocalModelAdmissionPhase::Inference,
            profile,
            &self.inner.shutdown,
            self.infer_once(profile, request),
        )
        .await;
        match outcome {
            AdmissionMonitored::Completed(result) => result,
            AdmissionMonitored::Shutdown => Err(anyhow!("local model engine is shutting down")),
            AdmissionMonitored::Rejected(error) => {
                // `monitor_inference_admission` owns and has now dropped the
                // request future, releasing the worker mutex before teardown.
                let detail = bounded_error(&error);
                let mut slot = self.inner.worker.lock().await;
                inference_event("resource_admission", "revoked");
                inference_event("worker_unload", "playback_priority");
                stop_worker(&mut slot).await;
                Err(InFlightAdmissionRejected(detail).into())
            }
        }
    }

    async fn infer_once(
        &self,
        profile: &LocalModelRuntimeProfile,
        request: &LocalModelRequest,
    ) -> Result<LocalModelCompletion> {
        let mut slot = self.inner.worker.lock().await;
        let worker = slot
            .worker
            .as_mut()
            .ok_or_else(|| anyhow!("local model worker is unavailable"))?;
        ensure!(
            worker.profile_fingerprint == profile.profile_fingerprint && worker.primed,
            "local model worker is not primed"
        );
        ensure!(
            worker.child.try_wait()?.is_none(),
            "local model worker exited before inference"
        );
        let address = worker.address;
        let response = match request {
            LocalModelRequest::Match(request) => {
                resolve_direct_request(&self.inner.http, address, request, profile).await?
            }
            LocalModelRequest::Semantic(request) => {
                resolve_semantic_request(&self.inner.http, address, request, profile).await?
            }
        };
        let resident_rss_bytes = match slot.worker.as_ref() {
            Some(worker) => worker_rss_bytes(&worker.child).await,
            None => None,
        };
        slot.resident_rss_bytes = resident_rss_bytes;
        slot.last_activity = Instant::now();
        transition_worker_state(&mut slot, LocalModelWorkerState::Ready);
        Ok(response)
    }

    async fn worker_exited(&self) -> bool {
        let mut slot = self.inner.worker.lock().await;
        let Some(worker) = slot.worker.as_mut() else {
            return true;
        };
        match worker.child.try_wait() {
            Ok(Some(status)) => {
                let detail = unexpected_worker_exit(worker, &status);
                slot.worker = None;
                slot.last_error = Some(detail);
                transition_worker_state(&mut slot, LocalModelWorkerState::Unavailable);
                inference_event("worker_exit", "unexpected");
                true
            }
            Ok(None) => false,
            Err(error) => {
                slot.last_error = Some(format!("checking llama-server status: {error}"));
                slot.worker = None;
                transition_worker_state(&mut slot, LocalModelWorkerState::Unavailable);
                inference_event("worker_status", "error");
                true
            }
        }
    }
}

fn resource_admission_deferred(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause.downcast_ref::<InFlightAdmissionRejected>().is_some()
            || cause.to_string().contains("resource admission")
            || cause.to_string().contains("admission was revoked")
    })
}

#[async_trait]
impl AnimeMatchEngine for LocalModelEngine {
    async fn match_candidates(&self, request: AnimeMatchRequest) -> Result<AnimeMatchResponse> {
        Ok(self.infer(request).await?.response)
    }

    async fn match_candidates_with_provenance(
        &self,
        request: AnimeMatchRequest,
    ) -> Result<AnimeMatchEngineOutput> {
        self.infer(request).await
    }
}

#[async_trait]
impl AnimeSemanticEvidenceEngine for LocalModelEngine {
    async fn select_hypothesis(
        &self,
        request: AnimeSemanticEvidenceRequest,
    ) -> Result<AnimeSemanticEvidenceResponse> {
        Ok(self.infer_semantic(request).await?.response)
    }

    async fn select_hypothesis_with_provenance(
        &self,
        request: AnimeSemanticEvidenceRequest,
    ) -> Result<AnimeSemanticEvidenceEngineOutput> {
        self.infer_semantic(request).await
    }
}

fn build_chat_request(
    request: &AnimeMatchRequest,
    profile: &LocalModelRuntimeProfile,
) -> Result<Value> {
    ensure!(
        request.schema_version == ANIME_MATCH_SCHEMA_VERSION,
        "unsupported anime match request schema version {}",
        request.schema_version
    );
    ensure!(
        !request.target.wanted_target_keys.is_empty(),
        "anime match request has no wanted targets"
    );
    ensure!(
        !request.candidates.is_empty(),
        "anime match request has no candidates"
    );
    let response_bounds = compact_response_bounds(request, profile.max_output_tokens as usize)?;
    let request_json = serde_json::to_string(&compact_direct_request(request)?)
        .context("encoding direct anime match request")?;
    Ok(json!({
        "model": profile.model_id,
        "messages": [
            {"role": "system", "content": DIRECT_MATCH_PROMPT},
            {"role": "user", "content": request_json}
        ],
        "max_tokens": profile.max_output_tokens,
        "temperature": profile.sampling.temperature,
        "top_p": profile.sampling.top_p,
        "top_k": profile.sampling.top_k,
        "min_p": profile.sampling.min_p,
        "seed": profile.sampling.seed,
        "stream": false,
        "chat_template_kwargs": {"enable_thinking": false},
        "grammar": compact_response_grammar(request, response_bounds.maximum_mappings)?
    }))
}

fn build_semantic_chat_request(
    request: &AnimeSemanticEvidenceRequest,
    profile: &LocalModelRuntimeProfile,
) -> Result<Value> {
    ensure!(
        request.schema_version == super::ANIME_SEMANTIC_EVIDENCE_SCHEMA_VERSION,
        "unsupported semantic evidence request schema version {}",
        request.schema_version
    );
    ensure!(
        !request.entities.is_empty() && !request.hypotheses.is_empty(),
        "semantic evidence request has no selectable interpretation"
    );
    let request_json = serde_json::to_string(&json!({
        "raw": request.raw,
        "parentRelease": request.parent_release,
        "titleCandidates": request.title_candidates,
        "observedSeasonNumbers": request.observed_season_numbers,
        "entities": request.entities,
        "hypotheses": request.hypotheses,
    }))
    .context("encoding semantic evidence request")?;
    Ok(json!({
        "model": profile.model_id,
        "messages": [
            {"role": "system", "content": SEMANTIC_EVIDENCE_PROMPT},
            {"role": "user", "content": request_json}
        ],
        "max_tokens": profile.max_output_tokens,
        "temperature": profile.sampling.temperature,
        "top_p": profile.sampling.top_p,
        "top_k": profile.sampling.top_k,
        "min_p": profile.sampling.min_p,
        "seed": profile.sampling.seed,
        "stream": false,
        "chat_template_kwargs": {"enable_thinking": false},
        "grammar": semantic_response_grammar(request)?
    }))
}

fn semantic_response_grammar(request: &AnimeSemanticEvidenceRequest) -> Result<String> {
    ensure!(
        !request.hypotheses.is_empty(),
        "cannot build semantic response grammar without hypotheses"
    );
    let indexes = request
        .hypotheses
        .iter()
        .map(|hypothesis| format!("\"{}\"", hypothesis.index))
        .collect::<Vec<_>>()
        .join(" | ");
    Ok(format!(
        "root ::= \"{{\\\"schemaVersion\\\":1,\\\"hypothesisIndex\\\":\" choice \"}}\"\nchoice ::= \"null\" | {indexes}\n"
    ))
}

fn compact_direct_request(request: &AnimeMatchRequest) -> Result<Value> {
    let mut target = serde_json::Map::new();
    target.insert(
        "title".to_string(),
        Value::String(request.target.canonical_title.clone()),
    );
    target.insert(
        "scope".to_string(),
        serde_json::to_value(request.target.scope).context("encoding anime target scope")?,
    );
    if let Some(season) = request.target.season_number {
        target.insert("season".to_string(), json!(season));
    }
    if !request.target.episode_numbers.is_empty() {
        target.insert(
            "episodes".to_string(),
            json!(request.target.episode_numbers),
        );
    }
    if !request.target.absolute_episode_numbers.is_empty() {
        target.insert(
            "absolute".to_string(),
            json!(request.target.absolute_episode_numbers),
        );
    }
    let audio = &request.target.audio_preference;
    let mut compact_audio = serde_json::Map::new();
    compact_audio.insert(
        "mode".to_string(),
        serde_json::to_value(audio.mode).context("encoding anime audio mode")?,
    );
    insert_non_empty_strings(&mut compact_audio, "languages", &audio.languages);
    insert_non_empty_strings(&mut compact_audio, "subtitles", &audio.subtitle_languages);
    insert_non_empty_strings(&mut compact_audio, "accepted", &audio.accepted_profiles);
    target.insert("audio".to_string(), Value::Object(compact_audio));

    let wanted_slots = request
        .target
        .wanted_target_keys
        .iter()
        .enumerate()
        .map(|(slot, key)| (key.as_str(), slot))
        .collect::<BTreeMap<_, _>>();
    let seasons = request
        .context
        .seasons
        .iter()
        .filter_map(|season| {
            let episodes = compact_context_targets(season, &wanted_slots);
            let aliases = compact_season_aliases(season, &request.target.canonical_title);
            if episodes.is_empty() && aliases.is_empty() {
                return None;
            }
            let mut compact_season = serde_json::Map::new();
            compact_season.insert("season".to_string(), json!(season.season_number));
            insert_non_empty_strings(&mut compact_season, "aliases", &aliases);
            if !episodes.is_empty() {
                compact_season.insert("episodes".to_string(), Value::Array(episodes));
            }
            Some(Value::Object(compact_season))
        })
        .collect::<Vec<_>>();

    let candidates = request
        .candidates
        .iter()
        .enumerate()
        .map(|(candidate_index, candidate)| {
            let mut compact = serde_json::Map::new();
            compact.insert("index".to_string(), json!(candidate_index));
            compact.insert(
                "release".to_string(),
                Value::String(candidate.title.clone()),
            );
            if !candidate.files.is_empty() {
                compact.insert(
                    "files".to_string(),
                    Value::Array(
                        candidate
                            .files
                            .iter()
                            .enumerate()
                            .map(|(file_index, file)| {
                                json!({"index": file_index, "name": file.path})
                            })
                            .collect(),
                    ),
                );
            }
            Value::Object(compact)
        })
        .collect::<Vec<_>>();

    Ok(json!({
        "target": target,
        "seasons": seasons,
        "candidates": candidates,
    }))
}

fn compact_season_aliases(season: &AnimeMatchSeasonContext, canonical_title: &str) -> Vec<String> {
    let canonical_key = anime_match_alias_equivalence_key(canonical_title);
    let mut seen = BTreeSet::new();
    let mut aliases = Vec::new();
    for alias in &season.aliases {
        let value = alias.value.trim();
        if value.is_empty() {
            continue;
        }
        let key = anime_match_alias_equivalence_key(value);
        if key.is_empty() || key == canonical_key || !seen.insert(key) {
            continue;
        }
        aliases.push(value.to_string());
    }
    aliases
}

fn compact_context_targets(
    season: &AnimeMatchSeasonContext,
    wanted_slots: &BTreeMap<&str, usize>,
) -> Vec<Value> {
    let wanted_indices = season
        .targets
        .iter()
        .enumerate()
        .filter_map(|(index, target)| {
            wanted_slots
                .contains_key(target.target_key.as_str())
                .then_some(index)
        })
        .collect::<Vec<_>>();
    if wanted_indices.is_empty() {
        return Vec::new();
    }

    let mut included = wanted_indices.iter().copied().collect::<BTreeSet<_>>();
    for wanted_index in wanted_indices {
        let wanted = &season.targets[wanted_index];
        if wanted.episode_number.is_none() || wanted.absolute_episode_number.is_none() {
            if wanted_index > 0 {
                included.insert(wanted_index - 1);
            }
            if wanted_index + 1 < season.targets.len() {
                included.insert(wanted_index + 1);
            }
        }
        for (index, target) in season.targets.iter().enumerate() {
            let seasonal_collision = wanted
                .episode_number
                .zip(target.episode_number)
                .is_some_and(|(left, right)| left == right);
            let absolute_collision = wanted
                .absolute_episode_number
                .zip(target.absolute_episode_number)
                .is_some_and(|(left, right)| left == right);
            if index != wanted_index && (seasonal_collision || absolute_collision) {
                included.insert(index);
            }
        }
    }

    included
        .into_iter()
        .filter_map(|index| season.targets.get(index))
        .filter_map(|target| compact_context_target(target, wanted_slots))
        .collect()
}

fn compact_context_target(
    target: &AnimeMatchContextTarget,
    wanted_slots: &BTreeMap<&str, usize>,
) -> Option<Value> {
    let mut compact = serde_json::Map::new();
    if !target.title.is_empty() {
        compact.insert("title".to_string(), Value::String(target.title.clone()));
    }
    if let Some(number) = target.episode_number {
        compact.insert("episode".to_string(), json!(number));
    }
    if let Some(number) = target.absolute_episode_number {
        compact.insert("absolute".to_string(), json!(number));
    }
    if let Some(slot) = wanted_slots.get(target.target_key.as_str()) {
        compact.insert("wanted".to_string(), json!(slot));
    }
    (!compact.is_empty()).then_some(Value::Object(compact))
}

fn insert_non_empty_strings(
    object: &mut serde_json::Map<String, Value>,
    key: &str,
    values: &[String],
) {
    if !values.is_empty() {
        object.insert(
            key.to_string(),
            Value::Array(values.iter().cloned().map(Value::String).collect()),
        );
    }
}

fn compact_response_grammar(
    request: &AnimeMatchRequest,
    maximum_mappings: usize,
) -> Result<String> {
    ensure!(
        !request.target.wanted_target_keys.is_empty(),
        "cannot build compact response grammar without wanted targets"
    );
    ensure!(
        !request.candidates.is_empty(),
        "cannot build compact response grammar without candidates"
    );
    ensure!(
        (1..=request.candidates.len()).contains(&maximum_mappings),
        "compact response mapping bound is outside the candidate cardinality"
    );

    let mapping_names = (0..request.candidates.len())
        .map(|index| format!("mapping{index}"))
        .collect::<Vec<_>>();
    let decisions = vec!["decision"; request.candidates.len()].join(" \",\" ");
    let mut rules = vec![
        "root ::= \"{\\\"d\\\":[\" decisions \"],\\\"m\\\":[]}\" | \"{\\\"d\\\":[\" decisions \"],\\\"m\\\":[\" mappings \"]}\""
            .to_string(),
        format!("decisions ::= {decisions}"),
        "decision ::= \"0\" | \"1\" | \"2\"".to_string(),
    ];
    rules.push(format!(
        "mappings ::= {}",
        finite_sequence_choices("mapping", maximum_mappings)
    ));
    rules.push(format!("mapping ::= {}", mapping_names.join(" | ")));
    rules.push(format!(
        "target ::= {}",
        grammar_integer_choices(request.target.wanted_target_keys.len())
    ));
    rules.push(format!(
        "targets ::= {}",
        finite_sequence_choices("target", request.target.wanted_target_keys.len())
    ));

    for (candidate_index, candidate) in request.candidates.iter().enumerate() {
        let mapping = if candidate.files.is_empty() {
            format!("\"[{candidate_index},[\" targets \"],[]]\"")
        } else {
            rules.push(format!(
                "file{candidate_index} ::= {}",
                grammar_integer_choices(candidate.files.len())
            ));
            rules.push(format!(
                "files{candidate_index} ::= {}",
                finite_sequence_choices(&format!("file{candidate_index}"), candidate.files.len())
            ));
            format!("\"[{candidate_index},[\" targets \"],[\" files{candidate_index} \"]]\"")
        };
        rules.push(format!("mapping{candidate_index} ::= {mapping}"));
    }
    Ok(rules.join("\n") + "\n")
}

fn finite_sequence_choices(symbol: &str, maximum: usize) -> String {
    (1..=maximum)
        .map(|count| vec![symbol; count].join(" \",\" "))
        .collect::<Vec<_>>()
        .join(" | ")
}

fn grammar_integer_choices(count: usize) -> String {
    (0..count)
        .map(|index| format!("\"{index}\""))
        .collect::<Vec<_>>()
        .join(" | ")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CompactResponseBounds {
    maximum_mappings: usize,
    maximum_response_bytes: usize,
}

fn compact_response_bounds(
    request: &AnimeMatchRequest,
    output_token_cap: usize,
) -> Result<CompactResponseBounds> {
    ensure!(
        !request.target.wanted_target_keys.is_empty(),
        "cannot bound compact response without wanted targets"
    );
    ensure!(
        !request.candidates.is_empty(),
        "cannot bound compact response without candidates"
    );
    let longest_mapping_bytes = maximum_compact_mapping_bytes(request)?;
    let minimum_response_bytes =
        maximum_compact_response_bytes(request.candidates.len(), 1, longest_mapping_bytes)?;
    let mut bounds = None;
    for mapping_count in 1..=request.candidates.len() {
        let response_bytes = maximum_compact_response_bytes(
            request.candidates.len(),
            mapping_count,
            longest_mapping_bytes,
        )?;
        if response_bytes > output_token_cap {
            break;
        }
        bounds = Some(CompactResponseBounds {
            maximum_mappings: mapping_count,
            maximum_response_bytes: response_bytes,
        });
    }
    bounds.ok_or_else(|| {
        anyhow!(
            "one bounded anime match mapping can require {} ASCII bytes but the profile reserves only {output_token_cap} output tokens",
            minimum_response_bytes
        )
    })
}

fn maximum_compact_mapping_bytes(request: &AnimeMatchRequest) -> Result<usize> {
    let target_count = request.target.wanted_target_keys.len();
    ensure!(
        target_count > 0,
        "cannot bound response without wanted targets"
    );
    let target_list_bytes =
        maximum_index_sequence_bytes(target_count, (target_count - 1).to_string().len())?;
    let mut longest = 0usize;
    for (candidate_index, candidate) in request.candidates.iter().enumerate() {
        let file_list_bytes = if candidate.files.is_empty() {
            0
        } else {
            maximum_index_sequence_bytes(
                candidate.files.len(),
                (candidate.files.len() - 1).to_string().len(),
            )?
        };
        let mapping_bytes = 8usize
            .checked_add(candidate_index.to_string().len())
            .and_then(|value| value.checked_add(target_list_bytes))
            .and_then(|value| value.checked_add(file_list_bytes))
            .ok_or_else(|| anyhow!("compact response byte bound overflow"))?;
        longest = longest.max(mapping_bytes);
    }
    ensure!(longest > 0, "cannot bound response without candidates");
    Ok(longest)
}

fn maximum_compact_response_bytes(
    decision_count: usize,
    mapping_count: usize,
    mapping_bytes: usize,
) -> Result<usize> {
    let decisions = maximum_index_sequence_bytes(decision_count, 1)?;
    maximum_index_sequence_bytes(mapping_count, mapping_bytes)?
        .checked_add(decisions)
        .and_then(|value| value.checked_add(15))
        .ok_or_else(|| anyhow!("compact response byte bound overflow"))
}

fn maximum_index_sequence_bytes(count: usize, element_width: usize) -> Result<usize> {
    ensure!(count > 0, "cannot bound an empty compact sequence");
    count
        .checked_mul(element_width)
        .and_then(|value| value.checked_add(count - 1))
        .ok_or_else(|| anyhow!("compact response byte bound overflow"))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InputTokenResponse {
    object: String,
    input_tokens: u32,
}

async fn count_input_tokens(client: &Client, address: SocketAddr, body: &Value) -> Result<u32> {
    let mut operation = MetricOperation::new("input_tokens");
    let url = loopback_url(address, "/v1/chat/completions/input_tokens")?;
    let response = client
        .post(url)
        .json(body)
        .send()
        .await
        .context("calling llama-server input-token endpoint")?;
    let bytes = bounded_response_bytes(response, "input-token").await?;
    let parsed: InputTokenResponse =
        serde_json::from_slice(&bytes).context("decoding llama-server input-token response")?;
    ensure!(
        parsed.object == "response.input_tokens",
        "unexpected llama-server input-token response object"
    );
    operation.succeed();
    Ok(parsed.input_tokens)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TokenizationResponse {
    tokens: Vec<i64>,
}

async fn request_tokenization(
    client: &Client,
    address: SocketAddr,
    content: &str,
) -> Result<Vec<i64>> {
    let url = loopback_url(address, "/tokenize")?;
    let response = client
        .post(url)
        .json(&json!({
            "content": content,
            "add_special": false,
            "parse_special": true,
            "with_pieces": false,
        }))
        .send()
        .await
        .context("calling llama-server tokenizer endpoint")?;
    let bytes = bounded_response_bytes(response, "tokenizer").await?;
    let parsed: TokenizationResponse =
        serde_json::from_slice(&bytes).context("decoding llama-server tokenizer response")?;
    ensure!(
        parsed.tokens.len() <= V1_CONTEXT_TOKENS as usize,
        "llama-server tokenizer response exceeds the context bound"
    );
    Ok(parsed.tokens)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AppliedTemplateResponse {
    prompt: String,
}

async fn request_applied_template(
    client: &Client,
    address: SocketAddr,
    model_id: &str,
    messages: &Value,
) -> Result<String> {
    let url = loopback_url(address, "/apply-template")?;
    let response = client
        .post(url)
        .json(&json!({
            "model": model_id,
            "messages": messages,
            "add_generation_prompt": true,
            "chat_template_kwargs": {"enable_thinking": false},
        }))
        .send()
        .await
        .context("calling llama-server chat-template endpoint")?;
    let bytes = bounded_response_bytes(response, "chat-template").await?;
    let parsed: AppliedTemplateResponse =
        serde_json::from_slice(&bytes).context("decoding llama-server chat-template response")?;
    ensure!(
        !parsed.prompt.is_empty() && parsed.prompt.len() <= MAX_HTTP_RESPONSE_BYTES,
        "llama-server returned an empty or oversized chat template"
    );
    Ok(parsed.prompt)
}

fn enforce_context_gate(input_tokens: u32, profile: &LocalModelRuntimeProfile) -> Result<()> {
    let total = input_tokens
        .checked_add(profile.max_output_tokens)
        .ok_or_else(|| anyhow!("local model context token count overflow"))?;
    ensure!(
        total <= profile.context_tokens,
        "templated request exceeds local model context ({} + {} > {})",
        input_tokens,
        profile.max_output_tokens,
        profile.context_tokens
    );
    Ok(())
}

#[derive(Debug, Deserialize)]
struct ChatCompletionResponse {
    choices: Vec<ChatCompletionChoice>,
    #[serde(default)]
    usage: Option<ChatCompletionUsage>,
    #[serde(default)]
    timings: Option<ChatCompletionTimings>,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionUsage {
    prompt_tokens: u64,
    completion_tokens: u64,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionTimings {
    prompt_ms: f64,
    predicted_ms: f64,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionChoice {
    message: ChatCompletionMessage,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionMessage {
    content: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CompactAnimeMatchResponse {
    d: Vec<u8>,
    m: Vec<CompactCandidateMapping>,
}

#[derive(Debug, Deserialize)]
struct CompactCandidateMapping(usize, Vec<usize>, Vec<usize>);

async fn resolve_direct_request(
    client: &Client,
    address: SocketAddr,
    request: &AnimeMatchRequest,
    profile: &LocalModelRuntimeProfile,
) -> Result<LocalModelCompletion> {
    let body = build_chat_request(request, profile)?;
    let input_tokens = count_input_tokens(client, address, &body).await?;
    enforce_context_gate(input_tokens, profile)?;
    request_direct_completion(client, address, &body, request, profile.max_output_tokens).await
}

async fn resolve_semantic_request(
    client: &Client,
    address: SocketAddr,
    request: &AnimeSemanticEvidenceRequest,
    profile: &LocalModelRuntimeProfile,
) -> Result<LocalModelCompletion> {
    let body = build_semantic_chat_request(request, profile)?;
    let input_tokens = count_input_tokens(client, address, &body).await?;
    enforce_context_gate(input_tokens, profile)?;
    request_semantic_completion(client, address, &body, request).await
}

async fn request_semantic_completion(
    client: &Client,
    address: SocketAddr,
    body: &Value,
    request: &AnimeSemanticEvidenceRequest,
) -> Result<LocalModelCompletion> {
    let mut operation = MetricOperation::new("semantic_chat_completion");
    let url = loopback_url(address, "/v1/chat/completions")?;
    let response = client
        .post(url)
        .json(body)
        .send()
        .await
        .context("calling llama-server semantic evidence endpoint")?;
    let bytes = bounded_response_bytes(response, "semantic-chat-completion").await?;
    let mut envelope: ChatCompletionResponse =
        serde_json::from_slice(&bytes).context("decoding llama-server semantic chat response")?;
    ensure!(
        envelope.choices.len() == 1,
        "llama-server returned {} semantic choices instead of one",
        envelope.choices.len()
    );
    let content = envelope.choices.remove(0).message.content;
    ensure!(
        !content.trim().is_empty(),
        "llama-server returned empty semantic model content"
    );
    let response: AnimeSemanticEvidenceResponse = serde_json::from_str(content.trim())
        .context("decoding grammar-constrained semantic evidence response")?;
    super::validate_semantic_evidence_response(request, &response)?;
    operation.succeed();
    Ok(LocalModelCompletion {
        response: LocalModelResponse::Semantic(response),
        prompt_tokens: envelope.usage.as_ref().map(|usage| usage.prompt_tokens),
        generated_tokens: envelope.usage.as_ref().map(|usage| usage.completion_tokens),
        prompt_time_ms: envelope
            .timings
            .as_ref()
            .and_then(|timings| finite_positive_millis(timings.prompt_ms)),
        generation_time_ms: envelope
            .timings
            .as_ref()
            .and_then(|timings| finite_positive_millis(timings.predicted_ms)),
    })
}

async fn request_direct_completion(
    client: &Client,
    address: SocketAddr,
    body: &Value,
    request: &AnimeMatchRequest,
    output_token_cap: u32,
) -> Result<LocalModelCompletion> {
    let mut operation = MetricOperation::new("chat_completion");
    let url = loopback_url(address, "/v1/chat/completions")?;
    let response = client
        .post(url)
        .json(body)
        .send()
        .await
        .context("calling llama-server chat completion endpoint")?;
    let bytes = bounded_response_bytes(response, "chat-completion").await?;
    let mut envelope: ChatCompletionResponse =
        serde_json::from_slice(&bytes).context("decoding llama-server chat response")?;
    ensure!(
        envelope.choices.len() == 1,
        "llama-server returned {} choices instead of one",
        envelope.choices.len()
    );
    let content = envelope.choices.remove(0).message.content;
    ensure!(
        !content.trim().is_empty(),
        "llama-server returned empty model content"
    );
    let response = decode_compact_response(content.trim(), request, output_token_cap)?;
    operation.succeed();
    Ok(LocalModelCompletion {
        response: LocalModelResponse::Match(response),
        prompt_tokens: envelope.usage.as_ref().map(|usage| usage.prompt_tokens),
        generated_tokens: envelope.usage.as_ref().map(|usage| usage.completion_tokens),
        prompt_time_ms: envelope
            .timings
            .as_ref()
            .and_then(|timings| finite_positive_millis(timings.prompt_ms)),
        generation_time_ms: envelope
            .timings
            .as_ref()
            .and_then(|timings| finite_positive_millis(timings.predicted_ms)),
    })
}

fn decode_compact_response(
    content: &str,
    request: &AnimeMatchRequest,
    output_token_cap: u32,
) -> Result<AnimeMatchResponse> {
    let compact: CompactAnimeMatchResponse = serde_json::from_str(content)
        .context("decoding grammar-constrained compact anime match response")?;
    let response_bounds = compact_response_bounds(request, output_token_cap as usize)?;
    ensure!(
        compact.d.len() == request.candidates.len(),
        "compact response decision cardinality differs from candidate cardinality"
    );
    ensure!(
        compact.d.iter().all(|decision| *decision <= 2),
        "compact response contains an unknown candidate decision"
    );
    ensure!(
        compact.m.len() <= response_bounds.maximum_mappings,
        "compact response exceeds grammar mapping cardinality"
    );
    let exact_candidates = compact
        .d
        .iter()
        .enumerate()
        .filter_map(|(candidate_slot, decision)| (*decision == 2).then_some(candidate_slot))
        .collect::<BTreeSet<_>>();
    let mut matches = Vec::with_capacity(compact.m.len());
    let mut seen_candidates = BTreeSet::new();
    for CompactCandidateMapping(candidate_slot, target_slots, file_slots) in compact.m {
        ensure!(
            seen_candidates.insert(candidate_slot),
            "compact response repeats candidate slot {candidate_slot}"
        );
        ensure!(
            exact_candidates.contains(&candidate_slot),
            "compact response maps candidate slot {candidate_slot} without an exact decision"
        );
        ensure!(
            !target_slots.is_empty(),
            "compact candidate slot {candidate_slot} accepted without targets"
        );
        let candidate = request
            .candidates
            .get(candidate_slot)
            .ok_or_else(|| anyhow!("compact response candidate slot is out of bounds"))?;
        ensure!(
            target_slots.len() <= request.target.wanted_target_keys.len(),
            "compact candidate slot {candidate_slot} exceeds target cardinality"
        );
        ensure!(
            file_slots.len() <= candidate.files.len(),
            "compact candidate slot {candidate_slot} exceeds file cardinality"
        );
        ensure!(
            candidate.files.is_empty() || !file_slots.is_empty(),
            "compact candidate slot {candidate_slot} accepted an inventoried candidate without selecting files"
        );
        ensure!(
            !candidate.files.is_empty() || file_slots.is_empty(),
            "compact candidate slot {candidate_slot} selected files for a fileless candidate"
        );

        let mut seen_targets = BTreeSet::new();
        let matched_target_keys = target_slots
            .into_iter()
            .map(|target_slot| {
                ensure!(
                    seen_targets.insert(target_slot),
                    "compact candidate slot {candidate_slot} repeats target slot {target_slot}"
                );
                request
                    .target
                    .wanted_target_keys
                    .get(target_slot)
                    .cloned()
                    .ok_or_else(|| {
                        anyhow!(
                            "compact candidate slot {candidate_slot} references unknown target slot {target_slot}"
                        )
                    })
            })
            .collect::<Result<Vec<_>>>()?;

        let mut seen_files = BTreeSet::new();
        let selected_file_keys = file_slots
            .into_iter()
            .map(|file_slot| {
                ensure!(
                    seen_files.insert(file_slot),
                    "compact candidate slot {candidate_slot} repeats local file slot {file_slot}"
                );
                candidate
                    .files
                    .get(file_slot)
                    .map(|file| file.file_key.clone())
                    .ok_or_else(|| {
                        anyhow!(
                            "compact candidate slot {candidate_slot} references unknown local file slot {file_slot}"
                        )
                    })
            })
            .collect::<Result<Vec<_>>>()?;

        matches.push(AnimeCandidateMatch {
            candidate_key: candidate.candidate_key.clone(),
            matched_target_keys,
            audio_profile: candidate_audio_profile(&candidate.parse_facts),
            selected_file_keys: (!selected_file_keys.is_empty()).then_some(selected_file_keys),
        });
    }
    ensure!(
        seen_candidates == exact_candidates,
        "compact response decisions and mappings disagree"
    );

    Ok(AnimeMatchResponse {
        schema_version: ANIME_MATCH_SCHEMA_VERSION,
        matches,
    })
}

fn candidate_audio_profile(facts: &super::AnimeMatchParseFacts) -> AnimeMatchAudioProfile {
    for (evidence, profile) in [
        ("dual_audio", AnimeMatchAudioProfile::DualAudio),
        ("dubbed", AnimeMatchAudioProfile::Dubbed),
        ("en_audio", AnimeMatchAudioProfile::EnAudio),
        ("ja_audio_en_subs", AnimeMatchAudioProfile::JaAudioEnSubs),
        ("subbed", AnimeMatchAudioProfile::Subbed),
    ] {
        if facts
            .audio_profiles
            .iter()
            .any(|value| value.trim().eq_ignore_ascii_case(evidence))
        {
            return profile;
        }
    }
    AnimeMatchAudioProfile::Unknown
}

fn finite_positive_millis(value: f64) -> Option<u64> {
    (value.is_finite() && value > 0.0).then(|| value.ceil().min(u64::MAX as f64) as u64)
}

async fn bounded_response_bytes(response: reqwest::Response, label: &str) -> Result<Vec<u8>> {
    let status = response.status();
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .ok_or_else(|| anyhow!("llama-server {label} response omitted Content-Type"))?
        .to_str()
        .with_context(|| format!("reading llama-server {label} Content-Type"))?;
    ensure!(
        is_json_content_type(content_type),
        "llama-server {label} returned non-JSON Content-Type '{content_type}'"
    );
    if let Some(length) = response.content_length() {
        ensure!(
            length <= MAX_HTTP_RESPONSE_BYTES as u64,
            "llama-server {label} response is too large"
        );
    }
    let mut response = response;
    let mut bytes = Vec::with_capacity(
        response
            .content_length()
            .and_then(|length| usize::try_from(length).ok())
            .unwrap_or(0)
            .min(MAX_HTTP_RESPONSE_BYTES),
    );
    while let Some(chunk) = response
        .chunk()
        .await
        .with_context(|| format!("reading llama-server {label} response"))?
    {
        ensure!(
            bytes.len().saturating_add(chunk.len()) <= MAX_HTTP_RESPONSE_BYTES,
            "llama-server {label} response is too large"
        );
        bytes.extend_from_slice(&chunk);
    }
    if !status.is_success() {
        let excerpt = String::from_utf8_lossy(&bytes);
        bail!(
            "llama-server {label} returned {status}: {}",
            excerpt.chars().take(512).collect::<String>()
        );
    }
    Ok(bytes)
}

fn is_json_content_type(value: &str) -> bool {
    let media_type = value
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    media_type == "application/json" || media_type.ends_with("+json")
}

fn loopback_url(address: SocketAddr, path: &str) -> Result<reqwest::Url> {
    ensure!(
        address.ip().is_loopback(),
        "refusing a non-loopback llama-server address"
    );
    ensure!(path.starts_with('/'), "llama-server path must be absolute");
    reqwest::Url::parse(&format!("http://{address}{path}"))
        .context("building loopback llama-server URL")
}

async fn spawn_worker(profile: &LocalModelRuntimeProfile) -> Result<ManagedWorker> {
    profile.validate_contract()?;
    profile.validate_installed_files().await?;
    let port = reserve_loopback_port()?;
    let address = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port));
    let args = worker_args(profile, port);
    let mut command = Command::new(&profile.worker_path);
    command
        .args(&args)
        .envs(worker_backend_environment(
            std::env::consts::OS,
            std::env::consts::ARCH,
            &profile.backend,
        ))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    {
        command.process_group(0);
    }
    #[cfg(windows)]
    {
        // A console-subsystem worker must remain invisible when Elixir is
        // launched from the tray/GUI. The Job Object below still owns its
        // lifetime and descendants.
        command.creation_flags(windows_sys::Win32::System::Threading::CREATE_NO_WINDOW);
    }
    let mut child = command
        .spawn()
        .with_context(|| format!("starting llama-server at {}", profile.worker_path.display()))?;
    #[cfg(unix)]
    let process_group_id = i32::try_from(
        child
            .id()
            .ok_or_else(|| anyhow!("llama-server process id is unavailable after spawn"))?,
    )
    .map_err(|_| anyhow!("llama-server process id is outside the platform range"))?;
    let isolation = ProcessIsolation::attach(&child)?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow!("llama-server stderr pipe is unavailable after spawn"))?;
    let diagnostic_tail = Arc::new(WorkerDiagnosticTail::default());
    let diagnostic_task = tokio::spawn(drain_worker_stderr(stderr, diagnostic_tail.clone()));
    let mut worker = ManagedWorker {
        _isolation: isolation,
        child,
        address,
        profile_fingerprint: profile.profile_fingerprint.clone(),
        primed: false,
        diagnostic_tail,
        diagnostic_task,
        #[cfg(unix)]
        process_group_id,
        #[cfg(unix)]
        kill_group_on_drop: true,
    };
    if let Err(error) = set_low_process_priority(&worker.child) {
        let _ = stop_managed_worker(&mut worker).await;
        return Err(error).context("lowering llama-server process priority");
    }
    Ok(worker)
}

fn worker_backend_environment(
    os: &str,
    arch: &str,
    backend: &str,
) -> Vec<(&'static str, &'static str)> {
    if os == "macos" && arch == "x86_64" && backend == "cpu" {
        // The Intel release is compiled without Metal. This additionally
        // keeps backend registration at zero Metal devices if incorrect
        // worker bytes ever reach the spawn boundary.
        vec![("GGML_METAL_DEVICES", "0")]
    } else {
        Vec::new()
    }
}

async fn drain_worker_stderr(
    mut stderr: tokio::process::ChildStderr,
    tail: Arc<WorkerDiagnosticTail>,
) {
    let mut buffer = [0_u8; 1_024];
    loop {
        match stderr.read(&mut buffer).await {
            Ok(0) | Err(_) => break,
            Ok(read) => tail.push(&buffer[..read]),
        }
    }
}

fn reserve_loopback_port() -> Result<u16> {
    let listener = StdTcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
        .context("reserving a loopback port for llama-server")?;
    let port = listener
        .local_addr()
        .context("reading reserved llama-server loopback port")?
        .port();
    drop(listener);
    Ok(port)
}

fn worker_args(profile: &LocalModelRuntimeProfile, port: u16) -> Vec<OsString> {
    let device = InferenceBackend::parse(&profile.backend)
        .map(InferenceBackend::llama_device_selector)
        // Invalid profiles are rejected before spawn; retaining `none` here
        // keeps this pure argument builder conservative in tests and tooling.
        .unwrap_or("none");
    let mut args = vec![
        OsString::from("--host"),
        OsString::from(LOOPBACK_HOST),
        OsString::from("--port"),
        OsString::from(port.to_string()),
        OsString::from("--model"),
        profile.model_path.as_os_str().to_owned(),
        OsString::from("--device"),
        OsString::from(device),
        OsString::from("--ctx-size"),
        OsString::from(V1_CONTEXT_TOKENS.to_string()),
        OsString::from("--parallel"),
        OsString::from(V1_PARALLEL.to_string()),
        OsString::from("--threads"),
        OsString::from(profile.threads.to_string()),
        OsString::from("--threads-batch"),
        OsString::from(profile.batch_threads.to_string()),
        OsString::from("--n-gpu-layers"),
        OsString::from(profile.gpu_layers.to_string()),
        OsString::from("--cache-type-k"),
        OsString::from(profile.kv_cache_type.as_str()),
        OsString::from("--cache-type-v"),
        OsString::from(profile.kv_cache_type.as_str()),
        // Elixir has already measured and selected an exact hardware profile.
        // Do not let a later llama.cpp heuristic silently change that profile.
        OsString::from("--fit"),
        OsString::from("off"),
    ];
    // llama.cpp requires Flash Attention when the V cache is quantized. The
    // bundle contract intentionally uses the same type for K and V, so a
    // q8_0 profile must make that runtime dependency explicit instead of
    // relying on backend-specific auto detection (which disables it during
    // partial Metal offload on Intel Macs).
    if profile.kv_cache_type == "q8_0" {
        args.extend([OsString::from("--flash-attn"), OsString::from("on")]);
    }
    args
}

async fn wait_until_ready(
    client: &Client,
    worker: &mut ManagedWorker,
    shutdown: &CancellationToken,
) -> Result<SocketAddr> {
    let url = loopback_url(worker.address, "/health")?;
    loop {
        if let Some(status) = worker
            .child
            .try_wait()
            .context("checking llama-server readiness process")?
        {
            let diagnostic = worker
                .diagnostic_tail
                .excerpt()
                .map(|tail| format!("; stderr: {tail}"))
                .unwrap_or_default();
            bail!("llama-server exited before readiness: {status}{diagnostic}");
        }
        match client.get(url.clone()).send().await {
            Ok(response) if response.status() == StatusCode::OK => return Ok(worker.address),
            Ok(response) if response.status().is_server_error() => {}
            Ok(response) => bail!("llama-server health returned {}", response.status()),
            Err(_) => {}
        }
        tokio::select! {
            _ = shutdown.cancelled() => bail!("local model engine is shutting down"),
            _ = sleep(READINESS_POLL_INTERVAL) => {}
        }
    }
}

async fn stop_worker(slot: &mut WorkerSlot) {
    if let Some(mut worker) = slot.worker.take() {
        match stop_managed_worker(&mut worker).await {
            Ok(()) => inference_event("worker_stop", "success"),
            Err(error) => {
                inference_event("worker_stop", "error");
                slot.last_error = Some(bounded_error(&error));
            }
        }
    }
    if slot.publish_runtime_metrics {
        ANIME_INFERENCE_WORKER_RSS_BYTES
            .with_label_values(&[slot.metric_backend])
            .set(0);
    }
    slot.resident_rss_bytes = None;
    transition_worker_state(slot, LocalModelWorkerState::Inactive);
}

/// Immediately terminate an over-deadline worker. A graceful half-second
/// shutdown is appropriate for idle unload and server shutdown, but it would
/// delay playback-priority fallback after the inference budget is exhausted.
async fn abort_worker(slot: &mut WorkerSlot) {
    if let Some(mut worker) = slot.worker.take() {
        match force_terminate_managed_worker(&mut worker).await {
            Ok(()) => inference_event("worker_abort", "success"),
            Err(error) => {
                inference_event("worker_abort", "error");
                slot.last_error = Some(bounded_error(&error));
            }
        }
    }
    if slot.publish_runtime_metrics {
        ANIME_INFERENCE_WORKER_RSS_BYTES
            .with_label_values(&[slot.metric_backend])
            .set(0);
    }
    slot.resident_rss_bytes = None;
    transition_worker_state(slot, LocalModelWorkerState::Inactive);
}

async fn force_terminate_managed_worker(worker: &mut ManagedWorker) -> Result<()> {
    if worker.child.try_wait()?.is_some() {
        #[cfg(unix)]
        {
            let _ = unsafe { libc::kill(-worker.process_group_id, libc::SIGKILL) };
        }
        return Ok(());
    }

    #[cfg(unix)]
    {
        let _ = unsafe { libc::kill(-worker.process_group_id, libc::SIGKILL) };
        worker.child.wait().await?;
        return Ok(());
    }

    #[cfg(windows)]
    {
        worker.child.start_kill()?;
        worker.child.wait().await?;
        return Ok(());
    }

    #[cfg(not(any(unix, windows)))]
    {
        worker.child.start_kill()?;
        worker.child.wait().await?;
        Ok(())
    }
}

async fn stop_managed_worker(worker: &mut ManagedWorker) -> Result<()> {
    let already_exited = worker.child.try_wait()?.is_some();
    #[cfg(unix)]
    {
        let process_group = worker.process_group_id;
        if already_exited {
            // The process leader can exit while leaving descendants behind.
            // Kill the whole group before disarming the drop guard.
            let _ = unsafe { libc::kill(-process_group, libc::SIGKILL) };
            worker.kill_group_on_drop = false;
            return Ok(());
        }
        let _ = unsafe { libc::kill(-process_group, libc::SIGTERM) };
        if matches!(
            tokio::time::timeout(PROCESS_STOP_GRACE, worker.child.wait()).await,
            Ok(Ok(_))
        ) {
            // Do not assume every descendant honored SIGTERM just because the
            // group leader exited.
            let _ = unsafe { libc::kill(-process_group, libc::SIGKILL) };
            worker.kill_group_on_drop = false;
            return Ok(());
        }
        let _ = unsafe { libc::kill(-process_group, libc::SIGKILL) };
        let wait_result = worker.child.wait().await;
        worker.kill_group_on_drop = false;
        wait_result.context("waiting for terminated llama-server")?;
        return Ok(());
    }
    #[cfg(not(unix))]
    {
        if already_exited {
            return Ok(());
        }
        worker
            .child
            .kill()
            .await
            .context("terminating llama-server")?;
        let _ = worker.child.wait().await;
        Ok(())
    }
}

const METRIC_BACKENDS: [&str; 7] = ["none", "cpu", "metal", "cuda", "hip", "vulkan", "other"];
const METRIC_STATES: [&str; 5] = [
    "inactive",
    "starting",
    "ready",
    "unavailable",
    "shutting_down",
];

fn metric_backend(backend: &str) -> &'static str {
    match backend {
        "cpu" => "cpu",
        "metal" => "metal",
        "cuda" => "cuda",
        "hip" => "hip",
        "vulkan" => "vulkan",
        _ => "other",
    }
}

fn metric_worker_state(state: LocalModelWorkerState) -> &'static str {
    match state {
        LocalModelWorkerState::Inactive => "inactive",
        LocalModelWorkerState::Starting => "starting",
        LocalModelWorkerState::Ready => "ready",
        LocalModelWorkerState::Unavailable => "unavailable",
        LocalModelWorkerState::ShuttingDown => "shutting_down",
    }
}

fn set_runtime_state_metric(state: LocalModelWorkerState, backend: &'static str) {
    for known_state in METRIC_STATES {
        for known_backend in METRIC_BACKENDS {
            ANIME_INFERENCE_RUNTIME_STATE
                .with_label_values(&[known_state, known_backend])
                .set(i64::from(
                    known_state == metric_worker_state(state) && known_backend == backend,
                ));
        }
    }
}

fn transition_worker_state(slot: &mut WorkerSlot, state: LocalModelWorkerState) {
    slot.state = state;
    refresh_worker_snapshot_cache(slot);
    if slot.publish_runtime_metrics {
        set_runtime_state_metric(state, slot.metric_backend);
    }
}

fn refresh_worker_snapshot_cache(slot: &WorkerSlot) {
    let mut snapshot = slot
        .snapshot_cache
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    snapshot.state = slot.state;
    snapshot.process_id = slot.worker.as_ref().and_then(|worker| worker.child.id());
    snapshot.loopback_port = slot.worker.as_ref().map(|worker| worker.address.port());
    snapshot.resident_rss_bytes = slot.resident_rss_bytes;
    snapshot.last_error = slot.last_error.clone();
}

fn update_snapshot_profile(
    cache: &StdRwLock<LocalModelSnapshot>,
    profile: Option<&LocalModelRuntimeProfile>,
) {
    let mut snapshot = cache
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    snapshot.profile_fingerprint = profile.map(|profile| profile.profile_fingerprint.clone());
    snapshot.backend = profile.map(|profile| profile.backend.clone());
}

fn inference_event(event: &'static str, result: &'static str) {
    ANIME_INFERENCE_EVENTS
        .with_label_values(&[event, result])
        .inc();
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

struct MetricOperation {
    operation: &'static str,
    result: &'static str,
    started: Instant,
}

impl MetricOperation {
    fn new(operation: &'static str) -> Self {
        Self {
            operation,
            result: "error",
            started: Instant::now(),
        }
    }

    fn succeed(&mut self) {
        self.result = "success";
    }
}

impl Drop for MetricOperation {
    fn drop(&mut self) {
        inference_event(self.operation, self.result);
        ANIME_INFERENCE_OPERATION_DURATION
            .with_label_values(&[self.operation, self.result])
            .observe(self.started.elapsed().as_secs_f64());
    }
}

struct QueueDepthGuard;

impl QueueDepthGuard {
    fn new() -> Self {
        ANIME_INFERENCE_QUEUE_DEPTH.inc();
        Self
    }
}

impl Drop for QueueDepthGuard {
    fn drop(&mut self) {
        ANIME_INFERENCE_QUEUE_DEPTH.dec();
    }
}

#[cfg(target_os = "linux")]
async fn worker_rss_bytes(child: &Child) -> Option<u64> {
    let process_id = child.id()?;
    let statm = tokio::fs::read_to_string(format!("/proc/{process_id}/statm"))
        .await
        .ok()?;
    let resident_pages = statm.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    (page_size > 0).then_some(resident_pages.saturating_mul(u64::try_from(page_size).ok()?))
}

#[cfg(target_os = "macos")]
async fn worker_rss_bytes(child: &Child) -> Option<u64> {
    use std::{ffi::c_void, mem};

    #[repr(C)]
    struct ProcTaskInfo {
        virtual_size: u64,
        resident_size: u64,
        total_user: u64,
        total_system: u64,
        threads_user: u64,
        threads_system: u64,
        policy: i32,
        faults: i32,
        pageins: i32,
        cow_faults: i32,
        messages_sent: i32,
        messages_received: i32,
        syscalls_mach: i32,
        syscalls_unix: i32,
        context_switches: i32,
        thread_count: i32,
        running_thread_count: i32,
        priority: i32,
    }

    #[link(name = "proc")]
    unsafe extern "C" {
        fn proc_pidinfo(
            pid: i32,
            flavor: i32,
            arg: u64,
            buffer: *mut c_void,
            buffer_size: i32,
        ) -> i32;
    }

    const PROC_PIDTASKINFO: i32 = 4;
    let process_id = i32::try_from(child.id()?).ok()?;
    let mut info = mem::MaybeUninit::<ProcTaskInfo>::zeroed();
    let size = i32::try_from(mem::size_of::<ProcTaskInfo>()).ok()?;
    let read = unsafe {
        proc_pidinfo(
            process_id,
            PROC_PIDTASKINFO,
            0,
            info.as_mut_ptr().cast(),
            size,
        )
    };
    (read == size).then(|| unsafe { info.assume_init().resident_size })
}

#[cfg(windows)]
async fn worker_rss_bytes(child: &Child) -> Option<u64> {
    use std::{ffi::c_void, mem};

    #[repr(C)]
    struct ProcessMemoryCounters {
        size: u32,
        page_fault_count: u32,
        peak_working_set_size: usize,
        working_set_size: usize,
        peak_paged_pool_usage: usize,
        paged_pool_usage: usize,
        peak_nonpaged_pool_usage: usize,
        nonpaged_pool_usage: usize,
        pagefile_usage: usize,
        peak_pagefile_usage: usize,
    }

    #[link(name = "kernel32")]
    unsafe extern "system" {
        #[link_name = "K32GetProcessMemoryInfo"]
        fn get_process_memory_info(
            process: *mut c_void,
            counters: *mut ProcessMemoryCounters,
            size: u32,
        ) -> i32;
    }

    let handle = child.raw_handle()? as *mut c_void;
    let mut counters = ProcessMemoryCounters {
        size: u32::try_from(mem::size_of::<ProcessMemoryCounters>()).ok()?,
        page_fault_count: 0,
        peak_working_set_size: 0,
        working_set_size: 0,
        peak_paged_pool_usage: 0,
        paged_pool_usage: 0,
        peak_nonpaged_pool_usage: 0,
        nonpaged_pool_usage: 0,
        pagefile_usage: 0,
        peak_pagefile_usage: 0,
    };
    let result = unsafe { get_process_memory_info(handle, &mut counters, counters.size) };
    (result != 0).then(|| u64::try_from(counters.working_set_size).unwrap_or(u64::MAX))
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
async fn worker_rss_bytes(_child: &Child) -> Option<u64> {
    None
}

fn bounded_error(error: &anyhow::Error) -> String {
    error.to_string().chars().take(1_024).collect()
}

struct ProcessIsolation {
    #[cfg(windows)]
    _job_object: WindowsJobObject,
}

impl ProcessIsolation {
    fn attach(_child: &Child) -> Result<Self> {
        Ok(Self {
            #[cfg(windows)]
            _job_object: WindowsJobObject::assign(_child)?,
        })
    }
}

#[cfg(unix)]
fn set_low_process_priority(child: &Child) -> Result<()> {
    let process_id = child
        .id()
        .ok_or_else(|| anyhow!("llama-server process id is unavailable"))?;
    let result = unsafe { libc::setpriority(libc::PRIO_PROCESS, process_id, 10) };
    if result != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

#[cfg(windows)]
fn set_low_process_priority(child: &Child) -> Result<()> {
    use windows_sys::Win32::System::Threading::{BELOW_NORMAL_PRIORITY_CLASS, SetPriorityClass};
    let handle = child
        .raw_handle()
        .ok_or_else(|| anyhow!("llama-server process handle is unavailable"))?;
    if unsafe { SetPriorityClass(handle as _, BELOW_NORMAL_PRIORITY_CLASS) } == 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn set_low_process_priority(_child: &Child) -> Result<()> {
    bail!("low-priority llama-server processes are unsupported on this platform")
}

#[cfg(windows)]
struct WindowsJobObject {
    handle: windows_sys::Win32::Foundation::HANDLE,
}

#[cfg(windows)]
unsafe impl Send for WindowsJobObject {}

#[cfg(windows)]
impl WindowsJobObject {
    fn assign(child: &Child) -> Result<Self> {
        use std::{ffi::c_void, mem, ptr};
        use windows_sys::Win32::{
            Foundation::{CloseHandle, HANDLE},
            System::JobObjects::{
                AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
                JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
                SetInformationJobObject,
            },
        };

        let handle = unsafe { CreateJobObjectW(ptr::null(), ptr::null()) };
        if handle.is_null() {
            return Err(std::io::Error::last_os_error())
                .context("creating llama-server job object");
        }
        let mut info = unsafe { mem::zeroed::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() };
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        let configured = unsafe {
            SetInformationJobObject(
                handle,
                JobObjectExtendedLimitInformation,
                &info as *const _ as *const c_void,
                mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        };
        if configured == 0 {
            let error = std::io::Error::last_os_error();
            unsafe { CloseHandle(handle) };
            return Err(error).context("configuring llama-server job object");
        }
        let process_handle = child
            .raw_handle()
            .ok_or_else(|| anyhow!("llama-server process handle is unavailable"))?
            as HANDLE;
        if unsafe { AssignProcessToJobObject(handle, process_handle) } == 0 {
            let error = std::io::Error::last_os_error();
            unsafe { CloseHandle(handle) };
            return Err(error).context("assigning llama-server to job object");
        }
        Ok(Self { handle })
    }
}

#[cfg(windows)]
impl Drop for WindowsJobObject {
    fn drop(&mut self) {
        unsafe { windows_sys::Win32::Foundation::CloseHandle(self.handle) };
    }
}

#[cfg(all(test, unix))]
#[path = "local_model_lifecycle_tests.rs"]
mod lifecycle_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::anime_matching::{
        AnimeMatchAlias, AnimeMatchAliasKind, AnimeMatchAudioPreference, AnimeMatchCandidate,
        AnimeMatchContext, AnimeMatchContextTarget, AnimeMatchFile, AnimeMatchMediaType,
        AnimeMatchParseFacts, AnimeMatchScope, AnimeMatchSeasonContext, AnimeMatchTarget,
        AnimeSemanticMediaKind, build_semantic_evidence_request, smoke_requests,
    };
    use sha2::{Digest, Sha256};

    struct RejectInferenceAdmission;

    impl LocalModelAdmission for RejectInferenceAdmission {
        fn admit(
            &self,
            phase: LocalModelAdmissionPhase,
            _profile: &LocalModelRuntimeProfile,
        ) -> Result<()> {
            match phase {
                LocalModelAdmissionPhase::Inference => bail!("fixture memory pressure"),
                _ => Ok(()),
            }
        }
    }

    struct RejectAfterFirstPoll {
        polls: std::sync::atomic::AtomicUsize,
    }

    impl LocalModelAdmission for RejectAfterFirstPoll {
        fn admit(
            &self,
            phase: LocalModelAdmissionPhase,
            _profile: &LocalModelRuntimeProfile,
        ) -> Result<()> {
            if phase == LocalModelAdmissionPhase::Inference
                && self.polls.fetch_add(1, Ordering::AcqRel) >= 1
            {
                bail!("playback became active")
            }
            Ok(())
        }
    }

    fn absolute_test_path(name: &str) -> PathBuf {
        #[cfg(windows)]
        {
            PathBuf::from(format!(r"C:\elixir\{name}"))
        }
        #[cfg(not(windows))]
        {
            PathBuf::from(format!("/opt/elixir/{name}"))
        }
    }

    fn profile() -> LocalModelRuntimeProfile {
        LocalModelRuntimeProfile {
            bundle_version: "2026.08.1".to_string(),
            model_id: "qwen3-8b".to_string(),
            model_revision: "elixir-q4km-r1".to_string(),
            worker_revision: "llama-b123".to_string(),
            backend: "cpu".to_string(),
            profile_fingerprint: "sha256:profile".to_string(),
            protocol_version: LLAMA_SERVER_PROTOCOL_VERSION,
            matcher_schema_version: ANIME_MATCH_SCHEMA_VERSION,
            prompt_revision: ANIME_MATCH_PROMPT_REVISION.to_string(),
            worker_path: absolute_test_path("llama-server"),
            model_path: absolute_test_path("model.gguf"),
            context_tokens: V1_CONTEXT_TOKENS,
            max_output_tokens: 256,
            threads: 4,
            batch_threads: 4,
            gpu_layers: 0,
            kv_cache_type: "f16".to_string(),
            peak_rss_bytes: 2 * 1024 * 1024 * 1024,
            idle_unload_seconds: 300,
            sampling: LocalModelSamplingProfile::default(),
        }
    }

    const ALM9_NATIVE_LLAMA_SERVER_ENV: &str = "ELIXIR_ALM9_LLAMA_SERVER_PATH";
    const ALM9_NATIVE_QWEN_MODEL_ENV: &str = "ELIXIR_ALM9_QWEN3_8B_Q4_K_M_PATH";
    const ALM9_NATIVE_LLAMA_SERVER_SHA256: &str =
        "11e02e3fd6c0ce1c770e79b8d9ccf5670a69d26c6252dfbfd55cb9caf22b95b7";
    const ALM9_NATIVE_QWEN_MODEL_SHA256: &str =
        "d98cdcbd03e17ce47681435b5150e34c1417f50b5c0019dd560e4882c5745785";

    fn alm9_native_release_path(variable: &str) -> Result<PathBuf> {
        let value = std::env::var_os(variable).ok_or_else(|| {
            anyhow!("required release-maintenance environment variable {variable} is unset")
        })?;
        ensure!(!value.is_empty(), "{variable} is empty");
        let path = PathBuf::from(value);
        ensure!(
            path.is_absolute(),
            "{variable} must contain an absolute path"
        );
        Ok(path)
    }

    fn alm9_native_cpu_profile(
        worker_path: PathBuf,
        model_path: PathBuf,
    ) -> LocalModelRuntimeProfile {
        LocalModelRuntimeProfile {
            bundle_version: "alm9-native-release-probe-v1".to_string(),
            model_id: "Qwen/Qwen3-8B".to_string(),
            model_revision: "validation-7c41481f57cb95916b40956ab2f0b139b296d974-q4-k-m"
                .to_string(),
            worker_revision: "llama.cpp-b9637-aedb2a5e9ca3d4064148bbb919e0ddc0c1b70ab3".to_string(),
            backend: "cpu".to_string(),
            profile_fingerprint:
                "sha256:70818f4afc343b4de24e5686b9f9f3c2b13981d492d3370f13920b68758de176"
                    .to_string(),
            protocol_version: LLAMA_SERVER_PROTOCOL_VERSION,
            matcher_schema_version: ANIME_MATCH_SCHEMA_VERSION,
            prompt_revision: ANIME_MATCH_PROMPT_REVISION.to_string(),
            worker_path,
            model_path,
            context_tokens: V1_CONTEXT_TOKENS,
            max_output_tokens: 256,
            threads: 4,
            batch_threads: 8,
            gpu_layers: 0,
            kv_cache_type: "f16".to_string(),
            peak_rss_bytes: 4 * 1024 * 1024 * 1024,
            idle_unload_seconds: 300,
            sampling: LocalModelSamplingProfile::default(),
        }
    }

    async fn alm9_native_sha256(path: &Path) -> Result<String> {
        let mut file = tokio::fs::File::open(path)
            .await
            .with_context(|| format!("opening native release artifact {}", path.display()))?;
        let mut digest = Sha256::new();
        let mut buffer = vec![0_u8; 1024 * 1024];
        loop {
            let read = file
                .read(&mut buffer)
                .await
                .with_context(|| format!("hashing native release artifact {}", path.display()))?;
            if read == 0 {
                break;
            }
            digest.update(&buffer[..read]);
        }
        Ok(format!("{:x}", digest.finalize()))
    }

    fn request() -> AnimeMatchRequest {
        AnimeMatchRequest {
            schema_version: ANIME_MATCH_SCHEMA_VERSION,
            request_id: "search-1".to_string(),
            target: AnimeMatchTarget {
                media_type: AnimeMatchMediaType::Anime,
                canonical_title: "Tokyo Ghoul".to_string(),
                scope: AnimeMatchScope::Episode,
                wanted_target_keys: vec!["S02E01".to_string()],
                season_number: Some(2),
                episode_numbers: vec![1],
                absolute_episode_numbers: vec![13],
                audio_preference: AnimeMatchAudioPreference::default(),
            },
            context: AnimeMatchContext {
                graph_fingerprint: "graph".to_string(),
                seasons: vec![AnimeMatchSeasonContext {
                    season_number: 2,
                    anilist_id: "27899".to_string(),
                    aliases: vec![
                        AnimeMatchAlias {
                            value: "Tokyo Ghoul Root A".to_string(),
                            kind: AnimeMatchAliasKind::English,
                            source: Some("anizip_title".to_string()),
                            language: Some("en".to_string()),
                        },
                        AnimeMatchAlias {
                            value: "東京喰種√A".to_string(),
                            kind: AnimeMatchAliasKind::Native,
                            source: Some("anilist_native".to_string()),
                            language: Some("ja".to_string()),
                        },
                    ],
                    targets: vec![AnimeMatchContextTarget {
                        target_key: "S02E01".to_string(),
                        title: "New Surge".to_string(),
                        season_number: Some(2),
                        episode_number: Some(1),
                        absolute_episode_number: Some(13),
                        tvdb_episode_id: Some("tvdb-opaque".to_string()),
                        anidb_episode_id: Some("anidb-opaque".to_string()),
                    }],
                }],
            },
            candidates: vec![AnimeMatchCandidate {
                candidate_key: "candidate-0".to_string(),
                title: "Tokyo Ghoul Root A - 01".to_string(),
                files: vec![AnimeMatchFile {
                    file_key: "candidate-0-file-0".to_string(),
                    path: "Tokyo Ghoul Root A - 01.mkv".to_string(),
                }],
                parse_facts: AnimeMatchParseFacts::default(),
            }],
        }
    }

    #[test]
    fn alm6_profile_enforces_fixed_v1_contract() {
        let valid = profile();
        valid.validate_contract().expect("valid profile");

        let mut invalid = valid.clone();
        invalid.context_tokens = 8_192;
        assert!(invalid.validate_contract().is_err());
        let mut invalid = valid.clone();
        invalid.prompt_revision = "downloaded-prompt".to_string();
        assert!(invalid.validate_contract().is_err());
        let mut invalid = valid;
        invalid.kv_cache_type = "q4_0".to_string();
        assert!(invalid.validate_contract().is_err());

        let mut invalid = profile();
        invalid.peak_rss_bytes = 0;
        assert!(invalid.validate_contract().is_err());
        let mut invalid = profile();
        invalid.sampling.revision = "unqualified-sampling".to_string();
        assert!(invalid.validate_contract().is_err());
        let mut invalid = profile();
        invalid.backend = "remote".to_string();
        assert!(invalid.validate_contract().is_err());
        let mut invalid = profile();
        invalid.gpu_layers = 1;
        assert!(invalid.validate_contract().is_err());
        let mut invalid = profile();
        invalid.batch_threads = invalid.threads - 1;
        assert!(invalid.validate_contract().is_err());
        let mut invalid = profile();
        invalid.batch_threads = 9;
        assert!(invalid.validate_contract().is_err());
    }

    #[test]
    fn alm6_worker_arguments_are_the_exact_managed_contract() {
        let profile = profile();
        let args = worker_args(&profile, 31_337)
            .into_iter()
            .map(|value| value.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(args.len(), 24);
        assert_eq!(
            &args[0..5],
            ["--host", "127.0.0.1", "--port", "31337", "--model"]
        );
        assert_eq!(args[5], profile.model_path.to_string_lossy().as_ref());
        assert_eq!(
            &args[6..],
            [
                "--device",
                "none",
                "--ctx-size",
                "4096",
                "--parallel",
                "1",
                "--threads",
                "4",
                "--threads-batch",
                "4",
                "--n-gpu-layers",
                "0",
                "--cache-type-k",
                "f16",
                "--cache-type-v",
                "f16",
                "--fit",
                "off",
            ]
        );
    }

    #[test]
    fn alm6_worker_arguments_pin_each_accelerator_to_exact_device_zero() {
        for (backend, expected) in [
            ("metal", "MTL0"),
            ("cuda", "CUDA0"),
            ("hip", "ROCm0"),
            ("vulkan", "Vulkan0"),
        ] {
            let mut profile = profile();
            profile.backend = backend.to_string();
            profile.gpu_layers = 24;
            let args = worker_args(&profile, 31_337)
                .into_iter()
                .map(|value| value.to_string_lossy().into_owned())
                .collect::<Vec<_>>();
            let device = args
                .windows(2)
                .find(|pair| pair[0] == "--device")
                .map(|pair| pair[1].as_str());
            assert_eq!(device, Some(expected), "backend {backend}");
            assert_eq!(args.iter().filter(|arg| *arg == "--device").count(), 1);
        }
    }

    #[test]
    fn alm9_intel_cpu_worker_forces_zero_metal_devices() {
        assert_eq!(
            worker_backend_environment("macos", "x86_64", "cpu"),
            vec![("GGML_METAL_DEVICES", "0")]
        );
        assert!(worker_backend_environment("macos", "aarch64", "cpu").is_empty());
        assert!(worker_backend_environment("macos", "aarch64", "metal").is_empty());
        assert!(worker_backend_environment("windows", "x86_64", "cpu").is_empty());
    }

    #[test]
    fn alm9_correctness_deadline_retains_separate_cold_start_allowances() {
        assert_eq!(COLD_READINESS_DEADLINE, Duration::from_secs(2 * 60));
        assert_eq!(PRIME_DEADLINE, Duration::from_secs(5 * 60));
        assert_eq!(REQUEST_DEADLINE, Duration::from_secs(30 * 60));
    }

    #[test]
    fn alm9_quantized_kv_cache_enables_required_flash_attention() {
        let mut profile = profile();
        profile.kv_cache_type = "q8_0".to_string();
        let args = worker_args(&profile, 31_337)
            .into_iter()
            .map(|value| value.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(&args[args.len() - 2..], ["--flash-attn", "on"]);
    }

    #[test]
    fn alm9_v13_direct_matcher_receives_target_context_and_candidates() {
        let mut request = request();
        request.context.seasons.insert(
            0,
            AnimeMatchSeasonContext {
                season_number: 1,
                anilist_id: "22319".to_string(),
                aliases: vec![AnimeMatchAlias {
                    value: "Tokyo Ghoul Season 1".to_string(),
                    kind: AnimeMatchAliasKind::English,
                    source: Some("anilist_english".to_string()),
                    language: Some("en".to_string()),
                }],
                targets: vec![AnimeMatchContextTarget {
                    target_key: "S01E12".to_string(),
                    title: "Ghoul".to_string(),
                    season_number: Some(1),
                    episode_number: Some(12),
                    absolute_episode_number: Some(12),
                    tvdb_episode_id: None,
                    anidb_episode_id: None,
                }],
            },
        );
        let body = build_chat_request(&request, &profile()).expect("direct match request");
        let user: Value = serde_json::from_str(
            body.pointer("/messages/1/content")
                .and_then(Value::as_str)
                .expect("direct user JSON"),
        )
        .expect("direct user object");
        assert_eq!(ANIME_MATCH_PROMPT_REVISION, "anime-semantic-evidence-v2");
        assert_eq!(
            ANIME_MATCH_RESPONSE_SCHEMA_REVISION,
            "anime-semantic-evidence-response-v1"
        );
        assert_eq!(user.pointer("/target/title"), Some(&json!("Tokyo Ghoul")));
        assert_eq!(user.pointer("/target/season"), Some(&json!(2)));
        assert_eq!(user.pointer("/target/audio/mode"), Some(&json!("any")));
        assert_eq!(user.pointer("/seasons/0/season"), Some(&json!(1)));
        assert_eq!(user.pointer("/seasons/1/season"), Some(&json!(2)));
        assert_eq!(
            user.pointer("/seasons/1/episodes/0/wanted"),
            Some(&json!(0))
        );
        assert_eq!(
            user.pointer("/candidates/0/release"),
            Some(&json!("Tokyo Ghoul Root A - 01"))
        );
        let encoded = serde_json::to_string(&user).expect("direct user JSON");
        assert!(!encoded.contains("candidate-0"));
        assert!(!encoded.contains("parseFacts"));
        assert!(!encoded.contains("22319"));
        let grammar = body["grammar"].as_str().expect("direct grammar");
        assert!(grammar.contains("decisions ::="));
        assert!(grammar.contains("decision ::= \"0\" | \"1\" | \"2\""));
        assert!(grammar.contains("mapping0"));
        assert!(grammar.contains("file0"));
        let prompt = body["messages"][0]["content"]
            .as_str()
            .expect("direct prompt");
        for rule in [
            "Check every anime candidate against the exact wanted target",
            "English, romaji, Japanese",
            "Tokyo Ghoul Root A is season 2",
            "An unrelated title is always 0",
            "An exact match can occur at any candidate index",
        ] {
            assert!(prompt.contains(rule), "missing direct rule: {rule}");
        }
    }

    #[test]
    fn alm9_v14_checklist_response_expands_only_request_local_references() {
        let request = request();
        let response = decode_compact_response("{\"d\":[2],\"m\":[[0,[0],[0]]]}", &request, 256)
            .expect("valid direct mapping");
        assert_eq!(response.matches.len(), 1);
        assert_eq!(response.matches[0].candidate_key, "candidate-0");
        assert_eq!(response.matches[0].matched_target_keys, ["S02E01"]);
        assert_eq!(
            response.matches[0].selected_file_keys,
            Some(vec!["candidate-0-file-0".to_string()])
        );

        for invalid in [
            "{\"d\":[],\"m\":[]}",
            "{\"d\":[3],\"m\":[]}",
            "{\"d\":[0],\"m\":[[0,[0],[0]]]}",
            "{\"d\":[2],\"m\":[]}",
            "{\"d\":[2],\"m\":[[1,[0],[0]]]}",
            "{\"d\":[2],\"m\":[[0,[1],[0]]]}",
            "{\"d\":[2],\"m\":[[0,[0],[1]]]}",
            "{\"d\":[2],\"m\":[[0,[0,0],[0]]]}",
        ] {
            assert!(
                decode_compact_response(invalid, &request, 256).is_err(),
                "invalid direct mapping was accepted: {invalid}"
            );
        }
    }

    #[test]
    fn alm9_semantic_prompt_exposes_only_complete_server_owned_hypotheses() {
        let request = build_semantic_evidence_request(
            &request(),
            "candidate-0",
            "Tokyo Ghoul Root A - 01",
            None,
            ["Tokyo Ghoul Root A".to_string()],
            [],
            [1],
            [13],
            [AnimeSemanticMediaKind::Episode],
        )
        .unwrap()
        .unwrap();
        let body = build_semantic_chat_request(&request, &profile()).unwrap();
        let user: Value = serde_json::from_str(
            body.pointer("/messages/1/content")
                .and_then(Value::as_str)
                .unwrap(),
        )
        .unwrap();

        assert_eq!(user["raw"], "Tokyo Ghoul Root A - 01");
        assert!(
            user["hypotheses"]
                .as_array()
                .is_some_and(|hypotheses| hypotheses.len() >= 3)
        );
        assert_eq!(user["titleCandidates"][0], "Tokyo Ghoul Root A");
        let encoded = serde_json::to_string(&user).unwrap();
        assert!(!encoded.contains("S02E01"));
        assert!(!encoded.contains("candidate-0"));
        assert!(!encoded.contains("27899"));
        assert_eq!(body["chat_template_kwargs"]["enable_thinking"], false);
        let grammar = body["grammar"].as_str().unwrap();
        assert!(grammar.contains("hypothesisIndex"));
        assert!(grammar.contains("choice ::= \"null\" | \"0\" | \"1\""));
    }

    #[test]
    fn alm6_input_token_response_is_strict() {
        let parsed: InputTokenResponse = serde_json::from_value(json!({
            "object": "response.input_tokens",
            "input_tokens": 123
        }))
        .expect("valid input-token response");
        assert_eq!(parsed.input_tokens, 123);
        assert!(
            serde_json::from_value::<InputTokenResponse>(json!({
                "object": "response.input_tokens",
                "input_tokens": 123,
                "unexpected": true
            }))
            .is_err()
        );
    }

    #[test]
    fn alm9_completion_telemetry_parses_real_llama_cpp_shape() {
        let parsed: ChatCompletionResponse = serde_json::from_value(json!({
            "choices": [{
                "message": {"content": "{\"schemaVersion\":1,\"matches\":[]}"}
            }],
            "usage": {
                "prompt_tokens": 321,
                "completion_tokens": 17
            },
            "timings": {
                "prompt_ms": 42.25,
                "predicted_ms": 108.5
            }
        }))
        .expect("llama.cpp completion telemetry");
        let usage = parsed.usage.expect("usage");
        let timings = parsed.timings.expect("timings");
        assert_eq!(usage.prompt_tokens, 321);
        assert_eq!(usage.completion_tokens, 17);
        assert_eq!(finite_positive_millis(timings.prompt_ms), Some(43));
        assert_eq!(finite_positive_millis(timings.predicted_ms), Some(109));
        assert_eq!(finite_positive_millis(f64::NAN), None);
        assert_eq!(finite_positive_millis(0.0), None);
    }

    /// Release-maintenance probe for the exact official model and packaged
    /// runtime. This deliberately stays ignored: it needs the two explicit
    /// artifact paths below, loads the 5.03 GB validation model, and exercises real CPU
    /// inference. Ordinary tests and the product expose no switch for it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires ELIXIR_ALM9_LLAMA_SERVER_PATH and ELIXIR_ALM9_QWEN3_8B_Q4_K_M_PATH"]
    async fn alm9_native_qwen3_8b_cpu_production_protocol_release_probe() {
        let result = async {
            let worker_path = alm9_native_release_path(ALM9_NATIVE_LLAMA_SERVER_ENV)?;
            let model_path = alm9_native_release_path(ALM9_NATIVE_QWEN_MODEL_ENV)?;
            ensure!(
                alm9_native_sha256(&worker_path).await? == ALM9_NATIVE_LLAMA_SERVER_SHA256,
                "native release probe worker bytes do not match pinned llama.cpp b9637"
            );
            ensure!(
                alm9_native_sha256(&model_path).await? == ALM9_NATIVE_QWEN_MODEL_SHA256,
                "native release probe model bytes do not match the pinned Qwen3-8B Q4_K_M validation artifact"
            );
            let profile = alm9_native_cpu_profile(worker_path, model_path);
            profile.validate_contract()?;
            ensure!(profile.threads == 4, "native probe must use four CPU threads");
            ensure!(
                profile.gpu_layers == 0 && profile.backend == "cpu",
                "native probe must disable GPU offload"
            );
            ensure!(
                profile.kv_cache_type == "f16" && profile.max_output_tokens == 256,
                "native probe must use the qualified f16/256 output profile"
            );
            ensure!(
                REQUEST_DEADLINE == Duration::from_secs(30 * 60),
                "native probe must exercise the correctness-first production deadline"
            );
            ensure!(
                PRIME_DEADLINE == Duration::from_secs(5 * 60),
                "native probe must keep priming inside the cold-start envelope"
            );

            let corpus: Value = serde_json::from_slice(include_bytes!(
                "fixtures/hardware-certification-requests.json"
            ))
            .context("decoding frozen ALM-9 hardware-certification requests")?;
            ensure!(
                corpus["schemaVersion"] == json!(1) && corpus["status"] == json!("frozen"),
                "hardware-certification request corpus is not the frozen V1 contract"
            );
            let requests = corpus["requests"]
                .as_array()
                .ok_or_else(|| anyhow!("hardware-certification corpus requests are missing"))?;
            ensure!(
                requests.len() == 2,
                "native release probe requires exactly two frozen requests"
            );
            let requests = requests
                .iter()
                .cloned()
                .map(serde_json::from_value::<AnimeMatchRequest>)
                .collect::<std::result::Result<Vec<_>, _>>()
                .context("decoding native release-probe requests")?;

            let expected = [
                (
                    "alm9-hardware-tokyo-ghoul-s2e1",
                    "S02E01",
                    AnimeMatchAudioProfile::DualAudio,
                ),
                (
                    "alm9-hardware-cross-script-absolute",
                    "S01E13",
                    AnimeMatchAudioProfile::Unknown,
                ),
            ];
            ensure!(
                requests
                    .iter()
                    .zip(expected)
                    .all(|(request, (request_id, _, _))| request.request_id == request_id),
                "hardware-certification request order or identity changed"
            );

            // Inspect the exact builder output before execution so this native
            // probe also pins the constrained grammar of each real request.
            for request in &requests {
                let body = build_chat_request(request, &profile)
                    .with_context(|| format!("building native request {}", request.request_id))?;
                let grammar = body["grammar"]
                    .as_str()
                    .ok_or_else(|| anyhow!("native request omitted constrained grammar"))?;
                ensure!(grammar.contains("root ::="), "native request omitted root rule");
                ensure!(
                    grammar.contains("decisions ::=")
                        && grammar.contains("decision ::= \"0\" | \"1\" | \"2\"")
                        && grammar.contains("mapping0 ::="),
                    "native request {} omitted constrained grammar rules",
                    request.request_id
                );
                ensure!(
                    !grammar.contains("entity") && !grammar.contains("integer"),
                    "native request {} retained the obsolete semantic wire",
                    request.request_id
                );
                ensure!(
                    grammar.lines().all(|line| {
                        let Some((rule, alternatives)) = line.split_once(" ::= ") else {
                            return false;
                        };
                        !rule.trim().is_empty() && !alternatives.trim().is_empty()
                    }),
                    "native request {} contains a malformed finite grammar rule",
                    request.request_id
                );
            }

            let engine = LocalModelEngine::allow_all_for_probe()
                .context("constructing isolated native release-probe engine")?;
            engine
                .activate_profile_for_probe(profile.clone())
                .await
                .context("activating native CPU release-probe profile")?;
            engine
                .prime()
                .await
                .context("priming native CPU release-probe worker")?;

            // Keep teardown outside the inference future so every ordinary
            // error path still gracefully stops and reaps the managed worker.
            let probe_result = async {
                for (request, (_, target_key, audio_profile)) in
                    requests.into_iter().zip(expected)
                {
                    let request_id = request.request_id.clone();
                    let measurement = engine
                        .benchmark_match(request)
                        .await
                        .with_context(|| format!("running native request {request_id}"))?;
                    ensure!(
                        measurement.output.response.schema_version
                            == ANIME_MATCH_SCHEMA_VERSION,
                        "native request {request_id} returned the wrong schema"
                    );
                    ensure!(
                        measurement.output.response.matches
                            == vec![AnimeCandidateMatch {
                                candidate_key: "candidate-0".to_string(),
                                matched_target_keys: vec![target_key.to_string()],
                                audio_profile,
                                selected_file_keys: Some(vec![
                                    "candidate-0-file-0".to_string()
                                ]),
                            }],
                        "native request {request_id} returned a wrong mapping: {:?}",
                        measurement.output.response.matches
                    );
                    ensure!(
                        measurement.output.runtime.as_ref() == Some(&profile.provenance()),
                        "native request {request_id} returned wrong runtime provenance"
                    );
                    ensure!(
                        measurement.prompt_tokens > 0
                            && measurement.generated_tokens > 0
                            && measurement.prompt_time_ms > 0
                            && measurement.generation_time_ms > 0
                            && measurement.elapsed_ms > 0,
                        "native request {request_id} returned non-positive telemetry: {measurement:?}"
                    );
                }
                Result::<()>::Ok(())
            }
            .await;

            engine.shutdown().await;
            let snapshot = engine.snapshot().await;
            ensure!(
                snapshot.state == LocalModelWorkerState::Inactive
                    && snapshot.process_id.is_none()
                    && snapshot.loopback_port.is_none(),
                "native release-probe worker was not cleanly stopped: {snapshot:?}"
            );
            probe_result
        }
        .await;

        result.unwrap_or_else(|error: anyhow::Error| {
            panic!("ALM-9 native Qwen production protocol probe failed: {error:#}")
        });
    }

    /// Release-maintenance latency diagnostic for the first request in the
    /// frozen qualification corpus. Unlike the production protocol probe,
    /// this allows the request to finish past the production deadline so a
    /// timeout can be separated into prompt-evaluation and generation cost.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires the ALM-9 native artifacts and ELIXIR_ALM9_CORPUS_PATH"]
    async fn alm9_native_qwen3_8b_cpu_frozen_case_latency_diagnostic() {
        let result = async {
            let worker_path = alm9_native_release_path(ALM9_NATIVE_LLAMA_SERVER_ENV)?;
            let model_path = alm9_native_release_path(ALM9_NATIVE_QWEN_MODEL_ENV)?;
            let corpus_path = alm9_native_release_path("ELIXIR_ALM9_CORPUS_PATH")?;
            ensure!(
                alm9_native_sha256(&worker_path).await? == ALM9_NATIVE_LLAMA_SERVER_SHA256,
                "native diagnostic worker bytes do not match pinned llama.cpp b9637"
            );
            ensure!(
                alm9_native_sha256(&model_path).await? == ALM9_NATIVE_QWEN_MODEL_SHA256,
                "native diagnostic model bytes do not match the pinned Qwen3-8B Q4_K_M validation artifact"
            );
            let corpus: Value = serde_json::from_slice(
                &tokio::fs::read(&corpus_path)
                    .await
                    .with_context(|| format!("reading {}", corpus_path.display()))?,
            )
            .context("decoding frozen qualification corpus")?;
            let case = corpus
                .pointer("/cases/0")
                .ok_or_else(|| anyhow!("qualification corpus has no first case"))?;
            let case_id = case
                .get("caseId")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("first qualification case has no ID"))?;
            let request: AnimeMatchRequest = serde_json::from_value(
                case.pointer("/input/request")
                    .cloned()
                    .ok_or_else(|| anyhow!("first qualification case has no request"))?,
            )
            .context("decoding first qualification request")?;
            let profile = alm9_native_cpu_profile(worker_path, model_path);
            let engine = LocalModelEngine::allow_all_for_probe()?;
            engine.activate_profile_for_probe(profile).await?;
            let prime_started = Instant::now();
            engine
                .prime()
                .await
                .context("running latency-diagnostic priming request")?;
            let prime_elapsed_ms = duration_millis(prime_started.elapsed());
            let measured = engine
                .infer_measured_with_deadline(request, Duration::from_secs(60))
                .await;
            engine.shutdown().await;
            let (_, completion) = measured.with_context(|| {
                format!("running frozen qualification latency diagnostic {case_id}")
            })?;
            eprintln!(
                "ALM9_PRIMING_LATENCY elapsed_ms={prime_elapsed_ms}",
            );
            eprintln!(
                "ALM9_WARM_FROZEN_CASE_LATENCY case={case_id} prompt_tokens={:?} generated_tokens={:?} prompt_ms={:?} generation_ms={:?}",
                completion.prompt_tokens,
                completion.generated_tokens,
                completion.prompt_time_ms,
                completion.generation_time_ms,
            );
            Result::<()>::Ok(())
        }
        .await;
        if let Err(error) = result {
            panic!("{error:#}");
        }
    }

    #[test]
    fn alm6_loopback_url_rejects_non_loopback_addresses() {
        assert!(loopback_url("127.0.0.1:8080".parse().unwrap(), "/health").is_ok());
        assert!(loopback_url("192.0.2.1:8080".parse().unwrap(), "/health").is_err());
    }

    #[test]
    fn alm6_json_content_type_validation_accepts_json_media_types_only() {
        assert!(is_json_content_type("application/json"));
        assert!(is_json_content_type("Application/JSON; charset=utf-8"));
        assert!(is_json_content_type("application/problem+json"));
        assert!(!is_json_content_type("text/plain"));
        assert!(!is_json_content_type("text/json"));
    }

    #[test]
    fn alm6_worker_diagnostic_tail_is_continuously_bounded() {
        let tail = WorkerDiagnosticTail::default();
        tail.push(&vec![b'a'; MAX_WORKER_DIAGNOSTIC_BYTES]);
        tail.push(b"final worker failure");
        let bytes = tail
            .bytes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(bytes.len(), MAX_WORKER_DIAGNOSTIC_BYTES);
        drop(bytes);
        assert!(
            tail.excerpt()
                .is_some_and(|excerpt| excerpt.ends_with("final worker failure"))
        );
    }

    #[test]
    fn alm6_restart_budget_resets_only_after_a_successful_completion() {
        let engine = LocalModelEngine::allow_all_for_probe().expect("engine");
        assert!(engine.claim_restart(), "first crash gets one restart");
        assert!(
            !engine.claim_restart(),
            "restart crash exhausts the episode"
        );
        engine.mark_successful_completion();
        assert!(engine.claim_restart(), "a later crash starts a new episode");
        assert!(
            !engine.claim_restart(),
            "the new episode is still bounded to one restart"
        );
    }

    #[tokio::test]
    async fn alm9_exhausted_restart_budget_cannot_be_bypassed_by_later_requests() {
        let directory = tempfile::tempdir().expect("temporary runtime directory");
        let worker_path = directory.path().join("llama-server");
        let model_path = directory.path().join("model.gguf");
        std::fs::write(&worker_path, b"fixture worker").expect("write worker fixture");
        std::fs::write(&model_path, b"fixture model").expect("write model fixture");
        let mut profile = profile();
        profile.worker_path = worker_path;
        profile.model_path = model_path;

        let engine = LocalModelEngine::allow_all().expect("engine");
        engine
            .activate_profile_cold(profile)
            .await
            .expect("cold activation");
        engine.inner.restart_used.store(true, Ordering::Release);
        let requested = engine
            .inner
            .background_prime_requested
            .load(Ordering::Acquire);

        for _ in 0..3 {
            let error = engine
                .match_candidates(request())
                .await
                .expect_err("exhausted restart must use deterministic fallback");
            assert!(error.to_string().contains("not primed"));
        }
        assert_eq!(
            engine
                .inner
                .background_prime_requested
                .load(Ordering::Acquire),
            requested,
            "a later request bypassed the exhausted restart budget"
        );
        assert!(!engine.inner.background_warm_active.load(Ordering::Acquire));
        assert!(engine.snapshot().await.process_id.is_none());
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn alm6_probe_activation_does_not_schedule_a_competing_warm_spawn() {
        let directory = tempfile::tempdir().expect("temporary runtime directory");
        let worker_path = directory.path().join("llama-server");
        let model_path = directory.path().join("model.gguf");
        std::fs::write(&worker_path, b"fixture worker").expect("write worker fixture");
        std::fs::write(&model_path, b"fixture model").expect("write model fixture");
        let mut profile = profile();
        profile.worker_path = worker_path;
        profile.model_path = model_path;
        let engine = LocalModelEngine::allow_all_for_probe().expect("engine");
        assert!(!engine.inner.publish_runtime_metrics);
        engine
            .activate_profile_for_probe(profile)
            .await
            .expect("cold probe activation");
        assert!(!engine.inner.background_warm_active.load(Ordering::Acquire));
        let snapshot = engine.snapshot().await;
        assert_eq!(snapshot.state, LocalModelWorkerState::Inactive);
        assert!(snapshot.process_id.is_none());
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn alm6_production_engine_cannot_run_manager_bypass_probe_phases() {
        let engine = LocalModelEngine::allow_all().expect("engine");
        let error = engine
            .probe(request(), request())
            .await
            .expect_err("production engine must reject probe API");
        assert!(error.to_string().contains("probe-only"));
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn alm9_probe_rejects_an_identical_priming_request_before_worker_start() {
        let directory = tempfile::tempdir().expect("temporary runtime directory");
        let worker_path = directory.path().join("llama-server");
        let model_path = directory.path().join("model.gguf");
        std::fs::write(&worker_path, b"fixture worker").expect("write worker fixture");
        std::fs::write(&model_path, b"fixture model").expect("write model fixture");
        let mut profile = profile();
        profile.worker_path = worker_path;
        profile.model_path = model_path;

        let engine = LocalModelEngine::allow_all_for_probe().expect("probe engine");
        engine
            .activate_profile_for_probe(profile)
            .await
            .expect("activate probe profile");
        let same_request = request();
        let error = engine
            .probe(same_request.clone(), same_request)
            .await
            .expect_err("identical priming input must not qualify a profile");
        assert!(
            error.to_string().contains("must be distinct"),
            "unexpected identical-probe error: {error:#}"
        );
        assert!(
            engine.snapshot().await.process_id.is_none(),
            "identical probe input started a worker"
        );
        engine.shutdown().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn alm9_probe_primes_then_measures_a_distinct_request_on_one_worker() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().expect("temporary runtime directory");
        let worker_path = directory.path().join("llama-server");
        let lifecycle_path = worker_path.with_extension("lifecycle");
        let model_path = directory.path().join("model.gguf");
        std::fs::write(
            &worker_path,
            br#"#!/usr/bin/env python3
import http.server
import json
import os
import signal
import sys
import time

LIFECYCLE_PATH = __file__ + ".lifecycle"
chat_calls = 0


def record(event):
    with open(LIFECYCLE_PATH, "a", encoding="utf-8") as lifecycle:
        lifecycle.write(f"{event}:{os.getpid()}\n")
        lifecycle.flush()
        os.fsync(lifecycle.fileno())


def stop(_signal, _frame):
    record("stopped")
    os._exit(0)


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def reply(self, status, body=b""):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            self.reply(200, b'{}')
        else:
            self.reply(404, b'{}')

    def do_POST(self):
        global chat_calls
        length = int(self.headers.get("Content-Length", "0"))
        raw_body = self.rfile.read(length)
        if self.path == "/v1/chat/completions/input_tokens":
            record("tokens")
            self.reply(
                200,
                b'{"object":"response.input_tokens","input_tokens":32}',
            )
        elif self.path == "/v1/chat/completions":
            chat_calls += 1
            record(f"chat-{chat_calls}")
            time.sleep(0.6 if chat_calls == 1 else 0.02)
            json.loads(raw_body)
            content = '{"d":[2,0,0,0],"m":[[0,[0],[0]]]}'
            payload = json.dumps({
                "choices": [{"message": {"content": content}}],
                "usage": {"prompt_tokens": 32, "completion_tokens": 14},
                "timings": {"prompt_ms": 10.0, "predicted_ms": 10.0},
            }).encode("utf-8")
            self.reply(
                200,
                payload,
            )
        else:
            self.reply(404, b'{}')

    def log_message(self, _format, *_arguments):
        pass


arguments = sys.argv
host = arguments[arguments.index("--host") + 1]
port = int(arguments[arguments.index("--port") + 1])
signal.signal(signal.SIGTERM, stop)
server = http.server.ThreadingHTTPServer((host, port), Handler)
server.daemon_threads = True
record("started")
server.serve_forever()
"#,
        )
        .expect("write worker fixture");
        std::fs::set_permissions(&worker_path, std::fs::Permissions::from_mode(0o755))
            .expect("make worker executable");
        std::fs::write(&model_path, b"fixture model").expect("write model fixture");
        let mut profile = profile();
        profile.worker_path = worker_path;
        profile.model_path = model_path;

        let [priming_request, measured_request] =
            smoke_requests().expect("compiled hardware probe fixtures");

        let engine = LocalModelEngine::allow_all_for_probe().expect("probe engine");
        engine
            .activate_profile_for_probe(profile)
            .await
            .expect("activate probe profile");
        let probe_started = Instant::now();
        let measurement = engine
            .probe(priming_request, measured_request)
            .await
            .expect("two-request probe");
        let probe_elapsed_ms = duration_millis(probe_started.elapsed());

        assert!(measurement.worker_ready && measurement.smoke_match_passed);
        assert_eq!(
            measurement.priming_response.matches[0].candidate_key,
            "candidate-0"
        );
        assert_eq!(measurement.response.matches[0].candidate_key, "candidate-0");
        assert!(
            measurement.warm_latency_ms + 400 < probe_elapsed_ms,
            "priming delay leaked into measured warm latency: measurement={measurement:?}, total={probe_elapsed_ms}ms"
        );
        assert!(
            measurement.warm_latency_ms >= 20,
            "measured request delay was not observed: {measurement:?}"
        );
        let process_id = engine
            .snapshot()
            .await
            .process_id
            .expect("probe worker process ID");
        let lifecycle = std::fs::read_to_string(&lifecycle_path).expect("probe lifecycle log");
        let events = lifecycle.lines().collect::<Vec<_>>();
        let expected_started = format!("started:{process_id}");
        assert_eq!(
            events
                .iter()
                .filter(|event| event.as_bytes() == expected_started.as_bytes())
                .count(),
            1,
            "probe did not retain exactly one worker: {events:?}"
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event.starts_with("tokens:"))
                .count(),
            2,
            "probe did not run exactly two direct token preflights: {events:?}"
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event.starts_with("chat-"))
                .count(),
            2,
            "probe did not run exactly two direct completions: {events:?}"
        );
        engine.shutdown().await;
        let snapshot = engine.snapshot().await;
        assert_eq!(snapshot.state, LocalModelWorkerState::Inactive);
        assert!(snapshot.process_id.is_none());
        assert!(snapshot.loopback_port.is_none());
        let lifecycle = std::fs::read_to_string(&lifecycle_path).expect("probe lifecycle log");
        let stopped = format!("stopped:{process_id}");
        assert_eq!(
            lifecycle
                .lines()
                .filter(|event| event.as_bytes() == stopped.as_bytes())
                .count(),
            1,
            "probe worker was not stopped exactly once: {lifecycle:?}"
        );
    }

    #[tokio::test]
    async fn alm6_manager_cold_activation_waits_for_explicit_warm() {
        let directory = tempfile::tempdir().expect("temporary runtime directory");
        let worker_path = directory.path().join("llama-server");
        let model_path = directory.path().join("model.gguf");
        std::fs::write(&worker_path, b"fixture worker").expect("write worker fixture");
        std::fs::write(&model_path, b"fixture model").expect("write model fixture");
        let mut profile = profile();
        profile.worker_path = worker_path;
        profile.model_path = model_path;

        let engine = LocalModelEngine::allow_all().expect("engine");
        engine
            .activate_profile_cold(profile)
            .await
            .expect("cold manager activation");
        assert!(!engine.inner.background_warm_active.load(Ordering::Acquire));
        assert_eq!(
            engine.snapshot().await.state,
            LocalModelWorkerState::Inactive
        );
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn alm6_missing_worker_or_model_never_activates_a_profile() {
        let directory = tempfile::tempdir().expect("temporary runtime directory");
        let engine = LocalModelEngine::allow_all_for_probe().expect("engine");

        let mut missing_worker = profile();
        missing_worker.worker_path = directory.path().join("missing-llama-server");
        missing_worker.model_path = directory.path().join("model.gguf");
        std::fs::write(&missing_worker.model_path, b"fixture model").expect("write model");
        assert!(
            engine
                .activate_profile_for_probe(missing_worker)
                .await
                .expect_err("missing worker must fail")
                .to_string()
                .contains("installed worker")
        );

        let mut missing_model = profile();
        missing_model.worker_path = directory.path().join("llama-server");
        missing_model.model_path = directory.path().join("missing-model.gguf");
        std::fs::write(&missing_model.worker_path, b"fixture worker").expect("write worker");
        assert!(
            engine
                .activate_profile_for_probe(missing_model)
                .await
                .expect_err("missing model must fail")
                .to_string()
                .contains("installed model")
        );
        engine.shutdown().await;
    }

    #[cfg(unix)]
    #[tokio::test(start_paused = true)]
    async fn alm6_stalled_worker_is_killed_at_the_readiness_deadline() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().expect("temporary runtime directory");
        let worker_path = directory.path().join("llama-server");
        let model_path = directory.path().join("model.gguf");
        std::fs::write(&worker_path, b"#!/bin/sh\nsleep 60\n").expect("write worker");
        std::fs::set_permissions(&worker_path, std::fs::Permissions::from_mode(0o755))
            .expect("make worker executable");
        std::fs::write(&model_path, b"fixture model").expect("write model");
        let mut profile = profile();
        profile.worker_path = worker_path;
        profile.model_path = model_path;

        let engine = LocalModelEngine::allow_all().expect("engine");
        engine
            .activate_profile_cold(profile)
            .await
            .expect("cold activation");
        let error = engine
            .warm_for_activation()
            .await
            .expect_err("stalled worker must time out");
        assert!(
            error.to_string().contains("readiness deadline exceeded"),
            "unexpected readiness failure: {error:#}"
        );
        let snapshot = engine.snapshot().await;
        assert_eq!(snapshot.state, LocalModelWorkerState::Unavailable);
        assert!(snapshot.process_id.is_none());
        engine.shutdown().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn alm6_stalled_generation_is_killed_before_background_rewarm() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().expect("temporary runtime directory");
        let worker_path = directory.path().join("llama-server");
        let lifecycle_path = worker_path.with_extension("lifecycle");
        let model_path = directory.path().join("model.gguf");
        std::fs::write(
            &worker_path,
            br#"#!/usr/bin/env python3
import http.server
import os
import signal
import sys
import time

LIFECYCLE_PATH = __file__ + ".lifecycle"
chat_calls = 0


def record(event):
    with open(LIFECYCLE_PATH, "a", encoding="utf-8") as lifecycle:
        lifecycle.write(f"{event}:{os.getpid()}\n")
        lifecycle.flush()
        os.fsync(lifecycle.fileno())


def stop(_signal, _frame):
    record("stopped")
    os._exit(0)


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def reply(self, status, body=b""):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            self.reply(200, b'{}')
        else:
            self.reply(404, b'{}')

    def do_POST(self):
        global chat_calls
        length = int(self.headers.get("Content-Length", "0"))
        self.rfile.read(length)
        if self.path == "/v1/chat/completions/input_tokens":
            self.reply(
                200,
                b'{"object":"response.input_tokens","input_tokens":32}',
            )
        elif self.path == "/v1/chat/completions":
            chat_calls += 1
            record(f"chat-{chat_calls}")
            if chat_calls == 1:
                self.reply(
                    200,
                    b'{"choices":[{"message":{"content":"{\\"d\\":[2,0,0,0],\\"m\\":[[0,[0],[0]]]}"}}],"usage":{"prompt_tokens":32,"completion_tokens":14},"timings":{"prompt_ms":10.0,"predicted_ms":10.0}}',
                )
            else:
                while True:
                    time.sleep(60)
        else:
            self.reply(404, b'{}')

    def log_message(self, _format, *_arguments):
        pass


arguments = sys.argv
host = arguments[arguments.index("--host") + 1]
port = int(arguments[arguments.index("--port") + 1])
signal.signal(signal.SIGTERM, stop)
server = http.server.ThreadingHTTPServer((host, port), Handler)
server.daemon_threads = True
record("started")
server.serve_forever()
"#,
        )
        .expect("write worker");
        std::fs::set_permissions(&worker_path, std::fs::Permissions::from_mode(0o755))
            .expect("make worker executable");
        std::fs::write(&model_path, b"fixture model").expect("write model");
        let mut profile = profile();
        profile.worker_path = worker_path;
        profile.model_path = model_path;
        let certification_profile = profile.clone();

        let engine = LocalModelEngine::allow_all_for_probe().expect("probe engine");
        engine
            .activate_profile_for_probe(profile)
            .await
            .expect("probe activation");
        engine
            .warm()
            .await
            .expect("fixture worker must become ready");
        let original_pid = engine
            .snapshot()
            .await
            .process_id
            .expect("ready worker process id");

        let request_deadline = Duration::from_secs(1);
        let request_started = Instant::now();
        let error = engine
            .match_with_deadline_for_certification(request(), request_deadline)
            .await
            .expect_err("stalled generation must return deterministic fallback error");
        let request_elapsed = request_started.elapsed();
        assert!(
            error
                .to_string()
                .contains("local model request deadline exceeded"),
            "unexpected stalled-generation failure: {error:#}"
        );
        assert!(
            request_elapsed >= request_deadline.saturating_sub(Duration::from_millis(250)),
            "generation returned before its absolute deadline: {request_elapsed:?}"
        );
        assert!(
            request_elapsed <= request_deadline + PROCESS_STOP_GRACE + Duration::from_secs(2),
            "generation timeout and worker teardown exceeded their bound: {request_elapsed:?}"
        );

        let original_pid = i32::try_from(original_pid).expect("worker pid fits platform range");
        assert_eq!(
            unsafe { libc::kill(original_pid, 0) },
            -1,
            "timed-out generation process remained alive"
        );
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH),
            "timed-out generation process was not fully reaped"
        );

        let replacement_deadline = Instant::now() + Duration::from_secs(3);
        let replacement_pid = loop {
            let snapshot = engine.snapshot().await;
            if snapshot.state == LocalModelWorkerState::Ready
                && let Some(process_id) = snapshot.process_id
                && process_id != u32::try_from(original_pid).expect("positive worker pid")
            {
                break process_id;
            }
            assert!(
                Instant::now() < replacement_deadline,
                "background warm did not produce a replacement worker: {snapshot:?}"
            );
            sleep(Duration::from_millis(20)).await;
        };

        let lifecycle = std::fs::read_to_string(&lifecycle_path).expect("worker lifecycle log");
        let events = lifecycle.lines().collect::<Vec<_>>();
        let original_started = events
            .iter()
            .position(|event| *event == format!("started:{original_pid}"))
            .expect("original worker start event");
        let original_chat = events
            .iter()
            .position(|event| *event == format!("chat-2:{original_pid}"))
            .expect("original worker generation event");
        let replacement_started = events
            .iter()
            .position(|event| *event == format!("started:{replacement_pid}"))
            .expect("replacement worker start event");
        assert!(
            original_started < original_chat && original_chat < replacement_started,
            "worker must abort stalled generation before replacement warm; events: {events:?}"
        );

        engine.shutdown().await;

        let certification_engine = LocalModelEngine::allow_all_for_probe().expect("probe engine");
        certification_engine
            .activate_profile_for_probe(certification_profile)
            .await
            .expect("probe activation");
        certification_engine
            .warm()
            .await
            .expect("certification fixture worker must become ready");
        let certification_pid = certification_engine
            .snapshot()
            .await
            .process_id
            .expect("certification worker process ID");
        assert_eq!(
            certification_engine
                .crash_active_worker_for_certification()
                .await
                .expect("forced certification crash"),
            certification_pid
        );
        let crash_fallback = certification_engine
            .match_with_deadline_for_certification(request(), Duration::from_millis(1))
            .await
            .expect_err("a crashed worker must immediately use deterministic fallback");
        assert!(
            crash_fallback
                .to_string()
                .contains("local model worker is not primed")
        );
        let replacement_deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let snapshot = certification_engine.snapshot().await;
            if snapshot.state == LocalModelWorkerState::Ready
                && snapshot
                    .process_id
                    .is_some_and(|pid| pid != certification_pid)
            {
                break;
            }
            assert!(
                Instant::now() < replacement_deadline,
                "certification replacement was not primed: {snapshot:?}"
            );
            sleep(Duration::from_millis(20)).await;
        }
        let deadline_error = certification_engine
            .match_with_deadline_for_certification(request(), Duration::from_millis(100))
            .await
            .expect_err("real stalled replacement must hit certification deadline");
        assert!(
            deadline_error
                .to_string()
                .contains("local model request deadline exceeded"),
            "unexpected replacement deadline failure: {deadline_error:#}"
        );
        certification_engine.shutdown().await;
    }

    #[tokio::test]
    async fn alm6_inflight_admission_poll_observes_playback_transition() {
        let admission = RejectAfterFirstPoll {
            polls: std::sync::atomic::AtomicUsize::new(0),
        };
        let shutdown = CancellationToken::new();
        let outcome = tokio::time::timeout(
            Duration::from_secs(1),
            monitor_inference_admission(
                &admission,
                LocalModelAdmissionPhase::Inference,
                &profile(),
                &shutdown,
                std::future::pending::<Result<()>>(),
            ),
        )
        .await
        .expect("admission transition must be observed promptly");
        assert!(matches!(outcome, AdmissionMonitored::Rejected(_)));
    }

    #[tokio::test]
    async fn alm6_completed_inference_gets_a_final_admission_check() {
        let shutdown = CancellationToken::new();
        let outcome = monitor_inference_admission(
            &RejectInferenceAdmission,
            LocalModelAdmissionPhase::Inference,
            &profile(),
            &shutdown,
            std::future::ready(Ok(())),
        )
        .await;
        assert!(matches!(outcome, AdmissionMonitored::Rejected(_)));
    }

    #[tokio::test]
    async fn alm6_snapshot_never_waits_for_inference_worker_mutex() {
        let engine = LocalModelEngine::allow_all().expect("engine");
        let mut slot = engine.inner.worker.lock().await;
        transition_worker_state(&mut slot, LocalModelWorkerState::Ready);
        let snapshot = tokio::time::timeout(Duration::from_millis(20), engine.snapshot())
            .await
            .expect("snapshot must use its nonblocking cache");
        assert_eq!(snapshot.state, LocalModelWorkerState::Ready);
        drop(slot);
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn alm6_absent_profile_returns_an_error_for_deterministic_fallback() {
        let engine = LocalModelEngine::allow_all().expect("engine");
        let result = engine.match_candidates(request()).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("no active local-model runtime profile")
        );
        engine.shutdown().await;
    }

    #[tokio::test]
    async fn alm6_inference_pressure_retains_profile_and_returns_fallback_error() {
        let directory = tempfile::tempdir().expect("temporary runtime directory");
        let worker_path = directory.path().join("llama-server");
        let model_path = directory.path().join("model.gguf");
        std::fs::write(&worker_path, b"fixture worker").expect("write worker fixture");
        std::fs::write(&model_path, b"fixture model").expect("write model fixture");
        let mut profile = profile();
        profile.worker_path = worker_path;
        profile.model_path = model_path;
        let expected_fingerprint = profile.profile_fingerprint.clone();

        let engine = LocalModelEngine::new_for_probe(Arc::new(RejectInferenceAdmission))
            .expect("probe engine");
        engine
            .activate_profile_for_probe(profile)
            .await
            .expect("probe activation");
        let error = engine
            .match_with_deadline_for_certification(request(), Duration::from_millis(100))
            .await
            .expect_err("pressure must defer inference");
        assert!(error.to_string().contains("resource admission"));
        let snapshot = engine.snapshot().await;
        assert_eq!(
            snapshot.profile_fingerprint.as_deref(),
            Some(expected_fingerprint.as_str())
        );
        assert_eq!(snapshot.state, LocalModelWorkerState::Inactive);
        assert!(snapshot.process_id.is_none());
        engine.shutdown().await;
    }
}
