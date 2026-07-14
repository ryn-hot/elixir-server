use std::{
    collections::VecDeque,
    fmt,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use axum::http::{
    HeaderMap, StatusCode,
    header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG},
};
use chrono::Utc;
use dashmap::DashMap;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt, SeekFrom},
    process::{Child, Command},
    sync::{Mutex, Notify, OnceCell, OwnedSemaphorePermit, Semaphore},
    task::JoinHandle,
    time,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::live::{
    config::LiveRemuxLimits,
    diagnostics::LiveRedactor,
    relay::{
        LiveRelayError, LiveRelayService,
        hls::{
            HlsManifestScope, HlsResourceId, HlsResourceKind, HlsResourceLimits, HlsResourceMap,
            HlsRewriteConfig, HlsRewriter,
        },
    },
    session::{DeliveryMode, LiveSessionRepository, SessionOwner, SessionProtocol, SessionRecord},
};

use super::{
    adapter::LiveRemuxAdapter,
    profile::{CopyRemuxProfile, RemuxProfileError, ffmpeg_args, ffprobe_args, parse_probe},
};

const MAX_PLAYLIST_BYTES: u64 = 1_048_576;
const JOB_PREFIX: &str = "job-";
const MARKER_FILE: &str = ".elixir-live-remux-v1.json";

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum LiveRemuxBuildError {
    #[error("invalid Live remux capacity")]
    InvalidCapacity,
    #[error("invalid Live remux temporary root")]
    InvalidTempRoot,
    #[error("Live remux temporary root is unavailable")]
    TempRootUnavailable,
    #[error("Live remux temporary root contains unsafe entries")]
    UnsafeTempRoot,
    #[error("Live remux binary is unavailable")]
    BinaryUnavailable,
    #[error("Live remux runtime probe failed")]
    RuntimeProbeFailed,
    #[error("Live remux HLS rewriter initialization failed")]
    Rewriter,
}

#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum LiveRemuxError {
    #[error("Live remux is unavailable")]
    Unavailable,
    #[error("Live remux capacity is exhausted")]
    CapacityExhausted,
    #[error("Live remux session expired")]
    SessionExpired,
    #[error("Live remux session does not match")]
    SessionMismatch,
    #[error("Live remux control fence is stale")]
    StaleControlFence,
    #[error("Live remux descriptor is invalid")]
    DescriptorInvalid,
    #[error("Live remux protocol is unsupported")]
    ProtocolUnsupported,
    #[error("Live remux probe rejected the source")]
    ProbeRejected,
    #[error("Live remux process failed")]
    ProcessFailed,
    #[error("Live remux startup timed out")]
    StartupTimeout,
    #[error("Live remux output is unhealthy")]
    OutputUnhealthy,
    #[error("Live remux disk pressure prevents admission")]
    DiskPressure,
    #[error("Live remux resource expired")]
    ResourceExpired,
    #[error("Live remux resource kind does not match")]
    ResourceKindMismatch,
    #[error("Live remux byte range is invalid")]
    RangeRejected,
    #[error("Live remux cleanup is incomplete")]
    CleanupIncomplete,
}

impl From<LiveRelayError> for LiveRemuxError {
    fn from(error: LiveRelayError) -> Self {
        match error {
            LiveRelayError::CapacityExhausted => Self::CapacityExhausted,
            LiveRelayError::SessionExpired => Self::SessionExpired,
            LiveRelayError::SessionMismatch => Self::SessionMismatch,
            LiveRelayError::StaleControlFence => Self::StaleControlFence,
            LiveRelayError::DescriptorInvalid | LiveRelayError::CredentialsRejected => {
                Self::DescriptorInvalid
            }
            LiveRelayError::ProtocolUnsupported => Self::ProtocolUnsupported,
            _ => Self::Unavailable,
        }
    }
}

impl From<RemuxProfileError> for LiveRemuxError {
    fn from(error: RemuxProfileError) -> Self {
        match error {
            RemuxProfileError::UnsupportedProtocol => Self::ProtocolUnsupported,
            RemuxProfileError::ProbeInvalid
            | RemuxProfileError::NoPlayableStream
            | RemuxProfileError::UnsupportedCodec => Self::ProbeRejected,
            RemuxProfileError::InvalidLoopbackInput | RemuxProfileError::InvalidOutputPath => {
                Self::Unavailable
            }
        }
    }
}

pub enum LiveRemuxPayloadBody {
    Bytes(Vec<u8>),
    File { file: fs::File, length: u64 },
}

impl fmt::Debug for LiveRemuxPayloadBody {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bytes(bytes) => formatter
                .debug_tuple("Bytes")
                .field(&format_args!("[{} BYTES]", bytes.len()))
                .finish(),
            Self::File { length, .. } => formatter
                .debug_struct("File")
                .field("length", length)
                .finish_non_exhaustive(),
        }
    }
}

#[derive(Debug)]
pub struct LiveRemuxPayload {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: LiveRemuxPayloadBody,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveRemuxSnapshot {
    pub active_jobs: usize,
    pub available_capacity: usize,
    pub jobs_started: u64,
    pub jobs_completed: u64,
    pub jobs_failed: u64,
    pub jobs_cancelled: u64,
    pub temp_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveRemuxJobDiagnostics {
    pub profile: &'static str,
    pub state: &'static str,
    pub stderr_tail: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveRemuxReconciliation {
    pub inspected: u32,
    pub restarted: u32,
    pub terminated: u32,
}

pub struct LiveRemuxService {
    repository: Arc<LiveSessionRepository>,
    relay: Arc<LiveRelayService>,
    redactor: Arc<LiveRedactor>,
    limits: LiveRemuxLimits,
    temp_root: PathBuf,
    capacity: Arc<Semaphore>,
    sessions: DashMap<Uuid, Arc<RemuxJob>>,
    admission_lock: Mutex<()>,
    startup_queue_timeout: Duration,
    initialized: OnceCell<()>,
    rewriter: HlsRewriter,
    jobs_started: AtomicU64,
    jobs_completed: AtomicU64,
    jobs_failed: AtomicU64,
    jobs_cancelled: AtomicU64,
}

impl fmt::Debug for LiveRemuxService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveRemuxService")
            .field("active_jobs", &self.sessions.len())
            .field("available_capacity", &self.capacity.available_permits())
            .field("temp_root", &self.temp_root)
            .finish_non_exhaustive()
    }
}

struct RemuxJob {
    job_id: String,
    session_id: Uuid,
    owner: SessionOwner,
    control_fencing_token: i64,
    token_revision: i64,
    protocol: SessionProtocol,
    profile: CopyRemuxProfile,
    directory: PathBuf,
    playlist: PathBuf,
    resources: Mutex<HlsResourceMap>,
    stderr_ring: Arc<std::sync::Mutex<VecDeque<u8>>>,
    cancellation: CancellationToken,
    ready: AtomicBool,
    completed: AtomicBool,
    cleanup_succeeded: AtomicBool,
    completion: Notify,
}

impl fmt::Debug for RemuxJob {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RemuxJob")
            .field("job_id", &self.job_id)
            .field("session_id", &self.session_id)
            .field("control_fencing_token", &self.control_fencing_token)
            .field("token_revision", &self.token_revision)
            .field("protocol", &self.protocol)
            .field("profile", &self.profile)
            .field("ready", &self.ready.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl RemuxJob {
    fn matches(&self, session: &SessionRecord) -> bool {
        self.session_id == session.id
            && self.owner == session.owner
            && self.control_fencing_token == session.control_fencing_token
            && self.token_revision == session.token_revision
            && self.protocol == session.protocol
            && session.delivery_mode == DeliveryMode::ServerRemux
            && session.remux_job_id.as_deref() == Some(self.job_id.as_str())
            && !session.state.is_terminal()
            && self.ready.load(Ordering::Acquire)
    }

    fn diagnostic_state(&self, session: &SessionRecord) -> Option<&'static str> {
        if self.owner != session.owner || self.control_fencing_token > session.control_fencing_token
        {
            return None;
        }
        Some(if self.completed.load(Ordering::Acquire) {
            if self.cleanup_succeeded.load(Ordering::Acquire) {
                "completed"
            } else {
                "failed"
            }
        } else if self.matches(session) {
            "ready"
        } else {
            "stopping"
        })
    }

    async fn wait_for_cleanup(&self, timeout: Duration) {
        if self.completed.load(Ordering::Acquire) {
            return;
        }
        let notified = self.completion.notified();
        if self.completed.load(Ordering::Acquire) {
            return;
        }
        let _ = time::timeout(timeout, notified).await;
    }

    fn mark_completed(&self) {
        self.completed.store(true, Ordering::Release);
        self.completion.notify_waiters();
    }

    fn ensure_cleanup_complete(&self) -> Result<(), LiveRemuxError> {
        if self.cleanup_succeeded.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(LiveRemuxError::CleanupIncomplete)
        }
    }
}

impl LiveRemuxService {
    pub fn new(
        repository: Arc<LiveSessionRepository>,
        relay: Arc<LiveRelayService>,
        redactor: Arc<LiveRedactor>,
        limits: LiveRemuxLimits,
        startup_queue_timeout: Duration,
    ) -> Result<Self, LiveRemuxBuildError> {
        let capacity = usize::try_from(limits.max_concurrent)
            .ok()
            .filter(|value| *value > 0)
            .ok_or(LiveRemuxBuildError::InvalidCapacity)?;
        if startup_queue_timeout.is_zero() {
            return Err(LiveRemuxBuildError::InvalidCapacity);
        }
        let temp_root = normalize_temp_root(&limits.temp_root)?;
        let rewriter = HlsRewriter::new(HlsRewriteConfig::default())
            .map_err(|_| LiveRemuxBuildError::Rewriter)?;
        Ok(Self {
            repository,
            relay,
            redactor,
            limits,
            temp_root,
            capacity: Arc::new(Semaphore::new(capacity)),
            sessions: DashMap::new(),
            admission_lock: Mutex::new(()),
            startup_queue_timeout,
            initialized: OnceCell::new(),
            rewriter,
            jobs_started: AtomicU64::new(0),
            jobs_completed: AtomicU64::new(0),
            jobs_failed: AtomicU64::new(0),
            jobs_cancelled: AtomicU64::new(0),
        })
    }

    pub async fn initialize(&self) -> Result<(), LiveRemuxBuildError> {
        self.initialized
            .get_or_try_init(|| async {
                prepare_private_root(&self.temp_root).await?;
                self.cleanup_orphans().await?;
                verify_binary(&self.limits.ffprobe_binary, "ffprobe").await?;
                verify_binary(&self.limits.ffmpeg_binary, "ffmpeg").await?;
                self.pressure_check()
                    .await
                    .map_err(|_| LiveRemuxBuildError::TempRootUnavailable)?;
                Ok(())
            })
            .await
            .map(|_| ())
    }

    pub fn available_capacity(&self) -> usize {
        self.capacity.available_permits()
    }

    pub async fn reconcile_startup(
        self: &Arc<Self>,
        control_fencing_token: i64,
    ) -> Result<LiveRemuxReconciliation, LiveRemuxError> {
        self.repository
            .assert_current_fence(control_fencing_token, Utc::now())
            .await
            .map_err(|_| LiveRemuxError::StaleControlFence)?;
        let sessions = self
            .repository
            .list_active(Utc::now(), 10_000)
            .await
            .map_err(|_| LiveRemuxError::Unavailable)?;
        let mut report = LiveRemuxReconciliation {
            inspected: 0,
            restarted: 0,
            terminated: 0,
        };
        for session in sessions.into_iter().filter(|session| {
            session.delivery_mode == DeliveryMode::ServerRemux
                && session.control_fencing_token == control_fencing_token
        }) {
            report.inspected = report.inspected.saturating_add(1);
            if self.admit_session(&session).await.is_ok() {
                report.restarted = report.restarted.saturating_add(1);
                continue;
            }
            if self
                .repository
                .terminate(
                    session.owner,
                    session.id,
                    session.revision,
                    control_fencing_token,
                    crate::live::session::TerminalReason {
                        state: crate::live::session::SessionState::Failed,
                        error_code: Some("LIVE_REMUX_UNAVAILABLE".to_string()),
                        error_detail_redacted: None,
                    },
                    Utc::now(),
                )
                .await
                .is_ok()
            {
                report.terminated = report.terminated.saturating_add(1);
            }
        }
        Ok(report)
    }

    pub async fn snapshot(&self) -> LiveRemuxSnapshot {
        LiveRemuxSnapshot {
            active_jobs: self.sessions.len(),
            available_capacity: self.available_capacity(),
            jobs_started: self.jobs_started.load(Ordering::Relaxed),
            jobs_completed: self.jobs_completed.load(Ordering::Relaxed),
            jobs_failed: self.jobs_failed.load(Ordering::Relaxed),
            jobs_cancelled: self.jobs_cancelled.load(Ordering::Relaxed),
            temp_bytes: directory_bytes(self.temp_root.clone())
                .await
                .unwrap_or(u64::MAX),
        }
    }

    pub fn diagnostics_for(&self, session: &SessionRecord) -> Option<LiveRemuxJobDiagnostics> {
        let job = self.sessions.get(&session.id)?.value().clone();
        let state = job.diagnostic_state(session)?;
        let stderr_tail = job
            .stderr_ring
            .lock()
            .ok()
            .map(|bytes| bytes.iter().copied().collect::<Vec<_>>())
            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
            .filter(|value| !value.is_empty())
            .map(|value| self.redactor.redact_bounded(&value, 4_096).into_string());
        Some(LiveRemuxJobDiagnostics {
            profile: job.profile.as_str(),
            state,
            stderr_tail,
        })
    }

    pub async fn admit_session(
        self: &Arc<Self>,
        session: &SessionRecord,
    ) -> Result<(), LiveRemuxError> {
        let result = self.admit_session_inner(session).await;
        if matches!(result, Err(LiveRemuxError::CapacityExhausted)) {
            crate::live::metrics::ADMISSION_REJECTIONS
                .with_label_values(&["remux", "capacity_exhausted"])
                .inc();
        }
        result
    }

    async fn admit_session_inner(
        self: &Arc<Self>,
        session: &SessionRecord,
    ) -> Result<(), LiveRemuxError> {
        if session.delivery_mode != DeliveryMode::ServerRemux || session.state.is_terminal() {
            return Err(LiveRemuxError::SessionMismatch);
        }
        let profile = CopyRemuxProfile::for_protocol(session.protocol)
            .ok_or(LiveRemuxError::ProtocolUnsupported)?;
        let source = self.relay.prepare_remux_source(session).await?;
        let _guard = time::timeout(self.startup_queue_timeout, self.admission_lock.lock())
            .await
            .map_err(|_| LiveRemuxError::CapacityExhausted)?;
        if let Some(existing) = self.sessions.get(&session.id) {
            if existing.matches(session) {
                return Ok(());
            }
            if existing.control_fencing_token > session.control_fencing_token
                || (existing.control_fencing_token == session.control_fencing_token
                    && existing.token_revision > session.token_revision)
                || existing.owner != session.owner
            {
                return Err(LiveRemuxError::StaleControlFence);
            }
        }
        self.cancel_session(session.id).await?;
        if self.sessions.contains_key(&session.id) {
            return Err(LiveRemuxError::Unavailable);
        }
        self.pressure_check().await?;
        let permit = self
            .capacity
            .clone()
            .try_acquire_owned()
            .map_err(|_| LiveRemuxError::CapacityExhausted)?;
        let job_uuid = Uuid::new_v4();
        let job_id = format!("lrj1_{}", job_uuid.simple());
        let directory = self.temp_root.join(format!("{JOB_PREFIX}{job_uuid}"));
        let resources = HlsResourceMap::new(
            session.id,
            session.control_fencing_token,
            HlsResourceLimits::default(),
        )
        .map_err(|_| LiveRemuxError::Unavailable)?;
        create_private_job_dir(&directory).await?;
        let cancellation = CancellationToken::new();
        let adapter = match LiveRemuxAdapter::start(
            self.relay.clone(),
            session.clone(),
            source,
            &cancellation,
        )
        .await
        {
            Ok(adapter) => adapter,
            Err(_) => {
                cleanup_directory(&directory).await;
                return Err(LiveRemuxError::Unavailable);
            }
        };
        if let Err(error) = self.probe(adapter.input_url()).await {
            adapter.stop().await;
            cleanup_directory(&directory).await;
            return Err(error);
        }
        let arguments = ffmpeg_args(
            profile,
            adapter.input_url(),
            &directory,
            self.limits.segment_seconds,
            self.limits.playlist_segments,
            self.limits.delete_threshold,
        )?;
        let mut command = Command::new(&self.limits.ffmpeg_binary);
        command
            .args(&arguments)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        {
            command.process_group(0);
        }
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(_) => {
                adapter.stop().await;
                cleanup_directory(&directory).await;
                return Err(LiveRemuxError::ProcessFailed);
            }
        };
        let Some(process_id) = child.id() else {
            stop_process(
                &mut child,
                Duration::from_secs(self.limits.graceful_stop_seconds),
            )
            .await;
            adapter.stop().await;
            cleanup_directory(&directory).await;
            return Err(LiveRemuxError::ProcessFailed);
        };
        let isolation = match ProcessIsolation::attach(&child) {
            Ok(isolation) => isolation,
            Err(error) => {
                stop_process(
                    &mut child,
                    Duration::from_secs(self.limits.graceful_stop_seconds),
                )
                .await;
                adapter.stop().await;
                cleanup_directory(&directory).await;
                return Err(error);
            }
        };
        if write_marker(&directory, &job_id, session.id, process_id)
            .await
            .is_err()
        {
            stop_process(
                &mut child,
                Duration::from_secs(self.limits.graceful_stop_seconds),
            )
            .await;
            adapter.stop().await;
            cleanup_directory(&directory).await;
            return Err(LiveRemuxError::Unavailable);
        }
        let stderr_ring = Arc::new(std::sync::Mutex::new(VecDeque::with_capacity(
            usize::try_from(self.limits.stderr_ring_bytes).unwrap_or(16_384),
        )));
        let stderr_task = child.stderr.take().map(|stderr| {
            spawn_stderr_reader(
                stderr,
                stderr_ring.clone(),
                usize::try_from(self.limits.stderr_ring_bytes).unwrap_or(16_384),
            )
        });
        let playlist = directory.join("index.m3u8");
        if let Err(error) = wait_for_playlist(
            &mut child,
            &playlist,
            Duration::from_secs(self.limits.startup_timeout_seconds),
        )
        .await
        {
            stop_process(
                &mut child,
                Duration::from_secs(self.limits.graceful_stop_seconds),
            )
            .await;
            if let Some(task) = stderr_task {
                let _ = task.await;
            }
            adapter.stop().await;
            cleanup_directory(&directory).await;
            return Err(error);
        }
        let bound = match self
            .repository
            .bind_remux_job(
                session.owner,
                session.id,
                session.revision,
                session.control_fencing_token,
                &job_id,
                Utc::now(),
            )
            .await
        {
            Ok(bound) => bound,
            Err(_) => {
                stop_process(
                    &mut child,
                    Duration::from_secs(self.limits.graceful_stop_seconds),
                )
                .await;
                if let Some(task) = stderr_task {
                    let _ = task.await;
                }
                adapter.stop().await;
                cleanup_directory(&directory).await;
                return Err(LiveRemuxError::StaleControlFence);
            }
        };
        let job = Arc::new(RemuxJob {
            job_id,
            session_id: session.id,
            owner: session.owner,
            control_fencing_token: session.control_fencing_token,
            token_revision: bound.token_revision,
            protocol: session.protocol,
            profile,
            directory,
            playlist,
            resources: Mutex::new(resources),
            stderr_ring: stderr_ring.clone(),
            cancellation,
            ready: AtomicBool::new(true),
            completed: AtomicBool::new(false),
            cleanup_succeeded: AtomicBool::new(false),
            completion: Notify::new(),
        });
        self.sessions.insert(session.id, job.clone());
        self.jobs_started.fetch_add(1, Ordering::Relaxed);
        crate::live::metrics::REMUX_JOBS_ACTIVE
            .with_label_values(&[profile.as_str()])
            .inc();
        tokio::spawn(run_job(
            Arc::downgrade(self),
            job,
            child,
            adapter,
            permit,
            isolation,
            stderr_ring,
            stderr_task,
        ));
        Ok(())
    }

    async fn probe(&self, input_url: &str) -> Result<(), LiveRemuxError> {
        let arguments = ffprobe_args(input_url)?;
        let mut command = Command::new(&self.limits.ffprobe_binary);
        command
            .args(arguments)
            .stdin(Stdio::null())
            .stderr(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true);
        let output = time::timeout(
            Duration::from_secs(self.limits.probe_timeout_seconds),
            command.output(),
        )
        .await
        .map_err(|_| LiveRemuxError::ProbeRejected)?
        .map_err(|_| LiveRemuxError::ProcessFailed)?;
        if !output.status.success() || output.stdout.len() > 65_536 {
            let stderr = self
                .redactor
                .redact_bounded(&String::from_utf8_lossy(&output.stderr), 4_096);
            tracing::warn!(
                exit_status = %output.status,
                stderr = %stderr,
                "Live copy-remux probe rejected the loopback input"
            );
            return Err(LiveRemuxError::ProbeRejected);
        }
        if let Err(error) = parse_probe(&output.stdout) {
            tracing::warn!(error = ?error, "Live copy-remux probe metadata was rejected");
            return Err(error.into());
        }
        Ok(())
    }

    pub async fn hls_manifest(
        &self,
        session: &SessionRecord,
    ) -> Result<LiveRemuxPayload, LiveRemuxError> {
        let job = self.current_job(session).await?;
        let body = read_bounded(&job.playlist, MAX_PLAYLIST_BYTES).await?;
        let parent_url = reqwest::Url::parse(&format!(
            "https://live-remux.invalid/{}/index.m3u8",
            job.job_id
        ))
        .map_err(|_| LiveRemuxError::OutputUnhealthy)?;
        let route_base = format!("/api/v1/live/sessions/{}/delivery/hls", session.id);
        let scope = HlsManifestScope::from_stable_key(job.job_id.as_bytes())
            .map_err(|_| LiveRemuxError::OutputUnhealthy)?;
        let mut resources = job.resources.lock().await;
        let rewritten = self
            .rewriter
            .rewrite_scoped_with_validator(
                &mut resources,
                session.control_fencing_token,
                scope,
                &parent_url,
                &route_base,
                &body,
                |descriptor| {
                    validate_output_descriptor(&job, descriptor)
                        .map(|_| ())
                        .map_err(|_| crate::live::relay::hls::HlsRewriteError::InvalidResourceUri)
                },
            )
            .map_err(|_| LiveRemuxError::OutputUnhealthy)?;
        let bytes = rewritten.body().to_vec();
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            "application/vnd.apple.mpegurl"
                .parse()
                .expect("static HLS content type"),
        );
        headers.insert(
            CONTENT_LENGTH,
            bytes
                .len()
                .to_string()
                .parse()
                .map_err(|_| LiveRemuxError::Unavailable)?,
        );
        headers.insert(
            ETAG,
            format!("\"{}\"", blake3::hash(&bytes).to_hex())
                .parse()
                .map_err(|_| LiveRemuxError::Unavailable)?,
        );
        Ok(LiveRemuxPayload {
            status: StatusCode::OK,
            headers,
            body: LiveRemuxPayloadBody::Bytes(bytes),
        })
    }

    pub async fn hls_resource(
        &self,
        session: &SessionRecord,
        resource_id: &HlsResourceId,
        client_range: Option<&str>,
    ) -> Result<LiveRemuxPayload, LiveRemuxError> {
        let job = self.current_job(session).await?;
        let descriptor = job
            .resources
            .lock()
            .await
            .resolve(resource_id, session.control_fencing_token)
            .map_err(|_| LiveRemuxError::ResourceExpired)?;
        if !matches!(
            descriptor.kind(),
            HlsResourceKind::MediaSegment | HlsResourceKind::InitializationSegment
        ) {
            return Err(LiveRemuxError::ResourceKindMismatch);
        }
        let path = validate_output_descriptor(&job, &descriptor)?;
        let metadata = fs::symlink_metadata(&path)
            .await
            .map_err(|_| LiveRemuxError::ResourceExpired)?;
        if !metadata.file_type().is_file()
            || metadata.file_type().is_symlink()
            || metadata.len() == 0
        {
            return Err(LiveRemuxError::ResourceExpired);
        }
        let mut file = open_output_file(path).await?;
        let file_length = metadata.len();
        let (status, start, length, content_range) = match client_range {
            None => (StatusCode::OK, 0, file_length, None),
            Some(value) => {
                let ranges = http_range::HttpRange::parse(value, file_length)
                    .map_err(|_| LiveRemuxError::RangeRejected)?;
                if ranges.len() != 1 || ranges[0].length == 0 {
                    return Err(LiveRemuxError::RangeRejected);
                }
                let range = ranges[0];
                let end = range
                    .start
                    .checked_add(range.length)
                    .and_then(|value| value.checked_sub(1))
                    .filter(|end| *end < file_length)
                    .ok_or(LiveRemuxError::RangeRejected)?;
                (
                    StatusCode::PARTIAL_CONTENT,
                    range.start,
                    range.length,
                    Some(format!("bytes {}-{end}/{file_length}", range.start)),
                )
            }
        };
        file.seek(SeekFrom::Start(start))
            .await
            .map_err(|_| LiveRemuxError::ResourceExpired)?;
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "video/mp2t".parse().expect("static TS type"));
        headers.insert(
            CONTENT_LENGTH,
            length
                .to_string()
                .parse()
                .map_err(|_| LiveRemuxError::Unavailable)?,
        );
        headers.insert(ACCEPT_RANGES, "bytes".parse().expect("static range value"));
        if let Some(content_range) = content_range {
            headers.insert(
                CONTENT_RANGE,
                content_range
                    .parse()
                    .map_err(|_| LiveRemuxError::Unavailable)?,
            );
        }
        Ok(LiveRemuxPayload {
            status,
            headers,
            body: LiveRemuxPayloadBody::File { file, length },
        })
    }

    async fn current_job(&self, session: &SessionRecord) -> Result<Arc<RemuxJob>, LiveRemuxError> {
        let job = self
            .sessions
            .get(&session.id)
            .map(|entry| entry.value().clone())
            .ok_or(LiveRemuxError::Unavailable)?;
        if !job.matches(session) {
            return Err(LiveRemuxError::SessionMismatch);
        }
        self.validate_job_authority(&job).await?;
        Ok(job)
    }

    async fn validate_job_authority(&self, job: &RemuxJob) -> Result<(), LiveRemuxError> {
        self.repository
            .assert_current_fence(job.control_fencing_token, Utc::now())
            .await
            .map_err(|_| LiveRemuxError::StaleControlFence)?;
        let session = self
            .repository
            .get_owned(job.owner, job.session_id)
            .await
            .map_err(|_| LiveRemuxError::Unavailable)?
            .ok_or(LiveRemuxError::SessionMismatch)?;
        if !job.matches(&session) {
            return Err(LiveRemuxError::SessionMismatch);
        }
        Ok(())
    }

    pub async fn end_session(&self, session_id: Uuid) -> Result<(), LiveRemuxError> {
        let job = self
            .sessions
            .get(&session_id)
            .map(|entry| entry.value().clone());
        if let Some(job) = job {
            job.ready.store(false, Ordering::Release);
            job.cancellation.cancel();
            job.wait_for_cleanup(Duration::from_secs(
                self.limits.graceful_stop_seconds.saturating_add(2),
            ))
            .await;
            self.finish_job_cleanup(&job).await;
            job.ensure_cleanup_complete()?;
        }
        Ok(())
    }

    async fn cancel_session(&self, session_id: Uuid) -> Result<(), LiveRemuxError> {
        self.end_session(session_id).await
    }

    pub async fn cancel_all(&self) {
        let jobs = self
            .sessions
            .iter()
            .map(|entry| entry.value().clone())
            .collect::<Vec<_>>();
        for job in &jobs {
            job.ready.store(false, Ordering::Release);
            job.cancellation.cancel();
        }
        for job in jobs {
            job.wait_for_cleanup(Duration::from_secs(
                self.limits.graceful_stop_seconds.saturating_add(2),
            ))
            .await;
            self.finish_job_cleanup(&job).await;
            if !job.cleanup_succeeded.load(Ordering::Acquire) {
                tracing::error!(
                    session_id = %job.session_id,
                    "Live remux cleanup remained incomplete during cancellation"
                );
            }
        }
    }

    async fn finish_job_cleanup(&self, job: &Arc<RemuxJob>) {
        if !job.completed.load(Ordering::Acquire) {
            return;
        }
        if !job.cleanup_succeeded.load(Ordering::Acquire) {
            let cleaned = cleanup_directory(&job.directory).await;
            crate::live::metrics::CLEANUP
                .with_label_values(&["remux_job", if cleaned { "completed" } else { "failed" }])
                .inc();
            job.cleanup_succeeded.store(cleaned, Ordering::Release);
        }
        if job.cleanup_succeeded.load(Ordering::Acquire)
            && self
                .sessions
                .get(&job.session_id)
                .is_some_and(|entry| entry.job_id == job.job_id)
        {
            self.sessions.remove(&job.session_id);
        }
    }

    pub async fn reap_stale(&self) {
        let jobs = self
            .sessions
            .iter()
            .map(|entry| entry.value().clone())
            .collect::<Vec<_>>();
        for job in jobs {
            if self.validate_job_authority(&job).await.is_err() {
                if let Err(error) = self.end_session(job.session_id).await {
                    tracing::error!(
                        session_id = %job.session_id,
                        error = %error,
                        "Live remux stale-job cleanup failed"
                    );
                }
            }
        }
    }

    async fn pressure_check(&self) -> Result<(), LiveRemuxError> {
        let bytes = directory_bytes(self.temp_root.clone()).await?;
        if bytes >= self.limits.temp_budget_bytes {
            return Err(LiveRemuxError::DiskPressure);
        }
        if available_space(&self.temp_root).await? < self.limits.minimum_free_bytes {
            return Err(LiveRemuxError::DiskPressure);
        }
        if !file_descriptor_headroom().await {
            return Err(LiveRemuxError::Unavailable);
        }
        Ok(())
    }

    async fn cleanup_orphans(&self) -> Result<(), LiveRemuxBuildError> {
        cleanup_orphans_in(&self.temp_root).await
    }
}

async fn cleanup_orphans_in(temp_root: &Path) -> Result<(), LiveRemuxBuildError> {
    let mut entries = fs::read_dir(temp_root)
        .await
        .map_err(|_| LiveRemuxBuildError::TempRootUnavailable)?;
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|_| LiveRemuxBuildError::TempRootUnavailable)?
    {
        let file_type = entry
            .file_type()
            .await
            .map_err(|_| LiveRemuxBuildError::UnsafeTempRoot)?;
        let name = entry.file_name();
        let name = name.to_str().ok_or(LiveRemuxBuildError::UnsafeTempRoot)?;
        if file_type.is_symlink() || !file_type.is_dir() || !valid_job_directory_name(name) {
            return Err(LiveRemuxBuildError::UnsafeTempRoot);
        }
        let directory = entry.path();
        let marked_process = match read_marker(&directory).await {
            Ok(marker) if process_matches(marker.process_id, &directory).await => {
                Some(marker.process_id)
            }
            _ => None,
        };
        if let Some(process_id) = match marked_process {
            Some(process_id) => Some(process_id),
            None => find_orphan_process(&directory).await,
        } {
            terminate_orphan(process_id).await;
        }
        fs::remove_dir_all(&directory)
            .await
            .map_err(|_| LiveRemuxBuildError::TempRootUnavailable)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_job(
    service: Weak<LiveRemuxService>,
    job: Arc<RemuxJob>,
    mut child: Child,
    adapter: LiveRemuxAdapter,
    permit: OwnedSemaphorePermit,
    isolation: ProcessIsolation,
    stderr_ring: Arc<std::sync::Mutex<VecDeque<u8>>>,
    stderr_task: Option<JoinHandle<()>>,
) {
    let Some(service_snapshot) = service.upgrade() else {
        job.cancellation.cancel();
        stop_process(&mut child, Duration::from_secs(3)).await;
        if let Some(task) = stderr_task {
            let _ = task.await;
        }
        adapter.stop().await;
        let cleaned = cleanup_directory(&job.directory).await;
        crate::live::metrics::REMUX_JOBS_ACTIVE
            .with_label_values(&[job.profile.as_str()])
            .dec();
        crate::live::metrics::REMUX_JOBS
            .with_label_values(&[job.profile.as_str(), "cancelled"])
            .inc();
        crate::live::metrics::CLEANUP
            .with_label_values(&["remux_job", if cleaned { "completed" } else { "failed" }])
            .inc();
        drop(isolation);
        drop(permit);
        job.cleanup_succeeded.store(cleaned, Ordering::Release);
        job.mark_completed();
        return;
    };
    let health_interval = Duration::from_secs(1);
    let no_output_timeout = Duration::from_secs(service_snapshot.limits.no_output_timeout_seconds);
    let per_job_budget = service_snapshot
        .limits
        .temp_budget_bytes
        .checked_div(u64::from(service_snapshot.limits.max_concurrent))
        .unwrap_or(service_snapshot.limits.temp_budget_bytes);
    let graceful_stop = Duration::from_secs(service_snapshot.limits.graceful_stop_seconds);
    drop(service_snapshot);
    let mut last_hash = None;
    let mut last_progress = Instant::now();
    let outcome = loop {
        tokio::select! {
            _ = job.cancellation.cancelled() => {
                stop_process(&mut child, graceful_stop).await;
                break JobOutcome::Cancelled;
            }
            status = child.wait() => {
                break if status.is_ok_and(|status| status.success()) {
                    JobOutcome::Completed
                } else {
                    JobOutcome::Failed
                };
            }
            _ = time::sleep(health_interval) => {
                let Some(service) = service.upgrade() else {
                    stop_process(&mut child, graceful_stop).await;
                    break JobOutcome::Cancelled;
                };
                if service.validate_job_authority(&job).await.is_err() {
                    stop_process(&mut child, graceful_stop).await;
                    break JobOutcome::Cancelled;
                }
                let over_budget = match directory_bytes(job.directory.clone()).await {
                    Ok(bytes) => bytes > per_job_budget,
                    Err(_) => true,
                };
                let disk_pressure = match available_space(&job.directory).await {
                    Ok(bytes) => bytes < service.limits.minimum_free_bytes,
                    Err(_) => true,
                };
                if over_budget || disk_pressure {
                    stop_process(&mut child, graceful_stop).await;
                    break JobOutcome::Failed;
                }
                match playlist_fingerprint(&job.playlist).await {
                    Ok(hash) if last_hash != Some(hash) => {
                        last_hash = Some(hash);
                        last_progress = Instant::now();
                    }
                    Ok(_) if last_progress.elapsed() > no_output_timeout => {
                        stop_process(&mut child, graceful_stop).await;
                        break JobOutcome::Failed;
                    }
                    Err(_) => {
                        stop_process(&mut child, graceful_stop).await;
                        break JobOutcome::Failed;
                    }
                    _ => {}
                }
            }
        }
    };
    job.ready.store(false, Ordering::Release);
    if let Some(task) = stderr_task {
        let _ = task.await;
    }
    adapter.stop().await;
    if let Some(service) = service.upgrade() {
        let _ = service
            .repository
            .clear_remux_job(
                job.owner,
                job.session_id,
                job.control_fencing_token,
                &job.job_id,
                Utc::now(),
            )
            .await;
        match outcome {
            JobOutcome::Completed => {
                service.jobs_completed.fetch_add(1, Ordering::Relaxed);
            }
            JobOutcome::Failed => {
                service.jobs_failed.fetch_add(1, Ordering::Relaxed);
            }
            JobOutcome::Cancelled => {
                service.jobs_cancelled.fetch_add(1, Ordering::Relaxed);
            }
        }
        if outcome == JobOutcome::Failed {
            let stderr = stderr_ring
                .lock()
                .map(|mut ring| String::from_utf8_lossy(ring.make_contiguous()).into_owned())
                .unwrap_or_default();
            let stderr = service.redactor.redact_bounded(&stderr, 4_096);
            tracing::warn!(
                session_id = %job.session_id,
                profile = job.profile.as_str(),
                stderr = %stderr,
                "Live copy-remux job failed"
            );
        }
    }
    crate::live::metrics::REMUX_JOBS_ACTIVE
        .with_label_values(&[job.profile.as_str()])
        .dec();
    crate::live::metrics::REMUX_JOBS
        .with_label_values(&[job.profile.as_str(), outcome.as_str()])
        .inc();
    let cleaned = cleanup_directory(&job.directory).await;
    crate::live::metrics::CLEANUP
        .with_label_values(&["remux_job", if cleaned { "completed" } else { "failed" }])
        .inc();
    drop(isolation);
    drop(permit);
    job.cleanup_succeeded.store(cleaned, Ordering::Release);
    job.mark_completed();
    if let Some(service) = service.upgrade() {
        if cleaned
            && service
                .sessions
                .get(&job.session_id)
                .is_some_and(|entry| entry.job_id == job.job_id)
        {
            service.sessions.remove(&job.session_id);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JobOutcome {
    Completed,
    Failed,
    Cancelled,
}

impl JobOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

async fn wait_for_playlist(
    child: &mut Child,
    playlist: &Path,
    timeout: Duration,
) -> Result<(), LiveRemuxError> {
    let started = Instant::now();
    loop {
        if playlist_fingerprint(playlist).await.is_ok() {
            return Ok(());
        }
        if child
            .try_wait()
            .map_err(|_| LiveRemuxError::ProcessFailed)?
            .is_some()
        {
            return Err(LiveRemuxError::ProcessFailed);
        }
        if started.elapsed() >= timeout {
            return Err(LiveRemuxError::StartupTimeout);
        }
        time::sleep(Duration::from_millis(100)).await;
    }
}

async fn playlist_fingerprint(path: &Path) -> Result<[u8; 32], LiveRemuxError> {
    let body = read_bounded(path, MAX_PLAYLIST_BYTES).await?;
    let text = std::str::from_utf8(&body).map_err(|_| LiveRemuxError::OutputUnhealthy)?;
    if !text.starts_with("#EXTM3U\n")
        || text.contains("#EXT-X-ENDLIST")
        || !text
            .lines()
            .any(|line| line.starts_with("segment-") && line.ends_with(".ts"))
    {
        return Err(LiveRemuxError::OutputUnhealthy);
    }
    Ok(*blake3::hash(&body).as_bytes())
}

async fn read_bounded(path: &Path, maximum: u64) -> Result<Vec<u8>, LiveRemuxError> {
    let metadata = fs::symlink_metadata(path)
        .await
        .map_err(|_| LiveRemuxError::OutputUnhealthy)?;
    if !metadata.file_type().is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() == 0
        || metadata.len() > maximum
    {
        return Err(LiveRemuxError::OutputUnhealthy);
    }
    fs::read(path)
        .await
        .map_err(|_| LiveRemuxError::OutputUnhealthy)
}

fn validate_output_descriptor(
    job: &RemuxJob,
    descriptor: &crate::live::relay::hls::HlsResourceDescriptor,
) -> Result<PathBuf, LiveRemuxError> {
    if !matches!(
        descriptor.kind(),
        HlsResourceKind::MediaSegment | HlsResourceKind::InitializationSegment
    ) {
        return Err(LiveRemuxError::ResourceKindMismatch);
    }
    let url = descriptor.url();
    if url.scheme() != "https"
        || url.host_str() != Some("live-remux.invalid")
        || url.port().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(LiveRemuxError::ResourceExpired);
    }
    let mut components = url.path_segments().ok_or(LiveRemuxError::ResourceExpired)?;
    if components.next() != Some(job.job_id.as_str()) {
        return Err(LiveRemuxError::ResourceExpired);
    }
    let name = components.next().ok_or(LiveRemuxError::ResourceExpired)?;
    if components.next().is_some() || !valid_segment_name(name) {
        return Err(LiveRemuxError::ResourceExpired);
    }
    Ok(job.directory.join(name))
}

fn valid_segment_name(name: &str) -> bool {
    name.strip_prefix("segment-").is_some_and(|suffix| {
        suffix.strip_suffix(".ts").is_some_and(|digits| {
            digits.len() == 10 && digits.bytes().all(|byte| byte.is_ascii_digit())
        })
    })
}

async fn open_output_file(path: PathBuf) -> Result<fs::File, LiveRemuxError> {
    tokio::task::spawn_blocking(move || {
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.custom_flags(libc::O_NOFOLLOW);
        }
        options.open(path).map(fs::File::from_std)
    })
    .await
    .map_err(|_| LiveRemuxError::ResourceExpired)?
    .map_err(|_| LiveRemuxError::ResourceExpired)
}

fn spawn_stderr_reader(
    mut stderr: tokio::process::ChildStderr,
    ring: Arc<std::sync::Mutex<VecDeque<u8>>>,
    maximum: usize,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut buffer = [0_u8; 1_024];
        loop {
            match stderr.read(&mut buffer).await {
                Ok(0) | Err(_) => return,
                Ok(read) => {
                    if let Ok(mut ring) = ring.lock() {
                        for byte in &buffer[..read] {
                            if ring.len() == maximum {
                                ring.pop_front();
                            }
                            ring.push_back(*byte);
                        }
                    }
                }
            }
        }
    })
}

async fn stop_process(child: &mut Child, grace: Duration) {
    if child.try_wait().ok().flatten().is_some() {
        return;
    }
    #[cfg(unix)]
    if let Some(process_id) = child.id() {
        let _ = Command::new("kill")
            .arg("-TERM")
            .arg(format!("-{process_id}"))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await;
        if time::timeout(grace, child.wait()).await.is_ok() {
            return;
        }
        let _ = Command::new("kill")
            .arg("-KILL")
            .arg(format!("-{process_id}"))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await;
        let _ = child.wait().await;
        return;
    }
    let _ = child.kill().await;
    let _ = child.wait().await;
}

struct ProcessIsolation {
    #[cfg(windows)]
    _job_object: Option<WindowsJobObject>,
}

impl ProcessIsolation {
    fn attach(_child: &Child) -> Result<Self, LiveRemuxError> {
        Ok(Self {
            #[cfg(windows)]
            _job_object: Some(
                WindowsJobObject::assign(_child).map_err(|_| LiveRemuxError::ProcessFailed)?,
            ),
        })
    }
}

#[cfg(windows)]
struct WindowsJobObject {
    handle: windows_sys::Win32::Foundation::HANDLE,
}

#[cfg(windows)]
unsafe impl Send for WindowsJobObject {}

#[cfg(windows)]
impl WindowsJobObject {
    fn assign(child: &Child) -> std::io::Result<Self> {
        use std::{ffi::c_void, mem, os::windows::io::AsRawHandle as _, ptr};
        use windows_sys::Win32::{
            Foundation::{CloseHandle, HANDLE},
            System::JobObjects::{
                AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
                JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
                SetInformationJobObject,
            },
        };
        let handle = unsafe { CreateJobObjectW(ptr::null(), ptr::null()) };
        if handle == 0 {
            return Err(std::io::Error::last_os_error());
        }
        let mut info = unsafe { mem::zeroed::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() };
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        if unsafe {
            SetInformationJobObject(
                handle,
                JobObjectExtendedLimitInformation,
                &info as *const _ as *const c_void,
                mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        } == 0
        {
            let error = std::io::Error::last_os_error();
            unsafe { CloseHandle(handle) };
            return Err(error);
        }
        if unsafe { AssignProcessToJobObject(handle, child.as_raw_handle() as HANDLE) } == 0 {
            let error = std::io::Error::last_os_error();
            unsafe { CloseHandle(handle) };
            return Err(error);
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

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct JobMarker {
    version: u32,
    job_id: String,
    session_id: Uuid,
    process_id: u32,
}

async fn write_marker(
    directory: &Path,
    job_id: &str,
    session_id: Uuid,
    process_id: u32,
) -> Result<(), LiveRemuxError> {
    let body = serde_json::to_vec(&JobMarker {
        version: 1,
        job_id: job_id.to_string(),
        session_id,
        process_id,
    })
    .map_err(|_| LiveRemuxError::Unavailable)?;
    let path = directory.join(MARKER_FILE);
    tokio::task::spawn_blocking(move || {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        use std::io::Write as _;
        let mut file = options.open(path)?;
        file.write_all(&body)?;
        file.sync_all()
    })
    .await
    .map_err(|_| LiveRemuxError::Unavailable)?
    .map_err(|_| LiveRemuxError::Unavailable)
}

async fn read_marker(directory: &Path) -> Result<JobMarker, LiveRemuxBuildError> {
    let path = directory.join(MARKER_FILE);
    let metadata = fs::symlink_metadata(&path)
        .await
        .map_err(|_| LiveRemuxBuildError::UnsafeTempRoot)?;
    if !metadata.file_type().is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() > 4_096
    {
        return Err(LiveRemuxBuildError::UnsafeTempRoot);
    }
    let body = fs::read(path)
        .await
        .map_err(|_| LiveRemuxBuildError::UnsafeTempRoot)?;
    let marker: JobMarker =
        serde_json::from_slice(&body).map_err(|_| LiveRemuxBuildError::UnsafeTempRoot)?;
    if marker.version != 1 || marker.process_id == 0 || !valid_job_id(&marker.job_id) {
        return Err(LiveRemuxBuildError::UnsafeTempRoot);
    }
    Ok(marker)
}

async fn prepare_private_root(path: &Path) -> Result<(), LiveRemuxBuildError> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        if path.exists() {
            let metadata = std::fs::symlink_metadata(&path)?;
            if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
                return Err(std::io::Error::other("unsafe Live remux temp root"));
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                if metadata.permissions().mode() & 0o077 != 0 {
                    return Err(std::io::Error::other(
                        "Live remux temp root permissions are not private",
                    ));
                }
            }
        } else {
            let mut builder = std::fs::DirBuilder::new();
            builder.recursive(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt as _;
                builder.mode(0o700);
            }
            builder.create(&path)?;
        }
        Ok::<_, std::io::Error>(())
    })
    .await
    .map_err(|_| LiveRemuxBuildError::TempRootUnavailable)?
    .map_err(|_| LiveRemuxBuildError::TempRootUnavailable)
}

fn normalize_temp_root(configured: &str) -> Result<PathBuf, LiveRemuxBuildError> {
    use std::path::Component;

    if configured.trim().is_empty() || configured.chars().any(char::is_control) {
        return Err(LiveRemuxBuildError::InvalidTempRoot);
    }
    let configured = PathBuf::from(configured);
    if configured.as_os_str().is_empty()
        || configured
            .components()
            .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(LiveRemuxBuildError::InvalidTempRoot);
    }
    let path = if configured.is_absolute() {
        configured
    } else {
        std::env::current_dir()
            .map_err(|_| LiveRemuxBuildError::InvalidTempRoot)?
            .join(configured)
    };
    if path.file_name().is_none() || path.parent().is_none() {
        return Err(LiveRemuxBuildError::InvalidTempRoot);
    }
    Ok(path)
}

async fn create_private_job_dir(path: &Path) -> Result<(), LiveRemuxError> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut builder = std::fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt as _;
            builder.mode(0o700);
        }
        builder.create(path)
    })
    .await
    .map_err(|_| LiveRemuxError::Unavailable)?
    .map_err(|_| LiveRemuxError::Unavailable)
}

async fn verify_binary(binary: &str, expected: &str) -> Result<(), LiveRemuxBuildError> {
    let mut command = Command::new(binary);
    command
        .arg("-version")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let output = time::timeout(Duration::from_secs(5), command.output())
        .await
        .map_err(|_| LiveRemuxBuildError::RuntimeProbeFailed)?
        .map_err(|_| LiveRemuxBuildError::BinaryUnavailable)?;
    if !output.status.success()
        || output.stdout.len() > 65_536
        || !String::from_utf8_lossy(&output.stdout)
            .to_ascii_lowercase()
            .starts_with(expected)
    {
        return Err(LiveRemuxBuildError::RuntimeProbeFailed);
    }
    Ok(())
}

async fn cleanup_directory(path: &Path) -> bool {
    for attempt in 0..3 {
        match fs::remove_dir_all(path).await {
            Ok(()) => return true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return true,
            Err(_) if attempt < 2 => time::sleep(Duration::from_millis(50)).await,
            Err(_) => {
                tracing::error!("Live remux temporary directory cleanup failed");
                return false;
            }
        }
    }
    false
}

async fn directory_bytes(path: PathBuf) -> Result<u64, LiveRemuxError> {
    tokio::task::spawn_blocking(move || directory_bytes_sync(&path))
        .await
        .map_err(|_| LiveRemuxError::Unavailable)?
        .map_err(|_| LiveRemuxError::Unavailable)
}

fn directory_bytes_sync(path: &Path) -> std::io::Result<u64> {
    let mut total = 0_u64;
    let mut pending = vec![path.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(directory)? {
            let entry = entry?;
            let metadata = std::fs::symlink_metadata(entry.path())?;
            if metadata.file_type().is_symlink() {
                return Err(std::io::Error::other("symlink in Live remux root"));
            }
            if metadata.is_dir() {
                pending.push(entry.path());
            } else if metadata.is_file() {
                total = total
                    .checked_add(metadata.len())
                    .ok_or_else(|| std::io::Error::other("Live remux byte overflow"))?;
            }
        }
    }
    Ok(total)
}

async fn available_space(path: &Path) -> Result<u64, LiveRemuxError> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || available_space_sync(&path))
        .await
        .map_err(|_| LiveRemuxError::Unavailable)?
}

#[cfg(unix)]
fn available_space_sync(path: &Path) -> Result<u64, LiveRemuxError> {
    use std::{ffi::CString, os::unix::ffi::OsStrExt as _};
    let path =
        CString::new(path.as_os_str().as_bytes()).map_err(|_| LiveRemuxError::Unavailable)?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    if unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) } != 0 {
        return Err(LiveRemuxError::Unavailable);
    }
    let stat = unsafe { stat.assume_init() };
    Ok(u64::from(stat.f_bavail).saturating_mul(u64::from(stat.f_frsize)))
}

#[cfg(not(unix))]
fn available_space_sync(_path: &Path) -> Result<u64, LiveRemuxError> {
    Ok(u64::MAX)
}

async fn file_descriptor_headroom() -> bool {
    #[cfg(unix)]
    {
        tokio::task::spawn_blocking(|| {
            let mut limit = std::mem::MaybeUninit::<libc::rlimit>::uninit();
            if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limit.as_mut_ptr()) } != 0 {
                return false;
            }
            let limit = unsafe { limit.assume_init() }.rlim_cur;
            let open = std::fs::read_dir("/proc/self/fd")
                .or_else(|_| std::fs::read_dir("/dev/fd"))
                .ok()
                .map(|entries| entries.filter_map(Result::ok).count() as u64)
                .unwrap_or(0);
            limit == libc::RLIM_INFINITY || limit.saturating_sub(open) >= 64
        })
        .await
        .unwrap_or(false)
    }
    #[cfg(not(unix))]
    {
        true
    }
}

fn valid_job_directory_name(name: &str) -> bool {
    name.strip_prefix(JOB_PREFIX)
        .and_then(|value| Uuid::parse_str(value).ok())
        .is_some()
}

fn valid_job_id(value: &str) -> bool {
    value.strip_prefix("lrj1_").is_some_and(|suffix| {
        suffix.len() == 32
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

async fn process_matches(process_id: u32, directory: &Path) -> bool {
    #[cfg(target_os = "linux")]
    {
        let path = format!("/proc/{process_id}/cmdline");
        return fs::read(path).await.is_ok_and(|body| {
            let command = String::from_utf8_lossy(&body);
            command.contains("ffmpeg") && command.contains(directory.to_string_lossy().as_ref())
        });
    }
    #[cfg(all(unix, not(target_os = "linux")))]
    {
        let output = Command::new("ps")
            .args(["-p", &process_id.to_string(), "-o", "command="])
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .output()
            .await;
        return output.is_ok_and(|output| {
            let command = String::from_utf8_lossy(&output.stdout);
            command.contains("ffmpeg") && command.contains(directory.to_string_lossy().as_ref())
        });
    }
    #[cfg(not(unix))]
    {
        let _ = (process_id, directory);
        false
    }
}

async fn find_orphan_process(directory: &Path) -> Option<u32> {
    #[cfg(target_os = "linux")]
    {
        let mut entries = fs::read_dir("/proc").await.ok()?;
        while let Ok(Some(entry)) = entries.next_entry().await {
            let Some(process_id) = entry
                .file_name()
                .to_str()
                .and_then(|value| value.parse::<u32>().ok())
            else {
                continue;
            };
            if process_matches(process_id, directory).await {
                return Some(process_id);
            }
        }
        None
    }
    #[cfg(all(unix, not(target_os = "linux")))]
    {
        let output = Command::new("ps")
            .args(["-ax", "-o", "pid=,command="])
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .output()
            .await
            .ok()?;
        let directory = directory.to_string_lossy();
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .find_map(|line| {
                let line = line.trim_start();
                let (process_id, command) = line.split_once(char::is_whitespace)?;
                (command.contains("ffmpeg") && command.contains(directory.as_ref()))
                    .then(|| process_id.parse::<u32>().ok())
                    .flatten()
            })
    }
    #[cfg(not(unix))]
    {
        let _ = directory;
        None
    }
}

async fn terminate_orphan(process_id: u32) {
    #[cfg(unix)]
    {
        let _ = Command::new("kill")
            .args(["-TERM", &format!("-{process_id}")])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await;
        time::sleep(Duration::from_millis(250)).await;
        let _ = Command::new("kill")
            .args(["-KILL", &format!("-{process_id}")])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await;
    }
    #[cfg(not(unix))]
    let _ = process_id;
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    fn diagnostic_session(
        owner: SessionOwner,
        state: crate::live::session::SessionState,
    ) -> SessionRecord {
        let now = Utc::now();
        SessionRecord {
            id: Uuid::new_v4(),
            owner,
            delivery_mode: DeliveryMode::ServerRemux,
            protocol: SessionProtocol::Dash,
            state,
            revision: 1,
            token_revision: 2,
            control_fencing_token: 3,
            source_index: 0,
            failover_count: 0,
            refresh_count: 0,
            egress_binding_id: None,
            remux_job_id: Some("lrj1_00000000000000000000000000000000".to_string()),
            created_at: now,
            last_heartbeat_at: now,
            expires_at: now + chrono::Duration::minutes(1),
            hard_expires_at: now + chrono::Duration::minutes(5),
            ended_at: None,
            error_code: None,
            error_detail_redacted: None,
        }
    }

    fn diagnostic_job(session: &SessionRecord) -> RemuxJob {
        RemuxJob {
            job_id: session.remux_job_id.clone().expect("remux job ID"),
            session_id: session.id,
            owner: session.owner,
            control_fencing_token: session.control_fencing_token,
            token_revision: session.token_revision,
            protocol: session.protocol,
            profile: CopyRemuxProfile::DashToHls,
            directory: PathBuf::from("unused-diagnostic-job"),
            playlist: PathBuf::from("unused-diagnostic-playlist"),
            resources: Mutex::new(
                HlsResourceMap::new(
                    session.id,
                    session.control_fencing_token,
                    HlsResourceLimits::default(),
                )
                .expect("resource map"),
            ),
            stderr_ring: Arc::new(std::sync::Mutex::new(VecDeque::new())),
            cancellation: CancellationToken::new(),
            ready: AtomicBool::new(true),
            completed: AtomicBool::new(false),
            cleanup_succeeded: AtomicBool::new(false),
            completion: Notify::new(),
        }
    }

    #[test]
    fn o11_remux_diagnostics_remain_conservative_until_cleanup_completes() {
        use crate::live::session::SessionState;

        let owner = SessionOwner {
            user_id: Uuid::new_v4(),
            home_id: Uuid::new_v4(),
            profile_id: Uuid::new_v4(),
            account_session_id: Uuid::new_v4(),
            provider_id: Uuid::new_v4(),
        };
        let mut session = diagnostic_session(owner, SessionState::Ready);
        let job = diagnostic_job(&session);
        assert_eq!(job.diagnostic_state(&session), Some("ready"));

        session.state = SessionState::Ended;
        session.remux_job_id = None;
        assert_eq!(job.diagnostic_state(&session), Some("stopping"));

        job.mark_completed();
        assert_eq!(job.diagnostic_state(&session), Some("failed"));
        assert_eq!(
            job.ensure_cleanup_complete(),
            Err(LiveRemuxError::CleanupIncomplete)
        );
        job.cleanup_succeeded.store(true, Ordering::Release);
        assert_eq!(job.diagnostic_state(&session), Some("completed"));
        assert_eq!(job.ensure_cleanup_complete(), Ok(()));

        session.control_fencing_token = 2;
        assert_eq!(job.diagnostic_state(&session), None);
        session.control_fencing_token = 3;
        session.owner.home_id = Uuid::new_v4();
        assert_eq!(job.diagnostic_state(&session), None);
    }

    #[test]
    fn m10_temp_root_normalization_accepts_documented_relative_default_and_rejects_escape() {
        let root = normalize_temp_root("data/live-remux").expect("documented relative root");
        assert!(root.is_absolute());
        assert!(root.ends_with("data/live-remux"));
        assert_eq!(
            normalize_temp_root("../live-remux"),
            Err(LiveRemuxBuildError::InvalidTempRoot)
        );
        assert_eq!(
            normalize_temp_root("/"),
            Err(LiveRemuxBuildError::InvalidTempRoot)
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn m10_existing_temp_root_must_already_be_private() {
        use std::os::unix::fs::PermissionsExt as _;

        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("remux");
        std::fs::create_dir(&root).expect("remux root");
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755))
            .expect("public permissions");
        assert_eq!(
            prepare_private_root(&root).await,
            Err(LiveRemuxBuildError::TempRootUnavailable)
        );
        assert_eq!(
            std::fs::metadata(&root)
                .expect("root metadata")
                .permissions()
                .mode()
                & 0o777,
            0o755
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn m10_startup_cleanup_finds_unmarked_orphan_process_and_removes_directory() {
        use std::os::unix::fs::PermissionsExt as _;

        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path().join("remux");
        std::fs::create_dir(&root).expect("remux root");
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
            .expect("private root");
        let directory = root.join(format!("{JOB_PREFIX}{}", Uuid::new_v4()));
        std::fs::create_dir(&directory).expect("orphan job directory");
        let executable = temporary.path().join("ffmpeg-m10-orphan-test");
        std::fs::write(
            &executable,
            "#!/bin/sh\ntrap 'exit 0' TERM INT\nwhile :; do sleep 1; done\n",
        )
        .expect("orphan fixture");
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700))
            .expect("executable fixture");
        let mut command = Command::new(&executable);
        command.arg(&directory).process_group(0).kill_on_drop(true);
        let mut child = command.spawn().expect("orphan fixture process");
        let process_id = child.id().expect("orphan process ID");
        time::sleep(Duration::from_millis(100)).await;
        assert!(process_matches(process_id, &directory).await);

        cleanup_orphans_in(&root)
            .await
            .expect("startup orphan cleanup");
        let status = time::timeout(Duration::from_secs(2), child.wait())
            .await
            .expect("orphan termination deadline")
            .expect("orphan wait");
        assert!(!status.success());
        assert!(
            fs::read_dir(&root)
                .await
                .expect("root directory")
                .next_entry()
                .await
                .expect("root read")
                .is_none()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn m10_playlist_startup_deadline_escalates_and_reaps_the_process_group() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let playlist = temporary.path().join("missing.m3u8");
        let mut command = Command::new("sh");
        command
            .args(["-c", "trap 'exit 0' TERM INT; while :; do sleep 1; done"])
            .process_group(0)
            .kill_on_drop(true);
        let mut child = command.spawn().expect("deadline fixture process");
        assert_eq!(
            wait_for_playlist(&mut child, &playlist, Duration::from_millis(150)).await,
            Err(LiveRemuxError::StartupTimeout)
        );
        stop_process(&mut child, Duration::from_millis(250)).await;
        assert!(child.try_wait().expect("process state").is_some());
    }
}
