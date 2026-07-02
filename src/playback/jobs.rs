use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::Serialize;
use serde_json::{Value, json};
use sqlx::AnyPool;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt},
    process::{Child, Command},
    sync::{Mutex, OwnedSemaphorePermit, Semaphore},
    time,
};
use uuid::Uuid;

use crate::{
    metrics::PLAYBACK_ADAPTIVE_RENDITION_SWITCHES,
    playback::{
        performance::record_playback_performance_observation,
        plan::{Delivery, HardwareAccelerationPlan, PlaybackMode, PlaybackPlan, StreamAction},
    },
};

use super::{
    ArtifactKind, ArtifactRegistry, HlsOutputLayout, PlaybackArtifact, SubtitleInfo,
    TranscodeHandle, TranscodeParams, detect_text_subtitles, spawn_ffmpeg,
};

pub type HardwareFailureCallback = Arc<dyn Fn() + Send + Sync>;

const DEFAULT_REMUX_JOBS: usize = 8;
const DEFAULT_PARTIAL_TRANSCODE_JOBS: usize = 4;
const DEFAULT_VIDEO_TRANSCODE_JOBS: usize = 2;
const DEFAULT_STARTUP_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_FIRST_SEGMENT_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_STALE_SEGMENT_TIMEOUT: Duration = Duration::from_secs(90);
const DEFAULT_MAX_LOG_BYTES: u64 = 1_048_576;
const DEFAULT_LOG_TAIL_BYTES: u64 = 8_192;
const DEFAULT_MAX_TEMP_DIR_BYTES: u64 = 20 * 1024 * 1024 * 1024;

pub fn playback_temp_root() -> PathBuf {
    std::env::temp_dir().join("elixir")
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PlaybackJobCapacityLimits {
    pub max_hls_jobs: Option<u32>,
    pub max_direct_streams: Option<u32>,
    pub max_video_transcodes: Option<u32>,
    pub max_hardware_transcodes: Option<u32>,
}

impl PlaybackJobCapacityLimits {
    fn hls_jobs(self) -> usize {
        positive_or_default(self.max_hls_jobs, DEFAULT_REMUX_JOBS)
    }

    fn direct_streams(self) -> usize {
        positive_or_default(self.max_direct_streams, DEFAULT_REMUX_JOBS)
    }

    fn partial_transcodes(self) -> usize {
        DEFAULT_PARTIAL_TRANSCODE_JOBS
    }

    fn video_transcodes(self) -> usize {
        positive_or_default(self.max_video_transcodes, DEFAULT_VIDEO_TRANSCODE_JOBS)
    }

    fn hardware_transcodes(self) -> usize {
        positive_or_default(self.max_hardware_transcodes, DEFAULT_VIDEO_TRANSCODE_JOBS)
    }
}

struct StartupQueueGuard {
    counter: Arc<AtomicUsize>,
}

impl StartupQueueGuard {
    fn new(counter: Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        Self { counter }
    }
}

impl Drop for StartupQueueGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

fn positive_or_default(value: Option<u32>, fallback: usize) -> usize {
    value
        .filter(|value| *value > 0)
        .map(|value| value as usize)
        .unwrap_or(fallback)
}

#[derive(Debug, Clone)]
pub struct PlaybackJobPlan {
    pub session_id: Uuid,
    pub media_file_id: String,
    pub media_path: String,
    pub params: TranscodeParams,
    pub playback_plan: Option<Value>,
}

impl PlaybackJobPlan {
    pub fn new(
        session_id: Uuid,
        media_file_id: impl Into<String>,
        media_path: impl Into<String>,
        params: TranscodeParams,
        playback_plan: Option<Value>,
    ) -> Self {
        Self {
            session_id,
            media_file_id: media_file_id.into(),
            media_path: media_path.into(),
            params,
            playback_plan,
        }
    }

    fn parsed_playback_plan(&self) -> Option<PlaybackPlan> {
        self.playback_plan
            .as_ref()
            .and_then(|value| serde_json::from_value(value.clone()).ok())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PlaybackJobState {
    Planned,
    Starting,
    PlaylistReady,
    Running,
    Restarting,
    Stopping,
    Stopped,
    Stalled,
    Failed,
}

impl PlaybackJobState {
    pub fn as_str(self) -> &'static str {
        match self {
            PlaybackJobState::Planned => "planned",
            PlaybackJobState::Starting => "starting",
            PlaybackJobState::PlaylistReady => "playlist_ready",
            PlaybackJobState::Running => "running",
            PlaybackJobState::Restarting => "restarting",
            PlaybackJobState::Stopping => "stopping",
            PlaybackJobState::Stopped => "stopped",
            PlaybackJobState::Stalled => "stalled",
            PlaybackJobState::Failed => "failed",
        }
    }

    fn is_ready(self) -> bool {
        matches!(
            self,
            PlaybackJobState::PlaylistReady | PlaybackJobState::Running
        )
    }
}

#[derive(Debug, Clone)]
pub struct PlaybackJobLimits {
    pub startup_timeout: Duration,
    pub first_segment_timeout: Duration,
    pub stale_segment_timeout: Duration,
    pub max_log_bytes: u64,
    pub log_tail_bytes: u64,
    pub max_temp_dir_bytes: u64,
}

impl Default for PlaybackJobLimits {
    fn default() -> Self {
        Self {
            startup_timeout: DEFAULT_STARTUP_TIMEOUT,
            first_segment_timeout: DEFAULT_FIRST_SEGMENT_TIMEOUT,
            stale_segment_timeout: DEFAULT_STALE_SEGMENT_TIMEOUT,
            max_log_bytes: DEFAULT_MAX_LOG_BYTES,
            log_tail_bytes: DEFAULT_LOG_TAIL_BYTES,
            max_temp_dir_bytes: DEFAULT_MAX_TEMP_DIR_BYTES,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PlaybackJobSnapshot {
    pub session_id: String,
    pub media_file_id: String,
    pub mode: String,
    pub delivery: String,
    pub state: String,
    pub logical_start_seconds: f32,
    pub temp_dir: String,
    pub artifacts: Vec<String>,
    pub process_id: Option<u32>,
    pub process_group_id: Option<u32>,
    pub started_at: Option<String>,
    pub last_progress_at: Option<String>,
    pub last_segment_at: Option<String>,
    pub log_path: String,
    pub error: Option<String>,
    pub error_code: Option<String>,
    pub error_kind: Option<String>,
    pub log_tail: Option<String>,
    pub playback_plan: Option<Value>,
    pub active_rung: Option<Value>,
}

impl PlaybackJobSnapshot {
    pub fn to_json(&self) -> Value {
        serde_json::to_value(self).unwrap_or_else(|_| {
            json!({
                "session_id": self.session_id,
                "state": self.state,
                "error": "job_snapshot_serialization_failed"
            })
        })
    }
}

struct PlaybackJob {
    plan: PlaybackJobPlan,
    state: PlaybackJobState,
    temp_dir: PathBuf,
    artifacts: ArtifactRegistry,
    process_id: Option<u32>,
    process_group_id: Option<u32>,
    child: Option<Child>,
    capacity_permits: Vec<OwnedSemaphorePermit>,
    started_at: Option<DateTime<Utc>>,
    last_progress_at: Option<DateTime<Utc>>,
    last_segment_at: Option<DateTime<Utc>>,
    log_path: PathBuf,
    error: Option<String>,
    error_code: Option<String>,
    error_kind: Option<String>,
    log_tail: Option<String>,
    subtitle_delay_seconds: Option<f64>,
    subtitles: Vec<SubtitleInfo>,
    active_rung_id: Option<String>,
}

#[derive(Debug, Clone)]
struct PlaybackPerformanceObservation {
    envelope_id: String,
    startup_latency_ms: Option<i64>,
    first_segment_latency_ms: Option<i64>,
    realtime_factor_millis: Option<i32>,
    failure_kind: Option<String>,
    fallback_kind: Option<String>,
    output_mode: Option<String>,
}

impl PlaybackJob {
    fn snapshot(&self) -> PlaybackJobSnapshot {
        PlaybackJobSnapshot {
            session_id: self.plan.session_id.to_string(),
            media_file_id: self.plan.media_file_id.clone(),
            mode: playback_mode_name(self.plan.params.mode).to_string(),
            delivery: delivery_name(self.plan.params.delivery).to_string(),
            state: self.state.as_str().to_string(),
            logical_start_seconds: self.plan.params.seek_seconds,
            temp_dir: self.temp_dir.to_string_lossy().to_string(),
            artifacts: self.artifacts.artifact_names(),
            process_id: self.process_id,
            process_group_id: self.process_group_id,
            started_at: self.started_at.map(|ts| ts.to_rfc3339()),
            last_progress_at: self.last_progress_at.map(|ts| ts.to_rfc3339()),
            last_segment_at: self.last_segment_at.map(|ts| ts.to_rfc3339()),
            log_path: self.log_path.to_string_lossy().to_string(),
            error: self.error.clone(),
            error_code: self.error_code.clone(),
            error_kind: self.error_kind.clone(),
            log_tail: self.log_tail.clone(),
            playback_plan: self.plan.playback_plan.clone(),
            active_rung: active_rung_snapshot(
                self.plan.playback_plan.as_ref(),
                self.active_rung_id.as_deref(),
            ),
        }
    }

    fn handle(&self) -> TranscodeHandle {
        let snapshot = self.snapshot();
        TranscodeHandle {
            playlist_path: self.temp_dir.join("master.m3u8"),
            log_path: self.log_path.clone(),
            temp_dir: self.temp_dir.clone(),
            pid: self.process_id,
            process_group_id: self.process_group_id,
            subtitles: self.subtitles.clone(),
            job_state: snapshot.to_json(),
        }
    }
}

#[derive(Clone)]
pub struct PlaybackJobManager {
    jobs: Arc<DashMap<Uuid, Arc<Mutex<PlaybackJob>>>>,
    capacities: Arc<JobCapacityPools>,
    startup_queue_len: Arc<AtomicUsize>,
    db_pool: AnyPool,
    limits: PlaybackJobLimits,
    hardware_failure_callback: Option<HardwareFailureCallback>,
}

struct JobCapacityPools {
    hls: Arc<Semaphore>,
    remux: Arc<Semaphore>,
    partial: Arc<Semaphore>,
    video: Arc<Semaphore>,
    hardware: Arc<Semaphore>,
}

impl PlaybackJobManager {
    pub fn new(db_pool: AnyPool, max_video_transcodes: Option<u32>) -> Self {
        Self::with_limits(db_pool, max_video_transcodes, PlaybackJobLimits::default())
    }

    pub fn with_limits(
        db_pool: AnyPool,
        max_video_transcodes: Option<u32>,
        limits: PlaybackJobLimits,
    ) -> Self {
        Self::with_capacity_limits(
            db_pool,
            PlaybackJobCapacityLimits {
                max_video_transcodes,
                ..PlaybackJobCapacityLimits::default()
            },
            limits,
        )
    }

    pub fn with_capacity_limits(
        db_pool: AnyPool,
        capacity_limits: PlaybackJobCapacityLimits,
        limits: PlaybackJobLimits,
    ) -> Self {
        Self::with_capacity_limits_and_hardware_failure_callback(
            db_pool,
            capacity_limits,
            limits,
            None,
        )
    }

    pub fn with_capacity_limits_and_hardware_failure_callback(
        db_pool: AnyPool,
        capacity_limits: PlaybackJobCapacityLimits,
        limits: PlaybackJobLimits,
        hardware_failure_callback: Option<HardwareFailureCallback>,
    ) -> Self {
        Self {
            jobs: Arc::new(DashMap::new()),
            capacities: Arc::new(JobCapacityPools {
                hls: Arc::new(Semaphore::new(capacity_limits.hls_jobs())),
                remux: Arc::new(Semaphore::new(capacity_limits.direct_streams())),
                partial: Arc::new(Semaphore::new(capacity_limits.partial_transcodes())),
                video: Arc::new(Semaphore::new(capacity_limits.video_transcodes())),
                hardware: Arc::new(Semaphore::new(capacity_limits.hardware_transcodes())),
            }),
            startup_queue_len: Arc::new(AtomicUsize::new(0)),
            db_pool,
            limits,
            hardware_failure_callback,
        }
    }

    pub fn startup_queue_len(&self) -> usize {
        self.startup_queue_len.load(Ordering::Relaxed)
    }

    pub async fn start(
        &self,
        mut plan: PlaybackJobPlan,
        logical_start_seconds: f32,
    ) -> Result<TranscodeHandle> {
        plan.params.seek_seconds = logical_start_seconds;
        if let Some(handle) = self.existing_handle(plan.session_id).await? {
            return Ok(handle);
        }
        self.launch_with_hardware_fallback(plan, PlaybackJobState::Planned)
            .await
    }

    pub async fn restart_at(
        &self,
        session_id: Uuid,
        logical_position_seconds: f32,
    ) -> Result<TranscodeHandle> {
        let entry = self
            .jobs
            .get(&session_id)
            .map(|entry| entry.value().clone())
            .ok_or_else(|| anyhow!("playback job not found"))?;
        let mut plan = {
            let mut job = entry.lock().await;
            job.state = PlaybackJobState::Restarting;
            job.last_progress_at = Some(Utc::now());
            job.plan.params.seek_seconds = logical_position_seconds;
            job.snapshot()
        };
        self.persist_snapshot_json(
            session_id,
            &plan.to_json(),
            Some(PlaybackJobState::Restarting),
        )
        .await?;

        self.stop_process_and_remove_dir(&entry, true).await;

        let mut restart_plan = {
            let job = entry.lock().await;
            job.plan.clone()
        };
        restart_plan.params.seek_seconds = logical_position_seconds;
        plan.logical_start_seconds = logical_position_seconds;
        self.launch_with_hardware_fallback(restart_plan, PlaybackJobState::Restarting)
            .await
    }

    pub async fn stop(&self, session_id: Uuid, reason: &str) {
        let Some(entry) = self
            .jobs
            .get(&session_id)
            .map(|entry| entry.value().clone())
        else {
            return;
        };

        let stopping = {
            let mut job = entry.lock().await;
            job.state = PlaybackJobState::Stopping;
            job.error = Some(reason.to_string());
            job.last_progress_at = Some(Utc::now());
            job.snapshot()
        };
        let _ = self
            .persist_snapshot_json(
                session_id,
                &stopping.to_json(),
                Some(PlaybackJobState::Stopping),
            )
            .await;

        self.stop_process_and_remove_dir(&entry, true).await;

        let stopped = {
            let mut job = entry.lock().await;
            job.state = PlaybackJobState::Stopped;
            job.last_progress_at = Some(Utc::now());
            job.snapshot()
        };
        let _ = self
            .persist_snapshot_json(
                session_id,
                &stopped.to_json(),
                Some(PlaybackJobState::Stopped),
            )
            .await;
        self.jobs.remove(&session_id);
    }

    pub async fn lookup_artifact(
        &self,
        session_id: Uuid,
        artifact_name: &str,
    ) -> Option<PlaybackArtifact> {
        let entry = self
            .jobs
            .get(&session_id)
            .map(|entry| entry.value().clone())?;
        if self.enforce_resource_limits(session_id).await.is_err() {
            return None;
        }

        let (artifact, snapshot) = {
            let mut job = entry.lock().await;
            let artifact = job.artifacts.resolve(&job.temp_dir, artifact_name)?;
            if job.plan.params.mode == PlaybackMode::AdaptiveTranscode {
                if let Some(rung_id) = adaptive_rung_id_from_artifact_name(&artifact.name) {
                    if job.active_rung_id.as_deref() != Some(rung_id.as_str()) {
                        PLAYBACK_ADAPTIVE_RENDITION_SWITCHES
                            .with_label_values(&[
                                adaptive_rung_switch_direction(
                                    job.active_rung_id.as_deref(),
                                    &rung_id,
                                ),
                                "hls_artifact_request",
                            ])
                            .inc();
                    }
                    job.active_rung_id = Some(rung_id);
                }
            }
            if matches!(
                artifact.kind,
                ArtifactKind::MediaSegment | ArtifactKind::SubtitleSegment
            ) {
                let now = Utc::now();
                job.last_segment_at = Some(now);
                job.last_progress_at = Some(now);
            }
            (artifact, job.snapshot())
        };
        let _ = self
            .persist_snapshot_json(session_id, &snapshot.to_json(), None)
            .await;
        Some(artifact)
    }

    pub async fn snapshot(&self, session_id: Uuid) -> Option<PlaybackJobSnapshot> {
        let entry = self
            .jobs
            .get(&session_id)
            .map(|entry| entry.value().clone())?;
        Some(entry.lock().await.snapshot())
    }

    pub async fn cleanup_expired(&self, now: DateTime<Utc>) {
        let session_ids = self
            .jobs
            .iter()
            .map(|entry| *entry.key())
            .collect::<Vec<_>>();
        for session_id in session_ids {
            let Some(snapshot) = self.snapshot(session_id).await else {
                continue;
            };
            if snapshot.state == PlaybackJobState::Stopped.as_str()
                || snapshot.state == PlaybackJobState::Failed.as_str()
            {
                self.jobs.remove(&session_id);
                continue;
            }

            let Some(last_segment_at) = snapshot
                .last_segment_at
                .as_deref()
                .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
                .map(|dt| dt.with_timezone(&Utc))
            else {
                continue;
            };
            if now.signed_duration_since(last_segment_at)
                > chrono::Duration::from_std(self.limits.stale_segment_timeout)
                    .unwrap_or_else(|_| chrono::Duration::seconds(90))
            {
                self.mark_stalled_then_failed(session_id, "stale_segment_timeout")
                    .await;
            }
        }
    }

    pub async fn start_or_get(
        &self,
        session_id: Uuid,
        media_path: &str,
        params: TranscodeParams,
    ) -> Result<TranscodeHandle> {
        let seek_seconds = params.seek_seconds;
        self.start(
            PlaybackJobPlan::new(
                session_id,
                session_id.to_string(),
                media_path.to_string(),
                params,
                None,
            ),
            seek_seconds,
        )
        .await
    }

    pub async fn restart(
        &self,
        session_id: Uuid,
        media_path: &str,
        params: TranscodeParams,
    ) -> Result<TranscodeHandle> {
        if self.jobs.contains_key(&session_id) {
            self.restart_at(session_id, params.seek_seconds).await
        } else {
            self.start_or_get(session_id, media_path, params).await
        }
    }

    pub async fn artifact_path(&self, session_id: Uuid, name: &str) -> Option<PlaybackArtifact> {
        self.lookup_artifact(session_id, name).await
    }

    pub async fn temp_dir(&self, session_id: Uuid) -> Option<PathBuf> {
        self.snapshot(session_id)
            .await
            .map(|snapshot| PathBuf::from(snapshot.temp_dir))
    }

    pub async fn subtitle_delay(&self, session_id: Uuid) -> Option<f64> {
        let entry = self
            .jobs
            .get(&session_id)
            .map(|entry| entry.value().clone())?;
        entry.lock().await.subtitle_delay_seconds
    }

    pub async fn seek_seconds(&self, session_id: Uuid) -> Option<f64> {
        self.snapshot(session_id)
            .await
            .map(|snapshot| snapshot.logical_start_seconds as f64)
    }

    pub async fn set_subtitle_delay(&self, session_id: Uuid, delay: f64) {
        let Some(entry) = self
            .jobs
            .get(&session_id)
            .map(|entry| entry.value().clone())
        else {
            return;
        };
        let snapshot = {
            let mut job = entry.lock().await;
            job.subtitle_delay_seconds = Some(delay);
            job.last_progress_at = Some(Utc::now());
            job.snapshot()
        };
        let _ = self
            .persist_snapshot_json(session_id, &snapshot.to_json(), None)
            .await;
    }

    pub async fn stop_and_remove(&self, session_id: Uuid) {
        self.stop(session_id, "removed").await;
    }

    pub async fn stop_all(&self) {
        let session_ids = self
            .jobs
            .iter()
            .map(|entry| *entry.key())
            .collect::<Vec<_>>();
        for session_id in session_ids {
            self.stop(session_id, "server_shutdown").await;
        }
    }

    #[cfg(test)]
    pub async fn insert_test_job(
        &self,
        session_id: Uuid,
        temp_dir: PathBuf,
        subtitle_count: usize,
    ) {
        self.insert_test_job_for_plan(
            session_id,
            temp_dir,
            PlaybackMode::VideoTranscode,
            Delivery::HlsMpegts,
            subtitle_count,
        )
        .await;
    }

    #[cfg(test)]
    pub async fn insert_test_job_for_plan(
        &self,
        session_id: Uuid,
        temp_dir: PathBuf,
        mode: PlaybackMode,
        delivery: Delivery,
        subtitle_count: usize,
    ) {
        let mut subtitles = Vec::new();
        for idx in 0..subtitle_count {
            subtitles.push(SubtitleInfo {
                stream_index: idx as i32,
                language: None,
                title: None,
                is_default: idx == 0,
                is_forced: false,
                is_hearing_impaired: false,
            });
        }
        let artifacts = ArtifactRegistry::for_plan(mode, delivery, subtitle_count);
        let job = PlaybackJob {
            plan: PlaybackJobPlan::new(
                session_id,
                "test-media-file",
                "test-media",
                TranscodeParams {
                    seek_seconds: 0.0,
                    mode,
                    delivery,
                },
                None,
            ),
            state: PlaybackJobState::Running,
            temp_dir: temp_dir.clone(),
            artifacts,
            process_id: None,
            process_group_id: None,
            child: None,
            capacity_permits: Vec::new(),
            started_at: Some(Utc::now()),
            last_progress_at: Some(Utc::now()),
            last_segment_at: Some(Utc::now()),
            log_path: temp_dir.join("ffmpeg.log"),
            error: None,
            error_code: None,
            error_kind: None,
            log_tail: None,
            subtitle_delay_seconds: None,
            subtitles,
            active_rung_id: None,
        };
        let snapshot = job.snapshot();
        self.jobs.insert(session_id, Arc::new(Mutex::new(job)));
        let _ = self
            .persist_snapshot_json(session_id, &snapshot.to_json(), None)
            .await;
    }

    async fn existing_handle(&self, session_id: Uuid) -> Result<Option<TranscodeHandle>> {
        let Some(entry) = self
            .jobs
            .get(&session_id)
            .map(|entry| entry.value().clone())
        else {
            return Ok(None);
        };

        let state = { entry.lock().await.state };
        if matches!(
            state,
            PlaybackJobState::Planned | PlaybackJobState::Starting | PlaybackJobState::Restarting
        ) {
            return self.wait_for_ready(session_id).await.map(Some);
        }
        if matches!(state, PlaybackJobState::Failed | PlaybackJobState::Stopped) {
            return Ok(None);
        }

        let mut remove_stale = false;
        let handle = {
            let mut job = entry.lock().await;
            let child_running = match job.child.as_mut() {
                Some(child) => child.try_wait()?.is_none(),
                None => false,
            };
            let playlist_exists = job.temp_dir.join("master.m3u8").exists();
            if state.is_ready() && (child_running || playlist_exists) {
                Some(job.handle())
            } else {
                remove_stale = true;
                None
            }
        };
        if remove_stale {
            self.stop(session_id, "stale_job_replaced").await;
        }
        Ok(handle)
    }

    async fn launch_with_hardware_fallback(
        &self,
        mut plan: PlaybackJobPlan,
        mut initial_state: PlaybackJobState,
    ) -> Result<TranscodeHandle> {
        let mut retried_with_software = false;
        loop {
            let attempted_plan = plan.clone();
            let fallback_allowed = job_plan_allows_hardware_software_fallback(&attempted_plan);
            match self.launch(attempted_plan.clone(), initial_state).await {
                Ok(handle) => return Ok(handle),
                Err(err) if fallback_allowed && !retried_with_software => {
                    retried_with_software = true;
                    let Some(fallback_plan) =
                        software_fallback_job_plan(&attempted_plan, &err.to_string())
                    else {
                        return Err(err);
                    };
                    if let Some(value) = fallback_plan.playback_plan.as_ref() {
                        let _ = self
                            .persist_playback_plan_json(fallback_plan.session_id, value)
                            .await;
                    }
                    plan = fallback_plan;
                    initial_state = PlaybackJobState::Restarting;
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn launch(
        &self,
        plan: PlaybackJobPlan,
        initial_state: PlaybackJobState,
    ) -> Result<TranscodeHandle> {
        let _startup_queue_guard = StartupQueueGuard::new(self.startup_queue_len.clone());
        let session_id = plan.session_id;
        let temp_dir = self.make_temp_dir(session_id).await?;
        let log_path = temp_dir.join("ffmpeg.log");
        let playback_plan = plan.parsed_playback_plan();
        let active_rung_id = playback_plan
            .as_ref()
            .and_then(|plan| plan.adaptive_ladder.as_ref())
            .map(|ladder| ladder.starting_rung_id.clone());
        let artifacts = ArtifactRegistry::for_plan(plan.params.mode, plan.params.delivery, 0);
        let now = Utc::now();
        let first_state = if initial_state == PlaybackJobState::Restarting {
            PlaybackJobState::Restarting
        } else {
            PlaybackJobState::Planned
        };
        let job = PlaybackJob {
            plan: plan.clone(),
            state: first_state,
            temp_dir: temp_dir.clone(),
            artifacts,
            process_id: None,
            process_group_id: None,
            child: None,
            capacity_permits: Vec::new(),
            started_at: None,
            last_progress_at: Some(now),
            last_segment_at: None,
            log_path: log_path.clone(),
            error: None,
            error_code: None,
            error_kind: None,
            log_tail: None,
            subtitle_delay_seconds: None,
            subtitles: Vec::new(),
            active_rung_id,
        };
        let job = Arc::new(Mutex::new(job));
        self.jobs.insert(session_id, job.clone());
        let snapshot = job.lock().await.snapshot();
        self.persist_snapshot_json(session_id, &snapshot.to_json(), Some(first_state))
            .await?;

        if first_state == PlaybackJobState::Planned {
            self.transition(&job, PlaybackJobState::Starting, None, None)
                .await?;
        }

        let permits = self.acquire_capacity(&plan).await?;
        let subtitles = match playback_plan.as_ref() {
            Some(playback_plan)
                if playback_plan.subtitle_action == StreamAction::ConvertTextToWebvtt =>
            {
                detect_text_subtitles(&plan.media_path, playback_plan.selected_subtitle_track).await
            }
            None if plan.params.mode == PlaybackMode::SubtitleTranscode => {
                detect_text_subtitles(&plan.media_path, None).await
            }
            _ => Vec::new(),
        };
        let layout = HlsOutputLayout::for_job(&temp_dir, plan.params.mode, plan.params.delivery);
        let child = match spawn_ffmpeg(
            &plan.media_path,
            &plan.params,
            playback_plan.as_ref(),
            &layout,
            &log_path,
            &temp_dir,
            &subtitles,
        )
        .await
        {
            Ok(child) => child,
            Err(err) => {
                drop(permits);
                self.fail_job(session_id, "spawn_failed", Some(err.to_string()), true)
                    .await;
                return Err(err);
            }
        };
        let process_id = child.id();
        let process_group_id = process_group_id(process_id);
        let snapshot = {
            let mut job = job.lock().await;
            job.artifacts =
                ArtifactRegistry::for_plan(plan.params.mode, plan.params.delivery, subtitles.len());
            job.subtitles = subtitles;
            job.child = Some(child);
            job.process_id = process_id;
            job.process_group_id = process_group_id;
            job.capacity_permits = permits;
            job.started_at = Some(Utc::now());
            job.last_progress_at = Some(Utc::now());
            job.snapshot()
        };
        self.persist_snapshot_json(session_id, &snapshot.to_json(), None)
            .await?;

        let playlist_path = if layout.direct_stream {
            layout.media_playlist_path.clone()
        } else {
            layout.master_playlist_path.clone()
        };
        if let Err(err) = wait_for_path_or_process_exit(
            &job,
            &playlist_path,
            self.limits.startup_timeout,
            Duration::from_millis(150),
        )
        .await
        {
            let reason = if err.to_string().contains("exited") {
                "startup_failed"
            } else {
                "startup_timeout"
            };
            self.fail_job(session_id, reason, Some(err.to_string()), true)
                .await;
            return Err(err);
        }
        if layout.direct_stream {
            if let Err(err) = write_direct_stream_master_playlist(
                &layout.master_playlist_path,
                "media.m3u8",
                playback_plan.as_ref(),
            )
            .await
            {
                self.fail_job(
                    session_id,
                    "master_playlist_failed",
                    Some(err.to_string()),
                    true,
                )
                .await;
                return Err(err);
            }
        }
        self.transition(&job, PlaybackJobState::PlaylistReady, None, None)
            .await?;
        let playlist_ready_at = Utc::now();

        if let Err(err) = wait_for_first_media_segment_or_process_exit(
            &job,
            &temp_dir,
            self.limits.first_segment_timeout,
            Duration::from_millis(150),
        )
        .await
        {
            let reason = if err.to_string().contains("exited") {
                "first_segment_failed"
            } else {
                "first_segment_timeout"
            };
            self.fail_job(session_id, reason, Some(err.to_string()), true)
                .await;
            return Err(err);
        }
        let (started_at, first_segment_at) = {
            let mut job = job.lock().await;
            let now = Utc::now();
            job.last_segment_at = Some(now);
            job.last_progress_at = Some(now);
            (job.started_at, now)
        };
        self.record_successful_performance_observation(
            playback_plan.as_ref(),
            started_at,
            Some(playlist_ready_at),
            first_segment_at,
        )
        .await;
        self.enforce_resource_limits(session_id).await?;
        self.transition(&job, PlaybackJobState::Running, None, None)
            .await?;

        Ok(job.lock().await.handle())
    }

    async fn wait_for_ready(&self, session_id: Uuid) -> Result<TranscodeHandle> {
        let deadline = self.limits.startup_timeout + self.limits.first_segment_timeout;
        let start = time::Instant::now();
        loop {
            if start.elapsed() > deadline {
                return Err(anyhow!("playback job did not become ready before timeout"));
            }
            let Some(entry) = self
                .jobs
                .get(&session_id)
                .map(|entry| entry.value().clone())
            else {
                return Err(anyhow!("playback job not found"));
            };
            {
                let job = entry.lock().await;
                if job.state == PlaybackJobState::Failed {
                    return Err(anyhow!(
                        "{}",
                        job.error
                            .clone()
                            .unwrap_or_else(|| "playback job failed".to_string())
                    ));
                }
                if job.state.is_ready() && job.temp_dir.join("master.m3u8").exists() {
                    return Ok(job.handle());
                }
            }
            time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn record_successful_performance_observation(
        &self,
        playback_plan: Option<&PlaybackPlan>,
        started_at: Option<DateTime<Utc>>,
        playlist_ready_at: Option<DateTime<Utc>>,
        first_segment_at: DateTime<Utc>,
    ) {
        let Some(playback_plan) = playback_plan else {
            return;
        };
        let Some(envelope_id) = selected_performance_envelope_id(playback_plan) else {
            return;
        };
        let observation = PlaybackPerformanceObservation {
            envelope_id: envelope_id.to_string(),
            startup_latency_ms: started_at
                .as_ref()
                .zip(playlist_ready_at.as_ref())
                .and_then(|(start, ready)| elapsed_millis(start, ready)),
            first_segment_latency_ms: started_at
                .as_ref()
                .and_then(|start| elapsed_millis(start, &first_segment_at)),
            realtime_factor_millis: started_at.as_ref().and_then(|start| {
                first_segment_realtime_factor_millis(playback_plan, start, &first_segment_at)
            }),
            failure_kind: None,
            fallback_kind: playback_plan_fallback_kind(playback_plan),
            output_mode: Some(playback_plan.mode.as_str().to_string()),
        };
        self.record_performance_observation(observation, true).await;
    }

    async fn record_failed_performance_observation(
        &self,
        observation: Option<PlaybackPerformanceObservation>,
    ) {
        let Some(observation) = observation else {
            return;
        };
        self.record_performance_observation(observation, false)
            .await;
    }

    async fn record_performance_observation(
        &self,
        observation: PlaybackPerformanceObservation,
        success: bool,
    ) {
        match record_playback_performance_observation(
            &self.db_pool,
            &observation.envelope_id,
            success,
            observation.startup_latency_ms,
            observation.first_segment_latency_ms,
            observation.realtime_factor_millis,
            observation.failure_kind.as_deref(),
            observation.fallback_kind.as_deref(),
            observation.output_mode.as_deref(),
        )
        .await
        {
            Ok(0) => tracing::debug!(
                envelope_id = %observation.envelope_id,
                success,
                "playback performance observation skipped because selected envelope no longer exists"
            ),
            Ok(_) => {}
            Err(err) => tracing::warn!(
                envelope_id = %observation.envelope_id,
                success,
                error = ?err,
                "failed to record playback performance observation"
            ),
        }
    }

    async fn transition(
        &self,
        job: &Arc<Mutex<PlaybackJob>>,
        state: PlaybackJobState,
        error: Option<String>,
        log_tail: Option<String>,
    ) -> Result<()> {
        let (session_id, snapshot) = {
            let mut job = job.lock().await;
            job.state = state;
            if let Some(error) = error {
                job.error = Some(error);
            }
            if let Some(log_tail) = log_tail {
                job.log_tail = Some(log_tail);
            }
            job.last_progress_at = Some(Utc::now());
            (job.plan.session_id, job.snapshot())
        };
        self.persist_snapshot_json(session_id, &snapshot.to_json(), Some(state))
            .await
    }

    async fn fail_job(
        &self,
        session_id: Uuid,
        reason: &str,
        detail: Option<String>,
        stop_process: bool,
    ) {
        let Some(entry) = self
            .jobs
            .get(&session_id)
            .map(|entry| entry.value().clone())
        else {
            return;
        };
        let (log_path, planned_hardware_api) = {
            let job = entry.lock().await;
            (job.log_path.clone(), job_planned_hardware_api(&job))
        };
        let log_tail = read_log_tail(&log_path, self.limits.log_tail_bytes)
            .await
            .ok()
            .filter(|tail| !tail.trim().is_empty());
        let error_kind = classify_playback_failure(reason, detail.as_deref(), log_tail.as_deref());
        let failure_observation = {
            let job = entry.lock().await;
            playback_performance_failure_observation(&job, reason, error_kind, Utc::now())
        };
        if error_kind == "hardware_unavailable" && planned_hardware_api.is_some() {
            if let Some(callback) = self.hardware_failure_callback.as_ref() {
                callback();
            }
            tracing::warn!(
                api = planned_hardware_api.as_deref().unwrap_or("unknown"),
                reason,
                "playback hardware failure invalidated readiness cache"
            );
        }
        let message = match detail {
            Some(detail) => format!("{reason}: {detail}"),
            None => reason.to_string(),
        };
        let redacted_log_tail = log_tail.map(redact_log_tail);
        if stop_process {
            self.stop_process_and_remove_dir(&entry, true).await;
        }
        self.record_failed_performance_observation(failure_observation)
            .await;
        let (session_id, snapshot) = {
            let mut job = entry.lock().await;
            job.state = PlaybackJobState::Failed;
            job.error = Some(message);
            job.error_code = Some(reason.to_string());
            job.error_kind = Some(error_kind.to_string());
            if let Some(log_tail) = redacted_log_tail {
                job.log_tail = Some(log_tail);
            }
            job.last_progress_at = Some(Utc::now());
            (job.plan.session_id, job.snapshot())
        };
        let _ = self
            .persist_snapshot_json(
                session_id,
                &snapshot.to_json(),
                Some(PlaybackJobState::Failed),
            )
            .await;
    }

    async fn mark_stalled_then_failed(&self, session_id: Uuid, reason: &str) {
        let Some(entry) = self
            .jobs
            .get(&session_id)
            .map(|entry| entry.value().clone())
        else {
            return;
        };
        let _ = self
            .transition(
                &entry,
                PlaybackJobState::Stalled,
                Some(reason.to_string()),
                None,
            )
            .await;
        self.fail_job(session_id, reason, None, true).await;
    }

    async fn stop_process_and_remove_dir(
        &self,
        job: &Arc<Mutex<PlaybackJob>>,
        release_permit: bool,
    ) {
        let (mut child, process_group_id, temp_dir, permits) = {
            let mut job = job.lock().await;
            let child = job.child.take();
            let permits = if release_permit {
                std::mem::take(&mut job.capacity_permits)
            } else {
                Vec::new()
            };
            (child, job.process_group_id, job.temp_dir.clone(), permits)
        };

        kill_child_process_group(&mut child, process_group_id).await;
        let _ = fs::remove_dir_all(temp_dir).await;
        drop(permits);
    }

    async fn enforce_resource_limits(&self, session_id: Uuid) -> Result<()> {
        let Some(snapshot) = self.snapshot(session_id).await else {
            return Ok(());
        };
        let log_path = PathBuf::from(&snapshot.log_path);
        if fs::metadata(&log_path)
            .await
            .map(|meta| meta.len() > self.limits.max_log_bytes)
            .unwrap_or(false)
        {
            self.fail_job(session_id, "max_log_size_exceeded", None, true)
                .await;
            return Err(anyhow!("playback job log exceeded max size"));
        }

        let temp_dir = PathBuf::from(&snapshot.temp_dir);
        if directory_size(&temp_dir).await.unwrap_or(0) > self.limits.max_temp_dir_bytes {
            self.fail_job(session_id, "max_temp_dir_size_exceeded", None, true)
                .await;
            return Err(anyhow!("playback job temp dir exceeded max size"));
        }
        Ok(())
    }

    async fn acquire_capacity(&self, plan: &PlaybackJobPlan) -> Result<Vec<OwnedSemaphorePermit>> {
        let mut permit_specs = vec![(self.capacities.hls.clone(), 1_usize)];
        match plan.params.mode {
            PlaybackMode::DirectStream => permit_specs.push((self.capacities.remux.clone(), 1)),
            PlaybackMode::AudioTranscode | PlaybackMode::SubtitleTranscode => {
                permit_specs.push((self.capacities.partial.clone(), 1));
            }
            PlaybackMode::VideoTranscode | PlaybackMode::DirectPlay => {
                permit_specs.push((self.capacities.video.clone(), 1));
            }
            PlaybackMode::AdaptiveTranscode => {
                permit_specs.push((self.capacities.video.clone(), 2));
            }
        }
        if plan
            .parsed_playback_plan()
            .is_some_and(|plan| plan.hardware_acceleration.enabled)
        {
            permit_specs.push((self.capacities.hardware.clone(), 1));
        }

        let total_weight = permit_specs.iter().map(|(_, weight)| *weight).sum();
        let mut permits = Vec::with_capacity(total_weight);
        for (semaphore, weight) in permit_specs {
            for _ in 0..weight {
                permits.push(
                    semaphore
                        .clone()
                        .acquire_owned()
                        .await
                        .context("playback job capacity semaphore closed")?,
                );
            }
        }
        Ok(permits)
    }

    async fn make_temp_dir(&self, session_id: Uuid) -> Result<PathBuf> {
        let dir = playback_temp_root().join(session_id.to_string());
        if dir.exists() {
            fs::remove_dir_all(&dir).await.ok();
        }
        fs::create_dir_all(&dir)
            .await
            .context("creating playback job temp dir")?;
        Ok(dir)
    }

    async fn persist_snapshot_json(
        &self,
        session_id: Uuid,
        job_state: &Value,
        state: Option<PlaybackJobState>,
    ) -> Result<()> {
        let job_state = job_state.to_string();
        if matches!(state, Some(PlaybackJobState::Failed)) {
            sqlx::query::<sqlx::Any>(
                "UPDATE playback_sessions
                 SET job_state_json = ?, transcode_state = ?, state = 'error', updated_at = CURRENT_TIMESTAMP
                 WHERE id = ?",
            )
            .bind(&job_state)
            .bind(&job_state)
            .bind(session_id.to_string())
            .execute(&self.db_pool)
            .await?;
        } else {
            sqlx::query::<sqlx::Any>(
                "UPDATE playback_sessions
                 SET job_state_json = ?, transcode_state = ?, updated_at = CURRENT_TIMESTAMP
                 WHERE id = ?",
            )
            .bind(&job_state)
            .bind(&job_state)
            .bind(session_id.to_string())
            .execute(&self.db_pool)
            .await?;
        }
        Ok(())
    }

    async fn persist_playback_plan_json(
        &self,
        session_id: Uuid,
        playback_plan: &Value,
    ) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "UPDATE playback_sessions
             SET playback_plan_json = ?, updated_at = CURRENT_TIMESTAMP
             WHERE id = ?",
        )
        .bind(playback_plan.to_string())
        .bind(session_id.to_string())
        .execute(&self.db_pool)
        .await?;
        Ok(())
    }
}

fn job_plan_allows_hardware_software_fallback(plan: &PlaybackJobPlan) -> bool {
    plan.parsed_playback_plan()
        .map(|plan| {
            plan.hardware_acceleration.enabled
                && plan
                    .hardware_acceleration
                    .fallback
                    .as_deref()
                    .is_some_and(|fallback| fallback.eq_ignore_ascii_case("software"))
        })
        .unwrap_or(false)
}

fn job_planned_hardware_api(job: &PlaybackJob) -> Option<String> {
    job.plan
        .parsed_playback_plan()
        .and_then(|plan| {
            plan.hardware_acceleration
                .enabled
                .then_some(plan.hardware_acceleration.api)
        })
        .flatten()
}

fn selected_performance_envelope_id(playback_plan: &PlaybackPlan) -> Option<&str> {
    playback_plan
        .feasibility
        .as_ref()
        .and_then(|feasibility| feasibility.selected_envelope_id.as_deref())
}

fn playback_performance_failure_observation(
    job: &PlaybackJob,
    reason: &str,
    error_kind: &str,
    observed_at: DateTime<Utc>,
) -> Option<PlaybackPerformanceObservation> {
    let playback_plan = job.plan.parsed_playback_plan()?;
    let envelope_id = selected_performance_envelope_id(&playback_plan)?.to_string();
    let elapsed_since_start = job
        .started_at
        .as_ref()
        .and_then(|started_at| elapsed_millis(started_at, &observed_at));
    let (startup_latency_ms, first_segment_latency_ms) = if reason.starts_with("startup") {
        (elapsed_since_start, None)
    } else if reason.starts_with("first_segment") {
        (None, elapsed_since_start)
    } else {
        (None, None)
    };
    Some(PlaybackPerformanceObservation {
        envelope_id,
        startup_latency_ms,
        first_segment_latency_ms,
        realtime_factor_millis: None,
        failure_kind: Some(error_kind.to_string()),
        fallback_kind: playback_plan_fallback_kind(&playback_plan),
        output_mode: Some(playback_plan.mode.as_str().to_string()),
    })
}

fn elapsed_millis(start: &DateTime<Utc>, end: &DateTime<Utc>) -> Option<i64> {
    let millis = end.signed_duration_since(*start).num_milliseconds();
    (millis >= 0).then_some(millis)
}

fn first_segment_realtime_factor_millis(
    playback_plan: &PlaybackPlan,
    started_at: &DateTime<Utc>,
    first_segment_at: &DateTime<Utc>,
) -> Option<i32> {
    let elapsed_ms = elapsed_millis(started_at, first_segment_at)?;
    if elapsed_ms <= 0 {
        return None;
    }
    let segment_seconds = selected_output_segment_seconds(playback_plan)?;
    let realtime_factor = segment_seconds / (elapsed_ms as f64 / 1000.0);
    Some(
        (realtime_factor * 1000.0)
            .round()
            .clamp(0.0, i32::MAX as f64) as i32,
    )
}

fn selected_output_segment_seconds(playback_plan: &PlaybackPlan) -> Option<f64> {
    playback_plan
        .video_output
        .as_ref()
        .and_then(|output| output.segment_seconds.parse::<f64>().ok())
        .or_else(|| {
            let ladder = playback_plan.adaptive_ladder.as_ref()?;
            let active = ladder
                .rungs
                .iter()
                .find(|rung| rung.id == ladder.active_rung_id)
                .or_else(|| ladder.rungs.first())?;
            active.video.segment_seconds.parse::<f64>().ok()
        })
        .filter(|seconds| *seconds > 0.0)
}

fn playback_plan_fallback_kind(playback_plan: &PlaybackPlan) -> Option<String> {
    playback_plan
        .hardware_acceleration
        .fallback
        .as_deref()
        .filter(|fallback| !fallback.trim().is_empty())
        .map(str::to_string)
}

fn active_rung_snapshot(
    playback_plan: Option<&Value>,
    active_rung_id: Option<&str>,
) -> Option<Value> {
    let plan = playback_plan?;
    let ladder = plan.get("adaptive_ladder")?;
    let active_rung_id = active_rung_id
        .or_else(|| ladder.get("active_rung_id").and_then(Value::as_str))
        .or_else(|| ladder.get("starting_rung_id").and_then(Value::as_str))?;
    ladder
        .get("rungs")
        .and_then(Value::as_array)
        .and_then(|rungs| {
            rungs.iter().find(|rung| {
                rung.get("id")
                    .and_then(Value::as_str)
                    .is_some_and(|id| id == active_rung_id)
            })
        })
        .cloned()
}

fn adaptive_rung_id_from_artifact_name(name: &str) -> Option<String> {
    if let Some(id) = name
        .strip_prefix("stream_")
        .and_then(|rest| rest.strip_suffix(".m3u8"))
    {
        return valid_adaptive_rung_id(id).then(|| id.to_string());
    }
    if let Some(id) = name
        .strip_prefix("init_")
        .and_then(|rest| rest.strip_suffix(".mp4"))
    {
        return valid_adaptive_rung_id(id).then(|| id.to_string());
    }
    let rest = name.strip_prefix("seg_")?;
    let (id, sequence) = rest.rsplit_once('_')?;
    let sequence = sequence
        .strip_suffix(".ts")
        .or_else(|| sequence.strip_suffix(".m4s"))?;
    (valid_adaptive_rung_id(id)
        && sequence.len() == 5
        && sequence.bytes().all(|b| b.is_ascii_digit()))
    .then(|| id.to_string())
}

fn adaptive_rung_switch_direction(previous: Option<&str>, next: &str) -> &'static str {
    let Some(previous) = previous else {
        return "initial";
    };
    match (previous.parse::<u32>(), next.parse::<u32>()) {
        (Ok(previous), Ok(next)) if next > previous => "up",
        (Ok(previous), Ok(next)) if next < previous => "down",
        (Ok(_), Ok(_)) => "same",
        _ => "changed",
    }
}

fn valid_adaptive_rung_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 4 && id.bytes().all(|b| b.is_ascii_digit())
}

fn software_fallback_job_plan(plan: &PlaybackJobPlan, reason: &str) -> Option<PlaybackJobPlan> {
    let mut playback_plan = plan.parsed_playback_plan()?;
    playback_plan.hardware_acceleration = HardwareAccelerationPlan {
        enabled: false,
        api: None,
        decoder: None,
        encoder: None,
        fallback: Some("software_after_hardware_failure".to_string()),
        ..HardwareAccelerationPlan::default()
    };
    push_plan_reason(
        &mut playback_plan.reasons,
        "hardware_startup_failed_software_retry",
    );
    push_plan_reason(
        &mut playback_plan.warnings,
        &format!("hardware_fallback_reason:{}", compact_error_reason(reason)),
    );
    if let Some(video_output) = playback_plan.video_output.as_mut() {
        if is_hardware_video_encoder(&video_output.encoder) {
            video_output.encoder = "libx264".to_string();
            if video_output.preset.trim().is_empty() {
                video_output.preset = "veryfast".to_string();
            }
            if video_output.bitrate_bps.is_none() && video_output.crf.is_none() {
                video_output.crf = Some(20);
            }
        }
        push_plan_reason(&mut video_output.reasons, "hardware_fallback_to_software");
    }
    if let Some(ladder) = playback_plan.adaptive_ladder.as_mut() {
        for rung in &mut ladder.rungs {
            if is_hardware_video_encoder(&rung.video.encoder) {
                rung.video.encoder = "libx264".to_string();
                if rung.video.preset.trim().is_empty() {
                    rung.video.preset = "veryfast".to_string();
                }
                if rung.video.bitrate_bps.is_none() && rung.video.crf.is_none() {
                    rung.video.crf = Some(20);
                }
            }
            push_plan_reason(&mut rung.video.reasons, "hardware_fallback_to_software");
        }
    }

    let mut fallback = plan.clone();
    fallback.playback_plan = serde_json::to_value(playback_plan).ok();
    Some(fallback)
}

fn is_hardware_video_encoder(encoder: &str) -> bool {
    matches!(
        encoder.to_ascii_lowercase().as_str(),
        "h264_videotoolbox"
            | "hevc_videotoolbox"
            | "h264_vaapi"
            | "hevc_vaapi"
            | "h264_qsv"
            | "hevc_qsv"
            | "h264_nvenc"
            | "hevc_nvenc"
            | "h264_amf"
            | "hevc_amf"
    )
}

fn push_plan_reason(reasons: &mut Vec<String>, reason: &str) {
    if !reasons.iter().any(|existing| existing == reason) {
        reasons.push(reason.to_string());
    }
}

fn compact_error_reason(reason: &str) -> String {
    reason
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | ':' | '.') {
                ch
            } else {
                '_'
            }
        })
        .take(160)
        .collect()
}

fn playback_mode_name(mode: PlaybackMode) -> &'static str {
    match mode {
        PlaybackMode::DirectPlay => "direct_play",
        PlaybackMode::DirectStream => "direct_stream",
        PlaybackMode::AudioTranscode => "audio_transcode",
        PlaybackMode::SubtitleTranscode => "subtitle_transcode",
        PlaybackMode::VideoTranscode => "video_transcode",
        PlaybackMode::AdaptiveTranscode => "adaptive_transcode",
    }
}

fn delivery_name(delivery: Delivery) -> &'static str {
    match delivery {
        Delivery::DirectFile => "direct_file",
        Delivery::HlsFmp4 => "hls_fmp4",
        Delivery::HlsMpegts => "hls_mpegts",
        Delivery::HlsAdaptiveFmp4 => "hls_adaptive_fmp4",
        Delivery::HlsAdaptiveMpegts => "hls_adaptive_mpegts",
    }
}

fn classify_playback_failure(
    reason: &str,
    detail: Option<&str>,
    log_tail: Option<&str>,
) -> &'static str {
    let reason_lower = reason.to_ascii_lowercase();
    let detail_lower = detail.unwrap_or_default().to_ascii_lowercase();
    let log_lower = log_tail.unwrap_or_default().to_ascii_lowercase();
    let combined = format!("{reason_lower}\n{detail_lower}\n{log_lower}");

    if combined.contains("no such filter")
        || combined.contains("error initializing filter")
        || combined.contains("filter not found")
        || combined.contains("failed to inject frame into filter")
        || combined.contains("subtitles filter")
        || combined.contains("overlay")
    {
        return "unsupported_filter";
    }
    if combined.contains("videotoolbox")
        || combined.contains("vaapi")
        || combined.contains("qsv")
        || combined.contains("nvenc")
        || combined.contains("amf")
        || combined.contains("hardware device")
        || combined.contains("allow_sw")
        || combined.contains("device creation failed")
    {
        return "hardware_unavailable";
    }
    if reason_lower.contains("timeout") || detail_lower.contains("timed out") {
        return "timeout";
    }
    if reason_lower.contains("startup")
        || reason_lower.contains("spawn")
        || detail_lower.contains("exited")
        || detail_lower.contains("exit status")
    {
        return "ffmpeg_exit";
    }
    if reason_lower.contains("first_segment") {
        return "missing_segment";
    }
    if reason_lower.contains("log_size") || reason_lower.contains("temp_dir_size") {
        return "resource_limit";
    }
    "playback_job_failure"
}

fn process_group_id(process_id: Option<u32>) -> Option<u32> {
    #[cfg(unix)]
    {
        process_id
    }
    #[cfg(not(unix))]
    {
        let _ = process_id;
        None
    }
}

async fn kill_child_process_group(child: &mut Option<Child>, process_group_id: Option<u32>) {
    #[cfg(unix)]
    if let Some(process_group_id) = process_group_id {
        let _ = Command::new("kill")
            .arg("-TERM")
            .arg(format!("-{process_group_id}"))
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .await;
        time::sleep(Duration::from_millis(250)).await;
        let _ = Command::new("kill")
            .arg("-KILL")
            .arg(format!("-{process_group_id}"))
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .await;
    }

    if let Some(child) = child.as_mut() {
        let _ = child.kill().await;
        let _ = child.wait().await;
    }
}

async fn wait_for_path_or_process_exit(
    job: &Arc<Mutex<PlaybackJob>>,
    path: &Path,
    timeout: Duration,
    interval: Duration,
) -> Result<()> {
    let start = time::Instant::now();
    loop {
        if fs::metadata(path).await.is_ok() {
            return Ok(());
        }
        if let Some(status) = child_exit_status(job).await? {
            return Err(anyhow!("ffmpeg exited before playlist was ready: {status}"));
        }
        if start.elapsed() > timeout {
            return Err(anyhow!("playback job startup timed out"));
        }
        time::sleep(interval).await;
    }
}

async fn wait_for_first_media_segment_or_process_exit(
    job: &Arc<Mutex<PlaybackJob>>,
    temp_dir: &Path,
    timeout: Duration,
    interval: Duration,
) -> Result<()> {
    let start = time::Instant::now();
    loop {
        if has_first_media_segment(temp_dir).await {
            return Ok(());
        }
        if let Some(status) = child_exit_status(job).await? {
            return Err(anyhow!(
                "ffmpeg exited before first media segment was ready: {status}"
            ));
        }
        if start.elapsed() > timeout {
            return Err(anyhow!("playback job first segment timed out"));
        }
        time::sleep(interval).await;
    }
}

async fn child_exit_status(job: &Arc<Mutex<PlaybackJob>>) -> Result<Option<String>> {
    let mut job = job.lock().await;
    let Some(child) = job.child.as_mut() else {
        return Ok(None);
    };
    Ok(child.try_wait()?.map(|status| status.to_string()))
}

async fn has_first_media_segment(temp_dir: &Path) -> bool {
    let Ok(mut entries) = fs::read_dir(temp_dir).await else {
        return false;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name().to_string_lossy().to_string();
        if (name.starts_with("seg_0_") || name.starts_with("segment_"))
            && (name.ends_with(".ts") || name.ends_with(".m4s"))
        {
            return true;
        }
    }
    false
}

async fn write_direct_stream_master_playlist(
    path: &Path,
    media_playlist_name: &str,
    playback_plan: Option<&PlaybackPlan>,
) -> Result<()> {
    let report = playback_plan.map(|plan| &plan.compatibility_report);
    let bandwidth = report
        .and_then(|report| report.source_bitrate_bps)
        .filter(|bitrate| *bitrate > 0)
        .unwrap_or(1_000_000);
    let resolution = report.and_then(|report| match (report.source_width, report.source_height) {
        (Some(width), Some(height)) if width > 0 && height > 0 => {
            Some(format!(",RESOLUTION={width}x{height}"))
        }
        _ => None,
    });

    let body = format!(
        "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-STREAM-INF:BANDWIDTH={bandwidth}{resolution}\n{media_playlist_name}\n",
        resolution = resolution.unwrap_or_default()
    );
    fs::write(path, body).await?;
    Ok(())
}

async fn read_log_tail(path: &Path, max_bytes: u64) -> Result<String> {
    let mut file = fs::File::open(path).await?;
    let len = file.metadata().await?.len();
    if len > max_bytes {
        file.seek(std::io::SeekFrom::Start(len - max_bytes)).await?;
    }
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).await?;
    Ok(String::from_utf8_lossy(&bytes).to_string())
}

fn redact_log_tail(raw: String) -> String {
    raw.lines()
        .map(|line| {
            line.split_whitespace()
                .map(|token| {
                    if token.to_ascii_lowercase().contains("authorization:")
                        || token.to_ascii_lowercase().contains("bearer")
                    {
                        "[redacted]".to_string()
                    } else {
                        redact_query_token(token)
                    }
                })
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn redact_query_token(token: &str) -> String {
    let mut redacted = token.to_string();
    for key in ["session", "sid", "token", "access_token", "x-plex-token"] {
        redacted = redact_query_key(&redacted, key);
    }
    redacted
}

fn redact_query_key(input: &str, key: &str) -> String {
    let Some(start) = input.find(&format!("{key}=")) else {
        return input.to_string();
    };
    let value_start = start + key.len() + 1;
    let value_end = input[value_start..]
        .find(['&', '"', '\'', ' '])
        .map(|offset| value_start + offset)
        .unwrap_or(input.len());
    format!(
        "{}{}=[redacted]{}",
        &input[..start],
        key,
        &input[value_end..]
    )
}

async fn directory_size(path: &Path) -> Result<u64> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || directory_size_sync(&path))
        .await
        .context("joining temp dir size task")?
}

fn directory_size_sync(path: &Path) -> Result<u64> {
    let mut total = 0;
    if !path.exists() {
        return Ok(0);
    }
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            total += directory_size_sync(&entry.path())?;
        } else {
            total += metadata.len();
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::Settings,
        db::Database,
        playback::plan::{
            AdaptiveAudioStrategy, AdaptiveLadderPlan, AdaptiveRungPlan, CompatibilityReport,
            HdrAction, PLAYBACK_PLAN_VERSION, PlaybackFeasibilityAction,
            PlaybackFeasibilityDecision, PlaybackPerformanceConfidence,
            PlaybackPerformanceDecision, PlaybackSupportDecision, SeekBehavior, VideoFrameRateMode,
            VideoFrameRatePlan, VideoOutputPlan,
        },
    };
    use tempfile::tempdir;

    async fn test_manager() -> Result<(PlaybackJobManager, Database)> {
        let mut settings = Settings::default();
        settings.database.url = "sqlite::memory:?cache=shared".to_string();
        settings.database.max_connections = 1;
        let database = Database::connect(&settings.database).await?;
        database.run_migrations().await?;
        let manager = PlaybackJobManager::with_limits(
            database.pool.clone(),
            Some(1),
            PlaybackJobLimits {
                startup_timeout: Duration::from_millis(300),
                first_segment_timeout: Duration::from_millis(300),
                stale_segment_timeout: Duration::from_millis(300),
                max_log_bytes: DEFAULT_MAX_LOG_BYTES,
                log_tail_bytes: DEFAULT_LOG_TAIL_BYTES,
                max_temp_dir_bytes: DEFAULT_MAX_TEMP_DIR_BYTES,
            },
        );
        Ok((manager, database))
    }

    #[tokio::test]
    async fn phase13_capacity_pools_keep_direct_partial_and_full_transcodes_independent()
    -> Result<()> {
        let mut settings = Settings::default();
        settings.database.url = "sqlite::memory:?cache=shared".to_string();
        settings.database.max_connections = 1;
        let database = Database::connect(&settings.database).await?;
        let manager = PlaybackJobManager::with_capacity_limits(
            database.pool.clone(),
            PlaybackJobCapacityLimits {
                max_hls_jobs: Some(8),
                max_direct_streams: Some(1),
                max_video_transcodes: Some(1),
                max_hardware_transcodes: Some(1),
            },
            PlaybackJobLimits::default(),
        );

        let direct_plan = PlaybackJobPlan::new(
            Uuid::new_v4(),
            "direct-file",
            "direct-source.mkv",
            TranscodeParams {
                seek_seconds: 0.0,
                mode: PlaybackMode::DirectStream,
                delivery: Delivery::HlsFmp4,
            },
            None,
        );
        let partial_plan = PlaybackJobPlan::new(
            Uuid::new_v4(),
            "partial-file",
            "partial-source.mkv",
            TranscodeParams {
                seek_seconds: 0.0,
                mode: PlaybackMode::AudioTranscode,
                delivery: Delivery::HlsFmp4,
            },
            None,
        );
        let video_plan = PlaybackJobPlan::new(
            Uuid::new_v4(),
            "video-file",
            "video-source.mkv",
            TranscodeParams {
                seek_seconds: 0.0,
                mode: PlaybackMode::VideoTranscode,
                delivery: Delivery::HlsFmp4,
            },
            None,
        );

        let direct_permits = manager.acquire_capacity(&direct_plan).await?;
        let video_permits = manager.acquire_capacity(&video_plan).await?;

        let partial_result = time::timeout(
            Duration::from_millis(100),
            manager.acquire_capacity(&partial_plan),
        )
        .await;
        assert!(
            partial_result.is_ok_and(|result| result.is_ok()),
            "partial transcode should not be blocked by saturated direct/video pools"
        );

        let second_direct = time::timeout(
            Duration::from_millis(50),
            manager.acquire_capacity(&direct_plan),
        )
        .await;
        assert!(
            second_direct.is_err(),
            "direct stream pool should reject only additional direct stream work"
        );

        let second_video = time::timeout(
            Duration::from_millis(50),
            manager.acquire_capacity(&video_plan),
        )
        .await;
        assert!(
            second_video.is_err(),
            "video transcode pool should reject only additional full video work"
        );

        drop(direct_permits);
        drop(video_permits);

        assert!(
            time::timeout(
                Duration::from_millis(100),
                manager.acquire_capacity(&direct_plan)
            )
            .await
            .is_ok_and(|result| result.is_ok())
        );
        assert!(
            time::timeout(
                Duration::from_millis(100),
                manager.acquire_capacity(&video_plan)
            )
            .await
            .is_ok_and(|result| result.is_ok())
        );
        Ok(())
    }

    async fn insert_session(pool: &AnyPool, session_id: Uuid) -> Result<()> {
        let user_id = Uuid::new_v4();
        let media_item_id = Uuid::new_v4();
        let media_file_id = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email, password_hash) VALUES (?1, ?2, ?3)")
            .bind(user_id.to_string())
            .bind(format!("{session_id}@example.com"))
            .bind("hashed")
            .execute(pool)
            .await?;
        sqlx::query(
            "INSERT INTO media_items (id, type, external_ids, title, year)
             VALUES (?, 'movie', '{}', 'Playback Job Test', 2024)",
        )
        .bind(media_item_id.to_string())
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO media_files
                (id, media_item_id, path, size_bytes, container, video_codec, audio_codec, width, height, bitrate_bps, scan_state)
             VALUES (?, ?, ?, 1, 'mkv', 'h264', 'aac', 1920, 1080, 1000000, 'ok')",
        )
        .bind(media_file_id.to_string())
        .bind(media_item_id.to_string())
        .bind(format!("/tmp/{session_id}.mkv"))
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO playback_sessions
                (id, user_id, media_file_id, mode, state, network_type, logical_position_seconds, duration_seconds, token)
             VALUES (?, ?, ?, 'transcode', 'active', 'lan', 0, 60, 'test-session-token')",
        )
        .bind(session_id.to_string())
        .bind(user_id.to_string())
        .bind(media_file_id.to_string())
        .execute(pool)
        .await?;
        Ok(())
    }

    fn test_adaptive_video_output(height: i32, bitrate_bps: i64) -> VideoOutputPlan {
        VideoOutputPlan {
            codec: "h264".to_string(),
            encoder: "libx264".to_string(),
            preset: "veryfast".to_string(),
            profile: Some("high".to_string()),
            level: Some("4.1".to_string()),
            crf: None,
            bitrate_bps: Some(bitrate_bps),
            maxrate_bps: Some(bitrate_bps),
            bufsize_bps: Some(bitrate_bps * 2),
            pixel_format: Some("yuv420p".to_string()),
            scale: None,
            tone_map: None,
            frame_rate: VideoFrameRatePlan {
                mode: VideoFrameRateMode::Source,
                source_fps: Some("24".to_string()),
                target_fps: None,
            },
            gop_frames: Some(96),
            segment_seconds: "4".to_string(),
            keyframe_expression: "expr:gte(t,n_forced*4)".to_string(),
            hls_delivery: Delivery::HlsAdaptiveFmp4,
            burn_in: None,
            reasons: vec![format!("adaptive_rung_{height}p")],
        }
    }

    fn test_adaptive_playback_plan() -> PlaybackPlan {
        let rungs = vec![
            AdaptiveRungPlan {
                id: "0".to_string(),
                label: "720p 3000k".to_string(),
                bandwidth_bps: 3_000_000,
                average_bandwidth_bps: 2_700_000,
                width: 1280,
                height: 720,
                resolution: "1280x720".to_string(),
                codecs: "avc1.640029,mp4a.40.2".to_string(),
                frame_rate: Some("24".to_string()),
                video: test_adaptive_video_output(720, 3_000_000),
            },
            AdaptiveRungPlan {
                id: "1".to_string(),
                label: "480p 1200k".to_string(),
                bandwidth_bps: 1_200_000,
                average_bandwidth_bps: 1_080_000,
                width: 854,
                height: 480,
                resolution: "854x480".to_string(),
                codecs: "avc1.640029,mp4a.40.2".to_string(),
                frame_rate: Some("24".to_string()),
                video: test_adaptive_video_output(480, 1_200_000),
            },
        ];
        PlaybackPlan {
            plan_version: PLAYBACK_PLAN_VERSION,
            mode: PlaybackMode::AdaptiveTranscode,
            delivery: Delivery::HlsAdaptiveFmp4,
            media_file_id: "media-file".to_string(),
            selected_video_track: Some(0),
            video_action: StreamAction::Transcode,
            audio_action: StreamAction::Transcode,
            subtitle_action: StreamAction::Disabled,
            seek_behavior: SeekBehavior::ServerHlsRestart,
            adaptive: true,
            selected_audio_track: Some(1),
            selected_subtitle_track: None,
            hdr_action: HdrAction::None,
            hardware_acceleration: HardwareAccelerationPlan::default(),
            audio_output: None,
            video_output: Some(test_adaptive_video_output(720, 3_000_000)),
            adaptive_ladder: Some(AdaptiveLadderPlan {
                rungs,
                starting_rung_id: "0".to_string(),
                active_rung_id: "0".to_string(),
                audio_strategy: AdaptiveAudioStrategy::PerRung,
                reasons: vec!["adaptive_ladder_source_aware".to_string()],
            }),
            video_transcode_reason: Some("adaptive_quality_requested".to_string()),
            workload_class: None,
            feasibility: None,
            compatibility_report: CompatibilityReport::empty("media-file"),
            reasons: vec!["adaptive_transcode_automatic_quality_requested".to_string()],
            warnings: Vec::new(),
            expected_outputs: Vec::new(),
            playable: true,
        }
    }

    fn test_feasibility_decision(envelope_id: &str) -> PlaybackFeasibilityDecision {
        PlaybackFeasibilityDecision {
            action: PlaybackFeasibilityAction::AllowTranscode,
            reason: "certified_realtime".to_string(),
            support_decision: PlaybackSupportDecision::Supported,
            performance_decision: PlaybackPerformanceDecision::RealtimeSafe,
            confidence: PlaybackPerformanceConfidence::Certified,
            selected_envelope_id: Some(envelope_id.to_string()),
            selected_hardware_api: Some("nvenc".to_string()),
            selected_envelope_p50_realtime_factor_millis: Some(1_800),
            selected_envelope_p95_realtime_factor_millis: Some(1_500),
            selected_envelope_startup_latency_ms: Some(400),
            selected_envelope_first_segment_latency_ms: Some(900),
            selected_envelope_failure_count: Some(0),
            selected_envelope_sample_count: Some(8),
            realtime_required_millis: 1000,
            reasons: vec!["certification_artifact".to_string()],
            warnings: Vec::new(),
            remediation_codes: Vec::new(),
            background_probe_queued: false,
        }
    }

    fn test_playback_job_with_plan(
        playback_plan: PlaybackPlan,
        started_at: DateTime<Utc>,
    ) -> PlaybackJob {
        let session_id = Uuid::new_v4();
        PlaybackJob {
            plan: PlaybackJobPlan::new(
                session_id,
                "media-file",
                "source.mkv",
                TranscodeParams {
                    seek_seconds: 0.0,
                    mode: PlaybackMode::AdaptiveTranscode,
                    delivery: Delivery::HlsAdaptiveFmp4,
                },
                Some(serde_json::to_value(playback_plan).expect("serialize playback plan")),
            ),
            state: PlaybackJobState::Running,
            temp_dir: PathBuf::from("/tmp/elixir-test"),
            artifacts: ArtifactRegistry::for_plan(
                PlaybackMode::AdaptiveTranscode,
                Delivery::HlsAdaptiveFmp4,
                0,
            ),
            process_id: None,
            process_group_id: None,
            child: None,
            capacity_permits: Vec::new(),
            started_at: Some(started_at),
            last_progress_at: Some(started_at),
            last_segment_at: None,
            log_path: PathBuf::from("/tmp/elixir-test/ffmpeg.log"),
            error: None,
            error_code: None,
            error_kind: None,
            log_tail: None,
            subtitle_delay_seconds: None,
            subtitles: Vec::new(),
            active_rung_id: Some("0".to_string()),
        }
    }

    #[test]
    fn phase20_failure_observation_extracts_selected_envelope_and_latency_bucket() {
        let mut playback_plan = test_adaptive_playback_plan();
        playback_plan.feasibility = Some(test_feasibility_decision("env-selected"));
        let started_at = Utc::now();
        let job = test_playback_job_with_plan(playback_plan, started_at);
        let observed_at = started_at + chrono::Duration::milliseconds(1_500);

        let first_segment = playback_performance_failure_observation(
            &job,
            "first_segment_timeout",
            "first_segment_timeout",
            observed_at,
        )
        .expect("first segment failure observation");
        assert_eq!(first_segment.envelope_id, "env-selected");
        assert_eq!(first_segment.startup_latency_ms, None);
        assert_eq!(first_segment.first_segment_latency_ms, Some(1_500));
        assert_eq!(
            first_segment.failure_kind.as_deref(),
            Some("first_segment_timeout")
        );
        assert_eq!(
            first_segment.output_mode.as_deref(),
            Some("adaptive_transcode")
        );

        let startup = playback_performance_failure_observation(
            &job,
            "startup_timeout",
            "startup_timeout",
            observed_at,
        )
        .expect("startup failure observation");
        assert_eq!(startup.envelope_id, "env-selected");
        assert_eq!(startup.startup_latency_ms, Some(1_500));
        assert_eq!(startup.first_segment_latency_ms, None);
    }

    #[tokio::test]
    async fn artifact_lookup_uses_per_job_lock_not_global_startup_lock() -> Result<()> {
        let (manager, database) = test_manager().await?;
        let ready_session = Uuid::new_v4();
        let blocked_session = Uuid::new_v4();
        insert_session(&database.pool, ready_session).await?;
        insert_session(&database.pool, blocked_session).await?;

        let ready_temp = tempdir()?;
        fs::write(ready_temp.path().join("seg_0_00000.ts"), b"segment").await?;
        manager
            .insert_test_job(ready_session, ready_temp.path().to_path_buf(), 0)
            .await;

        let blocked_temp = tempdir()?;
        manager
            .insert_test_job(blocked_session, blocked_temp.path().to_path_buf(), 0)
            .await;
        let blocked = manager
            .jobs
            .get(&blocked_session)
            .map(|entry| entry.value().clone())
            .expect("blocked job");
        let _held_lock = blocked.lock().await;

        let artifact = time::timeout(
            Duration::from_millis(100),
            manager.lookup_artifact(ready_session, "seg_0_00000.ts"),
        )
        .await?
        .expect("artifact should resolve while another job lock is held");
        assert_eq!(artifact.name, "seg_0_00000.ts");
        Ok(())
    }

    #[tokio::test]
    async fn adaptive_artifact_lookup_updates_active_rung_snapshot() -> Result<()> {
        let (manager, database) = test_manager().await?;
        let session_id = Uuid::new_v4();
        insert_session(&database.pool, session_id).await?;
        let temp = tempdir()?;
        let playback_plan = test_adaptive_playback_plan();
        let job = PlaybackJob {
            plan: PlaybackJobPlan::new(
                session_id,
                "media-file",
                "test-media",
                TranscodeParams {
                    seek_seconds: 0.0,
                    mode: PlaybackMode::AdaptiveTranscode,
                    delivery: Delivery::HlsAdaptiveFmp4,
                },
                Some(serde_json::to_value(playback_plan)?),
            ),
            state: PlaybackJobState::Running,
            temp_dir: temp.path().to_path_buf(),
            artifacts: ArtifactRegistry::for_plan(
                PlaybackMode::AdaptiveTranscode,
                Delivery::HlsAdaptiveFmp4,
                0,
            ),
            process_id: None,
            process_group_id: None,
            child: None,
            capacity_permits: Vec::new(),
            started_at: Some(Utc::now()),
            last_progress_at: Some(Utc::now()),
            last_segment_at: Some(Utc::now()),
            log_path: temp.path().join("ffmpeg.log"),
            error: None,
            error_code: None,
            error_kind: None,
            log_tail: None,
            subtitle_delay_seconds: None,
            subtitles: Vec::new(),
            active_rung_id: None,
        };
        manager.jobs.insert(session_id, Arc::new(Mutex::new(job)));

        let artifact = manager
            .lookup_artifact(session_id, "seg_1_00000.m4s")
            .await
            .context("adaptive segment should resolve")?;

        assert_eq!(artifact.name, "seg_1_00000.m4s");
        let snapshot = manager.snapshot(session_id).await.context("job snapshot")?;
        assert_eq!(
            snapshot
                .active_rung
                .as_ref()
                .and_then(|rung| rung.get("id"))
                .and_then(Value::as_str),
            Some("1")
        );
        let stored: Option<String> =
            sqlx::query_scalar("SELECT job_state_json FROM playback_sessions WHERE id = ?")
                .bind(session_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        let stored: Value = serde_json::from_str(stored.as_deref().unwrap_or("{}"))?;
        assert_eq!(
            stored
                .get("active_rung")
                .and_then(|rung| rung.get("id"))
                .and_then(Value::as_str),
            Some("1"),
            "{stored}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn stop_removes_temp_dir_and_persists_stopped_state() -> Result<()> {
        let (manager, database) = test_manager().await?;
        let session_id = Uuid::new_v4();
        insert_session(&database.pool, session_id).await?;
        let temp = tempdir()?;
        let temp_path = temp.path().to_path_buf();
        fs::write(temp_path.join("seg_0_00000.ts"), b"segment").await?;
        manager
            .insert_test_job(session_id, temp_path.clone(), 0)
            .await;

        manager.stop(session_id, "test_stop").await;

        assert!(fs::metadata(&temp_path).await.is_err());
        let stored: Option<String> =
            sqlx::query_scalar("SELECT job_state_json FROM playback_sessions WHERE id = ?")
                .bind(session_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        let stored: Value = serde_json::from_str(stored.as_deref().unwrap_or("{}"))?;
        assert_eq!(stored.get("state").and_then(Value::as_str), Some("stopped"));
        Ok(())
    }

    #[tokio::test]
    async fn restart_releases_existing_capacity_permit_before_relaunch() -> Result<()> {
        let (manager, database) = test_manager().await?;
        let session_id = Uuid::new_v4();
        insert_session(&database.pool, session_id).await?;
        let temp = tempdir()?;
        let permit = manager.capacities.video.clone().acquire_owned().await?;

        let job = PlaybackJob {
            plan: PlaybackJobPlan::new(
                session_id,
                "media-file",
                "missing-media",
                TranscodeParams {
                    seek_seconds: 0.0,
                    mode: PlaybackMode::VideoTranscode,
                    delivery: Delivery::HlsMpegts,
                },
                None,
            ),
            state: PlaybackJobState::Running,
            temp_dir: temp.path().to_path_buf(),
            artifacts: ArtifactRegistry::for_transcode(0),
            process_id: None,
            process_group_id: None,
            child: None,
            capacity_permits: vec![permit],
            started_at: Some(Utc::now()),
            last_progress_at: Some(Utc::now()),
            last_segment_at: Some(Utc::now()),
            log_path: temp.path().join("ffmpeg.log"),
            error: None,
            error_code: None,
            error_kind: None,
            log_tail: None,
            subtitle_delay_seconds: None,
            subtitles: Vec::new(),
            active_rung_id: None,
        };
        manager.jobs.insert(session_id, Arc::new(Mutex::new(job)));

        let result =
            time::timeout(Duration::from_secs(5), manager.restart_at(session_id, 12.0)).await;
        assert!(result.is_ok(), "restart should not wait on its own permit");
        assert!(
            result.unwrap().is_err(),
            "missing media should fail after acquiring restart capacity"
        );
        Ok(())
    }

    #[tokio::test]
    async fn failed_job_releases_capacity_permit() -> Result<()> {
        let (manager, database) = test_manager().await?;
        let session_id = Uuid::new_v4();
        insert_session(&database.pool, session_id).await?;
        let temp = tempdir()?;
        let permit = manager.capacities.video.clone().acquire_owned().await?;
        let job = PlaybackJob {
            plan: PlaybackJobPlan::new(
                session_id,
                "media-file",
                "test-media",
                TranscodeParams {
                    seek_seconds: 0.0,
                    mode: PlaybackMode::VideoTranscode,
                    delivery: Delivery::HlsMpegts,
                },
                None,
            ),
            state: PlaybackJobState::Running,
            temp_dir: temp.path().to_path_buf(),
            artifacts: ArtifactRegistry::for_transcode(0),
            process_id: None,
            process_group_id: None,
            child: None,
            capacity_permits: vec![permit],
            started_at: Some(Utc::now()),
            last_progress_at: Some(Utc::now()),
            last_segment_at: Some(Utc::now()),
            log_path: temp.path().join("ffmpeg.log"),
            error: None,
            error_code: None,
            error_kind: None,
            log_tail: None,
            subtitle_delay_seconds: None,
            subtitles: Vec::new(),
            active_rung_id: None,
        };
        manager.jobs.insert(session_id, Arc::new(Mutex::new(job)));

        manager
            .fail_job(session_id, "startup_failed", None, true)
            .await;

        let reacquired = time::timeout(
            Duration::from_millis(100),
            manager.capacities.video.clone().acquire_owned(),
        )
        .await;
        assert!(
            reacquired.is_ok(),
            "failed job should release video capacity"
        );
        Ok(())
    }

    #[tokio::test]
    async fn failed_startup_persists_structured_state_and_redacted_log_tail() -> Result<()> {
        let (manager, database) = test_manager().await?;
        let session_id = Uuid::new_v4();
        insert_session(&database.pool, session_id).await?;
        let temp = tempdir()?;
        let log_path = temp.path().join("ffmpeg.log");
        fs::write(
            &log_path,
            "opening /sessions/id/master.m3u8?session=secret&token=secret2\ninvalid data found\n",
        )
        .await?;
        manager
            .insert_test_job(session_id, temp.path().to_path_buf(), 0)
            .await;

        manager
            .fail_job(
                session_id,
                "startup_failed",
                Some("ffmpeg exited before playlist".to_string()),
                false,
            )
            .await;

        let stored: Option<String> =
            sqlx::query_scalar("SELECT job_state_json FROM playback_sessions WHERE id = ?")
                .bind(session_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        let stored: Value = serde_json::from_str(stored.as_deref().unwrap_or("{}"))?;
        assert_eq!(stored.get("state").and_then(Value::as_str), Some("failed"));
        assert_eq!(
            stored.get("error_code").and_then(Value::as_str),
            Some("startup_failed")
        );
        assert_eq!(
            stored.get("error_kind").and_then(Value::as_str),
            Some("ffmpeg_exit")
        );
        assert!(
            stored
                .get("error")
                .and_then(Value::as_str)
                .is_some_and(|error| error.contains("startup_failed")),
            "{stored}"
        );
        let tail = stored
            .get("log_tail")
            .and_then(Value::as_str)
            .context("failed state should persist log tail")?;
        assert!(tail.contains("invalid data found"), "{tail}");
        assert!(!tail.contains("secret"), "{tail}");
        assert!(tail.contains("session=[redacted]"), "{tail}");
        assert!(tail.contains("token=[redacted]"), "{tail}");
        Ok(())
    }

    #[test]
    fn classifies_ffmpeg_filter_failures_as_unsupported_filter() {
        assert_eq!(
            classify_playback_failure(
                "first_segment_failed",
                Some("ffmpeg exited before first media segment was ready"),
                Some("No such filter: 'subtitles'\nError initializing filter")
            ),
            "unsupported_filter"
        );
    }

    #[test]
    fn classifies_hardware_encoder_failures_as_hardware_unavailable() {
        assert_eq!(
            classify_playback_failure(
                "startup_failed",
                Some("ffmpeg exited before playlist"),
                Some("h264_videotoolbox encoder failed; device creation failed")
            ),
            "hardware_unavailable"
        );
    }

    fn hardware_playback_plan_fixture() -> PlaybackPlan {
        let mut report = CompatibilityReport::empty("media-file");
        report.source_video_codec = Some("h264".to_string());
        PlaybackPlan {
            plan_version: PLAYBACK_PLAN_VERSION,
            mode: PlaybackMode::VideoTranscode,
            delivery: Delivery::HlsFmp4,
            media_file_id: "media-file".to_string(),
            selected_video_track: Some(0),
            video_action: StreamAction::Transcode,
            audio_action: StreamAction::Transcode,
            subtitle_action: StreamAction::Disabled,
            seek_behavior: SeekBehavior::ServerHlsRestart,
            adaptive: false,
            selected_audio_track: Some(1),
            selected_subtitle_track: None,
            hdr_action: HdrAction::None,
            hardware_acceleration: HardwareAccelerationPlan {
                enabled: true,
                api: Some("videotoolbox".to_string()),
                decoder: Some("videotoolbox".to_string()),
                encoder: Some("h264_videotoolbox".to_string()),
                fallback: Some("software".to_string()),
                ..HardwareAccelerationPlan::default()
            },
            audio_output: None,
            video_output: Some(VideoOutputPlan {
                codec: "h264".to_string(),
                encoder: "h264_videotoolbox".to_string(),
                preset: "veryfast".to_string(),
                profile: Some("high".to_string()),
                level: Some("4.1".to_string()),
                crf: None,
                bitrate_bps: Some(3_000_000),
                maxrate_bps: Some(3_000_000),
                bufsize_bps: Some(6_000_000),
                pixel_format: Some("yuv420p".to_string()),
                scale: None,
                tone_map: None,
                frame_rate: VideoFrameRatePlan {
                    mode: VideoFrameRateMode::Source,
                    source_fps: Some("24".to_string()),
                    target_fps: None,
                },
                gop_frames: Some(96),
                segment_seconds: "4".to_string(),
                keyframe_expression: "expr:gte(t,n_forced*4)".to_string(),
                hls_delivery: Delivery::HlsFmp4,
                burn_in: None,
                reasons: vec!["hardware_encoder_selected:h264_videotoolbox".to_string()],
            }),
            adaptive_ladder: None,
            video_transcode_reason: Some("source_bitrate_exceeds_policy".to_string()),
            workload_class: None,
            feasibility: None,
            compatibility_report: report,
            reasons: vec!["source_bitrate_exceeds_policy".to_string()],
            warnings: Vec::new(),
            expected_outputs: Vec::new(),
            playable: true,
        }
    }

    #[tokio::test]
    async fn hardware_unavailable_failure_invokes_refresh_callback() -> Result<()> {
        let mut settings = Settings::default();
        settings.database.url = "sqlite::memory:?cache=shared".to_string();
        settings.database.max_connections = 1;
        let database = Database::connect(&settings.database).await?;
        database.run_migrations().await?;
        let calls = Arc::new(AtomicUsize::new(0));
        let callback_calls = calls.clone();
        let manager = PlaybackJobManager::with_capacity_limits_and_hardware_failure_callback(
            database.pool.clone(),
            PlaybackJobCapacityLimits::default(),
            PlaybackJobLimits::default(),
            Some(Arc::new(move || {
                callback_calls.fetch_add(1, Ordering::SeqCst);
            })),
        );
        let session_id = Uuid::new_v4();
        insert_session(&database.pool, session_id).await?;
        let temp = tempdir()?;
        let log_path = temp.path().join("ffmpeg.log");
        fs::write(
            &log_path,
            "h264_videotoolbox encoder failed; device creation failed\n",
        )
        .await?;
        let job = PlaybackJob {
            plan: PlaybackJobPlan::new(
                session_id,
                "media-file",
                "test-media",
                TranscodeParams {
                    seek_seconds: 0.0,
                    mode: PlaybackMode::VideoTranscode,
                    delivery: Delivery::HlsFmp4,
                },
                Some(serde_json::to_value(hardware_playback_plan_fixture())?),
            ),
            state: PlaybackJobState::Running,
            temp_dir: temp.path().to_path_buf(),
            artifacts: ArtifactRegistry::for_transcode(0),
            process_id: None,
            process_group_id: None,
            child: None,
            capacity_permits: Vec::new(),
            started_at: Some(Utc::now()),
            last_progress_at: Some(Utc::now()),
            last_segment_at: Some(Utc::now()),
            log_path,
            error: None,
            error_code: None,
            error_kind: None,
            log_tail: None,
            subtitle_delay_seconds: None,
            subtitles: Vec::new(),
            active_rung_id: None,
        };
        manager.jobs.insert(session_id, Arc::new(Mutex::new(job)));

        manager
            .fail_job(
                session_id,
                "startup_failed",
                Some("ffmpeg exited before playlist".to_string()),
                false,
            )
            .await;

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[test]
    fn hardware_failure_fallback_rewrites_plan_to_software_once() {
        let session_id = Uuid::new_v4();
        let mut report = CompatibilityReport::empty("media-file");
        report.source_video_codec = Some("h264".to_string());
        let playback_plan = PlaybackPlan {
            plan_version: PLAYBACK_PLAN_VERSION,
            mode: PlaybackMode::VideoTranscode,
            delivery: Delivery::HlsFmp4,
            media_file_id: "media-file".to_string(),
            selected_video_track: Some(0),
            video_action: StreamAction::Transcode,
            audio_action: StreamAction::Transcode,
            subtitle_action: StreamAction::Disabled,
            seek_behavior: SeekBehavior::ServerHlsRestart,
            adaptive: false,
            selected_audio_track: Some(1),
            selected_subtitle_track: None,
            hdr_action: HdrAction::None,
            hardware_acceleration: HardwareAccelerationPlan {
                enabled: true,
                api: Some("videotoolbox".to_string()),
                decoder: Some("videotoolbox".to_string()),
                encoder: Some("h264_videotoolbox".to_string()),
                fallback: Some("software".to_string()),
                ..HardwareAccelerationPlan::default()
            },
            audio_output: None,
            video_output: Some(VideoOutputPlan {
                codec: "h264".to_string(),
                encoder: "h264_videotoolbox".to_string(),
                preset: "veryfast".to_string(),
                profile: Some("high".to_string()),
                level: Some("4.1".to_string()),
                crf: None,
                bitrate_bps: Some(3_000_000),
                maxrate_bps: Some(3_000_000),
                bufsize_bps: Some(6_000_000),
                pixel_format: Some("yuv420p".to_string()),
                scale: None,
                tone_map: None,
                frame_rate: VideoFrameRatePlan {
                    mode: VideoFrameRateMode::Source,
                    source_fps: Some("24".to_string()),
                    target_fps: None,
                },
                gop_frames: Some(96),
                segment_seconds: "4".to_string(),
                keyframe_expression: "expr:gte(t,n_forced*4)".to_string(),
                hls_delivery: Delivery::HlsFmp4,
                burn_in: None,
                reasons: vec!["hardware_encoder_selected:h264_videotoolbox".to_string()],
            }),
            adaptive_ladder: None,
            video_transcode_reason: Some("source_bitrate_exceeds_policy".to_string()),
            workload_class: None,
            feasibility: None,
            compatibility_report: report,
            reasons: vec!["source_bitrate_exceeds_policy".to_string()],
            warnings: Vec::new(),
            expected_outputs: Vec::new(),
            playable: true,
        };
        let job_plan = PlaybackJobPlan::new(
            session_id,
            "media-file",
            "/media/source.mkv",
            TranscodeParams {
                seek_seconds: 0.0,
                mode: PlaybackMode::VideoTranscode,
                delivery: Delivery::HlsFmp4,
            },
            Some(serde_json::to_value(playback_plan).unwrap()),
        );

        let fallback = software_fallback_job_plan(&job_plan, "h264_videotoolbox failed").unwrap();
        let parsed = fallback.parsed_playback_plan().unwrap();

        assert!(!parsed.hardware_acceleration.enabled);
        assert_eq!(
            parsed.hardware_acceleration.fallback.as_deref(),
            Some("software_after_hardware_failure")
        );
        assert!(
            parsed
                .reasons
                .contains(&"hardware_startup_failed_software_retry".to_string()),
            "{:?}",
            parsed.reasons
        );
        assert_eq!(
            parsed
                .video_output
                .as_ref()
                .map(|output| output.encoder.as_str()),
            Some("libx264")
        );
        assert!(
            parsed
                .video_output
                .as_ref()
                .unwrap()
                .reasons
                .contains(&"hardware_fallback_to_software".to_string())
        );
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn stop_kills_process_group_and_removes_temp_dir() -> Result<()> {
        let (manager, database) = test_manager().await?;
        let session_id = Uuid::new_v4();
        insert_session(&database.pool, session_id).await?;
        let temp = tempdir()?;
        let temp_path = temp.path().to_path_buf();

        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg("sleep 60")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .process_group(0);
        let child = command.spawn()?;
        let pid = child.id().context("child pid")?;

        let job = PlaybackJob {
            plan: PlaybackJobPlan::new(
                session_id,
                "media-file",
                "test-media",
                TranscodeParams {
                    seek_seconds: 0.0,
                    mode: PlaybackMode::VideoTranscode,
                    delivery: Delivery::HlsMpegts,
                },
                None,
            ),
            state: PlaybackJobState::Running,
            temp_dir: temp_path.clone(),
            artifacts: ArtifactRegistry::for_transcode(0),
            process_id: Some(pid),
            process_group_id: Some(pid),
            child: Some(child),
            capacity_permits: Vec::new(),
            started_at: Some(Utc::now()),
            last_progress_at: Some(Utc::now()),
            last_segment_at: Some(Utc::now()),
            log_path: temp_path.join("ffmpeg.log"),
            error: None,
            error_code: None,
            error_kind: None,
            log_tail: None,
            subtitle_delay_seconds: None,
            subtitles: Vec::new(),
            active_rung_id: None,
        };
        manager.jobs.insert(session_id, Arc::new(Mutex::new(job)));

        manager.stop(session_id, "test_stop").await;

        assert!(fs::metadata(&temp_path).await.is_err());
        let alive = Command::new("kill")
            .arg("-0")
            .arg(pid.to_string())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .await
            .map(|status| status.success())
            .unwrap_or(false);
        assert!(!alive, "process {pid} should have been killed");
        Ok(())
    }
}
