use anyhow::{Context, Result, bail};
use chrono::{DateTime, Duration as ChronoDuration, NaiveDateTime, Utc};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{AnyPool, Row, any::AnyRow};
use std::collections::{BTreeMap, BTreeSet};
use std::process::Output;
use std::process::Stdio;
use std::sync::Mutex;
use std::time::{Duration as StdDuration, Instant as StdInstant};
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::time::{MissedTickBehavior, sleep, timeout};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    db::models::{Extension, ExtensionInstance, Provider, ProviderHealthState},
    extensions::store::ExtensionStore,
    http::handlers::extensions::resolve_control_provider_transport_base_url,
    media::ffprobe,
    metrics,
    orchestrator::model::ProviderEndpoint,
    state::AppState,
};

const PROVIDER_CHAPTER: &str = "chapter";
const CHAPTER_PROVIDER_ID: &str = "ffprobe_chapters";
const CHAPTER_PROVIDER_VERSION: &str = "1";
const DURATION_TOLERANCE_SECONDS: f64 = 5.0;
const EARLY_WINDOW_FRACTION: f64 = 0.35;
const EARLY_WINDOW_MAX_SECONDS: f64 = 900.0;
const LATE_WINDOW_FRACTION: f64 = 0.50;
const PROVIDER_THEINTRODB: &str = "theintrodb";
const PROVIDER_ANISKIP: &str = "aniskip";
const PROVIDER_LOCAL_AUDIO_RECURRING: &str = "local_audio_recurring";
const PROVIDER_LOCAL_VISUAL_RECURRING: &str = "local_visual_recurring";
const MEDIA_SEGMENT_PROVIDER_CAPABILITY: &str = "media.segment_provider";
const MEDIA_SEGMENT_PROVIDER_SCHEMA_VERSION: &str = "elixir.media_segment_provider.v1";
const MEDIA_SEGMENT_PROVIDER_LOOKUP_PATH: &str = "segment-provider/lookup";
const MEDIA_SEGMENT_PROVIDER_RESPONSE_MAX_BYTES: u64 = 256 * 1024;
const MEDIA_SEGMENT_PROVIDER_MAX_SEGMENTS: usize = 128;
const MEDIA_SEGMENT_PROVIDER_CERTIFICATION_POLICY_VERSION: &str = "midm-segment-provider-cert-v1";
const MEDIA_SEGMENT_PROVIDER_CERTIFICATION_EXPIRES_DAYS: i64 = 30;
const MEDIA_SEGMENT_JOB_PROVIDER_REFRESH: &str = "provider_refresh";
const MEDIA_SEGMENT_JOB_LOCAL_DETECTOR: &str = "local_detector";
const MEDIA_SEGMENT_JOB_AUDIO_FINGERPRINT: &str = "audio_fingerprint";
const MEDIA_SEGMENT_JOB_VIDEO_FRAME_HASH: &str = "video_frame_hash";
const MEDIA_SEGMENT_JOB_SCOPE_MEDIA_FILE: &str = "media_file";
const MEDIA_SEGMENT_JOB_SCOPE_SEASON: &str = "season";
const DEFAULT_THEINTRODB_BASE_URL: &str = "https://api.introdb.app";
const DEFAULT_ANISKIP_BASE_URL: &str = "https://api.aniskip.com";
const DEFAULT_PROVIDER_TIMEOUT_SECONDS: u64 = 8;
const DEFAULT_PROVIDER_CACHE_TTL_SECONDS: i64 = 60 * 60 * 24;
const DEFAULT_PROVIDER_RATE_LIMIT_PER_MINUTE: i64 = 30;
const PROVIDER_RATE_LIMIT_WINDOW_SECONDS: i64 = 60;
const PROVIDER_JOB_MAX_ATTEMPTS: i64 = 5;
const PROVIDER_JOB_RETRY_BACKOFF_SECONDS: i64 = 300;
const MEDIA_SEGMENT_WORKER_INTERVAL_SECONDS: u64 = 60;
const MEDIA_SEGMENT_WORKER_ENQUEUE_BATCH_LIMIT: usize = 50;
const MEDIA_SEGMENT_WORKER_RUN_BATCH_LIMIT: usize = 4;
const MEDIA_SEGMENT_WORKER_MAX_RUNTIME_SECONDS: u64 = 300;
const MEDIA_SEGMENT_STALE_RUNNING_JOB_SECONDS: i64 = 60 * 60 * 6;
const MEDIA_SEGMENT_JOB_CANCELLATION_POLL_SECONDS: u64 = 2;
const LOCAL_AUDIO_DETECTOR_VERSION: &str = "local-audio-recurring-v1";
const LOCAL_AUDIO_FINGERPRINT_VERSION: &str = "local-audio-fingerprint-v1";
const LOCAL_AUDIO_DETECTOR_MIN_REPEAT_COUNT: usize = 2;
const LOCAL_AUDIO_DETECTOR_MIN_SEASON_FILES: usize = 2;
const LOCAL_AUDIO_DETECTOR_MAX_INTRO_START_SECONDS: f64 = 600.0;
const LOCAL_AUDIO_DETECTOR_MIN_CONFIDENCE: f64 = 0.82;
const LOCAL_AUDIO_FINGERPRINT_SAMPLE_RATE_HZ: u32 = 8_000;
const LOCAL_AUDIO_FINGERPRINT_FRAME_SECONDS: f64 = 0.5;
const LOCAL_AUDIO_FINGERPRINT_STEP_SECONDS: f64 = 15.0;
const LOCAL_AUDIO_FINGERPRINT_MAX_RANGE_SECONDS: f64 = 900.0;
const LOCAL_AUDIO_FINGERPRINT_MIN_DURATION_SECONDS: f64 = 120.0;
const LOCAL_AUDIO_FINGERPRINT_MAX_WINDOWS_PER_FILE: usize = 160;
const LOCAL_AUDIO_FINGERPRINT_TIMEOUT_SECONDS: u64 = 120;
const LOCAL_AUDIO_FINGERPRINT_FFMPEG_STDERR_LIMIT: usize = 2_048;
const LOCAL_AUDIO_FINGERPRINT_WINDOW_LENGTHS_SECONDS: [f64; 2] = [60.0, 90.0];
const LOCAL_VISUAL_DETECTOR_VERSION: &str = "local-visual-credits-v1";
const LOCAL_VISUAL_CREDITS_MIN_FRAME_COUNT: usize = 3;
const LOCAL_VISUAL_CREDITS_MIN_SPAN_SECONDS: f64 = 20.0;
const LOCAL_VISUAL_CREDITS_MIN_DURATION_SECONDS: f64 = 300.0;
const LOCAL_VISUAL_CREDITS_MIN_START_FRACTION: f64 = 0.60;
const LOCAL_VISUAL_CREDITS_MAX_FRAME_GAP_SECONDS: f64 = 90.0;
const LOCAL_VISUAL_POST_CREDIT_SCENE_MIN_FRAMES: usize = 2;
const LOCAL_VISUAL_POST_CREDIT_SCENE_MIN_SPAN_SECONDS: f64 = 20.0;
const LOCAL_VISUAL_CREDITS_BLACK_RATIO_THRESHOLD: f64 = 0.70;
const LOCAL_VISUAL_CREDITS_TEXT_RATIO_THRESHOLD: f64 = 0.08;
const LOCAL_VISUAL_CREDITS_MIN_CONFIDENCE: f64 = 0.82;
const LOCAL_VISUAL_FRAME_HASH_VERSION: &str = "local-visual-frame-hash-v1";
const LOCAL_VISUAL_FRAME_HASH_WIDTH: usize = 160;
const LOCAL_VISUAL_FRAME_HASH_HEIGHT: usize = 90;
const LOCAL_VISUAL_FRAME_HASH_STEP_SECONDS: f64 = 30.0;
const LOCAL_VISUAL_FRAME_HASH_MAX_RANGE_SECONDS: f64 = 3_600.0;
const LOCAL_VISUAL_FRAME_HASH_MAX_FRAMES_PER_FILE: usize = 120;
const LOCAL_VISUAL_FRAME_HASH_TIMEOUT_SECONDS: u64 = 180;
const LOCAL_VISUAL_FRAME_HASH_FFMPEG_STDERR_LIMIT: usize = 2_048;
const LOCAL_VISUAL_FRAME_BLACK_LUMA_THRESHOLD: u8 = 48;
const LOCAL_VISUAL_FRAME_EDGE_LUMA_DELTA_THRESHOLD: u8 = 40;

static MEDIA_SEGMENTS_ACTIVE_LABELS: Lazy<Mutex<BTreeSet<(String, String)>>> =
    Lazy::new(|| Mutex::new(BTreeSet::new()));

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentCandidateInput {
    #[serde(alias = "mediaFileId")]
    pub media_file_id: String,
    #[serde(alias = "itemType")]
    pub item_type: Option<String>,
    #[serde(alias = "itemId")]
    pub item_id: Option<String>,
    #[serde(alias = "segmentType")]
    pub segment_type: String,
    #[serde(alias = "startSeconds")]
    pub start_seconds: f64,
    #[serde(alias = "endSeconds")]
    pub end_seconds: f64,
    #[serde(alias = "providerKind")]
    pub provider_kind: String,
    #[serde(alias = "providerId")]
    pub provider_id: String,
    #[serde(alias = "providerVersion")]
    pub provider_version: Option<String>,
    pub confidence: f64,
    #[serde(alias = "identityStrength")]
    pub identity_strength: String,
    #[serde(alias = "sourcePayload")]
    pub source_payload: Option<Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SegmentCandidateRecord {
    pub id: String,
    pub media_file_id: String,
    pub item_type: String,
    pub item_id: String,
    pub segment_type: String,
    pub start_seconds: f64,
    pub end_seconds: f64,
    pub provider_kind: String,
    pub provider_id: String,
    pub provider_version: Option<String>,
    pub confidence: f64,
    pub validation_state: String,
    pub validation_reason: Option<String>,
    pub identity_strength: String,
    pub source_payload: Option<Value>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ActiveMediaSegmentRecord {
    pub id: String,
    pub media_file_id: String,
    pub item_type: String,
    pub item_id: String,
    pub segment_type: String,
    pub start_seconds: f64,
    pub end_seconds: f64,
    pub canonical_candidate_id: Option<String>,
    pub source_label: String,
    pub confidence: f64,
    pub locked: bool,
    pub status: String,
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SegmentCandidateOutcome {
    pub candidate: SegmentCandidateRecord,
    pub activated_segment: Option<ActiveMediaSegmentRecord>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct ChapterIngestionSummary {
    pub media_file_id: String,
    pub chapters_seen: usize,
    pub candidates_submitted: usize,
    pub candidates_accepted: usize,
    pub candidates_rejected: usize,
    pub active_segments: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlaybackInteractionPreferences {
    pub skip_intro_behavior: String,
    pub skip_recap_behavior: String,
    pub skip_preview_behavior: String,
    pub skip_credits_behavior: String,
    pub skip_outro_behavior: String,
    pub autoplay_enabled: bool,
    pub autoplay_countdown_seconds: i32,
    pub autoplay_max_consecutive: i32,
    pub autoplay_max_elapsed_minutes: i32,
    pub segment_provider_settings: Value,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct PlaybackInteractionPreferencesPatch {
    #[serde(alias = "skipIntroBehavior")]
    pub skip_intro_behavior: Option<String>,
    #[serde(alias = "skipRecapBehavior")]
    pub skip_recap_behavior: Option<String>,
    #[serde(alias = "skipPreviewBehavior")]
    pub skip_preview_behavior: Option<String>,
    #[serde(alias = "skipCreditsBehavior")]
    pub skip_credits_behavior: Option<String>,
    #[serde(alias = "skipOutroBehavior")]
    pub skip_outro_behavior: Option<String>,
    #[serde(alias = "autoplayEnabled")]
    pub autoplay_enabled: Option<bool>,
    #[serde(alias = "autoplayCountdownSeconds")]
    pub autoplay_countdown_seconds: Option<i32>,
    #[serde(alias = "autoplayMaxConsecutive")]
    pub autoplay_max_consecutive: Option<i32>,
    #[serde(alias = "autoplayMaxElapsedMinutes")]
    pub autoplay_max_elapsed_minutes: Option<i32>,
    #[serde(alias = "segmentProviderSettings")]
    pub segment_provider_settings: Option<Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MediaInteractionLibrarySettingsRecord {
    pub source_config_id: String,
    pub extension_id: String,
    pub source_enabled: bool,
    pub segment_provider_settings: Value,
    pub effective_segment_provider_settings: Value,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct MediaInteractionLibrarySettingsPatch {
    #[serde(alias = "segmentProviderSettings")]
    pub segment_provider_settings: Option<Value>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct BuiltinProviderRefreshSummary {
    pub media_file_id: String,
    pub providers: Vec<BuiltinProviderRefreshOutcome>,
    pub candidates_submitted: usize,
    pub candidates_accepted: usize,
    pub candidates_rejected: usize,
    pub active_segments: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct BuiltinProviderRefreshOutcome {
    pub provider_kind: String,
    pub enabled: bool,
    pub status: String,
    pub cache_hit: bool,
    pub candidate_count: usize,
    pub accepted_count: usize,
    pub rejected_count: usize,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct BuiltinProviderRefreshOptions {
    #[serde(alias = "forceRefresh")]
    pub force_refresh: Option<bool>,
    #[serde(alias = "providerKind")]
    pub provider_kind: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MediaSegmentJobRecord {
    pub id: String,
    pub job_type: String,
    pub scope_type: String,
    pub scope_id: String,
    pub provider_kind: String,
    pub status: String,
    pub priority: i64,
    pub attempts: i64,
    pub max_attempts: i64,
    pub next_attempt_at: Option<String>,
    pub locked_by: Option<String>,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub error: Option<Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MediaSegmentJobRunRecord {
    pub job: MediaSegmentJobRecord,
    pub summary: Option<BuiltinProviderRefreshSummary>,
    pub local_audio_fingerprint_summary: Option<LocalAudioFingerprintSummary>,
    pub local_visual_frame_hash_summary: Option<LocalVisualFrameHashSummary>,
    pub local_audio_summary: Option<LocalAudioDetectorSummary>,
    pub local_visual_summary: Option<LocalVisualDetectorSummary>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct MediaSegmentJobListFilters {
    pub status: Option<String>,
    #[serde(alias = "providerKind")]
    pub provider_kind: Option<String>,
    #[serde(alias = "jobType")]
    pub job_type: Option<String>,
    #[serde(alias = "scopeType")]
    pub scope_type: Option<String>,
    #[serde(alias = "scopeId")]
    pub scope_id: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct MediaSegmentProviderCertificationFilters {
    #[serde(alias = "providerId")]
    pub provider_id: Option<String>,
    #[serde(alias = "providerKind")]
    pub provider_kind: Option<String>,
    pub status: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MediaSegmentProviderCertificationRecord {
    pub certification_id: String,
    pub provider_id: String,
    pub instance_id: String,
    pub provider_kind: String,
    pub status: String,
    pub failure_class: Option<String>,
    pub summary: Option<String>,
    pub media_type_results: Value,
    pub segment_type_results: Value,
    pub probe_targets: Value,
    pub response_evidence: Value,
    pub runtime_version: Option<String>,
    pub policy_version: String,
    pub certified_at: Option<String>,
    pub expires_at: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct MediaSegmentCandidateReviewFilters {
    #[serde(alias = "mediaFileId")]
    pub media_file_id: Option<String>,
    #[serde(alias = "itemType")]
    pub item_type: Option<String>,
    #[serde(alias = "itemId")]
    pub item_id: Option<String>,
    #[serde(alias = "segmentType")]
    pub segment_type: Option<String>,
    #[serde(alias = "providerKind")]
    pub provider_kind: Option<String>,
    #[serde(alias = "validationState", alias = "status")]
    pub validation_state: Option<String>,
    #[serde(alias = "validationReason")]
    pub validation_reason: Option<String>,
    #[serde(alias = "lowConfidence")]
    pub low_confidence: Option<bool>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MediaSegmentJobEnqueueRequest {
    #[serde(alias = "jobType")]
    pub job_type: String,
    #[serde(alias = "scopeType")]
    pub scope_type: String,
    #[serde(alias = "scopeId")]
    pub scope_id: String,
    #[serde(alias = "providerKind")]
    pub provider_kind: String,
    pub priority: Option<i64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct MediaSegmentJobActionRequest {
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct MediaSegmentItemAnalyzeRequest {
    pub force: Option<bool>,
    #[serde(alias = "includeBuiltins")]
    pub include_builtins: Option<bool>,
    #[serde(alias = "includeLocalDetectors")]
    pub include_local_detectors: Option<bool>,
    pub priority: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MediaSegmentItemAnalyzeSummary {
    pub item_type: String,
    pub item_id: String,
    pub force: bool,
    pub media_files_seen: usize,
    pub seasons_seen: usize,
    pub jobs: Vec<MediaSegmentJobRecord>,
    pub failures: Vec<MediaSegmentItemAnalyzeFailure>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MediaSegmentItemAnalyzeFailure {
    pub job_type: String,
    pub scope_type: String,
    pub scope_id: String,
    pub provider_kind: String,
    pub reason: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct MediaSegmentProviderEnqueueSummary {
    pub providers_seen: usize,
    pub files_seen: usize,
    pub jobs_queued: usize,
    pub jobs_failed: usize,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct MediaSegmentWorkerIterationSummary {
    pub enqueue: MediaSegmentProviderEnqueueSummary,
    pub stale_jobs_recovered: usize,
    pub stale_jobs_requeued: usize,
    pub stale_jobs_failed: usize,
    pub runtime_budget_seconds: u64,
    pub runtime_elapsed_ms: u64,
    pub runtime_budget_exhausted: bool,
    pub jobs_run: usize,
    pub jobs_succeeded: usize,
    pub jobs_skipped: usize,
    pub jobs_requeued: usize,
    pub jobs_failed: usize,
    pub jobs_cancelled: usize,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct LocalAudioDetectorSummary {
    pub season_id: String,
    pub status: String,
    pub files_seen: usize,
    pub files_with_fingerprints: usize,
    pub repeated_groups: usize,
    pub candidates_submitted: usize,
    pub candidates_accepted: usize,
    pub candidates_rejected: usize,
    pub active_segments: usize,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct LocalAudioFingerprintSummary {
    pub media_file_id: String,
    pub status: String,
    pub duration_seconds: Option<i64>,
    pub file_size_bytes: Option<i64>,
    pub windows_planned: usize,
    pub windows_fingerprinted: usize,
    pub extraction_ranges: usize,
    pub fingerprint_version: String,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct LocalVisualFrameHashSummary {
    pub media_file_id: String,
    pub status: String,
    pub duration_seconds: Option<i64>,
    pub file_size_bytes: Option<i64>,
    pub extraction_ranges: usize,
    pub frames_planned: usize,
    pub frames_extracted: usize,
    pub frame_width: usize,
    pub frame_height: usize,
    pub fingerprint_version: String,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct LocalVisualDetectorSummary {
    pub media_file_id: String,
    pub status: String,
    pub duration_seconds: Option<i64>,
    pub frames_seen: usize,
    pub credits_like_frames: usize,
    pub sustained_runs: usize,
    pub candidates_submitted: usize,
    pub candidates_accepted: usize,
    pub candidates_rejected: usize,
    pub active_segments: usize,
    pub detector_version: String,
    pub reason: Option<String>,
}

#[derive(Clone, Copy)]
struct MediaSegmentJobCancellation<'a> {
    pool: &'a AnyPool,
    job_id: &'a str,
    shutdown: Option<&'a CancellationToken>,
}

#[derive(Debug, Clone)]
struct SegmentItemContext {
    item_type: String,
    item_id: String,
}

#[derive(Debug, Clone)]
struct CandidateRow {
    id: String,
    media_file_id: String,
    item_type: String,
    item_id: String,
    segment_type: String,
    start_seconds: f64,
    end_seconds: f64,
    provider_kind: String,
    provider_id: String,
    provider_version: Option<String>,
    confidence: f64,
    identity_strength: String,
    source_payload: Option<Value>,
}

#[derive(Debug, Clone)]
struct ActiveWindow {
    start_seconds: f64,
    end_seconds: f64,
}

#[derive(Debug, Clone)]
struct ProviderMediaContext {
    media_file_id: String,
    item_type: String,
    item_id: String,
    imdb_id: Option<String>,
    tmdb_id: Option<String>,
    tvdb_id: Option<String>,
    anilist_id: Option<String>,
    mal_id: Option<String>,
    season_number: Option<i32>,
    episode_number: Option<i32>,
    absolute_episode_number: Option<i32>,
    duration_seconds: Option<f64>,
}

#[derive(Debug, Clone)]
struct MarketplaceSegmentProviderSelection {
    provider: Provider,
    extension: Extension,
    instance: ExtensionInstance,
    media_types: Vec<String>,
    segment_types: Vec<String>,
    actions: Vec<String>,
}

#[derive(Debug, Serialize)]
struct MarketplaceSegmentProviderInvocation<'a> {
    schema_version: &'static str,
    request: MarketplaceSegmentProviderLookupRequest<'a>,
    provider: MarketplaceSegmentProviderInvocationContext<'a>,
}

#[derive(Debug, Serialize)]
struct MarketplaceSegmentProviderLookupRequest<'a> {
    media_file_id: &'a str,
    item_type: &'a str,
    item_id: &'a str,
    media_type: &'a str,
    duration_seconds: Option<f64>,
    external_ids: Value,
    season_number: Option<i32>,
    episode_number: Option<i32>,
    absolute_episode_number: Option<i32>,
    requested_segment_types: &'a [String],
}

#[derive(Debug, Serialize)]
struct MarketplaceSegmentProviderInvocationContext<'a> {
    provider_id: Uuid,
    extension_id: &'a str,
    instance_id: Uuid,
    implementation: Option<&'a str>,
    config: Option<Value>,
}

#[derive(Debug)]
struct MediaSegmentProviderCertificationProbe {
    media_type: String,
    status: String,
    failure_class: Option<String>,
    summary: String,
    segment_count: usize,
    segment_type_counts: BTreeMap<String, usize>,
    response_evidence: Value,
}

#[derive(Debug, Clone)]
struct ProviderLookupResult {
    outcome: BuiltinProviderRefreshOutcome,
    response: Option<Value>,
}

pub async fn submit_segment_candidate(
    pool: &AnyPool,
    input: SegmentCandidateInput,
) -> Result<SegmentCandidateOutcome> {
    let candidate = normalize_candidate_input(pool, input).await?;
    let duration_seconds = load_media_duration_seconds(pool, &candidate.media_file_id).await?;
    let validation = validate_candidate(&candidate, duration_seconds);
    let source_payload_json = candidate.source_payload.as_ref().map(Value::to_string);

    sqlx::query::<sqlx::Any>(
        "INSERT INTO media_segment_candidates
            (id, media_file_id, item_type, item_id, segment_type, start_seconds, end_seconds,
             provider_kind, provider_id, provider_version, confidence, validation_state,
             validation_reason, identity_strength, source_payload_json, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
         ON CONFLICT(id) DO UPDATE SET
             media_file_id = excluded.media_file_id,
             item_type = excluded.item_type,
             item_id = excluded.item_id,
             segment_type = excluded.segment_type,
             start_seconds = excluded.start_seconds,
             end_seconds = excluded.end_seconds,
             provider_kind = excluded.provider_kind,
             provider_id = excluded.provider_id,
             provider_version = excluded.provider_version,
             confidence = excluded.confidence,
             validation_state = excluded.validation_state,
             validation_reason = excluded.validation_reason,
             identity_strength = excluded.identity_strength,
             source_payload_json = excluded.source_payload_json,
             updated_at = CURRENT_TIMESTAMP",
    )
    .bind(&candidate.id)
    .bind(&candidate.media_file_id)
    .bind(&candidate.item_type)
    .bind(&candidate.item_id)
    .bind(&candidate.segment_type)
    .bind(candidate.start_seconds)
    .bind(candidate.end_seconds)
    .bind(&candidate.provider_kind)
    .bind(&candidate.provider_id)
    .bind(candidate.provider_version.as_deref())
    .bind(candidate.confidence)
    .bind(validation.state)
    .bind(validation.reason.as_deref())
    .bind(&candidate.identity_strength)
    .bind(source_payload_json.as_deref())
    .execute(pool)
    .await
    .context("upserting media segment candidate")?;

    if validation.auto_activate {
        recalculate_active_segments(pool, &candidate.media_file_id, &candidate.segment_type)
            .await?;
    }

    let stored = load_segment_candidate(pool, &candidate.id)
        .await?
        .context("stored segment candidate was not found")?;
    record_media_segment_candidate(&stored.provider_kind, &stored.validation_state);
    let activated_segment = load_active_segment_for_candidate(pool, &candidate.id).await?;

    Ok(SegmentCandidateOutcome {
        candidate: stored,
        activated_segment,
    })
}

pub async fn ingest_chapter_segments_from_metadata(
    pool: &AnyPool,
    media_file_id: &str,
    metadata: &ffprobe::MediaMetadata,
) -> Result<ChapterIngestionSummary> {
    let mut summary = ChapterIngestionSummary {
        media_file_id: media_file_id.to_string(),
        chapters_seen: metadata.chapters.len(),
        ..ChapterIngestionSummary::default()
    };

    if metadata.chapters.is_empty() {
        return Ok(summary);
    }

    let duration_seconds = metadata.duration_seconds.map(|value| value as f64);
    for chapter in &metadata.chapters {
        let Some(title) = chapter_title(chapter) else {
            continue;
        };
        let Some(segment_type) = chapter_segment_type(&title) else {
            continue;
        };
        let Some(start_seconds) = chapter
            .start_time
            .as_deref()
            .and_then(parse_chapter_seconds)
        else {
            continue;
        };
        let Some(end_seconds) = chapter.end_time.as_deref().and_then(parse_chapter_seconds) else {
            continue;
        };

        let source_payload = json!({
            "source": "ffprobe_chapters",
            "chapter_id": chapter.id,
            "title": title,
            "duration_seconds": duration_seconds,
            "tags": chapter.tags,
        });
        let outcome = submit_segment_candidate(
            pool,
            SegmentCandidateInput {
                media_file_id: media_file_id.to_string(),
                item_type: None,
                item_id: None,
                segment_type: segment_type.to_string(),
                start_seconds,
                end_seconds,
                provider_kind: PROVIDER_CHAPTER.to_string(),
                provider_id: CHAPTER_PROVIDER_ID.to_string(),
                provider_version: Some(CHAPTER_PROVIDER_VERSION.to_string()),
                confidence: 0.85,
                identity_strength: "file_fingerprint".to_string(),
                source_payload: Some(source_payload),
            },
        )
        .await?;

        summary.candidates_submitted += 1;
        if outcome.candidate.validation_state == "accepted" {
            summary.candidates_accepted += 1;
        } else {
            summary.candidates_rejected += 1;
        }
    }

    summary.active_segments = list_active_segments_for_file(pool, media_file_id)
        .await?
        .len();
    Ok(summary)
}

pub async fn refresh_chapter_segments_from_probe(
    pool: &AnyPool,
    media_file_id: &str,
) -> Result<ChapterIngestionSummary> {
    let row = sqlx::query::<sqlx::Any>(
        "SELECT raw_json, normalized_json
         FROM media_file_probes
         WHERE media_file_id = $1 AND probe_status = 'ok'
         LIMIT 1",
    )
    .bind(media_file_id)
    .fetch_optional(pool)
    .await
    .context("loading media file probe for chapter refresh")?
    .context("media file probe is missing")?;

    let raw_json = row
        .try_get::<String, _>("raw_json")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .context("media file probe has no raw ffprobe json")?;
    let raw_value: Value =
        serde_json::from_str(&raw_json).context("decoding raw ffprobe json for chapters")?;
    let parsed: ffprobe::FfprobeStreams =
        serde_json::from_value(raw_value.clone()).context("decoding ffprobe chapter payload")?;
    let normalized_duration = row
        .try_get::<String, _>("normalized_json")
        .ok()
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
        .and_then(|value| value.get("duration_seconds").and_then(Value::as_f64));

    let metadata = ffprobe::MediaMetadata {
        duration_seconds: normalized_duration.map(|value| value.round() as i32),
        chapters: parsed.chapters,
        raw_json: raw_value,
        ..ffprobe::MediaMetadata::default()
    };
    ingest_chapter_segments_from_metadata(pool, media_file_id, &metadata).await
}

pub async fn refresh_builtin_provider_segments(
    pool: &AnyPool,
    media_file_id: &str,
    preferences: &PlaybackInteractionPreferences,
    options: BuiltinProviderRefreshOptions,
) -> Result<BuiltinProviderRefreshSummary> {
    let context = load_provider_media_context(pool, media_file_id).await?;
    let effective_segment_provider_settings = effective_segment_provider_settings_for_media_file(
        pool,
        media_file_id,
        &preferences.segment_provider_settings,
    )
    .await?;
    let client = reqwest::Client::builder()
        .timeout(StdDuration::from_secs(DEFAULT_PROVIDER_TIMEOUT_SECONDS))
        .user_agent("ElixirMediaServer/0.1 MIDM")
        .build()
        .context("building media segment provider HTTP client")?;
    let force_refresh = options.force_refresh.unwrap_or(false);
    let provider_kinds = selected_builtin_provider_kinds(options.provider_kind.as_deref())?;
    let mut summary = BuiltinProviderRefreshSummary {
        media_file_id: media_file_id.to_string(),
        ..BuiltinProviderRefreshSummary::default()
    };

    for provider_kind in provider_kinds {
        let provider_settings =
            provider_settings_for(&effective_segment_provider_settings, provider_kind);
        if !provider_settings_enabled(provider_settings.as_ref()) {
            summary.providers.push(BuiltinProviderRefreshOutcome {
                provider_kind: provider_kind.to_string(),
                enabled: false,
                status: "skipped".to_string(),
                cache_hit: false,
                candidate_count: 0,
                accepted_count: 0,
                rejected_count: 0,
                reason: Some("provider_disabled".to_string()),
            });
            continue;
        }

        let lookup = match provider_kind {
            PROVIDER_THEINTRODB => {
                refresh_theintrodb_provider(
                    pool,
                    &client,
                    &context,
                    provider_settings.as_ref(),
                    force_refresh,
                )
                .await?
            }
            PROVIDER_ANISKIP => {
                refresh_aniskip_provider(
                    pool,
                    &client,
                    &context,
                    provider_settings.as_ref(),
                    force_refresh,
                )
                .await?
            }
            _ => unreachable!(),
        };

        let mut outcome = lookup.outcome;
        if let Some(response) = lookup.response {
            let candidates = provider_response_to_candidates(provider_kind, &context, &response)?;
            outcome.candidate_count = candidates.len();
            for candidate in candidates {
                let result = submit_segment_candidate(pool, candidate).await?;
                summary.candidates_submitted += 1;
                if result.candidate.validation_state == "accepted" {
                    summary.candidates_accepted += 1;
                    outcome.accepted_count += 1;
                } else {
                    summary.candidates_rejected += 1;
                    outcome.rejected_count += 1;
                }
            }
        }
        summary.providers.push(outcome);
    }

    summary.active_segments = list_active_segments_for_file(pool, media_file_id)
        .await?
        .len();
    Ok(summary)
}

async fn refresh_marketplace_segment_provider_segments(
    pool: &AnyPool,
    media_file_id: &str,
    preferences: &PlaybackInteractionPreferences,
    provider_kind: &str,
) -> Result<BuiltinProviderRefreshSummary> {
    let provider_kind = normalize_provider_id(provider_kind)?;
    let context = load_provider_media_context(pool, media_file_id).await?;
    let effective_segment_provider_settings = effective_segment_provider_settings_for_media_file(
        pool,
        media_file_id,
        &preferences.segment_provider_settings,
    )
    .await?;
    let provider_settings =
        provider_settings_for(&effective_segment_provider_settings, &provider_kind);
    let mut summary = BuiltinProviderRefreshSummary {
        media_file_id: media_file_id.to_string(),
        ..BuiltinProviderRefreshSummary::default()
    };

    if !marketplace_provider_settings_enabled(provider_settings.as_ref()) {
        summary.providers.push(BuiltinProviderRefreshOutcome {
            provider_kind,
            enabled: false,
            status: "skipped".to_string(),
            cache_hit: false,
            candidate_count: 0,
            accepted_count: 0,
            rejected_count: 0,
            reason: Some("provider_disabled".to_string()),
        });
        summary.active_segments = list_active_segments_for_file(pool, media_file_id)
            .await?
            .len();
        return Ok(summary);
    }

    let store = ExtensionStore::new(pool);
    let media_type = marketplace_provider_media_type_for_context(&context);
    let selected =
        select_marketplace_segment_provider(&store, &provider_kind, Some(&media_type)).await?;
    let client = reqwest::Client::builder()
        .timeout(StdDuration::from_secs(DEFAULT_PROVIDER_TIMEOUT_SECONDS))
        .user_agent("ElixirMediaServer/0.1 MIDM")
        .build()
        .context("building marketplace media segment provider HTTP client")?;
    let lookup = refresh_marketplace_segment_provider(
        pool,
        &client,
        &context,
        &selected,
        &provider_kind,
        provider_settings.as_ref(),
        false,
    )
    .await?;

    let mut outcome = lookup.outcome;
    if let Some(response) = lookup.response {
        let candidates = marketplace_segment_provider_response_to_candidates(
            &provider_kind,
            &selected,
            &context,
            &response,
        )?;
        outcome.candidate_count = candidates.len();
        for candidate in candidates {
            let result = submit_segment_candidate(pool, candidate).await?;
            summary.candidates_submitted += 1;
            if result.candidate.validation_state == "accepted" {
                summary.candidates_accepted += 1;
                outcome.accepted_count += 1;
            } else {
                summary.candidates_rejected += 1;
                outcome.rejected_count += 1;
            }
        }
    }
    summary.providers.push(outcome);
    summary.active_segments = list_active_segments_for_file(pool, media_file_id)
        .await?
        .len();
    Ok(summary)
}

pub async fn enqueue_builtin_provider_refresh_jobs(
    pool: &AnyPool,
    media_file_id: &str,
    priority: i64,
) -> Result<Vec<MediaSegmentJobRecord>> {
    let mut jobs = Vec::new();
    for provider_kind in [PROVIDER_THEINTRODB, PROVIDER_ANISKIP] {
        jobs.push(
            enqueue_builtin_provider_refresh_job(pool, media_file_id, provider_kind, priority)
                .await?,
        );
    }
    Ok(jobs)
}

pub async fn enqueue_builtin_provider_refresh_job(
    pool: &AnyPool,
    media_file_id: &str,
    provider_kind: &str,
    priority: i64,
) -> Result<MediaSegmentJobRecord> {
    let provider_kind = normalize_single_builtin_provider_kind(provider_kind)?;
    load_provider_media_context(pool, media_file_id).await?;
    enqueue_media_segment_job(
        pool,
        MEDIA_SEGMENT_JOB_PROVIDER_REFRESH,
        MEDIA_SEGMENT_JOB_SCOPE_MEDIA_FILE,
        media_file_id,
        provider_kind,
        priority,
        PROVIDER_JOB_MAX_ATTEMPTS,
    )
    .await
}

pub async fn enqueue_marketplace_segment_provider_refresh_job(
    pool: &AnyPool,
    media_file_id: &str,
    provider_kind: &str,
    priority: i64,
) -> Result<MediaSegmentJobRecord> {
    let provider_kind = normalize_provider_id(provider_kind)?;
    let context = load_provider_media_context(pool, media_file_id).await?;
    let store = ExtensionStore::new(pool);
    let media_type = marketplace_provider_media_type_for_context(&context);
    select_marketplace_segment_provider(&store, &provider_kind, Some(&media_type)).await?;
    enqueue_media_segment_job(
        pool,
        MEDIA_SEGMENT_JOB_PROVIDER_REFRESH,
        MEDIA_SEGMENT_JOB_SCOPE_MEDIA_FILE,
        media_file_id,
        &provider_kind,
        priority,
        PROVIDER_JOB_MAX_ATTEMPTS,
    )
    .await
}

pub async fn enqueue_local_audio_recurring_detector_job(
    pool: &AnyPool,
    season_id: &str,
    priority: i64,
) -> Result<MediaSegmentJobRecord> {
    ensure_season_exists(pool, season_id).await?;
    enqueue_media_segment_job(
        pool,
        MEDIA_SEGMENT_JOB_LOCAL_DETECTOR,
        MEDIA_SEGMENT_JOB_SCOPE_SEASON,
        season_id,
        PROVIDER_LOCAL_AUDIO_RECURRING,
        priority,
        PROVIDER_JOB_MAX_ATTEMPTS,
    )
    .await
}

pub async fn enqueue_local_audio_fingerprint_job(
    pool: &AnyPool,
    media_file_id: &str,
    priority: i64,
) -> Result<MediaSegmentJobRecord> {
    ensure_media_file_exists(pool, media_file_id).await?;
    enqueue_media_segment_job(
        pool,
        MEDIA_SEGMENT_JOB_AUDIO_FINGERPRINT,
        MEDIA_SEGMENT_JOB_SCOPE_MEDIA_FILE,
        media_file_id,
        PROVIDER_LOCAL_AUDIO_RECURRING,
        priority,
        PROVIDER_JOB_MAX_ATTEMPTS,
    )
    .await
}

pub async fn enqueue_local_visual_credits_detector_job(
    pool: &AnyPool,
    media_file_id: &str,
    priority: i64,
) -> Result<MediaSegmentJobRecord> {
    load_provider_media_context(pool, media_file_id).await?;
    enqueue_media_segment_job(
        pool,
        MEDIA_SEGMENT_JOB_LOCAL_DETECTOR,
        MEDIA_SEGMENT_JOB_SCOPE_MEDIA_FILE,
        media_file_id,
        PROVIDER_LOCAL_VISUAL_RECURRING,
        priority,
        PROVIDER_JOB_MAX_ATTEMPTS,
    )
    .await
}

pub async fn enqueue_local_visual_frame_hash_job(
    pool: &AnyPool,
    media_file_id: &str,
    priority: i64,
) -> Result<MediaSegmentJobRecord> {
    ensure_media_file_exists(pool, media_file_id).await?;
    enqueue_media_segment_job(
        pool,
        MEDIA_SEGMENT_JOB_VIDEO_FRAME_HASH,
        MEDIA_SEGMENT_JOB_SCOPE_MEDIA_FILE,
        media_file_id,
        PROVIDER_LOCAL_VISUAL_RECURRING,
        priority,
        PROVIDER_JOB_MAX_ATTEMPTS,
    )
    .await
}

pub async fn enqueue_media_segment_job_request(
    pool: &AnyPool,
    request: MediaSegmentJobEnqueueRequest,
) -> Result<MediaSegmentJobRecord> {
    enqueue_media_segment_job_request_inner(pool, request, false).await
}

pub async fn enqueue_media_segment_job_request_with_marketplace(
    pool: &AnyPool,
    request: MediaSegmentJobEnqueueRequest,
) -> Result<MediaSegmentJobRecord> {
    enqueue_media_segment_job_request_inner(pool, request, true).await
}

struct NormalizedMediaSegmentJobEnqueueRequest {
    job_type: String,
    scope_type: String,
    scope_id: String,
    provider_kind: String,
    priority: i64,
}

fn normalize_media_segment_job_enqueue_request(
    request: MediaSegmentJobEnqueueRequest,
) -> Result<NormalizedMediaSegmentJobEnqueueRequest> {
    let job_type = normalize_required_text(&request.job_type, "job_type")?
        .to_ascii_lowercase()
        .replace('-', "_");
    let scope_type = normalize_required_text(&request.scope_type, "scope_type")?
        .to_ascii_lowercase()
        .replace('-', "_");
    let scope_id = normalize_required_text(&request.scope_id, "scope_id")?;
    let provider_kind = normalize_required_text(&request.provider_kind, "provider_kind")?
        .to_ascii_lowercase()
        .replace('-', "_");
    let priority = request.priority.unwrap_or(100).clamp(0, 10_000);

    Ok(NormalizedMediaSegmentJobEnqueueRequest {
        job_type,
        scope_type,
        scope_id,
        provider_kind,
        priority,
    })
}

async fn enqueue_media_segment_job_request_inner(
    pool: &AnyPool,
    request: MediaSegmentJobEnqueueRequest,
    allow_marketplace_providers: bool,
) -> Result<MediaSegmentJobRecord> {
    let request = normalize_media_segment_job_enqueue_request(request)?;

    match (
        request.job_type.as_str(),
        request.scope_type.as_str(),
        request.provider_kind.as_str(),
    ) {
        (
            MEDIA_SEGMENT_JOB_PROVIDER_REFRESH,
            MEDIA_SEGMENT_JOB_SCOPE_MEDIA_FILE,
            PROVIDER_THEINTRODB | PROVIDER_ANISKIP,
        ) => {
            enqueue_builtin_provider_refresh_job(
                pool,
                &request.scope_id,
                &request.provider_kind,
                request.priority,
            )
            .await
        }
        (MEDIA_SEGMENT_JOB_PROVIDER_REFRESH, MEDIA_SEGMENT_JOB_SCOPE_MEDIA_FILE, _)
            if allow_marketplace_providers
                && !is_reserved_media_segment_provider_kind(&request.provider_kind) =>
        {
            enqueue_marketplace_segment_provider_refresh_job(
                pool,
                &request.scope_id,
                &request.provider_kind,
                request.priority,
            )
            .await
        }
        (
            MEDIA_SEGMENT_JOB_AUDIO_FINGERPRINT,
            MEDIA_SEGMENT_JOB_SCOPE_MEDIA_FILE,
            PROVIDER_LOCAL_AUDIO_RECURRING,
        ) => enqueue_local_audio_fingerprint_job(pool, &request.scope_id, request.priority).await,
        (
            MEDIA_SEGMENT_JOB_VIDEO_FRAME_HASH,
            MEDIA_SEGMENT_JOB_SCOPE_MEDIA_FILE,
            PROVIDER_LOCAL_VISUAL_RECURRING,
        ) => enqueue_local_visual_frame_hash_job(pool, &request.scope_id, request.priority).await,
        (
            MEDIA_SEGMENT_JOB_LOCAL_DETECTOR,
            MEDIA_SEGMENT_JOB_SCOPE_SEASON,
            PROVIDER_LOCAL_AUDIO_RECURRING,
        ) => {
            enqueue_local_audio_recurring_detector_job(pool, &request.scope_id, request.priority)
                .await
        }
        (
            MEDIA_SEGMENT_JOB_LOCAL_DETECTOR,
            MEDIA_SEGMENT_JOB_SCOPE_MEDIA_FILE,
            PROVIDER_LOCAL_VISUAL_RECURRING,
        ) => {
            enqueue_local_visual_credits_detector_job(pool, &request.scope_id, request.priority)
                .await
        }
        _ => bail!("unsupported media segment job request"),
    }
}

fn is_reserved_media_segment_provider_kind(provider_kind: &str) -> bool {
    matches!(
        provider_kind,
        PROVIDER_THEINTRODB
            | PROVIDER_ANISKIP
            | PROVIDER_LOCAL_AUDIO_RECURRING
            | PROVIDER_LOCAL_VISUAL_RECURRING
            | PROVIDER_CHAPTER
    )
}

pub async fn list_media_segment_jobs(
    pool: &AnyPool,
    filters: MediaSegmentJobListFilters,
) -> Result<Vec<MediaSegmentJobRecord>> {
    let status = filters
        .status
        .as_deref()
        .map(|value| normalize_media_segment_job_status_filter(value))
        .transpose()?;
    let provider_kind = filters
        .provider_kind
        .as_deref()
        .map(|value| normalize_media_segment_job_identifier_filter(value, "provider_kind"))
        .transpose()?;
    let job_type = filters
        .job_type
        .as_deref()
        .map(|value| normalize_media_segment_job_identifier_filter(value, "job_type"))
        .transpose()?;
    let scope_type = filters
        .scope_type
        .as_deref()
        .map(|value| normalize_media_segment_job_identifier_filter(value, "scope_type"))
        .transpose()?;
    let scope_id = filters
        .scope_id
        .as_deref()
        .map(|value| normalize_required_text(value, "scope_id"))
        .transpose()?;
    let limit = filters.limit.unwrap_or(100).clamp(1, 500);

    let rows = sqlx::query::<sqlx::Any>(
        "SELECT id, job_type, scope_type, scope_id, provider_kind, status, priority, attempts,
                max_attempts,
                CAST(next_attempt_at AS TEXT) AS next_attempt_at,
                locked_by,
                CAST(started_at AS TEXT) AS started_at,
                CAST(finished_at AS TEXT) AS finished_at,
                error_json
         FROM media_segment_jobs
         WHERE ($1 IS NULL OR status = $2)
           AND ($3 IS NULL OR provider_kind = $4)
           AND ($5 IS NULL OR job_type = $6)
           AND ($7 IS NULL OR scope_type = $8)
           AND ($9 IS NULL OR scope_id = $10)
         ORDER BY
           CASE status
             WHEN 'running' THEN 0
             WHEN 'queued' THEN 1
             WHEN 'failed' THEN 2
             WHEN 'skipped' THEN 3
             ELSE 4
           END,
           priority ASC,
           created_at DESC
         LIMIT $11",
    )
    .bind(status.as_deref())
    .bind(status.as_deref())
    .bind(provider_kind.as_deref())
    .bind(provider_kind.as_deref())
    .bind(job_type.as_deref())
    .bind(job_type.as_deref())
    .bind(scope_type.as_deref())
    .bind(scope_type.as_deref())
    .bind(scope_id.as_deref())
    .bind(scope_id.as_deref())
    .bind(limit)
    .fetch_all(pool)
    .await
    .context("listing media segment jobs")?;

    Ok(rows.iter().map(media_segment_job_from_row).collect())
}

pub async fn list_media_segment_provider_certifications(
    pool: &AnyPool,
    filters: MediaSegmentProviderCertificationFilters,
) -> Result<Vec<MediaSegmentProviderCertificationRecord>> {
    let provider_id = filters
        .provider_id
        .as_deref()
        .map(|value| normalize_uuid_text(value, "provider_id"))
        .transpose()?;
    let provider_kind = filters
        .provider_kind
        .as_deref()
        .map(normalize_provider_id)
        .transpose()?;
    let status = filters
        .status
        .as_deref()
        .map(normalize_media_segment_provider_certification_status)
        .transpose()?;
    let limit = filters.limit.unwrap_or(100).clamp(1, 500);

    let rows = sqlx::query::<sqlx::Any>(
        "SELECT certification_id, provider_id, instance_id, provider_kind, status,
                failure_class, summary,
                CAST(media_type_results_json AS TEXT) AS media_type_results_json,
                CAST(segment_type_results_json AS TEXT) AS segment_type_results_json,
                CAST(probe_targets_json AS TEXT) AS probe_targets_json,
                CAST(response_evidence_json AS TEXT) AS response_evidence_json,
                CAST(runtime_version AS TEXT) AS runtime_version,
                policy_version,
                CAST(certified_at AS TEXT) AS certified_at,
                CAST(expires_at AS TEXT) AS expires_at,
                CAST(created_at AS TEXT) AS created_at,
                CAST(updated_at AS TEXT) AS updated_at
         FROM media_segment_provider_certifications
         WHERE ($1 IS NULL OR provider_id = $2)
           AND ($3 IS NULL OR provider_kind = $4)
           AND ($5 IS NULL OR status = $6)
         ORDER BY updated_at DESC
         LIMIT $7",
    )
    .bind(provider_id.as_deref())
    .bind(provider_id.as_deref())
    .bind(provider_kind.as_deref())
    .bind(provider_kind.as_deref())
    .bind(status.as_deref())
    .bind(status.as_deref())
    .bind(limit)
    .fetch_all(pool)
    .await
    .context("listing media segment provider certifications")?;

    rows.iter()
        .map(media_segment_provider_certification_from_row)
        .collect()
}

pub async fn certify_media_segment_provider(
    pool: &AnyPool,
    provider_id: &str,
) -> Result<MediaSegmentProviderCertificationRecord> {
    let provider_id = Uuid::parse_str(&normalize_uuid_text(provider_id, "provider_id")?)
        .context("parsing provider_id")?;
    let store = ExtensionStore::new(pool);
    let selected = select_marketplace_segment_provider_by_id(&store, provider_id).await?;
    let provider_kind = selected
        .provider
        .implementation
        .as_deref()
        .map(normalize_provider_id)
        .transpose()?
        .context("media segment provider implementation is missing")?;
    let probe_media_types = marketplace_segment_provider_certification_media_types(&selected);
    let client = reqwest::Client::builder()
        .timeout(StdDuration::from_secs(DEFAULT_PROVIDER_TIMEOUT_SECONDS))
        .user_agent("ElixirMediaServer/0.1 MIDM certification")
        .build()
        .context("building media segment provider certification HTTP client")?;

    let mut probes = Vec::new();
    if probe_media_types.is_empty() {
        probes.push(MediaSegmentProviderCertificationProbe {
            media_type: "unknown".to_string(),
            status: "broken".to_string(),
            failure_class: Some("unsupported_scope".to_string()),
            summary: "provider scope does not include a certifiable media type".to_string(),
            segment_count: 0,
            segment_type_counts: BTreeMap::new(),
            response_evidence: json!({"error": "unsupported_scope"}),
        });
    } else {
        for media_type in probe_media_types {
            let context = media_segment_provider_certification_context(&media_type);
            let probe =
                run_media_segment_provider_certification_probe(&client, &selected, &context).await;
            probes.push(probe);
        }
    }

    let status = if probes.iter().all(|probe| probe.status == "certified") {
        "certified"
    } else {
        "broken"
    };
    let failure_class = probes
        .iter()
        .find_map(|probe| probe.failure_class.as_deref())
        .map(ToOwned::to_owned);
    let summary = media_segment_provider_certification_summary(status, &probes);
    let media_type_results = media_segment_provider_certification_media_type_results(&probes);
    let segment_type_results = media_segment_provider_certification_segment_type_results(&probes);
    let probe_targets = media_segment_provider_certification_probe_targets(&probes);
    let response_evidence = Value::Array(
        probes
            .iter()
            .map(|probe| probe.response_evidence.clone())
            .collect(),
    );
    let certified_at = (status == "certified").then(timestamp_now);
    let expires_at = Some(timestamp_after_seconds(
        MEDIA_SEGMENT_PROVIDER_CERTIFICATION_EXPIRES_DAYS * 24 * 60 * 60,
    ));

    upsert_media_segment_provider_certification(
        pool,
        Uuid::new_v4(),
        selected.provider.provider_id,
        selected.instance.instance_id,
        &provider_kind,
        status,
        failure_class.as_deref(),
        Some(&summary),
        &media_type_results,
        &segment_type_results,
        &probe_targets,
        &response_evidence,
        selected
            .instance
            .runtime_version
            .as_deref()
            .or(Some(selected.extension.version.as_str())),
        MEDIA_SEGMENT_PROVIDER_CERTIFICATION_POLICY_VERSION,
        certified_at.as_deref(),
        expires_at.as_deref(),
    )
    .await?;

    latest_media_segment_provider_certification(
        pool,
        selected.provider.provider_id,
        MEDIA_SEGMENT_PROVIDER_CERTIFICATION_POLICY_VERSION,
    )
    .await?
    .context("stored media segment provider certification was not found")
}

pub async fn cancel_media_segment_job(
    pool: &AnyPool,
    job_id: &str,
    reason: Option<&str>,
) -> Result<MediaSegmentJobRecord> {
    let job_id = normalize_uuid_text(job_id, "job_id")?;
    let current = load_media_segment_job(pool, &job_id)
        .await?
        .context("media segment job not found")?;
    if current.status == "cancelled" {
        return Ok(current);
    }
    if matches!(current.status.as_str(), "succeeded") {
        bail!("succeeded media segment job cannot be cancelled");
    }

    let payload = json!({
        "reason": "admin_cancelled",
        "message": normalize_optional_reason(reason).unwrap_or_else(|| "cancelled by admin".to_string()),
        "previous_status": current.status,
        "cancelled_at": timestamp_now(),
    });
    finish_media_segment_job(pool, &job_id, "cancelled", Some(payload))
        .await?
        .context("cancelled media segment job was not found")
}

pub async fn retry_media_segment_job(
    pool: &AnyPool,
    job_id: &str,
    reason: Option<&str>,
) -> Result<MediaSegmentJobRecord> {
    let job_id = normalize_uuid_text(job_id, "job_id")?;
    let current = load_media_segment_job(pool, &job_id)
        .await?
        .context("media segment job not found")?;
    if current.status == "running" {
        bail!("running media segment job cannot be retried");
    }

    let payload = json!({
        "reason": "admin_retry",
        "message": normalize_optional_reason(reason).unwrap_or_else(|| "retry requested by admin".to_string()),
        "previous_status": current.status,
        "previous_error": current.error,
        "retried_at": timestamp_now(),
    });
    sqlx::query::<sqlx::Any>(
        "UPDATE media_segment_jobs
         SET status = 'queued',
             attempts = 0,
             next_attempt_at = CURRENT_TIMESTAMP,
             locked_by = NULL,
             started_at = NULL,
             finished_at = NULL,
             error_json = $1,
             updated_at = CURRENT_TIMESTAMP
         WHERE id = $2",
    )
    .bind(payload.to_string())
    .bind(&job_id)
    .execute(pool)
    .await
    .context("retrying media segment job")?;

    let job = load_media_segment_job(pool, &job_id)
        .await?
        .context("retried media segment job was not found")?;
    record_media_segment_job_status(&job);
    refresh_media_segment_job_backlog_metrics(pool).await;
    Ok(job)
}

pub async fn enqueue_media_segment_item_analysis(
    pool: &AnyPool,
    item_type: &str,
    item_id: &str,
    preferences: &PlaybackInteractionPreferences,
    request: MediaSegmentItemAnalyzeRequest,
) -> Result<MediaSegmentItemAnalyzeSummary> {
    let targets = load_media_segment_item_analysis_targets(pool, item_type, item_id).await?;
    let force = request.force.unwrap_or(false);
    let include_builtins = request.include_builtins.unwrap_or(true);
    let include_local_detectors = request.include_local_detectors.unwrap_or(true);
    let priority = request.priority.unwrap_or(80).clamp(0, 10_000);
    let mut summary = MediaSegmentItemAnalyzeSummary {
        item_type: targets.item_type.clone(),
        item_id: targets.item_id.clone(),
        force,
        media_files_seen: targets.media_file_ids.len(),
        seasons_seen: targets.season_ids.len(),
        jobs: Vec::new(),
        failures: Vec::new(),
    };

    if include_builtins {
        enqueue_item_builtin_analysis_jobs(
            pool,
            &targets,
            preferences,
            force,
            priority,
            &mut summary,
        )
        .await;
    }

    if include_local_detectors {
        enqueue_item_local_analysis_jobs(pool, &targets, preferences, priority, &mut summary).await;
    }

    Ok(summary)
}

#[derive(Debug, Clone)]
struct MediaSegmentItemAnalysisTargets {
    item_type: String,
    item_id: String,
    media_file_ids: Vec<String>,
    season_ids: Vec<String>,
}

async fn load_media_segment_item_analysis_targets(
    pool: &AnyPool,
    item_type: &str,
    item_id: &str,
) -> Result<MediaSegmentItemAnalysisTargets> {
    let requested_type = normalize_analysis_item_type(item_type)?;
    let item_id = normalize_required_text(item_id, "item_id")?;

    if requested_type == "movie" {
        let exists = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM movies WHERE id = $1")
            .bind(&item_id)
            .fetch_one(pool)
            .await
            .context("checking movie existence")?;
        if exists == 0 {
            bail!("item not found");
        }
        let rows = sqlx::query::<sqlx::Any>(
            "SELECT DISTINCT mf.id AS media_file_id
             FROM media_files mf
             JOIN movie_files mfv ON mfv.media_file_id = mf.id
             WHERE mfv.movie_id = $1
               AND mf.scan_state = 'ok'
             ORDER BY mf.id",
        )
        .bind(&item_id)
        .fetch_all(pool)
        .await
        .context("listing movie media segment analysis files")?;
        return Ok(MediaSegmentItemAnalysisTargets {
            item_type: "movie".to_string(),
            item_id,
            media_file_ids: rows.iter().map(|row| row.get("media_file_id")).collect(),
            season_ids: Vec::new(),
        });
    }

    let row = sqlx::query::<sqlx::Any>(
        "SELECT library_type
         FROM series
         WHERE id = $1
         LIMIT 1",
    )
    .bind(&item_id)
    .fetch_optional(pool)
    .await
    .context("loading series for media segment analysis")?
    .context("item not found")?;
    let actual_type = row
        .try_get::<String, _>("library_type")
        .unwrap_or_else(|_| "series".to_string());
    let normalized_actual_type = if actual_type == "anime" {
        "anime".to_string()
    } else {
        "series".to_string()
    };
    if requested_type == "anime" && normalized_actual_type != "anime" {
        bail!("item is not anime");
    }

    let file_rows = sqlx::query::<sqlx::Any>(
        "SELECT MIN(mf.id) AS media_file_id,
                e.season_number,
                COALESCE(e.absolute_episode_number, e.episode_number) AS sort_episode_number,
                e.episode_number
         FROM episodes e
         JOIN episode_files ef ON ef.episode_id = e.id
         JOIN media_files mf ON mf.id = ef.media_file_id
         WHERE e.series_id = $1
           AND mf.scan_state = 'ok'
         GROUP BY e.id, e.season_number, e.absolute_episode_number, e.episode_number
         ORDER BY e.season_number ASC,
                  sort_episode_number ASC,
                  e.episode_number ASC,
                  media_file_id ASC",
    )
    .bind(&item_id)
    .fetch_all(pool)
    .await
    .context("listing series media segment analysis files")?;
    let mut media_file_ids = Vec::new();
    for row in file_rows {
        let media_file_id: String = row.get("media_file_id");
        if !media_file_ids.contains(&media_file_id) {
            media_file_ids.push(media_file_id);
        }
    }

    let season_rows = sqlx::query::<sqlx::Any>(
        "SELECT e.season_id,
                MIN(COALESCE(e.season_number, 0)) AS season_sort
         FROM episodes e
         JOIN episode_files ef ON ef.episode_id = e.id
         JOIN media_files mf ON mf.id = ef.media_file_id
         WHERE e.series_id = $1
           AND e.season_id IS NOT NULL
           AND mf.scan_state = 'ok'
         GROUP BY e.season_id
         ORDER BY season_sort ASC, e.season_id ASC",
    )
    .bind(&item_id)
    .fetch_all(pool)
    .await
    .context("listing series media segment analysis seasons")?;
    let season_ids = season_rows
        .iter()
        .filter_map(|row| row.try_get::<String, _>("season_id").ok())
        .collect();

    Ok(MediaSegmentItemAnalysisTargets {
        item_type: normalized_actual_type,
        item_id,
        media_file_ids,
        season_ids,
    })
}

fn normalize_analysis_item_type(item_type: &str) -> Result<String> {
    let normalized = normalize_required_text(item_type, "item_type")?
        .to_ascii_lowercase()
        .replace('-', "_");
    match normalized.as_str() {
        "movie" | "movies" => Ok("movie".to_string()),
        "series" | "show" | "tv" | "anime" => Ok(normalized),
        _ => bail!("item_type must be movie, series, or anime"),
    }
}

async fn enqueue_item_builtin_analysis_jobs(
    pool: &AnyPool,
    targets: &MediaSegmentItemAnalysisTargets,
    preferences: &PlaybackInteractionPreferences,
    force: bool,
    priority: i64,
    summary: &mut MediaSegmentItemAnalyzeSummary,
) {
    for media_file_id in &targets.media_file_ids {
        let provider_settings = match effective_segment_provider_settings_for_media_file(
            pool,
            media_file_id,
            &preferences.segment_provider_settings,
        )
        .await
        {
            Ok(settings) => settings,
            Err(err) => {
                push_item_analysis_failure(
                    summary,
                    MEDIA_SEGMENT_JOB_PROVIDER_REFRESH,
                    MEDIA_SEGMENT_JOB_SCOPE_MEDIA_FILE,
                    media_file_id,
                    "all",
                    err,
                );
                continue;
            }
        };
        let theintrodb_enabled = provider_settings_enabled(
            provider_settings_for(&provider_settings, PROVIDER_THEINTRODB).as_ref(),
        );
        let aniskip_enabled = targets.item_type == "anime"
            && provider_settings_enabled(
                provider_settings_for(&provider_settings, PROVIDER_ANISKIP).as_ref(),
            );

        if theintrodb_enabled {
            if force
                && let Err(err) =
                    clear_media_segment_provider_cache(pool, media_file_id, PROVIDER_THEINTRODB)
                        .await
            {
                push_item_analysis_failure(
                    summary,
                    MEDIA_SEGMENT_JOB_PROVIDER_REFRESH,
                    MEDIA_SEGMENT_JOB_SCOPE_MEDIA_FILE,
                    media_file_id,
                    PROVIDER_THEINTRODB,
                    err,
                );
            }
            match enqueue_builtin_provider_refresh_job(
                pool,
                media_file_id,
                PROVIDER_THEINTRODB,
                priority,
            )
            .await
            {
                Ok(job) => summary.jobs.push(job),
                Err(err) => push_item_analysis_failure(
                    summary,
                    MEDIA_SEGMENT_JOB_PROVIDER_REFRESH,
                    MEDIA_SEGMENT_JOB_SCOPE_MEDIA_FILE,
                    media_file_id,
                    PROVIDER_THEINTRODB,
                    err,
                ),
            }
        }

        if aniskip_enabled {
            if force
                && let Err(err) =
                    clear_media_segment_provider_cache(pool, media_file_id, PROVIDER_ANISKIP).await
            {
                push_item_analysis_failure(
                    summary,
                    MEDIA_SEGMENT_JOB_PROVIDER_REFRESH,
                    MEDIA_SEGMENT_JOB_SCOPE_MEDIA_FILE,
                    media_file_id,
                    PROVIDER_ANISKIP,
                    err,
                );
            }
            match enqueue_builtin_provider_refresh_job(
                pool,
                media_file_id,
                PROVIDER_ANISKIP,
                priority,
            )
            .await
            {
                Ok(job) => summary.jobs.push(job),
                Err(err) => push_item_analysis_failure(
                    summary,
                    MEDIA_SEGMENT_JOB_PROVIDER_REFRESH,
                    MEDIA_SEGMENT_JOB_SCOPE_MEDIA_FILE,
                    media_file_id,
                    PROVIDER_ANISKIP,
                    err,
                ),
            }
        }
    }
}

async fn enqueue_item_local_analysis_jobs(
    pool: &AnyPool,
    targets: &MediaSegmentItemAnalysisTargets,
    preferences: &PlaybackInteractionPreferences,
    priority: i64,
    summary: &mut MediaSegmentItemAnalyzeSummary,
) {
    for media_file_id in &targets.media_file_ids {
        let provider_settings = match effective_segment_provider_settings_for_media_file(
            pool,
            media_file_id,
            &preferences.segment_provider_settings,
        )
        .await
        {
            Ok(settings) => settings,
            Err(err) => {
                push_item_analysis_failure(
                    summary,
                    MEDIA_SEGMENT_JOB_LOCAL_DETECTOR,
                    MEDIA_SEGMENT_JOB_SCOPE_MEDIA_FILE,
                    media_file_id,
                    "all",
                    err,
                );
                continue;
            }
        };
        let local_audio_enabled = targets.item_type != "movie"
            && provider_settings_enabled(
                provider_settings_for(&provider_settings, PROVIDER_LOCAL_AUDIO_RECURRING).as_ref(),
            );
        let local_visual_enabled = provider_settings_enabled(
            provider_settings_for(&provider_settings, PROVIDER_LOCAL_VISUAL_RECURRING).as_ref(),
        );

        if local_audio_enabled {
            match enqueue_local_audio_fingerprint_job(pool, media_file_id, priority + 20).await {
                Ok(job) => summary.jobs.push(job),
                Err(err) => push_item_analysis_failure(
                    summary,
                    MEDIA_SEGMENT_JOB_AUDIO_FINGERPRINT,
                    MEDIA_SEGMENT_JOB_SCOPE_MEDIA_FILE,
                    media_file_id,
                    PROVIDER_LOCAL_AUDIO_RECURRING,
                    err,
                ),
            }
        }
        if local_visual_enabled {
            match enqueue_local_visual_frame_hash_job(pool, media_file_id, priority + 40).await {
                Ok(job) => summary.jobs.push(job),
                Err(err) => push_item_analysis_failure(
                    summary,
                    MEDIA_SEGMENT_JOB_VIDEO_FRAME_HASH,
                    MEDIA_SEGMENT_JOB_SCOPE_MEDIA_FILE,
                    media_file_id,
                    PROVIDER_LOCAL_VISUAL_RECURRING,
                    err,
                ),
            }
            match enqueue_local_visual_credits_detector_job(pool, media_file_id, priority + 50)
                .await
            {
                Ok(job) => summary.jobs.push(job),
                Err(err) => push_item_analysis_failure(
                    summary,
                    MEDIA_SEGMENT_JOB_LOCAL_DETECTOR,
                    MEDIA_SEGMENT_JOB_SCOPE_MEDIA_FILE,
                    media_file_id,
                    PROVIDER_LOCAL_VISUAL_RECURRING,
                    err,
                ),
            }
        }
    }

    if targets.item_type != "movie" {
        for season_id in &targets.season_ids {
            let local_audio_enabled = match season_has_provider_enabled_file(
                pool,
                season_id,
                &preferences.segment_provider_settings,
                PROVIDER_LOCAL_AUDIO_RECURRING,
            )
            .await
            {
                Ok(enabled) => enabled,
                Err(err) => {
                    push_item_analysis_failure(
                        summary,
                        MEDIA_SEGMENT_JOB_LOCAL_DETECTOR,
                        MEDIA_SEGMENT_JOB_SCOPE_SEASON,
                        season_id,
                        PROVIDER_LOCAL_AUDIO_RECURRING,
                        err,
                    );
                    continue;
                }
            };
            if !local_audio_enabled {
                continue;
            }
            match enqueue_local_audio_recurring_detector_job(pool, season_id, priority + 30).await {
                Ok(job) => summary.jobs.push(job),
                Err(err) => push_item_analysis_failure(
                    summary,
                    MEDIA_SEGMENT_JOB_LOCAL_DETECTOR,
                    MEDIA_SEGMENT_JOB_SCOPE_SEASON,
                    season_id,
                    PROVIDER_LOCAL_AUDIO_RECURRING,
                    err,
                ),
            }
        }
    }
}

async fn clear_media_segment_provider_cache(
    pool: &AnyPool,
    media_file_id: &str,
    provider_kind: &str,
) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "DELETE FROM media_segment_provider_cache
         WHERE media_file_id = $1
           AND provider_kind = $2",
    )
    .bind(media_file_id)
    .bind(provider_kind)
    .execute(pool)
    .await
    .context("clearing media segment provider cache")?;
    Ok(())
}

fn push_item_analysis_failure(
    summary: &mut MediaSegmentItemAnalyzeSummary,
    job_type: &str,
    scope_type: &str,
    scope_id: &str,
    provider_kind: &str,
    err: anyhow::Error,
) {
    summary.failures.push(MediaSegmentItemAnalyzeFailure {
        job_type: job_type.to_string(),
        scope_type: scope_type.to_string(),
        scope_id: scope_id.to_string(),
        provider_kind: provider_kind.to_string(),
        reason: truncate_for_error(&err.to_string(), 500),
    });
}

pub async fn claim_next_media_segment_job(
    pool: &AnyPool,
    worker_id: &str,
) -> Result<Option<MediaSegmentJobRecord>> {
    let worker_id = normalize_worker_id(worker_id)?;

    for _ in 0..3 {
        let Some(row) = sqlx::query::<sqlx::Any>(
            "SELECT id
             FROM media_segment_jobs
             WHERE status = 'queued'
               AND attempts < max_attempts
               AND (next_attempt_at IS NULL OR next_attempt_at <= CURRENT_TIMESTAMP)
             ORDER BY priority ASC, created_at ASC
             LIMIT 1",
        )
        .fetch_optional(pool)
        .await
        .context("selecting next media segment job")?
        else {
            return Ok(None);
        };

        let job_id: String = row.get("id");
        let updated = sqlx::query::<sqlx::Any>(
            "UPDATE media_segment_jobs
             SET status = 'running',
                 attempts = attempts + 1,
                 locked_by = $1,
                 started_at = CURRENT_TIMESTAMP,
                 finished_at = NULL,
                 error_json = NULL,
                 updated_at = CURRENT_TIMESTAMP
             WHERE id = $2
               AND status = 'queued'
               AND attempts < max_attempts
               AND (next_attempt_at IS NULL OR next_attempt_at <= CURRENT_TIMESTAMP)",
        )
        .bind(&worker_id)
        .bind(&job_id)
        .execute(pool)
        .await
        .context("claiming media segment job")?;

        if updated.rows_affected() == 1 {
            let job = load_media_segment_job(pool, &job_id).await?;
            if let Some(job) = job.as_ref() {
                record_media_segment_job_status(job);
            }
            refresh_media_segment_job_backlog_metrics(pool).await;
            return Ok(job);
        }
    }

    Ok(None)
}

pub async fn run_next_media_segment_job(
    pool: &AnyPool,
    preferences: &PlaybackInteractionPreferences,
    worker_id: &str,
) -> Result<Option<MediaSegmentJobRunRecord>> {
    run_next_media_segment_job_with_shutdown(pool, preferences, worker_id, None).await
}

async fn run_next_media_segment_job_with_shutdown(
    pool: &AnyPool,
    preferences: &PlaybackInteractionPreferences,
    worker_id: &str,
    shutdown: Option<&CancellationToken>,
) -> Result<Option<MediaSegmentJobRunRecord>> {
    let Some(job) = claim_next_media_segment_job(pool, worker_id).await? else {
        return Ok(None);
    };

    if job.job_type == MEDIA_SEGMENT_JOB_AUDIO_FINGERPRINT
        && job.scope_type == MEDIA_SEGMENT_JOB_SCOPE_MEDIA_FILE
        && job.provider_kind == PROVIDER_LOCAL_AUDIO_RECURRING
    {
        match run_local_audio_fingerprint_for_media_file_with_job(
            pool,
            &job.scope_id,
            preferences,
            Some(&job.id),
            shutdown,
        )
        .await
        {
            Ok(summary) => {
                let terminal_status = if summary.status == "skipped" {
                    "skipped"
                } else {
                    "succeeded"
                };
                let error = if summary.status == "skipped" {
                    summary
                        .reason
                        .as_ref()
                        .map(|reason| json!({ "reason": reason }))
                } else {
                    None
                };
                let finished =
                    finish_media_segment_job(pool, &job.id, terminal_status, error).await?;
                return Ok(Some(MediaSegmentJobRunRecord {
                    job: finished.context("finished media segment job was not found")?,
                    summary: None,
                    local_audio_fingerprint_summary: Some(summary),
                    local_visual_frame_hash_summary: None,
                    local_audio_summary: None,
                    local_visual_summary: None,
                }));
            }
            Err(err) => {
                let error = json!({
                    "reason": "local_audio_fingerprint_failed",
                    "error": err.to_string(),
                    "provider_kind": job.provider_kind,
                });
                let retried = retry_or_fail_media_segment_job(pool, &job.id, error).await?;
                return Ok(Some(MediaSegmentJobRunRecord {
                    job: retried.context("retried media segment job was not found")?,
                    summary: None,
                    local_audio_fingerprint_summary: None,
                    local_visual_frame_hash_summary: None,
                    local_audio_summary: None,
                    local_visual_summary: None,
                }));
            }
        }
    }

    if job.job_type == MEDIA_SEGMENT_JOB_LOCAL_DETECTOR
        && job.scope_type == MEDIA_SEGMENT_JOB_SCOPE_SEASON
        && job.provider_kind == PROVIDER_LOCAL_AUDIO_RECURRING
    {
        match run_local_audio_recurring_detector_for_season(pool, &job.scope_id, preferences).await
        {
            Ok(summary) => {
                let terminal_status = if summary.status == "skipped" {
                    "skipped"
                } else {
                    "succeeded"
                };
                let error = if summary.status == "skipped" {
                    summary
                        .reason
                        .as_ref()
                        .map(|reason| json!({ "reason": reason }))
                } else {
                    None
                };
                let finished =
                    finish_media_segment_job(pool, &job.id, terminal_status, error).await?;
                return Ok(Some(MediaSegmentJobRunRecord {
                    job: finished.context("finished media segment job was not found")?,
                    summary: None,
                    local_audio_fingerprint_summary: None,
                    local_visual_frame_hash_summary: None,
                    local_audio_summary: Some(summary),
                    local_visual_summary: None,
                }));
            }
            Err(err) => {
                let error = json!({
                    "reason": "local_audio_detector_failed",
                    "error": err.to_string(),
                    "provider_kind": job.provider_kind,
                });
                let retried = retry_or_fail_media_segment_job(pool, &job.id, error).await?;
                return Ok(Some(MediaSegmentJobRunRecord {
                    job: retried.context("retried media segment job was not found")?,
                    summary: None,
                    local_audio_fingerprint_summary: None,
                    local_visual_frame_hash_summary: None,
                    local_audio_summary: None,
                    local_visual_summary: None,
                }));
            }
        }
    }

    if job.job_type == MEDIA_SEGMENT_JOB_VIDEO_FRAME_HASH
        && job.scope_type == MEDIA_SEGMENT_JOB_SCOPE_MEDIA_FILE
        && job.provider_kind == PROVIDER_LOCAL_VISUAL_RECURRING
    {
        match run_local_visual_frame_hash_for_media_file_with_job(
            pool,
            &job.scope_id,
            preferences,
            Some(&job.id),
            shutdown,
        )
        .await
        {
            Ok(summary) => {
                let terminal_status = if summary.status == "skipped" {
                    "skipped"
                } else {
                    "succeeded"
                };
                let error = if summary.status == "skipped" {
                    summary
                        .reason
                        .as_ref()
                        .map(|reason| json!({ "reason": reason }))
                } else {
                    None
                };
                let finished =
                    finish_media_segment_job(pool, &job.id, terminal_status, error).await?;
                return Ok(Some(MediaSegmentJobRunRecord {
                    job: finished.context("finished media segment job was not found")?,
                    summary: None,
                    local_audio_fingerprint_summary: None,
                    local_visual_frame_hash_summary: Some(summary),
                    local_audio_summary: None,
                    local_visual_summary: None,
                }));
            }
            Err(err) => {
                let error = json!({
                    "reason": "local_visual_frame_hash_failed",
                    "error": err.to_string(),
                    "provider_kind": job.provider_kind,
                });
                let retried = retry_or_fail_media_segment_job(pool, &job.id, error).await?;
                return Ok(Some(MediaSegmentJobRunRecord {
                    job: retried.context("retried media segment job was not found")?,
                    summary: None,
                    local_audio_fingerprint_summary: None,
                    local_visual_frame_hash_summary: None,
                    local_audio_summary: None,
                    local_visual_summary: None,
                }));
            }
        }
    }

    if job.job_type == MEDIA_SEGMENT_JOB_LOCAL_DETECTOR
        && job.scope_type == MEDIA_SEGMENT_JOB_SCOPE_MEDIA_FILE
        && job.provider_kind == PROVIDER_LOCAL_VISUAL_RECURRING
    {
        match run_local_visual_credits_detector_for_media_file(pool, &job.scope_id, preferences)
            .await
        {
            Ok(summary) => {
                let terminal_status = if summary.status == "skipped" {
                    "skipped"
                } else {
                    "succeeded"
                };
                let error = if summary.status == "skipped" {
                    summary
                        .reason
                        .as_ref()
                        .map(|reason| json!({ "reason": reason }))
                } else {
                    None
                };
                let finished =
                    finish_media_segment_job(pool, &job.id, terminal_status, error).await?;
                return Ok(Some(MediaSegmentJobRunRecord {
                    job: finished.context("finished media segment job was not found")?,
                    summary: None,
                    local_audio_fingerprint_summary: None,
                    local_visual_frame_hash_summary: None,
                    local_audio_summary: None,
                    local_visual_summary: Some(summary),
                }));
            }
            Err(err) => {
                let error = json!({
                    "reason": "local_visual_detector_failed",
                    "error": err.to_string(),
                    "provider_kind": job.provider_kind,
                });
                let retried = retry_or_fail_media_segment_job(pool, &job.id, error).await?;
                return Ok(Some(MediaSegmentJobRunRecord {
                    job: retried.context("retried media segment job was not found")?,
                    summary: None,
                    local_audio_fingerprint_summary: None,
                    local_visual_frame_hash_summary: None,
                    local_audio_summary: None,
                    local_visual_summary: None,
                }));
            }
        }
    }

    if job.job_type != MEDIA_SEGMENT_JOB_PROVIDER_REFRESH
        || job.scope_type != MEDIA_SEGMENT_JOB_SCOPE_MEDIA_FILE
    {
        return finish_unsupported_media_segment_job(pool, job).await;
    }

    let refresh_result = if matches!(
        job.provider_kind.as_str(),
        PROVIDER_THEINTRODB | PROVIDER_ANISKIP
    ) {
        refresh_builtin_provider_segments(
            pool,
            &job.scope_id,
            preferences,
            BuiltinProviderRefreshOptions {
                force_refresh: Some(false),
                provider_kind: Some(job.provider_kind.clone()),
            },
        )
        .await
    } else {
        refresh_marketplace_segment_provider_segments(
            pool,
            &job.scope_id,
            preferences,
            &job.provider_kind,
        )
        .await
    };

    match refresh_result {
        Ok(summary) => {
            if summary
                .providers
                .iter()
                .any(|provider| provider.status == "rate_limited")
            {
                let error = json!({
                    "reason": "provider_rate_limited",
                    "provider_kind": job.provider_kind,
                });
                let retried = retry_or_fail_media_segment_job(pool, &job.id, error).await?;
                return Ok(Some(MediaSegmentJobRunRecord {
                    job: retried.context("retried media segment job was not found")?,
                    summary: Some(summary),
                    local_audio_fingerprint_summary: None,
                    local_visual_frame_hash_summary: None,
                    local_audio_summary: None,
                    local_visual_summary: None,
                }));
            }

            let terminal_status = if summary
                .providers
                .iter()
                .all(|provider| provider.status == "skipped")
            {
                "skipped"
            } else {
                "succeeded"
            };
            let finished = finish_media_segment_job(pool, &job.id, terminal_status, None).await?;
            Ok(Some(MediaSegmentJobRunRecord {
                job: finished.context("finished media segment job was not found")?,
                summary: Some(summary),
                local_audio_fingerprint_summary: None,
                local_visual_frame_hash_summary: None,
                local_audio_summary: None,
                local_visual_summary: None,
            }))
        }
        Err(err) => {
            let error = json!({
                "reason": "provider_refresh_failed",
                "error": err.to_string(),
                "provider_kind": job.provider_kind,
            });
            let retried = retry_or_fail_media_segment_job(pool, &job.id, error).await?;
            Ok(Some(MediaSegmentJobRunRecord {
                job: retried.context("retried media segment job was not found")?,
                summary: None,
                local_audio_fingerprint_summary: None,
                local_visual_frame_hash_summary: None,
                local_audio_summary: None,
                local_visual_summary: None,
            }))
        }
    }
}

async fn finish_unsupported_media_segment_job(
    pool: &AnyPool,
    job: MediaSegmentJobRecord,
) -> Result<Option<MediaSegmentJobRunRecord>> {
    let error = json!({
        "reason": "unsupported_media_segment_job",
        "job_type": job.job_type,
        "scope_type": job.scope_type,
        "provider_kind": job.provider_kind,
    });
    let finished = finish_media_segment_job(pool, &job.id, "skipped", Some(error)).await?;
    Ok(Some(MediaSegmentJobRunRecord {
        job: finished.context("finished media segment job was not found")?,
        summary: None,
        local_audio_fingerprint_summary: None,
        local_visual_frame_hash_summary: None,
        local_audio_summary: None,
        local_visual_summary: None,
    }))
}

#[derive(Debug, Clone, Default)]
struct StaleMediaSegmentJobRecoverySummary {
    recovered: usize,
    requeued: usize,
    failed: usize,
}

async fn recover_stale_running_media_segment_jobs(
    pool: &AnyPool,
    stale_after_seconds: i64,
) -> Result<StaleMediaSegmentJobRecoverySummary> {
    let stale_after_seconds = stale_after_seconds.max(60);
    let cutoff = Utc::now() - ChronoDuration::seconds(stale_after_seconds);
    let rows = sqlx::query::<sqlx::Any>(
        "SELECT id, job_type, scope_type, scope_id, provider_kind, status, priority, attempts,
                max_attempts,
                CAST(next_attempt_at AS TEXT) AS next_attempt_at,
                locked_by,
                CAST(started_at AS TEXT) AS started_at,
                CAST(finished_at AS TEXT) AS finished_at,
                error_json
         FROM media_segment_jobs
         WHERE status = 'running'",
    )
    .fetch_all(pool)
    .await
    .context("listing stale running media segment jobs")?;

    let mut summary = StaleMediaSegmentJobRecoverySummary::default();
    for row in rows {
        let job = media_segment_job_from_row(&row);
        let Some(started_at) = job
            .started_at
            .as_deref()
            .and_then(parse_media_segment_job_timestamp)
        else {
            continue;
        };
        if started_at > cutoff {
            continue;
        }

        let terminal = job.attempts >= job.max_attempts;
        let status = if terminal { "failed" } else { "queued" };
        let next_attempt_at = if terminal {
            None
        } else {
            Some(timestamp_after_seconds(PROVIDER_JOB_RETRY_BACKOFF_SECONDS))
        };
        let finished_at = terminal.then(timestamp_now);
        let error = json!({
            "reason": "stale_running_job",
            "message": "running media segment job exceeded stale timeout and was recovered",
            "previous_status": job.status,
            "previous_locked_by": job.locked_by.clone(),
            "started_at": job.started_at.clone(),
            "stale_after_seconds": stale_after_seconds,
            "recovered_at": timestamp_now(),
        });
        let result = sqlx::query::<sqlx::Any>(
            "UPDATE media_segment_jobs
             SET status = $1,
                 locked_by = NULL,
                 next_attempt_at = $2,
                 finished_at = $3,
                 error_json = $4,
                 updated_at = CURRENT_TIMESTAMP
             WHERE id = $5
               AND status = 'running'
               AND started_at = $6",
        )
        .bind(status)
        .bind(next_attempt_at.as_deref())
        .bind(finished_at.as_deref())
        .bind(error.to_string())
        .bind(&job.id)
        .bind(job.started_at.as_deref())
        .execute(pool)
        .await
        .context("recovering stale running media segment job")?;

        if result.rows_affected() == 1 {
            summary.recovered += 1;
            if terminal {
                summary.failed += 1;
            } else {
                summary.requeued += 1;
            }
            if let Some(updated) = load_media_segment_job(pool, &job.id).await? {
                record_media_segment_job_status(&updated);
                record_media_segment_job_duration(&updated);
            }
        }
    }

    if summary.recovered > 0 {
        refresh_media_segment_job_backlog_metrics(pool).await;
    }

    Ok(summary)
}

pub async fn start_media_segment_job_worker_loop(state: AppState) {
    start_media_segment_job_worker_loop_until_shutdown(state, CancellationToken::new()).await;
}

pub async fn start_media_segment_job_worker_loop_until_shutdown(
    state: AppState,
    shutdown: CancellationToken,
) {
    start_media_segment_job_worker_loop_with_controls(
        state.db_pool.clone(),
        default_playback_preferences(),
        "elixir-midm-provider-worker".to_string(),
        shutdown,
        MEDIA_SEGMENT_WORKER_INTERVAL_SECONDS,
        MEDIA_SEGMENT_WORKER_ENQUEUE_BATCH_LIMIT,
        MEDIA_SEGMENT_WORKER_RUN_BATCH_LIMIT,
        MEDIA_SEGMENT_WORKER_MAX_RUNTIME_SECONDS,
    )
    .await;
}

async fn start_media_segment_job_worker_loop_with_controls(
    pool: AnyPool,
    preferences: PlaybackInteractionPreferences,
    worker_id: String,
    shutdown: CancellationToken,
    interval_seconds: u64,
    enqueue_batch_limit: usize,
    run_batch_limit: usize,
    max_runtime_seconds: u64,
) {
    let mut interval = tokio::time::interval(StdDuration::from_secs(interval_seconds.max(1)));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("media segment worker shutdown requested");
                break;
            }
            _ = interval.tick() => {
                if let Err(err) = run_media_segment_job_worker_iteration_with_preferences_and_shutdown(
                    &pool,
                    &preferences,
                    &worker_id,
                    enqueue_batch_limit,
                    run_batch_limit,
                    max_runtime_seconds,
                    Some(&shutdown),
                )
                .await
                {
                    tracing::warn!("media segment worker pass failed: {err}");
                }
            }
        }
    }
}

pub async fn run_media_segment_job_worker_iteration(
    state: &AppState,
) -> Result<MediaSegmentWorkerIterationSummary> {
    let preferences = default_playback_preferences();
    run_media_segment_job_worker_iteration_with_preferences(
        &state.db_pool,
        &preferences,
        "elixir-midm-provider-worker",
        MEDIA_SEGMENT_WORKER_ENQUEUE_BATCH_LIMIT,
        MEDIA_SEGMENT_WORKER_RUN_BATCH_LIMIT,
        MEDIA_SEGMENT_WORKER_MAX_RUNTIME_SECONDS,
    )
    .await
}

async fn run_media_segment_job_worker_iteration_with_preferences(
    pool: &AnyPool,
    preferences: &PlaybackInteractionPreferences,
    worker_id: &str,
    enqueue_batch_limit: usize,
    run_batch_limit: usize,
    max_runtime_seconds: u64,
) -> Result<MediaSegmentWorkerIterationSummary> {
    run_media_segment_job_worker_iteration_with_preferences_and_shutdown(
        pool,
        preferences,
        worker_id,
        enqueue_batch_limit,
        run_batch_limit,
        max_runtime_seconds,
        None,
    )
    .await
}

async fn run_media_segment_job_worker_iteration_with_preferences_and_shutdown(
    pool: &AnyPool,
    preferences: &PlaybackInteractionPreferences,
    worker_id: &str,
    enqueue_batch_limit: usize,
    run_batch_limit: usize,
    max_runtime_seconds: u64,
    shutdown: Option<&CancellationToken>,
) -> Result<MediaSegmentWorkerIterationSummary> {
    let iteration_started_at = StdInstant::now();
    let runtime_budget = StdDuration::from_secs(max_runtime_seconds);
    let stale_recovery =
        recover_stale_running_media_segment_jobs(pool, MEDIA_SEGMENT_STALE_RUNNING_JOB_SECONDS)
            .await?;
    let enqueue =
        enqueue_due_builtin_provider_refresh_jobs(pool, preferences, enqueue_batch_limit).await?;
    let mut summary = MediaSegmentWorkerIterationSummary {
        enqueue,
        stale_jobs_recovered: stale_recovery.recovered,
        stale_jobs_requeued: stale_recovery.requeued,
        stale_jobs_failed: stale_recovery.failed,
        runtime_budget_seconds: max_runtime_seconds,
        ..MediaSegmentWorkerIterationSummary::default()
    };
    let marketplace_enqueue = enqueue_due_marketplace_segment_provider_refresh_jobs(
        pool,
        preferences,
        enqueue_batch_limit,
    )
    .await?;
    summary.enqueue.providers_seen += marketplace_enqueue.providers_seen;
    summary.enqueue.files_seen += marketplace_enqueue.files_seen;
    summary.enqueue.jobs_queued += marketplace_enqueue.jobs_queued;
    summary.enqueue.jobs_failed += marketplace_enqueue.jobs_failed;

    let fingerprint_enqueue =
        enqueue_due_local_audio_fingerprint_jobs(pool, preferences, enqueue_batch_limit).await?;
    summary.enqueue.providers_seen += fingerprint_enqueue.providers_seen;
    summary.enqueue.files_seen += fingerprint_enqueue.files_seen;
    summary.enqueue.jobs_queued += fingerprint_enqueue.jobs_queued;
    summary.enqueue.jobs_failed += fingerprint_enqueue.jobs_failed;

    let detector_enqueue =
        enqueue_due_local_audio_detector_jobs(pool, preferences, enqueue_batch_limit).await?;
    if fingerprint_enqueue.providers_seen == 0 {
        summary.enqueue.providers_seen += detector_enqueue.providers_seen;
    }
    summary.enqueue.files_seen += detector_enqueue.files_seen;
    summary.enqueue.jobs_queued += detector_enqueue.jobs_queued;
    summary.enqueue.jobs_failed += detector_enqueue.jobs_failed;

    let visual_hash_enqueue =
        enqueue_due_local_visual_frame_hash_jobs(pool, preferences, enqueue_batch_limit).await?;
    summary.enqueue.providers_seen += visual_hash_enqueue.providers_seen;
    summary.enqueue.files_seen += visual_hash_enqueue.files_seen;
    summary.enqueue.jobs_queued += visual_hash_enqueue.jobs_queued;
    summary.enqueue.jobs_failed += visual_hash_enqueue.jobs_failed;

    let visual_enqueue =
        enqueue_due_local_visual_detector_jobs(pool, preferences, enqueue_batch_limit).await?;
    if visual_hash_enqueue.providers_seen == 0 {
        summary.enqueue.providers_seen += visual_enqueue.providers_seen;
    }
    summary.enqueue.files_seen += visual_enqueue.files_seen;
    summary.enqueue.jobs_queued += visual_enqueue.jobs_queued;
    summary.enqueue.jobs_failed += visual_enqueue.jobs_failed;

    for _ in 0..run_batch_limit {
        if max_runtime_seconds == 0 || iteration_started_at.elapsed() >= runtime_budget {
            summary.runtime_budget_exhausted = true;
            break;
        }
        if shutdown.is_some_and(CancellationToken::is_cancelled) {
            summary.runtime_budget_exhausted = true;
            break;
        }

        let Some(run) =
            run_next_media_segment_job_with_shutdown(pool, preferences, worker_id, shutdown)
                .await?
        else {
            break;
        };
        summary.jobs_run += 1;
        match run.job.status.as_str() {
            "succeeded" => summary.jobs_succeeded += 1,
            "skipped" => summary.jobs_skipped += 1,
            "queued" => summary.jobs_requeued += 1,
            "failed" => summary.jobs_failed += 1,
            "cancelled" => summary.jobs_cancelled += 1,
            _ => {}
        }
    }
    let elapsed_ms = iteration_started_at.elapsed().as_millis();
    summary.runtime_elapsed_ms = elapsed_ms.min(u128::from(u64::MAX)) as u64;
    if max_runtime_seconds == 0 || iteration_started_at.elapsed() >= runtime_budget {
        summary.runtime_budget_exhausted = true;
    }

    refresh_media_segment_job_backlog_metrics(pool).await;

    Ok(summary)
}

async fn enqueue_due_builtin_provider_refresh_jobs(
    pool: &AnyPool,
    preferences: &PlaybackInteractionPreferences,
    batch_limit: usize,
) -> Result<MediaSegmentProviderEnqueueSummary> {
    let mut summary = MediaSegmentProviderEnqueueSummary::default();
    let batch_limit = batch_limit.max(1);

    for provider_kind in [PROVIDER_THEINTRODB, PROVIDER_ANISKIP] {
        let provider_settings =
            provider_settings_for(&preferences.segment_provider_settings, provider_kind);
        if !provider_settings_enabled(provider_settings.as_ref()) {
            continue;
        }
        summary.providers_seen += 1;

        let media_file_ids =
            due_builtin_provider_media_files(pool, provider_kind, batch_limit).await?;
        summary.files_seen += media_file_ids.len();
        for media_file_id in media_file_ids {
            match enqueue_builtin_provider_refresh_job(pool, &media_file_id, provider_kind, 100)
                .await
            {
                Ok(_) => summary.jobs_queued += 1,
                Err(err) => {
                    summary.jobs_failed += 1;
                    tracing::warn!(
                        provider_kind,
                        media_file_id,
                        error = %err,
                        "failed to enqueue media segment provider refresh job"
                    );
                }
            }
        }
    }

    Ok(summary)
}

async fn enqueue_due_marketplace_segment_provider_refresh_jobs(
    pool: &AnyPool,
    preferences: &PlaybackInteractionPreferences,
    batch_limit: usize,
) -> Result<MediaSegmentProviderEnqueueSummary> {
    let mut summary = MediaSegmentProviderEnqueueSummary::default();
    let batch_limit = batch_limit.max(1);
    let store = ExtensionStore::new(pool);
    let providers = available_marketplace_segment_providers(&store, None, None).await?;

    for selected in providers {
        let Some(implementation) = selected.provider.implementation.as_deref() else {
            continue;
        };
        let provider_kind = normalize_provider_id(implementation)?;
        let provider_settings =
            provider_settings_for(&preferences.segment_provider_settings, &provider_kind);
        if !marketplace_provider_settings_enabled(provider_settings.as_ref()) {
            continue;
        }
        summary.providers_seen += 1;

        let media_file_ids = due_marketplace_segment_provider_media_files(
            pool,
            &selected,
            &provider_kind,
            batch_limit,
        )
        .await?;
        summary.files_seen += media_file_ids.len();
        for media_file_id in media_file_ids {
            let effective_settings = match effective_segment_provider_settings_for_media_file(
                pool,
                &media_file_id,
                &preferences.segment_provider_settings,
            )
            .await
            {
                Ok(settings) => settings,
                Err(err) => {
                    summary.jobs_failed += 1;
                    tracing::warn!(
                        provider_kind,
                        media_file_id,
                        error = %err,
                        "failed to resolve marketplace media segment provider settings"
                    );
                    continue;
                }
            };
            let file_provider_settings = provider_settings_for(&effective_settings, &provider_kind);
            if !marketplace_provider_settings_enabled(file_provider_settings.as_ref()) {
                continue;
            }
            match enqueue_media_segment_job(
                pool,
                MEDIA_SEGMENT_JOB_PROVIDER_REFRESH,
                MEDIA_SEGMENT_JOB_SCOPE_MEDIA_FILE,
                &media_file_id,
                &provider_kind,
                120,
                PROVIDER_JOB_MAX_ATTEMPTS,
            )
            .await
            {
                Ok(_) => summary.jobs_queued += 1,
                Err(err) => {
                    summary.jobs_failed += 1;
                    tracing::warn!(
                        provider_kind,
                        media_file_id,
                        error = %err,
                        "failed to enqueue marketplace media segment provider refresh job"
                    );
                }
            }
        }
    }

    Ok(summary)
}

async fn due_builtin_provider_media_files(
    pool: &AnyPool,
    provider_kind: &str,
    batch_limit: usize,
) -> Result<Vec<String>> {
    match provider_kind {
        PROVIDER_THEINTRODB => due_theintrodb_media_files(pool, batch_limit).await,
        PROVIDER_ANISKIP => due_aniskip_media_files(pool, batch_limit).await,
        _ => Ok(Vec::new()),
    }
}

async fn due_marketplace_segment_provider_media_files(
    pool: &AnyPool,
    selected: &MarketplaceSegmentProviderSelection,
    provider_kind: &str,
    batch_limit: usize,
) -> Result<Vec<String>> {
    let batch_limit = batch_limit.max(1);
    let supports_movies = segment_provider_supports_media_type(selected, "movie");
    let supports_series = segment_provider_supports_media_type(selected, "series");
    let supports_anime = segment_provider_supports_media_type(selected, "anime");
    let mut media_file_ids = Vec::new();

    if supports_movies {
        let movie_rows = sqlx::query::<sqlx::Any>(
            "SELECT DISTINCT mf.id AS media_file_id
             FROM media_files mf
             JOIN movie_files mfv ON mfv.media_file_id = mf.id
             JOIN movies m ON m.id = mfv.movie_id
             WHERE NOT EXISTS (
                   SELECT 1 FROM media_interaction_library_provider_settings lps
                   WHERE lps.source_config_id = mf.source_config_id
                     AND lps.provider_kind = $1
                     AND lps.enabled = FALSE
               )
               AND NOT EXISTS (
                   SELECT 1 FROM media_segment_provider_cache c
                   WHERE c.media_file_id = mf.id
                     AND c.provider_kind = $2
                     AND (c.expires_at IS NULL OR c.expires_at > CURRENT_TIMESTAMP)
               )
               AND NOT EXISTS (
                   SELECT 1 FROM media_segment_jobs j
                   WHERE j.scope_type = 'media_file'
                     AND j.scope_id = mf.id
                     AND j.provider_kind = $3
                     AND j.status IN ('queued', 'running')
               )
             ORDER BY mf.id
             LIMIT $4",
        )
        .bind(provider_kind)
        .bind(provider_kind)
        .bind(provider_kind)
        .bind(batch_limit as i64)
        .fetch_all(pool)
        .await
        .context("listing due marketplace segment provider movie files")?;
        media_file_ids.extend(movie_rows.iter().map(|row| row.get("media_file_id")));
    }

    let remaining = batch_limit.saturating_sub(media_file_ids.len());
    if remaining == 0 || (!supports_series && !supports_anime) {
        return Ok(media_file_ids);
    }

    let include_all_episodes = if supports_series { 1_i64 } else { 0_i64 };
    let episode_rows = sqlx::query::<sqlx::Any>(
        "SELECT DISTINCT mf.id AS media_file_id
         FROM media_files mf
         JOIN episode_files ef ON ef.media_file_id = mf.id
         JOIN episodes e ON e.id = ef.episode_id
         JOIN series s ON s.id = e.series_id
         WHERE COALESCE(e.absolute_episode_number, e.episode_number) > 0
           AND (
               $1 = 1
               OR (
                   (s.external_anilist IS NOT NULL AND TRIM(s.external_anilist) <> '')
                   OR EXISTS (
                       SELECT 1 FROM series_external_ids sei
                       WHERE sei.series_id = s.id
                         AND LOWER(sei.provider) IN ('mal', 'myanimelist', 'anilist', 'ani_list')
                         AND TRIM(sei.external_id) <> ''
                   )
                   OR EXISTS (
                       SELECT 1 FROM episode_external_ids eei
                       WHERE eei.episode_id = e.id
                         AND LOWER(eei.provider) IN ('mal', 'myanimelist', 'anilist', 'ani_list')
                         AND TRIM(eei.external_id) <> ''
                   )
               )
           )
           AND NOT EXISTS (
               SELECT 1 FROM media_interaction_library_provider_settings lps
               WHERE lps.source_config_id = mf.source_config_id
                 AND lps.provider_kind = $2
                 AND lps.enabled = FALSE
           )
           AND NOT EXISTS (
               SELECT 1 FROM media_segment_provider_cache c
               WHERE c.media_file_id = mf.id
                 AND c.provider_kind = $3
                 AND (c.expires_at IS NULL OR c.expires_at > CURRENT_TIMESTAMP)
           )
           AND NOT EXISTS (
               SELECT 1 FROM media_segment_jobs j
               WHERE j.scope_type = 'media_file'
                 AND j.scope_id = mf.id
                 AND j.provider_kind = $4
                 AND j.status IN ('queued', 'running')
           )
         ORDER BY mf.id
         LIMIT $5",
    )
    .bind(include_all_episodes)
    .bind(provider_kind)
    .bind(provider_kind)
    .bind(provider_kind)
    .bind(remaining as i64)
    .fetch_all(pool)
    .await
    .context("listing due marketplace segment provider episode files")?;
    for row in episode_rows {
        let media_file_id: String = row.get("media_file_id");
        if !media_file_ids.contains(&media_file_id) {
            media_file_ids.push(media_file_id);
        }
    }

    Ok(media_file_ids)
}

async fn due_theintrodb_media_files(pool: &AnyPool, batch_limit: usize) -> Result<Vec<String>> {
    let mut media_file_ids = Vec::new();
    let movie_rows = sqlx::query::<sqlx::Any>(
        "SELECT DISTINCT mf.id AS media_file_id
         FROM media_files mf
         JOIN movie_files mfv ON mfv.media_file_id = mf.id
         JOIN movies m ON m.id = mfv.movie_id
         WHERE (
             (m.external_imdb IS NOT NULL AND TRIM(m.external_imdb) <> '')
             OR EXISTS (
                 SELECT 1 FROM movie_external_ids mei
                 WHERE mei.movie_id = m.id
                   AND LOWER(mei.provider) IN ('imdb', 'imdb_id')
                   AND TRIM(mei.external_id) <> ''
             )
         )
           AND NOT EXISTS (
               SELECT 1 FROM media_interaction_library_provider_settings lps
               WHERE lps.source_config_id = mf.source_config_id
                 AND lps.provider_kind = 'theintrodb'
                 AND lps.enabled = FALSE
           )
           AND NOT EXISTS (
               SELECT 1 FROM media_segment_provider_cache c
               WHERE c.media_file_id = mf.id
                 AND c.provider_kind = 'theintrodb'
                 AND (c.expires_at IS NULL OR c.expires_at > CURRENT_TIMESTAMP)
           )
           AND NOT EXISTS (
               SELECT 1 FROM media_segment_jobs j
               WHERE j.scope_type = 'media_file'
                 AND j.scope_id = mf.id
                 AND j.provider_kind = 'theintrodb'
                 AND j.status IN ('queued', 'running')
           )
         ORDER BY mf.id
         LIMIT $1",
    )
    .bind(batch_limit as i64)
    .fetch_all(pool)
    .await
    .context("listing due TheIntroDB movie files")?;
    media_file_ids.extend(movie_rows.iter().map(|row| row.get("media_file_id")));

    let remaining = batch_limit.saturating_sub(media_file_ids.len());
    if remaining == 0 {
        return Ok(media_file_ids);
    }

    let episode_rows = sqlx::query::<sqlx::Any>(
        "SELECT DISTINCT mf.id AS media_file_id
         FROM media_files mf
         JOIN episode_files ef ON ef.media_file_id = mf.id
         JOIN episodes e ON e.id = ef.episode_id
         JOIN series s ON s.id = e.series_id
         WHERE e.episode_number > 0
           AND (
               (s.external_imdb IS NOT NULL AND TRIM(s.external_imdb) <> '')
               OR EXISTS (
                   SELECT 1 FROM series_external_ids sei
                   WHERE sei.series_id = s.id
                   AND LOWER(sei.provider) IN ('imdb', 'imdb_id')
                   AND TRIM(sei.external_id) <> ''
               )
           )
           AND NOT EXISTS (
               SELECT 1 FROM media_interaction_library_provider_settings lps
               WHERE lps.source_config_id = mf.source_config_id
                 AND lps.provider_kind = 'theintrodb'
                 AND lps.enabled = FALSE
           )
           AND NOT EXISTS (
               SELECT 1 FROM media_segment_provider_cache c
               WHERE c.media_file_id = mf.id
                 AND c.provider_kind = 'theintrodb'
                 AND (c.expires_at IS NULL OR c.expires_at > CURRENT_TIMESTAMP)
           )
           AND NOT EXISTS (
               SELECT 1 FROM media_segment_jobs j
               WHERE j.scope_type = 'media_file'
                 AND j.scope_id = mf.id
                 AND j.provider_kind = 'theintrodb'
                 AND j.status IN ('queued', 'running')
           )
         ORDER BY mf.id
         LIMIT $1",
    )
    .bind(remaining as i64)
    .fetch_all(pool)
    .await
    .context("listing due TheIntroDB episode files")?;
    for row in episode_rows {
        let media_file_id: String = row.get("media_file_id");
        if !media_file_ids.contains(&media_file_id) {
            media_file_ids.push(media_file_id);
        }
    }

    Ok(media_file_ids)
}

async fn due_aniskip_media_files(pool: &AnyPool, batch_limit: usize) -> Result<Vec<String>> {
    let rows = sqlx::query::<sqlx::Any>(
        "SELECT DISTINCT mf.id AS media_file_id
         FROM media_files mf
         JOIN episode_files ef ON ef.media_file_id = mf.id
         JOIN episodes e ON e.id = ef.episode_id
         JOIN series s ON s.id = e.series_id
         WHERE COALESCE(e.absolute_episode_number, e.episode_number) > 0
           AND (
               EXISTS (
                   SELECT 1 FROM series_external_ids sei
                   WHERE sei.series_id = s.id
                     AND LOWER(sei.provider) IN ('mal', 'myanimelist')
                     AND TRIM(sei.external_id) <> ''
               )
               OR EXISTS (
                   SELECT 1 FROM episode_external_ids eei
                   WHERE eei.episode_id = e.id
                   AND LOWER(eei.provider) IN ('mal', 'myanimelist')
                   AND TRIM(eei.external_id) <> ''
               )
           )
           AND NOT EXISTS (
               SELECT 1 FROM media_interaction_library_provider_settings lps
               WHERE lps.source_config_id = mf.source_config_id
                 AND lps.provider_kind = 'aniskip'
                 AND lps.enabled = FALSE
           )
           AND NOT EXISTS (
               SELECT 1 FROM media_segment_provider_cache c
               WHERE c.media_file_id = mf.id
                 AND c.provider_kind = 'aniskip'
                 AND (c.expires_at IS NULL OR c.expires_at > CURRENT_TIMESTAMP)
           )
           AND NOT EXISTS (
               SELECT 1 FROM media_segment_jobs j
               WHERE j.scope_type = 'media_file'
                 AND j.scope_id = mf.id
                 AND j.provider_kind = 'aniskip'
                 AND j.status IN ('queued', 'running')
           )
         ORDER BY mf.id
         LIMIT $1",
    )
    .bind(batch_limit as i64)
    .fetch_all(pool)
    .await
    .context("listing due AniSkip episode files")?;

    Ok(rows.iter().map(|row| row.get("media_file_id")).collect())
}

async fn enqueue_due_local_audio_fingerprint_jobs(
    pool: &AnyPool,
    preferences: &PlaybackInteractionPreferences,
    batch_limit: usize,
) -> Result<MediaSegmentProviderEnqueueSummary> {
    let mut summary = MediaSegmentProviderEnqueueSummary::default();
    let provider_settings = provider_settings_for(
        &preferences.segment_provider_settings,
        PROVIDER_LOCAL_AUDIO_RECURRING,
    );
    if !provider_settings_enabled(provider_settings.as_ref()) {
        return Ok(summary);
    }

    summary.providers_seen = 1;
    let media_file_ids = due_local_audio_fingerprint_media_files(pool, batch_limit.max(1)).await?;
    summary.files_seen = media_file_ids.len();
    for media_file_id in media_file_ids {
        match enqueue_local_audio_fingerprint_job(pool, &media_file_id, 180).await {
            Ok(_) => summary.jobs_queued += 1,
            Err(err) => {
                summary.jobs_failed += 1;
                tracing::warn!(
                    provider_kind = PROVIDER_LOCAL_AUDIO_RECURRING,
                    media_file_id,
                    error = %err,
                    "failed to enqueue local audio fingerprint job"
                );
            }
        }
    }

    Ok(summary)
}

async fn due_local_audio_fingerprint_media_files(
    pool: &AnyPool,
    batch_limit: usize,
) -> Result<Vec<String>> {
    let rows = sqlx::query::<sqlx::Any>(
        "SELECT DISTINCT mf.id AS media_file_id
         FROM media_files mf
         JOIN episode_files ef ON ef.media_file_id = mf.id
         JOIN episodes e ON e.id = ef.episode_id
         WHERE e.season_id IS NOT NULL
           AND mf.scan_state = 'ok'
           AND NOT EXISTS (
               SELECT 1 FROM media_interaction_library_provider_settings lps
               WHERE lps.source_config_id = mf.source_config_id
                 AND lps.provider_kind = 'local_audio_recurring'
                 AND lps.enabled = FALSE
           )
           AND NOT EXISTS (
               SELECT 1 FROM media_file_fingerprints fp
               WHERE fp.media_file_id = mf.id
                 AND fp.audio_fingerprint_json IS NOT NULL
                 AND TRIM(fp.audio_fingerprint_json) <> ''
           )
           AND NOT EXISTS (
               SELECT 1 FROM media_segment_jobs j
               WHERE j.job_type = 'audio_fingerprint'
                 AND j.scope_type = 'media_file'
                 AND j.scope_id = mf.id
                 AND j.provider_kind = 'local_audio_recurring'
                 AND j.status IN ('queued', 'running', 'succeeded', 'skipped', 'failed')
           )
         ORDER BY e.season_number ASC,
                  COALESCE(e.absolute_episode_number, e.episode_number) ASC,
                  e.episode_number ASC,
                  mf.id ASC
         LIMIT $1",
    )
    .bind(batch_limit as i64)
    .fetch_all(pool)
    .await
    .context("listing due local audio fingerprint files")?;

    Ok(rows.iter().map(|row| row.get("media_file_id")).collect())
}

async fn run_local_audio_fingerprint_for_media_file_with_job(
    pool: &AnyPool,
    media_file_id: &str,
    preferences: &PlaybackInteractionPreferences,
    job_id: Option<&str>,
    shutdown: Option<&CancellationToken>,
) -> Result<LocalAudioFingerprintSummary> {
    let media_file_id = normalize_required_text(media_file_id, "media_file_id")?;
    let provider_settings = provider_settings_for_media_file(
        pool,
        &media_file_id,
        &preferences.segment_provider_settings,
        PROVIDER_LOCAL_AUDIO_RECURRING,
    )
    .await?;
    let mut summary = LocalAudioFingerprintSummary {
        media_file_id: media_file_id.clone(),
        status: "ok".to_string(),
        fingerprint_version: LOCAL_AUDIO_FINGERPRINT_VERSION.to_string(),
        ..LocalAudioFingerprintSummary::default()
    };

    if !provider_settings_enabled(provider_settings.as_ref()) {
        summary.status = "skipped".to_string();
        summary.reason = Some("local_audio_detector_disabled".to_string());
        return Ok(summary);
    }

    let file = load_local_audio_fingerprint_media_file(pool, &media_file_id).await?;
    if file.scan_state != "ok" {
        summary.status = "skipped".to_string();
        summary.reason = Some("media_file_not_playable".to_string());
        return Ok(summary);
    }

    let fs_metadata = match tokio::fs::metadata(&file.path).await {
        Ok(metadata) => metadata,
        Err(err) => {
            summary.status = "skipped".to_string();
            summary.file_size_bytes = file.size_bytes;
            summary.reason = Some(format!("file_unavailable:{err}"));
            return Ok(summary);
        }
    };
    summary.file_size_bytes = Some(fs_metadata.len() as i64);

    let timeout_seconds = local_audio_fingerprint_timeout_seconds(provider_settings.as_ref());
    let metadata = match timeout(
        StdDuration::from_secs(timeout_seconds),
        ffprobe::probe(&file.path),
    )
    .await
    {
        Ok(Ok(metadata)) => metadata,
        Ok(Err(err)) => {
            summary.status = "skipped".to_string();
            summary.reason = Some(format!("ffprobe_failed:{err}"));
            return Ok(summary);
        }
        Err(_) => {
            summary.status = "skipped".to_string();
            summary.reason = Some("ffprobe_timeout".to_string());
            return Ok(summary);
        }
    };

    let duration_seconds = metadata
        .duration_seconds
        .map(i64::from)
        .or(file.duration_seconds)
        .filter(|duration| *duration > 0);
    summary.duration_seconds = duration_seconds;
    if !metadata_has_audio_stream(&metadata) {
        summary.status = "skipped".to_string();
        summary.reason = Some("no_audio_stream".to_string());
        upsert_media_file_audio_fingerprint(
            pool,
            &file,
            &metadata,
            summary.file_size_bytes,
            duration_seconds,
            None,
        )
        .await?;
        return Ok(summary);
    }

    let Some(duration_seconds) = duration_seconds else {
        summary.status = "skipped".to_string();
        summary.reason = Some("unknown_duration".to_string());
        return Ok(summary);
    };
    if duration_seconds as f64 <= LOCAL_AUDIO_FINGERPRINT_MIN_DURATION_SECONDS {
        summary.status = "skipped".to_string();
        summary.reason = Some("duration_too_short".to_string());
        return Ok(summary);
    }

    let plan = local_audio_fingerprint_plan(duration_seconds as f64);
    summary.extraction_ranges = plan.len();
    summary.windows_planned = plan.iter().map(|range| range.windows.len()).sum::<usize>();
    if summary.windows_planned == 0 {
        summary.status = "skipped".to_string();
        summary.reason = Some("no_fingerprint_windows".to_string());
        return Ok(summary);
    }

    let cancellation = job_id.map(|job_id| MediaSegmentJobCancellation {
        pool,
        job_id,
        shutdown,
    });
    let windows = match extract_local_audio_fingerprint_windows(
        &file.path,
        &plan,
        timeout_seconds,
        cancellation,
    )
    .await
    {
        Ok(windows) => windows,
        Err(err) => {
            if is_media_segment_worker_shutdown_interruption(&err) {
                return Err(err);
            }
            summary.status = "skipped".to_string();
            summary.reason = Some(format!("ffmpeg_failed:{err}"));
            return Ok(summary);
        }
    };
    summary.windows_fingerprinted = windows.len();
    if windows.is_empty() {
        summary.status = "skipped".to_string();
        summary.reason = Some("audio_fingerprint_extraction_empty".to_string());
        return Ok(summary);
    }

    let payload = local_audio_fingerprint_payload(duration_seconds, &windows);
    upsert_media_file_audio_fingerprint(
        pool,
        &file,
        &metadata,
        summary.file_size_bytes,
        Some(duration_seconds),
        Some(payload),
    )
    .await?;

    Ok(summary)
}

#[derive(Debug, Clone)]
struct LocalAudioFingerprintMediaFile {
    id: String,
    path: String,
    size_bytes: Option<i64>,
    container: Option<String>,
    video_codec: Option<String>,
    audio_codec: Option<String>,
    duration_seconds: Option<i64>,
    scan_state: String,
}

#[derive(Debug, Clone)]
struct LocalAudioFingerprintRange {
    start_seconds: f64,
    end_seconds: f64,
    windows: Vec<LocalAudioFingerprintWindowSpec>,
}

#[derive(Debug, Clone)]
struct LocalAudioFingerprintWindowSpec {
    start_seconds: f64,
    end_seconds: f64,
}

#[derive(Debug, Clone)]
struct LocalAudioFingerprintOutputWindow {
    start_seconds: f64,
    end_seconds: f64,
    hash: String,
}

async fn load_local_audio_fingerprint_media_file(
    pool: &AnyPool,
    media_file_id: &str,
) -> Result<LocalAudioFingerprintMediaFile> {
    let row = sqlx::query::<sqlx::Any>(
        "SELECT
             mf.id,
             mf.path,
             mf.size_bytes,
             mf.container,
             mf.video_codec,
             mf.audio_codec,
             mf.scan_state,
             COALESCE(fp.duration_seconds, mi.runtime_seconds) AS duration_seconds
         FROM media_files mf
         JOIN media_items mi ON mi.id = mf.media_item_id
         LEFT JOIN media_file_fingerprints fp ON fp.media_file_id = mf.id
         WHERE mf.id = $1
         LIMIT 1",
    )
    .bind(media_file_id)
    .fetch_optional(pool)
    .await
    .context("loading local audio fingerprint media file")?
    .context("media file not found")?;

    Ok(LocalAudioFingerprintMediaFile {
        id: row.get("id"),
        path: row.get("path"),
        size_bytes: row.try_get::<i64, _>("size_bytes").ok(),
        container: row_string(&row, "container"),
        video_codec: row_string(&row, "video_codec"),
        audio_codec: row_string(&row, "audio_codec"),
        duration_seconds: row.try_get::<i64, _>("duration_seconds").ok(),
        scan_state: row
            .try_get::<String, _>("scan_state")
            .unwrap_or_else(|_| "unknown".to_string()),
    })
}

fn metadata_has_audio_stream(metadata: &ffprobe::MediaMetadata) -> bool {
    metadata
        .streams
        .iter()
        .any(|stream| stream.codec_type.as_deref() == Some("audio"))
        || metadata
            .audio_codec
            .as_deref()
            .is_some_and(|codec| !codec.is_empty())
}

fn local_audio_fingerprint_plan(duration_seconds: f64) -> Vec<LocalAudioFingerprintRange> {
    if !duration_seconds.is_finite()
        || duration_seconds <= LOCAL_AUDIO_FINGERPRINT_MIN_DURATION_SECONDS
    {
        return Vec::new();
    }

    let early_end = (duration_seconds * EARLY_WINDOW_FRACTION)
        .min(LOCAL_AUDIO_FINGERPRINT_MAX_RANGE_SECONDS)
        .min(duration_seconds);
    let late_start = (duration_seconds * LATE_WINDOW_FRACTION)
        .max(duration_seconds - LOCAL_AUDIO_FINGERPRINT_MAX_RANGE_SECONDS)
        .max(0.0);

    let mut remaining = LOCAL_AUDIO_FINGERPRINT_MAX_WINDOWS_PER_FILE;
    let mut ranges = Vec::new();
    for (start_seconds, end_seconds) in [(0.0, early_end), (late_start, duration_seconds)] {
        if remaining == 0 || end_seconds <= start_seconds {
            continue;
        }
        let windows = local_audio_fingerprint_window_specs(start_seconds, end_seconds, remaining);
        if windows.is_empty() {
            continue;
        }
        remaining = remaining.saturating_sub(windows.len());
        ranges.push(LocalAudioFingerprintRange {
            start_seconds,
            end_seconds,
            windows,
        });
    }

    ranges
}

fn local_audio_fingerprint_window_specs(
    start_seconds: f64,
    end_seconds: f64,
    max_windows: usize,
) -> Vec<LocalAudioFingerprintWindowSpec> {
    let mut windows = Vec::new();
    for window_length in LOCAL_AUDIO_FINGERPRINT_WINDOW_LENGTHS_SECONDS {
        if windows.len() >= max_windows || end_seconds - start_seconds < window_length {
            continue;
        }
        let mut window_start = start_seconds;
        while windows.len() < max_windows && window_start + window_length <= end_seconds + 0.001 {
            windows.push(LocalAudioFingerprintWindowSpec {
                start_seconds: round_millis(window_start),
                end_seconds: round_millis(window_start + window_length),
            });
            window_start += LOCAL_AUDIO_FINGERPRINT_STEP_SECONDS;
        }
    }
    windows.sort_by(|left, right| {
        left.start_seconds
            .partial_cmp(&right.start_seconds)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                left.end_seconds
                    .partial_cmp(&right.end_seconds)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    });
    windows.dedup_by(|left, right| {
        (left.start_seconds - right.start_seconds).abs() < 0.001
            && (left.end_seconds - right.end_seconds).abs() < 0.001
    });
    windows
}

async fn extract_local_audio_fingerprint_windows(
    path: &str,
    plan: &[LocalAudioFingerprintRange],
    timeout_seconds: u64,
    cancellation: Option<MediaSegmentJobCancellation<'_>>,
) -> Result<Vec<LocalAudioFingerprintOutputWindow>> {
    let mut output_windows = Vec::new();
    let mut failed_ranges = 0usize;

    for range in plan {
        let range_duration = range.end_seconds - range.start_seconds;
        let pcm = match extract_local_audio_pcm_range(
            path,
            range.start_seconds,
            range_duration,
            timeout_seconds,
            cancellation,
        )
        .await
        {
            Ok(pcm) => pcm,
            Err(err) => {
                failed_ranges += 1;
                tracing::warn!(
                    path,
                    start_seconds = range.start_seconds,
                    end_seconds = range.end_seconds,
                    error = %err,
                    "local audio fingerprint extraction range failed"
                );
                continue;
            }
        };

        for window in &range.windows {
            let relative_start = window.start_seconds - range.start_seconds;
            let relative_end = window.end_seconds - range.start_seconds;
            if let Some(hash) =
                local_audio_fingerprint_hash_for_pcm_window(&pcm, relative_start, relative_end)
            {
                output_windows.push(LocalAudioFingerprintOutputWindow {
                    start_seconds: window.start_seconds,
                    end_seconds: window.end_seconds,
                    hash,
                });
            }
        }
    }

    if output_windows.is_empty() && failed_ranges > 0 {
        bail!("all local audio fingerprint extraction ranges failed");
    }

    Ok(output_windows)
}

async fn extract_local_audio_pcm_range(
    path: &str,
    start_seconds: f64,
    duration_seconds: f64,
    timeout_seconds: u64,
    cancellation: Option<MediaSegmentJobCancellation<'_>>,
) -> Result<Vec<u8>> {
    let mut command = Command::new("ffmpeg");
    command.kill_on_drop(true);
    let child = command
        .arg("-hide_banner")
        .arg("-nostdin")
        .arg("-v")
        .arg("error")
        .arg("-ss")
        .arg(format_seconds(start_seconds))
        .arg("-t")
        .arg(format_seconds(duration_seconds))
        .arg("-i")
        .arg(path)
        .arg("-map")
        .arg("0:a:0")
        .arg("-vn")
        .arg("-sn")
        .arg("-dn")
        .arg("-ac")
        .arg("1")
        .arg("-ar")
        .arg(LOCAL_AUDIO_FINGERPRINT_SAMPLE_RATE_HZ.to_string())
        .arg("-f")
        .arg("s16le")
        .arg("pipe:1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("spawning ffmpeg for local audio fingerprint extraction")?;

    let output = wait_for_local_ffmpeg_output(
        child,
        timeout_seconds,
        cancellation,
        "ffmpeg local audio fingerprint extraction",
    )
    .await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "ffmpeg local audio fingerprint extraction failed with code {:?}: {}",
            output.status.code(),
            truncate_for_error(&stderr, LOCAL_AUDIO_FINGERPRINT_FFMPEG_STDERR_LIMIT)
        );
    }

    Ok(output.stdout)
}

fn local_audio_fingerprint_hash_for_pcm_window(
    pcm: &[u8],
    relative_start_seconds: f64,
    relative_end_seconds: f64,
) -> Option<String> {
    if !relative_start_seconds.is_finite()
        || !relative_end_seconds.is_finite()
        || relative_end_seconds <= relative_start_seconds
    {
        return None;
    }

    let bytes_per_sample = 2usize;
    let sample_rate = LOCAL_AUDIO_FINGERPRINT_SAMPLE_RATE_HZ as usize;
    let start_sample = (relative_start_seconds * sample_rate as f64).round() as usize;
    let end_sample = (relative_end_seconds * sample_rate as f64).round() as usize;
    if end_sample <= start_sample {
        return None;
    }

    let start_byte = start_sample.saturating_mul(bytes_per_sample);
    let end_byte = end_sample
        .saturating_mul(bytes_per_sample)
        .min(pcm.len() - (pcm.len() % bytes_per_sample));
    if end_byte <= start_byte || end_byte > pcm.len() {
        return None;
    }

    local_audio_feature_hash(&pcm[start_byte..end_byte])
}

fn local_audio_feature_hash(pcm_window: &[u8]) -> Option<String> {
    let sample_count = pcm_window.len() / 2;
    let min_samples = (20.0 * LOCAL_AUDIO_FINGERPRINT_SAMPLE_RATE_HZ as f64) as usize;
    if sample_count < min_samples {
        return None;
    }

    let frame_samples = (LOCAL_AUDIO_FINGERPRINT_FRAME_SECONDS
        * LOCAL_AUDIO_FINGERPRINT_SAMPLE_RATE_HZ as f64)
        .round()
        .max(1.0) as usize;
    let mut frames = Vec::new();
    let mut offset = 0usize;
    while offset + frame_samples <= sample_count {
        frames.push(local_audio_frame_stats(pcm_window, offset, frame_samples));
        offset += frame_samples;
    }
    if frames.is_empty() {
        return None;
    }

    let average_rms = frames.iter().map(|frame| frame.rms).sum::<f64>() / frames.len() as f64;
    let average_rms = average_rms.max(1.0);
    let mut feature_bytes = Vec::with_capacity(frames.len() * 3 + 16);
    feature_bytes.extend_from_slice(LOCAL_AUDIO_FINGERPRINT_VERSION.as_bytes());
    feature_bytes.extend_from_slice(&(frames.len() as u32).to_le_bytes());
    for frame in frames {
        let relative_rms = ((frame.rms / average_rms) * 16.0).round().clamp(0.0, 63.0) as u8;
        let zero_crossing = ((frame.zero_crossing_rate * 255.0).round()).clamp(0.0, 255.0) as u8;
        let peak_ratio = ((frame.peak / average_rms) * 8.0).round().clamp(0.0, 63.0) as u8;
        feature_bytes.push(relative_rms);
        feature_bytes.push(zero_crossing);
        feature_bytes.push(peak_ratio);
    }

    Some(format!("laf1:{}", blake3::hash(&feature_bytes).to_hex()))
}

#[derive(Debug, Clone, Copy)]
struct LocalAudioFrameStats {
    rms: f64,
    zero_crossing_rate: f64,
    peak: f64,
}

fn local_audio_frame_stats(
    pcm_window: &[u8],
    start_sample: usize,
    sample_count: usize,
) -> LocalAudioFrameStats {
    let mut sum_squares = 0.0;
    let mut peak: f64 = 0.0;
    let mut zero_crossings = 0usize;
    let mut previous = 0i16;
    let mut have_previous = false;

    for sample_index in start_sample..start_sample + sample_count {
        let byte_index = sample_index * 2;
        if byte_index + 1 >= pcm_window.len() {
            break;
        }
        let sample = i16::from_le_bytes([pcm_window[byte_index], pcm_window[byte_index + 1]]);
        let sample_abs = f64::from(sample.unsigned_abs());
        peak = peak.max(sample_abs);
        sum_squares += f64::from(sample) * f64::from(sample);
        if have_previous && ((sample >= 0 && previous < 0) || (sample < 0 && previous >= 0)) {
            zero_crossings += 1;
        }
        previous = sample;
        have_previous = true;
    }

    let rms = (sum_squares / sample_count.max(1) as f64).sqrt();
    LocalAudioFrameStats {
        rms,
        zero_crossing_rate: zero_crossings as f64 / sample_count.max(1) as f64,
        peak,
    }
}

fn local_audio_fingerprint_payload(
    duration_seconds: i64,
    windows: &[LocalAudioFingerprintOutputWindow],
) -> Value {
    json!({
        "version": LOCAL_AUDIO_FINGERPRINT_VERSION,
        "detector": "local_audio_recurring",
        "sample_rate_hz": LOCAL_AUDIO_FINGERPRINT_SAMPLE_RATE_HZ,
        "channel_layout": "mono",
        "duration_seconds": duration_seconds,
        "window_lengths_seconds": LOCAL_AUDIO_FINGERPRINT_WINDOW_LENGTHS_SECONDS,
        "step_seconds": LOCAL_AUDIO_FINGERPRINT_STEP_SECONDS,
        "windows": windows.iter().map(|window| {
            json!({
                "start_seconds": window.start_seconds,
                "end_seconds": window.end_seconds,
                "hash": window.hash,
            })
        }).collect::<Vec<_>>()
    })
}

async fn upsert_media_file_audio_fingerprint(
    pool: &AnyPool,
    file: &LocalAudioFingerprintMediaFile,
    metadata: &ffprobe::MediaMetadata,
    file_size_bytes: Option<i64>,
    duration_seconds: Option<i64>,
    audio_fingerprint: Option<Value>,
) -> Result<()> {
    let container = metadata
        .container
        .clone()
        .or_else(|| file.container.clone());
    let video_codec = metadata
        .video_codec
        .clone()
        .or_else(|| file.video_codec.clone());
    let audio_codec = metadata
        .audio_codec
        .clone()
        .or_else(|| file.audio_codec.clone());
    let audio_fingerprint_json = audio_fingerprint.map(|value| value.to_string());

    sqlx::query::<sqlx::Any>(
        "INSERT INTO media_file_fingerprints
            (media_file_id, duration_seconds, file_size_bytes, container, video_codec,
             audio_codec, audio_fingerprint_json, fingerprint_version, computed_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP)
         ON CONFLICT(media_file_id) DO UPDATE SET
             duration_seconds = excluded.duration_seconds,
             file_size_bytes = excluded.file_size_bytes,
             container = excluded.container,
             video_codec = excluded.video_codec,
             audio_codec = excluded.audio_codec,
             audio_fingerprint_json = COALESCE(excluded.audio_fingerprint_json, media_file_fingerprints.audio_fingerprint_json),
             fingerprint_version = excluded.fingerprint_version,
             computed_at = CURRENT_TIMESTAMP",
    )
    .bind(&file.id)
    .bind(duration_seconds)
    .bind(file_size_bytes.or(file.size_bytes))
    .bind(container)
    .bind(video_codec)
    .bind(audio_codec)
    .bind(audio_fingerprint_json)
    .bind(LOCAL_AUDIO_FINGERPRINT_VERSION)
    .execute(pool)
    .await
    .context("upserting local audio media file fingerprint")?;

    Ok(())
}

fn local_audio_fingerprint_timeout_seconds(settings: Option<&Value>) -> u64 {
    settings
        .and_then(|value| {
            value
                .get("fingerprint_timeout_seconds")
                .or_else(|| value.get("fingerprintTimeoutSeconds"))
        })
        .and_then(Value::as_u64)
        .filter(|value| (5..=600).contains(value))
        .unwrap_or(LOCAL_AUDIO_FINGERPRINT_TIMEOUT_SECONDS)
}

async fn enqueue_due_local_visual_frame_hash_jobs(
    pool: &AnyPool,
    preferences: &PlaybackInteractionPreferences,
    batch_limit: usize,
) -> Result<MediaSegmentProviderEnqueueSummary> {
    let mut summary = MediaSegmentProviderEnqueueSummary::default();
    let provider_settings = provider_settings_for(
        &preferences.segment_provider_settings,
        PROVIDER_LOCAL_VISUAL_RECURRING,
    );
    if !provider_settings_enabled(provider_settings.as_ref()) {
        return Ok(summary);
    }

    summary.providers_seen = 1;
    let media_file_ids = due_local_visual_frame_hash_media_files(pool, batch_limit.max(1)).await?;
    summary.files_seen = media_file_ids.len();
    for media_file_id in media_file_ids {
        match enqueue_local_visual_frame_hash_job(pool, &media_file_id, 210).await {
            Ok(_) => summary.jobs_queued += 1,
            Err(err) => {
                summary.jobs_failed += 1;
                tracing::warn!(
                    provider_kind = PROVIDER_LOCAL_VISUAL_RECURRING,
                    media_file_id,
                    error = %err,
                    "failed to enqueue local visual frame hash job"
                );
            }
        }
    }
    Ok(summary)
}

async fn due_local_visual_frame_hash_media_files(
    pool: &AnyPool,
    batch_limit: usize,
) -> Result<Vec<String>> {
    let rows = sqlx::query::<sqlx::Any>(
        "SELECT DISTINCT mf.id AS media_file_id
         FROM media_files mf
         WHERE mf.scan_state = 'ok'
           AND NOT EXISTS (
               SELECT 1 FROM media_interaction_library_provider_settings lps
               WHERE lps.source_config_id = mf.source_config_id
                 AND lps.provider_kind = 'local_visual_recurring'
                 AND lps.enabled = FALSE
           )
           AND (
               EXISTS (
                   SELECT 1 FROM movie_files mfv
                   WHERE mfv.media_file_id = mf.id
               )
               OR EXISTS (
                   SELECT 1 FROM episode_files ef
                   WHERE ef.media_file_id = mf.id
               )
           )
           AND NOT EXISTS (
               SELECT 1 FROM media_file_fingerprints fp
               WHERE fp.media_file_id = mf.id
                 AND fp.video_frame_hash_json IS NOT NULL
                 AND TRIM(fp.video_frame_hash_json) <> ''
           )
           AND NOT EXISTS (
               SELECT 1 FROM media_segment_jobs j
               WHERE j.job_type = 'video_frame_hash'
                 AND j.scope_type = 'media_file'
                 AND j.scope_id = mf.id
                 AND j.provider_kind = 'local_visual_recurring'
                 AND j.status IN ('queued', 'running', 'succeeded', 'skipped', 'failed')
           )
         ORDER BY mf.id ASC
         LIMIT $1",
    )
    .bind(batch_limit as i64)
    .fetch_all(pool)
    .await
    .context("listing due local visual frame hash files")?;

    Ok(rows.iter().map(|row| row.get("media_file_id")).collect())
}

async fn run_local_visual_frame_hash_for_media_file_with_job(
    pool: &AnyPool,
    media_file_id: &str,
    preferences: &PlaybackInteractionPreferences,
    job_id: Option<&str>,
    shutdown: Option<&CancellationToken>,
) -> Result<LocalVisualFrameHashSummary> {
    let media_file_id = normalize_required_text(media_file_id, "media_file_id")?;
    let provider_settings = provider_settings_for_media_file(
        pool,
        &media_file_id,
        &preferences.segment_provider_settings,
        PROVIDER_LOCAL_VISUAL_RECURRING,
    )
    .await?;
    let mut summary = LocalVisualFrameHashSummary {
        media_file_id: media_file_id.clone(),
        status: "ok".to_string(),
        frame_width: LOCAL_VISUAL_FRAME_HASH_WIDTH,
        frame_height: LOCAL_VISUAL_FRAME_HASH_HEIGHT,
        fingerprint_version: LOCAL_VISUAL_FRAME_HASH_VERSION.to_string(),
        ..LocalVisualFrameHashSummary::default()
    };

    if !provider_settings_enabled(provider_settings.as_ref()) {
        summary.status = "skipped".to_string();
        summary.reason = Some("local_visual_detector_disabled".to_string());
        return Ok(summary);
    }

    let file = load_local_visual_frame_hash_media_file(pool, &media_file_id).await?;
    if file.scan_state != "ok" {
        summary.status = "skipped".to_string();
        summary.reason = Some("media_file_not_playable".to_string());
        return Ok(summary);
    }

    let fs_metadata = match tokio::fs::metadata(&file.path).await {
        Ok(metadata) => metadata,
        Err(err) => {
            summary.status = "skipped".to_string();
            summary.file_size_bytes = file.size_bytes;
            summary.reason = Some(format!("file_unavailable:{err}"));
            return Ok(summary);
        }
    };
    summary.file_size_bytes = Some(fs_metadata.len() as i64);

    let timeout_seconds = local_visual_frame_hash_timeout_seconds(provider_settings.as_ref());
    let metadata = match timeout(
        StdDuration::from_secs(timeout_seconds),
        ffprobe::probe(&file.path),
    )
    .await
    {
        Ok(Ok(metadata)) => metadata,
        Ok(Err(err)) => {
            summary.status = "skipped".to_string();
            summary.reason = Some(format!("ffprobe_failed:{err}"));
            return Ok(summary);
        }
        Err(_) => {
            summary.status = "skipped".to_string();
            summary.reason = Some("ffprobe_timeout".to_string());
            return Ok(summary);
        }
    };

    let duration_seconds = metadata
        .duration_seconds
        .map(i64::from)
        .or(file.duration_seconds)
        .filter(|duration| *duration > 0);
    summary.duration_seconds = duration_seconds;
    if !metadata_has_video_stream(&metadata) {
        summary.status = "skipped".to_string();
        summary.reason = Some("no_video_stream".to_string());
        upsert_media_file_visual_frame_hash(
            pool,
            &file,
            &metadata,
            summary.file_size_bytes,
            duration_seconds,
            None,
        )
        .await?;
        return Ok(summary);
    }

    let Some(duration_seconds) = duration_seconds else {
        summary.status = "skipped".to_string();
        summary.reason = Some("unknown_duration".to_string());
        return Ok(summary);
    };
    if duration_seconds as f64 <= LOCAL_VISUAL_CREDITS_MIN_DURATION_SECONDS {
        summary.status = "skipped".to_string();
        summary.reason = Some("duration_too_short".to_string());
        return Ok(summary);
    }

    let plan = local_visual_frame_hash_plan(duration_seconds as f64, provider_settings.as_ref());
    summary.extraction_ranges = plan.len();
    summary.frames_planned = plan.iter().map(|range| range.frames.len()).sum::<usize>();
    if summary.frames_planned == 0 {
        summary.status = "skipped".to_string();
        summary.reason = Some("no_video_frame_hash_samples".to_string());
        return Ok(summary);
    }

    let cancellation = job_id.map(|job_id| MediaSegmentJobCancellation {
        pool,
        job_id,
        shutdown,
    });
    let frames =
        match extract_local_visual_frame_hashes(&file.path, &plan, timeout_seconds, cancellation)
            .await
        {
            Ok(frames) => frames,
            Err(err) => {
                if is_media_segment_worker_shutdown_interruption(&err) {
                    return Err(err);
                }
                summary.status = "skipped".to_string();
                summary.reason = Some(format!("ffmpeg_failed:{err}"));
                return Ok(summary);
            }
        };
    summary.frames_extracted = frames.len();
    if frames.is_empty() {
        summary.status = "skipped".to_string();
        summary.reason = Some("video_frame_hash_extraction_empty".to_string());
        return Ok(summary);
    }

    let payload = local_visual_frame_hash_payload(duration_seconds, &plan, &frames);
    upsert_media_file_visual_frame_hash(
        pool,
        &file,
        &metadata,
        summary.file_size_bytes,
        Some(duration_seconds),
        Some(payload),
    )
    .await?;

    Ok(summary)
}

#[derive(Debug, Clone)]
struct LocalVisualFrameHashMediaFile {
    id: String,
    path: String,
    size_bytes: Option<i64>,
    container: Option<String>,
    video_codec: Option<String>,
    audio_codec: Option<String>,
    duration_seconds: Option<i64>,
    scan_state: String,
}

#[derive(Debug, Clone)]
struct LocalVisualFrameHashRange {
    start_seconds: f64,
    end_seconds: f64,
    step_seconds: f64,
    frames: Vec<LocalVisualFrameHashFrameSpec>,
}

#[derive(Debug, Clone)]
struct LocalVisualFrameHashFrameSpec {
    time_seconds: f64,
}

#[derive(Debug, Clone)]
struct LocalVisualFrameHashOutputFrame {
    time_seconds: f64,
    black_ratio: f64,
    text_ratio: f64,
    hash: String,
}

async fn load_local_visual_frame_hash_media_file(
    pool: &AnyPool,
    media_file_id: &str,
) -> Result<LocalVisualFrameHashMediaFile> {
    let row = sqlx::query::<sqlx::Any>(
        "SELECT
             mf.id,
             mf.path,
             mf.size_bytes,
             mf.container,
             mf.video_codec,
             mf.audio_codec,
             mf.scan_state,
             COALESCE(fp.duration_seconds, mi.runtime_seconds) AS duration_seconds
         FROM media_files mf
         JOIN media_items mi ON mi.id = mf.media_item_id
         LEFT JOIN media_file_fingerprints fp ON fp.media_file_id = mf.id
         WHERE mf.id = $1
         LIMIT 1",
    )
    .bind(media_file_id)
    .fetch_optional(pool)
    .await
    .context("loading local visual frame hash media file")?
    .context("media file not found")?;

    Ok(LocalVisualFrameHashMediaFile {
        id: row.get("id"),
        path: row.get("path"),
        size_bytes: row.try_get::<i64, _>("size_bytes").ok(),
        container: row_string(&row, "container"),
        video_codec: row_string(&row, "video_codec"),
        audio_codec: row_string(&row, "audio_codec"),
        duration_seconds: row.try_get::<i64, _>("duration_seconds").ok(),
        scan_state: row
            .try_get::<String, _>("scan_state")
            .unwrap_or_else(|_| "unknown".to_string()),
    })
}

fn metadata_has_video_stream(metadata: &ffprobe::MediaMetadata) -> bool {
    metadata
        .streams
        .iter()
        .any(|stream| stream.codec_type.as_deref() == Some("video"))
        || metadata
            .video_codec
            .as_deref()
            .is_some_and(|codec| !codec.is_empty())
}

fn local_visual_frame_hash_plan(
    duration_seconds: f64,
    settings: Option<&Value>,
) -> Vec<LocalVisualFrameHashRange> {
    if !duration_seconds.is_finite()
        || duration_seconds <= LOCAL_VISUAL_CREDITS_MIN_DURATION_SECONDS
    {
        return Vec::new();
    }

    let min_start = duration_seconds * local_visual_min_start_fraction(settings);
    let range_start = min_start
        .max(duration_seconds - LOCAL_VISUAL_FRAME_HASH_MAX_RANGE_SECONDS)
        .max(0.0);
    let range_end = duration_seconds;
    if range_end <= range_start {
        return Vec::new();
    }

    let step_seconds = local_visual_frame_hash_step_seconds(settings);
    let max_frames = local_visual_frame_hash_max_frames(settings);
    let mut frames = Vec::new();
    let mut time_seconds = range_start;
    while time_seconds <= range_end + 0.001 && frames.len() < max_frames {
        frames.push(LocalVisualFrameHashFrameSpec {
            time_seconds: round_millis(time_seconds),
        });
        time_seconds += step_seconds;
    }

    if frames.is_empty() {
        Vec::new()
    } else {
        vec![LocalVisualFrameHashRange {
            start_seconds: round_millis(range_start),
            end_seconds: round_millis(range_end),
            step_seconds,
            frames,
        }]
    }
}

async fn extract_local_visual_frame_hashes(
    path: &str,
    plan: &[LocalVisualFrameHashRange],
    timeout_seconds: u64,
    cancellation: Option<MediaSegmentJobCancellation<'_>>,
) -> Result<Vec<LocalVisualFrameHashOutputFrame>> {
    let mut output_frames = Vec::new();
    let mut failed_ranges = 0usize;

    for range in plan {
        let range_duration = range.end_seconds - range.start_seconds;
        let raw_frames = match extract_local_visual_raw_frame_range(
            path,
            range.start_seconds,
            range_duration,
            range.step_seconds,
            timeout_seconds,
            range.frames.len(),
            cancellation,
        )
        .await
        {
            Ok(raw_frames) => raw_frames,
            Err(err) => {
                failed_ranges += 1;
                tracing::warn!(
                    path,
                    start_seconds = range.start_seconds,
                    end_seconds = range.end_seconds,
                    error = %err,
                    "local visual frame hash extraction range failed"
                );
                continue;
            }
        };

        let frame_size = LOCAL_VISUAL_FRAME_HASH_WIDTH * LOCAL_VISUAL_FRAME_HASH_HEIGHT;
        for (index, frame_bytes) in raw_frames.chunks_exact(frame_size).enumerate() {
            let Some(spec) = range.frames.get(index) else {
                break;
            };
            output_frames.push(local_visual_frame_hash_for_gray_frame(
                spec.time_seconds,
                frame_bytes,
                LOCAL_VISUAL_FRAME_HASH_WIDTH,
                LOCAL_VISUAL_FRAME_HASH_HEIGHT,
            ));
        }
    }

    if output_frames.is_empty() && failed_ranges > 0 {
        bail!("all local visual frame hash extraction ranges failed");
    }

    Ok(output_frames)
}

async fn extract_local_visual_raw_frame_range(
    path: &str,
    start_seconds: f64,
    duration_seconds: f64,
    step_seconds: f64,
    timeout_seconds: u64,
    max_frames: usize,
    cancellation: Option<MediaSegmentJobCancellation<'_>>,
) -> Result<Vec<u8>> {
    let filter = format!(
        "fps=1/{step},scale={width}:{height}:force_original_aspect_ratio=decrease,pad={width}:{height}:(ow-iw)/2:(oh-ih)/2,format=gray",
        step = format_seconds(step_seconds),
        width = LOCAL_VISUAL_FRAME_HASH_WIDTH,
        height = LOCAL_VISUAL_FRAME_HASH_HEIGHT,
    );
    let mut command = Command::new("ffmpeg");
    command.kill_on_drop(true);
    let child = command
        .arg("-hide_banner")
        .arg("-nostdin")
        .arg("-v")
        .arg("error")
        .arg("-ss")
        .arg(format_seconds(start_seconds))
        .arg("-t")
        .arg(format_seconds(duration_seconds))
        .arg("-i")
        .arg(path)
        .arg("-map")
        .arg("0:v:0")
        .arg("-an")
        .arg("-sn")
        .arg("-dn")
        .arg("-vf")
        .arg(filter)
        .arg("-frames:v")
        .arg(max_frames.to_string())
        .arg("-f")
        .arg("rawvideo")
        .arg("pipe:1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("spawning ffmpeg for local visual frame hash extraction")?;

    let output = wait_for_local_ffmpeg_output(
        child,
        timeout_seconds,
        cancellation,
        "ffmpeg local visual frame hash extraction",
    )
    .await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "ffmpeg local visual frame hash extraction failed with code {:?}: {}",
            output.status.code(),
            truncate_for_error(&stderr, LOCAL_VISUAL_FRAME_HASH_FFMPEG_STDERR_LIMIT)
        );
    }

    Ok(output.stdout)
}

enum LocalFfmpegWaitOutcome {
    Completed(std::process::ExitStatus),
    TimedOut,
    Cancelled,
    Shutdown,
}

async fn wait_for_local_ffmpeg_output(
    mut child: tokio::process::Child,
    timeout_seconds: u64,
    cancellation: Option<MediaSegmentJobCancellation<'_>>,
    operation: &str,
) -> Result<Output> {
    let mut stdout = child
        .stdout
        .take()
        .with_context(|| format!("{operation} stdout was not piped"))?;
    let mut stderr = child
        .stderr
        .take()
        .with_context(|| format!("{operation} stderr was not piped"))?;
    let stdout_task = tokio::spawn(async move {
        let mut bytes = Vec::new();
        stdout.read_to_end(&mut bytes).await.map(|_| bytes)
    });
    let stderr_task = tokio::spawn(async move {
        let mut bytes = Vec::new();
        stderr.read_to_end(&mut bytes).await.map(|_| bytes)
    });

    let outcome = tokio::select! {
        status = child.wait() => {
            LocalFfmpegWaitOutcome::Completed(
                status.with_context(|| format!("waiting for {operation}"))?
            )
        }
        _ = sleep(StdDuration::from_secs(timeout_seconds)) => {
            LocalFfmpegWaitOutcome::TimedOut
        }
        cancelled = wait_for_media_segment_job_cancelled_or_pending(cancellation) => {
            cancelled?;
            LocalFfmpegWaitOutcome::Cancelled
        }
        shutdown = wait_for_media_segment_worker_shutdown(cancellation) => {
            shutdown?;
            LocalFfmpegWaitOutcome::Shutdown
        }
    };

    match outcome {
        LocalFfmpegWaitOutcome::Completed(status) => {
            let stdout = stdout_task
                .await
                .with_context(|| format!("joining {operation} stdout reader"))?
                .with_context(|| format!("reading {operation} stdout"))?;
            let stderr = stderr_task
                .await
                .with_context(|| format!("joining {operation} stderr reader"))?
                .with_context(|| format!("reading {operation} stderr"))?;
            Ok(Output {
                status,
                stdout,
                stderr,
            })
        }
        LocalFfmpegWaitOutcome::TimedOut => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            let _ = stdout_task.await;
            let _ = stderr_task.await;
            bail!("{operation} timed out");
        }
        LocalFfmpegWaitOutcome::Cancelled => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            let _ = stdout_task.await;
            let _ = stderr_task.await;
            bail!("{operation} cancelled");
        }
        LocalFfmpegWaitOutcome::Shutdown => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            let _ = stdout_task.await;
            let _ = stderr_task.await;
            bail!("{operation} interrupted by shutdown");
        }
    }
}

async fn wait_for_media_segment_job_cancelled_or_pending(
    cancellation: Option<MediaSegmentJobCancellation<'_>>,
) -> Result<()> {
    let Some(cancellation) = cancellation else {
        return std::future::pending::<Result<()>>().await;
    };

    loop {
        sleep(StdDuration::from_secs(
            MEDIA_SEGMENT_JOB_CANCELLATION_POLL_SECONDS,
        ))
        .await;
        let status = sqlx::query_scalar::<_, String>(
            "SELECT status
             FROM media_segment_jobs
             WHERE id = $1
             LIMIT 1",
        )
        .bind(cancellation.job_id)
        .fetch_optional(cancellation.pool)
        .await
        .context("checking media segment job cancellation")?;
        if status.as_deref() == Some("cancelled") {
            return Ok(());
        }
    }
}

async fn wait_for_media_segment_worker_shutdown(
    cancellation: Option<MediaSegmentJobCancellation<'_>>,
) -> Result<()> {
    let Some(shutdown) = cancellation.and_then(|cancellation| cancellation.shutdown) else {
        return std::future::pending::<Result<()>>().await;
    };
    shutdown.cancelled().await;
    Ok(())
}

fn is_media_segment_worker_shutdown_interruption(err: &anyhow::Error) -> bool {
    err.to_string().contains("interrupted by shutdown")
}

fn local_visual_frame_hash_for_gray_frame(
    time_seconds: f64,
    frame: &[u8],
    width: usize,
    height: usize,
) -> LocalVisualFrameHashOutputFrame {
    let pixel_count = frame.len().max(1);
    let black_pixels = frame
        .iter()
        .filter(|value| **value <= LOCAL_VISUAL_FRAME_BLACK_LUMA_THRESHOLD)
        .count();
    let mut edge_count = 0usize;
    let mut edge_total = 0usize;
    if width > 1 && height > 1 && frame.len() >= width * height {
        for y in 0..height {
            for x in 0..width {
                let index = y * width + x;
                let value = frame[index];
                if x + 1 < width {
                    edge_total += 1;
                    if value.abs_diff(frame[index + 1])
                        >= LOCAL_VISUAL_FRAME_EDGE_LUMA_DELTA_THRESHOLD
                    {
                        edge_count += 1;
                    }
                }
                if y + 1 < height {
                    edge_total += 1;
                    if value.abs_diff(frame[index + width])
                        >= LOCAL_VISUAL_FRAME_EDGE_LUMA_DELTA_THRESHOLD
                    {
                        edge_count += 1;
                    }
                }
            }
        }
    }
    let black_ratio = black_pixels as f64 / pixel_count as f64;
    let text_ratio = if edge_total == 0 {
        0.0
    } else {
        edge_count as f64 / edge_total as f64
    };
    let mut hash_input = Vec::with_capacity(frame.len() + 32);
    hash_input.extend_from_slice(LOCAL_VISUAL_FRAME_HASH_VERSION.as_bytes());
    hash_input.extend_from_slice(&(width as u32).to_le_bytes());
    hash_input.extend_from_slice(&(height as u32).to_le_bytes());
    hash_input.extend_from_slice(frame);

    LocalVisualFrameHashOutputFrame {
        time_seconds: round_millis(time_seconds),
        black_ratio,
        text_ratio,
        hash: format!("lvf1:{}", blake3::hash(&hash_input).to_hex()),
    }
}

fn local_visual_frame_hash_payload(
    duration_seconds: i64,
    plan: &[LocalVisualFrameHashRange],
    frames: &[LocalVisualFrameHashOutputFrame],
) -> Value {
    json!({
        "version": LOCAL_VISUAL_FRAME_HASH_VERSION,
        "detector": PROVIDER_LOCAL_VISUAL_RECURRING,
        "duration_seconds": duration_seconds,
        "frame_width": LOCAL_VISUAL_FRAME_HASH_WIDTH,
        "frame_height": LOCAL_VISUAL_FRAME_HASH_HEIGHT,
        "extraction_ranges": plan.iter().map(|range| {
            json!({
                "start_seconds": range.start_seconds,
                "end_seconds": range.end_seconds,
                "step_seconds": range.step_seconds,
                "frames_planned": range.frames.len(),
            })
        }).collect::<Vec<_>>(),
        "frames": frames.iter().map(|frame| {
            json!({
                "time_seconds": frame.time_seconds,
                "black_ratio": frame.black_ratio,
                "text_ratio": frame.text_ratio,
                "hash": frame.hash,
            })
        }).collect::<Vec<_>>()
    })
}

async fn upsert_media_file_visual_frame_hash(
    pool: &AnyPool,
    file: &LocalVisualFrameHashMediaFile,
    metadata: &ffprobe::MediaMetadata,
    file_size_bytes: Option<i64>,
    duration_seconds: Option<i64>,
    video_frame_hash: Option<Value>,
) -> Result<()> {
    let container = metadata
        .container
        .clone()
        .or_else(|| file.container.clone());
    let video_codec = metadata
        .video_codec
        .clone()
        .or_else(|| file.video_codec.clone());
    let audio_codec = metadata
        .audio_codec
        .clone()
        .or_else(|| file.audio_codec.clone());
    let video_frame_hash_json = video_frame_hash.map(|value| value.to_string());

    sqlx::query::<sqlx::Any>(
        "INSERT INTO media_file_fingerprints
            (media_file_id, duration_seconds, file_size_bytes, container, video_codec,
             audio_codec, video_frame_hash_json, fingerprint_version, computed_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP)
         ON CONFLICT(media_file_id) DO UPDATE SET
             duration_seconds = excluded.duration_seconds,
             file_size_bytes = excluded.file_size_bytes,
             container = excluded.container,
             video_codec = excluded.video_codec,
             audio_codec = excluded.audio_codec,
             video_frame_hash_json = COALESCE(excluded.video_frame_hash_json, media_file_fingerprints.video_frame_hash_json),
             fingerprint_version = excluded.fingerprint_version,
             computed_at = CURRENT_TIMESTAMP",
    )
    .bind(&file.id)
    .bind(duration_seconds)
    .bind(file_size_bytes.or(file.size_bytes))
    .bind(container)
    .bind(video_codec)
    .bind(audio_codec)
    .bind(video_frame_hash_json)
    .bind(LOCAL_VISUAL_FRAME_HASH_VERSION)
    .execute(pool)
    .await
    .context("upserting local visual media file fingerprint")?;

    Ok(())
}

fn local_visual_frame_hash_timeout_seconds(settings: Option<&Value>) -> u64 {
    settings
        .and_then(|value| {
            value
                .get("frame_hash_timeout_seconds")
                .or_else(|| value.get("frameHashTimeoutSeconds"))
        })
        .and_then(Value::as_u64)
        .filter(|value| (5..=900).contains(value))
        .unwrap_or(LOCAL_VISUAL_FRAME_HASH_TIMEOUT_SECONDS)
}

fn local_visual_frame_hash_step_seconds(settings: Option<&Value>) -> f64 {
    settings
        .and_then(|value| {
            value
                .get("frame_hash_step_seconds")
                .or_else(|| value.get("frameHashStepSeconds"))
        })
        .and_then(Value::as_f64)
        .filter(|value| (5.0..=120.0).contains(value))
        .unwrap_or(LOCAL_VISUAL_FRAME_HASH_STEP_SECONDS)
}

fn local_visual_frame_hash_max_frames(settings: Option<&Value>) -> usize {
    settings
        .and_then(|value| {
            value
                .get("frame_hash_max_frames")
                .or_else(|| value.get("frameHashMaxFrames"))
        })
        .and_then(Value::as_u64)
        .filter(|value| (10..=500).contains(value))
        .map(|value| value as usize)
        .unwrap_or(LOCAL_VISUAL_FRAME_HASH_MAX_FRAMES_PER_FILE)
}

fn round_millis(value: f64) -> f64 {
    (value * 1000.0).round() / 1000.0
}

fn format_seconds(value: f64) -> String {
    format!("{:.3}", value.max(0.0))
}

fn truncate_for_error(value: &str, max_chars: usize) -> String {
    let value = value.trim();
    if value.chars().count() <= max_chars {
        value.to_string()
    } else {
        value.chars().take(max_chars).collect()
    }
}

async fn enqueue_due_local_audio_detector_jobs(
    pool: &AnyPool,
    preferences: &PlaybackInteractionPreferences,
    batch_limit: usize,
) -> Result<MediaSegmentProviderEnqueueSummary> {
    let mut summary = MediaSegmentProviderEnqueueSummary::default();
    let provider_settings = provider_settings_for(
        &preferences.segment_provider_settings,
        PROVIDER_LOCAL_AUDIO_RECURRING,
    );
    if !provider_settings_enabled(provider_settings.as_ref()) {
        return Ok(summary);
    }
    summary.providers_seen = 1;
    let season_ids = due_local_audio_detector_seasons(pool, batch_limit.max(1)).await?;
    summary.files_seen = season_ids.len();
    for season_id in season_ids {
        match enqueue_local_audio_recurring_detector_job(pool, &season_id, 200).await {
            Ok(_) => summary.jobs_queued += 1,
            Err(err) => {
                summary.jobs_failed += 1;
                tracing::warn!(
                    provider_kind = PROVIDER_LOCAL_AUDIO_RECURRING,
                    season_id,
                    error = %err,
                    "failed to enqueue local audio detector job"
                );
            }
        }
    }
    Ok(summary)
}

async fn due_local_audio_detector_seasons(
    pool: &AnyPool,
    batch_limit: usize,
) -> Result<Vec<String>> {
    let rows = sqlx::query::<sqlx::Any>(
        "SELECT e.season_id AS season_id
         FROM episodes e
         JOIN episode_files ef ON ef.episode_id = e.id
         JOIN media_files mf ON mf.id = ef.media_file_id
         JOIN media_file_fingerprints fp ON fp.media_file_id = mf.id
         WHERE e.season_id IS NOT NULL
           AND mf.scan_state = 'ok'
           AND NOT EXISTS (
               SELECT 1 FROM media_interaction_library_provider_settings lps
               WHERE lps.source_config_id = mf.source_config_id
                 AND lps.provider_kind = 'local_audio_recurring'
                 AND lps.enabled = FALSE
           )
           AND fp.audio_fingerprint_json IS NOT NULL
           AND TRIM(fp.audio_fingerprint_json) <> ''
           AND NOT EXISTS (
               SELECT 1 FROM media_segment_jobs j
               WHERE j.scope_type = 'season'
                 AND j.scope_id = e.season_id
                 AND j.provider_kind = 'local_audio_recurring'
                 AND j.status IN ('queued', 'running', 'succeeded')
           )
         GROUP BY e.season_id
         HAVING COUNT(DISTINCT mf.id) >= 2
         ORDER BY MIN(e.season_number) ASC,
                  MIN(COALESCE(e.absolute_episode_number, e.episode_number)) ASC
         LIMIT $1",
    )
    .bind(batch_limit as i64)
    .fetch_all(pool)
    .await
    .context("listing due local audio detector seasons")?;

    Ok(rows.iter().map(|row| row.get("season_id")).collect())
}

async fn run_local_audio_recurring_detector_for_season(
    pool: &AnyPool,
    season_id: &str,
    preferences: &PlaybackInteractionPreferences,
) -> Result<LocalAudioDetectorSummary> {
    let season_id = normalize_required_text(season_id, "season_id")?;
    let provider_settings = provider_settings_for_first_enabled_season_file(
        pool,
        &season_id,
        &preferences.segment_provider_settings,
        PROVIDER_LOCAL_AUDIO_RECURRING,
    )
    .await?;
    if !provider_settings_enabled(provider_settings.as_ref()) {
        return Ok(LocalAudioDetectorSummary {
            season_id,
            status: "skipped".to_string(),
            reason: Some("local_audio_detector_disabled".to_string()),
            ..LocalAudioDetectorSummary::default()
        });
    }

    let files = load_season_audio_fingerprints(pool, &season_id).await?;
    let files_with_fingerprints = files.iter().filter(|file| !file.windows.is_empty()).count();
    let mut summary = LocalAudioDetectorSummary {
        season_id: season_id.clone(),
        status: "ok".to_string(),
        files_seen: files.len(),
        files_with_fingerprints,
        ..LocalAudioDetectorSummary::default()
    };

    if files_with_fingerprints < local_audio_min_season_files(provider_settings.as_ref()) {
        summary.status = "skipped".to_string();
        summary.reason = Some("insufficient_fingerprinted_files".to_string());
        return Ok(summary);
    }

    let detected = detect_recurring_audio_segments(&files, provider_settings.as_ref());
    summary.repeated_groups = detected.repeated_groups;
    for candidate in detected.candidates {
        let outcome = submit_segment_candidate(
            pool,
            SegmentCandidateInput {
                media_file_id: candidate.media_file_id,
                item_type: Some("episode".to_string()),
                item_id: Some(candidate.episode_id),
                segment_type: candidate.segment_type,
                start_seconds: candidate.start_seconds,
                end_seconds: candidate.end_seconds,
                provider_kind: PROVIDER_LOCAL_AUDIO_RECURRING.to_string(),
                provider_id: LOCAL_AUDIO_DETECTOR_VERSION.to_string(),
                provider_version: Some(LOCAL_AUDIO_DETECTOR_VERSION.to_string()),
                confidence: candidate.confidence,
                identity_strength: "file_fingerprint".to_string(),
                source_payload: Some(json!({
                    "label": candidate.label,
                    "hash": candidate.hash,
                    "repeat_count": candidate.repeat_count,
                    "season_id": season_id,
                    "detector_version": LOCAL_AUDIO_DETECTOR_VERSION,
                    "detector": "local_audio_recurring",
                })),
            },
        )
        .await?;
        summary.candidates_submitted += 1;
        if outcome.candidate.validation_state == "accepted" {
            summary.candidates_accepted += 1;
        } else {
            summary.candidates_rejected += 1;
        }
    }

    summary.active_segments =
        count_active_local_audio_segments_for_season(pool, &season_id).await? as usize;
    Ok(summary)
}

#[derive(Debug, Clone)]
struct SeasonAudioFingerprintFile {
    windows: Vec<AudioFingerprintWindow>,
}

#[derive(Debug, Clone)]
struct AudioFingerprintWindow {
    media_file_id: String,
    episode_id: String,
    start_seconds: f64,
    end_seconds: f64,
    hash: String,
    duration_seconds: Option<f64>,
}

#[derive(Debug, Clone)]
struct LocalAudioDetectedSegment {
    media_file_id: String,
    episode_id: String,
    segment_type: String,
    start_seconds: f64,
    end_seconds: f64,
    hash: String,
    repeat_count: usize,
    confidence: f64,
    label: String,
}

#[derive(Debug, Default)]
struct LocalAudioDetectionResult {
    repeated_groups: usize,
    candidates: Vec<LocalAudioDetectedSegment>,
}

async fn load_season_audio_fingerprints(
    pool: &AnyPool,
    season_id: &str,
) -> Result<Vec<SeasonAudioFingerprintFile>> {
    let rows = sqlx::query::<sqlx::Any>(
        "SELECT
             mf.id AS media_file_id,
             e.id AS episode_id,
             CAST(COALESCE(fp.duration_seconds, e.runtime_seconds, 0) AS REAL) AS duration_seconds,
             fp.audio_fingerprint_json AS audio_fingerprint_json
         FROM episodes e
         JOIN episode_files ef ON ef.episode_id = e.id
         JOIN media_files mf ON mf.id = ef.media_file_id
         LEFT JOIN media_file_fingerprints fp ON fp.media_file_id = mf.id
         WHERE e.season_id = $1
           AND mf.scan_state = 'ok'
           AND NOT EXISTS (
               SELECT 1 FROM media_interaction_library_provider_settings lps
               WHERE lps.source_config_id = mf.source_config_id
                 AND lps.provider_kind = 'local_audio_recurring'
                 AND lps.enabled = FALSE
           )
         ORDER BY e.season_number ASC,
                  COALESCE(e.absolute_episode_number, e.episode_number) ASC,
                  e.episode_number ASC,
                  mf.id ASC",
    )
    .bind(season_id)
    .fetch_all(pool)
    .await
    .context("loading season audio fingerprints")?;

    let mut files = Vec::with_capacity(rows.len());
    for row in rows {
        let media_file_id: String = row.get("media_file_id");
        let episode_id: String = row.get("episode_id");
        let duration_seconds = row
            .try_get::<f64, _>("duration_seconds")
            .ok()
            .filter(|value| *value > 0.0);
        let windows = row
            .try_get::<String, _>("audio_fingerprint_json")
            .ok()
            .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
            .map(|value| {
                audio_fingerprint_windows_from_value(
                    &media_file_id,
                    &episode_id,
                    duration_seconds,
                    &value,
                )
            })
            .unwrap_or_default();
        files.push(SeasonAudioFingerprintFile { windows });
    }

    Ok(files)
}

fn audio_fingerprint_windows_from_value(
    media_file_id: &str,
    episode_id: &str,
    duration_seconds: Option<f64>,
    value: &Value,
) -> Vec<AudioFingerprintWindow> {
    let windows = value
        .get("windows")
        .or_else(|| value.get("fingerprints"))
        .or_else(|| value.get("segments"))
        .and_then(Value::as_array)
        .cloned()
        .or_else(|| value.as_array().cloned())
        .unwrap_or_default();

    windows
        .iter()
        .filter_map(|window| {
            let hash = window
                .get("hash")
                .or_else(|| window.get("fingerprint"))
                .or_else(|| window.get("signature"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|hash| hash.len() >= 3)?;
            let start_seconds = provider_seconds(
                window,
                &["start_seconds", "startSeconds", "start", "start_time"],
            )?;
            let end_seconds =
                provider_seconds(window, &["end_seconds", "endSeconds", "end", "end_time"])
                    .or_else(|| {
                        provider_seconds(
                            window,
                            &["duration_seconds", "durationSeconds", "duration"],
                        )
                        .map(|duration| start_seconds + duration)
                    })?;
            if !start_seconds.is_finite()
                || !end_seconds.is_finite()
                || start_seconds < 0.0
                || end_seconds <= start_seconds
            {
                return None;
            }
            Some(AudioFingerprintWindow {
                media_file_id: media_file_id.to_string(),
                episode_id: episode_id.to_string(),
                start_seconds,
                end_seconds,
                hash: hash.to_ascii_lowercase(),
                duration_seconds,
            })
        })
        .collect()
}

fn detect_recurring_audio_segments(
    files: &[SeasonAudioFingerprintFile],
    settings: Option<&Value>,
) -> LocalAudioDetectionResult {
    let min_repeat = local_audio_min_repeat_count(settings);
    let mut by_hash: BTreeMap<String, Vec<AudioFingerprintWindow>> = BTreeMap::new();
    for file in files {
        for window in &file.windows {
            by_hash
                .entry(window.hash.clone())
                .or_default()
                .push(window.clone());
        }
    }

    let mut result = LocalAudioDetectionResult::default();
    let mut best_by_file_type: BTreeMap<(String, String), LocalAudioDetectedSegment> =
        BTreeMap::new();
    for (hash, windows) in by_hash {
        let distinct_files = distinct_media_file_count(&windows);
        if distinct_files < min_repeat {
            continue;
        }
        let classified = windows
            .into_iter()
            .filter_map(|window| classify_audio_window(window, &hash, distinct_files, files.len()))
            .collect::<Vec<_>>();
        if classified.is_empty() {
            continue;
        }
        result.repeated_groups += 1;
        for segment in classified {
            let key = (segment.media_file_id.clone(), segment.segment_type.clone());
            let replace = best_by_file_type
                .get(&key)
                .map(|current| segment.confidence > current.confidence)
                .unwrap_or(true);
            if replace {
                best_by_file_type.insert(key, segment);
            }
        }
    }

    result.candidates = best_by_file_type.into_values().collect();
    result
}

fn classify_audio_window(
    window: AudioFingerprintWindow,
    hash: &str,
    repeat_count: usize,
    season_file_count: usize,
) -> Option<LocalAudioDetectedSegment> {
    let window_duration = window.end_seconds - window.start_seconds;
    if !(20.0..=180.0).contains(&window_duration) {
        return None;
    }
    let duration = window.duration_seconds.unwrap_or(0.0);
    let intro_limit = if duration > 0.0 {
        (duration * EARLY_WINDOW_FRACTION).min(LOCAL_AUDIO_DETECTOR_MAX_INTRO_START_SECONDS)
    } else {
        LOCAL_AUDIO_DETECTOR_MAX_INTRO_START_SECONDS
    };
    let (segment_type, label) = if window.start_seconds <= intro_limit {
        ("intro".to_string(), "Local audio intro".to_string())
    } else if duration > 0.0 && window.start_seconds >= duration * LATE_WINDOW_FRACTION {
        ("outro".to_string(), "Local audio outro".to_string())
    } else {
        return None;
    };

    Some(LocalAudioDetectedSegment {
        media_file_id: window.media_file_id,
        episode_id: window.episode_id,
        segment_type,
        start_seconds: window.start_seconds,
        end_seconds: window.end_seconds,
        hash: hash.to_string(),
        repeat_count,
        confidence: local_audio_confidence(repeat_count, season_file_count),
        label,
    })
}

fn distinct_media_file_count(windows: &[AudioFingerprintWindow]) -> usize {
    let mut ids = Vec::<&str>::new();
    for window in windows {
        if !ids.contains(&window.media_file_id.as_str()) {
            ids.push(window.media_file_id.as_str());
        }
    }
    ids.len()
}

fn local_audio_confidence(repeat_count: usize, season_file_count: usize) -> f64 {
    let coverage = if season_file_count > 0 {
        repeat_count as f64 / season_file_count as f64
    } else {
        0.0
    };
    (LOCAL_AUDIO_DETECTOR_MIN_CONFIDENCE + coverage.min(1.0) * 0.12).min(0.97)
}

async fn count_active_local_audio_segments_for_season(
    pool: &AnyPool,
    season_id: &str,
) -> Result<i64> {
    sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)
         FROM media_segments ms
         JOIN media_segment_candidates c ON c.id = ms.canonical_candidate_id
         JOIN episode_files ef ON ef.media_file_id = ms.media_file_id
         JOIN episodes e ON e.id = ef.episode_id
         WHERE e.season_id = $1
           AND ms.status = 'active'
           AND c.provider_kind = 'local_audio_recurring'",
    )
    .bind(season_id)
    .fetch_one(pool)
    .await
    .context("counting active local audio detector segments")
}

async fn enqueue_due_local_visual_detector_jobs(
    pool: &AnyPool,
    preferences: &PlaybackInteractionPreferences,
    batch_limit: usize,
) -> Result<MediaSegmentProviderEnqueueSummary> {
    let mut summary = MediaSegmentProviderEnqueueSummary::default();
    let provider_settings = provider_settings_for(
        &preferences.segment_provider_settings,
        PROVIDER_LOCAL_VISUAL_RECURRING,
    );
    if !provider_settings_enabled(provider_settings.as_ref()) {
        return Ok(summary);
    }

    summary.providers_seen = 1;
    let media_file_ids = due_local_visual_detector_media_files(pool, batch_limit.max(1)).await?;
    summary.files_seen = media_file_ids.len();
    for media_file_id in media_file_ids {
        match enqueue_local_visual_credits_detector_job(pool, &media_file_id, 220).await {
            Ok(_) => summary.jobs_queued += 1,
            Err(err) => {
                summary.jobs_failed += 1;
                tracing::warn!(
                    provider_kind = PROVIDER_LOCAL_VISUAL_RECURRING,
                    media_file_id,
                    error = %err,
                    "failed to enqueue local visual detector job"
                );
            }
        }
    }
    Ok(summary)
}

async fn due_local_visual_detector_media_files(
    pool: &AnyPool,
    batch_limit: usize,
) -> Result<Vec<String>> {
    let rows = sqlx::query::<sqlx::Any>(
        "SELECT DISTINCT mf.id AS media_file_id
         FROM media_files mf
         JOIN media_file_fingerprints fp ON fp.media_file_id = mf.id
         WHERE mf.scan_state = 'ok'
           AND NOT EXISTS (
               SELECT 1 FROM media_interaction_library_provider_settings lps
               WHERE lps.source_config_id = mf.source_config_id
                 AND lps.provider_kind = 'local_visual_recurring'
                 AND lps.enabled = FALSE
           )
           AND fp.video_frame_hash_json IS NOT NULL
           AND TRIM(fp.video_frame_hash_json) <> ''
           AND (
               EXISTS (
                   SELECT 1 FROM movie_files mfv
                   WHERE mfv.media_file_id = mf.id
               )
               OR EXISTS (
                   SELECT 1 FROM episode_files ef
                   WHERE ef.media_file_id = mf.id
               )
           )
           AND NOT EXISTS (
               SELECT 1 FROM media_segment_jobs j
               WHERE j.job_type = 'local_detector'
                 AND j.scope_type = 'media_file'
                 AND j.scope_id = mf.id
                 AND j.provider_kind = 'local_visual_recurring'
                 AND j.status IN ('queued', 'running', 'succeeded')
           )
         ORDER BY fp.computed_at ASC, mf.id ASC
         LIMIT $1",
    )
    .bind(batch_limit as i64)
    .fetch_all(pool)
    .await
    .context("listing due local visual detector files")?;

    Ok(rows.iter().map(|row| row.get("media_file_id")).collect())
}

async fn run_local_visual_credits_detector_for_media_file(
    pool: &AnyPool,
    media_file_id: &str,
    preferences: &PlaybackInteractionPreferences,
) -> Result<LocalVisualDetectorSummary> {
    let media_file_id = normalize_required_text(media_file_id, "media_file_id")?;
    let provider_settings = provider_settings_for_media_file(
        pool,
        &media_file_id,
        &preferences.segment_provider_settings,
        PROVIDER_LOCAL_VISUAL_RECURRING,
    )
    .await?;
    let mut summary = LocalVisualDetectorSummary {
        media_file_id: media_file_id.clone(),
        status: "ok".to_string(),
        detector_version: LOCAL_VISUAL_DETECTOR_VERSION.to_string(),
        ..LocalVisualDetectorSummary::default()
    };

    if !provider_settings_enabled(provider_settings.as_ref()) {
        summary.status = "skipped".to_string();
        summary.reason = Some("local_visual_detector_disabled".to_string());
        return Ok(summary);
    }

    let context = load_provider_media_context(pool, &media_file_id).await?;
    let Some(duration_seconds) = context.duration_seconds.filter(|value| *value > 0.0) else {
        summary.status = "skipped".to_string();
        summary.reason = Some("unknown_duration".to_string());
        return Ok(summary);
    };
    summary.duration_seconds = Some(duration_seconds.round() as i64);
    if duration_seconds < LOCAL_VISUAL_CREDITS_MIN_DURATION_SECONDS {
        summary.status = "skipped".to_string();
        summary.reason = Some("duration_too_short".to_string());
        return Ok(summary);
    }

    let Some(frame_hash_payload) = load_video_frame_hash_payload(pool, &media_file_id).await?
    else {
        summary.status = "skipped".to_string();
        summary.reason = Some("missing_video_frame_hash".to_string());
        return Ok(summary);
    };
    let frames = visual_frame_samples_from_value(&frame_hash_payload);
    summary.frames_seen = frames.len();
    if frames.is_empty() {
        summary.status = "skipped".to_string();
        summary.reason = Some("no_video_frame_samples".to_string());
        return Ok(summary);
    }

    let detected =
        detect_visual_credits_segment(&frames, duration_seconds, provider_settings.as_ref());
    summary.credits_like_frames = detected.credits_like_frames;
    summary.sustained_runs = detected.sustained_runs;
    if detected.candidates.is_empty() {
        summary.reason = Some("no_sustained_credits_run".to_string());
        return Ok(summary);
    }

    for candidate in detected.candidates {
        let outcome = submit_segment_candidate(
            pool,
            SegmentCandidateInput {
                media_file_id: media_file_id.clone(),
                item_type: Some(context.item_type.clone()),
                item_id: Some(context.item_id.clone()),
                segment_type: "credits".to_string(),
                start_seconds: candidate.start_seconds,
                end_seconds: candidate.end_seconds,
                provider_kind: PROVIDER_LOCAL_VISUAL_RECURRING.to_string(),
                provider_id: LOCAL_VISUAL_DETECTOR_VERSION.to_string(),
                provider_version: Some(LOCAL_VISUAL_DETECTOR_VERSION.to_string()),
                confidence: candidate.confidence,
                identity_strength: "file_fingerprint".to_string(),
                source_payload: Some(json!({
                    "label": "Local visual credits",
                    "detector": PROVIDER_LOCAL_VISUAL_RECURRING,
                    "detector_version": LOCAL_VISUAL_DETECTOR_VERSION,
                    "frame_count": candidate.frame_count,
                    "evidence_span_seconds": candidate.evidence_span_seconds,
                    "first_frame_seconds": candidate.first_frame_seconds,
                    "last_frame_seconds": candidate.last_frame_seconds,
                    "average_black_ratio": candidate.average_black_ratio,
                    "average_text_ratio": candidate.average_text_ratio,
                    "segment_end_reason": candidate.end_reason,
                    "post_credit_scene_start_seconds": candidate.post_credit_scene_start_seconds,
                })),
            },
        )
        .await?;
        summary.candidates_submitted += 1;
        if outcome.candidate.validation_state == "accepted" {
            summary.candidates_accepted += 1;
        } else {
            summary.candidates_rejected += 1;
            summary.reason = outcome.candidate.validation_reason.clone();
        }
    }
    summary.active_segments =
        count_active_local_visual_segments_for_file(pool, &media_file_id).await? as usize;

    Ok(summary)
}

#[derive(Debug, Clone)]
struct VisualFrameSample {
    time_seconds: f64,
    black_ratio: f64,
    text_ratio: f64,
    credit_like: bool,
}

#[derive(Debug, Clone)]
struct LocalVisualDetectedSegment {
    start_seconds: f64,
    end_seconds: f64,
    confidence: f64,
    frame_count: usize,
    evidence_span_seconds: f64,
    first_frame_seconds: f64,
    last_frame_seconds: f64,
    average_black_ratio: f64,
    average_text_ratio: f64,
    end_reason: &'static str,
    post_credit_scene_start_seconds: Option<f64>,
}

#[derive(Debug, Default)]
struct LocalVisualDetectionResult {
    credits_like_frames: usize,
    sustained_runs: usize,
    candidates: Vec<LocalVisualDetectedSegment>,
}

async fn load_video_frame_hash_payload(
    pool: &AnyPool,
    media_file_id: &str,
) -> Result<Option<Value>> {
    let raw = sqlx::query_scalar::<_, String>(
        "SELECT video_frame_hash_json
         FROM media_file_fingerprints
         WHERE media_file_id = $1
           AND video_frame_hash_json IS NOT NULL
           AND TRIM(video_frame_hash_json) <> ''
         LIMIT 1",
    )
    .bind(media_file_id)
    .fetch_optional(pool)
    .await
    .context("loading local visual frame hash payload")?;

    raw.map(|raw| serde_json::from_str::<Value>(&raw).context("decoding video frame hash payload"))
        .transpose()
}

fn visual_frame_samples_from_value(value: &Value) -> Vec<VisualFrameSample> {
    let frames = value
        .get("frames")
        .or_else(|| value.get("frame_hashes"))
        .or_else(|| value.get("frameHashes"))
        .or_else(|| value.get("video_frames"))
        .or_else(|| value.get("videoFrames"))
        .or_else(|| value.get("samples"))
        .and_then(Value::as_array)
        .cloned()
        .or_else(|| value.as_array().cloned())
        .unwrap_or_default();

    let mut samples = frames
        .iter()
        .filter_map(|frame| {
            let time_seconds = provider_seconds(
                frame,
                &[
                    "time_seconds",
                    "timeSeconds",
                    "timestamp_seconds",
                    "timestampSeconds",
                    "timestamp",
                    "pts_seconds",
                    "ptsSeconds",
                    "start_seconds",
                    "startSeconds",
                    "start",
                    "time",
                ],
            )?;
            if !time_seconds.is_finite() || time_seconds < 0.0 {
                return None;
            }

            Some(VisualFrameSample {
                time_seconds,
                black_ratio: visual_ratio(
                    frame,
                    &[
                        "black_ratio",
                        "blackRatio",
                        "dark_ratio",
                        "darkRatio",
                        "luma_black_ratio",
                        "lumaBlackRatio",
                        "black",
                        "dark",
                    ],
                )
                .unwrap_or(0.0),
                text_ratio: visual_ratio(
                    frame,
                    &[
                        "text_ratio",
                        "textRatio",
                        "text_density",
                        "textDensity",
                        "edge_ratio",
                        "edgeRatio",
                        "ocr_text_ratio",
                        "ocrTextRatio",
                        "text",
                    ],
                )
                .unwrap_or(0.0),
                credit_like: visual_bool(
                    frame,
                    &[
                        "credit_like",
                        "creditLike",
                        "credits_like",
                        "creditsLike",
                        "is_credit",
                        "isCredit",
                        "is_credits",
                        "isCredits",
                    ],
                ),
            })
        })
        .collect::<Vec<_>>();

    samples.sort_by(|left, right| {
        left.time_seconds
            .partial_cmp(&right.time_seconds)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    samples
}

fn detect_visual_credits_segment(
    frames: &[VisualFrameSample],
    duration_seconds: f64,
    settings: Option<&Value>,
) -> LocalVisualDetectionResult {
    if !duration_seconds.is_finite() || duration_seconds <= 0.0 {
        return LocalVisualDetectionResult::default();
    }

    let min_start_seconds = duration_seconds * local_visual_min_start_fraction(settings);
    let late_frames = frames
        .iter()
        .filter(|frame| {
            frame.time_seconds >= min_start_seconds
                && frame.time_seconds <= duration_seconds + DURATION_TOLERANCE_SECONDS
        })
        .map(|frame| (frame, visual_frame_looks_like_credits(frame, settings)))
        .collect::<Vec<_>>();
    let credits_like_count = late_frames
        .iter()
        .filter(|(_, credit_like)| *credit_like)
        .count();
    let mut result = LocalVisualDetectionResult {
        credits_like_frames: credits_like_count,
        ..LocalVisualDetectionResult::default()
    };
    if credits_like_count == 0 {
        return result;
    }

    let mut runs = Vec::<Vec<&VisualFrameSample>>::new();
    let mut current = Vec::<&VisualFrameSample>::new();
    let mut current_last_late_index = None::<usize>;
    let max_gap = local_visual_max_frame_gap_seconds(settings);
    for (index, (frame, credit_like)) in late_frames.iter().enumerate() {
        if !*credit_like {
            continue;
        }

        let should_split = current_last_late_index
            .map(|previous_index| {
                let previous = late_frames[previous_index].0;
                frame.time_seconds - previous.time_seconds > max_gap
                    || post_credit_scene_start_in_frames(
                        &late_frames[previous_index + 1..index],
                        settings,
                    )
                    .is_some()
            })
            .unwrap_or(false);
        if should_split && !current.is_empty() {
            runs.push(current);
            current = Vec::new();
        }

        current.push(*frame);
        current_last_late_index = Some(index);
    }
    if !current.is_empty() {
        runs.push(current);
    }

    let min_frame_count = local_visual_min_frame_count(settings);
    let min_span_seconds = local_visual_min_span_seconds(settings);
    for run in runs {
        let Some(first) = run.first() else {
            continue;
        };
        let Some(last) = run.last() else {
            continue;
        };
        let evidence_span_seconds = last.time_seconds - first.time_seconds;
        if run.len() < min_frame_count || evidence_span_seconds < min_span_seconds {
            continue;
        }
        if duration_seconds - first.time_seconds < minimum_segment_duration("credits") {
            continue;
        }

        let post_credit_scene_start_seconds =
            post_credit_scene_start_after(&late_frames, last.time_seconds, settings);
        let end_seconds = post_credit_scene_start_seconds.unwrap_or(duration_seconds);
        if end_seconds - first.time_seconds < minimum_segment_duration("credits") {
            continue;
        }

        result.sustained_runs += 1;
        let average_black_ratio =
            run.iter().map(|frame| frame.black_ratio).sum::<f64>() / run.len() as f64;
        let average_text_ratio =
            run.iter().map(|frame| frame.text_ratio).sum::<f64>() / run.len() as f64;
        result.candidates.push(LocalVisualDetectedSegment {
            start_seconds: round_millis(first.time_seconds),
            end_seconds: round_millis(end_seconds),
            confidence: local_visual_confidence(
                run.len(),
                evidence_span_seconds,
                average_black_ratio,
                average_text_ratio,
            ),
            frame_count: run.len(),
            evidence_span_seconds: round_millis(evidence_span_seconds),
            first_frame_seconds: round_millis(first.time_seconds),
            last_frame_seconds: round_millis(last.time_seconds),
            average_black_ratio,
            average_text_ratio,
            end_reason: if post_credit_scene_start_seconds.is_some() {
                "post_credit_scene_detected"
            } else {
                "media_end"
            },
            post_credit_scene_start_seconds: post_credit_scene_start_seconds.map(round_millis),
        });
    }

    result.candidates.sort_by(|left, right| {
        left.start_seconds
            .partial_cmp(&right.start_seconds)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    result
}

fn visual_frame_looks_like_credits(frame: &VisualFrameSample, settings: Option<&Value>) -> bool {
    if frame.credit_like {
        return true;
    }
    frame.black_ratio >= local_visual_black_ratio_threshold(settings)
        && frame.text_ratio >= local_visual_text_ratio_threshold(settings)
}

fn post_credit_scene_start_after(
    late_frames: &[(&VisualFrameSample, bool)],
    last_credit_frame_seconds: f64,
    settings: Option<&Value>,
) -> Option<f64> {
    let start_index = late_frames
        .iter()
        .position(|(frame, _)| frame.time_seconds > last_credit_frame_seconds)?;
    let end_index = late_frames[start_index..]
        .iter()
        .position(|(_, credit_like)| *credit_like)
        .map(|offset| start_index + offset)
        .unwrap_or(late_frames.len());

    post_credit_scene_start_in_frames(&late_frames[start_index..end_index], settings)
}

fn post_credit_scene_start_in_frames(
    frames: &[(&VisualFrameSample, bool)],
    settings: Option<&Value>,
) -> Option<f64> {
    let non_credit_frames = frames
        .iter()
        .filter_map(|(frame, credit_like)| (!*credit_like).then_some(*frame))
        .collect::<Vec<_>>();
    if non_credit_frames.len() < local_visual_post_credit_scene_min_frames(settings) {
        return None;
    }

    let first = non_credit_frames.first()?;
    let last = non_credit_frames.last()?;
    let span_seconds = last.time_seconds - first.time_seconds;
    if span_seconds < local_visual_post_credit_scene_min_span_seconds(settings) {
        return None;
    }

    Some(first.time_seconds)
}

fn visual_ratio(value: &Value, keys: &[&str]) -> Option<f64> {
    for key in keys {
        let Some(raw) = value.get(*key) else {
            continue;
        };
        let number = if let Some(number) = raw.as_f64() {
            number
        } else if let Some(text) = raw.as_str().map(str::trim).filter(|text| !text.is_empty()) {
            match text.parse::<f64>() {
                Ok(number) => number,
                Err(_) => continue,
            }
        } else {
            continue;
        };
        if let Some(ratio) = normalize_visual_ratio(number) {
            return Some(ratio);
        }
    }
    None
}

fn normalize_visual_ratio(value: f64) -> Option<f64> {
    if !value.is_finite() || value < 0.0 {
        return None;
    }
    if value <= 1.0 {
        Some(value)
    } else if value <= 100.0 {
        Some(value / 100.0)
    } else {
        None
    }
}

fn visual_bool(value: &Value, keys: &[&str]) -> bool {
    keys.iter()
        .filter_map(|key| value.get(*key))
        .find_map(Value::as_bool)
        .unwrap_or(false)
}

fn local_visual_confidence(
    frame_count: usize,
    evidence_span_seconds: f64,
    average_black_ratio: f64,
    average_text_ratio: f64,
) -> f64 {
    let frame_bonus =
        (frame_count.saturating_sub(LOCAL_VISUAL_CREDITS_MIN_FRAME_COUNT) as f64 * 0.015).min(0.06);
    let span_bonus = (evidence_span_seconds / 300.0).clamp(0.0, 0.05);
    let dark_bonus = ((average_black_ratio - LOCAL_VISUAL_CREDITS_BLACK_RATIO_THRESHOLD).max(0.0)
        * 0.08)
        .min(0.03);
    let text_bonus = ((average_text_ratio - LOCAL_VISUAL_CREDITS_TEXT_RATIO_THRESHOLD).max(0.0)
        * 0.10)
        .min(0.03);
    (LOCAL_VISUAL_CREDITS_MIN_CONFIDENCE + frame_bonus + span_bonus + dark_bonus + text_bonus)
        .min(0.96)
}

fn local_visual_min_frame_count(settings: Option<&Value>) -> usize {
    settings
        .and_then(|value| {
            value
                .get("min_frame_count")
                .or_else(|| value.get("minFrameCount"))
        })
        .and_then(Value::as_u64)
        .filter(|value| (2..=20).contains(value))
        .map(|value| value as usize)
        .unwrap_or(LOCAL_VISUAL_CREDITS_MIN_FRAME_COUNT)
}

fn local_visual_min_span_seconds(settings: Option<&Value>) -> f64 {
    settings
        .and_then(|value| {
            value
                .get("min_span_seconds")
                .or_else(|| value.get("minSpanSeconds"))
        })
        .and_then(Value::as_f64)
        .filter(|value| (10.0..=600.0).contains(value))
        .unwrap_or(LOCAL_VISUAL_CREDITS_MIN_SPAN_SECONDS)
}

fn local_visual_min_start_fraction(settings: Option<&Value>) -> f64 {
    settings
        .and_then(|value| {
            value
                .get("min_start_fraction")
                .or_else(|| value.get("minStartFraction"))
        })
        .and_then(Value::as_f64)
        .filter(|value| (0.50..=0.95).contains(value))
        .unwrap_or(LOCAL_VISUAL_CREDITS_MIN_START_FRACTION)
}

fn local_visual_max_frame_gap_seconds(settings: Option<&Value>) -> f64 {
    settings
        .and_then(|value| {
            value
                .get("max_frame_gap_seconds")
                .or_else(|| value.get("maxFrameGapSeconds"))
        })
        .and_then(Value::as_f64)
        .filter(|value| (5.0..=300.0).contains(value))
        .unwrap_or(LOCAL_VISUAL_CREDITS_MAX_FRAME_GAP_SECONDS)
}

fn local_visual_post_credit_scene_min_frames(settings: Option<&Value>) -> usize {
    settings
        .and_then(|value| {
            value
                .get("post_credit_scene_min_frames")
                .or_else(|| value.get("postCreditSceneMinFrames"))
        })
        .and_then(Value::as_u64)
        .filter(|value| (1..=20).contains(value))
        .map(|value| value as usize)
        .unwrap_or(LOCAL_VISUAL_POST_CREDIT_SCENE_MIN_FRAMES)
}

fn local_visual_post_credit_scene_min_span_seconds(settings: Option<&Value>) -> f64 {
    settings
        .and_then(|value| {
            value
                .get("post_credit_scene_min_span_seconds")
                .or_else(|| value.get("postCreditSceneMinSpanSeconds"))
        })
        .and_then(Value::as_f64)
        .filter(|value| (5.0..=300.0).contains(value))
        .unwrap_or(LOCAL_VISUAL_POST_CREDIT_SCENE_MIN_SPAN_SECONDS)
}

fn local_visual_black_ratio_threshold(settings: Option<&Value>) -> f64 {
    settings
        .and_then(|value| {
            value
                .get("black_ratio_threshold")
                .or_else(|| value.get("blackRatioThreshold"))
        })
        .and_then(Value::as_f64)
        .filter(|value| (0.10..=1.0).contains(value))
        .unwrap_or(LOCAL_VISUAL_CREDITS_BLACK_RATIO_THRESHOLD)
}

fn local_visual_text_ratio_threshold(settings: Option<&Value>) -> f64 {
    settings
        .and_then(|value| {
            value
                .get("text_ratio_threshold")
                .or_else(|| value.get("textRatioThreshold"))
        })
        .and_then(Value::as_f64)
        .filter(|value| (0.01..=1.0).contains(value))
        .unwrap_or(LOCAL_VISUAL_CREDITS_TEXT_RATIO_THRESHOLD)
}

async fn count_active_local_visual_segments_for_file(
    pool: &AnyPool,
    media_file_id: &str,
) -> Result<i64> {
    sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)
         FROM media_segments ms
         JOIN media_segment_candidates c ON c.id = ms.canonical_candidate_id
         WHERE ms.media_file_id = $1
           AND ms.status = 'active'
           AND c.provider_kind = $2",
    )
    .bind(media_file_id)
    .bind(PROVIDER_LOCAL_VISUAL_RECURRING)
    .fetch_one(pool)
    .await
    .context("counting active local visual detector segments")
}

pub async fn list_active_segments_for_file(
    pool: &AnyPool,
    media_file_id: &str,
) -> Result<Vec<ActiveMediaSegmentRecord>> {
    let rows = sqlx::query::<sqlx::Any>(
        "SELECT id, media_file_id, item_type, item_id, segment_type, start_seconds, end_seconds,
                canonical_candidate_id, source_label, confidence,
                CASE WHEN locked THEN 1 ELSE 0 END AS locked, status, metadata_json
         FROM media_segments
         WHERE media_file_id = $1 AND status = 'active'
         ORDER BY start_seconds ASC, end_seconds ASC",
    )
    .bind(media_file_id)
    .fetch_all(pool)
    .await
    .context("listing active media segments")?;

    Ok(rows.iter().map(active_segment_from_row).collect())
}

pub async fn list_active_segments_for_item(
    pool: &AnyPool,
    item_type: &str,
    item_id: &str,
) -> Result<Vec<ActiveMediaSegmentRecord>> {
    let context = normalize_segment_item_context(item_type, item_id)?;
    ensure_segment_item_exists(pool, &context).await?;
    let rows = sqlx::query::<sqlx::Any>(
        "SELECT id, media_file_id, item_type, item_id, segment_type, start_seconds, end_seconds,
                canonical_candidate_id, source_label, confidence,
                CASE WHEN locked THEN 1 ELSE 0 END AS locked, status, metadata_json
         FROM media_segments
         WHERE item_type = $1 AND item_id = $2 AND status = 'active'
         ORDER BY media_file_id ASC, start_seconds ASC, end_seconds ASC",
    )
    .bind(&context.item_type)
    .bind(&context.item_id)
    .fetch_all(pool)
    .await
    .context("listing active media segments for item")?;

    Ok(rows.iter().map(active_segment_from_row).collect())
}

pub async fn list_segment_candidates_for_file(
    pool: &AnyPool,
    media_file_id: &str,
) -> Result<Vec<SegmentCandidateRecord>> {
    let rows = sqlx::query::<sqlx::Any>(
        "SELECT id, media_file_id, item_type, item_id, segment_type, start_seconds, end_seconds,
                provider_kind, provider_id, provider_version, confidence, validation_state,
                validation_reason, identity_strength, source_payload_json,
                CAST(created_at AS TEXT) AS created_at,
                CAST(updated_at AS TEXT) AS updated_at
         FROM media_segment_candidates
         WHERE media_file_id = $1
         ORDER BY created_at DESC, start_seconds ASC
         LIMIT 200",
    )
    .bind(media_file_id)
    .fetch_all(pool)
    .await
    .context("listing media segment candidates")?;

    Ok(rows.iter().map(candidate_record_from_row).collect())
}

pub async fn list_segment_candidates_for_item(
    pool: &AnyPool,
    item_type: &str,
    item_id: &str,
) -> Result<Vec<SegmentCandidateRecord>> {
    let context = normalize_segment_item_context(item_type, item_id)?;
    ensure_segment_item_exists(pool, &context).await?;
    let rows = sqlx::query::<sqlx::Any>(
        "SELECT id, media_file_id, item_type, item_id, segment_type, start_seconds, end_seconds,
                provider_kind, provider_id, provider_version, confidence, validation_state,
                validation_reason, identity_strength, source_payload_json,
                CAST(created_at AS TEXT) AS created_at,
                CAST(updated_at AS TEXT) AS updated_at
         FROM media_segment_candidates
         WHERE item_type = $1 AND item_id = $2
         ORDER BY created_at DESC, media_file_id ASC, start_seconds ASC
         LIMIT 200",
    )
    .bind(&context.item_type)
    .bind(&context.item_id)
    .fetch_all(pool)
    .await
    .context("listing media segment candidates for item")?;

    Ok(rows.iter().map(candidate_record_from_row).collect())
}

pub async fn list_segment_candidate_review_queue(
    pool: &AnyPool,
    filters: MediaSegmentCandidateReviewFilters,
) -> Result<Vec<SegmentCandidateRecord>> {
    let media_file_id = filters
        .media_file_id
        .as_deref()
        .map(|value| normalize_required_text(value, "media_file_id"))
        .transpose()?;
    let item_type = filters
        .item_type
        .as_deref()
        .map(normalize_optional_candidate_item_type)
        .transpose()?;
    let item_id = filters
        .item_id
        .as_deref()
        .map(|value| normalize_required_text(value, "item_id"))
        .transpose()?;
    let segment_type = filters
        .segment_type
        .as_deref()
        .map(normalize_candidate_segment_filter)
        .transpose()?;
    let provider_kind = filters
        .provider_kind
        .as_deref()
        .map(|value| normalize_media_segment_job_identifier_filter(value, "provider_kind"))
        .transpose()?;
    let validation_state = filters
        .validation_state
        .as_deref()
        .map(normalize_candidate_validation_state_filter)
        .transpose()?;
    let validation_reason = filters
        .validation_reason
        .as_deref()
        .map(normalize_candidate_validation_reason_filter)
        .transpose()?;
    let low_confidence = filters.low_confidence.unwrap_or(false);
    let limit = filters.limit.unwrap_or(100).clamp(1, 500);

    let rows = sqlx::query::<sqlx::Any>(
        "SELECT id, media_file_id, item_type, item_id, segment_type, start_seconds, end_seconds,
                provider_kind, provider_id, provider_version, confidence, validation_state,
                validation_reason, identity_strength, source_payload_json,
                CAST(created_at AS TEXT) AS created_at,
                CAST(updated_at AS TEXT) AS updated_at
         FROM media_segment_candidates
         WHERE ($1 IS NULL OR media_file_id = $2)
           AND ($3 IS NULL OR item_type = $4)
           AND ($5 IS NULL OR item_id = $6)
           AND ($7 IS NULL OR segment_type = $8)
           AND ($9 IS NULL OR provider_kind = $10)
           AND ($11 IS NULL OR validation_state = $12)
           AND ($13 IS NULL OR validation_reason = $14)
           AND ($15 = 0 OR validation_reason = 'confidence_below_threshold')
         ORDER BY
           CASE validation_state
             WHEN 'rejected' THEN 0
             WHEN 'pending' THEN 1
             WHEN 'accepted' THEN 2
             ELSE 3
           END,
           updated_at DESC,
           created_at DESC,
           confidence ASC,
           start_seconds ASC
         LIMIT $16",
    )
    .bind(media_file_id.as_deref())
    .bind(media_file_id.as_deref())
    .bind(item_type.as_deref())
    .bind(item_type.as_deref())
    .bind(item_id.as_deref())
    .bind(item_id.as_deref())
    .bind(segment_type.as_deref())
    .bind(segment_type.as_deref())
    .bind(provider_kind.as_deref())
    .bind(provider_kind.as_deref())
    .bind(validation_state.as_deref())
    .bind(validation_state.as_deref())
    .bind(validation_reason.as_deref())
    .bind(validation_reason.as_deref())
    .bind(if low_confidence { 1_i64 } else { 0_i64 })
    .bind(limit)
    .fetch_all(pool)
    .await
    .context("listing media segment candidate review queue")?;

    Ok(rows.iter().map(candidate_record_from_row).collect())
}

pub async fn disable_active_segment(
    pool: &AnyPool,
    segment_id: &str,
    reason: Option<&str>,
) -> Result<Option<ActiveMediaSegmentRecord>> {
    let current = load_active_segment(pool, segment_id).await?;
    let Some(current) = current else {
        return Ok(None);
    };
    let disable_reason = reason
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("disabled_by_user");

    sqlx::query::<sqlx::Any>(
        "UPDATE media_segments
         SET status = 'disabled',
             metadata_json = $1,
             updated_at = CURRENT_TIMESTAMP
         WHERE id = $2",
    )
    .bind(
        json!({
            "disabled_reason": disable_reason,
            "previous_metadata": current.metadata,
        })
        .to_string(),
    )
    .bind(segment_id)
    .execute(pool)
    .await
    .context("disabling active media segment")?;

    if let Some(candidate_id) = current.canonical_candidate_id.as_deref() {
        sqlx::query::<sqlx::Any>(
            "UPDATE media_segment_candidates
             SET validation_state = 'rejected',
                 validation_reason = $1,
                 updated_at = CURRENT_TIMESTAMP
             WHERE id = $2",
        )
        .bind(disable_reason)
        .bind(candidate_id)
        .execute(pool)
        .await
        .context("marking disabled segment candidate rejected")?;
    }

    recalculate_active_segments(pool, &current.media_file_id, &current.segment_type).await?;
    Ok(Some(current))
}

async fn refresh_theintrodb_provider(
    pool: &AnyPool,
    client: &reqwest::Client,
    context: &ProviderMediaContext,
    settings: Option<&Value>,
    force_refresh: bool,
) -> Result<ProviderLookupResult> {
    let Some(imdb_id) = context.imdb_id.as_deref() else {
        return Ok(provider_skipped(PROVIDER_THEINTRODB, "missing_imdb_id"));
    };
    let cache_key = format!(
        "imdb:{imdb_id}:season:{}:episode:{}",
        context
            .season_number
            .map(|value| value.to_string())
            .unwrap_or_else(|| "movie".to_string()),
        context
            .episode_number
            .map(|value| value.to_string())
            .unwrap_or_else(|| "movie".to_string())
    );

    if !force_refresh
        && let Some(cached) = load_provider_cache(pool, PROVIDER_THEINTRODB, &cache_key).await?
    {
        return Ok(ProviderLookupResult {
            outcome: BuiltinProviderRefreshOutcome {
                provider_kind: PROVIDER_THEINTRODB.to_string(),
                enabled: true,
                status: cached.status,
                cache_hit: true,
                candidate_count: 0,
                accepted_count: 0,
                rejected_count: 0,
                reason: cached.reason,
            },
            response: cached.response,
        });
    }

    let base_url = provider_base_url(settings, DEFAULT_THEINTRODB_BASE_URL)?;
    let mut url = format!(
        "{}/segments?imdb_id={}",
        base_url.trim_end_matches('/'),
        urlencoding::encode(imdb_id)
    );
    if let Some(season) = context.season_number {
        url.push_str(&format!("&season={season}"));
    }
    if let Some(episode) = context.episode_number {
        url.push_str(&format!("&episode={episode}"));
    }

    fetch_json_provider_response(
        pool,
        client,
        context,
        PROVIDER_THEINTRODB,
        &cache_key,
        &url,
        settings,
    )
    .await
}

async fn refresh_aniskip_provider(
    pool: &AnyPool,
    client: &reqwest::Client,
    context: &ProviderMediaContext,
    settings: Option<&Value>,
    force_refresh: bool,
) -> Result<ProviderLookupResult> {
    let Some(mal_id) = context.mal_id.as_deref() else {
        return Ok(provider_skipped(PROVIDER_ANISKIP, "missing_mal_id"));
    };
    let episode = context
        .absolute_episode_number
        .or(context.episode_number)
        .context("missing anime episode number")?;
    let duration = context
        .duration_seconds
        .filter(|value| *value > 0.0)
        .unwrap_or(0.0);
    let cache_key = format!("mal:{mal_id}:episode:{episode}:duration:{duration:.3}");

    if !force_refresh
        && let Some(cached) = load_provider_cache(pool, PROVIDER_ANISKIP, &cache_key).await?
    {
        return Ok(ProviderLookupResult {
            outcome: BuiltinProviderRefreshOutcome {
                provider_kind: PROVIDER_ANISKIP.to_string(),
                enabled: true,
                status: cached.status,
                cache_hit: true,
                candidate_count: 0,
                accepted_count: 0,
                rejected_count: 0,
                reason: cached.reason,
            },
            response: cached.response,
        });
    }

    let base_url = provider_base_url(settings, DEFAULT_ANISKIP_BASE_URL)?;
    let encoded_mal_id = urlencoding::encode(mal_id);
    let url = format!(
        "{}/v1/skip-times/{encoded_mal_id}/{episode}?types[]=op&types[]=ed&episodeLength={duration:.3}",
        base_url.trim_end_matches('/')
    );

    let mut result = fetch_json_provider_response(
        pool,
        client,
        context,
        PROVIDER_ANISKIP,
        &cache_key,
        &url,
        settings,
    )
    .await?;

    if result.outcome.status == "not_found" && !result.outcome.cache_hit {
        let fallback_url = format!(
            "{}/skip-times/{encoded_mal_id}/{episode}?types[]=op&types[]=ed&episodeLength={duration:.3}",
            base_url.trim_end_matches('/')
        );
        result = fetch_json_provider_response(
            pool,
            client,
            context,
            PROVIDER_ANISKIP,
            &cache_key,
            &fallback_url,
            settings,
        )
        .await?;
    }

    Ok(result)
}

async fn refresh_marketplace_segment_provider(
    pool: &AnyPool,
    client: &reqwest::Client,
    context: &ProviderMediaContext,
    selected: &MarketplaceSegmentProviderSelection,
    provider_kind: &str,
    settings: Option<&Value>,
    force_refresh: bool,
) -> Result<ProviderLookupResult> {
    let cache_key = marketplace_segment_provider_cache_key(selected, context);

    if !force_refresh
        && let Some(cached) = load_provider_cache(pool, provider_kind, &cache_key).await?
    {
        return Ok(ProviderLookupResult {
            outcome: BuiltinProviderRefreshOutcome {
                provider_kind: provider_kind.to_string(),
                enabled: true,
                status: cached.status,
                cache_hit: true,
                candidate_count: 0,
                accepted_count: 0,
                rejected_count: 0,
                reason: cached.reason,
            },
            response: cached.response,
        });
    }

    if !acquire_provider_rate_limit(pool, provider_kind, settings).await? {
        return Ok(ProviderLookupResult {
            outcome: BuiltinProviderRefreshOutcome {
                provider_kind: provider_kind.to_string(),
                enabled: true,
                status: "rate_limited".to_string(),
                cache_hit: false,
                candidate_count: 0,
                accepted_count: 0,
                rejected_count: 0,
                reason: Some("provider_rate_limit_exceeded".to_string()),
            },
            response: None,
        });
    }

    let endpoint_json = selected
        .provider
        .endpoint_json
        .clone()
        .context("media segment provider endpoint is missing")?;
    let endpoint: ProviderEndpoint =
        serde_json::from_value(endpoint_json).context("parsing media segment provider endpoint")?;
    let base_url =
        resolve_control_provider_transport_base_url(selected.instance.instance_id, &endpoint)
            .await?;
    let lookup_url = media_segment_provider_lookup_url(&base_url)?;
    let media_type = marketplace_provider_media_type_for_context(context);
    let invocation = MarketplaceSegmentProviderInvocation {
        schema_version: MEDIA_SEGMENT_PROVIDER_SCHEMA_VERSION,
        request: MarketplaceSegmentProviderLookupRequest {
            media_file_id: &context.media_file_id,
            item_type: &context.item_type,
            item_id: &context.item_id,
            media_type: &media_type,
            duration_seconds: context.duration_seconds,
            external_ids: marketplace_provider_external_ids(context),
            season_number: context.season_number,
            episode_number: context.episode_number,
            absolute_episode_number: context.absolute_episode_number,
            requested_segment_types: &selected.segment_types,
        },
        provider: MarketplaceSegmentProviderInvocationContext {
            provider_id: selected.provider.provider_id,
            extension_id: &selected.extension.extension_id,
            instance_id: selected.instance.instance_id,
            implementation: selected.provider.implementation.as_deref(),
            config: selected.instance.config_json.clone(),
        },
    };

    let response = client
        .post(lookup_url.clone())
        .timeout(provider_timeout(settings))
        .json(&invocation)
        .send()
        .await;
    let response = match response {
        Ok(response) => response,
        Err(err) => {
            let error = json!({
                "error": err.to_string(),
                "url": redact_provider_url(lookup_url.as_str()),
                "provider_id": selected.provider.provider_id,
                "extension_id": selected.extension.extension_id,
            });
            upsert_provider_cache(
                pool,
                context,
                provider_kind,
                &cache_key,
                "error",
                None,
                Some(error),
                provider_cache_ttl_seconds(settings).min(900),
            )
            .await?;
            return Ok(ProviderLookupResult {
                outcome: BuiltinProviderRefreshOutcome {
                    provider_kind: provider_kind.to_string(),
                    enabled: true,
                    status: "error".to_string(),
                    cache_hit: false,
                    candidate_count: 0,
                    accepted_count: 0,
                    rejected_count: 0,
                    reason: Some("provider_request_failed".to_string()),
                },
                response: None,
            });
        }
    };

    let status = response.status();
    if status == reqwest::StatusCode::NOT_FOUND {
        upsert_provider_cache(
            pool,
            context,
            provider_kind,
            &cache_key,
            "not_found",
            Some(json!({"found": false})),
            None,
            provider_cache_ttl_seconds(settings),
        )
        .await?;
        return Ok(ProviderLookupResult {
            outcome: BuiltinProviderRefreshOutcome {
                provider_kind: provider_kind.to_string(),
                enabled: true,
                status: "not_found".to_string(),
                cache_hit: false,
                candidate_count: 0,
                accepted_count: 0,
                rejected_count: 0,
                reason: Some("provider_no_segments".to_string()),
            },
            response: None,
        });
    }

    if !status.is_success() {
        let error = json!({
            "status": status.as_u16(),
            "url": redact_provider_url(lookup_url.as_str()),
            "provider_id": selected.provider.provider_id,
            "extension_id": selected.extension.extension_id,
        });
        upsert_provider_cache(
            pool,
            context,
            provider_kind,
            &cache_key,
            "error",
            None,
            Some(error),
            provider_cache_ttl_seconds(settings).min(900),
        )
        .await?;
        return Ok(ProviderLookupResult {
            outcome: BuiltinProviderRefreshOutcome {
                provider_kind: provider_kind.to_string(),
                enabled: true,
                status: "error".to_string(),
                cache_hit: false,
                candidate_count: 0,
                accepted_count: 0,
                rejected_count: 0,
                reason: Some(format!("provider_http_{}", status.as_u16())),
            },
            response: None,
        });
    }

    let payload = parse_bounded_media_segment_provider_response(response).await?;
    upsert_provider_cache(
        pool,
        context,
        provider_kind,
        &cache_key,
        "ok",
        Some(payload.clone()),
        None,
        provider_cache_ttl_seconds(settings),
    )
    .await?;

    Ok(ProviderLookupResult {
        outcome: BuiltinProviderRefreshOutcome {
            provider_kind: provider_kind.to_string(),
            enabled: true,
            status: "ok".to_string(),
            cache_hit: false,
            candidate_count: 0,
            accepted_count: 0,
            rejected_count: 0,
            reason: None,
        },
        response: Some(payload),
    })
}

async fn parse_bounded_media_segment_provider_response(
    response: reqwest::Response,
) -> Result<Value> {
    if let Some(length) = response.content_length()
        && length > MEDIA_SEGMENT_PROVIDER_RESPONSE_MAX_BYTES
    {
        bail!(
            "media segment provider response exceeds {} bytes",
            MEDIA_SEGMENT_PROVIDER_RESPONSE_MAX_BYTES
        );
    }
    let bytes = response
        .bytes()
        .await
        .context("reading media segment provider response")?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MEDIA_SEGMENT_PROVIDER_RESPONSE_MAX_BYTES {
        bail!(
            "media segment provider response exceeds {} bytes",
            MEDIA_SEGMENT_PROVIDER_RESPONSE_MAX_BYTES
        );
    }
    serde_json::from_slice::<Value>(&bytes).context("parsing media segment provider response")
}

fn media_segment_provider_lookup_url(base_url: &str) -> Result<reqwest::Url> {
    let mut base =
        reqwest::Url::parse(base_url).context("parsing media segment provider base URL")?;
    let mut path = base.path().trim_end_matches('/').to_string();
    path.push('/');
    base.set_path(&path);
    base.join(MEDIA_SEGMENT_PROVIDER_LOOKUP_PATH)
        .context("building media segment provider lookup URL")
}

async fn fetch_json_provider_response(
    pool: &AnyPool,
    client: &reqwest::Client,
    context: &ProviderMediaContext,
    provider_kind: &str,
    cache_key: &str,
    url: &str,
    settings: Option<&Value>,
) -> Result<ProviderLookupResult> {
    if !acquire_provider_rate_limit(pool, provider_kind, settings).await? {
        return Ok(ProviderLookupResult {
            outcome: BuiltinProviderRefreshOutcome {
                provider_kind: provider_kind.to_string(),
                enabled: true,
                status: "rate_limited".to_string(),
                cache_hit: false,
                candidate_count: 0,
                accepted_count: 0,
                rejected_count: 0,
                reason: Some("provider_rate_limit_exceeded".to_string()),
            },
            response: None,
        });
    }

    let timeout = provider_timeout(settings);
    let response = client.get(url).timeout(timeout).send().await;
    let response = match response {
        Ok(response) => response,
        Err(err) => {
            let error = json!({"error": err.to_string(), "url": redact_provider_url(url)});
            upsert_provider_cache(
                pool,
                context,
                provider_kind,
                cache_key,
                "error",
                None,
                Some(error),
                provider_cache_ttl_seconds(settings).min(900),
            )
            .await?;
            return Ok(ProviderLookupResult {
                outcome: BuiltinProviderRefreshOutcome {
                    provider_kind: provider_kind.to_string(),
                    enabled: true,
                    status: "error".to_string(),
                    cache_hit: false,
                    candidate_count: 0,
                    accepted_count: 0,
                    rejected_count: 0,
                    reason: Some("provider_request_failed".to_string()),
                },
                response: None,
            });
        }
    };

    let status = response.status();
    if status == reqwest::StatusCode::NOT_FOUND {
        upsert_provider_cache(
            pool,
            context,
            provider_kind,
            cache_key,
            "not_found",
            Some(json!({"found": false})),
            None,
            provider_cache_ttl_seconds(settings),
        )
        .await?;
        return Ok(ProviderLookupResult {
            outcome: BuiltinProviderRefreshOutcome {
                provider_kind: provider_kind.to_string(),
                enabled: true,
                status: "not_found".to_string(),
                cache_hit: false,
                candidate_count: 0,
                accepted_count: 0,
                rejected_count: 0,
                reason: Some("provider_no_segments".to_string()),
            },
            response: None,
        });
    }

    if !status.is_success() {
        let error = json!({
            "status": status.as_u16(),
            "url": redact_provider_url(url),
        });
        upsert_provider_cache(
            pool,
            context,
            provider_kind,
            cache_key,
            "error",
            None,
            Some(error),
            provider_cache_ttl_seconds(settings).min(900),
        )
        .await?;
        return Ok(ProviderLookupResult {
            outcome: BuiltinProviderRefreshOutcome {
                provider_kind: provider_kind.to_string(),
                enabled: true,
                status: "error".to_string(),
                cache_hit: false,
                candidate_count: 0,
                accepted_count: 0,
                rejected_count: 0,
                reason: Some(format!("provider_http_{}", status.as_u16())),
            },
            response: None,
        });
    }

    let payload = response
        .json::<Value>()
        .await
        .context("decoding media segment provider response")?;
    upsert_provider_cache(
        pool,
        context,
        provider_kind,
        cache_key,
        "ok",
        Some(payload.clone()),
        None,
        provider_cache_ttl_seconds(settings),
    )
    .await?;

    Ok(ProviderLookupResult {
        outcome: BuiltinProviderRefreshOutcome {
            provider_kind: provider_kind.to_string(),
            enabled: true,
            status: "ok".to_string(),
            cache_hit: false,
            candidate_count: 0,
            accepted_count: 0,
            rejected_count: 0,
            reason: None,
        },
        response: Some(payload),
    })
}

fn provider_response_to_candidates(
    provider_kind: &str,
    context: &ProviderMediaContext,
    response: &Value,
) -> Result<Vec<SegmentCandidateInput>> {
    match provider_kind {
        PROVIDER_THEINTRODB => theintrodb_response_to_candidates(context, response),
        PROVIDER_ANISKIP => aniskip_response_to_candidates(context, response),
        _ => Ok(Vec::new()),
    }
}

async fn select_marketplace_segment_provider(
    store: &ExtensionStore<'_>,
    provider_kind: &str,
    media_type: Option<&str>,
) -> Result<MarketplaceSegmentProviderSelection> {
    let provider_kind = normalize_provider_id(provider_kind)?;
    available_marketplace_segment_providers(store, Some(&provider_kind), media_type)
        .await?
        .into_iter()
        .next()
        .with_context(|| format!("media segment provider '{provider_kind}' is not available"))
}

async fn select_marketplace_segment_provider_by_id(
    store: &ExtensionStore<'_>,
    provider_id: Uuid,
) -> Result<MarketplaceSegmentProviderSelection> {
    let provider = store
        .get_provider(provider_id)
        .await?
        .with_context(|| format!("media segment provider '{provider_id}' was not found"))?;
    if provider.capability != MEDIA_SEGMENT_PROVIDER_CAPABILITY {
        bail!("provider is not a media segment provider");
    }
    if provider.endpoint_json.is_none() {
        bail!("media segment provider endpoint is missing");
    }
    let instance = store
        .get_instance(provider.instance_id)
        .await?
        .context("media segment provider instance was not found")?;
    if !instance.enabled {
        bail!("media segment provider instance is disabled");
    }
    let extension = store
        .get_extension(&instance.extension_id)
        .await?
        .context("media segment provider extension was not found")?;
    if !extension.enabled {
        bail!("media segment provider extension is disabled");
    }
    let (media_types, segment_types, actions) = media_segment_provider_scope(&provider);
    if !actions.iter().any(|action| action == "lookup") {
        bail!("media segment provider does not declare lookup action");
    }
    if segment_types.is_empty() {
        bail!("media segment provider does not declare usable segment types");
    }
    Ok(MarketplaceSegmentProviderSelection {
        provider,
        extension,
        instance,
        media_types,
        segment_types,
        actions,
    })
}

async fn available_marketplace_segment_providers(
    store: &ExtensionStore<'_>,
    provider_kind: Option<&str>,
    media_type: Option<&str>,
) -> Result<Vec<MarketplaceSegmentProviderSelection>> {
    let provider_kind = provider_kind
        .map(normalize_provider_id)
        .transpose()
        .context("normalizing marketplace segment provider kind")?;
    let mut selections = Vec::new();

    for detail in store.list_provider_details().await? {
        let provider = detail.provider;
        if provider.capability != MEDIA_SEGMENT_PROVIDER_CAPABILITY {
            continue;
        }
        if provider.health_state != ProviderHealthState::Healthy {
            continue;
        }
        let Some(implementation) = provider.implementation.as_deref() else {
            continue;
        };
        let normalized_implementation = normalize_provider_id(implementation)?;
        if let Some(provider_kind) = provider_kind.as_deref() {
            if normalized_implementation != provider_kind {
                continue;
            }
        }
        if provider.endpoint_json.is_none() {
            continue;
        }
        let Some(extension) = store.get_extension(&detail.extension_id).await? else {
            continue;
        };
        if !extension.enabled {
            continue;
        }
        let Some(instance) = store.get_instance(provider.instance_id).await? else {
            continue;
        };
        if !instance.enabled {
            continue;
        }
        let (media_types, segment_types, actions) = media_segment_provider_scope(&provider);
        if !actions.iter().any(|action| action == "lookup") {
            continue;
        }
        if segment_types.is_empty() {
            continue;
        }
        if let Some(media_type) = media_type
            && !media_types.is_empty()
            && !media_types.iter().any(|provider_media_type| {
                segment_provider_media_type_matches(provider_media_type, media_type)
            })
        {
            continue;
        }
        selections.push(MarketplaceSegmentProviderSelection {
            provider,
            extension,
            instance,
            media_types,
            segment_types,
            actions,
        });
    }

    selections.sort_by_key(|selection| {
        (
            selection.extension.name.clone(),
            selection.instance.instance_name.clone(),
            selection.provider.provider_id,
        )
    });
    Ok(selections)
}

fn marketplace_segment_provider_response_to_candidates(
    provider_kind: &str,
    selected: &MarketplaceSegmentProviderSelection,
    context: &ProviderMediaContext,
    response: &Value,
) -> Result<Vec<SegmentCandidateInput>> {
    let values = provider_segment_array(response);
    let mut candidates = Vec::new();
    for value in values.into_iter().take(MEDIA_SEGMENT_PROVIDER_MAX_SEGMENTS) {
        let Some(segment_type) = provider_segment_type(value) else {
            continue;
        };
        if !segment_provider_supports_segment_type(selected, &segment_type) {
            continue;
        }
        let Some(start_seconds) = provider_seconds(value, &["start_sec", "start_seconds", "start"])
        else {
            continue;
        };
        let Some(end_seconds) = provider_seconds(value, &["end_sec", "end_seconds", "end"]) else {
            continue;
        };
        candidates.push(SegmentCandidateInput {
            media_file_id: context.media_file_id.clone(),
            item_type: Some(context.item_type.clone()),
            item_id: Some(context.item_id.clone()),
            segment_type: segment_type.clone(),
            start_seconds,
            end_seconds,
            provider_kind: provider_kind.to_string(),
            provider_id: marketplace_segment_candidate_provider_id(selected, value),
            provider_version: marketplace_segment_provider_version(selected, value),
            confidence: provider_confidence(value).unwrap_or(0.80),
            identity_strength: marketplace_segment_identity_strength(value),
            source_payload: Some(json!({
                "provider": "marketplace",
                "provider_kind": provider_kind,
                "provider_id": selected.provider.provider_id,
                "extension_id": selected.extension.extension_id,
                "instance_id": selected.instance.instance_id,
                "implementation": selected.provider.implementation.as_deref(),
                "provider_segment_id": provider_id_from_value(value),
                "media_types": &selected.media_types,
                "segment_types": &selected.segment_types,
                "actions": &selected.actions,
                "imdb_id": context.imdb_id,
                "tmdb_id": context.tmdb_id,
                "tvdb_id": context.tvdb_id,
                "mal_id": context.mal_id,
                "anilist_id": context.anilist_id,
                "season_number": context.season_number,
                "episode_number": context.episode_number,
                "absolute_episode_number": context.absolute_episode_number,
                "raw": value,
            })),
        });
    }
    Ok(candidates)
}

async fn run_media_segment_provider_certification_probe(
    client: &reqwest::Client,
    selected: &MarketplaceSegmentProviderSelection,
    context: &ProviderMediaContext,
) -> MediaSegmentProviderCertificationProbe {
    match invoke_media_segment_provider_certification_probe(client, selected, context).await {
        Ok(response) => {
            validate_media_segment_provider_certification_response(selected, context, &response)
        }
        Err(err) => MediaSegmentProviderCertificationProbe {
            media_type: marketplace_provider_media_type_for_context(context),
            status: "broken".to_string(),
            failure_class: Some(classify_media_segment_provider_certification_error(&err)),
            summary: err.to_string(),
            segment_count: 0,
            segment_type_counts: BTreeMap::new(),
            response_evidence: json!({
                "media_type": marketplace_provider_media_type_for_context(context),
                "status": "broken",
                "error": err.to_string(),
            }),
        },
    }
}

async fn invoke_media_segment_provider_certification_probe(
    client: &reqwest::Client,
    selected: &MarketplaceSegmentProviderSelection,
    context: &ProviderMediaContext,
) -> Result<Value> {
    let endpoint_json = selected
        .provider
        .endpoint_json
        .clone()
        .context("media segment provider endpoint is missing")?;
    let endpoint: ProviderEndpoint =
        serde_json::from_value(endpoint_json).context("parsing media segment provider endpoint")?;
    let base_url =
        resolve_control_provider_transport_base_url(selected.instance.instance_id, &endpoint)
            .await?;
    let lookup_url = media_segment_provider_lookup_url(&base_url)?;
    let media_type = marketplace_provider_media_type_for_context(context);
    let invocation = MarketplaceSegmentProviderInvocation {
        schema_version: MEDIA_SEGMENT_PROVIDER_SCHEMA_VERSION,
        request: MarketplaceSegmentProviderLookupRequest {
            media_file_id: &context.media_file_id,
            item_type: &context.item_type,
            item_id: &context.item_id,
            media_type: &media_type,
            duration_seconds: context.duration_seconds,
            external_ids: marketplace_provider_external_ids(context),
            season_number: context.season_number,
            episode_number: context.episode_number,
            absolute_episode_number: context.absolute_episode_number,
            requested_segment_types: &selected.segment_types,
        },
        provider: MarketplaceSegmentProviderInvocationContext {
            provider_id: selected.provider.provider_id,
            extension_id: &selected.extension.extension_id,
            instance_id: selected.instance.instance_id,
            implementation: selected.provider.implementation.as_deref(),
            config: selected.instance.config_json.clone(),
        },
    };

    let response = client
        .post(lookup_url.clone())
        .json(&invocation)
        .send()
        .await
        .with_context(|| {
            format!("calling media segment provider certification probe at {lookup_url}")
        })?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        bail!(
            "media segment provider certification probe returned {status}: {}",
            truncate_for_error(&body, 512)
        );
    }
    parse_bounded_media_segment_provider_response(response).await
}

fn validate_media_segment_provider_certification_response(
    selected: &MarketplaceSegmentProviderSelection,
    context: &ProviderMediaContext,
    response: &Value,
) -> MediaSegmentProviderCertificationProbe {
    let media_type = marketplace_provider_media_type_for_context(context);
    let values = match provider_segment_array_for_certification(response) {
        Ok(values) => values,
        Err(err) => {
            return MediaSegmentProviderCertificationProbe {
                media_type,
                status: "broken".to_string(),
                failure_class: Some("invalid_response_shape".to_string()),
                summary: err.to_string(),
                segment_count: 0,
                segment_type_counts: BTreeMap::new(),
                response_evidence: json!({
                    "status": "broken",
                    "failure_class": "invalid_response_shape",
                    "error": err.to_string(),
                }),
            };
        }
    };
    if values.len() > MEDIA_SEGMENT_PROVIDER_MAX_SEGMENTS {
        return MediaSegmentProviderCertificationProbe {
            media_type,
            status: "broken".to_string(),
            failure_class: Some("response_too_many_segments".to_string()),
            summary: format!(
                "provider returned {} segments; maximum is {}",
                values.len(),
                MEDIA_SEGMENT_PROVIDER_MAX_SEGMENTS
            ),
            segment_count: values.len(),
            segment_type_counts: BTreeMap::new(),
            response_evidence: json!({
                "status": "broken",
                "failure_class": "response_too_many_segments",
                "segment_count": values.len(),
            }),
        };
    }

    let mut segment_type_counts = BTreeMap::new();
    for value in &values {
        let Some(segment_type) = provider_segment_type(value) else {
            return invalid_media_segment_provider_certification_segment(
                &media_type,
                values.len(),
                "unsupported_segment_type",
                value,
            );
        };
        if !segment_provider_supports_segment_type(selected, &segment_type) {
            return invalid_media_segment_provider_certification_segment(
                &media_type,
                values.len(),
                "segment_type_outside_scope",
                value,
            );
        }
        let Some(start_seconds) = provider_seconds(value, &["start_sec", "start_seconds", "start"])
        else {
            return invalid_media_segment_provider_certification_segment(
                &media_type,
                values.len(),
                "missing_start_seconds",
                value,
            );
        };
        let Some(end_seconds) = provider_seconds(value, &["end_sec", "end_seconds", "end"]) else {
            return invalid_media_segment_provider_certification_segment(
                &media_type,
                values.len(),
                "missing_end_seconds",
                value,
            );
        };
        if !start_seconds.is_finite() || !end_seconds.is_finite() || end_seconds <= start_seconds {
            return invalid_media_segment_provider_certification_segment(
                &media_type,
                values.len(),
                "invalid_time_range",
                value,
            );
        }
        *segment_type_counts.entry(segment_type).or_insert(0) += 1;
    }

    MediaSegmentProviderCertificationProbe {
        media_type: media_type.clone(),
        status: "certified".to_string(),
        failure_class: None,
        summary: format!("valid response with {} segment(s)", values.len()),
        segment_count: values.len(),
        segment_type_counts,
        response_evidence: json!({
            "media_type": media_type,
            "status": "certified",
            "segment_count": values.len(),
        }),
    }
}

fn invalid_media_segment_provider_certification_segment(
    media_type: &str,
    segment_count: usize,
    failure_class: &str,
    value: &Value,
) -> MediaSegmentProviderCertificationProbe {
    MediaSegmentProviderCertificationProbe {
        media_type: media_type.to_string(),
        status: "broken".to_string(),
        failure_class: Some(failure_class.to_string()),
        summary: format!("invalid segment response: {failure_class}"),
        segment_count,
        segment_type_counts: BTreeMap::new(),
        response_evidence: json!({
            "media_type": media_type,
            "status": "broken",
            "failure_class": failure_class,
            "segment_count": segment_count,
            "segment": value,
        }),
    }
}

fn provider_segment_array_for_certification(response: &Value) -> Result<Vec<&Value>> {
    if let Some(values) = response.as_array() {
        return Ok(values.iter().collect());
    }
    if response.is_object() {
        for key in ["segments", "results", "data"] {
            if let Some(values) = response.get(key).and_then(Value::as_array) {
                return Ok(values.iter().collect());
            }
        }
    }
    bail!("media segment provider response must be an array or contain segments/results/data array")
}

fn theintrodb_response_to_candidates(
    context: &ProviderMediaContext,
    response: &Value,
) -> Result<Vec<SegmentCandidateInput>> {
    let mut candidates = Vec::new();
    for (fallback_segment_type, value) in theintrodb_segment_entries(response) {
        let Some(segment_type) = provider_segment_type(value).or(fallback_segment_type) else {
            continue;
        };
        let Some(start_seconds) = provider_seconds(value, &["start_sec", "start_seconds", "start"])
        else {
            continue;
        };
        let Some(end_seconds) = provider_seconds(value, &["end_sec", "end_seconds", "end"]) else {
            continue;
        };
        candidates.push(SegmentCandidateInput {
            media_file_id: context.media_file_id.clone(),
            item_type: Some(context.item_type.clone()),
            item_id: Some(context.item_id.clone()),
            segment_type,
            start_seconds,
            end_seconds,
            provider_kind: PROVIDER_THEINTRODB.to_string(),
            provider_id: "introdb_segments".to_string(),
            provider_version: Some("1".to_string()),
            confidence: provider_confidence(value).unwrap_or(0.90),
            identity_strength: "external_id_exact".to_string(),
            source_payload: Some(json!({
                "provider": PROVIDER_THEINTRODB,
                "provider_segment_id": provider_id_from_value(value),
                "imdb_id": context.imdb_id,
                "tmdb_id": context.tmdb_id,
                "tvdb_id": context.tvdb_id,
                "season_number": context.season_number,
                "episode_number": context.episode_number,
                "raw": value,
            })),
        });
    }
    Ok(candidates)
}

fn theintrodb_segment_entries(response: &Value) -> Vec<(Option<String>, &Value)> {
    let mut entries = provider_segment_array(response)
        .into_iter()
        .map(|value| (None, value))
        .collect::<Vec<_>>();

    let Some(object) = response.as_object() else {
        return entries;
    };
    for (key, segment_type) in [
        ("intro", "intro"),
        ("recap", "recap"),
        ("outro", "outro"),
        ("credits", "credits"),
    ] {
        let Some(value) = object.get(key) else {
            continue;
        };
        if value.is_null() {
            continue;
        }
        if let Some(values) = value.as_array() {
            entries.extend(
                values
                    .iter()
                    .map(|value| (Some(segment_type.to_string()), value)),
            );
        } else {
            entries.push((Some(segment_type.to_string()), value));
        }
    }

    entries
}

fn aniskip_response_to_candidates(
    context: &ProviderMediaContext,
    response: &Value,
) -> Result<Vec<SegmentCandidateInput>> {
    let values = response
        .get("results")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut candidates = Vec::new();
    for value in values {
        let Some(skip_type) = value
            .get("skipType")
            .or_else(|| value.get("skip_type"))
            .and_then(Value::as_str)
        else {
            continue;
        };
        let Some(segment_type) = aniskip_segment_type(skip_type) else {
            continue;
        };
        let interval = value.get("interval").unwrap_or(&value);
        let Some(start_seconds) = provider_seconds(interval, &["startTime", "start_time", "start"])
        else {
            continue;
        };
        let Some(end_seconds) = provider_seconds(interval, &["endTime", "end_time", "end"]) else {
            continue;
        };
        candidates.push(SegmentCandidateInput {
            media_file_id: context.media_file_id.clone(),
            item_type: Some(context.item_type.clone()),
            item_id: Some(context.item_id.clone()),
            segment_type: segment_type.to_string(),
            start_seconds,
            end_seconds,
            provider_kind: PROVIDER_ANISKIP.to_string(),
            provider_id: "aniskip_skip_times".to_string(),
            provider_version: Some("1".to_string()),
            confidence: 0.88,
            identity_strength: "external_id_episode".to_string(),
            source_payload: Some(json!({
                "provider": PROVIDER_ANISKIP,
                "skip_id": value.get("skipId").or_else(|| value.get("skip_id")).cloned(),
                "skip_type": skip_type,
                "mal_id": context.mal_id,
                "anilist_id": context.anilist_id,
                "episode_number": context.episode_number,
                "absolute_episode_number": context.absolute_episode_number,
                "episode_length": value.get("episodeLength").or_else(|| value.get("episode_length")).cloned(),
                "raw": value,
            })),
        });
    }
    Ok(candidates)
}

fn provider_segment_array(response: &Value) -> Vec<&Value> {
    if let Some(values) = response.as_array() {
        return values.iter().collect();
    }
    for key in ["segments", "results", "data"] {
        if let Some(values) = response.get(key).and_then(Value::as_array) {
            return values.iter().collect();
        }
    }
    Vec::new()
}

fn provider_segment_type(value: &Value) -> Option<String> {
    let raw = value
        .get("segment_type")
        .or_else(|| value.get("segmentType"))
        .or_else(|| value.get("type"))
        .and_then(Value::as_str)?;
    let normalized = raw.trim().to_ascii_lowercase().replace('-', "_");
    let mapped = match normalized.as_str() {
        "opening" | "op" => "intro",
        "ending" | "ed" => "outro",
        "credit" => "credits",
        "intro" | "recap" | "preview" | "credits" | "outro" => normalized.as_str(),
        _ => return None,
    };
    Some(mapped.to_string())
}

fn aniskip_segment_type(skip_type: &str) -> Option<&'static str> {
    match skip_type.trim().to_ascii_lowercase().as_str() {
        "op" | "mixed-op" | "mixed_op" => Some("intro"),
        "ed" | "mixed-ed" | "mixed_ed" => Some("outro"),
        "recap" => Some("recap"),
        _ => None,
    }
}

fn provider_seconds(value: &Value, keys: &[&str]) -> Option<f64> {
    for key in keys {
        let Some(raw) = value.get(*key) else {
            continue;
        };
        if let Some(seconds) = raw.as_f64().filter(|value| value.is_finite()) {
            return Some(seconds);
        }
        if let Some(text) = raw.as_str().map(str::trim).filter(|text| !text.is_empty()) {
            if let Some(seconds) = parse_clock_or_seconds(text) {
                return Some(seconds);
            }
        }
    }
    None
}

fn parse_clock_or_seconds(value: &str) -> Option<f64> {
    if let Ok(seconds) = value.parse::<f64>() {
        return seconds.is_finite().then_some(seconds);
    }
    let parts = value.split(':').collect::<Vec<_>>();
    if !(2..=3).contains(&parts.len()) {
        return None;
    }
    let mut total = 0.0;
    for part in parts {
        let number = part.trim().parse::<f64>().ok()?;
        total = total * 60.0 + number;
    }
    total.is_finite().then_some(total)
}

fn provider_confidence(value: &Value) -> Option<f64> {
    value
        .get("confidence")
        .or_else(|| value.get("score"))
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite())
        .map(|value| value.clamp(0.0, 1.0))
}

fn provider_id_from_value(value: &Value) -> Option<Value> {
    value
        .get("id")
        .or_else(|| value.get("segment_id"))
        .or_else(|| value.get("segmentId"))
        .cloned()
}

fn marketplace_provider_settings_enabled(settings: Option<&Value>) -> bool {
    settings
        .and_then(|value| value.get("enabled"))
        .and_then(Value::as_bool)
        .unwrap_or(true)
}

fn marketplace_provider_media_type_for_context(context: &ProviderMediaContext) -> String {
    if context.item_type == "movie" {
        return "movie".to_string();
    }
    if context.mal_id.is_some() || context.anilist_id.is_some() {
        return "anime".to_string();
    }
    "series".to_string()
}

fn marketplace_provider_external_ids(context: &ProviderMediaContext) -> Value {
    json!({
        "imdb": context.imdb_id,
        "tmdb": context.tmdb_id,
        "tvdb": context.tvdb_id,
        "mal": context.mal_id,
        "anilist": context.anilist_id,
    })
}

fn marketplace_segment_provider_cache_key(
    selected: &MarketplaceSegmentProviderSelection,
    context: &ProviderMediaContext,
) -> String {
    format!(
        "provider:{}:media_file:{}:duration:{:.3}:season:{}:episode:{}:absolute:{}",
        selected.provider.provider_id,
        context.media_file_id,
        context.duration_seconds.unwrap_or(0.0),
        context
            .season_number
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_string()),
        context
            .episode_number
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_string()),
        context
            .absolute_episode_number
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_string())
    )
}

fn media_segment_provider_scope(provider: &Provider) -> (Vec<String>, Vec<String>, Vec<String>) {
    let Some(scope) = provider.scope_json.as_ref() else {
        return (Vec::new(), Vec::new(), Vec::new());
    };
    let media_types = scope
        .get("media_types")
        .or_else(|| scope.get("mediaTypes"))
        .and_then(Value::as_array)
        .map(|values| media_segment_provider_string_array(values))
        .unwrap_or_default();
    let segment_types = scope
        .get("segment_types")
        .or_else(|| scope.get("segmentTypes"))
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(|value| value.trim().to_ascii_lowercase().replace('-', "_"))
                .filter(|value| allowed_segment_type(value))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let actions = scope
        .get("actions")
        .and_then(Value::as_array)
        .map(|values| media_segment_provider_string_array(values))
        .unwrap_or_default();
    (media_types, segment_types, actions)
}

fn media_segment_provider_string_array(values: &[Value]) -> Vec<String> {
    values
        .iter()
        .filter_map(Value::as_str)
        .map(|value| value.trim().to_ascii_lowercase().replace('-', "_"))
        .filter(|value| !value.is_empty())
        .collect()
}

fn segment_provider_media_type_matches(provider_value: &str, requested_value: &str) -> bool {
    let Some(provider_type) = normalize_segment_provider_media_type(provider_value) else {
        return false;
    };
    let Some(requested_type) = normalize_segment_provider_media_type(requested_value) else {
        return false;
    };
    provider_type == requested_type || (provider_type == "series" && requested_type == "anime")
}

fn segment_provider_supports_media_type(
    selected: &MarketplaceSegmentProviderSelection,
    requested_value: &str,
) -> bool {
    selected
        .media_types
        .iter()
        .any(|provider_value| segment_provider_media_type_matches(provider_value, requested_value))
}

fn normalize_segment_provider_media_type(value: &str) -> Option<&'static str> {
    match value.trim().to_ascii_lowercase().as_str() {
        "movie" | "movies" => Some("movie"),
        "series" | "tv" | "show" | "shows" | "episode" | "episodes" => Some("series"),
        "anime" => Some("anime"),
        _ => None,
    }
}

fn segment_provider_supports_segment_type(
    selected: &MarketplaceSegmentProviderSelection,
    segment_type: &str,
) -> bool {
    selected
        .segment_types
        .iter()
        .any(|value| value == segment_type)
}

fn marketplace_segment_candidate_provider_id(
    selected: &MarketplaceSegmentProviderSelection,
    value: &Value,
) -> String {
    value
        .get("provider_id")
        .or_else(|| value.get("providerId"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| selected.provider.provider_id.to_string())
}

fn marketplace_segment_provider_version(
    selected: &MarketplaceSegmentProviderSelection,
    value: &Value,
) -> Option<String> {
    value
        .get("provider_version")
        .or_else(|| value.get("providerVersion"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| Some(selected.extension.version.clone()))
}

fn marketplace_segment_identity_strength(value: &Value) -> String {
    let raw = value
        .get("identity_strength")
        .or_else(|| value.get("identityStrength"))
        .and_then(Value::as_str)
        .unwrap_or("metadata_fuzzy")
        .trim()
        .to_ascii_lowercase()
        .replace('-', "_");
    match raw.as_str() {
        "external_id_exact" | "external_id_episode" | "metadata_fuzzy" | "unknown" => raw,
        _ => "metadata_fuzzy".to_string(),
    }
}

fn marketplace_segment_provider_certification_media_types(
    selected: &MarketplaceSegmentProviderSelection,
) -> Vec<String> {
    let mut media_types = BTreeSet::new();
    for media_type in &selected.media_types {
        if let Some(normalized) = normalize_segment_provider_media_type(media_type) {
            media_types.insert(normalized.to_string());
        }
    }
    ["movie", "series", "anime"]
        .iter()
        .filter(|media_type| media_types.contains(**media_type))
        .map(|media_type| (*media_type).to_string())
        .collect()
}

fn media_segment_provider_certification_context(media_type: &str) -> ProviderMediaContext {
    match media_type {
        "movie" => ProviderMediaContext {
            media_file_id: "certification-movie-file".to_string(),
            item_type: "movie".to_string(),
            item_id: "certification-movie".to_string(),
            imdb_id: Some("tt0000001".to_string()),
            tmdb_id: Some("1".to_string()),
            tvdb_id: None,
            anilist_id: None,
            mal_id: None,
            season_number: None,
            episode_number: None,
            absolute_episode_number: None,
            duration_seconds: Some(1_800.0),
        },
        "anime" => ProviderMediaContext {
            media_file_id: "certification-anime-file".to_string(),
            item_type: "episode".to_string(),
            item_id: "certification-anime-episode".to_string(),
            imdb_id: None,
            tmdb_id: None,
            tvdb_id: None,
            anilist_id: Some("1".to_string()),
            mal_id: Some("1".to_string()),
            season_number: Some(1),
            episode_number: Some(1),
            absolute_episode_number: Some(1),
            duration_seconds: Some(1_500.0),
        },
        _ => ProviderMediaContext {
            media_file_id: "certification-series-file".to_string(),
            item_type: "episode".to_string(),
            item_id: "certification-series-episode".to_string(),
            imdb_id: Some("tt0000002".to_string()),
            tmdb_id: None,
            tvdb_id: Some("2".to_string()),
            anilist_id: None,
            mal_id: None,
            season_number: Some(1),
            episode_number: Some(1),
            absolute_episode_number: Some(1),
            duration_seconds: Some(1_500.0),
        },
    }
}

fn media_segment_provider_certification_summary(
    status: &str,
    probes: &[MediaSegmentProviderCertificationProbe],
) -> String {
    let certified = probes
        .iter()
        .filter(|probe| probe.status == "certified")
        .count();
    let broken = probes.len().saturating_sub(certified);
    match status {
        "certified" => format!("{certified}/{} probe(s) certified", probes.len()),
        _ => format!("{broken}/{} probe(s) failed certification", probes.len()),
    }
}

fn media_segment_provider_certification_media_type_results(
    probes: &[MediaSegmentProviderCertificationProbe],
) -> Value {
    let mut object = serde_json::Map::new();
    for probe in probes {
        object.insert(
            probe.media_type.clone(),
            json!({
                "status": probe.status,
                "failure_class": probe.failure_class,
                "summary": probe.summary,
                "segment_count": probe.segment_count,
            }),
        );
    }
    Value::Object(object)
}

fn media_segment_provider_certification_segment_type_results(
    probes: &[MediaSegmentProviderCertificationProbe],
) -> Value {
    let mut counts = BTreeMap::<String, usize>::new();
    for probe in probes {
        for (segment_type, count) in &probe.segment_type_counts {
            *counts.entry(segment_type.clone()).or_insert(0) += *count;
        }
    }
    json!(counts)
}

fn media_segment_provider_certification_probe_targets(
    probes: &[MediaSegmentProviderCertificationProbe],
) -> Value {
    Value::Array(
        probes
            .iter()
            .map(|probe| {
                json!({
                    "media_type": probe.media_type,
                    "status": probe.status,
                })
            })
            .collect(),
    )
}

fn classify_media_segment_provider_certification_error(err: &anyhow::Error) -> String {
    let message = err.to_string().to_ascii_lowercase();
    if message.contains("response exceeds") {
        "response_too_large".to_string()
    } else if message.contains("timeout") || message.contains("timed out") {
        "timeout".to_string()
    } else if message.contains("not reachable") || message.contains("connection") {
        "network_blocked".to_string()
    } else if message.contains("returned 4") || message.contains("returned 5") {
        "provider_http_error".to_string()
    } else {
        "provider_probe_failed".to_string()
    }
}

fn normalize_media_segment_provider_certification_status(value: &str) -> Result<String> {
    let normalized = value.trim().to_ascii_lowercase().replace('-', "_");
    if matches!(
        normalized.as_str(),
        "certified" | "degraded" | "broken" | "unsupported" | "unknown"
    ) {
        Ok(normalized)
    } else {
        bail!("invalid media segment provider certification status");
    }
}

async fn upsert_media_segment_provider_certification(
    pool: &AnyPool,
    certification_id: Uuid,
    provider_id: Uuid,
    instance_id: Uuid,
    provider_kind: &str,
    status: &str,
    failure_class: Option<&str>,
    summary: Option<&str>,
    media_type_results: &Value,
    segment_type_results: &Value,
    probe_targets: &Value,
    response_evidence: &Value,
    runtime_version: Option<&str>,
    policy_version: &str,
    certified_at: Option<&str>,
    expires_at: Option<&str>,
) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "INSERT INTO media_segment_provider_certifications
            (certification_id, provider_id, instance_id, provider_kind, status, failure_class,
             summary, media_type_results_json, segment_type_results_json, probe_targets_json,
             response_evidence_json, runtime_version, policy_version, certified_at, expires_at,
             created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
         ON CONFLICT(provider_id, policy_version) DO UPDATE SET
             certification_id = excluded.certification_id,
             instance_id = excluded.instance_id,
             provider_kind = excluded.provider_kind,
             status = excluded.status,
             failure_class = excluded.failure_class,
             summary = excluded.summary,
             media_type_results_json = excluded.media_type_results_json,
             segment_type_results_json = excluded.segment_type_results_json,
             probe_targets_json = excluded.probe_targets_json,
             response_evidence_json = excluded.response_evidence_json,
             runtime_version = excluded.runtime_version,
             certified_at = excluded.certified_at,
             expires_at = excluded.expires_at,
             updated_at = CURRENT_TIMESTAMP",
    )
    .bind(certification_id.to_string())
    .bind(provider_id.to_string())
    .bind(instance_id.to_string())
    .bind(provider_kind)
    .bind(status)
    .bind(failure_class)
    .bind(summary)
    .bind(media_type_results.to_string())
    .bind(segment_type_results.to_string())
    .bind(probe_targets.to_string())
    .bind(response_evidence.to_string())
    .bind(runtime_version)
    .bind(policy_version)
    .bind(certified_at)
    .bind(expires_at)
    .execute(pool)
    .await
    .context("upserting media segment provider certification")?;
    Ok(())
}

async fn latest_media_segment_provider_certification(
    pool: &AnyPool,
    provider_id: Uuid,
    policy_version: &str,
) -> Result<Option<MediaSegmentProviderCertificationRecord>> {
    let row = sqlx::query::<sqlx::Any>(
        "SELECT certification_id, provider_id, instance_id, provider_kind, status,
                failure_class, summary,
                CAST(media_type_results_json AS TEXT) AS media_type_results_json,
                CAST(segment_type_results_json AS TEXT) AS segment_type_results_json,
                CAST(probe_targets_json AS TEXT) AS probe_targets_json,
                CAST(response_evidence_json AS TEXT) AS response_evidence_json,
                CAST(runtime_version AS TEXT) AS runtime_version,
                policy_version,
                CAST(certified_at AS TEXT) AS certified_at,
                CAST(expires_at AS TEXT) AS expires_at,
                CAST(created_at AS TEXT) AS created_at,
                CAST(updated_at AS TEXT) AS updated_at
         FROM media_segment_provider_certifications
         WHERE provider_id = $1
           AND policy_version = $2
         LIMIT 1",
    )
    .bind(provider_id.to_string())
    .bind(policy_version)
    .fetch_optional(pool)
    .await
    .context("loading media segment provider certification")?;

    row.as_ref()
        .map(media_segment_provider_certification_from_row)
        .transpose()
}

fn media_segment_provider_certification_from_row(
    row: &AnyRow,
) -> Result<MediaSegmentProviderCertificationRecord> {
    Ok(MediaSegmentProviderCertificationRecord {
        certification_id: row.get("certification_id"),
        provider_id: row.get("provider_id"),
        instance_id: row.get("instance_id"),
        provider_kind: row.get("provider_kind"),
        status: row.get("status"),
        failure_class: row_string(row, "failure_class"),
        summary: row_string(row, "summary"),
        media_type_results: row_json_value(row, "media_type_results_json")?,
        segment_type_results: row_json_value(row, "segment_type_results_json")?,
        probe_targets: row_json_value(row, "probe_targets_json")?,
        response_evidence: row_json_value(row, "response_evidence_json")?,
        runtime_version: row_string(row, "runtime_version"),
        policy_version: row.get("policy_version"),
        certified_at: row_string(row, "certified_at"),
        expires_at: row_string(row, "expires_at"),
        created_at: row_string(row, "created_at"),
        updated_at: row_string(row, "updated_at"),
    })
}

fn provider_skipped(provider_kind: &str, reason: &str) -> ProviderLookupResult {
    ProviderLookupResult {
        outcome: BuiltinProviderRefreshOutcome {
            provider_kind: provider_kind.to_string(),
            enabled: true,
            status: "skipped".to_string(),
            cache_hit: false,
            candidate_count: 0,
            accepted_count: 0,
            rejected_count: 0,
            reason: Some(reason.to_string()),
        },
        response: None,
    }
}

struct CachedProviderResponse {
    status: String,
    response: Option<Value>,
    reason: Option<String>,
}

async fn load_provider_cache(
    pool: &AnyPool,
    provider_kind: &str,
    cache_key: &str,
) -> Result<Option<CachedProviderResponse>> {
    let row = sqlx::query::<sqlx::Any>(
        "SELECT status, response_json, error_json
         FROM media_segment_provider_cache
         WHERE provider_kind = $1
           AND provider_cache_key = $2
           AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
         LIMIT 1",
    )
    .bind(provider_kind)
    .bind(cache_key)
    .fetch_optional(pool)
    .await
    .context("loading media segment provider cache")?;

    Ok(row.map(|row| {
        let response = row
            .try_get::<String, _>("response_json")
            .ok()
            .and_then(|raw| serde_json::from_str::<Value>(&raw).ok());
        let reason = row
            .try_get::<String, _>("error_json")
            .ok()
            .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
            .and_then(|value| value.get("error").or_else(|| value.get("status")).cloned())
            .map(|value| value.to_string());
        CachedProviderResponse {
            status: row.get("status"),
            response,
            reason,
        }
    }))
}

async fn upsert_provider_cache(
    pool: &AnyPool,
    context: &ProviderMediaContext,
    provider_kind: &str,
    cache_key: &str,
    status: &str,
    response: Option<Value>,
    error: Option<Value>,
    ttl_seconds: i64,
) -> Result<()> {
    let expires_at = (Utc::now() + ChronoDuration::seconds(ttl_seconds.max(0)))
        .format("%Y-%m-%d %H:%M:%S")
        .to_string();
    sqlx::query::<sqlx::Any>(
        "INSERT INTO media_segment_provider_cache
            (id, media_file_id, item_type, item_id, provider_kind, provider_cache_key,
             status, response_json, error_json, fetched_at, expires_at, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CURRENT_TIMESTAMP, $10, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
         ON CONFLICT(provider_kind, provider_cache_key) DO UPDATE SET
             media_file_id = excluded.media_file_id,
             item_type = excluded.item_type,
             item_id = excluded.item_id,
             status = excluded.status,
             response_json = excluded.response_json,
             error_json = excluded.error_json,
             fetched_at = CURRENT_TIMESTAMP,
             expires_at = excluded.expires_at,
             updated_at = CURRENT_TIMESTAMP",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(&context.media_file_id)
    .bind(&context.item_type)
    .bind(&context.item_id)
    .bind(provider_kind)
    .bind(cache_key)
    .bind(status)
    .bind(response.as_ref().map(Value::to_string))
    .bind(error.as_ref().map(Value::to_string))
    .bind(expires_at)
    .execute(pool)
    .await
    .context("upserting media segment provider cache")?;
    Ok(())
}

async fn acquire_provider_rate_limit(
    pool: &AnyPool,
    provider_kind: &str,
    settings: Option<&Value>,
) -> Result<bool> {
    let limit = provider_rate_limit_per_minute(settings);
    let now = timestamp_now();
    let active_window_cutoff = timestamp_after_seconds(-PROVIDER_RATE_LIMIT_WINDOW_SECONDS);

    let row = sqlx::query::<sqlx::Any>(
        "SELECT CAST(window_started_at AS TEXT) AS window_started_at, requests_in_window
         FROM media_segment_provider_rate_limits
         WHERE provider_kind = $1
         LIMIT 1",
    )
    .bind(provider_kind)
    .fetch_optional(pool)
    .await
    .context("loading media segment provider rate limit")?;

    if let Some(row) = row {
        let window_started_at = row_string(&row, "window_started_at").unwrap_or_default();
        let requests_in_window = row.try_get::<i64, _>("requests_in_window").unwrap_or(0);
        if window_started_at >= active_window_cutoff {
            if requests_in_window >= limit {
                return Ok(false);
            }

            let updated = sqlx::query::<sqlx::Any>(
                "UPDATE media_segment_provider_rate_limits
                 SET requests_in_window = requests_in_window + 1,
                     updated_at = CURRENT_TIMESTAMP
                 WHERE provider_kind = $1
                   AND window_started_at = $2
                   AND requests_in_window < $3",
            )
            .bind(provider_kind)
            .bind(&window_started_at)
            .bind(limit)
            .execute(pool)
            .await
            .context("incrementing media segment provider rate limit")?;
            return Ok(updated.rows_affected() == 1);
        }
    }

    sqlx::query::<sqlx::Any>(
        "INSERT INTO media_segment_provider_rate_limits
            (provider_kind, window_started_at, requests_in_window, updated_at)
         VALUES ($1, $2, 1, CURRENT_TIMESTAMP)
         ON CONFLICT(provider_kind) DO UPDATE SET
             window_started_at = excluded.window_started_at,
             requests_in_window = 1,
             updated_at = CURRENT_TIMESTAMP",
    )
    .bind(provider_kind)
    .bind(now)
    .execute(pool)
    .await
    .context("resetting media segment provider rate limit")?;

    Ok(true)
}

async fn enqueue_media_segment_job(
    pool: &AnyPool,
    job_type: &str,
    scope_type: &str,
    scope_id: &str,
    provider_kind: &str,
    priority: i64,
    max_attempts: i64,
) -> Result<MediaSegmentJobRecord> {
    let job_id = media_segment_job_id(job_type, scope_type, scope_id, provider_kind);
    sqlx::query::<sqlx::Any>(
        "INSERT INTO media_segment_jobs
            (id, job_type, scope_type, scope_id, provider_kind, status, priority, attempts,
             max_attempts, next_attempt_at, locked_by, started_at, finished_at, error_json,
             created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, 'queued', $6, 0, $7, CURRENT_TIMESTAMP, NULL, NULL, NULL, NULL,
                 CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
         ON CONFLICT(id) DO UPDATE SET
             job_type = excluded.job_type,
             scope_type = excluded.scope_type,
             scope_id = excluded.scope_id,
             provider_kind = excluded.provider_kind,
             status = CASE
                 WHEN media_segment_jobs.status = 'running' THEN media_segment_jobs.status
                 ELSE 'queued'
             END,
             priority = excluded.priority,
             attempts = CASE
                 WHEN media_segment_jobs.status = 'running' THEN media_segment_jobs.attempts
                 ELSE 0
             END,
             max_attempts = excluded.max_attempts,
             next_attempt_at = CASE
                 WHEN media_segment_jobs.status = 'running' THEN media_segment_jobs.next_attempt_at
                 ELSE CURRENT_TIMESTAMP
             END,
             locked_by = CASE
                 WHEN media_segment_jobs.status = 'running' THEN media_segment_jobs.locked_by
                 ELSE NULL
             END,
             started_at = CASE
                 WHEN media_segment_jobs.status = 'running' THEN media_segment_jobs.started_at
                 ELSE NULL
             END,
             finished_at = CASE
                 WHEN media_segment_jobs.status = 'running' THEN media_segment_jobs.finished_at
                 ELSE NULL
             END,
             error_json = CASE
                 WHEN media_segment_jobs.status = 'running' THEN media_segment_jobs.error_json
                 ELSE NULL
             END,
             updated_at = CURRENT_TIMESTAMP",
    )
    .bind(&job_id)
    .bind(job_type)
    .bind(scope_type)
    .bind(scope_id)
    .bind(provider_kind)
    .bind(priority.clamp(0, 10_000))
    .bind(max_attempts.max(1))
    .execute(pool)
    .await
    .context("upserting media segment job")?;

    let job = load_media_segment_job(pool, &job_id)
        .await?
        .context("queued media segment job was not found")?;
    if job.status != "running" {
        record_media_segment_job_status(&job);
    }
    refresh_media_segment_job_backlog_metrics(pool).await;
    Ok(job)
}

async fn load_media_segment_job(
    pool: &AnyPool,
    job_id: &str,
) -> Result<Option<MediaSegmentJobRecord>> {
    let row = sqlx::query::<sqlx::Any>(
        "SELECT id, job_type, scope_type, scope_id, provider_kind, status, priority, attempts,
                max_attempts,
                CAST(next_attempt_at AS TEXT) AS next_attempt_at,
                locked_by,
                CAST(started_at AS TEXT) AS started_at,
                CAST(finished_at AS TEXT) AS finished_at,
                error_json
         FROM media_segment_jobs
         WHERE id = $1
         LIMIT 1",
    )
    .bind(job_id)
    .fetch_optional(pool)
    .await
    .context("loading media segment job")?;

    Ok(row.as_ref().map(media_segment_job_from_row))
}

async fn finish_media_segment_job(
    pool: &AnyPool,
    job_id: &str,
    status: &str,
    error: Option<Value>,
) -> Result<Option<MediaSegmentJobRecord>> {
    ensure_terminal_job_status(status)?;
    let result = sqlx::query::<sqlx::Any>(
        "UPDATE media_segment_jobs
         SET status = $1,
             locked_by = NULL,
             next_attempt_at = NULL,
             finished_at = CURRENT_TIMESTAMP,
             error_json = $2,
             updated_at = CURRENT_TIMESTAMP
         WHERE id = $3
           AND (
               ($4 = 'cancelled' AND status <> 'succeeded')
               OR ($5 <> 'cancelled' AND status <> 'cancelled')
           )",
    )
    .bind(status)
    .bind(error.as_ref().map(Value::to_string))
    .bind(job_id)
    .bind(status)
    .bind(status)
    .execute(pool)
    .await
    .context("finishing media segment job")?;

    let job = load_media_segment_job(pool, job_id).await?;
    if result.rows_affected() == 1
        && let Some(job) = job.as_ref()
    {
        record_media_segment_job_status(job);
        record_media_segment_job_duration(job);
    }
    refresh_media_segment_job_backlog_metrics(pool).await;
    Ok(job)
}

async fn retry_or_fail_media_segment_job(
    pool: &AnyPool,
    job_id: &str,
    error: Value,
) -> Result<Option<MediaSegmentJobRecord>> {
    let current = load_media_segment_job(pool, job_id)
        .await?
        .context("media segment job was not found for retry")?;
    if current.status != "running" {
        return Ok(Some(current));
    }

    let terminal = current.attempts >= current.max_attempts;
    let status = if terminal { "failed" } else { "queued" };
    let next_attempt_at = if terminal {
        None
    } else {
        Some(timestamp_after_seconds(
            PROVIDER_JOB_RETRY_BACKOFF_SECONDS * current.attempts.max(1),
        ))
    };
    let finished_at = terminal.then(timestamp_now);

    sqlx::query::<sqlx::Any>(
        "UPDATE media_segment_jobs
         SET status = $1,
             locked_by = NULL,
             next_attempt_at = $2,
             finished_at = $3,
             error_json = $4,
             updated_at = CURRENT_TIMESTAMP
         WHERE id = $5",
    )
    .bind(status)
    .bind(next_attempt_at.as_deref())
    .bind(finished_at.as_deref())
    .bind(Some(error.to_string()))
    .bind(job_id)
    .execute(pool)
    .await
    .context("retrying or failing media segment job")?;

    let job = load_media_segment_job(pool, job_id).await?;
    if let Some(job) = job.as_ref() {
        record_media_segment_job_status(job);
        record_media_segment_job_duration(job);
    }
    refresh_media_segment_job_backlog_metrics(pool).await;
    Ok(job)
}

fn media_segment_job_from_row(row: &AnyRow) -> MediaSegmentJobRecord {
    MediaSegmentJobRecord {
        id: row.get("id"),
        job_type: row.get("job_type"),
        scope_type: row.get("scope_type"),
        scope_id: row.get("scope_id"),
        provider_kind: row.get("provider_kind"),
        status: row.get("status"),
        priority: row.try_get::<i64, _>("priority").unwrap_or(100),
        attempts: row.try_get::<i64, _>("attempts").unwrap_or(0),
        max_attempts: row.try_get::<i64, _>("max_attempts").unwrap_or(1),
        next_attempt_at: row_string(row, "next_attempt_at"),
        locked_by: row_string(row, "locked_by"),
        started_at: row_string(row, "started_at"),
        finished_at: row_string(row, "finished_at"),
        error: row_string(row, "error_json").and_then(|raw| serde_json::from_str(&raw).ok()),
    }
}

fn media_segment_job_id(
    job_type: &str,
    scope_type: &str,
    scope_id: &str,
    provider_kind: &str,
) -> String {
    let key = format!(
        "media_segment_job:{job_type}:{scope_type}:{scope_id}:{}",
        provider_kind.trim().to_ascii_lowercase()
    );
    Uuid::new_v5(&Uuid::NAMESPACE_OID, key.as_bytes()).to_string()
}

fn normalize_worker_id(worker_id: &str) -> Result<String> {
    let normalized = worker_id.trim();
    if normalized.is_empty() {
        bail!("media segment job worker_id must not be empty");
    }
    Ok(normalized.chars().take(80).collect())
}

fn normalize_media_segment_job_identifier_filter(value: &str, field: &str) -> Result<String> {
    Ok(normalize_required_text(value, field)?
        .to_ascii_lowercase()
        .replace('-', "_"))
}

fn normalize_media_segment_job_status_filter(value: &str) -> Result<String> {
    let status = normalize_media_segment_job_identifier_filter(value, "status")?;
    if matches!(
        status.as_str(),
        "queued" | "running" | "succeeded" | "skipped" | "failed" | "cancelled"
    ) {
        Ok(status)
    } else {
        bail!("invalid media segment job status filter");
    }
}

fn normalize_optional_candidate_item_type(value: &str) -> Result<String> {
    let item_type = normalize_required_text(value, "item_type")?
        .to_ascii_lowercase()
        .replace('-', "_");
    if matches!(item_type.as_str(), "movie" | "episode" | "tv" | "anime") {
        Ok(item_type)
    } else {
        bail!("invalid media segment candidate item_type filter");
    }
}

fn normalize_candidate_segment_filter(value: &str) -> Result<String> {
    let segment_type = normalize_required_text(value, "segment_type")?
        .to_ascii_lowercase()
        .replace('-', "_");
    if allowed_segment_type(&segment_type) {
        Ok(segment_type)
    } else {
        bail!("invalid media segment candidate segment_type filter");
    }
}

fn normalize_candidate_validation_state_filter(value: &str) -> Result<String> {
    let state = normalize_required_text(value, "validation_state")?
        .to_ascii_lowercase()
        .replace('-', "_");
    if matches!(state.as_str(), "pending" | "accepted" | "rejected") {
        Ok(state)
    } else {
        bail!("invalid media segment candidate validation_state filter");
    }
}

fn normalize_candidate_validation_reason_filter(value: &str) -> Result<String> {
    let reason = normalize_required_text(value, "validation_reason")?
        .to_ascii_lowercase()
        .replace('-', "_");
    if reason
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_')
    {
        Ok(reason)
    } else {
        bail!("invalid media segment candidate validation_reason filter");
    }
}

fn ensure_terminal_job_status(status: &str) -> Result<()> {
    if matches!(status, "succeeded" | "skipped" | "failed" | "cancelled") {
        Ok(())
    } else {
        bail!("invalid terminal media segment job status");
    }
}

fn is_terminal_media_segment_job_status(status: &str) -> bool {
    matches!(status, "succeeded" | "skipped" | "failed" | "cancelled")
}

fn record_media_segment_job_status(job: &MediaSegmentJobRecord) {
    metrics::MEDIA_SEGMENT_JOBS
        .with_label_values(&[&job.provider_kind, &job.job_type, &job.status])
        .inc();
}

fn record_media_segment_job_duration(job: &MediaSegmentJobRecord) {
    if !is_terminal_media_segment_job_status(&job.status) {
        return;
    }

    let (Some(started_at), Some(finished_at)) = (&job.started_at, &job.finished_at) else {
        return;
    };
    let (Some(started_at), Some(finished_at)) = (
        parse_media_segment_job_timestamp(started_at),
        parse_media_segment_job_timestamp(finished_at),
    ) else {
        return;
    };

    let seconds = (finished_at - started_at).num_milliseconds() as f64 / 1000.0;
    if seconds.is_sign_negative() {
        return;
    }

    metrics::MEDIA_SEGMENT_JOB_DURATION
        .with_label_values(&[&job.provider_kind, &job.job_type, &job.status])
        .observe(seconds);
}

fn record_media_segment_candidate(provider_kind: &str, validation_state: &str) {
    metrics::MEDIA_SEGMENT_CANDIDATES
        .with_label_values(&[provider_kind, validation_state])
        .inc();
}

async fn refresh_active_media_segment_metrics(pool: &AnyPool) {
    if let Err(err) = update_active_media_segment_metrics(pool).await {
        tracing::warn!("refreshing active media segment metrics failed: {err}");
    }
}

async fn update_active_media_segment_metrics(pool: &AnyPool) -> Result<()> {
    let rows = sqlx::query::<sqlx::Any>(
        "SELECT segment_type, source_label, COUNT(*) AS active_count
         FROM media_segments
         WHERE status = 'active'
         GROUP BY segment_type, source_label",
    )
    .fetch_all(pool)
    .await
    .context("counting active media segments")?;

    let mut current = BTreeMap::<(String, String), i64>::new();
    for row in rows {
        let segment_type: String = row.get("segment_type");
        let source_label: String = row.get("source_label");
        let active_count = row.try_get::<i64, _>("active_count").unwrap_or(0);
        current.insert((segment_type, source_label), active_count);
    }

    let mut labels = MEDIA_SEGMENTS_ACTIVE_LABELS
        .lock()
        .map_err(|_| anyhow::anyhow!("active media segment metric label cache poisoned"))?;
    for (segment_type, source_label) in labels.iter() {
        if !current.contains_key(&(segment_type.clone(), source_label.clone())) {
            metrics::MEDIA_SEGMENTS_ACTIVE
                .with_label_values(&[segment_type, source_label])
                .set(0);
        }
    }
    for ((segment_type, source_label), active_count) in &current {
        metrics::MEDIA_SEGMENTS_ACTIVE
            .with_label_values(&[segment_type, source_label])
            .set(*active_count);
    }
    *labels = current.keys().cloned().collect();

    Ok(())
}

async fn refresh_media_segment_job_backlog_metrics(pool: &AnyPool) {
    if let Err(err) = update_media_segment_job_backlog_metrics(pool).await {
        tracing::warn!("refreshing media segment job backlog metrics failed: {err}");
    }
}

async fn update_media_segment_job_backlog_metrics(pool: &AnyPool) -> Result<()> {
    for status in ["queued", "running", "failed"] {
        metrics::MEDIA_SEGMENT_JOB_BACKLOG
            .with_label_values(&[status])
            .set(0);
    }

    let rows = sqlx::query::<sqlx::Any>(
        "SELECT status, COUNT(*) AS job_count
         FROM media_segment_jobs
         WHERE status IN ('queued', 'running', 'failed')
         GROUP BY status",
    )
    .fetch_all(pool)
    .await
    .context("counting media segment job backlog")?;

    for row in rows {
        let status: String = row.get("status");
        if !matches!(status.as_str(), "queued" | "running" | "failed") {
            continue;
        }
        let job_count = row.try_get::<i64, _>("job_count").unwrap_or(0);
        metrics::MEDIA_SEGMENT_JOB_BACKLOG
            .with_label_values(&[&status])
            .set(job_count);
    }

    Ok(())
}

fn parse_media_segment_job_timestamp(value: &str) -> Option<DateTime<Utc>> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }

    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .ok()
        .or_else(|| {
            DateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f%:z")
                .map(|value| value.with_timezone(&Utc))
                .ok()
        })
        .or_else(|| {
            DateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f%z")
                .map(|value| value.with_timezone(&Utc))
                .ok()
        })
        .or_else(|| {
            NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
                .map(|value| DateTime::<Utc>::from_naive_utc_and_offset(value, Utc))
                .ok()
        })
}

async fn load_provider_media_context(
    pool: &AnyPool,
    media_file_id: &str,
) -> Result<ProviderMediaContext> {
    let duration_seconds = load_media_duration_seconds(pool, media_file_id).await?;

    if let Some(row) = sqlx::query::<sqlx::Any>(
        "SELECT m.id AS movie_id, m.external_imdb, m.external_tmdb
         FROM movie_files mf
         JOIN movies m ON m.id = mf.movie_id
         WHERE mf.media_file_id = $1
         LIMIT 1",
    )
    .bind(media_file_id)
    .fetch_optional(pool)
    .await
    .context("loading movie provider context")?
    {
        let movie_id: String = row.get("movie_id");
        let imdb_id = row
            .try_get::<String, _>("external_imdb")
            .ok()
            .and_then(|value| normalize_imdb_id(&value))
            .or(load_movie_external_id(pool, &movie_id, &["imdb", "imdb_id"]).await?);
        let tmdb_id = row
            .try_get::<String, _>("external_tmdb")
            .ok()
            .and_then(|value| normalize_numeric_id(&value))
            .or(load_movie_external_id(pool, &movie_id, &["tmdb", "tmdb_id"]).await?);
        return Ok(ProviderMediaContext {
            media_file_id: media_file_id.to_string(),
            item_type: "movie".to_string(),
            item_id: movie_id,
            imdb_id,
            tmdb_id,
            tvdb_id: None,
            anilist_id: None,
            mal_id: None,
            season_number: None,
            episode_number: None,
            absolute_episode_number: None,
            duration_seconds,
        });
    }

    let row = sqlx::query::<sqlx::Any>(
        "SELECT e.id AS episode_id, e.series_id, e.season_number, e.episode_number,
                e.absolute_episode_number, s.external_imdb, s.external_tvdb_series,
                s.external_anilist
         FROM episode_files ef
         JOIN episodes e ON e.id = ef.episode_id
         JOIN series s ON s.id = e.series_id
         WHERE ef.media_file_id = $1
         LIMIT 1",
    )
    .bind(media_file_id)
    .fetch_optional(pool)
    .await
    .context("loading episode provider context")?
    .context("media file is not linked to a movie or episode")?;

    let episode_id: String = row.get("episode_id");
    let series_id: String = row.get("series_id");
    let imdb_id = row
        .try_get::<String, _>("external_imdb")
        .ok()
        .and_then(|value| normalize_imdb_id(&value))
        .or(load_series_external_id(pool, &series_id, &["imdb", "imdb_id"]).await?);
    let tvdb_id = row
        .try_get::<String, _>("external_tvdb_series")
        .ok()
        .and_then(|value| normalize_numeric_id(&value))
        .or(load_series_external_id(pool, &series_id, &["tvdb", "tvdb_series", "thetvdb"]).await?);
    let anilist_id = row
        .try_get::<String, _>("external_anilist")
        .ok()
        .and_then(|value| normalize_numeric_id(&value))
        .or(load_series_external_id(pool, &series_id, &["anilist", "ani_list"]).await?);
    let mal_id = load_episode_external_id(pool, &episode_id, &["mal", "myanimelist"])
        .await?
        .or(load_series_external_id(pool, &series_id, &["mal", "myanimelist"]).await?);

    Ok(ProviderMediaContext {
        media_file_id: media_file_id.to_string(),
        item_type: "episode".to_string(),
        item_id: episode_id,
        imdb_id,
        tmdb_id: None,
        tvdb_id,
        anilist_id,
        mal_id,
        season_number: row
            .try_get::<i64, _>("season_number")
            .ok()
            .map(|value| value as i32),
        episode_number: row
            .try_get::<i64, _>("episode_number")
            .ok()
            .map(|value| value as i32),
        absolute_episode_number: row
            .try_get::<i64, _>("absolute_episode_number")
            .ok()
            .map(|value| value as i32),
        duration_seconds,
    })
}

async fn load_movie_external_id(
    pool: &AnyPool,
    movie_id: &str,
    providers: &[&str],
) -> Result<Option<String>> {
    for provider in providers {
        if let Some(value) = sqlx::query_scalar::<sqlx::Any, String>(
            "SELECT external_id FROM movie_external_ids
             WHERE movie_id = $1 AND provider = $2
             ORDER BY COALESCE(confidence, 0) DESC, created_at DESC
             LIMIT 1",
        )
        .bind(movie_id)
        .bind(*provider)
        .fetch_optional(pool)
        .await
        .context("loading movie external id")?
            && let Some(normalized) = normalize_external_id_for_provider(provider, &value)
        {
            return Ok(Some(normalized));
        }
    }
    Ok(None)
}

async fn load_series_external_id(
    pool: &AnyPool,
    series_id: &str,
    providers: &[&str],
) -> Result<Option<String>> {
    for provider in providers {
        if let Some(value) = sqlx::query_scalar::<sqlx::Any, String>(
            "SELECT external_id FROM series_external_ids
             WHERE series_id = $1 AND provider = $2
             ORDER BY COALESCE(confidence, 0) DESC, created_at DESC
             LIMIT 1",
        )
        .bind(series_id)
        .bind(*provider)
        .fetch_optional(pool)
        .await
        .context("loading series external id")?
            && let Some(normalized) = normalize_external_id_for_provider(provider, &value)
        {
            return Ok(Some(normalized));
        }
    }
    Ok(None)
}

async fn load_episode_external_id(
    pool: &AnyPool,
    episode_id: &str,
    providers: &[&str],
) -> Result<Option<String>> {
    for provider in providers {
        if let Some(value) = sqlx::query_scalar::<sqlx::Any, String>(
            "SELECT external_id FROM episode_external_ids
             WHERE episode_id = $1 AND provider = $2
             ORDER BY COALESCE(confidence, 0) DESC, created_at DESC
             LIMIT 1",
        )
        .bind(episode_id)
        .bind(*provider)
        .fetch_optional(pool)
        .await
        .context("loading episode external id")?
            && let Some(normalized) = normalize_external_id_for_provider(provider, &value)
        {
            return Ok(Some(normalized));
        }
    }
    Ok(None)
}

fn normalize_external_id_for_provider(provider: &str, value: &str) -> Option<String> {
    match provider {
        "imdb" | "imdb_id" => normalize_imdb_id(value),
        _ => normalize_numeric_id(value),
    }
}

fn normalize_imdb_id(value: &str) -> Option<String> {
    let normalized = value.trim().to_ascii_lowercase();
    if normalized.starts_with("tt")
        && normalized.len() >= 4
        && normalized.chars().skip(2).all(|ch| ch.is_ascii_digit())
    {
        Some(normalized)
    } else {
        None
    }
}

fn normalize_numeric_id(value: &str) -> Option<String> {
    let normalized = value.trim();
    if normalized.is_empty() || !normalized.chars().all(|ch| ch.is_ascii_digit()) {
        None
    } else {
        Some(normalized.to_string())
    }
}

fn provider_settings_for(settings: &Value, provider_kind: &str) -> Option<Value> {
    settings
        .as_object()
        .and_then(|object| object.get(provider_kind))
        .cloned()
}

fn local_audio_min_repeat_count(settings: Option<&Value>) -> usize {
    settings
        .and_then(|value| {
            value
                .get("min_repeat_count")
                .or_else(|| value.get("minRepeatCount"))
        })
        .and_then(Value::as_u64)
        .filter(|value| (2..=20).contains(value))
        .map(|value| value as usize)
        .unwrap_or(LOCAL_AUDIO_DETECTOR_MIN_REPEAT_COUNT)
}

fn local_audio_min_season_files(settings: Option<&Value>) -> usize {
    settings
        .and_then(|value| {
            value
                .get("min_season_files")
                .or_else(|| value.get("minSeasonFiles"))
        })
        .and_then(Value::as_u64)
        .filter(|value| (2..=50).contains(value))
        .map(|value| value as usize)
        .unwrap_or(LOCAL_AUDIO_DETECTOR_MIN_SEASON_FILES)
}

fn provider_settings_enabled(settings: Option<&Value>) -> bool {
    settings
        .and_then(|value| value.get("enabled"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn selected_builtin_provider_kinds(provider_kind: Option<&str>) -> Result<Vec<&'static str>> {
    let Some(provider_kind) = provider_kind
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(vec![PROVIDER_THEINTRODB, PROVIDER_ANISKIP]);
    };

    match provider_kind
        .to_ascii_lowercase()
        .replace('-', "_")
        .as_str()
    {
        "all" | "builtin" | "builtins" | "built_in" | "built_in_network" => {
            Ok(vec![PROVIDER_THEINTRODB, PROVIDER_ANISKIP])
        }
        value => Ok(vec![normalize_single_builtin_provider_kind(value)?]),
    }
}

fn normalize_single_builtin_provider_kind(provider_kind: &str) -> Result<&'static str> {
    match provider_kind
        .trim()
        .to_ascii_lowercase()
        .replace('-', "_")
        .as_str()
    {
        PROVIDER_THEINTRODB => Ok(PROVIDER_THEINTRODB),
        PROVIDER_ANISKIP => Ok(PROVIDER_ANISKIP),
        _ => bail!("unsupported built-in media segment provider"),
    }
}

fn provider_base_url(settings: Option<&Value>, default: &str) -> Result<String> {
    let raw = settings
        .and_then(|value| value.get("base_url").or_else(|| value.get("baseUrl")))
        .and_then(Value::as_str)
        .unwrap_or(default)
        .trim();
    if raw.starts_with("http://") || raw.starts_with("https://") {
        Ok(raw.to_string())
    } else {
        bail!("provider base_url must be an http(s) URL")
    }
}

fn provider_timeout(settings: Option<&Value>) -> StdDuration {
    settings
        .and_then(|value| value.get("timeout_ms").or_else(|| value.get("timeoutMs")))
        .and_then(Value::as_i64)
        .filter(|value| (250..=30_000).contains(value))
        .map(|value| StdDuration::from_millis(value as u64))
        .unwrap_or_else(|| StdDuration::from_secs(DEFAULT_PROVIDER_TIMEOUT_SECONDS))
}

fn provider_cache_ttl_seconds(settings: Option<&Value>) -> i64 {
    settings
        .and_then(|value| {
            value
                .get("cache_ttl_seconds")
                .or_else(|| value.get("cacheTtlSeconds"))
        })
        .and_then(Value::as_i64)
        .filter(|value| (0..=(60 * 60 * 24 * 30)).contains(value))
        .unwrap_or(DEFAULT_PROVIDER_CACHE_TTL_SECONDS)
}

fn provider_rate_limit_per_minute(settings: Option<&Value>) -> i64 {
    settings
        .and_then(|value| {
            value
                .get("rate_limit_per_minute")
                .or_else(|| value.get("rateLimitPerMinute"))
        })
        .and_then(Value::as_i64)
        .filter(|value| (1..=600).contains(value))
        .unwrap_or(DEFAULT_PROVIDER_RATE_LIMIT_PER_MINUTE)
}

fn redact_provider_url(url: &str) -> String {
    url.split('?').next().unwrap_or(url).to_string()
}

pub async fn list_media_interaction_library_settings(
    pool: &AnyPool,
) -> Result<Vec<MediaInteractionLibrarySettingsRecord>> {
    let rows = sqlx::query::<sqlx::Any>(
        "SELECT id, extension_id, CASE WHEN enabled THEN 1 ELSE 0 END AS source_enabled
         FROM source_configs
         ORDER BY extension_id ASC, id ASC",
    )
    .fetch_all(pool)
    .await
    .context("listing media interaction library settings")?;

    let mut records = Vec::with_capacity(rows.len());
    for row in rows {
        let source_config_id: String = row.get("id");
        records.push(
            media_interaction_library_settings_record_from_source_row(
                pool,
                &source_config_id,
                &row,
            )
            .await?,
        );
    }
    Ok(records)
}

pub async fn load_media_interaction_library_settings(
    pool: &AnyPool,
    source_config_id: &str,
) -> Result<MediaInteractionLibrarySettingsRecord> {
    let source_config_id = normalize_required_text(source_config_id, "source_config_id")?;
    let row = sqlx::query::<sqlx::Any>(
        "SELECT id, extension_id, CASE WHEN enabled THEN 1 ELSE 0 END AS source_enabled
         FROM source_configs
         WHERE id = $1
         LIMIT 1",
    )
    .bind(&source_config_id)
    .fetch_optional(pool)
    .await
    .context("loading media interaction library source")?
    .context("source config not found")?;

    media_interaction_library_settings_record_from_source_row(pool, &source_config_id, &row).await
}

pub async fn update_media_interaction_library_settings(
    pool: &AnyPool,
    source_config_id: &str,
    patch: MediaInteractionLibrarySettingsPatch,
) -> Result<MediaInteractionLibrarySettingsRecord> {
    let source_config_id = normalize_required_text(source_config_id, "source_config_id")?;
    ensure_source_config_exists(pool, &source_config_id).await?;

    if let Some(segment_provider_settings) = patch.segment_provider_settings {
        let patch_object = segment_provider_settings
            .as_object()
            .context("segment_provider_settings must be a JSON object")?;
        for (provider_id, patch_value) in patch_object {
            let provider_id = normalize_provider_id(provider_id)?;
            let current = load_library_provider_setting(pool, &source_config_id, &provider_id)
                .await?
                .or_else(|| {
                    provider_settings_for(&default_segment_provider_settings(), &provider_id)
                })
                .unwrap_or_else(|| {
                    json!({
                        "enabled": false,
                        "kind": "extension",
                        "label": provider_id
                    })
                });
            let updated = merge_provider_setting_entry(&current, patch_value)?;
            let enabled = provider_settings_enabled(Some(&updated));
            sqlx::query::<sqlx::Any>(
                "INSERT INTO media_interaction_library_provider_settings
                    (source_config_id, provider_kind, enabled, settings_json, created_at, updated_at)
                 VALUES ($1, $2, $3 != 0, $4, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                 ON CONFLICT(source_config_id, provider_kind) DO UPDATE SET
                    enabled = excluded.enabled,
                    settings_json = excluded.settings_json,
                    updated_at = CURRENT_TIMESTAMP",
            )
            .bind(&source_config_id)
            .bind(&provider_id)
            .bind(if enabled { 1_i64 } else { 0_i64 })
            .bind(updated.to_string())
            .execute(pool)
            .await
            .with_context(|| format!("updating library provider setting for {provider_id}"))?;
        }
    }

    load_media_interaction_library_settings(pool, &source_config_id).await
}

async fn media_interaction_library_settings_record_from_source_row(
    pool: &AnyPool,
    source_config_id: &str,
    row: &AnyRow,
) -> Result<MediaInteractionLibrarySettingsRecord> {
    let (segment_provider_settings, created_at, updated_at) =
        load_library_provider_settings_map(pool, source_config_id).await?;
    let effective_segment_provider_settings = merge_segment_provider_settings(
        &default_segment_provider_settings(),
        segment_provider_settings.clone(),
    )?;
    Ok(MediaInteractionLibrarySettingsRecord {
        source_config_id: source_config_id.to_string(),
        extension_id: row.get("extension_id"),
        source_enabled: row_bool(row, "source_enabled"),
        segment_provider_settings,
        effective_segment_provider_settings,
        created_at,
        updated_at,
    })
}

async fn ensure_source_config_exists(pool: &AnyPool, source_config_id: &str) -> Result<()> {
    let exists = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM source_configs WHERE id = $1")
        .bind(source_config_id)
        .fetch_one(pool)
        .await
        .context("checking source config existence")?;
    if exists == 0 {
        bail!("source config not found");
    }
    Ok(())
}

async fn load_library_provider_settings_map(
    pool: &AnyPool,
    source_config_id: &str,
) -> Result<(Value, Option<String>, Option<String>)> {
    let rows = sqlx::query::<sqlx::Any>(
        "SELECT provider_kind,
                CASE WHEN enabled THEN 1 ELSE 0 END AS enabled,
                settings_json,
                CAST(created_at AS TEXT) AS created_at,
                CAST(updated_at AS TEXT) AS updated_at
         FROM media_interaction_library_provider_settings
         WHERE source_config_id = $1
         ORDER BY provider_kind ASC",
    )
    .bind(source_config_id)
    .fetch_all(pool)
    .await
    .context("loading library provider settings")?;

    let mut map = BTreeMap::new();
    let mut created_at: Option<String> = None;
    let mut updated_at: Option<String> = None;
    for row in rows {
        let provider_kind = normalize_provider_id(row.get::<String, _>("provider_kind").as_str())?;
        let mut settings = row
            .try_get::<String, _>("settings_json")
            .ok()
            .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
            .filter(Value::is_object)
            .unwrap_or_else(|| json!({}));
        if let Some(object) = settings.as_object_mut() {
            object.insert(
                "enabled".to_string(),
                Value::Bool(row_bool(&row, "enabled")),
            );
        }
        map.insert(provider_kind, settings);
        if created_at.is_none() {
            created_at = row_string(&row, "created_at");
        }
        updated_at = row_string(&row, "updated_at").or(updated_at);
    }

    Ok((
        Value::Object(map.into_iter().collect()),
        created_at,
        updated_at,
    ))
}

async fn load_library_provider_setting(
    pool: &AnyPool,
    source_config_id: &str,
    provider_kind: &str,
) -> Result<Option<Value>> {
    let row = sqlx::query::<sqlx::Any>(
        "SELECT CASE WHEN enabled THEN 1 ELSE 0 END AS enabled, settings_json
         FROM media_interaction_library_provider_settings
         WHERE source_config_id = $1
           AND provider_kind = $2
         LIMIT 1",
    )
    .bind(source_config_id)
    .bind(provider_kind)
    .fetch_optional(pool)
    .await
    .context("loading library provider setting")?;

    Ok(row.map(|row| {
        let mut settings = row
            .try_get::<String, _>("settings_json")
            .ok()
            .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
            .filter(Value::is_object)
            .unwrap_or_else(|| json!({}));
        if let Some(object) = settings.as_object_mut() {
            object.insert(
                "enabled".to_string(),
                Value::Bool(row_bool(&row, "enabled")),
            );
        }
        settings
    }))
}

async fn effective_segment_provider_settings_for_media_file(
    pool: &AnyPool,
    media_file_id: &str,
    base_settings: &Value,
) -> Result<Value> {
    let Some(source_config_id) = load_media_file_source_config_id(pool, media_file_id).await?
    else {
        return Ok(base_settings.clone());
    };
    effective_segment_provider_settings_for_source_config(pool, &source_config_id, base_settings)
        .await
}

async fn effective_segment_provider_settings_for_source_config(
    pool: &AnyPool,
    source_config_id: &str,
    base_settings: &Value,
) -> Result<Value> {
    let (library_settings, _, _) =
        load_library_provider_settings_map(pool, source_config_id).await?;
    merge_segment_provider_settings(base_settings, library_settings)
}

async fn provider_settings_for_media_file(
    pool: &AnyPool,
    media_file_id: &str,
    base_settings: &Value,
    provider_kind: &str,
) -> Result<Option<Value>> {
    let effective =
        effective_segment_provider_settings_for_media_file(pool, media_file_id, base_settings)
            .await?;
    Ok(provider_settings_for(&effective, provider_kind))
}

async fn provider_settings_for_first_enabled_season_file(
    pool: &AnyPool,
    season_id: &str,
    base_settings: &Value,
    provider_kind: &str,
) -> Result<Option<Value>> {
    let rows = sqlx::query::<sqlx::Any>(
        "SELECT DISTINCT mf.id AS media_file_id
         FROM episodes e
         JOIN episode_files ef ON ef.episode_id = e.id
         JOIN media_files mf ON mf.id = ef.media_file_id
         WHERE e.season_id = $1
           AND mf.scan_state = 'ok'
         ORDER BY mf.id ASC",
    )
    .bind(season_id)
    .fetch_all(pool)
    .await
    .context("listing season files for provider settings")?;

    let mut first_settings = None;
    for row in rows {
        let media_file_id: String = row.get("media_file_id");
        let settings =
            provider_settings_for_media_file(pool, &media_file_id, base_settings, provider_kind)
                .await?;
        if first_settings.is_none() {
            first_settings = settings.clone();
        }
        if provider_settings_enabled(settings.as_ref()) {
            return Ok(settings);
        }
    }
    Ok(first_settings)
}

async fn season_has_provider_enabled_file(
    pool: &AnyPool,
    season_id: &str,
    base_settings: &Value,
    provider_kind: &str,
) -> Result<bool> {
    let settings = provider_settings_for_first_enabled_season_file(
        pool,
        season_id,
        base_settings,
        provider_kind,
    )
    .await?;
    Ok(provider_settings_enabled(settings.as_ref()))
}

async fn load_media_file_source_config_id(
    pool: &AnyPool,
    media_file_id: &str,
) -> Result<Option<String>> {
    let row = sqlx::query::<sqlx::Any>(
        "SELECT source_config_id
         FROM media_files
         WHERE id = $1
         LIMIT 1",
    )
    .bind(media_file_id)
    .fetch_optional(pool)
    .await
    .context("loading media file source config")?
    .context("media file not found")?;
    Ok(row_string(&row, "source_config_id"))
}

pub async fn load_or_create_playback_preferences(
    pool: &AnyPool,
    user_id: Uuid,
) -> Result<PlaybackInteractionPreferences> {
    if let Some(preferences) = load_playback_preferences(pool, user_id).await? {
        return Ok(preferences);
    }

    let defaults = default_playback_preferences();
    sqlx::query::<sqlx::Any>(
        "INSERT INTO user_playback_preferences
            (user_id, skip_intro_behavior, skip_recap_behavior, skip_preview_behavior,
             skip_credits_behavior, skip_outro_behavior, autoplay_enabled,
             autoplay_countdown_seconds, autoplay_max_consecutive,
             autoplay_max_elapsed_minutes, segment_provider_settings_json,
             created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7 != 0, $8, $9, $10, $11, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .bind(user_id.to_string())
    .bind(&defaults.skip_intro_behavior)
    .bind(&defaults.skip_recap_behavior)
    .bind(&defaults.skip_preview_behavior)
    .bind(&defaults.skip_credits_behavior)
    .bind(&defaults.skip_outro_behavior)
    .bind(if defaults.autoplay_enabled {
        1_i64
    } else {
        0_i64
    })
    .bind(defaults.autoplay_countdown_seconds)
    .bind(defaults.autoplay_max_consecutive)
    .bind(defaults.autoplay_max_elapsed_minutes)
    .bind(defaults.segment_provider_settings.to_string())
    .execute(pool)
    .await
    .context("creating default playback interaction preferences")?;

    Ok(defaults)
}

pub async fn update_playback_preferences(
    pool: &AnyPool,
    user_id: Uuid,
    patch: PlaybackInteractionPreferencesPatch,
) -> Result<PlaybackInteractionPreferences> {
    let mut preferences = load_or_create_playback_preferences(pool, user_id).await?;

    if let Some(value) = patch.skip_intro_behavior {
        preferences.skip_intro_behavior = validate_skip_behavior(&value)?;
    }
    if let Some(value) = patch.skip_recap_behavior {
        preferences.skip_recap_behavior = validate_skip_behavior(&value)?;
    }
    if let Some(value) = patch.skip_preview_behavior {
        preferences.skip_preview_behavior = validate_skip_behavior(&value)?;
    }
    if let Some(value) = patch.skip_credits_behavior {
        preferences.skip_credits_behavior = validate_skip_behavior(&value)?;
    }
    if let Some(value) = patch.skip_outro_behavior {
        preferences.skip_outro_behavior = validate_skip_behavior(&value)?;
    }
    if let Some(value) = patch.autoplay_enabled {
        preferences.autoplay_enabled = value;
    }
    if let Some(value) = patch.autoplay_countdown_seconds {
        preferences.autoplay_countdown_seconds = validate_countdown_seconds(value)?;
    }
    if let Some(value) = patch.autoplay_max_consecutive {
        preferences.autoplay_max_consecutive = validate_max_consecutive(value)?;
    }
    if let Some(value) = patch.autoplay_max_elapsed_minutes {
        preferences.autoplay_max_elapsed_minutes = validate_max_elapsed_minutes(value)?;
    }
    if let Some(value) = patch.segment_provider_settings {
        preferences.segment_provider_settings =
            merge_segment_provider_settings(&preferences.segment_provider_settings, value)?;
    }

    sqlx::query::<sqlx::Any>(
        "UPDATE user_playback_preferences
         SET skip_intro_behavior = $1,
             skip_recap_behavior = $2,
             skip_preview_behavior = $3,
             skip_credits_behavior = $4,
             skip_outro_behavior = $5,
             autoplay_enabled = $6 != 0,
             autoplay_countdown_seconds = $7,
             autoplay_max_consecutive = $8,
             autoplay_max_elapsed_minutes = $9,
             segment_provider_settings_json = $10,
             updated_at = CURRENT_TIMESTAMP
         WHERE user_id = $11",
    )
    .bind(&preferences.skip_intro_behavior)
    .bind(&preferences.skip_recap_behavior)
    .bind(&preferences.skip_preview_behavior)
    .bind(&preferences.skip_credits_behavior)
    .bind(&preferences.skip_outro_behavior)
    .bind(if preferences.autoplay_enabled {
        1_i64
    } else {
        0_i64
    })
    .bind(preferences.autoplay_countdown_seconds)
    .bind(preferences.autoplay_max_consecutive)
    .bind(preferences.autoplay_max_elapsed_minutes)
    .bind(preferences.segment_provider_settings.to_string())
    .bind(user_id.to_string())
    .execute(pool)
    .await
    .context("updating playback interaction preferences")?;

    Ok(preferences)
}

async fn normalize_candidate_input(
    pool: &AnyPool,
    input: SegmentCandidateInput,
) -> Result<CandidateRow> {
    let media_file_id = normalize_required_text(&input.media_file_id, "media_file_id")?;
    ensure_media_file_exists(pool, &media_file_id).await?;
    let context = match (input.item_type, input.item_id) {
        (Some(item_type), Some(item_id)) => SegmentItemContext {
            item_type: normalize_required_text(&item_type, "item_type")?.to_ascii_lowercase(),
            item_id: normalize_required_text(&item_id, "item_id")?,
        },
        (None, None) => resolve_item_context_for_file(pool, &media_file_id).await?,
        _ => bail!("item_type and item_id must be provided together"),
    };
    let segment_type = normalize_required_text(&input.segment_type, "segment_type")?
        .to_ascii_lowercase()
        .replace('-', "_");
    let provider_kind = normalize_required_text(&input.provider_kind, "provider_kind")?
        .to_ascii_lowercase()
        .replace('-', "_");
    let provider_id = normalize_required_text(&input.provider_id, "provider_id")?;
    let provider_version = input
        .provider_version
        .map(|value| normalize_required_text(&value, "provider_version"))
        .transpose()?;
    let identity_strength = normalize_required_text(&input.identity_strength, "identity_strength")?
        .to_ascii_lowercase()
        .replace('-', "_");
    if provider_kind == "manual" || identity_strength == "manual" {
        bail!("manual media segment candidates are not supported");
    }

    if !input.confidence.is_finite() {
        bail!("confidence must be finite");
    }
    let confidence = input.confidence.clamp(0.0, 1.0);
    let candidate_id = deterministic_candidate_id(
        &media_file_id,
        &context.item_type,
        &context.item_id,
        &segment_type,
        input.start_seconds,
        input.end_seconds,
        &provider_kind,
        &provider_id,
        input.source_payload.as_ref(),
    );

    Ok(CandidateRow {
        id: candidate_id,
        media_file_id,
        item_type: context.item_type,
        item_id: context.item_id,
        segment_type,
        start_seconds: input.start_seconds,
        end_seconds: input.end_seconds,
        provider_kind,
        provider_id,
        provider_version,
        confidence,
        identity_strength,
        source_payload: input.source_payload,
    })
}

async fn ensure_media_file_exists(pool: &AnyPool, media_file_id: &str) -> Result<()> {
    let exists = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM media_files WHERE id = $1")
        .bind(media_file_id)
        .fetch_one(pool)
        .await
        .context("checking media file existence")?;
    if exists == 0 {
        bail!("media file not found");
    }
    Ok(())
}

async fn ensure_season_exists(pool: &AnyPool, season_id: &str) -> Result<()> {
    let exists = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM seasons WHERE id = $1")
        .bind(season_id)
        .fetch_one(pool)
        .await
        .context("checking season existence")?;
    if exists == 0 {
        bail!("season not found");
    }
    Ok(())
}

fn normalize_segment_item_context(item_type: &str, item_id: &str) -> Result<SegmentItemContext> {
    let item_type = normalize_required_text(item_type, "item_type")?
        .to_ascii_lowercase()
        .replace('-', "_");
    if !matches!(item_type.as_str(), "movie" | "episode") {
        bail!("item_type must be movie or episode");
    }
    Ok(SegmentItemContext {
        item_type,
        item_id: normalize_required_text(item_id, "item_id")?,
    })
}

async fn ensure_segment_item_exists(pool: &AnyPool, context: &SegmentItemContext) -> Result<()> {
    let (table, column) = match context.item_type.as_str() {
        "movie" => ("movies", "id"),
        "episode" => ("episodes", "id"),
        _ => bail!("item_type must be movie or episode"),
    };
    let query = format!("SELECT COUNT(*) FROM {table} WHERE {column} = $1");
    let exists = sqlx::query_scalar::<_, i64>(&query)
        .bind(&context.item_id)
        .fetch_one(pool)
        .await
        .context("checking media segment item existence")?;
    if exists == 0 {
        bail!("item not found");
    }
    Ok(())
}

async fn resolve_item_context_for_file(
    pool: &AnyPool,
    media_file_id: &str,
) -> Result<SegmentItemContext> {
    if let Some(row) = sqlx::query::<sqlx::Any>(
        "SELECT movie_id
         FROM movie_files
         WHERE media_file_id = $1
         LIMIT 1",
    )
    .bind(media_file_id)
    .fetch_optional(pool)
    .await
    .context("resolving movie segment context")?
    {
        return Ok(SegmentItemContext {
            item_type: "movie".to_string(),
            item_id: row.get("movie_id"),
        });
    }

    if let Some(row) = sqlx::query::<sqlx::Any>(
        "SELECT episode_id
         FROM episode_files
         WHERE media_file_id = $1
         LIMIT 1",
    )
    .bind(media_file_id)
    .fetch_optional(pool)
    .await
    .context("resolving episode segment context")?
    {
        return Ok(SegmentItemContext {
            item_type: "episode".to_string(),
            item_id: row.get("episode_id"),
        });
    }

    let row = sqlx::query::<sqlx::Any>(
        "SELECT mi.id, mi.type
         FROM media_files mf
         JOIN media_items mi ON mi.id = mf.media_item_id
         WHERE mf.id = $1
         LIMIT 1",
    )
    .bind(media_file_id)
    .fetch_optional(pool)
    .await
    .context("resolving fallback media item segment context")?
    .context("media file is not linked to a media item")?;

    Ok(SegmentItemContext {
        item_type: row
            .try_get::<String, _>("type")
            .unwrap_or_else(|_| "media".to_string()),
        item_id: row.get("id"),
    })
}

async fn load_media_duration_seconds(pool: &AnyPool, media_file_id: &str) -> Result<Option<f64>> {
    if let Some(row) = sqlx::query::<sqlx::Any>(
        "SELECT duration_seconds
         FROM media_file_fingerprints
         WHERE media_file_id = $1
         LIMIT 1",
    )
    .bind(media_file_id)
    .fetch_optional(pool)
    .await
    .context("loading media file fingerprint duration")?
        && let Some(duration) = row.try_get::<i64, _>("duration_seconds").ok()
        && duration > 0
    {
        return Ok(Some(duration as f64));
    }

    if let Some(row) = sqlx::query::<sqlx::Any>(
        "SELECT normalized_json
         FROM media_file_probes
         WHERE media_file_id = $1 AND normalized_json IS NOT NULL
         LIMIT 1",
    )
    .bind(media_file_id)
    .fetch_optional(pool)
    .await
    .context("loading media file probe duration")?
        && let Some(duration) = row
            .try_get::<String, _>("normalized_json")
            .ok()
            .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
            .and_then(|value| value.get("duration_seconds").and_then(Value::as_f64))
            .filter(|value| *value > 0.0)
    {
        return Ok(Some(duration));
    }

    if let Some(row) = sqlx::query::<sqlx::Any>(
        "SELECT mi.runtime_seconds
         FROM media_files mf
         JOIN media_items mi ON mi.id = mf.media_item_id
         WHERE mf.id = $1
         LIMIT 1",
    )
    .bind(media_file_id)
    .fetch_optional(pool)
    .await
    .context("loading media item runtime duration")?
        && let Some(duration) = row.try_get::<i64, _>("runtime_seconds").ok()
        && duration > 0
    {
        return Ok(Some(duration as f64));
    }

    Ok(None)
}

#[derive(Debug)]
struct CandidateValidation {
    state: &'static str,
    reason: Option<String>,
    auto_activate: bool,
}

fn validate_candidate(
    candidate: &CandidateRow,
    duration_seconds: Option<f64>,
) -> CandidateValidation {
    if !allowed_segment_type(&candidate.segment_type) {
        return rejected("unsupported_segment_type");
    }
    if !candidate.start_seconds.is_finite()
        || !candidate.end_seconds.is_finite()
        || candidate.start_seconds < 0.0
    {
        return rejected("invalid_time_range");
    }
    if candidate.end_seconds <= candidate.start_seconds {
        return rejected("invalid_time_range");
    }
    if let Some(duration) = duration_seconds {
        if candidate.end_seconds > duration + DURATION_TOLERANCE_SECONDS {
            return rejected("outside_media_duration");
        }
        if is_early_segment(&candidate.segment_type) {
            let early_limit = (duration * EARLY_WINDOW_FRACTION).min(EARLY_WINDOW_MAX_SECONDS);
            if candidate.start_seconds > early_limit {
                return rejected("outside_early_window");
            }
        }
        if is_late_segment(&candidate.segment_type)
            && candidate.start_seconds < duration * LATE_WINDOW_FRACTION
            && !candidate_payload_bool(candidate, "non_final")
        {
            return rejected("outside_late_window");
        }
    }
    if candidate.end_seconds - candidate.start_seconds
        < minimum_segment_duration(&candidate.segment_type)
    {
        return rejected("segment_too_short");
    }
    if !identity_can_auto_activate(candidate) {
        return CandidateValidation {
            state: "rejected",
            reason: Some("weak_identity_for_auto_activation".to_string()),
            auto_activate: false,
        };
    }
    if candidate.confidence < minimum_confidence(candidate) {
        return rejected("confidence_below_threshold");
    }

    CandidateValidation {
        state: "accepted",
        reason: None,
        auto_activate: true,
    }
}

fn rejected(reason: &str) -> CandidateValidation {
    CandidateValidation {
        state: "rejected",
        reason: Some(reason.to_string()),
        auto_activate: false,
    }
}

async fn recalculate_active_segments(
    pool: &AnyPool,
    media_file_id: &str,
    segment_type: &str,
) -> Result<()> {
    let locked_rows = sqlx::query::<sqlx::Any>(
        "SELECT start_seconds, end_seconds
         FROM media_segments
         WHERE media_file_id = $1
           AND segment_type = $2
           AND status = 'active'
           AND locked = TRUE",
    )
    .bind(media_file_id)
    .bind(segment_type)
    .fetch_all(pool)
    .await
    .context("loading locked media segment windows")?;
    let locked_windows = locked_rows
        .iter()
        .map(|row| ActiveWindow {
            start_seconds: row.try_get("start_seconds").unwrap_or(0.0),
            end_seconds: row.try_get("end_seconds").unwrap_or(0.0),
        })
        .collect::<Vec<_>>();

    let rows = sqlx::query::<sqlx::Any>(
        "SELECT id, media_file_id, item_type, item_id, segment_type, start_seconds, end_seconds,
                provider_kind, provider_id, provider_version, confidence, identity_strength,
                source_payload_json
         FROM media_segment_candidates
         WHERE media_file_id = $1
           AND segment_type = $2
           AND validation_state = 'accepted'
         ORDER BY start_seconds ASC, end_seconds ASC
         LIMIT 200",
    )
    .bind(media_file_id)
    .bind(segment_type)
    .fetch_all(pool)
    .await
    .context("loading accepted media segment candidates")?;

    let mut candidates = rows.iter().map(candidate_row_from_row).collect::<Vec<_>>();
    candidates.sort_by(|left, right| compare_candidate_rank(right, left));
    let selected = select_non_overlapping_candidates(candidates, &locked_windows);

    sqlx::query::<sqlx::Any>(
        "UPDATE media_segments
         SET status = 'superseded', updated_at = CURRENT_TIMESTAMP
         WHERE media_file_id = $1
           AND segment_type = $2
           AND status = 'active'
           AND (locked = FALSE OR locked IS NULL)",
    )
    .bind(media_file_id)
    .bind(segment_type)
    .execute(pool)
    .await
    .context("superseding previous active media segments")?;

    for candidate in selected {
        upsert_active_segment_for_candidate(pool, &candidate).await?;
    }

    refresh_active_media_segment_metrics(pool).await;

    Ok(())
}

fn select_non_overlapping_candidates(
    candidates: Vec<CandidateRow>,
    locked_windows: &[ActiveWindow],
) -> Vec<CandidateRow> {
    let mut selected = Vec::new();
    let mut windows = locked_windows.to_vec();

    for candidate in candidates {
        if windows.iter().any(|window| {
            ranges_overlap(
                candidate.start_seconds,
                candidate.end_seconds,
                window.start_seconds,
                window.end_seconds,
            )
        }) {
            continue;
        }
        windows.push(ActiveWindow {
            start_seconds: candidate.start_seconds,
            end_seconds: candidate.end_seconds,
        });
        selected.push(candidate);
    }

    selected.sort_by(|left, right| {
        left.start_seconds
            .partial_cmp(&right.start_seconds)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                left.end_seconds
                    .partial_cmp(&right.end_seconds)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    });
    selected
}

async fn upsert_active_segment_for_candidate(
    pool: &AnyPool,
    candidate: &CandidateRow,
) -> Result<()> {
    let existing_id = sqlx::query::<sqlx::Any>(
        "SELECT id
         FROM media_segments
         WHERE canonical_candidate_id = $1
         LIMIT 1",
    )
    .bind(&candidate.id)
    .fetch_optional(pool)
    .await
    .context("loading active segment by canonical candidate")?
    .map(|row| row.get::<String, _>("id"));

    let metadata = active_segment_metadata(candidate).to_string();
    let source_label = source_label_for_candidate(candidate);
    let locked = false;

    if let Some(segment_id) = existing_id {
        sqlx::query::<sqlx::Any>(
            "UPDATE media_segments
             SET media_file_id = $1,
                 item_type = $2,
                 item_id = $3,
                 segment_type = $4,
                 start_seconds = $5,
                 end_seconds = $6,
                 source_label = $7,
                 confidence = $8,
                 locked = $9 != 0,
                 status = 'active',
                 metadata_json = $10,
                 updated_at = CURRENT_TIMESTAMP
             WHERE id = $11",
        )
        .bind(&candidate.media_file_id)
        .bind(&candidate.item_type)
        .bind(&candidate.item_id)
        .bind(&candidate.segment_type)
        .bind(candidate.start_seconds)
        .bind(candidate.end_seconds)
        .bind(source_label)
        .bind(candidate.confidence)
        .bind(if locked { 1_i64 } else { 0_i64 })
        .bind(metadata)
        .bind(segment_id)
        .execute(pool)
        .await
        .context("reactivating canonical media segment")?;
    } else {
        sqlx::query::<sqlx::Any>(
            "INSERT INTO media_segments
                (id, media_file_id, item_type, item_id, segment_type, start_seconds, end_seconds,
                 canonical_candidate_id, source_label, confidence, locked, status, metadata_json,
                 created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11 != 0, 'active', $12, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(&candidate.media_file_id)
        .bind(&candidate.item_type)
        .bind(&candidate.item_id)
        .bind(&candidate.segment_type)
        .bind(candidate.start_seconds)
        .bind(candidate.end_seconds)
        .bind(&candidate.id)
        .bind(source_label)
        .bind(candidate.confidence)
        .bind(if locked { 1_i64 } else { 0_i64 })
        .bind(metadata)
        .execute(pool)
        .await
        .context("inserting canonical media segment")?;
    }

    Ok(())
}

async fn load_segment_candidate(
    pool: &AnyPool,
    candidate_id: &str,
) -> Result<Option<SegmentCandidateRecord>> {
    let row = sqlx::query::<sqlx::Any>(
        "SELECT id, media_file_id, item_type, item_id, segment_type, start_seconds, end_seconds,
                provider_kind, provider_id, provider_version, confidence, validation_state,
                validation_reason, identity_strength, source_payload_json,
                CAST(created_at AS TEXT) AS created_at,
                CAST(updated_at AS TEXT) AS updated_at
         FROM media_segment_candidates
         WHERE id = $1
         LIMIT 1",
    )
    .bind(candidate_id)
    .fetch_optional(pool)
    .await
    .context("loading media segment candidate")?;

    Ok(row.as_ref().map(candidate_record_from_row))
}

async fn load_active_segment_for_candidate(
    pool: &AnyPool,
    candidate_id: &str,
) -> Result<Option<ActiveMediaSegmentRecord>> {
    let row = sqlx::query::<sqlx::Any>(
        "SELECT id, media_file_id, item_type, item_id, segment_type, start_seconds, end_seconds,
                canonical_candidate_id, source_label, confidence,
                CASE WHEN locked THEN 1 ELSE 0 END AS locked, status, metadata_json
         FROM media_segments
         WHERE canonical_candidate_id = $1 AND status = 'active'
         LIMIT 1",
    )
    .bind(candidate_id)
    .fetch_optional(pool)
    .await
    .context("loading active media segment for candidate")?;

    Ok(row.as_ref().map(active_segment_from_row))
}

async fn load_active_segment(
    pool: &AnyPool,
    segment_id: &str,
) -> Result<Option<ActiveMediaSegmentRecord>> {
    let row = sqlx::query::<sqlx::Any>(
        "SELECT id, media_file_id, item_type, item_id, segment_type, start_seconds, end_seconds,
                canonical_candidate_id, source_label, confidence,
                CASE WHEN locked THEN 1 ELSE 0 END AS locked, status, metadata_json
         FROM media_segments
         WHERE id = $1 AND status = 'active'
         LIMIT 1",
    )
    .bind(segment_id)
    .fetch_optional(pool)
    .await
    .context("loading active media segment")?;

    Ok(row.as_ref().map(active_segment_from_row))
}

fn candidate_row_from_row(row: &AnyRow) -> CandidateRow {
    CandidateRow {
        id: row.get("id"),
        media_file_id: row.get("media_file_id"),
        item_type: row.get("item_type"),
        item_id: row.get("item_id"),
        segment_type: row.get("segment_type"),
        start_seconds: row.try_get("start_seconds").unwrap_or(0.0),
        end_seconds: row.try_get("end_seconds").unwrap_or(0.0),
        provider_kind: row.get("provider_kind"),
        provider_id: row.get("provider_id"),
        provider_version: row.try_get("provider_version").ok(),
        confidence: row.try_get("confidence").unwrap_or(0.0),
        identity_strength: row.get("identity_strength"),
        source_payload: row
            .try_get::<String, _>("source_payload_json")
            .ok()
            .and_then(|raw| serde_json::from_str(&raw).ok()),
    }
}

fn candidate_record_from_row(row: &AnyRow) -> SegmentCandidateRecord {
    SegmentCandidateRecord {
        id: row.get("id"),
        media_file_id: row.get("media_file_id"),
        item_type: row.get("item_type"),
        item_id: row.get("item_id"),
        segment_type: row.get("segment_type"),
        start_seconds: row.try_get("start_seconds").unwrap_or(0.0),
        end_seconds: row.try_get("end_seconds").unwrap_or(0.0),
        provider_kind: row.get("provider_kind"),
        provider_id: row.get("provider_id"),
        provider_version: row.try_get("provider_version").ok(),
        confidence: row.try_get("confidence").unwrap_or(0.0),
        validation_state: row.get("validation_state"),
        validation_reason: row.try_get("validation_reason").ok(),
        identity_strength: row.get("identity_strength"),
        source_payload: row
            .try_get::<String, _>("source_payload_json")
            .ok()
            .and_then(|raw| serde_json::from_str(&raw).ok()),
        created_at: row_string(row, "created_at"),
        updated_at: row_string(row, "updated_at"),
    }
}

fn active_segment_from_row(row: &AnyRow) -> ActiveMediaSegmentRecord {
    ActiveMediaSegmentRecord {
        id: row.get("id"),
        media_file_id: row.get("media_file_id"),
        item_type: row.get("item_type"),
        item_id: row.get("item_id"),
        segment_type: row.get("segment_type"),
        start_seconds: row.try_get("start_seconds").unwrap_or(0.0),
        end_seconds: row.try_get("end_seconds").unwrap_or(0.0),
        canonical_candidate_id: row.try_get("canonical_candidate_id").ok(),
        source_label: row.get("source_label"),
        confidence: row.try_get("confidence").unwrap_or(0.0),
        locked: row_bool(row, "locked"),
        status: row.get("status"),
        metadata: row
            .try_get::<String, _>("metadata_json")
            .ok()
            .and_then(|raw| serde_json::from_str(&raw).ok()),
    }
}

fn compare_candidate_rank(left: &CandidateRow, right: &CandidateRow) -> std::cmp::Ordering {
    candidate_rank_score(left)
        .partial_cmp(&candidate_rank_score(right))
        .unwrap_or(std::cmp::Ordering::Equal)
        .then_with(|| {
            left.confidence
                .partial_cmp(&right.confidence)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .then_with(|| {
            right
                .start_seconds
                .partial_cmp(&left.start_seconds)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
}

fn candidate_rank_score(candidate: &CandidateRow) -> f64 {
    provider_priority(&candidate.provider_kind)
        + identity_priority(&candidate.identity_strength)
        + candidate.confidence
}

fn provider_priority(provider_kind: &str) -> f64 {
    match provider_kind {
        "imported" => 9_000.0,
        "chapter" => 7_000.0,
        "theintrodb" | "aniskip" | "anime_skip" => 6_500.0,
        "local_audio_recurring" | "local_visual_recurring" => 6_000.0,
        "extension" => 5_000.0,
        _ => 0.0,
    }
}

fn identity_priority(identity_strength: &str) -> f64 {
    match identity_strength {
        "file_fingerprint" => 200.0,
        "external_id_exact" => 150.0,
        "external_id_episode" => 140.0,
        "metadata_fuzzy" => -1_000.0,
        "unknown" => -2_000.0,
        _ => -2_000.0,
    }
}

fn minimum_confidence(candidate: &CandidateRow) -> f64 {
    if candidate.provider_kind == PROVIDER_CHAPTER {
        0.70
    } else if matches!(
        candidate.provider_kind.as_str(),
        "theintrodb" | "aniskip" | "anime_skip"
    ) {
        0.75
    } else {
        0.80
    }
}

fn identity_can_auto_activate(candidate: &CandidateRow) -> bool {
    matches!(
        candidate.identity_strength.as_str(),
        "file_fingerprint" | "external_id_exact" | "external_id_episode"
    )
}

fn allowed_segment_type(segment_type: &str) -> bool {
    matches!(
        segment_type,
        "intro" | "recap" | "preview" | "credits" | "outro" | "custom"
    )
}

fn minimum_segment_duration(segment_type: &str) -> f64 {
    match segment_type {
        "intro" => 20.0,
        "recap" => 10.0,
        "preview" => 10.0,
        "credits" => 15.0,
        "outro" => 15.0,
        _ => 1.0,
    }
}

fn is_early_segment(segment_type: &str) -> bool {
    matches!(segment_type, "intro" | "recap")
}

fn is_late_segment(segment_type: &str) -> bool {
    matches!(segment_type, "credits" | "outro")
}

fn candidate_payload_bool(candidate: &CandidateRow, key: &str) -> bool {
    candidate
        .source_payload
        .as_ref()
        .and_then(|payload| payload.get(key))
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn active_segment_metadata(candidate: &CandidateRow) -> Value {
    json!({
        "provider_kind": candidate.provider_kind,
        "provider_id": candidate.provider_id,
        "provider_version": candidate.provider_version,
        "identity_strength": candidate.identity_strength,
        "source_payload": candidate.source_payload,
    })
}

fn source_label_for_candidate(candidate: &CandidateRow) -> String {
    if let Some(label) = candidate
        .source_payload
        .as_ref()
        .and_then(|payload| payload.get("label").or_else(|| payload.get("title")))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return label.chars().take(80).collect();
    }

    match candidate.provider_kind.as_str() {
        "chapter" => "Embedded chapters".to_string(),
        "theintrodb" => "TheIntroDB".to_string(),
        "aniskip" => "AniSkip".to_string(),
        "anime_skip" => "Anime Skip".to_string(),
        "local_audio_recurring" => "Local audio detector".to_string(),
        "local_visual_recurring" => "Local visual detector".to_string(),
        "manual" => "Manual".to_string(),
        "imported" => "Imported".to_string(),
        "extension" => "Extension".to_string(),
        other => other.replace('_', " "),
    }
}

fn deterministic_candidate_id(
    media_file_id: &str,
    item_type: &str,
    item_id: &str,
    segment_type: &str,
    start_seconds: f64,
    end_seconds: f64,
    provider_kind: &str,
    provider_id: &str,
    source_payload: Option<&Value>,
) -> String {
    let source_key = source_payload
        .and_then(|payload| {
            payload
                .get("chapter_id")
                .or_else(|| payload.get("provider_segment_id"))
                .or_else(|| payload.get("id"))
        })
        .map(Value::to_string)
        .unwrap_or_default();
    let key = format!(
        "{media_file_id}|{item_type}|{item_id}|{segment_type}|{start:.3}|{end:.3}|{provider_kind}|{provider_id}|{source_key}",
        start = start_seconds,
        end = end_seconds
    );
    Uuid::new_v5(&Uuid::NAMESPACE_OID, key.as_bytes()).to_string()
}

fn normalize_required_text(value: &str, field: &str) -> Result<String> {
    let normalized = value.trim();
    if normalized.is_empty() {
        bail!("{field} is required");
    }
    Ok(normalized.to_string())
}

fn normalize_uuid_text(value: &str, field: &str) -> Result<String> {
    let normalized = normalize_required_text(value, field)?;
    Uuid::parse_str(&normalized).with_context(|| format!("{field} must be a valid UUID"))?;
    Ok(normalized)
}

fn normalize_optional_reason(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.chars().take(500).collect())
}

fn ranges_overlap(left_start: f64, left_end: f64, right_start: f64, right_end: f64) -> bool {
    left_start < right_end && right_start < left_end
}

fn chapter_title(chapter: &ffprobe::Chapter) -> Option<String> {
    chapter.tags.as_ref().and_then(|tags| {
        tags.iter()
            .find(|(key, value)| key.eq_ignore_ascii_case("title") && !value.trim().is_empty())
            .map(|(_, value)| value.trim().to_string())
    })
}

fn chapter_segment_type(title: &str) -> Option<&'static str> {
    let normalized = title.to_ascii_lowercase();
    let tokens = normalized
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();

    if normalized.contains("previously") || tokens.contains(&"recap") {
        return Some("recap");
    }
    if normalized.contains("next time")
        || normalized.contains("next episode")
        || tokens.contains(&"preview")
        || tokens.contains(&"trailer")
    {
        return Some("preview");
    }
    if normalized.contains("credit") {
        return Some("credits");
    }
    if normalized.contains("opening")
        || tokens.contains(&"intro")
        || tokens.contains(&"op")
        || normalized.contains("title sequence")
    {
        return Some("intro");
    }
    if normalized.contains("ending")
        || tokens.contains(&"outro")
        || tokens.contains(&"ed")
        || normalized.contains("end theme")
    {
        return Some("outro");
    }

    None
}

fn parse_chapter_seconds(value: &str) -> Option<f64> {
    value
        .trim()
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
}

fn row_bool(row: &AnyRow, column: &str) -> bool {
    row.try_get::<bool, _>(column)
        .or_else(|_| row.try_get::<i64, _>(column).map(|value| value != 0))
        .or_else(|_| row.try_get::<i32, _>(column).map(|value| value != 0))
        .unwrap_or(false)
}

fn row_string(row: &AnyRow, column: &str) -> Option<String> {
    row.try_get::<String, _>(column)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn row_json_value(row: &AnyRow, column: &str) -> Result<Value> {
    let raw = row
        .try_get::<String, _>(column)
        .with_context(|| format!("loading {column}"))?;
    serde_json::from_str::<Value>(&raw).with_context(|| format!("parsing {column}"))
}

fn timestamp_now() -> String {
    timestamp_after_seconds(0)
}

fn timestamp_after_seconds(seconds: i64) -> String {
    (Utc::now() + ChronoDuration::seconds(seconds))
        .format("%Y-%m-%d %H:%M:%S")
        .to_string()
}

async fn load_playback_preferences(
    pool: &AnyPool,
    user_id: Uuid,
) -> Result<Option<PlaybackInteractionPreferences>> {
    let row = sqlx::query::<sqlx::Any>(
        "SELECT skip_intro_behavior, skip_recap_behavior, skip_preview_behavior,
                skip_credits_behavior, skip_outro_behavior,
                CASE WHEN autoplay_enabled THEN 1 ELSE 0 END AS autoplay_enabled,
                autoplay_countdown_seconds, autoplay_max_consecutive,
                autoplay_max_elapsed_minutes,
                segment_provider_settings_json
         FROM user_playback_preferences
         WHERE user_id = $1
         LIMIT 1",
    )
    .bind(user_id.to_string())
    .fetch_optional(pool)
    .await
    .context("loading playback interaction preferences")?;

    Ok(row.as_ref().map(playback_preferences_from_row))
}

fn playback_preferences_from_row(row: &AnyRow) -> PlaybackInteractionPreferences {
    let defaults = default_playback_preferences();
    let stored_provider_settings = row
        .try_get::<String, _>("segment_provider_settings_json")
        .ok()
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
        .unwrap_or(Value::Null);

    PlaybackInteractionPreferences {
        skip_intro_behavior: row
            .try_get("skip_intro_behavior")
            .unwrap_or(defaults.skip_intro_behavior),
        skip_recap_behavior: row
            .try_get("skip_recap_behavior")
            .unwrap_or(defaults.skip_recap_behavior),
        skip_preview_behavior: row
            .try_get("skip_preview_behavior")
            .unwrap_or(defaults.skip_preview_behavior),
        skip_credits_behavior: row
            .try_get("skip_credits_behavior")
            .unwrap_or(defaults.skip_credits_behavior),
        skip_outro_behavior: row
            .try_get("skip_outro_behavior")
            .unwrap_or(defaults.skip_outro_behavior),
        autoplay_enabled: row_bool(row, "autoplay_enabled"),
        autoplay_countdown_seconds: row
            .try_get::<i64, _>("autoplay_countdown_seconds")
            .map(|value| value as i32)
            .unwrap_or(defaults.autoplay_countdown_seconds),
        autoplay_max_consecutive: row
            .try_get::<i64, _>("autoplay_max_consecutive")
            .map(|value| value as i32)
            .unwrap_or(defaults.autoplay_max_consecutive),
        autoplay_max_elapsed_minutes: row
            .try_get::<i64, _>("autoplay_max_elapsed_minutes")
            .map(|value| value as i32)
            .unwrap_or(defaults.autoplay_max_elapsed_minutes),
        segment_provider_settings: merge_segment_provider_settings(
            &defaults.segment_provider_settings,
            stored_provider_settings,
        )
        .unwrap_or(defaults.segment_provider_settings),
    }
}

fn default_playback_preferences() -> PlaybackInteractionPreferences {
    PlaybackInteractionPreferences {
        skip_intro_behavior: "prompt".to_string(),
        skip_recap_behavior: "prompt".to_string(),
        skip_preview_behavior: "prompt".to_string(),
        skip_credits_behavior: "prompt".to_string(),
        skip_outro_behavior: "prompt".to_string(),
        autoplay_enabled: true,
        autoplay_countdown_seconds: 10,
        autoplay_max_consecutive: 3,
        autoplay_max_elapsed_minutes: 180,
        segment_provider_settings: default_segment_provider_settings(),
    }
}

fn default_segment_provider_settings() -> Value {
    json!({
        "chapter": {
            "enabled": true,
            "kind": "local_metadata",
            "label": "Embedded chapters"
        },
        "theintrodb": {
            "enabled": true,
            "kind": "built_in_network",
            "label": "TheIntroDB",
            "rate_limit_per_minute": DEFAULT_PROVIDER_RATE_LIMIT_PER_MINUTE
        },
        "aniskip": {
            "enabled": true,
            "kind": "built_in_network",
            "label": "AniSkip",
            "rate_limit_per_minute": DEFAULT_PROVIDER_RATE_LIMIT_PER_MINUTE
        },
        "anime_skip": {
            "enabled": false,
            "kind": "built_in_network",
            "label": "Anime Skip"
        },
        "local_audio_recurring": {
            "enabled": false,
            "kind": "local_detector",
            "label": "Local audio recurring detector",
            "min_repeat_count": LOCAL_AUDIO_DETECTOR_MIN_REPEAT_COUNT,
            "min_season_files": LOCAL_AUDIO_DETECTOR_MIN_SEASON_FILES,
            "fingerprint_timeout_seconds": LOCAL_AUDIO_FINGERPRINT_TIMEOUT_SECONDS
        },
        "local_visual_recurring": {
            "enabled": false,
            "kind": "local_detector",
            "label": "Local visual recurring detector",
            "min_frame_count": LOCAL_VISUAL_CREDITS_MIN_FRAME_COUNT,
            "min_span_seconds": LOCAL_VISUAL_CREDITS_MIN_SPAN_SECONDS,
            "min_start_fraction": LOCAL_VISUAL_CREDITS_MIN_START_FRACTION,
            "max_frame_gap_seconds": LOCAL_VISUAL_CREDITS_MAX_FRAME_GAP_SECONDS,
            "post_credit_scene_min_frames": LOCAL_VISUAL_POST_CREDIT_SCENE_MIN_FRAMES,
            "post_credit_scene_min_span_seconds": LOCAL_VISUAL_POST_CREDIT_SCENE_MIN_SPAN_SECONDS,
            "black_ratio_threshold": LOCAL_VISUAL_CREDITS_BLACK_RATIO_THRESHOLD,
            "text_ratio_threshold": LOCAL_VISUAL_CREDITS_TEXT_RATIO_THRESHOLD,
            "frame_hash_timeout_seconds": LOCAL_VISUAL_FRAME_HASH_TIMEOUT_SECONDS,
            "frame_hash_step_seconds": LOCAL_VISUAL_FRAME_HASH_STEP_SECONDS,
            "frame_hash_max_frames": LOCAL_VISUAL_FRAME_HASH_MAX_FRAMES_PER_FILE
        }
    })
}

fn validate_skip_behavior(value: &str) -> Result<String> {
    let normalized = value.trim().to_ascii_lowercase().replace('-', "_");
    if matches!(
        normalized.as_str(),
        "disabled" | "prompt" | "auto" | "ask_each_time"
    ) {
        Ok(normalized)
    } else {
        bail!("invalid skip behavior")
    }
}

fn validate_countdown_seconds(value: i32) -> Result<i32> {
    if (0..=120).contains(&value) {
        Ok(value)
    } else {
        bail!("autoplay_countdown_seconds must be between 0 and 120")
    }
}

fn validate_max_consecutive(value: i32) -> Result<i32> {
    if (0..=20).contains(&value) {
        Ok(value)
    } else {
        bail!("autoplay_max_consecutive must be between 0 and 20")
    }
}

fn validate_max_elapsed_minutes(value: i32) -> Result<i32> {
    if (0..=1440).contains(&value) {
        Ok(value)
    } else {
        bail!("autoplay_max_elapsed_minutes must be between 0 and 1440")
    }
}

fn merge_segment_provider_settings(current: &Value, patch: Value) -> Result<Value> {
    let mut merged = provider_settings_map(current)?;
    let patch_object = patch
        .as_object()
        .context("segment_provider_settings must be a JSON object")?;

    for (provider_id, patch_value) in patch_object {
        let provider_id = normalize_provider_id(provider_id)?;
        let current_entry = merged.remove(&provider_id).unwrap_or_else(
            || json!({"enabled": false, "kind": "extension", "label": provider_id}),
        );
        let updated = merge_provider_setting_entry(&current_entry, patch_value)?;
        merged.insert(provider_id, updated);
    }

    Ok(Value::Object(merged.into_iter().collect()))
}

fn provider_settings_map(value: &Value) -> Result<BTreeMap<String, Value>> {
    let object = value
        .as_object()
        .context("segment_provider_settings must be a JSON object")?;
    let mut map = BTreeMap::new();
    for (provider_id, entry) in object {
        map.insert(normalize_provider_id(provider_id)?, entry.clone());
    }
    Ok(map)
}

fn merge_provider_setting_entry(current: &Value, patch: &Value) -> Result<Value> {
    if let Some(enabled) = patch.as_bool() {
        let mut object = current.as_object().cloned().unwrap_or_default();
        object.insert("enabled".to_string(), Value::Bool(enabled));
        return Ok(Value::Object(object));
    }

    let patch_object = patch
        .as_object()
        .context("provider setting must be an object or boolean")?;
    let mut object = current.as_object().cloned().unwrap_or_default();
    for (key, value) in patch_object {
        match key.as_str() {
            "enabled" => {
                let enabled = value
                    .as_bool()
                    .context("provider enabled setting must be boolean")?;
                object.insert(key.clone(), Value::Bool(enabled));
            }
            "label" | "kind" => {
                if let Some(text) = value
                    .as_str()
                    .map(str::trim)
                    .filter(|text| !text.is_empty())
                {
                    object.insert(key.clone(), Value::String(text.chars().take(80).collect()));
                }
            }
            "base_url" | "baseUrl" => {
                let text = value
                    .as_str()
                    .map(str::trim)
                    .filter(|text| text.starts_with("http://") || text.starts_with("https://"))
                    .context("provider base_url must be an http(s) URL")?;
                object.insert("base_url".to_string(), Value::String(text.to_string()));
            }
            "timeout_ms" | "timeoutMs" => {
                let timeout_ms = value
                    .as_i64()
                    .filter(|value| (250..=30_000).contains(value))
                    .context("provider timeout_ms must be between 250 and 30000")?;
                object.insert("timeout_ms".to_string(), Value::Number(timeout_ms.into()));
            }
            "cache_ttl_seconds" | "cacheTtlSeconds" => {
                let ttl = value
                    .as_i64()
                    .filter(|value| (0..=(60 * 60 * 24 * 30)).contains(value))
                    .context("provider cache_ttl_seconds must be between 0 and 2592000")?;
                object.insert("cache_ttl_seconds".to_string(), Value::Number(ttl.into()));
            }
            "rate_limit_per_minute" | "rateLimitPerMinute" => {
                let limit = value
                    .as_i64()
                    .filter(|value| (1..=600).contains(value))
                    .context("provider rate_limit_per_minute must be between 1 and 600")?;
                object.insert(
                    "rate_limit_per_minute".to_string(),
                    Value::Number(limit.into()),
                );
            }
            "min_repeat_count" | "minRepeatCount" => {
                let count = value
                    .as_i64()
                    .filter(|value| (2..=20).contains(value))
                    .context("provider min_repeat_count must be between 2 and 20")?;
                object.insert("min_repeat_count".to_string(), Value::Number(count.into()));
            }
            "min_frame_count" | "minFrameCount" => {
                let count = value
                    .as_i64()
                    .filter(|value| (2..=20).contains(value))
                    .context("provider min_frame_count must be between 2 and 20")?;
                object.insert("min_frame_count".to_string(), Value::Number(count.into()));
            }
            "min_season_files" | "minSeasonFiles" => {
                let count = value
                    .as_i64()
                    .filter(|value| (2..=50).contains(value))
                    .context("provider min_season_files must be between 2 and 50")?;
                object.insert("min_season_files".to_string(), Value::Number(count.into()));
            }
            "min_span_seconds" | "minSpanSeconds" => {
                insert_float_provider_setting(&mut object, value, "min_span_seconds", 10.0, 600.0)?;
            }
            "min_start_fraction" | "minStartFraction" => {
                insert_float_provider_setting(
                    &mut object,
                    value,
                    "min_start_fraction",
                    0.50,
                    0.95,
                )?;
            }
            "max_frame_gap_seconds" | "maxFrameGapSeconds" => {
                insert_float_provider_setting(
                    &mut object,
                    value,
                    "max_frame_gap_seconds",
                    5.0,
                    300.0,
                )?;
            }
            "black_ratio_threshold" | "blackRatioThreshold" => {
                insert_float_provider_setting(
                    &mut object,
                    value,
                    "black_ratio_threshold",
                    0.10,
                    1.0,
                )?;
            }
            "text_ratio_threshold" | "textRatioThreshold" => {
                insert_float_provider_setting(
                    &mut object,
                    value,
                    "text_ratio_threshold",
                    0.01,
                    1.0,
                )?;
            }
            "post_credit_scene_min_frames" | "postCreditSceneMinFrames" => {
                let count = value
                    .as_i64()
                    .filter(|value| (1..=20).contains(value))
                    .context("provider post_credit_scene_min_frames must be between 1 and 20")?;
                object.insert(
                    "post_credit_scene_min_frames".to_string(),
                    Value::Number(count.into()),
                );
            }
            "post_credit_scene_min_span_seconds" | "postCreditSceneMinSpanSeconds" => {
                insert_float_provider_setting(
                    &mut object,
                    value,
                    "post_credit_scene_min_span_seconds",
                    5.0,
                    300.0,
                )?;
            }
            "frame_hash_step_seconds" | "frameHashStepSeconds" => {
                insert_float_provider_setting(
                    &mut object,
                    value,
                    "frame_hash_step_seconds",
                    5.0,
                    120.0,
                )?;
            }
            "frame_hash_max_frames" | "frameHashMaxFrames" => {
                let count = value
                    .as_i64()
                    .filter(|value| (10..=500).contains(value))
                    .context("provider frame_hash_max_frames must be between 10 and 500")?;
                object.insert(
                    "frame_hash_max_frames".to_string(),
                    Value::Number(count.into()),
                );
            }
            "fingerprint_timeout_seconds" | "fingerprintTimeoutSeconds" => {
                let seconds = value
                    .as_i64()
                    .filter(|value| (5..=600).contains(value))
                    .context("provider fingerprint_timeout_seconds must be between 5 and 600")?;
                object.insert(
                    "fingerprint_timeout_seconds".to_string(),
                    Value::Number(seconds.into()),
                );
            }
            "frame_hash_timeout_seconds" | "frameHashTimeoutSeconds" => {
                let seconds = value
                    .as_i64()
                    .filter(|value| (5..=900).contains(value))
                    .context("provider frame_hash_timeout_seconds must be between 5 and 900")?;
                object.insert(
                    "frame_hash_timeout_seconds".to_string(),
                    Value::Number(seconds.into()),
                );
            }
            _ => {}
        }
    }
    Ok(Value::Object(object))
}

fn insert_float_provider_setting(
    object: &mut serde_json::Map<String, Value>,
    value: &Value,
    output_key: &str,
    min: f64,
    max: f64,
) -> Result<()> {
    let number = value
        .as_f64()
        .filter(|value| value.is_finite() && *value >= min && *value <= max)
        .with_context(|| format!("provider {output_key} must be between {min} and {max}"))?;
    let json_number = serde_json::Number::from_f64(number)
        .with_context(|| format!("provider {output_key} must be finite"))?;
    object.insert(output_key.to_string(), Value::Number(json_number));
    Ok(())
}

fn normalize_provider_id(value: &str) -> Result<String> {
    let normalized = value.trim().to_ascii_lowercase().replace('-', "_");
    if normalized.is_empty()
        || normalized.len() > 80
        || !normalized
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '.')
    {
        bail!("invalid segment provider id");
    }
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet, HashMap},
        net::SocketAddr,
        path::{Path, PathBuf},
        sync::{Arc, Mutex},
    };

    use anyhow::{Context, Result, ensure};
    use axum::{
        Json, Router,
        http::StatusCode,
        routing::{get, post},
    };
    use serde::Deserialize;
    use serde_json::json;
    use tokio::net::TcpListener;
    use uuid::Uuid;

    use super::*;
    use crate::{
        config::DatabaseConfig,
        db::{
            Database, DatabaseDriver,
            models::{ExtensionKind, ExtensionTrustLevel, SlotCardinality},
        },
        extensions::store::{ExtensionStore, NewExtension, NewExtensionInstance, NewProvider},
    };

    const MIDM_DETECTOR_CORPUS_MANIFEST: &str =
        include_str!("../../docs/contracts/midm-detector-corpus.yml");

    #[derive(Debug, Deserialize)]
    struct MidmDetectorCorpusManifest {
        schema_version: u32,
        title: String,
        owner: String,
        purpose: String,
        enable_env: String,
        suite_filter_env: String,
        consumer_surface: String,
        support_surface: String,
        cache_roots: BTreeMap<String, PathBuf>,
        quality_gates: MidmDetectorQualityGates,
        #[serde(default)]
        cases: Vec<MidmDetectorCorpusCase>,
    }

    #[derive(Debug, Deserialize)]
    struct MidmDetectorQualityGates {
        min_cases: usize,
        min_negative_cases: usize,
        min_release_cases: usize,
        min_release_positive_cases: usize,
        min_release_negative_cases: usize,
        max_false_positive_segments: usize,
        #[serde(default)]
        required_detectors: Vec<String>,
        #[serde(default)]
        require_release_positive_and_negative_per_detector: bool,
        #[serde(default)]
        require_release_cases_private: bool,
    }

    #[derive(Debug, Deserialize)]
    struct MidmDetectorCorpusCase {
        id: String,
        title: String,
        detector: String,
        media_type: String,
        suite: String,
        source: String,
        #[serde(default)]
        release_required: bool,
        local_path: Option<PathBuf>,
        license_policy: String,
        #[serde(default)]
        labels: Vec<String>,
        #[serde(default)]
        files: Vec<MidmDetectorCorpusFile>,
        expected: MidmDetectorExpected,
    }

    #[derive(Debug, Deserialize)]
    struct MidmDetectorCorpusFile {
        episode_number: Option<i64>,
        local_path: PathBuf,
        duration_seconds: Option<f64>,
    }

    #[derive(Debug, Deserialize)]
    struct MidmDetectorExpected {
        #[serde(default)]
        max_candidates: Option<usize>,
        #[serde(default)]
        max_active_segments: Option<usize>,
        #[serde(default)]
        segment_count: Option<String>,
        #[serde(default)]
        segments: Vec<MidmExpectedSegment>,
    }

    #[derive(Debug, Deserialize)]
    struct MidmExpectedSegment {
        segment_type: String,
        start_seconds_min: f64,
        start_seconds_max: f64,
        end_seconds_min: f64,
        end_seconds_max: f64,
        confidence_min: f64,
    }

    #[derive(Debug, Default)]
    struct RealMediaDetectorCorpusReport {
        cases_run: usize,
        release_cases_run: usize,
        release_positive_cases_run: usize,
        release_negative_cases_run: usize,
        release_positive_detectors: BTreeSet<String>,
        release_negative_detectors: BTreeSet<String>,
        false_positive_segments: usize,
        failures: Vec<String>,
        skipped: Vec<String>,
    }

    async fn test_pool() -> Result<AnyPool> {
        let database = Database::connect(&DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        })
        .await?;
        assert_eq!(database.driver, DatabaseDriver::Sqlite);
        database.run_migrations().await?;
        Ok(database.pool)
    }

    fn load_midm_detector_corpus_manifest() -> Result<MidmDetectorCorpusManifest> {
        serde_yaml::from_str(MIDM_DETECTOR_CORPUS_MANIFEST)
            .context("parse MIDM detector corpus manifest")
    }

    #[test]
    fn midm_detector_corpus_contract_is_valid() -> Result<()> {
        let manifest = load_midm_detector_corpus_manifest()?;
        ensure!(
            manifest.schema_version == 1,
            "unexpected MIDM detector corpus schema version {}",
            manifest.schema_version
        );
        ensure!(
            manifest.owner == "media_interactions",
            "MIDM detector corpus owner drifted"
        );
        ensure!(
            !manifest.title.trim().is_empty() && !manifest.purpose.trim().is_empty(),
            "MIDM detector corpus must include title and purpose"
        );
        ensure!(
            manifest.enable_env == "ELIXIR_MIDM_DETECTOR_CORPUS",
            "MIDM detector corpus enable env drifted"
        );
        ensure!(
            manifest.suite_filter_env == "ELIXIR_MIDM_DETECTOR_CORPUS_SUITE",
            "MIDM detector corpus suite env drifted"
        );
        ensure!(
            manifest.consumer_surface == "automatic_skip_prompt_or_toggle_only",
            "MIDM detector corpus must preserve automatic-only consumer behavior"
        );
        ensure!(
            manifest.support_surface == "diagnostics_and_bad_marker_disable_only",
            "MIDM detector corpus must keep marker diagnostics support-only"
        );
        ensure!(
            manifest
                .cache_roots
                .get("public")
                .is_some_and(|path| path == Path::new("data/playback-corpus/public")),
            "public detector corpus root drifted"
        );
        ensure!(
            manifest
                .cache_roots
                .get("private")
                .is_some_and(|path| path == Path::new("data/midm-detector-corpus/private")),
            "private detector corpus root drifted"
        );
        ensure!(
            manifest.quality_gates.max_false_positive_segments == 0,
            "MIDM detector release gate must remain zero false positives"
        );

        let required_detectors = manifest
            .quality_gates
            .required_detectors
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        ensure!(
            required_detectors
                == BTreeSet::from([
                    PROVIDER_LOCAL_AUDIO_RECURRING,
                    PROVIDER_LOCAL_VISUAL_RECURRING
                ]),
            "MIDM detector corpus must cover local audio and local visual detectors"
        );
        ensure!(
            manifest.cases.len() >= manifest.quality_gates.min_cases,
            "MIDM detector corpus has {} cases but requires at least {}",
            manifest.cases.len(),
            manifest.quality_gates.min_cases
        );

        let mut ids = BTreeSet::new();
        let mut detectors_seen = BTreeSet::new();
        let mut negative_cases = 0usize;
        let mut release_cases = 0usize;
        let mut release_positive_cases = 0usize;
        let mut release_negative_cases = 0usize;
        let mut release_positive = BTreeSet::new();
        let mut release_negative = BTreeSet::new();

        for case in &manifest.cases {
            ensure!(
                ids.insert(case.id.as_str()),
                "duplicate MIDM detector corpus case {}",
                case.id
            );
            ensure!(
                case.id
                    .chars()
                    .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_'),
                "{} must use stable lowercase snake_case id",
                case.id
            );
            ensure!(
                !case.title.trim().is_empty() && !case.license_policy.trim().is_empty(),
                "{} must include title and license policy",
                case.id
            );
            ensure!(
                ["smoke", "release", "tuning"].contains(&case.suite.as_str()),
                "{} has unsupported suite {}",
                case.id,
                case.suite
            );
            ensure!(
                required_detectors.contains(case.detector.as_str()),
                "{} uses unsupported detector {}",
                case.id,
                case.detector
            );
            ensure!(
                ["movie", "episode", "series_season"].contains(&case.media_type.as_str()),
                "{} has unsupported media type {}",
                case.id,
                case.media_type
            );
            ensure!(
                manifest.cache_roots.contains_key(&case.source),
                "{} source {} has no declared cache root",
                case.id,
                case.source
            );
            ensure!(
                case.labels
                    .iter()
                    .any(|label| label == &format!("detector:{}", case.detector)),
                "{} must include detector label",
                case.id
            );
            ensure!(
                case.labels
                    .iter()
                    .any(|label| label == &format!("suite:{}", case.suite)),
                "{} must include suite label",
                case.id
            );
            ensure!(
                case.labels
                    .iter()
                    .any(|label| label == &format!("source:{}", case.source)),
                "{} must include source label",
                case.id
            );
            ensure!(
                case.labels
                    .iter()
                    .any(|label| label == "surface:automatic-only"),
                "{} must remain automatic-only for normal users",
                case.id
            );

            let expected_positive = !case.expected.segments.is_empty();
            let is_release_case = case.suite == "release" || case.release_required;
            if is_release_case {
                ensure!(
                    case.suite == "release" && case.release_required,
                    "{} release cases must use suite=release and release_required=true",
                    case.id
                );
                if manifest.quality_gates.require_release_cases_private {
                    ensure!(
                        case.source == "private",
                        "{} release cases must use the private corpus root",
                        case.id
                    );
                }
                release_cases += 1;
                if expected_positive {
                    release_positive_cases += 1;
                } else {
                    release_negative_cases += 1;
                }
            }
            let expected_label = if expected_positive {
                "expectation:positive"
            } else {
                "expectation:negative"
            };
            ensure!(
                case.labels.iter().any(|label| label == expected_label),
                "{} must include {} label",
                case.id,
                expected_label
            );
            if expected_positive {
                if is_release_case {
                    release_positive.insert(case.detector.as_str());
                }
            } else {
                negative_cases += 1;
                ensure!(
                    case.expected.max_candidates == Some(0)
                        && case.expected.max_active_segments == Some(0),
                    "{} negative detector case must cap candidates and active segments at zero",
                    case.id
                );
                if is_release_case {
                    release_negative.insert(case.detector.as_str());
                }
            }

            match case.detector.as_str() {
                PROVIDER_LOCAL_AUDIO_RECURRING => {
                    ensure!(
                        case.media_type == "series_season" && case.local_path.is_none(),
                        "{} audio detector cases must be season-scoped file groups",
                        case.id
                    );
                    ensure!(
                        case.files.len() >= 2,
                        "{} audio detector cases need at least two episode files",
                        case.id
                    );
                    for file in &case.files {
                        validate_midm_detector_corpus_path(
                            &manifest,
                            &case.source,
                            &file.local_path,
                        )
                        .with_context(|| format!("{} invalid file path", case.id))?;
                        if let Some(duration_seconds) = file.duration_seconds {
                            ensure!(
                                duration_seconds > LOCAL_AUDIO_FINGERPRINT_MIN_DURATION_SECONDS,
                                "{} file duration must exceed audio fingerprint minimum",
                                case.id
                            );
                        }
                    }
                }
                PROVIDER_LOCAL_VISUAL_RECURRING => {
                    ensure!(
                        case.media_type == "movie" || case.media_type == "episode",
                        "{} visual detector cases must be media-file scoped",
                        case.id
                    );
                    let path = case
                        .local_path
                        .as_ref()
                        .with_context(|| format!("{} missing local_path", case.id))?;
                    validate_midm_detector_corpus_path(&manifest, &case.source, path)
                        .with_context(|| format!("{} invalid local_path", case.id))?;
                    ensure!(
                        case.files.is_empty(),
                        "{} visual detector cases should not declare episode file groups",
                        case.id
                    );
                }
                _ => unreachable!("validated detectors above"),
            }

            for segment in &case.expected.segments {
                validate_midm_expected_segment(&case.id, &case.detector, segment)?;
            }
            detectors_seen.insert(case.detector.as_str());
        }

        ensure!(
            negative_cases >= manifest.quality_gates.min_negative_cases,
            "MIDM detector corpus has {negative_cases} negative cases but requires at least {}",
            manifest.quality_gates.min_negative_cases
        );
        ensure!(
            release_cases >= manifest.quality_gates.min_release_cases,
            "MIDM detector corpus has {release_cases} release cases but requires at least {}",
            manifest.quality_gates.min_release_cases
        );
        ensure!(
            release_positive_cases >= manifest.quality_gates.min_release_positive_cases,
            "MIDM detector corpus has {release_positive_cases} release positive cases but requires at least {}",
            manifest.quality_gates.min_release_positive_cases
        );
        ensure!(
            release_negative_cases >= manifest.quality_gates.min_release_negative_cases,
            "MIDM detector corpus has {release_negative_cases} release negative cases but requires at least {}",
            manifest.quality_gates.min_release_negative_cases
        );
        ensure!(
            detectors_seen == required_detectors,
            "MIDM detector corpus detector coverage drifted"
        );
        if manifest
            .quality_gates
            .require_release_positive_and_negative_per_detector
        {
            ensure!(
                release_positive == required_detectors,
                "release-required positive detector coverage is incomplete"
            );
            ensure!(
                release_negative == required_detectors,
                "release-required negative detector coverage is incomplete"
            );
        }

        Ok(())
    }

    #[test]
    fn midm_real_media_release_report_requires_executed_detector_coverage() -> Result<()> {
        let manifest = load_midm_detector_corpus_manifest()?;
        let empty_error = validate_midm_real_media_release_report(
            &manifest,
            &RealMediaDetectorCorpusReport::default(),
        )
        .expect_err("empty real-media release report must fail");
        assert!(
            empty_error.to_string().contains("ran 0 release cases"),
            "unexpected empty release report error: {empty_error:?}"
        );

        let required_detectors = manifest
            .quality_gates
            .required_detectors
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let complete_report = RealMediaDetectorCorpusReport {
            cases_run: manifest.quality_gates.min_release_cases,
            release_cases_run: manifest.quality_gates.min_release_cases,
            release_positive_cases_run: manifest.quality_gates.min_release_positive_cases,
            release_negative_cases_run: manifest.quality_gates.min_release_negative_cases,
            release_positive_detectors: required_detectors.clone(),
            release_negative_detectors: required_detectors,
            false_positive_segments: 0,
            failures: Vec::new(),
            skipped: Vec::new(),
        };
        validate_midm_real_media_release_report(&manifest, &complete_report)?;

        let missing_negative_detector_report = RealMediaDetectorCorpusReport {
            release_negative_detectors: BTreeSet::new(),
            ..complete_report
        };
        let coverage_error =
            validate_midm_real_media_release_report(&manifest, &missing_negative_detector_report)
                .expect_err("missing negative detector coverage must fail");
        assert!(
            coverage_error
                .to_string()
                .contains("negative detector coverage incomplete"),
            "unexpected coverage error: {coverage_error:?}"
        );

        Ok(())
    }

    fn validate_midm_detector_corpus_path(
        manifest: &MidmDetectorCorpusManifest,
        source: &str,
        path: &Path,
    ) -> Result<()> {
        ensure!(
            !path.is_absolute(),
            "{} must be repo-relative, not absolute",
            path.display()
        );
        let root = manifest
            .cache_roots
            .get(source)
            .with_context(|| format!("missing detector corpus root for {source}"))?;
        ensure!(
            path.starts_with(root),
            "{} must stay under {}",
            path.display(),
            root.display()
        );
        Ok(())
    }

    fn validate_midm_expected_segment(
        case_id: &str,
        detector: &str,
        segment: &MidmExpectedSegment,
    ) -> Result<()> {
        match detector {
            PROVIDER_LOCAL_AUDIO_RECURRING => ensure!(
                ["intro", "outro"].contains(&segment.segment_type.as_str()),
                "{} audio detector expected unsupported segment type {}",
                case_id,
                segment.segment_type
            ),
            PROVIDER_LOCAL_VISUAL_RECURRING => ensure!(
                segment.segment_type == "credits",
                "{} visual detector expected unsupported segment type {}",
                case_id,
                segment.segment_type
            ),
            _ => unreachable!("validated detectors above"),
        }
        ensure!(
            segment.start_seconds_min >= 0.0
                && segment.start_seconds_max >= segment.start_seconds_min
                && segment.end_seconds_min > segment.start_seconds_min
                && segment.end_seconds_max >= segment.end_seconds_min,
            "{} has invalid expected segment bounds",
            case_id
        );
        ensure!(
            (0.0..=1.0).contains(&segment.confidence_min),
            "{} has invalid expected confidence minimum",
            case_id
        );
        Ok(())
    }

    #[test]
    fn parses_media_segment_job_timestamps() {
        assert!(parse_media_segment_job_timestamp("2026-07-04 18:30:15").is_some());
        assert!(parse_media_segment_job_timestamp("2026-07-04T18:30:15Z").is_some());
        assert!(parse_media_segment_job_timestamp("").is_none());
    }

    #[test]
    fn default_provider_settings_include_canonical_detector_tuning_defaults() -> Result<()> {
        let settings = default_segment_provider_settings();

        assert_eq!(
            settings.pointer("/local_audio_recurring/min_repeat_count"),
            Some(&json!(LOCAL_AUDIO_DETECTOR_MIN_REPEAT_COUNT))
        );
        assert_eq!(
            settings.pointer("/local_audio_recurring/min_season_files"),
            Some(&json!(LOCAL_AUDIO_DETECTOR_MIN_SEASON_FILES))
        );
        assert_eq!(
            settings.pointer("/local_audio_recurring/fingerprint_timeout_seconds"),
            Some(&json!(LOCAL_AUDIO_FINGERPRINT_TIMEOUT_SECONDS))
        );
        assert_eq!(
            settings.pointer("/local_visual_recurring/min_frame_count"),
            Some(&json!(LOCAL_VISUAL_CREDITS_MIN_FRAME_COUNT))
        );
        assert_eq!(
            settings.pointer("/local_visual_recurring/min_span_seconds"),
            Some(&json!(LOCAL_VISUAL_CREDITS_MIN_SPAN_SECONDS))
        );
        assert_eq!(
            settings.pointer("/local_visual_recurring/min_start_fraction"),
            Some(&json!(LOCAL_VISUAL_CREDITS_MIN_START_FRACTION))
        );
        assert_eq!(
            settings.pointer("/local_visual_recurring/frame_hash_step_seconds"),
            Some(&json!(LOCAL_VISUAL_FRAME_HASH_STEP_SECONDS))
        );
        assert_eq!(
            settings.pointer("/local_visual_recurring/frame_hash_max_frames"),
            Some(&json!(LOCAL_VISUAL_FRAME_HASH_MAX_FRAMES_PER_FILE))
        );

        Ok(())
    }

    #[test]
    fn provider_settings_normalize_desktop_detector_tuning_payload() -> Result<()> {
        let merged = merge_segment_provider_settings(
            &default_segment_provider_settings(),
            json!({
                "local_audio_recurring": {
                    "enabled": true,
                    "minRepeatCount": 4,
                    "minSeasonFiles": 7,
                    "fingerprintTimeoutSeconds": 240
                },
                "local_visual_recurring": {
                    "enabled": true,
                    "minFrameCount": 5,
                    "minSpanSeconds": 45.5,
                    "minStartFraction": 0.75,
                    "maxFrameGapSeconds": 120.0,
                    "postCreditSceneMinFrames": 3,
                    "postCreditSceneMinSpanSeconds": 30.0,
                    "blackRatioThreshold": 0.8,
                    "textRatioThreshold": 0.12,
                    "frameHashTimeoutSeconds": 360,
                    "frameHashStepSeconds": 20.0,
                    "frameHashMaxFrames": 180
                }
            }),
        )?;

        let audio = provider_settings_for(&merged, PROVIDER_LOCAL_AUDIO_RECURRING)
            .context("missing normalized local audio settings")?;
        assert!(provider_settings_enabled(Some(&audio)));
        assert_eq!(local_audio_min_repeat_count(Some(&audio)), 4);
        assert_eq!(local_audio_min_season_files(Some(&audio)), 7);
        assert_eq!(
            audio.pointer("/fingerprint_timeout_seconds"),
            Some(&json!(240))
        );
        assert!(audio.get("minRepeatCount").is_none());
        assert!(audio.get("minSeasonFiles").is_none());

        let visual = provider_settings_for(&merged, PROVIDER_LOCAL_VISUAL_RECURRING)
            .context("missing normalized local visual settings")?;
        assert!(provider_settings_enabled(Some(&visual)));
        assert_eq!(local_visual_min_frame_count(Some(&visual)), 5);
        assert_eq!(local_visual_min_span_seconds(Some(&visual)), 45.5);
        assert_eq!(local_visual_min_start_fraction(Some(&visual)), 0.75);
        assert_eq!(local_visual_max_frame_gap_seconds(Some(&visual)), 120.0);
        assert_eq!(local_visual_post_credit_scene_min_frames(Some(&visual)), 3);
        assert_eq!(
            local_visual_post_credit_scene_min_span_seconds(Some(&visual)),
            30.0
        );
        assert_eq!(local_visual_black_ratio_threshold(Some(&visual)), 0.8);
        assert_eq!(local_visual_text_ratio_threshold(Some(&visual)), 0.12);
        assert_eq!(local_visual_frame_hash_timeout_seconds(Some(&visual)), 360);
        assert_eq!(local_visual_frame_hash_step_seconds(Some(&visual)), 20.0);
        assert_eq!(local_visual_frame_hash_max_frames(Some(&visual)), 180);
        assert!(visual.get("minFrameCount").is_none());
        assert!(visual.get("frameHashMaxFrames").is_none());

        Ok(())
    }

    #[test]
    fn provider_settings_reject_out_of_range_detector_tuning() {
        let cases = [
            json!({"local_audio_recurring": {"minRepeatCount": 1}}),
            json!({"local_audio_recurring": {"minSeasonFiles": 51}}),
            json!({"local_audio_recurring": {"fingerprintTimeoutSeconds": 601}}),
            json!({"local_visual_recurring": {"minFrameCount": 1}}),
            json!({"local_visual_recurring": {"minSpanSeconds": 9.9}}),
            json!({"local_visual_recurring": {"minStartFraction": 0.96}}),
            json!({"local_visual_recurring": {"maxFrameGapSeconds": 301.0}}),
            json!({"local_visual_recurring": {"postCreditSceneMinFrames": 21}}),
            json!({"local_visual_recurring": {"postCreditSceneMinSpanSeconds": 4.9}}),
            json!({"local_visual_recurring": {"blackRatioThreshold": 0.09}}),
            json!({"local_visual_recurring": {"textRatioThreshold": 0.009}}),
            json!({"local_visual_recurring": {"frameHashTimeoutSeconds": 901}}),
            json!({"local_visual_recurring": {"frameHashStepSeconds": 4.9}}),
            json!({"local_visual_recurring": {"frameHashMaxFrames": 501}}),
        ];

        for patch in cases {
            assert!(
                merge_segment_provider_settings(&default_segment_provider_settings(), patch)
                    .is_err()
            );
        }
    }

    async fn seed_movie_file(pool: &AnyPool, duration_seconds: f64) -> Result<(String, String)> {
        let media_item_id = Uuid::new_v4().to_string();
        let media_file_id = Uuid::new_v4().to_string();
        let movie_id = Uuid::new_v4().to_string();
        sqlx::query("INSERT INTO media_items (id, type, title, runtime_seconds, external_ids) VALUES ($1, 'movie', 'Segment Movie', $2, '{}')")
            .bind(&media_item_id)
            .bind(duration_seconds.round() as i64)
            .execute(pool)
            .await?;
        sqlx::query("INSERT INTO media_files (id, media_item_id, path, size_bytes, scan_state) VALUES ($1, $2, $3, 1024, 'ok')")
            .bind(&media_file_id)
            .bind(&media_item_id)
            .bind(format!("/media/{media_file_id}.mkv"))
            .execute(pool)
            .await?;
        sqlx::query(
            "INSERT INTO movies (id, title, runtime_seconds) VALUES ($1, 'Segment Movie', $2)",
        )
        .bind(&movie_id)
        .bind(duration_seconds.round() as i64)
        .execute(pool)
        .await?;
        sqlx::query("INSERT INTO movie_files (movie_id, media_file_id) VALUES ($1, $2)")
            .bind(&movie_id)
            .bind(&media_file_id)
            .execute(pool)
            .await?;
        sqlx::query(
            "INSERT INTO media_file_probes
                (media_file_id, probe_version, ffprobe_version, probe_status, probed_at,
                 source_mtime_ms, source_size_bytes, normalized_json, raw_json, error,
                 created_at, updated_at)
             VALUES ($1, 2, 'test', 'ok', CURRENT_TIMESTAMP, 1, 1024, $2, NULL, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .bind(&media_file_id)
        .bind(json!({"duration_seconds": duration_seconds}).to_string())
        .execute(pool)
        .await?;
        Ok((media_file_id, movie_id))
    }

    async fn upsert_video_frame_hash(
        pool: &AnyPool,
        media_file_id: &str,
        duration_seconds: f64,
        payload: Value,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO media_file_fingerprints
                (media_file_id, duration_seconds, file_size_bytes, video_frame_hash_json,
                 fingerprint_version, computed_at)
             VALUES ($1, $2, 1024, $3, 'test-visual-v1', CURRENT_TIMESTAMP)
             ON CONFLICT(media_file_id) DO UPDATE SET
                 duration_seconds = excluded.duration_seconds,
                 file_size_bytes = excluded.file_size_bytes,
                 video_frame_hash_json = excluded.video_frame_hash_json,
                 fingerprint_version = excluded.fingerprint_version,
                 computed_at = CURRENT_TIMESTAMP",
        )
        .bind(media_file_id)
        .bind(duration_seconds.round() as i64)
        .bind(payload.to_string())
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn seed_anime_episode_file(
        pool: &AnyPool,
        duration_seconds: f64,
    ) -> Result<(String, String, String)> {
        let media_item_id = Uuid::new_v4().to_string();
        let media_file_id = Uuid::new_v4().to_string();
        let series_id = Uuid::new_v4().to_string();
        let season_id = Uuid::new_v4().to_string();
        let episode_id = Uuid::new_v4().to_string();
        sqlx::query("INSERT INTO media_items (id, type, title, runtime_seconds, external_ids) VALUES ($1, 'tv', 'Segment Anime', $2, '{}')")
            .bind(&media_item_id)
            .bind(duration_seconds.round() as i64)
            .execute(pool)
            .await?;
        sqlx::query("INSERT INTO media_files (id, media_item_id, path, size_bytes, scan_state) VALUES ($1, $2, $3, 1024, 'ok')")
            .bind(&media_file_id)
            .bind(&media_item_id)
            .bind(format!("/media/{media_file_id}.mkv"))
            .execute(pool)
            .await?;
        sqlx::query("INSERT INTO series (id, title, library_type, external_anilist) VALUES ($1, 'Segment Anime', 'anime', '21')")
            .bind(&series_id)
            .execute(pool)
            .await?;
        sqlx::query("INSERT INTO series_external_ids (id, series_id, provider, external_id, confidence, source) VALUES ($1, $2, 'mal', '1535', 1.0, 'test')")
            .bind(Uuid::new_v4().to_string())
            .bind(&series_id)
            .execute(pool)
            .await?;
        sqlx::query("INSERT INTO seasons (id, series_id, season_number, title) VALUES ($1, $2, 1, 'Season 1')")
            .bind(&season_id)
            .bind(&series_id)
            .execute(pool)
            .await?;
        sqlx::query(
            "INSERT INTO episodes
                (id, series_id, season_id, season_number, episode_number,
                 absolute_episode_number, title, runtime_seconds, has_file)
             VALUES ($1, $2, $3, 1, 1, 1, 'Episode 1', $4, 1)",
        )
        .bind(&episode_id)
        .bind(&series_id)
        .bind(&season_id)
        .bind(duration_seconds.round() as i64)
        .execute(pool)
        .await?;
        sqlx::query("INSERT INTO episode_files (episode_id, media_file_id) VALUES ($1, $2)")
            .bind(&episode_id)
            .bind(&media_file_id)
            .execute(pool)
            .await?;
        sqlx::query(
            "INSERT INTO media_file_probes
                (media_file_id, probe_version, ffprobe_version, probe_status, probed_at,
                 source_mtime_ms, source_size_bytes, normalized_json, raw_json, error,
                 created_at, updated_at)
             VALUES ($1, 2, 'test', 'ok', CURRENT_TIMESTAMP, 1, 1024, $2, NULL, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .bind(&media_file_id)
        .bind(json!({"duration_seconds": duration_seconds}).to_string())
        .execute(pool)
        .await?;
        Ok((media_file_id, series_id, episode_id))
    }

    async fn seed_audio_detector_season(
        pool: &AnyPool,
        fingerprints: Vec<Value>,
    ) -> Result<(String, Vec<String>)> {
        let media_item_id = Uuid::new_v4().to_string();
        let series_id = Uuid::new_v4().to_string();
        let season_id = Uuid::new_v4().to_string();
        sqlx::query("INSERT INTO media_items (id, type, title, runtime_seconds, external_ids) VALUES ($1, 'tv', 'Audio Detector Series', 1500, '{}')")
            .bind(&media_item_id)
            .execute(pool)
            .await?;
        sqlx::query(
            "INSERT INTO series (id, title, library_type) VALUES ($1, 'Audio Detector Series', 'series')",
        )
        .bind(&series_id)
        .execute(pool)
        .await?;
        sqlx::query("INSERT INTO seasons (id, series_id, season_number, title) VALUES ($1, $2, 1, 'Season 1')")
            .bind(&season_id)
            .bind(&series_id)
            .execute(pool)
            .await?;

        let mut media_file_ids = Vec::new();
        for (index, fingerprint) in fingerprints.into_iter().enumerate() {
            let episode_number = index as i64 + 1;
            let episode_id = Uuid::new_v4().to_string();
            let media_file_id = Uuid::new_v4().to_string();
            sqlx::query(
                "INSERT INTO episodes
                    (id, series_id, season_id, season_number, episode_number,
                     absolute_episode_number, title, runtime_seconds, has_file)
                 VALUES ($1, $2, $3, 1, $4, $5, $6, 1500, 1)",
            )
            .bind(&episode_id)
            .bind(&series_id)
            .bind(&season_id)
            .bind(episode_number)
            .bind(episode_number)
            .bind(format!("Episode {episode_number}"))
            .execute(pool)
            .await?;
            sqlx::query("INSERT INTO media_files (id, media_item_id, path, size_bytes, scan_state) VALUES ($1, $2, $3, 1024, 'ok')")
                .bind(&media_file_id)
                .bind(&media_item_id)
                .bind(format!("/media/{media_file_id}.mkv"))
                .execute(pool)
                .await?;
            sqlx::query("INSERT INTO episode_files (episode_id, media_file_id) VALUES ($1, $2)")
                .bind(&episode_id)
                .bind(&media_file_id)
                .execute(pool)
                .await?;
            sqlx::query(
                "INSERT INTO media_file_fingerprints
                    (media_file_id, duration_seconds, file_size_bytes, audio_fingerprint_json,
                     fingerprint_version, computed_at)
                 VALUES ($1, 1500, 1024, $2, 'test-audio-v1', CURRENT_TIMESTAMP)",
            )
            .bind(&media_file_id)
            .bind(fingerprint.to_string())
            .execute(pool)
            .await?;
            media_file_ids.push(media_file_id);
        }

        Ok((season_id, media_file_ids))
    }

    async fn seed_audio_detector_season_without_fingerprints(
        pool: &AnyPool,
        episode_count: usize,
    ) -> Result<(String, Vec<String>)> {
        let media_item_id = Uuid::new_v4().to_string();
        let series_id = Uuid::new_v4().to_string();
        let season_id = Uuid::new_v4().to_string();
        sqlx::query("INSERT INTO media_items (id, type, title, runtime_seconds, external_ids) VALUES ($1, 'tv', 'Unfingerprinted Series', 1500, '{}')")
            .bind(&media_item_id)
            .execute(pool)
            .await?;
        sqlx::query(
            "INSERT INTO series (id, title, library_type) VALUES ($1, 'Unfingerprinted Series', 'series')",
        )
        .bind(&series_id)
        .execute(pool)
        .await?;
        sqlx::query("INSERT INTO seasons (id, series_id, season_number, title) VALUES ($1, $2, 1, 'Season 1')")
            .bind(&season_id)
            .bind(&series_id)
            .execute(pool)
            .await?;

        let mut media_file_ids = Vec::new();
        for index in 0..episode_count {
            let episode_number = index as i64 + 1;
            let episode_id = Uuid::new_v4().to_string();
            let media_file_id = Uuid::new_v4().to_string();
            sqlx::query(
                "INSERT INTO episodes
                    (id, series_id, season_id, season_number, episode_number,
                     absolute_episode_number, title, runtime_seconds, has_file)
                 VALUES ($1, $2, $3, 1, $4, $5, $6, 1500, 1)",
            )
            .bind(&episode_id)
            .bind(&series_id)
            .bind(&season_id)
            .bind(episode_number)
            .bind(episode_number)
            .bind(format!("Episode {episode_number}"))
            .execute(pool)
            .await?;
            sqlx::query("INSERT INTO media_files (id, media_item_id, path, size_bytes, scan_state) VALUES ($1, $2, $3, 1024, 'ok')")
                .bind(&media_file_id)
                .bind(&media_item_id)
                .bind(format!("/missing/{media_file_id}.mkv"))
                .execute(pool)
                .await?;
            sqlx::query("INSERT INTO episode_files (episode_id, media_file_id) VALUES ($1, $2)")
                .bind(&episode_id)
                .bind(&media_file_id)
                .execute(pool)
                .await?;
            media_file_ids.push(media_file_id);
        }

        Ok((season_id, media_file_ids))
    }

    fn synthetic_pcm(seed: i16, seconds: usize) -> Vec<u8> {
        let sample_rate = LOCAL_AUDIO_FINGERPRINT_SAMPLE_RATE_HZ as usize;
        let mut bytes = Vec::with_capacity(seconds * sample_rate * 2);
        for index in 0..seconds * sample_rate {
            let phase = ((index as i32 * i32::from(seed)) % 4096) - 2048;
            let sample = (phase * 8).clamp(i32::from(i16::MIN), i32::from(i16::MAX)) as i16;
            bytes.extend_from_slice(&sample.to_le_bytes());
        }
        bytes
    }

    async fn fake_provider_base_url(app: Router) -> Result<String> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address: SocketAddr = listener.local_addr()?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(format!("http://{address}"))
    }

    async fn seed_marketplace_segment_provider(
        pool: &AnyPool,
        base_url: &str,
        implementation: &str,
        media_types: &[&str],
        segment_types: &[&str],
    ) -> Result<Uuid> {
        let base_url = reqwest::Url::parse(base_url)?;
        let host = base_url
            .host_str()
            .context("marketplace segment provider fixture host")?;
        let port = base_url
            .port_or_known_default()
            .context("marketplace segment provider fixture port")?;
        let base_path = if base_url.path().trim().is_empty() {
            "/"
        } else {
            base_url.path()
        };
        let normalized = normalize_provider_id(implementation)?;
        let extension_id = format!("elixir.test.segment_provider.{normalized}");
        let instance_id = Uuid::new_v4();
        let provider_id = Uuid::new_v4();
        let store = ExtensionStore::new(pool);
        store
            .upsert_extension(&NewExtension {
                extension_id: extension_id.clone(),
                name: "Fixture Segment Provider".to_string(),
                version: "1.2.3".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: Some("Elixir Test".to_string()),
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Community,
                manifest_json: json!({
                    "id": extension_id,
                    "version": "1.2.3",
                    "kind": "module",
                    "name": "Fixture Segment Provider",
                    "provides": [{
                        "capability": MEDIA_SEGMENT_PROVIDER_CAPABILITY,
                        "slot": "default",
                        "cardinality": "one",
                        "implementation": implementation,
                        "scope": {
                            "media_types": media_types,
                            "segment_types": segment_types,
                            "actions": ["lookup"]
                        }
                    }]
                }),
                package_hash: None,
                enabled: true,
            })
            .await?;
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: format!("elixir.test.segment_provider.{normalized}"),
                instance_name: "Fixture Instance".to_string(),
                config_json: Some(json!({"fixture": true})),
                enabled: true,
            })
            .await?;
        store
            .upsert_provider(&NewProvider {
                provider_id,
                instance_id,
                capability: MEDIA_SEGMENT_PROVIDER_CAPABILITY.to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: Some(implementation.to_string()),
                scope_json: Some(json!({
                    "media_types": media_types,
                    "segment_types": segment_types,
                    "actions": ["lookup"]
                })),
                endpoint_json: Some(json!({
                    "scheme": base_url.scheme(),
                    "host": host,
                    "port": port,
                    "base_path": base_path,
                    "network": null
                })),
                health_state: ProviderHealthState::Healthy,
            })
            .await?;
        Ok(provider_id)
    }

    fn preferences_with_provider_urls(
        theintrodb_base_url: Option<&str>,
        aniskip_base_url: Option<&str>,
    ) -> Result<PlaybackInteractionPreferences> {
        let mut preferences = default_playback_preferences();
        let mut patch = json!({
            "theintrodb": false,
            "aniskip": false
        });
        if let Some(base_url) = theintrodb_base_url {
            patch["theintrodb"] = json!({
                "enabled": true,
                "base_url": base_url,
                "cache_ttl_seconds": 3600
            });
        }
        if let Some(base_url) = aniskip_base_url {
            patch["aniskip"] = json!({
                "enabled": true,
                "base_url": base_url,
                "cache_ttl_seconds": 3600
            });
        }
        preferences.segment_provider_settings =
            merge_segment_provider_settings(&preferences.segment_provider_settings, patch)?;
        Ok(preferences)
    }

    #[test]
    fn theintrodb_default_base_url_targets_public_api_host() {
        assert_eq!(DEFAULT_THEINTRODB_BASE_URL, "https://api.introdb.app");
    }

    fn preferences_with_local_audio_detector() -> Result<PlaybackInteractionPreferences> {
        let mut preferences = default_playback_preferences();
        preferences.segment_provider_settings = merge_segment_provider_settings(
            &preferences.segment_provider_settings,
            json!({
                "theintrodb": false,
                "aniskip": false,
                "local_audio_recurring": {
                    "enabled": true,
                    "min_repeat_count": 2,
                    "min_season_files": 2
                }
            }),
        )?;
        Ok(preferences)
    }

    fn preferences_with_local_visual_detector() -> Result<PlaybackInteractionPreferences> {
        let mut preferences = default_playback_preferences();
        preferences.segment_provider_settings = merge_segment_provider_settings(
            &preferences.segment_provider_settings,
            json!({
                "theintrodb": false,
                "aniskip": false,
                "local_visual_recurring": {
                    "enabled": true,
                    "min_frame_count": 3,
                    "min_span_seconds": 20.0,
                    "min_start_fraction": 0.60,
                    "post_credit_scene_min_frames": 2,
                    "post_credit_scene_min_span_seconds": 20.0,
                    "frame_hash_step_seconds": 30.0,
                    "frame_hash_max_frames": 120
                }
            }),
        )?;
        Ok(preferences)
    }

    fn chapter(id: i64, start: &str, end: &str, title: &str) -> ffprobe::Chapter {
        ffprobe::Chapter {
            id: Some(id),
            start_time: Some(start.to_string()),
            end_time: Some(end.to_string()),
            tags: Some(HashMap::from([("title".to_string(), title.to_string())])),
        }
    }

    #[tokio::test]
    async fn chapter_provider_ingests_and_activates_known_segments() -> Result<()> {
        let pool = test_pool().await?;
        let (media_file_id, _) = seed_movie_file(&pool, 1800.0).await?;
        let metadata = ffprobe::MediaMetadata {
            duration_seconds: Some(1800),
            chapters: vec![
                chapter(1, "0.0", "90.0", "Intro"),
                chapter(2, "90.0", "1700.0", "Chapter 02"),
                chapter(3, "1700.0", "1800.0", "Credits"),
            ],
            ..ffprobe::MediaMetadata::default()
        };

        let summary =
            ingest_chapter_segments_from_metadata(&pool, &media_file_id, &metadata).await?;
        assert_eq!(summary.chapters_seen, 3);
        assert_eq!(summary.candidates_submitted, 2);
        assert_eq!(summary.candidates_accepted, 2);

        let active = list_active_segments_for_file(&pool, &media_file_id).await?;
        let segment_types = active
            .iter()
            .map(|segment| segment.segment_type.as_str())
            .collect::<Vec<_>>();
        assert_eq!(segment_types, vec!["intro", "credits"]);
        assert!(active.iter().all(|segment| segment.status == "active"));
        Ok(())
    }

    #[tokio::test]
    async fn invalid_candidate_is_preserved_but_not_activated() -> Result<()> {
        let pool = test_pool().await?;
        let (media_file_id, _) = seed_movie_file(&pool, 1800.0).await?;

        let outcome = submit_segment_candidate(
            &pool,
            SegmentCandidateInput {
                media_file_id: media_file_id.clone(),
                item_type: None,
                item_id: None,
                segment_type: "intro".to_string(),
                start_seconds: 5.0,
                end_seconds: 10.0,
                provider_kind: PROVIDER_CHAPTER.to_string(),
                provider_id: CHAPTER_PROVIDER_ID.to_string(),
                provider_version: Some(CHAPTER_PROVIDER_VERSION.to_string()),
                confidence: 0.9,
                identity_strength: "file_fingerprint".to_string(),
                source_payload: Some(json!({"title": "Intro"})),
            },
        )
        .await?;

        assert_eq!(outcome.candidate.validation_state, "rejected");
        assert_eq!(
            outcome.candidate.validation_reason.as_deref(),
            Some("segment_too_short")
        );
        assert!(outcome.activated_segment.is_none());
        assert!(
            list_active_segments_for_file(&pool, &media_file_id)
                .await?
                .is_empty()
        );
        Ok(())
    }

    #[tokio::test]
    async fn imported_candidate_supersedes_overlapping_provider_segment_without_locking()
    -> Result<()> {
        let pool = test_pool().await?;
        let (media_file_id, movie_id) = seed_movie_file(&pool, 1800.0).await?;

        submit_segment_candidate(
            &pool,
            SegmentCandidateInput {
                media_file_id: media_file_id.clone(),
                item_type: None,
                item_id: None,
                segment_type: "intro".to_string(),
                start_seconds: 0.0,
                end_seconds: 90.0,
                provider_kind: PROVIDER_CHAPTER.to_string(),
                provider_id: CHAPTER_PROVIDER_ID.to_string(),
                provider_version: Some(CHAPTER_PROVIDER_VERSION.to_string()),
                confidence: 0.85,
                identity_strength: "file_fingerprint".to_string(),
                source_payload: Some(json!({"title": "Intro"})),
            },
        )
        .await?;

        let imported = submit_segment_candidate(
            &pool,
            SegmentCandidateInput {
                media_file_id: media_file_id.clone(),
                item_type: Some("movie".to_string()),
                item_id: Some(movie_id),
                segment_type: "intro".to_string(),
                start_seconds: 2.0,
                end_seconds: 95.0,
                provider_kind: "imported".to_string(),
                provider_id: "trusted_import".to_string(),
                provider_version: Some("1".to_string()),
                confidence: 1.0,
                identity_strength: "file_fingerprint".to_string(),
                source_payload: Some(json!({"title": "Imported intro"})),
            },
        )
        .await?;

        assert_eq!(imported.candidate.validation_state, "accepted");
        let active = list_active_segments_for_file(&pool, &media_file_id).await?;
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].source_label, "Imported intro");
        assert!(
            !active[0].locked,
            "MIDM must not create hidden manual/admin locked markers"
        );

        let superseded_count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM media_segments WHERE media_file_id = $1 AND status = 'superseded'",
        )
        .bind(&media_file_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(superseded_count, 1);
        Ok(())
    }

    #[tokio::test]
    async fn manual_segment_candidate_is_rejected_by_core_service() -> Result<()> {
        let pool = test_pool().await?;
        let (media_file_id, movie_id) = seed_movie_file(&pool, 1800.0).await?;

        let error = submit_segment_candidate(
            &pool,
            SegmentCandidateInput {
                media_file_id: media_file_id.clone(),
                item_type: Some("movie".to_string()),
                item_id: Some(movie_id),
                segment_type: "intro".to_string(),
                start_seconds: 2.0,
                end_seconds: 95.0,
                provider_kind: "manual".to_string(),
                provider_id: "support_console".to_string(),
                provider_version: None,
                confidence: 1.0,
                identity_strength: "manual".to_string(),
                source_payload: Some(json!({"title": "Manual intro"})),
            },
        )
        .await
        .expect_err("manual marker candidates must not be accepted");

        assert!(
            error
                .to_string()
                .contains("manual media segment candidates are not supported"),
            "unexpected error: {error:#}"
        );
        assert!(
            list_active_segments_for_file(&pool, &media_file_id)
                .await?
                .is_empty()
        );
        Ok(())
    }

    #[tokio::test]
    async fn theintrodb_refresh_uses_strong_imdb_identity_and_activates_segments() -> Result<()> {
        let pool = test_pool().await?;
        let (media_file_id, movie_id) = seed_movie_file(&pool, 1800.0).await?;
        sqlx::query("UPDATE movies SET external_imdb = 'tt1234567' WHERE id = $1")
            .bind(&movie_id)
            .execute(&pool)
            .await?;
        let base_url = fake_provider_base_url(Router::new().route(
            "/segments",
            get(|| async {
                Json(json!({
                    "segments": [{
                        "id": "intro-1",
                        "type": "intro",
                        "start_sec": 0,
                        "end_sec": 45,
                        "confidence": 0.95
                    }]
                }))
            }),
        ))
        .await?;
        let preferences = preferences_with_provider_urls(Some(&base_url), None)?;

        let summary = refresh_builtin_provider_segments(
            &pool,
            &media_file_id,
            &preferences,
            BuiltinProviderRefreshOptions {
                force_refresh: Some(true),
                provider_kind: None,
            },
        )
        .await?;

        assert_eq!(summary.candidates_submitted, 1);
        assert_eq!(summary.candidates_accepted, 1);
        assert!(
            summary.providers.iter().any(|provider| {
                provider.provider_kind == PROVIDER_THEINTRODB
                    && provider.status == "ok"
                    && provider.accepted_count == 1
            }),
            "expected successful TheIntroDB provider summary: {summary:?}"
        );
        let active = list_active_segments_for_file(&pool, &media_file_id).await?;
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].segment_type, "intro");

        let cache_count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM media_segment_provider_cache WHERE provider_kind = 'theintrodb' AND status = 'ok'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(cache_count, 1);
        Ok(())
    }

    #[tokio::test]
    async fn theintrodb_refresh_accepts_live_named_segment_shape() -> Result<()> {
        let pool = test_pool().await?;
        let (media_file_id, series_id, _) = seed_anime_episode_file(&pool, 3600.0).await?;
        sqlx::query("UPDATE series SET external_imdb = 'tt0903747' WHERE id = $1")
            .bind(&series_id)
            .execute(&pool)
            .await?;
        let base_url = fake_provider_base_url(Router::new().route(
            "/segments",
            get(|| async {
                Json(json!({
                    "imdb_id": "tt0903747",
                    "season": 1,
                    "episode": 1,
                    "intro": {
                        "start_sec": 2,
                        "end_sec": 58,
                        "start_ms": 2000,
                        "end_ms": 58000,
                        "confidence": 1,
                        "submission_count": 3
                    },
                    "recap": null,
                    "outro": {
                        "start_sec": 3431,
                        "end_sec": 3500,
                        "start_ms": 3431000,
                        "end_ms": 3500000,
                        "confidence": 1,
                        "submission_count": 1
                    }
                }))
            }),
        ))
        .await?;
        let preferences = preferences_with_provider_urls(Some(&base_url), None)?;

        let summary = refresh_builtin_provider_segments(
            &pool,
            &media_file_id,
            &preferences,
            BuiltinProviderRefreshOptions {
                force_refresh: Some(true),
                provider_kind: Some(PROVIDER_THEINTRODB.to_string()),
            },
        )
        .await?;

        assert_eq!(summary.candidates_submitted, 2);
        assert_eq!(summary.candidates_accepted, 2);
        assert!(
            summary.providers.iter().any(|provider| {
                provider.provider_kind == PROVIDER_THEINTRODB
                    && provider.status == "ok"
                    && provider.accepted_count == 2
            }),
            "expected successful live-shape TheIntroDB summary: {summary:?}"
        );
        let active = list_active_segments_for_file(&pool, &media_file_id).await?;
        let segment_types = active
            .iter()
            .map(|segment| segment.segment_type.as_str())
            .collect::<Vec<_>>();
        assert_eq!(segment_types, vec!["intro", "outro"]);
        Ok(())
    }

    #[tokio::test]
    async fn aniskip_refresh_uses_mal_episode_identity_and_activates_op_ed() -> Result<()> {
        let pool = test_pool().await?;
        let (media_file_id, _, _) = seed_anime_episode_file(&pool, 1500.0).await?;
        let base_url = fake_provider_base_url(Router::new().route(
            "/v1/skip-times/:mal_id/:episode",
            get(|uri: axum::http::Uri| async move {
                let query = uri.query().unwrap_or_default();
                assert!(query.contains("op"), "AniSkip query missing op: {query}");
                assert!(query.contains("ed"), "AniSkip query missing ed: {query}");
                if query.contains("mixed") || query.contains("recap") {
                    (
                        StatusCode::BAD_REQUEST,
                        Json(json!({"error": "unsupported skip type requested"})),
                    )
                } else {
                    (
                        StatusCode::OK,
                        Json(json!({
                            "found": true,
                            "results": [
                                {
                                    "skipId": "op-1",
                                    "skipType": "op",
                                    "interval": {
                                        "startTime": 30.0,
                                        "endTime": 90.0
                                    }
                                },
                                {
                                    "skipId": "ed-1",
                                    "skipType": "ed",
                                    "interval": {
                                        "startTime": 1320.0,
                                        "endTime": 1410.0
                                    }
                                }
                            ]
                        })),
                    )
                }
            }),
        ))
        .await?;
        let preferences = preferences_with_provider_urls(None, Some(&base_url))?;

        let summary = refresh_builtin_provider_segments(
            &pool,
            &media_file_id,
            &preferences,
            BuiltinProviderRefreshOptions {
                force_refresh: Some(true),
                provider_kind: None,
            },
        )
        .await?;

        assert_eq!(summary.candidates_submitted, 2);
        assert_eq!(summary.candidates_accepted, 2);
        assert!(
            summary.providers.iter().any(|provider| {
                provider.provider_kind == PROVIDER_ANISKIP
                    && provider.status == "ok"
                    && provider.accepted_count == 2
            }),
            "expected successful AniSkip provider summary: {summary:?}"
        );
        let active = list_active_segments_for_file(&pool, &media_file_id).await?;
        let segment_types = active
            .iter()
            .map(|segment| segment.segment_type.as_str())
            .collect::<Vec<_>>();
        assert_eq!(segment_types, vec!["intro", "outro"]);
        Ok(())
    }

    #[tokio::test]
    async fn aniskip_refresh_falls_back_to_legacy_path_on_v1_not_found() -> Result<()> {
        let pool = test_pool().await?;
        let (media_file_id, _, _) = seed_anime_episode_file(&pool, 1500.0).await?;
        let base_url = fake_provider_base_url(
            Router::new()
                .route(
                    "/v1/skip-times/:mal_id/:episode",
                    get(|| async { (StatusCode::NOT_FOUND, Json(json!({"found": false}))) }),
                )
                .route(
                    "/skip-times/:mal_id/:episode",
                    get(|| async {
                        Json(json!({
                            "found": true,
                            "results": [{
                                "skipId": "op-legacy",
                                "skipType": "op",
                                "interval": {
                                    "startTime": 30.0,
                                    "endTime": 90.0
                                }
                            }]
                        }))
                    }),
                ),
        )
        .await?;
        let preferences = preferences_with_provider_urls(None, Some(&base_url))?;

        let summary = refresh_builtin_provider_segments(
            &pool,
            &media_file_id,
            &preferences,
            BuiltinProviderRefreshOptions {
                force_refresh: Some(true),
                provider_kind: Some(PROVIDER_ANISKIP.to_string()),
            },
        )
        .await?;

        assert_eq!(summary.candidates_submitted, 1);
        assert_eq!(summary.candidates_accepted, 1);
        assert!(
            summary.providers.iter().any(|provider| {
                provider.provider_kind == PROVIDER_ANISKIP
                    && provider.status == "ok"
                    && provider.accepted_count == 1
            }),
            "expected successful AniSkip fallback summary: {summary:?}"
        );
        let active = list_active_segments_for_file(&pool, &media_file_id).await?;
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].segment_type, "intro");
        Ok(())
    }

    #[tokio::test]
    async fn theintrodb_refresh_rejects_duration_mismatch_without_activation() -> Result<()> {
        let pool = test_pool().await?;
        let (media_file_id, movie_id) = seed_movie_file(&pool, 1800.0).await?;
        sqlx::query("UPDATE movies SET external_imdb = 'tt2223334' WHERE id = $1")
            .bind(&movie_id)
            .execute(&pool)
            .await?;
        let base_url = fake_provider_base_url(Router::new().route(
            "/segments",
            get(|| async {
                Json(json!({
                    "segments": [{
                        "id": "bad-duration-intro",
                        "type": "intro",
                        "start_sec": 20,
                        "end_sec": 1850,
                        "confidence": 0.99
                    }]
                }))
            }),
        ))
        .await?;
        let preferences = preferences_with_provider_urls(Some(&base_url), None)?;

        let summary = refresh_builtin_provider_segments(
            &pool,
            &media_file_id,
            &preferences,
            BuiltinProviderRefreshOptions {
                force_refresh: Some(true),
                provider_kind: Some(PROVIDER_THEINTRODB.to_string()),
            },
        )
        .await?;

        assert_eq!(summary.candidates_submitted, 1);
        assert_eq!(summary.candidates_accepted, 0);
        assert_eq!(summary.candidates_rejected, 1);
        assert_eq!(summary.active_segments, 0);
        assert!(
            summary.providers.iter().any(|provider| {
                provider.provider_kind == PROVIDER_THEINTRODB
                    && provider.status == "ok"
                    && provider.candidate_count == 1
                    && provider.accepted_count == 0
                    && provider.rejected_count == 1
            }),
            "expected rejected TheIntroDB provider summary: {summary:?}"
        );

        let active = list_active_segments_for_file(&pool, &media_file_id).await?;
        assert!(active.is_empty());
        let candidates = list_segment_candidates_for_file(&pool, &media_file_id).await?;
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].provider_kind, PROVIDER_THEINTRODB);
        assert_eq!(candidates[0].validation_state, "rejected");
        assert_eq!(
            candidates[0].validation_reason.as_deref(),
            Some("outside_media_duration")
        );
        Ok(())
    }

    #[tokio::test]
    async fn theintrodb_refresh_outage_caches_error_without_activation() -> Result<()> {
        let pool = test_pool().await?;
        let (media_file_id, movie_id) = seed_movie_file(&pool, 1800.0).await?;
        sqlx::query("UPDATE movies SET external_imdb = 'tt3334445' WHERE id = $1")
            .bind(&movie_id)
            .execute(&pool)
            .await?;
        let base_url = fake_provider_base_url(Router::new().route(
            "/segments",
            get(|| async {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "provider unavailable"})),
                )
            }),
        ))
        .await?;
        let preferences = preferences_with_provider_urls(Some(&base_url), None)?;

        let summary = refresh_builtin_provider_segments(
            &pool,
            &media_file_id,
            &preferences,
            BuiltinProviderRefreshOptions {
                force_refresh: Some(true),
                provider_kind: Some(PROVIDER_THEINTRODB.to_string()),
            },
        )
        .await?;

        assert_eq!(summary.candidates_submitted, 0);
        assert_eq!(summary.candidates_accepted, 0);
        assert_eq!(summary.candidates_rejected, 0);
        assert_eq!(summary.active_segments, 0);
        assert!(
            summary.providers.iter().any(|provider| {
                provider.provider_kind == PROVIDER_THEINTRODB
                    && provider.status == "error"
                    && provider.reason.as_deref() == Some("provider_http_500")
            }),
            "expected provider outage summary: {summary:?}"
        );
        assert!(
            list_active_segments_for_file(&pool, &media_file_id)
                .await?
                .is_empty()
        );
        let cache_count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM media_segment_provider_cache
             WHERE provider_kind = 'theintrodb' AND status = 'error'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(cache_count, 1);
        Ok(())
    }

    #[tokio::test]
    async fn disabled_theintrodb_provider_does_not_call_network_or_activate() -> Result<()> {
        let pool = test_pool().await?;
        let (media_file_id, movie_id) = seed_movie_file(&pool, 1800.0).await?;
        sqlx::query("UPDATE movies SET external_imdb = 'tt4445556' WHERE id = $1")
            .bind(&movie_id)
            .execute(&pool)
            .await?;

        let request_count = Arc::new(Mutex::new(0usize));
        let request_count_for_handler = request_count.clone();
        let base_url = fake_provider_base_url(Router::new().route(
            "/segments",
            get(move || {
                let request_count = request_count_for_handler.clone();
                async move {
                    *request_count.lock().expect("provider request count") += 1;
                    Json(json!({
                        "segments": [{
                            "id": "disabled-provider-intro",
                            "type": "intro",
                            "start_sec": 0,
                            "end_sec": 60,
                            "confidence": 1.0
                        }]
                    }))
                }
            }),
        ))
        .await?;
        let mut preferences = preferences_with_provider_urls(Some(&base_url), None)?;
        preferences.segment_provider_settings = merge_segment_provider_settings(
            &preferences.segment_provider_settings,
            json!({
                "theintrodb": {
                    "enabled": false,
                    "base_url": base_url,
                    "cache_ttl_seconds": 3600
                }
            }),
        )?;

        let summary = refresh_builtin_provider_segments(
            &pool,
            &media_file_id,
            &preferences,
            BuiltinProviderRefreshOptions {
                force_refresh: Some(true),
                provider_kind: Some(PROVIDER_THEINTRODB.to_string()),
            },
        )
        .await?;

        assert_eq!(summary.candidates_submitted, 0);
        assert_eq!(summary.candidates_accepted, 0);
        assert_eq!(summary.active_segments, 0);
        assert_eq!(
            *request_count.lock().expect("provider request count"),
            0,
            "disabled provider must not call network"
        );
        assert!(
            summary.providers.iter().any(|provider| {
                provider.provider_kind == PROVIDER_THEINTRODB
                    && !provider.enabled
                    && provider.status == "skipped"
                    && provider.reason.as_deref() == Some("provider_disabled")
            }),
            "expected disabled provider summary: {summary:?}"
        );
        assert!(
            list_active_segments_for_file(&pool, &media_file_id)
                .await?
                .is_empty()
        );
        Ok(())
    }

    #[tokio::test]
    async fn provider_refresh_job_claims_runs_and_finishes() -> Result<()> {
        let pool = test_pool().await?;
        let (media_file_id, movie_id) = seed_movie_file(&pool, 1800.0).await?;
        sqlx::query("UPDATE movies SET external_imdb = 'tt7654321' WHERE id = $1")
            .bind(&movie_id)
            .execute(&pool)
            .await?;
        let base_url = fake_provider_base_url(Router::new().route(
            "/segments",
            get(|| async {
                Json(json!({
                    "segments": [{
                        "id": "intro-job-1",
                        "type": "intro",
                        "start_sec": 12,
                        "end_sec": 72,
                        "confidence": 0.96
                    }]
                }))
            }),
        ))
        .await?;
        let preferences = preferences_with_provider_urls(Some(&base_url), None)?;

        let queued =
            enqueue_builtin_provider_refresh_job(&pool, &media_file_id, PROVIDER_THEINTRODB, 10)
                .await?;
        assert_eq!(queued.status, "queued");
        assert_eq!(queued.attempts, 0);

        let run = run_next_media_segment_job(&pool, &preferences, "midm-test-worker")
            .await?
            .expect("queued provider job should run");
        assert_eq!(run.job.status, "succeeded");
        assert_eq!(run.job.attempts, 1);
        assert!(run.job.locked_by.is_none());
        let summary = run.summary.expect("provider job should return summary");
        assert_eq!(summary.candidates_submitted, 1);
        assert_eq!(summary.candidates_accepted, 1);

        let active = list_active_segments_for_file(&pool, &media_file_id).await?;
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].segment_type, "intro");
        assert!(
            claim_next_media_segment_job(&pool, "midm-test-worker")
                .await?
                .is_none()
        );
        Ok(())
    }

    #[tokio::test]
    async fn marketplace_segment_provider_job_invokes_extension_and_activates_segments()
    -> Result<()> {
        let pool = test_pool().await?;
        let (media_file_id, movie_id) = seed_movie_file(&pool, 1800.0).await?;
        sqlx::query(
            "UPDATE movies SET external_imdb = 'tt2468135', external_tmdb = '12345' WHERE id = $1",
        )
        .bind(&movie_id)
        .execute(&pool)
        .await?;
        let user_id = Uuid::new_v4().to_string();
        sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
            .bind(&user_id)
            .bind(format!("midm-{user_id}@example.test"))
            .bind("hash")
            .execute(&pool)
            .await?;
        sqlx::query(
            "INSERT INTO user_media_state
                (user_id, item_type, item_id, media_file_id, resume_seconds,
                 duration_seconds, watched, play_count, last_played_at, state_source)
             VALUES ($1, 'movie', $2, $3, 744.0, 1800, 0, 3, CURRENT_TIMESTAMP, 'test')",
        )
        .bind(&user_id)
        .bind(&movie_id)
        .bind(&media_file_id)
        .execute(&pool)
        .await?;

        let observed_requests = Arc::new(Mutex::new(Vec::<Value>::new()));
        let observed_requests_for_handler = observed_requests.clone();
        let base_url = fake_provider_base_url(Router::new().route(
            "/segment-provider/lookup",
            post(move |Json(body): Json<Value>| {
                let observed_requests = observed_requests_for_handler.clone();
                async move {
                    observed_requests
                        .lock()
                        .expect("observed request")
                        .push(body);
                    Json(json!({
                        "segments": [{
                            "id": "fixture-intro-1",
                            "type": "intro",
                            "start_seconds": 15.0,
                            "end_seconds": 75.0,
                            "confidence": 0.93,
                            "identity_strength": "external_id_exact",
                            "provider_id": "fixture_segments",
                            "provider_version": "1.2.3"
                        }]
                    }))
                }
            }),
        ))
        .await?;
        let provider_id = seed_marketplace_segment_provider(
            &pool,
            &base_url,
            "community-markers",
            &["movie", "series", "anime"],
            &["intro", "credits"],
        )
        .await?;
        let mut preferences = default_playback_preferences();
        preferences.segment_provider_settings = merge_segment_provider_settings(
            &preferences.segment_provider_settings,
            json!({
                "community_markers": {
                    "enabled": true,
                    "kind": "extension",
                    "label": "Community Markers",
                    "cache_ttl_seconds": 3600
                }
            }),
        )?;

        let queued = enqueue_media_segment_job_request_with_marketplace(
            &pool,
            MediaSegmentJobEnqueueRequest {
                job_type: "provider_refresh".to_string(),
                scope_type: "media_file".to_string(),
                scope_id: media_file_id.clone(),
                provider_kind: "community-markers".to_string(),
                priority: Some(10),
            },
        )
        .await?;
        assert_eq!(queued.provider_kind, "community_markers");

        let run = run_next_media_segment_job(&pool, &preferences, "midm-marketplace-test")
            .await?
            .expect("queued marketplace provider job should run");
        assert_eq!(run.job.status, "succeeded");
        let summary = run.summary.expect("provider job should return summary");
        assert_eq!(summary.candidates_submitted, 1);
        assert_eq!(summary.candidates_accepted, 1);
        assert!(
            summary.providers.iter().any(|provider| {
                provider.provider_kind == "community_markers"
                    && provider.status == "ok"
                    && provider.accepted_count == 1
            }),
            "expected successful marketplace provider summary: {summary:?}"
        );

        let active = list_active_segments_for_file(&pool, &media_file_id).await?;
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].segment_type, "intro");
        let candidates = list_segment_candidates_for_file(&pool, &media_file_id).await?;
        assert!(
            candidates.iter().any(|candidate| {
                candidate.provider_kind == "community_markers"
                    && candidate.validation_state == "accepted"
            }),
            "expected accepted marketplace candidate: {candidates:?}"
        );

        let observed = observed_requests.lock().expect("observed requests");
        assert_eq!(observed.len(), 1);
        let request = &observed[0];
        assert_eq!(
            request.get("schema_version").and_then(Value::as_str),
            Some(MEDIA_SEGMENT_PROVIDER_SCHEMA_VERSION)
        );
        assert_eq!(
            request
                .pointer("/request/media_file_id")
                .and_then(Value::as_str),
            Some(media_file_id.as_str())
        );
        assert_eq!(
            request
                .pointer("/request/external_ids/imdb")
                .and_then(Value::as_str),
            Some("tt2468135")
        );
        assert_eq!(
            request
                .pointer("/request/requested_segment_types/0")
                .and_then(Value::as_str),
            Some("intro")
        );
        let provider_id_text = provider_id.to_string();
        assert_eq!(
            request
                .pointer("/provider/provider_id")
                .and_then(Value::as_str),
            Some(provider_id_text.as_str())
        );
        assert!(request.pointer("/request/watch_state").is_none());
        assert!(request.pointer("/request/playback").is_none());
        drop(observed);

        let cache_count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM media_segment_provider_cache
             WHERE provider_kind = 'community_markers' AND status = 'ok'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(cache_count, 1);
        Ok(())
    }

    #[tokio::test]
    async fn marketplace_segment_provider_oversized_response_fails_without_candidates() -> Result<()>
    {
        let pool = test_pool().await?;
        let (media_file_id, movie_id) = seed_movie_file(&pool, 1800.0).await?;
        sqlx::query(
            "UPDATE movies SET external_imdb = 'tt9753124', external_tmdb = '97531' WHERE id = $1",
        )
        .bind(&movie_id)
        .execute(&pool)
        .await?;

        let oversized_body = "x".repeat(usize::try_from(
            MEDIA_SEGMENT_PROVIDER_RESPONSE_MAX_BYTES + 1,
        )?);
        let base_url = fake_provider_base_url(Router::new().route(
            "/segment-provider/lookup",
            post(move || {
                let oversized_body = oversized_body.clone();
                async move { oversized_body }
            }),
        ))
        .await?;
        seed_marketplace_segment_provider(
            &pool,
            &base_url,
            "oversized-markers",
            &["movie"],
            &["intro"],
        )
        .await?;

        let mut preferences = default_playback_preferences();
        preferences.segment_provider_settings = merge_segment_provider_settings(
            &preferences.segment_provider_settings,
            json!({
                "oversized_markers": {
                    "enabled": true,
                    "kind": "extension",
                    "label": "Oversized Markers"
                }
            }),
        )?;
        let queued = enqueue_media_segment_job_request_with_marketplace(
            &pool,
            MediaSegmentJobEnqueueRequest {
                job_type: "provider_refresh".to_string(),
                scope_type: "media_file".to_string(),
                scope_id: media_file_id.clone(),
                provider_kind: "oversized-markers".to_string(),
                priority: Some(10),
            },
        )
        .await?;
        sqlx::query("UPDATE media_segment_jobs SET max_attempts = 1 WHERE id = $1")
            .bind(&queued.id)
            .execute(&pool)
            .await?;

        let run = run_next_media_segment_job(&pool, &preferences, "midm-marketplace-oversized")
            .await?
            .expect("queued oversized marketplace provider job should run");

        assert_eq!(run.job.status, "failed");
        assert!(run.summary.is_none());
        assert_eq!(
            run.job
                .error
                .as_ref()
                .and_then(|error| error.get("reason"))
                .and_then(Value::as_str),
            Some("provider_refresh_failed")
        );
        assert!(
            run.job
                .error
                .as_ref()
                .and_then(|error| error.get("error"))
                .and_then(Value::as_str)
                .is_some_and(|error| { error.contains("media segment provider response exceeds") }),
            "expected oversized response error: {:?}",
            run.job.error
        );
        assert!(
            list_segment_candidates_for_file(&pool, &media_file_id)
                .await?
                .is_empty()
        );
        assert!(
            list_active_segments_for_file(&pool, &media_file_id)
                .await?
                .is_empty()
        );
        Ok(())
    }

    #[tokio::test]
    async fn disabled_marketplace_segment_provider_skips_network_and_mutation() -> Result<()> {
        let pool = test_pool().await?;
        let (media_file_id, movie_id) = seed_movie_file(&pool, 1800.0).await?;
        sqlx::query(
            "UPDATE movies SET external_imdb = 'tt8642135', external_tmdb = '86421' WHERE id = $1",
        )
        .bind(&movie_id)
        .execute(&pool)
        .await?;

        let request_count = Arc::new(Mutex::new(0usize));
        let request_count_for_handler = request_count.clone();
        let base_url = fake_provider_base_url(Router::new().route(
            "/segment-provider/lookup",
            post(move |Json(_body): Json<Value>| {
                let request_count = request_count_for_handler.clone();
                async move {
                    *request_count.lock().expect("provider request count") += 1;
                    Json(json!({
                        "segments": [{
                            "id": "disabled-marketplace-intro",
                            "type": "intro",
                            "start_seconds": 0.0,
                            "end_seconds": 60.0,
                            "confidence": 1.0,
                            "identity_strength": "external_id_exact"
                        }]
                    }))
                }
            }),
        ))
        .await?;
        seed_marketplace_segment_provider(
            &pool,
            &base_url,
            "disabled-markers",
            &["movie"],
            &["intro"],
        )
        .await?;

        let mut preferences = default_playback_preferences();
        preferences.segment_provider_settings = merge_segment_provider_settings(
            &preferences.segment_provider_settings,
            json!({
                "disabled_markers": {
                    "enabled": false,
                    "kind": "extension",
                    "label": "Disabled Markers"
                }
            }),
        )?;
        enqueue_media_segment_job_request_with_marketplace(
            &pool,
            MediaSegmentJobEnqueueRequest {
                job_type: "provider_refresh".to_string(),
                scope_type: "media_file".to_string(),
                scope_id: media_file_id.clone(),
                provider_kind: "disabled-markers".to_string(),
                priority: Some(10),
            },
        )
        .await?;

        let run = run_next_media_segment_job(&pool, &preferences, "midm-marketplace-disabled")
            .await?
            .expect("queued disabled marketplace provider job should run");

        assert_eq!(run.job.status, "skipped");
        let summary = run
            .summary
            .expect("disabled provider should produce summary");
        assert_eq!(summary.candidates_submitted, 0);
        assert_eq!(summary.active_segments, 0);
        assert!(
            summary.providers.iter().any(|provider| {
                provider.provider_kind == "disabled_markers"
                    && !provider.enabled
                    && provider.status == "skipped"
                    && provider.reason.as_deref() == Some("provider_disabled")
            }),
            "expected disabled marketplace provider summary: {summary:?}"
        );
        assert_eq!(
            *request_count.lock().expect("provider request count"),
            0,
            "disabled marketplace provider must not call network"
        );
        assert!(
            list_segment_candidates_for_file(&pool, &media_file_id)
                .await?
                .is_empty()
        );
        assert!(
            list_active_segments_for_file(&pool, &media_file_id)
                .await?
                .is_empty()
        );
        Ok(())
    }

    #[tokio::test]
    async fn marketplace_segment_provider_due_enqueue_queues_enabled_provider_jobs() -> Result<()> {
        let pool = test_pool().await?;
        let (media_file_id, _) = seed_movie_file(&pool, 1800.0).await?;
        let base_url = fake_provider_base_url(Router::new().route(
            "/segment-provider/lookup",
            post(|| async { Json(json!({"segments": []})) }),
        ))
        .await?;
        seed_marketplace_segment_provider(
            &pool,
            &base_url,
            "community-markers",
            &["movie"],
            &["intro"],
        )
        .await?;
        let mut preferences = default_playback_preferences();
        preferences.segment_provider_settings = merge_segment_provider_settings(
            &preferences.segment_provider_settings,
            json!({
                "community_markers": {
                    "enabled": true,
                    "kind": "extension",
                    "label": "Community Markers"
                }
            }),
        )?;

        let summary =
            enqueue_due_marketplace_segment_provider_refresh_jobs(&pool, &preferences, 10).await?;

        assert_eq!(summary.providers_seen, 1);
        assert_eq!(summary.files_seen, 1);
        assert_eq!(summary.jobs_queued, 1);
        let queued_count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM media_segment_jobs
             WHERE scope_id = $1
               AND job_type = 'provider_refresh'
               AND provider_kind = 'community_markers'
               AND status = 'queued'",
        )
        .bind(&media_file_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(queued_count, 1);
        Ok(())
    }

    #[tokio::test]
    async fn marketplace_segment_provider_certification_persists_probe_evidence() -> Result<()> {
        let pool = test_pool().await?;
        let observed_requests = Arc::new(Mutex::new(Vec::<Value>::new()));
        let observed_requests_for_handler = observed_requests.clone();
        let base_url = fake_provider_base_url(Router::new().route(
            "/segment-provider/lookup",
            post(move |Json(body): Json<Value>| {
                let observed_requests = observed_requests_for_handler.clone();
                async move {
                    observed_requests
                        .lock()
                        .expect("observed certification request")
                        .push(body);
                    Json(json!({
                        "segments": [{
                            "id": "cert-intro",
                            "type": "intro",
                            "start_seconds": 10.0,
                            "end_seconds": 70.0,
                            "confidence": 0.90,
                            "identity_strength": "external_id_exact"
                        }]
                    }))
                }
            }),
        ))
        .await?;
        let provider_id = seed_marketplace_segment_provider(
            &pool,
            &base_url,
            "community-markers",
            &["movie", "series", "anime"],
            &["intro"],
        )
        .await?;

        let provider_id_text = provider_id.to_string();
        let certification = certify_media_segment_provider(&pool, &provider_id_text).await?;

        assert_eq!(certification.provider_id, provider_id_text);
        assert_eq!(certification.provider_kind, "community_markers");
        assert_eq!(certification.status, "certified");
        assert_eq!(
            certification
                .media_type_results
                .pointer("/movie/status")
                .and_then(Value::as_str),
            Some("certified")
        );
        assert_eq!(
            certification
                .media_type_results
                .pointer("/series/status")
                .and_then(Value::as_str),
            Some("certified")
        );
        assert_eq!(
            certification
                .media_type_results
                .pointer("/anime/status")
                .and_then(Value::as_str),
            Some("certified")
        );

        let listed = list_media_segment_provider_certifications(
            &pool,
            MediaSegmentProviderCertificationFilters {
                provider_id: Some(provider_id.to_string()),
                ..MediaSegmentProviderCertificationFilters::default()
            },
        )
        .await?;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].status, "certified");

        let observed = observed_requests.lock().expect("observed requests");
        assert_eq!(observed.len(), 3);
        for request in observed.iter() {
            assert_eq!(
                request.get("schema_version").and_then(Value::as_str),
                Some(MEDIA_SEGMENT_PROVIDER_SCHEMA_VERSION)
            );
            assert!(request.pointer("/request/watch_state").is_none());
            assert!(request.pointer("/request/playback").is_none());
            assert!(request.pointer("/request/external_ids").is_some());
        }
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_media_segment_job_is_not_overwritten_by_late_worker_finish() -> Result<()> {
        let pool = test_pool().await?;
        let (media_file_id, _) = seed_movie_file(&pool, 1800.0).await?;
        let queued =
            enqueue_builtin_provider_refresh_job(&pool, &media_file_id, PROVIDER_THEINTRODB, 10)
                .await?;
        assert_eq!(queued.status, "queued");

        let claimed = claim_next_media_segment_job(&pool, "midm-race-worker")
            .await?
            .expect("queued job should be claimed");
        assert_eq!(claimed.status, "running");
        assert_eq!(claimed.attempts, 1);

        let cancelled =
            cancel_media_segment_job(&pool, &claimed.id, Some("operator stopped detector")).await?;
        assert_eq!(cancelled.status, "cancelled");
        assert!(cancelled.locked_by.is_none());

        let late_finish = finish_media_segment_job(&pool, &claimed.id, "succeeded", None)
            .await?
            .expect("late-finished job should still be loadable");
        assert_eq!(late_finish.status, "cancelled");
        assert!(late_finish.locked_by.is_none());
        assert_eq!(
            late_finish
                .error
                .as_ref()
                .and_then(|error| error.get("reason"))
                .and_then(Value::as_str),
            Some("admin_cancelled")
        );
        assert_eq!(
            late_finish
                .error
                .as_ref()
                .and_then(|error| error.get("previous_status"))
                .and_then(Value::as_str),
            Some("running")
        );
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_media_segment_job_is_not_requeued_by_late_worker_retry() -> Result<()> {
        let pool = test_pool().await?;
        let (media_file_id, _) = seed_movie_file(&pool, 1800.0).await?;
        enqueue_local_visual_frame_hash_job(&pool, &media_file_id, 10).await?;

        let claimed = claim_next_media_segment_job(&pool, "midm-race-worker")
            .await?
            .expect("queued job should be claimed");
        assert_eq!(claimed.status, "running");
        assert_eq!(claimed.provider_kind, PROVIDER_LOCAL_VISUAL_RECURRING);

        cancel_media_segment_job(&pool, &claimed.id, Some("operator stopped detector")).await?;
        let late_retry = retry_or_fail_media_segment_job(
            &pool,
            &claimed.id,
            json!({
                "reason": "late_worker_error",
                "error": "worker observed failure after cancellation"
            }),
        )
        .await?
        .expect("cancelled job should still be loadable");

        assert_eq!(late_retry.status, "cancelled");
        assert_eq!(late_retry.attempts, 1);
        assert!(late_retry.next_attempt_at.is_none());
        assert!(late_retry.locked_by.is_none());
        assert_eq!(
            late_retry
                .error
                .as_ref()
                .and_then(|error| error.get("reason"))
                .and_then(Value::as_str),
            Some("admin_cancelled")
        );
        Ok(())
    }

    #[tokio::test]
    async fn media_segment_worker_loop_stops_when_shutdown_is_cancelled() -> Result<()> {
        let pool = test_pool().await?;
        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(start_media_segment_job_worker_loop_with_controls(
            pool,
            default_playback_preferences(),
            "midm-shutdown-test".to_string(),
            shutdown.clone(),
            60,
            0,
            0,
            MEDIA_SEGMENT_WORKER_MAX_RUNTIME_SECONDS,
        ));

        shutdown.cancel();
        tokio::time::timeout(StdDuration::from_secs(2), handle)
            .await
            .context("media segment worker loop did not stop after shutdown")??;
        Ok(())
    }

    #[tokio::test]
    async fn local_ffmpeg_wait_kills_child_on_worker_shutdown() -> Result<()> {
        let pool = test_pool().await?;
        let job_id = Uuid::new_v4().to_string();
        let shutdown = CancellationToken::new();
        let mut command = slow_test_command();
        let child = command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("spawning slow test process")?;

        shutdown.cancel();
        let started = StdInstant::now();
        let err = wait_for_local_ffmpeg_output(
            child,
            30,
            Some(MediaSegmentJobCancellation {
                pool: &pool,
                job_id: &job_id,
                shutdown: Some(&shutdown),
            }),
            "test ffmpeg",
        )
        .await
        .expect_err("shutdown should interrupt the slow process");

        assert!(
            started.elapsed() < StdDuration::from_secs(2),
            "shutdown should kill the child promptly"
        );
        assert!(
            is_media_segment_worker_shutdown_interruption(&err),
            "unexpected shutdown error: {err}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn local_ffmpeg_wait_kills_child_on_timeout() -> Result<()> {
        let mut command = slow_test_command();
        let child = command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("spawning slow timeout test process")?;

        let started = StdInstant::now();
        let err = wait_for_local_ffmpeg_output(child, 1, None, "test ffmpeg timeout")
            .await
            .expect_err("timeout should interrupt the slow process");

        assert!(
            started.elapsed() < StdDuration::from_secs(3),
            "timeout should kill the child promptly"
        );
        assert!(
            err.to_string().contains("test ffmpeg timeout timed out"),
            "unexpected timeout error: {err}"
        );
        Ok(())
    }

    fn slow_test_command() -> Command {
        #[cfg(windows)]
        {
            let mut command = Command::new("cmd");
            command.args(["/C", "ping -n 30 127.0.0.1 >NUL"]);
            command
        }
        #[cfg(not(windows))]
        {
            let mut command = Command::new("sh");
            command.args(["-c", "sleep 30"]);
            command
        }
    }

    #[tokio::test]
    async fn stale_running_media_segment_job_is_requeued_when_attempts_remain() -> Result<()> {
        let pool = test_pool().await?;
        let (media_file_id, _) = seed_movie_file(&pool, 1800.0).await?;
        enqueue_local_visual_frame_hash_job(&pool, &media_file_id, 10).await?;

        let claimed = claim_next_media_segment_job(&pool, "midm-stale-worker")
            .await?
            .expect("queued job should be claimed");
        assert_eq!(claimed.status, "running");
        assert_eq!(claimed.attempts, 1);

        sqlx::query(
            "UPDATE media_segment_jobs
             SET started_at = $1,
                 updated_at = $2
             WHERE id = $3",
        )
        .bind(timestamp_after_seconds(
            -MEDIA_SEGMENT_STALE_RUNNING_JOB_SECONDS - 60,
        ))
        .bind(timestamp_after_seconds(
            -MEDIA_SEGMENT_STALE_RUNNING_JOB_SECONDS - 60,
        ))
        .bind(&claimed.id)
        .execute(&pool)
        .await?;

        let summary = recover_stale_running_media_segment_jobs(
            &pool,
            MEDIA_SEGMENT_STALE_RUNNING_JOB_SECONDS,
        )
        .await?;
        assert_eq!(summary.recovered, 1);
        assert_eq!(summary.requeued, 1);
        assert_eq!(summary.failed, 0);

        let recovered = load_media_segment_job(&pool, &claimed.id)
            .await?
            .expect("recovered job should be loadable");
        assert_eq!(recovered.status, "queued");
        assert_eq!(recovered.attempts, 1);
        assert!(recovered.locked_by.is_none());
        assert!(recovered.next_attempt_at.is_some());
        assert_eq!(
            recovered
                .error
                .as_ref()
                .and_then(|error| error.get("reason"))
                .and_then(Value::as_str),
            Some("stale_running_job")
        );
        Ok(())
    }

    #[tokio::test]
    async fn stale_running_media_segment_job_fails_when_attempts_are_exhausted() -> Result<()> {
        let pool = test_pool().await?;
        let (media_file_id, _) = seed_movie_file(&pool, 1800.0).await?;
        enqueue_local_visual_frame_hash_job(&pool, &media_file_id, 10).await?;

        let claimed = claim_next_media_segment_job(&pool, "midm-stale-worker")
            .await?
            .expect("queued job should be claimed");
        sqlx::query(
            "UPDATE media_segment_jobs
             SET attempts = max_attempts,
                 started_at = $1,
                 updated_at = $2
             WHERE id = $3",
        )
        .bind(timestamp_after_seconds(
            -MEDIA_SEGMENT_STALE_RUNNING_JOB_SECONDS - 60,
        ))
        .bind(timestamp_after_seconds(
            -MEDIA_SEGMENT_STALE_RUNNING_JOB_SECONDS - 60,
        ))
        .bind(&claimed.id)
        .execute(&pool)
        .await?;

        let summary = recover_stale_running_media_segment_jobs(
            &pool,
            MEDIA_SEGMENT_STALE_RUNNING_JOB_SECONDS,
        )
        .await?;
        assert_eq!(summary.recovered, 1);
        assert_eq!(summary.requeued, 0);
        assert_eq!(summary.failed, 1);

        let recovered = load_media_segment_job(&pool, &claimed.id)
            .await?
            .expect("recovered job should be loadable");
        assert_eq!(recovered.status, "failed");
        assert!(recovered.locked_by.is_none());
        assert!(recovered.next_attempt_at.is_none());
        assert!(recovered.finished_at.is_some());
        assert_eq!(recovered.attempts, recovered.max_attempts);
        assert_eq!(
            recovered
                .error
                .as_ref()
                .and_then(|error| error.get("reason"))
                .and_then(Value::as_str),
            Some("stale_running_job")
        );
        Ok(())
    }

    #[tokio::test]
    async fn stale_local_audio_detector_job_requeues_and_resumes_successfully() -> Result<()> {
        let pool = test_pool().await?;
        let (season_id, media_file_ids) = seed_audio_detector_season(
            &pool,
            vec![
                json!({"windows": [
                    {"start_seconds": 30.0, "end_seconds": 90.0, "hash": "restart-opening"}
                ]}),
                json!({"windows": [
                    {"start_seconds": 31.0, "end_seconds": 91.0, "hash": "restart-opening"}
                ]}),
            ],
        )
        .await?;
        let preferences = preferences_with_local_audio_detector()?;
        enqueue_local_audio_recurring_detector_job(&pool, &season_id, 10).await?;

        let claimed = claim_next_media_segment_job(&pool, "midm-stale-audio-worker")
            .await?
            .expect("queued local audio detector job should be claimed");
        assert_eq!(claimed.status, "running");
        assert_eq!(claimed.job_type, MEDIA_SEGMENT_JOB_LOCAL_DETECTOR);
        assert_eq!(claimed.provider_kind, PROVIDER_LOCAL_AUDIO_RECURRING);
        assert_eq!(claimed.attempts, 1);

        sqlx::query(
            "UPDATE media_segment_jobs
             SET started_at = $1,
                 updated_at = $2
             WHERE id = $3",
        )
        .bind(timestamp_after_seconds(
            -MEDIA_SEGMENT_STALE_RUNNING_JOB_SECONDS - 60,
        ))
        .bind(timestamp_after_seconds(
            -MEDIA_SEGMENT_STALE_RUNNING_JOB_SECONDS - 60,
        ))
        .bind(&claimed.id)
        .execute(&pool)
        .await?;

        let recovery = recover_stale_running_media_segment_jobs(
            &pool,
            MEDIA_SEGMENT_STALE_RUNNING_JOB_SECONDS,
        )
        .await?;
        assert_eq!(recovery.recovered, 1);
        assert_eq!(recovery.requeued, 1);
        let recovered = load_media_segment_job(&pool, &claimed.id)
            .await?
            .expect("recovered audio detector job should be loadable");
        assert_eq!(recovered.status, "queued");
        assert_eq!(recovered.attempts, 1);
        assert!(recovered.locked_by.is_none());
        assert_eq!(
            recovered
                .error
                .as_ref()
                .and_then(|error| error.get("reason"))
                .and_then(Value::as_str),
            Some("stale_running_job")
        );

        sqlx::query(
            "UPDATE media_segment_jobs
             SET next_attempt_at = CURRENT_TIMESTAMP
             WHERE id = $1",
        )
        .bind(&claimed.id)
        .execute(&pool)
        .await?;

        let run = run_next_media_segment_job(&pool, &preferences, "midm-resumed-audio-worker")
            .await?
            .expect("recovered audio detector job should resume");
        assert_eq!(run.job.id, claimed.id);
        assert_eq!(run.job.status, "succeeded");
        assert_eq!(run.job.attempts, 2);
        assert!(run.job.locked_by.is_none());
        assert!(run.job.finished_at.is_some());
        let summary = run
            .local_audio_summary
            .expect("resumed audio detector job should return summary");
        assert_eq!(summary.candidates_submitted, media_file_ids.len());
        assert_eq!(summary.candidates_accepted, media_file_ids.len());

        for media_file_id in media_file_ids {
            let active = list_active_segments_for_file(&pool, &media_file_id).await?;
            assert_eq!(active.len(), 1);
            assert_eq!(active[0].segment_type, "intro");
        }
        Ok(())
    }

    #[test]
    fn local_audio_fingerprint_feature_hash_is_stable_and_discriminating() {
        let first = synthetic_pcm(17, 60);
        let same = synthetic_pcm(17, 60);
        let different = synthetic_pcm(29, 60);

        let first_hash = local_audio_feature_hash(&first).expect("first hash");
        let same_hash = local_audio_feature_hash(&same).expect("same hash");
        let different_hash = local_audio_feature_hash(&different).expect("different hash");

        assert_eq!(first_hash, same_hash);
        assert_ne!(first_hash, different_hash);
        assert!(first_hash.starts_with("laf1:"));
    }

    #[test]
    fn local_audio_fingerprint_plan_is_bounded_to_intro_and_outro_ranges() {
        let plan = local_audio_fingerprint_plan(1_500.0);
        assert_eq!(plan.len(), 2);
        assert!(plan.iter().all(|range| !range.windows.is_empty()));
        let total_windows = plan.iter().map(|range| range.windows.len()).sum::<usize>();
        assert!(total_windows <= LOCAL_AUDIO_FINGERPRINT_MAX_WINDOWS_PER_FILE);
        assert_eq!(plan[0].start_seconds, 0.0);
        assert!(plan[0].end_seconds <= LOCAL_AUDIO_FINGERPRINT_MAX_RANGE_SECONDS);
        assert!(plan[1].start_seconds >= 1_500.0 * LATE_WINDOW_FRACTION);
    }

    #[tokio::test]
    async fn local_audio_fingerprint_worker_enqueues_missing_episode_files() -> Result<()> {
        let pool = test_pool().await?;
        let (_season_id, media_file_ids) =
            seed_audio_detector_season_without_fingerprints(&pool, 3).await?;
        let preferences = preferences_with_local_audio_detector()?;

        let summary = run_media_segment_job_worker_iteration_with_preferences(
            &pool,
            &preferences,
            "midm-local-audio-fingerprint-enqueue-test",
            10,
            0,
            MEDIA_SEGMENT_WORKER_MAX_RUNTIME_SECONDS,
        )
        .await?;

        assert_eq!(summary.enqueue.providers_seen, 1);
        assert_eq!(summary.enqueue.jobs_queued, media_file_ids.len());
        assert_eq!(summary.jobs_run, 0);

        let queued_fingerprint_jobs = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM media_segment_jobs
             WHERE job_type = 'audio_fingerprint'
               AND provider_kind = 'local_audio_recurring'
               AND status = 'queued'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(queued_fingerprint_jobs, media_file_ids.len() as i64);

        let queued_detector_jobs = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM media_segment_jobs
             WHERE job_type = 'local_detector'
               AND provider_kind = 'local_audio_recurring'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(queued_detector_jobs, 0);
        Ok(())
    }

    #[tokio::test]
    async fn local_audio_fingerprint_job_skips_missing_file_cleanly() -> Result<()> {
        let pool = test_pool().await?;
        let (_season_id, media_file_ids) =
            seed_audio_detector_season_without_fingerprints(&pool, 1).await?;
        let preferences = preferences_with_local_audio_detector()?;

        let queued = enqueue_local_audio_fingerprint_job(&pool, &media_file_ids[0], 10).await?;
        assert_eq!(queued.status, "queued");
        let run = run_next_media_segment_job(
            &pool,
            &preferences,
            "midm-local-audio-fingerprint-missing-test",
        )
        .await?
        .expect("queued local audio fingerprint job should run");

        assert_eq!(run.job.status, "skipped");
        let summary = run
            .local_audio_fingerprint_summary
            .expect("fingerprint job should return summary");
        assert_eq!(summary.media_file_id, media_file_ids[0]);
        assert_eq!(summary.status, "skipped");
        assert!(
            summary
                .reason
                .as_deref()
                .unwrap_or_default()
                .starts_with("file_unavailable:")
        );
        Ok(())
    }

    #[tokio::test]
    async fn local_audio_detector_worker_emits_repeated_intro_candidates() -> Result<()> {
        let pool = test_pool().await?;
        let (_season_id, media_file_ids) = seed_audio_detector_season(
            &pool,
            vec![
                json!({"windows": [
                    {"start_seconds": 30.0, "end_seconds": 90.0, "hash": "opening-theme-a"},
                    {"start_seconds": 420.0, "end_seconds": 480.0, "hash": "episode-unique-1"}
                ]}),
                json!({"windows": [
                    {"start_seconds": 31.0, "end_seconds": 91.0, "hash": "opening-theme-a"},
                    {"start_seconds": 500.0, "end_seconds": 560.0, "hash": "episode-unique-2"}
                ]}),
                json!({"windows": [
                    {"start_seconds": 29.5, "end_seconds": 89.5, "hash": "opening-theme-a"},
                    {"start_seconds": 600.0, "end_seconds": 660.0, "hash": "episode-unique-3"}
                ]}),
            ],
        )
        .await?;
        let preferences = preferences_with_local_audio_detector()?;

        let summary = run_media_segment_job_worker_iteration_with_preferences(
            &pool,
            &preferences,
            "midm-local-audio-test",
            10,
            2,
            MEDIA_SEGMENT_WORKER_MAX_RUNTIME_SECONDS,
        )
        .await?;

        assert_eq!(summary.enqueue.providers_seen, 1);
        assert_eq!(summary.enqueue.jobs_queued, 1);
        assert_eq!(summary.jobs_run, 1);
        assert_eq!(summary.jobs_succeeded, 1);

        for media_file_id in media_file_ids {
            let active = list_active_segments_for_file(&pool, &media_file_id).await?;
            assert_eq!(active.len(), 1, "active segments for {media_file_id}");
            assert_eq!(active[0].segment_type, "intro");
            assert_eq!(active[0].source_label, "Local audio intro");
            assert!(active[0].confidence >= LOCAL_AUDIO_DETECTOR_MIN_CONFIDENCE);
        }
        Ok(())
    }

    #[tokio::test]
    async fn local_audio_detector_ignores_no_match_season() -> Result<()> {
        let pool = test_pool().await?;
        let (season_id, media_file_ids) = seed_audio_detector_season(
            &pool,
            vec![
                json!({"windows": [
                    {"start_seconds": 30.0, "end_seconds": 90.0, "hash": "episode-1-opening"}
                ]}),
                json!({"windows": [
                    {"start_seconds": 30.0, "end_seconds": 90.0, "hash": "episode-2-opening"}
                ]}),
                json!({"windows": [
                    {"start_seconds": 30.0, "end_seconds": 90.0, "hash": "episode-3-opening"}
                ]}),
            ],
        )
        .await?;
        let preferences = preferences_with_local_audio_detector()?;

        let queued = enqueue_local_audio_recurring_detector_job(&pool, &season_id, 10).await?;
        assert_eq!(queued.status, "queued");
        let run = run_next_media_segment_job(&pool, &preferences, "midm-local-audio-test")
            .await?
            .expect("queued local audio job should run");
        assert_eq!(run.job.status, "succeeded");
        let detector_summary = run
            .local_audio_summary
            .expect("local audio job should return detector summary");
        assert_eq!(detector_summary.repeated_groups, 0);
        assert_eq!(detector_summary.candidates_submitted, 0);

        for media_file_id in media_file_ids {
            assert!(
                list_active_segments_for_file(&pool, &media_file_id)
                    .await?
                    .is_empty()
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn local_visual_detector_emits_movie_credits_from_sustained_late_frames() -> Result<()> {
        let pool = test_pool().await?;
        let (media_file_id, _) = seed_movie_file(&pool, 1_800.0).await?;
        upsert_video_frame_hash(
            &pool,
            &media_file_id,
            1_800.0,
            json!({
                "version": "test-visual-v1",
                "frames": [
                    {"time_seconds": 900.0, "black_ratio": 0.95, "text_ratio": 0.02},
                    {"time_seconds": 1_660.0, "black_ratio": 0.91, "text_ratio": 0.16},
                    {"time_seconds": 1_682.0, "black_ratio": 0.93, "text_ratio": 0.22},
                    {"time_seconds": 1_704.0, "black_ratio": 0.88, "text_ratio": 0.18},
                    {"time_seconds": 1_730.0, "black_ratio": 0.92, "text_ratio": 0.20}
                ]
            }),
        )
        .await?;
        let preferences = preferences_with_local_visual_detector()?;

        let queued = enqueue_local_visual_credits_detector_job(&pool, &media_file_id, 10).await?;
        assert_eq!(queued.status, "queued");
        let run = run_next_media_segment_job(&pool, &preferences, "midm-local-visual-test")
            .await?
            .expect("queued local visual job should run");
        assert_eq!(run.job.status, "succeeded");
        let visual_summary = run
            .local_visual_summary
            .expect("local visual job should return detector summary");
        assert_eq!(visual_summary.frames_seen, 5);
        assert_eq!(visual_summary.credits_like_frames, 4);
        assert_eq!(visual_summary.candidates_submitted, 1);
        assert_eq!(visual_summary.candidates_accepted, 1);
        assert_eq!(visual_summary.active_segments, 1);

        let active = list_active_segments_for_file(&pool, &media_file_id).await?;
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].segment_type, "credits");
        assert_eq!(active[0].source_label, "Local visual credits");
        assert_eq!(active[0].start_seconds, 1_660.0);
        assert_eq!(active[0].end_seconds, 1_800.0);
        assert!(active[0].confidence >= LOCAL_VISUAL_CREDITS_MIN_CONFIDENCE);
        Ok(())
    }

    #[tokio::test]
    async fn local_visual_detector_preserves_post_credit_scene_between_credit_runs() -> Result<()> {
        let pool = test_pool().await?;
        let (media_file_id, _) = seed_movie_file(&pool, 2_400.0).await?;
        upsert_video_frame_hash(
            &pool,
            &media_file_id,
            2_400.0,
            json!({
                "version": "test-visual-v1",
                "frames": [
                    {"time_seconds": 1_800.0, "black_ratio": 0.90, "text_ratio": 0.16},
                    {"time_seconds": 1_830.0, "black_ratio": 0.92, "text_ratio": 0.20},
                    {"time_seconds": 1_860.0, "black_ratio": 0.91, "text_ratio": 0.18},
                    {"time_seconds": 1_890.0, "black_ratio": 0.12, "text_ratio": 0.01},
                    {"time_seconds": 1_920.0, "black_ratio": 0.14, "text_ratio": 0.01},
                    {"time_seconds": 1_950.0, "black_ratio": 0.16, "text_ratio": 0.02},
                    {"time_seconds": 2_220.0, "black_ratio": 0.93, "text_ratio": 0.22},
                    {"time_seconds": 2_250.0, "black_ratio": 0.94, "text_ratio": 0.21},
                    {"time_seconds": 2_280.0, "black_ratio": 0.92, "text_ratio": 0.19}
                ]
            }),
        )
        .await?;
        let preferences = preferences_with_local_visual_detector()?;

        enqueue_local_visual_credits_detector_job(&pool, &media_file_id, 10).await?;
        let run =
            run_next_media_segment_job(&pool, &preferences, "midm-local-visual-post-credit-test")
                .await?
                .expect("queued local visual job should run");
        assert_eq!(run.job.status, "succeeded");
        let visual_summary = run
            .local_visual_summary
            .expect("local visual job should return detector summary");
        assert_eq!(visual_summary.credits_like_frames, 6);
        assert_eq!(visual_summary.sustained_runs, 2);
        assert_eq!(visual_summary.candidates_submitted, 2);
        assert_eq!(visual_summary.candidates_accepted, 2);
        assert_eq!(visual_summary.active_segments, 2);

        let active = list_active_segments_for_file(&pool, &media_file_id).await?;
        assert_eq!(active.len(), 2);
        assert_eq!(active[0].segment_type, "credits");
        assert_eq!(active[0].start_seconds, 1_800.0);
        assert_eq!(active[0].end_seconds, 1_890.0);
        assert_eq!(active[1].segment_type, "credits");
        assert_eq!(active[1].start_seconds, 2_220.0);
        assert_eq!(active[1].end_seconds, 2_400.0);
        Ok(())
    }

    #[test]
    fn local_visual_frame_hash_plan_is_bounded_to_late_range() {
        let plan = local_visual_frame_hash_plan(7_200.0, None);
        assert_eq!(plan.len(), 1);
        let range = &plan[0];
        assert!(range.start_seconds >= 7_200.0 * LOCAL_VISUAL_CREDITS_MIN_START_FRACTION);
        assert!(range.end_seconds <= 7_200.0);
        assert_eq!(range.step_seconds, LOCAL_VISUAL_FRAME_HASH_STEP_SECONDS);
        assert!(!range.frames.is_empty());
        assert!(range.frames.len() <= LOCAL_VISUAL_FRAME_HASH_MAX_FRAMES_PER_FILE);
    }

    #[test]
    fn local_visual_frame_hash_gray_frame_detects_dark_text_edges() {
        let width = 16usize;
        let height = 8usize;
        let mut frame = vec![0_u8; width * height];
        for y in 2..6 {
            for x in 4..12 {
                if x == 4 || x == 11 || y == 2 || y == 5 {
                    frame[y * width + x] = 255;
                }
            }
        }

        let output = local_visual_frame_hash_for_gray_frame(1_700.0, &frame, width, height);
        assert_eq!(output.time_seconds, 1_700.0);
        assert!(output.black_ratio > 0.70);
        assert!(output.text_ratio > 0.05);
        assert!(output.hash.starts_with("lvf1:"));
    }

    #[tokio::test]
    async fn local_visual_frame_hash_worker_enqueues_missing_movie_hashes() -> Result<()> {
        let pool = test_pool().await?;
        let (media_file_id, _) = seed_movie_file(&pool, 1_800.0).await?;
        let preferences = preferences_with_local_visual_detector()?;

        let summary = run_media_segment_job_worker_iteration_with_preferences(
            &pool,
            &preferences,
            "midm-local-visual-hash-enqueue-test",
            10,
            0,
            MEDIA_SEGMENT_WORKER_MAX_RUNTIME_SECONDS,
        )
        .await?;

        assert_eq!(summary.enqueue.providers_seen, 1);
        assert_eq!(summary.enqueue.files_seen, 1);
        assert_eq!(summary.enqueue.jobs_queued, 1);
        assert_eq!(summary.jobs_run, 0);

        let queued = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM media_segment_jobs
             WHERE job_type = 'video_frame_hash'
               AND scope_type = 'media_file'
               AND provider_kind = 'local_visual_recurring'
               AND scope_id = $1
               AND status = 'queued'",
        )
        .bind(&media_file_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(queued, 1);

        let detector_jobs = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM media_segment_jobs
             WHERE job_type = 'local_detector'
               AND provider_kind = 'local_visual_recurring'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(detector_jobs, 0);
        Ok(())
    }

    #[tokio::test]
    async fn media_segment_worker_load_is_bounded_by_run_batch_limit() -> Result<()> {
        let pool = test_pool().await?;
        let preferences = preferences_with_local_visual_detector()?;
        for idx in 0..12 {
            let (media_file_id, _) = seed_movie_file(&pool, 1_800.0).await?;
            upsert_video_frame_hash(
                &pool,
                &media_file_id,
                1_800.0,
                json!({
                    "version": "test-visual-v1",
                    "frames": [
                        {"time_seconds": 1_500.0, "black_ratio": 0.12, "text_ratio": 0.01},
                        {"time_seconds": 1_560.0, "black_ratio": 0.14, "text_ratio": 0.02},
                        {"time_seconds": 1_620.0, "black_ratio": 0.13, "text_ratio": 0.01}
                    ]
                }),
            )
            .await?;
            let queued =
                enqueue_local_visual_credits_detector_job(&pool, &media_file_id, 10 + idx).await?;
            assert_eq!(queued.status, "queued");
        }

        let first_pass = run_media_segment_job_worker_iteration_with_preferences(
            &pool,
            &preferences,
            "midm-load-worker",
            0,
            4,
            MEDIA_SEGMENT_WORKER_MAX_RUNTIME_SECONDS,
        )
        .await?;
        assert_eq!(first_pass.enqueue.jobs_queued, 0);
        assert_eq!(first_pass.jobs_run, 4);
        assert_eq!(first_pass.jobs_succeeded, 4);
        assert_eq!(first_pass.jobs_failed, 0);
        assert!(!first_pass.runtime_budget_exhausted);

        let succeeded_after_first = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM media_segment_jobs
             WHERE job_type = 'local_detector'
               AND provider_kind = 'local_visual_recurring'
               AND status = 'succeeded'",
        )
        .fetch_one(&pool)
        .await?;
        let queued_after_first = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM media_segment_jobs
             WHERE job_type = 'local_detector'
               AND provider_kind = 'local_visual_recurring'
               AND status = 'queued'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(succeeded_after_first, 4);
        assert_eq!(queued_after_first, 8);

        let second_pass = run_media_segment_job_worker_iteration_with_preferences(
            &pool,
            &preferences,
            "midm-load-worker",
            0,
            4,
            MEDIA_SEGMENT_WORKER_MAX_RUNTIME_SECONDS,
        )
        .await?;
        assert_eq!(second_pass.jobs_run, 4);
        assert_eq!(second_pass.jobs_succeeded, 4);

        let remaining_queued = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM media_segment_jobs
             WHERE job_type = 'local_detector'
               AND provider_kind = 'local_visual_recurring'
               AND status = 'queued'",
        )
        .fetch_one(&pool)
        .await?;
        let running_count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM media_segment_jobs
             WHERE job_type = 'local_detector'
               AND provider_kind = 'local_visual_recurring'
               AND status = 'running'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(remaining_queued, 4);
        assert_eq!(running_count, 0);
        Ok(())
    }

    #[tokio::test]
    async fn local_visual_frame_hash_job_skips_missing_file_cleanly() -> Result<()> {
        let pool = test_pool().await?;
        let (media_file_id, _) = seed_movie_file(&pool, 1_800.0).await?;
        let preferences = preferences_with_local_visual_detector()?;

        let queued = enqueue_local_visual_frame_hash_job(&pool, &media_file_id, 10).await?;
        assert_eq!(queued.status, "queued");
        let run =
            run_next_media_segment_job(&pool, &preferences, "midm-local-visual-hash-missing-test")
                .await?
                .expect("queued local visual frame hash job should run");

        assert_eq!(run.job.status, "skipped");
        let summary = run
            .local_visual_frame_hash_summary
            .expect("visual frame hash job should return summary");
        assert_eq!(summary.media_file_id, media_file_id);
        assert_eq!(summary.status, "skipped");
        assert!(
            summary
                .reason
                .as_deref()
                .unwrap_or_default()
                .starts_with("file_unavailable:")
        );
        Ok(())
    }

    #[tokio::test]
    async fn local_visual_detector_ignores_midroll_dark_text_without_late_sustain() -> Result<()> {
        let pool = test_pool().await?;
        let (media_file_id, _) = seed_movie_file(&pool, 1_800.0).await?;
        upsert_video_frame_hash(
            &pool,
            &media_file_id,
            1_800.0,
            json!({
                "frames": [
                    {"time_seconds": 600.0, "black_ratio": 0.94, "text_ratio": 0.24},
                    {"time_seconds": 625.0, "black_ratio": 0.95, "text_ratio": 0.25},
                    {"time_seconds": 650.0, "black_ratio": 0.93, "text_ratio": 0.23},
                    {"time_seconds": 1_700.0, "black_ratio": 0.92, "text_ratio": 0.18}
                ]
            }),
        )
        .await?;
        let preferences = preferences_with_local_visual_detector()?;

        enqueue_local_visual_credits_detector_job(&pool, &media_file_id, 10).await?;
        let run = run_next_media_segment_job(&pool, &preferences, "midm-local-visual-negative")
            .await?
            .expect("queued local visual job should run");
        assert_eq!(run.job.status, "succeeded");
        let visual_summary = run
            .local_visual_summary
            .expect("local visual job should return detector summary");
        assert_eq!(visual_summary.credits_like_frames, 1);
        assert_eq!(visual_summary.candidates_submitted, 0);
        assert_eq!(
            visual_summary.reason.as_deref(),
            Some("no_sustained_credits_run")
        );
        assert!(
            list_active_segments_for_file(&pool, &media_file_id)
                .await?
                .is_empty()
        );
        Ok(())
    }

    #[derive(Debug)]
    struct DetectorCorpusOutcome {
        detector: &'static str,
        case_id: &'static str,
        expected_positive: bool,
        observed_active_segments: usize,
        observed_candidates: usize,
        passed: bool,
        detail: String,
    }

    impl DetectorCorpusOutcome {
        fn failure_summary(&self) -> String {
            format!(
                "{}:{} expected_positive={} observed_active={} observed_candidates={} detail={}",
                self.detector,
                self.case_id,
                self.expected_positive,
                self.observed_active_segments,
                self.observed_candidates,
                self.detail
            )
        }
    }

    #[derive(Debug, Default)]
    struct DetectorCorpusReport {
        cases_run: usize,
        expected_positive_cases: usize,
        expected_negative_cases: usize,
        failures: Vec<DetectorCorpusOutcome>,
    }

    impl DetectorCorpusReport {
        fn record(&mut self, outcome: DetectorCorpusOutcome) {
            self.cases_run += 1;
            if outcome.expected_positive {
                self.expected_positive_cases += 1;
            } else {
                self.expected_negative_cases += 1;
            }
            if !outcome.passed {
                self.failures.push(outcome);
            }
        }
    }

    struct AudioDetectorCorpusCase {
        id: &'static str,
        fingerprints: Vec<Value>,
        expected_segment_type: Option<&'static str>,
    }

    #[derive(Clone)]
    struct ExpectedVisualSegment {
        start_min: f64,
        start_max: f64,
        end_min: f64,
        end_max: f64,
    }

    struct VisualDetectorCorpusCase {
        id: &'static str,
        duration_seconds: f64,
        frames: Value,
        expected_segments: Vec<ExpectedVisualSegment>,
        expected_reason: Option<&'static str>,
    }

    #[tokio::test]
    async fn midm_synthetic_detector_corpus_meets_release_quality_floor() -> Result<()> {
        let mut report = DetectorCorpusReport::default();

        for case in synthetic_audio_detector_corpus() {
            report.record(run_audio_detector_corpus_case(case).await?);
        }
        for case in synthetic_visual_detector_corpus() {
            report.record(run_visual_detector_corpus_case(case).await?);
        }

        assert_eq!(
            report.cases_run, 9,
            "corpus case count drifted: {report:#?}"
        );
        assert_eq!(
            report.expected_positive_cases, 4,
            "corpus must keep positive detector coverage: {report:#?}"
        );
        assert_eq!(
            report.expected_negative_cases, 5,
            "corpus must keep false-positive coverage: {report:#?}"
        );
        let failure_summary = report
            .failures
            .iter()
            .map(DetectorCorpusOutcome::failure_summary)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            report.failures.is_empty(),
            "MIDM synthetic detector corpus failures:\n{failure_summary}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn midm_generated_media_detector_corpus_proves_positive_paths() -> Result<()> {
        if !midm_detector_tool_available("ffmpeg").await
            || !midm_detector_tool_available("ffprobe").await
        {
            eprintln!(
                "skipping MIDM generated-media detector corpus: ffmpeg or ffprobe unavailable"
            );
            return Ok(());
        }

        let temp_dir = tempfile::Builder::new()
            .prefix("elixir-midm-generated-detector-corpus-")
            .tempdir()
            .context("creating MIDM generated detector corpus temp dir")?;

        let audio_paths = [1_i64, 2, 3]
            .iter()
            .map(|episode| {
                temp_dir
                    .path()
                    .join(format!("generated_intro_positive_s01e{episode:02}.mka"))
            })
            .collect::<Vec<_>>();
        for (path, frequency) in audio_paths.iter().zip([610_u32, 730, 860]) {
            generate_midm_audio_detector_positive_episode(path, frequency).await?;
        }
        assert_generated_audio_detector_positive(&audio_paths).await?;

        let visual_path = temp_dir
            .path()
            .join("generated_visual_credits_positive.mkv");
        generate_midm_visual_detector_positive_movie(&visual_path).await?;
        assert_generated_visual_detector_positive(&visual_path).await?;

        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires hydrated real media from docs/contracts/midm-detector-corpus.yml"]
    async fn midm_detector_real_media_corpus_runs_when_present() -> Result<()> {
        let manifest = load_midm_detector_corpus_manifest()?;
        if std::env::var(&manifest.enable_env).is_err() {
            eprintln!(
                "skipping MIDM real-media detector corpus: set {}=1 to enable",
                manifest.enable_env
            );
            return Ok(());
        }

        let suite_filter = std::env::var(&manifest.suite_filter_env)
            .ok()
            .map(|value| value.trim().to_ascii_lowercase())
            .filter(|value| !value.is_empty() && value != "all");
        let release_signoff_selected =
            midm_detector_release_signoff_selected(suite_filter.as_deref());
        let tools_available = midm_detector_tool_available("ffmpeg").await
            && midm_detector_tool_available("ffprobe").await;
        if !tools_available {
            ensure!(
                !release_signoff_selected,
                "ffmpeg and ffprobe are required for MIDM release detector corpus signoff"
            );
            eprintln!("skipping MIDM real-media detector corpus: ffmpeg or ffprobe unavailable");
            return Ok(());
        }

        let mut report = RealMediaDetectorCorpusReport::default();
        let mut missing_required = Vec::new();

        for case in manifest
            .cases
            .iter()
            .filter(|case| midm_detector_suite_selected(case, suite_filter.as_deref()))
        {
            let missing_paths = midm_detector_missing_paths(case);
            if !missing_paths.is_empty() {
                let detail = format!("{} missing {}", case.id, missing_paths.join(", "));
                if case.release_required {
                    missing_required.push(detail);
                } else {
                    report.skipped.push(detail);
                }
                continue;
            }

            let observation = match case.detector.as_str() {
                PROVIDER_LOCAL_AUDIO_RECURRING => run_real_media_audio_detector_case(case).await?,
                PROVIDER_LOCAL_VISUAL_RECURRING => {
                    run_real_media_visual_detector_case(case).await?
                }
                _ => unreachable!("manifest validation restricts detector kinds"),
            };

            report.cases_run += 1;
            let expected_positive = midm_detector_case_expected_positive(case);
            if midm_detector_case_is_release(case) {
                report.release_cases_run += 1;
                if expected_positive {
                    report.release_positive_cases_run += 1;
                    report
                        .release_positive_detectors
                        .insert(case.detector.clone());
                } else {
                    report.release_negative_cases_run += 1;
                    report
                        .release_negative_detectors
                        .insert(case.detector.clone());
                }
            }
            if !expected_positive {
                report.false_positive_segments += observation.active_segments;
            }
            if let Some(detail) = observation.failure_detail {
                report.failures.push(format!("{}: {detail}", case.id));
            }
        }

        ensure!(
            missing_required.is_empty(),
            "MIDM required detector corpus files are missing:\n{}",
            missing_required.join("\n")
        );
        ensure!(
            report.cases_run > 0,
            "MIDM real-media detector corpus ran zero cases; skipped={:?}",
            report.skipped
        );
        ensure!(
            report.false_positive_segments <= manifest.quality_gates.max_false_positive_segments,
            "MIDM detector corpus observed {} false-positive active segments; allowed {}",
            report.false_positive_segments,
            manifest.quality_gates.max_false_positive_segments
        );
        ensure!(
            report.failures.is_empty(),
            "MIDM real-media detector corpus failures:\n{}",
            report.failures.join("\n")
        );
        if release_signoff_selected {
            validate_midm_real_media_release_report(&manifest, &report)?;
        }
        Ok(())
    }

    async fn assert_generated_audio_detector_positive(paths: &[PathBuf]) -> Result<()> {
        let pool = test_pool().await?;
        let files = paths
            .iter()
            .enumerate()
            .map(|(index, path)| MidmDetectorCorpusFile {
                episode_number: Some(index as i64 + 1),
                local_path: path.clone(),
                duration_seconds: Some(360.0),
            })
            .collect::<Vec<_>>();
        let (season_id, media_file_ids) =
            seed_audio_detector_real_media_season_absolute(&pool, &files).await?;
        let preferences = preferences_with_local_audio_detector()?;

        for media_file_id in &media_file_ids {
            enqueue_local_audio_fingerprint_job(&pool, media_file_id, 10).await?;
            let run = run_next_media_segment_job(
                &pool,
                &preferences,
                "midm-generated-audio-fingerprint-corpus",
            )
            .await?
            .context("generated audio fingerprint job was not claimed")?;
            let summary = run
                .local_audio_fingerprint_summary
                .context("generated audio fingerprint job returned no summary")?;
            assert_eq!(summary.status, "ok");
            assert!(
                summary.windows_fingerprinted > 0,
                "generated audio file should produce fingerprint windows: {summary:?}"
            );
        }

        enqueue_local_audio_recurring_detector_job(&pool, &season_id, 10).await?;
        let run = run_next_media_segment_job(&pool, &preferences, "midm-generated-audio-corpus")
            .await?
            .context("generated audio detector job was not claimed")?;
        let summary = run
            .local_audio_summary
            .context("generated audio detector job returned no summary")?;
        assert_eq!(summary.status, "ok");
        assert_eq!(summary.candidates_accepted, media_file_ids.len());

        let mut active = Vec::new();
        for media_file_id in &media_file_ids {
            active.extend(list_active_segments_for_file(&pool, media_file_id).await?);
        }
        assert_eq!(active.len(), media_file_ids.len());
        for segment in &active {
            assert_eq!(segment.segment_type, "intro");
            assert!(
                (0.0..=20.0).contains(&segment.start_seconds),
                "generated audio intro start drifted: {segment:?}"
            );
            assert!(
                (55.0..=80.0).contains(&segment.end_seconds),
                "generated audio intro end drifted: {segment:?}"
            );
            assert!(segment.confidence >= LOCAL_AUDIO_DETECTOR_MIN_CONFIDENCE);
        }
        Ok(())
    }

    async fn assert_generated_visual_detector_positive(path: &Path) -> Result<()> {
        let duration_seconds = probe_midm_detector_media_duration(path).await?;
        let pool = test_pool().await?;
        let (media_file_id, _) = seed_movie_file_at_path(&pool, path, duration_seconds).await?;
        let preferences = preferences_with_local_visual_detector()?;

        enqueue_local_visual_frame_hash_job(&pool, &media_file_id, 10).await?;
        let frame_hash_run = run_next_media_segment_job(
            &pool,
            &preferences,
            "midm-generated-visual-frame-hash-corpus",
        )
        .await?
        .context("generated visual frame-hash job was not claimed")?;
        let frame_hash_summary = frame_hash_run
            .local_visual_frame_hash_summary
            .context("generated visual frame-hash job returned no summary")?;
        assert_eq!(frame_hash_summary.status, "ok");
        assert!(
            frame_hash_summary.frames_extracted >= LOCAL_VISUAL_CREDITS_MIN_FRAME_COUNT,
            "generated visual file should produce enough sampled frames: {frame_hash_summary:?}"
        );

        enqueue_local_visual_credits_detector_job(&pool, &media_file_id, 10).await?;
        let detector_run =
            run_next_media_segment_job(&pool, &preferences, "midm-generated-visual-corpus")
                .await?
                .context("generated visual detector job was not claimed")?;
        let visual_summary = detector_run
            .local_visual_summary
            .context("generated visual detector job returned no summary")?;
        assert_eq!(visual_summary.status, "ok");
        assert_eq!(visual_summary.candidates_accepted, 1);
        assert!(
            visual_summary.credits_like_frames >= LOCAL_VISUAL_CREDITS_MIN_FRAME_COUNT,
            "generated visual credits evidence was too weak: {visual_summary:?}"
        );

        let active = list_active_segments_for_file(&pool, &media_file_id).await?;
        assert_eq!(active.len(), 1);
        let segment = &active[0];
        assert_eq!(segment.segment_type, "credits");
        assert!(
            (230.0..=260.0).contains(&segment.start_seconds),
            "generated visual credits start drifted: {segment:?}"
        );
        assert!(
            (355.0..=365.0).contains(&segment.end_seconds),
            "generated visual credits end drifted: {segment:?}"
        );
        assert!(segment.confidence >= LOCAL_VISUAL_CREDITS_MIN_CONFIDENCE);
        Ok(())
    }

    #[derive(Debug)]
    struct RealMediaDetectorCaseObservation {
        active_segments: usize,
        failure_detail: Option<String>,
    }

    fn midm_detector_suite_selected(
        case: &MidmDetectorCorpusCase,
        suite_filter: Option<&str>,
    ) -> bool {
        suite_filter
            .map(|suite| case.suite == suite)
            .unwrap_or(true)
    }

    fn midm_detector_release_signoff_selected(suite_filter: Option<&str>) -> bool {
        suite_filter.map(|suite| suite == "release").unwrap_or(true)
    }

    fn midm_detector_case_is_release(case: &MidmDetectorCorpusCase) -> bool {
        case.suite == "release" || case.release_required
    }

    fn midm_detector_case_expected_positive(case: &MidmDetectorCorpusCase) -> bool {
        !case.expected.segments.is_empty()
    }

    fn validate_midm_real_media_release_report(
        manifest: &MidmDetectorCorpusManifest,
        report: &RealMediaDetectorCorpusReport,
    ) -> Result<()> {
        ensure!(
            report.release_cases_run >= manifest.quality_gates.min_release_cases,
            "MIDM release detector corpus ran {} release cases but requires at least {}",
            report.release_cases_run,
            manifest.quality_gates.min_release_cases
        );
        ensure!(
            report.release_positive_cases_run >= manifest.quality_gates.min_release_positive_cases,
            "MIDM release detector corpus ran {} positive release cases but requires at least {}",
            report.release_positive_cases_run,
            manifest.quality_gates.min_release_positive_cases
        );
        ensure!(
            report.release_negative_cases_run >= manifest.quality_gates.min_release_negative_cases,
            "MIDM release detector corpus ran {} negative release cases but requires at least {}",
            report.release_negative_cases_run,
            manifest.quality_gates.min_release_negative_cases
        );
        if manifest
            .quality_gates
            .require_release_positive_and_negative_per_detector
        {
            let required_detectors = manifest
                .quality_gates
                .required_detectors
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>();
            ensure!(
                report.release_positive_detectors == required_detectors,
                "MIDM release detector corpus positive detector coverage incomplete: observed {:?}, required {:?}",
                report.release_positive_detectors,
                required_detectors
            );
            ensure!(
                report.release_negative_detectors == required_detectors,
                "MIDM release detector corpus negative detector coverage incomplete: observed {:?}, required {:?}",
                report.release_negative_detectors,
                required_detectors
            );
        }
        Ok(())
    }

    fn midm_repo_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("elixir-server should have a repository parent")
            .to_path_buf()
    }

    fn midm_detector_absolute_path(path: &Path) -> PathBuf {
        midm_repo_root().join(path)
    }

    fn midm_detector_missing_paths(case: &MidmDetectorCorpusCase) -> Vec<String> {
        let paths = if let Some(path) = &case.local_path {
            vec![path]
        } else {
            case.files
                .iter()
                .map(|file| &file.local_path)
                .collect::<Vec<_>>()
        };
        paths
            .into_iter()
            .filter_map(|path| {
                let absolute = midm_detector_absolute_path(path);
                (!absolute.is_file()).then(|| absolute.display().to_string())
            })
            .collect()
    }

    async fn midm_detector_tool_available(binary: &str) -> bool {
        tokio::process::Command::new(binary)
            .arg("-version")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .await
            .map(|status| status.success())
            .unwrap_or(false)
    }

    async fn generate_midm_audio_detector_positive_episode(
        path: &Path,
        unique_frequency: u32,
    ) -> Result<()> {
        let unique = format!("sine=frequency={unique_frequency}:sample_rate=8000:duration=285");
        let args = vec![
            "-hide_banner".to_string(),
            "-y".to_string(),
            "-nostdin".to_string(),
            "-v".to_string(),
            "error".to_string(),
            "-f".to_string(),
            "lavfi".to_string(),
            "-i".to_string(),
            "sine=frequency=440:sample_rate=8000:duration=75".to_string(),
            "-f".to_string(),
            "lavfi".to_string(),
            "-i".to_string(),
            unique,
            "-filter_complex".to_string(),
            "[0:a][1:a]concat=n=2:v=0:a=1[a]".to_string(),
            "-map".to_string(),
            "[a]".to_string(),
            "-c:a".to_string(),
            "pcm_s16le".to_string(),
            path.to_string_lossy().to_string(),
        ];
        run_midm_detector_ffmpeg(&args, 60, "generated MIDM audio detector episode").await
    }

    async fn generate_midm_visual_detector_positive_movie(path: &Path) -> Result<()> {
        let mut filters = Vec::new();
        for y in (8..=80).step_by(6) {
            filters.push(format!(
                "drawbox=x=20:y={y}:w=120:h=1:color=white:t=fill:enable='gte(t,240)'"
            ));
        }
        for x in [25, 45, 65, 85, 105, 125] {
            filters.push(format!(
                "drawbox=x={x}:y=8:w=1:h=74:color=white:t=fill:enable='gte(t,240)'"
            ));
        }
        let args = vec![
            "-hide_banner".to_string(),
            "-y".to_string(),
            "-nostdin".to_string(),
            "-v".to_string(),
            "error".to_string(),
            "-f".to_string(),
            "lavfi".to_string(),
            "-i".to_string(),
            "color=c=black:size=160x90:rate=1:duration=360".to_string(),
            "-vf".to_string(),
            filters.join(","),
            "-an".to_string(),
            "-c:v".to_string(),
            "mpeg4".to_string(),
            "-q:v".to_string(),
            "5".to_string(),
            path.to_string_lossy().to_string(),
        ];
        run_midm_detector_ffmpeg(&args, 60, "generated MIDM visual detector movie").await
    }

    async fn run_midm_detector_ffmpeg(
        args: &[String],
        timeout_seconds: u64,
        context: &str,
    ) -> Result<()> {
        let mut command = tokio::process::Command::new("ffmpeg");
        command.kill_on_drop(true);
        let output = tokio::time::timeout(
            StdDuration::from_secs(timeout_seconds),
            command.args(args).output(),
        )
        .await
        .with_context(|| format!("{context} timed out after {timeout_seconds}s"))?
        .with_context(|| format!("spawning ffmpeg for {context}"))?;
        ensure!(
            output.status.success(),
            "{} failed with code {:?}: {}",
            context,
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(())
    }

    async fn run_real_media_visual_detector_case(
        case: &MidmDetectorCorpusCase,
    ) -> Result<RealMediaDetectorCaseObservation> {
        ensure!(
            case.media_type == "movie",
            "{} visual real-media runner currently supports movie fixtures",
            case.id
        );
        let path = case
            .local_path
            .as_ref()
            .map(|path| midm_detector_absolute_path(path))
            .with_context(|| format!("{} missing local_path", case.id))?;
        let duration_seconds = probe_midm_detector_media_duration(&path).await?;
        let pool = test_pool().await?;
        let (media_file_id, _) = seed_movie_file_at_path(&pool, &path, duration_seconds).await?;
        let preferences = preferences_with_local_visual_detector()?;

        enqueue_local_visual_frame_hash_job(&pool, &media_file_id, 10).await?;
        let frame_hash_run =
            run_next_media_segment_job(&pool, &preferences, "midm-real-visual-frame-hash-corpus")
                .await?
                .with_context(|| format!("{} frame-hash job was not claimed", case.id))?;
        let frame_hash_summary = frame_hash_run
            .local_visual_frame_hash_summary
            .with_context(|| format!("{} frame-hash job returned no summary", case.id))?;
        if !case.expected.segments.is_empty() && frame_hash_summary.status != "ok" {
            return Ok(RealMediaDetectorCaseObservation {
                active_segments: 0,
                failure_detail: Some(format!(
                    "expected positive visual case but frame hash status={} reason={:?}",
                    frame_hash_summary.status, frame_hash_summary.reason
                )),
            });
        }

        enqueue_local_visual_credits_detector_job(&pool, &media_file_id, 10).await?;
        let detector_run =
            run_next_media_segment_job(&pool, &preferences, "midm-real-visual-corpus")
                .await?
                .with_context(|| format!("{} visual detector job was not claimed", case.id))?;
        let visual_summary = detector_run
            .local_visual_summary
            .with_context(|| format!("{} visual detector job returned no summary", case.id))?;
        let active = list_active_segments_for_file(&pool, &media_file_id).await?;
        let failure_detail = validate_midm_real_media_case_result(
            case,
            &active,
            visual_summary.candidates_submitted,
            1,
        );

        Ok(RealMediaDetectorCaseObservation {
            active_segments: active.len(),
            failure_detail,
        })
    }

    async fn run_real_media_audio_detector_case(
        case: &MidmDetectorCorpusCase,
    ) -> Result<RealMediaDetectorCaseObservation> {
        let pool = test_pool().await?;
        let (season_id, media_file_ids) =
            seed_audio_detector_real_media_season(&pool, &case.files).await?;
        let preferences = preferences_with_local_audio_detector()?;

        for media_file_id in &media_file_ids {
            enqueue_local_audio_fingerprint_job(&pool, media_file_id, 10).await?;
            let fingerprint_run = run_next_media_segment_job(
                &pool,
                &preferences,
                "midm-real-audio-fingerprint-corpus",
            )
            .await?
            .with_context(|| format!("{} audio fingerprint job was not claimed", case.id))?;
            let fingerprint_summary = fingerprint_run
                .local_audio_fingerprint_summary
                .with_context(|| {
                    format!("{} audio fingerprint job returned no summary", case.id)
                })?;
            if !case.expected.segments.is_empty() && fingerprint_summary.status != "ok" {
                return Ok(RealMediaDetectorCaseObservation {
                    active_segments: 0,
                    failure_detail: Some(format!(
                        "expected positive audio case but fingerprint status={} reason={:?}",
                        fingerprint_summary.status, fingerprint_summary.reason
                    )),
                });
            }
        }

        enqueue_local_audio_recurring_detector_job(&pool, &season_id, 10).await?;
        let detector_run =
            run_next_media_segment_job(&pool, &preferences, "midm-real-audio-corpus")
                .await?
                .with_context(|| format!("{} audio detector job was not claimed", case.id))?;
        let audio_summary = detector_run
            .local_audio_summary
            .with_context(|| format!("{} audio detector job returned no summary", case.id))?;
        let mut active = Vec::new();
        for media_file_id in &media_file_ids {
            active.extend(list_active_segments_for_file(&pool, media_file_id).await?);
        }
        let failure_detail = validate_midm_real_media_case_result(
            case,
            &active,
            audio_summary.candidates_submitted,
            media_file_ids.len(),
        );

        Ok(RealMediaDetectorCaseObservation {
            active_segments: active.len(),
            failure_detail,
        })
    }

    async fn probe_midm_detector_media_duration(path: &Path) -> Result<f64> {
        let path_text = path.to_string_lossy();
        let metadata = ffprobe::probe(path_text.as_ref())
            .await
            .with_context(|| format!("probing MIDM detector corpus media {}", path.display()))?;
        metadata
            .duration_seconds
            .map(f64::from)
            .filter(|duration| *duration > 0.0)
            .with_context(|| format!("{} has no positive probed duration", path.display()))
    }

    async fn seed_movie_file_at_path(
        pool: &AnyPool,
        path: &Path,
        duration_seconds: f64,
    ) -> Result<(String, String)> {
        let (media_file_id, movie_id) = seed_movie_file(pool, duration_seconds).await?;
        update_media_file_path(pool, &media_file_id, path).await?;
        Ok((media_file_id, movie_id))
    }

    async fn seed_audio_detector_real_media_season(
        pool: &AnyPool,
        files: &[MidmDetectorCorpusFile],
    ) -> Result<(String, Vec<String>)> {
        let (season_id, media_file_ids) =
            seed_audio_detector_season_without_fingerprints(pool, files.len()).await?;
        for (index, (media_file_id, file)) in media_file_ids.iter().zip(files).enumerate() {
            let path = midm_detector_absolute_path(&file.local_path);
            update_audio_detector_real_media_season_file(pool, media_file_id, file, &path, index)
                .await?;
        }
        Ok((season_id, media_file_ids))
    }

    async fn seed_audio_detector_real_media_season_absolute(
        pool: &AnyPool,
        files: &[MidmDetectorCorpusFile],
    ) -> Result<(String, Vec<String>)> {
        let (season_id, media_file_ids) =
            seed_audio_detector_season_without_fingerprints(pool, files.len()).await?;
        for (index, (media_file_id, file)) in media_file_ids.iter().zip(files).enumerate() {
            update_audio_detector_real_media_season_file(
                pool,
                media_file_id,
                file,
                &file.local_path,
                index,
            )
            .await?;
        }
        Ok((season_id, media_file_ids))
    }

    async fn update_audio_detector_real_media_season_file(
        pool: &AnyPool,
        media_file_id: &str,
        file: &MidmDetectorCorpusFile,
        path: &Path,
        index: usize,
    ) -> Result<()> {
        update_media_file_path(pool, media_file_id, path).await?;
        let episode_number = file.episode_number.unwrap_or(index as i64 + 1);
        let duration_seconds = file
            .duration_seconds
            .map(|duration| duration.round() as i64)
            .filter(|duration| *duration > 0);
        sqlx::query(
            "UPDATE episodes
                 SET episode_number = $1, absolute_episode_number = $2,
                     title = $3, runtime_seconds = COALESCE($4, runtime_seconds)
                 WHERE id IN (
                     SELECT episode_id FROM episode_files WHERE media_file_id = $5
                 )",
        )
        .bind(episode_number)
        .bind(episode_number)
        .bind(format!("Episode {episode_number}"))
        .bind(duration_seconds)
        .bind(media_file_id)
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn update_media_file_path(
        pool: &AnyPool,
        media_file_id: &str,
        path: &Path,
    ) -> Result<()> {
        let size_bytes = std::fs::metadata(path)
            .with_context(|| format!("stat {}", path.display()))?
            .len() as i64;
        sqlx::query(
            "UPDATE media_files
             SET path = $1, size_bytes = $2, scan_state = 'ok', updated_at = CURRENT_TIMESTAMP
             WHERE id = $3",
        )
        .bind(path.to_string_lossy().to_string())
        .bind(size_bytes)
        .bind(media_file_id)
        .execute(pool)
        .await?;
        Ok(())
    }

    fn validate_midm_real_media_case_result(
        case: &MidmDetectorCorpusCase,
        active: &[ActiveMediaSegmentRecord],
        candidates_submitted: usize,
        media_file_count: usize,
    ) -> Option<String> {
        if case.expected.segments.is_empty() {
            let max_active = case.expected.max_active_segments.unwrap_or(0);
            let max_candidates = case.expected.max_candidates.unwrap_or(0);
            if active.len() > max_active || candidates_submitted > max_candidates {
                return Some(format!(
                    "expected no detector output, observed active={} candidates={} segments={active:?}",
                    active.len(),
                    candidates_submitted
                ));
            }
            return None;
        }

        let expected_count = if case.expected.segment_count.as_deref() == Some("one_per_file") {
            media_file_count.saturating_mul(case.expected.segments.len())
        } else {
            case.expected.segments.len()
        };
        if active.len() != expected_count {
            return Some(format!(
                "expected {expected_count} active segments, observed {}: {active:?}",
                active.len()
            ));
        }

        for expected in &case.expected.segments {
            let matching_count = active
                .iter()
                .filter(|segment| {
                    segment.segment_type == expected.segment_type
                        && segment.start_seconds >= expected.start_seconds_min
                        && segment.start_seconds <= expected.start_seconds_max
                        && segment.end_seconds >= expected.end_seconds_min
                        && segment.end_seconds <= expected.end_seconds_max
                        && segment.confidence >= expected.confidence_min
                })
                .count();
            let required_matches = if case.expected.segment_count.as_deref() == Some("one_per_file")
            {
                media_file_count
            } else {
                1
            };
            if matching_count < required_matches {
                return Some(format!(
                    "expected at least {required_matches} matching {} segment(s), observed {matching_count}: {active:?}",
                    expected.segment_type
                ));
            }
        }

        None
    }

    fn synthetic_audio_detector_corpus() -> Vec<AudioDetectorCorpusCase> {
        vec![
            AudioDetectorCorpusCase {
                id: "audio_repeated_intro",
                expected_segment_type: Some("intro"),
                fingerprints: vec![
                    json!({"windows": [
                        {"start_seconds": 30.0, "end_seconds": 90.0, "hash": "corpus-opening-a"},
                        {"start_seconds": 430.0, "end_seconds": 490.0, "hash": "corpus-unique-1"}
                    ]}),
                    json!({"windows": [
                        {"start_seconds": 31.0, "end_seconds": 91.0, "hash": "corpus-opening-a"},
                        {"start_seconds": 510.0, "end_seconds": 570.0, "hash": "corpus-unique-2"}
                    ]}),
                    json!({"windows": [
                        {"start_seconds": 29.5, "end_seconds": 89.5, "hash": "corpus-opening-a"},
                        {"start_seconds": 610.0, "end_seconds": 670.0, "hash": "corpus-unique-3"}
                    ]}),
                ],
            },
            AudioDetectorCorpusCase {
                id: "audio_repeated_outro",
                expected_segment_type: Some("outro"),
                fingerprints: vec![
                    json!({"windows": [
                        {"start_seconds": 1_325.0, "end_seconds": 1_390.0, "hash": "corpus-ending-a"}
                    ]}),
                    json!({"windows": [
                        {"start_seconds": 1_330.0, "end_seconds": 1_395.0, "hash": "corpus-ending-a"}
                    ]}),
                    json!({"windows": [
                        {"start_seconds": 1_320.0, "end_seconds": 1_385.0, "hash": "corpus-ending-a"}
                    ]}),
                ],
            },
            AudioDetectorCorpusCase {
                id: "audio_repeated_midroll_negative",
                expected_segment_type: None,
                fingerprints: vec![
                    json!({"windows": [
                        {"start_seconds": 610.0, "end_seconds": 670.0, "hash": "corpus-midroll-a"}
                    ]}),
                    json!({"windows": [
                        {"start_seconds": 612.0, "end_seconds": 672.0, "hash": "corpus-midroll-a"}
                    ]}),
                    json!({"windows": [
                        {"start_seconds": 614.0, "end_seconds": 674.0, "hash": "corpus-midroll-a"}
                    ]}),
                ],
            },
            AudioDetectorCorpusCase {
                id: "audio_short_repeated_jingle_negative",
                expected_segment_type: None,
                fingerprints: vec![
                    json!({"windows": [
                        {"start_seconds": 30.0, "end_seconds": 42.0, "hash": "corpus-short-a"}
                    ]}),
                    json!({"windows": [
                        {"start_seconds": 31.0, "end_seconds": 43.0, "hash": "corpus-short-a"}
                    ]}),
                    json!({"windows": [
                        {"start_seconds": 29.0, "end_seconds": 41.0, "hash": "corpus-short-a"}
                    ]}),
                ],
            },
            AudioDetectorCorpusCase {
                id: "audio_intro_ending_too_late_negative",
                expected_segment_type: None,
                fingerprints: vec![
                    json!({"windows": [
                        {"start_seconds": 20.0, "end_seconds": 260.0, "hash": "corpus-overlong-opening"}
                    ]}),
                    json!({"windows": [
                        {"start_seconds": 22.0, "end_seconds": 262.0, "hash": "corpus-overlong-opening"}
                    ]}),
                    json!({"windows": [
                        {"start_seconds": 18.0, "end_seconds": 258.0, "hash": "corpus-overlong-opening"}
                    ]}),
                ],
            },
        ]
    }

    fn synthetic_visual_detector_corpus() -> Vec<VisualDetectorCorpusCase> {
        vec![
            VisualDetectorCorpusCase {
                id: "visual_sustained_movie_credits",
                duration_seconds: 1_800.0,
                expected_reason: None,
                expected_segments: vec![ExpectedVisualSegment {
                    start_min: 1_659.0,
                    start_max: 1_661.0,
                    end_min: 1_799.0,
                    end_max: 1_801.0,
                }],
                frames: json!([
                    {"time_seconds": 900.0, "black_ratio": 0.95, "text_ratio": 0.02},
                    {"time_seconds": 1_660.0, "black_ratio": 0.91, "text_ratio": 0.16},
                    {"time_seconds": 1_682.0, "black_ratio": 0.93, "text_ratio": 0.22},
                    {"time_seconds": 1_704.0, "black_ratio": 0.88, "text_ratio": 0.18},
                    {"time_seconds": 1_730.0, "black_ratio": 0.92, "text_ratio": 0.20}
                ]),
            },
            VisualDetectorCorpusCase {
                id: "visual_post_credit_split",
                duration_seconds: 2_400.0,
                expected_reason: None,
                expected_segments: vec![
                    ExpectedVisualSegment {
                        start_min: 1_799.0,
                        start_max: 1_801.0,
                        end_min: 1_889.0,
                        end_max: 1_891.0,
                    },
                    ExpectedVisualSegment {
                        start_min: 2_219.0,
                        start_max: 2_221.0,
                        end_min: 2_399.0,
                        end_max: 2_401.0,
                    },
                ],
                frames: json!([
                    {"time_seconds": 1_800.0, "black_ratio": 0.90, "text_ratio": 0.16},
                    {"time_seconds": 1_830.0, "black_ratio": 0.92, "text_ratio": 0.20},
                    {"time_seconds": 1_860.0, "black_ratio": 0.91, "text_ratio": 0.18},
                    {"time_seconds": 1_890.0, "black_ratio": 0.12, "text_ratio": 0.01},
                    {"time_seconds": 1_920.0, "black_ratio": 0.14, "text_ratio": 0.01},
                    {"time_seconds": 1_950.0, "black_ratio": 0.16, "text_ratio": 0.02},
                    {"time_seconds": 2_220.0, "black_ratio": 0.93, "text_ratio": 0.22},
                    {"time_seconds": 2_250.0, "black_ratio": 0.94, "text_ratio": 0.21},
                    {"time_seconds": 2_280.0, "black_ratio": 0.92, "text_ratio": 0.19}
                ]),
            },
            VisualDetectorCorpusCase {
                id: "visual_midroll_dark_text_negative",
                duration_seconds: 1_800.0,
                expected_segments: Vec::new(),
                expected_reason: Some("no_sustained_credits_run"),
                frames: json!([
                    {"time_seconds": 600.0, "black_ratio": 0.94, "text_ratio": 0.24},
                    {"time_seconds": 625.0, "black_ratio": 0.95, "text_ratio": 0.25},
                    {"time_seconds": 650.0, "black_ratio": 0.93, "text_ratio": 0.23},
                    {"time_seconds": 1_700.0, "black_ratio": 0.92, "text_ratio": 0.18}
                ]),
            },
            VisualDetectorCorpusCase {
                id: "visual_single_late_frame_negative",
                duration_seconds: 1_800.0,
                expected_segments: Vec::new(),
                expected_reason: Some("no_sustained_credits_run"),
                frames: json!([
                    {"time_seconds": 1_720.0, "black_ratio": 0.94, "text_ratio": 0.21}
                ]),
            },
        ]
    }

    async fn run_audio_detector_corpus_case(
        case: AudioDetectorCorpusCase,
    ) -> Result<DetectorCorpusOutcome> {
        let pool = test_pool().await?;
        let (season_id, media_file_ids) =
            seed_audio_detector_season(&pool, case.fingerprints).await?;
        let preferences = preferences_with_local_audio_detector()?;

        enqueue_local_audio_recurring_detector_job(&pool, &season_id, 10).await?;
        let run = run_next_media_segment_job(&pool, &preferences, "midm-audio-corpus")
            .await?
            .expect("queued local audio corpus job should run");
        let summary = run
            .local_audio_summary
            .expect("local audio corpus job should return detector summary");

        let mut observed_active = Vec::new();
        for media_file_id in &media_file_ids {
            observed_active.extend(list_active_segments_for_file(&pool, media_file_id).await?);
        }

        let expected_positive = case.expected_segment_type.is_some();
        let detail = if let Some(expected_type) = case.expected_segment_type {
            if observed_active.len() != media_file_ids.len() {
                format!(
                    "expected one {expected_type} segment per file, observed {} active segments across {} files",
                    observed_active.len(),
                    media_file_ids.len()
                )
            } else if observed_active
                .iter()
                .any(|segment| segment.segment_type != expected_type)
            {
                format!("expected only {expected_type} segments, observed {observed_active:?}")
            } else if observed_active
                .iter()
                .any(|segment| segment.confidence < LOCAL_AUDIO_DETECTOR_MIN_CONFIDENCE)
            {
                format!("observed low-confidence audio segment: {observed_active:?}")
            } else if summary.candidates_submitted != media_file_ids.len() {
                format!(
                    "expected {} submitted candidates, observed {}",
                    media_file_ids.len(),
                    summary.candidates_submitted
                )
            } else {
                String::new()
            }
        } else if !observed_active.is_empty() || summary.candidates_submitted != 0 {
            format!(
                "expected no audio segments/candidates, observed active={observed_active:?}, candidates={}",
                summary.candidates_submitted
            )
        } else {
            String::new()
        };

        Ok(DetectorCorpusOutcome {
            detector: "local_audio_recurring",
            case_id: case.id,
            expected_positive,
            observed_active_segments: observed_active.len(),
            observed_candidates: summary.candidates_submitted,
            passed: detail.is_empty(),
            detail,
        })
    }

    async fn run_visual_detector_corpus_case(
        case: VisualDetectorCorpusCase,
    ) -> Result<DetectorCorpusOutcome> {
        let pool = test_pool().await?;
        let (media_file_id, _) = seed_movie_file(&pool, case.duration_seconds).await?;
        upsert_video_frame_hash(
            &pool,
            &media_file_id,
            case.duration_seconds,
            json!({
                "version": "midm-synthetic-visual-corpus-v1",
                "frames": case.frames
            }),
        )
        .await?;
        let preferences = preferences_with_local_visual_detector()?;

        enqueue_local_visual_credits_detector_job(&pool, &media_file_id, 10).await?;
        let run = run_next_media_segment_job(&pool, &preferences, "midm-visual-corpus")
            .await?
            .expect("queued local visual corpus job should run");
        let summary = run
            .local_visual_summary
            .expect("local visual corpus job should return detector summary");
        let active = list_active_segments_for_file(&pool, &media_file_id).await?;
        let expected_positive = !case.expected_segments.is_empty();

        let mut detail = String::new();
        if expected_positive {
            if active.len() != case.expected_segments.len() {
                detail = format!(
                    "expected {} visual credit segments, observed {}: {active:?}",
                    case.expected_segments.len(),
                    active.len()
                );
            } else {
                for (index, (segment, expected)) in
                    active.iter().zip(case.expected_segments.iter()).enumerate()
                {
                    if segment.segment_type != "credits"
                        || segment.start_seconds < expected.start_min
                        || segment.start_seconds > expected.start_max
                        || segment.end_seconds < expected.end_min
                        || segment.end_seconds > expected.end_max
                        || segment.confidence < LOCAL_VISUAL_CREDITS_MIN_CONFIDENCE
                    {
                        detail = format!(
                            "visual segment {index} outside expected bounds/confidence: segment={segment:?}"
                        );
                        break;
                    }
                }
            }
            if detail.is_empty() && summary.candidates_submitted != case.expected_segments.len() {
                detail = format!(
                    "expected {} submitted visual candidates, observed {}",
                    case.expected_segments.len(),
                    summary.candidates_submitted
                );
            }
        } else if !active.is_empty() || summary.candidates_submitted != 0 {
            detail = format!(
                "expected no visual segments/candidates, observed active={active:?}, candidates={}",
                summary.candidates_submitted
            );
        } else if summary.reason.as_deref() != case.expected_reason {
            detail = format!(
                "expected visual reason {:?}, observed {:?}",
                case.expected_reason, summary.reason
            );
        }

        Ok(DetectorCorpusOutcome {
            detector: "local_visual_recurring",
            case_id: case.id,
            expected_positive,
            observed_active_segments: active.len(),
            observed_candidates: summary.candidates_submitted,
            passed: detail.is_empty(),
            detail,
        })
    }

    #[tokio::test]
    async fn local_visual_detector_worker_enqueues_due_frame_hash_files() -> Result<()> {
        let pool = test_pool().await?;
        let (media_file_id, _) = seed_movie_file(&pool, 1_800.0).await?;
        upsert_video_frame_hash(
            &pool,
            &media_file_id,
            1_800.0,
            json!({
                "frames": [
                    {"time_seconds": 1_660.0, "black_ratio": 0.91, "text_ratio": 0.16},
                    {"time_seconds": 1_682.0, "black_ratio": 0.93, "text_ratio": 0.22},
                    {"time_seconds": 1_704.0, "black_ratio": 0.88, "text_ratio": 0.18}
                ]
            }),
        )
        .await?;
        let preferences = preferences_with_local_visual_detector()?;

        let summary = run_media_segment_job_worker_iteration_with_preferences(
            &pool,
            &preferences,
            "midm-local-visual-enqueue-test",
            10,
            0,
            MEDIA_SEGMENT_WORKER_MAX_RUNTIME_SECONDS,
        )
        .await?;

        assert_eq!(summary.enqueue.providers_seen, 1);
        assert_eq!(summary.enqueue.files_seen, 1);
        assert_eq!(summary.enqueue.jobs_queued, 1);
        assert_eq!(summary.jobs_run, 0);

        let queued = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM media_segment_jobs
             WHERE job_type = 'local_detector'
               AND scope_type = 'media_file'
               AND provider_kind = 'local_visual_recurring'
               AND scope_id = $1
               AND status = 'queued'",
        )
        .bind(&media_file_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(queued, 1);
        Ok(())
    }

    #[tokio::test]
    async fn provider_rate_limit_blocks_second_network_acquire_in_window() -> Result<()> {
        let pool = test_pool().await?;
        let settings = json!({"rate_limit_per_minute": 1});

        assert!(acquire_provider_rate_limit(&pool, PROVIDER_THEINTRODB, Some(&settings)).await?);
        assert!(!acquire_provider_rate_limit(&pool, PROVIDER_THEINTRODB, Some(&settings)).await?);

        let requests = sqlx::query_scalar::<_, i64>(
            "SELECT requests_in_window
             FROM media_segment_provider_rate_limits
             WHERE provider_kind = 'theintrodb'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(requests, 1);
        Ok(())
    }

    #[tokio::test]
    async fn worker_iteration_runtime_budget_prevents_new_job_claims() -> Result<()> {
        let pool = test_pool().await?;
        let (media_file_id, movie_id) = seed_movie_file(&pool, 1800.0).await?;
        sqlx::query("UPDATE movies SET external_imdb = 'tt1357913' WHERE id = $1")
            .bind(&movie_id)
            .execute(&pool)
            .await?;
        let preferences = default_playback_preferences();

        let summary = run_media_segment_job_worker_iteration_with_preferences(
            &pool,
            &preferences,
            "midm-worker-budget-test",
            10,
            2,
            0,
        )
        .await?;
        assert_eq!(summary.runtime_budget_seconds, 0);
        assert!(summary.runtime_budget_exhausted);
        assert_eq!(summary.enqueue.jobs_queued, 1);
        assert_eq!(summary.jobs_run, 0);

        let queued = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM media_segment_jobs
             WHERE scope_id = $1
               AND provider_kind = 'theintrodb'
               AND status = 'queued'",
        )
        .bind(&media_file_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(queued, 1);
        Ok(())
    }

    #[tokio::test]
    async fn detector_worker_runtime_budget_prevents_local_detector_claims() -> Result<()> {
        let pool = test_pool().await?;
        let (season_id, _) = seed_audio_detector_season(
            &pool,
            vec![
                json!({"windows": [
                    {"start_seconds": 30.0, "end_seconds": 90.0, "hash": "budget-opening"}
                ]}),
                json!({"windows": [
                    {"start_seconds": 31.0, "end_seconds": 91.0, "hash": "budget-opening"}
                ]}),
            ],
        )
        .await?;
        let (visual_media_file_id, _) = seed_movie_file(&pool, 1_800.0).await?;
        upsert_video_frame_hash(
            &pool,
            &visual_media_file_id,
            1_800.0,
            json!({
                "version": "test-visual-v1",
                "frames": [
                    {"time_seconds": 1_660.0, "black_ratio": 0.91, "text_ratio": 0.16},
                    {"time_seconds": 1_682.0, "black_ratio": 0.93, "text_ratio": 0.22},
                    {"time_seconds": 1_704.0, "black_ratio": 0.88, "text_ratio": 0.18}
                ]
            }),
        )
        .await?;
        let mut preferences = default_playback_preferences();
        preferences.segment_provider_settings = merge_segment_provider_settings(
            &preferences.segment_provider_settings,
            json!({
                "theintrodb": false,
                "aniskip": false,
                "local_audio_recurring": {
                    "enabled": true,
                    "min_repeat_count": 2,
                    "min_season_files": 2
                },
                "local_visual_recurring": {
                    "enabled": true,
                    "min_frame_count": 3,
                    "min_span_seconds": 20.0,
                    "min_start_fraction": 0.60
                }
            }),
        )?;

        let summary = run_media_segment_job_worker_iteration_with_preferences(
            &pool,
            &preferences,
            "midm-detector-budget-test",
            10,
            10,
            0,
        )
        .await?;

        assert_eq!(summary.runtime_budget_seconds, 0);
        assert!(summary.runtime_budget_exhausted);
        assert_eq!(summary.jobs_run, 0);
        assert!(summary.enqueue.jobs_queued >= 2);

        let audio_queued = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM media_segment_jobs
             WHERE scope_type = 'season'
               AND scope_id = $1
               AND provider_kind = 'local_audio_recurring'
               AND status = 'queued'",
        )
        .bind(&season_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(audio_queued, 1);

        let visual_queued = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM media_segment_jobs
             WHERE scope_type = 'media_file'
               AND scope_id = $1
               AND provider_kind = 'local_visual_recurring'
               AND status = 'queued'",
        )
        .bind(&visual_media_file_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(visual_queued, 1);
        assert!(
            list_segment_candidates_for_file(&pool, &visual_media_file_id)
                .await?
                .is_empty()
        );
        assert!(
            list_active_segments_for_file(&pool, &visual_media_file_id)
                .await?
                .is_empty()
        );
        Ok(())
    }

    #[tokio::test]
    async fn worker_iteration_enqueues_due_provider_job_and_respects_fresh_cache() -> Result<()> {
        let pool = test_pool().await?;
        let (media_file_id, movie_id) = seed_movie_file(&pool, 1800.0).await?;
        sqlx::query("UPDATE movies SET external_imdb = 'tt2468101' WHERE id = $1")
            .bind(&movie_id)
            .execute(&pool)
            .await?;
        let base_url = fake_provider_base_url(Router::new().route(
            "/segments",
            get(|| async {
                Json(json!({
                    "segments": [{
                        "id": "intro-worker-1",
                        "type": "intro",
                        "start_sec": 15,
                        "end_sec": 75
                    }]
                }))
            }),
        ))
        .await?;
        let preferences = preferences_with_provider_urls(Some(&base_url), None)?;

        let summary = run_media_segment_job_worker_iteration_with_preferences(
            &pool,
            &preferences,
            "midm-worker-test",
            10,
            2,
            MEDIA_SEGMENT_WORKER_MAX_RUNTIME_SECONDS,
        )
        .await?;
        assert_eq!(summary.enqueue.providers_seen, 1);
        assert_eq!(summary.enqueue.files_seen, 1);
        assert_eq!(summary.enqueue.jobs_queued, 1);
        assert_eq!(summary.jobs_run, 1);
        assert_eq!(summary.jobs_succeeded, 1);

        let active = list_active_segments_for_file(&pool, &media_file_id).await?;
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].segment_type, "intro");

        let second = run_media_segment_job_worker_iteration_with_preferences(
            &pool,
            &preferences,
            "midm-worker-test",
            10,
            2,
            MEDIA_SEGMENT_WORKER_MAX_RUNTIME_SECONDS,
        )
        .await?;
        assert_eq!(second.enqueue.jobs_queued, 0);
        assert_eq!(second.jobs_run, 0);
        Ok(())
    }
}
