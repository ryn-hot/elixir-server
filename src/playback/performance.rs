use std::{
    fs,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use dashmap::DashSet;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{AnyPool, Row};
use tempfile::Builder as TempDirBuilder;
use tokio::{
    process::Command,
    time::{sleep, timeout},
};
use tracing::warn;
use uuid::Uuid;

use crate::{
    config::PlaybackConfig,
    media::ffprobe,
    metrics::PLAYBACK_PERFORMANCE_PROBE_DURATION,
    playback::{
        HlsOutputLayout, TranscodeParams, build_direct_stream_ffmpeg_args,
        build_transcode_ffmpeg_args,
        certification::{CaseReport, CaseStatus, CertificationReport},
        decision::{PlaybackSelection, plan_playback},
        detect_text_subtitles,
        hardware::{
            HardwareReadinessRecord, collect_host_hardware_inventory, host_hardware_fingerprint,
            load_current_hardware_readiness_records,
        },
        plan::{
            Delivery, HdrAction, PlaybackFeasibilityAction, PlaybackMode,
            PlaybackPerformanceConfidence, PlaybackPerformanceDecision,
            PlaybackPerformanceEnvelope, PlaybackPlan, PlaybackSupportDecision, StreamAction,
        },
        probe::{MediaCapabilities, normalize_ffprobe_metadata},
        probe_video_fps,
        profile::{
            AbrSupportType, ClientPlaybackProfile, EffectivePlaybackPolicy, NetworkClass,
            QualityMode, UnknownPerformancePolicy,
        },
    },
};

const LOCAL_PROBE_OUTPUT_SECONDS: f64 = 1.0;
const READINESS_PROBE_OUTPUT_SECONDS: f64 = 8.0;
const READINESS_PROBE_SOURCE_SECONDS: f64 = 10.0;
const READINESS_PROBE_POLL_INTERVAL: Duration = Duration::from_millis(100);
const REALTIME_SAFE_THRESHOLD: f64 = 1.25;
const REALTIME_MARGINAL_THRESHOLD: f64 = 1.0;
const LIVE_OBSERVATION_DECISION_MIN_SAMPLES: i64 = 3;
const LIVE_OBSERVATION_UNSAFE_FAILURES: i64 = 3;

fn current_elixir_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PlaybackHostIdentity {
    pub host_fingerprint: String,
    pub os_family: String,
    pub os_version: Option<String>,
    pub os_arch: String,
    pub gpu_vendor: Option<String>,
    pub gpu_model: Option<String>,
    pub gpu_driver_version: Option<String>,
    pub ffmpeg_path: Option<String>,
    pub ffmpeg_version: Option<String>,
    pub ffmpeg_sha256: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PlaybackAdaptiveReadinessSummary {
    pub status: String,
    pub host_fingerprint: Option<String>,
    pub adaptive_envelope_count: usize,
    pub realtime_safe_count: usize,
    pub realtime_marginal_count: usize,
    pub not_realtime_count: usize,
    pub unknown_count: usize,
    pub software_only_count: usize,
    pub last_observed_at: Option<String>,
    pub reasons: Vec<String>,
    pub warnings: Vec<String>,
    pub remediation_codes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PlaybackHdrToneMappingReadinessSummary {
    pub status: String,
    pub host_fingerprint: Option<String>,
    pub hdr_tonemap_envelope_count: usize,
    pub realtime_safe_count: usize,
    pub realtime_marginal_count: usize,
    pub not_realtime_count: usize,
    pub unknown_count: usize,
    pub software_only_count: usize,
    pub hardware_accelerated_count: usize,
    pub last_observed_at: Option<String>,
    pub reasons: Vec<String>,
    pub warnings: Vec<String>,
    pub remediation_codes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PlaybackReadinessProbeReport {
    pub host: PlaybackHostIdentity,
    pub adaptive_quality: PlaybackAdaptiveReadinessSummary,
    pub hdr_tone_mapping: PlaybackHdrToneMappingReadinessSummary,
    pub cases: Vec<PlaybackReadinessProbeCaseReport>,
    pub envelopes: Vec<PlaybackPerformanceEnvelope>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PlaybackReadinessProbeCaseReport {
    pub id: String,
    pub status: String,
    pub mode: PlaybackMode,
    pub delivery: Delivery,
    pub source_codec: String,
    pub source_width: i32,
    pub source_height: i32,
    pub adaptive_rung_count: usize,
    pub workload_class_id: Option<String>,
    pub pipeline_signature: Option<String>,
    pub support_decision: PlaybackSupportDecision,
    pub performance_decision: PlaybackPerformanceDecision,
    pub realtime_factor_millis: Option<i32>,
    pub startup_latency_ms: Option<i64>,
    pub first_segment_latency_ms: Option<i64>,
    pub reasons: Vec<String>,
    pub warnings: Vec<String>,
    pub remediation_codes: Vec<String>,
}

#[derive(Debug, Clone)]
struct ProbeHostMetadata {
    os_family: String,
    os_version: Option<String>,
    gpu_vendor: Option<String>,
    gpu_model: Option<String>,
    gpu_driver_version: Option<String>,
    hardware_api: Option<String>,
    ffmpeg_path: Option<String>,
    ffmpeg_version: Option<String>,
    ffmpeg_sha256: Option<String>,
}

impl ProbeHostMetadata {
    fn from_identity(identity: &PlaybackHostIdentity) -> Self {
        Self {
            os_family: identity.os_family.clone(),
            os_version: identity.os_version.clone(),
            gpu_vendor: identity.gpu_vendor.clone(),
            gpu_model: identity.gpu_model.clone(),
            gpu_driver_version: identity.gpu_driver_version.clone(),
            hardware_api: None,
            ffmpeg_path: identity.ffmpeg_path.clone(),
            ffmpeg_version: identity.ffmpeg_version.clone(),
            ffmpeg_sha256: identity.ffmpeg_sha256.clone(),
        }
    }

    fn from_readiness_record(record: Option<&HardwareReadinessRecord>) -> Self {
        Self {
            os_family: record
                .map(|record| record.os_family.clone())
                .unwrap_or_else(|| std::env::consts::OS.to_string()),
            os_version: record.and_then(|record| record.os_version.clone()),
            gpu_vendor: record.and_then(|record| record.gpu_vendor.clone()),
            gpu_model: record.and_then(|record| record.gpu_model.clone()),
            gpu_driver_version: record.and_then(|record| record.gpu_driver_version.clone()),
            hardware_api: record.map(|record| record.api.clone()),
            ffmpeg_path: record.and_then(|record| record.ffmpeg_path.clone()),
            ffmpeg_version: record.and_then(|record| record.ffmpeg_version.clone()),
            ffmpeg_sha256: record.and_then(|record| record.ffmpeg_sha256.clone()),
        }
    }
}

#[derive(Debug, Clone)]
struct FfmpegProbeRun {
    success: bool,
    timed_out: bool,
    elapsed: Duration,
    startup_latency_ms: Option<i64>,
    first_segment_latency_ms: Option<i64>,
    stderr: String,
}

pub async fn collect_playback_host_identity() -> PlaybackHostIdentity {
    // This intentionally performs inventory only. It must not load, update, or
    // execute the hardware readiness probe state machine.
    let inventory = collect_host_hardware_inventory().await;
    let gpu = inventory
        .gpus
        .iter()
        .find(|gpu| gpu.vendor.is_some() || gpu.model.is_some())
        .or_else(|| inventory.gpus.first());
    PlaybackHostIdentity {
        host_fingerprint: host_hardware_fingerprint(&inventory),
        os_family: inventory.os.family.clone(),
        os_version: inventory.os.version.clone(),
        os_arch: inventory.os.arch.clone(),
        gpu_vendor: gpu.and_then(|gpu| gpu.vendor.clone()),
        gpu_model: gpu.and_then(|gpu| gpu.model.clone()),
        gpu_driver_version: gpu.and_then(|gpu| gpu.driver_version.clone()),
        ffmpeg_path: inventory.ffmpeg.path.clone(),
        ffmpeg_version: inventory.ffmpeg.version.clone(),
        ffmpeg_sha256: inventory.ffmpeg.sha256.clone(),
    }
}

pub async fn load_playback_performance_envelopes(
    pool: &AnyPool,
    host_fingerprint: &str,
) -> Result<Vec<PlaybackPerformanceEnvelope>> {
    let rows = sqlx::query::<sqlx::Any>(
        "SELECT
            id,
            host_fingerprint,
            os_family,
            os_version,
            gpu_vendor,
            gpu_model,
            gpu_driver_version,
            hardware_api,
            ffmpeg_path,
            ffmpeg_version,
            ffmpeg_sha256,
            elixir_version,
            workload_class_id,
            pipeline_signature,
            support_decision,
            performance_decision,
            confidence,
            p50_realtime_factor_millis,
            p95_realtime_factor_millis,
            startup_latency_ms,
            first_segment_latency_ms,
            failure_count,
            sample_count,
            reasons_json,
            warnings_json,
            remediation_json,
            invalidation_fingerprint,
            last_observed_at
         FROM playback_performance_envelopes
         WHERE host_fingerprint = ?
           AND elixir_version = ?
         ORDER BY workload_class_id, pipeline_signature, confidence DESC, updated_at DESC",
    )
    .bind(host_fingerprint)
    .bind(current_elixir_version())
    .fetch_all(pool)
    .await
    .context("load playback performance envelopes")?;

    rows.into_iter().map(envelope_from_row).collect()
}

pub async fn upsert_playback_performance_envelope(
    pool: &AnyPool,
    envelope: &PlaybackPerformanceEnvelope,
) -> Result<String> {
    let id = if envelope.id.trim().is_empty() {
        Uuid::new_v4().to_string()
    } else {
        envelope.id.clone()
    };
    let now = Utc::now().to_rfc3339();
    let reasons_json = serde_json::to_string(&envelope.reasons).context("serialize reasons")?;
    let warnings_json = serde_json::to_string(&envelope.warnings).context("serialize warnings")?;
    let remediation_json =
        serde_json::to_string(&envelope.remediation_codes).context("serialize remediation")?;

    sqlx::query::<sqlx::Any>(
        "INSERT INTO playback_performance_envelopes (
            id,
            host_fingerprint,
            os_family,
            os_version,
            gpu_vendor,
            gpu_model,
            gpu_driver_version,
            hardware_api,
            ffmpeg_path,
            ffmpeg_version,
            ffmpeg_sha256,
            elixir_version,
            workload_class_id,
            pipeline_signature,
            support_decision,
            performance_decision,
            confidence,
            p50_realtime_factor_millis,
            p95_realtime_factor_millis,
            startup_latency_ms,
            first_segment_latency_ms,
            failure_count,
            sample_count,
            reasons_json,
            warnings_json,
            remediation_json,
            invalidation_fingerprint,
            last_observed_at,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(host_fingerprint, workload_class_id, pipeline_signature, invalidation_fingerprint)
        DO UPDATE SET
            id = excluded.id,
            os_family = excluded.os_family,
            os_version = excluded.os_version,
            gpu_vendor = excluded.gpu_vendor,
            gpu_model = excluded.gpu_model,
            gpu_driver_version = excluded.gpu_driver_version,
            hardware_api = excluded.hardware_api,
            ffmpeg_path = excluded.ffmpeg_path,
            ffmpeg_version = excluded.ffmpeg_version,
            ffmpeg_sha256 = excluded.ffmpeg_sha256,
            elixir_version = excluded.elixir_version,
            support_decision = excluded.support_decision,
            performance_decision = excluded.performance_decision,
            confidence = excluded.confidence,
            p50_realtime_factor_millis = excluded.p50_realtime_factor_millis,
            p95_realtime_factor_millis = excluded.p95_realtime_factor_millis,
            startup_latency_ms = excluded.startup_latency_ms,
            first_segment_latency_ms = excluded.first_segment_latency_ms,
            failure_count = excluded.failure_count,
            sample_count = excluded.sample_count,
            reasons_json = excluded.reasons_json,
            warnings_json = excluded.warnings_json,
            remediation_json = excluded.remediation_json,
            last_observed_at = excluded.last_observed_at,
            updated_at = excluded.updated_at",
    )
    .bind(&id)
    .bind(&envelope.host_fingerprint)
    .bind(&envelope.os_family)
    .bind(&envelope.os_version)
    .bind(&envelope.gpu_vendor)
    .bind(&envelope.gpu_model)
    .bind(&envelope.gpu_driver_version)
    .bind(&envelope.hardware_api)
    .bind(&envelope.ffmpeg_path)
    .bind(&envelope.ffmpeg_version)
    .bind(&envelope.ffmpeg_sha256)
    .bind(&envelope.elixir_version)
    .bind(&envelope.workload_class_id)
    .bind(&envelope.pipeline_signature)
    .bind(envelope.support_decision.as_str())
    .bind(envelope.performance_decision.as_str())
    .bind(envelope.confidence.as_str())
    .bind(envelope.p50_realtime_factor_millis.map(i64::from))
    .bind(envelope.p95_realtime_factor_millis.map(i64::from))
    .bind(envelope.startup_latency_ms)
    .bind(envelope.first_segment_latency_ms)
    .bind(envelope.failure_count)
    .bind(envelope.sample_count)
    .bind(reasons_json)
    .bind(warnings_json)
    .bind(remediation_json)
    .bind(&envelope.invalidation_fingerprint)
    .bind(&envelope.last_observed_at)
    .bind(&now)
    .bind(&now)
    .execute(pool)
    .await
    .context("upsert playback performance envelope")?;
    Ok(id)
}

pub async fn record_playback_performance_observation(
    pool: &AnyPool,
    envelope_id: &str,
    success: bool,
    startup_latency_ms: Option<i64>,
    first_segment_latency_ms: Option<i64>,
    realtime_factor_millis: Option<i32>,
    failure_kind: Option<&str>,
    fallback_kind: Option<&str>,
    output_mode: Option<&str>,
) -> Result<u64> {
    let Some(mut envelope) = load_playback_performance_envelope_by_id(pool, envelope_id).await?
    else {
        return Ok(0);
    };

    let now = Utc::now().to_rfc3339();
    apply_live_observation_to_envelope(
        &mut envelope,
        success,
        startup_latency_ms,
        first_segment_latency_ms,
        realtime_factor_millis,
        failure_kind,
        fallback_kind,
        output_mode,
        &now,
    );
    let reasons_json = serde_json::to_string(&envelope.reasons).context("serialize reasons")?;
    let warnings_json = serde_json::to_string(&envelope.warnings).context("serialize warnings")?;
    let remediation_json =
        serde_json::to_string(&envelope.remediation_codes).context("serialize remediation")?;
    let result = sqlx::query::<sqlx::Any>(
        "UPDATE playback_performance_envelopes
         SET support_decision = ?,
             performance_decision = ?,
             confidence = ?,
             p50_realtime_factor_millis = ?,
             p95_realtime_factor_millis = ?,
             startup_latency_ms = ?,
             first_segment_latency_ms = ?,
             failure_count = ?,
             sample_count = ?,
             reasons_json = ?,
             warnings_json = ?,
             remediation_json = ?,
             last_observed_at = ?,
             updated_at = ?
         WHERE id = ?",
    )
    .bind(envelope.support_decision.as_str())
    .bind(envelope.performance_decision.as_str())
    .bind(envelope.confidence.as_str())
    .bind(envelope.p50_realtime_factor_millis.map(i64::from))
    .bind(envelope.p95_realtime_factor_millis.map(i64::from))
    .bind(envelope.startup_latency_ms)
    .bind(envelope.first_segment_latency_ms)
    .bind(envelope.failure_count)
    .bind(envelope.sample_count)
    .bind(reasons_json)
    .bind(warnings_json)
    .bind(remediation_json)
    .bind(&now)
    .bind(&now)
    .bind(envelope_id)
    .execute(pool)
    .await
    .context("record playback performance observation")?;
    Ok(result.rows_affected())
}

pub fn summarize_adaptive_playback_readiness(
    host_fingerprint: Option<&str>,
    envelopes: &[PlaybackPerformanceEnvelope],
) -> PlaybackAdaptiveReadinessSummary {
    let adaptive_envelopes = envelopes
        .iter()
        .filter(|envelope| {
            envelope.hardware_api.is_none()
                && envelope
                    .workload_class_id
                    .starts_with("adaptive_transcode:")
        })
        .collect::<Vec<_>>();
    let realtime_safe_count = adaptive_envelopes
        .iter()
        .filter(|envelope| {
            envelope.performance_decision == PlaybackPerformanceDecision::RealtimeSafe
        })
        .count();
    let realtime_marginal_count = adaptive_envelopes
        .iter()
        .filter(|envelope| {
            envelope.performance_decision == PlaybackPerformanceDecision::RealtimeMarginal
        })
        .count();
    let not_realtime_count = adaptive_envelopes
        .iter()
        .filter(|envelope| {
            envelope.performance_decision == PlaybackPerformanceDecision::NotRealtime
        })
        .count();
    let unknown_count = adaptive_envelopes
        .iter()
        .filter(|envelope| envelope.performance_decision == PlaybackPerformanceDecision::Unknown)
        .count();
    let software_only_count = adaptive_envelopes
        .iter()
        .filter(|envelope| envelope.support_decision == PlaybackSupportDecision::SoftwareOnly)
        .count();
    let mut reasons = Vec::new();
    let mut warnings = Vec::new();
    let mut remediation_codes = Vec::new();
    for envelope in &adaptive_envelopes {
        for reason in &envelope.reasons {
            push_unique_string(&mut reasons, reason.clone());
        }
        for warning in &envelope.warnings {
            push_unique_string(&mut warnings, warning.clone());
        }
        for remediation in &envelope.remediation_codes {
            push_unique_string(&mut remediation_codes, remediation.clone());
        }
    }

    let status = if adaptive_envelopes.is_empty() {
        push_unique_string(&mut reasons, "adaptive_readiness_probe_not_run".to_string());
        push_unique_string(
            &mut remediation_codes,
            "run_playback_readiness_probe".to_string(),
        );
        "unknown"
    } else if realtime_safe_count > 0 && not_realtime_count == 0 && unknown_count == 0 {
        push_unique_string(
            &mut reasons,
            "adaptive_software_ladder_realtime_safe".to_string(),
        );
        "ready"
    } else if realtime_safe_count > 0 || realtime_marginal_count > 0 {
        push_unique_string(&mut warnings, "adaptive_readiness_limited".to_string());
        "limited"
    } else {
        push_unique_string(
            &mut reasons,
            "adaptive_software_ladder_not_ready".to_string(),
        );
        push_unique_string(
            &mut remediation_codes,
            "lower_adaptive_quality_or_use_original_quality".to_string(),
        );
        "not_ready"
    };

    let last_observed_at = adaptive_envelopes
        .iter()
        .filter_map(|envelope| envelope.last_observed_at.clone())
        .max();

    PlaybackAdaptiveReadinessSummary {
        status: status.to_string(),
        host_fingerprint: host_fingerprint.map(str::to_string),
        adaptive_envelope_count: adaptive_envelopes.len(),
        realtime_safe_count,
        realtime_marginal_count,
        not_realtime_count,
        unknown_count,
        software_only_count,
        last_observed_at,
        reasons,
        warnings,
        remediation_codes,
    }
}

pub fn summarize_hdr_tone_mapping_readiness(
    host_fingerprint: Option<&str>,
    envelopes: &[PlaybackPerformanceEnvelope],
) -> PlaybackHdrToneMappingReadinessSummary {
    let hdr_envelopes = envelopes
        .iter()
        .filter(|envelope| {
            envelope.workload_class_id.contains(":hdr_tonemap:")
                || envelope
                    .reasons
                    .iter()
                    .any(|reason| reason == "hdr_tone_mapping_required")
        })
        .collect::<Vec<_>>();
    let realtime_safe_count = hdr_envelopes
        .iter()
        .filter(|envelope| {
            envelope.performance_decision == PlaybackPerformanceDecision::RealtimeSafe
        })
        .count();
    let realtime_marginal_count = hdr_envelopes
        .iter()
        .filter(|envelope| {
            envelope.performance_decision == PlaybackPerformanceDecision::RealtimeMarginal
        })
        .count();
    let not_realtime_count = hdr_envelopes
        .iter()
        .filter(|envelope| {
            envelope.performance_decision == PlaybackPerformanceDecision::NotRealtime
        })
        .count();
    let unknown_count = hdr_envelopes
        .iter()
        .filter(|envelope| envelope.performance_decision == PlaybackPerformanceDecision::Unknown)
        .count();
    let software_only_count = hdr_envelopes
        .iter()
        .filter(|envelope| envelope.support_decision == PlaybackSupportDecision::SoftwareOnly)
        .count();
    let hardware_accelerated_count = hdr_envelopes
        .iter()
        .filter(|envelope| envelope.hardware_api.is_some())
        .count();
    let mut reasons = Vec::new();
    let mut warnings = Vec::new();
    let mut remediation_codes = Vec::new();
    for envelope in &hdr_envelopes {
        for reason in &envelope.reasons {
            push_unique_string(&mut reasons, reason.clone());
        }
        for warning in &envelope.warnings {
            push_unique_string(&mut warnings, warning.clone());
        }
        for remediation in &envelope.remediation_codes {
            push_unique_string(&mut remediation_codes, remediation.clone());
        }
    }

    let status = if hdr_envelopes.is_empty() {
        push_unique_string(
            &mut reasons,
            "hdr_tone_mapping_readiness_probe_not_run".to_string(),
        );
        push_unique_string(
            &mut remediation_codes,
            "run_playback_readiness_probe".to_string(),
        );
        "unknown"
    } else if realtime_safe_count > 0 && not_realtime_count == 0 && unknown_count == 0 {
        push_unique_string(&mut reasons, "hdr_tone_mapping_realtime_safe".to_string());
        "ready"
    } else if realtime_safe_count > 0 || realtime_marginal_count > 0 {
        push_unique_string(
            &mut warnings,
            "hdr_tone_mapping_readiness_limited".to_string(),
        );
        "limited"
    } else {
        push_unique_string(&mut reasons, "hdr_tone_mapping_not_ready".to_string());
        push_unique_string(
            &mut remediation_codes,
            "use_hdr_capable_client_or_lower_quality".to_string(),
        );
        "not_ready"
    };

    let last_observed_at = hdr_envelopes
        .iter()
        .filter_map(|envelope| envelope.last_observed_at.clone())
        .max();

    PlaybackHdrToneMappingReadinessSummary {
        status: status.to_string(),
        host_fingerprint: host_fingerprint.map(str::to_string),
        hdr_tonemap_envelope_count: hdr_envelopes.len(),
        realtime_safe_count,
        realtime_marginal_count,
        not_realtime_count,
        unknown_count,
        software_only_count,
        hardware_accelerated_count,
        last_observed_at,
        reasons,
        warnings,
        remediation_codes,
    }
}

pub async fn run_playback_readiness_probe(
    pool: &AnyPool,
    playback_config: &PlaybackConfig,
    timeout_seconds: u64,
) -> Result<PlaybackReadinessProbeReport> {
    let host = collect_playback_host_identity().await;
    let host_metadata = ProbeHostMetadata::from_identity(&host);
    let ffmpeg_program = host
        .ffmpeg_path
        .as_deref()
        .filter(|path| !path.trim().is_empty())
        .unwrap_or("ffmpeg");
    let timeout_duration = Duration::from_secs(timeout_seconds.max(5));
    let temp_dir = TempDirBuilder::new()
        .prefix("elixir-playback-readiness-")
        .tempdir()
        .context("create playback readiness probe temp dir")?;

    let h264_source = temp_dir.path().join("source-h264-1080p.mkv");
    generate_probe_source(
        ffmpeg_program,
        &h264_source,
        ProbeSourceCodec::H264,
        1920,
        1080,
        8_000_000,
        timeout_duration,
    )
    .await?;
    let h264_media = probe_generated_source(&h264_source).await?;

    let mut cases = Vec::new();
    let mut envelopes = Vec::new();

    let direct_stream_plan = direct_stream_probe_plan("probe-direct-stream-h264", &h264_media);
    run_readiness_case(
        pool,
        &host,
        &host_metadata,
        ffmpeg_program,
        &h264_source,
        direct_stream_plan,
        timeout_duration,
        &mut cases,
        &mut envelopes,
    )
    .await?;

    let video_transcode_plan = video_transcode_probe_plan(
        "probe-video-transcode-h264-720p",
        &h264_media,
        playback_config,
    );
    run_readiness_case(
        pool,
        &host,
        &host_metadata,
        ffmpeg_program,
        &h264_source,
        video_transcode_plan,
        timeout_duration,
        &mut cases,
        &mut envelopes,
    )
    .await?;

    let adaptive_plan =
        adaptive_readiness_probe_plan("probe-adaptive-h264-1080p", &h264_media, playback_config)?;
    run_readiness_case(
        pool,
        &host,
        &host_metadata,
        ffmpeg_program,
        &h264_source,
        adaptive_plan,
        timeout_duration,
        &mut cases,
        &mut envelopes,
    )
    .await?;

    let hevc_source = temp_dir.path().join("source-hevc-1080p.mkv");
    match generate_probe_source(
        ffmpeg_program,
        &hevc_source,
        ProbeSourceCodec::Hevc,
        1920,
        1080,
        8_000_000,
        timeout_duration,
    )
    .await
    {
        Ok(()) => {
            let hevc_media = probe_generated_source(&hevc_source).await?;
            let hevc_adaptive_plan = adaptive_readiness_probe_plan(
                "probe-adaptive-hevc-1080p",
                &hevc_media,
                playback_config,
            )?;
            run_readiness_case(
                pool,
                &host,
                &host_metadata,
                ffmpeg_program,
                &hevc_source,
                hevc_adaptive_plan,
                timeout_duration,
                &mut cases,
                &mut envelopes,
            )
            .await?;
        }
        Err(err) => {
            cases.push(PlaybackReadinessProbeCaseReport {
                id: "probe-adaptive-hevc-1080p".to_string(),
                status: "skipped".to_string(),
                mode: PlaybackMode::AdaptiveTranscode,
                delivery: Delivery::HlsAdaptiveFmp4,
                source_codec: "hevc".to_string(),
                source_width: 1920,
                source_height: 1080,
                adaptive_rung_count: 0,
                workload_class_id: None,
                pipeline_signature: None,
                support_decision: PlaybackSupportDecision::Unknown,
                performance_decision: PlaybackPerformanceDecision::Unknown,
                realtime_factor_millis: None,
                startup_latency_ms: None,
                first_segment_latency_ms: None,
                reasons: vec!["hevc_probe_source_generation_skipped".to_string()],
                warnings: vec![tail(&err.to_string())],
                remediation_codes: vec!["hevc_source_probe_unavailable".to_string()],
            });
        }
    }

    let hdr_source = temp_dir.path().join("source-hevc-hdr10-1080p.mkv");
    match generate_probe_source(
        ffmpeg_program,
        &hdr_source,
        ProbeSourceCodec::HevcHdr10,
        1920,
        1080,
        12_000_000,
        timeout_duration,
    )
    .await
    {
        Ok(()) => {
            let hdr_media = probe_generated_source(&hdr_source).await?;
            if hdr_media.primary_video().is_some_and(|video| video.hdr10) {
                let hdr_plan = hdr_tone_mapping_readiness_probe_plan(
                    "probe-hdr10-to-sdr-1080p",
                    &hdr_media,
                    playback_config,
                )?;
                run_readiness_case(
                    pool,
                    &host,
                    &host_metadata,
                    ffmpeg_program,
                    &hdr_source,
                    hdr_plan,
                    timeout_duration,
                    &mut cases,
                    &mut envelopes,
                )
                .await?;
            } else {
                cases.push(PlaybackReadinessProbeCaseReport {
                    id: "probe-hdr10-to-sdr-1080p".to_string(),
                    status: "skipped".to_string(),
                    mode: PlaybackMode::VideoTranscode,
                    delivery: Delivery::HlsFmp4,
                    source_codec: "hevc".to_string(),
                    source_width: 1920,
                    source_height: 1080,
                    adaptive_rung_count: 0,
                    workload_class_id: None,
                    pipeline_signature: None,
                    support_decision: PlaybackSupportDecision::Unknown,
                    performance_decision: PlaybackPerformanceDecision::Unknown,
                    realtime_factor_millis: None,
                    startup_latency_ms: None,
                    first_segment_latency_ms: None,
                    reasons: vec!["hdr10_probe_source_metadata_missing".to_string()],
                    warnings: Vec::new(),
                    remediation_codes: vec![
                        "ffmpeg_hdr10_metadata_generation_unavailable".to_string(),
                    ],
                });
            }
        }
        Err(err) => {
            cases.push(PlaybackReadinessProbeCaseReport {
                id: "probe-hdr10-to-sdr-1080p".to_string(),
                status: "skipped".to_string(),
                mode: PlaybackMode::VideoTranscode,
                delivery: Delivery::HlsFmp4,
                source_codec: "hevc".to_string(),
                source_width: 1920,
                source_height: 1080,
                adaptive_rung_count: 0,
                workload_class_id: None,
                pipeline_signature: None,
                support_decision: PlaybackSupportDecision::Unknown,
                performance_decision: PlaybackPerformanceDecision::Unknown,
                realtime_factor_millis: None,
                startup_latency_ms: None,
                first_segment_latency_ms: None,
                reasons: vec!["hdr10_probe_source_generation_skipped".to_string()],
                warnings: vec![tail(&err.to_string())],
                remediation_codes: vec!["hdr10_source_probe_unavailable".to_string()],
            });
        }
    }

    let loaded = load_playback_performance_envelopes(pool, &host.host_fingerprint).await?;
    let adaptive_quality =
        summarize_adaptive_playback_readiness(Some(&host.host_fingerprint), &loaded);
    let hdr_tone_mapping =
        summarize_hdr_tone_mapping_readiness(Some(&host.host_fingerprint), &loaded);
    Ok(PlaybackReadinessProbeReport {
        host,
        adaptive_quality,
        hdr_tone_mapping,
        cases,
        envelopes,
    })
}

async fn run_readiness_case(
    pool: &AnyPool,
    host: &PlaybackHostIdentity,
    host_metadata: &ProbeHostMetadata,
    ffmpeg_program: &str,
    input_path: &Path,
    mut plan: PlaybackPlan,
    timeout_duration: Duration,
    cases: &mut Vec<PlaybackReadinessProbeCaseReport>,
    envelopes: &mut Vec<PlaybackPerformanceEnvelope>,
) -> Result<()> {
    let workload = plan
        .workload_class
        .clone()
        .context("readiness probe plan missing workload class")?;
    let output_dir = TempDirBuilder::new()
        .prefix("elixir-playback-readiness-case-")
        .tempdir()
        .context("create playback readiness case temp dir")?;
    let layout = HlsOutputLayout::for_job(output_dir.path(), plan.mode, plan.delivery);
    let params = TranscodeParams {
        seek_seconds: 0.0,
        mode: plan.mode,
        delivery: plan.delivery,
    };
    let args = if plan.mode == PlaybackMode::DirectStream {
        let mut args = build_direct_stream_ffmpeg_args(
            &input_path.to_string_lossy(),
            &params,
            Some(&plan),
            &layout,
        );
        insert_output_duration_limit(&mut args, READINESS_PROBE_OUTPUT_SECONDS);
        args
    } else {
        let mut args = build_transcode_ffmpeg_args(
            &input_path.to_string_lossy(),
            &params,
            Some(&plan),
            &layout,
            output_dir.path(),
            &[],
            24.0,
        );
        insert_output_duration_limit(&mut args, READINESS_PROBE_OUTPUT_SECONDS);
        args
    };
    let run = run_ffmpeg_probe_command(
        ffmpeg_program,
        &args,
        output_dir.path(),
        &layout,
        &plan,
        timeout_duration,
    )
    .await?;
    let validation_failures = hls_probe_output_failures(output_dir.path(), &layout, &plan);
    let success = run.success && !run.timed_out && validation_failures.is_empty();
    let elapsed = run.elapsed.as_secs_f64().max(0.001);
    let realtime_factor = success.then_some(READINESS_PROBE_OUTPUT_SECONDS / elapsed);
    let performance_decision = if run.timed_out {
        PlaybackPerformanceDecision::NotRealtime
    } else if success {
        performance_decision_for_realtime_factor(realtime_factor.unwrap_or_default())
    } else {
        PlaybackPerformanceDecision::Unknown
    };
    let support_decision = if success {
        support_decision_for_probe_plan(&plan)
    } else if error_indicates_unsupported_hardware(&run.stderr) {
        PlaybackSupportDecision::Unsupported
    } else if !validation_failures.is_empty() {
        PlaybackSupportDecision::Unsupported
    } else {
        support_decision_for_probe_plan(&plan)
    };
    let mut reasons = vec![
        "playback_readiness_probe".to_string(),
        format!("readiness_case:{}", plan.media_file_id),
    ];
    for reason in &plan.reasons {
        push_unique_string(&mut reasons, reason.clone());
    }
    let mut warnings = Vec::new();
    if run.timed_out {
        push_unique_string(&mut reasons, "readiness_probe_timeout".to_string());
    }
    if !run.success {
        push_unique_string(&mut reasons, "readiness_probe_ffmpeg_failed".to_string());
        push_unique_string(&mut warnings, tail(&run.stderr));
    }
    for failure in validation_failures {
        push_unique_string(&mut reasons, failure);
    }
    if performance_decision == PlaybackPerformanceDecision::NotRealtime {
        push_unique_string(
            &mut reasons,
            "readiness_probe_realtime_factor_below_required".to_string(),
        );
    }
    let envelope = envelope_from_probe_result(
        &host.host_fingerprint,
        host_metadata,
        &plan,
        support_decision,
        performance_decision,
        realtime_factor,
        if success { 0 } else { 1 },
        reasons,
        warnings,
        run.startup_latency_ms,
        run.first_segment_latency_ms,
    );
    upsert_playback_performance_envelope(pool, &envelope).await?;
    let report = PlaybackReadinessProbeCaseReport {
        id: plan.media_file_id.clone(),
        status: if success { "passed" } else { "failed" }.to_string(),
        mode: plan.mode,
        delivery: plan.delivery,
        source_codec: workload
            .source_video_codec
            .clone()
            .unwrap_or_else(|| "unknown".to_string()),
        source_width: workload.source_width.unwrap_or_default(),
        source_height: workload.source_height.unwrap_or_default(),
        adaptive_rung_count: plan
            .adaptive_ladder
            .as_ref()
            .map(|ladder| ladder.rungs.len())
            .unwrap_or_default(),
        workload_class_id: Some(workload.class_id),
        pipeline_signature: Some(workload.pipeline_signature),
        support_decision: envelope.support_decision,
        performance_decision: envelope.performance_decision,
        realtime_factor_millis: envelope.p50_realtime_factor_millis,
        startup_latency_ms: envelope.startup_latency_ms,
        first_segment_latency_ms: envelope.first_segment_latency_ms,
        reasons: envelope.reasons.clone(),
        warnings: envelope.warnings.clone(),
        remediation_codes: envelope.remediation_codes.clone(),
    };
    envelopes.push(envelope);
    cases.push(report);
    plan.feasibility = None;
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum ProbeSourceCodec {
    H264,
    Hevc,
    HevcHdr10,
}

impl ProbeSourceCodec {
    fn encoder(self) -> &'static str {
        match self {
            Self::H264 => "libx264",
            Self::Hevc | Self::HevcHdr10 => "libx265",
        }
    }

    fn codec_name(self) -> &'static str {
        match self {
            Self::H264 => "h264",
            Self::Hevc => "hevc",
            Self::HevcHdr10 => "hevc_hdr10",
        }
    }

    fn is_hevc(self) -> bool {
        matches!(self, Self::Hevc | Self::HevcHdr10)
    }

    fn is_hdr10(self) -> bool {
        matches!(self, Self::HevcHdr10)
    }
}

async fn generate_probe_source(
    ffmpeg_program: &str,
    output_path: &Path,
    codec: ProbeSourceCodec,
    width: i32,
    height: i32,
    bitrate_bps: i64,
    timeout_duration: Duration,
) -> Result<()> {
    let bitrate = format!("{}k", bitrate_bps.saturating_div(1000).max(1));
    let duration = format!("{READINESS_PROBE_SOURCE_SECONDS}");
    let size = format!("{}x{}", width.max(2), height.max(2));
    let mut args = vec![
        "-y".to_string(),
        "-hide_banner".to_string(),
        "-nostdin".to_string(),
        "-loglevel".to_string(),
        "error".to_string(),
        "-f".to_string(),
        "lavfi".to_string(),
        "-i".to_string(),
        format!("testsrc2=size={size}:rate=24"),
        "-f".to_string(),
        "lavfi".to_string(),
        "-i".to_string(),
        "sine=frequency=1000:sample_rate=48000".to_string(),
        "-t".to_string(),
        duration,
        "-map".to_string(),
        "0:v:0".to_string(),
        "-map".to_string(),
        "1:a:0".to_string(),
        "-c:v".to_string(),
        codec.encoder().to_string(),
        "-preset".to_string(),
        "ultrafast".to_string(),
        "-b:v".to_string(),
        bitrate,
        "-pix_fmt".to_string(),
        if codec.is_hdr10() {
            "yuv420p10le".to_string()
        } else {
            "yuv420p".to_string()
        },
        "-c:a".to_string(),
        "aac".to_string(),
        "-b:a".to_string(),
        "128k".to_string(),
    ];
    if codec.is_hdr10() {
        args.extend(
            [
                "-color_primaries".to_string(),
                "bt2020".to_string(),
                "-color_trc".to_string(),
                "smpte2084".to_string(),
                "-colorspace".to_string(),
                "bt2020nc".to_string(),
            ]
            .into_iter(),
        );
    }
    if codec.is_hevc() {
        args.push("-x265-params".to_string());
        args.push(if codec.is_hdr10() {
            "log-level=error:repeat-headers=1:hdr-opt=1:colorprim=bt2020:transfer=smpte2084:colormatrix=bt2020nc".to_string()
        } else {
            "log-level=error".to_string()
        });
    }
    args.push(output_path.to_string_lossy().to_string());

    let output = timeout(
        timeout_duration,
        Command::new(ffmpeg_program)
            .args(&args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output(),
    )
    .await
    .with_context(|| {
        format!(
            "generate {} playback readiness source timed out",
            codec.codec_name()
        )
    })?
    .with_context(|| {
        format!(
            "spawn ffmpeg for {} playback readiness source",
            codec.codec_name()
        )
    })?;
    if !output.status.success() || !output_path.exists() {
        bail!(
            "generate {} playback readiness source failed: {}",
            codec.codec_name(),
            tail(&String::from_utf8_lossy(&output.stderr))
        );
    }
    Ok(())
}

async fn probe_generated_source(path: &Path) -> Result<MediaCapabilities> {
    let path = path.to_string_lossy().to_string();
    let metadata = ffprobe::probe(&path)
        .await
        .with_context(|| format!("probe generated playback readiness source {path}"))?;
    Ok(normalize_ffprobe_metadata(
        &metadata,
        ffprobe::ffprobe_version().await.ok(),
        Some(path),
    ))
}

fn direct_stream_probe_plan(media_file_id: &str, media: &MediaCapabilities) -> PlaybackPlan {
    let mut client = ClientPlaybackProfile::browser_like();
    client.quality_mode = QualityMode::Original;
    client.max_bitrate_bps = Some(50_000_000);
    client.max_resolution = Some("1080p".to_string());
    let policy = EffectivePlaybackPolicy {
        allow_direct_play: false,
        allow_direct_stream: true,
        allow_audio_transcode: true,
        allow_video_transcode: true,
        allow_adaptive_transcode: false,
        network_class: NetworkClass::Lan,
        max_bitrate_bps: Some(50_000_000),
        max_resolution: Some("1080p".to_string()),
        unknown_performance_policy: UnknownPerformancePolicy::AllowBestEffort,
        ..EffectivePlaybackPolicy::default()
    };
    plan_playback(
        media_file_id,
        media,
        PlaybackSelection::default(),
        &client,
        &policy,
    )
}

fn video_transcode_probe_plan(
    media_file_id: &str,
    media: &MediaCapabilities,
    playback_config: &PlaybackConfig,
) -> PlaybackPlan {
    let mut client = ClientPlaybackProfile::browser_like();
    client.quality_mode = QualityMode::Fixed;
    client.fixed_resolution = Some("720p".to_string());
    client.fixed_bitrate_bps = Some(4_000_000);
    client.max_resolution = Some("720p".to_string());
    client.max_bitrate_bps = Some(50_000_000);
    let policy = EffectivePlaybackPolicy {
        allow_direct_play: false,
        allow_direct_stream: true,
        allow_audio_transcode: true,
        allow_video_transcode: true,
        allow_adaptive_transcode: false,
        network_class: NetworkClass::Lan,
        max_bitrate_bps: Some(50_000_000),
        max_resolution: Some("720p".to_string()),
        fixed_resolution: Some("720p".to_string()),
        fixed_bitrate_bps: Some(4_000_000),
        video_encoder_preset: playback_config.video_encoder_preset.clone(),
        video_encoder_profile: playback_config.video_encoder_profile.clone(),
        video_encoder_level: playback_config.video_encoder_level.clone(),
        video_encoder_crf: playback_config.video_encoder_crf,
        video_encoder_bufsize_multiplier: playback_config.video_encoder_bufsize_multiplier,
        hardware_acceleration: "off".to_string(),
        allow_hardware_decode: false,
        allow_hardware_encode: false,
        unknown_performance_policy: UnknownPerformancePolicy::AllowBestEffort,
        ..EffectivePlaybackPolicy::default()
    };
    plan_playback(
        media_file_id,
        media,
        PlaybackSelection::default(),
        &client,
        &policy,
    )
}

fn hdr_tone_mapping_readiness_probe_plan(
    media_file_id: &str,
    media: &MediaCapabilities,
    playback_config: &PlaybackConfig,
) -> Result<PlaybackPlan> {
    let mut client = ClientPlaybackProfile::browser_like();
    client.quality_mode = QualityMode::Fixed;
    client.fixed_resolution = Some("720p".to_string());
    client.fixed_bitrate_bps = Some(4_000_000);
    client.max_resolution = Some("720p".to_string());
    client.max_bitrate_bps = Some(50_000_000);
    client.supports_hdr = false;
    client.supports_hdr10_plus = false;
    client.supports_dolby_vision = false;
    let policy = EffectivePlaybackPolicy {
        allow_direct_play: true,
        allow_direct_stream: true,
        allow_audio_transcode: true,
        allow_video_transcode: true,
        allow_adaptive_transcode: false,
        network_class: NetworkClass::Lan,
        max_bitrate_bps: Some(50_000_000),
        max_resolution: Some("720p".to_string()),
        fixed_resolution: Some("720p".to_string()),
        fixed_bitrate_bps: Some(4_000_000),
        video_encoder_preset: playback_config.video_encoder_preset.clone(),
        video_encoder_profile: playback_config.video_encoder_profile.clone(),
        video_encoder_level: playback_config.video_encoder_level.clone(),
        video_encoder_crf: playback_config.video_encoder_crf,
        video_encoder_bufsize_multiplier: playback_config.video_encoder_bufsize_multiplier,
        hardware_acceleration: "off".to_string(),
        allow_hardware_decode: false,
        allow_hardware_encode: false,
        unknown_performance_policy: UnknownPerformancePolicy::AllowBestEffort,
        ..EffectivePlaybackPolicy::default()
    };
    let plan = plan_playback(
        media_file_id,
        media,
        PlaybackSelection::default(),
        &client,
        &policy,
    );
    if plan.mode != PlaybackMode::VideoTranscode
        || plan.hdr_action != HdrAction::ToneMapToSdr
        || !plan
            .video_output
            .as_ref()
            .is_some_and(|output| output.tone_map.is_some())
    {
        bail!(
            "HDR tone-map readiness probe did not produce HDR-to-SDR transcode plan: {:?}",
            plan.reasons
        );
    }
    Ok(plan)
}

fn adaptive_readiness_probe_plan(
    media_file_id: &str,
    media: &MediaCapabilities,
    playback_config: &PlaybackConfig,
) -> Result<PlaybackPlan> {
    let mut client = ClientPlaybackProfile::browser_like();
    client.quality_mode = QualityMode::Automatic;
    client.abr_support_type = AbrSupportType::HlsJs;
    client.max_resolution = Some("1080p".to_string());
    client.max_bitrate_bps = Some(50_000_000);
    client.automatic_min_resolution = Some("360p".to_string());
    client.automatic_max_resolution = Some("1080p".to_string());
    client.automatic_min_bitrate_bps = Some(800_000);
    client.automatic_max_bitrate_bps = Some(8_000_000);
    let policy = EffectivePlaybackPolicy {
        allow_direct_play: true,
        allow_direct_stream: true,
        allow_audio_transcode: true,
        allow_video_transcode: true,
        allow_adaptive_transcode: true,
        network_class: NetworkClass::Lan,
        max_bitrate_bps: Some(50_000_000),
        max_resolution: Some("1080p".to_string()),
        automatic_min_bitrate_bps: Some(800_000),
        automatic_max_bitrate_bps: Some(8_000_000),
        automatic_min_resolution: Some("360p".to_string()),
        automatic_max_resolution: Some("1080p".to_string()),
        abr_support_type: AbrSupportType::HlsJs,
        video_encoder_preset: playback_config.video_encoder_preset.clone(),
        video_encoder_profile: playback_config.video_encoder_profile.clone(),
        video_encoder_level: playback_config.video_encoder_level.clone(),
        video_encoder_crf: playback_config.video_encoder_crf,
        video_encoder_bufsize_multiplier: playback_config.video_encoder_bufsize_multiplier,
        hardware_acceleration: "off".to_string(),
        allow_hardware_decode: false,
        allow_hardware_encode: false,
        unknown_performance_policy: UnknownPerformancePolicy::AllowBestEffort,
        ..EffectivePlaybackPolicy::default()
    };
    let plan = plan_playback(
        media_file_id,
        media,
        PlaybackSelection::default(),
        &client,
        &policy,
    );
    if plan.mode != PlaybackMode::AdaptiveTranscode || plan.adaptive_ladder.is_none() {
        bail!(
            "adaptive readiness probe did not produce adaptive transcode plan: {:?}",
            plan.reasons
        );
    }
    Ok(plan)
}

async fn run_ffmpeg_probe_command(
    ffmpeg_program: &str,
    args: &[String],
    output_dir: &Path,
    layout: &HlsOutputLayout,
    plan: &PlaybackPlan,
    timeout_duration: Duration,
) -> Result<FfmpegProbeRun> {
    let started = Instant::now();
    let mut child = Command::new(ffmpeg_program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .context("spawn playback readiness ffmpeg probe")?;
    let mut startup_latency_ms = None;
    let mut first_segment_latency_ms = None;
    let mut timed_out = false;

    loop {
        if startup_latency_ms.is_none() && layout.master_playlist_path.exists() {
            startup_latency_ms = Some(started.elapsed().as_millis() as i64);
        }
        if first_segment_latency_ms.is_none()
            && hls_first_segment_ready(output_dir, plan, plan.delivery)
        {
            first_segment_latency_ms = Some(started.elapsed().as_millis() as i64);
        }
        if child
            .try_wait()
            .context("poll playback readiness ffmpeg probe")?
            .is_some()
        {
            break;
        }
        if started.elapsed() >= timeout_duration {
            timed_out = true;
            let _ = child.kill().await;
            break;
        }
        sleep(READINESS_PROBE_POLL_INTERVAL).await;
    }

    let output = child
        .wait_with_output()
        .await
        .context("wait for playback readiness ffmpeg probe")?;
    Ok(FfmpegProbeRun {
        success: output.status.success(),
        timed_out,
        elapsed: started.elapsed(),
        startup_latency_ms,
        first_segment_latency_ms,
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
    })
}

fn hls_first_segment_ready(output_dir: &Path, plan: &PlaybackPlan, delivery: Delivery) -> bool {
    if plan.mode == PlaybackMode::AdaptiveTranscode {
        let Some(ladder) = plan.adaptive_ladder.as_ref() else {
            return false;
        };
        return ladder.rungs.iter().enumerate().all(|(idx, _)| {
            output_dir.join(format!("stream_{idx}.m3u8")).exists()
                && hls_segment_exists(output_dir, &format!("seg_{idx}_"), delivery)
        });
    }
    hls_segment_exists(
        output_dir,
        if plan.mode == PlaybackMode::DirectStream {
            "segment_"
        } else {
            "seg_0_"
        },
        delivery,
    )
}

fn hls_segment_exists(output_dir: &Path, prefix: &str, delivery: Delivery) -> bool {
    let suffix = match delivery {
        Delivery::HlsFmp4 | Delivery::HlsAdaptiveFmp4 => ".m4s",
        _ => ".ts",
    };
    match fs::read_dir(output_dir) {
        Ok(entries) => entries.filter_map(|entry| entry.ok()).any(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with(prefix) && name.ends_with(suffix))
        }),
        Err(_) => false,
    }
}

fn hls_probe_output_failures(
    output_dir: &Path,
    layout: &HlsOutputLayout,
    plan: &PlaybackPlan,
) -> Vec<String> {
    let mut failures = Vec::new();
    if !layout.master_playlist_path.exists() {
        failures.push("hls_master_playlist_missing".to_string());
        return failures;
    }
    if plan.mode == PlaybackMode::AdaptiveTranscode {
        let master = fs::read_to_string(&layout.master_playlist_path).unwrap_or_default();
        for token in [
            "#EXT-X-STREAM-INF",
            "BANDWIDTH=",
            "AVERAGE-BANDWIDTH=",
            "RESOLUTION=",
            "CODECS=",
        ] {
            if !master.contains(token) {
                failures.push(format!(
                    "adaptive_master_playlist_missing_{}",
                    token
                        .trim_matches(['#', '='])
                        .replace('-', "_")
                        .to_ascii_lowercase()
                ));
            }
        }
        if plan
            .adaptive_ladder
            .as_ref()
            .is_some_and(|ladder| ladder.rungs.iter().any(|rung| rung.frame_rate.is_some()))
            && !master.contains("FRAME-RATE=")
        {
            failures.push("adaptive_master_playlist_missing_frame_rate".to_string());
        }
        let Some(ladder) = plan.adaptive_ladder.as_ref() else {
            failures.push("adaptive_ladder_missing_from_probe_plan".to_string());
            return failures;
        };
        for (idx, _) in ladder.rungs.iter().enumerate() {
            if !output_dir.join(format!("stream_{idx}.m3u8")).exists() {
                failures.push(format!("adaptive_rung_playlist_missing:{idx}"));
            }
            if matches!(plan.delivery, Delivery::HlsAdaptiveFmp4)
                && !output_dir.join(format!("init_{idx}.mp4")).exists()
            {
                failures.push(format!("adaptive_rung_init_segment_missing:{idx}"));
            }
            if !hls_segment_exists(output_dir, &format!("seg_{idx}_"), plan.delivery) {
                failures.push(format!("adaptive_rung_media_segment_missing:{idx}"));
            }
        }
    } else if plan.mode == PlaybackMode::DirectStream {
        if !output_dir.join("media.m3u8").exists() {
            failures.push("hls_media_playlist_missing".to_string());
        }
        if !hls_segment_exists(output_dir, "segment_", plan.delivery) {
            failures.push("hls_media_segment_missing".to_string());
        }
    } else {
        if !output_dir.join("stream_0.m3u8").exists() {
            failures.push("hls_media_playlist_missing".to_string());
        }
        if !hls_segment_exists(output_dir, "seg_0_", plan.delivery) {
            failures.push("hls_media_segment_missing".to_string());
        }
    }
    failures
}

async fn load_playback_performance_envelope_by_id(
    pool: &AnyPool,
    envelope_id: &str,
) -> Result<Option<PlaybackPerformanceEnvelope>> {
    let row = sqlx::query::<sqlx::Any>(
        "SELECT
            id,
            host_fingerprint,
            os_family,
            os_version,
            gpu_vendor,
            gpu_model,
            gpu_driver_version,
            hardware_api,
            ffmpeg_path,
            ffmpeg_version,
            ffmpeg_sha256,
            elixir_version,
            workload_class_id,
            pipeline_signature,
            support_decision,
            performance_decision,
            confidence,
            p50_realtime_factor_millis,
            p95_realtime_factor_millis,
            startup_latency_ms,
            first_segment_latency_ms,
            failure_count,
            sample_count,
            reasons_json,
            warnings_json,
            remediation_json,
            invalidation_fingerprint,
            last_observed_at
         FROM playback_performance_envelopes
         WHERE id = ?",
    )
    .bind(envelope_id)
    .fetch_optional(pool)
    .await
    .context("load playback performance envelope by id")?;
    row.map(envelope_from_row).transpose()
}

fn apply_live_observation_to_envelope(
    envelope: &mut PlaybackPerformanceEnvelope,
    success: bool,
    startup_latency_ms: Option<i64>,
    first_segment_latency_ms: Option<i64>,
    realtime_factor_millis: Option<i32>,
    failure_kind: Option<&str>,
    fallback_kind: Option<&str>,
    output_mode: Option<&str>,
    observed_at: &str,
) {
    let previous_sample_count = envelope.sample_count.max(0);
    envelope.sample_count = envelope.sample_count.saturating_add(1);
    if !success {
        envelope.failure_count = envelope.failure_count.saturating_add(1);
    }
    if let Some(value) = startup_latency_ms {
        envelope.startup_latency_ms = Some(value);
    }
    if let Some(value) = first_segment_latency_ms {
        envelope.first_segment_latency_ms = Some(value);
    }
    if envelope.confidence == PlaybackPerformanceConfidence::Unknown
        || envelope.confidence == PlaybackPerformanceConfidence::StaticInferred
    {
        envelope.confidence = PlaybackPerformanceConfidence::LiveObserved;
    }
    envelope.last_observed_at = Some(observed_at.to_string());

    push_unique_string(
        &mut envelope.reasons,
        "live_playback_observation".to_string(),
    );
    if success {
        push_unique_string(&mut envelope.reasons, "live_playback_success".to_string());
    } else {
        push_unique_string(&mut envelope.warnings, "live_playback_failure".to_string());
        if let Some(kind) = non_empty_compact_token(failure_kind) {
            push_unique_string(&mut envelope.reasons, format!("live_failure_kind:{kind}"));
        }
    }
    if let Some(kind) = non_empty_compact_token(fallback_kind) {
        push_unique_string(&mut envelope.warnings, format!("live_fallback_path:{kind}"));
    }
    if let Some(mode) = non_empty_compact_token(output_mode) {
        push_unique_string(&mut envelope.reasons, format!("live_output_mode:{mode}"));
    }

    if envelope.support_decision == PlaybackSupportDecision::Unsupported {
        return;
    }

    if let Some(observed_realtime) = realtime_factor_millis.filter(|value| *value > 0) {
        envelope.p50_realtime_factor_millis = Some(rolling_realtime_factor_millis(
            envelope.p50_realtime_factor_millis,
            observed_realtime,
            previous_sample_count,
        ));
        envelope.p95_realtime_factor_millis = Some(conservative_realtime_floor_millis(
            envelope.p95_realtime_factor_millis,
            observed_realtime,
        ));
        if observed_realtime < realtime_factor_to_millis(REALTIME_MARGINAL_THRESHOLD) {
            push_unique_string(
                &mut envelope.warnings,
                "live_realtime_factor_below_required".to_string(),
            );
        } else if observed_realtime < realtime_factor_to_millis(REALTIME_SAFE_THRESHOLD) {
            push_unique_string(
                &mut envelope.warnings,
                "live_realtime_factor_marginal".to_string(),
            );
        }
    }

    let Some(p50_realtime) = envelope.p50_realtime_factor_millis else {
        maybe_downgrade_for_repeated_failures(envelope);
        return;
    };
    if envelope.sample_count < LIVE_OBSERVATION_DECISION_MIN_SAMPLES {
        maybe_downgrade_for_repeated_failures(envelope);
        return;
    }

    if envelope.failure_count >= LIVE_OBSERVATION_UNSAFE_FAILURES
        || p50_realtime < realtime_factor_to_millis(REALTIME_MARGINAL_THRESHOLD)
    {
        envelope.performance_decision = PlaybackPerformanceDecision::NotRealtime;
        push_unique_string(
            &mut envelope.reasons,
            "live_performance_below_realtime_threshold".to_string(),
        );
        push_unique_string(
            &mut envelope.remediation_codes,
            "use_original_quality_or_lower_quality".to_string(),
        );
    } else if p50_realtime < realtime_factor_to_millis(REALTIME_SAFE_THRESHOLD)
        && envelope.performance_decision == PlaybackPerformanceDecision::RealtimeSafe
    {
        envelope.performance_decision = PlaybackPerformanceDecision::RealtimeMarginal;
        push_unique_string(
            &mut envelope.warnings,
            "live_performance_realtime_marginal".to_string(),
        );
    } else if p50_realtime >= realtime_factor_to_millis(REALTIME_SAFE_THRESHOLD)
        && envelope.failure_count == 0
        && matches!(
            envelope.performance_decision,
            PlaybackPerformanceDecision::Unknown | PlaybackPerformanceDecision::RealtimeMarginal
        )
    {
        envelope.performance_decision = PlaybackPerformanceDecision::RealtimeSafe;
        push_unique_string(
            &mut envelope.reasons,
            "live_performance_realtime_safe".to_string(),
        );
    }
}

fn maybe_downgrade_for_repeated_failures(envelope: &mut PlaybackPerformanceEnvelope) {
    if envelope.support_decision != PlaybackSupportDecision::Unsupported
        && envelope.failure_count >= LIVE_OBSERVATION_UNSAFE_FAILURES
    {
        envelope.performance_decision = PlaybackPerformanceDecision::NotRealtime;
        push_unique_string(
            &mut envelope.reasons,
            "live_playback_failures_exceeded_threshold".to_string(),
        );
        push_unique_string(
            &mut envelope.remediation_codes,
            "use_original_quality_or_lower_quality".to_string(),
        );
    }
}

fn rolling_realtime_factor_millis(
    current: Option<i32>,
    observed: i32,
    previous_sample_count: i64,
) -> i32 {
    let Some(current) = current else {
        return observed;
    };
    let weight = previous_sample_count.clamp(1, 31);
    let weighted = i64::from(current)
        .saturating_mul(weight)
        .saturating_add(i64::from(observed));
    (weighted / (weight + 1)).clamp(0, i64::from(i32::MAX)) as i32
}

fn conservative_realtime_floor_millis(current: Option<i32>, observed: i32) -> i32 {
    current
        .map(|current| current.min(observed))
        .unwrap_or(observed)
}

fn non_empty_compact_token(raw: Option<&str>) -> Option<String> {
    let value = raw?.trim();
    (!value.is_empty()).then(|| {
        value
            .chars()
            .map(|ch| {
                if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                    ch.to_ascii_lowercase()
                } else {
                    '_'
                }
            })
            .collect::<String>()
            .trim_matches('_')
            .chars()
            .take(80)
            .collect()
    })
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlaybackPerformanceSeedSummary {
    pub artifacts_seen: usize,
    pub envelopes_seen: usize,
    pub envelopes_upserted: usize,
    pub envelopes_skipped_host_mismatch: usize,
}

pub async fn seed_playback_performance_envelopes_from_certification_artifacts(
    pool: &AnyPool,
    artifact_paths: &[String],
    active_host_fingerprint: Option<&str>,
) -> Result<PlaybackPerformanceSeedSummary> {
    let mut summary = PlaybackPerformanceSeedSummary::default();
    for raw_path in artifact_paths {
        let artifact_path = PathBuf::from(raw_path);
        let certification_path = certification_json_path(&artifact_path);
        if !certification_path.exists() {
            warn!(
                path = %artifact_path.display(),
                "configured playback performance certification artifact is missing"
            );
            continue;
        }
        summary.artifacts_seen += 1;
        let root = certification_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| artifact_path.clone());
        let report = read_certification_report(&certification_path)?;
        let envelopes = performance_envelopes_from_certification_report(&root, &report)?;
        summary.envelopes_seen += envelopes.len();
        for envelope in envelopes {
            if active_host_fingerprint
                .is_some_and(|host| host != envelope.host_fingerprint.as_str())
            {
                summary.envelopes_skipped_host_mismatch += 1;
                continue;
            }
            upsert_playback_performance_envelope(pool, &envelope).await?;
            summary.envelopes_upserted += 1;
        }
    }
    Ok(summary)
}

fn certification_json_path(path: &Path) -> PathBuf {
    if path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name == "certification.json")
    {
        path.to_path_buf()
    } else {
        path.join("certification.json")
    }
}

fn read_certification_report(path: &Path) -> Result<CertificationReport> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("read certification report {}", path.display()))?;
    serde_json::from_str(&raw)
        .with_context(|| format!("parse certification report {}", path.display()))
}

fn performance_envelopes_from_certification_report(
    artifact_root: &Path,
    report: &CertificationReport,
) -> Result<Vec<PlaybackPerformanceEnvelope>> {
    if !report.performance_envelopes.is_empty() {
        return Ok(report.performance_envelopes.clone());
    }

    let mut envelopes = Vec::new();
    for case in &report.cases.case_reports {
        let plan_path = artifact_root
            .join("cases")
            .join(&case.id)
            .join("playback-plan.json");
        if !plan_path.exists() {
            continue;
        }
        let raw = fs::read_to_string(&plan_path)
            .with_context(|| format!("read certification playback plan {}", plan_path.display()))?;
        let plan: PlaybackPlan = serde_json::from_str(&raw).with_context(|| {
            format!("parse certification playback plan {}", plan_path.display())
        })?;
        if let Some(envelope) = performance_envelope_from_certification_case(report, case, &plan) {
            envelopes.push(envelope);
        }
    }
    Ok(envelopes)
}

pub(crate) fn performance_envelope_from_certification_case(
    report: &CertificationReport,
    case: &CaseReport,
    plan: &PlaybackPlan,
) -> Option<PlaybackPerformanceEnvelope> {
    let workload = plan.workload_class.as_ref()?;
    if !plan.mode.is_hls_producing() {
        return None;
    }
    let host_fingerprint = report
        .hardware_readiness
        .as_ref()
        .map(|readiness| readiness.host_fingerprint.clone())
        .filter(|value| !value.trim().is_empty())?;
    let readiness_record = report
        .hardware_readiness
        .as_ref()
        .and_then(|readiness| select_readiness_record(&readiness.records, plan));
    let unsupported_failure = case
        .errors
        .iter()
        .any(|error| error_indicates_unsupported_hardware(error));
    let support_decision = if unsupported_failure {
        PlaybackSupportDecision::Unsupported
    } else if plan.hardware_acceleration.enabled || case.hardware_used {
        PlaybackSupportDecision::Supported
    } else {
        PlaybackSupportDecision::SoftwareOnly
    };
    let performance_decision = if unsupported_failure {
        PlaybackPerformanceDecision::Unknown
    } else if case
        .performance_gate
        .as_ref()
        .is_some_and(|gate| !gate.passed)
    {
        PlaybackPerformanceDecision::NotRealtime
    } else if let Some(realtime_factor) = case.realtime_factor {
        performance_decision_for_realtime_factor(realtime_factor)
    } else {
        PlaybackPerformanceDecision::Unknown
    };
    let confidence = if report.passed() && case.status == CaseStatus::Passed {
        PlaybackPerformanceConfidence::Certified
    } else {
        PlaybackPerformanceConfidence::LocalBenchmark
    };
    let mut reasons = vec![
        "certification_artifact".to_string(),
        format!("certification_case:{}", case.id),
    ];
    if unsupported_failure {
        reasons.push(unsupported_reason_for_plan(plan).to_string());
    }
    if performance_decision == PlaybackPerformanceDecision::NotRealtime {
        reasons.push(not_realtime_reason_for_plan(plan).to_string());
    }
    let mut warnings = case.warnings.clone();
    if !report.passed() {
        push_unique_string(&mut warnings, "certification_report_failed".to_string());
    }
    let remediation_codes =
        remediation_codes_for_decision(plan, support_decision, performance_decision);
    let realtime_millis = case
        .realtime_factor
        .map(realtime_factor_to_millis)
        .filter(|_| performance_decision != PlaybackPerformanceDecision::Unknown);
    let last_observed_at = report
        .finished_at
        .map(|finished| finished.to_rfc3339())
        .or_else(|| Some(report.started_at.to_rfc3339()));
    Some(PlaybackPerformanceEnvelope {
        id: format!(
            "cert-{}",
            short_hash(&format!(
                "{}|{}|{}|{}",
                host_fingerprint, workload.class_id, workload.pipeline_signature, case.id
            ))
        ),
        host_fingerprint: host_fingerprint.clone(),
        os_family: readiness_record
            .map(|record| record.os_family.clone())
            .unwrap_or_else(|| report.os.family.clone()),
        os_version: readiness_record
            .and_then(|record| record.os_version.clone())
            .or_else(|| report.os.version.clone()),
        gpu_vendor: readiness_record
            .and_then(|record| record.gpu_vendor.clone())
            .or_else(|| report.gpu.vendor.clone()),
        gpu_model: readiness_record
            .and_then(|record| record.gpu_model.clone())
            .or_else(|| report.gpu.model.clone()),
        gpu_driver_version: readiness_record
            .and_then(|record| record.gpu_driver_version.clone())
            .or_else(|| report.gpu.driver_version.clone()),
        hardware_api: plan
            .hardware_acceleration
            .api
            .clone()
            .or_else(|| report.hardware_api.clone()),
        ffmpeg_path: readiness_record.and_then(|record| record.ffmpeg_path.clone()),
        ffmpeg_version: readiness_record
            .and_then(|record| record.ffmpeg_version.clone())
            .or_else(|| report.ffmpeg.version.clone()),
        ffmpeg_sha256: readiness_record.and_then(|record| record.ffmpeg_sha256.clone()),
        elixir_version: Some(current_elixir_version().to_string()),
        workload_class_id: workload.class_id.clone(),
        pipeline_signature: workload.pipeline_signature.clone(),
        support_decision,
        performance_decision,
        confidence,
        p50_realtime_factor_millis: realtime_millis,
        p95_realtime_factor_millis: realtime_millis,
        startup_latency_ms: None,
        first_segment_latency_ms: None,
        failure_count: if case.status == CaseStatus::Failed {
            1
        } else {
            0
        },
        sample_count: if case.realtime_factor.is_some() || case.status == CaseStatus::Failed {
            1
        } else {
            0
        },
        invalidation_fingerprint: certification_invalidation_fingerprint(
            &host_fingerprint,
            readiness_record,
            workload,
        ),
        last_observed_at,
        reasons,
        warnings,
        remediation_codes,
    })
}

#[derive(Debug)]
pub struct PlaybackPerformanceProbeScheduler {
    enabled: bool,
    timeout: Duration,
    in_flight: Arc<DashSet<String>>,
}

impl PlaybackPerformanceProbeScheduler {
    pub fn new(enabled: bool, timeout_seconds: u64) -> Self {
        Self {
            enabled,
            timeout: Duration::from_secs(timeout_seconds.max(1)),
            in_flight: Arc::new(DashSet::new()),
        }
    }

    pub fn disabled() -> Self {
        Self::new(false, 20)
    }

    #[cfg(test)]
    fn try_mark_in_flight_for_test(&self, key: &str) -> bool {
        self.in_flight.insert(key.to_string())
    }

    pub fn queue_missing_workload_probe(
        &self,
        pool: AnyPool,
        input_path: impl Into<String>,
        host_fingerprint: impl Into<String>,
        plan: &mut PlaybackPlan,
    ) -> bool {
        if !self.enabled || !plan_needs_background_probe(plan) {
            return false;
        }
        let Some(workload) = plan.workload_class.as_ref() else {
            return false;
        };
        let input_path = input_path.into();
        let host_fingerprint = host_fingerprint.into();
        let key = format!(
            "{}|{}|{}|{}",
            host_fingerprint, workload.class_id, workload.pipeline_signature, input_path
        );
        if !self.in_flight.insert(key.clone()) {
            return false;
        }
        if let Some(feasibility) = plan.feasibility.as_mut() {
            feasibility.background_probe_queued = true;
        }
        let in_flight = self.in_flight.clone();
        let timeout_duration = self.timeout;
        let plan = plan.clone();
        tokio::spawn(async move {
            match timeout(
                timeout_duration,
                run_bounded_performance_probe(&pool, &input_path, &host_fingerprint, &plan),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    warn!(
                        error = %err,
                        workload_class = plan
                            .workload_class
                            .as_ref()
                            .map(|workload| workload.class_id.as_str())
                            .unwrap_or("unknown"),
                        "playback performance probe failed"
                    );
                }
                Err(_) => {
                    let _ = record_timeout_performance_probe(
                        &pool,
                        &host_fingerprint,
                        &plan,
                        timeout_duration,
                    )
                    .await;
                }
            };
            in_flight.remove(&key);
        });
        true
    }
}

impl Default for PlaybackPerformanceProbeScheduler {
    fn default() -> Self {
        Self::disabled()
    }
}

fn plan_needs_background_probe(plan: &PlaybackPlan) -> bool {
    if !plan.mode.is_hls_producing() || plan.workload_class.is_none() {
        return false;
    }
    let Some(feasibility) = plan.feasibility.as_ref() else {
        return false;
    };
    feasibility.selected_envelope_id.is_none()
        && feasibility.performance_decision == PlaybackPerformanceDecision::Unknown
        && matches!(
            feasibility.action,
            PlaybackFeasibilityAction::Reject | PlaybackFeasibilityAction::AllowWithWarning
        )
}

async fn run_bounded_performance_probe(
    pool: &AnyPool,
    input_path: &str,
    host_fingerprint: &str,
    plan: &PlaybackPlan,
) -> Result<()> {
    let workload = plan
        .workload_class
        .as_ref()
        .context("performance probe plan has no workload class")?;
    let records = load_current_hardware_readiness_records(pool, host_fingerprint)
        .await
        .unwrap_or_default();
    let readiness_record = select_readiness_record(&records, plan);
    let host_metadata = ProbeHostMetadata::from_readiness_record(readiness_record);
    let output_dir = TempDirBuilder::new()
        .prefix("elixir-playback-performance-probe-")
        .tempdir()
        .context("create playback performance probe temp dir")?;
    let layout = HlsOutputLayout::for_job(output_dir.path(), plan.mode, plan.delivery);
    let params = TranscodeParams {
        seek_seconds: 0.0,
        mode: plan.mode,
        delivery: plan.delivery,
    };
    let subtitles = if plan.subtitle_action == StreamAction::ConvertTextToWebvtt {
        detect_text_subtitles(input_path, plan.selected_subtitle_track).await
    } else {
        Vec::new()
    };
    let fps = probe_video_fps(input_path).await.unwrap_or(24.0);
    let mut args = build_transcode_ffmpeg_args(
        input_path,
        &params,
        Some(plan),
        &layout,
        output_dir.path(),
        &subtitles,
        fps,
    );
    insert_output_duration_limit(&mut args, LOCAL_PROBE_OUTPUT_SECONDS);

    let ffmpeg_program = host_metadata.ffmpeg_path.as_deref().unwrap_or("ffmpeg");
    let started = Instant::now();
    let output = Command::new(ffmpeg_program)
        .args(&args)
        .output()
        .await
        .context("run bounded playback performance probe ffmpeg")?;
    let elapsed = started.elapsed();
    let status = if output.status.success() && layout.master_playlist_path.exists() {
        "ok"
    } else {
        "failed"
    };
    PLAYBACK_PERFORMANCE_PROBE_DURATION
        .with_label_values(&[
            workload.class_id.as_str(),
            plan.hardware_acceleration
                .api
                .as_deref()
                .unwrap_or("software"),
            status,
        ])
        .observe(elapsed.as_secs_f64());

    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    let envelope = if output.status.success() && layout.master_playlist_path.exists() {
        let realtime_factor = LOCAL_PROBE_OUTPUT_SECONDS / elapsed.as_secs_f64().max(0.001);
        envelope_from_probe_result(
            host_fingerprint,
            &host_metadata,
            plan,
            PlaybackSupportDecision::Supported,
            performance_decision_for_realtime_factor(realtime_factor),
            Some(realtime_factor),
            0,
            vec!["bounded_local_probe".to_string()],
            Vec::new(),
            None,
            None,
        )
    } else if error_indicates_unsupported_hardware(&stderr) {
        envelope_from_probe_result(
            host_fingerprint,
            &host_metadata,
            plan,
            PlaybackSupportDecision::Unsupported,
            PlaybackPerformanceDecision::Unknown,
            None,
            1,
            vec![
                "bounded_local_probe".to_string(),
                unsupported_reason_for_plan(plan).to_string(),
            ],
            vec![tail(&stderr)],
            None,
            None,
        )
    } else {
        envelope_from_probe_result(
            host_fingerprint,
            &host_metadata,
            plan,
            support_decision_for_probe_plan(plan),
            PlaybackPerformanceDecision::Unknown,
            None,
            1,
            vec!["bounded_local_probe_failed".to_string()],
            vec![tail(&stderr)],
            None,
            None,
        )
    };
    upsert_playback_performance_envelope(pool, &envelope).await?;
    if status == "failed" {
        bail!(
            "bounded playback performance probe failed: {}",
            tail(&stderr)
        );
    }
    Ok(())
}

async fn record_timeout_performance_probe(
    pool: &AnyPool,
    host_fingerprint: &str,
    plan: &PlaybackPlan,
    timeout_duration: Duration,
) -> Result<()> {
    if let Some(workload) = plan.workload_class.as_ref() {
        PLAYBACK_PERFORMANCE_PROBE_DURATION
            .with_label_values(&[
                workload.class_id.as_str(),
                plan.hardware_acceleration
                    .api
                    .as_deref()
                    .unwrap_or("software"),
                "timeout",
            ])
            .observe(timeout_duration.as_secs_f64());
    }
    let records = load_current_hardware_readiness_records(pool, host_fingerprint)
        .await
        .unwrap_or_default();
    let readiness_record = select_readiness_record(&records, plan);
    let host_metadata = ProbeHostMetadata::from_readiness_record(readiness_record);
    let envelope = envelope_from_probe_result(
        host_fingerprint,
        &host_metadata,
        plan,
        support_decision_for_probe_plan(plan),
        PlaybackPerformanceDecision::NotRealtime,
        None,
        1,
        vec![
            "bounded_local_probe_timeout".to_string(),
            not_realtime_reason_for_plan(plan).to_string(),
        ],
        Vec::new(),
        None,
        None,
    );
    upsert_playback_performance_envelope(pool, &envelope).await?;
    Ok(())
}

fn envelope_from_probe_result(
    host_fingerprint: &str,
    host_metadata: &ProbeHostMetadata,
    plan: &PlaybackPlan,
    support_decision: PlaybackSupportDecision,
    performance_decision: PlaybackPerformanceDecision,
    realtime_factor: Option<f64>,
    failure_count: i64,
    reasons: Vec<String>,
    warnings: Vec<String>,
    startup_latency_ms: Option<i64>,
    first_segment_latency_ms: Option<i64>,
) -> PlaybackPerformanceEnvelope {
    let workload = plan
        .workload_class
        .as_ref()
        .expect("probe envelope requires workload class");
    let realtime_millis = realtime_factor.map(realtime_factor_to_millis);
    PlaybackPerformanceEnvelope {
        id: format!(
            "probe-{}",
            short_hash(&format!(
                "{}|{}|{}",
                host_fingerprint, workload.class_id, workload.pipeline_signature
            ))
        ),
        host_fingerprint: host_fingerprint.to_string(),
        os_family: host_metadata.os_family.clone(),
        os_version: host_metadata.os_version.clone(),
        gpu_vendor: host_metadata.gpu_vendor.clone(),
        gpu_model: host_metadata.gpu_model.clone(),
        gpu_driver_version: host_metadata.gpu_driver_version.clone(),
        hardware_api: plan
            .hardware_acceleration
            .api
            .clone()
            .or_else(|| host_metadata.hardware_api.clone()),
        ffmpeg_path: host_metadata.ffmpeg_path.clone(),
        ffmpeg_version: host_metadata.ffmpeg_version.clone(),
        ffmpeg_sha256: host_metadata.ffmpeg_sha256.clone(),
        elixir_version: Some(current_elixir_version().to_string()),
        workload_class_id: workload.class_id.clone(),
        pipeline_signature: workload.pipeline_signature.clone(),
        support_decision,
        performance_decision,
        confidence: PlaybackPerformanceConfidence::LocalBenchmark,
        p50_realtime_factor_millis: realtime_millis,
        p95_realtime_factor_millis: realtime_millis,
        startup_latency_ms,
        first_segment_latency_ms,
        failure_count,
        sample_count: 1,
        invalidation_fingerprint: runtime_invalidation_fingerprint(
            host_fingerprint,
            host_metadata,
            workload,
        ),
        last_observed_at: Some(Utc::now().to_rfc3339()),
        reasons,
        warnings: warnings
            .into_iter()
            .filter(|warning| !warning.trim().is_empty())
            .collect(),
        remediation_codes: remediation_codes_for_decision(
            plan,
            support_decision,
            performance_decision,
        ),
    }
}

fn envelope_from_row(row: sqlx::any::AnyRow) -> Result<PlaybackPerformanceEnvelope> {
    let support_decision: String = row.try_get("support_decision")?;
    let performance_decision: String = row.try_get("performance_decision")?;
    let confidence: String = row.try_get("confidence")?;
    let reasons_json: String = row.try_get("reasons_json")?;
    let warnings_json: String = row.try_get("warnings_json")?;
    let remediation_json: String = row.try_get("remediation_json")?;
    Ok(PlaybackPerformanceEnvelope {
        id: row.try_get("id")?,
        host_fingerprint: row.try_get("host_fingerprint")?,
        os_family: row.try_get("os_family")?,
        os_version: row.try_get("os_version")?,
        gpu_vendor: row.try_get("gpu_vendor")?,
        gpu_model: row.try_get("gpu_model")?,
        gpu_driver_version: row.try_get("gpu_driver_version")?,
        hardware_api: row.try_get("hardware_api")?,
        ffmpeg_path: row.try_get("ffmpeg_path")?,
        ffmpeg_version: row.try_get("ffmpeg_version")?,
        ffmpeg_sha256: row.try_get("ffmpeg_sha256")?,
        elixir_version: row.try_get("elixir_version")?,
        workload_class_id: row.try_get("workload_class_id")?,
        pipeline_signature: row.try_get("pipeline_signature")?,
        support_decision: parse_support_decision(&support_decision),
        performance_decision: parse_performance_decision(&performance_decision),
        confidence: parse_confidence(&confidence),
        p50_realtime_factor_millis: optional_i32_column(&row, "p50_realtime_factor_millis"),
        p95_realtime_factor_millis: optional_i32_column(&row, "p95_realtime_factor_millis"),
        startup_latency_ms: optional_i64_column(&row, "startup_latency_ms"),
        first_segment_latency_ms: optional_i64_column(&row, "first_segment_latency_ms"),
        failure_count: row.try_get("failure_count")?,
        sample_count: row.try_get("sample_count")?,
        invalidation_fingerprint: row.try_get("invalidation_fingerprint")?,
        last_observed_at: row.try_get("last_observed_at")?,
        reasons: parse_string_list(&reasons_json)?,
        warnings: parse_string_list(&warnings_json)?,
        remediation_codes: parse_string_list(&remediation_json)?,
    })
}

fn parse_support_decision(raw: &str) -> PlaybackSupportDecision {
    match raw {
        "supported" => PlaybackSupportDecision::Supported,
        "unsupported" => PlaybackSupportDecision::Unsupported,
        "mixed_fallback" => PlaybackSupportDecision::MixedFallback,
        "software_only" => PlaybackSupportDecision::SoftwareOnly,
        _ => PlaybackSupportDecision::Unknown,
    }
}

fn parse_performance_decision(raw: &str) -> PlaybackPerformanceDecision {
    match raw {
        "realtime_safe" => PlaybackPerformanceDecision::RealtimeSafe,
        "realtime_marginal" => PlaybackPerformanceDecision::RealtimeMarginal,
        "not_realtime" => PlaybackPerformanceDecision::NotRealtime,
        _ => PlaybackPerformanceDecision::Unknown,
    }
}

fn parse_confidence(raw: &str) -> PlaybackPerformanceConfidence {
    match raw {
        "certified" => PlaybackPerformanceConfidence::Certified,
        "local_benchmark" => PlaybackPerformanceConfidence::LocalBenchmark,
        "live_observed" => PlaybackPerformanceConfidence::LiveObserved,
        "static_inferred" => PlaybackPerformanceConfidence::StaticInferred,
        _ => PlaybackPerformanceConfidence::Unknown,
    }
}

fn optional_i32_column(row: &sqlx::any::AnyRow, column: &str) -> Option<i32> {
    optional_i64_column(row, column).and_then(|value| i32::try_from(value).ok())
}

fn optional_i64_column(row: &sqlx::any::AnyRow, column: &str) -> Option<i64> {
    row.try_get::<i64, _>(column).ok()
}

fn parse_string_list(raw: &str) -> Result<Vec<String>> {
    let value: Value = serde_json::from_str(raw).context("parse string list json")?;
    Ok(value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect())
}

pub fn validate_observation_timestamp(raw: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(raw)
        .with_context(|| format!("parse observation timestamp {raw:?}"))?
        .with_timezone(&Utc))
}

fn select_readiness_record<'a>(
    records: &'a [HardwareReadinessRecord],
    plan: &PlaybackPlan,
) -> Option<&'a HardwareReadinessRecord> {
    plan.hardware_acceleration
        .api
        .as_deref()
        .and_then(|api| {
            records
                .iter()
                .find(|record| record.api.eq_ignore_ascii_case(api))
        })
        .or_else(|| records.first())
}

fn performance_decision_for_realtime_factor(realtime_factor: f64) -> PlaybackPerformanceDecision {
    if realtime_factor >= REALTIME_SAFE_THRESHOLD {
        PlaybackPerformanceDecision::RealtimeSafe
    } else if realtime_factor >= REALTIME_MARGINAL_THRESHOLD {
        PlaybackPerformanceDecision::RealtimeMarginal
    } else {
        PlaybackPerformanceDecision::NotRealtime
    }
}

fn realtime_factor_to_millis(realtime_factor: f64) -> i32 {
    (realtime_factor * 1000.0)
        .round()
        .clamp(0.0, i32::MAX as f64) as i32
}

fn support_decision_for_probe_plan(plan: &PlaybackPlan) -> PlaybackSupportDecision {
    if plan.hardware_acceleration.enabled {
        PlaybackSupportDecision::Supported
    } else {
        PlaybackSupportDecision::SoftwareOnly
    }
}

fn unsupported_reason_for_plan(plan: &PlaybackPlan) -> &'static str {
    if plan.hardware_acceleration.decoder.is_some() {
        "hardware_decode_unsupported"
    } else if plan.hardware_acceleration.encoder.is_some() {
        "hardware_encode_unsupported"
    } else {
        "hardware_filter_unsupported"
    }
}

fn not_realtime_reason_for_plan(plan: &PlaybackPlan) -> &'static str {
    if plan.subtitle_action == StreamAction::BurnIn {
        "server_cannot_realtime_burn_subtitles"
    } else if plan.hdr_action == HdrAction::ToneMapToSdr {
        "server_cannot_realtime_tonemap_source"
    } else {
        "server_cannot_realtime_transcode_source"
    }
}

fn remediation_codes_for_decision(
    plan: &PlaybackPlan,
    support_decision: PlaybackSupportDecision,
    performance_decision: PlaybackPerformanceDecision,
) -> Vec<String> {
    if support_decision == PlaybackSupportDecision::Unsupported {
        if plan.hardware_acceleration.enabled {
            vec!["update_driver_or_use_original_quality".to_string()]
        } else {
            vec!["install_ffmpeg_with_h264_aac_hls_support".to_string()]
        }
    } else if performance_decision == PlaybackPerformanceDecision::NotRealtime {
        if plan.subtitle_action == StreamAction::BurnIn {
            vec!["disable_subtitle_burn_in_or_lower_quality".to_string()]
        } else {
            vec!["use_original_quality_or_lower_quality".to_string()]
        }
    } else if performance_decision == PlaybackPerformanceDecision::Unknown {
        vec!["try_original_quality_or_lower_quality".to_string()]
    } else {
        Vec::new()
    }
}

fn certification_invalidation_fingerprint(
    host_fingerprint: &str,
    readiness_record: Option<&HardwareReadinessRecord>,
    workload: &crate::playback::plan::PlaybackWorkloadClass,
) -> String {
    let mut digest = Sha256::new();
    digest.update(host_fingerprint.as_bytes());
    digest.update(b"\0");
    digest.update(workload.class_id.as_bytes());
    digest.update(b"\0");
    digest.update(workload.pipeline_signature.as_bytes());
    digest.update(b"\0");
    digest.update(current_elixir_version().as_bytes());
    digest.update(b"\0");
    if let Some(record) = readiness_record {
        digest.update(record.api.as_bytes());
        digest.update(b"\0");
        digest.update(
            record
                .gpu_driver_version
                .as_deref()
                .unwrap_or("")
                .as_bytes(),
        );
        digest.update(b"\0");
        digest.update(record.ffmpeg_sha256.as_deref().unwrap_or("").as_bytes());
        digest.update(b"\0");
        digest.update(record.ffmpeg_version.as_deref().unwrap_or("").as_bytes());
    }
    format!("sha256:{:x}", digest.finalize())
}

fn runtime_invalidation_fingerprint(
    host_fingerprint: &str,
    host_metadata: &ProbeHostMetadata,
    workload: &crate::playback::plan::PlaybackWorkloadClass,
) -> String {
    let mut digest = Sha256::new();
    digest.update(host_fingerprint.as_bytes());
    digest.update(b"\0");
    digest.update(workload.class_id.as_bytes());
    digest.update(b"\0");
    digest.update(workload.pipeline_signature.as_bytes());
    digest.update(b"\0");
    digest.update(current_elixir_version().as_bytes());
    digest.update(b"\0");
    digest.update(
        host_metadata
            .hardware_api
            .as_deref()
            .unwrap_or("")
            .as_bytes(),
    );
    digest.update(b"\0");
    digest.update(
        host_metadata
            .gpu_driver_version
            .as_deref()
            .unwrap_or("")
            .as_bytes(),
    );
    digest.update(b"\0");
    digest.update(
        host_metadata
            .ffmpeg_sha256
            .as_deref()
            .unwrap_or("")
            .as_bytes(),
    );
    digest.update(b"\0");
    digest.update(
        host_metadata
            .ffmpeg_version
            .as_deref()
            .unwrap_or("")
            .as_bytes(),
    );
    format!("sha256:{:x}", digest.finalize())
}

fn short_hash(raw: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(raw.as_bytes());
    format!("{:x}", digest.finalize())[..16].to_string()
}

fn push_unique_string(values: &mut Vec<String>, value: String) {
    if !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
}

fn error_indicates_unsupported_hardware(raw: &str) -> bool {
    let lower = raw.to_ascii_lowercase();
    [
        "not supported",
        "unsupported",
        "no capable devices",
        "no device available",
        "device does not support",
        "invalid encoder",
        "unknown encoder",
        "codec not currently supported",
        "driver does not support",
        "required nvenc",
        "cannot load",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn insert_output_duration_limit(args: &mut Vec<String>, seconds: f64) {
    let insertion = args
        .iter()
        .position(|arg| arg == "-f")
        .unwrap_or(args.len());
    args.splice(
        insertion..insertion,
        ["-t".to_string(), seconds.max(1.0).to_string()],
    );
}

fn tail(raw: &str) -> String {
    raw.lines()
        .rev()
        .take(8)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>()
        .join("\n")
        .chars()
        .take(2000)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{DatabaseConfig, PlaybackConfig},
        db::{Database, DatabaseDriver},
        playback::{
            certification::{
                CaseSummary, CertificationStatus, FfmpegInventoryReport, HostGpuReport,
                HostOsReport, PerformanceSummary,
            },
            hardware::HardwareCapabilities,
        },
    };
    use std::collections::BTreeMap;

    fn envelope_fixture() -> PlaybackPerformanceEnvelope {
        PlaybackPerformanceEnvelope {
            id: "envelope-fixture".to_string(),
            host_fingerprint: "host-a".to_string(),
            os_family: "windows".to_string(),
            os_version: Some("11".to_string()),
            gpu_vendor: Some("nvidia".to_string()),
            gpu_model: Some("RTX fixture".to_string()),
            gpu_driver_version: Some("576.80".to_string()),
            hardware_api: Some("nvenc".to_string()),
            ffmpeg_path: Some("ffmpeg".to_string()),
            ffmpeg_version: Some("ffmpeg fixture".to_string()),
            ffmpeg_sha256: Some("sha256:fixture".to_string()),
            elixir_version: Some(current_elixir_version().to_string()),
            workload_class_id: "video:h264:1080p:h264:720p".to_string(),
            pipeline_signature: "decode:cuda|encode:h264_nvenc".to_string(),
            support_decision: PlaybackSupportDecision::Supported,
            performance_decision: PlaybackPerformanceDecision::RealtimeSafe,
            confidence: PlaybackPerformanceConfidence::Certified,
            p50_realtime_factor_millis: Some(1_600),
            p95_realtime_factor_millis: Some(1_250),
            startup_latency_ms: Some(400),
            first_segment_latency_ms: Some(900),
            failure_count: 0,
            sample_count: 8,
            invalidation_fingerprint: "host-ffmpeg-server-policy".to_string(),
            last_observed_at: Some("2026-07-01T00:00:00Z".to_string()),
            reasons: vec!["certification_artifact".to_string()],
            warnings: vec!["thermal_margin_low".to_string()],
            remediation_codes: vec!["lower_quality".to_string()],
        }
    }

    async fn setup_database() -> Result<Database> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        assert_eq!(database.driver, DatabaseDriver::Sqlite);
        database.run_migrations().await?;
        Ok(database)
    }

    #[tokio::test]
    async fn performance_envelope_round_trips_and_upserts_by_invalidation_key() -> Result<()> {
        let database = setup_database().await?;
        let envelope = envelope_fixture();

        let id = upsert_playback_performance_envelope(&database.pool, &envelope).await?;
        assert_eq!(id, "envelope-fixture");

        let loaded = load_playback_performance_envelopes(&database.pool, "host-a").await?;
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].workload_class_id, envelope.workload_class_id);
        assert_eq!(
            loaded[0].performance_decision,
            PlaybackPerformanceDecision::RealtimeSafe
        );
        assert_eq!(
            loaded[0].reasons,
            vec!["certification_artifact".to_string()]
        );
        assert_eq!(loaded[0].warnings, vec!["thermal_margin_low".to_string()]);
        assert_eq!(
            loaded[0].remediation_codes,
            vec!["lower_quality".to_string()]
        );
        assert!(
            load_playback_performance_envelopes(&database.pool, "host-b")
                .await?
                .is_empty()
        );

        let mut updated = envelope;
        updated.id = "envelope-fixture-updated".to_string();
        updated.performance_decision = PlaybackPerformanceDecision::NotRealtime;
        updated.confidence = PlaybackPerformanceConfidence::LiveObserved;
        updated.failure_count = 3;
        updated.sample_count = 12;
        updated.reasons = vec!["server_cannot_realtime_transcode_source".to_string()];
        upsert_playback_performance_envelope(&database.pool, &updated).await?;

        let loaded = load_playback_performance_envelopes(&database.pool, "host-a").await?;
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, "envelope-fixture-updated");
        assert_eq!(
            loaded[0].performance_decision,
            PlaybackPerformanceDecision::NotRealtime
        );
        assert_eq!(
            loaded[0].confidence,
            PlaybackPerformanceConfidence::LiveObserved
        );
        assert_eq!(loaded[0].failure_count, 3);
        assert_eq!(loaded[0].sample_count, 12);

        let changed = record_playback_performance_observation(
            &database.pool,
            "envelope-fixture-updated",
            true,
            Some(275),
            Some(725),
            Some(1_400),
            None,
            None,
            Some("video_transcode"),
        )
        .await?;
        assert_eq!(changed, 1);
        let changed = record_playback_performance_observation(
            &database.pool,
            "envelope-fixture-updated",
            false,
            None,
            None,
            None,
            Some("first_segment_timeout"),
            None,
            Some("video_transcode"),
        )
        .await?;
        assert_eq!(changed, 1);
        let loaded = load_playback_performance_envelopes(&database.pool, "host-a").await?;
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].sample_count, 14);
        assert_eq!(loaded[0].failure_count, 4);
        assert_eq!(loaded[0].startup_latency_ms, Some(275));
        assert_eq!(loaded[0].first_segment_latency_ms, Some(725));
        assert_eq!(loaded[0].p95_realtime_factor_millis, Some(1_250));
        assert!(
            loaded[0]
                .reasons
                .contains(&"live_output_mode:video_transcode".to_string())
        );
        assert!(
            loaded[0]
                .reasons
                .contains(&"live_failure_kind:first_segment_timeout".to_string())
        );

        Ok(())
    }

    #[tokio::test]
    async fn live_observations_downgrade_only_after_repeated_evidence() -> Result<()> {
        let database = setup_database().await?;
        let envelope = envelope_fixture();
        upsert_playback_performance_envelope(&database.pool, &envelope).await?;

        record_playback_performance_observation(
            &database.pool,
            "envelope-fixture",
            true,
            Some(500),
            Some(4_500),
            Some(888),
            None,
            None,
            Some("video_transcode"),
        )
        .await?;
        let loaded = load_playback_performance_envelopes(&database.pool, "host-a").await?;
        assert_eq!(
            loaded[0].performance_decision,
            PlaybackPerformanceDecision::RealtimeSafe,
            "one slow live observation must not overfit a certified envelope"
        );
        assert_eq!(loaded[0].sample_count, 9);

        for _ in 0..3 {
            record_playback_performance_observation(
                &database.pool,
                "envelope-fixture",
                false,
                None,
                Some(4_500),
                None,
                Some("first_segment_timeout"),
                None,
                Some("video_transcode"),
            )
            .await?;
        }

        let loaded = load_playback_performance_envelopes(&database.pool, "host-a").await?;
        assert_eq!(
            loaded[0].performance_decision,
            PlaybackPerformanceDecision::NotRealtime
        );
        assert!(
            loaded[0].reasons.iter().any(|reason| reason
                == "live_playback_failures_exceeded_threshold"
                || reason == "live_performance_below_realtime_threshold"),
            "{:?}",
            loaded[0].reasons
        );

        Ok(())
    }

    #[tokio::test]
    async fn live_software_fallback_success_never_upgrades_unsupported_hardware() -> Result<()> {
        let database = setup_database().await?;
        let mut envelope = envelope_fixture();
        envelope.support_decision = PlaybackSupportDecision::Unsupported;
        envelope.performance_decision = PlaybackPerformanceDecision::Unknown;
        envelope.p50_realtime_factor_millis = None;
        envelope.p95_realtime_factor_millis = None;
        upsert_playback_performance_envelope(&database.pool, &envelope).await?;

        for _ in 0..4 {
            record_playback_performance_observation(
                &database.pool,
                "envelope-fixture",
                true,
                Some(250),
                Some(750),
                Some(5_333),
                None,
                Some("software_after_hardware_failure"),
                Some("video_transcode"),
            )
            .await?;
        }

        let loaded = load_playback_performance_envelopes(&database.pool, "host-a").await?;
        assert_eq!(
            loaded[0].support_decision,
            PlaybackSupportDecision::Unsupported
        );
        assert_eq!(
            loaded[0].performance_decision,
            PlaybackPerformanceDecision::Unknown
        );
        assert_eq!(loaded[0].p50_realtime_factor_millis, None);
        assert!(
            loaded[0]
                .warnings
                .contains(&"live_fallback_path:software_after_hardware_failure".to_string())
        );

        Ok(())
    }

    #[tokio::test]
    async fn load_filters_out_stale_server_version_envelopes() -> Result<()> {
        let database = setup_database().await?;
        let mut stale = envelope_fixture();
        stale.id = "stale-server-envelope".to_string();
        stale.elixir_version = Some("0.0.0-stale".to_string());
        stale.invalidation_fingerprint = "stale-server-version".to_string();
        stale.performance_decision = PlaybackPerformanceDecision::NotRealtime;
        stale.reasons = vec!["stale_server_version".to_string()];
        upsert_playback_performance_envelope(&database.pool, &stale).await?;

        let current = envelope_fixture();
        upsert_playback_performance_envelope(&database.pool, &current).await?;

        let loaded = load_playback_performance_envelopes(&database.pool, "host-a").await?;
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, current.id);
        assert_eq!(
            loaded[0].elixir_version.as_deref(),
            Some(current_elixir_version())
        );
        assert_eq!(
            loaded[0].performance_decision,
            PlaybackPerformanceDecision::RealtimeSafe
        );

        Ok(())
    }

    #[tokio::test]
    async fn seeds_performance_envelopes_from_certification_artifacts() -> Result<()> {
        let database = setup_database().await?;
        let temp = tempfile::tempdir()?;
        let envelope = envelope_fixture();
        let report = CertificationReport {
            schema_version: 1,
            status: CertificationStatus::Passed,
            target_id: "win11-nvidia-fixture".to_string(),
            suite: "torture".to_string(),
            started_at: Utc::now(),
            finished_at: Some(Utc::now()),
            commit_sha: Some("0123456789abcdef0123456789abcdef01234567".to_string()),
            run_id: Some("123".to_string()),
            corpus_lock_sha256: "b".repeat(64),
            os: HostOsReport {
                family: "windows".to_string(),
                arch: "x86_64".to_string(),
                version: Some("11".to_string()),
                raw: BTreeMap::new(),
            },
            gpu: HostGpuReport {
                vendor: Some("nvidia".to_string()),
                model: Some("RTX fixture".to_string()),
                device_id: Some("fixture".to_string()),
                driver_version: Some("576.80".to_string()),
                raw: BTreeMap::new(),
            },
            hardware_api: Some("nvenc".to_string()),
            requested_hardware_api: "nvenc".to_string(),
            require_hardware: true,
            ffmpeg: FfmpegInventoryReport {
                version: Some("ffmpeg fixture".to_string()),
                hwaccels: vec!["cuda".to_string()],
                encoders: vec!["h264_nvenc".to_string()],
                decoders: vec!["h264_cuvid".to_string()],
            },
            hardware_capabilities: HardwareCapabilities::software_only(),
            hardware_readiness: None,
            cases: CaseSummary::default(),
            performance: PerformanceSummary::default(),
            performance_envelopes: vec![envelope.clone()],
            failure_reasons: Vec::new(),
            artifact_digest: Some("sha256:".to_string() + &"a".repeat(64)),
        };
        std::fs::write(
            temp.path().join("certification.json"),
            serde_json::to_vec_pretty(&report)?,
        )?;

        let summary = seed_playback_performance_envelopes_from_certification_artifacts(
            &database.pool,
            &[temp.path().to_string_lossy().to_string()],
            Some("host-a"),
        )
        .await?;
        assert_eq!(
            summary,
            PlaybackPerformanceSeedSummary {
                artifacts_seen: 1,
                envelopes_seen: 1,
                envelopes_upserted: 1,
                envelopes_skipped_host_mismatch: 0,
            }
        );
        let loaded = load_playback_performance_envelopes(&database.pool, "host-a").await?;
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, envelope.id);

        let summary = seed_playback_performance_envelopes_from_certification_artifacts(
            &database.pool,
            &[temp
                .path()
                .join("certification.json")
                .to_string_lossy()
                .to_string()],
            Some("other-host"),
        )
        .await?;
        assert_eq!(summary.envelopes_skipped_host_mismatch, 1);

        Ok(())
    }

    #[test]
    fn performance_probe_scheduler_deduplicates_in_flight_keys() {
        let scheduler = PlaybackPerformanceProbeScheduler::new(true, 5);
        assert!(scheduler.try_mark_in_flight_for_test("host|class|pipeline|source"));
        assert!(!scheduler.try_mark_in_flight_for_test("host|class|pipeline|source"));
        assert!(scheduler.try_mark_in_flight_for_test("host|class|pipeline|other-source"));
    }

    fn capabilities(raw: &str) -> MediaCapabilities {
        let value: Value = serde_json::from_str(raw).unwrap();
        let parsed: ffprobe::FfprobeStreams = serde_json::from_value(value.clone()).unwrap();
        let metadata = ffprobe::MediaMetadata {
            container: parsed
                .format
                .as_ref()
                .and_then(|format| format.format_name.clone()),
            video_codec: parsed
                .streams
                .iter()
                .find(|stream| stream.codec_type.as_deref() == Some("video"))
                .and_then(|stream| stream.codec_name.clone()),
            audio_codec: parsed
                .streams
                .iter()
                .find(|stream| stream.codec_type.as_deref() == Some("audio"))
                .and_then(|stream| stream.codec_name.clone()),
            width: parsed
                .streams
                .iter()
                .find(|stream| stream.codec_type.as_deref() == Some("video"))
                .and_then(|stream| stream.width),
            height: parsed
                .streams
                .iter()
                .find(|stream| stream.codec_type.as_deref() == Some("video"))
                .and_then(|stream| stream.height),
            bitrate_bps: parsed
                .format
                .as_ref()
                .and_then(|format| format.bit_rate.as_deref())
                .and_then(|value| value.parse::<i64>().ok()),
            duration_seconds: parsed
                .format
                .as_ref()
                .and_then(|format| format.duration.as_deref())
                .and_then(|value| value.parse::<f64>().ok())
                .map(|seconds| seconds.round() as i32),
            streams: parsed.streams,
            format: parsed.format,
            chapters: parsed.chapters,
            raw_json: value,
        };
        normalize_ffprobe_metadata(&metadata, None, None)
    }

    #[test]
    fn adaptive_readiness_probe_plan_uses_software_adaptive_pipeline() -> Result<()> {
        let media = capabilities(include_str!("fixtures/h264_aac_mkv.json"));
        let plan = adaptive_readiness_probe_plan(
            "probe-adaptive-h264-1080p",
            &media,
            &PlaybackConfig::default(),
        )?;

        assert_eq!(plan.mode, PlaybackMode::AdaptiveTranscode);
        assert!(plan.adaptive);
        assert!(!plan.hardware_acceleration.enabled);
        assert_eq!(plan.hardware_acceleration.api, None);
        let workload = plan.workload_class.as_ref().expect("workload class");
        assert!(workload.class_id.starts_with("adaptive_transcode:"));
        assert!(
            workload
                .pipeline_stages
                .contains(&"software_decode".to_string()),
            "{:?}",
            workload.pipeline_stages
        );
        assert!(
            workload
                .pipeline_stages
                .contains(&"software_encode".to_string()),
            "{:?}",
            workload.pipeline_stages
        );
        let ladder = plan.adaptive_ladder.as_ref().expect("adaptive ladder");
        assert!(ladder.rungs.len() >= 2, "{ladder:?}");
        assert!(ladder.rungs.iter().all(|rung| {
            rung.bandwidth_bps > 0
                && rung.average_bandwidth_bps > 0
                && !rung.resolution.is_empty()
                && !rung.codecs.is_empty()
        }));

        Ok(())
    }

    fn hdr10_1080p_capabilities() -> MediaCapabilities {
        capabilities(
            r#"
            {
              "format": {
                "format_name": "matroska,webm",
                "bit_rate": "12000000",
                "duration": "30.000000"
              },
              "streams": [
                {
                  "index": 0,
                  "codec_type": "video",
                  "codec_name": "hevc",
                  "profile": "Main 10",
                  "pix_fmt": "yuv420p10le",
                  "width": 1920,
                  "height": 1080,
                  "avg_frame_rate": "24000/1001",
                  "bits_per_raw_sample": "10",
                  "bit_rate": "11800000",
                  "color_primaries": "bt2020",
                  "color_transfer": "smpte2084",
                  "color_space": "bt2020nc",
                  "side_data_list": [
                    { "side_data_type": "Mastering display metadata" },
                    { "side_data_type": "Content light level metadata" }
                  ]
                },
                {
                  "index": 1,
                  "codec_type": "audio",
                  "codec_name": "aac",
                  "channels": 2,
                  "channel_layout": "stereo",
                  "sample_rate": "48000",
                  "bit_rate": "192000"
                }
              ]
            }
            "#,
        )
    }

    #[test]
    fn hdr_tone_mapping_readiness_probe_plan_uses_software_tonemap_pipeline() -> Result<()> {
        let media = hdr10_1080p_capabilities();
        assert!(media.primary_video().is_some_and(|video| video.hdr10));

        let plan = hdr_tone_mapping_readiness_probe_plan(
            "probe-hdr10-to-sdr-1080p",
            &media,
            &PlaybackConfig::default(),
        )?;

        assert_eq!(plan.mode, PlaybackMode::VideoTranscode);
        assert_eq!(plan.hdr_action, HdrAction::ToneMapToSdr);
        assert!(!plan.hardware_acceleration.enabled);
        assert!(
            plan.reasons
                .contains(&"hdr_tone_mapping_required".to_string()),
            "{:?}",
            plan.reasons
        );
        let output = plan.video_output.as_ref().expect("video output");
        let tone_map = output.tone_map.as_ref().expect("tone map plan");
        assert_eq!(tone_map.input_primaries.as_deref(), Some("bt2020"));
        assert_eq!(tone_map.input_transfer.as_deref(), Some("smpte2084"));
        assert_eq!(tone_map.input_matrix.as_deref(), Some("bt2020nc"));
        assert_eq!(tone_map.output_primaries, "bt709");
        assert_eq!(tone_map.output_transfer, "bt709");
        assert_eq!(tone_map.output_matrix, "bt709");
        assert_eq!(output.scale.as_ref().map(|scale| scale.height), Some(720));
        let workload = plan.workload_class.as_ref().expect("workload class");
        assert!(workload.class_id.contains(":hdr_tonemap:"), "{workload:?}");
        assert!(workload.cost_labels.contains(&"hdr_tonemap".to_string()));
        assert!(
            workload
                .pipeline_stages
                .contains(&"software_filter".to_string()),
            "{:?}",
            workload.pipeline_stages
        );

        Ok(())
    }

    #[test]
    fn adaptive_hls_probe_validation_requires_rung_artifacts_and_metadata() -> Result<()> {
        let media = capabilities(include_str!("fixtures/h264_aac_mkv.json"));
        let plan = adaptive_readiness_probe_plan(
            "probe-adaptive-h264-1080p",
            &media,
            &PlaybackConfig::default(),
        )?;
        let ladder = plan.adaptive_ladder.as_ref().expect("adaptive ladder");
        let temp = tempfile::tempdir()?;
        let layout = HlsOutputLayout::for_job(
            temp.path(),
            PlaybackMode::AdaptiveTranscode,
            Delivery::HlsAdaptiveFmp4,
        );

        fs::write(
            &layout.master_playlist_path,
            "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1000000\nstream_0.m3u8\n",
        )?;
        let failures = hls_probe_output_failures(temp.path(), &layout, &plan);
        assert!(
            failures
                .iter()
                .any(|failure| failure.contains("average_bandwidth")),
            "{failures:?}"
        );
        assert!(
            failures
                .iter()
                .any(|failure| failure.starts_with("adaptive_rung_playlist_missing")),
            "{failures:?}"
        );

        let mut master = "#EXTM3U\n".to_string();
        for (idx, rung) in ladder.rungs.iter().enumerate() {
            master.push_str(&format!(
                "#EXT-X-STREAM-INF:BANDWIDTH={},AVERAGE-BANDWIDTH={},RESOLUTION={},CODECS=\"{}\",FRAME-RATE={}\nstream_{idx}.m3u8\n",
                rung.bandwidth_bps,
                rung.average_bandwidth_bps,
                rung.resolution,
                rung.codecs,
                rung.frame_rate.as_deref().unwrap_or("24")
            ));
            fs::write(temp.path().join(format!("stream_{idx}.m3u8")), "#EXTM3U\n")?;
            fs::write(temp.path().join(format!("init_{idx}.mp4")), [])?;
            fs::write(temp.path().join(format!("seg_{idx}_00000.m4s")), [])?;
        }
        fs::write(&layout.master_playlist_path, master)?;

        let failures = hls_probe_output_failures(temp.path(), &layout, &plan);
        assert!(failures.is_empty(), "{failures:?}");

        Ok(())
    }

    #[test]
    fn adaptive_readiness_summary_is_unknown_until_local_envelope_exists() {
        let summary = summarize_adaptive_playback_readiness(Some("host-a"), &[]);
        assert_eq!(summary.status, "unknown");
        assert!(
            summary
                .reasons
                .contains(&"adaptive_readiness_probe_not_run".to_string())
        );

        let mut envelope = envelope_fixture();
        envelope.hardware_api = None;
        envelope.support_decision = PlaybackSupportDecision::SoftwareOnly;
        envelope.workload_class_id =
            "adaptive_transcode:h264:1080p:h264:720p:sdr:sub_none:software_decode_software_encode:1080p_downscale".to_string();
        envelope.performance_decision = PlaybackPerformanceDecision::RealtimeSafe;
        envelope.reasons = vec!["playback_readiness_probe".to_string()];
        let summary = summarize_adaptive_playback_readiness(Some("host-a"), &[envelope]);
        assert_eq!(summary.status, "ready");
        assert_eq!(summary.adaptive_envelope_count, 1);
        assert_eq!(summary.realtime_safe_count, 1);
        assert_eq!(summary.software_only_count, 1);
    }

    #[test]
    fn hdr_tone_mapping_readiness_summary_uses_hdr_tonemap_envelopes() {
        let summary = summarize_hdr_tone_mapping_readiness(Some("host-a"), &[]);
        assert_eq!(summary.status, "unknown");
        assert!(
            summary
                .reasons
                .contains(&"hdr_tone_mapping_readiness_probe_not_run".to_string())
        );

        let mut envelope = envelope_fixture();
        envelope.hardware_api = None;
        envelope.support_decision = PlaybackSupportDecision::SoftwareOnly;
        envelope.workload_class_id =
            "video_transcode:hevc:1080p:h264:720p:hdr_tonemap:sub_none:software_decode_software_filter_software_encode:1080p_hdr_tonemap_downscale".to_string();
        envelope.performance_decision = PlaybackPerformanceDecision::RealtimeSafe;
        envelope.reasons = vec![
            "playback_readiness_probe".to_string(),
            "hdr_tone_mapping_required".to_string(),
        ];
        let summary = summarize_hdr_tone_mapping_readiness(Some("host-a"), &[envelope]);
        assert_eq!(summary.status, "ready");
        assert_eq!(summary.hdr_tonemap_envelope_count, 1);
        assert_eq!(summary.realtime_safe_count, 1);
        assert_eq!(summary.software_only_count, 1);
        assert_eq!(summary.hardware_accelerated_count, 0);
    }

    #[test]
    fn observation_timestamps_must_be_rfc3339_utc_compatible() {
        assert!(validate_observation_timestamp("2026-07-01T00:00:00Z").is_ok());
        assert!(validate_observation_timestamp("not-a-time").is_err());
    }
}
