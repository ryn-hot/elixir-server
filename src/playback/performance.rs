use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use dashmap::DashSet;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{AnyPool, Row};
use tempfile::Builder as TempDirBuilder;
use tokio::{process::Command, time::timeout};
use tracing::warn;
use uuid::Uuid;

use crate::{
    metrics::PLAYBACK_PERFORMANCE_PROBE_DURATION,
    playback::{
        HlsOutputLayout, TranscodeParams, build_transcode_ffmpeg_args,
        certification::{CaseReport, CaseStatus, CertificationReport},
        detect_text_subtitles,
        hardware::{HardwareReadinessRecord, load_current_hardware_readiness_records},
        plan::{
            HdrAction, PlaybackFeasibilityAction, PlaybackPerformanceConfidence,
            PlaybackPerformanceDecision, PlaybackPerformanceEnvelope, PlaybackPlan,
            PlaybackSupportDecision, StreamAction,
        },
        probe_video_fps,
    },
};

const LOCAL_PROBE_OUTPUT_SECONDS: f64 = 1.0;
const REALTIME_SAFE_THRESHOLD: f64 = 1.25;
const REALTIME_MARGINAL_THRESHOLD: f64 = 1.0;
const LIVE_OBSERVATION_DECISION_MIN_SAMPLES: i64 = 3;
const LIVE_OBSERVATION_UNSAFE_FAILURES: i64 = 3;

fn current_elixir_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
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

    let ffmpeg_program = readiness_record
        .and_then(|record| record.ffmpeg_path.as_deref())
        .unwrap_or("ffmpeg");
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
            readiness_record,
            plan,
            PlaybackSupportDecision::Supported,
            performance_decision_for_realtime_factor(realtime_factor),
            Some(realtime_factor),
            0,
            vec!["bounded_local_probe".to_string()],
            Vec::new(),
        )
    } else if error_indicates_unsupported_hardware(&stderr) {
        envelope_from_probe_result(
            host_fingerprint,
            readiness_record,
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
        )
    } else {
        envelope_from_probe_result(
            host_fingerprint,
            readiness_record,
            plan,
            support_decision_for_probe_plan(plan),
            PlaybackPerformanceDecision::Unknown,
            None,
            1,
            vec!["bounded_local_probe_failed".to_string()],
            vec![tail(&stderr)],
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
    let envelope = envelope_from_probe_result(
        host_fingerprint,
        readiness_record,
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
    );
    upsert_playback_performance_envelope(pool, &envelope).await?;
    Ok(())
}

fn envelope_from_probe_result(
    host_fingerprint: &str,
    readiness_record: Option<&HardwareReadinessRecord>,
    plan: &PlaybackPlan,
    support_decision: PlaybackSupportDecision,
    performance_decision: PlaybackPerformanceDecision,
    realtime_factor: Option<f64>,
    failure_count: i64,
    reasons: Vec<String>,
    warnings: Vec<String>,
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
        os_family: readiness_record
            .map(|record| record.os_family.clone())
            .unwrap_or_else(|| std::env::consts::OS.to_string()),
        os_version: readiness_record.and_then(|record| record.os_version.clone()),
        gpu_vendor: readiness_record.and_then(|record| record.gpu_vendor.clone()),
        gpu_model: readiness_record.and_then(|record| record.gpu_model.clone()),
        gpu_driver_version: readiness_record.and_then(|record| record.gpu_driver_version.clone()),
        hardware_api: plan.hardware_acceleration.api.clone(),
        ffmpeg_path: readiness_record.and_then(|record| record.ffmpeg_path.clone()),
        ffmpeg_version: readiness_record.and_then(|record| record.ffmpeg_version.clone()),
        ffmpeg_sha256: readiness_record.and_then(|record| record.ffmpeg_sha256.clone()),
        elixir_version: Some(current_elixir_version().to_string()),
        workload_class_id: workload.class_id.clone(),
        pipeline_signature: workload.pipeline_signature.clone(),
        support_decision,
        performance_decision,
        confidence: PlaybackPerformanceConfidence::LocalBenchmark,
        p50_realtime_factor_millis: realtime_millis,
        p95_realtime_factor_millis: realtime_millis,
        startup_latency_ms: None,
        first_segment_latency_ms: None,
        failure_count,
        sample_count: 1,
        invalidation_fingerprint: runtime_invalidation_fingerprint(
            host_fingerprint,
            readiness_record,
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
        vec!["update_driver_or_use_original_quality".to_string()]
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
        config::DatabaseConfig,
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

    #[test]
    fn observation_timestamps_must_be_rfc3339_utc_compatible() {
        assert!(validate_observation_timestamp("2026-07-01T00:00:00Z").is_ok());
        assert!(validate_observation_timestamp("not-a-time").is_err());
    }
}
