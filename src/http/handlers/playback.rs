use std::{
    cmp::Ordering,
    collections::VecDeque,
    path::{Path, PathBuf},
    time::Duration,
};

use axum::{
    Json,
    body::Body,
    extract::{Path as AxumPath, Query, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{Row, any::AnyRow};
use uuid::Uuid;

use crate::{
    config::RunEnvironment,
    db::models::MediaType,
    http::{
        auth::CurrentUser,
        error::{ApiError, ApiResult},
    },
    media::ffprobe,
    metrics::{
        PLAY_DECISIONS, PLAY_ERRORS, PLAY_LATENCY, PLAYBACK_CAPACITY_LEVELS,
        PLAYBACK_CAPACITY_REJECTIONS, PLAYBACK_DECISIONS, PLAYBACK_ERRORS,
        PLAYBACK_FEASIBILITY_DECISIONS, PLAYBACK_HLS_PLAYLIST_STARTUP_LATENCY,
        PLAYBACK_MISSING_SEGMENTS, PLAYBACK_PERFORMANCE_ENVELOPE_STATUS, PLAYBACK_SEGMENT_LATENCY,
        PLAYBACK_SESSION_EXPIRATIONS, PLAYBACK_TRANSCODE_DOWNGRADED,
        PLAYBACK_TRANSCODE_REALTIME_FACTOR, PLAYBACK_TRANSCODE_REJECTED, SEGMENT_SERVED,
        TRANSCODE_DURATION, TRANSCODE_ERRORS, TRANSCODE_STARTS,
    },
    network::registry::ensure_server_instance,
    playback::{
        ArtifactKind, HLS_SEGMENT_SECONDS, PlaybackArtifact, PlaybackJobPlan, SubtitleInfo,
        TranscodeParams,
        decision::{PlaybackSelection, SubtitleSelectionMode, plan_playback},
        hardware::{
            HardwareCapabilities, HardwareDetectionConfig, HardwarePreference,
            HardwareProviderCandidate, HardwareReadinessRecord, HardwareReadinessStatus,
            collect_host_hardware_inventory, hardware_capabilities_from_readiness,
            hardware_provider_candidates, host_hardware_fingerprint,
            load_current_hardware_readiness_records,
        },
        jobs::playback_temp_root,
        performance::load_playback_performance_envelopes,
        plan::{
            AdaptiveLadderPlan, AdaptiveRungPlan, Delivery, HardwareAccelerationPlan,
            PlaybackFeasibilityAction, PlaybackMode, PlaybackPerformanceEnvelope, PlaybackPlan,
            StreamAction,
        },
        probe::{
            MediaCapabilities, MediaProbeError, SubtitleKind, SubtitleStreamCapabilities,
            canonical_subtitle_codec, ensure_media_file_probe, subtitle_kind,
        },
        profile::{
            AbrSupportType, AssComplexitySupport, ClientKind, ClientPlaybackProfile,
            DefaultSubtitlePolicy, EffectivePlaybackPolicy, ForcedSubtitlePolicy,
            ImageSubtitleSupport, NetworkClass, NetworkPlaybackPolicy, QualityMode,
            ServerPlaybackPolicy, SubtitleBurnPolicy, SubtitleRendering, UnknownPerformancePolicy,
            derive_effective_playback_policy,
        },
        range::{
            DirectFileBody, DirectReadMetricLabels, build_direct_file_response, content_type_for,
            direct_file_body, record_direct_stream_range_status,
        },
    },
    state::AppState,
};
use tokio::time::sleep;
use tokio::{
    fs::{self, File},
    io::{AsyncReadExt, AsyncSeekExt, SeekFrom},
    process::Command,
};
use tracing::{info, warn};

#[derive(Debug, Deserialize)]
pub struct PlayRequest {
    pub media_item_id: String,
    pub preferred_file_id: Option<String>,
    pub preferred_episode_id: Option<String>,
    pub network_type: Option<String>,
    #[serde(alias = "shareId")]
    pub share_id: Option<String>,
    pub client_capabilities: Option<Value>,
    #[serde(alias = "audioStreamIndex")]
    pub audio_stream_index: Option<i32>,
    #[serde(alias = "subtitleStreamIndex")]
    pub subtitle_stream_index: Option<i32>,
    #[serde(alias = "subtitleMode")]
    pub subtitle_mode: Option<String>,
    #[serde(alias = "preferredSubtitleLanguage")]
    pub preferred_subtitle_language: Option<String>,
    #[serde(alias = "preferredSubtitleTitle")]
    pub preferred_subtitle_title: Option<String>,
    pub start_position_seconds: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct PlayResponse {
    pub session_id: String,
    pub mode: &'static str,
    pub delivery: &'static str,
    pub stream_url: String,
    pub subtitle_url: Option<String>,
    pub duration_seconds: Option<i32>,
    pub logical_start_seconds: i32,
    pub server_seek_required: bool,
    pub adaptive: bool,
    pub decision_reason: String,
    pub decision_reasons: Vec<String>,
    pub playback_plan: Value,
    pub media_file_id: String,
    pub selected_episode_id: Option<String>,
    pub episode_selection_reason: Option<String>,
    pub server_id: String,
    pub wan_direct_endpoint: Option<String>,
    pub stream_token_expires_at: String,
    pub remote_access: RemoteAccessContract,
    pub remote_policy: RemotePlaybackPolicySnapshot,
    pub state: String,
    pub logical_position_seconds: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteAccessContract {
    pub lan_direct_endpoint: Option<String>,
    pub wan_direct_endpoint: Option<String>,
    pub reverse_proxy_endpoint: Option<String>,
    pub reverse_proxy_behavior: String,
    pub https_required: bool,
    pub secure_connection_policy: String,
    pub request_transport: String,
    pub token_ttl_seconds: u64,
    pub stream_token_expires_at: String,
    pub session_revocation: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemotePlaybackPolicySnapshot {
    pub applied: bool,
    pub scope: String,
    pub policy_sources: Vec<String>,
    pub user_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub share_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_remote_bitrate_bps: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_resolution: Option<String>,
    #[serde(default = "default_remote_policy_true")]
    pub allow_downloads: bool,
    #[serde(default = "default_remote_policy_true")]
    pub allow_direct_play: bool,
    #[serde(default = "default_remote_policy_true")]
    pub allow_transcode: bool,
    #[serde(default = "default_remote_policy_true")]
    pub allow_hardware_transcode: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_sessions: Option<u32>,
    #[serde(default)]
    pub reasons: Vec<String>,
}

fn default_remote_policy_true() -> bool {
    true
}

#[derive(Debug, Deserialize, Default)]
pub struct HardwareReadinessQuery {
    pub diagnostics: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct HardwareReadinessResponse {
    pub enabled: bool,
    pub warmed: bool,
    pub host_fingerprint: Option<String>,
    pub candidates: Vec<HardwareProviderCandidate>,
    pub records: Vec<HardwareReadinessRecordResponse>,
    pub capabilities: HardwareCapabilities,
}

#[derive(Debug, Serialize)]
pub struct HardwareReadinessRecordResponse {
    pub id: String,
    pub accelerator_id: String,
    pub api: String,
    pub status: HardwareReadinessStatus,
    pub status_reason: String,
    pub user_message_code: String,
    pub gpu_vendor: Option<String>,
    pub gpu_model: Option<String>,
    pub gpu_driver_version: Option<String>,
    pub capabilities: crate::playback::hardware::HardwareCapabilityMatrix,
    pub probe_report: crate::playback::hardware::HardwareProbeReport,
    pub stale: bool,
    pub last_checked_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_error_excerpt: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct HardwareWarningResponse {
    pub accelerator_id: String,
    pub api: String,
    pub status: HardwareReadinessStatus,
    pub user_message_code: String,
    pub status_reason: String,
}

pub async fn hardware_readiness(
    State(state): State<AppState>,
    _user: CurrentUser,
    Query(query): Query<HardwareReadinessQuery>,
) -> ApiResult<Json<HardwareReadinessResponse>> {
    if !state.settings.playback.hardware_acceleration_enabled {
        let warmed = state.hardware_capabilities.read().await.is_some();
        return Ok(Json(HardwareReadinessResponse {
            enabled: false,
            warmed,
            host_fingerprint: None,
            candidates: Vec::new(),
            records: Vec::new(),
            capabilities: HardwareCapabilities::software_only(),
        }));
    }

    let diagnostics = query.diagnostics.unwrap_or(false);
    let config = HardwareDetectionConfig {
        preference: HardwarePreference::parse(&state.settings.playback.hardware_acceleration),
    };
    let inventory = collect_host_hardware_inventory().await;
    let host_fingerprint = host_hardware_fingerprint(&inventory);
    let candidates = hardware_provider_candidates(&inventory, config.preference);
    let records = load_current_hardware_readiness_records(&state.db_pool, &host_fingerprint)
        .await
        .map_err(|err| ApiError::internal(err.to_string()))?;
    let warmed_capabilities = state.hardware_capabilities.read().await.clone();
    let warmed = warmed_capabilities.is_some();
    let capabilities = warmed_capabilities
        .unwrap_or_else(|| hardware_capabilities_from_readiness(&inventory, &records));

    Ok(Json(HardwareReadinessResponse {
        enabled: true,
        warmed,
        host_fingerprint: Some(host_fingerprint),
        candidates,
        records: records
            .iter()
            .map(|record| readiness_record_response(record, diagnostics))
            .collect(),
        capabilities,
    }))
}

pub async fn hardware_warnings(
    State(state): State<AppState>,
    _user: CurrentUser,
) -> ApiResult<Json<Vec<HardwareWarningResponse>>> {
    if !state.settings.playback.hardware_acceleration_enabled {
        return Ok(Json(Vec::new()));
    }

    let inventory = collect_host_hardware_inventory().await;
    let host_fingerprint = host_hardware_fingerprint(&inventory);
    let records = load_current_hardware_readiness_records(&state.db_pool, &host_fingerprint)
        .await
        .map_err(|err| ApiError::internal(err.to_string()))?;
    let warnings = records
        .iter()
        .filter(|record| {
            !matches!(
                record.status,
                HardwareReadinessStatus::Available
                    | HardwareReadinessStatus::DisabledByConfig
                    | HardwareReadinessStatus::NotApplicable
            )
        })
        .map(|record| HardwareWarningResponse {
            accelerator_id: record.accelerator_id.clone(),
            api: record.api.clone(),
            status: record.status,
            user_message_code: record.user_message_code.clone(),
            status_reason: record.status_reason.clone(),
        })
        .collect();
    Ok(Json(warnings))
}

fn readiness_record_response(
    record: &HardwareReadinessRecord,
    diagnostics: bool,
) -> HardwareReadinessRecordResponse {
    HardwareReadinessRecordResponse {
        id: record.id.clone(),
        accelerator_id: record.accelerator_id.clone(),
        api: record.api.clone(),
        status: record.status,
        status_reason: record.status_reason.clone(),
        user_message_code: record.user_message_code.clone(),
        gpu_vendor: record.gpu_vendor.clone(),
        gpu_model: record.gpu_model.clone(),
        gpu_driver_version: record.gpu_driver_version.clone(),
        capabilities: record.capabilities.clone(),
        probe_report: record.probe_report.clone(),
        stale: record.stale,
        last_checked_at: record.last_checked_at.to_rfc3339(),
        raw_error_excerpt: diagnostics
            .then(|| record.raw_error_excerpt.clone())
            .flatten(),
    }
}

#[derive(Debug, Clone)]
pub struct EffectiveProfile {
    pub max_resolution: String,
    pub supported_containers: Vec<String>,
    pub supported_video_codecs: Vec<String>,
    pub supported_audio_codecs: Vec<String>,
    pub max_bitrate_bps: Option<i64>,
}
#[derive(Debug, Deserialize, Clone, Default)]
pub struct ClientCapabilities {
    pub profile_version: Option<u32>,
    pub client_kind: Option<String>,
    pub direct_play_preferred: Option<bool>,
    pub max_resolution: Option<String>,
    pub supported_containers: Option<Vec<String>>,
    pub supported_video_codecs: Option<Vec<String>>,
    pub supported_audio_codecs: Option<Vec<String>>,
    pub supported_subtitle_codecs: Option<Vec<String>>,
    pub supported_hls_segment_types: Option<Vec<String>>,
    pub max_audio_channels: Option<i32>,
    pub supports_hdr: Option<bool>,
    pub supports_hdr10_plus: Option<bool>,
    pub supports_dolby_vision: Option<bool>,
    pub supports_server_side_hls_seek: Option<bool>,
    pub supports_auth_headers_for_media: Option<bool>,
    pub subtitle_burn_policy: Option<String>,
    pub subtitle_rendering: Option<String>,
    pub ass_complexity_support: Option<String>,
    pub image_subtitle_support: Option<String>,
    pub forced_subtitle_policy: Option<String>,
    pub default_subtitle_policy: Option<String>,
    pub subtitle_mode: Option<String>,
    pub preferred_subtitle_language: Option<String>,
    pub preferred_subtitle_title: Option<String>,
    #[serde(alias = "qualityMode")]
    pub quality_mode: Option<String>,
    #[serde(alias = "fixedBitrateBps")]
    pub fixed_bitrate_bps: Option<i64>,
    #[serde(alias = "fixedResolution")]
    pub fixed_resolution: Option<String>,
    #[serde(alias = "automaticMinBitrateBps")]
    pub automatic_min_bitrate_bps: Option<i64>,
    #[serde(alias = "automaticMaxBitrateBps")]
    pub automatic_max_bitrate_bps: Option<i64>,
    #[serde(alias = "automaticMinResolution")]
    pub automatic_min_resolution: Option<String>,
    #[serde(alias = "automaticMaxResolution")]
    pub automatic_max_resolution: Option<String>,
    #[serde(alias = "abrSupportType")]
    pub abr_support_type: Option<String>,
    pub app_version: Option<String>,
    #[serde(alias = "maxBitrateBps")]
    pub max_bitrate_bps: Option<i64>,
}

#[derive(Debug, Clone)]
struct MediaItemRow {
    r#type: MediaType,
    runtime_seconds: Option<i32>,
}

#[derive(Debug, Clone)]
struct FileRow {
    id: String,
    path: String,
    container: Option<String>,
    video_codec: Option<String>,
    audio_codec: Option<String>,
    width: i32,
    height: i32,
    bitrate_bps: i64,
    size_bytes: Option<i64>,
}

pub async fn play(
    State(state): State<AppState>,
    headers: HeaderMap,
    user: CurrentUser,
    Json(body): Json<PlayRequest>,
) -> ApiResult<Json<PlayResponse>> {
    let latency_timer = PLAY_LATENCY.with_label_values(&["pending"]).start_timer();
    let movie = sqlx::query("SELECT runtime_seconds FROM movies WHERE id = ? LIMIT 1")
        .bind(&body.media_item_id)
        .fetch_optional(&state.db_pool)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;

    let item = if let Some(row) = movie {
        MediaItemRow {
            r#type: MediaType::Movie,
            runtime_seconds: row
                .try_get::<i64, _>("runtime_seconds")
                .ok()
                .map(|v| v as i32),
        }
    } else {
        let series = sqlx::query("SELECT library_type FROM series WHERE id = ? LIMIT 1")
            .bind(&body.media_item_id)
            .fetch_optional(&state.db_pool)
            .await
            .map_err(|e| ApiError::internal(e.to_string()))?;
        let series = series.ok_or_else(|| ApiError::not_found("media item not found"))?;
        MediaItemRow {
            r#type: item_type(series.get::<String, _>("library_type").as_str())
                .unwrap_or(MediaType::Series),
            runtime_seconds: None,
        }
    };

    let requested_file_id = body
        .preferred_file_id
        .as_deref()
        .map(str::trim)
        .filter(|id| !id.is_empty());
    let requested_episode_id = body
        .preferred_episode_id
        .as_deref()
        .map(str::trim)
        .filter(|id| !id.is_empty());
    if requested_file_id.is_some() && requested_episode_id.is_some() {
        return Err(ApiError::bad_request(
            "provide either preferred_file_id or preferred_episode_id, not both",
        ));
    }
    if matches!(item.r#type, MediaType::Movie) && requested_episode_id.is_some() {
        return Err(ApiError::bad_request(
            "preferred_episode_id is only valid for series items",
        ));
    }

    let explicit_episode_id = if matches!(item.r#type, MediaType::Movie) {
        None
    } else if let Some(episode_id) = requested_episode_id {
        let episode = sqlx::query_scalar::<_, String>(
            "SELECT id FROM episodes WHERE id = ? AND series_id = ? LIMIT 1",
        )
        .bind(episode_id)
        .bind(&body.media_item_id)
        .fetch_optional(&state.db_pool)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;
        Some(episode.ok_or_else(|| ApiError::not_found("episode not found for item"))?)
    } else if let Some(file_or_legacy_episode_id) = requested_file_id {
        sqlx::query_scalar::<_, String>(
            "SELECT id FROM episodes WHERE id = ? AND series_id = ? LIMIT 1",
        )
        .bind(file_or_legacy_episode_id)
        .bind(&body.media_item_id)
        .fetch_optional(&state.db_pool)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?
    } else {
        None
    };
    let auto_episode_choice = if !matches!(item.r#type, MediaType::Movie)
        && explicit_episode_id.is_none()
        && requested_file_id.is_none()
    {
        select_series_episode_for_playback(&state, &body.media_item_id, user.user_id).await?
    } else {
        None
    };
    let scoped_episode_id = explicit_episode_id.clone().or_else(|| {
        auto_episode_choice
            .as_ref()
            .map(|choice| choice.episode_id.clone())
    });
    let episode_selection_reason = explicit_episode_id
        .as_ref()
        .map(|_| "explicit_episode_requested".to_string())
        .or_else(|| {
            auto_episode_choice
                .as_ref()
                .map(|choice| choice.reason.clone())
        });
    let preferred_file_id = if scoped_episode_id.is_some() {
        None
    } else {
        requested_file_id
    };

    let rows = match item.r#type {
        MediaType::Movie => {
            sqlx::query(
                "SELECT mf.id, mf.path, mf.container, mf.video_codec, mf.audio_codec, COALESCE(mf.width, 0) as width, COALESCE(mf.height, 0) as height, COALESCE(mf.bitrate_bps, 0) as bitrate_bps, mf.size_bytes FROM media_files mf JOIN movie_files mlf ON mlf.media_file_id = mf.id WHERE mlf.movie_id = ? AND mf.scan_state = 'ok'",
            )
            .bind(&body.media_item_id)
            .fetch_all(&state.db_pool)
            .await
        }
        _ if scoped_episode_id.is_some() => {
            sqlx::query(
                "SELECT DISTINCT mf.id, mf.path, mf.container, mf.video_codec, mf.audio_codec, COALESCE(mf.width, 0) as width, COALESCE(mf.height, 0) as height, COALESCE(mf.bitrate_bps, 0) as bitrate_bps, mf.size_bytes FROM media_files mf JOIN episode_files ef ON ef.media_file_id = mf.id JOIN episodes e ON e.id = ef.episode_id WHERE e.series_id = ? AND e.id = ? AND mf.scan_state = 'ok'",
            )
            .bind(&body.media_item_id)
            .bind(scoped_episode_id.as_deref().unwrap_or_default())
            .fetch_all(&state.db_pool)
            .await
        }
        _ => {
            sqlx::query(
                "SELECT DISTINCT mf.id, mf.path, mf.container, mf.video_codec, mf.audio_codec, COALESCE(mf.width, 0) as width, COALESCE(mf.height, 0) as height, COALESCE(mf.bitrate_bps, 0) as bitrate_bps, mf.size_bytes FROM media_files mf JOIN episode_files ef ON ef.media_file_id = mf.id JOIN episodes e ON e.id = ef.episode_id WHERE e.series_id = ? AND mf.scan_state = 'ok'",
            )
            .bind(&body.media_item_id)
            .fetch_all(&state.db_pool)
            .await
        }
    }
    .map_err(|e| ApiError::internal(e.to_string()))?;

    let mut files = Vec::new();
    for row in rows {
        files.push(FileRow {
            id: row.get::<String, _>("id"),
            path: row.get::<String, _>("path"),
            container: row.try_get::<String, _>("container").ok(),
            video_codec: row.try_get::<String, _>("video_codec").ok(),
            audio_codec: row.try_get::<String, _>("audio_codec").ok(),
            width: row.get::<i64, _>("width") as i32,
            height: row.get::<i64, _>("height") as i32,
            bitrate_bps: row.get::<i64, _>("bitrate_bps"),
            size_bytes: row.try_get::<i64, _>("size_bytes").ok(),
        });
    }

    if files.is_empty() {
        let message = if scoped_episode_id.is_some() {
            "no playable files for episode"
        } else {
            "no playable files for item"
        };
        return Err(ApiError::not_found(message));
    }

    let network_class = classify_playback_network(body.network_type.as_deref());
    let share_id = validated_share_id(body.share_id.as_deref())?;
    let request_transport = playback_request_transport(&headers);
    enforce_remote_transport_policy(&state.settings, network_class, &request_transport)?;
    let profile = profile_for_network(&state.settings.playback, Some(network_class.as_str()));
    let caps_json = body.client_capabilities.clone();
    let mut caps = caps_json
        .clone()
        .and_then(|v| serde_json::from_value::<ClientCapabilities>(v).ok())
        .unwrap_or_else(|| {
            default_capabilities(
                &state.settings.playback,
                Some(network_class.as_str()),
                &profile,
            )
        });
    // Intersect client caps with profile caps to be conservative.
    caps = merge_caps_with_profile(caps, &profile);

    let selected = select_file(
        &files,
        preferred_file_id,
        &caps,
        &profile,
        Some(network_class.as_str()),
        item.runtime_seconds,
    )
    .ok_or_else(|| ApiError::not_found("requested file not found"))?;

    let mut media_capabilities =
        ensure_media_file_probe(&state.db_pool, &selected.id, &selected.path)
            .await
            .map_err(playback_probe_error)?;
    attach_external_subtitles(&state, &selected.id, &mut media_capabilities).await?;
    let client_profile = client_playback_profile_from_caps(&caps);
    if !playback_plan_contract_allowed(&state.settings) {
        record_playback_error_labels(
            "blocked",
            "none",
            client_kind_label(&client_profile.client_kind),
            network_class.as_str(),
            "plan_contract_disabled",
            "software",
        );
        return Err(ApiError::conflict_code(
            "playback_plan_contract_disabled",
            "playback planner is not enabled for this environment",
            serde_json::json!({
                "flag": "playback.plan_contract_enabled",
                "environment": state.settings.environment.as_str(),
                "retry": {
                    "allowed": false,
                    "strategy": "enable_release_gate"
                }
            }),
        ));
    }
    let hardware_capabilities = if state.settings.playback.hardware_acceleration_enabled {
        state
            .hardware_capabilities
            .read()
            .await
            .clone()
            .unwrap_or_else(|| {
                warn!(
                    "playback hardware readiness is not warm yet; using software fallback for this request"
                );
                HardwareCapabilities::software_only()
            })
    } else {
        HardwareCapabilities::software_only()
    };
    let mut effective_policy = effective_playback_policy_from_config(
        &state.settings.playback,
        &profile,
        &client_profile,
        network_class,
        hardware_capabilities,
    );
    let remote_policy = resolve_remote_playback_policy(
        &state.settings.playback,
        user.user_id,
        share_id.as_deref(),
        network_class,
    );
    apply_remote_policy_to_effective_policy(&mut effective_policy, &remote_policy);
    let hardware_host_fingerprint = state.hardware_host_fingerprint.read().await.clone();
    if let Some(host_fingerprint) = hardware_host_fingerprint.as_deref() {
        effective_policy.performance_envelopes =
            load_playback_performance_envelopes(&state.db_pool, host_fingerprint)
                .await
                .map_err(|err| ApiError::internal(err.to_string()))?;
        record_playback_performance_envelopes(&effective_policy.performance_envelopes);
    }
    effective_policy.active_video_transcodes = active_video_transcode_count(&state).await?;
    let start_position_seconds = validated_start_position_seconds(body.start_position_seconds)?;
    let subtitle_mode = body
        .subtitle_mode
        .as_deref()
        .or(caps.subtitle_mode.as_deref());
    let preferred_subtitle_language = body
        .preferred_subtitle_language
        .clone()
        .or_else(|| caps.preferred_subtitle_language.clone());
    let preferred_subtitle_title = body
        .preferred_subtitle_title
        .clone()
        .or_else(|| caps.preferred_subtitle_title.clone());
    let mut playback_plan = plan_playback(
        &selected.id,
        &media_capabilities,
        PlaybackSelection {
            audio_stream_index: body.audio_stream_index,
            subtitle_stream_index: body.subtitle_stream_index,
            subtitle_mode: subtitle_selection_mode(subtitle_mode),
            preferred_subtitle_language,
            preferred_subtitle_title,
            start_position_seconds,
        },
        &client_profile,
        &effective_policy,
    );
    if let Some(host_fingerprint) = hardware_host_fingerprint.as_deref() {
        state
            .playback_performance_probes
            .queue_missing_workload_probe(
                state.db_pool.clone(),
                selected.path.clone(),
                host_fingerprint.to_string(),
                &mut playback_plan,
            );
    }
    append_remote_policy_plan_reasons(
        &mut playback_plan,
        &remote_policy,
        &request_transport,
        &state.settings,
    );
    record_playback_feasibility_for_plan(
        &playback_plan,
        client_kind_label(&client_profile.client_kind),
    );
    if !playback_plan.playable {
        let error_code = playback_error_code_for_plan(&playback_plan);
        let details = if error_code == "transcode_capacity_exhausted" {
            playback_capacity_retry_details(&playback_plan)
        } else {
            playback_not_playable_details(&playback_plan)
        };
        record_playback_error_for_plan(
            &playback_plan,
            client_kind_label(&client_profile.client_kind),
            network_class.as_str(),
            error_code,
        );
        return Err(ApiError::conflict_code(
            error_code,
            format!("{error_code}: {}", playback_plan.decision_reason()),
            details,
        ));
    }
    if let Some(error) = playback_rollout_error(
        &state.settings.playback,
        &playback_plan,
        client_kind_label(&client_profile.client_kind),
        network_class.as_str(),
    ) {
        return Err(error);
    }
    if let Some(error) = enforce_remote_policy_session_limit(
        &state,
        &remote_policy,
        &playback_plan,
        client_kind_label(&client_profile.client_kind),
        network_class.as_str(),
    )
    .await?
    {
        return Err(error);
    }
    if let Some(error) = enforce_playback_capacity_before_start(
        &state,
        &user,
        &playback_plan,
        client_kind_label(&client_profile.client_kind),
        network_class.as_str(),
    )
    .await?
    {
        return Err(error);
    }
    if playback_plan.mode.is_hls_producing() {
        let hardware_label = hardware_metric_label(&playback_plan.hardware_acceleration);
        TRANSCODE_STARTS
            .with_label_values(&[
                "pending",
                selected.container.as_deref().unwrap_or("unknown"),
                selected.video_codec.as_deref().unwrap_or("unknown"),
                hardware_label,
            ])
            .inc();
    }
    let session_id = Uuid::new_v4();
    let session_token = Uuid::new_v4().to_string();
    let (stream_token_ttl_seconds, stream_token_expires_at) =
        stream_token_expiry(&state.settings.playback);
    info!(
        user = %user.user_id,
        item = %body.media_item_id,
        requested_file = ?requested_file_id,
        requested_episode = ?requested_episode_id,
        episode = ?scoped_episode_id,
        file = %selected.id,
        mode = %playback_plan.mode.as_str(),
        delivery = %playback_plan.delivery.as_str(),
        network = network_class.as_str(),
        reason = %playback_plan.decision_reason(),
        "play decision"
    );
    let decision_reason = playback_plan.decision_reason();
    PLAY_DECISIONS
        .with_label_values(&[
            playback_plan.mode.as_str(),
            network_class.as_str(),
            selected.container.as_deref().unwrap_or("unknown"),
            selected.video_codec.as_deref().unwrap_or("unknown"),
        ])
        .inc();
    PLAYBACK_DECISIONS
        .with_label_values(&[
            playback_plan.mode.as_str(),
            playback_plan.delivery.as_str(),
            client_kind_label(&client_profile.client_kind),
            network_class.as_str(),
            &decision_reason,
            hardware_metric_label(&playback_plan.hardware_acceleration),
        ])
        .inc();

    let server_id = ensure_server_instance(&state.db_pool, &state.settings, user.user_id).await?;
    let remote_access = remote_access_contract(
        &state,
        &server_id.to_string(),
        stream_token_ttl_seconds,
        &stream_token_expires_at,
        &request_transport,
    )
    .await;
    let wan_direct_endpoint = remote_access.wan_direct_endpoint.clone();

    let stream_url = if playback_plan.mode == PlaybackMode::DirectPlay {
        format!(
            "/stream/direct/{}?sid={}&session={}",
            selected.id, session_id, session_token
        )
    } else {
        format!(
            "/sessions/{}/master.m3u8?session={}",
            session_id, session_token
        )
    };
    let subtitle_url = direct_play_sidecar_subtitle_url(
        &playback_plan,
        &media_capabilities,
        &selected.id,
        &session_id.to_string(),
        &session_token,
    );

    let resolved_duration = resolve_duration_seconds(
        &state,
        &body.media_item_id,
        &selected.path,
        item.runtime_seconds,
        item.r#type,
    )
    .await;

    let transcode_state = if playback_plan.mode.is_hls_producing() {
        Some(serde_json::json!({
            "seek_seconds": start_position_seconds.unwrap_or(0) as f32,
            "mode": playback_plan.mode.as_str(),
            "delivery": playback_plan.delivery.as_str(),
        }))
    } else {
        None
    };
    let mut playback_plan_json =
        serde_json::to_value(&playback_plan).map_err(|e| ApiError::internal(e.to_string()))?;
    if let Some(plan) = playback_plan_json.as_object_mut() {
        plan.insert(
            "remote_access".to_string(),
            serde_json::to_value(&remote_access).map_err(|e| ApiError::internal(e.to_string()))?,
        );
        plan.insert(
            "remote_policy".to_string(),
            serde_json::to_value(&remote_policy).map_err(|e| ApiError::internal(e.to_string()))?,
        );
        plan.insert(
            "stream_token_expires_at".to_string(),
            Value::String(stream_token_expires_at.clone()),
        );
    }
    let remote_policy_json =
        serde_json::to_string(&remote_policy).map_err(|e| ApiError::internal(e.to_string()))?;
    let job_state_json = transcode_state.clone();

    sqlx::query::<sqlx::Any>("INSERT INTO playback_sessions (id, user_id, server_id, media_file_id, mode, state, network_type, logical_position_seconds, duration_seconds, client_capabilities, transcode_state, token, token_expires_at, share_id, remote_policy_json, playback_plan_json, job_state_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .bind(session_id.to_string())
        .bind(user.user_id.to_string())
        .bind(server_id.to_string())
        .bind(&selected.id)
        .bind(playback_plan.mode.as_str())
        .bind("active")
        .bind(Some(network_class.as_str().to_string()))
        .bind(start_position_seconds.unwrap_or(0) as f32)
        .bind(resolved_duration)
        .bind(caps_json.as_ref().map(|v| v.to_string()))
        .bind(transcode_state.as_ref().map(|s| s.to_string()))
        .bind(session_token.clone())
        .bind(stream_token_expires_at.clone())
        .bind(share_id.clone())
        .bind(Some(remote_policy_json))
        .bind(Some(playback_plan_json.to_string()))
        .bind(job_state_json.as_ref().map(|s| s.to_string()))
        .execute(&state.db_pool)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;

    latency_timer.stop_and_record();

    let response = PlayResponse {
        session_id: session_id.to_string(),
        mode: playback_plan.mode.as_str(),
        delivery: playback_plan.delivery.as_str(),
        stream_url,
        subtitle_url,
        duration_seconds: resolved_duration,
        logical_start_seconds: start_position_seconds.unwrap_or(0),
        server_seek_required: playback_plan.server_seek_required(),
        adaptive: playback_plan.adaptive,
        decision_reason,
        decision_reasons: playback_plan.reasons.clone(),
        playback_plan: playback_plan_json,
        media_file_id: selected.id.clone(),
        selected_episode_id: scoped_episode_id,
        episode_selection_reason,
        server_id: server_id.to_string(),
        wan_direct_endpoint,
        stream_token_expires_at,
        remote_access,
        remote_policy,
        state: "active".to_string(),
        logical_position_seconds: start_position_seconds.unwrap_or(0) as f32,
    };

    Ok(Json(response))
}

async fn resolve_duration_seconds(
    state: &AppState,
    media_item_id: &str,
    media_path: &str,
    item_duration: Option<i32>,
    item_type: MediaType,
) -> Option<i32> {
    let probe_duration = match ffprobe::probe(media_path).await {
        Ok(meta) => meta.duration_seconds,
        Err(err) => {
            tracing::warn!(%media_item_id, error = %err, "ffprobe duration lookup failed");
            None
        }
    };

    let Some(actual) = probe_duration else {
        return item_duration;
    };

    let should_replace = match item_duration {
        None => true,
        Some(existing) if existing <= 0 => true,
        Some(existing) => {
            let diff = (existing - actual).abs();
            let rel = diff as f64 / existing.max(1) as f64;
            diff >= 30 && rel >= 0.1
        }
    };

    if should_replace {
        if matches!(item_type, MediaType::Movie) {
            let _ = sqlx::query::<sqlx::Any>(
                "UPDATE movies SET runtime_seconds = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            )
            .bind(actual)
            .bind(media_item_id)
            .execute(&state.db_pool)
            .await;
        }
        let _ = sqlx::query::<sqlx::Any>(
            "UPDATE media_items SET runtime_seconds = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        )
        .bind(actual)
        .bind(media_item_id)
        .execute(&state.db_pool)
        .await;
        return Some(actual);
    }

    item_duration
}

#[derive(Debug, Clone)]
struct EpisodePlaybackChoice {
    episode_id: String,
    reason: String,
}

#[derive(Debug, Clone)]
struct EpisodePlaybackCandidate {
    id: String,
    season_number: i32,
    episode_number: i32,
    absolute_episode_number: Option<i32>,
    position_seconds: f64,
    duration_seconds: Option<f64>,
    last_played_at: Option<String>,
}

impl EpisodePlaybackCandidate {
    fn completed(&self) -> bool {
        let Some(duration) = self.duration_seconds.filter(|duration| *duration > 0.0) else {
            return false;
        };
        self.position_seconds >= duration * 0.9 || duration - self.position_seconds <= 120.0
    }

    fn resumable(&self) -> bool {
        self.last_played_at.is_some() && self.position_seconds >= 30.0 && !self.completed()
    }

    fn order_key(&self) -> (i32, i32, i32) {
        (
            self.season_number,
            self.absolute_episode_number.unwrap_or(self.episode_number),
            self.episode_number,
        )
    }
}

async fn select_series_episode_for_playback(
    state: &AppState,
    series_id: &str,
    user_id: Uuid,
) -> ApiResult<Option<EpisodePlaybackChoice>> {
    let rows = sqlx::query(
        "SELECT
             e.id,
             e.season_number,
             e.episode_number,
             e.absolute_episode_number,
             CAST(COALESCE(MAX(ps.logical_position_seconds), 0) AS REAL) AS position_seconds,
             CAST(MAX(COALESCE(ps.duration_seconds, e.runtime_seconds, 0)) AS REAL) AS duration_seconds,
             MAX(CAST(ps.updated_at AS TEXT)) AS last_played_at
         FROM episodes e
         JOIN episode_files ef ON ef.episode_id = e.id
         JOIN media_files mf ON mf.id = ef.media_file_id
         LEFT JOIN playback_sessions ps
           ON ps.media_file_id = mf.id AND ps.user_id = ?
         WHERE e.series_id = ? AND mf.scan_state = 'ok'
         GROUP BY e.id, e.season_number, e.episode_number, e.absolute_episode_number
         ORDER BY e.season_number ASC,
                  COALESCE(e.absolute_episode_number, e.episode_number) ASC,
                  e.episode_number ASC",
    )
    .bind(user_id.to_string())
    .bind(series_id)
    .fetch_all(&state.db_pool)
    .await
    .map_err(|e| ApiError::internal(e.to_string()))?;

    let mut candidates = Vec::with_capacity(rows.len());
    for row in rows {
        candidates.push(EpisodePlaybackCandidate {
            id: row.get::<String, _>("id"),
            season_number: row.get::<i64, _>("season_number") as i32,
            episode_number: row.get::<i64, _>("episode_number") as i32,
            absolute_episode_number: row
                .try_get::<i64, _>("absolute_episode_number")
                .ok()
                .map(|value| value as i32),
            position_seconds: row.try_get::<f64, _>("position_seconds").unwrap_or(0.0),
            duration_seconds: row
                .try_get::<f64, _>("duration_seconds")
                .ok()
                .filter(|value| *value > 0.0),
            last_played_at: row.try_get::<String, _>("last_played_at").ok(),
        });
    }

    if let Some(candidate) = candidates
        .iter()
        .filter(|candidate| candidate.resumable())
        .max_by(|a, b| a.last_played_at.cmp(&b.last_played_at))
    {
        return Ok(Some(EpisodePlaybackChoice {
            episode_id: candidate.id.clone(),
            reason: "continue_watching_episode".to_string(),
        }));
    }

    if let Some(candidate) = candidates.iter().find(|candidate| !candidate.completed()) {
        return Ok(Some(EpisodePlaybackChoice {
            episode_id: candidate.id.clone(),
            reason: "next_unwatched_episode".to_string(),
        }));
    }

    Ok(candidates
        .iter()
        .min_by_key(|candidate| candidate.order_key())
        .map(|candidate| EpisodePlaybackChoice {
            episode_id: candidate.id.clone(),
            reason: "first_available_episode".to_string(),
        }))
}

fn select_file<'a>(
    files: &'a [FileRow],
    preferred: Option<&str>,
    caps: &ClientCapabilities,
    profile: &EffectiveProfile,
    network: Option<&str>,
    item_duration: Option<i32>,
) -> Option<&'a FileRow> {
    if let Some(pref) = preferred {
        return files.iter().find(|f| f.id == pref);
    }

    files
        .iter()
        .max_by(|a, b| compare_files(a, b, caps, profile, network, item_duration))
}

fn compare_files(
    a: &FileRow,
    b: &FileRow,
    caps: &ClientCapabilities,
    profile: &EffectiveProfile,
    network: Option<&str>,
    item_duration: Option<i32>,
) -> Ordering {
    let cmp_tuple = |f: &FileRow| -> (bool, i32, i64, i64, i32) {
        let native_direct = native_direct_play_client(caps);
        let container_match = caps
            .supported_containers
            .as_ref()
            .map(|c| matches_or_unknown(f.container.as_deref(), c))
            .unwrap_or(true);
        let video_match = caps
            .supported_video_codecs
            .as_ref()
            .map(|c| matches_or_unknown(f.video_codec.as_deref(), c))
            .unwrap_or(true);
        let audio_match = caps
            .supported_audio_codecs
            .as_ref()
            .map(|c| matches_or_unknown(f.audio_codec.as_deref(), c))
            .unwrap_or(true);

        let res_ok = resolution_within_cap(f.height, caps.max_resolution.as_deref());

        let profile_bitrate_cap = (!native_direct)
            .then_some(profile.max_bitrate_bps)
            .flatten();
        let client_bitrate_cap = positive_bitrate_cap(caps.max_bitrate_bps);
        let bitrate_cap = match (network, client_bitrate_cap) {
            (Some("wan"), Some(max)) => Some(max.min(8_000_000)),
            (Some("wan"), None) => Some(8_000_000),
            (Some("lan"), _) => None,
            (_, max) => max.or(profile_bitrate_cap),
        };
        let bitrate_val = effective_bitrate(f, item_duration);
        let bitrate_ok = bitrate_cap.map(|max| bitrate_val <= max).unwrap_or(true);

        let direct_candidate =
            container_match && video_match && audio_match && res_ok && bitrate_ok;

        let codec_score = (container_match as i32) + (video_match as i32) + (audio_match as i32);
        let res_score = {
            let w = f.width as i64;
            let h = f.height as i64;
            if w == 0 || h == 0 { 0 } else { w * h }
        };
        let meta_score = {
            let mut score = 0;
            if f.container.is_some() {
                score += 1;
            }
            if f.video_codec.is_some() {
                score += 1;
            }
            if f.audio_codec.is_some() {
                score += 1;
            }
            score
        };

        // Prefer lower bitrate when capped; otherwise higher.
        let bitrate_pref = match bitrate_cap {
            Some(_) => -(bitrate_val as i64),
            None => bitrate_val as i64,
        };

        (
            direct_candidate,
            codec_score,
            res_score,
            bitrate_pref,
            meta_score,
        )
    };

    cmp_tuple(a).cmp(&cmp_tuple(b))
}

fn item_type(raw: &str) -> ApiResult<MediaType> {
    match raw {
        "movie" => Ok(MediaType::Movie),
        "series" => Ok(MediaType::Series),
        "anime" => Ok(MediaType::Anime),
        _ => Err(ApiError::internal("unknown media type")),
    }
}

fn default_capabilities(
    config: &crate::config::PlaybackConfig,
    network: Option<&str>,
    profile: &EffectiveProfile,
) -> ClientCapabilities {
    let max_bitrate = match network {
        Some("lan") => profile
            .max_bitrate_bps
            .or(config.default_lan_max_bitrate_bps),
        _ => profile
            .max_bitrate_bps
            .or(config.default_wan_max_bitrate_bps),
    };

    ClientCapabilities {
        profile_version: Some(1),
        client_kind: None,
        direct_play_preferred: None,
        max_resolution: Some(profile.max_resolution.clone()),
        supported_containers: Some(profile.supported_containers.clone()),
        supported_video_codecs: Some(profile.supported_video_codecs.clone()),
        supported_audio_codecs: Some(profile.supported_audio_codecs.clone()),
        supported_subtitle_codecs: Some(vec!["webvtt".to_string(), "srt".to_string()]),
        supported_hls_segment_types: Some(vec!["fmp4".to_string(), "mpegts".to_string()]),
        max_audio_channels: Some(2),
        supports_hdr: Some(false),
        supports_hdr10_plus: Some(false),
        supports_dolby_vision: Some(false),
        supports_server_side_hls_seek: Some(true),
        supports_auth_headers_for_media: Some(true),
        subtitle_burn_policy: Some("automatic".to_string()),
        subtitle_rendering: Some("hls_webvtt".to_string()),
        ass_complexity_support: Some("burn_in".to_string()),
        image_subtitle_support: Some("burn_in".to_string()),
        forced_subtitle_policy: Some("matching_audio".to_string()),
        default_subtitle_policy: Some("media_default".to_string()),
        subtitle_mode: Some("default".to_string()),
        preferred_subtitle_language: None,
        preferred_subtitle_title: None,
        quality_mode: Some("fixed".to_string()),
        fixed_bitrate_bps: max_bitrate,
        fixed_resolution: Some(profile.max_resolution.clone()),
        automatic_min_bitrate_bps: Some(800_000),
        automatic_max_bitrate_bps: max_bitrate,
        automatic_min_resolution: Some("360p".to_string()),
        automatic_max_resolution: Some(profile.max_resolution.clone()),
        abr_support_type: Some("hls_js".to_string()),
        app_version: None,
        max_bitrate_bps: max_bitrate,
    }
}

fn merge_caps_with_profile(
    mut caps: ClientCapabilities,
    profile: &EffectiveProfile,
) -> ClientCapabilities {
    if native_direct_play_client(&caps) {
        if caps
            .max_resolution
            .as_deref()
            .is_some_and(is_unlimited_resolution)
        {
            caps.max_resolution = None;
        }
        if caps.max_bitrate_bps.is_some_and(|value| value <= 0) {
            caps.max_bitrate_bps = None;
        }
        return caps;
    }

    // Merge supported containers/codecs by intersection when both present.
    if let Some(client) = caps.supported_containers.as_ref() {
        let merged: Vec<String> = client
            .iter()
            .filter(|c| {
                profile
                    .supported_containers
                    .iter()
                    .any(|p| eq_ci(Some(c.as_str()), Some(p.as_str())))
            })
            .cloned()
            .collect();
        caps.supported_containers = Some(merged);
    } else {
        caps.supported_containers = Some(profile.supported_containers.clone());
    }
    if let Some(client) = caps.supported_video_codecs.as_ref() {
        let merged: Vec<String> = client
            .iter()
            .filter(|c| {
                profile
                    .supported_video_codecs
                    .iter()
                    .any(|p| eq_ci(Some(c.as_str()), Some(p.as_str())))
            })
            .cloned()
            .collect();
        caps.supported_video_codecs = Some(merged);
    } else {
        caps.supported_video_codecs = Some(profile.supported_video_codecs.clone());
    }
    if let Some(client) = caps.supported_audio_codecs.as_ref() {
        let merged: Vec<String> = client
            .iter()
            .filter(|c| {
                profile
                    .supported_audio_codecs
                    .iter()
                    .any(|p| eq_ci(Some(c.as_str()), Some(p.as_str())))
            })
            .cloned()
            .collect();
        caps.supported_audio_codecs = Some(merged);
    } else {
        caps.supported_audio_codecs = Some(profile.supported_audio_codecs.clone());
    }

    // Min resolution
    caps.max_resolution = match (
        caps.max_resolution.clone(),
        Some(profile.max_resolution.clone()),
    ) {
        (Some(client), Some(profile)) => Some(min_resolution(&client, &profile)),
        (_, profile) => profile,
    };

    // Min bitrate cap if both present
    if let (Some(client), Some(profile_bps)) = (
        positive_bitrate_cap(caps.max_bitrate_bps),
        profile.max_bitrate_bps,
    ) {
        caps.max_bitrate_bps = Some(client.min(profile_bps));
    } else if caps.max_bitrate_bps.is_none() {
        caps.max_bitrate_bps = profile.max_bitrate_bps;
    } else {
        caps.max_bitrate_bps = positive_bitrate_cap(caps.max_bitrate_bps);
    }

    caps
}

fn client_playback_profile_from_caps(caps: &ClientCapabilities) -> ClientPlaybackProfile {
    let mut profile = if native_direct_play_client(caps) {
        ClientPlaybackProfile::native_mpv()
    } else {
        ClientPlaybackProfile::browser_like()
    };

    profile.profile_version = caps.profile_version.unwrap_or(1);
    profile.client_kind = client_kind_from_caps(caps);
    profile.direct_play_preferred = caps
        .direct_play_preferred
        .unwrap_or(profile.direct_play_preferred);
    profile.max_resolution = caps.max_resolution.clone().and_then(|value| {
        if is_unlimited_resolution(&value) {
            None
        } else {
            Some(value)
        }
    });
    profile.max_bitrate_bps = positive_bitrate_cap(caps.max_bitrate_bps);
    if let Some(values) = caps.supported_containers.clone() {
        profile.supported_containers = values
            .into_iter()
            .map(|v| normalize_container(&v))
            .collect();
    }
    if let Some(values) = caps.supported_video_codecs.clone() {
        profile.supported_video_codecs = values.into_iter().map(normalize_codec_token).collect();
    }
    if let Some(values) = caps.supported_audio_codecs.clone() {
        profile.supported_audio_codecs = values.into_iter().map(normalize_codec_token).collect();
    }
    if let Some(values) = caps.supported_subtitle_codecs.clone() {
        profile.supported_subtitle_codecs = values.into_iter().map(normalize_codec_token).collect();
    }
    if let Some(values) = caps.supported_hls_segment_types.clone() {
        profile.supported_hls_segment_types = values
            .into_iter()
            .map(|value| value.to_ascii_lowercase())
            .collect();
    }
    if let Some(value) = caps.max_audio_channels.filter(|value| *value > 0) {
        profile.max_audio_channels = Some(value);
    }
    if let Some(value) = caps.supports_hdr {
        profile.supports_hdr = value;
    }
    if let Some(value) = caps.supports_hdr10_plus {
        profile.supports_hdr10_plus = value;
    }
    if let Some(value) = caps.supports_dolby_vision {
        profile.supports_dolby_vision = value;
    }
    if let Some(value) = caps.supports_server_side_hls_seek {
        profile.supports_server_side_hls_seek = value;
    }
    if let Some(value) = caps.supports_auth_headers_for_media {
        profile.supports_auth_headers_for_media = value;
    }
    if let Some(value) = caps.subtitle_burn_policy.as_deref() {
        profile.subtitle_burn_policy = subtitle_burn_policy(value);
    }
    if let Some(value) = caps.subtitle_rendering.as_deref() {
        profile.subtitle_rendering = subtitle_rendering(value);
    }
    if let Some(value) = caps.ass_complexity_support.as_deref() {
        profile.ass_complexity_support = ass_complexity_support(value);
    }
    if let Some(value) = caps.image_subtitle_support.as_deref() {
        profile.image_subtitle_support = image_subtitle_support(value);
    }
    if let Some(value) = caps.forced_subtitle_policy.as_deref() {
        profile.forced_subtitle_policy = forced_subtitle_policy(value);
    }
    if let Some(value) = caps.default_subtitle_policy.as_deref() {
        profile.default_subtitle_policy = default_subtitle_policy(value);
    }
    if let Some(value) = caps.quality_mode.as_deref() {
        profile.quality_mode = quality_mode(value);
    }
    profile.fixed_bitrate_bps = positive_bitrate_cap(caps.fixed_bitrate_bps)
        .or_else(|| {
            (profile.quality_mode == QualityMode::Fixed)
                .then_some(profile.max_bitrate_bps)
                .flatten()
        })
        .or(profile.fixed_bitrate_bps);
    profile.fixed_resolution = caps
        .fixed_resolution
        .clone()
        .and_then(non_unlimited_resolution)
        .or_else(|| {
            (profile.quality_mode == QualityMode::Fixed)
                .then_some(profile.max_resolution.clone())
                .flatten()
        })
        .or(profile.fixed_resolution);
    profile.automatic_min_bitrate_bps =
        positive_bitrate_cap(caps.automatic_min_bitrate_bps).or(profile.automatic_min_bitrate_bps);
    profile.automatic_max_bitrate_bps =
        positive_bitrate_cap(caps.automatic_max_bitrate_bps).or(profile.automatic_max_bitrate_bps);
    profile.automatic_min_resolution = caps
        .automatic_min_resolution
        .clone()
        .and_then(non_unlimited_resolution)
        .or(profile.automatic_min_resolution);
    profile.automatic_max_resolution = caps
        .automatic_max_resolution
        .clone()
        .and_then(non_unlimited_resolution)
        .or(profile.automatic_max_resolution);
    if let Some(value) = caps.abr_support_type.as_deref() {
        profile.abr_support_type = abr_support_type(value);
    }
    profile.app_version = caps.app_version.clone();
    profile
}

fn effective_playback_policy_from_config(
    config: &crate::config::PlaybackConfig,
    profile: &EffectiveProfile,
    client_profile: &ClientPlaybackProfile,
    network_class: NetworkClass,
    hardware_capabilities: HardwareCapabilities,
) -> EffectivePlaybackPolicy {
    let hardware_enabled = config.hardware_acceleration_enabled;
    let server_policy = ServerPlaybackPolicy {
        allow_direct_play: config.allow_direct_play,
        allow_direct_stream: config.allow_direct_stream,
        allow_audio_transcode: config.allow_audio_transcode,
        allow_video_transcode: config.allow_video_transcode,
        allow_adaptive_transcode: config.allow_adaptive_transcode,
        max_remote_bitrate_bps: config.default_wan_max_bitrate_bps,
        server_upload_cap_bps: config.server_upload_cap_bps,
        max_resolution: Some(profile.max_resolution.clone())
            .filter(|value| !is_unlimited_resolution(value)),
        max_simultaneous_video_transcodes: config.video_transcode_capacity_limit(),
        force_direct_play_for_native_mpv: config.force_direct_play_for_native_mpv,
        video_encoder_preset: config.video_encoder_preset.clone(),
        video_encoder_profile: config.video_encoder_profile.clone(),
        video_encoder_level: config.video_encoder_level.clone(),
        video_encoder_crf: config.video_encoder_crf,
        video_encoder_bufsize_multiplier: config.video_encoder_bufsize_multiplier,
        hardware_acceleration: if hardware_enabled {
            config.hardware_acceleration.clone()
        } else {
            "off".to_string()
        },
        allow_hardware_decode: hardware_enabled && config.allow_hardware_decode,
        allow_hardware_encode: hardware_enabled && config.allow_hardware_encode,
        hardware_fallback: config.hardware_fallback.clone(),
        force_sdr_output: config.force_sdr_output,
        hardware_capabilities,
        unknown_performance_policy: UnknownPerformancePolicy::parse(
            &config.unknown_performance_policy,
        ),
        performance_envelopes: Vec::new(),
    };
    let network_policy = NetworkPlaybackPolicy {
        network_class,
        max_bitrate_bps: profile.max_bitrate_bps,
        max_remote_bitrate_bps: match network_class {
            NetworkClass::Wan | NetworkClass::Unknown => config.default_wan_max_bitrate_bps,
            NetworkClass::Lan => None,
        },
        max_resolution: Some(profile.max_resolution.clone())
            .filter(|value| !is_unlimited_resolution(value)),
        server_upload_cap_bps: config.server_upload_cap_bps,
    };

    derive_effective_playback_policy(client_profile, &server_policy, &network_policy)
}

#[derive(Debug, Clone)]
struct PlaybackRequestTransport {
    secure: bool,
    policy: String,
}

fn playback_request_transport(headers: &HeaderMap) -> PlaybackRequestTransport {
    let forwarded_proto = headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .map(str::trim)
        .map(str::to_ascii_lowercase);
    let forwarded_header = headers
        .get("forwarded")
        .and_then(|value| value.to_str().ok())
        .map(str::to_ascii_lowercase);
    let forwarded_ssl = headers
        .get("x-forwarded-ssl")
        .and_then(|value| value.to_str().ok())
        .map(str::to_ascii_lowercase);
    let forwarded_scheme = headers
        .get("x-forwarded-scheme")
        .and_then(|value| value.to_str().ok())
        .map(str::to_ascii_lowercase);

    let secure = forwarded_proto.as_deref() == Some("https")
        || forwarded_scheme.as_deref() == Some("https")
        || forwarded_ssl.as_deref() == Some("on")
        || forwarded_header
            .as_deref()
            .is_some_and(|value| value.contains("proto=https"));

    PlaybackRequestTransport {
        secure,
        policy: if secure {
            "https_or_forwarded_https".to_string()
        } else {
            "insecure_or_unreported".to_string()
        },
    }
}

fn enforce_remote_transport_policy(
    settings: &crate::config::Settings,
    network_class: NetworkClass,
    transport: &PlaybackRequestTransport,
) -> ApiResult<()> {
    let remote_like = remote_policy_applies(network_class, None);
    if remote_like
        && settings.playback.remote_require_https
        && !settings.playback.remote_allow_insecure
        && matches!(&settings.environment, RunEnvironment::Production)
        && !transport.secure
    {
        return Err(ApiError::conflict_code(
            "remote_https_required",
            "remote playback requires HTTPS",
            serde_json::json!({
                "network_class": network_class.as_str(),
                "secure_connection_policy": "require_https",
                "retry": {
                    "allowed": true,
                    "strategy": "use_https_or_configured_reverse_proxy"
                }
            }),
        ));
    }
    Ok(())
}

fn validated_share_id(value: Option<&str>) -> ApiResult<Option<String>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.len() > 128
        || !trimmed
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | ':' | '.'))
    {
        return Err(ApiError::bad_request("invalid share_id"));
    }
    Ok(Some(trimmed.to_string()))
}

fn remote_policy_applies(network_class: NetworkClass, share_id: Option<&str>) -> bool {
    share_id.is_some() || matches!(network_class, NetworkClass::Wan | NetworkClass::Unknown)
}

fn resolve_remote_playback_policy(
    config: &crate::config::PlaybackConfig,
    user_id: Uuid,
    share_id: Option<&str>,
    network_class: NetworkClass,
) -> RemotePlaybackPolicySnapshot {
    let applied = remote_policy_applies(network_class, share_id);
    let mut policy = RemotePlaybackPolicySnapshot {
        applied,
        scope: if share_id.is_some() {
            "share".to_string()
        } else if applied {
            "user".to_string()
        } else {
            "lan".to_string()
        },
        policy_sources: Vec::new(),
        user_id: user_id.to_string(),
        share_id: share_id.map(str::to_string),
        max_remote_bitrate_bps: applied
            .then_some(config.default_wan_max_bitrate_bps)
            .flatten(),
        max_resolution: None,
        allow_downloads: true,
        allow_direct_play: true,
        allow_transcode: true,
        allow_hardware_transcode: true,
        max_sessions: None,
        reasons: Vec::new(),
    };

    if applied {
        policy
            .policy_sources
            .push("playback.default_wan_max_bitrate_bps".to_string());
        apply_remote_policy_override(
            &mut policy,
            &config.default_remote_policy,
            "playback.default_remote_policy",
        );
        if let Some(user_policy) = config.remote_user_policies.get(&user_id.to_string()) {
            apply_remote_policy_override(&mut policy, user_policy, "playback.remote_user_policies");
        }
        if let Some(share_id) = share_id {
            if let Some(share_policy) = config.remote_share_policies.get(share_id) {
                apply_remote_policy_override(
                    &mut policy,
                    share_policy,
                    "playback.remote_share_policies",
                );
            }
        }
    }

    if policy.applied {
        policy.reasons.push("remote_policy_applied".to_string());
        policy
            .reasons
            .push(format!("remote_policy_scope_{}", policy.scope));
        if policy
            .max_remote_bitrate_bps
            .filter(|value| *value > 0)
            .is_some()
        {
            policy
                .reasons
                .push("remote_policy_max_bitrate_applied".to_string());
        }
        if policy.max_resolution.as_deref().is_some() {
            policy
                .reasons
                .push("remote_policy_max_resolution_applied".to_string());
        }
        if !policy.allow_downloads {
            policy
                .reasons
                .push("remote_policy_downloads_disabled".to_string());
        }
        if !policy.allow_direct_play {
            policy
                .reasons
                .push("remote_policy_direct_play_disabled".to_string());
        }
        if !policy.allow_transcode {
            policy
                .reasons
                .push("remote_policy_transcode_disabled".to_string());
        }
        if !policy.allow_hardware_transcode {
            policy
                .reasons
                .push("remote_policy_hardware_transcode_disabled".to_string());
        }
        if policy.max_sessions.filter(|value| *value > 0).is_some() {
            policy
                .reasons
                .push("remote_policy_session_limit_applied".to_string());
        }
    }

    policy
}

fn apply_remote_policy_override(
    policy: &mut RemotePlaybackPolicySnapshot,
    override_policy: &crate::config::PlaybackRemotePolicyOverride,
    source: &str,
) {
    let mut changed = false;
    if let Some(value) = override_policy
        .max_remote_bitrate_bps
        .filter(|value| *value > 0)
    {
        policy.max_remote_bitrate_bps =
            min_positive_i64_local(policy.max_remote_bitrate_bps, Some(value));
        changed = true;
    }
    if let Some(value) = override_policy.max_resolution.as_ref() {
        let value = value.trim();
        if !value.is_empty() && !is_unlimited_resolution(value) {
            policy.max_resolution = match policy.max_resolution.as_deref() {
                Some(existing) => Some(min_resolution(existing, value)),
                None => Some(value.to_string()),
            };
            changed = true;
        }
    }
    if let Some(value) = override_policy.allow_downloads {
        policy.allow_downloads = value;
        changed = true;
    }
    if let Some(value) = override_policy.allow_direct_play {
        policy.allow_direct_play = value;
        changed = true;
    }
    if let Some(value) = override_policy.allow_transcode {
        policy.allow_transcode = value;
        changed = true;
    }
    if let Some(value) = override_policy.allow_hardware_transcode {
        policy.allow_hardware_transcode = value;
        changed = true;
    }
    if let Some(value) = override_policy.max_sessions.filter(|value| *value > 0) {
        policy.max_sessions = policy
            .max_sessions
            .map(|existing| existing.min(value))
            .or(Some(value));
        changed = true;
    }
    if changed {
        policy.policy_sources.push(source.to_string());
    }
}

fn apply_remote_policy_to_effective_policy(
    effective_policy: &mut EffectivePlaybackPolicy,
    remote_policy: &RemotePlaybackPolicySnapshot,
) {
    if !remote_policy.applied {
        return;
    }

    if let Some(value) = remote_policy
        .max_remote_bitrate_bps
        .filter(|value| *value > 0)
    {
        effective_policy.max_bitrate_bps =
            min_positive_i64_local(effective_policy.max_bitrate_bps, Some(value));
        effective_policy.max_remote_bitrate_bps =
            min_positive_i64_local(effective_policy.max_remote_bitrate_bps, Some(value));
    }
    if let Some(value) = remote_policy.max_resolution.as_deref() {
        effective_policy.max_resolution = match effective_policy.max_resolution.as_deref() {
            Some(existing) => Some(min_resolution(existing, value)),
            None => Some(value.to_string()),
        };
    }
    if !remote_policy.allow_downloads || !remote_policy.allow_direct_play {
        effective_policy.allow_direct_play = false;
    }
    if !remote_policy.allow_transcode {
        effective_policy.allow_audio_transcode = false;
        effective_policy.allow_video_transcode = false;
        effective_policy.allow_adaptive_transcode = false;
    }
    if !remote_policy.allow_hardware_transcode {
        effective_policy.hardware_acceleration = "off".to_string();
        effective_policy.allow_hardware_decode = false;
        effective_policy.allow_hardware_encode = false;
    }
}

fn append_remote_policy_plan_reasons(
    plan: &mut PlaybackPlan,
    remote_policy: &RemotePlaybackPolicySnapshot,
    transport: &PlaybackRequestTransport,
    settings: &crate::config::Settings,
) {
    if !remote_policy.applied {
        return;
    }
    for reason in &remote_policy.reasons {
        push_plan_reason(plan, reason);
    }
    if settings.playback.remote_require_https {
        push_plan_reason(plan, "remote_transport_https_required_by_policy");
    }
    if settings.playback.remote_allow_insecure {
        push_plan_reason(plan, "remote_transport_insecure_allowed_by_policy");
    } else if matches!(&settings.environment, RunEnvironment::Development) && !transport.secure {
        push_plan_reason(plan, "remote_transport_insecure_allowed_in_development");
    } else if transport.secure {
        push_plan_reason(plan, "remote_transport_secure");
    }
}

fn push_plan_reason(plan: &mut PlaybackPlan, reason: &str) {
    if !plan.reasons.iter().any(|existing| existing == reason) {
        plan.reasons.push(reason.to_string());
    }
    if let Some(feasibility) = plan.feasibility.as_mut() {
        if !feasibility
            .reasons
            .iter()
            .any(|existing| existing == reason)
        {
            feasibility.reasons.push(reason.to_string());
        }
    }
}

fn min_positive_i64_local(a: Option<i64>, b: Option<i64>) -> Option<i64> {
    match (a.filter(|value| *value > 0), b.filter(|value| *value > 0)) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

fn stream_token_ttl_seconds(config: &crate::config::PlaybackConfig) -> u64 {
    let requested = config.stream_token_ttl_seconds;
    if requested == 0 {
        return config.session_ttl_seconds.max(1);
    }
    requested.min(config.session_ttl_seconds).max(1)
}

fn stream_token_expiry(config: &crate::config::PlaybackConfig) -> (u64, String) {
    let ttl = stream_token_ttl_seconds(config);
    let duration =
        chrono::Duration::from_std(Duration::from_secs(ttl)).unwrap_or(chrono::Duration::MAX);
    let expires_at = chrono::Utc::now() + duration;
    (ttl, expires_at.to_rfc3339())
}

async fn remote_access_contract(
    state: &AppState,
    server_id: &str,
    token_ttl_seconds: u64,
    token_expires_at: &str,
    transport: &PlaybackRequestTransport,
) -> RemoteAccessContract {
    let endpoints = playback_endpoints(state, server_id).await;
    RemoteAccessContract {
        lan_direct_endpoint: endpoints.lan_direct_endpoint,
        wan_direct_endpoint: endpoints.wan_direct_endpoint,
        reverse_proxy_endpoint: state
            .settings
            .playback
            .remote_reverse_proxy_endpoint
            .clone(),
        reverse_proxy_behavior:
            "preserve_authorization_header_and_query_tokens; honor_x_forwarded_proto".to_string(),
        https_required: state.settings.playback.remote_require_https,
        secure_connection_policy: secure_connection_policy(&state.settings, transport),
        request_transport: transport.policy.clone(),
        token_ttl_seconds,
        stream_token_expires_at: token_expires_at.to_string(),
        session_revocation:
            "ending, expiring, or token-expiring a session invalidates direct and HLS routes"
                .to_string(),
    }
}

#[derive(Debug, Default)]
struct PlaybackEndpoints {
    lan_direct_endpoint: Option<String>,
    wan_direct_endpoint: Option<String>,
}

async fn playback_endpoints(state: &AppState, server_id: &str) -> PlaybackEndpoints {
    let registry_row = sqlx::query(
        "SELECT lan_addresses, wan_direct_endpoint
         FROM server_registry
         WHERE server_id = ?
         ORDER BY last_seen_at DESC
         LIMIT 1",
    )
    .bind(server_id)
    .fetch_optional(&state.db_pool)
    .await
    .ok()
    .flatten();

    if let Some(row) = registry_row {
        let lan_addresses: Option<String> = row.try_get("lan_addresses").ok();
        let wan_direct_endpoint: Option<String> = row.try_get("wan_direct_endpoint").ok();
        return PlaybackEndpoints {
            lan_direct_endpoint: first_lan_endpoint(lan_addresses.as_deref())
                .or_else(|| Some(configured_lan_endpoint(&state.settings))),
            wan_direct_endpoint,
        };
    }

    let instance_row = sqlx::query(
        "SELECT lan_addresses, wan_direct_endpoint
         FROM server_instances
         WHERE id = ?
         LIMIT 1",
    )
    .bind(server_id)
    .fetch_optional(&state.db_pool)
    .await
    .ok()
    .flatten();

    if let Some(row) = instance_row {
        let lan_addresses: Option<String> = row.try_get("lan_addresses").ok();
        let wan_direct_endpoint: Option<String> = row.try_get("wan_direct_endpoint").ok();
        return PlaybackEndpoints {
            lan_direct_endpoint: first_lan_endpoint(lan_addresses.as_deref())
                .or_else(|| Some(configured_lan_endpoint(&state.settings))),
            wan_direct_endpoint,
        };
    }

    PlaybackEndpoints {
        lan_direct_endpoint: Some(configured_lan_endpoint(&state.settings)),
        wan_direct_endpoint: None,
    }
}

fn first_lan_endpoint(raw: Option<&str>) -> Option<String> {
    raw.and_then(|value| serde_json::from_str::<Vec<String>>(value).ok())
        .and_then(|values| values.into_iter().find(|value| !value.trim().is_empty()))
}

fn configured_lan_endpoint(settings: &crate::config::Settings) -> String {
    let host = if settings.server.host == "0.0.0.0" {
        "127.0.0.1"
    } else {
        settings.server.host.as_str()
    };
    format!("{host}:{}", settings.server.port)
}

fn secure_connection_policy(
    settings: &crate::config::Settings,
    transport: &PlaybackRequestTransport,
) -> String {
    if settings.playback.remote_allow_insecure {
        "allow_insecure_remote".to_string()
    } else if settings.playback.remote_require_https
        && matches!(&settings.environment, RunEnvironment::Production)
    {
        "require_https".to_string()
    } else if settings.playback.remote_require_https && transport.secure {
        "https_satisfied".to_string()
    } else if settings.playback.remote_require_https {
        "https_required_for_production_development_insecure_allowed".to_string()
    } else {
        "https_optional".to_string()
    }
}

fn hardware_metric_label(plan: &HardwareAccelerationPlan) -> &'static str {
    match (plan.decoder.is_some(), plan.encoder.is_some()) {
        (true, true) => "hardware_decode_encode",
        (true, false) => "hardware_decode",
        (false, true) => "hardware_encode",
        (false, false) => "software",
    }
}

fn hardware_metric_label_from_plan_value(value: Option<&Value>) -> String {
    value
        .and_then(|value| serde_json::from_value::<PlaybackPlan>(value.clone()).ok())
        .map(|plan| hardware_metric_label(&plan.hardware_acceleration).to_string())
        .unwrap_or_else(|| "software".to_string())
}

fn hardware_metric_label_from_job_state(job_state: &Value) -> String {
    job_state
        .get("playback_plan")
        .map(|value| hardware_metric_label_from_plan_value(Some(value)))
        .unwrap_or_else(|| "software".to_string())
}

fn record_playback_performance_envelopes(envelopes: &[PlaybackPerformanceEnvelope]) {
    for envelope in envelopes {
        let api = envelope.hardware_api.as_deref().unwrap_or("software");
        let status = format!(
            "{}_{}",
            envelope.support_decision.as_str(),
            envelope.performance_decision.as_str()
        );
        PLAYBACK_PERFORMANCE_ENVELOPE_STATUS
            .with_label_values(&[
                envelope.workload_class_id.as_str(),
                api,
                status.as_str(),
                envelope.confidence.as_str(),
            ])
            .set(1);
        if let Some(realtime_factor) = envelope.p95_realtime_factor_millis {
            PLAYBACK_TRANSCODE_REALTIME_FACTOR
                .with_label_values(&[
                    envelope.workload_class_id.as_str(),
                    api,
                    envelope.pipeline_signature.as_str(),
                ])
                .observe(f64::from(realtime_factor) / 1000.0);
        }
    }
}

fn record_playback_feasibility_for_plan(playback_plan: &PlaybackPlan, client_kind: &str) {
    let Some(feasibility) = playback_plan.feasibility.as_ref() else {
        return;
    };
    let workload_class = playback_plan
        .workload_class
        .as_ref()
        .map(|workload| workload.class_id.as_str())
        .unwrap_or("none");
    PLAYBACK_FEASIBILITY_DECISIONS
        .with_label_values(&[
            feasibility.action.as_str(),
            feasibility.reason.as_str(),
            playback_plan.mode.as_str(),
            client_kind,
        ])
        .inc();
    match feasibility.action {
        PlaybackFeasibilityAction::Reject => {
            PLAYBACK_TRANSCODE_REJECTED
                .with_label_values(&[feasibility.reason.as_str(), workload_class, client_kind])
                .inc();
        }
        PlaybackFeasibilityAction::DowngradeQuality => {
            PLAYBACK_TRANSCODE_DOWNGRADED
                .with_label_values(&[feasibility.reason.as_str(), workload_class, client_kind])
                .inc();
        }
        _ => {}
    }
}

fn playback_plan_contract_allowed(settings: &crate::config::Settings) -> bool {
    settings.playback.plan_contract_enabled || settings.environment == RunEnvironment::Development
}

fn client_kind_label(kind: &ClientKind) -> &'static str {
    match kind {
        ClientKind::NativeMpv => "native_mpv",
        ClientKind::Web => "web",
        ClientKind::Tv => "tv",
        ClientKind::Mobile => "mobile",
        ClientKind::Unknown => "unknown",
    }
}

fn playback_error_code_for_plan(playback_plan: &PlaybackPlan) -> &'static str {
    if playback_plan
        .reasons
        .iter()
        .any(|reason| reason == "transcode_capacity_exhausted")
    {
        "transcode_capacity_exhausted"
    } else if let Some(error_code) = playback_feasibility_error_code(playback_plan) {
        error_code
    } else if playback_plan
        .reasons
        .iter()
        .any(|reason| reason == "direct_stream_disabled")
    {
        "direct_stream_disabled"
    } else if playback_plan
        .reasons
        .iter()
        .any(|reason| reason == "video_transcode_disabled_by_policy")
    {
        "video_transcode_disabled"
    } else if playback_plan
        .reasons
        .iter()
        .any(|reason| reason.starts_with("hardware_unavailable"))
    {
        "hardware_unavailable"
    } else if playback_plan
        .reasons
        .iter()
        .any(|reason| matches!(reason.as_str(), "probe_failed" | "probe_required"))
    {
        "probe_failed"
    } else {
        "playback_not_playable"
    }
}

fn playback_feasibility_error_code(playback_plan: &PlaybackPlan) -> Option<&'static str> {
    const FEASIBILITY_ERRORS: &[&str] = &[
        "hardware_decode_unsupported",
        "hardware_encode_unsupported",
        "hardware_filter_unsupported",
        "software_decode_unsupported",
        "server_cannot_realtime_transcode_source",
        "server_cannot_realtime_tonemap_source",
        "server_cannot_realtime_burn_subtitles",
        "transcode_performance_unknown_policy_denied",
        "transcode_capacity_exhausted",
    ];
    playback_plan
        .feasibility
        .as_ref()
        .and_then(|feasibility| {
            FEASIBILITY_ERRORS
                .iter()
                .copied()
                .find(|error| feasibility.reason == *error)
                .or_else(|| {
                    FEASIBILITY_ERRORS
                        .iter()
                        .copied()
                        .find(|error| feasibility.reasons.iter().any(|reason| reason == *error))
                })
        })
        .or_else(|| {
            FEASIBILITY_ERRORS
                .iter()
                .copied()
                .find(|error| playback_plan.reasons.iter().any(|reason| reason == *error))
        })
}

fn record_playback_error_labels(
    mode: &str,
    delivery: &str,
    client_kind: &str,
    network_kind: &str,
    error_class: &str,
    hardware: &str,
) {
    PLAY_ERRORS.with_label_values(&[error_class]).inc();
    PLAYBACK_ERRORS
        .with_label_values(&[
            mode,
            delivery,
            client_kind,
            network_kind,
            error_class,
            hardware,
        ])
        .inc();
}

fn record_playback_error_for_plan(
    playback_plan: &PlaybackPlan,
    client_kind: &str,
    network_kind: &str,
    error_class: &str,
) {
    record_playback_error_labels(
        playback_plan.mode.as_str(),
        playback_plan.delivery.as_str(),
        client_kind,
        network_kind,
        error_class,
        hardware_metric_label(&playback_plan.hardware_acceleration),
    );
}

fn playback_release_gate_details(
    flag: &str,
    error_class: &str,
    playback_plan: &PlaybackPlan,
) -> Value {
    let plan_value = serde_json::to_value(playback_plan).ok();
    serde_json::json!({
        "flag": flag,
        "error_class": error_class,
        "reason": playback_plan.decision_reason(),
        "reasons": playback_plan.reasons.clone(),
        "plan_summary": plan_summary_from_plan(plan_value.as_ref(), None),
        "retry": {
            "allowed": false,
            "strategy": "enable_release_gate"
        }
    })
}

fn playback_rollout_error(
    config: &crate::config::PlaybackConfig,
    playback_plan: &PlaybackPlan,
    client_kind: &str,
    network_kind: &str,
) -> Option<ApiError> {
    let gate = match playback_plan.mode {
        PlaybackMode::DirectStream if !config.hls_direct_stream_enabled => Some((
            "direct_stream_disabled",
            "direct_stream_disabled",
            "playback.hls_direct_stream_enabled",
            "HLS direct stream is disabled by rollout policy",
        )),
        PlaybackMode::AudioTranscode if !config.audio_transcode_enabled => Some((
            "audio_transcode_disabled",
            "audio_transcode_disabled",
            "playback.audio_transcode_enabled",
            "audio transcode is disabled by rollout policy",
        )),
        PlaybackMode::SubtitleTranscode if !config.subtitle_transcode_enabled => Some((
            "subtitle_transcode_disabled",
            "subtitle_transcode_disabled",
            "playback.subtitle_transcode_enabled",
            "subtitle transcode is disabled by rollout policy",
        )),
        PlaybackMode::AdaptiveTranscode if !config.adaptive_quality_enabled => Some((
            "video_transcode_disabled",
            "adaptive_quality_disabled",
            "playback.adaptive_quality_enabled",
            "adaptive quality is disabled by rollout policy",
        )),
        _ if playback_plan.hardware_acceleration.enabled
            && !config.hardware_acceleration_enabled =>
        {
            Some((
                "hardware_unavailable",
                "hardware_disabled_by_rollout",
                "playback.hardware_acceleration_enabled",
                "hardware acceleration is disabled by rollout policy",
            ))
        }
        _ => None,
    }?;

    let (code, error_class, flag, message) = gate;
    record_playback_error_for_plan(playback_plan, client_kind, network_kind, error_class);
    Some(ApiError::conflict_code(
        code,
        message,
        playback_release_gate_details(flag, error_class, playback_plan),
    ))
}

#[derive(Debug)]
struct PlaybackCapacitySnapshot {
    active_sessions: u64,
    active_user_sessions: u64,
    active_direct_streams: u64,
    active_hls_jobs: u64,
    active_video_transcode_weight: u64,
    active_hardware_transcodes: u64,
    startup_queue_len: u64,
    temp_dir_bytes: u64,
    ffmpeg_log_bytes: u64,
}

#[derive(Debug)]
struct PlaybackCapacityViolation {
    resource: &'static str,
    limit: u64,
    observed: u64,
    requested: u64,
}

async fn enforce_playback_capacity_before_start(
    state: &AppState,
    user: &CurrentUser,
    playback_plan: &PlaybackPlan,
    client_kind: &str,
    network_kind: &str,
) -> ApiResult<Option<ApiError>> {
    let snapshot = playback_capacity_snapshot(state, user).await?;
    record_playback_capacity_levels(&snapshot);
    let Some(violation) =
        playback_capacity_violation(&state.settings.playback, playback_plan, &snapshot)
    else {
        return Ok(None);
    };

    let hardware = hardware_metric_label(&playback_plan.hardware_acceleration);
    PLAYBACK_CAPACITY_REJECTIONS
        .with_label_values(&[
            violation.resource,
            playback_plan.mode.as_str(),
            playback_plan.delivery.as_str(),
            client_kind,
            network_kind,
            "transcode_capacity_exhausted",
        ])
        .inc();
    record_playback_error_labels(
        playback_plan.mode.as_str(),
        playback_plan.delivery.as_str(),
        client_kind,
        network_kind,
        "transcode_capacity_exhausted",
        hardware,
    );

    Ok(Some(ApiError::conflict_code(
        "transcode_capacity_exhausted",
        format!("playback capacity exhausted: {}", violation.resource),
        playback_capacity_violation_details(playback_plan, &snapshot, &violation),
    )))
}

async fn enforce_remote_policy_session_limit(
    state: &AppState,
    remote_policy: &RemotePlaybackPolicySnapshot,
    playback_plan: &PlaybackPlan,
    client_kind: &str,
    network_kind: &str,
) -> ApiResult<Option<ApiError>> {
    let Some(limit) = remote_policy.max_sessions.filter(|value| *value > 0) else {
        return Ok(None);
    };
    if !remote_policy.applied {
        return Ok(None);
    }

    let (resource, observed) = if let Some(share_id) = remote_policy.share_id.as_deref() {
        let count = active_share_playback_session_count(state, share_id).await?;
        ("remote_share_sessions", count)
    } else {
        let count = active_user_playback_session_count(state, &remote_policy.user_id).await?;
        ("remote_user_sessions", count)
    };

    if observed < u64::from(limit) {
        return Ok(None);
    }

    let hardware = hardware_metric_label(&playback_plan.hardware_acceleration);
    PLAYBACK_CAPACITY_REJECTIONS
        .with_label_values(&[
            resource,
            playback_plan.mode.as_str(),
            playback_plan.delivery.as_str(),
            client_kind,
            network_kind,
            "transcode_capacity_exhausted",
        ])
        .inc();
    record_playback_error_labels(
        playback_plan.mode.as_str(),
        playback_plan.delivery.as_str(),
        client_kind,
        network_kind,
        "transcode_capacity_exhausted",
        hardware,
    );

    Ok(Some(ApiError::conflict_code(
        "transcode_capacity_exhausted",
        format!("remote playback capacity exhausted: {resource}"),
        serde_json::json!({
            "capacity": {
                "resource": resource,
                "limit": limit,
                "observed": observed,
                "requested": 1
            },
            "remote_policy": remote_policy,
            "plan_summary": playback_plan_summary(playback_plan),
            "retry": {
                "allowed": true,
                "strategy": "end_existing_remote_session_then_retry"
            }
        }),
    )))
}

async fn active_user_playback_session_count(state: &AppState, user_id: &str) -> ApiResult<u64> {
    let count: Option<String> = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT CAST(COUNT(*) AS TEXT)
         FROM playback_sessions
         WHERE state = 'active' AND user_id = ?",
    )
    .bind(user_id)
    .fetch_optional(&state.db_pool)
    .await
    .map_err(|e| ApiError::internal(e.to_string()))?;
    Ok(count
        .as_deref()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0))
}

async fn active_share_playback_session_count(state: &AppState, share_id: &str) -> ApiResult<u64> {
    let count: Option<String> = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT CAST(COUNT(*) AS TEXT)
         FROM playback_sessions
         WHERE state = 'active' AND share_id = ?",
    )
    .bind(share_id)
    .fetch_optional(&state.db_pool)
    .await
    .map_err(|e| ApiError::internal(e.to_string()))?;
    Ok(count
        .as_deref()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0))
}

async fn playback_capacity_snapshot(
    state: &AppState,
    user: &CurrentUser,
) -> ApiResult<PlaybackCapacitySnapshot> {
    let user_id = user.user_id.to_string();
    let rows = sqlx::query::<sqlx::Any>(
        "SELECT user_id, mode, playback_plan_json FROM playback_sessions WHERE state = 'active'",
    )
    .fetch_all(&state.db_pool)
    .await
    .map_err(|e| ApiError::internal(e.to_string()))?;

    let mut active_sessions = 0_u64;
    let mut active_user_sessions = 0_u64;
    let mut active_direct_streams = 0_u64;
    let mut active_hls_jobs = 0_u64;
    let mut active_video_transcode_weight = 0_u64;
    let mut active_hardware_transcodes = 0_u64;

    for row in rows {
        active_sessions += 1;
        if row.try_get::<String, _>("user_id").ok().as_deref() == Some(user_id.as_str()) {
            active_user_sessions += 1;
        }
        let mode = row.try_get::<String, _>("mode").unwrap_or_default();
        match mode.as_str() {
            "direct_stream" => {
                active_direct_streams += 1;
                active_hls_jobs += 1;
            }
            "audio_transcode" | "subtitle_transcode" => {
                active_hls_jobs += 1;
            }
            "video_transcode" | "transcode" => {
                active_hls_jobs += 1;
                active_video_transcode_weight += 1;
            }
            "adaptive_transcode" => {
                active_hls_jobs += 1;
                active_video_transcode_weight += 2;
            }
            _ => {}
        }
        if row
            .try_get::<String, _>("playback_plan_json")
            .ok()
            .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
            .and_then(|value| serde_json::from_value::<PlaybackPlan>(value).ok())
            .is_some_and(|plan| plan.hardware_acceleration.enabled)
        {
            active_hardware_transcodes += 1;
        }
    }

    let temp_root = playback_temp_root();
    let temp_dir_bytes = directory_bytes(&temp_root, DirectoryByteFilter::All)
        .await
        .unwrap_or(0);
    let ffmpeg_log_bytes = directory_bytes(&temp_root, DirectoryByteFilter::FfmpegLogs)
        .await
        .unwrap_or(0);

    Ok(PlaybackCapacitySnapshot {
        active_sessions,
        active_user_sessions,
        active_direct_streams,
        active_hls_jobs,
        active_video_transcode_weight,
        active_hardware_transcodes,
        startup_queue_len: state.transcodes.startup_queue_len() as u64,
        temp_dir_bytes,
        ffmpeg_log_bytes,
    })
}

fn record_playback_capacity_levels(snapshot: &PlaybackCapacitySnapshot) {
    let gauges = [
        ("active_sessions", snapshot.active_sessions),
        ("per_user_sessions", snapshot.active_user_sessions),
        ("direct_streams", snapshot.active_direct_streams),
        ("hls_jobs", snapshot.active_hls_jobs),
        ("video_transcodes", snapshot.active_video_transcode_weight),
        ("hardware_transcodes", snapshot.active_hardware_transcodes),
        ("startup_queue_length", snapshot.startup_queue_len),
        ("temp_dir_bytes", snapshot.temp_dir_bytes),
        ("ffmpeg_log_bytes", snapshot.ffmpeg_log_bytes),
    ];
    for (resource, value) in gauges {
        PLAYBACK_CAPACITY_LEVELS
            .with_label_values(&[resource, "all", "all"])
            .set(value.min(i64::MAX as u64) as i64);
    }
}

fn playback_capacity_violation(
    config: &crate::config::PlaybackConfig,
    playback_plan: &PlaybackPlan,
    snapshot: &PlaybackCapacitySnapshot,
) -> Option<PlaybackCapacityViolation> {
    limit_exceeded(
        "active_sessions",
        config.max_active_sessions.map(u64::from),
        snapshot.active_sessions,
        1,
        LimitMode::AtOrAbove,
    )
    .or_else(|| {
        limit_exceeded(
            "per_user_sessions",
            config.max_sessions_per_user.map(u64::from),
            snapshot.active_user_sessions,
            1,
            LimitMode::AtOrAbove,
        )
    })
    .or_else(|| {
        if playback_plan.mode == PlaybackMode::DirectStream {
            limit_exceeded(
                "direct_streams",
                config.max_active_direct_streams.map(u64::from),
                snapshot.active_direct_streams,
                1,
                LimitMode::AtOrAbove,
            )
        } else {
            None
        }
    })
    .or_else(|| {
        if playback_plan.mode.is_hls_producing() {
            limit_exceeded(
                "hls_jobs",
                config.max_active_hls_jobs.map(u64::from),
                snapshot.active_hls_jobs,
                1,
                LimitMode::AtOrAbove,
            )
        } else {
            None
        }
    })
    .or_else(|| {
        let requested = video_transcode_capacity_weight(playback_plan.mode);
        if requested > 0 {
            limit_exceeded(
                "video_transcodes",
                config.video_transcode_capacity_limit().map(u64::from),
                snapshot.active_video_transcode_weight,
                requested,
                LimitMode::IncludingRequest,
            )
        } else {
            None
        }
    })
    .or_else(|| {
        if playback_plan.hardware_acceleration.enabled {
            limit_exceeded(
                "hardware_transcodes",
                config.max_active_hardware_transcodes.map(u64::from),
                snapshot.active_hardware_transcodes,
                1,
                LimitMode::AtOrAbove,
            )
        } else {
            None
        }
    })
    .or_else(|| {
        if playback_plan.mode.is_hls_producing() {
            limit_exceeded(
                "startup_queue_length",
                config.max_startup_queue_length.map(u64::from),
                snapshot.startup_queue_len,
                1,
                LimitMode::AtOrAbove,
            )
        } else {
            None
        }
    })
    .or_else(|| {
        if playback_plan.mode.is_hls_producing() {
            limit_exceeded(
                "temp_dir_bytes",
                config.max_temp_dir_bytes,
                snapshot.temp_dir_bytes,
                1,
                LimitMode::AtOrAbove,
            )
        } else {
            None
        }
    })
    .or_else(|| {
        if playback_plan.mode.is_hls_producing() {
            limit_exceeded(
                "ffmpeg_log_bytes",
                config.max_ffmpeg_log_bytes,
                snapshot.ffmpeg_log_bytes,
                1,
                LimitMode::AtOrAbove,
            )
        } else {
            None
        }
    })
}

#[derive(Debug, Clone, Copy)]
enum LimitMode {
    AtOrAbove,
    IncludingRequest,
}

fn limit_exceeded(
    resource: &'static str,
    limit: Option<u64>,
    observed: u64,
    requested: u64,
    mode: LimitMode,
) -> Option<PlaybackCapacityViolation> {
    let limit = limit?;
    let exceeded = match mode {
        LimitMode::AtOrAbove => observed >= limit,
        LimitMode::IncludingRequest => observed.saturating_add(requested) > limit,
    };
    exceeded.then_some(PlaybackCapacityViolation {
        resource,
        limit,
        observed,
        requested,
    })
}

fn video_transcode_capacity_weight(mode: PlaybackMode) -> u64 {
    match mode {
        PlaybackMode::VideoTranscode => 1,
        PlaybackMode::AdaptiveTranscode => 2,
        _ => 0,
    }
}

fn playback_capacity_violation_details(
    playback_plan: &PlaybackPlan,
    snapshot: &PlaybackCapacitySnapshot,
    violation: &PlaybackCapacityViolation,
) -> Value {
    let plan_value = serde_json::to_value(playback_plan).ok();
    serde_json::json!({
        "reason": "transcode_capacity_exhausted",
        "reasons": playback_plan.reasons.clone(),
        "plan_summary": plan_summary_from_plan(plan_value.as_ref(), None),
        "capacity": {
            "resource": violation.resource,
            "limit": violation.limit,
            "observed": violation.observed,
            "requested": violation.requested,
            "snapshot": {
                "active_sessions": snapshot.active_sessions,
                "active_user_sessions": snapshot.active_user_sessions,
                "active_direct_streams": snapshot.active_direct_streams,
                "active_hls_jobs": snapshot.active_hls_jobs,
                "active_video_transcode_weight": snapshot.active_video_transcode_weight,
                "active_hardware_transcodes": snapshot.active_hardware_transcodes,
                "startup_queue_len": snapshot.startup_queue_len,
                "temp_dir_bytes": snapshot.temp_dir_bytes,
                "ffmpeg_log_bytes": snapshot.ffmpeg_log_bytes
            }
        },
        "retry": {
            "allowed": true,
            "after_seconds": 30,
            "strategy": "retry_same_request"
        },
        "fallback": {
            "server_recovery_required": true,
            "automatic_client_retry": false
        }
    })
}

fn playback_plan_summary(playback_plan: &PlaybackPlan) -> Value {
    let plan_value = serde_json::to_value(playback_plan).ok();
    plan_summary_from_plan(plan_value.as_ref(), None).unwrap_or_else(|| {
        serde_json::json!({
            "mode": playback_plan.mode.as_str(),
            "delivery": playback_plan.delivery.as_str(),
            "playable": playback_plan.playable,
            "reasons": playback_plan.reasons.clone(),
        })
    })
}

#[derive(Debug, Clone, Copy)]
enum DirectoryByteFilter {
    All,
    FfmpegLogs,
}

async fn directory_bytes(root: &Path, filter: DirectoryByteFilter) -> std::io::Result<u64> {
    if fs::metadata(root).await.is_err() {
        return Ok(0);
    }
    let mut total = 0_u64;
    let mut queue = VecDeque::from([root.to_path_buf()]);
    while let Some(path) = queue.pop_front() {
        let metadata = match fs::metadata(&path).await {
            Ok(metadata) => metadata,
            Err(_) => continue,
        };
        if metadata.is_file() {
            let include = match filter {
                DirectoryByteFilter::All => true,
                DirectoryByteFilter::FfmpegLogs => path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name == "ffmpeg.log"),
            };
            if include {
                total = total.saturating_add(metadata.len());
            }
            continue;
        }
        if !metadata.is_dir() {
            continue;
        }
        let mut entries = match fs::read_dir(&path).await {
            Ok(entries) => entries,
            Err(_) => continue,
        };
        while let Some(entry) = entries.next_entry().await? {
            queue.push_back(entry.path());
        }
    }
    Ok(total)
}

fn adaptive_ladder_from_plan(playback_plan: Option<&Value>) -> Option<Value> {
    playback_plan
        .and_then(|plan| plan.get("adaptive_ladder"))
        .cloned()
}

fn starting_rung_from_plan(playback_plan: Option<&Value>) -> Option<Value> {
    let ladder = playback_plan.and_then(|plan| plan.get("adaptive_ladder"))?;
    let starting_id = ladder.get("starting_rung_id").and_then(Value::as_str)?;
    rung_from_ladder(ladder, starting_id)
}

fn active_rung_from_state(
    playback_plan: Option<&Value>,
    job_state: Option<&Value>,
) -> Option<Value> {
    job_state
        .and_then(|state| state.get("active_rung"))
        .cloned()
        .or_else(|| {
            let ladder = playback_plan.and_then(|plan| plan.get("adaptive_ladder"))?;
            let active_id = ladder
                .get("active_rung_id")
                .and_then(Value::as_str)
                .or_else(|| ladder.get("starting_rung_id").and_then(Value::as_str))?;
            rung_from_ladder(ladder, active_id)
        })
}

fn plan_summary_from_plan(
    playback_plan: Option<&Value>,
    active_rung: Option<&Value>,
) -> Option<Value> {
    let plan = playback_plan?;
    let reasons = plan
        .get("reasons")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let decision_reason = reasons
        .first()
        .and_then(Value::as_str)
        .unwrap_or("playback_plan_created");
    let seek_behavior = plan
        .get("seek_behavior")
        .and_then(Value::as_str)
        .unwrap_or("client_range");
    Some(serde_json::json!({
        "mode": plan.get("mode").cloned().unwrap_or(Value::Null),
        "delivery": plan.get("delivery").cloned().unwrap_or(Value::Null),
        "media_file_id": plan.get("media_file_id").cloned().unwrap_or(Value::Null),
        "server_seek_required": seek_behavior == "server_hls_restart",
        "seek_behavior": seek_behavior,
        "selected_video_track": plan.get("selected_video_track").cloned().unwrap_or(Value::Null),
        "selected_audio_track": plan.get("selected_audio_track").cloned().unwrap_or(Value::Null),
        "selected_subtitle_track": plan.get("selected_subtitle_track").cloned().unwrap_or(Value::Null),
        "video_action": plan.get("video_action").cloned().unwrap_or(Value::Null),
        "audio_action": plan.get("audio_action").cloned().unwrap_or(Value::Null),
        "subtitle_action": plan.get("subtitle_action").cloned().unwrap_or(Value::Null),
        "hdr_action": plan.get("hdr_action").cloned().unwrap_or(Value::Null),
        "adaptive": plan.get("adaptive").cloned().unwrap_or(Value::Bool(false)),
        "active_rung": active_rung.cloned(),
        "decision_reason": decision_reason,
        "decision_reasons": reasons,
        "video_transcode_reason": plan.get("video_transcode_reason").cloned().unwrap_or(Value::Null),
        "tone_map": plan.pointer("/video_output/tone_map").cloned().unwrap_or(Value::Null),
        "hardware_acceleration": plan.get("hardware_acceleration").cloned().unwrap_or(Value::Null),
        "workload_class": plan.get("workload_class").cloned().unwrap_or(Value::Null),
        "feasibility": plan.get("feasibility").cloned().unwrap_or(Value::Null),
        "feasibility_remediation": feasibility_remediation_from_value(plan.get("feasibility")),
        "warnings": plan.get("warnings").cloned().unwrap_or(Value::Array(Vec::new())),
    }))
}

fn feasibility_remediation_from_value(feasibility: Option<&Value>) -> Value {
    let reason = feasibility
        .and_then(|value| value.get("reason"))
        .and_then(Value::as_str)
        .unwrap_or("playback_not_playable");
    let remediation_codes = feasibility
        .and_then(|value| value.get("remediation_codes"))
        .and_then(Value::as_array)
        .map(|codes| {
            codes
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    playback_feasibility_remediation(reason, &remediation_codes)
}

fn job_snapshot_from_state(job_state: Option<&Value>) -> Option<Value> {
    let state = job_state?;
    let error = state
        .get("error")
        .and_then(Value::as_str)
        .map(redact_playback_diagnostics_text)
        .map(Value::String)
        .unwrap_or(Value::Null);
    Some(serde_json::json!({
        "state": state.get("state").cloned().unwrap_or(Value::Null),
        "mode": state.get("mode").cloned().unwrap_or(Value::Null),
        "delivery": state.get("delivery").cloned().unwrap_or(Value::Null),
        "logical_start_seconds": state.get("logical_start_seconds").cloned().unwrap_or(Value::Null),
        "started_at": state.get("started_at").cloned().unwrap_or(Value::Null),
        "last_progress_at": state.get("last_progress_at").cloned().unwrap_or(Value::Null),
        "last_segment_at": state.get("last_segment_at").cloned().unwrap_or(Value::Null),
        "error": error,
        "error_code": state.get("error_code").cloned().unwrap_or(Value::Null),
        "error_kind": state.get("error_kind").cloned().unwrap_or(Value::Null),
        "artifacts": state.get("artifacts").cloned().unwrap_or(Value::Array(Vec::new())),
        "active_rung": state.get("active_rung").cloned().unwrap_or(Value::Null),
    }))
}

fn delivery_from_diagnostics(
    playback_plan: Option<&Value>,
    job_state: Option<&Value>,
    mode: &str,
) -> String {
    playback_plan
        .and_then(|plan| plan.get("delivery").and_then(Value::as_str))
        .or_else(|| job_state.and_then(|state| state.get("delivery").and_then(Value::as_str)))
        .unwrap_or_else(|| {
            if mode == "direct_play" {
                "direct_file"
            } else {
                "hls_mpegts"
            }
        })
        .to_string()
}

fn server_seek_required_from_plan(playback_plan: Option<&Value>) -> bool {
    playback_plan
        .and_then(|plan| plan.get("seek_behavior").and_then(Value::as_str))
        .is_some_and(|seek_behavior| seek_behavior == "server_hls_restart")
}

fn decision_reason_from_plan(playback_plan: Option<&Value>) -> Option<String> {
    playback_plan
        .and_then(|plan| plan.get("reasons").and_then(Value::as_array))
        .and_then(|reasons| reasons.first())
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn decision_reasons_from_plan(playback_plan: Option<&Value>) -> Vec<String> {
    playback_plan
        .and_then(|plan| plan.get("reasons").and_then(Value::as_array))
        .map(|reasons| {
            reasons
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn ffmpeg_log_tail_from_state(
    job_state: Option<&Value>,
    transcode_state: Option<&Value>,
) -> Option<String> {
    job_state
        .and_then(|state| state.get("log_tail").and_then(Value::as_str))
        .or_else(|| transcode_state.and_then(|state| state.get("log_tail").and_then(Value::as_str)))
        .map(redact_playback_diagnostics_text)
        .filter(|tail| !tail.trim().is_empty())
}

fn redact_playback_diagnostics_text(raw: &str) -> String {
    raw.lines()
        .map(|line| {
            let mut redact_next = false;
            line.split_whitespace()
                .map(|token| {
                    let lower = token.to_ascii_lowercase();
                    let lower_scheme = lower.trim_end_matches(':');
                    let is_auth_scheme = matches!(
                        lower_scheme,
                        "bearer" | "basic" | "digest" | "token" | "apikey" | "api-key"
                    );
                    if redact_next {
                        redact_next = is_auth_scheme;
                        "[redacted]".to_string()
                    } else if lower.contains("authorization:") || is_auth_scheme {
                        redact_next = true;
                        "[redacted]".to_string()
                    } else {
                        redact_playback_url_for_log(token)
                    }
                })
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn playback_not_playable_details(playback_plan: &PlaybackPlan) -> Value {
    let plan_value = serde_json::to_value(playback_plan).ok();
    let remediation = playback_plan
        .feasibility
        .as_ref()
        .map(|feasibility| {
            playback_feasibility_remediation(
                feasibility.reason.as_str(),
                &feasibility.remediation_codes,
            )
        })
        .unwrap_or_else(|| playback_feasibility_remediation("playback_not_playable", &[]));
    serde_json::json!({
        "reason": playback_plan.decision_reason(),
        "reasons": playback_plan.reasons.clone(),
        "workload_class": playback_plan.workload_class.clone(),
        "feasibility": playback_plan.feasibility.clone(),
        "remediation": remediation,
        "plan_summary": plan_summary_from_plan(plan_value.as_ref(), None),
    })
}

fn playback_capacity_retry_details(playback_plan: &PlaybackPlan) -> Value {
    let plan_value = serde_json::to_value(playback_plan).ok();
    let remediation = playback_plan
        .feasibility
        .as_ref()
        .map(|feasibility| {
            playback_feasibility_remediation(
                feasibility.reason.as_str(),
                &feasibility.remediation_codes,
            )
        })
        .unwrap_or_else(|| playback_feasibility_remediation("transcode_capacity_exhausted", &[]));
    serde_json::json!({
        "reason": playback_plan.decision_reason(),
        "reasons": playback_plan.reasons.clone(),
        "workload_class": playback_plan.workload_class.clone(),
        "feasibility": playback_plan.feasibility.clone(),
        "remediation": remediation,
        "plan_summary": plan_summary_from_plan(plan_value.as_ref(), None),
        "retry": {
            "allowed": true,
            "after_seconds": 30,
            "strategy": "retry_same_request"
        },
        "fallback": {
            "server_recovery_required": true,
            "automatic_client_retry": false
        }
    })
}

fn playback_feasibility_remediation(reason: &str, remediation_codes: &[String]) -> Value {
    let (user_message, admin_message, actions): (&str, &str, Vec<&str>) = match reason {
        "hardware_decode_unsupported" => (
            "This server cannot hardware-decode this video on the current GPU/driver path.",
            "Hardware decode is unsupported for the selected source workload. Verify the GPU driver, FFmpeg hardware decoder, and codec/profile capability row.",
            vec![
                "update_gpu_driver",
                "try_original_quality",
                "allow_software_decode_or_lower_quality",
            ],
        ),
        "hardware_encode_unsupported" => (
            "This server cannot hardware-encode the requested output on the current GPU/driver path.",
            "Hardware encode is unsupported for the selected output codec/profile/resolution. Verify FFmpeg encoder support and platform-specific encoder limits.",
            vec![
                "update_gpu_driver",
                "lower_quality",
                "allow_software_encode",
            ],
        ),
        "hardware_filter_unsupported" => (
            "This server cannot run the required hardware video filters for this playback.",
            "The selected scale, tone-map, format conversion, or subtitle path is unsupported on the selected hardware API.",
            vec![
                "lower_quality",
                "disable_subtitle_burn_in",
                "use_software_filter_path",
            ],
        ),
        "software_decode_unsupported" => (
            "This server cannot decode this video format.",
            "The selected source codec is not in the server's supported FFmpeg software decode set, and no hardware decoder was selected for this playback.",
            vec![
                "try_original_quality",
                "use_a_client_that_supports_the_source_codec",
                "replace_or_remux_media",
            ],
        ),
        "server_cannot_realtime_tonemap_source" => (
            "This server cannot convert this HDR video to SDR in real time.",
            "The selected HDR tone-map workload is below the realtime performance envelope for this host.",
            vec![
                "try_original_quality",
                "use_hdr_capable_client",
                "lower_quality",
                "upgrade_server_hardware",
            ],
        ),
        "server_cannot_realtime_burn_subtitles" => (
            "This server cannot burn these subtitles into video in real time.",
            "Subtitle burn-in is below the realtime envelope for the selected source/output workload.",
            vec![
                "disable_subtitle_burn_in",
                "choose_text_subtitles",
                "lower_quality",
            ],
        ),
        "server_cannot_realtime_transcode_source" => (
            "This server cannot convert this video in real time.",
            "The selected transcode workload is below the realtime performance envelope for this host.",
            vec![
                "try_original_quality",
                "lower_quality",
                "upgrade_server_hardware",
            ],
        ),
        "transcode_performance_unknown_policy_denied" => (
            "This server has not verified that it can convert this video in real time.",
            "Unknown workload performance is configured fail-closed. Seed a certification artifact, allow bounded local probes, or explicitly permit best-effort playback.",
            vec![
                "try_original_quality",
                "lower_quality",
                "enable_certification_seed_or_probe",
            ],
        ),
        "transcode_capacity_exhausted" => (
            "This server is already busy converting other videos.",
            "Playback transcode capacity admission rejected this request before starting FFmpeg.",
            vec![
                "retry_later",
                "try_original_quality",
                "increase_transcode_capacity",
            ],
        ),
        _ => (
            "This server cannot play this item with the requested settings.",
            "Playback planning returned no feasible direct, remux, transcode, fallback, or downgrade path.",
            vec![
                "try_original_quality",
                "lower_quality",
                "check_server_playback_settings",
            ],
        ),
    };
    serde_json::json!({
        "user_message": user_message,
        "admin_message": admin_message,
        "reason": reason,
        "remediation_codes": remediation_codes,
        "suggested_actions": actions,
    })
}

fn rung_from_ladder(ladder: &Value, rung_id: &str) -> Option<Value> {
    ladder
        .get("rungs")
        .and_then(Value::as_array)
        .and_then(|rungs| {
            rungs.iter().find(|rung| {
                rung.get("id")
                    .and_then(Value::as_str)
                    .is_some_and(|id| id == rung_id)
            })
        })
        .cloned()
}

async fn active_video_transcode_count(state: &AppState) -> ApiResult<u32> {
    let count_text: Option<String> = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT CAST(COALESCE(SUM(CASE WHEN mode = 'adaptive_transcode' THEN 2 ELSE 1 END), 0) AS TEXT) FROM playback_sessions
         WHERE state = 'active'
           AND mode IN ('video_transcode', 'adaptive_transcode', 'transcode')",
    )
    .fetch_optional(&state.db_pool)
    .await
    .map_err(|e| ApiError::internal(e.to_string()))?;

    Ok(count_text
        .as_deref()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(0))
}

fn validated_start_position_seconds(value: Option<f64>) -> ApiResult<Option<i32>> {
    match value {
        Some(seconds) if !seconds.is_finite() || seconds < 0.0 => {
            Err(ApiError::bad_request("invalid start_position_seconds"))
        }
        Some(seconds) if seconds > i32::MAX as f64 => {
            Err(ApiError::bad_request("invalid start_position_seconds"))
        }
        Some(seconds) => Ok(Some(seconds.round() as i32)),
        None => Ok(None),
    }
}

fn playback_probe_error(error: MediaProbeError) -> ApiError {
    match error {
        MediaProbeError::ProbeRequired => ApiError::conflict_code(
            "probe_failed",
            "media probe is required before playback can start",
            serde_json::json!({
                "reason": "probe_required",
                "retry": {
                    "allowed": true,
                    "strategy": "probe_then_retry"
                }
            }),
        ),
        MediaProbeError::ProbeFailed(message) => ApiError::conflict_code(
            "probe_failed",
            format!("probe_failed: {message}"),
            serde_json::json!({
                "reason": "probe_failed",
                "detail": message,
                "retry": {
                    "allowed": true,
                    "strategy": "rescan_or_reprobe_then_retry"
                }
            }),
        ),
        MediaProbeError::Database(err) => ApiError::internal(err.to_string()),
        MediaProbeError::Other(err) => ApiError::internal(err.to_string()),
    }
}

fn source_unreadable_error(path: &str, detail: String) -> ApiError {
    ApiError::structured(
        StatusCode::NOT_FOUND,
        "source_unreadable",
        "media source is not readable",
        Some(serde_json::json!({
            "path": path,
            "detail": detail,
            "retry": {
                "allowed": true,
                "strategy": "verify_source_then_retry"
            }
        })),
    )
}

fn client_kind_from_caps(caps: &ClientCapabilities) -> ClientKind {
    caps.client_kind
        .as_deref()
        .map(|kind| match kind.to_ascii_lowercase().as_str() {
            value if value.contains("mpv") || value.contains("native") => ClientKind::NativeMpv,
            value if value.contains("web") || value.contains("browser") => ClientKind::Web,
            value if value.contains("tv") => ClientKind::Tv,
            value
                if value.contains("mobile")
                    || value.contains("ios")
                    || value.contains("android") =>
            {
                ClientKind::Mobile
            }
            _ => ClientKind::Unknown,
        })
        .unwrap_or(ClientKind::Unknown)
}

fn normalize_codec_token(value: String) -> String {
    value.trim().to_ascii_lowercase()
}

fn subtitle_burn_policy(value: &str) -> SubtitleBurnPolicy {
    match value.trim().to_ascii_lowercase().as_str() {
        "never" => SubtitleBurnPolicy::Never,
        "image_only" | "image-only" => SubtitleBurnPolicy::ImageOnly,
        "always" => SubtitleBurnPolicy::Always,
        _ => SubtitleBurnPolicy::Automatic,
    }
}

fn subtitle_rendering(value: &str) -> SubtitleRendering {
    match value.trim().to_ascii_lowercase().as_str() {
        "native" => SubtitleRendering::Native,
        "sidecar" => SubtitleRendering::Sidecar,
        "burn_in_only" | "burn-in-only" | "burnin" => SubtitleRendering::BurnInOnly,
        _ => SubtitleRendering::HlsWebvtt,
    }
}

fn ass_complexity_support(value: &str) -> AssComplexitySupport {
    match value.trim().to_ascii_lowercase().as_str() {
        "native" | "full_native" | "full-native" => AssComplexitySupport::Native,
        "simple_webvtt" | "simple-webvtt" | "webvtt" => AssComplexitySupport::SimpleWebvtt,
        "unsupported" | "none" => AssComplexitySupport::Unsupported,
        _ => AssComplexitySupport::BurnIn,
    }
}

fn image_subtitle_support(value: &str) -> ImageSubtitleSupport {
    match value.trim().to_ascii_lowercase().as_str() {
        "native" => ImageSubtitleSupport::Native,
        "native_or_burn_in" | "native-or-burn-in" | "native_burn_in" => {
            ImageSubtitleSupport::NativeOrBurnIn
        }
        "unsupported" | "none" => ImageSubtitleSupport::Unsupported,
        _ => ImageSubtitleSupport::BurnIn,
    }
}

fn forced_subtitle_policy(value: &str) -> ForcedSubtitlePolicy {
    match value.trim().to_ascii_lowercase().as_str() {
        "disabled" | "off" | "none" => ForcedSubtitlePolicy::Disabled,
        "any" => ForcedSubtitlePolicy::Any,
        _ => ForcedSubtitlePolicy::MatchingAudio,
    }
}

fn default_subtitle_policy(value: &str) -> DefaultSubtitlePolicy {
    match value.trim().to_ascii_lowercase().as_str() {
        "disabled" | "off" | "none" => DefaultSubtitlePolicy::Disabled,
        _ => DefaultSubtitlePolicy::MediaDefault,
    }
}

fn subtitle_selection_mode(value: Option<&str>) -> SubtitleSelectionMode {
    match value
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "off" | "none" | "disabled" => SubtitleSelectionMode::Off,
        "forced" | "forced_only" | "forced-only" => SubtitleSelectionMode::Forced,
        "track" | "explicit" => SubtitleSelectionMode::Track,
        _ => SubtitleSelectionMode::Default,
    }
}

fn quality_mode(value: &str) -> QualityMode {
    match value.trim().to_ascii_lowercase().as_str() {
        "automatic" | "auto" => QualityMode::Automatic,
        "fixed" => QualityMode::Fixed,
        _ => QualityMode::Original,
    }
}

fn abr_support_type(value: &str) -> AbrSupportType {
    match value.trim().to_ascii_lowercase().as_str() {
        "native_hls" | "native" | "avplayer" | "exoplayer" => AbrSupportType::NativeHls,
        "hls_js" | "hlsjs" | "hls.js" | "web" => AbrSupportType::HlsJs,
        "mpv" | "libmpv" => AbrSupportType::Mpv,
        _ => AbrSupportType::None,
    }
}

fn non_unlimited_resolution(value: String) -> Option<String> {
    (!is_unlimited_resolution(&value)).then_some(value)
}

fn min_resolution(a: &str, b: &str) -> String {
    let rank = |r: &str| -> i32 {
        match r.to_ascii_lowercase().as_str() {
            "480p" => 0,
            "720p" => 1,
            "1080p" => 2,
            "1440p" => 3,
            "4k" | "2160p" => 4,
            "8k" | "4320p" => 5,
            _ if is_unlimited_resolution(r) => i32::MAX,
            _ => 0,
        }
    };
    if rank(a) <= rank(b) {
        a.to_string()
    } else {
        b.to_string()
    }
}

fn native_direct_play_client(caps: &ClientCapabilities) -> bool {
    if caps.direct_play_preferred == Some(true) {
        return true;
    }
    caps.client_kind
        .as_deref()
        .map(|kind| {
            let normalized = kind.to_ascii_lowercase();
            normalized.contains("mpv") || normalized.contains("native")
        })
        .unwrap_or(false)
}

fn classify_playback_network(network_type: Option<&str>) -> NetworkClass {
    match network_type
        .map(|value| value.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("lan" | "local" | "private") => NetworkClass::Lan,
        Some("wan" | "remote" | "public") => NetworkClass::Wan,
        _ => NetworkClass::Unknown,
    }
}

fn positive_bitrate_cap(value: Option<i64>) -> Option<i64> {
    value.filter(|cap| *cap > 0)
}

fn is_unlimited_resolution(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "" | "any" | "none" | "unlimited" | "original" | "source" | "direct" | "native"
    )
}

fn resolution_within_cap(height: i32, max_resolution: Option<&str>) -> bool {
    match max_resolution.map(|value| value.trim().to_ascii_lowercase()) {
        Some(value) if is_unlimited_resolution(&value) => true,
        Some(value) if value == "480p" => height <= 480,
        Some(value) if value == "720p" => height <= 720,
        Some(value) if value == "1080p" => height <= 1080,
        Some(value) if value == "1440p" => height <= 1440,
        Some(value) if value == "4k" || value == "2160p" => height <= 2160,
        Some(value) if value == "8k" || value == "4320p" => height <= 4320,
        _ => true,
    }
}

fn eq_ci(a: Option<&str>, b: Option<&str>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => a.eq_ignore_ascii_case(b),
        _ => false,
    }
}

fn matches_or_unknown(value: Option<&str>, allowed: &[String]) -> bool {
    match value {
        None => true,
        Some(v) => {
            let norm_allowed: Vec<String> =
                allowed.iter().map(|s| normalize_container(s)).collect();
            let tokens: Vec<String> = v
                .split(',')
                .map(|t| normalize_container(t.trim()))
                .collect();
            tokens.iter().any(|t| norm_allowed.iter().any(|a| a == t))
        }
    }
}

fn normalize_container(value: &str) -> String {
    let lower = value.to_ascii_lowercase();
    if lower.contains("matroska") {
        "mkv".to_string()
    } else if lower.contains("mp4") || lower.contains("mov") {
        "mp4".to_string()
    } else {
        lower
    }
}

pub fn profile_for_network(
    config: &crate::config::PlaybackConfig,
    network: Option<&str>,
) -> EffectiveProfile {
    let base = match network {
        Some("lan") => config.profiles.lan.as_ref(),
        Some("wan") => config.profiles.wan.as_ref(),
        _ => None,
    };

    let fallback = match network {
        Some("lan") => config.profiles.wan.as_ref(),
        _ => None,
    };

    let merged = base.or(fallback);

    EffectiveProfile {
        max_resolution: merged
            .and_then(|p| p.max_resolution.clone())
            .unwrap_or_else(|| config.default_max_resolution.clone()),
        supported_containers: merged
            .and_then(|p| p.supported_containers.clone())
            .unwrap_or_else(|| config.default_supported_containers.clone()),
        supported_video_codecs: merged
            .and_then(|p| p.supported_video_codecs.clone())
            .unwrap_or_else(|| config.default_supported_video_codecs.clone()),
        supported_audio_codecs: merged
            .and_then(|p| p.supported_audio_codecs.clone())
            .unwrap_or_else(|| config.default_supported_audio_codecs.clone()),
        max_bitrate_bps: merged
            .and_then(|p| p.max_bitrate_bps)
            .or_else(|| match network {
                Some("lan") => config.default_lan_max_bitrate_bps,
                _ => config.default_wan_max_bitrate_bps,
            }),
    }
}

fn effective_bitrate(file: &FileRow, duration_seconds: Option<i32>) -> i64 {
    if file.bitrate_bps > 0 {
        return file.bitrate_bps;
    }
    if let (Some(size), Some(dur)) = (file.size_bytes, duration_seconds) {
        if dur > 0 {
            return ((size as f64 * 8.0) / dur as f64).round() as i64;
        }
    }
    0
}

const EXTERNAL_SUBTITLE_INDEX_BASE: i32 = -100_000;

async fn attach_external_subtitles(
    state: &AppState,
    media_file_id: &str,
    capabilities: &mut MediaCapabilities,
) -> ApiResult<()> {
    let rows = sqlx::query(
        "SELECT id, path, language, title, format,
                CAST(is_default AS INTEGER) AS is_default,
                CAST(is_forced AS INTEGER) AS is_forced,
                CAST(is_hearing_impaired AS INTEGER) AS is_hearing_impaired
         FROM external_subtitles
         WHERE media_file_id = ?
         ORDER BY path, id",
    )
    .bind(media_file_id)
    .fetch_all(&state.db_pool)
    .await
    .map_err(|e| ApiError::internal(e.to_string()))?;

    for (offset, row) in rows.into_iter().enumerate() {
        let path: String = row.get("path");
        let format = row
            .try_get::<String, _>("format")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| {
                Path::new(&path)
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .map(str::to_string)
            });
        let codec = format.as_deref().map(canonical_subtitle_codec);
        let kind = codec
            .as_deref()
            .map(subtitle_kind)
            .unwrap_or(SubtitleKind::Unknown);
        let is_default = row
            .try_get::<i64, _>("is_default")
            .ok()
            .map(|value| value != 0)
            .unwrap_or(false);
        let is_forced = row
            .try_get::<i64, _>("is_forced")
            .ok()
            .map(|value| value != 0)
            .unwrap_or(false);
        let is_hearing_impaired = row
            .try_get::<i64, _>("is_hearing_impaired")
            .ok()
            .map(|value| value != 0)
            .unwrap_or(false);

        capabilities
            .subtitle_streams
            .push(SubtitleStreamCapabilities {
                index: Some(EXTERNAL_SUBTITLE_INDEX_BASE - offset as i32),
                external_id: Some(row.get("id")),
                codec,
                kind,
                language: row.try_get::<String, _>("language").ok(),
                title: row.try_get::<String, _>("title").ok(),
                is_default,
                is_forced,
                is_hearing_impaired,
                external_path: Some(path),
            });
    }

    Ok(())
}

fn direct_play_sidecar_subtitle_url(
    playback_plan: &PlaybackPlan,
    capabilities: &MediaCapabilities,
    media_file_id: &str,
    session_id: &str,
    session_token: &str,
) -> Option<String> {
    if playback_plan.mode != PlaybackMode::DirectPlay
        || !matches!(
            playback_plan.subtitle_action,
            StreamAction::Copy | StreamAction::Passthrough
        )
    {
        return None;
    }
    let selected_track = playback_plan.selected_subtitle_track?;
    let subtitle = capabilities
        .subtitle_streams
        .iter()
        .find(|stream| stream.index == Some(selected_track))?;
    let external_id = subtitle.external_id.as_ref()?;
    Some(format!(
        "/stream/subtitle/{media_file_id}/{external_id}?sid={session_id}&session={session_token}"
    ))
}

async fn canonical_sidecar_subtitle_path(
    media_path: &str,
    subtitle_path: &str,
) -> ApiResult<PathBuf> {
    let media_parent = Path::new(media_path)
        .parent()
        .ok_or_else(|| ApiError::forbidden("media path has no parent"))?;
    let media_parent = fs::canonicalize(media_parent)
        .await
        .map_err(|err| source_unreadable_error(media_path, err.to_string()))?;
    let subtitle_path = fs::canonicalize(subtitle_path)
        .await
        .map_err(|err| source_unreadable_error(subtitle_path, err.to_string()))?;
    if !subtitle_path.starts_with(&media_parent) {
        return Err(ApiError::forbidden(
            "subtitle path is outside the media directory",
        ));
    }
    Ok(subtitle_path)
}

fn subtitle_content_type(path: &Path, format: Option<&str>) -> String {
    let ext = format
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .or_else(|| {
            path.extension()
                .and_then(|ext| ext.to_str())
                .map(str::to_string)
        })
        .unwrap_or_default()
        .to_ascii_lowercase();
    match ext.as_str() {
        "vtt" | "webvtt" => "text/vtt; charset=utf-8".to_string(),
        "srt" | "subrip" => "application/x-subrip; charset=utf-8".to_string(),
        "ass" => "text/x-ass; charset=utf-8".to_string(),
        "ssa" => "text/x-ssa; charset=utf-8".to_string(),
        "idx" | "sub" => "application/octet-stream".to_string(),
        _ => "text/plain; charset=utf-8".to_string(),
    }
}

pub async fn stream_direct(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Query(params): Query<StreamQuery>,
    method: Method,
    headers: HeaderMap,
    user: CurrentUser,
) -> ApiResult<Response> {
    let session_id = params
        .sid
        .as_deref()
        .ok_or_else(|| ApiError::unauthorized("session id required"))?;
    let session_token = params
        .session
        .as_deref()
        .ok_or_else(|| ApiError::unauthorized("session token required"))?;
    let session_row = get_session_with_token(
        &state,
        &user,
        session_id,
        Some("direct_play"),
        session_token,
    )
    .await?;
    enforce_stream_route_remote_policy(&session_row, StreamRoutePolicyKind::DirectFile)?;
    let session_media_file_id: String = session_row.get("media_file_id");
    if session_media_file_id != id {
        tracing::warn!(
            session = %session_id,
            requested_file = %id,
            session_file = %session_media_file_id,
            "direct stream file mismatch"
        );
        return Err(ApiError::unauthorized("invalid session"));
    }

    let file_row = sqlx::query(
        "SELECT path, container FROM media_files WHERE id = ? AND scan_state = 'ok' LIMIT 1",
    )
    .bind(&id)
    .fetch_optional(&state.db_pool)
    .await
    .map_err(|e| ApiError::internal(e.to_string()))?
    .ok_or_else(|| ApiError::not_found("file not found"))?;

    let path: String = file_row.get("path");
    let container: Option<String> = file_row.try_get("container").ok();

    let mut file = File::open(&path)
        .await
        .map_err(|err| source_unreadable_error(&path, err.to_string()))?;
    let meta = file
        .metadata()
        .await
        .map_err(|err| source_unreadable_error(&path, err.to_string()))?;
    let file_len = meta.len();
    let modified = meta.modified().ok();
    let content_type = content_type_for(&path, container.as_deref());
    let direct_response =
        build_direct_file_response(&method, &headers, file_len, modified, &content_type);
    record_direct_stream_range_status(&direct_response, &method, "direct_file");
    touch_playback_session(&state, session_id, PlaybackActivityKind::DirectRead).await?;

    let body = match direct_response.body {
        DirectFileBody::Empty => Body::empty(),
        DirectFileBody::Full => direct_file_body(
            file,
            DirectReadMetricLabels {
                session_id: session_id.to_string(),
                user_id: user.user_id.to_string(),
                media_file_id: id.clone(),
                delivery: "direct_file".to_string(),
            },
        ),
        DirectFileBody::Range { start, length } => {
            file.seek(SeekFrom::Start(start))
                .await
                .map_err(|e| ApiError::internal(e.to_string()))?;
            direct_file_body(
                file.take(length),
                DirectReadMetricLabels {
                    session_id: session_id.to_string(),
                    user_id: user.user_id.to_string(),
                    media_file_id: id.clone(),
                    delivery: "direct_file".to_string(),
                },
            )
        }
        DirectFileBody::Error(body) => Body::from(body),
    };

    Ok((direct_response.status, direct_response.headers, body).into_response())
}

pub async fn stream_subtitle(
    State(state): State<AppState>,
    AxumPath((id, subtitle_id)): AxumPath<(String, String)>,
    Query(params): Query<StreamQuery>,
    method: Method,
    headers: HeaderMap,
    user: CurrentUser,
) -> ApiResult<Response> {
    let session_id = params
        .sid
        .as_deref()
        .ok_or_else(|| ApiError::unauthorized("session id required"))?;
    let session_token = params
        .session
        .as_deref()
        .ok_or_else(|| ApiError::unauthorized("session token required"))?;
    let session_row = get_session_with_token(
        &state,
        &user,
        session_id,
        Some("direct_play"),
        session_token,
    )
    .await?;
    enforce_stream_route_remote_policy(&session_row, StreamRoutePolicyKind::DirectFile)?;
    let session_media_file_id: String = session_row.get("media_file_id");
    if session_media_file_id != id {
        tracing::warn!(
            session = %session_id,
            requested_file = %id,
            session_file = %session_media_file_id,
            "sidecar subtitle file mismatch"
        );
        return Err(ApiError::unauthorized("invalid session"));
    }

    let subtitle_row = sqlx::query(
        "SELECT es.path, es.format, mf.path AS media_path
         FROM external_subtitles es
         JOIN media_files mf ON mf.id = es.media_file_id
         WHERE es.id = ? AND es.media_file_id = ? AND mf.scan_state = 'ok'
         LIMIT 1",
    )
    .bind(&subtitle_id)
    .bind(&id)
    .fetch_optional(&state.db_pool)
    .await
    .map_err(|e| ApiError::internal(e.to_string()))?
    .ok_or_else(|| ApiError::not_found("subtitle not found"))?;

    let subtitle_path: String = subtitle_row.get("path");
    let media_path: String = subtitle_row.get("media_path");
    let subtitle_path = canonical_sidecar_subtitle_path(&media_path, &subtitle_path).await?;
    let format: Option<String> = subtitle_row.try_get("format").ok();

    let mut file = File::open(&subtitle_path).await.map_err(|err| {
        source_unreadable_error(&subtitle_path.to_string_lossy(), err.to_string())
    })?;
    let meta = file.metadata().await.map_err(|err| {
        source_unreadable_error(&subtitle_path.to_string_lossy(), err.to_string())
    })?;
    let file_len = meta.len();
    let modified = meta.modified().ok();
    let content_type = subtitle_content_type(&subtitle_path, format.as_deref());
    let direct_response =
        build_direct_file_response(&method, &headers, file_len, modified, &content_type);
    record_direct_stream_range_status(&direct_response, &method, "direct_subtitle");
    touch_playback_session(&state, session_id, PlaybackActivityKind::DirectSubtitleRead).await?;

    let body = match direct_response.body {
        DirectFileBody::Empty => Body::empty(),
        DirectFileBody::Full => direct_file_body(
            file,
            DirectReadMetricLabels {
                session_id: session_id.to_string(),
                user_id: user.user_id.to_string(),
                media_file_id: id.clone(),
                delivery: "direct_subtitle".to_string(),
            },
        ),
        DirectFileBody::Range { start, length } => {
            file.seek(SeekFrom::Start(start))
                .await
                .map_err(|e| ApiError::internal(e.to_string()))?;
            direct_file_body(
                file.take(length),
                DirectReadMetricLabels {
                    session_id: session_id.to_string(),
                    user_id: user.user_id.to_string(),
                    media_file_id: id.clone(),
                    delivery: "direct_subtitle".to_string(),
                },
            )
        }
        DirectFileBody::Error(body) => Body::from(body),
    };

    Ok((direct_response.status, direct_response.headers, body).into_response())
}

pub async fn master_playlist(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    user: CurrentUser,
    Query(params): Query<StreamQuery>,
) -> ApiResult<impl IntoResponse> {
    let session_id = Uuid::parse_str(&id).map_err(|_| ApiError::bad_request("invalid session"))?;
    let session_token = params
        .session
        .as_deref()
        .ok_or_else(|| ApiError::unauthorized("session token required"))?;
    let session_row = get_session_with_token(
        &state,
        &user,
        &session_id.to_string(),
        Some("hls"),
        session_token,
    )
    .await?;
    enforce_stream_route_remote_policy(&session_row, StreamRoutePolicyKind::Hls)?;
    touch_playback_session(&state, &id, PlaybackActivityKind::HlsMasterPlaylist).await?;
    let media_file_id: String = session_row.get("media_file_id");
    let transcode_state: Option<String> = session_row.try_get("transcode_state").ok();
    let seek_seconds = transcode_state
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
        .and_then(|v| v.get("seek_seconds").and_then(Value::as_f64))
        .unwrap_or(0.0) as f32;

    let file_row = sqlx::query::<sqlx::Any>("SELECT path FROM media_files WHERE id = ? LIMIT 1")
        .bind(&media_file_id)
        .fetch_optional(&state.db_pool)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;
    let file_row = file_row.ok_or_else(|| ApiError::not_found("file not found"))?;
    let media_path: String = file_row.get("path");

    let playback_plan_json = session_row
        .try_get::<String, _>("playback_plan_json")
        .ok()
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok());
    let transcode_params = transcode_params_from_session(&session_row, seek_seconds);
    let hls_start_time = std::time::Instant::now();
    let hls_mode = transcode_params.mode.as_str();
    let hls_delivery = transcode_params.delivery.as_str();
    let planned_hardware_label = hardware_metric_label_from_plan_value(playback_plan_json.as_ref());
    let handle = state
        .transcodes
        .start(
            PlaybackJobPlan::new(
                session_id,
                media_file_id.clone(),
                media_path.clone(),
                transcode_params,
                playback_plan_json.clone(),
            ),
            seek_seconds,
        )
        .await
        .map_err(|e| {
            let error = e.to_string();
            let error_code = playback_job_start_error_code(&error);
            TRANSCODE_ERRORS.with_label_values(&[error_code]).inc();
            PLAYBACK_HLS_PLAYLIST_STARTUP_LATENCY
                .with_label_values(&[hls_mode, hls_delivery, planned_hardware_label.as_str()])
                .observe(hls_start_time.elapsed().as_secs_f64());
            playback_job_start_error(error)
        })?;
    let hardware_label = hardware_metric_label_from_job_state(&handle.job_state);
    TRANSCODE_STARTS
        .with_label_values(&["ok", "unknown", "unknown", hardware_label.as_str()])
        .inc();
    info!(
        session = %session_id,
        file = %media_file_id,
        log = %handle.log_path.to_string_lossy(),
        "transcode start or resume"
    );
    let start_time = std::time::Instant::now();

    // Wait briefly for playlist to appear; retry a few times to avoid flakiness.
    let content = match read_playlist_with_retry(
        &handle.playlist_path,
        60,
        250,
        Some(&handle.log_path),
    )
    .await
    {
        Ok(c) => c,
        Err(err) => {
            let msg = format!("{err:?}");
            let session_id = id.clone();
            let state_clone = state.clone();
            let hardware_label = hardware_metric_label_from_plan_value(playback_plan_json.as_ref());
            TRANSCODE_STARTS
                .with_label_values(&["error", "unknown", "unknown", hardware_label.as_str()])
                .inc();
            TRANSCODE_ERRORS
                .with_label_values(&["segment_timeout"])
                .inc();
            PLAYBACK_HLS_PLAYLIST_STARTUP_LATENCY
                .with_label_values(&[hls_mode, hls_delivery, hardware_label.as_str()])
                .observe(hls_start_time.elapsed().as_secs_f64());
            let _ = tokio::spawn(async move {
                mark_session_error(
                    state_clone,
                    &session_id,
                    Some(msg),
                    Some(handle.log_path.to_string_lossy().to_string()),
                )
                .await
            });
            return Err(err);
        }
    };
    let mut playlist_body = content;
    let adaptive_ladder_plan = adaptive_ladder_plan_from_plan_value(playback_plan_json.as_ref());
    if let Some(ladder) = adaptive_ladder_plan.as_ref() {
        playlist_body = normalize_adaptive_master_playlist_metadata(&playlist_body, ladder);
        if let Err(err) = validate_adaptive_master_playlist_metadata(&playlist_body, ladder) {
            tracing::warn!(
                session = %session_id,
                error = %err,
                "adaptive master playlist metadata failed validation"
            );
        }
    }
    if !handle.subtitles.is_empty() {
        let renditions = build_subtitle_renditions(&handle.subtitles, &handle.temp_dir).await;
        if !renditions.is_empty() {
            let ready =
                wait_for_subtitle_segments(&handle.temp_dir, renditions.len(), 20, 150).await;
            if !ready {
                tracing::warn!(
                    session = %session_id,
                    count = renditions.len(),
                    "subtitle playlists not ready before master response"
                );
            }
        }
        if !renditions.is_empty() {
            playlist_body = inject_subtitle_media(
                &playlist_body,
                &renditions,
                session_token,
                params.token.as_deref(),
                params.ts.as_deref(),
            );
        }
    }
    let playlist_body = rewrite_playlist_with_token(
        &playlist_body,
        session_token,
        params.token.as_deref(),
        params.ts.as_deref(),
    );

    let _ = sqlx::query::<sqlx::Any>(
        "UPDATE playback_sessions SET updated_at = CURRENT_TIMESTAMP, state = 'active', logical_position_seconds = ? WHERE id = ?",
    )
    .bind(seek_seconds)
    .bind(&id)
    .execute(&state.db_pool)
    .await;
    TRANSCODE_DURATION
        .with_label_values(&["ok"])
        .observe(start_time.elapsed().as_secs_f64());
    PLAYBACK_HLS_PLAYLIST_STARTUP_LATENCY
        .with_label_values(&[hls_mode, hls_delivery, hardware_label.as_str()])
        .observe(hls_start_time.elapsed().as_secs_f64());

    Ok((
        StatusCode::OK,
        [(
            CONTENT_TYPE,
            HeaderValue::from_static("application/vnd.apple.mpegurl"),
        )],
        playlist_body,
    ))
}

pub async fn serve_segment(
    State(state): State<AppState>,
    AxumPath((id, segment)): AxumPath<(String, String)>,
    user: CurrentUser,
    Query(params): Query<StreamQuery>,
) -> ApiResult<impl IntoResponse> {
    let segment_start_time = std::time::Instant::now();
    let session_id = Uuid::parse_str(&id).map_err(|_| ApiError::bad_request("invalid session"))?;
    // Ensure session belongs to user and token matches
    let session_token = params
        .session
        .as_deref()
        .ok_or_else(|| ApiError::unauthorized("session token required"))?;
    let session_row = get_session_with_token(
        &state,
        &user,
        &session_id.to_string(),
        Some("hls"),
        session_token,
    )
    .await?;
    enforce_stream_route_remote_policy(&session_row, StreamRoutePolicyKind::Hls)?;
    let artifact = state
        .transcodes
        .artifact_path(session_id, &segment)
        .await
        .ok_or_else(|| {
            PLAYBACK_MISSING_SEGMENTS
                .with_label_values(&["unregistered"])
                .inc();
            PLAYBACK_SEGMENT_LATENCY
                .with_label_values(&["missing", "unregistered"])
                .observe(segment_start_time.elapsed().as_secs_f64());
            ApiError::not_found("segment not found")
        })?;
    let artifact_kind = artifact_kind_label(artifact.kind);
    let path = canonical_artifact_path(&artifact).await?;
    info!(
        session = %session_id,
        artifact = %artifact.name,
        kind = ?artifact.kind,
        "serving hls artifact"
    );
    if fs::metadata(&path).await.is_err() {
        SEGMENT_SERVED.with_label_values(&["missing"]).inc();
        PLAYBACK_MISSING_SEGMENTS
            .with_label_values(&[artifact_kind])
            .inc();
        PLAYBACK_SEGMENT_LATENCY
            .with_label_values(&["missing", artifact_kind])
            .observe(segment_start_time.elapsed().as_secs_f64());
        return Err(ApiError::not_found("segment not found"));
    }
    touch_playback_session(
        &state,
        &session_id.to_string(),
        PlaybackActivityKind::from_artifact_kind(artifact.kind),
    )
    .await?;
    let ext = Path::new(&artifact.name)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    let subtitle_delay = if ext == "vtt" {
        resolve_subtitle_delay(&state, session_id).await
    } else {
        None
    };

    if matches!(
        artifact.kind,
        ArtifactKind::MediaPlaylist | ArtifactKind::SubtitlePlaylist
    ) {
        let raw = fs::read_to_string(&path).await.map_err(|e| {
            SEGMENT_SERVED.with_label_values(&["error"]).inc();
            PLAYBACK_SEGMENT_LATENCY
                .with_label_values(&["error", artifact_kind])
                .observe(segment_start_time.elapsed().as_secs_f64());
            ApiError::internal(e.to_string())
        })?;
        let rewritten = rewrite_playlist_with_token(
            &raw,
            session_token,
            params.token.as_deref(),
            params.ts.as_deref(),
        );
        SEGMENT_SERVED.with_label_values(&["ok"]).inc();
        PLAYBACK_SEGMENT_LATENCY
            .with_label_values(&["ok", artifact_kind])
            .observe(segment_start_time.elapsed().as_secs_f64());
        return Ok((
            StatusCode::OK,
            [(
                CONTENT_TYPE,
                HeaderValue::from_static("application/vnd.apple.mpegurl"),
            )],
            rewritten.into_bytes(),
        ));
    }

    if artifact.kind == ArtifactKind::SubtitleSegment {
        let raw = fs::read_to_string(&path).await.map_err(|e| {
            SEGMENT_SERVED.with_label_values(&["error"]).inc();
            PLAYBACK_SEGMENT_LATENCY
                .with_label_values(&["error", artifact_kind])
                .observe(segment_start_time.elapsed().as_secs_f64());
            ApiError::internal(e.to_string())
        })?;
        let adjusted = match subtitle_delay {
            Some(delay) if delay.abs() >= 0.01 => shift_vtt_cues(&raw, delay),
            _ => raw,
        };
        SEGMENT_SERVED.with_label_values(&["ok"]).inc();
        PLAYBACK_SEGMENT_LATENCY
            .with_label_values(&["ok", artifact_kind])
            .observe(segment_start_time.elapsed().as_secs_f64());
        return Ok((
            StatusCode::OK,
            [(
                CONTENT_TYPE,
                HeaderValue::from_static("text/vtt; charset=utf-8"),
            )],
            adjusted.into_bytes(),
        ));
    }

    let data = fs::read(&path).await.map_err(|e| {
        SEGMENT_SERVED.with_label_values(&["error"]).inc();
        PLAYBACK_SEGMENT_LATENCY
            .with_label_values(&["error", artifact_kind])
            .observe(segment_start_time.elapsed().as_secs_f64());
        ApiError::internal(e.to_string())
    })?;
    SEGMENT_SERVED.with_label_values(&["ok"]).inc();
    PLAYBACK_SEGMENT_LATENCY
        .with_label_values(&["ok", artifact_kind])
        .observe(segment_start_time.elapsed().as_secs_f64());
    let content_type = match ext.as_str() {
        "ts" => "video/MP2T",
        "m4s" => "video/iso.segment",
        "mp4" => "video/mp4",
        _ => "application/octet-stream",
    };
    Ok((
        StatusCode::OK,
        [(CONTENT_TYPE, HeaderValue::from_static(content_type))],
        data,
    ))
}

#[derive(Debug, Deserialize)]
pub struct SeekRequest {
    pub position_seconds: f32,
}

#[derive(Debug, Deserialize)]
pub struct StreamQuery {
    pub session: Option<String>,
    pub sid: Option<String>,
    pub token: Option<String>,
    pub ts: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SessionDetailResponse {
    pub id: String,
    pub media_file_id: String,
    pub server_id: Option<String>,
    pub mode: String,
    pub delivery: String,
    pub state: String,
    pub network_type: Option<String>,
    pub logical_position_seconds: f32,
    pub duration_seconds: Option<i32>,
    pub server_seek_required: bool,
    pub decision_reason: Option<String>,
    pub decision_reasons: Vec<String>,
    pub wan_direct_endpoint: Option<String>,
    pub stream_token_expires_at: Option<String>,
    pub remote_access: Option<RemoteAccessContract>,
    pub remote_policy: Option<RemotePlaybackPolicySnapshot>,
    pub playback_plan: Option<serde_json::Value>,
    pub plan_summary: Option<serde_json::Value>,
    pub job_snapshot: Option<serde_json::Value>,
    pub adaptive_ladder: Option<serde_json::Value>,
    pub starting_rung: Option<serde_json::Value>,
    pub active_rung: Option<serde_json::Value>,
    pub ffmpeg_log_tail: Option<String>,
    pub error: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, Copy)]
enum PlaybackActivityKind {
    DirectRead,
    DirectSubtitleRead,
    HlsMasterPlaylist,
    HlsMediaPlaylist,
    HlsSegment,
    HlsSubtitlePlaylist,
    HlsSubtitleSegment,
    SessionDetail,
    SessionPoll,
    Seek,
    Heartbeat,
}

impl PlaybackActivityKind {
    fn as_str(self) -> &'static str {
        match self {
            PlaybackActivityKind::DirectRead => "direct_read",
            PlaybackActivityKind::DirectSubtitleRead => "direct_subtitle_read",
            PlaybackActivityKind::HlsMasterPlaylist => "hls_master_playlist",
            PlaybackActivityKind::HlsMediaPlaylist => "hls_media_playlist",
            PlaybackActivityKind::HlsSegment => "hls_segment",
            PlaybackActivityKind::HlsSubtitlePlaylist => "hls_subtitle_playlist",
            PlaybackActivityKind::HlsSubtitleSegment => "hls_subtitle_segment",
            PlaybackActivityKind::SessionDetail => "session_detail",
            PlaybackActivityKind::SessionPoll => "session_poll",
            PlaybackActivityKind::Seek => "seek",
            PlaybackActivityKind::Heartbeat => "heartbeat",
        }
    }

    fn from_artifact_kind(kind: ArtifactKind) -> Self {
        match kind {
            ArtifactKind::MasterPlaylist => PlaybackActivityKind::HlsMasterPlaylist,
            ArtifactKind::MediaPlaylist => PlaybackActivityKind::HlsMediaPlaylist,
            ArtifactKind::InitSegment | ArtifactKind::MediaSegment => {
                PlaybackActivityKind::HlsSegment
            }
            ArtifactKind::SubtitlePlaylist => PlaybackActivityKind::HlsSubtitlePlaylist,
            ArtifactKind::SubtitleSegment => PlaybackActivityKind::HlsSubtitleSegment,
        }
    }
}

fn artifact_kind_label(kind: ArtifactKind) -> &'static str {
    match kind {
        ArtifactKind::MasterPlaylist => "master_playlist",
        ArtifactKind::MediaPlaylist => "media_playlist",
        ArtifactKind::InitSegment => "init_segment",
        ArtifactKind::MediaSegment => "media_segment",
        ArtifactKind::SubtitlePlaylist => "subtitle_playlist",
        ArtifactKind::SubtitleSegment => "subtitle_segment",
    }
}

async fn touch_playback_session(
    state: &AppState,
    session_id: &str,
    activity: PlaybackActivityKind,
) -> ApiResult<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE playback_sessions SET updated_at = CURRENT_TIMESTAMP WHERE id = ?",
    )
    .bind(session_id)
    .execute(&state.db_pool)
    .await
    .map_err(|e| ApiError::internal(e.to_string()))?;
    tracing::debug!(
        session = %session_id,
        activity = activity.as_str(),
        "playback session activity"
    );
    Ok(())
}

async fn canonical_artifact_path(artifact: &PlaybackArtifact) -> ApiResult<PathBuf> {
    let canonical_temp_dir = fs::canonicalize(&artifact.temp_dir)
        .await
        .map_err(|_| ApiError::not_found("segment not found"))?;
    let canonical_path = fs::canonicalize(&artifact.path)
        .await
        .map_err(|_| ApiError::not_found("segment not found"))?;
    if !canonical_path.starts_with(&canonical_temp_dir) {
        tracing::warn!(
            artifact = %artifact.name,
            path = %canonical_path.to_string_lossy(),
            temp_dir = %canonical_temp_dir.to_string_lossy(),
            "hls artifact escaped temp directory"
        );
        return Err(ApiError::not_found("segment not found"));
    }
    Ok(canonical_path)
}

async fn get_session(
    state: &AppState,
    user: &CurrentUser,
    session_id: &str,
    expected_mode: Option<&str>,
    require_active: bool,
) -> ApiResult<AnyRow> {
    let row = sqlx::query("SELECT id, user_id, server_id, media_file_id, mode, state, network_type, logical_position_seconds, duration_seconds, transcode_state, playback_plan_json, job_state_json, token, CAST(token_expires_at AS TEXT) as token_expires_at, share_id, remote_policy_json, CAST(updated_at AS TEXT) as updated_at FROM playback_sessions WHERE id = ? LIMIT 1")
        .bind(session_id)
        .fetch_optional(&state.db_pool)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;

    let row = row.ok_or_else(|| ApiError::unauthorized("invalid session"))?;

    let user_id: String = row.get("user_id");
    if user_id != user.user_id.to_string() {
        return Err(ApiError::unauthorized("invalid session"));
    }

    if require_active {
        let state_value: String = row.get("state");
        if state_value.to_ascii_lowercase() != "active" {
            return Err(ApiError::unauthorized("invalid session"));
        }
        if playback_session_expired(&row, state.settings.playback.session_ttl_seconds) {
            expire_active_playback_session(state, session_id).await?;
            return Err(ApiError::unauthorized("invalid session"));
        }
    }

    if let Some(expected) = expected_mode {
        let mode: String = row.get("mode");
        if !session_mode_matches(&mode, expected) {
            return Err(ApiError::unauthorized("invalid session"));
        }
    }

    Ok(row)
}

async fn expire_active_playback_session(state: &AppState, session_id: &str) -> ApiResult<()> {
    expire_active_playback_session_with_reason(state, session_id, "ttl", "session_expired").await
}

async fn expire_active_playback_session_with_reason(
    state: &AppState,
    session_id: &str,
    metric_reason: &'static str,
    stop_reason: &'static str,
) -> ApiResult<()> {
    if let Ok(id) = Uuid::parse_str(session_id) {
        state.transcodes.stop(id, stop_reason).await;
        PLAYBACK_SESSION_EXPIRATIONS
            .with_label_values(&[metric_reason])
            .inc();
    }
    sqlx::query::<sqlx::Any>(
        "UPDATE playback_sessions
         SET state = 'ended', updated_at = CURRENT_TIMESTAMP
         WHERE id = ? AND state = 'active'",
    )
    .bind(session_id)
    .execute(&state.db_pool)
    .await
    .map_err(|e| ApiError::internal(e.to_string()))?;
    Ok(())
}

fn playback_stream_token_expired(row: &AnyRow) -> bool {
    let Ok(expires_at) = row.try_get::<String, _>("token_expires_at") else {
        return false;
    };
    let trimmed = expires_at.trim();
    if trimmed.is_empty() {
        return false;
    }
    let Some(expires_at) = parse_playback_session_timestamp(trimmed) else {
        return false;
    };
    chrono::Utc::now() > expires_at
}

fn playback_session_expired(row: &AnyRow, ttl_seconds: u64) -> bool {
    let Ok(updated_at) = row.try_get::<String, _>("updated_at") else {
        return false;
    };
    let Some(updated_at) = parse_playback_session_timestamp(updated_at.trim()) else {
        return false;
    };
    let ttl = chrono::Duration::from_std(Duration::from_secs(ttl_seconds))
        .unwrap_or(chrono::Duration::MAX);
    chrono::Utc::now().signed_duration_since(updated_at) > ttl
}

fn parse_playback_session_timestamp(raw: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S") {
        return Some(chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(
            dt,
            chrono::Utc,
        ));
    }
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(raw) {
        return Some(dt.with_timezone(&chrono::Utc));
    }
    None
}

fn session_mode_matches(actual: &str, expected: &str) -> bool {
    if expected == "hls" {
        matches!(
            actual,
            "transcode"
                | "direct_stream"
                | "audio_transcode"
                | "subtitle_transcode"
                | "video_transcode"
                | "adaptive_transcode"
        )
    } else {
        actual == expected
    }
}

fn transcode_params_from_session(row: &AnyRow, seek_seconds: f32) -> TranscodeParams {
    let mode = row
        .try_get::<String, _>("mode")
        .ok()
        .and_then(|value| playback_mode_from_str(&value))
        .unwrap_or(PlaybackMode::VideoTranscode);
    let delivery = row
        .try_get::<String, _>("playback_plan_json")
        .ok()
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
        .and_then(|plan| {
            plan.get("delivery")
                .and_then(Value::as_str)
                .and_then(delivery_from_str)
        })
        .unwrap_or(Delivery::HlsMpegts);

    TranscodeParams {
        seek_seconds,
        mode,
        delivery,
    }
}

fn playback_mode_from_str(value: &str) -> Option<PlaybackMode> {
    match value {
        "direct_stream" => Some(PlaybackMode::DirectStream),
        "audio_transcode" => Some(PlaybackMode::AudioTranscode),
        "subtitle_transcode" => Some(PlaybackMode::SubtitleTranscode),
        "video_transcode" | "transcode" => Some(PlaybackMode::VideoTranscode),
        "adaptive_transcode" => Some(PlaybackMode::AdaptiveTranscode),
        _ => None,
    }
}

fn delivery_from_str(value: &str) -> Option<Delivery> {
    match value {
        "direct_file" => Some(Delivery::DirectFile),
        "hls_fmp4" => Some(Delivery::HlsFmp4),
        "hls_mpegts" => Some(Delivery::HlsMpegts),
        "hls_adaptive_fmp4" => Some(Delivery::HlsAdaptiveFmp4),
        "hls_adaptive_mpegts" => Some(Delivery::HlsAdaptiveMpegts),
        _ => None,
    }
}

async fn read_playlist_with_retry(
    path: &Path,
    attempts: usize,
    backoff_ms: u64,
    log_path: Option<&Path>,
) -> ApiResult<String> {
    let mut last_err = None;
    for _ in 0..attempts {
        match fs::read_to_string(path).await {
            Ok(content) => return Ok(content),
            Err(err) => {
                last_err = Some(err);
                sleep(std::time::Duration::from_millis(backoff_ms)).await;
            }
        }
    }
    let mut msg = format!(
        "playlist not ready: {}",
        last_err
            .map(|e| e.to_string())
            .unwrap_or_else(|| "unknown".to_string())
    );
    if let Some(lp) = log_path {
        msg.push_str(&format!(" (ffmpeg log: {})", lp.to_string_lossy()));
    }
    Err(ApiError::structured(
        StatusCode::GATEWAY_TIMEOUT,
        "segment_timeout",
        msg,
        Some(serde_json::json!({
            "reason": "playlist_not_ready",
            "retry": {
                "allowed": true,
                "after_seconds": 5,
                "strategy": "retry_playlist"
            }
        })),
    ))
}

fn playback_job_start_error_code(error: &str) -> &'static str {
    let lower = error.to_ascii_lowercase();
    if lower.contains("videotoolbox")
        || lower.contains("vaapi")
        || lower.contains("qsv")
        || lower.contains("nvenc")
        || lower.contains("amf")
        || lower.contains("hardware")
    {
        "hardware_unavailable"
    } else if lower.contains("first segment")
        || lower.contains("segment")
        || lower.contains("timed out")
        || lower.contains("timeout")
    {
        "segment_timeout"
    } else {
        "ffmpeg_startup_failed"
    }
}

fn playback_job_start_error(error: String) -> ApiError {
    let code = playback_job_start_error_code(&error);
    let status = if code == "segment_timeout" {
        StatusCode::GATEWAY_TIMEOUT
    } else {
        StatusCode::CONFLICT
    };
    ApiError::structured(
        status,
        code,
        format!("{code}: playback HLS job did not start"),
        Some(serde_json::json!({
            "reason": code,
            "detail": error,
            "retry": {
                "allowed": code != "hardware_unavailable",
                "after_seconds": 10,
                "strategy": if code == "hardware_unavailable" {
                    "change_hardware_policy_or_retry_software"
                } else {
                    "retry_same_request"
                }
            }
        })),
    )
}

async fn get_session_with_token(
    state: &AppState,
    user: &CurrentUser,
    session_id: &str,
    expected_mode: Option<&str>,
    token: &str,
) -> ApiResult<AnyRow> {
    let row = get_session(state, user, session_id, expected_mode, true).await?;
    let stored_token: Option<String> = row.try_get("token").ok();
    if let Some(stored) = stored_token {
        if stored != token {
            info!(session = session_id, "session token mismatch");
            return Err(ApiError::unauthorized("invalid session"));
        }
    }
    if playback_stream_token_expired(&row) {
        expire_active_playback_session_with_reason(
            state,
            session_id,
            "stream_token",
            "stream_token_expired",
        )
        .await?;
        return Err(ApiError::unauthorized("invalid session"));
    }
    Ok(row)
}

#[derive(Debug, Clone, Copy)]
enum StreamRoutePolicyKind {
    DirectFile,
    Hls,
}

fn enforce_stream_route_remote_policy(
    row: &AnyRow,
    route_kind: StreamRoutePolicyKind,
) -> ApiResult<()> {
    let Some(policy) = remote_policy_snapshot_from_session(row) else {
        return Ok(());
    };
    if !policy.applied {
        return Ok(());
    }

    let mode = row
        .try_get::<String, _>("mode")
        .unwrap_or_default()
        .to_ascii_lowercase();
    match route_kind {
        StreamRoutePolicyKind::DirectFile => {
            if !policy.allow_downloads || !policy.allow_direct_play || mode != "direct_play" {
                return Err(ApiError::unauthorized("invalid session"));
            }
        }
        StreamRoutePolicyKind::Hls => {
            if !policy.allow_transcode && session_mode_is_transcode(&mode) {
                return Err(ApiError::unauthorized("invalid session"));
            }
            if !policy.allow_hardware_transcode && session_plan_uses_hardware(row) {
                return Err(ApiError::unauthorized("invalid session"));
            }
        }
    }

    Ok(())
}

fn remote_policy_snapshot_from_session(row: &AnyRow) -> Option<RemotePlaybackPolicySnapshot> {
    row.try_get::<String, _>("remote_policy_json")
        .ok()
        .and_then(|raw| serde_json::from_str::<RemotePlaybackPolicySnapshot>(&raw).ok())
}

fn session_mode_is_transcode(mode: &str) -> bool {
    matches!(
        mode,
        "transcode"
            | "audio_transcode"
            | "subtitle_transcode"
            | "video_transcode"
            | "adaptive_transcode"
    )
}

fn session_plan_uses_hardware(row: &AnyRow) -> bool {
    row.try_get::<String, _>("playback_plan_json")
        .ok()
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
        .and_then(|value| {
            value
                .get("hardware_acceleration")
                .and_then(|hardware| hardware.get("enabled"))
                .and_then(Value::as_bool)
        })
        .unwrap_or(false)
}

fn rewrite_playlist_with_token(
    content: &str,
    session_token: &str,
    auth_token: Option<&str>,
    cache_bust: Option<&str>,
) -> String {
    content
        .lines()
        .map(|line| {
            if line.starts_with('#') {
                return rewrite_hls_uri_attributes(line, session_token, auth_token, cache_bust);
            }
            if line.trim().is_empty() {
                return line.to_string();
            }

            append_playback_query_params(line, session_token, auth_token, cache_bust)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn adaptive_ladder_plan_from_plan_value(
    playback_plan: Option<&Value>,
) -> Option<AdaptiveLadderPlan> {
    playback_plan
        .and_then(|plan| plan.get("adaptive_ladder"))
        .cloned()
        .and_then(|ladder| serde_json::from_value::<AdaptiveLadderPlan>(ladder).ok())
}

fn normalize_adaptive_master_playlist_metadata(
    content: &str,
    ladder: &AdaptiveLadderPlan,
) -> String {
    let mut rung_index = 0_usize;
    content
        .lines()
        .map(|line| {
            let Some(attrs) = line.strip_prefix("#EXT-X-STREAM-INF:") else {
                return line.to_string();
            };
            let Some(rung) = ladder.rungs.get(rung_index) else {
                return line.to_string();
            };
            rung_index += 1;
            format!(
                "#EXT-X-STREAM-INF:{}",
                normalized_stream_inf_attributes(attrs, rung)
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn validate_adaptive_master_playlist_metadata(
    content: &str,
    ladder: &AdaptiveLadderPlan,
) -> Result<(), String> {
    let stream_lines = content
        .lines()
        .filter_map(|line| line.strip_prefix("#EXT-X-STREAM-INF:"))
        .collect::<Vec<_>>();
    if stream_lines.len() != ladder.rungs.len() {
        return Err(format!(
            "variant_count_mismatch:{}:{}",
            stream_lines.len(),
            ladder.rungs.len()
        ));
    }
    for (attrs, rung) in stream_lines.into_iter().zip(&ladder.rungs) {
        let parsed = parse_hls_attributes(attrs);
        require_hls_attr(&parsed, "BANDWIDTH", &rung.bandwidth_bps.to_string())?;
        require_hls_attr(
            &parsed,
            "AVERAGE-BANDWIDTH",
            &rung.average_bandwidth_bps.to_string(),
        )?;
        require_hls_attr(&parsed, "RESOLUTION", &rung.resolution)?;
        require_hls_attr(&parsed, "CODECS", &format!("\"{}\"", rung.codecs))?;
        if let Some(frame_rate) = rung.frame_rate.as_deref() {
            require_hls_attr(&parsed, "FRAME-RATE", frame_rate)?;
        }
    }
    Ok(())
}

fn normalized_stream_inf_attributes(attrs: &str, rung: &AdaptiveRungPlan) -> String {
    let mut parsed = parse_hls_attributes(attrs);
    upsert_hls_attr(&mut parsed, "BANDWIDTH", rung.bandwidth_bps.to_string());
    upsert_hls_attr(
        &mut parsed,
        "AVERAGE-BANDWIDTH",
        rung.average_bandwidth_bps.to_string(),
    );
    upsert_hls_attr(&mut parsed, "RESOLUTION", rung.resolution.clone());
    upsert_hls_attr(&mut parsed, "CODECS", format!("\"{}\"", rung.codecs));
    if let Some(frame_rate) = rung.frame_rate.as_deref() {
        upsert_hls_attr(&mut parsed, "FRAME-RATE", frame_rate.to_string());
    }
    parsed
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn parse_hls_attributes(attrs: &str) -> Vec<(String, String)> {
    split_hls_attribute_list(attrs)
        .into_iter()
        .filter_map(|part| {
            let (key, value) = part.split_once('=')?;
            let key = key.trim();
            (!key.is_empty()).then(|| (key.to_ascii_uppercase(), value.trim().to_string()))
        })
        .collect()
}

fn split_hls_attribute_list(attrs: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    for ch in attrs.chars() {
        match ch {
            '"' => {
                quoted = !quoted;
                current.push(ch);
            }
            ',' if !quoted => {
                if !current.trim().is_empty() {
                    parts.push(current.trim().to_string());
                }
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    if !current.trim().is_empty() {
        parts.push(current.trim().to_string());
    }
    parts
}

fn upsert_hls_attr(attrs: &mut Vec<(String, String)>, key: &str, value: String) {
    if let Some((_, existing)) = attrs
        .iter_mut()
        .find(|(existing, _)| existing.eq_ignore_ascii_case(key))
    {
        *existing = value;
    } else {
        attrs.push((key.to_string(), value));
    }
}

fn require_hls_attr(attrs: &[(String, String)], key: &str, expected: &str) -> Result<(), String> {
    match attrs
        .iter()
        .find(|(existing, _)| existing.eq_ignore_ascii_case(key))
        .map(|(_, value)| value.as_str())
    {
        Some(actual) if actual == expected => Ok(()),
        Some(actual) => Err(format!("{key}_mismatch:{actual}:{expected}")),
        None => Err(format!("{key}_missing")),
    }
}

fn rewrite_hls_uri_attributes(
    line: &str,
    session_token: &str,
    auth_token: Option<&str>,
    cache_bust: Option<&str>,
) -> String {
    let mut rest = line;
    let mut rewritten = String::with_capacity(line.len());

    while let Some(index) = rest.find("URI=\"") {
        let after_marker = index + "URI=\"".len();
        rewritten.push_str(&rest[..after_marker]);
        let value_and_tail = &rest[after_marker..];
        let Some(end_quote) = value_and_tail.find('"') else {
            rewritten.push_str(value_and_tail);
            return rewritten;
        };
        let uri = &value_and_tail[..end_quote];
        if should_rewrite_playlist_uri(uri) {
            rewritten.push_str(&append_playback_query_params(
                uri,
                session_token,
                auth_token,
                cache_bust,
            ));
        } else {
            rewritten.push_str(uri);
        }
        rewritten.push('"');
        rest = &value_and_tail[end_quote + 1..];
    }

    rewritten.push_str(rest);
    rewritten
}

fn should_rewrite_playlist_uri(uri: &str) -> bool {
    let trimmed = uri.trim();
    !trimmed.is_empty()
        && !trimmed.starts_with("data:")
        && !trimmed.starts_with("skd:")
        && !trimmed.contains("://")
}

#[derive(Debug, Clone)]
struct SubtitleRendition {
    name: String,
    language: Option<String>,
    is_default: bool,
    is_forced: bool,
    is_hearing_impaired: bool,
    uri: String,
}

async fn build_subtitle_renditions(
    subtitles: &[SubtitleInfo],
    temp_dir: &Path,
) -> Vec<SubtitleRendition> {
    let default_index = subtitles.iter().position(|s| s.is_default).unwrap_or(0);
    let mut renditions = Vec::new();

    for (idx, sub) in subtitles.iter().enumerate() {
        let playlist_name = subtitle_playlist_name(idx);
        let path = temp_dir.join(&playlist_name);
        ensure_subtitle_playlist(&path).await;

        let name = subtitle_display_name(sub, idx);
        renditions.push(SubtitleRendition {
            name,
            language: sub.language.clone(),
            is_default: idx == default_index,
            is_forced: sub.is_forced,
            is_hearing_impaired: sub.is_hearing_impaired,
            uri: playlist_name,
        });
    }

    renditions
}

async fn ensure_subtitle_playlist(path: &Path) {
    if fs::metadata(path).await.is_ok() {
        return;
    }
    let placeholder = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        "#EXT-X-TARGETDURATION:4",
        "#EXT-X-MEDIA-SEQUENCE:0",
        "#EXT-X-PLAYLIST-TYPE:EVENT",
        "",
    ]
    .join("\n");
    let _ = fs::write(path, placeholder).await;
}

async fn wait_for_subtitle_segments(
    temp_dir: &Path,
    count: usize,
    retries: usize,
    delay_ms: u64,
) -> bool {
    for _ in 0..retries {
        if subtitles_ready(temp_dir, count).await {
            return true;
        }
        sleep(Duration::from_millis(delay_ms)).await;
    }
    false
}

async fn subtitles_ready(temp_dir: &Path, count: usize) -> bool {
    for idx in 0..count {
        let path = temp_dir.join(subtitle_playlist_name(idx));
        if !subtitle_playlist_has_segment(&path).await {
            return false;
        }
    }
    true
}

async fn subtitle_playlist_has_segment(path: &Path) -> bool {
    let data = match fs::read_to_string(path).await {
        Ok(data) => data,
        Err(_) => return false,
    };
    data.lines().any(|line| line.starts_with("#EXTINF"))
}

fn subtitle_playlist_name(index: usize) -> String {
    format!("sub_{index}.m3u8")
}

fn subtitle_display_name(info: &SubtitleInfo, index: usize) -> String {
    if let Some(title) = info.title.as_ref().filter(|t| !t.trim().is_empty()) {
        return title.to_string();
    }
    if let Some(language) = info
        .language
        .as_ref()
        .filter(|lang| !lang.trim().is_empty())
    {
        return language.to_string();
    }
    format!("Subtitle {}", index + 1)
}

fn escape_attribute(value: &str) -> String {
    value
        .replace('"', "'")
        .replace('\n', " ")
        .replace('\r', " ")
}

fn inject_subtitle_media(
    content: &str,
    renditions: &[SubtitleRendition],
    session_token: &str,
    auth_token: Option<&str>,
    cache_bust: Option<&str>,
) -> String {
    if renditions.is_empty() || content.contains("EXT-X-MEDIA:TYPE=SUBTITLES") {
        return content.to_string();
    }

    let mut media_lines = Vec::new();
    for rendition in renditions {
        let subtitle_url =
            append_playback_query_params(&rendition.uri, session_token, auth_token, cache_bust);

        let mut attrs = vec![
            "TYPE=SUBTITLES".to_string(),
            "GROUP-ID=\"subs\"".to_string(),
            format!("NAME=\"{}\"", escape_attribute(&rendition.name)),
            format!(
                "DEFAULT={}",
                if rendition.is_default { "YES" } else { "NO" }
            ),
            "AUTOSELECT=YES".to_string(),
        ];
        if let Some(lang) = rendition
            .language
            .as_ref()
            .filter(|lang| !lang.trim().is_empty())
        {
            attrs.push(format!("LANGUAGE=\"{}\"", escape_attribute(lang)));
        }
        if rendition.is_forced {
            attrs.push("FORCED=YES".to_string());
        }
        if rendition.is_hearing_impaired {
            attrs.push(
                "CHARACTERISTICS=\"public.accessibility.describes-spoken-dialog,public.accessibility.describes-music-and-sound\""
                    .to_string(),
            );
        }
        attrs.push(format!("URI=\"{}\"", subtitle_url));
        media_lines.push(format!("#EXT-X-MEDIA:{}", attrs.join(",")));
    }

    if media_lines.is_empty() {
        return content.to_string();
    }

    let mut lines = Vec::new();
    let mut inserted = false;
    for line in content.lines() {
        if !inserted && line.starts_with("#EXT-X-STREAM-INF") {
            lines.extend(media_lines.clone());
            inserted = true;
        }
        if line.starts_with("#EXT-X-STREAM-INF") && !line.contains("SUBTITLES=") {
            lines.push(format!("{line},SUBTITLES=\"subs\""));
        } else {
            lines.push(line.to_string());
        }
    }

    if !inserted {
        lines.extend(media_lines);
    }

    lines.join("\n")
}

fn append_query_param(url: &str, key: &str, value: &str) -> String {
    if value.trim().is_empty() || url.contains(&format!("{key}=")) {
        return url.to_string();
    }
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}{key}={value}")
}

fn append_playback_query_params(
    url: &str,
    session_token: &str,
    auth_token: Option<&str>,
    cache_bust: Option<&str>,
) -> String {
    let sanitized = strip_sensitive_playback_query_params(url);
    let mut rewritten = append_query_param(&sanitized, "session", session_token);
    if let Some(tok) = auth_token {
        rewritten = append_query_param(&rewritten, "token", tok);
    }
    if let Some(ts) = cache_bust {
        rewritten = append_query_param(&rewritten, "ts", ts);
    }
    rewritten
}

fn strip_sensitive_playback_query_params(url: &str) -> String {
    let (without_fragment, fragment) = url
        .split_once('#')
        .map(|(base, fragment)| (base, Some(fragment)))
        .unwrap_or((url, None));
    let Some((path, query)) = without_fragment.split_once('?') else {
        return url.to_string();
    };
    let retained = query
        .split('&')
        .filter(|pair| !pair.trim().is_empty())
        .filter(|pair| {
            let key = pair.split_once('=').map(|(key, _)| key).unwrap_or(pair);
            !is_sensitive_playback_query_key(key)
        })
        .collect::<Vec<_>>();
    let mut rewritten = if retained.is_empty() {
        path.to_string()
    } else {
        format!("{}?{}", path, retained.join("&"))
    };
    if let Some(fragment) = fragment {
        rewritten.push('#');
        rewritten.push_str(fragment);
    }
    rewritten
}

pub(crate) fn redact_playback_url_for_log(raw: &str) -> String {
    let Some((before_query, after_query)) = raw.split_once('?') else {
        return raw.to_string();
    };
    let (query, fragment) = after_query
        .split_once('#')
        .map(|(query, fragment)| (query, Some(fragment)))
        .unwrap_or((after_query, None));
    let redacted_query = query
        .split('&')
        .map(|pair| {
            let key = pair.split_once('=').map(|(key, _)| key).unwrap_or(pair);
            if is_sensitive_playback_query_key(key) {
                format!("{key}=[redacted]")
            } else {
                pair.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("&");
    match fragment {
        Some(fragment) => format!("{before_query}?{redacted_query}#{fragment}"),
        None => format!("{before_query}?{redacted_query}"),
    }
}

fn is_sensitive_playback_query_key(key: &str) -> bool {
    let decoded = urlencoding::decode(key)
        .map(|value| value.into_owned())
        .unwrap_or_else(|_| key.to_string());
    matches!(
        decoded.to_ascii_lowercase().as_str(),
        "session" | "sid" | "token" | "access_token" | "x-plex-token"
    )
}

#[derive(Debug, Deserialize)]
struct SegmentProbe {
    streams: Vec<SegmentStream>,
}

#[derive(Debug)]
struct SegmentInfo {
    path: PathBuf,
    index: i64,
    name: String,
}

#[derive(Debug, Deserialize)]
struct SegmentStream {
    start_time: Option<String>,
}

async fn resolve_subtitle_delay(state: &AppState, session_id: Uuid) -> Option<f64> {
    if let Some(delay) = state.transcodes.subtitle_delay(session_id).await {
        return Some(delay);
    }
    let temp_dir = state.transcodes.temp_dir(session_id).await?;
    for _ in 0..20 {
        if let Some(segment) = find_first_segment(&temp_dir).await {
            if let Some(start_time) = probe_segment_start_time(&segment.path).await {
                let offset = start_time - (segment.index as f64 * HLS_SEGMENT_SECONDS);
                state
                    .transcodes
                    .set_subtitle_delay(session_id, offset)
                    .await;
                info!(
                    session = %session_id,
                    segment = %segment.name,
                    segment_index = segment.index,
                    segment_start = start_time,
                    subtitle_delay = offset,
                    "resolved subtitle delay"
                );
                return Some(offset);
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    None
}

async fn find_first_segment(temp_dir: &Path) -> Option<SegmentInfo> {
    let mut entries = fs::read_dir(temp_dir).await.ok()?;
    let mut candidates: Vec<SegmentInfo> = Vec::new();
    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with("seg_0_") && name.ends_with(".ts") {
            if let Some(index) = parse_segment_index(&name) {
                candidates.push(SegmentInfo {
                    path: temp_dir.join(&name),
                    index,
                    name,
                });
            }
        }
    }
    candidates.sort_by(|a, b| a.index.cmp(&b.index));
    candidates.into_iter().next()
}

async fn probe_segment_start_time(path: &Path) -> Option<f64> {
    let output = Command::new("ffprobe")
        .arg("-v")
        .arg("quiet")
        .arg("-print_format")
        .arg("json")
        .arg("-show_streams")
        .arg("-select_streams")
        .arg("v:0")
        .arg(path)
        .output()
        .await
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let parsed: SegmentProbe = serde_json::from_slice(&output.stdout).ok()?;
    let start = parsed
        .streams
        .iter()
        .find_map(|stream| stream.start_time.as_ref())?;
    start.parse::<f64>().ok()
}

fn parse_segment_index(name: &str) -> Option<i64> {
    let base = name.strip_suffix(".ts")?;
    let (_, index) = base.rsplit_once('_')?;
    index.parse::<i64>().ok()
}

enum ShiftedLine {
    Replace(String),
    DropCue,
}

fn shift_vtt_cues(content: &str, offset_seconds: f64) -> String {
    if offset_seconds.abs() < 0.001 {
        return content.to_string();
    }
    let mut out: Vec<String> = Vec::new();
    let mut drop_cue = false;
    for line in content.lines() {
        if drop_cue {
            if line.trim().is_empty() {
                drop_cue = false;
                if !matches!(out.last(), Some(last) if last.is_empty()) {
                    out.push(String::new());
                }
            }
            continue;
        }
        if let Some(shifted) = shift_vtt_line(line, offset_seconds) {
            match shifted {
                ShiftedLine::Replace(line) => out.push(line),
                ShiftedLine::DropCue => {
                    drop_cue = true;
                }
            }
        } else {
            out.push(line.to_string());
        }
    }
    out.join("\n")
}

fn shift_vtt_line(line: &str, offset_seconds: f64) -> Option<ShiftedLine> {
    if !line.contains("-->") {
        return None;
    }
    let mut parts = line.splitn(2, "-->");
    let left = parts.next()?.trim();
    let right = parts.next()?.trim();
    if left.is_empty() || right.is_empty() {
        return None;
    }
    let (right_time, right_settings) = split_time_and_settings(right);
    let start = parse_vtt_time(left)?;
    let end = parse_vtt_time(right_time)?;
    let shifted_start = start + offset_seconds;
    let shifted_end = end + offset_seconds;
    if shifted_end <= 0.0 {
        return Some(ShiftedLine::DropCue);
    }
    let shifted_start = format_vtt_time(shifted_start.max(0.0));
    let shifted_end = format_vtt_time(shifted_end.max(0.0));
    let mut out = format!("{shifted_start} --> {shifted_end}");
    if let Some(settings) = right_settings {
        let trimmed = settings.trim();
        if !trimmed.is_empty() {
            out.push(' ');
            out.push_str(trimmed);
        }
    }
    Some(ShiftedLine::Replace(out))
}

fn split_time_and_settings(raw: &str) -> (&str, Option<&str>) {
    let mut iter = raw.splitn(2, |c: char| c.is_whitespace());
    let time = iter.next().unwrap_or("");
    let settings = iter.next();
    (time, settings)
}

fn parse_vtt_time(raw: &str) -> Option<f64> {
    let cleaned = raw.trim();
    if cleaned.is_empty() {
        return None;
    }
    let parts: Vec<&str> = cleaned.split(':').collect();
    let (hours, minutes, seconds_raw) = match parts.len() {
        3 => (
            parts[0].parse::<f64>().ok()?,
            parts[1].parse::<f64>().ok()?,
            parts[2],
        ),
        2 => (0.0, parts[0].parse::<f64>().ok()?, parts[1]),
        _ => return None,
    };
    let (sec_str, frac_str) = if let Some((sec, frac)) = seconds_raw.split_once('.') {
        (sec, frac)
    } else if let Some((sec, frac)) = seconds_raw.split_once(',') {
        (sec, frac)
    } else {
        (seconds_raw, "")
    };
    let secs = sec_str.parse::<f64>().ok()?;
    let frac = if frac_str.is_empty() {
        0.0
    } else {
        let scale = 10_f64.powi(frac_str.len() as i32);
        frac_str.parse::<f64>().ok()? / scale
    };
    Some(hours * 3600.0 + minutes * 60.0 + secs + frac)
}

fn format_vtt_time(seconds: f64) -> String {
    let mut total_ms = (seconds * 1000.0).round() as i64;
    if total_ms < 0 {
        total_ms = 0;
    }
    let ms = (total_ms % 1000) as i64;
    let total_seconds = total_ms / 1000;
    let secs = (total_seconds % 60) as i64;
    let total_minutes = total_seconds / 60;
    let mins = (total_minutes % 60) as i64;
    let hours = total_minutes / 60;
    format!("{:02}:{:02}:{:02}.{:03}", hours, mins, secs, ms)
}

async fn mark_session_error(
    state: AppState,
    session_id: &str,
    message: Option<String>,
    log_path: Option<String>,
) {
    let transcode_state = message.as_deref().map(|m| {
        serde_json::json!({
            "state": "failed",
            "error": m,
            "error_code": "playlist_read_failed",
            "error_kind": "missing_segment",
            "log_path": log_path,
        })
        .to_string()
    });
    let _ = sqlx::query::<sqlx::Any>(
        "UPDATE playback_sessions
         SET state = 'error',
             updated_at = CURRENT_TIMESTAMP,
             transcode_state = COALESCE(?, transcode_state),
             job_state_json = COALESCE(?, job_state_json)
         WHERE id = ?",
    )
    .bind(transcode_state.clone())
    .bind(transcode_state)
    .bind(session_id)
    .execute(&state.db_pool)
    .await;
}

pub async fn session_detail(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    user: CurrentUser,
) -> ApiResult<Json<SessionDetailResponse>> {
    let session = get_session(&state, &user, &id, None, false).await?;
    touch_playback_session(&state, &id, PlaybackActivityKind::SessionDetail).await?;
    let transcode_state: Option<serde_json::Value> = session
        .try_get::<String, _>("transcode_state")
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok());
    let playback_plan: Option<serde_json::Value> = session
        .try_get::<String, _>("playback_plan_json")
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok());
    let job_state: Option<serde_json::Value> = session
        .try_get::<String, _>("job_state_json")
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok());
    let error = transcode_state
        .as_ref()
        .and_then(|v| v.get("error").and_then(Value::as_str))
        .map(redact_playback_diagnostics_text);
    let adaptive_ladder = adaptive_ladder_from_plan(playback_plan.as_ref());
    let starting_rung = starting_rung_from_plan(playback_plan.as_ref());
    let active_rung = active_rung_from_state(playback_plan.as_ref(), job_state.as_ref());
    let plan_summary = plan_summary_from_plan(playback_plan.as_ref(), active_rung.as_ref());
    let job_snapshot = job_snapshot_from_state(job_state.as_ref());
    let mode: String = session.get("mode");
    let delivery = delivery_from_diagnostics(playback_plan.as_ref(), job_state.as_ref(), &mode);
    let server_seek_required = server_seek_required_from_plan(playback_plan.as_ref());
    let decision_reason = decision_reason_from_plan(playback_plan.as_ref());
    let decision_reasons = decision_reasons_from_plan(playback_plan.as_ref());
    let ffmpeg_log_tail = ffmpeg_log_tail_from_state(job_state.as_ref(), transcode_state.as_ref());
    let remote_access = playback_plan
        .as_ref()
        .and_then(|plan| plan.get("remote_access").cloned())
        .and_then(|value| serde_json::from_value::<RemoteAccessContract>(value).ok());
    let remote_policy = remote_policy_snapshot_from_session(&session);

    let logical_position_seconds = session
        .try_get::<f64, _>("logical_position_seconds")
        .ok()
        .map(|v| v as f32)
        .unwrap_or(0.0);

    let wan_direct_endpoint: Option<String> = sqlx::query_scalar(
        "SELECT wan_direct_endpoint FROM server_registry ORDER BY last_seen_at DESC LIMIT 1",
    )
    .fetch_optional(&state.db_pool)
    .await
    .ok()
    .flatten();

    let response = SessionDetailResponse {
        id: id.clone(),
        media_file_id: session.get("media_file_id"),
        server_id: session.try_get("server_id").ok(),
        mode,
        delivery,
        state: session.get("state"),
        network_type: session.try_get("network_type").ok(),
        logical_position_seconds,
        duration_seconds: session
            .try_get::<i64, _>("duration_seconds")
            .ok()
            .map(|v| v as i32),
        server_seek_required,
        decision_reason,
        decision_reasons,
        wan_direct_endpoint,
        stream_token_expires_at: session.try_get("token_expires_at").ok(),
        remote_access,
        remote_policy,
        playback_plan,
        plan_summary,
        job_snapshot,
        adaptive_ladder,
        starting_rung,
        active_rung,
        ffmpeg_log_tail,
        error,
        updated_at: session.try_get("updated_at").ok(),
    };

    Ok(Json(response))
}

pub async fn resume_session(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    user: CurrentUser,
) -> ApiResult<Json<SessionDetailResponse>> {
    session_detail(State(state), AxumPath(id), user).await
}

#[derive(Debug, Serialize)]
pub struct SessionPollResponse {
    pub id: String,
    pub state: String,
    pub mode: String,
    pub delivery: String,
    pub logical_position_seconds: f32,
    pub duration_seconds: Option<i32>,
    pub server_seek_required: bool,
    pub decision_reason: Option<String>,
    pub decision_reasons: Vec<String>,
    pub playback_plan: Option<serde_json::Value>,
    pub plan_summary: Option<serde_json::Value>,
    pub job_snapshot: Option<serde_json::Value>,
    pub adaptive_ladder: Option<serde_json::Value>,
    pub starting_rung: Option<serde_json::Value>,
    pub active_rung: Option<serde_json::Value>,
    pub ffmpeg_log_tail: Option<String>,
    pub error: Option<String>,
}

pub async fn poll_session(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    user: CurrentUser,
) -> ApiResult<Json<SessionPollResponse>> {
    let session = get_session(&state, &user, &id, None, false).await?;
    touch_playback_session(&state, &id, PlaybackActivityKind::SessionPoll).await?;
    let transcode_state: Option<serde_json::Value> = session
        .try_get::<String, _>("transcode_state")
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok());
    let playback_plan: Option<serde_json::Value> = session
        .try_get::<String, _>("playback_plan_json")
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok());
    let job_state: Option<serde_json::Value> = session
        .try_get::<String, _>("job_state_json")
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok());
    let error = transcode_state
        .as_ref()
        .and_then(|v| v.get("error").and_then(Value::as_str))
        .map(redact_playback_diagnostics_text);
    let adaptive_ladder = adaptive_ladder_from_plan(playback_plan.as_ref());
    let starting_rung = starting_rung_from_plan(playback_plan.as_ref());
    let active_rung = active_rung_from_state(playback_plan.as_ref(), job_state.as_ref());
    let plan_summary = plan_summary_from_plan(playback_plan.as_ref(), active_rung.as_ref());
    let job_snapshot = job_snapshot_from_state(job_state.as_ref());
    let mode: String = session.get("mode");
    let delivery = delivery_from_diagnostics(playback_plan.as_ref(), job_state.as_ref(), &mode);
    let server_seek_required = server_seek_required_from_plan(playback_plan.as_ref());
    let decision_reason = decision_reason_from_plan(playback_plan.as_ref());
    let decision_reasons = decision_reasons_from_plan(playback_plan.as_ref());
    let ffmpeg_log_tail = ffmpeg_log_tail_from_state(job_state.as_ref(), transcode_state.as_ref());
    let logical_position_seconds = session
        .try_get::<f64, _>("logical_position_seconds")
        .ok()
        .map(|v| v as f32)
        .unwrap_or(0.0);

    let response = SessionPollResponse {
        id: id.clone(),
        state: session.get("state"),
        mode,
        delivery,
        logical_position_seconds,
        duration_seconds: session
            .try_get::<i64, _>("duration_seconds")
            .ok()
            .map(|v| v as i32),
        server_seek_required,
        decision_reason,
        decision_reasons,
        playback_plan,
        plan_summary,
        job_snapshot,
        adaptive_ladder,
        starting_rung,
        active_rung,
        ffmpeg_log_tail,
        error,
    };

    Ok(Json(response))
}

pub async fn heartbeat_session(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    user: CurrentUser,
) -> ApiResult<Json<Value>> {
    let _ = get_session(&state, &user, &id, None, true).await?;
    touch_playback_session(&state, &id, PlaybackActivityKind::Heartbeat).await?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

pub async fn end_session(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    user: CurrentUser,
) -> ApiResult<Json<Value>> {
    let session_id =
        Uuid::parse_str(&id).map_err(|_| ApiError::bad_request("invalid session id"))?;
    // Validate ownership.
    let _ = get_session(&state, &user, &id, None, false).await?;

    state.transcodes.stop_and_remove(session_id).await;
    sqlx::query::<sqlx::Any>(
        "UPDATE playback_sessions SET state = 'ended', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
    )
    .bind(&id)
    .execute(&state.db_pool)
    .await
    .map_err(|e| ApiError::internal(e.to_string()))?;

    info!(session = %id, "session ended and cleaned up");
    Ok(Json(serde_json::json!({ "ok": true })))
}

pub async fn seek_transcode(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    user: CurrentUser,
    Json(body): Json<SeekRequest>,
) -> ApiResult<Json<Value>> {
    let session_id = Uuid::parse_str(&id).map_err(|_| ApiError::bad_request("invalid session"))?;
    let session_row =
        get_session(&state, &user, &session_id.to_string(), Some("hls"), true).await?;
    touch_playback_session(&state, &id, PlaybackActivityKind::Seek).await?;
    let media_file_id: String = session_row.get("media_file_id");

    let media_path: Option<String> = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT path FROM media_files WHERE id = ? LIMIT 1",
    )
    .bind(&media_file_id)
    .fetch_optional(&state.db_pool)
    .await
    .map_err(|e| ApiError::internal(e.to_string()))?;

    let media_path = media_path.ok_or_else(|| ApiError::not_found("file not found"))?;

    let playback_plan_json = session_row
        .try_get::<String, _>("playback_plan_json")
        .ok()
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok());
    let transcode_params = transcode_params_from_session(&session_row, body.position_seconds);
    let playback_plan = PlaybackJobPlan::new(
        session_id,
        media_file_id.clone(),
        media_path.clone(),
        transcode_params,
        playback_plan_json.clone(),
    );
    let _handle = if state.transcodes.snapshot(session_id).await.is_some() {
        state
            .transcodes
            .restart_at(session_id, body.position_seconds)
            .await
    } else {
        state
            .transcodes
            .start(playback_plan, body.position_seconds)
            .await
    }
    .map_err(|e| {
        TRANSCODE_ERRORS
            .with_label_values(&["restart_failed"])
            .inc();
        ApiError::internal(e.to_string())
    })?;
    let hardware_label = hardware_metric_label_from_plan_value(playback_plan_json.as_ref());
    TRANSCODE_STARTS
        .with_label_values(&["restart", "unknown", "unknown", hardware_label.as_str()])
        .inc();

    sqlx::query::<sqlx::Any>("UPDATE playback_sessions SET logical_position_seconds = ?, state = 'active', updated_at = CURRENT_TIMESTAMP WHERE id = ?")
        .bind(body.position_seconds)
        .bind(&id)
        .execute(&state.db_pool)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;

    Ok(Json(serde_json::json!({ "ok": true })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::playback::plan::{
        AdaptiveAudioStrategy, CompatibilityReport, HdrAction, PLAYBACK_PLAN_VERSION,
        PlaybackFeasibilityAction, PlaybackFeasibilityDecision, PlaybackPerformanceConfidence,
        PlaybackPerformanceDecision, PlaybackSupportDecision, SeekBehavior, StreamAction,
        VideoFrameRateMode, VideoFrameRatePlan, VideoOutputPlan,
    };

    fn phase13_test_plan(mode: PlaybackMode) -> PlaybackPlan {
        let delivery = match mode {
            PlaybackMode::DirectPlay => Delivery::DirectFile,
            PlaybackMode::AdaptiveTranscode => Delivery::HlsAdaptiveFmp4,
            _ => Delivery::HlsFmp4,
        };
        let seek_behavior = if mode == PlaybackMode::DirectPlay {
            SeekBehavior::ClientRange
        } else {
            SeekBehavior::ServerHlsRestart
        };
        PlaybackPlan {
            plan_version: PLAYBACK_PLAN_VERSION,
            mode,
            delivery,
            media_file_id: "phase13-capacity-media-file".to_string(),
            selected_video_track: Some(0),
            video_action: StreamAction::Copy,
            audio_action: StreamAction::Copy,
            subtitle_action: StreamAction::Disabled,
            seek_behavior,
            adaptive: mode == PlaybackMode::AdaptiveTranscode,
            selected_audio_track: Some(1),
            selected_subtitle_track: None,
            hdr_action: HdrAction::None,
            hardware_acceleration: HardwareAccelerationPlan::default(),
            workload_class: None,
            feasibility: None,
            audio_output: None,
            video_output: None,
            adaptive_ladder: None,
            video_transcode_reason: None,
            compatibility_report: CompatibilityReport::empty("phase13-capacity-media-file"),
            reasons: vec!["phase13_capacity_matrix".to_string()],
            warnings: Vec::new(),
            expected_outputs: Vec::new(),
            playable: true,
        }
    }

    fn phase13_capacity_snapshot() -> PlaybackCapacitySnapshot {
        PlaybackCapacitySnapshot {
            active_sessions: 0,
            active_user_sessions: 0,
            active_direct_streams: 0,
            active_hls_jobs: 0,
            active_video_transcode_weight: 0,
            active_hardware_transcodes: 0,
            startup_queue_len: 0,
            temp_dir_bytes: 0,
            ffmpeg_log_bytes: 0,
        }
    }

    fn assert_capacity_resource(
        config: &crate::config::PlaybackConfig,
        plan: &PlaybackPlan,
        snapshot: &PlaybackCapacitySnapshot,
        resource: &'static str,
    ) {
        let violation = playback_capacity_violation(config, plan, snapshot)
            .unwrap_or_else(|| panic!("expected {resource} capacity violation"));
        assert_eq!(violation.resource, resource);
    }

    #[test]
    fn phase20_structured_rejection_uses_feasibility_error_code_and_details() {
        let mut plan = phase13_test_plan(PlaybackMode::VideoTranscode);
        plan.playable = false;
        plan.reasons = vec!["transcode_performance_unknown_policy_denied".to_string()];
        plan.feasibility = Some(PlaybackFeasibilityDecision {
            action: PlaybackFeasibilityAction::Reject,
            reason: "transcode_performance_unknown_policy_denied".to_string(),
            support_decision: PlaybackSupportDecision::Supported,
            performance_decision: PlaybackPerformanceDecision::Unknown,
            confidence: PlaybackPerformanceConfidence::Unknown,
            selected_envelope_id: None,
            selected_hardware_api: None,
            selected_envelope_p50_realtime_factor_millis: None,
            selected_envelope_p95_realtime_factor_millis: None,
            selected_envelope_startup_latency_ms: None,
            selected_envelope_first_segment_latency_ms: None,
            selected_envelope_failure_count: None,
            selected_envelope_sample_count: None,
            realtime_required_millis: 1000,
            reasons: vec!["transcode_performance_unknown_policy_denied".to_string()],
            warnings: Vec::new(),
            remediation_codes: vec!["try_original_quality_or_lower_quality".to_string()],
            background_probe_queued: false,
        });

        assert_eq!(
            playback_error_code_for_plan(&plan),
            "transcode_performance_unknown_policy_denied"
        );
        let details = playback_not_playable_details(&plan);
        assert_eq!(
            details.get("reason").and_then(Value::as_str),
            Some("transcode_performance_unknown_policy_denied")
        );
        assert_eq!(
            details
                .get("feasibility")
                .and_then(|value| value.get("reason"))
                .and_then(Value::as_str),
            Some("transcode_performance_unknown_policy_denied")
        );
        assert_eq!(
            details
                .get("plan_summary")
                .and_then(|value| value.get("feasibility"))
                .and_then(|value| value.get("action"))
                .and_then(Value::as_str),
            Some("reject")
        );
        assert_eq!(
            details
                .get("remediation")
                .and_then(|value| value.get("suggested_actions"))
                .and_then(Value::as_array)
                .and_then(|actions| actions.first())
                .and_then(Value::as_str),
            Some("try_original_quality")
        );
        assert_eq!(
            details
                .get("plan_summary")
                .and_then(|value| value.get("feasibility_remediation"))
                .and_then(|value| value.get("admin_message"))
                .and_then(Value::as_str),
            Some(
                "Unknown workload performance is configured fail-closed. Seed a certification artifact, allow bounded local probes, or explicitly permit best-effort playback."
            )
        );
    }

    #[test]
    fn phase20_structured_rejection_explains_unsupported_hardware_capability() {
        let mut plan = phase13_test_plan(PlaybackMode::VideoTranscode);
        plan.playable = false;
        plan.reasons = vec!["hardware_decode_unsupported".to_string()];
        plan.feasibility = Some(PlaybackFeasibilityDecision {
            action: PlaybackFeasibilityAction::Reject,
            reason: "hardware_decode_unsupported".to_string(),
            support_decision: PlaybackSupportDecision::Unsupported,
            performance_decision: PlaybackPerformanceDecision::Unknown,
            confidence: PlaybackPerformanceConfidence::Certified,
            selected_envelope_id: Some("env-unsupported".to_string()),
            selected_hardware_api: Some("nvenc".to_string()),
            selected_envelope_p50_realtime_factor_millis: None,
            selected_envelope_p95_realtime_factor_millis: None,
            selected_envelope_startup_latency_ms: None,
            selected_envelope_first_segment_latency_ms: None,
            selected_envelope_failure_count: Some(2),
            selected_envelope_sample_count: Some(2),
            realtime_required_millis: 1000,
            reasons: vec!["hardware_decode_unsupported".to_string()],
            warnings: Vec::new(),
            remediation_codes: vec!["update_driver_or_use_original_quality".to_string()],
            background_probe_queued: false,
        });

        assert_eq!(
            playback_error_code_for_plan(&plan),
            "hardware_decode_unsupported"
        );
        let details = playback_not_playable_details(&plan);
        assert_eq!(
            details
                .get("remediation")
                .and_then(|value| value.get("suggested_actions"))
                .and_then(Value::as_array)
                .and_then(|actions| actions.first())
                .and_then(Value::as_str),
            Some("update_gpu_driver")
        );
        assert_eq!(
            details
                .get("remediation")
                .and_then(|value| value.get("admin_message"))
                .and_then(Value::as_str),
            Some(
                "Hardware decode is unsupported for the selected source workload. Verify the GPU driver, FFmpeg hardware decoder, and codec/profile capability row."
            )
        );
    }

    #[test]
    fn phase20_structured_rejection_explains_unsupported_software_decode() {
        let mut plan = phase13_test_plan(PlaybackMode::VideoTranscode);
        plan.playable = false;
        plan.reasons = vec!["software_decode_unsupported".to_string()];
        plan.feasibility = Some(PlaybackFeasibilityDecision {
            action: PlaybackFeasibilityAction::Reject,
            reason: "software_decode_unsupported".to_string(),
            support_decision: PlaybackSupportDecision::Unsupported,
            performance_decision: PlaybackPerformanceDecision::Unknown,
            confidence: PlaybackPerformanceConfidence::StaticInferred,
            selected_envelope_id: None,
            selected_hardware_api: None,
            selected_envelope_p50_realtime_factor_millis: None,
            selected_envelope_p95_realtime_factor_millis: None,
            selected_envelope_startup_latency_ms: None,
            selected_envelope_first_segment_latency_ms: None,
            selected_envelope_failure_count: None,
            selected_envelope_sample_count: None,
            realtime_required_millis: 1000,
            reasons: vec!["software_decode_unsupported".to_string()],
            warnings: Vec::new(),
            remediation_codes: vec!["replace_or_remux_media".to_string()],
            background_probe_queued: false,
        });

        assert_eq!(
            playback_error_code_for_plan(&plan),
            "software_decode_unsupported"
        );
        let details = playback_not_playable_details(&plan);
        assert_eq!(
            details
                .get("remediation")
                .and_then(|value| value.get("user_message"))
                .and_then(Value::as_str),
            Some("This server cannot decode this video format.")
        );
        assert_eq!(
            details
                .get("remediation")
                .and_then(|value| value.get("suggested_actions"))
                .and_then(Value::as_array)
                .and_then(|actions| actions.get(2))
                .and_then(Value::as_str),
            Some("replace_or_remux_media")
        );
    }

    #[test]
    fn phase20_structured_rejection_explains_below_realtime_envelope() {
        let mut plan = phase13_test_plan(PlaybackMode::VideoTranscode);
        plan.playable = false;
        plan.reasons = vec!["server_cannot_realtime_tonemap_source".to_string()];
        plan.feasibility = Some(PlaybackFeasibilityDecision {
            action: PlaybackFeasibilityAction::Reject,
            reason: "server_cannot_realtime_tonemap_source".to_string(),
            support_decision: PlaybackSupportDecision::Supported,
            performance_decision: PlaybackPerformanceDecision::NotRealtime,
            confidence: PlaybackPerformanceConfidence::Certified,
            selected_envelope_id: Some("env-not-realtime".to_string()),
            selected_hardware_api: Some("nvenc".to_string()),
            selected_envelope_p50_realtime_factor_millis: Some(650),
            selected_envelope_p95_realtime_factor_millis: Some(500),
            selected_envelope_startup_latency_ms: Some(2100),
            selected_envelope_first_segment_latency_ms: Some(3200),
            selected_envelope_failure_count: Some(1),
            selected_envelope_sample_count: Some(6),
            realtime_required_millis: 1000,
            reasons: vec!["server_cannot_realtime_tonemap_source".to_string()],
            warnings: Vec::new(),
            remediation_codes: vec!["use_original_quality_or_lower_quality".to_string()],
            background_probe_queued: false,
        });

        assert_eq!(
            playback_error_code_for_plan(&plan),
            "server_cannot_realtime_tonemap_source"
        );
        let details = playback_not_playable_details(&plan);
        assert_eq!(
            details
                .get("remediation")
                .and_then(|value| value.get("suggested_actions"))
                .and_then(Value::as_array)
                .and_then(|actions| actions.first())
                .and_then(Value::as_str),
            Some("try_original_quality")
        );
        assert_eq!(
            details
                .get("remediation")
                .and_then(|value| value.get("user_message"))
                .and_then(Value::as_str),
            Some("This server cannot convert this HDR video to SDR in real time.")
        );
    }

    #[test]
    fn playback_log_redaction_removes_query_auth_values() {
        let redacted = redact_playback_url_for_log(
            "/stream/direct/file-id?sid=session-id&session=secret-token&token=bearer&access_token=access&quality=1080#frag",
        );

        assert_eq!(
            redacted,
            "/stream/direct/file-id?sid=[redacted]&session=[redacted]&token=[redacted]&access_token=[redacted]&quality=1080#frag"
        );
        assert!(!redacted.contains("secret-token"));
        assert!(!redacted.contains("bearer"));
        assert!(!redacted.contains("access&"));
    }

    #[test]
    fn phase12_safe_diagnostics_summarize_plan_and_redact_log_tail() {
        let playback_plan = serde_json::json!({
            "mode": "adaptive_transcode",
            "delivery": "hls_adaptive_fmp4",
            "media_file_id": "file-1",
            "seek_behavior": "server_hls_restart",
            "selected_video_track": 0,
            "selected_audio_track": 1,
            "selected_subtitle_track": 2,
            "video_action": "transcode",
            "audio_action": "transcode",
            "subtitle_action": "burn_in",
            "hdr_action": "tone_map_to_sdr",
            "adaptive": true,
            "video_transcode_reason": "subtitle_requires_burn_in",
            "video_output": {
                "tone_map": {
                    "algorithm": "hable",
                    "input_primaries": "bt2020",
                    "input_transfer": "smpte2084",
                    "input_matrix": "bt2020nc",
                    "output_primaries": "bt709",
                    "output_transfer": "bt709",
                    "output_matrix": "bt709"
                }
            },
            "hardware_acceleration": {"enabled": false},
            "warnings": ["hardware_fallback_to_software"],
            "reasons": ["adaptive_transcode_automatic_quality_requested", "subtitle_requires_burn_in"]
        });
        let active_rung = serde_json::json!({
            "id": "1",
            "label": "720p 4000k",
            "bandwidth_bps": 4_000_000
        });
        let job_state = serde_json::json!({
            "state": "failed",
            "mode": "adaptive_transcode",
            "delivery": "hls_adaptive_fmp4",
            "temp_dir": "/tmp/elixir/session-id",
            "log_path": "/tmp/elixir/session-id/ffmpeg.log",
            "process_id": 12345,
            "error": "startup_failed",
            "error_code": "startup_failed",
            "error_kind": "ffmpeg_exit",
            "log_tail": "GET /sessions/id/master.m3u8?session=secret&token=secret2\nAuthorization: Bearer abc",
            "active_rung": active_rung.clone()
        });

        let summary =
            plan_summary_from_plan(Some(&playback_plan), Some(&active_rung)).expect("summary");
        assert_eq!(
            summary.get("server_seek_required").and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            summary.get("decision_reason").and_then(Value::as_str),
            Some("adaptive_transcode_automatic_quality_requested")
        );
        assert_eq!(
            summary
                .pointer("/active_rung/label")
                .and_then(Value::as_str),
            Some("720p 4000k")
        );
        assert_eq!(
            summary.get("hdr_action").and_then(Value::as_str),
            Some("tone_map_to_sdr")
        );
        assert_eq!(
            summary
                .pointer("/tone_map/output_primaries")
                .and_then(Value::as_str),
            Some("bt709")
        );

        let snapshot = job_snapshot_from_state(Some(&job_state)).expect("snapshot");
        assert!(snapshot.get("temp_dir").is_none(), "{snapshot}");
        assert!(snapshot.get("log_path").is_none(), "{snapshot}");
        assert!(snapshot.get("process_id").is_none(), "{snapshot}");

        let tail = ffmpeg_log_tail_from_state(Some(&job_state), None).expect("tail");
        assert!(tail.contains("session=[redacted]"), "{tail}");
        assert!(tail.contains("token=[redacted]"), "{tail}");
        assert!(!tail.contains("secret"), "{tail}");
        assert!(!tail.contains("Bearer abc"), "{tail}");
        assert!(!tail.contains("abc"), "{tail}");
    }

    #[test]
    fn playlist_rewrite_scopes_fmp4_init_map_and_segments() {
        let rewritten = rewrite_playlist_with_token(
            "#EXTM3U\n#EXT-X-MAP:URI=\"init.mp4\"\nsegment_00000.m4s\n",
            "session-token",
            Some("bearer-token"),
            Some("123"),
        );

        assert!(rewritten.contains(
            "#EXT-X-MAP:URI=\"init.mp4?session=session-token&token=bearer-token&ts=123\""
        ));
        assert!(
            rewritten.contains("segment_00000.m4s?session=session-token&token=bearer-token&ts=123")
        );
    }

    #[test]
    fn phase21_adaptive_master_playlist_metadata_is_normalized_and_validated() {
        let ladder = AdaptiveLadderPlan {
            rungs: vec![
                phase21_adaptive_rung("0", 3_128_000, 1280, 720),
                phase21_adaptive_rung("1", 1_628_000, 854, 480),
            ],
            starting_rung_id: "0".to_string(),
            active_rung_id: "0".to_string(),
            audio_strategy: AdaptiveAudioStrategy::PerRung,
            reasons: vec!["phase21_hls_metadata_fixture".to_string()],
        };
        let raw = "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-STREAM-INF:BANDWIDTH=1,OLD=YES\nstream_0.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=2,CODECS=\"old\"\nstream_1.m3u8\n";

        let normalized = normalize_adaptive_master_playlist_metadata(raw, &ladder);

        validate_adaptive_master_playlist_metadata(&normalized, &ladder)
            .expect("normalized playlist should pass HLS metadata validation");
        assert!(normalized.contains("BANDWIDTH=3128000"));
        assert!(normalized.contains("AVERAGE-BANDWIDTH=2815200"));
        assert!(normalized.contains("RESOLUTION=1280x720"));
        assert!(normalized.contains("CODECS=\"avc1.640029,mp4a.40.2\""));
        assert!(normalized.contains("FRAME-RATE=24"));
        assert!(normalized.contains("OLD=YES"));
    }

    #[test]
    fn phase21_client_capabilities_parse_quality_bounds_and_abr_support() {
        let caps: ClientCapabilities = serde_json::from_value(serde_json::json!({
            "client_kind": "web",
            "qualityMode": "automatic",
            "abrSupportType": "hls.js",
            "fixedBitrateBps": 2_000_000,
            "fixedResolution": "720p",
            "automaticMinBitrateBps": 800_000,
            "automaticMaxBitrateBps": 6_000_000,
            "automaticMinResolution": "360p",
            "automaticMaxResolution": "1080p",
            "maxBitrateBps": 8_000_000
        }))
        .expect("client capabilities");

        let profile = client_playback_profile_from_caps(&caps);

        assert_eq!(profile.quality_mode, QualityMode::Automatic);
        assert_eq!(profile.abr_support_type, AbrSupportType::HlsJs);
        assert_eq!(profile.fixed_bitrate_bps, Some(2_000_000));
        assert_eq!(profile.fixed_resolution.as_deref(), Some("720p"));
        assert_eq!(profile.automatic_min_bitrate_bps, Some(800_000));
        assert_eq!(profile.automatic_max_bitrate_bps, Some(6_000_000));
        assert_eq!(profile.automatic_min_resolution.as_deref(), Some("360p"));
        assert_eq!(profile.automatic_max_resolution.as_deref(), Some("1080p"));
    }

    fn phase21_adaptive_rung(
        id: &str,
        bandwidth_bps: i64,
        width: i32,
        height: i32,
    ) -> AdaptiveRungPlan {
        AdaptiveRungPlan {
            id: id.to_string(),
            label: format!("{height}p"),
            bandwidth_bps,
            average_bandwidth_bps: bandwidth_bps * 90 / 100,
            width,
            height,
            resolution: format!("{width}x{height}"),
            codecs: "avc1.640029,mp4a.40.2".to_string(),
            frame_rate: Some("24".to_string()),
            video: VideoOutputPlan {
                codec: "h264".to_string(),
                encoder: "libx264".to_string(),
                preset: "veryfast".to_string(),
                profile: Some("high".to_string()),
                level: Some("4.1".to_string()),
                crf: None,
                bitrate_bps: Some(bandwidth_bps),
                maxrate_bps: Some(bandwidth_bps),
                bufsize_bps: Some(bandwidth_bps * 2),
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
                reasons: Vec::new(),
            },
        }
    }

    #[test]
    fn phase17_hls_subtitle_media_preserves_metadata_and_escapes_attributes() {
        let playlist = "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1280000\nstream_0.m3u8\n";
        let renditions = vec![SubtitleRendition {
            name: "English \"SDH\"\nTrack".to_string(),
            language: Some("en\"g".to_string()),
            is_default: true,
            is_forced: true,
            is_hearing_impaired: true,
            uri: "sub_0.m3u8".to_string(),
        }];

        let rewritten = inject_subtitle_media(
            playlist,
            &renditions,
            "session-token",
            Some("bearer-token"),
            None,
        );

        assert!(rewritten.contains("#EXT-X-MEDIA:TYPE=SUBTITLES"));
        assert!(rewritten.contains("GROUP-ID=\"subs\""));
        assert!(rewritten.contains("NAME=\"English 'SDH' Track\""));
        assert!(rewritten.contains("LANGUAGE=\"en'g\""));
        assert!(rewritten.contains("DEFAULT=YES"));
        assert!(rewritten.contains("FORCED=YES"));
        assert!(rewritten.contains("CHARACTERISTICS=\"public.accessibility.describes-spoken-dialog,public.accessibility.describes-music-and-sound\""));
        assert!(rewritten.contains("URI=\"sub_0.m3u8?session=session-token&token=bearer-token\""));
        assert!(
            rewritten.find("#EXT-X-MEDIA").unwrap() < rewritten.find("#EXT-X-STREAM-INF").unwrap(),
            "{rewritten}"
        );
    }

    #[test]
    fn phase17_vtt_cue_shift_preserves_settings_and_drops_negative_cues() {
        let shifted = shift_vtt_cues(
            "WEBVTT\n\n00:00:00.100 --> 00:00:00.300\nexpired\n\n00:00:01.000 --> 00:00:03.250 align:start position:10%\nhello\n\n00:00:05,500 --> 00:00:06,000\ncomma\n",
            -0.5,
        );

        assert!(shifted.contains("WEBVTT"));
        assert!(!shifted.contains("expired"), "{shifted}");
        assert!(
            shifted.contains("00:00:00.500 --> 00:00:02.750 align:start position:10%"),
            "{shifted}"
        );
        assert!(shifted.contains("hello"), "{shifted}");
        assert!(
            shifted.contains("00:00:05.000 --> 00:00:05.500"),
            "{shifted}"
        );
        assert!(shifted.contains("comma"), "{shifted}");
    }

    #[test]
    fn phase17_vtt_timing_stays_within_tolerance_after_start_seek_and_boundary_shift() {
        fn cue_times(content: &str) -> Vec<(f64, f64)> {
            content
                .lines()
                .filter_map(|line| {
                    if !line.contains("-->") {
                        return None;
                    }
                    let mut parts = line.splitn(2, "-->");
                    let start = parse_vtt_time(parts.next()?.trim())?;
                    let (end_raw, _) = split_time_and_settings(parts.next()?.trim());
                    let end = parse_vtt_time(end_raw)?;
                    Some((start, end))
                })
                .collect()
        }

        let shifted = shift_vtt_cues(
            "WEBVTT\n\n00:00:00.000 --> 00:00:01.000\nstart\n\n00:00:03.900 --> 00:00:04.100 line:90%\nboundary\n\n01:02:03,456 --> 01:02:05,789 align:end\nlong\n",
            0.125,
        );
        let actual = cue_times(&shifted);
        let expected = [(0.125, 1.125), (4.025, 4.225), (3723.581, 3725.914)];

        assert_eq!(actual.len(), expected.len(), "{shifted}");
        for ((actual_start, actual_end), (expected_start, expected_end)) in
            actual.into_iter().zip(expected)
        {
            assert!(
                (actual_start - expected_start).abs() <= 0.001,
                "start drift exceeded 1 ms: actual={actual_start} expected={expected_start}\n{shifted}"
            );
            assert!(
                (actual_end - expected_end).abs() <= 0.001,
                "end drift exceeded 1 ms: actual={actual_end} expected={expected_end}\n{shifted}"
            );
            assert!(
                (actual_start - expected_start).abs() <= 0.250
                    && (actual_end - expected_end).abs() <= 0.250,
                "Phase 17 250 ms timing tolerance breached\n{shifted}"
            );
        }
    }

    #[test]
    fn playback_capacity_retry_details_match_recovery_contract() {
        let plan = PlaybackPlan {
            plan_version: PLAYBACK_PLAN_VERSION,
            mode: PlaybackMode::AdaptiveTranscode,
            delivery: Delivery::HlsAdaptiveFmp4,
            media_file_id: "media-file".to_string(),
            selected_video_track: None,
            video_action: StreamAction::Disabled,
            audio_action: StreamAction::Disabled,
            subtitle_action: StreamAction::Disabled,
            seek_behavior: SeekBehavior::ServerHlsRestart,
            adaptive: false,
            selected_audio_track: None,
            selected_subtitle_track: None,
            hdr_action: HdrAction::Unknown,
            hardware_acceleration: HardwareAccelerationPlan::default(),
            workload_class: None,
            feasibility: None,
            audio_output: None,
            video_output: None,
            adaptive_ladder: None,
            video_transcode_reason: None,
            compatibility_report: CompatibilityReport::empty("media-file"),
            reasons: vec![
                "adaptive_transcode_automatic_quality_requested".to_string(),
                "transcode_capacity_exhausted".to_string(),
                "adaptive_transcode_capacity_exhausted".to_string(),
            ],
            warnings: Vec::new(),
            expected_outputs: Vec::new(),
            playable: false,
        };

        let details = playback_capacity_retry_details(&plan);

        assert_eq!(
            details.get("reason").and_then(Value::as_str),
            Some("adaptive_transcode_automatic_quality_requested")
        );
        assert_eq!(
            details.pointer("/retry/allowed").and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            details
                .pointer("/retry/after_seconds")
                .and_then(Value::as_i64),
            Some(30)
        );
        assert_eq!(
            details.pointer("/retry/strategy").and_then(Value::as_str),
            Some("retry_same_request")
        );
        assert_eq!(
            details
                .pointer("/fallback/automatic_client_retry")
                .and_then(Value::as_bool),
            Some(false)
        );
        assert!(
            details
                .get("reasons")
                .and_then(Value::as_array)
                .is_some_and(|reasons| reasons
                    .iter()
                    .any(|reason| reason.as_str() == Some("transcode_capacity_exhausted"))),
            "{details}"
        );
    }

    #[test]
    fn phase13_playback_plan_contract_is_internal_dev_gated_by_default() {
        let mut settings = crate::config::Settings::default();
        settings.environment = RunEnvironment::Development;
        settings.playback.plan_contract_enabled = false;
        assert!(playback_plan_contract_allowed(&settings));

        settings.environment = RunEnvironment::Production;
        assert!(!playback_plan_contract_allowed(&settings));

        settings.playback.plan_contract_enabled = true;
        assert!(playback_plan_contract_allowed(&settings));
    }

    #[test]
    fn phase13_capacity_violation_covers_every_configured_resource() {
        let direct_play = phase13_test_plan(PlaybackMode::DirectPlay);
        let direct_stream = phase13_test_plan(PlaybackMode::DirectStream);
        let audio_transcode = phase13_test_plan(PlaybackMode::AudioTranscode);
        let video_transcode = phase13_test_plan(PlaybackMode::VideoTranscode);
        let adaptive_transcode = phase13_test_plan(PlaybackMode::AdaptiveTranscode);

        let mut config = crate::config::PlaybackConfig {
            max_active_sessions: Some(1),
            ..crate::config::PlaybackConfig::default()
        };
        let mut snapshot = phase13_capacity_snapshot();
        snapshot.active_sessions = 1;
        assert_capacity_resource(&config, &direct_play, &snapshot, "active_sessions");

        config = crate::config::PlaybackConfig {
            max_sessions_per_user: Some(1),
            ..crate::config::PlaybackConfig::default()
        };
        snapshot = phase13_capacity_snapshot();
        snapshot.active_user_sessions = 1;
        assert_capacity_resource(&config, &direct_play, &snapshot, "per_user_sessions");

        config = crate::config::PlaybackConfig {
            max_active_direct_streams: Some(1),
            ..crate::config::PlaybackConfig::default()
        };
        snapshot = phase13_capacity_snapshot();
        snapshot.active_direct_streams = 1;
        assert_capacity_resource(&config, &direct_stream, &snapshot, "direct_streams");
        assert!(
            playback_capacity_violation(&config, &audio_transcode, &snapshot).is_none(),
            "direct stream capacity must not block partial transcodes"
        );

        config = crate::config::PlaybackConfig {
            max_active_hls_jobs: Some(1),
            ..crate::config::PlaybackConfig::default()
        };
        snapshot = phase13_capacity_snapshot();
        snapshot.active_hls_jobs = 1;
        assert_capacity_resource(&config, &audio_transcode, &snapshot, "hls_jobs");
        assert!(
            playback_capacity_violation(&config, &direct_play, &snapshot).is_none(),
            "HLS job capacity must not block direct play"
        );

        config = crate::config::PlaybackConfig {
            max_active_video_transcodes: Some(1),
            ..crate::config::PlaybackConfig::default()
        };
        snapshot = phase13_capacity_snapshot();
        snapshot.active_video_transcode_weight = 1;
        assert_capacity_resource(&config, &video_transcode, &snapshot, "video_transcodes");

        snapshot = phase13_capacity_snapshot();
        assert_capacity_resource(&config, &adaptive_transcode, &snapshot, "video_transcodes");

        config = crate::config::PlaybackConfig {
            max_active_hardware_transcodes: Some(1),
            ..crate::config::PlaybackConfig::default()
        };
        let mut hardware_plan = video_transcode.clone();
        hardware_plan.hardware_acceleration.enabled = true;
        snapshot = phase13_capacity_snapshot();
        snapshot.active_hardware_transcodes = 1;
        assert_capacity_resource(&config, &hardware_plan, &snapshot, "hardware_transcodes");

        config = crate::config::PlaybackConfig {
            max_startup_queue_length: Some(1),
            ..crate::config::PlaybackConfig::default()
        };
        snapshot = phase13_capacity_snapshot();
        snapshot.startup_queue_len = 1;
        assert_capacity_resource(&config, &direct_stream, &snapshot, "startup_queue_length");

        config = crate::config::PlaybackConfig {
            max_temp_dir_bytes: Some(1),
            ..crate::config::PlaybackConfig::default()
        };
        snapshot = phase13_capacity_snapshot();
        snapshot.temp_dir_bytes = 1;
        assert_capacity_resource(&config, &direct_stream, &snapshot, "temp_dir_bytes");

        config = crate::config::PlaybackConfig {
            max_ffmpeg_log_bytes: Some(1),
            ..crate::config::PlaybackConfig::default()
        };
        snapshot = phase13_capacity_snapshot();
        snapshot.ffmpeg_log_bytes = 1;
        assert_capacity_resource(&config, &direct_stream, &snapshot, "ffmpeg_log_bytes");
    }
}
