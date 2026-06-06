use std::{
    cmp::Ordering,
    path::{Path, PathBuf},
    time::Duration,
};

use axum::{
    Json,
    body::Body,
    extract::{Path as AxumPath, Query, State},
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, RANGE},
    },
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{Row, any::AnyRow};
use uuid::Uuid;

use crate::{
    db::models::{MediaType, PlaybackMode},
    http::{
        auth::CurrentUser,
        error::{ApiError, ApiResult},
    },
    media::ffprobe,
    metrics::{
        PLAY_DECISIONS, PLAY_LATENCY, SEGMENT_SERVED, TRANSCODE_DURATION, TRANSCODE_ERRORS,
        TRANSCODE_STARTS,
    },
    network::registry::ensure_server_instance,
    playback::{HLS_SEGMENT_SECONDS, SubtitleInfo, TranscodeParams},
    state::AppState,
};
use tokio::time::sleep;
use tokio::{
    fs::{self, File},
    io::{AsyncReadExt, AsyncSeekExt, SeekFrom},
    process::Command,
};
use tokio_util::io::ReaderStream;
use tracing::info;

#[derive(Debug, Deserialize)]
pub struct PlayRequest {
    pub media_item_id: String,
    pub preferred_file_id: Option<String>,
    pub preferred_episode_id: Option<String>,
    pub network_type: Option<String>,
    pub client_capabilities: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct PlayResponse {
    pub session_id: String,
    pub mode: &'static str,
    pub stream_url: String,
    pub duration_seconds: Option<i32>,
    pub logical_start_seconds: i32,
    pub media_file_id: String,
    pub server_id: String,
    pub wan_direct_endpoint: Option<String>,
    pub state: String,
    pub logical_position_seconds: f32,
}

#[derive(Debug, Clone)]
pub struct EffectiveProfile {
    pub max_resolution: String,
    pub supported_containers: Vec<String>,
    pub supported_video_codecs: Vec<String>,
    pub supported_audio_codecs: Vec<String>,
    pub max_bitrate_bps: Option<i64>,
}
#[derive(Debug, Deserialize, Clone)]
pub struct ClientCapabilities {
    pub client_kind: Option<String>,
    pub direct_play_preferred: Option<bool>,
    pub max_resolution: Option<String>,
    pub supported_containers: Option<Vec<String>>,
    pub supported_video_codecs: Option<Vec<String>>,
    pub supported_audio_codecs: Option<Vec<String>>,
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

    let scoped_episode_id = if matches!(item.r#type, MediaType::Movie) {
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

    let profile = profile_for_network(&state.settings.playback, body.network_type.as_deref());
    let caps_json = body.client_capabilities.clone();
    let mut caps = caps_json
        .clone()
        .and_then(|v| serde_json::from_value::<ClientCapabilities>(v).ok())
        .unwrap_or_else(|| {
            default_capabilities(
                &state.settings.playback,
                body.network_type.as_deref(),
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
        body.network_type.as_deref(),
        item.runtime_seconds,
    )
    .ok_or_else(|| ApiError::not_found("requested file not found"))?;
    let decision = decide_playback(
        selected,
        &caps,
        &profile,
        body.network_type.as_deref(),
        item.runtime_seconds,
    );
    if decision.mode == PlaybackMode::Transcode {
        TRANSCODE_STARTS
            .with_label_values(&[
                "pending",
                selected.container.as_deref().unwrap_or("unknown"),
                selected.video_codec.as_deref().unwrap_or("unknown"),
            ])
            .inc();
    }
    let session_id = Uuid::new_v4();
    let session_token = Uuid::new_v4().to_string();
    info!(
        user = %user.user_id,
        item = %body.media_item_id,
        requested_file = ?requested_file_id,
        requested_episode = ?requested_episode_id,
        episode = ?scoped_episode_id,
        file = %selected.id,
        mode = %decision.mode_as_str(),
        network = ?body.network_type,
        reason = %decision.reason,
        "play decision"
    );
    PLAY_DECISIONS
        .with_label_values(&[
            decision.mode_as_str(),
            body.network_type.as_deref().unwrap_or("unknown"),
            selected.container.as_deref().unwrap_or("unknown"),
            selected.video_codec.as_deref().unwrap_or("unknown"),
        ])
        .inc();

    let server_id = ensure_server_instance(&state.db_pool, &state.settings, user.user_id).await?;
    let wan_direct_endpoint: Option<String> = sqlx::query_scalar(
        "SELECT wan_direct_endpoint FROM server_registry ORDER BY last_seen_at DESC LIMIT 1",
    )
    .fetch_optional(&state.db_pool)
    .await
    .ok()
    .flatten();

    let stream_url = match decision.mode {
        PlaybackMode::DirectPlay => {
            format!(
                "/stream/direct/{}?sid={}&session={}",
                selected.id, session_id, session_token
            )
        }
        PlaybackMode::Transcode => {
            format!(
                "/sessions/{}/master.m3u8?session={}",
                session_id, session_token
            )
        }
    };

    let resolved_duration = resolve_duration_seconds(
        &state,
        &body.media_item_id,
        &selected.path,
        item.runtime_seconds,
        item.r#type,
    )
    .await;

    let transcode_state = match decision.mode {
        PlaybackMode::Transcode => Some(serde_json::json!({
            "seek_seconds": 0.0,
        })),
        _ => None,
    };

    sqlx::query::<sqlx::Any>("INSERT INTO playback_sessions (id, user_id, server_id, media_file_id, mode, state, network_type, logical_position_seconds, duration_seconds, client_capabilities, transcode_state, token) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .bind(session_id.to_string())
        .bind(user.user_id.to_string())
        .bind(server_id.to_string())
        .bind(&selected.id)
        .bind(decision.mode_as_str())
        .bind("active")
        .bind(body.network_type.clone())
        .bind(0f32)
        .bind(resolved_duration)
        .bind(caps_json.as_ref().map(|v| v.to_string()))
        .bind(transcode_state.as_ref().map(|s| s.to_string()))
        .bind(session_token.clone())
        .execute(&state.db_pool)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;

    latency_timer.stop_and_record();

    let response = PlayResponse {
        session_id: session_id.to_string(),
        mode: decision.mode_as_str(),
        stream_url,
        duration_seconds: resolved_duration,
        logical_start_seconds: 0,
        media_file_id: selected.id.clone(),
        server_id: server_id.to_string(),
        wan_direct_endpoint,
        state: "active".to_string(),
        logical_position_seconds: 0.0,
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

struct PlaybackDecision {
    mode: PlaybackMode,
    reason: String,
}

impl PlaybackDecision {
    fn mode_as_str(&self) -> &'static str {
        match self.mode {
            PlaybackMode::DirectPlay => "direct_play",
            PlaybackMode::Transcode => "transcode",
        }
    }
}

fn default_capabilities(
    config: &crate::config::PlaybackConfig,
    network: Option<&str>,
    profile: &EffectiveProfile,
) -> ClientCapabilities {
    let max_bitrate = match network {
        Some("wan") => profile
            .max_bitrate_bps
            .or(config.default_wan_max_bitrate_bps),
        _ => profile
            .max_bitrate_bps
            .or(config.default_lan_max_bitrate_bps),
    };

    ClientCapabilities {
        client_kind: None,
        direct_play_preferred: None,
        max_resolution: Some(profile.max_resolution.clone()),
        supported_containers: Some(profile.supported_containers.clone()),
        supported_video_codecs: Some(profile.supported_video_codecs.clone()),
        supported_audio_codecs: Some(profile.supported_audio_codecs.clone()),
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

fn decide_playback(
    file: &FileRow,
    caps: &ClientCapabilities,
    profile: &EffectiveProfile,
    network_type: Option<&str>,
    item_duration: Option<i32>,
) -> PlaybackDecision {
    let allow_container = caps
        .supported_containers
        .as_ref()
        .map(|c| matches_or_unknown(file.container.as_deref(), c))
        .unwrap_or(true);
    let allow_video = caps
        .supported_video_codecs
        .as_ref()
        .map(|c| matches_or_unknown(file.video_codec.as_deref(), c))
        .unwrap_or(true);
    let allow_audio = caps
        .supported_audio_codecs
        .as_ref()
        .map(|c| matches_or_unknown(file.audio_codec.as_deref(), c))
        .unwrap_or(true);

    let res_ok = resolution_within_cap(file.height, caps.max_resolution.as_deref());

    let native_direct = native_direct_play_client(caps);
    let profile_bitrate_cap = (!native_direct)
        .then_some(profile.max_bitrate_bps)
        .flatten();
    let client_bitrate_cap = positive_bitrate_cap(caps.max_bitrate_bps);
    let bitrate_cap = match (network_type, client_bitrate_cap) {
        (Some("wan"), Some(max)) => Some(max.min(8_000_000)), // cap WAN more tightly
        (Some("wan"), None) => Some(8_000_000),
        (Some("lan"), _) => None, // relax bitrate enforcement on LAN
        (_, max) => max.or(profile_bitrate_cap),
    };

    let bitrate_val = effective_bitrate(file, item_duration);
    let bitrate_ok = bitrate_cap.map(|max| bitrate_val <= max).unwrap_or(true);
    let mut reasons = Vec::new();
    if !allow_container {
        reasons.push("container unsupported");
    }
    if !allow_video {
        reasons.push("video codec unsupported");
    }
    if !allow_audio {
        reasons.push("audio codec unsupported");
    }
    if !res_ok {
        reasons.push("resolution too high");
    }
    if !bitrate_ok {
        reasons.push("bitrate exceeds cap");
    }

    if reasons.is_empty() {
        PlaybackDecision {
            mode: PlaybackMode::DirectPlay,
            reason: "direct play: all capabilities satisfied".to_string(),
        }
    } else {
        PlaybackDecision {
            mode: PlaybackMode::Transcode,
            reason: format!("transcode: {}", reasons.join(", ")),
        }
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

pub async fn stream_direct(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Query(params): Query<StreamQuery>,
    headers: HeaderMap,
    user: CurrentUser,
) -> ApiResult<impl IntoResponse> {
    let session_id = params
        .sid
        .as_deref()
        .ok_or_else(|| ApiError::unauthorized("session id required"))?;
    let session_token = params
        .session
        .as_deref()
        .ok_or_else(|| ApiError::unauthorized("session token required"))?;
    let _ = get_session_with_token(
        &state,
        &user,
        session_id,
        Some("direct_play"),
        session_token,
    )
    .await?;

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
        .map_err(|_| ApiError::not_found("file not available on disk"))?;
    let meta = file
        .metadata()
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;
    let file_len = meta.len();

    let range_header = headers
        .get(RANGE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let content_type = content_type_for(&path, container.as_deref());

    if let Some(range_header) = range_header {
        let ranges = http_range::HttpRange::parse(&range_header, file_len)
            .map_err(|_| ApiError::bad_request("invalid range"))?;
        if ranges.is_empty() {
            return Err(ApiError::bad_request("invalid range"));
        }
        let first = ranges[0];
        let start = first.start;
        let end = start + first.length - 1;
        let len = first.length;

        file.seek(SeekFrom::Start(start))
            .await
            .map_err(|e| ApiError::internal(e.to_string()))?;
        let stream = ReaderStream::new(file.take(len));
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
        headers.insert(
            CONTENT_RANGE,
            HeaderValue::from_str(&format!("bytes {start}-{end}/{file_len}"))
                .unwrap_or_else(|_| HeaderValue::from_static("bytes */0")),
        );
        headers.insert(
            CONTENT_LENGTH,
            HeaderValue::from_str(&len.to_string()).unwrap_or(HeaderValue::from_static("0")),
        );
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_str(&content_type)
                .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
        );
        let body = Body::from_stream(stream);
        return Ok((StatusCode::PARTIAL_CONTENT, headers, body));
    }

    let stream = ReaderStream::new(file);
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    headers.insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&file_len.to_string()).unwrap_or(HeaderValue::from_static("0")),
    );
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_str(&content_type)
            .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );
    let body = Body::from_stream(stream);
    Ok((StatusCode::OK, headers, body))
}

fn content_type_for(path: &str, container: Option<&str>) -> String {
    if let Some(ext) = container.or_else(|| Path::new(path).extension().and_then(|e| e.to_str())) {
        match ext.to_ascii_lowercase().as_str() {
            "mp4" => "video/mp4".to_string(),
            "mkv" => "video/x-matroska".to_string(),
            "mov" => "video/quicktime".to_string(),
            "avi" => "video/x-msvideo".to_string(),
            _ => "application/octet-stream".to_string(),
        }
    } else {
        "application/octet-stream".to_string()
    }
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
        Some("transcode"),
        session_token,
    )
    .await?;
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

    let handle = state
        .transcodes
        .start_or_get(session_id, &media_path, TranscodeParams { seek_seconds })
        .await
        .map_err(|e| {
            let msg = format!("transcode spawn failed: {e}");
            let session_id = id.clone();
            let state_clone = state.clone();
            TRANSCODE_ERRORS.with_label_values(&["spawn_failed"]).inc();
            let _ = tokio::spawn(async move {
                mark_session_error(state_clone, &session_id, Some(msg), None).await
            });
            ApiError::internal(e.to_string())
        })?;
    TRANSCODE_STARTS
        .with_label_values(&["ok", "unknown", "unknown"])
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
            TRANSCODE_STARTS
                .with_label_values(&["error", "unknown", "unknown"])
                .inc();
            TRANSCODE_ERRORS.with_label_values(&["playlist_read"]).inc();
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

    // Touch updated_at for TTL and store log_path/seek for debugging.
    let _ = sqlx::query::<sqlx::Any>(
        "UPDATE playback_sessions SET updated_at = CURRENT_TIMESTAMP, state = 'active', logical_position_seconds = ?, transcode_state = ? WHERE id = ?",
    )
    .bind(seek_seconds)
    .bind(
        serde_json::json!({
            "seek_seconds": seek_seconds,
            "log_path": handle.log_path.to_string_lossy(),
            "temp_dir": handle.temp_dir.to_string_lossy(),
            "pid": handle.pid,
        })
        .to_string(),
    )
    .bind(&id)
    .execute(&state.db_pool)
    .await;
    TRANSCODE_DURATION
        .with_label_values(&["ok"])
        .observe(start_time.elapsed().as_secs_f64());

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
    let session_id = Uuid::parse_str(&id).map_err(|_| ApiError::bad_request("invalid session"))?;
    // Ensure session belongs to user and token matches
    let session_token = params
        .session
        .as_deref()
        .ok_or_else(|| ApiError::unauthorized("session token required"))?;
    let _session_row = get_session_with_token(
        &state,
        &user,
        &session_id.to_string(),
        Some("transcode"),
        session_token,
    )
    .await?;
    info!(
        session = %session_id,
        segment = %segment,
        "serving hls segment"
    );
    let path = state
        .transcodes
        .segment_path(session_id, &segment)
        .await
        .ok_or_else(|| ApiError::not_found("segment not found"))?;
    if !path.exists() {
        SEGMENT_SERVED.with_label_values(&["missing"]).inc();
        return Err(ApiError::not_found("segment not found"));
    }
    let ext = Path::new(&segment)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    let subtitle_delay = if ext == "vtt" {
        resolve_subtitle_delay(&state, session_id).await
    } else {
        None
    };

    if ext == "m3u8" {
        let raw = fs::read_to_string(&path).await.map_err(|e| {
            SEGMENT_SERVED.with_label_values(&["error"]).inc();
            ApiError::internal(e.to_string())
        })?;
        let rewritten = rewrite_playlist_with_token(
            &raw,
            session_token,
            params.token.as_deref(),
            params.ts.as_deref(),
        );
        SEGMENT_SERVED.with_label_values(&["ok"]).inc();
        return Ok((
            StatusCode::OK,
            [(
                CONTENT_TYPE,
                HeaderValue::from_static("application/vnd.apple.mpegurl"),
            )],
            rewritten.into_bytes(),
        ));
    }

    if ext == "vtt" {
        let raw = fs::read_to_string(&path).await.map_err(|e| {
            SEGMENT_SERVED.with_label_values(&["error"]).inc();
            ApiError::internal(e.to_string())
        })?;
        let adjusted = match subtitle_delay {
            Some(delay) if delay.abs() >= 0.01 => shift_vtt_cues(&raw, delay),
            _ => raw,
        };
        SEGMENT_SERVED.with_label_values(&["ok"]).inc();
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
        ApiError::internal(e.to_string())
    })?;
    SEGMENT_SERVED.with_label_values(&["ok"]).inc();
    let content_type = match ext.as_str() {
        "ts" => "video/MP2T",
        "m4s" => "video/iso.segment",
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
    pub state: String,
    pub network_type: Option<String>,
    pub logical_position_seconds: f32,
    pub duration_seconds: Option<i32>,
    pub wan_direct_endpoint: Option<String>,
    pub transcode_state: Option<serde_json::Value>,
    pub log_path: Option<String>,
    pub error: Option<String>,
    pub updated_at: Option<String>,
}

async fn get_session(
    state: &AppState,
    user: &CurrentUser,
    session_id: &str,
    expected_mode: Option<&str>,
    require_active: bool,
) -> ApiResult<AnyRow> {
    let row = sqlx::query("SELECT id, user_id, server_id, media_file_id, mode, state, network_type, logical_position_seconds, duration_seconds, transcode_state, token, CAST(updated_at AS TEXT) as updated_at FROM playback_sessions WHERE id = ? LIMIT 1")
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
    }

    if let Some(expected) = expected_mode {
        let mode: String = row.get("mode");
        if mode != expected {
            return Err(ApiError::unauthorized("invalid session"));
        }
    }

    Ok(row)
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
    Err(ApiError::internal(msg))
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
    Ok(row)
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
            if line.starts_with('#') || line.trim().is_empty() {
                return line.to_string();
            }

            let mut url = append_query_param(line, "session", session_token);
            if let Some(tok) = auth_token {
                url = append_query_param(&url, "token", tok);
            }
            if let Some(ts) = cache_bust {
                url = append_query_param(&url, "ts", ts);
            }
            url
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[derive(Debug, Clone)]
struct SubtitleRendition {
    name: String,
    language: Option<String>,
    is_default: bool,
    is_forced: bool,
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
        let mut subtitle_url = append_query_param(&rendition.uri, "session", session_token);
        if let Some(tok) = auth_token {
            subtitle_url = append_query_param(&subtitle_url, "token", tok);
        }
        if let Some(ts) = cache_bust {
            subtitle_url = append_query_param(&subtitle_url, "ts", ts);
        }

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
    let temp_anchor = state
        .transcodes
        .segment_path(session_id, "seg_0_00000.ts")
        .await?;
    let temp_dir = temp_anchor.parent().map(PathBuf::from)?;
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
            "error": m,
            "log_path": log_path,
        })
        .to_string()
    });
    let _ = sqlx::query::<sqlx::Any>(
        "UPDATE playback_sessions SET state = 'error', updated_at = CURRENT_TIMESTAMP, transcode_state = COALESCE(?, transcode_state) WHERE id = ?",
    )
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
    let transcode_state: Option<serde_json::Value> = session
        .try_get::<String, _>("transcode_state")
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok());
    let log_path = transcode_state
        .as_ref()
        .and_then(|v| v.get("log_path").and_then(Value::as_str))
        .map(|s| s.to_string());
    let error = transcode_state
        .as_ref()
        .and_then(|v| v.get("error").and_then(Value::as_str))
        .map(|s| s.to_string());

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
        mode: session.get("mode"),
        state: session.get("state"),
        network_type: session.try_get("network_type").ok(),
        logical_position_seconds,
        duration_seconds: session
            .try_get::<i64, _>("duration_seconds")
            .ok()
            .map(|v| v as i32),
        wan_direct_endpoint,
        transcode_state,
        log_path,
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
    pub logical_position_seconds: f32,
    pub duration_seconds: Option<i32>,
    pub log_path: Option<String>,
    pub error: Option<String>,
}

pub async fn poll_session(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    user: CurrentUser,
) -> ApiResult<Json<SessionPollResponse>> {
    let session = get_session(&state, &user, &id, None, false).await?;
    let transcode_state: Option<serde_json::Value> = session
        .try_get::<String, _>("transcode_state")
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok());
    let log_path = transcode_state
        .as_ref()
        .and_then(|v| v.get("log_path").and_then(Value::as_str))
        .map(|s| s.to_string());
    let error = transcode_state
        .as_ref()
        .and_then(|v| v.get("error").and_then(Value::as_str))
        .map(|s| s.to_string());
    let logical_position_seconds = session
        .try_get::<f64, _>("logical_position_seconds")
        .ok()
        .map(|v| v as f32)
        .unwrap_or(0.0);

    let response = SessionPollResponse {
        id: id.clone(),
        state: session.get("state"),
        mode: session.get("mode"),
        logical_position_seconds,
        duration_seconds: session
            .try_get::<i64, _>("duration_seconds")
            .ok()
            .map(|v| v as i32),
        log_path,
        error,
    };

    Ok(Json(response))
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
    let session_row = get_session(
        &state,
        &user,
        &session_id.to_string(),
        Some("transcode"),
        true,
    )
    .await?;
    let media_file_id: String = session_row.get("media_file_id");

    let media_path: Option<String> = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT path FROM media_files WHERE id = ? LIMIT 1",
    )
    .bind(&media_file_id)
    .fetch_optional(&state.db_pool)
    .await
    .map_err(|e| ApiError::internal(e.to_string()))?;

    let media_path = media_path.ok_or_else(|| ApiError::not_found("file not found"))?;

    let handle = state
        .transcodes
        .restart(session_id, &media_path, body.position_seconds)
        .await
        .map_err(|e| {
            let msg = format!("transcode restart failed: {e}");
            let session_id = id.clone();
            let state_clone = state.clone();
            let _ = tokio::spawn(async move {
                mark_session_error(state_clone, &session_id, Some(msg), None).await
            });
            TRANSCODE_ERRORS
                .with_label_values(&["restart_failed"])
                .inc();
            ApiError::internal(e.to_string())
        })?;
    TRANSCODE_STARTS
        .with_label_values(&["restart", "unknown", "unknown"])
        .inc();

    sqlx::query::<sqlx::Any>("UPDATE playback_sessions SET transcode_state = ?, logical_position_seconds = ?, state = 'active', updated_at = CURRENT_TIMESTAMP WHERE id = ?")
        .bind(
            serde_json::json!({
                "seek_seconds": body.position_seconds,
                "log_path": handle.log_path.to_string_lossy(),
                "temp_dir": handle.temp_dir.to_string_lossy(),
                "pid": handle.pid,
            })
            .to_string(),
        )
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

    fn constrained_profile() -> EffectiveProfile {
        EffectiveProfile {
            max_resolution: "1080p".to_string(),
            supported_containers: vec!["mp4".to_string(), "mkv".to_string()],
            supported_video_codecs: vec!["h264".to_string()],
            supported_audio_codecs: vec!["aac".to_string(), "ac3".to_string()],
            max_bitrate_bps: Some(6_000_000),
        }
    }

    fn succession_4k_file() -> FileRow {
        FileRow {
            id: "file-1".to_string(),
            path: "Succession.S01E01.2160p.HEVC.EAC3.mkv".to_string(),
            container: Some("matroska,webm".to_string()),
            video_codec: Some("hevc".to_string()),
            audio_codec: Some("eac3".to_string()),
            width: 3840,
            height: 2160,
            bitrate_bps: 22_000_000,
            size_bytes: Some(10_000_000_000),
        }
    }

    #[test]
    fn native_mpv_caps_direct_play_4k_hevc_without_profile_cap() {
        let profile = constrained_profile();
        let caps = ClientCapabilities {
            client_kind: Some("native_mpv".to_string()),
            direct_play_preferred: Some(true),
            max_resolution: Some("unlimited".to_string()),
            supported_containers: Some(vec!["mkv".to_string(), "mp4".to_string()]),
            supported_video_codecs: Some(vec!["h264".to_string(), "hevc".to_string()]),
            supported_audio_codecs: Some(vec![
                "aac".to_string(),
                "ac3".to_string(),
                "eac3".to_string(),
            ]),
            max_bitrate_bps: Some(0),
        };

        let merged = merge_caps_with_profile(caps, &profile);
        let decision = decide_playback(&succession_4k_file(), &merged, &profile, None, Some(3600));

        assert_eq!(decision.mode, PlaybackMode::DirectPlay);
        assert_eq!(decision.reason, "direct play: all capabilities satisfied");
    }

    #[test]
    fn generic_browser_caps_still_transcode_4k_hevc_against_profile() {
        let profile = constrained_profile();
        let caps = ClientCapabilities {
            client_kind: None,
            direct_play_preferred: None,
            max_resolution: Some("2160p".to_string()),
            supported_containers: Some(vec!["mkv".to_string(), "mp4".to_string()]),
            supported_video_codecs: Some(vec!["h264".to_string(), "hevc".to_string()]),
            supported_audio_codecs: Some(vec![
                "aac".to_string(),
                "ac3".to_string(),
                "eac3".to_string(),
            ]),
            max_bitrate_bps: Some(50_000_000),
        };

        let merged = merge_caps_with_profile(caps, &profile);
        let decision = decide_playback(&succession_4k_file(), &merged, &profile, None, Some(3600));

        assert_eq!(decision.mode, PlaybackMode::Transcode);
        assert!(
            decision.reason.contains("resolution too high")
                || decision.reason.contains("video codec unsupported")
                || decision.reason.contains("bitrate exceeds cap"),
            "{}",
            decision.reason
        );
    }
}
