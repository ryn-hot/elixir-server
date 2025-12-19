use std::{cmp::Ordering, path::Path};

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
    metrics::{
        PLAY_DECISIONS, PLAY_LATENCY, SEGMENT_SERVED, TRANSCODE_DURATION, TRANSCODE_ERRORS,
        TRANSCODE_STARTS,
    },
    network::registry::ensure_server_instance,
    playback::TranscodeParams,
    state::AppState,
};
use tokio::time::sleep;
use tokio::{
    fs::{self, File},
    io::{AsyncReadExt, AsyncSeekExt, SeekFrom},
};
use tokio_util::io::ReaderStream;
use tracing::info;

#[derive(Debug, Deserialize)]
pub struct PlayRequest {
    pub media_item_id: String,
    pub preferred_file_id: Option<String>,
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
    let item: Option<MediaItemRow> =
        sqlx::query("SELECT type, runtime_seconds FROM media_items WHERE id = ? LIMIT 1")
            .bind(&body.media_item_id)
            .fetch_optional(&state.db_pool)
            .await
            .map_err(|e| ApiError::internal(e.to_string()))?
            .map(|row| MediaItemRow {
                r#type: item_type(row.get::<String, _>("type").as_str())
                    .unwrap_or(MediaType::Movie),
                runtime_seconds: row
                    .try_get::<i64, _>("runtime_seconds")
                    .ok()
                    .map(|v| v as i32),
            });

    let item = item.ok_or_else(|| ApiError::not_found("media item not found"))?;

    let rows = sqlx::query(
        "SELECT id, path, container, video_codec, audio_codec, COALESCE(width, 0) as width, COALESCE(height, 0) as height, COALESCE(bitrate_bps, 0) as bitrate_bps, size_bytes FROM media_files WHERE media_item_id = ? AND scan_state = 'ok'",
    )
    .bind(&body.media_item_id)
    .fetch_all(&state.db_pool)
    .await
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
        return Err(ApiError::not_found("no playable files for item"));
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
        body.preferred_file_id.as_deref(),
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
        .bind(item.runtime_seconds)
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
        duration_seconds: item.runtime_seconds,
        logical_start_seconds: 0,
        media_file_id: selected.id.clone(),
        server_id: server_id.to_string(),
        wan_direct_endpoint,
        state: "active".to_string(),
        logical_position_seconds: 0.0,
    };

    Ok(Json(response))
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
        if let Some(f) = files.iter().find(|f| f.id == pref) {
            return Some(f);
        }
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

        let res_ok = match caps.max_resolution.as_deref() {
            Some("720p") => f.height <= 720,
            Some("1080p") => f.height <= 1080,
            Some("1440p") => f.height <= 1440,
            Some("4k") | Some("2160p") => f.height <= 2160,
            _ => true,
        };

        let bitrate_cap = match (network, caps.max_bitrate_bps) {
            (Some("wan"), Some(max)) => Some(max.min(8_000_000)),
            (Some("wan"), None) => Some(8_000_000),
            (Some("lan"), _) => None,
            (_, max) => max.or(profile.max_bitrate_bps),
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
    if let (Some(client), Some(profile_bps)) = (caps.max_bitrate_bps, profile.max_bitrate_bps) {
        caps.max_bitrate_bps = Some(client.min(profile_bps));
    } else if caps.max_bitrate_bps.is_none() {
        caps.max_bitrate_bps = profile.max_bitrate_bps;
    }

    caps
}

fn min_resolution(a: &str, b: &str) -> String {
    let rank = |r: &str| -> i32 {
        match r.to_ascii_lowercase().as_str() {
            "720p" => 1,
            "1080p" => 2,
            "1440p" => 3,
            "4k" | "2160p" => 4,
            _ => 0,
        }
    };
    if rank(a) <= rank(b) {
        a.to_string()
    } else {
        b.to_string()
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

    let res_ok = match caps.max_resolution.as_deref() {
        Some("720p") => file.height <= 720,
        Some("1080p") => file.height <= 1080,
        Some("1440p") => file.height <= 1440,
        Some("4k") | Some("2160p") => file.height <= 2160,
        _ => true,
    };

    let bitrate_cap = match (network_type, caps.max_bitrate_bps) {
        (Some("wan"), Some(max)) => Some(max.min(8_000_000)), // cap WAN more tightly
        (Some("wan"), None) => Some(8_000_000),
        (Some("lan"), _) => None, // relax bitrate enforcement on LAN
        (_, max) => max.or(profile.max_bitrate_bps),
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
    let playlist_body =
        rewrite_playlist_with_token(&content, session_token, params.token.as_deref());

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
    let _ = get_session_with_token(
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
    let data = fs::read(&path).await.map_err(|e| {
        SEGMENT_SERVED.with_label_values(&["error"]).inc();
        ApiError::internal(e.to_string())
    })?;
    SEGMENT_SERVED.with_label_values(&["ok"]).inc();
    Ok((
        StatusCode::OK,
        [(CONTENT_TYPE, HeaderValue::from_static("video/MP2T"))],
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
) -> String {
    content
        .lines()
        .map(|line| {
            if line.starts_with('#') {
                line.to_string()
            } else if line.contains("seg_") {
                let mut url = if line.contains('?') {
                    format!("{line}&session={session_token}")
                } else {
                    format!("{line}?session={session_token}")
                };
                if let Some(tok) = auth_token {
                    url.push('&');
                    url.push_str("token=");
                    url.push_str(tok);
                }
                url
            } else {
                let mut url = line.to_string();
                if let Some(tok) = auth_token {
                    if url.contains('?') {
                        url.push('&');
                        url.push_str("token=");
                        url.push_str(tok);
                    } else {
                        url.push_str("?token=");
                        url.push_str(tok);
                    }
                }
                url
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
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
) -> ApiResult<Json<&'static str>> {
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
    Ok(Json("ok"))
}

pub async fn seek_transcode(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    user: CurrentUser,
    Json(body): Json<SeekRequest>,
) -> ApiResult<Json<&'static str>> {
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

    Ok(Json("ok"))
}
