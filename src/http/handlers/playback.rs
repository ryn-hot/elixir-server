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
    metrics::{PLAY_DECISIONS, PLAY_LATENCY, TRANSCODE_STARTS},
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
#[serde(rename_all = "camelCase")]
pub struct PlayResponse {
    pub session_id: String,
    pub mode: &'static str,
    pub stream_url: String,
    pub duration_seconds: Option<i32>,
    pub logical_start_seconds: i32,
    pub media_file_id: String,
    pub server_id: String,
    pub wan_direct_endpoint: Option<String>,
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
        "SELECT id, path, container, video_codec, audio_codec, COALESCE(width, 0) as width, COALESCE(height, 0) as height, COALESCE(bitrate_bps, 0) as bitrate_bps FROM media_files WHERE media_item_id = ? AND scan_state = 'ok'",
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
        });
    }

    if files.is_empty() {
        return Err(ApiError::not_found("no playable files for item"));
    }

    let caps_json = body.client_capabilities.clone();
    let caps = caps_json
        .clone()
        .and_then(|v| serde_json::from_value::<ClientCapabilities>(v).ok())
        .unwrap_or_else(|| {
            default_capabilities(&state.settings.playback, body.network_type.as_deref())
        });

    let selected = select_file(
        &files,
        body.preferred_file_id.as_deref(),
        &caps,
        body.network_type.as_deref(),
    )
    .ok_or_else(|| ApiError::not_found("requested file not found"))?;
    let decision = decide_playback(selected, &caps, body.network_type.as_deref());
    let session_id = Uuid::new_v4();
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
            format!("/stream/direct/{}?session={}", selected.id, session_id)
        }
        PlaybackMode::Transcode => {
            format!(
                "/sessions/{}/master.m3u8?session={}",
                session_id, session_id
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
        .bind(session_id.to_string()) // simple token equal to session id for now
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
    };

    Ok(Json(response))
}

fn select_file<'a>(
    files: &'a [FileRow],
    preferred: Option<&str>,
    caps: &ClientCapabilities,
    network: Option<&str>,
) -> Option<&'a FileRow> {
    if let Some(pref) = preferred {
        if let Some(f) = files.iter().find(|f| f.id == pref) {
            return Some(f);
        }
    }

    files
        .iter()
        .max_by(|a, b| compare_files(a, b, caps, network))
}

fn compare_files(
    a: &FileRow,
    b: &FileRow,
    caps: &ClientCapabilities,
    network: Option<&str>,
) -> Ordering {
    let cmp_tuple = |f: &FileRow| -> (bool, i32, i64, i64, i32) {
        let container_match = caps
            .supported_containers
            .as_ref()
            .map(|c| c.iter().any(|s| eq_ci(f.container.as_deref(), Some(s))))
            .unwrap_or(true);
        let video_match = caps
            .supported_video_codecs
            .as_ref()
            .map(|c| c.iter().any(|s| eq_ci(f.video_codec.as_deref(), Some(s))))
            .unwrap_or(true);
        let audio_match = caps
            .supported_audio_codecs
            .as_ref()
            .map(|c| c.iter().any(|s| eq_ci(f.audio_codec.as_deref(), Some(s))))
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
            (_, max) => max,
        };
        let bitrate_ok = if let Some(max) = bitrate_cap {
            f.bitrate_bps <= max || f.bitrate_bps == 0
        } else {
            true
        };

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
            Some(_) => -f.bitrate_bps,
            None => f.bitrate_bps,
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
) -> ClientCapabilities {
    let max_bitrate = match network {
        Some("wan") => config.default_wan_max_bitrate_bps,
        _ => config.default_lan_max_bitrate_bps,
    };

    ClientCapabilities {
        max_resolution: Some(config.default_max_resolution.clone()),
        supported_containers: Some(config.default_supported_containers.clone()),
        supported_video_codecs: Some(config.default_supported_video_codecs.clone()),
        supported_audio_codecs: Some(config.default_supported_audio_codecs.clone()),
        max_bitrate_bps: max_bitrate,
    }
}

fn decide_playback(
    file: &FileRow,
    caps: &ClientCapabilities,
    network_type: Option<&str>,
) -> PlaybackDecision {
    let allow_container = caps
        .supported_containers
        .as_ref()
        .map(|c| c.iter().any(|s| eq_ci(file.container.as_deref(), Some(s))))
        .unwrap_or(true);
    let allow_video = caps
        .supported_video_codecs
        .as_ref()
        .map(|c| {
            c.iter()
                .any(|s| eq_ci(file.video_codec.as_deref(), Some(s)))
        })
        .unwrap_or(true);
    let allow_audio = caps
        .supported_audio_codecs
        .as_ref()
        .map(|c| {
            c.iter()
                .any(|s| eq_ci(file.audio_codec.as_deref(), Some(s)))
        })
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
        (_, max) => max,
    };

    let bitrate_ok = if let Some(max) = bitrate_cap {
        file.bitrate_bps <= max || file.bitrate_bps == 0
    } else {
        true
    };
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

pub async fn stream_direct(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Query(params): Query<StreamQuery>,
    headers: HeaderMap,
    user: CurrentUser,
) -> ApiResult<impl IntoResponse> {
    let session_id = params
        .session
        .as_deref()
        .ok_or_else(|| ApiError::unauthorized("session token required"))?;
    let _ = get_session(&state, &user, session_id, Some("direct_play"), true).await?;

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
            let _ = tokio::spawn(async move {
                mark_session_error(state_clone, &session_id, Some(msg)).await
            });
            ApiError::internal(e.to_string())
        })?;
    TRANSCODE_STARTS.with_label_values(&["ok"]).inc();
    info!(
        session = %session_id,
        file = %media_file_id,
        log = %handle.log_path.to_string_lossy(),
        "transcode start or resume"
    );

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
            TRANSCODE_STARTS.with_label_values(&["error"]).inc();
            let _ = tokio::spawn(async move {
                mark_session_error(state_clone, &session_id, Some(msg)).await
            });
            return Err(err);
        }
    };
    let playlist_body = rewrite_playlist_with_token(&content, session_token);

    // Touch updated_at for TTL and store log_path/seek for debugging.
    let _ = sqlx::query::<sqlx::Any>(
        "UPDATE playback_sessions SET updated_at = CURRENT_TIMESTAMP, state = 'active', logical_position_seconds = ?, transcode_state = ? WHERE id = ?",
    )
    .bind(seek_seconds)
    .bind(
        serde_json::json!({
            "seek_seconds": seek_seconds,
            "log_path": handle.log_path.to_string_lossy(),
        })
        .to_string(),
    )
    .bind(&id)
    .execute(&state.db_pool)
    .await;

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
        return Err(ApiError::not_found("segment not found"));
    }
    let data = fs::read(&path)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;
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
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
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

fn rewrite_playlist_with_token(content: &str, token: &str) -> String {
    content
        .lines()
        .map(|line| {
            if line.starts_with('#') {
                line.to_string()
            } else if line.contains("seg_") {
                if line.contains('?') {
                    format!("{line}&session={token}")
                } else {
                    format!("{line}?session={token}")
                }
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

async fn mark_session_error(state: AppState, session_id: &str, message: Option<String>) {
    let transcode_state = message.as_deref().map(|m| {
        serde_json::json!({
            "error": m,
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
        updated_at: session.try_get("updated_at").ok(),
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

    state
        .transcodes
        .restart(session_id, &media_path, body.position_seconds)
        .await
        .map_err(|e| {
            let msg = format!("transcode restart failed: {e}");
            let session_id = id.clone();
            let state_clone = state.clone();
            let _ = tokio::spawn(async move {
                mark_session_error(state_clone, &session_id, Some(msg)).await
            });
            ApiError::internal(e.to_string())
        })?;
    TRANSCODE_STARTS.with_label_values(&["restart"]).inc();

    sqlx::query::<sqlx::Any>("UPDATE playback_sessions SET transcode_state = ?, logical_position_seconds = ?, state = 'active', updated_at = CURRENT_TIMESTAMP WHERE id = ?")
        .bind(
            serde_json::json!({
                "seek_seconds": body.position_seconds
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
