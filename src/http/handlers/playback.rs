use std::{cmp::Ordering, path::Path};

use axum::{
    Json,
    body::Body,
    extract::{Path as AxumPath, State},
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, RANGE},
    },
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::Row;
use uuid::Uuid;

use crate::{
    db::models::{MediaType, PlaybackMode},
    http::{
        auth::CurrentUser,
        error::{ApiError, ApiResult},
    },
    playback::TranscodeParams,
    state::AppState,
};
use tokio::{
    fs::{self, File},
    io::{AsyncReadExt, AsyncSeekExt, SeekFrom},
};
use tokio_util::io::ReaderStream;

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

    let selected = select_file(&files, body.preferred_file_id.as_deref())
        .ok_or_else(|| ApiError::not_found("requested file not found"))?;

    let caps_json = body.client_capabilities.clone();
    let caps = caps_json
        .clone()
        .and_then(|v| serde_json::from_value::<ClientCapabilities>(v).ok())
        .unwrap_or_else(default_capabilities);
    let decision = decide_playback(selected, &caps, body.network_type.as_deref());
    let session_id = Uuid::new_v4();

    let stream_url = match decision.mode {
        PlaybackMode::DirectPlay => {
            format!("/stream/direct/{}?session={}", selected.id, session_id)
        }
        PlaybackMode::Transcode => format!("/sessions/{}/master.m3u8", session_id),
    };

    let transcode_state = match decision.mode {
        PlaybackMode::Transcode => Some(serde_json::json!({
            "seek_seconds": 0.0,
        })),
        _ => None,
    };

    sqlx::query::<sqlx::Any>("INSERT INTO playback_sessions (id, user_id, media_file_id, mode, network_type, client_capabilities, transcode_state) VALUES (?, ?, ?, ?, ?, ?, ?)")
        .bind(session_id.to_string())
        .bind(user.user_id.to_string())
        .bind(&selected.id)
        .bind(decision.mode_as_str())
        .bind(body.network_type.clone())
        .bind(caps_json.as_ref().map(|v| v.to_string()))
        .bind(transcode_state.as_ref().map(|s| s.to_string()))
        .execute(&state.db_pool)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;

    let response = PlayResponse {
        session_id: session_id.to_string(),
        mode: decision.mode_as_str(),
        stream_url,
        duration_seconds: item.runtime_seconds,
        logical_start_seconds: 0,
        media_file_id: selected.id.clone(),
    };

    Ok(Json(response))
}

fn select_file<'a>(files: &'a [FileRow], preferred: Option<&str>) -> Option<&'a FileRow> {
    if let Some(pref) = preferred {
        if let Some(f) = files.iter().find(|f| f.id == pref) {
            return Some(f);
        }
    }

    files.iter().max_by(|a, b| compare_resolution(a, b))
}

fn compare_resolution(a: &FileRow, b: &FileRow) -> Ordering {
    let score = |f: &FileRow| -> i64 {
        let w = f.width as i64;
        let h = f.height as i64;
        if w == 0 || h == 0 { 0 } else { w * h }
    };
    score(a).cmp(&score(b))
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
}

impl PlaybackDecision {
    fn mode_as_str(&self) -> &'static str {
        match self.mode {
            PlaybackMode::DirectPlay => "direct_play",
            PlaybackMode::Transcode => "transcode",
        }
    }
}

fn default_capabilities() -> ClientCapabilities {
    ClientCapabilities {
        max_resolution: Some("1080p".to_string()),
        supported_containers: Some(vec!["mkv".to_string(), "mp4".to_string()]),
        supported_video_codecs: Some(vec!["h264".to_string(), "hevc".to_string()]),
        supported_audio_codecs: Some(vec![
            "aac".to_string(),
            "ac3".to_string(),
            "opus".to_string(),
        ]),
        max_bitrate_bps: None,
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
        Some("4k") | Some("2160p") => file.height <= 2160,
        _ => true,
    };

    let bitrate_ok = if let Some(max) = caps.max_bitrate_bps {
        if matches!(network_type, Some("lan")) {
            true
        } else {
            file.bitrate_bps <= max || file.bitrate_bps == 0
        }
    } else {
        true
    };

    if allow_container && allow_video && allow_audio && res_ok && bitrate_ok {
        PlaybackDecision {
            mode: PlaybackMode::DirectPlay,
        }
    } else {
        PlaybackDecision {
            mode: PlaybackMode::Transcode,
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
    headers: HeaderMap,
    _user: CurrentUser,
) -> ApiResult<impl IntoResponse> {
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
    _user: CurrentUser,
) -> ApiResult<impl IntoResponse> {
    let session_id = Uuid::parse_str(&id).map_err(|_| ApiError::bad_request("invalid session"))?;
    let session_row = sqlx::query::<sqlx::Any>(
        "SELECT media_file_id, transcode_state FROM playback_sessions WHERE id = ? AND mode = 'transcode' LIMIT 1",
    )
    .bind(&id)
    .fetch_optional(&state.db_pool)
    .await
    .map_err(|e| ApiError::internal(e.to_string()))?;

    let session_row = session_row.ok_or_else(|| ApiError::not_found("session not found"))?;
    let media_file_id: String = session_row.get("media_file_id");
    let seek_seconds = session_row
        .try_get::<String, _>("transcode_state")
        .ok()
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

    let playlist_path = state
        .transcodes
        .start_or_get(session_id, &media_path, TranscodeParams { seek_seconds })
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;

    // Wait briefly for playlist to appear; fallback to not found if still missing.
    let content = fs::read_to_string(&playlist_path)
        .await
        .map_err(|_| ApiError::internal("playlist not ready"))?;

    Ok((
        StatusCode::OK,
        [(
            CONTENT_TYPE,
            HeaderValue::from_static("application/vnd.apple.mpegurl"),
        )],
        content,
    ))
}

pub async fn serve_segment(
    State(state): State<AppState>,
    AxumPath((id, segment)): AxumPath<(String, String)>,
    _user: CurrentUser,
) -> ApiResult<impl IntoResponse> {
    let session_id = Uuid::parse_str(&id).map_err(|_| ApiError::bad_request("invalid session"))?;
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

pub async fn seek_transcode(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    _user: CurrentUser,
    Json(body): Json<SeekRequest>,
) -> ApiResult<Json<&'static str>> {
    let session_id = Uuid::parse_str(&id).map_err(|_| ApiError::bad_request("invalid session"))?;
    let session_row = sqlx::query::<sqlx::Any>(
        "SELECT media_file_id FROM playback_sessions WHERE id = ? AND mode = 'transcode' LIMIT 1",
    )
    .bind(&id)
    .fetch_optional(&state.db_pool)
    .await
    .map_err(|e| ApiError::internal(e.to_string()))?;
    let session_row = session_row.ok_or_else(|| ApiError::not_found("session not found"))?;
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
        .map_err(|e| ApiError::internal(e.to_string()))?;

    sqlx::query::<sqlx::Any>("UPDATE playback_sessions SET transcode_state = ? WHERE id = ?")
        .bind(
            serde_json::json!({
                "seek_seconds": body.position_seconds
            })
            .to_string(),
        )
        .bind(&id)
        .execute(&state.db_pool)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;

    Ok(Json("ok"))
}
