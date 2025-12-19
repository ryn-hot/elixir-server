use axum::{
    Json,
    extract::{Path, Query, State},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::Row;

use crate::{
    extensions::ExternalIds, http::error::ApiResult, library::run_full_scan_with_metadata,
    state::AppState,
};

#[derive(Serialize)]
pub struct LibraryItemResponse {
    pub id: String,
    pub title: String,
    pub r#type: String,
    pub year: Option<i32>,
    pub updated_at: String,
    pub runtime_seconds: Option<i32>,
    pub metadata: Option<serde_json::Value>,
}

pub async fn list_items(
    State(state): State<AppState>,
) -> ApiResult<Json<Vec<LibraryItemResponse>>> {
    let rows = sqlx::query("SELECT id, title, type, year, CAST(runtime_seconds AS TEXT) as runtime_seconds, metadata_json, datetime(updated_at) as updated_at FROM media_items ORDER BY updated_at DESC LIMIT 200")
        .fetch_all(&state.db_pool)
        .await
        .map_err(|e| crate::http::error::ApiError::internal(e.to_string()))?;

    let items = rows
        .into_iter()
        .map(|row| LibraryItemResponse {
            id: row.get::<String, _>("id"),
            title: row.get::<String, _>("title"),
            r#type: row.get::<String, _>("type"),
            year: row.try_get::<i64, _>("year").ok().map(|v| v as i32),
            updated_at: row.get::<String, _>("updated_at"),
            runtime_seconds: row
                .try_get::<String, _>("runtime_seconds")
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .map(|v| v as i32),
            metadata: row
                .try_get::<String, _>("metadata_json")
                .ok()
                .and_then(|s| serde_json::from_str(&s).ok()),
        })
        .collect();

    Ok(Json(items))
}

#[derive(Debug, Deserialize)]
pub struct ScanQuery {
    #[serde(default)]
    pub force_metadata: bool,
}

pub async fn scan(
    State(state): State<AppState>,
    Query(params): Query<ScanQuery>,
) -> ApiResult<Json<&'static str>> {
    let candidates = state
        .extensions
        .scan_all_with_db(&state.db_pool, &state.settings.library.sonarr, None)
        .await
        .map_err(|e| crate::http::error::ApiError::internal(e.to_string()))?;
    run_full_scan_with_metadata(
        &state.db_pool,
        Some(&state.metadata),
        candidates,
        params.force_metadata,
        state.settings.library.hash_dedupe_enabled,
    )
    .await
    .map_err(|e| crate::http::error::ApiError::internal(e.to_string()))?;
    Ok(Json("ok"))
}

#[derive(Serialize)]
pub struct LibraryDetailResponse {
    pub id: String,
    pub title: String,
    pub r#type: String,
    pub year: Option<i32>,
    pub runtime_seconds: Option<i32>,
    pub external_ids: ExternalIds,
    pub metadata: Option<serde_json::Value>,
    pub description: Option<String>,
    pub genres: Vec<String>,
    pub files: Vec<LibraryFileResponse>,
}

#[derive(Serialize)]
pub struct LibraryFileResponse {
    pub id: String,
    pub path: String,
    pub container: Option<String>,
    pub video_codec: Option<String>,
    pub audio_codec: Option<String>,
    pub size_bytes: Option<i64>,
    pub scan_state: String,
    pub source_config_id: Option<String>,
    pub extension_metadata: Option<serde_json::Value>,
}

pub async fn detail(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<LibraryDetailResponse>> {
    let item = sqlx::query("SELECT id, title, type, year, external_ids, CAST(runtime_seconds AS TEXT) as runtime_seconds, metadata_json FROM media_items WHERE id = ? LIMIT 1")
        .bind(&id)
        .fetch_optional(&state.db_pool)
        .await
        .map_err(|e| crate::http::error::ApiError::internal(e.to_string()))?;

    let item = item.ok_or_else(|| crate::http::error::ApiError::not_found("item not found"))?;

    let files = sqlx::query("SELECT id, path, container, video_codec, audio_codec, size_bytes, scan_state, source_config_id, extension_metadata FROM media_files WHERE media_item_id = ?")
        .bind(&id)
        .fetch_all(&state.db_pool)
        .await
        .map_err(|e| crate::http::error::ApiError::internal(e.to_string()))?;

    let files = files
        .into_iter()
        .map(|row| LibraryFileResponse {
            id: row.get::<String, _>("id"),
            path: row.get::<String, _>("path"),
            container: row.try_get::<String, _>("container").ok(),
            video_codec: row.try_get::<String, _>("video_codec").ok(),
            audio_codec: row.try_get::<String, _>("audio_codec").ok(),
            size_bytes: row.try_get::<i64, _>("size_bytes").ok(),
            scan_state: row.get::<String, _>("scan_state"),
            source_config_id: row.try_get("source_config_id").ok(),
            extension_metadata: row
                .try_get::<String, _>("extension_metadata")
                .ok()
                .and_then(|s| serde_json::from_str(&s).ok()),
        })
        .collect();

    let metadata_json: Option<Value> = item
        .try_get::<String, _>("metadata_json")
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok());
    let (description, genres) = extract_metadata_fields(metadata_json.as_ref());

    let response = LibraryDetailResponse {
        id: item.get::<String, _>("id"),
        title: item.get::<String, _>("title"),
        r#type: item.get::<String, _>("type"),
        year: item.try_get::<i64, _>("year").ok().map(|v| v as i32),
        runtime_seconds: item
            .try_get::<String, _>("runtime_seconds")
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .map(|v| v as i32),
        external_ids: item
            .try_get::<String, _>("external_ids")
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default(),
        metadata: metadata_json,
        description,
        genres,
        files,
    };

    Ok(Json(response))
}

fn extract_metadata_fields(meta: Option<&Value>) -> (Option<String>, Vec<String>) {
    let mut description = None;
    let mut genres = Vec::new();

    if let Some(value) = meta {
        let desc_candidate = value
            .get("description")
            .and_then(Value::as_str)
            .or_else(|| value.get("overview").and_then(Value::as_str))
            .or_else(|| value.get("plot").and_then(Value::as_str))
            .or_else(|| value.get("summary").and_then(Value::as_str));

        if let Some(desc) = desc_candidate {
            let cleaned = clean_description(desc);
            if !cleaned.is_empty() {
                description = Some(cleaned);
            }
        }

        if let Some(arr) = value.get("genres").and_then(Value::as_array) {
            for g in arr {
                if let Some(s) = g.as_str() {
                    genres.push(s.to_string());
                } else if let Some(s) = g
                    .as_object()
                    .and_then(|o| o.get("name"))
                    .and_then(Value::as_str)
                {
                    genres.push(s.to_string());
                }
            }
        }

        if genres.is_empty() {
            if let Some(single) = value.get("genre").and_then(Value::as_str) {
                genres.push(single.to_string());
            }
        }
    }

    (description, genres)
}

fn clean_description(raw: &str) -> String {
    let normalized = raw
        .replace("<br>", "\n")
        .replace("<br />", "\n")
        .replace("<br/>", "\n");
    let mut cleaned = String::with_capacity(normalized.len());
    let mut in_tag = false;
    for ch in normalized.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => cleaned.push(ch),
            _ => {}
        }
    }

    let decoded = html_escape::decode_html_entities(&cleaned);
    decoded.split_whitespace().collect::<Vec<_>>().join(" ")
}
