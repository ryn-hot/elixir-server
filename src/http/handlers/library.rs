use axum::{
    Json,
    extract::{Path, Query, State},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::Row;
use std::collections::HashMap;

use crate::{
    extensions::ExternalIds, http::error::ApiResult, library::run_full_scan_with_metadata_and_linkers,
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
    let rows = sqlx::query("SELECT id, title, 'movie' as type, year, CAST(runtime_seconds AS TEXT) as runtime_seconds, metadata_json, CAST(updated_at AS TEXT) as updated_at FROM movies UNION ALL SELECT id, title, library_type as type, year, CAST(NULL AS TEXT) as runtime_seconds, metadata_json, CAST(updated_at AS TEXT) as updated_at FROM series ORDER BY updated_at DESC LIMIT 200")
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
    run_full_scan_with_metadata_and_linkers(
        &state.db_pool,
        Some(&state.metadata),
        Some(&state.linkers),
        Some(&state.settings.classifier),
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
    pub tracks: Vec<MediaTrackResponse>,
    pub external_subtitles: Vec<ExternalSubtitleResponse>,
}

#[derive(Serialize)]
pub struct MediaTrackResponse {
    pub id: String,
    pub track_type: String,
    pub language: Option<String>,
    pub title: Option<String>,
    pub codec: Option<String>,
    pub channels: Option<i32>,
    pub is_default: bool,
    pub is_forced: bool,
    pub stream_index: Option<i32>,
}

#[derive(Serialize)]
pub struct ExternalSubtitleResponse {
    pub id: String,
    pub path: String,
    pub language: Option<String>,
    pub title: Option<String>,
    pub format: Option<String>,
    pub is_default: bool,
    pub is_forced: bool,
}

pub async fn detail(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<LibraryDetailResponse>> {
    let movie = sqlx::query(
        "SELECT id, title, year, external_imdb, external_tmdb, CAST(runtime_seconds AS TEXT) as runtime_seconds, metadata_json FROM movies WHERE id = ? LIMIT 1",
    )
    .bind(&id)
    .fetch_optional(&state.db_pool)
    .await
    .map_err(|e| crate::http::error::ApiError::internal(e.to_string()))?;

    let (item_type, title, year, runtime_seconds, metadata_json, external_ids) =
        if let Some(row) = movie {
        let external_ids = ExternalIds {
            imdb: row.try_get::<String, _>("external_imdb").ok(),
            tmdb: row.try_get::<String, _>("external_tmdb").ok(),
            ..Default::default()
        };
        let metadata_json: Option<Value> = row
            .try_get::<String, _>("metadata_json")
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok());
        (
            "movie".to_string(),
            row.get::<String, _>("title"),
            row.try_get::<i64, _>("year").ok().map(|v| v as i32),
            row.try_get::<String, _>("runtime_seconds")
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .map(|v| v as i32),
            metadata_json,
            external_ids,
        )
    } else {
        let series = sqlx::query(
            "SELECT id, title, year, library_type, external_imdb, external_tvdb_series, external_anilist, metadata_json FROM series WHERE id = ? LIMIT 1",
        )
        .bind(&id)
        .fetch_optional(&state.db_pool)
        .await
        .map_err(|e| crate::http::error::ApiError::internal(e.to_string()))?;

        let series = series.ok_or_else(|| crate::http::error::ApiError::not_found("item not found"))?;
        let external_tvdb: Option<String> = series.try_get("external_tvdb_series").ok();
        let external_ids = ExternalIds {
            imdb: series.try_get::<String, _>("external_imdb").ok(),
            tvdb: external_tvdb.clone(),
            tvdb_series: external_tvdb,
            anilist: series.try_get::<String, _>("external_anilist").ok(),
            ..Default::default()
        };
        let metadata_json: Option<Value> = series
            .try_get::<String, _>("metadata_json")
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok());
        (
            series.get::<String, _>("library_type"),
            series.get::<String, _>("title"),
            series.try_get::<i64, _>("year").ok().map(|v| v as i32),
            None,
            metadata_json,
            external_ids,
        )
    };

    let files = if item_type == "movie" {
        sqlx::query("SELECT mf.id, mf.path, mf.container, mf.video_codec, mf.audio_codec, mf.size_bytes, mf.scan_state, mf.source_config_id, mf.extension_metadata FROM media_files mf JOIN movie_files mlf ON mlf.media_file_id = mf.id WHERE mlf.movie_id = ?")
            .bind(&id)
            .fetch_all(&state.db_pool)
            .await
            .map_err(|e| crate::http::error::ApiError::internal(e.to_string()))?
    } else {
        sqlx::query("SELECT DISTINCT mf.id, mf.path, mf.container, mf.video_codec, mf.audio_codec, mf.size_bytes, mf.scan_state, mf.source_config_id, mf.extension_metadata FROM media_files mf JOIN episode_files ef ON ef.media_file_id = mf.id JOIN episodes e ON e.id = ef.episode_id WHERE e.series_id = ?")
            .bind(&id)
            .fetch_all(&state.db_pool)
            .await
            .map_err(|e| crate::http::error::ApiError::internal(e.to_string()))?
    };

    let file_ids: Vec<String> = files
        .iter()
        .map(|row| row.get::<String, _>("id"))
        .collect();

    let mut tracks_by_file = load_media_tracks(&state.db_pool, &file_ids).await?;
    let mut subtitles_by_file = load_external_subtitles(&state.db_pool, &file_ids).await?;

    let files = files
        .into_iter()
        .map(|row| {
            let file_id = row.get::<String, _>("id");
            let tracks = tracks_by_file.remove(&file_id).unwrap_or_default();
            let external_subtitles = subtitles_by_file.remove(&file_id).unwrap_or_default();
            LibraryFileResponse {
                id: file_id,
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
                tracks,
                external_subtitles,
            }
        })
        .collect();

    let (description, genres) = extract_metadata_fields(metadata_json.as_ref());

    let response = LibraryDetailResponse {
        id,
        title,
        r#type: item_type,
        year,
        runtime_seconds,
        external_ids,
        metadata: metadata_json,
        description,
        genres,
        files,
    };

    Ok(Json(response))
}

async fn load_media_tracks(
    pool: &sqlx::AnyPool,
    file_ids: &[String],
) -> ApiResult<HashMap<String, Vec<MediaTrackResponse>>> {
    let mut by_file: HashMap<String, Vec<MediaTrackResponse>> = HashMap::new();
    if file_ids.is_empty() {
        return Ok(by_file);
    }

    let mut builder = sqlx::QueryBuilder::<sqlx::Any>::new(
        "SELECT id, media_file_id, track_type, language, title, codec, channels, is_default, is_forced, stream_index FROM media_tracks WHERE media_file_id IN (",
    );
    let mut separated = builder.separated(", ");
    for id in file_ids {
        separated.push_bind(id);
    }
    separated.push_unseparated(") ORDER BY media_file_id, track_type, stream_index");

    let rows = builder
        .build()
        .fetch_all(pool)
        .await
        .map_err(|e| crate::http::error::ApiError::internal(e.to_string()))?;

    for row in rows {
        let media_file_id = row.get::<String, _>("media_file_id");
        let entry = by_file.entry(media_file_id).or_default();
        entry.push(MediaTrackResponse {
            id: row.get::<String, _>("id"),
            track_type: row.get::<String, _>("track_type"),
            language: row.try_get::<String, _>("language").ok(),
            title: row.try_get::<String, _>("title").ok(),
            codec: row.try_get::<String, _>("codec").ok(),
            channels: row.try_get::<i64, _>("channels").ok().map(|v| v as i32),
            is_default: row.get::<bool, _>("is_default"),
            is_forced: row.get::<bool, _>("is_forced"),
            stream_index: row.try_get::<i64, _>("stream_index").ok().map(|v| v as i32),
        });
    }

    Ok(by_file)
}

async fn load_external_subtitles(
    pool: &sqlx::AnyPool,
    file_ids: &[String],
) -> ApiResult<HashMap<String, Vec<ExternalSubtitleResponse>>> {
    let mut by_file: HashMap<String, Vec<ExternalSubtitleResponse>> = HashMap::new();
    if file_ids.is_empty() {
        return Ok(by_file);
    }

    let mut builder = sqlx::QueryBuilder::<sqlx::Any>::new(
        "SELECT id, media_file_id, path, language, title, format, is_default, is_forced FROM external_subtitles WHERE media_file_id IN (",
    );
    let mut separated = builder.separated(", ");
    for id in file_ids {
        separated.push_bind(id);
    }
    separated.push_unseparated(") ORDER BY media_file_id, path");

    let rows = builder
        .build()
        .fetch_all(pool)
        .await
        .map_err(|e| crate::http::error::ApiError::internal(e.to_string()))?;

    for row in rows {
        let media_file_id = row.get::<String, _>("media_file_id");
        let entry = by_file.entry(media_file_id).or_default();
        entry.push(ExternalSubtitleResponse {
            id: row.get::<String, _>("id"),
            path: row.get::<String, _>("path"),
            language: row.try_get::<String, _>("language").ok(),
            title: row.try_get::<String, _>("title").ok(),
            format: row.try_get::<String, _>("format").ok(),
            is_default: row.get::<bool, _>("is_default"),
            is_forced: row.get::<bool, _>("is_forced"),
        });
    }

    Ok(by_file)
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
