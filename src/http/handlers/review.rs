use axum::{
    Json,
    extract::{Path, Query, State},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::Row;
use uuid::Uuid;

use crate::{
    extensions::ExternalIds,
    http::{
        auth::CurrentUser,
        error::{ApiError, ApiResult},
    },
    library::{
        apply_external_ids_to_movie, apply_external_ids_to_season, apply_external_ids_to_series,
        derive_override_key, normalize_override_key,
    },
    state::AppState,
};

#[derive(Debug, Deserialize)]
pub struct ReviewQueueQuery {
    pub status: Option<String>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct ReviewQueueItem {
    pub id: String,
    pub media_file_id: String,
    pub status: String,
    pub confidence: Option<f32>,
    pub path: Option<String>,
    pub scan_state: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ReviewQueueDetail {
    pub id: String,
    pub media_file_id: String,
    pub status: String,
    pub confidence: Option<f32>,
    pub hint: Option<Value>,
    pub candidates: Option<Value>,
    pub file: Option<ReviewFileInfo>,
    pub current_match: Option<ReviewMatchInfo>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ReviewFileInfo {
    pub id: String,
    pub path: String,
    pub scan_state: String,
    pub size_bytes: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct ReviewMatchInfo {
    pub id: String,
    pub kind: String,
    pub title: String,
    pub year: Option<i32>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum IdValue {
    String(String),
    Number(i64),
}

impl IdValue {
    fn as_string(&self) -> String {
        match self {
            IdValue::String(value) => value.clone(),
            IdValue::Number(value) => value.to_string(),
        }
    }
}

#[derive(Debug, Deserialize, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ExternalIdsInput {
    pub imdb: Option<IdValue>,
    pub tmdb: Option<IdValue>,
    pub tvdb: Option<IdValue>,
    pub tvdb_series: Option<IdValue>,
    pub tvdb_movie: Option<IdValue>,
    pub anilist: Option<IdValue>,
}

impl ExternalIdsInput {
    fn into_external_ids(self) -> ExternalIds {
        ExternalIds {
            imdb: self.imdb.map(|v| v.as_string()),
            tmdb: self.tmdb.map(|v| v.as_string()),
            tvdb: self.tvdb.map(|v| v.as_string()),
            tvdb_series: self.tvdb_series.map(|v| v.as_string()),
            tvdb_movie: self.tvdb_movie.map(|v| v.as_string()),
            anilist: self.anilist.map(|v| v.as_string()),
            ..Default::default()
        }
    }

    fn has_any(&self) -> bool {
        self.imdb.is_some()
            || self.tmdb.is_some()
            || self.tvdb.is_some()
            || self.tvdb_series.is_some()
            || self.tvdb_movie.is_some()
            || self.anilist.is_some()
    }
}

#[derive(Debug, Deserialize)]
pub struct ApplyReviewRequest {
    pub library_type: String,
    #[serde(default)]
    pub normalized_key: Option<String>,
    #[serde(default)]
    pub external_ids: ExternalIdsInput,
}

#[derive(Debug, Deserialize)]
pub struct OverrideRequest {
    pub library_type: String,
    pub normalized_key: String,
    pub imdb_id: Option<IdValue>,
    pub anilist_id: Option<IdValue>,
    pub tvdb_id: Option<IdValue>,
}

pub async fn list_queue(
    State(state): State<AppState>,
    _user: CurrentUser,
    Query(params): Query<ReviewQueueQuery>,
) -> ApiResult<Json<Vec<ReviewQueueItem>>> {
    let limit = params.limit.unwrap_or(100).min(200) as i64;
    let offset = params.offset.unwrap_or(0) as i64;

    let mut builder = sqlx::QueryBuilder::<sqlx::Any>::new(
        "SELECT rq.id, rq.media_file_id, rq.status, rq.confidence, CAST(rq.created_at AS TEXT) as created_at, CAST(rq.updated_at AS TEXT) as updated_at, mf.path, mf.scan_state FROM review_queue rq LEFT JOIN media_files mf ON mf.id = rq.media_file_id",
    );
    if let Some(status) = params.status.as_ref() {
        builder.push(" WHERE rq.status = ");
        builder.push_bind(status);
    }
    builder.push(" ORDER BY rq.updated_at DESC LIMIT ");
    builder.push_bind(limit);
    builder.push(" OFFSET ");
    builder.push_bind(offset);

    let rows = builder
        .build()
        .fetch_all(&state.db_pool)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;

    let items = rows
        .into_iter()
        .map(|row| ReviewQueueItem {
            id: row.get::<String, _>("id"),
            media_file_id: row.get::<String, _>("media_file_id"),
            status: row.get::<String, _>("status"),
            confidence: row.try_get::<f64, _>("confidence").ok().map(|v| v as f32),
            path: row.try_get::<String, _>("path").ok(),
            scan_state: row.try_get::<String, _>("scan_state").ok(),
            created_at: row.try_get::<String, _>("created_at").ok(),
            updated_at: row.try_get::<String, _>("updated_at").ok(),
        })
        .collect();

    Ok(Json(items))
}

pub async fn queue_detail(
    State(state): State<AppState>,
    _user: CurrentUser,
    Path(id): Path<String>,
) -> ApiResult<Json<ReviewQueueDetail>> {
    let row = sqlx::query(
        "SELECT rq.id, rq.media_file_id, rq.status, rq.confidence, rq.hint_json, rq.candidates_json, CAST(rq.created_at AS TEXT) as created_at, CAST(rq.updated_at AS TEXT) as updated_at, mf.path, mf.scan_state, mf.size_bytes FROM review_queue rq LEFT JOIN media_files mf ON mf.id = rq.media_file_id WHERE rq.id = ? LIMIT 1",
    )
    .bind(&id)
    .fetch_optional(&state.db_pool)
    .await
    .map_err(|e| ApiError::internal(e.to_string()))?;

    let row = row.ok_or_else(|| ApiError::not_found("review entry not found"))?;
    let media_file_id = row.get::<String, _>("media_file_id");
    let hint = row
        .try_get::<String, _>("hint_json")
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok());
    let candidates = row
        .try_get::<String, _>("candidates_json")
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok());

    let file = row
        .try_get::<String, _>("path")
        .ok()
        .map(|path| ReviewFileInfo {
            id: media_file_id.clone(),
            path,
            scan_state: row
                .try_get::<String, _>("scan_state")
                .unwrap_or_else(|_| "unknown".to_string()),
            size_bytes: row.try_get::<i64, _>("size_bytes").ok(),
        });

    let current_match = load_current_match(&state.db_pool, &media_file_id).await?;

    Ok(Json(ReviewQueueDetail {
        id,
        media_file_id,
        status: row.get::<String, _>("status"),
        confidence: row.try_get::<f64, _>("confidence").ok().map(|v| v as f32),
        hint,
        candidates,
        file,
        current_match,
        created_at: row.try_get::<String, _>("created_at").ok(),
        updated_at: row.try_get::<String, _>("updated_at").ok(),
    }))
}

pub async fn apply_review(
    State(state): State<AppState>,
    _user: CurrentUser,
    Path(id): Path<String>,
    Json(body): Json<ApplyReviewRequest>,
) -> ApiResult<Json<&'static str>> {
    let ApplyReviewRequest {
        library_type,
        normalized_key,
        external_ids,
    } = body;
    let library_type = normalize_library_type(&library_type)
        .ok_or_else(|| ApiError::bad_request("invalid library_type"))?;
    if !external_ids.has_any() {
        return Err(ApiError::bad_request(
            "external_ids must include at least one id",
        ));
    }

    let row = sqlx::query("SELECT media_file_id FROM review_queue WHERE id = ? LIMIT 1")
        .bind(&id)
        .fetch_optional(&state.db_pool)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;
    let row = row.ok_or_else(|| ApiError::not_found("review entry not found"))?;
    let media_file_id: String = row.get("media_file_id");
    let media_path: String = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT path FROM media_files WHERE id = ? LIMIT 1",
    )
    .bind(&media_file_id)
    .fetch_optional(&state.db_pool)
    .await
    .map_err(|e| ApiError::internal(e.to_string()))?
    .ok_or_else(|| ApiError::not_found("media file not found"))?;

    let target = resolve_review_target(&state.db_pool, &media_file_id).await?;
    let external_ids = external_ids.into_external_ids();

    match target {
        ReviewTarget::Movie { id: movie_id } => {
            if library_type != "movie" {
                return Err(ApiError::bad_request(
                    "library_type does not match current media item",
                ));
            }
            apply_external_ids_to_movie(&state.db_pool, movie_id, &external_ids, "override")
                .await
                .map_err(|e| ApiError::internal(e.to_string()))?;
        }
        ReviewTarget::Series {
            id: series_id,
            season_id,
            library_type: series_type,
        } => {
            if library_type != series_type {
                return Err(ApiError::bad_request(
                    "library_type does not match current media item",
                ));
            }
            if library_type == "anime" {
                apply_external_ids_to_season(
                    &state.db_pool,
                    season_id,
                    &external_ids,
                    "override",
                    Some(1.0),
                )
                .await
                .map_err(|e| ApiError::internal(e.to_string()))?;
            } else {
                apply_external_ids_to_series(&state.db_pool, series_id, &external_ids, "override")
                    .await
                    .map_err(|e| ApiError::internal(e.to_string()))?;
            }
        }
    }

    let normalized_key = if let Some(raw) = normalized_key.as_deref() {
        normalize_override_key(raw)
    } else {
        derive_override_key(library_type, &media_path)
    }
    .ok_or_else(|| ApiError::bad_request("unable to derive normalized_key"))?;

    let override_ids = OverrideIds::from_external_ids(&external_ids);
    upsert_override(&state.db_pool, library_type, &normalized_key, &override_ids).await?;

    sqlx::query::<sqlx::Any>(
        "UPDATE review_queue SET status = 'applied', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
    )
    .bind(&id)
    .execute(&state.db_pool)
    .await
    .map_err(|e| ApiError::internal(e.to_string()))?;

    Ok(Json("ok"))
}

pub async fn set_override(
    State(state): State<AppState>,
    _user: CurrentUser,
    Json(body): Json<OverrideRequest>,
) -> ApiResult<Json<&'static str>> {
    let library_type = normalize_library_type(&body.library_type)
        .ok_or_else(|| ApiError::bad_request("invalid library_type"))?;
    let normalized_key = normalize_override_key(&body.normalized_key)
        .ok_or_else(|| ApiError::bad_request("normalized_key is required"))?;

    let override_ids = OverrideIds {
        imdb_id: body.imdb_id.map(|v| v.as_string()),
        anilist_id: body.anilist_id.map(|v| v.as_string()),
        tvdb_id: body.tvdb_id.map(|v| v.as_string()),
    };
    if override_ids.is_empty() {
        return Err(ApiError::bad_request(
            "override must include at least one id",
        ));
    }

    upsert_override(&state.db_pool, library_type, &normalized_key, &override_ids).await?;

    Ok(Json("ok"))
}

#[derive(Debug)]
struct OverrideIds {
    imdb_id: Option<String>,
    anilist_id: Option<String>,
    tvdb_id: Option<String>,
}

impl OverrideIds {
    fn is_empty(&self) -> bool {
        self.imdb_id.is_none() && self.anilist_id.is_none() && self.tvdb_id.is_none()
    }

    fn from_external_ids(ids: &ExternalIds) -> Self {
        let tvdb = ids.tvdb_series.as_ref().or(ids.tvdb.as_ref()).cloned();
        Self {
            imdb_id: ids.imdb.clone(),
            anilist_id: ids.anilist.clone(),
            tvdb_id: tvdb,
        }
    }
}

async fn upsert_override(
    pool: &sqlx::AnyPool,
    library_type: &str,
    normalized_key: &str,
    ids: &OverrideIds,
) -> ApiResult<()> {
    sqlx::query::<sqlx::Any>(
        "INSERT INTO classifier_overrides (id, library_type, normalized_key, imdb_id, anilist_id, tvdb_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ON CONFLICT(library_type, normalized_key) DO UPDATE SET imdb_id = excluded.imdb_id, anilist_id = excluded.anilist_id, tvdb_id = excluded.tvdb_id, updated_at = CURRENT_TIMESTAMP",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(library_type)
    .bind(normalized_key)
    .bind(ids.imdb_id.as_ref())
    .bind(ids.anilist_id.as_ref())
    .bind(ids.tvdb_id.as_ref())
    .execute(pool)
    .await
    .map_err(|e| ApiError::internal(e.to_string()))?;
    Ok(())
}

async fn load_current_match(
    pool: &sqlx::AnyPool,
    media_file_id: &str,
) -> ApiResult<Option<ReviewMatchInfo>> {
    if let Some(row) = sqlx::query(
        "SELECT m.id, m.title, m.year FROM movies m JOIN movie_files mf ON mf.movie_id = m.id WHERE mf.media_file_id = ? LIMIT 1",
    )
    .bind(media_file_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| ApiError::internal(e.to_string()))?
    {
        return Ok(Some(ReviewMatchInfo {
            id: row.get::<String, _>("id"),
            kind: "movie".to_string(),
            title: row.get::<String, _>("title"),
            year: row.try_get::<i64, _>("year").ok().map(|v| v as i32),
        }));
    }

    if let Some(row) = sqlx::query(
        "SELECT s.id, s.title, s.year, s.library_type FROM series s JOIN episodes e ON e.series_id = s.id JOIN episode_files ef ON ef.episode_id = e.id WHERE ef.media_file_id = ? LIMIT 1",
    )
    .bind(media_file_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| ApiError::internal(e.to_string()))?
    {
        return Ok(Some(ReviewMatchInfo {
            id: row.get::<String, _>("id"),
            kind: row.get::<String, _>("library_type"),
            title: row.get::<String, _>("title"),
            year: row.try_get::<i64, _>("year").ok().map(|v| v as i32),
        }));
    }

    Ok(None)
}

enum ReviewTarget {
    Movie {
        id: Uuid,
    },
    Series {
        id: Uuid,
        season_id: Uuid,
        library_type: String,
    },
}

async fn resolve_review_target(
    pool: &sqlx::AnyPool,
    media_file_id: &str,
) -> ApiResult<ReviewTarget> {
    if let Some(movie_id) = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT movie_id FROM movie_files WHERE media_file_id = ? LIMIT 1",
    )
    .bind(media_file_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| ApiError::internal(e.to_string()))?
    {
        let movie_uuid =
            Uuid::parse_str(&movie_id).map_err(|_| ApiError::bad_request("invalid movie_id"))?;
        return Ok(ReviewTarget::Movie { id: movie_uuid });
    }

    if let Some(row) = sqlx::query(
        "SELECT s.id, s.library_type, e.season_id FROM series s JOIN episodes e ON e.series_id = s.id JOIN episode_files ef ON ef.episode_id = e.id WHERE ef.media_file_id = ? LIMIT 1",
    )
    .bind(media_file_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| ApiError::internal(e.to_string()))?
    {
        let series_id = row.get::<String, _>("id");
        let series_uuid = Uuid::parse_str(&series_id)
            .map_err(|_| ApiError::bad_request("invalid series_id"))?;
        let season_id = row.get::<String, _>("season_id");
        let season_uuid = Uuid::parse_str(&season_id)
            .map_err(|_| ApiError::bad_request("invalid season_id"))?;
        return Ok(ReviewTarget::Series {
            id: series_uuid,
            season_id: season_uuid,
            library_type: row.get::<String, _>("library_type"),
        });
    }

    Err(ApiError::not_found(
        "media file is not linked to a library item",
    ))
}

fn normalize_library_type(input: &str) -> Option<&'static str> {
    match input.trim().to_ascii_lowercase().as_str() {
        "movie" => Some("movie"),
        "series" | "tv" | "show" => Some("series"),
        "anime" => Some("anime"),
        _ => None,
    }
}
