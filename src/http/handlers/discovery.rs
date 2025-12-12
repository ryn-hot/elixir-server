use axum::{
    Json,
    extract::{Query, State},
};
use serde::Deserialize;

use crate::{
    db::models::MediaType,
    http::{
        auth::CurrentUser,
        error::{ApiError, ApiResult},
    },
    metadata::DiscoveryResult,
    state::AppState,
};

#[derive(Debug, Deserialize)]
pub struct DiscoveryQuery {
    pub q: Option<String>,
    pub r#type: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SuggestQuery {
    pub q: Option<String>,
    pub r#type: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_limit() -> usize {
    5
}

pub async fn search(
    State(state): State<AppState>,
    _user: CurrentUser,
    Query(params): Query<DiscoveryQuery>,
) -> ApiResult<Json<Vec<DiscoveryResult>>> {
    let query = params
        .q
        .as_deref()
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| ApiError::bad_request("q is required"))?;

    let media_type = params
        .r#type
        .as_deref()
        .and_then(|t| match t.to_ascii_lowercase().as_str() {
            "movie" => Some(MediaType::Movie),
            "series" => Some(MediaType::Series),
            "anime" => Some(MediaType::Anime),
            _ => None,
        });

    let results = state
        .metadata
        .discovery_search(query, media_type)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;

    Ok(Json(results))
}

pub async fn suggest(
    State(state): State<AppState>,
    _user: CurrentUser,
    Query(params): Query<SuggestQuery>,
) -> ApiResult<Json<Vec<DiscoveryResult>>> {
    let query = params
        .q
        .as_deref()
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| ApiError::bad_request("q is required"))?;

    let media_type = params
        .r#type
        .as_deref()
        .and_then(|t| match t.to_ascii_lowercase().as_str() {
            "movie" => Some(MediaType::Movie),
            "series" => Some(MediaType::Series),
            "anime" => Some(MediaType::Anime),
            _ => None,
        });

    let mut results = state
        .metadata
        .discovery_search(query, media_type)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;

    if results.len() > params.limit {
        results.truncate(params.limit);
    }

    Ok(Json(results))
}
