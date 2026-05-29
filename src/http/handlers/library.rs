use axum::{
    Json,
    extract::{Path, Query, State},
    http::HeaderMap,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::Row;
use std::{
    collections::{HashMap, HashSet},
    io::ErrorKind,
};
use tokio::fs;
use uuid::Uuid;

use crate::{
    acquisition::episode_state::{
        LibraryEpisodeAcquisitionProjection, LibraryEpisodeAcquisitionState,
        list_library_episode_acquisition_projections,
        rebuild_library_episode_acquisition_states_for_subscription,
    },
    db::models::MediaType,
    extensions::{
        ExternalIds, MediaIdentity,
        store::{
            ExtensionStore, ManagedEpisodeTombstone, ManagedLibraryProvenance,
            MediaOwnerReleaseEvent, MediaOwnership, MediaOwnershipReconcileReport,
            NewManagedEpisodeTombstone, NewManagedLibraryProvenance, NewManagedMediaTombstone,
            NewMediaOwnerReleaseEvent,
        },
    },
    http::error::{ApiError, ApiResult},
    library::{
        ensure_series_episode_catalog_from_local_metadata,
        managed_episode_tombstone_matches_series, match_managed_episode_tombstone,
        match_managed_ingest_intent, normalize_managed_intent_title,
        run_full_scan_with_metadata_and_linkers,
    },
    media::owner_release::{
        MediaOwnerReleaseRequest, OwnerReleaseAction, OwnerReleaseEpisodeScope,
        OwnerReleaseSummary, dispatch_media_owner_release,
    },
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
    pub description: Option<String>,
    pub genres: Vec<String>,
    pub poster_url: Option<String>,
    pub banner_url: Option<String>,
    pub backdrop_url: Option<String>,
    pub lifecycle: LibraryItemCardLifecycleResponse,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct LibraryItemCardLifecycleResponse {
    pub tracked_by_manager: bool,
    pub manager_label: Option<String>,
    pub can_stop_tracking: bool,
    pub owner_release_supported: bool,
    pub owner_release_label: String,
    pub owner_release_status: Option<String>,
    pub owner_release_status_reason: Option<String>,
    pub primary_owner: Option<LibraryOwnerResponse>,
    pub owners: Vec<LibraryOwnerResponse>,
}

pub async fn list_items(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> ApiResult<Json<Vec<LibraryItemResponse>>> {
    let preferred_languages = parse_language_header(&headers);
    let rows = sqlx::query("SELECT id, title, 'movie' as type, year, CAST(runtime_seconds AS TEXT) as runtime_seconds, metadata_json, CAST(updated_at AS TEXT) as updated_at FROM movies UNION ALL SELECT id, title, library_type as type, year, CAST(NULL AS TEXT) as runtime_seconds, metadata_json, CAST(updated_at AS TEXT) as updated_at FROM series ORDER BY updated_at DESC LIMIT 200")
        .fetch_all(&state.db_pool)
        .await
        .map_err(|e| crate::http::error::ApiError::internal(e.to_string()))?;

    let mut movie_ids = Vec::new();
    let mut series_ids = Vec::new();
    let mut anime_ids = Vec::new();
    for row in &rows {
        let id = row.get::<String, _>("id");
        let item_type = row.get::<String, _>("type");
        match item_type.as_str() {
            "movie" => movie_ids.push(id),
            "anime" => anime_ids.push(id),
            _ => series_ids.push(id),
        }
    }

    let movie_posters = load_primary_artwork(
        &state.db_pool,
        "movie",
        &movie_ids,
        "poster",
        &preferred_languages,
        &["tvdb", "cinemeta"],
    )
    .await?;
    let movie_backdrops = load_primary_artwork(
        &state.db_pool,
        "movie",
        &movie_ids,
        "backdrop",
        &preferred_languages,
        &["tvdb", "cinemeta"],
    )
    .await?;
    let series_posters = load_primary_artwork(
        &state.db_pool,
        "series",
        &series_ids,
        "poster",
        &preferred_languages,
        &["tvdb", "cinemeta"],
    )
    .await?;
    let series_banners = load_primary_artwork(
        &state.db_pool,
        "series",
        &series_ids,
        "banner",
        &preferred_languages,
        &["tvdb"],
    )
    .await?;
    let series_backdrops = load_primary_artwork(
        &state.db_pool,
        "series",
        &series_ids,
        "backdrop",
        &preferred_languages,
        &["tvdb", "cinemeta"],
    )
    .await?;
    let anime_posters = load_primary_artwork(
        &state.db_pool,
        "series",
        &anime_ids,
        "poster",
        &preferred_languages,
        &["anilist", "tvdb", "cinemeta"],
    )
    .await?;
    let anime_banners = load_primary_artwork(
        &state.db_pool,
        "series",
        &anime_ids,
        "banner",
        &preferred_languages,
        &["anilist", "tvdb"],
    )
    .await?;
    let anime_backdrops = load_primary_artwork(
        &state.db_pool,
        "series",
        &anime_ids,
        "backdrop",
        &preferred_languages,
        &["anilist", "tvdb", "cinemeta"],
    )
    .await?;

    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        let id = row.get::<String, _>("id");
        let item_type = row.get::<String, _>("type");
        let lifecycle = resolve_library_item_card_lifecycle(&state, &id).await?;
        let metadata = row
            .try_get::<String, _>("metadata_json")
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok());
        let (description, genres) =
            extract_metadata_fields(metadata.as_ref(), &preferred_languages);
        items.push(LibraryItemResponse {
            id: id.clone(),
            title: row.get::<String, _>("title"),
            r#type: item_type.clone(),
            year: row.try_get::<i64, _>("year").ok().map(|v| v as i32),
            updated_at: row.get::<String, _>("updated_at"),
            runtime_seconds: row
                .try_get::<String, _>("runtime_seconds")
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .map(|v| v as i32),
            metadata,
            description,
            genres,
            poster_url: match item_type.as_str() {
                "movie" => movie_posters.get(&id).cloned(),
                "anime" => anime_posters.get(&id).cloned(),
                _ => series_posters.get(&id).cloned(),
            },
            banner_url: match item_type.as_str() {
                "anime" => anime_banners.get(&id).cloned(),
                "movie" => None,
                _ => series_banners.get(&id).cloned(),
            },
            backdrop_url: match item_type.as_str() {
                "movie" => movie_backdrops.get(&id).cloned(),
                "anime" => anime_backdrops.get(&id).cloned(),
                _ => series_backdrops.get(&id).cloned(),
            },
            lifecycle,
        });
    }

    Ok(Json(items))
}

#[derive(Debug, Deserialize)]
pub struct ScanQuery {
    #[serde(default)]
    pub force_metadata: bool,
    #[serde(default)]
    pub force_reclassify: bool,
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
        Some(&state.artwork),
        candidates,
        params.force_metadata,
        params.force_reclassify,
        state.settings.library.hash_dedupe_enabled,
    )
    .await
    .map_err(|e| crate::http::error::ApiError::internal(e.to_string()))?;
    Ok(Json("ok"))
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct OwnerReleaseEventsQuery {
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OwnerReleaseEventResponse {
    pub release_event_id: String,
    pub media_item_id: Option<String>,
    pub ownership_id: Option<String>,
    pub requested_action: String,
    pub owner_type: String,
    pub owner_label: Option<String>,
    pub owner_provider_id: Option<String>,
    pub acquisition_subscription_id: Option<String>,
    pub status: String,
    pub status_reason: Option<String>,
    pub request: Option<Value>,
    pub response: Option<Value>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OwnerReleaseEventsResponse {
    pub media_item_id: Option<String>,
    pub limit: i64,
    pub total: usize,
    pub events: Vec<OwnerReleaseEventResponse>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OwnerReleaseReconcileResponse {
    pub status: &'static str,
    pub report: MediaOwnershipReconcileReport,
}

pub async fn list_owner_release_events(
    State(state): State<AppState>,
    Query(query): Query<OwnerReleaseEventsQuery>,
) -> ApiResult<Json<OwnerReleaseEventsResponse>> {
    let limit = owner_release_events_limit(query.limit);
    let store = ExtensionStore::new(&state.db_pool);
    let events = store
        .list_media_owner_release_events(limit)
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?;
    Ok(Json(OwnerReleaseEventsResponse {
        media_item_id: None,
        limit,
        total: events.len(),
        events: events
            .iter()
            .map(owner_release_event_response)
            .collect::<Vec<_>>(),
    }))
}

pub async fn list_item_owner_release_events(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<OwnerReleaseEventsQuery>,
) -> ApiResult<Json<OwnerReleaseEventsResponse>> {
    let media_item_id =
        Uuid::parse_str(&id).map_err(|_| ApiError::bad_request("library item id is invalid"))?;
    let limit = owner_release_events_limit(query.limit);
    let store = ExtensionStore::new(&state.db_pool);
    let mut events = store
        .list_media_owner_release_events_for_item(media_item_id)
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?;
    events.truncate(limit as usize);
    Ok(Json(OwnerReleaseEventsResponse {
        media_item_id: Some(media_item_id.to_string()),
        limit,
        total: events.len(),
        events: events
            .iter()
            .map(owner_release_event_response)
            .collect::<Vec<_>>(),
    }))
}

pub async fn reconcile_owner_release_state(
    State(state): State<AppState>,
) -> ApiResult<Json<OwnerReleaseReconcileResponse>> {
    let store = ExtensionStore::new(&state.db_pool);
    let report = store
        .reconcile_media_ownerships()
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?;
    tracing::info!(
        external_owners_created = report.external_owners_created,
        stale_owners_marked_unsupported = report.stale_owners_marked_unsupported,
        unsupported_events_created = report.unsupported_events_created,
        "media ownership reconciliation completed"
    );
    Ok(Json(OwnerReleaseReconcileResponse {
        status: "ok",
        report,
    }))
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
    pub poster_url: Option<String>,
    pub banner_url: Option<String>,
    pub backdrop_url: Option<String>,
    pub lifecycle: LibraryLifecycleResponse,
    pub files: Vec<LibraryFileResponse>,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct LibraryLifecycleResponse {
    pub tracked_by_manager: bool,
    pub manager_label: Option<String>,
    pub manager_implementation: Option<String>,
    pub can_stop_tracking: bool,
    pub owner_release_supported: bool,
    pub owner_release_label: String,
    pub owner_release_status: Option<String>,
    pub owner_release_status_reason: Option<String>,
    pub blocked_episode_count: i32,
    pub can_restore_blocked_episodes: bool,
    pub primary_owner: Option<LibraryOwnerResponse>,
    pub owners: Vec<LibraryOwnerResponse>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LibraryOwnerResponse {
    pub ownership_id: String,
    pub owner_type: String,
    pub owner_role: String,
    pub owner_label: Option<String>,
    pub owner_implementation: Option<String>,
    pub owner_provider_id: Option<String>,
    pub owner_instance_id: Option<String>,
    pub owner_extension_id: Option<String>,
    pub owner_external_id: Option<String>,
    pub acquisition_subscription_id: Option<String>,
    pub release_capability: String,
    pub release_policy: String,
    pub release_supported: bool,
    pub release_label: String,
    pub latest_release_status: Option<String>,
    pub latest_release_status_reason: Option<String>,
    pub active: bool,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct DeleteLibraryItemRequest {
    #[serde(default)]
    pub stop_tracking: bool,
    #[serde(default)]
    pub owner_release_action: Option<String>,
    #[serde(default)]
    pub owner_release_best_effort: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LibraryDeleteAction {
    DeleteLocalOnly,
    DeleteAndReleaseOwner,
    ReleaseOwnerOnly,
}

impl LibraryDeleteAction {
    fn from_item_request(payload: &DeleteLibraryItemRequest) -> ApiResult<Self> {
        if let Some(value) = payload.owner_release_action.as_deref() {
            return Self::from_api_value(value, false);
        }
        Ok(if payload.stop_tracking {
            Self::DeleteAndReleaseOwner
        } else {
            Self::DeleteLocalOnly
        })
    }

    fn from_api_value(value: &str, allow_block_episode: bool) -> ApiResult<Self> {
        match normalize_action_value(value).as_str() {
            "delete_local_only" | "deleteLocalOnly" => Ok(Self::DeleteLocalOnly),
            "delete_and_release_owner" | "deleteAndReleaseOwner" => Ok(Self::DeleteAndReleaseOwner),
            "release_owner_only" | "releaseOwnerOnly" => Ok(Self::ReleaseOwnerOnly),
            "block_episode" | "blockEpisode" if allow_block_episode => {
                Ok(Self::DeleteAndReleaseOwner)
            }
            "block_episode" | "blockEpisode" => Err(ApiError::bad_request(
                "ownerReleaseAction 'block_episode' is only valid for episode deletes",
            )),
            other => Err(ApiError::bad_request(format!(
                "unsupported ownerReleaseAction '{other}'"
            ))),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::DeleteLocalOnly => "delete_local_only",
            Self::DeleteAndReleaseOwner => "delete_and_release_owner",
            Self::ReleaseOwnerOnly => "release_owner_only",
        }
    }

    fn dispatcher_action(self) -> Option<OwnerReleaseAction> {
        match self {
            Self::DeleteLocalOnly => None,
            Self::DeleteAndReleaseOwner => Some(OwnerReleaseAction::DeleteAndReleaseOwner),
            Self::ReleaseOwnerOnly => Some(OwnerReleaseAction::ReleaseOwnerOnly),
        }
    }

    fn deletes_local(self) -> bool {
        matches!(self, Self::DeleteLocalOnly | Self::DeleteAndReleaseOwner)
    }

    fn releases_owner(self) -> bool {
        self.dispatcher_action().is_some()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EpisodeDeleteAction {
    DeleteLocalOnly,
    DeleteAndBlockEpisode,
}

impl EpisodeDeleteAction {
    fn from_request(payload: &DeleteEpisodeRequest) -> ApiResult<Self> {
        if let Some(value) = payload.owner_release_action.as_deref() {
            return match normalize_action_value(value).as_str() {
                "delete_local_only" | "deleteLocalOnly" => Ok(Self::DeleteLocalOnly),
                "block_episode" | "blockEpisode" => Ok(Self::DeleteAndBlockEpisode),
                other => Err(ApiError::bad_request(format!(
                    "unsupported ownerReleaseAction '{other}' for episode delete"
                ))),
            };
        }
        Ok(if payload.block_in_elixir {
            Self::DeleteAndBlockEpisode
        } else {
            Self::DeleteLocalOnly
        })
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::DeleteLocalOnly => "delete_local_only",
            Self::DeleteAndBlockEpisode => "block_episode",
        }
    }

    fn blocks_episode(self) -> bool {
        matches!(self, Self::DeleteAndBlockEpisode)
    }
}

fn normalize_action_value(value: &str) -> String {
    value.trim().replace('-', "_")
}

fn api_error_message(error: &ApiError) -> String {
    match error {
        ApiError::BadRequest(message)
        | ApiError::Unauthorized(message)
        | ApiError::Forbidden(message)
        | ApiError::NotFound(message)
        | ApiError::Conflict(message)
        | ApiError::Internal(message) => message.clone(),
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteLibraryItemResponse {
    pub id: String,
    pub r#type: String,
    pub stop_tracking: bool,
    pub owner_release_action: String,
    pub owner_release_best_effort: bool,
    pub local_delete_performed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_release: Option<OwnerReleaseSummary>,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct EpisodeLifecycleResponse {
    pub blocked_in_elixir: bool,
    pub can_delete_locally: bool,
    pub can_block_in_elixir: bool,
    pub can_restore: bool,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct DeleteEpisodeRequest {
    #[serde(default)]
    pub block_in_elixir: bool,
    #[serde(default)]
    pub owner_release_action: Option<String>,
    #[serde(default)]
    pub owner_release_best_effort: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteEpisodeResponse {
    pub id: String,
    pub series_id: String,
    pub blocked_in_elixir: bool,
    pub owner_release_action: String,
    pub owner_release_best_effort: bool,
    pub local_delete_performed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_release: Option<OwnerReleaseSummary>,
    pub message: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RestoreEpisodeResponse {
    pub id: String,
    pub series_id: String,
    pub restored: bool,
    pub message: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RestoreBlockedEpisodesResponse {
    pub id: String,
    pub restored_count: i32,
    pub message: String,
}

#[derive(Debug, Clone)]
struct ResolvedManagedLifecycle {
    manager_provider_id: Uuid,
    manager_item_id: Option<String>,
    manager_label: Option<String>,
    manager_implementation: Option<String>,
    intent_id: Option<Uuid>,
}

#[derive(Debug, Clone)]
struct SeriesIdentityContext {
    series_id: String,
    media_type: MediaType,
    title: String,
    year: Option<i32>,
    external_ids: ExternalIds,
}

#[derive(Debug, Clone)]
struct EpisodeDeleteTarget {
    episode_id: String,
    series: SeriesIdentityContext,
    season_number: i32,
    episode_number: i32,
    absolute_episode_number: Option<i32>,
    file_paths: Vec<String>,
    subtitle_paths: Vec<String>,
    media_file_ids: Vec<String>,
}

#[derive(Serialize)]
pub struct SeasonResponse {
    pub id: String,
    pub season_number: i32,
    pub title: Option<String>,
    pub episode_count: i32,
    pub has_files: bool,
    pub poster_url: Option<String>,
    pub banner_url: Option<String>,
}

#[derive(Serialize)]
pub struct SeasonDetailResponse {
    pub id: String,
    pub series_id: String,
    pub season_number: i32,
    pub title: Option<String>,
    pub description: Option<String>,
    pub poster_url: Option<String>,
    pub banner_url: Option<String>,
}

#[derive(Serialize)]
pub struct EpisodeResponse {
    pub id: String,
    pub season_number: i32,
    pub episode_number: i32,
    pub absolute_episode_number: Option<i32>,
    pub title: Option<String>,
    pub runtime_seconds: Option<i32>,
    pub description: Option<String>,
    pub thumbnail_url: Option<String>,
    pub has_file: bool,
    pub lifecycle: EpisodeLifecycleResponse,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acquisition: Option<EpisodeAcquisitionResponse>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EpisodeAcquisitionResponse {
    pub state: String,
    pub label: String,
    pub message: String,
    pub action: String,
    pub target_key: Option<String>,
    pub reason_code: Option<String>,
    pub source_provider_label: Option<String>,
    pub route_provider_label: Option<String>,
    pub subscription_id: Option<String>,
    pub target_id: Option<String>,
    pub release_id: Option<String>,
    pub job_id: Option<String>,
    pub candidate_count: Option<i64>,
    pub selected_release_title: Option<String>,
    pub last_attempt_at: Option<String>,
    pub updated_at: Option<String>,
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

async fn ensure_series_episode_catalog_for_read(
    state: &AppState,
    series_id: &str,
) -> ApiResult<()> {
    let media_item_id =
        Uuid::parse_str(series_id).map_err(|_| ApiError::bad_request("series id is invalid"))?;
    let result =
        ensure_series_episode_catalog_from_local_metadata(&state.db_pool, None, media_item_id)
            .await
            .map_err(|error| ApiError::internal(error.to_string()))?;

    if result.scaffolded_targets > 0 {
        for subscription_id in result.subscription_ids {
            rebuild_library_episode_acquisition_states_for_subscription(
                &state.db_pool,
                subscription_id,
            )
            .await
            .map_err(|error| ApiError::internal(error.to_string()))?;
        }
    }

    Ok(())
}

pub async fn list_seasons(
    State(state): State<AppState>,
    Path(series_id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Vec<SeasonResponse>>> {
    let preferred_languages = parse_language_header(&headers);
    let exists: Option<String> = sqlx::query_scalar("SELECT id FROM series WHERE id = ? LIMIT 1")
        .bind(&series_id)
        .fetch_optional(&state.db_pool)
        .await
        .map_err(|e| crate::http::error::ApiError::internal(e.to_string()))?;

    if exists.is_none() {
        return Err(crate::http::error::ApiError::not_found("series not found"));
    }

    ensure_series_episode_catalog_for_read(&state, &series_id).await?;

    let rows = sqlx::query(
        "SELECT s.id, s.season_number, s.title, COUNT(e.id) as episode_count, COALESCE(SUM(CASE WHEN e.has_file THEN 1 ELSE 0 END), 0) as file_count FROM seasons s LEFT JOIN episodes e ON e.season_id = s.id WHERE s.series_id = ? GROUP BY s.id ORDER BY s.season_number",
    )
    .bind(&series_id)
    .fetch_all(&state.db_pool)
    .await
    .map_err(|e| crate::http::error::ApiError::internal(e.to_string()))?;

    let season_ids: Vec<String> = rows.iter().map(|row| row.get::<String, _>("id")).collect();

    let posters = load_primary_artwork(
        &state.db_pool,
        "season",
        &season_ids,
        "poster",
        &preferred_languages,
        &["tvdb", "anilist"],
    )
    .await?;
    let banners = load_primary_artwork(
        &state.db_pool,
        "season",
        &season_ids,
        "banner",
        &preferred_languages,
        &["tvdb"],
    )
    .await?;

    let seasons = rows
        .into_iter()
        .map(|row| {
            let id = row.get::<String, _>("id");
            let banner_url = banners.get(&id).cloned();
            let poster_url = posters.get(&id).cloned().or_else(|| banner_url.clone());
            SeasonResponse {
                id,
                season_number: row
                    .try_get::<i64, _>("season_number")
                    .ok()
                    .unwrap_or_default() as i32,
                title: row.try_get::<String, _>("title").ok(),
                episode_count: row
                    .try_get::<i64, _>("episode_count")
                    .ok()
                    .unwrap_or_default() as i32,
                has_files: row.try_get::<i64, _>("file_count").ok().unwrap_or_default() > 0,
                poster_url,
                banner_url,
            }
        })
        .collect();

    Ok(Json(seasons))
}

pub async fn list_episodes(
    State(state): State<AppState>,
    Path(season_id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<Vec<EpisodeResponse>>> {
    let preferred_languages = parse_language_header(&headers);
    let series = load_series_identity_for_season(&state.db_pool, &season_id).await?;
    ensure_series_episode_catalog_for_read(&state, &series.series_id).await?;
    let store = ExtensionStore::new(&state.db_pool);
    let active_episode_tombstones = load_series_episode_tombstones(
        &store,
        series.media_type,
        &series.title,
        series.year,
        &series.external_ids,
    )
    .await?;

    let rows = sqlx::query(
        "SELECT e.id, e.season_number, e.episode_number, e.absolute_episode_number, e.title, e.runtime_seconds, CAST(e.has_file AS INTEGER) AS has_file, e.metadata_json, aem.title as anime_title, aem.duration_seconds as anime_duration, aem.snapshot_url FROM episodes e LEFT JOIN anime_episode_meta aem ON aem.season_id = e.season_id AND aem.episode_number = e.episode_number WHERE e.season_id = ? ORDER BY e.episode_number",
    )
    .bind(&season_id)
    .fetch_all(&state.db_pool)
    .await
    .map_err(|e| crate::http::error::ApiError::internal(e.to_string()))?;

    let episode_ids: Vec<String> = rows.iter().map(|row| row.get::<String, _>("id")).collect();
    let acquisition_projections =
        list_library_episode_acquisition_projections(&state.db_pool, &episode_ids)
            .await
            .map_err(|e| crate::http::error::ApiError::internal(e.to_string()))?;
    let thumbnails = load_primary_artwork(
        &state.db_pool,
        "episode",
        &episode_ids,
        "thumbnail",
        &preferred_languages,
        &["tvdb", "anizip"],
    )
    .await?;

    let episodes = rows
        .into_iter()
        .map(|row| {
            let id = row.get::<String, _>("id");
            let metadata_json: Option<Value> = row
                .try_get::<String, _>("metadata_json")
                .ok()
                .and_then(|s| serde_json::from_str(&s).ok());
            let description = extract_episode_description(metadata_json.as_ref());
            let anime_title: Option<String> = row.try_get("anime_title").ok();
            let raw_title: Option<String> = row.try_get("title").ok();
            let title = match raw_title
                .as_ref()
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
            {
                Some(_) => raw_title,
                None => anime_title,
            };
            let runtime_seconds = row
                .try_get::<i64, _>("runtime_seconds")
                .ok()
                .map(|v| v as i32)
                .or_else(|| {
                    row.try_get::<i64, _>("anime_duration")
                        .ok()
                        .map(|v| v as i32)
                });
            let snapshot_url: Option<String> = row.try_get("snapshot_url").ok();
            let thumbnail_url = thumbnails
                .get(&id)
                .cloned()
                .or_else(|| snapshot_url)
                .or_else(|| extract_episode_thumbnail(metadata_json.as_ref()));
            let has_file = row
                .try_get::<i64, _>("has_file")
                .ok()
                .map(|v| v != 0)
                .unwrap_or(false);
            let season_number = row
                .try_get::<i64, _>("season_number")
                .ok()
                .unwrap_or_default() as i32;
            let episode_number = row
                .try_get::<i64, _>("episode_number")
                .ok()
                .unwrap_or_default() as i32;
            let absolute_episode_number = row
                .try_get::<i64, _>("absolute_episode_number")
                .ok()
                .map(|v| v as i32);
            let blocked_in_elixir = match_managed_episode_tombstone(
                &MediaIdentity {
                    r#type: series.media_type,
                    external_ids: series.external_ids.clone(),
                    title: series.title.clone(),
                    year: series.year,
                    season: Some(season_number),
                    episode: Some(episode_number),
                },
                &series.external_ids,
                season_number,
                episode_number,
                absolute_episode_number,
                &active_episode_tombstones,
            )
            .is_some();
            let acquisition = Some(episode_acquisition_response(
                has_file,
                blocked_in_elixir,
                acquisition_projections.get(&id),
            ));

            EpisodeResponse {
                id,
                season_number,
                episode_number,
                absolute_episode_number,
                title,
                runtime_seconds,
                description,
                thumbnail_url,
                has_file,
                lifecycle: EpisodeLifecycleResponse {
                    blocked_in_elixir,
                    can_delete_locally: has_file,
                    can_block_in_elixir: has_file,
                    can_restore: blocked_in_elixir,
                },
                acquisition,
            }
        })
        .collect();

    Ok(Json(episodes))
}

fn episode_acquisition_response(
    has_file: bool,
    blocked_in_elixir: bool,
    projection: Option<&LibraryEpisodeAcquisitionProjection>,
) -> EpisodeAcquisitionResponse {
    let state = if blocked_in_elixir {
        "blocked"
    } else if has_file {
        "available"
    } else if let Some(projection) = projection {
        match projection.state {
            LibraryEpisodeAcquisitionState::Queued => "queued",
            LibraryEpisodeAcquisitionState::Searching => "searching",
            LibraryEpisodeAcquisitionState::Downloading => "downloading",
            LibraryEpisodeAcquisitionState::PostProcessing => "post_processing",
            LibraryEpisodeAcquisitionState::ReviewNeeded => "review_needed",
            LibraryEpisodeAcquisitionState::NoResults => "no_results",
            LibraryEpisodeAcquisitionState::Failed => "failed",
            LibraryEpisodeAcquisitionState::Imported => "post_processing",
        }
    } else {
        "missing"
    };
    let (label, action, message) = episode_acquisition_copy(
        state,
        projection.and_then(|item| item.source_provider_label.as_deref()),
        projection.and_then(|item| item.route_provider_label.as_deref()),
    );

    EpisodeAcquisitionResponse {
        state: state.to_string(),
        label: label.to_string(),
        message,
        action: action.to_string(),
        target_key: projection.map(|item| item.target_key.clone()),
        reason_code: projection.and_then(|item| item.reason_code.clone()),
        source_provider_label: projection.and_then(|item| item.source_provider_label.clone()),
        route_provider_label: projection.and_then(|item| item.route_provider_label.clone()),
        subscription_id: projection
            .and_then(|item| item.subscription_id.map(|value| value.to_string())),
        target_id: projection.and_then(|item| item.target_id.map(|value| value.to_string())),
        release_id: projection.and_then(|item| item.release_id.map(|value| value.to_string())),
        job_id: projection.and_then(|item| item.job_id.map(|value| value.to_string())),
        candidate_count: projection.and_then(|item| item.candidate_count),
        selected_release_title: projection.and_then(|item| item.selected_release_title.clone()),
        last_attempt_at: projection
            .and_then(|item| item.last_attempt_at.map(|value| value.to_rfc3339())),
        updated_at: projection.map(|item| item.updated_at.to_rfc3339()),
    }
}

fn episode_acquisition_copy(
    state: &str,
    source_provider_label: Option<&str>,
    route_provider_label: Option<&str>,
) -> (&'static str, &'static str, String) {
    match state {
        "available" => ("Available", "play", "Ready to play.".to_string()),
        "blocked" => (
            "Blocked",
            "allow_again",
            "This episode is blocked in Elixir.".to_string(),
        ),
        "queued" => (
            "Queued",
            "view_progress",
            "Queued for download.".to_string(),
        ),
        "searching" => (
            "Searching",
            "view_progress",
            match non_empty_provider_label(source_provider_label) {
                Some(label) => format!("Searching {label} for a safe release."),
                None => "Searching for a safe release.".to_string(),
            },
        ),
        "downloading" => (
            "Downloading",
            "view_progress",
            match route_provider_label
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                Some(label) => format!("Downloading through {label}."),
                None => "Downloading through the selected route.".to_string(),
            },
        ),
        "post_processing" => (
            "Post-processing",
            "view_progress",
            "Download complete. Elixir is importing the file.".to_string(),
        ),
        "review_needed" => (
            "Review needed",
            "review",
            "A release needs review before Elixir can import it.".to_string(),
        ),
        "no_results" => (
            "No results found",
            "search_again",
            match non_empty_provider_label(source_provider_label) {
                Some(label) => format!("No results found from {label}."),
                None => "No safe results were found.".to_string(),
            },
        ),
        "failed" => (
            "Search failed",
            "try_again",
            "Search failed. Try again when ready.".to_string(),
        ),
        _ => (
            "Missing",
            "get_episode",
            "No file is linked yet.".to_string(),
        ),
    }
}

fn non_empty_provider_label(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

pub async fn season_detail(
    State(state): State<AppState>,
    Path(season_id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<SeasonDetailResponse>> {
    let preferred_languages = parse_language_header(&headers);
    let row = sqlx::query(
        "SELECT id, series_id, season_number, title, metadata_json FROM seasons WHERE id = ? LIMIT 1",
    )
    .bind(&season_id)
    .fetch_optional(&state.db_pool)
    .await
    .map_err(|e| crate::http::error::ApiError::internal(e.to_string()))?;

    let Some(row) = row else {
        return Err(crate::http::error::ApiError::not_found("season not found"));
    };

    let metadata_json: Option<Value> = row
        .try_get::<String, _>("metadata_json")
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok());
    let description = extract_season_description(metadata_json.as_ref());

    let posters = load_primary_artwork(
        &state.db_pool,
        "season",
        &[season_id.clone()],
        "poster",
        &preferred_languages,
        &["tvdb", "anilist"],
    )
    .await?;
    let banners = load_primary_artwork(
        &state.db_pool,
        "season",
        &[season_id.clone()],
        "banner",
        &preferred_languages,
        &["tvdb"],
    )
    .await?;

    let poster_url = posters
        .get(&season_id)
        .cloned()
        .or_else(|| banners.get(&season_id).cloned());

    let response = SeasonDetailResponse {
        id: row.get::<String, _>("id"),
        series_id: row.get::<String, _>("series_id"),
        season_number: row
            .try_get::<i64, _>("season_number")
            .ok()
            .unwrap_or_default() as i32,
        title: row.try_get::<String, _>("title").ok(),
        description,
        poster_url,
        banner_url: banners.get(&season_id).cloned(),
    };

    Ok(Json(response))
}

pub async fn detail(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Json<LibraryDetailResponse>> {
    let preferred_languages = parse_language_header(&headers);
    let movie = sqlx::query(
        "SELECT id, title, year, external_imdb, external_tmdb, CAST(runtime_seconds AS TEXT) as runtime_seconds, metadata_json FROM movies WHERE id = ? LIMIT 1",
    )
    .bind(&id)
    .fetch_optional(&state.db_pool)
    .await
    .map_err(|e| crate::http::error::ApiError::internal(e.to_string()))?;

    let (media_type, item_type, title, year, runtime_seconds, metadata_json, external_ids) =
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
                MediaType::Movie,
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

            let series =
                series.ok_or_else(|| crate::http::error::ApiError::not_found("item not found"))?;
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
            let library_type = series.get::<String, _>("library_type");
            let media_type = if library_type == "anime" {
                MediaType::Anime
            } else {
                MediaType::Series
            };
            (
                media_type,
                library_type,
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

    let file_ids: Vec<String> = files.iter().map(|row| row.get::<String, _>("id")).collect();

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

    let (description, genres) =
        extract_metadata_fields(metadata_json.as_ref(), &preferred_languages);
    let owner_type = if item_type == "movie" {
        "movie"
    } else {
        "series"
    };
    let provider_priority: &[&str] = if item_type == "anime" {
        &["anilist", "tvdb", "cinemeta"]
    } else if item_type == "movie" {
        &["tvdb", "cinemeta"]
    } else {
        &["tvdb", "cinemeta"]
    };
    let poster_map = load_primary_artwork(
        &state.db_pool,
        owner_type,
        &[id.clone()],
        "poster",
        &preferred_languages,
        provider_priority,
    )
    .await?;
    let banner_map = load_primary_artwork(
        &state.db_pool,
        owner_type,
        &[id.clone()],
        "banner",
        &preferred_languages,
        provider_priority,
    )
    .await?;
    let backdrop_map = load_primary_artwork(
        &state.db_pool,
        owner_type,
        &[id.clone()],
        "backdrop",
        &preferred_languages,
        provider_priority,
    )
    .await?;

    let poster_url = poster_map.get(&id).cloned();
    let banner_url = banner_map.get(&id).cloned();
    let backdrop_url = backdrop_map.get(&id).cloned();
    let lifecycle =
        resolve_library_item_lifecycle(&state, &id, media_type, &title, year, &external_ids)
            .await?;

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
        poster_url,
        banner_url,
        backdrop_url,
        lifecycle,
        files,
    };

    Ok(Json(response))
}

struct LibraryDeleteTarget {
    media_type: MediaType,
    item_type: String,
    title: String,
    year: Option<i32>,
    external_ids: ExternalIds,
    file_paths: Vec<String>,
    subtitle_paths: Vec<String>,
}

pub async fn delete_item(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(payload): Json<DeleteLibraryItemRequest>,
) -> ApiResult<Json<DeleteLibraryItemResponse>> {
    let action = LibraryDeleteAction::from_item_request(&payload)?;
    let media_item_id =
        Uuid::parse_str(&id).map_err(|_| ApiError::bad_request("library item id is invalid"))?;
    let target = load_library_delete_target(&state.db_pool, &id).await?;
    let lifecycle = resolve_library_item_lifecycle_resolved(
        &state,
        &id,
        target.media_type,
        &target.title,
        target.year,
        &target.external_ids,
    )
    .await?;
    let store = ExtensionStore::new(&state.db_pool);
    let mut owner_release = None;

    if action.releases_owner() {
        if let Some(lifecycle) = lifecycle.as_ref() {
            if can_stop_tracking(lifecycle) {
                store
                    .upsert_managed_library_provenance(&NewManagedLibraryProvenance {
                        media_item_id,
                        media_type: target.media_type,
                        title: target.title.clone(),
                        normalized_title: normalize_managed_intent_title(&target.title),
                        year: target.year,
                        external_ids: Some(target.external_ids.clone()),
                        manager_provider_id: lifecycle.manager_provider_id,
                        manager_item_id: lifecycle.manager_item_id.clone(),
                        manager_label: lifecycle.manager_label.clone(),
                        manager_implementation: lifecycle.manager_implementation.clone(),
                        intent_id: lifecycle.intent_id,
                    })
                    .await
                    .map_err(|error| ApiError::internal(error.to_string()))?;
            }
        }
        let dispatcher_action = action
            .dispatcher_action()
            .ok_or_else(|| ApiError::bad_request("delete action does not include owner release"))?;
        let summary = dispatch_media_owner_release(
            &state,
            &store,
            MediaOwnerReleaseRequest {
                action: dispatcher_action,
                media_item_id,
                media_type: target.media_type,
                title: target.title.clone(),
                year: target.year,
                external_ids: target.external_ids.clone(),
                episode: None,
                fail_on_owner_error: !payload.owner_release_best_effort,
            },
        )
        .await
        .map_err(|error| ApiError::conflict(format!("Failed to release media owner: {error}")))?;
        owner_release = Some(summary);
    }

    if action.releases_owner() && !payload.owner_release_best_effort {
        if let Some(summary) = owner_release.as_ref() {
            if summary.has_failures() {
                return Err(ApiError::conflict(
                    "One or more media owners failed to release.",
                ));
            }
        }
    }

    if action.releases_owner() {
        if let Some(lifecycle) = lifecycle.as_ref() {
            store
                .upsert_managed_media_tombstone(&NewManagedMediaTombstone {
                    media_type: target.media_type,
                    title: target.title.clone(),
                    normalized_title: normalize_managed_intent_title(&target.title),
                    year: target.year,
                    external_ids: Some(target.external_ids.clone()),
                    manager_provider_id: Some(lifecycle.manager_provider_id),
                    manager_item_id: lifecycle.manager_item_id.clone(),
                    manager_label: lifecycle.manager_label.clone(),
                    manager_implementation: lifecycle.manager_implementation.clone(),
                    action: "stop_tracking".to_string(),
                })
                .await
                .map_err(|error| ApiError::internal(error.to_string()))?;
            if let Some(intent_id) = lifecycle.intent_id {
                store
                    .deactivate_managed_ingest_intent(intent_id)
                    .await
                    .map_err(|error| ApiError::internal(error.to_string()))?;
            }
        }
    }

    let local_delete_performed = action.deletes_local();
    if local_delete_performed {
        if let Err(error) = delete_library_item_locally(&state, &id, &target).await {
            if owner_release.is_some() {
                let error_message = api_error_message(&error);
                record_local_delete_failure_after_release(
                    &store,
                    media_item_id,
                    action.as_str(),
                    &error_message,
                )
                .await
                .map_err(|record_error| ApiError::internal(record_error.to_string()))?;
            }
            return Err(error);
        }
    }

    let message = if action.releases_owner() && action.deletes_local() {
        let released_count = owner_release
            .as_ref()
            .map(|summary| summary.released_count)
            .unwrap_or_default();
        let unsupported_count = owner_release
            .as_ref()
            .map(|summary| summary.unsupported_count)
            .unwrap_or_default();
        if released_count > 0 {
            "Deleted from Elixir and stopped the owner from tracking it.".to_string()
        } else if unsupported_count > 0 {
            "Deleted from Elixir. This item did not have a releasable upstream owner.".to_string()
        } else {
            "Deleted from Elixir and processed owner release.".to_string()
        }
    } else if action.releases_owner() {
        "Stopped owner tracking for this item. Local files and library rows were left in place."
            .to_string()
    } else {
        "Deleted from Elixir.".to_string()
    };

    Ok(Json(DeleteLibraryItemResponse {
        id,
        r#type: target.item_type,
        stop_tracking: action.releases_owner(),
        owner_release_action: action.as_str().to_string(),
        owner_release_best_effort: payload.owner_release_best_effort,
        local_delete_performed,
        owner_release,
        message,
    }))
}

pub async fn delete_episode(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(payload): Json<DeleteEpisodeRequest>,
) -> ApiResult<Json<DeleteEpisodeResponse>> {
    let action = EpisodeDeleteAction::from_request(&payload)?;
    let target = load_episode_delete_target(&state.db_pool, &id).await?;
    let store = ExtensionStore::new(&state.db_pool);

    let lifecycle = resolve_library_item_lifecycle_resolved(
        &state,
        &target.series.series_id,
        target.series.media_type,
        &target.series.title,
        target.series.year,
        &target.series.external_ids,
    )
    .await?;

    let mut owner_release = None;
    if action.blocks_episode() {
        let series_id = Uuid::parse_str(&target.series.series_id)
            .map_err(|_| ApiError::bad_request("series id is invalid"))?;
        let episode_id = Uuid::parse_str(&target.episode_id)
            .map_err(|_| ApiError::bad_request("episode id is invalid"))?;
        let summary = dispatch_media_owner_release(
            &state,
            &store,
            MediaOwnerReleaseRequest {
                action: OwnerReleaseAction::BlockEpisode,
                media_item_id: series_id,
                media_type: target.series.media_type,
                title: target.series.title.clone(),
                year: target.series.year,
                external_ids: target.series.external_ids.clone(),
                episode: Some(OwnerReleaseEpisodeScope {
                    episode_id,
                    season_number: target.season_number,
                    episode_number: target.episode_number,
                    absolute_episode_number: target.absolute_episode_number,
                }),
                fail_on_owner_error: !payload.owner_release_best_effort,
            },
        )
        .await
        .map_err(|error| ApiError::conflict(format!("Failed to release episode owner: {error}")))?;
        if !payload.owner_release_best_effort && summary.has_failures() {
            return Err(ApiError::conflict(
                "One or more media owners failed to release this episode.",
            ));
        }
        owner_release = Some(summary);
    }

    if let Err(error) = delete_episode_locally(&state, &target).await {
        if owner_release.is_some() {
            let error_message = api_error_message(&error);
            let series_id = Uuid::parse_str(&target.series.series_id)
                .map_err(|_| ApiError::bad_request("series id is invalid"))?;
            record_local_delete_failure_after_release(
                &store,
                series_id,
                action.as_str(),
                &error_message,
            )
            .await
            .map_err(|record_error| ApiError::internal(record_error.to_string()))?;
        }
        return Err(error);
    }

    if action.blocks_episode() {
        store
            .upsert_managed_episode_tombstone(&NewManagedEpisodeTombstone {
                media_type: target.series.media_type,
                title: target.series.title.clone(),
                normalized_title: normalize_managed_intent_title(&target.series.title),
                year: target.series.year,
                external_ids: Some(target.series.external_ids.clone()),
                manager_provider_id: lifecycle.as_ref().map(|value| value.manager_provider_id),
                manager_item_id: lifecycle
                    .as_ref()
                    .and_then(|value| value.manager_item_id.clone()),
                manager_label: lifecycle
                    .as_ref()
                    .and_then(|value| value.manager_label.clone()),
                manager_implementation: lifecycle
                    .as_ref()
                    .and_then(|value| value.manager_implementation.clone()),
                season_number: target.season_number,
                episode_number: target.episode_number,
                absolute_episode_number: target.absolute_episode_number,
                action: "block_episode".to_string(),
            })
            .await
            .map_err(|error| ApiError::internal(error.to_string()))?;
    }

    let message = if action.blocks_episode() {
        format!(
            "Deleted episode S{:02}E{:02} from Elixir and blocked it from being re-imported here.",
            target.season_number, target.episode_number
        )
    } else {
        format!(
            "Deleted episode S{:02}E{:02} from Elixir. It can return later if it is downloaded again.",
            target.season_number, target.episode_number
        )
    };

    Ok(Json(DeleteEpisodeResponse {
        id: target.episode_id,
        series_id: target.series.series_id,
        blocked_in_elixir: action.blocks_episode(),
        owner_release_action: action.as_str().to_string(),
        owner_release_best_effort: payload.owner_release_best_effort,
        local_delete_performed: true,
        owner_release,
        message,
    }))
}

pub async fn restore_episode(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<RestoreEpisodeResponse>> {
    let target = load_episode_delete_target(&state.db_pool, &id).await?;
    let store = ExtensionStore::new(&state.db_pool);
    let tombstones = load_series_episode_tombstones(
        &store,
        target.series.media_type,
        &target.series.title,
        target.series.year,
        &target.series.external_ids,
    )
    .await?;

    let tombstone = match_managed_episode_tombstone(
        &MediaIdentity {
            r#type: target.series.media_type,
            external_ids: target.series.external_ids.clone(),
            title: target.series.title.clone(),
            year: target.series.year,
            season: Some(target.season_number),
            episode: Some(target.episode_number),
        },
        &target.series.external_ids,
        target.season_number,
        target.episode_number,
        target.absolute_episode_number,
        &tombstones,
    )
    .ok_or_else(|| ApiError::conflict("This episode is not currently blocked in Elixir."))?;

    store
        .deactivate_managed_episode_tombstone(tombstone.tombstone_id)
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?;

    Ok(Json(RestoreEpisodeResponse {
        id: target.episode_id,
        series_id: target.series.series_id,
        restored: true,
        message: format!(
            "Episode S{:02}E{:02} can be imported into Elixir again.",
            target.season_number, target.episode_number
        ),
    }))
}

pub async fn restore_blocked_episodes(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<RestoreBlockedEpisodesResponse>> {
    let series = load_series_identity_for_item(&state.db_pool, &id).await?;
    let store = ExtensionStore::new(&state.db_pool);
    let tombstones = load_series_episode_tombstones(
        &store,
        series.media_type,
        &series.title,
        series.year,
        &series.external_ids,
    )
    .await?;

    if tombstones.is_empty() {
        return Ok(Json(RestoreBlockedEpisodesResponse {
            id,
            restored_count: 0,
            message: "There are no blocked episodes to restore.".to_string(),
        }));
    }

    for tombstone in &tombstones {
        store
            .deactivate_managed_episode_tombstone(tombstone.tombstone_id)
            .await
            .map_err(|error| ApiError::internal(error.to_string()))?;
    }

    Ok(Json(RestoreBlockedEpisodesResponse {
        id,
        restored_count: tombstones.len() as i32,
        message: if tombstones.len() == 1 {
            "Restored 1 blocked episode.".to_string()
        } else {
            format!("Restored {} blocked episodes.", tombstones.len())
        },
    }))
}

async fn resolve_library_item_lifecycle(
    state: &AppState,
    item_id: &str,
    media_type: MediaType,
    title: &str,
    year: Option<i32>,
    external_ids: &ExternalIds,
) -> ApiResult<LibraryLifecycleResponse> {
    let store = ExtensionStore::new(&state.db_pool);
    let lifecycle = resolve_library_item_lifecycle_resolved(
        state,
        item_id,
        media_type,
        title,
        year,
        external_ids,
    )
    .await?;
    let blocked_episode_count = if matches!(media_type, MediaType::Series | MediaType::Anime) {
        load_series_episode_tombstones(&store, media_type, title, year, external_ids)
            .await?
            .len() as i32
    } else {
        0
    };
    let owners = load_library_owner_responses(&store, item_id).await?;
    let primary_owner = primary_owner_response(&owners);
    let owner_release_supported = owners.iter().any(|owner| owner.release_supported);
    let (owner_release_status, owner_release_status_reason) = latest_owner_release_state(&owners);
    let owner_release_label = owner_release_label(primary_owner.as_ref());
    Ok(match lifecycle {
        Some(value) => {
            let can_stop = can_stop_tracking(&value) || owner_release_supported;
            LibraryLifecycleResponse {
                tracked_by_manager: true,
                manager_label: value.manager_label,
                manager_implementation: value.manager_implementation,
                can_stop_tracking: can_stop,
                owner_release_supported,
                owner_release_label,
                owner_release_status,
                owner_release_status_reason,
                blocked_episode_count,
                can_restore_blocked_episodes: blocked_episode_count > 0,
                primary_owner,
                owners,
            }
        }
        None => LibraryLifecycleResponse {
            blocked_episode_count,
            can_restore_blocked_episodes: blocked_episode_count > 0,
            can_stop_tracking: owner_release_supported,
            owner_release_supported,
            owner_release_label,
            owner_release_status,
            owner_release_status_reason,
            primary_owner,
            owners,
            ..LibraryLifecycleResponse::default()
        },
    })
}

async fn resolve_library_item_card_lifecycle(
    state: &AppState,
    item_id: &str,
) -> ApiResult<LibraryItemCardLifecycleResponse> {
    let store = ExtensionStore::new(&state.db_pool);
    let owners = load_library_owner_responses(&store, item_id).await?;
    let primary_owner = primary_owner_response(&owners);
    let owner_release_supported = owners.iter().any(|owner| owner.release_supported);
    let (owner_release_status, owner_release_status_reason) = latest_owner_release_state(&owners);
    let owner_release_label = owner_release_label(primary_owner.as_ref());
    let item_uuid = Uuid::parse_str(item_id)
        .map_err(|_| ApiError::bad_request("library item id is invalid"))?;
    let Some(provenance) = store
        .get_managed_library_provenance(item_uuid)
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?
    else {
        return Ok(LibraryItemCardLifecycleResponse {
            can_stop_tracking: owner_release_supported,
            owner_release_supported,
            owner_release_label,
            owner_release_status,
            owner_release_status_reason,
            primary_owner,
            owners,
            ..LibraryItemCardLifecycleResponse::default()
        });
    };
    let lifecycle = enrich_provenance_with_provider(&store, provenance).await?;
    let manager_label =
        if lifecycle.manager_implementation.is_some() || lifecycle.manager_label.is_some() {
            Some(manager_display_name(
                lifecycle.manager_implementation.as_deref(),
                lifecycle.manager_label.as_deref(),
            ))
        } else {
            None
        };
    Ok(LibraryItemCardLifecycleResponse {
        tracked_by_manager: true,
        manager_label,
        can_stop_tracking: can_stop_tracking(&lifecycle) || owner_release_supported,
        owner_release_supported,
        owner_release_label,
        owner_release_status,
        owner_release_status_reason,
        primary_owner,
        owners,
    })
}

async fn load_library_owner_responses(
    store: &ExtensionStore<'_>,
    item_id: &str,
) -> ApiResult<Vec<LibraryOwnerResponse>> {
    let item_uuid = Uuid::parse_str(item_id)
        .map_err(|_| ApiError::bad_request("library item id is invalid"))?;
    let owners = store
        .ensure_media_ownerships_for_item(item_uuid)
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?;
    let events = store
        .list_media_owner_release_events_for_item(item_uuid)
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?;
    Ok(owners
        .iter()
        .map(|owner| library_owner_response(owner, &events))
        .collect())
}

fn primary_owner_response(owners: &[LibraryOwnerResponse]) -> Option<LibraryOwnerResponse> {
    owners
        .iter()
        .find(|owner| owner.owner_role == "primary")
        .cloned()
        .or_else(|| owners.first().cloned())
}

fn library_owner_response(
    owner: &MediaOwnership,
    events: &[MediaOwnerReleaseEvent],
) -> LibraryOwnerResponse {
    let latest_event = events.iter().find(|event| {
        event
            .ownership_id
            .map(|ownership_id| ownership_id == owner.ownership_id)
            .unwrap_or(false)
    });
    LibraryOwnerResponse {
        ownership_id: owner.ownership_id.to_string(),
        owner_type: owner.owner_type.clone(),
        owner_role: owner.owner_role.clone(),
        owner_label: owner.owner_label.clone(),
        owner_implementation: owner.owner_implementation.clone(),
        owner_provider_id: owner.owner_provider_id.map(|value| value.to_string()),
        owner_instance_id: owner.owner_instance_id.map(|value| value.to_string()),
        owner_extension_id: owner.owner_extension_id.clone(),
        owner_external_id: owner.owner_external_id.clone(),
        acquisition_subscription_id: owner
            .acquisition_subscription_id
            .map(|value| value.to_string()),
        release_capability: owner.release_capability.clone(),
        release_policy: owner.release_policy.clone(),
        release_supported: owner_release_supported(owner),
        release_label: owner_label(owner),
        latest_release_status: latest_event.map(|event| event.status.clone()),
        latest_release_status_reason: latest_event.and_then(|event| event.status_reason.clone()),
        active: owner.active,
    }
}

fn owner_release_events_limit(limit: Option<i64>) -> i64 {
    limit.unwrap_or(100).clamp(1, 500)
}

fn owner_release_event_response(event: &MediaOwnerReleaseEvent) -> OwnerReleaseEventResponse {
    OwnerReleaseEventResponse {
        release_event_id: event.release_event_id.to_string(),
        media_item_id: event.media_item_id.map(|value| value.to_string()),
        ownership_id: event.ownership_id.map(|value| value.to_string()),
        requested_action: event.requested_action.clone(),
        owner_type: event.owner_type.clone(),
        owner_label: event.owner_label.clone(),
        owner_provider_id: event.owner_provider_id.map(|value| value.to_string()),
        acquisition_subscription_id: event
            .acquisition_subscription_id
            .map(|value| value.to_string()),
        status: event.status.clone(),
        status_reason: event.status_reason.clone(),
        request: event.request.as_ref().map(redact_owner_release_audit_value),
        response: event
            .response
            .as_ref()
            .map(redact_owner_release_audit_value),
        created_at: event.created_at.to_rfc3339(),
        updated_at: event.updated_at.to_rfc3339(),
    }
}

fn redact_owner_release_audit_value(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut redacted = serde_json::Map::with_capacity(object.len());
            for (key, value) in object {
                if is_sensitive_audit_key(key) {
                    redacted.insert(key.clone(), Value::String("[redacted]".to_string()));
                } else {
                    redacted.insert(key.clone(), redact_owner_release_audit_value(value));
                }
            }
            Value::Object(redacted)
        }
        Value::Array(items) => {
            Value::Array(items.iter().map(redact_owner_release_audit_value).collect())
        }
        _ => value.clone(),
    }
}

fn is_sensitive_audit_key(key: &str) -> bool {
    let normalized = key
        .chars()
        .filter(|ch| *ch != '_' && *ch != '-' && *ch != '.')
        .collect::<String>()
        .to_ascii_lowercase();
    matches!(
        normalized.as_str(),
        "apikey" | "authorization" | "bearer" | "cookie" | "password" | "secret" | "token"
    ) || normalized.ends_with("token")
        || normalized.ends_with("secret")
        || normalized.ends_with("password")
}

fn owner_release_supported(owner: &MediaOwnership) -> bool {
    match owner.owner_type.as_str() {
        "acquisition" => owner.acquisition_subscription_id.is_some(),
        "extension" => matches!(
            owner.release_capability.as_str(),
            "manager.remove_item" | "media.owner_release"
        ),
        _ => false,
    }
}

fn owner_label(owner: &MediaOwnership) -> String {
    match owner.owner_type.as_str() {
        "acquisition" => "Stop Elixir acquisition monitoring".to_string(),
        "extension" => {
            let display = manager_display_name(
                owner.owner_implementation.as_deref(),
                owner.owner_label.as_deref(),
            );
            format!("Stop tracking in {display}")
        }
        "external" => "Local import".to_string(),
        "system" => "System owner".to_string(),
        _ => "Owner".to_string(),
    }
}

fn owner_release_label(primary_owner: Option<&LibraryOwnerResponse>) -> String {
    primary_owner
        .map(|owner| owner.release_label.clone())
        .unwrap_or_else(|| "Stop tracking in owner".to_string())
}

fn latest_owner_release_state(owners: &[LibraryOwnerResponse]) -> (Option<String>, Option<String>) {
    owners
        .iter()
        .find_map(|owner| {
            owner.latest_release_status.as_ref().map(|status| {
                (
                    Some(status.clone()),
                    owner.latest_release_status_reason.clone(),
                )
            })
        })
        .unwrap_or((None, None))
}

async fn resolve_library_item_lifecycle_resolved(
    state: &AppState,
    item_id: &str,
    media_type: MediaType,
    title: &str,
    year: Option<i32>,
    external_ids: &ExternalIds,
) -> ApiResult<Option<ResolvedManagedLifecycle>> {
    let store = ExtensionStore::new(&state.db_pool);
    let item_uuid = Uuid::parse_str(item_id)
        .map_err(|_| ApiError::bad_request("library item id is invalid"))?;

    if let Some(provenance) = store
        .get_managed_library_provenance(item_uuid)
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?
    {
        return enrich_provenance_with_provider(&store, provenance)
            .await
            .map(Some);
    }

    let intents = store
        .list_active_managed_ingest_intents()
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?;
    let identity = MediaIdentity {
        r#type: media_type,
        external_ids: external_ids.clone(),
        title: title.to_string(),
        year,
        season: None,
        episode: None,
    };
    let Some(intent) = match_managed_ingest_intent(&identity, external_ids, &intents).cloned()
    else {
        return Ok(None);
    };
    let provider = store
        .get_provider(intent.manager_provider_id)
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?;
    let manager_implementation = provider.and_then(|value| value.implementation);
    Ok(Some(ResolvedManagedLifecycle {
        manager_provider_id: intent.manager_provider_id,
        manager_item_id: intent.manager_item_id,
        manager_label: intent.manager_label,
        manager_implementation,
        intent_id: Some(intent.intent_id),
    }))
}

async fn enrich_provenance_with_provider(
    store: &ExtensionStore<'_>,
    provenance: ManagedLibraryProvenance,
) -> ApiResult<ResolvedManagedLifecycle> {
    let provider = store
        .get_provider(provenance.manager_provider_id)
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?;
    let manager_implementation = provenance
        .manager_implementation
        .or_else(|| provider.and_then(|value| value.implementation));
    Ok(ResolvedManagedLifecycle {
        manager_provider_id: provenance.manager_provider_id,
        manager_item_id: provenance.manager_item_id,
        manager_label: provenance.manager_label,
        manager_implementation,
        intent_id: provenance.intent_id,
    })
}

async fn load_library_delete_target(
    pool: &sqlx::AnyPool,
    item_id: &str,
) -> ApiResult<LibraryDeleteTarget> {
    let movie = sqlx::query(
        "SELECT id, title, year, external_imdb, external_tmdb
         FROM movies
         WHERE id = ?
         LIMIT 1",
    )
    .bind(item_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| ApiError::internal(error.to_string()))?;

    let (media_type, item_type, title, year, external_ids) = if let Some(row) = movie {
        (
            MediaType::Movie,
            "movie".to_string(),
            row.get::<String, _>("title"),
            row.try_get::<i64, _>("year").ok().map(|value| value as i32),
            ExternalIds {
                imdb: row.try_get::<String, _>("external_imdb").ok(),
                tmdb: row.try_get::<String, _>("external_tmdb").ok(),
                ..Default::default()
            },
        )
    } else {
        let series = sqlx::query(
            "SELECT id, title, year, library_type, external_imdb, external_tvdb_series, external_anilist
             FROM series
             WHERE id = ?
             LIMIT 1",
        )
        .bind(item_id)
        .fetch_optional(pool)
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?
        .ok_or_else(|| ApiError::not_found("item not found"))?;
        let item_type = series.get::<String, _>("library_type");
        let tvdb_series = series.try_get::<String, _>("external_tvdb_series").ok();
        (
            if item_type == "anime" {
                MediaType::Anime
            } else {
                MediaType::Series
            },
            item_type,
            series.get::<String, _>("title"),
            series
                .try_get::<i64, _>("year")
                .ok()
                .map(|value| value as i32),
            ExternalIds {
                imdb: series.try_get::<String, _>("external_imdb").ok(),
                tvdb: tvdb_series.clone(),
                tvdb_series,
                anilist: series.try_get::<String, _>("external_anilist").ok(),
                ..Default::default()
            },
        )
    };

    let file_paths = if media_type == MediaType::Movie {
        sqlx::query_scalar::<sqlx::Any, String>(
            "SELECT mf.path
             FROM media_files mf
             JOIN movie_files mlf ON mlf.media_file_id = mf.id
             WHERE mlf.movie_id = ?",
        )
        .bind(item_id)
        .fetch_all(pool)
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?
    } else {
        sqlx::query_scalar::<sqlx::Any, String>(
            "SELECT DISTINCT mf.path
             FROM media_files mf
             JOIN episode_files ef ON ef.media_file_id = mf.id
             JOIN episodes e ON e.id = ef.episode_id
             WHERE e.series_id = ?",
        )
        .bind(item_id)
        .fetch_all(pool)
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?
    };

    let subtitle_paths = if media_type == MediaType::Movie {
        sqlx::query_scalar::<sqlx::Any, String>(
            "SELECT es.path
             FROM external_subtitles es
             JOIN media_files mf ON mf.id = es.media_file_id
             JOIN movie_files mlf ON mlf.media_file_id = mf.id
             WHERE mlf.movie_id = ?",
        )
        .bind(item_id)
        .fetch_all(pool)
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?
    } else {
        sqlx::query_scalar::<sqlx::Any, String>(
            "SELECT DISTINCT es.path
             FROM external_subtitles es
             JOIN media_files mf ON mf.id = es.media_file_id
             JOIN episode_files ef ON ef.media_file_id = mf.id
             JOIN episodes e ON e.id = ef.episode_id
             WHERE e.series_id = ?",
        )
        .bind(item_id)
        .fetch_all(pool)
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?
    };

    Ok(LibraryDeleteTarget {
        media_type,
        item_type,
        title,
        year,
        external_ids,
        file_paths,
        subtitle_paths,
    })
}

async fn delete_library_item_locally(
    state: &AppState,
    item_id: &str,
    target: &LibraryDeleteTarget,
) -> ApiResult<()> {
    let mut paths: HashSet<String> = HashSet::new();
    paths.extend(target.file_paths.iter().cloned());
    paths.extend(target.subtitle_paths.iter().cloned());
    for path in paths {
        delete_library_path(&path).await?;
    }

    match target.media_type {
        MediaType::Movie => {
            sqlx::query::<sqlx::Any>("DELETE FROM movies WHERE id = ?")
                .bind(item_id)
                .execute(&state.db_pool)
                .await
                .map_err(|error| ApiError::internal(error.to_string()))?;
        }
        MediaType::Series | MediaType::Anime => {
            sqlx::query::<sqlx::Any>("DELETE FROM series WHERE id = ?")
                .bind(item_id)
                .execute(&state.db_pool)
                .await
                .map_err(|error| ApiError::internal(error.to_string()))?;
        }
    }
    sqlx::query::<sqlx::Any>("DELETE FROM media_items WHERE id = ?")
        .bind(item_id)
        .execute(&state.db_pool)
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?;
    Ok(())
}

async fn record_local_delete_failure_after_release(
    store: &ExtensionStore<'_>,
    media_item_id: Uuid,
    action: &str,
    reason: &str,
) -> anyhow::Result<()> {
    let owners = store.list_active_media_ownerships(media_item_id).await?;
    for owner in owners {
        store
            .create_media_owner_release_event(&NewMediaOwnerReleaseEvent {
                release_event_id: Uuid::new_v4(),
                media_item_id: Some(media_item_id),
                ownership_id: Some(owner.ownership_id),
                requested_action: action.to_string(),
                owner_type: owner.owner_type.clone(),
                owner_label: owner.owner_label.clone(),
                owner_provider_id: owner.owner_provider_id,
                acquisition_subscription_id: owner.acquisition_subscription_id,
                status: "rolled_back_local_delete".to_string(),
                status_reason: Some(format!("Local delete failed after owner release: {reason}")),
                request: Some(json!({
                    "action": action,
                    "mediaItemId": media_item_id,
                    "owner": {
                        "ownershipId": owner.ownership_id,
                        "ownerType": owner.owner_type,
                        "ownerLabel": owner.owner_label,
                    }
                })),
                response: Some(json!({
                    "localDeleteError": reason,
                })),
            })
            .await?;
    }
    Ok(())
}

async fn load_series_identity_for_item(
    pool: &sqlx::AnyPool,
    item_id: &str,
) -> ApiResult<SeriesIdentityContext> {
    let series = sqlx::query(
        "SELECT id, title, year, library_type, external_imdb, external_tvdb_series, external_anilist
         FROM series
         WHERE id = ?
         LIMIT 1",
    )
    .bind(item_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| ApiError::internal(error.to_string()))?
    .ok_or_else(|| ApiError::not_found("series not found"))?;

    let item_type = series.get::<String, _>("library_type");
    let tvdb_series = series.try_get::<String, _>("external_tvdb_series").ok();

    Ok(SeriesIdentityContext {
        series_id: series.get::<String, _>("id"),
        media_type: if item_type == "anime" {
            MediaType::Anime
        } else {
            MediaType::Series
        },
        title: series.get::<String, _>("title"),
        year: series
            .try_get::<i64, _>("year")
            .ok()
            .map(|value| value as i32),
        external_ids: ExternalIds {
            imdb: series.try_get::<String, _>("external_imdb").ok(),
            tvdb: tvdb_series.clone(),
            tvdb_series,
            anilist: series.try_get::<String, _>("external_anilist").ok(),
            ..Default::default()
        },
    })
}

async fn load_series_identity_for_season(
    pool: &sqlx::AnyPool,
    season_id: &str,
) -> ApiResult<SeriesIdentityContext> {
    let series_id = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT series_id FROM seasons WHERE id = ? LIMIT 1",
    )
    .bind(season_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| ApiError::internal(error.to_string()))?
    .ok_or_else(|| ApiError::not_found("season not found"))?;
    load_series_identity_for_item(pool, &series_id).await
}

async fn load_episode_delete_target(
    pool: &sqlx::AnyPool,
    episode_id: &str,
) -> ApiResult<EpisodeDeleteTarget> {
    let row = sqlx::query(
        "SELECT e.id, e.series_id, e.season_number, e.episode_number, e.absolute_episode_number
         FROM episodes e
         WHERE e.id = ?
         LIMIT 1",
    )
    .bind(episode_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| ApiError::internal(error.to_string()))?
    .ok_or_else(|| ApiError::not_found("episode not found"))?;

    let series_id = row.get::<String, _>("series_id");
    let series = load_series_identity_for_item(pool, &series_id).await?;
    let season_number = row
        .try_get::<i64, _>("season_number")
        .ok()
        .unwrap_or_default() as i32;
    let episode_number = row
        .try_get::<i64, _>("episode_number")
        .ok()
        .unwrap_or_default() as i32;
    let absolute_episode_number = row
        .try_get::<i64, _>("absolute_episode_number")
        .ok()
        .map(|value| value as i32);

    let media_file_ids = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT media_file_id FROM episode_files WHERE episode_id = ?",
    )
    .bind(episode_id)
    .fetch_all(pool)
    .await
    .map_err(|error| ApiError::internal(error.to_string()))?;

    for media_file_id in &media_file_ids {
        let linked_episode_count: i64 = sqlx::query_scalar::<sqlx::Any, i64>(
            "SELECT COUNT(*) FROM episode_files WHERE media_file_id = ?",
        )
        .bind(media_file_id)
        .fetch_one(pool)
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?;
        if linked_episode_count > 1 {
            return Err(ApiError::conflict(
                "This file is linked to multiple episodes. Single-episode delete is not supported for multi-episode files yet.",
            ));
        }
    }

    let file_paths = if media_file_ids.is_empty() {
        Vec::new()
    } else {
        let mut builder =
            sqlx::QueryBuilder::<sqlx::Any>::new("SELECT path FROM media_files WHERE id IN (");
        let mut separated = builder.separated(", ");
        for media_file_id in &media_file_ids {
            separated.push_bind(media_file_id);
        }
        separated.push_unseparated(")");
        builder
            .build_query_scalar::<String>()
            .fetch_all(pool)
            .await
            .map_err(|error| ApiError::internal(error.to_string()))?
    };

    let subtitle_paths = if media_file_ids.is_empty() {
        Vec::new()
    } else {
        let mut builder = sqlx::QueryBuilder::<sqlx::Any>::new(
            "SELECT path FROM external_subtitles WHERE media_file_id IN (",
        );
        let mut separated = builder.separated(", ");
        for media_file_id in &media_file_ids {
            separated.push_bind(media_file_id);
        }
        separated.push_unseparated(")");
        builder
            .build_query_scalar::<String>()
            .fetch_all(pool)
            .await
            .map_err(|error| ApiError::internal(error.to_string()))?
    };

    Ok(EpisodeDeleteTarget {
        episode_id: episode_id.to_string(),
        series,
        season_number,
        episode_number,
        absolute_episode_number,
        file_paths,
        subtitle_paths,
        media_file_ids,
    })
}

async fn delete_episode_locally(state: &AppState, target: &EpisodeDeleteTarget) -> ApiResult<()> {
    let mut paths: HashSet<String> = HashSet::new();
    paths.extend(target.file_paths.iter().cloned());
    paths.extend(target.subtitle_paths.iter().cloned());
    for path in paths {
        delete_library_path(&path).await?;
    }

    for media_file_id in &target.media_file_ids {
        sqlx::query::<sqlx::Any>("DELETE FROM media_files WHERE id = ?")
            .bind(media_file_id)
            .execute(&state.db_pool)
            .await
            .map_err(|error| ApiError::internal(error.to_string()))?;
    }

    refresh_episode_has_file_state(&state.db_pool, &target.episode_id).await?;
    Ok(())
}

async fn load_series_episode_tombstones(
    store: &ExtensionStore<'_>,
    media_type: MediaType,
    title: &str,
    year: Option<i32>,
    external_ids: &ExternalIds,
) -> ApiResult<Vec<ManagedEpisodeTombstone>> {
    let tombstones = store
        .list_active_managed_episode_tombstones()
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?;
    Ok(tombstones
        .into_iter()
        .filter(|tombstone| {
            managed_episode_tombstone_matches_series(
                media_type,
                title,
                year,
                external_ids,
                tombstone,
            )
        })
        .collect())
}

async fn refresh_episode_has_file_state(pool: &sqlx::AnyPool, episode_id: &str) -> ApiResult<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE episodes
         SET has_file = CASE WHEN EXISTS (
             SELECT 1
             FROM episode_files ef
             JOIN media_files mf ON mf.id = ef.media_file_id
             WHERE ef.episode_id = ?
               AND mf.scan_state = 'ok'
         ) THEN 1 ELSE 0 END,
             updated_at = CURRENT_TIMESTAMP
         WHERE id = ?",
    )
    .bind(episode_id)
    .bind(episode_id)
    .execute(pool)
    .await
    .map_err(|error| ApiError::internal(error.to_string()))?;
    Ok(())
}

async fn delete_library_path(path: &str) -> ApiResult<()> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Ok(());
    }
    match fs::remove_file(trimmed).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(ApiError::internal(format!(
            "failed to delete '{}': {}",
            trimmed, error
        ))),
    }
}

fn can_stop_tracking(lifecycle: &ResolvedManagedLifecycle) -> bool {
    matches!(
        lifecycle
            .manager_implementation
            .as_deref()
            .map(|value| value.trim().to_ascii_lowercase())
            .as_deref(),
        Some("sonarr") | Some("radarr")
    ) && lifecycle
        .manager_item_id
        .as_deref()
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
}

fn manager_display_name(implementation: Option<&str>, label: Option<&str>) -> String {
    if let Some(value) = implementation
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return match value.to_ascii_lowercase().as_str() {
            "sonarr" => "Sonarr".to_string(),
            "radarr" => "Radarr".to_string(),
            other => {
                let mut chars = other.chars();
                match chars.next() {
                    Some(first) => first.to_ascii_uppercase().to_string() + chars.as_str(),
                    None => "the manager".to_string(),
                }
            }
        };
    }
    label
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("the manager")
        .to_string()
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
        "SELECT id, media_file_id, track_type, language, title, codec, channels, CAST(is_default AS INTEGER) AS is_default, CAST(is_forced AS INTEGER) AS is_forced, stream_index FROM media_tracks WHERE media_file_id IN (",
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
        let is_default = row
            .try_get::<i64, _>("is_default")
            .ok()
            .map(|v| v != 0)
            .unwrap_or(false);
        let is_forced = row
            .try_get::<i64, _>("is_forced")
            .ok()
            .map(|v| v != 0)
            .unwrap_or(false);
        entry.push(MediaTrackResponse {
            id: row.get::<String, _>("id"),
            track_type: row.get::<String, _>("track_type"),
            language: row.try_get::<String, _>("language").ok(),
            title: row.try_get::<String, _>("title").ok(),
            codec: row.try_get::<String, _>("codec").ok(),
            channels: row.try_get::<i64, _>("channels").ok().map(|v| v as i32),
            is_default,
            is_forced,
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
        "SELECT id, media_file_id, path, language, title, format, CAST(is_default AS INTEGER) AS is_default, CAST(is_forced AS INTEGER) AS is_forced FROM external_subtitles WHERE media_file_id IN (",
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
        let is_default = row
            .try_get::<i64, _>("is_default")
            .ok()
            .map(|v| v != 0)
            .unwrap_or(false);
        let is_forced = row
            .try_get::<i64, _>("is_forced")
            .ok()
            .map(|v| v != 0)
            .unwrap_or(false);
        entry.push(ExternalSubtitleResponse {
            id: row.get::<String, _>("id"),
            path: row.get::<String, _>("path"),
            language: row.try_get::<String, _>("language").ok(),
            title: row.try_get::<String, _>("title").ok(),
            format: row.try_get::<String, _>("format").ok(),
            is_default,
            is_forced,
        });
    }

    Ok(by_file)
}

struct ArtworkRow {
    id: String,
    language: Option<String>,
    provider: Option<String>,
    score: Option<f32>,
    width: Option<i32>,
    height: Option<i32>,
}

async fn load_primary_artwork(
    pool: &sqlx::AnyPool,
    owner_type: &str,
    owner_ids: &[String],
    kind: &str,
    preferred_languages: &[String],
    provider_priority: &[&str],
) -> ApiResult<HashMap<String, String>> {
    let mut by_owner: HashMap<String, String> = HashMap::new();
    if owner_ids.is_empty() {
        return Ok(by_owner);
    }

    let mut builder = sqlx::QueryBuilder::<sqlx::Any>::new(
        "SELECT owner_id, id, url, language, provider, score, width, height FROM artwork_refs WHERE owner_type = ",
    );
    builder.push_bind(owner_type);
    builder.push(" AND kind = ");
    builder.push_bind(kind);
    builder.push(" AND owner_id IN (");
    let mut separated = builder.separated(", ");
    for id in owner_ids {
        separated.push_bind(id);
    }
    separated.push_unseparated(")");

    let rows = builder
        .build()
        .fetch_all(pool)
        .await
        .map_err(|e| crate::http::error::ApiError::internal(e.to_string()))?;

    let mut grouped: HashMap<String, Vec<ArtworkRow>> = HashMap::new();
    for row in rows {
        let owner_id = row.get::<String, _>("owner_id");
        let url = row.get::<String, _>("url");
        if artwork_url_disallowed_for_kind(kind, &url) {
            continue;
        }
        grouped
            .entry(owner_id.clone())
            .or_default()
            .push(ArtworkRow {
                id: row.get::<String, _>("id"),
                language: row.try_get::<String, _>("language").ok(),
                provider: row.try_get::<String, _>("provider").ok(),
                score: row.try_get::<f64, _>("score").ok().map(|v| v as f32),
                width: row.try_get::<i64, _>("width").ok().map(|v| v as i32),
                height: row.try_get::<i64, _>("height").ok().map(|v| v as i32),
            });
    }

    for (owner_id, candidates) in grouped {
        if let Some(selected) =
            select_primary_artwork(&candidates, preferred_languages, provider_priority)
        {
            let url = format!("/api/v1/artwork/{}", selected.id);
            by_owner.insert(owner_id, url);
        }
    }

    Ok(by_owner)
}

fn artwork_url_disallowed_for_kind(kind: &str, url: &str) -> bool {
    let normalized = url.to_ascii_lowercase();
    if normalized.contains("/actor/") || normalized.contains("/person/") {
        return true;
    }
    if normalized.contains("/clearart/") || normalized.contains("/clearlogo/") {
        return matches!(kind, "poster" | "backdrop" | "banner");
    }
    false
}

fn extract_metadata_fields(
    meta: Option<&Value>,
    preferred_languages: &[String],
) -> (Option<String>, Vec<String>) {
    let mut description = None;
    let mut genres = Vec::new();

    if let Some(value) = meta {
        let desc_candidate = ["description", "overview", "plot", "summary"]
            .iter()
            .find_map(|key| value.get(*key).and_then(Value::as_str))
            .map(str::to_string)
            .or_else(|| extract_translated_description(value, preferred_languages));

        if let Some(desc) = desc_candidate {
            let cleaned = clean_description(&desc);
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

fn extract_translated_description(value: &Value, preferred_languages: &[String]) -> Option<String> {
    let translation_arrays = [
        value
            .get("translations")
            .and_then(|translations| translations.get("overviewTranslations")),
        value.get("overviewTranslations"),
        value.get("overview_translations"),
    ];

    for translations in translation_arrays.into_iter().flatten() {
        let Some(entries) = translations.as_array() else {
            continue;
        };
        if let Some(selected) = select_translated_description(entries, preferred_languages) {
            return Some(selected);
        }
    }

    None
}

fn select_translated_description(
    entries: &[Value],
    preferred_languages: &[String],
) -> Option<String> {
    for preferred in preferred_languages {
        for entry in entries {
            let language = entry
                .get("language")
                .or_else(|| entry.get("languageCode"))
                .and_then(Value::as_str)
                .map(normalize_language);
            if language
                .as_deref()
                .is_some_and(|language| language_matches(language, preferred))
            {
                if let Some(text) = translated_overview_text(entry) {
                    return Some(text);
                }
            }
        }
    }

    for entry in entries {
        if entry
            .get("isPrimary")
            .or_else(|| entry.get("is_primary"))
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            if let Some(text) = translated_overview_text(entry) {
                return Some(text);
            }
        }
    }

    entries.iter().find_map(translated_overview_text)
}

fn translated_overview_text(entry: &Value) -> Option<String> {
    entry
        .get("overview")
        .or_else(|| entry.get("description"))
        .or_else(|| entry.get("summary"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn parse_language_header(headers: &HeaderMap) -> Vec<String> {
    let mut languages = Vec::new();
    if let Some(value) = headers.get(axum::http::header::ACCEPT_LANGUAGE) {
        if let Ok(raw) = value.to_str() {
            for part in raw.split(',') {
                let tag = part.split(';').next().unwrap_or("").trim();
                if tag.is_empty() {
                    continue;
                }
                let normalized = normalize_language(tag);
                if !languages.contains(&normalized) {
                    languages.push(normalized.clone());
                }
                if let Some(base) = normalized.split('-').next() {
                    let base = base.to_string();
                    if !languages.contains(&base) {
                        languages.push(base);
                    }
                }
            }
        }
    }
    if !languages.iter().any(|lang| lang == "en") {
        languages.push("en".to_string());
    }
    languages
}

fn select_primary_artwork<'a>(
    candidates: &'a [ArtworkRow],
    preferred_languages: &[String],
    provider_priority: &[&str],
) -> Option<&'a ArtworkRow> {
    let mut best: Option<&ArtworkRow> = None;
    for candidate in candidates {
        if best.is_none() {
            best = Some(candidate);
            continue;
        }
        let current = best.unwrap();
        if artwork_cmp(candidate, current, preferred_languages, provider_priority).is_lt() {
            best = Some(candidate);
        }
    }
    best
}

fn artwork_cmp(
    left: &ArtworkRow,
    right: &ArtworkRow,
    preferred_languages: &[String],
    provider_priority: &[&str],
) -> std::cmp::Ordering {
    let left_lang = language_rank(left.language.as_deref(), preferred_languages);
    let right_lang = language_rank(right.language.as_deref(), preferred_languages);
    if left_lang != right_lang {
        return left_lang.cmp(&right_lang);
    }

    let left_provider = provider_rank(left.provider.as_deref(), provider_priority);
    let right_provider = provider_rank(right.provider.as_deref(), provider_priority);
    if left_provider != right_provider {
        return left_provider.cmp(&right_provider);
    }

    let left_score = left.score.unwrap_or(0.0);
    let right_score = right.score.unwrap_or(0.0);
    if (left_score - right_score).abs() > f32::EPSILON {
        return right_score
            .partial_cmp(&left_score)
            .unwrap_or(std::cmp::Ordering::Equal);
    }

    let left_area = left.width.unwrap_or(0) * left.height.unwrap_or(0);
    let right_area = right.width.unwrap_or(0) * right.height.unwrap_or(0);
    right_area.cmp(&left_area)
}

fn language_rank(language: Option<&str>, preferred_languages: &[String]) -> usize {
    let Some(language) = language else {
        return preferred_languages.len() + 2;
    };
    let normalized = normalize_language(language);
    for (idx, pref) in preferred_languages.iter().enumerate() {
        if language_matches(&normalized, pref) {
            return idx;
        }
    }
    preferred_languages.len() + 1
}

fn language_matches(candidate: &str, preferred: &str) -> bool {
    if candidate == preferred {
        return true;
    }
    candidate.starts_with(&format!("{}-", preferred))
        || preferred.starts_with(&format!("{}-", candidate))
}

fn normalize_language(value: &str) -> String {
    let normalized = value.trim().replace('_', "-").to_lowercase();
    let (primary, suffix) = normalized
        .split_once('-')
        .map(|(primary, suffix)| (primary, Some(suffix)))
        .unwrap_or((normalized.as_str(), None));
    let primary = match primary {
        "eng" => "en",
        "deu" | "ger" => "de",
        "fra" | "fre" => "fr",
        "spa" => "es",
        "ita" => "it",
        "por" => "pt",
        "jpn" => "ja",
        "zho" | "chi" => "zh",
        "kor" => "ko",
        "rus" => "ru",
        "pol" => "pl",
        "tur" => "tr",
        "dan" => "da",
        "fin" => "fi",
        "heb" => "he",
        "hun" => "hu",
        "est" => "et",
        other => other,
    };
    if let Some(suffix) = suffix {
        format!("{primary}-{suffix}")
    } else {
        primary.to_string()
    }
}

fn provider_rank(provider: Option<&str>, priority: &[&str]) -> usize {
    let Some(provider) = provider else {
        return priority.len();
    };
    priority
        .iter()
        .position(|p| p.eq_ignore_ascii_case(provider))
        .unwrap_or(priority.len())
}

fn extract_episode_description(meta: Option<&Value>) -> Option<String> {
    let meta = meta?;
    extract_description_from_meta(meta)
}

fn extract_episode_thumbnail(meta: Option<&Value>) -> Option<String> {
    let meta = meta?;
    meta.get("image")
        .or_else(|| meta.get("imageUrl"))
        .or_else(|| meta.get("thumbnail"))
        .and_then(Value::as_str)
        .map(|value| value.to_string())
}

fn extract_season_description(meta: Option<&Value>) -> Option<String> {
    let meta = meta?;
    if let Some(desc) = extract_description_from_meta(meta) {
        return Some(desc);
    }
    if let Some(tvdb) = meta.get("tvdb") {
        return extract_description_from_meta(tvdb);
    }
    None
}

fn extract_description_from_meta(meta: &Value) -> Option<String> {
    let desc = meta
        .get("overview")
        .and_then(Value::as_str)
        .or_else(|| meta.get("description").and_then(Value::as_str))
        .or_else(|| meta.get("summary").and_then(Value::as_str));
    desc.map(clean_description).filter(|d| !d.is_empty())
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn metadata_fields_use_tvdb_primary_overview_translation() {
        let meta = json!({
            "translations": {
                "overviewTranslations": [
                    { "language": "deu", "overview": "Deutsche Beschreibung" },
                    {
                        "isPrimary": true,
                        "language": "eng",
                        "overview": "English Casino Royale description."
                    }
                ]
            },
            "genres": [{ "name": "Action" }]
        });

        let (description, genres) = extract_metadata_fields(Some(&meta), &["en".to_string()]);

        assert_eq!(
            description.as_deref(),
            Some("English Casino Royale description.")
        );
        assert_eq!(genres, vec!["Action".to_string()]);
    }

    #[test]
    fn metadata_fields_prefer_requested_translation_when_available() {
        let meta = json!({
            "overviewTranslations": [
                {
                    "isPrimary": true,
                    "language": "eng",
                    "overview": "English description."
                },
                { "language": "fra", "overview": "Description francaise." }
            ]
        });

        let (description, _) = extract_metadata_fields(Some(&meta), &["fr".to_string()]);

        assert_eq!(description.as_deref(), Some("Description francaise."));
    }

    #[test]
    fn artwork_selection_rejects_non_backdrop_tvdb_artwork_paths() {
        assert!(artwork_url_disallowed_for_kind(
            "backdrop",
            "https://artworks.thetvdb.com/banners/v4/movie/330/clearart/6124a45419fa7.png"
        ));
        assert!(artwork_url_disallowed_for_kind(
            "poster",
            "https://artworks.thetvdb.com/banners/v4/actor/525247/photo/6075f14031529.jpg"
        ));
        assert!(!artwork_url_disallowed_for_kind(
            "backdrop",
            "https://artworks.thetvdb.com/banners/v4/movie/330/backgrounds/664a76d83adf3.jpg"
        ));
    }
}
