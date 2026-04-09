use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{Context, Result as AnyResult, bail};
use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use reqwest::header::{ACCEPT, HeaderMap, HeaderValue, USER_AGENT};
use reqwest::{Client, Method as ReqwestMethod, StatusCode as ReqwestStatusCode, Url};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Value, json};
use tokio::net::lookup_host;
use tokio::process::Command;
use tracing::{debug, info};
use uuid::Uuid;

use crate::{
    db::models::{ExtensionTrustLevel, MediaType, ProviderHealthState, SecretScope},
    extensions::{
        ExternalIds,
        manifest::ExtensionManifest,
        store::{ExtensionStore, NewManagedIngestIntent},
    },
    http::{
        auth::CurrentUser,
        error::{ApiError, ApiResult},
    },
    metadata::DiscoveryResult,
    orchestrator::model::ProviderEndpoint,
    state::AppState,
};

const MANAGER_PREF_MOVIE: &str = "manager_preference.movie";
const MANAGER_PREF_SERIES: &str = "manager_preference.series";
const MANAGER_PREF_ANIME: &str = "manager_preference.anime";
const CONTROL_DEFAULTS_SETTING_PREFIX: &str = "extensions.control_defaults.instance.";

#[derive(Debug, Clone)]
struct ManagerControlDefaults {
    monitor_on_add: bool,
    search_on_add: bool,
}

impl Default for ManagerControlDefaults {
    fn default() -> Self {
        Self {
            monitor_on_add: true,
            search_on_add: true,
        }
    }
}

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

#[derive(Debug, Deserialize)]
pub struct FindQuery {
    pub q: Option<String>,
    pub r#type: Option<String>,
    pub provider_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct FindMediaTargetsQuery {
    pub media_type: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct FindMediaAcquisitionQuery {
    #[serde(default = "default_acquisition_limit")]
    pub limit: usize,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FindMediaSearchRequest {
    #[serde(alias = "media_type")]
    pub media_type: Option<String>,
    pub query: Option<String>,
    #[serde(default, alias = "providers")]
    pub provider_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FindMediaAddRequest {
    #[serde(alias = "media_type")]
    pub media_type: Option<String>,
    pub item: Option<FindMediaAddItem>,
    #[serde(default, alias = "manager_provider_id")]
    pub manager_provider_id: Option<String>,
    #[serde(default)]
    pub options: FindMediaAddOptions,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FindMediaAddItem {
    pub title: Option<String>,
    #[serde(default)]
    pub year: Option<i32>,
    #[serde(default, alias = "external_ids")]
    pub external_ids: Option<ExternalIds>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct FindMediaAddOptions {
    #[serde(default)]
    pub monitor: Option<bool>,
    #[serde(default)]
    pub search: Option<bool>,
    #[serde(default, alias = "root_folder_path")]
    pub root_folder_path: Option<String>,
    #[serde(default, alias = "quality_profile_id")]
    pub quality_profile_id: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateManagerPreferencesRequest {
    #[serde(default, deserialize_with = "deserialize_optional_json_value")]
    pub movie_provider_id: Option<Value>,
    #[serde(default, deserialize_with = "deserialize_optional_json_value")]
    pub series_provider_id: Option<Value>,
    #[serde(default, deserialize_with = "deserialize_optional_json_value")]
    pub anime_provider_id: Option<Value>,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagerPreferenceState {
    pub movie_provider_id: Option<Uuid>,
    pub series_provider_id: Option<Uuid>,
    pub anime_provider_id: Option<Uuid>,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FindMediaPreferencesState {
    pub tv_default_manager_provider_id: Option<Uuid>,
    pub movies_default_manager_provider_id: Option<Uuid>,
    pub anime_default_manager_provider_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderSummary {
    pub provider_id: Uuid,
    pub extension_id: String,
    pub instance_id: Uuid,
    pub instance_name: String,
    pub capability: String,
    pub implementation: Option<String>,
    pub health_state: ProviderHealthState,
    pub media_types: Vec<String>,
    pub label: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagerPreferencesResponse {
    pub preferences: ManagerPreferenceState,
    pub movie_providers: Vec<ProviderSummary>,
    pub series_providers: Vec<ProviderSummary>,
    pub anime_providers: Vec<ProviderSummary>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderSearchError {
    pub provider_id: Uuid,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FindMediaResult {
    pub title: String,
    pub r#type: MediaType,
    pub year: Option<i32>,
    pub external_ids: Option<ExternalIds>,
    pub description: Option<String>,
    pub poster_url: Option<String>,
    pub popularity_score: Option<f64>,
    pub source_provider_ids: Vec<Uuid>,
    pub source_labels: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FindMediaResponse {
    pub query: String,
    pub media_type: MediaType,
    pub search_providers: Vec<ProviderSummary>,
    pub manager_providers: Vec<ProviderSummary>,
    pub preferred_manager_provider_id: Option<Uuid>,
    pub default_manager_provider_id: Option<Uuid>,
    pub results: Vec<FindMediaResult>,
    pub provider_errors: Vec<ProviderSearchError>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FindMediaTargetsResponse {
    pub media_type: String,
    pub search_providers: Vec<ProviderSummary>,
    pub manager_candidates: Vec<ProviderSummary>,
    pub default_manager_provider_id: Option<Uuid>,
    pub preferred_manager_provider_id: Option<Uuid>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FindMediaSearchResponse {
    pub query: String,
    pub media_type: String,
    pub results: Vec<FindMediaResult>,
    pub provider_errors: Vec<ProviderSearchError>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FindMediaAddResponse {
    pub operation_id: Uuid,
    pub intent_id: Uuid,
    pub manager_provider_id: Uuid,
    pub manager_label: String,
    pub media_type: String,
    pub title: String,
    pub manager_item_id: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FindMediaAcquisitionResponse {
    pub updated_at: DateTime<Utc>,
    pub active_count: usize,
    pub downloading_count: usize,
    pub needs_attention_count: usize,
    pub recent_completed_count: usize,
    pub total_download_rate_bps: Option<u64>,
    pub total_upload_rate_bps: Option<u64>,
    pub items: Vec<FindMediaAcquisitionItem>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FindMediaAcquisitionItem {
    pub intent_id: Uuid,
    pub title: String,
    pub media_type: String,
    pub year: Option<i32>,
    pub external_ids: Option<ExternalIds>,
    pub manager_provider_id: Uuid,
    pub manager_label: String,
    pub manager_item_id: Option<String>,
    pub source: String,
    pub stage: String,
    pub stage_label: String,
    pub description: String,
    pub progress_percent: Option<f64>,
    pub eta_seconds: Option<i64>,
    pub downloader_label: Option<String>,
    pub protocol: Option<String>,
    pub last_matched_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AcquisitionStage {
    Requested,
    Searching,
    Queued,
    Downloading,
    PostProcessing,
    Importing,
    Ready,
    NeedsAttention,
    Failed,
}

impl AcquisitionStage {
    fn as_str(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::Searching => "searching",
            Self::Queued => "queued",
            Self::Downloading => "downloading",
            Self::PostProcessing => "post_processing",
            Self::Importing => "importing",
            Self::Ready => "ready",
            Self::NeedsAttention => "needs_attention",
            Self::Failed => "failed",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Requested => "Requested",
            Self::Searching => "Searching",
            Self::Queued => "Queued",
            Self::Downloading => "Downloading",
            Self::PostProcessing => "Post-processing",
            Self::Importing => "Importing",
            Self::Ready => "Ready",
            Self::NeedsAttention => "Needs attention",
            Self::Failed => "Failed",
        }
    }

    fn sort_priority(self) -> i32 {
        match self {
            Self::NeedsAttention => 0,
            Self::Failed => 1,
            Self::Downloading => 2,
            Self::PostProcessing => 3,
            Self::Importing => 4,
            Self::Queued => 5,
            Self::Searching => 6,
            Self::Requested => 7,
            Self::Ready => 8,
        }
    }

    fn is_active(self) -> bool {
        !matches!(self, Self::Ready | Self::Failed)
    }
}

#[derive(Debug, Clone)]
struct AcquisitionItemState {
    stage: AcquisitionStage,
    description: String,
    progress_percent: Option<f64>,
    eta_seconds: Option<i64>,
    downloader_label: Option<String>,
    protocol: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct AcquisitionDownloaderTotals {
    total_download_rate_bps: Option<u64>,
    total_upload_rate_bps: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FindMediaPreferencesResponse {
    pub preferences: FindMediaPreferencesState,
    pub tv_manager_candidates: Vec<ProviderSummary>,
    pub movies_manager_candidates: Vec<ProviderSummary>,
    pub anime_manager_candidates: Vec<ProviderSummary>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PatchFindMediaPreferencesRequest {
    #[serde(
        default,
        alias = "tv_default_manager_provider_id",
        deserialize_with = "deserialize_optional_json_value"
    )]
    pub tv_default_manager_provider_id: Option<Value>,
    #[serde(
        default,
        alias = "movies_default_manager_provider_id",
        deserialize_with = "deserialize_optional_json_value"
    )]
    pub movies_default_manager_provider_id: Option<Value>,
    #[serde(
        default,
        alias = "anime_default_manager_provider_id",
        deserialize_with = "deserialize_optional_json_value"
    )]
    pub anime_default_manager_provider_id: Option<Value>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct ProviderScopeDocument {
    #[serde(default)]
    media_types: Vec<String>,
    #[serde(default)]
    actions: Vec<String>,
    #[serde(default)]
    requires_account: bool,
    #[serde(default)]
    required_fields: Vec<String>,
}

#[derive(Debug, Clone)]
struct ProviderContext {
    detail: crate::extensions::store::ProviderDetails,
    instance_name: String,
    instance_config: Option<Value>,
    scope: ProviderScopeDocument,
    media_types: Vec<MediaType>,
}

fn deserialize_optional_json_value<'de, D>(deserializer: D) -> Result<Option<Value>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Some(Value::deserialize(deserializer)?))
}

fn default_limit() -> usize {
    5
}

fn default_acquisition_limit() -> usize {
    12
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
    let media_type = parse_media_type(params.r#type.as_deref());

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
    let media_type = parse_media_type(params.r#type.as_deref());

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

pub async fn find_media(
    State(state): State<AppState>,
    _user: CurrentUser,
    Query(params): Query<FindQuery>,
) -> ApiResult<Json<FindMediaResponse>> {
    let query = params
        .q
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| ApiError::bad_request("q is required"))?
        .trim()
        .to_string();
    let media_type = parse_media_type(params.r#type.as_deref()).unwrap_or(MediaType::Movie);

    let requested_provider_ids = match params.provider_id.as_deref() {
        Some(raw_provider_id) => vec![
            Uuid::parse_str(raw_provider_id)
                .map_err(|_| ApiError::bad_request("invalid provider_id"))?,
        ],
        None => Vec::new(),
    };

    let response =
        execute_find_media_search(&state, query, media_type, requested_provider_ids).await?;
    Ok(Json(response))
}

pub async fn find_media_targets(
    State(state): State<AppState>,
    _user: CurrentUser,
    Query(params): Query<FindMediaTargetsQuery>,
) -> ApiResult<Json<FindMediaTargetsResponse>> {
    let media_type = parse_media_type(params.media_type.as_deref()).unwrap_or(MediaType::Movie);
    let store = ExtensionStore::new(&state.db_pool);
    let providers = load_provider_contexts(&store)
        .await
        .map_err(ApiError::from)?;
    let preferences = load_manager_preferences(&store, &providers)
        .await
        .map_err(ApiError::from)?;

    let (search_contexts, _search_errors) = filter_search_providers(
        &state,
        &store,
        collect_search_providers(&providers, media_type),
    )
    .await
    .map_err(ApiError::from)?;
    let (manager_contexts, _manager_errors) = filter_manager_providers(
        &state,
        &store,
        collect_manager_providers(&providers, media_type),
    )
    .await
    .map_err(ApiError::from)?;

    let preferred = preferred_manager_for_type(&preferences, media_type);
    let blueprint_preferred =
        resolve_blueprint_preferred_manager(&store, &manager_contexts, media_type)
            .await
            .map_err(ApiError::from)?;
    let default = resolve_default_manager(preferred, blueprint_preferred, &manager_contexts);

    let manager_candidates: Vec<ProviderSummary> =
        manager_contexts.iter().map(provider_summary).collect();
    let search_providers: Vec<ProviderSummary> =
        search_contexts.iter().map(provider_summary).collect();

    Ok(Json(FindMediaTargetsResponse {
        media_type: media_type_api_name(media_type).to_string(),
        search_providers,
        manager_candidates,
        default_manager_provider_id: default,
        preferred_manager_provider_id: preferred,
    }))
}

pub async fn find_media_search(
    State(state): State<AppState>,
    _user: CurrentUser,
    Json(payload): Json<FindMediaSearchRequest>,
) -> ApiResult<Json<FindMediaSearchResponse>> {
    let media_type = parse_media_type(payload.media_type.as_deref())
        .ok_or_else(|| ApiError::bad_request("media_type is required"))?;
    let query = payload
        .query
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ApiError::bad_request("query is required"))?
        .to_string();
    let provider_ids = parse_provider_ids(&payload.provider_ids)?;

    info!(
        media_type = media_type_api_name(media_type),
        query = %query,
        provider_filter_count = provider_ids.len(),
        "find media search requested"
    );

    let response =
        execute_find_media_search(&state, query.clone(), media_type, provider_ids).await?;

    info!(
        media_type = media_type_api_name(media_type),
        query = %query,
        result_count = response.results.len(),
        provider_error_count = response.provider_errors.len(),
        "find media search completed"
    );

    Ok(Json(FindMediaSearchResponse {
        query,
        media_type: media_type_api_name(media_type).to_string(),
        results: response.results,
        provider_errors: response.provider_errors,
    }))
}

pub async fn find_media_acquisition(
    State(state): State<AppState>,
    _user: CurrentUser,
    Query(params): Query<FindMediaAcquisitionQuery>,
) -> ApiResult<Json<FindMediaAcquisitionResponse>> {
    let store = ExtensionStore::new(&state.db_pool);
    let limit = params.limit.clamp(1, 50);
    let response = build_find_media_acquisition_response(&state, &store, limit)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(response))
}

pub async fn find_media_add(
    State(state): State<AppState>,
    _user: CurrentUser,
    Json(payload): Json<FindMediaAddRequest>,
) -> Result<Response, ApiError> {
    let media_type = match parse_media_type(payload.media_type.as_deref()) {
        Some(value) => value,
        None => {
            return Ok(conflict_response(
                "missing_manager",
                "media_type is required",
                json!({}),
            ));
        }
    };
    let item = match payload.item {
        Some(item) => item,
        None => {
            return Ok(conflict_response(
                "missing_manager",
                "item is required",
                json!({}),
            ));
        }
    };
    let title = item
        .title
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ApiError::bad_request("item.title is required"))?
        .to_string();
    let explicit_manager = parse_provider_id(payload.manager_provider_id.as_deref())?;

    info!(
        media_type = media_type_api_name(media_type),
        title = %title,
        year = item.year,
        explicit_manager = ?explicit_manager,
        "find media add requested"
    );

    let store = ExtensionStore::new(&state.db_pool);
    let providers = load_provider_contexts(&store)
        .await
        .map_err(ApiError::from)?;
    let preferences = load_manager_preferences(&store, &providers)
        .await
        .map_err(ApiError::from)?;
    let manager_contexts = collect_manager_providers(&providers, media_type);
    let manager_resolution = resolve_manager_for_add(
        &state,
        &store,
        &manager_contexts,
        &preferences,
        media_type,
        explicit_manager,
    )
    .await
    .map_err(ApiError::from)?;

    let manager = match manager_resolution {
        ManagerSelection::Selected(provider) => provider,
        ManagerSelection::Conflict(conflict) => return Ok(conflict.into_response()),
    };

    info!(
        media_type = media_type_api_name(media_type),
        title = %title,
        manager_provider_id = %manager.detail.provider.provider_id,
        manager_label = %provider_label(&manager),
        "find media add resolved manager"
    );

    let manager_item_id = add_with_manager_provider(
        &state,
        &store,
        &manager,
        media_type,
        &item,
        &payload.options,
    )
    .await
    .map_err(ApiError::from)?;

    let intent_id = persist_managed_ingest_intent(
        &store,
        media_type,
        &item,
        manager.detail.provider.provider_id,
        &provider_label(&manager),
        manager_item_id.as_deref(),
    )
    .await
    .map_err(ApiError::from)?;

    let response = FindMediaAddResponse {
        operation_id: Uuid::new_v4(),
        intent_id,
        manager_provider_id: manager.detail.provider.provider_id,
        manager_label: provider_label(&manager),
        media_type: media_type_api_name(media_type).to_string(),
        title,
        manager_item_id,
    };

    info!(
        media_type = %response.media_type,
        title = %response.title,
        manager_provider_id = %response.manager_provider_id,
        manager_label = %response.manager_label,
        manager_item_id = ?response.manager_item_id,
        "find media add completed"
    );

    Ok((StatusCode::OK, Json(response)).into_response())
}

async fn persist_managed_ingest_intent(
    store: &ExtensionStore<'_>,
    media_type: MediaType,
    item: &FindMediaAddItem,
    manager_provider_id: Uuid,
    manager_label: &str,
    manager_item_id: Option<&str>,
) -> AnyResult<Uuid> {
    let title = item
        .title
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("item.title is required for managed ingest intent"))?;

    let normalized_title = normalize_name(title);
    if normalized_title.is_empty() {
        bail!("item.title is required for managed ingest intent");
    }

    let intent_id = store
        .upsert_managed_ingest_intent(&NewManagedIngestIntent {
            media_type,
            title: title.to_string(),
            normalized_title,
            year: item.year,
            external_ids: item.external_ids.clone(),
            manager_provider_id,
            manager_item_id: manager_item_id.map(str::to_string),
            manager_label: Some(manager_label.to_string()),
            source: "find_media_add".to_string(),
        })
        .await?;

    debug!(
        media_type = ?media_type,
        title = %title,
        manager_provider_id = %manager_provider_id,
        manager_label = %manager_label,
        manager_item_id = ?manager_item_id,
        "managed ingest intent persisted"
    );

    Ok(intent_id)
}

const ACQUISITION_RECENT_WINDOW_HOURS: i64 = 6;

async fn build_find_media_acquisition_response(
    state: &AppState,
    store: &ExtensionStore<'_>,
    limit: usize,
) -> AnyResult<FindMediaAcquisitionResponse> {
    let provider_contexts = load_provider_contexts(store).await?;
    let provider_map: HashMap<Uuid, ProviderContext> = provider_contexts
        .into_iter()
        .map(|provider| (provider.detail.provider.provider_id, provider))
        .collect();
    let downloader_totals = load_acquisition_downloader_totals(state, store).await?;
    let recent_cutoff = Utc::now() - ChronoDuration::hours(ACQUISITION_RECENT_WINDOW_HOURS);

    let mut items = Vec::new();
    for intent in store.list_active_managed_ingest_intents().await? {
        let item = build_find_media_acquisition_item(state, store, &provider_map, &intent).await?;
        if item.stage == AcquisitionStage::Ready.as_str() {
            let reference = item.last_matched_at.unwrap_or(item.updated_at);
            if reference < recent_cutoff {
                continue;
            }
        }
        items.push(item);
    }

    items.sort_by(|left, right| {
        let left_stage = acquisition_stage_from_str(&left.stage);
        let right_stage = acquisition_stage_from_str(&right.stage);
        left_stage
            .sort_priority()
            .cmp(&right_stage.sort_priority())
            .then_with(|| right.updated_at.cmp(&left.updated_at))
    });

    let mut active_count = 0usize;
    let mut downloading_count = 0usize;
    let mut needs_attention_count = 0usize;
    let mut recent_completed_count = 0usize;
    for item in &items {
        let stage = acquisition_stage_from_str(&item.stage);
        if stage.is_active() {
            active_count += 1;
        }
        if stage == AcquisitionStage::Downloading {
            downloading_count += 1;
        }
        if matches!(stage, AcquisitionStage::NeedsAttention | AcquisitionStage::Failed) {
            needs_attention_count += 1;
        }
        if stage == AcquisitionStage::Ready {
            recent_completed_count += 1;
        }
    }

    if items.len() > limit {
        items.truncate(limit);
    }

    Ok(FindMediaAcquisitionResponse {
        updated_at: Utc::now(),
        active_count,
        downloading_count,
        needs_attention_count,
        recent_completed_count,
        total_download_rate_bps: downloader_totals.total_download_rate_bps,
        total_upload_rate_bps: downloader_totals.total_upload_rate_bps,
        items,
    })
}

async fn build_find_media_acquisition_item(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider_map: &HashMap<Uuid, ProviderContext>,
    intent: &crate::extensions::store::ManagedIngestIntent,
) -> AnyResult<FindMediaAcquisitionItem> {
    let manager_label = provider_map
        .get(&intent.manager_provider_id)
        .map(provider_label)
        .or_else(|| intent.manager_label.clone())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "Manager".to_string());
    let state_view =
        resolve_acquisition_item_state(state, store, provider_map.get(&intent.manager_provider_id), intent)
            .await?;

    Ok(FindMediaAcquisitionItem {
        intent_id: intent.intent_id,
        title: intent.title.clone(),
        media_type: media_type_api_name(intent.media_type).to_string(),
        year: intent.year,
        external_ids: intent.external_ids.clone(),
        manager_provider_id: intent.manager_provider_id,
        manager_label,
        manager_item_id: intent.manager_item_id.clone(),
        source: intent.source.clone(),
        stage: state_view.stage.as_str().to_string(),
        stage_label: state_view.stage.label().to_string(),
        description: state_view.description,
        progress_percent: state_view.progress_percent,
        eta_seconds: state_view.eta_seconds,
        downloader_label: state_view.downloader_label,
        protocol: state_view.protocol,
        last_matched_at: intent.last_matched_at,
        created_at: intent.created_at,
        updated_at: intent.updated_at,
    })
}

async fn resolve_acquisition_item_state(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider: Option<&ProviderContext>,
    intent: &crate::extensions::store::ManagedIngestIntent,
) -> AnyResult<AcquisitionItemState> {
    if intent.last_matched_at.is_some() {
        return Ok(AcquisitionItemState {
            stage: AcquisitionStage::Ready,
            description: "Imported and matched in the library.".to_string(),
            progress_percent: Some(100.0),
            eta_seconds: None,
            downloader_label: None,
            protocol: None,
        });
    }

    let Some(provider) = provider else {
        return Ok(acquisition_attention(
            "Selected manager is no longer available.",
        ));
    };

    if provider.detail.provider.health_state == ProviderHealthState::Unhealthy {
        return Ok(acquisition_attention("Selected manager is currently unavailable."));
    }

    let implementation = provider
        .detail
        .provider
        .implementation
        .as_deref()
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_default();
    if !matches!(implementation.as_str(), "sonarr" | "radarr") {
        return Ok(AcquisitionItemState {
            stage: AcquisitionStage::Requested,
            description: "Waiting for manager status.".to_string(),
            progress_percent: None,
            eta_seconds: None,
            downloader_label: None,
            protocol: None,
        });
    }

    let Some(manager_item_id) = intent.manager_item_id.as_deref() else {
        return Ok(AcquisitionItemState {
            stage: AcquisitionStage::Requested,
            description: "Request accepted. Waiting for manager confirmation.".to_string(),
            progress_percent: None,
            eta_seconds: None,
            downloader_label: None,
            protocol: None,
        });
    };

    let endpoint_json = provider
        .detail
        .provider
        .endpoint_json
        .clone()
        .ok_or_else(|| anyhow::anyhow!("provider endpoint is missing"))?;
    let endpoint: ProviderEndpoint =
        serde_json::from_value(endpoint_json).context("parsing provider endpoint")?;
    let base_url =
        resolve_provider_transport_base_url(provider.detail.provider.instance_id, &endpoint)
            .await?;
    let api_key = resolve_arr_api_key(state, store, provider, &implementation).await?;

    let item_value = match request_arr_json_with_query(
        &base_url,
        &api_key,
        &manager_item_paths(&implementation, manager_item_id),
        &[],
    )
    .await
    {
        Ok(value) => value,
        Err(err) => {
            return Ok(acquisition_attention(format!(
                "Manager status could not be loaded: {}",
                err
            )));
        }
    };

    let queue_value = request_arr_json_with_query(
        &base_url,
        &api_key,
        &manager_queue_paths(&implementation),
        &[("page", "1".to_string()), ("pageSize", "250".to_string())],
    )
    .await
    .ok();

    Ok(derive_arr_acquisition_state(
        &implementation,
        manager_item_id,
        &item_value,
        queue_value.as_ref(),
    ))
}

async fn load_acquisition_downloader_totals(
    state: &AppState,
    store: &ExtensionStore<'_>,
) -> AnyResult<AcquisitionDownloaderTotals> {
    let providers = store.list_provider_details().await?;
    let instances = store.list_instances(None).await?;
    let instance_map: HashMap<Uuid, crate::db::models::ExtensionInstance> = instances
        .into_iter()
        .map(|instance| (instance.instance_id, instance))
        .collect();

    let mut total_download_rate = 0u64;
    let mut total_upload_rate = 0u64;
    let mut has_download_rate = false;
    let mut has_upload_rate = false;

    for detail in providers {
        if detail.provider.capability != "downloader.torrent"
            && detail.provider.capability != "downloader.nzb"
        {
            continue;
        }
        if detail.provider.health_state == ProviderHealthState::Unhealthy {
            continue;
        }
        let Some(instance) = instance_map.get(&detail.provider.instance_id) else {
            continue;
        };
        let Ok(snapshot) = state
            .orchestrator
            .read_provider_state(&detail.provider, instance)
            .await
        else {
            continue;
        };
        let Some(activity) = snapshot.activity else {
            continue;
        };
        if let Some(rate) = activity.download_rate_bps {
            total_download_rate = total_download_rate.saturating_add(rate);
            has_download_rate = true;
        }
        if let Some(rate) = activity.upload_rate_bps {
            total_upload_rate = total_upload_rate.saturating_add(rate);
            has_upload_rate = true;
        }
    }

    Ok(AcquisitionDownloaderTotals {
        total_download_rate_bps: has_download_rate.then_some(total_download_rate),
        total_upload_rate_bps: has_upload_rate.then_some(total_upload_rate),
    })
}

fn acquisition_attention(message: impl Into<String>) -> AcquisitionItemState {
    AcquisitionItemState {
        stage: AcquisitionStage::NeedsAttention,
        description: message.into(),
        progress_percent: None,
        eta_seconds: None,
        downloader_label: None,
        protocol: None,
    }
}

fn acquisition_stage_from_str(value: &str) -> AcquisitionStage {
    match value {
        "requested" => AcquisitionStage::Requested,
        "searching" => AcquisitionStage::Searching,
        "queued" => AcquisitionStage::Queued,
        "downloading" => AcquisitionStage::Downloading,
        "post_processing" => AcquisitionStage::PostProcessing,
        "importing" => AcquisitionStage::Importing,
        "ready" => AcquisitionStage::Ready,
        "needs_attention" => AcquisitionStage::NeedsAttention,
        "failed" => AcquisitionStage::Failed,
        _ => AcquisitionStage::Requested,
    }
}

fn manager_item_paths(implementation: &str, manager_item_id: &str) -> [String; 2] {
    match implementation {
        "sonarr" => [
            format!("api/v3/series/{manager_item_id}"),
            format!("api/v4/series/{manager_item_id}"),
        ],
        "radarr" => [
            format!("api/v3/movie/{manager_item_id}"),
            format!("api/v4/movie/{manager_item_id}"),
        ],
        _ => [String::new(), String::new()],
    }
}

fn manager_queue_paths(implementation: &str) -> [&str; 2] {
    match implementation {
        "sonarr" => ["api/v3/queue", "api/v4/queue"],
        "radarr" => ["api/v3/queue", "api/v4/queue"],
        _ => ["", ""],
    }
}

fn derive_arr_acquisition_state(
    implementation: &str,
    manager_item_id: &str,
    item_value: &Value,
    queue_value: Option<&Value>,
) -> AcquisitionItemState {
    let queue_entries: Vec<Value> = queue_value
        .map(extract_arr_queue_records)
        .unwrap_or_default()
        .into_iter()
        .filter(|entry| queue_entry_matches_manager_item(entry, implementation, manager_item_id))
        .collect();

    let has_file = match implementation {
        "sonarr" => sonarr_item_has_files(item_value),
        "radarr" => radarr_item_has_file(item_value),
        _ => false,
    };

    if let Some(message) = queue_entries
        .iter()
        .find_map(|entry| queue_entry_error_message(entry))
    {
        return acquisition_attention(message);
    }

    if has_file {
        return AcquisitionItemState {
            stage: AcquisitionStage::Importing,
            description: "Manager has imported files. Waiting for library scan.".to_string(),
            progress_percent: Some(100.0),
            eta_seconds: None,
            downloader_label: queue_entries
                .first()
                .and_then(queue_entry_downloader_label),
            protocol: queue_entries.first().and_then(queue_entry_protocol),
        };
    }

    if let Some(entry) = queue_entries.first() {
        let progress_percent = queue_entry_progress_percent(entry);
        let eta_seconds = queue_entry_eta_seconds(entry);
        let downloader_label = queue_entry_downloader_label(entry);
        let protocol = queue_entry_protocol(entry);
        let tracked_state = queue_entry_state(entry);
        let stage = if tracked_state.contains("import") {
            AcquisitionStage::Importing
        } else if tracked_state.contains("post")
            || tracked_state.contains("extract")
            || tracked_state.contains("verif")
            || progress_percent
                .map(|value| value >= 99.5)
                .unwrap_or(false)
        {
            AcquisitionStage::PostProcessing
        } else if tracked_state.contains("download")
            || progress_percent.map(|value| value > 0.0).unwrap_or(false)
        {
            AcquisitionStage::Downloading
        } else {
            AcquisitionStage::Queued
        };

        let description = match stage {
            AcquisitionStage::Downloading => {
                if let Some(label) = downloader_label.as_deref() {
                    format!("Downloading via {label}.")
                } else {
                    "Download in progress.".to_string()
                }
            }
            AcquisitionStage::PostProcessing => {
                "Download finished. Waiting for post-processing.".to_string()
            }
            AcquisitionStage::Importing => {
                "Manager is importing the completed download.".to_string()
            }
            _ => {
                if let Some(label) = downloader_label.as_deref() {
                    format!("Queued with {label}.")
                } else {
                    "Waiting in the download queue.".to_string()
                }
            }
        };

        return AcquisitionItemState {
            stage,
            description,
            progress_percent,
            eta_seconds,
            downloader_label,
            protocol,
        };
    }

    AcquisitionItemState {
        stage: AcquisitionStage::Searching,
        description: "Manager accepted the item and is searching for releases.".to_string(),
        progress_percent: None,
        eta_seconds: None,
        downloader_label: None,
        protocol: None,
    }
}

fn extract_arr_queue_records(value: &Value) -> Vec<Value> {
    if let Some(items) = value.as_array() {
        return items.clone();
    }
    value
        .get("records")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

fn queue_entry_matches_manager_item(entry: &Value, implementation: &str, manager_item_id: &str) -> bool {
    let direct_id = match implementation {
        "sonarr" => entry
            .get("seriesId")
            .or_else(|| entry.get("series").and_then(|value| value.get("id"))),
        "radarr" => entry
            .get("movieId")
            .or_else(|| entry.get("movie").and_then(|value| value.get("id"))),
        _ => None,
    };
    as_id_string(direct_id.unwrap_or(&Value::Null))
        .map(|value| value == manager_item_id)
        .unwrap_or(false)
}

fn sonarr_item_has_files(value: &Value) -> bool {
    value
        .get("statistics")
        .and_then(|statistics| statistics.get("episodeFileCount"))
        .and_then(Value::as_i64)
        .map(|count| count > 0)
        .unwrap_or(false)
        || value
            .get("statistics")
            .and_then(|statistics| statistics.get("sizeOnDisk"))
            .and_then(Value::as_u64)
            .map(|size| size > 0)
            .unwrap_or(false)
}

fn radarr_item_has_file(value: &Value) -> bool {
    value.get("hasFile").and_then(Value::as_bool).unwrap_or(false)
        || value
            .get("movieFileId")
            .and_then(Value::as_i64)
            .map(|id| id > 0)
            .unwrap_or(false)
        || value
            .get("sizeOnDisk")
            .and_then(Value::as_u64)
            .map(|size| size > 0)
            .unwrap_or(false)
}

fn queue_entry_state(entry: &Value) -> String {
    let mut parts = Vec::new();
    for key in ["trackedDownloadState", "trackedDownloadStatus", "status"] {
        if let Some(value) = entry.get(key).and_then(Value::as_str) {
            let value = value.trim().to_ascii_lowercase();
            if !value.is_empty() {
                parts.push(value);
            }
        }
    }
    parts.join(" ")
}

fn queue_entry_error_message(entry: &Value) -> Option<String> {
    if let Some(value) = entry.get("errorMessage").and_then(Value::as_str) {
        let value = value.trim();
        if !value.is_empty() {
            return Some(value.to_string());
        }
    }
    let tracked_status = entry
        .get("trackedDownloadStatus")
        .and_then(Value::as_str)
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_default();
    if tracked_status.contains("warning") || tracked_status.contains("error") {
        return Some("Downloader reported a problem for this item.".to_string());
    }
    None
}

fn queue_entry_progress_percent(entry: &Value) -> Option<f64> {
    let size = entry.get("size").and_then(value_as_f64)?;
    if size <= 0.0 {
        return None;
    }
    let size_left = entry
        .get("sizeleft")
        .or_else(|| entry.get("sizeLeft"))
        .and_then(value_as_f64)
        .unwrap_or(size);
    let progress = ((size - size_left).max(0.0) / size) * 100.0;
    Some(progress.clamp(0.0, 100.0))
}

fn queue_entry_eta_seconds(entry: &Value) -> Option<i64> {
    if let Some(value) = entry
        .get("estimatedCompletionTime")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if let Ok(parsed) = DateTime::parse_from_rfc3339(value) {
            let eta = parsed.with_timezone(&Utc) - Utc::now();
            return Some(eta.num_seconds().max(0));
        }
    }
    if let Some(value) = entry
        .get("timeleft")
        .or_else(|| entry.get("timeLeft"))
        .and_then(Value::as_str)
    {
        return parse_arr_duration_seconds(value);
    }
    None
}

fn queue_entry_downloader_label(entry: &Value) -> Option<String> {
    entry.get("downloadClient")
        .or_else(|| entry.get("downloadClientName"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn queue_entry_protocol(entry: &Value) -> Option<String> {
    entry.get("protocol")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn parse_arr_duration_seconds(value: &str) -> Option<i64> {
    let mut parts = value
        .trim()
        .split(':')
        .filter_map(|item| item.trim().parse::<i64>().ok())
        .collect::<Vec<_>>();
    if parts.is_empty() {
        return None;
    }
    while parts.len() < 3 {
        parts.insert(0, 0);
    }
    Some(parts[0] * 3600 + parts[1] * 60 + parts[2])
}

pub async fn find_media_preferences(
    State(state): State<AppState>,
    _user: CurrentUser,
) -> ApiResult<Json<FindMediaPreferencesResponse>> {
    let store = ExtensionStore::new(&state.db_pool);
    let providers = load_provider_contexts(&store)
        .await
        .map_err(ApiError::from)?;
    let preferences = load_manager_preferences(&store, &providers)
        .await
        .map_err(ApiError::from)?;

    Ok(Json(FindMediaPreferencesResponse {
        preferences: FindMediaPreferencesState {
            tv_default_manager_provider_id: preferences.series_provider_id,
            movies_default_manager_provider_id: preferences.movie_provider_id,
            anime_default_manager_provider_id: preferences.anime_provider_id,
        },
        tv_manager_candidates: collect_manager_providers(&providers, MediaType::Series)
            .iter()
            .map(provider_summary)
            .collect(),
        movies_manager_candidates: collect_manager_providers(&providers, MediaType::Movie)
            .iter()
            .map(provider_summary)
            .collect(),
        anime_manager_candidates: collect_manager_providers(&providers, MediaType::Anime)
            .iter()
            .map(provider_summary)
            .collect(),
    }))
}

pub async fn patch_find_media_preferences(
    State(state): State<AppState>,
    _user: CurrentUser,
    Json(payload): Json<PatchFindMediaPreferencesRequest>,
) -> ApiResult<Json<FindMediaPreferencesResponse>> {
    let store = ExtensionStore::new(&state.db_pool);
    let providers = load_provider_contexts(&store)
        .await
        .map_err(ApiError::from)?;

    if let Some(value) = parse_provider_patch_value(payload.movies_default_manager_provider_id)? {
        validate_manager_preference_provider(value, MediaType::Movie, &providers)?;
        save_manager_preference(&store, MediaType::Movie, value)
            .await
            .map_err(ApiError::from)?;
    }
    if let Some(value) = parse_provider_patch_value(payload.tv_default_manager_provider_id)? {
        validate_manager_preference_provider(value, MediaType::Series, &providers)?;
        save_manager_preference(&store, MediaType::Series, value)
            .await
            .map_err(ApiError::from)?;
    }
    if let Some(value) = parse_provider_patch_value(payload.anime_default_manager_provider_id)? {
        validate_manager_preference_provider(value, MediaType::Anime, &providers)?;
        save_manager_preference(&store, MediaType::Anime, value)
            .await
            .map_err(ApiError::from)?;
    }

    let preferences = load_manager_preferences(&store, &providers)
        .await
        .map_err(ApiError::from)?;

    Ok(Json(FindMediaPreferencesResponse {
        preferences: FindMediaPreferencesState {
            tv_default_manager_provider_id: preferences.series_provider_id,
            movies_default_manager_provider_id: preferences.movie_provider_id,
            anime_default_manager_provider_id: preferences.anime_provider_id,
        },
        tv_manager_candidates: collect_manager_providers(&providers, MediaType::Series)
            .iter()
            .map(provider_summary)
            .collect(),
        movies_manager_candidates: collect_manager_providers(&providers, MediaType::Movie)
            .iter()
            .map(provider_summary)
            .collect(),
        anime_manager_candidates: collect_manager_providers(&providers, MediaType::Anime)
            .iter()
            .map(provider_summary)
            .collect(),
    }))
}

pub async fn manager_preferences(
    State(state): State<AppState>,
    _user: CurrentUser,
) -> ApiResult<Json<ManagerPreferencesResponse>> {
    let store = ExtensionStore::new(&state.db_pool);
    let providers = load_provider_contexts(&store)
        .await
        .map_err(ApiError::from)?;
    let preferences = load_manager_preferences(&store, &providers)
        .await
        .map_err(ApiError::from)?;

    Ok(Json(ManagerPreferencesResponse {
        preferences,
        movie_providers: collect_manager_providers(&providers, MediaType::Movie)
            .iter()
            .map(provider_summary)
            .collect(),
        series_providers: collect_manager_providers(&providers, MediaType::Series)
            .iter()
            .map(provider_summary)
            .collect(),
        anime_providers: collect_manager_providers(&providers, MediaType::Anime)
            .iter()
            .map(provider_summary)
            .collect(),
    }))
}

pub async fn update_manager_preferences(
    State(state): State<AppState>,
    _user: CurrentUser,
    Json(payload): Json<UpdateManagerPreferencesRequest>,
) -> ApiResult<Json<ManagerPreferencesResponse>> {
    let store = ExtensionStore::new(&state.db_pool);
    let providers = load_provider_contexts(&store)
        .await
        .map_err(ApiError::from)?;

    if let Some(value) = parse_provider_patch_value(payload.movie_provider_id)? {
        validate_manager_preference_provider(value, MediaType::Movie, &providers)?;
        save_manager_preference(&store, MediaType::Movie, value)
            .await
            .map_err(ApiError::from)?;
    }
    if let Some(value) = parse_provider_patch_value(payload.series_provider_id)? {
        validate_manager_preference_provider(value, MediaType::Series, &providers)?;
        save_manager_preference(&store, MediaType::Series, value)
            .await
            .map_err(ApiError::from)?;
    }
    if let Some(value) = parse_provider_patch_value(payload.anime_provider_id)? {
        validate_manager_preference_provider(value, MediaType::Anime, &providers)?;
        save_manager_preference(&store, MediaType::Anime, value)
            .await
            .map_err(ApiError::from)?;
    }

    manager_preferences(State(state), _user).await
}

fn parse_media_type(value: Option<&str>) -> Option<MediaType> {
    value.and_then(|value| match value.trim().to_ascii_lowercase().as_str() {
        "movie" | "movies" => Some(MediaType::Movie),
        "series" | "tv" => Some(MediaType::Series),
        "anime" => Some(MediaType::Anime),
        _ => None,
    })
}

fn media_type_api_name(media_type: MediaType) -> &'static str {
    match media_type {
        MediaType::Movie => "movies",
        MediaType::Series => "tv",
        MediaType::Anime => "anime",
    }
}

fn parse_provider_id(value: Option<&str>) -> ApiResult<Option<Uuid>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    let provider_id =
        Uuid::parse_str(value).map_err(|_| ApiError::bad_request("invalid provider id"))?;
    Ok(Some(provider_id))
}

fn parse_provider_ids(values: &[String]) -> ApiResult<Vec<Uuid>> {
    let mut out = Vec::new();
    for value in values {
        let provider_id = Uuid::parse_str(value.trim())
            .map_err(|_| ApiError::bad_request("invalid provider id"))?;
        if !out.contains(&provider_id) {
            out.push(provider_id);
        }
    }
    Ok(out)
}

fn parse_provider_patch_value(value: Option<Value>) -> ApiResult<Option<Option<Uuid>>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(Some(None));
    }
    let Some(text) = value.as_str() else {
        return Err(ApiError::bad_request(
            "provider id must be a string or null",
        ));
    };
    let parsed = parse_provider_id(Some(text))?;
    Ok(Some(parsed))
}

fn provider_summary(provider: &ProviderContext) -> ProviderSummary {
    ProviderSummary {
        provider_id: provider.detail.provider.provider_id,
        extension_id: provider.detail.extension_id.clone(),
        instance_id: provider.detail.provider.instance_id,
        instance_name: provider.instance_name.clone(),
        capability: provider.detail.provider.capability.clone(),
        implementation: provider.detail.provider.implementation.clone(),
        health_state: provider.detail.provider.health_state,
        media_types: provider
            .media_types
            .iter()
            .map(|item| media_type_name(*item).to_string())
            .collect(),
        label: provider_label(provider),
    }
}

#[derive(Debug, Clone)]
struct ProviderFilterError {
    provider: ProviderContext,
    message: String,
}

fn provider_label(provider: &ProviderContext) -> String {
    let implementation = provider
        .detail
        .provider
        .implementation
        .as_deref()
        .unwrap_or("provider");
    format!(
        "{} ({})",
        provider.instance_name,
        implementation.trim().to_ascii_lowercase()
    )
}

fn media_type_name(media_type: MediaType) -> &'static str {
    match media_type {
        MediaType::Movie => "movie",
        MediaType::Series => "series",
        MediaType::Anime => "anime",
    }
}

fn preferred_manager_for_type(
    preferences: &ManagerPreferenceState,
    media_type: MediaType,
) -> Option<Uuid> {
    match media_type {
        MediaType::Movie => preferences.movie_provider_id,
        MediaType::Series => preferences.series_provider_id,
        MediaType::Anime => preferences.anime_provider_id,
    }
}

fn resolve_default_manager(
    preferred: Option<Uuid>,
    blueprint_preferred: Option<Uuid>,
    providers: &[ProviderContext],
) -> Option<Uuid> {
    if let Some(preferred) = preferred {
        if providers
            .iter()
            .any(|provider| provider.detail.provider.provider_id == preferred)
        {
            return Some(preferred);
        }
    }
    if let Some(blueprint_preferred) = blueprint_preferred {
        if providers
            .iter()
            .any(|provider| provider.detail.provider.provider_id == blueprint_preferred)
        {
            return Some(blueprint_preferred);
        }
    }
    let mut ordered = providers.to_vec();
    ordered.sort_by(compare_manager_candidates);
    let Some(first) = ordered.first() else {
        return None;
    };
    if preferred.is_none() && blueprint_preferred.is_none() {
        let top_rank = trust_rank(first.detail.trust_level);
        let ambiguous = ordered
            .iter()
            .filter(|provider| trust_rank(provider.detail.trust_level) == top_rank)
            .take(2)
            .count();
        if ambiguous > 1 {
            return None;
        }
    }
    Some(first.detail.provider.provider_id)
}

fn collect_manager_providers(
    providers: &[ProviderContext],
    media_type: MediaType,
) -> Vec<ProviderContext> {
    let mut out: Vec<_> = providers
        .iter()
        .filter(|provider| is_manager_capability(&provider.detail.provider.capability))
        .filter(|provider| provider.media_types.contains(&media_type))
        .filter(|provider| provider_supports_action(provider, "add"))
        .filter(|provider| provider.detail.provider.health_state != ProviderHealthState::Unhealthy)
        .cloned()
        .collect();
    out.sort_by(compare_manager_candidates);
    out
}

fn collect_search_providers(
    providers: &[ProviderContext],
    media_type: MediaType,
) -> Vec<ProviderContext> {
    let mut candidates: Vec<_> = providers
        .iter()
        .filter(|provider| provider.media_types.contains(&media_type))
        .filter(|provider| provider.detail.provider.health_state != ProviderHealthState::Unhealthy)
        .filter(|provider| {
            provider
                .detail
                .provider
                .capability
                .starts_with("media.search.")
        })
        .filter(|provider| provider_supports_action(provider, "search"))
        .cloned()
        .collect();

    candidates.sort_by(|left, right| {
        let by_extension = left.detail.extension_id.cmp(&right.detail.extension_id);
        if by_extension == std::cmp::Ordering::Equal {
            left.detail
                .provider
                .provider_id
                .cmp(&right.detail.provider.provider_id)
        } else {
            by_extension
        }
    });

    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for provider in candidates {
        let key = (
            provider.detail.provider.instance_id,
            provider.detail.provider.implementation.clone(),
        );
        if seen.insert(key) {
            out.push(provider);
        }
    }
    out
}

fn parse_provider_scope(capability: &str, scope_json: Option<&Value>) -> ProviderScopeDocument {
    let mut scope = scope_json
        .and_then(|value| serde_json::from_value::<ProviderScopeDocument>(value.clone()).ok())
        .unwrap_or_default();

    if scope.media_types.is_empty() {
        scope.media_types = infer_media_types_from_capability(capability)
            .iter()
            .map(|value| media_type_name(*value).to_string())
            .collect();
    } else {
        let mut normalized = Vec::new();
        for media_type in &scope.media_types {
            if let Some(parsed) = parse_media_type(Some(media_type)) {
                let name = media_type_name(parsed).to_string();
                if !normalized.contains(&name) {
                    normalized.push(name);
                }
            }
        }
        scope.media_types = normalized;
    }

    if scope.actions.is_empty() {
        scope.actions = infer_actions_from_capability(capability)
            .into_iter()
            .map(str::to_string)
            .collect();
    } else {
        let mut normalized = Vec::new();
        for action in &scope.actions {
            let action = action.trim().to_ascii_lowercase();
            if !action.is_empty() && !normalized.contains(&action) {
                normalized.push(action);
            }
        }
        scope.actions = normalized;
    }

    if scope.requires_account && scope.required_fields.is_empty() {
        scope.required_fields.push("api_key".to_string());
    }

    scope
}

fn parse_scope_media_types(scope: &ProviderScopeDocument) -> Vec<MediaType> {
    let mut out = Vec::new();
    for media_type in &scope.media_types {
        if let Some(parsed) = parse_media_type(Some(media_type)) {
            if !out.contains(&parsed) {
                out.push(parsed);
            }
        }
    }
    out
}

fn infer_media_types_from_capability(capability: &str) -> Vec<MediaType> {
    match capability.trim().to_ascii_lowercase().as_str() {
        "media.manager.movies" => vec![MediaType::Movie],
        "media.manager.tv" => vec![MediaType::Series, MediaType::Anime],
        "media.manager.anime" => vec![MediaType::Anime],
        "media.search.movie" | "media.search.movies" => vec![MediaType::Movie],
        "media.search.series" | "media.search.tv" => vec![MediaType::Series],
        "media.search.anime" => vec![MediaType::Anime],
        _ => Vec::new(),
    }
}

fn infer_actions_from_capability(capability: &str) -> Vec<&'static str> {
    match capability.trim().to_ascii_lowercase().as_str() {
        "media.manager.movies" | "media.manager.tv" | "media.manager.anime" => {
            vec!["add", "monitor", "search"]
        }
        value if value.starts_with("media.search.") => vec!["search"],
        _ => Vec::new(),
    }
}

fn provider_supports_action(provider: &ProviderContext, action: &str) -> bool {
    let action = action.trim().to_ascii_lowercase();
    provider
        .scope
        .actions
        .iter()
        .any(|value| value.trim().eq_ignore_ascii_case(&action))
}

fn compare_manager_candidates(
    left: &ProviderContext,
    right: &ProviderContext,
) -> std::cmp::Ordering {
    let by_trust = trust_rank(left.detail.trust_level).cmp(&trust_rank(right.detail.trust_level));
    if by_trust != std::cmp::Ordering::Equal {
        return by_trust;
    }
    let by_extension = left.detail.extension_id.cmp(&right.detail.extension_id);
    if by_extension == std::cmp::Ordering::Equal {
        left.detail
            .provider
            .provider_id
            .cmp(&right.detail.provider.provider_id)
    } else {
        by_extension
    }
}

fn trust_rank(level: ExtensionTrustLevel) -> i32 {
    match level {
        ExtensionTrustLevel::Verified => 0,
        ExtensionTrustLevel::Community => 1,
        ExtensionTrustLevel::Untrusted => 2,
    }
}

fn is_manager_capability(capability: &str) -> bool {
    matches!(
        capability.trim().to_ascii_lowercase().as_str(),
        "media.manager.movies" | "media.manager.tv" | "media.manager.anime"
    )
}

#[derive(Debug)]
enum ManagerSelection {
    Selected(ProviderContext),
    Conflict(FindMediaConflict),
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct FindMediaConflict {
    code: String,
    message: String,
    details: Value,
}

impl IntoResponse for FindMediaConflict {
    fn into_response(self) -> Response {
        (StatusCode::CONFLICT, Json(self)).into_response()
    }
}

fn conflict_response(code: &str, message: impl Into<String>, details: Value) -> Response {
    FindMediaConflict {
        code: code.to_string(),
        message: message.into(),
        details,
    }
    .into_response()
}

fn conflict(code: &str, message: impl Into<String>, details: Value) -> FindMediaConflict {
    FindMediaConflict {
        code: code.to_string(),
        message: message.into(),
        details,
    }
}

async fn execute_find_media_search(
    state: &AppState,
    query: String,
    media_type: MediaType,
    requested_provider_ids: Vec<Uuid>,
) -> ApiResult<FindMediaResponse> {
    let store = ExtensionStore::new(&state.db_pool);
    let providers = load_provider_contexts(&store)
        .await
        .map_err(ApiError::from)?;
    let preferences = load_manager_preferences(&store, &providers)
        .await
        .map_err(ApiError::from)?;

    let (search_contexts, search_errors) = filter_search_providers(
        state,
        &store,
        collect_search_providers(&providers, media_type),
    )
    .await
    .map_err(ApiError::from)?;
    let (manager_contexts, _manager_errors) = filter_manager_providers(
        state,
        &store,
        collect_manager_providers(&providers, media_type),
    )
    .await
    .map_err(ApiError::from)?;

    let mut provider_errors: Vec<ProviderSearchError> = search_errors
        .into_iter()
        .map(|error| ProviderSearchError {
            provider_id: error.provider.detail.provider.provider_id,
            message: error.message,
        })
        .collect();

    let search_providers: Vec<ProviderSummary> =
        search_contexts.iter().map(provider_summary).collect();
    let manager_providers: Vec<ProviderSummary> =
        manager_contexts.iter().map(provider_summary).collect();

    let preferred_manager_provider_id = preferred_manager_for_type(&preferences, media_type);
    let blueprint_preferred =
        resolve_blueprint_preferred_manager(&store, &manager_contexts, media_type)
            .await
            .map_err(ApiError::from)?;
    let default_manager_provider_id = resolve_default_manager(
        preferred_manager_provider_id,
        blueprint_preferred,
        &manager_contexts,
    );
    let has_requested_provider_filter = !requested_provider_ids.is_empty();

    let filtered_search_contexts = if requested_provider_ids.is_empty() {
        if media_type == MediaType::Anime {
            // Anime discovery defaults to metadata providers (AniList path).
            // Provider-backed search is still available when explicitly requested.
            Vec::new()
        } else {
            search_contexts
        }
    } else {
        let mut out = Vec::new();
        for provider_id in requested_provider_ids {
            let Some(context) = search_contexts
                .iter()
                .find(|item| item.detail.provider.provider_id == provider_id)
                .cloned()
            else {
                return Err(ApiError::bad_request(
                    "provider_id is not available for this media type",
                ));
            };
            out.push(context);
        }
        out
    };

    let mut merged: BTreeMap<String, FindMediaResult> = BTreeMap::new();
    for provider in filtered_search_contexts {
        match search_with_provider(state, &store, &provider, &query, media_type).await {
            Ok(results) => {
                let label = provider_label(&provider);
                let provider_id = provider.detail.provider.provider_id;
                for result in results {
                    let key = discovery_result_key(&result);
                    let entry = merged.entry(key).or_insert_with(|| FindMediaResult {
                        title: result.title.clone(),
                        r#type: result.r#type,
                        year: result.year,
                        external_ids: result.external_ids.clone(),
                        description: result.description.clone(),
                        poster_url: result.poster_url.clone(),
                        popularity_score: result.popularity_score,
                        source_provider_ids: Vec::new(),
                        source_labels: Vec::new(),
                    });
                    if entry
                        .description
                        .as_ref()
                        .map(|text| text.trim().is_empty())
                        .unwrap_or(true)
                    {
                        entry.description = result.description.clone();
                    }
                    if entry.poster_url.is_none() {
                        entry.poster_url = result.poster_url.clone();
                    }
                    if entry.popularity_score.is_none()
                        || result.popularity_score.unwrap_or_default()
                            > entry.popularity_score.unwrap_or_default()
                    {
                        entry.popularity_score = result.popularity_score;
                    }
                    if !entry.source_provider_ids.contains(&provider_id) {
                        entry.source_provider_ids.push(provider_id);
                    }
                    if !entry.source_labels.contains(&label) {
                        entry.source_labels.push(label.clone());
                    }
                }
            }
            Err(err) => {
                provider_errors.push(ProviderSearchError {
                    provider_id: provider.detail.provider.provider_id,
                    message: err.to_string(),
                });
            }
        }
    }

    // Keep Find Media usable when manager-backed search providers are unavailable.
    if merged.is_empty() && !has_requested_provider_filter {
        const METADATA_SOURCE_LABEL: &str = "Elixir Metadata";
        if let Ok(results) = state
            .metadata
            .discovery_search(&query, Some(media_type))
            .await
        {
            for result in results {
                let key = discovery_result_key(&result);
                let entry = merged.entry(key).or_insert_with(|| FindMediaResult {
                    title: result.title.clone(),
                    r#type: result.r#type,
                    year: result.year,
                    external_ids: result.external_ids.clone(),
                    description: result.description.clone(),
                    poster_url: result.poster_url.clone(),
                    popularity_score: result.popularity_score,
                    source_provider_ids: Vec::new(),
                    source_labels: Vec::new(),
                });
                if entry
                    .description
                    .as_ref()
                    .map(|text| text.trim().is_empty())
                    .unwrap_or(true)
                {
                    entry.description = result.description.clone();
                }
                if entry.poster_url.is_none() {
                    entry.poster_url = result.poster_url.clone();
                }
                if entry.popularity_score.is_none()
                    || result.popularity_score.unwrap_or_default()
                        > entry.popularity_score.unwrap_or_default()
                {
                    entry.popularity_score = result.popularity_score;
                }
                if !entry
                    .source_labels
                    .iter()
                    .any(|label| label == METADATA_SOURCE_LABEL)
                {
                    entry.source_labels.push(METADATA_SOURCE_LABEL.to_string());
                }
            }
        }
    }

    let mut results: Vec<_> = merged.into_values().collect();
    sort_find_media_results(query.as_str(), &mut results);

    Ok(FindMediaResponse {
        query,
        media_type,
        search_providers,
        manager_providers,
        preferred_manager_provider_id,
        default_manager_provider_id,
        results,
        provider_errors,
    })
}

async fn filter_search_providers(
    state: &AppState,
    store: &ExtensionStore<'_>,
    providers: Vec<ProviderContext>,
) -> AnyResult<(Vec<ProviderContext>, Vec<ProviderFilterError>)> {
    let mut available = Vec::new();
    let mut unavailable = Vec::new();
    for provider in providers {
        let missing = missing_required_fields_for_provider(state, store, &provider).await?;
        if missing.is_empty() {
            available.push(provider);
            continue;
        }
        unavailable.push(ProviderFilterError {
            provider,
            message: format!("missing required secrets: {}", missing.join(", ")),
        });
    }
    Ok((available, unavailable))
}

async fn filter_manager_providers(
    state: &AppState,
    store: &ExtensionStore<'_>,
    providers: Vec<ProviderContext>,
) -> AnyResult<(Vec<ProviderContext>, Vec<ProviderFilterError>)> {
    filter_search_providers(state, store, providers).await
}

async fn resolve_blueprint_preferred_manager(
    store: &ExtensionStore<'_>,
    providers: &[ProviderContext],
    media_type: MediaType,
) -> AnyResult<Option<Uuid>> {
    let desired = store.list_desired_blueprints(Some(true)).await?;
    if desired.is_empty() {
        return Ok(None);
    }
    let extensions = store.list_extensions().await?;
    let extension_map: HashMap<String, _> = extensions
        .into_iter()
        .map(|extension| (extension.extension_id.clone(), extension))
        .collect();
    let preference_keys = manager_preference_keys(media_type);

    for item in desired {
        let Some(extension) = extension_map.get(&item.blueprint_extension_id) else {
            continue;
        };
        let Ok(manifest) =
            serde_json::from_value::<ExtensionManifest>(extension.manifest_json.clone())
        else {
            continue;
        };
        let Some(preferences) = manifest.preferences.as_ref() else {
            continue;
        };

        for key in &preference_keys {
            let Some(preference) = preferences.providers.get(*key) else {
                continue;
            };
            for extension_id in &preference.prefer {
                if let Some(provider) = providers
                    .iter()
                    .find(|provider| provider.detail.extension_id == *extension_id)
                {
                    return Ok(Some(provider.detail.provider.provider_id));
                }
            }
        }
    }

    Ok(None)
}

fn manager_preference_keys(media_type: MediaType) -> Vec<&'static str> {
    match media_type {
        MediaType::Movie => vec!["media.manager.movies/default"],
        MediaType::Series => vec!["media.manager.tv/default"],
        MediaType::Anime => vec!["media.manager.anime/default", "media.manager.tv/default"],
    }
}

fn discovery_result_key(result: &DiscoveryResult) -> String {
    if let Some(ids) = result.external_ids.as_ref() {
        if let Some(value) = ids.imdb.as_deref() {
            return format!("imdb:{}", value.trim().to_ascii_lowercase());
        }
        if let Some(value) = ids.tmdb.as_deref() {
            return format!("tmdb:{}", value.trim().to_ascii_lowercase());
        }
        if let Some(value) = ids.tvdb_movie.as_deref() {
            return format!("tvdb_movie:{}", value.trim().to_ascii_lowercase());
        }
        if let Some(value) = ids.tvdb_series.as_deref() {
            return format!("tvdb_series:{}", value.trim().to_ascii_lowercase());
        }
        if let Some(value) = ids.tvdb.as_deref() {
            return format!("tvdb:{}", value.trim().to_ascii_lowercase());
        }
        if let Some(value) = ids.anilist.as_deref() {
            return format!("anilist:{}", value.trim().to_ascii_lowercase());
        }
    }
    format!(
        "{}:{}",
        result.title.trim().to_ascii_lowercase(),
        result.year.unwrap_or_default()
    )
}

async fn load_provider_contexts(store: &ExtensionStore<'_>) -> AnyResult<Vec<ProviderContext>> {
    let details = store.list_provider_details().await?;
    let instances = store.list_instances(None).await?;
    let extensions = store.list_extensions().await?;

    let instance_map: HashMap<Uuid, _> = instances
        .into_iter()
        .map(|instance| (instance.instance_id, instance))
        .collect();
    let extension_map: HashMap<String, _> = extensions
        .into_iter()
        .map(|extension| (extension.extension_id.clone(), extension))
        .collect();

    let mut providers = Vec::new();
    for detail in details {
        let Some(instance) = instance_map.get(&detail.provider.instance_id) else {
            continue;
        };
        if !instance.enabled {
            continue;
        }
        let Some(extension) = extension_map.get(&detail.extension_id) else {
            continue;
        };
        if !extension.enabled {
            continue;
        }
        let scope = parse_provider_scope(
            &detail.provider.capability,
            detail.provider.scope_json.as_ref(),
        );
        let media_types = parse_scope_media_types(&scope);
        providers.push(ProviderContext {
            detail,
            instance_name: instance.instance_name.clone(),
            instance_config: instance.config_json.clone(),
            scope,
            media_types,
        });
    }
    Ok(providers)
}

async fn load_manager_preferences(
    store: &ExtensionStore<'_>,
    providers: &[ProviderContext],
) -> AnyResult<ManagerPreferenceState> {
    let movie_provider_id =
        sanitize_manager_preference(store, MANAGER_PREF_MOVIE, MediaType::Movie, providers).await?;
    let series_provider_id =
        sanitize_manager_preference(store, MANAGER_PREF_SERIES, MediaType::Series, providers)
            .await?;
    let anime_provider_id =
        sanitize_manager_preference(store, MANAGER_PREF_ANIME, MediaType::Anime, providers).await?;

    Ok(ManagerPreferenceState {
        movie_provider_id,
        series_provider_id,
        anime_provider_id,
    })
}

async fn load_manager_preference(store: &ExtensionStore<'_>, key: &str) -> AnyResult<Option<Uuid>> {
    let value = store.get_extension_setting(key).await?;
    let Some(value) = value else {
        return Ok(None);
    };
    let Some(value) = value.as_str() else {
        return Ok(None);
    };
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    let provider_id =
        Uuid::parse_str(value).with_context(|| format!("invalid manager preference '{key}'"))?;
    Ok(Some(provider_id))
}

async fn sanitize_manager_preference(
    store: &ExtensionStore<'_>,
    key: &str,
    media_type: MediaType,
    providers: &[ProviderContext],
) -> AnyResult<Option<Uuid>> {
    let provider_id = load_manager_preference(store, key).await?;
    let Some(provider_id) = provider_id else {
        return Ok(None);
    };

    let is_valid = collect_manager_providers(providers, media_type)
        .iter()
        .any(|provider| provider.detail.provider.provider_id == provider_id);
    if is_valid {
        return Ok(Some(provider_id));
    }

    store.delete_extension_setting(key).await?;
    info!(
        setting_key = key,
        media_type = media_type_api_name(media_type),
        stale_provider_id = %provider_id,
        "cleared stale manager preference"
    );
    Ok(None)
}

async fn save_manager_preference(
    store: &ExtensionStore<'_>,
    media_type: MediaType,
    provider_id: Option<Uuid>,
) -> AnyResult<()> {
    let key = match media_type {
        MediaType::Movie => MANAGER_PREF_MOVIE,
        MediaType::Series => MANAGER_PREF_SERIES,
        MediaType::Anime => MANAGER_PREF_ANIME,
    };
    let value = provider_id.map(|value| value.to_string());
    store
        .upsert_extension_setting(key, &serde_json::json!(value))
        .await
}

fn validate_manager_preference_provider(
    provider_id: Option<Uuid>,
    media_type: MediaType,
    providers: &[ProviderContext],
) -> ApiResult<()> {
    let Some(provider_id) = provider_id else {
        return Ok(());
    };
    let valid = collect_manager_providers(providers, media_type)
        .iter()
        .any(|provider| provider.detail.provider.provider_id == provider_id);
    if !valid {
        return Err(ApiError::bad_request(
            "provider is not a valid manager for the selected media type",
        ));
    }
    Ok(())
}

async fn resolve_manager_for_add(
    state: &AppState,
    store: &ExtensionStore<'_>,
    manager_contexts: &[ProviderContext],
    preferences: &ManagerPreferenceState,
    media_type: MediaType,
    explicit_manager: Option<Uuid>,
) -> AnyResult<ManagerSelection> {
    if manager_contexts.is_empty() {
        return Ok(ManagerSelection::Conflict(conflict(
            "missing_manager",
            "no compatible manager provider is available",
            json!({ "mediaType": media_type_api_name(media_type) }),
        )));
    }

    let mut missing_by_provider: HashMap<Uuid, Vec<String>> = HashMap::new();
    let mut eligible = Vec::new();
    for provider in manager_contexts {
        let missing = missing_required_fields_for_provider(state, store, provider).await?;
        if missing.is_empty() {
            eligible.push(provider.clone());
        } else {
            missing_by_provider.insert(provider.detail.provider.provider_id, missing);
        }
    }

    let blueprint_preferred =
        resolve_blueprint_preferred_manager(store, manager_contexts, media_type).await?;
    let user_preferred = preferred_manager_for_type(preferences, media_type);
    let preferred_manager = explicit_manager.or(user_preferred).or(blueprint_preferred);

    if let Some(provider_id) = preferred_manager {
        if let Some(missing) = missing_by_provider.get(&provider_id) {
            return Ok(ManagerSelection::Conflict(conflict(
                "missing_required_secrets",
                "selected manager is missing required secrets",
                json!({
                    "providerId": provider_id,
                    "missing": missing,
                }),
            )));
        }
        if let Some(provider) = eligible
            .iter()
            .find(|provider| provider.detail.provider.provider_id == provider_id)
        {
            return Ok(ManagerSelection::Selected(provider.clone()));
        }
        return Ok(ManagerSelection::Conflict(conflict(
            "missing_manager",
            "selected manager is not available for this media type",
            json!({
                "providerId": provider_id,
                "mediaType": media_type_api_name(media_type),
            }),
        )));
    }

    if eligible.is_empty() {
        let missing = missing_by_provider
            .into_iter()
            .flat_map(|(provider_id, keys)| {
                keys.into_iter()
                    .map(move |key| format!("provider:{provider_id}:{key}"))
            })
            .collect::<Vec<_>>();
        return Ok(ManagerSelection::Conflict(conflict(
            "missing_required_secrets",
            "all manager providers are missing required secrets",
            json!({ "missing": missing }),
        )));
    }

    eligible.sort_by(compare_manager_candidates);
    if explicit_manager.is_none() && user_preferred.is_none() && blueprint_preferred.is_none() {
        let top_rank = trust_rank(eligible[0].detail.trust_level);
        let ambiguous: Vec<_> = eligible
            .iter()
            .filter(|provider| trust_rank(provider.detail.trust_level) == top_rank)
            .collect();
        if ambiguous.len() > 1 {
            return Ok(ManagerSelection::Conflict(conflict(
                "manager_selection_required",
                "multiple manager providers match this media type",
                json!({
                    "candidates": ambiguous
                        .iter()
                        .map(|provider| provider.detail.provider.provider_id)
                        .collect::<Vec<_>>(),
                }),
            )));
        }
    }
    Ok(ManagerSelection::Selected(eligible[0].clone()))
}

async fn missing_required_fields_for_provider(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider: &ProviderContext,
) -> AnyResult<Vec<String>> {
    let mut required_fields = provider.scope.required_fields.clone();
    if provider.scope.requires_account && required_fields.is_empty() {
        required_fields.push("api_key".to_string());
    }
    if required_fields.is_empty() {
        return Ok(Vec::new());
    }

    let implementation = provider
        .detail
        .provider
        .implementation
        .as_deref()
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_default();

    let mut missing = Vec::new();
    for field in required_fields {
        if provider_field_is_available(state, store, provider, &implementation, &field).await? {
            continue;
        }
        missing.push(field);
    }
    Ok(missing)
}

async fn provider_field_is_available(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider: &ProviderContext,
    implementation: &str,
    field: &str,
) -> AnyResult<bool> {
    let field = field.trim();
    if field.is_empty() {
        return Ok(true);
    }
    if let Some(value) = provider
        .instance_config
        .as_ref()
        .and_then(|config| config.get(field))
        .and_then(Value::as_str)
        .map(str::trim)
    {
        if !value.is_empty() {
            return Ok(true);
        }
    }

    if field.eq_ignore_ascii_case("api_key")
        && resolve_arr_api_key(state, store, provider, implementation)
            .await
            .is_ok()
    {
        return Ok(true);
    }

    let mut secret_keys = vec![field.to_string()];
    if !implementation.is_empty() {
        secret_keys.push(format!("{implementation}_{field}"));
        secret_keys.push(format!("{implementation}.{field}"));
    }

    for key in secret_keys {
        let secret = store
            .get_secret(
                SecretScope::Instance,
                Some(provider.detail.provider.instance_id),
                &key,
            )
            .await?;
        let Some(secret) = secret else {
            continue;
        };
        if !state
            .secrets
            .decrypt(&secret.value_encrypted)?
            .trim()
            .is_empty()
        {
            return Ok(true);
        }
    }

    Ok(false)
}

fn control_defaults_setting_key(instance_id: Uuid) -> String {
    format!("{CONTROL_DEFAULTS_SETTING_PREFIX}{instance_id}")
}

async fn load_manager_control_defaults(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
) -> AnyResult<ManagerControlDefaults> {
    let value = store
        .get_extension_setting(&control_defaults_setting_key(instance_id))
        .await?;
    let mut defaults = ManagerControlDefaults::default();
    if let Some(object) = value.as_ref().and_then(Value::as_object) {
        if let Some(value) = object.get("monitorOnAdd").and_then(Value::as_bool) {
            defaults.monitor_on_add = value;
        }
        if let Some(value) = object.get("searchOnAdd").and_then(Value::as_bool) {
            defaults.search_on_add = value;
        }
    }
    Ok(defaults)
}

async fn resolve_find_media_add_options(
    store: &ExtensionStore<'_>,
    provider: &ProviderContext,
    options: &FindMediaAddOptions,
) -> AnyResult<FindMediaAddOptions> {
    let defaults = load_manager_control_defaults(store, provider.detail.provider.instance_id).await?;
    Ok(FindMediaAddOptions {
        monitor: Some(options.monitor.unwrap_or(defaults.monitor_on_add)),
        search: Some(options.search.unwrap_or(defaults.search_on_add)),
        root_folder_path: options.root_folder_path.clone(),
        quality_profile_id: options.quality_profile_id,
    })
}

async fn add_with_manager_provider(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider: &ProviderContext,
    media_type: MediaType,
    item: &FindMediaAddItem,
    options: &FindMediaAddOptions,
) -> AnyResult<Option<String>> {
    let endpoint_json = provider
        .detail
        .provider
        .endpoint_json
        .clone()
        .ok_or_else(|| anyhow::anyhow!("provider endpoint is missing"))?;
    let endpoint: ProviderEndpoint =
        serde_json::from_value(endpoint_json).context("parsing provider endpoint")?;
    let base_url =
        resolve_provider_transport_base_url(provider.detail.provider.instance_id, &endpoint)
            .await?;
    let implementation = provider
        .detail
        .provider
        .implementation
        .as_deref()
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_default();
    let api_key = resolve_arr_api_key(state, store, provider, &implementation).await?;

    debug!(
        manager_provider_id = %provider.detail.provider.provider_id,
        implementation = %implementation,
        capability = %provider.detail.provider.capability,
        base_url = %base_url,
        "dispatching find media add to manager provider"
    );

    let effective_options = resolve_find_media_add_options(store, provider, options).await?;

    match implementation.as_str() {
        "sonarr" => add_with_sonarr(&base_url, &api_key, media_type, item, &effective_options).await,
        "radarr" => add_with_radarr(&base_url, &api_key, item, &effective_options).await,
        _ => bail!(
            "manager implementation '{}' does not support add",
            implementation
        ),
    }
}

async fn add_with_sonarr(
    base_url: &str,
    api_key: &str,
    media_type: MediaType,
    item: &FindMediaAddItem,
    options: &FindMediaAddOptions,
) -> AnyResult<Option<String>> {
    let lookup_term = lookup_term_for_item(item);
    debug!(
        lookup_term = %lookup_term,
        media_type = ?media_type,
        title = ?item.title,
        "adding media through sonarr"
    );
    let items = request_arr_lookup(
        base_url,
        api_key,
        &lookup_term,
        &["api/v3/series/lookup", "api/v4/series/lookup"],
    )
    .await?;
    let mut selected = select_lookup_item(&items, item, media_type)
        .ok_or_else(|| anyhow::anyhow!("unable to resolve title in manager lookup"))?;

    let quality_profile_id = match options.quality_profile_id {
        Some(value) => value,
        None => {
            request_arr_first_id(
                base_url,
                api_key,
                &["api/v3/qualityprofile", "api/v4/qualityprofile"],
            )
            .await?
        }
    };
    let root_folder_path = match options.root_folder_path.as_deref() {
        Some(path) if !path.trim().is_empty() => path.trim().to_string(),
        _ => {
            request_arr_first_path(
                base_url,
                api_key,
                &["api/v3/rootfolder", "api/v4/rootfolder"],
            )
            .await?
        }
    };
    let monitored = options.monitor.unwrap_or(true);
    let search = options.search.unwrap_or(false);

    if let Some(payload) = selected.as_object_mut() {
        payload.insert(
            "qualityProfileId".to_string(),
            Value::Number(quality_profile_id.into()),
        );
        payload.insert(
            "rootFolderPath".to_string(),
            Value::String(root_folder_path.clone()),
        );
        payload.insert("monitored".to_string(), Value::Bool(monitored));
        payload.insert("seasonFolder".to_string(), Value::Bool(true));
        payload.insert(
            "addOptions".to_string(),
            json!({
                "searchForMissingEpisodes": search,
                "monitor": if monitored { "all" } else { "none" }
            }),
        );
    } else {
        bail!("series payload must be an object");
    }

    let created = request_arr_write(
        base_url,
        api_key,
        ReqwestMethod::POST,
        &["api/v3/series", "api/v4/series"],
        Some(&selected),
    )
    .await?;
    let created_id = created
        .get("id")
        .and_then(Value::as_i64)
        .map(|value| value.to_string());
    debug!(
        lookup_term = %lookup_term,
        created_id = ?created_id,
        "sonarr add completed"
    );
    Ok(created_id)
}

async fn add_with_radarr(
    base_url: &str,
    api_key: &str,
    item: &FindMediaAddItem,
    options: &FindMediaAddOptions,
) -> AnyResult<Option<String>> {
    let lookup_term = lookup_term_for_item(item);
    debug!(
        lookup_term = %lookup_term,
        title = ?item.title,
        "adding media through radarr"
    );
    let items = request_arr_lookup(
        base_url,
        api_key,
        &lookup_term,
        &["api/v3/movie/lookup", "api/v4/movie/lookup"],
    )
    .await?;
    let mut selected = select_lookup_item(&items, item, MediaType::Movie)
        .ok_or_else(|| anyhow::anyhow!("unable to resolve title in manager lookup"))?;

    let quality_profile_id = match options.quality_profile_id {
        Some(value) => value,
        None => {
            request_arr_first_id(
                base_url,
                api_key,
                &["api/v3/qualityprofile", "api/v4/qualityprofile"],
            )
            .await?
        }
    };
    let root_folder_path = match options.root_folder_path.as_deref() {
        Some(path) if !path.trim().is_empty() => path.trim().to_string(),
        _ => {
            request_arr_first_path(
                base_url,
                api_key,
                &["api/v3/rootfolder", "api/v4/rootfolder"],
            )
            .await?
        }
    };
    let monitored = options.monitor.unwrap_or(true);
    let search = options.search.unwrap_or(false);

    if let Some(payload) = selected.as_object_mut() {
        payload.insert(
            "qualityProfileId".to_string(),
            Value::Number(quality_profile_id.into()),
        );
        payload.insert(
            "rootFolderPath".to_string(),
            Value::String(root_folder_path.clone()),
        );
        payload.insert("monitored".to_string(), Value::Bool(monitored));
        payload.insert(
            "addOptions".to_string(),
            json!({
                "searchForMovie": search,
                "monitor": monitored
            }),
        );
    } else {
        bail!("movie payload must be an object");
    }

    let created = request_arr_write(
        base_url,
        api_key,
        ReqwestMethod::POST,
        &["api/v3/movie", "api/v4/movie"],
        Some(&selected),
    )
    .await?;
    let created_id = created
        .get("id")
        .and_then(Value::as_i64)
        .map(|value| value.to_string());
    debug!(
        lookup_term = %lookup_term,
        created_id = ?created_id,
        "radarr add completed"
    );
    Ok(created_id)
}

fn lookup_term_for_item(item: &FindMediaAddItem) -> String {
    if let Some(ids) = item.external_ids.as_ref() {
        if let Some(value) = ids.tvdb_movie.as_deref() {
            return value.trim().to_string();
        }
        if let Some(value) = ids.tvdb_series.as_deref() {
            return value.trim().to_string();
        }
        if let Some(value) = ids.tvdb.as_deref() {
            return value.trim().to_string();
        }
        if let Some(value) = ids.tmdb.as_deref() {
            return value.trim().to_string();
        }
        if let Some(value) = ids.imdb.as_deref() {
            return value.trim().to_string();
        }
        if let Some(value) = ids.anilist.as_deref() {
            return value.trim().to_string();
        }
    }
    item.title
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("")
        .to_string()
}

fn select_lookup_item(
    items: &[Value],
    item: &FindMediaAddItem,
    media_type: MediaType,
) -> Option<Value> {
    for value in items {
        if lookup_item_matches(value, item, media_type) {
            return Some(value.clone());
        }
    }
    items.first().cloned()
}

fn lookup_item_matches(value: &Value, item: &FindMediaAddItem, media_type: MediaType) -> bool {
    let Some(external_ids) = item.external_ids.as_ref() else {
        return lookup_title_year_matches(value, item);
    };

    if let Some(tvdb) = external_ids.tvdb_series.as_deref() {
        if value
            .get("tvdbId")
            .and_then(as_id_string)
            .map(|id| id == tvdb.trim())
            .unwrap_or(false)
        {
            return true;
        }
    }
    if media_type == MediaType::Movie {
        if let Some(tvdb) = external_ids
            .tvdb_movie
            .as_deref()
            .or(external_ids.tvdb.as_deref())
        {
            if value
                .get("tvdbId")
                .and_then(as_id_string)
                .map(|id| id == tvdb.trim())
                .unwrap_or(false)
            {
                return true;
            }
        }
    } else if let Some(tvdb) = external_ids.tvdb.as_deref() {
        if value
            .get("tvdbId")
            .and_then(as_id_string)
            .map(|id| id == tvdb.trim())
            .unwrap_or(false)
        {
            return true;
        }
    }
    if let Some(tmdb) = external_ids.tmdb.as_deref() {
        if value
            .get("tmdbId")
            .and_then(as_id_string)
            .map(|id| id == tmdb.trim())
            .unwrap_or(false)
        {
            return true;
        }
    }
    if let Some(imdb) = external_ids.imdb.as_deref() {
        if value
            .get("imdbId")
            .and_then(as_id_string)
            .map(|id| id.eq_ignore_ascii_case(imdb.trim()))
            .unwrap_or(false)
        {
            return true;
        }
    }
    if media_type == MediaType::Anime {
        if let Some(anilist) = external_ids.anilist.as_deref() {
            if value
                .get("tvdbId")
                .and_then(as_id_string)
                .map(|id| id == anilist.trim())
                .unwrap_or(false)
            {
                return true;
            }
        }
    }

    lookup_title_year_matches(value, item)
}

fn lookup_title_year_matches(value: &Value, item: &FindMediaAddItem) -> bool {
    let title_match = value
        .get("title")
        .and_then(Value::as_str)
        .map(|value| normalize_name(value) == normalize_name(item.title.as_deref().unwrap_or("")))
        .unwrap_or(false);
    if !title_match {
        return false;
    }
    match (
        value
            .get("year")
            .and_then(Value::as_i64)
            .map(|value| value as i32),
        item.year,
    ) {
        (_, None) => true,
        (Some(left), Some(right)) => left == right,
        (None, Some(_)) => true,
    }
}

fn normalize_name(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .replace([' ', '-', '_', ':'], "")
}

fn sort_find_media_results(query: &str, results: &mut [FindMediaResult]) {
    let query_tokens = tokenize_for_search(query);
    let query_compact = compact_for_search(query);
    let query_year = extract_year_from_query(query);

    results.sort_by(|left, right| {
        let left_score = find_media_rank(left, &query_tokens, &query_compact, query_year);
        let right_score = find_media_rank(right, &query_tokens, &query_compact, query_year);
        right_score
            .partial_cmp(&left_score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| right.year.cmp(&left.year))
            .then_with(|| left.title.cmp(&right.title))
    });
}

fn find_media_rank(
    item: &FindMediaResult,
    query_tokens: &[String],
    query_compact: &str,
    query_year: Option<i32>,
) -> f64 {
    let title = item.title.trim().to_ascii_lowercase();
    let title_tokens = tokenize_for_search(&title);
    let title_compact = compact_for_search(&title);

    let mut score = 0.0;
    if !query_compact.is_empty() {
        if title_compact == query_compact {
            score += 1200.0;
        } else if title_compact.starts_with(query_compact) {
            score += 900.0;
        } else if title_compact.contains(query_compact) {
            score += 650.0;
        }
    }

    if !query_tokens.is_empty() {
        let overlap = query_tokens
            .iter()
            .filter(|token| title_tokens.iter().any(|candidate| candidate == *token))
            .count();
        let overlap_ratio = overlap as f64 / query_tokens.len() as f64;
        score += overlap_ratio * 420.0;
        if overlap == query_tokens.len() && !query_tokens.is_empty() {
            score += 180.0;
        }
    }

    if let (Some(query_year), Some(result_year)) = (query_year, item.year) {
        if query_year == result_year {
            score += 120.0;
        }
    }

    if let Some(popularity) = item.popularity_score.filter(|value| *value > 0.0) {
        score += (popularity.ln_1p() * 45.0).min(220.0);
    }

    score
}

fn tokenize_for_search(value: &str) -> Vec<String> {
    value
        .to_ascii_lowercase()
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(|token| token.to_string())
        .collect()
}

fn compact_for_search(value: &str) -> String {
    value
        .to_ascii_lowercase()
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>()
}

fn extract_year_from_query(value: &str) -> Option<i32> {
    for token in value.split(|ch: char| !ch.is_ascii_alphanumeric()) {
        if token.len() == 4
            && token.chars().all(|ch| ch.is_ascii_digit())
            && let Ok(year) = token.parse::<i32>()
            && (1888..=2100).contains(&year)
        {
            return Some(year);
        }
    }
    None
}

#[cfg(test)]
mod ranking_tests {
    use super::*;

    #[test]
    fn ranks_exact_title_above_loose_match() {
        let mut results = vec![
            FindMediaResult {
                title: "The Good, the Bad and the Ugly".to_string(),
                r#type: MediaType::Movie,
                year: Some(1966),
                external_ids: None,
                description: None,
                poster_url: None,
                popularity_score: Some(9.1),
                source_provider_ids: Vec::new(),
                source_labels: Vec::new(),
            },
            FindMediaResult {
                title: "Good Cars: The Ugly Truth".to_string(),
                r#type: MediaType::Movie,
                year: Some(2022),
                external_ids: None,
                description: None,
                poster_url: None,
                popularity_score: Some(1.0),
                source_provider_ids: Vec::new(),
                source_labels: Vec::new(),
            },
        ];

        sort_find_media_results("The Good The Bad And The Ugly", &mut results);
        assert_eq!(results[0].title, "The Good, the Bad and the Ugly");
    }
}

async fn search_with_provider(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider: &ProviderContext,
    query: &str,
    media_type: MediaType,
) -> AnyResult<Vec<DiscoveryResult>> {
    let implementation = provider
        .detail
        .provider
        .implementation
        .as_deref()
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_default();

    let endpoint_json = provider
        .detail
        .provider
        .endpoint_json
        .clone()
        .ok_or_else(|| anyhow::anyhow!("provider endpoint is missing"))?;
    let endpoint: ProviderEndpoint =
        serde_json::from_value(endpoint_json).context("parsing provider endpoint")?;
    let base_url =
        resolve_provider_transport_base_url(provider.detail.provider.instance_id, &endpoint)
            .await?;

    let api_key = resolve_arr_api_key(state, store, provider, &implementation).await?;

    match implementation.as_str() {
        "sonarr" => search_sonarr(&base_url, &api_key, query, media_type).await,
        "radarr" => search_radarr(&base_url, &api_key, query).await,
        _ => bail!(
            "search is not supported for implementation '{}'",
            implementation
        ),
    }
}

async fn resolve_arr_api_key(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider: &ProviderContext,
    implementation: &str,
) -> AnyResult<String> {
    if let Some(value) = provider
        .instance_config
        .as_ref()
        .and_then(|value| value.get("api_key"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Ok(value.to_string());
    }

    let secret_key = match implementation {
        "sonarr" => "sonarr_api_key",
        "radarr" => "radarr_api_key",
        _ => "api_key",
    };
    if let Some(secret) = store
        .get_secret(
            crate::db::models::SecretScope::Instance,
            Some(provider.detail.provider.instance_id),
            secret_key,
        )
        .await?
    {
        return state
            .secrets
            .decrypt(&secret.value_encrypted)
            .context("decrypting manager api key");
    }
    if let Some(secret) = store
        .get_secret(
            crate::db::models::SecretScope::Instance,
            Some(provider.detail.provider.instance_id),
            "api_key",
        )
        .await?
    {
        return state
            .secrets
            .decrypt(&secret.value_encrypted)
            .context("decrypting manager api key");
    }

    bail!("manager api key is not available yet");
}

async fn search_sonarr(
    base_url: &str,
    api_key: &str,
    query: &str,
    media_type: MediaType,
) -> AnyResult<Vec<DiscoveryResult>> {
    let items = request_arr_lookup(
        base_url,
        api_key,
        query,
        &["api/v3/series/lookup", "api/v4/series/lookup"],
    )
    .await?;
    let mut out = Vec::new();
    for item in items {
        let title = item
            .get("title")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or_default();
        if title.is_empty() {
            continue;
        }
        let tvdb_series = item.get("tvdbId").and_then(as_id_string);
        let imdb = item.get("imdbId").and_then(as_id_string);
        let tmdb = item.get("tmdbId").and_then(as_id_string);
        let year = item
            .get("year")
            .and_then(Value::as_i64)
            .map(|value| value as i32)
            .or_else(|| parse_year_from_text(item.get("firstAired").and_then(Value::as_str)));
        let description = item
            .get("overview")
            .and_then(Value::as_str)
            .map(|value| value.to_string());
        let poster_url = extract_arr_poster_url(base_url, &item);
        let popularity_score = extract_arr_popularity_score(&item);

        out.push(DiscoveryResult {
            title: title.to_string(),
            r#type: if media_type == MediaType::Anime {
                MediaType::Anime
            } else {
                MediaType::Series
            },
            year,
            external_ids: Some(ExternalIds {
                imdb,
                tmdb,
                tvdb_series,
                ..Default::default()
            }),
            description,
            poster_url,
            popularity_score,
        });
    }
    Ok(out)
}

async fn search_radarr(
    base_url: &str,
    api_key: &str,
    query: &str,
) -> AnyResult<Vec<DiscoveryResult>> {
    let items = request_arr_lookup(
        base_url,
        api_key,
        query,
        &["api/v3/movie/lookup", "api/v4/movie/lookup"],
    )
    .await?;
    let mut out = Vec::new();
    for item in items {
        let title = item
            .get("title")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or_default();
        if title.is_empty() {
            continue;
        }
        let tmdb = item.get("tmdbId").and_then(as_id_string);
        let imdb = item.get("imdbId").and_then(as_id_string);
        let year = item
            .get("year")
            .and_then(Value::as_i64)
            .map(|value| value as i32);
        let description = item
            .get("overview")
            .and_then(Value::as_str)
            .map(|value| value.to_string());
        let poster_url = extract_arr_poster_url(base_url, &item);
        let popularity_score = extract_arr_popularity_score(&item);

        out.push(DiscoveryResult {
            title: title.to_string(),
            r#type: MediaType::Movie,
            year,
            external_ids: Some(ExternalIds {
                imdb,
                tmdb,
                ..Default::default()
            }),
            description,
            poster_url,
            popularity_score,
        });
    }
    Ok(out)
}

async fn request_arr_lookup(
    base_url: &str,
    api_key: &str,
    query: &str,
    paths: &[&str],
) -> AnyResult<Vec<Value>> {
    let client = build_arr_client(api_key)?;

    for path in paths {
        let mut url = build_arr_lookup_url(base_url, path)?;
        url.query_pairs_mut().append_pair("term", query);

        let resp = client
            .get(url)
            .send()
            .await
            .with_context(|| format!("calling {}", path))?;
        if resp.status() == ReqwestStatusCode::NOT_FOUND {
            continue;
        }
        if !resp.status().is_success() {
            let status = resp.status();
            let detail = resp.text().await.unwrap_or_default();
            bail!("lookup failed ({status}): {}", detail.trim());
        }
        let items = resp
            .json::<Vec<Value>>()
            .await
            .with_context(|| format!("parsing {}", path))?;
        return Ok(items);
    }

    bail!("lookup endpoint is not available");
}

async fn request_arr_json_with_query<P: AsRef<str>>(
    base_url: &str,
    api_key: &str,
    paths: &[P],
    query_pairs: &[(&str, String)],
) -> AnyResult<Value> {
    let client = build_arr_client(api_key)?;

    for path in paths {
        let path = path.as_ref();
        if path.trim().is_empty() {
            continue;
        }
        let mut url = build_arr_lookup_url(base_url, path)?;
        {
            let mut pairs = url.query_pairs_mut();
            for (key, value) in query_pairs {
                pairs.append_pair(key, value);
            }
        }

        let resp = client
            .get(url)
            .send()
            .await
            .with_context(|| format!("calling {path}"))?;
        if resp.status() == ReqwestStatusCode::NOT_FOUND {
            continue;
        }
        if !resp.status().is_success() {
            let status = resp.status();
            let detail = resp.text().await.unwrap_or_default();
            bail!("{path} failed ({status}): {}", detail.trim());
        }
        return resp
            .json::<Value>()
            .await
            .with_context(|| format!("parsing {path}"));
    }

    bail!("manager endpoint is not available");
}

async fn request_arr_first_id(base_url: &str, api_key: &str, paths: &[&str]) -> AnyResult<i64> {
    let value = request_arr_write(base_url, api_key, ReqwestMethod::GET, paths, None).await?;
    let items = value
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("expected array response"))?;
    let id = items
        .first()
        .and_then(|item| item.get("id"))
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow::anyhow!("unable to determine quality profile"))?;
    Ok(id)
}

async fn request_arr_first_path(
    base_url: &str,
    api_key: &str,
    paths: &[&str],
) -> AnyResult<String> {
    let value = request_arr_write(base_url, api_key, ReqwestMethod::GET, paths, None).await?;
    let items = value
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("expected array response"))?;
    let path = items
        .first()
        .and_then(|item| item.get("path"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("unable to determine root folder path"))?;
    Ok(path.to_string())
}

async fn request_arr_write(
    base_url: &str,
    api_key: &str,
    method: ReqwestMethod,
    paths: &[&str],
    body: Option<&Value>,
) -> AnyResult<Value> {
    let client = build_arr_client(api_key)?;
    for path in paths {
        let url = build_arr_lookup_url(base_url, path)?;
        let mut request = client.request(method.clone(), url);
        if let Some(body) = body {
            request = request.json(body);
        }
        let resp = request
            .send()
            .await
            .with_context(|| format!("{} {}", method.as_str(), path))?;
        if resp.status() == ReqwestStatusCode::NOT_FOUND {
            continue;
        }
        if !resp.status().is_success() {
            let status = resp.status();
            let detail = resp.text().await.unwrap_or_default();
            bail!("{} failed ({status}): {}", path, detail.trim());
        }
        if method == ReqwestMethod::GET {
            return resp
                .json::<Value>()
                .await
                .with_context(|| format!("parsing {}", path));
        }
        return resp
            .json::<Value>()
            .await
            .with_context(|| format!("parsing {}", path));
    }

    bail!("manager endpoint is not available")
}

async fn resolve_provider_transport_base_url(
    instance_id: Uuid,
    endpoint: &ProviderEndpoint,
) -> AnyResult<String> {
    let canonical = endpoint.canonical_url()?;
    if endpoint_host_resolves(&endpoint.host, endpoint.port).await {
        return Ok(canonical);
    }

    if let Some(host_port) = lookup_docker_published_port(instance_id, endpoint.port).await? {
        let base_path = if endpoint.base_path.trim().is_empty() {
            "/"
        } else {
            endpoint.base_path.as_str()
        };
        return Ok(format!(
            "{}://127.0.0.1:{}{}",
            endpoint.scheme, host_port, base_path
        ));
    }

    bail!(
        "provider endpoint {}:{} is not reachable from server host and has no published host port",
        endpoint.host,
        endpoint.port
    );
}

async fn endpoint_host_resolves(host: &str, port: u16) -> bool {
    lookup_host((host, port))
        .await
        .map(|mut values| values.next().is_some())
        .unwrap_or(false)
}

async fn lookup_docker_published_port(
    instance_id: Uuid,
    container_port: u16,
) -> AnyResult<Option<u16>> {
    let container_names = run_docker_stdout(&[
        "ps",
        "-a",
        "--filter",
        &format!("label=elixir.instance_id={instance_id}"),
        "--format",
        "{{.Names}}",
    ])
    .await?;
    let Some(container_name) = container_names
        .lines()
        .map(str::trim)
        .find(|value| !value.is_empty())
    else {
        return Ok(None);
    };

    let ports_json = run_docker_stdout(&[
        "inspect",
        "--format",
        "{{json .NetworkSettings.Ports}}",
        container_name,
    ])
    .await?;
    let ports: Value =
        serde_json::from_str(ports_json.trim()).context("parsing docker ports inspect output")?;
    let key = format!("{container_port}/tcp");
    let binding = ports
        .get(&key)
        .and_then(Value::as_array)
        .and_then(|values| values.first());
    let Some(binding) = binding else {
        return Ok(None);
    };
    let host_port = binding
        .get("HostPort")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .parse::<u16>()
        .ok();
    Ok(host_port)
}

async fn run_docker_stdout(args: &[&str]) -> AnyResult<String> {
    let output = Command::new("docker")
        .args(args)
        .output()
        .await
        .with_context(|| format!("executing docker {}", args.join(" ")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("docker {} failed: {}", args.join(" "), stderr.trim());
    }
    Ok(String::from_utf8(output.stdout).context("docker output was not utf-8")?)
}

fn build_arr_client(api_key: &str) -> AnyResult<Client> {
    let mut headers = HeaderMap::new();
    headers.insert(
        "X-Api-Key",
        HeaderValue::from_str(api_key).context("invalid api key header")?,
    );
    headers.insert(USER_AGENT, HeaderValue::from_static("Elixir/1.0"));
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .default_headers(headers)
        .build()
        .context("building arr discovery client")
}

fn build_arr_lookup_url(base_url: &str, path: &str) -> AnyResult<Url> {
    let mut root = Url::parse(base_url).context("parsing manager base url")?;
    let trimmed = root.path().trim_end_matches('/');
    let next_path = if trimmed.is_empty() || trimmed == "/" {
        format!("/{}", path.trim_start_matches('/'))
    } else {
        format!("{}/{}", trimmed, path.trim_start_matches('/'))
    };
    root.set_path(&next_path);
    root.set_query(None);
    Ok(root)
}

fn as_id_string(value: &Value) -> Option<String> {
    if let Some(value) = value.as_str() {
        let value = value.trim();
        if !value.is_empty() {
            return Some(value.to_string());
        }
        return None;
    }
    if let Some(value) = value.as_i64() {
        return Some(value.to_string());
    }
    if let Some(value) = value.as_u64() {
        return Some(value.to_string());
    }
    None
}

fn parse_year_from_text(value: Option<&str>) -> Option<i32> {
    let value = value?;
    let year = value.get(0..4)?;
    year.parse::<i32>().ok()
}

fn extract_arr_poster_url(base_url: &str, value: &Value) -> Option<String> {
    let images = value.get("images").and_then(Value::as_array)?;
    let url = images
        .iter()
        .find(|image| {
            image
                .get("coverType")
                .and_then(Value::as_str)
                .map(|cover| {
                    let cover = cover.trim().to_ascii_lowercase();
                    cover == "poster" || cover == "cover"
                })
                .unwrap_or(false)
        })
        .or_else(|| images.first())?
        .get("url")
        .and_then(Value::as_str)?
        .trim();
    if url.is_empty() {
        return None;
    }
    if url.starts_with("http://") || url.starts_with("https://") {
        return Some(url.to_string());
    }
    let root = Url::parse(base_url).ok()?;
    let joined = root.join(url.trim_start_matches('/')).ok()?;
    Some(joined.to_string())
}

fn extract_arr_popularity_score(value: &Value) -> Option<f64> {
    value
        .get("ratings")
        .and_then(|ratings| ratings.get("value"))
        .and_then(value_as_f64)
        .or_else(|| value.get("popularity").and_then(value_as_f64))
        .or_else(|| value.get("voteCount").and_then(value_as_f64))
}

fn value_as_f64(value: &Value) -> Option<f64> {
    if let Some(number) = value.as_f64() {
        return Some(number);
    }
    value.as_str()?.trim().parse::<f64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_arr_duration_seconds_handles_hms() {
        assert_eq!(parse_arr_duration_seconds("01:02:03"), Some(3723));
        assert_eq!(parse_arr_duration_seconds("12:34"), Some(754));
        assert_eq!(parse_arr_duration_seconds(""), None);
    }

    #[test]
    fn derive_arr_acquisition_state_marks_downloading_from_queue_progress() {
        let item = json!({
            "hasFile": false,
            "sizeOnDisk": 0
        });
        let queue = json!({
            "records": [
                {
                    "movieId": 42,
                    "downloadClient": "default (nzbget)",
                    "protocol": "usenet",
                    "trackedDownloadState": "downloading",
                    "size": 1000,
                    "sizeleft": 250
                }
            ]
        });

        let state = derive_arr_acquisition_state("radarr", "42", &item, Some(&queue));
        assert_eq!(state.stage, AcquisitionStage::Downloading);
        assert_eq!(state.downloader_label.as_deref(), Some("default (nzbget)"));
        assert_eq!(state.protocol.as_deref(), Some("usenet"));
        assert_eq!(state.progress_percent.map(|value| value.round() as i32), Some(75));
    }

    #[test]
    fn derive_arr_acquisition_state_marks_importing_when_files_exist() {
        let item = json!({
            "statistics": {
                "episodeFileCount": 3,
                "sizeOnDisk": 12345
            }
        });

        let state = derive_arr_acquisition_state("sonarr", "9", &item, None);
        assert_eq!(state.stage, AcquisitionStage::Importing);
        assert_eq!(
            state.description,
            "Manager has imported files. Waiting for library scan."
        );
    }

    #[test]
    fn derive_arr_acquisition_state_marks_attention_for_queue_errors() {
        let item = json!({
            "hasFile": false
        });
        let queue = json!([
            {
                "movieId": 77,
                "errorMessage": "Release was rejected"
            }
        ]);

        let state = derive_arr_acquisition_state("radarr", "77", &item, Some(&queue));
        assert_eq!(state.stage, AcquisitionStage::NeedsAttention);
        assert_eq!(state.description, "Release was rejected");
    }
}
