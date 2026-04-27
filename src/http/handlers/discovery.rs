use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::Path as StdPath,
};

use anyhow::{Context, Result as AnyResult, bail};
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use reqwest::header::{ACCEPT, HeaderMap, HeaderValue, USER_AGENT};
use reqwest::{Client, Method as ReqwestMethod, StatusCode as ReqwestStatusCode, Url};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Value, json};
use sqlx::Row;
use tokio::net::lookup_host;
use tokio::process::Command;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::{
    acquisition::{AUTO_RECOVERY_COOLDOWN_SECONDS, IntentRecoveryView, load_intent_recovery_views},
    db::models::{ExtensionTrustLevel, MediaType, ProviderHealthState, SecretScope},
    drivers::{
        AddMediaOptions as DriverAddMediaOptions, AddMediaRequest, DriverCtx, DriverRegistry,
    },
    extensions::{
        ExternalIds,
        manifest::ExtensionManifest,
        store::{ExtensionStore, ManagedImportFile, NewManagedImportEvent, NewManagedIngestIntent},
    },
    http::{
        auth::CurrentUser,
        error::{ApiError, ApiResult},
    },
    library::{ingest_managed_import_event, managed_episode_tombstone_matches_series},
    metadata::DiscoveryResult,
    orchestrator::model::ProviderEndpoint,
    state::AppState,
};

const MANAGER_PREF_MOVIE: &str = "manager_preference.movie";
const MANAGER_PREF_SERIES: &str = "manager_preference.series";
const MANAGER_PREF_ANIME: &str = "manager_preference.anime";
const CONTROL_DEFAULTS_SETTING_PREFIX: &str = "extensions.control_defaults.instance.";
const NZBGET_DRONE_DOWNLOAD_ID_PARAM: &str = "drone";
const TORRENT_METADATA_STALL_TIMEOUT_SECONDS: i64 = 10 * 60;
const TORRENT_ZERO_PROGRESS_TIMEOUT_SECONDS: i64 = 15 * 60;

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

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FindMediaAcquisitionActionResponse {
    pub message: String,
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
    pub phase: String,
    pub phase_label: String,
    pub headline: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocker: Option<FindMediaAcquisitionBlocker>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence: Vec<FindMediaAcquisitionEvidence>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<FindMediaAcquisitionAction>,
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<FindMediaAcquisitionChildItem>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FindMediaAcquisitionChildItem {
    pub id: String,
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subtitle: Option<String>,
    pub phase: String,
    pub phase_label: String,
    pub headline: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocker: Option<FindMediaAcquisitionBlocker>,
    pub progress_percent: Option<f64>,
    pub eta_seconds: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub downloader_label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AcquisitionPhase {
    Requested,
    AcceptedByManager,
    FindingAnotherRelease,
    QueuedInDownloader,
    Downloading,
    PostProcessing,
    Importing,
    Completed,
    NeedsAttention,
    Failed,
}

impl AcquisitionPhase {
    fn as_str(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::AcceptedByManager => "accepted_by_manager",
            Self::FindingAnotherRelease => "finding_another_release",
            Self::QueuedInDownloader => "queued_in_downloader",
            Self::Downloading => "downloading",
            Self::PostProcessing => "post_processing",
            Self::Importing => "importing",
            Self::Completed => "completed",
            Self::NeedsAttention => "needs_attention",
            Self::Failed => "failed",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Requested => "Requested",
            Self::AcceptedByManager => "Accepted by manager",
            Self::FindingAnotherRelease => "Finding another release",
            Self::QueuedInDownloader => "Queued in downloader",
            Self::Downloading => "Downloading",
            Self::PostProcessing => "Post-processing",
            Self::Importing => "Importing",
            Self::Completed => "Downloaded",
            Self::NeedsAttention => "Needs attention",
            Self::Failed => "Failed",
        }
    }

    fn sort_priority(self) -> i32 {
        match self {
            Self::NeedsAttention => 0,
            Self::Failed => 1,
            Self::FindingAnotherRelease => 2,
            Self::Downloading => 3,
            Self::PostProcessing => 4,
            Self::Importing => 5,
            Self::QueuedInDownloader => 6,
            Self::AcceptedByManager => 7,
            Self::Requested => 8,
            Self::Completed => 9,
        }
    }

    fn is_active(self) -> bool {
        !matches!(self, Self::Completed | Self::Failed)
    }

    fn counts_as_downloading(self) -> bool {
        matches!(self, Self::Downloading)
    }

    fn legacy_stage(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::AcceptedByManager => "searching",
            Self::FindingAnotherRelease => "searching",
            Self::QueuedInDownloader => "queued",
            Self::Downloading => "downloading",
            Self::PostProcessing => "post_processing",
            Self::Importing => "importing",
            Self::Completed => "ready",
            Self::NeedsAttention => "needs_attention",
            Self::Failed => "failed",
        }
    }

    fn legacy_stage_label(self) -> &'static str {
        match self.legacy_stage() {
            "requested" => "Requested",
            "searching" => "Searching",
            "queued" => "Queued",
            "downloading" => "Downloading",
            "post_processing" => "Post-processing",
            "importing" => "Importing",
            "ready" => "Ready",
            "needs_attention" => "Needs attention",
            "failed" => "Failed",
            _ => self.label(),
        }
    }
}

#[derive(Debug, Clone)]
struct AcquisitionItemState {
    phase: AcquisitionPhase,
    headline: String,
    detail: Option<String>,
    blocker: Option<FindMediaAcquisitionBlocker>,
    evidence: Vec<FindMediaAcquisitionEvidence>,
    actions: Vec<FindMediaAcquisitionAction>,
    progress_percent: Option<f64>,
    eta_seconds: Option<i64>,
    downloader_label: Option<String>,
    protocol: Option<String>,
    children: Vec<FindMediaAcquisitionChildItem>,
}

#[derive(Debug, Clone, Default)]
struct SonarrEpisodeDescriptor {
    season_number: i64,
    episode_number: i64,
    title: String,
}

#[derive(Debug, Clone, Copy, Default)]
struct SonarrSeriesStats {
    episode_count: usize,
    episode_file_count: usize,
}

#[derive(Debug, Clone, Copy, Default)]
struct SonarrBatchCounts {
    queued: usize,
    downloading: usize,
    post_processing: usize,
    importing: usize,
    needs_attention: usize,
    failed: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FindMediaAcquisitionBlocker {
    pub code: String,
    pub title: String,
    pub detail: String,
    pub severity: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FindMediaAcquisitionEvidence {
    pub label: String,
    pub value: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tone: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FindMediaAcquisitionAction {
    pub id: String,
    pub label: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confirm_text: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct AcquisitionDownloaderTotals {
    total_download_rate_bps: Option<u64>,
    total_upload_rate_bps: Option<u64>,
}

#[derive(Debug, Clone, Default)]
struct AcquisitionDownloaderProgressIndex {
    by_download_id: HashMap<String, AcquisitionDownloaderProgress>,
}

impl AcquisitionDownloaderProgressIndex {
    fn get(&self, download_id: &str) -> Option<&AcquisitionDownloaderProgress> {
        self.by_download_id.get(&normalize_download_id(download_id))
    }

    fn insert(&mut self, download_id: &str, progress: AcquisitionDownloaderProgress) {
        let normalized = normalize_download_id(download_id);
        if normalized.is_empty() {
            return;
        }
        self.by_download_id.entry(normalized).or_insert(progress);
    }
}

#[derive(Debug, Clone)]
struct AcquisitionDownloaderProgress {
    progress_percent: Option<f64>,
    eta_seconds: Option<i64>,
    issue: Option<AcquisitionDownloaderIssue>,
}

#[derive(Debug, Clone)]
struct AcquisitionDownloaderIssue {
    code: String,
    title: String,
    detail: String,
}

#[derive(Debug, Deserialize)]
struct AcquisitionQbittorrentTorrent {
    #[serde(default)]
    hash: String,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    progress: Option<f64>,
    #[serde(default)]
    downloaded: Option<u64>,
    #[serde(default)]
    dlspeed: Option<u64>,
    #[serde(default)]
    total_size: Option<u64>,
    #[serde(default)]
    size: Option<u64>,
    #[serde(default)]
    amount_left: Option<u64>,
    #[serde(default)]
    eta: Option<i64>,
    #[serde(default)]
    num_seeds: Option<u64>,
    #[serde(default)]
    num_complete: Option<u64>,
    #[serde(default)]
    num_leechs: Option<u64>,
    #[serde(default)]
    num_incomplete: Option<u64>,
    #[serde(default)]
    added_on: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct AcquisitionNzbgetGroup {
    #[serde(rename = "NZBID", default)]
    nzb_id: i64,
    #[serde(rename = "Status", default)]
    status: Option<String>,
    #[serde(rename = "FileSizeLo", default)]
    file_size_lo: Option<u64>,
    #[serde(rename = "FileSizeHi", default)]
    file_size_hi: Option<u64>,
    #[serde(rename = "RemainingSizeLo", default)]
    remaining_size_lo: Option<u64>,
    #[serde(rename = "RemainingSizeHi", default)]
    remaining_size_hi: Option<u64>,
    #[serde(rename = "DownloadedSizeLo", default)]
    downloaded_size_lo: Option<u64>,
    #[serde(rename = "DownloadedSizeHi", default)]
    downloaded_size_hi: Option<u64>,
    #[serde(rename = "FailedArticles", default)]
    failed_articles: Option<u64>,
    #[serde(rename = "Health", default)]
    health: Option<i64>,
    #[serde(rename = "CriticalHealth", default)]
    critical_health: Option<i64>,
    #[serde(rename = "Parameters", default)]
    parameters: Vec<AcquisitionNzbgetGroupParameter>,
}

#[derive(Debug, Deserialize)]
struct AcquisitionNzbgetGroupParameter {
    #[serde(rename = "Name", default)]
    name: String,
    #[serde(rename = "Value", default)]
    value: String,
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
pub(crate) struct ProviderContext {
    pub(crate) detail: crate::extensions::store::ProviderDetails,
    pub(crate) instance_name: String,
    pub(crate) instance_config: Option<Value>,
    scope: ProviderScopeDocument,
    pub(crate) media_types: Vec<MediaType>,
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

pub async fn find_media_acquisition_find_another_release(
    State(state): State<AppState>,
    _user: CurrentUser,
    Path(intent_id): Path<Uuid>,
) -> ApiResult<Json<FindMediaAcquisitionActionResponse>> {
    let store = ExtensionStore::new(&state.db_pool);
    let message = execute_find_another_release(&state, &store, intent_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(FindMediaAcquisitionActionResponse { message }))
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

    let tombstones = store.list_active_managed_media_tombstones().await?;
    for tombstone in tombstones {
        let same_provider_item = tombstone.manager_provider_id == Some(manager_provider_id)
            && tombstone.manager_item_id.as_deref() == manager_item_id;
        let same_title = managed_tombstone_media_type_compatible(media_type, tombstone.media_type)
            && tombstone.normalized_title == normalize_name(title)
            && match (tombstone.year, item.year) {
                (Some(left), Some(right)) => left == right,
                (None, _) | (_, None) => true,
            };
        if same_provider_item || same_title {
            store
                .deactivate_managed_media_tombstone(tombstone.tombstone_id)
                .await?;
        }
    }

    let episode_tombstones = store.list_active_managed_episode_tombstones().await?;
    let external_ids = item.external_ids.clone().unwrap_or_default();
    for tombstone in episode_tombstones {
        let same_provider_item = tombstone.manager_provider_id == Some(manager_provider_id)
            && tombstone.manager_item_id.as_deref() == manager_item_id;
        let same_series = managed_episode_tombstone_matches_series(
            media_type,
            title,
            item.year,
            &external_ids,
            &tombstone,
        );
        if same_provider_item || same_series {
            store
                .deactivate_managed_episode_tombstone(tombstone.tombstone_id)
                .await?;
        }
    }

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
        .iter()
        .cloned()
        .map(|provider| (provider.detail.provider.provider_id, provider))
        .collect();
    let recovery_views = load_intent_recovery_views(store).await?;
    let downloader_progress =
        load_acquisition_downloader_progress_index(state, store, &provider_contexts).await;
    let downloader_totals = load_acquisition_downloader_totals(state, store).await?;
    let recent_cutoff = Utc::now() - ChronoDuration::hours(ACQUISITION_RECENT_WINDOW_HOURS);

    let mut items = Vec::new();
    for intent in store.list_active_managed_ingest_intents().await? {
        let item = build_find_media_acquisition_item(
            state,
            store,
            &provider_map,
            &recovery_views,
            &downloader_progress,
            &intent,
        )
        .await?;
        if item.phase == AcquisitionPhase::Completed.as_str() {
            let reference = item.last_matched_at.unwrap_or(item.updated_at);
            if reference < recent_cutoff {
                continue;
            }
        }
        items.push(item);
    }

    items.sort_by(|left, right| {
        let left_phase = acquisition_phase_from_str(&left.phase);
        let right_phase = acquisition_phase_from_str(&right.phase);
        left_phase
            .sort_priority()
            .cmp(&right_phase.sort_priority())
            .then_with(|| right.updated_at.cmp(&left.updated_at))
    });

    let mut active_count = 0usize;
    let mut downloading_count = 0usize;
    let mut needs_attention_count = 0usize;
    let mut recent_completed_count = 0usize;
    for item in &items {
        let phase = acquisition_phase_from_str(&item.phase);
        if phase.is_active() {
            active_count += 1;
        }
        if phase.counts_as_downloading() {
            downloading_count += 1;
        }
        if matches!(
            phase,
            AcquisitionPhase::NeedsAttention | AcquisitionPhase::Failed
        ) {
            needs_attention_count += 1;
        }
        if phase == AcquisitionPhase::Completed {
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
    recovery_views: &HashMap<Uuid, IntentRecoveryView>,
    downloader_progress: &AcquisitionDownloaderProgressIndex,
    intent: &crate::extensions::store::ManagedIngestIntent,
) -> AnyResult<FindMediaAcquisitionItem> {
    let manager_label = provider_map
        .get(&intent.manager_provider_id)
        .map(provider_label)
        .or_else(|| intent.manager_label.clone())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "Manager".to_string());
    let state_view = resolve_acquisition_item_state(
        state,
        store,
        provider_map.get(&intent.manager_provider_id),
        recovery_views.get(&intent.intent_id),
        downloader_progress,
        intent,
    )
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
        phase: state_view.phase.as_str().to_string(),
        phase_label: state_view.phase.label().to_string(),
        headline: state_view.headline.clone(),
        detail: state_view.detail.clone(),
        blocker: state_view.blocker.clone(),
        evidence: state_view.evidence.clone(),
        actions: state_view.actions.clone(),
        stage: state_view.phase.legacy_stage().to_string(),
        stage_label: state_view.phase.legacy_stage_label().to_string(),
        description: state_view
            .detail
            .clone()
            .unwrap_or_else(|| state_view.headline.clone()),
        progress_percent: state_view.progress_percent,
        eta_seconds: state_view.eta_seconds,
        downloader_label: state_view.downloader_label,
        protocol: state_view.protocol,
        last_matched_at: intent.last_matched_at,
        created_at: intent.created_at,
        updated_at: intent.updated_at,
        children: state_view.children,
    })
}

async fn resolve_acquisition_item_state(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider: Option<&ProviderContext>,
    recovery_view: Option<&IntentRecoveryView>,
    downloader_progress: &AcquisitionDownloaderProgressIndex,
    intent: &crate::extensions::store::ManagedIngestIntent,
) -> AnyResult<AcquisitionItemState> {
    let mut library_matched = intent.last_matched_at.is_some();
    let library_needs_hydration = if library_matched {
        match managed_library_needs_hydration(&state.db_pool, intent).await {
            Ok(needs_hydration) => needs_hydration,
            Err(err) => {
                warn!(
                    intent_id = %intent.intent_id,
                    "failed to inspect managed library hydration state: {err}"
                );
                false
            }
        }
    } else {
        false
    };
    if library_matched
        && !library_needs_hydration
        && !matches!(intent.media_type, MediaType::Series | MediaType::Anime)
    {
        return Ok(completed_acquisition_state());
    }

    let Some(provider) = provider else {
        return Ok(acquisition_attention(
            "manager_missing",
            "Manager unavailable",
            "Selected manager is no longer available.",
            base_acquisition_evidence(
                intent.manager_item_id.is_some(),
                false,
                false,
                0,
                None,
                None,
            ),
        ));
    };

    if provider.detail.provider.health_state == ProviderHealthState::Unhealthy {
        return Ok(acquisition_attention(
            "manager_unhealthy",
            "Manager unavailable",
            "Selected manager is currently unavailable.",
            base_acquisition_evidence(
                intent.manager_item_id.is_some(),
                false,
                false,
                0,
                None,
                None,
            ),
        ));
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
            phase: AcquisitionPhase::Requested,
            headline: "Waiting for manager status.".to_string(),
            detail: Some(
                "Elixir created the request, but this manager does not expose a richer acquisition flow yet."
                    .to_string(),
            ),
            blocker: None,
            evidence: base_acquisition_evidence(
                intent.manager_item_id.is_some(),
                false,
                false,
                0,
                None,
                None,
            ),
            actions: Vec::new(),
            progress_percent: None,
            eta_seconds: None,
            downloader_label: None,
            protocol: None,
            children: Vec::new(),
        });
    }

    let Some(manager_item_id) = intent.manager_item_id.as_deref() else {
        return Ok(AcquisitionItemState {
            phase: AcquisitionPhase::Requested,
            headline: "Waiting for manager confirmation.".to_string(),
            detail: Some(
                "Elixir sent the request and is waiting for the manager to confirm the new item."
                    .to_string(),
            ),
            blocker: None,
            evidence: base_acquisition_evidence(false, false, false, 0, None, None),
            actions: Vec::new(),
            progress_percent: None,
            eta_seconds: None,
            downloader_label: None,
            protocol: None,
            children: Vec::new(),
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
            return Ok(acquisition_attention(
                "manager_status_unavailable",
                "Manager status unavailable",
                format!("Manager status could not be loaded: {err}"),
                base_acquisition_evidence(true, false, false, 0, None, None),
            ));
        }
    };

    if !library_matched || library_needs_hydration {
        match detect_and_ingest_managed_import_events(
            state,
            store,
            intent,
            &implementation,
            &item_value,
            &base_url,
            &api_key,
        )
        .await
        {
            Ok(true) => {
                library_matched = true;
            }
            Ok(false) => {}
            Err(err) => {
                warn!(
                    intent_id = %intent.intent_id,
                    implementation,
                    "failed to process managed import events: {err}"
                );
            }
        }
    }

    let queue_value = request_arr_json_with_query(
        &base_url,
        &api_key,
        &manager_queue_paths(&implementation),
        &[("page", "1".to_string()), ("pageSize", "250".to_string())],
    )
    .await
    .ok();

    let sonarr_episode_index = if implementation == "sonarr" {
        let has_matching_queue_entries = queue_value
            .as_ref()
            .map(extract_arr_queue_records)
            .unwrap_or_default()
            .into_iter()
            .any(|entry| {
                queue_entry_matches_manager_item(&entry, &implementation, manager_item_id)
            });
        if has_matching_queue_entries {
            match load_sonarr_episode_index(&base_url, &api_key, manager_item_id).await {
                Ok(index) => Some(index),
                Err(err) => {
                    warn!(
                        manager_item_id = manager_item_id,
                        "failed to load sonarr episode index for acquisition batch: {err}"
                    );
                    None
                }
            }
        } else {
            None
        }
    } else {
        None
    };

    Ok(derive_arr_acquisition_state(
        &implementation,
        manager_item_id,
        &item_value,
        queue_value.as_ref(),
        library_matched,
        recovery_view,
        downloader_progress,
        sonarr_episode_index.as_ref(),
    ))
}

async fn managed_library_needs_hydration(
    pool: &sqlx::AnyPool,
    intent: &crate::extensions::store::ManagedIngestIntent,
) -> AnyResult<bool> {
    let row = sqlx::query(
        "SELECT media_item_id, media_type
         FROM managed_library_provenance
         WHERE intent_id = ?
         ORDER BY updated_at DESC
         LIMIT 1",
    )
    .bind(intent.intent_id.to_string())
    .fetch_optional(pool)
    .await?;
    let Some(row) = row else {
        return Ok(true);
    };
    let media_item_id = row.get::<String, _>("media_item_id");
    let media_type_raw = row.get::<String, _>("media_type");
    let owner_type = if media_type_raw == "movie" {
        "movie"
    } else {
        "series"
    };
    let metadata_json: Option<String> = if owner_type == "movie" {
        sqlx::query_scalar::<sqlx::Any, String>(
            "SELECT COALESCE(CAST(metadata_json AS TEXT), '') FROM movies WHERE id = ? LIMIT 1",
        )
        .bind(&media_item_id)
        .fetch_optional(pool)
        .await?
    } else {
        sqlx::query_scalar::<sqlx::Any, String>(
            "SELECT COALESCE(CAST(metadata_json AS TEXT), '') FROM series WHERE id = ? LIMIT 1",
        )
        .bind(&media_item_id)
        .fetch_optional(pool)
        .await?
    };
    let has_metadata = metadata_json
        .as_deref()
        .map(str::trim)
        .is_some_and(|value| !value.is_empty());
    let artwork_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM artwork_refs
         WHERE owner_type = ?
           AND owner_id = ?
           AND kind IN ('poster', 'backdrop', 'banner')",
    )
    .bind(owner_type)
    .bind(&media_item_id)
    .fetch_one(pool)
    .await?;

    Ok(!has_metadata || artwork_count == 0)
}

async fn execute_find_another_release(
    state: &AppState,
    store: &ExtensionStore<'_>,
    intent_id: Uuid,
) -> AnyResult<String> {
    let _ = store;
    crate::acquisition::execute_find_another_release(state, intent_id).await
}

async fn load_acquisition_downloader_progress_index(
    state: &AppState,
    store: &ExtensionStore<'_>,
    providers: &[ProviderContext],
) -> AcquisitionDownloaderProgressIndex {
    let mut index = AcquisitionDownloaderProgressIndex::default();
    let mut seen_instances = HashSet::new();

    for provider in providers {
        let capability = provider.detail.provider.capability.as_str();
        if !matches!(capability, "downloader.nzb" | "downloader.torrent") {
            continue;
        }
        if provider.detail.provider.health_state == ProviderHealthState::Unhealthy {
            continue;
        }
        let instance_id = provider.detail.provider.instance_id;
        if !seen_instances.insert(instance_id) {
            continue;
        }

        let implementation = provider
            .detail
            .provider
            .implementation
            .as_deref()
            .map(|value| value.trim().to_ascii_lowercase())
            .unwrap_or_default();

        let result = match implementation.as_str() {
            "qbittorrent" => {
                load_qbittorrent_acquisition_progress_index(state, store, instance_id).await
            }
            "nzbget" => load_nzbget_acquisition_progress_index(state, store, instance_id).await,
            _ => Ok(AcquisitionDownloaderProgressIndex::default()),
        };

        match result {
            Ok(snapshot) => index.by_download_id.extend(snapshot.by_download_id),
            Err(err) => debug!(
                instance_id = %instance_id,
                capability,
                implementation,
                "skipping downloader-backed acquisition progress: {err}"
            ),
        }
    }

    index
}

pub(crate) fn select_nzbget_provider(providers: &[ProviderContext]) -> Option<&ProviderContext> {
    providers.iter().find(|provider| {
        provider.detail.provider.capability == "downloader.nzb"
            && provider
                .detail
                .provider
                .implementation
                .as_deref()
                .map(|value| value.trim().eq_ignore_ascii_case("nzbget"))
                .unwrap_or(false)
            && provider.detail.provider.health_state != ProviderHealthState::Unhealthy
    })
}

pub(crate) fn select_qbittorrent_provider(
    providers: &[ProviderContext],
) -> Option<&ProviderContext> {
    providers.iter().find(|provider| {
        provider.detail.provider.capability == "downloader.torrent"
            && provider
                .detail
                .provider
                .implementation
                .as_deref()
                .map(|value| value.trim().eq_ignore_ascii_case("qbittorrent"))
                .unwrap_or(false)
            && provider.detail.provider.health_state != ProviderHealthState::Unhealthy
    })
}

pub(crate) async fn remove_nzbget_download_by_download_id(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider: &ProviderContext,
    download_id: &str,
) -> AnyResult<bool> {
    let payload = super::extensions::request_instance_service_json(
        state,
        store,
        provider.detail.provider.instance_id,
        ReqwestMethod::POST,
        "jsonrpc",
        Some(json!({
            "version": "1.1",
            "method": "listgroups",
            "params": [0],
            "id": 1
        })),
    )
    .await?;

    if let Some(error) = payload.get("error").filter(|value| !value.is_null()) {
        bail!("nzbget listgroups returned error: {error}");
    }

    let groups: Vec<AcquisitionNzbgetGroup> = serde_json::from_value(
        payload
            .get("result")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("nzbget listgroups response missing result"))?,
    )
    .context("parsing nzbget groups for retry")?;

    let Some(group_id) = groups
        .iter()
        .find(|group| {
            nzbget_group_download_id(group)
                .map(|value| value.eq_ignore_ascii_case(download_id))
                .unwrap_or(false)
        })
        .map(|group| group.nzb_id)
    else {
        return Ok(false);
    };

    let payload = super::extensions::request_instance_service_json(
        state,
        store,
        provider.detail.provider.instance_id,
        ReqwestMethod::POST,
        "jsonrpc",
        Some(json!({
            "version": "1.1",
            "method": "editqueue",
            "params": ["GroupDelete", "", [group_id]],
            "id": 1
        })),
    )
    .await?;

    if let Some(error) = payload.get("error").filter(|value| !value.is_null()) {
        bail!("nzbget editqueue returned error: {error}");
    }
    let success = payload
        .get("result")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if !success {
        bail!("nzbget editqueue GroupDelete did not report success");
    }
    Ok(true)
}

pub(crate) async fn remove_qbittorrent_download_by_download_id(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider: &ProviderContext,
    download_id: &str,
) -> AnyResult<bool> {
    let hash = normalize_download_id(download_id);
    if hash.is_empty() {
        return Ok(false);
    }

    let mut fields = HashMap::new();
    fields.insert("hashes".to_string(), hash);
    fields.insert("deleteFiles".to_string(), "false".to_string());
    super::extensions::request_instance_service_form(
        state,
        store,
        provider.detail.provider.instance_id,
        "api/v2/torrents/delete",
        &fields,
    )
    .await?;
    Ok(true)
}

pub(crate) async fn request_arr_search_item(
    implementation: &str,
    base_url: &str,
    api_key: &str,
    manager_item_id: i64,
) -> AnyResult<()> {
    let body = match implementation {
        "sonarr" => json!({ "name": "SeriesSearch", "seriesId": manager_item_id }),
        "radarr" => json!({ "name": "MoviesSearch", "movieIds": [manager_item_id] }),
        _ => bail!("item search is not supported for implementation '{implementation}'"),
    };
    request_arr_write(
        base_url,
        api_key,
        &["api/v3/command", "api/v4/command"],
        ReqwestMethod::POST,
        &body,
    )
    .await
}

async fn load_qbittorrent_acquisition_progress_index(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
) -> AnyResult<AcquisitionDownloaderProgressIndex> {
    let value = super::extensions::request_instance_service_json(
        state,
        store,
        instance_id,
        ReqwestMethod::GET,
        "api/v2/torrents/info",
        None,
    )
    .await?;
    let torrents: Vec<AcquisitionQbittorrentTorrent> =
        serde_json::from_value(value).context("parsing qbittorrent acquisition queue")?;

    let mut index = AcquisitionDownloaderProgressIndex::default();
    for torrent in torrents {
        if let Some(progress) = qbittorrent_acquisition_progress(&torrent) {
            index.insert(&torrent.hash, progress);
        }
    }
    Ok(index)
}

async fn load_nzbget_acquisition_progress_index(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
) -> AnyResult<AcquisitionDownloaderProgressIndex> {
    let payload = super::extensions::request_instance_service_json(
        state,
        store,
        instance_id,
        ReqwestMethod::POST,
        "jsonrpc",
        Some(json!({
            "version": "1.1",
            "method": "listgroups",
            "params": [0],
            "id": 1
        })),
    )
    .await?;

    if let Some(error) = payload.get("error").filter(|value| !value.is_null()) {
        bail!("nzbget listgroups returned error: {error}");
    }

    let groups: Vec<AcquisitionNzbgetGroup> = serde_json::from_value(
        payload
            .get("result")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("nzbget listgroups response missing result"))?,
    )
    .context("parsing nzbget acquisition groups")?;

    let mut index = AcquisitionDownloaderProgressIndex::default();
    for group in groups {
        if let Some((download_id, progress)) = nzbget_acquisition_progress(&group) {
            index.insert(&download_id, progress);
        }
    }
    Ok(index)
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

#[derive(Debug, Deserialize)]
struct SonarrEpisodeRecord {
    #[serde(default)]
    id: i64,
    #[serde(rename = "seasonNumber", default)]
    season_number: i64,
    #[serde(rename = "episodeNumber", default)]
    episode_number: i64,
    #[serde(default)]
    title: String,
}

async fn load_sonarr_episode_index(
    base_url: &str,
    api_key: &str,
    series_id: &str,
) -> AnyResult<HashMap<i64, SonarrEpisodeDescriptor>> {
    let value = request_arr_json_with_query(
        base_url,
        api_key,
        &["api/v3/episode", "api/v4/episode"],
        &[("seriesId", series_id.to_string())],
    )
    .await?;

    let items = if let Some(entries) = value.as_array() {
        entries.clone()
    } else {
        value
            .get("records")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
    };

    let mut index = HashMap::new();
    for episode in serde_json::from_value::<Vec<SonarrEpisodeRecord>>(Value::Array(items))
        .context("parsing sonarr episode batch index")?
    {
        index.insert(
            episode.id,
            SonarrEpisodeDescriptor {
                season_number: episode.season_number,
                episode_number: episode.episode_number,
                title: episode.title,
            },
        );
    }
    Ok(index)
}

fn acquisition_attention(
    code: impl Into<String>,
    title: impl Into<String>,
    detail: impl Into<String>,
    evidence: Vec<FindMediaAcquisitionEvidence>,
) -> AcquisitionItemState {
    let title = title.into();
    let detail = detail.into();
    AcquisitionItemState {
        phase: AcquisitionPhase::NeedsAttention,
        headline: title.clone(),
        detail: Some(detail.clone()),
        blocker: Some(FindMediaAcquisitionBlocker {
            code: code.into(),
            title,
            detail,
            severity: "warning".to_string(),
        }),
        evidence,
        actions: Vec::new(),
        progress_percent: None,
        eta_seconds: None,
        downloader_label: None,
        protocol: None,
        children: Vec::new(),
    }
}

fn acquisition_phase_from_str(value: &str) -> AcquisitionPhase {
    match value {
        "requested" => AcquisitionPhase::Requested,
        "accepted_by_manager" => AcquisitionPhase::AcceptedByManager,
        "finding_another_release" => AcquisitionPhase::FindingAnotherRelease,
        "queued_in_downloader" => AcquisitionPhase::QueuedInDownloader,
        "downloading" => AcquisitionPhase::Downloading,
        "post_processing" => AcquisitionPhase::PostProcessing,
        "importing" => AcquisitionPhase::Importing,
        "completed" => AcquisitionPhase::Completed,
        "needs_attention" => AcquisitionPhase::NeedsAttention,
        "failed" => AcquisitionPhase::Failed,
        _ => AcquisitionPhase::Requested,
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

pub(crate) fn manager_queue_paths(implementation: &str) -> [&str; 2] {
    match implementation {
        "sonarr" => ["api/v3/queue", "api/v4/queue"],
        "radarr" => ["api/v3/queue", "api/v4/queue"],
        _ => ["", ""],
    }
}

async fn detect_and_ingest_managed_import_events(
    state: &AppState,
    store: &ExtensionStore<'_>,
    intent: &crate::extensions::store::ManagedIngestIntent,
    implementation: &str,
    item_value: &Value,
    base_url: &str,
    api_key: &str,
) -> AnyResult<bool> {
    let events =
        detect_managed_import_events(state, intent, implementation, item_value, base_url, api_key)
            .await?;
    let mut linked = false;

    for event in events {
        let persisted = store.upsert_managed_import_event(&event).await?;
        match ingest_managed_import_event(
            &state.db_pool,
            Some(state.metadata.as_ref()),
            Some(state.linkers.as_ref()),
            Some(state.artwork.as_ref()),
            intent,
            &persisted,
        )
        .await
        {
            Ok(Some(_)) => linked = true,
            Ok(None) => {
                debug!(
                    intent_id = %intent.intent_id,
                    event_key = %persisted.event_key,
                    "managed import event files are not visible to elixir yet"
                );
            }
            Err(err) => {
                let detail = err.to_string();
                store
                    .mark_managed_import_event_failed(persisted.event_id, &detail)
                    .await?;
                warn!(
                    intent_id = %intent.intent_id,
                    event_key = %persisted.event_key,
                    "failed to link managed import event: {detail}"
                );
            }
        }
    }

    Ok(linked)
}

async fn detect_managed_import_events(
    state: &AppState,
    intent: &crate::extensions::store::ManagedIngestIntent,
    implementation: &str,
    item_value: &Value,
    base_url: &str,
    api_key: &str,
) -> AnyResult<Vec<NewManagedImportEvent>> {
    match implementation {
        "radarr" => Ok(
            detect_radarr_managed_import_event(state, intent, item_value)
                .into_iter()
                .collect(),
        ),
        "sonarr" => {
            detect_sonarr_managed_import_events(state, intent, item_value, base_url, api_key).await
        }
        _ => Ok(Vec::new()),
    }
}

fn detect_radarr_managed_import_event(
    state: &AppState,
    intent: &crate::extensions::store::ManagedIngestIntent,
    item_value: &Value,
) -> Option<NewManagedImportEvent> {
    let manager_path = radarr_imported_file_path(item_value)?;
    let library_path = resolve_manager_imported_file_path(
        &state.settings.library.local_root,
        &manager_path,
        MediaType::Movie,
    )?;
    let file = ManagedImportFile {
        path: library_path,
        season_number: None,
        episode_number: None,
        absolute_episode_number: None,
        episode_title: None,
        size_bytes: item_value
            .get("movieFile")
            .and_then(|file| file.get("size"))
            .and_then(Value::as_i64),
        container: StdPath::new(&manager_path)
            .extension()
            .map(|value| value.to_string_lossy().to_string()),
        video_codec: None,
        audio_codec: None,
    };
    let event_key = managed_import_event_key(
        intent,
        "radarr",
        &[format!("movie:{}", file.path.to_ascii_lowercase())],
    );

    Some(NewManagedImportEvent {
        event_key,
        intent_id: intent.intent_id,
        media_type: MediaType::Movie,
        external_ids: Some(merge_external_ids_for_event(
            intent.external_ids.clone().unwrap_or_default(),
            radarr_external_ids_from_item(item_value),
        )),
        manager_provider_id: intent.manager_provider_id,
        manager_item_id: intent.manager_item_id.clone(),
        manager_label: intent.manager_label.clone(),
        manager_implementation: Some("radarr".to_string()),
        imported_files: vec![file],
        raw_manager_payload: Some(item_value.clone()),
        imported_at: Some(Utc::now()),
    })
}

async fn detect_sonarr_managed_import_events(
    state: &AppState,
    intent: &crate::extensions::store::ManagedIngestIntent,
    item_value: &Value,
    base_url: &str,
    api_key: &str,
) -> AnyResult<Vec<NewManagedImportEvent>> {
    let Some(manager_item_id) = intent.manager_item_id.as_deref() else {
        return Ok(Vec::new());
    };
    if !sonarr_item_has_files(item_value) {
        return Ok(Vec::new());
    }

    let episode_value = request_arr_json_with_query(
        base_url,
        api_key,
        &["api/v3/episode", "api/v4/episode"],
        &[("seriesId", manager_item_id.to_string())],
    )
    .await?;
    let episode_file_value = request_arr_json_with_query(
        base_url,
        api_key,
        &["api/v3/episodefile", "api/v4/episodefile"],
        &[("seriesId", manager_item_id.to_string())],
    )
    .await?;
    let episodes = arr_records(episode_value);
    let episode_files = arr_records(episode_file_value);
    let file_map: HashMap<i64, Value> = episode_files
        .into_iter()
        .filter_map(|file| {
            let id = file.get("id").and_then(Value::as_i64)?;
            Some((id, file))
        })
        .collect();

    let mut events = Vec::new();
    for episode in episodes {
        let Some(file_id) = episode.get("episodeFileId").and_then(Value::as_i64) else {
            continue;
        };
        if file_id <= 0 {
            continue;
        }
        let Some(file_value) = file_map.get(&file_id) else {
            continue;
        };
        let Some(manager_path) = sonarr_episode_file_path(item_value, file_value) else {
            continue;
        };
        let Some(library_path) = resolve_manager_imported_file_path(
            &state.settings.library.local_root,
            &manager_path,
            intent.media_type,
        ) else {
            warn!(
                intent_id = %intent.intent_id,
                manager_path = %manager_path,
                "sonarr imported file path is outside the managed tv root"
            );
            continue;
        };
        let season_number = episode
            .get("seasonNumber")
            .and_then(Value::as_i64)
            .map(|value| value as i32);
        let episode_number = episode
            .get("episodeNumber")
            .and_then(Value::as_i64)
            .map(|value| value as i32);
        let file = ManagedImportFile {
            path: library_path,
            season_number,
            episode_number,
            absolute_episode_number: episode
                .get("absoluteEpisodeNumber")
                .and_then(Value::as_i64)
                .map(|value| value as i32),
            episode_title: json_nonempty_string(episode.get("title")),
            size_bytes: file_value.get("size").and_then(Value::as_i64),
            container: StdPath::new(&manager_path)
                .extension()
                .map(|value| value.to_string_lossy().to_string()),
            video_codec: file_value
                .get("mediaInfo")
                .and_then(|info| json_nonempty_string(info.get("videoCodec")))
                .map(|value| value.to_ascii_lowercase()),
            audio_codec: file_value
                .get("mediaInfo")
                .and_then(|info| json_nonempty_string(info.get("audioCodec")))
                .map(|value| value.to_ascii_lowercase()),
        };
        let event_key = managed_import_event_key(
            intent,
            "sonarr",
            &[format!(
                "episode:{}:s{}e{}",
                file.path.to_ascii_lowercase(),
                file.season_number.unwrap_or_default(),
                file.episode_number.unwrap_or_default()
            )],
        );
        events.push(NewManagedImportEvent {
            event_key,
            intent_id: intent.intent_id,
            media_type: intent.media_type,
            external_ids: Some(merge_external_ids_for_event(
                intent.external_ids.clone().unwrap_or_default(),
                sonarr_external_ids_from_item(item_value),
            )),
            manager_provider_id: intent.manager_provider_id,
            manager_item_id: intent.manager_item_id.clone(),
            manager_label: intent.manager_label.clone(),
            manager_implementation: Some("sonarr".to_string()),
            imported_files: vec![file],
            raw_manager_payload: Some(json!({
                "series": item_value,
                "episode": episode,
                "episodeFile": file_value
            })),
            imported_at: Some(Utc::now()),
        });
    }

    Ok(events)
}

fn derive_arr_acquisition_state(
    implementation: &str,
    manager_item_id: &str,
    item_value: &Value,
    queue_value: Option<&Value>,
    library_matched: bool,
    recovery_view: Option<&IntentRecoveryView>,
    downloader_progress: &AcquisitionDownloaderProgressIndex,
    sonarr_episode_index: Option<&HashMap<i64, SonarrEpisodeDescriptor>>,
) -> AcquisitionItemState {
    let queue_entries: Vec<Value> = queue_value
        .map(extract_arr_queue_records)
        .unwrap_or_default()
        .into_iter()
        .filter(|entry| queue_entry_matches_manager_item(entry, implementation, manager_item_id))
        .collect();
    let queue_entry_count = queue_entries.len();

    let has_file = match implementation {
        "sonarr" => sonarr_item_has_files(item_value),
        "radarr" => radarr_item_has_file(item_value),
        _ => false,
    };

    if implementation == "sonarr" && !queue_entries.is_empty() {
        return derive_sonarr_batch_acquisition_state(
            item_value,
            &queue_entries,
            sonarr_episode_index,
            downloader_progress,
        );
    }

    if let Some(entry) = queue_entries.first() {
        return derive_arr_queue_entry_state(entry, queue_entry_count, downloader_progress);
    }

    if acquisition_is_completed(implementation, item_value, library_matched) {
        return completed_acquisition_state();
    }

    let downloader_label = queue_entries.first().and_then(queue_entry_downloader_label);
    let protocol = queue_entries.first().and_then(queue_entry_protocol);

    if has_file && library_matched {
        let mut evidence = base_acquisition_evidence(
            true,
            !queue_entries.is_empty(),
            true,
            queue_entry_count,
            downloader_label.as_deref(),
            protocol.as_deref(),
        );
        evidence.push(acquisition_evidence(
            "Elixir linked",
            "Yes",
            Some("success"),
        ));
        return AcquisitionItemState {
            phase: AcquisitionPhase::Importing,
            headline: "Imported files linked in Elixir.".to_string(),
            detail: Some(
                "The manager has imported files and Elixir linked them to the managed request. Waiting for remaining monitored media."
                    .to_string(),
            ),
            blocker: None,
            evidence,
            actions: Vec::new(),
            progress_percent: Some(100.0),
            eta_seconds: None,
            downloader_label,
            protocol,
            children: Vec::new(),
        };
    }

    if has_file {
        let mut evidence = base_acquisition_evidence(
            true,
            !queue_entries.is_empty(),
            true,
            queue_entry_count,
            downloader_label.as_deref(),
            protocol.as_deref(),
        );
        evidence.push(acquisition_evidence("Elixir linked", "No", Some("warning")));
        return AcquisitionItemState {
            phase: AcquisitionPhase::Importing,
            headline: "Imported in manager, not linked in Elixir.".to_string(),
            detail: Some(
                "The manager reports imported files, but Elixir has not linked the import to the managed request yet.".to_string(),
            ),
            blocker: None,
            evidence,
            actions: Vec::new(),
            progress_percent: Some(100.0),
            eta_seconds: None,
            downloader_label,
            protocol,
            children: Vec::new(),
        };
    }

    if acquisition_recovery_is_transitioning(recovery_view) {
        return AcquisitionItemState {
            phase: AcquisitionPhase::FindingAnotherRelease,
            headline: "Finding another release.".to_string(),
            detail: Some(
                "Elixir cleared the dead release and is waiting for the manager to provide another one."
                    .to_string(),
            ),
            blocker: None,
            evidence: base_acquisition_evidence(true, false, false, 0, None, None),
            actions: Vec::new(),
            progress_percent: None,
            eta_seconds: None,
            downloader_label: None,
            protocol: None,
            children: Vec::new(),
        };
    }

    AcquisitionItemState {
        phase: AcquisitionPhase::AcceptedByManager,
        headline: "Accepted by manager.".to_string(),
        detail: Some(
            "The manager accepted the item and is still looking for a valid release. No downloader has accepted it yet."
                .to_string(),
        ),
        blocker: None,
        evidence: base_acquisition_evidence(true, false, false, 0, None, None),
        actions: Vec::new(),
        progress_percent: None,
        eta_seconds: None,
        downloader_label: None,
        protocol: None,
        children: Vec::new(),
    }
}

fn derive_arr_queue_entry_state(
    entry: &Value,
    queue_entry_count: usize,
    downloader_progress_index: &AcquisitionDownloaderProgressIndex,
) -> AcquisitionItemState {
    let downloader_progress = queue_entry_download_id(entry)
        .and_then(|download_id| downloader_progress_index.get(&download_id));
    let progress_percent = downloader_progress.and_then(|item| item.progress_percent);
    let eta_seconds = downloader_progress.and_then(|item| item.eta_seconds);
    let downloader_label = queue_entry_downloader_label(entry);
    let protocol = queue_entry_protocol(entry);
    let status_message = queue_entry_status_message(entry);

    if let Some(message) = queue_entry_error_message(entry) {
        return acquisition_attention(
            "manager_queue_error",
            "Manager reported a problem",
            message,
            base_acquisition_evidence(
                true,
                false,
                false,
                queue_entry_count,
                downloader_label.as_deref(),
                protocol.as_deref(),
            ),
        );
    }

    if let Some(issue) = downloader_progress.and_then(|item| item.issue.clone()) {
        if issue.code.starts_with("nzbget_release_")
            || issue.code.starts_with("qbittorrent_release_")
        {
            let protected_payload = issue.code == "qbittorrent_release_failed_with_payload";
            let headline = if protected_payload {
                "Torrent needs manual recovery."
            } else {
                "Dead release detected."
            };
            let title = if protected_payload {
                issue.title.clone()
            } else {
                "Dead release detected".to_string()
            };
            return AcquisitionItemState {
                phase: AcquisitionPhase::NeedsAttention,
                headline: headline.to_string(),
                detail: Some(issue.detail.clone()),
                blocker: Some(FindMediaAcquisitionBlocker {
                    code: issue.code,
                    title,
                    detail: issue.detail,
                    severity: "warning".to_string(),
                }),
                evidence: base_acquisition_evidence(
                    true,
                    true,
                    false,
                    queue_entry_count,
                    downloader_label.as_deref(),
                    protocol.as_deref(),
                ),
                actions: Vec::new(),
                progress_percent: None,
                eta_seconds: None,
                downloader_label,
                protocol,
                children: Vec::new(),
            };
        }

        return AcquisitionItemState {
            phase: AcquisitionPhase::NeedsAttention,
            headline: issue.title.clone(),
            detail: Some(issue.detail.clone()),
            blocker: Some(FindMediaAcquisitionBlocker {
                code: issue.code,
                title: issue.title,
                detail: issue.detail,
                severity: "warning".to_string(),
            }),
            evidence: base_acquisition_evidence(
                true,
                true,
                false,
                queue_entry_count,
                downloader_label.as_deref(),
                protocol.as_deref(),
            ),
            actions: Vec::new(),
            progress_percent,
            eta_seconds,
            downloader_label,
            protocol,
            children: Vec::new(),
        };
    }

    let tracked_state = queue_entry_state(entry);
    let phase = if tracked_state.contains("import") {
        AcquisitionPhase::Importing
    } else if tracked_state.contains("post")
        || tracked_state.contains("extract")
        || tracked_state.contains("verif")
        || progress_percent.map(|value| value >= 99.5).unwrap_or(false)
    {
        AcquisitionPhase::PostProcessing
    } else if tracked_state.contains("download")
        || progress_percent.map(|value| value > 0.0).unwrap_or(false)
    {
        AcquisitionPhase::Downloading
    } else {
        AcquisitionPhase::QueuedInDownloader
    };

    let (headline, detail) =
        match phase {
            AcquisitionPhase::Downloading => {
                if let Some(label) = downloader_label.as_deref() {
                    (
                        format!("Downloading via {label}."),
                        Some(status_message.clone().unwrap_or_else(|| {
                            "Transfer is active in the downloader.".to_string()
                        })),
                    )
                } else {
                    (
                        "Download in progress.".to_string(),
                        Some(status_message.clone().unwrap_or_else(|| {
                            "Transfer is active in the downloader.".to_string()
                        })),
                    )
                }
            }
            AcquisitionPhase::PostProcessing => (
                "Download finished.".to_string(),
                Some("Waiting for verification, extraction, or downloader cleanup.".to_string()),
            ),
            AcquisitionPhase::Importing => (
                "Manager is importing the completed download.".to_string(),
                Some(
                    "The downloader finished and the manager is finishing the import.".to_string(),
                ),
            ),
            _ => {
                if let Some(label) = downloader_label.as_deref() {
                    (
                    format!("Queued with {label}."),
                    Some(status_message.clone().unwrap_or_else(|| {
                        "Manager handed the item to the downloader. Waiting for transfer to start."
                            .to_string()
                    })),
                )
                } else {
                    (
                    "Waiting in the download queue.".to_string(),
                    Some(status_message.unwrap_or_else(|| {
                        "Manager handed the item to the downloader. Waiting for transfer to start."
                            .to_string()
                    })),
                )
                }
            }
        };

    AcquisitionItemState {
        phase,
        headline,
        detail,
        blocker: None,
        evidence: base_acquisition_evidence(
            true,
            true,
            false,
            queue_entry_count,
            downloader_label.as_deref(),
            protocol.as_deref(),
        ),
        actions: Vec::new(),
        progress_percent,
        eta_seconds,
        downloader_label,
        protocol,
        children: Vec::new(),
    }
}

fn derive_sonarr_batch_acquisition_state(
    item_value: &Value,
    queue_entries: &[Value],
    sonarr_episode_index: Option<&HashMap<i64, SonarrEpisodeDescriptor>>,
    downloader_progress: &AcquisitionDownloaderProgressIndex,
) -> AcquisitionItemState {
    let mut children = queue_entries
        .iter()
        .map(|entry| build_sonarr_batch_child(entry, sonarr_episode_index, downloader_progress))
        .collect::<Vec<_>>();
    let counts = summarize_sonarr_batch_children(&children);
    let stats = sonarr_series_stats(item_value);
    let downloader_label = queue_entries.first().and_then(queue_entry_downloader_label);
    let protocol = queue_entries.first().and_then(queue_entry_protocol);
    let phase = summarize_sonarr_batch_phase(counts);

    let mut evidence = base_acquisition_evidence(
        true,
        true,
        false,
        queue_entries.len(),
        downloader_label.as_deref(),
        protocol.as_deref(),
    );
    evidence.push(acquisition_evidence(
        "Episodes queued",
        counts.queued.to_string(),
        Some("neutral"),
    ));
    if counts.downloading > 0 {
        evidence.push(acquisition_evidence(
            "Episodes downloading",
            counts.downloading.to_string(),
            Some("success"),
        ));
    }
    if counts.post_processing > 0 {
        evidence.push(acquisition_evidence(
            "Episodes post-processing",
            counts.post_processing.to_string(),
            Some("neutral"),
        ));
    }
    if counts.importing > 0 {
        evidence.push(acquisition_evidence(
            "Episodes importing",
            counts.importing.to_string(),
            Some("neutral"),
        ));
    }
    if counts.needs_attention + counts.failed > 0 {
        evidence.push(acquisition_evidence(
            "Episodes needing attention",
            (counts.needs_attention + counts.failed).to_string(),
            Some("warning"),
        ));
    }
    if stats.episode_count > 0 {
        evidence.push(acquisition_evidence(
            "Episodes imported",
            format!("{} / {}", stats.episode_file_count, stats.episode_count),
            Some(if stats.episode_file_count > 0 {
                "success"
            } else {
                "neutral"
            }),
        ));
    } else if stats.episode_file_count > 0 {
        evidence.push(acquisition_evidence(
            "Episodes imported",
            stats.episode_file_count.to_string(),
            Some("success"),
        ));
    }

    children.sort_by(|left, right| {
        let left_phase = acquisition_phase_from_str(&left.phase);
        let right_phase = acquisition_phase_from_str(&right.phase);
        left_phase
            .sort_priority()
            .cmp(&right_phase.sort_priority())
            .then_with(|| {
                left.title
                    .to_ascii_lowercase()
                    .cmp(&right.title.to_ascii_lowercase())
            })
    });

    AcquisitionItemState {
        phase,
        headline: format_sonarr_batch_headline(counts),
        detail: Some(format_sonarr_batch_detail(stats)),
        blocker: build_sonarr_batch_blocker(counts),
        evidence,
        actions: Vec::new(),
        progress_percent: None,
        eta_seconds: None,
        downloader_label,
        protocol,
        children,
    }
}

fn build_sonarr_batch_child(
    entry: &Value,
    sonarr_episode_index: Option<&HashMap<i64, SonarrEpisodeDescriptor>>,
    downloader_progress: &AcquisitionDownloaderProgressIndex,
) -> FindMediaAcquisitionChildItem {
    let state = derive_arr_queue_entry_state(entry, 1, downloader_progress);
    let (title, subtitle) = sonarr_batch_child_title(entry, sonarr_episode_index);
    let id = queue_entry_download_id(entry)
        .or_else(|| as_id_string(entry.get("episodeId").unwrap_or(&Value::Null)))
        .or_else(|| as_id_string(entry.get("id").unwrap_or(&Value::Null)))
        .unwrap_or_else(|| title.clone());

    FindMediaAcquisitionChildItem {
        id,
        title,
        subtitle,
        phase: state.phase.as_str().to_string(),
        phase_label: state.phase.label().to_string(),
        headline: state.headline,
        detail: state.detail,
        blocker: state.blocker,
        progress_percent: state.progress_percent,
        eta_seconds: state.eta_seconds,
        downloader_label: state.downloader_label,
        protocol: state.protocol,
    }
}

fn sonarr_batch_child_title(
    entry: &Value,
    sonarr_episode_index: Option<&HashMap<i64, SonarrEpisodeDescriptor>>,
) -> (String, Option<String>) {
    let release_title = entry
        .get("title")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    let Some(episode_id) = entry.get("episodeId").and_then(Value::as_i64) else {
        return (
            release_title
                .clone()
                .unwrap_or_else(|| "Episode".to_string()),
            None,
        );
    };
    let Some(episode) = sonarr_episode_index.and_then(|index| index.get(&episode_id)) else {
        return (
            release_title
                .clone()
                .unwrap_or_else(|| "Episode".to_string()),
            None,
        );
    };

    let code = format!(
        "S{:02}E{:02}",
        episode.season_number, episode.episode_number
    );
    let title = if episode.title.trim().is_empty() {
        code
    } else {
        format!("{code} • {}", episode.title.trim())
    };
    let subtitle = release_title.filter(|value| value.trim() != title);
    (title, subtitle)
}

fn summarize_sonarr_batch_children(
    children: &[FindMediaAcquisitionChildItem],
) -> SonarrBatchCounts {
    let mut counts = SonarrBatchCounts::default();
    for child in children {
        match acquisition_phase_from_str(&child.phase) {
            AcquisitionPhase::QueuedInDownloader => counts.queued += 1,
            AcquisitionPhase::Downloading => counts.downloading += 1,
            AcquisitionPhase::PostProcessing => counts.post_processing += 1,
            AcquisitionPhase::Importing => counts.importing += 1,
            AcquisitionPhase::NeedsAttention => counts.needs_attention += 1,
            AcquisitionPhase::Failed => counts.failed += 1,
            _ => {}
        }
    }
    counts
}

fn summarize_sonarr_batch_phase(counts: SonarrBatchCounts) -> AcquisitionPhase {
    if counts.needs_attention > 0 || counts.failed > 0 {
        AcquisitionPhase::NeedsAttention
    } else if counts.downloading > 0 {
        AcquisitionPhase::Downloading
    } else if counts.post_processing > 0 {
        AcquisitionPhase::PostProcessing
    } else if counts.importing > 0 {
        AcquisitionPhase::Importing
    } else {
        AcquisitionPhase::QueuedInDownloader
    }
}

fn format_sonarr_batch_headline(counts: SonarrBatchCounts) -> String {
    let mut parts = Vec::new();
    if counts.needs_attention > 0 || counts.failed > 0 {
        parts.push(format!(
            "{} need attention",
            format_episode_count(counts.needs_attention + counts.failed)
        ));
    }
    if counts.downloading > 0 {
        parts.push(format!(
            "{} downloading",
            format_episode_count(counts.downloading)
        ));
    }
    if counts.post_processing > 0 {
        parts.push(format!(
            "{} post-processing",
            format_episode_count(counts.post_processing)
        ));
    }
    if counts.importing > 0 {
        parts.push(format!(
            "{} importing",
            format_episode_count(counts.importing)
        ));
    }
    if counts.queued > 0 {
        parts.push(format!("{} queued", format_episode_count(counts.queued)));
    }

    if parts.is_empty() {
        "Waiting for episode activity.".to_string()
    } else {
        format!("{}.", parts.join(", "))
    }
}

fn format_sonarr_batch_detail(stats: SonarrSeriesStats) -> String {
    if stats.episode_count > 0 {
        format!(
            "Sonarr is handling this series as a batch. {} of {} episode files are imported so far.",
            stats.episode_file_count, stats.episode_count
        )
    } else if stats.episode_file_count > 0 {
        format!(
            "Sonarr is handling this series as a batch. {} episode files are already imported.",
            stats.episode_file_count
        )
    } else {
        "Sonarr is handling this series as a batch of episode downloads.".to_string()
    }
}

fn build_sonarr_batch_blocker(counts: SonarrBatchCounts) -> Option<FindMediaAcquisitionBlocker> {
    let attention = counts.needs_attention + counts.failed;
    (attention > 0).then(|| FindMediaAcquisitionBlocker {
        code: "series_batch_attention".to_string(),
        title: "Episodes need attention".to_string(),
        detail: format!(
            "{} in this series currently need attention.",
            format_episode_count(attention)
        ),
        severity: "warning".to_string(),
    })
}

fn sonarr_series_stats(value: &Value) -> SonarrSeriesStats {
    let statistics = value.get("statistics");
    SonarrSeriesStats {
        episode_count: statistics
            .and_then(|item| item.get("episodeCount"))
            .and_then(Value::as_u64)
            .map(|value| value as usize)
            .unwrap_or(0),
        episode_file_count: statistics
            .and_then(|item| item.get("episodeFileCount"))
            .and_then(Value::as_u64)
            .map(|value| value as usize)
            .unwrap_or(0),
    }
}

fn sonarr_series_is_complete(value: &Value) -> bool {
    let stats = sonarr_series_stats(value);
    stats.episode_count > 0 && stats.episode_file_count >= stats.episode_count
}

fn acquisition_is_completed(
    implementation: &str,
    item_value: &Value,
    library_matched: bool,
) -> bool {
    if !library_matched {
        return false;
    }
    match implementation {
        "sonarr" => sonarr_series_is_complete(item_value),
        "radarr" => true,
        _ => true,
    }
}

fn format_episode_count(count: usize) -> String {
    if count == 1 {
        "1 episode".to_string()
    } else {
        format!("{count} episodes")
    }
}

fn base_acquisition_evidence(
    manager_accepted: bool,
    downloader_accepted: bool,
    imported: bool,
    queue_entries: usize,
    downloader_label: Option<&str>,
    protocol: Option<&str>,
) -> Vec<FindMediaAcquisitionEvidence> {
    let mut evidence = vec![
        acquisition_evidence(
            "Manager accepted",
            if manager_accepted { "Yes" } else { "No" },
            Some(if manager_accepted {
                "success"
            } else {
                "warning"
            }),
        ),
        acquisition_evidence(
            "Downloader accepted",
            if downloader_accepted { "Yes" } else { "No" },
            Some(if downloader_accepted {
                "success"
            } else {
                "warning"
            }),
        ),
        acquisition_evidence(
            "Imported",
            if imported { "Yes" } else { "No" },
            Some(if imported { "success" } else { "neutral" }),
        ),
        acquisition_evidence("Queue entries", queue_entries.to_string(), Some("neutral")),
    ];

    if let Some(value) = downloader_label.filter(|value| !value.trim().is_empty()) {
        evidence.push(acquisition_evidence(
            "Downloader",
            value.trim(),
            Some("neutral"),
        ));
    }
    if let Some(value) = protocol.filter(|value| !value.trim().is_empty()) {
        evidence.push(acquisition_evidence(
            "Protocol",
            value.trim(),
            Some("neutral"),
        ));
    }

    evidence
}

fn completed_acquisition_state() -> AcquisitionItemState {
    AcquisitionItemState {
        phase: AcquisitionPhase::Completed,
        headline: "Downloaded.".to_string(),
        detail: None,
        blocker: None,
        evidence: Vec::new(),
        actions: Vec::new(),
        progress_percent: None,
        eta_seconds: None,
        downloader_label: None,
        protocol: None,
        children: Vec::new(),
    }
}

fn acquisition_evidence(
    label: impl Into<String>,
    value: impl Into<String>,
    tone: Option<&str>,
) -> FindMediaAcquisitionEvidence {
    FindMediaAcquisitionEvidence {
        label: label.into(),
        value: value.into(),
        tone: tone.map(str::to_string),
    }
}

pub(crate) fn extract_arr_queue_records(value: &Value) -> Vec<Value> {
    if let Some(items) = value.as_array() {
        return items.clone();
    }
    value
        .get("records")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

pub(crate) fn queue_entry_matches_manager_item(
    entry: &Value,
    implementation: &str,
    manager_item_id: &str,
) -> bool {
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
    value
        .get("hasFile")
        .and_then(Value::as_bool)
        .unwrap_or(false)
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

fn radarr_imported_file_path(value: &Value) -> Option<String> {
    let movie_file = value.get("movieFile")?;
    json_nonempty_string(movie_file.get("path"))
        .or_else(|| {
            let movie_path = json_nonempty_string(value.get("path"))?;
            let relative_path = json_nonempty_string(movie_file.get("relativePath"))?;
            Some(join_arr_path(&movie_path, &relative_path))
        })
        .filter(|path| !path.trim().is_empty())
}

fn sonarr_episode_file_path(series: &Value, file: &Value) -> Option<String> {
    json_nonempty_string(file.get("path"))
        .or_else(|| {
            let series_path = json_nonempty_string(series.get("path"))?;
            let relative_path = json_nonempty_string(file.get("relativePath"))?;
            Some(join_arr_path(&series_path, &relative_path))
        })
        .filter(|path| !path.trim().is_empty())
}

fn resolve_manager_imported_file_path(
    local_root: &str,
    manager_path: &str,
    media_type: MediaType,
) -> Option<String> {
    let manager_path = manager_path.trim();
    if manager_path.is_empty() {
        return None;
    }
    let local_root_path = StdPath::new(local_root);
    let manager_path_obj = StdPath::new(manager_path);
    if manager_path_obj.starts_with(local_root_path) {
        return Some(manager_path_obj.to_string_lossy().to_string());
    }
    let managed_root = match media_type {
        MediaType::Movie => "movies",
        MediaType::Series | MediaType::Anime => "tv",
    };
    let absolute_prefix = format!("/{managed_root}/");
    let relative = manager_path
        .strip_prefix(&absolute_prefix)
        .or_else(|| manager_path.strip_prefix(&format!("{managed_root}/")))?;
    Some(
        local_root_path
            .join(managed_root)
            .join(relative)
            .to_string_lossy()
            .to_string(),
    )
}

fn arr_records(value: Value) -> Vec<Value> {
    if let Some(items) = value.as_array() {
        return items.clone();
    }
    value
        .get("records")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

fn managed_import_event_key(
    intent: &crate::extensions::store::ManagedIngestIntent,
    implementation: &str,
    parts: &[String],
) -> String {
    let mut parts = parts.to_vec();
    parts.sort();
    format!(
        "{}:{}:{}:{}",
        intent.intent_id,
        implementation,
        intent.manager_item_id.as_deref().unwrap_or("unassigned"),
        parts.join("|")
    )
}

fn merge_external_ids_for_event(mut base: ExternalIds, incoming: ExternalIds) -> ExternalIds {
    base.imdb = base.imdb.or(incoming.imdb);
    base.tmdb = base.tmdb.or(incoming.tmdb);
    base.tvdb = base.tvdb.or(incoming.tvdb);
    base.tvdb_series = base.tvdb_series.or(incoming.tvdb_series);
    base.tvdb_movie = base.tvdb_movie.or(incoming.tvdb_movie);
    base.anilist = base.anilist.or(incoming.anilist);
    base.anidb = base.anidb.or(incoming.anidb);
    base.mal = base.mal.or(incoming.mal);
    base.kitsu = base.kitsu.or(incoming.kitsu);
    base
}

fn radarr_external_ids_from_item(value: &Value) -> ExternalIds {
    ExternalIds {
        imdb: json_nonempty_string(value.get("imdbId")),
        tmdb: value
            .get("tmdbId")
            .and_then(Value::as_i64)
            .map(|value| value.to_string())
            .or_else(|| json_nonempty_string(value.get("tmdbId"))),
        tvdb: json_nonempty_string(value.get("tvdbId")),
        tvdb_series: None,
        tvdb_movie: json_nonempty_string(value.get("tvdbId")),
        anilist: None,
        anidb: None,
        mal: None,
        kitsu: None,
    }
}

fn sonarr_external_ids_from_item(value: &Value) -> ExternalIds {
    let tvdb = value
        .get("tvdbId")
        .and_then(Value::as_i64)
        .map(|value| value.to_string())
        .or_else(|| json_nonempty_string(value.get("tvdbId")));
    ExternalIds {
        imdb: json_nonempty_string(value.get("imdbId")),
        tmdb: None,
        tvdb: tvdb.clone(),
        tvdb_series: tvdb,
        tvdb_movie: None,
        anilist: None,
        anidb: None,
        mal: None,
        kitsu: None,
    }
}

fn json_nonempty_string(value: Option<&Value>) -> Option<String> {
    value
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn join_arr_path(root: &str, relative: &str) -> String {
    format!(
        "{}/{}",
        root.trim_end_matches('/'),
        relative.trim_start_matches('/')
    )
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

fn queue_entry_status_message(entry: &Value) -> Option<String> {
    let message = entry
        .get("errorMessage")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())?;

    let tracked_status = entry
        .get("trackedDownloadStatus")
        .and_then(Value::as_str)
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_default();

    tracked_status.eq("ok").then(|| message.to_string())
}

fn queue_entry_error_message(entry: &Value) -> Option<String> {
    let error_message = entry
        .get("errorMessage")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let status = entry
        .get("status")
        .and_then(Value::as_str)
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_default();
    if status.contains("downloadclientunavailable") {
        return Some(error_message.unwrap_or_else(|| {
            "Manager could not hand this release to a download client.".to_string()
        }));
    }
    let tracked_status = entry
        .get("trackedDownloadStatus")
        .and_then(Value::as_str)
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_default();
    if tracked_status.contains("warning")
        || tracked_status.contains("error")
        || tracked_status.contains("unavailable")
    {
        return Some(
            error_message
                .unwrap_or_else(|| "Downloader reported a problem for this item.".to_string()),
        );
    }
    None
}

pub(crate) fn queue_entry_download_id(entry: &Value) -> Option<String> {
    entry
        .get("downloadId")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

pub(crate) fn queue_entry_downloader_label(entry: &Value) -> Option<String> {
    entry
        .get("downloadClient")
        .or_else(|| entry.get("downloadClientName"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn queue_entry_protocol(entry: &Value) -> Option<String> {
    entry
        .get("protocol")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn nzbget_group_download_id(group: &AcquisitionNzbgetGroup) -> Option<&str> {
    group.parameters.iter().find_map(|parameter| {
        parameter
            .name
            .trim()
            .eq_ignore_ascii_case(NZBGET_DRONE_DOWNLOAD_ID_PARAM)
            .then_some(parameter.value.trim())
            .filter(|value| !value.is_empty())
    })
}

fn normalize_download_id(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn qbittorrent_acquisition_progress(
    torrent: &AcquisitionQbittorrentTorrent,
) -> Option<AcquisitionDownloaderProgress> {
    let hash = torrent.hash.trim();
    if hash.is_empty() {
        return None;
    }
    let total_size = torrent.total_size.or(torrent.size);
    let progress_percent = torrent
        .progress
        .map(|value| (value * 100.0).clamp(0.0, 100.0))
        .or_else(|| {
            let total_size = total_size?;
            if total_size == 0 {
                return None;
            }
            let remaining = torrent.amount_left.unwrap_or(total_size);
            Some(
                (((total_size.saturating_sub(remaining)) as f64 / total_size as f64) * 100.0)
                    .clamp(0.0, 100.0),
            )
        });
    let issue = qbittorrent_torrent_issue(torrent, progress_percent);
    Some(AcquisitionDownloaderProgress {
        progress_percent,
        eta_seconds: torrent.eta.filter(|value| *value > 0),
        issue,
    })
}

fn qbittorrent_torrent_issue(
    torrent: &AcquisitionQbittorrentTorrent,
    progress_percent: Option<f64>,
) -> Option<AcquisitionDownloaderIssue> {
    let state = torrent
        .state
        .as_deref()
        .map(str::trim)
        .unwrap_or_default()
        .to_ascii_lowercase();
    if state.is_empty() {
        return None;
    }

    let downloaded = torrent.downloaded.unwrap_or(0);
    let has_progress = progress_percent.unwrap_or(0.0) > 0.0 || downloaded > 0;
    let has_local_payload = has_progress
        || progress_percent.map(|value| value >= 99.5).unwrap_or(false)
        || torrent.amount_left == Some(0);

    if matches!(state.as_str(), "error" | "missingfiles") {
        if has_local_payload {
            return Some(AcquisitionDownloaderIssue {
                code: "qbittorrent_release_failed_with_payload".to_string(),
                title: "Torrent needs manual recovery".to_string(),
                detail: "qBittorrent marked the current torrent as failed or missing, but local payload data is present. Elixir will not auto-remove it; inspect the downloader or manually import the recovered files."
                    .to_string(),
            });
        }
        return Some(AcquisitionDownloaderIssue {
            code: "qbittorrent_release_failed".to_string(),
            title: "Release failed in qBittorrent".to_string(),
            detail: "qBittorrent marked the current torrent as failed or missing. Elixir will remove it and ask the manager for another release."
                .to_string(),
        });
    }

    let added_on = torrent.added_on.and_then(timestamp_to_datetime)?;
    let age_seconds = (Utc::now() - added_on).num_seconds();
    let connected_seeds = torrent.num_seeds.unwrap_or(0);
    let connected_peers = torrent.num_leechs.unwrap_or(0);
    let known_seeds = torrent.num_complete.unwrap_or(0);
    let known_peers = torrent.num_incomplete.unwrap_or(0);
    let download_rate = torrent.dlspeed.unwrap_or(0);
    let no_connections = connected_seeds == 0 && connected_peers == 0;

    if matches!(state.as_str(), "metadl" | "forcedmetadl")
        && age_seconds >= TORRENT_METADATA_STALL_TIMEOUT_SECONDS
        && no_connections
        && download_rate == 0
    {
        return Some(AcquisitionDownloaderIssue {
            code: "qbittorrent_release_metadata_stalled".to_string(),
            title: "Torrent metadata never resolved".to_string(),
            detail: format!(
                "qBittorrent has been waiting on torrent metadata for over {} minutes with no connected peers. Elixir will remove it and ask the manager for another release. Known swarm: {} seeders, {} peers.",
                TORRENT_METADATA_STALL_TIMEOUT_SECONDS / 60,
                known_seeds,
                known_peers
            ),
        });
    }

    if matches!(state.as_str(), "downloading" | "stalleddl" | "forceddl")
        && age_seconds >= TORRENT_ZERO_PROGRESS_TIMEOUT_SECONDS
        && !has_progress
        && no_connections
        && download_rate == 0
    {
        return Some(AcquisitionDownloaderIssue {
            code: "qbittorrent_release_zero_progress".to_string(),
            title: "Torrent never became reachable".to_string(),
            detail: format!(
                "qBittorrent has had this torrent for over {} minutes with no progress, no connected peers, and no download speed. Elixir will remove it and ask the manager for another release. Known swarm: {} seeders, {} peers.",
                TORRENT_ZERO_PROGRESS_TIMEOUT_SECONDS / 60,
                known_seeds,
                known_peers
            ),
        });
    }

    None
}

fn timestamp_to_datetime(timestamp: i64) -> Option<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp(timestamp, 0)
}

fn nzbget_acquisition_progress(
    group: &AcquisitionNzbgetGroup,
) -> Option<(String, AcquisitionDownloaderProgress)> {
    let download_id = nzbget_group_download_id(group)?.to_string();

    let total_size = combine_size_parts(group.file_size_hi, group.file_size_lo);
    let remaining_size = combine_size_parts(group.remaining_size_hi, group.remaining_size_lo);
    let downloaded_size = combine_size_parts(group.downloaded_size_hi, group.downloaded_size_lo);
    let progress_percent = downloader_progress_percent(total_size, remaining_size, downloaded_size);

    Some((
        download_id,
        AcquisitionDownloaderProgress {
            progress_percent,
            eta_seconds: None,
            issue: nzbget_group_issue(group),
        },
    ))
}

fn nzbget_group_issue(group: &AcquisitionNzbgetGroup) -> Option<AcquisitionDownloaderIssue> {
    let status = group
        .status
        .as_deref()
        .map(str::trim)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let failed_articles = group.failed_articles.unwrap_or(0);
    if status.contains("failure") || status.contains("warning") {
        return Some(AcquisitionDownloaderIssue {
            code: "nzbget_release_failed".to_string(),
            title: "Release failed in NZBGet".to_string(),
            detail: nzbget_issue_detail(failed_articles, group.health, group.critical_health),
        });
    }

    if let (Some(health), Some(critical_health)) = (group.health, group.critical_health)
        && health <= critical_health
    {
        return Some(AcquisitionDownloaderIssue {
            code: "nzbget_release_unrecoverable".to_string(),
            title: "Release is damaged".to_string(),
            detail: nzbget_issue_detail(failed_articles, Some(health), Some(critical_health)),
        });
    }

    None
}

fn nzbget_issue_detail(
    failed_articles: u64,
    health: Option<i64>,
    critical_health: Option<i64>,
) -> String {
    let mut detail =
        "NZBGet reports this release is damaged or unrecoverable. Remove it and search for another release."
            .to_string();
    if failed_articles > 0 {
        detail.push_str(&format!(" Failed articles: {failed_articles}."));
    }
    if let (Some(health), Some(critical_health)) = (health, critical_health) {
        detail.push_str(&format!(
            " Health {health} is at or below the repair threshold {critical_health}."
        ));
    }
    detail
}

fn downloader_progress_percent(
    total_size: Option<u64>,
    remaining_size: Option<u64>,
    downloaded_size: Option<u64>,
) -> Option<f64> {
    let total_size = total_size?;
    if total_size == 0 {
        return None;
    }

    let completed = if let Some(downloaded_size) = downloaded_size {
        downloaded_size
    } else if let Some(remaining_size) = remaining_size {
        total_size.saturating_sub(remaining_size)
    } else {
        return None;
    };

    Some(((completed as f64 / total_size as f64) * 100.0).clamp(0.0, 100.0))
}

fn acquisition_recovery_is_transitioning(recovery_view: Option<&IntentRecoveryView>) -> bool {
    let Some(recovery_view) = recovery_view else {
        return false;
    };
    if !recovery_view.last_attempt_succeeded {
        return false;
    }
    if recovery_view
        .last_attempted_download_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_none()
    {
        return false;
    }

    let now = Utc::now();
    if recovery_view
        .cooldown_until
        .map(|until| until > now)
        .unwrap_or(false)
    {
        return true;
    }

    recovery_view
        .last_attempted_at
        .map(|at| (now - at) <= ChronoDuration::seconds(AUTO_RECOVERY_COOLDOWN_SECONDS.max(1)))
        .unwrap_or(false)
}

fn combine_size_parts(hi: Option<u64>, lo: Option<u64>) -> Option<u64> {
    match (hi, lo) {
        (Some(hi), Some(lo)) => Some((hi << 32) | lo),
        (Some(hi), None) => Some(hi << 32),
        (None, Some(lo)) => Some(lo),
        (None, None) => None,
    }
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
    for item in desired {
        let Some(extension) = extension_map.get(&item.blueprint_extension_id) else {
            continue;
        };
        let Ok(manifest) =
            serde_json::from_value::<ExtensionManifest>(extension.manifest_json.clone())
        else {
            continue;
        };
        if let Some(provider_id) =
            resolve_execution_preferred_manager(&extension_map, &manifest, providers, media_type)
        {
            return Ok(Some(provider_id));
        }
    }

    Ok(None)
}

fn resolve_execution_preferred_manager(
    extension_map: &HashMap<String, crate::db::models::Extension>,
    manifest: &ExtensionManifest,
    providers: &[ProviderContext],
    media_type: MediaType,
) -> Option<Uuid> {
    let execution = manifest.execution.as_ref()?;
    for capability in manager_capabilities_for_media_type(media_type) {
        for instance in &execution.instances {
            let Some(extension) = extension_map.get(&instance.extension_id) else {
                continue;
            };
            let Ok(module_manifest) =
                serde_json::from_value::<ExtensionManifest>(extension.manifest_json.clone())
            else {
                continue;
            };
            let provides_manager = module_manifest.provides.iter().any(|provide| {
                provide.capability == *capability && provide.slot.eq_ignore_ascii_case("default")
            });
            if !provides_manager {
                continue;
            }
            if let Some(provider) = providers.iter().find(|provider| {
                provider.detail.extension_id == instance.extension_id
                    && provider.instance_name == instance.name
                    && provider.detail.provider.capability == *capability
                    && provider
                        .detail
                        .provider
                        .slot_id
                        .eq_ignore_ascii_case("default")
            }) {
                return Some(provider.detail.provider.provider_id);
            }
        }
    }
    None
}

fn manager_capabilities_for_media_type(media_type: MediaType) -> &'static [&'static str] {
    match media_type {
        MediaType::Movie => &["media.manager.movies"],
        MediaType::Series => &["media.manager.tv"],
        MediaType::Anime => &["media.manager.anime", "media.manager.tv"],
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

pub(crate) async fn load_provider_contexts(
    store: &ExtensionStore<'_>,
) -> AnyResult<Vec<ProviderContext>> {
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
    let defaults =
        load_manager_control_defaults(store, provider.detail.provider.instance_id).await?;
    Ok(FindMediaAddOptions {
        monitor: Some(options.monitor.unwrap_or(defaults.monitor_on_add)),
        search: Some(options.search.unwrap_or(defaults.search_on_add)),
        root_folder_path: options.root_folder_path.clone(),
        quality_profile_id: options.quality_profile_id,
    })
}

async fn build_manager_driver_ctx(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider: &ProviderContext,
) -> AnyResult<DriverCtx> {
    let implementation = provider
        .detail
        .provider
        .implementation
        .as_deref()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty());

    let endpoint_json = provider
        .detail
        .provider
        .endpoint_json
        .clone()
        .ok_or_else(|| anyhow::anyhow!("provider endpoint is missing"))?;
    let endpoint: ProviderEndpoint =
        serde_json::from_value(endpoint_json).context("parsing provider endpoint")?;
    let transport_base_url =
        resolve_provider_transport_base_url(provider.detail.provider.instance_id, &endpoint)
            .await?;

    let mut secrets = HashMap::new();
    if let Some(api_key) = resolve_arr_api_key(
        state,
        store,
        provider,
        implementation.as_deref().unwrap_or_default(),
    )
    .await
    .ok()
    .filter(|value| !value.trim().is_empty())
    {
        secrets.insert("api_key".to_string(), api_key.clone());
        if let Some(implementation) = implementation.as_deref() {
            secrets.insert(format!("{implementation}_api_key"), api_key);
        }
    }

    Ok(DriverCtx::new(
        provider.detail.provider.provider_id,
        provider.detail.provider.instance_id,
        provider.detail.provider.capability.clone(),
        endpoint,
        Some(transport_base_url),
        implementation,
        provider.instance_config.clone(),
        secrets,
    ))
}

async fn add_with_manager_provider(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider: &ProviderContext,
    media_type: MediaType,
    item: &FindMediaAddItem,
    options: &FindMediaAddOptions,
) -> AnyResult<Option<String>> {
    let title = item
        .title
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("item.title is required"))?
        .to_string();
    let ctx = build_manager_driver_ctx(state, store, provider).await?;

    debug!(
        manager_provider_id = %provider.detail.provider.provider_id,
        implementation = ?ctx.implementation,
        capability = %provider.detail.provider.capability,
        base_url = ?ctx.transport_base_url,
        "dispatching find media add to manager provider"
    );

    let effective_options = resolve_find_media_add_options(store, provider, options).await?;
    let request = AddMediaRequest {
        media_type,
        title,
        year: item.year,
        external_ids: item.external_ids.clone(),
        options: DriverAddMediaOptions {
            monitor: effective_options.monitor.unwrap_or(true),
            search: effective_options.search.unwrap_or(false),
            root_folder_path: effective_options.root_folder_path.clone(),
            quality_profile_id: effective_options.quality_profile_id,
        },
    };
    let drivers = DriverRegistry::with_defaults();
    let driver = drivers
        .get(&provider.detail.provider.capability)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no driver registered for capability '{}'",
                provider.detail.provider.capability
            )
        })?;
    Ok(driver.add_media(ctx, request).await?.manager_item_id)
}

fn normalize_name(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .replace([' ', '-', '_', ':'], "")
}

fn managed_tombstone_media_type_compatible(candidate: MediaType, blocked: MediaType) -> bool {
    candidate == blocked
        || (candidate == MediaType::Series && blocked == MediaType::Anime)
        || (candidate == MediaType::Anime && blocked == MediaType::Series)
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

pub(crate) async fn resolve_arr_api_key(
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
        let tvdb_movie = item.get("tvdbId").and_then(as_id_string);
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
                tvdb_movie,
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

pub(crate) async fn request_arr_json_with_query<P: AsRef<str>>(
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

pub(crate) async fn request_arr_write(
    base_url: &str,
    api_key: &str,
    paths: &[&str],
    method: ReqwestMethod,
    body: &Value,
) -> AnyResult<()> {
    let client = build_arr_client(api_key)?;

    for path in paths {
        if path.trim().is_empty() {
            continue;
        }
        let url = build_arr_lookup_url(base_url, path)?;
        let response = client
            .request(method.clone(), url)
            .json(body)
            .send()
            .await
            .with_context(|| format!("calling {path}"))?;
        if response.status() == ReqwestStatusCode::NOT_FOUND {
            continue;
        }
        if !response.status().is_success() {
            let status = response.status();
            let detail = response.text().await.unwrap_or_default();
            bail!("{path} failed ({status}): {}", detail.trim());
        }
        return Ok(());
    }

    bail!("manager write endpoint is not available");
}

pub(crate) async fn resolve_provider_transport_base_url(
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
    fn derive_arr_acquisition_state_marks_downloading_from_queue_state_without_fake_progress() {
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

        let state = derive_arr_acquisition_state(
            "radarr",
            "42",
            &item,
            Some(&queue),
            false,
            None,
            &AcquisitionDownloaderProgressIndex::default(),
            None,
        );
        assert_eq!(state.phase, AcquisitionPhase::Downloading);
        assert_eq!(state.headline, "Downloading via default (nzbget).");
        assert_eq!(state.downloader_label.as_deref(), Some("default (nzbget)"));
        assert_eq!(state.protocol.as_deref(), Some("usenet"));
        assert_eq!(state.progress_percent, None);
        assert_eq!(state.eta_seconds, None);
    }

    #[test]
    fn derive_arr_acquisition_state_marks_importing_when_files_exist() {
        let item = json!({
            "statistics": {
                "episodeFileCount": 3,
                "sizeOnDisk": 12345
            }
        });

        let state = derive_arr_acquisition_state(
            "sonarr",
            "9",
            &item,
            None,
            false,
            None,
            &AcquisitionDownloaderProgressIndex::default(),
            None,
        );
        assert_eq!(state.phase, AcquisitionPhase::Importing);
        assert_eq!(state.headline, "Imported in manager, not linked in Elixir.");
        assert_eq!(
            state.detail.as_deref(),
            Some(
                "The manager reports imported files, but Elixir has not linked the import to the managed request yet."
            )
        );
        assert!(
            state
                .evidence
                .iter()
                .any(|item| item.label == "Elixir linked" && item.value == "No")
        );
    }

    #[test]
    fn derive_arr_acquisition_state_does_not_report_unlinked_when_partial_series_linked() {
        let item = json!({
            "statistics": {
                "episodeFileCount": 3,
                "episodeCount": 10,
                "sizeOnDisk": 12345
            }
        });

        let state = derive_arr_acquisition_state(
            "sonarr",
            "9",
            &item,
            None,
            true,
            None,
            &AcquisitionDownloaderProgressIndex::default(),
            None,
        );
        assert_eq!(state.phase, AcquisitionPhase::Importing);
        assert_eq!(state.headline, "Imported files linked in Elixir.");
        assert!(
            state
                .evidence
                .iter()
                .any(|item| item.label == "Elixir linked" && item.value == "Yes")
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
                "trackedDownloadStatus": "warning",
                "errorMessage": "Release was rejected"
            }
        ]);

        let state = derive_arr_acquisition_state(
            "radarr",
            "77",
            &item,
            Some(&queue),
            false,
            None,
            &AcquisitionDownloaderProgressIndex::default(),
            None,
        );
        assert_eq!(state.phase, AcquisitionPhase::NeedsAttention);
        assert_eq!(
            state.blocker.as_ref().map(|item| item.detail.as_str()),
            Some("Release was rejected")
        );
    }

    #[test]
    fn derive_arr_acquisition_state_keeps_metadata_fetch_as_downloading() {
        let item = json!({
            "hasFile": false
        });
        let queue = json!([
            {
                "movieId": 77,
                "status": "queued",
                "trackedDownloadState": "downloading",
                "trackedDownloadStatus": "ok",
                "errorMessage": "qBittorrent is downloading metadata",
                "downloadClient": "qBittorrent",
                "protocol": "torrent"
            }
        ]);

        let state = derive_arr_acquisition_state(
            "radarr",
            "77",
            &item,
            Some(&queue),
            false,
            None,
            &AcquisitionDownloaderProgressIndex::default(),
            None,
        );

        assert_eq!(state.phase, AcquisitionPhase::Downloading);
        assert!(state.blocker.is_none());
        assert_eq!(
            state.detail.as_deref(),
            Some("qBittorrent is downloading metadata")
        );
        assert!(
            state
                .evidence
                .iter()
                .any(|item| item.label == "Downloader accepted" && item.value == "Yes")
        );
    }

    #[test]
    fn derive_arr_acquisition_state_marks_download_client_unavailable_as_blocker() {
        let item = json!({
            "hasFile": false
        });
        let queue = json!([
            {
                "movieId": 77,
                "status": "downloadClientUnavailable",
                "downloadClient": "NZBGet",
                "protocol": "usenet"
            }
        ]);

        let state = derive_arr_acquisition_state(
            "radarr",
            "77",
            &item,
            Some(&queue),
            false,
            None,
            &AcquisitionDownloaderProgressIndex::default(),
            None,
        );
        assert_eq!(state.phase, AcquisitionPhase::NeedsAttention);
        assert_eq!(
            state.blocker.as_ref().map(|item| item.detail.as_str()),
            Some("Manager could not hand this release to a download client.")
        );
        assert!(
            state
                .evidence
                .iter()
                .any(|item| item.label == "Downloader accepted" && item.value == "No")
        );
    }

    #[test]
    fn derive_arr_acquisition_state_marks_accepted_by_manager_without_queue() {
        let item = json!({
            "statistics": {
                "episodeFileCount": 0,
                "sizeOnDisk": 0
            }
        });

        let state = derive_arr_acquisition_state(
            "sonarr",
            "9",
            &item,
            None,
            false,
            None,
            &AcquisitionDownloaderProgressIndex::default(),
            None,
        );
        assert_eq!(state.phase, AcquisitionPhase::AcceptedByManager);
        assert_eq!(state.headline, "Accepted by manager.");
        assert_eq!(
            state.detail.as_deref(),
            Some(
                "The manager accepted the item and is still looking for a valid release. No downloader has accepted it yet."
            )
        );
        assert!(
            state
                .evidence
                .iter()
                .any(|item| item.label == "Downloader accepted" && item.value == "No")
        );
    }

    #[test]
    fn derive_arr_acquisition_state_prefers_live_downloader_progress_over_manager_queue_math() {
        let item = json!({
            "hasFile": false,
            "sizeOnDisk": 0
        });
        let queue = json!({
            "records": [
                {
                    "movieId": 42,
                    "downloadId": "abc123",
                    "downloadClient": "NZBGet",
                    "protocol": "usenet",
                    "trackedDownloadState": "downloading",
                    "size": 1000,
                    "sizeleft": 810
                }
            ]
        });
        let mut downloader_progress = AcquisitionDownloaderProgressIndex::default();
        downloader_progress.insert(
            "abc123",
            AcquisitionDownloaderProgress {
                progress_percent: Some(77.0),
                eta_seconds: Some(321),
                issue: None,
            },
        );

        let state = derive_arr_acquisition_state(
            "radarr",
            "42",
            &item,
            Some(&queue),
            false,
            None,
            &downloader_progress,
            None,
        );

        assert_eq!(state.phase, AcquisitionPhase::Downloading);
        assert_eq!(state.progress_percent, Some(77.0));
        assert_eq!(state.eta_seconds, Some(321));
    }

    #[test]
    fn derive_arr_acquisition_state_marks_bad_nzbget_release_as_needing_attention() {
        let item = json!({
            "hasFile": false,
            "sizeOnDisk": 0
        });
        let queue = json!({
            "records": [
                {
                    "movieId": 42,
                    "downloadId": "deadbeef",
                    "downloadClient": "NZBGet",
                    "protocol": "usenet",
                    "trackedDownloadState": "downloading",
                    "size": 1000,
                    "sizeleft": 250
                }
            ]
        });
        let mut downloader_progress = AcquisitionDownloaderProgressIndex::default();
        downloader_progress.insert(
            "deadbeef",
            AcquisitionDownloaderProgress {
                progress_percent: Some(75.0),
                eta_seconds: None,
                issue: Some(AcquisitionDownloaderIssue {
                    code: "nzbget_release_unrecoverable".to_string(),
                    title: "Release is damaged".to_string(),
                    detail: "NZBGet reports this release is damaged or unrecoverable.".to_string(),
                }),
            },
        );

        let state = derive_arr_acquisition_state(
            "radarr",
            "42",
            &item,
            Some(&queue),
            false,
            None,
            &downloader_progress,
            None,
        );

        assert_eq!(state.phase, AcquisitionPhase::NeedsAttention);
        assert_eq!(state.progress_percent, None);
        assert_eq!(state.eta_seconds, None);
        assert_eq!(
            state.blocker.as_ref().map(|item| item.code.as_str()),
            Some("nzbget_release_unrecoverable")
        );
        assert!(state.actions.is_empty());
        assert_eq!(state.headline, "Dead release detected.");
    }

    #[test]
    fn derive_arr_acquisition_state_marks_dead_qbittorrent_release_as_needing_attention() {
        let item = json!({
            "hasFile": false,
            "sizeOnDisk": 0
        });
        let queue = json!({
            "records": [
                {
                    "movieId": 42,
                    "downloadId": "deadbeef",
                    "downloadClient": "qBittorrent",
                    "protocol": "torrent",
                    "trackedDownloadState": "downloading"
                }
            ]
        });
        let mut downloader_progress = AcquisitionDownloaderProgressIndex::default();
        downloader_progress.insert(
            "deadbeef",
            AcquisitionDownloaderProgress {
                progress_percent: Some(0.0),
                eta_seconds: None,
                issue: Some(AcquisitionDownloaderIssue {
                    code: "qbittorrent_release_metadata_stalled".to_string(),
                    title: "Torrent metadata never resolved".to_string(),
                    detail: "qBittorrent has not reached any peers for this torrent.".to_string(),
                }),
            },
        );

        let state = derive_arr_acquisition_state(
            "radarr",
            "42",
            &item,
            Some(&queue),
            false,
            None,
            &downloader_progress,
            None,
        );

        assert_eq!(state.phase, AcquisitionPhase::NeedsAttention);
        assert_eq!(
            state.blocker.as_ref().map(|item| item.code.as_str()),
            Some("qbittorrent_release_metadata_stalled")
        );
        assert_eq!(state.headline, "Dead release detected.");
        assert_eq!(
            state.detail.as_deref(),
            Some("qBittorrent has not reached any peers for this torrent.")
        );
    }

    #[test]
    fn derive_arr_acquisition_state_marks_qbittorrent_payload_issue_as_manual_recovery() {
        let item = json!({
            "hasFile": false,
            "sizeOnDisk": 0
        });
        let queue = json!({
            "records": [
                {
                    "movieId": 42,
                    "downloadId": "deadbeef",
                    "downloadClient": "qBittorrent",
                    "protocol": "torrent",
                    "trackedDownloadState": "downloading"
                }
            ]
        });
        let mut downloader_progress = AcquisitionDownloaderProgressIndex::default();
        downloader_progress.insert(
            "deadbeef",
            AcquisitionDownloaderProgress {
                progress_percent: Some(100.0),
                eta_seconds: None,
                issue: Some(AcquisitionDownloaderIssue {
                    code: "qbittorrent_release_failed_with_payload".to_string(),
                    title: "Torrent needs manual recovery".to_string(),
                    detail: "qBittorrent has local payload data.".to_string(),
                }),
            },
        );

        let state = derive_arr_acquisition_state(
            "radarr",
            "42",
            &item,
            Some(&queue),
            false,
            None,
            &downloader_progress,
            None,
        );

        assert_eq!(state.phase, AcquisitionPhase::NeedsAttention);
        assert_eq!(state.headline, "Torrent needs manual recovery.");
        assert_eq!(
            state.blocker.as_ref().map(|item| item.title.as_str()),
            Some("Torrent needs manual recovery")
        );
    }

    #[test]
    fn derive_arr_acquisition_state_marks_finding_another_release_after_successful_recovery() {
        let item = json!({
            "hasFile": false,
            "sizeOnDisk": 0
        });
        let recovery = IntentRecoveryView {
            last_attempted_download_id: Some("deadbeef".to_string()),
            last_attempted_at: Some(Utc::now()),
            cooldown_until: Some(Utc::now() + ChronoDuration::seconds(60)),
            last_attempt_succeeded: true,
        };

        let state = derive_arr_acquisition_state(
            "radarr",
            "42",
            &item,
            None,
            false,
            Some(&recovery),
            &AcquisitionDownloaderProgressIndex::default(),
            None,
        );

        assert_eq!(state.phase, AcquisitionPhase::FindingAnotherRelease);
        assert_eq!(state.progress_percent, None);
        assert_eq!(state.eta_seconds, None);
        assert_eq!(state.headline, "Finding another release.");
    }

    #[test]
    fn derive_arr_acquisition_state_groups_sonarr_batch_children() {
        let item = json!({
            "statistics": {
                "episodeFileCount": 0,
                "episodeCount": 14,
                "sizeOnDisk": 0
            }
        });
        let queue = json!({
            "records": [
                {
                    "seriesId": 6,
                    "episodeId": 38,
                    "title": "Firefly.S01E12.Release",
                    "downloadId": "child-a",
                    "downloadClient": "NZBGet",
                    "protocol": "usenet",
                    "trackedDownloadState": "downloading",
                    "status": "downloading"
                },
                {
                    "seriesId": 6,
                    "episodeId": 39,
                    "title": "Firefly.S01E13.Release",
                    "downloadId": "child-b",
                    "downloadClient": "NZBGet",
                    "protocol": "usenet",
                    "trackedDownloadState": "queued",
                    "status": "queued"
                }
            ]
        });
        let episode_index = HashMap::from([
            (
                38,
                SonarrEpisodeDescriptor {
                    season_number: 1,
                    episode_number: 12,
                    title: "The Message".to_string(),
                },
            ),
            (
                39,
                SonarrEpisodeDescriptor {
                    season_number: 1,
                    episode_number: 13,
                    title: "Heart of Gold".to_string(),
                },
            ),
        ]);
        let mut downloader_progress = AcquisitionDownloaderProgressIndex::default();
        downloader_progress.insert(
            "child-a",
            AcquisitionDownloaderProgress {
                progress_percent: Some(52.0),
                eta_seconds: Some(600),
                issue: None,
            },
        );

        let state = derive_arr_acquisition_state(
            "sonarr",
            "6",
            &item,
            Some(&queue),
            false,
            None,
            &downloader_progress,
            Some(&episode_index),
        );

        assert_eq!(state.phase, AcquisitionPhase::Downloading);
        assert_eq!(state.progress_percent, None);
        assert_eq!(state.children.len(), 2);
        assert_eq!(state.children[0].title, "S01E12 • The Message");
        assert_eq!(
            state.children[0].subtitle.as_deref(),
            Some("Firefly.S01E12.Release")
        );
        assert_eq!(state.children[0].progress_percent, Some(52.0));
        assert_eq!(state.children[1].phase, "queued_in_downloader");
        assert!(
            state
                .evidence
                .iter()
                .any(|item| item.label == "Episodes downloading" && item.value == "1")
        );
        assert!(
            state
                .evidence
                .iter()
                .any(|item| item.label == "Episodes queued" && item.value == "1")
        );
    }

    #[test]
    fn derive_arr_acquisition_state_keeps_sonarr_batch_queue_visible_when_some_files_exist() {
        let item = json!({
            "statistics": {
                "episodeFileCount": 3,
                "episodeCount": 14,
                "sizeOnDisk": 12345
            }
        });
        let queue = json!({
            "records": [
                {
                    "seriesId": 6,
                    "episodeId": 38,
                    "title": "Firefly.S01E12.Release",
                    "downloadId": "child-a",
                    "downloadClient": "NZBGet",
                    "protocol": "usenet",
                    "trackedDownloadState": "queued",
                    "status": "queued"
                }
            ]
        });
        let episode_index = HashMap::from([(
            38,
            SonarrEpisodeDescriptor {
                season_number: 1,
                episode_number: 12,
                title: "The Message".to_string(),
            },
        )]);

        let state = derive_arr_acquisition_state(
            "sonarr",
            "6",
            &item,
            Some(&queue),
            true,
            None,
            &AcquisitionDownloaderProgressIndex::default(),
            Some(&episode_index),
        );

        assert_eq!(state.phase, AcquisitionPhase::QueuedInDownloader);
        assert_eq!(state.children.len(), 1);
        assert_eq!(
            state.detail.as_deref(),
            Some(
                "Sonarr is handling this series as a batch. 3 of 14 episode files are imported so far."
            )
        );
    }

    #[test]
    fn derive_arr_acquisition_state_does_not_mark_sonarr_complete_after_first_import() {
        let item = json!({
            "statistics": {
                "episodeFileCount": 1,
                "episodeCount": 14,
                "sizeOnDisk": 4501507231u64
            }
        });
        let queue = json!({
            "records": [
                {
                    "seriesId": 6,
                    "episodeId": 40,
                    "title": "Firefly.S01E14.Release",
                    "downloadId": "child-a",
                    "downloadClient": "NZBGet",
                    "protocol": "usenet",
                    "trackedDownloadState": "downloading",
                    "status": "downloading"
                }
            ]
        });
        let episode_index = HashMap::from([(
            40,
            SonarrEpisodeDescriptor {
                season_number: 1,
                episode_number: 14,
                title: "The Message".to_string(),
            },
        )]);
        let mut downloader_progress = AcquisitionDownloaderProgressIndex::default();
        downloader_progress.insert(
            "child-a",
            AcquisitionDownloaderProgress {
                progress_percent: Some(25.0),
                eta_seconds: Some(1200),
                issue: None,
            },
        );

        let state = derive_arr_acquisition_state(
            "sonarr",
            "6",
            &item,
            Some(&queue),
            true,
            None,
            &downloader_progress,
            Some(&episode_index),
        );

        assert_eq!(state.phase, AcquisitionPhase::Downloading);
        assert_eq!(state.children.len(), 1);
        assert_eq!(
            state.detail.as_deref(),
            Some(
                "Sonarr is handling this series as a batch. 1 of 14 episode files are imported so far."
            )
        );
    }

    #[test]
    fn derive_arr_acquisition_state_marks_sonarr_complete_only_when_series_is_fully_downloaded() {
        let item = json!({
            "statistics": {
                "episodeFileCount": 14,
                "episodeCount": 14,
                "sizeOnDisk": 63021101234u64
            }
        });

        let state = derive_arr_acquisition_state(
            "sonarr",
            "6",
            &item,
            None,
            true,
            None,
            &AcquisitionDownloaderProgressIndex::default(),
            None,
        );

        assert_eq!(state.phase, AcquisitionPhase::Completed);
        assert_eq!(state.phase.label(), "Downloaded");
        assert_eq!(state.headline, "Downloaded.");
        assert!(state.evidence.is_empty());
        assert!(state.detail.is_none());
    }

    #[test]
    fn nzbget_acquisition_progress_uses_drone_download_id_and_actual_downloaded_bytes() {
        let group = AcquisitionNzbgetGroup {
            nzb_id: 6,
            status: Some("DOWNLOADING".to_string()),
            file_size_lo: Some(1000),
            file_size_hi: Some(0),
            remaining_size_lo: Some(250),
            remaining_size_hi: Some(0),
            downloaded_size_lo: Some(10),
            downloaded_size_hi: Some(0),
            failed_articles: Some(0),
            health: Some(1000),
            critical_health: Some(900),
            parameters: vec![AcquisitionNzbgetGroupParameter {
                name: "drone".to_string(),
                value: "838cfa292491470a93b2c777b1a6d0b1".to_string(),
            }],
        };

        let (download_id, progress) = nzbget_acquisition_progress(&group).expect("nzbget progress");

        assert_eq!(download_id, "838cfa292491470a93b2c777b1a6d0b1");
        assert_eq!(progress.progress_percent, Some(1.0));
        assert_eq!(progress.eta_seconds, None);
        assert!(progress.issue.is_none());
    }

    #[test]
    fn nzbget_group_issue_marks_unrecoverable_release() {
        let group = AcquisitionNzbgetGroup {
            nzb_id: 6,
            status: Some("DOWNLOADING".to_string()),
            file_size_lo: None,
            file_size_hi: None,
            remaining_size_lo: None,
            remaining_size_hi: None,
            downloaded_size_lo: None,
            downloaded_size_hi: None,
            failed_articles: Some(2416),
            health: Some(948),
            critical_health: Some(948),
            parameters: vec![],
        };

        let issue = nzbget_group_issue(&group).expect("issue");
        assert_eq!(issue.code, "nzbget_release_unrecoverable");
    }
}
