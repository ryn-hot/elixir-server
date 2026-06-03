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
use chrono::{DateTime, Duration as ChronoDuration, NaiveDate, Utc};
use reqwest::header::{ACCEPT, HeaderMap, HeaderValue, USER_AGENT};
use reqwest::{Client, Method as ReqwestMethod, StatusCode as ReqwestStatusCode, Url};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map as JsonMap, Value, json};
use sqlx::Row;
use tokio::net::lookup_host;
use tokio::process::Command;
use tracing::{debug, info, warn};
use uuid::Uuid;

use super::{
    acquisition_sources::ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY,
    download_broker::generic_debrid_error_message,
};
use crate::{
    acquisition::{
        AUTO_RECOVERY_COOLDOWN_SECONDS, IntentRecoveryView,
        imports::{AcquisitionImportRunState, list_import_runs_by_release},
        load_intent_recovery_views,
        release_resolution::anime::{
            AnimeMetadataGraphInput, AnimeSeasonMapping, build_anime_metadata_graph,
            infer_anizip_season_number,
        },
        release_resolution::{
            models::{AcquisitionReleaseState, ReleaseCoverageState, ReleaseJobState},
            store::{ReleaseListFilter, list_release_coverage, list_release_jobs, list_releases},
        },
        scoped_add::{
            AcquisitionRequestOrigin, FindMediaScopePreviewBlocker,
            FindMediaScopePreviewCapabilities, FindMediaScopePreviewEpisode,
            FindMediaScopePreviewRequest, FindMediaScopePreviewResponse,
            FindMediaScopePreviewSeason, FindMediaScopedAddRequest, FindMediaScopedAddResponse,
            ScopedAddMediaIdentity, ScopedAddScopeDocument, ScopedAddSelection,
            ScopedAddSelectionType, canonical_target_keys,
        },
        subscriptions::{
            AcquisitionCompletionPolicy, AcquisitionIntentTarget, AcquisitionMetadataPolicy,
            AcquisitionRequestMode, AcquisitionRequestScope, AcquisitionRoutePolicy,
            AcquisitionSubscription, AcquisitionSubscriptionFilter, AcquisitionSubscriptionStatus,
            AcquisitionTarget, AcquisitionTargetState, AcquisitionTargetStateUpdate,
            CreateAcquisitionIntent, NewAcquisitionTarget, create_or_update_acquisition_intent,
            list_subscription_targets, list_subscriptions, update_target_state,
        },
    },
    db::models::{ExtensionTrustLevel, MediaType, ProviderHealthState, SecretScope},
    debrid::{
        DEBRID_EXTENSION_ID, LEGACY_REAL_DEBRID_EXTENSION_ID, active_debrid_service_from_config,
        debrid_secret_exists_for_instance, is_debrid_service_implementation, load_debrid_progress,
    },
    download_broker::{
        DEBRID_ACCOUNT_MISSING_MESSAGE, DEBRID_DEFAULT_LOGICAL_ID,
        DEBRID_SERVICE_NOT_CONFIGURED_MESSAGE, DEBRID_SERVICE_UNAVAILABLE_MESSAGE,
        TORRENT_DEFAULT_LOGICAL_ID,
    },
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
    library::{
        AcquisitionLibraryTargetScaffold, AniListSeasonChainEntry, ingest_managed_import_event,
        managed_episode_tombstone_matches_series, resolve_anilist_season_chain,
        scaffold_acquisition_library_targets,
    },
    metadata::DiscoveryResult,
    orchestrator::model::ProviderEndpoint,
    runtime::RuntimePaths,
    state::AppState,
};

const MANAGER_PREF_MOVIE: &str = "manager_preference.movie";
const MANAGER_PREF_SERIES: &str = "manager_preference.series";
const MANAGER_PREF_ANIME: &str = "manager_preference.anime";
const SOURCE_PREF_MOVIE: &str = "acquisition_source_preference.movie";
const SOURCE_PREF_SERIES: &str = "acquisition_source_preference.series";
const SOURCE_PREF_ANIME: &str = "acquisition_source_preference.anime";
const CONTROL_DEFAULTS_SETTING_PREFIX: &str = "extensions.control_defaults.instance.";
const NZBGET_DRONE_DOWNLOAD_ID_PARAM: &str = "drone";
const TORRENT_METADATA_STALL_TIMEOUT_SECONDS: i64 = 10 * 60;
const TORRENT_ZERO_PROGRESS_TIMEOUT_SECONDS: i64 = 15 * 60;
const SOURCE_ACQUISITION_INTENT_SOURCE: &str = "acquisition_subscription";
const SOURCE_ACQUISITION_FILE_SELECTION_REASON: &str =
    "Downloaded pack needs file selection before import.";
const SOURCE_ACQUISITION_WAITING_FOR_FILE_REASON: &str =
    "Downloaded file is waiting for library visibility.";
const ADD_DEBRID_ACCOUNT_ACTION_ID: &str = "add_debrid_account";
const OPEN_REVIEW_ACTION_ID: &str = "open_review";
const OPEN_SHOW_ACTION_ID: &str = "open_show";
const RETRY_MISSING_ACTION_ID: &str = "retry_missing";
const FIND_ANOTHER_RELEASE_ACTION_ID: &str = "find_another_release";
const RETRY_IMPORT_ACTION_ID: &str = "retry_import";
const REMOVE_ACQUISITION_REQUEST_ACTION_ID: &str = "remove_acquisition_request";
const CANCEL_ACQUISITION_DOWNLOADS_ACTION_ID: &str = "cancel_acquisition_downloads";

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
pub struct SourcePreferenceState {
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
    pub tv_default_source_provider_id: Option<Uuid>,
    pub movies_default_source_provider_id: Option<Uuid>,
    pub anime_default_source_provider_id: Option<Uuid>,
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
    pub source_providers: Vec<ProviderSummary>,
    pub preferred_manager_provider_id: Option<Uuid>,
    pub default_manager_provider_id: Option<Uuid>,
    pub preferred_source_provider_id: Option<Uuid>,
    pub default_source_provider_id: Option<Uuid>,
    pub results: Vec<FindMediaResult>,
    pub provider_errors: Vec<ProviderSearchError>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FindMediaTargetsResponse {
    pub media_type: String,
    pub search_providers: Vec<ProviderSummary>,
    pub manager_candidates: Vec<ProviderSummary>,
    pub source_candidates: Vec<ProviderSummary>,
    pub default_manager_provider_id: Option<Uuid>,
    pub preferred_manager_provider_id: Option<Uuid>,
    pub default_source_provider_id: Option<Uuid>,
    pub preferred_source_provider_id: Option<Uuid>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_item_id: Option<String>,
    pub source: String,
    pub request_mode: String,
    pub request_scope: String,
    pub request_label: String,
    pub one_shot: bool,
    pub phase: String,
    pub phase_label: String,
    pub headline: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    pub target_count: usize,
    pub displayed_child_count: usize,
    pub hidden_child_count: usize,
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
    pub release_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_provider_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_provider_label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub route_provider_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub route_provider_label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub route_logical_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subtitle: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub download_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub downloaded_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remaining_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub download_rate_bps: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upload_rate_bps: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connected_seeds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connected_peers: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub known_seeds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub known_peers: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub availability: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seen_complete_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AcquisitionPhase {
    Requested,
    AcceptedByManager,
    FindingAnotherRelease,
    Staged,
    Submitted,
    QueuedInDownloader,
    Downloading,
    Materializing,
    PostProcessing,
    Importing,
    Completed,
    ReviewRequired,
    Quarantined,
    NeedsAttention,
    Failed,
}

impl AcquisitionPhase {
    fn as_str(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::AcceptedByManager => "accepted_by_manager",
            Self::FindingAnotherRelease => "finding_another_release",
            Self::Staged => "staged",
            Self::Submitted => "submitted",
            Self::QueuedInDownloader => "queued_in_downloader",
            Self::Downloading => "downloading",
            Self::Materializing => "materializing",
            Self::PostProcessing => "post_processing",
            Self::Importing => "importing",
            Self::Completed => "completed",
            Self::ReviewRequired => "review_required",
            Self::Quarantined => "quarantined",
            Self::NeedsAttention => "needs_attention",
            Self::Failed => "failed",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Requested => "Requested",
            Self::AcceptedByManager => "Accepted by manager",
            Self::FindingAnotherRelease => "Finding another release",
            Self::Staged => "Staged",
            Self::Submitted => "Submitted",
            Self::QueuedInDownloader => "Queued in downloader",
            Self::Downloading => "Downloading",
            Self::Materializing => "Materializing",
            Self::PostProcessing => "Post-processing",
            Self::Importing => "Importing",
            Self::Completed => "Downloaded",
            Self::ReviewRequired => "Review required",
            Self::Quarantined => "Quarantined",
            Self::NeedsAttention => "Needs attention",
            Self::Failed => "Failed",
        }
    }

    fn sort_priority(self) -> i32 {
        match self {
            Self::Downloading => 0,
            Self::Materializing => 1,
            Self::PostProcessing => 2,
            Self::Importing => 3,
            Self::NeedsAttention => 4,
            Self::Failed => 5,
            Self::ReviewRequired => 6,
            Self::Quarantined => 7,
            Self::FindingAnotherRelease => 8,
            Self::QueuedInDownloader => 9,
            Self::Submitted => 10,
            Self::Staged => 11,
            Self::AcceptedByManager => 12,
            Self::Requested => 13,
            Self::Completed => 14,
        }
    }

    fn is_active(self) -> bool {
        !matches!(self, Self::Completed | Self::Failed)
    }

    fn counts_as_downloading(self) -> bool {
        matches!(self, Self::Downloading | Self::Materializing)
    }

    fn is_route_work(self) -> bool {
        matches!(
            self,
            Self::Staged
                | Self::Submitted
                | Self::QueuedInDownloader
                | Self::Downloading
                | Self::Materializing
                | Self::PostProcessing
                | Self::Importing
                | Self::Completed
        )
    }

    fn legacy_stage(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::AcceptedByManager => "searching",
            Self::FindingAnotherRelease => "searching",
            Self::Staged => "queued",
            Self::Submitted => "queued",
            Self::QueuedInDownloader => "queued",
            Self::Downloading => "downloading",
            Self::Materializing => "post_processing",
            Self::PostProcessing => "post_processing",
            Self::Importing => "importing",
            Self::Completed => "ready",
            Self::ReviewRequired => "needs_attention",
            Self::Quarantined => "needs_attention",
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub navigate_extension_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub navigate_view: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub navigate_media_item_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub release_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscription_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cancel_mode: Option<String>,
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

#[derive(Debug, Clone, Default)]
struct AcquisitionDownloaderProgress {
    release_title: Option<String>,
    status: Option<String>,
    category: Option<String>,
    local_path: Option<String>,
    progress_percent: Option<f64>,
    eta_seconds: Option<i64>,
    size_bytes: Option<u64>,
    downloaded_bytes: Option<u64>,
    remaining_bytes: Option<u64>,
    download_rate_bps: Option<u64>,
    upload_rate_bps: Option<u64>,
    connected_seeds: Option<u64>,
    connected_peers: Option<u64>,
    known_seeds: Option<u64>,
    known_peers: Option<u64>,
    availability: Option<f64>,
    seen_complete_at: Option<DateTime<Utc>>,
    issue: Option<AcquisitionDownloaderIssue>,
}

#[derive(Debug, Clone)]
struct AcquisitionDownloaderIssue {
    code: String,
    title: String,
    detail: String,
}

#[derive(Debug)]
enum SourceImportSelection {
    Ready(ManagedImportFile),
    Pending,
    NeedsFileSelection(String),
}

#[derive(Debug, Clone, Copy, Default)]
struct SourceAcquisitionCounts {
    requested: usize,
    staged: usize,
    submitted: usize,
    queued: usize,
    downloading: usize,
    materializing: usize,
    post_processing: usize,
    importing: usize,
    completed: usize,
    no_results: usize,
    review_required: usize,
    quarantined: usize,
    needs_attention: usize,
    failed: usize,
}

#[derive(Debug, Clone, Default)]
struct AcquisitionUxContext {
    debrid_account_missing: bool,
}

#[derive(Debug, Clone)]
struct SourceTargetReleaseRuntime {
    release_id: Uuid,
    source_provider_id: Option<Uuid>,
    route_provider_id: Option<Uuid>,
    release_title: String,
    release_state: AcquisitionReleaseState,
    release_state_reason: Option<String>,
    coverage_state: Option<ReleaseCoverageState>,
    coverage_reason: Option<String>,
    selected_route_logical_id: Option<String>,
    download_id: Option<String>,
    job_state: Option<ReleaseJobState>,
    job_state_reason: Option<String>,
    import_state: Option<AcquisitionImportRunState>,
    import_state_reason: Option<String>,
    import_mismatch_class: Option<String>,
    updated_at: DateTime<Utc>,
}

impl SourceTargetReleaseRuntime {
    fn status_text(&self) -> Option<String> {
        if let Some(state) = self.import_state {
            return Some(state.as_str().to_string());
        }
        if let Some(state) = self.job_state {
            return Some(state.as_str().to_string());
        }
        Some(self.release_state.as_str().to_string())
    }

    fn state_reason(&self) -> Option<String> {
        self.import_state_reason
            .clone()
            .or_else(|| self.import_mismatch_class.clone())
            .or_else(|| self.job_state_reason.clone())
            .or_else(|| self.coverage_reason.clone())
            .or_else(|| self.release_state_reason.clone())
    }
}

#[derive(Debug, Deserialize)]
struct AcquisitionQbittorrentTorrent {
    #[serde(default)]
    hash: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    category: Option<String>,
    #[serde(default)]
    content_path: Option<String>,
    #[serde(default)]
    save_path: Option<String>,
    #[serde(default)]
    progress: Option<f64>,
    #[serde(default)]
    downloaded: Option<u64>,
    #[serde(default)]
    dlspeed: Option<u64>,
    #[serde(default)]
    upspeed: Option<u64>,
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
    availability: Option<f64>,
    #[serde(default)]
    seen_complete: Option<i64>,
    #[serde(default)]
    added_on: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct AcquisitionNzbgetGroup {
    #[serde(rename = "NZBID", default)]
    nzb_id: i64,
    #[serde(rename = "NZBName", default)]
    nzb_name: Option<String>,
    #[serde(rename = "NZBFilename", default)]
    nzb_filename: Option<String>,
    #[serde(rename = "Category", default)]
    category: Option<String>,
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
    pub tv_source_candidates: Vec<ProviderSummary>,
    pub movies_source_candidates: Vec<ProviderSummary>,
    pub anime_source_candidates: Vec<ProviderSummary>,
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
    #[serde(
        default,
        alias = "tv_default_source_provider_id",
        deserialize_with = "deserialize_optional_json_value"
    )]
    pub tv_default_source_provider_id: Option<Value>,
    #[serde(
        default,
        alias = "movies_default_source_provider_id",
        deserialize_with = "deserialize_optional_json_value"
    )]
    pub movies_default_source_provider_id: Option<Value>,
    #[serde(
        default,
        alias = "anime_default_source_provider_id",
        deserialize_with = "deserialize_optional_json_value"
    )]
    pub anime_default_source_provider_id: Option<Value>,
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
    let source_preferences = load_source_preferences(&store, &providers)
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
    let (source_contexts, _source_errors) = filter_search_providers(
        &state,
        &store,
        collect_source_providers(&providers, media_type),
    )
    .await
    .map_err(ApiError::from)?;

    let preferred = preferred_manager_for_type(&preferences, media_type);
    let blueprint_preferred =
        resolve_blueprint_preferred_manager(&store, &manager_contexts, media_type)
            .await
            .map_err(ApiError::from)?;
    let default = resolve_default_manager(preferred, blueprint_preferred, &manager_contexts);
    let preferred_source = preferred_source_for_type(&source_preferences, media_type);
    let default_source = resolve_default_source(preferred_source, &source_contexts);

    let manager_candidates: Vec<ProviderSummary> =
        manager_contexts.iter().map(provider_summary).collect();
    let search_providers: Vec<ProviderSummary> =
        search_contexts.iter().map(provider_summary).collect();
    let source_candidates: Vec<ProviderSummary> =
        source_contexts.iter().map(provider_summary).collect();

    Ok(Json(FindMediaTargetsResponse {
        media_type: media_type_api_name(media_type).to_string(),
        search_providers,
        manager_candidates,
        source_candidates,
        default_manager_provider_id: default,
        preferred_manager_provider_id: preferred,
        default_source_provider_id: default_source,
        preferred_source_provider_id: preferred_source,
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

pub async fn find_media_scope_preview(
    State(state): State<AppState>,
    _user: CurrentUser,
    Json(payload): Json<FindMediaScopePreviewRequest>,
) -> ApiResult<Json<FindMediaScopePreviewResponse>> {
    let store = ExtensionStore::new(&state.db_pool);
    let response = build_find_media_scope_preview(&state, &store, payload)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(response))
}

pub async fn find_media_scoped_add(
    State(state): State<AppState>,
    _user: CurrentUser,
    Json(payload): Json<FindMediaScopedAddRequest>,
) -> ApiResult<Json<FindMediaScopedAddResponse>> {
    if payload.result.kind != payload.media_type {
        return Err(ApiError::bad_request(
            "scoped add media_type must match result.kind",
        ));
    }
    let scope_document = payload
        .scope_document()
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let store = ExtensionStore::new(&state.db_pool);
    let preview = build_find_media_scope_preview(
        &state,
        &store,
        FindMediaScopePreviewRequest {
            provider_id: Some(payload.provider_id),
            media_type: payload.media_type,
            result: payload.result.clone(),
        },
    )
    .await
    .map_err(ApiError::from)?;

    if !preview.blockers.is_empty() {
        let first = &preview.blockers[0];
        return Err(ApiError::bad_request(format!(
            "{}{}",
            first.message,
            first
                .detail
                .as_deref()
                .map(|detail| format!(" {detail}"))
                .unwrap_or_default()
        )));
    }

    let selected_targets =
        select_scoped_add_targets_from_preview(&preview, &scope_document.selection)
            .map_err(|err| ApiError::bad_request(err.to_string()))?;
    if selected_targets.is_empty() {
        return Err(ApiError::bad_request(
            "scoped add selection did not match any preview targets",
        ));
    }

    let catalog = ensure_find_media_scoped_catalog(&state, &preview, &selected_targets)
        .await
        .map_err(ApiError::from)?;
    let now = Utc::now();
    let mut scope_json = scoped_add_scope_json(
        &scope_document,
        catalog.media_item_id,
        &selected_targets,
        &catalog.episode_ids_by_target_key,
    )
    .map_err(ApiError::from)?;
    let target =
        scoped_add_intent_target(&scope_document.selection, &selected_targets, &scope_json);
    let mut intent = CreateAcquisitionIntent {
        media_type: payload.media_type,
        title: preview.media.title.clone(),
        year: preview.media.year,
        external_ids: preview.media.external_ids.clone(),
        idempotency_key: None,
        request_mode: Some(AcquisitionRequestMode::OneShot),
        request_scope: Some(scope_document.selection.request_scope()),
        scope: Some(scope_json.clone()),
        metadata_policy: Some(AcquisitionMetadataPolicy::InitialOnly),
        completion_policy: Some(AcquisitionCompletionPolicy::TerminalSelectedTargets),
        monitor_policy: Some(
            crate::acquisition::subscriptions::AcquisitionMonitorPolicy::SelectedTargets,
        ),
        route_policy: payload.route_policy.or(scope_document.route_policy),
        source_provider_id: Some(payload.provider_id),
        release_delay_seconds: None,
        quality_profile: None,
        metadata_refresh_after: Some(now),
        candidate_search_after: Some(now),
        target: Some(target),
        targets: selected_targets
            .iter()
            .map(|target| {
                scoped_add_new_acquisition_target(
                    payload.media_type,
                    &preview.media.title,
                    &preview.media.aliases,
                    target,
                    catalog
                        .episode_ids_by_target_key
                        .get(&target.target_key)
                        .copied(),
                    catalog.media_item_id,
                    now,
                )
            })
            .collect(),
    };
    apply_find_media_source_provider_config_defaults(&store, &mut intent)
        .await
        .map_err(ApiError::from)?;
    let route_policy = intent.route_policy.unwrap_or_default();
    intent.route_policy = Some(route_policy);
    if let Some(scope) = intent.scope.as_mut() {
        add_scoped_add_effective_route_policy(scope, intent.route_policy);
    }
    scope_json = intent.scope.clone().unwrap_or(scope_json);
    intent.idempotency_key = Some(scoped_add_idempotency_key(
        payload.provider_id,
        payload.media_type,
        route_policy,
        &scope_json,
    ));

    let result = create_or_update_acquisition_intent(&state.db_pool, intent, now)
        .await
        .map_err(ApiError::from)?;

    info!(
        media_type = media_type_api_name(payload.media_type),
        title = %preview.media.title,
        provider_id = %payload.provider_id,
        subscription_id = %result.detail.subscription.subscription_id,
        target_count = result.detail.targets.len(),
        created = result.created,
        scope = ?scope_document.selection.selection_type,
        scope_hash = %stable_hash_hex(&serde_json::to_string(&scope_json).unwrap_or_default()),
        "find media scoped add accepted"
    );

    Ok(Json(FindMediaScopedAddResponse {
        accepted: true,
        subscription_id: result.detail.subscription.subscription_id,
        request_mode: AcquisitionRequestMode::OneShot,
        request_origin: AcquisitionRequestOrigin::FindMedia,
        request_scope: scope_document.selection.request_scope(),
        target_count: result.detail.targets.len(),
        status: "queued".to_string(),
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

#[derive(Debug, Clone)]
struct FindMediaScopedPreviewTarget {
    target_key: String,
    season_number: Option<i32>,
    episode_number: Option<i32>,
    absolute_episode_number: Option<i32>,
    title: Option<String>,
    air_date: Option<String>,
    thumbnail_url: Option<String>,
}

#[derive(Debug, Clone)]
struct FindMediaScopedCatalog {
    media_item_id: Uuid,
    episode_ids_by_target_key: HashMap<String, Uuid>,
}

fn select_scoped_add_targets_from_preview(
    preview: &FindMediaScopePreviewResponse,
    selection: &ScopedAddSelection,
) -> AnyResult<Vec<FindMediaScopedPreviewTarget>> {
    let selection = selection.validated()?;
    if preview.media.kind == MediaType::Movie {
        if matches!(
            selection.selection_type,
            ScopedAddSelectionType::Movie | ScopedAddSelectionType::EntireTitle
        ) {
            return Ok(vec![FindMediaScopedPreviewTarget {
                target_key: "MOVIE".to_string(),
                season_number: None,
                episode_number: None,
                absolute_episode_number: None,
                title: Some(preview.media.title.clone()),
                air_date: None,
                thumbnail_url: None,
            }]);
        }
        bail!("movie scoped add only supports movie or entire-title selection");
    }

    let preview_targets = flattened_scope_preview_targets(preview)?;
    let order: HashMap<_, _> = preview_targets
        .iter()
        .enumerate()
        .map(|(index, target)| (target.target_key.clone(), index))
        .collect();
    let by_key: HashMap<_, _> = preview_targets
        .iter()
        .cloned()
        .map(|target| (target.target_key.clone(), target))
        .collect();
    let mut selected = Vec::new();

    match selection.selection_type {
        ScopedAddSelectionType::Movie => {
            bail!("movie selection is only valid for movie results");
        }
        ScopedAddSelectionType::EntireTitle => {
            selected.extend(preview_targets);
        }
        ScopedAddSelectionType::Episode => {
            if !selection.target_keys.is_empty() {
                selected.extend(targets_for_keys(&by_key, &selection.target_keys)?);
            } else if let (Some(season), Some(episode)) =
                (selection.season_number, selection.episode_number)
            {
                selected.extend(preview_targets.into_iter().filter(|target| {
                    target.season_number == Some(season) && target.episode_number == Some(episode)
                }));
            } else if let Some(absolute) = selection.absolute_episode_number {
                selected.extend(
                    preview_targets
                        .into_iter()
                        .filter(|target| target.absolute_episode_number == Some(absolute)),
                );
            }
        }
        ScopedAddSelectionType::Season => {
            let season = selection
                .season_number
                .ok_or_else(|| anyhow::anyhow!("season scoped add requires seasonNumber"))?;
            selected.extend(
                preview_targets
                    .into_iter()
                    .filter(|target| target.season_number == Some(season)),
            );
        }
        ScopedAddSelectionType::Range => {
            if !selection.target_keys.is_empty() {
                selected.extend(targets_for_keys(&by_key, &selection.target_keys)?);
            } else if let Some(season) = selection.season_number {
                let start = selection
                    .episode_start
                    .or(selection.episode_number)
                    .ok_or_else(|| anyhow::anyhow!("range scoped add requires episodeStart"))?;
                let end = selection.episode_end.unwrap_or(start);
                selected.extend(preview_targets.into_iter().filter(|target| {
                    target.season_number == Some(season)
                        && target.episode_number.is_some_and(|episode| {
                            episode >= start.min(end) && episode <= start.max(end)
                        })
                }));
            } else {
                let start = selection
                    .absolute_episode_start
                    .or(selection.absolute_episode_number)
                    .ok_or_else(|| {
                        anyhow::anyhow!("range scoped add requires absoluteEpisodeStart")
                    })?;
                let end = selection.absolute_episode_end.unwrap_or(start);
                selected.extend(preview_targets.into_iter().filter(|target| {
                    target.absolute_episode_number.is_some_and(|absolute| {
                        absolute >= start.min(end) && absolute <= start.max(end)
                    })
                }));
            }
        }
        ScopedAddSelectionType::SelectedTargets | ScopedAddSelectionType::AnimeArc => {
            selected.extend(targets_for_keys(&by_key, &selection.target_keys)?);
        }
    }

    let mut seen = HashSet::new();
    selected.retain(|target| seen.insert(target.target_key.clone()));
    selected.sort_by_key(|target| order.get(&target.target_key).copied().unwrap_or(usize::MAX));
    Ok(selected)
}

fn flattened_scope_preview_targets(
    preview: &FindMediaScopePreviewResponse,
) -> AnyResult<Vec<FindMediaScopedPreviewTarget>> {
    let mut targets = Vec::new();
    for season in &preview.seasons {
        for episode in &season.episodes {
            targets.push(FindMediaScopedPreviewTarget {
                target_key: crate::acquisition::scoped_add::canonical_acquisition_target_key(
                    &episode.target_key,
                )?,
                season_number: episode.season_number.or(Some(season.season_number)),
                episode_number: episode.episode_number,
                absolute_episode_number: episode.absolute_episode_number,
                title: episode.title.clone(),
                air_date: episode.air_date.clone(),
                thumbnail_url: episode.thumbnail_url.clone(),
            });
        }
    }
    targets.sort_by_key(|target| {
        (
            target.season_number.unwrap_or(i32::MAX),
            target.episode_number.unwrap_or(i32::MAX),
            target.absolute_episode_number.unwrap_or(i32::MAX),
            target.target_key.clone(),
        )
    });
    Ok(targets)
}

fn targets_for_keys(
    by_key: &HashMap<String, FindMediaScopedPreviewTarget>,
    keys: &[String],
) -> AnyResult<Vec<FindMediaScopedPreviewTarget>> {
    let keys = canonical_target_keys(keys)?;
    let mut targets = Vec::new();
    for key in keys {
        let Some(target) = by_key.get(&key) else {
            bail!("selected targetKey {key} is not present in the canonical preview");
        };
        targets.push(target.clone());
    }
    Ok(targets)
}

async fn ensure_find_media_scoped_catalog(
    state: &AppState,
    preview: &FindMediaScopePreviewResponse,
    targets: &[FindMediaScopedPreviewTarget],
) -> AnyResult<FindMediaScopedCatalog> {
    let media_item_id = match preview.media.kind {
        MediaType::Movie => ensure_find_media_scoped_movie(&state.db_pool, &preview.media).await?,
        MediaType::Series | MediaType::Anime => {
            let media_item_id =
                ensure_find_media_scoped_series(&state.db_pool, &preview.media).await?;
            let scaffolds = targets
                .iter()
                .filter_map(|target| {
                    let season = target.season_number?;
                    let episode = target.episode_number?;
                    Some(AcquisitionLibraryTargetScaffold {
                        media_type: preview.media.kind,
                        title: target
                            .title
                            .clone()
                            .unwrap_or_else(|| format!("Episode {episode}")),
                        season_number: Some(season),
                        episode_number: Some(episode),
                        absolute_episode_number: target.absolute_episode_number,
                        metadata: Some(scoped_add_episode_metadata(target)),
                    })
                })
                .collect::<Vec<_>>();
            scaffold_acquisition_library_targets(
                &state.db_pool,
                Some(state.artwork.as_ref()),
                media_item_id,
                &scaffolds,
            )
            .await?;
            media_item_id
        }
    };
    let episode_ids_by_target_key =
        load_scoped_catalog_episode_ids(&state.db_pool, media_item_id, targets).await?;
    Ok(FindMediaScopedCatalog {
        media_item_id,
        episode_ids_by_target_key,
    })
}

async fn ensure_find_media_scoped_movie(
    pool: &sqlx::AnyPool,
    media: &ScopedAddMediaIdentity,
) -> AnyResult<Uuid> {
    let ids = media.external_ids.clone().unwrap_or_default();
    let media_item_id = find_existing_movie_id(pool, &ids, &media.title, media.year)
        .await?
        .unwrap_or_else(Uuid::new_v4);
    let external_ids_json = serde_json::to_string(&ids)?;
    let metadata_json = serde_json::to_string(&scoped_add_media_metadata(media))?;

    upsert_media_item_preserving_metadata(
        pool,
        media_item_id,
        MediaType::Movie,
        &media.title,
        media.year,
        &external_ids_json,
        &metadata_json,
    )
    .await?;

    if sqlx::query_scalar::<sqlx::Any, String>("SELECT id FROM movies WHERE id = ? LIMIT 1")
        .bind(media_item_id.to_string())
        .fetch_optional(pool)
        .await?
        .is_some()
    {
        sqlx::query::<sqlx::Any>(
            "UPDATE movies
             SET title = COALESCE(NULLIF(TRIM(title), ''), ?),
                 year = COALESCE(year, ?),
                 external_imdb = COALESCE(NULLIF(TRIM(external_imdb), ''), ?),
                 external_tmdb = COALESCE(NULLIF(TRIM(external_tmdb), ''), ?),
                 metadata_json = COALESCE(NULLIF(TRIM(CAST(metadata_json AS TEXT)), ''), ?),
                 updated_at = CURRENT_TIMESTAMP
             WHERE id = ?",
        )
        .bind(media.title.trim())
        .bind(media.year)
        .bind(trim_external_id(ids.imdb.as_deref()))
        .bind(trim_external_id(ids.tmdb.as_deref()))
        .bind(&metadata_json)
        .bind(media_item_id.to_string())
        .execute(pool)
        .await?;
    } else {
        sqlx::query::<sqlx::Any>(
            "INSERT INTO movies (id, title, year, external_imdb, external_tmdb, metadata_json, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .bind(media_item_id.to_string())
        .bind(media.title.trim())
        .bind(media.year)
        .bind(trim_external_id(ids.imdb.as_deref()))
        .bind(trim_external_id(ids.tmdb.as_deref()))
        .bind(&metadata_json)
        .execute(pool)
        .await?;
    }
    insert_movie_external_ids(pool, media_item_id, &ids).await?;
    Ok(media_item_id)
}

async fn ensure_find_media_scoped_series(
    pool: &sqlx::AnyPool,
    media: &ScopedAddMediaIdentity,
) -> AnyResult<Uuid> {
    let ids = media.external_ids.clone().unwrap_or_default();
    let media_item_id = find_existing_series_id(pool, media.kind, &ids, &media.title, media.year)
        .await?
        .unwrap_or_else(Uuid::new_v4);
    let external_ids_json = serde_json::to_string(&ids)?;
    let metadata_json = serde_json::to_string(&scoped_add_media_metadata(media))?;

    upsert_media_item_preserving_metadata(
        pool,
        media_item_id,
        media.kind,
        &media.title,
        media.year,
        &external_ids_json,
        &metadata_json,
    )
    .await?;

    let library_type = match media.kind {
        MediaType::Anime => "anime",
        _ => "series",
    };
    if sqlx::query_scalar::<sqlx::Any, String>("SELECT id FROM series WHERE id = ? LIMIT 1")
        .bind(media_item_id.to_string())
        .fetch_optional(pool)
        .await?
        .is_some()
    {
        sqlx::query::<sqlx::Any>(
            "UPDATE series
             SET title = COALESCE(NULLIF(TRIM(title), ''), ?),
                 year = COALESCE(year, ?),
                 library_type = ?,
                 external_imdb = COALESCE(NULLIF(TRIM(external_imdb), ''), ?),
                 external_tvdb_series = COALESCE(NULLIF(TRIM(external_tvdb_series), ''), ?),
                 external_anilist = COALESCE(NULLIF(TRIM(external_anilist), ''), ?),
                 metadata_json = COALESCE(NULLIF(TRIM(CAST(metadata_json AS TEXT)), ''), ?),
                 updated_at = CURRENT_TIMESTAMP
             WHERE id = ?",
        )
        .bind(media.title.trim())
        .bind(media.year)
        .bind(library_type)
        .bind(trim_external_id(ids.imdb.as_deref()))
        .bind(trim_external_id(
            ids.tvdb_series.as_deref().or(ids.tvdb.as_deref()),
        ))
        .bind(trim_external_id(ids.anilist.as_deref()))
        .bind(&metadata_json)
        .bind(media_item_id.to_string())
        .execute(pool)
        .await?;
    } else {
        sqlx::query::<sqlx::Any>(
            "INSERT INTO series (id, title, year, library_type, external_imdb, external_tvdb_series, external_anilist, metadata_json, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .bind(media_item_id.to_string())
        .bind(media.title.trim())
        .bind(media.year)
        .bind(library_type)
        .bind(trim_external_id(ids.imdb.as_deref()))
        .bind(trim_external_id(
            ids.tvdb_series.as_deref().or(ids.tvdb.as_deref()),
        ))
        .bind(trim_external_id(ids.anilist.as_deref()))
        .bind(&metadata_json)
        .execute(pool)
        .await?;
    }
    insert_series_external_ids(pool, media_item_id, &ids).await?;
    Ok(media_item_id)
}

async fn upsert_media_item_preserving_metadata(
    pool: &sqlx::AnyPool,
    media_item_id: Uuid,
    media_type: MediaType,
    title: &str,
    year: Option<i32>,
    external_ids_json: &str,
    metadata_json: &str,
) -> AnyResult<()> {
    if sqlx::query_scalar::<sqlx::Any, String>("SELECT id FROM media_items WHERE id = ? LIMIT 1")
        .bind(media_item_id.to_string())
        .fetch_optional(pool)
        .await?
        .is_some()
    {
        sqlx::query::<sqlx::Any>(
            "UPDATE media_items
             SET type = ?,
                 title = COALESCE(NULLIF(TRIM(title), ''), ?),
                 year = COALESCE(year, ?),
                 external_ids = COALESCE(NULLIF(TRIM(external_ids), ''), ?),
                 metadata_json = COALESCE(NULLIF(TRIM(CAST(metadata_json AS TEXT)), ''), ?),
                 updated_at = CURRENT_TIMESTAMP
             WHERE id = ?",
        )
        .bind(media_type.as_str())
        .bind(title.trim())
        .bind(year)
        .bind(external_ids_json)
        .bind(metadata_json)
        .bind(media_item_id.to_string())
        .execute(pool)
        .await?;
    } else {
        sqlx::query::<sqlx::Any>(
            "INSERT INTO media_items (id, type, external_ids, title, year, metadata_json, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        )
        .bind(media_item_id.to_string())
        .bind(media_type.as_str())
        .bind(external_ids_json)
        .bind(title.trim())
        .bind(year)
        .bind(metadata_json)
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn find_existing_movie_id(
    pool: &sqlx::AnyPool,
    ids: &ExternalIds,
    title: &str,
    year: Option<i32>,
) -> AnyResult<Option<Uuid>> {
    for (query, value) in [
        (
            "SELECT id FROM movies WHERE external_imdb = ? LIMIT 1",
            trim_external_id(ids.imdb.as_deref()),
        ),
        (
            "SELECT id FROM movies WHERE external_tmdb = ? LIMIT 1",
            trim_external_id(ids.tmdb.as_deref()),
        ),
    ] {
        if let Some(value) = value
            && let Some(id) = query_uuid_scalar(pool, query, value).await?
        {
            return Ok(Some(id));
        }
    }
    if let Some(id) =
        find_existing_external_id_owner(pool, "movie_external_ids", "movie_id", ids).await?
    {
        return Ok(Some(id));
    }
    find_existing_media_by_title(pool, "movies", "id", title, year, None).await
}

async fn find_existing_series_id(
    pool: &sqlx::AnyPool,
    media_type: MediaType,
    ids: &ExternalIds,
    title: &str,
    year: Option<i32>,
) -> AnyResult<Option<Uuid>> {
    for (query, value) in [
        (
            "SELECT id FROM series WHERE external_anilist = ? LIMIT 1",
            trim_external_id(ids.anilist.as_deref()),
        ),
        (
            "SELECT id FROM series WHERE external_tvdb_series = ? LIMIT 1",
            trim_external_id(ids.tvdb_series.as_deref().or(ids.tvdb.as_deref())),
        ),
        (
            "SELECT id FROM series WHERE external_imdb = ? LIMIT 1",
            trim_external_id(ids.imdb.as_deref()),
        ),
    ] {
        if let Some(value) = value
            && let Some(id) = query_uuid_scalar(pool, query, value).await?
        {
            return Ok(Some(id));
        }
    }
    if let Some(id) =
        find_existing_external_id_owner(pool, "series_external_ids", "series_id", ids).await?
    {
        return Ok(Some(id));
    }
    let library_type = match media_type {
        MediaType::Anime => Some("anime"),
        MediaType::Series => Some("series"),
        MediaType::Movie => None,
    };
    find_existing_media_by_title(pool, "series", "id", title, year, library_type).await
}

async fn query_uuid_scalar(
    pool: &sqlx::AnyPool,
    query: &str,
    value: &str,
) -> AnyResult<Option<Uuid>> {
    sqlx::query_scalar::<sqlx::Any, String>(query)
        .bind(value)
        .fetch_optional(pool)
        .await?
        .map(|id| Uuid::parse_str(&id).context("parsing library item id"))
        .transpose()
}

async fn find_existing_external_id_owner(
    pool: &sqlx::AnyPool,
    table: &str,
    owner_column: &str,
    ids: &ExternalIds,
) -> AnyResult<Option<Uuid>> {
    for (provider, external_id) in external_id_pairs(ids) {
        let query = format!(
            "SELECT {owner_column} FROM {table} WHERE provider = ? AND external_id = ? LIMIT 1"
        );
        if let Some(id) = sqlx::query_scalar::<sqlx::Any, String>(&query)
            .bind(provider)
            .bind(external_id)
            .fetch_optional(pool)
            .await?
            .map(|id| Uuid::parse_str(&id).context("parsing external id owner"))
            .transpose()?
        {
            return Ok(Some(id));
        }
    }
    Ok(None)
}

async fn find_existing_media_by_title(
    pool: &sqlx::AnyPool,
    table: &str,
    id_column: &str,
    title: &str,
    year: Option<i32>,
    library_type: Option<&str>,
) -> AnyResult<Option<Uuid>> {
    let normalized = normalize_name(title);
    if normalized.is_empty() {
        return Ok(None);
    }
    let rows = if let Some(library_type) = library_type {
        sqlx::query::<sqlx::Any>(&format!(
            "SELECT {id_column} AS id, title, year FROM {table} WHERE library_type = ?"
        ))
        .bind(library_type)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query::<sqlx::Any>(&format!(
            "SELECT {id_column} AS id, title, year FROM {table}"
        ))
        .fetch_all(pool)
        .await?
    };
    for row in rows {
        let row_title: String = row.try_get("title")?;
        let row_year = row.try_get::<Option<i64>, _>("year").ok().flatten();
        let year_matches = match (year, row_year) {
            (Some(left), Some(right)) => left as i64 == right,
            (None, _) | (_, None) => true,
        };
        if year_matches && normalize_name(&row_title) == normalized {
            let id: String = row.try_get("id")?;
            return Uuid::parse_str(&id)
                .map(Some)
                .context("parsing title-matched library id");
        }
    }
    Ok(None)
}

async fn insert_movie_external_ids(
    pool: &sqlx::AnyPool,
    movie_id: Uuid,
    ids: &ExternalIds,
) -> AnyResult<()> {
    for (provider, external_id) in external_id_pairs(ids) {
        sqlx::query::<sqlx::Any>(
            "INSERT INTO movie_external_ids (id, movie_id, provider, external_id, confidence, source)
             VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(movie_id.to_string())
        .bind(provider)
        .bind(external_id)
        .bind(1.0_f64)
        .bind("find_media_scoped_add")
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn insert_series_external_ids(
    pool: &sqlx::AnyPool,
    series_id: Uuid,
    ids: &ExternalIds,
) -> AnyResult<()> {
    for (provider, external_id) in external_id_pairs(ids) {
        sqlx::query::<sqlx::Any>(
            "INSERT INTO series_external_ids (id, series_id, provider, external_id, confidence, source)
             VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(series_id.to_string())
        .bind(provider)
        .bind(external_id)
        .bind(1.0_f64)
        .bind("find_media_scoped_add")
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn load_scoped_catalog_episode_ids(
    pool: &sqlx::AnyPool,
    media_item_id: Uuid,
    targets: &[FindMediaScopedPreviewTarget],
) -> AnyResult<HashMap<String, Uuid>> {
    let mut out = HashMap::new();
    for target in targets {
        let (Some(season), Some(episode)) = (target.season_number, target.episode_number) else {
            continue;
        };
        let Some(id) = sqlx::query_scalar::<sqlx::Any, String>(
            "SELECT id FROM episodes WHERE series_id = ? AND season_number = ? AND episode_number = ? LIMIT 1",
        )
        .bind(media_item_id.to_string())
        .bind(season)
        .bind(episode)
        .fetch_optional(pool)
        .await?
        else {
            continue;
        };
        out.insert(target.target_key.clone(), Uuid::parse_str(&id)?);
    }
    Ok(out)
}

fn scoped_add_media_metadata(media: &ScopedAddMediaIdentity) -> Value {
    json_object_without_nulls_local(json!({
        "source": "find_media_scoped_add",
        "title": media.title,
        "year": media.year,
        "externalIds": media.external_ids,
        "aliases": media.aliases,
    }))
}

fn scoped_add_episode_metadata(target: &FindMediaScopedPreviewTarget) -> Value {
    json_object_without_nulls_local(json!({
        "source": "find_media_scope_preview",
        "targetKey": target.target_key,
        "name": target.title,
        "title": target.title,
        "seasonNumber": target.season_number,
        "episodeNumber": target.episode_number,
        "absoluteNumber": target.absolute_episode_number,
        "airDate": target.air_date,
        "image": target.thumbnail_url,
        "thumbnail": target.thumbnail_url,
    }))
}

fn scoped_add_scope_json(
    document: &crate::acquisition::scoped_add::ScopedAddScopeDocument,
    media_item_id: Uuid,
    targets: &[FindMediaScopedPreviewTarget],
    episode_ids_by_target_key: &HashMap<String, Uuid>,
) -> AnyResult<Value> {
    let mut value = serde_json::to_value(document)?;
    let Some(map) = value.as_object_mut() else {
        bail!("scoped add scope document did not serialize to an object");
    };
    map.insert("mediaItemId".to_string(), json!(media_item_id.to_string()));
    map.insert(
        "targetKeys".to_string(),
        json!(
            targets
                .iter()
                .map(|target| target.target_key.clone())
                .collect::<Vec<_>>()
        ),
    );
    map.insert("selectedTargetCount".to_string(), json!(targets.len()));
    let mut target_map = JsonMap::new();
    for target in targets {
        target_map.insert(
            target.target_key.clone(),
            json_object_without_nulls_local(json!({
                "seasonNumber": target.season_number,
                "episodeNumber": target.episode_number,
                "absoluteEpisodeNumber": target.absolute_episode_number,
                "title": target.title,
                "airDate": target.air_date,
                "thumbnailUrl": target.thumbnail_url,
                "libraryEpisodeId": episode_ids_by_target_key
                    .get(&target.target_key)
                    .map(|id| id.to_string()),
            })),
        );
    }
    map.insert("targets".to_string(), Value::Object(target_map));
    Ok(value)
}

fn add_scoped_add_effective_route_policy(
    scope: &mut Value,
    route_policy: Option<AcquisitionRoutePolicy>,
) {
    if let (Some(map), Some(route_policy)) = (scope.as_object_mut(), route_policy) {
        map.insert(
            "effectiveRoutePolicy".to_string(),
            json!(route_policy.as_str()),
        );
    }
}

fn scoped_add_intent_target(
    selection: &ScopedAddSelection,
    _targets: &[FindMediaScopedPreviewTarget],
    scope_json: &Value,
) -> AcquisitionIntentTarget {
    AcquisitionIntentTarget {
        kind: Some(scoped_selection_kind(selection.selection_type).to_string()),
        title: None,
        target_key: None,
        target_keys: Vec::new(),
        season_number: None,
        episode_number: None,
        episode_start: None,
        episode_end: None,
        absolute_episode_number: None,
        absolute_episode_start: None,
        absolute_episode_end: None,
        air_date: None,
        air_time: None,
        metadata: Some(json_object_without_nulls_local(json!({
            "source": "find_media_scoped_add",
            "scope": scope_json,
        }))),
        targets: Vec::new(),
    }
}

fn scoped_add_new_acquisition_target(
    media_type: MediaType,
    media_title: &str,
    media_aliases: &[String],
    target: &FindMediaScopedPreviewTarget,
    library_episode_id: Option<Uuid>,
    media_item_id: Uuid,
    now: DateTime<Utc>,
) -> NewAcquisitionTarget {
    let title =
        target
            .title
            .clone()
            .or_else(|| match (target.season_number, target.episode_number) {
                (Some(season), Some(episode)) => {
                    Some(format!("{media_title} S{season:02}E{episode:02}"))
                }
                _ => Some(media_title.to_string()),
            });
    NewAcquisitionTarget {
        target_key: Some(target.target_key.clone()),
        media_type: Some(media_type),
        title,
        season_number: target.season_number,
        episode_number: target.episode_number,
        absolute_episode_number: target.absolute_episode_number,
        air_date: target.air_date.clone(),
        air_time: None,
        metadata: Some(json_object_without_nulls_local(json!({
            "source": "find_media_scoped_add",
            "mediaItemId": media_item_id.to_string(),
            "libraryEpisodeId": library_episode_id.map(|id| id.to_string()),
            "targetKey": target.target_key,
            "aliases": media_aliases,
            "title": target.title,
            "seasonNumber": target.season_number,
            "episodeNumber": target.episode_number,
            "absoluteEpisodeNumber": target.absolute_episode_number,
            "airDate": target.air_date,
            "thumbnailUrl": target.thumbnail_url,
            "scopeMetadata": {
                "mediaItemId": media_item_id.to_string(),
                "libraryEpisodeId": library_episode_id.map(|id| id.to_string()),
                "targetKey": target.target_key,
                "source": "find_media_scoped_add"
            }
        }))),
        state: Some(AcquisitionTargetState::Pending),
        next_search_after: Some(now),
    }
}

fn scoped_selection_kind(selection_type: ScopedAddSelectionType) -> &'static str {
    match selection_type {
        ScopedAddSelectionType::Movie => "movie",
        ScopedAddSelectionType::EntireTitle => "entire_title",
        ScopedAddSelectionType::Episode => "episode",
        ScopedAddSelectionType::Season => "season",
        ScopedAddSelectionType::Range => "range",
        ScopedAddSelectionType::SelectedTargets => "selected_targets",
        ScopedAddSelectionType::AnimeArc => "anime_arc",
    }
}

fn scoped_add_idempotency_key(
    provider_id: Uuid,
    media_type: MediaType,
    route_policy: AcquisitionRoutePolicy,
    scope_json: &Value,
) -> String {
    let material = serde_json::to_string(scope_json).unwrap_or_default();
    format!(
        "find-media:{}:{}:{}:{}",
        provider_id,
        media_type.as_str(),
        route_policy.as_str(),
        stable_hash_hex(&material)
    )
}

fn stable_hash_hex(value: &str) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

async fn apply_find_media_source_provider_config_defaults(
    store: &ExtensionStore<'_>,
    request: &mut CreateAcquisitionIntent,
) -> AnyResult<()> {
    let Some(source_provider_id) = request.source_provider_id else {
        return Ok(());
    };
    let provider = store
        .get_provider(source_provider_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("source provider was not found"))?;
    if !provider
        .capability
        .eq_ignore_ascii_case(ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY)
    {
        bail!("source provider must be an acquisition candidate provider");
    }
    let instance = store
        .get_instance(provider.instance_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("source provider instance was not found"))?;
    let Some(config) = instance.config_json.as_ref().and_then(Value::as_object) else {
        return Ok(());
    };

    if request.route_policy.is_none() {
        if let Some(route_policy) = config
            .get("routePolicy")
            .and_then(Value::as_str)
            .map(|value| value.parse::<AcquisitionRoutePolicy>())
            .transpose()?
        {
            request.route_policy = Some(route_policy);
        }
    }
    if request.release_delay_seconds.is_none()
        && let Some(delay) = config.get("releaseDelaySeconds").and_then(Value::as_i64)
    {
        if delay < 0 {
            bail!("releaseDelaySeconds cannot be negative");
        }
        request.release_delay_seconds = Some(delay);
    }
    if request.quality_profile.is_none() {
        request.quality_profile = find_media_source_quality_profile_from_config(config);
    }
    Ok(())
}

fn find_media_source_quality_profile_from_config(config: &JsonMap<String, Value>) -> Option<Value> {
    let mut profile = JsonMap::new();
    if let Some(values) = string_list_config(config.get("allowedQualities"))
        && !values.is_empty()
    {
        profile.insert("allowedQualities".to_string(), Value::Array(values));
    }
    if let Some(values) = string_list_config(config.get("requiredLanguages"))
        && !values.is_empty()
    {
        profile.insert("requiredLanguages".to_string(), Value::Array(values));
    }
    if let Some(max_size_bytes) = max_size_bytes_from_config(config) {
        profile.insert("maxSizeBytes".to_string(), json!(max_size_bytes));
    }
    (!profile.is_empty()).then_some(Value::Object(profile))
}

fn string_list_config(value: Option<&Value>) -> Option<Vec<Value>> {
    let values = match value {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| json!(value))
            .collect(),
        Some(Value::String(text)) => text
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| json!(value))
            .collect(),
        _ => return None,
    };
    Some(values)
}

fn max_size_bytes_from_config(config: &JsonMap<String, Value>) -> Option<u64> {
    let raw_bytes = config.get("maxSizeBytes").and_then(Value::as_u64);
    if raw_bytes.is_some() {
        return raw_bytes;
    }
    let max_size_gb = config
        .get("maxSizeGb")
        .and_then(|value| value.as_f64().filter(|gb| *gb > 0.0))?;
    Some((max_size_gb * 1024.0 * 1024.0 * 1024.0).round() as u64)
}

fn external_id_pairs(ids: &ExternalIds) -> Vec<(&'static str, String)> {
    [
        ("imdb", ids.imdb.as_deref()),
        ("tmdb", ids.tmdb.as_deref()),
        ("tvdb", ids.tvdb.as_deref()),
        ("tvdb_series", ids.tvdb_series.as_deref()),
        ("tvdb_movie", ids.tvdb_movie.as_deref()),
        ("anilist", ids.anilist.as_deref()),
        ("anidb", ids.anidb.as_deref()),
        ("mal", ids.mal.as_deref()),
        ("kitsu", ids.kitsu.as_deref()),
    ]
    .into_iter()
    .filter_map(|(provider, value)| {
        trim_external_id(value).map(|value| (provider, value.to_string()))
    })
    .collect()
}

fn trim_external_id(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn json_object_without_nulls_local(value: Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.into_iter()
                .filter_map(|(key, value)| (!value.is_null()).then_some((key, value)))
                .collect(),
        ),
        other => other,
    }
}

async fn build_find_media_scope_preview(
    state: &AppState,
    store: &ExtensionStore<'_>,
    payload: FindMediaScopePreviewRequest,
) -> AnyResult<FindMediaScopePreviewResponse> {
    if payload.result.kind != payload.media_type {
        bail!("scope preview media_type must match result.kind");
    }

    let mut media = payload.result.validated()?;
    let blockers =
        find_media_scope_provider_blockers(state, store, payload.media_type, payload.provider_id)
            .await?;

    let mut preview = match payload.media_type {
        MediaType::Movie => FindMediaScopePreviewResponse {
            media,
            capabilities: FindMediaScopePreviewCapabilities {
                entire_title: true,
                ..Default::default()
            },
            seasons: Vec::new(),
            arcs: Vec::new(),
            blockers,
        },
        MediaType::Series => {
            build_find_media_tv_scope_preview(state, media, MediaType::Series, blockers).await?
        }
        MediaType::Anime => build_find_media_anime_scope_preview(state, media, blockers).await?,
    };

    sort_scope_preview(&mut preview);
    media = preview.media.validated()?;
    preview.media = media;
    Ok(preview)
}

async fn find_media_scope_provider_blockers(
    state: &AppState,
    store: &ExtensionStore<'_>,
    media_type: MediaType,
    provider_id: Option<Uuid>,
) -> AnyResult<Vec<FindMediaScopePreviewBlocker>> {
    let providers = load_provider_contexts(store).await?;
    let source_contexts = collect_source_providers(&providers, media_type);
    let (available, unavailable) = filter_search_providers(state, store, source_contexts).await?;
    let mut blockers = Vec::new();

    if let Some(provider_id) = provider_id {
        if available
            .iter()
            .any(|provider| provider.detail.provider.provider_id == provider_id)
        {
            return Ok(blockers);
        }
        if let Some(error) = unavailable
            .iter()
            .find(|error| error.provider.detail.provider.provider_id == provider_id)
        {
            blockers.push(FindMediaScopePreviewBlocker {
                code: "source_provider_unavailable".to_string(),
                message: "The selected acquisition source is not ready.".to_string(),
                detail: Some(error.message.clone()),
            });
            return Ok(blockers);
        }
        blockers.push(FindMediaScopePreviewBlocker {
            code: "missing_provider".to_string(),
            message: "The selected acquisition source is not installed or enabled.".to_string(),
            detail: Some(provider_id.to_string()),
        });
        return Ok(blockers);
    }

    if available.is_empty() {
        blockers.push(FindMediaScopePreviewBlocker {
            code: "missing_provider".to_string(),
            message: "Install or enable an acquisition source before adding scoped media."
                .to_string(),
            detail: None,
        });
    }

    Ok(blockers)
}

async fn build_find_media_tv_scope_preview(
    state: &AppState,
    mut media: ScopedAddMediaIdentity,
    target_media_type: MediaType,
    mut blockers: Vec<FindMediaScopePreviewBlocker>,
) -> AnyResult<FindMediaScopePreviewResponse> {
    let mut ids = media.external_ids.clone().unwrap_or_default();
    let Some(tvdb_series_id) = resolve_scope_preview_tvdb_series_id(state, &ids).await? else {
        blockers.push(FindMediaScopePreviewBlocker {
            code: "ambiguous_identity".to_string(),
            message: "Elixir needs a TVDB series id or IMDb id before scoped episode selection."
                .to_string(),
            detail: None,
        });
        return Ok(scope_preview_response(
            media,
            Vec::new(),
            Vec::new(),
            blockers,
        ));
    };

    if !tvdb_api_key_configured(state) {
        blockers.push(FindMediaScopePreviewBlocker {
            code: "missing_metadata_credentials".to_string(),
            message: "TVDB metadata is not configured, so Elixir cannot preview episode scopes."
                .to_string(),
            detail: Some("Configure the TVDB API key on the server.".to_string()),
        });
        return Ok(scope_preview_response(
            media,
            Vec::new(),
            Vec::new(),
            blockers,
        ));
    }

    ids.tvdb_series = Some(tvdb_series_id.clone());
    if ids.tvdb.is_none() {
        ids.tvdb = Some(tvdb_series_id.clone());
    }
    media.external_ids = Some(ids);

    let season_values = state
        .linkers
        .fetch_tvdb_series_seasons(&tvdb_series_id)
        .await
        .with_context(|| format!("fetching TVDB seasons for scope preview {tvdb_series_id}"))?;
    let mut season_numbers = season_values
        .iter()
        .filter_map(extract_preview_season_number)
        .filter(|season| *season > 0)
        .collect::<Vec<_>>();
    season_numbers.sort_unstable();
    season_numbers.dedup();

    let mut seasons = Vec::new();
    for season_number in season_numbers {
        let episodes = state
            .linkers
            .fetch_tvdb_season_episodes(&tvdb_series_id, season_number)
            .await
            .with_context(|| {
                format!(
                    "fetching TVDB season {season_number} episodes for scope preview {tvdb_series_id}"
                )
            })?
            .into_iter()
            .filter_map(|episode| {
                let episode_number = episode.episode_number.filter(|value| *value > 0)?;
                Some(FindMediaScopePreviewEpisode {
                    target_key: format!("S{season_number:02}E{episode_number:02}"),
                    season_number: Some(season_number),
                    episode_number: Some(episode_number),
                    absolute_episode_number: episode.absolute_number.filter(|value| *value > 0),
                    title: episode.title,
                    air_date: extract_preview_air_date(&episode.raw),
                    thumbnail_url: episode.image,
                })
            })
            .collect::<Vec<_>>();
        if episodes.is_empty() {
            continue;
        }
        seasons.push(FindMediaScopePreviewSeason {
            season_number,
            episode_count: episodes.len(),
            episodes,
        });
    }

    if seasons.is_empty() {
        blockers.push(FindMediaScopePreviewBlocker {
            code: "metadata_unavailable".to_string(),
            message: "TVDB did not return selectable episodes for this title.".to_string(),
            detail: Some(tvdb_series_id),
        });
    }

    let mut response = scope_preview_response(media, seasons, Vec::new(), blockers);
    if target_media_type == MediaType::Movie {
        response.capabilities.entire_title = true;
    }
    Ok(response)
}

async fn build_find_media_anime_scope_preview(
    state: &AppState,
    mut media: ScopedAddMediaIdentity,
    mut blockers: Vec<FindMediaScopePreviewBlocker>,
) -> AnyResult<FindMediaScopePreviewResponse> {
    let ids = media.external_ids.clone().unwrap_or_default();
    let Some(seed_anilist_id) = ids.anilist.clone() else {
        if ids.tvdb_series.is_some() || ids.tvdb.is_some() || ids.imdb.is_some() {
            return build_find_media_tv_scope_preview(state, media, MediaType::Anime, blockers)
                .await;
        }
        blockers.push(FindMediaScopePreviewBlocker {
            code: "ambiguous_identity".to_string(),
            message: "Elixir needs an AniList id before scoped anime episode selection."
                .to_string(),
            detail: None,
        });
        return Ok(scope_preview_response(
            media,
            Vec::new(),
            Vec::new(),
            blockers,
        ));
    };

    let seed_mapping = state
        .linkers
        .fetch_anizip_mapping(&seed_anilist_id)
        .await
        .with_context(|| format!("fetching ani.zip mapping for scope preview {seed_anilist_id}"))?;
    let seed_season = seed_mapping
        .as_ref()
        .and_then(infer_anizip_season_number)
        .unwrap_or(1);
    let mut chain = resolve_anilist_season_chain(
        Some(&state.settings.classifier),
        seed_season,
        &seed_anilist_id,
        1.0,
    )
    .await
    .with_context(|| {
        format!("resolving AniList season chain for scope preview {seed_anilist_id}")
    })?;

    if chain.is_empty() {
        chain.push(AniListSeasonChainEntry {
            season_number: seed_season,
            anilist_id: seed_anilist_id.clone(),
            title: media.title.clone(),
            format: None,
            season_year: media.year,
            start_year: media.year,
            status: None,
            episodes: None,
            next_airing_episode: None,
            next_airing_at: None,
            confidence: 1.0,
        });
    }

    let mut season_mappings = Vec::new();
    let mut seen = HashSet::new();
    for season in chain {
        if !seen.insert(season.anilist_id.clone()) {
            continue;
        }
        let mapping = if season.anilist_id == seed_anilist_id {
            seed_mapping.clone()
        } else {
            state
                .linkers
                .fetch_anizip_mapping(&season.anilist_id)
                .await
                .with_context(|| {
                    format!(
                        "fetching ani.zip mapping for scope preview season {}",
                        season.anilist_id
                    )
                })?
        };
        season_mappings.push(AnimeSeasonMapping { season, mapping });
    }

    let graph = build_anime_metadata_graph(AnimeMetadataGraphInput {
        title: media.title.clone(),
        year: media.year,
        seed_anilist_id,
        seed_season_number: seed_season,
        external_ids: ids,
        seasons: season_mappings,
    });

    media.external_ids = Some(graph.external_ids.clone());
    media.aliases = graph.aliases.clone();
    let seasons = anime_scope_preview_seasons_from_graph(&graph);

    if seasons.is_empty() {
        blockers.push(FindMediaScopePreviewBlocker {
            code: "metadata_unavailable".to_string(),
            message: "AniList and ani.zip did not return selectable episodes for this anime."
                .to_string(),
            detail: None,
        });
    }

    Ok(scope_preview_response(media, seasons, Vec::new(), blockers))
}

fn anime_scope_preview_seasons_from_graph(
    graph: &crate::acquisition::release_resolution::anime::AnimeMetadataGraph,
) -> Vec<FindMediaScopePreviewSeason> {
    anime_scope_preview_seasons_from_graph_at(graph, Utc::now())
}

fn anime_scope_preview_seasons_from_graph_at(
    graph: &crate::acquisition::release_resolution::anime::AnimeMetadataGraph,
    now: DateTime<Utc>,
) -> Vec<FindMediaScopePreviewSeason> {
    graph
        .seasons
        .iter()
        .filter_map(|season| {
            let episodes = graph
                .targets
                .iter()
                .filter(|target| {
                    anime_scope_target_is_selectable(target, now)
                        && (target.season_number == Some(season.season_number)
                            || (target.season_number.is_none()
                                && target.anilist_season_id == season.anilist_id))
                })
                .map(|target| FindMediaScopePreviewEpisode {
                    target_key: target.target_key.clone(),
                    season_number: target.season_number.or(Some(season.season_number)),
                    episode_number: target.episode_number,
                    absolute_episode_number: target.absolute_episode_number,
                    title: Some(target.title.clone()),
                    air_date: target.air_date.clone(),
                    thumbnail_url: preview_thumbnail_from_raw(&target.raw),
                })
                .collect::<Vec<_>>();
            (!episodes.is_empty()).then_some(FindMediaScopePreviewSeason {
                season_number: season.season_number,
                episode_count: episodes.len(),
                episodes,
            })
        })
        .collect::<Vec<_>>()
}

fn anime_scope_target_is_selectable(
    target: &crate::acquisition::release_resolution::anime::AnimeGraphTarget,
    now: DateTime<Utc>,
) -> bool {
    if let Some(air_time) = target.air_time {
        return air_time <= now;
    }
    if let Some(air_date) = target
        .air_date
        .as_deref()
        .and_then(parse_scope_preview_air_date)
    {
        return air_date <= now.date_naive();
    }
    true
}

fn parse_scope_preview_air_date(value: &str) -> Option<NaiveDate> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    DateTime::parse_from_rfc3339(trimmed)
        .map(|value| value.with_timezone(&Utc).date_naive())
        .ok()
        .or_else(|| NaiveDate::parse_from_str(trimmed, "%Y-%m-%d").ok())
}

fn scope_preview_response(
    media: ScopedAddMediaIdentity,
    seasons: Vec<FindMediaScopePreviewSeason>,
    arcs: Vec<crate::acquisition::scoped_add::FindMediaScopePreviewArc>,
    blockers: Vec<FindMediaScopePreviewBlocker>,
) -> FindMediaScopePreviewResponse {
    let has_episodes = seasons.iter().any(|season| !season.episodes.is_empty());
    FindMediaScopePreviewResponse {
        media,
        capabilities: FindMediaScopePreviewCapabilities {
            entire_title: true,
            seasons: has_episodes,
            episode_range: has_episodes,
            selected_episodes: has_episodes,
            anime_arcs: !arcs.is_empty(),
        },
        seasons,
        arcs,
        blockers,
    }
}

fn sort_scope_preview(response: &mut FindMediaScopePreviewResponse) {
    response.seasons.sort_by_key(|season| season.season_number);
    for season in &mut response.seasons {
        season.episodes.sort_by_key(|episode| {
            (
                episode.absolute_episode_number.unwrap_or(i32::MAX),
                episode.episode_number.unwrap_or(i32::MAX),
                episode.season_number.unwrap_or(i32::MAX),
                episode.target_key.clone(),
            )
        });
        season.episode_count = season.episodes.len();
    }
    response.arcs.sort_by(|left, right| {
        left.label
            .cmp(&right.label)
            .then_with(|| left.arc_id.cmp(&right.arc_id))
    });
}

async fn resolve_scope_preview_tvdb_series_id(
    state: &AppState,
    ids: &ExternalIds,
) -> AnyResult<Option<String>> {
    if let Some(value) = ids.tvdb_series.as_ref().or(ids.tvdb.as_ref()) {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return Ok(Some(trimmed.to_string()));
        }
    }
    if let Some(imdb) = ids
        .imdb
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return state.linkers.link_tvdb_series_by_imdb(imdb).await;
    }
    Ok(None)
}

fn tvdb_api_key_configured(state: &AppState) -> bool {
    state
        .settings
        .classifier
        .tvdb_api_key
        .as_deref()
        .map(str::trim)
        .is_some_and(|value| !value.is_empty())
}

fn extract_preview_season_number(value: &Value) -> Option<i32> {
    for key in ["number", "seasonNumber", "season_number", "season"] {
        if let Some(number) = json_i32(value.get(key)) {
            return Some(number);
        }
    }
    None
}

fn extract_preview_air_date(value: &Value) -> Option<String> {
    for key in [
        "airdate",
        "aired",
        "firstAired",
        "first_aired",
        "airDate",
        "air_date",
    ] {
        if let Some(text) = value.get(key).and_then(Value::as_str).map(str::trim) {
            if !text.is_empty() {
                return Some(text.to_string());
            }
        }
    }
    None
}

fn preview_thumbnail_from_raw(value: &Value) -> Option<String> {
    for key in ["image", "thumbnail", "thumbnailUrl", "thumbnail_url"] {
        if let Some(text) = value.get(key).and_then(Value::as_str).map(str::trim) {
            if !text.is_empty() {
                return Some(text.to_string());
            }
        }
    }
    None
}

fn json_i32(value: Option<&Value>) -> Option<i32> {
    let value = value?;
    if let Some(number) = value.as_i64() {
        return i32::try_from(number).ok();
    }
    value.as_str()?.trim().parse::<i32>().ok()
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

    sync_source_acquisition_imports(state, store, &provider_map, &downloader_progress).await?;
    let ux_context = load_acquisition_ux_context(store).await?;

    let mut items = Vec::new();
    for intent in store.list_active_managed_ingest_intents().await? {
        if intent.source == SOURCE_ACQUISITION_INTENT_SOURCE {
            continue;
        }
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
    items.extend(
        build_source_acquisition_items(
            state,
            store,
            &provider_map,
            &downloader_progress,
            recent_cutoff,
            &ux_context,
        )
        .await?,
    );

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
            AcquisitionPhase::NeedsAttention
                | AcquisitionPhase::Failed
                | AcquisitionPhase::ReviewRequired
                | AcquisitionPhase::Quarantined
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
        media_item_id: None,
        source: intent.source.clone(),
        request_mode: "managed".to_string(),
        request_scope: "library_item".to_string(),
        request_label: "Manager add".to_string(),
        one_shot: false,
        phase: state_view.phase.as_str().to_string(),
        phase_label: state_view.phase.label().to_string(),
        headline: state_view.headline.clone(),
        detail: state_view.detail.clone(),
        target_count: state_view.children.len().max(1),
        displayed_child_count: state_view.children.len(),
        hidden_child_count: 0,
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

async fn sync_source_acquisition_imports(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider_map: &HashMap<Uuid, ProviderContext>,
    downloader_progress: &AcquisitionDownloaderProgressIndex,
) -> AnyResult<()> {
    let release_job_download_ids = acquisition_release_job_download_ids(&state.db_pool).await?;
    let subscriptions = list_subscriptions(
        &state.db_pool,
        AcquisitionSubscriptionFilter { active: Some(true) },
    )
    .await?;

    for subscription in subscriptions {
        let targets = list_subscription_targets(&state.db_pool, subscription.subscription_id)
            .await?
            .into_iter()
            .filter(|target| target.state == AcquisitionTargetState::Submitted)
            .collect::<Vec<_>>();
        if targets.is_empty() {
            continue;
        }

        let Some(source_provider_id) = source_provider_id_for_subscription(&subscription, &targets)
        else {
            continue;
        };
        let source_label = provider_map
            .get(&source_provider_id)
            .map(provider_label)
            .unwrap_or_else(|| "Acquisition source".to_string());
        let source_implementation = provider_map
            .get(&source_provider_id)
            .and_then(|provider| provider.detail.provider.implementation.clone());

        let mut intent = None;
        for target in targets {
            let Some(download_id) = target.download_id.as_deref() else {
                continue;
            };
            if release_job_download_ids.contains(download_id) {
                continue;
            }
            let Some(progress) = downloader_progress.get(download_id) else {
                continue;
            };
            if !downloader_progress_is_completed(progress) {
                continue;
            }

            match select_source_import_file(state, &subscription, &target, progress) {
                SourceImportSelection::Pending => {}
                SourceImportSelection::NeedsFileSelection(reason) => {
                    update_target_state(
                        &state.db_pool,
                        target.target_id,
                        AcquisitionTargetStateUpdate {
                            state: AcquisitionTargetState::Submitted,
                            state_reason: Some(reason),
                            ..Default::default()
                        },
                    )
                    .await?;
                }
                SourceImportSelection::Ready(file) => {
                    if intent.is_none() {
                        intent = Some(
                            upsert_source_managed_ingest_intent(
                                store,
                                &subscription,
                                source_provider_id,
                                &source_label,
                            )
                            .await?,
                        );
                    }
                    let Some(intent) = intent.as_ref() else {
                        continue;
                    };
                    let event = source_managed_import_event(
                        intent,
                        &subscription,
                        &target,
                        source_provider_id,
                        &source_label,
                        source_implementation.as_deref(),
                        file,
                        progress,
                    );
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
                        Ok(Some(_)) => {
                            update_target_state(
                                &state.db_pool,
                                target.target_id,
                                AcquisitionTargetStateUpdate {
                                    state: AcquisitionTargetState::Imported,
                                    state_reason: Some(
                                        "Imported into the Elixir library.".to_string(),
                                    ),
                                    import_event_id: Some(persisted.event_id),
                                    ..Default::default()
                                },
                            )
                            .await?;
                        }
                        Ok(None) => {
                            update_target_state(
                                &state.db_pool,
                                target.target_id,
                                AcquisitionTargetStateUpdate {
                                    state: AcquisitionTargetState::Submitted,
                                    state_reason: Some(
                                        SOURCE_ACQUISITION_WAITING_FOR_FILE_REASON.to_string(),
                                    ),
                                    import_event_id: Some(persisted.event_id),
                                    ..Default::default()
                                },
                            )
                            .await?;
                        }
                        Err(err) => {
                            let detail = err.to_string();
                            store
                                .mark_managed_import_event_failed(persisted.event_id, &detail)
                                .await?;
                            update_target_state(
                                &state.db_pool,
                                target.target_id,
                                AcquisitionTargetStateUpdate {
                                    state: AcquisitionTargetState::Submitted,
                                    state_reason: Some(format!("Import failed: {detail}")),
                                    import_event_id: Some(persisted.event_id),
                                    ..Default::default()
                                },
                            )
                            .await?;
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

async fn acquisition_release_job_download_ids(pool: &sqlx::AnyPool) -> AnyResult<HashSet<String>> {
    let rows = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT DISTINCT download_id
         FROM acquisition_release_jobs
         WHERE download_id IS NOT NULL
           AND TRIM(download_id) <> ''",
    )
    .fetch_all(pool)
    .await
    .context("loading acquisition release job download ids")?;
    Ok(rows.into_iter().collect())
}

async fn upsert_source_managed_ingest_intent(
    store: &ExtensionStore<'_>,
    subscription: &AcquisitionSubscription,
    source_provider_id: Uuid,
    source_label: &str,
) -> AnyResult<crate::extensions::store::ManagedIngestIntent> {
    let intent_id = store
        .upsert_managed_ingest_intent(&NewManagedIngestIntent {
            media_type: subscription.media_type,
            title: subscription.title.clone(),
            normalized_title: subscription.normalized_title.clone(),
            year: subscription.year,
            external_ids: subscription.external_ids.clone(),
            manager_provider_id: source_provider_id,
            manager_item_id: Some(subscription.subscription_id.to_string()),
            manager_label: Some(source_label.to_string()),
            source: SOURCE_ACQUISITION_INTENT_SOURCE.to_string(),
        })
        .await?;
    store
        .list_active_managed_ingest_intents()
        .await?
        .into_iter()
        .find(|intent| intent.intent_id == intent_id)
        .ok_or_else(|| anyhow::anyhow!("source acquisition managed intent was not readable"))
}

fn source_managed_import_event(
    intent: &crate::extensions::store::ManagedIngestIntent,
    subscription: &AcquisitionSubscription,
    target: &AcquisitionTarget,
    source_provider_id: Uuid,
    source_label: &str,
    source_implementation: Option<&str>,
    file: ManagedImportFile,
    progress: &AcquisitionDownloaderProgress,
) -> NewManagedImportEvent {
    let event_key = managed_import_event_key(
        intent,
        "source_acquisition",
        &[format!(
            "target:{}:{}",
            target.target_id,
            file.path.to_ascii_lowercase()
        )],
    );
    NewManagedImportEvent {
        event_key,
        intent_id: intent.intent_id,
        media_type: target.media_type,
        external_ids: subscription.external_ids.clone(),
        manager_provider_id: source_provider_id,
        manager_item_id: Some(subscription.subscription_id.to_string()),
        manager_label: Some(source_label.to_string()),
        manager_implementation: source_implementation.map(str::to_string),
        imported_files: vec![file],
        raw_manager_payload: Some(json!({
            "acquisitionSubscriptionId": subscription.subscription_id.to_string(),
            "acquisitionTargetId": target.target_id.to_string(),
            "targetKey": target.target_key.clone(),
            "selectedProviderId": target.selected_provider_id.map(|value| value.to_string()),
            "selectedRouteLogicalId": target.selected_route_logical_id.clone(),
            "selectedCandidate": target.selected_candidate.clone(),
            "downloadId": target.download_id.clone(),
            "download": {
                "releaseTitle": progress.release_title.clone(),
                "status": progress.status.clone(),
                "category": progress.category.clone(),
                "localPath": progress.local_path.clone(),
                "progressPercent": progress.progress_percent,
                "downloadedBytes": progress.downloaded_bytes,
                "totalBytes": progress.size_bytes
            }
        })),
        imported_at: Some(Utc::now()),
    }
}

async fn build_source_acquisition_items(
    state: &AppState,
    _store: &ExtensionStore<'_>,
    provider_map: &HashMap<Uuid, ProviderContext>,
    downloader_progress: &AcquisitionDownloaderProgressIndex,
    recent_cutoff: DateTime<Utc>,
    ux_context: &AcquisitionUxContext,
) -> AnyResult<Vec<FindMediaAcquisitionItem>> {
    let subscriptions = list_subscriptions(
        &state.db_pool,
        AcquisitionSubscriptionFilter { active: None },
    )
    .await?;
    let mut items = Vec::new();

    for subscription in subscriptions {
        if !source_subscription_can_surface_in_acquisition_log(&subscription) {
            continue;
        }
        let targets =
            list_subscription_targets(&state.db_pool, subscription.subscription_id).await?;
        let release_runtime =
            load_source_release_runtime_by_target(&state.db_pool, subscription.subscription_id)
                .await?;
        let Some(item) = build_source_acquisition_item(
            &subscription,
            &targets,
            provider_map,
            downloader_progress,
            &release_runtime,
            ux_context,
        ) else {
            continue;
        };
        if !source_acquisition_item_should_remain_visible(&subscription, &item, recent_cutoff) {
            continue;
        }
        items.push(item);
    }

    Ok(items)
}

fn source_subscription_can_surface_in_acquisition_log(
    subscription: &AcquisitionSubscription,
) -> bool {
    subscription.active
        || (subscription.request_mode.is_one_shot()
            && subscription.status == AcquisitionSubscriptionStatus::Completed)
}

fn source_acquisition_item_should_remain_visible(
    subscription: &AcquisitionSubscription,
    item: &FindMediaAcquisitionItem,
    recent_cutoff: DateTime<Utc>,
) -> bool {
    if subscription.active {
        return true;
    }
    if subscription.updated_at < recent_cutoff {
        return false;
    }
    if item.phase == AcquisitionPhase::Completed.as_str() {
        let reference = item.last_matched_at.unwrap_or(item.updated_at);
        return reference >= recent_cutoff;
    }
    true
}

async fn load_acquisition_ux_context(
    store: &ExtensionStore<'_>,
) -> AnyResult<AcquisitionUxContext> {
    let canonical_extension = store.get_extension(DEBRID_EXTENSION_ID).await?;
    let legacy_extension = store.get_extension(LEGACY_REAL_DEBRID_EXTENSION_ID).await?;
    let mut instances = store.list_instances(Some(DEBRID_EXTENSION_ID)).await?;
    instances.extend(
        store
            .list_instances(Some(LEGACY_REAL_DEBRID_EXTENSION_ID))
            .await?,
    );
    let mut has_enabled_token = false;
    for instance in instances.into_iter().filter(|instance| instance.enabled) {
        let service = active_debrid_service_from_config(instance.config_json.as_ref())?;
        if debrid_secret_exists_for_instance(store, instance.instance_id, service).await? {
            has_enabled_token = true;
            break;
        }
    }
    Ok(AcquisitionUxContext {
        debrid_account_missing: (canonical_extension.is_some() || legacy_extension.is_some())
            && !has_enabled_token,
    })
}

async fn load_source_release_runtime_by_target(
    pool: &sqlx::AnyPool,
    subscription_id: Uuid,
) -> AnyResult<HashMap<Uuid, SourceTargetReleaseRuntime>> {
    let releases = list_releases(
        pool,
        ReleaseListFilter {
            subscription_id: Some(subscription_id),
            state: None,
            limit: Some(500),
        },
    )
    .await?;
    let mut index = HashMap::new();

    for release in releases {
        let latest_job = list_release_jobs(pool, release.release_id)
            .await?
            .into_iter()
            .max_by_key(|job| job.updated_at);
        let latest_import = list_import_runs_by_release(pool, release.release_id)
            .await?
            .into_iter()
            .max_by_key(|run| run.updated_at);
        let coverages = list_release_coverage(pool, release.release_id).await?;

        for coverage in coverages {
            let mut updated_at = release.updated_at;
            if coverage.updated_at > updated_at {
                updated_at = coverage.updated_at;
            }
            if let Some(job) = latest_job
                .as_ref()
                .filter(|job| job.updated_at > updated_at)
            {
                updated_at = job.updated_at;
            }
            if let Some(import_run) = latest_import
                .as_ref()
                .filter(|import_run| import_run.updated_at > updated_at)
            {
                updated_at = import_run.updated_at;
            }

            let runtime = SourceTargetReleaseRuntime {
                release_id: release.release_id,
                source_provider_id: release.source_provider_id,
                route_provider_id: latest_job
                    .as_ref()
                    .and_then(|job| job.provider_id)
                    .or(release.selected_provider_id),
                release_title: release.release_title.clone(),
                release_state: release.state,
                release_state_reason: release.state_reason.clone(),
                coverage_state: Some(coverage.state),
                coverage_reason: coverage.reason.clone(),
                selected_route_logical_id: release
                    .selected_route_logical_id
                    .clone()
                    .or_else(|| latest_job.as_ref().map(|job| job.route_logical_id.clone())),
                download_id: release
                    .download_id
                    .clone()
                    .or_else(|| latest_job.as_ref().and_then(|job| job.download_id.clone()))
                    .or_else(|| {
                        latest_import
                            .as_ref()
                            .and_then(|import_run| import_run.download_id.clone())
                    }),
                job_state: latest_job.as_ref().map(|job| job.state),
                job_state_reason: latest_job.as_ref().and_then(|job| job.state_reason.clone()),
                import_state: latest_import.as_ref().map(|import_run| import_run.state),
                import_state_reason: latest_import
                    .as_ref()
                    .and_then(|import_run| import_run.state_reason.clone()),
                import_mismatch_class: latest_import
                    .as_ref()
                    .and_then(|import_run| import_run.mismatch_class.clone()),
                updated_at,
            };

            let should_replace = index
                .get(&coverage.target_id)
                .map(|existing: &SourceTargetReleaseRuntime| {
                    runtime.updated_at > existing.updated_at
                })
                .unwrap_or(true);
            if should_replace {
                index.insert(coverage.target_id, runtime);
            }
        }
    }

    Ok(index)
}

fn build_source_acquisition_item(
    subscription: &AcquisitionSubscription,
    targets: &[AcquisitionTarget],
    provider_map: &HashMap<Uuid, ProviderContext>,
    downloader_progress: &AcquisitionDownloaderProgressIndex,
    release_runtime: &HashMap<Uuid, SourceTargetReleaseRuntime>,
    ux_context: &AcquisitionUxContext,
) -> Option<FindMediaAcquisitionItem> {
    let source_provider_id = source_provider_id_for_subscription(subscription, targets);
    let source_provider = source_provider_id.and_then(|id| provider_map.get(&id));
    let manager_provider_id = source_provider_id.unwrap_or_else(Uuid::nil);
    let source_label = source_provider
        .map(provider_label)
        .unwrap_or_else(|| "Acquisition source".to_string());

    if targets.is_empty() {
        return Some(build_empty_source_acquisition_item(
            subscription,
            manager_provider_id,
            source_label,
            ux_context,
        ));
    }

    let mut children = targets
        .iter()
        .map(|target| {
            build_source_acquisition_child(
                target,
                source_provider_id,
                provider_map,
                target
                    .download_id
                    .as_deref()
                    .and_then(|download_id| downloader_progress.get(download_id))
                    .or_else(|| {
                        release_runtime
                            .get(&target.target_id)
                            .and_then(|runtime| runtime.download_id.as_deref())
                            .and_then(|download_id| downloader_progress.get(download_id))
                    }),
                release_runtime.get(&target.target_id),
            )
        })
        .collect::<Vec<_>>();
    children.sort_by(|left, right| {
        let left_phase = acquisition_phase_from_str(&left.phase);
        let right_phase = acquisition_phase_from_str(&right.phase);
        left_phase
            .sort_priority()
            .cmp(&right_phase.sort_priority())
            .then_with(|| {
                source_target_sort_key(&left.title).cmp(&source_target_sort_key(&right.title))
            })
    });
    let target_count = children.len();
    let counts = source_acquisition_counts(&children);
    let phase = summarize_source_acquisition_phase(counts);
    let last_matched_at = targets
        .iter()
        .filter(|target| target.state == AcquisitionTargetState::Imported)
        .map(|target| target.updated_at.clone())
        .max();
    let updated_at = targets
        .iter()
        .map(|target| target.updated_at.clone())
        .max()
        .unwrap_or_else(|| subscription.updated_at.clone());
    let progress_percent = source_parent_progress(&children);
    let downloader_label = children
        .iter()
        .filter_map(|child| child.downloader_label.clone())
        .next();
    let protocol = children
        .iter()
        .filter_map(|child| child.protocol.clone())
        .next();
    let debrid_account_missing =
        ux_context.debrid_account_missing && route_policy_allows_debrid(subscription.route_policy);
    let blocker = build_source_acquisition_blocker(&children)
        .or_else(|| debrid_account_missing.then(build_missing_debrid_account_blocker));
    let media_item_id = source_acquisition_media_item_id(subscription, targets);
    let actions = build_source_acquisition_actions(
        subscription,
        &children,
        counts,
        media_item_id.as_deref(),
        debrid_account_missing,
    );
    let mut evidence = vec![
        acquisition_evidence("Source", source_label.clone(), Some("neutral")),
        acquisition_evidence("Targets", target_count.to_string(), Some("neutral")),
    ];
    evidence.extend(source_status_count_evidence(counts));
    if let Some(route) = subscription_route_evidence(targets) {
        evidence.push(acquisition_evidence("Route", route, Some("neutral")));
    }
    if let Some(route_provider) = source_route_provider_evidence(&children) {
        evidence.push(acquisition_evidence(
            "Route provider",
            route_provider,
            Some("neutral"),
        ));
    }
    if debrid_account_missing {
        evidence.push(acquisition_evidence(
            "Debrid account",
            "Add debrid account",
            Some("warning"),
        ));
    }
    let hidden_child_count = children.len().saturating_sub(250);
    if children.len() > 250 {
        children.truncate(250);
    }
    let displayed_child_count = children.len();

    Some(FindMediaAcquisitionItem {
        intent_id: subscription.subscription_id,
        title: subscription.title.clone(),
        media_type: media_type_api_name(subscription.media_type).to_string(),
        year: subscription.year,
        external_ids: subscription.external_ids.clone(),
        manager_provider_id,
        manager_label: source_label,
        manager_item_id: Some(subscription.subscription_id.to_string()),
        media_item_id,
        source: SOURCE_ACQUISITION_INTENT_SOURCE.to_string(),
        request_mode: subscription.request_mode.as_str().to_string(),
        request_scope: subscription.request_scope.as_str().to_string(),
        request_label: source_acquisition_request_label(subscription),
        one_shot: subscription.request_mode.is_one_shot(),
        phase: phase.as_str().to_string(),
        phase_label: source_parent_phase_label(phase, counts, target_count),
        headline: format_source_acquisition_headline(counts, targets.len()),
        detail: Some(format_source_acquisition_detail(counts, targets.len())),
        target_count,
        displayed_child_count,
        hidden_child_count,
        blocker,
        evidence,
        actions,
        stage: phase.legacy_stage().to_string(),
        stage_label: phase.legacy_stage_label().to_string(),
        description: format_source_acquisition_detail(counts, targets.len()),
        progress_percent,
        eta_seconds: children.iter().find_map(|child| child.eta_seconds),
        downloader_label,
        protocol,
        last_matched_at,
        created_at: subscription.created_at.clone(),
        updated_at,
        children,
    })
}

fn source_acquisition_request_label(subscription: &AcquisitionSubscription) -> String {
    if subscription.request_mode != AcquisitionRequestMode::OneShot {
        return "Monitored acquisition".to_string();
    }
    if let Some(label) = find_media_scoped_request_label(subscription) {
        return label;
    }
    match subscription.request_scope {
        AcquisitionRequestScope::Movie => "One-time movie request",
        AcquisitionRequestScope::Episode => "One-time episode request",
        AcquisitionRequestScope::Season => "One-time season request",
        AcquisitionRequestScope::Range => "One-time range request",
        AcquisitionRequestScope::Missing => "One-time missing request",
        AcquisitionRequestScope::SelectedTargets => "One-time selected targets request",
        AcquisitionRequestScope::AnimeArc => "One-time anime arc request",
        AcquisitionRequestScope::Subscription => "One-time request",
    }
    .to_string()
}

fn find_media_scoped_request_label(subscription: &AcquisitionSubscription) -> Option<String> {
    let document = find_media_scoped_scope_document(subscription)?;
    let selection = &document.selection;
    Some(match selection.selection_type {
        ScopedAddSelectionType::Movie => "Movie requested".to_string(),
        ScopedAddSelectionType::EntireTitle => match document.media.kind {
            MediaType::Movie => "Movie requested".to_string(),
            MediaType::Series | MediaType::Anime => "Entire title requested".to_string(),
        },
        ScopedAddSelectionType::Episode => {
            if let Some(key) = selection.target_keys.first() {
                format!("{} requested", source_target_key_display(key))
            } else if let (Some(season), Some(episode)) =
                (selection.season_number, selection.episode_number)
            {
                format!("S{season:02}E{episode:02} requested")
            } else if let Some(absolute) = selection.absolute_episode_number {
                format!("Episode {absolute} requested")
            } else {
                "Episode requested".to_string()
            }
        }
        ScopedAddSelectionType::Season => selection
            .season_number
            .map(|season| format!("Season {season} requested"))
            .unwrap_or_else(|| "Season requested".to_string()),
        ScopedAddSelectionType::Range => {
            source_range_request_label(selection).unwrap_or_else(|| {
                source_episode_count_request_label(selection_target_count(
                    subscription.scope.as_ref(),
                    selection,
                ))
            })
        }
        ScopedAddSelectionType::SelectedTargets => source_selected_targets_request_label(
            selection_target_count(subscription.scope.as_ref(), selection),
        ),
        ScopedAddSelectionType::AnimeArc => selection
            .arc_label
            .as_deref()
            .map(str::trim)
            .filter(|label| !label.is_empty())
            .map(|label| format!("Arc: {label} requested"))
            .unwrap_or_else(|| {
                source_episode_count_request_label(selection_target_count(
                    subscription.scope.as_ref(),
                    selection,
                ))
            }),
    })
}

fn find_media_scoped_scope_document(
    subscription: &AcquisitionSubscription,
) -> Option<ScopedAddScopeDocument> {
    if subscription.request_mode != AcquisitionRequestMode::OneShot {
        return None;
    }
    let document: ScopedAddScopeDocument =
        serde_json::from_value(subscription.scope.as_ref()?.clone()).ok()?;
    let document = document.validated().ok()?;
    (document.origin == AcquisitionRequestOrigin::FindMedia).then_some(document)
}

fn selection_target_count(scope: Option<&Value>, selection: &ScopedAddSelection) -> usize {
    json_usize_at(scope, &["selectedTargetCount"])
        .filter(|count| *count > 0)
        .unwrap_or_else(|| selection.target_keys.len())
}

fn source_range_request_label(selection: &ScopedAddSelection) -> Option<String> {
    if let Some(season) = selection.season_number {
        let start = selection.episode_start.or(selection.episode_number)?;
        let end = selection.episode_end.unwrap_or(start);
        return Some(if start == end {
            format!("S{season:02}E{start:02} requested")
        } else {
            format!("S{season:02}E{start:02}-S{season:02}E{end:02} requested")
        });
    }

    let start = selection
        .absolute_episode_start
        .or(selection.absolute_episode_number)?;
    let end = selection.absolute_episode_end.unwrap_or(start);
    Some(if start == end {
        format!("Episode {start} requested")
    } else {
        format!("Episodes {start}-{end} requested")
    })
}

fn source_selected_targets_request_label(count: usize) -> String {
    match count {
        0 => "Selected episodes requested".to_string(),
        1 => "1 selected episode requested".to_string(),
        _ => format!("{count} selected episodes requested"),
    }
}

fn source_episode_count_request_label(count: usize) -> String {
    match count {
        0 => "Episodes requested".to_string(),
        1 => "1 episode requested".to_string(),
        _ => format!("{count} episodes requested"),
    }
}

fn source_target_key_display(key: &str) -> String {
    let normalized = key.trim().to_ascii_uppercase();
    if normalized == "MOVIE" {
        return "Movie".to_string();
    }
    if let Some((season, episode)) = parse_source_season_episode_key(&normalized) {
        return format!("S{season:02}E{episode:02}");
    }
    if let Some(absolute) = normalized
        .strip_prefix('A')
        .and_then(|value| value.parse::<i32>().ok())
    {
        return format!("Episode {absolute}");
    }
    normalized
}

fn parse_source_season_episode_key(value: &str) -> Option<(i32, i32)> {
    let rest = value.strip_prefix('S')?;
    let (season, episode) = rest.split_once('E')?;
    let season = season.parse::<i32>().ok()?;
    let episode = episode.parse::<i32>().ok()?;
    Some((season, episode))
}

fn source_acquisition_media_item_id(
    subscription: &AcquisitionSubscription,
    targets: &[AcquisitionTarget],
) -> Option<String> {
    json_string_at(subscription.scope.as_ref(), &["mediaItemId"])
        .or_else(|| json_string_at(subscription.scope.as_ref(), &["media_item_id"]))
        .or_else(|| {
            targets.iter().find_map(|target| {
                json_string_at(target.metadata.as_ref(), &["mediaItemId"])
                    .or_else(|| json_string_at(target.metadata.as_ref(), &["media_item_id"]))
                    .or_else(|| {
                        json_string_at(target.metadata.as_ref(), &["scopeMetadata", "mediaItemId"])
                    })
                    .or_else(|| {
                        json_string_at(
                            target.metadata.as_ref(),
                            &["scopeMetadata", "media_item_id"],
                        )
                    })
            })
        })
        .filter(|value| !value.trim().is_empty())
}

fn json_string_at(value: Option<&Value>, path: &[&str]) -> Option<String> {
    let mut current = value?;
    for key in path {
        current = current.get(*key)?;
    }
    current
        .as_str()
        .map(str::trim)
        .filter(|value: &&str| !value.is_empty())
        .map(str::to_string)
}

fn json_usize_at(value: Option<&Value>, path: &[&str]) -> Option<usize> {
    let mut current = value?;
    for key in path {
        current = current.get(*key)?;
    }
    current
        .as_u64()
        .and_then(|value| usize::try_from(value).ok())
}

fn build_empty_source_acquisition_item(
    subscription: &AcquisitionSubscription,
    manager_provider_id: Uuid,
    source_label: String,
    ux_context: &AcquisitionUxContext,
) -> FindMediaAcquisitionItem {
    let blocker = empty_source_acquisition_blocker(subscription);
    let phase = if blocker.is_some() {
        AcquisitionPhase::NeedsAttention
    } else {
        AcquisitionPhase::Requested
    };
    let debrid_account_missing =
        ux_context.debrid_account_missing && route_policy_allows_debrid(subscription.route_policy);
    let blocker =
        blocker.or_else(|| debrid_account_missing.then(build_missing_debrid_account_blocker));
    let mut actions = Vec::new();
    if let Some(media_item_id) = source_acquisition_media_item_id(subscription, &[]) {
        actions.push(build_open_show_action(&media_item_id));
    }
    if debrid_account_missing {
        actions.push(build_add_debrid_account_action());
    }
    actions.push(build_remove_acquisition_request_action(
        subscription.subscription_id,
    ));
    let headline = if phase == AcquisitionPhase::NeedsAttention {
        "Metadata target expansion needs attention.".to_string()
    } else {
        "Resolving episodes before source search.".to_string()
    };
    let detail = if subscription.last_metadata_refresh_at.is_some() {
        "Elixir has not created any acquisition targets for this request yet.".to_string()
    } else {
        "Elixir accepted the request and is waiting for the metadata expansion pass.".to_string()
    };
    let mut evidence = vec![
        acquisition_evidence("Source", source_label.clone(), Some("neutral")),
        acquisition_evidence("Targets", "0".to_string(), Some("warning")),
        acquisition_evidence(
            "Metadata",
            if subscription.last_metadata_refresh_at.is_some() {
                "No targets created".to_string()
            } else {
                "Waiting".to_string()
            },
            Some(if phase == AcquisitionPhase::NeedsAttention {
                "warning"
            } else {
                "neutral"
            }),
        ),
    ];
    if debrid_account_missing {
        evidence.push(acquisition_evidence(
            "Debrid account",
            "Add debrid account",
            Some("warning"),
        ));
    }

    FindMediaAcquisitionItem {
        intent_id: subscription.subscription_id,
        title: subscription.title.clone(),
        media_type: media_type_api_name(subscription.media_type).to_string(),
        year: subscription.year,
        external_ids: subscription.external_ids.clone(),
        manager_provider_id,
        manager_label: source_label,
        manager_item_id: Some(subscription.subscription_id.to_string()),
        media_item_id: source_acquisition_media_item_id(subscription, &[]),
        source: SOURCE_ACQUISITION_INTENT_SOURCE.to_string(),
        request_mode: subscription.request_mode.as_str().to_string(),
        request_scope: subscription.request_scope.as_str().to_string(),
        request_label: source_acquisition_request_label(subscription),
        one_shot: subscription.request_mode.is_one_shot(),
        phase: phase.as_str().to_string(),
        phase_label: phase.label().to_string(),
        headline: headline.clone(),
        detail: Some(detail.clone()),
        target_count: 0,
        displayed_child_count: 0,
        hidden_child_count: 0,
        blocker,
        evidence,
        actions,
        stage: phase.legacy_stage().to_string(),
        stage_label: phase.legacy_stage_label().to_string(),
        description: detail,
        progress_percent: None,
        eta_seconds: None,
        downloader_label: None,
        protocol: None,
        last_matched_at: None,
        created_at: subscription.created_at.clone(),
        updated_at: subscription
            .last_metadata_refresh_at
            .clone()
            .unwrap_or_else(|| subscription.updated_at.clone()),
        children: Vec::new(),
    }
}

fn empty_source_acquisition_blocker(
    subscription: &AcquisitionSubscription,
) -> Option<FindMediaAcquisitionBlocker> {
    subscription.last_metadata_refresh_at.as_ref()?;
    if subscription.media_type == MediaType::Series {
        return Some(FindMediaAcquisitionBlocker {
            code: "metadata_tvdb_targets_missing".to_string(),
            title: "TV series metadata did not produce episodes".to_string(),
            detail: "The running server accepted the acquisition request, but it has not expanded this TV series into episode targets. Check TVDB metadata configuration and external IDs, then retry or wait for metadata refresh.".to_string(),
            severity: "warning".to_string(),
        });
    }
    Some(FindMediaAcquisitionBlocker {
        code: "metadata_targets_missing".to_string(),
        title: "No acquisition targets were created".to_string(),
        detail: "The metadata refresh completed without creating movie, episode, or anime targets. Check the metadata provider configuration and external IDs for this item.".to_string(),
        severity: "warning".to_string(),
    })
}

fn build_source_acquisition_child(
    target: &AcquisitionTarget,
    subscription_source_provider_id: Option<Uuid>,
    provider_map: &HashMap<Uuid, ProviderContext>,
    progress: Option<&AcquisitionDownloaderProgress>,
    release_runtime: Option<&SourceTargetReleaseRuntime>,
) -> FindMediaAcquisitionChildItem {
    let phase = source_target_phase(target, progress, release_runtime);
    let blocker = source_target_blocker(target, progress, release_runtime, phase);
    let selected_title = selected_candidate_title(target)
        .or_else(|| release_runtime.map(|runtime| runtime.release_title.clone()));
    let title = source_target_title(target, selected_title.as_deref(), progress);
    let subtitle = source_target_subtitle(target, selected_title.as_deref());
    let route = target.selected_route_logical_id.as_deref().or_else(|| {
        release_runtime.and_then(|runtime| runtime.selected_route_logical_id.as_deref())
    });
    let route_logical_id = route.map(str::to_string);
    let downloader_label = source_route_downloader_label(route);
    let protocol = source_route_protocol(route);
    let source_provider_id =
        source_provider_id_for_target(subscription_source_provider_id, target, release_runtime);
    let route_provider_id = route_provider_id_for_target(target, release_runtime);
    let source_provider_label = source_provider_id
        .and_then(|provider_id| provider_map.get(&provider_id).map(provider_label));
    let route_provider_label = route_provider_id
        .and_then(|provider_id| provider_map.get(&provider_id).map(provider_label));
    let no_results = source_target_is_no_results(target);
    let status = if no_results {
        Some("no_results".to_string())
    } else if target.state == AcquisitionTargetState::Imported {
        Some(target.state.as_str().to_string())
    } else {
        progress
            .and_then(|item| item.status.clone())
            .or_else(|| release_runtime.and_then(SourceTargetReleaseRuntime::status_text))
            .or_else(|| Some(target.state.as_str().to_string()))
    };

    FindMediaAcquisitionChildItem {
        id: target.target_id.to_string(),
        title,
        release_id: release_runtime.map(|runtime| runtime.release_id),
        source_provider_id,
        source_provider_label,
        route_provider_id,
        route_provider_label,
        route_logical_id,
        subtitle,
        download_id: target
            .download_id
            .clone()
            .or_else(|| release_runtime.and_then(|runtime| runtime.download_id.clone())),
        status,
        category: progress.and_then(|item| item.category.clone()),
        phase: phase.as_str().to_string(),
        phase_label: if no_results {
            "No results".to_string()
        } else {
            phase.label().to_string()
        },
        headline: source_target_headline(
            target,
            release_runtime,
            phase,
            downloader_label.as_deref(),
        ),
        detail: source_target_detail(target, release_runtime, phase, selected_title.as_deref()),
        blocker,
        progress_percent: source_target_progress_percent(target, phase, progress),
        eta_seconds: progress.and_then(|item| item.eta_seconds),
        downloader_label,
        protocol,
        size_bytes: progress.and_then(|item| item.size_bytes),
        downloaded_bytes: progress.and_then(|item| item.downloaded_bytes),
        remaining_bytes: progress.and_then(|item| item.remaining_bytes),
        download_rate_bps: progress.and_then(|item| item.download_rate_bps),
        upload_rate_bps: progress.and_then(|item| item.upload_rate_bps),
        connected_seeds: progress.and_then(|item| item.connected_seeds),
        connected_peers: progress.and_then(|item| item.connected_peers),
        known_seeds: progress.and_then(|item| item.known_seeds),
        known_peers: progress.and_then(|item| item.known_peers),
        availability: progress.and_then(|item| item.availability),
        seen_complete_at: progress.and_then(|item| item.seen_complete_at),
    }
}

fn source_target_is_no_results(target: &AcquisitionTarget) -> bool {
    let Some(reason) = target
        .state_reason
        .as_deref()
        .map(str::trim)
        .filter(|reason| !reason.is_empty())
    else {
        return target.state == AcquisitionTargetState::Excluded;
    };
    let normalized = reason.to_ascii_lowercase();
    normalized.contains("no results")
        || normalized.contains("no safe candidate")
        || normalized.contains("no matching acquisition candidates")
        || normalized.contains("search exhausted")
        || normalized.contains("no acquisition candidates")
}

fn source_provider_id_for_subscription(
    subscription: &AcquisitionSubscription,
    targets: &[AcquisitionTarget],
) -> Option<Uuid> {
    subscription.source_provider_id.or_else(|| {
        targets
            .iter()
            .find_map(|target| selected_candidate_source_provider_id(target))
            .or_else(|| {
                targets
                    .iter()
                    .find_map(|target| target.selected_provider_id)
            })
    })
}

fn source_provider_id_for_target(
    subscription_source_provider_id: Option<Uuid>,
    target: &AcquisitionTarget,
    release_runtime: Option<&SourceTargetReleaseRuntime>,
) -> Option<Uuid> {
    subscription_source_provider_id
        .or_else(|| release_runtime.and_then(|runtime| runtime.source_provider_id))
        .or_else(|| selected_candidate_source_provider_id(target))
        .or(target.selected_provider_id)
}

fn route_provider_id_for_target(
    target: &AcquisitionTarget,
    release_runtime: Option<&SourceTargetReleaseRuntime>,
) -> Option<Uuid> {
    release_runtime
        .and_then(|runtime| runtime.route_provider_id)
        .or_else(|| selected_candidate_route_provider_id(target))
}

fn selected_candidate_source_provider_id(target: &AcquisitionTarget) -> Option<Uuid> {
    selected_candidate_uuid_at(target.selected_candidate.as_ref(), &["sourceProviderId"])
}

fn selected_candidate_route_provider_id(target: &AcquisitionTarget) -> Option<Uuid> {
    selected_candidate_uuid_at(
        target.selected_candidate.as_ref(),
        &["submissionResult", "routeProviderId"],
    )
    .or_else(|| {
        selected_candidate_uuid_at(target.selected_candidate.as_ref(), &["routeProviderId"])
    })
}

fn selected_candidate_uuid_at(candidate: Option<&Value>, path: &[&str]) -> Option<Uuid> {
    let mut value = candidate?;
    for key in path {
        value = value.get(*key)?;
    }
    value
        .as_str()
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .and_then(|text| Uuid::parse_str(text).ok())
}

fn select_source_import_file(
    state: &AppState,
    subscription: &AcquisitionSubscription,
    target: &AcquisitionTarget,
    progress: &AcquisitionDownloaderProgress,
) -> SourceImportSelection {
    let Some(raw_path) = progress
        .local_path
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return SourceImportSelection::Pending;
    };
    let path = resolve_download_visible_path(state, raw_path);
    select_source_import_file_from_visible_path(subscription, target, &path)
}

fn select_source_import_file_from_visible_path(
    subscription: &AcquisitionSubscription,
    target: &AcquisitionTarget,
    path: &str,
) -> SourceImportSelection {
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(_) => return SourceImportSelection::Pending,
    };
    if metadata.is_file() {
        if !is_video_file_path(StdPath::new(path)) {
            return SourceImportSelection::Pending;
        }
        return SourceImportSelection::Ready(managed_import_file_for_target(
            target,
            path,
            Some(metadata.len()),
        ));
    }
    if !metadata.is_dir() {
        return SourceImportSelection::Pending;
    }

    let mut files = collect_video_files(StdPath::new(path), 500);
    if files.is_empty() {
        return SourceImportSelection::Pending;
    }
    files.sort_by(|left, right| right.1.cmp(&left.1));

    if target.media_type == MediaType::Movie {
        let (path, size) = files.remove(0);
        return SourceImportSelection::Ready(managed_import_file_for_target(
            target,
            &path.to_string_lossy(),
            Some(size),
        ));
    }

    let hints = source_target_file_hints(subscription, target);
    let mut matches = files
        .iter()
        .filter(|(path, _)| video_path_matches_hints(path, &hints))
        .cloned()
        .collect::<Vec<_>>();
    matches.sort_by(|left, right| right.1.cmp(&left.1));
    if matches.len() == 1 {
        let (path, size) = matches.remove(0);
        return SourceImportSelection::Ready(managed_import_file_for_target(
            target,
            &path.to_string_lossy(),
            Some(size),
        ));
    }
    if files.len() == 1 {
        let (path, size) = files.remove(0);
        return SourceImportSelection::Ready(managed_import_file_for_target(
            target,
            &path.to_string_lossy(),
            Some(size),
        ));
    }

    SourceImportSelection::NeedsFileSelection(SOURCE_ACQUISITION_FILE_SELECTION_REASON.to_string())
}

fn resolve_download_visible_path(state: &AppState, path: &str) -> String {
    let path = path.trim();
    let raw = StdPath::new(path);
    let downloads = StdPath::new("/downloads");
    if let Ok(relative) = raw.strip_prefix(downloads) {
        let runtime_paths = RuntimePaths::from_roots(
            &state.settings.extensions.storage_root,
            &state.settings.library.local_root,
        );
        return StdPath::new(&runtime_paths.downloads_root)
            .join(relative)
            .to_string_lossy()
            .to_string();
    }
    path.to_string()
}

fn collect_video_files(root: &StdPath, max_files: usize) -> Vec<(std::path::PathBuf, u64)> {
    let mut out = Vec::new();
    collect_video_files_inner(root, max_files, &mut out);
    out
}

fn collect_video_files_inner(
    root: &StdPath,
    max_files: usize,
    out: &mut Vec<(std::path::PathBuf, u64)>,
) {
    if out.len() >= max_files {
        return;
    }
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        if out.len() >= max_files {
            break;
        }
        let path = entry.path();
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        if metadata.is_dir() {
            collect_video_files_inner(&path, max_files, out);
        } else if metadata.is_file() && is_video_file_path(&path) {
            out.push((path, metadata.len()));
        }
    }
}

fn is_video_file_path(path: &StdPath) -> bool {
    let Some(extension) = path.extension().and_then(|value| value.to_str()) else {
        return false;
    };
    matches!(
        extension.to_ascii_lowercase().as_str(),
        "mkv" | "mp4" | "m4v" | "avi" | "mov" | "wmv" | "ts" | "m2ts" | "webm"
    )
}

fn source_target_file_hints(
    subscription: &AcquisitionSubscription,
    target: &AcquisitionTarget,
) -> Vec<String> {
    let mut hints = Vec::new();
    if let (Some(season), Some(episode)) = (target.season_number, target.episode_number) {
        hints.push(normalize_file_hint(&format!("S{season:02}E{episode:02}")));
        hints.push(normalize_file_hint(&format!("{season}x{episode:02}")));
    }
    if let Some(value) = candidate_file_hint(target) {
        hints.push(normalize_file_hint(&value));
    }
    if let Some(title) = selected_candidate_title(target) {
        hints.push(normalize_file_hint(&title));
    }
    if target.title != subscription.title {
        hints.push(normalize_file_hint(&target.title));
    }
    hints.into_iter().filter(|value| value.len() >= 4).fold(
        Vec::<String>::new(),
        |mut acc, value| {
            if !acc.contains(&value) {
                acc.push(value);
            }
            acc
        },
    )
}

fn candidate_file_hint(target: &AcquisitionTarget) -> Option<String> {
    let candidate = target.selected_candidate.as_ref()?;
    [
        "/raw/stream/behaviorHints/filename",
        "/raw/stream/filename",
        "/raw/stream/fileName",
        "/raw/fileName",
        "/raw/filePath",
        "/fileName",
        "/filePath",
        "/path",
    ]
    .into_iter()
    .find_map(|pointer| {
        candidate
            .pointer(pointer)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    })
}

fn video_path_matches_hints(path: &StdPath, hints: &[String]) -> bool {
    if hints.is_empty() {
        return false;
    }
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    let normalized = normalize_file_hint(file_name);
    hints
        .iter()
        .any(|hint| !hint.is_empty() && (normalized.contains(hint) || hint.contains(&normalized)))
}

fn normalize_file_hint(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect()
}

fn managed_import_file_for_target(
    target: &AcquisitionTarget,
    path: &str,
    size_bytes: Option<u64>,
) -> ManagedImportFile {
    ManagedImportFile {
        path: path.to_string(),
        season_number: target.season_number,
        episode_number: target.episode_number,
        absolute_episode_number: target.absolute_episode_number,
        episode_title: Some(target.title.clone()).filter(|value| !value.trim().is_empty()),
        size_bytes: size_bytes.and_then(|value| i64::try_from(value).ok()),
        container: StdPath::new(path)
            .extension()
            .map(|value| value.to_string_lossy().to_string()),
        video_codec: None,
        audio_codec: None,
    }
}

fn downloader_progress_is_completed(progress: &AcquisitionDownloaderProgress) -> bool {
    progress
        .progress_percent
        .map(|value| value >= 99.5)
        .unwrap_or(false)
        || progress.remaining_bytes == Some(0)
        || progress
            .status
            .as_deref()
            .map(|value| {
                let status = value.trim().to_ascii_lowercase();
                matches!(
                    status.as_str(),
                    "completed"
                        | "debrid_downloaded"
                        | "rd_downloaded"
                        | "uploading"
                        | "stalledup"
                        | "forcedup"
                        | "pausedup"
                        | "queuedup"
                ) || status.contains("completed")
            })
            .unwrap_or(false)
}

fn source_target_phase(
    target: &AcquisitionTarget,
    progress: Option<&AcquisitionDownloaderProgress>,
    release_runtime: Option<&SourceTargetReleaseRuntime>,
) -> AcquisitionPhase {
    match target.state {
        AcquisitionTargetState::Imported => return AcquisitionPhase::Completed,
        AcquisitionTargetState::Blocked => return AcquisitionPhase::NeedsAttention,
        AcquisitionTargetState::Searching => return AcquisitionPhase::FindingAnotherRelease,
        AcquisitionTargetState::Excluded => return AcquisitionPhase::Completed,
        AcquisitionTargetState::Pending | AcquisitionTargetState::Submitted => {}
    }

    if target.state == AcquisitionTargetState::Pending
        && target
            .state_reason
            .as_deref()
            .is_some_and(source_reason_finding_next_release)
    {
        return AcquisitionPhase::FindingAnotherRelease;
    }
    if target.import_event_id.is_some()
        && target
            .state_reason
            .as_deref()
            .is_some_and(|reason| reason == SOURCE_ACQUISITION_WAITING_FOR_FILE_REASON)
    {
        return AcquisitionPhase::Importing;
    }

    if let Some(phase) = release_runtime
        .and_then(source_release_runtime_phase)
        .filter(|phase| *phase == AcquisitionPhase::Completed)
    {
        return phase;
    }
    if let Some(phase) = release_runtime.and_then(source_release_runtime_hard_attention_phase) {
        return phase;
    }
    if let Some(progress_phase) = progress.and_then(source_downloader_progress_phase) {
        return progress_phase;
    }
    if let Some(phase) = release_runtime
        .and_then(source_release_runtime_phase)
        .filter(|phase| phase.is_route_work())
    {
        return phase;
    }

    if target
        .state_reason
        .as_deref()
        .is_some_and(source_reason_needs_attention)
    {
        return AcquisitionPhase::NeedsAttention;
    }
    if let Some(phase) = release_runtime.and_then(source_release_runtime_attention_phase) {
        return phase;
    }
    if let Some(phase) = release_runtime.and_then(source_release_runtime_phase) {
        return phase;
    }
    if target.state == AcquisitionTargetState::Pending {
        return AcquisitionPhase::Requested;
    }
    if progress.is_none() {
        return AcquisitionPhase::QueuedInDownloader;
    }
    AcquisitionPhase::QueuedInDownloader
}

fn source_release_runtime_attention_phase(
    runtime: &SourceTargetReleaseRuntime,
) -> Option<AcquisitionPhase> {
    if matches!(
        runtime.import_state,
        Some(AcquisitionImportRunState::Blocked | AcquisitionImportRunState::Mismatched)
    ) {
        return Some(AcquisitionPhase::Quarantined);
    }
    if matches!(
        runtime.import_state,
        Some(AcquisitionImportRunState::Failed | AcquisitionImportRunState::Cancelled)
    ) {
        return Some(AcquisitionPhase::Failed);
    }
    if matches!(
        runtime.coverage_state,
        Some(ReleaseCoverageState::ReviewRequired | ReleaseCoverageState::Rejected)
    ) || runtime.release_state == AcquisitionReleaseState::ReviewRequired
    {
        return Some(AcquisitionPhase::ReviewRequired);
    }
    if matches!(
        runtime.job_state,
        Some(ReleaseJobState::Failed | ReleaseJobState::Cancelled)
    ) || matches!(
        runtime.release_state,
        AcquisitionReleaseState::Failed | AcquisitionReleaseState::Cancelled
    ) {
        return Some(AcquisitionPhase::Failed);
    }
    None
}

fn source_release_runtime_hard_attention_phase(
    runtime: &SourceTargetReleaseRuntime,
) -> Option<AcquisitionPhase> {
    if matches!(
        runtime.import_state,
        Some(AcquisitionImportRunState::Blocked | AcquisitionImportRunState::Mismatched)
    ) {
        return Some(AcquisitionPhase::Quarantined);
    }
    if matches!(
        runtime.import_state,
        Some(AcquisitionImportRunState::Failed | AcquisitionImportRunState::Cancelled)
    ) {
        return Some(AcquisitionPhase::Failed);
    }
    if matches!(
        runtime.job_state,
        Some(ReleaseJobState::Failed | ReleaseJobState::Cancelled)
    ) || matches!(
        runtime.release_state,
        AcquisitionReleaseState::Failed | AcquisitionReleaseState::Cancelled
    ) {
        return Some(AcquisitionPhase::Failed);
    }
    None
}

fn source_downloader_progress_phase(
    progress: &AcquisitionDownloaderProgress,
) -> Option<AcquisitionPhase> {
    if progress.issue.is_some() {
        return Some(AcquisitionPhase::NeedsAttention);
    }
    let status = progress
        .status
        .as_deref()
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_default();
    if status.contains("materializing") {
        return Some(AcquisitionPhase::Materializing);
    }
    if downloader_progress_is_completed(progress) {
        return Some(AcquisitionPhase::PostProcessing);
    }
    if progress
        .progress_percent
        .map(|value| value > 0.0)
        .unwrap_or(false)
        || progress.download_rate_bps.unwrap_or(0) > 0
    {
        return Some(AcquisitionPhase::Downloading);
    }
    None
}

fn source_target_progress_percent(
    target: &AcquisitionTarget,
    phase: AcquisitionPhase,
    progress: Option<&AcquisitionDownloaderProgress>,
) -> Option<f64> {
    if target.state == AcquisitionTargetState::Imported || phase == AcquisitionPhase::Completed {
        return Some(100.0);
    }

    let live_progress = progress.and_then(|item| {
        item.progress_percent
            .and_then(normalize_progress_percent)
            .or_else(|| source_downloader_progress_percent_from_bytes(item))
    });

    match phase {
        AcquisitionPhase::Downloading | AcquisitionPhase::Materializing => {
            Some(live_progress.unwrap_or(0.0))
        }
        AcquisitionPhase::PostProcessing | AcquisitionPhase::Importing => {
            Some(live_progress.unwrap_or(100.0))
        }
        _ => live_progress,
    }
}

fn normalize_progress_percent(value: f64) -> Option<f64> {
    value.is_finite().then(|| value.clamp(0.0, 100.0))
}

fn source_downloader_progress_percent_from_bytes(
    progress: &AcquisitionDownloaderProgress,
) -> Option<f64> {
    let size = progress.size_bytes?;
    if size == 0 {
        return None;
    }
    let downloaded = progress.downloaded_bytes.or_else(|| {
        progress
            .remaining_bytes
            .map(|remaining| size.saturating_sub(remaining))
    })?;
    normalize_progress_percent(downloaded as f64 * 100.0 / size as f64)
}

fn source_release_runtime_phase(runtime: &SourceTargetReleaseRuntime) -> Option<AcquisitionPhase> {
    if let Some(state) = runtime.import_state {
        return match state {
            AcquisitionImportRunState::Pending | AcquisitionImportRunState::Importing => {
                Some(AcquisitionPhase::Importing)
            }
            AcquisitionImportRunState::Imported => Some(AcquisitionPhase::Completed),
            AcquisitionImportRunState::Blocked | AcquisitionImportRunState::Mismatched => {
                Some(AcquisitionPhase::Quarantined)
            }
            AcquisitionImportRunState::Failed | AcquisitionImportRunState::Cancelled => {
                Some(AcquisitionPhase::Failed)
            }
        };
    }
    if runtime.coverage_state == Some(ReleaseCoverageState::Imported) {
        return Some(AcquisitionPhase::Completed);
    }
    if let Some(state) = runtime.job_state {
        return Some(match state {
            ReleaseJobState::Staging | ReleaseJobState::Ready => AcquisitionPhase::Staged,
            ReleaseJobState::Submitted => AcquisitionPhase::Submitted,
            ReleaseJobState::Downloading => AcquisitionPhase::Downloading,
            ReleaseJobState::Materializing => AcquisitionPhase::Materializing,
            ReleaseJobState::Completed => AcquisitionPhase::Importing,
            ReleaseJobState::Failed | ReleaseJobState::Cancelled => AcquisitionPhase::Failed,
        });
    }
    Some(match runtime.release_state {
        AcquisitionReleaseState::Candidate
        | AcquisitionReleaseState::Planned
        | AcquisitionReleaseState::Staging
        | AcquisitionReleaseState::Ready => AcquisitionPhase::Staged,
        AcquisitionReleaseState::Submitted => AcquisitionPhase::Submitted,
        AcquisitionReleaseState::Downloading => AcquisitionPhase::Downloading,
        AcquisitionReleaseState::Materializing => AcquisitionPhase::Materializing,
        AcquisitionReleaseState::Completed => AcquisitionPhase::Importing,
        AcquisitionReleaseState::ReviewRequired => AcquisitionPhase::ReviewRequired,
        AcquisitionReleaseState::Failed | AcquisitionReleaseState::Cancelled => {
            AcquisitionPhase::Failed
        }
    })
}

fn source_reason_needs_attention(reason: &str) -> bool {
    let normalized = reason.trim().to_ascii_lowercase();
    normalized.contains("blocked")
        || normalized.contains("failed")
        || normalized.contains("quarantine")
        || normalized.contains("mismatch")
        || normalized.contains("review required")
        || normalized.contains("token is not configured")
        || normalized.contains("real-debrid api token")
        || normalized.contains(&DEBRID_ACCOUNT_MISSING_MESSAGE.to_ascii_lowercase())
        || normalized.contains(&DEBRID_SERVICE_NOT_CONFIGURED_MESSAGE.to_ascii_lowercase())
        || normalized.contains(&DEBRID_SERVICE_UNAVAILABLE_MESSAGE.to_ascii_lowercase())
        || normalized.contains("protected local acquisition")
        || normalized.contains("download protection")
        || normalized.contains("needs file selection")
        || normalized == SOURCE_ACQUISITION_FILE_SELECTION_REASON.to_ascii_lowercase()
}

fn source_target_blocker(
    target: &AcquisitionTarget,
    progress: Option<&AcquisitionDownloaderProgress>,
    release_runtime: Option<&SourceTargetReleaseRuntime>,
    phase: AcquisitionPhase,
) -> Option<FindMediaAcquisitionBlocker> {
    if !matches!(
        phase,
        AcquisitionPhase::NeedsAttention
            | AcquisitionPhase::ReviewRequired
            | AcquisitionPhase::Quarantined
            | AcquisitionPhase::Failed
    ) {
        return None;
    }
    if let Some(issue) = progress.and_then(|item| item.issue.clone()) {
        return Some(FindMediaAcquisitionBlocker {
            code: issue.code,
            title: issue.title,
            detail: issue.detail,
            severity: "warning".to_string(),
        });
    }
    let raw_detail = release_runtime
        .and_then(SourceTargetReleaseRuntime::state_reason)
        .or_else(|| target.state_reason.clone())
        .unwrap_or_else(|| match phase {
            AcquisitionPhase::ReviewRequired => {
                "Elixir needs review before this release can be imported.".to_string()
            }
            AcquisitionPhase::Quarantined => {
                "Import verification quarantined this release.".to_string()
            }
            AcquisitionPhase::Failed => "This acquisition failed.".to_string(),
            _ => "This acquisition target needs attention.".to_string(),
        });
    let normalized = raw_detail.to_ascii_lowercase();
    let generic_debrid_message = generic_debrid_error_message(&raw_detail);
    let code = if raw_detail == SOURCE_ACQUISITION_FILE_SELECTION_REASON {
        "source_file_selection_required"
    } else if generic_debrid_message == Some(DEBRID_ACCOUNT_MISSING_MESSAGE) {
        "debrid_account_missing"
    } else if generic_debrid_message == Some(DEBRID_SERVICE_NOT_CONFIGURED_MESSAGE) {
        "debrid_service_not_configured"
    } else if generic_debrid_message == Some(DEBRID_SERVICE_UNAVAILABLE_MESSAGE) {
        "debrid_service_unavailable"
    } else if normalized.contains("protected local acquisition is blocked")
        || normalized.contains("protected downloads remain blocked")
        || normalized.contains("download protection")
    {
        "torrent_route_blocked_by_network_protection"
    } else if normalized.contains("protected local acquisition requires") {
        "torrent_route_not_selected_for_protection"
    } else if normalized.contains("qbittorrent could not fetch torrent metadata")
        || normalized.contains("qbittorrent_metadata_timeout")
    {
        "qbittorrent_metadata_timeout"
    } else if normalized.contains("swarm has no complete seeds")
        || normalized.contains("qbittorrent_zero_seed_stall")
        || normalized.contains("no useful seed availability")
    {
        "qbittorrent_zero_seed_stall"
    } else if normalized.contains("torbox accepted this torrent")
        || normalized.contains("no_seeds")
        || normalized.contains("no seeds")
    {
        "debrid_no_seeds"
    } else if normalized.contains("acquisition route blocked")
        || normalized.contains("route/provider status")
    {
        "acquisition_route_unavailable"
    } else if phase == AcquisitionPhase::ReviewRequired {
        "acquisition_review_required"
    } else if phase == AcquisitionPhase::Quarantined {
        "acquisition_import_quarantined"
    } else if normalized.contains("import failed") || phase == AcquisitionPhase::Failed {
        "source_import_failed"
    } else {
        "source_target_blocked"
    };
    let detail = source_user_attention_detail(phase, Some(&raw_detail), release_runtime);
    Some(FindMediaAcquisitionBlocker {
        code: code.to_string(),
        title: match code {
            "source_file_selection_required" => "File selection required".to_string(),
            "debrid_account_missing" => "Add debrid account".to_string(),
            "debrid_service_not_configured" => DEBRID_SERVICE_NOT_CONFIGURED_MESSAGE.to_string(),
            "debrid_service_unavailable" => DEBRID_SERVICE_UNAVAILABLE_MESSAGE.to_string(),
            "torrent_route_blocked_by_network_protection" => {
                "Torrent route blocked by network protection".to_string()
            }
            "torrent_route_not_selected_for_protection" => {
                "Torrent route not selected for protected downloads".to_string()
            }
            "qbittorrent_metadata_timeout" => "Torrent metadata did not resolve".to_string(),
            "qbittorrent_zero_seed_stall" => "Torrent swarm has no complete seeds".to_string(),
            "debrid_no_seeds" => "Debrid release has no seeds".to_string(),
            "acquisition_route_unavailable" => "Acquisition route unavailable".to_string(),
            "acquisition_review_required" => "Review required".to_string(),
            "acquisition_import_quarantined" => "Import quarantined".to_string(),
            "source_import_failed" => "Acquisition failed".to_string(),
            _ => "Acquisition needs attention".to_string(),
        },
        detail,
        severity: "warning".to_string(),
    })
}

fn source_target_title(
    target: &AcquisitionTarget,
    selected_title: Option<&str>,
    progress: Option<&AcquisitionDownloaderProgress>,
) -> String {
    target_episode_label(target)
        .or_else(|| selected_title.map(str::to_string))
        .or_else(|| progress.and_then(|item| item.release_title.clone()))
        .unwrap_or_else(|| target.title.clone())
}

fn source_target_subtitle(
    target: &AcquisitionTarget,
    selected_title: Option<&str>,
) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(title) = selected_title.filter(|value| !value.trim().is_empty()) {
        parts.push(title.trim().to_string());
    }
    if let Some(quality) = selected_candidate_quality(target) {
        parts.push(quality);
    }
    (!parts.is_empty()).then(|| parts.join(" · "))
}

fn source_target_headline(
    target: &AcquisitionTarget,
    release_runtime: Option<&SourceTargetReleaseRuntime>,
    phase: AcquisitionPhase,
    downloader_label: Option<&str>,
) -> String {
    if source_target_is_no_results(target) {
        return "No safe release was found.".to_string();
    }
    match phase {
        AcquisitionPhase::Requested => "Waiting for source search.".to_string(),
        AcquisitionPhase::FindingAnotherRelease => "Trying next release.".to_string(),
        AcquisitionPhase::Staged => "Release staged for routing.".to_string(),
        AcquisitionPhase::Submitted => downloader_label
            .map(|label| format!("Submitted to {label}."))
            .unwrap_or_else(|| "Submitted to acquisition route.".to_string()),
        AcquisitionPhase::QueuedInDownloader => downloader_label
            .map(|label| format!("Queued with {label}."))
            .unwrap_or_else(|| "Queued with downloader.".to_string()),
        AcquisitionPhase::Downloading => downloader_label
            .map(|label| format!("Downloading via {label}."))
            .unwrap_or_else(|| "Download in progress.".to_string()),
        AcquisitionPhase::Materializing => downloader_label
            .map(|label| format!("Materializing via {label}."))
            .unwrap_or_else(|| "Materializing selected files.".to_string()),
        AcquisitionPhase::PostProcessing => "Download finished.".to_string(),
        AcquisitionPhase::Importing => "Importing into Elixir.".to_string(),
        AcquisitionPhase::Completed => "Imported.".to_string(),
        AcquisitionPhase::ReviewRequired => "Review required before import.".to_string(),
        AcquisitionPhase::Quarantined => "Import quarantined after verification.".to_string(),
        AcquisitionPhase::NeedsAttention | AcquisitionPhase::Failed => {
            let raw_reason = target
                .state_reason
                .clone()
                .or_else(|| release_runtime.and_then(SourceTargetReleaseRuntime::state_reason));
            source_user_attention_headline(phase, raw_reason.as_deref(), release_runtime)
                .unwrap_or_else(|| {
                    raw_reason.unwrap_or_else(|| "Acquisition needs attention.".to_string())
                })
        }
        AcquisitionPhase::AcceptedByManager => "Accepted.".to_string(),
    }
}

fn source_target_detail(
    target: &AcquisitionTarget,
    release_runtime: Option<&SourceTargetReleaseRuntime>,
    phase: AcquisitionPhase,
    selected_title: Option<&str>,
) -> Option<String> {
    if source_target_is_no_results(target) {
        return Some(
            target
                .state_reason
                .clone()
                .filter(|reason| !reason.trim().is_empty())
                .unwrap_or_else(|| "Source search finished with no safe candidate.".to_string()),
        );
    }
    if matches!(
        phase,
        AcquisitionPhase::NeedsAttention
            | AcquisitionPhase::ReviewRequired
            | AcquisitionPhase::Quarantined
            | AcquisitionPhase::Failed
    ) {
        let raw_detail = release_runtime
            .and_then(SourceTargetReleaseRuntime::state_reason)
            .or_else(|| target.state_reason.clone());
        return Some(source_user_attention_detail(
            phase,
            raw_detail.as_deref(),
            release_runtime,
        ));
    }
    if phase == AcquisitionPhase::FindingAnotherRelease {
        let raw_detail = target
            .state_reason
            .clone()
            .or_else(|| release_runtime.and_then(SourceTargetReleaseRuntime::state_reason));
        return Some(source_finding_another_release_detail(raw_detail.as_deref()));
    }
    if target.state == AcquisitionTargetState::Imported {
        return Some(
            target
                .state_reason
                .clone()
                .filter(|reason| !reason.trim().is_empty())
                .unwrap_or_else(|| "Imported into the Elixir library.".to_string()),
        );
    }
    if matches!(
        phase,
        AcquisitionPhase::Staged
            | AcquisitionPhase::Submitted
            | AcquisitionPhase::QueuedInDownloader
            | AcquisitionPhase::Downloading
            | AcquisitionPhase::Materializing
            | AcquisitionPhase::PostProcessing
            | AcquisitionPhase::Importing
    ) {
        if let Some(reason) = release_runtime
            .and_then(SourceTargetReleaseRuntime::state_reason)
            .or_else(|| target.state_reason.clone())
            .filter(|reason| !reason.trim().is_empty())
        {
            return Some(reason);
        }
    }
    if let Some(title) = selected_title.filter(|value| !value.trim().is_empty()) {
        return Some(format!("Selected release: {}.", title.trim()));
    }
    release_runtime
        .and_then(SourceTargetReleaseRuntime::state_reason)
        .or_else(|| target.state_reason.clone())
}

fn source_user_attention_detail(
    phase: AcquisitionPhase,
    raw_reason: Option<&str>,
    release_runtime: Option<&SourceTargetReleaseRuntime>,
) -> String {
    if let Some(detail) = raw_reason.and_then(source_safe_failure_detail) {
        return detail;
    }
    if let Some(detail) = raw_reason.and_then(source_debrid_attention_detail) {
        return detail;
    }
    match phase {
        AcquisitionPhase::ReviewRequired => source_review_required_detail(raw_reason),
        AcquisitionPhase::Quarantined => {
            let mismatch_class =
                release_runtime.and_then(|runtime| runtime.import_mismatch_class.as_deref());
            source_quarantine_detail(raw_reason, mismatch_class)
        }
        AcquisitionPhase::Failed => {
            let reason = raw_reason
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or("The acquisition attempt failed.");
            format!(
                "{reason} You can find another release, or retry from review if the downloaded files are still available."
            )
        }
        AcquisitionPhase::NeedsAttention => raw_reason
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| {
                "This acquisition needs attention before it can continue.".to_string()
            }),
        _ => raw_reason
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| "This acquisition target needs attention.".to_string()),
    }
}

fn source_user_attention_headline(
    phase: AcquisitionPhase,
    raw_reason: Option<&str>,
    _release_runtime: Option<&SourceTargetReleaseRuntime>,
) -> Option<String> {
    let raw = raw_reason?.trim();
    if raw.is_empty() {
        return None;
    }
    let normalized = raw.to_ascii_lowercase();
    if source_reason_finding_next_release(raw) {
        return Some("Trying next release.".to_string());
    }
    if normalized.contains("falling back to qbittorrent")
        || normalized.contains("torrent fallback")
        || normalized.contains("qbittorrent fallback")
    {
        return Some("Falling back to qBittorrent.".to_string());
    }
    if normalized.contains("torbox accepted this torrent")
        || normalized.contains("no_seeds")
        || normalized.contains("no seeds")
        || normalized.contains("provider_stalled")
    {
        return Some("Debrid release stalled.".to_string());
    }
    if normalized.contains("qbittorrent could not fetch torrent metadata")
        || normalized.contains("qbittorrent_metadata_timeout")
    {
        return Some("Torrent metadata did not resolve.".to_string());
    }
    if normalized.contains("swarm has no complete seeds")
        || normalized.contains("qbittorrent_zero_seed_stall")
        || normalized.contains("no useful seed availability")
    {
        return Some("Torrent swarm has no complete seeds.".to_string());
    }
    if normalized.contains("acquisition route blocked")
        || normalized.contains("route/provider status")
    {
        return Some("Acquisition route unavailable.".to_string());
    }
    if normalized.contains("candidate automation failed") {
        return Some("Acquisition automation failed.".to_string());
    }
    if phase == AcquisitionPhase::Failed && normalized.contains("debrid could not complete") {
        return Some("Debrid release failed.".to_string());
    }
    None
}

fn source_finding_another_release_detail(raw_reason: Option<&str>) -> String {
    if let Some(detail) = raw_reason.and_then(source_safe_failure_detail) {
        return detail;
    }
    "Elixir is trying the next ranked release.".to_string()
}

fn source_reason_finding_next_release(reason: &str) -> bool {
    let normalized = reason.trim().to_ascii_lowercase();
    normalized.contains("trying the next ranked release")
        || normalized.contains("trying next release")
        || normalized.contains("try the next release")
        || normalized.contains("try another release")
}

fn source_safe_failure_detail(raw_reason: &str) -> Option<String> {
    let cleaned = strip_internal_failure_prefix(raw_reason);
    let normalized = cleaned.to_ascii_lowercase();
    let trying_next = source_reason_finding_next_release(raw_reason);

    if normalized.contains("parsing torbox torrent") {
        return Some(action_suffix(
            "TorBox returned an incomplete torrent response. Elixir kept the provider diagnostics in server logs and release provenance.",
            trying_next,
        ));
    }
    if normalized.contains("torbox accepted this torrent") {
        return Some(action_suffix(cleaned.trim_end_matches('.'), trying_next));
    }
    if normalized.contains("qbittorrent could not fetch torrent metadata")
        || normalized.contains("qbittorrent_metadata_timeout")
    {
        return Some(action_suffix(
            "qBittorrent could not fetch torrent metadata.",
            trying_next,
        ));
    }
    if normalized.contains("qbittorrent found metadata but the swarm has no complete seeds")
        || normalized.contains("qbittorrent_zero_seed_stall")
        || normalized.contains("no useful seed availability")
    {
        return Some(action_suffix(
            "qBittorrent found metadata but the swarm has no complete seeds.",
            trying_next,
        ));
    }
    if normalized.contains("debrid could not complete this release")
        || normalized.contains("the debrid provider accepted this torrent")
        || normalized.contains("the debrid provider rejected this magnet")
    {
        return Some(action_suffix(cleaned.trim_end_matches('.'), trying_next));
    }
    if normalized.contains("acquisition route blocked") {
        return Some(
            "The selected acquisition route is unavailable. Check the route/provider status, then retry or choose another release."
                .to_string(),
        );
    }
    if normalized.contains("candidate automation failed") {
        return Some(
            "Acquisition automation failed while processing this item. Check server logs, then retry the request."
                .to_string(),
        );
    }
    None
}

fn strip_internal_failure_prefix(raw_reason: &str) -> &str {
    let trimmed = raw_reason.trim();
    let normalized = trimmed.to_ascii_lowercase();
    if normalized.starts_with("debrid failure [")
        && let Some((_, rest)) = trimmed.split_once("]:")
    {
        return rest.trim();
    }
    if normalized.starts_with("acquisition route blocked:")
        && let Some((_, rest)) = trimmed.split_once(':')
    {
        return rest.trim();
    }
    trimmed
}

fn action_suffix(base: &str, trying_next: bool) -> String {
    let base = base
        .trim()
        .trim_end_matches('.')
        .replace(" Trying the next ranked release", "")
        .replace(" Trying next release", "")
        .replace(" Try the next release", "");
    let base = base.trim().trim_end_matches('.');
    if trying_next {
        format!("{base}. Trying next release.")
    } else {
        format!(
            "{base}. Elixir will try another release or use qBittorrent fallback when policy allows."
        )
    }
}

fn source_debrid_attention_detail(raw_reason: &str) -> Option<String> {
    match generic_debrid_error_message(raw_reason)? {
        DEBRID_ACCOUNT_MISSING_MESSAGE => {
            Some("Add a debrid account to enable direct HTTPS debrid downloads.".to_string())
        }
        DEBRID_SERVICE_NOT_CONFIGURED_MESSAGE => Some(format!(
            "{DEBRID_SERVICE_NOT_CONFIGURED_MESSAGE}. Add or select a debrid account before using direct HTTPS debrid downloads."
        )),
        DEBRID_SERVICE_UNAVAILABLE_MESSAGE => Some(format!(
            "{DEBRID_SERVICE_UNAVAILABLE_MESSAGE}. Try another configured debrid service or use torrent fallback when policy allows."
        )),
        _ => None,
    }
}

fn source_review_required_detail(raw_reason: Option<&str>) -> String {
    let reason = raw_reason
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or_default();
    let normalized = reason.to_ascii_lowercase();
    let base = if normalized.contains("pack")
        || normalized.contains("coverage")
        || normalized.contains("overfetch")
    {
        "Elixir could not prove exactly which requested episodes this release covers."
    } else if normalized.contains("file selection")
        || normalized.contains("multiple file")
        || reason == SOURCE_ACQUISITION_FILE_SELECTION_REASON
    {
        "Elixir found multiple files and needs the correct file choice before import."
    } else if normalized.contains("mapping")
        || normalized.contains("ambiguous")
        || normalized.contains("confidence")
        || normalized.contains("uncertain")
    {
        "Elixir could not classify this release with enough confidence for automatic import."
    } else {
        "Elixir needs manual review before this release can be imported."
    };
    format!("{base} Review the selection, approve the correct files, or find another release.")
}

fn source_quarantine_detail(raw_reason: Option<&str>, mismatch_class: Option<&str>) -> String {
    let reason = raw_reason
        .or(mismatch_class)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or_default();
    let normalized = reason.to_ascii_lowercase();
    let base = if normalized.contains("missing_release_file_mapping")
        || normalized.contains("file mapping")
    {
        "Import verification could not match the downloaded file to the planned target."
    } else if normalized.contains("missing_release_file") || normalized.contains("missing file") {
        "The selected release file was not present when Elixir tried to import it."
    } else if normalized.contains("missing_local_path") || normalized.contains("local path") {
        "The downloader did not expose a local file path Elixir could import."
    } else if normalized.contains("wrong episode")
        || normalized.contains("identity_mismatch")
        || normalized.contains("hash")
        || normalized.contains("mismatch")
    {
        "Verification found that the downloaded file appears to be a different episode than planned."
    } else {
        "Import verification could not confirm this file belongs to the requested target."
    };
    format!(
        "{base} The file was left quarantined; you can review it, retry import after fixing the issue, or find another release."
    )
}

fn target_episode_label(target: &AcquisitionTarget) -> Option<String> {
    if let (Some(season), Some(episode)) = (target.season_number, target.episode_number) {
        return Some(format!("S{season:02}E{episode:02}"));
    }
    target
        .absolute_episode_number
        .map(|absolute| format!("Episode {absolute}"))
}

fn selected_candidate_title(target: &AcquisitionTarget) -> Option<String> {
    target
        .selected_candidate
        .as_ref()
        .and_then(|candidate| candidate.get("title"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn selected_candidate_quality(target: &AcquisitionTarget) -> Option<String> {
    target
        .selected_candidate
        .as_ref()
        .and_then(|candidate| candidate.get("quality"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn source_route_downloader_label(route: Option<&str>) -> Option<String> {
    match route {
        Some(DEBRID_DEFAULT_LOGICAL_ID) => Some("Direct HTTPS debrid download".to_string()),
        Some(TORRENT_DEFAULT_LOGICAL_ID) => Some("Protected torrent downloader".to_string()),
        Some(_) => Some("Downloader".to_string()),
        None => None,
    }
}

fn source_route_protocol(route: Option<&str>) -> Option<String> {
    match route {
        Some(DEBRID_DEFAULT_LOGICAL_ID) => Some("direct HTTPS debrid download".to_string()),
        Some(TORRENT_DEFAULT_LOGICAL_ID) => {
            Some("torrent via protected downloader egress".to_string())
        }
        Some(value) => Some(value.to_string()),
        None => None,
    }
}

fn route_policy_allows_debrid(policy: AcquisitionRoutePolicy) -> bool {
    matches!(
        policy,
        AcquisitionRoutePolicy::DebridFirst | AcquisitionRoutePolicy::DebridOnly
    )
}

fn build_add_debrid_account_action() -> FindMediaAcquisitionAction {
    FindMediaAcquisitionAction {
        id: ADD_DEBRID_ACCOUNT_ACTION_ID.to_string(),
        label: "Add debrid account".to_string(),
        kind: "primary".to_string(),
        confirm_text: None,
        navigate_extension_id: Some(DEBRID_EXTENSION_ID.to_string()),
        navigate_view: Some("extension_control".to_string()),
        navigate_media_item_id: None,
        release_id: None,
        subscription_id: None,
        retry_mode: None,
        cancel_mode: None,
    }
}

fn build_source_acquisition_actions(
    subscription: &AcquisitionSubscription,
    children: &[FindMediaAcquisitionChildItem],
    counts: SourceAcquisitionCounts,
    media_item_id: Option<&str>,
    debrid_account_missing: bool,
) -> Vec<FindMediaAcquisitionAction> {
    let mut actions = Vec::new();
    if let Some(media_item_id) = media_item_id {
        actions.push(build_open_show_action(media_item_id));
    }
    if debrid_account_missing || children.iter().any(child_needs_debrid_account) {
        actions.push(build_add_debrid_account_action());
    }

    if let Some(child) = children
        .iter()
        .find(|child| acquisition_phase_from_str(&child.phase) == AcquisitionPhase::ReviewRequired)
        && let Some(release_id) = child.release_id
    {
        actions.push(build_release_review_action(
            release_id,
            subscription.subscription_id,
            "Review needed",
        ));
    }

    if let Some(child) = children
        .iter()
        .find(|child| acquisition_phase_from_str(&child.phase) == AcquisitionPhase::Quarantined)
        && let Some(release_id) = child.release_id
    {
        actions.push(build_release_review_action(
            release_id,
            subscription.subscription_id,
            "Review quarantine",
        ));
        actions.push(build_retry_import_action(
            release_id,
            subscription.subscription_id,
        ));
        actions.push(build_find_another_release_for_release_action(
            release_id,
            subscription.subscription_id,
        ));
    }

    if !actions
        .iter()
        .any(|action| action.id == FIND_ANOTHER_RELEASE_ACTION_ID)
        && let Some(child) = children.iter().find(|child| {
            matches!(
                acquisition_phase_from_str(&child.phase),
                AcquisitionPhase::Failed | AcquisitionPhase::NeedsAttention
            )
        })
        && let Some(release_id) = child.release_id
    {
        actions.push(build_find_another_release_for_release_action(
            release_id,
            subscription.subscription_id,
        ));
    }

    if subscription.request_mode.is_one_shot()
        && counts.no_results + counts.failed + counts.needs_attention > 0
        && !children.iter().any(child_has_active_download_work)
    {
        actions.push(build_retry_missing_action(subscription.subscription_id));
    }

    if children.iter().any(child_has_active_download_work) {
        actions.push(build_cancel_acquisition_downloads_action(
            subscription.subscription_id,
        ));
    } else {
        actions.push(build_remove_acquisition_request_action(
            subscription.subscription_id,
        ));
    }

    actions
}

fn child_has_active_download_work(child: &FindMediaAcquisitionChildItem) -> bool {
    matches!(
        acquisition_phase_from_str(&child.phase),
        AcquisitionPhase::Staged
            | AcquisitionPhase::Submitted
            | AcquisitionPhase::QueuedInDownloader
            | AcquisitionPhase::Downloading
            | AcquisitionPhase::Materializing
            | AcquisitionPhase::PostProcessing
            | AcquisitionPhase::Importing
    )
}

fn build_release_review_action(
    release_id: Uuid,
    subscription_id: Uuid,
    label: &str,
) -> FindMediaAcquisitionAction {
    FindMediaAcquisitionAction {
        id: OPEN_REVIEW_ACTION_ID.to_string(),
        label: label.to_string(),
        kind: "primary".to_string(),
        confirm_text: None,
        navigate_extension_id: None,
        navigate_view: Some("acquisition_review".to_string()),
        navigate_media_item_id: None,
        release_id: Some(release_id),
        subscription_id: Some(subscription_id),
        retry_mode: None,
        cancel_mode: None,
    }
}

fn build_open_show_action(media_item_id: &str) -> FindMediaAcquisitionAction {
    FindMediaAcquisitionAction {
        id: OPEN_SHOW_ACTION_ID.to_string(),
        label: "Open show".to_string(),
        kind: "secondary".to_string(),
        confirm_text: None,
        navigate_extension_id: None,
        navigate_view: Some("media_detail".to_string()),
        navigate_media_item_id: Some(media_item_id.to_string()),
        release_id: None,
        subscription_id: None,
        retry_mode: None,
        cancel_mode: None,
    }
}

fn build_retry_missing_action(subscription_id: Uuid) -> FindMediaAcquisitionAction {
    FindMediaAcquisitionAction {
        id: RETRY_MISSING_ACTION_ID.to_string(),
        label: "Retry missing".to_string(),
        kind: "secondary".to_string(),
        confirm_text: Some(
            "Retry missing or failed targets from this acquisition request? Imported files are not touched."
                .to_string(),
        ),
        navigate_extension_id: None,
        navigate_view: None,
        navigate_media_item_id: None,
        release_id: None,
        subscription_id: Some(subscription_id),
        retry_mode: Some("missing_targets".to_string()),
        cancel_mode: None,
    }
}

fn build_retry_import_action(
    release_id: Uuid,
    subscription_id: Uuid,
) -> FindMediaAcquisitionAction {
    FindMediaAcquisitionAction {
        id: RETRY_IMPORT_ACTION_ID.to_string(),
        label: "Retry import".to_string(),
        kind: "secondary".to_string(),
        confirm_text: Some(
            "Retry import for this release without deleting downloader data?".to_string(),
        ),
        navigate_extension_id: None,
        navigate_view: None,
        navigate_media_item_id: None,
        release_id: Some(release_id),
        subscription_id: Some(subscription_id),
        retry_mode: Some("import".to_string()),
        cancel_mode: None,
    }
}

fn build_find_another_release_for_release_action(
    release_id: Uuid,
    subscription_id: Uuid,
) -> FindMediaAcquisitionAction {
    FindMediaAcquisitionAction {
        id: FIND_ANOTHER_RELEASE_ACTION_ID.to_string(),
        label: "Find another release".to_string(),
        kind: "secondary".to_string(),
        confirm_text: Some("Search for another release for the affected target?".to_string()),
        navigate_extension_id: None,
        navigate_view: None,
        navigate_media_item_id: None,
        release_id: Some(release_id),
        subscription_id: Some(subscription_id),
        retry_mode: Some("source_discovery".to_string()),
        cancel_mode: None,
    }
}

fn build_remove_acquisition_request_action(subscription_id: Uuid) -> FindMediaAcquisitionAction {
    FindMediaAcquisitionAction {
        id: REMOVE_ACQUISITION_REQUEST_ACTION_ID.to_string(),
        label: "Remove request".to_string(),
        kind: "secondary".to_string(),
        confirm_text: Some(
            "Remove this acquisition request from the active queue? History is preserved."
                .to_string(),
        ),
        navigate_extension_id: None,
        navigate_view: None,
        navigate_media_item_id: None,
        release_id: None,
        subscription_id: Some(subscription_id),
        retry_mode: None,
        cancel_mode: Some("dismiss".to_string()),
    }
}

fn build_cancel_acquisition_downloads_action(subscription_id: Uuid) -> FindMediaAcquisitionAction {
    FindMediaAcquisitionAction {
        id: CANCEL_ACQUISITION_DOWNLOADS_ACTION_ID.to_string(),
        label: "Cancel download".to_string(),
        kind: "danger".to_string(),
        confirm_text: Some(
            "Cancel active download work and remove this acquisition request? Imported library files are not deleted."
                .to_string(),
        ),
        navigate_extension_id: None,
        navigate_view: None,
        navigate_media_item_id: None,
        release_id: None,
        subscription_id: Some(subscription_id),
        retry_mode: None,
        cancel_mode: Some("cancel_downloads".to_string()),
    }
}

fn build_missing_debrid_account_blocker() -> FindMediaAcquisitionBlocker {
    FindMediaAcquisitionBlocker {
        code: "debrid_account_missing".to_string(),
        title: "Add debrid account".to_string(),
        detail: "Add a debrid account to enable direct HTTPS debrid downloads. Torrent fallback only runs when the protected torrent route is allowed."
            .to_string(),
        severity: "warning".to_string(),
    }
}

fn child_needs_debrid_account(child: &FindMediaAcquisitionChildItem) -> bool {
    child
        .blocker
        .as_ref()
        .is_some_and(|blocker| blocker.code == "debrid_account_missing")
}

fn source_acquisition_counts(
    children: &[FindMediaAcquisitionChildItem],
) -> SourceAcquisitionCounts {
    let mut counts = SourceAcquisitionCounts::default();
    for child in children {
        if child.status.as_deref() == Some("no_results") {
            counts.no_results += 1;
            continue;
        }
        match acquisition_phase_from_str(&child.phase) {
            AcquisitionPhase::Requested => counts.requested += 1,
            AcquisitionPhase::Staged => counts.staged += 1,
            AcquisitionPhase::Submitted => counts.submitted += 1,
            AcquisitionPhase::QueuedInDownloader => counts.queued += 1,
            AcquisitionPhase::Downloading => counts.downloading += 1,
            AcquisitionPhase::Materializing => counts.materializing += 1,
            AcquisitionPhase::PostProcessing => counts.post_processing += 1,
            AcquisitionPhase::Importing => counts.importing += 1,
            AcquisitionPhase::Completed => counts.completed += 1,
            AcquisitionPhase::ReviewRequired => counts.review_required += 1,
            AcquisitionPhase::Quarantined => counts.quarantined += 1,
            AcquisitionPhase::NeedsAttention => counts.needs_attention += 1,
            AcquisitionPhase::Failed => counts.failed += 1,
            AcquisitionPhase::AcceptedByManager | AcquisitionPhase::FindingAnotherRelease => {
                counts.requested += 1
            }
        }
    }
    counts
}

fn source_status_count_evidence(
    counts: SourceAcquisitionCounts,
) -> Vec<FindMediaAcquisitionEvidence> {
    let mut evidence = Vec::new();
    let mut push = |label: &str, value: usize, tone: Option<&str>| {
        if value > 0 {
            evidence.push(acquisition_evidence(label, value.to_string(), tone));
        }
    };
    push("Staged", counts.staged, Some("neutral"));
    push("Submitted", counts.submitted, Some("neutral"));
    push("Queued", counts.queued, Some("neutral"));
    push("Downloading", counts.downloading, Some("success"));
    push("Materializing", counts.materializing, Some("success"));
    push("Importing", counts.importing, Some("success"));
    push("Imported", counts.completed, Some("success"));
    push("No results", counts.no_results, Some("warning"));
    push("Review", counts.review_required, Some("warning"));
    push("Quarantined", counts.quarantined, Some("warning"));
    push(
        "Failed",
        counts.failed + counts.needs_attention,
        Some("warning"),
    );
    evidence
}

fn summarize_source_acquisition_phase(counts: SourceAcquisitionCounts) -> AcquisitionPhase {
    if counts.downloading > 0 {
        AcquisitionPhase::Downloading
    } else if counts.materializing > 0 {
        AcquisitionPhase::Materializing
    } else if counts.post_processing > 0 {
        AcquisitionPhase::PostProcessing
    } else if counts.importing > 0 {
        AcquisitionPhase::Importing
    } else if counts.needs_attention > 0 || counts.failed > 0 {
        AcquisitionPhase::NeedsAttention
    } else if counts.review_required > 0 {
        AcquisitionPhase::ReviewRequired
    } else if counts.quarantined > 0 {
        AcquisitionPhase::Quarantined
    } else if counts.queued > 0 {
        AcquisitionPhase::QueuedInDownloader
    } else if counts.submitted > 0 {
        AcquisitionPhase::Submitted
    } else if counts.staged > 0 {
        AcquisitionPhase::Staged
    } else if counts.requested > 0 {
        AcquisitionPhase::Requested
    } else {
        AcquisitionPhase::Completed
    }
}

fn build_source_acquisition_blocker(
    children: &[FindMediaAcquisitionChildItem],
) -> Option<FindMediaAcquisitionBlocker> {
    children.iter().find_map(|child| child.blocker.clone())
}

fn source_parent_progress(children: &[FindMediaAcquisitionChildItem]) -> Option<f64> {
    if children.is_empty() {
        return None;
    }
    let values = children
        .iter()
        .map(source_child_progress_contribution)
        .collect::<Vec<_>>();
    Some((values.iter().sum::<f64>() / values.len() as f64).clamp(0.0, 100.0))
}

fn source_child_progress_contribution(child: &FindMediaAcquisitionChildItem) -> f64 {
    let phase = acquisition_phase_from_str(&child.phase);
    match phase {
        AcquisitionPhase::Completed => 100.0,
        AcquisitionPhase::PostProcessing | AcquisitionPhase::Importing => child
            .progress_percent
            .and_then(normalize_progress_percent)
            .unwrap_or(100.0),
        AcquisitionPhase::Downloading | AcquisitionPhase::Materializing => child
            .progress_percent
            .and_then(normalize_progress_percent)
            .unwrap_or(0.0),
        _ => child
            .progress_percent
            .and_then(normalize_progress_percent)
            .unwrap_or(0.0),
    }
}

fn source_parent_phase_label(
    phase: AcquisitionPhase,
    counts: SourceAcquisitionCounts,
    total_targets: usize,
) -> String {
    if phase == AcquisitionPhase::Completed
        && total_targets > 0
        && counts.completed == 0
        && counts.no_results >= total_targets
    {
        return "Completed".to_string();
    }
    phase.label().to_string()
}

fn format_source_acquisition_headline(
    counts: SourceAcquisitionCounts,
    total_targets: usize,
) -> String {
    if counts.needs_attention + counts.failed > 0 {
        return format!(
            "{} need attention.",
            format_transfer_count(counts.needs_attention + counts.failed)
        );
    }
    if counts.review_required > 0 {
        return format!(
            "{} need review.",
            format_transfer_count(counts.review_required)
        );
    }
    if counts.quarantined > 0 {
        return format!("{} quarantined.", format_transfer_count(counts.quarantined));
    }
    let mut parts = Vec::new();
    if counts.downloading > 0 {
        parts.push(format!(
            "{} downloading",
            format_transfer_count(counts.downloading)
        ));
    }
    if counts.materializing > 0 {
        parts.push(format!(
            "{} materializing",
            format_transfer_count(counts.materializing)
        ));
    }
    if counts.post_processing > 0 {
        parts.push(format!(
            "{} downloaded",
            format_transfer_count(counts.post_processing)
        ));
    }
    if counts.importing > 0 {
        parts.push(format!(
            "{} importing",
            format_transfer_count(counts.importing)
        ));
    }
    if counts.queued > 0 {
        parts.push(format!("{} queued", format_transfer_count(counts.queued)));
    }
    if counts.submitted > 0 {
        parts.push(format!(
            "{} submitted",
            format_transfer_count(counts.submitted)
        ));
    }
    if counts.staged > 0 {
        parts.push(format!("{} staged", format_transfer_count(counts.staged)));
    }
    if parts.is_empty() && counts.completed + counts.no_results >= total_targets {
        if total_targets > 0 && counts.no_results > 0 {
            format_terminal_source_summary(counts.completed, counts.no_results, total_targets)
        } else if counts.no_results > 0 {
            "No safe results found.".to_string()
        } else {
            "All targets imported.".to_string()
        }
    } else if parts.is_empty() {
        "Waiting for source search.".to_string()
    } else {
        format!("{}.", parts.join(", "))
    }
}

fn format_terminal_source_summary(
    imported: usize,
    no_results: usize,
    total_targets: usize,
) -> String {
    format!(
        "{imported} imported, {no_results} no results out of {total_targets} {}.",
        if total_targets == 1 {
            "target"
        } else {
            "targets"
        }
    )
}

fn format_source_acquisition_detail(
    counts: SourceAcquisitionCounts,
    total_targets: usize,
) -> String {
    let mut parts = Vec::new();
    let mut push = |label: &str, value: usize| {
        if value > 0 {
            parts.push(format!("{value} {label}"));
        }
    };
    push("staged", counts.staged);
    push("submitted", counts.submitted);
    push("queued", counts.queued);
    push("downloading", counts.downloading);
    push("materializing", counts.materializing);
    push("downloaded", counts.post_processing);
    push("importing", counts.importing);
    push("imported", counts.completed);
    push("with no results", counts.no_results);
    push("needing review", counts.review_required);
    push("quarantined", counts.quarantined);
    push("failed", counts.failed + counts.needs_attention);
    if parts.is_empty() {
        return format!("0 of {total_targets} targets imported.");
    }
    format!("{} out of {} targets.", parts.join(", "), total_targets)
}

fn subscription_route_evidence(targets: &[AcquisitionTarget]) -> Option<String> {
    let mut routes = targets
        .iter()
        .filter_map(|target| target.selected_route_logical_id.as_deref())
        .map(source_route_evidence_label)
        .collect::<Vec<_>>();
    routes.sort();
    routes.dedup();
    if routes.is_empty() {
        None
    } else if routes.len() == 1 {
        routes.into_iter().next()
    } else {
        Some(format!("{} routes", routes.len()))
    }
}

fn source_route_provider_evidence(children: &[FindMediaAcquisitionChildItem]) -> Option<String> {
    let mut providers = children
        .iter()
        .filter_map(|child| child.route_provider_label.as_deref())
        .map(str::to_string)
        .collect::<Vec<_>>();
    providers.sort();
    providers.dedup();
    if providers.is_empty() {
        None
    } else if providers.len() == 1 {
        providers.into_iter().next()
    } else {
        Some(format!("{} route providers", providers.len()))
    }
}

fn source_route_evidence_label(route: &str) -> String {
    match route {
        DEBRID_DEFAULT_LOGICAL_ID => "Direct HTTPS debrid download".to_string(),
        TORRENT_DEFAULT_LOGICAL_ID => "Torrent via protected downloader egress".to_string(),
        value => value.to_string(),
    }
}

fn source_target_sort_key(title: &str) -> String {
    title.to_ascii_lowercase()
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
        if !matches!(
            capability,
            "downloader.nzb" | "downloader.torrent" | "debrid.resolver"
        ) {
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
            implementation if is_debrid_service_implementation(Some(implementation)) => {
                load_debrid_acquisition_progress_index(
                    state,
                    store,
                    provider.detail.provider.provider_id,
                    instance_id,
                )
                .await
            }
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

async fn load_debrid_acquisition_progress_index(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider_id: Uuid,
    instance_id: Uuid,
) -> AnyResult<AcquisitionDownloaderProgressIndex> {
    let items = load_debrid_progress(state, store, provider_id, instance_id).await?;
    let mut index = AcquisitionDownloaderProgressIndex::default();
    for item in items {
        let progress_percent = item.progress.map(|value| (value * 100.0).clamp(0.0, 100.0));
        index.insert(
            &item.id,
            AcquisitionDownloaderProgress {
                release_title: item.name,
                status: item.state,
                category: item.category,
                local_path: item.local_path,
                progress_percent,
                eta_seconds: None,
                size_bytes: item.total_bytes,
                downloaded_bytes: item.downloaded_bytes,
                remaining_bytes: item.remaining_bytes,
                download_rate_bps: item.download_rate_bps,
                upload_rate_bps: None,
                connected_seeds: None,
                connected_peers: None,
                known_seeds: None,
                known_peers: None,
                availability: None,
                seen_complete_at: None,
                issue: None,
            },
        );
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
        if detail.provider.health_state == ProviderHealthState::Unhealthy {
            continue;
        }
        if detail.provider.capability == "debrid.resolver" {
            let implementation = detail
                .provider
                .implementation
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty());
            if !is_debrid_service_implementation(implementation) {
                continue;
            }
            let Ok(items) = load_debrid_progress(
                state,
                store,
                detail.provider.provider_id,
                detail.provider.instance_id,
            )
            .await
            else {
                continue;
            };
            for item in items {
                if let Some(rate) = item.download_rate_bps {
                    total_download_rate = total_download_rate.saturating_add(rate);
                    has_download_rate = true;
                }
            }
            continue;
        }
        if detail.provider.capability != "downloader.torrent"
            && detail.provider.capability != "downloader.nzb"
        {
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
        "staged" => AcquisitionPhase::Staged,
        "submitted" => AcquisitionPhase::Submitted,
        "queued_in_downloader" => AcquisitionPhase::QueuedInDownloader,
        "downloading" => AcquisitionPhase::Downloading,
        "materializing" => AcquisitionPhase::Materializing,
        "post_processing" => AcquisitionPhase::PostProcessing,
        "importing" => AcquisitionPhase::Importing,
        "completed" => AcquisitionPhase::Completed,
        "review_required" => AcquisitionPhase::ReviewRequired,
        "quarantined" => AcquisitionPhase::Quarantined,
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
        let mut state = derive_arr_queue_entry_state(entry, queue_entry_count, downloader_progress);
        state.children = vec![build_download_attempt_child(
            entry,
            sonarr_episode_index,
            downloader_progress,
        )];
        return state;
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
        .map(|entry| build_download_attempt_child(entry, sonarr_episode_index, downloader_progress))
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
        "Transfers queued",
        counts.queued.to_string(),
        Some("neutral"),
    ));
    if counts.downloading > 0 {
        evidence.push(acquisition_evidence(
            "Transfers downloading",
            counts.downloading.to_string(),
            Some("success"),
        ));
    }
    if counts.post_processing > 0 {
        evidence.push(acquisition_evidence(
            "Transfers post-processing",
            counts.post_processing.to_string(),
            Some("neutral"),
        ));
    }
    if counts.importing > 0 {
        evidence.push(acquisition_evidence(
            "Transfers importing",
            counts.importing.to_string(),
            Some("neutral"),
        ));
    }
    if counts.needs_attention + counts.failed > 0 {
        evidence.push(acquisition_evidence(
            "Transfers needing attention",
            (counts.needs_attention + counts.failed).to_string(),
            Some("warning"),
        ));
    }
    if stats.episode_count > 0 {
        evidence.push(acquisition_evidence(
            "Files imported",
            format!("{} / {}", stats.episode_file_count, stats.episode_count),
            Some(if stats.episode_file_count > 0 {
                "success"
            } else {
                "neutral"
            }),
        ));
    } else if stats.episode_file_count > 0 {
        evidence.push(acquisition_evidence(
            "Files imported",
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

fn build_download_attempt_child(
    entry: &Value,
    sonarr_episode_index: Option<&HashMap<i64, SonarrEpisodeDescriptor>>,
    downloader_progress: &AcquisitionDownloaderProgressIndex,
) -> FindMediaAcquisitionChildItem {
    let state = derive_arr_queue_entry_state(entry, 1, downloader_progress);
    let download_id = queue_entry_download_id(entry);
    let progress = download_id
        .as_deref()
        .and_then(|download_id| downloader_progress.get(download_id));
    let (title, subtitle) = download_attempt_title(entry, progress, sonarr_episode_index);
    let id = download_id
        .clone()
        .or_else(|| as_id_string(entry.get("episodeId").unwrap_or(&Value::Null)))
        .or_else(|| as_id_string(entry.get("id").unwrap_or(&Value::Null)))
        .unwrap_or_else(|| title.clone());

    FindMediaAcquisitionChildItem {
        id,
        title,
        release_id: None,
        source_provider_id: None,
        source_provider_label: None,
        route_provider_id: None,
        route_provider_label: None,
        route_logical_id: None,
        subtitle,
        download_id,
        status: progress
            .and_then(|item| item.status.clone())
            .or_else(|| Some(queue_entry_state(entry)).filter(|value| !value.trim().is_empty())),
        category: progress.and_then(|item| item.category.clone()),
        phase: state.phase.as_str().to_string(),
        phase_label: state.phase.label().to_string(),
        headline: state.headline,
        detail: state.detail,
        blocker: state.blocker,
        progress_percent: state.progress_percent,
        eta_seconds: state.eta_seconds,
        downloader_label: state.downloader_label,
        protocol: state.protocol,
        size_bytes: progress.and_then(|item| item.size_bytes),
        downloaded_bytes: progress.and_then(|item| item.downloaded_bytes),
        remaining_bytes: progress.and_then(|item| item.remaining_bytes),
        download_rate_bps: progress.and_then(|item| item.download_rate_bps),
        upload_rate_bps: progress.and_then(|item| item.upload_rate_bps),
        connected_seeds: progress.and_then(|item| item.connected_seeds),
        connected_peers: progress.and_then(|item| item.connected_peers),
        known_seeds: progress.and_then(|item| item.known_seeds),
        known_peers: progress.and_then(|item| item.known_peers),
        availability: progress.and_then(|item| item.availability),
        seen_complete_at: progress.and_then(|item| item.seen_complete_at),
    }
}

fn download_attempt_title(
    entry: &Value,
    downloader_progress: Option<&AcquisitionDownloaderProgress>,
    sonarr_episode_index: Option<&HashMap<i64, SonarrEpisodeDescriptor>>,
) -> (String, Option<String>) {
    let release_title = entry
        .get("title")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let title = downloader_progress
        .and_then(|item| item.release_title.clone())
        .or(release_title.clone())
        .unwrap_or_else(|| "Download".to_string());

    let Some(episode_id) = entry.get("episodeId").and_then(Value::as_i64) else {
        return (title, None);
    };
    let Some(episode) = sonarr_episode_index.and_then(|index| index.get(&episode_id)) else {
        return (title, None);
    };

    let code = format!(
        "S{:02}E{:02}",
        episode.season_number, episode.episode_number
    );
    let destination = if episode.title.trim().is_empty() {
        code
    } else {
        format!("{code} • {}", episode.title.trim())
    };
    (title, Some(destination))
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
            format_transfer_count(counts.needs_attention + counts.failed)
        ));
    }
    if counts.downloading > 0 {
        parts.push(format!(
            "{} downloading",
            format_transfer_count(counts.downloading)
        ));
    }
    if counts.post_processing > 0 {
        parts.push(format!(
            "{} post-processing",
            format_transfer_count(counts.post_processing)
        ));
    }
    if counts.importing > 0 {
        parts.push(format!(
            "{} importing",
            format_transfer_count(counts.importing)
        ));
    }
    if counts.queued > 0 {
        parts.push(format!("{} queued", format_transfer_count(counts.queued)));
    }

    if parts.is_empty() {
        "Waiting for download activity.".to_string()
    } else {
        format!("{}.", parts.join(", "))
    }
}

fn format_sonarr_batch_detail(stats: SonarrSeriesStats) -> String {
    if stats.episode_count > 0 {
        format!(
            "Sonarr is handling this series as release downloads. {} of {} episode files are imported so far.",
            stats.episode_file_count, stats.episode_count
        )
    } else if stats.episode_file_count > 0 {
        format!(
            "Sonarr is handling this series as release downloads. {} episode files are already imported.",
            stats.episode_file_count
        )
    } else {
        "Sonarr is handling this series as one or more release downloads.".to_string()
    }
}

fn build_sonarr_batch_blocker(counts: SonarrBatchCounts) -> Option<FindMediaAcquisitionBlocker> {
    let attention = counts.needs_attention + counts.failed;
    (attention > 0).then(|| FindMediaAcquisitionBlocker {
        code: "series_batch_attention".to_string(),
        title: "Downloads need attention".to_string(),
        detail: format!(
            "{} in this series currently need attention.",
            format_transfer_count(attention)
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

fn format_transfer_count(count: usize) -> String {
    if count == 1 {
        "1 transfer".to_string()
    } else {
        format!("{count} transfers")
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
        release_title: torrent
            .name
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string),
        status: torrent
            .state
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string),
        category: torrent
            .category
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string),
        local_path: qbittorrent_local_path(torrent),
        progress_percent,
        eta_seconds: torrent.eta.filter(|value| *value > 0),
        size_bytes: total_size,
        downloaded_bytes: torrent.downloaded,
        remaining_bytes: torrent.amount_left,
        download_rate_bps: torrent.dlspeed,
        upload_rate_bps: torrent.upspeed,
        connected_seeds: torrent.num_seeds,
        connected_peers: torrent.num_leechs,
        known_seeds: torrent.num_complete,
        known_peers: torrent.num_incomplete,
        availability: torrent.availability.filter(|value| *value >= 0.0),
        seen_complete_at: torrent.seen_complete.and_then(timestamp_to_datetime),
        issue,
    })
}

fn qbittorrent_local_path(torrent: &AcquisitionQbittorrentTorrent) -> Option<String> {
    torrent
        .content_path
        .as_deref()
        .or(torrent.save_path.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
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
            title: "qBittorrent is waiting for torrent metadata".to_string(),
            detail: format!(
                "qBittorrent is waiting for torrent metadata. It has had over {} minutes with no connected peers; Elixir will remove it and try the next release. Known swarm: {} seeders, {} peers.",
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
            title: "qBittorrent swarm has no complete seeds".to_string(),
            detail: format!(
                "qBittorrent found metadata but the swarm has no complete seeds. It has had over {} minutes with no progress, no connected peers, and no download speed; Elixir will remove it and try the next release. Known swarm: {} seeders, {} peers.",
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
            release_title: nzbget_group_title(group),
            status: group
                .status
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string),
            category: group
                .category
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string),
            local_path: None,
            progress_percent,
            eta_seconds: None,
            size_bytes: total_size,
            downloaded_bytes: downloaded_size,
            remaining_bytes: remaining_size,
            download_rate_bps: None,
            upload_rate_bps: None,
            connected_seeds: None,
            connected_peers: None,
            known_seeds: None,
            known_peers: None,
            availability: None,
            seen_complete_at: None,
            issue: nzbget_group_issue(group),
        },
    ))
}

fn nzbget_group_title(group: &AcquisitionNzbgetGroup) -> Option<String> {
    group
        .nzb_name
        .as_deref()
        .or(group.nzb_filename.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .strip_suffix(".nzb")
                .or_else(|| value.strip_suffix(".NZB"))
                .unwrap_or(value)
                .to_string()
        })
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
    let source_preferences = load_source_preferences(&store, &providers)
        .await
        .map_err(ApiError::from)?;

    Ok(Json(FindMediaPreferencesResponse {
        preferences: FindMediaPreferencesState {
            tv_default_manager_provider_id: preferences.series_provider_id,
            movies_default_manager_provider_id: preferences.movie_provider_id,
            anime_default_manager_provider_id: preferences.anime_provider_id,
            tv_default_source_provider_id: source_preferences.series_provider_id,
            movies_default_source_provider_id: source_preferences.movie_provider_id,
            anime_default_source_provider_id: source_preferences.anime_provider_id,
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
        tv_source_candidates: collect_source_providers(&providers, MediaType::Series)
            .iter()
            .map(provider_summary)
            .collect(),
        movies_source_candidates: collect_source_providers(&providers, MediaType::Movie)
            .iter()
            .map(provider_summary)
            .collect(),
        anime_source_candidates: collect_source_providers(&providers, MediaType::Anime)
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
    if let Some(value) = parse_provider_patch_value(payload.movies_default_source_provider_id)? {
        validate_source_preference_provider(value, MediaType::Movie, &providers)?;
        save_source_preference(&store, MediaType::Movie, value)
            .await
            .map_err(ApiError::from)?;
    }
    if let Some(value) = parse_provider_patch_value(payload.tv_default_source_provider_id)? {
        validate_source_preference_provider(value, MediaType::Series, &providers)?;
        save_source_preference(&store, MediaType::Series, value)
            .await
            .map_err(ApiError::from)?;
    }
    if let Some(value) = parse_provider_patch_value(payload.anime_default_source_provider_id)? {
        validate_source_preference_provider(value, MediaType::Anime, &providers)?;
        save_source_preference(&store, MediaType::Anime, value)
            .await
            .map_err(ApiError::from)?;
    }

    let preferences = load_manager_preferences(&store, &providers)
        .await
        .map_err(ApiError::from)?;
    let source_preferences = load_source_preferences(&store, &providers)
        .await
        .map_err(ApiError::from)?;

    Ok(Json(FindMediaPreferencesResponse {
        preferences: FindMediaPreferencesState {
            tv_default_manager_provider_id: preferences.series_provider_id,
            movies_default_manager_provider_id: preferences.movie_provider_id,
            anime_default_manager_provider_id: preferences.anime_provider_id,
            tv_default_source_provider_id: source_preferences.series_provider_id,
            movies_default_source_provider_id: source_preferences.movie_provider_id,
            anime_default_source_provider_id: source_preferences.anime_provider_id,
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
        tv_source_candidates: collect_source_providers(&providers, MediaType::Series)
            .iter()
            .map(provider_summary)
            .collect(),
        movies_source_candidates: collect_source_providers(&providers, MediaType::Movie)
            .iter()
            .map(provider_summary)
            .collect(),
        anime_source_candidates: collect_source_providers(&providers, MediaType::Anime)
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

fn preferred_source_for_type(
    preferences: &SourcePreferenceState,
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

fn resolve_default_source(preferred: Option<Uuid>, providers: &[ProviderContext]) -> Option<Uuid> {
    if let Some(preferred) = preferred {
        if providers
            .iter()
            .any(|provider| provider.detail.provider.provider_id == preferred)
        {
            return Some(preferred);
        }
    }
    let mut ordered = providers.to_vec();
    ordered.sort_by(compare_source_candidates);
    ordered
        .first()
        .map(|provider| provider.detail.provider.provider_id)
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

fn collect_source_providers(
    providers: &[ProviderContext],
    media_type: MediaType,
) -> Vec<ProviderContext> {
    let mut out: Vec<_> = providers
        .iter()
        .filter(|provider| {
            provider
                .detail
                .provider
                .capability
                .eq_ignore_ascii_case(ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY)
        })
        .filter(|provider| provider.media_types.contains(&media_type))
        .filter(|provider| provider_supports_action(provider, "search"))
        .filter(|provider| provider.detail.provider.health_state == ProviderHealthState::Healthy)
        .filter(|provider| provider.detail.provider.endpoint_json.is_some())
        .cloned()
        .collect();
    out.sort_by(compare_source_candidates);
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

fn compare_source_candidates(
    left: &ProviderContext,
    right: &ProviderContext,
) -> std::cmp::Ordering {
    let by_trust = trust_rank(left.detail.trust_level).cmp(&trust_rank(right.detail.trust_level));
    if by_trust != std::cmp::Ordering::Equal {
        return by_trust;
    }
    let by_extension = left.detail.extension_id.cmp(&right.detail.extension_id);
    if by_extension == std::cmp::Ordering::Equal {
        let by_instance = left.instance_name.cmp(&right.instance_name);
        if by_instance == std::cmp::Ordering::Equal {
            left.detail
                .provider
                .provider_id
                .cmp(&right.detail.provider.provider_id)
        } else {
            by_instance
        }
    } else {
        by_extension
    }
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
        ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY => {
            vec![MediaType::Movie, MediaType::Series, MediaType::Anime]
        }
        _ => Vec::new(),
    }
}

fn infer_actions_from_capability(capability: &str) -> Vec<&'static str> {
    match capability.trim().to_ascii_lowercase().as_str() {
        "media.manager.movies" | "media.manager.tv" | "media.manager.anime" => {
            vec!["add", "monitor", "search"]
        }
        value if value.starts_with("media.search.") => vec!["search"],
        ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY => vec!["search"],
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
    let source_preferences = load_source_preferences(&store, &providers)
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
    let (source_contexts, _source_errors) = filter_search_providers(
        state,
        &store,
        collect_source_providers(&providers, media_type),
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
    let source_providers: Vec<ProviderSummary> =
        source_contexts.iter().map(provider_summary).collect();

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
    let preferred_source_provider_id = preferred_source_for_type(&source_preferences, media_type);
    let default_source_provider_id =
        resolve_default_source(preferred_source_provider_id, &source_contexts);
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
        source_providers,
        preferred_manager_provider_id,
        default_manager_provider_id,
        preferred_source_provider_id,
        default_source_provider_id,
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

async fn load_source_preferences(
    store: &ExtensionStore<'_>,
    providers: &[ProviderContext],
) -> AnyResult<SourcePreferenceState> {
    let movie_provider_id =
        sanitize_source_preference(store, SOURCE_PREF_MOVIE, MediaType::Movie, providers).await?;
    let series_provider_id =
        sanitize_source_preference(store, SOURCE_PREF_SERIES, MediaType::Series, providers).await?;
    let anime_provider_id =
        sanitize_source_preference(store, SOURCE_PREF_ANIME, MediaType::Anime, providers).await?;

    Ok(SourcePreferenceState {
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

async fn sanitize_source_preference(
    store: &ExtensionStore<'_>,
    key: &str,
    media_type: MediaType,
    providers: &[ProviderContext],
) -> AnyResult<Option<Uuid>> {
    let provider_id = load_manager_preference(store, key).await?;
    let Some(provider_id) = provider_id else {
        return Ok(None);
    };

    let is_valid = collect_source_providers(providers, media_type)
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
        "cleared stale acquisition source preference"
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

async fn save_source_preference(
    store: &ExtensionStore<'_>,
    media_type: MediaType,
    provider_id: Option<Uuid>,
) -> AnyResult<()> {
    let key = match media_type {
        MediaType::Movie => SOURCE_PREF_MOVIE,
        MediaType::Series => SOURCE_PREF_SERIES,
        MediaType::Anime => SOURCE_PREF_ANIME,
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

fn validate_source_preference_provider(
    provider_id: Option<Uuid>,
    media_type: MediaType,
    providers: &[ProviderContext],
) -> ApiResult<()> {
    let Some(provider_id) = provider_id else {
        return Ok(());
    };
    let valid = collect_source_providers(providers, media_type)
        .iter()
        .any(|provider| provider.detail.provider.provider_id == provider_id);
    if !valid {
        return Err(ApiError::bad_request(
            "provider is not a valid acquisition source for the selected media type",
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
    use crate::{config::DatabaseConfig, db::Database};
    use std::fs;

    #[test]
    fn anime_scope_preview_includes_absolute_only_mainline_targets() {
        let season_ref = crate::acquisition::release_resolution::anime::AnimeGraphSeasonRef {
            season_number: 1,
            anilist_id: "21".to_string(),
            title: "Long Running Anime".to_string(),
            format: Some("TV".to_string()),
            season_year: Some(1999),
            start_year: Some(1999),
            status: Some("RELEASING".to_string()),
            episodes: None,
            next_airing_episode: None,
            next_airing_at: None,
            confidence: 1.0,
        };
        let graph = crate::acquisition::release_resolution::anime::AnimeMetadataGraph {
            resolver_version: "test".to_string(),
            seed_anilist_id: "21".to_string(),
            root_anilist_id: "21".to_string(),
            title: "Long Running Anime".to_string(),
            year: Some(1999),
            external_ids: ExternalIds {
                anilist: Some("21".to_string()),
                ..ExternalIds::default()
            },
            seasons: vec![crate::acquisition::release_resolution::anime::AnimeGraphSeason {
                season_number: 1,
                anilist_id: "21".to_string(),
                title: "Long Running Anime".to_string(),
                format: Some("TV".to_string()),
                season_year: Some(1999),
                start_year: Some(1999),
                status: Some("RELEASING".to_string()),
                episodes: None,
                next_airing_episode: None,
                next_airing_at: None,
                confidence: 1.0,
                mapping_available: true,
                mapped_episode_count: 3,
                target_count: 3,
            }],
            targets: vec![
                crate::acquisition::release_resolution::anime::AnimeGraphTarget {
                    source: crate::acquisition::release_resolution::anime::AnimeGraphTargetSource::AniZip,
                    target_key: "S01E01".to_string(),
                    canonical_key: "tvdb:81797:S01E01".to_string(),
                    title: "Episode 1".to_string(),
                    season_number: Some(1),
                    episode_number: Some(1),
                    absolute_episode_number: Some(1),
                    air_date: None,
                    air_time: None,
                    anilist_season_id: "21".to_string(),
                    anilist_status: Some("RELEASING".to_string()),
                    tvdb_series_id: Some("81797".to_string()),
                    tvdb_episode_id: Some("1001".to_string()),
                    anidb_anime_id: None,
                    anidb_episode_id: None,
                    season: season_ref.clone(),
                    raw: json!({ "episode": "1" }),
                },
                crate::acquisition::release_resolution::anime::AnimeGraphTarget {
                    source: crate::acquisition::release_resolution::anime::AnimeGraphTargetSource::AniZip,
                    target_key: "A0023".to_string(),
                    canonical_key: "anilist:21:A0023".to_string(),
                    title: "Episode 23".to_string(),
                    season_number: None,
                    episode_number: None,
                    absolute_episode_number: Some(23),
                    air_date: Some("2026-05-24".to_string()),
                    air_time: None,
                    anilist_season_id: "21".to_string(),
                    anilist_status: Some("RELEASING".to_string()),
                    tvdb_series_id: Some("81797".to_string()),
                    tvdb_episode_id: None,
                    anidb_anime_id: None,
                    anidb_episode_id: None,
                    season: season_ref.clone(),
                    raw: json!({ "episode": "23" }),
                },
                crate::acquisition::release_resolution::anime::AnimeGraphTarget {
                    source: crate::acquisition::release_resolution::anime::AnimeGraphTargetSource::AniZip,
                    target_key: "A0024".to_string(),
                    canonical_key: "anilist:21:A0024".to_string(),
                    title: "Episode 24".to_string(),
                    season_number: None,
                    episode_number: None,
                    absolute_episode_number: Some(24),
                    air_date: Some("2026-05-31".to_string()),
                    air_time: None,
                    anilist_season_id: "21".to_string(),
                    anilist_status: Some("RELEASING".to_string()),
                    tvdb_series_id: Some("81797".to_string()),
                    tvdb_episode_id: None,
                    anidb_anime_id: None,
                    anidb_episode_id: None,
                    season: season_ref,
                    raw: json!({ "episode": "24" }),
                },
            ],
            aliases: vec![],
            fingerprint: "test".to_string(),
        };

        let mut response = scope_preview_response(
            ScopedAddMediaIdentity {
                kind: MediaType::Anime,
                title: "Long Running Anime".to_string(),
                year: Some(1999),
                external_ids: None,
                aliases: Vec::new(),
            },
            anime_scope_preview_seasons_from_graph_at(
                &graph,
                DateTime::parse_from_rfc3339("2026-05-30T12:00:00Z")
                    .expect("valid test now")
                    .with_timezone(&Utc),
            ),
            Vec::new(),
            Vec::new(),
        );
        sort_scope_preview(&mut response);

        assert_eq!(response.seasons.len(), 1);
        assert_eq!(response.seasons[0].episode_count, 2);
        let target_keys = response.seasons[0]
            .episodes
            .iter()
            .map(|episode| episode.target_key.as_str())
            .collect::<Vec<_>>();
        assert_eq!(target_keys, vec!["S01E01", "A0023"]);
        assert!(!target_keys.contains(&"A0024"));
        assert_eq!(response.seasons[0].episodes[1].season_number, Some(1));
        assert_eq!(
            response.seasons[0].episodes[1].absolute_episode_number,
            Some(23)
        );
    }

    #[test]
    fn scoped_add_target_metadata_carries_media_aliases() {
        let target = FindMediaScopedPreviewTarget {
            target_key: "S01E01".to_string(),
            season_number: Some(1),
            episode_number: Some(1),
            absolute_episode_number: Some(1),
            title: Some("Fullmetal Alchemist".to_string()),
            air_date: Some("2009-04-05".to_string()),
            thumbnail_url: None,
        };
        let aliases = vec![
            "Hagane no Renkinjutsushi: FULLMETAL ALCHEMIST".to_string(),
            "Fullmetal Alchemist Brotherhood".to_string(),
        ];

        let acquisition_target = scoped_add_new_acquisition_target(
            MediaType::Anime,
            "Hagane no Renkinjutsushi: FULLMETAL ALCHEMIST",
            &aliases,
            &target,
            Some(Uuid::new_v4()),
            Uuid::new_v4(),
            Utc::now(),
        );

        let metadata = acquisition_target.metadata.expect("metadata");
        assert_eq!(
            metadata
                .get("aliases")
                .and_then(Value::as_array)
                .expect("aliases")
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>(),
            vec![
                "Hagane no Renkinjutsushi: FULLMETAL ALCHEMIST",
                "Fullmetal Alchemist Brotherhood"
            ]
        );
    }

    #[test]
    fn source_import_pack_requires_file_selection_without_episode_hint() {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::write(dir.path().join("Example Episode 01.mkv"), b"one").expect("write e1");
        fs::write(dir.path().join("Example Episode 02.mkv"), b"two").expect("write e2");
        let subscription = test_source_subscription(MediaType::Anime);
        let target = test_source_target(
            subscription.subscription_id,
            MediaType::Anime,
            None,
            None,
            None,
        );

        let selected = select_source_import_file_from_visible_path(
            &subscription,
            &target,
            &dir.path().to_string_lossy(),
        );

        assert!(matches!(
            selected,
            SourceImportSelection::NeedsFileSelection(_)
        ));
    }

    #[test]
    fn source_import_pack_uses_candidate_filename_hint() {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::write(dir.path().join("Example Episode 01.mkv"), b"one").expect("write e1");
        let expected_path = dir.path().join("Example Episode 02.mkv");
        fs::write(&expected_path, b"two").expect("write e2");
        let subscription = test_source_subscription(MediaType::Anime);
        let target = test_source_target(
            subscription.subscription_id,
            MediaType::Anime,
            None,
            None,
            Some(json!({
                "title": "Example Anime Pack",
                "raw": {
                    "stream": {
                        "behaviorHints": {
                            "filename": "Example Episode 02.mkv"
                        }
                    }
                }
            })),
        );

        let selected = select_source_import_file_from_visible_path(
            &subscription,
            &target,
            &dir.path().to_string_lossy(),
        );

        let SourceImportSelection::Ready(file) = selected else {
            panic!("expected pack file to be selected");
        };
        assert_eq!(file.path, expected_path.to_string_lossy());
    }

    #[tokio::test]
    async fn release_job_download_id_guard_collects_rr_managed_downloads() -> AnyResult<()> {
        let database = Database::connect(&DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            ..DatabaseConfig::default()
        })
        .await?;
        database.run_migrations().await?;

        let release_id = Uuid::new_v4();
        sqlx::query::<sqlx::Any>(
            "INSERT INTO acquisition_releases (
                release_id,
                source_extension_id,
                media_type,
                title,
                release_title,
                source,
                source_kind,
                fingerprint,
                release_kind,
                resolver_kind,
                resolver_version,
                confidence,
                state
            ) VALUES (?, 'torrentio', 'anime', 'Fullmetal Alchemist Brotherhood',
                '[Erai-raws] Fullmetal Alchemist Brotherhood Batch', 'torrentio',
                'magnet', 'release-fingerprint', 'season_pack', 'anime_shoko_style',
                'test', 'medium', 'submitted')",
        )
        .bind(release_id.to_string())
        .execute(&database.pool)
        .await?;

        for (download_id, job_suffix) in [
            (Some("rr-managed-download"), "managed"),
            (Some("   "), "blank"),
            (None, "missing"),
        ] {
            sqlx::query::<sqlx::Any>(
                "INSERT INTO acquisition_release_jobs (
                    release_job_id,
                    release_id,
                    route_logical_id,
                    download_id,
                    state,
                    active
                ) VALUES (?, ?, 'acquisition.debrid.default', ?, 'submitted', 1)",
            )
            .bind(format!("job-{job_suffix}"))
            .bind(release_id.to_string())
            .bind(download_id)
            .execute(&database.pool)
            .await?;
        }

        let guarded = acquisition_release_job_download_ids(&database.pool).await?;

        assert!(guarded.contains("rr-managed-download"));
        assert_eq!(guarded.len(), 1);
        Ok(())
    }

    #[test]
    fn source_acquisition_status_keeps_source_label_and_progress() {
        let source_provider_id = Uuid::new_v4();
        let subscription = AcquisitionSubscription {
            source_provider_id: Some(source_provider_id),
            ..test_source_subscription(MediaType::Movie)
        };
        let target = AcquisitionTarget {
            selected_provider_id: Some(source_provider_id),
            selected_route_logical_id: Some(DEBRID_DEFAULT_LOGICAL_ID.to_string()),
            selected_candidate: Some(json!({
                "title": "Example Movie 1080p",
                "quality": "1080p",
                "sourceKind": "magnet"
            })),
            download_id: Some("rd-job".to_string()),
            state: AcquisitionTargetState::Submitted,
            ..test_source_target(
                subscription.subscription_id,
                MediaType::Movie,
                None,
                None,
                None,
            )
        };
        let mut progress_index = AcquisitionDownloaderProgressIndex::default();
        progress_index.insert(
            "rd-job",
            AcquisitionDownloaderProgress {
                release_title: Some("Example Movie 1080p".to_string()),
                status: Some("materializing".to_string()),
                category: Some("movies".to_string()),
                progress_percent: Some(42.0),
                downloaded_bytes: Some(42),
                size_bytes: Some(100),
                download_rate_bps: Some(1_024),
                ..Default::default()
            },
        );
        let provider_map = HashMap::from([(
            source_provider_id,
            test_provider_context(source_provider_id, "External Source", "test_source"),
        )]);

        let item = build_source_acquisition_item(
            &subscription,
            &[target],
            &provider_map,
            &progress_index,
            &HashMap::new(),
            &AcquisitionUxContext::default(),
        )
        .expect("source acquisition item");

        assert_eq!(item.manager_label, "External Source (test_source)");
        assert_eq!(item.phase, "materializing");
        assert_eq!(item.children.len(), 1);
        assert_eq!(item.children[0].download_id.as_deref(), Some("rd-job"));
        assert_eq!(item.children[0].phase, "materializing");
        assert_eq!(item.children[0].progress_percent, Some(42.0));
        assert!(
            item.evidence
                .iter()
                .any(|evidence| evidence.label == "Source"
                    && evidence.value == "External Source (test_source)")
        );
        assert!(item.actions.iter().any(|action| {
            action.id == CANCEL_ACQUISITION_DOWNLOADS_ACTION_ID
                && action.subscription_id == Some(subscription.subscription_id)
                && action.cancel_mode.as_deref() == Some("cancel_downloads")
        }));
    }

    #[test]
    fn source_acquisition_status_exposes_source_and_route_provider_attribution() {
        let source_provider_id = Uuid::new_v4();
        let route_provider_id = Uuid::new_v4();
        let subscription = AcquisitionSubscription {
            source_provider_id: Some(source_provider_id),
            ..test_source_subscription(MediaType::Series)
        };
        let target = AcquisitionTarget {
            selected_provider_id: Some(source_provider_id),
            selected_route_logical_id: Some(TORRENT_DEFAULT_LOGICAL_ID.to_string()),
            selected_candidate: Some(json!({
                "title": "Example Series S01E01 1080p",
                "sourceProviderId": source_provider_id.to_string(),
                "submissionResult": {
                    "routeLogicalId": TORRENT_DEFAULT_LOGICAL_ID,
                    "routeProviderId": route_provider_id.to_string(),
                    "downloadId": "qb-fallback"
                }
            })),
            download_id: Some("qb-fallback".to_string()),
            state: AcquisitionTargetState::Submitted,
            ..test_source_target(
                subscription.subscription_id,
                MediaType::Series,
                Some(1),
                Some(1),
                None,
            )
        };
        let runtime = SourceTargetReleaseRuntime {
            release_id: Uuid::new_v4(),
            source_provider_id: Some(source_provider_id),
            route_provider_id: Some(route_provider_id),
            release_title: "Example Series S01E01 1080p".to_string(),
            release_state: AcquisitionReleaseState::Submitted,
            release_state_reason: Some("submitted through torrent fallback".to_string()),
            coverage_state: Some(ReleaseCoverageState::Submitted),
            coverage_reason: None,
            selected_route_logical_id: Some(TORRENT_DEFAULT_LOGICAL_ID.to_string()),
            download_id: Some("qb-fallback".to_string()),
            job_state: Some(ReleaseJobState::Submitted),
            job_state_reason: None,
            import_state: None,
            import_state_reason: None,
            import_mismatch_class: None,
            updated_at: Utc::now(),
        };
        let provider_map = HashMap::from([
            (
                source_provider_id,
                test_provider_context(source_provider_id, "Torrentio", "torrentio"),
            ),
            (
                route_provider_id,
                test_provider_context(route_provider_id, "qBittorrent", "qbittorrent"),
            ),
        ]);
        let release_runtime = HashMap::from([(target.target_id, runtime)]);

        let item = build_source_acquisition_item(
            &subscription,
            &[target],
            &provider_map,
            &AcquisitionDownloaderProgressIndex::default(),
            &release_runtime,
            &AcquisitionUxContext::default(),
        )
        .expect("source acquisition item");

        assert_eq!(item.manager_label, "Torrentio (torrentio)");
        assert!(item.evidence.iter().any(|evidence| {
            evidence.label == "Source" && evidence.value == "Torrentio (torrentio)"
        }));
        assert!(item.evidence.iter().any(|evidence| {
            evidence.label == "Route provider" && evidence.value == "qBittorrent (qbittorrent)"
        }));
        let child = item.children.first().expect("child");
        assert_eq!(child.source_provider_id, Some(source_provider_id));
        assert_eq!(
            child.source_provider_label.as_deref(),
            Some("Torrentio (torrentio)")
        );
        assert_eq!(child.route_provider_id, Some(route_provider_id));
        assert_eq!(
            child.route_provider_label.as_deref(),
            Some("qBittorrent (qbittorrent)")
        );
        assert_eq!(
            child.route_logical_id.as_deref(),
            Some(TORRENT_DEFAULT_LOGICAL_ID)
        );
    }

    #[test]
    fn asr9_source_failure_language_hides_internal_torbox_parse_context() {
        let detail = source_user_attention_detail(
            AcquisitionPhase::Failed,
            Some("Debrid failure [no_seeds]: parsing TorBox torrent '12345'"),
            None,
        );

        assert!(!detail.to_ascii_lowercase().contains("parsing torbox"));
        assert!(!detail.contains("12345"));
        assert!(detail.contains("TorBox returned an incomplete torrent response."));
    }

    #[test]
    fn asr9_pending_retry_target_surfaces_trying_next_release() {
        let subscription = test_source_subscription(MediaType::Series);
        let target = AcquisitionTarget {
            state: AcquisitionTargetState::Pending,
            state_reason: Some(
                "qBittorrent found metadata but the swarm has no complete seeds. Trying the next ranked release."
                    .to_string(),
            ),
            ..test_source_target(
                subscription.subscription_id,
                MediaType::Series,
                Some(1),
                Some(1),
                None,
            )
        };

        let child = build_source_acquisition_child(&target, None, &HashMap::new(), None, None);

        assert_eq!(child.phase, "finding_another_release");
        assert_eq!(child.headline, "Trying next release.");
        assert_eq!(
            child.detail.as_deref(),
            Some(
                "qBittorrent found metadata but the swarm has no complete seeds. Trying next release."
            )
        );
    }

    #[test]
    fn asr9_blocked_debrid_target_uses_safe_no_seed_language() {
        let subscription = test_source_subscription(MediaType::Series);
        let target = AcquisitionTarget {
            state: AcquisitionTargetState::Blocked,
            state_reason: Some(
                "Debrid failure [no_seeds]: TorBox accepted this torrent, but it is not cached and has no seeds."
                    .to_string(),
            ),
            ..test_source_target(
                subscription.subscription_id,
                MediaType::Series,
                Some(1),
                Some(1),
                None,
            )
        };

        let child = build_source_acquisition_child(&target, None, &HashMap::new(), None, None);

        assert_eq!(child.phase, "needs_attention");
        assert_eq!(child.headline, "Debrid release stalled.");
        assert_eq!(
            child.blocker.as_ref().map(|blocker| blocker.code.as_str()),
            Some("debrid_no_seeds")
        );
        let detail = child.detail.as_deref().expect("detail");
        assert!(detail.contains("TorBox accepted this torrent"));
        assert!(!detail.contains("Debrid failure"));
    }

    #[test]
    fn source_acquisition_parent_progress_includes_pending_and_completed_targets() {
        let source_provider_id = Uuid::new_v4();
        let subscription = AcquisitionSubscription {
            source_provider_id: Some(source_provider_id),
            ..test_source_subscription(MediaType::Series)
        };
        let mut active = AcquisitionTarget {
            selected_provider_id: Some(source_provider_id),
            selected_route_logical_id: Some(DEBRID_DEFAULT_LOGICAL_ID.to_string()),
            download_id: Some("debrid-active".to_string()),
            state: AcquisitionTargetState::Submitted,
            ..test_source_target(
                subscription.subscription_id,
                MediaType::Series,
                Some(1),
                Some(1),
                None,
            )
        };
        active.target_key = "S01E01".to_string();
        let mut pending = test_source_target(
            subscription.subscription_id,
            MediaType::Series,
            Some(1),
            Some(2),
            None,
        );
        pending.target_key = "S01E02".to_string();
        let mut imported = test_source_target(
            subscription.subscription_id,
            MediaType::Series,
            Some(1),
            Some(3),
            None,
        );
        imported.target_key = "S01E03".to_string();
        imported.state = AcquisitionTargetState::Imported;

        let mut progress_index = AcquisitionDownloaderProgressIndex::default();
        progress_index.insert(
            "debrid-active",
            AcquisitionDownloaderProgress {
                status: Some("materializing".to_string()),
                progress_percent: Some(50.0),
                ..Default::default()
            },
        );
        let provider_map = HashMap::from([(
            source_provider_id,
            test_provider_context(source_provider_id, "External Source", "test_source"),
        )]);

        let item = build_source_acquisition_item(
            &subscription,
            &[active, pending, imported],
            &provider_map,
            &progress_index,
            &HashMap::new(),
            &AcquisitionUxContext::default(),
        )
        .expect("source acquisition item");

        assert_eq!(item.progress_percent, Some(50.0));
    }

    #[test]
    fn source_acquisition_active_progress_has_phase_stable_bounds() {
        let source_provider_id = Uuid::new_v4();
        let subscription = AcquisitionSubscription {
            source_provider_id: Some(source_provider_id),
            ..test_source_subscription(MediaType::Series)
        };

        let materializing = AcquisitionTarget {
            selected_provider_id: Some(source_provider_id),
            selected_route_logical_id: Some(DEBRID_DEFAULT_LOGICAL_ID.to_string()),
            state: AcquisitionTargetState::Submitted,
            ..test_source_target(
                subscription.subscription_id,
                MediaType::Series,
                Some(1),
                Some(1),
                None,
            )
        };
        let runtime = SourceTargetReleaseRuntime {
            release_id: Uuid::new_v4(),
            source_provider_id: Some(source_provider_id),
            route_provider_id: None,
            release_title: "Example Series S01E01 1080p".to_string(),
            release_state: AcquisitionReleaseState::Materializing,
            release_state_reason: None,
            coverage_state: Some(ReleaseCoverageState::Submitted),
            coverage_reason: None,
            selected_route_logical_id: Some(DEBRID_DEFAULT_LOGICAL_ID.to_string()),
            download_id: Some("debrid-materializing".to_string()),
            job_state: Some(ReleaseJobState::Materializing),
            job_state_reason: None,
            import_state: None,
            import_state_reason: None,
            import_mismatch_class: None,
            updated_at: Utc::now(),
        };
        let release_runtime = HashMap::from([(materializing.target_id, runtime)]);
        let provider_map = HashMap::from([(
            source_provider_id,
            test_provider_context(source_provider_id, "External Source", "test_source"),
        )]);

        let item = build_source_acquisition_item(
            &subscription,
            &[materializing],
            &provider_map,
            &AcquisitionDownloaderProgressIndex::default(),
            &release_runtime,
            &AcquisitionUxContext::default(),
        )
        .expect("source acquisition item");

        assert_eq!(item.phase, "materializing");
        assert_eq!(item.progress_percent, Some(0.0));
        assert_eq!(item.children[0].phase, "materializing");
        assert_eq!(item.children[0].progress_percent, Some(0.0));
    }

    #[test]
    fn staged_source_acquisition_child_surfaces_route_state_reason() {
        let source_provider_id = Uuid::new_v4();
        let route_provider_id = Uuid::new_v4();
        let subscription = AcquisitionSubscription {
            source_provider_id: Some(source_provider_id),
            ..test_source_subscription(MediaType::Movie)
        };
        let target = AcquisitionTarget {
            selected_provider_id: Some(source_provider_id),
            selected_route_logical_id: Some(TORRENT_DEFAULT_LOGICAL_ID.to_string()),
            selected_candidate: Some(json!({
                "title": "The.Northman.2022.1080p.WEBRip.x265-RARBG.mp4",
                "submissionResult": {
                    "routeLogicalId": TORRENT_DEFAULT_LOGICAL_ID,
                    "routeProviderId": route_provider_id.to_string(),
                    "downloadId": "northman-qb"
                }
            })),
            download_id: Some("northman-qb".to_string()),
            state: AcquisitionTargetState::Submitted,
            ..test_source_target(
                subscription.subscription_id,
                MediaType::Movie,
                None,
                None,
                None,
            )
        };
        let runtime = SourceTargetReleaseRuntime {
            release_id: Uuid::new_v4(),
            source_provider_id: Some(source_provider_id),
            route_provider_id: Some(route_provider_id),
            release_title: "The.Northman.2022.1080p.WEBRip.x265-RARBG.mp4".to_string(),
            release_state: AcquisitionReleaseState::Staging,
            release_state_reason: Some(
                "qBittorrent torrent staged for deterministic file selection.".to_string(),
            ),
            coverage_state: Some(ReleaseCoverageState::Submitted),
            coverage_reason: None,
            selected_route_logical_id: Some(TORRENT_DEFAULT_LOGICAL_ID.to_string()),
            download_id: Some("northman-qb".to_string()),
            job_state: Some(ReleaseJobState::Staging),
            job_state_reason: Some(
                "qBittorrent metadata pending for staged file selection.".to_string(),
            ),
            import_state: None,
            import_state_reason: None,
            import_mismatch_class: None,
            updated_at: Utc::now(),
        };
        let provider_map = HashMap::from([
            (
                source_provider_id,
                test_provider_context(source_provider_id, "Torrentio", "torrentio"),
            ),
            (
                route_provider_id,
                test_provider_context(route_provider_id, "qBittorrent", "qbittorrent"),
            ),
        ]);
        let release_runtime = HashMap::from([(target.target_id, runtime)]);

        let item = build_source_acquisition_item(
            &subscription,
            &[target],
            &provider_map,
            &AcquisitionDownloaderProgressIndex::default(),
            &release_runtime,
            &AcquisitionUxContext::default(),
        )
        .expect("source acquisition item");

        let child = item.children.first().expect("child");
        assert_eq!(child.phase, "staged");
        assert_eq!(
            child.detail.as_deref(),
            Some("qBittorrent metadata pending for staged file selection.")
        );
    }

    #[test]
    fn imported_source_acquisition_child_ignores_duplicate_blocked_import_run() {
        let source_provider_id = Uuid::new_v4();
        let route_provider_id = Uuid::new_v4();
        let subscription = AcquisitionSubscription {
            source_provider_id: Some(source_provider_id),
            ..test_source_subscription(MediaType::Movie)
        };
        let target = AcquisitionTarget {
            selected_provider_id: Some(source_provider_id),
            selected_route_logical_id: Some(DEBRID_DEFAULT_LOGICAL_ID.to_string()),
            selected_candidate: Some(json!({
                "title": "The.Northman.2022.1080p.WEBRip.x264.AAC5.1-[YTS.MX].mp4",
                "submissionResult": {
                    "routeLogicalId": DEBRID_DEFAULT_LOGICAL_ID,
                    "routeProviderId": route_provider_id.to_string(),
                    "downloadId": "northman-debrid"
                }
            })),
            download_id: Some("northman-debrid".to_string()),
            import_event_id: Some(Uuid::new_v4()),
            state: AcquisitionTargetState::Imported,
            state_reason: Some("Imported into the Elixir library.".to_string()),
            ..test_source_target(
                subscription.subscription_id,
                MediaType::Movie,
                None,
                None,
                None,
            )
        };
        let runtime = SourceTargetReleaseRuntime {
            release_id: Uuid::new_v4(),
            source_provider_id: Some(source_provider_id),
            route_provider_id: Some(route_provider_id),
            release_title: "The.Northman.2022.1080p.WEBRip.x264.AAC5.1-[YTS.MX].mp4".to_string(),
            release_state: AcquisitionReleaseState::Completed,
            release_state_reason: Some("Debrid materializer completed selected files.".to_string()),
            coverage_state: Some(ReleaseCoverageState::Submitted),
            coverage_reason: None,
            selected_route_logical_id: Some(DEBRID_DEFAULT_LOGICAL_ID.to_string()),
            download_id: Some("northman-debrid".to_string()),
            job_state: Some(ReleaseJobState::Completed),
            job_state_reason: Some("Debrid materializer completed selected files.".to_string()),
            import_state: Some(AcquisitionImportRunState::Blocked),
            import_state_reason: Some("target is already imported by another release".to_string()),
            import_mismatch_class: Some("target_already_imported".to_string()),
            updated_at: Utc::now(),
        };
        let provider_map = HashMap::from([
            (
                source_provider_id,
                test_provider_context(source_provider_id, "Torrentio", "torrentio"),
            ),
            (
                route_provider_id,
                test_provider_context(route_provider_id, "TorBox", "torbox"),
            ),
        ]);
        let release_runtime = HashMap::from([(target.target_id, runtime)]);

        let item = build_source_acquisition_item(
            &subscription,
            &[target],
            &provider_map,
            &AcquisitionDownloaderProgressIndex::default(),
            &release_runtime,
            &AcquisitionUxContext::default(),
        )
        .expect("source acquisition item");

        let child = item.children.first().expect("child");
        assert_eq!(item.phase, "completed");
        assert_eq!(child.phase, "completed");
        assert_eq!(child.status.as_deref(), Some("imported"));
        assert_eq!(child.headline, "Imported.");
        assert_eq!(
            child.detail.as_deref(),
            Some("Imported into the Elixir library.")
        );
        assert!(child.blocker.is_none());
    }

    #[test]
    fn source_acquisition_importing_target_keeps_completed_download_progress() {
        let source_provider_id = Uuid::new_v4();
        let subscription = AcquisitionSubscription {
            source_provider_id: Some(source_provider_id),
            ..test_source_subscription(MediaType::Series)
        };

        let importing = AcquisitionTarget {
            selected_provider_id: Some(source_provider_id),
            selected_route_logical_id: Some(DEBRID_DEFAULT_LOGICAL_ID.to_string()),
            state: AcquisitionTargetState::Submitted,
            ..test_source_target(
                subscription.subscription_id,
                MediaType::Series,
                Some(1),
                Some(1),
                None,
            )
        };
        let mut pending = test_source_target(
            subscription.subscription_id,
            MediaType::Series,
            Some(1),
            Some(2),
            None,
        );
        pending.state = AcquisitionTargetState::Pending;
        let mut imported = test_source_target(
            subscription.subscription_id,
            MediaType::Series,
            Some(1),
            Some(3),
            None,
        );
        imported.state = AcquisitionTargetState::Imported;
        let runtime = SourceTargetReleaseRuntime {
            release_id: Uuid::new_v4(),
            source_provider_id: Some(source_provider_id),
            route_provider_id: None,
            release_title: "Example Series S01E01 1080p".to_string(),
            release_state: AcquisitionReleaseState::Completed,
            release_state_reason: None,
            coverage_state: Some(ReleaseCoverageState::Submitted),
            coverage_reason: None,
            selected_route_logical_id: Some(DEBRID_DEFAULT_LOGICAL_ID.to_string()),
            download_id: Some("debrid-complete".to_string()),
            job_state: Some(ReleaseJobState::Completed),
            job_state_reason: None,
            import_state: Some(AcquisitionImportRunState::Pending),
            import_state_reason: None,
            import_mismatch_class: None,
            updated_at: Utc::now(),
        };
        let release_runtime = HashMap::from([(importing.target_id, runtime)]);
        let provider_map = HashMap::from([(
            source_provider_id,
            test_provider_context(source_provider_id, "External Source", "test_source"),
        )]);

        let item = build_source_acquisition_item(
            &subscription,
            &[importing, pending, imported],
            &provider_map,
            &AcquisitionDownloaderProgressIndex::default(),
            &release_runtime,
            &AcquisitionUxContext::default(),
        )
        .expect("source acquisition item");

        assert_eq!(item.phase, "importing");
        assert!((item.progress_percent.unwrap_or_default() - 66.666_666).abs() < 0.001);
        let importing_child = item
            .children
            .iter()
            .find(|child| child.phase == "importing")
            .expect("importing child");
        assert_eq!(importing_child.progress_percent, Some(100.0));
    }

    #[test]
    fn source_acquisition_status_shows_subscription_before_targets_exist() {
        let source_provider_id = Uuid::new_v4();
        let subscription = AcquisitionSubscription {
            source_provider_id: Some(source_provider_id),
            last_metadata_refresh_at: Some(Utc::now()),
            ..test_source_subscription(MediaType::Series)
        };
        let provider_map = HashMap::from([(
            source_provider_id,
            test_provider_context(source_provider_id, "External Source", "test_source"),
        )]);

        let item = build_source_acquisition_item(
            &subscription,
            &[],
            &provider_map,
            &AcquisitionDownloaderProgressIndex::default(),
            &HashMap::new(),
            &AcquisitionUxContext::default(),
        )
        .expect("source acquisition item");

        assert_eq!(item.manager_label, "External Source (test_source)");
        assert_eq!(item.target_count, 0);
        assert_eq!(item.phase, "needs_attention");
        assert_eq!(
            item.blocker.as_ref().map(|blocker| blocker.code.as_str()),
            Some("metadata_tvdb_targets_missing")
        );
        assert!(item.actions.iter().any(|action| {
            action.id == REMOVE_ACQUISITION_REQUEST_ACTION_ID
                && action.subscription_id == Some(subscription.subscription_id)
                && action.cancel_mode.as_deref() == Some("dismiss")
        }));
        assert!(item.children.is_empty());
    }

    #[test]
    fn source_acquisition_status_surfaces_release_review_runtime() {
        let source_provider_id = Uuid::new_v4();
        let subscription = AcquisitionSubscription {
            source_provider_id: Some(source_provider_id),
            ..test_source_subscription(MediaType::Series)
        };
        let release_id = Uuid::new_v4();
        let target = test_source_target(
            subscription.subscription_id,
            MediaType::Series,
            Some(1),
            Some(1),
            Some(json!({ "title": "Example Series S01E01 1080p" })),
        );
        let runtime = SourceTargetReleaseRuntime {
            release_id,
            source_provider_id: Some(source_provider_id),
            route_provider_id: Some(source_provider_id),
            release_title: "Example Series S01E01 1080p".to_string(),
            release_state: AcquisitionReleaseState::ReviewRequired,
            release_state_reason: Some("Pack coverage is ambiguous.".to_string()),
            coverage_state: Some(ReleaseCoverageState::ReviewRequired),
            coverage_reason: Some("Episode coverage needs review.".to_string()),
            selected_route_logical_id: Some(DEBRID_DEFAULT_LOGICAL_ID.to_string()),
            download_id: Some("rd-review".to_string()),
            job_state: None,
            job_state_reason: None,
            import_state: None,
            import_state_reason: None,
            import_mismatch_class: None,
            updated_at: Utc::now(),
        };
        let provider_map = HashMap::from([(
            source_provider_id,
            test_provider_context(source_provider_id, "External Source", "test_source"),
        )]);
        let release_runtime = HashMap::from([(target.target_id, runtime)]);

        let item = build_source_acquisition_item(
            &subscription,
            &[target],
            &provider_map,
            &AcquisitionDownloaderProgressIndex::default(),
            &release_runtime,
            &AcquisitionUxContext::default(),
        )
        .expect("source acquisition item");

        assert_eq!(item.phase, "review_required");
        assert_eq!(item.children[0].phase, "review_required");
        assert_eq!(
            item.children[0]
                .blocker
                .as_ref()
                .map(|blocker| blocker.code.as_str()),
            Some("acquisition_review_required")
        );
        assert_eq!(item.children[0].download_id.as_deref(), Some("rd-review"));
        assert!(
            item.children[0]
                .blocker
                .as_ref()
                .map(|blocker| blocker.detail.contains("Review the selection"))
                .unwrap_or(false)
        );
        assert!(item.actions.iter().any(|action| {
            action.id == OPEN_REVIEW_ACTION_ID
                && action.release_id == Some(release_id)
                && action.subscription_id == Some(subscription.subscription_id)
        }));
    }

    #[test]
    fn source_acquisition_manual_review_transfer_overrides_stale_review_marker() {
        let source_provider_id = Uuid::new_v4();
        let route_provider_id = Uuid::new_v4();
        let subscription = AcquisitionSubscription {
            source_provider_id: Some(source_provider_id),
            ..test_source_subscription(MediaType::Series)
        };
        let release_id = Uuid::new_v4();
        let target = AcquisitionTarget {
            selected_provider_id: Some(source_provider_id),
            selected_route_logical_id: Some(DEBRID_DEFAULT_LOGICAL_ID.to_string()),
            selected_candidate: Some(json!({
                "title": "Example Series Pack 1080p",
                "sourceProviderId": source_provider_id.to_string(),
                "submissionResult": {
                    "routeLogicalId": DEBRID_DEFAULT_LOGICAL_ID,
                    "routeProviderId": route_provider_id.to_string(),
                    "downloadId": "manual-review-download"
                }
            })),
            download_id: Some("manual-review-download".to_string()),
            state: AcquisitionTargetState::Submitted,
            state_reason: Some(
                "TV candidate needs manual review before download: ambiguous_release.".to_string(),
            ),
            ..test_source_target(
                subscription.subscription_id,
                MediaType::Series,
                Some(3),
                Some(1),
                None,
            )
        };
        let runtime = SourceTargetReleaseRuntime {
            release_id,
            source_provider_id: Some(source_provider_id),
            route_provider_id: Some(route_provider_id),
            release_title: "Example Series Pack 1080p".to_string(),
            release_state: AcquisitionReleaseState::ReviewRequired,
            release_state_reason: Some("manual review required".to_string()),
            coverage_state: Some(ReleaseCoverageState::ReviewRequired),
            coverage_reason: Some("coverage needed review".to_string()),
            selected_route_logical_id: Some(DEBRID_DEFAULT_LOGICAL_ID.to_string()),
            download_id: Some("manual-review-download".to_string()),
            job_state: Some(ReleaseJobState::Downloading),
            job_state_reason: Some("manual review approved selected files".to_string()),
            import_state: None,
            import_state_reason: None,
            import_mismatch_class: None,
            updated_at: Utc::now(),
        };
        let mut progress_index = AcquisitionDownloaderProgressIndex::default();
        progress_index.insert(
            "manual-review-download",
            AcquisitionDownloaderProgress {
                status: Some("downloading".to_string()),
                size_bytes: Some(1_000),
                downloaded_bytes: Some(250),
                download_rate_bps: Some(10_000),
                ..Default::default()
            },
        );
        let provider_map = HashMap::from([
            (
                source_provider_id,
                test_provider_context(source_provider_id, "Torrentio", "torrentio"),
            ),
            (
                route_provider_id,
                test_provider_context(route_provider_id, "TorBox", "torbox"),
            ),
        ]);
        let release_runtime = HashMap::from([(target.target_id, runtime)]);

        let item = build_source_acquisition_item(
            &subscription,
            &[target],
            &provider_map,
            &progress_index,
            &release_runtime,
            &AcquisitionUxContext::default(),
        )
        .expect("source acquisition item");

        assert_eq!(item.phase, "downloading");
        assert_eq!(item.phase_label, "Downloading");
        assert!(item.blocker.is_none());
        assert_eq!(item.progress_percent, Some(25.0));
        assert!(
            item.evidence
                .iter()
                .any(|evidence| { evidence.label == "Downloading" && evidence.value == "1" })
        );
        assert!(
            !item
                .evidence
                .iter()
                .any(|evidence| { evidence.label == "Review" && evidence.value == "1" })
        );

        let child = item.children.first().expect("child");
        assert_eq!(child.phase, "downloading");
        assert_eq!(child.phase_label, "Downloading");
        assert_eq!(child.progress_percent, Some(25.0));
        assert_eq!(child.download_rate_bps, Some(10_000));
        assert!(child.blocker.is_none());
        assert_eq!(
            child.route_provider_label.as_deref(),
            Some("TorBox (torbox)")
        );
    }

    #[test]
    fn active_source_acquisition_completed_history_stays_visible_after_recent_window() {
        let now = Utc::now();
        let old = now - ChronoDuration::hours(ACQUISITION_RECENT_WINDOW_HOURS + 2);
        let cutoff = now - ChronoDuration::hours(ACQUISITION_RECENT_WINDOW_HOURS);
        let mut subscription = test_source_subscription(MediaType::Series);
        subscription.updated_at = old;
        subscription.created_at = old;
        let mut target = test_source_target(
            subscription.subscription_id,
            MediaType::Series,
            Some(1),
            Some(1),
            None,
        );
        target.state = AcquisitionTargetState::Imported;
        target.state_reason = Some("Imported into the Elixir library.".to_string());
        target.updated_at = old;
        target.created_at = old;

        let item = build_source_acquisition_item(
            &subscription,
            &[target],
            &HashMap::new(),
            &AcquisitionDownloaderProgressIndex::default(),
            &HashMap::new(),
            &AcquisitionUxContext::default(),
        )
        .expect("source acquisition item");

        assert_eq!(item.phase, AcquisitionPhase::Completed.as_str());
        assert!(
            item.last_matched_at
                .as_ref()
                .is_some_and(|value| value < &cutoff)
        );
        assert!(source_acquisition_item_should_remain_visible(
            &subscription,
            &item,
            cutoff
        ));

        subscription.active = false;
        subscription.request_mode = AcquisitionRequestMode::OneShot;
        subscription.status = AcquisitionSubscriptionStatus::Completed;
        assert!(source_subscription_can_surface_in_acquisition_log(
            &subscription
        ));
        assert!(!source_acquisition_item_should_remain_visible(
            &subscription,
            &item,
            cutoff
        ));
    }

    #[test]
    fn source_acquisition_large_backfill_summarizes_all_targets_before_truncation() {
        let subscription = test_source_subscription(MediaType::Anime);
        let mut targets = Vec::new();
        for episode in 1..=300 {
            let state = match episode {
                1..=20 => AcquisitionTargetState::Imported,
                21..=30 => AcquisitionTargetState::Submitted,
                31..=35 => AcquisitionTargetState::Pending,
                _ => AcquisitionTargetState::Submitted,
            };
            let mut target = test_source_target(
                subscription.subscription_id,
                MediaType::Anime,
                Some(((episode - 1) / 100) + 1),
                Some(((episode - 1) % 100) + 1),
                Some(json!({ "title": format!("Example Anime Episode {episode}") })),
            );
            target.state = state;
            if episode <= 20 {
                target.updated_at = Utc::now();
            }
            if episode > 35 {
                target.download_id = Some(format!("queued-{episode}"));
            }
            targets.push(target);
        }

        let item = build_source_acquisition_item(
            &subscription,
            &targets,
            &HashMap::new(),
            &AcquisitionDownloaderProgressIndex::default(),
            &HashMap::new(),
            &AcquisitionUxContext::default(),
        )
        .expect("source acquisition item");

        assert_eq!(item.target_count, 300);
        assert_eq!(item.displayed_child_count, 250);
        assert_eq!(item.hidden_child_count, 50);
        assert!(
            item.detail
                .as_deref()
                .unwrap_or_default()
                .contains("300 targets")
        );
        assert!(
            item.evidence
                .iter()
                .any(|evidence| { evidence.label == "Imported" && evidence.value == "20" })
        );
        assert!(
            item.evidence
                .iter()
                .any(|evidence| { evidence.label == "Queued" && evidence.value == "275" })
        );
    }

    #[test]
    fn source_acquisition_quarantine_surfaces_retry_and_find_another_actions() {
        let subscription = test_source_subscription(MediaType::Anime);
        let release_id = Uuid::new_v4();
        let target = test_source_target(
            subscription.subscription_id,
            MediaType::Anime,
            Some(1),
            Some(7),
            Some(json!({ "title": "Example Anime 007 1080p" })),
        );
        let runtime = SourceTargetReleaseRuntime {
            release_id,
            source_provider_id: None,
            route_provider_id: None,
            release_title: "Example Anime 007 1080p".to_string(),
            release_state: AcquisitionReleaseState::Completed,
            release_state_reason: None,
            coverage_state: Some(ReleaseCoverageState::Submitted),
            coverage_reason: None,
            selected_route_logical_id: Some(DEBRID_DEFAULT_LOGICAL_ID.to_string()),
            download_id: Some("rd-quarantine".to_string()),
            job_state: Some(ReleaseJobState::Completed),
            job_state_reason: None,
            import_state: Some(AcquisitionImportRunState::Mismatched),
            import_state_reason: Some("anime_hash_identity_mismatch".to_string()),
            import_mismatch_class: Some("anime_hash_identity_mismatch".to_string()),
            updated_at: Utc::now(),
        };
        let release_runtime = HashMap::from([(target.target_id, runtime)]);

        let item = build_source_acquisition_item(
            &subscription,
            &[target],
            &HashMap::new(),
            &AcquisitionDownloaderProgressIndex::default(),
            &release_runtime,
            &AcquisitionUxContext::default(),
        )
        .expect("source acquisition item");

        assert_eq!(item.phase, "quarantined");
        let blocker = item.blocker.as_ref().expect("quarantine blocker");
        assert_eq!(blocker.code, "acquisition_import_quarantined");
        assert!(blocker.detail.contains("different episode than planned"));
        assert!(blocker.detail.contains("left quarantined"));
        assert!(item.actions.iter().any(|action| {
            action.id == OPEN_REVIEW_ACTION_ID && action.release_id == Some(release_id)
        }));
        assert!(item.actions.iter().any(|action| {
            action.id == RETRY_IMPORT_ACTION_ID
                && action.release_id == Some(release_id)
                && action.retry_mode.as_deref() == Some("import")
        }));
        assert!(item.actions.iter().any(|action| {
            action.id == FIND_ANOTHER_RELEASE_ACTION_ID
                && action.release_id == Some(release_id)
                && action.retry_mode.as_deref() == Some("source_discovery")
        }));
    }

    #[test]
    fn source_acquisition_missing_debrid_account_surfaces_add_account_action() {
        let subscription = test_source_subscription(MediaType::Movie);
        let target = AcquisitionTarget {
            state: AcquisitionTargetState::Pending,
            ..test_source_target(
                subscription.subscription_id,
                MediaType::Movie,
                None,
                None,
                None,
            )
        };

        let item = build_source_acquisition_item(
            &subscription,
            &[target],
            &HashMap::new(),
            &AcquisitionDownloaderProgressIndex::default(),
            &HashMap::new(),
            &AcquisitionUxContext {
                debrid_account_missing: true,
            },
        )
        .expect("source acquisition item");

        assert_eq!(
            item.blocker.as_ref().map(|blocker| blocker.code.as_str()),
            Some("debrid_account_missing")
        );
        assert!(item.actions.iter().any(|action| {
            action.id == ADD_DEBRID_ACCOUNT_ACTION_ID
                && action.navigate_extension_id.as_deref() == Some(DEBRID_EXTENSION_ID)
                && action.navigate_view.as_deref() == Some("extension_control")
        }));
        assert!(item.evidence.iter().any(|evidence| {
            evidence.label == "Debrid account" && evidence.value == "Add debrid account"
        }));
    }

    #[test]
    fn source_acquisition_torrent_protection_blocker_preserves_reason() {
        let subscription = test_source_subscription(MediaType::Series);
        let reason = "protected local acquisition is blocked by 'warp_health': Protected downloads remain blocked whenever WARP health or leak checks fail.";
        let target = AcquisitionTarget {
            state: AcquisitionTargetState::Blocked,
            state_reason: Some(reason.to_string()),
            selected_route_logical_id: Some(TORRENT_DEFAULT_LOGICAL_ID.to_string()),
            ..test_source_target(
                subscription.subscription_id,
                MediaType::Series,
                Some(1),
                Some(2),
                Some(json!({ "title": "Example Series S01E02 1080p" })),
            )
        };

        let item = build_source_acquisition_item(
            &subscription,
            &[target],
            &HashMap::new(),
            &AcquisitionDownloaderProgressIndex::default(),
            &HashMap::new(),
            &AcquisitionUxContext::default(),
        )
        .expect("source acquisition item");

        let blocker = item.blocker.expect("parent blocker");
        assert_eq!(blocker.code, "torrent_route_blocked_by_network_protection");
        assert_eq!(blocker.title, "Torrent route blocked by network protection");
        assert_eq!(blocker.detail, reason);
        assert!(item.evidence.iter().any(|evidence| {
            evidence.label == "Route" && evidence.value == "Torrent via protected downloader egress"
        }));
    }

    #[test]
    fn osr5_source_acquisition_item_exposes_one_shot_request_context() {
        let subscription = AcquisitionSubscription {
            request_mode: AcquisitionRequestMode::OneShot,
            request_scope: AcquisitionRequestScope::Season,
            ..test_source_subscription(MediaType::Series)
        };
        let target = AcquisitionTarget {
            state: AcquisitionTargetState::Pending,
            ..test_source_target(
                subscription.subscription_id,
                MediaType::Series,
                Some(1),
                Some(1),
                Some(json!({ "title": "Example Series S01E01 1080p" })),
            )
        };

        let item = build_source_acquisition_item(
            &subscription,
            &[target],
            &HashMap::new(),
            &AcquisitionDownloaderProgressIndex::default(),
            &HashMap::new(),
            &AcquisitionUxContext::default(),
        )
        .expect("source acquisition item");

        assert_eq!(item.request_mode, "one_shot");
        assert_eq!(item.request_scope, "season");
        assert_eq!(item.request_label, "One-time season request");
        assert!(item.one_shot);
    }

    #[test]
    fn fmsa5_find_media_scoped_request_label_is_user_facing() {
        let scope = test_find_media_scope(
            MediaType::Series,
            ScopedAddSelection {
                selection_type: ScopedAddSelectionType::Season,
                season_number: Some(2),
                target_keys: vec!["S02E01".to_string(), "S02E02".to_string()],
                ..empty_scoped_selection(ScopedAddSelectionType::Season)
            },
            2,
        );
        let subscription = AcquisitionSubscription {
            request_mode: AcquisitionRequestMode::OneShot,
            request_scope: AcquisitionRequestScope::Season,
            scope: Some(scope),
            ..test_source_subscription(MediaType::Series)
        };
        let target = AcquisitionTarget {
            state: AcquisitionTargetState::Pending,
            ..test_source_target(
                subscription.subscription_id,
                MediaType::Series,
                Some(2),
                Some(1),
                None,
            )
        };

        let item = build_source_acquisition_item(
            &subscription,
            &[target],
            &HashMap::new(),
            &AcquisitionDownloaderProgressIndex::default(),
            &HashMap::new(),
            &AcquisitionUxContext::default(),
        )
        .expect("source acquisition item");

        assert_eq!(item.request_mode, "one_shot");
        assert_eq!(item.request_scope, "season");
        assert_eq!(item.request_label, "Season 2 requested");
        assert!(item.one_shot);
    }

    #[test]
    fn fmsa5_terminal_no_result_scoped_request_is_completed_not_downloaded() {
        let scope = test_find_media_scope(
            MediaType::Series,
            ScopedAddSelection {
                selection_type: ScopedAddSelectionType::Season,
                season_number: Some(2),
                target_keys: vec!["S02E01".to_string(), "S02E02".to_string()],
                ..empty_scoped_selection(ScopedAddSelectionType::Season)
            },
            2,
        );
        let subscription = AcquisitionSubscription {
            request_mode: AcquisitionRequestMode::OneShot,
            request_scope: AcquisitionRequestScope::Season,
            scope: Some(scope),
            ..test_source_subscription(MediaType::Series)
        };
        let mut first = test_source_target(
            subscription.subscription_id,
            MediaType::Series,
            Some(2),
            Some(1),
            None,
        );
        first.state = AcquisitionTargetState::Excluded;
        first.state_reason = Some("No safe candidate found.".to_string());
        let mut second = test_source_target(
            subscription.subscription_id,
            MediaType::Series,
            Some(2),
            Some(2),
            None,
        );
        second.state = AcquisitionTargetState::Excluded;
        second.state_reason = Some("No safe candidate found.".to_string());

        let item = build_source_acquisition_item(
            &subscription,
            &[first, second],
            &HashMap::new(),
            &AcquisitionDownloaderProgressIndex::default(),
            &HashMap::new(),
            &AcquisitionUxContext::default(),
        )
        .expect("source acquisition item");

        assert_eq!(item.phase, AcquisitionPhase::Completed.as_str());
        assert_eq!(item.phase_label, "Completed");
        assert_eq!(item.request_label, "Season 2 requested");
        assert_eq!(item.headline, "0 imported, 2 no results out of 2 targets.");
    }

    fn test_find_media_scope(
        media_type: MediaType,
        selection: ScopedAddSelection,
        selected_target_count: usize,
    ) -> Value {
        let document = ScopedAddScopeDocument::find_media(
            None,
            None,
            ScopedAddMediaIdentity {
                kind: media_type,
                title: "Example Series".to_string(),
                year: Some(2026),
                external_ids: None,
                aliases: Vec::new(),
            },
            selection,
        )
        .expect("scope document");
        let mut value = serde_json::to_value(document).expect("scope json");
        value.as_object_mut().expect("scope object").insert(
            "selectedTargetCount".to_string(),
            json!(selected_target_count),
        );
        value
    }

    fn empty_scoped_selection(selection_type: ScopedAddSelectionType) -> ScopedAddSelection {
        ScopedAddSelection {
            selection_type,
            season_number: None,
            episode_number: None,
            episode_start: None,
            episode_end: None,
            absolute_episode_number: None,
            absolute_episode_start: None,
            absolute_episode_end: None,
            target_keys: Vec::new(),
            arc_id: None,
            arc_label: None,
        }
    }

    fn test_source_subscription(media_type: MediaType) -> AcquisitionSubscription {
        let now = Utc::now();
        AcquisitionSubscription {
            subscription_id: Uuid::new_v4(),
            media_type,
            title: match media_type {
                MediaType::Movie => "Example Movie",
                MediaType::Series => "Example Series",
                MediaType::Anime => "Example Anime",
            }
            .to_string(),
            normalized_title: "example".to_string(),
            year: Some(2026),
            external_ids: None,
            idempotency_key: None,
            request_mode: crate::acquisition::subscriptions::AcquisitionRequestMode::Monitored,
            request_scope: crate::acquisition::subscriptions::AcquisitionRequestScope::Subscription,
            scope: None,
            metadata_policy:
                crate::acquisition::subscriptions::AcquisitionMetadataPolicy::Recurring,
            completion_policy:
                crate::acquisition::subscriptions::AcquisitionCompletionPolicy::Manual,
            monitor_policy: crate::acquisition::subscriptions::AcquisitionMonitorPolicy::AllMissing,
            route_policy: crate::acquisition::subscriptions::AcquisitionRoutePolicy::DebridFirst,
            source_provider_id: None,
            release_delay_seconds: 0,
            quality_profile: None,
            metadata_refresh_after: now,
            candidate_search_after: now,
            last_metadata_refresh_at: None,
            last_candidate_search_at: None,
            tracking_started_at: None,
            status: crate::acquisition::subscriptions::AcquisitionSubscriptionStatus::Active,
            active: true,
            created_at: now,
            updated_at: now,
        }
    }

    fn test_source_target(
        subscription_id: Uuid,
        media_type: MediaType,
        season_number: Option<i32>,
        episode_number: Option<i32>,
        selected_candidate: Option<Value>,
    ) -> AcquisitionTarget {
        let now = Utc::now();
        AcquisitionTarget {
            target_id: Uuid::new_v4(),
            subscription_id,
            target_key: match (season_number, episode_number) {
                (Some(season), Some(episode)) => format!("S{season:02}E{episode:02}"),
                _ => "movie".to_string(),
            },
            media_type,
            title: "Example Target".to_string(),
            season_number,
            episode_number,
            absolute_episode_number: None,
            air_date: None,
            air_time: None,
            metadata: None,
            state: AcquisitionTargetState::Submitted,
            state_reason: None,
            selected_provider_id: None,
            selected_route_logical_id: None,
            selected_candidate,
            download_id: None,
            import_event_id: None,
            search_attempts: 0,
            last_search_at: None,
            next_search_after: None,
            created_at: now,
            updated_at: now,
        }
    }

    fn test_provider_context(
        provider_id: Uuid,
        instance_name: &str,
        implementation: &str,
    ) -> ProviderContext {
        let now = Utc::now();
        let instance_id = Uuid::new_v4();
        ProviderContext {
            detail: crate::extensions::store::ProviderDetails {
                provider: crate::db::models::Provider {
                    provider_id,
                    instance_id,
                    capability: "acquisition.candidate_provider".to_string(),
                    slot_id: "default".to_string(),
                    cardinality: crate::db::models::SlotCardinality::Many,
                    implementation: Some(implementation.to_string()),
                    scope_json: None,
                    endpoint_json: None,
                    health_state: ProviderHealthState::Healthy,
                    last_healthcheck_at: None,
                    created_at: now,
                    updated_at: now,
                },
                extension_id: "elixir.sources.test".to_string(),
                trust_level: ExtensionTrustLevel::Community,
            },
            instance_name: instance_name.to_string(),
            instance_config: None,
            scope: ProviderScopeDocument::default(),
            media_types: vec![MediaType::Movie, MediaType::Series, MediaType::Anime],
        }
    }

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
                ..Default::default()
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
                ..Default::default()
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
                ..Default::default()
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
                ..Default::default()
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
                release_title: Some("Firefly.S01E12.Release".to_string()),
                progress_percent: Some(52.0),
                eta_seconds: Some(600),
                issue: None,
                size_bytes: Some(1_000_000_000),
                downloaded_bytes: Some(520_000_000),
                download_rate_bps: Some(2_000_000),
                ..Default::default()
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
        assert_eq!(state.children[0].title, "Firefly.S01E12.Release");
        assert_eq!(
            state.children[0].subtitle.as_deref(),
            Some("S01E12 • The Message")
        );
        assert_eq!(state.children[0].progress_percent, Some(52.0));
        assert_eq!(state.children[0].size_bytes, Some(1_000_000_000));
        assert_eq!(state.children[0].downloaded_bytes, Some(520_000_000));
        assert_eq!(state.children[0].download_rate_bps, Some(2_000_000));
        assert_eq!(state.children[1].phase, "queued_in_downloader");
        assert!(
            state
                .evidence
                .iter()
                .any(|item| item.label == "Transfers downloading" && item.value == "1")
        );
        assert!(
            state
                .evidence
                .iter()
                .any(|item| item.label == "Transfers queued" && item.value == "1")
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
                "Sonarr is handling this series as release downloads. 3 of 14 episode files are imported so far."
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
                ..Default::default()
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
                "Sonarr is handling this series as release downloads. 1 of 14 episode files are imported so far."
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
            nzb_name: Some("Firefly.S01E12.Release".to_string()),
            nzb_filename: None,
            category: Some("tv".to_string()),
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
        assert_eq!(
            progress.release_title.as_deref(),
            Some("Firefly.S01E12.Release")
        );
        assert_eq!(progress.category.as_deref(), Some("tv"));
        assert_eq!(progress.progress_percent, Some(1.0));
        assert_eq!(progress.size_bytes, Some(1000));
        assert_eq!(progress.downloaded_bytes, Some(10));
        assert_eq!(progress.remaining_bytes, Some(250));
        assert_eq!(progress.eta_seconds, None);
        assert!(progress.issue.is_none());
    }

    #[test]
    fn nzbget_group_issue_marks_unrecoverable_release() {
        let group = AcquisitionNzbgetGroup {
            nzb_id: 6,
            nzb_name: None,
            nzb_filename: None,
            category: None,
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
