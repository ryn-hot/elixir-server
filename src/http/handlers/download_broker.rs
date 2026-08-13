use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path as FsPath, PathBuf};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

#[cfg(test)]
use std::sync::Arc;

use anyhow::{Context, anyhow, bail};
use axum::{
    Json,
    extract::{Path, Query, State},
};
use reqwest::Method as ReqwestMethod;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::Row;
use uuid::Uuid;

use crate::{
    acquisition::anime_matching::{
        acquisition_anime_deterministic_state, acquisition_anime_match_batch_input,
        assess_acquisition_anime_provider_file_audio, bind_exact_single_anime_provider_file,
        bind_exact_single_anime_provider_file_with_semantic_evidence,
    },
    acquisition::automation::{
        anime_semantic_candidate_evidence, anime_semantic_media_kinds,
        mark_anime_semantic_coverage_fallback,
    },
    acquisition::subscriptions::{
        AcquisitionSubscription, AcquisitionTarget, AcquisitionTargetState,
        AcquisitionTargetStateUpdate, get_subscription, list_subscription_targets,
        reset_target_for_candidate_retry, update_target_state,
    },
    acquisition::{
        language_policy::LanguagePreferenceAssessmentState,
        release_resolution::{
            anime::{
                AnimeCandidateInput, AnimeCandidateScoringContext, AnimeCandidateTarget,
                AnimeCoverageOptions, AnimeFileCoveragePlan, AnimeReleaseFileInput,
                AnimeScopedAlias, anime_parser_diagnostics, parse_anime_release_title,
                plan_anime_file_coverage_with_options,
                plan_anime_file_coverage_with_semantic_evidence, score_anime_candidate,
            },
            fingerprint::{ReleaseFingerprintInput, build_release_fingerprint},
            hashing::{HashFileJob, queue_anime_hash_file},
            models::{
                AcquisitionRelease, AcquisitionReleaseCoverage, AcquisitionReleaseFile,
                AcquisitionReleaseState, NewAcquisitionRelease, NewAcquisitionReleaseCoverage,
                NewAcquisitionReleaseFile, NewAcquisitionReleaseJob, ReleaseConfidence,
                ReleaseCoverageKind, ReleaseCoverageState, ReleaseJobState, ReleaseJobStateUpdate,
                ReleaseKind, ReleaseResolverKind,
            },
            movie::{
                MOVIE_RADARR_STYLE_RESOLVER_VERSION, MovieReleaseFileSelectionInput,
                select_movie_main_file,
            },
            movie_radarr_parser::MovieRadarrStyleParser,
            store::{
                get_release, get_release_by_download_id, get_release_by_fingerprint,
                list_active_releases_by_route, list_release_coverage, list_release_files,
                list_release_jobs, update_release_coverage_review_state, update_release_job_state,
                upsert_release, upsert_release_coverage, upsert_release_file, upsert_release_job,
            },
            tv::{TvCoverageOptions, TvReleaseFileInput, TvSonarrStyleResolver, TvTarget},
        },
        stream_materializer::cleanup_http_stream_partial_from_plan,
    },
    anime_matching::{
        ANIME_MATCH_SCHEMA_VERSION, AnimeMatchAssistProvenance, AnimeMatchAssistResult,
        AnimeMatchAssistSource, AnimeMatchAudioProfile, AnimeMatchFallbackReason,
        AnimeMatchingService, DeterministicMatchState, build_semantic_evidence_request,
    },
    db::models::MediaType,
    debrid::{
        DebridReleaseSubmitContext, DebridSubmitOptions, cancel_debrid_job, debrid_source_kind,
        is_debrid_service_implementation, load_debrid_progress, submit_debrid,
    },
    download_broker::{
        DEBRID_ACCOUNT_MISSING_MESSAGE, DEBRID_DEFAULT_LOGICAL_ID,
        DEBRID_SERVICE_NOT_CONFIGURED_MESSAGE, DEBRID_SERVICE_UNAVAILABLE_MESSAGE,
        DownloadBrokerBindingKind, DownloadBrokerInventory, DownloadBrokerProviderRecord,
        DownloadBrokerRole, DownloadBrokerRouteInventory, DownloadBrokerRouteRecord,
        DownloadBrokerRouteUpdate, HTTP_STREAM_DEFAULT_LOGICAL_ID, ResolvedDownloadBrokerProvider,
        TORRENT_DEFAULT_LOGICAL_ID, USENET_DEFAULT_LOGICAL_ID, list_acquisition_routes,
        list_logical_downloaders, resolve_logical_downloader_for_owner, upsert_acquisition_route,
    },
    extensions::store::ExtensionStore,
    http::{
        auth::CurrentUser,
        error::{ApiError, ApiResult},
        handlers::{
            acquisition_sources::{
                AcquisitionCandidate, AcquisitionCandidateFile,
                validate_stream_candidate_for_broker,
            },
            extensions::{request_instance_service_form, request_instance_service_json},
        },
    },
    network::protection::observed_download_protection_status,
    runtime::RuntimePaths,
    state::AppState,
};

const QBITTORRENT_STAGING_RESOLVER_VERSION: &str = "rr5a-qbittorrent-staging-v1";
const QBITTORRENT_SELECTION_POLICY_VERSION: &str = "rr5b-qbittorrent-file-priority-v1";
const HTTP_STREAM_BROKER_RESOLVER_VERSION: &str = "ess5-http-stream-broker-v1";
const QBITTORRENT_WANTED_FILE_PRIORITY: i64 = 1;
const QBITTORRENT_SKIPPED_FILE_PRIORITY: i64 = 0;
const QBITTORRENT_METADATA_BACKOFF_INITIAL_SECONDS: u64 = 5;
const QBITTORRENT_METADATA_BACKOFF_MAX_SECONDS: u64 = 60;
const QBITTORRENT_STALE_RELEASE_BATCH_LIMIT: i64 = 100;
const QBITTORRENT_METADATA_TIMEOUT_SECONDS: i64 = 10 * 60;
const QBITTORRENT_ZERO_SEED_STALL_TIMEOUT_SECONDS: i64 = 15 * 60;
const QBITTORRENT_STALE_RELEASE_RETRY_SECONDS: i64 = 30;

static QBITTORRENT_METADATA_POLL_STATE: LazyLock<
    Mutex<HashMap<Uuid, QbittorrentMetadataPollState>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

#[cfg(test)]
#[derive(Clone)]
struct QbittorrentTestServiceFixture {
    requests: Arc<Mutex<Vec<Value>>>,
    file_rows: Value,
    torrent_info_rows: Value,
    fail_torrent_info: bool,
    fail_file_list: bool,
    fail_form_paths_once: Arc<Mutex<HashSet<String>>>,
}

#[cfg(test)]
impl QbittorrentTestServiceFixture {
    fn fail_form_paths_once(&self, paths: &[&str]) {
        self.fail_form_paths_once
            .lock()
            .expect("qBittorrent one-shot form failure lock")
            .extend(paths.iter().map(|path| path.trim_matches('/').to_string()));
    }
}

#[cfg(test)]
static QBITTORRENT_TEST_SERVICE_FIXTURES: LazyLock<
    Mutex<HashMap<Uuid, QbittorrentTestServiceFixture>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Debug, Clone)]
struct QbittorrentMetadataPollState {
    attempts: u32,
    last_attempt: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QbittorrentStaleReleaseKind {
    MetadataTimeout,
    ZeroSeedStall,
    ProviderOperationTimeout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QbittorrentProviderOperation {
    TorrentInspection,
    FilePolicyRefresh,
}

impl QbittorrentProviderOperation {
    fn reason_code(self) -> &'static str {
        match self {
            Self::TorrentInspection => "qbittorrent_torrent_inspection_timeout",
            Self::FilePolicyRefresh => "qbittorrent_file_policy_refresh_timeout",
        }
    }

    fn user_message(self) -> &'static str {
        match self {
            Self::TorrentInspection => {
                "qBittorrent remained unavailable while Elixir inspected the active torrent."
            }
            Self::FilePolicyRefresh => {
                "qBittorrent could not apply or resume the selected anime files in the allowed window."
            }
        }
    }

    fn evidence_state(self) -> &'static str {
        match self {
            Self::TorrentInspection => "torrent_inspection_error",
            Self::FilePolicyRefresh => "file_policy_refresh_error",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QbittorrentStaleReleaseDecision {
    kind: QbittorrentStaleReleaseKind,
    age_seconds: i64,
    reason_code: &'static str,
    user_message: &'static str,
}

impl QbittorrentStaleReleaseDecision {
    fn metadata_timeout(age_seconds: i64) -> Self {
        Self {
            kind: QbittorrentStaleReleaseKind::MetadataTimeout,
            age_seconds,
            reason_code: "qbittorrent_metadata_timeout",
            user_message: "qBittorrent could not fetch torrent metadata in the allowed window.",
        }
    }

    fn zero_seed_stall(age_seconds: i64) -> Self {
        Self {
            kind: QbittorrentStaleReleaseKind::ZeroSeedStall,
            age_seconds,
            reason_code: "qbittorrent_zero_seed_stall",
            user_message: "qBittorrent found metadata but the swarm has no complete seeds.",
        }
    }

    fn provider_operation_timeout(
        age_seconds: i64,
        operation: QbittorrentProviderOperation,
    ) -> Self {
        Self {
            kind: QbittorrentStaleReleaseKind::ProviderOperationTimeout,
            age_seconds,
            reason_code: operation.reason_code(),
            user_message: operation.user_message(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadBrokerSubmitRequest {
    pub source: String,
    #[serde(default)]
    pub category: Option<String>,
    #[serde(default)]
    pub paused: Option<bool>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub priority: Option<i64>,
    #[serde(default)]
    pub add_to_top: Option<bool>,
    #[serde(default)]
    pub subscription_id: Option<Uuid>,
    #[serde(default)]
    pub source_provider_id: Option<Uuid>,
    #[serde(default)]
    pub source_extension_id: Option<String>,
    #[serde(default)]
    pub media_type: Option<MediaType>,
    #[serde(default)]
    pub media_title: Option<String>,
    #[serde(default)]
    pub selected_candidate: Option<AcquisitionCandidate>,
    #[serde(default)]
    pub request_scope_evidence: Option<Value>,
    #[serde(default)]
    pub selected_stream_candidate: Option<Value>,
    #[serde(default)]
    pub release_fingerprint: Option<String>,
    /// Internal acquisition writer barrier. This is never accepted from the
    /// HTTP payload; only in-process acquisition submission can enable it.
    #[serde(skip)]
    pub(crate) defer_anime_debrid_refinement: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadBrokerRouteQuery {
    #[serde(default)]
    owner_id: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadBrokerSubmitResponse {
    pub logical_id: String,
    pub provider_id: Uuid,
    pub provider_implementation: Option<String>,
    pub accepted: bool,
    pub download_id: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadBrokerProgressResponse {
    logical_id: String,
    provider_id: Uuid,
    role: DownloadBrokerRole,
    items: Vec<DownloadBrokerProgressItem>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadBrokerProgressItem {
    id: String,
    name: Option<String>,
    state: Option<String>,
    category: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    local_path: Option<String>,
    progress: Option<f64>,
    downloaded_bytes: Option<u64>,
    total_bytes: Option<u64>,
    remaining_bytes: Option<u64>,
    download_rate_bps: Option<u64>,
    upload_rate_bps: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    debrid: Option<DownloadBrokerDebridEvidence>,
    #[serde(skip_serializing_if = "Option::is_none")]
    torrent: Option<DownloadBrokerTorrentEvidence>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream: Option<DownloadBrokerStreamEvidence>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadBrokerDebridEvidence {
    provider_name: Option<String>,
    provider_implementation: Option<String>,
    provider_capabilities: Option<Value>,
    provider_status: Option<Value>,
    remote_status: Option<String>,
    selection_mode: Option<String>,
    selected_file_count: usize,
    skipped_file_count: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    review_reasons: Vec<String>,
    failure_class: Option<String>,
    last_error: Option<String>,
    fallback_state: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadBrokerStreamEvidence {
    provider_name: Option<String>,
    provider_implementation: Option<String>,
    stream_type: Option<String>,
    source_kind: Option<String>,
    source_module: Option<String>,
    target_key: Option<String>,
    materializer_state: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DownloadBrokerTorrentEvidence {
    provider_name: Option<String>,
    provider_implementation: Option<String>,
    torrent_hash: String,
    runtime_state: String,
    metadata_state: String,
    priority_state: String,
    selected_file_count: usize,
    skipped_file_count: usize,
    review_reasons: Vec<String>,
    policy_version: Option<String>,
    coverage_fingerprint: Option<String>,
    route_owner_id: Option<String>,
    route_logical_id: Option<String>,
    category: Option<String>,
    source_extension_id: String,
    source_provider_id: Option<Uuid>,
    candidate_title: Option<String>,
    priority_applied: bool,
    user_approved: bool,
    blocker: Option<String>,
    failure_state: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadBrokerCancelQuery {
    #[serde(default)]
    delete_files: Option<bool>,
    #[serde(default)]
    owner_id: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadBrokerCancelResponse {
    pub logical_id: String,
    pub provider_id: Uuid,
    pub removed: bool,
}

pub async fn list_downloaders(
    State(state): State<AppState>,
    _user: CurrentUser,
) -> ApiResult<Json<DownloadBrokerInventory>> {
    let store = ExtensionStore::new(&state.db_pool);
    Ok(Json(list_logical_downloaders(&store).await?))
}

pub async fn list_routes(
    State(state): State<AppState>,
    _user: CurrentUser,
) -> ApiResult<Json<DownloadBrokerRouteInventory>> {
    let store = ExtensionStore::new(&state.db_pool);
    Ok(Json(list_acquisition_routes(&state.db_pool, &store).await?))
}

pub async fn update_route(
    State(state): State<AppState>,
    _user: CurrentUser,
    Path(logical_id): Path<String>,
    Json(request): Json<DownloadBrokerRouteUpdate>,
) -> ApiResult<Json<DownloadBrokerRouteRecord>> {
    let store = ExtensionStore::new(&state.db_pool);
    let record = upsert_acquisition_route(&state.db_pool, &store, &logical_id, request)
        .await
        .map_err(|err| {
            let message = err.to_string();
            if message.contains("unknown downloader logical id") {
                ApiError::not_found(message)
            } else if message.contains("binding")
                || message.contains("provider")
                || message.contains("route")
            {
                ApiError::bad_request(message)
            } else {
                ApiError::internal(message)
            }
        })?;
    Ok(Json(record))
}

pub async fn submit(
    State(state): State<AppState>,
    _user: CurrentUser,
    Path(logical_id): Path<String>,
    Query(query): Query<DownloadBrokerRouteQuery>,
    Json(request): Json<DownloadBrokerSubmitRequest>,
) -> ApiResult<Json<DownloadBrokerSubmitResponse>> {
    let store = ExtensionStore::new(&state.db_pool);
    Ok(Json(
        submit_to_broker(
            &state,
            &store,
            &logical_id,
            query.owner_id.as_deref(),
            request,
        )
        .await?,
    ))
}

pub(crate) async fn submit_to_broker(
    state: &AppState,
    store: &ExtensionStore<'_>,
    logical_id: &str,
    owner_id: Option<&str>,
    request: DownloadBrokerSubmitRequest,
) -> ApiResult<DownloadBrokerSubmitResponse> {
    let resolved = resolve_broker_provider(&state.db_pool, store, logical_id, owner_id).await?;
    ensure_route_allows_submit(state, &resolved).await?;
    let source = normalized_source(&request.source)?;

    let download_id = match resolved.record.role {
        DownloadBrokerRole::Torrent => {
            submit_qbittorrent(state, store, &resolved, source, &request, owner_id).await?
        }
        DownloadBrokerRole::Usenet => {
            Some(submit_nzbget(state, store, &resolved, source, &request).await?)
        }
        DownloadBrokerRole::DebridResolver => {
            Some(submit_debrid_broker(state, store, &resolved, source, &request, owner_id).await?)
        }
        DownloadBrokerRole::HttpStream => Some(
            submit_http_stream_broker(&state.db_pool, &resolved, source, &request, owner_id)
                .await?,
        ),
    };

    Ok(DownloadBrokerSubmitResponse {
        logical_id: logical_id.to_string(),
        provider_id: resolved.record.provider_id,
        provider_implementation: resolved.record.implementation.clone(),
        accepted: true,
        download_id,
    })
}

pub async fn progress(
    State(state): State<AppState>,
    _user: CurrentUser,
    Path(logical_id): Path<String>,
    Query(query): Query<DownloadBrokerRouteQuery>,
) -> ApiResult<Json<DownloadBrokerProgressResponse>> {
    let store = ExtensionStore::new(&state.db_pool);
    let resolved = resolve_broker_provider(
        &state.db_pool,
        &store,
        &logical_id,
        query.owner_id.as_deref(),
    )
    .await?;
    let items = match resolved.record.role {
        DownloadBrokerRole::Torrent => {
            load_qbittorrent_progress(&state, &store, &resolved.record).await?
        }
        DownloadBrokerRole::Usenet => {
            load_nzbget_progress(&state, &store, &resolved.record).await?
        }
        DownloadBrokerRole::DebridResolver => {
            load_debrid_broker_progress(&state, &store, &resolved.record).await?
        }
        DownloadBrokerRole::HttpStream => {
            load_http_stream_broker_progress(&state.db_pool, &resolved.record).await?
        }
    };
    Ok(Json(DownloadBrokerProgressResponse {
        logical_id,
        provider_id: resolved.record.provider_id,
        role: resolved.record.role,
        items,
    }))
}

pub async fn cancel(
    State(state): State<AppState>,
    _user: CurrentUser,
    Path((logical_id, download_id)): Path<(String, String)>,
    Query(query): Query<DownloadBrokerCancelQuery>,
) -> ApiResult<Json<DownloadBrokerCancelResponse>> {
    let store = ExtensionStore::new(&state.db_pool);
    let response = cancel_download_item(
        &state,
        &store,
        &logical_id,
        query.owner_id.as_deref(),
        &download_id,
        query.delete_files.unwrap_or(false),
    )
    .await?;
    Ok(Json(response))
}

pub async fn cancel_download_item(
    state: &AppState,
    store: &ExtensionStore<'_>,
    logical_id: &str,
    owner_id: Option<&str>,
    download_id: &str,
    delete_files: bool,
) -> ApiResult<DownloadBrokerCancelResponse> {
    let resolved = resolve_broker_provider(&state.db_pool, store, logical_id, owner_id).await?;
    let removed = match resolved.record.role {
        DownloadBrokerRole::Torrent => {
            cancel_qbittorrent(state, store, &resolved.record, download_id, delete_files).await?
        }
        DownloadBrokerRole::Usenet => {
            cancel_nzbget(state, store, &resolved.record, download_id).await?
        }
        DownloadBrokerRole::DebridResolver => {
            cancel_debrid_broker(state, store, &resolved.record, download_id).await?
        }
        DownloadBrokerRole::HttpStream => {
            let runtime_paths = RuntimePaths::from_roots(
                &state.settings.extensions.storage_root,
                &state.settings.library.local_root,
            );
            cancel_http_stream_broker(
                &state.db_pool,
                &resolved.record,
                download_id,
                Some(FsPath::new(&runtime_paths.downloads_root)),
            )
            .await?
        }
    };
    Ok(DownloadBrokerCancelResponse {
        logical_id: logical_id.to_string(),
        provider_id: resolved.record.provider_id,
        removed,
    })
}

async fn resolve_broker_provider(
    pool: &sqlx::AnyPool,
    store: &ExtensionStore<'_>,
    logical_id: &str,
    owner_id: Option<&str>,
) -> ApiResult<ResolvedDownloadBrokerProvider> {
    let is_debrid_route = logical_id == DEBRID_DEFAULT_LOGICAL_ID;
    resolve_logical_downloader_for_owner(
        pool,
        store,
        logical_id,
        owner_id.unwrap_or(crate::download_broker::DEFAULT_ROUTE_OWNER_ID),
    )
    .await
    .map_err(|err| {
        let message = err.to_string();
        if is_debrid_route && debrid_error_is_not_configured(&message) {
            return ApiError::conflict(DEBRID_SERVICE_NOT_CONFIGURED_MESSAGE);
        }
        if is_debrid_route && message.contains(DEBRID_SERVICE_UNAVAILABLE_MESSAGE) {
            return ApiError::conflict(DEBRID_SERVICE_UNAVAILABLE_MESSAGE);
        }
        if message.contains("unknown downloader logical id")
            || message.contains("no downloader provider")
            || message.contains("No provider is registered")
            || message.contains("no acquisition route")
        {
            ApiError::not_found(message)
        } else {
            ApiError::conflict(message)
        }
    })
}

async fn ensure_route_allows_submit(
    state: &AppState,
    resolved: &ResolvedDownloadBrokerProvider,
) -> ApiResult<()> {
    if resolved.record.role == DownloadBrokerRole::DebridResolver
        && resolved.record.health_state == crate::db::models::ProviderHealthState::Unhealthy
    {
        return Err(ApiError::conflict(DEBRID_SERVICE_UNAVAILABLE_MESSAGE));
    }
    if resolved.binding_kind != DownloadBrokerBindingKind::ManagedProtected {
        return Ok(());
    }
    let status =
        observed_download_protection_status(&state.settings, &state.db_pool, &state.secrets)
            .await
            .map_err(ApiError::from)?;
    if let Some(blocker) = status.blocker {
        return Err(ApiError::conflict(format!(
            "protected local acquisition is blocked by '{}': {}",
            blocker.code, blocker.detail
        )));
    }
    let required_app = match resolved.record.role {
        DownloadBrokerRole::Torrent => "qbittorrent",
        DownloadBrokerRole::Usenet => "nzbget",
        DownloadBrokerRole::DebridResolver => {
            return Err(ApiError::conflict(
                "debrid resolver routes cannot use protected local downloader binding",
            ));
        }
        DownloadBrokerRole::HttpStream => {
            return Err(ApiError::conflict(
                "http stream materializer routes cannot use protected local downloader binding",
            ));
        }
    };
    if !status
        .protected_apps
        .iter()
        .any(|app| app.eq_ignore_ascii_case(required_app))
    {
        return Err(ApiError::conflict(format!(
            "protected local acquisition requires '{}' to be selected by the active download protection profile",
            required_app
        )));
    }
    Ok(())
}

#[cfg(test)]
fn qbittorrent_test_service_json(instance_id: Uuid, path: &str) -> Option<anyhow::Result<Value>> {
    let fixture = QBITTORRENT_TEST_SERVICE_FIXTURES
        .lock()
        .expect("qBittorrent test service fixture lock")
        .get(&instance_id)
        .cloned()?;
    let normalized_path = path.split('?').next().unwrap_or(path).trim_matches('/');
    let (value, failed) = match normalized_path {
        "api/v2/torrents/info" => (&fixture.torrent_info_rows, fixture.fail_torrent_info),
        "api/v2/torrents/files" => (&fixture.file_rows, fixture.fail_file_list),
        _ => return None,
    };
    fixture
        .requests
        .lock()
        .expect("qBittorrent test request fixture lock")
        .push(json!({ "path": normalized_path }));
    Some(if failed {
        Err(anyhow!(
            "simulated qBittorrent service failure for {normalized_path}"
        ))
    } else {
        Ok(value.clone())
    })
}

#[cfg(test)]
fn qbittorrent_test_service_form(
    instance_id: Uuid,
    path: &str,
    fields: &HashMap<String, String>,
) -> Option<anyhow::Result<()>> {
    let fixture = QBITTORRENT_TEST_SERVICE_FIXTURES
        .lock()
        .expect("qBittorrent test service fixture lock")
        .get(&instance_id)
        .cloned()?;
    let normalized_path = path.trim_matches('/');
    if !matches!(
        normalized_path,
        "api/v2/torrents/add"
            | "api/v2/torrents/filePrio"
            | "api/v2/torrents/resume"
            | "api/v2/torrents/delete"
    ) {
        return None;
    }
    fixture
        .requests
        .lock()
        .expect("qBittorrent test request fixture lock")
        .push(json!({
            "path": normalized_path,
            "fields": fields,
        }));
    let failed = fixture
        .fail_form_paths_once
        .lock()
        .expect("qBittorrent one-shot form failure lock")
        .remove(normalized_path);
    Some(if failed {
        Err(anyhow!(
            "simulated qBittorrent service failure for {normalized_path}"
        ))
    } else {
        Ok(())
    })
}

async fn request_qbittorrent_service_json(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    method: ReqwestMethod,
    path: &str,
    body: Option<Value>,
) -> anyhow::Result<Value> {
    #[cfg(test)]
    if let Some(result) = qbittorrent_test_service_json(instance_id, path) {
        return result;
    }
    request_instance_service_json(state, store, instance_id, method, path, body).await
}

async fn request_qbittorrent_service_form(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    path: &str,
    fields: &HashMap<String, String>,
) -> anyhow::Result<()> {
    #[cfg(test)]
    if let Some(result) = qbittorrent_test_service_form(instance_id, path, fields) {
        return result;
    }
    request_instance_service_form(state, store, instance_id, path, fields).await
}

async fn submit_qbittorrent(
    state: &AppState,
    store: &ExtensionStore<'_>,
    resolved: &ResolvedDownloadBrokerProvider,
    source: &str,
    request: &DownloadBrokerSubmitRequest,
    owner_id: Option<&str>,
) -> ApiResult<Option<String>> {
    validate_torrent_source(source)?;
    let category =
        non_empty(request.category.as_deref()).or_else(|| non_empty(resolved.category.as_deref()));
    let release_context = qbittorrent_release_context(request, resolved, source, owner_id);
    let force_paused = release_context.is_some();
    let raw_candidate_info_hash = request
        .selected_candidate
        .as_ref()
        .and_then(|candidate| candidate.info_hash.as_deref());
    let download_id = qbittorrent_submission_download_id(
        source,
        raw_candidate_info_hash,
        release_context.is_some(),
    )?;

    if let Some(download_id) = download_id.as_deref() {
        cleanup_pending_qbittorrent_hash_attempts(state, store, download_id)
            .await
            .map_err(ApiError::from)?;
    }

    if let Some(context) = release_context.as_ref()
        && let Some(existing) =
            reusable_qbittorrent_release(&state.db_pool, context, &resolved.record.logical_id)
                .await
                .map_err(ApiError::from)?
    {
        if let Some(hash) = existing.download_id.as_deref() {
            if qbittorrent_release_needs_file_policy(&existing) {
                refresh_staged_qbittorrent_metadata(state, store, &existing, hash, true, false)
                    .await
                    .map_err(ApiError::from)?;
            }
            return Ok(Some(hash.to_string()));
        }
    }

    let fields = qbittorrent_add_fields(source, category, request, force_paused);
    request_qbittorrent_service_form(
        state,
        store,
        resolved.record.instance_id,
        "api/v2/torrents/add",
        &fields,
    )
    .await
    .map_err(ApiError::from)?;

    let Some(context) = release_context.as_ref() else {
        return Ok(None);
    };
    let release = upsert_qbittorrent_acquisition_release(
        &state.db_pool,
        resolved,
        context,
        category,
        download_id.as_deref(),
    )
    .await
    .map_err(ApiError::from)?;
    upsert_qbittorrent_release_job(&state.db_pool, resolved, &release, download_id.as_deref())
        .await
        .map_err(ApiError::from)?;

    // Acquisition persists its release coverage and target submission after
    // the broker returns. The active-release poll performs the first metadata
    // refinement once that bookkeeping is durable; refining here could make a
    // correct automatic retry or ready decision and then have it overwritten
    // by the caller's post-submit persistence.
    Ok(download_id)
}

async fn submit_nzbget(
    state: &AppState,
    store: &ExtensionStore<'_>,
    resolved: &ResolvedDownloadBrokerProvider,
    source: &str,
    request: &DownloadBrokerSubmitRequest,
) -> ApiResult<String> {
    validate_nzb_submit_source(source, request)?;
    let payload = request_instance_service_json(
        state,
        store,
        resolved.record.instance_id,
        ReqwestMethod::POST,
        "jsonrpc",
        Some(json!({
            "version": "1.1",
            "method": "append",
            "params": [
                "",
                source,
                non_empty(request.category.as_deref())
                    .or_else(|| non_empty(resolved.category.as_deref()))
                    .unwrap_or_default(),
                request.priority.unwrap_or(0),
                request.add_to_top.unwrap_or(false),
                request.paused.unwrap_or(false),
                "",
                0,
                "SCORE",
                false,
                []
            ],
            "id": 1
        })),
    )
    .await
    .map_err(ApiError::from)?;
    ensure_nzbget_rpc_ok(&payload, "append").map_err(ApiError::from)?;
    let id = payload
        .get("result")
        .and_then(Value::as_i64)
        .filter(|value| *value > 0)
        .ok_or_else(|| ApiError::internal("nzbget append did not return a positive NZBID"))?;
    Ok(id.to_string())
}

async fn submit_debrid_broker(
    state: &AppState,
    store: &ExtensionStore<'_>,
    resolved: &ResolvedDownloadBrokerProvider,
    source: &str,
    request: &DownloadBrokerSubmitRequest,
    owner_id: Option<&str>,
) -> ApiResult<String> {
    validate_debrid_source(source)?;
    let category =
        non_empty(request.category.as_deref()).or_else(|| non_empty(resolved.category.as_deref()));
    let release_context = debrid_release_context(request, resolved, owner_id);
    let job_id = submit_debrid(
        state,
        store,
        resolved.record.provider_id,
        resolved.record.instance_id,
        resolved.record.implementation.as_deref(),
        source,
        DebridSubmitOptions {
            owner_id: owner_id.unwrap_or(crate::download_broker::DEFAULT_ROUTE_OWNER_ID),
            category,
            name: non_empty(request.name.as_deref()),
            paused: request.paused.unwrap_or(false),
            release_context,
        },
    )
    .await
    .map_err(|err| {
        let message = err.to_string();
        if message.contains("source must") {
            ApiError::bad_request(message)
        } else if let Some(message) = generic_debrid_error_message(&message) {
            ApiError::conflict(message)
        } else if message.contains("token")
            || message.contains("Debrid provider API")
            || message.contains("Real-Debrid API")
            || message.contains("native adapter")
        {
            ApiError::conflict(message)
        } else {
            ApiError::from(err)
        }
    })?;
    Ok(job_id.to_string())
}

async fn submit_http_stream_broker(
    pool: &sqlx::AnyPool,
    resolved: &ResolvedDownloadBrokerProvider,
    source: &str,
    request: &DownloadBrokerSubmitRequest,
    owner_id: Option<&str>,
) -> ApiResult<String> {
    let Some(raw_candidate) = request.selected_stream_candidate.clone() else {
        return Err(ApiError::bad_request(
            "selectedStreamCandidate is required for the http stream materializer route",
        ));
    };
    let (candidate, warnings) = validate_stream_candidate_for_broker(raw_candidate)
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let candidate_source = stream_candidate_string(&candidate, "/source").ok_or_else(|| {
        ApiError::bad_request("selectedStreamCandidate.source is required for stream submit")
    })?;
    if candidate_source.trim() != source {
        return Err(ApiError::bad_request(
            "request source must match selectedStreamCandidate.source",
        ));
    }
    let media_type = request
        .media_type
        .or_else(|| {
            stream_candidate_string(&candidate, "/targetEvidence/mediaType")
                .and_then(|value| parse_stream_media_type(&value))
        })
        .ok_or_else(|| {
            ApiError::bad_request(
                "mediaType or selectedStreamCandidate.targetEvidence.mediaType is required",
            )
        })?;
    let source_extension_id = request
        .source_extension_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| stream_candidate_string(&candidate, "/sourceModule/id"))
        .or_else(|| {
            owner_id
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        })
        .unwrap_or_else(|| "elixir.extension_suite.stream".to_string());
    let owner_id = owner_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(crate::download_broker::DEFAULT_ROUTE_OWNER_ID)
        .to_string();
    let release_title = stream_candidate_string(&candidate, "/title")
        .ok_or_else(|| ApiError::bad_request("selectedStreamCandidate.title is required"))?;
    let title = request
        .media_title
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .or_else(|| {
            request
                .name
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
        })
        .map(str::to_string)
        .unwrap_or_else(|| release_title.clone());
    let source_kind = stream_candidate_string(&candidate, "/sourceKind")
        .unwrap_or_else(|| "http_stream".to_string());
    let fingerprint = request.release_fingerprint.clone().unwrap_or_else(|| {
        build_http_stream_release_fingerprint(&HttpStreamFingerprintInput {
            source_extension_id: &source_extension_id,
            source_provider_id: request.source_provider_id,
            source,
            source_kind: &source_kind,
            release_title: &release_title,
            candidate: &candidate,
        })
    });
    let download_id = http_stream_download_id(&fingerprint);
    let confidence = stream_candidate_string(&candidate, "/targetEvidence/confidence")
        .as_deref()
        .map(stream_release_confidence)
        .unwrap_or(ReleaseConfidence::Low);
    let score = stream_candidate_f64(&candidate, "/score");
    let delivery_stream_type = stream_candidate_string(&candidate, "/delivery/streamType");
    let target_key = stream_candidate_string(&candidate, "/targetEvidence/targetKey");
    let source_module = stream_candidate_string(&candidate, "/sourceModule/name")
        .or_else(|| stream_candidate_string(&candidate, "/sourceModule/id"));

    let release = upsert_release(
        pool,
        NewAcquisitionRelease {
            release_id: None,
            subscription_id: request.subscription_id,
            source_provider_id: request.source_provider_id,
            source_extension_id: source_extension_id.clone(),
            owner_id: owner_id.clone(),
            media_type,
            title,
            release_title: release_title.clone(),
            source: source.to_string(),
            source_kind: source_kind.clone(),
            info_hash: None,
            fingerprint,
            release_kind: ReleaseKind::Unknown,
            resolver_kind: ReleaseResolverKind::Unresolved,
            resolver_version: HTTP_STREAM_BROKER_RESOLVER_VERSION.to_string(),
            confidence,
            score,
            selected_route_logical_id: Some(HTTP_STREAM_DEFAULT_LOGICAL_ID.to_string()),
            selected_provider_id: None,
            download_id: Some(download_id.clone()),
            remote_release_id: Some(download_id.clone()),
            state: AcquisitionReleaseState::Submitted,
            state_reason: Some(
                "HTTP stream candidate accepted for Elixir-managed materialization.".to_string(),
            ),
            selected_candidate: Some(candidate.clone()),
            coverage_plan: Some(json!({
                "source": "http_stream_broker",
                "resolverVersion": HTTP_STREAM_BROKER_RESOLVER_VERSION,
                "routeOwnerId": owner_id,
                "routeLogicalId": HTTP_STREAM_DEFAULT_LOGICAL_ID,
                "providerId": resolved.record.provider_id,
                "providerImplementation": resolved.record.implementation,
                "materializerState": "submitted",
                "sourceKind": source_kind,
                "streamType": delivery_stream_type,
                "targetKey": target_key,
                "sourceModule": source_module,
                "validationWarnings": warnings
            })),
        },
    )
    .await
    .map_err(ApiError::from)?;

    upsert_release_job(
        pool,
        NewAcquisitionReleaseJob {
            release_job_id: None,
            release_id: release.release_id,
            route_logical_id: HTTP_STREAM_DEFAULT_LOGICAL_ID.to_string(),
            provider_id: None,
            download_id: Some(download_id.clone()),
            remote_release_id: Some(download_id.clone()),
            state: ReleaseJobState::Submitted,
            state_reason: Some("HTTP stream materializer job accepted by the broker.".to_string()),
            active: true,
            started_at: Some(chrono::Utc::now()),
            completed_at: None,
        },
    )
    .await
    .map_err(ApiError::from)?;

    Ok(download_id)
}

pub(crate) fn generic_debrid_error_message(message: &str) -> Option<&'static str> {
    let normalized = message.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return None;
    }
    if normalized.contains(&DEBRID_ACCOUNT_MISSING_MESSAGE.to_ascii_lowercase())
        || normalized.contains("api token is not configured")
        || normalized.contains("token is not configured")
        || normalized.contains("provider_auth_missing")
        || normalized.contains("authentication_failed")
        || normalized.contains("authentication failed")
    {
        Some(DEBRID_ACCOUNT_MISSING_MESSAGE)
    } else if generic_debrid_error_is_not_configured(&normalized) {
        Some(DEBRID_SERVICE_NOT_CONFIGURED_MESSAGE)
    } else if normalized.contains(&DEBRID_SERVICE_UNAVAILABLE_MESSAGE.to_ascii_lowercase())
        || normalized.contains("provider_unavailable")
        || normalized.contains("provider unavailable")
        || normalized.contains("rate_limit")
        || normalized.contains("rate limit")
        || normalized.contains("account_limit")
        || normalized.contains("service_down")
        || normalized.contains("service unavailable")
        || normalized.contains("temporarily unavailable")
        || normalized.contains("native adapter")
        || normalized.contains("debrid provider api")
        || normalized.contains("real-debrid api")
    {
        Some(DEBRID_SERVICE_UNAVAILABLE_MESSAGE)
    } else {
        None
    }
}

fn generic_debrid_error_is_not_configured(message: &str) -> bool {
    let normalized = message.trim().to_ascii_lowercase();
    normalized.contains(&DEBRID_SERVICE_NOT_CONFIGURED_MESSAGE.to_ascii_lowercase())
        || normalized.contains("active debrid service")
        || normalized.contains("debrid service is not configured")
        || normalized.contains("multiple debrid resolver providers")
}

fn debrid_error_is_not_configured(message: &str) -> bool {
    let normalized = message.trim().to_ascii_lowercase();
    generic_debrid_error_is_not_configured(&normalized)
        || normalized.contains("no downloader provider")
        || normalized.contains("no provider is registered")
        || normalized.contains("no acquisition route")
        || normalized.contains("no provider matches binding")
        || normalized.contains("multiple debrid resolver providers")
        || normalized.contains("none is selected")
}

fn debrid_release_context(
    request: &DownloadBrokerSubmitRequest,
    resolved: &ResolvedDownloadBrokerProvider,
    owner_id: Option<&str>,
) -> Option<DebridReleaseSubmitContext> {
    let candidate = request.selected_candidate.as_ref()?;
    let media_type = request.media_type?;
    let source_extension_id = request
        .source_extension_id
        .as_deref()
        .or(owner_id)
        .map(str::trim)
        .filter(|value| !value.is_empty())?
        .to_string();
    let title = request
        .media_title
        .as_deref()
        .or_else(|| non_empty(request.name.as_deref()))
        .unwrap_or(&candidate.title)
        .to_string();
    let mut selected_candidate = broker_selected_candidate_evidence(
        candidate,
        request.request_scope_evidence.as_ref(),
        resolved.record.provider_id,
    );
    if media_type == MediaType::Anime
        && request.defer_anime_debrid_refinement
        && let Some(object) = selected_candidate.as_object_mut()
    {
        object.insert(
            "submissionBookkeeping".to_string(),
            json!({
                "status": "pending",
                "policyVersion": "acquisition-broker-bookkeeping-v1"
            }),
        );
    }
    Some(DebridReleaseSubmitContext {
        subscription_id: request.subscription_id,
        source_provider_id: request.source_provider_id,
        source_extension_id,
        media_type,
        title,
        release_title: candidate.title.clone(),
        info_hash: candidate.info_hash.clone(),
        fingerprint: request.release_fingerprint.clone(),
        score: candidate.score,
        selected_candidate: Some(selected_candidate),
    })
}

#[derive(Debug, Clone)]
struct QbittorrentReleaseSubmitContext {
    subscription_id: Option<Uuid>,
    source_provider_id: Option<Uuid>,
    source_extension_id: String,
    route_owner_id: String,
    media_type: MediaType,
    title: String,
    release_title: String,
    source: String,
    source_kind: String,
    info_hash: Option<String>,
    fingerprint: String,
    score: Option<f64>,
    selected_candidate: Value,
}

fn qbittorrent_release_context(
    request: &DownloadBrokerSubmitRequest,
    resolved: &ResolvedDownloadBrokerProvider,
    source: &str,
    owner_id: Option<&str>,
) -> Option<QbittorrentReleaseSubmitContext> {
    let candidate = request.selected_candidate.as_ref()?;
    let media_type = request.media_type?;
    let source_extension_id = request
        .source_extension_id
        .as_deref()
        .or(owner_id)
        .map(str::trim)
        .filter(|value| !value.is_empty())?
        .to_string();
    let route_owner_id = owner_id
        .unwrap_or(crate::download_broker::DEFAULT_ROUTE_OWNER_ID)
        .trim()
        .to_string();
    let title = request
        .media_title
        .as_deref()
        .or_else(|| non_empty(request.name.as_deref()))
        .unwrap_or(&candidate.title)
        .to_string();
    let source_kind = non_empty(Some(candidate.source_kind.as_str()))
        .unwrap_or_else(|| torrent_source_kind(source))
        .to_string();
    let info_hash = candidate
        .info_hash
        .as_deref()
        .and_then(qbittorrent_trackable_info_hash)
        .or_else(|| qbittorrent_magnet_info_hash(source));
    let fingerprint = request.release_fingerprint.clone().unwrap_or_else(|| {
        build_release_fingerprint(&ReleaseFingerprintInput {
            source_kind: &source_kind,
            source,
            info_hash: info_hash.as_deref(),
            release_title: &candidate.title,
            size_bytes: candidate.size_bytes,
            source_provider_id: request.source_provider_id,
        })
    });
    Some(QbittorrentReleaseSubmitContext {
        subscription_id: request.subscription_id,
        source_provider_id: request.source_provider_id,
        source_extension_id,
        route_owner_id,
        media_type,
        title,
        release_title: candidate.title.clone(),
        source: source.to_string(),
        source_kind,
        info_hash,
        fingerprint,
        score: candidate.score,
        selected_candidate: broker_selected_candidate_evidence(
            candidate,
            request.request_scope_evidence.as_ref(),
            resolved.record.provider_id,
        ),
    })
}

fn broker_selected_candidate_evidence(
    candidate: &AcquisitionCandidate,
    request_scope_evidence: Option<&Value>,
    route_provider_id: Uuid,
) -> Value {
    let mut value = serde_json::to_value(candidate)
        .unwrap_or_else(|_| json!({ "sourceProviderId": route_provider_id }));
    if !value.is_object() {
        value = json!({ "candidate": value });
    }
    if let Some(object) = value.as_object_mut() {
        object.insert(
            "routeProviderId".to_string(),
            json!(route_provider_id.to_string()),
        );
        if let Some(evidence) = request_scope_evidence {
            object.insert("requestScopeEvidence".to_string(), evidence.clone());
        }
    }
    value
}

fn qbittorrent_add_fields(
    source: &str,
    category: Option<&str>,
    request: &DownloadBrokerSubmitRequest,
    force_paused: bool,
) -> HashMap<String, String> {
    let mut fields = HashMap::new();
    fields.insert("urls".to_string(), source.to_string());
    if let Some(category) = category {
        fields.insert("category".to_string(), category.to_string());
    }
    if let Some(name) = non_empty(request.name.as_deref()) {
        fields.insert("rename".to_string(), name.to_string());
    }
    if force_paused {
        if qbittorrent_source_needs_metadata_fetch(source) {
            fields.insert("paused".to_string(), "false".to_string());
            fields.insert("stopCondition".to_string(), "MetadataReceived".to_string());
        } else {
            fields.insert("paused".to_string(), "true".to_string());
        }
    } else if let Some(paused) = request.paused {
        fields.insert("paused".to_string(), paused.to_string());
    }
    fields
}

fn qbittorrent_source_needs_metadata_fetch(source: &str) -> bool {
    let source = source.trim_start();
    source
        .get(..7)
        .map(|scheme| scheme.eq_ignore_ascii_case("magnet:"))
        .unwrap_or(false)
}

fn qbittorrent_trackable_info_hash(value: &str) -> Option<String> {
    let trimmed = value.trim();
    let stripped = trimmed
        .get(..9)
        .filter(|prefix| prefix.eq_ignore_ascii_case("urn:btih:"))
        .map(|_| &trimmed[9..])
        .unwrap_or(trimmed);
    if stripped.len() == 40 && stripped.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Some(stripped.to_ascii_lowercase());
    }
    if stripped.len() != 32
        || !stripped
            .chars()
            .all(|ch| ch.is_ascii_alphabetic() || matches!(ch, '2'..='7'))
    {
        return None;
    }
    let mut accumulator = 0_u32;
    let mut bits = 0_u8;
    let mut decoded = Vec::with_capacity(20);
    for ch in stripped.bytes() {
        let upper = ch.to_ascii_uppercase();
        let value = match upper {
            b'A'..=b'Z' => upper - b'A',
            b'2'..=b'7' => upper - b'2' + 26,
            _ => return None,
        };
        accumulator = (accumulator << 5) | u32::from(value);
        bits += 5;
        while bits >= 8 {
            bits -= 8;
            decoded.push(((accumulator >> bits) & 0xff) as u8);
        }
    }
    if decoded.len() != 20 || bits != 0 {
        return None;
    }
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut canonical = String::with_capacity(40);
    for byte in decoded {
        canonical.push(HEX[(byte >> 4) as usize] as char);
        canonical.push(HEX[(byte & 0x0f) as usize] as char);
    }
    Some(canonical)
}

fn qbittorrent_magnet_info_hash(source: &str) -> Option<String> {
    let trimmed = source.trim();
    if !trimmed
        .get(..7)
        .map(|prefix| prefix.eq_ignore_ascii_case("magnet:"))
        .unwrap_or(false)
    {
        return None;
    }
    let query = trimmed.split_once('?')?.1;
    for pair in query.split('&') {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if !key.eq_ignore_ascii_case("xt") {
            continue;
        }
        let Ok(decoded) = urlencoding::decode(value) else {
            continue;
        };
        if let Some(hash) = qbittorrent_trackable_info_hash(&decoded) {
            return Some(hash);
        }
    }
    None
}

fn qbittorrent_submission_download_id(
    source: &str,
    candidate_info_hash: Option<&str>,
    acquisition_owned: bool,
) -> ApiResult<Option<String>> {
    let canonical_candidate = candidate_info_hash.and_then(qbittorrent_trackable_info_hash);
    if candidate_info_hash.is_some() && canonical_candidate.is_none() {
        return Err(ApiError::bad_request(
            "candidate info hash is malformed or is not a canonical v1 BTIH",
        ));
    }
    let magnet_info_hash = qbittorrent_magnet_info_hash(source);
    if qbittorrent_source_needs_metadata_fetch(source) && magnet_info_hash.is_none() {
        return Err(ApiError::bad_request(
            "qBittorrent magnet submissions require a valid v1 info hash",
        ));
    }
    if let (Some(candidate_hash), Some(magnet_hash)) =
        (canonical_candidate.as_deref(), magnet_info_hash.as_deref())
        && candidate_hash != magnet_hash
    {
        return Err(ApiError::bad_request(
            "candidate info hash does not match the submitted magnet",
        ));
    }
    let download_id = magnet_info_hash.or(canonical_candidate);
    if acquisition_owned && download_id.is_none() {
        return Err(ApiError::bad_request(
            "acquisition-owned qBittorrent submissions require an info hash so Elixir can reconcile staged metadata automatically",
        ));
    }
    Ok(download_id)
}

fn same_optional_qbittorrent_hash(left: Option<&str>, right: Option<&str>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => {
            qbittorrent_trackable_info_hash(left) == qbittorrent_trackable_info_hash(right)
                && qbittorrent_trackable_info_hash(left).is_some()
        }
        (None, None) => true,
        _ => false,
    }
}

fn qbittorrent_torrent_files_path(hash: &str) -> String {
    format!("api/v2/torrents/files?hash={}", urlencoding::encode(hash))
}

fn qbittorrent_torrents_info_path(hash: &str) -> String {
    format!("api/v2/torrents/info?hashes={}", urlencoding::encode(hash))
}

async fn reusable_qbittorrent_release(
    pool: &sqlx::AnyPool,
    context: &QbittorrentReleaseSubmitContext,
    route_logical_id: &str,
) -> anyhow::Result<Option<AcquisitionRelease>> {
    if let Some(release) = get_release_by_fingerprint(
        pool,
        crate::download_broker::DEFAULT_ROUTE_OWNER_ID,
        &context.source_extension_id,
        &context.fingerprint,
    )
    .await?
        && release_is_reusable_for_qbittorrent(pool, &release, route_logical_id).await?
    {
        return Ok(Some(release));
    }
    if let Some(info_hash) = context.info_hash.as_deref()
        && let Some(release) = get_release_by_download_id(pool, info_hash).await?
        && release.source_extension_id == context.source_extension_id
        && release_is_reusable_for_qbittorrent(pool, &release, route_logical_id).await?
    {
        return Ok(Some(release));
    }
    Ok(None)
}

async fn release_is_reusable_for_qbittorrent(
    pool: &sqlx::AnyPool,
    release: &AcquisitionRelease,
    route_logical_id: &str,
) -> anyhow::Result<bool> {
    if matches!(
        release.state,
        AcquisitionReleaseState::Failed
            | AcquisitionReleaseState::Cancelled
            | AcquisitionReleaseState::Completed
    ) {
        return Ok(false);
    }
    if release.download_id.as_deref().is_none() {
        return Ok(false);
    }
    let jobs = list_release_jobs(pool, release.release_id).await?;
    Ok(jobs.into_iter().any(|job| {
        job.active
            && job.route_logical_id == route_logical_id
            && !matches!(
                job.state,
                ReleaseJobState::Failed | ReleaseJobState::Cancelled | ReleaseJobState::Completed
            )
    }))
}

async fn upsert_qbittorrent_acquisition_release(
    pool: &sqlx::AnyPool,
    resolved: &ResolvedDownloadBrokerProvider,
    context: &QbittorrentReleaseSubmitContext,
    category: Option<&str>,
    download_id: Option<&str>,
) -> anyhow::Result<AcquisitionRelease> {
    let info_hash = context
        .info_hash
        .clone()
        .or_else(|| download_id.map(str::to_string));
    upsert_release(
        pool,
        NewAcquisitionRelease {
            release_id: None,
            subscription_id: context.subscription_id,
            source_provider_id: context.source_provider_id,
            source_extension_id: context.source_extension_id.clone(),
            owner_id: crate::download_broker::DEFAULT_ROUTE_OWNER_ID.to_string(),
            media_type: context.media_type,
            title: context.title.clone(),
            release_title: context.release_title.clone(),
            source: context.source.clone(),
            source_kind: context.source_kind.clone(),
            info_hash,
            fingerprint: context.fingerprint.clone(),
            release_kind: ReleaseKind::Unknown,
            resolver_kind: ReleaseResolverKind::Unresolved,
            resolver_version: QBITTORRENT_STAGING_RESOLVER_VERSION.to_string(),
            confidence: ReleaseConfidence::Low,
            score: context.score,
            selected_route_logical_id: Some(resolved.record.logical_id.clone()),
            selected_provider_id: Some(resolved.record.provider_id),
            download_id: download_id.map(str::to_string),
            remote_release_id: download_id.map(str::to_string),
            state: AcquisitionReleaseState::Staging,
            state_reason: Some(
                "qBittorrent torrent staged for deterministic file selection.".to_string(),
            ),
            selected_candidate: Some(context.selected_candidate.clone()),
            coverage_plan: Some(json!({
                "source": "qbittorrent_staged_submit",
                "resolverVersion": QBITTORRENT_STAGING_RESOLVER_VERSION,
                "routeOwnerId": context.route_owner_id,
                "routeLogicalId": resolved.record.logical_id,
                "providerId": resolved.record.provider_id,
                "providerImplementation": resolved.record.implementation,
                "category": category,
                "metadataStopCondition": if qbittorrent_source_needs_metadata_fetch(&context.source) {
                    "metadata_received"
                } else {
                    "already_available"
                },
                "metadataState": "pending",
                "torrentHash": download_id
            })),
        },
    )
    .await
}

async fn upsert_qbittorrent_release_job(
    pool: &sqlx::AnyPool,
    resolved: &ResolvedDownloadBrokerProvider,
    release: &AcquisitionRelease,
    download_id: Option<&str>,
) -> anyhow::Result<()> {
    upsert_release_job(
        pool,
        NewAcquisitionReleaseJob {
            release_job_id: None,
            release_id: release.release_id,
            route_logical_id: resolved.record.logical_id.clone(),
            provider_id: Some(resolved.record.provider_id),
            download_id: download_id.map(str::to_string),
            remote_release_id: download_id.map(str::to_string),
            state: ReleaseJobState::Staging,
            state_reason: Some(
                "qBittorrent metadata pending for staged file selection.".to_string(),
            ),
            active: true,
            started_at: Some(chrono::Utc::now()),
            completed_at: None,
        },
    )
    .await?;
    Ok(())
}

async fn load_qbittorrent_progress(
    state: &AppState,
    store: &ExtensionStore<'_>,
    record: &DownloadBrokerProviderRecord,
) -> ApiResult<Vec<DownloadBrokerProgressItem>> {
    let value = request_qbittorrent_service_json(
        state,
        store,
        record.instance_id,
        ReqwestMethod::GET,
        "api/v2/torrents/info",
        None,
    )
    .await
    .map_err(ApiError::from)?;
    let items = value
        .as_array()
        .ok_or_else(|| ApiError::internal("qbittorrent torrents/info response was not an array"))?;
    let mut staged_releases = HashMap::new();
    for item in items {
        if let Some(hash) = item.get("hash").and_then(Value::as_str)
            && let Some(release) = get_release_by_download_id(&state.db_pool, hash)
                .await
                .map_err(ApiError::from)?
            && release.selected_route_logical_id.as_deref() == Some(record.logical_id.as_str())
            && release.selected_provider_id == Some(record.provider_id)
            && !matches!(
                release.state,
                AcquisitionReleaseState::Completed
                    | AcquisitionReleaseState::Failed
                    | AcquisitionReleaseState::Cancelled
            )
            && let Err(err) =
                refresh_staged_qbittorrent_metadata(state, store, &release, hash, false, false)
                    .await
        {
            tracing::debug!(
                release_id = %release.release_id,
                torrent_hash = hash,
                "qBittorrent staged metadata refresh skipped: {err}"
            );
        }
        if let Some(hash) = item.get("hash").and_then(Value::as_str)
            && let Some(release) = get_release_by_download_id(&state.db_pool, hash)
                .await
                .map_err(ApiError::from)?
            && release.selected_route_logical_id.as_deref() == Some(record.logical_id.as_str())
            && release.selected_provider_id == Some(record.provider_id)
            && !qbittorrent_submission_bookkeeping_pending(&release)
        {
            staged_releases.insert(hash.to_string(), release);
        }
    }
    let mut progress = Vec::new();
    for item in items {
        let Some(id) = item.get("hash").and_then(Value::as_str).map(str::to_string) else {
            continue;
        };
        let torrent = if let Some(release) = staged_releases.get(&id) {
            Some(
                load_qbittorrent_torrent_evidence(&state.db_pool, record, release, item)
                    .await
                    .map_err(ApiError::from)?,
            )
        } else {
            None
        };
        progress.push(DownloadBrokerProgressItem {
            id,
            name: string_field(item, "name"),
            state: string_field(item, "state"),
            category: string_field(item, "category"),
            local_path: string_field(item, "content_path")
                .or_else(|| string_field(item, "save_path")),
            progress: item.get("progress").and_then(Value::as_f64),
            downloaded_bytes: number_field(item, "downloaded"),
            total_bytes: number_field(item, "total_size"),
            remaining_bytes: number_field(item, "amount_left"),
            download_rate_bps: number_field(item, "dlspeed"),
            upload_rate_bps: number_field(item, "upspeed"),
            debrid: None,
            torrent,
            stream: None,
        });
    }
    Ok(progress)
}

pub(crate) async fn process_stale_qbittorrent_acquisition_releases(
    state: &AppState,
    limit: i64,
) -> anyhow::Result<usize> {
    let store = ExtensionStore::new(&state.db_pool);
    reconcile_qbittorrent_terminal_release_jobs(
        &state.db_pool,
        limit.clamp(1, QBITTORRENT_STALE_RELEASE_BATCH_LIMIT),
    )
    .await?;
    replay_pending_anime_qbittorrent_cleanup(
        state,
        &store,
        limit.clamp(1, QBITTORRENT_STALE_RELEASE_BATCH_LIMIT),
    )
    .await?;
    let now = chrono::Utc::now();
    let releases = list_active_releases_by_route(
        &state.db_pool,
        TORRENT_DEFAULT_LOGICAL_ID,
        limit.clamp(1, QBITTORRENT_STALE_RELEASE_BATCH_LIMIT),
    )
    .await?;
    let mut failed = 0usize;

    for mut release in releases {
        if qbittorrent_submission_bookkeeping_pending(&release) {
            let Some(torrent_hash) = release.download_id.as_deref() else {
                continue;
            };
            if reconcile_qbittorrent_submission_bookkeeping(&state.db_pool, &release, torrent_hash)
                .await?
            {
                let Some(reconciled) = get_release(&state.db_pool, release.release_id).await?
                else {
                    continue;
                };
                release = reconciled;
            } else if !qbittorrent_anime_provider_operation_retry_eligible(&release) {
                continue;
            }
        }
        let Some(torrent_hash) = release.download_id.clone() else {
            continue;
        };
        let Some(provider_id) = release.selected_provider_id else {
            continue;
        };
        let torrent_info = match load_qbittorrent_torrent_info_for_release(
            state,
            &store,
            provider_id,
            &torrent_hash,
        )
        .await
        {
            Ok(Some(value)) => value,
            Ok(None) => {
                if retry_aged_anime_qbittorrent_provider_failure(
                    state,
                    &store,
                    release.release_id,
                    &torrent_hash,
                    QbittorrentProviderOperation::TorrentInspection,
                    now,
                )
                .await?
                {
                    failed += 1;
                }
                continue;
            }
            Err(err) => {
                let retried = retry_aged_anime_qbittorrent_provider_failure(
                    state,
                    &store,
                    release.release_id,
                    &torrent_hash,
                    QbittorrentProviderOperation::TorrentInspection,
                    now,
                )
                .await?;
                if retried {
                    failed += 1;
                    tracing::warn!(
                        release_id = %release.release_id,
                        torrent_hash = torrent_hash,
                        "qBittorrent inspection remained unavailable through the attempt window; anime release scheduled for automatic retry: {err}"
                    );
                } else {
                    tracing::debug!(
                        release_id = %release.release_id,
                        torrent_hash = torrent_hash,
                        "qBittorrent stale-release inspection skipped: {err}"
                    );
                }
                continue;
            }
        };
        let state_before_refresh = release.state;
        if let Err(err) =
            refresh_staged_qbittorrent_metadata(state, &store, &release, &torrent_hash, false, true)
                .await
        {
            let retried = retry_aged_anime_qbittorrent_provider_failure(
                state,
                &store,
                release.release_id,
                &torrent_hash,
                QbittorrentProviderOperation::FilePolicyRefresh,
                now,
            )
            .await?;
            if retried {
                failed += 1;
                tracing::warn!(
                    release_id = %release.release_id,
                    torrent_hash = torrent_hash,
                    "qBittorrent file-policy refresh kept failing through the attempt window; anime release scheduled for automatic retry: {err}"
                );
            } else {
                tracing::debug!(
                    release_id = %release.release_id,
                    torrent_hash = torrent_hash,
                    "qBittorrent stale-release metadata refresh skipped: {err}"
                );
            }
            continue;
        }
        let Some(release) = get_release(&state.db_pool, release.release_id).await? else {
            continue;
        };
        if matches!(
            release.state,
            AcquisitionReleaseState::Failed
                | AcquisitionReleaseState::Cancelled
                | AcquisitionReleaseState::Completed
        ) {
            continue;
        }
        if release.state != state_before_refresh || release.state == AcquisitionReleaseState::Ready
        {
            continue;
        }
        let files = list_release_files(&state.db_pool, release.release_id).await?;
        let attempt_started_at = qbittorrent_active_attempt_started_at(
            &state.db_pool,
            release.release_id,
            &torrent_hash,
        )
        .await?;
        let Some(decision) = qbittorrent_stale_release_decision(
            now,
            &release,
            &files,
            &torrent_info,
            attempt_started_at,
        ) else {
            continue;
        };
        mark_qbittorrent_release_stale_for_retry(
            &state.db_pool,
            &release,
            &torrent_hash,
            &torrent_info,
            decision,
            now,
        )
        .await?;
        let cleanup_result = if release.media_type == MediaType::Anime {
            cleanup_qbittorrent_retry_torrent(state, &store, &release, &torrent_hash).await
        } else {
            remove_stale_qbittorrent_runtime_torrent(state, &store, &release, &torrent_hash).await
        };
        if let Err(err) = cleanup_result {
            tracing::warn!(
                release_id = %release.release_id,
                torrent_hash = torrent_hash,
                "failed to remove stale qBittorrent runtime torrent after release retry: {err}"
            );
        }
        failed += 1;
    }

    Ok(failed)
}

async fn reconcile_qbittorrent_terminal_release_jobs(
    pool: &sqlx::AnyPool,
    limit: i64,
) -> anyhow::Result<usize> {
    let rows = sqlx::query(
        "SELECT CAST(j.release_job_id AS TEXT) AS release_job_id,
                r.state AS release_state,
                CAST(r.state_reason AS TEXT) AS state_reason
         FROM acquisition_release_jobs j
         JOIN acquisition_releases r ON r.release_id = j.release_id
         WHERE j.active = 1
           AND j.route_logical_id = $1
           AND r.state IN ('completed', 'failed', 'cancelled')
         ORDER BY j.updated_at ASC
         LIMIT $2",
    )
    .bind(TORRENT_DEFAULT_LOGICAL_ID)
    .bind(limit.clamp(1, QBITTORRENT_STALE_RELEASE_BATCH_LIMIT))
    .fetch_all(pool)
    .await?;
    let mut reconciled = 0usize;
    for row in rows {
        let release_job_id: String = row.try_get("release_job_id")?;
        let release_state: String = row.try_get("release_state")?;
        let state_reason: Option<String> = row.try_get("state_reason")?;
        let updated = sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_release_jobs
             SET state = $1,
                 state_reason = $2,
                 active = 0,
                 completed_at = COALESCE(completed_at, CURRENT_TIMESTAMP),
                 updated_at = CURRENT_TIMESTAMP
             WHERE release_job_id = $3
               AND active = 1",
        )
        .bind(release_state)
        .bind(state_reason.as_deref())
        .bind(release_job_id)
        .execute(pool)
        .await?;
        reconciled += updated.rows_affected() as usize;
    }
    Ok(reconciled)
}

async fn remove_stale_qbittorrent_runtime_torrent(
    state: &AppState,
    store: &ExtensionStore<'_>,
    release: &AcquisitionRelease,
    torrent_hash: &str,
) -> anyhow::Result<()> {
    let provider_id = release
        .selected_provider_id
        .context("stale qBittorrent release is missing selected provider")?;
    let provider = store
        .get_provider(provider_id)
        .await?
        .context("stale qBittorrent provider is missing")?;
    let fields = qbittorrent_delete_fields(torrent_hash, false);
    request_qbittorrent_service_form(
        state,
        store,
        provider.instance_id,
        "api/v2/torrents/delete",
        &fields,
    )
    .await
}

async fn cleanup_qbittorrent_retry_torrent(
    state: &AppState,
    store: &ExtensionStore<'_>,
    release: &AcquisitionRelease,
    torrent_hash: &str,
) -> anyhow::Result<()> {
    let canonical_hash = qbittorrent_trackable_info_hash(torrent_hash)
        .context("anime qBittorrent cleanup has no canonical info hash")?;
    let current = get_release(&state.db_pool, release.release_id)
        .await?
        .context("anime qBittorrent cleanup release is missing")?;
    if current.state != AcquisitionReleaseState::Failed
        || !anime_qbittorrent_cleanup_pending(&current)
    {
        return Ok(());
    }
    let current_hash = current
        .download_id
        .as_deref()
        .and_then(qbittorrent_trackable_info_hash)
        .context("anime qBittorrent cleanup release has no canonical download hash")?;
    let marker_hash = current
        .coverage_plan
        .as_ref()
        .and_then(|plan| plan.pointer("/torrentCleanup/torrentHash"))
        .and_then(Value::as_str)
        .and_then(qbittorrent_trackable_info_hash)
        .context("anime qBittorrent cleanup marker has no canonical torrent hash")?;
    if current_hash != canonical_hash || marker_hash != canonical_hash {
        anyhow::bail!("anime qBittorrent cleanup hash no longer owns the current release attempt");
    }
    reconcile_anime_qbittorrent_retry_bookkeeping(
        &state.db_pool,
        current.release_id,
        &canonical_hash,
    )
    .await?;
    remove_stale_qbittorrent_runtime_torrent(state, store, &current, &canonical_hash).await?;
    mark_anime_qbittorrent_cleanup_completed(&state.db_pool, current.release_id, &canonical_hash)
        .await
}

async fn cleanup_pending_qbittorrent_hash_attempts(
    state: &AppState,
    store: &ExtensionStore<'_>,
    torrent_hash: &str,
) -> anyhow::Result<()> {
    let canonical_hash = qbittorrent_trackable_info_hash(torrent_hash)
        .context("qBittorrent submission has no canonical torrent hash")?;
    let rows = sqlx::query(
        "SELECT CAST(release_id AS TEXT) AS release_id
         FROM acquisition_releases
         WHERE selected_route_logical_id = $1
           AND media_type = 'anime'
           AND state = 'failed'
           AND LOWER(download_id) = LOWER($2)
           AND coverage_plan_json LIKE '%\"torrentCleanup\"%'
           AND coverage_plan_json LIKE '%\"status\":\"pending\"%'",
    )
    .bind(TORRENT_DEFAULT_LOGICAL_ID)
    .bind(&canonical_hash)
    .fetch_all(&state.db_pool)
    .await?;
    for row in rows {
        let raw: String = row.try_get("release_id")?;
        let release_id = Uuid::parse_str(&raw)
            .with_context(|| format!("invalid qBittorrent cleanup release id '{raw}'"))?;
        let release = get_release(&state.db_pool, release_id)
            .await?
            .context("pending qBittorrent cleanup release disappeared")?;
        cleanup_qbittorrent_retry_torrent(state, store, &release, &canonical_hash).await?;
    }
    let pending: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM acquisition_releases
         WHERE selected_route_logical_id = $1
           AND media_type = 'anime'
           AND state = 'failed'
           AND LOWER(download_id) = LOWER($2)
           AND coverage_plan_json LIKE '%\"torrentCleanup\"%'
           AND coverage_plan_json LIKE '%\"status\":\"pending\"%'",
    )
    .bind(TORRENT_DEFAULT_LOGICAL_ID)
    .bind(&canonical_hash)
    .fetch_one(&state.db_pool)
    .await?;
    if pending != 0 {
        anyhow::bail!(
            "qBittorrent torrent {canonical_hash} is still owned by pending automatic cleanup"
        );
    }
    Ok(())
}

async fn replay_pending_anime_qbittorrent_cleanup(
    state: &AppState,
    store: &ExtensionStore<'_>,
    limit: i64,
) -> anyhow::Result<usize> {
    let rows = sqlx::query(
        "SELECT CAST(release_id AS TEXT) AS release_id
         FROM acquisition_releases
         WHERE selected_route_logical_id = $1
           AND media_type = 'anime'
           AND state = 'failed'
           AND coverage_plan_json LIKE '%\"torrentCleanup\"%'
           AND coverage_plan_json LIKE '%\"status\":\"pending\"%'
         ORDER BY updated_at ASC
         LIMIT $2",
    )
    .bind(TORRENT_DEFAULT_LOGICAL_ID)
    .bind(limit.clamp(1, QBITTORRENT_STALE_RELEASE_BATCH_LIMIT))
    .fetch_all(&state.db_pool)
    .await?;
    let mut completed = 0usize;
    for row in rows {
        let raw: String = row.try_get("release_id")?;
        let Ok(release_id) = Uuid::parse_str(&raw) else {
            continue;
        };
        let Some(release) = get_release(&state.db_pool, release_id).await? else {
            continue;
        };
        if !anime_qbittorrent_cleanup_pending(&release) {
            continue;
        }
        let Some(torrent_hash) = release
            .coverage_plan
            .as_ref()
            .and_then(|plan| plan.pointer("/torrentCleanup/torrentHash"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .or_else(|| {
                release
                    .download_id
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string)
            })
        else {
            continue;
        };
        match cleanup_qbittorrent_retry_torrent(state, store, &release, &torrent_hash).await {
            Ok(()) => completed += 1,
            Err(err) => tracing::warn!(
                release_id = %release.release_id,
                torrent_hash,
                "pending anime qBittorrent cleanup will be retried: {err}"
            ),
        }
    }
    Ok(completed)
}

async fn load_qbittorrent_torrent_info_for_release(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider_id: Uuid,
    torrent_hash: &str,
) -> anyhow::Result<Option<Value>> {
    let provider = store
        .get_provider(provider_id)
        .await?
        .ok_or_else(|| anyhow!("qBittorrent provider {provider_id} is missing"))?;
    if provider.implementation.as_deref() != Some("qbittorrent") {
        return Ok(None);
    }
    let value = request_qbittorrent_service_json(
        state,
        store,
        provider.instance_id,
        ReqwestMethod::GET,
        &qbittorrent_torrents_info_path(torrent_hash),
        None,
    )
    .await
    .map_err(|err| anyhow!(err.to_string()))?;
    let rows = value
        .as_array()
        .context("qbittorrent torrents/info response was not an array")?;
    Ok(exact_qbittorrent_torrent_info(rows, torrent_hash))
}

fn exact_qbittorrent_torrent_info(rows: &[Value], torrent_hash: &str) -> Option<Value> {
    let canonical_hash = qbittorrent_trackable_info_hash(torrent_hash)?;
    rows.iter()
        .find(|row| {
            string_field(row, "hash")
                .as_deref()
                .and_then(qbittorrent_trackable_info_hash)
                .as_deref()
                == Some(canonical_hash.as_str())
        })
        .cloned()
}

fn reserve_qbittorrent_metadata_poll(release_id: Uuid, force: bool) -> bool {
    let now = Instant::now();
    let mut states = QBITTORRENT_METADATA_POLL_STATE
        .lock()
        .expect("qBittorrent metadata poll state poisoned");
    let Some(state) = states.get_mut(&release_id) else {
        states.insert(
            release_id,
            QbittorrentMetadataPollState {
                attempts: 1,
                last_attempt: now,
            },
        );
        return true;
    };
    if force
        || now.duration_since(state.last_attempt) >= qbittorrent_metadata_backoff(state.attempts)
    {
        state.attempts = state.attempts.saturating_add(1);
        state.last_attempt = now;
        return true;
    }
    false
}

fn finish_qbittorrent_metadata_poll(release_id: Uuid, persisted_files: usize) {
    if persisted_files == 0 {
        return;
    }
    QBITTORRENT_METADATA_POLL_STATE
        .lock()
        .expect("qBittorrent metadata poll state poisoned")
        .remove(&release_id);
}

fn qbittorrent_metadata_backoff(attempts: u32) -> Duration {
    let shift = attempts.saturating_sub(1).min(6);
    let seconds = QBITTORRENT_METADATA_BACKOFF_INITIAL_SECONDS
        .saturating_mul(1_u64 << shift)
        .min(QBITTORRENT_METADATA_BACKOFF_MAX_SECONDS);
    Duration::from_secs(seconds)
}

async fn load_qbittorrent_torrent_evidence(
    pool: &sqlx::AnyPool,
    record: &DownloadBrokerProviderRecord,
    release: &AcquisitionRelease,
    torrent_info: &Value,
) -> anyhow::Result<DownloadBrokerTorrentEvidence> {
    let torrent_hash = string_field(torrent_info, "hash")
        .or_else(|| release.download_id.clone())
        .unwrap_or_default();
    sync_qbittorrent_release_file_runtime_paths(pool, release, torrent_info).await?;
    let mut release = get_release_by_download_id(pool, &torrent_hash)
        .await?
        .unwrap_or_else(|| release.clone());
    let mut files = list_release_files(pool, release.release_id).await?;
    let runtime_state = qbittorrent_runtime_state(&release, &files, torrent_info);
    let mut evidence =
        qbittorrent_torrent_evidence(record, &release, &files, torrent_info, &runtime_state);

    if runtime_state == "completed" && release.state != AcquisitionReleaseState::Completed {
        let coverage_plan =
            merge_qbittorrent_runtime_evidence(release.coverage_plan.clone(), &evidence);
        mark_qbittorrent_release_completed(
            pool,
            &release,
            &torrent_hash,
            "qBittorrent reported the selected torrent files completed.",
            coverage_plan,
        )
        .await?;
        queue_qbittorrent_anime_hashes_for_completed_release(pool, &release, &files).await?;
        release = get_release_by_download_id(pool, &torrent_hash)
            .await?
            .unwrap_or(release);
        files = list_release_files(pool, release.release_id).await?;
        evidence =
            qbittorrent_torrent_evidence(record, &release, &files, torrent_info, "completed");
    } else if runtime_state == "failed" && release.state != AcquisitionReleaseState::Failed {
        let coverage_plan =
            merge_qbittorrent_runtime_evidence(release.coverage_plan.clone(), &evidence);
        let provider_reason = qbittorrent_failure_reason(torrent_info).unwrap_or_else(|| {
            "qBittorrent reported a failed torrent state for the staged release.".to_string()
        });
        if release.media_type == MediaType::Anime {
            let retry_reason = format!(
                "{} Elixir will try the next release automatically.",
                provider_reason.trim_end_matches('.')
            );
            persist_anime_qbittorrent_runtime_retry(
                pool,
                &release,
                &torrent_hash,
                coverage_plan,
                "anime_qbittorrent_runtime_failed",
                &retry_reason,
            )
            .await?;
            release = get_release_by_download_id(pool, &torrent_hash)
                .await?
                .unwrap_or(release);
            files = list_release_files(pool, release.release_id).await?;
            evidence =
                qbittorrent_torrent_evidence(record, &release, &files, torrent_info, "failed");
        } else {
            mark_qbittorrent_release_failed(
                pool,
                &release,
                &torrent_hash,
                &provider_reason,
                coverage_plan,
            )
            .await?;
        }
    } else {
        let current_runtime_state = release
            .coverage_plan
            .as_ref()
            .and_then(|value| value.get("torrentRuntime"))
            .and_then(|value| value.get("runtimeState"))
            .and_then(Value::as_str);
        let current_metadata_state = release
            .coverage_plan
            .as_ref()
            .and_then(|value| value.get("torrentRuntime"))
            .and_then(|value| value.get("metadataState"))
            .and_then(Value::as_str);
        if current_runtime_state != Some(evidence.runtime_state.as_str())
            || current_metadata_state != Some(evidence.metadata_state.as_str())
        {
            record_qbittorrent_runtime_evidence(pool, &release, &evidence).await?;
        }
    }

    Ok(evidence)
}

async fn sync_qbittorrent_release_file_runtime_paths(
    pool: &sqlx::AnyPool,
    release: &AcquisitionRelease,
    torrent_info: &Value,
) -> anyhow::Result<()> {
    let save_path = string_field(torrent_info, "save_path");
    let content_path = string_field(torrent_info, "content_path");
    if save_path.is_none() && content_path.is_none() {
        return Ok(());
    }
    let files = list_release_files(pool, release.release_id).await?;
    let selected_count = files
        .iter()
        .filter(|file| file.selected == Some(true))
        .count();
    let completed = qbittorrent_torrent_is_complete(torrent_info);
    for file in files.iter().filter(|file| file.selected == Some(true)) {
        let Some(local_path) = qbittorrent_local_file_path(
            &file.path,
            save_path.as_deref(),
            content_path.as_deref(),
            selected_count,
        ) else {
            continue;
        };
        let mut metadata = file.provider_metadata.clone().unwrap_or_else(|| json!({}));
        if !metadata.is_object() {
            metadata = json!({ "previousProviderMetadata": metadata });
        }
        if let Value::Object(ref mut object) = metadata {
            object.insert("localPath".to_string(), json!(local_path));
            object.insert("torrentSavePath".to_string(), json!(save_path));
            object.insert("torrentContentPath".to_string(), json!(content_path));
            object.insert("completed".to_string(), json!(completed));
        }
        update_release_file_provider_metadata(pool, file.release_file_id, metadata).await?;
    }
    Ok(())
}

fn qbittorrent_local_file_path(
    file_path: &str,
    save_path: Option<&str>,
    content_path: Option<&str>,
    selected_count: usize,
) -> Option<String> {
    let file_path = file_path.trim().replace('\\', "/");
    if file_path.is_empty() {
        return None;
    }
    let file = FsPath::new(&file_path);
    if file.is_absolute() {
        return Some(file_path);
    }

    let basename = file.file_name().and_then(|value| value.to_str());
    if let Some(content_path) = content_path.and_then(|value| non_empty(Some(value))) {
        let content = FsPath::new(content_path);
        let content_basename = content.file_name().and_then(|value| value.to_str());
        if selected_count == 1 && basename.is_some() && basename == content_basename {
            return Some(content_path.to_string());
        }
        if let Some(root) = file_path
            .split('/')
            .next()
            .filter(|value| !value.is_empty())
            && Some(root) == content_basename
            && let Some(parent) = content.parent()
        {
            return Some(path_join_string(parent, FsPath::new(&file_path)));
        }
        if content.extension().is_none() {
            return Some(path_join_string(content, file));
        }
    }

    save_path
        .and_then(|value| non_empty(Some(value)))
        .map(|base| path_join_string(FsPath::new(base), file))
}

fn path_join_string(base: &FsPath, relative: &FsPath) -> String {
    base.join(relative).to_string_lossy().to_string()
}

fn qbittorrent_runtime_state(
    release: &AcquisitionRelease,
    files: &[AcquisitionReleaseFile],
    torrent_info: &Value,
) -> String {
    if release.state == AcquisitionReleaseState::Cancelled {
        return "cancelled".to_string();
    }
    if release.state == AcquisitionReleaseState::Failed
        || qbittorrent_torrent_is_failed(torrent_info)
    {
        return "failed".to_string();
    }
    if release.state == AcquisitionReleaseState::Completed
        || qbittorrent_torrent_is_complete(torrent_info)
    {
        return "completed".to_string();
    }
    if release.state == AcquisitionReleaseState::ReviewRequired {
        return "review_required".to_string();
    }
    if files.is_empty() || qbittorrent_state(torrent_info).as_deref() == Some("metadl") {
        return "waiting_metadata".to_string();
    }
    let Some(policy) = qbittorrent_priority_policy(release) else {
        return "files_available".to_string();
    };
    if policy.get("status").and_then(Value::as_str) == Some("review_required") {
        return "review_required".to_string();
    }
    if policy
        .get("priorityApplied")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        if qbittorrent_torrent_is_paused(torrent_info) {
            return "priority_applied".to_string();
        }
        return "downloading".to_string();
    }
    if policy.get("status").and_then(Value::as_str) == Some("approved") {
        return "ready".to_string();
    }
    "staging".to_string()
}

fn qbittorrent_torrent_evidence(
    record: &DownloadBrokerProviderRecord,
    release: &AcquisitionRelease,
    files: &[AcquisitionReleaseFile],
    torrent_info: &Value,
    runtime_state: &str,
) -> DownloadBrokerTorrentEvidence {
    let policy = qbittorrent_priority_policy(release);
    let selected_ids = policy
        .and_then(|value| value.get("selectedFileIds"))
        .and_then(Value::as_array)
        .map(|values| values.iter().filter_map(Value::as_str).count())
        .unwrap_or_else(|| {
            files
                .iter()
                .filter(|file| file.selected == Some(true))
                .count()
        });
    let skipped_ids = policy
        .and_then(|value| value.get("skippedFileIds"))
        .and_then(Value::as_array)
        .map(|values| values.iter().filter_map(Value::as_str).count())
        .unwrap_or_else(|| {
            files
                .iter()
                .filter(|file| file.selected == Some(false))
                .count()
        });
    let review_reasons = policy
        .and_then(|value| value.get("reviewReasons"))
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .filter(|values| !values.is_empty())
        .or_else(|| {
            (release.state == AcquisitionReleaseState::ReviewRequired)
                .then(|| release.state_reason.clone())
                .flatten()
                .map(|reason| vec![reason])
        })
        .unwrap_or_default();
    let priority_applied = policy
        .and_then(|value| value.get("priorityApplied"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let priority_state = policy
        .and_then(|value| value.get("status"))
        .and_then(Value::as_str)
        .map(|status| {
            if priority_applied && status == "approved" {
                "applied".to_string()
            } else {
                status.to_string()
            }
        })
        .unwrap_or_else(|| "pending".to_string());
    DownloadBrokerTorrentEvidence {
        provider_name: Some("qBittorrent".to_string()),
        provider_implementation: record.implementation.clone(),
        torrent_hash: string_field(torrent_info, "hash")
            .or_else(|| release.download_id.clone())
            .unwrap_or_default(),
        runtime_state: runtime_state.to_string(),
        metadata_state: if files.is_empty() {
            "waiting_metadata".to_string()
        } else {
            "files_available".to_string()
        },
        priority_state,
        selected_file_count: selected_ids,
        skipped_file_count: skipped_ids,
        review_reasons,
        policy_version: policy
            .and_then(|value| value.get("policyVersion"))
            .and_then(Value::as_str)
            .map(str::to_string),
        coverage_fingerprint: policy
            .and_then(|value| value.get("coverageFingerprint"))
            .and_then(Value::as_str)
            .map(str::to_string),
        route_owner_id: release
            .coverage_plan
            .as_ref()
            .and_then(|value| value.get("routeOwnerId"))
            .and_then(Value::as_str)
            .map(str::to_string),
        route_logical_id: release.selected_route_logical_id.clone(),
        category: string_field(torrent_info, "category").or_else(|| {
            release
                .coverage_plan
                .as_ref()
                .and_then(|value| value.get("category"))
                .and_then(Value::as_str)
                .map(str::to_string)
        }),
        source_extension_id: release.source_extension_id.clone(),
        source_provider_id: release.source_provider_id,
        candidate_title: release
            .selected_candidate
            .as_ref()
            .and_then(|value| value.get("title"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| Some(release.release_title.clone())),
        priority_applied,
        user_approved: policy
            .and_then(|value| value.get("userApproved"))
            .and_then(Value::as_bool)
            .unwrap_or(false),
        blocker: None,
        failure_state: qbittorrent_failure_reason(torrent_info),
    }
}

fn qbittorrent_priority_policy(release: &AcquisitionRelease) -> Option<&Value> {
    release
        .coverage_plan
        .as_ref()
        .and_then(|value| value.get("priorityPolicy"))
}

fn qbittorrent_state(torrent_info: &Value) -> Option<String> {
    string_field(torrent_info, "state").map(|value| value.to_ascii_lowercase())
}

fn qbittorrent_torrent_is_complete(torrent_info: &Value) -> bool {
    if torrent_info
        .get("progress")
        .and_then(Value::as_f64)
        .map(|value| value >= 0.9999)
        .unwrap_or(false)
    {
        return true;
    }
    matches!(
        qbittorrent_state(torrent_info).as_deref(),
        Some("uploading" | "stalledup" | "pausedup" | "queuedup" | "checkingup" | "forcedup")
    )
}

fn qbittorrent_torrent_is_failed(torrent_info: &Value) -> bool {
    qbittorrent_state(torrent_info)
        .map(|state| state.contains("error") || state.contains("missing"))
        .unwrap_or(false)
}

fn qbittorrent_torrent_is_paused(torrent_info: &Value) -> bool {
    qbittorrent_state(torrent_info)
        .map(|state| state.starts_with("paused"))
        .unwrap_or(false)
}

fn qbittorrent_failure_reason(torrent_info: &Value) -> Option<String> {
    qbittorrent_torrent_is_failed(torrent_info).then(|| {
        format!(
            "qBittorrent reported state '{}'.",
            string_field(torrent_info, "state").unwrap_or_else(|| "unknown".to_string())
        )
    })
}

async fn qbittorrent_active_attempt_started_at(
    pool: &sqlx::AnyPool,
    release_id: Uuid,
    torrent_hash: &str,
) -> anyhow::Result<Option<chrono::DateTime<chrono::Utc>>> {
    Ok(list_release_jobs(pool, release_id)
        .await?
        .into_iter()
        .filter(|job| {
            job.active
                && job.route_logical_id == TORRENT_DEFAULT_LOGICAL_ID
                && job
                    .download_id
                    .as_deref()
                    .map(|value| value.eq_ignore_ascii_case(torrent_hash))
                    .unwrap_or(false)
        })
        .filter_map(|job| job.started_at)
        .max())
}

fn qbittorrent_provider_operation_failure_decision(
    now: chrono::DateTime<chrono::Utc>,
    release: &AcquisitionRelease,
    attempt_started_at: Option<chrono::DateTime<chrono::Utc>>,
    operation: QbittorrentProviderOperation,
) -> Option<QbittorrentStaleReleaseDecision> {
    if !qbittorrent_anime_provider_operation_retry_eligible(release) {
        return None;
    }
    let age_seconds = now
        .signed_duration_since(attempt_started_at.unwrap_or(release.created_at))
        .num_seconds()
        .max(0);
    (age_seconds >= QBITTORRENT_METADATA_TIMEOUT_SECONDS).then(|| {
        QbittorrentStaleReleaseDecision::provider_operation_timeout(age_seconds, operation)
    })
}

fn qbittorrent_anime_provider_operation_retry_eligible(release: &AcquisitionRelease) -> bool {
    release.media_type == MediaType::Anime
        && matches!(
            release.state,
            AcquisitionReleaseState::Staging
                | AcquisitionReleaseState::Submitted
                | AcquisitionReleaseState::Ready
        )
}

async fn terminalize_aged_anime_qbittorrent_provider_failure(
    pool: &sqlx::AnyPool,
    release_id: Uuid,
    torrent_hash: &str,
    operation: QbittorrentProviderOperation,
    now: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<Option<AcquisitionRelease>> {
    let Some(release) = get_release(pool, release_id).await? else {
        return Ok(None);
    };
    let attempt_started_at =
        qbittorrent_active_attempt_started_at(pool, release.release_id, torrent_hash).await?;
    let Some(decision) = qbittorrent_provider_operation_failure_decision(
        now,
        &release,
        attempt_started_at,
        operation,
    ) else {
        return Ok(None);
    };
    let torrent_info = json!({
        "hash": torrent_hash,
        "state": operation.evidence_state(),
    });
    mark_qbittorrent_release_stale_for_retry(
        pool,
        &release,
        torrent_hash,
        &torrent_info,
        decision,
        now,
    )
    .await?;
    Ok(Some(release))
}

async fn retry_aged_anime_qbittorrent_provider_failure(
    state: &AppState,
    store: &ExtensionStore<'_>,
    release_id: Uuid,
    torrent_hash: &str,
    operation: QbittorrentProviderOperation,
    now: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<bool> {
    let Some(release) = terminalize_aged_anime_qbittorrent_provider_failure(
        &state.db_pool,
        release_id,
        torrent_hash,
        operation,
        now,
    )
    .await?
    else {
        return Ok(false);
    };
    if let Err(err) = cleanup_qbittorrent_retry_torrent(state, store, &release, torrent_hash).await
    {
        tracing::warn!(
            release_id = %release.release_id,
            torrent_hash,
            "failed to remove qBittorrent torrent after provider-operation retry was scheduled: {err}"
        );
    }
    Ok(true)
}

fn qbittorrent_stale_release_decision(
    now: chrono::DateTime<chrono::Utc>,
    release: &AcquisitionRelease,
    files: &[AcquisitionReleaseFile],
    torrent_info: &Value,
    attempt_started_at: Option<chrono::DateTime<chrono::Utc>>,
) -> Option<QbittorrentStaleReleaseDecision> {
    if matches!(
        release.state,
        AcquisitionReleaseState::Completed
            | AcquisitionReleaseState::Failed
            | AcquisitionReleaseState::Cancelled
            | AcquisitionReleaseState::ReviewRequired
    ) {
        return None;
    }
    let age_seconds = now
        .signed_duration_since(attempt_started_at.unwrap_or(release.created_at))
        .num_seconds()
        .max(0);
    if qbittorrent_waiting_for_metadata(files, torrent_info)
        && age_seconds >= QBITTORRENT_METADATA_TIMEOUT_SECONDS
    {
        return Some(QbittorrentStaleReleaseDecision::metadata_timeout(
            age_seconds,
        ));
    }
    if qbittorrent_zero_seed_stall(torrent_info)
        && age_seconds >= QBITTORRENT_ZERO_SEED_STALL_TIMEOUT_SECONDS
    {
        return Some(QbittorrentStaleReleaseDecision::zero_seed_stall(
            age_seconds,
        ));
    }
    None
}

fn qbittorrent_waiting_for_metadata(
    files: &[AcquisitionReleaseFile],
    torrent_info: &Value,
) -> bool {
    files.is_empty() || qbittorrent_state(torrent_info).as_deref() == Some("metadl")
}

fn qbittorrent_zero_seed_stall(torrent_info: &Value) -> bool {
    if qbittorrent_state(torrent_info).as_deref() != Some("stalleddl") {
        return false;
    }
    if numeric_u64_field(torrent_info, "dlspeed").unwrap_or(0) != 0 {
        return false;
    }
    if torrent_info
        .get("progress")
        .and_then(Value::as_f64)
        .map(|progress| progress >= 0.9999)
        .unwrap_or(false)
    {
        return false;
    }
    let connected_seeds = numeric_i64_field(torrent_info, "num_seeds").unwrap_or(0);
    let complete_seeds = numeric_i64_field(torrent_info, "num_complete").unwrap_or(0);
    let availability = numeric_f64_field(torrent_info, "availability");
    let no_complete_seed = connected_seeds <= 0 && complete_seeds <= 0;
    let low_availability = availability.map(|value| value < 1.0).unwrap_or(false);
    no_complete_seed || (complete_seeds <= 0 && low_availability)
}

async fn mark_qbittorrent_release_stale_for_retry(
    pool: &sqlx::AnyPool,
    release: &AcquisitionRelease,
    torrent_hash: &str,
    torrent_info: &Value,
    decision: QbittorrentStaleReleaseDecision,
    now: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<usize> {
    let coverage_plan = merge_qbittorrent_stale_failure_evidence(
        release.coverage_plan.clone(),
        torrent_hash,
        torrent_info,
        &decision,
        now,
    );
    if release.media_type == MediaType::Anime {
        let retry_reason = format!(
            "{} Elixir will try the next release automatically.",
            decision.user_message.trim_end_matches('.')
        );
        return persist_anime_qbittorrent_runtime_retry(
            pool,
            release,
            torrent_hash,
            coverage_plan,
            decision.reason_code,
            &retry_reason,
        )
        .await;
    }
    mark_qbittorrent_release_failed(
        pool,
        release,
        torrent_hash,
        decision.user_message,
        coverage_plan,
    )
    .await?;
    let coverages = list_release_coverage(pool, release.release_id).await?;
    for coverage in &coverages {
        update_release_coverage_review_state(
            pool,
            coverage.coverage_id,
            ReleaseCoverageState::Rejected,
            Some(decision.user_message.to_string()),
            Some("asr2_stale_release_recovery".to_string()),
        )
        .await?;
    }
    let target_ids =
        stale_release_retry_target_ids(pool, release, &coverages, torrent_hash).await?;
    let mut reset = 0usize;
    for target_id in target_ids {
        let retry_after = now
            + chrono::Duration::seconds(
                QBITTORRENT_STALE_RELEASE_RETRY_SECONDS + i64::from(target_id.as_bytes()[0] % 15),
            );
        if reset_target_for_candidate_retry(
            pool,
            target_id,
            format!("{} Trying the next ranked release.", decision.user_message),
            retry_after,
        )
        .await?
        .is_some()
        {
            reset += 1;
        }
    }
    Ok(reset)
}

async fn stale_release_retry_target_ids(
    pool: &sqlx::AnyPool,
    release: &AcquisitionRelease,
    coverages: &[AcquisitionReleaseCoverage],
    torrent_hash: &str,
) -> anyhow::Result<BTreeSet<Uuid>> {
    let mut target_ids = coverages
        .iter()
        .map(|coverage| coverage.target_id)
        .collect::<BTreeSet<_>>();
    if !target_ids.is_empty() {
        return Ok(target_ids);
    }
    let Some(subscription_id) = release.subscription_id else {
        return Ok(target_ids);
    };
    let rows = sqlx::query(
        "SELECT CAST(target_id AS TEXT) AS target_id
         FROM acquisition_targets
         WHERE subscription_id = $1
           AND selected_route_logical_id = $2
           AND download_id = $3
           AND state = 'submitted'",
    )
    .bind(subscription_id.to_string())
    .bind(TORRENT_DEFAULT_LOGICAL_ID)
    .bind(torrent_hash)
    .fetch_all(pool)
    .await?;
    for row in rows {
        let raw: String = row.try_get("target_id")?;
        if let Ok(target_id) = Uuid::parse_str(&raw) {
            target_ids.insert(target_id);
        }
    }
    Ok(target_ids)
}

fn merge_qbittorrent_stale_failure_evidence(
    mut coverage_plan: Option<Value>,
    torrent_hash: &str,
    torrent_info: &Value,
    decision: &QbittorrentStaleReleaseDecision,
    now: chrono::DateTime<chrono::Utc>,
) -> Value {
    let runtime = json!({
        "runtimeState": "failed",
        "failureState": decision.reason_code,
        "torrentHash": torrent_hash,
        "rawState": string_field(torrent_info, "state"),
        "ageSeconds": decision.age_seconds,
        "progress": torrent_info.get("progress").and_then(Value::as_f64),
        "downloadRateBps": numeric_u64_field(torrent_info, "dlspeed"),
        "connectedSeeds": numeric_i64_field(torrent_info, "num_seeds"),
        "completeSeeds": numeric_i64_field(torrent_info, "num_complete"),
        "availability": numeric_f64_field(torrent_info, "availability"),
        "message": decision.user_message,
        "policyVersion": "asr2-stale-release-recovery-v1",
        "failedAt": now,
    });
    let retry_suppression = json!({
        "status": "rejected",
        "suppressAutomaticRediscovery": true,
        "reason": decision.reason_code,
        "message": decision.user_message,
        "failedAt": now,
    });
    match coverage_plan.take() {
        Some(Value::Object(mut object)) => {
            object.insert("torrentRuntime".to_string(), runtime);
            object.insert("retrySuppression".to_string(), retry_suppression);
            Value::Object(object)
        }
        Some(value) => json!({
            "previousCoveragePlan": value,
            "torrentRuntime": runtime,
            "retrySuppression": retry_suppression
        }),
        None => json!({
            "torrentRuntime": runtime,
            "retrySuppression": retry_suppression
        }),
    }
}

async fn record_qbittorrent_runtime_evidence(
    pool: &sqlx::AnyPool,
    release: &AcquisitionRelease,
    evidence: &DownloadBrokerTorrentEvidence,
) -> anyhow::Result<()> {
    let torrent_hash = release
        .download_id
        .as_deref()
        .context("qBittorrent runtime evidence is missing its torrent hash")?;
    let coverage_plan = merge_qbittorrent_runtime_evidence(release.coverage_plan.clone(), evidence);
    update_qbittorrent_release_state_only(
        pool,
        release.release_id,
        torrent_hash,
        release.state,
        release.state,
        release
            .state_reason
            .as_deref()
            .unwrap_or("qBittorrent staged runtime evidence refreshed."),
        coverage_plan,
    )
    .await
}

fn merge_qbittorrent_runtime_evidence(
    mut coverage_plan: Option<Value>,
    evidence: &DownloadBrokerTorrentEvidence,
) -> Value {
    let evidence = serde_json::to_value(evidence).unwrap_or_else(|_| json!({}));
    match coverage_plan.take() {
        Some(Value::Object(mut object)) => {
            object.insert("torrentRuntime".to_string(), evidence);
            Value::Object(object)
        }
        Some(value) => json!({
            "previousCoveragePlan": value,
            "torrentRuntime": evidence
        }),
        None => json!({
            "torrentRuntime": evidence
        }),
    }
}

async fn update_release_file_provider_metadata(
    pool: &sqlx::AnyPool,
    release_file_id: Uuid,
    metadata: Value,
) -> anyhow::Result<()> {
    let metadata_json = serde_json::to_string(&metadata)?;
    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_release_files
         SET provider_metadata_json = $1,
             updated_at = CURRENT_TIMESTAMP
         WHERE release_file_id = $2",
    )
    .bind(metadata_json)
    .bind(release_file_id.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

async fn refresh_staged_qbittorrent_metadata(
    state: &AppState,
    store: &ExtensionStore<'_>,
    release: &AcquisitionRelease,
    torrent_hash: &str,
    force: bool,
    report_provider_errors: bool,
) -> anyhow::Result<usize> {
    let hash = torrent_hash.trim();
    if hash.is_empty() {
        return Ok(0);
    }
    if qbittorrent_submission_bookkeeping_pending(release) {
        return Ok(0);
    }
    if !reserve_qbittorrent_metadata_poll(release.release_id, force) {
        return Ok(0);
    }
    let provider_id = release
        .selected_provider_id
        .context("staged qBittorrent release is missing selected provider")?;
    let store_provider = store
        .get_provider(provider_id)
        .await?
        .context("staged qBittorrent provider is missing")?;
    let value = match request_qbittorrent_service_json(
        state,
        store,
        store_provider.instance_id,
        ReqwestMethod::GET,
        &qbittorrent_torrent_files_path(hash),
        None,
    )
    .await
    {
        Ok(value) => value,
        Err(err) => {
            tracing::debug!(
                release_id = %release.release_id,
                torrent_hash = hash,
                "qBittorrent file metadata is not available yet: {err}"
            );
            finish_qbittorrent_metadata_poll(release.release_id, 0);
            if report_provider_errors {
                return Err(err).context("qBittorrent torrent file-list request failed");
            }
            return Ok(0);
        }
    };
    let rows = value
        .as_array()
        .context("qbittorrent torrents/files response was not an array")?;
    let persisted =
        persist_qbittorrent_release_file_rows(&state.db_pool, release, hash, rows).await?;
    if persisted > 0 && qbittorrent_release_needs_file_policy(release) {
        refine_and_apply_qbittorrent_file_policy(state, store, release, hash).await?;
    }
    finish_qbittorrent_metadata_poll(release.release_id, persisted);
    Ok(persisted)
}

fn qbittorrent_submission_bookkeeping_marked_pending(release: &AcquisitionRelease) -> bool {
    release
        .coverage_plan
        .as_ref()
        .and_then(|plan| plan.pointer("/submissionBookkeeping/status"))
        .and_then(Value::as_str)
        == Some("pending")
}

fn qbittorrent_submission_bookkeeping_pending(release: &AcquisitionRelease) -> bool {
    release.subscription_id.is_some()
        && (release.state == AcquisitionReleaseState::Staging
            || qbittorrent_submission_bookkeeping_marked_pending(release))
}

// The metadata poll waits for post-submit bookkeeping for every media type.
// Priority persistence additionally treats staged anime as a hard ownership
// barrier because ALM-7 refinement can mutate coverage and schedule retries.
// Existing deterministic Series/Movie decisions may persist from a staged
// snapshot, but only while the exact torrent hash still owns the release.
fn qbittorrent_priority_bookkeeping_pending(release: &AcquisitionRelease) -> bool {
    release.subscription_id.is_some()
        && (qbittorrent_submission_bookkeeping_marked_pending(release)
            || (release.media_type == MediaType::Anime
                && release.state == AcquisitionReleaseState::Staging))
}

fn qbittorrent_release_needs_file_policy(release: &AcquisitionRelease) -> bool {
    !qbittorrent_submission_bookkeeping_pending(release)
        && matches!(
            release.state,
            AcquisitionReleaseState::Staging
                | AcquisitionReleaseState::Submitted
                | AcquisitionReleaseState::Ready
        )
}

async fn reconcile_qbittorrent_submission_bookkeeping(
    pool: &sqlx::AnyPool,
    release: &AcquisitionRelease,
    torrent_hash: &str,
) -> anyhow::Result<bool> {
    if release
        .coverage_plan
        .as_ref()
        .and_then(|plan| plan.pointer("/submissionBookkeeping/status"))
        .and_then(Value::as_str)
        != Some("pending")
        || release.state != AcquisitionReleaseState::Submitted
    {
        return Ok(false);
    }
    let Some(subscription_id) = release.subscription_id else {
        return Ok(false);
    };
    let targets = list_subscription_targets(pool, subscription_id).await?;
    let coverage_rows = list_release_coverage(pool, release.release_id).await?;
    let expected_targets =
        qbittorrent_release_scoped_targets(release, &targets, &coverage_rows, torrent_hash);
    if expected_targets.is_empty() {
        return Ok(false);
    }
    let expected_ids = expected_targets
        .iter()
        .map(|target| target.target_id)
        .collect::<BTreeSet<_>>();
    let covered_ids = coverage_rows
        .into_iter()
        .filter(|coverage| coverage.state != ReleaseCoverageState::Rejected)
        .map(|coverage| coverage.target_id)
        .collect::<BTreeSet<_>>();
    if expected_ids != covered_ids
        || expected_targets.iter().any(|target| {
            target.state != AcquisitionTargetState::Submitted
                || target.selected_route_logical_id.as_deref() != Some(TORRENT_DEFAULT_LOGICAL_ID)
                || target
                    .download_id
                    .as_deref()
                    .map(|value| !value.eq_ignore_ascii_case(torrent_hash))
                    .unwrap_or(true)
        })
    {
        return Ok(false);
    }
    let job_is_durable = list_release_jobs(pool, release.release_id)
        .await?
        .into_iter()
        .any(|job| {
            job.active
                && job.route_logical_id == TORRENT_DEFAULT_LOGICAL_ID
                && job
                    .download_id
                    .as_deref()
                    .map(|value| value.eq_ignore_ascii_case(torrent_hash))
                    .unwrap_or(false)
        });
    if !job_is_durable {
        return Ok(false);
    }

    let original_coverage_plan = release.coverage_plan.clone().unwrap_or_else(|| json!({}));
    let original_coverage_plan_json = serde_json::to_string(&original_coverage_plan)?;
    let mut coverage_plan = original_coverage_plan;
    let completed = json!({
        "status": "complete",
        "policyVersion": "acquisition-broker-bookkeeping-v1",
        "updatedAt": chrono::Utc::now(),
        "reconciled": true,
    });
    if let Some(object) = coverage_plan.as_object_mut() {
        object.insert("submissionBookkeeping".to_string(), completed);
    } else {
        coverage_plan = json!({
            "coveragePlan": coverage_plan,
            "submissionBookkeeping": completed,
        });
    }
    let updated = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_releases
         SET coverage_plan_json = $1,
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = $2
           AND state = 'submitted'
           AND coverage_plan_json = $3",
    )
    .bind(serde_json::to_string(&coverage_plan)?)
    .bind(release.release_id.to_string())
    .bind(original_coverage_plan_json)
    .execute(pool)
    .await?;
    Ok(updated.rows_affected() == 1)
}

async fn persist_qbittorrent_release_file_rows(
    pool: &sqlx::AnyPool,
    release: &AcquisitionRelease,
    torrent_hash: &str,
    rows: &[Value],
) -> anyhow::Result<usize> {
    let mut persisted = 0usize;
    for (offset, row) in rows.iter().enumerate() {
        let Some(path) = string_field(row, "name").or_else(|| string_field(row, "path")) else {
            continue;
        };
        let file_index = row
            .get("index")
            .and_then(Value::as_i64)
            .or_else(|| i64::try_from(offset).ok());
        let provider_file_id = file_index
            .map(|index| index.to_string())
            .unwrap_or_else(|| path.clone());
        let basename = basename_from_path(&path);
        let size_bytes = number_field(row, "size").and_then(u64_to_i64);
        let priority = row.get("priority").and_then(Value::as_i64);
        let selected = priority.map(|value| value > 0);
        let classification = classify_release_file_path(&path);
        let parsed = parsed_qbittorrent_file_metadata(release.media_type, &path);
        upsert_release_file(
            pool,
            NewAcquisitionReleaseFile {
                release_file_id: None,
                release_id: release.release_id,
                file_index,
                file_id: Some(provider_file_id.clone()),
                provider_file_id: Some(provider_file_id.clone()),
                path: path.clone(),
                basename: Some(basename),
                size_bytes,
                selectable: true,
                selected,
                parsed_title: parsed.title,
                parsed_season_number: parsed.season_number,
                parsed_episode_number: parsed.episode_number,
                parsed_episode_end_number: parsed.episode_end_number,
                parsed_absolute_episode_number: parsed.absolute_episode_number,
                parsed_absolute_episode_end_number: parsed.absolute_episode_end_number,
                parsed_air_date: parsed.air_date,
                parsed_quality: parsed.quality,
                parsed_language: parsed.language,
                parsed_release_group: parsed.release_group,
                parser_confidence: parsed.confidence,
                parser_reason: parsed.reason,
                raw: Some(row.clone()),
                provider_metadata: Some(json!({
                    "provider": "qbittorrent",
                    "torrentHash": torrent_hash,
                    "providerFileId": provider_file_id,
                    "fileIndex": file_index,
                    "sizeBytes": size_bytes,
                    "priority": priority,
                    "progress": row.get("progress").and_then(Value::as_f64),
                    "availability": row.get("availability").and_then(Value::as_f64),
                    "mediaClassification": classification,
                    "media": classification == "media",
                    "selected": selected
                })),
            },
        )
        .await?;
        persisted += 1;
    }
    Ok(persisted)
}

#[derive(Debug, Clone)]
struct QbittorrentCoverageRefinement {
    release_kind: ReleaseKind,
    resolver_kind: ReleaseResolverKind,
    resolver_version: String,
    confidence: ReleaseConfidence,
    review_reasons: Vec<String>,
    coverage_plan: Value,
    anime_match_assist: Option<AnimeMatchAssistProvenance>,
    anime_model_audio_profile: Option<AnimeMatchAudioProfile>,
    anime_model_audio_policy: Option<Value>,
    automatic_retry: bool,
    suppress_automatic_rediscovery: bool,
}

#[derive(Debug, Clone)]
struct QbittorrentAnimeModelAudioEvidence {
    profile: AnimeMatchAudioProfile,
    policy: Value,
}

#[derive(Debug, Clone)]
struct QbittorrentAnimeResolvedCoverage {
    plan: AnimeFileCoveragePlan,
    model_audio: Option<QbittorrentAnimeModelAudioEvidence>,
    required_audio_satisfied: bool,
    audio_hard_mismatch: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum QbittorrentFilePriorityDecisionStatus {
    Approved,
    ReviewRequired,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QbittorrentFilePriorityDecision {
    status: QbittorrentFilePriorityDecisionStatus,
    selected_file_ids: Vec<String>,
    skipped_file_ids: Vec<String>,
    review_reasons: Vec<String>,
    policy_version: String,
    coverage_fingerprint: String,
    wanted_priority: i64,
    skipped_priority: i64,
    user_approved: bool,
}

impl QbittorrentFilePriorityDecision {
    fn is_approved(&self) -> bool {
        self.status == QbittorrentFilePriorityDecisionStatus::Approved
    }
}

async fn refine_and_apply_qbittorrent_file_policy(
    state: &AppState,
    store: &ExtensionStore<'_>,
    release: &AcquisitionRelease,
    torrent_hash: &str,
) -> anyhow::Result<()> {
    let Some(subscription_id) = release.subscription_id else {
        let files = list_release_files(&state.db_pool, release.release_id).await?;
        let coverage = list_release_coverage(&state.db_pool, release.release_id).await?;
        if release.media_type == MediaType::Anime {
            let refinement = QbittorrentCoverageRefinement {
                release_kind: ReleaseKind::Unknown,
                resolver_kind: ReleaseResolverKind::Unresolved,
                resolver_version: QBITTORRENT_SELECTION_POLICY_VERSION.to_string(),
                confidence: ReleaseConfidence::Low,
                review_reasons: Vec::new(),
                coverage_plan: json!({
                    "source": "qbittorrent_file_list",
                    "torrentHash": torrent_hash,
                    "automaticResolution": {
                        "status": "retryable",
                        "reason": "missing_subscription_context"
                    }
                }),
                anime_match_assist: None,
                anime_model_audio_profile: None,
                anime_model_audio_policy: None,
                automatic_retry: true,
                suppress_automatic_rediscovery: false,
            };
            persist_anime_qbittorrent_retry(&state.db_pool, release, torrent_hash, &refinement)
                .await?;
            if let Err(err) =
                cleanup_qbittorrent_retry_torrent(state, store, release, torrent_hash).await
            {
                tracing::warn!(
                    release_id = %release.release_id,
                    torrent_hash,
                    "failed to remove unresolved anime qBittorrent candidate after scheduling retry: {err}"
                );
            }
            return Ok(());
        }
        let refinement = QbittorrentCoverageRefinement {
            release_kind: ReleaseKind::Unknown,
            resolver_kind: ReleaseResolverKind::Unresolved,
            resolver_version: QBITTORRENT_SELECTION_POLICY_VERSION.to_string(),
            confidence: ReleaseConfidence::ReviewRequired,
            review_reasons: vec!["missing_subscription_context".to_string()],
            coverage_plan: json!({
                "source": "qbittorrent_file_list",
                "torrentHash": torrent_hash,
                "reviewReasons": ["missing_subscription_context"]
            }),
            anime_match_assist: None,
            anime_model_audio_profile: None,
            anime_model_audio_policy: None,
            automatic_retry: false,
            suppress_automatic_rediscovery: false,
        };
        let decision = decide_qbittorrent_file_priority(release, &refinement, &files, &coverage);
        persist_qbittorrent_priority_decision(
            &state.db_pool,
            release,
            &files,
            &coverage,
            &refinement,
            &decision,
            false,
        )
        .await?;
        return Ok(());
    };

    let subscription = get_subscription(&state.db_pool, subscription_id).await?;
    let all_targets = list_subscription_targets(&state.db_pool, subscription_id).await?;
    let files = list_release_files(&state.db_pool, release.release_id).await?;
    let existing_coverage = list_release_coverage(&state.db_pool, release.release_id).await?;
    let targets =
        qbittorrent_release_scoped_targets(release, &all_targets, &existing_coverage, torrent_hash);
    let matching_service = state.anime_inference.matching_service();
    let refinement = refine_qbittorrent_coverage(
        &state.db_pool,
        &matching_service,
        subscription.as_ref(),
        release,
        torrent_hash,
        &targets,
        &files,
    )
    .await?;
    if refinement
        .anime_match_assist
        .as_ref()
        .is_some_and(|assist| {
            matches!(
                assist.reason,
                Some(
                    AnimeMatchFallbackReason::EngineUnavailable
                        | AnimeMatchFallbackReason::EngineError
                )
            )
        })
    {
        state.anime_inference.request_runtime_repair();
    }
    let coverage = list_release_coverage(&state.db_pool, release.release_id).await?;
    let base_decision = decide_qbittorrent_file_priority(release, &refinement, &files, &coverage);
    if release.media_type == MediaType::Anime
        && (refinement.automatic_retry || !base_decision.is_approved())
    {
        persist_anime_qbittorrent_retry(&state.db_pool, release, torrent_hash, &refinement).await?;
        if let Err(err) =
            cleanup_qbittorrent_retry_torrent(state, store, release, torrent_hash).await
        {
            tracing::warn!(
                release_id = %release.release_id,
                torrent_hash,
                "failed to remove unresolved anime qBittorrent candidate after scheduling retry: {err}"
            );
        }
        return Ok(());
    }
    let decision = qbittorrent_priority_decision_with_legacy_override(release, base_decision);
    persist_qbittorrent_priority_decision(
        &state.db_pool,
        release,
        &files,
        &coverage,
        &refinement,
        &decision,
        false,
    )
    .await?;
    if !decision.is_approved() {
        return Ok(());
    }

    apply_qbittorrent_file_priorities(state, store, release, torrent_hash, &decision).await?;
    persist_qbittorrent_priority_decision(
        &state.db_pool,
        release,
        &files,
        &coverage,
        &refinement,
        &decision,
        true,
    )
    .await?;
    resume_qbittorrent_torrent(state, store, release, torrent_hash).await?;
    mark_qbittorrent_release_resumed(&state.db_pool, release, torrent_hash).await?;
    Ok(())
}

async fn refine_qbittorrent_coverage(
    pool: &sqlx::AnyPool,
    anime_matching_service: &AnimeMatchingService,
    subscription: Option<&AcquisitionSubscription>,
    release: &AcquisitionRelease,
    torrent_hash: &str,
    targets: &[AcquisitionTarget],
    files: &[AcquisitionReleaseFile],
) -> anyhow::Result<QbittorrentCoverageRefinement> {
    match release.media_type {
        MediaType::Series => {
            refine_tv_qbittorrent_coverage(pool, release, torrent_hash, targets, files).await
        }
        MediaType::Anime => {
            refine_anime_qbittorrent_coverage(
                pool,
                anime_matching_service,
                subscription,
                release,
                torrent_hash,
                targets,
                files,
            )
            .await
        }
        MediaType::Movie => {
            refine_movie_qbittorrent_coverage(pool, release, torrent_hash, targets, files).await
        }
    }
}

async fn refine_tv_qbittorrent_coverage(
    pool: &sqlx::AnyPool,
    release: &AcquisitionRelease,
    torrent_hash: &str,
    targets: &[AcquisitionTarget],
    files: &[AcquisitionReleaseFile],
) -> anyhow::Result<QbittorrentCoverageRefinement> {
    let resolver = TvSonarrStyleResolver;
    let parsed = resolver.parse_title(&release.release_title);
    let tv_targets = targets
        .iter()
        .filter_map(|target| {
            Some(TvTarget {
                target_id: target.target_id,
                target_key: target.target_key.clone(),
                season_number: target.season_number?,
                episode_number: target.episode_number?,
                air_date: target.air_date.clone(),
            })
        })
        .collect::<Vec<_>>();
    let file_inputs = files
        .iter()
        .filter_map(qbittorrent_tv_release_file_input)
        .collect::<Vec<_>>();
    let plan = resolver.plan_coverage(
        &parsed,
        &tv_targets,
        &file_inputs,
        TvCoverageOptions {
            allow_partial_pack: false,
            file_selection_supported: true,
        },
    );
    let file_ids = release_file_ids_by_provider_file_id(files);
    for entry in &plan.entries {
        upsert_release_coverage(
            pool,
            NewAcquisitionReleaseCoverage {
                coverage_id: None,
                release_id: release.release_id,
                release_file_id: entry
                    .release_file_id
                    .as_ref()
                    .and_then(|file_id| file_ids.get(file_id))
                    .copied(),
                target_id: entry.target_id,
                coverage_kind: entry.coverage_kind,
                confidence: plan.confidence,
                score: None,
                reason: Some("rr5b_tv_qbittorrent_file_list".to_string()),
                state: entry.state,
                verified_by: Some("rr5b_qbittorrent_tv_file_list".to_string()),
            },
        )
        .await?;
    }
    let review_reasons = plan
        .rejection_reasons
        .iter()
        .map(|reason| reason.as_str().to_string())
        .collect::<Vec<_>>();
    Ok(QbittorrentCoverageRefinement {
        release_kind: plan.release_kind,
        resolver_kind: plan.resolver_kind,
        resolver_version: plan.resolver_version.clone(),
        confidence: plan.confidence,
        review_reasons: review_reasons.clone(),
        coverage_plan: json!({
            "source": "qbittorrent_file_list",
            "torrentHash": torrent_hash,
            "tv": plan,
            "reviewReasons": review_reasons
        }),
        anime_match_assist: None,
        anime_model_audio_profile: None,
        anime_model_audio_policy: None,
        automatic_retry: false,
        suppress_automatic_rediscovery: false,
    })
}

async fn refine_anime_qbittorrent_coverage(
    pool: &sqlx::AnyPool,
    matching_service: &AnimeMatchingService,
    subscription: Option<&AcquisitionSubscription>,
    release: &AcquisitionRelease,
    torrent_hash: &str,
    targets: &[AcquisitionTarget],
    files: &[AcquisitionReleaseFile],
) -> anyhow::Result<QbittorrentCoverageRefinement> {
    let context = anime_scoring_context_from_qbittorrent_release(subscription, release, targets);
    let acquisition_candidate = qbittorrent_anime_acquisition_candidate(release, files);
    let candidate = qbittorrent_anime_candidate_input(&acquisition_candidate);
    let file_inputs = files
        .iter()
        .filter_map(qbittorrent_anime_release_file_input)
        .collect::<Vec<_>>();
    let deterministic_plan = plan_anime_file_coverage_with_options(
        &context,
        &candidate,
        &file_inputs,
        AnimeCoverageOptions {
            // qBittorrent exposes exact per-file priorities after metadata is
            // available, so safe deterministic and model mappings may exclude
            // unrelated media instead of escalating to a user decision.
            file_selection_supported: true,
        },
    );
    let deterministic_plan = bind_exact_single_anime_provider_file(
        deterministic_plan,
        &context,
        &candidate,
        &file_inputs,
    );
    let (deterministic_required_audio_satisfied, deterministic_audio_hard_mismatch) = subscription
        .map(|subscription| {
            let (assessment, satisfied) = assess_acquisition_anime_provider_file_audio(
                subscription,
                &acquisition_candidate,
                &deterministic_plan.selected_file_keys,
            );
            (
                satisfied,
                !satisfied && assessment.state == LanguagePreferenceAssessmentState::Mismatch,
            )
        })
        .unwrap_or((true, false));

    let deterministic_state = acquisition_anime_deterministic_state(&deterministic_plan);
    let deterministic_is_automatic =
        anime_qbittorrent_plan_is_automatic(&deterministic_plan, targets);
    let deterministic_resolution = QbittorrentAnimeResolvedCoverage {
        plan: deterministic_plan,
        model_audio: None,
        required_audio_satisfied: deterministic_required_audio_satisfied,
        audio_hard_mismatch: deterministic_audio_hard_mismatch,
    };
    let (resolution, anime_match_assist) = if deterministic_state
        == DeterministicMatchState::Definitive
        && deterministic_is_automatic
        && deterministic_required_audio_satisfied
    {
        (
            deterministic_resolution,
            Some(anime_deterministic_fast_path_provenance()),
        )
    } else if let Some(subscription) = subscription {
        match acquisition_anime_match_batch_input(
            format!("qbittorrent:{}:{torrent_hash}", release.release_id),
            subscription,
            targets,
            &context,
            std::slice::from_ref(&acquisition_candidate),
        ) {
            Ok(input) => 'semantic: {
                let prepared = match AnimeMatchingService::prepare_request(input) {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        break 'semantic (
                            deterministic_resolution,
                            Some(anime_invalid_request_fallback_provenance(error.to_string())),
                        );
                    }
                };
                let Some(wire_candidate) = prepared.request().candidates.first() else {
                    unreachable!("validated anime request contains a candidate")
                };
                let facts = wire_candidate.parse_facts.clone();
                let semantic_request = build_semantic_evidence_request(
                    prepared.request(),
                    wire_candidate.candidate_key.clone(),
                    wire_candidate.title.clone(),
                    None,
                    facts.season_numbers.iter().copied(),
                    facts.episode_numbers.iter().copied(),
                    facts.absolute_episode_numbers.iter().copied(),
                    anime_semantic_media_kinds(&wire_candidate.title, &facts),
                );
                let semantic_request = match semantic_request {
                    Ok(Some(request)) => request,
                    Ok(None) => {
                        break 'semantic (
                            deterministic_resolution,
                            Some(anime_invalid_request_fallback_provenance(
                                "no bounded semantic hypotheses".to_string(),
                            )),
                        );
                    }
                    Err(error) => {
                        break 'semantic (
                            deterministic_resolution,
                            Some(anime_invalid_request_fallback_provenance(error.to_string())),
                        );
                    }
                };
                let outcome = matching_service
                    .select_semantic_hypothesis(semantic_request.clone())
                    .await;
                let mut provenance = outcome.provenance;
                let Some(hypothesis) = outcome.hypothesis.as_ref() else {
                    break 'semantic (deterministic_resolution, Some(provenance));
                };
                let Some(evidence) =
                    anime_semantic_candidate_evidence(&semantic_request, hypothesis)
                else {
                    mark_anime_semantic_coverage_fallback(
                        &mut provenance,
                        "selected hypothesis has no canonical entity",
                    );
                    break 'semantic (deterministic_resolution, Some(provenance));
                };
                let semantic_plan = plan_anime_file_coverage_with_semantic_evidence(
                    &context,
                    &candidate,
                    &file_inputs,
                    AnimeCoverageOptions {
                        file_selection_supported: true,
                    },
                    &evidence,
                )
                .map(|plan| {
                    bind_exact_single_anime_provider_file_with_semantic_evidence(
                        plan,
                        &context,
                        &candidate,
                        &file_inputs,
                        &evidence,
                    )
                });
                let Some(semantic_plan) = semantic_plan else {
                    mark_anime_semantic_coverage_fallback(
                        &mut provenance,
                        "semantic evidence did not produce deterministic coverage",
                    );
                    break 'semantic (deterministic_resolution, Some(provenance));
                };
                let (assessment, required_audio_satisfied) =
                    assess_acquisition_anime_provider_file_audio(
                        subscription,
                        &acquisition_candidate,
                        &semantic_plan.selected_file_keys,
                    );
                let audio_hard_mismatch = !required_audio_satisfied
                    && assessment.state == LanguagePreferenceAssessmentState::Mismatch;
                if !anime_qbittorrent_plan_is_automatic(&semantic_plan, targets)
                    || !required_audio_satisfied
                {
                    mark_anime_semantic_coverage_fallback(
                        &mut provenance,
                        "semantic evidence failed deterministic coverage or audio policy",
                    );
                    let mut fallback = deterministic_resolution;
                    fallback.audio_hard_mismatch |= audio_hard_mismatch;
                    (fallback, Some(provenance))
                } else {
                    (
                        QbittorrentAnimeResolvedCoverage {
                            plan: semantic_plan,
                            model_audio: None,
                            required_audio_satisfied,
                            audio_hard_mismatch,
                        },
                        Some(provenance),
                    )
                }
            }
            Err(error) => (
                deterministic_resolution,
                Some(anime_invalid_request_fallback_provenance(error.to_string())),
            ),
        }
    } else {
        (
            deterministic_resolution,
            Some(anime_invalid_request_fallback_provenance(
                "staged anime release has no subscription context".to_string(),
            )),
        )
    };

    let QbittorrentAnimeResolvedCoverage {
        plan,
        model_audio,
        required_audio_satisfied,
        audio_hard_mismatch,
    } = resolution;
    let anime_model_audio_profile = model_audio.as_ref().map(|evidence| evidence.profile);
    let anime_model_audio_policy = model_audio.as_ref().map(|evidence| evidence.policy.clone());
    let automatic_retry =
        !anime_qbittorrent_plan_is_automatic(&plan, targets) || !required_audio_satisfied;
    let targets_by_key = targets
        .iter()
        .map(|target| (target.target_key.clone(), target.target_id))
        .collect::<HashMap<_, _>>();
    let file_ids = release_file_ids_by_provider_file_id(files);
    if !automatic_retry {
        for entry in &plan.entries {
            let target_id = targets_by_key
                .get(&entry.target_key)
                .copied()
                .ok_or_else(|| {
                    anyhow!(
                        "qBittorrent anime mapping references unknown scoped target '{}'",
                        entry.target_key
                    )
                })?;
            let release_file_key = entry.release_file_key.as_ref().ok_or_else(|| {
                anyhow!(
                    "qBittorrent anime mapping for '{}' has no selected release file",
                    entry.target_key
                )
            })?;
            let release_file_id = file_ids.get(release_file_key).copied().ok_or_else(|| {
                anyhow!(
                    "qBittorrent anime mapping references unavailable release file '{}'",
                    release_file_key
                )
            })?;
            upsert_release_coverage(
                pool,
                NewAcquisitionReleaseCoverage {
                    coverage_id: None,
                    release_id: release.release_id,
                    release_file_id: Some(release_file_id),
                    target_id,
                    coverage_kind: entry.coverage_kind,
                    confidence: entry.confidence,
                    score: entry.score,
                    reason: Some(entry.reason.clone()),
                    state: entry.state,
                    verified_by: Some("alm7_qbittorrent_anime_file_list".to_string()),
                },
            )
            .await?;
        }
    }
    let mut review_reasons = plan.review_reasons.clone();
    review_reasons.extend(plan.rejection_reasons.clone());
    review_reasons.sort();
    review_reasons.dedup();
    let score = score_anime_candidate(&context, &candidate);
    let diagnostics = anime_parser_diagnostics(&context, &score, Some(&plan));
    Ok(QbittorrentCoverageRefinement {
        release_kind: plan.release_kind,
        resolver_kind: plan.resolver_kind,
        resolver_version: plan.resolver_version.clone(),
        confidence: plan.confidence,
        review_reasons: review_reasons.clone(),
        coverage_plan: json!({
            "source": "qbittorrent_file_list",
            "torrentHash": torrent_hash,
            "anime": plan,
            "diagnostics": diagnostics,
            "animeMatchAssist": anime_match_assist,
            "modelAudioProfile": anime_model_audio_profile,
            "modelAudioPolicy": anime_model_audio_policy,
            "reviewReasons": review_reasons
        }),
        anime_match_assist,
        anime_model_audio_profile,
        anime_model_audio_policy,
        automatic_retry,
        suppress_automatic_rediscovery: audio_hard_mismatch,
    })
}

async fn refine_movie_qbittorrent_coverage(
    pool: &sqlx::AnyPool,
    release: &AcquisitionRelease,
    torrent_hash: &str,
    targets: &[AcquisitionTarget],
    files: &[AcquisitionReleaseFile],
) -> anyhow::Result<QbittorrentCoverageRefinement> {
    let file_inputs = files
        .iter()
        .filter_map(qbittorrent_movie_release_file_input)
        .collect::<Vec<_>>();
    let selection = select_movie_main_file(&file_inputs);
    let mut review_reasons = selection.review_reasons.clone();
    if targets.is_empty() {
        review_reasons.push("missing_movie_target".to_string());
    }
    let confidence = if review_reasons.is_empty() {
        ReleaseConfidence::High
    } else {
        ReleaseConfidence::ReviewRequired
    };
    let selected_release_file = (confidence == ReleaseConfidence::High)
        .then(|| {
            selection.selected_file_id.as_ref().and_then(|selected_id| {
                files
                    .iter()
                    .find(|file| qbittorrent_file_key(file).as_deref() == Some(selected_id))
            })
        })
        .flatten();
    if let Some(target) = targets.first() {
        upsert_release_coverage(
            pool,
            NewAcquisitionReleaseCoverage {
                coverage_id: None,
                release_id: release.release_id,
                release_file_id: selected_release_file.map(|file| file.release_file_id),
                target_id: target.target_id,
                coverage_kind: ReleaseCoverageKind::Movie,
                confidence,
                score: Some(1.0),
                reason: Some(if confidence == ReleaseConfidence::High {
                    "rrm5_qbittorrent_movie_main_file".to_string()
                } else {
                    "rrm5_qbittorrent_movie_file_list_review".to_string()
                }),
                state: if confidence == ReleaseConfidence::High {
                    ReleaseCoverageState::Planned
                } else {
                    ReleaseCoverageState::ReviewRequired
                },
                verified_by: Some("rrm5_qbittorrent_movie_file_list".to_string()),
            },
        )
        .await?;
    }
    review_reasons.sort();
    review_reasons.dedup();
    Ok(QbittorrentCoverageRefinement {
        release_kind: if confidence == ReleaseConfidence::High {
            ReleaseKind::Single
        } else {
            ReleaseKind::Unknown
        },
        resolver_kind: ReleaseResolverKind::MovieRadarrStyle,
        resolver_version: MOVIE_RADARR_STYLE_RESOLVER_VERSION.to_string(),
        confidence,
        review_reasons: review_reasons.clone(),
        coverage_plan: json!({
            "source": "qbittorrent_file_list",
            "torrentHash": torrent_hash,
            "movie": {
                "confidence": confidence,
                "coverageKind": ReleaseCoverageKind::Movie,
                "fileSelection": selection,
                "selectedFileId": selected_release_file.and_then(qbittorrent_file_key),
                "mainCandidateCount": selection.main_candidate_count
            },
            "reviewReasons": review_reasons
        }),
        anime_match_assist: None,
        anime_model_audio_profile: None,
        anime_model_audio_policy: None,
        automatic_retry: false,
        suppress_automatic_rediscovery: false,
    })
}

fn decide_qbittorrent_file_priority(
    release: &AcquisitionRelease,
    refinement: &QbittorrentCoverageRefinement,
    files: &[AcquisitionReleaseFile],
    coverage: &[AcquisitionReleaseCoverage],
) -> QbittorrentFilePriorityDecision {
    let mut review_reasons = refinement
        .review_reasons
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    if refinement.confidence != ReleaseConfidence::High {
        review_reasons.insert("coverage_not_high_confidence".to_string());
    }
    if files.is_empty() {
        review_reasons.insert("missing_file_list".to_string());
    }
    let selectable_media_files = selectable_qbittorrent_media_files(files);
    if !files.is_empty() && selectable_media_files.is_empty() {
        review_reasons.insert("no_media_files".to_string());
    }

    let files_by_release_file_id = files
        .iter()
        .map(|file| (file.release_file_id, file))
        .collect::<HashMap<_, _>>();
    let mut selected_file_ids = coverage
        .iter()
        .filter(|coverage| coverage.confidence == ReleaseConfidence::High)
        .filter(|coverage| coverage.state != ReleaseCoverageState::Rejected)
        .filter_map(|coverage| coverage.release_file_id)
        .filter_map(|release_file_id| files_by_release_file_id.get(&release_file_id))
        .filter(|file| file.selectable)
        .filter_map(|file| qbittorrent_file_key(file))
        .collect::<BTreeSet<_>>();

    if selected_file_ids.is_empty()
        && matches!(
            refinement.release_kind,
            ReleaseKind::Single | ReleaseKind::MultiEpisode
        )
        && selectable_media_files.len() == 1
        && refinement.confidence == ReleaseConfidence::High
        && let Some(file_id) = qbittorrent_file_key(selectable_media_files[0])
    {
        selected_file_ids.insert(file_id);
    }
    if selected_file_ids.is_empty() {
        review_reasons.insert("no_selected_files".to_string());
    }

    let selected_file_ids = selected_file_ids.into_iter().collect::<Vec<_>>();
    let selected_set = selected_file_ids.iter().cloned().collect::<HashSet<_>>();
    let mut skipped_file_ids = files
        .iter()
        .filter_map(qbittorrent_file_key)
        .filter(|file_id| !selected_set.contains(file_id))
        .collect::<Vec<_>>();
    skipped_file_ids.sort();
    skipped_file_ids.dedup();

    let review_reasons = review_reasons.into_iter().collect::<Vec<_>>();
    let status = if review_reasons.is_empty() {
        QbittorrentFilePriorityDecisionStatus::Approved
    } else {
        QbittorrentFilePriorityDecisionStatus::ReviewRequired
    };
    QbittorrentFilePriorityDecision {
        status,
        selected_file_ids,
        skipped_file_ids,
        review_reasons,
        policy_version: QBITTORRENT_SELECTION_POLICY_VERSION.to_string(),
        coverage_fingerprint: qbittorrent_coverage_fingerprint(
            release, refinement, files, coverage,
        ),
        wanted_priority: QBITTORRENT_WANTED_FILE_PRIORITY,
        skipped_priority: QBITTORRENT_SKIPPED_FILE_PRIORITY,
        user_approved: false,
    }
}

fn approved_qbittorrent_user_override(
    release: &AcquisitionRelease,
    fallback: &QbittorrentFilePriorityDecision,
) -> Option<QbittorrentFilePriorityDecision> {
    let policy = qbittorrent_priority_policy(release)?;
    if policy.get("status").and_then(Value::as_str) != Some("approved")
        || policy.get("userApproved").and_then(Value::as_bool) != Some(true)
    {
        return None;
    }
    let selected_file_ids = policy
        .get("selectedFileIds")?
        .as_array()?
        .iter()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect::<Vec<_>>();
    if selected_file_ids.is_empty() {
        return None;
    }
    let skipped_file_ids = policy
        .get("skippedFileIds")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    Some(QbittorrentFilePriorityDecision {
        status: QbittorrentFilePriorityDecisionStatus::Approved,
        selected_file_ids,
        skipped_file_ids,
        review_reasons: Vec::new(),
        policy_version: policy
            .get("policyVersion")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| fallback.policy_version.clone()),
        coverage_fingerprint: policy
            .get("coverageFingerprint")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| fallback.coverage_fingerprint.clone()),
        wanted_priority: policy
            .get("wantedPriority")
            .and_then(Value::as_i64)
            .unwrap_or(fallback.wanted_priority),
        skipped_priority: policy
            .get("skippedPriority")
            .and_then(Value::as_i64)
            .unwrap_or(fallback.skipped_priority),
        user_approved: true,
    })
}

fn qbittorrent_priority_decision_with_legacy_override(
    release: &AcquisitionRelease,
    fallback: QbittorrentFilePriorityDecision,
) -> QbittorrentFilePriorityDecision {
    if release.media_type == MediaType::Anime {
        return fallback;
    }
    approved_qbittorrent_user_override(release, &fallback).unwrap_or(fallback)
}

async fn apply_qbittorrent_file_priorities(
    state: &AppState,
    store: &ExtensionStore<'_>,
    release: &AcquisitionRelease,
    torrent_hash: &str,
    decision: &QbittorrentFilePriorityDecision,
) -> anyhow::Result<()> {
    let provider_id = release
        .selected_provider_id
        .context("staged qBittorrent release is missing selected provider")?;
    let store_provider = store
        .get_provider(provider_id)
        .await?
        .context("staged qBittorrent provider is missing")?;
    if !decision.skipped_file_ids.is_empty() {
        request_qbittorrent_file_priority(
            state,
            store,
            store_provider.instance_id,
            torrent_hash,
            &decision.skipped_file_ids,
            decision.skipped_priority,
        )
        .await?;
    }
    if !decision.selected_file_ids.is_empty() {
        request_qbittorrent_file_priority(
            state,
            store,
            store_provider.instance_id,
            torrent_hash,
            &decision.selected_file_ids,
            decision.wanted_priority,
        )
        .await?;
    }
    Ok(())
}

async fn request_qbittorrent_file_priority(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    torrent_hash: &str,
    file_ids: &[String],
    priority: i64,
) -> anyhow::Result<()> {
    let fields = qbittorrent_file_priority_fields(torrent_hash, file_ids, priority);
    request_qbittorrent_service_form(
        state,
        store,
        instance_id,
        "api/v2/torrents/filePrio",
        &fields,
    )
    .await
}

fn qbittorrent_file_priority_fields(
    torrent_hash: &str,
    file_ids: &[String],
    priority: i64,
) -> HashMap<String, String> {
    let mut fields = HashMap::new();
    fields.insert("hash".to_string(), torrent_hash.to_string());
    fields.insert("id".to_string(), file_ids.join("|"));
    fields.insert("priority".to_string(), priority.to_string());
    fields
}

async fn resume_qbittorrent_torrent(
    state: &AppState,
    store: &ExtensionStore<'_>,
    release: &AcquisitionRelease,
    torrent_hash: &str,
) -> anyhow::Result<()> {
    let provider_id = release
        .selected_provider_id
        .context("staged qBittorrent release is missing selected provider")?;
    let store_provider = store
        .get_provider(provider_id)
        .await?
        .context("staged qBittorrent provider is missing")?;
    let fields = qbittorrent_resume_fields(torrent_hash);
    request_qbittorrent_service_form(
        state,
        store,
        store_provider.instance_id,
        "api/v2/torrents/resume",
        &fields,
    )
    .await
}

fn qbittorrent_resume_fields(torrent_hash: &str) -> HashMap<String, String> {
    let mut fields = HashMap::new();
    fields.insert("hashes".to_string(), torrent_hash.to_string());
    fields
}

async fn persist_anime_qbittorrent_retry(
    pool: &sqlx::AnyPool,
    release: &AcquisitionRelease,
    torrent_hash: &str,
    refinement: &QbittorrentCoverageRefinement,
) -> anyhow::Result<()> {
    debug_assert_eq!(release.media_type, MediaType::Anime);
    let reason =
        "Anime files could not be mapped safely. Elixir will try the next release automatically.";
    let suppress_automatic_rediscovery = anime_qbittorrent_retry_suppresses_rediscovery(refinement);
    persist_anime_qbittorrent_retry_disposition(
        pool,
        release,
        torrent_hash,
        refinement,
        "anime_qbittorrent_file_mapping_unresolved",
        reason,
        suppress_automatic_rediscovery,
    )
    .await
    .map(|_| ())
}

#[allow(clippy::too_many_arguments)]
async fn persist_anime_qbittorrent_retry_disposition(
    pool: &sqlx::AnyPool,
    release: &AcquisitionRelease,
    torrent_hash: &str,
    refinement: &QbittorrentCoverageRefinement,
    retry_reason_code: &str,
    reason: &str,
    suppress_automatic_rediscovery: bool,
) -> anyhow::Result<usize> {
    debug_assert_eq!(release.media_type, MediaType::Anime);
    let now = chrono::Utc::now();
    let coverage_plan = merge_anime_qbittorrent_retry_evidence(
        refinement.coverage_plan.clone(),
        torrent_hash,
        now,
        retry_reason_code,
        suppress_automatic_rediscovery,
    );
    terminalize_anime_qbittorrent_retry(
        pool,
        release,
        torrent_hash,
        refinement,
        reason,
        coverage_plan,
    )
    .await?;
    reconcile_anime_qbittorrent_retry_bookkeeping(pool, release.release_id, torrent_hash).await
}

async fn persist_anime_qbittorrent_runtime_retry(
    pool: &sqlx::AnyPool,
    release: &AcquisitionRelease,
    torrent_hash: &str,
    coverage_plan: Value,
    retry_reason_code: &str,
    reason: &str,
) -> anyhow::Result<usize> {
    debug_assert_eq!(release.media_type, MediaType::Anime);
    let coverage = list_release_coverage(pool, release.release_id).await?;
    let all_targets = match release.subscription_id {
        Some(subscription_id) => list_subscription_targets(pool, subscription_id).await?,
        None => Vec::new(),
    };
    let targets =
        qbittorrent_release_scoped_targets(release, &all_targets, &coverage, torrent_hash);
    let refinement = QbittorrentCoverageRefinement {
        release_kind: release.release_kind,
        resolver_kind: release.resolver_kind,
        resolver_version: release.resolver_version.clone(),
        confidence: release.confidence,
        review_reasons: Vec::new(),
        coverage_plan,
        anime_match_assist: None,
        anime_model_audio_profile: None,
        anime_model_audio_policy: None,
        automatic_retry: true,
        suppress_automatic_rediscovery: false,
    };
    let reset = persist_anime_qbittorrent_retry_disposition(
        pool,
        release,
        torrent_hash,
        &refinement,
        retry_reason_code,
        reason,
        true,
    )
    .await?;
    Ok(reset.min(targets.len()))
}

async fn terminalize_anime_qbittorrent_retry(
    pool: &sqlx::AnyPool,
    release: &AcquisitionRelease,
    torrent_hash: &str,
    refinement: &QbittorrentCoverageRefinement,
    reason: &str,
    coverage_plan: Value,
) -> anyhow::Result<()> {
    let canonical_hash = qbittorrent_trackable_info_hash(torrent_hash)
        .context("anime qBittorrent retry has no canonical torrent hash")?;
    let current = get_release(pool, release.release_id)
        .await?
        .context("anime qBittorrent retry release disappeared")?;
    if current.state == AcquisitionReleaseState::Failed
        && current
            .download_id
            .as_deref()
            .and_then(qbittorrent_trackable_info_hash)
            .as_deref()
            == Some(canonical_hash.as_str())
        && anime_qbittorrent_cleanup_pending(&current)
    {
        let current_suppresses = current
            .coverage_plan
            .as_ref()
            .and_then(|plan| plan.pointer("/retrySuppression/suppressAutomaticRediscovery"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let desired_suppresses = coverage_plan
            .pointer("/retrySuppression/suppressAutomaticRediscovery")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if desired_suppresses && !current_suppresses {
            let original_plan_json = current
                .coverage_plan
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?
                .unwrap_or_default();
            let upgraded = sqlx::query::<sqlx::Any>(
                "UPDATE acquisition_releases
                 SET release_kind = $1,
                     resolver_kind = $2,
                     resolver_version = $3,
                     confidence = $4,
                     state_reason = $5,
                     coverage_plan_json = $6,
                     updated_at = CURRENT_TIMESTAMP
                 WHERE release_id = $7
                   AND state = 'failed'
                   AND LOWER(download_id) = LOWER($8)
                   AND COALESCE(coverage_plan_json, '') = $9",
            )
            .bind(refinement.release_kind.as_str())
            .bind(refinement.resolver_kind.as_str())
            .bind(refinement.resolver_version.as_str())
            .bind(refinement.confidence.as_str())
            .bind(reason)
            .bind(serde_json::to_string(&coverage_plan)?)
            .bind(release.release_id.to_string())
            .bind(&canonical_hash)
            .bind(original_plan_json)
            .execute(pool)
            .await?;
            if upgraded.rows_affected() != 1 {
                anyhow::bail!(
                    "anime qBittorrent retry could not strengthen the active cleanup disposition"
                );
            }
        }
        return Ok(());
    }
    if matches!(
        current.state,
        AcquisitionReleaseState::Failed
            | AcquisitionReleaseState::Cancelled
            | AcquisitionReleaseState::Completed
    ) {
        return Ok(());
    }
    let expected_coverage_plan_json = release
        .coverage_plan
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?
        .unwrap_or_default();
    let mut transaction = pool.begin().await?;
    let release_update = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_releases
         SET release_kind = $1,
             resolver_kind = $2,
             resolver_version = $3,
             confidence = $4,
             state = $5,
             state_reason = $6,
             coverage_plan_json = $7,
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = $8
           AND LOWER(download_id) = LOWER($9)
           AND state NOT IN ('failed', 'cancelled', 'completed')
           AND COALESCE(coverage_plan_json, '') = $10
           AND NOT EXISTS (
               SELECT 1
               FROM acquisition_release_coverage c
               JOIN acquisition_targets t ON t.target_id = c.target_id
               WHERE c.release_id = acquisition_releases.release_id
                 AND c.state != 'rejected'
                 AND t.state NOT IN ('imported', 'excluded')
                 AND (
                     (COALESCE(t.download_id, '') != '' AND LOWER(t.download_id) != LOWER($9))
                     OR (COALESCE(t.selected_route_logical_id, '') != ''
                         AND t.selected_route_logical_id != $11)
                 )
           )",
    )
    .bind(refinement.release_kind.as_str())
    .bind(refinement.resolver_kind.as_str())
    .bind(refinement.resolver_version.as_str())
    .bind(refinement.confidence.as_str())
    .bind(AcquisitionReleaseState::Failed.as_str())
    .bind(reason)
    .bind(serde_json::to_string(&coverage_plan)?)
    .bind(release.release_id.to_string())
    .bind(&canonical_hash)
    .bind(expected_coverage_plan_json)
    .bind(TORRENT_DEFAULT_LOGICAL_ID)
    .execute(&mut *transaction)
    .await?;
    if release_update.rows_affected() != 1 {
        anyhow::bail!(
            "anime qBittorrent retry lost ownership of the active release attempt for hash {torrent_hash}"
        );
    }
    let job_update = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_release_jobs
         SET state = $1,
             state_reason = $2,
             active = 0,
             completed_at = COALESCE(completed_at, CURRENT_TIMESTAMP),
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = $3
           AND LOWER(download_id) = LOWER($4)
           AND active = 1",
    )
    .bind(ReleaseJobState::Failed.as_str())
    .bind(reason)
    .bind(release.release_id.to_string())
    .bind(&canonical_hash)
    .execute(&mut *transaction)
    .await?;
    if job_update.rows_affected() == 0 {
        anyhow::bail!(
            "anime qBittorrent retry could not terminalize its active release job for hash {torrent_hash}"
        );
    }
    transaction.commit().await?;
    Ok(())
}

async fn reconcile_anime_qbittorrent_retry_bookkeeping(
    pool: &sqlx::AnyPool,
    release_id: Uuid,
    torrent_hash: &str,
) -> anyhow::Result<usize> {
    let canonical_hash = qbittorrent_trackable_info_hash(torrent_hash)
        .context("anime qBittorrent retry reconciliation has no canonical torrent hash")?;
    let release = get_release(pool, release_id)
        .await?
        .context("anime qBittorrent retry reconciliation release disappeared")?;
    if release.state != AcquisitionReleaseState::Failed
        || !anime_qbittorrent_cleanup_pending(&release)
        || release
            .download_id
            .as_deref()
            .and_then(qbittorrent_trackable_info_hash)
            .as_deref()
            != Some(canonical_hash.as_str())
    {
        return Ok(0);
    }

    let coverage = list_release_coverage(pool, release_id).await?;
    let all_targets = match release.subscription_id {
        Some(subscription_id) => list_subscription_targets(pool, subscription_id).await?,
        None => Vec::new(),
    };
    let targets =
        qbittorrent_release_scoped_targets(&release, &all_targets, &coverage, &canonical_hash);
    let scoped_target_ids = targets
        .iter()
        .map(|target| target.target_id)
        .collect::<BTreeSet<_>>();
    let retry_reason_code = release
        .coverage_plan
        .as_ref()
        .and_then(|plan| plan.pointer("/automaticRetry/reason"))
        .and_then(Value::as_str)
        .unwrap_or("anime_qbittorrent_file_mapping_unresolved");
    let reason = release.state_reason.as_deref().unwrap_or(
        "Anime files could not be mapped safely. Elixir will try the next release automatically.",
    );

    for file in list_release_files(pool, release_id).await? {
        update_release_file_selected(pool, file.release_file_id, false).await?;
    }
    for entry in coverage
        .iter()
        .filter(|entry| scoped_target_ids.contains(&entry.target_id))
    {
        update_release_coverage_review_state(
            pool,
            entry.coverage_id,
            ReleaseCoverageState::Rejected,
            Some(retry_reason_code.to_string()),
            Some("alm7_qbittorrent_automatic_retry".to_string()),
        )
        .await?;
    }

    let now = chrono::Utc::now();
    let mut reset = 0usize;
    for target in targets {
        let target_still_owns_attempt = target
            .download_id
            .as_deref()
            .and_then(qbittorrent_trackable_info_hash)
            .as_deref()
            == Some(canonical_hash.as_str())
            && target.selected_route_logical_id.as_deref() == Some(TORRENT_DEFAULT_LOGICAL_ID);
        if !target_still_owns_attempt {
            continue;
        }
        let retry_after = now
            + chrono::Duration::seconds(
                QBITTORRENT_STALE_RELEASE_RETRY_SECONDS
                    + i64::from(target.target_id.as_bytes()[0] % 15),
            );
        if reset_target_for_candidate_retry(pool, target.target_id, reason.to_string(), retry_after)
            .await?
            .is_some()
        {
            reset += 1;
        }
    }
    Ok(reset)
}

fn merge_anime_qbittorrent_retry_evidence(
    coverage_plan: Value,
    torrent_hash: &str,
    now: chrono::DateTime<chrono::Utc>,
    retry_reason_code: &str,
    suppress_automatic_rediscovery: bool,
) -> Value {
    let automatic_retry = json!({
        "status": "scheduled",
        "reason": retry_reason_code,
        "torrentHash": torrent_hash,
        "scheduledAt": now,
        "policyVersion": "alm7-qbittorrent-anime-retry-v1",
    });
    let retry_suppression = json!({
        "status": if suppress_automatic_rediscovery { "rejected" } else { "retryable" },
        "suppressAutomaticRediscovery": suppress_automatic_rediscovery,
        "reason": retry_reason_code,
        "failedAt": now,
    });
    let torrent_cleanup = json!({
        "status": "pending",
        "torrentHash": torrent_hash,
        "deleteFiles": false,
        "requestedAt": now,
        "policyVersion": "alm7-qbittorrent-cleanup-v1",
    });
    match coverage_plan {
        Value::Object(mut object) => {
            object.insert("automaticRetry".to_string(), automatic_retry);
            object.insert("retrySuppression".to_string(), retry_suppression);
            object.insert("torrentCleanup".to_string(), torrent_cleanup);
            Value::Object(object)
        }
        value => json!({
            "previousCoveragePlan": value,
            "automaticRetry": automatic_retry,
            "retrySuppression": retry_suppression,
            "torrentCleanup": torrent_cleanup,
        }),
    }
}

fn anime_qbittorrent_cleanup_pending(release: &AcquisitionRelease) -> bool {
    release
        .coverage_plan
        .as_ref()
        .and_then(|plan| plan.pointer("/torrentCleanup/status"))
        .and_then(Value::as_str)
        == Some("pending")
}

async fn mark_anime_qbittorrent_cleanup_completed(
    pool: &sqlx::AnyPool,
    release_id: Uuid,
    torrent_hash: &str,
) -> anyhow::Result<()> {
    let Some(release) = get_release(pool, release_id).await? else {
        return Ok(());
    };
    if release.state != AcquisitionReleaseState::Failed
        || !anime_qbittorrent_cleanup_pending(&release)
    {
        return Ok(());
    }
    let original_coverage_plan = release.coverage_plan.clone().unwrap_or_else(|| json!({}));
    let original_coverage_plan_json = serde_json::to_string(&original_coverage_plan)?;
    let completed = json!({
        "status": "completed",
        "torrentHash": torrent_hash,
        "deleteFiles": false,
        "completedAt": chrono::Utc::now(),
        "policyVersion": "alm7-qbittorrent-cleanup-v1",
    });
    let coverage_plan = match original_coverage_plan {
        Value::Object(mut object) => {
            object.insert("torrentCleanup".to_string(), completed);
            Value::Object(object)
        }
        value => json!({
            "previousCoveragePlan": value,
            "torrentCleanup": completed,
        }),
    };
    let updated = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_releases
         SET coverage_plan_json = $1,
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = $2
           AND state = 'failed'
           AND LOWER(download_id) = LOWER($3)
           AND coverage_plan_json = $4",
    )
    .bind(serde_json::to_string(&coverage_plan)?)
    .bind(release.release_id.to_string())
    .bind(torrent_hash)
    .bind(original_coverage_plan_json)
    .execute(pool)
    .await?;
    if updated.rows_affected() == 1 {
        return Ok(());
    }
    let current = get_release(pool, release_id).await?;
    if current.as_ref().is_some_and(|release| {
        release
            .coverage_plan
            .as_ref()
            .and_then(|plan| plan.pointer("/torrentCleanup/status"))
            .and_then(Value::as_str)
            == Some("completed")
    }) {
        return Ok(());
    }
    anyhow::bail!("anime qBittorrent cleanup completion lost ownership of release {release_id}")
}

fn anime_qbittorrent_retry_suppresses_rediscovery(
    refinement: &QbittorrentCoverageRefinement,
) -> bool {
    refinement.suppress_automatic_rediscovery
        || refinement
            .anime_match_assist
            .as_ref()
            .is_some_and(|assist| assist.result != AnimeMatchAssistResult::Fallback)
}

async fn persist_qbittorrent_priority_decision(
    pool: &sqlx::AnyPool,
    release: &AcquisitionRelease,
    files: &[AcquisitionReleaseFile],
    coverage: &[AcquisitionReleaseCoverage],
    refinement: &QbittorrentCoverageRefinement,
    decision: &QbittorrentFilePriorityDecision,
    priority_applied: bool,
) -> anyhow::Result<()> {
    let current = get_release(pool, release.release_id)
        .await?
        .context("qBittorrent priority release disappeared")?;
    if matches!(
        current.state,
        AcquisitionReleaseState::Failed
            | AcquisitionReleaseState::Cancelled
            | AcquisitionReleaseState::Completed
    ) || qbittorrent_priority_bookkeeping_pending(&current)
        || !same_optional_qbittorrent_hash(
            current.download_id.as_deref(),
            release.download_id.as_deref(),
        )
    {
        anyhow::bail!("qBittorrent priority decision no longer owns the active release attempt");
    }
    let selected_ids = decision
        .selected_file_ids
        .iter()
        .cloned()
        .collect::<HashSet<_>>();
    for file in files {
        update_release_file_selected(
            pool,
            file.release_file_id,
            qbittorrent_file_key(file)
                .map(|file_id| selected_ids.contains(&file_id))
                .unwrap_or(false),
        )
        .await?;
    }
    for entry in coverage {
        let selected = entry
            .release_file_id
            .and_then(|release_file_id| {
                files
                    .iter()
                    .find(|file| file.release_file_id == release_file_id)
            })
            .and_then(qbittorrent_file_key)
            .map(|file_id| selected_ids.contains(&file_id))
            .unwrap_or(false);
        upsert_release_coverage(
            pool,
            NewAcquisitionReleaseCoverage {
                coverage_id: Some(entry.coverage_id),
                release_id: entry.release_id,
                release_file_id: entry.release_file_id,
                target_id: entry.target_id,
                coverage_kind: entry.coverage_kind,
                confidence: entry.confidence,
                score: entry.score,
                reason: entry.reason.clone(),
                state: if decision.is_approved() && selected {
                    ReleaseCoverageState::Selected
                } else if decision.is_approved() {
                    entry.state
                } else {
                    ReleaseCoverageState::ReviewRequired
                },
                verified_by: entry.verified_by.clone(),
            },
        )
        .await?;
    }

    let release_state = if decision.is_approved() {
        AcquisitionReleaseState::Ready
    } else {
        AcquisitionReleaseState::ReviewRequired
    };
    let job_state = if decision.is_approved() {
        ReleaseJobState::Ready
    } else {
        ReleaseJobState::Staging
    };
    let reason = if decision.is_approved() {
        "RR-5B deterministic qBittorrent file priorities approved."
    } else {
        "RR-5B deterministic qBittorrent file priorities require review."
    };
    let coverage_plan = merge_qbittorrent_policy_evidence(
        refinement.coverage_plan.clone(),
        decision,
        priority_applied,
    );
    let torrent_hash = release
        .download_id
        .as_deref()
        .context("qBittorrent priority decision is missing its torrent hash")?;
    update_qbittorrent_release_refinement(
        pool,
        release.release_id,
        torrent_hash,
        refinement,
        release_state,
        reason,
        coverage_plan,
    )
    .await?;
    update_qbittorrent_release_job_state(
        pool,
        release.release_id,
        release.download_id.as_deref(),
        job_state,
        reason,
    )
    .await?;
    sync_qbittorrent_target_states(pool, release, files, coverage, decision, priority_applied)
        .await?;
    Ok(())
}

async fn sync_qbittorrent_target_states(
    pool: &sqlx::AnyPool,
    release: &AcquisitionRelease,
    files: &[AcquisitionReleaseFile],
    coverage: &[AcquisitionReleaseCoverage],
    decision: &QbittorrentFilePriorityDecision,
    priority_applied: bool,
) -> anyhow::Result<()> {
    let selected_ids = decision
        .selected_file_ids
        .iter()
        .cloned()
        .collect::<HashSet<_>>();
    let mut touched_targets = BTreeSet::new();
    if decision.is_approved() {
        let files_by_id = files
            .iter()
            .map(|file| (file.release_file_id, file))
            .collect::<HashMap<_, _>>();
        for entry in coverage {
            let selected = entry
                .release_file_id
                .and_then(|release_file_id| files_by_id.get(&release_file_id))
                .and_then(|file| {
                    qbittorrent_file_key(file).map(|file_id| selected_ids.contains(&file_id))
                })
                .unwrap_or(entry.state == ReleaseCoverageState::Selected);
            if !selected || !touched_targets.insert(entry.target_id) {
                continue;
            }
            update_target_state(
                pool,
                entry.target_id,
                AcquisitionTargetStateUpdate {
                    state: AcquisitionTargetState::Submitted,
                    state_reason: Some(if priority_applied {
                        "qBittorrent file priorities applied and torrent resumed.".to_string()
                    } else {
                        "qBittorrent staged file selection approved.".to_string()
                    }),
                    selected_provider_id: release.source_provider_id,
                    selected_route_logical_id: release.selected_route_logical_id.clone(),
                    selected_candidate: release.selected_candidate.clone(),
                    download_id: release.download_id.clone(),
                    next_search_after: None,
                    increment_search_attempts: false,
                    ..Default::default()
                },
            )
            .await?;
        }
    } else {
        let reason = if decision.review_reasons.is_empty() {
            "qBittorrent staged file selection requires review.".to_string()
        } else {
            format!(
                "qBittorrent staged file selection requires review: {}.",
                decision.review_reasons.join(", ")
            )
        };
        for entry in coverage {
            if !touched_targets.insert(entry.target_id) {
                continue;
            }
            update_target_state(
                pool,
                entry.target_id,
                AcquisitionTargetStateUpdate {
                    state: AcquisitionTargetState::Blocked,
                    state_reason: Some(reason.clone()),
                    selected_provider_id: release.source_provider_id,
                    selected_route_logical_id: release.selected_route_logical_id.clone(),
                    selected_candidate: release.selected_candidate.clone(),
                    download_id: release.download_id.clone(),
                    next_search_after: None,
                    increment_search_attempts: false,
                    ..Default::default()
                },
            )
            .await?;
        }
    }
    Ok(())
}

async fn update_qbittorrent_release_refinement(
    pool: &sqlx::AnyPool,
    release_id: Uuid,
    torrent_hash: &str,
    refinement: &QbittorrentCoverageRefinement,
    state: AcquisitionReleaseState,
    reason: &str,
    coverage_plan: Value,
) -> anyhow::Result<()> {
    let coverage_plan_json = serde_json::to_string(&coverage_plan)?;
    let updated = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_releases
         SET release_kind = $1,
             resolver_kind = $2,
             resolver_version = $3,
             confidence = $4,
             state = $5,
             state_reason = $6,
             coverage_plan_json = $7,
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = $8
           AND state NOT IN ('failed', 'cancelled', 'completed')
           AND LOWER(download_id) = LOWER($9)",
    )
    .bind(refinement.release_kind.as_str())
    .bind(refinement.resolver_kind.as_str())
    .bind(refinement.resolver_version.as_str())
    .bind(refinement.confidence.as_str())
    .bind(state.as_str())
    .bind(reason)
    .bind(coverage_plan_json)
    .bind(release_id.to_string())
    .bind(torrent_hash)
    .execute(pool)
    .await?;
    if updated.rows_affected() != 1 {
        anyhow::bail!("qBittorrent refinement lost ownership of the active release attempt");
    }
    Ok(())
}

async fn update_qbittorrent_release_job_state(
    pool: &sqlx::AnyPool,
    release_id: Uuid,
    download_id: Option<&str>,
    state: ReleaseJobState,
    reason: &str,
) -> anyhow::Result<()> {
    let Some(download_id) = download_id else {
        return Ok(());
    };
    let updated = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_release_jobs
         SET state = $1,
             state_reason = $2,
             active = $3,
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = $4
           AND LOWER(download_id) = LOWER($5)
           AND active = 1",
    )
    .bind(state.as_str())
    .bind(reason)
    .bind(
        if matches!(
            state,
            ReleaseJobState::Completed | ReleaseJobState::Failed | ReleaseJobState::Cancelled
        ) {
            0_i64
        } else {
            1_i64
        },
    )
    .bind(release_id.to_string())
    .bind(download_id)
    .execute(pool)
    .await?;
    if updated.rows_affected() != 1 {
        anyhow::bail!("qBittorrent release job update lost ownership of the active attempt");
    }
    Ok(())
}

async fn mark_qbittorrent_release_resumed(
    pool: &sqlx::AnyPool,
    release: &AcquisitionRelease,
    torrent_hash: &str,
) -> anyhow::Result<()> {
    let mut transaction = pool.begin().await?;
    let updated = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_releases
         SET state = $1,
             state_reason = $2,
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = $3
           AND state = 'ready'
           AND LOWER(download_id) = LOWER($4)",
    )
    .bind(AcquisitionReleaseState::Downloading.as_str())
    .bind("qBittorrent accepted deterministic file priorities and resumed.")
    .bind(release.release_id.to_string())
    .bind(torrent_hash)
    .execute(&mut *transaction)
    .await?;
    if updated.rows_affected() != 1 {
        anyhow::bail!("qBittorrent resume lost ownership of the active release attempt");
    }
    let job_update = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_release_jobs
         SET state = $1,
             state_reason = $2,
             active = 1,
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = $3
           AND LOWER(download_id) = LOWER($4)
           AND active = 1",
    )
    .bind(ReleaseJobState::Downloading.as_str())
    .bind("qBittorrent accepted deterministic file priorities and resumed.")
    .bind(release.release_id.to_string())
    .bind(torrent_hash)
    .execute(&mut *transaction)
    .await?;
    if job_update.rows_affected() != 1 {
        anyhow::bail!("qBittorrent resume lost ownership of the active release job");
    }
    transaction.commit().await?;
    Ok(())
}

async fn mark_qbittorrent_release_completed(
    pool: &sqlx::AnyPool,
    release: &AcquisitionRelease,
    torrent_hash: &str,
    reason: &str,
    coverage_plan: Value,
) -> anyhow::Result<()> {
    terminalize_qbittorrent_release_and_job(
        pool,
        release,
        torrent_hash,
        AcquisitionReleaseState::Completed,
        ReleaseJobState::Completed,
        reason,
        coverage_plan,
    )
    .await
}

async fn mark_qbittorrent_release_failed(
    pool: &sqlx::AnyPool,
    release: &AcquisitionRelease,
    torrent_hash: &str,
    reason: &str,
    coverage_plan: Value,
) -> anyhow::Result<()> {
    terminalize_qbittorrent_release_and_job(
        pool,
        release,
        torrent_hash,
        AcquisitionReleaseState::Failed,
        ReleaseJobState::Failed,
        reason,
        coverage_plan,
    )
    .await
}

async fn mark_qbittorrent_release_cancelled(
    pool: &sqlx::AnyPool,
    release: &AcquisitionRelease,
    torrent_hash: &str,
    delete_files: bool,
) -> anyhow::Result<()> {
    let evidence = json!({
        "runtimeState": "cancelled",
        "torrentHash": torrent_hash,
        "deleteFiles": delete_files,
        "message": "qBittorrent staged acquisition job was cancelled."
    });
    let coverage_plan = match release.coverage_plan.clone() {
        Some(Value::Object(mut object)) => {
            object.insert("torrentRuntime".to_string(), evidence);
            Value::Object(object)
        }
        Some(value) => json!({
            "previousCoveragePlan": value,
            "torrentRuntime": evidence
        }),
        None => json!({ "torrentRuntime": evidence }),
    };
    terminalize_qbittorrent_release_and_job(
        pool,
        release,
        torrent_hash,
        AcquisitionReleaseState::Cancelled,
        ReleaseJobState::Cancelled,
        "qBittorrent staged acquisition job was cancelled.",
        coverage_plan,
    )
    .await
}

async fn terminalize_qbittorrent_release_and_job(
    pool: &sqlx::AnyPool,
    release: &AcquisitionRelease,
    torrent_hash: &str,
    release_state: AcquisitionReleaseState,
    job_state: ReleaseJobState,
    reason: &str,
    coverage_plan: Value,
) -> anyhow::Result<()> {
    let mut transaction = pool.begin().await?;
    let release_update = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_releases
         SET state = $1,
             state_reason = $2,
             coverage_plan_json = $3,
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = $4
           AND LOWER(download_id) = LOWER($5)
           AND state = $6
           AND state NOT IN ('failed', 'cancelled', 'completed')",
    )
    .bind(release_state.as_str())
    .bind(reason)
    .bind(serde_json::to_string(&coverage_plan)?)
    .bind(release.release_id.to_string())
    .bind(torrent_hash)
    .bind(release.state.as_str())
    .execute(&mut *transaction)
    .await?;
    if release_update.rows_affected() != 1 {
        anyhow::bail!("qBittorrent terminal transition lost ownership of the active release");
    }
    let job_update = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_release_jobs
         SET state = $1,
             state_reason = $2,
             active = 0,
             completed_at = COALESCE(completed_at, CURRENT_TIMESTAMP),
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = $3
           AND LOWER(download_id) = LOWER($4)
           AND active = 1",
    )
    .bind(job_state.as_str())
    .bind(reason)
    .bind(release.release_id.to_string())
    .bind(torrent_hash)
    .execute(&mut *transaction)
    .await?;
    if job_update.rows_affected() != 1 {
        anyhow::bail!("qBittorrent terminal transition lost ownership of the active release job");
    }
    transaction.commit().await?;
    Ok(())
}

async fn update_qbittorrent_release_state_only(
    pool: &sqlx::AnyPool,
    release_id: Uuid,
    torrent_hash: &str,
    expected_state: AcquisitionReleaseState,
    state: AcquisitionReleaseState,
    reason: &str,
    coverage_plan: Value,
) -> anyhow::Result<()> {
    let coverage_plan_json = serde_json::to_string(&coverage_plan)?;
    let updated = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_releases
         SET state = $1,
             state_reason = $2,
             coverage_plan_json = $3,
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = $4
           AND LOWER(download_id) = LOWER($5)
           AND state NOT IN ('failed', 'cancelled', 'completed')
           AND state = $6",
    )
    .bind(state.as_str())
    .bind(reason)
    .bind(coverage_plan_json)
    .bind(release_id.to_string())
    .bind(torrent_hash)
    .bind(expected_state.as_str())
    .execute(pool)
    .await?;
    if updated.rows_affected() != 1 {
        anyhow::bail!(
            "qBittorrent release state update lost ownership of the active release attempt"
        );
    }
    Ok(())
}

async fn update_http_stream_release_state_only(
    pool: &sqlx::AnyPool,
    release_id: Uuid,
    state: AcquisitionReleaseState,
    reason: &str,
    coverage_plan: Value,
) -> anyhow::Result<()> {
    let coverage_plan_json = serde_json::to_string(&coverage_plan)?;
    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_releases
         SET state = $1,
             state_reason = $2,
             coverage_plan_json = $3,
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = $4",
    )
    .bind(state.as_str())
    .bind(reason)
    .bind(coverage_plan_json)
    .bind(release_id.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

async fn update_release_file_selected(
    pool: &sqlx::AnyPool,
    release_file_id: Uuid,
    selected: bool,
) -> anyhow::Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_release_files
         SET selected = $1, updated_at = CURRENT_TIMESTAMP
         WHERE release_file_id = $2",
    )
    .bind(if selected { 1_i64 } else { 0_i64 })
    .bind(release_file_id.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

async fn queue_qbittorrent_anime_hashes_for_completed_release(
    pool: &sqlx::AnyPool,
    release: &AcquisitionRelease,
    files: &[AcquisitionReleaseFile],
) -> anyhow::Result<()> {
    if release.media_type != MediaType::Anime {
        return Ok(());
    }
    for file in files.iter().filter(|file| file.selected == Some(true)) {
        let Some(local_path) = file
            .provider_metadata
            .as_ref()
            .and_then(|value| value.get("localPath"))
            .and_then(Value::as_str)
        else {
            continue;
        };
        queue_anime_hash_file(
            pool,
            HashFileJob {
                release_file_id: Some(file.release_file_id),
                local_file_id: Some(format!("release-file:{}", file.release_file_id)),
                file_path: PathBuf::from(local_path),
                force_rehash: false,
            },
        )
        .await?;
    }
    Ok(())
}

fn merge_qbittorrent_policy_evidence(
    mut coverage_plan: Value,
    decision: &QbittorrentFilePriorityDecision,
    priority_applied: bool,
) -> Value {
    let evidence = json!({
        "policyVersion": decision.policy_version,
        "status": if decision.is_approved() { "approved" } else { "review_required" },
        "priorityApplied": priority_applied,
        "selectedFileIds": decision.selected_file_ids,
        "skippedFileIds": decision.skipped_file_ids,
        "wantedPriority": decision.wanted_priority,
        "skippedPriority": decision.skipped_priority,
        "coverageFingerprint": decision.coverage_fingerprint,
        "reviewReasons": decision.review_reasons,
        "userApproved": decision.user_approved,
    });
    match coverage_plan {
        Value::Object(ref mut object) => {
            object.insert("priorityPolicy".to_string(), evidence);
            coverage_plan
        }
        value => json!({
            "previousCoveragePlan": value,
            "priorityPolicy": evidence
        }),
    }
}

fn qbittorrent_tv_release_file_input(file: &AcquisitionReleaseFile) -> Option<TvReleaseFileInput> {
    Some(TvReleaseFileInput {
        file_id: qbittorrent_file_key(file)?,
        path: file.path.clone(),
        size_bytes: file.size_bytes,
        selectable: file.selectable,
    })
}

fn qbittorrent_release_scoped_targets(
    release: &AcquisitionRelease,
    targets: &[AcquisitionTarget],
    existing_coverage: &[AcquisitionReleaseCoverage],
    torrent_hash: &str,
) -> Vec<AcquisitionTarget> {
    if let Some(scoped_targets) = qbittorrent_request_scope_targets(release, targets) {
        if scoped_targets.is_empty() {
            return Vec::new();
        }
        let scoped_ids = scoped_targets
            .iter()
            .map(|target| target.target_id)
            .collect::<BTreeSet<_>>();
        let covered_ids = existing_coverage
            .iter()
            .filter(|coverage| coverage.state != ReleaseCoverageState::Rejected)
            .map(|coverage| coverage.target_id)
            .collect::<BTreeSet<_>>();
        let bound_ids = targets
            .iter()
            .filter(|target| {
                target
                    .download_id
                    .as_deref()
                    .map(|value| value.eq_ignore_ascii_case(torrent_hash))
                    .unwrap_or(false)
                    && target.selected_route_logical_id.as_deref()
                        == Some(TORRENT_DEFAULT_LOGICAL_ID)
            })
            .map(|target| target.target_id)
            .collect::<BTreeSet<_>>();
        if (!covered_ids.is_empty() && covered_ids != scoped_ids)
            || (!bound_ids.is_empty() && bound_ids != scoped_ids)
        {
            return Vec::new();
        }
        return scoped_targets;
    }

    let covered_target_ids = existing_coverage
        .iter()
        .filter(|coverage| coverage.state != ReleaseCoverageState::Rejected)
        .map(|coverage| coverage.target_id)
        .collect::<BTreeSet<_>>();
    if !covered_target_ids.is_empty() {
        return targets
            .iter()
            .filter(|target| covered_target_ids.contains(&target.target_id))
            .cloned()
            .collect();
    }

    let bound_targets = targets
        .iter()
        .filter(|target| {
            target
                .download_id
                .as_deref()
                .map(|value| value.eq_ignore_ascii_case(torrent_hash))
                .unwrap_or(false)
                && target.selected_route_logical_id.as_deref() == Some(TORRENT_DEFAULT_LOGICAL_ID)
        })
        .cloned()
        .collect::<Vec<_>>();
    if !bound_targets.is_empty() {
        return bound_targets;
    }

    if targets.len() == 1 {
        return targets.to_vec();
    }
    Vec::new()
}

fn qbittorrent_request_scope_targets(
    release: &AcquisitionRelease,
    targets: &[AcquisitionTarget],
) -> Option<Vec<AcquisitionTarget>> {
    let evidence = release.selected_candidate.as_ref().and_then(|candidate| {
        candidate
            .get("requestScopeEvidence")
            .or_else(|| candidate.get("request_scope_evidence"))
    })?;
    let ids_value = evidence
        .get("targetIds")
        .or_else(|| evidence.get("target_ids"));
    let keys_value = evidence
        .get("targetKeys")
        .or_else(|| evidence.get("target_keys"));
    let ids_present = ids_value.is_some();
    let keys_present = keys_value.is_some();
    if !ids_present && !keys_present {
        return Some(Vec::new());
    }
    let mut scoped_ids = BTreeSet::new();
    if let Some(ids) = ids_value {
        let Some(ids) = ids.as_array() else {
            return Some(Vec::new());
        };
        if ids.is_empty() {
            return Some(Vec::new());
        }
        for value in ids {
            let Some(value) = value.as_str() else {
                return Some(Vec::new());
            };
            let Ok(target_id) = Uuid::parse_str(value.trim()) else {
                return Some(Vec::new());
            };
            if !targets.iter().any(|target| target.target_id == target_id) {
                return Some(Vec::new());
            }
            scoped_ids.insert(target_id);
        }
    }
    let mut keyed_ids = BTreeSet::new();
    if let Some(keys) = keys_value {
        let Some(keys) = keys.as_array() else {
            return Some(Vec::new());
        };
        if keys.is_empty() {
            return Some(Vec::new());
        }
        for value in keys {
            let Some(key) = value.as_str().map(str::trim).filter(|key| !key.is_empty()) else {
                return Some(Vec::new());
            };
            let matching = targets
                .iter()
                .filter(|target| target.target_key == key)
                .map(|target| target.target_id)
                .collect::<BTreeSet<_>>();
            if matching.len() != 1 {
                return Some(Vec::new());
            }
            keyed_ids.extend(matching);
        }
    }
    if ids_present && keys_present && scoped_ids != keyed_ids {
        return Some(Vec::new());
    }
    let selected_ids = if ids_present { scoped_ids } else { keyed_ids };
    Some(
        targets
            .iter()
            .filter(|target| selected_ids.contains(&target.target_id))
            .cloned()
            .collect(),
    )
}

fn qbittorrent_anime_acquisition_candidate(
    release: &AcquisitionRelease,
    files: &[AcquisitionReleaseFile],
) -> AcquisitionCandidate {
    let mut candidate = release
        .selected_candidate
        .as_ref()
        .and_then(|value| serde_json::from_value::<AcquisitionCandidate>(value.clone()).ok())
        .unwrap_or_else(|| AcquisitionCandidate {
            id: None,
            title: release.release_title.clone(),
            source: release.source.clone(),
            source_kind: release.source_kind.clone(),
            info_hash: release.info_hash.clone(),
            file_index: None,
            quality: None,
            size_bytes: None,
            seeders: None,
            language: None,
            cached_debrid: None,
            rank: None,
            score: release.score,
            score_badges: Vec::new(),
            files: Vec::new(),
            supported_routes: vec![TORRENT_DEFAULT_LOGICAL_ID.to_string()],
            default_route: Some(TORRENT_DEFAULT_LOGICAL_ID.to_string()),
            raw: None,
        });
    candidate.title = release.release_title.clone();
    candidate.source = release.source.clone();
    candidate.source_kind = release.source_kind.clone();
    candidate.info_hash = release.info_hash.clone().or(candidate.info_hash);
    if !candidate
        .supported_routes
        .iter()
        .any(|route| route == TORRENT_DEFAULT_LOGICAL_ID)
    {
        candidate
            .supported_routes
            .push(TORRENT_DEFAULT_LOGICAL_ID.to_string());
    }
    candidate.default_route = Some(TORRENT_DEFAULT_LOGICAL_ID.to_string());
    candidate.files = files
        .iter()
        .filter_map(|file| {
            Some(AcquisitionCandidateFile {
                file_id: Some(qbittorrent_file_key(file)?),
                file_index: file.file_index,
                path: file.path.clone(),
                size_bytes: file.size_bytes.and_then(|value| u64::try_from(value).ok()),
                selectable: Some(file.selectable),
            })
        })
        .collect();
    candidate
}

fn qbittorrent_anime_candidate_input(candidate: &AcquisitionCandidate) -> AnimeCandidateInput {
    AnimeCandidateInput {
        title: candidate.title.clone(),
        source_kind: candidate.source_kind.clone(),
        quality: candidate.quality.clone(),
        size_bytes: candidate.size_bytes,
        seeders: candidate.seeders,
        cached_debrid: candidate.cached_debrid,
        rank: candidate.rank,
        source_score: candidate.score,
        supported_routes: candidate.supported_routes.clone(),
        default_route: candidate.default_route.clone(),
    }
}

fn anime_qbittorrent_plan_is_automatic(
    plan: &AnimeFileCoveragePlan,
    targets: &[AcquisitionTarget],
) -> bool {
    let wanted_target_keys = targets
        .iter()
        .map(|target| target.target_key.as_str())
        .collect::<BTreeSet<_>>();
    let planned_target_keys = plan
        .entries
        .iter()
        .map(|entry| entry.target_key.as_str())
        .collect::<BTreeSet<_>>();
    !wanted_target_keys.is_empty()
        && wanted_target_keys == planned_target_keys
        && planned_target_keys.len() == plan.entries.len()
        && plan.confidence == ReleaseConfidence::High
        && !plan.entries.is_empty()
        && !plan.selected_file_keys.is_empty()
        && plan.review_reasons.is_empty()
        && plan.rejection_reasons.is_empty()
        && plan.entries.iter().all(|entry| {
            entry.confidence == ReleaseConfidence::High
                && entry.release_file_key.is_some()
                && !matches!(
                    entry.state,
                    ReleaseCoverageState::ReviewRequired | ReleaseCoverageState::Rejected
                )
        })
}

fn anime_deterministic_fast_path_provenance() -> AnimeMatchAssistProvenance {
    AnimeMatchAssistProvenance {
        source: AnimeMatchAssistSource::DeterministicFastPath,
        result: AnimeMatchAssistResult::Definitive,
        matcher_schema_version: ANIME_MATCH_SCHEMA_VERSION,
        request_fingerprint: None,
        reason: None,
        detail: None,
        runtime: None,
        latency_ms: 0,
    }
}

fn anime_invalid_request_fallback_provenance(detail: String) -> AnimeMatchAssistProvenance {
    AnimeMatchAssistProvenance {
        source: AnimeMatchAssistSource::DeterministicFallback,
        result: AnimeMatchAssistResult::Fallback,
        matcher_schema_version: ANIME_MATCH_SCHEMA_VERSION,
        request_fingerprint: None,
        reason: Some(AnimeMatchFallbackReason::InvalidRequest),
        detail: Some(detail),
        runtime: None,
        latency_ms: 0,
    }
}

fn qbittorrent_anime_release_file_input(
    file: &AcquisitionReleaseFile,
) -> Option<AnimeReleaseFileInput> {
    let file_key = qbittorrent_file_key(file)?;
    Some(AnimeReleaseFileInput {
        file_key: file_key.clone(),
        file_id: Some(file_key),
        file_index: file.file_index,
        path: file.path.clone(),
        size_bytes: file.size_bytes,
        selectable: file.selectable,
    })
}

fn qbittorrent_movie_release_file_input(
    file: &AcquisitionReleaseFile,
) -> Option<MovieReleaseFileSelectionInput> {
    Some(MovieReleaseFileSelectionInput {
        file_id: qbittorrent_file_key(file)?,
        path: file.path.clone(),
        size_bytes: qbittorrent_release_file_selection_size_bytes(file),
        selectable: file.selectable,
    })
}

fn qbittorrent_release_file_selection_size_bytes(file: &AcquisitionReleaseFile) -> Option<i64> {
    file.size_bytes
        .filter(|size| *size >= 0)
        .or_else(|| {
            file.provider_metadata
                .as_ref()
                .and_then(|metadata| metadata.get("sizeBytes"))
                .and_then(|value| {
                    value
                        .as_i64()
                        .filter(|size| *size >= 0)
                        .or_else(|| value.as_u64().and_then(u64_to_i64))
                })
        })
        .or_else(|| {
            file.raw
                .as_ref()
                .and_then(|raw| number_field(raw, "size"))
                .and_then(u64_to_i64)
        })
}

fn release_file_ids_by_provider_file_id(files: &[AcquisitionReleaseFile]) -> HashMap<String, Uuid> {
    files
        .iter()
        .filter_map(|file| Some((qbittorrent_file_key(file)?, file.release_file_id)))
        .collect()
}

fn selectable_qbittorrent_media_files(
    files: &[AcquisitionReleaseFile],
) -> Vec<&AcquisitionReleaseFile> {
    files
        .iter()
        .filter(|file| file.selectable)
        .filter(|file| classify_release_file_path(&file.path) == "media")
        .collect()
}

fn qbittorrent_file_key(file: &AcquisitionReleaseFile) -> Option<String> {
    file.provider_file_id
        .clone()
        .or_else(|| file.file_id.clone())
        .or_else(|| file.file_index.map(|index| index.to_string()))
}

fn anime_scoring_context_from_qbittorrent_release(
    subscription: Option<&AcquisitionSubscription>,
    release: &AcquisitionRelease,
    targets: &[AcquisitionTarget],
) -> AnimeCandidateScoringContext {
    let mut aliases = Vec::new();
    let mut scoped_aliases = HashMap::<String, AnimeScopedAlias>::new();
    if let Some(subscription) = subscription {
        push_unique_alias(&mut aliases, &subscription.title);
        if let Some(scope) = subscription.scope.as_ref() {
            if let Some(title) = scope.pointer("/media/title").and_then(Value::as_str) {
                push_unique_alias(&mut aliases, title);
            }
            push_aliases_from_json(&mut aliases, scope.pointer("/media/aliases"));
        }
    }
    push_unique_alias(&mut aliases, &release.title);
    for target in targets {
        push_unique_alias(&mut aliases, &target.title);
        if let Some(metadata) = target.metadata.as_ref() {
            for key in ["aliases", "titles", "anilistTitles"] {
                push_aliases_from_json(&mut aliases, metadata.get(key));
            }
            for key in ["scopedAliases", "scoped_aliases"] {
                push_scoped_aliases_from_json(&mut scoped_aliases, metadata.get(key));
            }
        }
    }
    let graph_fingerprints = targets
        .iter()
        .filter_map(|target| metadata_json_string(target.metadata.as_ref(), "graphFingerprint"))
        .collect::<BTreeSet<_>>();
    let graph_fingerprint = match graph_fingerprints.len() {
        1 => graph_fingerprints.first().cloned(),
        0 => release
            .coverage_plan
            .as_ref()
            .and_then(|value| {
                value.get("graphFingerprint").or_else(|| {
                    value.pointer("/diagnostics/parserProvenance/graph/graphFingerprint")
                })
            })
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string),
        _ => None,
    };
    let mut scoped_aliases = scoped_aliases.into_values().collect::<Vec<_>>();
    scoped_aliases.sort_by(|left, right| {
        left.season_number
            .cmp(&right.season_number)
            .then_with(|| left.anilist_season_id.cmp(&right.anilist_season_id))
            .then_with(|| left.display.cmp(&right.display))
            .then_with(|| left.source.cmp(&right.source))
    });
    AnimeCandidateScoringContext {
        graph_fingerprint,
        aliases,
        scoped_aliases,
        targets: targets
            .iter()
            .map(|target| {
                let metadata = target.metadata.as_ref();
                AnimeCandidateTarget {
                    target_key: target.target_key.clone(),
                    canonical_key: metadata_json_string(metadata, "targetCanonicalKey"),
                    title: target.title.clone(),
                    season_number: target.season_number,
                    anilist_season_id: metadata_json_string(metadata, "anilistSeasonId"),
                    episode_number: target.episode_number,
                    absolute_episode_number: target.absolute_episode_number,
                    tvdb_episode_id: metadata_json_string(metadata, "tvdbEpisodeId"),
                    anidb_episode_id: metadata_json_string(metadata, "anidbEpisodeId"),
                }
            })
            .collect(),
    }
}

fn push_aliases_from_json(aliases: &mut Vec<String>, value: Option<&Value>) {
    let Some(values) = value.and_then(Value::as_array) else {
        return;
    };
    for value in values.iter().filter_map(Value::as_str) {
        push_unique_alias(aliases, value);
    }
}

fn push_scoped_aliases_from_json(
    aliases: &mut HashMap<String, AnimeScopedAlias>,
    value: Option<&Value>,
) {
    let Some(values) = value.and_then(Value::as_array) else {
        return;
    };
    for value in values {
        let Ok(alias) = serde_json::from_value::<AnimeScopedAlias>(value.clone()) else {
            continue;
        };
        let display = alias.display.trim();
        if display.is_empty()
            || (alias.season_number.is_none() && alias.anilist_season_id.is_none())
        {
            continue;
        }
        let key = format!(
            "{}:{}:{}:{}",
            display.to_ascii_lowercase(),
            alias.source.to_ascii_lowercase(),
            alias
                .season_number
                .map(|season| season.to_string())
                .unwrap_or_default(),
            alias
                .anilist_season_id
                .as_deref()
                .unwrap_or_default()
                .to_ascii_lowercase(),
        );
        aliases.entry(key).or_insert(alias);
    }
}

fn push_unique_alias(aliases: &mut Vec<String>, value: &str) {
    let trimmed = value.trim();
    if !trimmed.is_empty() && !aliases.iter().any(|alias| alias == trimmed) {
        aliases.push(trimmed.to_string());
    }
}

fn metadata_json_string(metadata: Option<&Value>, key: &str) -> Option<String> {
    metadata?
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn qbittorrent_coverage_fingerprint(
    release: &AcquisitionRelease,
    refinement: &QbittorrentCoverageRefinement,
    files: &[AcquisitionReleaseFile],
    coverage: &[AcquisitionReleaseCoverage],
) -> String {
    let mut file_rows = files
        .iter()
        .map(|file| {
            format!(
                "{}:{}:{}:{}",
                qbittorrent_file_key(file).unwrap_or_default(),
                file.path,
                file.size_bytes.unwrap_or_default(),
                file.selected.unwrap_or(false)
            )
        })
        .collect::<Vec<_>>();
    file_rows.sort();
    let mut coverage_rows = coverage
        .iter()
        .map(|entry| {
            format!(
                "{}:{}:{}:{}",
                entry.target_id,
                entry
                    .release_file_id
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
                entry.coverage_kind.as_str(),
                entry.confidence.as_str()
            )
        })
        .collect::<Vec<_>>();
    coverage_rows.sort();
    let mut hasher = Sha256::new();
    hasher.update(release.fingerprint.as_bytes());
    hasher.update(refinement.resolver_version.as_bytes());
    for row in file_rows {
        hasher.update(row.as_bytes());
        hasher.update(b"\n");
    }
    for row in coverage_rows {
        hasher.update(row.as_bytes());
        hasher.update(b"\n");
    }
    format!("sha256:{:x}", hasher.finalize())
}

async fn load_nzbget_progress(
    state: &AppState,
    store: &ExtensionStore<'_>,
    record: &DownloadBrokerProviderRecord,
) -> ApiResult<Vec<DownloadBrokerProgressItem>> {
    let payload = request_instance_service_json(
        state,
        store,
        record.instance_id,
        ReqwestMethod::POST,
        "jsonrpc",
        Some(json!({
            "version": "1.1",
            "method": "listgroups",
            "params": [0],
            "id": 1
        })),
    )
    .await
    .map_err(ApiError::from)?;
    ensure_nzbget_rpc_ok(&payload, "listgroups").map_err(ApiError::from)?;
    let groups = payload
        .get("result")
        .and_then(Value::as_array)
        .ok_or_else(|| ApiError::internal("nzbget listgroups response missing result array"))?;
    Ok(groups
        .iter()
        .filter_map(|group| {
            let id = group.get("NZBID").and_then(Value::as_i64)?.to_string();
            let downloaded_bytes = combine_hi_lo(
                number_field(group, "DownloadedSizeHi"),
                number_field(group, "DownloadedSizeLo"),
            )
            .or_else(|| number_field(group, "DownloadedSizeMB").map(|value| value * 1024 * 1024));
            let total_bytes = combine_hi_lo(
                number_field(group, "FileSizeHi"),
                number_field(group, "FileSizeLo"),
            );
            let remaining_bytes = combine_hi_lo(
                number_field(group, "RemainingSizeHi"),
                number_field(group, "RemainingSizeLo"),
            );
            Some(DownloadBrokerProgressItem {
                id,
                name: string_field(group, "NZBName").or_else(|| string_field(group, "NZBFilename")),
                state: string_field(group, "Status"),
                category: string_field(group, "Category"),
                local_path: None,
                progress: progress_fraction(downloaded_bytes, total_bytes),
                downloaded_bytes,
                total_bytes,
                remaining_bytes,
                download_rate_bps: None,
                upload_rate_bps: None,
                debrid: None,
                torrent: None,
                stream: None,
            })
        })
        .collect())
}

async fn load_debrid_broker_progress(
    state: &AppState,
    store: &ExtensionStore<'_>,
    record: &DownloadBrokerProviderRecord,
) -> ApiResult<Vec<DownloadBrokerProgressItem>> {
    if !is_debrid_service_implementation(record.implementation.as_deref()) {
        return Err(ApiError::conflict(
            "the selected debrid provider does not expose native debrid progress",
        ));
    }
    let items = load_debrid_progress(state, store, record.provider_id, record.instance_id)
        .await
        .map_err(ApiError::from)?;
    Ok(items
        .into_iter()
        .map(|item| DownloadBrokerProgressItem {
            id: item.id,
            name: item.name,
            state: item.state,
            category: item.category,
            local_path: item.local_path,
            progress: item.progress,
            downloaded_bytes: item.downloaded_bytes,
            total_bytes: item.total_bytes,
            remaining_bytes: item.remaining_bytes,
            download_rate_bps: item.download_rate_bps,
            upload_rate_bps: None,
            debrid: item.debrid.map(|evidence| DownloadBrokerDebridEvidence {
                provider_name: evidence.provider_name,
                provider_implementation: evidence.provider_implementation,
                provider_capabilities: evidence.provider_capabilities,
                provider_status: evidence.provider_status,
                remote_status: evidence.remote_status,
                selection_mode: evidence.selection_mode,
                selected_file_count: evidence.selected_file_count,
                skipped_file_count: evidence.skipped_file_count,
                review_reasons: evidence.review_reasons,
                failure_class: evidence.failure_class,
                last_error: evidence.last_error,
                fallback_state: evidence.fallback_state,
            }),
            torrent: None,
            stream: None,
        })
        .collect())
}

async fn load_http_stream_broker_progress(
    pool: &sqlx::AnyPool,
    record: &DownloadBrokerProviderRecord,
) -> ApiResult<Vec<DownloadBrokerProgressItem>> {
    let releases = list_active_releases_by_route(pool, HTTP_STREAM_DEFAULT_LOGICAL_ID, 100)
        .await
        .map_err(ApiError::from)?;
    let mut items = Vec::with_capacity(releases.len());
    for release in releases {
        if release.selected_provider_id.is_some()
            && release.selected_provider_id != Some(record.provider_id)
        {
            continue;
        }
        let jobs = list_release_jobs(pool, release.release_id)
            .await
            .map_err(ApiError::from)?;
        let job = jobs
            .iter()
            .find(|job| {
                job.route_logical_id == HTTP_STREAM_DEFAULT_LOGICAL_ID
                    && (job.provider_id.is_none() || job.provider_id == Some(record.provider_id))
                    && job.active
            })
            .or_else(|| {
                jobs.iter()
                    .find(|job| job.route_logical_id == HTTP_STREAM_DEFAULT_LOGICAL_ID)
            });
        let state = job
            .map(|job| job.state.as_str().to_string())
            .unwrap_or_else(|| release.state.as_str().to_string());
        let candidate = release.selected_candidate.as_ref();
        let runtime = release
            .coverage_plan
            .as_ref()
            .and_then(|plan| plan.get("streamRuntime"));
        let materializer_state = runtime
            .and_then(|value| value.get("runtimeState"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| state.clone());
        let downloaded_bytes =
            runtime.and_then(|value| stream_runtime_u64(value, "downloadedBytes"));
        let total_bytes = runtime.and_then(|value| stream_runtime_u64(value, "totalBytes"));
        let warnings = release
            .coverage_plan
            .as_ref()
            .and_then(|plan| plan.get("validationWarnings"))
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        items.push(DownloadBrokerProgressItem {
            id: release
                .download_id
                .clone()
                .or_else(|| job.and_then(|job| job.download_id.clone()))
                .unwrap_or_else(|| release.release_id.to_string()),
            name: Some(release.release_title.clone()),
            state: Some(materializer_state.clone()),
            category: None,
            local_path: runtime.and_then(|value| stream_runtime_string(value, "localPath")),
            progress: runtime.and_then(|value| stream_runtime_f64(value, "progress")),
            downloaded_bytes,
            total_bytes,
            remaining_bytes: match (downloaded_bytes, total_bytes) {
                (Some(downloaded), Some(total)) => Some(total.saturating_sub(downloaded)),
                _ => None,
            },
            download_rate_bps: runtime
                .and_then(|value| stream_runtime_u64(value, "downloadRateBps")),
            upload_rate_bps: None,
            debrid: None,
            torrent: None,
            stream: Some(DownloadBrokerStreamEvidence {
                provider_name: Some("Elixir HTTP stream materializer".to_string()),
                provider_implementation: record.implementation.clone(),
                stream_type: candidate
                    .and_then(|value| stream_candidate_string(value, "/delivery/streamType")),
                source_kind: candidate
                    .and_then(|value| stream_candidate_string(value, "/sourceKind")),
                source_module: candidate
                    .and_then(|value| stream_candidate_string(value, "/sourceModule/name"))
                    .or_else(|| {
                        candidate
                            .and_then(|value| stream_candidate_string(value, "/sourceModule/id"))
                    }),
                target_key: candidate
                    .and_then(|value| stream_candidate_string(value, "/targetEvidence/targetKey")),
                materializer_state,
                warnings,
            }),
        });
    }
    Ok(items)
}

async fn cancel_qbittorrent(
    state: &AppState,
    store: &ExtensionStore<'_>,
    record: &DownloadBrokerProviderRecord,
    download_id: &str,
    delete_files: bool,
) -> ApiResult<bool> {
    let id = normalized_source(download_id)?;
    let fields = qbittorrent_delete_fields(id, delete_files);
    request_qbittorrent_service_form(
        state,
        store,
        record.instance_id,
        "api/v2/torrents/delete",
        &fields,
    )
    .await
    .map_err(ApiError::from)?;
    if let Some(release) = get_release_by_download_id(&state.db_pool, id)
        .await
        .map_err(ApiError::from)?
        && release.selected_provider_id == Some(record.provider_id)
        && release.selected_route_logical_id.as_deref() == Some(record.logical_id.as_str())
    {
        mark_qbittorrent_release_cancelled(&state.db_pool, &release, id, delete_files)
            .await
            .map_err(ApiError::from)?;
    }
    Ok(true)
}

fn qbittorrent_delete_fields(torrent_hash: &str, delete_files: bool) -> HashMap<String, String> {
    let mut fields = HashMap::new();
    fields.insert("hashes".to_string(), torrent_hash.to_string());
    fields.insert("deleteFiles".to_string(), delete_files.to_string());
    fields
}

async fn cancel_nzbget(
    state: &AppState,
    store: &ExtensionStore<'_>,
    record: &DownloadBrokerProviderRecord,
    download_id: &str,
) -> ApiResult<bool> {
    let payload = request_instance_service_json(
        state,
        store,
        record.instance_id,
        ReqwestMethod::POST,
        "jsonrpc",
        Some(json!({
            "version": "1.1",
            "method": "listgroups",
            "params": [0],
            "id": 1
        })),
    )
    .await
    .map_err(ApiError::from)?;
    ensure_nzbget_rpc_ok(&payload, "listgroups").map_err(ApiError::from)?;
    let groups = payload
        .get("result")
        .and_then(Value::as_array)
        .ok_or_else(|| ApiError::internal("nzbget listgroups response missing result array"))?;
    let Some(group_id) = resolve_nzbget_group_id(groups, download_id) else {
        return Ok(false);
    };

    let payload = request_instance_service_json(
        state,
        store,
        record.instance_id,
        ReqwestMethod::POST,
        "jsonrpc",
        Some(json!({
            "version": "1.1",
            "method": "editqueue",
            "params": ["GroupDelete", "", [group_id]],
            "id": 1
        })),
    )
    .await
    .map_err(ApiError::from)?;
    ensure_nzbget_rpc_ok(&payload, "editqueue").map_err(ApiError::from)?;
    let success = payload
        .get("result")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if !success {
        return Err(ApiError::internal(
            "nzbget editqueue GroupDelete did not report success",
        ));
    }
    Ok(true)
}

async fn cancel_debrid_broker(
    state: &AppState,
    store: &ExtensionStore<'_>,
    record: &DownloadBrokerProviderRecord,
    download_id: &str,
) -> ApiResult<bool> {
    if !is_debrid_service_implementation(record.implementation.as_deref()) {
        return Err(ApiError::conflict(
            "the selected debrid provider does not support native debrid cancellation",
        ));
    }
    cancel_debrid_job(
        state,
        store,
        record.provider_id,
        record.instance_id,
        download_id,
    )
    .await
    .map_err(ApiError::from)
}

async fn cancel_http_stream_broker(
    pool: &sqlx::AnyPool,
    record: &DownloadBrokerProviderRecord,
    download_id: &str,
    downloads_root: Option<&FsPath>,
) -> ApiResult<bool> {
    let id = normalized_source(download_id)?;
    let Some(release) = get_release_by_download_id(pool, id)
        .await
        .map_err(ApiError::from)?
    else {
        return Ok(false);
    };
    if release.selected_route_logical_id.as_deref() != Some(HTTP_STREAM_DEFAULT_LOGICAL_ID)
        || (release.selected_provider_id.is_some()
            && release.selected_provider_id != Some(record.provider_id))
    {
        return Ok(false);
    }
    let reason = "HTTP stream materializer job was cancelled.";
    let coverage_plan = merge_http_stream_runtime_evidence(
        release.coverage_plan.clone(),
        json!({
            "runtimeState": "cancelled",
            "downloadId": id,
            "message": reason
        }),
    );
    update_http_stream_release_state_only(
        pool,
        release.release_id,
        AcquisitionReleaseState::Cancelled,
        reason,
        coverage_plan,
    )
    .await
    .map_err(ApiError::from)?;
    if let Some(downloads_root) = downloads_root {
        cleanup_http_stream_partial_from_plan(downloads_root, release.coverage_plan.as_ref()).await;
    }
    let jobs = list_release_jobs(pool, release.release_id)
        .await
        .map_err(ApiError::from)?;
    for job in jobs
        .into_iter()
        .filter(|job| job.route_logical_id == HTTP_STREAM_DEFAULT_LOGICAL_ID)
    {
        update_release_job_state(
            pool,
            job.release_job_id,
            ReleaseJobStateUpdate {
                state: ReleaseJobState::Cancelled,
                state_reason: Some(reason.to_string()),
                active: Some(false),
                completed_at: Some(chrono::Utc::now()),
                ..Default::default()
            },
        )
        .await
        .map_err(ApiError::from)?;
    }
    Ok(true)
}

fn resolve_nzbget_group_id(groups: &[Value], download_id: &str) -> Option<i64> {
    if let Ok(group_id) = download_id.parse::<i64>() {
        if groups
            .iter()
            .any(|group| group.get("NZBID").and_then(Value::as_i64) == Some(group_id))
        {
            return Some(group_id);
        }
    }
    groups
        .iter()
        .find(|group| {
            string_field(group, "NZBName")
                .or_else(|| string_field(group, "NZBFilename"))
                .map(|value| value.eq_ignore_ascii_case(download_id))
                .unwrap_or(false)
        })
        .and_then(|group| group.get("NZBID").and_then(Value::as_i64))
}

fn ensure_nzbget_rpc_ok(payload: &Value, method: &str) -> anyhow::Result<()> {
    if let Some(error) = payload.get("error").filter(|value| !value.is_null()) {
        bail!("nzbget {method} returned error: {error}");
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct ParsedQbittorrentReleaseFileMetadata {
    title: Option<String>,
    season_number: Option<i32>,
    episode_number: Option<i32>,
    episode_end_number: Option<i32>,
    absolute_episode_number: Option<i32>,
    absolute_episode_end_number: Option<i32>,
    air_date: Option<String>,
    quality: Option<String>,
    language: Option<String>,
    release_group: Option<String>,
    confidence: ReleaseConfidence,
    reason: Option<String>,
}

fn parsed_qbittorrent_file_metadata(
    media_type: MediaType,
    path: &str,
) -> ParsedQbittorrentReleaseFileMetadata {
    match media_type {
        MediaType::Series => {
            let parsed = TvSonarrStyleResolver.parse_file(path);
            let has_air_date = parsed.air_date.is_some();
            ParsedQbittorrentReleaseFileMetadata {
                title: parsed.normalized_series_title,
                season_number: parsed.season_number,
                episode_number: parsed.episode_numbers.first().copied(),
                episode_end_number: parsed.episode_numbers.last().copied(),
                absolute_episode_number: parsed.anime_absolute_hints.first().copied(),
                absolute_episode_end_number: parsed.anime_absolute_hints.last().copied(),
                air_date: parsed.air_date,
                quality: parsed
                    .quality
                    .resolution
                    .map(|resolution| format!("{resolution:?}")),
                language: parsed.modifiers.languages.first().cloned(),
                release_group: parsed.release_group,
                confidence: if parsed.season_number.is_some()
                    && (!parsed.episode_numbers.is_empty() || has_air_date)
                {
                    ReleaseConfidence::High
                } else {
                    ReleaseConfidence::ReviewRequired
                },
                reason: None,
            }
        }
        MediaType::Anime => {
            let parsed = parse_anime_release_title(path);
            ParsedQbittorrentReleaseFileMetadata {
                title: parsed.series_title,
                season_number: parsed.season_number,
                episode_number: parsed.episode_start_number,
                episode_end_number: parsed.episode_end_number,
                absolute_episode_number: parsed.absolute_episode_numbers.first().copied(),
                absolute_episode_end_number: parsed.absolute_episode_numbers.last().copied(),
                air_date: None,
                quality: parsed.quality.resolution,
                language: parsed
                    .subtitle_languages
                    .first()
                    .cloned()
                    .or_else(|| parsed.audio_languages.first().cloned()),
                release_group: parsed.release_group,
                confidence: parsed.confidence,
                reason: (!parsed.review_reasons.is_empty())
                    .then(|| parsed.review_reasons.join(",")),
            }
        }
        MediaType::Movie => {
            let parsed = MovieRadarrStyleParser.parse_path(path);
            ParsedQbittorrentReleaseFileMetadata {
                title: parsed
                    .as_ref()
                    .and_then(|parsed| parsed.primary_movie_title().map(ToString::to_string)),
                season_number: None,
                episode_number: None,
                episode_end_number: None,
                absolute_episode_number: None,
                absolute_episode_end_number: None,
                air_date: None,
                quality: parsed
                    .as_ref()
                    .and_then(|parsed| parsed.quality.quality.clone()),
                language: parsed
                    .as_ref()
                    .and_then(|parsed| parsed.languages.first().cloned()),
                release_group: parsed
                    .as_ref()
                    .and_then(|parsed| parsed.release_group.clone()),
                confidence: parsed
                    .as_ref()
                    .map(|_| ReleaseConfidence::High)
                    .unwrap_or(ReleaseConfidence::ReviewRequired),
                reason: parsed
                    .is_none()
                    .then(|| "movie_file_path_unparseable".to_string()),
            }
        }
    }
}

fn torrent_source_kind(source: &str) -> &'static str {
    let lowered = source.trim().to_ascii_lowercase();
    if lowered.starts_with("magnet:") {
        "magnet"
    } else if lowered.starts_with("http://") || lowered.starts_with("https://") {
        "torrent_url"
    } else if lowered.starts_with("bc://bt/") {
        "bc_link"
    } else {
        "torrent"
    }
}

fn classify_release_file_path(path: &str) -> &'static str {
    let basename = basename_from_path(path).to_ascii_lowercase();
    let extension = FsPath::new(&basename)
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    if is_media_extension(extension) {
        if basename.contains("sample") {
            "sample"
        } else {
            "media"
        }
    } else if matches!(extension, "srt" | "ass" | "ssa" | "vtt" | "sub" | "idx") {
        "subtitle"
    } else if matches!(
        extension,
        "nfo" | "txt" | "jpg" | "jpeg" | "png" | "webp" | "json" | "xml" | "sfv" | "m3u"
    ) {
        "sidecar"
    } else {
        "unknown"
    }
}

fn is_media_extension(extension: &str) -> bool {
    matches!(
        extension,
        "mkv"
            | "mp4"
            | "m4v"
            | "avi"
            | "mov"
            | "wmv"
            | "ts"
            | "m2ts"
            | "webm"
            | "flv"
            | "mpg"
            | "mpeg"
    )
}

fn basename_from_path(path: &str) -> String {
    path.rsplit(['/', '\\'])
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(path.trim())
        .to_string()
}

fn u64_to_i64(value: u64) -> Option<i64> {
    i64::try_from(value).ok()
}

fn normalized_source(value: &str) -> ApiResult<&str> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ApiError::bad_request("source is required"));
    }
    Ok(trimmed)
}

fn validate_torrent_source(source: &str) -> ApiResult<()> {
    let lowered = source.to_ascii_lowercase();
    if lowered.starts_with("magnet:")
        || lowered.starts_with("http://")
        || lowered.starts_with("https://")
        || lowered.starts_with("bc://bt/")
    {
        return Ok(());
    }
    Err(ApiError::bad_request(
        "torrent source must be a magnet, http, https, or bc link",
    ))
}

fn validate_nzb_source(source: &str) -> ApiResult<()> {
    let lowered = source.to_ascii_lowercase();
    if lowered.starts_with("http://") || lowered.starts_with("https://") {
        return Ok(());
    }
    Err(ApiError::bad_request(
        "usenet source must be an http or https NZB URL",
    ))
}

fn validate_nzb_submit_source(
    source: &str,
    request: &DownloadBrokerSubmitRequest,
) -> ApiResult<()> {
    validate_nzb_source(source)?;
    let Some(candidate) = request.selected_candidate.as_ref() else {
        return Ok(());
    };
    if candidate_declares_usenet_route(candidate) {
        return Ok(());
    }
    let source_kind = candidate.source_kind.trim().to_ascii_lowercase();
    if matches!(source_kind.as_str(), "nzb" | "usenet") {
        return Ok(());
    }
    if matches!(source_kind.as_str(), "http" | "url") && source_looks_like_nzb(source) {
        return Ok(());
    }
    Err(ApiError::bad_request(
        "selected candidate is not an NZB/usenet source",
    ))
}

fn candidate_declares_usenet_route(candidate: &AcquisitionCandidate) -> bool {
    candidate
        .supported_routes
        .iter()
        .any(|route| route.eq_ignore_ascii_case(USENET_DEFAULT_LOGICAL_ID))
}

fn source_looks_like_nzb(source: &str) -> bool {
    let trimmed = source.trim();
    if trimmed.is_empty() {
        return false;
    }
    let without_fragment = trimmed.split('#').next().unwrap_or(trimmed);
    let without_query = without_fragment
        .split('?')
        .next()
        .unwrap_or(without_fragment);
    without_query.to_ascii_lowercase().ends_with(".nzb")
}

fn validate_debrid_source(source: &str) -> ApiResult<()> {
    debrid_source_kind(source)
        .map(|_| ())
        .map_err(|err| ApiError::bad_request(err.to_string()))
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

#[derive(Debug)]
struct HttpStreamFingerprintInput<'a> {
    source_extension_id: &'a str,
    source_provider_id: Option<Uuid>,
    source: &'a str,
    source_kind: &'a str,
    release_title: &'a str,
    candidate: &'a Value,
}

fn build_http_stream_release_fingerprint(input: &HttpStreamFingerprintInput<'_>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"http-stream-v1\n");
    hasher.update(input.source_extension_id.trim().as_bytes());
    hasher.update(b"\n");
    if let Some(provider_id) = input.source_provider_id {
        hasher.update(provider_id.to_string().as_bytes());
    }
    hasher.update(b"\n");
    hasher.update(input.source_kind.trim().to_ascii_lowercase().as_bytes());
    hasher.update(b"\n");
    hasher.update(input.source.trim().as_bytes());
    hasher.update(b"\n");
    hasher.update(input.release_title.trim().as_bytes());
    hasher.update(b"\n");
    for pointer in [
        "/id",
        "/targetEvidence/mediaType",
        "/targetEvidence/targetKey",
        "/delivery/streamType",
        "/sourceModule/id",
    ] {
        if let Some(value) = stream_candidate_string(input.candidate, pointer) {
            hasher.update(value.as_bytes());
        }
        hasher.update(b"\n");
    }
    format!("sha256:{:x}", hasher.finalize())
}

fn http_stream_download_id(fingerprint: &str) -> String {
    let suffix = fingerprint
        .strip_prefix("sha256:")
        .unwrap_or(fingerprint)
        .chars()
        .filter(|ch| ch.is_ascii_hexdigit())
        .take(32)
        .collect::<String>();
    if suffix.len() >= 16 {
        format!("http-stream:{suffix}")
    } else {
        let mut hasher = Sha256::new();
        hasher.update(fingerprint.trim().as_bytes());
        let hash = format!("{:x}", hasher.finalize());
        format!("http-stream:{}", &hash[..32])
    }
}

fn parse_stream_media_type(value: &str) -> Option<MediaType> {
    match value.trim().to_ascii_lowercase().as_str() {
        "movie" | "movies" => Some(MediaType::Movie),
        "series" | "tv" | "show" | "shows" => Some(MediaType::Series),
        "anime" => Some(MediaType::Anime),
        _ => None,
    }
}

fn stream_release_confidence(value: &str) -> ReleaseConfidence {
    match value.trim().to_ascii_lowercase().as_str() {
        "high" => ReleaseConfidence::High,
        "medium" => ReleaseConfidence::Medium,
        "review_required" | "review-required" => ReleaseConfidence::ReviewRequired,
        _ => ReleaseConfidence::Low,
    }
}

fn stream_candidate_string(candidate: &Value, pointer: &str) -> Option<String> {
    candidate
        .pointer(pointer)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn stream_candidate_f64(candidate: &Value, pointer: &str) -> Option<f64> {
    candidate.pointer(pointer).and_then(Value::as_f64)
}

fn stream_runtime_string(runtime: &Value, key: &str) -> Option<String> {
    runtime
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn stream_runtime_u64(runtime: &Value, key: &str) -> Option<u64> {
    runtime.get(key).and_then(|value| {
        value
            .as_u64()
            .or_else(|| value.as_i64().and_then(|value| value.try_into().ok()))
            .or_else(|| {
                value
                    .as_str()
                    .and_then(|value| value.trim().parse::<u64>().ok())
            })
    })
}

fn stream_runtime_f64(runtime: &Value, key: &str) -> Option<f64> {
    runtime.get(key).and_then(|value| {
        value
            .as_f64()
            .or_else(|| value.as_i64().map(|value| value as f64))
            .or_else(|| value.as_u64().map(|value| value as f64))
            .or_else(|| {
                value
                    .as_str()
                    .and_then(|value| value.trim().parse::<f64>().ok())
            })
    })
}

fn merge_http_stream_runtime_evidence(
    coverage_plan: Option<Value>,
    runtime_evidence: Value,
) -> Value {
    match coverage_plan {
        Some(Value::Object(mut object)) => {
            object.insert("streamRuntime".to_string(), runtime_evidence);
            Value::Object(object)
        }
        Some(value) => json!({
            "previousCoveragePlan": value,
            "streamRuntime": runtime_evidence
        }),
        None => json!({ "streamRuntime": runtime_evidence }),
    }
}

fn string_field(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn number_field(value: &Value, key: &str) -> Option<u64> {
    value.get(key).and_then(Value::as_u64)
}

fn numeric_u64_field(value: &Value, key: &str) -> Option<u64> {
    value.get(key).and_then(|value| {
        value
            .as_u64()
            .or_else(|| value.as_i64().and_then(|v| v.try_into().ok()))
    })
}

fn numeric_i64_field(value: &Value, key: &str) -> Option<i64> {
    value.get(key).and_then(|value| {
        value
            .as_i64()
            .or_else(|| value.as_u64().and_then(|v| v.try_into().ok()))
    })
}

fn numeric_f64_field(value: &Value, key: &str) -> Option<f64> {
    value.get(key).and_then(|value| {
        value
            .as_f64()
            .or_else(|| value.as_i64().map(|v| v as f64))
            .or_else(|| value.as_u64().map(|v| v as f64))
    })
}

fn combine_hi_lo(hi: Option<u64>, lo: Option<u64>) -> Option<u64> {
    match (hi, lo) {
        (Some(hi), Some(lo)) => Some((hi << 32) | lo),
        (Some(hi), None) => Some(hi << 32),
        (None, Some(lo)) => Some(lo),
        (None, None) => None,
    }
}

fn progress_fraction(downloaded: Option<u64>, total: Option<u64>) -> Option<f64> {
    let total = total?;
    if total == 0 {
        return None;
    }
    Some((downloaded.unwrap_or(0) as f64 / total as f64).clamp(0.0, 1.0))
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use async_trait::async_trait;

    use super::*;
    use crate::{
        acquisition::{
            release_resolution::store::{
                get_release, list_release_coverage, list_release_files, list_release_jobs,
            },
            subscriptions::{
                AcquisitionMonitorPolicy, AcquisitionRoutePolicy, AcquisitionSubscription,
                NewAcquisitionSubscription, NewAcquisitionTarget, create_subscription,
                upsert_subscription_targets,
            },
        },
        anime_matching::{
            ANIME_SEMANTIC_EVIDENCE_SCHEMA_VERSION, AnimeSemanticEvidenceEngine,
            AnimeSemanticEvidenceRequest, AnimeSemanticEvidenceResponse,
        },
        artwork::ArtworkService,
        auth::AuthService,
        config::{DatabaseConfig, Settings},
        db::{
            Database,
            models::{ExtensionKind, ExtensionTrustLevel, ProviderHealthState, SlotCardinality},
        },
        download_broker::{
            DownloadBrokerEndpointContract, DownloadBrokerProviderKind,
            HTTP_STREAM_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID,
            resolve_logical_downloader_for_owner,
        },
        extensions::{
            ExtensionManager,
            store::{NewExtension, NewExtensionInstance, NewProvider},
        },
        library::LinkerService,
        metadata::MetadataService,
        secrets::SecretsManager,
    };

    const TEST_HASH: &str = "0123456789abcdef0123456789abcdef01234567";
    const TEST_MAGNET: &str =
        "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&dn=Show.S01";

    #[derive(Debug, Clone, Copy)]
    enum QbittorrentAnimeEngineBehavior {
        MatchWantedFiles,
        Empty,
        Error,
        Timeout,
        InvalidFileReference,
    }

    #[derive(Debug)]
    struct QbittorrentAnimeMatchEngine {
        behavior: QbittorrentAnimeEngineBehavior,
        calls: AtomicUsize,
    }

    impl QbittorrentAnimeMatchEngine {
        fn new(behavior: QbittorrentAnimeEngineBehavior) -> Arc<Self> {
            Arc::new(Self {
                behavior,
                calls: AtomicUsize::new(0),
            })
        }

        fn service(self: &Arc<Self>) -> AnimeMatchingService {
            AnimeMatchingService::with_semantic_engine(self.clone())
        }

        fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl AnimeSemanticEvidenceEngine for QbittorrentAnimeMatchEngine {
        async fn select_hypothesis(
            &self,
            request: AnimeSemanticEvidenceRequest,
        ) -> anyhow::Result<AnimeSemanticEvidenceResponse> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self.behavior {
                QbittorrentAnimeEngineBehavior::Error => {
                    bail!("simulated qBittorrent anime matcher failure")
                }
                QbittorrentAnimeEngineBehavior::Timeout => {
                    bail!("local model request deadline exceeded")
                }
                QbittorrentAnimeEngineBehavior::Empty => Ok(AnimeSemanticEvidenceResponse {
                    schema_version: ANIME_SEMANTIC_EVIDENCE_SCHEMA_VERSION,
                    hypothesis_index: None,
                }),
                QbittorrentAnimeEngineBehavior::InvalidFileReference => {
                    Ok(AnimeSemanticEvidenceResponse {
                        schema_version: ANIME_SEMANTIC_EVIDENCE_SCHEMA_VERSION,
                        hypothesis_index: Some(usize::MAX),
                    })
                }
                QbittorrentAnimeEngineBehavior::MatchWantedFiles => {
                    let hypothesis_index = request
                        .hypotheses
                        .iter()
                        .max_by_key(|hypothesis| hypothesis.target_keys.len())
                        .map(|hypothesis| hypothesis.index)
                        .context("semantic matcher fixture expected a hypothesis")?;
                    Ok(AnimeSemanticEvidenceResponse {
                        schema_version: ANIME_SEMANTIC_EVIDENCE_SCHEMA_VERSION,
                        hypothesis_index: Some(hypothesis_index),
                    })
                }
            }
        }
    }

    async fn setup_db() -> anyhow::Result<Database> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        Ok(database)
    }

    fn setup_state_for_database(database: &Database) -> anyhow::Result<AppState> {
        let settings = Settings::default();
        let auth_service = AuthService::new(settings.auth.clone())?;
        let metadata = MetadataService::new(settings.metadata.clone())?;
        let linkers = LinkerService::new(settings.classifier.clone())?;
        let artwork = ArtworkService::new(
            settings.library.artwork_cache_dir.clone(),
            settings.metadata.request_timeout_seconds,
        )?;
        Ok(AppState::new(
            settings,
            database.clone(),
            auth_service,
            ExtensionManager::new(),
            metadata,
            linkers,
            artwork,
            SecretsManager::from_key_bytes([0u8; 32], true),
        ))
    }

    async fn start_qbittorrent_priority_fixture(
        file_rows: Vec<Value>,
    ) -> anyhow::Result<(QbittorrentTestServiceFixture, Arc<Mutex<Vec<Value>>>)> {
        start_qbittorrent_priority_fixture_with_failures(file_rows, false, false).await
    }

    async fn start_qbittorrent_priority_fixture_with_failures(
        file_rows: Vec<Value>,
        fail_torrent_info: bool,
        fail_file_list: bool,
    ) -> anyhow::Result<(QbittorrentTestServiceFixture, Arc<Mutex<Vec<Value>>>)> {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let fixture = QbittorrentTestServiceFixture {
            requests: requests.clone(),
            file_rows: Value::Array(file_rows),
            torrent_info_rows: json!([{
                "hash": TEST_HASH,
                "state": "pausedDL",
                "progress": 0.0,
                "dlspeed": 0,
            }]),
            fail_torrent_info,
            fail_file_list,
            fail_form_paths_once: Arc::new(Mutex::new(HashSet::new())),
        };
        Ok((fixture, requests))
    }

    async fn configure_qbittorrent_priority_fixture_endpoint(
        database: &Database,
        resolved: &ResolvedDownloadBrokerProvider,
        fixture: QbittorrentTestServiceFixture,
    ) -> anyhow::Result<()> {
        QBITTORRENT_TEST_SERVICE_FIXTURES
            .lock()
            .expect("qBittorrent test service fixture lock")
            .insert(resolved.record.instance_id, fixture);
        ExtensionStore::new(&database.pool)
            .upsert_provider(&NewProvider {
                provider_id: resolved.record.provider_id,
                instance_id: resolved.record.instance_id,
                capability: resolved.record.capability.clone(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: Some("qbittorrent".to_string()),
                scope_json: Some(json!({
                    "downloadBroker": {
                        "enabled": true,
                        "logicalId": TORRENT_DEFAULT_LOGICAL_ID,
                        "providerKind": "managed"
                    }
                })),
                endpoint_json: None,
                health_state: ProviderHealthState::Healthy,
            })
            .await?;
        Ok(())
    }

    async fn setup_qbittorrent_resolved(
        database: &Database,
    ) -> anyhow::Result<ResolvedDownloadBrokerProvider> {
        let store = ExtensionStore::new(&database.pool);
        let instance_id = Uuid::new_v4();
        let provider_id = Uuid::new_v4();
        store
            .upsert_extension(&NewExtension {
                extension_id: "elixir.modules.qbittorrent".to_string(),
                name: "qBittorrent".to_string(),
                version: "1.0.0".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: None,
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Verified,
                manifest_json: json!({ "id": "elixir.modules.qbittorrent" }),
                package_hash: None,
                enabled: true,
            })
            .await?;
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "elixir.modules.qbittorrent".to_string(),
                instance_name: "qBittorrent".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;
        store
            .upsert_provider(&NewProvider {
                provider_id,
                instance_id,
                capability: "downloader.torrent".to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: Some("qbittorrent".to_string()),
                scope_json: Some(json!({
                    "downloadBroker": {
                        "enabled": true,
                        "logicalId": TORRENT_DEFAULT_LOGICAL_ID,
                        "providerKind": "managed"
                    }
                })),
                endpoint_json: None,
                health_state: ProviderHealthState::Healthy,
            })
            .await?;

        Ok(ResolvedDownloadBrokerProvider {
            record: DownloadBrokerProviderRecord {
                logical_id: TORRENT_DEFAULT_LOGICAL_ID.to_string(),
                broker_path: "/api/download-broker/downloaders.torrent.default".to_string(),
                endpoints: DownloadBrokerEndpointContract {
                    base_path: "/api/download-broker/downloaders.torrent.default".to_string(),
                    submit_path: "/api/download-broker/downloaders.torrent.default/submit"
                        .to_string(),
                    progress_path: "/api/download-broker/downloaders.torrent.default/progress"
                        .to_string(),
                    cancel_path_template:
                        "/api/download-broker/downloaders.torrent.default/{downloadId}".to_string(),
                },
                role: DownloadBrokerRole::Torrent,
                provider_kind: DownloadBrokerProviderKind::Managed,
                provider_id,
                instance_id,
                extension_id: "elixir.modules.qbittorrent".to_string(),
                capability: "downloader.torrent".to_string(),
                implementation: Some("qbittorrent".to_string()),
                health_state: ProviderHealthState::Healthy,
                selected_for_default: true,
            },
            binding_kind: DownloadBrokerBindingKind::ManagedDirect,
            category: Some("series".to_string()),
        })
    }

    async fn setup_series_subscription(
        database: &Database,
        episodes: std::ops::RangeInclusive<i32>,
    ) -> anyhow::Result<(AcquisitionSubscription, Vec<AcquisitionTarget>)> {
        setup_subscription_with_targets(
            database,
            MediaType::Series,
            "Show",
            episodes
                .map(|episode| NewAcquisitionTarget {
                    season_number: Some(1),
                    episode_number: Some(episode),
                    ..empty_target()
                })
                .collect(),
        )
        .await
    }

    async fn setup_movie_subscription(
        database: &Database,
        title: &str,
    ) -> anyhow::Result<(AcquisitionSubscription, Vec<AcquisitionTarget>)> {
        setup_subscription_with_targets(
            database,
            MediaType::Movie,
            title,
            vec![NewAcquisitionTarget {
                target_key: Some("movie".to_string()),
                media_type: Some(MediaType::Movie),
                title: Some(title.to_string()),
                ..empty_target()
            }],
        )
        .await
    }

    async fn setup_subscription_with_targets(
        database: &Database,
        media_type: MediaType,
        title: &str,
        targets: Vec<NewAcquisitionTarget>,
    ) -> anyhow::Result<(AcquisitionSubscription, Vec<AcquisitionTarget>)> {
        let subscription = create_subscription(
            &database.pool,
            NewAcquisitionSubscription {
                media_type,
                title: title.to_string(),
                year: Some(2026),
                external_ids: None,
                idempotency_key: None,
                request_mode: None,
                request_scope: None,
                scope: None,
                metadata_policy: None,
                completion_policy: None,
                monitor_policy: AcquisitionMonitorPolicy::AllMissing,
                route_policy: AcquisitionRoutePolicy::DebridFirst,
                source_provider_id: None,
                release_delay_seconds: None,
                quality_profile: None,
                metadata_refresh_after: None,
                candidate_search_after: None,
            },
        )
        .await?;
        let targets =
            upsert_subscription_targets(&database.pool, subscription.subscription_id, targets)
                .await?;
        Ok((subscription, targets))
    }

    async fn setup_staged_qbittorrent_release(
        database: &Database,
        resolved: &ResolvedDownloadBrokerProvider,
        subscription_id: Uuid,
        release_title: &str,
    ) -> anyhow::Result<AcquisitionRelease> {
        setup_staged_qbittorrent_release_for(
            database,
            resolved,
            subscription_id,
            MediaType::Series,
            "Show",
            "series",
            release_title,
        )
        .await
    }

    async fn setup_staged_qbittorrent_release_for(
        database: &Database,
        resolved: &ResolvedDownloadBrokerProvider,
        subscription_id: Uuid,
        media_type: MediaType,
        media_title: &str,
        category: &str,
        release_title: &str,
    ) -> anyhow::Result<AcquisitionRelease> {
        let mut request = sample_request(true);
        request.subscription_id = Some(subscription_id);
        request.category = Some(category.to_string());
        request.media_type = Some(media_type);
        request.media_title = Some(media_title.to_string());
        request.name = Some(release_title.to_string());
        if let Some(candidate) = request.selected_candidate.as_mut() {
            candidate.title = release_title.to_string();
            candidate.quality = Some("1080p".to_string());
        }
        let context = qbittorrent_release_context(&request, resolved, TEST_MAGNET, None)
            .expect("release context");
        let release = upsert_qbittorrent_acquisition_release(
            &database.pool,
            resolved,
            &context,
            Some(category),
            Some(TEST_HASH),
        )
        .await?;
        upsert_qbittorrent_release_job(&database.pool, resolved, &release, Some(TEST_HASH)).await?;
        Ok(release)
    }

    async fn setup_ready_anime_qbittorrent_provider_attempt(
        database: &Database,
        resolved: &ResolvedDownloadBrokerProvider,
        started_at: chrono::DateTime<chrono::Utc>,
    ) -> anyhow::Result<(AcquisitionRelease, AcquisitionTarget)> {
        let (subscription, targets) = setup_subscription_with_targets(
            database,
            MediaType::Anime,
            "Provider Retry Anime",
            vec![NewAcquisitionTarget {
                target_key: Some("anilist:9101:A0001".to_string()),
                media_type: Some(MediaType::Anime),
                title: Some("Provider Retry Anime".to_string()),
                season_number: Some(1),
                episode_number: Some(1),
                absolute_episode_number: Some(1),
                ..empty_target()
            }],
        )
        .await?;
        let release = setup_staged_qbittorrent_release_for(
            database,
            resolved,
            subscription.subscription_id,
            MediaType::Anime,
            "Provider Retry Anime",
            "anime",
            "[Group] Provider Retry Anime - 01 [1080p]",
        )
        .await?;
        upsert_release_coverage(
            &database.pool,
            NewAcquisitionReleaseCoverage {
                coverage_id: None,
                release_id: release.release_id,
                release_file_id: None,
                target_id: targets[0].target_id,
                coverage_kind: ReleaseCoverageKind::SingleEpisode,
                confidence: ReleaseConfidence::High,
                score: Some(1.0),
                reason: Some("selected anime target".to_string()),
                state: ReleaseCoverageState::Submitted,
                verified_by: Some("alm7_provider_operation_retry_test".to_string()),
            },
        )
        .await?;
        update_target_state(
            &database.pool,
            targets[0].target_id,
            AcquisitionTargetStateUpdate {
                state: AcquisitionTargetState::Submitted,
                state_reason: Some("submitted to qBittorrent".to_string()),
                selected_provider_id: release.source_provider_id,
                selected_route_logical_id: Some(TORRENT_DEFAULT_LOGICAL_ID.to_string()),
                selected_candidate: release.selected_candidate.clone(),
                download_id: Some(TEST_HASH.to_string()),
                ..Default::default()
            },
        )
        .await?;

        let mut coverage_plan = release.coverage_plan.clone().unwrap_or_else(|| json!({}));
        let complete = json!({
            "status": "complete",
            "policyVersion": "acquisition-broker-bookkeeping-v1",
        });
        if let Some(object) = coverage_plan.as_object_mut() {
            object.insert("submissionBookkeeping".to_string(), complete);
        }
        sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_releases
             SET state = 'ready',
                 state_reason = 'file policy ready for provider application',
                 coverage_plan_json = $1,
                 updated_at = CURRENT_TIMESTAMP
             WHERE release_id = $2",
        )
        .bind(serde_json::to_string(&coverage_plan)?)
        .bind(release.release_id.to_string())
        .execute(&database.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_release_jobs
             SET state = 'ready',
                 state_reason = 'file policy ready for provider application',
                 active = 1,
                 started_at = $1,
                 completed_at = NULL,
                 updated_at = CURRENT_TIMESTAMP
             WHERE release_id = $2
               AND LOWER(download_id) = LOWER($3)",
        )
        .bind(started_at.format("%Y-%m-%d %H:%M:%S").to_string())
        .bind(release.release_id.to_string())
        .bind(TEST_HASH)
        .execute(&database.pool)
        .await?;
        let ready = get_release(&database.pool, release.release_id)
            .await?
            .context("ready anime qBittorrent provider attempt")?;
        Ok((ready, targets[0].clone()))
    }

    fn sample_candidate() -> AcquisitionCandidate {
        AcquisitionCandidate {
            id: Some("candidate-1".to_string()),
            title: "Show.S01.COMPLETE.1080p.WEB-DL-GROUP".to_string(),
            source: TEST_MAGNET.to_string(),
            source_kind: "magnet".to_string(),
            info_hash: Some(TEST_HASH.to_string()),
            file_index: None,
            quality: Some("1080p".to_string()),
            size_bytes: Some(10 * 1024 * 1024 * 1024),
            seeders: Some(50),
            language: Some("en".to_string()),
            cached_debrid: None,
            rank: Some(1),
            score: Some(99.0),
            score_badges: Vec::new(),
            files: Vec::new(),
            supported_routes: vec![TORRENT_DEFAULT_LOGICAL_ID.to_string()],
            default_route: Some(TORRENT_DEFAULT_LOGICAL_ID.to_string()),
            raw: Some(json!({ "source": "test" })),
        }
    }

    fn sample_request(acquisition_owned: bool) -> DownloadBrokerSubmitRequest {
        DownloadBrokerSubmitRequest {
            source: TEST_MAGNET.to_string(),
            category: Some("series".to_string()),
            paused: Some(false),
            name: Some("Show S01".to_string()),
            priority: None,
            add_to_top: None,
            subscription_id: None,
            source_provider_id: None,
            source_extension_id: acquisition_owned
                .then(|| "elixir.marketplace.torrentio".to_string()),
            media_type: acquisition_owned.then_some(MediaType::Series),
            media_title: acquisition_owned.then(|| "Show".to_string()),
            selected_candidate: acquisition_owned.then(sample_candidate),
            request_scope_evidence: None,
            selected_stream_candidate: None,
            release_fingerprint: None,
            defer_anime_debrid_refinement: false,
        }
    }

    fn sample_stream_candidate() -> Value {
        json!({
            "candidateKind": "stream",
            "id": "stream-fixture-1",
            "title": "Fullmetal Alchemist Brotherhood S01E01",
            "source": "https://stream.example.test/fmab/s01e01/master.m3u8",
            "sourceKind": "http_stream",
            "score": 91.5,
            "supportedRoutes": [HTTP_STREAM_DEFAULT_LOGICAL_ID],
            "defaultRoute": HTTP_STREAM_DEFAULT_LOGICAL_ID,
            "targetEvidence": {
                "mediaType": "anime",
                "targetKey": "anilist:5114:A0001",
                "confidence": "high",
                "reasons": ["provider episode id matched target"]
            },
            "delivery": {
                "streamType": "hls",
                "url": "https://cdn.example.test/fmab/s01e01/master.m3u8",
                "referer": "https://stream.example.test/title/fmab",
                "headers": {
                    "user-agent": "ElixirTest/1.0"
                }
            },
            "sourceModule": {
                "id": "fixture.cloudstream",
                "name": "Fixture CloudStream",
                "type": "cloudstream"
            },
            "raw": {
                "hosterUrl": "https://stream.example.test/watch?api_key=secret"
            }
        })
    }

    fn empty_target() -> NewAcquisitionTarget {
        NewAcquisitionTarget {
            target_key: None,
            media_type: None,
            title: None,
            season_number: None,
            episode_number: None,
            absolute_episode_number: None,
            air_date: None,
            air_time: None,
            metadata: None,
            state: None,
            next_search_after: None,
        }
    }

    fn tokyo_ghoul_root_a_target_metadata(episode_number: i32) -> Value {
        json!({
            "graphFingerprint": "tokyo-ghoul-graph-v1",
            "targetCanonicalKey": format!("anilist:1002:episode:{episode_number}"),
            "anilistSeasonId": "1002",
            "aliases": ["Tokyo Ghoul"],
            "scopedAliases": [{
                "display": "Tokyo Ghoul Root A",
                "source": "anilist_english",
                "language": "en",
                "seasonNumber": 2,
                "anilistSeasonId": "1002"
            }, {
                "display": "東京喰種トーキョーグール√A",
                "source": "anilist_native",
                "language": "ja",
                "seasonNumber": 2,
                "anilistSeasonId": "1002"
            }]
        })
    }

    async fn alm7_qbittorrent_retry_persistence_snapshot(
        database: &Database,
        release_id: Uuid,
        targets: &[AcquisitionTarget],
    ) -> anyhow::Result<Value> {
        let release = get_release(&database.pool, release_id)
            .await?
            .context("ALM-7 retry release was not persisted")?;
        let mut jobs = list_release_jobs(&database.pool, release_id)
            .await?
            .into_iter()
            .map(|job| (job.state.as_str().to_string(), job.active))
            .collect::<Vec<_>>();
        jobs.sort();
        let mut coverage = list_release_coverage(&database.pool, release_id)
            .await?
            .into_iter()
            .map(|entry| {
                (
                    entry.target_id.to_string(),
                    entry.state.as_str().to_string(),
                    entry.verified_by,
                )
            })
            .collect::<Vec<_>>();
        coverage.sort();
        let mut files = list_release_files(&database.pool, release_id)
            .await?
            .into_iter()
            .map(|file| (file.provider_file_id, file.selected))
            .collect::<Vec<_>>();
        files.sort();
        let mut target_states = Vec::new();
        for target in targets {
            let persisted =
                crate::acquisition::subscriptions::get_target(&database.pool, target.target_id)
                    .await?
                    .context("ALM-7 retry target was not persisted")?;
            target_states.push((
                persisted.target_id.to_string(),
                persisted.state.as_str().to_string(),
                persisted.next_search_after.is_some(),
                persisted.selected_candidate.is_some(),
                persisted.download_id.is_some(),
            ));
        }
        target_states.sort();
        let review_release_count = sqlx::query_scalar::<sqlx::Any, i64>(
            "SELECT COUNT(*) FROM acquisition_releases
             WHERE subscription_id = $1 AND state = 'review_required'",
        )
        .bind(
            release
                .subscription_id
                .context("ALM-7 retry release is missing its subscription")?
                .to_string(),
        )
        .fetch_one(&database.pool)
        .await?;

        Ok(json!({
            "release": {
                "state": release.state.as_str(),
                "confidence": release.confidence.as_str(),
                "selectedCandidate": release.selected_candidate.is_some(),
                "downloadId": release.download_id,
            },
            "jobs": jobs,
            "coverage": coverage,
            "files": files,
            "targets": target_states,
            "reviewReleaseCount": review_release_count,
        }))
    }

    #[test]
    fn validates_broker_submit_sources() {
        assert!(validate_torrent_source("magnet:?xt=urn:btih:abc").is_ok());
        assert!(validate_torrent_source("https://example.test/file.torrent").is_ok());
        assert!(validate_torrent_source("ftp://example.test/file.torrent").is_err());

        assert!(validate_nzb_source("https://example.test/file.nzb").is_ok());
        assert!(validate_nzb_source("magnet:?xt=urn:btih:abc").is_err());

        let mut nzb_request = sample_request(true);
        let mut nzb_candidate = sample_candidate();
        nzb_candidate.source = "https://indexer.example/api?t=get&id=123".to_string();
        nzb_candidate.source_kind = "nzb".to_string();
        nzb_candidate.supported_routes = vec![USENET_DEFAULT_LOGICAL_ID.to_string()];
        nzb_request.source = nzb_candidate.source.clone();
        nzb_request.selected_candidate = Some(nzb_candidate);
        assert!(validate_nzb_submit_source(&nzb_request.source, &nzb_request).is_ok());

        let mut hoster_request = sample_request(true);
        let mut hoster_candidate = sample_candidate();
        hoster_candidate.source = "https://hoster.example/video.mkv".to_string();
        hoster_candidate.source_kind = "http".to_string();
        hoster_candidate.supported_routes = Vec::new();
        hoster_request.source = hoster_candidate.source.clone();
        hoster_request.selected_candidate = Some(hoster_candidate);
        assert!(validate_nzb_submit_source(&hoster_request.source, &hoster_request).is_err());
    }

    #[tokio::test]
    async fn ess5_http_stream_submit_progress_and_cancel_are_ledger_backed() -> anyhow::Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        let resolved = resolve_logical_downloader_for_owner(
            &database.pool,
            &store,
            HTTP_STREAM_DEFAULT_LOGICAL_ID,
            crate::download_broker::DEFAULT_ROUTE_OWNER_ID,
        )
        .await?;
        let candidate = sample_stream_candidate();
        let mut request = sample_request(false);
        request.source = candidate
            .get("source")
            .and_then(Value::as_str)
            .expect("candidate source")
            .to_string();
        request.name = Some("Fullmetal Alchemist Brotherhood S01E01".to_string());
        request.source_extension_id = Some("elixir.extension_suite.stream".to_string());
        request.media_type = Some(MediaType::Anime);
        request.media_title = Some("Fullmetal Alchemist Brotherhood".to_string());
        request.selected_stream_candidate = Some(candidate);

        let download_id =
            submit_http_stream_broker(&database.pool, &resolved, &request.source, &request, None)
                .await
                .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert!(download_id.starts_with("http-stream:"));

        let release = get_release_by_download_id(&database.pool, &download_id)
            .await?
            .expect("submitted stream release");
        assert_eq!(release.state, AcquisitionReleaseState::Submitted);
        assert_eq!(
            release.selected_route_logical_id.as_deref(),
            Some(HTTP_STREAM_DEFAULT_LOGICAL_ID)
        );
        assert_eq!(release.selected_provider_id, None);
        let provider_id = resolved.record.provider_id.to_string();
        assert_eq!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|value| value.get("providerId"))
                .and_then(Value::as_str),
            Some(provider_id.as_str())
        );
        assert_eq!(
            release.resolver_version,
            HTTP_STREAM_BROKER_RESOLVER_VERSION
        );
        assert_eq!(release.confidence, ReleaseConfidence::High);
        assert_eq!(release.score, Some(91.5));
        assert_eq!(
            release
                .selected_candidate
                .as_ref()
                .and_then(|value| value.pointer("/raw/hosterUrl"))
                .and_then(Value::as_str),
            Some("https://stream.example.test/watch?api_key=%5BREDACTED%5D")
        );
        let jobs = list_release_jobs(&database.pool, release.release_id).await?;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].state, ReleaseJobState::Submitted);
        assert!(jobs[0].active);

        let items = load_http_stream_broker_progress(&database.pool, &resolved.record)
            .await
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].id, download_id);
        assert_eq!(items[0].state.as_deref(), Some("submitted"));
        let stream = items[0].stream.as_ref().expect("stream evidence");
        assert_eq!(stream.stream_type.as_deref(), Some("hls"));
        assert_eq!(stream.source_kind.as_deref(), Some("http_stream"));
        assert_eq!(stream.target_key.as_deref(), Some("anilist:5114:A0001"));
        assert_eq!(stream.materializer_state, "submitted");

        let removed =
            cancel_http_stream_broker(&database.pool, &resolved.record, &download_id, None)
                .await
                .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert!(removed);
        let cancelled = get_release(&database.pool, release.release_id)
            .await?
            .expect("cancelled stream release");
        assert_eq!(cancelled.state, AcquisitionReleaseState::Cancelled);
        assert_eq!(
            cancelled
                .coverage_plan
                .as_ref()
                .and_then(|value| value.get("streamRuntime"))
                .and_then(|value| value.get("runtimeState"))
                .and_then(Value::as_str),
            Some("cancelled")
        );
        let jobs = list_release_jobs(&database.pool, release.release_id).await?;
        assert_eq!(jobs[0].state, ReleaseJobState::Cancelled);
        assert!(!jobs[0].active);
        let items = load_http_stream_broker_progress(&database.pool, &resolved.record)
            .await
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert!(items.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn ess5_http_stream_submit_rejects_missing_stream_candidate() -> anyhow::Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        let resolved = resolve_logical_downloader_for_owner(
            &database.pool,
            &store,
            HTTP_STREAM_DEFAULT_LOGICAL_ID,
            crate::download_broker::DEFAULT_ROUTE_OWNER_ID,
        )
        .await?;
        let mut request = sample_request(false);
        request.source = "https://stream.example.test/fmab/s01e01/master.m3u8".to_string();
        request.media_type = Some(MediaType::Anime);
        let result =
            submit_http_stream_broker(&database.pool, &resolved, &request.source, &request, None)
                .await;
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn qbittorrent_add_fields_force_paused_only_for_acquisition_staging() {
        let manual = sample_request(false);
        let manual_fields = qbittorrent_add_fields(TEST_MAGNET, Some("series"), &manual, false);
        assert_eq!(
            manual_fields.get("paused").map(String::as_str),
            Some("false")
        );
        assert_eq!(
            manual_fields.get("category").map(String::as_str),
            Some("series")
        );

        let mut unpaused_manual = sample_request(false);
        unpaused_manual.paused = None;
        let unpaused_fields =
            qbittorrent_add_fields(TEST_MAGNET, Some("series"), &unpaused_manual, false);
        assert!(!unpaused_fields.contains_key("paused"));

        let staged = sample_request(true);
        let staged_fields = qbittorrent_add_fields(TEST_MAGNET, Some("series"), &staged, true);
        assert_eq!(
            staged_fields.get("paused").map(String::as_str),
            Some("false")
        );
        assert_eq!(
            staged_fields.get("stopCondition").map(String::as_str),
            Some("MetadataReceived")
        );

        let staged_file_fields = qbittorrent_add_fields(
            "https://example.test/release.torrent",
            Some("series"),
            &staged,
            true,
        );
        assert_eq!(
            staged_file_fields.get("paused").map(String::as_str),
            Some("true")
        );
    }

    #[test]
    fn alm7_qbittorrent_info_hashes_are_canonical_and_mismatches_fail_before_submit() {
        let zero_hex = "0000000000000000000000000000000000000000";
        let zero_base32 = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        assert_eq!(
            qbittorrent_trackable_info_hash(&zero_hex.to_ascii_uppercase()).as_deref(),
            Some(zero_hex)
        );
        assert_eq!(
            qbittorrent_trackable_info_hash(&format!("urn:btih:{zero_base32}")).as_deref(),
            Some(zero_hex)
        );
        assert!(qbittorrent_trackable_info_hash("").is_none());
        assert!(
            qbittorrent_trackable_info_hash("00000000000000000000-00000000000000000000").is_none()
        );
        assert!(
            qbittorrent_trackable_info_hash("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz").is_none()
        );

        let equivalent_magnet = format!("magnet:?xt=urn%3Abtih%3A{zero_base32}&dn=canonical");
        assert_eq!(
            qbittorrent_submission_download_id(&equivalent_magnet, Some(zero_hex), true)
                .expect("equivalent base32 and hexadecimal BTIH values")
                .as_deref(),
            Some(zero_hex)
        );
        assert!(
            qbittorrent_submission_download_id(
                &equivalent_magnet,
                Some("1111111111111111111111111111111111111111"),
                true,
            )
            .is_err(),
            "candidate/magnet disagreement must fail before qBittorrent mutation"
        );
        assert!(
            qbittorrent_submission_download_id(
                TEST_MAGNET,
                Some("0123456789abcdef0123-456789abcdef01234567"),
                true,
            )
            .is_err()
        );
    }

    #[test]
    fn alm7_qbittorrent_torrent_lookup_never_falls_back_to_an_unrelated_row() {
        let rows = vec![
            json!({
                "hash": "1111111111111111111111111111111111111111",
                "name": "unrelated"
            }),
            json!({ "hash": TEST_HASH.to_ascii_uppercase(), "name": "wanted" }),
        ];
        assert_eq!(
            exact_qbittorrent_torrent_info(&rows, TEST_HASH)
                .as_ref()
                .and_then(|row| row.get("name"))
                .and_then(Value::as_str),
            Some("wanted")
        );
        assert!(
            exact_qbittorrent_torrent_info(&rows[..1], "2222222222222222222222222222222222222222")
                .is_none()
        );
    }

    #[test]
    fn qbittorrent_file_priority_fields_are_exact() {
        let file_ids = vec!["0".to_string(), "3".to_string(), "7".to_string()];
        let fields = qbittorrent_file_priority_fields(TEST_HASH, &file_ids, 0);
        assert_eq!(fields.get("hash").map(String::as_str), Some(TEST_HASH));
        assert_eq!(fields.get("id").map(String::as_str), Some("0|3|7"));
        assert_eq!(fields.get("priority").map(String::as_str), Some("0"));
        assert_eq!(fields.len(), 3);
    }

    #[test]
    fn qbittorrent_resume_fields_are_exact() {
        let fields = qbittorrent_resume_fields(TEST_HASH);
        assert_eq!(fields.get("hashes").map(String::as_str), Some(TEST_HASH));
        assert_eq!(fields.len(), 1);
    }

    #[test]
    fn asr5_qbittorrent_service_paths_preserve_hash_query_strings() {
        assert_eq!(
            qbittorrent_torrent_files_path(TEST_HASH),
            format!("api/v2/torrents/files?hash={TEST_HASH}")
        );
        assert_eq!(
            qbittorrent_torrents_info_path(TEST_HASH),
            format!("api/v2/torrents/info?hashes={TEST_HASH}")
        );

        let weird_hash = "abc 123/?:&=";
        assert_eq!(
            qbittorrent_torrent_files_path(weird_hash),
            "api/v2/torrents/files?hash=abc%20123%2F%3F%3A%26%3D"
        );
        assert_eq!(
            qbittorrent_torrents_info_path(weird_hash),
            "api/v2/torrents/info?hashes=abc%20123%2F%3F%3A%26%3D"
        );
    }

    #[test]
    fn qbittorrent_delete_fields_are_conservative_by_default() {
        let fields = qbittorrent_delete_fields(TEST_HASH, false);
        assert_eq!(fields.get("hashes").map(String::as_str), Some(TEST_HASH));
        assert_eq!(fields.get("deleteFiles").map(String::as_str), Some("false"));
        assert_eq!(fields.len(), 2);
    }

    #[test]
    fn qbittorrent_metadata_polling_backs_off_until_forced_or_completed() {
        let release_id = Uuid::new_v4();
        assert!(reserve_qbittorrent_metadata_poll(release_id, false));
        assert!(!reserve_qbittorrent_metadata_poll(release_id, false));
        assert!(reserve_qbittorrent_metadata_poll(release_id, true));
        assert!(!reserve_qbittorrent_metadata_poll(release_id, false));
        finish_qbittorrent_metadata_poll(release_id, 2);
        assert!(reserve_qbittorrent_metadata_poll(release_id, false));
        finish_qbittorrent_metadata_poll(release_id, 1);
    }

    #[tokio::test]
    async fn amr2_broker_context_preserves_review_release_fingerprint() -> anyhow::Result<()> {
        let database = setup_db().await?;
        let resolved = setup_qbittorrent_resolved(&database).await?;
        let mut request = sample_request(true);
        request.release_fingerprint = Some("sha256:manual-review-candidate".to_string());

        let torrent_context = qbittorrent_release_context(&request, &resolved, TEST_MAGNET, None)
            .expect("torrent release context");
        assert_eq!(
            torrent_context.fingerprint,
            "sha256:manual-review-candidate"
        );

        let debrid_context =
            debrid_release_context(&request, &resolved, Some("elixir.marketplace.torrentio"))
                .expect("debrid release context");
        assert_eq!(
            debrid_context.fingerprint.as_deref(),
            Some("sha256:manual-review-candidate")
        );
        Ok(())
    }

    #[tokio::test]
    async fn alm7_normal_broker_debrid_context_owns_internal_anime_bookkeeping_barrier()
    -> anyhow::Result<()> {
        let database = setup_db().await?;
        let resolved = setup_qbittorrent_resolved(&database).await?;
        let mut request = sample_request(true);
        request.media_type = Some(MediaType::Anime);
        request.defer_anime_debrid_refinement = true;

        let context =
            debrid_release_context(&request, &resolved, Some("elixir.marketplace.torrentio"))
                .context("anime Debrid broker context")?;
        assert_eq!(
            context
                .selected_candidate
                .as_ref()
                .and_then(|candidate| candidate.pointer("/submissionBookkeeping/status"))
                .and_then(Value::as_str),
            Some("pending")
        );

        request.defer_anime_debrid_refinement = false;
        let ordinary =
            debrid_release_context(&request, &resolved, Some("elixir.marketplace.torrentio"))
                .context("ordinary Debrid broker context")?;
        assert!(
            ordinary
                .selected_candidate
                .as_ref()
                .and_then(|candidate| candidate.get("submissionBookkeeping"))
                .is_none()
        );

        let decoded: DownloadBrokerSubmitRequest = serde_json::from_value(json!({
            "source": TEST_MAGNET,
            "deferAnimeDebridRefinement": true
        }))?;
        assert!(
            !decoded.defer_anime_debrid_refinement,
            "the HTTP payload must not control the internal acquisition barrier"
        );
        Ok(())
    }

    #[tokio::test]
    async fn alm7_initial_qbittorrent_scope_evidence_selects_one_target_and_fails_closed_otherwise()
    -> anyhow::Result<()> {
        let database = setup_db().await?;
        let resolved = setup_qbittorrent_resolved(&database).await?;
        let (subscription, targets) = setup_subscription_with_targets(
            &database,
            MediaType::Anime,
            "Scoped Anime",
            vec![
                NewAcquisitionTarget {
                    target_key: Some("anilist:7001:s01e01".to_string()),
                    media_type: Some(MediaType::Anime),
                    title: Some("Episode One".to_string()),
                    season_number: Some(1),
                    episode_number: Some(1),
                    absolute_episode_number: Some(1),
                    ..empty_target()
                },
                NewAcquisitionTarget {
                    target_key: Some("anilist:7001:s01e02".to_string()),
                    media_type: Some(MediaType::Anime),
                    title: Some("Episode Two".to_string()),
                    season_number: Some(1),
                    episode_number: Some(2),
                    absolute_episode_number: Some(2),
                    ..empty_target()
                },
            ],
        )
        .await?;
        let wanted = &targets[1];
        let wanted_id = wanted.target_id.to_string();
        let mut request = sample_request(true);
        request.subscription_id = Some(subscription.subscription_id);
        request.category = Some("anime".to_string());
        request.media_type = Some(MediaType::Anime);
        request.media_title = Some("Scoped Anime".to_string());
        request.name = Some("Opaque.Initial.Release".to_string());
        request.request_scope_evidence = Some(json!({
            "targetIds": [wanted_id.clone()],
            "targetKeys": [wanted.target_key.clone()],
        }));
        if let Some(candidate) = request.selected_candidate.as_mut() {
            candidate.title = "Opaque.Initial.Release".to_string();
        }
        let context = qbittorrent_release_context(&request, &resolved, TEST_MAGNET, None)
            .context("initial qBittorrent release context")?;
        let release = upsert_qbittorrent_acquisition_release(
            &database.pool,
            &resolved,
            &context,
            Some("anime"),
            Some(TEST_HASH),
        )
        .await?;
        let coverage = list_release_coverage(&database.pool, release.release_id).await?;
        assert!(coverage.is_empty());
        assert_eq!(
            release
                .selected_candidate
                .as_ref()
                .and_then(|candidate| candidate.pointer("/requestScopeEvidence/targetIds/0"))
                .and_then(Value::as_str),
            Some(wanted_id.as_str())
        );

        let scoped = qbittorrent_release_scoped_targets(&release, &targets, &coverage, TEST_HASH);
        assert_eq!(scoped.len(), 1);
        assert_eq!(scoped[0].target_id, wanted.target_id);

        let mut absent = release.clone();
        absent
            .selected_candidate
            .as_mut()
            .and_then(Value::as_object_mut)
            .context("selected candidate evidence object")?
            .remove("requestScopeEvidence");
        assert!(
            qbittorrent_release_scoped_targets(&absent, &targets, &coverage, TEST_HASH).is_empty(),
            "multiple unbound targets without initial scope evidence must fail closed"
        );

        let mut invalid = release.clone();
        invalid
            .selected_candidate
            .as_mut()
            .and_then(Value::as_object_mut)
            .context("selected candidate evidence object")?
            .insert(
                "requestScopeEvidence".to_string(),
                json!({
                    "targetIds": ["not-a-uuid"],
                    "targetKeys": ["anilist:7001:s99e99"],
                }),
            );
        assert!(
            qbittorrent_release_scoped_targets(&invalid, &targets, &coverage, TEST_HASH).is_empty(),
            "invalid initial scope evidence must not widen to the subscription"
        );

        let mut partial_disagreement = release.clone();
        partial_disagreement
            .selected_candidate
            .as_mut()
            .and_then(Value::as_object_mut)
            .context("selected candidate evidence object")?
            .insert(
                "requestScopeEvidence".to_string(),
                json!({
                    "targetIds": [targets[0].target_id, targets[1].target_id],
                    "targetKeys": [targets[0].target_key.clone()],
                }),
            );
        assert!(
            qbittorrent_request_scope_targets(&partial_disagreement, &targets)
                .is_some_and(|targets| targets.is_empty()),
            "partially overlapping ID/key evidence must fail closed"
        );

        for target in &targets {
            upsert_release_coverage(
                &database.pool,
                NewAcquisitionReleaseCoverage {
                    coverage_id: None,
                    release_id: release.release_id,
                    release_file_id: None,
                    target_id: target.target_id,
                    coverage_kind: ReleaseCoverageKind::SingleEpisode,
                    confidence: ReleaseConfidence::High,
                    score: Some(1.0),
                    reason: Some("scope-authority regression".to_string()),
                    state: ReleaseCoverageState::Submitted,
                    verified_by: Some("alm7_scope_test".to_string()),
                },
            )
            .await?;
        }
        let widened_coverage = list_release_coverage(&database.pool, release.release_id).await?;
        assert!(
            qbittorrent_release_scoped_targets(&release, &targets, &widened_coverage, TEST_HASH,)
                .is_empty(),
            "coverage outside authoritative request scope must fail closed"
        );

        let mut widened_bindings = targets.clone();
        for target in &mut widened_bindings {
            target.download_id = Some(TEST_HASH.to_string());
            target.selected_route_logical_id = Some(TORRENT_DEFAULT_LOGICAL_ID.to_string());
        }
        assert!(
            qbittorrent_release_scoped_targets(&release, &widened_bindings, &[], TEST_HASH)
                .is_empty(),
            "bindings outside authoritative request scope must fail closed"
        );
        Ok(())
    }

    #[tokio::test]
    async fn alm7_qbittorrent_pending_bookkeeping_reconciles_after_a_writer_crash()
    -> anyhow::Result<()> {
        let database = setup_db().await?;
        let resolved = setup_qbittorrent_resolved(&database).await?;
        let (subscription, targets) = setup_series_subscription(&database, 1..=1).await?;
        let release = setup_staged_qbittorrent_release(
            &database,
            &resolved,
            subscription.subscription_id,
            "Show.S01E01.1080p.WEB-DL-GROUP",
        )
        .await?;
        upsert_release_coverage(
            &database.pool,
            NewAcquisitionReleaseCoverage {
                coverage_id: None,
                release_id: release.release_id,
                release_file_id: None,
                target_id: targets[0].target_id,
                coverage_kind: ReleaseCoverageKind::SingleEpisode,
                confidence: ReleaseConfidence::High,
                score: Some(1.0),
                reason: Some("durable acquisition bookkeeping".to_string()),
                state: ReleaseCoverageState::Submitted,
                verified_by: Some("alm7_bookkeeping_test".to_string()),
            },
        )
        .await?;
        update_target_state(
            &database.pool,
            targets[0].target_id,
            AcquisitionTargetStateUpdate {
                state: AcquisitionTargetState::Submitted,
                state_reason: Some("submitted before writer restart".to_string()),
                selected_provider_id: release.source_provider_id,
                selected_route_logical_id: Some(TORRENT_DEFAULT_LOGICAL_ID.to_string()),
                selected_candidate: release.selected_candidate.clone(),
                download_id: Some(TEST_HASH.to_string()),
                ..Default::default()
            },
        )
        .await?;
        let mut pending_plan = release.coverage_plan.clone().unwrap_or_else(|| json!({}));
        pending_plan
            .as_object_mut()
            .context("bookkeeping coverage plan object")?
            .insert(
                "submissionBookkeeping".to_string(),
                json!({
                    "status": "pending",
                    "policyVersion": "acquisition-broker-bookkeeping-v1"
                }),
            );
        update_qbittorrent_release_state_only(
            &database.pool,
            release.release_id,
            TEST_HASH,
            AcquisitionReleaseState::Staging,
            AcquisitionReleaseState::Submitted,
            "acquisition writer stopped before closing its bookkeeping marker",
            pending_plan,
        )
        .await?;
        let pending = get_release(&database.pool, release.release_id)
            .await?
            .context("pending bookkeeping release")?;
        assert!(qbittorrent_submission_bookkeeping_pending(&pending));
        assert!(
            reconcile_qbittorrent_submission_bookkeeping(&database.pool, &pending, TEST_HASH)
                .await?
        );
        let reconciled = get_release(&database.pool, release.release_id)
            .await?
            .context("reconciled bookkeeping release")?;
        assert_eq!(
            reconciled
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.pointer("/submissionBookkeeping/status"))
                .and_then(Value::as_str),
            Some("complete")
        );
        assert!(!qbittorrent_submission_bookkeeping_pending(&reconciled));
        Ok(())
    }

    #[tokio::test]
    async fn asr4_qbittorrent_staged_magnet_lifecycle_contract() -> anyhow::Result<()> {
        let database = setup_db().await?;
        let resolved = setup_qbittorrent_resolved(&database).await?;
        let (subscription, targets) = setup_series_subscription(&database, 1..=1).await?;
        let mut request = sample_request(true);
        request.subscription_id = Some(subscription.subscription_id);
        request.category = Some("series".to_string());
        request.media_type = Some(MediaType::Series);
        request.media_title = Some("Show".to_string());
        request.name = Some("Show.S01E01.1080p.WEB-DL-GROUP".to_string());
        if let Some(candidate) = request.selected_candidate.as_mut() {
            candidate.title = "Show.S01E01.1080p.WEB-DL-GROUP".to_string();
        }

        let add_fields = qbittorrent_add_fields(
            "MAGNET:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
            request.category.as_deref(),
            &request,
            true,
        );
        assert_eq!(add_fields.get("paused").map(String::as_str), Some("false"));
        assert_eq!(
            add_fields.get("stopCondition").map(String::as_str),
            Some("MetadataReceived")
        );

        let context = qbittorrent_release_context(
            &request,
            &resolved,
            TEST_MAGNET,
            Some("elixir.marketplace.torrentio"),
        )
        .expect("release context");
        let release = upsert_qbittorrent_acquisition_release(
            &database.pool,
            &resolved,
            &context,
            Some("series"),
            Some(TEST_HASH),
        )
        .await?;
        upsert_qbittorrent_release_job(&database.pool, &resolved, &release, Some(TEST_HASH))
            .await?;
        assert!(qbittorrent_submission_bookkeeping_pending(&release));
        assert!(!qbittorrent_priority_bookkeeping_pending(&release));

        assert_eq!(release.state, AcquisitionReleaseState::Staging);
        assert_eq!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.get("metadataStopCondition"))
                .and_then(Value::as_str),
            Some("metadata_received")
        );
        assert_eq!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.get("metadataState"))
                .and_then(Value::as_str),
            Some("pending")
        );
        let jobs = list_release_jobs(&database.pool, release.release_id).await?;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].state, ReleaseJobState::Staging);

        let files = list_release_files(&database.pool, release.release_id).await?;
        assert!(files.is_empty());
        let waiting = qbittorrent_runtime_state(
            &release,
            &files,
            &json!({
                "hash": TEST_HASH,
                "state": "metaDL",
                "progress": 0.0,
                "category": "series"
            }),
        );
        assert_eq!(waiting, "waiting_metadata");

        let rows = vec![
            json!({
                "index": 0,
                "name": "Show.S01E01.1080p.WEB-DL-GROUP.mkv",
                "size": 1_500_000_000u64,
                "priority": 1,
                "progress": 0.0
            }),
            json!({
                "index": 1,
                "name": "sample.mkv",
                "size": 25_000_000u64,
                "priority": 1,
                "progress": 0.0
            }),
        ];
        assert_eq!(
            persist_qbittorrent_release_file_rows(&database.pool, &release, TEST_HASH, &rows)
                .await?,
            2
        );
        let files = list_release_files(&database.pool, release.release_id).await?;
        let refinement =
            refine_tv_qbittorrent_coverage(&database.pool, &release, TEST_HASH, &targets, &files)
                .await?;
        let coverage = list_release_coverage(&database.pool, release.release_id).await?;
        let decision = decide_qbittorrent_file_priority(&release, &refinement, &files, &coverage);
        assert!(decision.is_approved());
        assert_eq!(decision.selected_file_ids, vec!["0"]);
        assert_eq!(decision.skipped_file_ids, vec!["1"]);

        persist_qbittorrent_priority_decision(
            &database.pool,
            &release,
            &files,
            &coverage,
            &refinement,
            &decision,
            true,
        )
        .await?;
        let ready = get_release(&database.pool, release.release_id)
            .await?
            .expect("ready release");
        assert_eq!(ready.state, AcquisitionReleaseState::Ready);
        assert_eq!(
            ready
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.get("priorityPolicy"))
                .and_then(|policy| policy.get("priorityApplied"))
                .and_then(Value::as_bool),
            Some(true)
        );
        let files = list_release_files(&database.pool, release.release_id).await?;
        assert_eq!(
            files
                .iter()
                .find(|file| file.provider_file_id.as_deref() == Some("0"))
                .and_then(|file| file.selected),
            Some(true)
        );
        assert_eq!(
            files
                .iter()
                .find(|file| file.provider_file_id.as_deref() == Some("1"))
                .and_then(|file| file.selected),
            Some(false)
        );
        mark_qbittorrent_release_resumed(&database.pool, &ready, TEST_HASH).await?;
        let resumed = get_release(&database.pool, release.release_id)
            .await?
            .expect("resumed release");
        assert_eq!(resumed.state, AcquisitionReleaseState::Downloading);
        Ok(())
    }

    #[tokio::test]
    async fn qbittorrent_staged_release_job_and_file_rows_are_deduped() -> anyhow::Result<()> {
        let database = setup_db().await?;
        let resolved = setup_qbittorrent_resolved(&database).await?;
        let request = sample_request(true);
        let context = qbittorrent_release_context(
            &request,
            &resolved,
            TEST_MAGNET,
            Some("elixir.marketplace.torrentio"),
        )
        .expect("release context");

        let first = upsert_qbittorrent_acquisition_release(
            &database.pool,
            &resolved,
            &context,
            Some("series"),
            Some(TEST_HASH),
        )
        .await?;
        upsert_qbittorrent_release_job(&database.pool, &resolved, &first, Some(TEST_HASH)).await?;

        let second = upsert_qbittorrent_acquisition_release(
            &database.pool,
            &resolved,
            &context,
            Some("series"),
            Some(TEST_HASH),
        )
        .await?;
        upsert_qbittorrent_release_job(&database.pool, &resolved, &second, Some(TEST_HASH)).await?;

        assert_eq!(first.release_id, second.release_id);
        assert_eq!(second.download_id.as_deref(), Some(TEST_HASH));
        assert_eq!(second.state, AcquisitionReleaseState::Staging);
        assert_eq!(
            list_release_jobs(&database.pool, second.release_id)
                .await?
                .len(),
            1
        );
        let reusable =
            reusable_qbittorrent_release(&database.pool, &context, TORRENT_DEFAULT_LOGICAL_ID)
                .await?
                .expect("reusable release");
        assert_eq!(reusable.release_id, first.release_id);

        let rows = vec![
            json!({
                "index": 0,
                "name": "Show/Season 01/Show.S01E01.1080p.WEB-DL-GROUP.mkv",
                "size": 1_500_000_000u64,
                "priority": 1,
                "progress": 0.25,
                "availability": 0.9
            }),
            json!({
                "index": 1,
                "name": "Show/Season 01/sample.mkv",
                "size": 25_000_000u64,
                "priority": 0,
                "progress": 0.0,
                "availability": 1.0
            }),
        ];
        assert_eq!(
            persist_qbittorrent_release_file_rows(&database.pool, &second, TEST_HASH, &rows)
                .await?,
            2
        );
        assert_eq!(
            persist_qbittorrent_release_file_rows(&database.pool, &second, TEST_HASH, &rows)
                .await?,
            2
        );
        let files = list_release_files(&database.pool, second.release_id).await?;
        assert_eq!(files.len(), 2);
        let episode = files
            .iter()
            .find(|file| file.file_index == Some(0))
            .expect("episode file");
        assert_eq!(episode.provider_file_id.as_deref(), Some("0"));
        assert_eq!(episode.selected, Some(true));
        assert_eq!(episode.parsed_season_number, Some(1));
        assert_eq!(episode.parsed_episode_number, Some(1));
        assert_eq!(
            episode
                .provider_metadata
                .as_ref()
                .and_then(|value| value.get("mediaClassification"))
                .and_then(Value::as_str),
            Some("media")
        );
        let sample = files
            .iter()
            .find(|file| file.file_index == Some(1))
            .expect("sample file");
        assert_eq!(sample.selected, Some(false));
        assert_eq!(
            sample
                .provider_metadata
                .as_ref()
                .and_then(|value| value.get("mediaClassification"))
                .and_then(Value::as_str),
            Some("sample")
        );
        Ok(())
    }

    #[tokio::test]
    async fn qbittorrent_tv_season_pack_selects_targets_and_skips_sample() -> anyhow::Result<()> {
        let database = setup_db().await?;
        let resolved = setup_qbittorrent_resolved(&database).await?;
        let (subscription, targets) = setup_series_subscription(&database, 1..=2).await?;
        let release = setup_staged_qbittorrent_release(
            &database,
            &resolved,
            subscription.subscription_id,
            "Show.S01.COMPLETE.1080p.WEB-DL-GROUP",
        )
        .await?;
        assert_eq!(release.state, AcquisitionReleaseState::Staging);
        assert!(
            list_release_files(&database.pool, release.release_id)
                .await?
                .is_empty(),
            "fresh qBittorrent submission must wait for the active-release metadata poll"
        );
        let rows = vec![
            json!({
                "index": 0,
                "name": "Show/Season 01/Show.S01E01.1080p.WEB-DL-GROUP.mkv",
                "size": 1_500_000_000u64,
                "priority": 1,
                "progress": 0.0
            }),
            json!({
                "index": 1,
                "name": "Show/Season 01/Show.S01E02.1080p.WEB-DL-GROUP.mkv",
                "size": 1_500_000_000u64,
                "priority": 1,
                "progress": 0.0
            }),
            json!({
                "index": 2,
                "name": "Show/Season 01/sample.mkv",
                "size": 25_000_000u64,
                "priority": 1,
                "progress": 0.0
            }),
        ];
        persist_qbittorrent_release_file_rows(&database.pool, &release, TEST_HASH, &rows).await?;

        let files = list_release_files(&database.pool, release.release_id).await?;
        let refinement =
            refine_tv_qbittorrent_coverage(&database.pool, &release, TEST_HASH, &targets, &files)
                .await?;
        assert_eq!(refinement.release_kind, ReleaseKind::SeasonPack);
        assert_eq!(refinement.confidence, ReleaseConfidence::High);

        let coverage = list_release_coverage(&database.pool, release.release_id).await?;
        assert_eq!(coverage.len(), 2);
        let decision = decide_qbittorrent_file_priority(&release, &refinement, &files, &coverage);
        assert!(decision.is_approved());
        assert_eq!(decision.selected_file_ids, vec!["0", "1"]);
        assert_eq!(decision.skipped_file_ids, vec!["2"]);
        assert!(decision.review_reasons.is_empty());

        persist_qbittorrent_priority_decision(
            &database.pool,
            &release,
            &files,
            &coverage,
            &refinement,
            &decision,
            false,
        )
        .await?;

        let updated_files = list_release_files(&database.pool, release.release_id).await?;
        assert_eq!(
            updated_files
                .iter()
                .find(|file| file.provider_file_id.as_deref() == Some("0"))
                .and_then(|file| file.selected),
            Some(true)
        );
        assert_eq!(
            updated_files
                .iter()
                .find(|file| file.provider_file_id.as_deref() == Some("1"))
                .and_then(|file| file.selected),
            Some(true)
        );
        assert_eq!(
            updated_files
                .iter()
                .find(|file| file.provider_file_id.as_deref() == Some("2"))
                .and_then(|file| file.selected),
            Some(false)
        );
        let updated_coverage = list_release_coverage(&database.pool, release.release_id).await?;
        assert!(
            updated_coverage
                .iter()
                .all(|entry| entry.state == ReleaseCoverageState::Selected)
        );
        let updated_release = get_release(&database.pool, release.release_id)
            .await?
            .expect("release");
        assert_eq!(updated_release.state, AcquisitionReleaseState::Ready);
        assert_eq!(
            updated_release
                .coverage_plan
                .as_ref()
                .and_then(|value| value.get("priorityPolicy"))
                .and_then(|value| value.get("status"))
                .and_then(Value::as_str),
            Some("approved")
        );
        let jobs = list_release_jobs(&database.pool, release.release_id).await?;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].state, ReleaseJobState::Ready);
        let updated_targets =
            list_subscription_targets(&database.pool, subscription.subscription_id).await?;
        assert!(
            updated_targets
                .iter()
                .all(|target| target.state == AcquisitionTargetState::Submitted)
        );
        Ok(())
    }

    #[tokio::test]
    async fn qbittorrent_tv_multi_episode_single_file_is_selected() -> anyhow::Result<()> {
        let database = setup_db().await?;
        let resolved = setup_qbittorrent_resolved(&database).await?;
        let (subscription, targets) = setup_series_subscription(&database, 1..=2).await?;
        let release = setup_staged_qbittorrent_release(
            &database,
            &resolved,
            subscription.subscription_id,
            "Show.S01E01-E02.1080p.WEB-DL-GROUP",
        )
        .await?;
        let rows = vec![
            json!({
                "index": 0,
                "name": "Show.S01E01-E02.1080p.WEB-DL-GROUP.mkv",
                "size": 2_500_000_000u64,
                "priority": 1,
                "progress": 0.0
            }),
            json!({
                "index": 1,
                "name": "sample.mkv",
                "size": 25_000_000u64,
                "priority": 1,
                "progress": 0.0
            }),
        ];
        persist_qbittorrent_release_file_rows(&database.pool, &release, TEST_HASH, &rows).await?;

        let files = list_release_files(&database.pool, release.release_id).await?;
        let refinement =
            refine_tv_qbittorrent_coverage(&database.pool, &release, TEST_HASH, &targets, &files)
                .await?;
        assert_eq!(refinement.release_kind, ReleaseKind::MultiEpisode);
        assert_eq!(refinement.confidence, ReleaseConfidence::High);

        let coverage = list_release_coverage(&database.pool, release.release_id).await?;
        assert_eq!(coverage.len(), 2);
        let decision = decide_qbittorrent_file_priority(&release, &refinement, &files, &coverage);
        assert!(decision.is_approved());
        assert_eq!(decision.selected_file_ids, vec!["0"]);
        assert_eq!(decision.skipped_file_ids, vec!["1"]);
        Ok(())
    }

    #[tokio::test]
    async fn qbittorrent_tv_multi_season_pack_selects_only_wanted_files() -> anyhow::Result<()> {
        let database = setup_db().await?;
        let resolved = setup_qbittorrent_resolved(&database).await?;
        let (subscription, targets) = setup_subscription_with_targets(
            &database,
            MediaType::Series,
            "Show",
            vec![
                NewAcquisitionTarget {
                    season_number: Some(1),
                    episode_number: Some(1),
                    ..empty_target()
                },
                NewAcquisitionTarget {
                    season_number: Some(1),
                    episode_number: Some(2),
                    ..empty_target()
                },
                NewAcquisitionTarget {
                    season_number: Some(2),
                    episode_number: Some(1),
                    ..empty_target()
                },
                NewAcquisitionTarget {
                    season_number: Some(2),
                    episode_number: Some(2),
                    ..empty_target()
                },
            ],
        )
        .await?;
        let release = setup_staged_qbittorrent_release(
            &database,
            &resolved,
            subscription.subscription_id,
            "Show.S01-S02.1080p.WEB-DL-GROUP",
        )
        .await?;
        let rows = vec![
            json!({ "index": 0, "name": "Show/Season 01/Show.S01E01.1080p.mkv", "size": 1_500_000_000u64, "priority": 1, "progress": 0.0 }),
            json!({ "index": 1, "name": "Show/Season 01/Show.S01E02.1080p.mkv", "size": 1_500_000_000u64, "priority": 1, "progress": 0.0 }),
            json!({ "index": 2, "name": "Show/Season 02/Show.S02E01.1080p.mkv", "size": 1_500_000_000u64, "priority": 1, "progress": 0.0 }),
            json!({ "index": 3, "name": "Show/Season 02/Show.S02E02.1080p.mkv", "size": 1_500_000_000u64, "priority": 1, "progress": 0.0 }),
            json!({ "index": 4, "name": "Show/Season 02/sample.mkv", "size": 25_000_000u64, "priority": 1, "progress": 0.0 }),
            json!({ "index": 5, "name": "Show/Season 02/Show.S02E02.en.srt", "size": 50_000u64, "priority": 1, "progress": 0.0 }),
        ];
        persist_qbittorrent_release_file_rows(&database.pool, &release, TEST_HASH, &rows).await?;

        let files = list_release_files(&database.pool, release.release_id).await?;
        let refinement =
            refine_tv_qbittorrent_coverage(&database.pool, &release, TEST_HASH, &targets, &files)
                .await?;
        assert_eq!(refinement.release_kind, ReleaseKind::MultiSeasonPack);
        assert_eq!(refinement.confidence, ReleaseConfidence::High);

        let coverage = list_release_coverage(&database.pool, release.release_id).await?;
        assert_eq!(coverage.len(), 4);
        let decision = decide_qbittorrent_file_priority(&release, &refinement, &files, &coverage);
        assert!(decision.is_approved());
        assert_eq!(decision.selected_file_ids, vec!["0", "1", "2", "3"]);
        assert_eq!(decision.skipped_file_ids, vec!["4", "5"]);
        Ok(())
    }

    #[tokio::test]
    async fn qbittorrent_tv_daily_release_maps_by_air_date_and_selects_file() -> anyhow::Result<()>
    {
        let database = setup_db().await?;
        let resolved = setup_qbittorrent_resolved(&database).await?;
        let (subscription, targets) = setup_subscription_with_targets(
            &database,
            MediaType::Series,
            "A Late Talk Show",
            vec![NewAcquisitionTarget {
                title: Some("Gov. Deval Patrick".to_string()),
                season_number: Some(2011),
                episode_number: Some(412),
                air_date: Some("2011-04-12".to_string()),
                ..empty_target()
            }],
        )
        .await?;
        let release = setup_staged_qbittorrent_release_for(
            &database,
            &resolved,
            subscription.subscription_id,
            MediaType::Series,
            "A Late Talk Show",
            "series",
            "A Late Talk Show - 2011-04-12 - Gov. Deval Patrick",
        )
        .await?;
        let rows = vec![json!({
            "index": 0,
            "name": "A Late Talk Show - 2011-04-12 - Gov. Deval Patrick.mkv",
            "size": 1_500_000_000u64,
            "priority": 1,
            "progress": 0.0
        })];
        persist_qbittorrent_release_file_rows(&database.pool, &release, TEST_HASH, &rows).await?;

        let files = list_release_files(&database.pool, release.release_id).await?;
        let refinement =
            refine_tv_qbittorrent_coverage(&database.pool, &release, TEST_HASH, &targets, &files)
                .await?;
        assert_eq!(refinement.release_kind, ReleaseKind::Single);
        assert_eq!(refinement.confidence, ReleaseConfidence::High);
        let coverage = list_release_coverage(&database.pool, release.release_id).await?;
        assert_eq!(coverage.len(), 1);
        let decision = decide_qbittorrent_file_priority(&release, &refinement, &files, &coverage);
        assert!(decision.is_approved());
        assert_eq!(decision.selected_file_ids, vec!["0"]);
        assert!(decision.skipped_file_ids.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn qbittorrent_runtime_evidence_completes_release_and_attaches_local_paths()
    -> anyhow::Result<()> {
        let database = setup_db().await?;
        let resolved = setup_qbittorrent_resolved(&database).await?;
        let (subscription, targets) = setup_series_subscription(&database, 1..=1).await?;
        let release = setup_staged_qbittorrent_release(
            &database,
            &resolved,
            subscription.subscription_id,
            "Show.S01E01.1080p.WEB-DL-GROUP",
        )
        .await?;
        let rows = vec![json!({
            "index": 0,
            "name": "Show/Season 01/Show.S01E01.1080p.WEB-DL-GROUP.mkv",
            "size": 1_500_000_000u64,
            "priority": 1,
            "progress": 1.0
        })];
        persist_qbittorrent_release_file_rows(&database.pool, &release, TEST_HASH, &rows).await?;
        let files = list_release_files(&database.pool, release.release_id).await?;
        let refinement =
            refine_tv_qbittorrent_coverage(&database.pool, &release, TEST_HASH, &targets, &files)
                .await?;
        let coverage = list_release_coverage(&database.pool, release.release_id).await?;
        let decision = decide_qbittorrent_file_priority(&release, &refinement, &files, &coverage);
        persist_qbittorrent_priority_decision(
            &database.pool,
            &release,
            &files,
            &coverage,
            &refinement,
            &decision,
            true,
        )
        .await?;
        let release = get_release(&database.pool, release.release_id)
            .await?
            .expect("release");

        let evidence = load_qbittorrent_torrent_evidence(
            &database.pool,
            &resolved.record,
            &release,
            &json!({
                "hash": TEST_HASH,
                "state": "uploading",
                "progress": 1.0,
                "category": "series",
                "save_path": "/downloads",
                "content_path": "/downloads/Show"
            }),
        )
        .await?;

        assert_eq!(evidence.runtime_state, "completed");
        assert_eq!(evidence.metadata_state, "files_available");
        assert_eq!(evidence.priority_state, "applied");
        assert_eq!(evidence.selected_file_count, 1);
        assert_eq!(evidence.skipped_file_count, 0);
        assert_eq!(
            evidence.route_logical_id.as_deref(),
            Some(TORRENT_DEFAULT_LOGICAL_ID)
        );

        let completed = get_release(&database.pool, release.release_id)
            .await?
            .expect("completed release");
        assert_eq!(completed.state, AcquisitionReleaseState::Completed);
        assert_eq!(
            completed
                .coverage_plan
                .as_ref()
                .and_then(|value| value.get("torrentRuntime"))
                .and_then(|value| value.get("runtimeState"))
                .and_then(Value::as_str),
            Some("completed")
        );
        let jobs = list_release_jobs(&database.pool, release.release_id).await?;
        assert_eq!(jobs[0].state, ReleaseJobState::Completed);
        assert!(!jobs[0].active);
        let files = list_release_files(&database.pool, release.release_id).await?;
        assert_eq!(
            files[0]
                .provider_metadata
                .as_ref()
                .and_then(|value| value.get("localPath"))
                .and_then(Value::as_str),
            Some("/downloads/Show/Season 01/Show.S01E01.1080p.WEB-DL-GROUP.mkv")
        );
        Ok(())
    }

    #[tokio::test]
    async fn qbittorrent_tv_single_episode_selects_wanted_file_and_skips_sample()
    -> anyhow::Result<()> {
        let database = setup_db().await?;
        let resolved = setup_qbittorrent_resolved(&database).await?;
        let (subscription, targets) = setup_series_subscription(&database, 1..=1).await?;
        let release = setup_staged_qbittorrent_release(
            &database,
            &resolved,
            subscription.subscription_id,
            "Show.S01E01.1080p.WEB-DL-GROUP",
        )
        .await?;
        let rows = vec![
            json!({
                "index": 0,
                "name": "Show.S01E01.1080p.WEB-DL-GROUP.mkv",
                "size": 1_500_000_000u64,
                "priority": 1,
                "progress": 0.0
            }),
            json!({
                "index": 1,
                "name": "sample.mkv",
                "size": 25_000_000u64,
                "priority": 1,
                "progress": 0.0
            }),
        ];
        persist_qbittorrent_release_file_rows(&database.pool, &release, TEST_HASH, &rows).await?;

        let files = list_release_files(&database.pool, release.release_id).await?;
        let refinement =
            refine_tv_qbittorrent_coverage(&database.pool, &release, TEST_HASH, &targets, &files)
                .await?;
        assert_eq!(refinement.release_kind, ReleaseKind::Single);
        assert_eq!(refinement.confidence, ReleaseConfidence::High);

        let coverage = list_release_coverage(&database.pool, release.release_id).await?;
        let decision = decide_qbittorrent_file_priority(&release, &refinement, &files, &coverage);
        assert!(decision.is_approved());
        assert_eq!(decision.selected_file_ids, vec!["0"]);
        assert_eq!(decision.skipped_file_ids, vec!["1"]);

        persist_qbittorrent_priority_decision(
            &database.pool,
            &release,
            &files,
            &coverage,
            &refinement,
            &decision,
            false,
        )
        .await?;
        let updated_files = list_release_files(&database.pool, release.release_id).await?;
        assert_eq!(
            updated_files
                .iter()
                .find(|file| file.provider_file_id.as_deref() == Some("0"))
                .and_then(|file| file.selected),
            Some(true)
        );
        assert_eq!(
            updated_files
                .iter()
                .find(|file| file.provider_file_id.as_deref() == Some("1"))
                .and_then(|file| file.selected),
            Some(false)
        );
        Ok(())
    }

    #[tokio::test]
    async fn rrm5_qbittorrent_movie_selects_dominant_main_file_and_skips_sidecars()
    -> anyhow::Result<()> {
        let database = setup_db().await?;
        let resolved = setup_qbittorrent_resolved(&database).await?;
        let (subscription, targets) = setup_movie_subscription(&database, "Movie").await?;
        let release = setup_staged_qbittorrent_release_for(
            &database,
            &resolved,
            subscription.subscription_id,
            MediaType::Movie,
            "Movie",
            "movies",
            "Movie.2026.1080p.BluRay-GROUP",
        )
        .await?;
        let rows = vec![
            json!({ "index": 0, "name": "Movie.2026.1080p.BluRay/Movie.2026.1080p.BluRay-GROUP.mkv", "size": 8_000_000_000u64, "priority": 1, "progress": 0.0 }),
            json!({ "index": 1, "name": "Movie.2026.1080p.BluRay/Movie.2026.Commentary.Track.mkv", "size": 900_000_000u64, "priority": 1, "progress": 0.0 }),
            json!({ "index": 2, "name": "Movie.2026.1080p.BluRay/sample.mkv", "size": 50_000_000u64, "priority": 1, "progress": 0.0 }),
            json!({ "index": 3, "name": "Movie.2026.1080p.BluRay/Movie.2026.1080p.BluRay-GROUP.srt", "size": 100_000u64, "priority": 1, "progress": 0.0 }),
        ];
        persist_qbittorrent_release_file_rows(&database.pool, &release, TEST_HASH, &rows).await?;

        let files = list_release_files(&database.pool, release.release_id).await?;
        assert_eq!(
            files
                .iter()
                .find(|file| file.provider_file_id.as_deref() == Some("0"))
                .and_then(|file| file.size_bytes),
            Some(8_000_000_000)
        );
        assert_eq!(
            files
                .iter()
                .find(|file| file.provider_file_id.as_deref() == Some("0"))
                .and_then(|file| file.parsed_title.as_deref()),
            Some("Movie")
        );
        let refinement = refine_movie_qbittorrent_coverage(
            &database.pool,
            &release,
            TEST_HASH,
            &targets,
            &files,
        )
        .await?;
        assert_eq!(refinement.release_kind, ReleaseKind::Single);
        assert_eq!(
            refinement.resolver_kind,
            ReleaseResolverKind::MovieRadarrStyle
        );
        assert_eq!(refinement.confidence, ReleaseConfidence::High);
        assert_eq!(
            refinement
                .coverage_plan
                .pointer("/movie/fileSelection/status")
                .and_then(Value::as_str),
            Some("approved")
        );

        let coverage = list_release_coverage(&database.pool, release.release_id).await?;
        assert_eq!(coverage.len(), 1);
        assert_eq!(coverage[0].coverage_kind, ReleaseCoverageKind::Movie);
        let main_release_file_id = files
            .iter()
            .find(|file| file.provider_file_id.as_deref() == Some("0"))
            .map(|file| file.release_file_id);
        assert_eq!(coverage[0].release_file_id, main_release_file_id);
        let decision = decide_qbittorrent_file_priority(&release, &refinement, &files, &coverage);
        assert!(decision.is_approved());
        assert_eq!(decision.selected_file_ids, vec!["0"]);
        assert_eq!(decision.skipped_file_ids, vec!["1", "2", "3"]);

        persist_qbittorrent_priority_decision(
            &database.pool,
            &release,
            &files,
            &coverage,
            &refinement,
            &decision,
            false,
        )
        .await?;
        let updated_files = list_release_files(&database.pool, release.release_id).await?;
        assert_eq!(
            updated_files
                .iter()
                .find(|file| file.provider_file_id.as_deref() == Some("0"))
                .and_then(|file| file.selected),
            Some(true)
        );
        assert!(
            updated_files
                .iter()
                .filter(|file| file.provider_file_id.as_deref() != Some("0"))
                .all(|file| file.selected == Some(false))
        );
        Ok(())
    }

    #[tokio::test]
    async fn rrm5_qbittorrent_movie_comparable_media_files_require_review() -> anyhow::Result<()> {
        let database = setup_db().await?;
        let resolved = setup_qbittorrent_resolved(&database).await?;
        let (subscription, targets) = setup_movie_subscription(&database, "Movie").await?;
        let release = setup_staged_qbittorrent_release_for(
            &database,
            &resolved,
            subscription.subscription_id,
            MediaType::Movie,
            "Movie",
            "movies",
            "Movie.2026.1080p.BluRay-GROUP",
        )
        .await?;
        let rows = vec![
            json!({ "index": 0, "name": "Movie.2026.1080p.BluRay/Movie.2026.Part1.mkv", "size": 4_000_000_000u64, "priority": 1, "progress": 0.0 }),
            json!({ "index": 1, "name": "Movie.2026.1080p.BluRay/Movie.2026.Part2.mkv", "size": 3_900_000_000u64, "priority": 1, "progress": 0.0 }),
        ];
        persist_qbittorrent_release_file_rows(&database.pool, &release, TEST_HASH, &rows).await?;

        let files = list_release_files(&database.pool, release.release_id).await?;
        let refinement = refine_movie_qbittorrent_coverage(
            &database.pool,
            &release,
            TEST_HASH,
            &targets,
            &files,
        )
        .await?;
        assert_eq!(refinement.release_kind, ReleaseKind::Unknown);
        assert_eq!(
            refinement.resolver_kind,
            ReleaseResolverKind::MovieRadarrStyle
        );
        assert_eq!(refinement.confidence, ReleaseConfidence::ReviewRequired);
        assert!(
            refinement
                .review_reasons
                .iter()
                .any(|reason| reason == "ambiguous_movie_main_file")
        );

        let coverage = list_release_coverage(&database.pool, release.release_id).await?;
        assert_eq!(coverage.len(), 1);
        assert_eq!(coverage[0].coverage_kind, ReleaseCoverageKind::Movie);
        assert!(coverage[0].release_file_id.is_none());
        let decision = decide_qbittorrent_file_priority(&release, &refinement, &files, &coverage);
        assert_eq!(
            decision.status,
            QbittorrentFilePriorityDecisionStatus::ReviewRequired
        );
        assert!(decision.selected_file_ids.is_empty());
        assert!(
            decision
                .review_reasons
                .iter()
                .any(|reason| reason == "ambiguous_movie_main_file")
        );
        Ok(())
    }

    #[tokio::test]
    async fn qbittorrent_anime_absolute_episode_selects_wanted_file() -> anyhow::Result<()> {
        let database = setup_db().await?;
        let resolved = setup_qbittorrent_resolved(&database).await?;
        let (subscription, targets) = setup_subscription_with_targets(
            &database,
            MediaType::Anime,
            "Example Title",
            vec![NewAcquisitionTarget {
                title: Some("Episode One".to_string()),
                absolute_episode_number: Some(1),
                metadata: Some(json!({
                    "aliases": ["Example Title", "Example Title Alternative"]
                })),
                ..empty_target()
            }],
        )
        .await?;
        let release = setup_staged_qbittorrent_release_for(
            &database,
            &resolved,
            subscription.subscription_id,
            MediaType::Anime,
            "Example Title",
            "anime",
            "[SubsPlease] Example Title - 01 [1080p]",
        )
        .await?;
        let rows = vec![json!({
            "index": 0,
            "name": "Example Title - 01 [1080p].mkv",
            "size": 1_500_000_000u64,
            "priority": 1,
            "progress": 0.0
        })];
        persist_qbittorrent_release_file_rows(&database.pool, &release, TEST_HASH, &rows).await?;

        let files = list_release_files(&database.pool, release.release_id).await?;
        let engine =
            QbittorrentAnimeMatchEngine::new(QbittorrentAnimeEngineBehavior::MatchWantedFiles);
        let refinement = refine_anime_qbittorrent_coverage(
            &database.pool,
            &engine.service(),
            Some(&subscription),
            &release,
            TEST_HASH,
            &targets,
            &files,
        )
        .await?;
        assert_eq!(engine.call_count(), 0);
        assert_eq!(
            refinement
                .anime_match_assist
                .as_ref()
                .map(|assist| assist.source),
            Some(AnimeMatchAssistSource::DeterministicFastPath)
        );
        assert_eq!(refinement.release_kind, ReleaseKind::Single);
        assert_eq!(refinement.confidence, ReleaseConfidence::High);
        let coverage = list_release_coverage(&database.pool, release.release_id).await?;
        assert_eq!(coverage.len(), 1);
        let decision = decide_qbittorrent_file_priority(&release, &refinement, &files, &coverage);
        assert!(decision.is_approved());
        assert_eq!(decision.selected_file_ids, vec!["0"]);
        assert!(decision.skipped_file_ids.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn alm7_qbittorrent_required_audio_uses_selected_provider_filename_before_fast_path()
    -> anyhow::Result<()> {
        let database = setup_db().await?;
        let resolved = setup_qbittorrent_resolved(&database).await?;
        let (mut subscription, targets) = setup_subscription_with_targets(
            &database,
            MediaType::Anime,
            "Example Title",
            vec![NewAcquisitionTarget {
                target_key: Some("anilist:9301:A0001".to_string()),
                media_type: Some(MediaType::Anime),
                title: Some("Episode One".to_string()),
                season_number: Some(1),
                episode_number: Some(1),
                absolute_episode_number: Some(1),
                metadata: Some(json!({
                    "graphFingerprint": "example-title-graph-v1",
                    "anilistSeasonId": "9301",
                    "targetCanonicalKey": "anilist:9301:episode:1",
                    "aliases": ["Example Title"],
                    "scopedAliases": [{
                        "display": "Example Title",
                        "source": "anilist_english",
                        "language": "en",
                        "seasonNumber": 1,
                        "anilistSeasonId": "9301"
                    }]
                })),
                ..empty_target()
            }],
        )
        .await?;
        subscription.quality_profile = Some(json!({
            "animeAudioPreference": {
                "mode": "require_dub_review",
                "language": "en"
            }
        }));
        let release = setup_staged_qbittorrent_release_for(
            &database,
            &resolved,
            subscription.subscription_id,
            MediaType::Anime,
            "Example Title",
            "anime",
            "[Group] Example Title - 01 [Dual Audio] [1080p]",
        )
        .await?;
        persist_qbittorrent_release_file_rows(
            &database.pool,
            &release,
            TEST_HASH,
            &[json!({
                "index": 0,
                "name": "Example Title - 01 [Subbed] [1080p].mkv",
                "size": 1_500_000_000u64,
                "priority": 1,
                "progress": 0.0
            })],
        )
        .await?;
        let files = list_release_files(&database.pool, release.release_id).await?;
        let provider_candidate = qbittorrent_anime_acquisition_candidate(&release, &files);
        let provider_file_ids = vec!["0".to_string()];
        assert!(
            !assess_acquisition_anime_provider_file_audio(
                &subscription,
                &provider_candidate,
                &provider_file_ids,
            )
            .1,
            "the public require_dub_review contract must reject an explicitly subbed selected file"
        );
        let engine =
            QbittorrentAnimeMatchEngine::new(QbittorrentAnimeEngineBehavior::MatchWantedFiles);
        let refinement = refine_anime_qbittorrent_coverage(
            &database.pool,
            &engine.service(),
            Some(&subscription),
            &release,
            TEST_HASH,
            &targets,
            &files,
        )
        .await?;

        assert_eq!(
            engine.call_count(),
            1,
            "explicit selected-file audio mismatch must bypass the deterministic fast path"
        );
        assert_eq!(
            refinement
                .anime_match_assist
                .as_ref()
                .map(|assist| assist.source),
            Some(AnimeMatchAssistSource::DeterministicFallback)
        );
        assert!(
            refinement.automatic_retry,
            "the model's non-dub mapping must not satisfy a required English dub"
        );
        assert_eq!(
            refinement.anime_model_audio_policy.as_ref(),
            None,
            "audio policy remains deterministic and is never model-authored"
        );
        Ok(())
    }

    #[tokio::test]
    async fn qbittorrent_anime_batch_selects_files_and_skips_extras() -> anyhow::Result<()> {
        let database = setup_db().await?;
        let resolved = setup_qbittorrent_resolved(&database).await?;
        let (subscription, targets) = setup_subscription_with_targets(
            &database,
            MediaType::Anime,
            "Example Title",
            vec![
                NewAcquisitionTarget {
                    title: Some("Episode One".to_string()),
                    season_number: Some(1),
                    episode_number: Some(1),
                    absolute_episode_number: Some(1),
                    metadata: Some(json!({
                        "aliases": ["Example Title", "Example Title Alternative"]
                    })),
                    ..empty_target()
                },
                NewAcquisitionTarget {
                    title: Some("Episode Two".to_string()),
                    season_number: Some(1),
                    episode_number: Some(2),
                    absolute_episode_number: Some(2),
                    metadata: Some(json!({
                        "aliases": ["Example Title", "Example Title Alternative"]
                    })),
                    ..empty_target()
                },
            ],
        )
        .await?;
        let release = setup_staged_qbittorrent_release_for(
            &database,
            &resolved,
            subscription.subscription_id,
            MediaType::Anime,
            "Example Title",
            "anime",
            "[SubsPlease] Example Title S01 Batch [1080p]",
        )
        .await?;
        let rows = vec![
            json!({ "index": 0, "name": "Example Title - 01 [1080p].mkv", "size": 1_500_000_000u64, "priority": 1, "progress": 0.0 }),
            json!({ "index": 1, "name": "Example Title - 02 [1080p].mkv", "size": 1_500_000_000u64, "priority": 1, "progress": 0.0 }),
            json!({ "index": 2, "name": "sample.mkv", "size": 25_000_000u64, "priority": 1, "progress": 0.0 }),
            json!({ "index": 3, "name": "Example Title - 02 [1080p].eng.srt", "size": 50_000u64, "priority": 1, "progress": 0.0 }),
        ];
        persist_qbittorrent_release_file_rows(&database.pool, &release, TEST_HASH, &rows).await?;

        let files = list_release_files(&database.pool, release.release_id).await?;
        let matching_service = AnimeMatchingService::disabled();
        let refinement = refine_anime_qbittorrent_coverage(
            &database.pool,
            &matching_service,
            Some(&subscription),
            &release,
            TEST_HASH,
            &targets,
            &files,
        )
        .await?;
        assert_eq!(refinement.release_kind, ReleaseKind::SeasonPack);
        assert_eq!(refinement.confidence, ReleaseConfidence::High);
        let coverage = list_release_coverage(&database.pool, release.release_id).await?;
        assert_eq!(coverage.len(), 2);
        let decision = decide_qbittorrent_file_priority(&release, &refinement, &files, &coverage);
        assert!(decision.is_approved());
        assert_eq!(decision.selected_file_ids, vec!["0", "1"]);
        assert_eq!(decision.skipped_file_ids, vec!["2", "3"]);

        let (fixture, requests) = start_qbittorrent_priority_fixture(rows.clone()).await?;
        fixture.fail_form_paths_once(&["api/v2/torrents/filePrio", "api/v2/torrents/resume"]);
        configure_qbittorrent_priority_fixture_endpoint(&database, &resolved, fixture).await?;
        let state = setup_state_for_database(&database)?;
        let store = ExtensionStore::new(&database.pool);
        let submitted = get_release(&database.pool, release.release_id)
            .await?
            .context("fresh qBittorrent release")?;
        assert_eq!(submitted.state, AcquisitionReleaseState::Staging);
        assert_eq!(
            refresh_staged_qbittorrent_metadata(
                &state,
                &store,
                &submitted,
                TEST_HASH,
                true,
                false,
            )
                .await?,
            0,
            "broker staging must not race acquisition's post-submit bookkeeping"
        );
        assert!(
            requests
                .lock()
                .expect("qBittorrent priority fixture lock")
                .is_empty()
        );
        update_qbittorrent_release_state_only(
            &database.pool,
            submitted.release_id,
            TEST_HASH,
            AcquisitionReleaseState::Staging,
            AcquisitionReleaseState::Submitted,
            "Acquisition caller bookkeeping is durable.",
            submitted.coverage_plan.clone().unwrap_or_else(|| json!({})),
        )
        .await?;
        let submitted = get_release(&database.pool, release.release_id)
            .await?
            .context("bookkept qBittorrent release")?;
        let priority_error =
            refresh_staged_qbittorrent_metadata(&state, &store, &submitted, TEST_HASH, true, false)
                .await
                .expect_err("the first file-priority request must fail once");
        assert!(priority_error.to_string().contains("filePrio"));
        let ready_after_priority_failure = get_release(&database.pool, release.release_id)
            .await?
            .context("Ready release after file-priority failure")?;
        assert_eq!(
            ready_after_priority_failure.state,
            AcquisitionReleaseState::Ready
        );
        assert_eq!(
            ready_after_priority_failure
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.pointer("/priorityPolicy/priorityApplied"))
                .and_then(Value::as_bool),
            Some(false)
        );

        let resume_error = refresh_staged_qbittorrent_metadata(
            &state,
            &store,
            &ready_after_priority_failure,
            TEST_HASH,
            true,
            false,
        )
        .await
        .expect_err("the first resume request must fail once");
        assert!(resume_error.to_string().contains("resume"));
        let ready_after_resume_failure = get_release(&database.pool, release.release_id)
            .await?
            .context("Ready release after resume failure")?;
        assert_eq!(
            ready_after_resume_failure.state,
            AcquisitionReleaseState::Ready
        );
        assert_eq!(
            ready_after_resume_failure
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.pointer("/priorityPolicy/priorityApplied"))
                .and_then(Value::as_bool),
            Some(true)
        );

        assert_eq!(
            refresh_staged_qbittorrent_metadata(
                &state,
                &store,
                &ready_after_resume_failure,
                TEST_HASH,
                true,
                false,
            )
            .await?,
            rows.len()
        );
        let captured = requests
            .lock()
            .expect("qBittorrent priority fixture lock")
            .clone();
        let captured_paths = captured
            .iter()
            .filter_map(|request| request.pointer("/path").and_then(Value::as_str))
            .collect::<Vec<_>>();
        assert_eq!(
            captured_paths,
            vec![
                "api/v2/torrents/files",
                "api/v2/torrents/filePrio",
                "api/v2/torrents/files",
                "api/v2/torrents/filePrio",
                "api/v2/torrents/filePrio",
                "api/v2/torrents/resume",
                "api/v2/torrents/files",
                "api/v2/torrents/filePrio",
                "api/v2/torrents/filePrio",
                "api/v2/torrents/resume",
            ]
        );
        assert_eq!(
            captured[1].pointer("/fields/id").and_then(Value::as_str),
            Some("2|3")
        );
        assert_eq!(
            captured[1]
                .pointer("/fields/priority")
                .and_then(Value::as_str),
            Some("0")
        );
        assert_eq!(
            captured[8].pointer("/fields/id").and_then(Value::as_str),
            Some("0|1")
        );
        assert_eq!(
            captured[8]
                .pointer("/fields/priority")
                .and_then(Value::as_str),
            Some("1")
        );
        assert_eq!(
            captured[9]
                .pointer("/fields/hashes")
                .and_then(Value::as_str),
            Some(TEST_HASH)
        );
        let downloading = get_release(&database.pool, release.release_id)
            .await?
            .context("outer qBittorrent policy release")?;
        assert_eq!(downloading.state, AcquisitionReleaseState::Downloading);
        assert_eq!(
            downloading
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.pointer("/priorityPolicy/priorityApplied"))
                .and_then(Value::as_bool),
            Some(true)
        );
        Ok(())
    }

    #[tokio::test]
    async fn alm7_qbittorrent_anime_never_lets_model_map_opaque_pack_files() -> anyhow::Result<()> {
        let database = setup_db().await?;
        let resolved = setup_qbittorrent_resolved(&database).await?;
        let (subscription, targets) = setup_subscription_with_targets(
            &database,
            MediaType::Anime,
            "Tokyo Ghoul",
            vec![
                NewAcquisitionTarget {
                    target_key: Some("anilist:1002:s02e01".to_string()),
                    title: Some("Episode One".to_string()),
                    season_number: Some(2),
                    episode_number: Some(1),
                    absolute_episode_number: Some(13),
                    metadata: Some(tokyo_ghoul_root_a_target_metadata(1)),
                    ..empty_target()
                },
                NewAcquisitionTarget {
                    target_key: Some("anilist:1002:s02e02".to_string()),
                    title: Some("Episode Two".to_string()),
                    season_number: Some(2),
                    episode_number: Some(2),
                    absolute_episode_number: Some(14),
                    metadata: Some(tokyo_ghoul_root_a_target_metadata(2)),
                    ..empty_target()
                },
            ],
        )
        .await?;
        let release = setup_staged_qbittorrent_release_for(
            &database,
            &resolved,
            subscription.subscription_id,
            MediaType::Anime,
            "Tokyo Ghoul",
            "anime",
            "[Group] Tokyo Ghoul Root A S02 Complete [1080p]",
        )
        .await?;
        let rows = vec![
            json!({ "index": 0, "name": "Tokyo Ghoul Root A S02E01.mkv", "size": 1_500_000_000u64, "priority": 1, "progress": 0.0 }),
            json!({ "index": 1, "name": "Tokyo Ghoul Root A S02E02.mkv", "size": 1_500_000_000u64, "priority": 1, "progress": 0.0 }),
            json!({ "index": 2, "name": "TGRA-X-A/other-show-track.mkv", "size": 900_000_000u64, "priority": 1, "progress": 0.0 }),
        ];
        persist_qbittorrent_release_file_rows(&database.pool, &release, TEST_HASH, &rows).await?;
        let files = list_release_files(&database.pool, release.release_id).await?;
        let engine =
            QbittorrentAnimeMatchEngine::new(QbittorrentAnimeEngineBehavior::MatchWantedFiles);
        let refinement = refine_anime_qbittorrent_coverage(
            &database.pool,
            &engine.service(),
            Some(&subscription),
            &release,
            TEST_HASH,
            &targets,
            &files,
        )
        .await?;

        assert_eq!(engine.call_count(), 1);
        assert_eq!(refinement.confidence, ReleaseConfidence::ReviewRequired);
        assert!(refinement.automatic_retry);
        assert_eq!(refinement.anime_model_audio_profile, None);
        assert_eq!(refinement.anime_model_audio_policy.as_ref(), None);
        assert_eq!(
            refinement
                .anime_match_assist
                .as_ref()
                .map(|assist| assist.source),
            Some(AnimeMatchAssistSource::DeterministicFallback)
        );
        assert_eq!(
            refinement
                .anime_match_assist
                .as_ref()
                .map(|assist| assist.result),
            Some(AnimeMatchAssistResult::Fallback)
        );
        let coverage = list_release_coverage(&database.pool, release.release_id).await?;
        assert!(coverage.is_empty());
        let decision = decide_qbittorrent_file_priority(&release, &refinement, &files, &coverage);
        assert!(!decision.is_approved());
        assert!(decision.selected_file_ids.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn alm7_qbittorrent_pending_cleanup_replays_and_preserves_terminal_targets()
    -> anyhow::Result<()> {
        let database = setup_db().await?;
        let resolved = setup_qbittorrent_resolved(&database).await?;
        let (subscription, targets) = setup_subscription_with_targets(
            &database,
            MediaType::Anime,
            "Cleanup Anime",
            vec![NewAcquisitionTarget {
                target_key: Some("anilist:9001:s01e01".to_string()),
                media_type: Some(MediaType::Anime),
                title: Some("Episode One".to_string()),
                season_number: Some(1),
                episode_number: Some(1),
                absolute_episode_number: Some(1),
                ..empty_target()
            }],
        )
        .await?;
        let release = setup_staged_qbittorrent_release_for(
            &database,
            &resolved,
            subscription.subscription_id,
            MediaType::Anime,
            "Cleanup Anime",
            "anime",
            "[Group] Cleanup.Anime.01 [1080p]",
        )
        .await?;
        upsert_release_coverage(
            &database.pool,
            NewAcquisitionReleaseCoverage {
                coverage_id: None,
                release_id: release.release_id,
                release_file_id: None,
                target_id: targets[0].target_id,
                coverage_kind: ReleaseCoverageKind::SingleEpisode,
                confidence: ReleaseConfidence::High,
                score: Some(1.0),
                reason: Some("selected anime target".to_string()),
                state: ReleaseCoverageState::Submitted,
                verified_by: Some("alm7_cleanup_test".to_string()),
            },
        )
        .await?;
        update_target_state(
            &database.pool,
            targets[0].target_id,
            AcquisitionTargetStateUpdate {
                state: AcquisitionTargetState::Submitted,
                state_reason: Some("submitted to qBittorrent".to_string()),
                selected_provider_id: release.source_provider_id,
                selected_route_logical_id: Some(TORRENT_DEFAULT_LOGICAL_ID.to_string()),
                selected_candidate: release.selected_candidate.clone(),
                download_id: Some(TEST_HASH.to_string()),
                ..Default::default()
            },
        )
        .await?;
        let refinement = QbittorrentCoverageRefinement {
            release_kind: ReleaseKind::Unknown,
            resolver_kind: ReleaseResolverKind::Unresolved,
            resolver_version: QBITTORRENT_SELECTION_POLICY_VERSION.to_string(),
            confidence: ReleaseConfidence::Low,
            review_reasons: Vec::new(),
            coverage_plan: release.coverage_plan.clone().unwrap_or_else(|| json!({})),
            anime_match_assist: None,
            anime_model_audio_profile: None,
            anime_model_audio_policy: None,
            automatic_retry: true,
            suppress_automatic_rediscovery: false,
        };
        persist_anime_qbittorrent_retry(&database.pool, &release, TEST_HASH, &refinement).await?;
        let failed = get_release(&database.pool, release.release_id)
            .await?
            .context("failed anime release with cleanup intent")?;
        assert!(anime_qbittorrent_cleanup_pending(&failed));

        let (fixture, requests) = start_qbittorrent_priority_fixture(Vec::new()).await?;
        fixture.fail_form_paths_once(&["api/v2/torrents/delete"]);
        configure_qbittorrent_priority_fixture_endpoint(&database, &resolved, fixture).await?;
        let state = setup_state_for_database(&database)?;
        let store = ExtensionStore::new(&database.pool);
        let failed_replay = replay_pending_anime_qbittorrent_cleanup(&state, &store, 10).await?;
        assert_eq!(failed_replay, 0);
        let still_pending = get_release(&database.pool, release.release_id)
            .await?
            .context("anime release after failed cleanup request")?;
        assert_eq!(still_pending.state, AcquisitionReleaseState::Failed);
        assert!(anime_qbittorrent_cleanup_pending(&still_pending));

        let replayed = replay_pending_anime_qbittorrent_cleanup(&state, &store, 10).await?;
        assert_eq!(replayed, 1);
        let captured = requests.lock().expect("cleanup request fixture").clone();
        assert_eq!(captured.len(), 2);
        for request in &captured {
            assert_eq!(
                request.pointer("/path").and_then(Value::as_str),
                Some("api/v2/torrents/delete")
            );
            assert_eq!(
                request.pointer("/fields/hashes").and_then(Value::as_str),
                Some(TEST_HASH)
            );
        }
        let cleaned = get_release(&database.pool, release.release_id)
            .await?
            .context("cleaned anime release")?;
        assert_eq!(
            cleaned
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.pointer("/torrentCleanup/status"))
                .and_then(Value::as_str),
            Some("completed")
        );
        let target =
            crate::acquisition::subscriptions::get_target(&database.pool, targets[0].target_id)
                .await?
                .context("automatic retry target")?;
        assert_eq!(target.state, AcquisitionTargetState::Pending);
        Ok(())
    }

    #[tokio::test]
    async fn alm7_qbittorrent_stale_retry_cannot_mutate_a_new_target_binding() -> anyhow::Result<()>
    {
        let database = setup_db().await?;
        let resolved = setup_qbittorrent_resolved(&database).await?;
        let (subscription, targets) = setup_subscription_with_targets(
            &database,
            MediaType::Anime,
            "Attempt Ownership Anime",
            vec![NewAcquisitionTarget {
                target_key: Some("anilist:9101:s01e01".to_string()),
                media_type: Some(MediaType::Anime),
                season_number: Some(1),
                episode_number: Some(1),
                ..empty_target()
            }],
        )
        .await?;
        let release = setup_staged_qbittorrent_release_for(
            &database,
            &resolved,
            subscription.subscription_id,
            MediaType::Anime,
            "Attempt Ownership Anime",
            "anime",
            "[Group] Attempt.Ownership.01 [1080p]",
        )
        .await?;
        upsert_release_coverage(
            &database.pool,
            NewAcquisitionReleaseCoverage {
                coverage_id: None,
                release_id: release.release_id,
                release_file_id: None,
                target_id: targets[0].target_id,
                coverage_kind: ReleaseCoverageKind::SingleEpisode,
                confidence: ReleaseConfidence::High,
                score: Some(1.0),
                reason: Some("attempt A coverage".to_string()),
                state: ReleaseCoverageState::Submitted,
                verified_by: Some("alm7_attempt_a".to_string()),
            },
        )
        .await?;
        let newer_hash = "1111111111111111111111111111111111111111";
        update_target_state(
            &database.pool,
            targets[0].target_id,
            AcquisitionTargetStateUpdate {
                state: AcquisitionTargetState::Submitted,
                state_reason: Some("newer attempt B".to_string()),
                selected_route_logical_id: Some(TORRENT_DEFAULT_LOGICAL_ID.to_string()),
                download_id: Some(newer_hash.to_string()),
                ..Default::default()
            },
        )
        .await?;
        let refinement = QbittorrentCoverageRefinement {
            release_kind: ReleaseKind::Unknown,
            resolver_kind: ReleaseResolverKind::Unresolved,
            resolver_version: QBITTORRENT_SELECTION_POLICY_VERSION.to_string(),
            confidence: ReleaseConfidence::Low,
            review_reasons: Vec::new(),
            coverage_plan: release.coverage_plan.clone().unwrap_or_else(|| json!({})),
            anime_match_assist: None,
            anime_model_audio_profile: None,
            anime_model_audio_policy: None,
            automatic_retry: true,
            suppress_automatic_rediscovery: false,
        };
        assert!(
            persist_anime_qbittorrent_retry(&database.pool, &release, TEST_HASH, &refinement)
                .await
                .is_err()
        );
        let current_release = get_release(&database.pool, release.release_id)
            .await?
            .context("attempt A release")?;
        assert_eq!(current_release.state, AcquisitionReleaseState::Staging);
        let jobs = list_release_jobs(&database.pool, release.release_id).await?;
        assert_eq!(jobs[0].state, ReleaseJobState::Staging);
        assert!(jobs[0].active);
        let coverage = list_release_coverage(&database.pool, release.release_id).await?;
        assert_eq!(coverage[0].state, ReleaseCoverageState::Submitted);
        let current_target =
            crate::acquisition::subscriptions::get_target(&database.pool, targets[0].target_id)
                .await?
                .context("attempt B target")?;
        assert_eq!(current_target.state, AcquisitionTargetState::Submitted);
        assert_eq!(current_target.download_id.as_deref(), Some(newer_hash));
        update_target_state(
            &database.pool,
            targets[0].target_id,
            AcquisitionTargetStateUpdate {
                state: AcquisitionTargetState::Imported,
                state_reason: Some("concurrent import completed".to_string()),
                ..Default::default()
            },
        )
        .await?;
        assert!(
            reset_target_for_candidate_retry(
                &database.pool,
                targets[0].target_id,
                "late retry".to_string(),
                chrono::Utc::now(),
            )
            .await?
            .is_none()
        );
        let imported =
            crate::acquisition::subscriptions::get_target(&database.pool, targets[0].target_id)
                .await?
                .context("imported target")?;
        assert_eq!(imported.state, AcquisitionTargetState::Imported);
        Ok(())
    }

    #[tokio::test]
    async fn alm7_qbittorrent_anime_unresolved_mapping_fails_and_schedules_retry()
    -> anyhow::Result<()> {
        let database = setup_db().await?;
        let resolved = setup_qbittorrent_resolved(&database).await?;
        let (subscription, targets) = setup_subscription_with_targets(
            &database,
            MediaType::Anime,
            "Example Title",
            vec![NewAcquisitionTarget {
                target_key: Some("anilist:2001:s01e01".to_string()),
                title: Some("Episode One".to_string()),
                season_number: Some(1),
                episode_number: Some(1),
                absolute_episode_number: Some(1),
                metadata: Some(json!({
                    "graphFingerprint": "example-title-graph-v1",
                    "targetCanonicalKey": "anilist:2001:episode:1",
                    "anilistSeasonId": "2001",
                    "aliases": ["Example Title"],
                    "scoped_aliases": [{
                        "display": "Example Title",
                        "source": "anilist_english",
                        "language": "en",
                        "seasonNumber": 1,
                        "anilistSeasonId": "2001"
                    }]
                })),
                ..empty_target()
            }],
        )
        .await?;
        let release = setup_staged_qbittorrent_release_for(
            &database,
            &resolved,
            subscription.subscription_id,
            MediaType::Anime,
            "Example Title",
            "anime",
            "[Group] opaque-collection-x [1080p]",
        )
        .await?;
        let rows = vec![
            json!({ "index": 0, "name": "opaque-collection-x/track-alpha.mkv", "size": 1_500_000_000u64, "priority": 1, "progress": 0.0 }),
            json!({ "index": 1, "name": "opaque-collection-x/track-beta.mkv", "size": 1_500_000_000u64, "priority": 1, "progress": 0.0 }),
        ];
        persist_qbittorrent_release_file_rows(&database.pool, &release, TEST_HASH, &rows).await?;

        let files = list_release_files(&database.pool, release.release_id).await?;
        upsert_release_coverage(
            &database.pool,
            NewAcquisitionReleaseCoverage {
                coverage_id: None,
                release_id: release.release_id,
                release_file_id: None,
                target_id: targets[0].target_id,
                coverage_kind: ReleaseCoverageKind::SingleEpisode,
                confidence: ReleaseConfidence::High,
                score: Some(1.0),
                reason: Some("initial candidate target".to_string()),
                state: ReleaseCoverageState::Submitted,
                verified_by: Some("test".to_string()),
            },
        )
        .await?;
        update_target_state(
            &database.pool,
            targets[0].target_id,
            AcquisitionTargetStateUpdate {
                state: AcquisitionTargetState::Submitted,
                state_reason: Some("submitted to qBittorrent".to_string()),
                selected_provider_id: release.source_provider_id,
                selected_route_logical_id: release.selected_route_logical_id.clone(),
                selected_candidate: release.selected_candidate.clone(),
                download_id: release.download_id.clone(),
                ..Default::default()
            },
        )
        .await?;

        let disabled_refinement = refine_anime_qbittorrent_coverage(
            &database.pool,
            &AnimeMatchingService::disabled(),
            Some(&subscription),
            &release,
            TEST_HASH,
            &targets,
            &files,
        )
        .await?;
        let error_engine = QbittorrentAnimeMatchEngine::new(QbittorrentAnimeEngineBehavior::Error);
        let error_refinement = refine_anime_qbittorrent_coverage(
            &database.pool,
            &error_engine.service(),
            Some(&subscription),
            &release,
            TEST_HASH,
            &targets,
            &files,
        )
        .await?;
        let timeout_engine =
            QbittorrentAnimeMatchEngine::new(QbittorrentAnimeEngineBehavior::Timeout);
        let timeout_refinement = refine_anime_qbittorrent_coverage(
            &database.pool,
            &timeout_engine.service(),
            Some(&subscription),
            &release,
            TEST_HASH,
            &targets,
            &files,
        )
        .await?;
        let invalid_engine =
            QbittorrentAnimeMatchEngine::new(QbittorrentAnimeEngineBehavior::InvalidFileReference);
        let invalid_refinement = refine_anime_qbittorrent_coverage(
            &database.pool,
            &invalid_engine.service(),
            Some(&subscription),
            &release,
            TEST_HASH,
            &targets,
            &files,
        )
        .await?;
        let engine = QbittorrentAnimeMatchEngine::new(QbittorrentAnimeEngineBehavior::Empty);
        let refinement = refine_anime_qbittorrent_coverage(
            &database.pool,
            &engine.service(),
            Some(&subscription),
            &release,
            TEST_HASH,
            &targets,
            &files,
        )
        .await?;
        assert_eq!(error_engine.call_count(), 1);
        assert_eq!(timeout_engine.call_count(), 1);
        assert_eq!(invalid_engine.call_count(), 1);
        assert_eq!(engine.call_count(), 1);
        assert!(disabled_refinement.automatic_retry);
        assert!(error_refinement.automatic_retry);
        assert!(timeout_refinement.automatic_retry);
        assert!(invalid_refinement.automatic_retry);
        assert!(refinement.automatic_retry);
        assert_eq!(
            disabled_refinement.coverage_plan.get("anime"),
            refinement.coverage_plan.get("anime"),
            "empty model output must retain the exact deterministic coverage plan"
        );
        assert_eq!(
            disabled_refinement.coverage_plan.get("anime"),
            error_refinement.coverage_plan.get("anime"),
            "model errors must retain the exact deterministic coverage plan"
        );
        assert_eq!(
            disabled_refinement.coverage_plan.get("anime"),
            timeout_refinement.coverage_plan.get("anime"),
            "model deadlines must retain the exact deterministic coverage plan"
        );
        assert_eq!(
            disabled_refinement.coverage_plan.get("anime"),
            invalid_refinement.coverage_plan.get("anime"),
            "invalid model file references must retain the exact deterministic coverage plan"
        );
        assert_eq!(
            error_refinement
                .anime_match_assist
                .as_ref()
                .and_then(|assist| assist.reason),
            Some(AnimeMatchFallbackReason::EngineError)
        );
        assert_eq!(
            timeout_refinement
                .anime_match_assist
                .as_ref()
                .and_then(|assist| assist.reason),
            Some(AnimeMatchFallbackReason::EngineError)
        );
        assert_eq!(
            invalid_refinement
                .anime_match_assist
                .as_ref()
                .and_then(|assist| assist.reason),
            Some(AnimeMatchFallbackReason::InvalidModelResponse)
        );
        assert_eq!(
            refinement
                .anime_match_assist
                .as_ref()
                .and_then(|assist| assist.reason),
            Some(AnimeMatchFallbackReason::EmptyModelMatches)
        );
        assert!(
            !anime_qbittorrent_retry_suppresses_rediscovery(&disabled_refinement),
            "a missing local runtime must remain eligible after scheduler backoff"
        );
        assert!(
            !anime_qbittorrent_retry_suppresses_rediscovery(&error_refinement),
            "a transient model failure must remain eligible after runtime repair"
        );
        assert!(
            !anime_qbittorrent_retry_suppresses_rediscovery(&timeout_refinement),
            "a model deadline must remain eligible after scheduler backoff"
        );
        assert!(
            !anime_qbittorrent_retry_suppresses_rediscovery(&invalid_refinement),
            "invalid model output must preserve disabled-inference retry semantics"
        );
        assert!(
            !anime_qbittorrent_retry_suppresses_rediscovery(&refinement),
            "empty model output must preserve disabled-inference retry semantics"
        );

        let mut required_dub_subscription = subscription.clone();
        required_dub_subscription.quality_profile = Some(json!({
            "animeAudioPreference": {
                "mode": "require_dub_review",
                "language": "en"
            }
        }));
        let required_audio_engine =
            QbittorrentAnimeMatchEngine::new(QbittorrentAnimeEngineBehavior::MatchWantedFiles);
        let required_audio_refinement = refine_anime_qbittorrent_coverage(
            &database.pool,
            &required_audio_engine.service(),
            Some(&required_dub_subscription),
            &release,
            TEST_HASH,
            &targets,
            &files,
        )
        .await?;
        assert_eq!(required_audio_engine.call_count(), 1);
        assert!(required_audio_refinement.automatic_retry);
        assert_eq!(required_audio_refinement.anime_model_audio_profile, None);
        assert!(required_audio_refinement.anime_model_audio_policy.is_none());
        assert_eq!(
            required_audio_refinement
                .anime_match_assist
                .as_ref()
                .map(|assist| (assist.source, assist.result, assist.reason)),
            Some((
                AnimeMatchAssistSource::DeterministicFallback,
                AnimeMatchAssistResult::Fallback,
                Some(AnimeMatchFallbackReason::CoverageValidationFailed)
            )),
            "a model mapping rejected by required audio policy must report fallback"
        );
        assert!(
            !anime_qbittorrent_retry_suppresses_rediscovery(&required_audio_refinement),
            "opaque filenames provide no deterministic audio mismatch evidence"
        );
        let coverage = list_release_coverage(&database.pool, release.release_id).await?;
        assert_eq!(
            coverage.len(),
            1,
            "required-audio rejection must not persist model file coverage"
        );
        let disabled_decision =
            decide_qbittorrent_file_priority(&release, &disabled_refinement, &files, &coverage);
        let error_decision =
            decide_qbittorrent_file_priority(&release, &error_refinement, &files, &coverage);
        let timeout_decision =
            decide_qbittorrent_file_priority(&release, &timeout_refinement, &files, &coverage);
        let invalid_decision =
            decide_qbittorrent_file_priority(&release, &invalid_refinement, &files, &coverage);
        let decision = decide_qbittorrent_file_priority(&release, &refinement, &files, &coverage);
        assert_eq!(disabled_decision.status, error_decision.status);
        assert_eq!(disabled_decision.status, timeout_decision.status);
        assert_eq!(disabled_decision.status, invalid_decision.status);
        assert_eq!(disabled_decision.status, decision.status);
        assert_eq!(
            disabled_decision.selected_file_ids,
            error_decision.selected_file_ids
        );
        assert_eq!(
            disabled_decision.selected_file_ids,
            timeout_decision.selected_file_ids
        );
        assert_eq!(
            disabled_decision.selected_file_ids,
            invalid_decision.selected_file_ids
        );
        assert_eq!(
            disabled_decision.selected_file_ids,
            decision.selected_file_ids
        );
        assert_eq!(
            disabled_decision.skipped_file_ids,
            error_decision.skipped_file_ids
        );
        assert_eq!(
            disabled_decision.skipped_file_ids,
            timeout_decision.skipped_file_ids
        );
        assert_eq!(
            disabled_decision.skipped_file_ids,
            invalid_decision.skipped_file_ids
        );
        assert_eq!(
            disabled_decision.skipped_file_ids,
            decision.skipped_file_ids
        );
        assert!(!decision.is_approved());

        let mut release_with_stored_override = release.clone();
        release_with_stored_override.coverage_plan = Some(json!({
            "priorityPolicy": {
                "status": "approved",
                "userApproved": true,
                "selectedFileIds": ["0"],
                "skippedFileIds": ["1"]
            }
        }));
        let decision = qbittorrent_priority_decision_with_legacy_override(
            &release_with_stored_override,
            decision,
        );
        assert!(!decision.is_approved());
        assert!(!decision.user_approved);

        persist_anime_qbittorrent_retry(&database.pool, &release, TEST_HASH, &disabled_refinement)
            .await?;
        let disabled_persistence =
            alm7_qbittorrent_retry_persistence_snapshot(&database, release.release_id, &targets)
                .await?;

        persist_anime_qbittorrent_retry(&database.pool, &release, TEST_HASH, &timeout_refinement)
            .await?;
        assert_eq!(
            alm7_qbittorrent_retry_persistence_snapshot(&database, release.release_id, &targets,)
                .await?,
            disabled_persistence,
            "timeout fallback persistence must equal disabled-inference retry persistence"
        );

        persist_anime_qbittorrent_retry(&database.pool, &release, TEST_HASH, &invalid_refinement)
            .await?;
        assert_eq!(
            alm7_qbittorrent_retry_persistence_snapshot(&database, release.release_id, &targets,)
                .await?,
            disabled_persistence,
            "invalid-model fallback persistence must equal disabled-inference retry persistence"
        );

        persist_anime_qbittorrent_retry(&database.pool, &release, TEST_HASH, &invalid_refinement)
            .await?;
        assert_eq!(
            alm7_qbittorrent_retry_persistence_snapshot(&database, release.release_id, &targets,)
                .await?,
            disabled_persistence,
            "repeating the same retry persistence must not duplicate or drift canonical rows"
        );

        persist_anime_qbittorrent_retry(
            &database.pool,
            &release,
            TEST_HASH,
            &required_audio_refinement,
        )
        .await?;
        let updated_release = get_release(&database.pool, release.release_id)
            .await?
            .expect("release");
        assert_eq!(updated_release.state, AcquisitionReleaseState::Failed);
        assert_eq!(
            updated_release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.pointer("/automaticRetry/status"))
                .and_then(Value::as_str),
            Some("scheduled")
        );
        assert_eq!(
            updated_release
                .coverage_plan
                .as_ref()
                .and_then(|plan| { plan.pointer("/retrySuppression/suppressAutomaticRediscovery") })
                .and_then(Value::as_bool),
            Some(false)
        );
        assert_eq!(
            updated_release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.get("modelAudioProfile"))
                .and_then(Value::as_str),
            None
        );
        assert_eq!(
            updated_release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.pointer("/modelAudioPolicy/requiredPreferenceSatisfied"))
                .and_then(Value::as_bool),
            None
        );
        let jobs = list_release_jobs(&database.pool, release.release_id).await?;
        assert_eq!(jobs[0].state, ReleaseJobState::Failed);
        assert!(!jobs[0].active);
        let rejected_coverage = list_release_coverage(&database.pool, release.release_id).await?;
        assert!(
            rejected_coverage
                .iter()
                .all(|entry| entry.state == ReleaseCoverageState::Rejected)
        );
        let updated_files = list_release_files(&database.pool, release.release_id).await?;
        assert!(
            updated_files
                .iter()
                .all(|file| file.selected == Some(false))
        );
        let target =
            crate::acquisition::subscriptions::get_target(&database.pool, targets[0].target_id)
                .await?
                .expect("target");
        assert_eq!(target.state, AcquisitionTargetState::Pending);
        assert!(target.next_search_after.is_some());
        assert!(target.selected_candidate.is_none());
        let manual_review_count = sqlx::query_scalar::<sqlx::Any, i64>(
            "SELECT COUNT(*) FROM acquisition_releases
             WHERE subscription_id = $1 AND state = 'review_required'",
        )
        .bind(subscription.subscription_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(manual_review_count, 0);
        Ok(())
    }

    #[tokio::test]
    async fn qbittorrent_tv_unmapped_pack_stays_paused_for_review() -> anyhow::Result<()> {
        let database = setup_db().await?;
        let resolved = setup_qbittorrent_resolved(&database).await?;
        let (subscription, targets) = setup_series_subscription(&database, 1..=1).await?;
        let release = setup_staged_qbittorrent_release(
            &database,
            &resolved,
            subscription.subscription_id,
            "Show.S01.COMPLETE.1080p.WEB-DL-GROUP",
        )
        .await?;
        let rows = vec![
            json!({
                "index": 0,
                "name": "Show/Season 01/Show.S01E01.1080p.WEB-DL-GROUP.mkv",
                "size": 1_500_000_000u64,
                "priority": 1,
                "progress": 0.0
            }),
            json!({
                "index": 1,
                "name": "Show/Season 01/Bonus Feature.mkv",
                "size": 400_000_000u64,
                "priority": 1,
                "progress": 0.0
            }),
        ];
        persist_qbittorrent_release_file_rows(&database.pool, &release, TEST_HASH, &rows).await?;

        let files = list_release_files(&database.pool, release.release_id).await?;
        let refinement =
            refine_tv_qbittorrent_coverage(&database.pool, &release, TEST_HASH, &targets, &files)
                .await?;
        assert_eq!(refinement.confidence, ReleaseConfidence::ReviewRequired);
        assert!(
            refinement
                .review_reasons
                .iter()
                .any(|reason| reason == "unmapped_media_file")
        );

        let coverage = list_release_coverage(&database.pool, release.release_id).await?;
        let decision = decide_qbittorrent_file_priority(&release, &refinement, &files, &coverage);
        assert!(!decision.is_approved());
        assert!(decision.selected_file_ids.is_empty());
        assert!(
            decision
                .review_reasons
                .iter()
                .any(|reason| reason == "unmapped_media_file")
        );
        persist_qbittorrent_priority_decision(
            &database.pool,
            &release,
            &files,
            &coverage,
            &refinement,
            &decision,
            false,
        )
        .await?;

        let updated_release = get_release(&database.pool, release.release_id)
            .await?
            .expect("release");
        assert_eq!(
            updated_release.state,
            AcquisitionReleaseState::ReviewRequired
        );
        let jobs = list_release_jobs(&database.pool, release.release_id).await?;
        assert_eq!(jobs[0].state, ReleaseJobState::Staging);
        Ok(())
    }

    #[tokio::test]
    async fn qbittorrent_no_metadata_reports_waiting_metadata_without_resuming()
    -> anyhow::Result<()> {
        let database = setup_db().await?;
        let resolved = setup_qbittorrent_resolved(&database).await?;
        let (subscription, _targets) = setup_series_subscription(&database, 1..=2).await?;
        let release = setup_staged_qbittorrent_release(
            &database,
            &resolved,
            subscription.subscription_id,
            "Show.S01.COMPLETE.1080p.WEB-DL-GROUP",
        )
        .await?;
        let files = list_release_files(&database.pool, release.release_id).await?;
        assert!(files.is_empty());

        let torrent_info = json!({
            "hash": TEST_HASH,
            "state": "metadl",
            "progress": 0.0,
            "category": "series"
        });
        let runtime_state = qbittorrent_runtime_state(&release, &files, &torrent_info);
        assert_eq!(runtime_state, "waiting_metadata");
        let evidence = qbittorrent_torrent_evidence(
            &resolved.record,
            &release,
            &files,
            &torrent_info,
            &runtime_state,
        );
        assert_eq!(evidence.metadata_state, "waiting_metadata");
        assert_eq!(evidence.priority_state, "pending");
        assert_eq!(evidence.selected_file_count, 0);
        assert_eq!(evidence.skipped_file_count, 0);

        let jobs = list_release_jobs(&database.pool, release.release_id).await?;
        assert_eq!(jobs[0].state, ReleaseJobState::Staging);
        Ok(())
    }

    #[tokio::test]
    async fn alm7_qbittorrent_fresh_ready_provider_failures_remain_retryable_in_place()
    -> anyhow::Result<()> {
        let database = setup_db().await?;
        let resolved = setup_qbittorrent_resolved(&database).await?;
        let now = chrono::Utc::now();
        let started_at = now - chrono::Duration::seconds(QBITTORRENT_METADATA_TIMEOUT_SECONDS - 1);
        let (ready, target) =
            setup_ready_anime_qbittorrent_provider_attempt(&database, &resolved, started_at)
                .await?;

        for operation in [
            QbittorrentProviderOperation::TorrentInspection,
            QbittorrentProviderOperation::FilePolicyRefresh,
        ] {
            assert!(
                terminalize_aged_anime_qbittorrent_provider_failure(
                    &database.pool,
                    ready.release_id,
                    TEST_HASH,
                    operation,
                    now,
                )
                .await?
                .is_none(),
                "transient {operation:?} failures must retain a fresh Ready attempt"
            );
        }

        let retained = get_release(&database.pool, ready.release_id)
            .await?
            .context("fresh Ready qBittorrent attempt")?;
        assert_eq!(retained.state, AcquisitionReleaseState::Ready);
        assert!(
            retained
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.get("automaticRetry"))
                .is_none()
        );
        let jobs = list_release_jobs(&database.pool, ready.release_id).await?;
        assert_eq!(jobs[0].state, ReleaseJobState::Ready);
        assert!(jobs[0].active);
        let target =
            crate::acquisition::subscriptions::get_target(&database.pool, target.target_id)
                .await?
                .context("fresh Ready target")?;
        assert_eq!(target.state, AcquisitionTargetState::Submitted);
        assert_eq!(target.download_id.as_deref(), Some(TEST_HASH));
        Ok(())
    }

    #[tokio::test]
    async fn alm7_qbittorrent_aged_ready_provider_failures_retry_without_review()
    -> anyhow::Result<()> {
        for operation in [
            QbittorrentProviderOperation::TorrentInspection,
            QbittorrentProviderOperation::FilePolicyRefresh,
        ] {
            let database = setup_db().await?;
            let resolved = setup_qbittorrent_resolved(&database).await?;
            let now = chrono::Utc::now();
            let started_at =
                now - chrono::Duration::seconds(QBITTORRENT_METADATA_TIMEOUT_SECONDS + 1);
            let (ready, target) =
                setup_ready_anime_qbittorrent_provider_attempt(&database, &resolved, started_at)
                    .await?;

            assert!(
                terminalize_aged_anime_qbittorrent_provider_failure(
                    &database.pool,
                    ready.release_id,
                    TEST_HASH,
                    operation,
                    now,
                )
                .await?
                .is_some(),
                "aged {operation:?} failures must leave Ready through automatic retry"
            );

            let failed = get_release(&database.pool, ready.release_id)
                .await?
                .context("provider-failed anime qBittorrent release")?;
            assert_eq!(failed.state, AcquisitionReleaseState::Failed);
            assert_eq!(
                failed
                    .coverage_plan
                    .as_ref()
                    .and_then(|plan| plan.pointer("/automaticRetry/status"))
                    .and_then(Value::as_str),
                Some("scheduled")
            );
            assert_eq!(
                failed
                    .coverage_plan
                    .as_ref()
                    .and_then(|plan| plan.pointer("/automaticRetry/reason"))
                    .and_then(Value::as_str),
                Some(operation.reason_code())
            );
            assert_eq!(
                failed
                    .coverage_plan
                    .as_ref()
                    .and_then(|plan| plan.pointer("/torrentCleanup/status"))
                    .and_then(Value::as_str),
                Some("pending"),
                "the existing durable cleanup path must own provider-operation retries"
            );
            assert!(
                failed
                    .coverage_plan
                    .as_ref()
                    .and_then(|plan| plan.get("reviewReasons"))
                    .is_none()
            );
            let coverage = list_release_coverage(&database.pool, ready.release_id).await?;
            assert!(
                coverage
                    .iter()
                    .all(|entry| entry.state == ReleaseCoverageState::Rejected)
            );
            let jobs = list_release_jobs(&database.pool, ready.release_id).await?;
            assert_eq!(jobs[0].state, ReleaseJobState::Failed);
            assert!(!jobs[0].active);
            let target =
                crate::acquisition::subscriptions::get_target(&database.pool, target.target_id)
                    .await?
                    .context("automatic provider retry target")?;
            assert_eq!(target.state, AcquisitionTargetState::Pending);
            assert!(target.selected_candidate.is_none());
            assert!(target.selected_route_logical_id.is_none());
            assert!(target.download_id.is_none());
            assert!(target.next_search_after.is_some());
        }
        Ok(())
    }

    #[tokio::test]
    async fn alm7_qbittorrent_aged_staging_inspection_failure_retries_without_review()
    -> anyhow::Result<()> {
        let database = setup_db().await?;
        let resolved = setup_qbittorrent_resolved(&database).await?;
        let (subscription, targets) = setup_subscription_with_targets(
            &database,
            MediaType::Anime,
            "Staging Provider Retry Anime",
            vec![NewAcquisitionTarget {
                target_key: Some("anilist:9201:A0001".to_string()),
                media_type: Some(MediaType::Anime),
                title: Some("Staging Provider Retry Anime".to_string()),
                season_number: Some(1),
                episode_number: Some(1),
                absolute_episode_number: Some(1),
                ..empty_target()
            }],
        )
        .await?;
        let release = setup_staged_qbittorrent_release_for(
            &database,
            &resolved,
            subscription.subscription_id,
            MediaType::Anime,
            "Staging Provider Retry Anime",
            "anime",
            "[Group] Staging Provider Retry Anime - 01 [1080p]",
        )
        .await?;
        upsert_release_coverage(
            &database.pool,
            NewAcquisitionReleaseCoverage {
                coverage_id: None,
                release_id: release.release_id,
                release_file_id: None,
                target_id: targets[0].target_id,
                coverage_kind: ReleaseCoverageKind::SingleEpisode,
                confidence: ReleaseConfidence::High,
                score: Some(1.0),
                reason: Some("submitted anime target".to_string()),
                state: ReleaseCoverageState::Submitted,
                verified_by: Some("alm7_staging_provider_retry_test".to_string()),
            },
        )
        .await?;
        update_target_state(
            &database.pool,
            targets[0].target_id,
            AcquisitionTargetStateUpdate {
                state: AcquisitionTargetState::Submitted,
                state_reason: Some("submitted to qBittorrent".to_string()),
                selected_provider_id: release.source_provider_id,
                selected_route_logical_id: Some(TORRENT_DEFAULT_LOGICAL_ID.to_string()),
                selected_candidate: release.selected_candidate.clone(),
                download_id: Some(TEST_HASH.to_string()),
                ..Default::default()
            },
        )
        .await?;
        let started_at = chrono::Utc::now()
            - chrono::Duration::seconds(QBITTORRENT_METADATA_TIMEOUT_SECONDS + 1);
        sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_release_jobs
             SET started_at = $1,
                 updated_at = CURRENT_TIMESTAMP
             WHERE release_id = $2
               AND LOWER(download_id) = LOWER($3)
               AND active = 1",
        )
        .bind(started_at.format("%Y-%m-%d %H:%M:%S").to_string())
        .bind(release.release_id.to_string())
        .bind(TEST_HASH)
        .execute(&database.pool)
        .await?;

        let (fixture, requests) =
            start_qbittorrent_priority_fixture_with_failures(Vec::new(), true, false).await?;
        configure_qbittorrent_priority_fixture_endpoint(&database, &resolved, fixture).await?;
        let state = setup_state_for_database(&database)?;
        assert_eq!(
            process_stale_qbittorrent_acquisition_releases(&state, 10).await?,
            1
        );
        let failed = get_release(&database.pool, release.release_id)
            .await?
            .context("failed staging provider attempt")?;
        assert_eq!(failed.state, AcquisitionReleaseState::Failed);
        assert_eq!(
            failed
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.pointer("/automaticRetry/reason"))
                .and_then(Value::as_str),
            Some("qbittorrent_torrent_inspection_timeout")
        );
        assert_eq!(
            failed
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.pointer("/torrentCleanup/status"))
                .and_then(Value::as_str),
            Some("completed")
        );
        let coverage = list_release_coverage(&database.pool, release.release_id).await?;
        assert!(
            coverage
                .iter()
                .all(|entry| entry.state == ReleaseCoverageState::Rejected)
        );
        let target =
            crate::acquisition::subscriptions::get_target(&database.pool, targets[0].target_id)
                .await?
                .context("staging provider retry target")?;
        assert_eq!(target.state, AcquisitionTargetState::Pending);
        assert!(target.download_id.is_none());
        let review_count = sqlx::query_scalar::<sqlx::Any, i64>(
            "SELECT COUNT(*) FROM acquisition_releases
             WHERE subscription_id = $1 AND state = 'review_required'",
        )
        .bind(subscription.subscription_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(review_count, 0);
        let captured = requests.lock().expect("provider retry fixture").clone();
        assert!(captured.iter().any(|request| {
            request.pointer("/path").and_then(Value::as_str) == Some("api/v2/torrents/info")
        }));
        assert!(captured.iter().any(|request| {
            request.pointer("/path").and_then(Value::as_str) == Some("api/v2/torrents/delete")
        }));
        Ok(())
    }

    #[tokio::test]
    async fn alm7_qbittorrent_aged_ready_file_list_failure_retries_without_review()
    -> anyhow::Result<()> {
        let database = setup_db().await?;
        let resolved = setup_qbittorrent_resolved(&database).await?;
        let started_at = chrono::Utc::now()
            - chrono::Duration::seconds(QBITTORRENT_METADATA_TIMEOUT_SECONDS + 1);
        let (ready, target) =
            setup_ready_anime_qbittorrent_provider_attempt(&database, &resolved, started_at)
                .await?;
        let (fixture, requests) =
            start_qbittorrent_priority_fixture_with_failures(Vec::new(), false, true).await?;
        configure_qbittorrent_priority_fixture_endpoint(&database, &resolved, fixture).await?;
        let state = setup_state_for_database(&database)?;
        assert_eq!(
            process_stale_qbittorrent_acquisition_releases(&state, 10).await?,
            1
        );
        let failed = get_release(&database.pool, ready.release_id)
            .await?
            .context("failed Ready file-list attempt")?;
        assert_eq!(failed.state, AcquisitionReleaseState::Failed);
        assert_eq!(
            failed
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.pointer("/automaticRetry/reason"))
                .and_then(Value::as_str),
            Some("qbittorrent_file_policy_refresh_timeout")
        );
        assert_eq!(
            failed
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.pointer("/torrentCleanup/status"))
                .and_then(Value::as_str),
            Some("completed")
        );
        let target =
            crate::acquisition::subscriptions::get_target(&database.pool, target.target_id)
                .await?
                .context("Ready file-list retry target")?;
        assert_eq!(target.state, AcquisitionTargetState::Pending);
        assert!(target.download_id.is_none());
        let review_count = sqlx::query_scalar::<sqlx::Any, i64>(
            "SELECT COUNT(*) FROM acquisition_releases
             WHERE subscription_id = $1 AND state = 'review_required'",
        )
        .bind(
            ready
                .subscription_id
                .context("Ready anime subscription")?
                .to_string(),
        )
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(review_count, 0);
        let captured = requests.lock().expect("file-list retry fixture").clone();
        for expected_path in [
            "api/v2/torrents/info",
            "api/v2/torrents/files",
            "api/v2/torrents/delete",
        ] {
            assert!(captured.iter().any(|request| {
                request.pointer("/path").and_then(Value::as_str) == Some(expected_path)
            }));
        }
        Ok(())
    }

    #[tokio::test]
    async fn qbittorrent_stale_policy_classifies_metadata_timeout() -> anyhow::Result<()> {
        let database = setup_db().await?;
        let resolved = setup_qbittorrent_resolved(&database).await?;
        let (subscription, _targets) = setup_series_subscription(&database, 1..=1).await?;
        let mut release = setup_staged_qbittorrent_release(
            &database,
            &resolved,
            subscription.subscription_id,
            "Show.S01E01.1080p.WEB-DL-GROUP",
        )
        .await?;
        let now = chrono::Utc::now();
        release.created_at =
            now - chrono::Duration::seconds(QBITTORRENT_METADATA_TIMEOUT_SECONDS + 1);
        let files = list_release_files(&database.pool, release.release_id).await?;
        let torrent_info = json!({
            "hash": TEST_HASH,
            "state": "metaDL",
            "progress": 0.0,
            "dlspeed": 0
        });

        let decision =
            qbittorrent_stale_release_decision(now, &release, &files, &torrent_info, None)
                .expect("metadata timeout");
        assert_eq!(decision.kind, QbittorrentStaleReleaseKind::MetadataTimeout);
        assert_eq!(decision.reason_code, "qbittorrent_metadata_timeout");

        let fresh_attempt =
            now - chrono::Duration::seconds(QBITTORRENT_METADATA_TIMEOUT_SECONDS - 1);
        assert!(
            qbittorrent_stale_release_decision(
                now,
                &release,
                &files,
                &torrent_info,
                Some(fresh_attempt),
            )
            .is_none(),
            "a new job attempt must not inherit the reused release row's age"
        );

        release.created_at =
            now - chrono::Duration::seconds(QBITTORRENT_METADATA_TIMEOUT_SECONDS - 1);
        assert!(
            qbittorrent_stale_release_decision(now, &release, &files, &torrent_info, None)
                .is_none(),
            "fresh metadata waits should not be failed"
        );
        Ok(())
    }

    #[tokio::test]
    async fn alm7_qbittorrent_late_runtime_evidence_cannot_regress_a_newer_state()
    -> anyhow::Result<()> {
        let database = setup_db().await?;
        let resolved = setup_qbittorrent_resolved(&database).await?;
        let (subscription, _targets) = setup_series_subscription(&database, 1..=1).await?;
        let stale = setup_staged_qbittorrent_release(
            &database,
            &resolved,
            subscription.subscription_id,
            "Show.S01E01.1080p.WEB-DL-GROUP",
        )
        .await?;
        sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_releases
             SET state = 'downloading',
                 state_reason = 'newer attempt is downloading',
                 coverage_plan_json = $1,
                 updated_at = CURRENT_TIMESTAMP
             WHERE release_id = $2",
        )
        .bind(serde_json::to_string(&json!({ "newerAttempt": true }))?)
        .bind(stale.release_id.to_string())
        .execute(&database.pool)
        .await?;
        let files = Vec::new();
        let torrent_info = json!({
            "hash": TEST_HASH,
            "state": "metaDL",
            "progress": 0.0,
            "dlspeed": 0
        });
        let runtime_state = qbittorrent_runtime_state(&stale, &files, &torrent_info);
        let evidence = qbittorrent_torrent_evidence(
            &resolved.record,
            &stale,
            &files,
            &torrent_info,
            &runtime_state,
        );
        assert!(
            record_qbittorrent_runtime_evidence(&database.pool, &stale, &evidence)
                .await
                .is_err(),
            "the stale Staging snapshot must lose its conditional update"
        );
        let current = get_release(&database.pool, stale.release_id)
            .await?
            .context("current qBittorrent release")?;
        assert_eq!(current.state, AcquisitionReleaseState::Downloading);
        assert_eq!(
            current
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.get("newerAttempt"))
                .and_then(Value::as_bool),
            Some(true)
        );
        Ok(())
    }

    #[tokio::test]
    async fn alm7_qbittorrent_terminal_release_reconciles_an_active_job() -> anyhow::Result<()> {
        let database = setup_db().await?;
        let resolved = setup_qbittorrent_resolved(&database).await?;
        let (subscription, _targets) = setup_series_subscription(&database, 1..=1).await?;
        let release = setup_staged_qbittorrent_release(
            &database,
            &resolved,
            subscription.subscription_id,
            "Show.S01E01.1080p.WEB-DL-GROUP",
        )
        .await?;
        sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_releases
             SET state = 'completed', state_reason = 'simulated crash after release commit'
             WHERE release_id = $1",
        )
        .bind(release.release_id.to_string())
        .execute(&database.pool)
        .await?;
        assert_eq!(
            reconcile_qbittorrent_terminal_release_jobs(&database.pool, 10).await?,
            1
        );
        let jobs = list_release_jobs(&database.pool, release.release_id).await?;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].state, ReleaseJobState::Completed);
        assert!(!jobs[0].active);
        assert!(jobs[0].completed_at.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn qbittorrent_stale_policy_classifies_zero_seed_stall() -> anyhow::Result<()> {
        let database = setup_db().await?;
        let resolved = setup_qbittorrent_resolved(&database).await?;
        let (subscription, _targets) = setup_series_subscription(&database, 1..=1).await?;
        let mut release = setup_staged_qbittorrent_release(
            &database,
            &resolved,
            subscription.subscription_id,
            "Show.S01E01.1080p.WEB-DL-GROUP",
        )
        .await?;
        let now = chrono::Utc::now();
        release.created_at =
            now - chrono::Duration::seconds(QBITTORRENT_ZERO_SEED_STALL_TIMEOUT_SECONDS + 1);
        let files = vec![AcquisitionReleaseFile {
            release_file_id: Uuid::new_v4(),
            release_id: release.release_id,
            file_index: Some(0),
            file_id: Some("0".to_string()),
            provider_file_id: Some("0".to_string()),
            path: "Show.S01E01.1080p.WEB-DL-GROUP.mkv".to_string(),
            basename: "Show.S01E01.1080p.WEB-DL-GROUP.mkv".to_string(),
            size_bytes: Some(1_000_000_000),
            selectable: true,
            selected: Some(true),
            parsed_title: None,
            parsed_season_number: Some(1),
            parsed_episode_number: Some(1),
            parsed_episode_end_number: Some(1),
            parsed_absolute_episode_number: None,
            parsed_absolute_episode_end_number: None,
            parsed_air_date: None,
            parsed_quality: Some("1080p".to_string()),
            parsed_language: None,
            parsed_release_group: None,
            parser_confidence: ReleaseConfidence::High,
            parser_reason: None,
            raw: None,
            provider_metadata: None,
            created_at: now,
            updated_at: now,
        }];
        let torrent_info = json!({
            "hash": TEST_HASH,
            "state": "stalledDL",
            "progress": 0.25,
            "dlspeed": 0,
            "num_seeds": 0,
            "num_complete": 0,
            "availability": 0.0,
            "amount_left": 750_000_000_u64
        });

        let decision =
            qbittorrent_stale_release_decision(now, &release, &files, &torrent_info, None)
                .expect("zero-seed stall");
        assert_eq!(decision.kind, QbittorrentStaleReleaseKind::ZeroSeedStall);
        assert_eq!(decision.reason_code, "qbittorrent_zero_seed_stall");

        let healthy_swarm = json!({
            "hash": TEST_HASH,
            "state": "stalledDL",
            "progress": 0.25,
            "dlspeed": 0,
            "num_seeds": 2,
            "num_complete": 1,
            "availability": 1.0
        });
        assert!(
            qbittorrent_stale_release_decision(now, &release, &files, &healthy_swarm, None)
                .is_none(),
            "a stalled qB state with complete seed evidence should remain eligible"
        );
        Ok(())
    }

    #[tokio::test]
    async fn qbittorrent_stale_release_is_failed_and_targets_retry_next_candidate()
    -> anyhow::Result<()> {
        let database = setup_db().await?;
        let resolved = setup_qbittorrent_resolved(&database).await?;
        let (subscription, targets) = setup_series_subscription(&database, 1..=1).await?;
        let release = setup_staged_qbittorrent_release(
            &database,
            &resolved,
            subscription.subscription_id,
            "Show.S01E01.1080p.WEB-DL-GROUP",
        )
        .await?;
        upsert_release_coverage(
            &database.pool,
            NewAcquisitionReleaseCoverage {
                coverage_id: None,
                release_id: release.release_id,
                release_file_id: None,
                target_id: targets[0].target_id,
                coverage_kind: ReleaseCoverageKind::SingleEpisode,
                confidence: ReleaseConfidence::High,
                score: Some(1.0),
                reason: Some("selected release".to_string()),
                state: ReleaseCoverageState::Submitted,
                verified_by: Some("test".to_string()),
            },
        )
        .await?;
        update_target_state(
            &database.pool,
            targets[0].target_id,
            AcquisitionTargetStateUpdate {
                state: AcquisitionTargetState::Submitted,
                state_reason: Some("submitted".to_string()),
                selected_provider_id: release.source_provider_id,
                selected_route_logical_id: release.selected_route_logical_id.clone(),
                selected_candidate: release.selected_candidate.clone(),
                download_id: release.download_id.clone(),
                ..Default::default()
            },
        )
        .await?;
        let now = chrono::Utc::now();
        let decision =
            QbittorrentStaleReleaseDecision::metadata_timeout(QBITTORRENT_METADATA_TIMEOUT_SECONDS);
        let torrent_info = json!({
            "hash": TEST_HASH,
            "state": "metaDL",
            "progress": 0.0,
            "dlspeed": 0
        });

        let reset = mark_qbittorrent_release_stale_for_retry(
            &database.pool,
            &release,
            TEST_HASH,
            &torrent_info,
            decision,
            now,
        )
        .await?;
        assert_eq!(reset, 1);

        let failed = get_release(&database.pool, release.release_id)
            .await?
            .expect("release");
        assert_eq!(failed.state, AcquisitionReleaseState::Failed);
        assert_eq!(
            failed
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.get("retrySuppression"))
                .and_then(|value| value.get("suppressAutomaticRediscovery"))
                .and_then(Value::as_bool),
            Some(true)
        );
        let jobs = list_release_jobs(&database.pool, release.release_id).await?;
        assert_eq!(jobs[0].state, ReleaseJobState::Failed);
        assert!(!jobs[0].active);
        let coverage = list_release_coverage(&database.pool, release.release_id).await?;
        assert_eq!(coverage[0].state, ReleaseCoverageState::Rejected);

        let target =
            crate::acquisition::subscriptions::get_target(&database.pool, targets[0].target_id)
                .await?
                .expect("target");
        assert_eq!(target.state, AcquisitionTargetState::Pending);
        assert!(target.selected_candidate.is_none());
        assert!(target.selected_route_logical_id.is_none());
        assert!(target.download_id.is_none());
        assert!(target.next_search_after.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn qbittorrent_high_confidence_policy_can_transition_to_resumed() -> anyhow::Result<()> {
        let database = setup_db().await?;
        let resolved = setup_qbittorrent_resolved(&database).await?;
        let (subscription, targets) = setup_series_subscription(&database, 1..=1).await?;
        let release = setup_staged_qbittorrent_release(
            &database,
            &resolved,
            subscription.subscription_id,
            "Show.S01E01.1080p.WEB-DL-GROUP",
        )
        .await?;
        let rows = vec![json!({
            "index": 0,
            "name": "Show.S01E01.1080p.WEB-DL-GROUP.mkv",
            "size": 1_500_000_000u64,
            "priority": 1,
            "progress": 0.0
        })];
        persist_qbittorrent_release_file_rows(&database.pool, &release, TEST_HASH, &rows).await?;
        let files = list_release_files(&database.pool, release.release_id).await?;
        let refinement =
            refine_tv_qbittorrent_coverage(&database.pool, &release, TEST_HASH, &targets, &files)
                .await?;
        let coverage = list_release_coverage(&database.pool, release.release_id).await?;
        let decision = decide_qbittorrent_file_priority(&release, &refinement, &files, &coverage);
        assert!(decision.is_approved());
        persist_qbittorrent_priority_decision(
            &database.pool,
            &release,
            &files,
            &coverage,
            &refinement,
            &decision,
            true,
        )
        .await?;

        let ready_release = get_release(&database.pool, release.release_id)
            .await?
            .expect("ready release");
        assert_eq!(ready_release.state, AcquisitionReleaseState::Ready);
        mark_qbittorrent_release_resumed(&database.pool, &ready_release, TEST_HASH).await?;
        let resumed = get_release(&database.pool, release.release_id)
            .await?
            .expect("resumed release");
        assert_eq!(resumed.state, AcquisitionReleaseState::Downloading);
        let jobs = list_release_jobs(&database.pool, release.release_id).await?;
        assert_eq!(jobs[0].state, ReleaseJobState::Downloading);
        Ok(())
    }

    #[tokio::test]
    async fn qbittorrent_user_approved_override_preserves_explicit_file_selection()
    -> anyhow::Result<()> {
        let database = setup_db().await?;
        let resolved = setup_qbittorrent_resolved(&database).await?;
        let (subscription, targets) = setup_series_subscription(&database, 1..=1).await?;
        let release = setup_staged_qbittorrent_release(
            &database,
            &resolved,
            subscription.subscription_id,
            "Show.S01.COMPLETE.1080p.WEB-DL-GROUP",
        )
        .await?;
        let rows = vec![
            json!({ "index": 0, "name": "Show/Season 01/Show.S01E01.1080p.mkv", "size": 1_500_000_000u64, "priority": 1, "progress": 0.0 }),
            json!({ "index": 1, "name": "Show/Season 01/Bonus Feature.mkv", "size": 400_000_000u64, "priority": 1, "progress": 0.0 }),
        ];
        persist_qbittorrent_release_file_rows(&database.pool, &release, TEST_HASH, &rows).await?;
        let files = list_release_files(&database.pool, release.release_id).await?;
        let refinement =
            refine_tv_qbittorrent_coverage(&database.pool, &release, TEST_HASH, &targets, &files)
                .await?;
        let coverage = list_release_coverage(&database.pool, release.release_id).await?;
        let fallback = decide_qbittorrent_file_priority(&release, &refinement, &files, &coverage);
        assert!(!fallback.is_approved());

        update_qbittorrent_release_state_only(
            &database.pool,
            release.release_id,
            TEST_HASH,
            AcquisitionReleaseState::Staging,
            AcquisitionReleaseState::ReviewRequired,
            "manual review approved selected files",
            json!({
                "priorityPolicy": {
                    "policyVersion": QBITTORRENT_SELECTION_POLICY_VERSION,
                    "status": "approved",
                    "priorityApplied": false,
                    "selectedFileIds": ["0"],
                    "skippedFileIds": ["1"],
                    "wantedPriority": QBITTORRENT_WANTED_FILE_PRIORITY,
                    "skippedPriority": QBITTORRENT_SKIPPED_FILE_PRIORITY,
                    "coverageFingerprint": "sha256:user-approved-test",
                    "reviewReasons": [],
                    "userApproved": true
                }
            }),
        )
        .await?;
        let approved_release = get_release(&database.pool, release.release_id)
            .await?
            .expect("approved release");
        let approved =
            approved_qbittorrent_user_override(&approved_release, &fallback).expect("override");
        assert!(approved.is_approved());
        assert!(approved.user_approved);
        assert_eq!(approved.selected_file_ids, vec!["0"]);
        assert_eq!(approved.skipped_file_ids, vec!["1"]);
        assert!(approved.review_reasons.is_empty());

        let merged = merge_qbittorrent_policy_evidence(json!({}), &approved, true);
        assert_eq!(
            merged
                .get("priorityPolicy")
                .and_then(|value| value.get("userApproved"))
                .and_then(Value::as_bool),
            Some(true)
        );
        Ok(())
    }

    #[tokio::test]
    async fn qbittorrent_cancelled_staged_release_records_conservative_runtime()
    -> anyhow::Result<()> {
        let database = setup_db().await?;
        let resolved = setup_qbittorrent_resolved(&database).await?;
        let (subscription, _targets) = setup_series_subscription(&database, 1..=1).await?;
        let release = setup_staged_qbittorrent_release(
            &database,
            &resolved,
            subscription.subscription_id,
            "Show.S01E01.1080p.WEB-DL-GROUP",
        )
        .await?;

        mark_qbittorrent_release_cancelled(&database.pool, &release, TEST_HASH, false).await?;
        let cancelled = get_release(&database.pool, release.release_id)
            .await?
            .expect("cancelled release");
        assert_eq!(cancelled.state, AcquisitionReleaseState::Cancelled);
        assert_eq!(
            cancelled
                .coverage_plan
                .as_ref()
                .and_then(|value| value.get("torrentRuntime"))
                .and_then(|value| value.get("deleteFiles"))
                .and_then(Value::as_bool),
            Some(false)
        );
        let jobs = list_release_jobs(&database.pool, release.release_id).await?;
        assert_eq!(jobs[0].state, ReleaseJobState::Cancelled);
        assert!(!jobs[0].active);
        Ok(())
    }

    #[test]
    fn resolves_nzbget_group_id_by_id_or_name() {
        let groups = vec![
            json!({ "NZBID": 4, "NZBName": "One" }),
            json!({ "NZBID": 9, "NZBName": "Two" }),
        ];
        assert_eq!(resolve_nzbget_group_id(&groups, "9"), Some(9));
        assert_eq!(resolve_nzbget_group_id(&groups, "two"), Some(9));
        assert_eq!(resolve_nzbget_group_id(&groups, "missing"), None);
    }

    #[test]
    fn maps_progress_fraction_from_nzbget_sizes() {
        assert_eq!(progress_fraction(Some(50), Some(100)), Some(0.5));
        assert_eq!(progress_fraction(Some(150), Some(100)), Some(1.0));
        assert_eq!(progress_fraction(Some(1), Some(0)), None);
    }

    #[test]
    fn serializes_debrid_progress_evidence() {
        let item = DownloadBrokerProgressItem {
            id: "job-1".to_string(),
            name: Some("Show.S01.PACK".to_string()),
            state: Some("review_required".to_string()),
            category: Some("series".to_string()),
            local_path: None,
            progress: Some(0.0),
            downloaded_bytes: Some(0),
            total_bytes: Some(1024),
            remaining_bytes: Some(1024),
            download_rate_bps: None,
            upload_rate_bps: None,
            debrid: Some(DownloadBrokerDebridEvidence {
                provider_name: Some("Real-Debrid".to_string()),
                provider_implementation: Some("real_debrid".to_string()),
                provider_capabilities: Some(json!({
                    "supportsFileSelection": false,
                    "fileSelectionMode": "unsupported"
                })),
                provider_status: Some(json!({
                    "providerImplementation": "real_debrid",
                    "status": "review_required"
                })),
                remote_status: Some("review_required".to_string()),
                selection_mode: Some("unsupported".to_string()),
                selected_file_count: 0,
                skipped_file_count: 2,
                review_reasons: vec!["file_selection_unsupported".to_string()],
                failure_class: None,
                last_error: None,
                fallback_state: "not_attempted_review_required".to_string(),
            }),
            torrent: None,
            stream: None,
        };

        let value = serde_json::to_value(&item).unwrap();
        let debrid = value.get("debrid").unwrap();
        assert_eq!(
            debrid.get("providerName").and_then(Value::as_str),
            Some("Real-Debrid")
        );
        assert_eq!(
            debrid
                .get("providerStatus")
                .and_then(|status| status.get("status"))
                .and_then(Value::as_str),
            Some("review_required")
        );
        assert_eq!(
            debrid
                .get("reviewReasons")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(1)
        );
        assert_eq!(
            debrid.get("fallbackState").and_then(Value::as_str),
            Some("not_attempted_review_required")
        );
    }

    #[test]
    fn serializes_qbittorrent_progress_evidence() {
        let item = DownloadBrokerProgressItem {
            id: TEST_HASH.to_string(),
            name: Some("Show.S01.COMPLETE".to_string()),
            state: Some("pausedDL".to_string()),
            category: Some("series".to_string()),
            local_path: Some("/downloads/Show".to_string()),
            progress: Some(0.0),
            downloaded_bytes: Some(0),
            total_bytes: Some(1024),
            remaining_bytes: Some(1024),
            download_rate_bps: None,
            upload_rate_bps: None,
            debrid: None,
            torrent: Some(DownloadBrokerTorrentEvidence {
                provider_name: Some("qBittorrent".to_string()),
                provider_implementation: Some("qbittorrent".to_string()),
                torrent_hash: TEST_HASH.to_string(),
                runtime_state: "review_required".to_string(),
                metadata_state: "files_available".to_string(),
                priority_state: "review_required".to_string(),
                selected_file_count: 0,
                skipped_file_count: 2,
                review_reasons: vec!["unmapped_media_file".to_string()],
                policy_version: Some(QBITTORRENT_SELECTION_POLICY_VERSION.to_string()),
                coverage_fingerprint: Some("sha256:test".to_string()),
                route_owner_id: Some("default".to_string()),
                route_logical_id: Some(TORRENT_DEFAULT_LOGICAL_ID.to_string()),
                category: Some("series".to_string()),
                source_extension_id: "elixir.marketplace.torrentio".to_string(),
                source_provider_id: None,
                candidate_title: Some("Show.S01.COMPLETE".to_string()),
                priority_applied: false,
                user_approved: false,
                blocker: None,
                failure_state: None,
            }),
            stream: None,
        };

        let value = serde_json::to_value(&item).unwrap();
        let torrent = value.get("torrent").unwrap();
        assert_eq!(
            torrent.get("runtimeState").and_then(Value::as_str),
            Some("review_required")
        );
        assert_eq!(
            torrent
                .get("reviewReasons")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(1)
        );
        assert!(value.get("debrid").is_none());
    }
}
