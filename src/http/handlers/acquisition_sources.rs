use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    net::IpAddr,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use axum::{
    Json,
    extract::{Query, State},
};
use reqwest::{
    Url,
    header::{HeaderName, HeaderValue},
};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value, json};
use tokio::task::JoinSet;
use uuid::Uuid;

use crate::{
    acquisition::language_policy::{
        AcquisitionLanguagePreference, CandidateLanguageEvidence,
        LanguagePreferenceAssessmentState, add_language_evidence_text, add_language_evidence_value,
        add_subtitle_language_evidence_value, assess_language_preference,
        language_preference_from_quality_profile, normalize_language_value,
    },
    db::models::{Extension, ExtensionInstance, Provider, ProviderHealthState},
    download_broker::{
        DEBRID_DEFAULT_LOGICAL_ID, DownloadBrokerRouteRecord, HTTP_STREAM_DEFAULT_LOGICAL_ID,
        TORRENT_DEFAULT_LOGICAL_ID, list_acquisition_routes,
    },
    extensions::{
        ExternalIds,
        cloudstream_registry::CLOUDSTREAM_COMPAT_EXTENSION_ID,
        manifest::ExtensionManifest,
        nuvio_registry::is_prism_extension_id,
        store::{
            ExtensionSourceModule, ExtensionSourceModuleVersion, ExtensionSourceRegistry,
            ExtensionStore,
        },
    },
    http::{
        auth::CurrentUser,
        error::{ApiError, ApiResult},
        handlers::extensions::resolve_control_provider_transport_base_url,
    },
    orchestrator::model::ProviderEndpoint,
    state::AppState,
};

pub const ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY: &str = "acquisition.candidate_provider";
pub const ACQUISITION_STREAM_CANDIDATE_PROVIDER_CAPABILITY: &str =
    "acquisition.stream_candidate_provider";

const CANDIDATE_PROVIDER_SCHEMA_VERSION: u32 = 1;
const CANDIDATE_PROVIDER_SEARCH_PATH: &str = "search";
const CANDIDATE_PROVIDER_TIMEOUT_SECONDS: u64 = 30;
const STREAM_CANDIDATE_DEFAULT_LIMIT: u32 = 25;
const STREAM_CANDIDATE_MAX_LIMIT: u32 = 100;
const STREAM_CANDIDATE_MAX_TARGETS: usize = 100;
const STREAM_CANDIDATE_MAX_TITLE_VARIANTS: usize = 32;
const STREAM_CANDIDATE_PROVIDER_RESPONSE_MAX_BYTES: u64 = 2 * 1024 * 1024;
const STREAM_CANDIDATE_MAX_BYTES: usize = 256 * 1024;
const STREAM_CANDIDATE_RAW_MAX_BYTES: usize = 64 * 1024;
const STREAM_CANDIDATE_MAX_HEADERS: usize = 64;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CandidateProvidersQuery {
    #[serde(default)]
    pub media_type: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CandidateProvidersResponse {
    pub providers: Vec<CandidateProviderSummary>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CandidateProviderSummary {
    pub provider_id: Uuid,
    pub extension_id: String,
    pub extension_name: String,
    pub instance_id: Uuid,
    pub instance_name: String,
    pub capability: String,
    pub implementation: Option<String>,
    pub health_state: ProviderHealthState,
    pub media_types: Vec<String>,
    pub actions: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CandidateSearchRequest {
    #[serde(default)]
    pub provider_id: Option<Uuid>,
    pub media_type: String,
    pub title: String,
    #[serde(default)]
    pub year: Option<i32>,
    #[serde(default)]
    pub external_ids: Option<ExternalIds>,
    #[serde(default)]
    pub target: Option<CandidateSearchTarget>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search_intent: Option<CandidateSearchIntent>,
    #[serde(default)]
    pub preferences: CandidateSearchPreferences,
    #[serde(default)]
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CandidateSearchPreferences {
    #[serde(default)]
    pub route_policy: Option<String>,
    #[serde(default)]
    pub allowed_qualities: Vec<String>,
    #[serde(default)]
    pub max_size_bytes: Option<u64>,
    #[serde(default)]
    pub required_languages: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language_preference: Option<AcquisitionLanguagePreference>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CandidateSearchTarget {
    #[serde(default)]
    pub target_key: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub season_number: Option<i32>,
    #[serde(default)]
    pub episode_number: Option<i32>,
    #[serde(default)]
    pub absolute_episode_number: Option<i32>,
    #[serde(default)]
    pub air_date: Option<String>,
    #[serde(default)]
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CandidateSearchIntent {
    pub kind: String,
    #[serde(default)]
    pub season_number: Option<i32>,
    #[serde(default)]
    pub episode_start: Option<i32>,
    #[serde(default)]
    pub episode_end: Option<i32>,
    #[serde(default)]
    pub absolute_episode_start: Option<i32>,
    #[serde(default)]
    pub absolute_episode_end: Option<i32>,
    #[serde(default)]
    pub target_count: u32,
    #[serde(default)]
    pub target_keys: Vec<String>,
    #[serde(default)]
    pub retry_bucket: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamCandidateSearchRequest {
    #[serde(default)]
    pub provider_id: Option<Uuid>,
    pub media_type: String,
    pub title: String,
    #[serde(default)]
    pub year: Option<i32>,
    #[serde(default)]
    pub external_ids: Option<ExternalIds>,
    #[serde(default)]
    pub titles: Vec<StreamTitleVariant>,
    #[serde(default)]
    pub targets: Vec<StreamSearchTarget>,
    #[serde(default)]
    pub preferences: StreamSearchPreferences,
    #[serde(default)]
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamTitleVariant {
    pub value: String,
    pub kind: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamSearchTarget {
    #[serde(default)]
    pub target_key: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub season_number: Option<i32>,
    #[serde(default)]
    pub episode_number: Option<i32>,
    #[serde(default)]
    pub absolute_episode_number: Option<i32>,
    #[serde(default)]
    pub air_date: Option<String>,
    #[serde(default)]
    pub runtime_seconds: Option<u32>,
    #[serde(default)]
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamSearchPreferences {
    #[serde(default)]
    pub allowed_qualities: Vec<String>,
    #[serde(default)]
    pub required_languages: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language_preference: Option<AcquisitionLanguagePreference>,
    #[serde(default)]
    pub language_profiles: Vec<String>,
    #[serde(default)]
    pub subtitle_mode: Option<String>,
    #[serde(default)]
    pub max_size_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CandidateSearchResponse {
    pub schema_version: u32,
    pub provider: CandidateProviderSummary,
    pub route_options: Vec<CandidateRouteOption>,
    pub candidates: Vec<AcquisitionCandidate>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamCandidateSearchResponse {
    pub schema_version: u32,
    pub provider: CandidateProviderSummary,
    pub candidates: Vec<Value>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CandidateRouteOption {
    pub logical_id: String,
    pub label: String,
    pub available: bool,
    pub selected_provider_id: Option<Uuid>,
    pub selected_extension_id: Option<String>,
    pub blocker: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AcquisitionCandidate {
    #[serde(default)]
    pub id: Option<String>,
    pub title: String,
    pub source: String,
    pub source_kind: String,
    #[serde(default)]
    pub info_hash: Option<String>,
    #[serde(default)]
    pub file_index: Option<i64>,
    #[serde(default)]
    pub quality: Option<String>,
    #[serde(default)]
    pub size_bytes: Option<u64>,
    #[serde(default)]
    pub seeders: Option<u32>,
    #[serde(default)]
    pub language: Option<String>,
    #[serde(default)]
    pub cached_debrid: Option<bool>,
    #[serde(default)]
    pub rank: Option<u32>,
    #[serde(default)]
    pub score: Option<f64>,
    #[serde(default)]
    pub score_badges: Vec<CandidateScoreBadge>,
    #[serde(default)]
    pub files: Vec<AcquisitionCandidateFile>,
    #[serde(default)]
    pub supported_routes: Vec<String>,
    #[serde(default)]
    pub default_route: Option<String>,
    #[serde(default)]
    pub raw: Option<Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AcquisitionCandidateFile {
    #[serde(default, alias = "id")]
    pub file_id: Option<String>,
    #[serde(default)]
    pub file_index: Option<i64>,
    pub path: String,
    #[serde(default)]
    pub size_bytes: Option<u64>,
    #[serde(default)]
    pub selectable: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CandidateScoreBadge {
    pub label: String,
    #[serde(default)]
    pub detail: Option<String>,
    #[serde(default)]
    pub score: Option<f64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CandidateProviderInvocation<'a> {
    schema_version: u32,
    request: &'a CandidateSearchRequest,
    provider: CandidateProviderInvocationContext<'a>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StreamCandidateProviderInvocation<'a> {
    schema_version: u32,
    request: &'a StreamCandidateSearchRequest,
    provider: CandidateProviderInvocationContext<'a>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CandidateProviderInvocationContext<'a> {
    provider_id: Uuid,
    extension_id: &'a str,
    instance_id: Uuid,
    implementation: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    config: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CandidateProviderUpstreamResponse {
    #[serde(default)]
    candidates: Vec<Value>,
    #[serde(default)]
    warnings: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StreamCandidateProviderUpstreamResponse {
    #[serde(default)]
    candidates: Vec<Value>,
    #[serde(default)]
    warnings: Vec<String>,
}

#[derive(Debug, Clone)]
struct CandidateProviderSelection {
    summary: CandidateProviderSummary,
    provider: Provider,
    extension: Extension,
    instance: ExtensionInstance,
}

pub(crate) fn is_extension_suite_source_provider_capability(capability: &str) -> bool {
    capability.eq_ignore_ascii_case(ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY)
        || capability.eq_ignore_ascii_case(ACQUISITION_STREAM_CANDIDATE_PROVIDER_CAPABILITY)
}

pub async fn list_candidate_providers(
    _user: CurrentUser,
    State(state): State<AppState>,
    Query(query): Query<CandidateProvidersQuery>,
) -> ApiResult<Json<CandidateProvidersResponse>> {
    let store = ExtensionStore::new(&state.db_pool);
    let providers = available_candidate_providers(&store, query.media_type.as_deref())
        .await
        .map_err(ApiError::from)?;
    Ok(Json(CandidateProvidersResponse {
        providers: providers.into_iter().map(|item| item.summary).collect(),
    }))
}

pub async fn search_candidates(
    _user: CurrentUser,
    State(state): State<AppState>,
    Json(request): Json<CandidateSearchRequest>,
) -> ApiResult<Json<CandidateSearchResponse>> {
    validate_candidate_search_request(&request)
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let response = search_candidates_with_store(&state.db_pool, request)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(response))
}

pub(crate) async fn search_candidates_with_store(
    pool: &sqlx::AnyPool,
    request: CandidateSearchRequest,
) -> Result<CandidateSearchResponse> {
    let request = normalize_candidate_search_request(request)?;
    let store = ExtensionStore::new(pool);
    let provider =
        select_candidate_provider(&store, request.provider_id, Some(&request.media_type)).await?;
    let route_options =
        candidate_route_options(pool, &store, &provider.extension.extension_id).await?;
    let upstream = invoke_candidate_provider(&store, &provider, &request).await?;
    candidate_search_response_from_upstream(provider.summary, route_options, upstream, &request)
}

pub(crate) async fn search_candidate_suite_with_store(
    pool: &sqlx::AnyPool,
    request: CandidateSearchRequest,
) -> Result<CandidateSearchResponse> {
    let request = normalize_candidate_search_request(request)?;
    let store = ExtensionStore::new(pool);
    let providers = available_candidate_providers(&store, Some(&request.media_type)).await?;
    search_candidate_suite_with_providers(pool, request, providers, None).await
}

#[allow(dead_code)]
pub(crate) async fn search_stream_candidate_suite_with_store(
    pool: &sqlx::AnyPool,
    request: StreamCandidateSearchRequest,
) -> Result<StreamCandidateSearchResponse> {
    let request = normalize_stream_candidate_search_request(request)?;
    let store = ExtensionStore::new(pool);
    let providers = available_stream_candidate_providers(&store, Some(&request.media_type)).await?;
    search_stream_candidate_suite_with_providers(pool, request, providers, None).await
}

#[cfg(test)]
pub(crate) async fn search_candidates_with_store_at_base_url(
    pool: &sqlx::AnyPool,
    request: CandidateSearchRequest,
    base_url: &str,
) -> Result<CandidateSearchResponse> {
    let request = normalize_candidate_search_request(request)?;
    let store = ExtensionStore::new(pool);
    let provider =
        select_candidate_provider(&store, request.provider_id, Some(&request.media_type)).await?;
    let route_options =
        candidate_route_options(pool, &store, &provider.extension.extension_id).await?;
    let upstream =
        invoke_candidate_provider_at_base_url(&store, base_url, &provider, &request).await?;
    candidate_search_response_from_upstream(provider.summary, route_options, upstream, &request)
}

#[cfg(test)]
pub(crate) async fn search_candidate_suite_with_store_at_base_urls(
    pool: &sqlx::AnyPool,
    request: CandidateSearchRequest,
    base_urls: std::collections::HashMap<Uuid, String>,
) -> Result<CandidateSearchResponse> {
    let request = normalize_candidate_search_request(request)?;
    let store = ExtensionStore::new(pool);
    let providers = available_candidate_providers(&store, Some(&request.media_type)).await?;
    search_candidate_suite_with_providers(pool, request, providers, Some(base_urls)).await
}

#[cfg(test)]
pub(crate) async fn search_stream_candidate_suite_with_store_at_base_urls(
    pool: &sqlx::AnyPool,
    request: StreamCandidateSearchRequest,
    base_urls: std::collections::HashMap<Uuid, String>,
) -> Result<StreamCandidateSearchResponse> {
    let request = normalize_stream_candidate_search_request(request)?;
    let store = ExtensionStore::new(pool);
    let providers = available_stream_candidate_providers(&store, Some(&request.media_type)).await?;
    search_stream_candidate_suite_with_providers(pool, request, providers, Some(base_urls)).await
}

async fn search_candidate_suite_with_providers(
    pool: &sqlx::AnyPool,
    request: CandidateSearchRequest,
    providers: Vec<CandidateProviderSelection>,
    #[cfg_attr(not(test), allow(unused_variables))] test_base_urls: Option<
        std::collections::HashMap<Uuid, String>,
    >,
) -> Result<CandidateSearchResponse> {
    if providers.is_empty() {
        return Ok(extension_suite_response(
            &request.media_type,
            Vec::new(),
            Vec::new(),
            vec![format!(
                "extension_suite:no_provider: no eligible acquisition candidate providers are available for {}",
                request.media_type
            )],
        ));
    }

    let mut tasks = JoinSet::new();
    for (index, selected) in providers.into_iter().enumerate() {
        let pool = pool.clone();
        let mut provider_request = request.clone();
        provider_request.provider_id = Some(selected.summary.provider_id);
        #[cfg(test)]
        let base_url = test_base_urls
            .as_ref()
            .and_then(|urls| urls.get(&selected.summary.provider_id).cloned());

        tasks.spawn(async move {
            let store = ExtensionStore::new(&pool);
            let route_options =
                candidate_route_options(&pool, &store, &selected.extension.extension_id).await?;
            let upstream = {
                #[cfg(test)]
                {
                    if let Some(base_url) = base_url {
                        invoke_candidate_provider_at_base_url(
                            &store,
                            &base_url,
                            &selected,
                            &provider_request,
                        )
                        .await?
                    } else {
                        invoke_candidate_provider(&store, &selected, &provider_request).await?
                    }
                }
                #[cfg(not(test))]
                {
                    invoke_candidate_provider(&store, &selected, &provider_request).await?
                }
            };
            let mut response = candidate_search_response_from_upstream(
                selected.summary.clone(),
                route_options,
                upstream,
                &provider_request,
            )?;
            apply_candidate_result_cap(&mut response, provider_request.limit);
            let provider = response.provider.clone();
            let route_options = response.route_options.clone();
            let warnings = response.warnings.clone();
            for candidate in &mut response.candidates {
                attach_extension_suite_provider_evidence(
                    candidate,
                    &provider,
                    &route_options,
                    &warnings,
                );
            }
            Ok::<_, anyhow::Error>((index, response))
        });
    }

    let mut successes = Vec::new();
    let mut warnings = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(Ok(success)) => successes.push(success),
            Ok(Err(err)) => warnings.push(format!("extension_suite:provider_failed: {err}")),
            Err(err) => warnings.push(format!(
                "extension_suite:provider_failed: task failed: {err}"
            )),
        }
    }
    successes.sort_by_key(|(index, _)| *index);

    let mut candidates = Vec::new();
    let mut route_options_by_key = BTreeMap::new();
    for (_, response) in successes {
        for warning in response.warnings {
            warnings.push(format!(
                "extension_suite:{}:{warning}",
                response.provider.extension_id
            ));
        }
        for option in response.route_options {
            route_options_by_key
                .entry((option.logical_id.clone(), option.selected_provider_id))
                .or_insert(option);
        }
        candidates.extend(response.candidates);
    }

    let candidates = dedupe_extension_suite_candidates(candidates);

    if candidates.is_empty() {
        if warnings.is_empty() {
            warnings.push(
                "extension_suite:no_results: no suite provider returned acquisition candidates"
                    .to_string(),
            );
        } else {
            warnings.push("extension_suite:all_failed_or_no_results: no suite provider returned usable acquisition candidates".to_string());
        }
    }

    Ok(extension_suite_response(
        &request.media_type,
        route_options_by_key.into_values().collect(),
        candidates,
        warnings,
    ))
}

async fn search_stream_candidate_suite_with_providers(
    pool: &sqlx::AnyPool,
    request: StreamCandidateSearchRequest,
    providers: Vec<CandidateProviderSelection>,
    #[cfg_attr(not(test), allow(unused_variables))] test_base_urls: Option<
        std::collections::HashMap<Uuid, String>,
    >,
) -> Result<StreamCandidateSearchResponse> {
    let limit = stream_candidate_effective_limit(request.limit) as usize;
    if providers.is_empty() {
        return Ok(extension_suite_stream_response(
            &request.media_type,
            Vec::new(),
            vec![format!(
                "extension_suite:stream:no_provider: no eligible acquisition stream candidate providers are available for {}",
                request.media_type
            )],
        ));
    }

    let mut tasks = JoinSet::new();
    for (index, selected) in providers.into_iter().enumerate() {
        let pool = pool.clone();
        let mut provider_request = request.clone();
        provider_request.provider_id = Some(selected.summary.provider_id);
        provider_request.limit = Some(stream_candidate_effective_limit(provider_request.limit));
        #[cfg(test)]
        let base_url = test_base_urls
            .as_ref()
            .and_then(|urls| urls.get(&selected.summary.provider_id).cloned());

        tasks.spawn(async move {
            let store = ExtensionStore::new(&pool);
            let upstream = {
                #[cfg(test)]
                {
                    if let Some(base_url) = base_url {
                        invoke_stream_candidate_provider_at_base_url(
                            &store,
                            &base_url,
                            &selected,
                            &provider_request,
                        )
                        .await?
                    } else {
                        invoke_stream_candidate_provider(&store, &selected, &provider_request)
                            .await?
                    }
                }
                #[cfg(not(test))]
                {
                    invoke_stream_candidate_provider(&store, &selected, &provider_request).await?
                }
            };
            let mut response = stream_candidate_search_response_from_upstream(
                selected.summary.clone(),
                upstream,
                &provider_request,
            );
            let provider = response.provider.clone();
            let warnings = response.warnings.clone();
            for candidate in &mut response.candidates {
                attach_extension_suite_stream_provider_evidence(candidate, &provider, &warnings);
            }
            apply_stream_candidate_result_cap(&mut response, Some(limit as u32));
            Ok::<_, anyhow::Error>((index, response))
        });
    }

    let mut successes = Vec::new();
    let mut warnings = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(Ok(success)) => successes.push(success),
            Ok(Err(err)) => warnings.push(format!("extension_suite:stream_provider_failed: {err}")),
            Err(err) => warnings.push(format!(
                "extension_suite:stream_provider_failed: task failed: {err}"
            )),
        }
    }
    successes.sort_by_key(|(index, _)| *index);

    let mut candidates = Vec::new();
    for (_, response) in successes {
        for warning in response.warnings {
            warnings.push(format!(
                "extension_suite:{}:{warning}",
                response.provider.extension_id
            ));
        }
        candidates.extend(response.candidates);
    }

    let mut candidates = dedupe_extension_suite_stream_candidates(candidates);
    if candidates.len() > limit {
        candidates.truncate(limit);
    }

    if candidates.is_empty() {
        if warnings.is_empty() {
            warnings.push(
                "extension_suite:stream:no_results: no stream provider returned candidates"
                    .to_string(),
            );
        } else {
            warnings.push("extension_suite:stream:all_failed_or_no_results: no stream provider returned usable candidates".to_string());
        }
    }

    Ok(extension_suite_stream_response(
        &request.media_type,
        candidates,
        warnings,
    ))
}

fn apply_candidate_result_cap(response: &mut CandidateSearchResponse, limit: Option<u32>) {
    let Some(limit) = limit.and_then(|limit| usize::try_from(limit).ok()) else {
        return;
    };
    if response.candidates.len() > limit {
        response.candidates.truncate(limit);
    }
}

fn apply_stream_candidate_result_cap(
    response: &mut StreamCandidateSearchResponse,
    limit: Option<u32>,
) {
    let limit = stream_candidate_effective_limit(limit) as usize;
    if response.candidates.len() > limit {
        response.candidates.truncate(limit);
    }
}

fn stream_candidate_effective_limit(limit: Option<u32>) -> u32 {
    limit
        .unwrap_or(STREAM_CANDIDATE_DEFAULT_LIMIT)
        .clamp(1, STREAM_CANDIDATE_MAX_LIMIT)
}

fn normalize_candidate_search_request(
    mut request: CandidateSearchRequest,
) -> Result<CandidateSearchRequest> {
    validate_candidate_search_request(&request)?;
    request.media_type = request.media_type.trim().to_string();
    request.title = request.title.trim().to_string();
    request.preferences.allowed_qualities =
        normalize_string_list(request.preferences.allowed_qualities);
    request.preferences.required_languages =
        normalize_language_preference_hints(request.preferences.required_languages);
    if let Some(preference) = request.preferences.language_preference.take() {
        let normalized = preference.normalized();
        if request.preferences.required_languages.is_empty() {
            if let Some(media_type) = media_type_from_request(&request.media_type) {
                request.preferences.required_languages =
                    normalized.provider_language_hints(media_type);
            }
        }
        request.preferences.language_preference = Some(normalized);
    }
    Ok(request)
}

fn normalize_stream_candidate_search_request(
    mut request: StreamCandidateSearchRequest,
) -> Result<StreamCandidateSearchRequest> {
    request.media_type = request.media_type.trim().to_string();
    request.title = request.title.trim().to_string();
    if request.media_type.is_empty() {
        bail!("mediaType is required");
    }
    if request.title.is_empty() {
        bail!("title is required");
    }
    if request.limit == Some(0) {
        bail!("limit must be greater than zero");
    }
    if request.targets.is_empty() {
        bail!("targets must include at least one canonical target");
    }
    if request.targets.len() > STREAM_CANDIDATE_MAX_TARGETS {
        bail!(
            "targets must include no more than {} items",
            STREAM_CANDIDATE_MAX_TARGETS
        );
    }
    request.limit = Some(stream_candidate_effective_limit(request.limit));
    request.titles = normalize_stream_title_variants(&request.title, request.titles);
    request.targets = request
        .targets
        .into_iter()
        .enumerate()
        .map(|(index, target)| normalize_stream_search_target(index, target))
        .collect::<Result<Vec<_>>>()?;
    request.preferences.allowed_qualities =
        normalize_string_list(request.preferences.allowed_qualities);
    request.preferences.required_languages =
        normalize_language_preference_hints(request.preferences.required_languages);
    request.preferences.language_profiles =
        normalize_string_list(request.preferences.language_profiles);
    if let Some(preference) = request.preferences.language_preference.take() {
        let normalized = preference.normalized();
        if request.preferences.required_languages.is_empty() {
            if let Some(media_type) = media_type_from_request(&request.media_type) {
                request.preferences.required_languages =
                    normalized.provider_language_hints(media_type);
            }
        }
        if request.preferences.language_profiles.is_empty()
            && let Some(media_type) = media_type_from_request(&request.media_type)
        {
            request.preferences.language_profiles =
                normalized.rule_for_media_type(media_type).profiles;
        }
        request.preferences.language_preference = Some(normalized);
    }
    request.preferences.subtitle_mode = request
        .preferences
        .subtitle_mode
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty());
    Ok(request)
}

fn normalize_language_preference_hints(values: Vec<String>) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for value in values {
        let normalized =
            normalize_language_value(&value).unwrap_or_else(|| value.trim().to_string());
        if normalized.is_empty() {
            continue;
        }
        let key = normalized.to_ascii_lowercase();
        if seen.insert(key) {
            out.push(normalized);
        }
    }
    out
}

fn normalize_stream_title_variants(
    canonical_title: &str,
    titles: Vec<StreamTitleVariant>,
) -> Vec<StreamTitleVariant> {
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    push_stream_title_variant(&mut out, &mut seen, canonical_title, "canonical");
    for title in titles {
        if out.len() >= STREAM_CANDIDATE_MAX_TITLE_VARIANTS {
            break;
        }
        push_stream_title_variant(&mut out, &mut seen, &title.value, &title.kind);
    }
    out
}

fn push_stream_title_variant(
    out: &mut Vec<StreamTitleVariant>,
    seen: &mut BTreeSet<String>,
    value: &str,
    kind: &str,
) {
    let value = value.trim();
    if value.is_empty() {
        return;
    }
    let kind = {
        let trimmed = kind.trim();
        if trimmed.is_empty() { "alias" } else { trimmed }
    };
    let key = format!(
        "{}\u{1f}{}",
        value.to_ascii_lowercase(),
        kind.to_ascii_lowercase()
    );
    if seen.insert(key) {
        out.push(StreamTitleVariant {
            value: value.to_string(),
            kind: kind.to_string(),
        });
    }
}

fn normalize_stream_search_target(
    index: usize,
    mut target: StreamSearchTarget,
) -> Result<StreamSearchTarget> {
    target.target_key = target
        .target_key
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    if target.target_key.is_none() {
        bail!("targets[{index}].targetKey is required");
    }
    target.title = target
        .title
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    target.air_date = target
        .air_date
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    target.metadata = target.metadata.map(redact_sensitive_value);
    Ok(target)
}

fn normalize_string_list(values: Vec<String>) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for value in values {
        let value = value.trim();
        if value.is_empty() {
            continue;
        }
        let key = value.to_ascii_lowercase();
        if seen.insert(key) {
            out.push(value.to_string());
        }
    }
    out
}

#[derive(Debug)]
struct SuiteCandidateDedupeGroup {
    key: String,
    items: Vec<SuiteCandidateDedupeItem>,
}

#[derive(Debug)]
struct SuiteCandidateDedupeItem {
    index: usize,
    candidate: AcquisitionCandidate,
}

#[derive(Debug)]
struct StreamCandidateDedupeGroup {
    key: String,
    items: Vec<StreamCandidateDedupeItem>,
}

#[derive(Debug)]
struct StreamCandidateDedupeItem {
    index: usize,
    candidate: Value,
}

fn dedupe_extension_suite_candidates(
    candidates: Vec<AcquisitionCandidate>,
) -> Vec<AcquisitionCandidate> {
    let mut groups = Vec::<SuiteCandidateDedupeGroup>::new();
    let mut group_index_by_key = BTreeMap::<String, usize>::new();
    for (index, candidate) in candidates.into_iter().enumerate() {
        let key = suite_candidate_dedupe_key(&candidate);
        let group_index = if let Some(group_index) = group_index_by_key.get(&key).copied() {
            group_index
        } else {
            let group_index = groups.len();
            group_index_by_key.insert(key.clone(), group_index);
            groups.push(SuiteCandidateDedupeGroup {
                key,
                items: Vec::new(),
            });
            group_index
        };
        groups[group_index]
            .items
            .push(SuiteCandidateDedupeItem { index, candidate });
    }

    groups
        .into_iter()
        .map(merge_extension_suite_candidate_group)
        .collect()
}

fn dedupe_extension_suite_stream_candidates(candidates: Vec<Value>) -> Vec<Value> {
    let mut groups = Vec::<StreamCandidateDedupeGroup>::new();
    let mut group_index_by_key = BTreeMap::<String, usize>::new();
    for (index, candidate) in candidates.into_iter().enumerate() {
        let key = stream_candidate_dedupe_key(&candidate);
        let group_index = if let Some(group_index) = group_index_by_key.get(&key).copied() {
            group_index
        } else {
            let group_index = groups.len();
            group_index_by_key.insert(key.clone(), group_index);
            groups.push(StreamCandidateDedupeGroup {
                key,
                items: Vec::new(),
            });
            group_index
        };
        groups[group_index]
            .items
            .push(StreamCandidateDedupeItem { index, candidate });
    }

    groups
        .into_iter()
        .map(merge_extension_suite_stream_candidate_group)
        .collect()
}

fn merge_extension_suite_candidate_group(group: SuiteCandidateDedupeGroup) -> AcquisitionCandidate {
    let primary_index = group
        .items
        .iter()
        .enumerate()
        .max_by(|(_, left), (_, right)| compare_suite_primary_candidates(left, right))
        .map(|(index, _)| index)
        .unwrap_or(0);
    let primary_item = &group.items[primary_index];
    let mut merged = primary_item.candidate.clone();
    let mut badges = merged.score_badges.clone();
    for item in &group.items {
        if item.index == primary_item.index {
            continue;
        }
        merge_candidate_hints(&mut merged, &item.candidate, &mut badges);
    }
    merged.score_badges = badges;
    let evidence = merged_extension_suite_evidence(&group, primary_item.index);
    upsert_candidate_server_evidence(&mut merged, "extensionSuite", evidence);
    merged
}

fn merge_extension_suite_stream_candidate_group(group: StreamCandidateDedupeGroup) -> Value {
    let primary_index = group
        .items
        .iter()
        .enumerate()
        .max_by(|(_, left), (_, right)| compare_stream_primary_candidates(left, right))
        .map(|(index, _)| index)
        .unwrap_or(0);
    let primary_item = &group.items[primary_index];
    let mut merged = primary_item.candidate.clone();
    for item in &group.items {
        if item.index == primary_item.index {
            continue;
        }
        merge_stream_candidate_hints(&mut merged, &item.candidate);
    }
    let evidence = merged_extension_suite_stream_evidence(&group, primary_item.index);
    upsert_stream_candidate_server_evidence(&mut merged, "extensionSuite", evidence);
    merged
}

fn compare_suite_primary_candidates(
    left: &SuiteCandidateDedupeItem,
    right: &SuiteCandidateDedupeItem,
) -> Ordering {
    suite_primary_score(&left.candidate)
        .cmp(&suite_primary_score(&right.candidate))
        .then_with(|| right.index.cmp(&left.index))
}

fn compare_stream_primary_candidates(
    left: &StreamCandidateDedupeItem,
    right: &StreamCandidateDedupeItem,
) -> Ordering {
    stream_primary_score(&left.candidate)
        .cmp(&stream_primary_score(&right.candidate))
        .then_with(|| right.index.cmp(&left.index))
}

fn suite_primary_score(candidate: &AcquisitionCandidate) -> (i32, i32, i64, i64, i64, i64) {
    (
        suite_route_availability_score(candidate),
        cached_debrid_score(candidate.cached_debrid),
        finite_score(candidate.score),
        source_rank_score(candidate.rank),
        i64::from(candidate.seeders.unwrap_or_default()),
        i64::try_from(candidate.files.len()).unwrap_or(i64::MAX),
    )
}

fn stream_primary_score(candidate: &Value) -> (i32, i32, i64, i64, i32, i32, i64) {
    (
        stream_route_availability_score(candidate),
        stream_target_confidence_score(candidate),
        finite_score(json_f64_at(candidate, &["score"])),
        source_rank_score(json_u32_at(candidate, &["rank"])),
        stream_delivery_resolution_score(candidate),
        stream_delivery_type_score(candidate),
        stream_resolution_score(candidate),
    )
}

fn suite_route_availability_score(candidate: &AcquisitionCandidate) -> i32 {
    let Some(route_options) = candidate_extension_suite_route_options(candidate) else {
        return 1;
    };
    if route_options
        .iter()
        .any(|option| option.available && option.blocker.is_none())
    {
        2
    } else {
        0
    }
}

fn stream_route_availability_score(candidate: &Value) -> i32 {
    let supports_route = json_string_array_at(candidate, &["supportedRoutes"])
        .iter()
        .any(|route| route.eq_ignore_ascii_case(HTTP_STREAM_DEFAULT_LOGICAL_ID));
    let default_route_matches = json_string_at(candidate, &["defaultRoute"])
        .is_some_and(|route| route.eq_ignore_ascii_case(HTTP_STREAM_DEFAULT_LOGICAL_ID));
    match (supports_route, default_route_matches) {
        (true, true) => 2,
        (true, false) => 1,
        _ => 0,
    }
}

pub(crate) fn stream_target_confidence_score(candidate: &Value) -> i32 {
    match json_string_at(candidate, &["targetEvidence", "confidence"])
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "exact" | "high" => 3,
        "medium" => 2,
        "low" => 1,
        _ => 0,
    }
}

pub(crate) fn stream_delivery_resolution_score(candidate: &Value) -> i32 {
    let has_url = json_string_at(candidate, &["delivery", "url"]).is_some();
    let has_resolve_handle = json_string_at(candidate, &["delivery", "resolveHandle"]).is_some();
    let resolve_required =
        json_bool_at(candidate, &["delivery", "resolveRequired"]).unwrap_or(false);
    match (has_url, resolve_required, has_resolve_handle) {
        (true, false, _) => 3,
        (true, true, _) => 2,
        (false, _, true) => 1,
        _ => 0,
    }
}

pub(crate) fn stream_delivery_type_score(candidate: &Value) -> i32 {
    match json_string_at(candidate, &["delivery", "streamType"])
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "direct_file" => 3,
        "hls" => 2,
        "dash" => 1,
        _ => 0,
    }
}

fn stream_resolution_score(candidate: &Value) -> i64 {
    json_i64_at(candidate, &["mediaEvidence", "resolution"])
        .or_else(|| resolution_from_quality(json_string_at(candidate, &["quality"]).as_deref()))
        .unwrap_or_default()
}

fn resolution_from_quality(value: Option<&str>) -> Option<i64> {
    let value = value?;
    let digits = value
        .chars()
        .skip_while(|ch| !ch.is_ascii_digit())
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>();
    digits.parse::<i64>().ok()
}

fn cached_debrid_score(value: Option<bool>) -> i32 {
    match value {
        Some(true) => 2,
        None => 1,
        Some(false) => 0,
    }
}

fn finite_score(value: Option<f64>) -> i64 {
    value
        .filter(|score| score.is_finite())
        .map(|score| (score * 1_000.0).round() as i64)
        .unwrap_or_default()
}

fn source_rank_score(rank: Option<u32>) -> i64 {
    rank.map(|rank| i64::from(u32::MAX - rank))
        .unwrap_or_default()
}

fn merge_candidate_hints(
    primary: &mut AcquisitionCandidate,
    other: &AcquisitionCandidate,
    badges: &mut Vec<CandidateScoreBadge>,
) {
    primary.cached_debrid = match (primary.cached_debrid, other.cached_debrid) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), Some(false)) => Some(false),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    };
    primary.seeders = max_option(primary.seeders, other.seeders);
    primary.rank = min_option(primary.rank, other.rank);
    primary.score = max_finite_option(primary.score, other.score);
    if primary.quality.is_none() {
        primary.quality.clone_from(&other.quality);
    }
    if primary.size_bytes.is_none() {
        primary.size_bytes = other.size_bytes;
    }
    if primary.language.is_none() {
        primary.language.clone_from(&other.language);
    }
    if primary.file_index.is_none() {
        primary.file_index = other.file_index;
    }
    if primary.files.is_empty() && !other.files.is_empty() {
        primary.files.clone_from(&other.files);
    }
    merge_supported_routes(&mut primary.supported_routes, &other.supported_routes);
    if primary.default_route.is_none() {
        primary.default_route.clone_from(&other.default_route);
    }
    merge_score_badges(badges, &other.score_badges);
}

fn merge_stream_candidate_hints(primary: &mut Value, other: &Value) {
    let (Some(primary), Some(other)) = (primary.as_object_mut(), other.as_object()) else {
        return;
    };
    for key in ["quality", "language", "sizeBytes"] {
        if value_missing(primary.get(key)) && !value_missing(other.get(key)) {
            if let Some(value) = other.get(key) {
                primary.insert(key.to_string(), value.clone());
            }
        }
    }
    merge_optional_json_max_f64(primary, other, "score");
    merge_optional_json_min_u64(primary, other, "rank");
    merge_json_string_array_field(primary, other, "supportedRoutes");
    merge_stream_target_reasons(primary, other);
    merge_stream_media_evidence(primary, other);
}

fn max_option<T: Ord + Copy>(left: Option<T>, right: Option<T>) -> Option<T> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn min_option<T: Ord + Copy>(left: Option<T>, right: Option<T>) -> Option<T> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn max_finite_option(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    match (
        left.filter(|value| value.is_finite()),
        right.filter(|value| value.is_finite()),
    ) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn value_missing(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) => true,
        Some(Value::String(value)) => value.trim().is_empty(),
        Some(Value::Array(values)) => values.is_empty(),
        Some(Value::Object(object)) => object.is_empty(),
        Some(_) => false,
    }
}

fn merge_optional_json_max_f64(
    primary: &mut JsonMap<String, Value>,
    other: &JsonMap<String, Value>,
    key: &str,
) {
    let primary_value = primary.get(key).and_then(Value::as_f64);
    let other_value = other.get(key).and_then(Value::as_f64);
    if let Some(value) = max_finite_option(primary_value, other_value) {
        primary.insert(key.to_string(), json!(value));
    }
}

fn merge_optional_json_min_u64(
    primary: &mut JsonMap<String, Value>,
    other: &JsonMap<String, Value>,
    key: &str,
) {
    let primary_value = primary.get(key).and_then(Value::as_u64);
    let other_value = other.get(key).and_then(Value::as_u64);
    let value = match (primary_value, other_value) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    };
    if let Some(value) = value {
        primary.insert(key.to_string(), json!(value));
    }
}

fn merge_json_string_array_field(
    primary: &mut JsonMap<String, Value>,
    other: &JsonMap<String, Value>,
    key: &str,
) {
    let mut values = primary
        .get(key)
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut seen = values
        .iter()
        .map(|value| value.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    if let Some(other_values) = other.get(key).and_then(Value::as_array) {
        for value in other_values.iter().filter_map(Value::as_str) {
            let key = value.to_ascii_lowercase();
            if seen.insert(key) {
                values.push(value.to_string());
            }
        }
    }
    if !values.is_empty() {
        primary.insert(key.to_string(), json!(values));
    }
}

fn merge_stream_target_reasons(
    primary: &mut JsonMap<String, Value>,
    other: &JsonMap<String, Value>,
) {
    let Some(primary_target) = primary
        .get_mut("targetEvidence")
        .and_then(Value::as_object_mut)
    else {
        return;
    };
    let Some(other_target) = other.get("targetEvidence").and_then(Value::as_object) else {
        return;
    };
    merge_json_string_array_field(primary_target, other_target, "reasons");
}

fn merge_stream_media_evidence(
    primary: &mut JsonMap<String, Value>,
    other: &JsonMap<String, Value>,
) {
    let Some(other_media) = other.get("mediaEvidence").and_then(Value::as_object) else {
        return;
    };
    if !primary
        .get("mediaEvidence")
        .is_some_and(|value| value.is_object())
    {
        primary.insert("mediaEvidence".to_string(), Value::Object(JsonMap::new()));
    }
    let Some(primary_media) = primary
        .get_mut("mediaEvidence")
        .and_then(Value::as_object_mut)
    else {
        return;
    };
    for (key, value) in other_media {
        if matches!(key.as_str(), "audioLanguages" | "subtitleLanguages") {
            merge_json_string_array_field(primary_media, other_media, key);
        } else if value_missing(primary_media.get(key)) && !value_missing(Some(value)) {
            primary_media.insert(key.clone(), value.clone());
        }
    }
}

fn merge_supported_routes(primary: &mut Vec<String>, other: &[String]) {
    let mut seen = primary
        .iter()
        .map(|route| route.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    for route in other {
        let key = route.to_ascii_lowercase();
        if seen.insert(key) {
            primary.push(route.clone());
        }
    }
}

fn merge_score_badges(primary: &mut Vec<CandidateScoreBadge>, other: &[CandidateScoreBadge]) {
    let mut seen = primary.iter().map(score_badge_key).collect::<BTreeSet<_>>();
    for badge in other {
        let key = score_badge_key(badge);
        if seen.insert(key) {
            primary.push(badge.clone());
        }
    }
}

fn score_badge_key(badge: &CandidateScoreBadge) -> String {
    format!(
        "{}\u{1f}{}",
        badge.label.trim().to_ascii_lowercase(),
        badge
            .detail
            .as_deref()
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
    )
}

fn merged_extension_suite_evidence(
    group: &SuiteCandidateDedupeGroup,
    primary_item_index: usize,
) -> Value {
    let primary_item = group
        .items
        .iter()
        .find(|item| item.index == primary_item_index)
        .or_else(|| group.items.first())
        .expect("suite dedupe groups are non-empty");
    let mut evidence = suite_provider_evidence_object(&primary_item.candidate);
    let contributors = group
        .items
        .iter()
        .map(|item| suite_candidate_contributor_evidence(item, item.index == primary_item_index))
        .collect::<Vec<_>>();
    let warnings = group
        .items
        .iter()
        .flat_map(|item| suite_candidate_provider_warnings(&item.candidate))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(Value::String)
        .collect::<Vec<_>>();
    evidence.insert("dedupeKey".to_string(), Value::String(group.key.clone()));
    evidence.insert(
        "dedupeFingerprintVersion".to_string(),
        Value::String("es4-suite-v1".to_string()),
    );
    evidence.insert("contributorCount".to_string(), json!(contributors.len()));
    evidence.insert("contributors".to_string(), Value::Array(contributors));
    evidence.insert("warnings".to_string(), Value::Array(warnings));
    Value::Object(evidence)
}

fn merged_extension_suite_stream_evidence(
    group: &StreamCandidateDedupeGroup,
    primary_item_index: usize,
) -> Value {
    let primary_item = group
        .items
        .iter()
        .find(|item| item.index == primary_item_index)
        .or_else(|| group.items.first())
        .expect("stream dedupe groups are non-empty");
    let mut evidence = stream_provider_evidence_object(&primary_item.candidate);
    let contributors = group
        .items
        .iter()
        .map(|item| stream_candidate_contributor_evidence(item, item.index == primary_item_index))
        .collect::<Vec<_>>();
    let warnings = group
        .items
        .iter()
        .flat_map(|item| stream_candidate_provider_warnings(&item.candidate))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(Value::String)
        .collect::<Vec<_>>();
    evidence.insert("lane".to_string(), Value::String("stream".to_string()));
    evidence.insert("dedupeKey".to_string(), Value::String(group.key.clone()));
    evidence.insert(
        "dedupeFingerprintVersion".to_string(),
        Value::String("ess4-stream-v1".to_string()),
    );
    if let Some(target_key) = stream_candidate_target_key(&primary_item.candidate) {
        evidence.insert("targetKey".to_string(), Value::String(target_key));
    }
    evidence.insert("contributorCount".to_string(), json!(contributors.len()));
    evidence.insert("contributors".to_string(), Value::Array(contributors));
    evidence.insert("warnings".to_string(), Value::Array(warnings));
    Value::Object(evidence)
}

fn suite_candidate_contributor_evidence(item: &SuiteCandidateDedupeItem, primary: bool) -> Value {
    let provider = suite_provider_evidence_object(&item.candidate);
    json!({
        "primary": primary,
        "providerId": provider.get("providerId").cloned().unwrap_or(Value::Null),
        "extensionId": provider.get("extensionId").cloned().unwrap_or(Value::Null),
        "extensionName": provider.get("extensionName").cloned().unwrap_or(Value::Null),
        "instanceId": provider.get("instanceId").cloned().unwrap_or(Value::Null),
        "instanceName": provider.get("instanceName").cloned().unwrap_or(Value::Null),
        "capability": provider.get("capability").cloned().unwrap_or(Value::Null),
        "implementation": provider.get("implementation").cloned().unwrap_or(Value::Null),
        "mediaTypes": provider.get("mediaTypes").cloned().unwrap_or_else(|| Value::Array(Vec::new())),
        "actions": provider.get("actions").cloned().unwrap_or_else(|| Value::Array(Vec::new())),
        "warnings": provider.get("warnings").cloned().unwrap_or_else(|| Value::Array(Vec::new())),
        "routeOptions": provider.get("routeOptions").cloned().unwrap_or_else(|| Value::Array(Vec::new())),
        "candidate": suite_candidate_snapshot(&item.candidate),
    })
}

fn stream_candidate_contributor_evidence(item: &StreamCandidateDedupeItem, primary: bool) -> Value {
    let provider = stream_provider_evidence_object(&item.candidate);
    json!({
        "primary": primary,
        "providerId": provider.get("providerId").cloned().unwrap_or(Value::Null),
        "extensionId": provider.get("extensionId").cloned().unwrap_or(Value::Null),
        "extensionName": provider.get("extensionName").cloned().unwrap_or(Value::Null),
        "instanceId": provider.get("instanceId").cloned().unwrap_or(Value::Null),
        "instanceName": provider.get("instanceName").cloned().unwrap_or(Value::Null),
        "capability": provider.get("capability").cloned().unwrap_or(Value::Null),
        "implementation": provider.get("implementation").cloned().unwrap_or(Value::Null),
        "mediaTypes": provider.get("mediaTypes").cloned().unwrap_or_else(|| Value::Array(Vec::new())),
        "actions": provider.get("actions").cloned().unwrap_or_else(|| Value::Array(Vec::new())),
        "warnings": provider.get("warnings").cloned().unwrap_or_else(|| Value::Array(Vec::new())),
        "sourceModule": item.candidate.get("sourceModule").cloned().unwrap_or(Value::Null),
        "targetEvidence": item.candidate.get("targetEvidence").cloned().unwrap_or(Value::Null),
        "candidate": stream_candidate_snapshot(&item.candidate),
    })
}

fn suite_candidate_snapshot(candidate: &AcquisitionCandidate) -> Value {
    let mut object = JsonMap::new();
    insert_optional_json(&mut object, "id", candidate.id.clone());
    object.insert("title".to_string(), json!(candidate.title));
    object.insert("sourceKind".to_string(), json!(candidate.source_kind));
    insert_optional_json(&mut object, "infoHash", candidate.info_hash.clone());
    insert_optional_json(&mut object, "fileIndex", candidate.file_index);
    insert_optional_json(&mut object, "quality", candidate.quality.clone());
    insert_optional_json(&mut object, "sizeBytes", candidate.size_bytes);
    insert_optional_json(&mut object, "seeders", candidate.seeders);
    insert_optional_json(&mut object, "language", candidate.language.clone());
    insert_optional_json(&mut object, "cachedDebrid", candidate.cached_debrid);
    insert_optional_json(&mut object, "rank", candidate.rank);
    insert_optional_json(&mut object, "score", candidate.score);
    object.insert("fileCount".to_string(), json!(candidate.files.len()));
    if !candidate.score_badges.is_empty() {
        object.insert("scoreBadges".to_string(), json!(candidate.score_badges));
    }
    if let Some(raw) = contributor_raw_evidence(candidate) {
        object.insert("raw".to_string(), raw);
    }
    Value::Object(object)
}

fn stream_candidate_snapshot(candidate: &Value) -> Value {
    let mut object = JsonMap::new();
    for key in [
        "id",
        "title",
        "candidateKind",
        "sourceKind",
        "quality",
        "language",
        "sizeBytes",
        "rank",
        "score",
        "mediaEvidence",
    ] {
        if let Some(value) = candidate
            .get(key)
            .filter(|value| !value_missing(Some(value)))
        {
            object.insert(key.to_string(), redact_sensitive_value(value.clone()));
        }
    }
    if let Some(target_key) = stream_candidate_target_key(candidate) {
        object.insert("targetKey".to_string(), Value::String(target_key));
    }
    if let Some(delivery) = stream_candidate_delivery_snapshot(candidate) {
        object.insert("delivery".to_string(), delivery);
    }
    if let Some(raw) = stream_contributor_raw_evidence(candidate) {
        object.insert("raw".to_string(), raw);
    }
    Value::Object(object)
}

fn stream_candidate_delivery_snapshot(candidate: &Value) -> Option<Value> {
    let delivery = candidate.get("delivery")?.as_object()?;
    let mut object = JsonMap::new();
    for key in [
        "streamType",
        "url",
        "referer",
        "expiresAt",
        "resolveRequired",
    ] {
        if let Some(value) = delivery
            .get(key)
            .filter(|value| !value_missing(Some(value)))
        {
            object.insert(key.to_string(), redact_sensitive_value(value.clone()));
        }
    }
    if delivery
        .get("resolveHandle")
        .and_then(Value::as_str)
        .is_some()
    {
        object.insert("hasResolveHandle".to_string(), Value::Bool(true));
    }
    if let Some(headers) = delivery.get("headers").and_then(Value::as_object) {
        let header_names = headers.keys().cloned().collect::<Vec<_>>();
        if !header_names.is_empty() {
            object.insert("headerNames".to_string(), json!(header_names));
        }
    }
    (!object.is_empty()).then_some(Value::Object(object))
}

fn insert_optional_json<T: Serialize>(
    object: &mut JsonMap<String, Value>,
    key: &str,
    value: Option<T>,
) {
    if let Some(value) = value {
        object.insert(key.to_string(), json!(value));
    }
}

fn contributor_raw_evidence(candidate: &AcquisitionCandidate) -> Option<Value> {
    let mut raw = candidate.raw.clone()?;
    if let Value::Object(root) = &mut raw {
        let remove_server_evidence = if let Some(server_evidence) = root
            .get_mut("serverEvidence")
            .and_then(Value::as_object_mut)
        {
            server_evidence.remove("extensionSuite");
            server_evidence.is_empty()
        } else {
            false
        };
        if remove_server_evidence {
            root.remove("serverEvidence");
        }
        if root.is_empty() {
            return None;
        }
    }
    Some(raw)
}

fn stream_contributor_raw_evidence(candidate: &Value) -> Option<Value> {
    let mut raw = candidate.get("raw").cloned()?;
    if let Value::Object(root) = &mut raw {
        let remove_server_evidence = if let Some(server_evidence) = root
            .get_mut("serverEvidence")
            .and_then(Value::as_object_mut)
        {
            server_evidence.remove("extensionSuite");
            server_evidence.is_empty()
        } else {
            false
        };
        if remove_server_evidence {
            root.remove("serverEvidence");
        }
        if root.is_empty() {
            return None;
        }
    }
    Some(redact_sensitive_value(raw))
}

fn suite_provider_evidence_object(candidate: &AcquisitionCandidate) -> JsonMap<String, Value> {
    candidate
        .raw
        .as_ref()
        .and_then(|raw| raw.pointer("/serverEvidence/extensionSuite"))
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default()
}

fn stream_provider_evidence_object(candidate: &Value) -> JsonMap<String, Value> {
    candidate
        .pointer("/raw/serverEvidence/extensionSuite")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default()
}

fn suite_candidate_provider_warnings(candidate: &AcquisitionCandidate) -> Vec<String> {
    suite_provider_evidence_object(candidate)
        .get("warnings")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn stream_candidate_provider_warnings(candidate: &Value) -> Vec<String> {
    stream_provider_evidence_object(candidate)
        .get("warnings")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn candidate_extension_suite_route_options(
    candidate: &AcquisitionCandidate,
) -> Option<Vec<CandidateRouteOption>> {
    candidate
        .raw
        .as_ref()
        .and_then(|raw| raw.pointer("/serverEvidence/extensionSuite/routeOptions"))
        .cloned()
        .and_then(|value| serde_json::from_value::<Vec<CandidateRouteOption>>(value).ok())
}

fn suite_candidate_dedupe_key(candidate: &AcquisitionCandidate) -> String {
    let source_kind = candidate.source_kind.trim().to_ascii_lowercase();
    match source_kind.as_str() {
        "magnet" | "torrent" => normalized_info_hash(candidate.info_hash.as_deref())
            .or_else(|| magnet_source_info_hash(&candidate.source))
            .map(|hash| format!("torrent:infohash:{hash}"))
            .unwrap_or_else(|| {
                format!(
                    "{}:source:{}",
                    source_kind,
                    source_fingerprint(&candidate.source)
                )
            }),
        "nzb" | "usenet" => candidate
            .id
            .as_deref()
            .map(stable_value_fingerprint)
            .map(|id| format!("usenet:id:{id}"))
            .unwrap_or_else(|| format!("usenet:source:{}", source_fingerprint(&candidate.source))),
        "url" if source_looks_like_nzb(&candidate.source) => {
            format!("usenet:source:{}", source_fingerprint(&candidate.source))
        }
        "http" | "hoster" | "url" => candidate
            .id
            .as_deref()
            .map(stable_value_fingerprint)
            .map(|id| format!("{source_kind}:id:{id}"))
            .unwrap_or_else(|| {
                format!(
                    "{}:source:{}",
                    source_kind,
                    source_fingerprint(&candidate.source)
                )
            }),
        _ => format!(
            "{}:source:{}",
            source_kind,
            source_fingerprint(&candidate.source)
        ),
    }
}

fn stream_candidate_dedupe_key(candidate: &Value) -> String {
    let target_key = stream_candidate_target_key(candidate)
        .unwrap_or_else(|| "unknown-target".to_string())
        .to_ascii_lowercase();
    if let Some(identity) = stream_candidate_provider_identity(candidate) {
        return format!("stream:{target_key}:identity:{identity}");
    }
    if let Some(url) = json_string_at(candidate, &["delivery", "url"]) {
        return format!(
            "stream:{target_key}:delivery:{}",
            stable_value_fingerprint(&canonical_stream_delivery_url_for_fingerprint(&url))
        );
    }
    if let Some(source) = json_string_at(candidate, &["source"]) {
        return format!("stream:{target_key}:source:{}", source_fingerprint(&source));
    }
    if let Some(id) = json_string_at(candidate, &["id"]) {
        return format!("stream:{target_key}:id:{}", stable_value_fingerprint(&id));
    }
    format!(
        "stream:{target_key}:candidate:{}",
        stable_value_fingerprint(&candidate.to_string())
    )
}

fn stream_candidate_provider_identity(candidate: &Value) -> Option<String> {
    let source_module_id = json_string_at(candidate, &["sourceModule", "id"])?;
    let media_id = stream_candidate_identity_component(
        candidate,
        &[
            &["sourceModule", "providerMediaId"],
            &["sourceModule", "mediaId"],
            &["raw", "providerMediaId"],
            &["raw", "mediaId"],
            &["raw", "media_id"],
            &["raw", "showId"],
            &["raw", "show_id"],
        ],
    )?;
    let episode_id = stream_candidate_identity_component(
        candidate,
        &[
            &["sourceModule", "providerEpisodeId"],
            &["sourceModule", "episodeId"],
            &["raw", "providerEpisodeId"],
            &["raw", "episodeId"],
            &["raw", "episode_id"],
            &["raw", "urlEpisodeId"],
        ],
    )?;
    let hoster_id = stream_candidate_identity_component(
        candidate,
        &[
            &["sourceModule", "hosterId"],
            &["sourceModule", "hoster"],
            &["raw", "hosterId"],
            &["raw", "hoster"],
            &["raw", "hosterName"],
            &["raw", "server"],
            &["raw", "sourceName"],
        ],
    )?;
    Some(format!(
        "{}:{}:{}:{}",
        stream_identity_component_key(&source_module_id),
        stream_identity_component_key(&media_id),
        stream_identity_component_key(&episode_id),
        stream_identity_component_key(&hoster_id),
    ))
}

fn stream_candidate_identity_component(candidate: &Value, paths: &[&[&str]]) -> Option<String> {
    paths
        .iter()
        .find_map(|path| json_string_at(candidate, path))
        .or_else(|| stream_candidate_id_component(candidate, paths))
}

fn stream_candidate_id_component(candidate: &Value, paths: &[&[&str]]) -> Option<String> {
    let id = json_string_at(candidate, &["id"])?;
    let parts = id.split(':').map(str::trim).collect::<Vec<_>>();
    if parts.len() < 4 {
        return None;
    }
    let wants_media = paths.iter().any(|path| {
        path.last()
            .is_some_and(|key| key.to_ascii_lowercase().contains("media"))
    });
    let wants_episode = paths.iter().any(|path| {
        path.last()
            .is_some_and(|key| key.to_ascii_lowercase().contains("episode"))
    });
    let wants_hoster = paths.iter().any(|path| {
        path.last().is_some_and(|key| {
            let key = key.to_ascii_lowercase();
            key.contains("hoster") || key == "server" || key == "sourcename"
        })
    });
    if wants_media {
        Some(parts[parts.len() - 3].to_string())
    } else if wants_episode {
        Some(parts[parts.len() - 2].to_string())
    } else if wants_hoster {
        Some(parts[parts.len() - 1].to_string())
    } else {
        None
    }
}

fn stream_identity_component_key(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn stream_candidate_target_key(candidate: &Value) -> Option<String> {
    json_string_at(candidate, &["targetEvidence", "targetKey"])
}

fn normalized_info_hash(value: Option<&str>) -> Option<String> {
    let trimmed = value?.trim();
    let hash = trimmed
        .strip_prefix("urn:btih:")
        .or_else(|| trimmed.strip_prefix("URN:BTIH:"))
        .unwrap_or(trimmed)
        .trim()
        .to_ascii_lowercase();
    (!hash.is_empty()
        && hash
            .chars()
            .all(|ch| ch.is_ascii_hexdigit() || ch.is_ascii_alphanumeric()))
    .then_some(hash)
}

fn magnet_source_info_hash(source: &str) -> Option<String> {
    let (scheme, query) = source.split_once('?')?;
    if !scheme.eq_ignore_ascii_case("magnet:") {
        return None;
    }
    for pair in query.split('&') {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let Ok(key) = urlencoding::decode(key) else {
            continue;
        };
        if !key.eq_ignore_ascii_case("xt") {
            continue;
        }
        let Ok(value) = urlencoding::decode(value) else {
            continue;
        };
        if let Some(hash) = normalized_info_hash(Some(
            value
                .strip_prefix("urn:btih:")
                .or_else(|| value.strip_prefix("URN:BTIH:"))
                .unwrap_or(value.as_ref()),
        )) {
            return Some(hash);
        }
    }
    None
}

fn source_fingerprint(source: &str) -> String {
    stable_value_fingerprint(&canonical_source_for_fingerprint(source))
}

fn stable_value_fingerprint(value: &str) -> String {
    blake3::hash(value.trim().as_bytes()).to_hex().to_string()
}

fn canonical_source_for_fingerprint(source: &str) -> String {
    let trimmed = source.trim();
    let Ok(mut url) = Url::parse(trimmed) else {
        return trimmed.to_ascii_lowercase();
    };
    let scheme = url.scheme().to_ascii_lowercase();
    let host = url.host_str().map(str::to_ascii_lowercase);
    let mut query_pairs = url
        .query_pairs()
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    if !query_pairs.is_empty() {
        query_pairs.sort();
        url.set_query(None);
        {
            let mut query = url.query_pairs_mut();
            for (key, value) in query_pairs {
                query.append_pair(&key, &value);
            }
        }
    }
    url.set_fragment(None);
    if let Some(host) = host.as_deref() {
        let _ = url.set_host(Some(host));
    }
    let _ = url.set_scheme(&scheme);
    url.to_string()
}

fn canonical_stream_delivery_url_for_fingerprint(source: &str) -> String {
    let trimmed = source.trim();
    let Ok(mut url) = Url::parse(trimmed) else {
        return trimmed.to_ascii_lowercase();
    };
    let scheme = url.scheme().to_ascii_lowercase();
    let host = url.host_str().map(str::to_ascii_lowercase);
    let mut query_pairs = url
        .query_pairs()
        .filter(|(key, _)| !is_sensitive_key(key))
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    if !query_pairs.is_empty() {
        query_pairs.sort();
        url.set_query(None);
        {
            let mut query = url.query_pairs_mut();
            for (key, value) in query_pairs {
                query.append_pair(&key, &value);
            }
        }
    } else {
        url.set_query(None);
    }
    url.set_fragment(None);
    if let Some(host) = host.as_deref() {
        let _ = url.set_host(Some(host));
    }
    let _ = url.set_scheme(&scheme);
    url.to_string()
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

fn extension_suite_response(
    media_type: &str,
    route_options: Vec<CandidateRouteOption>,
    candidates: Vec<AcquisitionCandidate>,
    warnings: Vec<String>,
) -> CandidateSearchResponse {
    CandidateSearchResponse {
        schema_version: CANDIDATE_PROVIDER_SCHEMA_VERSION,
        provider: CandidateProviderSummary {
            provider_id: Uuid::nil(),
            extension_id: "elixir.extension_suite".to_string(),
            extension_name: "Elixir Extension Suite".to_string(),
            instance_id: Uuid::nil(),
            instance_name: "default".to_string(),
            capability: ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY.to_string(),
            implementation: Some("extension_suite".to_string()),
            health_state: ProviderHealthState::Healthy,
            media_types: vec![media_type.to_string()],
            actions: vec!["search".to_string()],
        },
        route_options,
        candidates,
        warnings,
    }
}

fn extension_suite_stream_response(
    media_type: &str,
    candidates: Vec<Value>,
    warnings: Vec<String>,
) -> StreamCandidateSearchResponse {
    StreamCandidateSearchResponse {
        schema_version: CANDIDATE_PROVIDER_SCHEMA_VERSION,
        provider: CandidateProviderSummary {
            provider_id: Uuid::nil(),
            extension_id: "elixir.extension_suite".to_string(),
            extension_name: "Elixir Extension Suite".to_string(),
            instance_id: Uuid::nil(),
            instance_name: "default".to_string(),
            capability: ACQUISITION_STREAM_CANDIDATE_PROVIDER_CAPABILITY.to_string(),
            implementation: Some("extension_suite_stream".to_string()),
            health_state: ProviderHealthState::Healthy,
            media_types: vec![media_type.to_string()],
            actions: vec!["search".to_string()],
        },
        candidates,
        warnings,
    }
}

fn attach_extension_suite_provider_evidence(
    candidate: &mut AcquisitionCandidate,
    provider: &CandidateProviderSummary,
    route_options: &[CandidateRouteOption],
    warnings: &[String],
) {
    upsert_candidate_server_evidence(
        candidate,
        "extensionSuite",
        json!({
            "providerId": provider.provider_id.to_string(),
            "extensionId": provider.extension_id,
            "extensionName": provider.extension_name,
            "instanceId": provider.instance_id.to_string(),
            "instanceName": provider.instance_name,
            "capability": provider.capability,
            "implementation": provider.implementation,
            "mediaTypes": provider.media_types,
            "actions": provider.actions,
            "warnings": warnings,
            "routeOptions": route_options,
        }),
    );
}

fn attach_extension_suite_stream_provider_evidence(
    candidate: &mut Value,
    provider: &CandidateProviderSummary,
    warnings: &[String],
) {
    upsert_stream_candidate_server_evidence(
        candidate,
        "extensionSuite",
        json!({
            "providerId": provider.provider_id.to_string(),
            "extensionId": provider.extension_id,
            "extensionName": provider.extension_name,
            "instanceId": provider.instance_id.to_string(),
            "instanceName": provider.instance_name,
            "capability": provider.capability,
            "implementation": provider.implementation,
            "mediaTypes": provider.media_types,
            "actions": provider.actions,
            "warnings": warnings,
        }),
    );
}

fn candidate_search_response_from_upstream(
    provider: CandidateProviderSummary,
    route_options: Vec<CandidateRouteOption>,
    upstream: CandidateProviderUpstreamResponse,
    request: &CandidateSearchRequest,
) -> Result<CandidateSearchResponse> {
    let (mut candidates, normalization_warnings) =
        normalize_upstream_candidates(upstream.candidates);
    apply_release_candidate_language_preference(request, &mut candidates);
    let mut warnings = upstream.warnings;
    warnings.extend(normalization_warnings);

    Ok(CandidateSearchResponse {
        schema_version: CANDIDATE_PROVIDER_SCHEMA_VERSION,
        provider,
        route_options,
        candidates,
        warnings,
    })
}

fn stream_candidate_search_response_from_upstream(
    provider: CandidateProviderSummary,
    upstream: StreamCandidateProviderUpstreamResponse,
    request: &StreamCandidateSearchRequest,
) -> StreamCandidateSearchResponse {
    let (candidates, validation_warnings) =
        validate_upstream_stream_candidates(upstream.candidates);
    let candidates = apply_stream_candidate_language_preference(request, candidates);
    let mut warnings = upstream
        .warnings
        .into_iter()
        .map(|warning| warning.trim().to_string())
        .filter(|warning| !warning.is_empty())
        .collect::<Vec<_>>();
    warnings.extend(validation_warnings);

    StreamCandidateSearchResponse {
        schema_version: CANDIDATE_PROVIDER_SCHEMA_VERSION,
        provider,
        candidates,
        warnings,
    }
}

fn apply_release_candidate_language_preference(
    request: &CandidateSearchRequest,
    candidates: &mut [AcquisitionCandidate],
) {
    let Some(media_type) = media_type_from_request(&request.media_type) else {
        return;
    };
    let preference = request
        .preferences
        .language_preference
        .clone()
        .unwrap_or_else(|| {
            let profile = (!request.preferences.required_languages.is_empty()).then(|| {
                json!({
                    "requiredLanguages": &request.preferences.required_languages,
                })
            });
            language_preference_from_quality_profile(profile.as_ref(), media_type)
        });
    if !preference.active() {
        return;
    }

    for candidate in candidates {
        let evidence = release_candidate_language_evidence(candidate);
        let assessment = assess_language_preference(&preference, media_type, &evidence);
        if assessment.state == LanguagePreferenceAssessmentState::Off {
            continue;
        }
        if assessment.score_delta != 0.0 {
            candidate.score = Some(
                candidate
                    .score
                    .filter(|score| score.is_finite())
                    .unwrap_or_default()
                    + assessment.score_delta,
            );
        }
        ensure_score_badge(
            candidate,
            "Language preference",
            language_preference_badge_detail(&assessment),
            Some(assessment.score_delta),
        );
        upsert_candidate_server_evidence(
            candidate,
            "languagePreference",
            language_preference_evidence_json(&preference, &assessment),
        );
    }
}

fn apply_stream_candidate_language_preference(
    request: &StreamCandidateSearchRequest,
    mut candidates: Vec<Value>,
) -> Vec<Value> {
    let Some(media_type) = media_type_from_request(&request.media_type) else {
        return candidates;
    };
    let preference = request
        .preferences
        .language_preference
        .clone()
        .unwrap_or_else(|| {
            let profile = (!request.preferences.required_languages.is_empty()
                || !request.preferences.language_profiles.is_empty())
            .then(|| {
                json!({
                    "requiredLanguages": &request.preferences.required_languages,
                    "languagePreference": {
                        "mode": "prefer",
                        "anime": { "profiles": &request.preferences.language_profiles },
                    }
                })
            });
            language_preference_from_quality_profile(profile.as_ref(), media_type)
        });
    if !preference.active() {
        return candidates;
    }

    for candidate in &mut candidates {
        let evidence = stream_candidate_language_evidence(candidate);
        let assessment = assess_language_preference(&preference, media_type, &evidence);
        if assessment.state == LanguagePreferenceAssessmentState::Off {
            continue;
        }
        if let Some(object) = candidate.as_object_mut() {
            let score = object
                .get("score")
                .and_then(Value::as_f64)
                .filter(|score| score.is_finite())
                .unwrap_or_default()
                + assessment.score_delta;
            object.insert("score".to_string(), json!(score));
            append_stream_score_badge(
                object,
                "Language preference",
                language_preference_badge_detail(&assessment),
                assessment.score_delta,
            );
        }
        upsert_stream_candidate_server_evidence(
            candidate,
            "languagePreference",
            language_preference_evidence_json(&preference, &assessment),
        );
    }
    candidates
}

fn release_candidate_language_evidence(
    candidate: &AcquisitionCandidate,
) -> CandidateLanguageEvidence {
    let mut evidence = CandidateLanguageEvidence::default();
    add_language_evidence_text(&mut evidence, &candidate.title);
    if let Some(language) = candidate.language.as_deref() {
        add_language_evidence_text(&mut evidence, language);
    }
    if let Some(raw) = candidate.raw.as_ref() {
        add_language_evidence_from_paths(
            &mut evidence,
            raw,
            &[
                &["language"][..],
                &["languages"][..],
                &["audioLanguage"][..],
                &["audioLanguages"][..],
                &["mediaEvidence", "language"][..],
                &["mediaEvidence", "audioLanguage"][..],
                &["mediaEvidence", "audioLanguages"][..],
                &["parsedHints", "language"][..],
                &["parsedHints", "languages"][..],
                &["raw", "language"][..],
                &["raw", "languages"][..],
                &["raw", "audioLanguage"][..],
                &["raw", "audioLanguages"][..],
            ],
        );
        add_subtitle_evidence_from_paths(
            &mut evidence,
            raw,
            &[
                &["subtitleLanguage"][..],
                &["subtitleLanguages"][..],
                &["mediaEvidence", "subtitleLanguage"][..],
                &["mediaEvidence", "subtitleLanguages"][..],
                &["parsedHints", "subtitleLanguage"][..],
                &["parsedHints", "subtitleLanguages"][..],
                &["raw", "subtitleLanguage"][..],
                &["raw", "subtitleLanguages"][..],
            ],
        );
    }
    evidence
}

fn stream_candidate_language_evidence(candidate: &Value) -> CandidateLanguageEvidence {
    let mut evidence = CandidateLanguageEvidence::default();
    for pointer in [
        "/title",
        "/language",
        "/audioLanguage",
        "/audioLanguages",
        "/mediaEvidence/language",
        "/mediaEvidence/audioLanguage",
        "/mediaEvidence/audioLanguages",
        "/sourceModule/languageTags",
        "/raw/language",
        "/raw/languages",
        "/raw/audioLanguage",
        "/raw/audioLanguages",
    ] {
        if let Some(value) = candidate.pointer(pointer) {
            add_language_evidence_value(&mut evidence, value);
        }
    }
    for pointer in [
        "/subtitleLanguage",
        "/subtitleLanguages",
        "/mediaEvidence/subtitleLanguage",
        "/mediaEvidence/subtitleLanguages",
        "/raw/subtitleLanguage",
        "/raw/subtitleLanguages",
    ] {
        if let Some(value) = candidate.pointer(pointer) {
            add_subtitle_language_evidence_value(&mut evidence, value);
        }
    }
    evidence
}

fn add_language_evidence_from_paths(
    evidence: &mut CandidateLanguageEvidence,
    root: &Value,
    paths: &[&[&str]],
) {
    for path in paths {
        if let Some(value) = json_path(root, path) {
            add_language_evidence_value(evidence, value);
        }
    }
}

fn add_subtitle_evidence_from_paths(
    evidence: &mut CandidateLanguageEvidence,
    root: &Value,
    paths: &[&[&str]],
) {
    for path in paths {
        if let Some(value) = json_path(root, path) {
            add_subtitle_language_evidence_value(evidence, value);
        }
    }
}

fn json_path<'a>(root: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut cursor = root;
    for key in path {
        cursor = cursor.get(*key)?;
    }
    Some(cursor)
}

fn language_preference_badge_detail(
    assessment: &crate::acquisition::language_policy::LanguagePreferenceAssessment,
) -> String {
    match assessment.state {
        LanguagePreferenceAssessmentState::Match => {
            let mut parts = Vec::new();
            if !assessment.matching_audio.is_empty() {
                parts.push(format!("audio {}", assessment.matching_audio.join(", ")));
            }
            if !assessment.matching_subtitles.is_empty() {
                parts.push(format!(
                    "subtitles {}",
                    assessment.matching_subtitles.join(", ")
                ));
            }
            if !assessment.matching_profiles.is_empty() {
                parts.push(format!(
                    "profile {}",
                    assessment.matching_profiles.join(", ")
                ));
            }
            format!(
                "Candidate has desired language evidence: {}.",
                parts.join("; ")
            )
        }
        LanguagePreferenceAssessmentState::Mismatch => {
            "Candidate language evidence does not match the saved preference.".to_string()
        }
        LanguagePreferenceAssessmentState::Unknown => {
            "Candidate language evidence is unknown; it remains eligible.".to_string()
        }
        LanguagePreferenceAssessmentState::Off => "Language preference is off.".to_string(),
    }
}

fn language_preference_evidence_json(
    preference: &AcquisitionLanguagePreference,
    assessment: &crate::acquisition::language_policy::LanguagePreferenceAssessment,
) -> Value {
    json!({
        "policyVersion": "lp3-language-preference-v1",
        "mode": preference.mode.as_str(),
        "unknownLanguage": preference.unknown_language.as_str(),
        "state": assessment.state.as_str(),
        "scoreDelta": assessment.score_delta,
        "desiredAudioLanguages": assessment.desired_audio,
        "desiredSubtitleLanguages": assessment.desired_subtitles,
        "desiredProfiles": assessment.desired_profiles,
        "matchingAudioLanguages": assessment.matching_audio,
        "matchingSubtitleLanguages": assessment.matching_subtitles,
        "matchingProfiles": assessment.matching_profiles,
        "evidenceAudioLanguages": assessment.evidence_audio,
        "evidenceSubtitleLanguages": assessment.evidence_subtitles,
        "evidenceProfiles": assessment.evidence_profiles,
        "languageIsIdentityEvidence": false,
        "unknownLanguageIsRejected": false,
        "requiresReview": preference.mode
            == crate::acquisition::language_policy::LanguagePreferenceMode::RequireReview
            && assessment.state != LanguagePreferenceAssessmentState::Match,
    })
}

fn append_stream_score_badge(
    object: &mut JsonMap<String, Value>,
    label: &str,
    detail: String,
    score: f64,
) {
    let badges = object
        .entry("scoreBadges".to_string())
        .or_insert_with(|| Value::Array(Vec::new()));
    if !badges.is_array() {
        *badges = Value::Array(Vec::new());
    }
    let Some(items) = badges.as_array_mut() else {
        return;
    };
    let exists = items.iter().any(|item| {
        item.get("label")
            .and_then(Value::as_str)
            .is_some_and(|value| value.eq_ignore_ascii_case(label))
    });
    if !exists {
        items.push(json!({
            "label": label,
            "detail": detail,
            "score": score,
        }));
    }
}

fn media_type_from_request(value: &str) -> Option<crate::db::models::MediaType> {
    match value.trim().to_ascii_lowercase().as_str() {
        "movie" | "movies" => Some(crate::db::models::MediaType::Movie),
        "series" | "tv" | "show" | "shows" => Some(crate::db::models::MediaType::Series),
        "anime" => Some(crate::db::models::MediaType::Anime),
        _ => None,
    }
}

fn validate_upstream_stream_candidates(values: Vec<Value>) -> (Vec<Value>, Vec<String>) {
    let mut candidates = Vec::new();
    let mut warnings = Vec::new();
    for (index, value) in values.into_iter().enumerate() {
        match validate_stream_candidate_value(value) {
            Ok((candidate, candidate_warnings)) => {
                candidates.push(candidate);
                warnings.extend(
                    candidate_warnings
                        .into_iter()
                        .map(|warning| format!("candidate[{index}]: {warning}")),
                );
            }
            Err(err) => warnings.push(format!("candidate[{index}] rejected: {err}")),
        }
    }
    (candidates, warnings)
}

pub(crate) fn validate_stream_candidate_for_broker(value: Value) -> Result<(Value, Vec<String>)> {
    validate_stream_candidate_value(value)
}

fn validate_stream_candidate_value(value: Value) -> Result<(Value, Vec<String>)> {
    let mut object = match value {
        Value::Object(object) => object,
        _ => bail!("stream candidate must be an object"),
    };
    let mut warnings = Vec::new();

    let candidate_kind = normalize_required_object_string(&mut object, "candidateKind")?;
    if !candidate_kind.eq_ignore_ascii_case("stream") {
        bail!("candidateKind must be stream");
    }
    object.insert(
        "candidateKind".to_string(),
        Value::String("stream".to_string()),
    );

    normalize_required_object_string(&mut object, "id")?;
    normalize_required_object_string(&mut object, "title")?;
    let source = normalize_required_object_string(&mut object, "source")?;
    validate_stream_source_reference(&source).context("invalid source")?;

    let source_kind = normalize_required_object_string(&mut object, "sourceKind")?;
    let source_kind = source_kind.to_ascii_lowercase();
    if !matches!(source_kind.as_str(), "http_file" | "http_stream") {
        bail!("sourceKind must be http_file or http_stream");
    }
    object.insert("sourceKind".to_string(), Value::String(source_kind));

    let supported_routes = normalize_required_string_array_field(&mut object, "supportedRoutes")?;
    if !supported_routes
        .iter()
        .any(|route| route.eq_ignore_ascii_case(HTTP_STREAM_DEFAULT_LOGICAL_ID))
    {
        bail!("supportedRoutes must include {HTTP_STREAM_DEFAULT_LOGICAL_ID}");
    }
    let default_route = normalize_required_object_string(&mut object, "defaultRoute")?;
    if !default_route.eq_ignore_ascii_case(HTTP_STREAM_DEFAULT_LOGICAL_ID) {
        bail!("defaultRoute must be {HTTP_STREAM_DEFAULT_LOGICAL_ID}");
    }
    object.insert(
        "defaultRoute".to_string(),
        Value::String(HTTP_STREAM_DEFAULT_LOGICAL_ID.to_string()),
    );

    validate_stream_target_evidence(&mut object)?;
    validate_stream_delivery(&mut object)?;
    validate_stream_source_module(&mut object)?;
    sanitize_stream_candidate_raw(&mut object, &mut warnings)?;

    let candidate = Value::Object(object);
    let candidate_size = json_size_bytes(&candidate)?;
    if candidate_size > STREAM_CANDIDATE_MAX_BYTES {
        bail!("stream candidate exceeds {STREAM_CANDIDATE_MAX_BYTES} bytes");
    }
    Ok((candidate, warnings))
}

fn validate_stream_target_evidence(object: &mut JsonMap<String, Value>) -> Result<()> {
    let target = object
        .get_mut("targetEvidence")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| anyhow!("targetEvidence is required"))?;
    let media_type = normalize_required_nested_string(target, "targetEvidence", "mediaType")?;
    if normalize_candidate_media_type(&media_type).is_none() {
        bail!("targetEvidence.mediaType is unsupported");
    }
    normalize_required_nested_string(target, "targetEvidence", "targetKey")?;
    normalize_required_nested_string(target, "targetEvidence", "confidence")?;
    let reasons = normalize_required_nested_string_array(target, "targetEvidence", "reasons")?;
    if reasons.is_empty() {
        bail!("targetEvidence.reasons must include at least one reason");
    }
    Ok(())
}

fn validate_stream_delivery(object: &mut JsonMap<String, Value>) -> Result<()> {
    let delivery = object
        .get_mut("delivery")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| anyhow!("delivery is required"))?;
    let stream_type =
        normalize_required_nested_string(delivery, "delivery", "streamType")?.to_ascii_lowercase();
    if !matches!(stream_type.as_str(), "direct_file" | "hls" | "dash") {
        bail!("delivery.streamType is unsupported");
    }
    delivery.insert("streamType".to_string(), Value::String(stream_type));

    let url = normalize_optional_nested_string(delivery, "url");
    let resolve_handle = normalize_optional_nested_string(delivery, "resolveHandle");
    if url.is_none() && resolve_handle.is_none() {
        bail!("delivery must include url or resolveHandle");
    }
    if let Some(url) = url.as_deref() {
        validate_safe_http_url(url).context("delivery.url is unsafe")?;
    }
    if let Some(referer) = normalize_optional_nested_string(delivery, "referer") {
        validate_safe_http_url(&referer).context("delivery.referer is unsafe")?;
    }
    validate_stream_delivery_headers(delivery)?;
    Ok(())
}

fn validate_stream_delivery_headers(delivery: &mut JsonMap<String, Value>) -> Result<()> {
    let Some(headers_value) = delivery.get_mut("headers") else {
        return Ok(());
    };
    if headers_value.is_null() {
        delivery.remove("headers");
        return Ok(());
    }
    let headers = headers_value
        .as_object_mut()
        .ok_or_else(|| anyhow!("delivery.headers must be an object"))?;
    if headers.len() > STREAM_CANDIDATE_MAX_HEADERS {
        bail!("delivery.headers includes too many headers");
    }
    let mut normalized = JsonMap::new();
    for (name, value) in std::mem::take(headers) {
        let name = name.trim();
        if name.is_empty() {
            bail!("delivery.headers includes an empty header name");
        }
        let header_name = HeaderName::from_bytes(name.as_bytes())
            .with_context(|| format!("delivery.headers.{name} has an invalid header name"))?;
        let value = value
            .as_str()
            .ok_or_else(|| anyhow!("delivery.headers.{name} must be a string"))?
            .trim()
            .to_string();
        HeaderValue::from_str(&value)
            .with_context(|| format!("delivery.headers.{name} has an invalid header value"))?;
        normalized.insert(header_name.as_str().to_string(), Value::String(value));
    }
    *headers = normalized;
    Ok(())
}

fn validate_stream_source_module(object: &mut JsonMap<String, Value>) -> Result<()> {
    let module = object
        .get_mut("sourceModule")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| anyhow!("sourceModule is required"))?;
    normalize_required_nested_string(module, "sourceModule", "id")?;
    normalize_required_nested_string(module, "sourceModule", "name")?;
    normalize_required_nested_string(module, "sourceModule", "type")?;
    Ok(())
}

fn sanitize_stream_candidate_raw(
    object: &mut JsonMap<String, Value>,
    warnings: &mut Vec<String>,
) -> Result<()> {
    let Some(raw) = object.remove("raw") else {
        return Ok(());
    };
    if raw.is_null() {
        return Ok(());
    }
    let redacted = redact_sensitive_value(raw);
    let size = json_size_bytes(&redacted)?;
    if size > STREAM_CANDIDATE_RAW_MAX_BYTES {
        object.insert(
            "raw".to_string(),
            json!({
                "truncated": true,
                "reason": "stream candidate raw evidence exceeded size limit",
                "maxBytes": STREAM_CANDIDATE_RAW_MAX_BYTES
            }),
        );
        warnings.push(format!(
            "raw evidence exceeded {} bytes and was truncated",
            STREAM_CANDIDATE_RAW_MAX_BYTES
        ));
    } else {
        object.insert("raw".to_string(), redacted);
    }
    Ok(())
}

fn validate_stream_source_reference(source: &str) -> Result<()> {
    let Ok(url) = Url::parse(source) else {
        return Ok(());
    };
    match url.scheme().to_ascii_lowercase().as_str() {
        "http" | "https" => {
            validate_safe_http_url(source)?;
            Ok(())
        }
        "provider" | "stream-provider" => Ok(()),
        scheme => bail!("source URL scheme '{scheme}' is not allowed"),
    }
}

pub(crate) fn validate_safe_http_url(value: &str) -> Result<Url> {
    let url = Url::parse(value).context("parsing URL")?;
    if !matches!(url.scheme(), "http" | "https") {
        bail!("URL scheme must be http or https");
    }
    if !url.username().is_empty() || url.password().is_some() {
        bail!("URL must not include credentials");
    }
    let host = url
        .host_str()
        .map(str::trim)
        .filter(|host| !host.is_empty())
        .ok_or_else(|| anyhow!("URL host is required"))?;
    validate_safe_stream_host(host)?;
    Ok(url)
}

fn validate_safe_stream_host(host: &str) -> Result<()> {
    let normalized = host.trim().trim_end_matches('.').to_ascii_lowercase();
    if normalized.is_empty() {
        bail!("URL host is required");
    }
    if normalized == "localhost"
        || normalized.ends_with(".localhost")
        || normalized.ends_with(".local")
        || normalized.ends_with(".internal")
    {
        bail!("local or internal hostnames are not allowed");
    }
    if !normalized.contains('.') && normalized.parse::<IpAddr>().is_err() {
        bail!("single-label hostnames are not allowed");
    }
    if let Ok(ip) = normalized.parse::<IpAddr>() {
        validate_safe_stream_ip(ip)?;
    }
    Ok(())
}

fn validate_safe_stream_ip(ip: IpAddr) -> Result<()> {
    let unsafe_ip = match ip {
        IpAddr::V4(value) => {
            value.is_loopback()
                || value.is_private()
                || value.is_link_local()
                || value.is_unspecified()
                || value.is_broadcast()
                || value.is_multicast()
        }
        IpAddr::V6(value) => {
            value.is_loopback()
                || value.is_unspecified()
                || value.is_multicast()
                || value.is_unicast_link_local()
                || value.is_unique_local()
        }
    };
    if unsafe_ip {
        bail!("private, local, link-local, multicast, and unspecified IPs are not allowed");
    }
    Ok(())
}

fn normalize_required_object_string(
    object: &mut JsonMap<String, Value>,
    field: &str,
) -> Result<String> {
    normalize_required_nested_string(object, "", field)
}

fn normalize_required_nested_string(
    object: &mut JsonMap<String, Value>,
    parent: &str,
    field: &str,
) -> Result<String> {
    let value = object
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("{}{}{} is required", parent, dot_prefix(parent), field))?
        .to_string();
    object.insert(field.to_string(), Value::String(value.clone()));
    Ok(value)
}

fn normalize_optional_nested_string(
    object: &mut JsonMap<String, Value>,
    field: &str,
) -> Option<String> {
    let value = object
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string);
    match value.as_ref() {
        Some(value) => {
            object.insert(field.to_string(), Value::String(value.clone()));
        }
        None => {
            object.remove(field);
        }
    }
    value
}

fn normalize_required_string_array_field(
    object: &mut JsonMap<String, Value>,
    field: &str,
) -> Result<Vec<String>> {
    normalize_required_nested_string_array(object, "", field)
}

fn normalize_required_nested_string_array(
    object: &mut JsonMap<String, Value>,
    parent: &str,
    field: &str,
) -> Result<Vec<String>> {
    let values = object
        .get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("{}{}{} must be an array", parent, dot_prefix(parent), field))?;
    let values = values
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToString::to_string)
                .ok_or_else(|| {
                    anyhow!(
                        "{}{}{} must contain strings",
                        parent,
                        dot_prefix(parent),
                        field
                    )
                })
        })
        .collect::<Result<Vec<_>>>()?;
    object.insert(field.to_string(), json!(values));
    Ok(values)
}

fn dot_prefix(parent: &str) -> &'static str {
    if parent.is_empty() { "" } else { "." }
}

fn json_size_bytes(value: &Value) -> Result<usize> {
    serde_json::to_vec(value)
        .map(|bytes| bytes.len())
        .context("serializing JSON for size check")
}

async fn available_candidate_providers(
    store: &ExtensionStore<'_>,
    media_type: Option<&str>,
) -> Result<Vec<CandidateProviderSelection>> {
    available_source_providers_for_capability(
        store,
        media_type,
        ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY,
    )
    .await
}

async fn available_stream_candidate_providers(
    store: &ExtensionStore<'_>,
    media_type: Option<&str>,
) -> Result<Vec<CandidateProviderSelection>> {
    available_source_providers_for_capability(
        store,
        media_type,
        ACQUISITION_STREAM_CANDIDATE_PROVIDER_CAPABILITY,
    )
    .await
}

async fn available_source_providers_for_capability(
    store: &ExtensionStore<'_>,
    media_type: Option<&str>,
    capability: &str,
) -> Result<Vec<CandidateProviderSelection>> {
    let mut providers = Vec::new();
    for detail in store.list_provider_details().await? {
        if detail.provider.capability != capability {
            continue;
        }
        if detail.provider.health_state != ProviderHealthState::Healthy {
            continue;
        }
        let Some(extension) = store.get_extension(&detail.extension_id).await? else {
            continue;
        };
        if !extension.enabled {
            continue;
        }
        let Some(instance) = store.get_instance(detail.provider.instance_id).await? else {
            continue;
        };
        if !instance.enabled {
            continue;
        }
        if detail.provider.endpoint_json.is_none() {
            continue;
        }
        let (media_types, actions) = provider_scope(&detail.provider);
        if let Some(media_type) = media_type {
            if !media_types.is_empty()
                && !media_types
                    .iter()
                    .any(|item| candidate_media_type_matches(item, media_type))
            {
                continue;
            }
        }
        providers.push(CandidateProviderSelection {
            summary: CandidateProviderSummary {
                provider_id: detail.provider.provider_id,
                extension_id: extension.extension_id.clone(),
                extension_name: extension.name.clone(),
                instance_id: instance.instance_id,
                instance_name: instance.instance_name.clone(),
                capability: detail.provider.capability.clone(),
                implementation: detail.provider.implementation.clone(),
                health_state: detail.provider.health_state,
                media_types,
                actions,
            },
            provider: detail.provider,
            extension,
            instance,
        });
    }
    providers.sort_by_key(|item| {
        (
            item.summary.extension_name.clone(),
            item.summary.instance_name.clone(),
            item.summary.provider_id,
        )
    });
    Ok(providers)
}

async fn select_candidate_provider(
    store: &ExtensionStore<'_>,
    provider_id: Option<Uuid>,
    media_type: Option<&str>,
) -> Result<CandidateProviderSelection> {
    let providers = available_candidate_providers(store, media_type).await?;
    if let Some(provider_id) = provider_id {
        return providers
            .into_iter()
            .find(|item| item.summary.provider_id == provider_id)
            .ok_or_else(|| anyhow!("candidate provider '{provider_id}' is not available"));
    }
    providers
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("no acquisition candidate provider is available"))
}

async fn invoke_candidate_provider(
    store: &ExtensionStore<'_>,
    selected: &CandidateProviderSelection,
    request: &CandidateSearchRequest,
) -> Result<CandidateProviderUpstreamResponse> {
    let endpoint_json = selected
        .provider
        .endpoint_json
        .clone()
        .ok_or_else(|| anyhow!("candidate provider endpoint is missing"))?;
    let endpoint: ProviderEndpoint =
        serde_json::from_value(endpoint_json).context("parsing candidate provider endpoint")?;
    let base_url =
        resolve_control_provider_transport_base_url(selected.instance.instance_id, &endpoint)
            .await?;
    invoke_candidate_provider_at_base_url(store, &base_url, selected, request).await
}

async fn invoke_candidate_provider_at_base_url(
    store: &ExtensionStore<'_>,
    base_url: &str,
    selected: &CandidateProviderSelection,
    request: &CandidateSearchRequest,
) -> Result<CandidateProviderUpstreamResponse> {
    let search_url = candidate_provider_search_url(&base_url)?;
    let provider_config = candidate_provider_invocation_config_for_store(
        store,
        &selected.extension,
        &selected.instance,
    )
    .await?;
    let invocation = CandidateProviderInvocation {
        schema_version: CANDIDATE_PROVIDER_SCHEMA_VERSION,
        request,
        provider: CandidateProviderInvocationContext {
            provider_id: selected.provider.provider_id,
            extension_id: &selected.extension.extension_id,
            instance_id: selected.instance.instance_id,
            implementation: selected.provider.implementation.as_deref(),
            config: provider_config,
        },
    };
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(CANDIDATE_PROVIDER_TIMEOUT_SECONDS))
        .build()
        .context("building candidate provider HTTP client")?;
    let response = client
        .post(search_url.clone())
        .json(&invocation)
        .send()
        .await
        .with_context(|| format!("calling candidate provider at {search_url}"))?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        bail!("candidate provider returned {status}: {body}");
    }
    response
        .json::<CandidateProviderUpstreamResponse>()
        .await
        .context("parsing candidate provider response")
}

async fn invoke_stream_candidate_provider(
    store: &ExtensionStore<'_>,
    selected: &CandidateProviderSelection,
    request: &StreamCandidateSearchRequest,
) -> Result<StreamCandidateProviderUpstreamResponse> {
    let endpoint_json = selected
        .provider
        .endpoint_json
        .clone()
        .ok_or_else(|| anyhow!("stream candidate provider endpoint is missing"))?;
    let endpoint: ProviderEndpoint = serde_json::from_value(endpoint_json)
        .context("parsing stream candidate provider endpoint")?;
    let base_url =
        resolve_control_provider_transport_base_url(selected.instance.instance_id, &endpoint)
            .await?;
    invoke_stream_candidate_provider_at_base_url(store, &base_url, selected, request).await
}

async fn invoke_stream_candidate_provider_at_base_url(
    store: &ExtensionStore<'_>,
    base_url: &str,
    selected: &CandidateProviderSelection,
    request: &StreamCandidateSearchRequest,
) -> Result<StreamCandidateProviderUpstreamResponse> {
    let search_url = candidate_provider_search_url(&base_url)?;
    let provider_config = candidate_provider_invocation_config_for_store(
        store,
        &selected.extension,
        &selected.instance,
    )
    .await?;
    let invocation = StreamCandidateProviderInvocation {
        schema_version: CANDIDATE_PROVIDER_SCHEMA_VERSION,
        request,
        provider: CandidateProviderInvocationContext {
            provider_id: selected.provider.provider_id,
            extension_id: &selected.extension.extension_id,
            instance_id: selected.instance.instance_id,
            implementation: selected.provider.implementation.as_deref(),
            config: provider_config,
        },
    };
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(CANDIDATE_PROVIDER_TIMEOUT_SECONDS))
        .build()
        .context("building stream candidate provider HTTP client")?;
    let response = client
        .post(search_url.clone())
        .json(&invocation)
        .send()
        .await
        .with_context(|| format!("calling stream candidate provider at {search_url}"))?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        bail!(
            "stream candidate provider returned {status}: {}",
            truncate_diagnostic(&body, 1024)
        );
    }
    parse_bounded_stream_candidate_provider_response(response).await
}

async fn parse_bounded_stream_candidate_provider_response(
    response: reqwest::Response,
) -> Result<StreamCandidateProviderUpstreamResponse> {
    if let Some(length) = response.content_length() {
        if length > STREAM_CANDIDATE_PROVIDER_RESPONSE_MAX_BYTES {
            bail!(
                "stream candidate provider response exceeds {} bytes",
                STREAM_CANDIDATE_PROVIDER_RESPONSE_MAX_BYTES
            );
        }
    }
    let bytes = response
        .bytes()
        .await
        .context("reading stream candidate provider response")?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > STREAM_CANDIDATE_PROVIDER_RESPONSE_MAX_BYTES
    {
        bail!(
            "stream candidate provider response exceeds {} bytes",
            STREAM_CANDIDATE_PROVIDER_RESPONSE_MAX_BYTES
        );
    }
    serde_json::from_slice::<StreamCandidateProviderUpstreamResponse>(&bytes)
        .context("parsing stream candidate provider response")
}

fn truncate_diagnostic(value: &str, max_chars: usize) -> String {
    let mut out = String::new();
    for ch in value.chars().take(max_chars) {
        out.push(ch);
    }
    if value.chars().count() > max_chars {
        out.push_str("...");
    }
    out
}

async fn candidate_route_options(
    pool: &sqlx::AnyPool,
    store: &ExtensionStore<'_>,
    extension_id: &str,
) -> Result<Vec<CandidateRouteOption>> {
    let routes = list_acquisition_routes(pool, store).await?;
    Ok(routes
        .routes
        .into_iter()
        .filter(|route| route.owner_id == extension_id)
        .filter(|route| {
            route.logical_id == DEBRID_DEFAULT_LOGICAL_ID
                || route.logical_id == TORRENT_DEFAULT_LOGICAL_ID
        })
        .map(route_option_from_record)
        .collect())
}

fn route_option_from_record(route: DownloadBrokerRouteRecord) -> CandidateRouteOption {
    CandidateRouteOption {
        label: route_label(&route.logical_id).to_string(),
        available: route.blocker.is_none() && route.selected_provider_id.is_some(),
        logical_id: route.logical_id,
        selected_provider_id: route.selected_provider_id,
        selected_extension_id: route.selected_extension_id,
        blocker: route.blocker,
    }
}

fn route_label(logical_id: &str) -> &'static str {
    match logical_id {
        DEBRID_DEFAULT_LOGICAL_ID => "Direct HTTPS debrid download",
        TORRENT_DEFAULT_LOGICAL_ID => "Protected/local torrent download",
        _ => "Download route",
    }
}

fn candidate_provider_search_url(base_url: &str) -> Result<Url> {
    let mut base = Url::parse(base_url).context("parsing candidate provider base URL")?;
    let mut path = base.path().trim_end_matches('/').to_string();
    if path.is_empty() {
        path.push('/');
    } else {
        path.push('/');
    }
    base.set_path(&path);
    base.join(CANDIDATE_PROVIDER_SEARCH_PATH)
        .context("building candidate provider search URL")
}

fn validate_candidate_search_request(request: &CandidateSearchRequest) -> Result<()> {
    if request.media_type.trim().is_empty() {
        bail!("mediaType is required");
    }
    if request.title.trim().is_empty() {
        bail!("title is required");
    }
    if let Some(limit) = request.limit {
        if limit == 0 {
            bail!("limit must be greater than zero");
        }
    }
    Ok(())
}

pub(crate) fn normalize_acquisition_candidate(
    mut candidate: AcquisitionCandidate,
) -> Result<AcquisitionCandidate> {
    candidate.title = candidate.title.trim().to_string();
    candidate.source = candidate.source.trim().to_string();
    candidate.source_kind = candidate.source_kind.trim().to_ascii_lowercase();
    if candidate.title.is_empty() {
        bail!("candidate title is required");
    }
    if candidate.source.is_empty() {
        bail!("candidate source is required");
    }
    if candidate.source_kind.is_empty() {
        bail!("candidate sourceKind is required");
    }
    candidate.supported_routes = candidate
        .supported_routes
        .into_iter()
        .map(|route| route.trim().to_string())
        .filter(|route| !route.is_empty())
        .collect();
    candidate.files = candidate
        .files
        .into_iter()
        .filter_map(normalize_candidate_file)
        .collect();
    candidate.raw = candidate.raw.map(redact_sensitive_value);
    enrich_torrent_candidate_health_evidence(&mut candidate);
    Ok(candidate)
}

pub(crate) fn acquisition_candidate_tracker_count(candidate: &AcquisitionCandidate) -> usize {
    let mut count = magnet_tracker_count(&candidate.source);
    for path in [
        "/parsedHints/trackerCount",
        "/serverEvidence/torrentHealth/trackerCount",
    ] {
        if let Some(raw_count) = candidate
            .raw
            .as_ref()
            .and_then(|raw| raw.pointer(path))
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
        {
            count = count.max(raw_count);
        }
    }
    count
}

fn enrich_torrent_candidate_health_evidence(candidate: &mut AcquisitionCandidate) {
    if !candidate.source_kind.eq_ignore_ascii_case("magnet") {
        return;
    }
    let tracker_count = acquisition_candidate_tracker_count(candidate);
    let seeder_state = match candidate.seeders {
        Some(0) => "zero",
        Some(1..=4) => "very_low",
        Some(5..=14) => "low",
        Some(15..) => "reported",
        None => "unknown",
    };
    upsert_candidate_server_evidence(
        candidate,
        "torrentHealth",
        json!({
            "policyVersion": "asr8-candidate-quality-v1",
            "sourceKind": candidate.source_kind.as_str(),
            "cachedDebrid": candidate.cached_debrid,
            "trackerCount": tracker_count,
            "seeders": candidate.seeders,
            "seederState": seeder_state,
            "seedersAreSourceHints": true,
            "liveDownloaderEvidenceOverridesSourceHints": true,
        }),
    );

    ensure_score_badge(
        candidate,
        "Tracker evidence",
        format!(
            "{} tracker URL{} detected before submission.",
            tracker_count,
            if tracker_count == 1 { "" } else { "s" }
        ),
        Some(match tracker_count {
            3.. => 0.05,
            1..=2 => 0.02,
            _ => -0.08,
        }),
    );

    if candidate.cached_debrid == Some(true) {
        return;
    }

    let mut weak_reasons = Vec::new();
    if tracker_count == 0 {
        weak_reasons.push("no tracker URLs".to_string());
    }
    match candidate.seeders {
        Some(0) => weak_reasons.push("zero reported seeders".to_string()),
        Some(1..=4) => weak_reasons.push("very low reported seeders".to_string()),
        None => weak_reasons.push("unknown reported seeders".to_string()),
        Some(_) => {}
    }
    if weak_reasons.is_empty() {
        return;
    }
    ensure_score_badge(
        candidate,
        "Weak swarm",
        format!(
            "Uncached magnet has {}; source seeder hints are advisory until live downloader evidence is available.",
            weak_reasons.join(" and ")
        ),
        Some(-0.12),
    );
}

fn upsert_candidate_server_evidence(candidate: &mut AcquisitionCandidate, key: &str, value: Value) {
    let mut root = match candidate.raw.take() {
        Some(Value::Object(object)) => object,
        Some(previous_raw) => {
            let mut object = JsonMap::new();
            object.insert("sourceRaw".to_string(), previous_raw);
            object
        }
        None => JsonMap::new(),
    };
    let server_evidence = root
        .entry("serverEvidence".to_string())
        .or_insert_with(|| Value::Object(JsonMap::new()));
    if !server_evidence.is_object() {
        *server_evidence = Value::Object(JsonMap::new());
    }
    if let Some(object) = server_evidence.as_object_mut() {
        object.insert(key.to_string(), value);
    }
    candidate.raw = Some(Value::Object(root));
}

fn upsert_stream_candidate_server_evidence(candidate: &mut Value, key: &str, value: Value) {
    let Some(candidate_object) = candidate.as_object_mut() else {
        return;
    };
    let mut raw = match candidate_object.remove("raw") {
        Some(Value::Object(object)) => object,
        Some(Value::Null) | None => JsonMap::new(),
        Some(previous_raw) => {
            let mut object = JsonMap::new();
            object.insert("sourceRaw".to_string(), previous_raw);
            object
        }
    };
    let server_evidence = raw
        .entry("serverEvidence".to_string())
        .or_insert_with(|| Value::Object(JsonMap::new()));
    if !server_evidence.is_object() {
        *server_evidence = Value::Object(JsonMap::new());
    }
    if let Some(object) = server_evidence.as_object_mut() {
        object.insert(key.to_string(), value);
    }
    candidate_object.insert("raw".to_string(), Value::Object(raw));
}

fn ensure_score_badge(
    candidate: &mut AcquisitionCandidate,
    label: &str,
    detail: String,
    score: Option<f64>,
) {
    if candidate
        .score_badges
        .iter()
        .any(|badge| badge.label.eq_ignore_ascii_case(label))
    {
        return;
    }
    candidate.score_badges.push(CandidateScoreBadge {
        label: label.to_string(),
        detail: Some(detail),
        score,
    });
}

fn magnet_tracker_count(source: &str) -> usize {
    let Some((scheme, query)) = source.split_once('?') else {
        return 0;
    };
    if !scheme.eq_ignore_ascii_case("magnet:") {
        return 0;
    }
    query
        .split('&')
        .filter_map(|pair| pair.split_once('=').map(|(key, _)| key).or(Some(pair)))
        .filter(|key| {
            urlencoding::decode(key)
                .map(|decoded| decoded.eq_ignore_ascii_case("tr"))
                .unwrap_or(false)
        })
        .count()
}

fn normalize_upstream_candidates(values: Vec<Value>) -> (Vec<AcquisitionCandidate>, Vec<String>) {
    let mut candidates = Vec::new();
    let mut warnings = Vec::new();
    for (index, value) in values.into_iter().enumerate() {
        match serde_json::from_value::<AcquisitionCandidate>(value)
            .context("deserializing acquisition candidate")
            .and_then(normalize_acquisition_candidate)
        {
            Ok(candidate) => candidates.push(candidate),
            Err(err) => warnings.push(format!("candidate[{index}] rejected: {err}")),
        }
    }
    (candidates, warnings)
}

pub(crate) fn candidate_provider_invocation_config(
    extension: &Extension,
    instance: &ExtensionInstance,
) -> Result<Option<Value>> {
    let manifest = serde_json::from_value::<ExtensionManifest>(extension.manifest_json.clone())
        .with_context(|| format!("parsing extension manifest '{}'", extension.extension_id))?;
    let mut allowed_keys = manifest
        .control_surface
        .as_ref()
        .map(|surface| {
            surface
                .owned_settings
                .iter()
                .filter(|setting| {
                    !setting.secret
                        && setting
                            .storage
                            .r#type
                            .trim()
                            .eq_ignore_ascii_case("instance_setting")
                        && !is_sensitive_key(&setting.storage.key)
                })
                .map(|setting| setting.storage.key.clone())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let config_object = instance.config_json.as_ref().and_then(Value::as_object);
    if allowed_keys.is_empty() {
        allowed_keys = config_object
            .map(|object| {
                object
                    .keys()
                    .filter(|key| is_public_instance_config_key(key))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
    }
    allowed_keys.sort();
    allowed_keys.dedup();

    let mut filtered = JsonMap::new();
    if let Some(control_surface) = manifest.control_surface.as_ref() {
        for setting in &control_surface.owned_settings {
            if setting.secret
                || !setting
                    .storage
                    .r#type
                    .trim()
                    .eq_ignore_ascii_case("instance_setting")
                || is_sensitive_key(&setting.storage.key)
                || !allowed_keys.iter().any(|key| key == &setting.storage.key)
            {
                continue;
            }
            let Some(default) = setting.default.as_ref() else {
                continue;
            };
            if !default.is_null() {
                filtered.insert(setting.storage.key.clone(), default.clone());
            }
        }
    }
    for key in allowed_keys {
        let Some(value) = config_object.and_then(|object| object.get(&key)) else {
            continue;
        };
        if value.is_null() {
            continue;
        }
        filtered.insert(key, redact_sensitive_value(value.clone()));
    }

    Ok((!filtered.is_empty()).then_some(Value::Object(filtered)))
}

pub(crate) async fn candidate_provider_invocation_config_for_store(
    store: &ExtensionStore<'_>,
    extension: &Extension,
    instance: &ExtensionInstance,
) -> Result<Option<Value>> {
    let base = candidate_provider_invocation_config(extension, instance)?;
    match extension.extension_id.as_str() {
        CLOUDSTREAM_COMPAT_EXTENSION_ID => {
            cloudstream_candidate_provider_invocation_config(store, instance, base).await
        }
        extension_id if is_prism_extension_id(extension_id) => {
            nuvio_candidate_provider_invocation_config(store, instance, base).await
        }
        _ => Ok(base),
    }
}

async fn cloudstream_candidate_provider_invocation_config(
    store: &ExtensionStore<'_>,
    instance: &ExtensionInstance,
    base: Option<Value>,
) -> Result<Option<Value>> {
    let modules = store
        .list_source_modules(Some(instance.instance_id), None)
        .await?;
    if modules.is_empty() {
        return Ok(base);
    }

    let registries = store
        .list_source_registries(Some(instance.instance_id))
        .await?;
    let registry_by_id = registries
        .into_iter()
        .map(|registry| (registry.registry_id, registry))
        .collect::<BTreeMap<_, _>>();
    let module_id_by_source_uuid = modules
        .iter()
        .map(|module| {
            (
                module.source_module_id,
                cloudstream_invocation_module_id(module),
            )
        })
        .collect::<BTreeMap<_, _>>();

    let mut object = base
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
    if let Some(policy) = instance
        .config_json
        .as_ref()
        .and_then(|config| config.get("sourcePackPolicy"))
        .cloned()
    {
        object.insert(
            "sourcePackPolicy".to_string(),
            redact_sensitive_value(policy),
        );
    }
    let explicit_modules = cloudstream_explicit_source_modules_from_config(&object);
    let mut projected = Vec::new();
    let mut sorted_modules = modules;
    sorted_modules.sort_by(|left, right| left.module_key.cmp(&right.module_key));
    for module in &sorted_modules {
        let Some(registry) = registry_by_id.get(&module.registry_id) else {
            continue;
        };
        projected.push(
            cloudstream_source_module_invocation_descriptor(
                store,
                module,
                registry,
                &module_id_by_source_uuid,
            )
            .await?,
        );
    }

    let mut seen = BTreeSet::new();
    let mut merged = Vec::new();
    for module in projected.into_iter().chain(explicit_modules) {
        let key = cloudstream_invocation_module_key(&module);
        if seen.insert(key) {
            merged.push(module);
        }
    }
    if !merged.is_empty() {
        object.insert("sourceModules".to_string(), Value::Array(merged));
    }
    Ok((!object.is_empty()).then_some(Value::Object(object)))
}

async fn cloudstream_source_module_invocation_descriptor(
    store: &ExtensionStore<'_>,
    module: &ExtensionSourceModule,
    registry: &ExtensionSourceRegistry,
    module_id_by_source_uuid: &BTreeMap<Uuid, String>,
) -> Result<Value> {
    let metadata = module.metadata_json.as_ref();
    let cloudstream = metadata
        .and_then(|value| value.get("cloudstream"))
        .and_then(Value::as_object);
    let active_version = active_source_module_version(store, module).await?;
    let active_version_metadata = active_version
        .as_ref()
        .and_then(|version| version.metadata_json.as_ref());
    let mut descriptor = JsonMap::new();
    let module_id = cloudstream_invocation_module_id(module);
    descriptor.insert("id".to_string(), json!(module_id));
    descriptor.insert("name".to_string(), json!(module.display_name));
    descriptor.insert("type".to_string(), json!("cloudstream"));
    descriptor.insert(
        "adapter".to_string(),
        json!(
            cloudstream
                .and_then(|value| value.get("adapter"))
                .and_then(Value::as_str)
                .unwrap_or("cloudstream_plugin_v1")
        ),
    );
    descriptor.insert(
        "enabled".to_string(),
        json!(registry.enabled && module.enabled),
    );
    descriptor.insert("installed".to_string(), json!(module.installed));
    descriptor.insert(
        "requiresAccount".to_string(),
        json!(module.account_required),
    );
    descriptor.insert(
        "accountConfigured".to_string(),
        json!(!module.account_required),
    );
    descriptor.insert("registryKey".to_string(), json!(registry.registry_key));
    descriptor.insert("registryType".to_string(), json!(registry.registry_type));
    descriptor.insert("trustClass".to_string(), json!(registry.trust_class));
    descriptor.insert(
        "trustedForExecutableUpdates".to_string(),
        json!(registry.trusted_for_executable_updates),
    );
    descriptor.insert(
        "healthState".to_string(),
        json!(if registry.enabled && module.enabled {
            module.health_state.as_str()
        } else {
            "disabled"
        }),
    );
    if let Some(value) = module.media_types_json.clone() {
        descriptor.insert("mediaTypes".to_string(), value);
    }
    if let Some(value) = module.language_tags_json.clone() {
        descriptor.insert("languageTags".to_string(), value);
    }
    if let Some(value) = module.region_tags_json.clone() {
        descriptor.insert("regionTags".to_string(), value);
    }
    if let Some(value) = module.source_domains_json.clone() {
        descriptor.insert("sourceDomains".to_string(), value);
    }
    if let Some(value) = module.active_version.as_deref() {
        descriptor.insert("activeVersion".to_string(), json!(value));
        descriptor.insert("version".to_string(), json!(value));
    }
    if let Some(value) = module.rollback_version.as_deref() {
        descriptor.insert("rollbackVersion".to_string(), json!(value));
    }
    if let Some(value) = module.pinned_version.as_deref() {
        descriptor.insert("pinnedVersion".to_string(), json!(value));
    }
    if let Some(value) = module.replacement_recommendation_key.as_deref() {
        descriptor.insert("replacementRecommendationKey".to_string(), json!(value));
    }
    if let Some(value) = module.last_error.as_deref() {
        descriptor.insert("lastError".to_string(), json!(value));
    }
    if module.unsupported {
        descriptor.insert(
            "unsupportedReason".to_string(),
            json!(
                module
                    .unsupported_reason
                    .as_deref()
                    .unwrap_or("unsupported by Elixir source registry")
            ),
        );
    }
    if let Some(value) = cloudstream.and_then(|value| value.get("moduleId")).cloned() {
        descriptor.insert("moduleId".to_string(), value);
    }
    if let Some(value) = cloudstream
        .and_then(|value| value.get("internalName"))
        .cloned()
    {
        descriptor.insert("internalName".to_string(), value);
    }
    if let Some(value) = cloudstream
        .and_then(|value| value.get("pluginPackage"))
        .cloned()
    {
        descriptor.insert("pluginPackage".to_string(), value);
    } else if let Some(value) = module.plugin_package.as_deref() {
        descriptor.insert("pluginPackage".to_string(), json!(value));
    }
    if let Some(value) = cloudstream
        .and_then(|value| value.get("providerClass"))
        .cloned()
    {
        descriptor.insert("providerClass".to_string(), value);
    }
    if let Some(value) = cloudstream
        .and_then(|value| value.get("pluginJarPath"))
        .cloned()
        .or_else(|| {
            active_version_metadata
                .as_ref()
                .and_then(|metadata| metadata.get("cloudstream"))
                .and_then(|value| value.get("pluginJarPath"))
                .cloned()
        })
        .or_else(|| {
            active_version_metadata
                .as_ref()
                .and_then(|metadata| metadata.get("artifact"))
                .and_then(|value| value.get("containerPath"))
                .cloned()
        })
    {
        descriptor.insert("pluginJarPath".to_string(), value);
    }
    if let Some(value) = cloudstream
        .and_then(|value| value.get("pluginJarSha256"))
        .cloned()
        .or_else(|| {
            active_version_metadata
                .as_ref()
                .and_then(|metadata| metadata.get("cloudstream"))
                .and_then(|value| value.get("pluginJarSha256"))
                .cloned()
        })
        .or_else(|| {
            active_version_metadata
                .as_ref()
                .and_then(|metadata| metadata.get("artifact"))
                .and_then(|value| value.get("sha256"))
                .cloned()
        })
    {
        descriptor.insert("pluginJarSha256".to_string(), value);
    } else if let Some(value) = cloudstream
        .and_then(|value| value.get("artifactSha256"))
        .cloned()
    {
        descriptor.insert("pluginJarSha256".to_string(), value);
    }
    if let Some(value) = metadata
        .and_then(|value| value.get("sourcePackId"))
        .cloned()
    {
        descriptor.insert("sourcePackId".to_string(), value);
    }

    let recommendations = store
        .list_source_replacement_recommendations(Some(module.source_module_id), true)
        .await?;
    if let Some(recommendation) = recommendations.first() {
        let replacement_module_id = recommendation
            .replacement_source_module_id
            .and_then(|source_module_id| module_id_by_source_uuid.get(&source_module_id))
            .cloned();
        descriptor.insert(
            "replacementRecommendation".to_string(),
            json!({
                "recommendationKey": recommendation.recommendation_key,
                "action": recommendation.action,
                "recommendedVersion": recommendation.recommended_version,
                "reason": recommendation.reason,
                "active": recommendation.active,
                "replacementModuleId": replacement_module_id,
            }),
        );
    }

    let health_events = store
        .list_source_health_events(module.source_module_id, 3)
        .await?
        .into_iter()
        .map(|event| {
            json!({
                "eventType": event.event_type,
                "state": event.state,
                "severity": event.severity,
                "reason": event.reason,
                "observedAt": event.observed_at,
            })
        })
        .collect::<Vec<_>>();
    if !health_events.is_empty() {
        descriptor.insert("healthEvents".to_string(), Value::Array(health_events));
    }

    Ok(Value::Object(descriptor))
}

async fn nuvio_candidate_provider_invocation_config(
    store: &ExtensionStore<'_>,
    instance: &ExtensionInstance,
    base: Option<Value>,
) -> Result<Option<Value>> {
    let modules = store
        .list_source_modules(Some(instance.instance_id), None)
        .await?;
    if modules.is_empty() {
        return Ok(base);
    }

    let registries = store
        .list_source_registries(Some(instance.instance_id))
        .await?;
    let registry_by_id = registries
        .into_iter()
        .map(|registry| (registry.registry_id, registry))
        .collect::<BTreeMap<_, _>>();

    let mut object = base
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
    let explicit_modules = source_modules_from_config(&object);
    let mut projected = Vec::new();
    let mut sorted_modules = modules;
    sorted_modules.sort_by(|left, right| left.module_key.cmp(&right.module_key));
    for module in &sorted_modules {
        if module.ecosystem != "nuvio" && module.ecosystem != "stremio" {
            continue;
        }
        let Some(registry) = registry_by_id.get(&module.registry_id) else {
            continue;
        };
        projected.push(nuvio_source_module_invocation_descriptor(store, module, registry).await?);
    }

    let mut seen = BTreeSet::new();
    let mut merged = Vec::new();
    for module in projected.into_iter().chain(explicit_modules) {
        let key = stream_source_module_invocation_key(&module);
        if seen.insert(key) {
            merged.push(module);
        }
    }
    if !merged.is_empty() {
        object.insert("sourceModules".to_string(), Value::Array(merged));
    }
    Ok((!object.is_empty()).then_some(Value::Object(object)))
}

async fn nuvio_source_module_invocation_descriptor(
    store: &ExtensionStore<'_>,
    module: &ExtensionSourceModule,
    registry: &ExtensionSourceRegistry,
) -> Result<Value> {
    let metadata = module.metadata_json.as_ref();
    let nuvio = metadata
        .and_then(|value| value.get("nuvio"))
        .and_then(Value::as_object);
    let active_version = active_source_module_version(store, module).await?;
    let active_version_metadata = active_version
        .as_ref()
        .and_then(|version| version.metadata_json.as_ref());

    let mut descriptor = JsonMap::new();
    let module_id = nuvio_invocation_module_id(module);
    descriptor.insert("id".to_string(), json!(module_id));
    descriptor.insert("name".to_string(), json!(module.display_name));
    descriptor.insert("type".to_string(), json!(module.ecosystem));
    descriptor.insert(
        "adapter".to_string(),
        json!(
            nuvio
                .and_then(|value| value.get("adapter"))
                .and_then(Value::as_str)
                .unwrap_or("nuvio_js_v1")
        ),
    );
    descriptor.insert(
        "enabled".to_string(),
        json!(registry.enabled && module.enabled),
    );
    descriptor.insert("installed".to_string(), json!(module.installed));
    descriptor.insert(
        "requiresAccount".to_string(),
        json!(module.account_required),
    );
    descriptor.insert(
        "accountConfigured".to_string(),
        json!(!module.account_required),
    );
    descriptor.insert("registryKey".to_string(), json!(registry.registry_key));
    descriptor.insert("registryType".to_string(), json!(registry.registry_type));
    descriptor.insert("trustClass".to_string(), json!(registry.trust_class));
    descriptor.insert(
        "trustedForExecutableUpdates".to_string(),
        json!(registry.trusted_for_executable_updates),
    );
    descriptor.insert(
        "healthState".to_string(),
        json!(if registry.enabled && module.enabled {
            module.health_state.as_str()
        } else {
            "disabled"
        }),
    );
    if let Some(value) = module.media_types_json.clone() {
        descriptor.insert("mediaTypes".to_string(), value);
    }
    if let Some(value) = module.language_tags_json.clone() {
        descriptor.insert("languageTags".to_string(), value);
    }
    if let Some(value) = module.source_domains_json.clone() {
        descriptor.insert("sourceDomains".to_string(), value);
    }
    if let Some(value) = module.active_version.as_deref() {
        descriptor.insert("activeVersion".to_string(), json!(value));
        descriptor.insert("version".to_string(), json!(value));
    }
    if let Some(value) = module.rollback_version.as_deref() {
        descriptor.insert("rollbackVersion".to_string(), json!(value));
    }
    if let Some(value) = module.pinned_version.as_deref() {
        descriptor.insert("pinnedVersion".to_string(), json!(value));
    }
    if let Some(value) = module.last_error.as_deref() {
        descriptor.insert("lastError".to_string(), json!(value));
    }
    if module.unsupported {
        descriptor.insert(
            "unsupportedReason".to_string(),
            json!(
                module
                    .unsupported_reason
                    .as_deref()
                    .unwrap_or("unsupported by Elixir source registry")
            ),
        );
    }
    if let Some(value) = nuvio.and_then(|value| value.get("moduleId")).cloned() {
        descriptor.insert("moduleId".to_string(), value);
    }
    if let Some(value) = nuvio.and_then(|value| value.get("hasSettings")).cloned() {
        descriptor.insert("hasSettings".to_string(), value);
    }
    if let Some(value) = nuvio.and_then(|value| value.get("formats")).cloned() {
        descriptor.insert("formats".to_string(), value);
    }
    if let Some(value) = active_version_metadata
        .and_then(|metadata| metadata.get("nuvio"))
        .and_then(|value| value.get("scriptPath"))
        .cloned()
        .or_else(|| {
            active_version_metadata
                .and_then(|metadata| metadata.get("artifact"))
                .and_then(|value| value.get("containerPath"))
                .cloned()
        })
    {
        descriptor.insert("scriptPath".to_string(), value);
    }
    if let Some(value) = active_version_metadata
        .and_then(|metadata| metadata.get("nuvio"))
        .and_then(|value| value.get("artifactSha256"))
        .cloned()
        .or_else(|| {
            active_version_metadata
                .and_then(|metadata| metadata.get("artifact"))
                .and_then(|value| value.get("sha256"))
                .cloned()
        })
    {
        descriptor.insert("artifactSha256".to_string(), value);
    }
    if let Some(version) = active_version.as_ref() {
        if let Some(value) = version.artifact_url.as_deref() {
            descriptor.insert("artifactUrl".to_string(), json!(value));
        }
    }
    Ok(Value::Object(descriptor))
}

fn cloudstream_explicit_source_modules_from_config(object: &JsonMap<String, Value>) -> Vec<Value> {
    source_modules_from_config(object)
}

fn cloudstream_invocation_module_id(module: &ExtensionSourceModule) -> String {
    module
        .metadata_json
        .as_ref()
        .and_then(|metadata| metadata.get("cloudstream"))
        .and_then(|cloudstream| cloudstream.get("moduleId"))
        .and_then(Value::as_str)
        .map(cloudstream_stable_invocation_id)
        .unwrap_or_else(|| {
            module
                .module_key
                .rsplit(':')
                .next()
                .map(cloudstream_stable_invocation_id)
                .unwrap_or_else(|| cloudstream_stable_invocation_id(&module.display_name))
        })
}

fn cloudstream_invocation_module_key(value: &Value) -> String {
    stream_source_module_invocation_key(value)
}

fn source_modules_from_config(object: &JsonMap<String, Value>) -> Vec<Value> {
    if let Some(modules) = object.get("sourceModules").and_then(Value::as_array) {
        return modules.clone();
    }
    for key in ["sourceModulesJson", "source_modules_json", "modulesJson"] {
        let Some(raw) = object.get(key).and_then(Value::as_str) else {
            continue;
        };
        let Ok(value) = serde_json::from_str::<Value>(raw) else {
            continue;
        };
        if let Some(modules) = value.as_array() {
            return modules.clone();
        }
    }
    Vec::new()
}

fn stream_source_module_invocation_key(value: &Value) -> String {
    value
        .get("id")
        .or_else(|| value.get("moduleId"))
        .or_else(|| value.get("sourceModuleId"))
        .or_else(|| value.get("name"))
        .and_then(Value::as_str)
        .map(cloudstream_stable_invocation_id)
        .unwrap_or_else(|| "source".to_string())
}

fn nuvio_invocation_module_id(module: &ExtensionSourceModule) -> String {
    module
        .metadata_json
        .as_ref()
        .and_then(|metadata| metadata.get("nuvio"))
        .and_then(|nuvio| nuvio.get("moduleId"))
        .and_then(Value::as_str)
        .map(cloudstream_stable_invocation_id)
        .unwrap_or_else(|| {
            module
                .module_key
                .rsplit(':')
                .next()
                .map(cloudstream_stable_invocation_id)
                .unwrap_or_else(|| cloudstream_stable_invocation_id(&module.display_name))
        })
}

async fn active_source_module_version(
    store: &ExtensionStore<'_>,
    module: &ExtensionSourceModule,
) -> Result<Option<ExtensionSourceModuleVersion>> {
    let versions = store
        .list_source_module_versions(module.source_module_id)
        .await?;
    if let Some(active) = module.active_version.as_deref() {
        if let Some(version) = versions.iter().find(|version| version.version == active) {
            return Ok(Some(version.clone()));
        }
    }
    Ok(versions
        .iter()
        .find(|version| version.install_state == "active")
        .or_else(|| {
            versions
                .iter()
                .find(|version| version.install_state == "installed")
        })
        .cloned())
}

fn cloudstream_stable_invocation_id(value: &str) -> String {
    let mut output = String::new();
    let mut last_dash = false;
    for ch in value.trim().chars() {
        if ch.is_ascii_alphanumeric() {
            output.push(ch.to_ascii_lowercase());
            last_dash = false;
        } else if !last_dash && !output.is_empty() {
            output.push('-');
            last_dash = true;
        }
    }
    while output.ends_with('-') {
        output.pop();
    }
    if output.is_empty() {
        "source".to_string()
    } else {
        output
    }
}

fn is_public_instance_config_key(key: &str) -> bool {
    let normalized = normalize_sensitive_key(key);
    if matches!(
        normalized.as_str(),
        "runtime" | "manageddefaults" | "managed" | "secrets" | "secret" | "credentials"
    ) {
        return false;
    }
    !is_sensitive_normalized_key(&normalized)
}

fn redact_sensitive_value(value: Value) -> Value {
    match value {
        Value::Array(values) => {
            Value::Array(values.into_iter().map(redact_sensitive_value).collect())
        }
        Value::Object(object) => Value::Object(
            object
                .into_iter()
                .map(|(key, value)| {
                    let value = if is_sensitive_key(&key) {
                        Value::String("[REDACTED]".to_string())
                    } else {
                        redact_sensitive_value(value)
                    };
                    (key, value)
                })
                .collect(),
        ),
        Value::String(value) => Value::String(redact_sensitive_url(&value)),
        other => other,
    }
}

fn redact_sensitive_url(value: &str) -> String {
    let Ok(mut url) = Url::parse(value) else {
        return value.to_string();
    };
    let Some(_) = url.query() else {
        return value.to_string();
    };
    let pairs = url
        .query_pairs()
        .map(|(key, value)| {
            let redacted = if is_sensitive_key(&key) {
                "[REDACTED]".to_string()
            } else {
                redact_sensitive_url(&value)
            };
            (key.into_owned(), redacted)
        })
        .collect::<Vec<_>>();
    url.set_query(None);
    {
        let mut query = url.query_pairs_mut();
        for (key, value) in pairs {
            query.append_pair(&key, &value);
        }
    }
    url.to_string()
}

fn is_sensitive_key(key: &str) -> bool {
    is_sensitive_normalized_key(&normalize_sensitive_key(key))
}

fn normalize_sensitive_key(key: &str) -> String {
    key.chars()
        .filter(|ch| !matches!(ch, '_' | '-' | ' '))
        .flat_map(char::to_lowercase)
        .collect()
}

fn is_sensitive_normalized_key(key: &str) -> bool {
    key.contains("token")
        || key.contains("apikey")
        || key.contains("secret")
        || key.contains("password")
        || matches!(
            key,
            "key" | "pass" | "auth" | "authorization" | "signature" | "sig"
        )
}

fn normalize_candidate_file(
    mut file: AcquisitionCandidateFile,
) -> Option<AcquisitionCandidateFile> {
    file.path = file.path.trim().replace('\\', "/");
    if file.path.is_empty() {
        return None;
    }
    file.file_id = file
        .file_id
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    Some(file)
}

fn provider_scope(provider: &Provider) -> (Vec<String>, Vec<String>) {
    let Some(scope) = provider.scope_json.as_ref() else {
        return (Vec::new(), Vec::new());
    };
    let media_types = scope
        .get("media_types")
        .or_else(|| scope.get("mediaTypes"))
        .and_then(Value::as_array)
        .map(|values| string_array(values))
        .unwrap_or_default();
    let actions = scope
        .get("actions")
        .and_then(Value::as_array)
        .map(|values| string_array(values))
        .unwrap_or_default();
    (media_types, actions)
}

fn candidate_media_type_matches(provider_value: &str, requested_value: &str) -> bool {
    let Some(provider_type) = normalize_candidate_media_type(provider_value) else {
        return false;
    };
    let Some(requested_type) = normalize_candidate_media_type(requested_value) else {
        return false;
    };
    provider_type == requested_type
}

fn normalize_candidate_media_type(value: &str) -> Option<&'static str> {
    match value.trim().to_ascii_lowercase().as_str() {
        "movie" | "movies" => Some("movie"),
        "series" | "tv" | "show" | "shows" => Some("series"),
        "anime" => Some("anime"),
        _ => None,
    }
}

fn string_array(values: &[Value]) -> Vec<String> {
    values
        .iter()
        .filter_map(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn json_value_at<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    Some(current)
}

fn json_string_at(value: &Value, path: &[&str]) -> Option<String> {
    json_value_at(value, path)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn json_string_array_at(value: &Value, path: &[&str]) -> Vec<String> {
    json_value_at(value, path)
        .and_then(Value::as_array)
        .map(|values| string_array(values))
        .unwrap_or_default()
}

fn json_bool_at(value: &Value, path: &[&str]) -> Option<bool> {
    json_value_at(value, path).and_then(Value::as_bool)
}

fn json_f64_at(value: &Value, path: &[&str]) -> Option<f64> {
    json_value_at(value, path).and_then(Value::as_f64)
}

fn json_u32_at(value: &Value, path: &[&str]) -> Option<u32> {
    json_value_at(value, path)
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
}

fn json_i64_at(value: &Value, path: &[&str]) -> Option<i64> {
    json_value_at(value, path).and_then(Value::as_i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::DatabaseConfig,
        db::{
            Database,
            models::{ExtensionKind, ExtensionTrustLevel, SlotCardinality},
        },
        extensions::nuvio_registry::PRISM_EXTENSION_ID,
        extensions::store::{
            NewExtension, NewExtensionInstance, NewExtensionSourceModule,
            NewExtensionSourceModuleVersion, NewExtensionSourceRegistry, NewProvider,
        },
        orchestrator::planner::stable_provider_id,
    };
    use axum::{Router, http::StatusCode, routing::post};
    use serde_json::json;
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };
    use tokio::net::TcpListener;

    async fn setup_db() -> Result<Database> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        Ok(database)
    }

    #[tokio::test]
    async fn cs11_cloudstream_provider_config_projects_registry_modules() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        store
            .upsert_extension(&NewExtension {
                extension_id: CLOUDSTREAM_COMPAT_EXTENSION_ID.to_string(),
                name: "CloudStream Compat".to_string(),
                version: "0.1.0".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: Some("Elixir".to_string()),
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Community,
                manifest_json: json!({
                    "id": CLOUDSTREAM_COMPAT_EXTENSION_ID,
                    "version": "0.1.0",
                    "kind": "module",
                    "name": "CloudStream Compat",
                    "provides": [{
                        "capability": ACQUISITION_STREAM_CANDIDATE_PROVIDER_CAPABILITY,
                        "slot": "default",
                        "cardinality": "many",
                        "implementation": "cloudstream_compat",
                        "scope": {
                            "media_types": ["movie", "series", "anime"],
                            "actions": ["search", "resolve"]
                        }
                    }],
                    "runtime": {
                        "type": "container",
                        "image": "elixir/cloudstream-compat-provider:0.1.0"
                    },
                    "control_surface": {
                        "adapter": "generic_v1",
                        "owned_settings": [{
                            "id": "sourceModulesJson",
                            "label": "Source modules",
                            "type": "textarea",
                            "storage": {
                                "type": "instance_setting",
                                "key": "sourceModulesJson"
                            }
                        }]
                    }
                }),
                package_hash: Some("cloudstream-fixture".to_string()),
                enabled: true,
            })
            .await?;
        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: CLOUDSTREAM_COMPAT_EXTENSION_ID.to_string(),
                instance_name: "default".to_string(),
                config_json: Some(json!({
                    "sourceModulesJson": "[{\"id\":\"legacy-static\",\"adapter\":\"static_fixture_v1\",\"enabled\":true}]",
                    "sourcePackPolicy": {
                        "customRepoExecutableAutoUpdate": true
                    }
                })),
                enabled: true,
            })
            .await?;
        let registry_id = Uuid::new_v4();
        store
            .upsert_source_registry(&NewExtensionSourceRegistry {
                registry_id,
                instance_id,
                registry_key: "cloudstream.recommended".to_string(),
                registry_type: "elixir_curated_cloudstream_pack".to_string(),
                trust_class: "curated".to_string(),
                display_name: "Recommended CloudStream Sources".to_string(),
                url: Some("bundled://cloudstream/recommended".to_string()),
                enabled: true,
                auto_refresh: true,
                trusted_for_executable_updates: true,
                etag: None,
                last_modified: None,
                metadata_json: None,
            })
            .await?;
        let source_module_id = Uuid::new_v4();
        store
            .upsert_source_module(&NewExtensionSourceModule {
                source_module_id,
                instance_id,
                registry_id,
                module_key: "cloudstream:cloudstream-recommended:fixture-native".to_string(),
                display_name: "Fixture Native".to_string(),
                ecosystem: "cloudstream".to_string(),
                plugin_package: Some("FixtureProvider".to_string()),
                active_version: Some("1.2.3".to_string()),
                rollback_version: Some("1.2.2".to_string()),
                media_types_json: Some(json!(["movie", "series"])),
                language_tags_json: Some(json!(["eng"])),
                region_tags_json: None,
                source_domains_json: Some(json!(["fixture.example"])),
                account_required: false,
                unsupported: false,
                unsupported_reason: None,
                enabled: true,
                installed: true,
                pinned_version: None,
                health_state: "healthy".to_string(),
                replacement_recommendation_key: None,
                last_error: None,
                metadata_json: Some(json!({
                    "cloudstream": {
                        "moduleId": "fixture-native",
                        "internalName": "FixtureProvider",
                        "pluginPackage": "FixtureProvider",
                        "artifactSha256": "abc123"
                    },
                    "sourcePackId": "elixir.sourcepacks.cloudstream.recommended"
                })),
            })
            .await?;

        let extension = store
            .get_extension(CLOUDSTREAM_COMPAT_EXTENSION_ID)
            .await?
            .expect("extension");
        let instance = store.get_instance(instance_id).await?.expect("instance");
        let config = candidate_provider_invocation_config_for_store(&store, &extension, &instance)
            .await?
            .expect("provider config");
        let modules = config
            .pointer("/sourceModules")
            .and_then(Value::as_array)
            .expect("source modules");

        assert_eq!(modules.len(), 2);
        let native = modules
            .iter()
            .find(|module| module.get("id").and_then(Value::as_str) == Some("fixture-native"))
            .expect("native module");
        assert_eq!(
            native.get("adapter").and_then(Value::as_str),
            Some("cloudstream_plugin_v1")
        );
        assert_eq!(
            native.get("registryType").and_then(Value::as_str),
            Some("elixir_curated_cloudstream_pack")
        );
        assert_eq!(
            native.get("activeVersion").and_then(Value::as_str),
            Some("1.2.3")
        );
        assert_eq!(native.get("enabled").and_then(Value::as_bool), Some(true));
        assert!(
            modules.iter().any(|module| {
                module.get("id").and_then(Value::as_str) == Some("legacy-static")
            })
        );
        assert_eq!(
            config
                .pointer("/sourcePackPolicy/customRepoExecutableAutoUpdate")
                .and_then(Value::as_bool),
            Some(true)
        );
        Ok(())
    }

    #[tokio::test]
    async fn layered_scraper_prism_provider_config_projects_installed_scripts() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        store
            .upsert_extension(&NewExtension {
                extension_id: PRISM_EXTENSION_ID.to_string(),
                name: "Prism".to_string(),
                version: "0.1.0".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: Some("Elixir".to_string()),
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Community,
                manifest_json: json!({
                    "id": PRISM_EXTENSION_ID,
                    "version": "0.1.0",
                    "kind": "module",
                    "name": "Prism",
                    "provides": [{
                        "capability": ACQUISITION_STREAM_CANDIDATE_PROVIDER_CAPABILITY,
                        "slot": "default",
                        "cardinality": "many",
                        "implementation": "prism",
                        "scope": {
                            "media_types": ["movie", "tv", "anime"],
                            "actions": ["search", "resolve"]
                        }
                    }],
                    "runtime": {
                        "type": "container",
                        "image": "elixir/prism-source-provider:0.1.0"
                    },
                    "control_surface": {
                        "adapter": "generic_v1",
                        "owned_settings": [{
                            "id": "sourceModulesJson",
                            "label": "Source modules",
                            "type": "textarea",
                            "storage": {
                                "type": "instance_setting",
                                "key": "sourceModulesJson"
                            }
                        }]
                    }
                }),
                package_hash: Some("prism-fixture".to_string()),
                enabled: true,
            })
            .await?;
        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: PRISM_EXTENSION_ID.to_string(),
                instance_name: "default".to_string(),
                config_json: Some(json!({
                    "sourceModulesJson": "[{\"id\":\"legacy-nuvio\",\"adapter\":\"nuvio_js_v1\",\"enabled\":true,\"sourceCode\":\"module.exports={getStreams:async()=>[]}\"}]"
                })),
                enabled: true,
            })
            .await?;
        let registry_id = Uuid::new_v4();
        store
            .upsert_source_registry(&NewExtensionSourceRegistry {
                registry_id,
                instance_id,
                registry_key: "nuvio.custom.fixture".to_string(),
                registry_type: "nuvio_manifest_json".to_string(),
                trust_class: "maintainer_known".to_string(),
                display_name: "Fixture Nuvio Sources".to_string(),
                url: Some("https://example.invalid/manifest.json".to_string()),
                enabled: true,
                auto_refresh: true,
                trusted_for_executable_updates: true,
                etag: None,
                last_modified: None,
                metadata_json: None,
            })
            .await?;
        let source_module_id = Uuid::new_v4();
        store
            .upsert_source_module(&NewExtensionSourceModule {
                source_module_id,
                instance_id,
                registry_id,
                module_key: "nuvio:nuvio-custom-fixture:moviesdrive".to_string(),
                display_name: "MoviesDrive".to_string(),
                ecosystem: "nuvio".to_string(),
                plugin_package: Some("moviesdrive".to_string()),
                active_version: Some("1.1.1".to_string()),
                rollback_version: None,
                media_types_json: Some(json!(["movie", "tv"])),
                language_tags_json: Some(json!(["en"])),
                region_tags_json: None,
                source_domains_json: Some(json!(["raw.githubusercontent.com"])),
                account_required: false,
                unsupported: false,
                unsupported_reason: None,
                enabled: true,
                installed: true,
                pinned_version: None,
                health_state: "available".to_string(),
                replacement_recommendation_key: None,
                last_error: None,
                metadata_json: Some(json!({
                    "nuvio": {
                        "moduleId": "moviesdrive",
                        "adapter": "nuvio_js_v1",
                        "formats": ["m3u8"]
                    },
                    "registryKey": "nuvio.custom.fixture"
                })),
            })
            .await?;
        store
            .upsert_source_module_version(&NewExtensionSourceModuleVersion {
                version_id: Uuid::new_v4(),
                source_module_id,
                version: "1.1.1".to_string(),
                artifact_url: Some(
                    "https://raw.githubusercontent.com/example/repo/main/providers/moviesdrive.js"
                        .to_string(),
                ),
                artifact_sha256: Some(
                    "sha256-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_string(),
                ),
                signature: None,
                install_state: "active".to_string(),
                smoke_status: "unknown".to_string(),
                smoke_error: None,
                rollback_of_version_id: None,
                installed_at: None,
                activated_at: None,
                metadata_json: Some(json!({
                    "artifact": {
                        "kind": "javascript",
                        "containerPath": "/app/source-modules/sha256/aa/hash/moviesdrive.js",
                        "sha256": "sha256-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    },
                    "nuvio": {
                        "scriptPath": "/app/source-modules/sha256/aa/hash/moviesdrive.js",
                        "artifactSha256": "sha256-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    }
                })),
            })
            .await?;

        let extension = store
            .get_extension(PRISM_EXTENSION_ID)
            .await?
            .expect("extension");
        let instance = store.get_instance(instance_id).await?.expect("instance");
        let config = candidate_provider_invocation_config_for_store(&store, &extension, &instance)
            .await?
            .expect("provider config");
        let modules = config
            .pointer("/sourceModules")
            .and_then(Value::as_array)
            .expect("source modules");

        assert_eq!(modules.len(), 2);
        let moviesdrive = modules
            .iter()
            .find(|module| module.get("id").and_then(Value::as_str) == Some("moviesdrive"))
            .expect("moviesdrive module");
        assert_eq!(
            moviesdrive.get("adapter").and_then(Value::as_str),
            Some("nuvio_js_v1")
        );
        assert_eq!(
            moviesdrive.get("registryType").and_then(Value::as_str),
            Some("nuvio_manifest_json")
        );
        assert_eq!(
            moviesdrive.get("scriptPath").and_then(Value::as_str),
            Some("/app/source-modules/sha256/aa/hash/moviesdrive.js")
        );
        assert_eq!(
            moviesdrive.get("artifactSha256").and_then(Value::as_str),
            Some("sha256-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        );
        assert!(
            modules
                .iter()
                .any(|module| { module.get("id").and_then(Value::as_str) == Some("legacy-nuvio") })
        );
        Ok(())
    }

    #[test]
    fn candidate_provider_search_url_appends_search_to_base_path() -> Result<()> {
        let url = candidate_provider_search_url("http://127.0.0.1:1234/api/candidates")?;
        assert_eq!(url.as_str(), "http://127.0.0.1:1234/api/candidates/search");
        Ok(())
    }

    #[test]
    fn debrid_route_label_is_service_neutral() {
        assert_eq!(
            route_label(DEBRID_DEFAULT_LOGICAL_ID),
            "Direct HTTPS debrid download"
        );
    }

    #[test]
    fn normalize_candidate_rejects_missing_source() {
        let err = normalize_acquisition_candidate(AcquisitionCandidate {
            id: None,
            title: "Release".to_string(),
            source: " ".to_string(),
            source_kind: "magnet".to_string(),
            info_hash: None,
            file_index: None,
            quality: None,
            size_bytes: None,
            seeders: None,
            language: None,
            cached_debrid: None,
            rank: None,
            score: None,
            score_badges: Vec::new(),
            files: Vec::new(),
            supported_routes: Vec::new(),
            default_route: None,
            raw: None,
        })
        .expect_err("missing source should fail");
        assert!(err.to_string().contains("candidate source is required"));
    }

    #[test]
    fn normalize_upstream_candidates_keeps_valid_rows_and_warns_for_bad_rows() {
        let (candidates, warnings) = normalize_upstream_candidates(vec![
            json!({
                "title": "Valid Release",
                "source": "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
                "sourceKind": "magnet",
                "raw": {
                    "url": "https://source.example/path?token=secret"
                }
            }),
            json!({
                "title": "Missing Source",
                "sourceKind": "magnet"
            }),
            json!({
                "title": "Bad File",
                "source": "magnet:?xt=urn:btih:bad",
                "sourceKind": "magnet",
                "files": [{ "sizeBytes": 10 }]
            }),
        ]);

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].title, "Valid Release");
        assert_eq!(warnings.len(), 2);
        assert!(warnings[0].contains("candidate[1] rejected"));
        assert!(warnings[1].contains("candidate[2] rejected"));
    }

    #[derive(Clone)]
    struct SuiteProviderFixtureState {
        requests: Arc<Mutex<Vec<Value>>>,
        response: Value,
        status: StatusCode,
        delay_ms: u64,
    }

    async fn start_suite_provider_fixture(
        response: Value,
        status: StatusCode,
        delay_ms: u64,
    ) -> Result<(String, Arc<Mutex<Vec<Value>>>, tokio::task::JoinHandle<()>)> {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let state = SuiteProviderFixtureState {
            requests: Arc::clone(&requests),
            response,
            status,
            delay_ms,
        };
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let app = Router::new()
            .route("/candidate-provider/search", post(suite_provider_fixture))
            .with_state(state);
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("test server");
        });
        Ok((
            format!("http://127.0.0.1:{port}/candidate-provider"),
            requests,
            handle,
        ))
    }

    async fn suite_provider_fixture(
        State(state): State<SuiteProviderFixtureState>,
        Json(payload): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        state.requests.lock().expect("requests lock").push(payload);
        if state.delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(state.delay_ms)).await;
        }
        (state.status, Json(state.response.clone()))
    }

    fn suite_candidate(title: &str, hash: &str) -> Value {
        json!({
            "title": title,
            "source": format!("magnet:?xt=urn:btih:{hash}"),
            "sourceKind": "magnet",
            "infoHash": hash,
            "quality": "1080p",
            "seeders": 42,
            "cachedDebrid": true,
            "supportedRoutes": [
                DEBRID_DEFAULT_LOGICAL_ID,
                TORRENT_DEFAULT_LOGICAL_ID
            ],
            "defaultRoute": DEBRID_DEFAULT_LOGICAL_ID
        })
    }

    fn suite_candidate_with_evidence(
        title: &str,
        hash: &str,
        score: f64,
        seeders: u32,
        label: &str,
    ) -> Value {
        let mut candidate = suite_candidate(title, hash);
        let object = candidate.as_object_mut().expect("candidate object");
        object.insert("score".to_string(), json!(score));
        object.insert("seeders".to_string(), json!(seeders));
        object.insert(
            "scoreBadges".to_string(),
            json!([{
                "label": label,
                "detail": format!("{label} scoring evidence"),
                "score": score
            }]),
        );
        object.insert(
            "raw".to_string(),
            json!({
                "fixture": label,
                "sourceUrl": format!("https://source.example/{label}?token=secret-token")
            }),
        );
        candidate
    }

    async fn seed_suite_provider(
        store: &ExtensionStore<'_>,
        extension_id: &str,
        extension_name: &str,
        port: u16,
    ) -> Result<Uuid> {
        store
            .upsert_extension(&NewExtension {
                extension_id: extension_id.to_string(),
                name: extension_name.to_string(),
                version: "1.0.0".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: Some("Elixir Test".to_string()),
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Community,
                manifest_json: json!({
                    "id": extension_id,
                    "version": "1.0.0",
                    "kind": "module",
                    "name": extension_name,
                    "provides": [{
                        "capability": ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY,
                        "slot": "default",
                        "cardinality": "one",
                        "implementation": "suite_fixture",
                        "scope": {
                            "media_types": ["movie", "series", "anime"],
                            "actions": ["search"]
                        }
                    }],
                    "runtime": {
                        "type": "container",
                        "image": "example/suite-fixture:1"
                    }
                }),
                package_hash: Some(extension_id.to_string()),
                enabled: true,
            })
            .await?;
        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: extension_id.to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;
        let provider_id = stable_provider_id(
            instance_id,
            ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY,
            "default",
        );
        store
            .upsert_provider(&NewProvider {
                provider_id,
                instance_id,
                capability: ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY.to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: Some("suite_fixture".to_string()),
                scope_json: Some(json!({
                    "media_types": ["movie", "series", "anime"],
                    "actions": ["search"]
                })),
                endpoint_json: Some(json!({
                    "scheme": "http",
                    "host": "127.0.0.1",
                    "port": port,
                    "base_path": "/candidate-provider",
                    "network": null
                })),
                health_state: ProviderHealthState::Healthy,
            })
            .await?;
        Ok(provider_id)
    }

    async fn seed_stream_suite_provider(
        store: &ExtensionStore<'_>,
        extension_id: &str,
        extension_name: &str,
        port: u16,
        media_types: Vec<&str>,
        health_state: ProviderHealthState,
    ) -> Result<Uuid> {
        store
            .upsert_extension(&NewExtension {
                extension_id: extension_id.to_string(),
                name: extension_name.to_string(),
                version: "1.0.0".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: Some("Elixir Test".to_string()),
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Community,
                manifest_json: json!({
                    "id": extension_id,
                    "version": "1.0.0",
                    "kind": "module",
                    "name": extension_name,
                    "provides": [{
                        "capability": ACQUISITION_STREAM_CANDIDATE_PROVIDER_CAPABILITY,
                        "slot": "default",
                        "cardinality": "many",
                        "implementation": "stream_fixture",
                        "scope": {
                            "media_types": media_types,
                            "actions": ["search", "resolve"]
                        }
                    }],
                    "runtime": {
                        "type": "container",
                        "image": "example/stream-fixture:1"
                    },
                    "control_surface": {
                        "adapter": "generic_v1",
                        "owned_settings": [
                            {
                                "id": "sourcePack",
                                "label": "Source pack",
                                "type": "text",
                                "storage": {
                                    "type": "instance_setting",
                                    "key": "sourcePack"
                                }
                            }
                        ]
                    }
                }),
                package_hash: Some(extension_id.to_string()),
                enabled: true,
            })
            .await?;
        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: extension_id.to_string(),
                instance_name: "default".to_string(),
                config_json: Some(json!({
                    "sourcePack": "fixture-pack",
                    "apiToken": "must-not-cross-boundary"
                })),
                enabled: true,
            })
            .await?;
        let provider_id = stable_provider_id(
            instance_id,
            ACQUISITION_STREAM_CANDIDATE_PROVIDER_CAPABILITY,
            "default",
        );
        store
            .upsert_provider(&NewProvider {
                provider_id,
                instance_id,
                capability: ACQUISITION_STREAM_CANDIDATE_PROVIDER_CAPABILITY.to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::Many,
                implementation: Some("stream_fixture".to_string()),
                scope_json: Some(json!({
                    "media_types": media_types,
                    "actions": ["search", "resolve"]
                })),
                endpoint_json: Some(json!({
                    "scheme": "http",
                    "host": "127.0.0.1",
                    "port": port,
                    "base_path": "/stream-provider",
                    "network": null
                })),
                health_state,
            })
            .await?;
        Ok(provider_id)
    }

    async fn start_stream_provider_fixture(
        response: Value,
        status: StatusCode,
        delay_ms: u64,
    ) -> Result<(String, Arc<Mutex<Vec<Value>>>, tokio::task::JoinHandle<()>)> {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let state = SuiteProviderFixtureState {
            requests: Arc::clone(&requests),
            response,
            status,
            delay_ms,
        };
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let app = Router::new()
            .route("/stream-provider/search", post(suite_provider_fixture))
            .with_state(state);
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("test server");
        });
        Ok((
            format!("http://127.0.0.1:{port}/stream-provider"),
            requests,
            handle,
        ))
    }

    fn stream_search_request(limit: Option<u32>) -> StreamCandidateSearchRequest {
        StreamCandidateSearchRequest {
            provider_id: None,
            media_type: "anime".to_string(),
            title: "Fullmetal Alchemist: Brotherhood".to_string(),
            year: Some(2009),
            external_ids: Some(ExternalIds {
                anilist: Some("5114".to_string()),
                tvdb: Some("85249".to_string()),
                ..Default::default()
            }),
            titles: vec![
                StreamTitleVariant {
                    value: "Hagane no Renkinjutsushi: Fullmetal Alchemist".to_string(),
                    kind: "romaji".to_string(),
                },
                StreamTitleVariant {
                    value: "Fullmetal Alchemist: Brotherhood".to_string(),
                    kind: "canonical".to_string(),
                },
            ],
            targets: vec![
                StreamSearchTarget {
                    target_key: Some("S01E01".to_string()),
                    title: Some("Fullmetal Alchemist".to_string()),
                    season_number: Some(1),
                    episode_number: Some(1),
                    absolute_episode_number: Some(1),
                    air_date: Some("2009-04-05".to_string()),
                    runtime_seconds: Some(1440),
                    metadata: Some(json!({
                        "episodeProviderKey": "anilist:5114:A0001",
                        "sourceUrl": "https://metadata.example/episode?token=secret"
                    })),
                },
                StreamSearchTarget {
                    target_key: Some("S01E02".to_string()),
                    title: Some("The First Day".to_string()),
                    season_number: Some(1),
                    episode_number: Some(2),
                    absolute_episode_number: Some(2),
                    air_date: Some("2009-04-12".to_string()),
                    runtime_seconds: Some(1440),
                    metadata: None,
                },
            ],
            preferences: StreamSearchPreferences {
                allowed_qualities: vec![
                    "1080p".to_string(),
                    "720p".to_string(),
                    "1080p".to_string(),
                ],
                required_languages: vec!["jpn".to_string(), "eng".to_string()],
                language_preference: None,
                language_profiles: Vec::new(),
                subtitle_mode: Some("Allowed".to_string()),
                max_size_bytes: Some(2_000_000_000),
            },
            limit,
        }
    }

    fn stream_candidate(id: &str, target_key: &str) -> Value {
        json!({
            "id": id,
            "candidateKind": "stream",
            "title": format!("Fullmetal Alchemist: Brotherhood - {target_key} - 1080p"),
            "source": format!("provider://fixture/{id}"),
            "sourceKind": "http_stream",
            "quality": "1080p",
            "language": "jpn",
            "rank": 1,
            "score": 82.0,
            "supportedRoutes": ["acquisition.http_stream.default"],
            "defaultRoute": "acquisition.http_stream.default",
            "targetEvidence": {
                "mediaType": "anime",
                "targetKey": target_key,
                "seasonNumber": 1,
                "episodeNumber": 2,
                "absoluteEpisodeNumber": 2,
                "episodeTitle": "The First Day",
                "confidence": "high",
                "reasons": ["provider episode number matched"]
            },
            "delivery": {
                "streamType": "hls",
                "url": format!("https://stream.example/{id}/master.m3u8?token=secret-token"),
                "headers": {
                    "authorization": "Bearer secret-token"
                },
                "referer": "https://source.example/",
                "resolveRequired": false,
                "resolveHandle": null
            },
            "mediaEvidence": {
                "resolution": 1080,
                "audioLanguages": ["jpn"],
                "subtitleLanguages": ["eng"]
            },
            "sourceModule": {
                "id": "fixture-source",
                "name": "Fixture Source",
                "type": "cloudstream"
            },
            "raw": {
                "hosterUrl": format!("https://hoster.example/{id}?api_key=secret")
            }
        })
    }

    fn stream_candidate_with_identity(
        id: &str,
        target_key: &str,
        provider_media_id: &str,
        provider_episode_id: &str,
        hoster_id: &str,
    ) -> Value {
        let mut candidate = stream_candidate(id, target_key);
        let raw = candidate
            .pointer_mut("/raw")
            .and_then(Value::as_object_mut)
            .expect("raw object");
        raw.insert("providerMediaId".to_string(), json!(provider_media_id));
        raw.insert("providerEpisodeId".to_string(), json!(provider_episode_id));
        raw.insert("hosterId".to_string(), json!(hoster_id));
        candidate
    }

    fn suite_search_request(limit: Option<u32>) -> CandidateSearchRequest {
        CandidateSearchRequest {
            provider_id: None,
            media_type: "movie".to_string(),
            title: "Shared Request".to_string(),
            year: Some(2026),
            external_ids: Some(ExternalIds {
                imdb: Some("tt1234567".to_string()),
                ..Default::default()
            }),
            target: None,
            search_intent: None,
            preferences: CandidateSearchPreferences::default(),
            limit,
        }
    }

    fn test_language_preference(
        media_type: crate::db::models::MediaType,
        value: Value,
    ) -> AcquisitionLanguagePreference {
        language_preference_from_quality_profile(
            Some(&json!({
                "languagePreference": value
            })),
            media_type,
        )
    }

    #[test]
    fn lp3_release_language_preference_scores_without_filtering_candidates() -> Result<()> {
        let mut request = suite_search_request(None);
        request.preferences.language_preference = Some(test_language_preference(
            crate::db::models::MediaType::Movie,
            json!({
                "mode": "prefer",
                "movie": { "audio": ["English"] },
                "unknownLanguage": "allow_lower_priority"
            }),
        ));
        let (mut candidates, warnings) = normalize_upstream_candidates(vec![
            json!({
                "title": "Example.Movie.2026.1080p.English-GROUP",
                "source": "magnet:?xt=urn:btih:1111111111111111111111111111111111111111",
                "sourceKind": "magnet",
                "infoHash": "1111111111111111111111111111111111111111",
                "language": "eng",
                "score": 10.0
            }),
            json!({
                "title": "Example.Movie.2026.1080p-GROUP",
                "source": "magnet:?xt=urn:btih:2222222222222222222222222222222222222222",
                "sourceKind": "magnet",
                "infoHash": "2222222222222222222222222222222222222222",
                "score": 10.0
            }),
            json!({
                "title": "Example.Movie.2026.1080p.Russian-GROUP",
                "source": "magnet:?xt=urn:btih:3333333333333333333333333333333333333333",
                "sourceKind": "magnet",
                "infoHash": "3333333333333333333333333333333333333333",
                "language": "rus",
                "score": 10.0
            }),
        ]);
        assert!(warnings.is_empty(), "{warnings:?}");

        apply_release_candidate_language_preference(&request, &mut candidates);

        assert_eq!(candidates.len(), 3);
        let by_title = candidates
            .iter()
            .map(|candidate| (candidate.title.as_str(), candidate))
            .collect::<BTreeMap<_, _>>();
        let english = by_title["Example.Movie.2026.1080p.English-GROUP"];
        let unknown = by_title["Example.Movie.2026.1080p-GROUP"];
        let russian = by_title["Example.Movie.2026.1080p.Russian-GROUP"];
        assert!(english.score.unwrap() > unknown.score.unwrap());
        assert!(unknown.score.unwrap() > russian.score.unwrap());
        assert_eq!(
            english
                .raw
                .as_ref()
                .and_then(|raw| raw.pointer("/serverEvidence/languagePreference/state"))
                .and_then(Value::as_str),
            Some("match")
        );
        assert_eq!(
            unknown
                .raw
                .as_ref()
                .and_then(|raw| raw.pointer("/serverEvidence/languagePreference/state"))
                .and_then(Value::as_str),
            Some("unknown")
        );
        assert_eq!(
            russian
                .raw
                .as_ref()
                .and_then(|raw| raw.pointer("/serverEvidence/languagePreference/state"))
                .and_then(Value::as_str),
            Some("mismatch")
        );
        assert_eq!(
            unknown
                .raw
                .as_ref()
                .and_then(|raw| raw
                    .pointer("/serverEvidence/languagePreference/unknownLanguageIsRejected"))
                .and_then(Value::as_bool),
            Some(false)
        );
        Ok(())
    }

    #[test]
    fn lp3_stream_language_preference_scores_without_filtering_unknown_candidates() {
        let mut request = stream_search_request(None);
        request.preferences.language_preference = Some(test_language_preference(
            crate::db::models::MediaType::Anime,
            json!({
                "mode": "prefer",
                "anime": {
                    "profiles": ["ja_audio_en_subs", "dual_audio", "en_audio"]
                },
                "unknownLanguage": "allow_lower_priority"
            }),
        ));
        let mut unknown = stream_candidate("unknown-stream", "S01E02");
        unknown
            .as_object_mut()
            .expect("unknown object")
            .remove("language");
        unknown
            .as_object_mut()
            .expect("unknown object")
            .remove("mediaEvidence");
        unknown
            .as_object_mut()
            .expect("unknown object")
            .insert("score".to_string(), json!(82.0));
        let matched = stream_candidate("matched-stream", "S01E02");

        let candidates =
            apply_stream_candidate_language_preference(&request, vec![matched, unknown]);

        assert_eq!(candidates.len(), 2);
        let by_id = candidates
            .iter()
            .map(|candidate| {
                (
                    candidate.get("id").and_then(Value::as_str).unwrap(),
                    candidate,
                )
            })
            .collect::<BTreeMap<_, _>>();
        let matched = by_id["matched-stream"];
        let unknown = by_id["unknown-stream"];
        assert!(
            matched.get("score").and_then(Value::as_f64).unwrap()
                > unknown.get("score").and_then(Value::as_f64).unwrap()
        );
        assert_eq!(
            matched
                .pointer("/raw/serverEvidence/languagePreference/state")
                .and_then(Value::as_str),
            Some("match")
        );
        assert_eq!(
            unknown
                .pointer("/raw/serverEvidence/languagePreference/state")
                .and_then(Value::as_str),
            Some("unknown")
        );
        assert_eq!(
            unknown
                .pointer("/raw/serverEvidence/languagePreference/unknownLanguageIsRejected")
                .and_then(Value::as_bool),
            Some(false)
        );
    }

    #[tokio::test]
    async fn extension_suite_fanout_returns_partial_results_when_one_provider_fails() -> Result<()>
    {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        let (success_base_url, success_requests, success_server) = start_suite_provider_fixture(
            json!({
                "candidates": [suite_candidate("A Release", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")],
                "warnings": ["fixture warning"]
            }),
            StatusCode::OK,
            0,
        )
        .await?;
        let success_port = Url::parse(&success_base_url)?.port().unwrap();
        let (failure_base_url, failure_requests, failure_server) = start_suite_provider_fixture(
            json!({ "error": "upstream failed" }),
            StatusCode::INTERNAL_SERVER_ERROR,
            0,
        )
        .await?;
        let failure_port = Url::parse(&failure_base_url)?.port().unwrap();
        let success_provider =
            seed_suite_provider(&store, "elixir.sources.suite.a", "A Source", success_port).await?;
        let failure_provider =
            seed_suite_provider(&store, "elixir.sources.suite.b", "B Source", failure_port).await?;

        let mut base_urls = HashMap::new();
        base_urls.insert(success_provider, success_base_url);
        base_urls.insert(failure_provider, failure_base_url);
        let response = search_candidate_suite_with_store_at_base_urls(
            &database.pool,
            suite_search_request(Some(5)),
            base_urls,
        )
        .await?;

        assert_eq!(response.provider.provider_id, Uuid::nil());
        assert_eq!(response.candidates.len(), 1, "{:?}", response.warnings);
        assert_eq!(response.candidates[0].title, "A Release");
        let success_provider_text = success_provider.to_string();
        let failure_provider_text = failure_provider.to_string();
        assert_eq!(
            response.candidates[0]
                .raw
                .as_ref()
                .and_then(|value| value.pointer("/serverEvidence/extensionSuite/providerId"))
                .and_then(Value::as_str),
            Some(success_provider_text.as_str())
        );
        assert!(
            response
                .warnings
                .iter()
                .any(|warning| warning.contains("fixture warning"))
        );
        assert!(
            response
                .warnings
                .iter()
                .any(|warning| warning.contains("provider_failed"))
        );
        let success_payload = success_requests.lock().expect("success requests")[0].clone();
        let failure_payload = failure_requests.lock().expect("failure requests")[0].clone();
        assert_eq!(
            success_payload.pointer("/request/title"),
            failure_payload.pointer("/request/title")
        );
        assert_eq!(
            success_payload.pointer("/request/mediaType"),
            failure_payload.pointer("/request/mediaType")
        );
        assert_eq!(
            success_payload.pointer("/request/year"),
            failure_payload.pointer("/request/year")
        );
        assert_eq!(
            success_payload
                .pointer("/request/providerId")
                .and_then(Value::as_str),
            Some(success_provider_text.as_str())
        );
        assert_eq!(
            failure_payload
                .pointer("/request/providerId")
                .and_then(Value::as_str),
            Some(failure_provider_text.as_str())
        );
        success_server.abort();
        failure_server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn extension_suite_fanout_keeps_deterministic_provider_order() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        let (slow_base_url, _slow_requests, slow_server) = start_suite_provider_fixture(
            json!({
                "candidates": [suite_candidate("A Sorted Release", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")]
            }),
            StatusCode::OK,
            100,
        )
        .await?;
        let (fast_base_url, _fast_requests, fast_server) = start_suite_provider_fixture(
            json!({
                "candidates": [suite_candidate("B Sorted Release", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")]
            }),
            StatusCode::OK,
            0,
        )
        .await?;
        let slow_provider = seed_suite_provider(
            &store,
            "elixir.sources.suite.sorted_a",
            "A Source",
            Url::parse(&slow_base_url)?.port().unwrap(),
        )
        .await?;
        let fast_provider = seed_suite_provider(
            &store,
            "elixir.sources.suite.sorted_b",
            "B Source",
            Url::parse(&fast_base_url)?.port().unwrap(),
        )
        .await?;

        let mut base_urls = HashMap::new();
        base_urls.insert(slow_provider, slow_base_url);
        base_urls.insert(fast_provider, fast_base_url);
        let response = search_candidate_suite_with_store_at_base_urls(
            &database.pool,
            suite_search_request(Some(5)),
            base_urls,
        )
        .await?;

        assert_eq!(
            response
                .candidates
                .iter()
                .map(|candidate| candidate.title.as_str())
                .collect::<Vec<_>>(),
            vec!["A Sorted Release", "B Sorted Release"],
            "{:?}",
            response.warnings
        );
        slow_server.abort();
        fast_server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn extension_suite_fanout_all_failures_returns_explainable_empty_response() -> Result<()>
    {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        let (first_base_url, _first_requests, first_server) = start_suite_provider_fixture(
            json!({ "error": "first failed" }),
            StatusCode::BAD_GATEWAY,
            0,
        )
        .await?;
        let (second_base_url, _second_requests, second_server) = start_suite_provider_fixture(
            json!({ "error": "second failed" }),
            StatusCode::INTERNAL_SERVER_ERROR,
            0,
        )
        .await?;
        let first_provider = seed_suite_provider(
            &store,
            "elixir.sources.suite.fail_a",
            "A Source",
            Url::parse(&first_base_url)?.port().unwrap(),
        )
        .await?;
        let second_provider = seed_suite_provider(
            &store,
            "elixir.sources.suite.fail_b",
            "B Source",
            Url::parse(&second_base_url)?.port().unwrap(),
        )
        .await?;

        let mut base_urls = HashMap::new();
        base_urls.insert(first_provider, first_base_url);
        base_urls.insert(second_provider, second_base_url);
        let response = search_candidate_suite_with_store_at_base_urls(
            &database.pool,
            suite_search_request(Some(5)),
            base_urls,
        )
        .await?;

        assert_eq!(response.provider.provider_id, Uuid::nil());
        assert!(response.candidates.is_empty());
        assert!(
            response
                .warnings
                .iter()
                .any(|warning| warning.contains("provider_failed"))
        );
        assert!(
            response
                .warnings
                .iter()
                .any(|warning| warning.contains("all_failed_or_no_results"))
        );
        first_server.abort();
        second_server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn extension_suite_fanout_no_results_returns_explainable_empty_response() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        let (base_url, _requests, server) = start_suite_provider_fixture(
            json!({
                "candidates": []
            }),
            StatusCode::OK,
            0,
        )
        .await?;
        let provider = seed_suite_provider(
            &store,
            "elixir.sources.suite.empty",
            "Empty Source",
            Url::parse(&base_url)?.port().unwrap(),
        )
        .await?;

        let mut base_urls = HashMap::new();
        base_urls.insert(provider, base_url);
        let response = search_candidate_suite_with_store_at_base_urls(
            &database.pool,
            suite_search_request(Some(5)),
            base_urls,
        )
        .await?;

        assert_eq!(response.provider.provider_id, Uuid::nil());
        assert!(response.candidates.is_empty());
        assert!(
            response
                .warnings
                .iter()
                .any(|warning| warning.contains("no_results"))
        );
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn ess2_stream_suite_invocation_sends_canonical_target_payload() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        let (base_url, requests, server) = start_stream_provider_fixture(
            json!({
                "candidates": [stream_candidate("candidate-1", "S01E02")],
                "warnings": ["stream fixture warning"]
            }),
            StatusCode::OK,
            0,
        )
        .await?;
        let provider = seed_stream_suite_provider(
            &store,
            "elixir.sources.stream.payload",
            "Stream Payload",
            Url::parse(&base_url)?.port().unwrap(),
            vec!["anime"],
            ProviderHealthState::Healthy,
        )
        .await?;

        let mut base_urls = HashMap::new();
        base_urls.insert(provider, base_url);
        let response = search_stream_candidate_suite_with_store_at_base_urls(
            &database.pool,
            stream_search_request(Some(10)),
            base_urls,
        )
        .await?;

        assert_eq!(response.provider.provider_id, Uuid::nil());
        assert_eq!(
            response.provider.capability,
            ACQUISITION_STREAM_CANDIDATE_PROVIDER_CAPABILITY
        );
        assert_eq!(response.candidates.len(), 1, "{:?}", response.warnings);
        assert!(
            response
                .warnings
                .iter()
                .any(|warning| warning.contains("stream fixture warning"))
        );
        let payload = requests.lock().expect("requests")[0].clone();
        assert_eq!(
            payload.pointer("/schemaVersion").and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            payload
                .pointer("/request/providerId")
                .and_then(Value::as_str),
            Some(provider.to_string().as_str())
        );
        assert_eq!(
            payload
                .pointer("/request/mediaType")
                .and_then(Value::as_str),
            Some("anime")
        );
        assert_eq!(
            payload.pointer("/request/externalIds/anilist"),
            Some(&json!("5114"))
        );
        assert_eq!(
            payload
                .pointer("/request/titles/0/value")
                .and_then(Value::as_str),
            Some("Fullmetal Alchemist: Brotherhood")
        );
        assert_eq!(
            payload
                .pointer("/request/titles/0/kind")
                .and_then(Value::as_str),
            Some("canonical")
        );
        assert_eq!(
            payload
                .pointer("/request/targets/1/targetKey")
                .and_then(Value::as_str),
            Some("S01E02")
        );
        assert_eq!(
            payload
                .pointer("/request/targets/1/title")
                .and_then(Value::as_str),
            Some("The First Day")
        );
        assert_eq!(
            payload
                .pointer("/request/targets/1/runtimeSeconds")
                .and_then(Value::as_u64),
            Some(1440)
        );
        assert_eq!(
            payload.pointer("/request/preferences/allowedQualities"),
            Some(&json!(["1080p", "720p"]))
        );
        assert_eq!(
            payload
                .pointer("/request/preferences/subtitleMode")
                .and_then(Value::as_str),
            Some("allowed")
        );
        assert_eq!(
            payload
                .pointer("/provider/config/sourcePack")
                .and_then(Value::as_str),
            Some("fixture-pack")
        );
        assert!(payload.pointer("/provider/config/apiToken").is_none());
        assert_eq!(
            payload
                .pointer("/request/targets/0/metadata/sourceUrl")
                .and_then(Value::as_str),
            Some("https://metadata.example/episode?token=%5BREDACTED%5D")
        );
        assert_eq!(
            response.candidates[0]
                .pointer("/raw/hosterUrl")
                .and_then(Value::as_str),
            Some("https://hoster.example/candidate-1?api_key=%5BREDACTED%5D")
        );
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn ess2_stream_suite_fanout_keeps_partial_results_when_provider_fails() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        let (success_base_url, _success_requests, success_server) = start_stream_provider_fixture(
            json!({
                "candidates": [stream_candidate("candidate-success", "S01E01")]
            }),
            StatusCode::OK,
            0,
        )
        .await?;
        let (failure_base_url, _failure_requests, failure_server) = start_stream_provider_fixture(
            json!({ "error": "source failed" }),
            StatusCode::BAD_GATEWAY,
            0,
        )
        .await?;
        let success_provider = seed_stream_suite_provider(
            &store,
            "elixir.sources.stream.partial_a",
            "A Stream Source",
            Url::parse(&success_base_url)?.port().unwrap(),
            vec!["anime"],
            ProviderHealthState::Healthy,
        )
        .await?;
        let failure_provider = seed_stream_suite_provider(
            &store,
            "elixir.sources.stream.partial_b",
            "B Stream Source",
            Url::parse(&failure_base_url)?.port().unwrap(),
            vec!["anime"],
            ProviderHealthState::Healthy,
        )
        .await?;

        let mut base_urls = HashMap::new();
        base_urls.insert(success_provider, success_base_url);
        base_urls.insert(failure_provider, failure_base_url);
        let response = search_stream_candidate_suite_with_store_at_base_urls(
            &database.pool,
            stream_search_request(Some(10)),
            base_urls,
        )
        .await?;

        assert_eq!(response.candidates.len(), 1, "{:?}", response.warnings);
        assert!(
            response
                .warnings
                .iter()
                .any(|warning| warning.contains("stream_provider_failed"))
        );
        success_server.abort();
        failure_server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn ess2_stream_suite_filters_by_health_and_media_scope() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        let (anime_base_url, anime_requests, anime_server) = start_stream_provider_fixture(
            json!({
                "candidates": [stream_candidate("candidate-anime", "S01E01")]
            }),
            StatusCode::OK,
            0,
        )
        .await?;
        let (movie_base_url, movie_requests, movie_server) = start_stream_provider_fixture(
            json!({
                "candidates": [stream_candidate("candidate-movie", "S01E01")]
            }),
            StatusCode::OK,
            0,
        )
        .await?;
        let (unhealthy_base_url, unhealthy_requests, unhealthy_server) =
            start_stream_provider_fixture(
                json!({
                    "candidates": [stream_candidate("candidate-unhealthy", "S01E01")]
                }),
                StatusCode::OK,
                0,
            )
            .await?;
        let anime_provider = seed_stream_suite_provider(
            &store,
            "elixir.sources.stream.scope_anime",
            "Anime Stream",
            Url::parse(&anime_base_url)?.port().unwrap(),
            vec!["anime"],
            ProviderHealthState::Healthy,
        )
        .await?;
        let movie_provider = seed_stream_suite_provider(
            &store,
            "elixir.sources.stream.scope_movie",
            "Movie Stream",
            Url::parse(&movie_base_url)?.port().unwrap(),
            vec!["movie"],
            ProviderHealthState::Healthy,
        )
        .await?;
        let unhealthy_provider = seed_stream_suite_provider(
            &store,
            "elixir.sources.stream.scope_unhealthy",
            "Unhealthy Stream",
            Url::parse(&unhealthy_base_url)?.port().unwrap(),
            vec!["anime"],
            ProviderHealthState::Unhealthy,
        )
        .await?;

        let mut base_urls = HashMap::new();
        base_urls.insert(anime_provider, anime_base_url);
        base_urls.insert(movie_provider, movie_base_url);
        base_urls.insert(unhealthy_provider, unhealthy_base_url);
        let response = search_stream_candidate_suite_with_store_at_base_urls(
            &database.pool,
            stream_search_request(Some(10)),
            base_urls,
        )
        .await?;

        assert_eq!(response.candidates.len(), 1, "{:?}", response.warnings);
        assert_eq!(anime_requests.lock().expect("anime requests").len(), 1);
        assert!(movie_requests.lock().expect("movie requests").is_empty());
        assert!(
            unhealthy_requests
                .lock()
                .expect("unhealthy requests")
                .is_empty()
        );
        anime_server.abort();
        movie_server.abort();
        unhealthy_server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn ess2_stream_suite_clamps_provider_and_response_candidate_limits() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        let candidates = (0..150)
            .map(|index| stream_candidate(&format!("candidate-{index}"), "S01E01"))
            .collect::<Vec<_>>();
        let (base_url, requests, server) =
            start_stream_provider_fixture(json!({ "candidates": candidates }), StatusCode::OK, 0)
                .await?;
        let provider = seed_stream_suite_provider(
            &store,
            "elixir.sources.stream.limit",
            "Limit Stream",
            Url::parse(&base_url)?.port().unwrap(),
            vec!["anime"],
            ProviderHealthState::Healthy,
        )
        .await?;

        let mut base_urls = HashMap::new();
        base_urls.insert(provider, base_url);
        let response = search_stream_candidate_suite_with_store_at_base_urls(
            &database.pool,
            stream_search_request(Some(500)),
            base_urls,
        )
        .await?;

        assert_eq!(
            requests.lock().expect("requests")[0]
                .pointer("/request/limit")
                .and_then(Value::as_u64),
            Some(u64::from(STREAM_CANDIDATE_MAX_LIMIT))
        );
        assert_eq!(
            response.candidates.len(),
            usize::try_from(STREAM_CANDIDATE_MAX_LIMIT).unwrap()
        );
        server.abort();
        Ok(())
    }

    #[test]
    fn ess3_stream_validation_rejects_missing_target_evidence() {
        let mut candidate = stream_candidate("missing-target", "S01E01");
        candidate
            .as_object_mut()
            .expect("candidate object")
            .remove("targetEvidence");

        let (candidates, warnings) = validate_upstream_stream_candidates(vec![candidate]);

        assert!(candidates.is_empty());
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("targetEvidence is required")),
            "{warnings:?}"
        );
    }

    #[test]
    fn ess3_stream_validation_rejects_unsupported_stream_type() {
        let mut candidate = stream_candidate("bad-stream-type", "S01E01");
        *candidate
            .pointer_mut("/delivery/streamType")
            .expect("stream type") = json!("rtmp");

        let (candidates, warnings) = validate_upstream_stream_candidates(vec![candidate]);

        assert!(candidates.is_empty());
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("delivery.streamType is unsupported")),
            "{warnings:?}"
        );
    }

    #[test]
    fn ess3_stream_validation_rejects_unsafe_delivery_urls() {
        let mut candidate = stream_candidate("unsafe-url", "S01E01");
        *candidate
            .pointer_mut("/delivery/url")
            .expect("delivery url") = json!("http://127.0.0.1/master.m3u8");

        let (candidates, warnings) = validate_upstream_stream_candidates(vec![candidate]);

        assert!(candidates.is_empty());
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("delivery.url is unsafe")),
            "{warnings:?}"
        );
    }

    #[test]
    fn ess3_stream_validation_rejects_malformed_headers_and_referers() {
        let mut bad_header = stream_candidate("bad-header", "S01E01");
        bad_header
            .pointer_mut("/delivery/headers")
            .expect("headers")
            .as_object_mut()
            .expect("headers object")
            .insert("bad header".to_string(), json!("value"));
        let mut bad_referer = stream_candidate("bad-referer", "S01E01");
        *bad_referer
            .pointer_mut("/delivery/referer")
            .expect("referer") = json!("file:///tmp/source");

        let (candidates, warnings) =
            validate_upstream_stream_candidates(vec![bad_header, bad_referer]);

        assert!(candidates.is_empty());
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("invalid header name")),
            "{warnings:?}"
        );
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("delivery.referer is unsafe")),
            "{warnings:?}"
        );
    }

    #[test]
    fn ess3_stream_validation_redacts_raw_provenance() {
        let mut candidate = stream_candidate("raw-redaction", "S01E01");
        candidate.as_object_mut().expect("candidate object").insert(
            "raw".to_string(),
            json!({
                "safe": "visible",
                "sourceUrl": "https://source.example/path?token=secret-token",
                "authorization": "Bearer secret-token"
            }),
        );

        let (candidates, warnings) = validate_upstream_stream_candidates(vec![candidate]);

        assert_eq!(candidates.len(), 1, "{warnings:?}");
        assert_eq!(
            candidates[0].pointer("/raw/safe").and_then(Value::as_str),
            Some("visible")
        );
        assert_eq!(
            candidates[0]
                .pointer("/raw/sourceUrl")
                .and_then(Value::as_str),
            Some("https://source.example/path?token=%5BREDACTED%5D")
        );
        assert_eq!(
            candidates[0]
                .pointer("/raw/authorization")
                .and_then(Value::as_str),
            Some("[REDACTED]")
        );
    }

    #[test]
    fn ess3_stream_validation_bounds_raw_provenance() {
        let mut candidate = stream_candidate("raw-bound", "S01E01");
        candidate.as_object_mut().expect("candidate object").insert(
            "raw".to_string(),
            json!({
                "oversized": "x".repeat(STREAM_CANDIDATE_RAW_MAX_BYTES + 1)
            }),
        );

        let (candidates, warnings) = validate_upstream_stream_candidates(vec![candidate]);

        assert_eq!(candidates.len(), 1, "{warnings:?}");
        assert_eq!(
            candidates[0]
                .pointer("/raw/truncated")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("raw evidence exceeded")),
            "{warnings:?}"
        );
    }

    #[tokio::test]
    async fn ess4_stream_suite_dedupes_same_target_identity_and_merges_contributors() -> Result<()>
    {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        let mut first = stream_candidate_with_identity(
            "fixture-source:fmab:ep-2:alpha",
            "S01E02",
            "fmab",
            "ep-2",
            "alpha",
        );
        *first.pointer_mut("/score").expect("score") = json!(10.0);
        *first.pointer_mut("/rank").expect("rank") = json!(8);
        *first
            .pointer_mut("/mediaEvidence/resolution")
            .expect("resolution") = json!(720);
        first
            .pointer_mut("/targetEvidence/reasons")
            .and_then(Value::as_array_mut)
            .expect("reasons")
            .push(json!("first source reason"));
        let mut second = stream_candidate_with_identity(
            "fixture-source:fmab:ep-2:alpha",
            "S01E02",
            "fmab",
            "ep-2",
            "alpha",
        );
        *second.pointer_mut("/score").expect("score") = json!(95.0);
        *second.pointer_mut("/rank").expect("rank") = json!(1);
        *second
            .pointer_mut("/mediaEvidence/resolution")
            .expect("resolution") = json!(1080);
        second
            .pointer_mut("/targetEvidence/reasons")
            .and_then(Value::as_array_mut)
            .expect("reasons")
            .push(json!("second source reason"));

        let (first_base_url, _first_requests, first_server) = start_stream_provider_fixture(
            json!({
                "candidates": [first],
                "warnings": ["first stream warning"]
            }),
            StatusCode::OK,
            0,
        )
        .await?;
        let (second_base_url, _second_requests, second_server) = start_stream_provider_fixture(
            json!({
                "candidates": [second],
                "warnings": ["second stream warning"]
            }),
            StatusCode::OK,
            0,
        )
        .await?;
        let first_provider = seed_stream_suite_provider(
            &store,
            "elixir.sources.stream.dedupe_a",
            "A Stream Source",
            Url::parse(&first_base_url)?.port().unwrap(),
            vec!["anime"],
            ProviderHealthState::Healthy,
        )
        .await?;
        let second_provider = seed_stream_suite_provider(
            &store,
            "elixir.sources.stream.dedupe_b",
            "B Stream Source",
            Url::parse(&second_base_url)?.port().unwrap(),
            vec!["anime"],
            ProviderHealthState::Healthy,
        )
        .await?;

        let mut base_urls = HashMap::new();
        base_urls.insert(first_provider, first_base_url);
        base_urls.insert(second_provider, second_base_url);
        let response = search_stream_candidate_suite_with_store_at_base_urls(
            &database.pool,
            stream_search_request(Some(10)),
            base_urls,
        )
        .await?;

        assert_eq!(response.candidates.len(), 1, "{:?}", response.warnings);
        let candidate = &response.candidates[0];
        assert_eq!(
            candidate.pointer("/score").and_then(Value::as_f64),
            Some(95.0)
        );
        assert_eq!(candidate.pointer("/rank").and_then(Value::as_u64), Some(1));
        let reasons = candidate
            .pointer("/targetEvidence/reasons")
            .and_then(Value::as_array)
            .expect("target reasons")
            .iter()
            .filter_map(Value::as_str)
            .collect::<BTreeSet<_>>();
        assert!(reasons.contains("first source reason"));
        assert!(reasons.contains("second source reason"));
        let evidence = candidate
            .pointer("/raw/serverEvidence/extensionSuite")
            .expect("extension suite evidence");
        assert_eq!(
            evidence.pointer("/providerId").and_then(Value::as_str),
            Some(second_provider.to_string().as_str())
        );
        assert_eq!(
            evidence
                .pointer("/contributorCount")
                .and_then(Value::as_u64),
            Some(2)
        );
        assert_eq!(
            evidence.pointer("/dedupeKey").and_then(Value::as_str),
            Some("stream:s01e02:identity:fixture-source:fmab:ep-2:alpha")
        );
        let contributor_provider_ids = evidence
            .pointer("/contributors")
            .and_then(Value::as_array)
            .expect("contributors")
            .iter()
            .filter_map(|value| value.get("providerId").and_then(Value::as_str))
            .collect::<BTreeSet<_>>();
        assert!(contributor_provider_ids.contains(first_provider.to_string().as_str()));
        assert!(contributor_provider_ids.contains(second_provider.to_string().as_str()));
        let warnings = evidence
            .get("warnings")
            .and_then(Value::as_array)
            .expect("merged warnings")
            .iter()
            .filter_map(Value::as_str)
            .collect::<BTreeSet<_>>();
        assert!(warnings.contains("first stream warning"));
        assert!(warnings.contains("second stream warning"));
        let evidence_text = serde_json::to_string(evidence)?;
        assert!(!evidence_text.contains("secret-token"));
        assert!(evidence_text.contains("%5BREDACTED%5D"));
        first_server.abort();
        second_server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn ess4_stream_suite_never_dedupes_different_target_keys() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        let mut first = stream_candidate_with_identity(
            "fixture-source:fmab:shared:alpha",
            "S01E01",
            "fmab",
            "shared",
            "alpha",
        );
        let mut second = stream_candidate_with_identity(
            "fixture-source:fmab:shared:alpha",
            "S01E02",
            "fmab",
            "shared",
            "alpha",
        );
        *first.pointer_mut("/delivery/url").expect("first url") =
            json!("https://stream.example/shared/master.m3u8?token=first-secret");
        *second.pointer_mut("/delivery/url").expect("second url") =
            json!("https://stream.example/shared/master.m3u8?token=second-secret");

        let (first_base_url, _first_requests, first_server) =
            start_stream_provider_fixture(json!({ "candidates": [first] }), StatusCode::OK, 0)
                .await?;
        let (second_base_url, _second_requests, second_server) =
            start_stream_provider_fixture(json!({ "candidates": [second] }), StatusCode::OK, 0)
                .await?;
        let first_provider = seed_stream_suite_provider(
            &store,
            "elixir.sources.stream.target_a",
            "Target A Stream",
            Url::parse(&first_base_url)?.port().unwrap(),
            vec!["anime"],
            ProviderHealthState::Healthy,
        )
        .await?;
        let second_provider = seed_stream_suite_provider(
            &store,
            "elixir.sources.stream.target_b",
            "Target B Stream",
            Url::parse(&second_base_url)?.port().unwrap(),
            vec!["anime"],
            ProviderHealthState::Healthy,
        )
        .await?;

        let mut base_urls = HashMap::new();
        base_urls.insert(first_provider, first_base_url);
        base_urls.insert(second_provider, second_base_url);
        let response = search_stream_candidate_suite_with_store_at_base_urls(
            &database.pool,
            stream_search_request(Some(10)),
            base_urls,
        )
        .await?;

        assert_eq!(response.candidates.len(), 2, "{:?}", response.warnings);
        let keys = response
            .candidates
            .iter()
            .filter_map(|candidate| {
                candidate
                    .pointer("/raw/serverEvidence/extensionSuite/dedupeKey")
                    .and_then(Value::as_str)
            })
            .collect::<BTreeSet<_>>();
        assert!(keys.contains("stream:s01e01:identity:fixture-source:fmab:shared:alpha"));
        assert!(keys.contains("stream:s01e02:identity:fixture-source:fmab:shared:alpha"));
        first_server.abort();
        second_server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn ess4_stream_suite_delivery_url_fallback_ignores_sensitive_query_params() -> Result<()>
    {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        let mut first = stream_candidate("url-fallback-a", "S01E02");
        let mut second = stream_candidate("url-fallback-b", "S01E02");
        for candidate in [&mut first, &mut second] {
            let raw = candidate
                .pointer_mut("/raw")
                .and_then(Value::as_object_mut)
                .expect("raw object");
            raw.remove("providerMediaId");
            raw.remove("providerEpisodeId");
            raw.remove("hosterId");
        }
        *first.pointer_mut("/delivery/url").expect("first url") =
            json!("https://cdn.example/video/master.m3u8?token=first-secret&quality=1080");
        *second.pointer_mut("/delivery/url").expect("second url") =
            json!("https://cdn.example/video/master.m3u8?quality=1080&token=second-secret");

        let (first_base_url, _first_requests, first_server) =
            start_stream_provider_fixture(json!({ "candidates": [first] }), StatusCode::OK, 0)
                .await?;
        let (second_base_url, _second_requests, second_server) =
            start_stream_provider_fixture(json!({ "candidates": [second] }), StatusCode::OK, 0)
                .await?;
        let first_provider = seed_stream_suite_provider(
            &store,
            "elixir.sources.stream.url_a",
            "URL A Stream",
            Url::parse(&first_base_url)?.port().unwrap(),
            vec!["anime"],
            ProviderHealthState::Healthy,
        )
        .await?;
        let second_provider = seed_stream_suite_provider(
            &store,
            "elixir.sources.stream.url_b",
            "URL B Stream",
            Url::parse(&second_base_url)?.port().unwrap(),
            vec!["anime"],
            ProviderHealthState::Healthy,
        )
        .await?;

        let mut base_urls = HashMap::new();
        base_urls.insert(first_provider, first_base_url);
        base_urls.insert(second_provider, second_base_url);
        let response = search_stream_candidate_suite_with_store_at_base_urls(
            &database.pool,
            stream_search_request(Some(10)),
            base_urls,
        )
        .await?;

        assert_eq!(response.candidates.len(), 1, "{:?}", response.warnings);
        let evidence = response.candidates[0]
            .pointer("/raw/serverEvidence/extensionSuite")
            .expect("extension suite evidence");
        assert_eq!(
            evidence
                .pointer("/contributorCount")
                .and_then(Value::as_u64),
            Some(2)
        );
        let dedupe_key = evidence
            .pointer("/dedupeKey")
            .and_then(Value::as_str)
            .expect("dedupe key");
        assert!(dedupe_key.starts_with("stream:s01e02:delivery:"));
        assert!(!dedupe_key.contains("secret"));
        first_server.abort();
        second_server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn ess2_failing_stream_provider_does_not_poison_release_suite_search() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        let (release_base_url, _release_requests, release_server) = start_suite_provider_fixture(
            json!({
                "candidates": [suite_candidate("Release Candidate", "ffffffffffffffffffffffffffffffffffffffff")]
            }),
            StatusCode::OK,
            0,
        )
        .await?;
        let (stream_base_url, stream_requests, stream_server) = start_stream_provider_fixture(
            json!({ "error": "stream source failed" }),
            StatusCode::INTERNAL_SERVER_ERROR,
            0,
        )
        .await?;
        let release_provider = seed_suite_provider(
            &store,
            "elixir.sources.release.isolated",
            "Release Source",
            Url::parse(&release_base_url)?.port().unwrap(),
        )
        .await?;
        let stream_provider = seed_stream_suite_provider(
            &store,
            "elixir.sources.stream.isolated",
            "Failing Stream Source",
            Url::parse(&stream_base_url)?.port().unwrap(),
            vec!["movie", "anime"],
            ProviderHealthState::Healthy,
        )
        .await?;

        let mut base_urls = HashMap::new();
        base_urls.insert(release_provider, release_base_url);
        base_urls.insert(stream_provider, stream_base_url);
        let response = search_candidate_suite_with_store_at_base_urls(
            &database.pool,
            suite_search_request(Some(10)),
            base_urls,
        )
        .await?;

        assert_eq!(response.candidates.len(), 1, "{:?}", response.warnings);
        assert_eq!(response.candidates[0].title, "Release Candidate");
        assert!(stream_requests.lock().expect("stream requests").is_empty());
        assert!(
            response
                .warnings
                .iter()
                .all(|warning| !warning.contains("stream_provider_failed"))
        );
        release_server.abort();
        stream_server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn extension_suite_fanout_dedupes_same_info_hash_and_merges_contributors() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        let hash = "cccccccccccccccccccccccccccccccccccccccc";
        let (first_base_url, _first_requests, first_server) = start_suite_provider_fixture(
            json!({
                "candidates": [suite_candidate_with_evidence("Same Release A", hash, 10.0, 10, "first")],
                "warnings": ["first provider warning"]
            }),
            StatusCode::OK,
            0,
        )
        .await?;
        let (second_base_url, _second_requests, second_server) = start_suite_provider_fixture(
            json!({
                "candidates": [suite_candidate_with_evidence("Same Release B", hash, 99.0, 90, "second")],
                "warnings": ["second provider warning"]
            }),
            StatusCode::OK,
            0,
        )
        .await?;
        let first_provider = seed_suite_provider(
            &store,
            "elixir.sources.suite.dedupe_a",
            "A Source",
            Url::parse(&first_base_url)?.port().unwrap(),
        )
        .await?;
        let second_provider = seed_suite_provider(
            &store,
            "elixir.sources.suite.dedupe_b",
            "B Source",
            Url::parse(&second_base_url)?.port().unwrap(),
        )
        .await?;

        let mut base_urls = HashMap::new();
        base_urls.insert(first_provider, first_base_url);
        base_urls.insert(second_provider, second_base_url);
        let response = search_candidate_suite_with_store_at_base_urls(
            &database.pool,
            suite_search_request(Some(5)),
            base_urls,
        )
        .await?;

        assert_eq!(response.candidates.len(), 1, "{:?}", response.warnings);
        let candidate = &response.candidates[0];
        assert_eq!(candidate.title, "Same Release B");
        assert_eq!(candidate.seeders, Some(90));
        assert_eq!(candidate.score, Some(99.0));
        assert_eq!(candidate.cached_debrid, Some(true));
        assert!(
            candidate
                .score_badges
                .iter()
                .any(|badge| badge.label == "first")
        );
        assert!(
            candidate
                .score_badges
                .iter()
                .any(|badge| badge.label == "second")
        );
        let evidence = candidate
            .raw
            .as_ref()
            .and_then(|value| value.pointer("/serverEvidence/extensionSuite"))
            .expect("extension suite evidence");
        assert_eq!(
            evidence.pointer("/providerId").and_then(Value::as_str),
            Some(second_provider.to_string().as_str())
        );
        assert_eq!(
            evidence
                .pointer("/contributorCount")
                .and_then(Value::as_u64),
            Some(2)
        );
        assert_eq!(
            evidence.pointer("/dedupeKey").and_then(Value::as_str),
            Some(format!("torrent:infohash:{hash}").as_str())
        );
        let contributor_provider_ids = evidence
            .pointer("/contributors")
            .and_then(Value::as_array)
            .expect("contributors")
            .iter()
            .filter_map(|value| value.get("providerId").and_then(Value::as_str))
            .collect::<BTreeSet<_>>();
        assert!(contributor_provider_ids.contains(first_provider.to_string().as_str()));
        assert!(contributor_provider_ids.contains(second_provider.to_string().as_str()));
        let warnings = evidence
            .get("warnings")
            .and_then(Value::as_array)
            .expect("merged warnings")
            .iter()
            .filter_map(Value::as_str)
            .collect::<BTreeSet<_>>();
        assert!(warnings.contains("first provider warning"));
        assert!(warnings.contains("second provider warning"));
        let raw_text = serde_json::to_string(evidence)?;
        assert!(!raw_text.contains("secret-token"));
        assert!(raw_text.contains("%5BREDACTED%5D"));
        first_server.abort();
        second_server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn extension_suite_fanout_keeps_same_title_different_info_hashes_separate() -> Result<()>
    {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        let (first_base_url, _first_requests, first_server) = start_suite_provider_fixture(
            json!({
                "candidates": [suite_candidate("Shared Title", "dddddddddddddddddddddddddddddddddddddddd")]
            }),
            StatusCode::OK,
            0,
        )
        .await?;
        let (second_base_url, _second_requests, second_server) = start_suite_provider_fixture(
            json!({
                "candidates": [suite_candidate("Shared Title", "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")]
            }),
            StatusCode::OK,
            0,
        )
        .await?;
        let first_provider = seed_suite_provider(
            &store,
            "elixir.sources.suite.distinct_a",
            "A Source",
            Url::parse(&first_base_url)?.port().unwrap(),
        )
        .await?;
        let second_provider = seed_suite_provider(
            &store,
            "elixir.sources.suite.distinct_b",
            "B Source",
            Url::parse(&second_base_url)?.port().unwrap(),
        )
        .await?;

        let mut base_urls = HashMap::new();
        base_urls.insert(first_provider, first_base_url.clone());
        base_urls.insert(second_provider, second_base_url.clone());
        let first_response = search_candidate_suite_with_store_at_base_urls(
            &database.pool,
            suite_search_request(Some(5)),
            base_urls.clone(),
        )
        .await?;
        let second_response = search_candidate_suite_with_store_at_base_urls(
            &database.pool,
            suite_search_request(Some(5)),
            base_urls,
        )
        .await?;

        let first_keys = first_response
            .candidates
            .iter()
            .filter_map(|candidate| {
                candidate
                    .raw
                    .as_ref()
                    .and_then(|value| value.pointer("/serverEvidence/extensionSuite/dedupeKey"))
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
            })
            .collect::<Vec<_>>();
        let second_keys = second_response
            .candidates
            .iter()
            .filter_map(|candidate| {
                candidate
                    .raw
                    .as_ref()
                    .and_then(|value| value.pointer("/serverEvidence/extensionSuite/dedupeKey"))
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
            })
            .collect::<Vec<_>>();
        assert_eq!(first_response.candidates.len(), 2);
        assert_eq!(first_keys, second_keys);
        assert!(
            first_keys
                .iter()
                .any(|key| key.ends_with("dddddddddddddddddddddddddddddddddddddddd"))
        );
        assert!(
            first_keys
                .iter()
                .any(|key| key.ends_with("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"))
        );
        first_server.abort();
        second_server.abort();
        Ok(())
    }

    #[test]
    fn normalize_candidate_redacts_sensitive_raw_provenance() -> Result<()> {
        let candidate = normalize_acquisition_candidate(AcquisitionCandidate {
            id: None,
            title: "Release".to_string(),
            source: "https://hoster.example/file.mkv?token=usable-submission-token".to_string(),
            source_kind: "http".to_string(),
            info_hash: None,
            file_index: None,
            quality: None,
            size_bytes: None,
            seeders: None,
            language: None,
            cached_debrid: None,
            rank: None,
            score: None,
            score_badges: Vec::new(),
            files: Vec::new(),
            supported_routes: Vec::new(),
            default_route: None,
            raw: Some(json!({
                "stream": {
                    "url": "https://hoster.example/file.mkv?token=secret&safe=visible",
                    "authorization": "Bearer secret",
                    "nested": {
                        "api_key": "secret-api-key"
                    }
                }
            })),
        })?;

        assert_eq!(
            candidate.source,
            "https://hoster.example/file.mkv?token=usable-submission-token"
        );
        let raw = candidate.raw.expect("raw");
        assert_eq!(
            raw.pointer("/stream/url").and_then(Value::as_str),
            Some("https://hoster.example/file.mkv?token=%5BREDACTED%5D&safe=visible")
        );
        assert_eq!(
            raw.pointer("/stream/authorization").and_then(Value::as_str),
            Some("[REDACTED]")
        );
        assert_eq!(
            raw.pointer("/stream/nested/api_key")
                .and_then(Value::as_str),
            Some("[REDACTED]")
        );
        Ok(())
    }

    #[test]
    fn normalize_candidate_adds_server_torrent_health_evidence() -> Result<()> {
        let candidate = normalize_acquisition_candidate(AcquisitionCandidate {
            id: None,
            title: "Weak Release".to_string(),
            source: "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567".to_string(),
            source_kind: "magnet".to_string(),
            info_hash: Some("0123456789abcdef0123456789abcdef01234567".to_string()),
            file_index: None,
            quality: Some("1080p".to_string()),
            size_bytes: Some(1_000_000),
            seeders: None,
            language: None,
            cached_debrid: Some(false),
            rank: None,
            score: Some(0.95),
            score_badges: Vec::new(),
            files: Vec::new(),
            supported_routes: vec!["acquisition.debrid.default".to_string()],
            default_route: Some("acquisition.debrid.default".to_string()),
            raw: None,
        })?;

        let raw = candidate.raw.as_ref().expect("server evidence raw");
        assert_eq!(
            raw.pointer("/serverEvidence/torrentHealth/trackerCount")
                .and_then(Value::as_u64),
            Some(0)
        );
        assert_eq!(
            raw.pointer("/serverEvidence/torrentHealth/seederState")
                .and_then(Value::as_str),
            Some("unknown")
        );
        assert_eq!(
            raw.pointer("/serverEvidence/torrentHealth/liveDownloaderEvidenceOverridesSourceHints")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert!(candidate.score_badges.iter().any(|badge| {
            badge.label == "Tracker evidence"
                && badge
                    .detail
                    .as_deref()
                    .is_some_and(|detail| detail.contains("0 tracker URLs"))
        }));
        assert!(candidate.score_badges.iter().any(|badge| {
            badge.label == "Weak swarm"
                && badge
                    .detail
                    .as_deref()
                    .is_some_and(|detail| detail.contains("unknown reported seeders"))
        }));
        Ok(())
    }

    #[test]
    fn normalize_candidate_accepts_release_resolution_hint_envelope() -> Result<()> {
        let candidate = normalize_acquisition_candidate(AcquisitionCandidate {
            id: Some("abcdefabcdefabcdefabcdefabcdefabcdefabcd".to_string()),
            title: "  Example Show S02E01-E03 2160p WEB-DL  ".to_string(),
            source: " magnet:?xt=urn:btih:abcdefabcdefabcdefabcdefabcdefabcdefabcd ".to_string(),
            source_kind: "MAGNET".to_string(),
            info_hash: Some("abcdefabcdefabcdefabcdefabcdefabcdefabcd".to_string()),
            file_index: Some(0),
            quality: Some("2160p".to_string()),
            size_bytes: Some(12_345),
            seeders: Some(42),
            language: Some("en".to_string()),
            cached_debrid: Some(true),
            rank: Some(1),
            score: Some(0.92),
            score_badges: vec![CandidateScoreBadge {
                label: "2160p".to_string(),
                detail: Some("Detected source quality".to_string()),
                score: Some(0.28),
            }],
            files: vec![
                AcquisitionCandidateFile {
                    file_id: None,
                    file_index: Some(0),
                    path: "Example Show\\Example.Show.S02E01.mkv".to_string(),
                    size_bytes: Some(4_096),
                    selectable: Some(true),
                },
                AcquisitionCandidateFile {
                    file_id: Some("  ".to_string()),
                    file_index: Some(1),
                    path: "   ".to_string(),
                    size_bytes: None,
                    selectable: None,
                },
            ],
            supported_routes: vec![
                " acquisition.debrid.default ".to_string(),
                " ".to_string(),
                "downloaders.torrent.default".to_string(),
            ],
            default_route: Some("acquisition.debrid.default".to_string()),
            raw: Some(json!({
                "provider": "torrentio_stremio",
                "parsedHints": {
                    "authoritative": false,
                    "pack": {
                        "kind": "multi_episode",
                        "episodes": [1, 2, 3],
                        "authoritative": false
                    }
                },
                "stream": {
                    "url": "https://hoster.example/file.mkv?token=secret&safe=visible"
                }
            })),
        })?;

        assert_eq!(candidate.title, "Example Show S02E01-E03 2160p WEB-DL");
        assert_eq!(candidate.source_kind, "magnet");
        assert_eq!(
            candidate.supported_routes,
            vec![
                "acquisition.debrid.default".to_string(),
                "downloaders.torrent.default".to_string()
            ]
        );
        assert_eq!(candidate.files.len(), 1);
        assert_eq!(
            candidate.files[0].path,
            "Example Show/Example.Show.S02E01.mkv"
        );
        let raw = candidate.raw.expect("raw");
        assert_eq!(
            raw.pointer("/parsedHints/pack/kind")
                .and_then(Value::as_str),
            Some("multi_episode")
        );
        assert_eq!(
            raw.pointer("/stream/url").and_then(Value::as_str),
            Some("https://hoster.example/file.mkv?token=%5BREDACTED%5D&safe=visible")
        );
        Ok(())
    }

    #[tokio::test]
    async fn search_candidates_dispatches_to_external_provider_runtime() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let app = Router::new().route(
            "/candidate-provider/search",
            post(|| async {
                Json(json!({
                    "candidates": [{
                        "id": "candidate-1",
                        "title": "Example Release",
                        "source": "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
                        "sourceKind": "magnet",
                        "infoHash": "0123456789abcdef0123456789abcdef01234567",
                        "quality": "1080p",
                        "supportedRoutes": [
                            "acquisition.debrid.default",
                            "downloaders.torrent.default"
                        ],
                        "defaultRoute": "acquisition.debrid.default"
                    }],
                    "warnings": ["fixture"]
                }))
            }),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("test server");
        });

        let extension_id = "ext.test.candidates";
        let instance_id = Uuid::new_v4();
        let manifest = json!({
            "id": extension_id,
            "version": "1.0.0",
            "kind": "module",
            "name": "Test Candidate Source",
            "provides": [{
                "capability": ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY,
                "slot": "default",
                "cardinality": "one",
                "implementation": "test_source",
                "scope": {
                    "media_types": ["movie", "tv", "anime"],
                    "actions": ["search"]
                }
            }],
            "requires": {
                "downloads": [
                    { "kind": "debrid", "mode": "broker" },
                    { "kind": "torrent", "mode": "broker" }
                ]
            },
            "runtime": {
                "type": "container",
                "image": "example/test-source:1"
            }
        });
        store
            .upsert_extension(&NewExtension {
                extension_id: extension_id.to_string(),
                name: "Test Candidate Source".to_string(),
                version: "1.0.0".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: Some("Test".to_string()),
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Community,
                manifest_json: manifest,
                package_hash: Some("test".to_string()),
                enabled: true,
            })
            .await?;
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: extension_id.to_string(),
                instance_name: "default".to_string(),
                config_json: Some(json!({ "resultLimit": 10 })),
                enabled: true,
            })
            .await?;
        let provider_id = stable_provider_id(
            instance_id,
            ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY,
            "default",
        );
        store
            .upsert_provider(&NewProvider {
                provider_id,
                instance_id,
                capability: ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY.to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: Some("test_source".to_string()),
                scope_json: Some(json!({
                    "media_types": ["movie", "tv", "anime"],
                    "actions": ["search"]
                })),
                endpoint_json: Some(json!({
                    "scheme": "http",
                    "host": "candidate-provider.internal",
                    "port": addr.port(),
                    "base_path": "/candidate-provider",
                    "network": null
                })),
                health_state: ProviderHealthState::Healthy,
            })
            .await?;

        let request = CandidateSearchRequest {
            provider_id: Some(provider_id),
            media_type: "movie".to_string(),
            title: "Example".to_string(),
            year: Some(2026),
            external_ids: Some(ExternalIds {
                imdb: Some("tt1234567".to_string()),
                ..Default::default()
            }),
            target: None,
            search_intent: None,
            preferences: CandidateSearchPreferences::default(),
            limit: Some(10),
        };
        let selected_for_series =
            select_candidate_provider(&store, request.provider_id, Some("series")).await?;
        assert_eq!(selected_for_series.summary.provider_id, provider_id);

        let selected =
            select_candidate_provider(&store, request.provider_id, Some("movie")).await?;
        let route_options = candidate_route_options(&database.pool, &store, extension_id).await?;
        let upstream = invoke_candidate_provider_at_base_url(
            &store,
            &format!("http://127.0.0.1:{}/candidate-provider", addr.port()),
            &selected,
            &request,
        )
        .await?;
        let (candidates, normalization_warnings) =
            normalize_upstream_candidates(upstream.candidates);

        assert_eq!(selected.summary.provider_id, provider_id);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].title, "Example Release");
        assert_eq!(route_options.len(), 2);
        assert!(upstream.warnings.iter().any(|item| item == "fixture"));
        assert!(normalization_warnings.is_empty());
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn candidate_provider_invocation_sends_only_public_instance_settings() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        let captured = Arc::new(Mutex::new(None::<Value>));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let app = Router::new().route(
            "/candidate-provider/search",
            post({
                let captured = Arc::clone(&captured);
                move |Json(payload): Json<Value>| {
                    let captured = Arc::clone(&captured);
                    async move {
                        *captured.lock().expect("capture lock") = Some(payload);
                        Json(json!({
                            "candidates": [{
                                "title": "Example Release",
                                "source": "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
                                "sourceKind": "magnet"
                            }]
                        }))
                    }
                }
            }),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("test server");
        });

        let extension_id = "ext.test.secure-candidates";
        let instance_id = Uuid::new_v4();
        let manifest = json!({
            "id": extension_id,
            "version": "1.0.0",
            "kind": "module",
            "name": "Secure Candidate Source",
            "provides": [{
                "capability": ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY,
                "slot": "default",
                "cardinality": "one",
                "implementation": "test_source"
            }],
            "runtime": {
                "type": "container",
                "image": "example/test-source:1"
            },
            "control_surface": {
                "adapter": "generic_v1",
                "owned_settings": [
                    {
                        "id": "baseUrl",
                        "label": "Base URL",
                        "type": "text",
                        "storage": {
                            "type": "instance_setting",
                            "key": "baseUrl"
                        }
                    },
                    {
                        "id": "resultLimit",
                        "label": "Result limit",
                        "type": "number",
                        "storage": {
                            "type": "instance_setting",
                            "key": "resultLimit"
                        }
                    }
                ]
            }
        });
        store
            .upsert_extension(&NewExtension {
                extension_id: extension_id.to_string(),
                name: "Secure Candidate Source".to_string(),
                version: "1.0.0".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: Some("Test".to_string()),
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Community,
                manifest_json: manifest,
                package_hash: Some("test".to_string()),
                enabled: true,
            })
            .await?;
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: extension_id.to_string(),
                instance_name: "default".to_string(),
                config_json: Some(json!({
                    "baseUrl": "https://source.example/manifest.json",
                    "resultLimit": 10,
                    "runtime": {
                        "config_dir": "/private/elixir/source"
                    },
                    "realDebridApiToken": "must-not-cross-boundary",
                    "debrid.real_debrid.api_token": "must-not-cross-boundary",
                    "debrid.torbox.api_token": "must-not-cross-boundary",
                    "api_key": "must-not-cross-boundary"
                })),
                enabled: true,
            })
            .await?;
        let provider_id = stable_provider_id(
            instance_id,
            ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY,
            "default",
        );
        store
            .upsert_provider(&NewProvider {
                provider_id,
                instance_id,
                capability: ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY.to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: Some("test_source".to_string()),
                scope_json: None,
                endpoint_json: Some(json!({
                    "scheme": "http",
                    "host": "candidate-provider.internal",
                    "port": addr.port(),
                    "base_path": "/candidate-provider",
                    "network": null
                })),
                health_state: ProviderHealthState::Healthy,
            })
            .await?;

        let request = CandidateSearchRequest {
            provider_id: Some(provider_id),
            media_type: "movie".to_string(),
            title: "Example".to_string(),
            year: Some(2026),
            external_ids: Some(ExternalIds {
                imdb: Some("tt1234567".to_string()),
                ..Default::default()
            }),
            target: None,
            search_intent: None,
            preferences: CandidateSearchPreferences::default(),
            limit: Some(10),
        };
        let selected =
            select_candidate_provider(&store, request.provider_id, Some("movie")).await?;
        invoke_candidate_provider_at_base_url(
            &store,
            &format!("http://127.0.0.1:{}/candidate-provider", addr.port()),
            &selected,
            &request,
        )
        .await?;

        let payload = captured
            .lock()
            .expect("capture lock")
            .clone()
            .expect("captured invocation");
        assert_eq!(
            payload
                .pointer("/provider/config/baseUrl")
                .and_then(Value::as_str),
            Some("https://source.example/manifest.json")
        );
        assert_eq!(
            payload
                .pointer("/provider/config/resultLimit")
                .and_then(Value::as_i64),
            Some(10)
        );
        assert!(payload.pointer("/provider/config/runtime").is_none());
        assert!(
            payload
                .pointer("/provider/config/realDebridApiToken")
                .is_none()
        );
        assert!(
            payload
                .pointer("/provider/config/debrid.real_debrid.api_token")
                .is_none()
        );
        assert!(
            payload
                .pointer("/provider/config/debrid.torbox.api_token")
                .is_none()
        );
        assert!(payload.pointer("/provider/config/api_key").is_none());
        server.abort();
        Ok(())
    }
}
