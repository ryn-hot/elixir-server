use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use axum::{
    Json,
    extract::{Query, State},
};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value, json};
use tokio::task::JoinSet;
use uuid::Uuid;

use crate::{
    db::models::{Extension, ExtensionInstance, Provider, ProviderHealthState},
    download_broker::{
        DEBRID_DEFAULT_LOGICAL_ID, DownloadBrokerRouteRecord, TORRENT_DEFAULT_LOGICAL_ID,
        list_acquisition_routes,
    },
    extensions::{ExternalIds, manifest::ExtensionManifest, store::ExtensionStore},
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
    validate_candidate_search_request(&request)?;
    let store = ExtensionStore::new(pool);
    let provider =
        select_candidate_provider(&store, request.provider_id, Some(&request.media_type)).await?;
    let route_options =
        candidate_route_options(pool, &store, &provider.extension.extension_id).await?;
    let upstream = invoke_candidate_provider(&provider, &request).await?;
    candidate_search_response_from_upstream(provider.summary, route_options, upstream)
}

pub(crate) async fn search_candidate_suite_with_store(
    pool: &sqlx::AnyPool,
    request: CandidateSearchRequest,
) -> Result<CandidateSearchResponse> {
    validate_candidate_search_request(&request)?;
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
    search_stream_candidate_suite_with_providers(request, providers, None).await
}

#[cfg(test)]
pub(crate) async fn search_candidates_with_store_at_base_url(
    pool: &sqlx::AnyPool,
    request: CandidateSearchRequest,
    base_url: &str,
) -> Result<CandidateSearchResponse> {
    validate_candidate_search_request(&request)?;
    let store = ExtensionStore::new(pool);
    let provider =
        select_candidate_provider(&store, request.provider_id, Some(&request.media_type)).await?;
    let route_options =
        candidate_route_options(pool, &store, &provider.extension.extension_id).await?;
    let upstream = invoke_candidate_provider_at_base_url(base_url, &provider, &request).await?;
    candidate_search_response_from_upstream(provider.summary, route_options, upstream)
}

#[cfg(test)]
pub(crate) async fn search_candidate_suite_with_store_at_base_urls(
    pool: &sqlx::AnyPool,
    request: CandidateSearchRequest,
    base_urls: std::collections::HashMap<Uuid, String>,
) -> Result<CandidateSearchResponse> {
    validate_candidate_search_request(&request)?;
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
    search_stream_candidate_suite_with_providers(request, providers, Some(base_urls)).await
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
                            &base_url,
                            &selected,
                            &provider_request,
                        )
                        .await?
                    } else {
                        invoke_candidate_provider(&selected, &provider_request).await?
                    }
                }
                #[cfg(not(test))]
                {
                    invoke_candidate_provider(&selected, &provider_request).await?
                }
            };
            let mut response = candidate_search_response_from_upstream(
                selected.summary.clone(),
                route_options,
                upstream,
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
        let mut provider_request = request.clone();
        provider_request.provider_id = Some(selected.summary.provider_id);
        provider_request.limit = Some(stream_candidate_effective_limit(provider_request.limit));
        #[cfg(test)]
        let base_url = test_base_urls
            .as_ref()
            .and_then(|urls| urls.get(&selected.summary.provider_id).cloned());

        tasks.spawn(async move {
            let upstream = {
                #[cfg(test)]
                {
                    if let Some(base_url) = base_url {
                        invoke_stream_candidate_provider_at_base_url(
                            &base_url,
                            &selected,
                            &provider_request,
                        )
                        .await?
                    } else {
                        invoke_stream_candidate_provider(&selected, &provider_request).await?
                    }
                }
                #[cfg(not(test))]
                {
                    invoke_stream_candidate_provider(&selected, &provider_request).await?
                }
            };
            let mut response =
                stream_candidate_search_response_from_upstream(selected.summary.clone(), upstream);
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
        if candidates.len() >= limit {
            candidates.truncate(limit);
            break;
        }
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
        normalize_string_list(request.preferences.required_languages);
    request.preferences.subtitle_mode = request
        .preferences
        .subtitle_mode
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty());
    Ok(request)
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

fn compare_suite_primary_candidates(
    left: &SuiteCandidateDedupeItem,
    right: &SuiteCandidateDedupeItem,
) -> Ordering {
    suite_primary_score(&left.candidate)
        .cmp(&suite_primary_score(&right.candidate))
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

fn suite_provider_evidence_object(candidate: &AcquisitionCandidate) -> JsonMap<String, Value> {
    candidate
        .raw
        .as_ref()
        .and_then(|raw| raw.pointer("/serverEvidence/extensionSuite"))
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

fn candidate_search_response_from_upstream(
    provider: CandidateProviderSummary,
    route_options: Vec<CandidateRouteOption>,
    upstream: CandidateProviderUpstreamResponse,
) -> Result<CandidateSearchResponse> {
    let (candidates, normalization_warnings) = normalize_upstream_candidates(upstream.candidates);
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
) -> StreamCandidateSearchResponse {
    StreamCandidateSearchResponse {
        schema_version: CANDIDATE_PROVIDER_SCHEMA_VERSION,
        provider,
        candidates: upstream
            .candidates
            .into_iter()
            .map(redact_sensitive_value)
            .collect(),
        warnings: upstream
            .warnings
            .into_iter()
            .map(|warning| warning.trim().to_string())
            .filter(|warning| !warning.is_empty())
            .collect(),
    }
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
    invoke_candidate_provider_at_base_url(&base_url, selected, request).await
}

async fn invoke_candidate_provider_at_base_url(
    base_url: &str,
    selected: &CandidateProviderSelection,
    request: &CandidateSearchRequest,
) -> Result<CandidateProviderUpstreamResponse> {
    let search_url = candidate_provider_search_url(&base_url)?;
    let provider_config =
        candidate_provider_invocation_config(&selected.extension, &selected.instance)?;
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
    invoke_stream_candidate_provider_at_base_url(&base_url, selected, request).await
}

async fn invoke_stream_candidate_provider_at_base_url(
    base_url: &str,
    selected: &CandidateProviderSelection,
    request: &StreamCandidateSearchRequest,
) -> Result<StreamCandidateProviderUpstreamResponse> {
    let search_url = candidate_provider_search_url(&base_url)?;
    let provider_config =
        candidate_provider_invocation_config(&selected.extension, &selected.instance)?;
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

fn candidate_provider_invocation_config(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::DatabaseConfig,
        db::{
            Database,
            models::{ExtensionKind, ExtensionTrustLevel, SlotCardinality},
        },
        extensions::store::{NewExtension, NewExtensionInstance, NewProvider},
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
        let response_text = serde_json::to_string(&response.candidates)?;
        assert!(!response_text.contains("secret-token"));
        assert!(response_text.contains("%5BREDACTED%5D"));
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
