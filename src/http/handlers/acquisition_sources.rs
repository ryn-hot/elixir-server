use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use axum::{
    Json,
    extract::{Query, State},
};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value, json};
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

const CANDIDATE_PROVIDER_SCHEMA_VERSION: u32 = 1;
const CANDIDATE_PROVIDER_SEARCH_PATH: &str = "search";
const CANDIDATE_PROVIDER_TIMEOUT_SECONDS: u64 = 30;

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

#[derive(Debug, Clone, Serialize)]
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

#[derive(Debug, Clone)]
struct CandidateProviderSelection {
    summary: CandidateProviderSummary,
    provider: Provider,
    extension: Extension,
    instance: ExtensionInstance,
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
    let store = ExtensionStore::new(pool);
    let provider =
        select_candidate_provider(&store, request.provider_id, Some(&request.media_type)).await?;
    let route_options =
        candidate_route_options(pool, &store, &provider.extension.extension_id).await?;
    let upstream = invoke_candidate_provider(&provider, &request).await?;
    candidate_search_response_from_upstream(provider.summary, route_options, upstream)
}

#[cfg(test)]
pub(crate) async fn search_candidates_with_store_at_base_url(
    pool: &sqlx::AnyPool,
    request: CandidateSearchRequest,
    base_url: &str,
) -> Result<CandidateSearchResponse> {
    let store = ExtensionStore::new(pool);
    let provider =
        select_candidate_provider(&store, request.provider_id, Some(&request.media_type)).await?;
    let route_options =
        candidate_route_options(pool, &store, &provider.extension.extension_id).await?;
    let upstream = invoke_candidate_provider_at_base_url(base_url, &provider, &request).await?;
    candidate_search_response_from_upstream(provider.summary, route_options, upstream)
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

async fn available_candidate_providers(
    store: &ExtensionStore<'_>,
    media_type: Option<&str>,
) -> Result<Vec<CandidateProviderSelection>> {
    let mut providers = Vec::new();
    for detail in store.list_provider_details().await? {
        if detail.provider.capability != ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY {
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
    use axum::{Router, routing::post};
    use serde_json::json;
    use std::sync::{Arc, Mutex};
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
