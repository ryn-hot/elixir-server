use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use axum::{
    Json,
    extract::{Query, State},
};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{
    db::models::{Extension, ExtensionInstance, Provider, ProviderHealthState},
    download_broker::{
        DEBRID_DEFAULT_LOGICAL_ID, DownloadBrokerRouteRecord, TORRENT_DEFAULT_LOGICAL_ID,
        list_acquisition_routes,
    },
    extensions::{ExternalIds, store::ExtensionStore},
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
    pub season_number: Option<i32>,
    #[serde(default)]
    pub episode_number: Option<i32>,
    #[serde(default)]
    pub absolute_episode_number: Option<i32>,
    #[serde(default)]
    pub air_date: Option<String>,
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
    config: Option<&'a Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CandidateProviderUpstreamResponse {
    #[serde(default)]
    candidates: Vec<AcquisitionCandidate>,
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
    let candidates = upstream
        .candidates
        .into_iter()
        .map(normalize_acquisition_candidate)
        .collect::<Result<Vec<_>>>()?;

    Ok(CandidateSearchResponse {
        schema_version: CANDIDATE_PROVIDER_SCHEMA_VERSION,
        provider: provider.summary,
        route_options,
        candidates,
        warnings: upstream.warnings,
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
        if detail.provider.health_state == ProviderHealthState::Unhealthy {
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
                    .any(|item| item.eq_ignore_ascii_case(media_type.trim()))
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
    let invocation = CandidateProviderInvocation {
        schema_version: CANDIDATE_PROVIDER_SCHEMA_VERSION,
        request,
        provider: CandidateProviderInvocationContext {
            provider_id: selected.provider.provider_id,
            extension_id: &selected.extension.extension_id,
            instance_id: selected.instance.instance_id,
            implementation: selected.provider.implementation.as_deref(),
            config: selected.instance.config_json.as_ref(),
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
    Ok(candidate)
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
        let selected =
            select_candidate_provider(&store, request.provider_id, Some("movie")).await?;
        let route_options = candidate_route_options(&database.pool, &store, extension_id).await?;
        let upstream = invoke_candidate_provider_at_base_url(
            &format!("http://127.0.0.1:{}/candidate-provider", addr.port()),
            &selected,
            &request,
        )
        .await?;
        let candidates = upstream
            .candidates
            .into_iter()
            .map(normalize_acquisition_candidate)
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(selected.summary.provider_id, provider_id);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].title, "Example Release");
        assert_eq!(route_options.len(), 2);
        assert!(upstream.warnings.iter().any(|item| item == "fixture"));
        server.abort();
        Ok(())
    }
}
