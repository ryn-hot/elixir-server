use std::collections::HashMap;

use anyhow::bail;
use axum::{
    Json,
    extract::{Path, Query, State},
};
use reqwest::Method as ReqwestMethod;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    debrid::{
        DebridSubmitOptions, cancel_real_debrid_job, debrid_source_kind,
        is_real_debrid_implementation, load_real_debrid_progress, submit_real_debrid,
    },
    download_broker::{
        DownloadBrokerBindingKind, DownloadBrokerInventory, DownloadBrokerProviderRecord,
        DownloadBrokerRole, DownloadBrokerRouteInventory, DownloadBrokerRouteRecord,
        DownloadBrokerRouteUpdate, ResolvedDownloadBrokerProvider, list_acquisition_routes,
        list_logical_downloaders, resolve_logical_downloader_for_owner, upsert_acquisition_route,
    },
    extensions::store::ExtensionStore,
    http::{
        auth::CurrentUser,
        error::{ApiError, ApiResult},
        handlers::extensions::{request_instance_service_form, request_instance_service_json},
    },
    network::protection::observed_download_protection_status,
    state::AppState,
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadBrokerSubmitRequest {
    source: String,
    #[serde(default)]
    category: Option<String>,
    #[serde(default)]
    paused: Option<bool>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    priority: Option<i64>,
    #[serde(default)]
    add_to_top: Option<bool>,
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
    logical_id: String,
    provider_id: Uuid,
    accepted: bool,
    download_id: Option<String>,
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
    progress: Option<f64>,
    downloaded_bytes: Option<u64>,
    total_bytes: Option<u64>,
    remaining_bytes: Option<u64>,
    download_rate_bps: Option<u64>,
    upload_rate_bps: Option<u64>,
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
    logical_id: String,
    provider_id: Uuid,
    removed: bool,
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
    let resolved = resolve_broker_provider(
        &state.db_pool,
        &store,
        &logical_id,
        query.owner_id.as_deref(),
    )
    .await?;
    ensure_route_allows_submit(&state, &resolved).await?;
    let source = normalized_source(&request.source)?;

    let download_id = match resolved.record.role {
        DownloadBrokerRole::Torrent => {
            submit_qbittorrent(&state, &store, &resolved, source, &request).await?;
            None
        }
        DownloadBrokerRole::Usenet => {
            Some(submit_nzbget(&state, &store, &resolved, source, &request).await?)
        }
        DownloadBrokerRole::DebridResolver => Some(
            submit_real_debrid_broker(
                &state,
                &store,
                &resolved,
                source,
                &request,
                query.owner_id.as_deref(),
            )
            .await?,
        ),
    };

    Ok(Json(DownloadBrokerSubmitResponse {
        logical_id,
        provider_id: resolved.record.provider_id,
        accepted: true,
        download_id,
    }))
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
            load_real_debrid_broker_progress(&state, &store, &resolved.record).await?
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
    let resolved = resolve_broker_provider(
        &state.db_pool,
        &store,
        &logical_id,
        query.owner_id.as_deref(),
    )
    .await?;
    let removed = match resolved.record.role {
        DownloadBrokerRole::Torrent => {
            cancel_qbittorrent(
                &state,
                &store,
                &resolved.record,
                &download_id,
                query.delete_files.unwrap_or(false),
            )
            .await?
        }
        DownloadBrokerRole::Usenet => {
            cancel_nzbget(&state, &store, &resolved.record, &download_id).await?
        }
        DownloadBrokerRole::DebridResolver => {
            cancel_real_debrid_broker(&state, &store, &resolved.record, &download_id).await?
        }
    };
    Ok(Json(DownloadBrokerCancelResponse {
        logical_id,
        provider_id: resolved.record.provider_id,
        removed,
    }))
}

async fn resolve_broker_provider(
    pool: &sqlx::AnyPool,
    store: &ExtensionStore<'_>,
    logical_id: &str,
    owner_id: Option<&str>,
) -> ApiResult<ResolvedDownloadBrokerProvider> {
    resolve_logical_downloader_for_owner(
        pool,
        store,
        logical_id,
        owner_id.unwrap_or(crate::download_broker::DEFAULT_ROUTE_OWNER_ID),
    )
    .await
    .map_err(|err| {
        let message = err.to_string();
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

async fn submit_qbittorrent(
    state: &AppState,
    store: &ExtensionStore<'_>,
    resolved: &ResolvedDownloadBrokerProvider,
    source: &str,
    request: &DownloadBrokerSubmitRequest,
) -> ApiResult<()> {
    validate_torrent_source(source)?;
    let mut fields = HashMap::new();
    fields.insert("urls".to_string(), source.to_string());
    if let Some(category) =
        non_empty(request.category.as_deref()).or_else(|| non_empty(resolved.category.as_deref()))
    {
        fields.insert("category".to_string(), category.to_string());
    }
    if let Some(name) = non_empty(request.name.as_deref()) {
        fields.insert("rename".to_string(), name.to_string());
    }
    if let Some(paused) = request.paused {
        fields.insert("paused".to_string(), paused.to_string());
    }
    request_instance_service_form(
        state,
        store,
        resolved.record.instance_id,
        "api/v2/torrents/add",
        &fields,
    )
    .await
    .map_err(ApiError::from)
}

async fn submit_nzbget(
    state: &AppState,
    store: &ExtensionStore<'_>,
    resolved: &ResolvedDownloadBrokerProvider,
    source: &str,
    request: &DownloadBrokerSubmitRequest,
) -> ApiResult<String> {
    validate_nzb_source(source)?;
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

async fn submit_real_debrid_broker(
    state: &AppState,
    store: &ExtensionStore<'_>,
    resolved: &ResolvedDownloadBrokerProvider,
    source: &str,
    request: &DownloadBrokerSubmitRequest,
    owner_id: Option<&str>,
) -> ApiResult<String> {
    validate_debrid_source(source)?;
    if !is_real_debrid_implementation(resolved.record.implementation.as_deref()) {
        return Err(ApiError::conflict(
            "the selected debrid provider is not supported by the native Real-Debrid broker yet",
        ));
    }
    let category =
        non_empty(request.category.as_deref()).or_else(|| non_empty(resolved.category.as_deref()));
    let job_id = submit_real_debrid(
        state,
        store,
        resolved.record.provider_id,
        resolved.record.instance_id,
        source,
        DebridSubmitOptions {
            owner_id: owner_id.unwrap_or(crate::download_broker::DEFAULT_ROUTE_OWNER_ID),
            category,
            name: non_empty(request.name.as_deref()),
            paused: request.paused.unwrap_or(false),
        },
    )
    .await
    .map_err(|err| {
        let message = err.to_string();
        if message.contains("source must") {
            ApiError::bad_request(message)
        } else if message.contains("token") || message.contains("Real-Debrid API") {
            ApiError::conflict(message)
        } else {
            ApiError::from(err)
        }
    })?;
    Ok(job_id.to_string())
}

async fn load_qbittorrent_progress(
    state: &AppState,
    store: &ExtensionStore<'_>,
    record: &DownloadBrokerProviderRecord,
) -> ApiResult<Vec<DownloadBrokerProgressItem>> {
    let value = request_instance_service_json(
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
    Ok(items
        .iter()
        .filter_map(|item| {
            let id = item.get("hash").and_then(Value::as_str)?.to_string();
            Some(DownloadBrokerProgressItem {
                id,
                name: string_field(item, "name"),
                state: string_field(item, "state"),
                category: string_field(item, "category"),
                progress: item.get("progress").and_then(Value::as_f64),
                downloaded_bytes: number_field(item, "downloaded"),
                total_bytes: number_field(item, "total_size"),
                remaining_bytes: number_field(item, "amount_left"),
                download_rate_bps: number_field(item, "dlspeed"),
                upload_rate_bps: number_field(item, "upspeed"),
            })
        })
        .collect())
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
                progress: progress_fraction(downloaded_bytes, total_bytes),
                downloaded_bytes,
                total_bytes,
                remaining_bytes,
                download_rate_bps: None,
                upload_rate_bps: None,
            })
        })
        .collect())
}

async fn load_real_debrid_broker_progress(
    state: &AppState,
    store: &ExtensionStore<'_>,
    record: &DownloadBrokerProviderRecord,
) -> ApiResult<Vec<DownloadBrokerProgressItem>> {
    if !is_real_debrid_implementation(record.implementation.as_deref()) {
        return Err(ApiError::conflict(
            "the selected debrid provider does not expose native Real-Debrid progress",
        ));
    }
    let items = load_real_debrid_progress(state, store, record.provider_id, record.instance_id)
        .await
        .map_err(ApiError::from)?;
    Ok(items
        .into_iter()
        .map(|item| DownloadBrokerProgressItem {
            id: item.id,
            name: item.name,
            state: item.state,
            category: item.category,
            progress: item.progress,
            downloaded_bytes: item.downloaded_bytes,
            total_bytes: item.total_bytes,
            remaining_bytes: item.remaining_bytes,
            download_rate_bps: item.download_rate_bps,
            upload_rate_bps: None,
        })
        .collect())
}

async fn cancel_qbittorrent(
    state: &AppState,
    store: &ExtensionStore<'_>,
    record: &DownloadBrokerProviderRecord,
    download_id: &str,
    delete_files: bool,
) -> ApiResult<bool> {
    let id = normalized_source(download_id)?;
    let mut fields = HashMap::new();
    fields.insert("hashes".to_string(), id.to_string());
    fields.insert("deleteFiles".to_string(), delete_files.to_string());
    request_instance_service_form(
        state,
        store,
        record.instance_id,
        "api/v2/torrents/delete",
        &fields,
    )
    .await
    .map_err(ApiError::from)?;
    Ok(true)
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

async fn cancel_real_debrid_broker(
    state: &AppState,
    store: &ExtensionStore<'_>,
    record: &DownloadBrokerProviderRecord,
    download_id: &str,
) -> ApiResult<bool> {
    if !is_real_debrid_implementation(record.implementation.as_deref()) {
        return Err(ApiError::conflict(
            "the selected debrid provider does not support native Real-Debrid cancel",
        ));
    }
    cancel_real_debrid_job(
        state,
        store,
        record.provider_id,
        record.instance_id,
        download_id,
    )
    .await
    .map_err(ApiError::from)
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

fn validate_debrid_source(source: &str) -> ApiResult<()> {
    debrid_source_kind(source)
        .map(|_| ())
        .map_err(|err| ApiError::bad_request(err.to_string()))
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
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
    use super::*;

    #[test]
    fn validates_broker_submit_sources() {
        assert!(validate_torrent_source("magnet:?xt=urn:btih:abc").is_ok());
        assert!(validate_torrent_source("https://example.test/file.torrent").is_ok());
        assert!(validate_torrent_source("ftp://example.test/file.torrent").is_err());

        assert!(validate_nzb_source("https://example.test/file.nzb").is_ok());
        assert!(validate_nzb_source("magnet:?xt=urn:btih:abc").is_err());
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
}
