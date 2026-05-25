use anyhow::Context;
use axum::{
    Json,
    extract::{Path, Query, State},
};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue, json};
use std::str::FromStr;
use uuid::Uuid;

use crate::{
    acquisition::subscriptions::{
        AcquisitionRoutePolicy, AcquisitionSubscription, AcquisitionSubscriptionDetail,
        AcquisitionSubscriptionFilter, AcquisitionSubscriptionStopTrackingResult,
        AcquisitionSubscriptionUpdate, AcquisitionTarget, AcquisitionTargetState,
        AcquisitionTargetStateUpdate, CreateAcquisitionIntent, NewAcquisitionSubscription,
        NewAcquisitionTarget, create_or_update_acquisition_intent, create_subscription,
        get_subscription, get_subscription_detail, get_target, list_subscriptions,
        stop_subscription_tracking, update_subscription, update_target_state,
        upsert_subscription_targets, validate_new_targets,
    },
    db::models::ProviderHealthState,
    download_broker::{DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID},
    extensions::store::ExtensionStore,
    http::{
        auth::CurrentUser,
        error::{ApiError, ApiResult},
        handlers::{
            acquisition_sources::{
                ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY, AcquisitionCandidate,
                normalize_acquisition_candidate,
            },
            download_broker::{
                DownloadBrokerSubmitRequest, cancel_download_item, generic_debrid_error_message,
                submit_to_broker,
            },
        },
    },
    state::AppState,
};
use sqlx::Row;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListAcquisitionSubscriptionsQuery {
    #[serde(default)]
    active: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateAcquisitionSubscriptionRequest {
    #[serde(flatten)]
    subscription: NewAcquisitionSubscription,
    #[serde(default)]
    targets: Vec<NewAcquisitionTarget>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpsertAcquisitionTargetsRequest {
    #[serde(default)]
    targets: Vec<NewAcquisitionTarget>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AcquisitionSubscriptionsResponse {
    subscriptions: Vec<AcquisitionSubscription>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AcquisitionTargetsResponse {
    targets: Vec<AcquisitionTarget>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubmitAcquisitionCandidateRequest {
    #[serde(default)]
    provider_id: Option<Uuid>,
    #[serde(default, alias = "routeLogicalId")]
    selected_route_logical_id: Option<String>,
    candidate: AcquisitionCandidate,
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

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AcquisitionTargetSubmitResponse {
    target: AcquisitionTarget,
    source_provider_id: Uuid,
    source_extension_id: String,
    route_logical_id: String,
    broker_provider_id: Uuid,
    accepted: bool,
    download_id: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CancelAcquisitionSubscriptionMode {
    Dismiss,
    StopTracking,
    CancelDownloads,
}

impl Default for CancelAcquisitionSubscriptionMode {
    fn default() -> Self {
        Self::Dismiss
    }
}

impl CancelAcquisitionSubscriptionMode {
    fn default_reason(self) -> &'static str {
        match self {
            Self::Dismiss => "User removed acquisition request.",
            Self::StopTracking => "User stopped acquisition tracking.",
            Self::CancelDownloads => "User cancelled acquisition downloads.",
        }
    }

    fn message(self, failures: usize) -> String {
        match (self, failures) {
            (Self::CancelDownloads, 0) => {
                "Acquisition request removed and active downloads were cancelled.".to_string()
            }
            (Self::CancelDownloads, count) => format!(
                "Acquisition request removed. {count} downloader cancellation attempt(s) need attention."
            ),
            (Self::StopTracking, _) => {
                "Acquisition tracking stopped without deleting downloader data.".to_string()
            }
            (Self::Dismiss, _) => "Acquisition request removed.".to_string(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelAcquisitionSubscriptionRequest {
    #[serde(default)]
    pub mode: CancelAcquisitionSubscriptionMode,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub delete_files: Option<bool>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelAcquisitionDownloadFailure {
    pub route_logical_id: String,
    pub download_id: String,
    pub error: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelAcquisitionSubscriptionResponse {
    pub mode: CancelAcquisitionSubscriptionMode,
    pub message: String,
    pub result: AcquisitionSubscriptionStopTrackingResult,
    pub downloads_cancel_attempted: usize,
    pub downloads_cancelled: usize,
    pub download_cancel_failures: Vec<CancelAcquisitionDownloadFailure>,
}

#[derive(Debug)]
struct ActiveSubscriptionDownloadJob {
    route_logical_id: String,
    owner_id: String,
    download_id: String,
}

pub async fn list_acquisition_subscriptions(
    _user: CurrentUser,
    State(state): State<AppState>,
    Query(query): Query<ListAcquisitionSubscriptionsQuery>,
) -> ApiResult<Json<AcquisitionSubscriptionsResponse>> {
    let subscriptions = list_subscriptions(
        &state.db_pool,
        AcquisitionSubscriptionFilter {
            active: query.active,
        },
    )
    .await
    .map_err(ApiError::from)?;
    Ok(Json(AcquisitionSubscriptionsResponse { subscriptions }))
}

pub async fn create_acquisition_subscription(
    _user: CurrentUser,
    State(state): State<AppState>,
    Json(request): Json<CreateAcquisitionSubscriptionRequest>,
) -> ApiResult<Json<AcquisitionSubscriptionDetail>> {
    validate_new_targets(request.subscription.media_type, &request.targets)
        .map_err(map_acquisition_input_error)?;
    let subscription = create_subscription(&state.db_pool, request.subscription)
        .await
        .map_err(map_acquisition_input_error)?;
    if !request.targets.is_empty() {
        upsert_subscription_targets(
            &state.db_pool,
            subscription.subscription_id,
            request.targets,
        )
        .await
        .map_err(map_acquisition_input_error)?;
    }
    let detail = get_subscription_detail(&state.db_pool, subscription.subscription_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::internal("created acquisition subscription was not readable"))?;
    Ok(Json(detail))
}

pub async fn create_acquisition_intent(
    _user: CurrentUser,
    State(state): State<AppState>,
    Json(mut request): Json<CreateAcquisitionIntent>,
) -> ApiResult<Json<crate::acquisition::subscriptions::AcquisitionIntentCreation>> {
    let store = ExtensionStore::new(&state.db_pool);
    apply_source_provider_config_defaults(&store, &mut request).await?;
    let result = create_or_update_acquisition_intent(&state.db_pool, request, chrono::Utc::now())
        .await
        .map_err(map_acquisition_input_error)?;
    Ok(Json(result))
}

async fn apply_source_provider_config_defaults(
    store: &ExtensionStore<'_>,
    request: &mut CreateAcquisitionIntent,
) -> ApiResult<()> {
    let Some(source_provider_id) = request.source_provider_id else {
        return Ok(());
    };
    let provider = store
        .get_provider(source_provider_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::bad_request("source provider was not found"))?;
    if !provider
        .capability
        .eq_ignore_ascii_case(ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY)
    {
        return Err(ApiError::bad_request(
            "source provider must be an acquisition candidate provider",
        ));
    }
    let instance = store
        .get_instance(provider.instance_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::bad_request("source provider instance was not found"))?;
    let Some(config) = instance.config_json.as_ref().and_then(JsonValue::as_object) else {
        return Ok(());
    };

    if request.route_policy.is_none() {
        if let Some(route_policy) = config
            .get("routePolicy")
            .and_then(JsonValue::as_str)
            .map(AcquisitionRoutePolicy::from_str)
            .transpose()
            .map_err(|err| ApiError::bad_request(err.to_string()))?
        {
            request.route_policy = Some(route_policy);
        }
    }
    if request.release_delay_seconds.is_none() {
        if let Some(delay) = config
            .get("releaseDelaySeconds")
            .and_then(JsonValue::as_i64)
        {
            if delay < 0 {
                return Err(ApiError::bad_request(
                    "releaseDelaySeconds cannot be negative",
                ));
            }
            request.release_delay_seconds = Some(delay);
        }
    }
    if request.quality_profile.is_none() {
        request.quality_profile = source_quality_profile_from_config(config);
    }
    Ok(())
}

fn source_quality_profile_from_config(config: &JsonMap<String, JsonValue>) -> Option<JsonValue> {
    let mut profile = JsonMap::new();
    if let Some(values) = string_list_config(config.get("allowedQualities")) {
        if !values.is_empty() {
            profile.insert("allowedQualities".to_string(), JsonValue::Array(values));
        }
    }
    if let Some(values) = string_list_config(config.get("requiredLanguages")) {
        if !values.is_empty() {
            profile.insert("requiredLanguages".to_string(), JsonValue::Array(values));
        }
    }
    if let Some(max_size_bytes) = max_size_bytes_from_config(config) {
        profile.insert("maxSizeBytes".to_string(), json!(max_size_bytes));
    }
    (!profile.is_empty()).then_some(JsonValue::Object(profile))
}

fn string_list_config(value: Option<&JsonValue>) -> Option<Vec<JsonValue>> {
    let values = match value {
        Some(JsonValue::Array(items)) => items
            .iter()
            .filter_map(JsonValue::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| json!(value))
            .collect(),
        Some(JsonValue::String(text)) => text
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| json!(value))
            .collect(),
        _ => return None,
    };
    Some(values)
}

fn max_size_bytes_from_config(config: &JsonMap<String, JsonValue>) -> Option<u64> {
    let raw_bytes = config.get("maxSizeBytes").and_then(JsonValue::as_u64);
    if raw_bytes.is_some() {
        return raw_bytes;
    }
    let max_size_gb = config
        .get("maxSizeGb")
        .and_then(|value| value.as_f64().filter(|gb| *gb > 0.0))?;
    Some((max_size_gb * 1024.0 * 1024.0 * 1024.0).round() as u64)
}

pub async fn get_acquisition_subscription(
    _user: CurrentUser,
    State(state): State<AppState>,
    Path(subscription_id): Path<Uuid>,
) -> ApiResult<Json<AcquisitionSubscriptionDetail>> {
    let detail = get_subscription_detail(&state.db_pool, subscription_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("acquisition subscription not found"))?;
    Ok(Json(detail))
}

pub async fn patch_acquisition_subscription(
    _user: CurrentUser,
    State(state): State<AppState>,
    Path(subscription_id): Path<Uuid>,
    Json(request): Json<AcquisitionSubscriptionUpdate>,
) -> ApiResult<Json<AcquisitionSubscriptionDetail>> {
    update_subscription(&state.db_pool, subscription_id, request)
        .await
        .map_err(map_acquisition_input_error)?
        .ok_or_else(|| ApiError::not_found("acquisition subscription not found"))?;
    let detail = get_subscription_detail(&state.db_pool, subscription_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("acquisition subscription not found"))?;
    Ok(Json(detail))
}

pub async fn cancel_acquisition_subscription(
    _user: CurrentUser,
    State(state): State<AppState>,
    Path(subscription_id): Path<Uuid>,
    Json(request): Json<CancelAcquisitionSubscriptionRequest>,
) -> ApiResult<Json<CancelAcquisitionSubscriptionResponse>> {
    let store = ExtensionStore::new(&state.db_pool);
    let jobs = if request.mode == CancelAcquisitionSubscriptionMode::CancelDownloads {
        list_active_subscription_download_jobs(&state.db_pool, subscription_id)
            .await
            .map_err(ApiError::from)?
    } else {
        Vec::new()
    };
    let mut failures = Vec::new();
    let mut downloads_cancelled = 0usize;
    for job in &jobs {
        match cancel_download_item(
            &state,
            &store,
            &job.route_logical_id,
            Some(&job.owner_id),
            &job.download_id,
            request.delete_files.unwrap_or(false),
        )
        .await
        {
            Ok(response) if response.removed => {
                downloads_cancelled += 1;
            }
            Ok(_) => failures.push(CancelAcquisitionDownloadFailure {
                route_logical_id: job.route_logical_id.clone(),
                download_id: job.download_id.clone(),
                error: "Downloader did not report an active item for this id.".to_string(),
            }),
            Err(err) => failures.push(CancelAcquisitionDownloadFailure {
                route_logical_id: job.route_logical_id.clone(),
                download_id: job.download_id.clone(),
                error: api_error_message(&err).to_string(),
            }),
        }
    }

    let reason = request
        .reason
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| request.mode.default_reason());
    let result = stop_subscription_tracking(&state.db_pool, subscription_id, reason)
        .await
        .map_err(map_acquisition_input_error)?
        .ok_or_else(|| ApiError::not_found("acquisition subscription not found"))?;

    Ok(Json(CancelAcquisitionSubscriptionResponse {
        mode: request.mode,
        message: request.mode.message(failures.len()),
        result,
        downloads_cancel_attempted: jobs.len(),
        downloads_cancelled,
        download_cancel_failures: failures,
    }))
}

pub async fn upsert_acquisition_targets(
    _user: CurrentUser,
    State(state): State<AppState>,
    Path(subscription_id): Path<Uuid>,
    Json(request): Json<UpsertAcquisitionTargetsRequest>,
) -> ApiResult<Json<AcquisitionTargetsResponse>> {
    let targets = upsert_subscription_targets(&state.db_pool, subscription_id, request.targets)
        .await
        .map_err(map_acquisition_input_error)?;
    Ok(Json(AcquisitionTargetsResponse { targets }))
}

pub async fn patch_acquisition_target_state(
    _user: CurrentUser,
    State(state): State<AppState>,
    Path(target_id): Path<Uuid>,
    Json(request): Json<AcquisitionTargetStateUpdate>,
) -> ApiResult<Json<AcquisitionTarget>> {
    let target = update_target_state(&state.db_pool, target_id, request)
        .await
        .map_err(map_acquisition_input_error)?
        .ok_or_else(|| ApiError::not_found("acquisition target not found"))?;
    Ok(Json(target))
}

pub async fn submit_acquisition_target_candidate(
    _user: CurrentUser,
    State(state): State<AppState>,
    Path(target_id): Path<Uuid>,
    Json(request): Json<SubmitAcquisitionCandidateRequest>,
) -> ApiResult<Json<AcquisitionTargetSubmitResponse>> {
    let store = ExtensionStore::new(&state.db_pool);
    let target = get_target(&state.db_pool, target_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("acquisition target not found"))?;
    let subscription = get_subscription(&state.db_pool, target.subscription_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("acquisition subscription not found"))?;
    let candidate = normalize_acquisition_candidate(request.candidate)
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let source_provider_id = request
        .provider_id
        .or(target.selected_provider_id)
        .or(subscription.source_provider_id)
        .ok_or_else(|| {
            ApiError::bad_request("providerId is required when the target has no source provider")
        })?;
    let source_extension_id =
        source_extension_id_for_candidate_provider(&store, source_provider_id)
            .await
            .map_err(map_source_provider_error)?;
    let mut route_logical_id = select_candidate_route(
        request.selected_route_logical_id.as_deref(),
        subscription.route_policy,
        &candidate,
    )?;
    let selected_candidate =
        selected_candidate_provenance(&candidate, source_provider_id, &source_extension_id)?;
    let broker_request = DownloadBrokerSubmitRequest {
        source: candidate.source.clone(),
        category: request.category,
        paused: request.paused,
        name: request.name,
        priority: request.priority,
        add_to_top: request.add_to_top,
        subscription_id: Some(subscription.subscription_id),
        source_provider_id: Some(source_provider_id),
        source_extension_id: Some(source_extension_id.clone()),
        media_type: Some(subscription.media_type),
        media_title: Some(subscription.title.clone()),
        selected_candidate: Some(candidate.clone()),
        release_fingerprint: None,
    };

    let mut state_reason = format!("Submitted through '{}'.", route_logical_id);
    let broker_response = match submit_to_broker(
        &state,
        &store,
        &route_logical_id,
        Some(&source_extension_id),
        broker_request.clone(),
    )
    .await
    {
        Ok(response) => response,
        Err(err)
            if route_logical_id == DEBRID_DEFAULT_LOGICAL_ID
                && subscription.route_policy == AcquisitionRoutePolicy::DebridFirst
                && candidate_supports_route(&candidate, TORRENT_DEFAULT_LOGICAL_ID) =>
        {
            match submit_to_broker(
                &state,
                &store,
                TORRENT_DEFAULT_LOGICAL_ID,
                Some(&source_extension_id),
                broker_request,
            )
            .await
            {
                Ok(response) => {
                    route_logical_id = TORRENT_DEFAULT_LOGICAL_ID.to_string();
                    state_reason =
                        "Debrid rejected the candidate; submitted torrent fallback.".to_string();
                    response
                }
                Err(fallback_err) => {
                    if should_record_target_blocker(&fallback_err) {
                        let message = format!(
                            "Debrid route failed: {}; torrent fallback failed: {}",
                            acquisition_state_error_message(&err),
                            acquisition_state_error_message(&fallback_err)
                        );
                        set_target_state_after_submission(
                            &state.db_pool,
                            target_id,
                            AcquisitionTargetState::Blocked,
                            Some(message),
                            source_provider_id,
                            &route_logical_id,
                            selected_candidate,
                            None,
                        )
                        .await?;
                    }
                    return Err(err);
                }
            }
        }
        Err(err) => {
            if should_record_target_blocker(&err) {
                let message = acquisition_state_error_message(&err);
                set_target_state_after_submission(
                    &state.db_pool,
                    target_id,
                    AcquisitionTargetState::Blocked,
                    Some(message),
                    source_provider_id,
                    &route_logical_id,
                    selected_candidate,
                    None,
                )
                .await?;
            }
            return Err(err);
        }
    };

    let download_id = broker_response
        .download_id
        .clone()
        .or_else(|| candidate.info_hash.clone());
    let target = set_target_state_after_submission(
        &state.db_pool,
        target_id,
        AcquisitionTargetState::Submitted,
        Some(state_reason),
        source_provider_id,
        &route_logical_id,
        selected_candidate,
        download_id.clone(),
    )
    .await?;

    Ok(Json(AcquisitionTargetSubmitResponse {
        target,
        source_provider_id,
        source_extension_id,
        route_logical_id,
        broker_provider_id: broker_response.provider_id,
        accepted: broker_response.accepted,
        download_id,
    }))
}

async fn list_active_subscription_download_jobs(
    pool: &sqlx::AnyPool,
    subscription_id: Uuid,
) -> anyhow::Result<Vec<ActiveSubscriptionDownloadJob>> {
    let rows = sqlx::query(
        "SELECT
            j.route_logical_id,
            j.download_id,
            r.owner_id
         FROM acquisition_release_jobs j
         JOIN acquisition_releases r ON r.release_id = j.release_id
         WHERE r.subscription_id = ?
           AND j.active = 1
           AND j.download_id IS NOT NULL
           AND j.state IN ('staging', 'ready', 'submitted', 'downloading', 'materializing')",
    )
    .bind(subscription_id.to_string())
    .fetch_all(pool)
    .await
    .context("listing active acquisition download jobs")?;

    rows.into_iter()
        .map(|row| {
            Ok(ActiveSubscriptionDownloadJob {
                route_logical_id: row.try_get::<String, _>("route_logical_id")?,
                owner_id: row.try_get::<String, _>("owner_id")?,
                download_id: row.try_get::<String, _>("download_id")?,
            })
        })
        .collect()
}

async fn source_extension_id_for_candidate_provider(
    store: &ExtensionStore<'_>,
    provider_id: Uuid,
) -> anyhow::Result<String> {
    let provider = store
        .get_provider(provider_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("candidate provider '{provider_id}' was not found"))?;
    if provider.capability != ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY {
        anyhow::bail!(
            "provider '{}' is '{}', not '{}'",
            provider_id,
            provider.capability,
            ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY
        );
    }
    if provider.health_state != ProviderHealthState::Healthy {
        anyhow::bail!("candidate provider '{}' is not healthy", provider_id);
    }
    let instance = store
        .get_instance(provider.instance_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("candidate provider instance was not found"))?;
    if !instance.enabled {
        anyhow::bail!("candidate provider instance is disabled");
    }
    let extension = store
        .get_extension(&instance.extension_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("candidate provider extension was not found"))?;
    if !extension.enabled {
        anyhow::bail!("candidate provider extension is disabled");
    }
    Ok(instance.extension_id)
}

fn select_candidate_route(
    requested_route: Option<&str>,
    route_policy: AcquisitionRoutePolicy,
    candidate: &AcquisitionCandidate,
) -> ApiResult<String> {
    if let Some(route) = requested_route.and_then(non_empty) {
        return validate_selected_candidate_route(route, candidate).map(str::to_string);
    }

    let selected = match route_policy {
        AcquisitionRoutePolicy::DebridFirst => {
            if candidate_supports_route(candidate, DEBRID_DEFAULT_LOGICAL_ID) {
                Some(DEBRID_DEFAULT_LOGICAL_ID)
            } else if candidate_supports_route(candidate, TORRENT_DEFAULT_LOGICAL_ID) {
                Some(TORRENT_DEFAULT_LOGICAL_ID)
            } else {
                candidate.default_route.as_deref()
            }
        }
        AcquisitionRoutePolicy::DebridOnly => Some(DEBRID_DEFAULT_LOGICAL_ID),
        AcquisitionRoutePolicy::TorrentOnly => Some(TORRENT_DEFAULT_LOGICAL_ID),
        AcquisitionRoutePolicy::Manual => candidate.default_route.as_deref().or_else(|| {
            if candidate.supported_routes.len() == 1 {
                candidate.supported_routes.first().map(String::as_str)
            } else {
                None
            }
        }),
    };
    let Some(route) = selected else {
        return Err(ApiError::bad_request(
            "selectedRouteLogicalId is required for this candidate",
        ));
    };
    validate_selected_candidate_route(route, candidate).map(str::to_string)
}

fn validate_selected_candidate_route<'a>(
    route: &'a str,
    candidate: &AcquisitionCandidate,
) -> ApiResult<&'a str> {
    let route = route.trim();
    if route != DEBRID_DEFAULT_LOGICAL_ID && route != TORRENT_DEFAULT_LOGICAL_ID {
        return Err(ApiError::bad_request(format!(
            "unsupported selected route '{}'",
            route
        )));
    }
    if !candidate_supports_route(candidate, route) {
        return Err(ApiError::bad_request(format!(
            "candidate does not support route '{}'",
            route
        )));
    }
    Ok(route)
}

fn candidate_supports_route(candidate: &AcquisitionCandidate, route: &str) -> bool {
    if !candidate.supported_routes.is_empty() {
        return candidate
            .supported_routes
            .iter()
            .any(|item| item.eq_ignore_ascii_case(route));
    }
    match (candidate.source_kind.as_str(), route) {
        ("magnet", DEBRID_DEFAULT_LOGICAL_ID | TORRENT_DEFAULT_LOGICAL_ID) => true,
        ("http" | "hoster", DEBRID_DEFAULT_LOGICAL_ID) => true,
        ("torrent", TORRENT_DEFAULT_LOGICAL_ID) => true,
        _ => false,
    }
}

fn selected_candidate_provenance(
    candidate: &AcquisitionCandidate,
    source_provider_id: Uuid,
    source_extension_id: &str,
) -> ApiResult<JsonValue> {
    let mut value = serde_json::to_value(candidate)
        .map_err(|err| ApiError::internal(format!("serializing selected candidate: {err}")))?;
    if let Some(object) = value.as_object_mut() {
        object.insert(
            "sourceProviderId".to_string(),
            json!(source_provider_id.to_string()),
        );
        object.insert("sourceExtensionId".to_string(), json!(source_extension_id));
    }
    Ok(value)
}

async fn set_target_state_after_submission(
    pool: &sqlx::AnyPool,
    target_id: Uuid,
    state: AcquisitionTargetState,
    state_reason: Option<String>,
    source_provider_id: Uuid,
    route_logical_id: &str,
    selected_candidate: JsonValue,
    download_id: Option<String>,
) -> ApiResult<AcquisitionTarget> {
    update_target_state(
        pool,
        target_id,
        AcquisitionTargetStateUpdate {
            state,
            state_reason,
            selected_provider_id: Some(source_provider_id),
            selected_route_logical_id: Some(route_logical_id.to_string()),
            selected_candidate: Some(selected_candidate),
            download_id,
            import_event_id: None,
            next_search_after: None,
            increment_search_attempts: false,
        },
    )
    .await
    .map_err(map_acquisition_input_error)?
    .ok_or_else(|| ApiError::not_found("acquisition target not found"))
}

fn should_record_target_blocker(err: &ApiError) -> bool {
    matches!(err, ApiError::Conflict(_) | ApiError::NotFound(_))
}

fn api_error_message(err: &ApiError) -> &str {
    match err {
        ApiError::BadRequest(message)
        | ApiError::Unauthorized(message)
        | ApiError::Forbidden(message)
        | ApiError::NotFound(message)
        | ApiError::Conflict(message)
        | ApiError::Internal(message) => message,
    }
}

fn acquisition_state_error_message(err: &ApiError) -> String {
    let message = api_error_message(err);
    generic_debrid_error_message(message)
        .unwrap_or(message)
        .to_string()
}

fn map_source_provider_error(err: anyhow::Error) -> ApiError {
    let message = err.to_string();
    if message.contains("not found") {
        ApiError::not_found(message)
    } else if message.contains("disabled")
        || message.contains("unhealthy")
        || message.contains("not healthy")
    {
        ApiError::conflict(message)
    } else {
        ApiError::bad_request(message)
    }
}

fn non_empty(value: &str) -> Option<&str> {
    let value = value.trim();
    (!value.is_empty()).then_some(value)
}

fn map_acquisition_input_error(err: anyhow::Error) -> ApiError {
    let message = err.to_string();
    if message.contains("not found") {
        ApiError::not_found(message)
    } else if message.contains("required")
        || message.contains("cannot")
        || message.contains("must")
        || message.contains("duplicate")
        || message.contains("unknown acquisition")
        || message.contains("targetKey")
    {
        ApiError::bad_request(message)
    } else {
        ApiError::internal(message)
    }
}
