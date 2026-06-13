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
    acquisition::{
        language_policy::{
            load_saved_language_preference, quality_profile_with_language_preference,
        },
        release_resolution::fingerprint::candidate_release_fingerprint,
        route_attempts::{RouteAttemptRecord, RouteAttemptStatus, attach_route_attempt_ledger},
        subscriptions::{
            AcquisitionRequestBlocker, AcquisitionRequestRetryResult,
            AcquisitionRequestTargetCounts, AcquisitionRoutePolicy, AcquisitionSubscription,
            AcquisitionSubscriptionDetail, AcquisitionSubscriptionFilter,
            AcquisitionSubscriptionStopTrackingResult, AcquisitionSubscriptionUpdate,
            AcquisitionTarget, AcquisitionTargetState, AcquisitionTargetStateUpdate,
            CreateAcquisitionIntent, NewAcquisitionSubscription, NewAcquisitionTarget,
            acquisition_request_blockers, acquisition_request_target_counts,
            create_or_update_acquisition_intent, create_subscription, get_subscription,
            get_subscription_detail, get_target, list_subscriptions,
            retry_acquisition_request as retry_acquisition_request_store,
            stop_subscription_tracking, update_subscription, update_target_state,
            upsert_subscription_targets, validate_new_targets,
        },
    },
    db::models::ProviderHealthState,
    download_broker::{
        DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID, USENET_DEFAULT_LOGICAL_ID,
    },
    extensions::store::ExtensionStore,
    http::{
        auth::CurrentUser,
        error::{ApiError, ApiResult},
        handlers::{
            acquisition_sources::{
                ACQUISITION_CANDIDATE_PROVIDER_CAPABILITY, AcquisitionCandidate,
                is_extension_suite_source_provider_capability, normalize_acquisition_candidate,
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

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AcquisitionRequestResponse {
    request: AcquisitionSubscription,
    targets: Vec<AcquisitionTarget>,
    target_counts: AcquisitionRequestTargetCounts,
    blockers: Vec<AcquisitionRequestBlocker>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetryAcquisitionRequestRequest {
    #[serde(default)]
    pub reason: Option<String>,
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

pub async fn create_acquisition_request(
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
        let language_preference = load_saved_language_preference(store)
            .await
            .map_err(ApiError::from)?;
        request.quality_profile = quality_profile_with_language_preference(
            request.quality_profile.take(),
            request.media_type,
            &language_preference,
        );
        return Ok(());
    };
    let provider = store
        .get_provider(source_provider_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::bad_request("source provider was not found"))?;
    if !is_extension_suite_source_provider_capability(&provider.capability) {
        return Err(ApiError::bad_request(
            "source provider must be an Extension Suite source provider",
        ));
    }
    let instance = store
        .get_instance(provider.instance_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::bad_request("source provider instance was not found"))?;
    if let Some(config) = instance.config_json.as_ref().and_then(JsonValue::as_object) {
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
    }
    let language_preference = load_saved_language_preference(store)
        .await
        .map_err(ApiError::from)?;
    request.quality_profile = quality_profile_with_language_preference(
        request.quality_profile.take(),
        request.media_type,
        &language_preference,
    );
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

fn acquisition_request_response(
    detail: AcquisitionSubscriptionDetail,
) -> AcquisitionRequestResponse {
    let target_counts = acquisition_request_target_counts(&detail.targets);
    let blockers = acquisition_request_blockers(&detail.targets);
    AcquisitionRequestResponse {
        request: detail.subscription,
        targets: detail.targets,
        target_counts,
        blockers,
    }
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

pub async fn get_acquisition_request(
    _user: CurrentUser,
    State(state): State<AppState>,
    Path(subscription_id): Path<Uuid>,
) -> ApiResult<Json<AcquisitionRequestResponse>> {
    let detail = get_subscription_detail(&state.db_pool, subscription_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("acquisition request not found"))?;
    Ok(Json(acquisition_request_response(detail)))
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

pub async fn retry_acquisition_request(
    _user: CurrentUser,
    State(state): State<AppState>,
    Path(subscription_id): Path<Uuid>,
    Json(request): Json<RetryAcquisitionRequestRequest>,
) -> ApiResult<Json<AcquisitionRequestRetryResult>> {
    let result = retry_acquisition_request_store(
        &state.db_pool,
        subscription_id,
        chrono::Utc::now(),
        request.reason.as_deref(),
    )
    .await
    .map_err(map_acquisition_input_error)?
    .ok_or_else(|| ApiError::not_found("acquisition request not found"))?;
    Ok(Json(result))
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
    let mut selected_candidate =
        selected_candidate_provenance(&candidate, source_provider_id, &source_extension_id)?;
    let candidate_fingerprint = candidate_release_fingerprint(&candidate, Some(source_provider_id));
    let mut route_attempts = Vec::new();
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
        selected_stream_candidate: None,
        release_fingerprint: Some(candidate_fingerprint.clone()),
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
                && candidate_local_fallback_route(&candidate).is_some() =>
        {
            let local_route = candidate_local_fallback_route(&candidate)
                .expect("checked candidate has local fallback route");
            route_attempts.push(RouteAttemptRecord::new(
                &route_logical_id,
                None,
                None,
                None,
                RouteAttemptStatus::Failed,
                None,
                Some(format!(
                    "Debrid route failed before direct target fallback: {}",
                    acquisition_state_error_message(&err)
                )),
            ));
            match submit_to_broker(
                &state,
                &store,
                local_route,
                Some(&source_extension_id),
                broker_request,
            )
            .await
            {
                Ok(response) => {
                    route_logical_id = local_route.to_string();
                    state_reason = format!(
                        "Debrid rejected the candidate; submitted {} fallback.",
                        local_route_label(local_route)
                    );
                    response
                }
                Err(fallback_err) => {
                    if should_record_target_blocker(&fallback_err) {
                        let message = format!(
                            "Debrid route failed: {}; {} fallback failed: {}",
                            acquisition_state_error_message(&err),
                            local_route_label(local_route),
                            acquisition_state_error_message(&fallback_err)
                        );
                        route_attempts.push(RouteAttemptRecord::new(
                            local_route,
                            None,
                            None,
                            None,
                            RouteAttemptStatus::Failed,
                            None,
                            Some(format!(
                                "{} fallback failed: {}",
                                local_route_label_title_case(local_route),
                                acquisition_state_error_message(&fallback_err)
                            )),
                        ));
                        attach_route_attempt_ledger(
                            &mut selected_candidate,
                            &candidate_fingerprint,
                            &route_attempts,
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
                    return Err(fallback_err);
                }
            }
        }
        Err(err) => {
            if should_record_target_blocker(&err) {
                let message = acquisition_state_error_message(&err);
                route_attempts.push(RouteAttemptRecord::new(
                    &route_logical_id,
                    None,
                    None,
                    None,
                    RouteAttemptStatus::Blocked,
                    None,
                    Some(message.clone()),
                ));
                attach_route_attempt_ledger(
                    &mut selected_candidate,
                    &candidate_fingerprint,
                    &route_attempts,
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
    };

    let download_id = broker_response
        .download_id
        .clone()
        .or_else(|| candidate.info_hash.clone());
    route_attempts.push(RouteAttemptRecord::new(
        &broker_response.logical_id,
        Some(broker_response.provider_id),
        broker_response.provider_implementation.as_deref(),
        broker_response.download_id.clone(),
        RouteAttemptStatus::Submitted,
        None,
        Some(state_reason.clone()),
    ));
    attach_route_attempt_ledger(
        &mut selected_candidate,
        &candidate_fingerprint,
        &route_attempts,
    );
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
            } else if let Some(route) = candidate_local_fallback_route(candidate) {
                Some(route)
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
    if route != DEBRID_DEFAULT_LOGICAL_ID
        && route != TORRENT_DEFAULT_LOGICAL_ID
        && route != USENET_DEFAULT_LOGICAL_ID
    {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandidateRouteFamily {
    Debrid,
    Torrent,
    Usenet,
}

fn candidate_route_family(route: &str) -> Option<CandidateRouteFamily> {
    match route {
        DEBRID_DEFAULT_LOGICAL_ID => Some(CandidateRouteFamily::Debrid),
        TORRENT_DEFAULT_LOGICAL_ID => Some(CandidateRouteFamily::Torrent),
        USENET_DEFAULT_LOGICAL_ID => Some(CandidateRouteFamily::Usenet),
        _ => None,
    }
}

fn candidate_local_fallback_route(candidate: &AcquisitionCandidate) -> Option<&'static str> {
    if candidate_supports_route(candidate, TORRENT_DEFAULT_LOGICAL_ID) {
        Some(TORRENT_DEFAULT_LOGICAL_ID)
    } else if candidate_supports_route(candidate, USENET_DEFAULT_LOGICAL_ID) {
        Some(USENET_DEFAULT_LOGICAL_ID)
    } else {
        None
    }
}

fn candidate_supports_route(candidate: &AcquisitionCandidate, route: &str) -> bool {
    if !candidate.supported_routes.is_empty() {
        return candidate
            .supported_routes
            .iter()
            .any(|item| item.eq_ignore_ascii_case(route));
    }
    let source_kind = candidate.source_kind.trim().to_ascii_lowercase();
    match candidate_route_family(route) {
        Some(CandidateRouteFamily::Debrid) => matches!(
            source_kind.as_str(),
            "magnet" | "http" | "hoster" | "url" | "nzb" | "usenet"
        ),
        Some(CandidateRouteFamily::Torrent) => matches!(source_kind.as_str(), "magnet" | "torrent"),
        Some(CandidateRouteFamily::Usenet) => {
            matches!(source_kind.as_str(), "nzb" | "usenet")
                || (matches!(source_kind.as_str(), "http" | "url")
                    && candidate_source_looks_like_nzb(&candidate.source))
        }
        None => false,
    }
}

fn candidate_source_looks_like_nzb(source: &str) -> bool {
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

fn local_route_label(route: &str) -> &'static str {
    match route {
        TORRENT_DEFAULT_LOGICAL_ID => "torrent",
        USENET_DEFAULT_LOGICAL_ID => "usenet",
        _ => "local",
    }
}

fn local_route_label_title_case(route: &str) -> &'static str {
    match route {
        TORRENT_DEFAULT_LOGICAL_ID => "Torrent",
        USENET_DEFAULT_LOGICAL_ID => "Usenet",
        _ => "Local",
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

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(source: &str, source_kind: &str, routes: Vec<&str>) -> AcquisitionCandidate {
        AcquisitionCandidate {
            id: Some("candidate-1".to_string()),
            title: "Example.Release.1080p-GROUP".to_string(),
            source: source.to_string(),
            source_kind: source_kind.to_string(),
            info_hash: None,
            file_index: None,
            quality: Some("1080p".to_string()),
            size_bytes: Some(1024),
            seeders: None,
            language: None,
            cached_debrid: None,
            rank: None,
            score: None,
            score_badges: Vec::new(),
            files: Vec::new(),
            supported_routes: routes.into_iter().map(ToString::to_string).collect(),
            default_route: None,
            raw: None,
        }
    }

    #[test]
    fn dfu6_direct_submit_route_support_accepts_usenet_nzb() {
        let nzb = candidate(
            "https://indexer.example/releases/example.nzb",
            "nzb",
            Vec::new(),
        );

        assert!(candidate_supports_route(&nzb, DEBRID_DEFAULT_LOGICAL_ID));
        assert!(candidate_supports_route(&nzb, USENET_DEFAULT_LOGICAL_ID));
        assert!(!candidate_supports_route(&nzb, TORRENT_DEFAULT_LOGICAL_ID));
        assert_eq!(
            candidate_local_fallback_route(&nzb),
            Some(USENET_DEFAULT_LOGICAL_ID)
        );
    }

    #[test]
    fn dfu6_direct_submit_route_support_keeps_hoster_debrid_only() {
        let hoster = candidate("https://hoster.example/video.mkv", "http", Vec::new());

        assert!(candidate_supports_route(&hoster, DEBRID_DEFAULT_LOGICAL_ID));
        assert!(!candidate_supports_route(
            &hoster,
            TORRENT_DEFAULT_LOGICAL_ID
        ));
        assert!(!candidate_supports_route(
            &hoster,
            USENET_DEFAULT_LOGICAL_ID
        ));
        assert_eq!(candidate_local_fallback_route(&hoster), None);
    }

    #[test]
    fn dfu6_direct_submit_selects_usenet_for_nzb_when_debrid_not_supported() {
        let nzb = candidate(
            "https://indexer.example/download?id=123",
            "nzb",
            vec![USENET_DEFAULT_LOGICAL_ID],
        );

        assert_eq!(
            select_candidate_route(None, AcquisitionRoutePolicy::DebridFirst, &nzb).expect("route"),
            USENET_DEFAULT_LOGICAL_ID
        );
    }

    #[test]
    fn dfu6_direct_submit_rejects_magnet_to_usenet() {
        let magnet = candidate("magnet:?xt=urn:btih:abc", "magnet", Vec::new());

        let err = validate_selected_candidate_route(USENET_DEFAULT_LOGICAL_ID, &magnet)
            .expect_err("magnet should not support usenet");
        assert!(api_error_message(&err).contains("does not support route"));
    }
}
