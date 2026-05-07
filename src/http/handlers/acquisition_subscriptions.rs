use axum::{
    Json,
    extract::{Path, Query, State},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    acquisition::subscriptions::{
        AcquisitionSubscription, AcquisitionSubscriptionDetail, AcquisitionSubscriptionFilter,
        AcquisitionSubscriptionUpdate, AcquisitionTarget, AcquisitionTargetStateUpdate,
        NewAcquisitionSubscription, NewAcquisitionTarget, create_subscription,
        get_subscription_detail, list_subscriptions, update_subscription, update_target_state,
        upsert_subscription_targets, validate_new_targets,
    },
    http::{
        auth::CurrentUser,
        error::{ApiError, ApiResult},
    },
    state::AppState,
};

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
