use axum::{
    Json,
    extract::{Path as AxumPath, Query, State},
};
use serde::{Deserialize, Serialize};

use crate::{
    http::{
        auth::CurrentUser,
        error::{ApiError, ApiResult},
    },
    media_interactions::{
        ActiveMediaSegmentRecord, BuiltinProviderRefreshOptions, BuiltinProviderRefreshSummary,
        MediaInteractionLibrarySettingsPatch, MediaInteractionLibrarySettingsRecord,
        MediaSegmentCandidateReviewFilters, MediaSegmentItemAnalyzeRequest,
        MediaSegmentItemAnalyzeSummary, MediaSegmentJobActionRequest,
        MediaSegmentJobEnqueueRequest, MediaSegmentJobListFilters, MediaSegmentJobRecord,
        MediaSegmentProviderCertificationFilters, MediaSegmentProviderCertificationRecord,
        MediaSegmentWorkerIterationSummary, SegmentCandidateInput, SegmentCandidateOutcome,
        SegmentCandidateRecord, cancel_media_segment_job, certify_media_segment_provider,
        disable_active_segment, enqueue_media_segment_item_analysis,
        enqueue_media_segment_job_request_with_marketplace, list_active_segments_for_file,
        list_active_segments_for_item, list_media_interaction_library_settings,
        list_media_segment_jobs, list_media_segment_provider_certifications,
        list_segment_candidate_review_queue, list_segment_candidates_for_file,
        list_segment_candidates_for_item, load_media_interaction_library_settings,
        load_or_create_playback_preferences, refresh_builtin_provider_segments,
        refresh_chapter_segments_from_probe, retry_media_segment_job,
        run_media_segment_job_worker_iteration, submit_segment_candidate,
        update_media_interaction_library_settings,
    },
    state::AppState,
};

#[derive(Debug, Serialize)]
pub struct FileMediaSegmentsResponse {
    pub media_file_id: String,
    pub active: Vec<ActiveMediaSegmentRecord>,
    pub candidates: Vec<SegmentCandidateRecord>,
}

#[derive(Debug, Serialize)]
pub struct ItemMediaSegmentsResponse {
    pub item_type: String,
    pub item_id: String,
    pub active: Vec<ActiveMediaSegmentRecord>,
    pub candidates: Vec<SegmentCandidateRecord>,
}

#[derive(Debug, Deserialize)]
pub struct DisableSegmentRequest {
    pub reason: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct DisableSegmentResponse {
    pub disabled: bool,
    pub segment: Option<ActiveMediaSegmentRecord>,
    pub active: Vec<ActiveMediaSegmentRecord>,
}

#[derive(Debug, Serialize)]
pub struct RefreshBuiltinProvidersResponse {
    pub summary: BuiltinProviderRefreshSummary,
    pub active: Vec<ActiveMediaSegmentRecord>,
    pub candidates: Vec<SegmentCandidateRecord>,
}

#[derive(Debug, Serialize)]
pub struct MediaSegmentJobsResponse {
    pub jobs: Vec<MediaSegmentJobRecord>,
}

#[derive(Debug, Serialize)]
pub struct MediaSegmentProviderCertificationsResponse {
    pub certifications: Vec<MediaSegmentProviderCertificationRecord>,
}

#[derive(Debug, Serialize)]
pub struct MediaSegmentProviderCertificationResponse {
    pub certification: MediaSegmentProviderCertificationRecord,
}

#[derive(Debug, Serialize)]
pub struct EnqueueMediaSegmentJobResponse {
    pub job: MediaSegmentJobRecord,
}

#[derive(Debug, Serialize)]
pub struct MediaSegmentCandidateReviewResponse {
    pub candidates: Vec<SegmentCandidateRecord>,
}

#[derive(Debug, Serialize)]
pub struct MediaSegmentJobActionResponse {
    pub job: MediaSegmentJobRecord,
}

#[derive(Debug, Serialize)]
pub struct MediaSegmentWorkerRunResponse {
    pub summary: MediaSegmentWorkerIterationSummary,
}

#[derive(Debug, Serialize)]
pub struct MediaSegmentItemAnalyzeResponse {
    pub summary: MediaSegmentItemAnalyzeSummary,
}

#[derive(Debug, Serialize)]
pub struct MediaInteractionLibrarySettingsListResponse {
    pub libraries: Vec<MediaInteractionLibrarySettingsRecord>,
}

#[derive(Debug, Serialize)]
pub struct MediaInteractionLibrarySettingsResponse {
    pub library: MediaInteractionLibrarySettingsRecord,
}

pub async fn list_file_segments(
    State(state): State<AppState>,
    _user: CurrentUser,
    AxumPath(media_file_id): AxumPath<String>,
) -> ApiResult<Json<FileMediaSegmentsResponse>> {
    require_media_interaction_support_api(&state)?;
    let active = list_active_segments_for_file(&state.db_pool, &media_file_id)
        .await
        .map_err(map_media_interaction_error)?;
    let candidates = list_segment_candidates_for_file(&state.db_pool, &media_file_id)
        .await
        .map_err(map_media_interaction_error)?;

    Ok(Json(FileMediaSegmentsResponse {
        media_file_id,
        active,
        candidates,
    }))
}

pub async fn list_item_segments(
    State(state): State<AppState>,
    _user: CurrentUser,
    AxumPath((item_type, item_id)): AxumPath<(String, String)>,
) -> ApiResult<Json<ItemMediaSegmentsResponse>> {
    require_media_interaction_support_api(&state)?;
    let active = list_active_segments_for_item(&state.db_pool, &item_type, &item_id)
        .await
        .map_err(map_media_interaction_error)?;
    let candidates = list_segment_candidates_for_item(&state.db_pool, &item_type, &item_id)
        .await
        .map_err(map_media_interaction_error)?;
    let normalized_item_type = item_type.trim().to_ascii_lowercase().replace('-', "_");

    Ok(Json(ItemMediaSegmentsResponse {
        item_type: normalized_item_type,
        item_id,
        active,
        candidates,
    }))
}

pub async fn refresh_file_chapter_segments(
    State(state): State<AppState>,
    _user: CurrentUser,
    AxumPath(media_file_id): AxumPath<String>,
) -> ApiResult<Json<FileMediaSegmentsResponse>> {
    require_media_interaction_support_api(&state)?;
    refresh_chapter_segments_from_probe(&state.db_pool, &media_file_id)
        .await
        .map_err(map_media_interaction_error)?;
    list_file_segments(State(state), _user, AxumPath(media_file_id)).await
}

pub async fn refresh_file_builtin_provider_segments(
    State(state): State<AppState>,
    user: CurrentUser,
    AxumPath(media_file_id): AxumPath<String>,
    body: Option<Json<BuiltinProviderRefreshOptions>>,
) -> ApiResult<Json<RefreshBuiltinProvidersResponse>> {
    require_media_interaction_support_api(&state)?;
    let preferences = load_or_create_playback_preferences(&state.db_pool, user.user_id)
        .await
        .map_err(map_media_interaction_error)?;
    let summary = refresh_builtin_provider_segments(
        &state.db_pool,
        &media_file_id,
        &preferences,
        body.map(|Json(value)| value).unwrap_or_default(),
    )
    .await
    .map_err(map_media_interaction_error)?;
    let active = list_active_segments_for_file(&state.db_pool, &media_file_id)
        .await
        .map_err(map_media_interaction_error)?;
    let candidates = list_segment_candidates_for_file(&state.db_pool, &media_file_id)
        .await
        .map_err(map_media_interaction_error)?;

    Ok(Json(RefreshBuiltinProvidersResponse {
        summary,
        active,
        candidates,
    }))
}

pub async fn create_segment_candidate(
    State(state): State<AppState>,
    _user: CurrentUser,
    Json(body): Json<SegmentCandidateInput>,
) -> ApiResult<Json<SegmentCandidateOutcome>> {
    require_media_interaction_support_api(&state)?;
    reject_manual_segment_candidate_from_support_api(&body)?;
    let outcome = submit_segment_candidate(&state.db_pool, body)
        .await
        .map_err(map_media_interaction_error)?;
    Ok(Json(outcome))
}

pub async fn list_segment_candidate_review(
    State(state): State<AppState>,
    _user: CurrentUser,
    Query(filters): Query<MediaSegmentCandidateReviewFilters>,
) -> ApiResult<Json<MediaSegmentCandidateReviewResponse>> {
    require_media_interaction_support_api(&state)?;
    let candidates = list_segment_candidate_review_queue(&state.db_pool, filters)
        .await
        .map_err(map_media_interaction_error)?;
    Ok(Json(MediaSegmentCandidateReviewResponse { candidates }))
}

pub async fn disable_segment(
    State(state): State<AppState>,
    _user: CurrentUser,
    AxumPath(segment_id): AxumPath<String>,
    Json(body): Json<DisableSegmentRequest>,
) -> ApiResult<Json<DisableSegmentResponse>> {
    require_media_interaction_support_api(&state)?;
    let disabled = disable_active_segment(&state.db_pool, &segment_id, body.reason.as_deref())
        .await
        .map_err(map_media_interaction_error)?;
    let active = if let Some(segment) = disabled.as_ref() {
        list_active_segments_for_file(&state.db_pool, &segment.media_file_id)
            .await
            .map_err(map_media_interaction_error)?
    } else {
        Vec::new()
    };

    Ok(Json(DisableSegmentResponse {
        disabled: disabled.is_some(),
        segment: disabled,
        active,
    }))
}

pub async fn list_jobs(
    State(state): State<AppState>,
    _user: CurrentUser,
    Query(filters): Query<MediaSegmentJobListFilters>,
) -> ApiResult<Json<MediaSegmentJobsResponse>> {
    require_media_interaction_support_api(&state)?;
    let jobs = list_media_segment_jobs(&state.db_pool, filters)
        .await
        .map_err(map_media_interaction_error)?;
    Ok(Json(MediaSegmentJobsResponse { jobs }))
}

pub async fn list_provider_certifications(
    State(state): State<AppState>,
    _user: CurrentUser,
    Query(filters): Query<MediaSegmentProviderCertificationFilters>,
) -> ApiResult<Json<MediaSegmentProviderCertificationsResponse>> {
    require_media_interaction_support_api(&state)?;
    let certifications = list_media_segment_provider_certifications(&state.db_pool, filters)
        .await
        .map_err(map_media_interaction_error)?;
    Ok(Json(MediaSegmentProviderCertificationsResponse {
        certifications,
    }))
}

pub async fn certify_provider(
    State(state): State<AppState>,
    _user: CurrentUser,
    AxumPath(provider_id): AxumPath<String>,
) -> ApiResult<Json<MediaSegmentProviderCertificationResponse>> {
    require_media_interaction_support_api(&state)?;
    let certification = certify_media_segment_provider(&state.db_pool, &provider_id)
        .await
        .map_err(map_media_interaction_error)?;
    Ok(Json(MediaSegmentProviderCertificationResponse {
        certification,
    }))
}

pub async fn enqueue_job(
    State(state): State<AppState>,
    _user: CurrentUser,
    Json(body): Json<MediaSegmentJobEnqueueRequest>,
) -> ApiResult<Json<EnqueueMediaSegmentJobResponse>> {
    require_media_interaction_support_api(&state)?;
    let job = enqueue_media_segment_job_request_with_marketplace(&state.db_pool, body)
        .await
        .map_err(map_media_interaction_error)?;
    Ok(Json(EnqueueMediaSegmentJobResponse { job }))
}

pub async fn analyze_item(
    State(state): State<AppState>,
    user: CurrentUser,
    AxumPath((item_type, item_id)): AxumPath<(String, String)>,
    body: Option<Json<MediaSegmentItemAnalyzeRequest>>,
) -> ApiResult<Json<MediaSegmentItemAnalyzeResponse>> {
    require_media_interaction_support_api(&state)?;
    let preferences = load_or_create_playback_preferences(&state.db_pool, user.user_id)
        .await
        .map_err(map_media_interaction_error)?;
    let summary = enqueue_media_segment_item_analysis(
        &state.db_pool,
        &item_type,
        &item_id,
        &preferences,
        body.map(|Json(value)| value).unwrap_or_default(),
    )
    .await
    .map_err(map_media_interaction_error)?;
    Ok(Json(MediaSegmentItemAnalyzeResponse { summary }))
}

pub async fn list_library_settings(
    State(state): State<AppState>,
    _user: CurrentUser,
) -> ApiResult<Json<MediaInteractionLibrarySettingsListResponse>> {
    require_media_interaction_support_api(&state)?;
    let libraries = list_media_interaction_library_settings(&state.db_pool)
        .await
        .map_err(map_media_interaction_error)?;
    Ok(Json(MediaInteractionLibrarySettingsListResponse {
        libraries,
    }))
}

pub async fn get_library_settings(
    State(state): State<AppState>,
    _user: CurrentUser,
    AxumPath(source_config_id): AxumPath<String>,
) -> ApiResult<Json<MediaInteractionLibrarySettingsResponse>> {
    require_media_interaction_support_api(&state)?;
    let library = load_media_interaction_library_settings(&state.db_pool, &source_config_id)
        .await
        .map_err(map_media_interaction_error)?;
    Ok(Json(MediaInteractionLibrarySettingsResponse { library }))
}

pub async fn update_library_settings(
    State(state): State<AppState>,
    _user: CurrentUser,
    AxumPath(source_config_id): AxumPath<String>,
    Json(body): Json<MediaInteractionLibrarySettingsPatch>,
) -> ApiResult<Json<MediaInteractionLibrarySettingsResponse>> {
    require_media_interaction_support_api(&state)?;
    let library =
        update_media_interaction_library_settings(&state.db_pool, &source_config_id, body)
            .await
            .map_err(map_media_interaction_error)?;
    Ok(Json(MediaInteractionLibrarySettingsResponse { library }))
}

pub async fn cancel_job(
    State(state): State<AppState>,
    _user: CurrentUser,
    AxumPath(job_id): AxumPath<String>,
    body: Option<Json<MediaSegmentJobActionRequest>>,
) -> ApiResult<Json<MediaSegmentJobActionResponse>> {
    require_media_interaction_support_api(&state)?;
    let job = cancel_media_segment_job(
        &state.db_pool,
        &job_id,
        body.as_ref()
            .and_then(|Json(value)| value.reason.as_deref()),
    )
    .await
    .map_err(map_media_interaction_error)?;
    Ok(Json(MediaSegmentJobActionResponse { job }))
}

pub async fn retry_job(
    State(state): State<AppState>,
    _user: CurrentUser,
    AxumPath(job_id): AxumPath<String>,
    body: Option<Json<MediaSegmentJobActionRequest>>,
) -> ApiResult<Json<MediaSegmentJobActionResponse>> {
    require_media_interaction_support_api(&state)?;
    let job = retry_media_segment_job(
        &state.db_pool,
        &job_id,
        body.as_ref()
            .and_then(|Json(value)| value.reason.as_deref()),
    )
    .await
    .map_err(map_media_interaction_error)?;
    Ok(Json(MediaSegmentJobActionResponse { job }))
}

pub async fn run_worker(
    State(state): State<AppState>,
    _user: CurrentUser,
) -> ApiResult<Json<MediaSegmentWorkerRunResponse>> {
    require_media_interaction_support_api(&state)?;
    let summary = run_media_segment_job_worker_iteration(&state)
        .await
        .map_err(map_media_interaction_error)?;
    Ok(Json(MediaSegmentWorkerRunResponse { summary }))
}

fn require_media_interaction_support_api(state: &AppState) -> ApiResult<()> {
    if state.settings.media_interactions.support_api_enabled {
        Ok(())
    } else {
        Err(ApiError::forbidden(
            "MIDM support APIs are disabled for this server",
        ))
    }
}

fn reject_manual_segment_candidate_from_support_api(
    input: &SegmentCandidateInput,
) -> ApiResult<()> {
    let provider_kind = input.provider_kind.trim().to_ascii_lowercase();
    let identity_strength = input.identity_strength.trim().to_ascii_lowercase();
    if provider_kind == "manual" || identity_strength == "manual" {
        return Err(ApiError::bad_request(
            "manual media segment candidates are not supported through MIDM support APIs",
        ));
    }
    Ok(())
}

fn map_media_interaction_error(err: anyhow::Error) -> ApiError {
    let message = err.to_string();
    if message.contains("not found") || message.contains("missing") {
        ApiError::not_found(message)
    } else if message.contains("required")
        || message.contains("must")
        || message.contains("invalid")
        || message.contains("cannot")
        || message.contains("outside")
        || message.contains("no raw")
        || message.contains("unsupported")
    {
        ApiError::bad_request(message)
    } else {
        ApiError::internal(message)
    }
}
