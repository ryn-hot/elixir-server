use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

use anyhow::Result;
use axum::{
    Json,
    extract::{Path, Query, State},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue, json};
use sha2::{Digest, Sha256};
use sqlx::AnyPool;
use uuid::Uuid;

use crate::{
    acquisition::{
        audit::{
            EVENT_INSPECT_REQUESTED, EVENT_MANUAL_APPROVAL, EVENT_MANUAL_REJECTION,
            NewAcquisitionAuditEvent, record_acquisition_audit_event,
        },
        imports::{
            AcquisitionImportFileLink, AcquisitionImportRun, AcquisitionImportRunState,
            cancel_import_runs_for_release, list_import_file_links,
            list_import_file_links_by_release, list_import_runs_by_release,
            reset_import_runs_for_release,
        },
        release_resolution::{
            models::{
                AcquisitionAnimeIdentityMismatch, AcquisitionAnimeMatchAttempt,
                AcquisitionFileHash, AcquisitionRelease, AcquisitionReleaseCoverage,
                AcquisitionReleaseFile, AcquisitionReleaseJob, AcquisitionReleaseState,
                NewAcquisitionReleaseCoverage, ReleaseConfidence, ReleaseCoverageKind,
                ReleaseCoverageState, ReleaseJobState, ReleaseJobStateUpdate, ReleaseResolverKind,
            },
            review_candidates::{
                SYNTHETIC_SOURCE_CANDIDATE_FILE_ID, ensure_manual_review_release_files,
            },
            store::{
                ReleaseListFilter, get_file_hash_by_path, get_release,
                list_anime_identity_mismatches_by_release, list_anime_match_attempts_by_release,
                list_release_coverage, list_release_files, list_release_jobs, list_releases,
                update_release_coverage_review_state, update_release_file_selection,
                update_release_job_state, update_release_review_state, upsert_release_coverage,
            },
        },
        route_attempts::{RouteAttemptRecord, RouteAttemptStatus, route_attempt_ledger},
        subscriptions::{
            AcquisitionSubscription, AcquisitionTarget, AcquisitionTargetState,
            AcquisitionTargetStateUpdate, clear_target_next_search_after, get_subscription,
            get_target, list_subscription_targets, update_target_state,
        },
    },
    db::models::MediaType,
    download_broker::{
        DEBRID_DEFAULT_LOGICAL_ID, DEFAULT_ROUTE_OWNER_ID, TORRENT_DEFAULT_LOGICAL_ID,
    },
    extensions::store::ExtensionStore,
    http::{
        auth::CurrentUser,
        error::{ApiError, ApiResult},
        handlers::{
            acquisition_sources::AcquisitionCandidate,
            download_broker::{
                DownloadBrokerSubmitRequest, DownloadBrokerSubmitResponse, submit_to_broker,
            },
        },
    },
    state::AppState,
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListAcquisitionReleasesQuery {
    #[serde(default)]
    subscription_id: Option<Uuid>,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    limit: Option<i64>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ApproveAcquisitionReleaseRequest {
    #[serde(default)]
    route_logical_id: Option<String>,
    #[serde(default)]
    selected_release_file_ids: Vec<Uuid>,
    #[serde(default)]
    skipped_release_file_ids: Vec<Uuid>,
    #[serde(default)]
    selected_file_ids: Vec<String>,
    #[serde(default)]
    skipped_file_ids: Vec<String>,
    #[serde(default)]
    mappings: Vec<ManualCoverageMappingRequest>,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    note: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct InspectAcquisitionReleaseRequest {
    #[serde(default)]
    route_logical_id: Option<String>,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManualCoverageMappingRequest {
    target_id: Uuid,
    #[serde(default)]
    release_file_id: Option<Uuid>,
    #[serde(default)]
    coverage_kind: Option<ReleaseCoverageKind>,
    #[serde(default)]
    confidence: Option<ReleaseConfidence>,
    #[serde(default)]
    score: Option<f64>,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RejectAcquisitionReleaseRequest {
    reason: String,
    #[serde(default)]
    note: Option<String>,
    #[serde(default)]
    target_policy: RejectTargetPolicy,
}

#[derive(Debug, Clone, Copy, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RejectTargetPolicy {
    Blocked,
    #[default]
    Pending,
    Unchanged,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetryAcquisitionReleaseRequest {
    #[serde(default)]
    mode: RetryMode,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    next_search_after: Option<DateTime<Utc>>,
    #[serde(default)]
    clear_suppression: bool,
}

#[derive(Debug, Clone, Copy, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RetryMode {
    #[default]
    SameRelease,
    SourceDiscovery,
    Import,
    Verification,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AcquisitionReleaseListResponse {
    releases: Vec<AcquisitionReleaseSummary>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AcquisitionReleaseSummary {
    release: AcquisitionRelease,
    counts: ReleaseReviewCounts,
    review_status: String,
    evidence: ReleaseReviewEvidence,
    import_summary: ReleaseImportSummary,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AcquisitionReleaseDetailResponse {
    release: AcquisitionRelease,
    subscription: Option<AcquisitionSubscription>,
    files: Vec<ReleaseFileReview>,
    coverage: Vec<ReleaseCoverageReview>,
    jobs: Vec<AcquisitionReleaseJob>,
    imports: Vec<AcquisitionImportRunReview>,
    anime_verification: AnimeImportVerificationReview,
    counts: ReleaseReviewCounts,
    review_status: String,
    evidence: ReleaseReviewEvidence,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AcquisitionSubscriptionCoverageResponse {
    subscription: AcquisitionSubscription,
    targets: Vec<TargetCoverageReview>,
    releases: Vec<AcquisitionReleaseSummary>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TargetCoverageReview {
    target: AcquisitionTarget,
    coverage: Vec<ReleaseCoverageReview>,
    import_links: Vec<AcquisitionImportFileLink>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReleaseFileReview {
    release_file_id: Uuid,
    file_index: Option<i64>,
    file_id: Option<String>,
    provider_file_id: Option<String>,
    path: String,
    basename: String,
    size_bytes: Option<i64>,
    selectable: bool,
    selected: Option<bool>,
    local_path: Option<String>,
    provider_metadata: Option<JsonValue>,
    raw: Option<JsonValue>,
    parsed: ReleaseFileParsedMetadata,
    review_reasons: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReleaseFileParsedMetadata {
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

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ReleaseCoverageReview {
    coverage: AcquisitionReleaseCoverage,
    target: Option<AcquisitionTarget>,
    release_file_id: Option<Uuid>,
    evidence: JsonValue,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReleaseReviewCounts {
    file_count: usize,
    selected_file_count: usize,
    skipped_file_count: usize,
    selectable_file_count: usize,
    coverage_count: usize,
    selected_coverage_count: usize,
    submitted_coverage_count: usize,
    review_required_coverage_count: usize,
    rejected_coverage_count: usize,
    active_job_count: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReleaseReviewEvidence {
    source_candidate: Option<JsonValue>,
    resolver_evidence: Option<JsonValue>,
    route_policy: Option<JsonValue>,
    target_scope: Option<JsonValue>,
    source_provider_id: Option<Uuid>,
    route_provider_id: Option<Uuid>,
    route_logical_id: Option<String>,
    selected_candidate: Option<JsonValue>,
    coverage_plan: Option<JsonValue>,
    scheduler_dispatch: Option<JsonValue>,
    submission_result: Option<JsonValue>,
    priority_policy: Option<JsonValue>,
    manual_review: Option<JsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    diagnostics: Option<JsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    movie_evidence: Option<JsonValue>,
    retry_policy: Option<JsonValue>,
    debrid_runtime: Option<JsonValue>,
    torrent_runtime: Option<JsonValue>,
    import_state: Option<JsonValue>,
    anime_verification: Option<JsonValue>,
}

#[derive(Debug, Serialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct ReleaseImportSummary {
    run_count: usize,
    pending_run_count: usize,
    blocked_run_count: usize,
    mismatched_run_count: usize,
    imported_run_count: usize,
    file_link_count: usize,
    pending_file_link_count: usize,
    blocked_file_link_count: usize,
    imported_file_link_count: usize,
    latest_state: Option<String>,
    latest_reason: Option<String>,
    latest_mismatch_class: Option<String>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AcquisitionImportRunReview {
    run: AcquisitionImportRun,
    file_links: Vec<AcquisitionImportFileReview>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AcquisitionImportFileReview {
    link: AcquisitionImportFileLink,
    file_hash: Option<AcquisitionFileHash>,
}

#[derive(Debug, Serialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct AnimeImportVerificationReview {
    file_hashes: Vec<AcquisitionFileHash>,
    match_attempts: Vec<AcquisitionAnimeMatchAttempt>,
    mismatches: Vec<AcquisitionAnimeIdentityMismatch>,
}

pub async fn list_acquisition_releases(
    user: CurrentUser,
    State(state): State<AppState>,
    Query(query): Query<ListAcquisitionReleasesQuery>,
) -> ApiResult<Json<AcquisitionReleaseListResponse>> {
    let requested_state = parse_optional_release_state(query.state.as_deref())?;
    if requested_state == Some(AcquisitionReleaseState::ReviewRequired) {
        prune_review_candidates_for_covered_targets(
            &state.db_pool,
            user.user_id,
            query.subscription_id,
        )
        .await?;
    }
    let releases = list_releases(
        &state.db_pool,
        ReleaseListFilter {
            subscription_id: query.subscription_id,
            state: requested_state,
            limit: query.limit,
        },
    )
    .await
    .map_err(ApiError::from)?;
    let mut summaries = Vec::with_capacity(releases.len());
    for release in releases {
        summaries.push(build_release_summary(&state.db_pool, release).await?);
    }
    Ok(Json(AcquisitionReleaseListResponse {
        releases: summaries,
    }))
}

pub async fn get_acquisition_release(
    user: CurrentUser,
    State(state): State<AppState>,
    Path(release_id): Path<Uuid>,
) -> ApiResult<Json<AcquisitionReleaseDetailResponse>> {
    prune_review_candidates_for_covered_targets(&state.db_pool, user.user_id, None).await?;
    let detail = load_release_detail(&state.db_pool, release_id).await?;
    Ok(Json(detail))
}

pub async fn acquisition_subscription_coverage(
    _user: CurrentUser,
    State(state): State<AppState>,
    Path(subscription_id): Path<Uuid>,
) -> ApiResult<Json<AcquisitionSubscriptionCoverageResponse>> {
    let subscription = get_subscription(&state.db_pool, subscription_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("acquisition subscription not found"))?;
    let targets = list_subscription_targets(&state.db_pool, subscription_id)
        .await
        .map_err(ApiError::from)?;
    let releases = list_releases(
        &state.db_pool,
        ReleaseListFilter {
            subscription_id: Some(subscription_id),
            state: None,
            limit: Some(500),
        },
    )
    .await
    .map_err(ApiError::from)?;
    let mut target_rows = targets
        .into_iter()
        .map(|target| {
            (
                target.target_id,
                TargetCoverageReview {
                    target,
                    coverage: Vec::new(),
                    import_links: Vec::new(),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut summaries = Vec::with_capacity(releases.len());
    for release in releases {
        let coverage = list_release_coverage(&state.db_pool, release.release_id)
            .await
            .map_err(ApiError::from)?;
        let detail_targets =
            targets_for_release(&state.db_pool, release.subscription_id, &coverage)
                .await
                .map_err(ApiError::from)?;
        for row in coverage {
            if let Some(target) = target_rows.get_mut(&row.target_id) {
                let target_detail = detail_targets.get(&row.target_id).cloned();
                target.coverage.push(ReleaseCoverageReview {
                    release_file_id: row.release_file_id,
                    evidence: coverage_row_evidence(&release, &row),
                    coverage: row,
                    target: target_detail,
                });
            }
        }
        for link in list_import_file_links_by_release(&state.db_pool, release.release_id)
            .await
            .map_err(ApiError::from)?
        {
            if let Some(target_id) = link.target_id
                && let Some(target) = target_rows.get_mut(&target_id)
            {
                target.import_links.push(link);
            }
        }
        summaries.push(build_release_summary(&state.db_pool, release).await?);
    }
    Ok(Json(AcquisitionSubscriptionCoverageResponse {
        subscription,
        targets: target_rows.into_values().collect(),
        releases: summaries,
    }))
}

pub async fn approve_acquisition_release(
    user: CurrentUser,
    State(state): State<AppState>,
    Path(release_id): Path<Uuid>,
    Json(request): Json<ApproveAcquisitionReleaseRequest>,
) -> ApiResult<Json<AcquisitionReleaseDetailResponse>> {
    approve_release_for_review(
        Some(&state),
        &state.db_pool,
        user.user_id,
        release_id,
        request,
    )
    .await?;
    let detail = load_release_detail(&state.db_pool, release_id).await?;
    Ok(Json(detail))
}

pub async fn inspect_acquisition_release(
    user: CurrentUser,
    State(state): State<AppState>,
    Path(release_id): Path<Uuid>,
    Json(request): Json<InspectAcquisitionReleaseRequest>,
) -> ApiResult<Json<AcquisitionReleaseDetailResponse>> {
    inspect_release_for_review(&state, user.user_id, release_id, request).await?;
    let detail = load_release_detail(&state.db_pool, release_id).await?;
    Ok(Json(detail))
}

pub async fn reject_acquisition_release(
    user: CurrentUser,
    State(state): State<AppState>,
    Path(release_id): Path<Uuid>,
    Json(request): Json<RejectAcquisitionReleaseRequest>,
) -> ApiResult<Json<AcquisitionReleaseDetailResponse>> {
    reject_release_for_review(&state.db_pool, user.user_id, release_id, request).await?;
    let detail = load_release_detail(&state.db_pool, release_id).await?;
    Ok(Json(detail))
}

pub async fn retry_acquisition_release(
    user: CurrentUser,
    State(state): State<AppState>,
    Path(release_id): Path<Uuid>,
    Json(request): Json<RetryAcquisitionReleaseRequest>,
) -> ApiResult<Json<AcquisitionReleaseDetailResponse>> {
    retry_release_for_review(&state.db_pool, user.user_id, release_id, request).await?;
    let detail = load_release_detail(&state.db_pool, release_id).await?;
    Ok(Json(detail))
}

async fn load_release_detail(
    pool: &AnyPool,
    release_id: Uuid,
) -> ApiResult<AcquisitionReleaseDetailResponse> {
    let release = get_release(pool, release_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("acquisition release not found"))?;
    let files = ensure_manual_review_release_files(pool, &release)
        .await
        .map_err(ApiError::from)?;
    let coverage = list_release_coverage(pool, release_id)
        .await
        .map_err(ApiError::from)?;
    let jobs = list_release_jobs(pool, release_id)
        .await
        .map_err(ApiError::from)?;
    let import_runs = list_import_runs_by_release(pool, release_id)
        .await
        .map_err(ApiError::from)?;
    let import_links = list_import_file_links_by_release(pool, release_id)
        .await
        .map_err(ApiError::from)?;
    let anime_verification =
        load_anime_import_verification(pool, release_id, &import_links).await?;
    let subscription = match release.subscription_id {
        Some(subscription_id) => get_subscription(pool, subscription_id)
            .await
            .map_err(ApiError::from)?,
        None => None,
    };
    let targets = targets_for_release(pool, release.subscription_id, &coverage)
        .await
        .map_err(ApiError::from)?;
    let imports = build_import_run_reviews(pool, &import_runs).await?;
    let response = build_release_detail(
        release,
        subscription,
        files,
        coverage,
        jobs,
        imports,
        anime_verification,
        targets,
    );
    Ok(response)
}

async fn build_release_summary(
    pool: &AnyPool,
    release: AcquisitionRelease,
) -> ApiResult<AcquisitionReleaseSummary> {
    let files = list_release_files(pool, release.release_id)
        .await
        .map_err(ApiError::from)?;
    let coverage = list_release_coverage(pool, release.release_id)
        .await
        .map_err(ApiError::from)?;
    let jobs = list_release_jobs(pool, release.release_id)
        .await
        .map_err(ApiError::from)?;
    let import_runs = list_import_runs_by_release(pool, release.release_id)
        .await
        .map_err(ApiError::from)?;
    let import_links = list_import_file_links_by_release(pool, release.release_id)
        .await
        .map_err(ApiError::from)?;
    Ok(AcquisitionReleaseSummary {
        review_status: release_review_status(&release),
        evidence: release_evidence(&release, &import_runs, &import_links),
        counts: review_counts(&files, &coverage, &jobs),
        import_summary: import_summary(&import_runs, &import_links),
        release,
    })
}

fn build_release_detail(
    release: AcquisitionRelease,
    subscription: Option<AcquisitionSubscription>,
    files: Vec<AcquisitionReleaseFile>,
    coverage: Vec<AcquisitionReleaseCoverage>,
    jobs: Vec<AcquisitionReleaseJob>,
    imports: Vec<AcquisitionImportRunReview>,
    anime_verification: AnimeImportVerificationReview,
    targets: BTreeMap<Uuid, AcquisitionTarget>,
) -> AcquisitionReleaseDetailResponse {
    let counts = review_counts(&files, &coverage, &jobs);
    let review_status = release_review_status(&release);
    let flat_links = imports
        .iter()
        .flat_map(|run| run.file_links.iter().map(|file| file.link.clone()))
        .collect::<Vec<_>>();
    let import_runs = imports
        .iter()
        .map(|run| run.run.clone())
        .collect::<Vec<_>>();
    let evidence = release_evidence(&release, &import_runs, &flat_links);
    let file_rows = files.iter().map(release_file_review).collect();
    let coverage_rows = coverage
        .into_iter()
        .map(|row| ReleaseCoverageReview {
            release_file_id: row.release_file_id,
            evidence: coverage_row_evidence(&release, &row),
            target: targets.get(&row.target_id).cloned(),
            coverage: row,
        })
        .collect();
    AcquisitionReleaseDetailResponse {
        release,
        subscription,
        files: file_rows,
        coverage: coverage_rows,
        jobs,
        imports,
        anime_verification,
        counts,
        review_status,
        evidence,
    }
}

async fn targets_for_release(
    pool: &AnyPool,
    subscription_id: Option<Uuid>,
    coverage: &[AcquisitionReleaseCoverage],
) -> Result<BTreeMap<Uuid, AcquisitionTarget>> {
    let mut targets = BTreeMap::new();
    if let Some(subscription_id) = subscription_id {
        for target in list_subscription_targets(pool, subscription_id).await? {
            targets.insert(target.target_id, target);
        }
    }
    for row in coverage {
        if !targets.contains_key(&row.target_id) {
            if let Some(target) = get_target(pool, row.target_id).await? {
                targets.insert(row.target_id, target);
            }
        }
    }
    Ok(targets)
}

async fn approve_release_for_review(
    broker_state: Option<&AppState>,
    pool: &AnyPool,
    user_id: Uuid,
    release_id: Uuid,
    request: ApproveAcquisitionReleaseRequest,
) -> ApiResult<()> {
    let release = get_release(pool, release_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("acquisition release not found"))?;
    if release.state == AcquisitionReleaseState::Cancelled {
        return Err(ApiError::conflict(
            "cancelled acquisition release cannot be approved",
        ));
    }
    let files = list_release_files(pool, release_id)
        .await
        .map_err(ApiError::from)?;
    let coverage = list_release_coverage(pool, release_id)
        .await
        .map_err(ApiError::from)?;
    let file_selection = resolve_manual_file_selection(&files, &coverage, &request)?;
    validate_manual_review_target_mappings(
        pool,
        &release,
        &files,
        &coverage,
        &file_selection,
        &request.mappings,
    )
    .await?;
    let approved_target_mappings =
        approved_manual_target_mappings(&file_selection, &request.mappings);
    let active_release = if let Some(state) = broker_state
        && release.download_id.is_none()
        && !matches!(
            release.state,
            AcquisitionReleaseState::Submitted
                | AcquisitionReleaseState::Downloading
                | AcquisitionReleaseState::Materializing
                | AcquisitionReleaseState::Completed
        ) {
        submit_review_release_to_broker(
            state,
            user_id,
            release_id,
            request.route_logical_id.as_deref(),
            "Approved by acquisition release review.",
            true,
        )
        .await?
    } else {
        release.clone()
    };
    for file in &files {
        let selected = if file_selection.explicit {
            Some(
                file_selection
                    .selected_release_file_ids
                    .contains(&file.release_file_id),
            )
        } else if file_selection
            .selected_release_file_ids
            .contains(&file.release_file_id)
        {
            Some(true)
        } else if file_selection
            .skipped_release_file_ids
            .contains(&file.release_file_id)
        {
            Some(false)
        } else {
            file.selected
        };
        if selected != file.selected {
            update_release_file_selection(pool, file.release_file_id, selected)
                .await
                .map_err(ApiError::from)?;
        }
    }

    let reviewer = reviewer_id(user_id);
    let selected_coverage_state = selected_coverage_state(&active_release);
    for row in &coverage {
        let state = manual_approval_coverage_state(
            row,
            &file_selection,
            &approved_target_mappings,
            selected_coverage_state,
        );
        let reason = if state == ReleaseCoverageState::Rejected {
            Some(
                if approved_target_mappings.is_empty() {
                    "Skipped by manual acquisition review."
                } else {
                    "Not selected by manual acquisition review mapping."
                }
                .to_string(),
            )
        } else {
            request
                .reason
                .clone()
                .or_else(|| Some("Approved by acquisition release review.".to_string()))
        };
        update_release_coverage_review_state(
            pool,
            row.coverage_id,
            state,
            reason,
            Some(reviewer.clone()),
        )
        .await
        .map_err(ApiError::from)?;
    }
    for mapping in &request.mappings {
        let Some(release_file_id) = mapping.release_file_id.map(|release_file_id| {
            file_selection
                .release_file_aliases
                .get(&release_file_id)
                .copied()
                .unwrap_or(release_file_id)
        }) else {
            continue;
        };
        upsert_release_coverage(
            pool,
            NewAcquisitionReleaseCoverage {
                coverage_id: None,
                release_id,
                release_file_id: Some(release_file_id),
                target_id: mapping.target_id,
                coverage_kind: mapping
                    .coverage_kind
                    .unwrap_or(ReleaseCoverageKind::ManualOverride),
                confidence: mapping.confidence.unwrap_or(ReleaseConfidence::High),
                score: mapping.score,
                reason: mapping
                    .reason
                    .clone()
                    .or_else(|| request.reason.clone())
                    .or_else(|| Some("Manual acquisition review coverage mapping.".to_string())),
                state: selected_coverage_state,
                verified_by: Some(reviewer.clone()),
            },
        )
        .await
        .map_err(ApiError::from)?;
    }
    reconcile_manual_review_file_mappings(pool, release_id)
        .await
        .map_err(ApiError::from)?;

    let policy = approval_policy_json(
        &release,
        &files,
        &file_selection,
        user_id,
        request.reason.as_deref(),
        request.note.as_deref(),
    );
    let merged_plan = merge_review_policy(active_release.coverage_plan.as_ref(), policy);
    update_release_review_state(
        pool,
        release_id,
        AcquisitionReleaseState::Ready,
        Some("Approved by acquisition release review.".to_string()),
        Some(merged_plan),
    )
    .await
    .map_err(ApiError::from)?;
    resume_debrid_job_after_manual_approval(pool, release_id)
        .await
        .map_err(ApiError::from)?;
    let jobs = list_release_jobs(pool, release_id)
        .await
        .map_err(ApiError::from)?;
    for job in jobs {
        update_release_job_state(
            pool,
            job.release_job_id,
            ReleaseJobStateUpdate {
                state: ReleaseJobState::Ready,
                state_reason: Some("Approved by acquisition release review.".to_string()),
                active: Some(true),
                download_id: job.download_id,
                remote_release_id: job.remote_release_id,
                completed_at: None,
            },
        )
        .await
        .map_err(ApiError::from)?;
    }

    let active_release = get_release(pool, release_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("acquisition release not found"))?;

    mark_approved_release_targets_submitted(
        pool,
        &active_release,
        "Approved by acquisition release review.",
    )
    .await?;
    let prune_summary =
        prune_competing_review_candidates_after_approval(pool, user_id, &active_release).await?;
    record_acquisition_audit_event(
        pool,
        NewAcquisitionAuditEvent {
            event_type: EVENT_MANUAL_APPROVAL.to_string(),
            release_id: Some(release_id),
            subscription_id: active_release.subscription_id,
            actor_user_id: Some(user_id),
            state: Some(AcquisitionReleaseState::Ready.as_str().to_string()),
            reason: request
                .reason
                .clone()
                .or_else(|| Some("Approved by acquisition release review.".to_string())),
            evidence: Some(json!({
                "releaseFingerprint": active_release.fingerprint,
                "routeLogicalId": active_release.selected_route_logical_id,
                "routeProviderId": active_release.selected_provider_id,
                "downloadId": active_release.download_id,
                "selectedReleaseFileIds": file_selection
                    .selected_release_file_ids
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>(),
                "skippedReleaseFileIds": file_selection
                    .skipped_release_file_ids
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>(),
                "mappingCount": request.mappings.len(),
                "note": request.note,
                "prunedCompetingReviewReleases": prune_summary.cancelled_releases,
                "prunedCompetingCoverageRows": prune_summary.rejected_coverage_rows,
            })),
            ..NewAcquisitionAuditEvent::default()
        },
    )
    .await
    .map_err(ApiError::from)?;
    Ok(())
}

async fn inspect_release_for_review(
    state: &AppState,
    user_id: Uuid,
    release_id: Uuid,
    request: InspectAcquisitionReleaseRequest,
) -> ApiResult<()> {
    let release = get_release(&state.db_pool, release_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("acquisition release not found"))?;
    if release.state == AcquisitionReleaseState::Cancelled {
        return Err(ApiError::conflict(
            "cancelled acquisition release cannot be inspected",
        ));
    }
    if release.download_id.is_some() {
        mark_existing_release_inspection_requested(
            &state.db_pool,
            user_id,
            &release,
            request
                .reason
                .as_deref()
                .unwrap_or("Inspect release files before approval."),
        )
        .await?;
        return Ok(());
    }

    submit_review_release_to_broker(
        state,
        user_id,
        release_id,
        request.route_logical_id.as_deref(),
        request
            .reason
            .as_deref()
            .unwrap_or("Inspect release files before approval."),
        false,
    )
    .await?;
    Ok(())
}

async fn mark_existing_release_inspection_requested(
    pool: &AnyPool,
    user_id: Uuid,
    release: &AcquisitionRelease,
    reason: &str,
) -> ApiResult<()> {
    let merged_plan = merge_review_policy(
        release.coverage_plan.as_ref(),
        json!({
            "manualReview": {
                "status": "inspection_requested",
                "reviewerUserId": user_id,
                "reviewedAt": Utc::now(),
                "reason": reason,
                "previousState": release.state.as_str()
            }
        }),
    );
    update_release_review_state(
        pool,
        release.release_id,
        release.state,
        Some("Inspecting release files before approval.".to_string()),
        Some(merged_plan),
    )
    .await
    .map_err(ApiError::from)?;
    record_acquisition_audit_event(
        pool,
        NewAcquisitionAuditEvent {
            event_type: EVENT_INSPECT_REQUESTED.to_string(),
            release_id: Some(release.release_id),
            subscription_id: release.subscription_id,
            actor_user_id: Some(user_id),
            state: Some(release.state.as_str().to_string()),
            reason: Some(reason.to_string()),
            evidence: Some(json!({
                "releaseFingerprint": release.fingerprint,
                "downloadId": release.download_id,
                "remoteReleaseId": release.remote_release_id,
                "routeLogicalId": release.selected_route_logical_id,
                "alreadySubmitted": true,
            })),
            ..NewAcquisitionAuditEvent::default()
        },
    )
    .await
    .map_err(ApiError::from)?;
    Ok(())
}

async fn submit_review_release_to_broker(
    state: &AppState,
    user_id: Uuid,
    release_id: Uuid,
    requested_route_logical_id: Option<&str>,
    reason: &str,
    approved: bool,
) -> ApiResult<AcquisitionRelease> {
    let release = get_release(&state.db_pool, release_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("acquisition release not found"))?;
    let candidate = release_candidate(&release)?;
    let route_logical_id = select_review_route_logical_id(&release, requested_route_logical_id)?;
    let store = ExtensionStore::new(&state.db_pool);
    let broker_response = submit_to_broker(
        state,
        &store,
        &route_logical_id,
        Some(DEFAULT_ROUTE_OWNER_ID),
        DownloadBrokerSubmitRequest {
            source: release.source.clone(),
            category: None,
            paused: Some(false),
            name: Some(release.release_title.clone()),
            priority: None,
            add_to_top: None,
            subscription_id: release.subscription_id,
            source_provider_id: release.source_provider_id,
            source_extension_id: Some(release.source_extension_id.clone()),
            media_type: Some(release.media_type),
            media_title: Some(release.title.clone()),
            selected_candidate: Some(candidate),
            release_fingerprint: Some(release.fingerprint.clone()),
        },
    )
    .await?;
    let submitted = get_release(&state.db_pool, release_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("acquisition release not found"))?;
    let merged_plan = merge_review_policy(
        submitted.coverage_plan.as_ref(),
        review_submission_policy_json(&release, &broker_response, user_id, reason, approved),
    );
    let state_reason = if approved {
        "Approved by acquisition release review and submitted to acquisition route."
    } else {
        "Release staged for file inspection before approval."
    };
    let state_after_submit = submitted.state;
    update_release_review_state(
        &state.db_pool,
        release_id,
        state_after_submit,
        Some(state_reason.to_string()),
        Some(merged_plan),
    )
    .await
    .map_err(ApiError::from)?;
    let refreshed = get_release(&state.db_pool, release_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("acquisition release not found"))?;
    if !approved {
        mark_review_release_targets_inspecting(&state.db_pool, &refreshed, reason).await?;
        record_acquisition_audit_event(
            &state.db_pool,
            NewAcquisitionAuditEvent {
                event_type: EVENT_INSPECT_REQUESTED.to_string(),
                release_id: Some(release_id),
                subscription_id: refreshed.subscription_id,
                actor_user_id: Some(user_id),
                state: Some(refreshed.state.as_str().to_string()),
                reason: Some(reason.to_string()),
                evidence: Some(json!({
                    "releaseFingerprint": refreshed.fingerprint,
                    "routeLogicalId": broker_response.logical_id,
                    "routeProviderId": broker_response.provider_id,
                    "downloadId": broker_response.download_id,
                    "accepted": broker_response.accepted,
                    "alreadySubmitted": false,
                })),
                ..NewAcquisitionAuditEvent::default()
            },
        )
        .await
        .map_err(ApiError::from)?;
    }
    Ok(refreshed)
}

fn release_candidate(release: &AcquisitionRelease) -> ApiResult<AcquisitionCandidate> {
    let candidate_value = release
        .selected_candidate
        .clone()
        .ok_or_else(|| ApiError::bad_request("release has no source candidate to submit"))?;
    serde_json::from_value(candidate_value)
        .map_err(|err| ApiError::bad_request(format!("release source candidate is invalid: {err}")))
}

fn select_review_route_logical_id(
    release: &AcquisitionRelease,
    requested_route_logical_id: Option<&str>,
) -> ApiResult<String> {
    if let Some(route) = requested_route_logical_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Ok(route.to_string());
    }
    if let Some(route) = release
        .selected_route_logical_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Ok(route.to_string());
    }
    let allowed = release
        .coverage_plan
        .as_ref()
        .and_then(|plan| plan.get("routePolicy"))
        .and_then(|policy| policy.get("allowedRoutes"))
        .and_then(JsonValue::as_array)
        .into_iter()
        .flatten()
        .filter_map(JsonValue::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    if let Some(route) = allowed
        .iter()
        .find(|route| route.as_str() == DEBRID_DEFAULT_LOGICAL_ID)
        .or_else(|| {
            allowed
                .iter()
                .find(|route| route.as_str() == TORRENT_DEFAULT_LOGICAL_ID)
        })
        .or_else(|| allowed.first())
    {
        return Ok(route.clone());
    }
    Err(ApiError::bad_request(
        "release has no review route; choose a route before submitting",
    ))
}

fn review_submission_policy_json(
    release: &AcquisitionRelease,
    broker_response: &DownloadBrokerSubmitResponse,
    user_id: Uuid,
    reason: &str,
    approved: bool,
) -> JsonValue {
    let status = if approved {
        "approved"
    } else {
        "inspection_requested"
    };
    let route_attempt = RouteAttemptRecord::new(
        &broker_response.logical_id,
        Some(broker_response.provider_id),
        broker_response.provider_implementation.as_deref(),
        broker_response.download_id.clone(),
        RouteAttemptStatus::Submitted,
        None,
        Some(reason.to_string()),
    );
    let mut manual_review = release
        .coverage_plan
        .as_ref()
        .and_then(|plan| plan.get("manualReview"))
        .cloned()
        .unwrap_or_else(|| json!({}));
    merge_json_object(
        &mut manual_review,
        json!({
            "status": status,
            "userApproved": approved,
            "reviewerUserId": user_id,
            "reviewedAt": Utc::now(),
            "reason": reason,
            "previousState": release.state.as_str()
        }),
    );
    let mut patch = json!({
        "manualReview": {
        },
        "submissionResult": {
            "accepted": broker_response.accepted,
            "routeLogicalId": broker_response.logical_id,
            "routeProviderId": broker_response.provider_id,
            "routeProviderImplementation": broker_response.provider_implementation.clone(),
            "downloadId": broker_response.download_id.clone()
        },
        "routeAttemptLedger": route_attempt_ledger(&release.fingerprint, &[route_attempt])
    });
    if let JsonValue::Object(map) = &mut patch {
        map.insert("manualReview".to_string(), manual_review);
        if approved {
            for key in ["priorityPolicy", "animeVerification"] {
                if let Some(value) = release
                    .coverage_plan
                    .as_ref()
                    .and_then(|plan| plan.get(key))
                    .cloned()
                {
                    map.insert(key.to_string(), value);
                }
            }
        }
    }
    patch
}

async fn mark_review_release_targets_inspecting(
    pool: &AnyPool,
    release: &AcquisitionRelease,
    reason: &str,
) -> ApiResult<()> {
    let coverage = list_release_coverage(pool, release.release_id)
        .await
        .map_err(ApiError::from)?;
    let mut target_ids = BTreeSet::new();
    for row in coverage {
        if !target_ids.insert(row.target_id) {
            continue;
        }
        update_target_state(
            pool,
            row.target_id,
            AcquisitionTargetStateUpdate {
                state: AcquisitionTargetState::Pending,
                state_reason: Some(reason.to_string()),
                selected_provider_id: release.selected_provider_id.or(release.source_provider_id),
                selected_route_logical_id: release.selected_route_logical_id.clone(),
                selected_candidate: release.selected_candidate.clone(),
                download_id: release.download_id.clone(),
                import_event_id: None,
                next_search_after: None,
                increment_search_attempts: false,
            },
        )
        .await
        .map_err(ApiError::from)?;
    }
    Ok(())
}

async fn mark_approved_release_targets_submitted(
    pool: &AnyPool,
    release: &AcquisitionRelease,
    reason: &str,
) -> ApiResult<()> {
    let coverage = list_release_coverage(pool, release.release_id)
        .await
        .map_err(ApiError::from)?;
    let mut target_ids = BTreeSet::new();
    for row in coverage {
        if !matches!(
            row.state,
            ReleaseCoverageState::Selected | ReleaseCoverageState::Submitted
        ) {
            continue;
        }
        if !target_ids.insert(row.target_id) {
            continue;
        }
        update_target_state(
            pool,
            row.target_id,
            AcquisitionTargetStateUpdate {
                state: AcquisitionTargetState::Submitted,
                state_reason: Some(reason.to_string()),
                selected_provider_id: release.selected_provider_id.or(release.source_provider_id),
                selected_route_logical_id: release.selected_route_logical_id.clone(),
                selected_candidate: release.selected_candidate.clone(),
                download_id: release.download_id.clone(),
                import_event_id: None,
                next_search_after: None,
                increment_search_attempts: false,
            },
        )
        .await
        .map_err(ApiError::from)?;
    }
    Ok(())
}

#[derive(Debug, Default)]
struct ManualReviewPruneSummary {
    rejected_coverage_rows: usize,
    cancelled_releases: usize,
}

async fn prune_competing_review_candidates_after_approval(
    pool: &AnyPool,
    user_id: Uuid,
    approved_release: &AcquisitionRelease,
) -> ApiResult<ManualReviewPruneSummary> {
    let Some(subscription_id) = approved_release.subscription_id else {
        return Ok(ManualReviewPruneSummary::default());
    };
    let approved_coverage = list_release_coverage(pool, approved_release.release_id)
        .await
        .map_err(ApiError::from)?;
    let approved_target_ids = approved_coverage
        .iter()
        .filter(|row| {
            matches!(
                row.state,
                ReleaseCoverageState::Selected
                    | ReleaseCoverageState::Submitted
                    | ReleaseCoverageState::Imported
            )
        })
        .map(|row| row.target_id)
        .collect::<BTreeSet<_>>();
    if approved_target_ids.is_empty() {
        return Ok(ManualReviewPruneSummary::default());
    }

    let reviewer = reviewer_id(user_id);
    let prune_reason = format!(
        "Removed from manual review because target was covered by approved release {}.",
        approved_release.release_id
    );
    let competing_releases = list_releases(
        pool,
        ReleaseListFilter {
            subscription_id: Some(subscription_id),
            state: Some(AcquisitionReleaseState::ReviewRequired),
            limit: Some(500),
        },
    )
    .await
    .map_err(ApiError::from)?;
    let mut summary = ManualReviewPruneSummary::default();

    for competing in competing_releases {
        if competing.release_id == approved_release.release_id {
            continue;
        }
        let coverage = list_release_coverage(pool, competing.release_id)
            .await
            .map_err(ApiError::from)?;
        let mut rejected_this_release = 0usize;
        for row in &coverage {
            if !approved_target_ids.contains(&row.target_id)
                || row.state == ReleaseCoverageState::Rejected
            {
                continue;
            }
            update_release_coverage_review_state(
                pool,
                row.coverage_id,
                ReleaseCoverageState::Rejected,
                Some(prune_reason.clone()),
                Some(reviewer.clone()),
            )
            .await
            .map_err(ApiError::from)?;
            rejected_this_release += 1;
        }
        if rejected_this_release == 0 {
            continue;
        }
        summary.rejected_coverage_rows += rejected_this_release;

        let remaining_coverage = list_release_coverage(pool, competing.release_id)
            .await
            .map_err(ApiError::from)?;
        let has_remaining_candidate_targets = remaining_coverage
            .iter()
            .any(|row| row.state != ReleaseCoverageState::Rejected);
        if has_remaining_candidate_targets {
            continue;
        }

        let jobs = list_release_jobs(pool, competing.release_id)
            .await
            .map_err(ApiError::from)?;
        let job_count = jobs.len();
        for job in jobs {
            update_release_job_state(
                pool,
                job.release_job_id,
                ReleaseJobStateUpdate {
                    state: ReleaseJobState::Cancelled,
                    state_reason: Some(prune_reason.clone()),
                    active: Some(false),
                    download_id: job.download_id,
                    remote_release_id: job.remote_release_id,
                    completed_at: Some(Utc::now()),
                },
            )
            .await
            .map_err(ApiError::from)?;
        }
        let cancelled_import_runs =
            cancel_import_runs_for_release(pool, competing.release_id, &prune_reason)
                .await
                .map_err(ApiError::from)?;
        let merged_plan = merge_review_policy(
            competing.coverage_plan.as_ref(),
            json!({
                "manualReview": {
                    "status": "auto_pruned",
                    "userApproved": false,
                    "reviewerUserId": user_id,
                    "reviewedAt": Utc::now(),
                    "reason": prune_reason.clone(),
                    "approvedReleaseId": approved_release.release_id,
                    "previousState": competing.state.as_str()
                },
                "retrySuppression": {
                    "status": "covered_by_approved_release",
                    "fingerprint": competing.fingerprint.clone(),
                    "sourceExtensionId": competing.source_extension_id.clone(),
                    "suppressAutomaticRediscovery": true,
                    "recordedAt": Utc::now()
                }
            }),
        );
        update_release_review_state(
            pool,
            competing.release_id,
            AcquisitionReleaseState::Cancelled,
            Some(
                "Removed from review because all targets were covered by another approved release."
                    .to_string(),
            ),
            Some(merged_plan),
        )
        .await
        .map_err(ApiError::from)?;
        record_acquisition_audit_event(
            pool,
            NewAcquisitionAuditEvent {
                event_type: EVENT_MANUAL_REJECTION.to_string(),
                release_id: Some(competing.release_id),
                subscription_id: competing.subscription_id,
                actor_user_id: Some(user_id),
                state: Some(AcquisitionReleaseState::Cancelled.as_str().to_string()),
                reason: Some(
                    "Auto-removed from manual review after another release covered every target."
                        .to_string(),
                ),
                evidence: Some(json!({
                    "approvedReleaseId": approved_release.release_id,
                    "releaseFingerprint": competing.fingerprint,
                    "cancelledReleaseJobs": job_count,
                    "cancelledImportRuns": cancelled_import_runs,
                    "downloadId": competing.download_id,
                    "remoteReleaseId": competing.remote_release_id,
                    "cleanupPolicy": "database_safe_state_no_downloader_delete",
                })),
                ..NewAcquisitionAuditEvent::default()
            },
        )
        .await
        .map_err(ApiError::from)?;
        summary.cancelled_releases += 1;
    }

    Ok(summary)
}

async fn prune_review_candidates_for_covered_targets(
    pool: &AnyPool,
    user_id: Uuid,
    subscription_id: Option<Uuid>,
) -> ApiResult<ManualReviewPruneSummary> {
    let review_releases = list_releases(
        pool,
        ReleaseListFilter {
            subscription_id,
            state: Some(AcquisitionReleaseState::ReviewRequired),
            limit: Some(500),
        },
    )
    .await
    .map_err(ApiError::from)?;
    let reviewer = reviewer_id(user_id);
    let prune_reason =
        "Removed from manual review because this target is already covered by another acquisition."
            .to_string();
    let mut summary = ManualReviewPruneSummary::default();

    for release in review_releases {
        let coverage = list_release_coverage(pool, release.release_id)
            .await
            .map_err(ApiError::from)?;
        let mut rejected_this_release = 0usize;
        for row in &coverage {
            if row.state == ReleaseCoverageState::Rejected {
                continue;
            }
            let Some(target) = get_target(pool, row.target_id)
                .await
                .map_err(ApiError::from)?
            else {
                continue;
            };
            if !matches!(
                target.state,
                AcquisitionTargetState::Submitted | AcquisitionTargetState::Imported
            ) {
                continue;
            }
            update_release_coverage_review_state(
                pool,
                row.coverage_id,
                ReleaseCoverageState::Rejected,
                Some(prune_reason.clone()),
                Some(reviewer.clone()),
            )
            .await
            .map_err(ApiError::from)?;
            rejected_this_release += 1;
        }
        if rejected_this_release == 0 {
            continue;
        }
        summary.rejected_coverage_rows += rejected_this_release;
        let remaining_coverage = list_release_coverage(pool, release.release_id)
            .await
            .map_err(ApiError::from)?;
        if remaining_coverage
            .iter()
            .any(|row| row.state != ReleaseCoverageState::Rejected)
        {
            continue;
        }

        let jobs = list_release_jobs(pool, release.release_id)
            .await
            .map_err(ApiError::from)?;
        let job_count = jobs.len();
        for job in jobs {
            update_release_job_state(
                pool,
                job.release_job_id,
                ReleaseJobStateUpdate {
                    state: ReleaseJobState::Cancelled,
                    state_reason: Some(prune_reason.clone()),
                    active: Some(false),
                    download_id: job.download_id,
                    remote_release_id: job.remote_release_id,
                    completed_at: Some(Utc::now()),
                },
            )
            .await
            .map_err(ApiError::from)?;
        }
        let cancelled_import_runs =
            cancel_import_runs_for_release(pool, release.release_id, &prune_reason)
                .await
                .map_err(ApiError::from)?;
        let merged_plan = merge_review_policy(
            release.coverage_plan.as_ref(),
            json!({
                "manualReview": {
                    "status": "auto_pruned",
                    "userApproved": false,
                    "reviewerUserId": user_id,
                    "reviewedAt": Utc::now(),
                    "reason": prune_reason.clone(),
                    "previousState": release.state.as_str()
                },
                "retrySuppression": {
                    "status": "target_already_covered",
                    "fingerprint": release.fingerprint.clone(),
                    "sourceExtensionId": release.source_extension_id.clone(),
                    "suppressAutomaticRediscovery": true,
                    "recordedAt": Utc::now()
                }
            }),
        );
        update_release_review_state(
            pool,
            release.release_id,
            AcquisitionReleaseState::Cancelled,
            Some("Removed from review because all targets are already covered.".to_string()),
            Some(merged_plan),
        )
        .await
        .map_err(ApiError::from)?;
        record_acquisition_audit_event(
            pool,
            NewAcquisitionAuditEvent {
                event_type: EVENT_MANUAL_REJECTION.to_string(),
                release_id: Some(release.release_id),
                subscription_id: release.subscription_id,
                actor_user_id: Some(user_id),
                state: Some(AcquisitionReleaseState::Cancelled.as_str().to_string()),
                reason: Some(
                    "Auto-removed from manual review because all targets are already covered."
                        .to_string(),
                ),
                evidence: Some(json!({
                    "releaseFingerprint": release.fingerprint,
                    "cancelledReleaseJobs": job_count,
                    "cancelledImportRuns": cancelled_import_runs,
                    "downloadId": release.download_id,
                    "remoteReleaseId": release.remote_release_id,
                    "cleanupPolicy": "database_safe_state_no_downloader_delete",
                })),
                ..NewAcquisitionAuditEvent::default()
            },
        )
        .await
        .map_err(ApiError::from)?;
        summary.cancelled_releases += 1;
    }

    Ok(summary)
}

async fn reconcile_manual_review_file_mappings(pool: &AnyPool, release_id: Uuid) -> Result<()> {
    let files = list_release_files(pool, release_id).await?;
    let aliases = synthetic_source_candidate_aliases(&files);
    if !aliases.is_empty() {
        for (synthetic_file_id, provider_file_id) in &aliases {
            update_release_file_selection(pool, *synthetic_file_id, Some(false)).await?;
            update_release_file_selection(pool, *provider_file_id, Some(true)).await?;
        }
        let coverage = list_release_coverage(pool, release_id).await?;
        for row in coverage {
            let Some(release_file_id) = row.release_file_id else {
                continue;
            };
            let Some(provider_file_id) = aliases.get(&release_file_id).copied() else {
                continue;
            };
            upsert_release_coverage(
                pool,
                NewAcquisitionReleaseCoverage {
                    coverage_id: Some(row.coverage_id),
                    release_id: row.release_id,
                    release_file_id: Some(provider_file_id),
                    target_id: row.target_id,
                    coverage_kind: row.coverage_kind,
                    confidence: row.confidence,
                    score: row.score,
                    reason: row.reason.clone(),
                    state: row.state,
                    verified_by: row.verified_by.clone(),
                },
            )
            .await?;
        }
    }
    reject_superseded_placeholder_coverage(pool, release_id).await
}

async fn reject_superseded_placeholder_coverage(pool: &AnyPool, release_id: Uuid) -> Result<()> {
    let coverage = list_release_coverage(pool, release_id).await?;
    let selected_file_targets = coverage
        .iter()
        .filter(|row| row.release_file_id.is_some())
        .filter(|row| {
            matches!(
                row.state,
                ReleaseCoverageState::Selected | ReleaseCoverageState::Submitted
            )
        })
        .map(|row| row.target_id)
        .collect::<BTreeSet<_>>();
    if selected_file_targets.is_empty() {
        return Ok(());
    }
    for row in coverage {
        if row.release_file_id.is_some()
            || !selected_file_targets.contains(&row.target_id)
            || row.state == ReleaseCoverageState::Rejected
        {
            continue;
        }
        update_release_coverage_review_state(
            pool,
            row.coverage_id,
            ReleaseCoverageState::Rejected,
            Some("Superseded by manual file mapping.".to_string()),
            row.verified_by.clone(),
        )
        .await?;
    }
    Ok(())
}

async fn resume_debrid_job_after_manual_approval(pool: &AnyPool, release_id: Uuid) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE debrid_download_jobs
         SET status = 'submitted',
             remote_release_status = CASE
                 WHEN remote_release_status = 'review_required' THEN 'submitted'
                 ELSE remote_release_status
             END,
             selected_file_ids_json = '[]',
             skipped_file_ids_json = '[]',
             selection_error = NULL,
             last_error = NULL,
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = ?
           AND status = 'review_required'",
    )
    .bind(release_id.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

async fn reject_release_for_review(
    pool: &AnyPool,
    user_id: Uuid,
    release_id: Uuid,
    request: RejectAcquisitionReleaseRequest,
) -> ApiResult<()> {
    let release = get_release(pool, release_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("acquisition release not found"))?;
    let coverage = list_release_coverage(pool, release_id)
        .await
        .map_err(ApiError::from)?;
    let reviewer = reviewer_id(user_id);
    for row in &coverage {
        update_release_coverage_review_state(
            pool,
            row.coverage_id,
            ReleaseCoverageState::Rejected,
            Some(request.reason.clone()),
            Some(reviewer.clone()),
        )
        .await
        .map_err(ApiError::from)?;
    }
    let jobs = list_release_jobs(pool, release_id)
        .await
        .map_err(ApiError::from)?;
    let job_count = jobs.len();
    for job in jobs {
        update_release_job_state(
            pool,
            job.release_job_id,
            ReleaseJobStateUpdate {
                state: ReleaseJobState::Cancelled,
                state_reason: Some(request.reason.clone()),
                active: Some(false),
                download_id: job.download_id,
                remote_release_id: job.remote_release_id,
                completed_at: Some(Utc::now()),
            },
        )
        .await
        .map_err(ApiError::from)?;
    }
    let cancelled_import_runs =
        cancel_import_runs_for_release(pool, release_id, "Rejected by acquisition release review.")
            .await
            .map_err(ApiError::from)?;
    if request.target_policy != RejectTargetPolicy::Unchanged {
        let target_state = match request.target_policy {
            RejectTargetPolicy::Blocked => AcquisitionTargetState::Blocked,
            RejectTargetPolicy::Pending => AcquisitionTargetState::Pending,
            RejectTargetPolicy::Unchanged => AcquisitionTargetState::Pending,
        };
        let mut target_ids = BTreeSet::new();
        for row in &coverage {
            if target_ids.insert(row.target_id) {
                update_target_state(
                    pool,
                    row.target_id,
                    AcquisitionTargetStateUpdate {
                        state: target_state,
                        state_reason: Some(request.reason.clone()),
                        selected_provider_id: None,
                        selected_route_logical_id: None,
                        selected_candidate: None,
                        download_id: None,
                        import_event_id: None,
                        next_search_after: None,
                        increment_search_attempts: false,
                    },
                )
                .await
                .map_err(ApiError::from)?;
            }
        }
    }

    let merged_plan = merge_review_policy(
        release.coverage_plan.as_ref(),
        json!({
            "manualReview": {
                "status": "rejected",
                "userApproved": false,
                "reviewerUserId": user_id,
                "reviewedAt": Utc::now(),
                "reason": request.reason.clone(),
                "note": request.note.clone(),
                "fingerprint": release.fingerprint.clone(),
                "targetPolicy": match request.target_policy {
                    RejectTargetPolicy::Blocked => "blocked",
                    RejectTargetPolicy::Pending => "pending",
                    RejectTargetPolicy::Unchanged => "unchanged",
                },
                "previousState": release.state.as_str()
            },
            "retrySuppression": {
                "status": "rejected",
                "fingerprint": release.fingerprint.clone(),
                "sourceExtensionId": release.source_extension_id,
                "suppressAutomaticRediscovery": true,
                "recordedAt": Utc::now()
            }
        }),
    );
    update_release_review_state(
        pool,
        release_id,
        AcquisitionReleaseState::Cancelled,
        Some("Rejected by acquisition release review.".to_string()),
        Some(merged_plan),
    )
    .await
    .map_err(ApiError::from)?;
    record_acquisition_audit_event(
        pool,
        NewAcquisitionAuditEvent {
            event_type: EVENT_MANUAL_REJECTION.to_string(),
            release_id: Some(release_id),
            subscription_id: release.subscription_id,
            actor_user_id: Some(user_id),
            state: Some(AcquisitionReleaseState::Cancelled.as_str().to_string()),
            reason: Some(request.reason.clone()),
            evidence: Some(json!({
                "releaseFingerprint": release.fingerprint,
                "targetPolicy": match request.target_policy {
                    RejectTargetPolicy::Blocked => "blocked",
                    RejectTargetPolicy::Pending => "pending",
                    RejectTargetPolicy::Unchanged => "unchanged",
                },
                "note": request.note,
                "cancelledReleaseJobs": job_count,
                "cancelledImportRuns": cancelled_import_runs,
                "downloadId": release.download_id,
                "remoteReleaseId": release.remote_release_id,
                "cleanupPolicy": "database_safe_state_no_downloader_delete",
            })),
            ..NewAcquisitionAuditEvent::default()
        },
    )
    .await
    .map_err(ApiError::from)?;
    Ok(())
}

async fn retry_release_for_review(
    pool: &AnyPool,
    user_id: Uuid,
    release_id: Uuid,
    request: RetryAcquisitionReleaseRequest,
) -> ApiResult<()> {
    let release = get_release(pool, release_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("acquisition release not found"))?;
    let reason = request
        .reason
        .clone()
        .unwrap_or_else(|| "Retry requested by acquisition release review.".to_string());
    let retry_policy = json!({
        "retryPolicy": {
            "mode": match request.mode {
                RetryMode::SameRelease => "same_release",
                RetryMode::SourceDiscovery => "source_discovery",
                RetryMode::Import => "import",
                RetryMode::Verification => "verification",
            },
            "requestedBy": reviewer_id(user_id),
            "requestedAt": Utc::now(),
            "reason": reason.clone(),
            "nextSearchAfter": request.next_search_after,
            "clearSuppression": request.clear_suppression
        }
    });
    let base_plan = if request.clear_suppression {
        clear_review_suppression(release.coverage_plan.as_ref())
    } else {
        release.coverage_plan.clone()
    };
    let merged_plan = merge_review_policy(base_plan.as_ref(), retry_policy);
    match request.mode {
        RetryMode::SameRelease => {
            let next_release_state = if release_has_approval(&release) {
                AcquisitionReleaseState::Ready
            } else {
                AcquisitionReleaseState::Staging
            };
            let next_job_state = if next_release_state == AcquisitionReleaseState::Ready {
                ReleaseJobState::Ready
            } else {
                ReleaseJobState::Staging
            };
            update_release_review_state(
                pool,
                release_id,
                next_release_state,
                Some(reason.clone()),
                Some(merged_plan),
            )
            .await
            .map_err(ApiError::from)?;
            for job in list_release_jobs(pool, release_id)
                .await
                .map_err(ApiError::from)?
            {
                update_release_job_state(
                    pool,
                    job.release_job_id,
                    ReleaseJobStateUpdate {
                        state: next_job_state,
                        state_reason: Some(reason.clone()),
                        active: Some(true),
                        download_id: job.download_id,
                        remote_release_id: job.remote_release_id,
                        completed_at: None,
                    },
                )
                .await
                .map_err(ApiError::from)?;
            }
        }
        RetryMode::SourceDiscovery => {
            let coverage = list_release_coverage(pool, release_id)
                .await
                .map_err(ApiError::from)?;
            let mut target_ids = BTreeSet::new();
            for row in &coverage {
                if target_ids.insert(row.target_id) {
                    update_target_state(
                        pool,
                        row.target_id,
                        AcquisitionTargetStateUpdate {
                            state: AcquisitionTargetState::Pending,
                            state_reason: Some(reason.clone()),
                            selected_provider_id: None,
                            selected_route_logical_id: None,
                            selected_candidate: None,
                            download_id: None,
                            import_event_id: None,
                            next_search_after: request
                                .next_search_after
                                .or_else(|| Some(Utc::now())),
                            increment_search_attempts: false,
                        },
                    )
                    .await
                    .map_err(ApiError::from)?;
                }
            }
            for job in list_release_jobs(pool, release_id)
                .await
                .map_err(ApiError::from)?
            {
                update_release_job_state(
                    pool,
                    job.release_job_id,
                    ReleaseJobStateUpdate {
                        state: ReleaseJobState::Cancelled,
                        state_reason: Some(reason.clone()),
                        active: Some(false),
                        download_id: job.download_id,
                        remote_release_id: job.remote_release_id,
                        completed_at: Some(Utc::now()),
                    },
                )
                .await
                .map_err(ApiError::from)?;
            }
            update_release_review_state(
                pool,
                release_id,
                AcquisitionReleaseState::Cancelled,
                Some(reason),
                Some(merged_plan),
            )
            .await
            .map_err(ApiError::from)?;
        }
        RetryMode::Import | RetryMode::Verification => {
            let reset_verification = request.mode == RetryMode::Verification;
            reset_import_runs_for_release(pool, release_id, &reason, reset_verification)
                .await
                .map_err(ApiError::from)?;
            update_release_review_state(
                pool,
                release_id,
                AcquisitionReleaseState::Completed,
                Some(reason.clone()),
                Some(merged_plan),
            )
            .await
            .map_err(ApiError::from)?;
            for job in list_release_jobs(pool, release_id)
                .await
                .map_err(ApiError::from)?
            {
                update_release_job_state(
                    pool,
                    job.release_job_id,
                    ReleaseJobStateUpdate {
                        state: ReleaseJobState::Completed,
                        state_reason: Some(reason.clone()),
                        active: Some(false),
                        download_id: job.download_id,
                        remote_release_id: job.remote_release_id,
                        completed_at: job.completed_at.or_else(|| Some(Utc::now())),
                    },
                )
                .await
                .map_err(ApiError::from)?;
            }
            let coverage = list_release_coverage(pool, release_id)
                .await
                .map_err(ApiError::from)?;
            let mut target_ids = BTreeSet::new();
            for row in &coverage {
                if target_ids.insert(row.target_id) {
                    update_target_state(
                        pool,
                        row.target_id,
                        AcquisitionTargetStateUpdate {
                            state: AcquisitionTargetState::Submitted,
                            state_reason: Some(reason.clone()),
                            selected_provider_id: release
                                .selected_provider_id
                                .or(release.source_provider_id),
                            selected_route_logical_id: release.selected_route_logical_id.clone(),
                            selected_candidate: release.selected_candidate.clone(),
                            download_id: release.download_id.clone(),
                            import_event_id: None,
                            next_search_after: None,
                            increment_search_attempts: false,
                        },
                    )
                    .await
                    .map_err(ApiError::from)?;
                    clear_target_next_search_after(pool, row.target_id)
                        .await
                        .map_err(ApiError::from)?;
                }
            }
        }
    }
    Ok(())
}

#[derive(Debug)]
struct ResolvedManualFileSelection {
    explicit: bool,
    selected_release_file_ids: BTreeSet<Uuid>,
    skipped_release_file_ids: BTreeSet<Uuid>,
    selected_file_ids: Vec<String>,
    skipped_file_ids: Vec<String>,
    release_file_aliases: BTreeMap<Uuid, Uuid>,
}

fn resolve_manual_file_selection(
    files: &[AcquisitionReleaseFile],
    coverage: &[AcquisitionReleaseCoverage],
    request: &ApproveAcquisitionReleaseRequest,
) -> ApiResult<ResolvedManualFileSelection> {
    let explicit = !request.selected_release_file_ids.is_empty()
        || !request.skipped_release_file_ids.is_empty()
        || !request.selected_file_ids.is_empty()
        || !request.skipped_file_ids.is_empty();
    let mut selected = request
        .selected_release_file_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let mut skipped = request
        .skipped_release_file_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    selected.extend(resolve_file_key_ids(files, &request.selected_file_ids)?);
    skipped.extend(resolve_file_key_ids(files, &request.skipped_file_ids)?);
    let release_file_aliases = synthetic_source_candidate_aliases(files);
    apply_release_file_aliases(&release_file_aliases, &mut selected, &mut skipped);
    if !selected.is_disjoint(&skipped) {
        return Err(ApiError::bad_request(
            "manual file selection cannot select and skip the same release file",
        ));
    }
    let known_file_ids = files
        .iter()
        .map(|file| file.release_file_id)
        .collect::<BTreeSet<_>>();
    for release_file_id in selected.iter().chain(skipped.iter()) {
        let Some(file) = files
            .iter()
            .find(|file| file.release_file_id == *release_file_id)
        else {
            return Err(ApiError::bad_request(format!(
                "unknown release file id {release_file_id}"
            )));
        };
        if selected.contains(release_file_id) && !file.selectable {
            return Err(ApiError::bad_request(format!(
                "release file {release_file_id} is not selectable"
            )));
        }
    }
    if !explicit {
        selected.extend(
            files
                .iter()
                .filter(|file| file.selected == Some(true))
                .map(|file| file.release_file_id),
        );
        skipped.extend(
            files
                .iter()
                .filter(|file| file.selected == Some(false))
                .map(|file| file.release_file_id),
        );
        if selected.is_empty() {
            selected.extend(
                coverage
                    .iter()
                    .filter_map(|row| row.release_file_id)
                    .filter(|release_file_id| known_file_ids.contains(release_file_id)),
            );
        }
        if selected.is_empty() && files.len() == 1 && files[0].selectable {
            selected.insert(files[0].release_file_id);
        }
    }
    if explicit {
        skipped.extend(
            files
                .iter()
                .filter(|file| file.selectable && !selected.contains(&file.release_file_id))
                .map(|file| file.release_file_id),
        );
    }
    let selected_file_ids = files
        .iter()
        .filter(|file| selected.contains(&file.release_file_id))
        .filter_map(file_policy_key)
        .collect::<Vec<_>>();
    let skipped_file_ids = files
        .iter()
        .filter(|file| skipped.contains(&file.release_file_id))
        .filter_map(file_policy_key)
        .collect::<Vec<_>>();
    Ok(ResolvedManualFileSelection {
        explicit,
        selected_release_file_ids: selected,
        skipped_release_file_ids: skipped,
        selected_file_ids,
        skipped_file_ids,
        release_file_aliases,
    })
}

fn approved_manual_target_mappings(
    selection: &ResolvedManualFileSelection,
    mappings: &[ManualCoverageMappingRequest],
) -> BTreeMap<Uuid, Uuid> {
    mappings
        .iter()
        .filter_map(|mapping| {
            let release_file_id = mapping.release_file_id?;
            let release_file_id = selection
                .release_file_aliases
                .get(&release_file_id)
                .copied()
                .unwrap_or(release_file_id);
            Some((mapping.target_id, release_file_id))
        })
        .collect()
}

fn manual_approval_coverage_state(
    row: &AcquisitionReleaseCoverage,
    selection: &ResolvedManualFileSelection,
    approved_target_mappings: &BTreeMap<Uuid, Uuid>,
    selected_state: ReleaseCoverageState,
) -> ReleaseCoverageState {
    if row.state == ReleaseCoverageState::Rejected {
        return ReleaseCoverageState::Rejected;
    }

    if !approved_target_mappings.is_empty() {
        let Some(release_file_id) = row.release_file_id else {
            return ReleaseCoverageState::Rejected;
        };
        let release_file_id = selection
            .release_file_aliases
            .get(&release_file_id)
            .copied()
            .unwrap_or(release_file_id);
        return if approved_target_mappings.get(&row.target_id) == Some(&release_file_id) {
            selected_state
        } else {
            ReleaseCoverageState::Rejected
        };
    }

    match row.release_file_id {
        Some(release_file_id)
            if selection
                .skipped_release_file_ids
                .contains(&release_file_id) =>
        {
            ReleaseCoverageState::Rejected
        }
        Some(release_file_id)
            if selection.explicit
                && !selection
                    .selected_release_file_ids
                    .contains(&release_file_id) =>
        {
            ReleaseCoverageState::Rejected
        }
        None if selection.explicit && !selection.selected_release_file_ids.is_empty() => {
            ReleaseCoverageState::Rejected
        }
        _ => selected_state,
    }
}

fn apply_release_file_aliases(
    aliases: &BTreeMap<Uuid, Uuid>,
    selected: &mut BTreeSet<Uuid>,
    skipped: &mut BTreeSet<Uuid>,
) {
    for (synthetic_id, provider_id) in aliases {
        if selected.remove(synthetic_id) {
            selected.insert(*provider_id);
            skipped.remove(provider_id);
            skipped.insert(*synthetic_id);
        }
        if skipped.remove(provider_id) && !selected.contains(provider_id) {
            skipped.insert(*provider_id);
        }
    }
}

async fn validate_manual_review_target_mappings(
    pool: &AnyPool,
    release: &AcquisitionRelease,
    files: &[AcquisitionReleaseFile],
    coverage: &[AcquisitionReleaseCoverage],
    selection: &ResolvedManualFileSelection,
    mappings: &[ManualCoverageMappingRequest],
) -> ApiResult<()> {
    if files.is_empty() {
        return Ok(());
    }

    let files_by_id = files
        .iter()
        .map(|file| (file.release_file_id, file))
        .collect::<BTreeMap<_, _>>();
    let mut targets_by_id = BTreeMap::new();
    let mut targets_by_file = BTreeMap::<Uuid, BTreeMap<Uuid, AcquisitionTarget>>::new();

    for row in coverage {
        let Some(release_file_id) = row.release_file_id else {
            continue;
        };
        if row.state == ReleaseCoverageState::Rejected
            || selection
                .skipped_release_file_ids
                .contains(&release_file_id)
            || !selection
                .selected_release_file_ids
                .contains(&release_file_id)
        {
            continue;
        }
        let Some(file) = files_by_id.get(&release_file_id).copied() else {
            return Err(ApiError::bad_request(format!(
                "coverage references unknown release file {release_file_id}"
            )));
        };
        let target = load_review_mapping_target(pool, &mut targets_by_id, row.target_id).await?;
        validate_review_file_matches_target(release, file, &target)?;
        targets_by_file
            .entry(release_file_id)
            .or_default()
            .insert(target.target_id, target);
    }

    let mut mapped_targets = BTreeSet::new();
    for mapping in mappings {
        if !mapped_targets.insert(mapping.target_id) {
            return Err(ApiError::bad_request(format!(
                "manual review includes multiple file mappings for target {}",
                mapping.target_id
            )));
        }
        let Some(requested_release_file_id) = mapping.release_file_id else {
            continue;
        };
        let release_file_id = selection
            .release_file_aliases
            .get(&requested_release_file_id)
            .copied()
            .unwrap_or(requested_release_file_id);
        let Some(file) = files_by_id.get(&release_file_id).copied() else {
            return Err(ApiError::bad_request(format!(
                "manual mapping references unknown release file {requested_release_file_id}"
            )));
        };
        if !selection
            .selected_release_file_ids
            .contains(&release_file_id)
        {
            return Err(ApiError::bad_request(format!(
                "manual mapping for target {} points to an unselected release file",
                mapping.target_id
            )));
        }
        let target =
            load_review_mapping_target(pool, &mut targets_by_id, mapping.target_id).await?;
        validate_review_file_matches_target(release, file, &target)?;
        targets_by_file
            .entry(release_file_id)
            .or_default()
            .insert(target.target_id, target);
    }

    for (release_file_id, targets) in targets_by_file {
        if targets.len() <= 1 {
            continue;
        }
        let Some(file) = files_by_id.get(&release_file_id).copied() else {
            continue;
        };
        if !review_file_can_cover_multiple_targets(file, targets.values()) {
            return Err(ApiError::bad_request(format!(
                "release file {} cannot be mapped to {} targets without parsed range evidence",
                review_file_label(file),
                targets.len()
            )));
        }
    }

    Ok(())
}

async fn load_review_mapping_target(
    pool: &AnyPool,
    cache: &mut BTreeMap<Uuid, AcquisitionTarget>,
    target_id: Uuid,
) -> ApiResult<AcquisitionTarget> {
    if let Some(target) = cache.get(&target_id) {
        return Ok(target.clone());
    }
    let target = get_target(pool, target_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::bad_request(format!("unknown acquisition target {target_id}")))?;
    cache.insert(target_id, target.clone());
    Ok(target)
}

fn validate_review_file_matches_target(
    release: &AcquisitionRelease,
    file: &AcquisitionReleaseFile,
    target: &AcquisitionTarget,
) -> ApiResult<()> {
    if !matches!(target.media_type, MediaType::Anime | MediaType::Series)
        && !matches!(release.media_type, MediaType::Anime | MediaType::Series)
    {
        return Ok(());
    }

    if let (Some(file_season), Some(target_season)) =
        (file.parsed_season_number, target.season_number)
        && file_season != target_season
        && file.parsed_absolute_episode_number.is_none()
    {
        return Err(review_mapping_mismatch(
            file,
            target,
            format!("parsed as season {file_season}"),
        ));
    }

    if let Some(start) = file.parsed_episode_number {
        let end = file.parsed_episode_end_number.unwrap_or(start);
        if let Some(target_episode) = target.episode_number
            && !number_in_range(target_episode, start, end)
        {
            return Err(review_mapping_mismatch(
                file,
                target,
                format!("parsed as episode {}", format_number_range(start, end)),
            ));
        }
    }

    if let Some(start) = file.parsed_absolute_episode_number {
        let end = file.parsed_absolute_episode_end_number.unwrap_or(start);
        let comparable_target_episode = target.absolute_episode_number.or_else(|| {
            target
                .episode_number
                .filter(|_| target.season_number.unwrap_or(1) == 1)
        });
        if let Some(target_episode) = comparable_target_episode
            && !number_in_range(target_episode, start, end)
        {
            return Err(review_mapping_mismatch(
                file,
                target,
                format!(
                    "parsed as absolute episode {}",
                    format_number_range(start, end)
                ),
            ));
        }
    }

    if let (Some(file_air_date), Some(target_air_date)) =
        (file.parsed_air_date.as_deref(), target.air_date.as_deref())
        && !file_air_date.trim().is_empty()
        && !target_air_date.trim().is_empty()
        && file_air_date.trim() != target_air_date.trim()
    {
        return Err(review_mapping_mismatch(
            file,
            target,
            format!("parsed as air date {}", file_air_date.trim()),
        ));
    }

    Ok(())
}

fn review_file_can_cover_multiple_targets<'a>(
    file: &AcquisitionReleaseFile,
    targets: impl Iterator<Item = &'a AcquisitionTarget>,
) -> bool {
    let targets = targets.collect::<Vec<_>>();
    if targets.len() <= 1 {
        return true;
    }
    let has_episode_range = file
        .parsed_episode_number
        .zip(file.parsed_episode_end_number)
        .is_some_and(|(start, end)| end > start);
    let has_absolute_range = file
        .parsed_absolute_episode_number
        .zip(file.parsed_absolute_episode_end_number)
        .is_some_and(|(start, end)| end > start);
    if !has_episode_range && !has_absolute_range {
        return false;
    }
    targets
        .into_iter()
        .all(|target| validate_review_file_matches_target_for_range(file, target))
}

fn validate_review_file_matches_target_for_range(
    file: &AcquisitionReleaseFile,
    target: &AcquisitionTarget,
) -> bool {
    if let Some(start) = file.parsed_episode_number {
        let end = file.parsed_episode_end_number.unwrap_or(start);
        if let Some(target_episode) = target.episode_number
            && !number_in_range(target_episode, start, end)
        {
            return false;
        }
    }
    if let Some(start) = file.parsed_absolute_episode_number {
        let end = file.parsed_absolute_episode_end_number.unwrap_or(start);
        let comparable_target_episode = target.absolute_episode_number.or_else(|| {
            target
                .episode_number
                .filter(|_| target.season_number.unwrap_or(1) == 1)
        });
        if let Some(target_episode) = comparable_target_episode
            && !number_in_range(target_episode, start, end)
        {
            return false;
        }
    }
    true
}

fn review_mapping_mismatch(
    file: &AcquisitionReleaseFile,
    target: &AcquisitionTarget,
    parsed_detail: String,
) -> ApiError {
    ApiError::bad_request(format!(
        "manual mapping mismatch: release file {} {parsed_detail}, but target is {}",
        review_file_label(file),
        review_target_label(target)
    ))
}

fn review_file_label(file: &AcquisitionReleaseFile) -> String {
    let label = file.basename.trim();
    if !label.is_empty() {
        return label.to_string();
    }
    file.path
        .rsplit(['/', '\\'])
        .next()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(file.path.as_str())
        .to_string()
}

fn review_target_label(target: &AcquisitionTarget) -> String {
    if let (Some(season), Some(episode)) = (target.season_number, target.episode_number) {
        return format!("S{season:02}E{episode:02}");
    }
    if let Some(absolute) = target.absolute_episode_number {
        return format!("A{absolute:04}");
    }
    target.target_key.clone()
}

fn number_in_range(value: i32, start: i32, end: i32) -> bool {
    if start <= end {
        value >= start && value <= end
    } else {
        value >= end && value <= start
    }
}

fn format_number_range(start: i32, end: i32) -> String {
    if start == end {
        start.to_string()
    } else {
        format!("{start}-{end}")
    }
}

fn synthetic_source_candidate_aliases(files: &[AcquisitionReleaseFile]) -> BTreeMap<Uuid, Uuid> {
    let mut aliases = BTreeMap::new();
    for file in files
        .iter()
        .filter(|file| is_synthetic_source_candidate_file(file))
    {
        if let Some(provider_file_id) = matching_inspected_provider_file(files, file) {
            aliases.insert(file.release_file_id, provider_file_id);
        }
    }
    aliases
}

fn matching_inspected_provider_file(
    files: &[AcquisitionReleaseFile],
    synthetic: &AcquisitionReleaseFile,
) -> Option<Uuid> {
    let synthetic_basename = normalized_review_basename(&synthetic.basename);
    if synthetic_basename.is_empty() {
        return None;
    }
    let matches = files
        .iter()
        .filter(|file| !is_synthetic_source_candidate_file(file))
        .filter(|file| file.selectable)
        .filter(|file| {
            file.provider_file_id.as_deref().is_some_and(|value| {
                !value.is_empty() && value != SYNTHETIC_SOURCE_CANDIDATE_FILE_ID
            })
        })
        .filter(|file| normalized_review_basename(&file.basename) == synthetic_basename)
        .map(|file| file.release_file_id)
        .collect::<Vec<_>>();
    if matches.len() == 1 {
        matches.into_iter().next()
    } else {
        None
    }
}

fn is_synthetic_source_candidate_file(file: &AcquisitionReleaseFile) -> bool {
    file.file_id.as_deref() == Some(SYNTHETIC_SOURCE_CANDIDATE_FILE_ID)
        || file.provider_file_id.as_deref() == Some(SYNTHETIC_SOURCE_CANDIDATE_FILE_ID)
        || file
            .raw
            .as_ref()
            .and_then(|value| value.get("source"))
            .and_then(JsonValue::as_str)
            == Some("manual_review_source_candidate")
}

fn normalized_review_basename(value: &str) -> String {
    value
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or(value)
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn resolve_file_key_ids(
    files: &[AcquisitionReleaseFile],
    keys: &[String],
) -> ApiResult<BTreeSet<Uuid>> {
    let mut resolved = BTreeSet::new();
    for key in keys {
        let key = key.trim();
        if key.is_empty() {
            continue;
        }
        let matches = files
            .iter()
            .filter(|file| file_identity_keys(file).contains(key))
            .map(|file| file.release_file_id)
            .collect::<BTreeSet<_>>();
        match matches.len() {
            0 => {
                return Err(ApiError::bad_request(format!(
                    "unknown release file selector '{key}'"
                )));
            }
            1 => {
                resolved.extend(matches);
            }
            _ => {
                return Err(ApiError::bad_request(format!(
                    "ambiguous release file selector '{key}'"
                )));
            }
        }
    }
    Ok(resolved)
}

fn file_identity_keys(file: &AcquisitionReleaseFile) -> BTreeSet<String> {
    let mut keys = BTreeSet::from([
        file.release_file_id.to_string(),
        file.path.clone(),
        file.basename.clone(),
    ]);
    if let Some(file_id) = file.file_id.as_ref().filter(|value| !value.is_empty()) {
        keys.insert(file_id.clone());
    }
    if let Some(provider_file_id) = file
        .provider_file_id
        .as_ref()
        .filter(|value| !value.is_empty())
    {
        keys.insert(provider_file_id.clone());
    }
    if let Some(index) = file.file_index {
        keys.insert(index.to_string());
    }
    keys
}

fn file_policy_key(file: &AcquisitionReleaseFile) -> Option<String> {
    if is_synthetic_source_candidate_file(file) {
        return Some(file.release_file_id.to_string());
    }
    file.file_id
        .clone()
        .or_else(|| file.provider_file_id.clone())
        .or_else(|| file.file_index.map(|index| index.to_string()))
        .or_else(|| Some(file.release_file_id.to_string()))
}

fn approval_policy_json(
    release: &AcquisitionRelease,
    files: &[AcquisitionReleaseFile],
    selection: &ResolvedManualFileSelection,
    user_id: Uuid,
    reason: Option<&str>,
    note: Option<&str>,
) -> JsonValue {
    let review_reasons = release_review_reasons(release, files);
    let selected_release_file_ids = selection
        .selected_release_file_ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let skipped_release_file_ids = selection
        .skipped_release_file_ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let fingerprint = coverage_fingerprint(
        release,
        &selection.selected_file_ids,
        &selection.skipped_file_ids,
        reason,
        note,
    );
    json!({
        "manualReview": {
            "status": "approved",
            "userApproved": true,
            "selectedReleaseFileIds": selected_release_file_ids,
            "skippedReleaseFileIds": skipped_release_file_ids,
            "selectedFileIds": selection.selected_file_ids,
            "skippedFileIds": selection.skipped_file_ids,
            "coverageFingerprint": fingerprint,
            "reviewReasons": review_reasons,
            "reviewerUserId": user_id,
            "reviewedAt": Utc::now(),
            "reason": reason,
            "note": note,
            "previousState": release.state.as_str()
        },
        "priorityPolicy": {
            "policyVersion": "rr7a-manual-review-v1",
            "status": "approved",
            "priorityApplied": false,
            "selectedFileIds": selection.selected_file_ids,
            "skippedFileIds": selection.skipped_file_ids,
            "coverageFingerprint": fingerprint,
            "reviewReasons": review_reasons,
            "userApproved": true
        },
        "animeVerification": {
            "manualReviewApproved": true,
            "manualRemapApproved": !selection.selected_release_file_ids.is_empty(),
            "manualApprovalRequiresImportVerification": true,
            "approvedAt": Utc::now(),
            "reviewerUserId": user_id,
            "reason": reason,
            "note": note
        }
    })
}

fn coverage_fingerprint(
    release: &AcquisitionRelease,
    selected_file_ids: &[String],
    skipped_file_ids: &[String],
    reason: Option<&str>,
    note: Option<&str>,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(release.release_id.as_bytes());
    hasher.update(release.fingerprint.as_bytes());
    for value in selected_file_ids {
        hasher.update(b"\0selected\0");
        hasher.update(value.as_bytes());
    }
    for value in skipped_file_ids {
        hasher.update(b"\0skipped\0");
        hasher.update(value.as_bytes());
    }
    if let Some(reason) = reason {
        hasher.update(b"\0reason\0");
        hasher.update(reason.as_bytes());
    }
    if let Some(note) = note {
        hasher.update(b"\0note\0");
        hasher.update(note.as_bytes());
    }
    format!("sha256:{:x}", hasher.finalize())
}

fn merge_review_policy(existing: Option<&JsonValue>, patch: JsonValue) -> JsonValue {
    let mut merged = match existing {
        Some(JsonValue::Object(object)) => JsonValue::Object(object.clone()),
        Some(value) => json!({ "previousCoveragePlan": value }),
        None => json!({}),
    };
    merge_json_object(&mut merged, patch);
    merged
}

fn merge_json_object(target: &mut JsonValue, patch: JsonValue) {
    let JsonValue::Object(target_object) = target else {
        *target = JsonValue::Object(JsonMap::new());
        merge_json_object(target, patch);
        return;
    };
    if let JsonValue::Object(patch_object) = patch {
        for (key, value) in patch_object {
            target_object.insert(key, value);
        }
    }
}

fn clear_review_suppression(existing: Option<&JsonValue>) -> Option<JsonValue> {
    let Some(existing) = existing else {
        return None;
    };
    let mut value = existing.clone();
    if let JsonValue::Object(map) = &mut value {
        map.remove("retrySuppression");
        if let Some(JsonValue::Object(manual_review)) = map.get_mut("manualReview")
            && manual_review.get("status").and_then(JsonValue::as_str) == Some("rejected")
        {
            manual_review.insert(
                "status".to_string(),
                JsonValue::String("retry_requested".to_string()),
            );
            manual_review.insert("suppressionClearedAt".to_string(), json!(Utc::now()));
        }
    }
    Some(value)
}

fn release_has_approval(release: &AcquisitionRelease) -> bool {
    release
        .coverage_plan
        .as_ref()
        .and_then(|plan| {
            plan.get("manualReview")
                .or_else(|| plan.get("priorityPolicy"))
        })
        .and_then(|policy| policy.get("status"))
        .and_then(JsonValue::as_str)
        == Some("approved")
}

fn selected_coverage_state(release: &AcquisitionRelease) -> ReleaseCoverageState {
    if matches!(
        release.state,
        AcquisitionReleaseState::Submitted
            | AcquisitionReleaseState::Downloading
            | AcquisitionReleaseState::Materializing
            | AcquisitionReleaseState::Completed
    ) {
        ReleaseCoverageState::Submitted
    } else {
        ReleaseCoverageState::Selected
    }
}

fn review_counts(
    files: &[AcquisitionReleaseFile],
    coverage: &[AcquisitionReleaseCoverage],
    jobs: &[AcquisitionReleaseJob],
) -> ReleaseReviewCounts {
    ReleaseReviewCounts {
        file_count: files.len(),
        selected_file_count: files
            .iter()
            .filter(|file| file.selected == Some(true))
            .count(),
        skipped_file_count: files
            .iter()
            .filter(|file| file.selected == Some(false))
            .count(),
        selectable_file_count: files.iter().filter(|file| file.selectable).count(),
        coverage_count: coverage.len(),
        selected_coverage_count: coverage
            .iter()
            .filter(|row| row.state == ReleaseCoverageState::Selected)
            .count(),
        submitted_coverage_count: coverage
            .iter()
            .filter(|row| row.state == ReleaseCoverageState::Submitted)
            .count(),
        review_required_coverage_count: coverage
            .iter()
            .filter(|row| row.state == ReleaseCoverageState::ReviewRequired)
            .count(),
        rejected_coverage_count: coverage
            .iter()
            .filter(|row| row.state == ReleaseCoverageState::Rejected)
            .count(),
        active_job_count: jobs.iter().filter(|job| job.active).count(),
    }
}

fn release_file_review(file: &AcquisitionReleaseFile) -> ReleaseFileReview {
    ReleaseFileReview {
        release_file_id: file.release_file_id,
        file_index: file.file_index,
        file_id: file.file_id.clone(),
        provider_file_id: file.provider_file_id.clone(),
        path: file.path.clone(),
        basename: file.basename.clone(),
        size_bytes: file.size_bytes,
        selectable: file.selectable,
        selected: file.selected,
        local_path: file
            .provider_metadata
            .as_ref()
            .and_then(|value| string_json_path(value, &["localPath"]))
            .or_else(|| {
                file.raw
                    .as_ref()
                    .and_then(|value| string_json_path(value, &["localPath"]))
            }),
        provider_metadata: file.provider_metadata.clone(),
        raw: file.raw.clone(),
        parsed: ReleaseFileParsedMetadata {
            title: file.parsed_title.clone(),
            season_number: file.parsed_season_number,
            episode_number: file.parsed_episode_number,
            episode_end_number: file.parsed_episode_end_number,
            absolute_episode_number: file.parsed_absolute_episode_number,
            absolute_episode_end_number: file.parsed_absolute_episode_end_number,
            air_date: file.parsed_air_date.clone(),
            quality: file.parsed_quality.clone(),
            language: file.parsed_language.clone(),
            release_group: file.parsed_release_group.clone(),
            confidence: file.parser_confidence,
            reason: file.parser_reason.clone(),
        },
        review_reasons: release_file_review_reasons(file),
    }
}

fn release_file_review_reasons(file: &AcquisitionReleaseFile) -> Vec<String> {
    let mut reasons = Vec::new();
    if let Some(reason) = file
        .parser_reason
        .as_ref()
        .filter(|value| !value.is_empty())
    {
        reasons.push(reason.clone());
    }
    for source in [file.provider_metadata.as_ref(), file.raw.as_ref()]
        .into_iter()
        .flatten()
    {
        if let Some(values) = source.get("reviewReasons").and_then(JsonValue::as_array) {
            for value in values.iter().filter_map(JsonValue::as_str) {
                if !value.is_empty() {
                    reasons.push(value.to_string());
                }
            }
        }
    }
    reasons.sort();
    reasons.dedup();
    reasons
}

fn release_review_reasons(
    release: &AcquisitionRelease,
    files: &[AcquisitionReleaseFile],
) -> Vec<String> {
    let mut reasons = Vec::new();
    if let Some(reason) = release
        .state_reason
        .as_ref()
        .filter(|value| !value.is_empty())
    {
        reasons.push(reason.clone());
    }
    for file in files {
        reasons.extend(release_file_review_reasons(file));
    }
    reasons.sort();
    reasons.dedup();
    reasons
}

fn release_review_status(release: &AcquisitionRelease) -> String {
    release
        .coverage_plan
        .as_ref()
        .and_then(|plan| {
            plan.get("manualReview")
                .or_else(|| plan.get("priorityPolicy"))
                .and_then(|policy| policy.get("status"))
                .and_then(JsonValue::as_str)
        })
        .map(str::to_string)
        .unwrap_or_else(|| release.state.as_str().to_string())
}

async fn build_import_run_reviews(
    pool: &AnyPool,
    runs: &[AcquisitionImportRun],
) -> ApiResult<Vec<AcquisitionImportRunReview>> {
    let mut reviews = Vec::with_capacity(runs.len());
    for run in runs {
        let links = list_import_file_links(pool, run.import_run_id)
            .await
            .map_err(ApiError::from)?;
        let mut file_links = Vec::with_capacity(links.len());
        for link in links {
            let file_hash = match link.local_path.as_deref() {
                Some(path) => get_file_hash_by_path(pool, path)
                    .await
                    .map_err(ApiError::from)?,
                None => None,
            };
            file_links.push(AcquisitionImportFileReview { link, file_hash });
        }
        reviews.push(AcquisitionImportRunReview {
            run: run.clone(),
            file_links,
        });
    }
    Ok(reviews)
}

async fn load_anime_import_verification(
    pool: &AnyPool,
    release_id: Uuid,
    import_links: &[AcquisitionImportFileLink],
) -> ApiResult<AnimeImportVerificationReview> {
    let mut file_hashes = Vec::new();
    let mut seen_paths = BTreeSet::new();
    for link in import_links {
        let Some(path) = link.local_path.as_deref() else {
            continue;
        };
        if !seen_paths.insert(path.to_string()) {
            continue;
        }
        if let Some(hash) = get_file_hash_by_path(pool, path)
            .await
            .map_err(ApiError::from)?
        {
            file_hashes.push(hash);
        }
    }
    Ok(AnimeImportVerificationReview {
        file_hashes,
        match_attempts: list_anime_match_attempts_by_release(pool, release_id)
            .await
            .map_err(ApiError::from)?,
        mismatches: list_anime_identity_mismatches_by_release(pool, release_id)
            .await
            .map_err(ApiError::from)?,
    })
}

fn import_summary(
    runs: &[AcquisitionImportRun],
    links: &[AcquisitionImportFileLink],
) -> ReleaseImportSummary {
    let latest = runs
        .iter()
        .max_by_key(|run| run.updated_at)
        .or_else(|| runs.last());
    ReleaseImportSummary {
        run_count: runs.len(),
        pending_run_count: runs
            .iter()
            .filter(|run| {
                matches!(
                    run.state,
                    AcquisitionImportRunState::Pending | AcquisitionImportRunState::Importing
                )
            })
            .count(),
        blocked_run_count: runs
            .iter()
            .filter(|run| run.state == AcquisitionImportRunState::Blocked)
            .count(),
        mismatched_run_count: runs
            .iter()
            .filter(|run| run.state == AcquisitionImportRunState::Mismatched)
            .count(),
        imported_run_count: runs
            .iter()
            .filter(|run| run.state == AcquisitionImportRunState::Imported)
            .count(),
        file_link_count: links.len(),
        pending_file_link_count: links
            .iter()
            .filter(|link| link.state.as_str() == "pending")
            .count(),
        blocked_file_link_count: links
            .iter()
            .filter(|link| link.state.as_str() == "blocked")
            .count(),
        imported_file_link_count: links
            .iter()
            .filter(|link| link.state.as_str() == "imported")
            .count(),
        latest_state: latest.map(|run| run.state.as_str().to_string()),
        latest_reason: latest.and_then(|run| run.state_reason.clone()),
        latest_mismatch_class: latest.and_then(|run| run.mismatch_class.clone()),
    }
}

fn release_evidence(
    release: &AcquisitionRelease,
    import_runs: &[AcquisitionImportRun],
    import_links: &[AcquisitionImportFileLink],
) -> ReleaseReviewEvidence {
    let candidate = release.selected_candidate.clone();
    let plan = release.coverage_plan.clone();
    let import_state = (!import_runs.is_empty() || !import_links.is_empty()).then(|| {
        json!({
            "runs": import_runs,
            "fileLinks": import_links,
            "summary": import_summary(import_runs, import_links),
        })
    });
    ReleaseReviewEvidence {
        source_candidate: plan
            .as_ref()
            .and_then(|value| value.get("sourceCandidate"))
            .cloned()
            .or_else(|| candidate.clone()),
        resolver_evidence: plan
            .as_ref()
            .and_then(|value| value.get("resolverEvidence"))
            .cloned(),
        route_policy: plan
            .as_ref()
            .and_then(|value| value.get("routePolicy"))
            .cloned(),
        target_scope: plan
            .as_ref()
            .and_then(|value| value.get("targetScope"))
            .cloned(),
        source_provider_id: release.source_provider_id,
        route_provider_id: release.selected_provider_id,
        route_logical_id: release.selected_route_logical_id.clone(),
        selected_candidate: candidate.clone(),
        coverage_plan: plan.clone(),
        scheduler_dispatch: candidate
            .as_ref()
            .and_then(|value| json_path(value, &["schedulerDispatch"]))
            .cloned()
            .or_else(|| {
                plan.as_ref()
                    .and_then(|value| json_path(value, &["schedulerDispatch"]))
                    .cloned()
            }),
        submission_result: candidate
            .as_ref()
            .and_then(|value| json_path(value, &["submissionResult"]))
            .cloned()
            .or_else(|| {
                plan.as_ref()
                    .and_then(|value| json_path(value, &["submissionResult"]))
                    .cloned()
            }),
        priority_policy: plan
            .as_ref()
            .and_then(|value| value.get("priorityPolicy"))
            .cloned(),
        manual_review: plan
            .as_ref()
            .and_then(|value| value.get("manualReview"))
            .cloned(),
        diagnostics: release_diagnostics(plan.as_ref()),
        movie_evidence: release_movie_evidence(release),
        retry_policy: plan
            .as_ref()
            .and_then(|value| value.get("retryPolicy"))
            .cloned(),
        debrid_runtime: plan
            .as_ref()
            .and_then(|value| {
                value
                    .get("debridRuntime")
                    .or_else(|| value.get("debridSelection"))
                    .or_else(|| value.get("debridStaging"))
                    .or_else(|| value.get("debridDownload"))
            })
            .cloned(),
        torrent_runtime: plan
            .as_ref()
            .and_then(|value| {
                value
                    .get("torrentRuntime")
                    .or_else(|| value.get("qbittorrentRuntime"))
                    .or_else(|| value.get("priorityPolicy"))
            })
            .cloned(),
        import_state,
        anime_verification: plan
            .as_ref()
            .and_then(|value| value.get("animeVerification"))
            .cloned(),
    }
}

fn release_diagnostics(plan: Option<&JsonValue>) -> Option<JsonValue> {
    let plan = plan?;
    plan.get("diagnostics")
        .or_else(|| plan.pointer("/resolverEvidence/parsedRelease/diagnostics"))
        .or_else(|| plan.pointer("/movie/fileSelection/diagnostics"))
        .cloned()
}

fn release_movie_evidence(release: &AcquisitionRelease) -> Option<JsonValue> {
    if release.media_type != MediaType::Movie
        && release.resolver_kind != ReleaseResolverKind::MovieRadarrStyle
    {
        return None;
    }
    let source_plan = review_movie_source_coverage_plan(release);
    let parsed_release = source_plan
        .as_ref()
        .and_then(|plan| plan.get("parsedRelease"))
        .cloned();
    let graph = source_plan
        .as_ref()
        .and_then(|plan| plan.get("graph"))
        .cloned();
    let reconciliation = source_plan
        .as_ref()
        .and_then(|plan| plan.get("reconciliation"))
        .cloned();
    Some(json!({
        "resolver": {
            "kind": release.resolver_kind.as_str(),
            "version": release.resolver_version,
            "confidence": release.confidence.as_str(),
        },
        "route": {
            "sourceProviderId": release.source_provider_id,
            "routeProviderId": release.selected_provider_id,
            "routeLogicalId": release.selected_route_logical_id,
            "downloadId": release.download_id,
            "remoteReleaseId": release.remote_release_id,
        },
        "sourceMovieCoveragePlan": source_plan,
        "parsedRelease": parsed_release,
        "graph": graph,
        "reconciliation": reconciliation,
        "fileSelection": review_movie_file_selection_evidence(release.coverage_plan.as_ref()),
        "selectionPolicy": review_movie_selection_policy_evidence(release.coverage_plan.as_ref()),
        "runtime": review_movie_runtime_evidence(release.coverage_plan.as_ref()),
    }))
}

fn review_movie_source_coverage_plan(release: &AcquisitionRelease) -> Option<JsonValue> {
    [
        release
            .selected_candidate
            .as_ref()
            .and_then(|value| value.get("movieCoveragePlan")),
        release
            .coverage_plan
            .as_ref()
            .and_then(|value| value.get("movieCoveragePlan")),
        release
            .coverage_plan
            .as_ref()
            .and_then(|value| value.get("coveragePlan")),
        release
            .coverage_plan
            .as_ref()
            .and_then(|value| value.pointer("/resolverEvidence/parsedRelease/coveragePlan")),
        release.coverage_plan.as_ref().filter(|value| {
            value.get("parsedRelease").is_some()
                || value.get("graph").is_some()
                || value.get("reconciliation").is_some()
        }),
    ]
    .into_iter()
    .flatten()
    .next()
    .cloned()
}

fn review_movie_file_selection_evidence(plan: Option<&JsonValue>) -> Option<JsonValue> {
    let plan = plan?;
    plan.pointer("/movie/fileSelection")
        .or_else(|| plan.get("fileSelection"))
        .cloned()
}

fn review_movie_selection_policy_evidence(plan: Option<&JsonValue>) -> Option<JsonValue> {
    let plan = plan?;
    plan.get("selectionPolicy")
        .or_else(|| plan.get("priorityPolicy"))
        .or_else(|| plan.get("manualReview"))
        .cloned()
}

fn review_movie_runtime_evidence(plan: Option<&JsonValue>) -> Option<JsonValue> {
    let plan = plan?;
    let debrid = plan.get("debridRuntime").cloned();
    let torrent = plan
        .get("torrentRuntime")
        .or_else(|| plan.get("qbittorrentRuntime"))
        .cloned();
    if debrid.is_none() && torrent.is_none() {
        return None;
    }
    Some(json!({
        "debrid": debrid,
        "torrent": torrent,
    }))
}

fn coverage_row_evidence(
    release: &AcquisitionRelease,
    row: &AcquisitionReleaseCoverage,
) -> JsonValue {
    json!({
        "releaseId": release.release_id,
        "releaseFingerprint": release.fingerprint,
        "releaseState": release.state.as_str(),
        "coverageState": row.state.as_str(),
        "coverageKind": row.coverage_kind.as_str(),
        "confidence": row.confidence.as_str(),
        "score": row.score,
        "reason": row.reason,
        "verifiedBy": row.verified_by,
    })
}

fn parse_optional_release_state(state: Option<&str>) -> ApiResult<Option<AcquisitionReleaseState>> {
    match state.map(str::trim).filter(|value| !value.is_empty()) {
        Some(value) => AcquisitionReleaseState::from_str(value)
            .map(Some)
            .map_err(|err| ApiError::bad_request(err.to_string())),
        None => Ok(None),
    }
}

fn reviewer_id(user_id: Uuid) -> String {
    format!("user:{user_id}")
}

fn json_path<'a>(value: &'a JsonValue, path: &[&str]) -> Option<&'a JsonValue> {
    let mut current = value;
    for segment in path {
        current = current.get(*segment)?;
    }
    Some(current)
}

fn string_json_path(value: &JsonValue, path: &[&str]) -> Option<String> {
    json_path(value, path)
        .and_then(JsonValue::as_str)
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Row;

    use crate::{
        acquisition::{
            audit::count_acquisition_audit_events,
            imports::{
                AcquisitionImportFileLinkState, AcquisitionImportRunState,
                NewAcquisitionImportFileLink, NewAcquisitionImportRun, create_or_get_import_run,
                get_import_run_by_release_job, list_import_file_links, upsert_import_file_link,
            },
            release_resolution::{
                models::{
                    AnimeFileHashStatus, NewAcquisitionFileHash, NewAcquisitionRelease,
                    NewAcquisitionReleaseFile, NewAcquisitionReleaseJob, ReleaseKind,
                    ReleaseResolverKind,
                },
                review_candidates::{
                    ManualReviewResolverEvidence, ManualReviewRoutePolicyEvidence,
                    ManualReviewTargetScope, NewManualReviewCandidateRelease,
                    upsert_manual_review_candidate_release,
                },
                store::{
                    upsert_file_hash, upsert_release, upsert_release_file, upsert_release_job,
                },
            },
            subscriptions::{
                AcquisitionCompletionPolicy, AcquisitionMetadataPolicy, AcquisitionMonitorPolicy,
                AcquisitionRequestMode, AcquisitionRequestScope, AcquisitionRoutePolicy,
                NewAcquisitionSubscription, NewAcquisitionTarget, create_subscription,
                list_due_candidate_targets, upsert_subscription_targets,
            },
        },
        config::DatabaseConfig,
        db::{Database, models::MediaType},
    };

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

    async fn setup_provider_refs(pool: &AnyPool) -> Result<(Uuid, Uuid)> {
        let instance_id = Uuid::new_v4();
        let provider_id = Uuid::new_v4();
        let extension_id = format!("test.review.debrid.{instance_id}");
        sqlx::query::<sqlx::Any>(
            "INSERT INTO extensions (
                extension_id, name, version, kind, trust_level, manifest_json, enabled
             ) VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&extension_id)
        .bind("Test Review Debrid")
        .bind("0.1.0")
        .bind("module")
        .bind("verified")
        .bind("{}")
        .bind(true)
        .execute(pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO extension_instances (
                instance_id, extension_id, instance_name, config_json, enabled
             ) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(instance_id.to_string())
        .bind(&extension_id)
        .bind("default")
        .bind("{}")
        .bind(true)
        .execute(pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO providers (
                provider_id, instance_id, capability, slot_id, cardinality, implementation
             ) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(provider_id.to_string())
        .bind(instance_id.to_string())
        .bind("debrid.resolver")
        .bind("default")
        .bind("one")
        .bind("test_debrid")
        .execute(pool)
        .await?;
        Ok((provider_id, instance_id))
    }

    async fn setup_release(database: &Database) -> Result<(Uuid, Uuid)> {
        let subscription = create_subscription(
            &database.pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Series,
                title: "Example Show".to_string(),
                year: Some(2024),
                external_ids: None,
                idempotency_key: None,
                request_mode: None,
                request_scope: None,
                scope: None,
                metadata_policy: None,
                completion_policy: None,
                monitor_policy: Default::default(),
                route_policy: AcquisitionRoutePolicy::DebridFirst,
                source_provider_id: None,
                release_delay_seconds: None,
                quality_profile: None,
                metadata_refresh_after: None,
                candidate_search_after: None,
            },
        )
        .await?;
        let targets = upsert_subscription_targets(
            &database.pool,
            subscription.subscription_id,
            vec![NewAcquisitionTarget {
                target_key: Some("S01E01".to_string()),
                media_type: Some(MediaType::Series),
                title: Some("Pilot".to_string()),
                season_number: Some(1),
                episode_number: Some(1),
                absolute_episode_number: None,
                air_date: None,
                air_time: None,
                metadata: None,
                state: Some(AcquisitionTargetState::Searching),
                next_search_after: None,
            }],
        )
        .await?;
        let release = upsert_release(
            &database.pool,
            NewAcquisitionRelease {
                release_id: None,
                subscription_id: Some(subscription.subscription_id),
                source_provider_id: None,
                source_extension_id: "elixir.extensions.test-source".to_string(),
                owner_id: "test".to_string(),
                media_type: MediaType::Series,
                title: "Example Show".to_string(),
                release_title: "Example.Show.S01.COMPLETE.1080p.WEB-DL-GROUP".to_string(),
                source: "test-source".to_string(),
                source_kind: "torrent".to_string(),
                info_hash: Some("0123456789abcdef0123456789abcdef01234567".to_string()),
                fingerprint: "sha256:test-release".to_string(),
                release_kind: ReleaseKind::SeasonPack,
                resolver_kind: ReleaseResolverKind::TvSonarrStyle,
                resolver_version: "test".to_string(),
                confidence: ReleaseConfidence::ReviewRequired,
                score: Some(95.0),
                selected_route_logical_id: Some("acquisition.torrent.default".to_string()),
                selected_provider_id: None,
                download_id: Some("test-download".to_string()),
                remote_release_id: None,
                state: AcquisitionReleaseState::ReviewRequired,
                state_reason: Some("requires manual review".to_string()),
                selected_candidate: Some(json!({
                    "title": "Example.Show.S01.COMPLETE.1080p.WEB-DL-GROUP",
                    "schedulerDispatch": { "schedulerPhase": "rr6c" }
                })),
                coverage_plan: Some(json!({
                    "priorityPolicy": {
                        "status": "review_required",
                        "reviewReasons": ["ambiguous pack"]
                    }
                })),
            },
        )
        .await?;
        let file = upsert_release_file(
            &database.pool,
            NewAcquisitionReleaseFile {
                release_file_id: None,
                release_id: release.release_id,
                file_index: Some(0),
                file_id: Some("0".to_string()),
                provider_file_id: None,
                path: "Example.Show.S01E01.1080p.mkv".to_string(),
                basename: None,
                size_bytes: Some(1_000_000_000),
                selectable: true,
                selected: None,
                parsed_title: Some("Example Show".to_string()),
                parsed_season_number: Some(1),
                parsed_episode_number: Some(1),
                parsed_episode_end_number: None,
                parsed_absolute_episode_number: None,
                parsed_absolute_episode_end_number: None,
                parsed_air_date: None,
                parsed_quality: Some("1080p WEB-DL".to_string()),
                parsed_language: Some("eng".to_string()),
                parsed_release_group: Some("GROUP".to_string()),
                parser_confidence: ReleaseConfidence::High,
                parser_reason: None,
                raw: None,
                provider_metadata: None,
            },
        )
        .await?;
        upsert_release_coverage(
            &database.pool,
            NewAcquisitionReleaseCoverage {
                coverage_id: None,
                release_id: release.release_id,
                release_file_id: Some(file.release_file_id),
                target_id: targets[0].target_id,
                coverage_kind: ReleaseCoverageKind::SeasonPack,
                confidence: ReleaseConfidence::ReviewRequired,
                score: Some(80.0),
                reason: Some("pack needs review".to_string()),
                state: ReleaseCoverageState::ReviewRequired,
                verified_by: None,
            },
        )
        .await?;
        upsert_release_job(
            &database.pool,
            NewAcquisitionReleaseJob {
                release_job_id: None,
                release_id: release.release_id,
                route_logical_id: "acquisition.torrent.default".to_string(),
                provider_id: release.selected_provider_id,
                download_id: release.download_id.clone(),
                remote_release_id: None,
                state: ReleaseJobState::Staging,
                state_reason: Some("waiting review".to_string()),
                active: true,
                started_at: Some(Utc::now()),
                completed_at: None,
            },
        )
        .await?;
        Ok((release.release_id, targets[0].target_id))
    }

    async fn setup_prejob_review_candidate(database: &Database) -> Result<(Uuid, Uuid)> {
        let subscription = create_subscription(
            &database.pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Series,
                title: "Ambiguous Show".to_string(),
                year: Some(2026),
                external_ids: None,
                idempotency_key: None,
                request_mode: None,
                request_scope: None,
                scope: None,
                metadata_policy: None,
                completion_policy: None,
                monitor_policy: Default::default(),
                route_policy: AcquisitionRoutePolicy::DebridFirst,
                source_provider_id: None,
                release_delay_seconds: None,
                quality_profile: None,
                metadata_refresh_after: None,
                candidate_search_after: None,
            },
        )
        .await?;
        let targets = upsert_subscription_targets(
            &database.pool,
            subscription.subscription_id,
            vec![NewAcquisitionTarget {
                target_key: Some("S01E01".to_string()),
                media_type: Some(MediaType::Series),
                title: Some("Pilot".to_string()),
                season_number: Some(1),
                episode_number: Some(1),
                absolute_episode_number: None,
                air_date: None,
                air_time: None,
                metadata: None,
                state: Some(AcquisitionTargetState::Searching),
                next_search_after: Some(Utc::now()),
            }],
        )
        .await?;
        let candidate = AcquisitionCandidate {
            id: Some("torrentio:ambiguous-show:s1".to_string()),
            title: "Ambiguous.Show.S01.Pack.1080p-GROUP".to_string(),
            source: "magnet:?xt=urn:btih:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa&dn=Ambiguous.Show.S01.Pack.1080p-GROUP".to_string(),
            source_kind: "magnet".to_string(),
            info_hash: Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string()),
            file_index: None,
            quality: Some("1080p".to_string()),
            size_bytes: Some(4_000_000_000),
            seeders: Some(12),
            language: Some("en".to_string()),
            cached_debrid: Some(false),
            rank: Some(1),
            score: Some(61.0),
            score_badges: Vec::new(),
            files: Vec::new(),
            supported_routes: vec![
                DEBRID_DEFAULT_LOGICAL_ID.to_string(),
                TORRENT_DEFAULT_LOGICAL_ID.to_string(),
            ],
            default_route: Some(DEBRID_DEFAULT_LOGICAL_ID.to_string()),
            raw: Some(json!({
                "provider": "torrentio",
                "streamName": "Ambiguous Show S01"
            })),
        };
        let release = upsert_manual_review_candidate_release(
            &database.pool,
            NewManualReviewCandidateRelease {
                subscription_id: Some(subscription.subscription_id),
                source_provider_id: None,
                source_extension_id: "elixir.sources.torrentio_stremio".to_string(),
                owner_id: "default".to_string(),
                media_type: MediaType::Series,
                title: "Ambiguous Show".to_string(),
                candidate,
                target_scope: ManualReviewTargetScope {
                    subscription_id: Some(subscription.subscription_id),
                    media_type: MediaType::Series,
                    targets: vec![targets[0].target_id],
                    target_keys: vec!["S01E01".to_string()],
                    season_number: Some(1),
                    episode_numbers: vec![1],
                    absolute_episode_numbers: Vec::new(),
                },
                resolver_evidence: ManualReviewResolverEvidence {
                    resolver_kind: ReleaseResolverKind::TvSonarrStyle,
                    resolver_version: "amr2-test".to_string(),
                    parsed_release: Some(json!({
                        "title": "Ambiguous Show",
                        "seasonNumber": 1,
                        "episodeNumber": null
                    })),
                    rejection_codes: vec![
                        "ambiguous_episode_numbering".to_string(),
                        "pack_shape_not_proven".to_string(),
                    ],
                    candidate_score: Some(61.0),
                    reason: Some("Pack shape cannot be proven before inspection.".to_string()),
                },
                route_policy: ManualReviewRoutePolicyEvidence {
                    preferred: Some("debrid_first".to_string()),
                    allowed_routes: vec![
                        DEBRID_DEFAULT_LOGICAL_ID.to_string(),
                        TORRENT_DEFAULT_LOGICAL_ID.to_string(),
                    ],
                },
                release_kind: ReleaseKind::Unknown,
                score: Some(61.0),
                state_reason: None,
            },
        )
        .await?;
        Ok((release.release_id, targets[0].target_id))
    }

    async fn seed_blocked_import_run(
        database: &Database,
        release_id: Uuid,
        target_id: Uuid,
        local_path: &str,
    ) -> Result<Uuid> {
        let release = get_release(&database.pool, release_id)
            .await?
            .expect("release");
        let files = list_release_files(&database.pool, release_id).await?;
        let release_file_id = files[0].release_file_id;
        let jobs = list_release_jobs(&database.pool, release_id).await?;
        let job = jobs.first().expect("job");
        let (run, _) = create_or_get_import_run(
            &database.pool,
            NewAcquisitionImportRun {
                import_run_id: None,
                release_id,
                release_job_id: job.release_job_id,
                route_logical_id: job.route_logical_id.clone(),
                provider_id: job.provider_id,
                download_id: job.download_id.clone(),
                remote_release_id: job.remote_release_id.clone(),
                state: AcquisitionImportRunState::Blocked,
                state_reason: Some("import path validation failed".to_string()),
                mismatch_class: Some("invalid_import_path".to_string()),
                retry_count: 0,
                provenance: Some(json!({ "test": "rr8d" })),
                started_at: Some(Utc::now()),
                completed_at: None,
            },
        )
        .await?;
        upsert_import_file_link(
            &database.pool,
            NewAcquisitionImportFileLink {
                import_link_id: None,
                import_run_id: run.import_run_id,
                release_id,
                release_file_id: Some(release_file_id),
                target_id: Some(target_id),
                local_path: Some(local_path.to_string()),
                media_file_id: None,
                movie_id: None,
                episode_id: None,
                state: AcquisitionImportFileLinkState::Blocked,
                state_reason: Some("import path validation failed".to_string()),
                verification_state: Some("deferred".to_string()),
                mismatch_class: Some("invalid_import_path".to_string()),
                evidence: Some(json!({
                    "phase": "rr8d-test",
                    "releaseId": release.release_id,
                    "targetId": target_id,
                    "localPath": local_path
                })),
            },
        )
        .await?;
        Ok(run.import_run_id)
    }

    struct ReviewPackFixture {
        release_id: Uuid,
        target_ids: Vec<Uuid>,
        file_ids: Vec<Uuid>,
    }

    async fn setup_tv_review_pack(database: &Database) -> Result<ReviewPackFixture> {
        let subscription = create_subscription(
            &database.pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Series,
                title: "Example Show".to_string(),
                year: Some(2024),
                external_ids: None,
                idempotency_key: None,
                request_mode: None,
                request_scope: None,
                scope: None,
                metadata_policy: None,
                completion_policy: None,
                monitor_policy: Default::default(),
                route_policy: AcquisitionRoutePolicy::DebridFirst,
                source_provider_id: None,
                release_delay_seconds: None,
                quality_profile: None,
                metadata_refresh_after: None,
                candidate_search_after: None,
            },
        )
        .await?;
        let targets = upsert_subscription_targets(
            &database.pool,
            subscription.subscription_id,
            vec![
                NewAcquisitionTarget {
                    target_key: Some("S01E01".to_string()),
                    media_type: Some(MediaType::Series),
                    title: Some("Episode 1".to_string()),
                    season_number: Some(1),
                    episode_number: Some(1),
                    absolute_episode_number: None,
                    air_date: None,
                    air_time: None,
                    metadata: None,
                    state: Some(AcquisitionTargetState::Searching),
                    next_search_after: Some(Utc::now()),
                },
                NewAcquisitionTarget {
                    target_key: Some("S01E02".to_string()),
                    media_type: Some(MediaType::Series),
                    title: Some("Episode 2".to_string()),
                    season_number: Some(1),
                    episode_number: Some(2),
                    absolute_episode_number: None,
                    air_date: None,
                    air_time: None,
                    metadata: None,
                    state: Some(AcquisitionTargetState::Searching),
                    next_search_after: Some(Utc::now()),
                },
            ],
        )
        .await?;
        let release = upsert_release(
            &database.pool,
            NewAcquisitionRelease {
                release_id: None,
                subscription_id: Some(subscription.subscription_id),
                source_provider_id: None,
                source_extension_id: "elixir.extensions.test-source".to_string(),
                owner_id: "test".to_string(),
                media_type: MediaType::Series,
                title: "Example Show".to_string(),
                release_title: "Example.Show.S01.COMPLETE.1080p.WEB-DL-GROUP".to_string(),
                source: "magnet:?xt=urn:btih:pack".to_string(),
                source_kind: "magnet".to_string(),
                info_hash: Some("abcdefabcdefabcdefabcdefabcdefabcdefabcd".to_string()),
                fingerprint: "sha256:rr7c-tv-pack".to_string(),
                release_kind: ReleaseKind::SeasonPack,
                resolver_kind: ReleaseResolverKind::TvSonarrStyle,
                resolver_version: "rr7c-test".to_string(),
                confidence: ReleaseConfidence::ReviewRequired,
                score: Some(90.0),
                selected_route_logical_id: Some("acquisition.torrent.default".to_string()),
                selected_provider_id: None,
                download_id: Some("pack-download".to_string()),
                remote_release_id: Some("pack-download".to_string()),
                state: AcquisitionReleaseState::ReviewRequired,
                state_reason: Some("pack requires manual file review".to_string()),
                selected_candidate: Some(json!({
                    "title": "Example.Show.S01.COMPLETE.1080p.WEB-DL-GROUP",
                    "source": "magnet:?xt=urn:btih:pack"
                })),
                coverage_plan: Some(json!({
                    "priorityPolicy": {
                        "status": "review_required",
                        "reviewReasons": ["ambiguous season pack"]
                    },
                    "schedulerDispatch": {
                        "schedulerPhase": "rr6c",
                        "groupKey": "test-group"
                    }
                })),
            },
        )
        .await?;
        let file_specs = [
            ("ep1", "Example.Show.S01E01.1080p.mkv", Some(1)),
            ("ep2", "Example.Show.S01E02.1080p.mkv", Some(2)),
            ("sample", "Sample.mkv", None),
        ];
        let mut file_ids = Vec::new();
        for (index, (file_id, path, episode)) in file_specs.into_iter().enumerate() {
            let file = upsert_release_file(
                &database.pool,
                NewAcquisitionReleaseFile {
                    release_file_id: None,
                    release_id: release.release_id,
                    file_index: Some(index as i64),
                    file_id: Some(file_id.to_string()),
                    provider_file_id: Some(file_id.to_string()),
                    path: path.to_string(),
                    basename: None,
                    size_bytes: Some(1_000_000),
                    selectable: true,
                    selected: None,
                    parsed_title: Some("Example Show".to_string()),
                    parsed_season_number: episode.map(|_| 1),
                    parsed_episode_number: episode,
                    parsed_episode_end_number: episode,
                    parsed_absolute_episode_number: None,
                    parsed_absolute_episode_end_number: None,
                    parsed_air_date: None,
                    parsed_quality: Some("1080p WEB-DL".to_string()),
                    parsed_language: Some("eng".to_string()),
                    parsed_release_group: Some("GROUP".to_string()),
                    parser_confidence: episode
                        .map(|_| ReleaseConfidence::High)
                        .unwrap_or(ReleaseConfidence::Low),
                    parser_reason: episode.is_none().then(|| "sample file".to_string()),
                    raw: None,
                    provider_metadata: None,
                },
            )
            .await?;
            file_ids.push(file.release_file_id);
        }
        for (target, file_id) in targets.iter().zip(file_ids.iter().take(2)) {
            upsert_release_coverage(
                &database.pool,
                NewAcquisitionReleaseCoverage {
                    coverage_id: None,
                    release_id: release.release_id,
                    release_file_id: Some(*file_id),
                    target_id: target.target_id,
                    coverage_kind: ReleaseCoverageKind::SeasonPack,
                    confidence: ReleaseConfidence::ReviewRequired,
                    score: Some(80.0),
                    reason: Some("season pack needs review".to_string()),
                    state: ReleaseCoverageState::ReviewRequired,
                    verified_by: None,
                },
            )
            .await?;
        }
        upsert_release_job(
            &database.pool,
            NewAcquisitionReleaseJob {
                release_job_id: None,
                release_id: release.release_id,
                route_logical_id: "acquisition.torrent.default".to_string(),
                provider_id: None,
                download_id: Some("pack-download".to_string()),
                remote_release_id: Some("pack-download".to_string()),
                state: ReleaseJobState::Staging,
                state_reason: Some("waiting review".to_string()),
                active: true,
                started_at: Some(Utc::now()),
                completed_at: None,
            },
        )
        .await?;
        Ok(ReviewPackFixture {
            release_id: release.release_id,
            target_ids: targets.into_iter().map(|target| target.target_id).collect(),
            file_ids,
        })
    }

    async fn setup_competing_review_pack(
        database: &Database,
        subscription_id: Uuid,
        target_ids: &[Uuid],
        suffix: &str,
    ) -> Result<ReviewPackFixture> {
        let release = upsert_release(
            &database.pool,
            NewAcquisitionRelease {
                release_id: None,
                subscription_id: Some(subscription_id),
                source_provider_id: None,
                source_extension_id: "elixir.extensions.test-source".to_string(),
                owner_id: "test".to_string(),
                media_type: MediaType::Series,
                title: "Example Show".to_string(),
                release_title: format!("Example.Show.S01.COMPLETE.1080p.{suffix}"),
                source: format!("magnet:?xt=urn:btih:{suffix}"),
                source_kind: "magnet".to_string(),
                info_hash: Some(format!("{suffix:0<40}").chars().take(40).collect()),
                fingerprint: format!("sha256:competing-{suffix}"),
                release_kind: ReleaseKind::SeasonPack,
                resolver_kind: ReleaseResolverKind::TvSonarrStyle,
                resolver_version: "review-prune-test".to_string(),
                confidence: ReleaseConfidence::ReviewRequired,
                score: Some(75.0),
                selected_route_logical_id: Some("acquisition.torrent.default".to_string()),
                selected_provider_id: None,
                download_id: Some(format!("competing-download-{suffix}")),
                remote_release_id: Some(format!("competing-remote-{suffix}")),
                state: AcquisitionReleaseState::ReviewRequired,
                state_reason: Some("competing pack requires manual file review".to_string()),
                selected_candidate: Some(json!({
                    "title": format!("Example.Show.S01.COMPLETE.1080p.{suffix}"),
                    "source": format!("magnet:?xt=urn:btih:{suffix}")
                })),
                coverage_plan: Some(json!({
                    "priorityPolicy": {
                        "status": "review_required",
                        "reviewReasons": ["competing ambiguous season pack"]
                    }
                })),
            },
        )
        .await?;
        let mut file_ids = Vec::new();
        for (index, target_id) in target_ids.iter().enumerate() {
            let episode_number = (index + 1) as i32;
            let file = upsert_release_file(
                &database.pool,
                NewAcquisitionReleaseFile {
                    release_file_id: None,
                    release_id: release.release_id,
                    file_index: Some(index as i64),
                    file_id: Some(format!("{suffix}-ep{episode_number}")),
                    provider_file_id: Some(format!("{suffix}-ep{episode_number}")),
                    path: format!("Example.Show.S01E{:02}.1080p.{suffix}.mkv", episode_number),
                    basename: None,
                    size_bytes: Some(1_000_000),
                    selectable: true,
                    selected: None,
                    parsed_title: Some("Example Show".to_string()),
                    parsed_season_number: Some(1),
                    parsed_episode_number: Some(episode_number),
                    parsed_episode_end_number: Some(episode_number),
                    parsed_absolute_episode_number: None,
                    parsed_absolute_episode_end_number: None,
                    parsed_air_date: None,
                    parsed_quality: Some("1080p WEB-DL".to_string()),
                    parsed_language: Some("eng".to_string()),
                    parsed_release_group: Some("GROUP".to_string()),
                    parser_confidence: ReleaseConfidence::ReviewRequired,
                    parser_reason: Some("competing file needs review".to_string()),
                    raw: None,
                    provider_metadata: None,
                },
            )
            .await?;
            upsert_release_coverage(
                &database.pool,
                NewAcquisitionReleaseCoverage {
                    coverage_id: None,
                    release_id: release.release_id,
                    release_file_id: Some(file.release_file_id),
                    target_id: *target_id,
                    coverage_kind: ReleaseCoverageKind::SeasonPack,
                    confidence: ReleaseConfidence::ReviewRequired,
                    score: Some(70.0),
                    reason: Some("competing pack needs review".to_string()),
                    state: ReleaseCoverageState::ReviewRequired,
                    verified_by: None,
                },
            )
            .await?;
            file_ids.push(file.release_file_id);
        }
        upsert_release_job(
            &database.pool,
            NewAcquisitionReleaseJob {
                release_job_id: None,
                release_id: release.release_id,
                route_logical_id: "acquisition.torrent.default".to_string(),
                provider_id: None,
                download_id: Some(format!("competing-download-{suffix}")),
                remote_release_id: Some(format!("competing-remote-{suffix}")),
                state: ReleaseJobState::Staging,
                state_reason: Some("waiting review".to_string()),
                active: true,
                started_at: Some(Utc::now()),
                completed_at: None,
            },
        )
        .await?;
        Ok(ReviewPackFixture {
            release_id: release.release_id,
            target_ids: target_ids.to_vec(),
            file_ids,
        })
    }

    async fn setup_anime_review_pack(database: &Database) -> Result<ReviewPackFixture> {
        let subscription = create_subscription(
            &database.pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Anime,
                title: "Example Anime".to_string(),
                year: Some(2024),
                external_ids: None,
                idempotency_key: None,
                request_mode: None,
                request_scope: None,
                scope: None,
                metadata_policy: None,
                completion_policy: None,
                monitor_policy: Default::default(),
                route_policy: AcquisitionRoutePolicy::DebridFirst,
                source_provider_id: None,
                release_delay_seconds: None,
                quality_profile: None,
                metadata_refresh_after: None,
                candidate_search_after: None,
            },
        )
        .await?;
        let targets = upsert_subscription_targets(
            &database.pool,
            subscription.subscription_id,
            vec![NewAcquisitionTarget {
                target_key: Some("S01E01".to_string()),
                media_type: Some(MediaType::Anime),
                title: Some("Episode 1".to_string()),
                season_number: Some(1),
                episode_number: Some(1),
                absolute_episode_number: Some(1),
                air_date: None,
                air_time: None,
                metadata: None,
                state: Some(AcquisitionTargetState::Searching),
                next_search_after: Some(Utc::now()),
            }],
        )
        .await?;
        let release = upsert_release(
            &database.pool,
            NewAcquisitionRelease {
                release_id: None,
                subscription_id: Some(subscription.subscription_id),
                source_provider_id: None,
                source_extension_id: "elixir.extensions.test-source".to_string(),
                owner_id: "test".to_string(),
                media_type: MediaType::Anime,
                title: "Example Anime".to_string(),
                release_title: "[Group] Example Anime Batch [1080p]".to_string(),
                source: "magnet:?xt=urn:btih:animepack".to_string(),
                source_kind: "magnet".to_string(),
                info_hash: Some("1234512345123451234512345123451234512345".to_string()),
                fingerprint: "sha256:rr7c-anime-pack".to_string(),
                release_kind: ReleaseKind::SeasonPack,
                resolver_kind: ReleaseResolverKind::AnimeShokoStyle,
                resolver_version: "rr7c-test".to_string(),
                confidence: ReleaseConfidence::ReviewRequired,
                score: Some(90.0),
                selected_route_logical_id: Some("acquisition.debrid.default".to_string()),
                selected_provider_id: None,
                download_id: Some("anime-pack-download".to_string()),
                remote_release_id: Some("remote-anime-pack".to_string()),
                state: AcquisitionReleaseState::ReviewRequired,
                state_reason: Some("anime pack requires review".to_string()),
                selected_candidate: Some(json!({
                    "title": "[Group] Example Anime Batch [1080p]",
                    "source": "magnet:?xt=urn:btih:animepack"
                })),
                coverage_plan: Some(json!({
                    "manualReview": {
                        "status": "review_required",
                        "reviewReasons": ["unmapped extra files"]
                    }
                })),
            },
        )
        .await?;
        let wanted = upsert_release_file(
            &database.pool,
            NewAcquisitionReleaseFile {
                release_file_id: None,
                release_id: release.release_id,
                file_index: Some(0),
                file_id: Some("wanted".to_string()),
                provider_file_id: Some("wanted".to_string()),
                path: "Example Anime - 01 [1080p].mkv".to_string(),
                basename: None,
                size_bytes: Some(1_000_000),
                selectable: true,
                selected: None,
                parsed_title: Some("Example Anime".to_string()),
                parsed_season_number: Some(1),
                parsed_episode_number: Some(1),
                parsed_episode_end_number: Some(1),
                parsed_absolute_episode_number: Some(1),
                parsed_absolute_episode_end_number: Some(1),
                parsed_air_date: None,
                parsed_quality: Some("1080p".to_string()),
                parsed_language: Some("jpn".to_string()),
                parsed_release_group: Some("Group".to_string()),
                parser_confidence: ReleaseConfidence::ReviewRequired,
                parser_reason: Some("manual anime mapping required".to_string()),
                raw: None,
                provider_metadata: None,
            },
        )
        .await?;
        let extra = upsert_release_file(
            &database.pool,
            NewAcquisitionReleaseFile {
                release_file_id: None,
                release_id: release.release_id,
                file_index: Some(1),
                file_id: Some("extra".to_string()),
                provider_file_id: Some("extra".to_string()),
                path: "NCOP.mkv".to_string(),
                basename: None,
                size_bytes: Some(100_000),
                selectable: true,
                selected: None,
                parsed_title: None,
                parsed_season_number: None,
                parsed_episode_number: None,
                parsed_episode_end_number: None,
                parsed_absolute_episode_number: None,
                parsed_absolute_episode_end_number: None,
                parsed_air_date: None,
                parsed_quality: Some("1080p".to_string()),
                parsed_language: None,
                parsed_release_group: Some("Group".to_string()),
                parser_confidence: ReleaseConfidence::Low,
                parser_reason: Some("extra file".to_string()),
                raw: None,
                provider_metadata: None,
            },
        )
        .await?;
        upsert_release_coverage(
            &database.pool,
            NewAcquisitionReleaseCoverage {
                coverage_id: None,
                release_id: release.release_id,
                release_file_id: None,
                target_id: targets[0].target_id,
                coverage_kind: ReleaseCoverageKind::SeasonPack,
                confidence: ReleaseConfidence::ReviewRequired,
                score: Some(60.0),
                reason: Some("needs manual file mapping".to_string()),
                state: ReleaseCoverageState::ReviewRequired,
                verified_by: None,
            },
        )
        .await?;
        upsert_release_job(
            &database.pool,
            NewAcquisitionReleaseJob {
                release_job_id: None,
                release_id: release.release_id,
                route_logical_id: "acquisition.debrid.default".to_string(),
                provider_id: None,
                download_id: Some("anime-pack-download".to_string()),
                remote_release_id: Some("remote-anime-pack".to_string()),
                state: ReleaseJobState::Staging,
                state_reason: Some("waiting review".to_string()),
                active: true,
                started_at: Some(Utc::now()),
                completed_at: None,
            },
        )
        .await?;
        Ok(ReviewPackFixture {
            release_id: release.release_id,
            target_ids: vec![targets[0].target_id],
            file_ids: vec![wanted.release_file_id, extra.release_file_id],
        })
    }

    #[tokio::test]
    async fn detail_response_is_ui_ready() -> Result<()> {
        let database = setup_db().await?;
        let (release_id, _) = setup_release(&database).await?;
        let detail = load_release_detail(&database.pool, release_id)
            .await
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert_eq!(detail.files.len(), 1);
        assert_eq!(detail.coverage.len(), 1);
        assert_eq!(detail.counts.review_required_coverage_count, 1);
        assert!(detail.evidence.scheduler_dispatch.is_some());
        assert_eq!(detail.review_status, "review_required");
        Ok(())
    }

    #[tokio::test]
    async fn rrm6_movie_detail_exposes_diagnostics_without_polluting_file_summary() -> Result<()> {
        let database = setup_db().await?;
        let subscription = create_subscription(
            &database.pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Movie,
                title: "Movie".to_string(),
                year: Some(2024),
                external_ids: None,
                idempotency_key: None,
                request_mode: None,
                request_scope: None,
                scope: None,
                metadata_policy: None,
                completion_policy: None,
                monitor_policy: AcquisitionMonitorPolicy::SelectedTargets,
                route_policy: AcquisitionRoutePolicy::DebridFirst,
                source_provider_id: None,
                release_delay_seconds: Some(0),
                quality_profile: None,
                metadata_refresh_after: None,
                candidate_search_after: None,
            },
        )
        .await?;
        let targets = upsert_subscription_targets(
            &database.pool,
            subscription.subscription_id,
            vec![NewAcquisitionTarget {
                target_key: Some("movie".to_string()),
                media_type: Some(MediaType::Movie),
                title: Some("Movie".to_string()),
                season_number: None,
                episode_number: None,
                absolute_episode_number: None,
                air_date: None,
                air_time: None,
                metadata: None,
                state: Some(AcquisitionTargetState::Submitted),
                next_search_after: None,
            }],
        )
        .await?;
        let source_plan = json!({
            "parsedRelease": {
                "movieTitle": "Movie",
                "year": 2024
            },
            "graph": {
                "targetTitle": "Movie",
                "targetYear": 2024
            },
            "reconciliation": {
                "outcome": "planned",
                "confidence": "high"
            }
        });
        let release = upsert_release(
            &database.pool,
            NewAcquisitionRelease {
                release_id: None,
                subscription_id: Some(subscription.subscription_id),
                source_provider_id: None,
                source_extension_id: "test.source".to_string(),
                owner_id: DEFAULT_ROUTE_OWNER_ID.to_string(),
                media_type: MediaType::Movie,
                title: "Movie".to_string(),
                release_title: "Movie.2024.1080p.WEB-DL-GROUP".to_string(),
                source: "magnet:?xt=urn:btih:rrm6-review".to_string(),
                source_kind: "magnet".to_string(),
                info_hash: Some("rrm6-review".to_string()),
                fingerprint: "rrm6-review".to_string(),
                release_kind: ReleaseKind::Single,
                resolver_kind: ReleaseResolverKind::MovieRadarrStyle,
                resolver_version: "test".to_string(),
                confidence: ReleaseConfidence::ReviewRequired,
                score: Some(100.0),
                selected_route_logical_id: Some(DEBRID_DEFAULT_LOGICAL_ID.to_string()),
                selected_provider_id: None,
                download_id: Some("download-rrm6-review".to_string()),
                remote_release_id: Some("remote-rrm6-review".to_string()),
                state: AcquisitionReleaseState::ReviewRequired,
                state_reason: Some("movie file list requires review".to_string()),
                selected_candidate: Some(json!({
                    "title": "Movie.2024.1080p.WEB-DL-GROUP",
                    "movieCoveragePlan": source_plan,
                })),
                coverage_plan: Some(json!({
                    "source": "debrid_provider_file_list",
                    "movie": {
                        "fileSelection": {
                            "status": "review_required",
                            "selectedFileId": null,
                            "diagnostics": [{
                                "fileId": "0",
                                "path": "Movie.2024.1080p.mkv",
                                "role": "main_candidate",
                                "selected": false
                            }]
                        }
                    },
                    "selectionPolicy": {
                        "status": "review_required",
                        "reviewReasons": ["ambiguous_movie_main_file"]
                    },
                    "debridRuntime": {
                        "status": "waiting_files"
                    }
                })),
            },
        )
        .await?;
        let file = upsert_release_file(
            &database.pool,
            NewAcquisitionReleaseFile {
                release_file_id: None,
                release_id: release.release_id,
                file_index: Some(0),
                file_id: Some("0".to_string()),
                provider_file_id: Some("0".to_string()),
                path: "Movie.2024.1080p.mkv".to_string(),
                basename: None,
                size_bytes: Some(1_000_000_000),
                selectable: true,
                selected: None,
                parsed_title: Some("Movie".to_string()),
                parsed_season_number: None,
                parsed_episode_number: None,
                parsed_episode_end_number: None,
                parsed_absolute_episode_number: None,
                parsed_absolute_episode_end_number: None,
                parsed_air_date: None,
                parsed_quality: Some("1080p WEB-DL".to_string()),
                parsed_language: Some("eng".to_string()),
                parsed_release_group: Some("GROUP".to_string()),
                parser_confidence: ReleaseConfidence::High,
                parser_reason: None,
                raw: Some(json!({
                    "parsed": {
                        "movieTitle": "Movie",
                        "year": 2024
                    }
                })),
                provider_metadata: None,
            },
        )
        .await?;
        upsert_release_coverage(
            &database.pool,
            NewAcquisitionReleaseCoverage {
                coverage_id: None,
                release_id: release.release_id,
                release_file_id: Some(file.release_file_id),
                target_id: targets[0].target_id,
                coverage_kind: ReleaseCoverageKind::Movie,
                confidence: ReleaseConfidence::ReviewRequired,
                score: Some(80.0),
                reason: Some("movie file list requires review".to_string()),
                state: ReleaseCoverageState::ReviewRequired,
                verified_by: Some("rrm5_debrid_movie_file_list".to_string()),
            },
        )
        .await?;

        let detail = load_release_detail(&database.pool, release.release_id)
            .await
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert_eq!(detail.files.len(), 1);
        assert_eq!(detail.files[0].parsed.title.as_deref(), Some("Movie"));
        assert!(detail.files[0].review_reasons.is_empty());
        assert_eq!(
            detail
                .evidence
                .movie_evidence
                .as_ref()
                .and_then(|value| value.pointer("/parsedRelease/movieTitle"))
                .and_then(JsonValue::as_str),
            Some("Movie")
        );
        assert_eq!(
            detail
                .evidence
                .movie_evidence
                .as_ref()
                .and_then(|value| value.pointer("/fileSelection/status"))
                .and_then(JsonValue::as_str),
            Some("review_required")
        );
        assert_eq!(
            detail
                .evidence
                .diagnostics
                .as_ref()
                .and_then(JsonValue::as_array)
                .map(Vec::len),
            Some(1)
        );
        Ok(())
    }

    #[tokio::test]
    async fn amr2_prejob_detail_exposes_source_candidate_evidence() -> Result<()> {
        let database = setup_db().await?;
        let (release_id, target_id) = setup_prejob_review_candidate(&database).await?;
        let detail = load_release_detail(&database.pool, release_id)
            .await
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        assert_eq!(detail.release.download_id, None);
        assert!(detail.jobs.is_empty());
        assert!(detail.files.is_empty());
        assert_eq!(detail.coverage.len(), 1);
        assert_eq!(detail.coverage[0].coverage.target_id, target_id);
        assert_eq!(
            detail.coverage[0].coverage.state,
            ReleaseCoverageState::ReviewRequired
        );
        assert_eq!(detail.evidence.source_provider_id, None);
        assert_eq!(detail.evidence.route_provider_id, None);
        assert_eq!(detail.evidence.route_logical_id, None);
        assert_eq!(
            detail
                .evidence
                .source_candidate
                .as_ref()
                .and_then(|value| value.get("extensionId"))
                .and_then(JsonValue::as_str),
            Some("elixir.sources.torrentio_stremio")
        );
        assert_eq!(
            detail
                .evidence
                .resolver_evidence
                .as_ref()
                .and_then(|value| value.get("rejectionCodes"))
                .and_then(JsonValue::as_array)
                .map(|values| values.len()),
            Some(2)
        );
        assert_eq!(
            detail
                .evidence
                .target_scope
                .as_ref()
                .and_then(|value| value.get("targetKeys"))
                .and_then(JsonValue::as_array)
                .and_then(|values| values.first())
                .and_then(JsonValue::as_str),
            Some("S01E01")
        );
        assert_eq!(
            detail
                .evidence
                .route_policy
                .as_ref()
                .and_then(|value| value.get("preferred"))
                .and_then(JsonValue::as_str),
            Some("debrid_first")
        );
        Ok(())
    }

    #[tokio::test]
    async fn amr5_file_like_review_candidate_exposes_mapping_file() -> Result<()> {
        let database = setup_db().await?;
        let subscription = create_subscription(
            &database.pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Series,
                title: "Star Wars Clone Wars".to_string(),
                year: Some(2003),
                external_ids: None,
                idempotency_key: None,
                request_mode: None,
                request_scope: None,
                scope: None,
                metadata_policy: None,
                completion_policy: None,
                monitor_policy: Default::default(),
                route_policy: AcquisitionRoutePolicy::DebridFirst,
                source_provider_id: None,
                release_delay_seconds: None,
                quality_profile: None,
                metadata_refresh_after: None,
                candidate_search_after: None,
            },
        )
        .await?;
        let targets = upsert_subscription_targets(
            &database.pool,
            subscription.subscription_id,
            vec![NewAcquisitionTarget {
                target_key: Some("S01E01".to_string()),
                media_type: Some(MediaType::Series),
                title: Some("Chapter I".to_string()),
                season_number: Some(1),
                episode_number: Some(1),
                absolute_episode_number: None,
                air_date: None,
                air_time: None,
                metadata: None,
                state: Some(AcquisitionTargetState::Searching),
                next_search_after: Some(Utc::now()),
            }],
        )
        .await?;
        let candidate_title = "Star Wars Clone Wars [2003] Volume 01.mkv";
        let release = upsert_manual_review_candidate_release(
            &database.pool,
            NewManualReviewCandidateRelease {
                subscription_id: Some(subscription.subscription_id),
                source_provider_id: None,
                source_extension_id: "elixir.sources.torrentio_stremio".to_string(),
                owner_id: "default".to_string(),
                media_type: MediaType::Series,
                title: "Star Wars Clone Wars".to_string(),
                candidate: AcquisitionCandidate {
                    id: Some("torrentio:clone-wars-volume-01".to_string()),
                    title: candidate_title.to_string(),
                    source: "magnet:?xt=urn:btih:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb&dn=Star%20Wars%20Clone%20Wars%20Volume%2001".to_string(),
                    source_kind: "magnet".to_string(),
                    info_hash: Some("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string()),
                    file_index: None,
                    quality: Some("480p".to_string()),
                    size_bytes: Some(900_000_000),
                    seeders: Some(5),
                    language: Some("en".to_string()),
                    cached_debrid: Some(false),
                    rank: Some(1),
                    score: Some(42.0),
                    score_badges: Vec::new(),
                    files: Vec::new(),
                    supported_routes: vec![
                        DEBRID_DEFAULT_LOGICAL_ID.to_string(),
                        TORRENT_DEFAULT_LOGICAL_ID.to_string(),
                    ],
                    default_route: Some(DEBRID_DEFAULT_LOGICAL_ID.to_string()),
                    raw: Some(json!({ "provider": "torrentio" })),
                },
                target_scope: ManualReviewTargetScope {
                    subscription_id: Some(subscription.subscription_id),
                    media_type: MediaType::Series,
                    targets: vec![targets[0].target_id],
                    target_keys: vec!["S01E01".to_string()],
                    season_number: Some(1),
                    episode_numbers: vec![1],
                    absolute_episode_numbers: Vec::new(),
                },
                resolver_evidence: ManualReviewResolverEvidence {
                    resolver_kind: ReleaseResolverKind::TvSonarrStyle,
                    resolver_version: "amr5-test".to_string(),
                    parsed_release: Some(json!({ "title": candidate_title })),
                    rejection_codes: vec!["unknown_numbering".to_string()],
                    candidate_score: Some(42.0),
                    reason: Some("Candidate title needs manual mapping.".to_string()),
                },
                route_policy: ManualReviewRoutePolicyEvidence {
                    preferred: Some("debrid_first".to_string()),
                    allowed_routes: vec![
                        DEBRID_DEFAULT_LOGICAL_ID.to_string(),
                        TORRENT_DEFAULT_LOGICAL_ID.to_string(),
                    ],
                },
                release_kind: ReleaseKind::Unknown,
                score: Some(42.0),
                state_reason: None,
            },
        )
        .await?;

        let detail = load_release_detail(&database.pool, release.release_id)
            .await
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        assert_eq!(detail.files.len(), 1);
        assert_eq!(detail.files[0].path, candidate_title);
        assert!(detail.files[0].selectable);
        assert_eq!(detail.files[0].size_bytes, Some(900_000_000));
        assert_eq!(detail.coverage.len(), 1);
        assert_eq!(
            detail.coverage[0].target.as_ref().unwrap().title,
            "Chapter I"
        );
        Ok(())
    }

    #[tokio::test]
    async fn amr2_prejob_route_selection_prefers_debrid_and_allows_override() -> Result<()> {
        let database = setup_db().await?;
        let (release_id, _) = setup_prejob_review_candidate(&database).await?;
        let release = get_release(&database.pool, release_id)
            .await?
            .expect("release");

        assert_eq!(
            select_review_route_logical_id(&release, None)
                .map_err(|err| anyhow::anyhow!("{err:?}"))?,
            DEBRID_DEFAULT_LOGICAL_ID
        );
        assert_eq!(
            select_review_route_logical_id(&release, Some(TORRENT_DEFAULT_LOGICAL_ID))
                .map_err(|err| anyhow::anyhow!("{err:?}"))?,
            TORRENT_DEFAULT_LOGICAL_ID
        );
        Ok(())
    }

    #[tokio::test]
    async fn amr2_approve_prejob_candidate_persists_manual_policy_without_existing_job()
    -> Result<()> {
        let database = setup_db().await?;
        let user_id = Uuid::new_v4();
        let (release_id, target_id) = setup_prejob_review_candidate(&database).await?;

        approve_release_for_review(
            None,
            &database.pool,
            user_id,
            release_id,
            ApproveAcquisitionReleaseRequest {
                reason: Some("user verified source candidate".to_string()),
                ..Default::default()
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        let release = get_release(&database.pool, release_id)
            .await?
            .expect("release");
        assert_eq!(release.state, AcquisitionReleaseState::Ready);
        assert_eq!(release.download_id, None);
        let plan = release.coverage_plan.as_ref().expect("coverage plan");
        assert_eq!(
            plan.get("manualReview")
                .and_then(|value| value.get("status"))
                .and_then(JsonValue::as_str),
            Some("approved")
        );
        let coverage = list_release_coverage(&database.pool, release_id).await?;
        assert_eq!(coverage[0].state, ReleaseCoverageState::Selected);
        let target = get_target(&database.pool, target_id)
            .await?
            .expect("target");
        assert_eq!(target.state, AcquisitionTargetState::Submitted);
        assert!(
            list_release_jobs(&database.pool, release_id)
                .await?
                .is_empty()
        );
        assert_eq!(
            count_acquisition_audit_events(&database.pool, release_id, EVENT_MANUAL_APPROVAL)
                .await?,
            1
        );
        Ok(())
    }

    #[tokio::test]
    async fn amr2_inspect_existing_release_marks_inspection_requested() -> Result<()> {
        let database = setup_db().await?;
        let user_id = Uuid::new_v4();
        let (release_id, target_id) = setup_release(&database).await?;
        let before = get_release(&database.pool, release_id)
            .await?
            .expect("release");

        mark_existing_release_inspection_requested(
            &database.pool,
            user_id,
            &before,
            "inspect files before approving",
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        let release = get_release(&database.pool, release_id)
            .await?
            .expect("release");
        assert_eq!(release.state, before.state);
        assert_eq!(release.download_id, before.download_id);
        assert_eq!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|value| value.get("manualReview"))
                .and_then(|value| value.get("status"))
                .and_then(JsonValue::as_str),
            Some("inspection_requested")
        );
        let target = get_target(&database.pool, target_id)
            .await?
            .expect("target");
        assert_eq!(target.state, AcquisitionTargetState::Searching);
        assert_eq!(
            count_acquisition_audit_events(&database.pool, release_id, EVENT_INSPECT_REQUESTED)
                .await?,
            1
        );
        Ok(())
    }

    #[tokio::test]
    async fn amr2_reject_defaults_pending_and_retry_clears_suppression_only_when_requested()
    -> Result<()> {
        let database = setup_db().await?;
        let user_id = Uuid::new_v4();
        let (release_id, target_id) = setup_prejob_review_candidate(&database).await?;

        reject_release_for_review(
            &database.pool,
            user_id,
            release_id,
            RejectAcquisitionReleaseRequest {
                reason: "wrong release".to_string(),
                note: None,
                target_policy: RejectTargetPolicy::default(),
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        let rejected = get_release(&database.pool, release_id)
            .await?
            .expect("release");
        let plan = rejected.coverage_plan.as_ref().expect("coverage plan");
        assert_eq!(rejected.state, AcquisitionReleaseState::Cancelled);
        assert!(plan.get("retrySuppression").is_some());
        assert_eq!(
            plan.get("manualReview")
                .and_then(|value| value.get("targetPolicy"))
                .and_then(JsonValue::as_str),
            Some("pending")
        );
        let target = get_target(&database.pool, target_id)
            .await?
            .expect("target");
        assert_eq!(target.state, AcquisitionTargetState::Pending);
        assert_eq!(
            count_acquisition_audit_events(&database.pool, release_id, EVENT_MANUAL_REJECTION)
                .await?,
            1
        );

        retry_release_for_review(
            &database.pool,
            user_id,
            release_id,
            RetryAcquisitionReleaseRequest {
                mode: RetryMode::SourceDiscovery,
                reason: Some("retry without clearing suppression".to_string()),
                next_search_after: None,
                clear_suppression: false,
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        let still_suppressed = get_release(&database.pool, release_id)
            .await?
            .expect("release");
        let plan = still_suppressed
            .coverage_plan
            .as_ref()
            .expect("coverage plan");
        assert!(plan.get("retrySuppression").is_some());
        assert_eq!(
            plan.get("retryPolicy")
                .and_then(|value| value.get("clearSuppression"))
                .and_then(JsonValue::as_bool),
            Some(false)
        );

        retry_release_for_review(
            &database.pool,
            user_id,
            release_id,
            RetryAcquisitionReleaseRequest {
                mode: RetryMode::SourceDiscovery,
                reason: Some("retry and allow rediscovery".to_string()),
                next_search_after: None,
                clear_suppression: true,
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        let unsuppressed = get_release(&database.pool, release_id)
            .await?
            .expect("release");
        let plan = unsuppressed.coverage_plan.as_ref().expect("coverage plan");
        assert!(plan.get("retrySuppression").is_none());
        assert_eq!(
            plan.get("manualReview")
                .and_then(|value| value.get("status"))
                .and_then(JsonValue::as_str),
            Some("retry_requested")
        );
        assert_eq!(
            plan.get("retryPolicy")
                .and_then(|value| value.get("clearSuppression"))
                .and_then(JsonValue::as_bool),
            Some(true)
        );
        Ok(())
    }

    #[tokio::test]
    async fn rr8d_detail_response_exposes_import_state_and_links() -> Result<()> {
        let database = setup_db().await?;
        let (release_id, target_id) = setup_release(&database).await?;
        seed_blocked_import_run(
            &database,
            release_id,
            target_id,
            "/tmp/elixir-rr8d/Example.Show.S01E01.mkv",
        )
        .await?;

        let detail = load_release_detail(&database.pool, release_id)
            .await
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        assert_eq!(detail.imports.len(), 1);
        assert_eq!(
            detail.imports[0].run.state,
            AcquisitionImportRunState::Blocked
        );
        assert_eq!(detail.imports[0].file_links.len(), 1);
        assert_eq!(
            detail.imports[0].file_links[0]
                .link
                .mismatch_class
                .as_deref(),
            Some("invalid_import_path")
        );
        assert_eq!(
            detail.imports[0].file_links[0]
                .link
                .verification_state
                .as_deref(),
            Some("deferred")
        );
        assert!(detail.evidence.import_state.is_some());

        let coverage = acquisition_subscription_coverage_for_test(&database, release_id).await?;
        let target = coverage
            .targets
            .iter()
            .find(|row| row.target.target_id == target_id)
            .expect("target row");
        assert_eq!(target.import_links.len(), 1);
        Ok(())
    }

    async fn acquisition_subscription_coverage_for_test(
        database: &Database,
        release_id: Uuid,
    ) -> Result<AcquisitionSubscriptionCoverageResponse> {
        let release = get_release(&database.pool, release_id)
            .await?
            .expect("release");
        let subscription_id = release.subscription_id.expect("subscription");
        let subscription = get_subscription(&database.pool, subscription_id)
            .await?
            .expect("subscription");
        let targets = list_subscription_targets(&database.pool, subscription_id).await?;
        let releases = list_releases(
            &database.pool,
            ReleaseListFilter {
                subscription_id: Some(subscription_id),
                state: None,
                limit: Some(500),
            },
        )
        .await?;
        let mut target_rows = targets
            .into_iter()
            .map(|target| {
                (
                    target.target_id,
                    TargetCoverageReview {
                        target,
                        coverage: Vec::new(),
                        import_links: Vec::new(),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut summaries = Vec::new();
        for release in releases {
            let coverage = list_release_coverage(&database.pool, release.release_id).await?;
            let detail_targets =
                targets_for_release(&database.pool, release.subscription_id, &coverage).await?;
            for row in coverage {
                if let Some(target) = target_rows.get_mut(&row.target_id) {
                    target.coverage.push(ReleaseCoverageReview {
                        release_file_id: row.release_file_id,
                        evidence: coverage_row_evidence(&release, &row),
                        target: detail_targets.get(&row.target_id).cloned(),
                        coverage: row,
                    });
                }
            }
            for link in
                list_import_file_links_by_release(&database.pool, release.release_id).await?
            {
                if let Some(target_id) = link.target_id
                    && let Some(target) = target_rows.get_mut(&target_id)
                {
                    target.import_links.push(link);
                }
            }
            summaries.push(
                build_release_summary(&database.pool, release)
                    .await
                    .map_err(|err| anyhow::anyhow!("{err:?}"))?,
            );
        }
        Ok(AcquisitionSubscriptionCoverageResponse {
            subscription,
            targets: target_rows.into_values().collect(),
            releases: summaries,
        })
    }

    #[tokio::test]
    async fn approve_persists_policy_and_file_selection() -> Result<()> {
        let database = setup_db().await?;
        let user_id = Uuid::new_v4();
        let (release_id, _) = setup_release(&database).await?;
        approve_release_for_review(
            None,
            &database.pool,
            user_id,
            release_id,
            ApproveAcquisitionReleaseRequest {
                selected_file_ids: vec!["0".to_string()],
                reason: Some("verified pack".to_string()),
                ..Default::default()
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        let release = get_release(&database.pool, release_id)
            .await?
            .expect("release");
        assert_eq!(release.state, AcquisitionReleaseState::Ready);
        let plan = release.coverage_plan.expect("coverage plan");
        assert_eq!(
            plan.get("priorityPolicy")
                .and_then(|value| value.get("status"))
                .and_then(JsonValue::as_str),
            Some("approved")
        );
        let files = list_release_files(&database.pool, release_id).await?;
        assert_eq!(files[0].selected, Some(true));
        let coverage = list_release_coverage(&database.pool, release_id).await?;
        assert_eq!(coverage[0].state, ReleaseCoverageState::Selected);
        Ok(())
    }

    #[tokio::test]
    async fn approve_resolves_synthetic_source_candidate_to_inspected_provider_file() -> Result<()>
    {
        let database = setup_db().await?;
        let user_id = Uuid::new_v4();
        let (release_id, target_id) = setup_release(&database).await?;
        let synthetic = upsert_release_file(
            &database.pool,
            NewAcquisitionReleaseFile {
                release_file_id: None,
                release_id,
                file_index: Some(8),
                file_id: Some(SYNTHETIC_SOURCE_CANDIDATE_FILE_ID.to_string()),
                provider_file_id: Some(SYNTHETIC_SOURCE_CANDIDATE_FILE_ID.to_string()),
                path: "Star Wars Clone Wars [2003] Volume 01.mkv".to_string(),
                basename: None,
                size_bytes: Some(906_970_000),
                selectable: true,
                selected: None,
                parsed_title: None,
                parsed_season_number: None,
                parsed_episode_number: None,
                parsed_episode_end_number: None,
                parsed_absolute_episode_number: None,
                parsed_absolute_episode_end_number: None,
                parsed_air_date: None,
                parsed_quality: Some("DVDRip".to_string()),
                parsed_language: None,
                parsed_release_group: None,
                parser_confidence: ReleaseConfidence::ReviewRequired,
                parser_reason: Some(
                    "Source candidate file row created for manual review mapping.".to_string(),
                ),
                raw: Some(json!({
                    "source": "manual_review_source_candidate",
                    "synthetic": true
                })),
                provider_metadata: None,
            },
        )
        .await?;
        let inspected = upsert_release_file(
            &database.pool,
            NewAcquisitionReleaseFile {
                release_file_id: None,
                release_id,
                file_index: Some(6),
                file_id: Some("6".to_string()),
                provider_file_id: Some("6".to_string()),
                path: "/completed/hash/Star Wars Clone Wars [2003] Volume 01.mkv".to_string(),
                basename: None,
                size_bytes: Some(951_026_048),
                selectable: true,
                selected: None,
                parsed_title: None,
                parsed_season_number: None,
                parsed_episode_number: None,
                parsed_episode_end_number: None,
                parsed_absolute_episode_number: None,
                parsed_absolute_episode_end_number: None,
                parsed_air_date: None,
                parsed_quality: Some("DVDRip".to_string()),
                parsed_language: None,
                parsed_release_group: None,
                parser_confidence: ReleaseConfidence::ReviewRequired,
                parser_reason: Some("manual mapping required".to_string()),
                raw: None,
                provider_metadata: None,
            },
        )
        .await?;

        approve_release_for_review(
            None,
            &database.pool,
            user_id,
            release_id,
            ApproveAcquisitionReleaseRequest {
                selected_release_file_ids: vec![synthetic.release_file_id],
                skipped_release_file_ids: vec![inspected.release_file_id],
                mappings: vec![ManualCoverageMappingRequest {
                    target_id,
                    release_file_id: Some(synthetic.release_file_id),
                    coverage_kind: Some(ReleaseCoverageKind::ManualOverride),
                    confidence: Some(ReleaseConfidence::High),
                    score: Some(100.0),
                    reason: Some("manual volume mapping".to_string()),
                }],
                reason: Some("verified inspected provider file".to_string()),
                ..Default::default()
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        let release = get_release(&database.pool, release_id)
            .await?
            .expect("release");
        let plan = release.coverage_plan.expect("coverage plan");
        assert_eq!(
            plan.pointer("/manualReview/selectedFileIds/0")
                .and_then(JsonValue::as_str),
            Some("6")
        );
        let synthetic_release_file_id = synthetic.release_file_id.to_string();
        assert!(
            plan.pointer("/manualReview/skippedFileIds")
                .and_then(JsonValue::as_array)
                .expect("skipped ids")
                .iter()
                .any(|value| value.as_str() == Some(synthetic_release_file_id.as_str()))
        );

        let files = list_release_files(&database.pool, release_id).await?;
        let selected = files
            .iter()
            .map(|file| (file.release_file_id, file.selected))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(selected.get(&inspected.release_file_id), Some(&Some(true)));
        assert_eq!(selected.get(&synthetic.release_file_id), Some(&Some(false)));

        let coverage = list_release_coverage(&database.pool, release_id).await?;
        assert!(coverage.iter().any(|row| {
            row.target_id == target_id
                && row.release_file_id == Some(inspected.release_file_id)
                && row.confidence == ReleaseConfidence::High
                && row.state == ReleaseCoverageState::Selected
        }));
        Ok(())
    }

    #[tokio::test]
    async fn approve_resumes_review_required_debrid_job_for_post_transfer_selection() -> Result<()>
    {
        let database = setup_db().await?;
        let user_id = Uuid::new_v4();
        let (release_id, _) = setup_release(&database).await?;
        let job_id = Uuid::new_v4();
        let (provider_id, instance_id) = setup_provider_refs(&database.pool).await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO debrid_download_jobs (
                job_id, provider_id, instance_id, owner_id, source, source_kind,
                status, remote_release_status, selected_file_ids_json,
                skipped_file_ids_json, selection_error, last_error, release_id
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(job_id.to_string())
        .bind(provider_id.to_string())
        .bind(instance_id.to_string())
        .bind("default")
        .bind("magnet:?xt=urn:btih:review")
        .bind("magnet")
        .bind("review_required")
        .bind("review_required")
        .bind("[]")
        .bind("[\"0\",\"1\",\"source-candidate\"]")
        .bind("coverage_not_high_confidence,no_selected_files")
        .bind("manual review required")
        .bind(release_id.to_string())
        .execute(&database.pool)
        .await?;

        approve_release_for_review(
            None,
            &database.pool,
            user_id,
            release_id,
            ApproveAcquisitionReleaseRequest {
                selected_file_ids: vec!["0".to_string()],
                reason: Some("verified debrid file".to_string()),
                ..Default::default()
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        let row = sqlx::query::<sqlx::Any>(
            "SELECT status, remote_release_status, selected_file_ids_json,
                    skipped_file_ids_json,
                    CASE WHEN selection_error IS NULL THEN 1 ELSE 0 END AS selection_error_null,
                    CASE WHEN last_error IS NULL THEN 1 ELSE 0 END AS last_error_null
             FROM debrid_download_jobs
             WHERE job_id = ?",
        )
        .bind(job_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(row.try_get::<String, _>("status")?, "submitted");
        assert_eq!(
            row.try_get::<Option<String>, _>("remote_release_status")?
                .as_deref(),
            Some("submitted")
        );
        assert_eq!(row.try_get::<String, _>("selected_file_ids_json")?, "[]");
        assert_eq!(row.try_get::<String, _>("skipped_file_ids_json")?, "[]");
        assert_eq!(row.try_get::<i64, _>("selection_error_null")?, 1);
        assert_eq!(row.try_get::<i64, _>("last_error_null")?, 1);
        Ok(())
    }

    #[tokio::test]
    async fn rr7c_tv_pack_approval_submits_targets_and_preserves_exact_files() -> Result<()> {
        let database = setup_db().await?;
        let user_id = Uuid::new_v4();
        let fixture = setup_tv_review_pack(&database).await?;
        approve_release_for_review(
            None,
            &database.pool,
            user_id,
            fixture.release_id,
            ApproveAcquisitionReleaseRequest {
                selected_release_file_ids: fixture.file_ids[0..2].to_vec(),
                skipped_release_file_ids: vec![fixture.file_ids[2]],
                reason: Some("verified exact TV pack files".to_string()),
                ..Default::default()
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        let release = get_release(&database.pool, fixture.release_id)
            .await?
            .expect("release");
        assert_eq!(release.state, AcquisitionReleaseState::Ready);
        assert_eq!(release.download_id.as_deref(), Some("pack-download"));
        let plan = release.coverage_plan.as_ref().expect("coverage plan");
        assert_eq!(
            plan.get("manualReview")
                .and_then(|value| value.get("status"))
                .and_then(JsonValue::as_str),
            Some("approved")
        );
        let files = list_release_files(&database.pool, fixture.release_id).await?;
        let selected = files
            .iter()
            .map(|file| (file.release_file_id, file.selected))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(selected.get(&fixture.file_ids[0]), Some(&Some(true)));
        assert_eq!(selected.get(&fixture.file_ids[1]), Some(&Some(true)));
        assert_eq!(selected.get(&fixture.file_ids[2]), Some(&Some(false)));
        let coverage = list_release_coverage(&database.pool, fixture.release_id).await?;
        assert_eq!(coverage.len(), 2);
        assert!(
            coverage
                .iter()
                .all(|row| row.state == ReleaseCoverageState::Selected)
        );
        for target_id in &fixture.target_ids {
            let target = get_target(&database.pool, *target_id)
                .await?
                .expect("target");
            assert_eq!(target.state, AcquisitionTargetState::Submitted);
            assert_eq!(target.download_id.as_deref(), Some("pack-download"));
            assert_eq!(
                target.selected_route_logical_id.as_deref(),
                Some("acquisition.torrent.default")
            );
        }
        let due = list_due_candidate_targets(&database.pool, Utc::now(), 10).await?;
        assert!(
            due.is_empty(),
            "approved targets must not be searched again"
        );
        Ok(())
    }

    #[tokio::test]
    async fn osr4_manual_review_approval_selects_only_requested_episode_from_pack() -> Result<()> {
        let database = setup_db().await?;
        let user_id = Uuid::new_v4();
        let subscription = create_subscription(
            &database.pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Series,
                title: "Scoped Show".to_string(),
                year: Some(2026),
                external_ids: None,
                idempotency_key: Some("osr4-manual-scope".to_string()),
                request_mode: Some(AcquisitionRequestMode::OneShot),
                request_scope: Some(AcquisitionRequestScope::Episode),
                scope: Some(json!({
                    "kind": "episode",
                    "seasonNumber": 1,
                    "episodeNumber": 1,
                    "targetKey": "S01E01"
                })),
                metadata_policy: Some(AcquisitionMetadataPolicy::InitialOnly),
                completion_policy: Some(AcquisitionCompletionPolicy::TerminalSelectedTargets),
                monitor_policy: AcquisitionMonitorPolicy::SelectedTargets,
                route_policy: AcquisitionRoutePolicy::DebridFirst,
                source_provider_id: None,
                release_delay_seconds: Some(0),
                quality_profile: None,
                metadata_refresh_after: None,
                candidate_search_after: None,
            },
        )
        .await?;
        let targets = upsert_subscription_targets(
            &database.pool,
            subscription.subscription_id,
            vec![NewAcquisitionTarget {
                target_key: Some("S01E01".to_string()),
                media_type: Some(MediaType::Series),
                title: Some("Episode 1".to_string()),
                season_number: Some(1),
                episode_number: Some(1),
                absolute_episode_number: None,
                air_date: None,
                air_time: None,
                metadata: None,
                state: Some(AcquisitionTargetState::Searching),
                next_search_after: Some(Utc::now()),
            }],
        )
        .await?;
        let target_id = targets[0].target_id;
        let release = upsert_release(
            &database.pool,
            NewAcquisitionRelease {
                release_id: None,
                subscription_id: Some(subscription.subscription_id),
                source_provider_id: None,
                source_extension_id: "elixir.extensions.test-source".to_string(),
                owner_id: "test".to_string(),
                media_type: MediaType::Series,
                title: "Scoped Show".to_string(),
                release_title: "Scoped.Show.S01.COMPLETE.1080p.WEB-DL-GROUP".to_string(),
                source: "magnet:?xt=urn:btih:osr4manualscope".to_string(),
                source_kind: "magnet".to_string(),
                info_hash: Some("1111111111111111111111111111111111111111".to_string()),
                fingerprint: "sha256:osr4-manual-scope-pack".to_string(),
                release_kind: ReleaseKind::SeasonPack,
                resolver_kind: ReleaseResolverKind::TvSonarrStyle,
                resolver_version: "osr4-test".to_string(),
                confidence: ReleaseConfidence::ReviewRequired,
                score: Some(92.0),
                selected_route_logical_id: Some(DEBRID_DEFAULT_LOGICAL_ID.to_string()),
                selected_provider_id: None,
                download_id: Some("osr4-manual-download".to_string()),
                remote_release_id: Some("osr4-manual-download".to_string()),
                state: AcquisitionReleaseState::ReviewRequired,
                state_reason: Some("one-shot pack requires manual scoped selection".to_string()),
                selected_candidate: Some(json!({
                    "title": "Scoped.Show.S01.COMPLETE.1080p.WEB-DL-GROUP",
                    "source": "magnet:?xt=urn:btih:osr4manualscope"
                })),
                coverage_plan: Some(json!({
                    "requestScopeEvidence": {
                        "requestMode": "one_shot",
                        "requestScope": "episode",
                        "metadataPolicy": "initial_only",
                        "completionPolicy": "terminal_selected_targets",
                        "monitorPolicy": "selected_targets",
                        "targetCount": 1,
                        "targetIds": [target_id],
                        "targetKeys": ["S01E01"]
                    },
                    "priorityPolicy": {
                        "status": "review_required",
                        "reviewReasons": ["scoped pack needs manual file selection"]
                    }
                })),
            },
        )
        .await?;
        let file_specs = [
            ("ep1", "Scoped.Show.S01E01.1080p.mkv", Some(1)),
            ("ep2", "Scoped.Show.S01E02.1080p.mkv", Some(2)),
            ("sample", "Sample.mkv", None),
        ];
        let mut file_ids = Vec::new();
        for (index, (file_id, path, episode)) in file_specs.into_iter().enumerate() {
            let file = upsert_release_file(
                &database.pool,
                NewAcquisitionReleaseFile {
                    release_file_id: None,
                    release_id: release.release_id,
                    file_index: Some(index as i64),
                    file_id: Some(file_id.to_string()),
                    provider_file_id: Some(file_id.to_string()),
                    path: path.to_string(),
                    basename: None,
                    size_bytes: Some(1_000_000),
                    selectable: true,
                    selected: None,
                    parsed_title: Some("Scoped Show".to_string()),
                    parsed_season_number: episode.map(|_| 1),
                    parsed_episode_number: episode,
                    parsed_episode_end_number: episode,
                    parsed_absolute_episode_number: None,
                    parsed_absolute_episode_end_number: None,
                    parsed_air_date: None,
                    parsed_quality: Some("1080p WEB-DL".to_string()),
                    parsed_language: Some("eng".to_string()),
                    parsed_release_group: Some("GROUP".to_string()),
                    parser_confidence: episode
                        .map(|_| ReleaseConfidence::High)
                        .unwrap_or(ReleaseConfidence::Low),
                    parser_reason: episode.is_none().then(|| "sample file".to_string()),
                    raw: None,
                    provider_metadata: None,
                },
            )
            .await?;
            file_ids.push(file.release_file_id);
        }
        upsert_release_coverage(
            &database.pool,
            NewAcquisitionReleaseCoverage {
                coverage_id: None,
                release_id: release.release_id,
                release_file_id: Some(file_ids[0]),
                target_id,
                coverage_kind: ReleaseCoverageKind::SeasonPack,
                confidence: ReleaseConfidence::ReviewRequired,
                score: Some(80.0),
                reason: Some("episode request matched a season pack".to_string()),
                state: ReleaseCoverageState::ReviewRequired,
                verified_by: None,
            },
        )
        .await?;
        upsert_release_job(
            &database.pool,
            NewAcquisitionReleaseJob {
                release_job_id: None,
                release_id: release.release_id,
                route_logical_id: DEBRID_DEFAULT_LOGICAL_ID.to_string(),
                provider_id: None,
                download_id: Some("osr4-manual-download".to_string()),
                remote_release_id: Some("osr4-manual-download".to_string()),
                state: ReleaseJobState::Staging,
                state_reason: Some("waiting review".to_string()),
                active: true,
                started_at: Some(Utc::now()),
                completed_at: None,
            },
        )
        .await?;

        approve_release_for_review(
            None,
            &database.pool,
            user_id,
            release.release_id,
            ApproveAcquisitionReleaseRequest {
                selected_release_file_ids: vec![file_ids[0]],
                skipped_release_file_ids: file_ids[1..].to_vec(),
                reason: Some("verified requested episode only".to_string()),
                ..Default::default()
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        let approved_release = get_release(&database.pool, release.release_id)
            .await?
            .expect("release");
        assert_eq!(approved_release.state, AcquisitionReleaseState::Ready);
        let plan = approved_release
            .coverage_plan
            .as_ref()
            .expect("coverage plan");
        assert_eq!(
            plan.pointer("/requestScopeEvidence/requestMode")
                .and_then(JsonValue::as_str),
            Some("one_shot")
        );
        assert_eq!(
            plan.pointer("/manualReview/status")
                .and_then(JsonValue::as_str),
            Some("approved")
        );
        let files = list_release_files(&database.pool, release.release_id).await?;
        let selected = files
            .iter()
            .map(|file| (file.release_file_id, file.selected))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(selected.get(&file_ids[0]), Some(&Some(true)));
        assert_eq!(selected.get(&file_ids[1]), Some(&Some(false)));
        assert_eq!(selected.get(&file_ids[2]), Some(&Some(false)));
        let coverage = list_release_coverage(&database.pool, release.release_id).await?;
        assert_eq!(coverage.len(), 1);
        assert_eq!(coverage[0].target_id, target_id);
        assert_eq!(coverage[0].release_file_id, Some(file_ids[0]));
        assert_eq!(coverage[0].state, ReleaseCoverageState::Selected);
        let target = get_target(&database.pool, target_id)
            .await?
            .expect("target");
        assert_eq!(target.state, AcquisitionTargetState::Submitted);
        assert_eq!(
            target.selected_route_logical_id.as_deref(),
            Some(DEBRID_DEFAULT_LOGICAL_ID)
        );
        let due = list_due_candidate_targets(&database.pool, Utc::now(), 10).await?;
        assert!(
            due.is_empty(),
            "approved scoped target must not be searched again"
        );
        Ok(())
    }

    #[tokio::test]
    async fn manual_approval_prunes_depleted_competing_review_candidate() -> Result<()> {
        let database = setup_db().await?;
        let user_id = Uuid::new_v4();
        let fixture = setup_tv_review_pack(&database).await?;
        let approved_release = get_release(&database.pool, fixture.release_id)
            .await?
            .expect("approved release");
        let competing = setup_competing_review_pack(
            &database,
            approved_release.subscription_id.expect("subscription"),
            &fixture.target_ids,
            "aaaaaaaa",
        )
        .await?;

        approve_release_for_review(
            None,
            &database.pool,
            user_id,
            fixture.release_id,
            ApproveAcquisitionReleaseRequest {
                selected_release_file_ids: fixture.file_ids[0..2].to_vec(),
                skipped_release_file_ids: vec![fixture.file_ids[2]],
                reason: Some("verified exact TV pack files".to_string()),
                ..Default::default()
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        let competing_release = get_release(&database.pool, competing.release_id)
            .await?
            .expect("competing release");
        assert_eq!(competing_release.state, AcquisitionReleaseState::Cancelled);
        assert_eq!(
            competing_release
                .coverage_plan
                .as_ref()
                .and_then(|value| value.pointer("/manualReview/status"))
                .and_then(JsonValue::as_str),
            Some("auto_pruned")
        );
        let competing_coverage =
            list_release_coverage(&database.pool, competing.release_id).await?;
        assert!(
            competing_coverage
                .iter()
                .all(|row| row.state == ReleaseCoverageState::Rejected)
        );
        let competing_jobs = list_release_jobs(&database.pool, competing.release_id).await?;
        assert!(
            competing_jobs
                .iter()
                .all(|job| job.state == ReleaseJobState::Cancelled && !job.active)
        );
        assert_eq!(
            count_acquisition_audit_events(
                &database.pool,
                competing.release_id,
                EVENT_MANUAL_REJECTION
            )
            .await?,
            1
        );
        let review_queue = list_releases(
            &database.pool,
            ReleaseListFilter {
                subscription_id: approved_release.subscription_id,
                state: Some(AcquisitionReleaseState::ReviewRequired),
                limit: Some(10),
            },
        )
        .await?;
        assert!(
            review_queue
                .iter()
                .all(|release| release.release_id != competing.release_id)
        );
        Ok(())
    }

    #[tokio::test]
    async fn manual_approval_only_prunes_overlapping_targets_from_competing_candidate() -> Result<()>
    {
        let database = setup_db().await?;
        let user_id = Uuid::new_v4();
        let fixture = setup_tv_review_pack(&database).await?;
        let approved_release = get_release(&database.pool, fixture.release_id)
            .await?
            .expect("approved release");
        let competing = setup_competing_review_pack(
            &database,
            approved_release.subscription_id.expect("subscription"),
            &fixture.target_ids,
            "bbbbbbbb",
        )
        .await?;

        approve_release_for_review(
            None,
            &database.pool,
            user_id,
            fixture.release_id,
            ApproveAcquisitionReleaseRequest {
                selected_release_file_ids: vec![fixture.file_ids[0]],
                skipped_release_file_ids: fixture.file_ids[1..].to_vec(),
                reason: Some("verified only first episode file".to_string()),
                ..Default::default()
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        let competing_release = get_release(&database.pool, competing.release_id)
            .await?
            .expect("competing release");
        assert_eq!(
            competing_release.state,
            AcquisitionReleaseState::ReviewRequired
        );
        let competing_coverage =
            list_release_coverage(&database.pool, competing.release_id).await?;
        let rejected_targets = competing_coverage
            .iter()
            .filter(|row| row.state == ReleaseCoverageState::Rejected)
            .map(|row| row.target_id)
            .collect::<BTreeSet<_>>();
        assert_eq!(rejected_targets, BTreeSet::from([fixture.target_ids[0]]));
        let remaining_review_targets = competing_coverage
            .iter()
            .filter(|row| row.state == ReleaseCoverageState::ReviewRequired)
            .map(|row| row.target_id)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            remaining_review_targets,
            BTreeSet::from([fixture.target_ids[1]])
        );
        Ok(())
    }

    #[tokio::test]
    async fn review_queue_reconcile_prunes_candidates_for_already_submitted_targets() -> Result<()>
    {
        let database = setup_db().await?;
        let user_id = Uuid::new_v4();
        let fixture = setup_tv_review_pack(&database).await?;
        for target_id in &fixture.target_ids {
            update_target_state(
                &database.pool,
                *target_id,
                AcquisitionTargetStateUpdate {
                    state: AcquisitionTargetState::Submitted,
                    state_reason: Some("covered before queue reconciliation".to_string()),
                    selected_provider_id: None,
                    selected_route_logical_id: Some("acquisition.debrid.default".to_string()),
                    selected_candidate: Some(json!({ "title": "already covered" })),
                    download_id: Some("already-covered-download".to_string()),
                    import_event_id: None,
                    next_search_after: None,
                    increment_search_attempts: false,
                },
            )
            .await?;
        }

        let summary = prune_review_candidates_for_covered_targets(&database.pool, user_id, None)
            .await
            .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        assert_eq!(summary.rejected_coverage_rows, 2);
        assert_eq!(summary.cancelled_releases, 1);
        let release = get_release(&database.pool, fixture.release_id)
            .await?
            .expect("release");
        assert_eq!(release.state, AcquisitionReleaseState::Cancelled);
        let coverage = list_release_coverage(&database.pool, fixture.release_id).await?;
        assert!(
            coverage
                .iter()
                .all(|row| row.state == ReleaseCoverageState::Rejected)
        );
        Ok(())
    }

    #[tokio::test]
    async fn rr7c_anime_manual_mapping_survives_retry_same_release() -> Result<()> {
        let database = setup_db().await?;
        let user_id = Uuid::new_v4();
        let fixture = setup_anime_review_pack(&database).await?;
        approve_release_for_review(
            None,
            &database.pool,
            user_id,
            fixture.release_id,
            ApproveAcquisitionReleaseRequest {
                selected_release_file_ids: vec![fixture.file_ids[0]],
                skipped_release_file_ids: vec![fixture.file_ids[1]],
                mappings: vec![ManualCoverageMappingRequest {
                    target_id: fixture.target_ids[0],
                    release_file_id: Some(fixture.file_ids[0]),
                    coverage_kind: Some(ReleaseCoverageKind::ManualOverride),
                    confidence: Some(ReleaseConfidence::High),
                    score: Some(100.0),
                    reason: Some("manual anime episode mapping".to_string()),
                }],
                reason: Some("verified anime file mapping".to_string()),
                ..Default::default()
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        retry_release_for_review(
            &database.pool,
            user_id,
            fixture.release_id,
            RetryAcquisitionReleaseRequest {
                mode: RetryMode::SameRelease,
                reason: Some("refresh provider state after approval".to_string()),
                next_search_after: None,
                clear_suppression: false,
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        let release = get_release(&database.pool, fixture.release_id)
            .await?
            .expect("release");
        assert_eq!(release.state, AcquisitionReleaseState::Ready);
        let plan = release.coverage_plan.as_ref().expect("coverage plan");
        assert_eq!(
            plan.get("manualReview")
                .and_then(|value| value.get("status"))
                .and_then(JsonValue::as_str),
            Some("approved")
        );
        assert_eq!(
            plan.get("retryPolicy")
                .and_then(|value| value.get("mode"))
                .and_then(JsonValue::as_str),
            Some("same_release")
        );
        let files = list_release_files(&database.pool, fixture.release_id).await?;
        let selected = files
            .iter()
            .map(|file| (file.release_file_id, file.selected))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(selected.get(&fixture.file_ids[0]), Some(&Some(true)));
        assert_eq!(selected.get(&fixture.file_ids[1]), Some(&Some(false)));
        let coverage = list_release_coverage(&database.pool, fixture.release_id).await?;
        assert!(coverage.iter().any(|row| {
            row.target_id == fixture.target_ids[0]
                && row.release_file_id == Some(fixture.file_ids[0])
                && row.coverage_kind == ReleaseCoverageKind::ManualOverride
                && row.state == ReleaseCoverageState::Selected
        }));
        let jobs = list_release_jobs(&database.pool, fixture.release_id).await?;
        assert_eq!(jobs[0].state, ReleaseJobState::Ready);
        assert!(jobs[0].active);
        Ok(())
    }

    #[tokio::test]
    async fn manual_approval_can_map_subset_of_placeholder_review_targets() -> Result<()> {
        let database = setup_db().await?;
        let user_id = Uuid::new_v4();
        let fixture = setup_anime_review_pack(&database).await?;
        let release = get_release(&database.pool, fixture.release_id)
            .await?
            .expect("release");
        let subscription_id = release.subscription_id.expect("subscription");
        let placeholder_targets = upsert_subscription_targets(
            &database.pool,
            subscription_id,
            vec![
                NewAcquisitionTarget {
                    target_key: Some("S01E02".to_string()),
                    media_type: Some(MediaType::Anime),
                    title: Some("Episode 2".to_string()),
                    season_number: Some(1),
                    episode_number: Some(2),
                    absolute_episode_number: Some(2),
                    air_date: None,
                    air_time: None,
                    metadata: None,
                    state: Some(AcquisitionTargetState::Searching),
                    next_search_after: Some(Utc::now()),
                },
                NewAcquisitionTarget {
                    target_key: Some("S01E03".to_string()),
                    media_type: Some(MediaType::Anime),
                    title: Some("Episode 3".to_string()),
                    season_number: Some(1),
                    episode_number: Some(3),
                    absolute_episode_number: Some(3),
                    air_date: None,
                    air_time: None,
                    metadata: None,
                    state: Some(AcquisitionTargetState::Searching),
                    next_search_after: Some(Utc::now()),
                },
            ],
        )
        .await?
        .into_iter()
        .filter(|target| !fixture.target_ids.contains(&target.target_id))
        .collect::<Vec<_>>();
        for target in &placeholder_targets {
            upsert_release_coverage(
                &database.pool,
                NewAcquisitionReleaseCoverage {
                    coverage_id: None,
                    release_id: fixture.release_id,
                    release_file_id: None,
                    target_id: target.target_id,
                    coverage_kind: ReleaseCoverageKind::SeasonPack,
                    confidence: ReleaseConfidence::ReviewRequired,
                    score: Some(60.0),
                    reason: Some("needs manual file mapping".to_string()),
                    state: ReleaseCoverageState::ReviewRequired,
                    verified_by: None,
                },
            )
            .await?;
        }
        let all_target_ids = fixture
            .target_ids
            .iter()
            .copied()
            .chain(placeholder_targets.iter().map(|target| target.target_id))
            .collect::<Vec<_>>();
        let competing =
            setup_competing_review_pack(&database, subscription_id, &all_target_ids, "cccccccc")
                .await?;

        approve_release_for_review(
            None,
            &database.pool,
            user_id,
            fixture.release_id,
            ApproveAcquisitionReleaseRequest {
                selected_release_file_ids: vec![fixture.file_ids[0]],
                skipped_release_file_ids: vec![fixture.file_ids[1]],
                mappings: vec![ManualCoverageMappingRequest {
                    target_id: fixture.target_ids[0],
                    release_file_id: Some(fixture.file_ids[0]),
                    coverage_kind: Some(ReleaseCoverageKind::ManualOverride),
                    confidence: Some(ReleaseConfidence::High),
                    score: Some(100.0),
                    reason: Some("manual anime episode mapping".to_string()),
                }],
                reason: Some("verified one anime episode file".to_string()),
                ..Default::default()
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        let approved_coverage = list_release_coverage(&database.pool, fixture.release_id).await?;
        let selected_targets = approved_coverage
            .iter()
            .filter(|row| row.state == ReleaseCoverageState::Selected)
            .map(|row| row.target_id)
            .collect::<BTreeSet<_>>();
        assert_eq!(selected_targets, BTreeSet::from([fixture.target_ids[0]]));
        for target in &placeholder_targets {
            assert!(
                approved_coverage.iter().any(|row| {
                    row.target_id == target.target_id && row.state == ReleaseCoverageState::Rejected
                }),
                "unmapped target {} should stay unapproved on approved release",
                target.target_key
            );
        }
        let submitted_target = get_target(&database.pool, fixture.target_ids[0])
            .await?
            .expect("submitted target");
        assert_eq!(submitted_target.state, AcquisitionTargetState::Submitted);

        let competing_release = get_release(&database.pool, competing.release_id)
            .await?
            .expect("competing release");
        assert_eq!(
            competing_release.state,
            AcquisitionReleaseState::ReviewRequired
        );
        let competing_coverage =
            list_release_coverage(&database.pool, competing.release_id).await?;
        let competing_rejected = competing_coverage
            .iter()
            .filter(|row| row.state == ReleaseCoverageState::Rejected)
            .map(|row| row.target_id)
            .collect::<BTreeSet<_>>();
        assert_eq!(competing_rejected, BTreeSet::from([fixture.target_ids[0]]));
        let competing_remaining = competing_coverage
            .iter()
            .filter(|row| row.state == ReleaseCoverageState::ReviewRequired)
            .map(|row| row.target_id)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            competing_remaining,
            placeholder_targets
                .iter()
                .map(|target| target.target_id)
                .collect::<BTreeSet<_>>()
        );
        Ok(())
    }

    #[tokio::test]
    async fn manual_approval_rejects_single_anime_episode_file_mapped_to_multiple_targets()
    -> Result<()> {
        let database = setup_db().await?;
        let user_id = Uuid::new_v4();
        let fixture = setup_anime_review_pack(&database).await?;
        let release = get_release(&database.pool, fixture.release_id)
            .await?
            .expect("release");
        upsert_subscription_targets(
            &database.pool,
            release.subscription_id.expect("subscription"),
            vec![NewAcquisitionTarget {
                target_key: Some("S01E02".to_string()),
                media_type: Some(MediaType::Anime),
                title: Some("Episode 2".to_string()),
                season_number: Some(1),
                episode_number: Some(2),
                absolute_episode_number: Some(2),
                air_date: None,
                air_time: None,
                metadata: None,
                state: Some(AcquisitionTargetState::Searching),
                next_search_after: Some(Utc::now()),
            }],
        )
        .await?;
        let extra_target =
            list_subscription_targets(&database.pool, release.subscription_id.unwrap())
                .await?
                .into_iter()
                .find(|target| target.target_key == "S01E02")
                .expect("extra target");
        upsert_release_coverage(
            &database.pool,
            NewAcquisitionReleaseCoverage {
                coverage_id: None,
                release_id: fixture.release_id,
                release_file_id: None,
                target_id: extra_target.target_id,
                coverage_kind: ReleaseCoverageKind::SeasonPack,
                confidence: ReleaseConfidence::ReviewRequired,
                score: Some(60.0),
                reason: Some("needs manual file mapping".to_string()),
                state: ReleaseCoverageState::ReviewRequired,
                verified_by: None,
            },
        )
        .await?;

        let err = approve_release_for_review(
            None,
            &database.pool,
            user_id,
            fixture.release_id,
            ApproveAcquisitionReleaseRequest {
                selected_release_file_ids: vec![fixture.file_ids[0]],
                skipped_release_file_ids: vec![fixture.file_ids[1]],
                mappings: vec![
                    ManualCoverageMappingRequest {
                        target_id: fixture.target_ids[0],
                        release_file_id: Some(fixture.file_ids[0]),
                        coverage_kind: Some(ReleaseCoverageKind::ManualOverride),
                        confidence: Some(ReleaseConfidence::High),
                        score: Some(100.0),
                        reason: Some("manual anime episode mapping".to_string()),
                    },
                    ManualCoverageMappingRequest {
                        target_id: extra_target.target_id,
                        release_file_id: Some(fixture.file_ids[0]),
                        coverage_kind: Some(ReleaseCoverageKind::ManualOverride),
                        confidence: Some(ReleaseConfidence::High),
                        score: Some(100.0),
                        reason: Some("manual anime episode mapping".to_string()),
                    },
                ],
                reason: Some("verified anime file mapping".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect_err("single episode file must not cover multiple anime targets");

        match err {
            ApiError::BadRequest(message) => {
                assert!(message.contains("manual mapping mismatch"), "{message}");
                assert!(message.contains("S01E02"), "{message}");
            }
            other => panic!("expected bad request, got {other:?}"),
        }

        let release = get_release(&database.pool, fixture.release_id)
            .await?
            .expect("release");
        assert_eq!(release.state, AcquisitionReleaseState::ReviewRequired);
        let files = list_release_files(&database.pool, fixture.release_id).await?;
        assert!(files.iter().all(|file| file.selected.is_none()));
        let selected_rows = list_release_coverage(&database.pool, fixture.release_id)
            .await?
            .into_iter()
            .filter(|row| row.state == ReleaseCoverageState::Selected)
            .count();
        assert_eq!(selected_rows, 0);
        Ok(())
    }

    #[tokio::test]
    async fn rr8d_retry_import_resets_blocked_import_without_source_rediscovery() -> Result<()> {
        let database = setup_db().await?;
        let user_id = Uuid::new_v4();
        let (release_id, target_id) = setup_release(&database).await?;
        let run_id = seed_blocked_import_run(
            &database,
            release_id,
            target_id,
            "/tmp/elixir-rr8d/Example.Show.S01E01.mkv",
        )
        .await?;

        retry_release_for_review(
            &database.pool,
            user_id,
            release_id,
            RetryAcquisitionReleaseRequest {
                mode: RetryMode::Import,
                reason: Some("retry local import".to_string()),
                next_search_after: None,
                clear_suppression: false,
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        let release = get_release(&database.pool, release_id)
            .await?
            .expect("release");
        assert_eq!(release.state, AcquisitionReleaseState::Completed);
        let run = get_import_run_by_release_job(
            &database.pool,
            list_release_jobs(&database.pool, release_id).await?[0].release_job_id,
        )
        .await?
        .expect("run");
        assert_eq!(run.import_run_id, run_id);
        assert_eq!(run.state, AcquisitionImportRunState::Pending);
        assert_eq!(run.retry_count, 1);
        let links = list_import_file_links(&database.pool, run.import_run_id).await?;
        assert_eq!(links[0].state, AcquisitionImportFileLinkState::Pending);
        assert!(links[0].mismatch_class.is_none());
        let target = get_target(&database.pool, target_id)
            .await?
            .expect("target");
        assert_eq!(target.state, AcquisitionTargetState::Submitted);
        assert!(target.next_search_after.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn rr8d_retry_verification_invalidates_existing_anime_hash() -> Result<()> {
        let database = setup_db().await?;
        let user_id = Uuid::new_v4();
        let fixture = setup_anime_review_pack(&database).await?;
        let local_path = "/tmp/elixir-rr8d/Example.Anime.S01E01.mkv";
        seed_blocked_import_run(
            &database,
            fixture.release_id,
            fixture.target_ids[0],
            local_path,
        )
        .await?;
        upsert_file_hash(
            &database.pool,
            NewAcquisitionFileHash {
                file_hash_id: None,
                release_file_id: Some(fixture.file_ids[0]),
                local_file_id: Some(format!("release-file:{}", fixture.file_ids[0])),
                file_path: local_path.to_string(),
                size_bytes: 1024,
                mtime_fingerprint: Some("test-mtime".to_string()),
                ed2k: Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string()),
                crc32: Some("bbbbbbbb".to_string()),
                hash_status: AnimeFileHashStatus::Hashed,
                hash_computed_at: Some(Utc::now()),
                hash_invalidated_at: None,
                filename_history: json!([local_path]),
            },
        )
        .await?;

        retry_release_for_review(
            &database.pool,
            user_id,
            fixture.release_id,
            RetryAcquisitionReleaseRequest {
                mode: RetryMode::Verification,
                reason: Some("retry anime identity verification".to_string()),
                next_search_after: None,
                clear_suppression: false,
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;

        let hash = get_file_hash_by_path(&database.pool, local_path)
            .await?
            .expect("hash");
        assert_eq!(hash.hash_status, AnimeFileHashStatus::Invalidated);
        assert!(hash.hash_invalidated_at.is_some());
        let run = get_import_run_by_release_job(
            &database.pool,
            list_release_jobs(&database.pool, fixture.release_id).await?[0].release_job_id,
        )
        .await?
        .expect("run");
        assert_eq!(run.state, AcquisitionImportRunState::Pending);
        Ok(())
    }

    #[tokio::test]
    async fn reject_cancels_without_deleting_downloader_data() -> Result<()> {
        let database = setup_db().await?;
        let user_id = Uuid::new_v4();
        let (release_id, target_id) = setup_release(&database).await?;
        let run_id = seed_blocked_import_run(
            &database,
            release_id,
            target_id,
            "/tmp/elixir-amr4/Example.Show.S01E01.mkv",
        )
        .await?;
        reject_release_for_review(
            &database.pool,
            user_id,
            release_id,
            RejectAcquisitionReleaseRequest {
                reason: "wrong season".to_string(),
                note: Some("manual check".to_string()),
                target_policy: RejectTargetPolicy::Blocked,
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        let release = get_release(&database.pool, release_id)
            .await?
            .expect("release");
        assert_eq!(release.state, AcquisitionReleaseState::Cancelled);
        assert_eq!(release.download_id.as_deref(), Some("test-download"));
        let jobs = list_release_jobs(&database.pool, release_id).await?;
        assert_eq!(jobs[0].state, ReleaseJobState::Cancelled);
        assert!(!jobs[0].active);
        let target = get_target(&database.pool, target_id)
            .await?
            .expect("target");
        assert_eq!(target.state, AcquisitionTargetState::Blocked);
        let run_state: String =
            sqlx::query_scalar("SELECT state FROM acquisition_import_runs WHERE import_run_id = ?")
                .bind(run_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(run_state, AcquisitionImportRunState::Cancelled.as_str());
        let links = list_import_file_links(&database.pool, run_id).await?;
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].state, AcquisitionImportFileLinkState::Skipped);
        assert_eq!(
            count_acquisition_audit_events(&database.pool, release_id, EVENT_MANUAL_REJECTION)
                .await?,
            1
        );
        Ok(())
    }

    #[tokio::test]
    async fn retry_source_discovery_requeues_targets_and_keeps_release_terminal() -> Result<()> {
        let database = setup_db().await?;
        let user_id = Uuid::new_v4();
        let (release_id, target_id) = setup_release(&database).await?;
        retry_release_for_review(
            &database.pool,
            user_id,
            release_id,
            RetryAcquisitionReleaseRequest {
                mode: RetryMode::SourceDiscovery,
                reason: Some("try another release".to_string()),
                next_search_after: None,
                clear_suppression: false,
            },
        )
        .await
        .map_err(|err| anyhow::anyhow!("{err:?}"))?;
        let release = get_release(&database.pool, release_id)
            .await?
            .expect("release");
        assert_eq!(release.state, AcquisitionReleaseState::Cancelled);
        let target = get_target(&database.pool, target_id)
            .await?
            .expect("target");
        assert_eq!(target.state, AcquisitionTargetState::Pending);
        assert!(target.next_search_after.is_some());
        Ok(())
    }
}
