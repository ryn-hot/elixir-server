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
        imports::{
            AcquisitionImportFileLink, AcquisitionImportRun, AcquisitionImportRunState,
            list_import_file_links, list_import_file_links_by_release, list_import_runs_by_release,
            reset_import_runs_for_release,
        },
        release_resolution::{
            models::{
                AcquisitionAnimeIdentityMismatch, AcquisitionAnimeMatchAttempt,
                AcquisitionFileHash, AcquisitionRelease, AcquisitionReleaseCoverage,
                AcquisitionReleaseFile, AcquisitionReleaseJob, AcquisitionReleaseState,
                NewAcquisitionReleaseCoverage, ReleaseConfidence, ReleaseCoverageKind,
                ReleaseCoverageState, ReleaseJobState, ReleaseJobStateUpdate,
            },
            store::{
                ReleaseListFilter, get_file_hash_by_path, get_release,
                list_anime_identity_mismatches_by_release, list_anime_match_attempts_by_release,
                list_release_coverage, list_release_files, list_release_jobs, list_releases,
                update_release_coverage_review_state, update_release_file_selection,
                update_release_job_state, update_release_review_state, upsert_release_coverage,
            },
        },
        subscriptions::{
            AcquisitionSubscription, AcquisitionTarget, AcquisitionTargetState,
            AcquisitionTargetStateUpdate, clear_target_next_search_after, get_subscription,
            get_target, list_subscription_targets, update_target_state,
        },
    },
    http::{
        auth::CurrentUser,
        error::{ApiError, ApiResult},
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
    #[default]
    Blocked,
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
    selected_candidate: Option<JsonValue>,
    coverage_plan: Option<JsonValue>,
    scheduler_dispatch: Option<JsonValue>,
    submission_result: Option<JsonValue>,
    priority_policy: Option<JsonValue>,
    manual_review: Option<JsonValue>,
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
    _user: CurrentUser,
    State(state): State<AppState>,
    Query(query): Query<ListAcquisitionReleasesQuery>,
) -> ApiResult<Json<AcquisitionReleaseListResponse>> {
    let releases = list_releases(
        &state.db_pool,
        ReleaseListFilter {
            subscription_id: query.subscription_id,
            state: parse_optional_release_state(query.state.as_deref())?,
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
    _user: CurrentUser,
    State(state): State<AppState>,
    Path(release_id): Path<Uuid>,
) -> ApiResult<Json<AcquisitionReleaseDetailResponse>> {
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
    approve_release_for_review(&state.db_pool, user.user_id, release_id, request).await?;
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
    let files = list_release_files(pool, release_id)
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
    let selected_coverage_state = selected_coverage_state(&release);
    for row in &coverage {
        let state = match row.release_file_id {
            Some(release_file_id)
                if file_selection
                    .skipped_release_file_ids
                    .contains(&release_file_id) =>
            {
                ReleaseCoverageState::Rejected
            }
            Some(release_file_id)
                if file_selection.explicit
                    && !file_selection
                        .selected_release_file_ids
                        .contains(&release_file_id) =>
            {
                ReleaseCoverageState::Rejected
            }
            _ => selected_coverage_state,
        };
        let reason = if state == ReleaseCoverageState::Rejected {
            Some("Skipped by manual acquisition review.".to_string())
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
        upsert_release_coverage(
            pool,
            NewAcquisitionReleaseCoverage {
                coverage_id: None,
                release_id,
                release_file_id: mapping.release_file_id,
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

    let policy = approval_policy_json(
        &release,
        &files,
        &file_selection,
        user_id,
        request.reason.as_deref(),
        request.note.as_deref(),
    );
    let merged_plan = merge_review_policy(release.coverage_plan.as_ref(), policy);
    update_release_review_state(
        pool,
        release_id,
        AcquisitionReleaseState::Ready,
        Some("Approved by acquisition release review.".to_string()),
        Some(merged_plan),
    )
    .await
    .map_err(ApiError::from)?;
    mark_approved_release_targets_submitted(
        pool,
        &release,
        "Approved by acquisition release review.",
    )
    .await?;
    for job in list_release_jobs(pool, release_id)
        .await
        .map_err(ApiError::from)?
    {
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
                selected_provider_id: release.source_provider_id.or(release.selected_provider_id),
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
    for job in list_release_jobs(pool, release_id)
        .await
        .map_err(ApiError::from)?
    {
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
            "nextSearchAfter": request.next_search_after
        }
    });
    let merged_plan = merge_review_policy(release.coverage_plan.as_ref(), retry_policy);
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
                                .source_provider_id
                                .or(release.selected_provider_id),
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
    })
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
            "userApprovedImportOverride": true,
            "manualRemapApproved": !selection.selected_release_file_ids.is_empty(),
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
    use crate::{
        acquisition::{
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
                store::{
                    upsert_file_hash, upsert_release, upsert_release_file, upsert_release_job,
                },
            },
            subscriptions::{
                AcquisitionRoutePolicy, NewAcquisitionSubscription, NewAcquisitionTarget,
                create_subscription, list_due_candidate_targets, upsert_subscription_targets,
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

    async fn setup_release(database: &Database) -> Result<(Uuid, Uuid)> {
        let subscription = create_subscription(
            &database.pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Series,
                title: "Example Show".to_string(),
                year: Some(2024),
                external_ids: None,
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

    async fn setup_anime_review_pack(database: &Database) -> Result<ReviewPackFixture> {
        let subscription = create_subscription(
            &database.pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Anime,
                title: "Example Anime".to_string(),
                year: Some(2024),
                external_ids: None,
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
    async fn rr7c_tv_pack_approval_submits_targets_and_preserves_exact_files() -> Result<()> {
        let database = setup_db().await?;
        let user_id = Uuid::new_v4();
        let fixture = setup_tv_review_pack(&database).await?;
        approve_release_for_review(
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
    async fn rr7c_anime_manual_mapping_survives_retry_same_release() -> Result<()> {
        let database = setup_db().await?;
        let user_id = Uuid::new_v4();
        let fixture = setup_anime_review_pack(&database).await?;
        approve_release_for_review(
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
