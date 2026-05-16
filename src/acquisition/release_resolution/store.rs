use std::str::FromStr;

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, NaiveDateTime, Utc};
use serde_json::Value as JsonValue;
use sqlx::{AnyPool, Row, TypeInfo, Value as SqlxValue, ValueRef, any::AnyRow};
use uuid::Uuid;

use crate::{acquisition::release_resolution::models::*, db::models::MediaType};

#[derive(Debug, Clone, Default)]
pub struct ReleaseListFilter {
    pub subscription_id: Option<Uuid>,
    pub state: Option<AcquisitionReleaseState>,
    pub limit: Option<i64>,
}

pub async fn upsert_release(
    pool: &AnyPool,
    data: NewAcquisitionRelease,
) -> Result<AcquisitionRelease> {
    validate_release_input(&data)?;
    let selected_candidate_json = json_to_string(data.selected_candidate.as_ref())?;
    let coverage_plan_json = json_to_string(data.coverage_plan.as_ref())?;
    let existing = if let Some(release_id) = data.release_id {
        get_release(pool, release_id).await?
    } else {
        get_release_by_fingerprint(
            pool,
            &data.owner_id,
            &data.source_extension_id,
            &data.fingerprint,
        )
        .await?
    };

    let release_id = existing
        .as_ref()
        .map(|release| release.release_id)
        .or(data.release_id)
        .unwrap_or_else(Uuid::new_v4);

    if existing.is_some() {
        sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_releases
             SET subscription_id = ?,
                 source_provider_id = ?,
                 source_extension_id = ?,
                 owner_id = ?,
                 media_type = ?,
                 title = ?,
                 release_title = ?,
                 source = ?,
                 source_kind = ?,
                 info_hash = ?,
                 fingerprint = ?,
                 release_kind = ?,
                 resolver_kind = ?,
                 resolver_version = ?,
                 confidence = ?,
                 score = ?,
                 selected_route_logical_id = ?,
                 selected_provider_id = ?,
                 download_id = ?,
                 remote_release_id = ?,
                 state = ?,
                 state_reason = ?,
                 selected_candidate_json = ?,
                 coverage_plan_json = ?,
                 updated_at = CURRENT_TIMESTAMP
             WHERE release_id = ?",
        )
        .bind(data.subscription_id.map(|value| value.to_string()))
        .bind(data.source_provider_id.map(|value| value.to_string()))
        .bind(data.source_extension_id.trim())
        .bind(data.owner_id.trim())
        .bind(data.media_type.as_str())
        .bind(data.title.trim())
        .bind(data.release_title.trim())
        .bind(data.source.trim())
        .bind(data.source_kind.trim().to_ascii_lowercase())
        .bind(data.info_hash.as_deref())
        .bind(data.fingerprint.trim())
        .bind(data.release_kind.as_str())
        .bind(data.resolver_kind.as_str())
        .bind(data.resolver_version.trim())
        .bind(data.confidence.as_str())
        .bind(data.score)
        .bind(data.selected_route_logical_id.as_deref())
        .bind(data.selected_provider_id.map(|value| value.to_string()))
        .bind(data.download_id.as_deref())
        .bind(data.remote_release_id.as_deref())
        .bind(data.state.as_str())
        .bind(data.state_reason.as_deref())
        .bind(selected_candidate_json.as_deref())
        .bind(coverage_plan_json.as_deref())
        .bind(release_id.to_string())
        .execute(pool)
        .await
        .context("updating acquisition release")?;
    } else {
        sqlx::query::<sqlx::Any>(
            "INSERT INTO acquisition_releases (
                release_id,
                subscription_id,
                source_provider_id,
                source_extension_id,
                owner_id,
                media_type,
                title,
                release_title,
                source,
                source_kind,
                info_hash,
                fingerprint,
                release_kind,
                resolver_kind,
                resolver_version,
                confidence,
                score,
                selected_route_logical_id,
                selected_provider_id,
                download_id,
                remote_release_id,
                state,
                state_reason,
                selected_candidate_json,
                coverage_plan_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(release_id.to_string())
        .bind(data.subscription_id.map(|value| value.to_string()))
        .bind(data.source_provider_id.map(|value| value.to_string()))
        .bind(data.source_extension_id.trim())
        .bind(data.owner_id.trim())
        .bind(data.media_type.as_str())
        .bind(data.title.trim())
        .bind(data.release_title.trim())
        .bind(data.source.trim())
        .bind(data.source_kind.trim().to_ascii_lowercase())
        .bind(data.info_hash.as_deref())
        .bind(data.fingerprint.trim())
        .bind(data.release_kind.as_str())
        .bind(data.resolver_kind.as_str())
        .bind(data.resolver_version.trim())
        .bind(data.confidence.as_str())
        .bind(data.score)
        .bind(data.selected_route_logical_id.as_deref())
        .bind(data.selected_provider_id.map(|value| value.to_string()))
        .bind(data.download_id.as_deref())
        .bind(data.remote_release_id.as_deref())
        .bind(data.state.as_str())
        .bind(data.state_reason.as_deref())
        .bind(selected_candidate_json.as_deref())
        .bind(coverage_plan_json.as_deref())
        .execute(pool)
        .await
        .context("creating acquisition release")?;
    }

    get_release(pool, release_id)
        .await?
        .ok_or_else(|| anyhow!("upserted acquisition release was not readable"))
}

pub async fn get_release(pool: &AnyPool, release_id: Uuid) -> Result<Option<AcquisitionRelease>> {
    let row = sqlx::query(RELEASE_SELECT_BY_ID)
        .bind(release_id.to_string())
        .fetch_optional(pool)
        .await?;
    row.map(|row| map_release(&row)).transpose()
}

pub async fn get_release_by_fingerprint(
    pool: &AnyPool,
    owner_id: &str,
    source_extension_id: &str,
    fingerprint: &str,
) -> Result<Option<AcquisitionRelease>> {
    let row = sqlx::query(RELEASE_SELECT_BY_FINGERPRINT)
        .bind(owner_id.trim())
        .bind(source_extension_id.trim())
        .bind(fingerprint.trim())
        .fetch_optional(pool)
        .await?;
    row.map(|row| map_release(&row)).transpose()
}

pub async fn get_release_by_download_id(
    pool: &AnyPool,
    download_id: &str,
) -> Result<Option<AcquisitionRelease>> {
    let row = sqlx::query(RELEASE_SELECT_BY_DOWNLOAD_ID)
        .bind(download_id.trim())
        .fetch_optional(pool)
        .await?;
    row.map(|row| map_release(&row)).transpose()
}

pub async fn list_releases(
    pool: &AnyPool,
    filter: ReleaseListFilter,
) -> Result<Vec<AcquisitionRelease>> {
    let limit = filter.limit.unwrap_or(100).clamp(1, 500);
    let rows = match (filter.subscription_id, filter.state) {
        (Some(subscription_id), Some(state)) => {
            sqlx::query(RELEASE_SELECT_BY_SUBSCRIPTION_AND_STATE)
                .bind(subscription_id.to_string())
                .bind(state.as_str())
                .bind(limit)
                .fetch_all(pool)
                .await?
        }
        (Some(subscription_id), None) => {
            sqlx::query(RELEASE_SELECT_BY_SUBSCRIPTION)
                .bind(subscription_id.to_string())
                .bind(limit)
                .fetch_all(pool)
                .await?
        }
        (None, Some(state)) => {
            sqlx::query(RELEASE_SELECT_BY_STATE)
                .bind(state.as_str())
                .bind(limit)
                .fetch_all(pool)
                .await?
        }
        (None, None) => {
            sqlx::query(RELEASE_SELECT_RECENT)
                .bind(limit)
                .fetch_all(pool)
                .await?
        }
    };
    rows.into_iter().map(|row| map_release(&row)).collect()
}

pub async fn update_release_review_state(
    pool: &AnyPool,
    release_id: Uuid,
    state: AcquisitionReleaseState,
    state_reason: Option<String>,
    coverage_plan: Option<JsonValue>,
) -> Result<Option<AcquisitionRelease>> {
    let coverage_plan_json = json_to_string(coverage_plan.as_ref())?;
    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_releases
         SET state = ?,
             state_reason = ?,
             coverage_plan_json = ?,
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = ?",
    )
    .bind(state.as_str())
    .bind(state_reason.as_deref())
    .bind(coverage_plan_json.as_deref())
    .bind(release_id.to_string())
    .execute(pool)
    .await
    .context("updating acquisition release review state")?;

    get_release(pool, release_id).await
}

pub async fn upsert_release_file(
    pool: &AnyPool,
    data: NewAcquisitionReleaseFile,
) -> Result<AcquisitionReleaseFile> {
    validate_release_file_input(&data)?;
    let raw_json = json_to_string(data.raw.as_ref())?;
    let provider_metadata_json = json_to_string(data.provider_metadata.as_ref())?;
    let existing = find_release_file(pool, &data).await?;
    let release_file_id = existing
        .as_ref()
        .map(|file| file.release_file_id)
        .or(data.release_file_id)
        .unwrap_or_else(Uuid::new_v4);
    let basename = data
        .basename
        .clone()
        .unwrap_or_else(|| basename_from_path(&data.path));

    if existing.is_some() {
        sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_release_files
             SET release_id = ?,
                 file_index = ?,
                 file_id = ?,
                 provider_file_id = ?,
                 path = ?,
                 basename = ?,
                 size_bytes = ?,
                 selectable = ?,
                 selected = ?,
                 parsed_title = ?,
                 parsed_season_number = ?,
                 parsed_episode_number = ?,
                 parsed_episode_end_number = ?,
                 parsed_absolute_episode_number = ?,
                 parsed_absolute_episode_end_number = ?,
                 parsed_air_date = ?,
                 parsed_quality = ?,
                 parsed_language = ?,
                 parsed_release_group = ?,
                 parser_confidence = ?,
                 parser_reason = ?,
                 raw_json = ?,
                 provider_metadata_json = ?,
                 updated_at = CURRENT_TIMESTAMP
             WHERE release_file_id = ?",
        )
        .bind(data.release_id.to_string())
        .bind(data.file_index)
        .bind(data.file_id.as_deref())
        .bind(data.provider_file_id.as_deref())
        .bind(data.path.trim())
        .bind(basename)
        .bind(data.size_bytes)
        .bind(data.selectable)
        .bind(data.selected)
        .bind(data.parsed_title.as_deref())
        .bind(data.parsed_season_number)
        .bind(data.parsed_episode_number)
        .bind(data.parsed_episode_end_number)
        .bind(data.parsed_absolute_episode_number)
        .bind(data.parsed_absolute_episode_end_number)
        .bind(data.parsed_air_date.as_deref())
        .bind(data.parsed_quality.as_deref())
        .bind(data.parsed_language.as_deref())
        .bind(data.parsed_release_group.as_deref())
        .bind(data.parser_confidence.as_str())
        .bind(data.parser_reason.as_deref())
        .bind(raw_json.as_deref())
        .bind(provider_metadata_json.as_deref())
        .bind(release_file_id.to_string())
        .execute(pool)
        .await
        .context("updating acquisition release file")?;
    } else {
        sqlx::query::<sqlx::Any>(
            "INSERT INTO acquisition_release_files (
                release_file_id,
                release_id,
                file_index,
                file_id,
                provider_file_id,
                path,
                basename,
                size_bytes,
                selectable,
                selected,
                parsed_title,
                parsed_season_number,
                parsed_episode_number,
                parsed_episode_end_number,
                parsed_absolute_episode_number,
                parsed_absolute_episode_end_number,
                parsed_air_date,
                parsed_quality,
                parsed_language,
                parsed_release_group,
                parser_confidence,
                parser_reason,
                raw_json,
                provider_metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(release_file_id.to_string())
        .bind(data.release_id.to_string())
        .bind(data.file_index)
        .bind(data.file_id.as_deref())
        .bind(data.provider_file_id.as_deref())
        .bind(data.path.trim())
        .bind(basename)
        .bind(data.size_bytes)
        .bind(data.selectable)
        .bind(data.selected)
        .bind(data.parsed_title.as_deref())
        .bind(data.parsed_season_number)
        .bind(data.parsed_episode_number)
        .bind(data.parsed_episode_end_number)
        .bind(data.parsed_absolute_episode_number)
        .bind(data.parsed_absolute_episode_end_number)
        .bind(data.parsed_air_date.as_deref())
        .bind(data.parsed_quality.as_deref())
        .bind(data.parsed_language.as_deref())
        .bind(data.parsed_release_group.as_deref())
        .bind(data.parser_confidence.as_str())
        .bind(data.parser_reason.as_deref())
        .bind(raw_json.as_deref())
        .bind(provider_metadata_json.as_deref())
        .execute(pool)
        .await
        .context("creating acquisition release file")?;
    }

    get_release_file(pool, release_file_id)
        .await?
        .ok_or_else(|| anyhow!("upserted acquisition release file was not readable"))
}

pub async fn list_release_files(
    pool: &AnyPool,
    release_id: Uuid,
) -> Result<Vec<AcquisitionReleaseFile>> {
    let rows = sqlx::query(RELEASE_FILE_SELECT_BY_RELEASE)
        .bind(release_id.to_string())
        .fetch_all(pool)
        .await?;
    rows.into_iter().map(|row| map_release_file(&row)).collect()
}

pub async fn update_release_file_selection(
    pool: &AnyPool,
    release_file_id: Uuid,
    selected: Option<bool>,
) -> Result<Option<AcquisitionReleaseFile>> {
    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_release_files
         SET selected = ?,
             updated_at = CURRENT_TIMESTAMP
         WHERE release_file_id = ?",
    )
    .bind(selected)
    .bind(release_file_id.to_string())
    .execute(pool)
    .await
    .context("updating acquisition release file selection")?;

    get_release_file(pool, release_file_id).await
}

pub async fn upsert_release_coverage(
    pool: &AnyPool,
    data: NewAcquisitionReleaseCoverage,
) -> Result<AcquisitionReleaseCoverage> {
    let existing = find_release_coverage(pool, &data).await?;
    let coverage_id = existing
        .as_ref()
        .map(|coverage| coverage.coverage_id)
        .or(data.coverage_id)
        .unwrap_or_else(Uuid::new_v4);

    if existing.is_some() {
        sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_release_coverage
             SET release_id = ?,
                 release_file_id = ?,
                 target_id = ?,
                 coverage_kind = ?,
                 confidence = ?,
                 score = ?,
                 reason = ?,
                 state = ?,
                 verified_by = ?,
                 updated_at = CURRENT_TIMESTAMP
             WHERE coverage_id = ?",
        )
        .bind(data.release_id.to_string())
        .bind(data.release_file_id.map(|value| value.to_string()))
        .bind(data.target_id.to_string())
        .bind(data.coverage_kind.as_str())
        .bind(data.confidence.as_str())
        .bind(data.score)
        .bind(data.reason.as_deref())
        .bind(data.state.as_str())
        .bind(data.verified_by.as_deref())
        .bind(coverage_id.to_string())
        .execute(pool)
        .await
        .context("updating acquisition release coverage")?;
    } else {
        sqlx::query::<sqlx::Any>(
            "INSERT INTO acquisition_release_coverage (
                coverage_id,
                release_id,
                release_file_id,
                target_id,
                coverage_kind,
                confidence,
                score,
                reason,
                state,
                verified_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(coverage_id.to_string())
        .bind(data.release_id.to_string())
        .bind(data.release_file_id.map(|value| value.to_string()))
        .bind(data.target_id.to_string())
        .bind(data.coverage_kind.as_str())
        .bind(data.confidence.as_str())
        .bind(data.score)
        .bind(data.reason.as_deref())
        .bind(data.state.as_str())
        .bind(data.verified_by.as_deref())
        .execute(pool)
        .await
        .context("creating acquisition release coverage")?;
    }

    get_release_coverage(pool, coverage_id)
        .await?
        .ok_or_else(|| anyhow!("upserted acquisition release coverage was not readable"))
}

pub async fn list_release_coverage(
    pool: &AnyPool,
    release_id: Uuid,
) -> Result<Vec<AcquisitionReleaseCoverage>> {
    let rows = sqlx::query(RELEASE_COVERAGE_SELECT_BY_RELEASE)
        .bind(release_id.to_string())
        .fetch_all(pool)
        .await?;
    rows.into_iter()
        .map(|row| map_release_coverage(&row))
        .collect()
}

pub async fn update_release_coverage_review_state(
    pool: &AnyPool,
    coverage_id: Uuid,
    state: ReleaseCoverageState,
    reason: Option<String>,
    verified_by: Option<String>,
) -> Result<Option<AcquisitionReleaseCoverage>> {
    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_release_coverage
         SET state = ?,
             reason = ?,
             verified_by = ?,
             updated_at = CURRENT_TIMESTAMP
         WHERE coverage_id = ?",
    )
    .bind(state.as_str())
    .bind(reason.as_deref())
    .bind(verified_by.as_deref())
    .bind(coverage_id.to_string())
    .execute(pool)
    .await
    .context("updating acquisition release coverage review state")?;

    get_release_coverage(pool, coverage_id).await
}

pub async fn upsert_release_job(
    pool: &AnyPool,
    data: NewAcquisitionReleaseJob,
) -> Result<AcquisitionReleaseJob> {
    validate_release_job_input(&data)?;
    let existing = find_release_job(pool, &data).await?;
    let release_job_id = existing
        .as_ref()
        .map(|job| job.release_job_id)
        .or(data.release_job_id)
        .unwrap_or_else(Uuid::new_v4);

    if existing.is_some() {
        sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_release_jobs
             SET release_id = ?,
                 route_logical_id = ?,
                 provider_id = ?,
                 download_id = ?,
                 remote_release_id = ?,
                 state = ?,
                 state_reason = ?,
                 active = ?,
                 started_at = ?,
                 completed_at = ?,
                 updated_at = CURRENT_TIMESTAMP
             WHERE release_job_id = ?",
        )
        .bind(data.release_id.to_string())
        .bind(data.route_logical_id.trim())
        .bind(data.provider_id.map(|value| value.to_string()))
        .bind(data.download_id.as_deref())
        .bind(data.remote_release_id.as_deref())
        .bind(data.state.as_str())
        .bind(data.state_reason.as_deref())
        .bind(data.active)
        .bind(data.started_at.map(db_datetime_string))
        .bind(data.completed_at.map(db_datetime_string))
        .bind(release_job_id.to_string())
        .execute(pool)
        .await
        .context("updating acquisition release job")?;
    } else {
        sqlx::query::<sqlx::Any>(
            "INSERT INTO acquisition_release_jobs (
                release_job_id,
                release_id,
                route_logical_id,
                provider_id,
                download_id,
                remote_release_id,
                state,
                state_reason,
                active,
                started_at,
                completed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(release_job_id.to_string())
        .bind(data.release_id.to_string())
        .bind(data.route_logical_id.trim())
        .bind(data.provider_id.map(|value| value.to_string()))
        .bind(data.download_id.as_deref())
        .bind(data.remote_release_id.as_deref())
        .bind(data.state.as_str())
        .bind(data.state_reason.as_deref())
        .bind(data.active)
        .bind(data.started_at.map(db_datetime_string))
        .bind(data.completed_at.map(db_datetime_string))
        .execute(pool)
        .await
        .context("creating acquisition release job")?;
    }

    get_release_job(pool, release_job_id)
        .await?
        .ok_or_else(|| anyhow!("upserted acquisition release job was not readable"))
}

pub async fn update_release_job_state(
    pool: &AnyPool,
    release_job_id: Uuid,
    update: ReleaseJobStateUpdate,
) -> Result<Option<AcquisitionReleaseJob>> {
    let Some(existing) = get_release_job(pool, release_job_id).await? else {
        return Ok(None);
    };
    let completed_at = update.completed_at.or(existing.completed_at);
    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_release_jobs
         SET state = ?,
             state_reason = ?,
             active = ?,
             download_id = ?,
             remote_release_id = ?,
             completed_at = ?,
             updated_at = CURRENT_TIMESTAMP
         WHERE release_job_id = ?",
    )
    .bind(update.state.as_str())
    .bind(update.state_reason.or(existing.state_reason))
    .bind(update.active.unwrap_or(existing.active))
    .bind(update.download_id.or(existing.download_id))
    .bind(update.remote_release_id.or(existing.remote_release_id))
    .bind(completed_at.map(db_datetime_string))
    .bind(release_job_id.to_string())
    .execute(pool)
    .await
    .context("updating acquisition release job state")?;

    get_release_job(pool, release_job_id).await
}

pub async fn list_release_jobs(
    pool: &AnyPool,
    release_id: Uuid,
) -> Result<Vec<AcquisitionReleaseJob>> {
    let rows = sqlx::query(RELEASE_JOB_SELECT_BY_RELEASE)
        .bind(release_id.to_string())
        .fetch_all(pool)
        .await?;
    rows.into_iter().map(|row| map_release_job(&row)).collect()
}

pub async fn count_active_release_jobs_by_route(
    pool: &AnyPool,
    route_logical_id: &str,
) -> Result<i64> {
    let count = sqlx::query_scalar::<sqlx::Any, i64>(
        "SELECT COUNT(*)
         FROM acquisition_release_jobs
         WHERE active = 1
           AND route_logical_id = ?
           AND state IN ('staging', 'ready', 'submitted', 'downloading', 'materializing')",
    )
    .bind(route_logical_id.trim())
    .fetch_one(pool)
    .await
    .context("counting active acquisition release jobs by route")?;
    Ok(count)
}

pub async fn count_active_release_jobs(pool: &AnyPool) -> Result<i64> {
    let count = sqlx::query_scalar::<sqlx::Any, i64>(
        "SELECT COUNT(*)
         FROM acquisition_release_jobs
         WHERE active = 1
           AND state IN ('staging', 'ready', 'submitted', 'downloading', 'materializing')",
    )
    .fetch_one(pool)
    .await
    .context("counting active acquisition release jobs")?;
    Ok(count)
}

pub async fn count_active_release_jobs_by_subscription(
    pool: &AnyPool,
    subscription_id: Uuid,
) -> Result<i64> {
    let count = sqlx::query_scalar::<sqlx::Any, i64>(
        "SELECT COUNT(*)
         FROM acquisition_release_jobs j
         JOIN acquisition_releases r ON r.release_id = j.release_id
         WHERE j.active = 1
           AND r.subscription_id = ?
           AND j.state IN ('staging', 'ready', 'submitted', 'downloading', 'materializing')",
    )
    .bind(subscription_id.to_string())
    .fetch_one(pool)
    .await
    .context("counting active acquisition release jobs by subscription")?;
    Ok(count)
}

pub async fn count_active_release_jobs_by_subscription_route(
    pool: &AnyPool,
    subscription_id: Uuid,
    route_logical_id: &str,
) -> Result<i64> {
    let count = sqlx::query_scalar::<sqlx::Any, i64>(
        "SELECT COUNT(*)
         FROM acquisition_release_jobs j
         JOIN acquisition_releases r ON r.release_id = j.release_id
         WHERE j.active = 1
           AND j.route_logical_id = ?
           AND r.subscription_id = ?
           AND j.state IN ('staging', 'ready', 'submitted', 'downloading', 'materializing')",
    )
    .bind(route_logical_id.trim())
    .bind(subscription_id.to_string())
    .fetch_one(pool)
    .await
    .context("counting active acquisition release jobs by subscription route")?;
    Ok(count)
}

pub async fn count_stale_active_release_jobs(
    pool: &AnyPool,
    stale_before: DateTime<Utc>,
) -> Result<i64> {
    let count = sqlx::query_scalar::<sqlx::Any, i64>(
        "SELECT COUNT(*)
         FROM acquisition_release_jobs
         WHERE active = 1
           AND state IN ('staging', 'ready', 'submitted', 'downloading', 'materializing')
           AND updated_at <= ?",
    )
    .bind(db_datetime_string(stale_before))
    .fetch_one(pool)
    .await
    .context("counting stale active acquisition release jobs")?;
    Ok(count)
}

pub async fn upsert_anime_graph_snapshot(
    pool: &AnyPool,
    data: NewAcquisitionAnimeGraphSnapshot,
) -> Result<AcquisitionAnimeGraphSnapshot> {
    validate_anime_graph_snapshot_input(&data)?;
    let graph_json = json_to_required_string(&data.graph, "anime graph")?;
    let aliases_json = json_to_required_string(&data.aliases, "anime aliases")?;
    let existing = find_anime_graph_snapshot(pool, &data).await?;
    let graph_snapshot_id = existing
        .as_ref()
        .map(|snapshot| snapshot.graph_snapshot_id)
        .or(data.graph_snapshot_id)
        .unwrap_or_else(Uuid::new_v4);

    if existing.is_some() {
        sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_anime_graph_snapshots
             SET subscription_id = ?,
                 owner_id = ?,
                 media_type = ?,
                 anilist_root_id = ?,
                 anilist_season_id = ?,
                 anilist_status = ?,
                 anilist_next_airing_at = ?,
                 tvdb_series_id = ?,
                 anidb_anime_id = ?,
                 fingerprint = ?,
                 graph_json = ?,
                 aliases_json = ?,
                 updated_at = CURRENT_TIMESTAMP
             WHERE graph_snapshot_id = ?",
        )
        .bind(data.subscription_id.map(|value| value.to_string()))
        .bind(data.owner_id.trim())
        .bind(data.media_type.as_str())
        .bind(data.anilist_root_id)
        .bind(data.anilist_season_id)
        .bind(data.anilist_status.as_deref())
        .bind(data.anilist_next_airing_at.map(db_datetime_string))
        .bind(data.tvdb_series_id)
        .bind(data.anidb_anime_id)
        .bind(data.fingerprint.trim())
        .bind(graph_json)
        .bind(aliases_json)
        .bind(graph_snapshot_id.to_string())
        .execute(pool)
        .await
        .context("updating anime graph snapshot")?;
    } else {
        sqlx::query::<sqlx::Any>(
            "INSERT INTO acquisition_anime_graph_snapshots (
                graph_snapshot_id,
                subscription_id,
                owner_id,
                media_type,
                anilist_root_id,
                anilist_season_id,
                anilist_status,
                anilist_next_airing_at,
                tvdb_series_id,
                anidb_anime_id,
                fingerprint,
                graph_json,
                aliases_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(graph_snapshot_id.to_string())
        .bind(data.subscription_id.map(|value| value.to_string()))
        .bind(data.owner_id.trim())
        .bind(data.media_type.as_str())
        .bind(data.anilist_root_id)
        .bind(data.anilist_season_id)
        .bind(data.anilist_status.as_deref())
        .bind(data.anilist_next_airing_at.map(db_datetime_string))
        .bind(data.tvdb_series_id)
        .bind(data.anidb_anime_id)
        .bind(data.fingerprint.trim())
        .bind(graph_json)
        .bind(aliases_json)
        .execute(pool)
        .await
        .context("creating anime graph snapshot")?;
    }

    get_anime_graph_snapshot(pool, graph_snapshot_id)
        .await?
        .ok_or_else(|| anyhow!("upserted anime graph snapshot was not readable"))
}

pub async fn get_anime_graph_snapshot(
    pool: &AnyPool,
    graph_snapshot_id: Uuid,
) -> Result<Option<AcquisitionAnimeGraphSnapshot>> {
    let row = sqlx::query(ANIME_GRAPH_SELECT_BY_ID)
        .bind(graph_snapshot_id.to_string())
        .fetch_optional(pool)
        .await?;
    row.map(|row| map_anime_graph_snapshot(&row)).transpose()
}

pub async fn upsert_anime_candidate_parse(
    pool: &AnyPool,
    data: NewAcquisitionAnimeCandidateParse,
) -> Result<AcquisitionAnimeCandidateParse> {
    validate_anime_candidate_parse_input(&data)?;
    let parsed_json = json_to_required_string(&data.parsed, "anime candidate parse")?;
    let review_reasons_json =
        json_to_required_string(&data.review_reasons, "anime candidate review reasons")?;
    let existing = find_anime_candidate_parse(pool, &data).await?;
    let candidate_parse_id = existing
        .as_ref()
        .map(|parse| parse.candidate_parse_id)
        .or(data.candidate_parse_id)
        .unwrap_or_else(Uuid::new_v4);

    if existing.is_some() {
        sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_anime_candidate_parses
             SET release_id = ?,
                 source_provider_id = ?,
                 source_candidate_id = ?,
                 release_title = ?,
                 normalized_title = ?,
                 parsed_json = ?,
                 confidence = ?,
                 review_reasons_json = ?,
                 updated_at = CURRENT_TIMESTAMP
             WHERE candidate_parse_id = ?",
        )
        .bind(data.release_id.to_string())
        .bind(data.source_provider_id.map(|value| value.to_string()))
        .bind(data.source_candidate_id.as_deref())
        .bind(data.release_title.trim())
        .bind(data.normalized_title.as_deref())
        .bind(parsed_json)
        .bind(data.confidence.as_str())
        .bind(review_reasons_json)
        .bind(candidate_parse_id.to_string())
        .execute(pool)
        .await
        .context("updating anime candidate parse")?;
    } else {
        sqlx::query::<sqlx::Any>(
            "INSERT INTO acquisition_anime_candidate_parses (
                candidate_parse_id,
                release_id,
                source_provider_id,
                source_candidate_id,
                release_title,
                normalized_title,
                parsed_json,
                confidence,
                review_reasons_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(candidate_parse_id.to_string())
        .bind(data.release_id.to_string())
        .bind(data.source_provider_id.map(|value| value.to_string()))
        .bind(data.source_candidate_id.as_deref())
        .bind(data.release_title.trim())
        .bind(data.normalized_title.as_deref())
        .bind(parsed_json)
        .bind(data.confidence.as_str())
        .bind(review_reasons_json)
        .execute(pool)
        .await
        .context("creating anime candidate parse")?;
    }

    get_anime_candidate_parse(pool, candidate_parse_id)
        .await?
        .ok_or_else(|| anyhow!("upserted anime candidate parse was not readable"))
}

pub async fn get_anime_candidate_parse(
    pool: &AnyPool,
    candidate_parse_id: Uuid,
) -> Result<Option<AcquisitionAnimeCandidateParse>> {
    let row = sqlx::query(ANIME_CANDIDATE_PARSE_SELECT_BY_ID)
        .bind(candidate_parse_id.to_string())
        .fetch_optional(pool)
        .await?;
    row.map(|row| map_anime_candidate_parse(&row)).transpose()
}

pub async fn upsert_file_hash(
    pool: &AnyPool,
    data: NewAcquisitionFileHash,
) -> Result<AcquisitionFileHash> {
    validate_file_hash_input(&data)?;
    let filename_history_json =
        json_to_required_string(&data.filename_history, "file hash filename history")?;
    let existing = find_file_hash(pool, &data).await?;
    let file_hash_id = existing
        .as_ref()
        .map(|hash| hash.file_hash_id)
        .or(data.file_hash_id)
        .unwrap_or_else(Uuid::new_v4);

    if existing.is_some() {
        sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_file_hashes
             SET release_file_id = ?,
                 local_file_id = ?,
                 file_path = ?,
                 size_bytes = ?,
                 mtime_fingerprint = ?,
                 ed2k = ?,
                 crc32 = ?,
                 hash_status = ?,
                 hash_computed_at = ?,
                 hash_invalidated_at = ?,
                 filename_history_json = ?,
                 updated_at = CURRENT_TIMESTAMP
             WHERE file_hash_id = ?",
        )
        .bind(data.release_file_id.map(|value| value.to_string()))
        .bind(data.local_file_id.as_deref())
        .bind(data.file_path.trim())
        .bind(data.size_bytes)
        .bind(data.mtime_fingerprint.as_deref())
        .bind(data.ed2k.as_deref())
        .bind(data.crc32.as_deref())
        .bind(data.hash_status.as_str())
        .bind(data.hash_computed_at.map(db_datetime_string))
        .bind(data.hash_invalidated_at.map(db_datetime_string))
        .bind(filename_history_json)
        .bind(file_hash_id.to_string())
        .execute(pool)
        .await
        .context("updating file hash")?;
    } else {
        sqlx::query::<sqlx::Any>(
            "INSERT INTO acquisition_file_hashes (
                file_hash_id,
                release_file_id,
                local_file_id,
                file_path,
                size_bytes,
                mtime_fingerprint,
                ed2k,
                crc32,
                hash_status,
                hash_computed_at,
                hash_invalidated_at,
                filename_history_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(file_hash_id.to_string())
        .bind(data.release_file_id.map(|value| value.to_string()))
        .bind(data.local_file_id.as_deref())
        .bind(data.file_path.trim())
        .bind(data.size_bytes)
        .bind(data.mtime_fingerprint.as_deref())
        .bind(data.ed2k.as_deref())
        .bind(data.crc32.as_deref())
        .bind(data.hash_status.as_str())
        .bind(data.hash_computed_at.map(db_datetime_string))
        .bind(data.hash_invalidated_at.map(db_datetime_string))
        .bind(filename_history_json)
        .execute(pool)
        .await
        .context("creating file hash")?;
    }

    get_file_hash(pool, file_hash_id)
        .await?
        .ok_or_else(|| anyhow!("upserted file hash was not readable"))
}

pub async fn get_file_hash(
    pool: &AnyPool,
    file_hash_id: Uuid,
) -> Result<Option<AcquisitionFileHash>> {
    let row = sqlx::query(FILE_HASH_SELECT_BY_ID)
        .bind(file_hash_id.to_string())
        .fetch_optional(pool)
        .await?;
    row.map(|row| map_file_hash(&row)).transpose()
}

pub async fn get_file_hash_by_ed2k_size(
    pool: &AnyPool,
    ed2k: &str,
    size_bytes: i64,
) -> Result<Option<AcquisitionFileHash>> {
    let row = sqlx::query(FILE_HASH_SELECT_BY_ED2K_SIZE)
        .bind(ed2k.trim().to_ascii_lowercase())
        .bind(size_bytes)
        .fetch_optional(pool)
        .await?;
    row.map(|row| map_file_hash(&row)).transpose()
}

pub async fn get_file_hash_by_path(
    pool: &AnyPool,
    file_path: &str,
) -> Result<Option<AcquisitionFileHash>> {
    let row = sqlx::query(FILE_HASH_SELECT_BY_PATH)
        .bind(file_path.trim())
        .fetch_optional(pool)
        .await?;
    row.map(|row| map_file_hash(&row)).transpose()
}

pub async fn get_file_hash_by_local_file_id(
    pool: &AnyPool,
    local_file_id: &str,
) -> Result<Option<AcquisitionFileHash>> {
    let row = sqlx::query(FILE_HASH_SELECT_BY_LOCAL_FILE_ID)
        .bind(local_file_id.trim())
        .fetch_optional(pool)
        .await?;
    row.map(|row| map_file_hash(&row)).transpose()
}

pub async fn list_file_hash_work(pool: &AnyPool, limit: i64) -> Result<Vec<AcquisitionFileHash>> {
    let rows = sqlx::query(FILE_HASH_SELECT_WORK)
        .bind(limit.max(0))
        .fetch_all(pool)
        .await?;
    rows.into_iter().map(|row| map_file_hash(&row)).collect()
}

pub async fn upsert_anidb_file_cache(
    pool: &AnyPool,
    data: NewAcquisitionAniDbFileCache,
) -> Result<AcquisitionAniDbFileCache> {
    validate_anidb_file_cache_input(&data)?;
    let episode_ids_json = json_to_required_string(&data.anidb_episode_ids, "AniDB episode ids")?;
    let audio_languages_json =
        json_to_required_string(&data.anidb_audio_languages, "AniDB audio languages")?;
    let subtitle_languages_json =
        json_to_required_string(&data.anidb_subtitle_languages, "AniDB subtitle languages")?;
    let state_flags_json = json_to_required_string(&data.anidb_state_flags, "AniDB state flags")?;

    if get_anidb_file_cache(pool, &data.lookup_key)
        .await?
        .is_some()
    {
        sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_anidb_file_cache
             SET ed2k = ?,
                 size_bytes = ?,
                 lookup_status = ?,
                 anidb_file_id = ?,
                 anidb_anime_id = ?,
                 anidb_episode_ids_json = ?,
                 anidb_group_id = ?,
                 anidb_group_name = ?,
                 anidb_group_short_name = ?,
                 anidb_version = ?,
                 anidb_source = ?,
                 anidb_quality = ?,
                 anidb_audio_languages_json = ?,
                 anidb_subtitle_languages_json = ?,
                 anidb_state_flags_json = ?,
                 anidb_original_filename = ?,
                 released_at = ?,
                 raw_response = ?,
                 positive_cached_at = ?,
                 negative_cached_until = ?,
                 last_lookup_attempt_at = ?,
                 updated_at = CURRENT_TIMESTAMP
             WHERE lookup_key = ?",
        )
        .bind(data.ed2k.trim().to_ascii_lowercase())
        .bind(data.size_bytes)
        .bind(data.lookup_status.as_str())
        .bind(data.anidb_file_id)
        .bind(data.anidb_anime_id)
        .bind(episode_ids_json)
        .bind(data.anidb_group_id)
        .bind(data.anidb_group_name.as_deref())
        .bind(data.anidb_group_short_name.as_deref())
        .bind(data.anidb_version)
        .bind(data.anidb_source.as_deref())
        .bind(data.anidb_quality.as_deref())
        .bind(audio_languages_json)
        .bind(subtitle_languages_json)
        .bind(state_flags_json)
        .bind(data.anidb_original_filename.as_deref())
        .bind(data.released_at.map(db_datetime_string))
        .bind(data.raw_response.as_deref())
        .bind(data.positive_cached_at.map(db_datetime_string))
        .bind(data.negative_cached_until.map(db_datetime_string))
        .bind(data.last_lookup_attempt_at.map(db_datetime_string))
        .bind(data.lookup_key.trim())
        .execute(pool)
        .await
        .context("updating AniDB file cache")?;
    } else {
        sqlx::query::<sqlx::Any>(
            "INSERT INTO acquisition_anidb_file_cache (
                lookup_key,
                ed2k,
                size_bytes,
                lookup_status,
                anidb_file_id,
                anidb_anime_id,
                anidb_episode_ids_json,
                anidb_group_id,
                anidb_group_name,
                anidb_group_short_name,
                anidb_version,
                anidb_source,
                anidb_quality,
                anidb_audio_languages_json,
                anidb_subtitle_languages_json,
                anidb_state_flags_json,
                anidb_original_filename,
                released_at,
                raw_response,
                positive_cached_at,
                negative_cached_until,
                last_lookup_attempt_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(data.lookup_key.trim())
        .bind(data.ed2k.trim().to_ascii_lowercase())
        .bind(data.size_bytes)
        .bind(data.lookup_status.as_str())
        .bind(data.anidb_file_id)
        .bind(data.anidb_anime_id)
        .bind(episode_ids_json)
        .bind(data.anidb_group_id)
        .bind(data.anidb_group_name.as_deref())
        .bind(data.anidb_group_short_name.as_deref())
        .bind(data.anidb_version)
        .bind(data.anidb_source.as_deref())
        .bind(data.anidb_quality.as_deref())
        .bind(audio_languages_json)
        .bind(subtitle_languages_json)
        .bind(state_flags_json)
        .bind(data.anidb_original_filename.as_deref())
        .bind(data.released_at.map(db_datetime_string))
        .bind(data.raw_response.as_deref())
        .bind(data.positive_cached_at.map(db_datetime_string))
        .bind(data.negative_cached_until.map(db_datetime_string))
        .bind(data.last_lookup_attempt_at.map(db_datetime_string))
        .execute(pool)
        .await
        .context("creating AniDB file cache")?;
    }

    get_anidb_file_cache(pool, &data.lookup_key)
        .await?
        .ok_or_else(|| anyhow!("upserted AniDB file cache was not readable"))
}

pub async fn get_anidb_file_cache(
    pool: &AnyPool,
    lookup_key: &str,
) -> Result<Option<AcquisitionAniDbFileCache>> {
    let row = sqlx::query(ANIDB_FILE_CACHE_SELECT_BY_KEY)
        .bind(lookup_key.trim())
        .fetch_optional(pool)
        .await?;
    row.map(|row| map_anidb_file_cache(&row)).transpose()
}

pub async fn upsert_anidb_file_xref(
    pool: &AnyPool,
    data: NewAcquisitionAniDbFileXref,
) -> Result<AcquisitionAniDbFileXref> {
    validate_anidb_file_xref_input(&data)?;
    let existing = find_anidb_file_xref(pool, &data).await?;
    let xref_id = existing
        .as_ref()
        .map(|xref| xref.xref_id)
        .or(data.xref_id)
        .unwrap_or_else(Uuid::new_v4);

    if existing.is_some() {
        sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_anidb_file_xrefs
             SET lookup_key = ?,
                 release_file_id = ?,
                 anidb_file_id = ?,
                 anidb_anime_id = ?,
                 anidb_episode_id = ?,
                 episode_type = ?,
                 percentage_start = ?,
                 percentage_end = ?,
                 episode_order = ?,
                 provider = ?,
                 confidence = ?,
                 is_manual_override = ?,
                 created_from_release_id = ?,
                 created_from_target_id = ?,
                 updated_at = CURRENT_TIMESTAMP
             WHERE xref_id = ?",
        )
        .bind(data.lookup_key.trim())
        .bind(data.release_file_id.map(|value| value.to_string()))
        .bind(data.anidb_file_id)
        .bind(data.anidb_anime_id)
        .bind(data.anidb_episode_id)
        .bind(data.episode_type.as_str())
        .bind(data.percentage_start)
        .bind(data.percentage_end)
        .bind(data.episode_order)
        .bind(data.provider.trim())
        .bind(data.confidence.as_str())
        .bind(data.is_manual_override)
        .bind(data.created_from_release_id.map(|value| value.to_string()))
        .bind(data.created_from_target_id.map(|value| value.to_string()))
        .bind(xref_id.to_string())
        .execute(pool)
        .await
        .context("updating AniDB file xref")?;
    } else {
        sqlx::query::<sqlx::Any>(
            "INSERT INTO acquisition_anidb_file_xrefs (
                xref_id,
                lookup_key,
                release_file_id,
                anidb_file_id,
                anidb_anime_id,
                anidb_episode_id,
                episode_type,
                percentage_start,
                percentage_end,
                episode_order,
                provider,
                confidence,
                is_manual_override,
                created_from_release_id,
                created_from_target_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(xref_id.to_string())
        .bind(data.lookup_key.trim())
        .bind(data.release_file_id.map(|value| value.to_string()))
        .bind(data.anidb_file_id)
        .bind(data.anidb_anime_id)
        .bind(data.anidb_episode_id)
        .bind(data.episode_type.as_str())
        .bind(data.percentage_start)
        .bind(data.percentage_end)
        .bind(data.episode_order)
        .bind(data.provider.trim())
        .bind(data.confidence.as_str())
        .bind(data.is_manual_override)
        .bind(data.created_from_release_id.map(|value| value.to_string()))
        .bind(data.created_from_target_id.map(|value| value.to_string()))
        .execute(pool)
        .await
        .context("creating AniDB file xref")?;
    }

    get_anidb_file_xref(pool, xref_id)
        .await?
        .ok_or_else(|| anyhow!("upserted AniDB file xref was not readable"))
}

pub async fn get_anidb_file_xref(
    pool: &AnyPool,
    xref_id: Uuid,
) -> Result<Option<AcquisitionAniDbFileXref>> {
    let row = sqlx::query(ANIDB_FILE_XREF_SELECT_BY_ID)
        .bind(xref_id.to_string())
        .fetch_optional(pool)
        .await?;
    row.map(|row| map_anidb_file_xref(&row)).transpose()
}

pub async fn list_anidb_file_xrefs(
    pool: &AnyPool,
    lookup_key: &str,
) -> Result<Vec<AcquisitionAniDbFileXref>> {
    let rows = sqlx::query(ANIDB_FILE_XREF_SELECT_BY_LOOKUP)
        .bind(lookup_key.trim())
        .fetch_all(pool)
        .await?;
    rows.into_iter()
        .map(|row| map_anidb_file_xref(&row))
        .collect()
}

pub async fn create_anime_match_attempt(
    pool: &AnyPool,
    data: NewAcquisitionAnimeMatchAttempt,
) -> Result<AcquisitionAnimeMatchAttempt> {
    validate_anime_match_attempt_input(&data)?;
    let match_attempt_id = data.match_attempt_id.unwrap_or_else(Uuid::new_v4);
    let attempted_providers_json =
        json_to_required_string(&data.attempted_providers, "anime attempted providers")?;
    let planned_targets_json =
        json_to_required_string(&data.planned_targets, "anime planned targets")?;
    let verified_targets_json =
        json_to_required_string(&data.verified_targets, "anime verified targets")?;

    sqlx::query::<sqlx::Any>(
        "INSERT INTO acquisition_anime_match_attempts (
            match_attempt_id,
            release_id,
            release_file_id,
            attempted_providers_json,
            selected_provider,
            ed2k,
            size_bytes,
            candidate_fingerprint,
            planned_targets_json,
            verified_targets_json,
            outcome,
            rejection_reason
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(match_attempt_id.to_string())
    .bind(data.release_id.map(|value| value.to_string()))
    .bind(data.release_file_id.map(|value| value.to_string()))
    .bind(attempted_providers_json)
    .bind(data.selected_provider.as_deref())
    .bind(data.ed2k.as_deref())
    .bind(data.size_bytes)
    .bind(data.candidate_fingerprint.as_deref())
    .bind(planned_targets_json)
    .bind(verified_targets_json)
    .bind(data.outcome.as_str())
    .bind(data.rejection_reason.as_deref())
    .execute(pool)
    .await
    .context("creating anime match attempt")?;

    get_anime_match_attempt(pool, match_attempt_id)
        .await?
        .ok_or_else(|| anyhow!("created anime match attempt was not readable"))
}

pub async fn get_anime_match_attempt(
    pool: &AnyPool,
    match_attempt_id: Uuid,
) -> Result<Option<AcquisitionAnimeMatchAttempt>> {
    let row = sqlx::query(ANIME_MATCH_ATTEMPT_SELECT_BY_ID)
        .bind(match_attempt_id.to_string())
        .fetch_optional(pool)
        .await?;
    row.map(|row| map_anime_match_attempt(&row)).transpose()
}

pub async fn list_anime_match_attempts_by_release(
    pool: &AnyPool,
    release_id: Uuid,
) -> Result<Vec<AcquisitionAnimeMatchAttempt>> {
    let rows = sqlx::query(ANIME_MATCH_ATTEMPT_SELECT_BY_RELEASE)
        .bind(release_id.to_string())
        .fetch_all(pool)
        .await?;
    rows.into_iter()
        .map(|row| map_anime_match_attempt(&row))
        .collect()
}

pub async fn create_anime_identity_mismatch(
    pool: &AnyPool,
    data: NewAcquisitionAnimeIdentityMismatch,
) -> Result<AcquisitionAnimeIdentityMismatch> {
    validate_anime_identity_mismatch_input(&data)?;
    let mismatch_id = data.mismatch_id.unwrap_or_else(Uuid::new_v4);
    let planned_target_json =
        json_to_required_string(&data.planned_target, "anime mismatch planned target")?;
    let verified_identity_json =
        json_to_required_string(&data.verified_identity, "anime mismatch verified identity")?;

    sqlx::query::<sqlx::Any>(
        "INSERT INTO acquisition_anime_identity_mismatches (
            mismatch_id,
            release_id,
            release_file_id,
            target_id,
            planned_target_json,
            verified_identity_json,
            provider,
            confidence,
            state,
            reason
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(mismatch_id.to_string())
    .bind(data.release_id.map(|value| value.to_string()))
    .bind(data.release_file_id.map(|value| value.to_string()))
    .bind(data.target_id.map(|value| value.to_string()))
    .bind(planned_target_json)
    .bind(verified_identity_json)
    .bind(data.provider.trim())
    .bind(data.confidence.as_str())
    .bind(data.state.as_str())
    .bind(data.reason.as_deref())
    .execute(pool)
    .await
    .context("creating anime identity mismatch")?;

    get_anime_identity_mismatch(pool, mismatch_id)
        .await?
        .ok_or_else(|| anyhow!("created anime identity mismatch was not readable"))
}

pub async fn get_anime_identity_mismatch(
    pool: &AnyPool,
    mismatch_id: Uuid,
) -> Result<Option<AcquisitionAnimeIdentityMismatch>> {
    let row = sqlx::query(ANIME_IDENTITY_MISMATCH_SELECT_BY_ID)
        .bind(mismatch_id.to_string())
        .fetch_optional(pool)
        .await?;
    row.map(|row| map_anime_identity_mismatch(&row)).transpose()
}

pub async fn list_anime_identity_mismatches_by_release(
    pool: &AnyPool,
    release_id: Uuid,
) -> Result<Vec<AcquisitionAnimeIdentityMismatch>> {
    let rows = sqlx::query(ANIME_IDENTITY_MISMATCH_SELECT_BY_RELEASE)
        .bind(release_id.to_string())
        .fetch_all(pool)
        .await?;
    rows.into_iter()
        .map(|row| map_anime_identity_mismatch(&row))
        .collect()
}

async fn find_release_file(
    pool: &AnyPool,
    data: &NewAcquisitionReleaseFile,
) -> Result<Option<AcquisitionReleaseFile>> {
    if let Some(release_file_id) = data.release_file_id {
        return get_release_file(pool, release_file_id).await;
    }
    if let Some(provider_file_id) = data.provider_file_id.as_deref() {
        let row = sqlx::query(RELEASE_FILE_SELECT_BY_PROVIDER_FILE_ID)
            .bind(data.release_id.to_string())
            .bind(provider_file_id)
            .fetch_optional(pool)
            .await?;
        return row.map(|row| map_release_file(&row)).transpose();
    }
    if let Some(file_id) = data.file_id.as_deref() {
        let row = sqlx::query(RELEASE_FILE_SELECT_BY_FILE_ID)
            .bind(data.release_id.to_string())
            .bind(file_id)
            .fetch_optional(pool)
            .await?;
        return row.map(|row| map_release_file(&row)).transpose();
    }
    if let Some(file_index) = data.file_index {
        let row = sqlx::query(RELEASE_FILE_SELECT_BY_FILE_INDEX)
            .bind(data.release_id.to_string())
            .bind(file_index)
            .fetch_optional(pool)
            .await?;
        return row.map(|row| map_release_file(&row)).transpose();
    }
    let row = sqlx::query(RELEASE_FILE_SELECT_BY_PATH)
        .bind(data.release_id.to_string())
        .bind(data.path.trim())
        .fetch_optional(pool)
        .await?;
    row.map(|row| map_release_file(&row)).transpose()
}

async fn get_release_file(
    pool: &AnyPool,
    release_file_id: Uuid,
) -> Result<Option<AcquisitionReleaseFile>> {
    let row = sqlx::query(RELEASE_FILE_SELECT_BY_ID)
        .bind(release_file_id.to_string())
        .fetch_optional(pool)
        .await?;
    row.map(|row| map_release_file(&row)).transpose()
}

async fn find_release_coverage(
    pool: &AnyPool,
    data: &NewAcquisitionReleaseCoverage,
) -> Result<Option<AcquisitionReleaseCoverage>> {
    if let Some(coverage_id) = data.coverage_id {
        return get_release_coverage(pool, coverage_id).await;
    }
    let row = if let Some(release_file_id) = data.release_file_id {
        sqlx::query(RELEASE_COVERAGE_SELECT_BY_FILE_TARGET)
            .bind(data.release_id.to_string())
            .bind(data.target_id.to_string())
            .bind(release_file_id.to_string())
            .fetch_optional(pool)
            .await?
    } else {
        sqlx::query(RELEASE_COVERAGE_SELECT_BY_TARGET_WITHOUT_FILE)
            .bind(data.release_id.to_string())
            .bind(data.target_id.to_string())
            .fetch_optional(pool)
            .await?
    };
    row.map(|row| map_release_coverage(&row)).transpose()
}

async fn get_release_coverage(
    pool: &AnyPool,
    coverage_id: Uuid,
) -> Result<Option<AcquisitionReleaseCoverage>> {
    let row = sqlx::query(RELEASE_COVERAGE_SELECT_BY_ID)
        .bind(coverage_id.to_string())
        .fetch_optional(pool)
        .await?;
    row.map(|row| map_release_coverage(&row)).transpose()
}

async fn find_release_job(
    pool: &AnyPool,
    data: &NewAcquisitionReleaseJob,
) -> Result<Option<AcquisitionReleaseJob>> {
    if let Some(release_job_id) = data.release_job_id {
        return get_release_job(pool, release_job_id).await;
    }
    if let Some(download_id) = data.download_id.as_deref() {
        let row = sqlx::query(RELEASE_JOB_SELECT_BY_DOWNLOAD_ID)
            .bind(data.release_id.to_string())
            .bind(download_id)
            .fetch_optional(pool)
            .await?;
        return row.map(|row| map_release_job(&row)).transpose();
    }
    if let Some(remote_release_id) = data.remote_release_id.as_deref() {
        let row = sqlx::query(RELEASE_JOB_SELECT_BY_REMOTE_ID)
            .bind(data.release_id.to_string())
            .bind(remote_release_id)
            .fetch_optional(pool)
            .await?;
        return row.map(|row| map_release_job(&row)).transpose();
    }
    Ok(None)
}

async fn get_release_job(
    pool: &AnyPool,
    release_job_id: Uuid,
) -> Result<Option<AcquisitionReleaseJob>> {
    let row = sqlx::query(RELEASE_JOB_SELECT_BY_ID)
        .bind(release_job_id.to_string())
        .fetch_optional(pool)
        .await?;
    row.map(|row| map_release_job(&row)).transpose()
}

async fn find_anime_graph_snapshot(
    pool: &AnyPool,
    data: &NewAcquisitionAnimeGraphSnapshot,
) -> Result<Option<AcquisitionAnimeGraphSnapshot>> {
    if let Some(graph_snapshot_id) = data.graph_snapshot_id {
        return get_anime_graph_snapshot(pool, graph_snapshot_id).await;
    }
    let row = if let Some(subscription_id) = data.subscription_id {
        sqlx::query(ANIME_GRAPH_SELECT_BY_SUBSCRIPTION_FINGERPRINT)
            .bind(subscription_id.to_string())
            .bind(data.fingerprint.trim())
            .fetch_optional(pool)
            .await?
    } else {
        sqlx::query(ANIME_GRAPH_SELECT_BY_OWNER_FINGERPRINT)
            .bind(data.owner_id.trim())
            .bind(data.fingerprint.trim())
            .fetch_optional(pool)
            .await?
    };
    row.map(|row| map_anime_graph_snapshot(&row)).transpose()
}

async fn find_anime_candidate_parse(
    pool: &AnyPool,
    data: &NewAcquisitionAnimeCandidateParse,
) -> Result<Option<AcquisitionAnimeCandidateParse>> {
    if let Some(candidate_parse_id) = data.candidate_parse_id {
        return get_anime_candidate_parse(pool, candidate_parse_id).await;
    }
    let row = if let Some(source_candidate_id) = data.source_candidate_id.as_deref() {
        sqlx::query(ANIME_CANDIDATE_PARSE_SELECT_BY_SOURCE_ID)
            .bind(data.release_id.to_string())
            .bind(source_candidate_id.trim())
            .fetch_optional(pool)
            .await?
    } else {
        sqlx::query(ANIME_CANDIDATE_PARSE_SELECT_BY_RELEASE_TITLE)
            .bind(data.release_id.to_string())
            .bind(data.release_title.trim())
            .fetch_optional(pool)
            .await?
    };
    row.map(|row| map_anime_candidate_parse(&row)).transpose()
}

async fn find_file_hash(
    pool: &AnyPool,
    data: &NewAcquisitionFileHash,
) -> Result<Option<AcquisitionFileHash>> {
    if let Some(file_hash_id) = data.file_hash_id {
        return get_file_hash(pool, file_hash_id).await;
    }
    if let Some(local_file_id) = data.local_file_id.as_deref() {
        if let Some(hash) = get_file_hash_by_local_file_id(pool, local_file_id).await? {
            return Ok(Some(hash));
        }
    }
    let row = sqlx::query(FILE_HASH_SELECT_BY_PATH)
        .bind(data.file_path.trim())
        .fetch_optional(pool)
        .await?;
    row.map(|row| map_file_hash(&row)).transpose()
}

async fn find_anidb_file_xref(
    pool: &AnyPool,
    data: &NewAcquisitionAniDbFileXref,
) -> Result<Option<AcquisitionAniDbFileXref>> {
    if let Some(xref_id) = data.xref_id {
        return get_anidb_file_xref(pool, xref_id).await;
    }
    let row = sqlx::query(ANIDB_FILE_XREF_SELECT_BY_IDENTITY)
        .bind(data.lookup_key.trim())
        .bind(data.anidb_episode_id)
        .bind(data.percentage_start)
        .bind(data.percentage_end)
        .bind(data.episode_order)
        .fetch_optional(pool)
        .await?;
    row.map(|row| map_anidb_file_xref(&row)).transpose()
}

fn validate_release_input(data: &NewAcquisitionRelease) -> Result<()> {
    if data.source_extension_id.trim().is_empty() {
        bail!("source_extension_id is required");
    }
    if data.owner_id.trim().is_empty() {
        bail!("owner_id is required");
    }
    if data.title.trim().is_empty() {
        bail!("title is required");
    }
    if data.release_title.trim().is_empty() {
        bail!("release_title is required");
    }
    if data.source.trim().is_empty() {
        bail!("source is required");
    }
    if data.source_kind.trim().is_empty() {
        bail!("source_kind is required");
    }
    if data.fingerprint.trim().is_empty() {
        bail!("fingerprint is required");
    }
    if data.resolver_version.trim().is_empty() {
        bail!("resolver_version is required");
    }
    Ok(())
}

fn validate_release_file_input(data: &NewAcquisitionReleaseFile) -> Result<()> {
    if data.path.trim().is_empty() {
        bail!("release file path is required");
    }
    if let Some(size_bytes) = data.size_bytes
        && size_bytes < 0
    {
        bail!("release file size_bytes cannot be negative");
    }
    Ok(())
}

fn validate_release_job_input(data: &NewAcquisitionReleaseJob) -> Result<()> {
    if data.route_logical_id.trim().is_empty() {
        bail!("route_logical_id is required");
    }
    Ok(())
}

fn validate_anime_graph_snapshot_input(data: &NewAcquisitionAnimeGraphSnapshot) -> Result<()> {
    if data.owner_id.trim().is_empty() {
        bail!("anime graph owner_id is required");
    }
    if data.fingerprint.trim().is_empty() {
        bail!("anime graph fingerprint is required");
    }
    Ok(())
}

fn validate_anime_candidate_parse_input(data: &NewAcquisitionAnimeCandidateParse) -> Result<()> {
    if data.release_title.trim().is_empty() {
        bail!("anime candidate release_title is required");
    }
    Ok(())
}

fn validate_file_hash_input(data: &NewAcquisitionFileHash) -> Result<()> {
    if data.file_path.trim().is_empty() {
        bail!("file hash path is required");
    }
    if data.size_bytes < 0 {
        bail!("file hash size_bytes cannot be negative");
    }
    Ok(())
}

fn validate_anidb_file_cache_input(data: &NewAcquisitionAniDbFileCache) -> Result<()> {
    if data.lookup_key.trim().is_empty() {
        bail!("AniDB lookup_key is required");
    }
    if data.ed2k.trim().is_empty() {
        bail!("AniDB ed2k is required");
    }
    if data.size_bytes < 0 {
        bail!("AniDB size_bytes cannot be negative");
    }
    Ok(())
}

fn validate_anidb_file_xref_input(data: &NewAcquisitionAniDbFileXref) -> Result<()> {
    if data.lookup_key.trim().is_empty() {
        bail!("AniDB xref lookup_key is required");
    }
    if data.anidb_anime_id <= 0 {
        bail!("AniDB xref anime id must be positive");
    }
    if data.anidb_episode_id <= 0 {
        bail!("AniDB xref episode id must be positive");
    }
    if data.percentage_start < 0 || data.percentage_end > 100 {
        bail!("AniDB xref percentage range must be inside 0..100");
    }
    if data.percentage_start >= data.percentage_end {
        bail!("AniDB xref percentage_start must be less than percentage_end");
    }
    if data.provider.trim().is_empty() {
        bail!("AniDB xref provider is required");
    }
    Ok(())
}

fn validate_anime_match_attempt_input(data: &NewAcquisitionAnimeMatchAttempt) -> Result<()> {
    if let Some(size_bytes) = data.size_bytes
        && size_bytes < 0
    {
        bail!("anime match attempt size_bytes cannot be negative");
    }
    Ok(())
}

fn validate_anime_identity_mismatch_input(
    data: &NewAcquisitionAnimeIdentityMismatch,
) -> Result<()> {
    if data.provider.trim().is_empty() {
        bail!("anime mismatch provider is required");
    }
    Ok(())
}

fn map_release(row: &AnyRow) -> Result<AcquisitionRelease> {
    let release_id_raw: String = row.try_get("release_id")?;
    let subscription_id_raw = row_get_opt_string(row, "subscription_id")?;
    let source_provider_id_raw = row_get_opt_string(row, "source_provider_id")?;
    let selected_provider_id_raw = row_get_opt_string(row, "selected_provider_id")?;
    let media_type_raw: String = row.try_get("media_type")?;
    let release_kind_raw: String = row.try_get("release_kind")?;
    let resolver_kind_raw: String = row.try_get("resolver_kind")?;
    let confidence_raw: String = row.try_get("confidence")?;
    let state_raw: String = row.try_get("state")?;

    Ok(AcquisitionRelease {
        release_id: parse_uuid(&release_id_raw, "acquisition_releases.release_id")?,
        subscription_id: parse_uuid_opt(
            subscription_id_raw,
            "acquisition_releases.subscription_id",
        )?,
        source_provider_id: parse_uuid_opt(
            source_provider_id_raw,
            "acquisition_releases.source_provider_id",
        )?,
        source_extension_id: row.try_get("source_extension_id")?,
        owner_id: row.try_get("owner_id")?,
        media_type: parse_media_type(&media_type_raw, "acquisition_releases.media_type")?,
        title: row.try_get("title")?,
        release_title: row.try_get("release_title")?,
        source: row.try_get("source")?,
        source_kind: row.try_get("source_kind")?,
        info_hash: row_get_opt_string(row, "info_hash")?,
        fingerprint: row.try_get("fingerprint")?,
        release_kind: ReleaseKind::from_str(&release_kind_raw)?,
        resolver_kind: ReleaseResolverKind::from_str(&resolver_kind_raw)?,
        resolver_version: row.try_get("resolver_version")?,
        confidence: ReleaseConfidence::from_str(&confidence_raw)?,
        score: row_get_f64_opt(row, "score")?,
        selected_route_logical_id: row_get_opt_string(row, "selected_route_logical_id")?,
        selected_provider_id: parse_uuid_opt(
            selected_provider_id_raw,
            "acquisition_releases.selected_provider_id",
        )?,
        download_id: row_get_opt_string(row, "download_id")?,
        remote_release_id: row_get_opt_string(row, "remote_release_id")?,
        state: AcquisitionReleaseState::from_str(&state_raw)?,
        state_reason: row_get_opt_string(row, "state_reason")?,
        selected_candidate: parse_json_opt(
            row_get_opt_string(row, "selected_candidate_json")?,
            "acquisition_releases.selected_candidate_json",
        )?,
        coverage_plan: parse_json_opt(
            row_get_opt_string(row, "coverage_plan_json")?,
            "acquisition_releases.coverage_plan_json",
        )?,
        created_at: parse_datetime(
            &row.try_get::<String, _>("created_at")?,
            "acquisition_releases.created_at",
        )?,
        updated_at: parse_datetime(
            &row.try_get::<String, _>("updated_at")?,
            "acquisition_releases.updated_at",
        )?,
    })
}

fn map_release_file(row: &AnyRow) -> Result<AcquisitionReleaseFile> {
    let release_file_id_raw: String = row.try_get("release_file_id")?;
    let release_id_raw: String = row.try_get("release_id")?;
    let parser_confidence_raw: String = row.try_get("parser_confidence")?;
    Ok(AcquisitionReleaseFile {
        release_file_id: parse_uuid(
            &release_file_id_raw,
            "acquisition_release_files.release_file_id",
        )?,
        release_id: parse_uuid(&release_id_raw, "acquisition_release_files.release_id")?,
        file_index: row_get_i64_opt(row, "file_index")?,
        file_id: row_get_opt_string(row, "file_id")?,
        provider_file_id: row_get_opt_string(row, "provider_file_id")?,
        path: row.try_get("path")?,
        basename: row.try_get("basename")?,
        size_bytes: row_get_i64_opt(row, "size_bytes")?,
        selectable: row_get_bool(row, "selectable")?,
        selected: row_get_bool_opt(row, "selected")?,
        parsed_title: row_get_opt_string(row, "parsed_title")?,
        parsed_season_number: row_get_i64_opt(row, "parsed_season_number")?
            .map(|value| value as i32),
        parsed_episode_number: row_get_i64_opt(row, "parsed_episode_number")?
            .map(|value| value as i32),
        parsed_episode_end_number: row_get_i64_opt(row, "parsed_episode_end_number")?
            .map(|value| value as i32),
        parsed_absolute_episode_number: row_get_i64_opt(row, "parsed_absolute_episode_number")?
            .map(|value| value as i32),
        parsed_absolute_episode_end_number: row_get_i64_opt(
            row,
            "parsed_absolute_episode_end_number",
        )?
        .map(|value| value as i32),
        parsed_air_date: row_get_opt_string(row, "parsed_air_date")?,
        parsed_quality: row_get_opt_string(row, "parsed_quality")?,
        parsed_language: row_get_opt_string(row, "parsed_language")?,
        parsed_release_group: row_get_opt_string(row, "parsed_release_group")?,
        parser_confidence: ReleaseConfidence::from_str(&parser_confidence_raw)?,
        parser_reason: row_get_opt_string(row, "parser_reason")?,
        raw: parse_json_opt(
            row_get_opt_string(row, "raw_json")?,
            "acquisition_release_files.raw_json",
        )?,
        provider_metadata: parse_json_opt(
            row_get_opt_string(row, "provider_metadata_json")?,
            "acquisition_release_files.provider_metadata_json",
        )?,
        created_at: parse_datetime(
            &row.try_get::<String, _>("created_at")?,
            "acquisition_release_files.created_at",
        )?,
        updated_at: parse_datetime(
            &row.try_get::<String, _>("updated_at")?,
            "acquisition_release_files.updated_at",
        )?,
    })
}

fn map_release_coverage(row: &AnyRow) -> Result<AcquisitionReleaseCoverage> {
    let coverage_id_raw: String = row.try_get("coverage_id")?;
    let release_id_raw: String = row.try_get("release_id")?;
    let release_file_id_raw = row_get_opt_string(row, "release_file_id")?;
    let target_id_raw: String = row.try_get("target_id")?;
    let coverage_kind_raw: String = row.try_get("coverage_kind")?;
    let confidence_raw: String = row.try_get("confidence")?;
    let state_raw: String = row.try_get("state")?;
    Ok(AcquisitionReleaseCoverage {
        coverage_id: parse_uuid(&coverage_id_raw, "acquisition_release_coverage.coverage_id")?,
        release_id: parse_uuid(&release_id_raw, "acquisition_release_coverage.release_id")?,
        release_file_id: parse_uuid_opt(
            release_file_id_raw,
            "acquisition_release_coverage.release_file_id",
        )?,
        target_id: parse_uuid(&target_id_raw, "acquisition_release_coverage.target_id")?,
        coverage_kind: ReleaseCoverageKind::from_str(&coverage_kind_raw)?,
        confidence: ReleaseConfidence::from_str(&confidence_raw)?,
        score: row_get_f64_opt(row, "score")?,
        reason: row_get_opt_string(row, "reason")?,
        state: ReleaseCoverageState::from_str(&state_raw)?,
        verified_by: row_get_opt_string(row, "verified_by")?,
        created_at: parse_datetime(
            &row.try_get::<String, _>("created_at")?,
            "acquisition_release_coverage.created_at",
        )?,
        updated_at: parse_datetime(
            &row.try_get::<String, _>("updated_at")?,
            "acquisition_release_coverage.updated_at",
        )?,
    })
}

fn map_release_job(row: &AnyRow) -> Result<AcquisitionReleaseJob> {
    let release_job_id_raw: String = row.try_get("release_job_id")?;
    let release_id_raw: String = row.try_get("release_id")?;
    let provider_id_raw = row_get_opt_string(row, "provider_id")?;
    let state_raw: String = row.try_get("state")?;
    Ok(AcquisitionReleaseJob {
        release_job_id: parse_uuid(
            &release_job_id_raw,
            "acquisition_release_jobs.release_job_id",
        )?,
        release_id: parse_uuid(&release_id_raw, "acquisition_release_jobs.release_id")?,
        route_logical_id: row.try_get("route_logical_id")?,
        provider_id: parse_uuid_opt(provider_id_raw, "acquisition_release_jobs.provider_id")?,
        download_id: row_get_opt_string(row, "download_id")?,
        remote_release_id: row_get_opt_string(row, "remote_release_id")?,
        state: ReleaseJobState::from_str(&state_raw)?,
        state_reason: row_get_opt_string(row, "state_reason")?,
        active: row_get_bool(row, "active")?,
        started_at: parse_datetime_opt(
            row_get_opt_string(row, "started_at")?,
            "acquisition_release_jobs.started_at",
        )?,
        completed_at: parse_datetime_opt(
            row_get_opt_string(row, "completed_at")?,
            "acquisition_release_jobs.completed_at",
        )?,
        created_at: parse_datetime(
            &row.try_get::<String, _>("created_at")?,
            "acquisition_release_jobs.created_at",
        )?,
        updated_at: parse_datetime(
            &row.try_get::<String, _>("updated_at")?,
            "acquisition_release_jobs.updated_at",
        )?,
    })
}

fn map_anime_graph_snapshot(row: &AnyRow) -> Result<AcquisitionAnimeGraphSnapshot> {
    let graph_snapshot_id_raw: String = row.try_get("graph_snapshot_id")?;
    let subscription_id_raw = row_get_opt_string(row, "subscription_id")?;
    let media_type_raw: String = row.try_get("media_type")?;
    Ok(AcquisitionAnimeGraphSnapshot {
        graph_snapshot_id: parse_uuid(
            &graph_snapshot_id_raw,
            "acquisition_anime_graph_snapshots.graph_snapshot_id",
        )?,
        subscription_id: parse_uuid_opt(
            subscription_id_raw,
            "acquisition_anime_graph_snapshots.subscription_id",
        )?,
        owner_id: row.try_get("owner_id")?,
        media_type: parse_media_type(
            &media_type_raw,
            "acquisition_anime_graph_snapshots.media_type",
        )?,
        anilist_root_id: row_get_i64_opt(row, "anilist_root_id")?,
        anilist_season_id: row_get_i64_opt(row, "anilist_season_id")?,
        anilist_status: row_get_opt_string(row, "anilist_status")?,
        anilist_next_airing_at: parse_datetime_opt(
            row_get_opt_string(row, "anilist_next_airing_at")?,
            "acquisition_anime_graph_snapshots.anilist_next_airing_at",
        )?,
        tvdb_series_id: row_get_i64_opt(row, "tvdb_series_id")?,
        anidb_anime_id: row_get_i64_opt(row, "anidb_anime_id")?,
        fingerprint: row.try_get("fingerprint")?,
        graph: parse_json(
            &row.try_get::<String, _>("graph_json")?,
            "acquisition_anime_graph_snapshots.graph_json",
        )?,
        aliases: parse_json(
            &row.try_get::<String, _>("aliases_json")?,
            "acquisition_anime_graph_snapshots.aliases_json",
        )?,
        created_at: parse_datetime(
            &row.try_get::<String, _>("created_at")?,
            "acquisition_anime_graph_snapshots.created_at",
        )?,
        updated_at: parse_datetime(
            &row.try_get::<String, _>("updated_at")?,
            "acquisition_anime_graph_snapshots.updated_at",
        )?,
    })
}

fn map_anime_candidate_parse(row: &AnyRow) -> Result<AcquisitionAnimeCandidateParse> {
    let candidate_parse_id_raw: String = row.try_get("candidate_parse_id")?;
    let release_id_raw: String = row.try_get("release_id")?;
    let source_provider_id_raw = row_get_opt_string(row, "source_provider_id")?;
    let confidence_raw: String = row.try_get("confidence")?;
    Ok(AcquisitionAnimeCandidateParse {
        candidate_parse_id: parse_uuid(
            &candidate_parse_id_raw,
            "acquisition_anime_candidate_parses.candidate_parse_id",
        )?,
        release_id: parse_uuid(
            &release_id_raw,
            "acquisition_anime_candidate_parses.release_id",
        )?,
        source_provider_id: parse_uuid_opt(
            source_provider_id_raw,
            "acquisition_anime_candidate_parses.source_provider_id",
        )?,
        source_candidate_id: row_get_opt_string(row, "source_candidate_id")?,
        release_title: row.try_get("release_title")?,
        normalized_title: row_get_opt_string(row, "normalized_title")?,
        parsed: parse_json(
            &row.try_get::<String, _>("parsed_json")?,
            "acquisition_anime_candidate_parses.parsed_json",
        )?,
        confidence: ReleaseConfidence::from_str(&confidence_raw)?,
        review_reasons: parse_json(
            &row.try_get::<String, _>("review_reasons_json")?,
            "acquisition_anime_candidate_parses.review_reasons_json",
        )?,
        created_at: parse_datetime(
            &row.try_get::<String, _>("created_at")?,
            "acquisition_anime_candidate_parses.created_at",
        )?,
        updated_at: parse_datetime(
            &row.try_get::<String, _>("updated_at")?,
            "acquisition_anime_candidate_parses.updated_at",
        )?,
    })
}

fn map_file_hash(row: &AnyRow) -> Result<AcquisitionFileHash> {
    let file_hash_id_raw: String = row.try_get("file_hash_id")?;
    let release_file_id_raw = row_get_opt_string(row, "release_file_id")?;
    let hash_status_raw: String = row.try_get("hash_status")?;
    Ok(AcquisitionFileHash {
        file_hash_id: parse_uuid(&file_hash_id_raw, "acquisition_file_hashes.file_hash_id")?,
        release_file_id: parse_uuid_opt(
            release_file_id_raw,
            "acquisition_file_hashes.release_file_id",
        )?,
        local_file_id: row_get_opt_string(row, "local_file_id")?,
        file_path: row.try_get("file_path")?,
        size_bytes: row.try_get("size_bytes")?,
        mtime_fingerprint: row_get_opt_string(row, "mtime_fingerprint")?,
        ed2k: row_get_opt_string(row, "ed2k")?,
        crc32: row_get_opt_string(row, "crc32")?,
        hash_status: AnimeFileHashStatus::from_str(&hash_status_raw)?,
        hash_computed_at: parse_datetime_opt(
            row_get_opt_string(row, "hash_computed_at")?,
            "acquisition_file_hashes.hash_computed_at",
        )?,
        hash_invalidated_at: parse_datetime_opt(
            row_get_opt_string(row, "hash_invalidated_at")?,
            "acquisition_file_hashes.hash_invalidated_at",
        )?,
        filename_history: parse_json(
            &row.try_get::<String, _>("filename_history_json")?,
            "acquisition_file_hashes.filename_history_json",
        )?,
        created_at: parse_datetime(
            &row.try_get::<String, _>("created_at")?,
            "acquisition_file_hashes.created_at",
        )?,
        updated_at: parse_datetime(
            &row.try_get::<String, _>("updated_at")?,
            "acquisition_file_hashes.updated_at",
        )?,
    })
}

fn map_anidb_file_cache(row: &AnyRow) -> Result<AcquisitionAniDbFileCache> {
    let lookup_status_raw: String = row.try_get("lookup_status")?;
    Ok(AcquisitionAniDbFileCache {
        lookup_key: row.try_get("lookup_key")?,
        ed2k: row.try_get("ed2k")?,
        size_bytes: row.try_get("size_bytes")?,
        lookup_status: AniDbFileLookupStatus::from_str(&lookup_status_raw)?,
        anidb_file_id: row_get_i64_opt(row, "anidb_file_id")?,
        anidb_anime_id: row_get_i64_opt(row, "anidb_anime_id")?,
        anidb_episode_ids: parse_json(
            &row.try_get::<String, _>("anidb_episode_ids_json")?,
            "acquisition_anidb_file_cache.anidb_episode_ids_json",
        )?,
        anidb_group_id: row_get_i64_opt(row, "anidb_group_id")?,
        anidb_group_name: row_get_opt_string(row, "anidb_group_name")?,
        anidb_group_short_name: row_get_opt_string(row, "anidb_group_short_name")?,
        anidb_version: row_get_i64_opt(row, "anidb_version")?,
        anidb_source: row_get_opt_string(row, "anidb_source")?,
        anidb_quality: row_get_opt_string(row, "anidb_quality")?,
        anidb_audio_languages: parse_json(
            &row.try_get::<String, _>("anidb_audio_languages_json")?,
            "acquisition_anidb_file_cache.anidb_audio_languages_json",
        )?,
        anidb_subtitle_languages: parse_json(
            &row.try_get::<String, _>("anidb_subtitle_languages_json")?,
            "acquisition_anidb_file_cache.anidb_subtitle_languages_json",
        )?,
        anidb_state_flags: parse_json(
            &row.try_get::<String, _>("anidb_state_flags_json")?,
            "acquisition_anidb_file_cache.anidb_state_flags_json",
        )?,
        anidb_original_filename: row_get_opt_string(row, "anidb_original_filename")?,
        released_at: parse_datetime_opt(
            row_get_opt_string(row, "released_at")?,
            "acquisition_anidb_file_cache.released_at",
        )?,
        raw_response: row_get_opt_string(row, "raw_response")?,
        positive_cached_at: parse_datetime_opt(
            row_get_opt_string(row, "positive_cached_at")?,
            "acquisition_anidb_file_cache.positive_cached_at",
        )?,
        negative_cached_until: parse_datetime_opt(
            row_get_opt_string(row, "negative_cached_until")?,
            "acquisition_anidb_file_cache.negative_cached_until",
        )?,
        last_lookup_attempt_at: parse_datetime_opt(
            row_get_opt_string(row, "last_lookup_attempt_at")?,
            "acquisition_anidb_file_cache.last_lookup_attempt_at",
        )?,
        created_at: parse_datetime(
            &row.try_get::<String, _>("created_at")?,
            "acquisition_anidb_file_cache.created_at",
        )?,
        updated_at: parse_datetime(
            &row.try_get::<String, _>("updated_at")?,
            "acquisition_anidb_file_cache.updated_at",
        )?,
    })
}

fn map_anidb_file_xref(row: &AnyRow) -> Result<AcquisitionAniDbFileXref> {
    let xref_id_raw: String = row.try_get("xref_id")?;
    let release_file_id_raw = row_get_opt_string(row, "release_file_id")?;
    let created_from_release_id_raw = row_get_opt_string(row, "created_from_release_id")?;
    let created_from_target_id_raw = row_get_opt_string(row, "created_from_target_id")?;
    let episode_type_raw: String = row.try_get("episode_type")?;
    let confidence_raw: String = row.try_get("confidence")?;
    Ok(AcquisitionAniDbFileXref {
        xref_id: parse_uuid(&xref_id_raw, "acquisition_anidb_file_xrefs.xref_id")?,
        lookup_key: row.try_get("lookup_key")?,
        release_file_id: parse_uuid_opt(
            release_file_id_raw,
            "acquisition_anidb_file_xrefs.release_file_id",
        )?,
        anidb_file_id: row_get_i64_opt(row, "anidb_file_id")?,
        anidb_anime_id: row.try_get("anidb_anime_id")?,
        anidb_episode_id: row.try_get("anidb_episode_id")?,
        episode_type: AnimeEpisodeType::from_str(&episode_type_raw)?,
        percentage_start: row.try_get("percentage_start")?,
        percentage_end: row.try_get("percentage_end")?,
        episode_order: row.try_get("episode_order")?,
        provider: row.try_get("provider")?,
        confidence: ReleaseConfidence::from_str(&confidence_raw)?,
        is_manual_override: row_get_bool(row, "is_manual_override")?,
        created_from_release_id: parse_uuid_opt(
            created_from_release_id_raw,
            "acquisition_anidb_file_xrefs.created_from_release_id",
        )?,
        created_from_target_id: parse_uuid_opt(
            created_from_target_id_raw,
            "acquisition_anidb_file_xrefs.created_from_target_id",
        )?,
        created_at: parse_datetime(
            &row.try_get::<String, _>("created_at")?,
            "acquisition_anidb_file_xrefs.created_at",
        )?,
        updated_at: parse_datetime(
            &row.try_get::<String, _>("updated_at")?,
            "acquisition_anidb_file_xrefs.updated_at",
        )?,
    })
}

fn map_anime_match_attempt(row: &AnyRow) -> Result<AcquisitionAnimeMatchAttempt> {
    let match_attempt_id_raw: String = row.try_get("match_attempt_id")?;
    let release_id_raw = row_get_opt_string(row, "release_id")?;
    let release_file_id_raw = row_get_opt_string(row, "release_file_id")?;
    let outcome_raw: String = row.try_get("outcome")?;
    Ok(AcquisitionAnimeMatchAttempt {
        match_attempt_id: parse_uuid(
            &match_attempt_id_raw,
            "acquisition_anime_match_attempts.match_attempt_id",
        )?,
        release_id: parse_uuid_opt(
            release_id_raw,
            "acquisition_anime_match_attempts.release_id",
        )?,
        release_file_id: parse_uuid_opt(
            release_file_id_raw,
            "acquisition_anime_match_attempts.release_file_id",
        )?,
        attempted_providers: parse_json(
            &row.try_get::<String, _>("attempted_providers_json")?,
            "acquisition_anime_match_attempts.attempted_providers_json",
        )?,
        selected_provider: row_get_opt_string(row, "selected_provider")?,
        ed2k: row_get_opt_string(row, "ed2k")?,
        size_bytes: row_get_i64_opt(row, "size_bytes")?,
        candidate_fingerprint: row_get_opt_string(row, "candidate_fingerprint")?,
        planned_targets: parse_json(
            &row.try_get::<String, _>("planned_targets_json")?,
            "acquisition_anime_match_attempts.planned_targets_json",
        )?,
        verified_targets: parse_json(
            &row.try_get::<String, _>("verified_targets_json")?,
            "acquisition_anime_match_attempts.verified_targets_json",
        )?,
        outcome: AnimeMatchOutcome::from_str(&outcome_raw)?,
        rejection_reason: row_get_opt_string(row, "rejection_reason")?,
        created_at: parse_datetime(
            &row.try_get::<String, _>("created_at")?,
            "acquisition_anime_match_attempts.created_at",
        )?,
        updated_at: parse_datetime(
            &row.try_get::<String, _>("updated_at")?,
            "acquisition_anime_match_attempts.updated_at",
        )?,
    })
}

fn map_anime_identity_mismatch(row: &AnyRow) -> Result<AcquisitionAnimeIdentityMismatch> {
    let mismatch_id_raw: String = row.try_get("mismatch_id")?;
    let release_id_raw = row_get_opt_string(row, "release_id")?;
    let release_file_id_raw = row_get_opt_string(row, "release_file_id")?;
    let target_id_raw = row_get_opt_string(row, "target_id")?;
    let confidence_raw: String = row.try_get("confidence")?;
    let state_raw: String = row.try_get("state")?;
    Ok(AcquisitionAnimeIdentityMismatch {
        mismatch_id: parse_uuid(
            &mismatch_id_raw,
            "acquisition_anime_identity_mismatches.mismatch_id",
        )?,
        release_id: parse_uuid_opt(
            release_id_raw,
            "acquisition_anime_identity_mismatches.release_id",
        )?,
        release_file_id: parse_uuid_opt(
            release_file_id_raw,
            "acquisition_anime_identity_mismatches.release_file_id",
        )?,
        target_id: parse_uuid_opt(
            target_id_raw,
            "acquisition_anime_identity_mismatches.target_id",
        )?,
        planned_target: parse_json(
            &row.try_get::<String, _>("planned_target_json")?,
            "acquisition_anime_identity_mismatches.planned_target_json",
        )?,
        verified_identity: parse_json(
            &row.try_get::<String, _>("verified_identity_json")?,
            "acquisition_anime_identity_mismatches.verified_identity_json",
        )?,
        provider: row.try_get("provider")?,
        confidence: ReleaseConfidence::from_str(&confidence_raw)?,
        state: AnimeMismatchState::from_str(&state_raw)?,
        reason: row_get_opt_string(row, "reason")?,
        created_at: parse_datetime(
            &row.try_get::<String, _>("created_at")?,
            "acquisition_anime_identity_mismatches.created_at",
        )?,
        updated_at: parse_datetime(
            &row.try_get::<String, _>("updated_at")?,
            "acquisition_anime_identity_mismatches.updated_at",
        )?,
    })
}

fn basename_from_path(path: &str) -> String {
    path.rsplit(['/', '\\'])
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(path.trim())
        .to_string()
}

fn parse_media_type(raw: &str, field: &str) -> Result<MediaType> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "movie" => Ok(MediaType::Movie),
        "series" => Ok(MediaType::Series),
        "anime" => Ok(MediaType::Anime),
        _ => bail!("invalid enum value '{}' for field {}", raw, field),
    }
}

fn json_to_string(value: Option<&JsonValue>) -> Result<Option<String>> {
    match value {
        Some(value) => Ok(Some(
            serde_json::to_string(value).context("serializing json")?,
        )),
        None => Ok(None),
    }
}

fn json_to_required_string(value: &JsonValue, label: &str) -> Result<String> {
    serde_json::to_string(value).with_context(|| format!("serializing {label} json"))
}

fn parse_uuid(value: &str, field: &str) -> Result<Uuid> {
    Uuid::parse_str(value.trim()).with_context(|| format!("invalid {field} uuid '{value}'"))
}

fn parse_uuid_opt(value: Option<String>, field: &str) -> Result<Option<Uuid>> {
    value
        .as_deref()
        .map(|value| parse_uuid(value, field))
        .transpose()
}

fn parse_datetime(value: &str, field: &str) -> Result<DateTime<Utc>> {
    let value = value.trim();
    let parsed = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f"))
        .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S"))
        .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f"))
        .with_context(|| format!("invalid {field} '{value}'"))?;
    Ok(DateTime::<Utc>::from_naive_utc_and_offset(parsed, Utc))
}

fn parse_datetime_opt(value: Option<String>, field: &str) -> Result<Option<DateTime<Utc>>> {
    match value {
        Some(value) => Ok(Some(parse_datetime(&value, field)?)),
        None => Ok(None),
    }
}

fn db_datetime_string(value: DateTime<Utc>) -> String {
    value.format("%Y-%m-%d %H:%M:%S").to_string()
}

fn parse_json(value: &str, field: &str) -> Result<JsonValue> {
    serde_json::from_str(value).with_context(|| format!("invalid {field} json"))
}

fn parse_json_opt(value: Option<String>, field: &str) -> Result<Option<JsonValue>> {
    match value {
        Some(value) => Ok(Some(parse_json(&value, field)?)),
        None => Ok(None),
    }
}

fn row_get_opt_string(row: &AnyRow, field: &str) -> Result<Option<String>> {
    let raw = row.try_get_raw(field)?;
    if raw.type_info().name() == "NULL" {
        return Ok(None);
    }
    let value = ValueRef::to_owned(&raw).try_decode::<String>()?;
    Ok(Some(value))
}

fn row_get_i64_opt(row: &AnyRow, field: &str) -> Result<Option<i64>> {
    let raw = row.try_get_raw(field)?;
    if raw.type_info().name() == "NULL" {
        return Ok(None);
    }
    if let Ok(value) = row.try_get::<i64, _>(field) {
        return Ok(Some(value));
    }
    if let Ok(value) = row.try_get::<i32, _>(field) {
        return Ok(Some(value as i64));
    }
    let value = ValueRef::to_owned(&raw).try_decode::<String>()?;
    Ok(Some(value.parse::<i64>().with_context(|| {
        format!("invalid integer value for {field}: {value}")
    })?))
}

fn row_get_f64_opt(row: &AnyRow, field: &str) -> Result<Option<f64>> {
    let raw = row.try_get_raw(field)?;
    if raw.type_info().name() == "NULL" {
        return Ok(None);
    }
    if let Ok(value) = row.try_get::<f64, _>(field) {
        return Ok(Some(value));
    }
    if let Ok(value) = row.try_get::<f32, _>(field) {
        return Ok(Some(value as f64));
    }
    let value = ValueRef::to_owned(&raw).try_decode::<String>()?;
    Ok(Some(value.parse::<f64>().with_context(|| {
        format!("invalid float value for {field}: {value}")
    })?))
}

fn row_get_bool(row: &AnyRow, field: &str) -> Result<bool> {
    if let Ok(value) = row.try_get::<bool, _>(field) {
        return Ok(value);
    }
    if let Ok(value) = row.try_get::<i64, _>(field) {
        return Ok(value != 0);
    }
    if let Ok(value) = row.try_get::<i32, _>(field) {
        return Ok(value != 0);
    }
    let value: String = row
        .try_get(field)
        .with_context(|| format!("missing {field}"))?;
    Ok(matches!(value.as_str(), "1" | "true" | "TRUE"))
}

fn row_get_bool_opt(row: &AnyRow, field: &str) -> Result<Option<bool>> {
    let raw = row.try_get_raw(field)?;
    if raw.type_info().name() == "NULL" {
        return Ok(None);
    }
    row_get_bool(row, field).map(Some)
}

macro_rules! release_columns {
    () => {
        "release_id,
subscription_id,
source_provider_id,
source_extension_id,
owner_id,
media_type,
title,
release_title,
source,
source_kind,
CAST(info_hash AS TEXT) AS info_hash,
fingerprint,
release_kind,
resolver_kind,
resolver_version,
confidence,
score,
CAST(selected_route_logical_id AS TEXT) AS selected_route_logical_id,
CAST(selected_provider_id AS TEXT) AS selected_provider_id,
CAST(download_id AS TEXT) AS download_id,
CAST(remote_release_id AS TEXT) AS remote_release_id,
state,
CAST(state_reason AS TEXT) AS state_reason,
CAST(selected_candidate_json AS TEXT) AS selected_candidate_json,
CAST(coverage_plan_json AS TEXT) AS coverage_plan_json,
CAST(created_at AS TEXT) AS created_at,
CAST(updated_at AS TEXT) AS updated_at"
    };
}

const RELEASE_SELECT_BY_ID: &str = concat!(
    "SELECT ",
    release_columns!(),
    " FROM acquisition_releases WHERE release_id = ? LIMIT 1"
);

const RELEASE_SELECT_BY_FINGERPRINT: &str = concat!(
    "SELECT ",
    release_columns!(),
    " FROM acquisition_releases WHERE owner_id = ? AND source_extension_id = ? AND fingerprint = ? LIMIT 1"
);
const RELEASE_SELECT_BY_DOWNLOAD_ID: &str = concat!(
    "SELECT ",
    release_columns!(),
    " FROM acquisition_releases WHERE download_id = ? ORDER BY updated_at DESC LIMIT 1"
);
const RELEASE_SELECT_RECENT: &str = concat!(
    "SELECT ",
    release_columns!(),
    " FROM acquisition_releases ORDER BY updated_at DESC LIMIT ?"
);
const RELEASE_SELECT_BY_SUBSCRIPTION: &str = concat!(
    "SELECT ",
    release_columns!(),
    " FROM acquisition_releases WHERE subscription_id = ? ORDER BY updated_at DESC LIMIT ?"
);
const RELEASE_SELECT_BY_STATE: &str = concat!(
    "SELECT ",
    release_columns!(),
    " FROM acquisition_releases WHERE state = ? ORDER BY updated_at DESC LIMIT ?"
);
const RELEASE_SELECT_BY_SUBSCRIPTION_AND_STATE: &str = concat!(
    "SELECT ",
    release_columns!(),
    " FROM acquisition_releases WHERE subscription_id = ? AND state = ? ORDER BY updated_at DESC LIMIT ?"
);

macro_rules! release_file_columns {
    () => {
        "release_file_id,
release_id,
file_index,
CAST(file_id AS TEXT) AS file_id,
CAST(provider_file_id AS TEXT) AS provider_file_id,
path,
basename,
size_bytes,
CAST(selectable AS INTEGER) AS selectable,
CAST(selected AS INTEGER) AS selected,
CAST(parsed_title AS TEXT) AS parsed_title,
parsed_season_number,
parsed_episode_number,
parsed_episode_end_number,
parsed_absolute_episode_number,
parsed_absolute_episode_end_number,
CAST(parsed_air_date AS TEXT) AS parsed_air_date,
CAST(parsed_quality AS TEXT) AS parsed_quality,
CAST(parsed_language AS TEXT) AS parsed_language,
CAST(parsed_release_group AS TEXT) AS parsed_release_group,
parser_confidence,
CAST(parser_reason AS TEXT) AS parser_reason,
CAST(raw_json AS TEXT) AS raw_json,
CAST(provider_metadata_json AS TEXT) AS provider_metadata_json,
CAST(created_at AS TEXT) AS created_at,
CAST(updated_at AS TEXT) AS updated_at"
    };
}

const RELEASE_FILE_SELECT_BY_ID: &str = concat!(
    "SELECT ",
    release_file_columns!(),
    " FROM acquisition_release_files WHERE release_file_id = ? LIMIT 1"
);
const RELEASE_FILE_SELECT_BY_RELEASE: &str = concat!(
    "SELECT ",
    release_file_columns!(),
    " FROM acquisition_release_files WHERE release_id = ? ORDER BY file_index, path"
);
const RELEASE_FILE_SELECT_BY_FILE_ID: &str = concat!(
    "SELECT ",
    release_file_columns!(),
    " FROM acquisition_release_files WHERE release_id = ? AND file_id = ? LIMIT 1"
);
const RELEASE_FILE_SELECT_BY_PROVIDER_FILE_ID: &str = concat!(
    "SELECT ",
    release_file_columns!(),
    " FROM acquisition_release_files WHERE release_id = ? AND provider_file_id = ? LIMIT 1"
);
const RELEASE_FILE_SELECT_BY_FILE_INDEX: &str = concat!(
    "SELECT ",
    release_file_columns!(),
    " FROM acquisition_release_files WHERE release_id = ? AND file_index = ? LIMIT 1"
);
const RELEASE_FILE_SELECT_BY_PATH: &str = concat!(
    "SELECT ",
    release_file_columns!(),
    " FROM acquisition_release_files WHERE release_id = ? AND path = ? LIMIT 1"
);

macro_rules! release_coverage_columns {
    () => {
        "coverage_id,
release_id,
CAST(release_file_id AS TEXT) AS release_file_id,
target_id,
coverage_kind,
confidence,
score,
CAST(reason AS TEXT) AS reason,
state,
CAST(verified_by AS TEXT) AS verified_by,
CAST(created_at AS TEXT) AS created_at,
CAST(updated_at AS TEXT) AS updated_at"
    };
}

const RELEASE_COVERAGE_SELECT_BY_ID: &str = concat!(
    "SELECT ",
    release_coverage_columns!(),
    " FROM acquisition_release_coverage WHERE coverage_id = ? LIMIT 1"
);
const RELEASE_COVERAGE_SELECT_BY_RELEASE: &str = concat!(
    "SELECT ",
    release_coverage_columns!(),
    " FROM acquisition_release_coverage WHERE release_id = ? ORDER BY target_id, release_file_id"
);
const RELEASE_COVERAGE_SELECT_BY_FILE_TARGET: &str = concat!(
    "SELECT ",
    release_coverage_columns!(),
    " FROM acquisition_release_coverage WHERE release_id = ? AND target_id = ? AND release_file_id = ? LIMIT 1"
);
const RELEASE_COVERAGE_SELECT_BY_TARGET_WITHOUT_FILE: &str = concat!(
    "SELECT ",
    release_coverage_columns!(),
    " FROM acquisition_release_coverage WHERE release_id = ? AND target_id = ? AND release_file_id IS NULL LIMIT 1"
);

macro_rules! release_job_columns {
    () => {
        "release_job_id,
release_id,
route_logical_id,
CAST(provider_id AS TEXT) AS provider_id,
CAST(download_id AS TEXT) AS download_id,
CAST(remote_release_id AS TEXT) AS remote_release_id,
state,
CAST(state_reason AS TEXT) AS state_reason,
CAST(active AS INTEGER) AS active,
CAST(started_at AS TEXT) AS started_at,
CAST(completed_at AS TEXT) AS completed_at,
CAST(created_at AS TEXT) AS created_at,
CAST(updated_at AS TEXT) AS updated_at"
    };
}

const RELEASE_JOB_SELECT_BY_ID: &str = concat!(
    "SELECT ",
    release_job_columns!(),
    " FROM acquisition_release_jobs WHERE release_job_id = ? LIMIT 1"
);
const RELEASE_JOB_SELECT_BY_RELEASE: &str = concat!(
    "SELECT ",
    release_job_columns!(),
    " FROM acquisition_release_jobs WHERE release_id = ? ORDER BY created_at"
);
const RELEASE_JOB_SELECT_BY_DOWNLOAD_ID: &str = concat!(
    "SELECT ",
    release_job_columns!(),
    " FROM acquisition_release_jobs WHERE release_id = ? AND download_id = ? LIMIT 1"
);
const RELEASE_JOB_SELECT_BY_REMOTE_ID: &str = concat!(
    "SELECT ",
    release_job_columns!(),
    " FROM acquisition_release_jobs WHERE release_id = ? AND remote_release_id = ? LIMIT 1"
);

macro_rules! anime_graph_columns {
    () => {
        "graph_snapshot_id,
CAST(subscription_id AS TEXT) AS subscription_id,
owner_id,
media_type,
anilist_root_id,
anilist_season_id,
CAST(anilist_status AS TEXT) AS anilist_status,
CAST(anilist_next_airing_at AS TEXT) AS anilist_next_airing_at,
tvdb_series_id,
anidb_anime_id,
fingerprint,
CAST(graph_json AS TEXT) AS graph_json,
CAST(aliases_json AS TEXT) AS aliases_json,
CAST(created_at AS TEXT) AS created_at,
CAST(updated_at AS TEXT) AS updated_at"
    };
}

const ANIME_GRAPH_SELECT_BY_ID: &str = concat!(
    "SELECT ",
    anime_graph_columns!(),
    " FROM acquisition_anime_graph_snapshots WHERE graph_snapshot_id = ? LIMIT 1"
);
const ANIME_GRAPH_SELECT_BY_SUBSCRIPTION_FINGERPRINT: &str = concat!(
    "SELECT ",
    anime_graph_columns!(),
    " FROM acquisition_anime_graph_snapshots WHERE subscription_id = ? AND fingerprint = ? LIMIT 1"
);
const ANIME_GRAPH_SELECT_BY_OWNER_FINGERPRINT: &str = concat!(
    "SELECT ",
    anime_graph_columns!(),
    " FROM acquisition_anime_graph_snapshots WHERE subscription_id IS NULL AND owner_id = ? AND fingerprint = ? LIMIT 1"
);

macro_rules! anime_candidate_parse_columns {
    () => {
        "candidate_parse_id,
release_id,
CAST(source_provider_id AS TEXT) AS source_provider_id,
CAST(source_candidate_id AS TEXT) AS source_candidate_id,
release_title,
CAST(normalized_title AS TEXT) AS normalized_title,
CAST(parsed_json AS TEXT) AS parsed_json,
confidence,
CAST(review_reasons_json AS TEXT) AS review_reasons_json,
CAST(created_at AS TEXT) AS created_at,
CAST(updated_at AS TEXT) AS updated_at"
    };
}

const ANIME_CANDIDATE_PARSE_SELECT_BY_ID: &str = concat!(
    "SELECT ",
    anime_candidate_parse_columns!(),
    " FROM acquisition_anime_candidate_parses WHERE candidate_parse_id = ? LIMIT 1"
);
const ANIME_CANDIDATE_PARSE_SELECT_BY_SOURCE_ID: &str = concat!(
    "SELECT ",
    anime_candidate_parse_columns!(),
    " FROM acquisition_anime_candidate_parses WHERE release_id = ? AND source_candidate_id = ? LIMIT 1"
);
const ANIME_CANDIDATE_PARSE_SELECT_BY_RELEASE_TITLE: &str = concat!(
    "SELECT ",
    anime_candidate_parse_columns!(),
    " FROM acquisition_anime_candidate_parses WHERE release_id = ? AND source_candidate_id IS NULL AND release_title = ? LIMIT 1"
);

macro_rules! file_hash_columns {
    () => {
        "file_hash_id,
CAST(release_file_id AS TEXT) AS release_file_id,
CAST(local_file_id AS TEXT) AS local_file_id,
file_path,
size_bytes,
CAST(mtime_fingerprint AS TEXT) AS mtime_fingerprint,
CAST(ed2k AS TEXT) AS ed2k,
CAST(crc32 AS TEXT) AS crc32,
hash_status,
CAST(hash_computed_at AS TEXT) AS hash_computed_at,
CAST(hash_invalidated_at AS TEXT) AS hash_invalidated_at,
CAST(filename_history_json AS TEXT) AS filename_history_json,
CAST(created_at AS TEXT) AS created_at,
CAST(updated_at AS TEXT) AS updated_at"
    };
}

const FILE_HASH_SELECT_BY_ID: &str = concat!(
    "SELECT ",
    file_hash_columns!(),
    " FROM acquisition_file_hashes WHERE file_hash_id = ? LIMIT 1"
);
const FILE_HASH_SELECT_BY_PATH: &str = concat!(
    "SELECT ",
    file_hash_columns!(),
    " FROM acquisition_file_hashes WHERE file_path = ? LIMIT 1"
);
const FILE_HASH_SELECT_BY_LOCAL_FILE_ID: &str = concat!(
    "SELECT ",
    file_hash_columns!(),
    " FROM acquisition_file_hashes WHERE local_file_id = ? LIMIT 1"
);
const FILE_HASH_SELECT_BY_ED2K_SIZE: &str = concat!(
    "SELECT ",
    file_hash_columns!(),
    " FROM acquisition_file_hashes WHERE ed2k = ? AND size_bytes = ? LIMIT 1"
);
const FILE_HASH_SELECT_WORK: &str = concat!(
    "SELECT ",
    file_hash_columns!(),
    " FROM acquisition_file_hashes WHERE hash_status IN ('pending', 'invalidated') ORDER BY updated_at, created_at LIMIT ?"
);

macro_rules! anidb_file_cache_columns {
    () => {
        "lookup_key,
ed2k,
size_bytes,
lookup_status,
anidb_file_id,
anidb_anime_id,
CAST(anidb_episode_ids_json AS TEXT) AS anidb_episode_ids_json,
anidb_group_id,
CAST(anidb_group_name AS TEXT) AS anidb_group_name,
CAST(anidb_group_short_name AS TEXT) AS anidb_group_short_name,
anidb_version,
CAST(anidb_source AS TEXT) AS anidb_source,
CAST(anidb_quality AS TEXT) AS anidb_quality,
CAST(anidb_audio_languages_json AS TEXT) AS anidb_audio_languages_json,
CAST(anidb_subtitle_languages_json AS TEXT) AS anidb_subtitle_languages_json,
CAST(anidb_state_flags_json AS TEXT) AS anidb_state_flags_json,
CAST(anidb_original_filename AS TEXT) AS anidb_original_filename,
CAST(released_at AS TEXT) AS released_at,
CAST(raw_response AS TEXT) AS raw_response,
CAST(positive_cached_at AS TEXT) AS positive_cached_at,
CAST(negative_cached_until AS TEXT) AS negative_cached_until,
CAST(last_lookup_attempt_at AS TEXT) AS last_lookup_attempt_at,
CAST(created_at AS TEXT) AS created_at,
CAST(updated_at AS TEXT) AS updated_at"
    };
}

const ANIDB_FILE_CACHE_SELECT_BY_KEY: &str = concat!(
    "SELECT ",
    anidb_file_cache_columns!(),
    " FROM acquisition_anidb_file_cache WHERE lookup_key = ? LIMIT 1"
);

macro_rules! anidb_file_xref_columns {
    () => {
        "xref_id,
lookup_key,
CAST(release_file_id AS TEXT) AS release_file_id,
anidb_file_id,
anidb_anime_id,
anidb_episode_id,
episode_type,
percentage_start,
percentage_end,
episode_order,
provider,
confidence,
CAST(is_manual_override AS INTEGER) AS is_manual_override,
CAST(created_from_release_id AS TEXT) AS created_from_release_id,
CAST(created_from_target_id AS TEXT) AS created_from_target_id,
CAST(created_at AS TEXT) AS created_at,
CAST(updated_at AS TEXT) AS updated_at"
    };
}

const ANIDB_FILE_XREF_SELECT_BY_ID: &str = concat!(
    "SELECT ",
    anidb_file_xref_columns!(),
    " FROM acquisition_anidb_file_xrefs WHERE xref_id = ? LIMIT 1"
);
const ANIDB_FILE_XREF_SELECT_BY_IDENTITY: &str = concat!(
    "SELECT ",
    anidb_file_xref_columns!(),
    " FROM acquisition_anidb_file_xrefs WHERE lookup_key = ? AND anidb_episode_id = ? AND percentage_start = ? AND percentage_end = ? AND episode_order = ? LIMIT 1"
);
const ANIDB_FILE_XREF_SELECT_BY_LOOKUP: &str = concat!(
    "SELECT ",
    anidb_file_xref_columns!(),
    " FROM acquisition_anidb_file_xrefs WHERE lookup_key = ? ORDER BY episode_order, anidb_episode_id"
);

macro_rules! anime_match_attempt_columns {
    () => {
        "match_attempt_id,
CAST(release_id AS TEXT) AS release_id,
CAST(release_file_id AS TEXT) AS release_file_id,
CAST(attempted_providers_json AS TEXT) AS attempted_providers_json,
CAST(selected_provider AS TEXT) AS selected_provider,
CAST(ed2k AS TEXT) AS ed2k,
size_bytes,
CAST(candidate_fingerprint AS TEXT) AS candidate_fingerprint,
CAST(planned_targets_json AS TEXT) AS planned_targets_json,
CAST(verified_targets_json AS TEXT) AS verified_targets_json,
outcome,
CAST(rejection_reason AS TEXT) AS rejection_reason,
CAST(created_at AS TEXT) AS created_at,
CAST(updated_at AS TEXT) AS updated_at"
    };
}

const ANIME_MATCH_ATTEMPT_SELECT_BY_ID: &str = concat!(
    "SELECT ",
    anime_match_attempt_columns!(),
    " FROM acquisition_anime_match_attempts WHERE match_attempt_id = ? LIMIT 1"
);
const ANIME_MATCH_ATTEMPT_SELECT_BY_RELEASE: &str = concat!(
    "SELECT ",
    anime_match_attempt_columns!(),
    " FROM acquisition_anime_match_attempts WHERE release_id = ? ORDER BY created_at"
);

macro_rules! anime_identity_mismatch_columns {
    () => {
        "mismatch_id,
CAST(release_id AS TEXT) AS release_id,
CAST(release_file_id AS TEXT) AS release_file_id,
CAST(target_id AS TEXT) AS target_id,
CAST(planned_target_json AS TEXT) AS planned_target_json,
CAST(verified_identity_json AS TEXT) AS verified_identity_json,
provider,
confidence,
state,
CAST(reason AS TEXT) AS reason,
CAST(created_at AS TEXT) AS created_at,
CAST(updated_at AS TEXT) AS updated_at"
    };
}

const ANIME_IDENTITY_MISMATCH_SELECT_BY_ID: &str = concat!(
    "SELECT ",
    anime_identity_mismatch_columns!(),
    " FROM acquisition_anime_identity_mismatches WHERE mismatch_id = ? LIMIT 1"
);
const ANIME_IDENTITY_MISMATCH_SELECT_BY_RELEASE: &str = concat!(
    "SELECT ",
    anime_identity_mismatch_columns!(),
    " FROM acquisition_anime_identity_mismatches WHERE release_id = ? ORDER BY created_at"
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        acquisition::{
            release_resolution::fingerprint::{ReleaseFingerprintInput, build_release_fingerprint},
            subscriptions::{
                AcquisitionMonitorPolicy, AcquisitionRoutePolicy, NewAcquisitionSubscription,
                NewAcquisitionTarget, create_subscription, upsert_subscription_targets,
            },
        },
        config::DatabaseConfig,
        db::Database,
        download_broker::{DEBRID_DEFAULT_LOGICAL_ID, TORRENT_DEFAULT_LOGICAL_ID},
    };
    use serde_json::json;

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

    async fn table_columns(pool: &AnyPool, table: &str) -> Result<Vec<String>> {
        let rows = sqlx::query(&format!("PRAGMA table_info({table})"))
            .fetch_all(pool)
            .await?;
        rows.into_iter()
            .map(|row| row.try_get::<String, _>("name").map_err(Into::into))
            .collect()
    }

    fn sample_release(fingerprint: String) -> NewAcquisitionRelease {
        NewAcquisitionRelease {
            release_id: None,
            subscription_id: None,
            source_provider_id: None,
            source_extension_id: "elixir.marketplace.torrentio".to_string(),
            owner_id: "default".to_string(),
            media_type: MediaType::Series,
            title: "Show".to_string(),
            release_title: "Show.S01.COMPLETE.1080p".to_string(),
            source: "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567".to_string(),
            source_kind: "magnet".to_string(),
            info_hash: Some("0123456789abcdef0123456789abcdef01234567".to_string()),
            fingerprint,
            release_kind: ReleaseKind::SeasonPack,
            resolver_kind: ReleaseResolverKind::Unresolved,
            resolver_version: "rr1".to_string(),
            confidence: ReleaseConfidence::Low,
            score: Some(42.0),
            selected_route_logical_id: None,
            selected_provider_id: None,
            download_id: None,
            remote_release_id: None,
            state: AcquisitionReleaseState::Candidate,
            state_reason: Some("candidate only".to_string()),
            selected_candidate: Some(json!({ "title": "Show.S01.COMPLETE.1080p" })),
            coverage_plan: None,
        }
    }

    #[tokio::test]
    async fn generic_debrid_staging_migration_columns_exist() -> Result<()> {
        let database = setup_db().await?;
        let job_columns = table_columns(&database.pool, "debrid_download_jobs").await?;
        for column in [
            "provider_implementation",
            "remote_release_id",
            "remote_release_status",
            "provider_capabilities_json",
            "selection_mode",
            "selected_file_ids_json",
            "skipped_file_ids_json",
            "selection_error",
            "release_id",
        ] {
            assert!(
                job_columns.iter().any(|candidate| candidate == column),
                "missing debrid_download_jobs.{column}"
            );
        }

        let file_columns = table_columns(&database.pool, "acquisition_release_files").await?;
        for column in ["provider_file_id", "selected", "provider_metadata_json"] {
            assert!(
                file_columns.iter().any(|candidate| candidate == column),
                "missing acquisition_release_files.{column}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn release_upsert_dedupes_by_owner_extension_fingerprint() -> Result<()> {
        let database = setup_db().await?;
        let fingerprint = build_release_fingerprint(&ReleaseFingerprintInput {
            source_kind: "magnet",
            source: "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
            info_hash: None,
            release_title: "Show.S01.COMPLETE.1080p",
            size_bytes: Some(10 * 1024 * 1024 * 1024),
            source_provider_id: None,
        });

        let created = upsert_release(&database.pool, sample_release(fingerprint.clone())).await?;
        let updated = upsert_release(
            &database.pool,
            NewAcquisitionRelease {
                state: AcquisitionReleaseState::Planned,
                confidence: ReleaseConfidence::Medium,
                coverage_plan: Some(json!({ "targets": ["S01E01"] })),
                ..sample_release(fingerprint.clone())
            },
        )
        .await?;

        assert_eq!(created.release_id, updated.release_id);
        assert_eq!(updated.state, AcquisitionReleaseState::Planned);
        assert_eq!(updated.confidence, ReleaseConfidence::Medium);
        assert_eq!(
            updated.coverage_plan,
            Some(json!({ "targets": ["S01E01"] }))
        );

        let fetched = get_release_by_fingerprint(
            &database.pool,
            "default",
            "elixir.marketplace.torrentio",
            &fingerprint,
        )
        .await?
        .expect("release by fingerprint");
        assert_eq!(fetched.release_id, created.release_id);
        Ok(())
    }

    #[tokio::test]
    async fn release_file_coverage_and_job_upserts_are_stable() -> Result<()> {
        let database = setup_db().await?;
        let subscription = create_subscription(
            &database.pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Series,
                title: "Show".to_string(),
                year: Some(2026),
                external_ids: None,
                monitor_policy: AcquisitionMonitorPolicy::AllMissing,
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
                season_number: Some(1),
                episode_number: Some(1),
                ..empty_target()
            }],
        )
        .await?;
        let release = upsert_release(
            &database.pool,
            NewAcquisitionRelease {
                subscription_id: Some(subscription.subscription_id),
                ..sample_release("v1:magnet:test:test:1".to_string())
            },
        )
        .await?;

        let file = upsert_release_file(
            &database.pool,
            NewAcquisitionReleaseFile {
                release_file_id: None,
                release_id: release.release_id,
                file_index: Some(0),
                file_id: Some("rd-file-0".to_string()),
                provider_file_id: Some("rd-file-0".to_string()),
                path: "Show/Season 01/Show.S01E01.mkv".to_string(),
                basename: None,
                size_bytes: Some(1024),
                selectable: true,
                selected: Some(false),
                parsed_title: Some("Show".to_string()),
                parsed_season_number: Some(1),
                parsed_episode_number: Some(1),
                parsed_episode_end_number: None,
                parsed_absolute_episode_number: None,
                parsed_absolute_episode_end_number: None,
                parsed_air_date: None,
                parsed_quality: Some("1080p".to_string()),
                parsed_language: None,
                parsed_release_group: None,
                parser_confidence: ReleaseConfidence::High,
                parser_reason: Some("exact SxxEyy".to_string()),
                raw: Some(json!({ "id": "rd-file-0" })),
                provider_metadata: Some(json!({ "providerFileId": "rd-file-0" })),
            },
        )
        .await?;
        let updated_file = upsert_release_file(
            &database.pool,
            NewAcquisitionReleaseFile {
                size_bytes: Some(2048),
                ..NewAcquisitionReleaseFile {
                    release_file_id: None,
                    release_id: release.release_id,
                    file_index: Some(0),
                    file_id: Some("rd-file-0".to_string()),
                    provider_file_id: Some("rd-file-0".to_string()),
                    path: "Show/Season 01/Show.S01E01.mkv".to_string(),
                    basename: None,
                    size_bytes: Some(1024),
                    selectable: true,
                    selected: Some(true),
                    parsed_title: Some("Show".to_string()),
                    parsed_season_number: Some(1),
                    parsed_episode_number: Some(1),
                    parsed_episode_end_number: None,
                    parsed_absolute_episode_number: None,
                    parsed_absolute_episode_end_number: None,
                    parsed_air_date: None,
                    parsed_quality: Some("1080p".to_string()),
                    parsed_language: None,
                    parsed_release_group: None,
                    parser_confidence: ReleaseConfidence::High,
                    parser_reason: Some("exact SxxEyy".to_string()),
                    raw: Some(json!({ "id": "rd-file-0" })),
                    provider_metadata: Some(
                        json!({ "providerFileId": "rd-file-0", "selected": true }),
                    ),
                }
            },
        )
        .await?;
        assert_eq!(file.release_file_id, updated_file.release_file_id);
        assert_eq!(updated_file.size_bytes, Some(2048));
        assert_eq!(updated_file.provider_file_id.as_deref(), Some("rd-file-0"));
        assert_eq!(updated_file.selected, Some(true));
        assert_eq!(
            updated_file.provider_metadata,
            Some(json!({ "providerFileId": "rd-file-0", "selected": true }))
        );

        let coverage = upsert_release_coverage(
            &database.pool,
            NewAcquisitionReleaseCoverage {
                coverage_id: None,
                release_id: release.release_id,
                release_file_id: Some(file.release_file_id),
                target_id: targets[0].target_id,
                coverage_kind: ReleaseCoverageKind::SingleEpisode,
                confidence: ReleaseConfidence::High,
                score: Some(1.0),
                reason: Some("exact file".to_string()),
                state: ReleaseCoverageState::Planned,
                verified_by: None,
            },
        )
        .await?;
        let selected = upsert_release_coverage(
            &database.pool,
            NewAcquisitionReleaseCoverage {
                state: ReleaseCoverageState::Selected,
                ..NewAcquisitionReleaseCoverage {
                    coverage_id: None,
                    release_id: release.release_id,
                    release_file_id: Some(file.release_file_id),
                    target_id: targets[0].target_id,
                    coverage_kind: ReleaseCoverageKind::SingleEpisode,
                    confidence: ReleaseConfidence::High,
                    score: Some(1.0),
                    reason: Some("exact file".to_string()),
                    state: ReleaseCoverageState::Planned,
                    verified_by: None,
                }
            },
        )
        .await?;
        assert_eq!(coverage.coverage_id, selected.coverage_id);
        assert_eq!(selected.state, ReleaseCoverageState::Selected);

        let job = upsert_release_job(
            &database.pool,
            NewAcquisitionReleaseJob {
                release_job_id: None,
                release_id: release.release_id,
                route_logical_id: DEBRID_DEFAULT_LOGICAL_ID.to_string(),
                provider_id: None,
                download_id: Some("rd-job".to_string()),
                remote_release_id: Some("rd-remote".to_string()),
                state: ReleaseJobState::Submitted,
                state_reason: None,
                active: true,
                started_at: Some(Utc::now()),
                completed_at: None,
            },
        )
        .await?;
        let completed = update_release_job_state(
            &database.pool,
            job.release_job_id,
            ReleaseJobStateUpdate {
                state: ReleaseJobState::Completed,
                state_reason: Some("done".to_string()),
                active: Some(false),
                completed_at: Some(Utc::now()),
                ..Default::default()
            },
        )
        .await?
        .expect("updated job");
        assert_eq!(completed.release_job_id, job.release_job_id);
        assert_eq!(completed.state, ReleaseJobState::Completed);
        assert!(!completed.active);
        assert!(completed.completed_at.is_some());

        assert_eq!(
            list_release_files(&database.pool, release.release_id)
                .await?
                .len(),
            1
        );
        assert_eq!(
            list_release_coverage(&database.pool, release.release_id)
                .await?
                .len(),
            1
        );
        assert_eq!(
            list_release_jobs(&database.pool, release.release_id)
                .await?
                .len(),
            1
        );
        Ok(())
    }

    #[tokio::test]
    async fn release_jobs_upsert_by_download_id() -> Result<()> {
        let database = setup_db().await?;
        let release = upsert_release(
            &database.pool,
            sample_release("v1:magnet:job:test:1".to_string()),
        )
        .await?;
        let first = upsert_release_job(
            &database.pool,
            NewAcquisitionReleaseJob {
                release_job_id: None,
                release_id: release.release_id,
                route_logical_id: TORRENT_DEFAULT_LOGICAL_ID.to_string(),
                provider_id: None,
                download_id: Some("torrent-hash".to_string()),
                remote_release_id: None,
                state: ReleaseJobState::Staging,
                state_reason: None,
                active: true,
                started_at: None,
                completed_at: None,
            },
        )
        .await?;
        let second = upsert_release_job(
            &database.pool,
            NewAcquisitionReleaseJob {
                state: ReleaseJobState::Downloading,
                ..NewAcquisitionReleaseJob {
                    release_job_id: None,
                    release_id: release.release_id,
                    route_logical_id: TORRENT_DEFAULT_LOGICAL_ID.to_string(),
                    provider_id: None,
                    download_id: Some("torrent-hash".to_string()),
                    remote_release_id: None,
                    state: ReleaseJobState::Staging,
                    state_reason: None,
                    active: true,
                    started_at: None,
                    completed_at: None,
                }
            },
        )
        .await?;
        assert_eq!(first.release_job_id, second.release_job_id);
        assert_eq!(second.state, ReleaseJobState::Downloading);
        Ok(())
    }

    #[tokio::test]
    async fn active_release_job_counts_are_scoped_by_route_and_subscription() -> Result<()> {
        let database = setup_db().await?;
        let subscription_a = create_subscription(
            &database.pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Anime,
                title: "Anime A".to_string(),
                year: Some(2026),
                external_ids: None,
                monitor_policy: AcquisitionMonitorPolicy::AllMissing,
                route_policy: AcquisitionRoutePolicy::DebridFirst,
                source_provider_id: None,
                release_delay_seconds: None,
                quality_profile: None,
                metadata_refresh_after: None,
                candidate_search_after: None,
            },
        )
        .await?;
        let subscription_b = create_subscription(
            &database.pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Anime,
                title: "Anime B".to_string(),
                year: Some(2026),
                external_ids: None,
                monitor_policy: AcquisitionMonitorPolicy::AllMissing,
                route_policy: AcquisitionRoutePolicy::DebridFirst,
                source_provider_id: None,
                release_delay_seconds: None,
                quality_profile: None,
                metadata_refresh_after: None,
                candidate_search_after: None,
            },
        )
        .await?;

        let release_a = upsert_release(
            &database.pool,
            NewAcquisitionRelease {
                subscription_id: Some(subscription_a.subscription_id),
                ..sample_release("v1:magnet:counts:a".to_string())
            },
        )
        .await?;
        let release_a_completed = upsert_release(
            &database.pool,
            NewAcquisitionRelease {
                subscription_id: Some(subscription_a.subscription_id),
                ..sample_release("v1:magnet:counts:a-complete".to_string())
            },
        )
        .await?;
        let release_b = upsert_release(
            &database.pool,
            NewAcquisitionRelease {
                subscription_id: Some(subscription_b.subscription_id),
                ..sample_release("v1:magnet:counts:b".to_string())
            },
        )
        .await?;

        for (release_id, route, state, active, download_id) in [
            (
                release_a.release_id,
                DEBRID_DEFAULT_LOGICAL_ID,
                ReleaseJobState::Submitted,
                true,
                "rd-a",
            ),
            (
                release_a.release_id,
                TORRENT_DEFAULT_LOGICAL_ID,
                ReleaseJobState::Downloading,
                true,
                "torrent-a",
            ),
            (
                release_a_completed.release_id,
                DEBRID_DEFAULT_LOGICAL_ID,
                ReleaseJobState::Completed,
                true,
                "rd-a-complete",
            ),
            (
                release_b.release_id,
                DEBRID_DEFAULT_LOGICAL_ID,
                ReleaseJobState::Materializing,
                true,
                "rd-b",
            ),
        ] {
            upsert_release_job(
                &database.pool,
                NewAcquisitionReleaseJob {
                    release_job_id: None,
                    release_id,
                    route_logical_id: route.to_string(),
                    provider_id: None,
                    download_id: Some(download_id.to_string()),
                    remote_release_id: None,
                    state,
                    state_reason: None,
                    active,
                    started_at: Some(Utc::now()),
                    completed_at: None,
                },
            )
            .await?;
        }

        assert_eq!(count_active_release_jobs(&database.pool).await?, 3);
        assert_eq!(
            count_active_release_jobs_by_subscription(
                &database.pool,
                subscription_a.subscription_id,
            )
            .await?,
            2
        );
        assert_eq!(
            count_active_release_jobs_by_subscription(
                &database.pool,
                subscription_b.subscription_id,
            )
            .await?,
            1
        );
        assert_eq!(
            count_active_release_jobs_by_route(&database.pool, DEBRID_DEFAULT_LOGICAL_ID).await?,
            2
        );
        assert_eq!(
            count_active_release_jobs_by_route(&database.pool, TORRENT_DEFAULT_LOGICAL_ID).await?,
            1
        );
        assert_eq!(
            count_active_release_jobs_by_subscription_route(
                &database.pool,
                subscription_a.subscription_id,
                DEBRID_DEFAULT_LOGICAL_ID,
            )
            .await?,
            1
        );
        assert_eq!(
            count_active_release_jobs_by_subscription_route(
                &database.pool,
                subscription_b.subscription_id,
                DEBRID_DEFAULT_LOGICAL_ID,
            )
            .await?,
            1
        );
        assert_eq!(
            count_active_release_jobs_by_subscription_route(
                &database.pool,
                subscription_a.subscription_id,
                TORRENT_DEFAULT_LOGICAL_ID,
            )
            .await?,
            1
        );
        sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_release_jobs
             SET updated_at = ?
             WHERE download_id = ?",
        )
        .bind(db_datetime_string(Utc::now() - chrono::Duration::hours(8)))
        .bind("torrent-a")
        .execute(&database.pool)
        .await?;
        assert_eq!(
            count_stale_active_release_jobs(
                &database.pool,
                Utc::now() - chrono::Duration::hours(6)
            )
            .await?,
            1
        );
        let jobs = list_release_jobs(&database.pool, release_a.release_id).await?;
        let stale = jobs
            .iter()
            .find(|job| job.download_id.as_deref() == Some("torrent-a"))
            .expect("stale job remains present");
        assert!(stale.active);
        assert_eq!(stale.state, ReleaseJobState::Downloading);
        Ok(())
    }

    #[tokio::test]
    async fn anime_graph_and_candidate_parse_upserts_preserve_provenance() -> Result<()> {
        let database = setup_db().await?;
        let subscription = create_subscription(
            &database.pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Anime,
                title: "Anime Series".to_string(),
                year: Some(2026),
                external_ids: None,
                monitor_policy: AcquisitionMonitorPolicy::AllMissing,
                route_policy: AcquisitionRoutePolicy::DebridFirst,
                source_provider_id: None,
                release_delay_seconds: None,
                quality_profile: None,
                metadata_refresh_after: None,
                candidate_search_after: None,
            },
        )
        .await?;

        let graph = upsert_anime_graph_snapshot(
            &database.pool,
            NewAcquisitionAnimeGraphSnapshot {
                graph_snapshot_id: None,
                subscription_id: Some(subscription.subscription_id),
                owner_id: "default".to_string(),
                media_type: MediaType::Anime,
                anilist_root_id: Some(100),
                anilist_season_id: Some(101),
                anilist_status: Some("RELEASING".to_string()),
                anilist_next_airing_at: Some(Utc::now()),
                tvdb_series_id: Some(200),
                anidb_anime_id: Some(300),
                fingerprint: "anime-graph-v1".to_string(),
                graph: json!({ "targets": [{ "absolute": 1, "season": 1, "episode": 1 }] }),
                aliases: json!(["Anime Series", "Anime Series English"]),
            },
        )
        .await?;
        let updated_graph = upsert_anime_graph_snapshot(
            &database.pool,
            NewAcquisitionAnimeGraphSnapshot {
                anilist_status: Some("FINISHED".to_string()),
                graph: json!({ "targets": [{ "absolute": 1 }, { "absolute": 2 }] }),
                aliases: json!(["Anime Series"]),
                ..NewAcquisitionAnimeGraphSnapshot {
                    graph_snapshot_id: None,
                    subscription_id: Some(subscription.subscription_id),
                    owner_id: "default".to_string(),
                    media_type: MediaType::Anime,
                    anilist_root_id: Some(100),
                    anilist_season_id: Some(101),
                    anilist_status: Some("RELEASING".to_string()),
                    anilist_next_airing_at: None,
                    tvdb_series_id: Some(200),
                    anidb_anime_id: Some(300),
                    fingerprint: "anime-graph-v1".to_string(),
                    graph: json!({}),
                    aliases: json!([]),
                }
            },
        )
        .await?;
        assert_eq!(graph.graph_snapshot_id, updated_graph.graph_snapshot_id);
        assert_eq!(updated_graph.anilist_status.as_deref(), Some("FINISHED"));
        assert_eq!(updated_graph.graph["targets"].as_array().unwrap().len(), 2);

        let release = upsert_release(
            &database.pool,
            NewAcquisitionRelease {
                media_type: MediaType::Anime,
                resolver_kind: ReleaseResolverKind::AnimeShokoStyle,
                resolver_version: "rr3-anime-shoko-style-v0".to_string(),
                ..sample_release("v1:anime:candidate:test:1".to_string())
            },
        )
        .await?;

        let parse = upsert_anime_candidate_parse(
            &database.pool,
            NewAcquisitionAnimeCandidateParse {
                candidate_parse_id: None,
                release_id: release.release_id,
                source_provider_id: None,
                source_candidate_id: Some("candidate-1".to_string()),
                release_title: "[SubsPlease] Anime Series - 01 (1080p)".to_string(),
                normalized_title: Some("Anime Series".to_string()),
                parsed: json!({ "absolute": 1, "releaseGroup": "SubsPlease" }),
                confidence: ReleaseConfidence::Medium,
                review_reasons: json!([]),
            },
        )
        .await?;
        let updated_parse = upsert_anime_candidate_parse(
            &database.pool,
            NewAcquisitionAnimeCandidateParse {
                confidence: ReleaseConfidence::High,
                review_reasons: json!(["anizip_absolute_match"]),
                ..NewAcquisitionAnimeCandidateParse {
                    candidate_parse_id: None,
                    release_id: release.release_id,
                    source_provider_id: None,
                    source_candidate_id: Some("candidate-1".to_string()),
                    release_title: "[SubsPlease] Anime Series - 01 (1080p)".to_string(),
                    normalized_title: Some("Anime Series".to_string()),
                    parsed: json!({ "absolute": 1, "releaseGroup": "SubsPlease" }),
                    confidence: ReleaseConfidence::Medium,
                    review_reasons: json!([]),
                }
            },
        )
        .await?;
        assert_eq!(parse.candidate_parse_id, updated_parse.candidate_parse_id);
        assert_eq!(updated_parse.confidence, ReleaseConfidence::High);
        assert_eq!(
            updated_parse.review_reasons,
            json!(["anizip_absolute_match"])
        );
        Ok(())
    }

    #[tokio::test]
    async fn anime_file_hash_and_anidb_cache_upserts_are_stable() -> Result<()> {
        let database = setup_db().await?;
        let release = upsert_release(
            &database.pool,
            NewAcquisitionRelease {
                media_type: MediaType::Anime,
                resolver_kind: ReleaseResolverKind::AnimeShokoStyle,
                ..sample_release("v1:anime:hash:test:1".to_string())
            },
        )
        .await?;
        let file = upsert_release_file(
            &database.pool,
            NewAcquisitionReleaseFile {
                release_file_id: None,
                release_id: release.release_id,
                file_index: Some(0),
                file_id: Some("file-1".to_string()),
                provider_file_id: Some("file-1".to_string()),
                path: "Anime Series/Anime Series - 01.mkv".to_string(),
                basename: None,
                size_bytes: Some(1234),
                selectable: true,
                selected: None,
                parsed_title: Some("Anime Series".to_string()),
                parsed_season_number: None,
                parsed_episode_number: None,
                parsed_episode_end_number: None,
                parsed_absolute_episode_number: Some(1),
                parsed_absolute_episode_end_number: None,
                parsed_air_date: None,
                parsed_quality: Some("1080p".to_string()),
                parsed_language: Some("japanese".to_string()),
                parsed_release_group: Some("SubsPlease".to_string()),
                parser_confidence: ReleaseConfidence::High,
                parser_reason: Some("absolute episode".to_string()),
                raw: Some(json!({ "index": 0 })),
                provider_metadata: None,
            },
        )
        .await?;

        let hash = upsert_file_hash(
            &database.pool,
            NewAcquisitionFileHash {
                file_hash_id: None,
                release_file_id: Some(file.release_file_id),
                local_file_id: Some("local-file-1".to_string()),
                file_path: "/library/Anime Series/Anime Series - 01.mkv".to_string(),
                size_bytes: 1234,
                mtime_fingerprint: Some("mtime-1".to_string()),
                ed2k: Some("0123456789abcdef0123456789abcdef".to_string()),
                crc32: Some("89ABCDEF".to_string()),
                hash_status: AnimeFileHashStatus::Hashed,
                hash_computed_at: Some(Utc::now()),
                hash_invalidated_at: None,
                filename_history: json!(["Anime Series - 01.mkv"]),
            },
        )
        .await?;
        let invalidated = upsert_file_hash(
            &database.pool,
            NewAcquisitionFileHash {
                size_bytes: 2048,
                hash_status: AnimeFileHashStatus::Invalidated,
                hash_invalidated_at: Some(Utc::now()),
                filename_history: json!(["Anime Series - 01.mkv", "Anime Series - 01v2.mkv"]),
                ..NewAcquisitionFileHash {
                    file_hash_id: None,
                    release_file_id: Some(file.release_file_id),
                    local_file_id: Some("local-file-1".to_string()),
                    file_path: "/library/Anime Series/Anime Series - 01.mkv".to_string(),
                    size_bytes: 1234,
                    mtime_fingerprint: Some("mtime-2".to_string()),
                    ed2k: Some("0123456789abcdef0123456789abcdef".to_string()),
                    crc32: Some("89ABCDEF".to_string()),
                    hash_status: AnimeFileHashStatus::Hashed,
                    hash_computed_at: Some(Utc::now()),
                    hash_invalidated_at: None,
                    filename_history: json!([]),
                }
            },
        )
        .await?;
        assert_eq!(hash.file_hash_id, invalidated.file_hash_id);
        assert_eq!(invalidated.size_bytes, 2048);
        assert_eq!(invalidated.hash_status, AnimeFileHashStatus::Invalidated);
        assert_eq!(
            get_file_hash_by_ed2k_size(&database.pool, "0123456789abcdef0123456789abcdef", 2048,)
                .await?
                .expect("hash by ed2k:size")
                .file_hash_id,
            hash.file_hash_id
        );

        let cache = upsert_anidb_file_cache(
            &database.pool,
            NewAcquisitionAniDbFileCache {
                lookup_key: "0123456789abcdef0123456789abcdef:2048".to_string(),
                ed2k: "0123456789abcdef0123456789abcdef".to_string(),
                size_bytes: 2048,
                lookup_status: AniDbFileLookupStatus::Hit,
                anidb_file_id: Some(10),
                anidb_anime_id: Some(20),
                anidb_episode_ids: json!([30]),
                anidb_group_id: Some(40),
                anidb_group_name: Some("Group Name".to_string()),
                anidb_group_short_name: Some("GRP".to_string()),
                anidb_version: Some(1),
                anidb_source: Some("Web".to_string()),
                anidb_quality: Some("1080p".to_string()),
                anidb_audio_languages: json!(["japanese"]),
                anidb_subtitle_languages: json!(["english"]),
                anidb_state_flags: json!(["crc_match"]),
                anidb_original_filename: Some("Anime Series - 01.mkv".to_string()),
                released_at: Some(Utc::now()),
                raw_response: Some("220 FILE".to_string()),
                positive_cached_at: Some(Utc::now()),
                negative_cached_until: None,
                last_lookup_attempt_at: Some(Utc::now()),
            },
        )
        .await?;
        assert_eq!(cache.lookup_status, AniDbFileLookupStatus::Hit);
        assert_eq!(cache.anidb_episode_ids, json!([30]));

        let negative = upsert_anidb_file_cache(
            &database.pool,
            NewAcquisitionAniDbFileCache {
                lookup_key: "ffffffffffffffffffffffffffffffff:1234".to_string(),
                ed2k: "ffffffffffffffffffffffffffffffff".to_string(),
                size_bytes: 1234,
                lookup_status: AniDbFileLookupStatus::NoSuchFile,
                anidb_file_id: None,
                anidb_anime_id: None,
                anidb_episode_ids: json!([]),
                anidb_group_id: None,
                anidb_group_name: None,
                anidb_group_short_name: None,
                anidb_version: None,
                anidb_source: None,
                anidb_quality: None,
                anidb_audio_languages: json!([]),
                anidb_subtitle_languages: json!([]),
                anidb_state_flags: json!([]),
                anidb_original_filename: None,
                released_at: None,
                raw_response: None,
                positive_cached_at: None,
                negative_cached_until: Some(Utc::now()),
                last_lookup_attempt_at: Some(Utc::now()),
            },
        )
        .await?;
        assert_eq!(negative.lookup_status, AniDbFileLookupStatus::NoSuchFile);
        assert!(negative.negative_cached_until.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn anime_xrefs_match_attempts_and_mismatches_store_provenance() -> Result<()> {
        let database = setup_db().await?;
        let subscription = create_subscription(
            &database.pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Anime,
                title: "Anime Series".to_string(),
                year: Some(2026),
                external_ids: None,
                monitor_policy: AcquisitionMonitorPolicy::AllMissing,
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
                absolute_episode_number: Some(1),
                ..empty_target()
            }],
        )
        .await?;
        let release = upsert_release(
            &database.pool,
            NewAcquisitionRelease {
                subscription_id: Some(subscription.subscription_id),
                media_type: MediaType::Anime,
                resolver_kind: ReleaseResolverKind::AnimeShokoStyle,
                ..sample_release("v1:anime:xref:test:1".to_string())
            },
        )
        .await?;
        let file = upsert_release_file(
            &database.pool,
            NewAcquisitionReleaseFile {
                release_file_id: None,
                release_id: release.release_id,
                file_index: Some(0),
                file_id: Some("file-1".to_string()),
                provider_file_id: Some("file-1".to_string()),
                path: "Anime Series/Anime Series - 01.mkv".to_string(),
                basename: None,
                size_bytes: Some(1234),
                selectable: true,
                selected: None,
                parsed_title: Some("Anime Series".to_string()),
                parsed_season_number: None,
                parsed_episode_number: None,
                parsed_episode_end_number: None,
                parsed_absolute_episode_number: Some(1),
                parsed_absolute_episode_end_number: None,
                parsed_air_date: None,
                parsed_quality: None,
                parsed_language: None,
                parsed_release_group: None,
                parser_confidence: ReleaseConfidence::High,
                parser_reason: Some("absolute episode".to_string()),
                raw: None,
                provider_metadata: None,
            },
        )
        .await?;
        upsert_anidb_file_cache(
            &database.pool,
            NewAcquisitionAniDbFileCache {
                lookup_key: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:1234".to_string(),
                ed2k: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
                size_bytes: 1234,
                lookup_status: AniDbFileLookupStatus::Hit,
                anidb_file_id: Some(10),
                anidb_anime_id: Some(20),
                anidb_episode_ids: json!([30]),
                anidb_group_id: None,
                anidb_group_name: None,
                anidb_group_short_name: None,
                anidb_version: Some(1),
                anidb_source: None,
                anidb_quality: None,
                anidb_audio_languages: json!([]),
                anidb_subtitle_languages: json!([]),
                anidb_state_flags: json!([]),
                anidb_original_filename: None,
                released_at: None,
                raw_response: Some("220 FILE".to_string()),
                positive_cached_at: Some(Utc::now()),
                negative_cached_until: None,
                last_lookup_attempt_at: Some(Utc::now()),
            },
        )
        .await?;

        let xref = upsert_anidb_file_xref(
            &database.pool,
            NewAcquisitionAniDbFileXref {
                xref_id: None,
                lookup_key: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:1234".to_string(),
                release_file_id: Some(file.release_file_id),
                anidb_file_id: Some(10),
                anidb_anime_id: 20,
                anidb_episode_id: 30,
                episode_type: AnimeEpisodeType::Normal,
                percentage_start: 0,
                percentage_end: 100,
                episode_order: 0,
                provider: "AniDB".to_string(),
                confidence: ReleaseConfidence::High,
                is_manual_override: false,
                created_from_release_id: Some(release.release_id),
                created_from_target_id: Some(targets[0].target_id),
            },
        )
        .await?;
        let manual = upsert_anidb_file_xref(
            &database.pool,
            NewAcquisitionAniDbFileXref {
                is_manual_override: true,
                ..NewAcquisitionAniDbFileXref {
                    xref_id: None,
                    lookup_key: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:1234".to_string(),
                    release_file_id: Some(file.release_file_id),
                    anidb_file_id: Some(10),
                    anidb_anime_id: 20,
                    anidb_episode_id: 30,
                    episode_type: AnimeEpisodeType::Normal,
                    percentage_start: 0,
                    percentage_end: 100,
                    episode_order: 0,
                    provider: "AniDB".to_string(),
                    confidence: ReleaseConfidence::High,
                    is_manual_override: false,
                    created_from_release_id: Some(release.release_id),
                    created_from_target_id: Some(targets[0].target_id),
                }
            },
        )
        .await?;
        assert_eq!(xref.xref_id, manual.xref_id);
        assert!(manual.is_manual_override);
        assert_eq!(
            list_anidb_file_xrefs(&database.pool, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:1234")
                .await?
                .len(),
            1
        );

        let attempt = create_anime_match_attempt(
            &database.pool,
            NewAcquisitionAnimeMatchAttempt {
                match_attempt_id: None,
                release_id: Some(release.release_id),
                release_file_id: Some(file.release_file_id),
                attempted_providers: json!(["local_cache", "AniDB"]),
                selected_provider: Some("AniDB".to_string()),
                ed2k: Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string()),
                size_bytes: Some(1234),
                candidate_fingerprint: Some(release.fingerprint.clone()),
                planned_targets: json!([{ "absolute": 1 }]),
                verified_targets: json!([{ "anidbEpisodeId": 30 }]),
                outcome: AnimeMatchOutcome::Verified,
                rejection_reason: None,
            },
        )
        .await?;
        assert_eq!(attempt.outcome, AnimeMatchOutcome::Verified);
        assert_eq!(
            list_anime_match_attempts_by_release(&database.pool, release.release_id)
                .await?
                .len(),
            1
        );

        let mismatch = create_anime_identity_mismatch(
            &database.pool,
            NewAcquisitionAnimeIdentityMismatch {
                mismatch_id: None,
                release_id: Some(release.release_id),
                release_file_id: Some(file.release_file_id),
                target_id: Some(targets[0].target_id),
                planned_target: json!({ "absolute": 1 }),
                verified_identity: json!({ "anidbEpisodeId": 999 }),
                provider: "AniDB".to_string(),
                confidence: ReleaseConfidence::High,
                state: AnimeMismatchState::Open,
                reason: Some("hash identity disagrees with plan".to_string()),
            },
        )
        .await?;
        assert_eq!(mismatch.state, AnimeMismatchState::Open);
        assert_eq!(
            list_anime_identity_mismatches_by_release(&database.pool, release.release_id)
                .await?
                .len(),
            1
        );
        Ok(())
    }

    fn empty_target() -> NewAcquisitionTarget {
        NewAcquisitionTarget {
            target_key: None,
            media_type: None,
            title: None,
            season_number: None,
            episode_number: None,
            absolute_episode_number: None,
            air_date: None,
            air_time: None,
            metadata: None,
            state: None,
            next_search_after: None,
        }
    }
}
