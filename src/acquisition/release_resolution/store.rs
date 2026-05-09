use std::str::FromStr;

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, NaiveDateTime, Utc};
use serde_json::Value as JsonValue;
use sqlx::{AnyPool, Row, TypeInfo, Value as SqlxValue, ValueRef, any::AnyRow};
use uuid::Uuid;

use crate::{acquisition::release_resolution::models::*, db::models::MediaType};

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

pub async fn upsert_release_file(
    pool: &AnyPool,
    data: NewAcquisitionReleaseFile,
) -> Result<AcquisitionReleaseFile> {
    validate_release_file_input(&data)?;
    let raw_json = json_to_string(data.raw.as_ref())?;
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
                 path = ?,
                 basename = ?,
                 size_bytes = ?,
                 selectable = ?,
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
                 updated_at = CURRENT_TIMESTAMP
             WHERE release_file_id = ?",
        )
        .bind(data.release_id.to_string())
        .bind(data.file_index)
        .bind(data.file_id.as_deref())
        .bind(data.path.trim())
        .bind(basename)
        .bind(data.size_bytes)
        .bind(data.selectable)
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
                path,
                basename,
                size_bytes,
                selectable,
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
                raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(release_file_id.to_string())
        .bind(data.release_id.to_string())
        .bind(data.file_index)
        .bind(data.file_id.as_deref())
        .bind(data.path.trim())
        .bind(basename)
        .bind(data.size_bytes)
        .bind(data.selectable)
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

async fn find_release_file(
    pool: &AnyPool,
    data: &NewAcquisitionReleaseFile,
) -> Result<Option<AcquisitionReleaseFile>> {
    if let Some(release_file_id) = data.release_file_id {
        return get_release_file(pool, release_file_id).await;
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
        path: row.try_get("path")?,
        basename: row.try_get("basename")?,
        size_bytes: row_get_i64_opt(row, "size_bytes")?,
        selectable: row_get_bool(row, "selectable")?,
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

macro_rules! release_file_columns {
    () => {
        "release_file_id,
release_id,
file_index,
CAST(file_id AS TEXT) AS file_id,
path,
basename,
size_bytes,
CAST(selectable AS INTEGER) AS selectable,
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
                path: "Show/Season 01/Show.S01E01.mkv".to_string(),
                basename: None,
                size_bytes: Some(1024),
                selectable: true,
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
                    path: "Show/Season 01/Show.S01E01.mkv".to_string(),
                    basename: None,
                    size_bytes: Some(1024),
                    selectable: true,
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
                }
            },
        )
        .await?;
        assert_eq!(file.release_file_id, updated_file.release_file_id);
        assert_eq!(updated_file.size_bytes, Some(2048));

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
