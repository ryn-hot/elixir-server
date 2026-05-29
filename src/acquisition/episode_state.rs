use std::{collections::HashMap, str::FromStr};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sqlx::{AnyPool, Row, TypeInfo, Value as SqlxValue, ValueRef, any::AnyRow};
use uuid::Uuid;

use crate::{
    acquisition::subscriptions::{
        AcquisitionRequestMode, AcquisitionSubscription, AcquisitionTarget, AcquisitionTargetState,
        get_subscription, list_subscription_targets,
    },
    db::models::MediaType,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LibraryEpisodeAcquisitionState {
    Queued,
    Searching,
    Downloading,
    PostProcessing,
    ReviewNeeded,
    NoResults,
    Failed,
    Imported,
}

impl LibraryEpisodeAcquisitionState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Searching => "searching",
            Self::Downloading => "downloading",
            Self::PostProcessing => "post_processing",
            Self::ReviewNeeded => "review_needed",
            Self::NoResults => "no_results",
            Self::Failed => "failed",
            Self::Imported => "imported",
        }
    }
}

impl FromStr for LibraryEpisodeAcquisitionState {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "queued" => Ok(Self::Queued),
            "searching" => Ok(Self::Searching),
            "downloading" => Ok(Self::Downloading),
            "post_processing" | "post-processing" => Ok(Self::PostProcessing),
            "review_needed" | "review" => Ok(Self::ReviewNeeded),
            "no_results" | "no-results" => Ok(Self::NoResults),
            "failed" => Ok(Self::Failed),
            "imported" => Ok(Self::Imported),
            other => bail!("unknown library episode acquisition state '{other}'"),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LibraryEpisodeAcquisitionProjection {
    pub episode_id: Uuid,
    pub media_item_id: Uuid,
    pub season_id: Uuid,
    pub target_key: String,
    pub state: LibraryEpisodeAcquisitionState,
    pub reason_code: Option<String>,
    pub reason_message: Option<String>,
    pub source_provider_id: Option<Uuid>,
    pub source_provider_label: Option<String>,
    pub route_provider_id: Option<Uuid>,
    pub route_provider_label: Option<String>,
    pub subscription_id: Option<Uuid>,
    pub target_id: Option<Uuid>,
    pub release_id: Option<Uuid>,
    pub job_id: Option<Uuid>,
    pub candidate_count: Option<i64>,
    pub selected_release_title: Option<String>,
    pub last_attempt_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct EpisodeProjectionContext {
    episode_id: Uuid,
    media_item_id: Uuid,
    season_id: Uuid,
}

#[derive(Debug, Clone)]
struct TargetReleaseEvidence {
    release_id: Option<Uuid>,
    job_id: Option<Uuid>,
}

#[derive(Debug, Clone)]
struct ProviderLabel {
    provider_id: Uuid,
    label: Option<String>,
}

#[derive(Debug, Clone)]
struct StateProjection {
    state: LibraryEpisodeAcquisitionState,
    reason_code: Option<String>,
    reason_message: Option<String>,
    candidate_count: Option<i64>,
}

pub async fn sync_library_episode_acquisition_state_for_target(
    pool: &AnyPool,
    target: &AcquisitionTarget,
) -> Result<()> {
    if target.media_type == MediaType::Movie {
        return Ok(());
    }

    let Some(subscription) = get_subscription(pool, target.subscription_id).await? else {
        return Ok(());
    };
    let Some(context) = resolve_episode_projection_context(pool, &subscription, target).await?
    else {
        return Ok(());
    };

    let state_projection = classify_target_projection(&subscription, target);
    let source_provider_id = target
        .selected_provider_id
        .or(subscription.source_provider_id);
    let source_provider = load_provider_label(pool, source_provider_id).await?;
    let route_provider = load_provider_label(
        pool,
        route_provider_id_from_selected_candidate(target).await?,
    )
    .await?;
    let release_evidence = load_target_release_evidence(pool, target.target_id).await?;
    let selected_release_title = selected_release_title(target);
    let last_attempt_at = target.last_search_at.or(Some(target.updated_at));

    sqlx::query::<sqlx::Any>(
        "INSERT INTO library_episode_acquisition_state (
            episode_id,
            media_item_id,
            season_id,
            target_key,
            state,
            reason_code,
            reason_message,
            source_provider_id,
            source_provider_label,
            route_provider_id,
            route_provider_label,
            subscription_id,
            target_id,
            release_id,
            job_id,
            candidate_count,
            selected_release_title,
            last_attempt_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(episode_id) DO UPDATE SET
            media_item_id = excluded.media_item_id,
            season_id = excluded.season_id,
            target_key = excluded.target_key,
            state = excluded.state,
            reason_code = excluded.reason_code,
            reason_message = excluded.reason_message,
            source_provider_id = excluded.source_provider_id,
            source_provider_label = excluded.source_provider_label,
            route_provider_id = excluded.route_provider_id,
            route_provider_label = excluded.route_provider_label,
            subscription_id = excluded.subscription_id,
            target_id = excluded.target_id,
            release_id = excluded.release_id,
            job_id = excluded.job_id,
            candidate_count = excluded.candidate_count,
            selected_release_title = excluded.selected_release_title,
            last_attempt_at = excluded.last_attempt_at,
            updated_at = CURRENT_TIMESTAMP",
    )
    .bind(context.episode_id.to_string())
    .bind(context.media_item_id.to_string())
    .bind(context.season_id.to_string())
    .bind(&target.target_key)
    .bind(state_projection.state.as_str())
    .bind(state_projection.reason_code.as_deref())
    .bind(state_projection.reason_message.as_deref())
    .bind(
        source_provider
            .as_ref()
            .map(|item| item.provider_id.to_string()),
    )
    .bind(
        source_provider
            .as_ref()
            .and_then(|item| item.label.as_deref()),
    )
    .bind(
        route_provider
            .as_ref()
            .map(|item| item.provider_id.to_string()),
    )
    .bind(
        route_provider
            .as_ref()
            .and_then(|item| item.label.as_deref()),
    )
    .bind(subscription.subscription_id.to_string())
    .bind(target.target_id.to_string())
    .bind(release_evidence.release_id.map(|value| value.to_string()))
    .bind(release_evidence.job_id.map(|value| value.to_string()))
    .bind(state_projection.candidate_count)
    .bind(selected_release_title.as_deref())
    .bind(last_attempt_at.map(db_datetime_string))
    .execute(pool)
    .await
    .context("upserting library episode acquisition state")?;

    Ok(())
}

#[allow(dead_code)]
pub async fn rebuild_library_episode_acquisition_states_for_subscription(
    pool: &AnyPool,
    subscription_id: Uuid,
) -> Result<usize> {
    let targets = list_subscription_targets(pool, subscription_id).await?;
    let mut rebuilt = 0usize;
    for target in targets {
        sync_library_episode_acquisition_state_for_target(pool, &target).await?;
        rebuilt += 1;
    }
    Ok(rebuilt)
}

#[allow(dead_code)]
pub async fn get_library_episode_acquisition_projection(
    pool: &AnyPool,
    episode_id: Uuid,
) -> Result<Option<LibraryEpisodeAcquisitionProjection>> {
    let row = sqlx::query(
        "SELECT
            episode_id,
            media_item_id,
            season_id,
            target_key,
            state,
            CAST(reason_code AS TEXT) AS reason_code,
            CAST(reason_message AS TEXT) AS reason_message,
            CAST(source_provider_id AS TEXT) AS source_provider_id,
            CAST(source_provider_label AS TEXT) AS source_provider_label,
            CAST(route_provider_id AS TEXT) AS route_provider_id,
            CAST(route_provider_label AS TEXT) AS route_provider_label,
            CAST(subscription_id AS TEXT) AS subscription_id,
            CAST(target_id AS TEXT) AS target_id,
            CAST(release_id AS TEXT) AS release_id,
            CAST(job_id AS TEXT) AS job_id,
            candidate_count,
            CAST(selected_release_title AS TEXT) AS selected_release_title,
            CAST(last_attempt_at AS TEXT) AS last_attempt_at,
            CAST(updated_at AS TEXT) AS updated_at
         FROM library_episode_acquisition_state
         WHERE episode_id = ?
         LIMIT 1",
    )
    .bind(episode_id.to_string())
    .fetch_optional(pool)
    .await
    .context("loading library episode acquisition state")?;

    row.map(|row| map_projection(&row)).transpose()
}

pub async fn list_library_episode_acquisition_projections(
    pool: &AnyPool,
    episode_ids: &[String],
) -> Result<HashMap<String, LibraryEpisodeAcquisitionProjection>> {
    if episode_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let placeholders = std::iter::repeat_n("?", episode_ids.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT
            episode_id,
            media_item_id,
            season_id,
            target_key,
            state,
            CAST(reason_code AS TEXT) AS reason_code,
            CAST(reason_message AS TEXT) AS reason_message,
            CAST(source_provider_id AS TEXT) AS source_provider_id,
            CAST(source_provider_label AS TEXT) AS source_provider_label,
            CAST(route_provider_id AS TEXT) AS route_provider_id,
            CAST(route_provider_label AS TEXT) AS route_provider_label,
            CAST(subscription_id AS TEXT) AS subscription_id,
            CAST(target_id AS TEXT) AS target_id,
            CAST(release_id AS TEXT) AS release_id,
            CAST(job_id AS TEXT) AS job_id,
            candidate_count,
            CAST(selected_release_title AS TEXT) AS selected_release_title,
            CAST(last_attempt_at AS TEXT) AS last_attempt_at,
            CAST(updated_at AS TEXT) AS updated_at
         FROM library_episode_acquisition_state
         WHERE episode_id IN ({placeholders})",
    );
    let mut query = sqlx::query(&sql);
    for episode_id in episode_ids {
        query = query.bind(episode_id);
    }

    let rows = query
        .fetch_all(pool)
        .await
        .context("loading library episode acquisition states")?;
    let mut projections = HashMap::with_capacity(rows.len());
    for row in rows {
        let projection = map_projection(&row)?;
        projections.insert(projection.episode_id.to_string(), projection);
    }
    Ok(projections)
}

async fn resolve_episode_projection_context(
    pool: &AnyPool,
    subscription: &AcquisitionSubscription,
    target: &AcquisitionTarget,
) -> Result<Option<EpisodeProjectionContext>> {
    if let Some(episode_id) = episode_id_from_target_metadata(target)
        && let Some(context) = load_episode_projection_context(pool, episode_id).await?
    {
        return Ok(Some(context));
    }

    let series_id =
        resolve_series_id_from_target_or_subscription(pool, subscription, target).await?;
    let Some(series_id) = series_id else {
        return Ok(None);
    };

    let row = if let (Some(season), Some(episode)) = (target.season_number, target.episode_number) {
        sqlx::query(
            "SELECT id, series_id, season_id
             FROM episodes
             WHERE series_id = ? AND season_number = ? AND episode_number = ?
             LIMIT 1",
        )
        .bind(series_id.to_string())
        .bind(season)
        .bind(episode)
        .fetch_optional(pool)
        .await?
    } else if let Some(absolute) = target.absolute_episode_number {
        sqlx::query(
            "SELECT id, series_id, season_id
             FROM episodes
             WHERE series_id = ? AND absolute_episode_number = ?
             LIMIT 1",
        )
        .bind(series_id.to_string())
        .bind(absolute)
        .fetch_optional(pool)
        .await?
    } else {
        None
    };

    row.map(|row| {
        Ok(EpisodeProjectionContext {
            episode_id: parse_uuid(row.get::<String, _>("id"), "episodes.id")?,
            media_item_id: parse_uuid(row.get::<String, _>("series_id"), "episodes.series_id")?,
            season_id: parse_uuid(row.get::<String, _>("season_id"), "episodes.season_id")?,
        })
    })
    .transpose()
}

async fn load_episode_projection_context(
    pool: &AnyPool,
    episode_id: Uuid,
) -> Result<Option<EpisodeProjectionContext>> {
    let row = sqlx::query(
        "SELECT id, series_id, season_id
         FROM episodes
         WHERE id = ?
         LIMIT 1",
    )
    .bind(episode_id.to_string())
    .fetch_optional(pool)
    .await?;

    row.map(|row| {
        Ok(EpisodeProjectionContext {
            episode_id: parse_uuid(row.get::<String, _>("id"), "episodes.id")?,
            media_item_id: parse_uuid(row.get::<String, _>("series_id"), "episodes.series_id")?,
            season_id: parse_uuid(row.get::<String, _>("season_id"), "episodes.season_id")?,
        })
    })
    .transpose()
}

fn episode_id_from_target_metadata(target: &AcquisitionTarget) -> Option<Uuid> {
    let metadata = target.metadata.as_ref()?;
    metadata_string(
        metadata,
        &[
            "libraryEpisodeId",
            "library_episode_id",
            "episodeId",
            "episode_id",
        ],
    )
    .and_then(|value| Uuid::parse_str(&value).ok())
}

async fn resolve_series_id_from_target_or_subscription(
    pool: &AnyPool,
    subscription: &AcquisitionSubscription,
    target: &AcquisitionTarget,
) -> Result<Option<Uuid>> {
    if let Some(series_id) = explicit_series_id_from_target_or_subscription(subscription, target) {
        return Ok(Some(series_id));
    }

    let metadata_ids = target
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("externalIds"));
    let tvdb_series = metadata_ids
        .and_then(|ids| metadata_string(ids, &["tvdbSeries", "tvdb_series"]))
        .or_else(|| metadata_ids.and_then(|ids| metadata_string(ids, &["tvdb"])))
        .or_else(|| {
            subscription
                .external_ids
                .as_ref()
                .and_then(|ids| ids.tvdb_series.clone().or_else(|| ids.tvdb.clone()))
        });
    let imdb = metadata_ids
        .and_then(|ids| metadata_string(ids, &["imdb"]))
        .or_else(|| {
            subscription
                .external_ids
                .as_ref()
                .and_then(|ids| ids.imdb.clone())
        });
    let anilist = metadata_ids
        .and_then(|ids| metadata_string(ids, &["anilist"]))
        .or_else(|| {
            subscription
                .external_ids
                .as_ref()
                .and_then(|ids| ids.anilist.clone())
        });

    if tvdb_series.is_none() && imdb.is_none() && anilist.is_none() {
        return Ok(None);
    }

    let row = sqlx::query(
        "SELECT id
         FROM series
         WHERE (? IS NOT NULL AND external_tvdb_series = ?)
            OR (? IS NOT NULL AND external_imdb = ?)
            OR (? IS NOT NULL AND external_anilist = ?)
         ORDER BY updated_at DESC
         LIMIT 1",
    )
    .bind(tvdb_series.as_deref())
    .bind(tvdb_series.as_deref())
    .bind(imdb.as_deref())
    .bind(imdb.as_deref())
    .bind(anilist.as_deref())
    .bind(anilist.as_deref())
    .fetch_optional(pool)
    .await?;

    row.map(|row| parse_uuid(row.get::<String, _>("id"), "series.id"))
        .transpose()
}

fn explicit_series_id_from_target_or_subscription(
    subscription: &AcquisitionSubscription,
    target: &AcquisitionTarget,
) -> Option<Uuid> {
    target
        .metadata
        .as_ref()
        .and_then(|metadata| metadata_string(metadata, &["mediaItemId", "media_item_id"]))
        .or_else(|| {
            subscription
                .scope
                .as_ref()
                .and_then(|scope| metadata_string(scope, &["mediaItemId", "media_item_id"]))
        })
        .or_else(|| {
            subscription.scope.as_ref().and_then(|scope| {
                scope
                    .get("requestedScope")
                    .and_then(|value| metadata_string(value, &["mediaItemId", "media_item_id"]))
            })
        })
        .and_then(|value| Uuid::parse_str(&value).ok())
}

fn classify_target_projection(
    subscription: &AcquisitionSubscription,
    target: &AcquisitionTarget,
) -> StateProjection {
    let reason = target.state_reason.as_deref().unwrap_or_default();
    let reason_lower = reason.to_ascii_lowercase();
    let state = match target.state {
        AcquisitionTargetState::Imported => LibraryEpisodeAcquisitionState::Imported,
        AcquisitionTargetState::Searching => LibraryEpisodeAcquisitionState::Searching,
        AcquisitionTargetState::Submitted if looks_like_post_processing(&reason_lower) => {
            LibraryEpisodeAcquisitionState::PostProcessing
        }
        AcquisitionTargetState::Submitted => LibraryEpisodeAcquisitionState::Downloading,
        AcquisitionTargetState::Pending if looks_like_review_needed(&reason_lower) => {
            LibraryEpisodeAcquisitionState::ReviewNeeded
        }
        AcquisitionTargetState::Pending if looks_like_no_results(&reason_lower) => {
            LibraryEpisodeAcquisitionState::NoResults
        }
        AcquisitionTargetState::Pending
            if subscription.request_mode == AcquisitionRequestMode::OneShot
                && target
                    .next_search_after
                    .map(|next_search_after| next_search_after <= Utc::now())
                    .unwrap_or(true) =>
        {
            LibraryEpisodeAcquisitionState::Searching
        }
        AcquisitionTargetState::Pending => LibraryEpisodeAcquisitionState::Queued,
        AcquisitionTargetState::Blocked if looks_like_queue_capacity(&reason_lower) => {
            LibraryEpisodeAcquisitionState::Queued
        }
        AcquisitionTargetState::Blocked if looks_like_review_needed(&reason_lower) => {
            LibraryEpisodeAcquisitionState::ReviewNeeded
        }
        AcquisitionTargetState::Blocked if looks_like_no_results(&reason_lower) => {
            LibraryEpisodeAcquisitionState::NoResults
        }
        AcquisitionTargetState::Blocked => LibraryEpisodeAcquisitionState::Failed,
        AcquisitionTargetState::Excluded if looks_like_no_results(&reason_lower) => {
            LibraryEpisodeAcquisitionState::NoResults
        }
        AcquisitionTargetState::Excluded if looks_like_cancelled(&reason_lower) => {
            LibraryEpisodeAcquisitionState::Failed
        }
        AcquisitionTargetState::Excluded
            if subscription.request_mode == AcquisitionRequestMode::OneShot =>
        {
            LibraryEpisodeAcquisitionState::NoResults
        }
        AcquisitionTargetState::Excluded => LibraryEpisodeAcquisitionState::Failed,
    };
    StateProjection {
        reason_code: reason_code_for_state(state, &reason_lower),
        reason_message: (!reason.trim().is_empty()).then(|| reason.trim().to_string()),
        candidate_count: candidate_count_for_state(state, &reason_lower),
        state,
    }
}

async fn route_provider_id_from_selected_candidate(
    target: &AcquisitionTarget,
) -> Result<Option<Uuid>> {
    let Some(candidate) = target.selected_candidate.as_ref() else {
        return Ok(None);
    };
    for pointer in [
        "/submissionResult/routeProviderId",
        "/routeProviderId",
        "/route_provider_id",
    ] {
        if let Some(value) = candidate.pointer(pointer).and_then(JsonValue::as_str) {
            return Ok(Some(parse_uuid(
                value.to_string(),
                "selected candidate route provider id",
            )?));
        }
    }
    Ok(None)
}

async fn load_provider_label(
    pool: &AnyPool,
    provider_id: Option<Uuid>,
) -> Result<Option<ProviderLabel>> {
    let Some(provider_id) = provider_id else {
        return Ok(None);
    };
    let row = sqlx::query(
        "SELECT
            CAST(i.instance_name AS TEXT) AS instance_name,
            CAST(e.name AS TEXT) AS extension_name,
            CAST(p.capability AS TEXT) AS capability,
            CAST(p.slot_id AS TEXT) AS slot_id
         FROM providers p
         LEFT JOIN extension_instances i ON i.instance_id = p.instance_id
         LEFT JOIN extensions e ON e.extension_id = i.extension_id
         WHERE p.provider_id = ?
         LIMIT 1",
    )
    .bind(provider_id.to_string())
    .fetch_optional(pool)
    .await?;

    let label = row.and_then(|row| {
        row_get_opt_string(&row, "instance_name")
            .ok()
            .flatten()
            .or_else(|| row_get_opt_string(&row, "extension_name").ok().flatten())
            .or_else(|| row_get_opt_string(&row, "capability").ok().flatten())
            .or_else(|| row_get_opt_string(&row, "slot_id").ok().flatten())
    });
    Ok(Some(ProviderLabel { provider_id, label }))
}

async fn load_target_release_evidence(
    pool: &AnyPool,
    target_id: Uuid,
) -> Result<TargetReleaseEvidence> {
    let row = sqlx::query(
        "SELECT
            CAST(c.release_id AS TEXT) AS release_id,
            CAST(j.release_job_id AS TEXT) AS job_id
         FROM acquisition_release_coverage c
         LEFT JOIN acquisition_release_jobs j ON j.release_id = c.release_id
         WHERE c.target_id = ?
         ORDER BY c.updated_at DESC, j.updated_at DESC
         LIMIT 1",
    )
    .bind(target_id.to_string())
    .fetch_optional(pool)
    .await?;

    let Some(row) = row else {
        return Ok(TargetReleaseEvidence {
            release_id: None,
            job_id: None,
        });
    };
    Ok(TargetReleaseEvidence {
        release_id: row_get_opt_string(&row, "release_id")?
            .map(|value| parse_uuid(value, "acquisition_release_coverage.release_id"))
            .transpose()?,
        job_id: row_get_opt_string(&row, "job_id")?
            .map(|value| parse_uuid(value, "acquisition_release_jobs.release_job_id"))
            .transpose()?,
    })
}

fn selected_release_title(target: &AcquisitionTarget) -> Option<String> {
    target
        .selected_candidate
        .as_ref()
        .and_then(|candidate| {
            candidate
                .get("title")
                .or_else(|| candidate.get("releaseTitle"))
                .or_else(|| candidate.get("release_title"))
        })
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn reason_code_for_state(
    state: LibraryEpisodeAcquisitionState,
    reason_lower: &str,
) -> Option<String> {
    let code = match state {
        LibraryEpisodeAcquisitionState::Queued if looks_like_queue_capacity(reason_lower) => {
            "queue_capacity"
        }
        LibraryEpisodeAcquisitionState::Queued => "queued",
        LibraryEpisodeAcquisitionState::Searching => "searching",
        LibraryEpisodeAcquisitionState::Downloading => "submitted",
        LibraryEpisodeAcquisitionState::PostProcessing => "post_processing",
        LibraryEpisodeAcquisitionState::ReviewNeeded => "review_required",
        LibraryEpisodeAcquisitionState::NoResults
            if reason_lower.contains("no acquisition candidates") =>
        {
            "no_candidates"
        }
        LibraryEpisodeAcquisitionState::NoResults => "no_safe_candidates",
        LibraryEpisodeAcquisitionState::Failed if looks_like_cancelled(reason_lower) => "cancelled",
        LibraryEpisodeAcquisitionState::Failed if reason_lower.contains("route") => "route_failed",
        LibraryEpisodeAcquisitionState::Failed => "failed",
        LibraryEpisodeAcquisitionState::Imported => "imported",
    };
    Some(code.to_string())
}

fn candidate_count_for_state(
    state: LibraryEpisodeAcquisitionState,
    reason_lower: &str,
) -> Option<i64> {
    (state == LibraryEpisodeAcquisitionState::NoResults
        && reason_lower.contains("no acquisition candidates"))
    .then_some(0)
}

fn looks_like_review_needed(reason_lower: &str) -> bool {
    reason_lower.contains("awaiting manual")
        || reason_lower.contains("manual review")
        || reason_lower.contains("review item")
        || reason_lower.contains("waiting review")
}

fn looks_like_no_results(reason_lower: &str) -> bool {
    reason_lower.contains("no acquisition candidates")
        || reason_lower.contains("no matching acquisition candidates")
        || reason_lower.contains("no matching candidates")
        || reason_lower.contains("none matched")
        || reason_lower.contains("could not safely match")
        || reason_lower.contains("no safe candidate")
}

fn looks_like_queue_capacity(reason_lower: &str) -> bool {
    reason_lower.contains("queue capacity")
}

fn looks_like_post_processing(reason_lower: &str) -> bool {
    reason_lower.contains("materializer completed")
        || reason_lower.contains("download completed")
        || reason_lower.contains("completed selected files")
        || reason_lower.contains("importing completed acquisition files")
        || reason_lower.contains("ready for import")
}

fn looks_like_cancelled(reason_lower: &str) -> bool {
    reason_lower.contains("cancelled")
        || reason_lower.contains("canceled")
        || reason_lower.contains("removed acquisition request")
        || reason_lower.contains("user removed")
}

fn metadata_string(value: &JsonValue, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(value) = value.get(*key).and_then(JsonValue::as_str) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

fn map_projection(row: &AnyRow) -> Result<LibraryEpisodeAcquisitionProjection> {
    let episode_id = parse_uuid(row.get::<String, _>("episode_id"), "episode_id")?;
    let media_item_id = parse_uuid(row.get::<String, _>("media_item_id"), "media_item_id")?;
    let season_id = parse_uuid(row.get::<String, _>("season_id"), "season_id")?;
    let state = LibraryEpisodeAcquisitionState::from_str(row.get::<String, _>("state").as_str())?;
    Ok(LibraryEpisodeAcquisitionProjection {
        episode_id,
        media_item_id,
        season_id,
        target_key: row.get::<String, _>("target_key"),
        state,
        reason_code: row_get_opt_string(row, "reason_code")?,
        reason_message: row_get_opt_string(row, "reason_message")?,
        source_provider_id: row_get_opt_string(row, "source_provider_id")?
            .map(|value| parse_uuid(value, "source_provider_id"))
            .transpose()?,
        source_provider_label: row_get_opt_string(row, "source_provider_label")?,
        route_provider_id: row_get_opt_string(row, "route_provider_id")?
            .map(|value| parse_uuid(value, "route_provider_id"))
            .transpose()?,
        route_provider_label: row_get_opt_string(row, "route_provider_label")?,
        subscription_id: row_get_opt_string(row, "subscription_id")?
            .map(|value| parse_uuid(value, "subscription_id"))
            .transpose()?,
        target_id: row_get_opt_string(row, "target_id")?
            .map(|value| parse_uuid(value, "target_id"))
            .transpose()?,
        release_id: row_get_opt_string(row, "release_id")?
            .map(|value| parse_uuid(value, "release_id"))
            .transpose()?,
        job_id: row_get_opt_string(row, "job_id")?
            .map(|value| parse_uuid(value, "job_id"))
            .transpose()?,
        candidate_count: row_get_i64_opt(row, "candidate_count")?,
        selected_release_title: row_get_opt_string(row, "selected_release_title")?,
        last_attempt_at: row_get_opt_string(row, "last_attempt_at")?
            .map(|value| parse_datetime(&value, "last_attempt_at"))
            .transpose()?,
        updated_at: parse_datetime(&row.get::<String, _>("updated_at"), "updated_at")?,
    })
}

fn parse_uuid(value: impl AsRef<str>, field: &str) -> Result<Uuid> {
    let value = value.as_ref();
    Uuid::parse_str(value.trim()).with_context(|| format!("invalid {field} uuid '{value}'"))
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

fn db_datetime_string(value: DateTime<Utc>) -> String {
    value.format("%Y-%m-%d %H:%M:%S").to_string()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        acquisition::subscriptions::{
            AcquisitionCompletionPolicy, AcquisitionMetadataPolicy, AcquisitionMonitorPolicy,
            AcquisitionRequestScope, AcquisitionRoutePolicy, AcquisitionTargetStateUpdate,
            NewAcquisitionSubscription, NewAcquisitionTarget, create_subscription,
            reset_target_for_candidate_retry, stop_subscription_tracking, update_target_state,
            upsert_subscription_targets,
        },
        config::DatabaseConfig,
        db::Database,
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

    async fn seed_episode(pool: &AnyPool) -> Result<(Uuid, Uuid, Uuid)> {
        let series_id = Uuid::new_v4();
        let season_id = Uuid::new_v4();
        let episode_id = Uuid::new_v4();
        sqlx::query::<sqlx::Any>(
            "INSERT INTO series (id, title, year, library_type) VALUES (?, ?, ?, ?)",
        )
        .bind(series_id.to_string())
        .bind("Projection Show")
        .bind(2026_i64)
        .bind("series")
        .execute(pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO seasons (id, series_id, season_number, title) VALUES (?, ?, ?, ?)",
        )
        .bind(season_id.to_string())
        .bind(series_id.to_string())
        .bind(1_i64)
        .bind("Season 1")
        .execute(pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO episodes (
                id, series_id, season_id, season_number, episode_number, title, has_file
            ) VALUES (?, ?, ?, ?, ?, ?, 0)",
        )
        .bind(episode_id.to_string())
        .bind(series_id.to_string())
        .bind(season_id.to_string())
        .bind(1_i64)
        .bind(1_i64)
        .bind("Pilot")
        .execute(pool)
        .await?;
        Ok((series_id, season_id, episode_id))
    }

    fn one_shot_subscription(series_id: Uuid) -> NewAcquisitionSubscription {
        NewAcquisitionSubscription {
            media_type: MediaType::Series,
            title: "Projection Show".to_string(),
            year: Some(2026),
            external_ids: None,
            idempotency_key: None,
            request_mode: Some(AcquisitionRequestMode::OneShot),
            request_scope: Some(AcquisitionRequestScope::Episode),
            scope: Some(json!({
                "mediaItemId": series_id,
                "seasonNumber": 1,
                "episodeNumber": 1
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
        }
    }

    async fn count_projection_rows(pool: &AnyPool, episode_id: Uuid) -> Result<i64> {
        let row = sqlx::query::<sqlx::Any>(
            "SELECT COUNT(*) AS count
             FROM library_episode_acquisition_state
             WHERE episode_id = ?",
        )
        .bind(episode_id.to_string())
        .fetch_one(pool)
        .await?;
        row_get_i64_opt(&row, "count").map(|value| value.unwrap_or_default())
    }

    fn library_episode_target(series_id: Uuid, episode_id: Uuid) -> NewAcquisitionTarget {
        NewAcquisitionTarget {
            target_key: Some("S01E01".to_string()),
            media_type: Some(MediaType::Series),
            title: Some("Pilot".to_string()),
            season_number: Some(1),
            episode_number: Some(1),
            absolute_episode_number: None,
            air_date: None,
            air_time: None,
            metadata: Some(json!({
                "mediaItemId": series_id,
                "libraryEpisodeId": episode_id
            })),
            state: Some(AcquisitionTargetState::Pending),
            next_search_after: None,
        }
    }

    #[tokio::test]
    async fn mmr0_projection_upsert_is_idempotent_and_cascades_with_episode() -> Result<()> {
        let database = setup_db().await?;
        let (series_id, _, episode_id) = seed_episode(&database.pool).await?;
        let subscription =
            create_subscription(&database.pool, one_shot_subscription(series_id)).await?;

        upsert_subscription_targets(
            &database.pool,
            subscription.subscription_id,
            vec![library_episode_target(series_id, episode_id)],
        )
        .await?;
        upsert_subscription_targets(
            &database.pool,
            subscription.subscription_id,
            vec![NewAcquisitionTarget {
                title: Some("Pilot - refreshed".to_string()),
                ..library_episode_target(series_id, episode_id)
            }],
        )
        .await?;

        assert_eq!(count_projection_rows(&database.pool, episode_id).await?, 1);
        let projection = get_library_episode_acquisition_projection(&database.pool, episode_id)
            .await?
            .expect("projection");
        assert_eq!(projection.target_key, "S01E01");
        assert_eq!(projection.media_item_id, series_id);

        sqlx::query::<sqlx::Any>("DELETE FROM episodes WHERE id = ?")
            .bind(episode_id.to_string())
            .execute(&database.pool)
            .await?;
        assert_eq!(count_projection_rows(&database.pool, episode_id).await?, 0);
        assert!(
            get_library_episode_acquisition_projection(&database.pool, episode_id)
                .await?
                .is_none()
        );

        let (series_id, _, episode_id) = seed_episode(&database.pool).await?;
        let subscription =
            create_subscription(&database.pool, one_shot_subscription(series_id)).await?;
        upsert_subscription_targets(
            &database.pool,
            subscription.subscription_id,
            vec![library_episode_target(series_id, episode_id)],
        )
        .await?;
        assert_eq!(count_projection_rows(&database.pool, episode_id).await?, 1);

        sqlx::query::<sqlx::Any>("DELETE FROM series WHERE id = ?")
            .bind(series_id.to_string())
            .execute(&database.pool)
            .await?;
        assert_eq!(count_projection_rows(&database.pool, episode_id).await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn mmr0_projection_tracks_target_lifecycle_for_library_episode() -> Result<()> {
        let database = setup_db().await?;
        let (series_id, _, episode_id) = seed_episode(&database.pool).await?;
        let subscription =
            create_subscription(&database.pool, one_shot_subscription(series_id)).await?;
        let targets = upsert_subscription_targets(
            &database.pool,
            subscription.subscription_id,
            vec![library_episode_target(series_id, episode_id)],
        )
        .await?;
        let target = &targets[0];

        let projection = get_library_episode_acquisition_projection(&database.pool, episode_id)
            .await?
            .expect("projection");
        assert_eq!(projection.state, LibraryEpisodeAcquisitionState::Searching);
        assert_eq!(projection.target_id, Some(target.target_id));

        update_target_state(
            &database.pool,
            target.target_id,
            AcquisitionTargetStateUpdate {
                state: AcquisitionTargetState::Searching,
                state_reason: Some("Searching acquisition source provider.".to_string()),
                ..Default::default()
            },
        )
        .await?;
        assert_eq!(
            get_library_episode_acquisition_projection(&database.pool, episode_id)
                .await?
                .expect("projection")
                .state,
            LibraryEpisodeAcquisitionState::Searching
        );

        update_target_state(
            &database.pool,
            target.target_id,
            AcquisitionTargetStateUpdate {
                state: AcquisitionTargetState::Submitted,
                state_reason: Some("Submitted through acquisition route.".to_string()),
                selected_candidate: Some(json!({ "title": "Projection.Show.S01E01.1080p" })),
                download_id: Some("download-1".to_string()),
                ..Default::default()
            },
        )
        .await?;
        let projection = get_library_episode_acquisition_projection(&database.pool, episode_id)
            .await?
            .expect("projection");
        assert_eq!(
            projection.state,
            LibraryEpisodeAcquisitionState::Downloading
        );
        assert_eq!(
            projection.selected_release_title.as_deref(),
            Some("Projection.Show.S01E01.1080p")
        );

        update_target_state(
            &database.pool,
            target.target_id,
            AcquisitionTargetStateUpdate {
                state: AcquisitionTargetState::Submitted,
                state_reason: Some("Debrid materializer completed selected files.".to_string()),
                ..Default::default()
            },
        )
        .await?;
        assert_eq!(
            get_library_episode_acquisition_projection(&database.pool, episode_id)
                .await?
                .expect("projection")
                .state,
            LibraryEpisodeAcquisitionState::PostProcessing
        );

        update_target_state(
            &database.pool,
            target.target_id,
            AcquisitionTargetStateUpdate {
                state: AcquisitionTargetState::Imported,
                state_reason: Some("Imported into the Elixir library.".to_string()),
                ..Default::default()
            },
        )
        .await?;
        assert_eq!(
            get_library_episode_acquisition_projection(&database.pool, episode_id)
                .await?
                .expect("projection")
                .state,
            LibraryEpisodeAcquisitionState::Imported
        );

        sqlx::query::<sqlx::Any>(
            "DELETE FROM library_episode_acquisition_state WHERE episode_id = ?",
        )
        .bind(episode_id.to_string())
        .execute(&database.pool)
        .await?;
        assert_eq!(
            rebuild_library_episode_acquisition_states_for_subscription(
                &database.pool,
                subscription.subscription_id,
            )
            .await?,
            1
        );
        assert_eq!(
            get_library_episode_acquisition_projection(&database.pool, episode_id)
                .await?
                .expect("projection")
                .state,
            LibraryEpisodeAcquisitionState::Imported
        );
        Ok(())
    }

    #[tokio::test]
    async fn mmr1_projection_marks_review_and_no_results_states() -> Result<()> {
        let database = setup_db().await?;
        let (series_id, _, episode_id) = seed_episode(&database.pool).await?;
        let subscription =
            create_subscription(&database.pool, one_shot_subscription(series_id)).await?;
        let targets = upsert_subscription_targets(
            &database.pool,
            subscription.subscription_id,
            vec![library_episode_target(series_id, episode_id)],
        )
        .await?;
        let target_id = targets[0].target_id;

        update_target_state(
            &database.pool,
            target_id,
            AcquisitionTargetStateUpdate {
                state: AcquisitionTargetState::Pending,
                state_reason: Some(
                    "Candidates found; awaiting manual release selection (1 review item)."
                        .to_string(),
                ),
                ..Default::default()
            },
        )
        .await?;
        assert_eq!(
            get_library_episode_acquisition_projection(&database.pool, episode_id)
                .await?
                .expect("projection")
                .state,
            LibraryEpisodeAcquisitionState::ReviewNeeded
        );

        update_target_state(
            &database.pool,
            target_id,
            AcquisitionTargetStateUpdate {
                state: AcquisitionTargetState::Excluded,
                state_reason: Some(
                    "No acquisition candidates were returned for episode.".to_string(),
                ),
                ..Default::default()
            },
        )
        .await?;
        let projection = get_library_episode_acquisition_projection(&database.pool, episode_id)
            .await?
            .expect("projection");
        assert_eq!(projection.state, LibraryEpisodeAcquisitionState::NoResults);
        assert_eq!(projection.candidate_count, Some(0));
        assert_eq!(projection.reason_code.as_deref(), Some("no_candidates"));

        update_target_state(
            &database.pool,
            target_id,
            AcquisitionTargetStateUpdate {
                state: AcquisitionTargetState::Blocked,
                state_reason: Some("Acquisition route blocked: provider unavailable.".to_string()),
                ..Default::default()
            },
        )
        .await?;
        let projection = get_library_episode_acquisition_projection(&database.pool, episode_id)
            .await?
            .expect("projection");
        assert_eq!(projection.state, LibraryEpisodeAcquisitionState::Failed);
        assert_eq!(projection.reason_code.as_deref(), Some("route_failed"));

        reset_target_for_candidate_retry(
            &database.pool,
            target_id,
            "User retried acquisition request.".to_string(),
            Utc::now(),
        )
        .await?;
        let projection = get_library_episode_acquisition_projection(&database.pool, episode_id)
            .await?
            .expect("projection");
        assert_eq!(projection.state, LibraryEpisodeAcquisitionState::Searching);
        assert_eq!(projection.reason_code.as_deref(), Some("searching"));

        stop_subscription_tracking(
            &database.pool,
            subscription.subscription_id,
            "User cancelled acquisition request.",
        )
        .await?;
        let projection = get_library_episode_acquisition_projection(&database.pool, episode_id)
            .await?
            .expect("projection");
        assert_eq!(projection.state, LibraryEpisodeAcquisitionState::Failed);
        assert_eq!(projection.reason_code.as_deref(), Some("cancelled"));
        Ok(())
    }
}
