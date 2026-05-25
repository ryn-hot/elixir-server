use std::{collections::BTreeSet, str::FromStr};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sqlx::{AnyPool, Row, TypeInfo, Value as SqlxValue, ValueRef, any::AnyRow};
use uuid::Uuid;

use crate::{
    db::models::MediaType, download_broker::DEBRID_DEFAULT_LOGICAL_ID, extensions::ExternalIds,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AcquisitionMonitorPolicy {
    AllMissing,
    FutureOnly,
    SelectedSeasons,
    SelectedTargets,
}

impl Default for AcquisitionMonitorPolicy {
    fn default() -> Self {
        Self::AllMissing
    }
}

impl AcquisitionMonitorPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AllMissing => "all_missing",
            Self::FutureOnly => "future_only",
            Self::SelectedSeasons => "selected_seasons",
            Self::SelectedTargets => "selected_targets",
        }
    }
}

impl FromStr for AcquisitionMonitorPolicy {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "all_missing" => Ok(Self::AllMissing),
            "future_only" => Ok(Self::FutureOnly),
            "selected_seasons" => Ok(Self::SelectedSeasons),
            "selected_targets" | "selected" => Ok(Self::SelectedTargets),
            other => bail!("unknown acquisition monitor policy '{other}'"),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AcquisitionRoutePolicy {
    DebridFirst,
    DebridOnly,
    TorrentOnly,
    Manual,
}

impl Default for AcquisitionRoutePolicy {
    fn default() -> Self {
        Self::DebridFirst
    }
}

impl AcquisitionRoutePolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DebridFirst => "debrid_first",
            Self::DebridOnly => "debrid_only",
            Self::TorrentOnly => "torrent_only",
            Self::Manual => "manual",
        }
    }
}

impl FromStr for AcquisitionRoutePolicy {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "debrid_first" => Ok(Self::DebridFirst),
            "debrid_only" => Ok(Self::DebridOnly),
            "torrent_only" => Ok(Self::TorrentOnly),
            "manual" => Ok(Self::Manual),
            other => bail!("unknown acquisition route policy '{other}'"),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AcquisitionSubscriptionStatus {
    Active,
    Paused,
    Completed,
    Cancelled,
}

impl Default for AcquisitionSubscriptionStatus {
    fn default() -> Self {
        Self::Active
    }
}

impl AcquisitionSubscriptionStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Paused => "paused",
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
        }
    }
}

impl FromStr for AcquisitionSubscriptionStatus {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "active" => Ok(Self::Active),
            "paused" => Ok(Self::Paused),
            "completed" => Ok(Self::Completed),
            "cancelled" | "canceled" => Ok(Self::Cancelled),
            other => bail!("unknown acquisition subscription status '{other}'"),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AcquisitionTargetState {
    Pending,
    Searching,
    Submitted,
    Blocked,
    Imported,
    Excluded,
}

impl Default for AcquisitionTargetState {
    fn default() -> Self {
        Self::Pending
    }
}

impl AcquisitionTargetState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Searching => "searching",
            Self::Submitted => "submitted",
            Self::Blocked => "blocked",
            Self::Imported => "imported",
            Self::Excluded => "excluded",
        }
    }
}

impl FromStr for AcquisitionTargetState {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "pending" => Ok(Self::Pending),
            "searching" => Ok(Self::Searching),
            "submitted" => Ok(Self::Submitted),
            "blocked" => Ok(Self::Blocked),
            "imported" => Ok(Self::Imported),
            "excluded" => Ok(Self::Excluded),
            other => bail!("unknown acquisition target state '{other}'"),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NewAcquisitionSubscription {
    pub media_type: MediaType,
    pub title: String,
    #[serde(default)]
    pub year: Option<i32>,
    #[serde(default)]
    pub external_ids: Option<ExternalIds>,
    #[serde(default)]
    pub monitor_policy: AcquisitionMonitorPolicy,
    #[serde(default)]
    pub route_policy: AcquisitionRoutePolicy,
    #[serde(default)]
    pub source_provider_id: Option<Uuid>,
    #[serde(default)]
    pub release_delay_seconds: Option<i64>,
    #[serde(default)]
    pub quality_profile: Option<JsonValue>,
    #[serde(default)]
    pub metadata_refresh_after: Option<DateTime<Utc>>,
    #[serde(default)]
    pub candidate_search_after: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AcquisitionSubscriptionUpdate {
    #[serde(default)]
    pub monitor_policy: Option<AcquisitionMonitorPolicy>,
    #[serde(default)]
    pub route_policy: Option<AcquisitionRoutePolicy>,
    #[serde(default)]
    pub source_provider_id: Option<Uuid>,
    #[serde(default)]
    pub release_delay_seconds: Option<i64>,
    #[serde(default)]
    pub quality_profile: Option<JsonValue>,
    #[serde(default)]
    pub metadata_refresh_after: Option<DateTime<Utc>>,
    #[serde(default)]
    pub candidate_search_after: Option<DateTime<Utc>>,
    #[serde(default)]
    pub status: Option<AcquisitionSubscriptionStatus>,
    #[serde(default)]
    pub active: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AcquisitionSubscription {
    pub subscription_id: Uuid,
    pub media_type: MediaType,
    pub title: String,
    pub normalized_title: String,
    pub year: Option<i32>,
    pub external_ids: Option<ExternalIds>,
    pub monitor_policy: AcquisitionMonitorPolicy,
    pub route_policy: AcquisitionRoutePolicy,
    pub source_provider_id: Option<Uuid>,
    pub release_delay_seconds: i64,
    pub quality_profile: Option<JsonValue>,
    pub metadata_refresh_after: DateTime<Utc>,
    pub candidate_search_after: DateTime<Utc>,
    pub last_metadata_refresh_at: Option<DateTime<Utc>>,
    pub last_candidate_search_at: Option<DateTime<Utc>>,
    pub tracking_started_at: Option<DateTime<Utc>>,
    pub status: AcquisitionSubscriptionStatus,
    pub active: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NewAcquisitionTarget {
    #[serde(default)]
    pub target_key: Option<String>,
    #[serde(default)]
    pub media_type: Option<MediaType>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub season_number: Option<i32>,
    #[serde(default)]
    pub episode_number: Option<i32>,
    #[serde(default)]
    pub absolute_episode_number: Option<i32>,
    #[serde(default)]
    pub air_date: Option<String>,
    #[serde(default)]
    pub air_time: Option<DateTime<Utc>>,
    #[serde(default)]
    pub metadata: Option<JsonValue>,
    #[serde(default)]
    pub state: Option<AcquisitionTargetState>,
    #[serde(default)]
    pub next_search_after: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AcquisitionTargetStateUpdate {
    pub state: AcquisitionTargetState,
    #[serde(default)]
    pub state_reason: Option<String>,
    #[serde(default)]
    pub selected_provider_id: Option<Uuid>,
    #[serde(default)]
    pub selected_route_logical_id: Option<String>,
    #[serde(default)]
    pub selected_candidate: Option<JsonValue>,
    #[serde(default)]
    pub download_id: Option<String>,
    #[serde(default)]
    pub import_event_id: Option<Uuid>,
    #[serde(default)]
    pub next_search_after: Option<DateTime<Utc>>,
    #[serde(default)]
    pub increment_search_attempts: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AcquisitionTarget {
    pub target_id: Uuid,
    pub subscription_id: Uuid,
    pub target_key: String,
    pub media_type: MediaType,
    pub title: String,
    pub season_number: Option<i32>,
    pub episode_number: Option<i32>,
    pub absolute_episode_number: Option<i32>,
    pub air_date: Option<String>,
    pub air_time: Option<DateTime<Utc>>,
    pub metadata: Option<JsonValue>,
    pub state: AcquisitionTargetState,
    pub state_reason: Option<String>,
    pub selected_provider_id: Option<Uuid>,
    pub selected_route_logical_id: Option<String>,
    pub selected_candidate: Option<JsonValue>,
    pub download_id: Option<String>,
    pub import_event_id: Option<Uuid>,
    pub search_attempts: i64,
    pub last_search_at: Option<DateTime<Utc>>,
    pub next_search_after: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AcquisitionSubscriptionDetail {
    pub subscription: AcquisitionSubscription,
    pub targets: Vec<AcquisitionTarget>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AcquisitionSubscriptionStopTrackingResult {
    pub subscription: AcquisitionSubscription,
    pub targets_excluded: u64,
    pub releases_cancelled: u64,
    pub release_jobs_cancelled: u64,
    pub coverage_rejected: u64,
}

#[derive(Debug, Clone, Default)]
pub struct AcquisitionSubscriptionFilter {
    pub active: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateAcquisitionIntent {
    pub media_type: MediaType,
    pub title: String,
    #[serde(default)]
    pub year: Option<i32>,
    #[serde(default)]
    pub external_ids: Option<ExternalIds>,
    #[serde(default)]
    pub monitor_policy: Option<AcquisitionMonitorPolicy>,
    #[serde(default)]
    pub route_policy: Option<AcquisitionRoutePolicy>,
    #[serde(default)]
    pub source_provider_id: Option<Uuid>,
    #[serde(default)]
    pub release_delay_seconds: Option<i64>,
    #[serde(default)]
    pub quality_profile: Option<JsonValue>,
    #[serde(default)]
    pub metadata_refresh_after: Option<DateTime<Utc>>,
    #[serde(default)]
    pub candidate_search_after: Option<DateTime<Utc>>,
    #[serde(default)]
    pub target: Option<AcquisitionIntentTarget>,
    #[serde(default)]
    pub targets: Vec<NewAcquisitionTarget>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AcquisitionIntentTarget {
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub target_key: Option<String>,
    #[serde(default)]
    pub target_keys: Vec<String>,
    #[serde(default)]
    pub season_number: Option<i32>,
    #[serde(default)]
    pub episode_number: Option<i32>,
    #[serde(default)]
    pub episode_start: Option<i32>,
    #[serde(default)]
    pub episode_end: Option<i32>,
    #[serde(default)]
    pub absolute_episode_number: Option<i32>,
    #[serde(default)]
    pub absolute_episode_start: Option<i32>,
    #[serde(default)]
    pub absolute_episode_end: Option<i32>,
    #[serde(default)]
    pub air_date: Option<String>,
    #[serde(default)]
    pub air_time: Option<DateTime<Utc>>,
    #[serde(default)]
    pub metadata: Option<JsonValue>,
    #[serde(default)]
    pub targets: Vec<NewAcquisitionTarget>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AcquisitionIntentCreation {
    pub created: bool,
    pub expanded_target_count: usize,
    pub detail: AcquisitionSubscriptionDetail,
}

pub async fn create_or_update_acquisition_intent(
    pool: &AnyPool,
    intent: CreateAcquisitionIntent,
    now: DateTime<Utc>,
) -> Result<AcquisitionIntentCreation> {
    validate_intent_input(&intent)?;
    let explicit_targets = intent_explicit_targets(&intent, now)?;
    let has_explicit_targets = !explicit_targets.is_empty();
    let subscription_data = intent_subscription_data(&intent, has_explicit_targets, now);

    let existing = find_subscription_by_intent_identity(pool, &subscription_data).await?;
    let (subscription, created) = if let Some(existing) = existing {
        let update = AcquisitionSubscriptionUpdate {
            monitor_policy: Some(subscription_data.monitor_policy),
            route_policy: Some(subscription_data.route_policy),
            source_provider_id: subscription_data.source_provider_id,
            release_delay_seconds: Some(
                subscription_data.release_delay_seconds.unwrap_or_default(),
            ),
            quality_profile: subscription_data.quality_profile.clone(),
            metadata_refresh_after: Some(subscription_data.metadata_refresh_after.unwrap_or(now)),
            candidate_search_after: Some(subscription_data.candidate_search_after.unwrap_or(now)),
            status: Some(AcquisitionSubscriptionStatus::Active),
            active: Some(true),
        };
        (
            update_subscription(pool, existing.subscription_id, update)
                .await?
                .ok_or_else(|| anyhow!("existing acquisition subscription was not readable"))?,
            false,
        )
    } else {
        (create_subscription(pool, subscription_data).await?, true)
    };

    if !explicit_targets.is_empty() {
        upsert_subscription_targets(pool, subscription.subscription_id, explicit_targets).await?;
    }

    let detail = get_subscription_detail(pool, subscription.subscription_id)
        .await?
        .ok_or_else(|| anyhow!("acquisition subscription was not readable"))?;
    Ok(AcquisitionIntentCreation {
        created,
        expanded_target_count: has_explicit_targets
            .then_some(detail.targets.len())
            .unwrap_or(0),
        detail,
    })
}

pub async fn create_subscription(
    pool: &AnyPool,
    data: NewAcquisitionSubscription,
) -> Result<AcquisitionSubscription> {
    validate_subscription_input(&data)?;
    let now = Utc::now();
    let subscription_id = Uuid::new_v4();
    let normalized_title = normalize_acquisition_title(&data.title);
    let external_ids_json = external_ids_json(data.external_ids.as_ref())?;
    let quality_profile_json = json_to_string(data.quality_profile.as_ref())?;
    let metadata_refresh_after = data.metadata_refresh_after.unwrap_or(now);
    let candidate_search_after = data.candidate_search_after.unwrap_or(now);
    let release_delay_seconds = data.release_delay_seconds.unwrap_or_default();

    sqlx::query::<sqlx::Any>(
        "INSERT INTO acquisition_subscriptions (
            subscription_id,
            media_type,
            title,
            normalized_title,
            year,
            external_ids_json,
            monitor_policy,
            route_policy,
            source_provider_id,
            release_delay_seconds,
            quality_profile_json,
            metadata_refresh_after,
            candidate_search_after,
            status,
            active
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', 1)",
    )
    .bind(subscription_id.to_string())
    .bind(data.media_type.as_str())
    .bind(data.title.trim())
    .bind(&normalized_title)
    .bind(data.year)
    .bind(external_ids_json.as_deref())
    .bind(data.monitor_policy.as_str())
    .bind(data.route_policy.as_str())
    .bind(data.source_provider_id.map(|value| value.to_string()))
    .bind(release_delay_seconds)
    .bind(quality_profile_json.as_deref())
    .bind(db_datetime_string(metadata_refresh_after))
    .bind(db_datetime_string(candidate_search_after))
    .execute(pool)
    .await
    .context("creating acquisition subscription")?;

    get_subscription(pool, subscription_id)
        .await?
        .ok_or_else(|| anyhow!("created acquisition subscription was not readable"))
}

pub async fn update_subscription(
    pool: &AnyPool,
    subscription_id: Uuid,
    update: AcquisitionSubscriptionUpdate,
) -> Result<Option<AcquisitionSubscription>> {
    let Some(existing) = get_subscription(pool, subscription_id).await? else {
        return Ok(None);
    };
    let release_delay_seconds = update
        .release_delay_seconds
        .unwrap_or(existing.release_delay_seconds);
    if release_delay_seconds < 0 {
        bail!("releaseDelaySeconds cannot be negative");
    }
    let quality_profile = update.quality_profile.or(existing.quality_profile);
    let quality_profile_json = json_to_string(quality_profile.as_ref())?;
    let source_provider_id = update.source_provider_id.or(existing.source_provider_id);
    let metadata_refresh_after = update
        .metadata_refresh_after
        .unwrap_or(existing.metadata_refresh_after);
    let candidate_search_after = update
        .candidate_search_after
        .unwrap_or(existing.candidate_search_after);
    let status = update.status.unwrap_or(existing.status);
    let active = update.active.unwrap_or(existing.active);

    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_subscriptions
         SET monitor_policy = ?,
             route_policy = ?,
             source_provider_id = ?,
             release_delay_seconds = ?,
             quality_profile_json = ?,
             metadata_refresh_after = ?,
             candidate_search_after = ?,
             status = ?,
             active = ?,
             updated_at = CURRENT_TIMESTAMP
         WHERE subscription_id = ?",
    )
    .bind(
        update
            .monitor_policy
            .unwrap_or(existing.monitor_policy)
            .as_str(),
    )
    .bind(
        update
            .route_policy
            .unwrap_or(existing.route_policy)
            .as_str(),
    )
    .bind(source_provider_id.map(|value| value.to_string()))
    .bind(release_delay_seconds)
    .bind(quality_profile_json.as_deref())
    .bind(db_datetime_string(metadata_refresh_after))
    .bind(db_datetime_string(candidate_search_after))
    .bind(status.as_str())
    .bind(active)
    .bind(subscription_id.to_string())
    .execute(pool)
    .await
    .context("updating acquisition subscription")?;

    get_subscription(pool, subscription_id).await
}

pub async fn update_subscription_external_ids(
    pool: &AnyPool,
    subscription_id: Uuid,
    external_ids: &ExternalIds,
) -> Result<Option<AcquisitionSubscription>> {
    let external_ids_json = external_ids_json(Some(external_ids))?;
    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_subscriptions
         SET external_ids_json = ?,
             updated_at = CURRENT_TIMESTAMP
         WHERE subscription_id = ?",
    )
    .bind(external_ids_json.as_deref())
    .bind(subscription_id.to_string())
    .execute(pool)
    .await
    .context("updating acquisition subscription external ids")?;

    get_subscription(pool, subscription_id).await
}

pub async fn stop_subscription_tracking(
    pool: &AnyPool,
    subscription_id: Uuid,
    reason: &str,
) -> Result<Option<AcquisitionSubscriptionStopTrackingResult>> {
    if get_subscription(pool, subscription_id).await?.is_none() {
        return Ok(None);
    }
    let reason = reason.trim();
    let reason = if reason.is_empty() {
        "User removed acquisition request."
    } else {
        reason
    };

    let target_result = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_targets
         SET state = 'excluded',
             state_reason = ?,
             next_search_after = NULL,
             updated_at = CURRENT_TIMESTAMP
         WHERE subscription_id = ?
           AND state IN ('pending', 'searching', 'blocked', 'submitted')",
    )
    .bind(reason)
    .bind(subscription_id.to_string())
    .execute(pool)
    .await
    .context("excluding acquisition targets for stopped subscription")?;

    let coverage_result = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_release_coverage
         SET state = 'rejected',
             reason = ?,
             verified_by = 'user_cancelled',
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id IN (
             SELECT release_id
             FROM acquisition_releases
             WHERE subscription_id = ?
         )
           AND state NOT IN ('imported', 'rejected')",
    )
    .bind(reason)
    .bind(subscription_id.to_string())
    .execute(pool)
    .await
    .context("rejecting acquisition release coverage for stopped subscription")?;

    let job_result = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_release_jobs
         SET state = 'cancelled',
             state_reason = ?,
             active = 0,
             completed_at = COALESCE(completed_at, CURRENT_TIMESTAMP),
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id IN (
             SELECT release_id
             FROM acquisition_releases
             WHERE subscription_id = ?
         )
           AND state NOT IN ('completed', 'cancelled')",
    )
    .bind(reason)
    .bind(subscription_id.to_string())
    .execute(pool)
    .await
    .context("cancelling acquisition release jobs for stopped subscription")?;

    let release_result = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_releases
         SET state = 'cancelled',
             state_reason = ?,
             updated_at = CURRENT_TIMESTAMP
         WHERE subscription_id = ?
           AND state NOT IN ('completed', 'cancelled')",
    )
    .bind(reason)
    .bind(subscription_id.to_string())
    .execute(pool)
    .await
    .context("cancelling acquisition releases for stopped subscription")?;

    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_subscriptions
         SET status = 'cancelled',
             active = 0,
             candidate_search_after = CURRENT_TIMESTAMP,
             metadata_refresh_after = CURRENT_TIMESTAMP,
             updated_at = CURRENT_TIMESTAMP
         WHERE subscription_id = ?",
    )
    .bind(subscription_id.to_string())
    .execute(pool)
    .await
    .context("stopping acquisition subscription tracking")?;

    let subscription = get_subscription(pool, subscription_id)
        .await?
        .ok_or_else(|| anyhow!("stopped acquisition subscription was not readable"))?;
    Ok(Some(AcquisitionSubscriptionStopTrackingResult {
        subscription,
        targets_excluded: target_result.rows_affected(),
        releases_cancelled: release_result.rows_affected(),
        release_jobs_cancelled: job_result.rows_affected(),
        coverage_rejected: coverage_result.rows_affected(),
    }))
}

pub async fn list_subscriptions(
    pool: &AnyPool,
    filter: AcquisitionSubscriptionFilter,
) -> Result<Vec<AcquisitionSubscription>> {
    let rows = if let Some(active) = filter.active {
        sqlx::query(
            "SELECT
                subscription_id,
                media_type,
                title,
                normalized_title,
                year,
                CAST(external_ids_json AS TEXT) AS external_ids_json,
                monitor_policy,
                route_policy,
                CAST(source_provider_id AS TEXT) AS source_provider_id,
                release_delay_seconds,
                CAST(quality_profile_json AS TEXT) AS quality_profile_json,
                CAST(metadata_refresh_after AS TEXT) AS metadata_refresh_after,
                CAST(candidate_search_after AS TEXT) AS candidate_search_after,
                CAST(last_metadata_refresh_at AS TEXT) AS last_metadata_refresh_at,
                CAST(last_candidate_search_at AS TEXT) AS last_candidate_search_at,
                CAST(tracking_started_at AS TEXT) AS tracking_started_at,
                status,
                CAST(active AS INTEGER) AS active,
                CAST(created_at AS TEXT) AS created_at,
                CAST(updated_at AS TEXT) AS updated_at
             FROM acquisition_subscriptions
             WHERE active = ?
             ORDER BY created_at DESC",
        )
        .bind(active)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query(
            "SELECT
                subscription_id,
                media_type,
                title,
                normalized_title,
                year,
                CAST(external_ids_json AS TEXT) AS external_ids_json,
                monitor_policy,
                route_policy,
                CAST(source_provider_id AS TEXT) AS source_provider_id,
                release_delay_seconds,
                CAST(quality_profile_json AS TEXT) AS quality_profile_json,
                CAST(metadata_refresh_after AS TEXT) AS metadata_refresh_after,
                CAST(candidate_search_after AS TEXT) AS candidate_search_after,
                CAST(last_metadata_refresh_at AS TEXT) AS last_metadata_refresh_at,
                CAST(last_candidate_search_at AS TEXT) AS last_candidate_search_at,
                CAST(tracking_started_at AS TEXT) AS tracking_started_at,
                status,
                CAST(active AS INTEGER) AS active,
                CAST(created_at AS TEXT) AS created_at,
                CAST(updated_at AS TEXT) AS updated_at
             FROM acquisition_subscriptions
             ORDER BY created_at DESC",
        )
        .fetch_all(pool)
        .await?
    };

    rows.into_iter().map(|row| map_subscription(&row)).collect()
}

pub async fn get_subscription(
    pool: &AnyPool,
    subscription_id: Uuid,
) -> Result<Option<AcquisitionSubscription>> {
    let row = sqlx::query(
        "SELECT
            subscription_id,
            media_type,
            title,
            normalized_title,
            year,
            CAST(external_ids_json AS TEXT) AS external_ids_json,
            monitor_policy,
            route_policy,
            CAST(source_provider_id AS TEXT) AS source_provider_id,
            release_delay_seconds,
            CAST(quality_profile_json AS TEXT) AS quality_profile_json,
            CAST(metadata_refresh_after AS TEXT) AS metadata_refresh_after,
            CAST(candidate_search_after AS TEXT) AS candidate_search_after,
            CAST(last_metadata_refresh_at AS TEXT) AS last_metadata_refresh_at,
            CAST(last_candidate_search_at AS TEXT) AS last_candidate_search_at,
            CAST(tracking_started_at AS TEXT) AS tracking_started_at,
            status,
            CAST(active AS INTEGER) AS active,
            CAST(created_at AS TEXT) AS created_at,
            CAST(updated_at AS TEXT) AS updated_at
         FROM acquisition_subscriptions
         WHERE subscription_id = ?
         LIMIT 1",
    )
    .bind(subscription_id.to_string())
    .fetch_optional(pool)
    .await?;
    row.map(|row| map_subscription(&row)).transpose()
}

async fn find_subscription_by_intent_identity(
    pool: &AnyPool,
    data: &NewAcquisitionSubscription,
) -> Result<Option<AcquisitionSubscription>> {
    let normalized_title = normalize_acquisition_title(&data.title);
    let rows = if let Some(year) = data.year {
        sqlx::query(
            "SELECT
                subscription_id,
                media_type,
                title,
                normalized_title,
                year,
                CAST(external_ids_json AS TEXT) AS external_ids_json,
                monitor_policy,
                route_policy,
                CAST(source_provider_id AS TEXT) AS source_provider_id,
                release_delay_seconds,
                CAST(quality_profile_json AS TEXT) AS quality_profile_json,
                CAST(metadata_refresh_after AS TEXT) AS metadata_refresh_after,
                CAST(candidate_search_after AS TEXT) AS candidate_search_after,
                CAST(last_metadata_refresh_at AS TEXT) AS last_metadata_refresh_at,
                CAST(last_candidate_search_at AS TEXT) AS last_candidate_search_at,
                CAST(tracking_started_at AS TEXT) AS tracking_started_at,
                status,
                CAST(active AS INTEGER) AS active,
                CAST(created_at AS TEXT) AS created_at,
                CAST(updated_at AS TEXT) AS updated_at
             FROM acquisition_subscriptions
             WHERE media_type = ?
               AND normalized_title = ?
               AND year = ?
               AND active = 1
             ORDER BY created_at ASC
             LIMIT 25",
        )
        .bind(data.media_type.as_str())
        .bind(&normalized_title)
        .bind(year)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query(
            "SELECT
                subscription_id,
                media_type,
                title,
                normalized_title,
                year,
                CAST(external_ids_json AS TEXT) AS external_ids_json,
                monitor_policy,
                route_policy,
                CAST(source_provider_id AS TEXT) AS source_provider_id,
                release_delay_seconds,
                CAST(quality_profile_json AS TEXT) AS quality_profile_json,
                CAST(metadata_refresh_after AS TEXT) AS metadata_refresh_after,
                CAST(candidate_search_after AS TEXT) AS candidate_search_after,
                CAST(last_metadata_refresh_at AS TEXT) AS last_metadata_refresh_at,
                CAST(last_candidate_search_at AS TEXT) AS last_candidate_search_at,
                CAST(tracking_started_at AS TEXT) AS tracking_started_at,
                status,
                CAST(active AS INTEGER) AS active,
                CAST(created_at AS TEXT) AS created_at,
                CAST(updated_at AS TEXT) AS updated_at
             FROM acquisition_subscriptions
             WHERE media_type = ?
               AND normalized_title = ?
               AND year IS NULL
               AND active = 1
             ORDER BY created_at ASC
             LIMIT 25",
        )
        .bind(data.media_type.as_str())
        .bind(&normalized_title)
        .fetch_all(pool)
        .await?
    };
    let subscriptions = rows
        .iter()
        .map(map_subscription)
        .collect::<Result<Vec<_>>>()?;
    Ok(match data.external_ids.as_ref() {
        Some(request_ids) if external_ids_has_value(request_ids) => subscriptions
            .iter()
            .find(|subscription| {
                subscription
                    .external_ids
                    .as_ref()
                    .is_some_and(|existing| external_ids_overlap(existing, request_ids))
            })
            .cloned()
            .or_else(|| {
                subscriptions
                    .iter()
                    .all(|subscription| {
                        subscription
                            .external_ids
                            .as_ref()
                            .is_none_or(|ids| !external_ids_has_value(ids))
                    })
                    .then(|| subscriptions.first().cloned())
                    .flatten()
            }),
        _ => subscriptions.into_iter().next(),
    })
}

pub async fn get_subscription_detail(
    pool: &AnyPool,
    subscription_id: Uuid,
) -> Result<Option<AcquisitionSubscriptionDetail>> {
    let Some(subscription) = get_subscription(pool, subscription_id).await? else {
        return Ok(None);
    };
    let targets = list_subscription_targets(pool, subscription_id).await?;
    Ok(Some(AcquisitionSubscriptionDetail {
        subscription,
        targets,
    }))
}

pub async fn upsert_subscription_targets(
    pool: &AnyPool,
    subscription_id: Uuid,
    targets: Vec<NewAcquisitionTarget>,
) -> Result<Vec<AcquisitionTarget>> {
    let subscription = get_subscription(pool, subscription_id)
        .await?
        .ok_or_else(|| anyhow!("acquisition subscription '{subscription_id}' was not found"))?;
    validate_new_targets(subscription.media_type, &targets)?;

    for target in targets {
        upsert_subscription_target(pool, &subscription, target).await?;
    }
    list_subscription_targets(pool, subscription_id).await
}

pub fn validate_new_targets(media_type: MediaType, targets: &[NewAcquisitionTarget]) -> Result<()> {
    let mut seen_keys = BTreeSet::new();
    for target in targets {
        validate_target_input(media_type, target)?;
        let target_key = normalized_target_key(
            target
                .target_key
                .clone()
                .unwrap_or_else(|| generated_target_key(media_type, target)),
        )?;
        if !seen_keys.insert(target_key) {
            bail!("duplicate acquisition target key in request");
        }
    }
    Ok(())
}

pub async fn list_subscription_targets(
    pool: &AnyPool,
    subscription_id: Uuid,
) -> Result<Vec<AcquisitionTarget>> {
    let rows = sqlx::query(
        "SELECT
            target_id,
            subscription_id,
            target_key,
            media_type,
            title,
            season_number,
            episode_number,
            absolute_episode_number,
            CAST(air_date AS TEXT) AS air_date,
            CAST(air_time AS TEXT) AS air_time,
            CAST(metadata_json AS TEXT) AS metadata_json,
            state,
            CAST(state_reason AS TEXT) AS state_reason,
            CAST(selected_provider_id AS TEXT) AS selected_provider_id,
            CAST(selected_route_logical_id AS TEXT) AS selected_route_logical_id,
            CAST(selected_candidate_json AS TEXT) AS selected_candidate_json,
            CAST(download_id AS TEXT) AS download_id,
            CAST(import_event_id AS TEXT) AS import_event_id,
            search_attempts,
            CAST(last_search_at AS TEXT) AS last_search_at,
            CAST(next_search_after AS TEXT) AS next_search_after,
            CAST(created_at AS TEXT) AS created_at,
            CAST(updated_at AS TEXT) AS updated_at
         FROM acquisition_targets
         WHERE subscription_id = ?
         ORDER BY season_number, episode_number, absolute_episode_number, target_key",
    )
    .bind(subscription_id.to_string())
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(|row| map_target(&row)).collect()
}

pub async fn update_target_state(
    pool: &AnyPool,
    target_id: Uuid,
    update: AcquisitionTargetStateUpdate,
) -> Result<Option<AcquisitionTarget>> {
    let Some(existing) = get_target(pool, target_id).await? else {
        return Ok(None);
    };
    let selected_candidate = update.selected_candidate.or(existing.selected_candidate);
    let selected_candidate_json = json_to_string(selected_candidate.as_ref())?;
    let import_event_id = update
        .import_event_id
        .or(existing.import_event_id)
        .map(|value| value.to_string());
    let search_attempts = if update.increment_search_attempts {
        existing.search_attempts.saturating_add(1)
    } else {
        existing.search_attempts
    };
    let last_search_at = if update.increment_search_attempts {
        Some(Utc::now())
    } else {
        existing.last_search_at
    };

    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_targets
         SET state = ?,
             state_reason = ?,
             selected_provider_id = ?,
             selected_route_logical_id = ?,
             selected_candidate_json = ?,
             download_id = ?,
             import_event_id = ?,
             search_attempts = ?,
             last_search_at = ?,
             next_search_after = ?,
             updated_at = CURRENT_TIMESTAMP
         WHERE target_id = ?",
    )
    .bind(update.state.as_str())
    .bind(update.state_reason.or(existing.state_reason))
    .bind(
        update
            .selected_provider_id
            .or(existing.selected_provider_id)
            .map(|value| value.to_string()),
    )
    .bind(
        update
            .selected_route_logical_id
            .or(existing.selected_route_logical_id),
    )
    .bind(selected_candidate_json.as_deref())
    .bind(update.download_id.or(existing.download_id))
    .bind(import_event_id.as_deref())
    .bind(search_attempts)
    .bind(last_search_at.map(db_datetime_string))
    .bind(
        update
            .next_search_after
            .or(existing.next_search_after)
            .map(db_datetime_string),
    )
    .bind(target_id.to_string())
    .execute(pool)
    .await
    .context("updating acquisition target state")?;

    get_target(pool, target_id).await
}

pub async fn reset_target_for_candidate_retry(
    pool: &AnyPool,
    target_id: Uuid,
    state_reason: String,
    next_search_after: DateTime<Utc>,
) -> Result<Option<AcquisitionTarget>> {
    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_targets
         SET state = ?,
             state_reason = ?,
             selected_provider_id = NULL,
             selected_route_logical_id = NULL,
             selected_candidate_json = NULL,
             download_id = NULL,
             next_search_after = ?,
             updated_at = CURRENT_TIMESTAMP
         WHERE target_id = ?",
    )
    .bind(AcquisitionTargetState::Pending.as_str())
    .bind(state_reason)
    .bind(db_datetime_string(next_search_after))
    .bind(target_id.to_string())
    .execute(pool)
    .await
    .context("resetting acquisition target for candidate retry")?;

    get_target(pool, target_id).await
}

pub async fn clear_target_next_search_after(pool: &AnyPool, target_id: Uuid) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_targets
         SET next_search_after = NULL,
             updated_at = CURRENT_TIMESTAMP
         WHERE target_id = ?",
    )
    .bind(target_id.to_string())
    .execute(pool)
    .await
    .context("clearing acquisition target search schedule")?;
    Ok(())
}

pub async fn list_due_metadata_subscriptions(
    pool: &AnyPool,
    now: DateTime<Utc>,
    limit: i64,
) -> Result<Vec<AcquisitionSubscription>> {
    let rows = sqlx::query(
        "SELECT
            subscription_id,
            media_type,
            title,
            normalized_title,
            year,
            CAST(external_ids_json AS TEXT) AS external_ids_json,
            monitor_policy,
            route_policy,
            CAST(source_provider_id AS TEXT) AS source_provider_id,
            release_delay_seconds,
            CAST(quality_profile_json AS TEXT) AS quality_profile_json,
            CAST(metadata_refresh_after AS TEXT) AS metadata_refresh_after,
            CAST(candidate_search_after AS TEXT) AS candidate_search_after,
            CAST(last_metadata_refresh_at AS TEXT) AS last_metadata_refresh_at,
            CAST(last_candidate_search_at AS TEXT) AS last_candidate_search_at,
            CAST(tracking_started_at AS TEXT) AS tracking_started_at,
            status,
            CAST(active AS INTEGER) AS active,
            CAST(created_at AS TEXT) AS created_at,
            CAST(updated_at AS TEXT) AS updated_at
         FROM acquisition_subscriptions
         WHERE active = 1
           AND status = 'active'
           AND metadata_refresh_after <= ?
           AND (
                tracking_started_at IS NOT NULL
                OR last_metadata_refresh_at IS NULL
                OR NOT EXISTS (
                    SELECT 1
                    FROM acquisition_targets t
                    WHERE t.subscription_id = acquisition_subscriptions.subscription_id
                )
           )
         ORDER BY metadata_refresh_after ASC
         LIMIT ?",
    )
    .bind(db_datetime_string(now))
    .bind(limit.max(1))
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(|row| map_subscription(&row)).collect()
}

pub async fn list_due_candidate_targets(
    pool: &AnyPool,
    now: DateTime<Utc>,
    limit: i64,
) -> Result<Vec<AcquisitionTarget>> {
    let rows = sqlx::query(
        "SELECT
            t.target_id,
            t.subscription_id,
            t.target_key,
            t.media_type,
            t.title,
            t.season_number,
            t.episode_number,
            t.absolute_episode_number,
            CAST(t.air_date AS TEXT) AS air_date,
            CAST(t.air_time AS TEXT) AS air_time,
            CAST(t.metadata_json AS TEXT) AS metadata_json,
            t.state,
            CAST(t.state_reason AS TEXT) AS state_reason,
            CAST(t.selected_provider_id AS TEXT) AS selected_provider_id,
            CAST(t.selected_route_logical_id AS TEXT) AS selected_route_logical_id,
            CAST(t.selected_candidate_json AS TEXT) AS selected_candidate_json,
            CAST(t.download_id AS TEXT) AS download_id,
            CAST(t.import_event_id AS TEXT) AS import_event_id,
            t.search_attempts,
            CAST(t.last_search_at AS TEXT) AS last_search_at,
            CAST(t.next_search_after AS TEXT) AS next_search_after,
            CAST(t.created_at AS TEXT) AS created_at,
            CAST(t.updated_at AS TEXT) AS updated_at
         FROM acquisition_targets t
         JOIN acquisition_subscriptions s ON s.subscription_id = t.subscription_id
         WHERE s.active = 1
           AND s.status = 'active'
           AND t.state IN ('pending', 'searching', 'blocked')
           AND COALESCE(t.next_search_after, s.candidate_search_after) <= ?
           AND (t.air_time IS NULL OR t.air_time <= ?)
         ORDER BY COALESCE(t.next_search_after, s.candidate_search_after) ASC
         LIMIT ?",
    )
    .bind(db_datetime_string(now))
    .bind(db_datetime_string(now))
    .bind(limit.max(1))
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(|row| map_target(&row)).collect()
}

pub async fn record_metadata_refresh(
    pool: &AnyPool,
    subscription_id: Uuid,
    next_after: DateTime<Utc>,
) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_subscriptions
         SET last_metadata_refresh_at = CURRENT_TIMESTAMP,
             metadata_refresh_after = ?,
             updated_at = CURRENT_TIMESTAMP
         WHERE subscription_id = ?",
    )
    .bind(db_datetime_string(next_after))
    .bind(subscription_id.to_string())
    .execute(pool)
    .await
    .context("recording acquisition metadata refresh")?;
    Ok(())
}

pub async fn start_subscription_tracking_if_initial_download_complete(
    pool: &AnyPool,
    subscription_id: Uuid,
    now: DateTime<Utc>,
) -> Result<bool> {
    let incomplete_due_targets: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM acquisition_targets
         WHERE subscription_id = ?
           AND state IN ('pending', 'searching', 'blocked', 'submitted')
           AND (air_time IS NULL OR air_time <= ?)",
    )
    .bind(subscription_id.to_string())
    .bind(db_datetime_string(now))
    .fetch_one(pool)
    .await
    .context("checking initial acquisition target completion")?;

    if incomplete_due_targets != 0 {
        return Ok(false);
    }

    let result = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_subscriptions
         SET tracking_started_at = ?,
             metadata_refresh_after = ?,
             updated_at = CURRENT_TIMESTAMP
         WHERE subscription_id = ?
           AND tracking_started_at IS NULL",
    )
    .bind(db_datetime_string(now))
    .bind(db_datetime_string(now))
    .bind(subscription_id.to_string())
    .execute(pool)
    .await
    .context("starting acquisition subscription tracking")?;

    Ok(result.rows_affected() > 0)
}

#[allow(dead_code)]
pub async fn record_candidate_search(
    pool: &AnyPool,
    target_id: Uuid,
    next_after: DateTime<Utc>,
) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_targets
         SET last_search_at = CURRENT_TIMESTAMP,
             next_search_after = ?,
             search_attempts = search_attempts + 1,
             updated_at = CURRENT_TIMESTAMP
         WHERE target_id = ?",
    )
    .bind(db_datetime_string(next_after))
    .bind(target_id.to_string())
    .execute(pool)
    .await
    .context("recording acquisition candidate search")?;
    Ok(())
}

pub async fn list_submitted_debrid_targets(
    pool: &AnyPool,
    limit: i64,
) -> Result<Vec<AcquisitionTarget>> {
    let rows = sqlx::query(
        "SELECT
            t.target_id,
            t.subscription_id,
            t.target_key,
            t.media_type,
            t.title,
            t.season_number,
            t.episode_number,
            t.absolute_episode_number,
            CAST(t.air_date AS TEXT) AS air_date,
            CAST(t.air_time AS TEXT) AS air_time,
            CAST(t.metadata_json AS TEXT) AS metadata_json,
            t.state,
            CAST(t.state_reason AS TEXT) AS state_reason,
            CAST(t.selected_provider_id AS TEXT) AS selected_provider_id,
            CAST(t.selected_route_logical_id AS TEXT) AS selected_route_logical_id,
            CAST(t.selected_candidate_json AS TEXT) AS selected_candidate_json,
            CAST(t.download_id AS TEXT) AS download_id,
            CAST(t.import_event_id AS TEXT) AS import_event_id,
            t.search_attempts,
            CAST(t.last_search_at AS TEXT) AS last_search_at,
            CAST(t.next_search_after AS TEXT) AS next_search_after,
            CAST(t.created_at AS TEXT) AS created_at,
            CAST(t.updated_at AS TEXT) AS updated_at
         FROM acquisition_targets t
         JOIN acquisition_subscriptions s ON s.subscription_id = t.subscription_id
         WHERE s.active = 1
           AND s.status = 'active'
           AND t.state = 'submitted'
           AND t.selected_route_logical_id = ?
           AND t.download_id IS NOT NULL
         ORDER BY t.updated_at ASC
         LIMIT ?",
    )
    .bind(DEBRID_DEFAULT_LOGICAL_ID)
    .bind(limit.max(1))
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(|row| map_target(&row)).collect()
}

async fn upsert_subscription_target(
    pool: &AnyPool,
    subscription: &AcquisitionSubscription,
    target: NewAcquisitionTarget,
) -> Result<()> {
    validate_target_input(subscription.media_type, &target)?;
    let target_key = normalized_target_key(
        target
            .target_key
            .clone()
            .unwrap_or_else(|| generated_target_key(subscription.media_type, &target)),
    )?;
    let existing = get_target_by_key(pool, subscription.subscription_id, &target_key).await?;
    let media_type = target.media_type.unwrap_or(subscription.media_type);
    let title = target
        .title
        .clone()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| subscription.title.clone());
    let metadata = match existing.as_ref() {
        Some(existing) => target
            .metadata
            .clone()
            .or_else(|| existing.metadata.clone()),
        None => target.metadata.clone(),
    };
    let metadata_json = json_to_string(metadata.as_ref())?;

    if let Some(existing) = existing {
        sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_targets
             SET media_type = ?,
                 title = ?,
                 season_number = ?,
                 episode_number = ?,
                 absolute_episode_number = ?,
                 air_date = ?,
                 air_time = ?,
                 metadata_json = ?,
                 state = ?,
                 state_reason = ?,
                 next_search_after = ?,
                 updated_at = CURRENT_TIMESTAMP
             WHERE target_id = ?",
        )
        .bind(media_type.as_str())
        .bind(title)
        .bind(target.season_number)
        .bind(target.episode_number)
        .bind(target.absolute_episode_number)
        .bind(target.air_date.as_deref())
        .bind(target.air_time.map(db_datetime_string))
        .bind(metadata_json.as_deref())
        .bind(refreshed_target_state(existing.state, target.state).as_str())
        .bind(existing.state_reason.as_deref())
        .bind(
            target
                .next_search_after
                .or(existing.next_search_after)
                .map(db_datetime_string),
        )
        .bind(existing.target_id.to_string())
        .execute(pool)
        .await
        .context("updating acquisition target")?;
    } else {
        let target_id = Uuid::new_v4();
        sqlx::query::<sqlx::Any>(
            "INSERT INTO acquisition_targets (
                target_id,
                subscription_id,
                target_key,
                media_type,
                title,
                season_number,
                episode_number,
                absolute_episode_number,
                air_date,
                air_time,
                metadata_json,
                state,
                next_search_after
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(target_id.to_string())
        .bind(subscription.subscription_id.to_string())
        .bind(target_key)
        .bind(media_type.as_str())
        .bind(title)
        .bind(target.season_number)
        .bind(target.episode_number)
        .bind(target.absolute_episode_number)
        .bind(target.air_date.as_deref())
        .bind(target.air_time.map(db_datetime_string))
        .bind(metadata_json.as_deref())
        .bind(target.state.unwrap_or_default().as_str())
        .bind(
            target
                .next_search_after
                .unwrap_or(subscription.candidate_search_after)
                .format("%Y-%m-%d %H:%M:%S")
                .to_string(),
        )
        .execute(pool)
        .await
        .context("creating acquisition target")?;
    }
    Ok(())
}

fn refreshed_target_state(
    existing: AcquisitionTargetState,
    requested: Option<AcquisitionTargetState>,
) -> AcquisitionTargetState {
    match (existing, requested) {
        (
            AcquisitionTargetState::Submitted | AcquisitionTargetState::Imported,
            Some(AcquisitionTargetState::Pending | AcquisitionTargetState::Searching),
        ) => existing,
        (_, Some(state)) => state,
        _ => existing,
    }
}

pub async fn get_target(pool: &AnyPool, target_id: Uuid) -> Result<Option<AcquisitionTarget>> {
    let row = sqlx::query(
        "SELECT
            target_id,
            subscription_id,
            target_key,
            media_type,
            title,
            season_number,
            episode_number,
            absolute_episode_number,
            CAST(air_date AS TEXT) AS air_date,
            CAST(air_time AS TEXT) AS air_time,
            CAST(metadata_json AS TEXT) AS metadata_json,
            state,
            CAST(state_reason AS TEXT) AS state_reason,
            CAST(selected_provider_id AS TEXT) AS selected_provider_id,
            CAST(selected_route_logical_id AS TEXT) AS selected_route_logical_id,
            CAST(selected_candidate_json AS TEXT) AS selected_candidate_json,
            CAST(download_id AS TEXT) AS download_id,
            CAST(import_event_id AS TEXT) AS import_event_id,
            search_attempts,
            CAST(last_search_at AS TEXT) AS last_search_at,
            CAST(next_search_after AS TEXT) AS next_search_after,
            CAST(created_at AS TEXT) AS created_at,
            CAST(updated_at AS TEXT) AS updated_at
         FROM acquisition_targets
         WHERE target_id = ?
         LIMIT 1",
    )
    .bind(target_id.to_string())
    .fetch_optional(pool)
    .await?;
    row.map(|row| map_target(&row)).transpose()
}

async fn get_target_by_key(
    pool: &AnyPool,
    subscription_id: Uuid,
    target_key: &str,
) -> Result<Option<AcquisitionTarget>> {
    let row = sqlx::query(
        "SELECT
            target_id,
            subscription_id,
            target_key,
            media_type,
            title,
            season_number,
            episode_number,
            absolute_episode_number,
            CAST(air_date AS TEXT) AS air_date,
            CAST(air_time AS TEXT) AS air_time,
            CAST(metadata_json AS TEXT) AS metadata_json,
            state,
            CAST(state_reason AS TEXT) AS state_reason,
            CAST(selected_provider_id AS TEXT) AS selected_provider_id,
            CAST(selected_route_logical_id AS TEXT) AS selected_route_logical_id,
            CAST(selected_candidate_json AS TEXT) AS selected_candidate_json,
            CAST(download_id AS TEXT) AS download_id,
            CAST(import_event_id AS TEXT) AS import_event_id,
            search_attempts,
            CAST(last_search_at AS TEXT) AS last_search_at,
            CAST(next_search_after AS TEXT) AS next_search_after,
            CAST(created_at AS TEXT) AS created_at,
            CAST(updated_at AS TEXT) AS updated_at
         FROM acquisition_targets
         WHERE subscription_id = ? AND target_key = ?
         LIMIT 1",
    )
    .bind(subscription_id.to_string())
    .bind(target_key)
    .fetch_optional(pool)
    .await?;
    row.map(|row| map_target(&row)).transpose()
}

fn validate_subscription_input(data: &NewAcquisitionSubscription) -> Result<()> {
    if data.title.trim().is_empty() {
        bail!("title is required");
    }
    if data.release_delay_seconds.unwrap_or_default() < 0 {
        bail!("releaseDelaySeconds cannot be negative");
    }
    Ok(())
}

fn validate_intent_input(intent: &CreateAcquisitionIntent) -> Result<()> {
    if intent.title.trim().is_empty() {
        bail!("title is required");
    }
    if intent.release_delay_seconds.unwrap_or_default() < 0 {
        bail!("releaseDelaySeconds cannot be negative");
    }
    Ok(())
}

fn intent_subscription_data(
    intent: &CreateAcquisitionIntent,
    has_explicit_targets: bool,
    now: DateTime<Utc>,
) -> NewAcquisitionSubscription {
    NewAcquisitionSubscription {
        media_type: intent.media_type,
        title: intent.title.trim().to_string(),
        year: intent.year,
        external_ids: intent.external_ids.clone(),
        monitor_policy: intent.monitor_policy.unwrap_or_else(|| {
            if has_explicit_targets {
                AcquisitionMonitorPolicy::SelectedTargets
            } else {
                AcquisitionMonitorPolicy::AllMissing
            }
        }),
        route_policy: intent.route_policy.unwrap_or_default(),
        source_provider_id: intent.source_provider_id,
        release_delay_seconds: intent.release_delay_seconds,
        quality_profile: intent.quality_profile.clone(),
        metadata_refresh_after: Some(intent.metadata_refresh_after.unwrap_or(now)),
        candidate_search_after: Some(intent.candidate_search_after.unwrap_or(now)),
    }
}

fn intent_explicit_targets(
    intent: &CreateAcquisitionIntent,
    now: DateTime<Utc>,
) -> Result<Vec<NewAcquisitionTarget>> {
    let mut targets = intent.targets.clone();
    if let Some(scope) = intent.target.as_ref() {
        targets.extend(scope.targets.clone());
        targets.extend(targets_from_scope(intent, scope, now)?);
    }
    if targets.is_empty() && intent.media_type == MediaType::Movie {
        targets.push(movie_intent_target(intent, now));
    }
    validate_new_targets(intent.media_type, &targets)?;
    Ok(targets)
}

fn targets_from_scope(
    intent: &CreateAcquisitionIntent,
    scope: &AcquisitionIntentTarget,
    now: DateTime<Utc>,
) -> Result<Vec<NewAcquisitionTarget>> {
    let mut targets = Vec::new();
    if let Some(key) = scope.target_key.as_deref() {
        targets.push(target_from_key(intent, scope, key, now)?);
    }
    for key in &scope.target_keys {
        targets.push(target_from_key(intent, scope, key, now)?);
    }
    if let (Some(season), Some(episode)) = (scope.season_number, scope.episode_number) {
        targets.push(episode_intent_target(
            intent, scope, season, episode, None, now,
        )?);
    }
    if let Some(absolute) = scope.absolute_episode_number {
        targets.push(absolute_intent_target(intent, scope, absolute, now)?);
    }
    if let Some(season) = scope.season_number
        && let Some(start) = scope.episode_start
    {
        let end = scope.episode_end.unwrap_or(start);
        validate_target_range(start, end, "episode")?;
        for episode in start.min(end)..=start.max(end) {
            targets.push(episode_intent_target(
                intent, scope, season, episode, None, now,
            )?);
        }
    }
    if let Some(start) = scope.absolute_episode_start {
        let end = scope.absolute_episode_end.unwrap_or(start);
        validate_target_range(start, end, "absolute episode")?;
        for absolute in start.min(end)..=start.max(end) {
            targets.push(absolute_intent_target(intent, scope, absolute, now)?);
        }
    }
    if targets.is_empty() && is_movie_scope(intent, scope) {
        targets.push(movie_intent_target(intent, now));
    }
    if targets.is_empty() && is_explicit_scope_without_targets(scope) {
        bail!(
            "target scope '{}' requires targetKey, targetKeys, episodeNumber, episode range, absoluteEpisodeNumber, absolute episode range, or targets",
            scope.kind.as_deref().unwrap_or("selected")
        );
    }
    Ok(targets)
}

fn is_movie_scope(intent: &CreateAcquisitionIntent, scope: &AcquisitionIntentTarget) -> bool {
    intent.media_type == MediaType::Movie
        || scope
            .kind
            .as_deref()
            .map(|value| value.eq_ignore_ascii_case("movie"))
            .unwrap_or(false)
}

fn is_explicit_scope_without_targets(scope: &AcquisitionIntentTarget) -> bool {
    scope
        .kind
        .as_deref()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "episode"
                    | "season"
                    | "season_pack"
                    | "backlog"
                    | "selected"
                    | "selected_targets"
                    | "absolute_episode"
            )
        })
        .unwrap_or(false)
}

fn validate_target_range(start: i32, end: i32, label: &str) -> Result<()> {
    if start <= 0 || end <= 0 {
        bail!("{label} range values must be greater than zero");
    }
    if (start - end).abs() > 2000 {
        bail!("{label} range is too large");
    }
    Ok(())
}

fn movie_intent_target(
    intent: &CreateAcquisitionIntent,
    now: DateTime<Utc>,
) -> NewAcquisitionTarget {
    NewAcquisitionTarget {
        target_key: Some("movie".to_string()),
        media_type: Some(MediaType::Movie),
        title: Some(intent.title.trim().to_string()),
        season_number: None,
        episode_number: None,
        absolute_episode_number: None,
        air_date: None,
        air_time: None,
        metadata: Some(intent_target_metadata(intent, None)),
        state: Some(AcquisitionTargetState::Pending),
        next_search_after: Some(now),
    }
}

fn episode_intent_target(
    intent: &CreateAcquisitionIntent,
    scope: &AcquisitionIntentTarget,
    season: i32,
    episode: i32,
    target_key: Option<String>,
    now: DateTime<Utc>,
) -> Result<NewAcquisitionTarget> {
    if season <= 0 || episode <= 0 {
        bail!("seasonNumber and episodeNumber must be greater than zero");
    }
    Ok(NewAcquisitionTarget {
        target_key: target_key.or_else(|| Some(format!("S{season:02}E{episode:02}"))),
        media_type: Some(intent.media_type),
        title: target_title(intent, scope, Some(season), Some(episode), None),
        season_number: Some(season),
        episode_number: Some(episode),
        absolute_episode_number: None,
        air_date: scope.air_date.clone(),
        air_time: scope.air_time,
        metadata: Some(intent_target_metadata(intent, Some(scope))),
        state: Some(AcquisitionTargetState::Pending),
        next_search_after: Some(next_search_after_for_intent_target(
            scope.air_time,
            intent,
            now,
        )),
    })
}

fn absolute_intent_target(
    intent: &CreateAcquisitionIntent,
    scope: &AcquisitionIntentTarget,
    absolute: i32,
    now: DateTime<Utc>,
) -> Result<NewAcquisitionTarget> {
    if absolute <= 0 {
        bail!("absoluteEpisodeNumber must be greater than zero");
    }
    Ok(NewAcquisitionTarget {
        target_key: Some(format!("A{absolute:04}")),
        media_type: Some(intent.media_type),
        title: target_title(intent, scope, None, None, Some(absolute)),
        season_number: None,
        episode_number: None,
        absolute_episode_number: Some(absolute),
        air_date: scope.air_date.clone(),
        air_time: scope.air_time,
        metadata: Some(intent_target_metadata(intent, Some(scope))),
        state: Some(AcquisitionTargetState::Pending),
        next_search_after: Some(next_search_after_for_intent_target(
            scope.air_time,
            intent,
            now,
        )),
    })
}

fn target_from_key(
    intent: &CreateAcquisitionIntent,
    scope: &AcquisitionIntentTarget,
    key: &str,
    now: DateTime<Utc>,
) -> Result<NewAcquisitionTarget> {
    let key = normalized_target_key(key.to_string())?;
    if let Some((season, episode)) = parse_season_episode_key(&key) {
        return episode_intent_target(intent, scope, season, episode, Some(key), now);
    }
    if let Some(absolute) = parse_absolute_key(&key) {
        return absolute_intent_target(intent, scope, absolute, now);
    }
    Ok(NewAcquisitionTarget {
        target_key: Some(key),
        media_type: Some(intent.media_type),
        title: target_title(intent, scope, None, None, None),
        season_number: None,
        episode_number: None,
        absolute_episode_number: None,
        air_date: scope.air_date.clone(),
        air_time: scope.air_time,
        metadata: Some(intent_target_metadata(intent, Some(scope))),
        state: Some(AcquisitionTargetState::Pending),
        next_search_after: Some(next_search_after_for_intent_target(
            scope.air_time,
            intent,
            now,
        )),
    })
}

fn parse_season_episode_key(key: &str) -> Option<(i32, i32)> {
    let rest = key.strip_prefix('S')?;
    let (season, episode) = rest.split_once('E')?;
    Some((season.parse().ok()?, episode.parse().ok()?))
}

fn parse_absolute_key(key: &str) -> Option<i32> {
    key.strip_prefix('A')?.parse().ok()
}

fn external_ids_has_value(ids: &ExternalIds) -> bool {
    [
        ids.imdb.as_deref(),
        ids.tmdb.as_deref(),
        ids.tvdb.as_deref(),
        ids.tvdb_series.as_deref(),
        ids.tvdb_movie.as_deref(),
        ids.anilist.as_deref(),
        ids.anidb.as_deref(),
        ids.mal.as_deref(),
        ids.kitsu.as_deref(),
    ]
    .into_iter()
    .flatten()
    .any(|value| !value.trim().is_empty())
}

fn external_ids_overlap(left: &ExternalIds, right: &ExternalIds) -> bool {
    external_id_matches(&left.imdb, &right.imdb)
        || external_id_matches(&left.tmdb, &right.tmdb)
        || external_id_matches(&left.tvdb, &right.tvdb)
        || external_id_matches(&left.tvdb_series, &right.tvdb_series)
        || external_id_matches(&left.tvdb_movie, &right.tvdb_movie)
        || external_id_matches(&left.anilist, &right.anilist)
        || external_id_matches(&left.anidb, &right.anidb)
        || external_id_matches(&left.mal, &right.mal)
        || external_id_matches(&left.kitsu, &right.kitsu)
}

fn external_id_matches(left: &Option<String>, right: &Option<String>) -> bool {
    match (left.as_deref(), right.as_deref()) {
        (Some(left), Some(right)) => {
            let left = left.trim();
            let right = right.trim();
            !left.is_empty() && left.eq_ignore_ascii_case(right)
        }
        _ => false,
    }
}

fn target_title(
    intent: &CreateAcquisitionIntent,
    scope: &AcquisitionIntentTarget,
    season: Option<i32>,
    episode: Option<i32>,
    absolute: Option<i32>,
) -> Option<String> {
    scope
        .title
        .clone()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| match (season, episode, absolute) {
            (Some(season), Some(episode), _) => {
                Some(format!("{} S{season:02}E{episode:02}", intent.title.trim()))
            }
            (_, _, Some(absolute)) => Some(format!("{} A{absolute:04}", intent.title.trim())),
            _ => Some(intent.title.trim().to_string()),
        })
}

fn intent_target_metadata(
    intent: &CreateAcquisitionIntent,
    scope: Option<&AcquisitionIntentTarget>,
) -> JsonValue {
    json_object_without_nulls(serde_json::json!({
        "source": "acquisition_intent",
        "intentKind": scope.and_then(|value| value.kind.clone()),
        "externalIds": intent.external_ids.clone(),
        "scopeMetadata": scope.and_then(|value| value.metadata.clone()),
    }))
}

fn json_object_without_nulls(value: JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(map) => JsonValue::Object(
            map.into_iter()
                .filter_map(|(key, value)| (!value.is_null()).then_some((key, value)))
                .collect(),
        ),
        other => other,
    }
}

fn next_search_after_for_intent_target(
    air_time: Option<DateTime<Utc>>,
    intent: &CreateAcquisitionIntent,
    now: DateTime<Utc>,
) -> DateTime<Utc> {
    match air_time {
        Some(air_time)
            if air_time
                + chrono::Duration::seconds(intent.release_delay_seconds.unwrap_or_default())
                > now =>
        {
            air_time + chrono::Duration::seconds(intent.release_delay_seconds.unwrap_or_default())
        }
        _ => now,
    }
}

fn validate_target_input(
    subscription_media_type: MediaType,
    target: &NewAcquisitionTarget,
) -> Result<()> {
    let media_type = target.media_type.unwrap_or(subscription_media_type);
    if media_type == MediaType::Movie
        && (target.season_number.is_some()
            || target.episode_number.is_some()
            || target.absolute_episode_number.is_some())
    {
        bail!("movie acquisition targets cannot include episode numbers");
    }
    match (target.season_number, target.episode_number) {
        (Some(season), Some(episode)) => {
            if season <= 0 || episode <= 0 {
                bail!("seasonNumber and episodeNumber must be greater than zero");
            }
        }
        (Some(_), None) | (None, Some(_)) => {
            bail!("seasonNumber and episodeNumber must be provided together");
        }
        (None, None) => {}
    }
    if let Some(absolute) = target.absolute_episode_number
        && absolute <= 0
    {
        bail!("absoluteEpisodeNumber must be greater than zero");
    }
    Ok(())
}

fn generated_target_key(media_type: MediaType, target: &NewAcquisitionTarget) -> String {
    if let (Some(season), Some(episode)) = (target.season_number, target.episode_number) {
        return format!("S{season:02}E{episode:02}");
    }
    if let Some(absolute) = target.absolute_episode_number {
        return format!("A{absolute:04}");
    }
    if let Some(air_date) = target.air_date.as_deref()
        && !air_date.trim().is_empty()
    {
        return format!("date:{}", air_date.trim());
    }
    match media_type {
        MediaType::Movie => "movie".to_string(),
        MediaType::Series | MediaType::Anime => "series".to_string(),
    }
}

fn normalized_target_key(value: String) -> Result<String> {
    let normalized = value.trim().to_ascii_uppercase();
    if normalized.is_empty() {
        bail!("targetKey cannot be empty");
    }
    Ok(normalized)
}

pub fn normalize_acquisition_title(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .replace([' ', '-', '_', ':'], "")
}

fn map_subscription(row: &AnyRow) -> Result<AcquisitionSubscription> {
    let subscription_id_raw: String = row.try_get("subscription_id")?;
    let media_type_raw: String = row.try_get("media_type")?;
    let monitor_policy_raw: String = row.try_get("monitor_policy")?;
    let route_policy_raw: String = row.try_get("route_policy")?;
    let source_provider_id_raw = row_get_opt_string(row, "source_provider_id")?;
    let created_at_raw: String = row.try_get("created_at")?;
    let updated_at_raw: String = row.try_get("updated_at")?;
    let external_ids = parse_json_opt(
        row_get_opt_string(row, "external_ids_json")?,
        "acquisition_subscriptions.external_ids_json",
    )?
    .map(serde_json::from_value::<ExternalIds>)
    .transpose()
    .context("parsing acquisition subscription external ids")?;
    let quality_profile = parse_json_opt(
        row_get_opt_string(row, "quality_profile_json")?,
        "acquisition_subscriptions.quality_profile_json",
    )?;
    let status_raw: String = row.try_get("status")?;
    let year = row_get_i64_opt(row, "year")?;

    Ok(AcquisitionSubscription {
        subscription_id: parse_uuid(
            &subscription_id_raw,
            "acquisition_subscriptions.subscription_id",
        )?,
        media_type: parse_media_type(&media_type_raw, "acquisition_subscriptions.media_type")?,
        title: row.try_get("title")?,
        normalized_title: row.try_get("normalized_title")?,
        year: year.map(|value| value as i32),
        external_ids,
        monitor_policy: AcquisitionMonitorPolicy::from_str(&monitor_policy_raw)?,
        route_policy: AcquisitionRoutePolicy::from_str(&route_policy_raw)?,
        source_provider_id: source_provider_id_raw
            .as_deref()
            .map(|value| parse_uuid(value, "acquisition_subscriptions.source_provider_id"))
            .transpose()?,
        release_delay_seconds: row.try_get::<i64, _>("release_delay_seconds")?,
        quality_profile,
        metadata_refresh_after: parse_datetime(
            &row.try_get::<String, _>("metadata_refresh_after")?,
            "acquisition_subscriptions.metadata_refresh_after",
        )?,
        candidate_search_after: parse_datetime(
            &row.try_get::<String, _>("candidate_search_after")?,
            "acquisition_subscriptions.candidate_search_after",
        )?,
        last_metadata_refresh_at: parse_datetime_opt(
            row_get_opt_string(row, "last_metadata_refresh_at")?,
            "acquisition_subscriptions.last_metadata_refresh_at",
        )?,
        last_candidate_search_at: parse_datetime_opt(
            row_get_opt_string(row, "last_candidate_search_at")?,
            "acquisition_subscriptions.last_candidate_search_at",
        )?,
        tracking_started_at: parse_datetime_opt(
            row_get_opt_string(row, "tracking_started_at")?,
            "acquisition_subscriptions.tracking_started_at",
        )?,
        status: AcquisitionSubscriptionStatus::from_str(&status_raw)?,
        active: row_get_bool(row, "active")?,
        created_at: parse_datetime(&created_at_raw, "acquisition_subscriptions.created_at")?,
        updated_at: parse_datetime(&updated_at_raw, "acquisition_subscriptions.updated_at")?,
    })
}

fn map_target(row: &AnyRow) -> Result<AcquisitionTarget> {
    let target_id_raw: String = row.try_get("target_id")?;
    let subscription_id_raw: String = row.try_get("subscription_id")?;
    let media_type_raw: String = row.try_get("media_type")?;
    let state_raw: String = row.try_get("state")?;
    let selected_provider_id_raw = row_get_opt_string(row, "selected_provider_id")?;
    let import_event_id_raw = row_get_opt_string(row, "import_event_id")?;
    let created_at_raw: String = row.try_get("created_at")?;
    let updated_at_raw: String = row.try_get("updated_at")?;

    let metadata = parse_json_opt(
        row_get_opt_string(row, "metadata_json")?,
        "acquisition_targets.metadata_json",
    )?;
    let selected_candidate = parse_json_opt(
        row_get_opt_string(row, "selected_candidate_json")?,
        "acquisition_targets.selected_candidate_json",
    )?;

    Ok(AcquisitionTarget {
        target_id: parse_uuid(&target_id_raw, "acquisition_targets.target_id")?,
        subscription_id: parse_uuid(&subscription_id_raw, "acquisition_targets.subscription_id")?,
        target_key: row.try_get("target_key")?,
        media_type: parse_media_type(&media_type_raw, "acquisition_targets.media_type")?,
        title: row.try_get("title")?,
        season_number: row_get_i64_opt(row, "season_number")?.map(|value| value as i32),
        episode_number: row_get_i64_opt(row, "episode_number")?.map(|value| value as i32),
        absolute_episode_number: row_get_i64_opt(row, "absolute_episode_number")?
            .map(|value| value as i32),
        air_date: row_get_opt_string(row, "air_date")?,
        air_time: parse_datetime_opt(
            row_get_opt_string(row, "air_time")?,
            "acquisition_targets.air_time",
        )?,
        metadata,
        state: AcquisitionTargetState::from_str(&state_raw)?,
        state_reason: row_get_opt_string(row, "state_reason")?,
        selected_provider_id: selected_provider_id_raw
            .as_deref()
            .map(|value| parse_uuid(value, "acquisition_targets.selected_provider_id"))
            .transpose()?,
        selected_route_logical_id: row_get_opt_string(row, "selected_route_logical_id")?,
        selected_candidate,
        download_id: row_get_opt_string(row, "download_id")?,
        import_event_id: import_event_id_raw
            .as_deref()
            .map(|value| parse_uuid(value, "acquisition_targets.import_event_id"))
            .transpose()?,
        search_attempts: row.try_get::<i64, _>("search_attempts")?,
        last_search_at: parse_datetime_opt(
            row_get_opt_string(row, "last_search_at")?,
            "acquisition_targets.last_search_at",
        )?,
        next_search_after: parse_datetime_opt(
            row_get_opt_string(row, "next_search_after")?,
            "acquisition_targets.next_search_after",
        )?,
        created_at: parse_datetime(&created_at_raw, "acquisition_targets.created_at")?,
        updated_at: parse_datetime(&updated_at_raw, "acquisition_targets.updated_at")?,
    })
}

fn parse_media_type(raw: &str, field: &str) -> Result<MediaType> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "movie" => Ok(MediaType::Movie),
        "series" => Ok(MediaType::Series),
        "anime" => Ok(MediaType::Anime),
        _ => bail!("invalid enum value '{}' for field {}", raw, field),
    }
}

fn external_ids_json(value: Option<&ExternalIds>) -> Result<Option<String>> {
    match value {
        Some(value) => Ok(Some(
            serde_json::to_string(value).context("serializing acquisition external ids")?,
        )),
        None => Ok(None),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::DatabaseConfig,
        db::{Database, models::MediaType},
    };
    use chrono::Duration as ChronoDuration;
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

    fn series_subscription(title: &str) -> NewAcquisitionSubscription {
        NewAcquisitionSubscription {
            media_type: MediaType::Series,
            title: title.to_string(),
            year: Some(2026),
            external_ids: None,
            monitor_policy: AcquisitionMonitorPolicy::AllMissing,
            route_policy: AcquisitionRoutePolicy::DebridFirst,
            source_provider_id: None,
            release_delay_seconds: Some(30 * 60),
            quality_profile: None,
            metadata_refresh_after: None,
            candidate_search_after: None,
        }
    }

    #[tokio::test]
    async fn upserting_targets_dedupes_by_episode_key_and_preserves_state() -> Result<()> {
        let database = setup_db().await?;
        let subscription = create_subscription(
            &database.pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Anime,
                ..series_subscription("Example Anime")
            },
        )
        .await?;

        let targets = upsert_subscription_targets(
            &database.pool,
            subscription.subscription_id,
            vec![NewAcquisitionTarget {
                season_number: Some(4),
                episode_number: Some(1),
                title: Some("Premiere".to_string()),
                metadata: Some(json!({ "anilistSeasonId": 123 })),
                ..empty_target()
            }],
        )
        .await?;
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].target_key, "S04E01");

        let updated = update_target_state(
            &database.pool,
            targets[0].target_id,
            AcquisitionTargetStateUpdate {
                state: AcquisitionTargetState::Submitted,
                selected_route_logical_id: Some("acquisition.debrid.default".to_string()),
                download_id: Some("rd-job".to_string()),
                ..Default::default()
            },
        )
        .await?
        .expect("updated target");
        assert_eq!(updated.state, AcquisitionTargetState::Submitted);

        let refreshed = upsert_subscription_targets(
            &database.pool,
            subscription.subscription_id,
            vec![NewAcquisitionTarget {
                season_number: Some(4),
                episode_number: Some(1),
                title: Some("The Premiere".to_string()),
                metadata: Some(json!({ "anilistSeasonId": 456 })),
                ..empty_target()
            }],
        )
        .await?;

        assert_eq!(refreshed.len(), 1);
        assert_eq!(refreshed[0].target_key, "S04E01");
        assert_eq!(refreshed[0].state, AcquisitionTargetState::Submitted);
        assert_eq!(refreshed[0].download_id.as_deref(), Some("rd-job"));
        assert_eq!(refreshed[0].title, "The Premiere");
        assert_eq!(
            refreshed[0]
                .metadata
                .as_ref()
                .and_then(|value| value.get("anilistSeasonId")),
            Some(&json!(456))
        );
        Ok(())
    }

    #[tokio::test]
    async fn due_candidate_targets_skip_future_air_times() -> Result<()> {
        let database = setup_db().await?;
        let now = Utc::now();
        let subscription =
            create_subscription(&database.pool, series_subscription("Example Show")).await?;

        upsert_subscription_targets(
            &database.pool,
            subscription.subscription_id,
            vec![
                NewAcquisitionTarget {
                    season_number: Some(1),
                    episode_number: Some(1),
                    air_time: Some(now - ChronoDuration::minutes(30)),
                    next_search_after: Some(now - ChronoDuration::minutes(5)),
                    ..empty_target()
                },
                NewAcquisitionTarget {
                    season_number: Some(1),
                    episode_number: Some(2),
                    air_time: Some(now + ChronoDuration::hours(1)),
                    next_search_after: Some(now - ChronoDuration::minutes(5)),
                    ..empty_target()
                },
            ],
        )
        .await?;

        let due = list_due_candidate_targets(&database.pool, now, 10).await?;
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].target_key, "S01E01");
        Ok(())
    }

    #[tokio::test]
    async fn metadata_due_list_respects_active_status() -> Result<()> {
        let database = setup_db().await?;
        let now = Utc::now();
        let due = create_subscription(
            &database.pool,
            NewAcquisitionSubscription {
                metadata_refresh_after: Some(now - ChronoDuration::minutes(1)),
                ..series_subscription("Due Show")
            },
        )
        .await?;
        let paused = create_subscription(
            &database.pool,
            NewAcquisitionSubscription {
                metadata_refresh_after: Some(now - ChronoDuration::minutes(1)),
                ..series_subscription("Paused Show")
            },
        )
        .await?;
        update_subscription(
            &database.pool,
            paused.subscription_id,
            AcquisitionSubscriptionUpdate {
                status: Some(AcquisitionSubscriptionStatus::Paused),
                active: Some(false),
                ..Default::default()
            },
        )
        .await?;

        let items = list_due_metadata_subscriptions(&database.pool, now, 10).await?;
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].subscription_id, due.subscription_id);
        Ok(())
    }

    #[tokio::test]
    async fn metadata_tracking_starts_only_after_initial_due_downloads_import() -> Result<()> {
        let database = setup_db().await?;
        let now = Utc::now();
        let subscription = create_subscription(
            &database.pool,
            NewAcquisitionSubscription {
                metadata_refresh_after: Some(now - ChronoDuration::minutes(1)),
                ..series_subscription("Bootstrap Show")
            },
        )
        .await?;

        let due = list_due_metadata_subscriptions(&database.pool, now, 10).await?;
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].subscription_id, subscription.subscription_id);

        record_metadata_refresh(
            &database.pool,
            subscription.subscription_id,
            now - ChronoDuration::minutes(1),
        )
        .await?;
        let targets = upsert_subscription_targets(
            &database.pool,
            subscription.subscription_id,
            vec![NewAcquisitionTarget {
                season_number: Some(1),
                episode_number: Some(1),
                air_time: Some(now - ChronoDuration::hours(1)),
                next_search_after: Some(now - ChronoDuration::minutes(1)),
                ..empty_target()
            }],
        )
        .await?;

        let suspended = list_due_metadata_subscriptions(&database.pool, now, 10).await?;
        assert!(
            suspended.is_empty(),
            "metadata refresh should suspend after bootstrap target expansion until import"
        );

        let started = start_subscription_tracking_if_initial_download_complete(
            &database.pool,
            subscription.subscription_id,
            now,
        )
        .await?;
        assert!(
            !started,
            "tracking cannot begin while a due initial target is still missing"
        );

        update_target_state(
            &database.pool,
            targets[0].target_id,
            AcquisitionTargetStateUpdate {
                state: AcquisitionTargetState::Imported,
                state_reason: Some("Imported during test.".to_string()),
                ..Default::default()
            },
        )
        .await?;
        let started = start_subscription_tracking_if_initial_download_complete(
            &database.pool,
            subscription.subscription_id,
            now,
        )
        .await?;
        assert!(started);

        let subscription = get_subscription(&database.pool, subscription.subscription_id)
            .await?
            .expect("subscription");
        assert!(subscription.tracking_started_at.is_some());
        let due =
            list_due_metadata_subscriptions(&database.pool, now + ChronoDuration::seconds(1), 10)
                .await?;
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].subscription_id, subscription.subscription_id);
        Ok(())
    }

    #[tokio::test]
    async fn acquisition_intent_creates_movie_subscription_and_target() -> Result<()> {
        let database = setup_db().await?;
        let now = Utc::now();
        let result = create_or_update_acquisition_intent(
            &database.pool,
            CreateAcquisitionIntent {
                media_type: MediaType::Movie,
                title: "Example Movie".to_string(),
                year: Some(2026),
                external_ids: Some(ExternalIds {
                    imdb: Some("tt1234567".to_string()),
                    ..Default::default()
                }),
                monitor_policy: None,
                route_policy: None,
                source_provider_id: None,
                release_delay_seconds: None,
                quality_profile: None,
                metadata_refresh_after: None,
                candidate_search_after: None,
                target: None,
                targets: Vec::new(),
            },
            now,
        )
        .await?;

        assert!(result.created);
        assert_eq!(result.expanded_target_count, 1);
        assert_eq!(result.detail.subscription.media_type, MediaType::Movie);
        assert_eq!(
            result.detail.subscription.route_policy,
            AcquisitionRoutePolicy::DebridFirst
        );
        assert_eq!(result.detail.targets.len(), 1);
        assert_eq!(result.detail.targets[0].target_key, "MOVIE");
        assert_eq!(
            result.detail.targets[0].state,
            AcquisitionTargetState::Pending
        );
        assert_eq!(
            result.detail.targets[0]
                .next_search_after
                .map(|value| value.timestamp()),
            Some(now.timestamp())
        );
        Ok(())
    }

    #[tokio::test]
    async fn acquisition_intent_expands_episode_season_anime_and_backlog_targets() -> Result<()> {
        let database = setup_db().await?;
        let now = Utc::now();

        let episode = create_or_update_acquisition_intent(
            &database.pool,
            CreateAcquisitionIntent {
                media_type: MediaType::Series,
                title: "Example Show".to_string(),
                year: Some(2026),
                external_ids: Some(ExternalIds {
                    tvdb_series: Some("321".to_string()),
                    ..Default::default()
                }),
                monitor_policy: None,
                route_policy: Some(AcquisitionRoutePolicy::DebridOnly),
                source_provider_id: None,
                release_delay_seconds: Some(900),
                quality_profile: None,
                metadata_refresh_after: None,
                candidate_search_after: None,
                target: Some(AcquisitionIntentTarget {
                    kind: Some("episode".to_string()),
                    season_number: Some(4),
                    episode_number: Some(1),
                    ..Default::default()
                }),
                targets: Vec::new(),
            },
            now,
        )
        .await?;
        assert_eq!(
            episode.detail.subscription.monitor_policy,
            AcquisitionMonitorPolicy::SelectedTargets
        );
        assert_eq!(
            episode.detail.subscription.route_policy,
            AcquisitionRoutePolicy::DebridOnly
        );
        assert_eq!(episode.detail.targets.len(), 1);
        assert_eq!(episode.detail.targets[0].target_key, "S04E01");

        let season = create_or_update_acquisition_intent(
            &database.pool,
            CreateAcquisitionIntent {
                media_type: MediaType::Series,
                title: "Example Season".to_string(),
                year: Some(2026),
                external_ids: None,
                monitor_policy: None,
                route_policy: None,
                source_provider_id: None,
                release_delay_seconds: None,
                quality_profile: None,
                metadata_refresh_after: None,
                candidate_search_after: None,
                target: Some(AcquisitionIntentTarget {
                    kind: Some("season".to_string()),
                    season_number: Some(2),
                    episode_start: Some(1),
                    episode_end: Some(3),
                    ..Default::default()
                }),
                targets: Vec::new(),
            },
            now,
        )
        .await?;
        assert_eq!(
            season
                .detail
                .targets
                .iter()
                .map(|target| target.target_key.as_str())
                .collect::<Vec<_>>(),
            vec!["S02E01", "S02E02", "S02E03"]
        );

        let anime = create_or_update_acquisition_intent(
            &database.pool,
            CreateAcquisitionIntent {
                media_type: MediaType::Anime,
                title: "Example Anime".to_string(),
                year: Some(2026),
                external_ids: Some(ExternalIds {
                    anilist: Some("42".to_string()),
                    ..Default::default()
                }),
                monitor_policy: None,
                route_policy: None,
                source_provider_id: None,
                release_delay_seconds: None,
                quality_profile: None,
                metadata_refresh_after: None,
                candidate_search_after: None,
                target: Some(AcquisitionIntentTarget {
                    kind: Some("absolute_episode".to_string()),
                    absolute_episode_number: Some(1000),
                    ..Default::default()
                }),
                targets: Vec::new(),
            },
            now,
        )
        .await?;
        assert_eq!(anime.detail.targets[0].target_key, "A1000");
        assert_eq!(anime.detail.targets[0].absolute_episode_number, Some(1000));

        let backlog = create_or_update_acquisition_intent(
            &database.pool,
            CreateAcquisitionIntent {
                media_type: MediaType::Anime,
                title: "Backlog Anime".to_string(),
                year: Some(2026),
                external_ids: None,
                monitor_policy: None,
                route_policy: None,
                source_provider_id: None,
                release_delay_seconds: None,
                quality_profile: None,
                metadata_refresh_after: None,
                candidate_search_after: None,
                target: Some(AcquisitionIntentTarget {
                    kind: Some("backlog".to_string()),
                    target_keys: vec!["S01E01".to_string(), "S01E02".to_string()],
                    ..Default::default()
                }),
                targets: Vec::new(),
            },
            now,
        )
        .await?;
        assert_eq!(
            backlog
                .detail
                .targets
                .iter()
                .map(|target| (
                    target.target_key.as_str(),
                    target.season_number,
                    target.episode_number
                ))
                .collect::<Vec<_>>(),
            vec![("S01E01", Some(1), Some(1)), ("S01E02", Some(1), Some(2))]
        );
        Ok(())
    }

    #[tokio::test]
    async fn acquisition_intent_readd_is_idempotent_and_preserves_target_state() -> Result<()> {
        let database = setup_db().await?;
        let now = Utc::now();
        let intent = CreateAcquisitionIntent {
            media_type: MediaType::Series,
            title: "Idempotent Show".to_string(),
            year: Some(2026),
            external_ids: None,
            monitor_policy: None,
            route_policy: None,
            source_provider_id: None,
            release_delay_seconds: None,
            quality_profile: None,
            metadata_refresh_after: None,
            candidate_search_after: None,
            target: Some(AcquisitionIntentTarget {
                kind: Some("season".to_string()),
                season_number: Some(1),
                episode_start: Some(1),
                episode_end: Some(2),
                ..Default::default()
            }),
            targets: Vec::new(),
        };
        let first =
            create_or_update_acquisition_intent(&database.pool, intent.clone(), now).await?;
        let submitted = update_target_state(
            &database.pool,
            first.detail.targets[0].target_id,
            AcquisitionTargetStateUpdate {
                state: AcquisitionTargetState::Submitted,
                selected_route_logical_id: Some("acquisition.debrid.default".to_string()),
                download_id: Some("rd-job".to_string()),
                ..Default::default()
            },
        )
        .await?
        .expect("updated target");
        assert_eq!(submitted.state, AcquisitionTargetState::Submitted);

        let second = create_or_update_acquisition_intent(
            &database.pool,
            intent,
            now + ChronoDuration::minutes(5),
        )
        .await?;

        assert!(!second.created);
        assert_eq!(
            first.detail.subscription.subscription_id,
            second.detail.subscription.subscription_id
        );
        assert_eq!(second.detail.targets.len(), 2);
        assert_eq!(
            second.detail.targets[0].state,
            AcquisitionTargetState::Submitted
        );
        assert_eq!(
            second.detail.targets[0].download_id.as_deref(),
            Some("rd-job")
        );
        Ok(())
    }

    #[tokio::test]
    async fn acquisition_intent_does_not_merge_distinct_external_ids_for_same_title_year()
    -> Result<()> {
        let database = setup_db().await?;
        let now = Utc::now();
        let first = create_or_update_acquisition_intent(
            &database.pool,
            CreateAcquisitionIntent {
                media_type: MediaType::Series,
                title: "Shared Name".to_string(),
                year: Some(2026),
                external_ids: Some(ExternalIds {
                    tvdb_series: Some("111".to_string()),
                    ..Default::default()
                }),
                monitor_policy: None,
                route_policy: None,
                source_provider_id: None,
                release_delay_seconds: None,
                quality_profile: None,
                metadata_refresh_after: None,
                candidate_search_after: None,
                target: Some(AcquisitionIntentTarget {
                    kind: Some("episode".to_string()),
                    season_number: Some(1),
                    episode_number: Some(1),
                    ..Default::default()
                }),
                targets: Vec::new(),
            },
            now,
        )
        .await?;
        let second_intent = CreateAcquisitionIntent {
            media_type: MediaType::Series,
            title: "Shared Name".to_string(),
            year: Some(2026),
            external_ids: Some(ExternalIds {
                tvdb_series: Some("222".to_string()),
                ..Default::default()
            }),
            monitor_policy: None,
            route_policy: None,
            source_provider_id: None,
            release_delay_seconds: None,
            quality_profile: None,
            metadata_refresh_after: None,
            candidate_search_after: None,
            target: Some(AcquisitionIntentTarget {
                kind: Some("episode".to_string()),
                season_number: Some(1),
                episode_number: Some(1),
                ..Default::default()
            }),
            targets: Vec::new(),
        };
        let second =
            create_or_update_acquisition_intent(&database.pool, second_intent.clone(), now).await?;
        let second_readd = create_or_update_acquisition_intent(
            &database.pool,
            second_intent,
            now + ChronoDuration::minutes(1),
        )
        .await?;

        assert!(first.created);
        assert!(second.created);
        assert!(!second_readd.created);
        assert_ne!(
            first.detail.subscription.subscription_id,
            second.detail.subscription.subscription_id
        );
        assert_eq!(
            second.detail.subscription.subscription_id,
            second_readd.detail.subscription.subscription_id
        );
        let subscriptions = list_subscriptions(
            &database.pool,
            AcquisitionSubscriptionFilter { active: Some(true) },
        )
        .await?;
        assert_eq!(subscriptions.len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn acquisition_intent_does_not_mutate_arr_managed_intents() -> Result<()> {
        let database = setup_db().await?;
        let manager_provider_id = Uuid::new_v4();
        sqlx::query::<sqlx::Any>(
            "INSERT INTO managed_ingest_intents (
                intent_id,
                media_type,
                title,
                normalized_title,
                year,
                manager_provider_id,
                manager_item_id,
                manager_label,
                source
            ) VALUES (?, 'series', 'Arr Show', 'arrshow', 2026, ?, 'sonarr-1', 'Sonarr', 'find_media_add')",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(manager_provider_id.to_string())
        .execute(&database.pool)
        .await?;

        create_or_update_acquisition_intent(
            &database.pool,
            CreateAcquisitionIntent {
                media_type: MediaType::Series,
                title: "Arr Show".to_string(),
                year: Some(2026),
                external_ids: None,
                monitor_policy: None,
                route_policy: None,
                source_provider_id: None,
                release_delay_seconds: None,
                quality_profile: None,
                metadata_refresh_after: None,
                candidate_search_after: None,
                target: Some(AcquisitionIntentTarget {
                    kind: Some("episode".to_string()),
                    season_number: Some(1),
                    episode_number: Some(1),
                    ..Default::default()
                }),
                targets: Vec::new(),
            },
            Utc::now(),
        )
        .await?;

        let rows: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM managed_ingest_intents
             WHERE manager_provider_id = ? AND manager_item_id = 'sonarr-1' AND active = 1",
        )
        .bind(manager_provider_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(rows, 1);
        let subscriptions = list_subscriptions(
            &database.pool,
            AcquisitionSubscriptionFilter { active: Some(true) },
        )
        .await?;
        assert_eq!(subscriptions.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn stop_subscription_tracking_hides_request_and_cancels_active_work() -> Result<()> {
        let database = setup_db().await?;
        let subscription =
            create_subscription(&database.pool, series_subscription("Stale Show")).await?;
        let targets = upsert_subscription_targets(
            &database.pool,
            subscription.subscription_id,
            vec![NewAcquisitionTarget {
                season_number: Some(1),
                episode_number: Some(1),
                title: Some("Pilot".to_string()),
                ..empty_target()
            }],
        )
        .await?;
        let target = update_target_state(
            &database.pool,
            targets[0].target_id,
            AcquisitionTargetStateUpdate {
                state: AcquisitionTargetState::Submitted,
                selected_route_logical_id: Some("acquisition.debrid.default".to_string()),
                download_id: Some("debrid-job".to_string()),
                ..Default::default()
            },
        )
        .await?
        .expect("submitted target");
        let release_id = Uuid::new_v4();
        let release_job_id = Uuid::new_v4();
        let coverage_id = Uuid::new_v4();
        sqlx::query::<sqlx::Any>(
            "INSERT INTO acquisition_releases (
                release_id,
                subscription_id,
                source_extension_id,
                owner_id,
                media_type,
                title,
                release_title,
                source,
                source_kind,
                fingerprint,
                release_kind,
                resolver_kind,
                resolver_version,
                confidence,
                selected_route_logical_id,
                download_id,
                state
            ) VALUES (?, ?, 'test.source', 'default', 'series', 'Stale Show',
                'Stale.Show.S01E01', 'magnet:?xt=urn:btih:test', 'magnet',
                'fingerprint-stale-show', 'single', 'tv_sonarr_style', 'test',
                'high', 'acquisition.debrid.default', 'debrid-job', 'submitted')",
        )
        .bind(release_id.to_string())
        .bind(subscription.subscription_id.to_string())
        .execute(&database.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO acquisition_release_jobs (
                release_job_id,
                release_id,
                route_logical_id,
                download_id,
                state,
                active
            ) VALUES (?, ?, 'acquisition.debrid.default', 'debrid-job', 'submitted', 1)",
        )
        .bind(release_job_id.to_string())
        .bind(release_id.to_string())
        .execute(&database.pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO acquisition_release_coverage (
                coverage_id,
                release_id,
                target_id,
                coverage_kind,
                confidence,
                state
            ) VALUES (?, ?, ?, 'single_episode', 'high', 'submitted')",
        )
        .bind(coverage_id.to_string())
        .bind(release_id.to_string())
        .bind(target.target_id.to_string())
        .execute(&database.pool)
        .await?;

        let result = stop_subscription_tracking(
            &database.pool,
            subscription.subscription_id,
            "User removed acquisition request.",
        )
        .await?
        .expect("stopped subscription");

        assert!(!result.subscription.active);
        assert_eq!(
            result.subscription.status,
            AcquisitionSubscriptionStatus::Cancelled
        );
        assert_eq!(result.targets_excluded, 1);
        assert_eq!(result.releases_cancelled, 1);
        assert_eq!(result.release_jobs_cancelled, 1);
        assert_eq!(result.coverage_rejected, 1);

        let active = list_subscriptions(
            &database.pool,
            AcquisitionSubscriptionFilter { active: Some(true) },
        )
        .await?;
        assert!(active.is_empty());

        let target = get_target(&database.pool, target.target_id)
            .await?
            .expect("target");
        assert_eq!(target.state, AcquisitionTargetState::Excluded);
        let release_state: String =
            sqlx::query_scalar("SELECT state FROM acquisition_releases WHERE release_id = ?")
                .bind(release_id.to_string())
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(release_state, "cancelled");
        let job_state: String = sqlx::query_scalar(
            "SELECT state FROM acquisition_release_jobs WHERE release_job_id = ?",
        )
        .bind(release_job_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(job_state, "cancelled");
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
