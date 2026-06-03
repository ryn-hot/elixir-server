use anyhow::{Context, Result};
use serde_json::Value as JsonValue;
use sqlx::AnyPool;
use uuid::Uuid;

pub const EVENT_REVIEW_CANDIDATE_CREATED: &str = "review_candidate_created";
pub const EVENT_INSPECT_REQUESTED: &str = "inspect_requested";
pub const EVENT_MANUAL_APPROVAL: &str = "manual_approval";
pub const EVENT_MANUAL_REJECTION: &str = "manual_rejection";
pub const EVENT_MANUAL_IMPORT_QUARANTINED: &str = "manual_import_quarantined";
pub const EVENT_MANUAL_IMPORT_COMPLETED: &str = "manual_import_completed";
pub const EVENT_ACQUISITION_SEARCH_SCHEDULED: &str = "acquisition_search_scheduled";
pub const EVENT_ACQUISITION_REQUEST_COMPLETED: &str = "acquisition_request_completed";
pub const EVENT_ROUTE_FALLBACK: &str = "route_fallback";

#[derive(Debug, Clone, Default)]
pub struct NewAcquisitionAuditEvent {
    pub audit_event_id: Option<Uuid>,
    pub event_type: String,
    pub release_id: Option<Uuid>,
    pub subscription_id: Option<Uuid>,
    pub target_id: Option<Uuid>,
    pub release_job_id: Option<Uuid>,
    pub import_run_id: Option<Uuid>,
    pub import_link_id: Option<Uuid>,
    pub actor_user_id: Option<Uuid>,
    pub state: Option<String>,
    pub reason: Option<String>,
    pub evidence: Option<JsonValue>,
}

pub async fn record_acquisition_audit_event(
    pool: &AnyPool,
    data: NewAcquisitionAuditEvent,
) -> Result<Uuid> {
    let audit_event_id = data.audit_event_id.unwrap_or_else(Uuid::new_v4);
    let evidence_json = data
        .evidence
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .context("serializing acquisition audit evidence")?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO acquisition_audit_events (
            audit_event_id,
            event_type,
            release_id,
            subscription_id,
            target_id,
            release_job_id,
            import_run_id,
            import_link_id,
            actor_user_id,
            state,
            reason,
            evidence_json
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(audit_event_id.to_string())
    .bind(data.event_type.trim())
    .bind(data.release_id.map(|value| value.to_string()))
    .bind(data.subscription_id.map(|value| value.to_string()))
    .bind(data.target_id.map(|value| value.to_string()))
    .bind(data.release_job_id.map(|value| value.to_string()))
    .bind(data.import_run_id.map(|value| value.to_string()))
    .bind(data.import_link_id.map(|value| value.to_string()))
    .bind(data.actor_user_id.map(|value| value.to_string()))
    .bind(data.state.as_deref())
    .bind(data.reason.as_deref())
    .bind(evidence_json.as_deref())
    .execute(pool)
    .await
    .context("recording acquisition audit event")?;
    Ok(audit_event_id)
}

#[cfg(test)]
pub async fn count_acquisition_audit_events(
    pool: &AnyPool,
    release_id: Uuid,
    event_type: &str,
) -> Result<i64> {
    sqlx::query_scalar::<sqlx::Any, i64>(
        "SELECT COUNT(*)
         FROM acquisition_audit_events
         WHERE release_id = ?
           AND event_type = ?",
    )
    .bind(release_id.to_string())
    .bind(event_type)
    .fetch_one(pool)
    .await
    .context("counting acquisition audit events")
}

#[cfg(test)]
pub async fn count_acquisition_audit_events_for_subscription(
    pool: &AnyPool,
    subscription_id: Uuid,
    event_type: &str,
) -> Result<i64> {
    sqlx::query_scalar::<sqlx::Any, i64>(
        "SELECT COUNT(*)
         FROM acquisition_audit_events
         WHERE subscription_id = ?
           AND event_type = ?",
    )
    .bind(subscription_id.to_string())
    .bind(event_type)
    .fetch_one(pool)
    .await
    .context("counting acquisition subscription audit events")
}
