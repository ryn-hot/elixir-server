use std::sync::Arc;

use chrono::{DateTime, NaiveDateTime, Utc};
use serde::Serialize;
use sqlx::{AnyPool, Row};
use thiserror::Error;
use uuid::Uuid;

use crate::live::{
    admin::{ActorSnapshot, AdminAction, AuditReference, LiveAuditChain, LiveAuditError},
    session::{
        DeliveryMode, LiveSessionRepository, SessionOwner, SessionProtocol, SessionRepositoryError,
        SessionState, TerminalReason,
    },
};

const MAX_ADMIN_SESSIONS: i64 = 200;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminSessionSummary {
    pub session_id: Uuid,
    pub profile_id: Uuid,
    pub provider_id: Uuid,
    pub delivery_mode: DeliveryMode,
    pub protocol: SessionProtocol,
    pub state: SessionState,
    pub revision: i64,
    pub source_index: i32,
    pub failover_count: i32,
    pub refresh_count: i32,
    pub created_at: DateTime<Utc>,
    pub last_heartbeat_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub error_code: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionTerminateMutation {
    pub status: &'static str,
    pub revision: i64,
    pub session_id: Uuid,
    pub state: SessionState,
    pub audit: AuditReference,
}

#[derive(Clone)]
pub struct LiveSessionAdminRepository {
    pool: AnyPool,
    sessions: Arc<LiveSessionRepository>,
}

impl LiveSessionAdminRepository {
    pub fn new(pool: AnyPool, sessions: Arc<LiveSessionRepository>) -> Self {
        Self { pool, sessions }
    }

    pub async fn list(
        &self,
        home_id: Uuid,
    ) -> Result<Vec<AdminSessionSummary>, LiveSessionAdminError> {
        let rows = sqlx::query(
            "SELECT id, profile_id, provider_id, delivery_mode, protocol, state, revision,
                    source_index, failover_count, refresh_count,
                    CAST(created_at AS TEXT) AS created_at,
                    CAST(last_heartbeat_at AS TEXT) AS last_heartbeat_at,
                    CAST(expires_at AS TEXT) AS expires_at,
                    CAST(ended_at AS TEXT) AS ended_at, error_code
             FROM live_playback_sessions
             WHERE home_id = $1
               AND (state NOT IN ('ended', 'expired', 'failed') OR state = 'failed')
             ORDER BY CASE WHEN state NOT IN ('ended', 'expired', 'failed') THEN 0 ELSE 1 END,
                      created_at DESC, id
             LIMIT $2",
        )
        .bind(home_id.to_string())
        .bind(MAX_ADMIN_SESSIONS)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(decode_summary).collect()
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn terminate(
        &self,
        home_id: Uuid,
        session_id: Uuid,
        expected_revision: i64,
        control_fencing_token: i64,
        actor: &ActorSnapshot,
        audit: &LiveAuditChain,
        now: DateTime<Utc>,
    ) -> Result<SessionTerminateMutation, LiveSessionAdminError> {
        if expected_revision < 1 || control_fencing_token < 1 {
            return Err(LiveSessionAdminError::InvalidInput);
        }
        let mut transaction = self.pool.begin().await?;
        let membership_role: Option<String> = sqlx::query_scalar(
            "SELECT role FROM home_members
             WHERE home_id = $1 AND user_id = $2 AND status = 'active'
             LIMIT 1",
        )
        .bind(home_id.to_string())
        .bind(actor.actor_user_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?;
        if membership_role.as_deref() != Some(actor.home_role.as_str()) {
            return Err(LiveSessionAdminError::Forbidden);
        }
        let row = sqlx::query(
            "SELECT user_id, home_id, profile_id, account_session_id, provider_id,
                    delivery_mode, protocol, state, revision
             FROM live_playback_sessions
             WHERE id = $1 AND home_id = $2 LIMIT 1",
        )
        .bind(session_id.to_string())
        .bind(home_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(LiveSessionAdminError::NotFound)?;
        let owner = SessionOwner {
            user_id: parse_uuid(&row.try_get::<String, _>("user_id")?)?,
            home_id: parse_uuid(&row.try_get::<String, _>("home_id")?)?,
            profile_id: parse_uuid(&row.try_get::<String, _>("profile_id")?)?,
            account_session_id: parse_uuid(&row.try_get::<String, _>("account_session_id")?)?,
            provider_id: parse_uuid(&row.try_get::<String, _>("provider_id")?)?,
        };
        let current_revision = positive(row.try_get("revision")?)?;
        if current_revision != expected_revision {
            return Err(LiveSessionAdminError::RevisionChanged);
        }
        let current_state = parse_state(&row.try_get::<String, _>("state")?)?;
        let delivery_mode = parse_delivery_mode(&row.try_get::<String, _>("delivery_mode")?)?;
        let protocol = parse_protocol(&row.try_get::<String, _>("protocol")?)?;
        let before = serde_json::json!({
            "deliveryMode": delivery_mode,
            "profileId": owner.profile_id,
            "protocol": protocol,
            "providerId": owner.provider_id,
            "revision": current_revision,
            "sessionId": session_id,
            "state": current_state,
        });
        let mutation = self
            .sessions
            .terminate_in_transaction(
                &mut transaction,
                owner,
                session_id,
                expected_revision,
                control_fencing_token,
                TerminalReason::ended(),
                now,
            )
            .await?;
        if mutation.previous_revision != expected_revision {
            return Err(LiveSessionAdminError::RevisionChanged);
        }
        let after = serde_json::json!({
            "deliveryMode": mutation.session.delivery_mode,
            "profileId": mutation.session.owner.profile_id,
            "protocol": mutation.session.protocol,
            "providerId": mutation.session.owner.provider_id,
            "revision": mutation.session.revision,
            "sessionId": mutation.session.id,
            "state": mutation.session.state,
        });
        let audit = audit
            .append(
                &mut transaction,
                home_id,
                AdminAction::SessionTerminate,
                "session",
                &session_id.to_string(),
                actor,
                Some(&before),
                Some(&after),
                None,
                now,
            )
            .await?;
        transaction.commit().await?;
        Ok(SessionTerminateMutation {
            status: "completed",
            revision: mutation.session.revision,
            session_id,
            state: mutation.session.state,
            audit,
        })
    }
}

fn decode_summary(row: &sqlx::any::AnyRow) -> Result<AdminSessionSummary, LiveSessionAdminError> {
    Ok(AdminSessionSummary {
        session_id: parse_uuid(&row.try_get::<String, _>("id")?)?,
        profile_id: parse_uuid(&row.try_get::<String, _>("profile_id")?)?,
        provider_id: parse_uuid(&row.try_get::<String, _>("provider_id")?)?,
        delivery_mode: parse_delivery_mode(&row.try_get::<String, _>("delivery_mode")?)?,
        protocol: parse_protocol(&row.try_get::<String, _>("protocol")?)?,
        state: parse_state(&row.try_get::<String, _>("state")?)?,
        revision: positive(row.try_get("revision")?)?,
        source_index: nonnegative_i32(row.try_get("source_index")?)?,
        failover_count: nonnegative_i32(row.try_get("failover_count")?)?,
        refresh_count: nonnegative_i32(row.try_get("refresh_count")?)?,
        created_at: parse_timestamp(&row.try_get::<String, _>("created_at")?)?,
        last_heartbeat_at: parse_timestamp(&row.try_get::<String, _>("last_heartbeat_at")?)?,
        expires_at: parse_timestamp(&row.try_get::<String, _>("expires_at")?)?,
        ended_at: row
            .try_get::<Option<String>, _>("ended_at")?
            .map(|value| parse_timestamp(&value))
            .transpose()?,
        error_code: row.try_get("error_code")?,
    })
}

fn parse_uuid(value: &str) -> Result<Uuid, LiveSessionAdminError> {
    Uuid::parse_str(value).map_err(|_| LiveSessionAdminError::InvalidState)
}

fn positive(value: i64) -> Result<i64, LiveSessionAdminError> {
    (value > 0)
        .then_some(value)
        .ok_or(LiveSessionAdminError::InvalidState)
}

fn nonnegative_i32(value: i64) -> Result<i32, LiveSessionAdminError> {
    if value < 0 {
        return Err(LiveSessionAdminError::InvalidState);
    }
    i32::try_from(value).map_err(|_| LiveSessionAdminError::InvalidState)
}

fn parse_delivery_mode(value: &str) -> Result<DeliveryMode, LiveSessionAdminError> {
    value
        .try_into()
        .map_err(|_| LiveSessionAdminError::InvalidState)
}

fn parse_protocol(value: &str) -> Result<SessionProtocol, LiveSessionAdminError> {
    value
        .try_into()
        .map_err(|_| LiveSessionAdminError::InvalidState)
}

fn parse_state(value: &str) -> Result<SessionState, LiveSessionAdminError> {
    value
        .try_into()
        .map_err(|_| LiveSessionAdminError::InvalidState)
}

fn parse_timestamp(value: &str) -> Result<DateTime<Utc>, LiveSessionAdminError> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .or_else(|_| {
            NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
                .map(|value| value.and_utc())
        })
        .map_err(|_| LiveSessionAdminError::InvalidState)
}

#[derive(Debug, Error)]
pub enum LiveSessionAdminError {
    #[error("invalid Live session administrative input")]
    InvalidInput,
    #[error("Live session administrative target was not found")]
    NotFound,
    #[error("Live session administrator is forbidden")]
    Forbidden,
    #[error("Live session administrative revision changed")]
    RevisionChanged,
    #[error("invalid persisted Live session administrative state")]
    InvalidState,
    #[error("Live session administrative storage failed")]
    Storage(#[from] sqlx::Error),
    #[error("Live session administrative mutation failed")]
    Session(#[from] SessionRepositoryError),
    #[error("Live session administrative audit failed")]
    Audit(#[from] LiveAuditError),
}
