use std::time::Duration;

use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{Any, AnyPool, Row, Transaction, ValueRef, any::AnyRow};
use thiserror::Error;
use tokio::sync::broadcast;
use uuid::Uuid;

const REGISTRY_KEY: &str = "authorization-revocation-v1";
const DEFAULT_RETENTION_DAYS: i64 = 30;
const MAX_PAYLOAD_BYTES: usize = 16 * 1024;
const MAX_CLAIM_SCAN: i64 = 64;
const MAX_LEASE_SECONDS: u64 = 300;
const NOTIFICATION_CAPACITY: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorizationRevocationEventType {
    AccountSessionRevoked,
    AccountRevoked,
    ProfileSwitched,
    ProfileDisabled,
    AuthorizationContextChanged,
    ProviderDisabled,
    ProviderPolicyChanged,
    ProviderGrantRevoked,
}

impl AuthorizationRevocationEventType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AccountSessionRevoked => "account_session_revoked",
            Self::AccountRevoked => "account_revoked",
            Self::ProfileSwitched => "profile_switched",
            Self::ProfileDisabled => "profile_disabled",
            Self::AuthorizationContextChanged => "authorization_context_changed",
            Self::ProviderDisabled => "provider_disabled",
            Self::ProviderPolicyChanged => "provider_policy_changed",
            Self::ProviderGrantRevoked => "provider_grant_revoked",
        }
    }
}

impl TryFrom<&str> for AuthorizationRevocationEventType {
    type Error = RevocationError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "account_session_revoked" => Ok(Self::AccountSessionRevoked),
            "account_revoked" => Ok(Self::AccountRevoked),
            "profile_switched" => Ok(Self::ProfileSwitched),
            "profile_disabled" => Ok(Self::ProfileDisabled),
            "authorization_context_changed" => Ok(Self::AuthorizationContextChanged),
            "provider_disabled" => Ok(Self::ProviderDisabled),
            "provider_policy_changed" => Ok(Self::ProviderPolicyChanged),
            "provider_grant_revoked" => Ok(Self::ProviderGrantRevoked),
            _ => Err(RevocationError::InvalidState("revocation event type")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorizationSubjectType {
    AccountSession,
    Account,
    Profile,
    Provider,
    ProviderGrant,
}

impl AuthorizationSubjectType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AccountSession => "account_session",
            Self::Account => "account",
            Self::Profile => "profile",
            Self::Provider => "provider",
            Self::ProviderGrant => "provider_grant",
        }
    }
}

impl TryFrom<&str> for AuthorizationSubjectType {
    type Error = RevocationError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "account_session" => Ok(Self::AccountSession),
            "account" => Ok(Self::Account),
            "profile" => Ok(Self::Profile),
            "provider" => Ok(Self::Provider),
            "provider_grant" => Ok(Self::ProviderGrant),
            _ => Err(RevocationError::InvalidState("authorization subject type")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct NewAuthorizationRevocation {
    pub home_id: Uuid,
    pub event_type: AuthorizationRevocationEventType,
    pub subject_type: AuthorizationSubjectType,
    pub subject_id: String,
    pub actor_user_id: Option<Uuid>,
    pub account_session_id: Option<Uuid>,
    pub profile_id: Option<Uuid>,
    pub provider_id: Option<Uuid>,
    pub grant_id: Option<Uuid>,
    pub reason_code: String,
    pub payload: Value,
}

impl NewAuthorizationRevocation {
    pub fn account_session(
        home_id: Uuid,
        account_session_id: Uuid,
        profile_id: Option<Uuid>,
        reason_code: impl Into<String>,
    ) -> Self {
        Self {
            home_id,
            event_type: AuthorizationRevocationEventType::AccountSessionRevoked,
            subject_type: AuthorizationSubjectType::AccountSession,
            subject_id: account_session_id.to_string(),
            actor_user_id: None,
            account_session_id: Some(account_session_id),
            profile_id,
            provider_id: None,
            grant_id: None,
            reason_code: reason_code.into(),
            payload: serde_json::json!({}),
        }
    }

    pub fn account(home_id: Uuid, user_id: Uuid, reason_code: impl Into<String>) -> Self {
        Self {
            home_id,
            event_type: AuthorizationRevocationEventType::AccountRevoked,
            subject_type: AuthorizationSubjectType::Account,
            subject_id: user_id.to_string(),
            actor_user_id: None,
            account_session_id: None,
            profile_id: None,
            provider_id: None,
            grant_id: None,
            reason_code: reason_code.into(),
            payload: serde_json::json!({}),
        }
    }

    pub fn profile_switched(
        home_id: Uuid,
        user_id: Uuid,
        account_session_id: Uuid,
        previous_profile_id: Uuid,
        selected_profile_id: Uuid,
    ) -> Self {
        Self {
            home_id,
            event_type: AuthorizationRevocationEventType::ProfileSwitched,
            subject_type: AuthorizationSubjectType::AccountSession,
            subject_id: account_session_id.to_string(),
            actor_user_id: Some(user_id),
            account_session_id: Some(account_session_id),
            profile_id: Some(previous_profile_id),
            provider_id: None,
            grant_id: None,
            reason_code: "profile_selected".to_string(),
            payload: serde_json::json!({
                "previous_profile_id": previous_profile_id,
                "selected_profile_id": selected_profile_id,
            }),
        }
    }

    pub fn provider_grant_revoked(
        home_id: Uuid,
        actor_user_id: Uuid,
        profile_id: Uuid,
        provider_id: Uuid,
        grant_id: Uuid,
        reason_code: impl Into<String>,
        before: (bool, bool),
        after: (bool, bool),
    ) -> Self {
        Self {
            home_id,
            event_type: AuthorizationRevocationEventType::ProviderGrantRevoked,
            subject_type: AuthorizationSubjectType::ProviderGrant,
            subject_id: grant_id.to_string(),
            actor_user_id: Some(actor_user_id),
            account_session_id: None,
            profile_id: Some(profile_id),
            provider_id: Some(provider_id),
            grant_id: Some(grant_id),
            reason_code: reason_code.into(),
            payload: serde_json::json!({
                "before": {"can_browse": before.0, "can_play": before.1},
                "after": {"can_browse": after.0, "can_play": after.1},
            }),
        }
    }

    pub fn provider_policy_changed(
        home_id: Uuid,
        actor_user_id: Uuid,
        provider_id: Uuid,
        reason_code: impl Into<String>,
        provider_revision: i64,
    ) -> Self {
        Self {
            home_id,
            event_type: AuthorizationRevocationEventType::ProviderPolicyChanged,
            subject_type: AuthorizationSubjectType::Provider,
            subject_id: provider_id.to_string(),
            actor_user_id: Some(actor_user_id),
            account_session_id: None,
            profile_id: None,
            provider_id: Some(provider_id),
            grant_id: None,
            reason_code: reason_code.into(),
            payload: serde_json::json!({
                "provider_revision": provider_revision,
            }),
        }
    }

    pub fn provider_disabled(
        home_id: Uuid,
        actor_user_id: Uuid,
        provider_id: Uuid,
        reason_code: impl Into<String>,
    ) -> Self {
        Self {
            home_id,
            event_type: AuthorizationRevocationEventType::ProviderDisabled,
            subject_type: AuthorizationSubjectType::Provider,
            subject_id: provider_id.to_string(),
            actor_user_id: Some(actor_user_id),
            account_session_id: None,
            profile_id: None,
            provider_id: Some(provider_id),
            grant_id: None,
            reason_code: reason_code.into(),
            payload: serde_json::json!({}),
        }
    }

    fn validate(&self) -> Result<(), RevocationError> {
        validate_identifier(&self.subject_id, "subject id", 512)?;
        validate_identifier(&self.reason_code, "reason code", 128)?;
        if !self.payload.is_object() {
            return Err(RevocationError::InvalidInput(
                "revocation payload must be an object",
            ));
        }
        let encoded = serde_json::to_vec(&self.payload)?;
        if encoded.len() > MAX_PAYLOAD_BYTES {
            return Err(RevocationError::InvalidInput(
                "revocation payload exceeds 16 KiB",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AuthorizationRevocationEvent {
    pub id: Uuid,
    pub home_id: Uuid,
    pub event_type: AuthorizationRevocationEventType,
    pub subject_type: AuthorizationSubjectType,
    pub subject_id: String,
    pub actor_user_id: Option<Uuid>,
    pub account_session_id: Option<Uuid>,
    pub profile_id: Option<Uuid>,
    pub provider_id: Option<Uuid>,
    pub grant_id: Option<Uuid>,
    pub reason_code: String,
    pub payload: Value,
    pub occurred_at: DateTime<Utc>,
    pub retain_until: DateTime<Utc>,
    pub published_at: Option<DateTime<Utc>>,
    pub publish_attempts: i32,
    pub last_error_redacted: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClaimedAuthorizationRevocation {
    pub event: AuthorizationRevocationEvent,
    pub consumer_name: String,
    pub lease_owner: String,
    pub lease_expires_at: DateTime<Utc>,
    pub attempts: i32,
}

struct ClaimCandidate {
    row: AnyRow,
    previous_owner: Option<String>,
    previous_expires_at: Option<String>,
    event_id: Uuid,
    attempts: i32,
}

#[derive(Debug, Error)]
pub enum RevocationError {
    #[error("invalid revocation input: {0}")]
    InvalidInput(&'static str),
    #[error("invalid persisted revocation state: {0}")]
    InvalidState(&'static str),
    #[error("authorization revocation consumer is not registered")]
    ConsumerNotRegistered,
    #[error("authorization revocation claim is no longer owned")]
    LeaseLost,
    #[error("authorization revocation registry revision overflow")]
    RegistryRevisionOverflow,
    #[error("authorization revocation database operation failed")]
    Storage(#[from] sqlx::Error),
    #[error("authorization revocation payload serialization failed")]
    Serialization(#[from] serde_json::Error),
}

#[derive(Debug, Clone)]
pub struct AuthorizationRevocationNotifier {
    sender: broadcast::Sender<Uuid>,
}

impl Default for AuthorizationRevocationNotifier {
    fn default() -> Self {
        Self::new()
    }
}

impl AuthorizationRevocationNotifier {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(NOTIFICATION_CAPACITY);
        Self { sender }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Uuid> {
        self.sender.subscribe()
    }

    pub fn publish(&self, event_id: Uuid) {
        let _ = self.sender.send(event_id);
    }
}

#[derive(Clone, Copy)]
pub struct AuthorizationRevocationStore<'a> {
    pool: &'a AnyPool,
}

impl<'a> AuthorizationRevocationStore<'a> {
    pub const fn new(pool: &'a AnyPool) -> Self {
        Self { pool }
    }

    pub async fn append(
        &self,
        event: &NewAuthorizationRevocation,
        notifier: Option<&AuthorizationRevocationNotifier>,
    ) -> Result<AuthorizationRevocationEvent, RevocationError> {
        let mut transaction = self.pool.begin().await?;
        let event = append_authorization_revocation_in_transaction(&mut transaction, event).await?;
        transaction.commit().await?;
        if let Some(notifier) = notifier {
            notifier.publish(event.id);
        }
        Ok(event)
    }

    pub async fn register_consumer(&self, consumer_name: &str) -> Result<(), RevocationError> {
        validate_consumer_identity(consumer_name, "consumer name")?;
        let mut transaction = self.pool.begin().await?;
        serialize_registry(&mut transaction).await?;
        sqlx::query(
            "INSERT INTO authorization_revocation_consumers (consumer_name)
             VALUES ($1)
             ON CONFLICT(consumer_name) DO UPDATE
             SET last_seen_at = CURRENT_TIMESTAMP,
                 updated_at = CURRENT_TIMESTAMP",
        )
        .bind(consumer_name)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO authorization_revocation_receipts (event_id, consumer_name)
             SELECT id, $1
             FROM authorization_revocation_outbox
             WHERE TRUE
             ON CONFLICT(event_id, consumer_name) DO NOTHING",
        )
        .bind(consumer_name)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn claim_next(
        &self,
        consumer_name: &str,
        lease_owner: &str,
        lease_duration: Duration,
    ) -> Result<Option<ClaimedAuthorizationRevocation>, RevocationError> {
        validate_consumer_identity(consumer_name, "consumer name")?;
        validate_consumer_identity(lease_owner, "lease owner")?;
        if lease_duration.is_zero() || lease_duration.as_secs() > MAX_LEASE_SECONDS {
            return Err(RevocationError::InvalidInput(
                "claim lease must be between 1 and 300 seconds",
            ));
        }
        let lease_duration = chrono::Duration::from_std(lease_duration)
            .map_err(|_| RevocationError::InvalidInput("claim lease is too large"))?;

        for _ in 0..3 {
            let mut transaction = self.pool.begin().await?;
            require_registered_consumer(&mut transaction, consumer_name).await?;
            let now = Utc::now();
            let Some(candidate) =
                load_claim_candidate(&mut transaction, consumer_name, now).await?
            else {
                transaction.commit().await?;
                return Ok(None);
            };
            let lease_expires_at =
                now.checked_add_signed(lease_duration)
                    .ok_or(RevocationError::InvalidInput(
                        "claim lease expiration overflow",
                    ))?;
            if let Some(claimed) = claim_candidate(
                &mut transaction,
                candidate,
                consumer_name,
                lease_owner,
                lease_expires_at,
            )
            .await?
            {
                transaction.commit().await?;
                return Ok(Some(claimed));
            }
            transaction.rollback().await?;
        }
        Ok(None)
    }

    pub async fn acknowledge(
        &self,
        event_id: Uuid,
        consumer_name: &str,
        lease_owner: &str,
    ) -> Result<(), RevocationError> {
        self.finish_claim(event_id, consumer_name, lease_owner, None)
            .await
    }

    pub async fn fail_claim(
        &self,
        event_id: Uuid,
        consumer_name: &str,
        lease_owner: &str,
        error_redacted: &str,
    ) -> Result<(), RevocationError> {
        let error_redacted = bounded_redacted_error(error_redacted)?;
        self.finish_claim(event_id, consumer_name, lease_owner, Some(error_redacted))
            .await
    }

    async fn finish_claim(
        &self,
        event_id: Uuid,
        consumer_name: &str,
        lease_owner: &str,
        error_redacted: Option<String>,
    ) -> Result<(), RevocationError> {
        validate_consumer_identity(consumer_name, "consumer name")?;
        validate_consumer_identity(lease_owner, "lease owner")?;
        let mut transaction = self.pool.begin().await?;
        let lease_expires_at: Option<String> = sqlx::query_scalar(
            "SELECT CAST(lease_expires_at AS TEXT)
             FROM authorization_revocation_receipts
             WHERE event_id = $1
               AND consumer_name = $2
               AND lease_owner = $3
               AND acknowledged_at IS NULL",
        )
        .bind(event_id.to_string())
        .bind(consumer_name)
        .bind(lease_owner)
        .fetch_optional(&mut *transaction)
        .await?;
        let lease_expires_at_raw = lease_expires_at
            .as_deref()
            .ok_or(RevocationError::LeaseLost)?;
        let lease_expires_at = parse_timestamp(lease_expires_at_raw)?;
        let now = Utc::now();
        if lease_expires_at <= now {
            return Err(RevocationError::LeaseLost);
        }
        let result = if let Some(error_redacted) = error_redacted {
            sqlx::query(
                "UPDATE authorization_revocation_receipts
                 SET lease_owner = NULL,
                     lease_expires_at = NULL,
                     last_error_redacted = $1,
                     updated_at = CURRENT_TIMESTAMP
                 WHERE event_id = $2
                   AND consumer_name = $3
                   AND lease_owner = $4
                   AND lease_expires_at = $5
                   AND acknowledged_at IS NULL",
            )
            .bind(error_redacted)
            .bind(event_id.to_string())
            .bind(consumer_name)
            .bind(lease_owner)
            .bind(lease_expires_at_raw)
            .execute(&mut *transaction)
            .await?
        } else {
            sqlx::query(
                "UPDATE authorization_revocation_receipts
                 SET acknowledged_at = CURRENT_TIMESTAMP,
                     lease_owner = NULL,
                     lease_expires_at = NULL,
                     last_error_redacted = NULL,
                     updated_at = CURRENT_TIMESTAMP
                 WHERE event_id = $1
                   AND consumer_name = $2
                   AND lease_owner = $3
                   AND lease_expires_at = $4
                   AND acknowledged_at IS NULL",
            )
            .bind(event_id.to_string())
            .bind(consumer_name)
            .bind(lease_owner)
            .bind(lease_expires_at_raw)
            .execute(&mut *transaction)
            .await?
        };
        if result.rows_affected() != 1 {
            return Err(RevocationError::LeaseLost);
        }
        sqlx::query(
            "UPDATE authorization_revocation_consumers
             SET last_seen_at = CURRENT_TIMESTAMP,
                 updated_at = CURRENT_TIMESTAMP
             WHERE consumer_name = $1",
        )
        .bind(consumer_name)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn record_publish_result(
        &self,
        event_id: Uuid,
        error_redacted: Option<&str>,
    ) -> Result<(), RevocationError> {
        let error_redacted = error_redacted.map(bounded_redacted_error).transpose()?;
        let result = if let Some(error_redacted) = error_redacted {
            sqlx::query(
                "UPDATE authorization_revocation_outbox
                 SET publish_attempts = publish_attempts + 1,
                     last_error_redacted = $1
                 WHERE id = $2 AND publish_attempts < $3",
            )
            .bind(error_redacted)
            .bind(event_id.to_string())
            .bind(i32::MAX)
            .execute(self.pool)
            .await?
        } else {
            sqlx::query(
                "UPDATE authorization_revocation_outbox
                 SET published_at = COALESCE(published_at, CURRENT_TIMESTAMP),
                     publish_attempts = publish_attempts + 1,
                     last_error_redacted = NULL
                 WHERE id = $1 AND publish_attempts < $2",
            )
            .bind(event_id.to_string())
            .bind(i32::MAX)
            .execute(self.pool)
            .await?
        };
        if result.rows_affected() != 1 {
            return Err(RevocationError::InvalidState(
                "event missing or publish attempt overflow",
            ));
        }
        Ok(())
    }

    pub async fn cleanup_acknowledged_before(
        &self,
        cutoff: DateTime<Utc>,
    ) -> Result<u64, RevocationError> {
        let cutoff = std::cmp::min(cutoff, Utc::now());
        let mut transaction = self.pool.begin().await?;
        serialize_registry(&mut transaction).await?;
        let result = sqlx::query(
            "DELETE FROM authorization_revocation_outbox
             WHERE authorization_revocation_outbox.retain_until <= $1
               AND NOT EXISTS (
                   SELECT 1
                   FROM authorization_revocation_consumers AS consumer
                   LEFT JOIN authorization_revocation_receipts AS receipt
                     ON receipt.event_id = authorization_revocation_outbox.id
                    AND receipt.consumer_name = consumer.consumer_name
                   WHERE receipt.event_id IS NULL
                      OR receipt.acknowledged_at IS NULL
               )",
        )
        .bind(cutoff.to_rfc3339())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(result.rows_affected())
    }
}

async fn require_registered_consumer(
    transaction: &mut Transaction<'_, Any>,
    consumer_name: &str,
) -> Result<(), RevocationError> {
    let registered: Option<i64> = sqlx::query_scalar(
        "SELECT 1 FROM authorization_revocation_consumers WHERE consumer_name = $1",
    )
    .bind(consumer_name)
    .fetch_optional(&mut **transaction)
    .await?;
    if registered.is_none() {
        return Err(RevocationError::ConsumerNotRegistered);
    }
    Ok(())
}

async fn load_claim_candidate(
    transaction: &mut Transaction<'_, Any>,
    consumer_name: &str,
    now: DateTime<Utc>,
) -> Result<Option<ClaimCandidate>, RevocationError> {
    let rows = sqlx::query(
        "SELECT r.event_id,
                CAST(r.lease_owner AS TEXT) AS receipt_lease_owner,
                CAST(r.lease_expires_at AS TEXT) AS receipt_lease_expires_at,
                r.attempts AS receipt_attempts,
                o.home_id, o.event_type, o.subject_type, o.subject_id,
                CAST(o.actor_user_id AS TEXT) AS actor_user_id,
                CAST(o.account_session_id AS TEXT) AS account_session_id,
                CAST(o.profile_id AS TEXT) AS profile_id,
                CAST(o.provider_id AS TEXT) AS provider_id,
                CAST(o.grant_id AS TEXT) AS grant_id,
                o.reason_code, o.payload_json,
                CAST(o.occurred_at AS TEXT) AS occurred_at,
                CAST(o.retain_until AS TEXT) AS retain_until,
                CAST(o.published_at AS TEXT) AS published_at,
                o.publish_attempts,
                CAST(o.last_error_redacted AS TEXT) AS last_error_redacted
         FROM authorization_revocation_receipts AS r
         JOIN authorization_revocation_outbox AS o ON o.id = r.event_id
         WHERE r.consumer_name = $1
           AND r.acknowledged_at IS NULL
         ORDER BY o.occurred_at, o.id
         LIMIT $2",
    )
    .bind(consumer_name)
    .bind(MAX_CLAIM_SCAN)
    .fetch_all(&mut **transaction)
    .await?;

    for row in rows {
        let previous_owner = optional_string(&row, "receipt_lease_owner")?;
        let previous_expires_at = optional_string(&row, "receipt_lease_expires_at")?;
        let claimable = match (previous_owner.as_deref(), previous_expires_at.as_deref()) {
            (None, None) => true,
            (Some(_), Some(expires_at)) => parse_timestamp(expires_at)? <= now,
            _ => return Err(RevocationError::InvalidState("receipt lease pair")),
        };
        if !claimable {
            continue;
        }
        let event_id: String = row.try_get("event_id")?;
        let attempts: i32 = row.try_get("receipt_attempts")?;
        if attempts == i32::MAX {
            return Err(RevocationError::InvalidState("receipt attempt overflow"));
        }
        return Ok(Some(ClaimCandidate {
            event_id: parse_uuid(&event_id, "revocation event id")?,
            row,
            previous_owner,
            previous_expires_at,
            attempts,
        }));
    }
    Ok(None)
}

async fn claim_candidate(
    transaction: &mut Transaction<'_, Any>,
    candidate: ClaimCandidate,
    consumer_name: &str,
    lease_owner: &str,
    lease_expires_at: DateTime<Utc>,
) -> Result<Option<ClaimedAuthorizationRevocation>, RevocationError> {
    let result = match (
        candidate.previous_owner.as_deref(),
        candidate.previous_expires_at.as_deref(),
    ) {
        (None, None) => {
            sqlx::query(
                "UPDATE authorization_revocation_receipts
                 SET lease_owner = $1,
                     lease_expires_at = $2,
                     attempts = attempts + 1,
                     last_error_redacted = NULL,
                     updated_at = CURRENT_TIMESTAMP
                 WHERE event_id = $3
                   AND consumer_name = $4
                   AND acknowledged_at IS NULL
                   AND lease_owner IS NULL
                   AND lease_expires_at IS NULL
                   AND attempts < $5",
            )
            .bind(lease_owner)
            .bind(lease_expires_at.to_rfc3339())
            .bind(candidate.event_id.to_string())
            .bind(consumer_name)
            .bind(i32::MAX)
            .execute(&mut **transaction)
            .await?
        }
        (Some(previous_owner), Some(previous_expires_at)) => {
            sqlx::query(
                "UPDATE authorization_revocation_receipts
                 SET lease_owner = $1,
                     lease_expires_at = $2,
                     attempts = attempts + 1,
                     last_error_redacted = NULL,
                     updated_at = CURRENT_TIMESTAMP
                 WHERE event_id = $3
                   AND consumer_name = $4
                   AND acknowledged_at IS NULL
                   AND lease_owner = $5
                   AND lease_expires_at = $6
                   AND attempts < $7",
            )
            .bind(lease_owner)
            .bind(lease_expires_at.to_rfc3339())
            .bind(candidate.event_id.to_string())
            .bind(consumer_name)
            .bind(previous_owner)
            .bind(previous_expires_at)
            .bind(i32::MAX)
            .execute(&mut **transaction)
            .await?
        }
        _ => return Err(RevocationError::InvalidState("receipt lease pair")),
    };
    if result.rows_affected() != 1 {
        return Ok(None);
    }
    Ok(Some(ClaimedAuthorizationRevocation {
        event: map_event(&candidate.row)?,
        consumer_name: consumer_name.to_string(),
        lease_owner: lease_owner.to_string(),
        lease_expires_at,
        attempts: candidate.attempts + 1,
    }))
}

pub async fn append_authorization_revocation_in_transaction(
    transaction: &mut Transaction<'_, Any>,
    event: &NewAuthorizationRevocation,
) -> Result<AuthorizationRevocationEvent, RevocationError> {
    event.validate()?;
    serialize_registry(transaction).await?;
    let id = Uuid::new_v4();
    let occurred_at = Utc::now();
    let retain_until = occurred_at
        .checked_add_signed(chrono::Duration::days(DEFAULT_RETENTION_DAYS))
        .ok_or(RevocationError::InvalidInput(
            "retention expiration overflow",
        ))?;
    let payload_json = serde_json::to_string(&event.payload)?;
    sqlx::query(
        "INSERT INTO authorization_revocation_outbox (
            id, home_id, event_type, subject_type, subject_id, actor_user_id,
            account_session_id, profile_id, provider_id, grant_id, reason_code,
            payload_json, occurred_at, retain_until
         ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
         )",
    )
    .bind(id.to_string())
    .bind(event.home_id.to_string())
    .bind(event.event_type.as_str())
    .bind(event.subject_type.as_str())
    .bind(&event.subject_id)
    .bind(event.actor_user_id.map(|value| value.to_string()))
    .bind(event.account_session_id.map(|value| value.to_string()))
    .bind(event.profile_id.map(|value| value.to_string()))
    .bind(event.provider_id.map(|value| value.to_string()))
    .bind(event.grant_id.map(|value| value.to_string()))
    .bind(&event.reason_code)
    .bind(payload_json)
    .bind(occurred_at.to_rfc3339())
    .bind(retain_until.to_rfc3339())
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "INSERT INTO authorization_revocation_receipts (event_id, consumer_name)
         SELECT $1, consumer_name
         FROM authorization_revocation_consumers
         WHERE TRUE
         ON CONFLICT(event_id, consumer_name) DO NOTHING",
    )
    .bind(id.to_string())
    .execute(&mut **transaction)
    .await?;
    Ok(AuthorizationRevocationEvent {
        id,
        home_id: event.home_id,
        event_type: event.event_type,
        subject_type: event.subject_type,
        subject_id: event.subject_id.clone(),
        actor_user_id: event.actor_user_id,
        account_session_id: event.account_session_id,
        profile_id: event.profile_id,
        provider_id: event.provider_id,
        grant_id: event.grant_id,
        reason_code: event.reason_code.clone(),
        payload: event.payload.clone(),
        occurred_at,
        retain_until,
        published_at: None,
        publish_attempts: 0,
        last_error_redacted: None,
    })
}

async fn serialize_registry(
    transaction: &mut Transaction<'_, Any>,
) -> Result<i64, RevocationError> {
    let revision: Option<i64> = sqlx::query_scalar(
        "UPDATE authorization_revocation_registry
         SET revision = revision + 1,
             updated_at = CURRENT_TIMESTAMP
         WHERE singleton_key = $1
           AND revision < $2
         RETURNING revision",
    )
    .bind(REGISTRY_KEY)
    .bind(i64::MAX)
    .fetch_optional(&mut **transaction)
    .await?;
    revision.ok_or(RevocationError::RegistryRevisionOverflow)
}

fn map_event(row: &AnyRow) -> Result<AuthorizationRevocationEvent, RevocationError> {
    let id: String = row.try_get("event_id")?;
    let home_id: String = row.try_get("home_id")?;
    let event_type: String = row.try_get("event_type")?;
    let subject_type: String = row.try_get("subject_type")?;
    let payload_json: String = row.try_get("payload_json")?;
    let occurred_at: String = row.try_get("occurred_at")?;
    let retain_until: String = row.try_get("retain_until")?;
    Ok(AuthorizationRevocationEvent {
        id: parse_uuid(&id, "revocation event id")?,
        home_id: parse_uuid(&home_id, "revocation home id")?,
        event_type: AuthorizationRevocationEventType::try_from(event_type.as_str())?,
        subject_type: AuthorizationSubjectType::try_from(subject_type.as_str())?,
        subject_id: row.try_get("subject_id")?,
        actor_user_id: parse_optional_uuid(
            optional_string(row, "actor_user_id")?,
            "actor user id",
        )?,
        account_session_id: parse_optional_uuid(
            optional_string(row, "account_session_id")?,
            "account session id",
        )?,
        profile_id: parse_optional_uuid(optional_string(row, "profile_id")?, "profile id")?,
        provider_id: parse_optional_uuid(optional_string(row, "provider_id")?, "provider id")?,
        grant_id: parse_optional_uuid(optional_string(row, "grant_id")?, "grant id")?,
        reason_code: row.try_get("reason_code")?,
        payload: serde_json::from_str(&payload_json)?,
        occurred_at: parse_timestamp(&occurred_at)?,
        retain_until: parse_timestamp(&retain_until)?,
        published_at: optional_string(row, "published_at")?
            .as_deref()
            .map(parse_timestamp)
            .transpose()?,
        publish_attempts: row.try_get("publish_attempts")?,
        last_error_redacted: optional_string(row, "last_error_redacted")?,
    })
}

fn validate_identifier(
    value: &str,
    label: &'static str,
    max_len: usize,
) -> Result<(), RevocationError> {
    if value.trim() != value
        || value.is_empty()
        || value.len() > max_len
        || value.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(RevocationError::InvalidInput(label));
    }
    Ok(())
}

fn validate_consumer_identity(value: &str, label: &'static str) -> Result<(), RevocationError> {
    validate_identifier(value, label, 128)?;
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':'))
    {
        return Err(RevocationError::InvalidInput(label));
    }
    Ok(())
}

fn bounded_redacted_error(value: &str) -> Result<String, RevocationError> {
    let value = value.trim();
    if value.is_empty() || value.len() > 512 || value.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(RevocationError::InvalidInput("redacted consumer error"));
    }
    Ok(value.to_string())
}

fn optional_string(row: &AnyRow, field: &str) -> Result<Option<String>, sqlx::Error> {
    let raw = row.try_get_raw(field)?;
    if raw.is_null() {
        Ok(None)
    } else {
        row.try_get(field).map(Some)
    }
}

fn parse_optional_uuid(
    value: Option<String>,
    field: &'static str,
) -> Result<Option<Uuid>, RevocationError> {
    value
        .as_deref()
        .map(|value| parse_uuid(value, field))
        .transpose()
}

fn parse_uuid(value: &str, _field: &'static str) -> Result<Uuid, RevocationError> {
    Uuid::parse_str(value).map_err(|_| RevocationError::InvalidState("UUID"))
}

fn parse_timestamp(value: &str) -> Result<DateTime<Utc>, RevocationError> {
    if let Ok(value) = DateTime::parse_from_rfc3339(value) {
        return Ok(value.with_timezone(&Utc));
    }
    for format in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S%.f"] {
        if let Ok(value) = NaiveDateTime::parse_from_str(value, format) {
            return Ok(value.and_utc());
        }
    }
    Err(RevocationError::InvalidState("timestamp"))
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::*;
    use crate::{config::DatabaseConfig, db::Database};

    async fn test_database() -> Result<Database> {
        let database = Database::connect(&DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        })
        .await?;
        database.run_migrations().await?;
        Ok(database)
    }

    fn account_event(home_id: Uuid, account_id: Uuid, reason: &str) -> NewAuthorizationRevocation {
        NewAuthorizationRevocation::account(home_id, account_id, reason)
    }

    #[tokio::test]
    async fn a12_revocation_outbox_backfills_claims_retries_and_cleans_exact_receipts() -> Result<()>
    {
        let database = test_database().await?;
        let store = AuthorizationRevocationStore::new(&database.pool);
        let notifier = AuthorizationRevocationNotifier::new();
        let mut notifications = notifier.subscribe();
        let home_id = Uuid::new_v4();
        let account_id = Uuid::new_v4();

        let first = store
            .append(
                &account_event(home_id, account_id, "account_disabled"),
                Some(&notifier),
            )
            .await?;
        assert_eq!(notifications.try_recv()?, first.id);
        store
            .record_publish_result(first.id, Some("broker unavailable"))
            .await?;
        store.record_publish_result(first.id, None).await?;

        store.register_consumer("live-session-revoker").await?;
        let receipt_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM authorization_revocation_receipts
             WHERE consumer_name = 'live-session-revoker'",
        )
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(
            receipt_count, 1,
            "registration must backfill retained events"
        );

        let claimed = store
            .claim_next("live-session-revoker", "worker-a", Duration::from_secs(30))
            .await?
            .expect("first retained event should be claimable");
        assert_eq!(claimed.event.id, first.id);
        assert_eq!(claimed.attempts, 1);
        assert!(
            store
                .claim_next("live-session-revoker", "worker-b", Duration::from_secs(30),)
                .await?
                .is_none(),
            "a live lease must exclude a second owner"
        );
        assert!(matches!(
            store
                .acknowledge(first.id, "live-session-revoker", "worker-b")
                .await,
            Err(RevocationError::LeaseLost)
        ));
        store
            .fail_claim(
                first.id,
                "live-session-revoker",
                "worker-a",
                "transient downstream failure",
            )
            .await?;
        let reclaimed = store
            .claim_next("live-session-revoker", "worker-b", Duration::from_secs(30))
            .await?
            .expect("failed claim should be immediately retryable");
        assert_eq!(reclaimed.event.id, first.id);
        assert_eq!(reclaimed.attempts, 2);
        store
            .acknowledge(first.id, "live-session-revoker", "worker-b")
            .await?;

        let second = store
            .append(&account_event(home_id, account_id, "password_reset"), None)
            .await?;
        let claimed = store
            .claim_next("live-session-revoker", "worker-a", Duration::from_secs(30))
            .await?
            .expect("events appended after registration need a receipt");
        assert_eq!(claimed.event.id, second.id);
        store
            .acknowledge(second.id, "live-session-revoker", "worker-a")
            .await?;

        store.register_consumer("security-audit").await?;
        sqlx::query(
            "UPDATE authorization_revocation_outbox
             SET retain_until = $1",
        )
        .bind((Utc::now() - chrono::Duration::hours(1)).to_rfc3339())
        .execute(&database.pool)
        .await?;
        assert_eq!(
            store
                .cleanup_acknowledged_before(Utc::now() + chrono::Duration::days(365))
                .await?,
            0,
            "cleanup must retain events with a pending registered consumer"
        );

        let mut audited = Vec::new();
        while let Some(claim) = store
            .claim_next("security-audit", "audit-worker", Duration::from_secs(30))
            .await?
        {
            audited.push(claim.event.id);
            store
                .acknowledge(claim.event.id, "security-audit", "audit-worker")
                .await?;
        }
        audited.sort_unstable();
        let mut expected = vec![first.id, second.id];
        expected.sort_unstable();
        assert_eq!(audited, expected);
        assert_eq!(
            store
                .cleanup_acknowledged_before(Utc::now() + chrono::Duration::days(365))
                .await?,
            2
        );
        let remaining: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM authorization_revocation_outbox")
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(remaining, 0);
        Ok(())
    }
}
