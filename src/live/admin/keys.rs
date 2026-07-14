use std::sync::Arc;

use base64::{Engine as _, engine::general_purpose};
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{Any, AnyPool, Row, Transaction};
use thiserror::Error;
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::{
    live::{
        admin::{ActorSnapshot, AdminAction, AuditReference, LiveAuditChain, LiveAuditError},
        crypto::{LiveCrypto, LiveCryptoError, validate_live_key_id},
        session::{LiveSessionRepository, SessionRepositoryError},
    },
    secrets::SecretsManager,
};

use super::LiveAuditKey;

const KEY_STATE_ID: &str = "live-crypto-v1";
const AUDIT_SECRET_PREFIX: &str = "live.crypto.audit.";
const ROTATION_BATCH: u32 = 256;
const MAX_ROTATION_BATCHES: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LiveKeyState {
    pub envelope_primary_key_id: String,
    pub token_hash_primary_key_id: String,
    pub audit_primary_key_id: String,
    pub revision: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveKeyDomain {
    Envelope,
    TokenHash,
    Audit,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LiveKeyRotationMutation {
    pub status: &'static str,
    pub revision: i64,
    pub key_domain: LiveKeyDomain,
    pub primary_key_id: String,
    pub previous_primary_key_id: String,
    pub reencrypted_sessions: u64,
    pub reencrypted_replays: u64,
    pub terminated_sessions: u64,
    pub invalidated_cache_entries: u64,
    pub audit: AuditReference,
}

#[derive(Clone)]
pub struct LiveKeyAdminService {
    pool: AnyPool,
    sessions: Arc<LiveSessionRepository>,
    crypto: Arc<LiveCrypto>,
    audit: Arc<LiveAuditChain>,
    secrets: Arc<SecretsManager>,
}

impl LiveKeyAdminService {
    pub fn new(
        pool: AnyPool,
        sessions: Arc<LiveSessionRepository>,
        crypto: Arc<LiveCrypto>,
        audit: Arc<LiveAuditChain>,
        secrets: Arc<SecretsManager>,
    ) -> Self {
        Self {
            pool,
            sessions,
            crypto,
            audit,
            secrets,
        }
    }

    pub async fn state(&self) -> Result<LiveKeyState, LiveKeyAdminError> {
        let row = sqlx::query(
            "SELECT envelope_primary_key_id, token_hash_primary_key_id,
                    audit_primary_key_id, revision
             FROM live_key_rotation_state WHERE state_id = $1",
        )
        .bind(KEY_STATE_ID)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(LiveKeyAdminError::InvalidState)?;
        decode_key_state(&row)
    }

    pub async fn rotate_envelope(
        &self,
        home_id: Uuid,
        expected_revision: i64,
        key_id: &str,
        control_fencing_token: i64,
        actor: &ActorSnapshot,
        now: DateTime<Utc>,
    ) -> Result<LiveKeyRotationMutation, LiveKeyAdminError> {
        validate_rotation_input(expected_revision, key_id)?;
        self.assert_actor(home_id, actor).await?;
        if !self.crypto.has_envelope_key(key_id) {
            return Err(LiveKeyAdminError::KeyNotConfigured);
        }
        let before = self.state().await?;
        if before.revision != expected_revision {
            return Err(LiveKeyAdminError::RevisionChanged);
        }
        let previous_key_id = self.crypto.rotate_envelope_primary(key_id)?;
        if previous_key_id != before.envelope_primary_key_id {
            let _ = self
                .crypto
                .rotate_envelope_primary(&before.envelope_primary_key_id);
            return Err(LiveKeyAdminError::InvalidState);
        }

        let operation = async {
            let mut reencrypted_sessions = 0_u64;
            let mut reencrypted_replays = 0_u64;
            for _ in 0..MAX_ROTATION_BATCHES {
                if self
                    .sessions
                    .count_active_envelopes_not_using(key_id, now)
                    .await?
                    == 0
                {
                    break;
                }
                let report = self
                    .sessions
                    .reencrypt_active_envelopes(now, control_fencing_token, ROTATION_BATCH)
                    .await?;
                if report.reencrypted_sessions == 0 && report.reencrypted_replays == 0 {
                    return Err(LiveKeyAdminError::InvalidState);
                }
                reencrypted_sessions = reencrypted_sessions
                    .checked_add(report.reencrypted_sessions)
                    .ok_or(LiveKeyAdminError::InvalidState)?;
                reencrypted_replays = reencrypted_replays
                    .checked_add(report.reencrypted_replays)
                    .ok_or(LiveKeyAdminError::InvalidState)?;
            }
            if self
                .sessions
                .count_active_envelopes_not_using(key_id, now)
                .await?
                != 0
            {
                return Err(LiveKeyAdminError::CapacityExceeded);
            }
            let invalidated_cache_entries = sqlx::query("DELETE FROM live_provider_cache")
                .execute(&self.pool)
                .await?
                .rows_affected();
            self.commit_rotation(
                home_id,
                expected_revision,
                key_id,
                LiveKeyDomain::Envelope,
                AdminAction::EnvelopeKeyRotate,
                actor,
                RotationEffects {
                    reencrypted_sessions,
                    reencrypted_replays,
                    invalidated_cache_entries,
                    ..RotationEffects::default()
                },
                now,
            )
            .await
        }
        .await;
        if operation.is_err() {
            let _ = self.crypto.rotate_envelope_primary(&previous_key_id);
        }
        operation
    }

    pub async fn rotate_token_hash(
        &self,
        home_id: Uuid,
        expected_revision: i64,
        key_id: &str,
        control_fencing_token: i64,
        actor: &ActorSnapshot,
        now: DateTime<Utc>,
    ) -> Result<LiveKeyRotationMutation, LiveKeyAdminError> {
        validate_rotation_input(expected_revision, key_id)?;
        self.assert_actor(home_id, actor).await?;
        if !self.crypto.has_token_hash_key(key_id) {
            return Err(LiveKeyAdminError::KeyNotConfigured);
        }
        let before = self.state().await?;
        if before.revision != expected_revision {
            return Err(LiveKeyAdminError::RevisionChanged);
        }
        if self.crypto.token_hash_primary_key_id()? != before.token_hash_primary_key_id {
            return Err(LiveKeyAdminError::InvalidState);
        }
        let mut terminated_sessions = 0_u64;
        if key_id != before.token_hash_primary_key_id {
            for _ in 0..MAX_ROTATION_BATCHES {
                if self.sessions.count_active_server_delivery().await? == 0 {
                    break;
                }
                let terminated = self
                    .sessions
                    .terminate_server_delivery_for_token_rotation(
                        now,
                        control_fencing_token,
                        ROTATION_BATCH,
                    )
                    .await?;
                if terminated == 0 {
                    return Err(LiveKeyAdminError::InvalidState);
                }
                terminated_sessions = terminated_sessions
                    .checked_add(terminated)
                    .ok_or(LiveKeyAdminError::InvalidState)?;
            }
            if self.sessions.count_active_server_delivery().await? != 0 {
                return Err(LiveKeyAdminError::CapacityExceeded);
            }
        }
        let mutation = self
            .commit_rotation(
                home_id,
                expected_revision,
                key_id,
                LiveKeyDomain::TokenHash,
                AdminAction::TokenHashKeyRotate,
                actor,
                RotationEffects {
                    terminated_sessions,
                    ..RotationEffects::default()
                },
                now,
            )
            .await?;
        self.crypto.rotate_token_hash_primary(key_id)?;
        Ok(mutation)
    }

    pub async fn rotate_audit(
        &self,
        home_id: Uuid,
        expected_revision: i64,
        key_id: &str,
        actor: &ActorSnapshot,
        now: DateTime<Utc>,
    ) -> Result<LiveKeyRotationMutation, LiveKeyAdminError> {
        validate_rotation_input(expected_revision, key_id)?;
        self.assert_actor(home_id, actor).await?;
        let before = self.state().await?;
        if before.revision != expected_revision {
            return Err(LiveKeyAdminError::RevisionChanged);
        }
        if self.audit.primary_key_id()? != before.audit_primary_key_id {
            return Err(LiveKeyAdminError::InvalidState);
        }
        let new_key = self.load_audit_key(key_id).await?;
        let mutation = self
            .commit_rotation(
                home_id,
                expected_revision,
                key_id,
                LiveKeyDomain::Audit,
                AdminAction::AuditKeyRotate,
                actor,
                RotationEffects::default(),
                now,
            )
            .await?;
        self.audit.rotate_primary(new_key)?;
        Ok(mutation)
    }

    #[allow(clippy::too_many_arguments)]
    async fn commit_rotation(
        &self,
        home_id: Uuid,
        expected_revision: i64,
        key_id: &str,
        domain: LiveKeyDomain,
        action: AdminAction,
        actor: &ActorSnapshot,
        effects: RotationEffects,
        now: DateTime<Utc>,
    ) -> Result<LiveKeyRotationMutation, LiveKeyAdminError> {
        let mut transaction = self.pool.begin().await?;
        assert_actor_in_transaction(&mut transaction, home_id, actor).await?;
        let row = sqlx::query(
            "UPDATE live_key_rotation_state SET updated_at = updated_at
             WHERE state_id = $1
             RETURNING envelope_primary_key_id, token_hash_primary_key_id,
                       audit_primary_key_id, revision",
        )
        .bind(KEY_STATE_ID)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(LiveKeyAdminError::InvalidState)?;
        let before = decode_key_state(&row)?;
        if before.revision != expected_revision {
            return Err(LiveKeyAdminError::RevisionChanged);
        }
        let previous_primary_key_id = before.primary(domain).to_string();
        let changed = previous_primary_key_id != key_id;
        let revision = if changed {
            expected_revision
                .checked_add(1)
                .ok_or(LiveKeyAdminError::InvalidState)?
        } else {
            expected_revision
        };
        let mut after = before.clone();
        *after.primary_mut(domain) = key_id.to_string();
        after.revision = revision;
        let before_json = rotation_snapshot(&before, domain, &RotationEffects::default());
        let after_json = rotation_snapshot(&after, domain, &effects);
        let audit = self
            .audit
            .append(
                &mut transaction,
                home_id,
                action,
                "key_configuration",
                domain.as_str(),
                actor,
                Some(&before_json),
                Some(&after_json),
                None,
                now,
            )
            .await?;
        if changed {
            let updated = sqlx::query(
                "UPDATE live_key_rotation_state
                 SET envelope_primary_key_id = $1, token_hash_primary_key_id = $2,
                     audit_primary_key_id = $3, revision = $4,
                     updated_at = CURRENT_TIMESTAMP
                 WHERE state_id = $5 AND revision = $6
                   AND envelope_primary_key_id = $7
                   AND token_hash_primary_key_id = $8
                   AND audit_primary_key_id = $9",
            )
            .bind(&after.envelope_primary_key_id)
            .bind(&after.token_hash_primary_key_id)
            .bind(&after.audit_primary_key_id)
            .bind(revision)
            .bind(KEY_STATE_ID)
            .bind(expected_revision)
            .bind(&before.envelope_primary_key_id)
            .bind(&before.token_hash_primary_key_id)
            .bind(&before.audit_primary_key_id)
            .execute(&mut *transaction)
            .await?;
            if updated.rows_affected() != 1 {
                return Err(LiveKeyAdminError::RevisionChanged);
            }
        }
        transaction.commit().await?;
        Ok(LiveKeyRotationMutation {
            status: "completed",
            revision,
            key_domain: domain,
            primary_key_id: key_id.to_string(),
            previous_primary_key_id,
            reencrypted_sessions: effects.reencrypted_sessions,
            reencrypted_replays: effects.reencrypted_replays,
            terminated_sessions: effects.terminated_sessions,
            invalidated_cache_entries: effects.invalidated_cache_entries,
            audit,
        })
    }

    async fn assert_actor(
        &self,
        home_id: Uuid,
        actor: &ActorSnapshot,
    ) -> Result<(), LiveKeyAdminError> {
        let role: Option<String> = sqlx::query_scalar(
            "SELECT role FROM home_members
             WHERE home_id = $1 AND user_id = $2 AND status = 'active' LIMIT 1",
        )
        .bind(home_id.to_string())
        .bind(actor.actor_user_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        if role.as_deref() != Some(actor.home_role.as_str()) {
            return Err(LiveKeyAdminError::Forbidden);
        }
        Ok(())
    }

    async fn load_audit_key(&self, key_id: &str) -> Result<LiveAuditKey, LiveKeyAdminError> {
        let secret_key = format!("{AUDIT_SECRET_PREFIX}{key_id}");
        let encrypted: Option<String> = sqlx::query_scalar(
            "SELECT value_encrypted FROM secrets
             WHERE scope = 'global' AND scope_id IS NULL AND key = $1 LIMIT 1",
        )
        .bind(secret_key)
        .fetch_optional(&self.pool)
        .await?;
        let encrypted = encrypted.ok_or(LiveKeyAdminError::KeyNotConfigured)?;
        if !SecretsManager::is_encrypted(&encrypted) {
            return Err(LiveKeyAdminError::InvalidState);
        }
        let plaintext = Zeroizing::new(
            self.secrets
                .decrypt(&encrypted)
                .map_err(LiveKeyAdminError::SecretStore)?,
        );
        let decoded = general_purpose::STANDARD
            .decode(plaintext.trim())
            .map(Zeroizing::new)
            .map_err(|_| LiveKeyAdminError::InvalidState)?;
        if decoded.len() != 32 {
            return Err(LiveKeyAdminError::InvalidState);
        }
        let mut material = [0_u8; 32];
        material.copy_from_slice(decoded.as_slice());
        LiveAuditKey::new(key_id, material).map_err(LiveKeyAdminError::Audit)
    }
}

impl LiveKeyState {
    fn primary(&self, domain: LiveKeyDomain) -> &str {
        match domain {
            LiveKeyDomain::Envelope => &self.envelope_primary_key_id,
            LiveKeyDomain::TokenHash => &self.token_hash_primary_key_id,
            LiveKeyDomain::Audit => &self.audit_primary_key_id,
        }
    }

    fn primary_mut(&mut self, domain: LiveKeyDomain) -> &mut String {
        match domain {
            LiveKeyDomain::Envelope => &mut self.envelope_primary_key_id,
            LiveKeyDomain::TokenHash => &mut self.token_hash_primary_key_id,
            LiveKeyDomain::Audit => &mut self.audit_primary_key_id,
        }
    }
}

impl LiveKeyDomain {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Envelope => "envelope",
            Self::TokenHash => "token_hash",
            Self::Audit => "audit",
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct RotationEffects {
    reencrypted_sessions: u64,
    reencrypted_replays: u64,
    terminated_sessions: u64,
    invalidated_cache_entries: u64,
}

fn rotation_snapshot(
    state: &LiveKeyState,
    domain: LiveKeyDomain,
    effects: &RotationEffects,
) -> serde_json::Value {
    serde_json::json!({
        "invalidatedCacheEntries": effects.invalidated_cache_entries,
        "keyDomain": domain,
        "primaryKeyId": state.primary(domain),
        "reencryptedReplays": effects.reencrypted_replays,
        "reencryptedSessions": effects.reencrypted_sessions,
        "revision": state.revision,
        "terminatedSessions": effects.terminated_sessions,
    })
}

fn decode_key_state(row: &sqlx::any::AnyRow) -> Result<LiveKeyState, LiveKeyAdminError> {
    let state = LiveKeyState {
        envelope_primary_key_id: row.try_get("envelope_primary_key_id")?,
        token_hash_primary_key_id: row.try_get("token_hash_primary_key_id")?,
        audit_primary_key_id: row.try_get("audit_primary_key_id")?,
        revision: row.try_get("revision")?,
    };
    for key_id in [
        state.envelope_primary_key_id.as_str(),
        state.token_hash_primary_key_id.as_str(),
        state.audit_primary_key_id.as_str(),
    ] {
        validate_live_key_id(key_id).map_err(|_| LiveKeyAdminError::InvalidState)?;
    }
    if state.revision < 1 {
        return Err(LiveKeyAdminError::InvalidState);
    }
    Ok(state)
}

fn validate_rotation_input(expected_revision: i64, key_id: &str) -> Result<(), LiveKeyAdminError> {
    if expected_revision < 1 || validate_live_key_id(key_id).is_err() {
        return Err(LiveKeyAdminError::InvalidInput);
    }
    Ok(())
}

async fn assert_actor_in_transaction(
    transaction: &mut Transaction<'_, Any>,
    home_id: Uuid,
    actor: &ActorSnapshot,
) -> Result<(), LiveKeyAdminError> {
    let role: Option<String> = sqlx::query_scalar(
        "SELECT role FROM home_members
         WHERE home_id = $1 AND user_id = $2 AND status = 'active' LIMIT 1",
    )
    .bind(home_id.to_string())
    .bind(actor.actor_user_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?;
    if role.as_deref() != Some(actor.home_role.as_str()) {
        return Err(LiveKeyAdminError::Forbidden);
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum LiveKeyAdminError {
    #[error("invalid Live key rotation input")]
    InvalidInput,
    #[error("Live key rotation actor is forbidden")]
    Forbidden,
    #[error("Live key rotation revision changed")]
    RevisionChanged,
    #[error("Live key ID is not configured")]
    KeyNotConfigured,
    #[error("Live key rotation capacity was exceeded")]
    CapacityExceeded,
    #[error("invalid persisted Live key rotation state")]
    InvalidState,
    #[error("Live key rotation storage failed")]
    Storage(#[from] sqlx::Error),
    #[error("Live key rotation cryptography failed")]
    Crypto(#[from] LiveCryptoError),
    #[error("Live key rotation session operation failed")]
    Session(#[from] SessionRepositoryError),
    #[error("Live key rotation audit failed")]
    Audit(#[from] LiveAuditError),
    #[error("Live key rotation secret storage failed")]
    SecretStore(anyhow::Error),
}
