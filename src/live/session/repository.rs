use std::sync::Arc;

use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Any, AnyPool, Row, Transaction, any::AnyRow};
use thiserror::Error;
use uuid::Uuid;

use crate::live::{
    config::LiveSessionLimits,
    crypto::{
        CorrelationHashPurpose, EnvelopeContext, EnvelopePurpose, LiveCrypto, LiveCryptoError,
        LiveDeliveryToken, SecretBytes,
    },
};

use super::RecoveryAction;
use super::types::{
    DeliveryMode, IdempotencyRequest, LiveTrackPreferenceUpdate, LiveTrackPreferences,
    LiveTrackSelection, NewSession, SessionGrant, SessionMutation, SessionOwner, SessionRecord,
    SessionRecoveryFailure, SessionRecoveryReplacement, SessionSecretMaterial, SessionState,
    TerminalReason,
};

const SESSION_TABLE: &str = "live_playback_sessions";
const IDEMPOTENCY_TABLE: &str = "live_session_idempotency";
const IDEMPOTENCY_TTL_SECONDS: i64 = 300;
const TERMINAL_RETENTION_SECONDS: i64 = 7 * 24 * 60 * 60;
const MAX_BATCH: u32 = 10_000;
const MAX_ERROR_CODE_BYTES: usize = 128;
const MAX_ERROR_DETAIL_BYTES: usize = 4_096;
const MAX_REPLAY_BYTES: usize = 16_384;
const CREATE_RETRIES: usize = 8;

#[derive(Debug, Error)]
pub enum SessionRepositoryError {
    #[error("invalid Live session input")]
    InvalidInput,
    #[error("Live session owner is not currently authorized")]
    OwnerUnavailable,
    #[error("Live session capacity is exhausted")]
    Capacity,
    #[error("Live session idempotency key was reused for another request")]
    IdempotencyConflict,
    #[error("Live session was not found")]
    NotFound,
    #[error("Live session revision changed")]
    RevisionChanged,
    #[error("Live control-server fence is stale")]
    FenceLost,
    #[error("invalid Live session state transition")]
    InvalidTransition,
    #[error("Live session has expired")]
    Expired,
    #[error("invalid persisted Live session state")]
    InvalidState,
    #[error("Live session cryptography failed")]
    Crypto(#[from] LiveCryptoError),
    #[error("Live session database operation failed")]
    Storage(#[from] sqlx::Error),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SessionCleanupReport {
    pub expired_sessions: u64,
    pub deleted_idempotency_rows: u64,
    pub purged_terminal_sessions: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CryptoRotationReport {
    pub reencrypted_sessions: u64,
    pub reencrypted_replays: u64,
    pub terminated_for_token_key_rotation: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplayPayload {
    session_id: Uuid,
    token_revision: i64,
    token: String,
}

#[derive(Clone)]
pub struct LiveSessionRepository {
    pool: AnyPool,
    crypto: Arc<LiveCrypto>,
    limits: LiveSessionLimits,
}

impl LiveSessionRepository {
    pub fn new(pool: AnyPool, crypto: Arc<LiveCrypto>, limits: LiveSessionLimits) -> Self {
        Self {
            pool,
            crypto,
            limits,
        }
    }

    pub async fn create(
        &self,
        input: NewSession,
        idempotency: Option<IdempotencyRequest>,
    ) -> Result<SessionGrant, SessionRepositoryError> {
        self.validate_create(&input, idempotency.as_ref())?;
        let idempotency_hashes = idempotency
            .as_ref()
            .map(|request| {
                Ok::<_, LiveCryptoError>((
                    self.crypto.hash_correlation(
                        CorrelationHashPurpose::IdempotencyKey,
                        request.key.expose_secret(),
                    )?,
                    self.crypto.hash_correlation(
                        CorrelationHashPurpose::IdempotencyRequest,
                        request.request_identity.expose_secret(),
                    )?,
                ))
            })
            .transpose()?;

        for attempt in 0..CREATE_RETRIES {
            match self.create_once(&input, idempotency_hashes.as_ref()).await {
                Err(SessionRepositoryError::Storage(error))
                    if attempt + 1 < CREATE_RETRIES && transient_database_conflict(&error) =>
                {
                    tokio::time::sleep(std::time::Duration::from_millis(5 * (attempt as u64 + 1)))
                        .await;
                }
                result => return result,
            }
        }
        unreachable!("bounded session-create retry loop always returns")
    }

    pub async fn lookup_idempotency(
        &self,
        owner: SessionOwner,
        request: &IdempotencyRequest,
        now: DateTime<Utc>,
    ) -> Result<Option<SessionGrant>, SessionRepositoryError> {
        if request.key.is_empty()
            || request.key.len() > 512
            || request.request_identity.is_empty()
            || request.request_identity.len() > 65_536
        {
            return Err(SessionRepositoryError::InvalidInput);
        }
        let key_hash = self.crypto.hash_correlation(
            CorrelationHashPurpose::IdempotencyKey,
            request.key.expose_secret(),
        )?;
        let request_hash = self.crypto.hash_correlation(
            CorrelationHashPurpose::IdempotencyRequest,
            request.request_identity.expose_secret(),
        )?;
        let mut transaction = self.pool.begin().await?;
        let replay = self
            .load_replay_in_transaction(&mut transaction, owner, &key_hash, &request_hash, now)
            .await?;
        transaction.commit().await?;
        Ok(replay)
    }

    async fn create_once(
        &self,
        input: &NewSession,
        idempotency: Option<&(String, String)>,
    ) -> Result<SessionGrant, SessionRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        if let Some((key_hash, request_hash)) = idempotency {
            if let Some(replay) = self
                .load_replay_in_transaction(
                    &mut transaction,
                    input.owner,
                    key_hash,
                    request_hash,
                    input.now,
                )
                .await?
            {
                transaction.commit().await?;
                return Ok(replay);
            }
        }
        self.assert_active_fence_in_transaction(
            &mut transaction,
            input.control_fencing_token,
            input.now,
        )
        .await?;
        self.validate_owner_in_transaction(&mut transaction, input)
            .await?;
        self.enforce_capacity_in_transaction(&mut transaction, input)
            .await?;

        let session_id = Uuid::new_v4();
        let session_id_text = session_id.to_string();
        let token = LiveDeliveryToken::generate()?;
        let token_hash = self.crypto.hash_delivery_token(&token)?;
        let item_key_hash = self.crypto.hash_correlation(
            CorrelationHashPurpose::ItemKey,
            input.item_key.expose_secret(),
        )?;
        let stream_option_key_hash = self.crypto.hash_correlation(
            CorrelationHashPurpose::StreamOptionKey,
            input.stream_option_key.expose_secret(),
        )?;
        let encrypted_item_snapshot = self.crypto.encrypt(
            session_context(
                EnvelopePurpose::ItemSnapshot,
                &session_id_text,
                "encrypted_item_snapshot",
            )?,
            &input.item_snapshot,
        )?;
        let encrypted_descriptor = self.crypto.encrypt(
            session_context(
                EnvelopePurpose::Descriptor,
                &session_id_text,
                "encrypted_descriptor",
            )?,
            &input.descriptor,
        )?;
        let lease_seconds = duration_seconds(self.limits.lease_seconds)?;
        let hard_seconds = duration_seconds(self.limits.max_lifetime_seconds)?;
        let hard_expires_at = input
            .now
            .checked_add_signed(Duration::seconds(hard_seconds))
            .ok_or(SessionRepositoryError::InvalidInput)?;
        let expires_at = input
            .now
            .checked_add_signed(Duration::seconds(lease_seconds))
            .ok_or(SessionRepositoryError::InvalidInput)?
            .min(hard_expires_at);

        sqlx::query(
            "INSERT INTO live_playback_sessions (
                id, user_id, home_id, profile_id, account_session_id, provider_id,
                item_key_hash, stream_option_key_hash, encrypted_item_snapshot,
                delivery_mode, protocol, state, revision, token_revision,
                control_fencing_token, token_hash, encrypted_descriptor,
                source_index, failover_count, refresh_count,
                created_at, last_heartbeat_at, expires_at, hard_expires_at
             ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9,
                $10, $11, 'resolving', 1, 1, $12, $13, $14,
                $15, 0, 0, $16, $17, $18, $19
             )",
        )
        .bind(&session_id_text)
        .bind(input.owner.user_id.to_string())
        .bind(input.owner.home_id.to_string())
        .bind(input.owner.profile_id.to_string())
        .bind(input.owner.account_session_id.to_string())
        .bind(input.owner.provider_id.to_string())
        .bind(item_key_hash)
        .bind(stream_option_key_hash)
        .bind(encrypted_item_snapshot)
        .bind(input.delivery_mode.as_str())
        .bind(input.protocol.as_str())
        .bind(input.control_fencing_token)
        .bind(token_hash.as_str())
        .bind(encrypted_descriptor)
        .bind(input.source_index)
        .bind(input.now.to_rfc3339())
        .bind(input.now.to_rfc3339())
        .bind(expires_at.to_rfc3339())
        .bind(hard_expires_at.to_rfc3339())
        .execute(&mut *transaction)
        .await?;

        if let Some((key_hash, request_hash)) = idempotency {
            let replay_expires_at = input
                .now
                .checked_add_signed(Duration::seconds(IDEMPOTENCY_TTL_SECONDS))
                .ok_or(SessionRepositoryError::InvalidInput)?;
            let encrypted_response = self.encrypt_replay(
                input.owner,
                key_hash,
                &ReplayPayload {
                    session_id,
                    token_revision: 1,
                    token: token.expose_secret().to_string(),
                },
            )?;
            let result = sqlx::query(
                "INSERT INTO live_session_idempotency (
                    user_id, profile_id, idempotency_key_hash, request_hash,
                    session_id, encrypted_response, created_at, expires_at
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                 ON CONFLICT(user_id, profile_id, idempotency_key_hash) DO NOTHING",
            )
            .bind(input.owner.user_id.to_string())
            .bind(input.owner.profile_id.to_string())
            .bind(key_hash)
            .bind(request_hash)
            .bind(&session_id_text)
            .bind(encrypted_response)
            .bind(input.now.to_rfc3339())
            .bind(replay_expires_at.to_rfc3339())
            .execute(&mut *transaction)
            .await?;
            if result.rows_affected() != 1 {
                transaction.rollback().await?;
                return self
                    .load_replay_after_race(input.owner, key_hash, request_hash, input.now)
                    .await;
            }
        }

        transaction.commit().await?;
        let session = self
            .get_owned(input.owner, session_id)
            .await?
            .ok_or(SessionRepositoryError::InvalidState)?;
        Ok(SessionGrant {
            session,
            token,
            replayed: false,
        })
    }

    pub async fn get_owned(
        &self,
        owner: SessionOwner,
        session_id: Uuid,
    ) -> Result<Option<SessionRecord>, SessionRepositoryError> {
        let row = sqlx::query(&owned_session_select())
            .bind(session_id.to_string())
            .bind(owner.user_id.to_string())
            .bind(owner.home_id.to_string())
            .bind(owner.profile_id.to_string())
            .bind(owner.account_session_id.to_string())
            .bind(owner.provider_id.to_string())
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(decode_session).transpose()
    }

    pub async fn get_for_account(
        &self,
        user_id: Uuid,
        home_id: Uuid,
        profile_id: Uuid,
        account_session_id: Uuid,
        session_id: Uuid,
    ) -> Result<Option<SessionRecord>, SessionRepositoryError> {
        let row = sqlx::query(&format!(
            "{} WHERE id = $1 AND user_id = $2 AND home_id = $3 AND profile_id = $4
               AND account_session_id = $5",
            session_projection("SELECT")
        ))
        .bind(session_id.to_string())
        .bind(user_id.to_string())
        .bind(home_id.to_string())
        .bind(profile_id.to_string())
        .bind(account_session_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(decode_session).transpose()
    }

    pub(crate) async fn get_for_home(
        &self,
        home_id: Uuid,
        session_id: Uuid,
    ) -> Result<Option<SessionRecord>, SessionRepositoryError> {
        let row = sqlx::query(&format!(
            "{} WHERE id = $1 AND home_id = $2",
            session_projection("SELECT")
        ))
        .bind(session_id.to_string())
        .bind(home_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(decode_session).transpose()
    }

    pub async fn decrypt_secrets(
        &self,
        owner: SessionOwner,
        session_id: Uuid,
    ) -> Result<SessionSecretMaterial, SessionRepositoryError> {
        let row = sqlx::query(
            "SELECT encrypted_item_snapshot, encrypted_descriptor, state
             FROM live_playback_sessions
             WHERE id = $1 AND user_id = $2 AND home_id = $3 AND profile_id = $4
               AND account_session_id = $5 AND provider_id = $6",
        )
        .bind(session_id.to_string())
        .bind(owner.user_id.to_string())
        .bind(owner.home_id.to_string())
        .bind(owner.profile_id.to_string())
        .bind(owner.account_session_id.to_string())
        .bind(owner.provider_id.to_string())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(SessionRepositoryError::NotFound)?;
        let state = parse_state(&row.try_get::<String, _>("state")?)?;
        if state.is_terminal() {
            return Err(SessionRepositoryError::Expired);
        }
        let session_id_text = session_id.to_string();
        Ok(SessionSecretMaterial {
            item_snapshot: self.crypto.decrypt(
                session_context(
                    EnvelopePurpose::ItemSnapshot,
                    &session_id_text,
                    "encrypted_item_snapshot",
                )?,
                &row.try_get::<String, _>("encrypted_item_snapshot")?,
            )?,
            descriptor: self.crypto.decrypt(
                session_context(
                    EnvelopePurpose::Descriptor,
                    &session_id_text,
                    "encrypted_descriptor",
                )?,
                &row.try_get::<String, _>("encrypted_descriptor")?,
            )?,
        })
    }

    pub async fn verify_delivery_token(
        &self,
        session_id: Uuid,
        presented_token: &str,
        now: DateTime<Utc>,
    ) -> Result<SessionRecord, SessionRepositoryError> {
        let row = sqlx::query(&format!(
            "{} WHERE id = $1
               AND control_fencing_token = (
                   SELECT fencing_token FROM live_control_server_leases
                   WHERE lease_name = 'live-control-v1' AND owner_instance_id IS NOT NULL
                     AND expires_at > $2
               )
               AND EXISTS (
                   SELECT 1 FROM account_sessions
                   WHERE account_sessions.id = live_playback_sessions.account_session_id
                     AND account_sessions.user_id = live_playback_sessions.user_id
                     AND account_sessions.home_id = live_playback_sessions.home_id
                     AND account_sessions.active_profile_id = live_playback_sessions.profile_id
                     AND account_sessions.revoked_at IS NULL
                     AND account_sessions.expires_at > $2
               )",
            session_projection("SELECT")
        ))
        .bind(session_id.to_string())
        .bind(now.to_rfc3339())
        .fetch_optional(&self.pool)
        .await?
        .ok_or(SessionRepositoryError::NotFound)?;
        let token_hash: String = row.try_get("token_hash")?;
        let session = decode_session(&row)?;
        if session.state.is_terminal()
            || session.expires_at <= now
            || session.hard_expires_at <= now
        {
            return Err(SessionRepositoryError::Expired);
        }
        if !self
            .crypto
            .verify_delivery_token(presented_token, &token_hash)
        {
            return Err(SessionRepositoryError::NotFound);
        }
        Ok(session)
    }

    pub async fn list_active(
        &self,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<SessionRecord>, SessionRepositoryError> {
        let limit = bounded_batch(limit)?;
        let rows = sqlx::query(&format!(
            "{} WHERE state NOT IN ('ended', 'expired', 'failed')
               AND hard_expires_at > $1 ORDER BY created_at, id LIMIT $2",
            session_projection("SELECT")
        ))
        .bind(now.to_rfc3339())
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(decode_session).collect()
    }

    pub async fn list_nonterminal(
        &self,
        limit: u32,
    ) -> Result<Vec<SessionRecord>, SessionRepositoryError> {
        let limit = bounded_batch(limit)?;
        let rows = sqlx::query(&format!(
            "{} WHERE state NOT IN ('ended', 'expired', 'failed')
               ORDER BY created_at, id LIMIT $1",
            session_projection("SELECT")
        ))
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(decode_session).collect()
    }

    pub async fn assert_current_fence(
        &self,
        control_fencing_token: i64,
        now: DateTime<Utc>,
    ) -> Result<(), SessionRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        self.assert_active_fence_in_transaction(&mut transaction, control_fencing_token, now)
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn adopt_control_fence(
        &self,
        owner: SessionOwner,
        session_id: Uuid,
        expected_revision: i64,
        previous_fencing_token: i64,
        new_fencing_token: i64,
        now: DateTime<Utc>,
    ) -> Result<SessionMutation, SessionRepositoryError> {
        if previous_fencing_token < 1 || previous_fencing_token >= new_fencing_token {
            return Err(SessionRepositoryError::InvalidInput);
        }
        let mut transaction = self.pool.begin().await?;
        self.assert_active_fence_in_transaction(&mut transaction, new_fencing_token, now)
            .await?;
        let row = self
            .load_owned_in_transaction(&mut transaction, owner, session_id)
            .await?
            .ok_or(SessionRepositoryError::NotFound)?;
        let current = decode_session(&row)?;
        if current.state.is_terminal() {
            return Err(SessionRepositoryError::Expired);
        }
        if current.revision != expected_revision {
            return Err(SessionRepositoryError::RevisionChanged);
        }
        if current.control_fencing_token != previous_fencing_token {
            return Err(SessionRepositoryError::FenceLost);
        }
        let updated = sqlx::query(&format!(
            "UPDATE live_playback_sessions
             SET control_fencing_token = $1, revision = revision + 1, remux_job_id = NULL
             WHERE id = $2 AND revision = $3 AND control_fencing_token = $4
               AND state NOT IN ('ended', 'expired', 'failed')
             RETURNING {}",
            session_columns()
        ))
        .bind(new_fencing_token)
        .bind(session_id.to_string())
        .bind(expected_revision)
        .bind(previous_fencing_token)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(SessionRepositoryError::RevisionChanged)?;
        let mutation = SessionMutation {
            session: decode_session(&updated)?,
            previous_revision: expected_revision,
        };
        transaction.commit().await?;
        Ok(mutation)
    }

    pub async fn transition(
        &self,
        owner: SessionOwner,
        session_id: Uuid,
        expected_revision: i64,
        control_fencing_token: i64,
        next: SessionState,
        now: DateTime<Utc>,
    ) -> Result<SessionMutation, SessionRepositoryError> {
        if expected_revision < 1 || control_fencing_token < 1 || next.is_terminal() {
            return Err(SessionRepositoryError::InvalidInput);
        }
        let mut transaction = self.pool.begin().await?;
        self.assert_active_fence_in_transaction(&mut transaction, control_fencing_token, now)
            .await?;
        let row = self
            .load_owned_in_transaction(&mut transaction, owner, session_id)
            .await?
            .ok_or(SessionRepositoryError::NotFound)?;
        let current = decode_session(&row)?;
        self.validate_mutation(&current, expected_revision, control_fencing_token, now)?;
        if !current.state.can_transition_to(next) {
            return Err(SessionRepositoryError::InvalidTransition);
        }
        let row = sqlx::query(&format!(
            "UPDATE live_playback_sessions
             SET state = $1, revision = revision + 1
             WHERE id = $2 AND revision = $3 AND control_fencing_token = $4
               AND state = $5 AND state NOT IN ('ended', 'expired', 'failed')
             RETURNING {}",
            session_columns()
        ))
        .bind(next.as_str())
        .bind(session_id.to_string())
        .bind(expected_revision)
        .bind(control_fencing_token)
        .bind(current.state.as_str())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(SessionRepositoryError::RevisionChanged)?;
        let mutation = SessionMutation {
            session: decode_session(&row)?,
            previous_revision: expected_revision,
        };
        transaction.commit().await?;
        Ok(mutation)
    }

    pub async fn heartbeat(
        &self,
        owner: SessionOwner,
        session_id: Uuid,
        expected_revision: i64,
        control_fencing_token: i64,
        now: DateTime<Utc>,
    ) -> Result<SessionMutation, SessionRepositoryError> {
        self.heartbeat_with_track_preferences(
            owner,
            session_id,
            expected_revision,
            control_fencing_token,
            now,
            None,
        )
        .await
    }

    pub async fn heartbeat_with_track_preferences(
        &self,
        owner: SessionOwner,
        session_id: Uuid,
        expected_revision: i64,
        control_fencing_token: i64,
        now: DateTime<Utc>,
        track_update: Option<&LiveTrackPreferenceUpdate>,
    ) -> Result<SessionMutation, SessionRepositoryError> {
        if track_update
            .is_some_and(|update| update.is_empty() || !valid_track_preference_update(update))
        {
            return Err(SessionRepositoryError::InvalidInput);
        }
        let mut transaction = self.pool.begin().await?;
        self.assert_active_fence_in_transaction(&mut transaction, control_fencing_token, now)
            .await?;
        let row = self
            .load_owned_in_transaction(&mut transaction, owner, session_id)
            .await?
            .ok_or(SessionRepositoryError::NotFound)?;
        let current = decode_session(&row)?;
        self.validate_mutation(&current, expected_revision, control_fencing_token, now)?;
        let lease_seconds = duration_seconds(self.limits.lease_seconds)?;
        let expires_at = now
            .checked_add_signed(Duration::seconds(lease_seconds))
            .ok_or(SessionRepositoryError::InvalidInput)?
            .min(current.hard_expires_at);
        let row = sqlx::query(&format!(
            "UPDATE live_playback_sessions
             SET last_heartbeat_at = $1, expires_at = $2, revision = revision + 1
             WHERE id = $3 AND revision = $4 AND control_fencing_token = $5
               AND state NOT IN ('ended', 'expired', 'failed')
             RETURNING {}",
            session_columns()
        ))
        .bind(now.to_rfc3339())
        .bind(expires_at.to_rfc3339())
        .bind(session_id.to_string())
        .bind(expected_revision)
        .bind(control_fencing_token)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(SessionRepositoryError::RevisionChanged)?;
        let mutation = SessionMutation {
            session: decode_session(&row)?,
            previous_revision: expected_revision,
        };
        if let Some(update) = track_update {
            let audio = update.audio.as_ref();
            let subtitle = update.subtitle.as_ref();
            sqlx::query(
                "INSERT INTO live_track_preferences
                 (user_id, provider_id,
                  audio_track_id, audio_language, audio_title,
                  subtitle_track_id, subtitle_language, subtitle_title,
                  revision, updated_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 1, $9)
                 ON CONFLICT(user_id, provider_id) DO UPDATE SET
                   audio_track_id = COALESCE(excluded.audio_track_id,
                                             live_track_preferences.audio_track_id),
                   audio_language = CASE WHEN excluded.audio_track_id IS NULL
                     THEN live_track_preferences.audio_language
                     ELSE excluded.audio_language END,
                   audio_title = CASE WHEN excluded.audio_track_id IS NULL
                     THEN live_track_preferences.audio_title
                     ELSE excluded.audio_title END,
                   subtitle_track_id = COALESCE(excluded.subtitle_track_id,
                                                live_track_preferences.subtitle_track_id),
                   subtitle_language = CASE WHEN excluded.subtitle_track_id IS NULL
                     THEN live_track_preferences.subtitle_language
                     ELSE excluded.subtitle_language END,
                   subtitle_title = CASE WHEN excluded.subtitle_track_id IS NULL
                     THEN live_track_preferences.subtitle_title
                     ELSE excluded.subtitle_title END,
                   revision = live_track_preferences.revision + 1,
                   updated_at = excluded.updated_at",
            )
            .bind(owner.user_id.to_string())
            .bind(owner.provider_id.to_string())
            .bind(audio.map(|selection| selection.track_id.as_str()))
            .bind(audio.and_then(|selection| selection.language.as_deref()))
            .bind(audio.and_then(|selection| selection.title.as_deref()))
            .bind(subtitle.map(|selection| selection.track_id.as_str()))
            .bind(subtitle.and_then(|selection| selection.language.as_deref()))
            .bind(subtitle.and_then(|selection| selection.title.as_deref()))
            .bind(now.to_rfc3339())
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(mutation)
    }

    pub async fn track_preferences(
        &self,
        user_id: Uuid,
        provider_id: Uuid,
    ) -> Result<Option<LiveTrackPreferences>, SessionRepositoryError> {
        let row = sqlx::query(
            "SELECT audio_track_id, audio_language, audio_title,
                    subtitle_track_id, subtitle_language, subtitle_title,
                    revision, CAST(updated_at AS TEXT) AS updated_at
             FROM live_track_preferences
             WHERE user_id = $1 AND provider_id = $2",
        )
        .bind(user_id.to_string())
        .bind(provider_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        row.as_ref().map(decode_track_preferences).transpose()
    }

    pub async fn rotate_delivery_token(
        &self,
        owner: SessionOwner,
        session_id: Uuid,
        expected_revision: i64,
        control_fencing_token: i64,
        now: DateTime<Utc>,
    ) -> Result<SessionGrant, SessionRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        self.assert_active_fence_in_transaction(&mut transaction, control_fencing_token, now)
            .await?;
        let row = self
            .load_owned_in_transaction(&mut transaction, owner, session_id)
            .await?
            .ok_or(SessionRepositoryError::NotFound)?;
        let current = decode_session(&row)?;
        self.validate_mutation(&current, expected_revision, control_fencing_token, now)?;
        let (token, token_hash, next_token_revision) = self
            .prepare_rotated_token(
                &mut transaction,
                owner,
                session_id,
                current.token_revision,
                now,
            )
            .await?;
        let updated = sqlx::query(&format!(
            "UPDATE live_playback_sessions
             SET token_hash = $1, token_revision = $2, revision = revision + 1
             WHERE id = $3 AND revision = $4 AND control_fencing_token = $5
               AND state NOT IN ('ended', 'expired', 'failed')
             RETURNING {}",
            session_columns()
        ))
        .bind(token_hash)
        .bind(next_token_revision)
        .bind(session_id.to_string())
        .bind(expected_revision)
        .bind(control_fencing_token)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(SessionRepositoryError::RevisionChanged)?;
        let session = decode_session(&updated)?;
        transaction.commit().await?;
        Ok(SessionGrant {
            session,
            token,
            replayed: false,
        })
    }

    pub async fn bind_remux_job(
        &self,
        owner: SessionOwner,
        session_id: Uuid,
        expected_revision: i64,
        control_fencing_token: i64,
        job_id: &str,
        now: DateTime<Utc>,
    ) -> Result<SessionRecord, SessionRepositoryError> {
        if expected_revision < 1 || control_fencing_token < 1 || !valid_remux_job_id(job_id) {
            return Err(SessionRepositoryError::InvalidInput);
        }
        let mut transaction = self.pool.begin().await?;
        self.assert_active_fence_in_transaction(&mut transaction, control_fencing_token, now)
            .await?;
        let row = self
            .load_owned_in_transaction(&mut transaction, owner, session_id)
            .await?
            .ok_or(SessionRepositoryError::NotFound)?;
        let current = decode_session(&row)?;
        self.validate_mutation(&current, expected_revision, control_fencing_token, now)?;
        if current.delivery_mode != DeliveryMode::ServerRemux
            || current
                .remux_job_id
                .as_deref()
                .is_some_and(|value| value != job_id)
        {
            return Err(SessionRepositoryError::InvalidState);
        }
        let updated = sqlx::query(&format!(
            "UPDATE live_playback_sessions
             SET remux_job_id = $1
             WHERE id = $2 AND user_id = $3 AND home_id = $4 AND profile_id = $5
               AND account_session_id = $6 AND provider_id = $7 AND revision = $8
               AND control_fencing_token = $9 AND delivery_mode = 'server_remux'
               AND (remux_job_id IS NULL OR remux_job_id = $1)
               AND state NOT IN ('ended', 'expired', 'failed')
             RETURNING {}",
            session_columns()
        ))
        .bind(job_id)
        .bind(session_id.to_string())
        .bind(owner.user_id.to_string())
        .bind(owner.home_id.to_string())
        .bind(owner.profile_id.to_string())
        .bind(owner.account_session_id.to_string())
        .bind(owner.provider_id.to_string())
        .bind(expected_revision)
        .bind(control_fencing_token)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(SessionRepositoryError::RevisionChanged)?;
        let session = decode_session(&updated)?;
        transaction.commit().await?;
        Ok(session)
    }

    pub async fn clear_remux_job(
        &self,
        owner: SessionOwner,
        session_id: Uuid,
        control_fencing_token: i64,
        job_id: &str,
        now: DateTime<Utc>,
    ) -> Result<(), SessionRepositoryError> {
        if control_fencing_token < 1 || !valid_remux_job_id(job_id) {
            return Err(SessionRepositoryError::InvalidInput);
        }
        let mut transaction = self.pool.begin().await?;
        self.assert_active_fence_in_transaction(&mut transaction, control_fencing_token, now)
            .await?;
        sqlx::query(
            "UPDATE live_playback_sessions SET remux_job_id = NULL
             WHERE id = $1 AND user_id = $2 AND home_id = $3 AND profile_id = $4
               AND account_session_id = $5 AND provider_id = $6
               AND control_fencing_token = $7 AND remux_job_id = $8",
        )
        .bind(session_id.to_string())
        .bind(owner.user_id.to_string())
        .bind(owner.home_id.to_string())
        .bind(owner.profile_id.to_string())
        .bind(owner.account_session_id.to_string())
        .bind(owner.provider_id.to_string())
        .bind(control_fencing_token)
        .bind(job_id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn replace_for_recovery(
        &self,
        input: SessionRecoveryReplacement,
    ) -> Result<SessionGrant, SessionRepositoryError> {
        if input.expected_revision < 1
            || input.control_fencing_token < 1
            || input.descriptor.is_empty()
            || input.source_index < 0
        {
            return Err(SessionRepositoryError::InvalidInput);
        }
        let mut transaction = self.pool.begin().await?;
        self.assert_active_fence_in_transaction(
            &mut transaction,
            input.control_fencing_token,
            input.now,
        )
        .await?;
        let row = self
            .load_owned_in_transaction(&mut transaction, input.owner, input.session_id)
            .await?
            .ok_or(SessionRepositoryError::NotFound)?;
        let current = decode_session(&row)?;
        self.validate_mutation(
            &current,
            input.expected_revision,
            input.control_fencing_token,
            input.now,
        )?;
        let recovery_state = recovery_state(input.action);
        if !current.state.can_transition_to(recovery_state) {
            return Err(SessionRepositoryError::InvalidTransition);
        }
        let mut revision = transition_in_transaction(
            &mut transaction,
            input.session_id,
            input.expected_revision,
            input.control_fencing_token,
            current.state,
            recovery_state,
        )
        .await?;
        let mut before_ready = recovery_state;
        if input.action == RecoveryAction::Failover {
            revision = transition_in_transaction(
                &mut transaction,
                input.session_id,
                revision,
                input.control_fencing_token,
                SessionState::FailingOver,
                SessionState::Planning,
            )
            .await?;
            before_ready = SessionState::Planning;
        }
        if !before_ready.can_transition_to(SessionState::Ready) {
            return Err(SessionRepositoryError::InvalidTransition);
        }

        let session_id_text = input.session_id.to_string();
        let encrypted_descriptor = self.crypto.encrypt(
            session_context(
                EnvelopePurpose::Descriptor,
                &session_id_text,
                "encrypted_descriptor",
            )?,
            &input.descriptor,
        )?;
        let (token, token_hash, token_revision) = self
            .prepare_rotated_token(
                &mut transaction,
                input.owner,
                input.session_id,
                current.token_revision,
                input.now,
            )
            .await?;
        let (refresh_increment, failover_increment) = match input.action {
            RecoveryAction::Refresh => (1_i64, 0_i64),
            RecoveryAction::Failover => (0_i64, 1_i64),
        };
        let updated = sqlx::query(&format!(
            "UPDATE live_playback_sessions
             SET state = 'ready', revision = revision + 1,
                 token_hash = $1, token_revision = $2, encrypted_descriptor = $3,
                 delivery_mode = $4, protocol = $5, source_index = $6,
                 refresh_count = refresh_count + $7,
                 failover_count = failover_count + $8, remux_job_id = NULL
             WHERE id = $9 AND revision = $10 AND control_fencing_token = $11
               AND state = $12 AND state NOT IN ('ended', 'expired', 'failed')
             RETURNING {}",
            session_columns()
        ))
        .bind(token_hash)
        .bind(token_revision)
        .bind(encrypted_descriptor)
        .bind(input.delivery_mode.as_str())
        .bind(input.protocol.as_str())
        .bind(input.source_index)
        .bind(refresh_increment)
        .bind(failover_increment)
        .bind(&session_id_text)
        .bind(revision)
        .bind(input.control_fencing_token)
        .bind(before_ready.as_str())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(SessionRepositoryError::RevisionChanged)?;
        let session = decode_session(&updated)?;
        transaction.commit().await?;
        Ok(SessionGrant {
            session,
            token,
            replayed: false,
        })
    }

    pub async fn record_recovery_failure(
        &self,
        input: SessionRecoveryFailure,
    ) -> Result<SessionMutation, SessionRepositoryError> {
        if input.expected_revision < 1
            || input.control_fencing_token < 1
            || input.descriptor.is_empty()
        {
            return Err(SessionRepositoryError::InvalidInput);
        }
        let mut transaction = self.pool.begin().await?;
        self.assert_active_fence_in_transaction(
            &mut transaction,
            input.control_fencing_token,
            input.now,
        )
        .await?;
        let row = self
            .load_owned_in_transaction(&mut transaction, input.owner, input.session_id)
            .await?
            .ok_or(SessionRepositoryError::NotFound)?;
        let current = decode_session(&row)?;
        self.validate_mutation(
            &current,
            input.expected_revision,
            input.control_fencing_token,
            input.now,
        )?;
        let recovery_state = recovery_state(input.action);
        if !current.state.can_transition_to(recovery_state) {
            return Err(SessionRepositoryError::InvalidTransition);
        }
        let revision = transition_in_transaction(
            &mut transaction,
            input.session_id,
            input.expected_revision,
            input.control_fencing_token,
            current.state,
            recovery_state,
        )
        .await?;
        let resume_state = if current.state == SessionState::Playing {
            SessionState::Playing
        } else {
            SessionState::Ready
        };
        if !recovery_state.can_transition_to(resume_state) {
            return Err(SessionRepositoryError::InvalidTransition);
        }
        let session_id_text = input.session_id.to_string();
        let encrypted_descriptor = self.crypto.encrypt(
            session_context(
                EnvelopePurpose::Descriptor,
                &session_id_text,
                "encrypted_descriptor",
            )?,
            &input.descriptor,
        )?;
        let (refresh_increment, failover_increment) = match input.action {
            RecoveryAction::Refresh => (1_i64, 0_i64),
            RecoveryAction::Failover => (0_i64, 1_i64),
        };
        let updated = sqlx::query(&format!(
            "UPDATE live_playback_sessions
             SET state = $1, revision = revision + 1, encrypted_descriptor = $2,
                 refresh_count = refresh_count + $3,
                 failover_count = failover_count + $4
             WHERE id = $5 AND revision = $6 AND control_fencing_token = $7
               AND state = $8 AND state NOT IN ('ended', 'expired', 'failed')
             RETURNING {}",
            session_columns()
        ))
        .bind(resume_state.as_str())
        .bind(encrypted_descriptor)
        .bind(refresh_increment)
        .bind(failover_increment)
        .bind(&session_id_text)
        .bind(revision)
        .bind(input.control_fencing_token)
        .bind(recovery_state.as_str())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(SessionRepositoryError::RevisionChanged)?;
        let mutation = SessionMutation {
            session: decode_session(&updated)?,
            previous_revision: input.expected_revision,
        };
        transaction.commit().await?;
        Ok(mutation)
    }

    pub async fn terminate(
        &self,
        owner: SessionOwner,
        session_id: Uuid,
        expected_revision: i64,
        control_fencing_token: i64,
        reason: TerminalReason,
        now: DateTime<Utc>,
    ) -> Result<SessionMutation, SessionRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let mutation = self
            .terminate_in_transaction(
                &mut transaction,
                owner,
                session_id,
                expected_revision,
                control_fencing_token,
                reason,
                now,
            )
            .await?;
        transaction.commit().await?;
        Ok(mutation)
    }

    pub(crate) async fn terminate_in_transaction(
        &self,
        transaction: &mut Transaction<'_, Any>,
        owner: SessionOwner,
        session_id: Uuid,
        expected_revision: i64,
        control_fencing_token: i64,
        reason: TerminalReason,
        now: DateTime<Utc>,
    ) -> Result<SessionMutation, SessionRepositoryError> {
        validate_terminal_reason(&reason)?;
        self.assert_active_fence_in_transaction(transaction, control_fencing_token, now)
            .await?;
        let row = self
            .load_owned_in_transaction(transaction, owner, session_id)
            .await?
            .ok_or(SessionRepositoryError::NotFound)?;
        let current = decode_session(&row)?;
        if current.state.is_terminal() {
            let previous_revision = current.revision;
            sqlx::query("DELETE FROM live_session_idempotency WHERE session_id = $1")
                .bind(session_id.to_string())
                .execute(&mut **transaction)
                .await?;
            return Ok(SessionMutation {
                session: current,
                previous_revision,
            });
        }
        self.validate_revision_and_fence(&current, expected_revision, control_fencing_token)?;
        let session_id_text = session_id.to_string();
        let empty = SecretBytes::new(Vec::new());
        let snapshot_tombstone = self.crypto.encrypt(
            session_context(
                EnvelopePurpose::ItemSnapshot,
                &session_id_text,
                "encrypted_item_snapshot",
            )?,
            &empty,
        )?;
        let descriptor_tombstone = self.crypto.encrypt(
            session_context(
                EnvelopePurpose::Descriptor,
                &session_id_text,
                "encrypted_descriptor",
            )?,
            &empty,
        )?;
        let discarded_token = LiveDeliveryToken::generate()?;
        let discarded_hash = self.crypto.hash_delivery_token(&discarded_token)?;
        let updated = sqlx::query(&format!(
            "UPDATE live_playback_sessions
             SET state = $1, revision = revision + 1, token_revision = token_revision + 1,
                 token_hash = $2, encrypted_item_snapshot = $3, encrypted_descriptor = $4,
                 egress_binding_id = NULL, remux_job_id = NULL, ended_at = $5,
                 expires_at = CASE
                     WHEN hard_expires_at < $5 THEN hard_expires_at ELSE $5
                 END,
                 error_code = $6, error_detail_redacted = $7
             WHERE id = $8 AND revision = $9 AND control_fencing_token = $10
               AND state NOT IN ('ended', 'expired', 'failed')
             RETURNING {}",
            session_columns()
        ))
        .bind(reason.state.as_str())
        .bind(discarded_hash.as_str())
        .bind(snapshot_tombstone)
        .bind(descriptor_tombstone)
        .bind(now.to_rfc3339())
        .bind(reason.error_code)
        .bind(reason.error_detail_redacted)
        .bind(&session_id_text)
        .bind(expected_revision)
        .bind(control_fencing_token)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(SessionRepositoryError::RevisionChanged)?;
        sqlx::query("DELETE FROM live_session_idempotency WHERE session_id = $1")
            .bind(&session_id_text)
            .execute(&mut **transaction)
            .await?;
        let session = decode_session(&updated)?;
        Ok(SessionMutation {
            session,
            previous_revision: expected_revision,
        })
    }

    pub async fn cleanup(
        &self,
        now: DateTime<Utc>,
        control_fencing_token: i64,
        limit: u32,
    ) -> Result<SessionCleanupReport, SessionRepositoryError> {
        let limit = bounded_batch(limit)?;
        if control_fencing_token < 1 {
            return Err(SessionRepositoryError::InvalidInput);
        }
        let due_rows = sqlx::query(&format!(
            "{} WHERE state NOT IN ('ended', 'expired', 'failed')
               AND (expires_at <= $1 OR hard_expires_at <= $1)
               AND control_fencing_token = $2
             ORDER BY expires_at, id LIMIT $3",
            session_projection("SELECT")
        ))
        .bind(now.to_rfc3339())
        .bind(control_fencing_token)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        let mut expired_sessions = 0;
        for row in due_rows {
            let session = decode_session(&row)?;
            if self
                .terminate(
                    session.owner,
                    session.id,
                    session.revision,
                    control_fencing_token,
                    TerminalReason {
                        state: SessionState::Expired,
                        error_code: Some("LIVE_SESSION_EXPIRED".to_string()),
                        error_detail_redacted: None,
                    },
                    now,
                )
                .await
                .is_ok()
            {
                expired_sessions += 1;
            }
        }
        let mut transaction = self.pool.begin().await?;
        self.assert_active_fence_in_transaction(&mut transaction, control_fencing_token, now)
            .await?;
        let deleted_idempotency_rows = sqlx::query(
            "DELETE FROM live_session_idempotency
             WHERE (user_id, profile_id, idempotency_key_hash) IN (
                 SELECT user_id, profile_id, idempotency_key_hash
                 FROM live_session_idempotency
                 WHERE expires_at <= $1 ORDER BY expires_at LIMIT $2
             )",
        )
        .bind(now.to_rfc3339())
        .bind(i64::from(limit))
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        let terminal_cutoff = now - Duration::seconds(TERMINAL_RETENTION_SECONDS);
        let purged_terminal_sessions = sqlx::query(
            "DELETE FROM live_playback_sessions
             WHERE id IN (
                 SELECT id FROM live_playback_sessions
                 WHERE state IN ('ended', 'expired', 'failed') AND ended_at <= $1
                 ORDER BY ended_at, id LIMIT $2
             )",
        )
        .bind(terminal_cutoff.to_rfc3339())
        .bind(i64::from(limit))
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        transaction.commit().await?;
        Ok(SessionCleanupReport {
            expired_sessions,
            deleted_idempotency_rows,
            purged_terminal_sessions,
        })
    }

    pub async fn rotate_encryption_keys(
        &self,
        now: DateTime<Utc>,
        control_fencing_token: i64,
        limit: u32,
    ) -> Result<CryptoRotationReport, SessionRepositoryError> {
        let limit = bounded_batch(limit)?;
        let rows = sqlx::query(&format!(
            "SELECT {}, encrypted_item_snapshot, encrypted_descriptor
             FROM live_playback_sessions
             WHERE state NOT IN ('ended', 'expired', 'failed')
               AND hard_expires_at > $1 ORDER BY created_at, id LIMIT $2",
            session_columns()
        ))
        .bind(now.to_rfc3339())
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        let mut report = CryptoRotationReport::default();
        let token_hash_primary_key_id = self.crypto.token_hash_primary_key_id()?;
        for row in rows {
            let session = decode_session(&row)?;
            let token_hash: String = row.try_get("token_hash")?;
            if self.crypto.token_hash_key_id(&token_hash)
                != Some(token_hash_primary_key_id.as_str())
            {
                if self
                    .terminate(
                        session.owner,
                        session.id,
                        session.revision,
                        control_fencing_token,
                        TerminalReason {
                            state: SessionState::Failed,
                            error_code: Some("LIVE_TOKEN_KEY_ROTATED".to_string()),
                            error_detail_redacted: None,
                        },
                        now,
                    )
                    .await
                    .is_ok()
                {
                    report.terminated_for_token_key_rotation += 1;
                }
                continue;
            }
            let (session_changed, replay_count) = self
                .reencrypt_session_values(session, &row, now, control_fencing_token)
                .await?;
            report.reencrypted_sessions += u64::from(session_changed);
            report.reencrypted_replays += replay_count;
        }
        Ok(report)
    }

    pub async fn reencrypt_active_envelopes(
        &self,
        now: DateTime<Utc>,
        control_fencing_token: i64,
        limit: u32,
    ) -> Result<CryptoRotationReport, SessionRepositoryError> {
        let limit = bounded_batch(limit)?;
        self.assert_current_fence(control_fencing_token, now)
            .await?;
        let primary_key_id = self.crypto.primary_key_id()?;
        let primary_pattern = format!("elx-live:v1:{primary_key_id}:%");
        let rows = sqlx::query(&format!(
            "SELECT {}, encrypted_item_snapshot, encrypted_descriptor
             FROM live_playback_sessions AS sessions
             WHERE state NOT IN ('ended', 'expired', 'failed')
               AND hard_expires_at > $1
               AND (
                   encrypted_item_snapshot NOT LIKE $2
                   OR encrypted_descriptor NOT LIKE $2
                   OR EXISTS (
                       SELECT 1 FROM live_session_idempotency AS replay
                       WHERE replay.session_id = sessions.id
                         AND replay.expires_at > $1
                         AND replay.encrypted_response NOT LIKE $2
                   )
               )
             ORDER BY created_at, id LIMIT $3",
            session_columns()
        ))
        .bind(now.to_rfc3339())
        .bind(primary_pattern)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        let mut report = CryptoRotationReport::default();
        for row in rows {
            let session = decode_session(&row)?;
            let (session_changed, replay_count) = self
                .reencrypt_session_values(session, &row, now, control_fencing_token)
                .await?;
            report.reencrypted_sessions += u64::from(session_changed);
            report.reencrypted_replays += replay_count;
        }
        Ok(report)
    }

    pub async fn count_active_envelopes_not_using(
        &self,
        key_id: &str,
        now: DateTime<Utc>,
    ) -> Result<u64, SessionRepositoryError> {
        crate::live::crypto::validate_live_key_id(key_id)?;
        let primary_pattern = format!("elx-live:v1:{key_id}:%");
        let count: i64 = sqlx::query_scalar(
            "SELECT
                (SELECT COUNT(*) FROM live_playback_sessions
                 WHERE state NOT IN ('ended', 'expired', 'failed')
                   AND hard_expires_at > $1
                   AND (encrypted_item_snapshot NOT LIKE $2
                        OR encrypted_descriptor NOT LIKE $2))
                +
                (SELECT COUNT(*) FROM live_session_idempotency AS replay
                 JOIN live_playback_sessions AS sessions ON sessions.id = replay.session_id
                 WHERE sessions.state NOT IN ('ended', 'expired', 'failed')
                   AND sessions.hard_expires_at > $1 AND replay.expires_at > $1
                   AND replay.encrypted_response NOT LIKE $2)",
        )
        .bind(now.to_rfc3339())
        .bind(primary_pattern)
        .fetch_one(&self.pool)
        .await?;
        u64::try_from(count).map_err(|_| SessionRepositoryError::InvalidState)
    }

    pub async fn terminate_server_delivery_for_token_rotation(
        &self,
        now: DateTime<Utc>,
        control_fencing_token: i64,
        limit: u32,
    ) -> Result<u64, SessionRepositoryError> {
        let limit = bounded_batch(limit)?;
        let rows = sqlx::query(&format!(
            "{} WHERE state NOT IN ('ended', 'expired', 'failed')
               AND delivery_mode IN ('server_relay', 'server_remux')
             ORDER BY created_at, id LIMIT $1",
            session_projection("SELECT")
        ))
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        let mut terminated = 0_u64;
        for row in rows {
            let session = decode_session(&row)?;
            self.terminate(
                session.owner,
                session.id,
                session.revision,
                control_fencing_token,
                TerminalReason {
                    state: SessionState::Failed,
                    error_code: Some("LIVE_TOKEN_KEY_ROTATED".to_string()),
                    error_detail_redacted: None,
                },
                now,
            )
            .await?;
            terminated = terminated
                .checked_add(1)
                .ok_or(SessionRepositoryError::InvalidState)?;
        }
        Ok(terminated)
    }

    pub async fn count_active_server_delivery(&self) -> Result<u64, SessionRepositoryError> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM live_playback_sessions
             WHERE state NOT IN ('ended', 'expired', 'failed')
               AND delivery_mode IN ('server_relay', 'server_remux')",
        )
        .fetch_one(&self.pool)
        .await?;
        u64::try_from(count).map_err(|_| SessionRepositoryError::InvalidState)
    }

    async fn reencrypt_session_values(
        &self,
        session: SessionRecord,
        selected_row: &AnyRow,
        now: DateTime<Utc>,
        control_fencing_token: i64,
    ) -> Result<(bool, u64), SessionRepositoryError> {
        let session_id = session.id.to_string();
        let old_snapshot: String = selected_row.try_get("encrypted_item_snapshot")?;
        let old_descriptor: String = selected_row.try_get("encrypted_descriptor")?;
        let new_snapshot = self.crypto.reencrypt(
            session_context(
                EnvelopePurpose::ItemSnapshot,
                &session_id,
                "encrypted_item_snapshot",
            )?,
            &old_snapshot,
        )?;
        let new_descriptor = self.crypto.reencrypt(
            session_context(
                EnvelopePurpose::Descriptor,
                &session_id,
                "encrypted_descriptor",
            )?,
            &old_descriptor,
        )?;
        let session_changed = new_snapshot != old_snapshot || new_descriptor != old_descriptor;
        let mut transaction = self.pool.begin().await?;
        self.assert_active_fence_in_transaction(&mut transaction, control_fencing_token, now)
            .await?;
        if session.control_fencing_token != control_fencing_token {
            return Err(SessionRepositoryError::FenceLost);
        }
        let replay_rows = sqlx::query(
            "SELECT user_id, profile_id, idempotency_key_hash, encrypted_response
             FROM live_session_idempotency
             WHERE session_id = $1 AND expires_at > $2",
        )
        .bind(&session_id)
        .bind(now.to_rfc3339())
        .fetch_all(&mut *transaction)
        .await?;
        let mut replay_count = 0;
        for replay in replay_rows {
            let replay_owner = SessionOwner {
                user_id: parse_uuid(&replay.try_get::<String, _>("user_id")?)?,
                profile_id: parse_uuid(&replay.try_get::<String, _>("profile_id")?)?,
                ..session.owner
            };
            let key_hash: String = replay.try_get("idempotency_key_hash")?;
            let old_envelope: String = replay.try_get("encrypted_response")?;
            let record_id = replay_record_id(replay_owner, &key_hash);
            let new_envelope = self
                .crypto
                .reencrypt(replay_context(&record_id)?, &old_envelope)?;
            if new_envelope != old_envelope {
                let result = sqlx::query(
                    "UPDATE live_session_idempotency SET encrypted_response = $1
                     WHERE user_id = $2 AND profile_id = $3 AND idempotency_key_hash = $4
                       AND session_id = $5 AND encrypted_response = $6",
                )
                .bind(new_envelope)
                .bind(replay_owner.user_id.to_string())
                .bind(replay_owner.profile_id.to_string())
                .bind(&key_hash)
                .bind(&session_id)
                .bind(old_envelope)
                .execute(&mut *transaction)
                .await?;
                replay_count += result.rows_affected();
            }
        }
        if session_changed {
            let result = sqlx::query(
                "UPDATE live_playback_sessions
                 SET encrypted_item_snapshot = $1, encrypted_descriptor = $2,
                     revision = revision + 1
                 WHERE id = $3 AND revision = $4 AND control_fencing_token = $5
                   AND state NOT IN ('ended', 'expired', 'failed')",
            )
            .bind(new_snapshot)
            .bind(new_descriptor)
            .bind(&session_id)
            .bind(session.revision)
            .bind(control_fencing_token)
            .execute(&mut *transaction)
            .await?;
            if result.rows_affected() != 1 {
                return Err(SessionRepositoryError::RevisionChanged);
            }
        }
        transaction.commit().await?;
        Ok((session_changed, replay_count))
    }

    async fn prepare_rotated_token(
        &self,
        transaction: &mut Transaction<'_, Any>,
        owner: SessionOwner,
        session_id: Uuid,
        current_token_revision: i64,
        now: DateTime<Utc>,
    ) -> Result<(LiveDeliveryToken, String, i64), SessionRepositoryError> {
        let token = LiveDeliveryToken::generate()?;
        let token_hash = self.crypto.hash_delivery_token(&token)?;
        let next_token_revision = current_token_revision
            .checked_add(1)
            .ok_or(SessionRepositoryError::InvalidState)?;
        let replay_rows = sqlx::query(
            "SELECT user_id, profile_id, idempotency_key_hash
             FROM live_session_idempotency
             WHERE session_id = $1 AND expires_at > $2",
        )
        .bind(session_id.to_string())
        .bind(now.to_rfc3339())
        .fetch_all(&mut **transaction)
        .await?;
        for replay_row in replay_rows {
            let replay_owner = SessionOwner {
                user_id: parse_uuid(&replay_row.try_get::<String, _>("user_id")?)?,
                profile_id: parse_uuid(&replay_row.try_get::<String, _>("profile_id")?)?,
                ..owner
            };
            let key_hash: String = replay_row.try_get("idempotency_key_hash")?;
            let encrypted = self.encrypt_replay(
                replay_owner,
                &key_hash,
                &ReplayPayload {
                    session_id,
                    token_revision: next_token_revision,
                    token: token.expose_secret().to_string(),
                },
            )?;
            let updated = sqlx::query(
                "UPDATE live_session_idempotency
                 SET encrypted_response = $1
                 WHERE user_id = $2 AND profile_id = $3 AND idempotency_key_hash = $4
                   AND session_id = $5 AND expires_at > $6",
            )
            .bind(encrypted)
            .bind(replay_owner.user_id.to_string())
            .bind(replay_owner.profile_id.to_string())
            .bind(key_hash)
            .bind(session_id.to_string())
            .bind(now.to_rfc3339())
            .execute(&mut **transaction)
            .await?;
            if updated.rows_affected() != 1 {
                return Err(SessionRepositoryError::RevisionChanged);
            }
        }
        Ok((token, token_hash.as_str().to_string(), next_token_revision))
    }

    async fn validate_owner_in_transaction(
        &self,
        transaction: &mut Transaction<'_, Any>,
        input: &NewSession,
    ) -> Result<(), SessionRepositoryError> {
        let valid: Option<i64> = sqlx::query_scalar(
            "SELECT 1
             FROM account_sessions AS sessions
             JOIN users ON users.id = sessions.user_id
             JOIN homes ON homes.id = sessions.home_id
             JOIN home_members AS membership
               ON membership.home_id = sessions.home_id
              AND membership.user_id = sessions.user_id
              AND membership.status = 'active'
             JOIN profiles ON profiles.id = sessions.active_profile_id
             JOIN providers ON providers.provider_id = $5
             WHERE sessions.id = $1 AND sessions.user_id = $2 AND sessions.home_id = $3
               AND sessions.active_profile_id = $4 AND sessions.revoked_at IS NULL
               AND sessions.expires_at > $6 AND profiles.home_id = sessions.home_id
             LIMIT 1",
        )
        .bind(input.owner.account_session_id.to_string())
        .bind(input.owner.user_id.to_string())
        .bind(input.owner.home_id.to_string())
        .bind(input.owner.profile_id.to_string())
        .bind(input.owner.provider_id.to_string())
        .bind(input.now.to_rfc3339())
        .fetch_optional(&mut **transaction)
        .await?;
        if valid.is_none() {
            return Err(SessionRepositoryError::OwnerUnavailable);
        }
        Ok(())
    }

    async fn assert_active_fence_in_transaction(
        &self,
        transaction: &mut Transaction<'_, Any>,
        control_fencing_token: i64,
        now: DateTime<Utc>,
    ) -> Result<(), SessionRepositoryError> {
        if control_fencing_token < 1 {
            return Err(SessionRepositoryError::InvalidInput);
        }
        let result = sqlx::query(
            "UPDATE live_control_server_leases
             SET heartbeat_at = heartbeat_at
             WHERE lease_name = 'live-control-v1' AND fencing_token = $1
               AND owner_instance_id IS NOT NULL AND expires_at > $2",
        )
        .bind(control_fencing_token)
        .bind(now.to_rfc3339())
        .execute(&mut **transaction)
        .await?;
        if result.rows_affected() != 1 {
            return Err(SessionRepositoryError::FenceLost);
        }
        Ok(())
    }

    async fn enforce_capacity_in_transaction(
        &self,
        transaction: &mut Transaction<'_, Any>,
        input: &NewSession,
    ) -> Result<(), SessionRepositoryError> {
        let row = sqlx::query(
            "SELECT
                COUNT(*) AS server_count,
                SUM(CASE WHEN user_id = $1 THEN 1 ELSE 0 END) AS user_count
             FROM live_playback_sessions
             WHERE state NOT IN ('ended', 'expired', 'failed')
               AND expires_at > $2 AND hard_expires_at > $2",
        )
        .bind(input.owner.user_id.to_string())
        .bind(input.now.to_rfc3339())
        .fetch_one(&mut **transaction)
        .await?;
        let server_count: i64 = row.try_get("server_count")?;
        let user_count: i64 = row.try_get::<Option<i64>, _>("user_count")?.unwrap_or(0);
        if server_count >= i64::from(self.limits.server_total)
            || user_count >= i64::from(self.limits.per_user)
        {
            return Err(SessionRepositoryError::Capacity);
        }
        Ok(())
    }

    async fn load_owned_in_transaction(
        &self,
        transaction: &mut Transaction<'_, Any>,
        owner: SessionOwner,
        session_id: Uuid,
    ) -> Result<Option<AnyRow>, SessionRepositoryError> {
        Ok(sqlx::query(&owned_session_select())
            .bind(session_id.to_string())
            .bind(owner.user_id.to_string())
            .bind(owner.home_id.to_string())
            .bind(owner.profile_id.to_string())
            .bind(owner.account_session_id.to_string())
            .bind(owner.provider_id.to_string())
            .fetch_optional(&mut **transaction)
            .await?)
    }

    async fn load_replay_in_transaction(
        &self,
        transaction: &mut Transaction<'_, Any>,
        owner: SessionOwner,
        key_hash: &str,
        request_hash: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<SessionGrant>, SessionRepositoryError> {
        let row = sqlx::query(
            "SELECT request_hash, session_id, encrypted_response, CAST(expires_at AS TEXT) AS expires_at
             FROM live_session_idempotency
             WHERE user_id = $1 AND profile_id = $2 AND idempotency_key_hash = $3",
        )
        .bind(owner.user_id.to_string())
        .bind(owner.profile_id.to_string())
        .bind(key_hash)
        .fetch_optional(&mut **transaction)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        if row.try_get::<String, _>("request_hash")? != request_hash {
            return Err(SessionRepositoryError::IdempotencyConflict);
        }
        if parse_timestamp(&row.try_get::<String, _>("expires_at")?)? <= now {
            sqlx::query(
                "DELETE FROM live_session_idempotency
                 WHERE user_id = $1 AND profile_id = $2 AND idempotency_key_hash = $3",
            )
            .bind(owner.user_id.to_string())
            .bind(owner.profile_id.to_string())
            .bind(key_hash)
            .execute(&mut **transaction)
            .await?;
            return Ok(None);
        }
        let session_id = parse_uuid(&row.try_get::<String, _>("session_id")?)?;
        let payload = self.decrypt_replay(
            owner,
            key_hash,
            &row.try_get::<String, _>("encrypted_response")?,
        )?;
        if payload.session_id != session_id {
            return Err(SessionRepositoryError::InvalidState);
        }
        let session_row = self
            .load_owned_in_transaction(transaction, owner, session_id)
            .await?
            .ok_or(SessionRepositoryError::InvalidState)?;
        let stored_hash: String = session_row.try_get("token_hash")?;
        let session = decode_session(&session_row)?;
        if session.state.is_terminal()
            || session.expires_at <= now
            || session.hard_expires_at <= now
            || payload.token_revision != session.token_revision
            || !self
                .crypto
                .verify_delivery_token(&payload.token, &stored_hash)
        {
            return Err(SessionRepositoryError::InvalidState);
        }
        Ok(Some(SessionGrant {
            session,
            token: LiveDeliveryToken::parse(payload.token)?,
            replayed: true,
        }))
    }

    async fn load_replay_after_race(
        &self,
        owner: SessionOwner,
        key_hash: &str,
        request_hash: &str,
        now: DateTime<Utc>,
    ) -> Result<SessionGrant, SessionRepositoryError> {
        let mut transaction = self.pool.begin().await?;
        let replay = self
            .load_replay_in_transaction(&mut transaction, owner, key_hash, request_hash, now)
            .await?
            .ok_or(SessionRepositoryError::RevisionChanged)?;
        transaction.commit().await?;
        Ok(replay)
    }

    fn encrypt_replay(
        &self,
        owner: SessionOwner,
        key_hash: &str,
        payload: &ReplayPayload,
    ) -> Result<String, SessionRepositoryError> {
        let encoded =
            serde_json::to_vec(payload).map_err(|_| SessionRepositoryError::InvalidState)?;
        if encoded.len() > MAX_REPLAY_BYTES {
            return Err(SessionRepositoryError::InvalidState);
        }
        let record_id = replay_record_id(owner, key_hash);
        Ok(self
            .crypto
            .encrypt(replay_context(&record_id)?, &SecretBytes::new(encoded))?)
    }

    fn decrypt_replay(
        &self,
        owner: SessionOwner,
        key_hash: &str,
        envelope: &str,
    ) -> Result<ReplayPayload, SessionRepositoryError> {
        let record_id = replay_record_id(owner, key_hash);
        let plaintext = self.crypto.decrypt(replay_context(&record_id)?, envelope)?;
        if plaintext.len() > MAX_REPLAY_BYTES {
            return Err(SessionRepositoryError::InvalidState);
        }
        serde_json::from_slice(plaintext.expose_secret())
            .map_err(|_| SessionRepositoryError::InvalidState)
    }

    fn validate_create(
        &self,
        input: &NewSession,
        idempotency: Option<&IdempotencyRequest>,
    ) -> Result<(), SessionRepositoryError> {
        if input.control_fencing_token < 1
            || input.item_key.is_empty()
            || input.stream_option_key.is_empty()
            || input.item_snapshot.is_empty()
            || input.descriptor.is_empty()
            || input.source_index < 0
            || idempotency.is_some_and(|request| {
                request.key.is_empty()
                    || request.key.len() > 512
                    || request.request_identity.is_empty()
                    || request.request_identity.len() > 65_536
            })
        {
            return Err(SessionRepositoryError::InvalidInput);
        }
        Ok(())
    }

    fn validate_mutation(
        &self,
        session: &SessionRecord,
        expected_revision: i64,
        control_fencing_token: i64,
        now: DateTime<Utc>,
    ) -> Result<(), SessionRepositoryError> {
        self.validate_revision_and_fence(session, expected_revision, control_fencing_token)?;
        if session.state.is_terminal()
            || session.expires_at <= now
            || session.hard_expires_at <= now
        {
            return Err(SessionRepositoryError::Expired);
        }
        Ok(())
    }

    fn validate_revision_and_fence(
        &self,
        session: &SessionRecord,
        expected_revision: i64,
        control_fencing_token: i64,
    ) -> Result<(), SessionRepositoryError> {
        if expected_revision < 1 || control_fencing_token < 1 {
            return Err(SessionRepositoryError::InvalidInput);
        }
        if session.control_fencing_token != control_fencing_token {
            return Err(SessionRepositoryError::FenceLost);
        }
        if session.revision != expected_revision {
            return Err(SessionRepositoryError::RevisionChanged);
        }
        Ok(())
    }
}

fn recovery_state(action: RecoveryAction) -> SessionState {
    match action {
        RecoveryAction::Refresh => SessionState::Refreshing,
        RecoveryAction::Failover => SessionState::FailingOver,
    }
}

async fn transition_in_transaction(
    transaction: &mut Transaction<'_, Any>,
    session_id: Uuid,
    expected_revision: i64,
    control_fencing_token: i64,
    current: SessionState,
    next: SessionState,
) -> Result<i64, SessionRepositoryError> {
    if !current.can_transition_to(next) {
        return Err(SessionRepositoryError::InvalidTransition);
    }
    let next_revision = expected_revision
        .checked_add(1)
        .ok_or(SessionRepositoryError::InvalidState)?;
    let updated = sqlx::query(
        "UPDATE live_playback_sessions
         SET state = $1, revision = revision + 1
         WHERE id = $2 AND revision = $3 AND control_fencing_token = $4
           AND state = $5 AND state NOT IN ('ended', 'expired', 'failed')",
    )
    .bind(next.as_str())
    .bind(session_id.to_string())
    .bind(expected_revision)
    .bind(control_fencing_token)
    .bind(current.as_str())
    .execute(&mut **transaction)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(SessionRepositoryError::RevisionChanged);
    }
    Ok(next_revision)
}

fn session_context<'a>(
    purpose: EnvelopePurpose,
    session_id: &'a str,
    column: &'a str,
) -> Result<EnvelopeContext<'a>, LiveCryptoError> {
    EnvelopeContext::new(purpose, SESSION_TABLE, session_id, column)
}

fn valid_track_preference_update(update: &LiveTrackPreferenceUpdate) -> bool {
    update
        .audio
        .iter()
        .chain(update.subtitle.iter())
        .all(valid_track_selection)
}

fn valid_track_selection(selection: &LiveTrackSelection) -> bool {
    valid_preference_text(&selection.track_id, 256)
        && selection
            .language
            .as_deref()
            .is_none_or(|value| valid_preference_text(value, 64))
        && selection
            .title
            .as_deref()
            .is_none_or(|value| valid_preference_text(value, 256))
}

fn valid_preference_text(value: &str, max_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_bytes
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

fn decode_track_preferences(row: &AnyRow) -> Result<LiveTrackPreferences, SessionRepositoryError> {
    let audio_track_id = row.try_get::<Option<String>, _>("audio_track_id")?;
    let subtitle_track_id = row.try_get::<Option<String>, _>("subtitle_track_id")?;
    let selection = |track_id: Option<String>,
                     language: Option<String>,
                     title: Option<String>|
     -> Result<Option<LiveTrackSelection>, SessionRepositoryError> {
        match track_id {
            Some(track_id) => {
                let value = LiveTrackSelection {
                    track_id,
                    language,
                    title,
                };
                valid_track_selection(&value)
                    .then_some(Some(value))
                    .ok_or(SessionRepositoryError::InvalidState)
            }
            None if language.is_none() && title.is_none() => Ok(None),
            None => Err(SessionRepositoryError::InvalidState),
        }
    };
    let audio = selection(
        audio_track_id,
        row.try_get("audio_language")?,
        row.try_get("audio_title")?,
    )?;
    let subtitle = selection(
        subtitle_track_id,
        row.try_get("subtitle_language")?,
        row.try_get("subtitle_title")?,
    )?;
    if audio.is_none() && subtitle.is_none() {
        return Err(SessionRepositoryError::InvalidState);
    }
    Ok(LiveTrackPreferences {
        audio,
        subtitle,
        revision: positive(row.try_get("revision")?)?,
        updated_at: parse_timestamp(&row.try_get::<String, _>("updated_at")?)?,
    })
}

fn replay_context(record_id: &str) -> Result<EnvelopeContext<'_>, LiveCryptoError> {
    EnvelopeContext::new(
        EnvelopePurpose::IdempotencyResponse,
        IDEMPOTENCY_TABLE,
        record_id,
        "encrypted_response",
    )
}

fn replay_record_id(owner: SessionOwner, key_hash: &str) -> String {
    format!("{}:{}:{key_hash}", owner.user_id, owner.profile_id)
}

fn owned_session_select() -> String {
    format!(
        "{} WHERE id = $1 AND user_id = $2 AND home_id = $3 AND profile_id = $4
           AND account_session_id = $5 AND provider_id = $6",
        session_projection("SELECT")
    )
}

fn session_projection(prefix: &str) -> String {
    format!("{prefix} {} FROM live_playback_sessions", session_columns())
}

fn session_columns() -> &'static str {
    "id, user_id, home_id, profile_id, account_session_id, provider_id,
     delivery_mode, protocol, state, revision, token_revision, control_fencing_token,
     token_hash, source_index, failover_count, refresh_count, egress_binding_id, remux_job_id,
     CAST(created_at AS TEXT) AS created_at,
     CAST(last_heartbeat_at AS TEXT) AS last_heartbeat_at,
     CAST(expires_at AS TEXT) AS expires_at,
     CAST(hard_expires_at AS TEXT) AS hard_expires_at,
     CAST(ended_at AS TEXT) AS ended_at, error_code, error_detail_redacted"
}

fn decode_session(row: &AnyRow) -> Result<SessionRecord, SessionRepositoryError> {
    Ok(SessionRecord {
        id: parse_uuid(&row.try_get::<String, _>("id")?)?,
        owner: SessionOwner {
            user_id: parse_uuid(&row.try_get::<String, _>("user_id")?)?,
            home_id: parse_uuid(&row.try_get::<String, _>("home_id")?)?,
            profile_id: parse_uuid(&row.try_get::<String, _>("profile_id")?)?,
            account_session_id: parse_uuid(&row.try_get::<String, _>("account_session_id")?)?,
            provider_id: parse_uuid(&row.try_get::<String, _>("provider_id")?)?,
        },
        delivery_mode: row
            .try_get::<String, _>("delivery_mode")?
            .as_str()
            .try_into()
            .map_err(|_| SessionRepositoryError::InvalidState)?,
        protocol: row
            .try_get::<String, _>("protocol")?
            .as_str()
            .try_into()
            .map_err(|_| SessionRepositoryError::InvalidState)?,
        state: parse_state(&row.try_get::<String, _>("state")?)?,
        revision: positive(row.try_get("revision")?)?,
        token_revision: positive(row.try_get("token_revision")?)?,
        control_fencing_token: positive(row.try_get("control_fencing_token")?)?,
        source_index: nonnegative_i32(row.try_get("source_index")?)?,
        failover_count: nonnegative_i32(row.try_get("failover_count")?)?,
        refresh_count: nonnegative_i32(row.try_get("refresh_count")?)?,
        egress_binding_id: row
            .try_get::<Option<String>, _>("egress_binding_id")?
            .map(|value| parse_uuid(&value))
            .transpose()?,
        remux_job_id: row.try_get("remux_job_id")?,
        created_at: parse_timestamp(&row.try_get::<String, _>("created_at")?)?,
        last_heartbeat_at: parse_timestamp(&row.try_get::<String, _>("last_heartbeat_at")?)?,
        expires_at: parse_timestamp(&row.try_get::<String, _>("expires_at")?)?,
        hard_expires_at: parse_timestamp(&row.try_get::<String, _>("hard_expires_at")?)?,
        ended_at: optional_timestamp(row.try_get::<Option<String>, _>("ended_at")?)?,
        error_code: row.try_get("error_code")?,
        error_detail_redacted: row.try_get("error_detail_redacted")?,
    })
}

fn parse_state(value: &str) -> Result<SessionState, SessionRepositoryError> {
    value
        .try_into()
        .map_err(|_| SessionRepositoryError::InvalidState)
}

fn parse_uuid(value: &str) -> Result<Uuid, SessionRepositoryError> {
    Uuid::parse_str(value).map_err(|_| SessionRepositoryError::InvalidState)
}

fn parse_timestamp(value: &str) -> Result<DateTime<Utc>, SessionRepositoryError> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .or_else(|_| {
            NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
                .map(|value| value.and_utc())
        })
        .map_err(|_| SessionRepositoryError::InvalidState)
}

fn optional_timestamp(
    value: Option<String>,
) -> Result<Option<DateTime<Utc>>, SessionRepositoryError> {
    value.map(|value| parse_timestamp(&value)).transpose()
}

fn positive(value: i64) -> Result<i64, SessionRepositoryError> {
    (value > 0)
        .then_some(value)
        .ok_or(SessionRepositoryError::InvalidState)
}

fn nonnegative_i32(value: i64) -> Result<i32, SessionRepositoryError> {
    if value < 0 {
        return Err(SessionRepositoryError::InvalidState);
    }
    i32::try_from(value).map_err(|_| SessionRepositoryError::InvalidState)
}

fn duration_seconds(value: u64) -> Result<i64, SessionRepositoryError> {
    i64::try_from(value).map_err(|_| SessionRepositoryError::InvalidInput)
}

fn bounded_batch(limit: u32) -> Result<u32, SessionRepositoryError> {
    if !(1..=MAX_BATCH).contains(&limit) {
        return Err(SessionRepositoryError::InvalidInput);
    }
    Ok(limit)
}

fn valid_remux_job_id(value: &str) -> bool {
    value.strip_prefix("lrj1_").is_some_and(|suffix| {
        suffix.len() == 32 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit())
    })
}

fn validate_terminal_reason(reason: &TerminalReason) -> Result<(), SessionRepositoryError> {
    if !reason.state.is_terminal()
        || reason
            .error_code
            .as_ref()
            .is_some_and(|value| value.is_empty() || value.len() > MAX_ERROR_CODE_BYTES)
        || reason
            .error_detail_redacted
            .as_ref()
            .is_some_and(|value| value.len() > MAX_ERROR_DETAIL_BYTES)
    {
        return Err(SessionRepositoryError::InvalidInput);
    }
    Ok(())
}

fn transient_database_conflict(error: &sqlx::Error) -> bool {
    let text = error.to_string().to_ascii_lowercase();
    text.contains("database is locked")
        || text.contains("database table is locked")
        || text.contains("database is deadlocked")
        || text.contains("serialization failure")
        || text.contains("deadlock detected")
}
