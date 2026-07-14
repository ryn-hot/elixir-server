use std::{collections::HashSet, fmt};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, NaiveDateTime, Utc};
use hmac::{Hmac, Mac};
use rand_core::{OsRng, RngCore};
use sha2::Sha256;
use sqlx::{Any, AnyPool, Transaction};
use thiserror::Error;
use uuid::Uuid;
use zeroize::Zeroize;

use super::{AuthService, VerifiedAccessClaims};
use crate::auth::{
    home_profiles::{HomeRole, ProfileType, ensure_owner_home_in_transaction},
    revocation::{
        NewAuthorizationRevocation, RevocationError, append_authorization_revocation_in_transaction,
    },
};

const REFRESH_TOKEN_PREFIX: &str = "elx_refresh_v1_";
const REFRESH_TOKEN_DOMAIN: &[u8] = b"elixir.auth.refresh-token.v1";
const CSRF_TOKEN_PREFIX: &str = "elx_csrf_v1_";
const CSRF_TOKEN_DOMAIN: &[u8] = b"elixir.auth.csrf.v1";
const MAX_CSRF_REVISION: i32 = i32::MAX;
const LAST_SEEN_TOUCH_INTERVAL_MINUTES: i64 = 5;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, PartialEq, Eq)]
pub struct SensitiveToken(String);

impl SensitiveToken {
    pub(crate) fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SensitiveToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SensitiveToken([REDACTED])")
    }
}

impl Drop for SensitiveToken {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

#[derive(Debug, Clone)]
pub struct LoginContext {
    pub device_name: Option<String>,
    pub device_type: Option<String>,
    pub client_name: Option<String>,
    pub client_version: Option<String>,
    pub user_agent: Option<String>,
    pub ip_hash: Option<String>,
    pub remember_device: bool,
}

impl Default for LoginContext {
    fn default() -> Self {
        Self {
            device_name: None,
            device_type: None,
            client_name: None,
            client_version: None,
            user_agent: None,
            ip_hash: None,
            remember_device: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionProfile {
    pub id: Uuid,
    pub display_name: String,
    pub profile_type: ProfileType,
}

#[derive(Clone)]
pub struct LoginTokens {
    pub access_token: SensitiveToken,
    pub access_expires_at: DateTime<Utc>,
    pub refresh_token: SensitiveToken,
    pub refresh_expires_at: DateTime<Utc>,
    pub session_id: Uuid,
    pub home_id: Uuid,
    pub profile_id: Uuid,
    pub role: HomeRole,
    pub csrf_token: SensitiveToken,
    pub profile: SessionProfile,
}

impl fmt::Debug for LoginTokens {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LoginTokens")
            .field("access_token", &self.access_token)
            .field("access_expires_at", &self.access_expires_at)
            .field("refresh_token", &self.refresh_token)
            .field("refresh_expires_at", &self.refresh_expires_at)
            .field("session_id", &self.session_id)
            .field("home_id", &self.home_id)
            .field("profile_id", &self.profile_id)
            .field("role", &self.role)
            .field("csrf_token", &self.csrf_token)
            .field("profile", &self.profile)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionPrincipal {
    pub user_id: Uuid,
    pub account_session_id: Uuid,
    pub home_id: Uuid,
    pub profile_id: Uuid,
    pub role: HomeRole,
    pub profile_type: ProfileType,
    pub profile_display_name: String,
    pub remember_device: bool,
    pub csrf_revision: i32,
    pub capability_revision: i64,
    pub session_expires_at: DateTime<Utc>,
}

#[derive(Debug, Error)]
pub enum AuthSessionError {
    #[error("invalid refresh token")]
    InvalidRefreshToken,
    #[error("refresh token reuse detected")]
    RefreshTokenReused,
    #[error("account session is revoked")]
    SessionRevoked,
    #[error("account session is expired")]
    SessionExpired,
    #[error("account session has no valid principal")]
    InvalidPrincipal,
    #[error("selected profile is unavailable to this account session")]
    ProfileUnavailable,
    #[error("account-session profile changed concurrently")]
    ProfileSwitchConflict,
    #[error("invalid authentication request metadata: {0}")]
    InvalidContext(&'static str),
    #[error("invalid persisted authentication state: {0}")]
    InvalidState(&'static str),
    #[error("authentication database operation failed")]
    Storage(#[from] sqlx::Error),
    #[error("account bootstrap failed")]
    Bootstrap(#[source] anyhow::Error),
    #[error("access-token signing failed")]
    TokenSigning(#[source] anyhow::Error),
    #[error("authentication cryptography failed")]
    Cryptography,
    #[error("authorization revocation operation failed")]
    Revocation(#[from] RevocationError),
}

impl AuthSessionError {
    pub fn is_authentication_failure(&self) -> bool {
        matches!(
            self,
            Self::InvalidRefreshToken
                | Self::RefreshTokenReused
                | Self::SessionRevoked
                | Self::SessionExpired
                | Self::InvalidPrincipal
        )
    }
}

#[derive(sqlx::FromRow)]
struct RefreshStateRow {
    token_id: String,
    session_id: String,
    token_family: String,
    token_expires_at: String,
    used_at: Option<String>,
    token_revoked_at: Option<String>,
    user_id: String,
    remember_device: i64,
    session_expires_at: String,
    session_revoked_at: Option<String>,
}

#[derive(sqlx::FromRow)]
struct PrincipalRow {
    user_id: String,
    session_id: String,
    home_id: String,
    profile_id: String,
    role: String,
    profile_type: String,
    profile_display_name: String,
    remember_device: i64,
    csrf_revision: i32,
    last_seen_at: String,
    capability_revision: i64,
    session_expires_at: String,
}

#[derive(sqlx::FromRow)]
struct RevokedSessionRow {
    home_id: Option<String>,
    profile_id: Option<String>,
}

impl TryFrom<PrincipalRow> for SessionPrincipal {
    type Error = AuthSessionError;

    fn try_from(row: PrincipalRow) -> Result<Self, Self::Error> {
        let profile_type = ProfileType::try_from(row.profile_type.as_str())
            .map_err(|_| AuthSessionError::InvalidState("profile type"))?;
        let membership_role = HomeRole::try_from(row.role.as_str())
            .map_err(|_| AuthSessionError::InvalidState("home role"))?;
        Ok(Self {
            user_id: parse_uuid(&row.user_id)?,
            account_session_id: parse_uuid(&row.session_id)?,
            home_id: parse_uuid(&row.home_id)?,
            profile_id: parse_uuid(&row.profile_id)?,
            role: if profile_type == ProfileType::Managed {
                HomeRole::Viewer
            } else {
                membership_role
            },
            profile_type,
            profile_display_name: row.profile_display_name,
            remember_device: row.remember_device != 0,
            csrf_revision: row.csrf_revision,
            capability_revision: row.capability_revision,
            session_expires_at: parse_timestamp(&row.session_expires_at)?,
        })
    }
}

impl AuthService {
    pub async fn issue_login_tokens(
        &self,
        pool: &AnyPool,
        user_id: Uuid,
        context: LoginContext,
    ) -> Result<LoginTokens, AuthSessionError> {
        let mut transaction = pool.begin().await?;
        let tokens = self
            .issue_login_tokens_in_transaction(&mut transaction, user_id, context)
            .await?;
        transaction.commit().await?;
        Ok(tokens)
    }

    pub(crate) async fn issue_login_tokens_in_transaction(
        &self,
        transaction: &mut Transaction<'_, Any>,
        user_id: Uuid,
        context: LoginContext,
    ) -> Result<LoginTokens, AuthSessionError> {
        let context = normalize_context(context)?;
        let bootstrap = ensure_owner_home_in_transaction(transaction, user_id)
            .await
            .map_err(AuthSessionError::Bootstrap)?;
        let now = Utc::now();
        let refresh_expires_at = self.session_expiration(now, context.remember_device)?;
        let session_id = Uuid::new_v4();
        let token_id = Uuid::new_v4();
        let token_family = Uuid::new_v4();
        let refresh_token = generate_refresh_token();
        let token_hash = self.hash_refresh_token(refresh_token.expose_secret())?;

        sqlx::query(
            "INSERT INTO account_sessions (
                id, user_id, home_id, active_profile_id, device_name, device_type,
                client_name, client_version, user_agent, ip_hash, remember_device,
                csrf_revision, last_seen_at, recent_auth_at, expires_at
             ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 1,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, $12
             )",
        )
        .bind(session_id.to_string())
        .bind(user_id.to_string())
        .bind(bootstrap.home.id.to_string())
        .bind(bootstrap.profile.id.to_string())
        .bind(context.device_name)
        .bind(context.device_type)
        .bind(context.client_name)
        .bind(context.client_version)
        .bind(context.user_agent)
        .bind(context.ip_hash)
        .bind(context.remember_device)
        .bind(refresh_expires_at.to_rfc3339())
        .execute(&mut **transaction)
        .await?;
        sqlx::query(
            "INSERT INTO refresh_tokens (
                id, session_id, token_hash, token_family, expires_at
             ) VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(token_id.to_string())
        .bind(session_id.to_string())
        .bind(token_hash)
        .bind(token_family.to_string())
        .bind(refresh_expires_at.to_rfc3339())
        .execute(&mut **transaction)
        .await?;

        let (access_token, access_expires_at) = self
            .sign_session_access_token(
                user_id,
                session_id,
                bootstrap.home.id,
                bootstrap.profile.id,
                bootstrap.membership.role,
            )
            .map_err(AuthSessionError::TokenSigning)?;
        let csrf_token = self.csrf_token(session_id, 1)?;
        Ok(LoginTokens {
            access_token: SensitiveToken(access_token),
            access_expires_at,
            refresh_token,
            refresh_expires_at,
            session_id,
            home_id: bootstrap.home.id,
            profile_id: bootstrap.profile.id,
            role: bootstrap.membership.role,
            csrf_token,
            profile: SessionProfile {
                id: bootstrap.profile.id,
                display_name: bootstrap.profile.display_name,
                profile_type: bootstrap.profile.profile_type,
            },
        })
    }

    pub async fn refresh_session(
        &self,
        pool: &AnyPool,
        presented_token: &str,
        context: LoginContext,
    ) -> Result<LoginTokens, AuthSessionError> {
        validate_refresh_token_shape(presented_token)?;
        let context = normalize_context(context)?;
        let presented_hash = self.hash_refresh_token(presented_token)?;
        let mut transaction = pool.begin().await?;
        let claimed = sqlx::query(
            "UPDATE refresh_tokens
             SET used_at = CURRENT_TIMESTAMP
             WHERE token_hash = $1 AND used_at IS NULL AND revoked_at IS NULL",
        )
        .bind(&presented_hash)
        .execute(&mut *transaction)
        .await?;
        let state = load_refresh_state(&mut transaction, &presented_hash)
            .await?
            .ok_or(AuthSessionError::InvalidRefreshToken)?;
        let session_id = parse_uuid(&state.session_id)?;
        let token_id = parse_uuid(&state.token_id)?;
        let token_family = parse_uuid(&state.token_family)?;
        let now = Utc::now();

        if claimed.rows_affected() != 1 {
            if state.used_at.is_some() {
                let event_id = revoke_family_in_transaction(
                    &mut transaction,
                    session_id,
                    token_family,
                    "refresh_token_reuse",
                )
                .await?;
                transaction.commit().await?;
                if let Some(event_id) = event_id {
                    self.publish_authorization_revocation(event_id);
                }
                return Err(AuthSessionError::RefreshTokenReused);
            }
            if state.token_revoked_at.is_some() || state.session_revoked_at.is_some() {
                return Err(AuthSessionError::SessionRevoked);
            }
            return Err(AuthSessionError::InvalidState("refresh-token claim"));
        }
        if state.token_revoked_at.is_some() || state.session_revoked_at.is_some() {
            return Err(AuthSessionError::SessionRevoked);
        }
        if parse_timestamp(&state.token_expires_at)? <= now
            || parse_timestamp(&state.session_expires_at)? <= now
        {
            return Err(AuthSessionError::SessionExpired);
        }

        let remember_device = state.remember_device != 0;
        let refresh_expires_at = self.session_expiration(now, remember_device)?;
        let csrf_revision: Option<i32> = sqlx::query_scalar(
            "UPDATE account_sessions
             SET device_name = COALESCE($1, device_name),
                 device_type = COALESCE($2, device_type),
                 client_name = COALESCE($3, client_name),
                 client_version = COALESCE($4, client_version),
                 user_agent = COALESCE($5, user_agent),
                 ip_hash = COALESCE($6, ip_hash),
                 last_seen_at = CURRENT_TIMESTAMP,
                 expires_at = $7,
                 csrf_revision = csrf_revision + 1
             WHERE id = $8
               AND revoked_at IS NULL
               AND csrf_revision < $9
             RETURNING csrf_revision",
        )
        .bind(context.device_name)
        .bind(context.device_type)
        .bind(context.client_name)
        .bind(context.client_version)
        .bind(context.user_agent)
        .bind(context.ip_hash)
        .bind(refresh_expires_at.to_rfc3339())
        .bind(session_id.to_string())
        .bind(MAX_CSRF_REVISION)
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(csrf_revision) = csrf_revision else {
            let event_id = revoke_family_in_transaction(
                &mut transaction,
                session_id,
                token_family,
                "csrf_revision_overflow_or_revoked",
            )
            .await?;
            transaction.commit().await?;
            if let Some(event_id) = event_id {
                self.publish_authorization_revocation(event_id);
            }
            return Err(AuthSessionError::SessionRevoked);
        };

        let replacement_id = Uuid::new_v4();
        let replacement_token = generate_refresh_token();
        let replacement_hash = self.hash_refresh_token(replacement_token.expose_secret())?;
        sqlx::query(
            "INSERT INTO refresh_tokens (
                id, session_id, token_hash, token_family, previous_token_id, expires_at
             ) VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(replacement_id.to_string())
        .bind(session_id.to_string())
        .bind(replacement_hash)
        .bind(token_family.to_string())
        .bind(token_id.to_string())
        .bind(refresh_expires_at.to_rfc3339())
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE refresh_tokens
             SET replaced_by_token_id = $1
             WHERE id = $2 AND used_at IS NOT NULL",
        )
        .bind(replacement_id.to_string())
        .bind(token_id.to_string())
        .execute(&mut *transaction)
        .await?;

        let principal = load_principal_in_transaction(
            &mut transaction,
            parse_uuid(&state.user_id)?,
            session_id,
        )
        .await?;
        let Some(principal) = principal else {
            let event_id = revoke_family_in_transaction(
                &mut transaction,
                session_id,
                token_family,
                "invalid_principal",
            )
            .await?;
            transaction.commit().await?;
            if let Some(event_id) = event_id {
                self.publish_authorization_revocation(event_id);
            }
            return Err(AuthSessionError::InvalidPrincipal);
        };
        let (access_token, access_expires_at) = self
            .sign_session_access_token(
                principal.user_id,
                principal.account_session_id,
                principal.home_id,
                principal.profile_id,
                principal.role,
            )
            .map_err(AuthSessionError::TokenSigning)?;
        let csrf_token = self.csrf_token(session_id, csrf_revision)?;
        transaction.commit().await?;

        Ok(LoginTokens {
            access_token: SensitiveToken(access_token),
            access_expires_at,
            refresh_token: replacement_token,
            refresh_expires_at,
            session_id,
            home_id: principal.home_id,
            profile_id: principal.profile_id,
            role: principal.role,
            csrf_token,
            profile: SessionProfile {
                id: principal.profile_id,
                display_name: principal.profile_display_name,
                profile_type: principal.profile_type,
            },
        })
    }

    pub async fn revoke_session(
        &self,
        pool: &AnyPool,
        session_id: Uuid,
        reason: &str,
    ) -> Result<(), AuthSessionError> {
        let reason = normalize_required("revocation reason", reason, 128)?;
        let mut transaction = pool.begin().await?;
        let event_id = revoke_session_in_transaction(&mut transaction, session_id, &reason).await?;
        transaction.commit().await?;
        if let Some(event_id) = event_id {
            self.publish_authorization_revocation(event_id);
        }
        Ok(())
    }

    pub async fn revoke_all_sessions(
        &self,
        pool: &AnyPool,
        user_id: Uuid,
        reason: &str,
    ) -> Result<(), AuthSessionError> {
        let reason = normalize_required("revocation reason", reason, 128)?;
        let mut transaction = pool.begin().await?;
        let event_ids = self
            .revoke_all_sessions_in_transaction(&mut transaction, user_id, &reason)
            .await?;
        transaction.commit().await?;
        for event_id in event_ids {
            self.publish_authorization_revocation(event_id);
        }
        Ok(())
    }

    pub(crate) async fn revoke_all_sessions_in_transaction(
        &self,
        transaction: &mut Transaction<'_, Any>,
        user_id: Uuid,
        reason: &str,
    ) -> Result<Vec<Uuid>, AuthSessionError> {
        let reason = normalize_required("revocation reason", reason, 128)?;
        let rows: Vec<RevokedSessionRow> = sqlx::query_as(
            "UPDATE account_sessions
             SET revoked_at = CURRENT_TIMESTAMP,
                 revoked_reason = $1
             WHERE user_id = $2 AND revoked_at IS NULL
             RETURNING CAST(home_id AS TEXT) AS home_id,
                       CAST(active_profile_id AS TEXT) AS profile_id",
        )
        .bind(&reason)
        .bind(user_id.to_string())
        .fetch_all(&mut **transaction)
        .await?;
        sqlx::query(
            "UPDATE refresh_tokens
             SET revoked_at = COALESCE(revoked_at, CURRENT_TIMESTAMP)
             WHERE session_id IN (
                 SELECT id FROM account_sessions WHERE user_id = $1
             )",
        )
        .bind(user_id.to_string())
        .execute(&mut **transaction)
        .await?;

        let mut homes = HashSet::new();
        for row in rows {
            let _ = row.profile_id;
            if let Some(home_id) = row.home_id {
                homes.insert(parse_uuid(&home_id)?);
            }
        }
        let mut event_ids = Vec::with_capacity(homes.len());
        for home_id in homes {
            let event = append_authorization_revocation_in_transaction(
                transaction,
                &NewAuthorizationRevocation::account(home_id, user_id, &reason),
            )
            .await?;
            event_ids.push(event.id);
        }
        Ok(event_ids)
    }

    pub async fn select_active_profile(
        &self,
        pool: &AnyPool,
        user_id: Uuid,
        session_id: Uuid,
        home_id: Uuid,
        current_profile_id: Uuid,
        selected_profile_id: Uuid,
        verified_pin_hash: Option<&str>,
    ) -> Result<SessionPrincipal, AuthSessionError> {
        let expected_pin_hash = verified_pin_hash.unwrap_or_default();
        let mut transaction = pool.begin().await?;
        let profile_available: Option<i64> = sqlx::query_scalar(
            "SELECT 1
             FROM profiles
             WHERE id = $1
               AND home_id = $2
               AND COALESCE(CAST(pin_hash AS TEXT), '') = $3
               AND (
                   (profile_type = 'account' AND user_id = $4)
                   OR (profile_type = 'managed' AND user_id IS NULL)
               )",
        )
        .bind(selected_profile_id.to_string())
        .bind(home_id.to_string())
        .bind(expected_pin_hash)
        .bind(user_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?;
        if profile_available.is_none() {
            return Err(AuthSessionError::ProfileUnavailable);
        }

        if current_profile_id == selected_profile_id {
            let principal = load_principal_in_transaction(&mut transaction, user_id, session_id)
                .await?
                .ok_or(AuthSessionError::InvalidPrincipal)?;
            if principal.profile_id != selected_profile_id {
                return Err(AuthSessionError::ProfileSwitchConflict);
            }
            transaction.commit().await?;
            return Ok(principal);
        }

        let csrf_revision: Option<i32> = sqlx::query_scalar(
            "UPDATE account_sessions
             SET active_profile_id = $1,
                 csrf_revision = csrf_revision + 1,
                 last_seen_at = CURRENT_TIMESTAMP
             WHERE id = $2
               AND user_id = $3
               AND home_id = $4
               AND active_profile_id = $5
               AND revoked_at IS NULL
               AND csrf_revision < $6
               AND EXISTS (
                   SELECT 1
                   FROM profiles
                   WHERE id = $7
                     AND home_id = $8
                     AND COALESCE(CAST(pin_hash AS TEXT), '') = $9
                     AND (
                         (profile_type = 'account' AND user_id = $10)
                         OR (profile_type = 'managed' AND user_id IS NULL)
                     )
               )
             RETURNING csrf_revision",
        )
        .bind(selected_profile_id.to_string())
        .bind(session_id.to_string())
        .bind(user_id.to_string())
        .bind(home_id.to_string())
        .bind(current_profile_id.to_string())
        .bind(MAX_CSRF_REVISION)
        .bind(selected_profile_id.to_string())
        .bind(home_id.to_string())
        .bind(expected_pin_hash)
        .bind(user_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?;
        if csrf_revision.is_none() {
            let current: Option<(String, i32, Option<String>)> = sqlx::query_as(
                "SELECT active_profile_id, csrf_revision, CAST(revoked_at AS TEXT)
                 FROM account_sessions
                 WHERE id = $1 AND user_id = $2 AND home_id = $3",
            )
            .bind(session_id.to_string())
            .bind(user_id.to_string())
            .bind(home_id.to_string())
            .fetch_optional(&mut *transaction)
            .await?;
            let Some((active_profile_id, current_csrf_revision, revoked_at)) = current else {
                return Err(AuthSessionError::InvalidPrincipal);
            };
            if revoked_at.is_some() {
                return Err(AuthSessionError::SessionRevoked);
            }
            if active_profile_id != current_profile_id.to_string() {
                return Err(AuthSessionError::ProfileSwitchConflict);
            }
            if current_csrf_revision < MAX_CSRF_REVISION {
                return Err(AuthSessionError::ProfileUnavailable);
            }
            let event_id = revoke_session_in_transaction(
                &mut transaction,
                session_id,
                "csrf_revision_overflow",
            )
            .await?;
            transaction.commit().await?;
            if let Some(event_id) = event_id {
                self.publish_authorization_revocation(event_id);
            }
            return Err(AuthSessionError::SessionRevoked);
        }

        let principal = load_principal_in_transaction(&mut transaction, user_id, session_id)
            .await?
            .ok_or(AuthSessionError::InvalidPrincipal)?;
        let event = append_authorization_revocation_in_transaction(
            &mut transaction,
            &NewAuthorizationRevocation::profile_switched(
                home_id,
                user_id,
                session_id,
                current_profile_id,
                selected_profile_id,
            ),
        )
        .await?;
        transaction.commit().await?;
        self.publish_authorization_revocation(event.id);
        Ok(principal)
    }

    pub async fn load_principal(
        &self,
        pool: &AnyPool,
        claims: &VerifiedAccessClaims,
    ) -> Result<SessionPrincipal, AuthSessionError> {
        let row = load_principal(pool, claims.user_id, claims.session_id)
            .await?
            .ok_or(AuthSessionError::InvalidPrincipal)?;
        let (row, last_seen_at) = row;
        let threshold = Utc::now() - chrono::Duration::minutes(LAST_SEEN_TOUCH_INTERVAL_MINUTES);
        if last_seen_at < threshold {
            if let Err(error) = sqlx::query(
                "UPDATE account_sessions
                 SET last_seen_at = CURRENT_TIMESTAMP
                 WHERE id = $1 AND revoked_at IS NULL",
            )
            .bind(row.account_session_id.to_string())
            .execute(pool)
            .await
            {
                tracing::warn!(error = %error, "failed to update account session last-seen time");
            }
        }
        Ok(row)
    }

    pub fn csrf_token(
        &self,
        session_id: Uuid,
        csrf_revision: i32,
    ) -> Result<SensitiveToken, AuthSessionError> {
        if csrf_revision <= 0 {
            return Err(AuthSessionError::InvalidState("CSRF revision"));
        }
        let mut mac = HmacSha256::new_from_slice(&self.csrf_key)
            .map_err(|_| AuthSessionError::Cryptography)?;
        update_framed(&mut mac, session_id.as_bytes())?;
        update_framed(&mut mac, &csrf_revision.to_be_bytes())?;
        update_framed(&mut mac, CSRF_TOKEN_DOMAIN)?;
        Ok(SensitiveToken(format!(
            "{CSRF_TOKEN_PREFIX}{}",
            URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
        )))
    }

    fn hash_refresh_token(&self, token: &str) -> Result<String, AuthSessionError> {
        let mut mac = HmacSha256::new_from_slice(&self.refresh_token_key)
            .map_err(|_| AuthSessionError::Cryptography)?;
        update_framed(&mut mac, REFRESH_TOKEN_DOMAIN)?;
        update_framed(&mut mac, token.as_bytes())?;
        Ok(URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()))
    }

    fn session_expiration(
        &self,
        now: DateTime<Utc>,
        remember_device: bool,
    ) -> Result<DateTime<Utc>, AuthSessionError> {
        let seconds = if remember_device {
            self.config
                .remembered_device_ttl_days
                .checked_mul(24 * 60 * 60)
        } else {
            self.config.access_token_ttl_minutes.checked_mul(60)
        }
        .ok_or(AuthSessionError::InvalidState("session TTL"))?;
        let seconds =
            i64::try_from(seconds).map_err(|_| AuthSessionError::InvalidState("session TTL"))?;
        let duration = chrono::Duration::try_seconds(seconds)
            .ok_or(AuthSessionError::InvalidState("session TTL"))?;
        now.checked_add_signed(duration)
            .ok_or(AuthSessionError::InvalidState("session expiration"))
    }
}

async fn load_refresh_state(
    transaction: &mut Transaction<'_, Any>,
    token_hash: &str,
) -> Result<Option<RefreshStateRow>, sqlx::Error> {
    sqlx::query_as(
        "SELECT
            rt.id AS token_id,
            rt.session_id,
            rt.token_family,
            CAST(rt.expires_at AS TEXT) AS token_expires_at,
            CAST(rt.used_at AS TEXT) AS used_at,
            CAST(rt.revoked_at AS TEXT) AS token_revoked_at,
            s.user_id,
            CAST(CASE WHEN s.remember_device THEN 1 ELSE 0 END AS BIGINT) AS remember_device,
            CAST(s.expires_at AS TEXT) AS session_expires_at,
            CAST(s.revoked_at AS TEXT) AS session_revoked_at
         FROM refresh_tokens AS rt
         JOIN account_sessions AS s ON s.id = rt.session_id
         WHERE rt.token_hash = $1
         LIMIT 1",
    )
    .bind(token_hash)
    .fetch_optional(&mut **transaction)
    .await
}

async fn load_principal(
    pool: &AnyPool,
    user_id: Uuid,
    session_id: Uuid,
) -> Result<Option<(SessionPrincipal, DateTime<Utc>)>, AuthSessionError> {
    let row: Option<PrincipalRow> = sqlx::query_as(PRINCIPAL_QUERY)
        .bind(user_id.to_string())
        .bind(session_id.to_string())
        .fetch_optional(pool)
        .await?;
    decode_active_principal(row)
}

async fn load_principal_in_transaction(
    transaction: &mut Transaction<'_, Any>,
    user_id: Uuid,
    session_id: Uuid,
) -> Result<Option<SessionPrincipal>, AuthSessionError> {
    let row: Option<PrincipalRow> = sqlx::query_as(PRINCIPAL_QUERY)
        .bind(user_id.to_string())
        .bind(session_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?;
    Ok(decode_active_principal(row)?.map(|(principal, _)| principal))
}

fn decode_active_principal(
    row: Option<PrincipalRow>,
) -> Result<Option<(SessionPrincipal, DateTime<Utc>)>, AuthSessionError> {
    let Some(row) = row else {
        return Ok(None);
    };
    let last_seen_at = parse_timestamp(&row.last_seen_at)?;
    let principal = SessionPrincipal::try_from(row)?;
    if principal.session_expires_at <= Utc::now() {
        return Ok(None);
    }
    Ok(Some((principal, last_seen_at)))
}

const PRINCIPAL_QUERY: &str = "SELECT
            s.user_id,
            s.id AS session_id,
            s.home_id,
            s.active_profile_id AS profile_id,
            hm.role,
            p.profile_type,
            p.display_name AS profile_display_name,
            CAST(CASE WHEN s.remember_device THEN 1 ELSE 0 END AS BIGINT) AS remember_device,
            s.csrf_revision,
            CAST(s.last_seen_at AS TEXT) AS last_seen_at,
            authorization.revision AS capability_revision,
            CAST(s.expires_at AS TEXT) AS session_expires_at
         FROM account_sessions AS s
         JOIN profiles AS p
           ON p.id = s.active_profile_id
          AND p.home_id = s.home_id
         JOIN home_members AS hm
           ON hm.home_id = s.home_id
          AND hm.user_id = s.user_id
          AND hm.status = 'active'
         JOIN profile_authorization_revisions AS authorization
           ON authorization.profile_id = s.active_profile_id
          AND authorization.home_id = s.home_id
         WHERE s.user_id = $1
           AND s.id = $2
           AND s.revoked_at IS NULL
           AND (
               (p.profile_type = 'account' AND p.user_id = s.user_id)
               OR (p.profile_type = 'managed' AND p.user_id IS NULL)
           )
         LIMIT 1";

async fn revoke_family_in_transaction(
    transaction: &mut Transaction<'_, Any>,
    session_id: Uuid,
    token_family: Uuid,
    reason: &str,
) -> Result<Option<Uuid>, AuthSessionError> {
    sqlx::query(
        "UPDATE refresh_tokens
         SET revoked_at = COALESCE(revoked_at, CURRENT_TIMESTAMP)
         WHERE token_family = $1",
    )
    .bind(token_family.to_string())
    .execute(&mut **transaction)
    .await?;
    revoke_session_in_transaction(transaction, session_id, reason).await
}

async fn revoke_session_in_transaction(
    transaction: &mut Transaction<'_, Any>,
    session_id: Uuid,
    reason: &str,
) -> Result<Option<Uuid>, AuthSessionError> {
    let revoked: Option<RevokedSessionRow> = sqlx::query_as(
        "UPDATE account_sessions
         SET revoked_at = CURRENT_TIMESTAMP,
             revoked_reason = $1
         WHERE id = $2 AND revoked_at IS NULL
         RETURNING CAST(home_id AS TEXT) AS home_id,
                   CAST(active_profile_id AS TEXT) AS profile_id",
    )
    .bind(reason)
    .bind(session_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?;
    sqlx::query(
        "UPDATE refresh_tokens
         SET revoked_at = COALESCE(revoked_at, CURRENT_TIMESTAMP)
         WHERE session_id = $1",
    )
    .bind(session_id.to_string())
    .execute(&mut **transaction)
    .await?;
    let Some(revoked) = revoked else {
        return Ok(None);
    };
    let Some(home_id) = revoked.home_id else {
        return Ok(None);
    };
    let profile_id = revoked.profile_id.as_deref().map(parse_uuid).transpose()?;
    let event = append_authorization_revocation_in_transaction(
        transaction,
        &NewAuthorizationRevocation::account_session(
            parse_uuid(&home_id)?,
            session_id,
            profile_id,
            reason,
        ),
    )
    .await?;
    Ok(Some(event.id))
}

fn generate_refresh_token() -> SensitiveToken {
    let mut bytes = [0_u8; 32];
    OsRng.fill_bytes(&mut bytes);
    let token = SensitiveToken(format!(
        "{REFRESH_TOKEN_PREFIX}{}",
        URL_SAFE_NO_PAD.encode(bytes)
    ));
    bytes.zeroize();
    token
}

fn validate_refresh_token_shape(token: &str) -> Result<(), AuthSessionError> {
    let encoded = token
        .strip_prefix(REFRESH_TOKEN_PREFIX)
        .ok_or(AuthSessionError::InvalidRefreshToken)?;
    if encoded.len() != 43 {
        return Err(AuthSessionError::InvalidRefreshToken);
    }
    let decoded = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| AuthSessionError::InvalidRefreshToken)?;
    if decoded.len() != 32 {
        return Err(AuthSessionError::InvalidRefreshToken);
    }
    Ok(())
}

fn normalize_context(context: LoginContext) -> Result<LoginContext, AuthSessionError> {
    Ok(LoginContext {
        device_name: normalize_optional("device name", context.device_name, 128)?,
        device_type: normalize_optional("device type", context.device_type, 64)?,
        client_name: normalize_optional("client name", context.client_name, 64)?,
        client_version: normalize_optional("client version", context.client_version, 64)?,
        user_agent: normalize_optional("user agent", context.user_agent, 512)?,
        ip_hash: normalize_optional("IP hash", context.ip_hash, 128)?,
        remember_device: context.remember_device,
    })
}

fn normalize_optional(
    label: &'static str,
    value: Option<String>,
    max_chars: usize,
) -> Result<Option<String>, AuthSessionError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.trim().is_empty() {
        return Ok(None);
    }
    normalize_required(label, &value, max_chars).map(Some)
}

fn normalize_required(
    label: &'static str,
    value: &str,
    max_chars: usize,
) -> Result<String, AuthSessionError> {
    let value = value.trim();
    if value.is_empty() || value.chars().count() > max_chars || value.chars().any(char::is_control)
    {
        return Err(AuthSessionError::InvalidContext(label));
    }
    Ok(value.to_string())
}

fn update_framed(mac: &mut HmacSha256, value: &[u8]) -> Result<(), AuthSessionError> {
    let length = u64::try_from(value.len()).map_err(|_| AuthSessionError::Cryptography)?;
    mac.update(&length.to_be_bytes());
    mac.update(value);
    Ok(())
}

fn parse_uuid(value: &str) -> Result<Uuid, AuthSessionError> {
    Uuid::parse_str(value).map_err(|_| AuthSessionError::InvalidState("UUID"))
}

fn parse_timestamp(value: &str) -> Result<DateTime<Utc>, AuthSessionError> {
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(value) {
        return Ok(timestamp.with_timezone(&Utc));
    }
    for format in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S%.f"] {
        if let Ok(timestamp) = NaiveDateTime::parse_from_str(value, format) {
            return Ok(timestamp.and_utc());
        }
    }
    Err(AuthSessionError::InvalidState("timestamp"))
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::*;
    use crate::{
        config::{AuthConfig, DatabaseConfig},
        db::Database,
    };

    async fn test_database() -> Result<Database> {
        let database = Database::connect(&DatabaseConfig {
            url: format!(
                "sqlite:file:auth-sessions-{}?mode=memory&cache=shared",
                Uuid::new_v4()
            ),
            max_connections: 4,
            connect_timeout_seconds: 5,
        })
        .await?;
        database.run_migrations().await?;
        Ok(database)
    }

    fn test_auth() -> Result<AuthService> {
        let config = AuthConfig {
            access_token_secret: "access-session-test-secret-000000000000000000000000".to_string(),
            refresh_token_secret: Some(
                "refresh-session-test-secret-00000000000000000000000".to_string(),
            ),
            csrf_secret: Some("csrf-session-test-secret-0000000000000000000000000".to_string()),
            ..AuthConfig::default()
        };
        AuthService::new(config)
    }

    async fn create_user(database: &Database) -> Result<Uuid> {
        let user_id = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
            .bind(user_id.to_string())
            .bind(format!("{user_id}@example.test"))
            .bind("hashed")
            .execute(&database.pool)
            .await?;
        Ok(user_id)
    }

    #[tokio::test]
    async fn login_persists_only_keyed_refresh_hash_and_database_principal() -> Result<()> {
        let database = test_database().await?;
        let user_id = create_user(&database).await?;
        let auth = test_auth()?;
        let tokens = auth
            .issue_login_tokens(
                &database.pool,
                user_id,
                LoginContext {
                    device_name: Some("Test Desktop".to_string()),
                    client_name: Some("elixir-test".to_string()),
                    remember_device: true,
                    ..LoginContext::default()
                },
            )
            .await?;

        let stored: (String, String, i64) = sqlx::query_as(
            "SELECT token_hash,
                    token_family,
                    (SELECT COUNT(*) FROM account_sessions WHERE id = refresh_tokens.session_id)
             FROM refresh_tokens",
        )
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(stored.0.len(), 43);
        assert_ne!(stored.0, tokens.refresh_token.expose_secret());
        assert!(!stored.1.is_empty());
        assert_eq!(stored.2, 1);
        let claims = auth.verify_access_claims(tokens.access_token.expose_secret())?;
        let principal = auth.load_principal(&database.pool, &claims).await?;
        assert_eq!(principal.user_id, user_id);
        assert_eq!(principal.account_session_id, tokens.session_id);
        assert_eq!(principal.home_id, tokens.home_id);
        assert_eq!(principal.profile_id, tokens.profile_id);
        assert_eq!(principal.role, HomeRole::Owner);
        assert_eq!(principal.profile_type, ProfileType::Account);
        assert_eq!(principal.csrf_revision, 1);
        Ok(())
    }

    #[tokio::test]
    async fn refresh_rotates_links_and_reuse_revokes_the_entire_session() -> Result<()> {
        let database = test_database().await?;
        let user_id = create_user(&database).await?;
        let auth = test_auth()?;
        let first = auth
            .issue_login_tokens(
                &database.pool,
                user_id,
                LoginContext {
                    remember_device: true,
                    ..LoginContext::default()
                },
            )
            .await?;
        let second = auth
            .refresh_session(
                &database.pool,
                first.refresh_token.expose_secret(),
                LoginContext::default(),
            )
            .await?;
        assert_ne!(
            first.refresh_token.expose_secret(),
            second.refresh_token.expose_secret()
        );
        assert_ne!(
            first.csrf_token.expose_secret(),
            second.csrf_token.expose_secret()
        );
        let links: (i64, i64, i32) = sqlx::query_as(
            "SELECT
                SUM(CASE WHEN used_at IS NOT NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN previous_token_id IS NOT NULL THEN 1 ELSE 0 END),
                (SELECT csrf_revision FROM account_sessions WHERE id = $1)
             FROM refresh_tokens",
        )
        .bind(first.session_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(links, (1, 1, 2));

        let reuse = auth
            .refresh_session(
                &database.pool,
                first.refresh_token.expose_secret(),
                LoginContext::default(),
            )
            .await;
        assert!(matches!(reuse, Err(AuthSessionError::RefreshTokenReused)));
        let revoked: (i64, i64) = sqlx::query_as(
            "SELECT
                CASE WHEN revoked_at IS NOT NULL THEN 1 ELSE 0 END,
                (SELECT COUNT(*) FROM refresh_tokens WHERE revoked_at IS NOT NULL)
             FROM account_sessions WHERE id = $1",
        )
        .bind(first.session_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(revoked, (1, 2));
        assert!(matches!(
            auth.refresh_session(
                &database.pool,
                second.refresh_token.expose_secret(),
                LoginContext::default(),
            )
            .await,
            Err(AuthSessionError::SessionRevoked)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn revoked_expired_and_suspended_sessions_fail_closed() -> Result<()> {
        let database = test_database().await?;
        let user_id = create_user(&database).await?;
        let auth = test_auth()?;
        let tokens = auth
            .issue_login_tokens(&database.pool, user_id, LoginContext::default())
            .await?;
        auth.revoke_session(&database.pool, tokens.session_id, "test_logout")
            .await?;
        assert!(matches!(
            auth.refresh_session(
                &database.pool,
                tokens.refresh_token.expose_secret(),
                LoginContext::default(),
            )
            .await,
            Err(AuthSessionError::SessionRevoked)
        ));
        let claims = auth.verify_access_claims(tokens.access_token.expose_secret())?;
        assert!(matches!(
            auth.load_principal(&database.pool, &claims).await,
            Err(AuthSessionError::InvalidPrincipal)
        ));

        let replacement = auth
            .issue_login_tokens(&database.pool, user_id, LoginContext::default())
            .await?;
        sqlx::query("UPDATE account_sessions SET expires_at = $1 WHERE id = $2")
            .bind((Utc::now() - chrono::Duration::minutes(1)).to_rfc3339())
            .bind(replacement.session_id.to_string())
            .execute(&database.pool)
            .await?;
        assert!(matches!(
            auth.refresh_session(
                &database.pool,
                replacement.refresh_token.expose_secret(),
                LoginContext::default(),
            )
            .await,
            Err(AuthSessionError::SessionExpired)
        ));
        let claims = auth.verify_access_claims(replacement.access_token.expose_secret())?;
        assert!(matches!(
            auth.load_principal(&database.pool, &claims).await,
            Err(AuthSessionError::InvalidPrincipal)
        ));

        let active = auth
            .issue_login_tokens(&database.pool, user_id, LoginContext::default())
            .await?;
        sqlx::query(
            "UPDATE home_members
             SET role = 'viewer', status = 'suspended'
             WHERE user_id = $1",
        )
        .bind(user_id.to_string())
        .execute(&database.pool)
        .await?;
        let claims = auth.verify_access_claims(active.access_token.expose_secret())?;
        assert!(matches!(
            auth.load_principal(&database.pool, &claims).await,
            Err(AuthSessionError::InvalidPrincipal)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_refresh_detects_reuse_and_leaves_no_active_family() -> Result<()> {
        let database = test_database().await?;
        let user_id = create_user(&database).await?;
        let auth = test_auth()?;
        for _ in 0..8 {
            let tokens = auth
                .issue_login_tokens(
                    &database.pool,
                    user_id,
                    LoginContext {
                        remember_device: true,
                        ..LoginContext::default()
                    },
                )
                .await?;
            let token = tokens.refresh_token.expose_secret().to_string();
            let first = auth.refresh_session(&database.pool, &token, LoginContext::default());
            let second = auth.refresh_session(&database.pool, &token, LoginContext::default());
            let (first, second) = tokio::join!(first, second);
            let outcomes = [first, second];
            assert_eq!(
                outcomes.iter().filter(|result| result.is_ok()).count(),
                1,
                "unexpected concurrent refresh outcomes: {outcomes:?}"
            );
            assert_eq!(
                outcomes
                    .iter()
                    .filter(|result| matches!(result, Err(AuthSessionError::RefreshTokenReused)))
                    .count(),
                1,
                "unexpected concurrent refresh outcomes: {outcomes:?}"
            );
            let active_sessions: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM account_sessions WHERE id = $1 AND revoked_at IS NULL",
            )
            .bind(tokens.session_id.to_string())
            .fetch_one(&database.pool)
            .await?;
            assert_eq!(active_sessions, 0);
            let active_tokens: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM refresh_tokens WHERE session_id = $1 AND revoked_at IS NULL",
            )
            .bind(tokens.session_id.to_string())
            .fetch_one(&database.pool)
            .await?;
            assert_eq!(active_tokens, 0);
        }
        Ok(())
    }

    #[tokio::test]
    async fn invalid_metadata_rolls_back_bootstrap_and_malformed_refresh_tokens_fail_closed()
    -> Result<()> {
        let database = test_database().await?;
        let user_id = create_user(&database).await?;
        let auth = test_auth()?;
        let result = auth
            .issue_login_tokens(
                &database.pool,
                user_id,
                LoginContext {
                    device_name: Some("invalid\ndevice".to_string()),
                    ..LoginContext::default()
                },
            )
            .await;
        assert!(matches!(
            result,
            Err(AuthSessionError::InvalidContext("device name"))
        ));
        let counts: (i64, i64, i64, i64) = sqlx::query_as(
            "SELECT
                (SELECT COUNT(*) FROM homes),
                (SELECT COUNT(*) FROM home_members),
                (SELECT COUNT(*) FROM profiles),
                (SELECT COUNT(*) FROM account_sessions)",
        )
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(counts, (0, 0, 0, 0));

        for token in [
            "",
            "not-a-refresh-token",
            "elx_refresh_v1_short",
            "elx_refresh_v1_!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!",
        ] {
            assert!(matches!(
                auth.refresh_session(&database.pool, token, LoginContext::default())
                    .await,
                Err(AuthSessionError::InvalidRefreshToken)
            ));
        }
        Ok(())
    }

    #[tokio::test]
    async fn csrf_revision_overflow_revokes_the_session_and_token_family() -> Result<()> {
        let database = test_database().await?;
        let user_id = create_user(&database).await?;
        let auth = test_auth()?;
        let tokens = auth
            .issue_login_tokens(&database.pool, user_id, LoginContext::default())
            .await?;
        sqlx::query("UPDATE account_sessions SET csrf_revision = $1 WHERE id = $2")
            .bind(i32::MAX)
            .bind(tokens.session_id.to_string())
            .execute(&database.pool)
            .await?;

        assert!(matches!(
            auth.refresh_session(
                &database.pool,
                tokens.refresh_token.expose_secret(),
                LoginContext::default(),
            )
            .await,
            Err(AuthSessionError::SessionRevoked)
        ));
        let state: (String, i64) = sqlx::query_as(
            "SELECT revoked_reason,
                    (SELECT COUNT(*) FROM refresh_tokens
                     WHERE session_id = $1 AND revoked_at IS NULL)
             FROM account_sessions
             WHERE id = $2",
        )
        .bind(tokens.session_id.to_string())
        .bind(tokens.session_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(state, ("csrf_revision_overflow_or_revoked".to_string(), 0));
        Ok(())
    }

    #[tokio::test]
    async fn nonremembered_refresh_preserves_ttl_and_database_role_overrides_jwt_hint() -> Result<()>
    {
        let database = test_database().await?;
        let user_id = create_user(&database).await?;
        let auth = AuthService::new(AuthConfig {
            access_token_secret: "short-session-access-secret".to_string(),
            access_token_ttl_minutes: 2,
            refresh_token_secret: Some("short-session-refresh-secret-00000000000000".to_string()),
            csrf_secret: Some("short-session-csrf-secret-00000000000000000".to_string()),
            ..AuthConfig::default()
        })?;
        let first = auth
            .issue_login_tokens(
                &database.pool,
                user_id,
                LoginContext {
                    remember_device: false,
                    ..LoginContext::default()
                },
            )
            .await?;
        assert!(first.refresh_expires_at <= Utc::now() + chrono::Duration::minutes(3));
        let second = auth
            .refresh_session(
                &database.pool,
                first.refresh_token.expose_secret(),
                LoginContext::default(),
            )
            .await?;
        assert!(second.refresh_expires_at <= Utc::now() + chrono::Duration::minutes(3));
        let claims = auth.verify_access_claims(second.access_token.expose_secret())?;
        assert_eq!(claims.role_hint, Some(HomeRole::Owner));

        sqlx::query("UPDATE home_members SET role = 'viewer' WHERE user_id = $1")
            .bind(user_id.to_string())
            .execute(&database.pool)
            .await?;
        let principal = auth.load_principal(&database.pool, &claims).await?;
        assert_eq!(principal.role, HomeRole::Viewer);
        assert!(!principal.remember_device);
        Ok(())
    }

    #[tokio::test]
    async fn a12_account_and_session_revocations_are_durable_and_notified() -> Result<()> {
        let database = test_database().await?;
        let user_id = create_user(&database).await?;
        let auth = test_auth()?;
        let mut notifications = auth.authorization_revocation_notifier().subscribe();

        let first = auth
            .issue_login_tokens(&database.pool, user_id, LoginContext::default())
            .await?;
        let second = auth
            .issue_login_tokens(&database.pool, user_id, LoginContext::default())
            .await?;
        auth.revoke_all_sessions(&database.pool, user_id, "account_disabled")
            .await?;
        let account_event_id = notifications.try_recv()?;
        let account_event: (String, String, String, String) = sqlx::query_as(
            "SELECT event_type, subject_type, subject_id, reason_code
             FROM authorization_revocation_outbox WHERE id = $1",
        )
        .bind(account_event_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(
            account_event,
            (
                "account_revoked".to_string(),
                "account".to_string(),
                user_id.to_string(),
                "account_disabled".to_string(),
            )
        );
        let revoked: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM account_sessions
             WHERE id IN ($1, $2) AND revoked_at IS NOT NULL",
        )
        .bind(first.session_id.to_string())
        .bind(second.session_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(revoked, 2);

        let third = auth
            .issue_login_tokens(&database.pool, user_id, LoginContext::default())
            .await?;
        let _replacement = auth
            .refresh_session(
                &database.pool,
                third.refresh_token.expose_secret(),
                LoginContext::default(),
            )
            .await?;
        assert!(matches!(
            auth.refresh_session(
                &database.pool,
                third.refresh_token.expose_secret(),
                LoginContext::default(),
            )
            .await,
            Err(AuthSessionError::RefreshTokenReused)
        ));
        let session_event_id = notifications.try_recv()?;
        let session_event: (String, String, String, String, String) = sqlx::query_as(
            "SELECT event_type, subject_type, subject_id, profile_id, reason_code
             FROM authorization_revocation_outbox WHERE id = $1",
        )
        .bind(session_event_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(session_event.0, "account_session_revoked");
        assert_eq!(session_event.1, "account_session");
        assert_eq!(session_event.2, third.session_id.to_string());
        assert_eq!(session_event.3, third.profile_id.to_string());
        assert_eq!(session_event.4, "refresh_token_reuse");
        Ok(())
    }

    #[test]
    fn keyed_tokens_are_domain_scoped_secret_specific_and_reject_zero_revision() -> Result<()> {
        let first = test_auth()?;
        let second = AuthService::new(AuthConfig {
            access_token_secret: "second-access-session-secret".to_string(),
            refresh_token_secret: Some(
                "second-refresh-session-secret-0000000000000000".to_string(),
            ),
            csrf_secret: Some("second-csrf-session-secret-000000000000000000".to_string()),
            ..AuthConfig::default()
        })?;
        let raw = SensitiveToken(format!(
            "{REFRESH_TOKEN_PREFIX}{}",
            URL_SAFE_NO_PAD.encode([7_u8; 32])
        ));
        validate_refresh_token_shape(raw.expose_secret())?;
        let first_hash = first.hash_refresh_token(raw.expose_secret())?;
        assert_eq!(first_hash, first.hash_refresh_token(raw.expose_secret())?);
        assert_ne!(first_hash, second.hash_refresh_token(raw.expose_secret())?);
        assert_eq!(first_hash.len(), 43);

        let session_id = Uuid::new_v4();
        assert_ne!(
            first.csrf_token(session_id, 1)?.expose_secret(),
            second.csrf_token(session_id, 1)?.expose_secret()
        );
        assert!(matches!(
            first.csrf_token(session_id, 0),
            Err(AuthSessionError::InvalidState("CSRF revision"))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn csrf_and_refresh_secrets_are_deterministic_scoped_and_redacted() -> Result<()> {
        let auth = test_auth()?;
        let session_id = Uuid::new_v4();
        let first = auth.csrf_token(session_id, 1)?;
        let same = auth.csrf_token(session_id, 1)?;
        let next = auth.csrf_token(session_id, 2)?;
        assert_eq!(first.expose_secret(), same.expose_secret());
        assert_ne!(first.expose_secret(), next.expose_secret());
        assert!(first.expose_secret().starts_with(CSRF_TOKEN_PREFIX));
        assert!(!format!("{first:?}").contains(first.expose_secret()));

        let database = test_database().await?;
        let user_id = create_user(&database).await?;
        let tokens = auth
            .issue_login_tokens(&database.pool, user_id, LoginContext::default())
            .await?;
        let rendered = format!("{tokens:?}");
        assert!(!rendered.contains(tokens.access_token.expose_secret()));
        assert!(!rendered.contains(tokens.refresh_token.expose_secret()));
        assert!(!rendered.contains(tokens.csrf_token.expose_secret()));
        Ok(())
    }
}
