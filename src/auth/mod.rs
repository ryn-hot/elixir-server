pub mod home_profiles;
pub mod revocation;
pub mod sessions;

use std::{fmt, sync::Arc};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    auth::{home_profiles::HomeRole, revocation::AuthorizationRevocationNotifier},
    config::{AuthConfig, RunEnvironment},
};

const ACCESS_TOKEN_TYP: &str = "JWT";

#[derive(Debug, Serialize, Deserialize)]
struct AccessClaims {
    sub: String,
    sid: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    hid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    role: Option<String>,
    exp: usize,
    iat: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedAccessClaims {
    pub user_id: Uuid,
    pub session_id: Uuid,
    pub home_id_hint: Option<Uuid>,
    pub profile_id_hint: Option<Uuid>,
    pub role_hint: Option<HomeRole>,
    pub expires_at: DateTime<Utc>,
    pub issued_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct AuthService {
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
    refresh_token_key: Arc<[u8]>,
    csrf_key: Arc<[u8]>,
    revocation_notifier: AuthorizationRevocationNotifier,
    config: AuthConfig,
}

#[derive(Clone)]
pub struct AccessToken {
    pub token: String,
    pub expires_at: DateTime<Utc>,
    pub session_id: Uuid,
}

impl fmt::Debug for AccessToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AccessToken")
            .field("token", &"[REDACTED]")
            .field("expires_at", &self.expires_at)
            .field("session_id", &self.session_id)
            .finish()
    }
}

impl AuthService {
    pub fn new(config: AuthConfig) -> Result<Self> {
        config.validate(&RunEnvironment::Development)?;
        let refresh_token_key = resolve_session_key(
            config.refresh_token_secret.as_deref(),
            &config.access_token_secret,
            b"elixir.auth.development.refresh-token-key.v1",
        );
        let csrf_key = resolve_session_key(
            config.csrf_secret.as_deref(),
            &config.access_token_secret,
            b"elixir.auth.development.csrf-key.v1",
        );

        Ok(Self {
            encoding_key: EncodingKey::from_secret(config.access_token_secret.as_bytes()),
            decoding_key: DecodingKey::from_secret(config.access_token_secret.as_bytes()),
            refresh_token_key,
            csrf_key,
            revocation_notifier: AuthorizationRevocationNotifier::new(),
            config,
        })
    }

    pub fn authorization_revocation_notifier(&self) -> AuthorizationRevocationNotifier {
        self.revocation_notifier.clone()
    }

    pub(crate) fn publish_authorization_revocation(&self, event_id: Uuid) {
        self.revocation_notifier.publish(event_id);
    }

    pub fn issue_access_token(&self, user_id: Uuid) -> Result<AccessToken> {
        let session_id = Uuid::new_v4();
        let (token, expires_at) = self.sign_access_token(user_id, session_id)?;
        Ok(AccessToken {
            token,
            expires_at,
            session_id,
        })
    }

    pub fn verify_access_token(&self, token: &str) -> Result<(Uuid, Uuid)> {
        let claims = self.verify_access_claims(token)?;
        Ok((claims.user_id, claims.session_id))
    }

    pub fn verify_access_claims(&self, token: &str) -> Result<VerifiedAccessClaims> {
        let validation = Validation::new(Algorithm::HS256);
        let token_data = decode::<AccessClaims>(token, &self.decoding_key, &validation)?;
        let user_id = Uuid::parse_str(&token_data.claims.sub)
            .map_err(|_| anyhow::anyhow!("invalid user id in token"))?;
        let session_id = Uuid::parse_str(&token_data.claims.sid)
            .map_err(|_| anyhow::anyhow!("invalid session id in token"))?;
        let home_id_hint = token_data
            .claims
            .hid
            .as_deref()
            .map(Uuid::parse_str)
            .transpose()
            .context("invalid home id in token")?;
        let profile_id_hint = token_data
            .claims
            .pid
            .as_deref()
            .map(Uuid::parse_str)
            .transpose()
            .context("invalid profile id in token")?;
        let role_hint = token_data
            .claims
            .role
            .as_deref()
            .map(HomeRole::try_from)
            .transpose()
            .context("invalid home role in token")?;
        Ok(VerifiedAccessClaims {
            user_id,
            session_id,
            home_id_hint,
            profile_id_hint,
            role_hint,
            expires_at: timestamp_from_claim(token_data.claims.exp, "expiration")?,
            issued_at: timestamp_from_claim(token_data.claims.iat, "issued-at")?,
        })
    }

    pub fn sign_access_token(
        &self,
        user_id: Uuid,
        session_id: Uuid,
    ) -> Result<(String, DateTime<Utc>)> {
        self.sign_access_token_claims(
            user_id,
            session_id,
            None,
            self.config.access_token_ttl_minutes,
        )
    }

    pub fn sign_access_token_with_ttl_minutes(
        &self,
        user_id: Uuid,
        session_id: Uuid,
        ttl_minutes: u64,
    ) -> Result<(String, DateTime<Utc>)> {
        self.sign_access_token_claims(user_id, session_id, None, ttl_minutes)
    }

    pub(crate) fn sign_session_access_token(
        &self,
        user_id: Uuid,
        session_id: Uuid,
        home_id: Uuid,
        profile_id: Uuid,
        role: HomeRole,
    ) -> Result<(String, DateTime<Utc>)> {
        self.sign_access_token_claims(
            user_id,
            session_id,
            Some((home_id, profile_id, role)),
            self.config.access_token_ttl_minutes,
        )
    }

    fn sign_access_token_claims(
        &self,
        user_id: Uuid,
        session_id: Uuid,
        principal: Option<(Uuid, Uuid, HomeRole)>,
        ttl_minutes: u64,
    ) -> Result<(String, DateTime<Utc>)> {
        let ttl_seconds = ttl_minutes
            .checked_mul(60)
            .context("access token TTL overflow")?;
        let ttl_seconds = i64::try_from(ttl_seconds).context("access token TTL is too large")?;
        let ttl =
            chrono::Duration::try_seconds(ttl_seconds).context("access token TTL is too large")?;
        let issued_at = Utc::now();
        let expires_at = issued_at
            .checked_add_signed(ttl)
            .context("access token expiration overflow")?;
        let iat =
            usize::try_from(issued_at.timestamp()).context("invalid access token issue time")?;
        let exp = usize::try_from(expires_at.timestamp())
            .context("invalid access token expiration time")?;
        let (hid, pid, role) = principal
            .map(|(home_id, profile_id, role)| {
                (
                    Some(home_id.to_string()),
                    Some(profile_id.to_string()),
                    Some(role.as_str().to_string()),
                )
            })
            .unwrap_or((None, None, None));
        let claims = AccessClaims {
            sub: user_id.to_string(),
            sid: session_id.to_string(),
            hid,
            pid,
            role,
            exp,
            iat,
        };

        let mut header = Header::new(Algorithm::HS256);
        header.typ = Some(ACCESS_TOKEN_TYP.to_string());
        let token = encode(&header, &claims, &self.encoding_key)?;
        Ok((token, expires_at))
    }
}

fn resolve_session_key(configured: Option<&str>, access_secret: &str, domain: &[u8]) -> Arc<[u8]> {
    configured
        .map(|secret| Arc::<[u8]>::from(secret.as_bytes()))
        .unwrap_or_else(|| {
            let mut digest = Sha256::new();
            digest.update(domain);
            digest.update((access_secret.len() as u64).to_be_bytes());
            digest.update(access_secret.as_bytes());
            Arc::<[u8]>::from(digest.finalize().to_vec())
        })
}

fn timestamp_from_claim(value: usize, label: &str) -> Result<DateTime<Utc>> {
    let seconds = i64::try_from(value).with_context(|| format!("invalid {label} claim"))?;
    DateTime::from_timestamp(seconds, 0).with_context(|| format!("invalid {label} claim"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn access_tokens_redact_debug_and_preserve_legacy_and_session_claims() -> Result<()> {
        let auth = AuthService::new(AuthConfig::default())?;
        let user_id = Uuid::new_v4();
        let legacy = auth.issue_access_token(user_id)?;
        assert!(!format!("{legacy:?}").contains(&legacy.token));
        let legacy_claims = auth.verify_access_claims(&legacy.token)?;
        assert_eq!(legacy_claims.user_id, user_id);
        assert!(legacy_claims.home_id_hint.is_none());
        assert!(legacy_claims.profile_id_hint.is_none());
        assert!(legacy_claims.role_hint.is_none());

        let session_id = Uuid::new_v4();
        let home_id = Uuid::new_v4();
        let profile_id = Uuid::new_v4();
        let (token, _) = auth.sign_session_access_token(
            user_id,
            session_id,
            home_id,
            profile_id,
            HomeRole::Viewer,
        )?;
        let claims = auth.verify_access_claims(&token)?;
        assert_eq!(claims.session_id, session_id);
        assert_eq!(claims.home_id_hint, Some(home_id));
        assert_eq!(claims.profile_id_hint, Some(profile_id));
        assert_eq!(claims.role_hint, Some(HomeRole::Viewer));
        Ok(())
    }
}
