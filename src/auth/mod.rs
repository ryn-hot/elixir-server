use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use chrono::{DateTime, Utc};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::config::AuthConfig;

const ACCESS_TOKEN_TYP: &str = "JWT";

#[derive(Debug, Serialize, Deserialize)]
struct AccessClaims {
    sub: String,
    sid: String,
    exp: usize,
    iat: usize,
}

#[derive(Clone)]
pub struct AuthService {
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
    config: AuthConfig,
}

#[derive(Debug, Clone)]
pub struct AccessToken {
    pub token: String,
    pub expires_at: DateTime<Utc>,
    pub session_id: Uuid,
}

impl AuthService {
    pub fn new(config: AuthConfig) -> Result<Self> {
        if config.access_token_secret.is_empty() {
            anyhow::bail!("access_token_secret must not be empty");
        }

        Ok(Self {
            encoding_key: EncodingKey::from_secret(config.access_token_secret.as_bytes()),
            decoding_key: DecodingKey::from_secret(config.access_token_secret.as_bytes()),
            config,
        })
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
        let validation = Validation::new(Algorithm::HS256);
        let token_data = decode::<AccessClaims>(token, &self.decoding_key, &validation)?;
        let user_id = Uuid::parse_str(&token_data.claims.sub)
            .map_err(|_| anyhow::anyhow!("invalid user id in token"))?;
        let session_id = Uuid::parse_str(&token_data.claims.sid)
            .map_err(|_| anyhow::anyhow!("invalid session id in token"))?;
        Ok((user_id, session_id))
    }

    pub fn sign_access_token(
        &self,
        user_id: Uuid,
        session_id: Uuid,
    ) -> Result<(String, DateTime<Utc>)> {
        self.sign_access_token_with_ttl_minutes(
            user_id,
            session_id,
            self.config.access_token_ttl_minutes,
        )
    }

    pub fn sign_access_token_with_ttl_minutes(
        &self,
        user_id: Uuid,
        session_id: Uuid,
        ttl_minutes: u64,
    ) -> Result<(String, DateTime<Utc>)> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as usize;
        let exp = now + (ttl_minutes * 60) as usize;
        let claims = AccessClaims {
            sub: user_id.to_string(),
            sid: session_id.to_string(),
            exp,
            iat: now,
        };

        let mut header = Header::new(Algorithm::HS256);
        header.typ = Some(ACCESS_TOKEN_TYP.to_string());
        let token = encode(&header, &claims, &self.encoding_key)?;
        let expires_at = DateTime::<Utc>::from(UNIX_EPOCH + Duration::from_secs(exp as u64));
        Ok((token, expires_at))
    }
}
