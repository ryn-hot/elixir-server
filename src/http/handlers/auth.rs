use axum::{Json, extract::State};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    http::error::{ApiError, ApiResult},
    state::AppState,
};

#[derive(Deserialize)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenResponse {
    pub access_token: String,
    pub access_expires_at: DateTime<Utc>,
    pub token_type: &'static str,
}

impl From<crate::auth::AccessToken> for TokenResponse {
    fn from(value: crate::auth::AccessToken) -> Self {
        Self {
            access_token: value.token,
            access_expires_at: value.expires_at,
            token_type: "bearer",
        }
    }
}

pub async fn login(
    State(state): State<AppState>,
    Json(body): Json<LoginRequest>,
) -> ApiResult<Json<TokenResponse>> {
    if body.email.trim().is_empty() || body.password.is_empty() {
        return Err(ApiError::bad_request("email and password are required"));
    }

    #[derive(sqlx::FromRow)]
    struct LoginRow {
        id: String,
        password_hash: String,
    }

    let user: Option<LoginRow> =
        sqlx::query_as("SELECT id, password_hash FROM users WHERE email = ?1 LIMIT 1")
            .bind(&body.email)
            .fetch_optional(&state.db_pool)
            .await
            .map_err(|e| ApiError::internal(e.to_string()))?;

    let user = user.ok_or_else(|| ApiError::unauthorized("invalid credentials"))?;

    verify_password(&body.password, &user.password_hash)?;

    let user_id = Uuid::parse_str(&user.id).map_err(|_| ApiError::internal("invalid user id"))?;
    let token = state.auth_service.issue_access_token(user_id)?;

    Ok(Json(token.into()))
}

fn verify_password(password: &str, hashed: &str) -> ApiResult<()> {
    use argon2::{Argon2, PasswordHash, PasswordVerifier};

    let parsed_hash = PasswordHash::new(hashed)
        .map_err(|_| ApiError::internal("stored password hash is invalid"))?;

    Argon2::default()
        .verify_password(password.as_bytes(), &parsed_hash)
        .map_err(|_| ApiError::unauthorized("invalid credentials"))?;

    Ok(())
}
