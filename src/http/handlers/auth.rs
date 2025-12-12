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

#[derive(Deserialize)]
pub struct SignupRequest {
    pub email: String,
    pub password: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SignupResponse {
    pub access_token: String,
    pub access_expires_at: DateTime<Utc>,
    pub token_type: &'static str,
}

#[derive(Deserialize)]
pub struct PasswordResetStartRequest {
    pub email: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PasswordResetStartResponse {
    pub token: String,
    pub expires_at: DateTime<Utc>,
}

#[derive(Deserialize)]
pub struct PasswordResetCompleteRequest {
    pub token: String,
    pub new_password: String,
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

pub async fn signup(
    State(state): State<AppState>,
    Json(body): Json<SignupRequest>,
) -> ApiResult<Json<SignupResponse>> {
    if body.email.trim().is_empty() || body.password.len() < 8 {
        return Err(ApiError::bad_request(
            "email required and password must be at least 8 characters",
        ));
    }

    let email = body.email.trim().to_lowercase();
    // Check for existing user
    let exists: Option<(String,)> = sqlx::query_as("SELECT id FROM users WHERE email = ? LIMIT 1")
        .bind(&email)
        .fetch_optional(&state.db_pool)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;
    if exists.is_some() {
        return Err(ApiError::conflict("email already registered"));
    }

    let user_id = Uuid::new_v4();
    let password_hash = hash_password(&body.password)?;
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES (?1, ?2, ?3)")
        .bind(user_id.to_string())
        .bind(&email)
        .bind(password_hash)
        .execute(&state.db_pool)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;

    let token = state.auth_service.issue_access_token(user_id)?;
    Ok(Json(SignupResponse {
        access_token: token.token,
        access_expires_at: token.expires_at,
        token_type: "bearer",
    }))
}

pub async fn start_password_reset(
    State(state): State<AppState>,
    Json(body): Json<PasswordResetStartRequest>,
) -> ApiResult<Json<PasswordResetStartResponse>> {
    let email = body.email.trim().to_lowercase();
    let user_id: Option<String> =
        sqlx::query_scalar("SELECT id FROM users WHERE email = ? LIMIT 1")
            .bind(&email)
            .fetch_optional(&state.db_pool)
            .await
            .ok()
            .flatten();

    let token = Uuid::new_v4();
    let expires_at = Utc::now() + chrono::Duration::minutes(15);
    if let Some(uid) = user_id {
        let _ = sqlx::query("INSERT INTO password_resets (id, user_id, token, expires_at, used) VALUES (?1, ?2, ?3, ?4, 0)")
            .bind(Uuid::new_v4().to_string())
            .bind(uid)
            .bind(token.to_string())
            .bind(expires_at.to_rfc3339())
            .execute(&state.db_pool)
            .await;
    }

    Ok(Json(PasswordResetStartResponse {
        token: token.to_string(),
        expires_at,
    }))
}

pub async fn complete_password_reset(
    State(state): State<AppState>,
    Json(body): Json<PasswordResetCompleteRequest>,
) -> ApiResult<Json<&'static str>> {
    if body.new_password.len() < 8 {
        return Err(ApiError::bad_request(
            "new password must be at least 8 characters",
        ));
    }
    #[derive(sqlx::FromRow)]
    struct ResetRow {
        user_id: String,
        expires_at: String,
        used: i64,
    }

    let reset: Option<ResetRow> = sqlx::query_as::<_, ResetRow>(
        "SELECT user_id, CAST(expires_at AS TEXT) as expires_at, CAST(used AS INTEGER) as used FROM password_resets WHERE token = ? LIMIT 1",
    )
    .bind(&body.token)
    .fetch_optional(&state.db_pool)
    .await
    .map_err(|e| ApiError::internal(e.to_string()))?;

    let reset = reset.ok_or_else(|| ApiError::bad_request("invalid token"))?;
    if reset.used != 0 {
        return Err(ApiError::bad_request("token already used"));
    }
    let expires = DateTime::parse_from_rfc3339(&reset.expires_at)
        .map_err(|_| ApiError::internal("invalid expires format"))?
        .with_timezone(&Utc);
    if Utc::now() > expires {
        return Err(ApiError::bad_request("token expired"));
    }

    let new_hash = hash_password(&body.new_password)?;
    sqlx::query("UPDATE users SET password_hash = ? WHERE id = ?")
        .bind(&new_hash)
        .bind(&reset.user_id)
        .execute(&state.db_pool)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;

    sqlx::query("UPDATE password_resets SET used = 1 WHERE token = ?")
        .bind(&body.token)
        .execute(&state.db_pool)
        .await
        .ok();

    Ok(Json("ok"))
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

fn hash_password(password: &str) -> ApiResult<String> {
    use argon2::password_hash::SaltString;
    use argon2::{Argon2, PasswordHasher};
    use rand_core::OsRng;

    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map_err(|_| ApiError::internal("failed to hash password"))
        .map(|ph| ph.to_string())
}
