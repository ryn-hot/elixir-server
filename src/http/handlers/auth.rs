use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, header},
    response::{IntoResponse, Response},
};
use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    auth::{
        home_profiles::{HomeMemberStatus, HomeProfileRepository, HomeRole, ProfileType},
        sessions::{AuthSessionError, LoginContext, LoginTokens},
    },
    authz::{AuthorizationRepository, Capability},
    http::auth::{AccountAuthTransport, CurrentPrincipal},
    http::error::{ApiError, ApiResult},
    state::AppState,
};

#[derive(Deserialize)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
    #[serde(default = "default_remember_device")]
    pub remember_device: bool,
    #[serde(default)]
    pub device_name: Option<String>,
    #[serde(default)]
    pub device_type: Option<String>,
    #[serde(default)]
    pub client_name: Option<String>,
    #[serde(default)]
    pub client_version: Option<String>,
}

#[derive(Serialize)]
pub struct TokenResponse {
    pub access_token: String,
    pub access_expires_at: DateTime<Utc>,
    pub refresh_token: String,
    pub refresh_expires_at: DateTime<Utc>,
    pub token_type: &'static str,
    pub session_id: Uuid,
    pub home_id: Uuid,
    pub profile_id: Uuid,
    pub role: HomeRole,
    pub csrf_token: String,
    pub profile: ProfileResponse,
}

impl From<&LoginTokens> for TokenResponse {
    fn from(value: &LoginTokens) -> Self {
        Self {
            access_token: value.access_token.expose_secret().to_string(),
            access_expires_at: value.access_expires_at,
            refresh_token: value.refresh_token.expose_secret().to_string(),
            refresh_expires_at: value.refresh_expires_at,
            token_type: "bearer",
            session_id: value.session_id,
            home_id: value.home_id,
            profile_id: value.profile_id,
            role: value.role,
            csrf_token: value.csrf_token.expose_secret().to_string(),
            profile: ProfileResponse {
                id: value.profile.id,
                display_name: value.profile.display_name.clone(),
                profile_type: value.profile.profile_type,
            },
        }
    }
}

#[derive(Serialize)]
pub struct ProfileResponse {
    pub id: Uuid,
    pub display_name: String,
    pub profile_type: ProfileType,
}

#[derive(Deserialize)]
pub struct SignupRequest {
    pub email: String,
    pub password: String,
    #[serde(default = "default_remember_device")]
    pub remember_device: bool,
    #[serde(default)]
    pub device_name: Option<String>,
    #[serde(default)]
    pub device_type: Option<String>,
    #[serde(default)]
    pub client_name: Option<String>,
    #[serde(default)]
    pub client_version: Option<String>,
}

#[derive(Deserialize)]
pub struct RefreshRequest {
    pub refresh_token: String,
    #[serde(default)]
    pub device_name: Option<String>,
    #[serde(default)]
    pub device_type: Option<String>,
    #[serde(default)]
    pub client_name: Option<String>,
    #[serde(default)]
    pub client_version: Option<String>,
}

#[derive(Deserialize)]
pub struct LogoutRequest {
    #[serde(default)]
    pub refresh_token: Option<String>,
}

#[derive(Serialize)]
pub struct SessionResponse {
    pub user_id: Uuid,
    pub session_id: Uuid,
    pub home_id: Uuid,
    pub active_profile_id: Uuid,
    pub role: HomeRole,
    pub capability_revision: i64,
    pub capabilities: Vec<Capability>,
    pub profile: ProfileResponse,
    pub access_expires_at: DateTime<Utc>,
    pub session_expires_at: DateTime<Utc>,
    pub remember_device: bool,
    pub csrf_token: String,
}

#[derive(Serialize)]
pub struct ProfilesResponse {
    pub profiles: Vec<ProfileSummaryResponse>,
}

#[derive(Serialize)]
pub struct ProfileSummaryResponse {
    pub id: Uuid,
    pub display_name: String,
    pub profile_type: ProfileType,
    pub has_pin: bool,
    pub role: HomeRole,
    pub avatar_color: Option<String>,
}

#[derive(Deserialize)]
pub struct SelectProfileRequest {
    #[serde(default)]
    pub pin: Option<String>,
}

#[derive(Deserialize)]
pub struct PasswordResetStartRequest {
    pub email: String,
}

#[derive(Serialize)]
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
    headers: HeaderMap,
    Json(body): Json<LoginRequest>,
) -> ApiResult<Response> {
    if body.email.trim().is_empty() || body.password.is_empty() {
        return Err(ApiError::bad_request("email and password are required"));
    }

    #[derive(sqlx::FromRow)]
    struct LoginRow {
        id: String,
        password_hash: String,
    }

    let email = body.email.trim().to_lowercase();
    let user: Option<LoginRow> =
        sqlx::query_as("SELECT id, password_hash FROM users WHERE email = $1 LIMIT 1")
            .bind(&email)
            .fetch_optional(&state.db_pool)
            .await
            .map_err(|error| database_error("load account for login", error))?;

    let user = user.ok_or_else(|| ApiError::unauthorized("invalid credentials"))?;

    verify_password(&body.password, &user.password_hash)?;

    let user_id = Uuid::parse_str(&user.id).map_err(|_| ApiError::internal("invalid user id"))?;
    let tokens = state
        .auth_service
        .issue_login_tokens(
            &state.db_pool,
            user_id,
            login_context(
                body.remember_device,
                body.device_name,
                body.device_type,
                body.client_name,
                body.client_version,
                &headers,
            ),
        )
        .await
        .map_err(map_session_error)?;

    Ok(no_store_json(TokenResponse::from(&tokens)))
}

pub async fn signup(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<SignupRequest>,
) -> ApiResult<Response> {
    if body.email.trim().is_empty() || body.password.len() < 8 {
        return Err(ApiError::bad_request(
            "email required and password must be at least 8 characters",
        ));
    }

    let email = body.email.trim().to_lowercase();
    let user_id = Uuid::new_v4();
    let password_hash = hash_password(&body.password)?;
    let context = login_context(
        body.remember_device,
        body.device_name,
        body.device_type,
        body.client_name,
        body.client_version,
        &headers,
    );
    let mut transaction = state
        .db_pool
        .begin()
        .await
        .map_err(|error| database_error("begin account signup", error))?;
    let insert_result =
        sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
            .bind(user_id.to_string())
            .bind(&email)
            .bind(password_hash)
            .execute(&mut *transaction)
            .await;
    if let Err(error) = insert_result {
        if is_unique_violation(&error) {
            return Err(ApiError::conflict("email already registered"));
        }
        return Err(database_error("create signup account", error));
    }

    let tokens = state
        .auth_service
        .issue_login_tokens_in_transaction(&mut transaction, user_id, context)
        .await
        .map_err(map_session_error)?;
    transaction
        .commit()
        .await
        .map_err(|error| database_error("commit account signup", error))?;
    Ok(no_store_json(TokenResponse::from(&tokens)))
}

pub async fn refresh(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<RefreshRequest>,
) -> ApiResult<Response> {
    let tokens = state
        .auth_service
        .refresh_session(
            &state.db_pool,
            &body.refresh_token,
            login_context(
                true,
                body.device_name,
                body.device_type,
                body.client_name,
                body.client_version,
                &headers,
            ),
        )
        .await
        .map_err(map_session_error)?;
    Ok(no_store_json(TokenResponse::from(&tokens)))
}

pub async fn logout(
    State(state): State<AppState>,
    principal: CurrentPrincipal,
    Json(body): Json<LogoutRequest>,
) -> ApiResult<Json<&'static str>> {
    require_bearer_transport(&principal)?;
    let _ = body.refresh_token;
    state
        .auth_service
        .revoke_session(&state.db_pool, principal.account_session_id, "user_logout")
        .await
        .map_err(map_session_error)?;
    Ok(Json("ok"))
}

pub async fn session(
    State(state): State<AppState>,
    principal: CurrentPrincipal,
) -> ApiResult<Response> {
    require_bearer_transport(&principal)?;
    let csrf_token = state
        .auth_service
        .csrf_token(principal.account_session_id, principal.csrf_revision)
        .map_err(map_session_error)?;
    Ok(no_store_json(SessionResponse {
        user_id: principal.user_id,
        session_id: principal.account_session_id,
        home_id: principal.home_id,
        active_profile_id: principal.profile_id,
        role: principal.role,
        capability_revision: principal.capability_revision,
        capabilities: principal.capabilities().collect(),
        profile: ProfileResponse {
            id: principal.profile_id,
            display_name: principal.profile_display_name,
            profile_type: principal.profile_type,
        },
        access_expires_at: principal.access_expires_at,
        session_expires_at: principal.session_expires_at,
        remember_device: principal.remember_device,
        csrf_token: csrf_token.expose_secret().to_string(),
    }))
}

pub async fn profiles(
    State(state): State<AppState>,
    principal: CurrentPrincipal,
) -> ApiResult<Response> {
    require_bearer_transport(&principal)?;
    let repository = HomeProfileRepository::new(&state.db_pool);
    let membership = repository
        .membership(principal.home_id, principal.user_id)
        .await
        .map_err(|error| {
            tracing::error!(error = %error, "failed to load profile-list membership");
            ApiError::internal("profile service unavailable")
        })?
        .filter(|membership| membership.status == HomeMemberStatus::Active)
        .ok_or_else(|| ApiError::unauthorized("home membership is no longer active"))?;
    let profiles = repository
        .list_profiles(principal.home_id)
        .await
        .map_err(|error| {
            tracing::error!(error = %error, "failed to list account profiles");
            ApiError::internal("profile service unavailable")
        })?
        .into_iter()
        .filter(|profile| {
            profile.profile_type == ProfileType::Managed
                || profile.user_id == Some(principal.user_id)
        })
        .map(|profile| ProfileSummaryResponse {
            id: profile.id,
            display_name: profile.display_name,
            profile_type: profile.profile_type,
            has_pin: profile.pin_hash.is_some(),
            role: if profile.profile_type == ProfileType::Managed {
                HomeRole::Viewer
            } else {
                membership.role
            },
            avatar_color: profile.avatar_color,
        })
        .collect();
    Ok(no_store_json(ProfilesResponse { profiles }))
}

pub async fn select_profile(
    State(state): State<AppState>,
    principal: CurrentPrincipal,
    Path(profile_id): Path<Uuid>,
    Json(body): Json<SelectProfileRequest>,
) -> ApiResult<Response> {
    require_bearer_transport(&principal)?;
    let profile = HomeProfileRepository::new(&state.db_pool)
        .profile(profile_id)
        .await
        .map_err(|error| {
            tracing::error!(error = %error, "failed to load selected profile");
            ApiError::internal("profile service unavailable")
        })?
        .filter(|profile| {
            profile.home_id == principal.home_id
                && (profile.profile_type == ProfileType::Managed
                    || profile.user_id == Some(principal.user_id))
        })
        .ok_or_else(|| ApiError::forbidden("profile is unavailable"))?;
    let verified_pin_hash = verify_profile_pin(body.pin, profile.pin_hash.clone()).await?;
    let selected = state
        .auth_service
        .select_active_profile(
            &state.db_pool,
            principal.user_id,
            principal.account_session_id,
            principal.home_id,
            principal.profile_id,
            profile_id,
            verified_pin_hash.as_deref(),
        )
        .await
        .map_err(map_profile_selection_error)?;
    let effective = AuthorizationRepository::new(&state.db_pool)
        .load_effective(selected.profile_id, selected.role, selected.profile_type)
        .await
        .map_err(|error| {
            tracing::error!(error = %error, "failed to load selected profile capabilities");
            ApiError::internal("authorization service unavailable")
        })?;
    if effective.revision != selected.capability_revision {
        return Err(ApiError::internal(
            "authorization changed while selecting the profile",
        ));
    }
    let csrf_token = state
        .auth_service
        .csrf_token(selected.account_session_id, selected.csrf_revision)
        .map_err(map_session_error)?;
    Ok(no_store_json(SessionResponse {
        user_id: selected.user_id,
        session_id: selected.account_session_id,
        home_id: selected.home_id,
        active_profile_id: selected.profile_id,
        role: selected.role,
        capability_revision: effective.revision,
        capabilities: effective.capabilities.iter().collect(),
        profile: ProfileResponse {
            id: selected.profile_id,
            display_name: selected.profile_display_name,
            profile_type: selected.profile_type,
        },
        access_expires_at: principal.access_expires_at,
        session_expires_at: selected.session_expires_at,
        remember_device: selected.remember_device,
        csrf_token: csrf_token.expose_secret().to_string(),
    }))
}

pub async fn start_password_reset(
    State(state): State<AppState>,
    Json(body): Json<PasswordResetStartRequest>,
) -> ApiResult<Response> {
    let email = body.email.trim().to_lowercase();
    if email.is_empty() {
        return Err(ApiError::bad_request("email is required"));
    }
    let user_id: Option<String> =
        sqlx::query_scalar("SELECT id FROM users WHERE email = $1 LIMIT 1")
            .bind(&email)
            .fetch_optional(&state.db_pool)
            .await
            .map_err(|error| database_error("load password-reset account", error))?;

    let token = Uuid::new_v4();
    let expires_at = Utc::now() + chrono::Duration::minutes(15);
    if let Some(uid) = user_id {
        sqlx::query("INSERT INTO password_resets (id, user_id, token, expires_at, used) VALUES ($1, $2, $3, $4, FALSE)")
            .bind(Uuid::new_v4().to_string())
            .bind(uid)
            .bind(token.to_string())
            .bind(expires_at.to_rfc3339())
            .execute(&state.db_pool)
            .await
            .map_err(|error| database_error("create password reset", error))?;
    }

    Ok(no_store_json(PasswordResetStartResponse {
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

    let new_hash = hash_password(&body.new_password)?;
    let mut transaction = state
        .db_pool
        .begin()
        .await
        .map_err(|error| database_error("begin password reset", error))?;
    let reset: Option<ResetRow> = sqlx::query_as::<_, ResetRow>(
        "SELECT user_id, CAST(expires_at AS TEXT) as expires_at, CAST(CASE WHEN used THEN 1 ELSE 0 END AS BIGINT) as used FROM password_resets WHERE token = $1 LIMIT 1",
    )
    .bind(&body.token)
    .fetch_optional(&mut *transaction)
    .await
    .map_err(|error| database_error("load password reset", error))?;

    let reset = reset.ok_or_else(|| ApiError::bad_request("invalid token"))?;
    if reset.used != 0 {
        return Err(ApiError::bad_request("token already used"));
    }
    let expires = parse_database_timestamp(&reset.expires_at)
        .ok_or_else(|| ApiError::internal("invalid password-reset expiration"))?;
    if Utc::now() >= expires {
        return Err(ApiError::bad_request("token expired"));
    }

    let claimed = sqlx::query(
        "UPDATE password_resets
         SET used = TRUE
         WHERE token = $1 AND used = FALSE",
    )
    .bind(&body.token)
    .execute(&mut *transaction)
    .await
    .map_err(|error| database_error("claim password reset", error))?;
    if claimed.rows_affected() != 1 {
        return Err(ApiError::bad_request("token already used"));
    }
    let updated_user = sqlx::query("UPDATE users SET password_hash = $1 WHERE id = $2")
        .bind(&new_hash)
        .bind(&reset.user_id)
        .execute(&mut *transaction)
        .await
        .map_err(|error| database_error("update password", error))?;
    if updated_user.rows_affected() != 1 {
        tracing::error!(user_id = %reset.user_id, "password-reset account disappeared");
        return Err(ApiError::internal("authentication service unavailable"));
    }
    let reset_user_id = Uuid::parse_str(&reset.user_id)
        .map_err(|_| ApiError::internal("invalid password-reset user id"))?;
    let revocation_event_ids = state
        .auth_service
        .revoke_all_sessions_in_transaction(&mut transaction, reset_user_id, "password_reset")
        .await
        .map_err(map_session_error)?;
    transaction
        .commit()
        .await
        .map_err(|error| database_error("commit password reset", error))?;
    for event_id in revocation_event_ids {
        state
            .auth_service
            .publish_authorization_revocation(event_id);
    }

    Ok(Json("ok"))
}

fn default_remember_device() -> bool {
    true
}

fn no_store_json<T: Serialize>(body: T) -> Response {
    let mut response = Json(body).into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-store, private"),
    );
    response
        .headers_mut()
        .insert(header::PRAGMA, HeaderValue::from_static("no-cache"));
    response
}

fn login_context(
    remember_device: bool,
    device_name: Option<String>,
    device_type: Option<String>,
    client_name: Option<String>,
    client_version: Option<String>,
    headers: &HeaderMap,
) -> LoginContext {
    LoginContext {
        device_name,
        device_type,
        client_name,
        client_version,
        user_agent: headers
            .get(header::USER_AGENT)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string),
        ip_hash: None,
        remember_device,
    }
}

fn require_bearer_transport(principal: &CurrentPrincipal) -> ApiResult<()> {
    if principal.transport != AccountAuthTransport::Bearer {
        return Err(ApiError::forbidden(
            "this account-session operation requires bearer authentication",
        ));
    }
    Ok(())
}

fn map_session_error(error: AuthSessionError) -> ApiError {
    if matches!(error, AuthSessionError::InvalidContext(_)) {
        return ApiError::bad_request("invalid device or client metadata");
    }
    if error.is_authentication_failure() {
        return ApiError::unauthorized("invalid refresh token or account session");
    }
    tracing::error!(error = %error, "account-session operation failed");
    ApiError::internal("authentication service unavailable")
}

fn map_profile_selection_error(error: AuthSessionError) -> ApiError {
    match error {
        AuthSessionError::ProfileUnavailable => ApiError::forbidden("profile is unavailable"),
        AuthSessionError::ProfileSwitchConflict => {
            ApiError::conflict("profile changed concurrently; retry the selection")
        }
        other => map_session_error(other),
    }
}

async fn verify_profile_pin(
    presented_pin: Option<String>,
    stored_hash: Option<String>,
) -> ApiResult<Option<String>> {
    let Some(stored_hash) = stored_hash else {
        return Ok(None);
    };
    let pin = presented_pin.unwrap_or_default();
    if !(4..=12).contains(&pin.len()) || !pin.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(ApiError::forbidden("profile PIN is invalid"));
    }
    let hash_for_verification = stored_hash.clone();
    let verified = tokio::task::spawn_blocking(move || {
        use argon2::{Argon2, PasswordHash, PasswordVerifier};

        let parsed_hash = PasswordHash::new(&hash_for_verification).ok()?;
        Some(
            Argon2::default()
                .verify_password(pin.as_bytes(), &parsed_hash)
                .is_ok(),
        )
    })
    .await
    .map_err(|error| {
        tracing::error!(error = %error, "profile PIN verifier task failed");
        ApiError::internal("profile service unavailable")
    })?
    .ok_or_else(|| ApiError::internal("stored profile PIN is invalid"))?;
    if !verified {
        return Err(ApiError::forbidden("profile PIN is invalid"));
    }
    Ok(Some(stored_hash))
}

fn database_error(operation: &'static str, error: sqlx::Error) -> ApiError {
    tracing::error!(operation, error = %error, "authentication database operation failed");
    ApiError::internal("authentication service unavailable")
}

fn is_unique_violation(error: &sqlx::Error) -> bool {
    matches!(error, sqlx::Error::Database(database_error) if database_error.is_unique_violation())
}

fn parse_database_timestamp(value: &str) -> Option<DateTime<Utc>> {
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(value) {
        return Some(timestamp.with_timezone(&Utc));
    }
    for format in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S%.f"] {
        if let Ok(timestamp) = NaiveDateTime::parse_from_str(value, format) {
            return Some(timestamp.and_utc());
        }
    }
    None
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
