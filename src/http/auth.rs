use axum::{
    async_trait,
    extract::FromRequestParts,
    http::{HeaderMap, request::Parts},
};
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::{
    auth::{
        AuthService,
        home_profiles::{HomeRole, ProfileType},
        sessions::AuthSessionError,
    },
    authz::{AuthorizationRepository, Capability, CapabilitySet},
    http::error::ApiError,
    state::AppState,
};

#[derive(Debug, Clone)]
pub struct CurrentUser {
    pub user_id: Uuid,
    pub session_id: Uuid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountAuthTransport {
    Bearer,
    Cookie,
    Query,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentPrincipal {
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
    pub access_expires_at: DateTime<Utc>,
    pub transport: AccountAuthTransport,
    capabilities: CapabilitySet,
}

impl CurrentPrincipal {
    pub fn has_capability(&self, capability: Capability) -> bool {
        self.capabilities.contains(capability)
    }

    pub fn require(&self, capability: Capability) -> Result<(), ApiError> {
        if self.has_capability(capability) {
            Ok(())
        } else {
            Err(ApiError::forbidden(
                "principal lacks the required capability",
            ))
        }
    }

    pub fn require_home_role(&self, minimum: HomeRole) -> Result<(), ApiError> {
        if home_role_rank(self.role) >= home_role_rank(minimum) {
            Ok(())
        } else {
            Err(ApiError::forbidden("principal role is insufficient"))
        }
    }

    pub fn capabilities(&self) -> impl Iterator<Item = Capability> {
        self.capabilities.iter()
    }
}

#[async_trait]
impl FromRequestParts<AppState> for CurrentUser {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        parse_user(parts, &state.auth_service)
    }
}

#[async_trait]
impl FromRequestParts<AppState> for CurrentPrincipal {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let (token, transport) = extract_token_with_transport(parts)?;
        let claims = state
            .auth_service
            .verify_access_claims(&token)
            .map_err(|_| ApiError::unauthorized("invalid or expired token"))?;
        let authorization = AuthorizationRepository::new(&state.db_pool);
        for _ in 0..3 {
            let principal = state
                .auth_service
                .load_principal(&state.db_pool, &claims)
                .await
                .map_err(map_principal_error)?;
            let effective = authorization
                .load_effective(principal.profile_id, principal.role, principal.profile_type)
                .await
                .map_err(|error| {
                    tracing::error!(error = %error, "failed to load principal capabilities");
                    ApiError::internal("authorization service unavailable")
                })?;
            if effective.revision != principal.capability_revision {
                continue;
            }
            return Ok(Self {
                user_id: principal.user_id,
                account_session_id: principal.account_session_id,
                home_id: principal.home_id,
                profile_id: principal.profile_id,
                role: principal.role,
                profile_type: principal.profile_type,
                profile_display_name: principal.profile_display_name,
                remember_device: principal.remember_device,
                csrf_revision: principal.csrf_revision,
                capability_revision: principal.capability_revision,
                session_expires_at: principal.session_expires_at,
                access_expires_at: claims.expires_at,
                transport,
                capabilities: effective.capabilities,
            });
        }
        Err(ApiError::internal(
            "authorization changed while loading the account principal",
        ))
    }
}

const fn home_role_rank(role: HomeRole) -> u8 {
    match role {
        HomeRole::Viewer => 0,
        HomeRole::Manager => 1,
        HomeRole::Admin => 2,
        HomeRole::Owner => 3,
    }
}

fn parse_user(parts: &Parts, auth: &AuthService) -> Result<CurrentUser, ApiError> {
    let token = extract_token(parts)?;
    let (user_id, session_id) = auth
        .verify_access_token(&token)
        .map_err(|_| ApiError::unauthorized("invalid or expired token"))?;

    Ok(CurrentUser {
        user_id,
        session_id,
    })
}

fn extract_token(parts: &Parts) -> Result<String, ApiError> {
    extract_token_with_transport(parts).map(|(token, _)| token)
}

fn extract_token_with_transport(parts: &Parts) -> Result<(String, AccountAuthTransport), ApiError> {
    if let Some(tok) = bearer_from_headers(&parts.headers) {
        return Ok((tok, AccountAuthTransport::Bearer));
    }

    if let Some(tok) = token_from_cookies(&parts.headers) {
        return Ok((tok, AccountAuthTransport::Cookie));
    }

    if let Some(tok) = token_from_query(parts.uri.query()) {
        return Ok((tok, AccountAuthTransport::Query));
    }

    Err(ApiError::unauthorized("missing authorization token"))
}

fn map_principal_error(error: AuthSessionError) -> ApiError {
    if error.is_authentication_failure() {
        return ApiError::unauthorized("account session is no longer valid");
    }
    tracing::error!(error = %error, "failed to load current account principal");
    ApiError::internal("authentication service unavailable")
}

fn bearer_from_headers(headers: &HeaderMap) -> Option<String> {
    let raw = headers.get("authorization")?;
    let raw_str = raw.to_str().ok()?;
    raw_str.strip_prefix("Bearer ").map(|s| s.to_string())
}

fn token_from_query(query: Option<&str>) -> Option<String> {
    let q = query?;
    for pair in q.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            if matches!(k, "token" | "access_token") {
                return Some(v.to_string());
            }
        }
    }
    None
}

fn token_from_cookies(headers: &HeaderMap) -> Option<String> {
    let raw = headers.get("cookie")?;
    let raw_str = raw.to_str().ok()?;
    for cookie in raw_str.split(';') {
        let cookie = cookie.trim();
        if let Some((key, value)) = cookie.split_once('=') {
            if key.trim() == "elixir_ui_token" {
                let token = value.trim();
                if !token.is_empty() {
                    return Some(token.to_string());
                }
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AuthConfig;
    use axum::http::Request;

    #[test]
    fn parses_valid_bearer_token() {
        let auth = AuthService::new(AuthConfig::default()).unwrap();
        let user_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let (token, _) = auth.sign_access_token(user_id, session_id).unwrap();

        let request = Request::builder()
            .header("authorization", format!("Bearer {}", token))
            .body(())
            .unwrap();
        let (parts, _) = request.into_parts();

        let current_user = parse_user(&parts, &auth).expect("should parse bearer token");
        assert_eq!(current_user.user_id, user_id);
        assert_eq!(current_user.session_id, session_id);
    }

    #[test]
    fn parses_valid_cookie_token() {
        let auth = AuthService::new(AuthConfig::default()).unwrap();
        let user_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let (token, _) = auth.sign_access_token(user_id, session_id).unwrap();

        let request = Request::builder()
            .header(
                "cookie",
                format!("foo=bar; elixir_ui_token={token}; theme=dark"),
            )
            .body(())
            .unwrap();
        let (parts, _) = request.into_parts();

        let current_user = parse_user(&parts, &auth).expect("should parse cookie token");
        assert_eq!(current_user.user_id, user_id);
        assert_eq!(current_user.session_id, session_id);
    }
}
