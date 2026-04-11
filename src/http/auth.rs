use axum::{
    async_trait,
    extract::FromRequestParts,
    http::{HeaderMap, request::Parts},
};
use uuid::Uuid;

use crate::{auth::AuthService, http::error::ApiError, state::AppState};

#[derive(Debug, Clone)]
pub struct CurrentUser {
    pub user_id: Uuid,
    pub session_id: Uuid,
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
    if let Some(tok) = bearer_from_headers(&parts.headers) {
        return Ok(tok);
    }

    if let Some(tok) = token_from_cookies(&parts.headers) {
        return Ok(tok);
    }

    if let Some(tok) = token_from_query(parts.uri.query()) {
        return Ok(tok);
    }

    Err(ApiError::unauthorized("missing authorization token"))
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
            .header("cookie", format!("foo=bar; elixir_ui_token={token}; theme=dark"))
            .body(())
            .unwrap();
        let (parts, _) = request.into_parts();

        let current_user = parse_user(&parts, &auth).expect("should parse cookie token");
        assert_eq!(current_user.user_id, user_id);
        assert_eq!(current_user.session_id, session_id);
    }
}
