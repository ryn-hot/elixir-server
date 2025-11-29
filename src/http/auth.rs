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
        parse_user_from_headers(&parts.headers, &state.auth_service)
    }
}

fn parse_user_from_headers(
    headers: &HeaderMap,
    auth: &AuthService,
) -> Result<CurrentUser, ApiError> {
    let raw = headers
        .get("authorization")
        .ok_or_else(|| ApiError::unauthorized("missing authorization header"))?;

    let raw_str = raw
        .to_str()
        .map_err(|_| ApiError::bad_request("invalid authorization header"))?;

    let token = raw_str
        .strip_prefix("Bearer ")
        .ok_or_else(|| ApiError::unauthorized("invalid authorization scheme"))?;

    let (user_id, session_id) = auth
        .verify_access_token(token)
        .map_err(|_| ApiError::unauthorized("invalid or expired token"))?;

    Ok(CurrentUser {
        user_id,
        session_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AuthConfig;
    use axum::http::HeaderMap;

    #[test]
    fn parses_valid_bearer_token() {
        let auth = AuthService::new(AuthConfig::default()).unwrap();
        let user_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let (token, _) = auth.sign_access_token(user_id, session_id).unwrap();

        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            format!("Bearer {}", token).parse().unwrap(),
        );

        let current_user =
            parse_user_from_headers(&headers, &auth).expect("should parse bearer token");
        assert_eq!(current_user.user_id, user_id);
        assert_eq!(current_user.session_id, session_id);
    }
}
