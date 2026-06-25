use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorResponse {
    pub code: &'static str,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

#[derive(Debug)]
pub enum ApiError {
    BadRequest(String),
    Unauthorized(String),
    Forbidden(String),
    NotFound(String),
    Conflict(String),
    ConflictWithDetails {
        message: String,
        details: Value,
    },
    Structured {
        status: StatusCode,
        code: &'static str,
        message: String,
        details: Option<Value>,
    },
    Internal(String),
}

impl ApiError {
    pub fn bad_request(msg: impl Into<String>) -> Self {
        ApiError::BadRequest(msg.into())
    }

    pub fn unauthorized(msg: impl Into<String>) -> Self {
        ApiError::Unauthorized(msg.into())
    }

    pub fn forbidden(msg: impl Into<String>) -> Self {
        ApiError::Forbidden(msg.into())
    }

    pub fn not_found(msg: impl Into<String>) -> Self {
        ApiError::NotFound(msg.into())
    }

    pub fn conflict(msg: impl Into<String>) -> Self {
        ApiError::Conflict(msg.into())
    }

    pub fn conflict_details(msg: impl Into<String>, details: Value) -> Self {
        ApiError::ConflictWithDetails {
            message: msg.into(),
            details,
        }
    }

    pub fn conflict_code(code: &'static str, msg: impl Into<String>, details: Value) -> Self {
        ApiError::Structured {
            status: StatusCode::CONFLICT,
            code,
            message: msg.into(),
            details: Some(details),
        }
    }

    pub fn structured(
        status: StatusCode,
        code: &'static str,
        msg: impl Into<String>,
        details: Option<Value>,
    ) -> Self {
        ApiError::Structured {
            status,
            code,
            message: msg.into(),
            details,
        }
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        ApiError::Internal(msg.into())
    }
}

pub type ApiResult<T> = Result<T, ApiError>;

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message, details) = match self {
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, "bad_request", msg, None),
            ApiError::Unauthorized(msg) => (StatusCode::UNAUTHORIZED, "unauthorized", msg, None),
            ApiError::Forbidden(msg) => (StatusCode::FORBIDDEN, "forbidden", msg, None),
            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, "not_found", msg, None),
            ApiError::Conflict(msg) => (StatusCode::CONFLICT, "conflict", msg, None),
            ApiError::ConflictWithDetails { message, details } => {
                (StatusCode::CONFLICT, "conflict", message, Some(details))
            }
            ApiError::Structured {
                status,
                code,
                message,
                details,
            } => (status, code, message, details),
            ApiError::Internal(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                msg,
                None,
            ),
        };

        let body = Json(ErrorResponse {
            code,
            message,
            details,
        });

        (status, body).into_response()
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(err: anyhow::Error) -> Self {
        ApiError::internal(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;

    #[test]
    fn maps_status_codes() {
        let cases = vec![
            (ApiError::bad_request(""), StatusCode::BAD_REQUEST),
            (ApiError::unauthorized(""), StatusCode::UNAUTHORIZED),
            (ApiError::forbidden(""), StatusCode::FORBIDDEN),
            (ApiError::not_found(""), StatusCode::NOT_FOUND),
            (ApiError::conflict(""), StatusCode::CONFLICT),
            (ApiError::internal(""), StatusCode::INTERNAL_SERVER_ERROR),
        ];

        for (err, expected_status) in cases {
            let response = err.into_response();
            assert_eq!(response.status(), expected_status);
        }
    }
}
