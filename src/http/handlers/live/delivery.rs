//! Header-authenticated HTTP boundary for Live relay delivery.

use axum::{
    body::{Body, Bytes},
    extract::{Path, RawQuery, State},
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{AUTHORIZATION, CACHE_CONTROL, RANGE, VARY},
    },
    response::{IntoResponse, Response},
};
use chrono::Utc;
use futures_util::stream;
use tokio::io::AsyncReadExt as _;
use tokio_util::io::ReaderStream;
use uuid::Uuid;

#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::{
    live::{
        relay::{LiveRelayError, LiveRelayPayload, LiveRelayPayloadBody, hls::HlsResourceId},
        remux::{LiveRemuxError, LiveRemuxPayload, LiveRemuxPayloadBody},
        session::{DeliveryMode, SessionRecord, SessionRepositoryError},
    },
    state::AppState,
};

use super::catalog::LiveHttpRejection;

#[cfg(test)]
static TEST_HLS_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static TEST_HLS_AUTHORIZED: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static TEST_HLS_SUCCEEDED: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static TEST_HLS_LAST_ERROR: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
pub(crate) fn reset_test_delivery_counters() {
    TEST_HLS_ATTEMPTS.store(0, Ordering::SeqCst);
    TEST_HLS_AUTHORIZED.store(0, Ordering::SeqCst);
    TEST_HLS_SUCCEEDED.store(0, Ordering::SeqCst);
    TEST_HLS_LAST_ERROR.store(0, Ordering::SeqCst);
}

#[cfg(test)]
pub(crate) fn test_delivery_counters() -> (usize, usize, usize, usize) {
    (
        TEST_HLS_ATTEMPTS.load(Ordering::SeqCst),
        TEST_HLS_AUTHORIZED.load(Ordering::SeqCst),
        TEST_HLS_SUCCEEDED.load(Ordering::SeqCst),
        TEST_HLS_LAST_ERROR.load(Ordering::SeqCst),
    )
}

pub struct LiveDeliveryAuthorization {
    pub session: SessionRecord,
}

pub async fn hls_manifest(
    State(state): State<AppState>,
    Path(session_id): Path<Uuid>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
) -> Response {
    let result = async {
        #[cfg(test)]
        TEST_HLS_ATTEMPTS.fetch_add(1, Ordering::SeqCst);
        require_delivery_enabled(&state)?;
        let authorization =
            authenticate(&state, session_id, &headers, raw_query.as_deref()).await?;
        #[cfg(test)]
        TEST_HLS_AUTHORIZED.fetch_add(1, Ordering::SeqCst);
        validate_manifest_range(&headers)?;
        let payload = match authorization.session.delivery_mode {
            DeliveryMode::ServerRelay => {
                let result = relay_service(&state)?
                    .hls_manifest(&authorization.session, None)
                    .await;
                record_relay_request("manifest", &result);
                result
            }
            DeliveryMode::ServerRemux => {
                let payload = remux_service(&state)?
                    .hls_manifest(&authorization.session)
                    .await
                    .map_err(map_remux_error)?;
                return Ok::<_, LiveHttpRejection>(remux_payload_response(payload));
            }
            DeliveryMode::ClientDirect => return Err(auth_required()),
        };
        let payload = match payload {
            Ok(payload) => {
                #[cfg(test)]
                TEST_HLS_SUCCEEDED.fetch_add(1, Ordering::SeqCst);
                payload
            }
            Err(error) => {
                #[cfg(test)]
                TEST_HLS_LAST_ERROR.store(test_error_code(error), Ordering::SeqCst);
                return Err(map_relay_error(error));
            }
        };
        Ok::<_, LiveHttpRejection>(payload_response(payload))
    }
    .await;
    result.unwrap_or_else(IntoResponse::into_response)
}

#[cfg(test)]
fn test_error_code(error: LiveRelayError) -> usize {
    match error {
        LiveRelayError::Unavailable => 1,
        LiveRelayError::CapacityExhausted => 2,
        LiveRelayError::SessionExpired => 3,
        LiveRelayError::SessionMismatch => 4,
        LiveRelayError::StaleControlFence => 5,
        LiveRelayError::DescriptorInvalid => 6,
        LiveRelayError::ProtocolUnsupported => 7,
        LiveRelayError::ResourceExpired => 8,
        LiveRelayError::ResourceKindMismatch => 9,
        LiveRelayError::PolicyRejected => 10,
        LiveRelayError::CredentialsRejected => 11,
        LiveRelayError::ManifestRejected => 12,
        LiveRelayError::ContentTypeRejected => 13,
        LiveRelayError::RangeRejected => 14,
        LiveRelayError::UpstreamStatus => 15,
        LiveRelayError::Upstream(_) => 16,
    }
}

pub async fn hls_resource(
    State(state): State<AppState>,
    Path((session_id, resource_id)): Path<(Uuid, String)>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
) -> Response {
    let result = async {
        require_delivery_enabled(&state)?;
        let authorization =
            authenticate(&state, session_id, &headers, raw_query.as_deref()).await?;
        let resource_id = HlsResourceId::parse(&resource_id).ok_or_else(resource_not_found)?;
        let range = request_range(&headers)?;
        match authorization.session.delivery_mode {
            DeliveryMode::ServerRelay => {
                let result = relay_service(&state)?
                    .hls_resource(&authorization.session, &resource_id, range)
                    .await;
                record_relay_request("resource", &result);
                let payload = result.map_err(map_relay_error)?;
                Ok::<_, LiveHttpRejection>(payload_response(payload))
            }
            DeliveryMode::ServerRemux => {
                let payload = remux_service(&state)?
                    .hls_resource(&authorization.session, &resource_id, range)
                    .await
                    .map_err(map_remux_error)?;
                Ok(remux_payload_response(payload))
            }
            DeliveryMode::ClientDirect => Err(auth_required()),
        }
    }
    .await;
    result.unwrap_or_else(IntoResponse::into_response)
}

pub async fn progressive_stream(
    State(state): State<AppState>,
    Path(session_id): Path<Uuid>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
) -> Response {
    let result = async {
        require_delivery_enabled(&state)?;
        let authorization =
            authenticate(&state, session_id, &headers, raw_query.as_deref()).await?;
        let range = request_range(&headers)?;
        let relay = relay_service(&state)?;
        let result = relay
            .progressive_stream(&authorization.session, range)
            .await;
        record_relay_request("progressive", &result);
        let payload = result.map_err(map_relay_error)?;
        Ok::<_, LiveHttpRejection>(payload_response(payload))
    }
    .await;
    result.unwrap_or_else(IntoResponse::into_response)
}

impl std::fmt::Debug for LiveDeliveryAuthorization {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LiveDeliveryAuthorization")
            .field("session_id", &self.session.id)
            .field("token", &"[REDACTED]")
            .finish()
    }
}

pub async fn authenticate(
    state: &AppState,
    session_id: Uuid,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Result<LiveDeliveryAuthorization, LiveHttpRejection> {
    if raw_query.is_some_and(query_contains_token) {
        return Err(auth_required());
    }
    let values = headers.get_all(AUTHORIZATION).iter().collect::<Vec<_>>();
    if values.len() != 1 {
        return Err(auth_required());
    }
    let authorization = values[0].to_str().map_err(|_| auth_required())?;
    let token = authorization
        .strip_prefix("Bearer ")
        .filter(|token| !token.is_empty() && !token.bytes().any(|byte| byte.is_ascii_whitespace()))
        .ok_or_else(auth_required)?;
    let repository = state
        .live
        .session_repository()
        .ok_or_else(control_unavailable)?;
    let session = repository
        .verify_delivery_token(session_id, token, Utc::now())
        .await
        .map_err(map_repository_error)?;
    if session.delivery_mode == DeliveryMode::ClientDirect {
        return Err(auth_required());
    }
    Ok(LiveDeliveryAuthorization { session })
}

fn require_delivery_enabled(state: &AppState) -> Result<(), LiveHttpRejection> {
    let config = state.live.config();
    if config.enabled && config.playback_enabled && config.relay_enabled {
        Ok(())
    } else {
        Err(LiveHttpRejection::not_found())
    }
}

fn relay_service(
    state: &AppState,
) -> Result<std::sync::Arc<crate::live::relay::LiveRelayService>, LiveHttpRejection> {
    state.live.relay_service().ok_or_else(control_unavailable)
}

fn remux_service(
    state: &AppState,
) -> Result<std::sync::Arc<crate::live::remux::LiveRemuxService>, LiveHttpRejection> {
    state.live.remux_service().ok_or_else(control_unavailable)
}

fn payload_response(payload: LiveRelayPayload) -> Response {
    let metric_kind = payload.metric_kind;
    let body = match payload.body {
        LiveRelayPayloadBody::Bytes(bytes) => {
            let stream = stream::once(async move {
                crate::live::metrics::RELAY_CLIENT_BYTES
                    .with_label_values(&[metric_kind])
                    .inc_by(bytes.len() as u64);
                Ok::<Bytes, std::io::Error>(Bytes::from(bytes))
            });
            Body::from_stream(stream)
        }
        LiveRelayPayloadBody::Stream(upstream) => {
            let stream = stream::unfold(Some((upstream, metric_kind)), |state| async move {
                let (mut upstream, metric_kind) = state?;
                match upstream.next_chunk().await {
                    Ok(Some(chunk)) => {
                        let bytes = chunk.into_bytes();
                        let length = bytes.len() as u64;
                        crate::live::metrics::RELAY_UPSTREAM_BYTES
                            .with_label_values(&[metric_kind])
                            .inc_by(length);
                        crate::live::metrics::RELAY_CLIENT_BYTES
                            .with_label_values(&[metric_kind])
                            .inc_by(length);
                        Some((
                            Ok::<Bytes, std::io::Error>(Bytes::from(bytes)),
                            Some((upstream, metric_kind)),
                        ))
                    }
                    Ok(None) => None,
                    Err(_) => Some((
                        Err(std::io::Error::other("Live relay upstream stream failed")),
                        None,
                    )),
                }
            });
            Body::from_stream(stream)
        }
    };
    let mut response = Response::new(body);
    *response.status_mut() = payload.status;
    *response.headers_mut() = payload.headers;
    apply_delivery_headers(response.headers_mut());
    response
}

fn record_relay_request(kind: &'static str, result: &Result<LiveRelayPayload, LiveRelayError>) {
    let outcome = match result {
        Ok(_) => "success",
        Err(error) => relay_outcome_label(*error),
    };
    crate::live::metrics::RELAY_REQUESTS
        .with_label_values(&[kind, outcome])
        .inc();
    if matches!(result, Err(LiveRelayError::CapacityExhausted)) {
        crate::live::metrics::ADMISSION_REJECTIONS
            .with_label_values(&["relay", "capacity_exhausted"])
            .inc();
    }
}

const fn relay_outcome_label(error: LiveRelayError) -> &'static str {
    match error {
        LiveRelayError::Unavailable => "unavailable",
        LiveRelayError::CapacityExhausted => "capacity_exhausted",
        LiveRelayError::SessionExpired => "session_expired",
        LiveRelayError::SessionMismatch => "session_mismatch",
        LiveRelayError::StaleControlFence => "stale_control_fence",
        LiveRelayError::DescriptorInvalid => "descriptor_invalid",
        LiveRelayError::ProtocolUnsupported => "protocol_unsupported",
        LiveRelayError::ResourceExpired => "resource_expired",
        LiveRelayError::ResourceKindMismatch => "resource_kind_mismatch",
        LiveRelayError::PolicyRejected => "policy_rejected",
        LiveRelayError::CredentialsRejected => "credentials_rejected",
        LiveRelayError::ManifestRejected => "manifest_rejected",
        LiveRelayError::ContentTypeRejected => "content_type_rejected",
        LiveRelayError::RangeRejected => "range_rejected",
        LiveRelayError::UpstreamStatus => "upstream_status",
        LiveRelayError::Upstream(_) => "upstream",
    }
}

fn remux_payload_response(payload: LiveRemuxPayload) -> Response {
    let body = match payload.body {
        LiveRemuxPayloadBody::Bytes(bytes) => Body::from(bytes),
        LiveRemuxPayloadBody::File { file, length } => {
            Body::from_stream(ReaderStream::new(file.take(length)))
        }
    };
    let mut response = Response::new(body);
    *response.status_mut() = payload.status;
    *response.headers_mut() = payload.headers;
    apply_delivery_headers(response.headers_mut());
    response
}

fn apply_delivery_headers(headers: &mut HeaderMap) {
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(VARY, HeaderValue::from_static("Authorization"));
    headers.insert("pragma", HeaderValue::from_static("no-cache"));
    headers.insert("referrer-policy", HeaderValue::from_static("no-referrer"));
    headers.insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        "cross-origin-resource-policy",
        HeaderValue::from_static("same-origin"),
    );
}

fn validate_manifest_range(headers: &HeaderMap) -> Result<(), LiveHttpRejection> {
    if let Some(range) = request_range(headers)? {
        if range != "bytes=0-" {
            return Err(range_rejected());
        }
    }
    Ok(())
}

fn request_range(headers: &HeaderMap) -> Result<Option<&str>, LiveHttpRejection> {
    let mut values = headers.get_all(RANGE).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(range_rejected());
    }
    value.to_str().map(Some).map_err(|_| range_rejected())
}

pub(super) fn map_relay_error(error: LiveRelayError) -> LiveHttpRejection {
    match error {
        LiveRelayError::CapacityExhausted => LiveHttpRejection::new(
            StatusCode::TOO_MANY_REQUESTS,
            "LIVE_RELAY_CAPACITY",
            "Live relay capacity is exhausted.",
            true,
        ),
        LiveRelayError::SessionExpired => LiveHttpRejection::new(
            StatusCode::GONE,
            "LIVE_SESSION_EXPIRED",
            "The Live delivery session expired.",
            false,
        ),
        LiveRelayError::SessionMismatch | LiveRelayError::StaleControlFence => {
            control_unavailable()
        }
        LiveRelayError::ProtocolUnsupported => LiveHttpRejection::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "LIVE_PROTOCOL_UNSUPPORTED",
            "The Live relay protocol is unsupported.",
            false,
        ),
        LiveRelayError::ResourceExpired | LiveRelayError::ResourceKindMismatch => {
            resource_not_found()
        }
        LiveRelayError::RangeRejected => range_rejected(),
        LiveRelayError::PolicyRejected => LiveHttpRejection::new(
            StatusCode::BAD_GATEWAY,
            "LIVE_UPSTREAM_REJECTED",
            "The Live upstream source failed security validation.",
            false,
        ),
        LiveRelayError::DescriptorInvalid
        | LiveRelayError::CredentialsRejected
        | LiveRelayError::ManifestRejected
        | LiveRelayError::ContentTypeRejected => LiveHttpRejection::new(
            StatusCode::BAD_GATEWAY,
            "LIVE_UPSTREAM_INVALID",
            "The Live upstream response is invalid.",
            false,
        ),
        LiveRelayError::UpstreamStatus | LiveRelayError::Upstream(_) => LiveHttpRejection::new(
            StatusCode::BAD_GATEWAY,
            "LIVE_UPSTREAM_UNAVAILABLE",
            "The Live upstream source is unavailable.",
            true,
        ),
        LiveRelayError::Unavailable => LiveHttpRejection::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "LIVE_RELAY_UNAVAILABLE",
            "The Live relay is unavailable.",
            true,
        ),
    }
}

pub(super) fn map_remux_error(error: LiveRemuxError) -> LiveHttpRejection {
    match error {
        LiveRemuxError::CapacityExhausted => LiveHttpRejection::new(
            StatusCode::TOO_MANY_REQUESTS,
            "LIVE_REMUX_CAPACITY",
            "Live remux capacity is exhausted.",
            true,
        ),
        LiveRemuxError::SessionExpired => LiveHttpRejection::new(
            StatusCode::GONE,
            "LIVE_SESSION_EXPIRED",
            "The Live delivery session expired.",
            false,
        ),
        LiveRemuxError::SessionMismatch | LiveRemuxError::StaleControlFence => {
            control_unavailable()
        }
        LiveRemuxError::ProtocolUnsupported | LiveRemuxError::ProbeRejected => {
            LiveHttpRejection::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "LIVE_REMUX_UNSUPPORTED",
                "The Live source cannot be copy-remuxed.",
                false,
            )
        }
        LiveRemuxError::ResourceExpired | LiveRemuxError::ResourceKindMismatch => {
            resource_not_found()
        }
        LiveRemuxError::RangeRejected => range_rejected(),
        LiveRemuxError::DescriptorInvalid => LiveHttpRejection::new(
            StatusCode::BAD_GATEWAY,
            "LIVE_UPSTREAM_INVALID",
            "The Live upstream response is invalid.",
            false,
        ),
        LiveRemuxError::ProcessFailed
        | LiveRemuxError::StartupTimeout
        | LiveRemuxError::OutputUnhealthy => LiveHttpRejection::new(
            StatusCode::BAD_GATEWAY,
            "LIVE_REMUX_UNAVAILABLE",
            "The Live remux output is unavailable.",
            true,
        ),
        LiveRemuxError::DiskPressure => LiveHttpRejection::new(
            StatusCode::INSUFFICIENT_STORAGE,
            "LIVE_REMUX_STORAGE_PRESSURE",
            "Live remux storage capacity is unavailable.",
            true,
        ),
        LiveRemuxError::Unavailable => LiveHttpRejection::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "LIVE_REMUX_UNAVAILABLE",
            "The Live remux service is unavailable.",
            true,
        ),
    }
}

fn range_rejected() -> LiveHttpRejection {
    LiveHttpRejection::new(
        StatusCode::RANGE_NOT_SATISFIABLE,
        "LIVE_RANGE_REJECTED",
        "The requested Live byte range is unavailable.",
        false,
    )
}

fn resource_not_found() -> LiveHttpRejection {
    LiveHttpRejection::new(
        StatusCode::NOT_FOUND,
        "LIVE_RESOURCE_NOT_FOUND",
        "The Live delivery resource is unavailable.",
        false,
    )
}

fn query_contains_token(query: &str) -> bool {
    serde_urlencoded::from_str::<Vec<(String, String)>>(query).map_or(true, |pairs| {
        pairs.into_iter().any(|(name, _)| {
            matches!(
                name.as_str(),
                "token" | "access_token" | "sessionToken" | "session_token"
            )
        })
    })
}

fn map_repository_error(error: SessionRepositoryError) -> LiveHttpRejection {
    match error {
        SessionRepositoryError::Expired => LiveHttpRejection::new(
            axum::http::StatusCode::GONE,
            "LIVE_SESSION_EXPIRED",
            "The Live delivery session expired.",
            false,
        ),
        SessionRepositoryError::NotFound => auth_required(),
        SessionRepositoryError::FenceLost => control_unavailable(),
        _ => LiveHttpRejection::new(
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "LIVE_PROVIDER_UNAVAILABLE",
            "Live delivery authentication is unavailable.",
            true,
        ),
    }
}

fn auth_required() -> LiveHttpRejection {
    LiveHttpRejection::new(
        axum::http::StatusCode::UNAUTHORIZED,
        "LIVE_AUTH_REQUIRED",
        "A valid Live delivery bearer token is required.",
        false,
    )
}

fn control_unavailable() -> LiveHttpRejection {
    LiveHttpRejection::new(
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "LIVE_CONTROL_LEASE_UNAVAILABLE",
        "The Live control service is unavailable.",
        true,
    )
}
