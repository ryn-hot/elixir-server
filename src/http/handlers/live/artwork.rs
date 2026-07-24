use axum::{
    body::Body,
    extract::{Path, RawQuery, State},
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE, ETAG, IF_NONE_MATCH, VARY},
    },
    response::Response,
};

use crate::{
    live::{
        artwork::{ArtworkFetchRequest, LiveArtworkError, LiveArtworkErrorCode},
        catalog::{LiveProviderAccess, LivePublicKeyCodec},
    },
    state::AppState,
};

use super::catalog::{
    CancelOnDrop, LiveBrowsePrincipal, LiveHttpRejection, access_context, admit_artwork,
    error_response, key_scope, reject_query, request_id,
};

pub async fn get(
    State(state): State<AppState>,
    LiveBrowsePrincipal(principal): LiveBrowsePrincipal,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Path(artwork_id): Path<String>,
) -> Response {
    let request_id = request_id();
    let result = async {
        reject_query(raw_query.as_deref())?;
        let _admission = admit_artwork(principal.user_id)?;
        let context = access_context(&principal, &headers)?;
        let scope = key_scope(&context);
        let crypto = state
            .live
            .crypto()
            .await
            .ok_or_else(LiveHttpRejection::unavailable)?;
        let opened = LivePublicKeyCodec::new(crypto)
            .open_artwork(&artwork_id, scope, context.now)
            .map_err(|_| LiveHttpRejection::not_found())?;
        let provider_id = opened.provider_id;
        let catalog = state
            .live
            .catalog_service()
            .ok_or_else(LiveHttpRejection::unavailable)?;
        let visibility = catalog
            .grants()
            .visibility(
                context.home_id,
                context.profile_id,
                context.role,
                context.profile_type,
                provider_id,
                LiveProviderAccess::Browse,
            )
            .await
            .map_err(|_| LiveHttpRejection::unavailable())?;
        if !visibility.allowed
            || visibility.authorization_revision != context.authorization_revision
        {
            return Err(LiveHttpRejection::provider_forbidden());
        }
        let service = state
            .live
            .artwork_service()
            .ok_or_else(LiveHttpRejection::unavailable)?;
        let cancellation = CancelOnDrop::new();
        let artwork = service
            .fetch(
                ArtworkFetchRequest::from_opened(opened, scope),
                cancellation.token(),
            )
            .await
            .map_err(map_artwork_error)?;
        let current_visibility = catalog
            .grants()
            .visibility(
                context.home_id,
                context.profile_id,
                context.role,
                context.profile_type,
                provider_id,
                LiveProviderAccess::Browse,
            )
            .await
            .map_err(|_| LiveHttpRejection::unavailable())?;
        if !current_visibility.allowed
            || current_visibility.authorization_revision != context.authorization_revision
        {
            return Err(LiveHttpRejection::provider_forbidden());
        }
        if etag_matches(&headers, &artwork.etag)? {
            return Ok(not_modified(&artwork.etag));
        }
        Ok(image_response(artwork))
    }
    .await;
    result.unwrap_or_else(|error| error_response(error, Some(request_id)))
}

fn map_artwork_error(error: LiveArtworkError) -> LiveHttpRejection {
    match error.code() {
        LiveArtworkErrorCode::InvalidRequest => LiveHttpRejection::not_found(),
        LiveArtworkErrorCode::PolicyDenied => LiveHttpRejection::new(
            StatusCode::FORBIDDEN,
            "LIVE_ARTWORK_POLICY_DENIED",
            "The Live artwork destination is not approved.",
            false,
        ),
        LiveArtworkErrorCode::MediaTypeRejected
        | LiveArtworkErrorCode::ImageRejected
        | LiveArtworkErrorCode::ImageTooLarge => LiveHttpRejection::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "LIVE_ARTWORK_REJECTED",
            "The Live artwork response is not a supported image.",
            false,
        ),
        LiveArtworkErrorCode::Cancelled => LiveHttpRejection::new(
            StatusCode::REQUEST_TIMEOUT,
            "LIVE_ARTWORK_CANCELLED",
            "The Live artwork request was cancelled.",
            true,
        ),
        LiveArtworkErrorCode::UpstreamUnavailable
        | LiveArtworkErrorCode::UpstreamStatus
        | LiveArtworkErrorCode::DecodeTimeout
        | LiveArtworkErrorCode::Internal => LiveHttpRejection::new(
            StatusCode::BAD_GATEWAY,
            "LIVE_ARTWORK_UNAVAILABLE",
            "The Live artwork is temporarily unavailable.",
            true,
        ),
    }
}

fn etag_matches(headers: &HeaderMap, etag: &str) -> Result<bool, LiveHttpRejection> {
    let values = headers.get_all(IF_NONE_MATCH);
    if values.iter().count() > 1 {
        return Err(LiveHttpRejection::invalid_request());
    }
    let Some(value) = values.iter().next() else {
        return Ok(false);
    };
    let value = value
        .to_str()
        .map_err(|_| LiveHttpRejection::invalid_request())?;
    if value.len() > 512 || value.chars().any(char::is_control) {
        return Err(LiveHttpRejection::invalid_request());
    }
    Ok(value
        .split(',')
        .map(str::trim)
        .any(|candidate| candidate == "*" || candidate == etag))
}

fn image_response(artwork: crate::live::artwork::LiveArtwork) -> Response {
    let length = artwork.bytes.len();
    let mut response = Response::new(Body::from(artwork.bytes));
    *response.status_mut() = StatusCode::OK;
    let headers = response.headers_mut();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(artwork.content_type));
    if let Ok(value) = HeaderValue::from_str(&artwork.etag) {
        headers.insert(ETAG, value);
    }
    if let Ok(value) = HeaderValue::from_str(&length.to_string()) {
        headers.insert(CONTENT_LENGTH, value);
    }
    security_headers(headers);
    response
}

fn not_modified(etag: &str) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::NOT_MODIFIED;
    if let Ok(value) = HeaderValue::from_str(etag) {
        response.headers_mut().insert(ETAG, value);
    }
    security_headers(response.headers_mut());
    response
}

fn security_headers(headers: &mut HeaderMap) {
    headers.insert(
        CACHE_CONTROL,
        HeaderValue::from_static("private, max-age=300, must-revalidate"),
    );
    headers.insert(
        VARY,
        HeaderValue::from_static("Authorization, Cookie, Accept-Encoding"),
    );
    headers.insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        "cross-origin-resource-policy",
        HeaderValue::from_static("same-origin"),
    );
    headers.insert("referrer-policy", HeaderValue::from_static("no-referrer"));
}
