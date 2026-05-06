use axum::{Json, extract::State};

use crate::{
    http::{
        auth::CurrentUser,
        error::{ApiError, ApiResult},
    },
    state::AppState,
    torrentio::{
        TorrentioCandidateSearchRequest, TorrentioCandidateSearchResponse,
        search_torrentio_candidates,
    },
};

pub async fn search_candidates(
    State(state): State<AppState>,
    _user: CurrentUser,
    Json(request): Json<TorrentioCandidateSearchRequest>,
) -> ApiResult<Json<TorrentioCandidateSearchResponse>> {
    search_torrentio_candidates(&state, request)
        .await
        .map(Json)
        .map_err(|err| {
            let message = err.to_string();
            if message.contains("requires")
                || message.contains("required")
                || message.contains("unknown media type")
                || message.contains("unsupported characters")
                || message.contains("not available")
                || message.contains("disabled")
                || message.contains("debrid account tokens")
            {
                ApiError::bad_request(message)
            } else {
                ApiError::from(err)
            }
        })
}
