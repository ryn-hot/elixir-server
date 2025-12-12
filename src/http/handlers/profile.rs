use axum::{
    Json,
    extract::{Query, State},
};
use serde::Serialize;

use crate::{http::auth::CurrentUser, http::error::ApiResult, state::AppState};

#[derive(Debug, serde::Deserialize)]
pub struct ProfileQuery {
    pub network_type: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PlaybackProfile {
    max_resolution: String,
    supported_containers: Vec<String>,
    supported_video_codecs: Vec<String>,
    supported_audio_codecs: Vec<String>,
    max_bitrate_bps: Option<i64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProfileResponse {
    network_type: String,
    profile: PlaybackProfile,
}

pub async fn profile(
    State(state): State<AppState>,
    _user: CurrentUser,
    Query(params): Query<ProfileQuery>,
) -> ApiResult<Json<ProfileResponse>> {
    let cfg = &state.settings.playback;
    // Default to WAN profile for safety if client doesn't specify.
    let profile =
        crate::http::handlers::playback::profile_for_network(cfg, params.network_type.as_deref());
    Ok(Json(ProfileResponse {
        network_type: params.network_type.unwrap_or_else(|| "wan".to_string()),
        profile: PlaybackProfile {
            max_resolution: profile.max_resolution,
            supported_containers: profile.supported_containers,
            supported_video_codecs: profile.supported_video_codecs,
            supported_audio_codecs: profile.supported_audio_codecs,
            max_bitrate_bps: profile.max_bitrate_bps,
        },
    }))
}
