use axum::{Json, extract::State};
use serde::Serialize;

use crate::{http::auth::CurrentUser, http::error::ApiResult, state::AppState};

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
) -> ApiResult<Json<ProfileResponse>> {
    let cfg = &state.settings.playback;
    // Default to WAN profile for safety if client doesn't specify.
    let profile = PlaybackProfile {
        max_resolution: cfg.default_max_resolution.clone(),
        supported_containers: cfg.default_supported_containers.clone(),
        supported_video_codecs: cfg.default_supported_video_codecs.clone(),
        supported_audio_codecs: cfg.default_supported_audio_codecs.clone(),
        max_bitrate_bps: cfg.default_wan_max_bitrate_bps,
    };
    Ok(Json(ProfileResponse {
        network_type: "wan".to_string(),
        profile,
    }))
}
