use axum::{
    Json,
    extract::{Query, State},
};
use serde::Serialize;

use crate::{
    http::{
        auth::CurrentUser,
        error::ApiResult,
        handlers::playback::{
            effective_playback_policy_from_config, merge_client_profile_with_server_profile,
            profile_for_network,
        },
    },
    playback::{
        hardware::HardwareCapabilities,
        profile::{
            ClientPlaybackProfile, EffectivePlaybackPolicy, NetworkClass,
            PlaybackProfileCatalogEntry, PlaybackProfileNegotiation, negotiate_playback_profile,
            playback_profile_catalog,
        },
    },
    state::AppState,
};

#[derive(Debug, serde::Deserialize)]
pub struct ProfileQuery {
    pub network_type: Option<String>,
    pub profile_id: Option<String>,
    #[serde(alias = "profileId")]
    pub playback_profile_id: Option<String>,
    pub profile_version: Option<u32>,
    pub app_version: Option<String>,
    pub client_kind: Option<String>,
}

#[derive(Serialize)]
pub struct PlaybackProfile {
    max_resolution: String,
    supported_containers: Vec<String>,
    supported_video_codecs: Vec<String>,
    supported_audio_codecs: Vec<String>,
    max_bitrate_bps: Option<i64>,
}

#[derive(Serialize)]
pub struct ProfileResponse {
    network_type: String,
    server_profile: PlaybackProfile,
    profile: ClientPlaybackProfile,
    effective_policy: EffectivePlaybackPolicy,
    negotiation: PlaybackProfileNegotiation,
    catalog: Vec<PlaybackProfileCatalogEntry>,
}

pub async fn profile(
    State(state): State<AppState>,
    _user: CurrentUser,
    Query(params): Query<ProfileQuery>,
) -> ApiResult<Json<ProfileResponse>> {
    let cfg = &state.settings.playback;
    // Default to WAN profile for safety if client doesn't specify.
    let network_type = params.network_type.unwrap_or_else(|| "wan".to_string());
    let server_profile = profile_for_network(cfg, Some(network_type.as_str()));
    let network_class = match network_type.trim().to_ascii_lowercase().as_str() {
        "lan" | "local" | "private" => NetworkClass::Lan,
        "wan" | "remote" | "public" => NetworkClass::Wan,
        _ => NetworkClass::Unknown,
    };
    let requested_profile_id = params
        .playback_profile_id
        .as_deref()
        .or(params.profile_id.as_deref());
    let negotiated = negotiate_playback_profile(
        requested_profile_id,
        params.profile_version,
        params.app_version.as_deref(),
        params.client_kind.as_deref(),
    );
    let effective_profile =
        merge_client_profile_with_server_profile(negotiated.profile.clone(), &server_profile);
    let effective_policy = effective_playback_policy_from_config(
        cfg,
        &server_profile,
        &effective_profile,
        network_class,
        state
            .hardware_capabilities
            .read()
            .await
            .clone()
            .unwrap_or_else(HardwareCapabilities::software_only),
    );
    Ok(Json(ProfileResponse {
        network_type,
        server_profile: PlaybackProfile {
            max_resolution: server_profile.max_resolution,
            supported_containers: server_profile.supported_containers,
            supported_video_codecs: server_profile.supported_video_codecs,
            supported_audio_codecs: server_profile.supported_audio_codecs,
            max_bitrate_bps: server_profile.max_bitrate_bps,
        },
        profile: effective_profile,
        effective_policy,
        negotiation: negotiated.negotiation,
        catalog: playback_profile_catalog(),
    }))
}
