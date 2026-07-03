use axum::{Json, extract::State};
use serde::Serialize;

use crate::{
    config::{RunEnvironment, Settings},
    http::error::ApiResult,
    state::AppState,
};

#[derive(Serialize)]
pub struct SettingsResponse {
    environment: String,
    server: ServerSettings,
    network: NetworkSettings,
    vpn: VpnSettings,
    database: DatabaseSettings,
    telemetry: TelemetrySettings,
    playback: PlaybackSettings,
}

#[derive(Serialize)]
pub struct ServerSettings {
    host: String,
    port: u16,
}

#[derive(Serialize)]
pub struct NetworkSettings {
    mdns_enabled: bool,
    mdns_name: String,
    wan_enabled: bool,
    wan_direct_endpoint: Option<String>,
    default_lan_max_bitrate_bps: Option<i64>,
    default_wan_max_bitrate_bps: Option<i64>,
    wan_status: String,
}

#[derive(Serialize)]
pub struct VpnSettings {
    enabled: bool,
    detect_host_vpn: bool,
    auto_wrap_qbittorrent: bool,
    auto_wrap_nzbget: bool,
    wireguard_config_secret: String,
    wireguard_gateway_image: String,
    host_vpn_detected: bool,
    host_vpn_interfaces: Vec<String>,
    warning: Option<String>,
}

#[derive(Serialize)]
pub struct DatabaseSettings {
    driver: &'static str,
    max_connections: u32,
    connect_timeout_seconds: u64,
}

#[derive(Serialize)]
pub struct TelemetrySettings {
    log_directives: String,
}

#[derive(Serialize)]
pub struct PlaybackSettings {
    rollout_gates: Vec<PlaybackRolloutGateSettings>,
    planner_policy: PlaybackPlannerPolicySettings,
}

#[derive(Serialize)]
pub struct PlaybackRolloutGateSettings {
    flag: &'static str,
    raw_enabled: bool,
    effective_enabled: bool,
    default_enabled: bool,
    runtime_enforced: bool,
    release_evidence_gate: bool,
    description: &'static str,
}

#[derive(Serialize)]
pub struct PlaybackPlannerPolicySettings {
    allow_direct_play: bool,
    allow_direct_stream: bool,
    allow_audio_transcode: bool,
    allow_video_transcode: bool,
    allow_adaptive_transcode: bool,
    force_direct_play_for_native_mpv: bool,
}

pub async fn settings(State(app_state): State<AppState>) -> ApiResult<Json<SettingsResponse>> {
    let settings = &app_state.settings;

    Ok(Json(SettingsResponse {
        environment: settings.environment.as_str().to_string(),
        server: ServerSettings {
            host: settings.server.host.clone(),
            port: settings.server.port,
        },
        network: NetworkSettings {
            mdns_enabled: settings.network.mdns_enabled,
            mdns_name: settings.network.mdns_name.clone(),
            wan_enabled: settings.network.wan_enabled,
            wan_direct_endpoint: latest_wan_endpoint(&app_state).await,
            default_lan_max_bitrate_bps: settings.playback.default_lan_max_bitrate_bps,
            default_wan_max_bitrate_bps: settings.playback.default_wan_max_bitrate_bps,
            wan_status: wan_status(&app_state).await,
        },
        vpn: vpn_settings(&app_state),
        database: DatabaseSettings {
            driver: app_state.db_driver.as_str(),
            max_connections: settings.database.max_connections,
            connect_timeout_seconds: settings.database.connect_timeout_seconds,
        },
        telemetry: TelemetrySettings {
            log_directives: settings.telemetry.log_directives.clone(),
        },
        playback: playback_settings(settings),
    }))
}

fn playback_settings(settings: &Settings) -> PlaybackSettings {
    PlaybackSettings {
        rollout_gates: playback_rollout_gates(settings),
        planner_policy: PlaybackPlannerPolicySettings {
            allow_direct_play: settings.playback.allow_direct_play,
            allow_direct_stream: settings.playback.allow_direct_stream,
            allow_audio_transcode: settings.playback.allow_audio_transcode,
            allow_video_transcode: settings.playback.allow_video_transcode,
            allow_adaptive_transcode: settings.playback.allow_adaptive_transcode,
            force_direct_play_for_native_mpv: settings.playback.force_direct_play_for_native_mpv,
        },
    }
}

fn playback_rollout_gates(settings: &Settings) -> Vec<PlaybackRolloutGateSettings> {
    let playback = &settings.playback;
    let plan_contract_effective =
        playback.plan_contract_enabled || settings.environment == RunEnvironment::Development;

    vec![
        playback_gate(
            "playback.plan_contract_enabled",
            playback.plan_contract_enabled,
            plan_contract_effective,
            true,
            true,
            false,
            "playback plan response contract",
        ),
        playback_gate(
            "playback.hls_direct_stream_enabled",
            playback.hls_direct_stream_enabled,
            playback.hls_direct_stream_enabled,
            true,
            true,
            false,
            "HLS direct stream remux",
        ),
        playback_gate(
            "playback.audio_transcode_enabled",
            playback.audio_transcode_enabled,
            playback.audio_transcode_enabled,
            true,
            true,
            false,
            "audio-only transcode",
        ),
        playback_gate(
            "playback.subtitle_transcode_enabled",
            playback.subtitle_transcode_enabled,
            playback.subtitle_transcode_enabled,
            true,
            true,
            false,
            "subtitle extraction, conversion, and burn-in",
        ),
        playback_gate(
            "playback.video_transcode_enabled",
            playback.video_transcode_enabled,
            playback.video_transcode_enabled,
            true,
            true,
            false,
            "full video transcode",
        ),
        playback_gate(
            "playback.transcode_feasibility_enabled",
            playback.transcode_feasibility_enabled,
            playback.transcode_feasibility_enabled,
            true,
            true,
            false,
            "runtime transcode capability and performance admission",
        ),
        playback_gate(
            "playback.adaptive_quality_enabled",
            playback.adaptive_quality_enabled,
            playback.adaptive_quality_enabled,
            false,
            true,
            false,
            "adaptive quality ladder playback",
        ),
        playback_gate(
            "playback.hardware_acceleration_enabled",
            playback.hardware_acceleration_enabled,
            playback.hardware_acceleration_enabled,
            false,
            true,
            false,
            "hardware acceleration",
        ),
        playback_gate(
            "playback.hdr_tone_mapping_enabled",
            playback.hdr_tone_mapping_enabled,
            playback.hdr_tone_mapping_enabled,
            false,
            true,
            false,
            "HDR to SDR tone mapping",
        ),
        playback_gate(
            "playback.public_corpus_required",
            playback.public_corpus_required,
            playback.public_corpus_required,
            false,
            false,
            true,
            "release-candidate public corpus evidence",
        ),
        playback_gate(
            "playback.client_automation_required",
            playback.client_automation_required,
            playback.client_automation_required,
            false,
            false,
            true,
            "release-candidate client playback automation evidence",
        ),
    ]
}

fn playback_gate(
    flag: &'static str,
    raw_enabled: bool,
    effective_enabled: bool,
    default_enabled: bool,
    runtime_enforced: bool,
    release_evidence_gate: bool,
    description: &'static str,
) -> PlaybackRolloutGateSettings {
    PlaybackRolloutGateSettings {
        flag,
        raw_enabled,
        effective_enabled,
        default_enabled,
        runtime_enforced,
        release_evidence_gate,
        description,
    }
}

fn vpn_settings(state: &AppState) -> VpnSettings {
    let detect_host_vpn = state.settings.network.vpn.detect_host_vpn;
    let status = if detect_host_vpn {
        crate::network::vpn::detect_host_vpn()
    } else {
        crate::network::vpn::HostVpnStatus {
            detected: false,
            interfaces: Vec::new(),
            warning: None,
        }
    };
    VpnSettings {
        enabled: state.settings.network.vpn.enabled,
        detect_host_vpn,
        auto_wrap_qbittorrent: state.settings.network.vpn.auto_wrap_qbittorrent,
        auto_wrap_nzbget: state.settings.network.vpn.auto_wrap_nzbget,
        wireguard_config_secret: state.settings.network.vpn.wireguard_config_secret.clone(),
        wireguard_gateway_image: state.settings.network.vpn.wireguard_gateway_image.clone(),
        host_vpn_detected: status.detected,
        host_vpn_interfaces: status.interfaces,
        warning: status.warning,
    }
}

async fn latest_wan_endpoint(state: &AppState) -> Option<String> {
    sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT wan_direct_endpoint FROM server_registry ORDER BY last_seen_at DESC LIMIT 1",
    )
    .fetch_optional(&state.db_pool)
    .await
    .ok()
    .flatten()
}

async fn wan_status(state: &AppState) -> String {
    if !state.settings.network.wan_enabled {
        return "disabled".to_string();
    }
    let endpoint = latest_wan_endpoint(state).await;
    if endpoint.is_some() {
        "ok".to_string()
    } else {
        "unknown".to_string()
    }
}
