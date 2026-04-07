use axum::{Json, extract::State};
use serde::Serialize;

use crate::{http::error::ApiResult, state::AppState};

#[derive(Serialize)]
pub struct SettingsResponse {
    environment: String,
    server: ServerSettings,
    network: NetworkSettings,
    vpn: VpnSettings,
    database: DatabaseSettings,
    telemetry: TelemetrySettings,
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
    }))
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
