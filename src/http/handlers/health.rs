use std::time::Instant;

use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::Row;

use crate::{http::error::ApiResult, metrics, state::AppState};

#[derive(Serialize)]
pub struct HealthResponse {
    status: &'static str,
    environment: &'static str,
    timestamp_utc: DateTime<Utc>,
    database: DatabaseHealth,
    mdns: MdnsHealth,
    wan: WanHealth,
    vpn: VpnHealth,
}

#[derive(Serialize)]
pub struct DatabaseHealth {
    status: &'static str,
    driver: &'static str,
    latency_ms: Option<u128>,
    error: Option<String>,
}

#[derive(Serialize)]
pub struct MdnsHealth {
    status: &'static str,
    name: Option<String>,
}

#[derive(Serialize)]
pub struct WanHealth {
    status: &'static str,
    wan_direct_endpoint: Option<String>,
}

#[derive(Serialize)]
pub struct VpnHealth {
    status: &'static str,
    host_vpn_detected: bool,
    host_vpn_interfaces: Vec<String>,
    warning: Option<String>,
}

pub async fn healthcheck(State(app_state): State<AppState>) -> ApiResult<Json<HealthResponse>> {
    let environment = app_state.settings.environment.as_str();
    let database = check_database(&app_state).await;
    let mdns_status = check_mdns(&app_state);
    let wan_status = check_wan(&app_state).await;
    let vpn_status = check_vpn(&app_state);

    Ok(Json(HealthResponse {
        status: "ok",
        environment,
        timestamp_utc: Utc::now(),
        database,
        mdns: mdns_status,
        wan: wan_status,
        vpn: vpn_status,
    }))
}

async fn check_database(state: &AppState) -> DatabaseHealth {
    let start = Instant::now();
    let driver = state.db_driver.as_str();

    match sqlx::query("SELECT 1").fetch_one(&state.db_pool).await {
        Ok(row) => {
            let _: i64 = row.get(0);
            DatabaseHealth {
                status: "ok",
                driver,
                latency_ms: Some(start.elapsed().as_millis()),
                error: None,
            }
        }
        Err(err) => DatabaseHealth {
            status: "error",
            driver,
            latency_ms: Some(start.elapsed().as_millis()),
            error: Some(err.to_string()),
        },
    }
}

async fn check_wan(state: &AppState) -> WanHealth {
    if !state.settings.network.wan_enabled {
        return WanHealth {
            status: "disabled",
            wan_direct_endpoint: None,
        };
    }

    let wan_direct_endpoint: Option<String> = sqlx::query_scalar(
        "SELECT wan_direct_endpoint FROM server_registry ORDER BY last_seen_at DESC LIMIT 1",
    )
    .fetch_optional(&state.db_pool)
    .await
    .ok()
    .flatten();

    WanHealth {
        status: if wan_direct_endpoint.is_some() {
            "ok"
        } else {
            "unknown"
        },
        wan_direct_endpoint,
    }
}

fn check_mdns(state: &AppState) -> MdnsHealth {
    if !state.settings.network.mdns_enabled {
        return MdnsHealth {
            status: "disabled",
            name: None,
        };
    }

    let active = state.mdns_active.load(std::sync::atomic::Ordering::Relaxed);
    MdnsHealth {
        status: if active { "ok" } else { "error" },
        name: Some(state.settings.network.mdns_name.clone()),
    }
}

fn check_vpn(state: &AppState) -> VpnHealth {
    if !state.settings.network.vpn.detect_host_vpn {
        return VpnHealth {
            status: "disabled",
            host_vpn_detected: false,
            host_vpn_interfaces: Vec::new(),
            warning: None,
        };
    }
    let status = crate::network::vpn::detect_host_vpn();
    VpnHealth {
        status: if status.detected { "warning" } else { "ok" },
        host_vpn_detected: status.detected,
        host_vpn_interfaces: status.interfaces,
        warning: status.warning,
    }
}

pub async fn metrics() -> impl IntoResponse {
    let body = metrics::gather();
    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        body,
    )
}
