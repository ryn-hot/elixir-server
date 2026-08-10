use std::time::Instant;

use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::Row;

use crate::{
    http::error::ApiResult,
    metrics,
    runtime::health::{
        DockerRuntimeHealthSnapshot, DockerRuntimeHealthState, DockerRuntimeSubsystemImpact,
        docker_auto_reset_cooldown_seconds, docker_auto_reset_max_attempts_per_window,
        docker_auto_reset_window_seconds, docker_runtime_affected_subsystems,
    },
    state::AppState,
};

#[derive(Serialize)]
pub struct HealthResponse {
    status: &'static str,
    environment: &'static str,
    timestamp_utc: DateTime<Utc>,
    database: DatabaseHealth,
    runtime: RuntimeHealth,
    docker_runtime: RuntimeHealth,
    mdns: MdnsHealth,
    wan: WanHealth,
    vpn: VpnHealth,
    anime_inference: crate::anime_matching::AnimeInferenceSnapshot,
}

#[derive(Serialize)]
pub struct DatabaseHealth {
    status: &'static str,
    driver: &'static str,
    latency_ms: Option<u128>,
    error: Option<String>,
}

#[derive(Clone, Serialize)]
pub struct RuntimeHealth {
    state: &'static str,
    status: &'static str,
    code: Option<String>,
    reason: Option<String>,
    until: Option<DateTime<Utc>>,
    host_warning: Option<String>,
    reboot_recommended: bool,
    dependency_actions_deferred_until: Option<DateTime<Utc>>,
    quarantined_instances: usize,
    last_failure: Option<RuntimeLastFailure>,
    last_failure_code: Option<String>,
    last_failure_reason: Option<String>,
    last_failure_at: Option<DateTime<Utc>>,
    last_reset_attempt_at: Option<DateTime<Utc>>,
    auto_reset_budget: RuntimeAutoResetBudget,
    affected_subsystems: Vec<DockerRuntimeSubsystemImpact>,
}

#[derive(Clone, Serialize)]
pub struct RuntimeLastFailure {
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    at: Option<DateTime<Utc>>,
}

#[derive(Clone, Serialize)]
pub struct RuntimeAutoResetBudget {
    attempts_used: u32,
    attempts_allowed: u32,
    window_seconds: i64,
    cooldown_seconds: i64,
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
    let runtime = check_runtime(&app_state);
    let mdns_status = check_mdns(&app_state);
    let wan_status = check_wan(&app_state).await;
    let vpn_status = check_vpn(&app_state);
    let anime_inference = app_state.anime_inference.snapshot().await;

    Ok(Json(HealthResponse {
        status: "ok",
        environment,
        timestamp_utc: Utc::now(),
        database,
        runtime: runtime.clone(),
        docker_runtime: runtime,
        mdns: mdns_status,
        wan: wan_status,
        vpn: vpn_status,
        anime_inference,
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

fn check_runtime(state: &AppState) -> RuntimeHealth {
    RuntimeHealth::from_snapshot(state.orchestrator.docker_runtime_snapshot())
}

impl RuntimeHealth {
    fn from_snapshot(snapshot: DockerRuntimeHealthSnapshot) -> Self {
        let state = match snapshot.state {
            DockerRuntimeHealthState::Healthy => "healthy",
            DockerRuntimeHealthState::Recovering => "recovering",
            DockerRuntimeHealthState::Degraded => "degraded",
        };
        let show_active_runtime_issue =
            !matches!(snapshot.state, DockerRuntimeHealthState::Healthy);
        let last_failure = runtime_last_failure(&snapshot);
        let affected_subsystems = docker_runtime_affected_subsystems(&snapshot);

        Self {
            state,
            status: state,
            code: show_active_runtime_issue.then_some(snapshot.code).flatten(),
            reason: show_active_runtime_issue
                .then_some(snapshot.reason)
                .flatten(),
            until: snapshot.until,
            host_warning: snapshot.host_warning,
            reboot_recommended: snapshot.reboot_recommended,
            dependency_actions_deferred_until: snapshot.dependency_actions_deferred_until,
            quarantined_instances: snapshot.quarantined_instances.len(),
            last_failure,
            last_failure_code: snapshot.last_failure_code,
            last_failure_reason: snapshot.last_failure_reason,
            last_failure_at: snapshot.last_failure_at,
            last_reset_attempt_at: snapshot.last_reset_attempt_at,
            auto_reset_budget: RuntimeAutoResetBudget {
                attempts_used: snapshot.auto_reset_attempts_in_window,
                attempts_allowed: docker_auto_reset_max_attempts_per_window(),
                window_seconds: docker_auto_reset_window_seconds(),
                cooldown_seconds: docker_auto_reset_cooldown_seconds(),
            },
            affected_subsystems,
        }
    }
}

fn runtime_last_failure(snapshot: &DockerRuntimeHealthSnapshot) -> Option<RuntimeLastFailure> {
    if snapshot.last_failure_code.is_none()
        && snapshot.last_failure_reason.is_none()
        && snapshot.last_failure_at.is_none()
    {
        return None;
    }

    Some(RuntimeLastFailure {
        code: snapshot.last_failure_code.clone(),
        reason: snapshot.last_failure_reason.clone(),
        at: snapshot.last_failure_at,
    })
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
