use std::time::Instant;

use axum::{Json, extract::State};
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::Row;

use crate::{http::error::ApiResult, state::AppState};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthResponse {
    status: &'static str,
    environment: &'static str,
    timestamp_utc: DateTime<Utc>,
    database: DatabaseHealth,
    mdns: MdnsHealth,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseHealth {
    status: &'static str,
    driver: &'static str,
    latency_ms: Option<u128>,
    error: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MdnsHealth {
    status: &'static str,
    name: Option<String>,
}

pub async fn healthcheck(State(app_state): State<AppState>) -> ApiResult<Json<HealthResponse>> {
    let environment = app_state.settings.environment.as_str();
    let database = check_database(&app_state).await;
    let mdns_status = check_mdns(&app_state);

    Ok(Json(HealthResponse {
        status: "ok",
        environment,
        timestamp_utc: Utc::now(),
        database,
        mdns: mdns_status,
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
