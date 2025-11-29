use axum::{Json, extract::State};
use serde::Serialize;

use crate::{http::error::ApiResult, state::AppState};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SettingsResponse {
    environment: String,
    server: ServerSettings,
    database: DatabaseSettings,
    telemetry: TelemetrySettings,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerSettings {
    host: String,
    port: u16,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseSettings {
    driver: &'static str,
    max_connections: u32,
    connect_timeout_seconds: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
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
