use axum::{Json, extract::State};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use uuid::Uuid;

use crate::{
    http::{
        auth::CurrentUser,
        error::{ApiError, ApiResult},
    },
    state::AppState,
};

#[derive(Debug, Deserialize)]
pub struct RegisterServerRequest {
    pub server_id: Option<String>,
    pub device_name: String,
    pub lan_addresses: Vec<String>,
    pub wan_direct_endpoint: Option<String>,
    pub overlay_endpoint: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ServersResponse {
    pub servers: Vec<RegistryEntry>,
}

#[derive(Debug, Serialize)]
pub struct RegistryEntry {
    pub server_id: String,
    pub device_name: String,
    pub lan_addresses: Vec<String>,
    pub wan_direct_endpoint: Option<String>,
    pub overlay_endpoint: Option<String>,
    pub status: String,
    pub last_seen_at: Option<String>,
}

pub async fn register(
    State(state): State<AppState>,
    user: CurrentUser,
    Json(body): Json<RegisterServerRequest>,
) -> ApiResult<Json<&'static str>> {
    let server_id = body
        .server_id
        .as_deref()
        .and_then(|s| Uuid::parse_str(s).ok())
        .unwrap_or_else(Uuid::new_v4);

    let lan_addresses =
        serde_json::to_string(&body.lan_addresses).unwrap_or_else(|_| "[]".to_string());

    sqlx::query::<sqlx::Any>("INSERT INTO server_registry (id, user_id, server_id, device_name, lan_addresses, wan_direct_endpoint, overlay_endpoint, status, last_seen_at) VALUES (?, ?, ?, ?, ?, ?, ?, 'online', CURRENT_TIMESTAMP) ON CONFLICT(user_id, server_id) DO UPDATE SET device_name = excluded.device_name, lan_addresses = excluded.lan_addresses, wan_direct_endpoint = excluded.wan_direct_endpoint, overlay_endpoint = excluded.overlay_endpoint, status = 'online', last_seen_at = CURRENT_TIMESTAMP")
        .bind(Uuid::new_v4().to_string())
        .bind(user.user_id.to_string())
        .bind(server_id.to_string())
        .bind(body.device_name)
        .bind(lan_addresses)
        .bind(body.wan_direct_endpoint)
        .bind(body.overlay_endpoint)
        .execute(&state.db_pool)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;

    Ok(Json("ok"))
}

pub async fn list(
    State(state): State<AppState>,
    user: CurrentUser,
) -> ApiResult<Json<ServersResponse>> {
    let rows = sqlx::query("SELECT server_id, device_name, lan_addresses, wan_direct_endpoint, overlay_endpoint, status, datetime(last_seen_at) as last_seen_at FROM server_registry WHERE user_id = ? ORDER BY last_seen_at DESC")
        .bind(user.user_id.to_string())
        .fetch_all(&state.db_pool)
        .await
        .map_err(|e| ApiError::internal(e.to_string()))?;

    let mut servers = Vec::new();
    for row in rows {
        let lan_addresses_raw: String = row.get("lan_addresses");
        let lan_addresses: Vec<String> =
            serde_json::from_str(&lan_addresses_raw).unwrap_or_default();
        servers.push(RegistryEntry {
            server_id: row.get("server_id"),
            device_name: row.get("device_name"),
            lan_addresses,
            wan_direct_endpoint: row.try_get("wan_direct_endpoint").ok(),
            overlay_endpoint: row.try_get("overlay_endpoint").ok(),
            status: row.try_get("status").unwrap_or("unknown".to_string()),
            last_seen_at: row.try_get("last_seen_at").ok(),
        });
    }

    Ok(Json(ServersResponse { servers }))
}
