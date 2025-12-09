use axum::{Json, extract::State};
use serde::{Deserialize, Serialize};

use crate::{
    http::error::ApiResult,
    state::{AppState, RegistryEntry},
};

#[derive(Debug, Deserialize)]
pub struct RegisterServerRequest {
    pub device_name: String,
    pub lan_addresses: Vec<String>,
    pub wan_direct_endpoint: Option<String>,
    pub overlay_endpoint: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ServersResponse {
    pub servers: Vec<RegistryEntry>,
}

pub async fn register(
    State(state): State<AppState>,
    Json(body): Json<RegisterServerRequest>,
) -> ApiResult<Json<&'static str>> {
    let entry = RegistryEntry {
        server_id: uuid::Uuid::new_v4().to_string(),
        device_name: body.device_name,
        lan_addresses: body.lan_addresses,
        wan_direct_endpoint: body.wan_direct_endpoint,
        overlay_endpoint: body.overlay_endpoint,
        status: "online",
    };

    {
        let mut guard = state.server_registry.write().await;
        guard.retain(|e| e.device_name != entry.device_name);
        guard.push(entry);
    }

    Ok(Json("ok"))
}

pub async fn list(State(state): State<AppState>) -> ApiResult<Json<ServersResponse>> {
    let guard = state.server_registry.read().await;
    Ok(Json(ServersResponse {
        servers: guard.clone(),
    }))
}
