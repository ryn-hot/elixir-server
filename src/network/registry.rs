use anyhow::Result;
use sqlx::AnyPool;
use uuid::Uuid;

use crate::config::Settings;

/// Ensure a server_instances row exists for the given user and return its id.
pub async fn ensure_server_instance(
    pool: &AnyPool,
    settings: &Settings,
    user_id: Uuid,
) -> Result<Uuid> {
    if let Some(existing) = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT id FROM server_instances WHERE user_id = ? LIMIT 1",
    )
    .bind(user_id.to_string())
    .fetch_optional(pool)
    .await?
    {
        return Ok(Uuid::parse_str(&existing)?);
    }

    let server_id = Uuid::new_v4();
    let device_name = settings.network.mdns_name.clone();
    let host = if settings.server.host == "0.0.0.0" {
        "127.0.0.1".to_string()
    } else {
        settings.server.host.clone()
    };
    let lan_address = format!("{}:{}", host, settings.server.port);
    let lan_addresses =
        serde_json::to_string(&vec![lan_address]).unwrap_or_else(|_| "[]".to_string());

    sqlx::query::<sqlx::Any>("INSERT INTO server_instances (id, user_id, device_name, lan_addresses, last_seen_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)")
        .bind(server_id.to_string())
        .bind(user_id.to_string())
        .bind(device_name)
        .bind(lan_addresses)
        .execute(pool)
        .await?;

    Ok(server_id)
}
