use anyhow::Context;
use reqwest::Client;
use uuid::Uuid;

use crate::{metrics::WAN_EVENTS, network::registry::ensure_server_instance, state::AppState};

pub fn start_wan_tasks(state: AppState) {
    if !state.settings.network.wan_enabled {
        return;
    }

    tokio::spawn(async move {
        if let Err(err) = attempt_wan_registration(&state).await {
            WAN_EVENTS.with_label_values(&["register", "error"]).inc();
            tracing::warn!("WAN setup failed: {err}");
        } else {
            WAN_EVENTS.with_label_values(&["register", "ok"]).inc();
        }
    });
}

async fn attempt_wan_registration(state: &AppState) -> anyhow::Result<()> {
    let port = state.settings.server.port;

    // Try UPnP/NAT-PMP to open a port and learn external IP; fall back to HTTP public IP.
    let upnp_ip = try_upnp_map(port).await.unwrap_or_else(|e| {
        WAN_EVENTS.with_label_values(&["upnp", "error"]).inc();
        tracing::warn!("WAN: UPnP mapping failed: {e}");
        None
    });
    if upnp_ip.is_some() {
        WAN_EVENTS.with_label_values(&["upnp", "ok"]).inc();
    }

    let public_ip = upnp_ip.or(fetch_public_ip().await.unwrap_or_else(|e| {
        WAN_EVENTS.with_label_values(&["public_ip", "error"]).inc();
        tracing::warn!("WAN: public IP fetch failed: {e}");
        None
    }));

    let wan_endpoint = public_ip.map(|ip| format!("{ip}:{port}"));
    if wan_endpoint.is_none() {
        tracing::info!("WAN: no public IP discovered; registering without wan_direct_endpoint");
    }

    // Choose a LAN address to advertise.
    let host = if state.settings.server.host == "0.0.0.0" {
        "127.0.0.1".to_string()
    } else {
        state.settings.server.host.clone()
    };
    let lan_addresses = vec![format!("{}:{}", host, port)];

    // Grab any user to own this server instance; best-effort.
    let user_id_raw: Option<String> = sqlx::query_scalar("SELECT id FROM users LIMIT 1")
        .fetch_optional(&state.db_pool)
        .await?;
    let Some(user_id_raw) = user_id_raw else {
        tracing::info!("WAN: no users present yet; skipping auto-register");
        return Ok(());
    };
    let user_id = Uuid::parse_str(&user_id_raw)?;

    let server_id = ensure_server_instance(&state.db_pool, &state.settings, user_id).await?;
    let token = state
        .auth_service
        .issue_access_token(user_id)
        .context("issuing internal access token")?;

    let payload = serde_json::json!({
        "server_id": server_id.to_string(),
        "device_name": state.settings.network.mdns_name,
        "lan_addresses": lan_addresses,
        "wan_direct_endpoint": wan_endpoint,
        "overlay_endpoint": null
    });

    let url = format!(
        "http://127.0.0.1:{}/api/v1/servers/register",
        state.settings.server.port
    );
    let client = Client::new();
    let resp = client
        .post(&url)
        .bearer_auth(&token.token)
        .json(&payload)
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("register failed: {} body={}", status, body);
    }

    tracing::info!(
        lan = ?payload["lan_addresses"],
        wan = ?payload["wan_direct_endpoint"],
        server_id = %server_id,
        "WAN auto-registration complete"
    );

    Ok(())
}

async fn try_upnp_map(port: u16) -> anyhow::Result<Option<String>> {
    let handle = tokio::task::spawn_blocking(move || -> anyhow::Result<Option<String>> {
        let gateway = igd::search_gateway(Default::default())?;
        let local_addr = local_ipv4().unwrap_or_else(|| "0.0.0.0".parse().unwrap());
        let socket = std::net::SocketAddrV4::new(local_addr, port);
        let _ = gateway.add_port(
            igd::PortMappingProtocol::TCP,
            port,
            socket,
            3600,
            "elixir-media",
        );
        if let Ok(ext) = gateway.get_external_ip() {
            return Ok(Some(ext.to_string()));
        }
        // Some gateways don't support external IP; return none.
        Ok(None)
    });

    match handle.await {
        Ok(res) => Ok(res.unwrap_or(None)),
        Err(_) => Ok(None),
    }
}

async fn fetch_public_ip() -> anyhow::Result<Option<String>> {
    let client = Client::new();
    let endpoints = vec![
        "https://api.ipify.org",
        "https://ifconfig.me/ip",
        "https://checkip.amazonaws.com",
    ];

    for url in endpoints {
        let resp = client.get(url).send().await;
        match resp {
            Ok(r) => {
                if !r.status().is_success() {
                    continue;
                }
                let text = r.text().await.unwrap_or_default();
                let trimmed = text.trim();
                if !trimmed.is_empty() {
                    return Ok(Some(trimmed.to_string()));
                }
            }
            Err(_) => continue,
        }
    }
    Ok(None)
}

fn local_ipv4() -> Option<std::net::Ipv4Addr> {
    let addrs = local_ip_address::list_afinet_netifas().ok()?;
    for (_iface, ip) in addrs {
        if let std::net::IpAddr::V4(v4) = ip {
            return Some(v4);
        }
    }
    None
}
