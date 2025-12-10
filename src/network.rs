use anyhow::Result;
use hostname;
use mdns_sd::{ServiceDaemon, ServiceInfo};

use crate::config::ServerConfig;

/// Guard to keep the mdns-sd daemon alive for the lifetime of the process.
pub struct MdnsHandle {
    _daemon: ServiceDaemon,
}

pub fn start_mdns(config: &ServerConfig, service_name: &str) -> Result<MdnsHandle> {
    let daemon = ServiceDaemon::new()?;

    let service_type = "_elixir-media._tcp.local.";
    let host_base = hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_else(|| "elixir-host".to_string());
    let host = if host_base.ends_with(".local") {
        format!("{host_base}.")
    } else {
        format!("{host_base}.local.")
    };
    let ip: std::net::IpAddr = if config.host == "0.0.0.0" {
        "127.0.0.1".parse().unwrap()
    } else {
        config
            .host
            .parse()
            .unwrap_or_else(|_| "127.0.0.1".parse().unwrap())
    };
    let properties = Vec::<mdns_sd::TxtProperty>::new();
    let info = ServiceInfo::new(
        service_type,
        service_name,
        &host,
        ip,
        config.port,
        properties,
    )?;

    daemon.register(info)?;
    Ok(MdnsHandle { _daemon: daemon })
}
