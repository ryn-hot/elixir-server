use anyhow::Result;
use mdns_sd::{ServiceDaemon, ServiceInfo};

use crate::config::ServerConfig;

/// Guard to keep the mdns-sd daemon alive for the lifetime of the process.
pub struct MdnsHandle {
    _daemon: ServiceDaemon,
}

pub fn start_mdns(config: &ServerConfig, service_name: &str) -> Result<MdnsHandle> {
    let daemon = ServiceDaemon::new()?;

    let service_type = "_elixir-media._tcp.local.";
    let host = format!("{}.local", config.host);
    let ip = "0.0.0.0";
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
