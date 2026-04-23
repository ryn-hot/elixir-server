use std::collections::HashSet;
use std::net::IpAddr;
#[cfg(target_os = "macos")]
use std::process::Command;

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct HostVpnStatus {
    pub detected: bool,
    pub interfaces: Vec<String>,
    pub warning: Option<String>,
}

pub fn detect_host_vpn() -> HostVpnStatus {
    let interfaces = detect_vpn_interfaces();
    if interfaces.is_empty() {
        return HostVpnStatus {
            detected: false,
            interfaces,
            warning: None,
        };
    }
    HostVpnStatus {
        detected: true,
        interfaces: interfaces.clone(),
        warning: Some(format!(
            "Host VPN interface(s) detected ({}). Full-tunnel VPN on the server can disrupt extension networking and reduce streaming performance.",
            interfaces.join(", ")
        )),
    }
}

pub fn detect_vpn_interfaces() -> Vec<String> {
    let mut seen = HashSet::new();
    let mut matches = Vec::new();
    let macos_active_vpn_service = macos_active_vpn_service_detected();
    if let Ok(addrs) = local_ip_address::list_afinet_netifas() {
        for (name, addr) in addrs {
            if interface_indicates_vpn(&name, addr, macos_active_vpn_service)
                && seen.insert(name.clone())
            {
                matches.push(name);
            }
        }
    }
    matches.sort();
    matches
}

fn interface_indicates_vpn(name: &str, addr: IpAddr, macos_active_vpn_service: bool) -> bool {
    let lowered = name.trim().to_ascii_lowercase();
    if lowered.is_empty() {
        return false;
    }

    if is_named_vpn_interface(&lowered) {
        return true;
    }

    if is_generic_tunnel_interface(&lowered) {
        return has_meaningful_tunnel_address(addr) || macos_active_vpn_service;
    }

    false
}

fn is_named_vpn_interface(lowered: &str) -> bool {
    lowered.starts_with("wg")
        || lowered.starts_with("tailscale")
        || lowered.starts_with("nordlynx")
        || lowered.starts_with("zt")
        || lowered.contains("proton")
        || lowered.contains("surfshark")
        || lowered.contains("warp")
}

fn is_generic_tunnel_interface(lowered: &str) -> bool {
    lowered.starts_with("utun") || lowered.starts_with("tun") || lowered.starts_with("tap")
}

fn has_meaningful_tunnel_address(addr: IpAddr) -> bool {
    match addr {
        IpAddr::V4(value) => {
            !(value.is_loopback() || value.is_link_local() || value.is_unspecified())
        }
        IpAddr::V6(value) => {
            !(value.is_loopback() || value.is_unicast_link_local() || value.is_unspecified())
        }
    }
}

#[cfg(target_os = "macos")]
fn macos_active_vpn_service_detected() -> bool {
    let output = match Command::new("scutil").args(["--nc", "list"]).output() {
        Ok(output) if output.status.success() => output,
        _ => return false,
    };
    let stdout = String::from_utf8_lossy(&output.stdout).to_ascii_lowercase();
    stdout
        .lines()
        .any(|line| line.contains("connected") || line.contains("connecting"))
}

#[cfg(not(target_os = "macos"))]
fn macos_active_vpn_service_detected() -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn detects_named_vpn_interfaces() {
        assert!(interface_indicates_vpn(
            "wg0",
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
            false
        ));
        assert!(interface_indicates_vpn(
            "tailscale0",
            IpAddr::V4(Ipv4Addr::new(100, 64, 0, 1)),
            false
        ));
        assert!(!interface_indicates_vpn(
            "eth0",
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 2)),
            false
        ));
    }

    #[test]
    fn generic_utun_without_real_signal_is_ignored() {
        assert!(!interface_indicates_vpn(
            "utun4",
            IpAddr::V6(Ipv6Addr::new(0xfe80, 0, 0, 0, 1, 2, 3, 4)),
            false
        ));
    }

    #[test]
    fn generic_utun_with_active_service_is_detected() {
        assert!(interface_indicates_vpn(
            "utun4",
            IpAddr::V6(Ipv6Addr::new(0xfe80, 0, 0, 0, 1, 2, 3, 4)),
            true
        ));
    }

    #[test]
    fn generic_utun_with_non_link_local_address_is_detected() {
        assert!(interface_indicates_vpn(
            "utun4",
            IpAddr::V4(Ipv4Addr::new(10, 8, 0, 2)),
            false
        ));
    }
}
