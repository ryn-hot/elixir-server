use std::collections::HashSet;

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
    if let Ok(addrs) = local_ip_address::list_afinet_netifas() {
        for (name, _) in addrs {
            if is_probable_vpn_interface(&name) && seen.insert(name.clone()) {
                matches.push(name);
            }
        }
    }
    matches.sort();
    matches
}

fn is_probable_vpn_interface(name: &str) -> bool {
    let lowered = name.trim().to_ascii_lowercase();
    if lowered.is_empty() {
        return false;
    }
    lowered.starts_with("utun")
        || lowered.starts_with("tun")
        || lowered.starts_with("tap")
        || lowered.starts_with("wg")
        || lowered.starts_with("tailscale")
        || lowered.starts_with("nordlynx")
        || lowered.starts_with("zt")
        || lowered.contains("proton")
        || lowered.contains("surfshark")
        || lowered.contains("warp")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_common_vpn_interface_names() {
        assert!(is_probable_vpn_interface("utun4"));
        assert!(is_probable_vpn_interface("wg0"));
        assert!(is_probable_vpn_interface("tailscale0"));
        assert!(!is_probable_vpn_interface("eth0"));
    }
}
