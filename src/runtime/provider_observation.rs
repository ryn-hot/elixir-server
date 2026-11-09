//! Bounded provider telemetry DATA. Only the original owner plus native
//! inspection can establish its origin; parsing never grants packet traffic.
use anyhow::{Context, Result, ensure};
use elixir_marketplace::{
    canonical,
    packet_guard::{PacketPolicy, VpnEndpoint, public_ip},
};
use serde_json::Value;
use std::{
    collections::BTreeSet,
    net::{IpAddr, SocketAddr},
};

#[derive(Clone, PartialEq, Eq)]
pub struct ProviderTransport {
    pub namespace_inode: u64,
    pub endpoint: VpnEndpoint,
    pub tunnel_addresses: Vec<IpAddr>,
}
impl ProviderTransport {
    pub(crate) fn require_policy(&self, namespace: u64, policy: &PacketPolicy) -> Result<()> {
        let mut expected = policy.tunnel_addresses.clone();
        expected.sort();
        let mut actual = self.tunnel_addresses.clone();
        actual.sort();
        ensure!(
            self.namespace_inode == namespace
                && actual == expected
                && policy.endpoints == [self.endpoint.clone()],
            "provider transport changed"
        );
        Ok(())
    }
}

pub(crate) struct SocketObservation {
    pub endpoint: VpnEndpoint,
    pub pid: u32,
}

pub(crate) fn socket(stats: &str, sockets: &str) -> Result<SocketObservation> {
    // Vendor telemetry is NOT the signed marketplace protocol: ignored stats
    // legitimately contain fractional loss/latency values. A typed projection
    // still rejects duplicate security-relevant fields, without weakening JCS.
    #[derive(serde::Deserialize)]
    struct Stats {
        v4_endpoint: IpAddr,
        v6_endpoint: IpAddr,
        warp_is_on: bool,
        protocol: String,
    }
    ensure!(stats.len() <= 65536, "provider stats bounds");
    let stats: Stats =
        serde_json::from_str(stats).map_err(|_| anyhow::anyhow!("invalid provider stats"))?;
    ensure!(
        stats.warp_is_on
            && stats.protocol == "MASQUE (HTTP/3)"
            && stats.v4_endpoint.is_ipv4()
            && stats.v6_endpoint.is_ipv6(),
        "provider tunnel is not active MASQUE over HTTP/3"
    );
    // WARP reports IPs, with ::/0.0.0.0 for an inactive family, NOT ports.
    // Take both ports from the actual connected service socket, never guess
    // 443 or infer an exception from this telemetry alone.
    let advertised: BTreeSet<IpAddr> = [stats.v4_endpoint, stats.v6_endpoint]
        .into_iter()
        .filter(|ip| public_ip(*ip))
        .collect();
    ensure!(
        !advertised.is_empty() && sockets.len() <= 65536 && sockets.lines().count() <= 128,
        "provider socket observation bounds"
    );
    let mut selected = None;
    for line in sockets.lines() {
        let fields: Vec<_> = line.split_whitespace().collect();
        if !fields.iter().any(|s| s.contains("\"warp-svc\"")) {
            continue;
        }
        // ss -a includes the state column AND unconnected service sockets.
        // Those sockets cannot identify the active encrypted transport.
        if fields.first() == Some(&"UNCONN") {
            continue;
        }
        ensure!(
            fields.len() == 6
                && fields[0] == "ESTAB"
                && fields[1].parse::<u64>().is_ok()
                && fields[2].parse::<u64>().is_ok(),
            "provider UDP socket shape"
        );
        let local: SocketAddr = fields[3].parse().context("provider local UDP tuple")?;
        let remote: SocketAddr = fields[4].parse().context("provider remote UDP tuple")?;
        if !advertised.contains(&remote.ip()) {
            continue;
        }
        let process = fields[5]
            .strip_prefix("users:((\"warp-svc\",pid=")
            .and_then(|v| v.strip_suffix("))"))
            .context("provider socket process shape")?;
        let (pid, fd) = process
            .split_once(",fd=")
            .context("provider socket process binding")?;
        let pid: u32 = pid.parse().context("provider service PID")?;
        ensure!(
            pid > 1
                && fd.parse::<u32>().is_ok()
                && public_ip(remote.ip())
                && remote.port() != 0
                && local.port() >= 1024
                && local.is_ipv4() == remote.is_ipv4()
                && !local.ip().is_unspecified()
                && selected.is_none(),
            "provider UDP socket is ambiguous or unprotected"
        );
        selected = Some(SocketObservation {
            endpoint: VpnEndpoint {
                address: remote.ip(),
                port: remote.port(),
                local_port: local.port(),
            },
            pid,
        });
    }
    selected.context("active provider-owned UDP socket missing")
}

pub(crate) fn addresses(bytes: &str) -> Result<Vec<IpAddr>> {
    let value: Value = canonical::parse(bytes.as_bytes(), 8192)?;
    let rows = value
        .as_array()
        .filter(|a| a.len() == 1)
        .context("provider link observation")?;
    let row = &rows[0];
    ensure!(
        row["ifname"] == "CloudflareWARP" && row["ifindex"].as_u64().is_some_and(|v| v > 0),
        "provider link identity"
    );
    let entries = row["addr_info"]
        .as_array()
        .filter(|a| a.len() <= 8)
        .context("provider address bounds")?;
    let mut result = BTreeSet::new();
    for entry in entries.iter().filter(|v| v["scope"] == "global") {
        let ip: IpAddr = entry["local"]
            .as_str()
            .context("provider address")?
            .parse()?;
        ensure!(
            !ip.is_unspecified() && !ip.is_loopback() && !ip.is_multicast() && result.insert(ip),
            "provider tunnel address shape"
        );
    }
    ensure!(
        !result.is_empty() && result.len() <= 4,
        "provider tunnel address bounds"
    );
    Ok(result.into_iter().collect())
}

/// No raw trace or exit address is printed. The fixed HTTPS endpoint supplies
/// this DATA; origin, UID, DNS/TLS verification and firewall ownership are
/// established by the closed runtime/owner path, not by these text fields.
pub(crate) fn trace(bytes: &str, ipv6: bool) -> Result<()> {
    ensure!(
        bytes.len() <= 4096 && bytes.lines().count() <= 64,
        "provider TLS trace bounds"
    );
    let mut fields = std::collections::BTreeMap::new();
    for line in bytes.lines() {
        let (key, value) = line.split_once('=').context("provider TLS trace shape")?;
        ensure!(
            !key.is_empty() && fields.insert(key, value).is_none(),
            "duplicate provider trace field"
        );
    }
    ensure!(
        fields.get("warp") == Some(&"on")
            && fields.get("h") == Some(&"www.cloudflare.com")
            && fields.get("tls") == Some(&"TLSv1.3"),
        "protected provider TLS unavailable"
    );
    let exit: IpAddr = fields
        .get("ip")
        .context("provider exit address missing")?
        .parse()?;
    ensure!(
        public_ip(exit) && exit.is_ipv6() == ipv6,
        "provider exit family differs"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    const STATS: &str = r#"{"v4_endpoint":"162.159.198.2","v6_endpoint":"::","warp_is_on":true,"protocol":"MASQUE (HTTP/3)","estimated_loss":0.024635557}"#;
    const SOCKET: &str =
        "ESTAB 0 0 172.20.0.2:33730 162.159.198.2:443 users:((\"warp-svc\",pid=41,fd=24))\n";
    #[test]
    fn exact_connected_service_tuple_not_a_guessed_endpoint_or_process_name() {
        let good = socket(STATS, SOCKET).unwrap();
        assert_eq!(
            (good.pid, good.endpoint.port, good.endpoint.local_port),
            (41, 443, 33730)
        );
        for bad in [
            SOCKET.repeat(2),
            SOCKET.replace("198.2", "198.3"),
            SOCKET.replace("33730", "0"),
            SOCKET.replace("pid=41", "pid=0"),
            SOCKET.replace("ESTAB", "UNCONN"),
            SOCKET.replace("warp-svc", "worker"),
            SOCKET.replace("fd=24", "fd=24),(\"worker\",pid=42,fd=5"),
            SOCKET.replace("172.20.0.2", "0.0.0.0"),
        ] {
            assert!(socket(STATS, &bad).is_err());
        }
        for bad in [
            STATS.replace("MASQUE", "WireGuard"),
            STATS.replace("HTTP/3", "HTTP/2"),
            STATS.replace("true", "false"),
            STATS.replace("162.159.198.2", "162.159.198.3"),
            STATS.replace("162.159.198.2", "127.0.0.1"),
            STATS.replace(
                "\"warp_is_on\":true",
                "\"warp_is_on\":true,\"warp_is_on\":false",
            ),
        ] {
            assert!(socket(&bad, SOCKET).is_err());
        }
        let different_ports = socket(
            STATS,
            &SOCKET.replace("33730", "12345").replace(":443", ":2408"),
        )
        .unwrap();
        assert_eq!(
            (
                different_ports.endpoint.local_port,
                different_ports.endpoint.port
            ),
            (12345, 2408)
        );
        let unrelated = format!(
            "UNCONN 0 0 0.0.0.0:5353 0.0.0.0:* users:((\"warp-svc\",pid=41,fd=19))\n{}{}",
            SOCKET.replace("162.159.198.2", "1.1.1.1"),
            SOCKET
        );
        assert_eq!(socket(STATS, &unrelated).unwrap().endpoint, good.endpoint);
    }
    #[test]
    fn addresses_and_protected_trace_are_bounded_and_reject_duplicates_or_downgrades() {
        let link = r#"[{"ifname":"CloudflareWARP","ifindex":3,"addr_info":[{"scope":"global","local":"172.16.0.2"},{"scope":"link","local":"fe80::1"}]}]"#;
        assert_eq!(
            addresses(link).unwrap(),
            ["172.16.0.2".parse::<IpAddr>().unwrap()]
        );
        assert!(addresses(&link.replace("172.16.0.2", "0.0.0.0")).is_err());
        assert!(addresses(&link.replace("CloudflareWARP", "dummy0")).is_err());
        let good = "h=www.cloudflare.com\nip=8.8.8.8\nwarp=on\ntls=TLSv1.3\n";
        trace(good, false).unwrap();
        for bad in [
            good.replace("on", "off"),
            good.replace("TLSv1.3", "TLSv1.2"),
            good.replace("8.8.8.8", "192.168.1.1"),
            format!("{good}warp=on\n"),
        ] {
            assert!(trace(&bad, false).is_err());
        }
        assert!(trace(good, true).is_err());
    }
}
