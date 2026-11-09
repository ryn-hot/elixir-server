//! A saved, purpose-bound WireGuard profile, not a downloader gateway, WARP
//! registration, selected container image, health receipt or packet allowance.
use super::*;
use std::{collections::BTreeMap, net::SocketAddrV4};

pub(super) struct Binding {
    database: Database,
    secrets: SecretsManager,
    profile: String,
    material: Digest,
    device: String,
}
impl Binding {
    pub(super) async fn current(&self) -> Result<()> {
        let current = crate::network::protection::marketplace_network_material(
            &self.database.pool,
            &self.secrets,
            &self.profile,
        )
        .await?;
        ensure!(
            Digest::of(current.as_bytes()) == self.material,
            "marketplace connection configuration changed"
        );
        crate::network::vpn_identity::current(
            &self.database.pool,
            &self.device,
            crate::network::vpn_identity::Purpose::MarketplaceNetwork,
        )
        .await
    }
}
pub(super) async fn current(binding: &Option<Binding>) -> Result<()> {
    if let Some(binding) = binding {
        timeout(Duration::from_secs(10), binding.current())
            .await
            .context("saved connection configuration deadline")??;
    }
    Ok(())
}

struct Configuration {
    endpoint: SocketAddrV4,
    local_port: u16,
    addresses: Vec<Ipv4Addr>,
    mtu: u32,
    keepalive: u16,
    peer: String,
    secrets: WireguardSecrets,
}
fn key(value: &str) -> Result<Zeroizing<[u8; 32]>> {
    let bytes = Zeroizing::new(
        STANDARD
            .decode(value)
            .map_err(|_| anyhow::anyhow!("invalid WireGuard key encoding"))?,
    );
    ensure!(
        bytes.len() == 32
            && STANDARD.encode(&bytes) == value
            && bytes.iter().any(|byte| *byte != 0),
        "invalid WireGuard key"
    );
    let mut key = Zeroizing::new([0; 32]);
    key.copy_from_slice(&bytes);
    Ok(key)
}
fn parse(text: &str) -> Result<Configuration> {
    ensure!(
        !text.is_empty() && text.len() <= 256 * 1024 && !text.contains('\0'),
        "invalid saved WireGuard configuration bounds"
    );
    let mut fields = BTreeMap::new();
    let mut section = None;
    let mut interface = false;
    let mut peer = false;
    for line in text
        .lines()
        .map(|line| line.split('#').next().unwrap_or("").trim())
        .filter(|line| !line.is_empty())
    {
        if line.starts_with('[') {
            if line.eq_ignore_ascii_case("[Interface]") && !interface && !peer {
                interface = true;
                section = Some("interface");
            } else if line.eq_ignore_ascii_case("[Peer]") && interface && !peer {
                peer = true;
                section = Some("peer");
            } else {
                anyhow::bail!("saved marketplace WireGuard requires one interface and one peer");
            }
            continue;
        }
        let (name, value) = line
            .split_once('=')
            .context("invalid saved WireGuard field")?;
        let name = name.trim().to_ascii_lowercase();
        let value = value.trim();
        let section = section.context("saved WireGuard fields require a section")?;
        let allowed = match section {
            "interface" => matches!(
                name.as_str(),
                "privatekey" | "address" | "listenport" | "mtu" | "dns"
            ),
            _ => matches!(
                name.as_str(),
                "publickey" | "presharedkey" | "allowedips" | "endpoint" | "persistentkeepalive"
            ),
        };
        ensure!(
            allowed && !value.is_empty() && fields.insert((section, name), value).is_none(),
            "unsupported or duplicate saved WireGuard field"
        );
    }
    ensure!(
        interface && peer,
        "saved WireGuard interface or peer missing"
    );
    let get = |section, name| fields.get(&(section, String::from(name))).copied();
    let required =
        |section, name| get(section, name).context("required saved WireGuard field missing");
    let secrets = WireguardSecrets::from_keys(
        key(required("interface", "privatekey")?)?,
        get("peer", "presharedkey").map(key).transpose()?,
    )?;
    let peer = peer_key(required("peer", "publickey")?)?;
    ensure!(
        required("peer", "allowedips")?.trim() == "0.0.0.0/0",
        "saved marketplace connection currently requires an IPv4 full tunnel; IPv6 stays blocked"
    );
    let endpoint: SocketAddrV4 = required("peer", "endpoint")?.parse().context(
        "saved marketplace VPN endpoint requires a literal public IPv4 address and port",
    )?;
    let mut addresses = Vec::new();
    for address in required("interface", "address")?.split(',').map(str::trim) {
        let (address, prefix) = address
            .split_once('/')
            .context("saved WireGuard address requires its prefix")?;
        let address: Ipv4Addr = address
            .parse()
            .context("saved marketplace connection currently requires IPv4 tunnel addresses")?;
        let prefix: u8 = prefix
            .parse()
            .context("invalid saved WireGuard address prefix")?;
        ensure!(
            prefix <= 32 && !addresses.contains(&address) && addresses.len() < 4,
            "invalid saved WireGuard address allocation"
        );
        addresses.push(address);
    }
    ensure!(!addresses.is_empty(), "saved WireGuard address missing");
    if let Some(dns) = get("interface", "dns") {
        // No DNS resolution or search suffixes during preparation. The native
        // fixed connectivity helper/worker retains its protected resolver policy.
        ensure!(
            dns.split(',').count() <= 3
                && dns
                    .split(',')
                    .all(|value| value.trim().parse::<std::net::IpAddr>().is_ok()),
            "saved WireGuard DNS must contain only bounded IP literals"
        );
    }
    Ok(Configuration {
        endpoint,
        addresses,
        secrets,
        peer,
        local_port: get("interface", "listenport")
            .map(str::parse)
            .transpose()?
            .unwrap_or(51821),
        mtu: get("interface", "mtu")
            .map(str::parse)
            .transpose()?
            .unwrap_or(1420),
        keepalive: get("peer", "persistentkeepalive")
            .map(str::parse)
            .transpose()?
            .unwrap_or(25),
    })
}
fn prepared(config: Configuration, limits: &OrdinaryLimits) -> Result<PreparedVpn> {
    let policy = PacketPolicy {
        schema_version: 1,
        epoch: Digest::of(b"unassigned-saved-startup-epoch"),
        tunnel_interface: "wg0".into(),
        tunnel_addresses: config
            .addresses
            .iter()
            .map(|address| (*address).into())
            .collect(),
        endpoints: vec![VpnEndpoint {
            address: (*config.endpoint.ip()).into(),
            port: config.endpoint.port(),
            local_port: config.local_port,
        }],
        lease_seconds: 15,
        upload_bytes_per_second: limits.upload_bytes_per_second,
        download_bytes_per_second: limits.download_bytes_per_second,
    };
    let device = config.secrets.device_public_key()?;
    let setup = WireguardSetup {
        device_public_key: device.clone(),
        peer_public_key: config.peer.clone(),
        mtu: config.mtu,
        keepalive_seconds: config.keepalive,
    };
    setup.validate(&policy)?;
    Ok(PreparedVpn {
        setup,
        policy,
        secrets: config.secrets,
        identity: WireguardIdentity {
            device,
            peer: config.peer,
        },
        binding: None,
    })
}
pub(super) fn validate(text: &str) -> Result<()> {
    prepared(parse(text)?, &OrdinaryLimits::default())?;
    Ok(())
}
pub(super) async fn resolve(
    database: &Database,
    manager: &SecretsManager,
    id: &str,
    limits: &OrdinaryLimits,
) -> Result<PreparedVpn> {
    let material =
        crate::network::protection::marketplace_network_material(&database.pool, manager, id)
            .await?;
    let mut prepared = prepared(parse(&material)?, limits)?;
    crate::network::protection::reserve_marketplace_network_identity(
        &database.pool,
        manager,
        &prepared.identity.device,
    )
    .await?;
    let binding = Binding {
        database: database.clone(),
        secrets: manager.clone(),
        profile: id.into(),
        material: Digest::of(material.as_bytes()),
        device: prepared.identity.device.clone(),
    };
    binding.current().await?;
    prepared.binding = Some(binding);
    Ok(prepared)
}

#[cfg(test)]
mod tests;
