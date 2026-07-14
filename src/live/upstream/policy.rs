use std::{
    collections::BTreeSet,
    fmt,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
};

use reqwest::Url;

use super::error::{Result, UpstreamErrorCode};

const MAX_URL_BYTES: usize = 4_096;
const MAX_RULES: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkScope {
    Public,
    PrivateLan,
}

#[derive(Clone, PartialEq, Eq)]
pub struct BlockedNetwork {
    network: IpAddr,
    prefix: u8,
}

impl fmt::Debug for BlockedNetwork {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BlockedNetwork")
            .field(
                "address_family",
                &if self.network.is_ipv4() {
                    "ipv4"
                } else {
                    "ipv6"
                },
            )
            .field("prefix", &self.prefix)
            .finish()
    }
}

impl BlockedNetwork {
    pub fn new(network: IpAddr, prefix: u8) -> Result<Self> {
        let maximum = if network.is_ipv4() { 32 } else { 128 };
        if prefix > maximum {
            return Err(UpstreamErrorCode::AddressForbidden.into());
        }
        Ok(Self {
            network: mask_address(network, prefix),
            prefix,
        })
    }

    fn contains(&self, address: IpAddr) -> bool {
        address.is_ipv4() == self.network.is_ipv4()
            && mask_address(address, self.prefix) == self.network
    }
}

#[derive(Clone, Default)]
pub struct LocalDestinationDenylist {
    addresses: BTreeSet<IpAddr>,
    networks: Vec<BlockedNetwork>,
}

impl fmt::Debug for LocalDestinationDenylist {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalDestinationDenylist")
            .field("address_count", &self.addresses.len())
            .field("network_count", &self.networks.len())
            .finish()
    }
}

impl LocalDestinationDenylist {
    pub fn new(addresses: Vec<IpAddr>, networks: Vec<BlockedNetwork>) -> Result<Self> {
        if addresses.len() > 256 || networks.len() > 256 {
            return Err(UpstreamErrorCode::AddressForbidden.into());
        }
        Ok(Self {
            addresses: addresses.into_iter().collect(),
            networks,
        })
    }

    pub fn empty() -> Self {
        Self::default()
    }

    fn is_empty(&self) -> bool {
        self.addresses.is_empty() && self.networks.is_empty()
    }

    fn contains(&self, address: IpAddr) -> bool {
        self.addresses.contains(&address)
            || self
                .networks
                .iter()
                .any(|network| network.contains(address))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PrivateLanGate {
    pub server_enabled: bool,
    pub provider_permission: bool,
    pub descriptor_requested: bool,
    pub owner_rule: bool,
}

impl PrivateLanGate {
    fn permits(self) -> bool {
        self.server_enabled
            && self.provider_permission
            && self.descriptor_requested
            && self.owner_rule
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct DestinationRule {
    scheme: String,
    host: String,
    port: u16,
    exact_path: String,
    network_scope: NetworkScope,
    allow_fetch: bool,
}

impl fmt::Debug for DestinationRule {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DestinationRule")
            .field("destination", &"<redacted>")
            .field("network_scope", &self.network_scope)
            .field("allow_fetch", &self.allow_fetch)
            .finish()
    }
}

impl DestinationRule {
    pub fn new(
        scheme: &str,
        host: &str,
        port: u16,
        exact_path: &str,
        network_scope: NetworkScope,
        allow_fetch: bool,
    ) -> Result<Self> {
        let scheme = scheme.to_ascii_lowercase();
        if !matches!(scheme.as_str(), "http" | "https") {
            return Err(UpstreamErrorCode::SchemeForbidden.into());
        }
        let host = normalize_host(host)?;
        if port == 0 {
            return Err(UpstreamErrorCode::PortForbidden.into());
        }
        if !valid_exact_path(exact_path) {
            return Err(UpstreamErrorCode::InvalidUrl.into());
        }
        Ok(Self {
            scheme,
            host,
            port,
            exact_path: exact_path.to_string(),
            network_scope,
            allow_fetch,
        })
    }

    fn matches(&self, target: &ValidatedUrl) -> bool {
        self.allow_fetch
            && self.scheme == target.scheme
            && self.host == target.host
            && self.port == target.port
            && self.exact_path == target.url.path()
    }
}

#[derive(Clone)]
pub struct DestinationPolicy {
    rules: Vec<DestinationRule>,
    private_lan: PrivateLanGate,
    allow_http: bool,
    local_denylist: LocalDestinationDenylist,
    #[cfg(test)]
    allow_fixture_loopback: bool,
}

impl fmt::Debug for DestinationPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DestinationPolicy")
            .field("rule_count", &self.rules.len())
            .field("private_lan", &self.private_lan)
            .field("allow_http", &self.allow_http)
            .field("local_denylist", &self.local_denylist)
            .finish()
    }
}

impl DestinationPolicy {
    pub fn new(
        rules: Vec<DestinationRule>,
        private_lan: PrivateLanGate,
        allow_http: bool,
        local_denylist: LocalDestinationDenylist,
    ) -> Result<Self> {
        if rules.is_empty() || rules.len() > MAX_RULES {
            return Err(UpstreamErrorCode::DestinationForbidden.into());
        }
        if rules
            .iter()
            .any(|rule| rule.network_scope == NetworkScope::PrivateLan)
            && local_denylist.is_empty()
        {
            return Err(UpstreamErrorCode::DestinationForbidden.into());
        }
        Ok(Self {
            rules,
            private_lan,
            allow_http,
            local_denylist,
            #[cfg(test)]
            allow_fixture_loopback: false,
        })
    }

    #[cfg(test)]
    pub(crate) fn allow_fixture_loopback(mut self) -> Self {
        self.allow_fixture_loopback = true;
        self
    }

    pub(crate) fn validate_initial(&self, raw: &str) -> Result<ValidatedUrl> {
        self.validate_url(raw, None)
    }

    pub(crate) fn validate_redirect(
        &self,
        current: &ValidatedUrl,
        location: &str,
    ) -> Result<ValidatedUrl> {
        if location.is_empty()
            || location.len() > MAX_URL_BYTES
            || location.chars().any(char::is_control)
        {
            return Err(UpstreamErrorCode::RedirectInvalid.into());
        }
        let joined = current
            .url
            .join(location)
            .map_err(|_| UpstreamErrorCode::RedirectInvalid)?;
        self.validate_parsed(joined, Some(current))
    }

    fn validate_url(&self, raw: &str, previous: Option<&ValidatedUrl>) -> Result<ValidatedUrl> {
        if raw.is_empty() || raw.len() > MAX_URL_BYTES || raw.chars().any(char::is_control) {
            return Err(UpstreamErrorCode::InvalidUrl.into());
        }
        let url = Url::parse(raw).map_err(|_| UpstreamErrorCode::InvalidUrl)?;
        self.validate_parsed(url, previous)
    }

    fn validate_parsed(&self, url: Url, previous: Option<&ValidatedUrl>) -> Result<ValidatedUrl> {
        let scheme = url.scheme().to_ascii_lowercase();
        if scheme != "https" && !(scheme == "http" && self.allow_http) {
            return Err(UpstreamErrorCode::SchemeForbidden.into());
        }
        if previous.is_some_and(|value| value.scheme == "https" && scheme == "http") {
            return Err(UpstreamErrorCode::RedirectDowngrade.into());
        }
        if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
            return Err(UpstreamErrorCode::InvalidUrl.into());
        }
        if contains_encoded_control(url.path()) || url.query().is_some_and(contains_encoded_control)
        {
            return Err(UpstreamErrorCode::InvalidUrl.into());
        }
        let raw_host = url.host_str().ok_or(UpstreamErrorCode::HostForbidden)?;
        let host = normalize_host(raw_host)?;
        if forbidden_hostname(&host) {
            return Err(UpstreamErrorCode::HostForbidden.into());
        }
        let port = url
            .port_or_known_default()
            .ok_or(UpstreamErrorCode::PortForbidden)?;
        let candidate = ValidatedUrl {
            url,
            scheme,
            host,
            port,
        };
        if !self.rules.iter().any(|rule| rule.matches(&candidate)) {
            return Err(UpstreamErrorCode::DestinationForbidden.into());
        }
        Ok(candidate)
    }

    pub(crate) fn resolve_target(
        &self,
        target: ValidatedUrl,
        addresses: Vec<IpAddr>,
    ) -> Result<ResolvedTarget> {
        if addresses.is_empty() {
            return Err(UpstreamErrorCode::DnsEmpty.into());
        }
        let mut unique = BTreeSet::new();
        for address in addresses {
            unique.insert(address);
        }
        let mut scope = None;
        for address in &unique {
            if self.local_denylist.contains(*address) {
                return Err(UpstreamErrorCode::AddressForbidden.into());
            }
            let current = classify(*address);
            #[cfg(test)]
            let current = if self.allow_fixture_loopback && address.is_loopback() {
                AddressClass::FixtureLoopback
            } else {
                current
            };
            if current == AddressClass::Forbidden {
                return Err(UpstreamErrorCode::AddressForbidden.into());
            }
            if scope.is_some_and(|existing| existing != current) {
                return Err(UpstreamErrorCode::DnsMixedScope.into());
            }
            scope = Some(current);
        }
        let expected = self
            .rules
            .iter()
            .find(|rule| rule.matches(&target))
            .map(|rule| rule.network_scope)
            .ok_or(UpstreamErrorCode::DestinationForbidden)?;
        match scope.ok_or(UpstreamErrorCode::DnsEmpty)? {
            AddressClass::Public if expected != NetworkScope::Public => {
                return Err(UpstreamErrorCode::NetworkScopeMismatch.into());
            }
            AddressClass::PrivateLan => {
                if expected != NetworkScope::PrivateLan {
                    return Err(UpstreamErrorCode::NetworkScopeMismatch.into());
                }
                if !self.private_lan.permits() {
                    return Err(UpstreamErrorCode::PrivateLanUnauthorized.into());
                }
            }
            #[cfg(test)]
            AddressClass::FixtureLoopback => {}
            AddressClass::Forbidden => {
                return Err(UpstreamErrorCode::AddressForbidden.into());
            }
            AddressClass::Public => {}
        }
        let socket_addresses = unique
            .into_iter()
            .map(|address| SocketAddr::new(address, target.port))
            .collect();
        Ok(ResolvedTarget {
            target,
            socket_addresses,
        })
    }
}

#[derive(Clone)]
pub(crate) struct ValidatedUrl {
    pub(crate) url: Url,
    pub(crate) scheme: String,
    pub(crate) host: String,
    pub(crate) port: u16,
}

impl fmt::Debug for ValidatedUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ValidatedUrl(<redacted>)")
    }
}

impl ValidatedUrl {
    pub(crate) fn authority_matches(&self, scheme: &str, host: &str, port: u16) -> bool {
        self.scheme == scheme && self.host == normalize_host_lossy(host) && self.port == port
    }

    pub(crate) fn origin(&self) -> ResponseOrigin {
        ResponseOrigin {
            scheme: self.scheme.clone(),
            host: self.host.clone(),
            port: self.port,
        }
    }

    pub(crate) fn canonical_visit_key(&self) -> String {
        self.url.as_str().to_string()
    }
}

pub(crate) struct ResolvedTarget {
    pub(crate) target: ValidatedUrl,
    pub(crate) socket_addresses: Vec<SocketAddr>,
}

impl fmt::Debug for ResolvedTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedTarget")
            .field("target", &self.target)
            .field("address_count", &self.socket_addresses.len())
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ResponseOrigin {
    scheme: String,
    host: String,
    port: u16,
}

impl fmt::Debug for ResponseOrigin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ResponseOrigin(<redacted>)")
    }
}

impl ResponseOrigin {
    pub fn is_https(&self) -> bool {
        self.scheme == "https"
    }

    pub fn same_authority(&self, other: &Self) -> bool {
        self.scheme == other.scheme && self.host == other.host && self.port == other.port
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AddressClass {
    Public,
    PrivateLan,
    Forbidden,
    #[cfg(test)]
    FixtureLoopback,
}

fn classify(address: IpAddr) -> AddressClass {
    match address {
        IpAddr::V4(value) => classify_v4(value),
        IpAddr::V6(value) => classify_v6(value),
    }
}

fn classify_v4(value: Ipv4Addr) -> AddressClass {
    let octets = value.octets();
    if octets[0] == 10
        || (octets[0] == 172 && (16..=31).contains(&octets[1]))
        || (octets[0] == 192 && octets[1] == 168)
    {
        return AddressClass::PrivateLan;
    }
    let forbidden = octets[0] == 0
        || octets[0] == 127
        || octets[0] >= 224
        || (octets[0] == 100 && (64..=127).contains(&octets[1]))
        || (octets[0] == 169 && octets[1] == 254)
        || (octets[0] == 192 && octets[1] == 0 && octets[2] == 0)
        || (octets[0] == 192 && octets[1] == 0 && octets[2] == 2)
        || (octets[0] == 192 && octets[1] == 88 && octets[2] == 99)
        || (octets[0] == 198 && (octets[1] == 18 || octets[1] == 19))
        || (octets[0] == 198 && octets[1] == 51 && octets[2] == 100)
        || (octets[0] == 203 && octets[1] == 0 && octets[2] == 113);
    if forbidden {
        AddressClass::Forbidden
    } else {
        AddressClass::Public
    }
}

fn classify_v6(value: Ipv6Addr) -> AddressClass {
    let segments = value.segments();
    if segments[0] & 0xfe00 == 0xfc00 {
        return AddressClass::PrivateLan;
    }
    let forbidden = value.is_unspecified()
        || value.is_loopback()
        || value.is_multicast()
        || value.to_ipv4_mapped().is_some()
        || segments[0] & 0xffc0 == 0xfe80
        || segments[0] & 0xffc0 == 0xfec0
        || (segments[0] == 0x0064 && segments[1] == 0xff9b)
        || (segments[0] == 0x0100 && segments[1] == 0 && segments[2] == 0 && segments[3] == 0)
        || (segments[0] == 0x2001 && segments[1] < 0x0200)
        || (segments[0] == 0x2001 && segments[1] == 0x0db8)
        || segments[0] == 0x2002
        || segments[0] & 0xfff0 == 0x3ff0;
    if forbidden || segments[0] & 0xe000 != 0x2000 {
        AddressClass::Forbidden
    } else {
        AddressClass::Public
    }
}

fn normalize_host(value: &str) -> Result<String> {
    let normalized = normalize_host_lossy(value);
    if normalized.is_empty()
        || normalized.len() > 253
        || normalized.chars().any(char::is_control)
        || normalized.contains(['/', '\\', '@', '#', '?'])
    {
        return Err(UpstreamErrorCode::HostForbidden.into());
    }
    Ok(normalized)
}

fn normalize_host_lossy(value: &str) -> String {
    value
        .trim_start_matches('[')
        .trim_end_matches(']')
        .trim_end_matches('.')
        .to_ascii_lowercase()
}

fn forbidden_hostname(host: &str) -> bool {
    host == "localhost"
        || host.ends_with(".localhost")
        || matches!(
            host,
            "host.docker.internal"
                | "gateway.docker.internal"
                | "metadata.google.internal"
                | "metadata.aws.internal"
                | "kubernetes.default.svc"
        )
}

fn valid_exact_path(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= 2_048
        && path.starts_with('/')
        && !path.contains(['?', '#'])
        && !path.chars().any(char::is_control)
        && !contains_encoded_control(path)
}

fn contains_encoded_control(value: &str) -> bool {
    let bytes = value.as_bytes();
    let mut index = 0usize;
    while index + 2 < bytes.len() {
        if bytes[index] == b'%'
            && let (Some(high), Some(low)) = (hex(bytes[index + 1]), hex(bytes[index + 2]))
        {
            let decoded = high * 16 + low;
            if decoded < 0x20 || decoded == 0x7f {
                return true;
            }
            index += 3;
            continue;
        }
        index += 1;
    }
    false
}

fn hex(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn mask_address(address: IpAddr, prefix: u8) -> IpAddr {
    match address {
        IpAddr::V4(value) => {
            let mask = if prefix == 0 {
                0
            } else {
                u32::MAX << (32 - prefix)
            };
            IpAddr::V4(Ipv4Addr::from(u32::from(value) & mask))
        }
        IpAddr::V6(value) => {
            let mask = if prefix == 0 {
                0
            } else {
                u128::MAX << (128 - prefix)
            };
            IpAddr::V6(Ipv6Addr::from(u128::from(value) & mask))
        }
    }
}
