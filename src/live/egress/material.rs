use std::{
    collections::HashSet,
    fs::{Metadata, OpenOptions},
    io::Read,
    net::IpAddr,
    path::Path,
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

use crate::live::{
    config::{LiveEgressProfileConfig, LiveEgressProfileKind, is_public_egress_ip},
    upstream::{DnsResolver, SystemDnsResolver},
};

pub(crate) const GATEWAY_CONFIG_ROLE: &str = "gateway_config";
pub(crate) const OPENVPN_USERNAME_ROLE: &str = "openvpn_username";
pub(crate) const OPENVPN_PASSWORD_ROLE: &str = "openvpn_password";

pub(crate) const WIREGUARD_CONFIG_ROOT: &str = "/gluetun/wireguard";
pub(crate) const OPENVPN_CONFIG_ROOT: &str = "/gluetun/elixir-openvpn";
pub(crate) const OPENVPN_USERNAME_ROOT: &str = "/run/elixir-openvpn-username";
pub(crate) const OPENVPN_PASSWORD_ROOT: &str = "/run/elixir-openvpn-password";
pub(crate) const OPENVPN_CONFIG_PATH: &str = "/gluetun/elixir-openvpn/custom.conf";
pub(crate) const OPENVPN_USERNAME_PATH: &str = "/run/elixir-openvpn-username/username";
pub(crate) const OPENVPN_PASSWORD_PATH: &str = "/run/elixir-openvpn-password/password";

const MAX_CONFIG_BYTES: u64 = 256 * 1024;
const MAX_AUTH_BYTES: u64 = 4 * 1024;
const DNS_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy)]
enum AddressFamily {
    Any,
    V4,
    V6,
}

#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("VPN gateway material is invalid")]
pub(crate) struct GatewayMaterialError;

pub(crate) struct PreparedMaterialFile {
    pub role: &'static str,
    pub file_name: &'static str,
    contents: Zeroizing<Vec<u8>>,
}

impl PreparedMaterialFile {
    pub fn contents(&self) -> &[u8] {
        self.contents.as_slice()
    }
}

pub(crate) async fn prepare_gateway_material(
    profile: &LiveEgressProfileConfig,
) -> Result<Vec<PreparedMaterialFile>, GatewayMaterialError> {
    match profile.kind {
        LiveEgressProfileKind::Warp => Ok(Vec::new()),
        LiveEgressProfileKind::Wireguard => {
            let path = profile
                .config_host_path
                .as_deref()
                .ok_or(GatewayMaterialError)?;
            let source = read_private_file(Path::new(path), MAX_CONFIG_BYTES)?;
            prepare_wireguard_material(&source).await
        }
        LiveEgressProfileKind::Openvpn => {
            let path = profile
                .config_host_path
                .as_deref()
                .ok_or(GatewayMaterialError)?;
            let source = read_private_file(Path::new(path), MAX_CONFIG_BYTES)?;
            let credentials = match profile.auth_host_path.as_deref() {
                Some(auth_path) => {
                    let auth = read_private_file(Path::new(auth_path), MAX_AUTH_BYTES)?;
                    Some(parse_openvpn_auth(&auth)?)
                }
                None => None,
            };
            prepare_openvpn_material(&source, credentials).await
        }
    }
}

pub(crate) async fn prepare_gateway_material_from_secret_values(
    kind: LiveEgressProfileKind,
    config: &[u8],
    username: Option<&[u8]>,
    password: Option<&[u8]>,
) -> Result<Vec<PreparedMaterialFile>, GatewayMaterialError> {
    if config.is_empty()
        || config.len() as u64 > MAX_CONFIG_BYTES
        || config.contains(&0)
        || username.is_some() != password.is_some()
    {
        return Err(GatewayMaterialError);
    }
    match kind {
        LiveEgressProfileKind::Wireguard if username.is_none() => {
            prepare_wireguard_material(config).await
        }
        LiveEgressProfileKind::Openvpn => {
            let credentials = match (username, password) {
                (Some(username), Some(password)) => {
                    let mut auth = Zeroizing::new(Vec::with_capacity(
                        username
                            .len()
                            .saturating_add(password.len())
                            .saturating_add(2),
                    ));
                    auth.extend_from_slice(username);
                    auth.push(b'\n');
                    auth.extend_from_slice(password);
                    auth.push(b'\n');
                    if auth.len() as u64 > MAX_AUTH_BYTES || auth.contains(&0) {
                        return Err(GatewayMaterialError);
                    }
                    Some(parse_openvpn_auth(&auth)?)
                }
                (None, None) => None,
                _ => return Err(GatewayMaterialError),
            };
            prepare_openvpn_material(config, credentials).await
        }
        _ => Err(GatewayMaterialError),
    }
}

async fn prepare_wireguard_material(
    source: &[u8],
) -> Result<Vec<PreparedMaterialFile>, GatewayMaterialError> {
    let normalized = normalize_wireguard(source).await?;
    Ok(vec![PreparedMaterialFile {
        role: GATEWAY_CONFIG_ROLE,
        file_name: "wg0.conf",
        contents: Zeroizing::new(normalized.as_bytes().to_vec()),
    }])
}

async fn prepare_openvpn_material(
    source: &[u8],
    credentials: Option<(Zeroizing<String>, Zeroizing<String>)>,
) -> Result<Vec<PreparedMaterialFile>, GatewayMaterialError> {
    let requires_credentials = openvpn_requires_credentials(source)?;
    if requires_credentials != credentials.is_some() {
        return Err(GatewayMaterialError);
    }
    let normalized = normalize_openvpn(source).await?;
    let mut files = vec![PreparedMaterialFile {
        role: GATEWAY_CONFIG_ROLE,
        file_name: "custom.conf",
        contents: Zeroizing::new(normalized.as_bytes().to_vec()),
    }];
    if let Some((username, password)) = credentials {
        files.push(PreparedMaterialFile {
            role: OPENVPN_USERNAME_ROLE,
            file_name: "username",
            contents: Zeroizing::new(username.as_bytes().to_vec()),
        });
        files.push(PreparedMaterialFile {
            role: OPENVPN_PASSWORD_ROLE,
            file_name: "password",
            contents: Zeroizing::new(password.as_bytes().to_vec()),
        });
    }
    Ok(files)
}

fn read_private_file(
    path: &Path,
    max_bytes: u64,
) -> Result<Zeroizing<Vec<u8>>, GatewayMaterialError> {
    if !path.is_absolute() || max_bytes == 0 {
        return Err(GatewayMaterialError);
    }
    let path_metadata = std::fs::symlink_metadata(path).map_err(|_| GatewayMaterialError)?;
    if !path_metadata.is_file() || path_metadata.file_type().is_symlink() {
        return Err(GatewayMaterialError);
    }

    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    }
    let mut file = options.open(path).map_err(|_| GatewayMaterialError)?;
    let before = file.metadata().map_err(|_| GatewayMaterialError)?;
    validate_private_metadata(&before, max_bytes)?;

    let mut body = Zeroizing::new(Vec::with_capacity(
        usize::try_from(before.len()).map_err(|_| GatewayMaterialError)?,
    ));
    (&mut file)
        .take(max_bytes + 1)
        .read_to_end(&mut body)
        .map_err(|_| GatewayMaterialError)?;
    if body.is_empty() || body.len() as u64 > max_bytes || body.contains(&0) {
        return Err(GatewayMaterialError);
    }
    let after = file.metadata().map_err(|_| GatewayMaterialError)?;
    validate_private_metadata(&after, max_bytes)?;
    if !same_file_version(&before, &after) || after.len() != body.len() as u64 {
        return Err(GatewayMaterialError);
    }
    Ok(body)
}

#[cfg(unix)]
fn validate_private_metadata(
    metadata: &Metadata,
    max_bytes: u64,
) -> Result<(), GatewayMaterialError> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    if !metadata.is_file()
        || metadata.permissions().mode() & 0o777 != 0o600
        || metadata.nlink() != 1
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.len() == 0
        || metadata.len() > max_bytes
    {
        return Err(GatewayMaterialError);
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_private_metadata(
    metadata: &Metadata,
    max_bytes: u64,
) -> Result<(), GatewayMaterialError> {
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > max_bytes {
        return Err(GatewayMaterialError);
    }
    Ok(())
}

#[cfg(unix)]
fn same_file_version(before: &Metadata, after: &Metadata) -> bool {
    use std::os::unix::fs::MetadataExt as _;

    before.dev() == after.dev()
        && before.ino() == after.ino()
        && before.len() == after.len()
        && before.mtime() == after.mtime()
        && before.mtime_nsec() == after.mtime_nsec()
        && before.ctime() == after.ctime()
        && before.ctime_nsec() == after.ctime_nsec()
}

#[cfg(not(unix))]
fn same_file_version(before: &Metadata, after: &Metadata) -> bool {
    before.len() == after.len() && before.modified().ok() == after.modified().ok()
}

async fn normalize_wireguard(source: &[u8]) -> Result<Zeroizing<String>, GatewayMaterialError> {
    let text = std::str::from_utf8(source).map_err(|_| GatewayMaterialError)?;
    let lines = text.lines().collect::<Vec<_>>();
    let mut section = "";
    let mut interface_sections = 0_u8;
    let mut peer_sections = 0_u8;
    let mut interface_keys = HashSet::new();
    let mut peer_keys = HashSet::new();
    let mut has_ipv6_address = false;
    let mut has_ipv4_default = false;
    let mut has_ipv6_default = false;
    let mut endpoint = None;

    for (index, source_line) in lines.iter().enumerate() {
        let line = source_line.trim_end_matches('\r').trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if line.starts_with('[') {
            section = match line.to_ascii_lowercase().as_str() {
                "[interface]" => {
                    interface_sections = interface_sections.saturating_add(1);
                    "interface"
                }
                "[peer]" => {
                    peer_sections = peer_sections.saturating_add(1);
                    "peer"
                }
                _ => return Err(GatewayMaterialError),
            };
            continue;
        }
        let (key, value) = line.split_once('=').ok_or(GatewayMaterialError)?;
        let key = key.trim().to_ascii_lowercase();
        let value = value.trim();
        if value.is_empty() || value.chars().any(char::is_control) {
            return Err(GatewayMaterialError);
        }
        let keys = match section {
            "interface" => &mut interface_keys,
            "peer" => &mut peer_keys,
            _ => return Err(GatewayMaterialError),
        };
        if !keys.insert(key.clone()) {
            return Err(GatewayMaterialError);
        }
        match (section, key.as_str()) {
            ("interface", "privatekey") => validate_wireguard_key(value)?,
            ("interface", "address") => {
                for address in comma_values(value)? {
                    let (address, _) = parse_cidr(address)?;
                    has_ipv6_address |= address.is_ipv6();
                }
            }
            ("interface", "dns") => {
                for address in comma_values(value)? {
                    let _: IpAddr = address.parse().map_err(|_| GatewayMaterialError)?;
                }
            }
            ("interface", "mtu") => validate_integer(value, 576, 65_535)?,
            ("interface", "listenport") => validate_integer(value, 1, 65_535)?,
            ("peer", "publickey") | ("peer", "presharedkey") => validate_wireguard_key(value)?,
            ("peer", "allowedips") => {
                for route in comma_values(value)? {
                    let (address, prefix) = parse_cidr(route)?;
                    has_ipv4_default |=
                        address == IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED) && prefix == 0;
                    has_ipv6_default |=
                        address == IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED) && prefix == 0;
                }
            }
            ("peer", "endpoint") => {
                let (host, port) = split_wireguard_endpoint(value)?;
                endpoint = Some((index, host, port));
            }
            ("peer", "persistentkeepalive") => validate_integer(value, 0, 65_535)?,
            _ => return Err(GatewayMaterialError),
        }
    }

    if interface_sections != 1
        || peer_sections != 1
        || !interface_keys.contains("privatekey")
        || !interface_keys.contains("address")
        || !peer_keys.contains("publickey")
        || !peer_keys.contains("allowedips")
        || !peer_keys.contains("endpoint")
        || !has_ipv4_default
        || (has_ipv6_address && !has_ipv6_default)
    {
        return Err(GatewayMaterialError);
    }
    let (endpoint_line, endpoint_host, endpoint_port) = endpoint.ok_or(GatewayMaterialError)?;
    let endpoint_ip =
        resolve_public_host(&endpoint_host, endpoint_port, AddressFamily::Any).await?;
    let endpoint = match endpoint_ip {
        IpAddr::V4(address) => format!("{address}:{endpoint_port}"),
        IpAddr::V6(address) => format!("[{address}]:{endpoint_port}"),
    };

    let mut rendered = Zeroizing::new(String::with_capacity(text.len() + 1));
    for (index, line) in lines.iter().enumerate() {
        if index == endpoint_line {
            rendered.push_str("Endpoint = ");
            rendered.push_str(&endpoint);
        } else {
            rendered.push_str(line.trim_end_matches('\r'));
        }
        rendered.push('\n');
    }
    Ok(rendered)
}

fn validate_wireguard_key(value: &str) -> Result<(), GatewayMaterialError> {
    let decoded = STANDARD.decode(value).map_err(|_| GatewayMaterialError)?;
    if decoded.len() != 32 || STANDARD.encode(decoded) != value {
        return Err(GatewayMaterialError);
    }
    Ok(())
}

fn comma_values(value: &str) -> Result<impl Iterator<Item = &str>, GatewayMaterialError> {
    let values = value.split(',').map(str::trim).collect::<Vec<_>>();
    if values.is_empty() || values.iter().any(|value| value.is_empty()) {
        return Err(GatewayMaterialError);
    }
    Ok(values.into_iter())
}

fn parse_cidr(value: &str) -> Result<(IpAddr, u8), GatewayMaterialError> {
    let (address, prefix) = value.split_once('/').ok_or(GatewayMaterialError)?;
    let address = address
        .parse::<IpAddr>()
        .map_err(|_| GatewayMaterialError)?;
    let prefix = prefix.parse::<u8>().map_err(|_| GatewayMaterialError)?;
    if prefix > if address.is_ipv4() { 32 } else { 128 }
        || address.is_unspecified() && prefix != 0
        || address.is_multicast()
    {
        return Err(GatewayMaterialError);
    }
    Ok((address, prefix))
}

fn validate_integer(value: &str, minimum: u32, maximum: u32) -> Result<(), GatewayMaterialError> {
    let value = value.parse::<u32>().map_err(|_| GatewayMaterialError)?;
    if !(minimum..=maximum).contains(&value) {
        return Err(GatewayMaterialError);
    }
    Ok(())
}

fn split_wireguard_endpoint(value: &str) -> Result<(String, u16), GatewayMaterialError> {
    let (host, port) = if let Some(value) = value.strip_prefix('[') {
        let (host, port) = value.split_once("]:").ok_or(GatewayMaterialError)?;
        (host, port)
    } else {
        let (host, port) = value.rsplit_once(':').ok_or(GatewayMaterialError)?;
        if host.contains(':') {
            return Err(GatewayMaterialError);
        }
        (host, port)
    };
    let port = port.parse::<u16>().map_err(|_| GatewayMaterialError)?;
    if host.is_empty() || port == 0 {
        return Err(GatewayMaterialError);
    }
    Ok((host.to_string(), port))
}

fn openvpn_requires_credentials(source: &[u8]) -> Result<bool, GatewayMaterialError> {
    let text = std::str::from_utf8(source).map_err(|_| GatewayMaterialError)?;
    Ok(text.lines().any(|line| {
        line.trim_start()
            .to_ascii_lowercase()
            .starts_with("auth-user-pass")
    }))
}

async fn normalize_openvpn(source: &[u8]) -> Result<Zeroizing<String>, GatewayMaterialError> {
    let text = std::str::from_utf8(source).map_err(|_| GatewayMaterialError)?;
    let lines = text.lines().collect::<Vec<_>>();
    let mut inline_block: Option<String> = None;
    let mut inline_has_content = false;
    let mut inline_blocks = HashSet::new();
    let mut directives = HashSet::new();
    let mut remote = None;
    let mut auth_line = None;
    let mut client = false;
    let mut pull = false;
    let mut dev_tun = false;
    let mut protocol = None;
    let mut remote_cert_tls = false;
    let mut verify_x509 = false;
    let mut tls_minimum = false;

    for (index, source_line) in lines.iter().enumerate() {
        let line = source_line.trim_end_matches('\r').trim();
        if let Some(block) = inline_block.as_deref() {
            if line.eq_ignore_ascii_case(&format!("</{block}>")) {
                if !inline_has_content {
                    return Err(GatewayMaterialError);
                }
                inline_block = None;
                inline_has_content = false;
            } else if line.starts_with('<') {
                return Err(GatewayMaterialError);
            } else if !line.is_empty() {
                inline_has_content = true;
            }
            continue;
        }
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if line.starts_with('<') {
            let block = line
                .strip_prefix('<')
                .and_then(|value| value.strip_suffix('>'))
                .ok_or(GatewayMaterialError)?
                .to_ascii_lowercase();
            if !matches!(
                block.as_str(),
                "ca" | "cert" | "key" | "tls-auth" | "tls-crypt" | "tls-crypt-v2"
            ) || !inline_blocks.insert(block.clone())
            {
                return Err(GatewayMaterialError);
            }
            inline_block = Some(block);
            inline_has_content = false;
            continue;
        }

        let fields = line.split_ascii_whitespace().collect::<Vec<_>>();
        let directive = fields
            .first()
            .ok_or(GatewayMaterialError)?
            .to_ascii_lowercase();
        if !openvpn_directive_allowed(&directive) {
            return Err(GatewayMaterialError);
        }
        validate_openvpn_directive_values(&directive, &fields)?;
        if matches!(
            directive.as_str(),
            "client"
                | "dev"
                | "proto"
                | "remote"
                | "remote-cert-tls"
                | "verify-x509-name"
                | "tls-version-min"
                | "auth-user-pass"
        ) && !directives.insert(directive.clone())
        {
            return Err(GatewayMaterialError);
        }
        match directive.as_str() {
            "client" => client = fields.len() == 1,
            "pull" => pull = fields.len() == 1,
            "dev" => {
                dev_tun = fields.len() == 2 && fields[1].eq_ignore_ascii_case("tun");
            }
            "proto" => {
                if fields.len() == 2 && openvpn_protocol_allowed(fields[1]) {
                    protocol = Some(fields[1].to_ascii_lowercase());
                }
            }
            "remote" => {
                if !(3..=4).contains(&fields.len()) {
                    return Err(GatewayMaterialError);
                }
                let port = fields[2].parse::<u16>().map_err(|_| GatewayMaterialError)?;
                if port == 0
                    || fields
                        .get(3)
                        .is_some_and(|value| !openvpn_protocol_allowed(value))
                {
                    return Err(GatewayMaterialError);
                }
                remote = Some((index, fields[1].to_string(), port, fields.get(3).copied()));
            }
            "remote-cert-tls" => {
                remote_cert_tls = fields.len() == 2 && fields[1].eq_ignore_ascii_case("server");
            }
            "verify-x509-name" => {
                verify_x509 = (2..=3).contains(&fields.len())
                    && fields[1].len() <= 253
                    && fields[1].chars().all(|value| !value.is_control())
                    && fields.get(2).is_none_or(|value| {
                        matches!(
                            value.to_ascii_lowercase().as_str(),
                            "name" | "name-prefix" | "subject"
                        )
                    });
            }
            "tls-version-min" => {
                tls_minimum = fields
                    .get(1)
                    .and_then(|value| value.parse::<f32>().ok())
                    .is_some_and(|value| value >= 1.2)
                    && fields.len() <= 3;
            }
            "auth-user-pass" => {
                if fields.len() > 2 {
                    return Err(GatewayMaterialError);
                }
                auth_line = Some(index);
            }
            _ => {}
        }
    }
    if inline_block.is_some()
        || !inline_blocks.contains("ca")
        || !client
        || !pull
        || !dev_tun
        || protocol.is_none()
        || !remote_cert_tls
        || !verify_x509
        || !tls_minimum
    {
        return Err(GatewayMaterialError);
    }
    let (remote_line, remote_host, remote_port, remote_protocol) =
        remote.ok_or(GatewayMaterialError)?;
    let effective_protocol =
        remote_protocol.unwrap_or(protocol.as_deref().ok_or(GatewayMaterialError)?);
    let family = if effective_protocol.ends_with('4') {
        AddressFamily::V4
    } else if effective_protocol.ends_with('6') {
        AddressFamily::V6
    } else {
        AddressFamily::Any
    };
    let remote_ip = resolve_public_host(&remote_host, remote_port, family).await?;
    let mut rendered = Zeroizing::new(String::with_capacity(text.len() + 1));
    for (index, line) in lines.iter().enumerate() {
        if index == remote_line {
            rendered.push_str("remote ");
            rendered.push_str(&remote_ip.to_string());
            rendered.push(' ');
            rendered.push_str(&remote_port.to_string());
            if let Some(protocol) = remote_protocol {
                rendered.push(' ');
                rendered.push_str(protocol);
            }
        } else if Some(index) == auth_line {
            rendered.push_str("auth-user-pass");
        } else {
            rendered.push_str(line.trim_end_matches('\r'));
        }
        rendered.push('\n');
    }
    Ok(rendered)
}

fn openvpn_directive_allowed(directive: &str) -> bool {
    matches!(
        directive,
        "auth"
            | "auth-nocache"
            | "auth-retry"
            | "auth-user-pass"
            | "cipher"
            | "client"
            | "connect-retry"
            | "connect-retry-max"
            | "connect-timeout"
            | "data-ciphers"
            | "data-ciphers-fallback"
            | "dev"
            | "explicit-exit-notify"
            | "fast-io"
            | "hand-window"
            | "key-direction"
            | "link-mtu"
            | "mute"
            | "nobind"
            | "persist-key"
            | "persist-tun"
            | "ping"
            | "ping-exit"
            | "ping-restart"
            | "ping-timer-rem"
            | "proto"
            | "pull"
            | "rcvbuf"
            | "remote"
            | "remote-cert-tls"
            | "remote-random"
            | "reneg-sec"
            | "resolv-retry"
            | "server-poll-timeout"
            | "sndbuf"
            | "tls-cipher"
            | "tls-client"
            | "tls-version-max"
            | "tls-version-min"
            | "tun-mtu"
            | "verb"
            | "verify-x509-name"
    )
}

fn openvpn_protocol_allowed(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "udp" | "udp4" | "udp6" | "tcp-client" | "tcp4-client" | "tcp6-client"
    )
}

fn validate_openvpn_directive_values(
    directive: &str,
    fields: &[&str],
) -> Result<(), GatewayMaterialError> {
    if matches!(
        directive,
        "auth" | "cipher" | "data-ciphers" | "data-ciphers-fallback" | "tls-cipher"
    ) {
        if fields.len() < 2
            || fields[1..]
                .iter()
                .flat_map(|value| {
                    value
                        .split([':', ','])
                        .map(|token| token.to_ascii_uppercase())
                })
                .any(|token| {
                    token == "NONE"
                        || token.contains("NULL")
                        || token.contains("DES")
                        || token.contains("RC2")
                        || token.contains("RC4")
                        || token.contains("BLOWFISH")
                        || token == "BF-CBC"
                        || token == "MD5"
                })
        {
            return Err(GatewayMaterialError);
        }
    }
    if directive == "tls-version-max"
        && (fields.len() != 2
            || fields[1]
                .parse::<f32>()
                .ok()
                .is_none_or(|value| value < 1.2))
    {
        return Err(GatewayMaterialError);
    }
    Ok(())
}

fn parse_openvpn_auth(
    source: &[u8],
) -> Result<(Zeroizing<String>, Zeroizing<String>), GatewayMaterialError> {
    let text = std::str::from_utf8(source).map_err(|_| GatewayMaterialError)?;
    let lines = text
        .split_terminator('\n')
        .map(|line| line.strip_suffix('\r').unwrap_or(line))
        .collect::<Vec<_>>();
    if lines.len() != 2 {
        return Err(GatewayMaterialError);
    }
    for line in &lines {
        if line.is_empty()
            || line.len() > 512
            || line.trim() != *line
            || line.chars().any(char::is_control)
        {
            return Err(GatewayMaterialError);
        }
    }
    Ok((
        Zeroizing::new(lines[0].to_string()),
        Zeroizing::new(lines[1].to_string()),
    ))
}

async fn resolve_public_host(
    host: &str,
    port: u16,
    family: AddressFamily,
) -> Result<IpAddr, GatewayMaterialError> {
    let host = normalize_host(host)?;
    if let Ok(address) = host.parse::<IpAddr>() {
        return (is_public_egress_ip(address) && address_family_matches(address, family))
            .then_some(address)
            .ok_or(GatewayMaterialError);
    }
    let resolver = SystemDnsResolver::new(DNS_TIMEOUT).map_err(|_| GatewayMaterialError)?;
    let cancellation = CancellationToken::new();
    let mut addresses = resolver
        .resolve(&host, port, &cancellation)
        .await
        .map_err(|_| GatewayMaterialError)?;
    addresses.sort_by_key(|address| (address.is_ipv6(), address.to_string()));
    addresses.dedup();
    if addresses.is_empty()
        || addresses
            .iter()
            .any(|address| !is_public_egress_ip(*address))
    {
        return Err(GatewayMaterialError);
    }
    addresses
        .into_iter()
        .find(|address| address_family_matches(*address, family))
        .ok_or(GatewayMaterialError)
}

fn address_family_matches(address: IpAddr, family: AddressFamily) -> bool {
    match family {
        AddressFamily::Any => true,
        AddressFamily::V4 => address.is_ipv4(),
        AddressFamily::V6 => address.is_ipv6(),
    }
}

fn normalize_host(input: &str) -> Result<String, GatewayMaterialError> {
    if input.is_empty()
        || input.len() > 255
        || input.trim() != input
        || input.chars().any(char::is_control)
        || input.contains(['/', '\\', '?', '#', '@', '*'])
    {
        return Err(GatewayMaterialError);
    }
    let bracketless = input
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(input);
    if let Ok(address) = bracketless.parse::<IpAddr>() {
        return Ok(address.to_string());
    }
    if input.contains(':') {
        return Err(GatewayMaterialError);
    }
    let host = idna::domain_to_ascii(input.trim_end_matches('.'))
        .map_err(|_| GatewayMaterialError)?
        .to_ascii_lowercase();
    if host.is_empty() || host.len() > 253 {
        return Err(GatewayMaterialError);
    }
    if host.split('.').any(|label| {
        label.is_empty()
            || label.len() > 63
            || label.starts_with('-')
            || label.ends_with('-')
            || !label
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    }) {
        return Err(GatewayMaterialError);
    }
    Ok(host)
}

#[cfg(test)]
mod tests {
    use super::*;

    const PRIVATE_KEY: &str = "yAnzdtF2rM8Nl1N8MPm+2MvmFo0xSg6u40qCMgfHdC0=";
    const PUBLIC_KEY: &str = "fNBdb9h9NP7VDaRao7IhiHBpjz2uVH54camzato3tr0=";

    fn wireguard(endpoint: &str) -> String {
        format!(
            "[Interface]\nPrivateKey = {PRIVATE_KEY}\nAddress = 10.64.0.2/32,fd00::2/128\nDNS = 10.64.0.1\n\n[Peer]\nPublicKey = {PUBLIC_KEY}\nAllowedIPs = 0.0.0.0/0,::/0\nEndpoint = {endpoint}\nPersistentKeepalive = 25\n"
        )
    }

    fn openvpn(extra: &str) -> String {
        format!(
            "client\ndev tun\nproto udp\nremote 1.1.1.1 1194\nresolv-retry infinite\nnobind\npull\nremote-cert-tls server\nverify-x509-name vpn.example name\ntls-version-min 1.2\nauth-user-pass old.txt\n{extra}<ca>\ncertificate\n</ca>\n<tls-crypt>\nkey\n</tls-crypt>\n"
        )
    }

    #[tokio::test]
    async fn n11_material_accepts_full_tunnel_wireguard_and_normalizes_endpoint() {
        let normalized = normalize_wireguard(wireguard("1.1.1.1:51820").as_bytes())
            .await
            .expect("valid WireGuard material");
        assert!(normalized.contains("Endpoint = 1.1.1.1:51820"));
        assert!(normalized.contains(PRIVATE_KEY));
    }

    #[tokio::test]
    async fn n11_material_rejects_wireguard_hooks_split_routes_and_private_endpoint() {
        let hook = wireguard("1.1.1.1:51820").replace(
            "DNS = 10.64.0.1",
            "DNS = 10.64.0.1\nPostUp = curl https://example.com",
        );
        assert!(normalize_wireguard(hook.as_bytes()).await.is_err());
        let split = wireguard("1.1.1.1:51820").replace("0.0.0.0/0", "10.0.0.0/8");
        assert!(normalize_wireguard(split.as_bytes()).await.is_err());
        assert!(
            normalize_wireguard(wireguard("127.0.0.1:51820").as_bytes())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn n11_material_accepts_hardened_inline_openvpn_and_rewrites_auth() {
        let normalized = normalize_openvpn(openvpn("").as_bytes())
            .await
            .expect("valid OpenVPN material");
        assert!(normalized.contains("remote 1.1.1.1 1194"));
        assert!(normalized.contains("\nauth-user-pass\n"));
        assert!(!normalized.contains("old.txt"));
    }

    #[tokio::test]
    async fn n11_material_rejects_openvpn_scripts_routes_and_weak_peer_identity() {
        for unsafe_line in [
            "script-security 2\n",
            "up /tmp/script\n",
            "route 10.0.0.0 255.0.0.0\n",
            "management 0.0.0.0 5555\n",
            "compress lzo\n",
            "cipher none\n",
            "data-ciphers AES-256-GCM:BF-CBC\n",
        ] {
            assert!(
                normalize_openvpn(openvpn(unsafe_line).as_bytes())
                    .await
                    .is_err()
            );
        }
        let weak = openvpn("").replace("remote-cert-tls server\n", "");
        assert!(normalize_openvpn(weak.as_bytes()).await.is_err());
        let empty_ca = openvpn("").replace("<ca>\ncertificate\n</ca>", "<ca>\n</ca>");
        assert!(normalize_openvpn(empty_ca.as_bytes()).await.is_err());
    }

    #[test]
    fn n11_material_requires_exact_two_line_openvpn_credentials() {
        let (username, password) =
            parse_openvpn_auth(b"service-user\nservice-password\n").expect("valid credentials");
        assert_eq!(username.as_str(), "service-user");
        assert_eq!(password.as_str(), "service-password");
        assert!(parse_openvpn_auth(b"one-line-only\n").is_err());
        assert!(parse_openvpn_auth(b" user\npassword\n").is_err());
    }

    #[tokio::test]
    async fn live_projected_material_accepts_zeroizing_secret_values() {
        let wireguard = prepare_gateway_material_from_secret_values(
            LiveEgressProfileKind::Wireguard,
            wireguard("1.1.1.1:51820").as_bytes(),
            None,
            None,
        )
        .await
        .expect("projected WireGuard material");
        assert_eq!(wireguard.len(), 1);
        assert_eq!(wireguard[0].file_name, "wg0.conf");

        let openvpn = prepare_gateway_material_from_secret_values(
            LiveEgressProfileKind::Openvpn,
            openvpn("").as_bytes(),
            Some(b"service-user"),
            Some(b"service-password"),
        )
        .await
        .expect("projected OpenVPN material");
        assert_eq!(openvpn.len(), 3);
        assert_eq!(openvpn[1].contents(), b"service-user");
        assert_eq!(openvpn[2].contents(), b"service-password");
    }

    #[tokio::test]
    async fn live_projected_material_rejects_incomplete_credentials() {
        assert!(
            prepare_gateway_material_from_secret_values(
                LiveEgressProfileKind::Openvpn,
                openvpn("").as_bytes(),
                Some(b"service-user"),
                None,
            )
            .await
            .is_err()
        );
    }

    #[cfg(unix)]
    #[test]
    fn n11_material_rejects_public_or_symbolic_source_files() {
        use std::os::unix::fs::{PermissionsExt as _, symlink};

        let temporary = tempfile::tempdir().expect("temporary directory");
        let private = temporary.path().join("private.conf");
        std::fs::write(&private, wireguard("1.1.1.1:51820")).expect("write private config");
        std::fs::set_permissions(&private, std::fs::Permissions::from_mode(0o600))
            .expect("private permissions");
        assert!(read_private_file(&private, MAX_CONFIG_BYTES).is_ok());
        std::fs::set_permissions(&private, std::fs::Permissions::from_mode(0o644))
            .expect("public permissions");
        assert!(read_private_file(&private, MAX_CONFIG_BYTES).is_err());

        let symbolic = temporary.path().join("symbolic.conf");
        symlink(&private, &symbolic).expect("symbolic config");
        assert!(read_private_file(&symbolic, MAX_CONFIG_BYTES).is_err());
    }

    #[tokio::test]
    #[ignore = "requires private certification input paths"]
    async fn n11_private_certification_vpn_material_is_accepted_when_configured() {
        let wireguard_path = std::env::var("ELIXIR_TEST_LIVE_WIREGUARD_CONFIG")
            .expect("ELIXIR_TEST_LIVE_WIREGUARD_CONFIG");
        let openvpn_path = std::env::var("ELIXIR_TEST_LIVE_OPENVPN_CONFIG")
            .expect("ELIXIR_TEST_LIVE_OPENVPN_CONFIG");
        let auth_path =
            std::env::var("ELIXIR_TEST_LIVE_OPENVPN_AUTH").expect("ELIXIR_TEST_LIVE_OPENVPN_AUTH");
        let wireguard_profile = LiveEgressProfileConfig {
            kind: LiveEgressProfileKind::Wireguard,
            config_host_path: Some(wireguard_path),
            ..LiveEgressProfileConfig::default()
        };
        let openvpn_profile = LiveEgressProfileConfig {
            kind: LiveEgressProfileKind::Openvpn,
            config_host_path: Some(openvpn_path),
            auth_host_path: Some(auth_path),
            ..LiveEgressProfileConfig::default()
        };
        let wireguard = prepare_gateway_material(&wireguard_profile)
            .await
            .expect("private WireGuard material");
        let openvpn = prepare_gateway_material(&openvpn_profile)
            .await
            .expect("private OpenVPN material");
        assert_eq!(wireguard.len(), 1);
        assert_eq!(openvpn.len(), 3);
    }
}
