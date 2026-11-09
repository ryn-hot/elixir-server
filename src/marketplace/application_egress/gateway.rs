//! Closed ephemeral Gluetun configuration and private material framing.
//! These are DATA, not a held gateway, admission, an installed grant, readiness
//! or permission to release HTTP input. WARP needs its own owned-state adapter.
use super::selection::Selection;
use crate::{
    live::egress::PreparedMaterialFile,
    network::protection::DownloadNetworkProfileKind,
    runtime::{
        model::{
            ContainerLogOptions, ContainerSecurityOptions, ContainerSpec, ContainerTmpfsMount,
            ELIXIR_DEPLOYMENT_ID_LABEL, ELIXIR_EXTENSION_ID_LABEL, ELIXIR_INSTANCE_ID_LABEL,
            ELIXIR_MANAGED_LABEL, EnvVar, apply_container_spec_fingerprint,
        },
        packet_network::immutable_id,
    },
};
use anyhow::{Result, bail};
use elixir_marketplace::identity::Digest;
use std::collections::HashMap;
use zeroize::Zeroizing;

pub(crate) const ISOLATION: &str = "application-gateway-v1";
pub(crate) const ENTRYPOINT: &str = "/bin/busybox";
pub(crate) const MATERIAL_ACCEPTED: &[u8] = b"ELIXIR_GATEWAY_MATERIAL_ACCEPTED_V1\n";
const TRAILER: &[u8] = b"ELIXIR_GATEWAY_MATERIAL_V1";
const CONFIG_LIMIT: usize = 256 * 1024;
const AUTH_LIMIT: usize = 4096;
const CONFIG_ROLE: &str = "gateway_config";
const USER_ROLE: &str = "openvpn_username";
const PASSWORD_ROLE: &str = "openvpn_password";

/// One normalized private input, not authority. No caller-selected framing,
/// mutable bytes, Clone or serialization; wipe on completion/cancellation.
pub(in crate::marketplace) struct GatewayInput {
    bytes: Zeroizing<Vec<u8>>,
    body_len: usize,
}
impl GatewayInput {
    pub(super) fn new(bytes: Zeroizing<Vec<u8>>) -> Result<Self> {
        if bytes.len() <= TRAILER.len()
            || !bytes.ends_with(TRAILER)
            || bytes.len() > CONFIG_LIMIT + 2 * AUTH_LIMIT + TRAILER.len()
        {
            bail!("gateway private material framing differs");
        }
        let body_len = bytes.len() - TRAILER.len();
        Ok(Self { bytes, body_len })
    }
    pub(in crate::marketplace) fn body(&self) -> &[u8] {
        &self.bytes[..self.body_len]
    }
    pub(in crate::marketplace) fn trailer(&self) -> &[u8] {
        &self.bytes[self.body_len..]
    }
}

// Only trusted constant paths and bounded decimal lengths enter the shell.
// Material is NEVER shell text, arguments, environment, labels, disk-backed
// volumes or a host bind. BusyBox head buffers past -c on a pipe, so it must
// NOT be used to consume adjacent frames. dd's bounded full block consumes
// exactly the original length while tolerating fragmented private input. Short
// input/trailer mismatch fails before starting Gluetun. The outer native
// timeout also bounds a stalled/missing private input or VPN startup.
const LAUNCH: &str = r#"set -eu
umask 077
__ELIXIR_INSTALL_APPLICATION_GUARD__
read_file() {
    dd bs="$2" count=1 iflag=fullblock 2>/dev/null > "$1"
    [ "$(wc -c < "$1")" -eq "$2" ]
    chmod 600 "$1"
}
case "$1" in
    wireguard)
        mkdir -p /gluetun/wireguard
        read_file /gluetun/wireguard/wg0.conf "$2"
        ;;
    openvpn|openvpn-auth)
        mkdir -p /gluetun/elixir-openvpn
        read_file /gluetun/elixir-openvpn/custom.conf "$2"
        if [ "$1" = openvpn-auth ]; then
            read_file /gluetun/elixir-openvpn/username "$3"
            read_file /gluetun/elixir-openvpn/password "$4"
        fi
        ;;
    *) exit 64 ;;
esac
[ "$(dd bs=26 count=1 iflag=fullblock 2>/dev/null)" = ELIXIR_GATEWAY_MATERIAL_V1 ]
printf '%s\n' ELIXIR_GATEWAY_MATERIAL_ACCEPTED_V1
exec /gluetun-entrypoint < /dev/null
"#;

#[derive(Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum Kind {
    Wireguard,
    Openvpn,
    OpenvpnAuth,
}
impl Kind {
    fn name(self) -> &'static str {
        match self {
            Self::Wireguard => "wireguard",
            Self::Openvpn => "openvpn",
            Self::OpenvpnAuth => "openvpn-auth",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Layout {
    kind: Kind,
    config: usize,
    username: usize,
    password: usize,
    // Public sizing metadata only. Omitted for old intents/default MTU, so
    // recovering an old frozen profile does not invent a new environment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    mtu: Option<u16>,
    // Old durable intents omit this and retain their exact original topology
    // for retirement. New tunnels must reap BusyBox timeout's orphan monitors.
    #[serde(default, skip_serializing_if = "is_false")]
    reap_children: bool,
}
fn is_false(value: &bool) -> bool {
    !value
}
impl Layout {
    fn validate(self) -> Result<()> {
        let auth = self.kind == Kind::OpenvpnAuth;
        if !(1..=CONFIG_LIMIT).contains(&self.config)
            || self
                .mtu
                .is_some_and(|mtu| self.kind != Kind::Wireguard || mtu < 576)
            || if auth {
                !(1..=AUTH_LIMIT).contains(&self.username)
                    || !(1..=AUTH_LIMIT).contains(&self.password)
            } else {
                self.username != 0 || self.password != 0
            }
        {
            bail!("gateway material lengths exceed the closed profile");
        }
        Ok(())
    }
    fn from_files(
        kind: &DownloadNetworkProfileKind,
        files: &[PreparedMaterialFile],
    ) -> Result<Self> {
        let mut layout = Self {
            kind: match kind {
                DownloadNetworkProfileKind::WireguardConfig => Kind::Wireguard,
                DownloadNetworkProfileKind::OpenvpnConfig => Kind::Openvpn,
                _ => bail!("Gluetun material cannot represent a direct or WARP profile"),
            },
            config: 0,
            username: 0,
            password: 0,
            mtu: None,
            reap_children: true,
        };
        for file in files {
            let (slot, name, maximum) = match file.role {
                CONFIG_ROLE => (
                    &mut layout.config,
                    if layout.kind == Kind::Wireguard {
                        "wg0.conf"
                    } else {
                        "custom.conf"
                    },
                    CONFIG_LIMIT,
                ),
                USER_ROLE => (&mut layout.username, "username", AUTH_LIMIT),
                PASSWORD_ROLE => (&mut layout.password, "password", AUTH_LIMIT),
                _ => bail!("unexpected gateway material role"),
            };
            if *slot != 0
                || file.file_name != name
                || file.contents().is_empty()
                || file.contents().len() > maximum
                || file.contents().contains(&0)
            {
                bail!("gateway material shape is invalid");
            }
            *slot = file.contents().len();
            if file.role == CONFIG_ROLE && layout.kind == Kind::Wireguard {
                layout.mtu = wireguard_mtu(file.contents())?;
            }
        }
        if layout.config == 0
            || (layout.username == 0) != (layout.password == 0)
            || (layout.kind == Kind::Wireguard && layout.username != 0)
        {
            bail!("gateway material is incomplete or differs from its VPN kind");
        }
        if layout.username != 0 {
            layout.kind = Kind::OpenvpnAuth;
        }
        layout.validate()?;
        Ok(layout)
    }
}

/// Extract only validated, non-secret interface sizing from normalized private
/// material. Do not copy the config, expose its text, or accept shell options.
fn wireguard_mtu(bytes: &[u8]) -> Result<Option<u16>> {
    let text = std::str::from_utf8(bytes)?;
    let mut interface = false;
    let mut mtu = None;
    for line in text.lines().map(str::trim) {
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if line.starts_with('[') {
            interface = line.eq_ignore_ascii_case("[interface]");
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        if key.trim().eq_ignore_ascii_case("mtu") {
            if !interface || mtu.is_some() {
                bail!("gateway interface MTU is ambiguous");
            }
            let value = value.trim().parse::<u16>()?;
            if value < 576 {
                bail!("gateway interface MTU is invalid");
            }
            mtu = Some(value);
        }
    }
    Ok(mtu)
}

/// One invocation's finite desired configuration, not an ownership capability.
/// No Clone/Deserialize, caller-provided command, mutable spec or volume names.
pub struct ApplicationGatewayProfile {
    spec: ContainerSpec,
    layout: Layout,
    scope: Digest,
    invocation: Digest,
    intent: ApplicationGatewayIntent,
}
/// Recovery DATA only; contains lengths and immutable configuration identity,
/// never VPN file bytes, credentials, a grant or a native/readiness capability.
#[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ApplicationGatewayIntent {
    deployment: String,
    network_id: String,
    image: String,
    layout: Layout,
    scope: Digest,
    invocation: Digest,
    fingerprint: String,
}
impl ApplicationGatewayIntent {
    pub(crate) fn profile(&self) -> Result<ApplicationGatewayProfile> {
        let profile = ApplicationGatewayProfile::compile(
            &self.deployment,
            &self.network_id,
            &self.image,
            self.layout,
            self.scope,
            self.invocation,
        )?;
        if profile.intent.fingerprint != self.fingerprint {
            bail!("gateway recovery differs from its original frozen profile");
        }
        Ok(profile)
    }
}
impl ApplicationGatewayProfile {
    pub(in crate::marketplace) fn warm_scope() -> Digest {
        Digest::of(b"elixir/application-warm-tunnel/v1")
    }
    pub(in crate::marketplace) fn is_warm(&self) -> bool {
        self.scope == Self::warm_scope()
    }
    pub(super) fn gluetun(
        deployment: &str,
        network_id: &str,
        selected: &Selection,
        files: &[PreparedMaterialFile],
        scope: Digest,
        invocation: Digest,
    ) -> Result<Self> {
        Self::compile(
            deployment,
            network_id,
            selected.image_id(),
            Layout::from_files(&selected.profile().kind, files)?,
            scope,
            invocation,
        )
    }

    fn compile(
        deployment: &str,
        network_id: &str,
        image: &str,
        layout: Layout,
        scope: Digest,
        invocation: Digest,
    ) -> Result<Self> {
        layout.validate()?;
        if deployment.is_empty()
            || deployment.len() > 64
            || !deployment
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            || !immutable_id(network_id)
            || !image.strip_prefix("sha256:").is_some_and(immutable_id)
            || scope.as_bytes() == &[0; 32]
            || invocation.as_bytes() == &[0; 32]
        {
            bail!("application gateway requires exact deployment, image, network and invocation");
        }
        let owner = Digest::of(format!(
            "elixir/application-gateway/deployment/v1\0{deployment}"
        ));
        let instance = Digest::of(format!(
            "elixir/application-gateway/invocation/v1\0{deployment}\0{scope}\0{invocation}"
        ));
        let instance = format!("extension-gateway-{}", hex(instance));
        let env = |name: &str, value: &str| EnvVar {
            name: name.into(),
            value: value.into(),
        };
        let mut environment = vec![
            env("PUID", "0"),
            env("PGID", "0"),
            env("VPN_SERVICE_PROVIDER", "custom"),
            env("VPN_INTERFACE", "tun0"),
            env(
                "VPN_TYPE",
                if layout.kind == Kind::Wireguard {
                    "wireguard"
                } else {
                    "openvpn"
                },
            ),
            env("FIREWALL", "on"),
            env(
                "FIREWALL_ENABLED_DISABLING_IT_SHOOTS_YOU_IN_YOUR_FOOT",
                "on",
            ),
            env("FIREWALL_OUTBOUND_SUBNETS", ""),
            env("FIREWALL_INPUT_PORTS", ""),
            env("FIREWALL_VPN_INPUT_PORTS", ""),
            env("DNS_SERVER", "off"),
            env("BLOCK_MALICIOUS", "off"),
            env("PUBLICIP_ENABLED", "off"),
            env("VERSION_INFORMATION", "off"),
            env("UPDATER_PERIOD", "0"),
            env("HEALTH_RESTART_VPN", "off"),
            env("HEALTH_TARGET_ADDRESSES", "1.1.1.1:443"),
            env("HEALTH_ICMP_TARGET_IPS", "1.1.1.1"),
            env("HEALTH_SERVER_ADDRESS", "127.0.0.1:9999"),
            env("HTTP_CONTROL_SERVER_ADDRESS", "127.0.0.1:8000"),
            env("HTTP_CONTROL_SERVER_LOG", "off"),
            env("HTTPPROXY", "off"),
            env("SHADOWSOCKS", "off"),
        ];
        if layout.kind == Kind::Wireguard {
            // The pinned Gluetun version does not apply this INI field to the
            // effective link. Preserve the operator's normalized MTU explicitly;
            // mismatched TCP MSS/PMTU can otherwise stall TLS until its deadline.
            if let Some(mtu) = layout.mtu {
                environment.push(env("WIREGUARD_MTU", &mtu.to_string()));
            }
            environment.push(env(
                "WIREGUARD_CONF_SECRETFILE",
                "/gluetun/wireguard/wg0.conf",
            ));
        } else {
            environment.push(env(
                "OPENVPN_CUSTOM_CONFIG",
                "/gluetun/elixir-openvpn/custom.conf",
            ));
            if layout.kind == Kind::OpenvpnAuth {
                environment.push(env(
                    "OPENVPN_USER_SECRETFILE",
                    "/gluetun/elixir-openvpn/username",
                ));
                environment.push(env(
                    "OPENVPN_PASSWORD_SECRETFILE",
                    "/gluetun/elixir-openvpn/password",
                ));
            }
        }
        let tmpfs = |path: &str| ContainerTmpfsMount {
            path: path.into(),
            size_mb: Some(16),
            owner_uid: Some(0),
            owner_gid: Some(0),
            mode: Some(0o700),
            no_exec: true,
            no_suid: true,
            no_dev: true,
        };
        let mut spec = ContainerSpec {
            name: format!("elixir-{instance}"),
            image: image.into(),
            // Use the original immutable network ID, never a reusable name.
            network: network_id.into(),
            network_mode: None,
            aliases: vec![],
            env: environment,
            volumes: vec![],
            ports: vec![],
            labels: HashMap::from([
                (ELIXIR_MANAGED_LABEL.into(), "true".into()),
                (
                    ELIXIR_DEPLOYMENT_ID_LABEL.into(),
                    format!("application-gateway-{}", &hex(owner)[..32]),
                ),
                (
                    ELIXIR_EXTENSION_ID_LABEL.into(),
                    "elixir-extension-gateway".into(),
                ),
                (ELIXIR_INSTANCE_ID_LABEL.into(), instance),
                ("elixir.marketplace.isolation".into(), ISOLATION.into()),
                (
                    "elixir.marketplace.application_gateway.owner_deployment".into(),
                    deployment.into(),
                ),
                (
                    "elixir.marketplace.application_gateway.scope".into(),
                    scope.to_string(),
                ),
                (
                    "elixir.marketplace.application_gateway.invocation".into(),
                    invocation.to_string(),
                ),
            ]),
            command: vec![
                "timeout".into(),
                "-s".into(),
                "KILL".into(),
                if scope == Self::warm_scope() {
                    "330"
                } else {
                    "180"
                }
                .into(),
                "/bin/sh".into(),
                "-c".into(),
                LAUNCH.replace(
                    "__ELIXIR_INSTALL_APPLICATION_GUARD__",
                    super::observation::INSTALL_GUARD,
                ),
                "elixir-application-gateway".into(),
                layout.kind.name().into(),
                layout.config.to_string(),
                layout.username.to_string(),
                layout.password.to_string(),
            ],
            // The trusted VPN process needs tunnel/firewall and startup-user
            // operations. This is NOT a publisher extension's capability set.
            cap_add: [
                "NET_ADMIN",
                "NET_RAW",
                "CHOWN",
                "SETUID",
                "SETGID",
                "DAC_OVERRIDE",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
            cap_drop: vec!["ALL".into()],
            devices: vec!["/dev/net/tun:/dev/net/tun".into()],
            sysctls: HashMap::from([("net.ipv4.conf.all.src_valid_mark".into(), "1".into())]),
            security: ContainerSecurityOptions {
                user: Some("0:0".into()),
                read_only_rootfs: true,
                no_new_privileges: true,
                tmpfs: vec![tmpfs("/tmp"), tmpfs("/gluetun")],
                memory_limit_mb: Some(128),
                memory_swap_limit_mb: Some(128),
                pids_limit: Some(64),
                cpu_quota: Some("0.100".into()),
                shared_memory_mb: Some(1),
                disable_core_dumps: true,
                open_files_limit: Some(256),
                file_size_limit_bytes: Some(16 * 1024 * 1024),
                log_options: Some(ContainerLogOptions::Disabled),
                stdin_open: true,
                init_process: layout.reap_children,
                ..Default::default()
            },
        };
        apply_container_spec_fingerprint(&mut spec);
        let intent = ApplicationGatewayIntent {
            deployment: deployment.into(),
            network_id: network_id.into(),
            image: image.into(),
            layout,
            scope,
            invocation,
            fingerprint: spec.labels[crate::runtime::model::CONTAINER_SPEC_HASH_LABEL].clone(),
        };
        Ok(Self {
            spec,
            layout,
            scope,
            invocation,
            intent,
        })
    }

    pub(crate) fn intent(&self) -> ApplicationGatewayIntent {
        self.intent.clone()
    }

    pub(in crate::marketplace) fn owner_deployment(&self) -> &str {
        &self.intent.deployment
    }

    pub(crate) fn require_nonce(&self, invocation: Digest) -> Result<()> {
        if self.invocation != invocation {
            bail!("gateway belongs to another original invocation");
        }
        Ok(())
    }

    pub(crate) fn spec(&self) -> &ContainerSpec {
        &self.spec
    }

    /// Desired-config fixture only, not native/selection/installed authority.
    #[cfg(test)]
    pub(crate) fn fixture(
        deployment: &str,
        network_id: &str,
        image: &str,
        kind: &DownloadNetworkProfileKind,
        files: &[PreparedMaterialFile],
    ) -> Result<Self> {
        Self::compile(
            deployment,
            network_id,
            image,
            Layout::from_files(kind, files)?,
            Digest::of(b"gateway material fixture"),
            Digest::of(deployment),
        )
    }
    #[cfg(test)]
    pub(crate) fn warm_fixture(
        deployment: &str,
        network_id: &str,
        image: &str,
        kind: &DownloadNetworkProfileKind,
        files: &[PreparedMaterialFile],
    ) -> Result<Self> {
        Self::compile(
            deployment,
            network_id,
            image,
            Layout::from_files(kind, files)?,
            Self::warm_scope(),
            Digest::of(deployment),
        )
    }

    #[cfg(test)]
    pub(crate) fn short_watchdog_fixture(&mut self) {
        // Only the failed-input watchdog fixture differs from the real 180s
        // profile. Never count this as production-duration/CPU qualification.
        self.spec.command[3] = "3".into();
        apply_container_spec_fingerprint(&mut self.spec);
    }

    pub(in crate::marketplace) fn require_invocation(
        &self,
        scope: Digest,
        invocation: Digest,
    ) -> Result<()> {
        if self.scope != scope || self.invocation != invocation {
            bail!("application gateway belongs to another source invocation");
        }
        Ok(())
    }

    /// Consume only bounded normalized controller material, in fixed role
    /// order. This buffer must stay with its ORIGINAL owned pipe/CLI task until
    /// actual completion. A matching layout does not attest secret freshness;
    /// the live Selection must additionally be rechecked at effect boundaries.
    pub(in crate::marketplace) fn input(
        &self,
        kind: &DownloadNetworkProfileKind,
        files: &[PreparedMaterialFile],
    ) -> Result<Zeroizing<Vec<u8>>> {
        if Layout::from_files(kind, files)? != self.layout {
            bail!("gateway material changed after profile capture");
        }
        let mut bytes = Zeroizing::new(Vec::with_capacity(
            self.layout.config + self.layout.username + self.layout.password + TRAILER.len(),
        ));
        for role in [CONFIG_ROLE, USER_ROLE, PASSWORD_ROLE] {
            if let Some(file) = files.iter().find(|file| file.role == role) {
                bytes.extend_from_slice(file.contents());
            }
        }
        bytes.extend_from_slice(TRAILER);
        Ok(bytes)
    }

    #[cfg(test)]
    pub(crate) fn fixture_input(
        &self,
        kind: &DownloadNetworkProfileKind,
        files: &[PreparedMaterialFile],
    ) -> Result<Zeroizing<Vec<u8>>> {
        self.input(kind, files)
    }
}

fn hex(digest: Digest) -> String {
    digest.to_string().trim_start_matches("sha256:").to_owned()
}

#[cfg(test)]
mod tests;
