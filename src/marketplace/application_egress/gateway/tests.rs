//! Configuration/framing only. Native tests separately exercise the real
//! pinned image; neither is a VPN/privacy, aggregate or playback attestation.
use super::*;
use crate::live::{
    config::LiveEgressProfileKind, egress::prepare_gateway_material_from_secret_values,
};
use crate::runtime::model::{CONTAINER_SPEC_HASH_LABEL, container_spec_fingerprint};

const WG: &str = "[Interface]\nPrivateKey = yAnzdtF2rM8Nl1N8MPm+2MvmFo0xSg6u40qCMgfHdC0=\nAddress = 10.64.0.2/32\n[Peer]\nPublicKey = fNBdb9h9NP7VDaRao7IhiHBpjz2uVH54camzato3tr0=\nAllowedIPs = 0.0.0.0/0\nEndpoint = 1.1.1.1:51820\n";
const OVPN: &str = "client\ndev tun\nproto udp\nremote 1.1.1.1 1194\nnobind\npull\nremote-cert-tls server\nverify-x509-name vpn.example name\ntls-version-min 1.2\nauth-user-pass old.txt\n<ca>\ncertificate\n</ca>\n<tls-crypt>\nkey\n</tls-crypt>\n";
const USER: &[u8] = b"$(touch /gluetun/not-a-command)";
const PASSWORD: &[u8] = b"literal-';${IFS}test-password";

#[tokio::test]
async fn gateway_recovery_intent_round_trips_only_the_original_closed_configuration() -> Result<()>
{
    let kind = DownloadNetworkProfileKind::WireguardConfig;
    let files = files(&kind).await?;
    let original = profile(&kind, &files)?;
    let bytes = elixir_marketplace::canonical::to_vec(&original.intent())?;
    assert!(!String::from_utf8_lossy(&bytes).contains("yAnzdtF2rM8Nl1N8MPm"));
    let decoded: ApplicationGatewayIntent = elixir_marketplace::canonical::decode(&bytes, 4096)?;
    let restored = decoded.profile()?;
    assert_eq!(
        restored.spec().labels[CONTAINER_SPEC_HASH_LABEL],
        original.spec().labels[CONTAINER_SPEC_HASH_LABEL]
    );
    let mut changed = decoded.clone();
    changed.layout.config += 1;
    assert!(changed.profile().is_err());
    let mut changed = decoded;
    changed.layout.config = 0;
    assert!(changed.profile().is_err());
    Ok(())
}

#[tokio::test]
async fn gateway_reaper_is_fingerprint_bound_and_legacy_intents_keep_their_original_topology()
-> Result<()> {
    let kind = DownloadNetworkProfileKind::WireguardConfig;
    let files = files(&kind).await?;
    let modern = profile(&kind, &files)?;
    assert!(modern.spec().security.init_process);
    let mut layout = Layout::from_files(&kind, &files)?;
    layout.reap_children = false;
    let legacy = ApplicationGatewayProfile::compile(
        "storage-fixture",
        &"b".repeat(64),
        &format!("sha256:{}", "a".repeat(64)),
        layout,
        Digest::of(b"source"),
        Digest::of(b"invocation"),
    )?;
    let encoded = elixir_marketplace::canonical::to_vec(&legacy.intent())?;
    assert!(!String::from_utf8_lossy(&encoded).contains("reap_children"));
    assert!(!serde_json::to_string(legacy.spec())?.contains("init_process"));
    let decoded: ApplicationGatewayIntent = elixir_marketplace::canonical::decode(&encoded, 4096)?;
    assert!(!decoded.profile()?.spec().security.init_process);
    assert_ne!(legacy.intent().fingerprint, modern.intent().fingerprint);
    let mut changed = decoded;
    changed.layout.reap_children = true;
    assert!(
        changed.profile().is_err(),
        "recovery cannot silently change frozen topology"
    );
    let mut changed = modern.intent().clone();
    changed.layout.reap_children = false;
    assert!(
        changed.profile().is_err(),
        "new tunnels cannot lose their original reaper"
    );
    Ok(())
}

async fn files(kind: &DownloadNetworkProfileKind) -> Result<Vec<PreparedMaterialFile>> {
    let (kind, config, username, password) = match kind {
        DownloadNetworkProfileKind::WireguardConfig => {
            (LiveEgressProfileKind::Wireguard, WG, None, None)
        }
        DownloadNetworkProfileKind::OpenvpnConfig => (
            LiveEgressProfileKind::Openvpn,
            OVPN,
            Some(USER),
            Some(PASSWORD),
        ),
        _ => bail!("unexpected fixture kind"),
    };
    Ok(
        prepare_gateway_material_from_secret_values(kind, config.as_bytes(), username, password)
            .await?,
    )
}
fn profile(
    kind: &DownloadNetworkProfileKind,
    files: &[PreparedMaterialFile],
) -> Result<ApplicationGatewayProfile> {
    ApplicationGatewayProfile::compile(
        "storage-fixture",
        &"b".repeat(64),
        &format!("sha256:{}", "a".repeat(64)),
        Layout::from_files(kind, files)?,
        Digest::of(b"source"),
        Digest::of(b"invocation"),
    )
}

#[tokio::test]
async fn application_gateway_is_ephemeral_bounded_and_not_a_main_reconciler_deployment()
-> Result<()> {
    let kind = DownloadNetworkProfileKind::WireguardConfig;
    let files = files(&kind).await?;
    let profile = profile(&kind, &files)?;
    let spec = profile.spec();
    assert_eq!(spec.network, "b".repeat(64));
    assert!(spec.network_mode.is_none() && spec.ports.is_empty() && spec.aliases.is_empty());
    assert!(spec.volumes.is_empty());
    assert!(spec.labels[ELIXIR_DEPLOYMENT_ID_LABEL].starts_with("application-gateway-"));
    assert_ne!(spec.labels[ELIXIR_DEPLOYMENT_ID_LABEL], "storage-fixture");
    assert_eq!(
        spec.labels[CONTAINER_SPEC_HASH_LABEL],
        container_spec_fingerprint(spec)
    );
    assert_eq!(spec.cap_drop, ["ALL"]);
    assert_eq!(spec.devices, ["/dev/net/tun:/dev/net/tun"]);
    let security = &spec.security;
    assert!(security.read_only_rootfs && security.no_new_privileges && security.stdin_open);
    assert!(security.init_process);
    assert_eq!(security.user.as_deref(), Some("0:0"));
    assert_eq!(security.memory_limit_mb, Some(128));
    assert_eq!(security.memory_swap_limit_mb, Some(128));
    assert_eq!(security.cpu_quota.as_deref(), Some("0.100"));
    assert_eq!(security.pids_limit, Some(64));
    assert_eq!(security.log_options, Some(ContainerLogOptions::Disabled));
    assert_eq!(security.tmpfs.len(), 2);
    for mount in &security.tmpfs {
        assert!(matches!(mount.path.as_str(), "/tmp" | "/gluetun"));
        assert_eq!(mount.size_mb, Some(16));
        assert_eq!(mount.mode, Some(0o700));
        assert!(mount.no_exec && mount.no_dev && mount.no_suid);
    }
    assert_eq!(&spec.command[..4], ["timeout", "-s", "KILL", "180"]);
    let launcher = &spec.command[6];
    assert!(!launcher.contains("__ELIXIR_INSTALL_APPLICATION_GUARD__"));
    assert!(launcher.find("-N ELIXIR_APP").unwrap() < launcher.find("read_file()").unwrap());
    assert!(launcher.contains("--uid-owner 10005"));
    assert!(launcher.contains("! -o tun0 -j DROP"));
    assert!(launcher.contains("--hashlimit-htable-max 1"));
    assert!(launcher.contains("--hashlimit-above 2048kb/s"));
    assert!(launcher.contains("iptables-nft ip6tables-nft"));
    Ok(())
}

#[tokio::test]
async fn application_gateway_never_places_material_in_oci_configuration() -> Result<()> {
    for kind in [
        DownloadNetworkProfileKind::WireguardConfig,
        DownloadNetworkProfileKind::OpenvpnConfig,
    ] {
        let files = files(&kind).await?;
        let profile = profile(&kind, &files)?;
        let json = serde_json::to_string(profile.spec())?;
        for secret in [
            "yAnzdtF2rM8Nl1N8MPm+2MvmFo0xSg6u40qCMgfHdC0=",
            std::str::from_utf8(USER)?,
            std::str::from_utf8(PASSWORD)?,
        ] {
            assert!(!json.contains(secret));
        }
        let env: HashMap<_, _> = profile
            .spec()
            .env
            .iter()
            .map(|v| (v.name.as_str(), v.value.as_str()))
            .collect();
        assert_eq!(env.get("FIREWALL_OUTBOUND_SUBNETS"), Some(&""));
        assert_eq!(env.get("FIREWALL_INPUT_PORTS"), Some(&""));
        assert_eq!(env.get("FIREWALL_VPN_INPUT_PORTS"), Some(&""));
        assert_eq!(env.get("HEALTH_RESTART_VPN"), Some(&"off"));
        assert_eq!(env.get("PUBLICIP_ENABLED"), Some(&"off"));
        assert_eq!(env.get("PUID"), Some(&"0"));
        assert_eq!(env.get("PGID"), Some(&"0"));
        for name in ["WIREGUARD_PRIVATE_KEY", "OPENVPN_USER", "OPENVPN_PASSWORD"] {
            assert!(!env.contains_key(name));
        }
    }
    Ok(())
}

#[tokio::test]
async fn application_gateway_material_order_is_fixed_and_literal_not_shell_text() -> Result<()> {
    let kind = DownloadNetworkProfileKind::OpenvpnConfig;
    let mut files = files(&kind).await?;
    let profile = profile(&kind, &files)?;
    let expected = profile.input(&kind, &files)?;
    files.reverse();
    assert_eq!(*profile.input(&kind, &files)?, *expected);
    let mut offset = 0;
    for role in [CONFIG_ROLE, USER_ROLE, PASSWORD_ROLE] {
        let file = files.iter().find(|f| f.role == role).unwrap();
        assert_eq!(
            &expected[offset..offset + file.contents().len()],
            file.contents()
        );
        offset += file.contents().len();
    }
    assert_eq!(&expected[offset..], TRAILER);
    assert_eq!(TRAILER.len(), 26);
    assert!(LAUNCH.contains("dd bs=26 count=1 iflag=fullblock"));
    assert!(!LAUNCH.contains("head -c"));
    assert!(LAUNCH.contains(std::str::from_utf8(TRAILER)?));
    Ok(())
}

#[tokio::test]
async fn application_gateway_rejects_duplicates_wrong_paths_missing_auth_and_changed_layout()
-> Result<()> {
    let kind = DownloadNetworkProfileKind::OpenvpnConfig;
    let mut files = files(&kind).await?;
    let profile = profile(&kind, &files)?;
    files[0].file_name = "../../elsewhere";
    assert!(Layout::from_files(&kind, &files).is_err());
    files[0].file_name = "custom.conf";
    files[1].role = CONFIG_ROLE;
    assert!(Layout::from_files(&kind, &files).is_err());
    files[1].role = USER_ROLE;
    files.pop();
    assert!(Layout::from_files(&kind, &files).is_err());
    files.pop();
    // No-auth OpenVPN is a different closed layout, not a retry of this one.
    assert!(profile.input(&kind, &files).is_err());
    assert!(Layout::from_files(&DownloadNetworkProfileKind::WireguardConfig, &files).is_err());
    assert!(Layout::from_files(&DownloadNetworkProfileKind::CloudflareWarp, &files).is_err());
    Ok(())
}

#[test]
fn application_gateway_length_limits_are_finite_and_kind_specific() -> Result<()> {
    let good = Layout {
        kind: Kind::Wireguard,
        config: CONFIG_LIMIT,
        username: 0,
        password: 0,
        mtu: None,
        reap_children: true,
    };
    good.validate()?;
    for bad in [
        Layout {
            mtu: Some(575),
            ..good
        },
        Layout {
            kind: Kind::Openvpn,
            mtu: Some(1280),
            ..good
        },
        Layout { config: 0, ..good },
        Layout {
            config: CONFIG_LIMIT + 1,
            ..good
        },
        Layout {
            username: 1,
            ..good
        },
        Layout {
            password: 1,
            ..good
        },
        Layout {
            kind: Kind::OpenvpnAuth,
            username: 1,
            ..good
        },
        Layout {
            kind: Kind::OpenvpnAuth,
            username: AUTH_LIMIT + 1,
            password: 1,
            ..good
        },
    ] {
        assert!(bad.validate().is_err());
    }
    Layout {
        kind: Kind::OpenvpnAuth,
        config: CONFIG_LIMIT,
        username: AUTH_LIMIT,
        password: AUTH_LIMIT,
        mtu: None,
        reap_children: true,
    }
    .validate()?;
    Ok(())
}

#[test]
fn wireguard_mtu_is_bounded_interface_metadata_not_secret_or_shell_text() -> Result<()> {
    assert_eq!(wireguard_mtu(WG.as_bytes())?, None);
    for mtu in [576, 1280, 1420, 65535] {
        assert_eq!(
            wireguard_mtu(format!("[interface]\n mtU = {mtu}\n[Peer]\n").as_bytes())?,
            Some(mtu)
        );
    }
    for input in [
        "[Interface]\nMTU=0",
        "[Interface]\nMTU=575",
        "[Interface]\nMTU=65536",
        "[Interface]\nMTU=$(command)",
        "[Interface]\nMTU=1280\nmtu=1280",
        "[Peer]\nMTU=1280",
        "MTU=1280",
    ] {
        assert!(wireguard_mtu(input.as_bytes()).is_err());
    }
    Ok(())
}

#[tokio::test]
async fn normalized_wireguard_mtu_is_frozen_in_gateway_and_recovery_without_credentials()
-> Result<()> {
    let config = WG.replace("[Peer]", "MTU = 1280\n[Peer]");
    let files = prepare_gateway_material_from_secret_values(
        LiveEgressProfileKind::Wireguard,
        config.as_bytes(),
        None,
        None,
    )
    .await?;
    let profile = profile(&DownloadNetworkProfileKind::WireguardConfig, &files)?;
    assert_eq!(profile.layout.mtu, Some(1280));
    assert_eq!(
        profile
            .spec()
            .env
            .iter()
            .find(|v| v.name == "WIREGUARD_MTU")
            .map(|v| v.value.as_str()),
        Some("1280")
    );
    let bytes = elixir_marketplace::canonical::to_vec(&profile.intent())?;
    assert!(!String::from_utf8_lossy(&bytes).contains("yAnzdtF2rM8Nl1N8MPm"));
    let recovered: ApplicationGatewayIntent = elixir_marketplace::canonical::decode(&bytes, 4096)?;
    assert_eq!(
        recovered.profile()?.spec().labels[CONTAINER_SPEC_HASH_LABEL],
        profile.spec().labels[CONTAINER_SPEC_HASH_LABEL]
    );
    assert!(super::super::observation::OBSERVE_GUARD.contains("/sys/class/net/tun0/mtu"));
    Ok(())
}

#[tokio::test]
async fn application_gateway_binds_original_network_image_and_source_invocation() -> Result<()> {
    let kind = DownloadNetworkProfileKind::WireguardConfig;
    let files = files(&kind).await?;
    let good = profile(&kind, &files)?;
    let scope = good.scope;
    let invocation = good.invocation;
    good.require_invocation(scope, invocation)?;
    assert!(
        good.require_invocation(Digest::of(b"other"), invocation)
            .is_err()
    );
    assert!(
        good.require_invocation(scope, Digest::of(b"other"))
            .is_err()
    );
    for (deployment, network, image) in [
        (
            "../unsafe".to_owned(),
            "b".repeat(64),
            format!("sha256:{}", "a".repeat(64)),
        ),
        (
            "fixture".into(),
            "bridge".into(),
            format!("sha256:{}", "a".repeat(64)),
        ),
        (
            "fixture".into(),
            "b".repeat(64),
            "qmcgaw/gluetun:latest".into(),
        ),
    ] {
        assert!(
            ApplicationGatewayProfile::compile(
                &deployment,
                &network,
                &image,
                good.layout,
                scope,
                invocation
            )
            .is_err()
        );
    }
    let repeat = profile(&kind, &files)?;
    assert_eq!(
        good.spec.labels[CONTAINER_SPEC_HASH_LABEL],
        repeat.spec.labels[CONTAINER_SPEC_HASH_LABEL]
    );
    Ok(())
}
