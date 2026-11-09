//! Actual pinned Gluetun + isolated internal network. Qualifies private input,
//! ephemeral file handling and the launcher only; NO live VPN, grant, native
//! gateway ownership, bandwidth/admission or public HTTP path is manufactured.
use super::*;
use crate::{
    live::{config::LiveEgressProfileKind, egress::prepare_gateway_material_from_secret_values},
    marketplace::application_egress::{ApplicationGatewayProfile, ENTRYPOINT, MATERIAL_ACCEPTED},
    network::protection::DownloadNetworkProfileKind,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

const WG: &str = "[Interface]\nPrivateKey = yAnzdtF2rM8Nl1N8MPm+2MvmFo0xSg6u40qCMgfHdC0=\nAddress = 10.64.0.2/32\n[Peer]\nPublicKey = fNBdb9h9NP7VDaRao7IhiHBpjz2uVH54camzato3tr0=\nAllowedIPs = 0.0.0.0/0\nEndpoint = 1.1.1.1:51820\n";
const OVPN: &str = "client\ndev tun\nproto udp\nremote 1.1.1.1 1194\nnobind\npull\nremote-cert-tls server\nverify-x509-name vpn.example name\ntls-version-min 1.2\nauth-user-pass old.txt\n<ca>\ncertificate\n</ca>\n<tls-crypt>\nkey\n</tls-crypt>\n";
const USER: &[u8] = b"$(touch /gluetun/not-a-command)";
const PASSWORD: &[u8] = b"literal-';${IFS}test-password";

struct Emergency {
    container: Option<ContainerHandle>,
    labels: HashMap<String, String>,
    network: PacketNetworkSpec,
    network_id: String,
}
impl Drop for Emergency {
    fn drop(&mut self) {
        // Exact fixture resources + freshly checked ownership only. Emergency
        // cleanup never counts as a passed lifecycle or successful retirement.
        if let Some(handle) = &self.container
            && let Ok(output) = std::process::Command::new("docker")
                .args(["inspect", "--format", "{{json .Config.Labels}}", &handle.id])
                .output()
            && output.status.success()
            && serde_json::from_slice::<HashMap<String, String>>(&output.stdout)
                .is_ok_and(|actual| self.labels.iter().all(|(k, v)| actual.get(k) == Some(v)))
        {
            let _ = std::process::Command::new("docker")
                .args(["rm", "-f", &handle.id])
                .output();
        }
        if let Ok(output) = std::process::Command::new("docker")
            .args(["network", "inspect", &self.network_id])
            .output()
            && output.status.success()
            && serde_json::from_slice::<Vec<Value>>(&output.stdout)
                .map_err(anyhow::Error::from)
                .and_then(|values| {
                    values
                        .first()
                        .context("fixture network missing")
                        .and_then(PacketNetworkInspection::parse)
                })
                .is_ok_and(|actual| {
                    self.network.verify(&actual).is_ok() && actual.members.is_empty()
                })
        {
            let _ = std::process::Command::new("docker")
                .args(["network", "rm", &self.network_id])
                .output();
        }
    }
}
async fn discard(mut reader: impl AsyncRead + Unpin) -> Result<usize> {
    let mut buffer = [0; 4096];
    let mut total = 0_usize;
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            return Ok(total);
        }
        total = total.saturating_add(read);
    }
}

#[derive(Clone, Copy, Debug)]
enum Case {
    Wireguard,
    WireguardLarge,
    OpenvpnAuth,
    OpenvpnNoAuth,
    WrongTrailer,
    Truncated,
}

async fn case(case: Case, image: &str) -> Result<()> {
    let runtime = DockerRuntimeManager::new(None);
    // No endpoint can leave this fixture bridge, including the normalized
    // public test address, Gluetun health checks or a misconfigured tunnel.
    let deployment = format!("gateway-test-{}", Uuid::new_v4().simple());
    let network = PacketNetworkSpec::isolated_fixture(&deployment, false)?;
    let actual = runtime.ensure_packet_uplink(&network).await?;
    network.verify(&actual)?;
    let network_id = actual.id;
    let mut emergency = Emergency {
        container: None,
        labels: HashMap::new(),
        network,
        network_id,
    };
    let openvpn = matches!(case, Case::OpenvpnAuth | Case::OpenvpnNoAuth);
    let auth = matches!(case, Case::OpenvpnAuth);
    let kind = if openvpn {
        DownloadNetworkProfileKind::OpenvpnConfig
    } else {
        DownloadNetworkProfileKind::WireguardConfig
    };
    let config = if matches!(case, Case::WireguardLarge) {
        // A complete 256KiB normalized file must cross pipe/CLI chunks; dd
        // without fullblock would truncate on the first short read.
        format!("{WG}#{}\n", "x".repeat(256 * 1024 - WG.len() - 2))
    } else if matches!(case, Case::OpenvpnNoAuth) {
        OVPN.replace("auth-user-pass old.txt\n", "")
    } else if openvpn {
        OVPN.into()
    } else {
        WG.into()
    };
    let files = prepare_gateway_material_from_secret_values(
        if openvpn {
            LiveEgressProfileKind::Openvpn
        } else {
            LiveEgressProfileKind::Wireguard
        },
        config.as_bytes(),
        auth.then_some(USER),
        auth.then_some(PASSWORD),
    )
    .await?;
    let mut profile = ApplicationGatewayProfile::fixture(
        &deployment,
        &emergency.network_id,
        image,
        &kind,
        &files,
    )?;
    if matches!(case, Case::Truncated) {
        profile.short_watchdog_fixture();
    }
    let mut input = profile.fixture_input(&kind, &files)?;
    emergency.labels = profile.spec().labels.clone();
    // This create-only internal fixture does not introduce a production
    // creation API or a journal/lease substitute. No detached helper is used.
    let handle = runtime
        .create_container_entrypoint(profile.spec(), false, Some(ENTRYPOINT))
        .await?;
    emergency.container = Some(handle.clone());
    eprintln!("gateway material fixture {case:?}: {}", handle.id);
    let actual = runtime.inspect_container(&handle.id).await?;
    assert_eq!(
        actual.pointer("/State/Status").and_then(Value::as_str),
        Some("created")
    );
    assert_eq!(
        actual.pointer("/HostConfig/Init").and_then(Value::as_bool),
        Some(false)
    );
    assert_eq!(
        actual
            .pointer("/HostConfig/NetworkMode")
            .and_then(Value::as_str),
        Some(emergency.network_id.as_str())
    );
    assert_eq!(
        actual.pointer("/Config/Cmd"),
        Some(&serde_json::to_value(&profile.spec().command)?)
    );
    assert_eq!(
        actual
            .pointer("/HostConfig/LogConfig/Type")
            .and_then(Value::as_str),
        Some("none")
    );
    assert_eq!(
        actual
            .pointer("/HostConfig/ReadonlyRootfs")
            .and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        actual.pointer("/HostConfig/Memory").and_then(Value::as_u64),
        Some(128 * 1024 * 1024)
    );
    assert_eq!(
        actual
            .pointer("/HostConfig/MemorySwap")
            .and_then(Value::as_u64),
        Some(128 * 1024 * 1024)
    );
    assert_eq!(
        actual
            .pointer("/HostConfig/NanoCpus")
            .and_then(Value::as_u64),
        Some(100_000_000)
    );
    runtime
        .run_capture(&["start".into(), handle.id.clone()])
        .await?;
    let mut child = Command::new("docker")
        .args(["attach", "--sig-proxy=false", &handle.id])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let mut writer = child.stdin.take().context("fixture input")?;
    let mut reader = child.stdout.take().context("fixture output")?;
    let diagnostics = tokio::spawn(discard(child.stderr.take().context("fixture diagnostics")?));
    let success = matches!(
        case,
        Case::Wireguard | Case::WireguardLarge | Case::OpenvpnAuth | Case::OpenvpnNoAuth
    );
    if matches!(case, Case::WrongTrailer) {
        let last = input.len() - 1;
        input[last] ^= 1;
    }
    let sent = if matches!(case, Case::Truncated) {
        1
    } else {
        input.len() - 26
    };
    if matches!(case, Case::OpenvpnAuth) {
        writer.write_all(&input[..7]).await?;
        writer.flush().await?;
        tokio::time::sleep(TokioDuration::from_millis(100)).await;
        writer.write_all(&input[7..sent]).await?;
    } else {
        writer.write_all(&input[..sent]).await?;
    }
    writer.flush().await?;
    if !matches!(case, Case::Truncated) {
        // The process blocks at its fixed trailer. Inspect byte-identical
        // files and 0600 permissions before allowing any VPN executable.
        timeout(TokioDuration::from_secs(20), async {
            loop {
                let mut matched = true;
                for file in &files {
                    let path = match file.role {
                        "gateway_config" if !openvpn => "/gluetun/wireguard/wg0.conf",
                        "gateway_config" => "/gluetun/elixir-openvpn/custom.conf",
                        "openvpn_username" => "/gluetun/elixir-openvpn/username",
                        "openvpn_password" => "/gluetun/elixir-openvpn/password",
                        _ => bail!("unexpected fixture role"),
                    };
                    matched &= runtime
                        .run_capture(&[
                            "exec".into(),
                            handle.id.clone(),
                            "/bin/cat".into(),
                            path.into(),
                        ])
                        .await
                        .is_ok_and(|actual| actual.stdout.as_bytes() == file.contents());
                }
                if matched {
                    break;
                }
                tokio::time::sleep(TokioDuration::from_millis(100)).await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("gateway material write observation")??;
        let output = runtime
            .run_capture(&[
                "exec".into(),
                handle.id.clone(),
                "/bin/sh".into(),
                "-c".into(),
                "find /gluetun -type f -exec stat -c %a {} ';'".into(),
            ])
            .await?;
        assert_eq!(output.stdout.lines().count(), files.len());
        assert!(output.stdout.lines().all(|line| line == "600"));
        assert!(
            runtime
                .run_capture(&[
                    "exec".into(),
                    handle.id.clone(),
                    "/bin/cat".into(),
                    "/gluetun/not-a-command".into()
                ])
                .await
                .is_err()
        );
        writer.write_all(&input[sent..]).await?;
        writer.flush().await?;
    }
    drop(writer);
    if success {
        let mut accepted = [0; 36];
        timeout(
            TokioDuration::from_secs(20),
            reader.read_exact(&mut accepted),
        )
        .await??;
        assert_eq!(accepted, MATERIAL_ACCEPTED);
        // Material ACK is NOT tunnel readiness. Stop the exact fixture actor
        // even if Gluetun later exits because this is an offline/fake account.
        let state = runtime.inspect_container(&handle.id).await?;
        assert!(
            !state
                .pointer("/State/OOMKilled")
                .and_then(Value::as_bool)
                .unwrap_or(true)
        );
        if state.pointer("/State/Running").and_then(Value::as_bool) == Some(true) {
            runtime
                .run_capture(&["kill".into(), "--signal=KILL".into(), handle.id.clone()])
                .await?;
        }
    }
    let output = tokio::spawn(discard(reader));
    let status = timeout(TokioDuration::from_secs(15), child.wait())
        .await
        .context("original gateway fixture attach join")??;
    let remaining = timeout(TokioDuration::from_secs(15), output).await???;
    timeout(TokioDuration::from_secs(15), diagnostics).await???;
    let actual = runtime.inspect_container(&handle.id).await?;
    assert_eq!(
        actual.pointer("/State/Running").and_then(Value::as_bool),
        Some(false)
    );
    if !success {
        assert!(!status.success());
        assert_eq!(
            remaining, 0,
            "invalid material must not ACK or start Gluetun"
        );
        let code = actual
            .pointer("/State/ExitCode")
            .and_then(Value::as_i64)
            .context("invalid-input native exit observation missing")?;
        assert_ne!(code, 0);
    }
    runtime.remove_container(&handle).await?;
    emergency.container = None;
    runtime
        .remove_packet_uplink(&emergency.network, &emergency.network_id)
        .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires local immutable Gluetun image and Docker; isolated boot/input only, no live VPN"]
async fn native_application_gateway_private_material_boot_and_fail_closed_input() -> Result<()> {
    let image = std::env::var("ELIXIR_TEST_APPLICATION_GATEWAY_IMAGE")
        .context("explicit immutable test image required")?;
    if !image.strip_prefix("sha256:").is_some_and(immutable_id) {
        bail!("immutable Gluetun test image required");
    }
    for variant in [
        Case::Wireguard,
        Case::WireguardLarge,
        Case::OpenvpnAuth,
        Case::OpenvpnNoAuth,
        Case::WrongTrailer,
        Case::Truncated,
    ] {
        case(variant, &image).await?;
    }
    Ok(())
}
