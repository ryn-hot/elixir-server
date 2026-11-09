//! Real Docker create/start/stop/recovery with a synthetic PARENT JOURNAL only.
//! No fake NativeScope/Ready/grant, no material input or VPN/public HTTP effect.
use super::*;
use crate::runtime::packet_network::{PacketNetworkInspection, PacketNetworkSpec};
use std::process::Command;

struct Emergency {
    actor: Option<ContainerHandle>,
    labels: std::collections::HashMap<String, String>,
    network: PacketNetworkSpec,
    network_id: String,
}
impl Drop for Emergency {
    fn drop(&mut self) {
        if let Some(h) = &self.actor
            && let Ok(out) = Command::new("docker")
                .args(["inspect", "--format", "{{json .Config.Labels}}", &h.id])
                .output()
            && out.status.success()
            && serde_json::from_slice::<std::collections::HashMap<String, String>>(&out.stdout)
                .is_ok_and(|actual| self.labels.iter().all(|(k, v)| actual.get(k) == Some(v)))
        {
            let _ = Command::new("docker").args(["rm", "-f", &h.id]).output();
        }
        if let Ok(out) = Command::new("docker")
            .args(["network", "inspect", &self.network_id])
            .output()
            && out.status.success()
            && serde_json::from_slice::<Vec<serde_json::Value>>(&out.stdout)
                .map_err(anyhow::Error::from)
                .and_then(|rows| {
                    rows.first()
                        .context("fixture network missing")
                        .and_then(PacketNetworkInspection::parse)
                })
                .is_ok_and(|actual| {
                    self.network.verify(&actual).is_ok() && actual.members.is_empty()
                })
        {
            let _ = Command::new("docker")
                .args(["network", "rm", &self.network_id])
                .output();
        }
    }
}
async fn scenario(image: &str, started: bool) -> Result<()> {
    let runtime = DockerRuntimeManager::new(None);
    let deployment = format!("gateway-journal-{}", Uuid::new_v4().simple());
    let network = PacketNetworkSpec::isolated_fixture(&deployment, false)?;
    let actual = runtime.ensure_packet_uplink(&network).await?;
    network.verify(&actual)?;
    let mut emergency = Emergency {
        actor: None,
        labels: Default::default(),
        network,
        network_id: actual.id,
    };
    let files = prepare_gateway_material_from_secret_values(
        LiveEgressProfileKind::Wireguard,
        WG,
        None,
        None,
    )
    .await?;
    let profile = ApplicationGatewayProfile::fixture(
        &deployment,
        &emergency.network_id,
        image,
        &DownloadNetworkProfileKind::WireguardConfig,
        &files,
    )?;
    let (_temp, lease, mut root) = lane()?;
    let mut fence =
        GatewayFence::begin(&lease, &"f".repeat(64), Digest::of(&deployment), &profile)?;
    let attempt = fence.creating(&lease)?;
    let handle = match runtime
        .create_application_gateway(&profile, attempt)
        .await?
    {
        crate::runtime::gateway_creation::GatewayCreation::Created(handle) => handle,
        crate::runtime::gateway_creation::GatewayCreation::NotAttempted(outcome) => {
            return Err(outcome.into_error());
        }
    };
    eprintln!("native gateway journal started={started}: {}", handle.id);
    emergency.actor = Some(handle.clone());
    emergency.labels = profile.spec().labels.clone();
    if started {
        fence.bind(&lease, &handle.id)?;
        runtime.start_application_gateway(&handle, &profile).await?;
        assert!(
            runtime
                .start_application_gateway(&handle, &profile)
                .await
                .is_err()
        );
        assert!(
            runtime
                .remove_application_gateway(&handle, &profile)
                .await
                .is_err()
        );
        // A prepared HTTP child must prevent namespace removal, even after an
        // immediate native stop. No actual HTTP actor or request is created.
        let http_profile =
            crate::marketplace::runtime::application_http::ApplicationHttpProfile::new(
                &deployment,
                image,
                &handle.id,
                Digest::of("fixture source"),
                Digest::of(&deployment),
            )?;
        let mut http = http::HttpFence::begin(
            &lease,
            &"f".repeat(64),
            Digest::of(&deployment),
            &http_profile,
        )?;
        assert!(fence.retire(&runtime, &lease).await.is_err());
        assert!(runtime.get_container_handle(&handle.id).await?.is_some());
        http.retire(&runtime, &lease).await?;
    }
    assert!(root.clear(&lease).is_err());
    let path = lease.directory.clone();
    drop(fence);
    drop(root);
    drop(lease);
    assert!(ControllerLease::acquire(&path).is_err());
    let recovery = ControllerLease::acquire_for_recovery(&path)?;
    // started=false deliberately lost its create reply BEFORE CID bind.
    // Recovery must observe exact Created state, persist CID, then retire it.
    GatewayFence::recover(&runtime, &recovery).await?;
    assert!(runtime.get_container_handle(&handle.id).await?.is_none());
    emergency.actor = None;
    Fence::load(&recovery)?
        .context("parent journal disappeared")?
        .clear(&recovery)?;
    drop(recovery);
    ControllerLease::acquire(&path)?;
    runtime
        .remove_packet_uplink(&emergency.network, &emergency.network_id)
        .await?;
    Ok(())
}
#[tokio::test]
#[ignore = "exclusive actual Docker gateway journal/unbound-Created and bound-stop/restart cleanup; synthetic parent journal, no VPN/HTTP"]
async fn native_gateway_journal_recovers_original_created_and_started_actors_without_replacement()
-> Result<()> {
    let image = std::env::var("ELIXIR_TEST_APPLICATION_GATEWAY_IMAGE")
        .context("explicit immutable Gluetun image required")?;
    scenario(&image, false).await?;
    scenario(&image, true).await
}
