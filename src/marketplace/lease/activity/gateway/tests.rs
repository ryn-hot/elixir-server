//! Journal/OS-lease fixtures. Parent records here are tracking DATA, not actual
//! source execution, a grant or a VPN/readiness capability.
use super::*;
use crate::{
    live::{config::LiveEgressProfileKind, egress::prepare_gateway_material_from_secret_values},
    network::protection::DownloadNetworkProfileKind,
    runtime::docker::DockerRuntimeManager,
};

const WG: &[u8]=b"[Interface]\nPrivateKey = yAnzdtF2rM8Nl1N8MPm+2MvmFo0xSg6u40qCMgfHdC0=\nAddress = 10.64.0.2/32\n[Peer]\nPublicKey = fNBdb9h9NP7VDaRao7IhiHBpjz2uVH54camzato3tr0=\nAllowedIPs = 0.0.0.0/0\nEndpoint = 1.1.1.1:51820\n";
fn lane() -> Result<(tempfile::TempDir, ControllerLease, Fence)> {
    let temp = tempfile::tempdir()?;
    let lease = ControllerLease::acquire(&temp.path().canonicalize()?.join("lane"))?;
    let root = Fence::begin(
        &lease,
        Target::SourceWriter {
            deployment: "parent-fixture".into(),
            container_id: Some("f".repeat(64)),
        },
    )?;
    Ok((temp, lease, root))
}
async fn profile() -> Result<ApplicationGatewayProfile> {
    let files = prepare_gateway_material_from_secret_values(
        LiveEgressProfileKind::Wireguard,
        WG,
        None,
        None,
    )
    .await?;
    ApplicationGatewayProfile::fixture(
        "gateway-fixture",
        &"b".repeat(64),
        &format!("sha256:{}", "a".repeat(64)),
        &DownloadNetworkProfileKind::WireguardConfig,
        &files,
    )
}
async fn child(lease: &ControllerLease) -> Result<GatewayFence> {
    GatewayFence::begin(
        lease,
        &"f".repeat(64),
        Digest::of("gateway-fixture"),
        &profile().await?,
    )
}
fn no_docker() -> DockerRuntimeManager {
    DockerRuntimeManager::new(Some("/elixir-fixture-no-docker".into()))
}
async fn reject(
    fence: &mut GatewayFence,
    lease: &ControllerLease,
    attempt: GatewayCreateAttempt,
) -> Result<()> {
    let crate::runtime::gateway_creation::GatewayCreation::NotAttempted(outcome) = no_docker()
        .create_application_gateway(&fence.record.intent.profile()?, attempt)
        .await?
    else {
        bail!("fixture must not issue CREATE");
    };
    fence.not_attempted(lease, outcome)?;
    fence.retire(&no_docker(), lease).await
}

#[tokio::test]
async fn gateway_prepared_record_survives_owner_exit_and_blocks_parent_until_recovery() -> Result<()>
{
    let (_temp, lease, mut root) = lane()?;
    let fence = child(&lease).await?;
    let bytes = fs::read(lease.directory.join(GATEWAY_FILE_NAME))?;
    assert!(
        !String::from_utf8_lossy(&bytes).contains("standalone"),
        "legacy cold journals retain their exact canonical shape"
    );
    assert!(bytes.len() <= MAX_BYTES);
    assert!(!String::from_utf8_lossy(&bytes).contains("yAnzdtF2rM8Nl1N8MPm"));
    assert!(root.clear(&lease).is_err());
    let path = lease.directory.clone();
    drop(fence);
    drop(root);
    drop(lease);
    assert!(ControllerLease::acquire(&path).is_err());
    let lease = ControllerLease::acquire_for_recovery(&path)?;
    assert!(GatewayFence::recover(&no_docker(), &lease).await?);
    assert!(!GatewayFence::recover(&no_docker(), &lease).await?);
    Fence::load(&lease)?.unwrap().clear(&lease)?;
    drop(lease);
    ControllerLease::acquire(&path)?;
    Ok(())
}
#[tokio::test]
async fn gateway_attempt_is_single_use_and_only_original_precreate_rejection_unlocks() -> Result<()>
{
    let (_temp, lease, mut root) = lane()?;
    let mut fence = child(&lease).await?;
    let attempt = fence.creating(&lease)?;
    assert!(fence.creating(&lease).is_err());
    assert!(fence.bind(&lease, &"f".repeat(64)).is_err());
    assert!(root.clear(&lease).is_err());
    reject(&mut fence, &lease, attempt).await?;
    assert!(GatewayFence::load(&lease)?.is_none());
    root.clear(&lease)?;
    Ok(())
}
#[tokio::test]
async fn gateway_precreate_rejection_is_bound_to_original_operation_even_for_same_profile()
-> Result<()> {
    let (_a, lease_a, mut root_a) = lane()?;
    let (_b, lease_b, mut root_b) = lane()?;
    let mut a = child(&lease_a).await?;
    let mut b = child(&lease_b).await?;
    let original = a.creating(&lease_a)?;
    let other = b.creating(&lease_b)?;
    let crate::runtime::gateway_creation::GatewayCreation::NotAttempted(outcome) = no_docker()
        .create_application_gateway(&b.record.intent.profile()?, other)
        .await?
    else {
        bail!("fixture must not issue CREATE");
    };
    assert!(
        outcome
            .require(a.record.operation_id, &a.record.intent.profile()?)
            .is_err()
    );
    b.not_attempted(&lease_b, outcome)?;
    b.retire(&no_docker(), &lease_b).await?;
    root_b.clear(&lease_b)?;
    reject(&mut a, &lease_a, original).await?;
    root_a.clear(&lease_a)?;
    Ok(())
}
#[tokio::test]
async fn gateway_unknown_create_never_turns_error_drop_or_restart_into_absence_proof() -> Result<()>
{
    let (_temp, lease, mut root) = lane()?;
    let mut fence = child(&lease).await?;
    let _lost_attempt = fence.creating(&lease)?; // No completed outcome: deliberately fenced.
    assert!(fence.retire(&no_docker(), &lease).await.is_err());
    assert!(root.clear(&lease).is_err());
    let path = lease.directory.clone();
    drop(fence);
    drop(root);
    drop(lease);
    let lease = ControllerLease::acquire_for_recovery(&path)?;
    assert!(GatewayFence::recover(&no_docker(), &lease).await.is_err());
    assert!(lease.directory.join(GATEWAY_FILE_NAME).is_file());
    assert!(lease.directory.join(FILE_NAME).is_file());
    Ok(())
}
#[tokio::test]
async fn gateway_rejects_wrong_parent_nonce_and_replaced_or_corrupt_journal() -> Result<()> {
    let (_temp, lease, mut root) = lane()?;
    let p = profile().await?;
    assert!(
        GatewayFence::begin(&lease, &"e".repeat(64), Digest::of("gateway-fixture"), &p).is_err()
    );
    assert!(GatewayFence::begin(&lease, &"f".repeat(64), Digest::of("wrong"), &p).is_err());
    let mut fence = child(&lease).await?;
    let path = lease.directory.join(GATEWAY_FILE_NAME);
    let data = fs::read(&path)?;
    fs::rename(&path, lease.directory.join("old-gateway-record"))?;
    fs::write(&path, &data)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
    }
    assert!(fence.retire(&no_docker(), &lease).await.is_err());
    fs::write(&path, b"corrupt")?;
    assert!(GatewayFence::load(&lease).is_err());
    assert!(root.clear(&lease).is_err());
    fs::write(&path, vec![b'x'; MAX_BYTES + 1])?;
    assert!(GatewayFence::load(&lease).is_err());
    Ok(())
}
#[tokio::test]
async fn gateway_cannot_remove_its_record_before_original_http_child_is_retired() -> Result<()> {
    let (_temp, lease, mut root) = lane()?;
    let mut gateway = child(&lease).await?;
    let http_profile = crate::marketplace::runtime::application_http::ApplicationHttpProfile::new(
        "fixture",
        &format!("sha256:{}", "a".repeat(64)),
        &"e".repeat(64),
        Digest::of("source"),
        Digest::of("gateway-fixture"),
    )?;
    let mut http = http::HttpFence::begin(
        &lease,
        &"f".repeat(64),
        Digest::of("gateway-fixture"),
        &http_profile,
    )?;
    assert!(gateway.retire(&no_docker(), &lease).await.is_err());
    assert!(root.clear(&lease).is_err());
    http.retire(&no_docker(), &lease).await?;
    gateway.retire(&no_docker(), &lease).await?;
    root.clear(&lease)?;
    Ok(())
}
#[test]
fn gateway_orphan_or_malformed_marker_still_blocks_ordinary_global_lane() -> Result<()> {
    let (_temp, lease, mut root) = lane()?;
    root.clear(&lease)?;
    let path = lease.directory.clone();
    fs::write(path.join(GATEWAY_FILE_NAME), b"malformed")?;
    drop(lease);
    assert!(ControllerLease::acquire(&path).is_err());
    let lease = ControllerLease::acquire_for_recovery(&path)?;
    assert!(GatewayFence::load(&lease).is_err());
    Ok(())
}

mod native;
