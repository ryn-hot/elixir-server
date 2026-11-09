//! Local journal/phase tests only. No test constructs RuntimeImage from JSON.
use super::*;
use elixir_marketplace::container_image::import::ImportIntent;
fn setup() -> Result<(tempfile::TempDir, ControllerLease, Fence)> {
    let temp = tempfile::tempdir()?;
    let lease = ControllerLease::acquire(&temp.path().canonicalize()?.join("lane"))?;
    let intent = ImportIntent {
        deployment: "reference-unit".into(),
        request: Digest::of(b"request"),
    };
    let fence = Fence::begin(&lease, Target::ImageImport { intent })?;
    Ok((temp, lease, fence))
}
fn adopted() -> Result<(tempfile::TempDir, ControllerLease, Record)> {
    let (temp, lease, mut fence) = setup()?;
    prepare(&lease, &fence)?;
    let (_, record) = read(&lease, FILE)?;
    fence.adopt_image_reference(&lease)?;
    fence.clear(&lease)?;
    Ok((temp, lease, record))
}
#[test]
fn passive_handoff_persists_exact_original_operation_and_does_not_start_an_actor() -> Result<()> {
    let (_temp, lease, record) = adopted()?;
    assert!(Fence::load(&lease)?.is_none());
    require_active(&lease, record.operation_id, record.intent())?;
    assert!(require_absent(&lease).is_err());
    let directory = lease.directory().to_path_buf();
    drop(lease);
    assert!(ControllerLease::acquire(&directory).is_err());
    let recovery = ControllerLease::acquire_for_recovery(&directory)?;
    require_active(&recovery, record.operation_id, record.intent())?;
    Ok(())
}
#[tokio::test]
async fn terminal_transition_can_finish_after_restart_without_a_daemon_or_an_import() -> Result<()>
{
    let (_temp, lease, record) = adopted()?;
    // Synthetic terminal journal ONLY; this tests atomic-file recovery, not a
    // real daemon cleanup receipt. The separate native case proves that path.
    let mut terminal = record.clone();
    terminal.target = Kind::ImageImportRetired {
        intent: record.intent().clone(),
    };
    write_new(&lease, NEXT, &terminal)?;
    let directory = lease.directory().to_path_buf();
    drop(lease);
    let recovery = ControllerLease::acquire_for_recovery(&directory)?;
    assert!(recover(&super::super::peer_tests::Unavailable, &recovery).await?);
    require_absent(&recovery)?;
    drop(recovery);
    ControllerLease::acquire(&directory)?;
    Ok(())
}
#[tokio::test]
async fn image_cleanup_never_overtakes_an_original_native_actor() -> Result<()> {
    let (_temp, lease, _record) = adopted()?;
    let _actor = Fence::begin(
        &lease,
        Target::Measurement {
            deployment: "reference-unit".into(),
            container_id: None,
        },
    )?;
    assert!(
        recover(&super::super::peer_tests::Unavailable, &lease)
            .await
            .is_err()
    );
    assert!(!absent(&lease, FILE)?);
    Ok(())
}
#[test]
fn foreign_terminal_transition_does_not_replace_active_reference() -> Result<()> {
    let (_temp, lease, record) = adopted()?;
    let foreign = Record {
        schema_version: 1,
        operation_id: Uuid::new_v4(),
        target: Kind::ImageImportRetired {
            intent: record.intent().clone(),
        },
    };
    write_new(&lease, NEXT, &foreign)?;
    assert!(settle_next(&lease).is_err());
    assert!(read(&lease, FILE)?.1 == record);
    assert!(require_active(&lease, record.operation_id, record.intent()).is_err());
    Ok(())
}
#[test]
fn copied_or_missing_reference_cannot_clear_an_adoption_handoff() -> Result<()> {
    let (_temp, lease, mut fence) = setup()?;
    assert!(fence.adopt_image_reference(&lease).is_err());
    prepare(&lease, &fence)?;
    fence.adopt_image_reference(&lease)?;
    fs::remove_file(lease.directory().join(FILE))?;
    assert!(fence.clear(&lease).is_err());
    assert!(Fence::load(&lease)?.is_some());
    Ok(())
}
#[test]
fn symlink_hardlink_and_oversized_roles_are_never_cleanup_records() -> Result<()> {
    let (_temp, lease, _fence) = setup()?;
    std::os::unix::fs::symlink(
        lease.directory().join("missing"),
        lease.directory().join(FILE),
    )?;
    assert!(read(&lease, FILE).is_err());
    assert!(require_absent(&lease).is_err());
    fs::remove_file(lease.directory().join(FILE))?;
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(lease.directory().join(FILE))?;
    file.set_len(4097)?;
    assert!(read(&lease, FILE).is_err());
    file.set_len(1)?;
    fs::hard_link(
        lease.directory().join(FILE),
        lease.directory().join("foreign"),
    )?;
    assert!(read(&lease, FILE).is_err());
    Ok(())
}
#[test]
fn interrupted_initial_handoff_can_mirror_already_confirmed_root_retirement() -> Result<()> {
    let (_temp, lease, fence) = setup()?;
    prepare(&lease, &fence)?;
    let (_, record) = read(&lease, FILE)?;
    // Phase-data test only; no native cleanup or image capability is implied.
    mirror_retired(&lease, record.operation_id, record.intent())?;
    assert!(read(&lease, FILE)?.1.retired());
    assert!(require_active(&lease, record.operation_id, record.intent()).is_err());
    Ok(())
}
