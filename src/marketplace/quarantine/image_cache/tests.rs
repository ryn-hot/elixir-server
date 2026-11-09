use super::*;
use std::os::unix::fs::PermissionsExt;

fn cleanup_fixture() -> Result<(tempfile::TempDir, Slot)> {
    use elixir_marketplace::{
        container_image::ContainerCommand, records::Blob, reproduction::BuildPlatform,
    };
    // Filesystem transition fixture ONLY. These simulated retired controls
    // cannot create an Anchor/RuntimeImage or qualify any native retirement.
    let root = tempfile::tempdir()?;
    let parent = root.path().canonicalize()?;
    let lane = parent.join("cache");
    let lease = Arc::new(ControllerLease::acquire_for_recovery(&lane)?);
    let socket = parent.join("daemon.sock");
    let _listener = std::os::unix::net::UnixListener::bind(&socket)?;
    let binary = parent.join("never-execute");
    std::fs::write(&binary, b"not an executable keeper")?;
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700))?;
    let profile = Arc::new(Profile::from_operator_configuration(
        binary,
        Digest::of(b"not an executable keeper"),
        socket,
    )?);
    let core = Arc::new(Core {
        lease,
        profile,
        runtime: Arc::new(super::super::peer_tests::Unavailable),
    });
    let request = ReferenceRequest {
        schema_version: 1,
        operation_id: "11111111-2222-4333-8444-555555555555".into(),
        daemon_id: "fixture".into(),
        storage_namespace: format!("sha256:{}", core.lease.storage_namespace()?).parse()?,
        socket: core.profile.socket(),
        expires_unix_millis: 1,
        image: ContainerImageSummary {
            schema_version: 1,
            release: Digest::of(b"r"),
            artifact: Blob {
                sha256: Digest::of(b"a"),
                size: 4096,
            },
            platform: BuildPlatform::LinuxAmd64,
            manifest: Digest::of(b"m"),
            config: Digest::of(b"c"),
            layers: vec![Blob {
                sha256: Digest::of(b"l"),
                size: 1024,
            }],
            command: ContainerCommand {
                entrypoint: vec!["/app/provider".into()],
                arguments: vec![],
                environment: Default::default(),
                working_directory: "/app".into(),
            },
        },
    };
    let file = core.write(&request)?;
    for (name, bytes) in [
        ("image-cache.lock", Vec::new()),
        ("image-cache.retired", request.receipt()?),
    ] {
        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(lane.join(name))?;
        f.write_all(&bytes)?;
        f.sync_all()?;
    }
    Ok((
        root,
        Slot {
            core,
            pending: Some(Pending {
                request,
                file: Some(file),
                child: None,
            }),
            entry: None,
            cleanup: None,
        },
    ))
}

#[test]
fn failed_write_cleanup_preserves_a_replaced_inode_and_existing_controls() -> Result<()> {
    let (_root, slot) = cleanup_fixture()?;
    let directory = slot.core.lease.directory();
    let request = std::fs::read(directory.join(REQUEST))?;
    assert!(
        slot.core
            .write_body(REQUEST, b"must not replace existing data")
            .is_err()
    );
    assert_eq!(std::fs::read(directory.join(REQUEST))?, request);
    let original = slot.core.write_body(CLEARING, b"original")?;
    std::fs::rename(
        directory.join(CLEARING),
        directory.join("original-private-control"),
    )?;
    std::fs::write(directory.join(CLEARING), b"foreign replacement")?;
    assert!(slot.core.retire_failed_write(CLEARING, &original).is_err());
    assert_eq!(
        std::fs::read(directory.join(CLEARING))?,
        b"foreign replacement"
    );
    assert_eq!(
        std::fs::read(directory.join("original-private-control"))?,
        b"original"
    );
    Ok(())
}

#[test]
fn kernel_file_limit_failures_do_not_poison_new_cache_controls() -> Result<()> {
    // RLIMIT_FSIZE and SIGXFSZ are process-wide: never change the server/test
    // runner's limits. Only a single explicitly selected child gets the limit.
    let mut command = std::process::Command::new(std::env::current_exe()?);
    command
        .args([
            "marketplace::quarantine::image_cache::tests::kernel_file_limit_child",
            "--exact",
            "--ignored",
            "--nocapture",
        ])
        .env_clear();
    if let Some(directory) = std::env::var_os("TMPDIR") {
        command.env("TMPDIR", directory);
    }
    let output = command.output()?;
    assert!(
        output.status.success(),
        "bounded write-failure child failed: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("kernel EFBIG: empty/partial request and clearing writes retired without replacing original controls"));
    Ok(())
}

#[test]
fn failed_request_rollback_excludes_a_live_keeper_and_preserves_attempted_work() -> Result<()> {
    for attempted in [false, true] {
        let (_root, slot) = cleanup_fixture()?;
        let directory = slot.core.lease.directory();
        std::fs::remove_file(directory.join(REQUEST))?;
        std::fs::remove_file(directory.join("image-cache.retired"))?;
        let original = slot
            .core
            .write_body(REQUEST, b"incomplete original request")?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .open(directory.join("image-cache.lock"))?;
        if attempted {
            std::fs::write(
                directory.join("image-cache.attempt"),
                b"uncertain native attempt",
            )?;
        } else {
            lock.try_lock()?;
        }
        assert!(slot.core.retire_failed_write(REQUEST, &original).is_err());
        assert_eq!(
            std::fs::read(directory.join(REQUEST))?,
            b"incomplete original request"
        );
        drop(lock);
        if attempted {
            assert_eq!(
                std::fs::read(directory.join("image-cache.attempt"))?,
                b"uncertain native attempt"
            );
        } else {
            slot.core.retire_failed_write(REQUEST, &original)?;
            assert!(!directory.join(REQUEST).exists());
        }
    }
    Ok(())
}

#[tokio::test]
#[ignore = "single subprocess only: actual kernel file-size limits, filesystem transitions; not native authority or disk-full proof"]
async fn kernel_file_limit_child() -> Result<()> {
    struct Limit {
        previous: libc::rlimit,
        handler: libc::sighandler_t,
    }
    impl Limit {
        fn set(bytes: u64) -> Result<Self> {
            let mut previous = libc::rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };
            if unsafe { libc::getrlimit(libc::RLIMIT_FSIZE, &mut previous) } != 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            let handler = unsafe { libc::signal(libc::SIGXFSZ, libc::SIG_IGN) };
            if handler == libc::SIG_ERR {
                return Err(std::io::Error::last_os_error().into());
            }
            let limit = Self { previous, handler };
            let cap = libc::rlimit {
                rlim_cur: bytes as libc::rlim_t,
                rlim_max: limit.previous.rlim_max,
            };
            if unsafe { libc::setrlimit(libc::RLIMIT_FSIZE, &cap) } != 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            Ok(limit)
        }
    }
    impl Drop for Limit {
        fn drop(&mut self) {
            assert_eq!(
                unsafe { libc::setrlimit(libc::RLIMIT_FSIZE, &self.previous) },
                0
            );
            assert_ne!(
                unsafe { libc::signal(libc::SIGXFSZ, self.handler) },
                libc::SIG_ERR
            );
        }
    }
    let (_root, mut slot) = cleanup_fixture()?;
    let request = slot.pending.as_ref().unwrap().request.clone();
    let bytes = canonical::to_vec(&request)?;
    for role in [REQUEST, CLEARING] {
        for cap in [0, 64] {
            if role == REQUEST {
                std::fs::remove_file(slot.core.lease.directory().join(REQUEST))?;
                std::fs::remove_file(slot.core.lease.directory().join("image-cache.retired"))?;
                slot.pending.as_mut().unwrap().file = None;
            }
            let error = {
                let _limit = Limit::set(cap)?;
                slot.core
                    .write_body(role, &bytes)
                    .err()
                    .context("kernel limit did not reject cache write")?
            };
            assert_eq!(
                error
                    .downcast_ref::<std::io::Error>()
                    .and_then(|e| e.raw_os_error()),
                Some(libc::EFBIG)
            );
            assert!(
                !slot.core.lease.directory().join(role).exists(),
                "failed write poisoned {role}"
            );
            if role == REQUEST {
                slot.pending.as_mut().unwrap().file = Some(slot.core.write(&request)?);
                let mut retired = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .open(slot.core.lease.directory().join("image-cache.retired"))?;
                retired.write_all(&request.receipt()?)?;
                retired.sync_all()?;
            } else {
                assert_eq!(slot.core.read(REQUEST, MAX_REQUEST_BYTES)?.1, bytes);
            }
            assert_eq!(
                slot.core.read("image-cache.retired", 4096)?.1,
                request.receipt()?
            );
        }
    }
    // Fake retired controls here qualify filesystem recovery only. There is
    // deliberately no Anchor, verified image or native retirement capability.
    slot.clear().await?;
    assert!(slot.pending.is_none() && slot.entry.is_none());
    println!(
        "kernel EFBIG: empty/partial request and clearing writes retired without replacing original controls"
    );
    Ok(())
}

#[tokio::test]
async fn terminal_clearing_survives_removed_request_and_missing_earlier_controls() -> Result<()> {
    let (_root, mut slot) = cleanup_fixture()?;
    let request = slot.pending.as_ref().unwrap().request.clone();
    slot.core
        .write_body(CLEARING, &canonical::to_vec(&request)?)?;
    std::fs::remove_file(slot.core.lease.directory().join(REQUEST))?;
    std::fs::remove_file(slot.core.lease.directory().join("image-cache.retired"))?;
    slot.pending.as_mut().unwrap().file = None;
    slot.clear().await?;
    assert!(slot.entry.is_none() && slot.pending.is_none());
    assert!(!slot.core.lease.directory().join(CLEARING).exists());
    Ok(())
}

#[tokio::test]
async fn interrupted_terminal_write_retries_only_with_intact_original_retirement() -> Result<()> {
    let (_root, mut slot) = cleanup_fixture()?;
    let request = canonical::to_vec(&slot.pending.as_ref().unwrap().request)?;
    slot.core
        .write_body(CLEARING, &request[..request.len() / 2])?;
    slot.clear().await?;
    assert!(slot.pending.is_none());
    let (_root, mut bad) = cleanup_fixture()?;
    bad.core.write_body(CLEARING, b"foreign")?;
    assert!(bad.clear().await.is_err());
    assert!(bad.core.lease.directory().join(REQUEST).exists());
    Ok(())
}

#[tokio::test]
async fn retained_bytes_share_the_existing_ceiling_without_requiring_free_space_twice() -> Result<()>
{
    let root = tempfile::tempdir()?;
    let source =
        super::super::tests::matched_fixture(&root.path().canonicalize()?.join("budget")).await?;
    let mut plan = source.source.job.plan.clone();
    let (total, allocation, remaining, limits) = (
        plan.disk_reservation,
        plan.disk_allocation_bytes(),
        plan.remaining_disk_reservation_bytes(),
        (
            plan.disk_limit,
            plan.input_limit,
            plan.output_limit,
            plan.workspace.timeout_seconds,
        ),
    );
    assert!(plan.fits_cached_image(MIB));
    plan.reserve_cached_image(MIB)?;
    assert_eq!(plan.disk_reservation, total + MIB);
    assert_eq!(plan.disk_allocation_bytes(), allocation);
    assert_eq!(plan.remaining_disk_reservation_bytes(), remaining);
    assert_eq!(
        (
            plan.disk_limit,
            plan.input_limit,
            plan.output_limit,
            plan.workspace.timeout_seconds
        ),
        limits
    );
    assert!(plan.reserve_cached_image(u64::MAX).is_err());
    assert_eq!(plan.retained_disk_bytes, MIB);
    Ok(())
}

#[tokio::test]
async fn orphan_reference_data_cannot_be_ignored_when_its_cleanup_profile_is_absent() -> Result<()>
{
    let root = tempfile::tempdir()?;
    let lane = root.path().canonicalize()?.join("lane");
    let foreground = ControllerLease::acquire(&lane)?;
    let runtime: Arc<dyn RuntimeManager> = Arc::new(super::super::peer_tests::Unavailable);
    before_source(runtime.clone(), &foreground).await?;
    let cache = ControllerLease::acquire_for_recovery(&lane.join(DIRECTORY))?;
    drop(cache);
    before_source(runtime.clone(), &foreground).await?;
    let request = lane.join(DIRECTORY).join(REQUEST);
    std::fs::write(&request, b"untrusted cleanup data, not a runtime permit")?;
    std::fs::set_permissions(&request, std::fs::Permissions::from_mode(0o400))?;
    assert!(before_source(runtime, &foreground).await.is_err());
    assert_eq!(
        std::fs::read(request)?,
        b"untrusted cleanup data, not a runtime permit"
    );
    Ok(())
}

#[tokio::test]
async fn empty_slot_uses_a_separate_os_owner_and_claim_binds_exact_job_accounting() -> Result<()> {
    // Scheduling/ownership unit only: no actual image or verified-code entry
    // can be manufactured here. The native composed case qualifies reuse.
    let root = tempfile::tempdir()?;
    let parent = root.path().canonicalize()?;
    let source = super::super::tests::matched_fixture(&parent.join("sample")).await?;
    let mut plan = source.source.job.plan.clone();
    let lane = parent.join("lane");
    let foreground = ControllerLease::acquire(&lane)?;
    let listener = std::os::unix::net::UnixListener::bind(parent.join("daemon.sock"))?;
    let binary = parent.join("approved-fixture");
    let contents = b"this empty-slot fixture must never execute a keeper";
    std::fs::write(&binary, contents)?;
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700))?;
    let profile = Arc::new(Profile::from_operator_configuration(
        binary,
        Digest::of(contents),
        parent.join("daemon.sock"),
    )?);
    let pool = Pool::open(
        Arc::new(super::super::peer_tests::Unavailable),
        profile,
        &lane,
    )
    .await?;
    drop(foreground);
    let _next_foreground = ControllerLease::acquire(&lane)?;
    assert!(ControllerLease::acquire_for_recovery(&lane.join(DIRECTORY)).is_err());
    let borrow = pool.claim(&mut plan).await?;
    borrow.require_plan(&plan, &lane)?;
    let mut wrong = plan.clone();
    wrong.disk_reservation += 1;
    assert!(borrow.require_plan(&wrong, &lane).is_err());
    assert!(borrow.require_plan(&plan, &parent.join("other")).is_err());
    assert!(
        tokio::time::timeout(Duration::from_millis(20), pool.evict())
            .await
            .is_err(),
        "eviction must not race the original borrowed owner"
    );
    drop(borrow);
    pool.evict().await?;
    assert!(pool.snapshot().await?.is_none());
    drop(pool);
    drop(listener);
    Ok(())
}
