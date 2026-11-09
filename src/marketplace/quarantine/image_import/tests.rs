//! Journal/data tests only. These never fabricate an imported-image capability.
use super::*;
#[test]
fn dangling_attempt_link_is_not_idle_and_does_not_discard_the_request() -> Result<()> {
    let (_root, lease) = lane()?;
    write_request(&lease, &request())?;
    std::os::unix::fs::symlink(
        lease.directory().join("missing"),
        lease.directory().join("image-import.attempt"),
    )?;
    assert!(cleanup_idle(&lease).is_err());
    assert!(lease.directory().join("image-import.request").exists());
    Ok(())
}
use elixir_marketplace::container_image::{
    ContainerCommand, filesystem::job::ContainerFilesystemSummary, import::SocketBinding,
    job::ContainerImageSummary,
};
fn request() -> ImportRequest {
    ImportRequest {
        schema_version: 1,
        daemon_id: "fixture-daemon".into(),
        deployment: "import-journal".into(),
        storage_namespace: Digest::of(b"namespace"),
        archive: elixir_marketplace::records::Blob {
            sha256: Digest::of(b"export"),
            size: 2048,
        },
        filesystem: ContainerFilesystemSummary {
            schema_version: 1,
            entries: 2,
            final_entries: 3,
            reserved_unpacked_bytes: 8192,
            root_sha256: Digest::of(b"root"),
            entrypoint: "/app/provider".into(),
            working_directory: "/app".into(),
            image: ContainerImageSummary {
                schema_version: 1,
                release: Digest::of(b"release"),
                artifact: elixir_marketplace::records::Blob {
                    sha256: Digest::of(b"artifact"),
                    size: 4096,
                },
                platform: elixir_marketplace::reproduction::BuildPlatform::LinuxAmd64,
                manifest: Digest::of(b"manifest"),
                config: Digest::of(b"config"),
                layers: vec![elixir_marketplace::records::Blob {
                    sha256: Digest::of(b"layer"),
                    size: 1024,
                }],
                command: ContainerCommand {
                    entrypoint: vec!["/app/provider".into()],
                    arguments: vec![],
                    environment: Default::default(),
                    working_directory: "/app".into(),
                },
            },
        },
        socket: SocketBinding {
            path: "/does-not-authorize-a-socket".into(),
            device: 1,
            inode: 1,
        },
        expires_unix_millis: 1,
    }
}
fn lane() -> Result<(tempfile::TempDir, ControllerLease)> {
    let root = tempfile::tempdir()?;
    let lease = ControllerLease::acquire(&root.path().canonicalize()?.join("lane"))?;
    Ok((root, lease))
}
fn put(lease: &ControllerLease, name: &str, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o400)
        .open(lease.directory().join(name))?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}
#[test]
fn active_import_fence_cannot_clear_from_image_presence_or_missing_result() -> Result<()> {
    let (_root, lease) = lane()?;
    let request = request();
    write_request(&lease, &request)?;
    let mut fence = Fence::begin(
        &lease,
        Target::ImageImport {
            intent: request.intent()?,
        },
    )?;
    assert!(fence.clear(&lease).is_err());
    assert!(fence.retire_import(&lease).is_err());
    assert!(cleanup_idle(&lease).is_err());
    assert!(ControllerLease::acquire(lease.directory()).is_err());
    Ok(())
}
#[tokio::test]
async fn durable_retired_phase_recovers_partial_control_cleanup_without_daemon_or_new_load()
-> Result<()> {
    let (_root, lease) = lane()?;
    let request = request();
    write_request(&lease, &request)?;
    let mut fence = Fence::begin(
        &lease,
        Target::ImageImport {
            intent: request.intent()?,
        },
    )?;
    let old = fs::read(lease.directory().join("native-job.pending"))?;
    put(&lease, "image-import.retired", &old)?;
    // Tests only this local phase transition/receipt binding, NOT a real
    // helper result. No import proof or publisher runtime can be minted here.
    fence.retire_import(&lease)?;
    assert!(fence.clear(&lease).is_err());
    drop(fence);
    let mut recovered = Fence::load(&lease)?.context("retired fence disappeared")?;
    assert!(
        retire_activity(
            &super::super::peer_tests::Unavailable,
            &lease,
            &mut recovered
        )
        .await?
    );
    require_control_absent(&lease)?;
    recovered.clear(&lease)?;
    assert!(Fence::load(&lease)?.is_none());
    Ok(())
}
#[test]
fn changed_request_and_active_keeper_preserve_control_and_root_fence() -> Result<()> {
    let (_root, lease) = lane()?;
    let request = request();
    write_request(&lease, &request)?;
    let mut fence = Fence::begin(
        &lease,
        Target::ImageImport {
            intent: request.intent()?,
        },
    )?;
    put(
        &lease,
        "image-import.retired",
        &fs::read(lease.directory().join("native-job.pending"))?,
    )?;
    fence.retire_import(&lease)?;
    let lock = keeper_lock(&lease)?;
    assert!(clear_retired(&lease, &fence).is_err());
    drop(lock);
    fs::remove_file(lease.directory().join("image-import.request"))?;
    let mut changed = request.clone();
    changed.daemon_id = "different".into();
    write_request_unchecked(&lease, &changed)?;
    assert!(clear_retired(&lease, &fence).is_err());
    assert!(lease.directory().join("image-import.retired").exists());
    assert!(lease.directory().join("native-job.pending").exists());
    Ok(())
}
#[test]
fn completed_keeper_releases_its_lock_before_an_unrelated_fork_child_exits() -> Result<()> {
    use std::os::fd::{FromRawFd, OwnedFd};
    let (_root, lease) = lane()?;
    let held = keeper_lock(&lease)?;
    let mut pipe = [-1; 2];
    if unsafe { libc::pipe(pipe.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // Owned parent endpoints guarantee EOF if this fixture unwinds/exits.
    let read_end = unsafe { OwnedFd::from_raw_fd(pipe[0]) };
    let write_end = unsafe { OwnedFd::from_raw_fd(pipe[1]) };
    let pid = unsafe { libc::fork() };
    if pid < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    if pid == 0 {
        // Fork may originate in a multithreaded test process. Use ONLY
        // async-signal-safe syscalls in this child; no Rust runtime/destructors.
        unsafe {
            libc::close(pipe[1]);
            let mut byte = 0_u8;
            libc::read(pipe[0], (&mut byte as *mut u8).cast(), 1);
            libc::_exit(0);
        }
    }
    drop(read_end);
    drop(held);
    // The child is still blocked holding the inherited file description. This
    // fails with a bare File's close-only Drop, despite O_CLOEXEC being set.
    let acquired = keeper_lock(&lease);
    drop(write_end);
    let mut status = 0;
    loop {
        let waited = unsafe { libc::waitpid(pid, &mut status, 0) };
        if waited == pid {
            break;
        }
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::Interrupted {
            return Err(error.into());
        }
    }
    acquired.context("completed keeper retained ownership through a fork child")?;
    assert_eq!(status, 0, "original fork fixture did not exit cleanly");
    Ok(())
}
fn write_request_unchecked(lease: &ControllerLease, request: &ImportRequest) -> Result<()> {
    put(lease, "image-import.request", &canonical::to_vec(request)?)
}
#[test]
fn idle_pre_fence_request_is_bounded_removable_but_unknown_attempt_is_not() -> Result<()> {
    let (_root, lease) = lane()?;
    let request = request();
    write_request(&lease, &request)?;
    cleanup_idle(&lease)?;
    require_control_absent(&lease)?;
    write_request(&lease, &request)?;
    put(&lease, "image-import.attempt", b"unknown")?;
    assert!(cleanup_idle(&lease).is_err());
    assert!(lease.directory().join("image-import.request").exists());
    Ok(())
}
#[test]
fn source_only_plan_remains_distinct_and_import_allocation_counts_staging_and_history() -> Result<()>
{
    let request = request();
    let summary = elixir_marketplace::container_image::export::ContainerExportSummary {
        schema_version: 1,
        filesystem: request.filesystem,
        archive: request.archive,
    };
    assert_eq!(
        allocation(&summary)?,
        3 * 2048 + 2 * 8192 + 2 * 4096 + 16 * MIB
    );
    Ok(())
}
