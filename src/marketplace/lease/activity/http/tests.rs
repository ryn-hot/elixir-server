//! Real bounded files/OS leases and synthetic runtime cleanup observations.
//! No fake grant/Ready flag here is production networking authority.
use super::*;
use crate::runtime::model::{ContainerSpec, ContainerState};
use std::{
    path::Path,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

fn profile() -> ApplicationHttpProfile {
    ApplicationHttpProfile::new(
        "storage-fixture",
        &format!("sha256:{}", "c".repeat(64)),
        &"d".repeat(64),
        Digest::of(b"installed scope"),
        Digest::of(b"HTTP invocation"),
    )
    .unwrap()
}
fn setup() -> Result<(tempfile::TempDir, ControllerLease, Fence)> {
    let root = tempfile::tempdir()?;
    let path = root.path().canonicalize()?.join("global");
    let lease = ControllerLease::acquire(&path)?;
    let mut parent = Fence::begin(
        &lease,
        Target::WriterCreate {
            deployment: "source-fixture".into(),
        },
    )?;
    parent.bind_writer(&lease, &"a".repeat(64))?;
    Ok((root, lease, parent))
}
fn child(lease: &ControllerLease) -> Result<HttpFence> {
    HttpFence::begin(
        lease,
        &"a".repeat(64),
        Digest::of(b"actual source invocation fixture"),
        &profile(),
    )
}
async fn rejected(
    profile: &ApplicationHttpProfile,
    attempt: HttpCreateAttempt,
) -> Result<crate::runtime::http_creation::HttpNotAttempted> {
    let driver = crate::runtime::docker::DockerRuntimeManager::new(Some(
        "/must-not-run-http-journal-create-fixture".into(),
    ));
    match driver.create_application_http(profile, attempt).await? {
        crate::runtime::http_creation::HttpCreation::NotAttempted(proof) => Ok(proof),
        crate::runtime::http_creation::HttpCreation::Created(_) => {
            bail!("missing CLI fixture created a container")
        }
    }
}
struct Runtime {
    calls: Mutex<Vec<String>>,
    present: AtomicBool,
    unstarted: AtomicBool,
    fail_remove: AtomicBool,
    hold_stop: AtomicBool,
    wrong_lookup_id: AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
impl Runtime {
    fn new(present: bool) -> Self {
        Self {
            calls: Mutex::default(),
            present: AtomicBool::new(present),
            unstarted: AtomicBool::new(present),
            fail_remove: AtomicBool::new(false),
            hold_stop: AtomicBool::new(false),
            wrong_lookup_id: AtomicBool::new(false),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        }
    }
    fn record(&self, value: impl Into<String>) {
        self.calls.lock().unwrap().push(value.into());
    }
}
fn unused<T>() -> Result<T> {
    bail!("unexpected child fixture runtime operation")
}
#[async_trait::async_trait]
impl RuntimeManager for Runtime {
    async fn inspect_unstarted_application_http(
        &self,
        p: &ApplicationHttpProfile,
    ) -> Result<ContainerHandle> {
        self.record("unstarted");
        if !self.unstarted.load(Ordering::SeqCst) {
            bail!("unknown create; no named container");
        }
        Ok(ContainerHandle {
            id: "b".repeat(64),
            name: p.spec().name.clone(),
        })
    }
    async fn get_container_handle(&self, id: &str) -> Result<Option<ContainerHandle>> {
        self.record(format!("lookup:{id}"));
        if id == "a".repeat(64) {
            return Ok(None);
        } // synthetic already-stopped native parent
        assert_eq!(id, "b".repeat(64));
        Ok(self
            .present
            .load(Ordering::SeqCst)
            .then(|| ContainerHandle {
                id: if self.wrong_lookup_id.load(Ordering::SeqCst) {
                    "e".repeat(64)
                } else {
                    id.into()
                },
                name: id.into(), // Match real Docker's lookup alias, NOT native name.
            }))
    }
    async fn stop_application_http(
        &self,
        h: &ContainerHandle,
        p: &ApplicationHttpProfile,
    ) -> Result<()> {
        assert_eq!(h.id, "b".repeat(64));
        assert_eq!(h.name, p.spec().name);
        self.record(format!("stop:{}", h.id));
        self.entered.notify_one();
        if self.hold_stop.load(Ordering::SeqCst) {
            self.release.notified().await;
        }
        Ok(())
    }
    async fn remove_application_http(
        &self,
        h: &ContainerHandle,
        _: &ApplicationHttpProfile,
    ) -> Result<()> {
        self.record(format!("remove:{}", h.id));
        if self.fail_remove.load(Ordering::SeqCst) {
            bail!("uncertain removal");
        }
        self.present.store(false, Ordering::SeqCst);
        Ok(())
    }
    async fn ensure_network(&self, _: &str) -> Result<()> {
        unused()
    }
    async fn ensure_container(&self, _: &ContainerSpec) -> Result<ContainerHandle> {
        unused()
    }
    async fn start_container(&self, _: &ContainerHandle) -> Result<()> {
        unused()
    }
    async fn stop_container(&self, _: &ContainerHandle) -> Result<()> {
        unused()
    }
    async fn rename_container(&self, _: &ContainerHandle, _: &str) -> Result<ContainerHandle> {
        unused()
    }
    async fn remove_container(&self, _: &ContainerHandle) -> Result<()> {
        unused()
    }
    async fn container_logs(
        &self,
        _: &ContainerHandle,
        _: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<String> {
        unused()
    }
    async fn inspect(&self, _: &ContainerHandle) -> Result<ContainerState> {
        unused()
    }
    async fn read_container_file(&self, _: &ContainerHandle, _: &str) -> Result<Option<Vec<u8>>> {
        unused()
    }
    async fn copy_host_path_to_container(
        &self,
        _: &ContainerHandle,
        _: &Path,
        _: &str,
    ) -> Result<()> {
        unused()
    }
    async fn ensure_container_directories(&self, _: &ContainerHandle, _: &[String]) -> Result<()> {
        unused()
    }
    async fn ensure_container_directories_owned_like(
        &self,
        _: &ContainerHandle,
        _: &str,
        _: &[String],
    ) -> Result<bool> {
        unused()
    }
}

#[tokio::test]
async fn no_attempt_outcome_binds_original_operation_profile_and_survives_reopening() -> Result<()>
{
    let (_root, lease, mut parent) = setup()?;
    let mut c = child(&lease)?;
    let attempt = c.creating(&lease)?;
    let p = profile();
    let proof = rejected(&p, attempt).await?;
    assert!(proof.require(Uuid::new_v4(), &p).is_err());
    let other = ApplicationHttpProfile::new(
        "other-owner",
        &p.spec().image,
        &"d".repeat(64),
        Digest::of(b"other scope"),
        Digest::of(b"other invocation"),
    )?;
    assert!(proof.require(c.record.operation_id, &other).is_err());
    let _reason = c.not_attempted(&lease, proof)?;
    assert!(matches!(c.record.state, Phase::NotAttempted));
    assert!(c.bind(&lease, &"b".repeat(64)).is_err());
    assert!(c.creating(&lease).is_err());
    assert!(parent.clear(&lease).is_err());
    drop(c);
    let runtime = Runtime::new(true);
    HttpFence::recover(&runtime, &lease).await?;
    assert!(
        runtime.calls.lock().unwrap().is_empty(),
        "proven pre-CREATE cleanup must not infer from runtime absence"
    );
    parent.clear(&lease)?;
    Ok(())
}

#[tokio::test]
async fn lookup_alias_cannot_replace_the_frozen_handle_or_hide_a_wrong_cid() -> Result<()> {
    let (_root, lease, mut parent) = setup()?;
    let mut c = child(&lease)?;
    c.creating(&lease)?;
    c.bind(&lease, &"b".repeat(64))?;
    let runtime = Runtime::new(true);
    runtime.wrong_lookup_id.store(true, Ordering::SeqCst);
    assert!(c.retire(&runtime, &lease).await.is_err());
    assert!(parent.clear(&lease).is_err());
    assert!(
        !runtime
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|call| call.starts_with("stop:"))
    );
    runtime.wrong_lookup_id.store(false, Ordering::SeqCst);
    c.retire(&runtime, &lease).await?;
    parent.clear(&lease)?;
    Ok(())
}

#[tokio::test]
async fn one_child_is_bound_to_the_recorded_parent_and_blocks_parent_clear() -> Result<()> {
    let (_root, lease, mut parent) = setup()?;
    let mut c = child(&lease)?;
    assert!(child(&lease).is_err());
    assert!(parent.clear(&lease).is_err());
    assert!(c.bound(&lease).is_err());
    assert!(c.bind(&lease, &"b".repeat(64)).is_err());
    c.creating(&lease)?;
    assert!(c.creating(&lease).is_err());
    assert!(c.bind(&lease, "mutable-name").is_err());
    assert!(c.bind(&lease, &"a".repeat(64)).is_err());
    c.bind(&lease, &"b".repeat(64))?;
    assert!(c.bind(&lease, &"b".repeat(64)).is_err());
    assert_eq!(c.bound(&lease)?.1.id, "b".repeat(64));
    assert!(fs::metadata(lease.directory.join(FILE_NAME))?.len() <= MAX_BYTES as u64);
    let runtime = Runtime::new(true);
    c.retire(&runtime, &lease).await?;
    assert_eq!(
        runtime.calls.lock().unwrap().as_slice(),
        [
            format!("lookup:{}", "b".repeat(64)),
            format!("stop:{}", "b".repeat(64)),
            format!("remove:{}", "b".repeat(64))
        ]
    );
    parent.clear(&lease)?;
    Ok(())
}

#[tokio::test]
async fn prepared_tracking_has_no_effect_and_parent_roles_cannot_be_forged() -> Result<()> {
    let (_root, lease, mut parent) = setup()?;
    assert!(HttpFence::begin(&lease, &"f".repeat(64), Digest::of(b"invoke"), &profile()).is_err());
    let before = fs::read(lease.directory.join(super::super::FILE_NAME))?;
    let mut c = child(&lease)?;
    let runtime = Runtime::new(false);
    c.retire(&runtime, &lease).await?;
    assert!(runtime.calls.lock().unwrap().is_empty());
    assert_eq!(
        fs::read(lease.directory.join(super::super::FILE_NAME))?,
        before
    );
    parent.clear(&lease)?;
    let mut measurement = Fence::begin(
        &lease,
        Target::Measurement {
            deployment: "source-fixture".into(),
            container_id: None,
        },
    )?;
    measurement.bind_measurement(&lease, &"a".repeat(64))?;
    assert!(child(&lease).is_err());
    measurement.clear(&lease)?;
    Ok(())
}

#[tokio::test]
async fn unknown_create_is_not_absence_and_recovered_cid_is_bound_before_removal() -> Result<()> {
    let (_root, lease, mut parent) = setup()?;
    let mut c = child(&lease)?;
    c.creating(&lease)?;
    let runtime = Runtime::new(false);
    assert!(c.retire(&runtime, &lease).await.is_err());
    assert!(parent.clear(&lease).is_err());
    assert!(matches!(
        HttpFence::load(&lease)?.unwrap().record.state,
        Phase::Creating
    ));
    runtime.unstarted.store(true, Ordering::SeqCst);
    runtime.present.store(true, Ordering::SeqCst);
    runtime.fail_remove.store(true, Ordering::SeqCst);
    assert!(c.retire(&runtime, &lease).await.is_err());
    assert!(matches!(
        HttpFence::load(&lease)?.unwrap().record.state,
        Phase::Bound { .. }
    ));
    // A crash after successful deletion but before unlink can use exact CID
    // absence; it must not go back to an unbound name lookup.
    runtime.present.store(false, Ordering::SeqCst);
    let calls = runtime.calls.lock().unwrap().len();
    c.retire(&runtime, &lease).await?;
    assert_eq!(
        runtime.calls.lock().unwrap()[calls..],
        [format!("lookup:{}", "b".repeat(64))]
    );
    parent.clear(&lease)?;
    Ok(())
}

#[tokio::test]
async fn cancelled_cleanup_keeps_the_same_journal_and_global_lane_blocked() -> Result<()> {
    let (_root, lease, mut parent) = setup()?;
    let mut c = child(&lease)?;
    c.creating(&lease)?;
    c.bind(&lease, &"b".repeat(64))?;
    let runtime = Runtime::new(true);
    runtime.hold_stop.store(true, Ordering::SeqCst);
    {
        let attempt = c.retire(&runtime, &lease);
        tokio::pin!(attempt);
        tokio::select! {
            _=runtime.entered.notified()=>{},
            result=&mut attempt=>{result?;bail!("cleanup unexpectedly completed");},
            _=tokio::time::sleep(std::time::Duration::from_secs(5))=>bail!("cleanup did not reach its original stop"),
        }
    }
    assert!(parent.clear(&lease).is_err());
    assert!(HttpFence::load(&lease)?.is_some());
    runtime.hold_stop.store(false, Ordering::SeqCst);
    c.retire(&runtime, &lease).await?;
    parent.clear(&lease)?;
    Ok(())
}

#[tokio::test]
async fn malformed_or_orphan_child_blocks_ordinary_lease_even_without_parent_record() -> Result<()>
{
    let (_root, lease, parent) = setup()?;
    let mut c = child(&lease)?;
    c.creating(&lease)?;
    let path = lease.directory.join(FILE_NAME);
    fs::write(&path, b"{broken")?;
    assert!(HttpFence::load(&lease).is_err());
    let runtime = Runtime::new(true);
    assert!(
        crate::marketplace::quarantine::SourceQuarantineJob::recover_leased(&runtime, &lease)
            .await
            .is_err()
    );
    assert!(
        runtime.calls.lock().unwrap().is_empty(),
        "corrupt child must block parent cleanup before any runtime effect"
    );
    let directory = lease.directory.clone();
    fs::remove_file(directory.join(super::super::FILE_NAME))?;
    drop(c);
    drop(parent);
    drop(lease);
    assert!(ControllerLease::acquire(&directory).is_err());
    let recovery = ControllerLease::acquire_for_recovery(&directory)?;
    assert!(HttpFence::load(&recovery).is_err());
    Ok(())
}

#[tokio::test]
async fn global_restart_recovery_retires_child_before_parent_and_preserves_fences_on_error()
-> Result<()> {
    let (_root, lease, _parent) = setup()?;
    let mut c = child(&lease)?;
    c.creating(&lease)?;
    c.bind(&lease, &"b".repeat(64))?;
    drop(c);
    let runtime = Runtime::new(true);
    runtime.fail_remove.store(true, Ordering::SeqCst);
    assert!(
        crate::marketplace::quarantine::SourceQuarantineJob::recover_leased(&runtime, &lease)
            .await
            .is_err()
    );
    assert!(Fence::load(&lease)?.is_some());
    assert!(HttpFence::load(&lease)?.is_some());
    assert!(
        !runtime
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|call| call == &format!("lookup:{}", "a".repeat(64)))
    );
    runtime.fail_remove.store(false, Ordering::SeqCst);
    assert!(
        crate::marketplace::quarantine::SourceQuarantineJob::recover_leased(&runtime, &lease)
            .await?
    );
    assert!(Fence::load(&lease)?.is_none());
    assert!(HttpFence::load(&lease)?.is_none());
    assert_eq!(
        runtime.calls.lock().unwrap().last(),
        Some(&format!("lookup:{}", "a".repeat(64)))
    );
    Ok(())
}

#[test]
fn changed_parent_operation_inode_or_profile_cannot_be_adopted() -> Result<()> {
    let (_root, lease, _parent) = setup()?;
    let mut c = child(&lease)?;
    let path = lease.directory.join(FILE_NAME);
    let original = fs::read(&path)?;
    let mut changed: serde_json::Value = canonical::decode(&original, MAX_BYTES)?;
    changed["intent"]["fingerprint"] = serde_json::json!("e".repeat(64));
    fs::write(&path, canonical::to_vec(&changed)?)?;
    assert!(HttpFence::load(&lease).is_err());
    fs::write(&path, &original)?;
    let parent_path = lease.directory.join(super::super::FILE_NAME);
    let parent_bytes = fs::read(&parent_path)?;
    let mut root_record: Record = canonical::decode(&fs::read(&parent_path)?, MAX_BYTES)?;
    root_record.operation_id = Uuid::new_v4();
    fs::write(&parent_path, canonical::to_vec(&root_record)?)?;
    assert!(c.creating(&lease).is_err());
    assert!(HttpFence::load(&lease).is_err());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::write(&parent_path, parent_bytes)?;
        fs::rename(&path, lease.directory.join("retired-http.fixture"))?;
        fs::write(&path, &original)?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
        assert!(
            c.creating(&lease).is_err(),
            "original owner must reject an equivalent replacement inode"
        );
    }
    Ok(())
}

#[tokio::test]
async fn child_begin_and_parent_clear_are_one_serialized_transition() -> Result<()> {
    for _ in 0..8 {
        let (_root, lease, mut parent) = setup()?;
        let barrier = std::sync::Barrier::new(2);
        let (created, cleared) = std::thread::scope(|threads| {
            let finish = threads.spawn(|| {
                barrier.wait();
                parent.clear(&lease)
            });
            barrier.wait();
            let created = child(&lease);
            let cleared = finish.join().unwrap();
            (created, cleared)
        });
        assert_ne!(
            created.is_ok(),
            cleared.is_ok(),
            "parent removal and child creation cannot both win"
        );
        if let Ok(mut c) = created {
            c.retire(&Runtime::new(false), &lease).await?;
            parent.clear(&lease)?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn process_exit_preserves_both_records_until_child_then_parent_cleanup() -> Result<()> {
    const CHILD: &str = "ELIXIR_MARKETPLACE_HTTP_CHILD_PROCESS_FIXTURE";
    const REJECTED: &str = "ELIXIR_MARKETPLACE_HTTP_CHILD_NO_ATTEMPT";
    if let Some(path) = std::env::var_os(CHILD) {
        let lease = ControllerLease::acquire(Path::new(&path))?;
        let mut root = Fence::begin(
            &lease,
            Target::WriterCreate {
                deployment: "source-fixture".into(),
            },
        )?;
        root.bind_writer(&lease, &"a".repeat(64))?;
        let mut c = child(&lease)?;
        let attempt = c.creating(&lease)?;
        if std::env::var_os(REJECTED).is_some() {
            let proof = rejected(&profile(), attempt).await?;
            let _reason = c.not_attempted(&lease, proof)?;
        } else {
            c.bind(&lease, &"b".repeat(64))?;
        }
        std::process::exit(0);
    }
    for no_attempt in [false, true] {
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?.join("global");
        let mut command = tokio::process::Command::new(std::env::current_exe()?);
        command.args(["marketplace::lease::activity::http::tests::process_exit_preserves_both_records_until_child_then_parent_cleanup","--exact","--nocapture"])
        .env(CHILD,&directory).env_remove(REJECTED).kill_on_drop(true);
        if no_attempt {
            command.env(REJECTED, "1");
        }
        let status =
            tokio::time::timeout(std::time::Duration::from_secs(30), command.status()).await??;
        assert!(status.success());
        assert!(ControllerLease::acquire(&directory).is_err());
        let lease = ControllerLease::acquire_for_recovery(&directory)?;
        let mut parent = Fence::load(&lease)?.unwrap();
        assert!(parent.clear(&lease).is_err());
        let runtime = Runtime::new(false);
        HttpFence::recover(&runtime, &lease).await?;
        if no_attempt {
            assert!(runtime.calls.lock().unwrap().is_empty());
        }
        parent.clear(&lease)?;
        drop(parent);
        drop(lease);
        drop(ControllerLease::acquire(&directory)?);
    }
    Ok(())
}
