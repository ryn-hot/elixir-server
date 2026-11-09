//! Deterministic native cleanup fault tests; no Docker/network required.
use super::*;
use crate::runtime::model::{ContainerRuntimeState, ContainerState};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Absent,
    Owned,
    Foreign,
    MissingDescription,
    WrongIdentity,
    StopFailed,
    StillRunning,
    StopStalls,
    RetireFails,
    StillPresent,
    CreateStalls,
    CreateFails,
    StartStalls,
    StartFails,
    ProvisionStalls,
    ProvisionFails,
}
struct Native {
    mode: Mode,
    calls: Mutex<Vec<String>>,
    running: AtomicBool,
    stopping: tokio::sync::Notify,
    release_start: tokio::sync::Notify,
    created: AtomicBool,
    retired: AtomicBool,
    block_stop: AtomicBool,
    measurement: AtomicBool,
    lane: Mutex<Option<std::path::PathBuf>>,
}
impl Native {
    fn new(mode: Mode) -> Self {
        Self {
            mode,
            calls: Mutex::default(),
            running: AtomicBool::new(true),
            stopping: tokio::sync::Notify::new(),
            release_start: tokio::sync::Notify::new(),
            created: AtomicBool::new(false),
            retired: AtomicBool::new(false),
            block_stop: AtomicBool::new(false),
            measurement: AtomicBool::new(false),
            lane: Mutex::default(),
        }
    }
    fn record(&self, value: impl Into<String>) {
        self.calls.lock().unwrap().push(value.into());
    }
    fn actor_name(&self) -> String {
        if self.measurement.load(Ordering::SeqCst) {
            crate::runtime::measurement::name("first")
        } else {
            writer_name("first")
        }
    }
}
fn unused<T>() -> Result<T> {
    bail!("unexpected native operation in cleanup fixture")
}
fn id() -> String {
    "a".repeat(64)
}
#[async_trait::async_trait]
impl RuntimeManager for Native {
    async fn inspect_unstarted_quarantine_marketplace_writer(
        &self,
        intent: &crate::marketplace::runtime::source_writer::SourceWriterIntent,
    ) -> Result<ContainerHandle> {
        self.record("unstarted");
        assert_eq!(intent.deployment(), "first");
        if !matches!(self.mode, Mode::Owned | Mode::RetireFails) {
            bail!("unbound creation not positively matched");
        }
        self.running.store(false, Ordering::SeqCst);
        Ok(ContainerHandle {
            id: id(),
            name: writer_name("first"),
        })
    }
    async fn get_container_handle(&self, name: &str) -> Result<Option<ContainerHandle>> {
        self.record(format!("lookup:{name}"));
        if self.retired.load(Ordering::SeqCst) {
            assert!(name == id() || name == writer_name("first"));
            return Ok(None);
        }
        if matches!(
            self.mode,
            Mode::CreateStalls
                | Mode::CreateFails
                | Mode::StartStalls
                | Mode::StartFails
                | Mode::ProvisionStalls
                | Mode::ProvisionFails
        ) {
            if name == writer_name("first") {
                return Ok(None);
            }
            assert_eq!(name, id());
            if !self.created.load(Ordering::SeqCst) {
                return Ok(None);
            }
        }
        assert_eq!(name, id(), "recovery must not target a replaceable name");
        if self.mode == Mode::Absent {
            return Ok(None);
        }
        Ok(Some(ContainerHandle {
            id: if self.mode == Mode::WrongIdentity {
                "b".repeat(64)
            } else {
                id()
            },
            name: self.actor_name(),
        }))
    }
    async fn describe_container_runtime_state(
        &self,
        name: &str,
    ) -> Result<Option<ContainerRuntimeState>> {
        self.record(format!("ownership:{name}"));
        assert_eq!(name, id());
        if self.mode == Mode::MissingDescription {
            return Ok(None);
        }
        Ok(Some(ContainerRuntimeState {
            name: self.actor_name(),
            network_mode: Some("none".into()),
            labels: HashMap::from([
                (ELIXIR_MANAGED_LABEL.into(), "true".into()),
                (
                    ELIXIR_DEPLOYMENT_ID_LABEL.into(),
                    if self.mode == Mode::Foreign {
                        "foreign"
                    } else {
                        "first"
                    }
                    .into(),
                ),
                (
                    "elixir.marketplace.isolation".into(),
                    if self.measurement.load(Ordering::SeqCst) {
                        "measurement-v1"
                    } else {
                        "quarantine-v1"
                    }
                    .into(),
                ),
            ]),
            mounts: vec![],
            published_ports: vec![],
            security: Default::default(),
        }))
    }
    async fn stop_container(&self, handle: &ContainerHandle) -> Result<()> {
        assert_eq!(handle.id, id());
        assert_eq!(handle.name, id());
        self.record(format!("stop:{}", handle.id));
        let lane = self.lane.lock().unwrap().clone();
        if let Some(lane) = lane {
            let record: serde_json::Value =
                serde_json::from_slice(&std::fs::read(lane.join("native-job.pending"))?)?;
            assert_eq!(
                record["target"]["container_id"],
                id(),
                "CID must be durable before cleanup"
            );
        }
        if self.mode == Mode::StopFailed {
            bail!("injected stop failure");
        }
        if self.mode == Mode::StopStalls || self.block_stop.load(Ordering::SeqCst) {
            self.stopping.notify_one();
            std::future::pending::<()>().await;
        }
        if self.mode != Mode::StillRunning {
            self.running.store(false, Ordering::SeqCst);
        }
        Ok(())
    }
    async fn stop_quarantine_marketplace_writer(&self, handle: &ContainerHandle) -> Result<()> {
        self.stop_container(handle).await
    }
    async fn stop_host_measurement(&self, handle: &ContainerHandle) -> Result<()> {
        assert!(self.measurement.load(Ordering::SeqCst));
        self.stop_container(handle).await
    }
    async fn inspect(&self, handle: &ContainerHandle) -> Result<ContainerState> {
        assert_eq!(handle.id, id());
        assert_eq!(handle.name, id());
        self.record(format!("inspect:{}", handle.id));
        Ok(ContainerState {
            id: id(),
            name: writer_name("first"),
            status: "fixture".into(),
            running: self.running.load(Ordering::SeqCst),
            health: None,
        })
    }
    async fn ensure_network(&self, _: &str) -> Result<()> {
        unused()
    }
    async fn image_labels(&self, _: &str) -> Result<HashMap<String, String>> {
        Ok(HashMap::from([
            ("elixir.marketplace.worker.source_match".into(), "2".into()),
            (
                "elixir.marketplace.worker.native_deadline".into(),
                "1".into(),
            ),
        ]))
    }
    async fn ensure_quarantine_marketplace_volume(
        &self,
        _: &QuarantineMarketplaceVolumeSpec,
    ) -> Result<()> {
        self.record("provision");
        if self.mode == Mode::ProvisionFails {
            bail!("injected uncertain provisioning failure");
        }
        if self.mode == Mode::ProvisionStalls {
            self.stopping.notify_one();
            self.release_start.notified().await;
            return Ok(());
        }
        assert!(matches!(
            self.mode,
            Mode::CreateStalls | Mode::CreateFails | Mode::StartStalls | Mode::StartFails
        ));
        Ok(())
    }
    async fn ensure_container(&self, _: &ContainerSpec) -> Result<ContainerHandle> {
        unused()
    }
    async fn create_host_measurement(
        &self,
        spec: &crate::runtime::measurement::MeasurementSpec,
    ) -> Result<ContainerHandle> {
        self.measurement.store(true, Ordering::SeqCst);
        let handle = self
            .create_quarantine_marketplace_writer(spec.spec())
            .await?;
        Ok(ContainerHandle {
            id: handle.id,
            name: self.actor_name(),
        })
    }
    async fn start_host_measurement(
        &self,
        handle: &ContainerHandle,
        spec: &crate::runtime::measurement::MeasurementSpec,
    ) -> Result<()> {
        let limits =
            SourceJobLimits::new(spec.limits().memory_bytes, spec.limits().cpu_millicores)?;
        self.start_quarantine_marketplace_writer(handle, spec.spec(), &limits)
            .await
    }
    async fn create_quarantine_marketplace_writer(
        &self,
        _: &ContainerSpec,
    ) -> Result<ContainerHandle> {
        if matches!(
            self.mode,
            Mode::CreateStalls | Mode::CreateFails | Mode::StartStalls | Mode::StartFails
        ) {
            self.record("create");
            if matches!(self.mode, Mode::CreateStalls | Mode::CreateFails) {
                self.stopping.notify_one();
                self.release_start.notified().await;
            }
            if self.mode == Mode::CreateFails {
                bail!("uncertain native create failure");
            }
            self.created.store(true, Ordering::SeqCst);
            self.running.store(false, Ordering::SeqCst);
            return Ok(ContainerHandle {
                id: id(),
                name: writer_name("first"),
            });
        }
        unused()
    }
    async fn start_quarantine_marketplace_writer(
        &self,
        handle: &ContainerHandle,
        _: &ContainerSpec,
        _: &SourceJobLimits,
    ) -> Result<()> {
        assert_eq!(handle.id, id());
        assert_eq!(handle.name, id());
        assert!(self.created.load(Ordering::SeqCst));
        assert!(!self.running.load(Ordering::SeqCst));
        if let Some(lane) = self.lane.lock().unwrap().as_ref() {
            let record: serde_json::Value =
                serde_json::from_slice(&std::fs::read(lane.join("native-job.pending"))?)?;
            assert_eq!(
                record["target"]["type"],
                if self.measurement.load(Ordering::SeqCst) {
                    "measurement"
                } else {
                    "source_writer"
                }
            );
            assert_eq!(record["target"]["container_id"], id());
        }
        self.record("start");
        self.running.store(true, Ordering::SeqCst);
        if matches!(self.mode, Mode::StartStalls | Mode::StartFails) {
            self.stopping.notify_one();
            self.release_start.notified().await;
            if self.mode == Mode::StartFails {
                bail!("injected lost start response after actual execution");
            }
        }
        Ok(())
    }
    async fn remove_private_file_volume(&self, _: &str, _: &HashMap<String, String>) -> Result<()> {
        self.record("remove_volume");
        Ok(())
    }
    async fn start_container(&self, _: &ContainerHandle) -> Result<()> {
        unused()
    }
    async fn rename_container(&self, _: &ContainerHandle, _: &str) -> Result<ContainerHandle> {
        unused()
    }
    async fn remove_container(&self, handle: &ContainerHandle) -> Result<()> {
        assert_eq!(handle.id, id());
        assert_eq!(handle.name, id());
        assert!(!self.running.load(Ordering::SeqCst));
        self.record("retire");
        if self.mode == Mode::RetireFails {
            bail!("injected uncertain native removal");
        }
        if self.mode == Mode::StillPresent {
            return Ok(());
        }
        self.retired.store(true, Ordering::SeqCst);
        Ok(())
    }
    async fn container_logs(
        &self,
        _: &ContainerHandle,
        _: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<String> {
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

fn creation_intent_fixture()
-> Result<crate::marketplace::runtime::source_writer::SourceWriterIntent> {
    // Recovery DATA fixture only, not a runnable execution or admission profile.
    Ok(serde_json::from_value(
        serde_json::json!({"deployment":"first","fingerprint":"b".repeat(64)}),
    )?)
}

#[tokio::test]
async fn recorded_unstarted_creation_binds_before_cleanup_and_retries_only_immutable_id()
-> Result<()> {
    let root = tempfile::tempdir()?;
    let directory = root.path().canonicalize()?.join("expensive");
    interrupted(
        &directory,
        Target::WriterCreateIntent {
            intent: creation_intent_fixture()?,
        },
    )?;
    let native = Native::new(Mode::RetireFails);
    *native.lane.lock().unwrap() = Some(directory.clone());
    assert!(
        SourceQuarantineJob::recover_interrupted(&native, &directory)
            .await
            .is_err()
    );
    let lease = ControllerLease::acquire_for_recovery(&directory)?;
    let recorded = Fence::load(&lease)?.unwrap();
    assert!(
        matches!(recorded.target(),Target::SourceWriter{container_id:Some(cid),..} if cid==&id())
    );
    drop(recorded);
    drop(lease);
    assert!(ControllerLease::acquire(&directory).is_err());
    let absent = Native::new(Mode::Absent);
    assert!(SourceQuarantineJob::recover_interrupted(&absent, &directory).await?);
    assert!(!absent.calls.lock().unwrap().contains(&"unstarted".into()));
    Ok(())
}

#[tokio::test]
async fn unbound_creation_absence_or_foreign_match_never_clears_the_fence() -> Result<()> {
    for mode in [Mode::Absent, Mode::Foreign] {
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?.join("expensive");
        interrupted(
            &directory,
            Target::WriterCreateIntent {
                intent: creation_intent_fixture()?,
            },
        )?;
        let native = Native::new(mode);
        assert!(
            SourceQuarantineJob::recover_interrupted(&native, &directory)
                .await
                .is_err()
        );
        assert_eq!(native.calls.lock().unwrap().as_slice(), ["unstarted"]);
        assert!(ControllerLease::acquire(&directory).is_err());
    }
    Ok(())
}
fn interrupted(directory: &Path, target: Target) -> Result<()> {
    let lease = ControllerLease::acquire(directory)?;
    let _fence = Fence::begin(&lease, target)?;
    Ok(())
}
fn writer() -> Target {
    Target::SourceWriter {
        deployment: "first".into(),
        container_id: Some(id()),
    }
}

#[tokio::test]
async fn known_writer_requires_immutable_owned_stopped_identity_before_lane_reopens() -> Result<()>
{
    for mode in [Mode::Owned, Mode::Absent] {
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?.join("expensive");
        interrupted(&directory, writer())?;
        assert!(ControllerLease::acquire(&directory).is_err());
        let native = Native::new(mode);
        assert!(SourceQuarantineJob::recover_interrupted(&native, &directory).await?);
        assert!(!SourceQuarantineJob::recover_interrupted(&native, &directory).await?);
        drop(ControllerLease::acquire(&directory)?);
        let calls = native.calls.lock().unwrap();
        assert_eq!(calls.len(), if mode == Mode::Owned { 6 } else { 1 });
        assert_eq!(calls[0], format!("lookup:{}", id()));
    }
    Ok(())
}
#[tokio::test]
async fn failed_foreign_missing_or_still_running_cleanup_keeps_the_global_lane_fenced() -> Result<()>
{
    for mode in [
        Mode::Foreign,
        Mode::MissingDescription,
        Mode::WrongIdentity,
        Mode::StopFailed,
        Mode::RetireFails,
        Mode::StillPresent,
        Mode::StillRunning,
    ] {
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?.join("expensive");
        interrupted(&directory, writer())?;
        let native = Native::new(mode);
        for _ in 0..2 {
            assert!(
                SourceQuarantineJob::recover_interrupted(&native, &directory)
                    .await
                    .is_err()
            );
            assert!(ControllerLease::acquire(&directory).is_err());
        }
        if matches!(
            mode,
            Mode::Foreign | Mode::MissingDescription | Mode::WrongIdentity
        ) {
            assert!(
                !native
                    .calls
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|call| call.starts_with("stop:"))
            );
        }
        assert_eq!(
            std::fs::read_dir(&directory)?.count(),
            2,
            "no retry history/state growth"
        );
        assert!(
            SourceQuarantineJob::recover_interrupted(&Native::new(Mode::Owned), &directory).await?
        );
    }
    Ok(())
}
#[tokio::test]
async fn uncertain_writer_and_provisioner_startups_are_not_cleared_by_empty_name_lookups()
-> Result<()> {
    for target in [
        Target::SourceWriter {
            deployment: "first".into(),
            container_id: None,
        },
        Target::SourceProvisioner {
            deployment: "first".into(),
        },
        Target::WriterCreate {
            deployment: "first".into(),
        },
        Target::Measurement {
            deployment: "first".into(),
            container_id: None,
        },
    ] {
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?.join("expensive");
        interrupted(&directory, target)?;
        let native = Native::new(Mode::Absent);
        assert!(
            SourceQuarantineJob::recover_interrupted(&native, &directory)
                .await
                .is_err()
        );
        assert!(
            native.calls.lock().unwrap().is_empty(),
            "unknown starts must not be converted into absence proofs"
        );
        assert!(ControllerLease::acquire(&directory).is_err());
    }
    Ok(())
}
#[tokio::test]
async fn cancelling_recovery_does_not_remove_its_fence_or_allow_a_replacement_job() -> Result<()> {
    let root = tempfile::tempdir()?;
    let directory = root.path().canonicalize()?.join("expensive");
    interrupted(&directory, writer())?;
    let native = Native::new(Mode::StopStalls);
    let mut recovery = Box::pin(SourceQuarantineJob::recover_interrupted(
        &native, &directory,
    ));
    tokio::select! {
        result = &mut recovery => { result?; bail!("stalled recovery unexpectedly completed"); },
        _ = native.stopping.notified() => {}
    }
    drop(recovery);
    assert!(ControllerLease::acquire(&directory).is_err());
    assert!(SourceQuarantineJob::recover_interrupted(&Native::new(Mode::Owned), &directory).await?);
    drop(ControllerLease::acquire(&directory)?);
    Ok(())
}

#[tokio::test]
async fn cancelled_provision_or_create_stays_owned_until_late_completion_is_cleaned() -> Result<()>
{
    let (release, package, _) = tests::fixture()?;
    let plan = SourceQuarantinePlan::for_release(&release, None, &tests::budget())?;
    let image = format!("sha256:{}", "1".repeat(64));
    for mode in [Mode::ProvisionStalls, Mode::CreateStalls] {
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?.join("expensive");
        let native = Arc::new(Native::new(mode));
        if mode == Mode::ProvisionStalls {
            let mut open = Box::pin(SourceQuarantineJob::open(
                native.clone(),
                &directory,
                "first",
                &image,
                &plan,
            ));
            tokio::select! {
                result = &mut open => { result?; bail!("stalled provision unexpectedly completed"); },
                _ = native.stopping.notified() => {}
            }
            drop(open);
        } else {
            let mut job =
                SourceQuarantineJob::open(native.clone(), &directory, "first", &image, &plan)
                    .await?;
            let mut body = package.as_slice();
            let mut receive =
                Box::pin(job.receive(native.as_ref(), QuarantinePurpose::Package, &mut body));
            tokio::select! {
                result = &mut receive => { result?; bail!("stalled create unexpectedly completed"); },
                _ = native.stopping.notified() => {}
            }
            drop(receive);
            assert!(job.completed.is_empty());
            drop(job);
        }
        assert!(ControllerLease::acquire(&directory).is_err());
        assert!(
            SourceQuarantineJob::open(native.clone(), &directory, "replacement", &image, &plan)
                .await
                .is_err()
        );
        let absent = Native::new(Mode::Absent);
        assert!(
            SourceQuarantineJob::recover_interrupted(&absent, &directory)
                .await
                .is_err()
        );
        assert!(absent.calls.lock().unwrap().is_empty());
        assert_eq!(std::fs::read_dir(&directory)?.count(), 2);
        native.release_start.notify_one();
        wait_idle(&directory).await?;
        if mode == Mode::CreateStalls {
            assert!(!native.running.load(Ordering::SeqCst));
            assert_eq!(
                native
                    .calls
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|call| *call == "create")
                    .count(),
                1
            );
        }
    }
    Ok(())
}

async fn wait_idle(directory: &Path) -> Result<()> {
    timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(lease) = ControllerLease::acquire(directory) {
                drop(lease);
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .context("owned startup did not finish cleanup")?
}

#[tokio::test]
async fn explicit_cancel_joins_the_inflight_start_and_never_acknowledges_an_uncertain_failure()
-> Result<()> {
    let (release, package, _) = tests::fixture()?;
    let plan = SourceQuarantinePlan::for_release(&release, None, &tests::budget())?;
    let image = format!("sha256:{}", "1".repeat(64));
    for mode in [Mode::CreateStalls, Mode::CreateFails] {
        let root = tempfile::tempdir()?;
        let directory = root.path().canonicalize()?.join("expensive");
        let native = Arc::new(Native::new(mode));
        let mut job =
            SourceQuarantineJob::open(native.clone(), &directory, "first", &image, &plan).await?;
        let mut body = package.as_slice();
        let mut receive =
            Box::pin(job.receive(native.as_ref(), QuarantinePurpose::Package, &mut body));
        tokio::select! {
            result = &mut receive => { result?; bail!("stalled create unexpectedly completed"); },
            _ = native.stopping.notified() => {}
        }
        drop(receive);
        assert!(job.completed.is_empty());
        native.release_start.notify_one();
        let result = job.cancel(native.as_ref()).await;
        if mode == Mode::CreateStalls {
            result?;
            assert!(!native.running.load(Ordering::SeqCst));
            wait_idle(&directory).await?;
        } else {
            assert!(result.is_err());
            assert!(ControllerLease::acquire(&directory).is_err());
            assert!(
                SourceQuarantineJob::recover_interrupted(&Native::new(Mode::Absent), &directory)
                    .await
                    .is_err()
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn dropping_a_received_but_unadopted_startup_result_stops_the_exact_native_writer()
-> Result<()> {
    let (release, _, _) = tests::fixture()?;
    let plan = SourceQuarantinePlan::for_release(&release, None, &tests::budget())?;
    let image = format!("sha256:{}", "1".repeat(64));
    let root = tempfile::tempdir()?;
    let directory = root.path().canonicalize()?.join("expensive");
    let native = Arc::new(Native::new(Mode::CreateStalls));
    let mut job =
        SourceQuarantineJob::open(native.clone(), &directory, "first", &image, &plan).await?;
    let spec = job.profile(&plan.workspace.descriptor(QuarantinePurpose::Package)?)?;
    job.begin_writer(&spec)?;
    let fence = job.activity.take().unwrap();
    let mut start = launch::Launch::begin(
        native.clone(),
        job._lease.clone(),
        fence,
        launch::Operation::Writer(Box::new(spec), job.plan.limits.clone()),
        job.expires,
    );
    native.stopping.notified().await;
    native.release_start.notify_one();
    let started = start.wait().await?;
    assert!(native.running.load(Ordering::SeqCst));
    drop(job);
    assert!(ControllerLease::acquire(&directory).is_err());
    drop(started);
    wait_idle(&directory).await?;
    assert!(!native.running.load(Ordering::SeqCst));
    Ok(())
}

#[tokio::test]
async fn a_lost_start_response_keeps_the_durable_identity_until_positive_retirement() -> Result<()>
{
    let (release, _, _) = tests::fixture()?;
    let plan = SourceQuarantinePlan::for_release(&release, None, &tests::budget())?;
    let image = format!("sha256:{}", "1".repeat(64));
    for mode in [Mode::StartStalls, Mode::StartFails] {
        let root = tempfile::tempdir()?;
        let lane = root.path().canonicalize()?.join("expensive");
        let native = Arc::new(Native::new(mode));
        *native.lane.lock().unwrap() = Some(lane.clone());
        let mut job =
            SourceQuarantineJob::open(native.clone(), &lane, "first", &image, &plan).await?;
        let spec = job.profile(&plan.workspace.descriptor(QuarantinePurpose::Package)?)?;
        let mut start = Box::pin(job.launch_writer(&spec));
        tokio::select! {
            result=&mut start=>{result?;bail!("start did not pause after durable binding");},
            _=native.stopping.notified()=>{},
        }
        assert!(native.running.load(Ordering::SeqCst));
        assert!(ControllerLease::acquire(&lane).is_err());
        native.release_start.notify_one();
        if mode == Mode::StartFails {
            assert!(start.await.is_err());
            assert!(job.failed_launch.is_some());
            assert!(
                !job.uncertain_start,
                "a known CID must not be lost with an error response"
            );
            native.block_stop.store(true, Ordering::SeqCst);
            let mut cleanup = Box::pin(job.discard_retained());
            tokio::select! {
                result=&mut cleanup=>{result?;bail!("cleanup did not retain its failed-start owner");},
                _=native.stopping.notified()=>{},
            }
            drop(cleanup);
            assert!(
                job.failed_launch.is_some(),
                "cancelling cleanup must not drop its known identity"
            );
            assert!(ControllerLease::acquire(&lane).is_err());
            native.block_stop.store(false, Ordering::SeqCst);
        } else {
            drop(start);
            assert!(job.launch.is_some());
        }
        assert!(!job.admission_unstarted());
        job.discard_retained().await?;
        assert!(!native.running.load(Ordering::SeqCst));
        assert!(native.retired.load(Ordering::SeqCst));
        let calls = native.calls.lock().unwrap();
        assert!(calls.iter().position(|c| c == "create") < calls.iter().position(|c| c == "start"));
        assert!(calls.iter().position(|c| c == "start") < calls.iter().position(|c| c == "retire"));
        drop(calls);
        drop(job);
        drop(ControllerLease::acquire(&lane)?);
    }
    Ok(())
}

#[tokio::test]
async fn a_start_deadline_retires_the_known_identity_instead_of_leaving_an_unowned_child()
-> Result<()> {
    tokio::time::pause();
    let (release, _, _) = tests::fixture()?;
    let plan = SourceQuarantinePlan::for_release(&release, None, &tests::budget())?;
    let root = tempfile::tempdir()?;
    let lane = root.path().canonicalize()?.join("expensive");
    let native = Arc::new(Native::new(Mode::StartStalls));
    *native.lane.lock().unwrap() = Some(lane.clone());
    let mut job = SourceQuarantineJob::open(
        native.clone(),
        &lane,
        "first",
        &format!("sha256:{}", "1".repeat(64)),
        &plan,
    )
    .await?;
    let spec = job.profile(&plan.workspace.descriptor(QuarantinePurpose::Package)?)?;
    let expires = Instant::now() + Duration::from_secs(1);
    assert!(job.launch_writer_until(&spec, expires).await.is_err());
    assert!(native.running.load(Ordering::SeqCst));
    assert!(job.failed_launch.is_some());
    assert!(ControllerLease::acquire(&lane).is_err());
    job.discard_retained().await?;
    assert!(native.retired.load(Ordering::SeqCst));
    assert!(!native.running.load(Ordering::SeqCst));
    drop(job);
    drop(ControllerLease::acquire(&lane)?);
    Ok(())
}

#[tokio::test]
async fn a_source_job_cannot_switch_runtime_owners_between_provisioning_and_receiving() -> Result<()>
{
    let (release, package, _) = tests::fixture()?;
    let plan = SourceQuarantinePlan::for_release(&release, None, &tests::budget())?;
    let image = format!("sha256:{}", "1".repeat(64));
    let root = tempfile::tempdir()?;
    let directory = root.path().canonicalize()?.join("expensive");
    let native = Arc::new(Native::new(Mode::CreateStalls));
    let mut job =
        SourceQuarantineJob::open(native.clone(), &directory, "first", &image, &plan).await?;
    let impostor = Native::new(Mode::CreateStalls);
    let mut body = package.as_slice();
    assert!(
        job.receive(&impostor, QuarantinePurpose::Package, &mut body)
            .await
            .is_err()
    );
    assert!(impostor.calls.lock().unwrap().is_empty());
    assert!(!native.created.load(Ordering::SeqCst));
    assert!(job.completed.is_empty());
    job.cancel(native.as_ref()).await?;
    wait_idle(&directory).await?;
    Ok(())
}

#[tokio::test]
async fn cancel_timeout_reports_pending_without_abandoning_the_owned_startup() -> Result<()> {
    tokio::time::pause();
    let (release, package, _) = tests::fixture()?;
    let plan = SourceQuarantinePlan::for_release(&release, None, &tests::budget())?;
    let image = format!("sha256:{}", "1".repeat(64));
    let root = tempfile::tempdir()?;
    let directory = root.path().canonicalize()?.join("expensive");
    let native = Arc::new(Native::new(Mode::CreateStalls));
    let mut job =
        SourceQuarantineJob::open(native.clone(), &directory, "first", &image, &plan).await?;
    let mut body = package.as_slice();
    let mut receive = Box::pin(job.receive(native.as_ref(), QuarantinePurpose::Package, &mut body));
    tokio::select! {
        result = &mut receive => { result?; bail!("stalled create unexpectedly completed"); },
        _ = native.stopping.notified() => {}
    }
    drop(receive);
    let result = job.cancel(native.as_ref()).await;
    assert!(result.is_err());
    assert!(ControllerLease::acquire(&directory).is_err());
    native.release_start.notify_one();
    wait_idle(&directory).await?;
    assert!(!native.running.load(Ordering::SeqCst));
    Ok(())
}

struct Admitted;
struct Measure;
#[async_trait::async_trait]
impl ResourceAdmission for Measure {
    fn needs_measurement(&self) -> bool {
        true
    }
    async fn check(
        &self,
        _: &SourceQuarantinePlan,
        _: bool,
        _: Option<&HostObservation>,
    ) -> Result<()> {
        bail!("cancelled measurement must not reach physical admission");
    }
}
#[tokio::test]
async fn cancelled_measurement_start_retains_its_native_owner_but_never_counts_as_source_execution()
-> Result<()> {
    let (release, _, _) = tests::fixture()?;
    let plan = SourceQuarantinePlan::for_release(&release, None, &tests::budget())?;
    for lost_caller in [false, true] {
        let root = tempfile::tempdir()?;
        let lane = root.path().canonicalize()?.join("measurement");
        let native = Arc::new(Native::new(Mode::CreateStalls));
        *native.lane.lock().unwrap() = Some(lane.clone());
        let mut slot = None;
        let image = format!("sha256:{}", "1".repeat(64));
        let mut open = Box::pin(SourceQuarantineJob::open_leased_retained(
            &mut slot,
            native.clone(),
            "first",
            &image,
            &plan,
            ControllerLease::acquire(&lane)?,
            Arc::new(Measure),
        ));
        tokio::select! {
            result=&mut open=>{result?;bail!("measurement did not pause at create");},
            _=native.stopping.notified()=>{},
        }
        drop(open);
        let job = slot.as_mut().unwrap();
        assert!(
            !job.admission_unstarted(),
            "in-flight measurement is not cleanup"
        );
        assert!(!job.native_attempted, "no source/provision work has begun");
        assert!(!job.provisioned);
        assert!(ControllerLease::acquire(&lane).is_err());
        native.release_start.notify_one();
        if lost_caller {
            drop(slot);
        } else {
            let job = slot.as_mut().unwrap();
            job.discard_retained().await?;
            assert!(
                job.admission_unstarted(),
                "only positive measurement retirement can return the original unstarted claim"
            );
            drop(slot);
        }
        wait_idle(&lane).await?;
        assert!(native.retired.load(Ordering::SeqCst));
        assert!(!native.running.load(Ordering::SeqCst));
    }
    Ok(())
}

#[tokio::test]
async fn recorded_measurements_require_exact_owned_identity_and_confirmed_retirement() -> Result<()>
{
    for mode in [
        Mode::Owned,
        Mode::Absent,
        Mode::Foreign,
        Mode::StopFailed,
        Mode::StillRunning,
        Mode::StillPresent,
    ] {
        let root = tempfile::tempdir()?;
        let lane = root.path().canonicalize()?.join("measurement");
        interrupted(
            &lane,
            Target::Measurement {
                deployment: "first".into(),
                container_id: Some(id()),
            },
        )?;
        let native = Native::new(mode);
        native.measurement.store(true, Ordering::SeqCst);
        let result = SourceQuarantineJob::recover_interrupted(&native, &lane).await;
        if matches!(mode, Mode::Owned | Mode::Absent) {
            assert!(result?);
            drop(ControllerLease::acquire(&lane)?);
        } else {
            assert!(result.is_err());
            assert!(ControllerLease::acquire(&lane).is_err());
        }
    }
    Ok(())
}
#[async_trait::async_trait]
impl ResourceAdmission for Admitted {
    async fn check(
        &self,
        _: &SourceQuarantinePlan,
        _: bool,
        _: Option<&HostObservation>,
    ) -> Result<()> {
        Ok(())
    }
}

struct Deferred;
#[async_trait::async_trait]
impl ResourceAdmission for Deferred {
    async fn check(
        &self,
        _: &SourceQuarantinePlan,
        _: bool,
        _: Option<&HostObservation>,
    ) -> Result<()> {
        Err(crate::marketplace::worker::SourceResourceWait::fixture().into())
    }
}
#[tokio::test]
async fn initial_opening_deferrals_retain_the_same_job_lease_plan_counters_and_deadline()
-> Result<()> {
    // Synthetic denial/cleanup fixture, NOT positive native admission evidence.
    let (release, _, _) = tests::fixture()?;
    let plan = SourceQuarantinePlan::for_release(&release, None, &tests::budget())?;
    let root = tempfile::tempdir()?;
    let lane = root.path().canonicalize()?.join("initial-opening");
    let native = Arc::new(Native::new(Mode::CreateStalls));
    let admission: Arc<dyn ResourceAdmission> = Arc::new(Deferred);
    let mut slot = None;
    let error = SourceQuarantineJob::open_leased_retained(
        &mut slot,
        native.clone(),
        "first",
        &format!("sha256:{}", "1".repeat(64)),
        &plan,
        ControllerLease::acquire(&lane)?,
        admission.clone(),
    )
    .await
    .unwrap_err();
    assert!(crate::marketplace::worker::ParticipationClient::source_resources_deferred(&error));
    let job = slot.as_mut().context("original unopened job lost")?;
    assert!(job.admission_unstarted());
    let original = (job.input_used, job.output_used, job.expires);
    let calls = native.calls.lock().unwrap().clone();
    for _ in 0..12 {
        let error = job
            .resume_opening_retained(native.as_ref(), &admission, &plan)
            .await
            .unwrap_err();
        assert!(crate::marketplace::worker::ParticipationClient::source_resources_deferred(&error));
        assert_eq!((job.input_used, job.output_used, job.expires), original);
        assert_eq!(
            *native.calls.lock().unwrap(),
            calls,
            "denial must start nothing"
        );
        assert!(ControllerLease::acquire(&lane).is_err());
    }
    let replacement: Arc<dyn ResourceAdmission> = Arc::new(Deferred);
    assert!(
        job.resume_opening_retained(native.as_ref(), &replacement, &plan)
            .await
            .is_err()
    );
    assert!(
        job.resume_opening_retained(&Native::new(Mode::CreateStalls), &admission, &plan)
            .await
            .is_err()
    );
    for field in ["input", "output", "disk", "memory", "workspace", "match"] {
        let mut changed = plan.clone();
        match field {
            "input" => changed.input_limit += 1,
            "output" => changed.output_limit += 1,
            "disk" => changed.disk_reservation += 1,
            "memory" => {
                changed.limits = SourceJobLimits::new(
                    changed.limits.memory_bytes + MIB,
                    changed.limits.cpu_millicores,
                )?
            }
            "workspace" => changed.workspace.timeout_seconds += 1,
            "match" => changed.source_match.release.display_name.push('x'),
            _ => unreachable!(),
        }
        assert!(
            job.resume_opening_retained(native.as_ref(), &admission, &changed)
                .await
                .is_err(),
            "{field}"
        );
    }
    assert_eq!((job.input_used, job.output_used, job.expires), original);
    assert_eq!(*native.calls.lock().unwrap(), calls);
    job.discard_retained().await?;
    slot = None;
    assert!(slot.is_none());
    drop(ControllerLease::acquire(&lane)?);
    Ok(())
}
#[tokio::test]
async fn execution_reservation_waits_for_admission_is_atomic_and_survives_cancelled_start()
-> Result<()> {
    let (release, _, _) = tests::fixture()?;
    let plan = SourceQuarantinePlan::for_release(&release, None, &tests::budget())?;
    let root = tempfile::tempdir()?;
    let lane = root.path().canonicalize()?.join("execution-reservation");
    let native = Arc::new(Native::new(Mode::CreateStalls));
    let mut job = SourceQuarantineJob::open(
        native.clone(),
        &lane,
        "first",
        &format!("sha256:{}", "1".repeat(64)),
        &plan,
    )
    .await?;
    let spec = job.profile(&plan.workspace.descriptor(QuarantinePurpose::Package)?)?;
    let before = (job.input_used, job.output_used, job.expires);
    let calls = native.calls.lock().unwrap().clone();
    job.admission = Some(Arc::new(Deferred));
    for _ in 0..12 {
        let error = job
            .launch_writer_budgeted(&spec, job.expires, Some((7, 11)))
            .await
            .unwrap_err();
        assert!(crate::marketplace::worker::ParticipationClient::source_resources_deferred(&error));
        assert_eq!((job.input_used, job.output_used, job.expires), before);
        assert_eq!(
            *native.calls.lock().unwrap(),
            calls,
            "resource wait must launch nothing"
        );
        assert!(job.launch.is_none() && job.activity.is_none());
    }
    job.admission = Some(Arc::new(Admitted));
    let output_limit = job.plan.output_limit;
    job.plan.output_limit = job.output_used + 10;
    assert!(
        job.launch_writer_budgeted(&spec, job.expires, Some((7, 11)))
            .await
            .is_err()
    );
    assert_eq!(
        (job.input_used, job.output_used, job.expires),
        before,
        "failed output check cannot partially charge input"
    );
    assert_eq!(*native.calls.lock().unwrap(), calls);
    job.plan.output_limit = output_limit;
    let mut launch = Box::pin(job.launch_writer_budgeted(&spec, before.2, Some((7, 11))));
    tokio::select! {
        result=&mut launch=>{result?;bail!("stalled native create unexpectedly completed");},
        _=native.stopping.notified()=>{},
    }
    drop(launch);
    assert_eq!(
        (job.input_used, job.output_used),
        (before.0 + 7, before.1 + 11)
    );
    assert!(job.launch.is_some());
    assert!(ControllerLease::acquire(&lane).is_err());
    native.release_start.notify_one();
    job.discard_retained().await?;
    assert_eq!(
        (job.input_used, job.output_used),
        (before.0 + 7, before.1 + 11),
        "actual admitted attempts are never refunded"
    );
    assert_eq!(
        native
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|c| c.as_str() == "create")
            .count(),
        1
    );
    drop(job);
    drop(ControllerLease::acquire(&lane)?);
    Ok(())
}

#[tokio::test]
async fn retained_open_keeps_the_partial_job_and_joins_late_provisioning_before_disposal()
-> Result<()> {
    let (release, _, _) = tests::fixture()?;
    let plan = SourceQuarantinePlan::for_release(&release, None, &tests::budget())?;
    let root = tempfile::tempdir()?;
    let lane = root.path().canonicalize()?.join("retained");
    let lease = ControllerLease::acquire(&lane)?;
    let native = Arc::new(Native::new(Mode::ProvisionStalls));
    let mut slot = None;
    let image = format!("sha256:{}", "1".repeat(64));
    let mut opening = Box::pin(SourceQuarantineJob::open_leased_retained(
        &mut slot,
        native.clone(),
        "first",
        &image,
        &plan,
        lease,
        Arc::new(Admitted),
    ));
    tokio::select! {
        result=&mut opening=>{result?;bail!("stalled provisioning unexpectedly completed");},
        _=native.stopping.notified()=>{},
    }
    drop(opening);
    let job = slot
        .as_mut()
        .context("cancelled open dropped the partial job")?;
    assert!(!job.admission_unstarted());
    assert!(job.launch.is_some());
    let mut disposal = Box::pin(job.discard_retained());
    assert!(
        timeout(Duration::from_millis(10), &mut disposal)
            .await
            .is_err()
    );
    drop(disposal);
    assert!(
        job.launch.is_some(),
        "cancelled disposal retains the same startup owner"
    );
    assert!(
        !job.uncertain_start,
        "a cancelled waiter is not an unknown native outcome"
    );
    assert!(ControllerLease::acquire(&lane).is_err());
    native.release_start.notify_one();
    job.discard_retained().await?;
    assert!(
        !job.admission_unstarted(),
        "completed cleanup never resets admission history"
    );
    assert!(
        native
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|call| call == "remove_volume")
    );
    slot = None;
    assert!(slot.is_none());
    drop(ControllerLease::acquire(&lane)?);
    Ok(())
}

#[tokio::test]
async fn retained_failed_provisioning_is_not_a_deferral_or_a_missing_name_cleanup_proof()
-> Result<()> {
    let (release, _, _) = tests::fixture()?;
    let plan = SourceQuarantinePlan::for_release(&release, None, &tests::budget())?;
    let root = tempfile::tempdir()?;
    let lane = root.path().canonicalize()?.join("retained");
    let native = Arc::new(Native::new(Mode::ProvisionFails));
    let mut slot = None;
    assert!(
        SourceQuarantineJob::open_leased_retained(
            &mut slot,
            native.clone(),
            "first",
            &format!("sha256:{}", "1".repeat(64)),
            &plan,
            ControllerLease::acquire(&lane)?,
            Arc::new(Admitted)
        )
        .await
        .is_err()
    );
    let job = slot.as_mut().context("failed open dropped its owner")?;
    assert!(!job.uncertain_start);
    assert!(
        job.failed_launch.is_some(),
        "uncertain provision retains its actual owner"
    );
    assert!(!job.admission_unstarted());
    assert!(job.discard_retained().await.is_err());
    assert!(ControllerLease::acquire(&lane).is_err());
    drop(slot);
    assert!(
        ControllerLease::acquire(&lane).is_err(),
        "durable native fence survives OS unlock"
    );
    assert!(
        SourceQuarantineJob::recover_interrupted(native.as_ref(), &lane)
            .await
            .is_err()
    );
    Ok(())
}
