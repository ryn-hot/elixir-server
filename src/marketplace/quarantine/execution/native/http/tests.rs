//! Real files, OS locks and retained Tokio tasks; synthetic runtime and grant.
//! Not VPN, resource admission, native OCI or application-data qualification.
use super::*;
use crate::{
    marketplace::lease::activity::{Fence, Target},
    runtime::model::ContainerState,
};
use std::{path::Path, sync::Mutex};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Gate {
    None,
    Create,
    Start,
    Stop,
    CreateFailure,
    Rejected,
    StartFailure,
    Panic,
}
struct Runtime {
    gate: Gate,
    lane: std::path::PathBuf,
    calls: Mutex<Vec<&'static str>>,
    name: Mutex<String>,
    present: AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
impl Runtime {
    fn record(&self, event: &'static str) {
        self.calls.lock().unwrap().push(event);
    }
    async fn gate(&self, gate: Gate) {
        if self.gate == gate {
            self.entered.notify_one();
            self.release.notified().await;
        }
    }
    fn record_phase(&self, phase: &str) -> serde_json::Value {
        let value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(self.lane.join("http-helper.pending")).unwrap())
                .unwrap();
        assert_eq!(value["state"]["phase"], phase);
        value
    }
}
fn unused<T>() -> Result<T> {
    bail!("unexpected HTTP lifetime fixture operation")
}
#[async_trait::async_trait]
impl RuntimeManager for Runtime {
    async fn create_application_http(
        &self,
        profile: &ApplicationHttpProfile,
        attempt: crate::marketplace::runtime::application_http::HttpCreateAttempt,
    ) -> Result<crate::runtime::http_creation::HttpCreation> {
        self.record("create");
        self.record_phase("creating");
        if self.gate == Gate::Rejected {
            let result = crate::runtime::docker::DockerRuntimeManager::new(Some(
                "/must-not-run-original-http-create-fixture".into(),
            ))
            .create_application_http(profile, attempt)
            .await?;
            self.gate(Gate::Rejected).await;
            return Ok(result);
        }
        self.gate(Gate::Create).await;
        assert!(self.gate != Gate::Panic, "controlled original task panic");
        if self.gate == Gate::CreateFailure {
            bail!("unknown create outcome");
        }
        self.present.store(true, Ordering::SeqCst);
        *self.name.lock().unwrap() = profile.spec().name.clone();
        Ok(crate::runtime::http_creation::HttpCreation::Created(
            ContainerHandle {
                id: "b".repeat(64),
                name: profile.spec().name.clone(),
            },
        ))
    }
    async fn start_application_http(
        &self,
        handle: &ContainerHandle,
        _: &ApplicationHttpProfile,
    ) -> Result<()> {
        self.record("start");
        assert_eq!(
            self.record_phase("bound")["state"]["container_id"],
            handle.id
        );
        self.gate(Gate::Start).await;
        if self.gate == Gate::StartFailure {
            bail!("lost original start reply");
        }
        Ok(())
    }
    async fn inspect_unstarted_application_http(
        &self,
        _: &ApplicationHttpProfile,
    ) -> Result<ContainerHandle> {
        self.record("unstarted");
        bail!("unknown name is not retirement proof")
    }
    async fn get_container_handle(&self, id: &str) -> Result<Option<ContainerHandle>> {
        self.record("lookup");
        assert_eq!(id, "b".repeat(64));
        Ok(self
            .present
            .load(Ordering::SeqCst)
            .then(|| ContainerHandle {
                id: id.into(),
                name: id.into(), // Generic lookup aliases do not inspect native names.
            }))
    }
    async fn stop_application_http(
        &self,
        h: &ContainerHandle,
        p: &ApplicationHttpProfile,
    ) -> Result<()> {
        assert_eq!(h.name, p.spec().name);
        assert_eq!(h.name, *self.name.lock().unwrap());
        self.record("stop");
        self.gate(Gate::Stop).await;
        Ok(())
    }
    async fn remove_application_http(
        &self,
        _: &ContainerHandle,
        _: &ApplicationHttpProfile,
    ) -> Result<()> {
        self.record("remove");
        self.present.store(false, Ordering::SeqCst);
        Ok(())
    }
    async fn inspect(&self, identity: &ContainerHandle) -> Result<ContainerState> {
        self.record("inspect");
        assert_eq!(identity.id, "a".repeat(64));
        assert_eq!(identity.name, identity.id);
        Ok(ContainerState {
            id: identity.id.clone(),
            name: "source-fixture".into(),
            status: "running".into(),
            running: true,
            health: None,
        })
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
struct Authority(AtomicBool);
#[async_trait::async_trait]
impl SdkHttpAuthority for Authority {
    async fn current(&self) -> Result<()> {
        if !self.0.load(Ordering::SeqCst) {
            bail!("synthetic grant revoked");
        }
        Ok(())
    }
}
struct Fixture {
    _root: tempfile::TempDir,
    scope: NativeScope,
    native: Arc<NativeInvocation>,
    runtime: Arc<Runtime>,
    parent: Fence,
    authority: Arc<Authority>,
}
impl Fixture {
    fn new(gate: Gate) -> Result<Self> {
        let root = tempfile::tempdir()?;
        let lane = root.path().canonicalize()?.join("global");
        let lease = Arc::new(ControllerLease::acquire(&lane)?);
        let mut parent = Fence::begin(
            &lease,
            Target::WriterCreate {
                deployment: "source-fixture".into(),
            },
        )?;
        parent.bind_writer(&lease, &"a".repeat(64))?;
        let (release, _, _) = crate::marketplace::quarantine::tests::fixture()?;
        let plan = SourceQuarantinePlan::for_release(
            &release,
            None,
            &crate::marketplace::quarantine::tests::budget(),
        )?;
        let descriptor = SourceExecutionDescriptor {
            mode: SourceExecutionMode::Invoke,
            schema_version: 1,
            invocation_id: Digest::of(b"original HTTP lifetime fixture"),
            source: plan.source_match,
            limits: SourceExecutionLimits {
                profile: Default::default(),
                heap_bytes: 16 * MIB,
                timeout_millis: 3000,
                max_file_bytes: 4 * MIB,
                json_bytes: MIB,
                pending_jobs: 128,
                sdk: None,
            },
        };
        let runtime = Arc::new(Runtime {
            gate,
            lane,
            calls: Mutex::default(),
            name: Mutex::default(),
            present: AtomicBool::new(false),
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let (scope, native) = NativeScope::open(
            runtime.clone(),
            ContainerHandle {
                id: "a".repeat(64),
                name: "source-fixture".into(),
            },
            &descriptor,
            lease,
            Instant::now() + Duration::from_secs(30),
        )?;
        Ok(Self {
            _root: root,
            scope,
            native: Arc::new(native),
            runtime,
            parent,
            authority: Arc::new(Authority(AtomicBool::new(true))),
        })
    }
    async fn launch(&self) -> Result<HttpLaunch> {
        self.native
            .launch_application_http(
                "storage-fixture",
                &format!("sha256:{}", "c".repeat(64)),
                &"d".repeat(64),
                self.authority.clone(),
            )
            .await
    }
    async fn entered(&self) -> Result<()> {
        timeout(Duration::from_secs(5), self.runtime.entered.notified())
            .await
            .context("fixture original runtime boundary not reached")
    }
}

#[tokio::test]
async fn default_native_authority_composition_is_fresh_and_never_uses_cached_approval() -> Result<()>
{
    let f = Fixture::new(Gate::None)?;
    let checks = || {
        f.runtime
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|&&s| s == "inspect")
            .count()
    };
    f.authority.current_native(&f.native).await?;
    assert_eq!(checks(), 1);
    f.authority.current_native(&f.native).await?;
    assert_eq!(checks(), 2, "a second effect requires a fresh native check");
    f.authority.0.store(false, Ordering::SeqCst);
    assert!(f.authority.current_native(&f.native).await.is_err());
    assert_eq!(checks(), 3);
    drop(f.scope);
    assert!(f.authority.current_native(&f.native).await.is_err());
    assert_eq!(
        checks(),
        3,
        "revoked native scope must fail before runtime I/O"
    );
    Ok(())
}

#[tokio::test]
async fn definite_pre_create_rejection_retires_without_name_or_stop_inference() -> Result<()> {
    let mut f = Fixture::new(Gate::Rejected)?;
    f.runtime.release.notify_one();
    let mut launch = f.launch().await?;
    assert!(launch.ready().await.is_err());
    launch.cancel().await?;
    assert!(!f.runtime.present.load(Ordering::SeqCst));
    assert!(
        !f.runtime
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|call| matches!(*call, "lookup" | "unstarted" | "stop" | "remove" | "start"))
    );
    f.parent.clear(&f.native.lease)?;
    Ok(())
}

#[tokio::test]
async fn cancelled_rejection_waiter_retains_original_attempt_until_no_effect_is_durable()
-> Result<()> {
    let mut f = Fixture::new(Gate::Rejected)?;
    let mut launch = f.launch().await?;
    f.entered().await?;
    assert!(
        timeout(Duration::from_millis(20), launch.cancel())
            .await
            .is_err()
    );
    f.runtime.record_phase("creating");
    assert!(f.parent.clear(&f.native.lease).is_err());
    f.runtime.release.notify_one();
    timeout(Duration::from_secs(5), launch.cancel()).await??;
    assert!(!f.runtime.calls.lock().unwrap().contains(&"unstarted"));
    f.parent.clear(&f.native.lease)?;
    Ok(())
}

#[tokio::test]
async fn ready_keeps_original_lifetime_and_parent_fence_until_explicit_join() -> Result<()> {
    let mut f = Fixture::new(Gate::None)?;
    let mut launch = f.launch().await?;
    let id = launch.ready().await?.id.clone();
    assert_eq!(id, "b".repeat(64));
    let name = launch.profile().spec().name.clone();
    assert_eq!(name, launch.ready().await?.name);
    assert!(f.parent.clear(&f.native.lease).is_err());
    assert!(f.launch().await.is_err());
    launch.cancel().await?;
    launch.cancel().await?;
    assert!(launch.ready().await.is_err());
    assert!(!f.runtime.present.load(Ordering::SeqCst));
    f.parent.clear(&f.native.lease)?;
    assert_eq!(
        f.runtime
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|&&c| c == "create")
            .count(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn cancelled_ready_waiter_joins_late_create_and_never_starts_it() -> Result<()> {
    let mut f = Fixture::new(Gate::Create)?;
    let mut launch = f.launch().await?;
    f.entered().await?;
    assert!(
        timeout(Duration::from_millis(10), launch.ready())
            .await
            .is_err()
    );
    assert!(
        timeout(Duration::from_millis(10), launch.cancel())
            .await
            .is_err()
    );
    assert!(f.parent.clear(&f.native.lease).is_err());
    assert!(ControllerLease::acquire(f.native.lease.directory()).is_err());
    f.runtime.release.notify_one();
    timeout(Duration::from_secs(5), launch.cancel()).await??;
    assert!(!f.runtime.calls.lock().unwrap().contains(&"start"));
    assert!(f.runtime.calls.lock().unwrap().contains(&"remove"));
    f.parent.clear(&f.native.lease)?;
    Ok(())
}

#[tokio::test]
async fn dropped_owner_retains_task_and_lane_across_a_late_start() -> Result<()> {
    let mut f = Fixture::new(Gate::Start)?;
    let launch = f.launch().await?;
    f.entered().await?;
    drop(launch);
    assert!(f.parent.clear(&f.native.lease).is_err());
    f.runtime.release.notify_one();
    timeout(Duration::from_secs(5), async {
        while f
            .native
            .lease
            .directory()
            .join("http-helper.pending")
            .exists()
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert!(!f.runtime.present.load(Ordering::SeqCst));
    f.parent.clear(&f.native.lease)?;
    Ok(())
}

#[tokio::test]
async fn cancelled_cleanup_waiter_retains_same_stop_and_durable_cid() -> Result<()> {
    let mut f = Fixture::new(Gate::Stop)?;
    let mut launch = f.launch().await?;
    launch.ready().await?;
    assert!(
        timeout(Duration::from_millis(20), launch.cancel())
            .await
            .is_err()
    );
    f.entered().await?;
    assert!(f.parent.clear(&f.native.lease).is_err());
    assert_eq!(
        f.runtime.record_phase("bound")["state"]["container_id"],
        "b".repeat(64)
    );
    f.runtime.release.notify_one();
    timeout(Duration::from_secs(5), launch.cancel()).await??;
    assert_eq!(
        f.runtime
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|&&c| c == "stop")
            .count(),
        1
    );
    f.parent.clear(&f.native.lease)?;
    Ok(())
}

#[tokio::test]
async fn unknown_create_and_panicked_join_never_release_or_retry_the_lane() -> Result<()> {
    for gate in [Gate::CreateFailure, Gate::Panic] {
        let mut f = Fixture::new(gate)?;
        let mut launch = f.launch().await?;
        assert!(launch.ready().await.is_err());
        assert!(launch.cancel().await.is_err());
        assert!(launch.cancel().await.is_err());
        assert!(f.parent.clear(&f.native.lease).is_err());
        f.runtime.record_phase("creating");
        assert_eq!(
            f.runtime
                .calls
                .lock()
                .unwrap()
                .iter()
                .filter(|&&c| c == "create")
                .count(),
            1
        );
        assert!(!f.runtime.calls.lock().unwrap().contains(&"start"));
    }
    Ok(())
}

#[tokio::test]
async fn lost_start_reply_retires_the_bound_identity_without_recreation() -> Result<()> {
    let mut f = Fixture::new(Gate::StartFailure)?;
    let mut launch = f.launch().await?;
    assert!(launch.ready().await.is_err());
    launch.cancel().await?;
    assert!(f.runtime.calls.lock().unwrap().contains(&"remove"));
    assert!(!f.runtime.calls.lock().unwrap().contains(&"unstarted"));
    f.parent.clear(&f.native.lease)?;
    Ok(())
}

#[tokio::test]
async fn invocation_binding_rejects_different_profile_before_inspection_or_marker() -> Result<()> {
    let mut f = Fixture::new(Gate::None)?;
    for (scope, invocation) in [
        (Digest::of(b"other source"), f.native.invocation),
        (f.native.source, Digest::of(b"other invocation")),
    ] {
        let profile = ApplicationHttpProfile::new(
            "storage-fixture",
            &format!("sha256:{}", "c".repeat(64)),
            &"d".repeat(64),
            scope,
            invocation,
        )?;
        assert!(f.native.begin_application_http(&profile).await.is_err());
    }
    assert!(f.runtime.calls.lock().unwrap().is_empty());
    assert!(
        !f.native
            .lease
            .directory()
            .join("http-helper.pending")
            .exists()
    );
    f.parent.clear(&f.native.lease)?;
    Ok(())
}

#[tokio::test]
async fn revoked_authority_before_create_and_after_create_prevents_start() -> Result<()> {
    for before in [true, false] {
        let mut f = Fixture::new(if before { Gate::None } else { Gate::Create })?;
        if before {
            f.authority.0.store(false, Ordering::SeqCst);
        }
        let mut launch = f.launch().await?;
        if !before {
            f.entered().await?;
            f.authority.0.store(false, Ordering::SeqCst);
            f.runtime.release.notify_one();
        }
        assert!(launch.ready().await.is_err());
        launch.cancel().await?;
        assert!(!f.runtime.calls.lock().unwrap().contains(&"start"));
        if before {
            assert!(!f.runtime.calls.lock().unwrap().contains(&"create"));
        } else {
            assert!(f.runtime.calls.lock().unwrap().contains(&"remove"));
        }
        f.parent.clear(&f.native.lease)?;
    }
    Ok(())
}

#[tokio::test]
async fn native_scope_loss_retires_the_original_ready_helper() -> Result<()> {
    let f = Fixture::new(Gate::None)?;
    let mut launch = f.launch().await?;
    launch.ready().await?;
    drop(f.scope);
    timeout(Duration::from_secs(5), async {
        while f
            .native
            .lease
            .directory()
            .join("http-helper.pending")
            .exists()
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert!(launch.ready().await.is_err());
    launch.cancel().await?;
    assert!(!f.runtime.present.load(Ordering::SeqCst));
    Ok(())
}
