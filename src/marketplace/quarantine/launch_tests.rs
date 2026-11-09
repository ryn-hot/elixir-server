//! Real Docker create/reply-loss boundary. No VPN, public peers or install grant.
use super::*;
use crate::runtime::{
    docker::DockerRuntimeManager,
    model::{ContainerRuntimeState, ContainerState},
};
use futures_util::FutureExt;
use std::{
    pin::Pin,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context as TaskContext, Poll},
};
use tokio::io::ReadBuf;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Gate {
    Created,
    LostStartReply,
    CrashCreated,
    LostRecoveryRemoveReply,
}
struct Delayed {
    inner: Arc<DockerRuntimeManager>,
    created: tokio::sync::Notify,
    release: tokio::sync::Notify,
    handle: Mutex<Option<ContainerHandle>>,
    lane: std::path::PathBuf,
    gate: Gate,
    started: AtomicBool,
}
fn unexpected<T>() -> Result<T> {
    bail!("unexpected operation before native startup handoff")
}
#[async_trait::async_trait]
impl RuntimeManager for Delayed {
    async fn image_labels(&self, image: &str) -> Result<HashMap<String, String>> {
        self.inner.image_labels(image).await
    }
    async fn ensure_quarantine_marketplace_volume(
        &self,
        spec: &QuarantineMarketplaceVolumeSpec,
    ) -> Result<()> {
        self.inner.ensure_quarantine_marketplace_volume(spec).await
    }
    async fn ensure_container(&self, _: &ContainerSpec) -> Result<ContainerHandle> {
        unexpected()
    }
    async fn create_quarantine_marketplace_writer(
        &self,
        spec: &ContainerSpec,
    ) -> Result<ContainerHandle> {
        let handle = self
            .inner
            .create_quarantine_marketplace_writer(spec)
            .await?;
        *self.handle.lock().unwrap() = Some(handle.clone());
        if self.gate == Gate::CrashCreated {
            let record: serde_json::Value =
                serde_json::from_slice(&std::fs::read(self.lane.join("native-job.pending"))?)?;
            assert_eq!(record["target"]["type"], "source_writer_create_v2");
            assert_eq!(
                record["target"]["intent"]["fingerprint"],
                spec.labels[crate::runtime::model::CONTAINER_SPEC_HASH_LABEL]
            );
            std::process::exit(0); // Actual crash point: no returned/durable CID, no Drop.
        }
        if self.gate == Gate::Created {
            self.created.notify_one();
            self.release.notified().await;
        }
        Ok(handle)
    }
    async fn start_quarantine_marketplace_writer(
        &self,
        handle: &ContainerHandle,
        spec: &ContainerSpec,
        limits: &SourceJobLimits,
    ) -> Result<()> {
        let record: serde_json::Value =
            serde_json::from_slice(&std::fs::read(self.lane.join("native-job.pending"))?)?;
        assert_eq!(record["target"]["type"], "source_writer");
        assert_eq!(record["target"]["container_id"], handle.id);
        self.inner
            .start_quarantine_marketplace_writer(handle, spec, limits)
            .await?;
        self.started.store(true, Ordering::SeqCst);
        if self.gate == Gate::LostStartReply {
            self.created.notify_one();
            self.release.notified().await;
            bail!("controlled start reply loss after real native execution");
        }
        Ok(())
    }
    async fn get_container_handle(&self, reference: &str) -> Result<Option<ContainerHandle>> {
        self.inner.get_container_handle(reference).await
    }
    async fn inspect_unstarted_quarantine_marketplace_writer(
        &self,
        intent: &crate::marketplace::runtime::source_writer::SourceWriterIntent,
    ) -> Result<ContainerHandle> {
        self.inner
            .inspect_unstarted_quarantine_marketplace_writer(intent)
            .await
    }
    async fn describe_container_runtime_state(
        &self,
        reference: &str,
    ) -> Result<Option<ContainerRuntimeState>> {
        self.inner.describe_container_runtime_state(reference).await
    }
    async fn stop_container(&self, handle: &ContainerHandle) -> Result<()> {
        self.inner.stop_container(handle).await
    }
    async fn stop_quarantine_marketplace_writer(&self, handle: &ContainerHandle) -> Result<()> {
        self.inner.stop_quarantine_marketplace_writer(handle).await
    }
    async fn inspect(&self, handle: &ContainerHandle) -> Result<ContainerState> {
        self.inner.inspect(handle).await
    }
    async fn ensure_network(&self, _: &str) -> Result<()> {
        unexpected()
    }
    async fn start_container(&self, _: &ContainerHandle) -> Result<()> {
        unexpected()
    }
    async fn rename_container(&self, _: &ContainerHandle, _: &str) -> Result<ContainerHandle> {
        unexpected()
    }
    async fn remove_container(&self, handle: &ContainerHandle) -> Result<()> {
        if self.gate == Gate::LostRecoveryRemoveReply {
            let expected = self
                .handle
                .lock()
                .unwrap()
                .clone()
                .context("original recovery fixture CID missing")?;
            assert_eq!(handle.id, expected.id);
            let record: serde_json::Value =
                serde_json::from_slice(&std::fs::read(self.lane.join("native-job.pending"))?)?;
            assert_eq!(
                record["target"]["container_id"], handle.id,
                "recovery CID must be durable before deletion"
            );
            self.inner.remove_container(handle).await?;
            bail!("controlled lost remove reply after actual source deletion");
        }
        self.inner.remove_container(handle).await
    }
    async fn container_logs(
        &self,
        _: &ContainerHandle,
        _: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<String> {
        unexpected()
    }
    async fn read_container_file(&self, _: &ContainerHandle, _: &str) -> Result<Option<Vec<u8>>> {
        unexpected()
    }
    async fn copy_host_path_to_container(
        &self,
        _: &ContainerHandle,
        _: &Path,
        _: &str,
    ) -> Result<()> {
        unexpected()
    }
    async fn ensure_container_directories(&self, _: &ContainerHandle, _: &[String]) -> Result<()> {
        unexpected()
    }
    async fn ensure_container_directories_owned_like(
        &self,
        _: &ContainerHandle,
        _: &str,
        _: &[String],
    ) -> Result<bool> {
        unexpected()
    }
}
struct Unread(Arc<AtomicUsize>);
impl AsyncRead for Unread {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut TaskContext<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(Err(std::io::Error::other(
            "body polled before startup handoff",
        )))
    }
}
async fn idle(directory: &Path) -> Result<()> {
    timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(lease) = ControllerLease::acquire(directory) {
                drop(lease);
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("real native startup cleanup did not release the lane")?
}

#[tokio::test]
#[ignore = "explicit offline Docker cancellation after real create but before immutable startup handoff"]
async fn caller_loss_after_real_native_create_keeps_lane_owned_until_late_writer_is_stopped()
-> Result<()> {
    native_case(Gate::Created).await
}

#[tokio::test]
#[ignore = "explicit offline Docker lost reply after a real durably bound native start"]
async fn lost_real_start_reply_retires_the_known_container_before_releasing_the_lane() -> Result<()>
{
    native_case(Gate::LostStartReply).await
}

async fn native_case(gate: Gate) -> Result<()> {
    let inner = Arc::new(DockerRuntimeManager::new(None));
    let image = std::env::var("ELIXIR_MARKETPLACE_WORKER_IMAGE")
        .context("explicit immutable worker image required")?;
    if !image_is_pinned(&image) {
        bail!("requires an immutable worker image");
    }
    let root = tempfile::tempdir()?;
    let private = root.path().canonicalize()?.join("expensive");
    let runtime = Arc::new(Delayed {
        inner: inner.clone(),
        created: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        handle: Mutex::new(None),
        lane: private.clone(),
        gate,
        started: AtomicBool::new(false),
    });
    let deployment = format!("late-{}", uuid::Uuid::new_v4().simple());
    let writer = writer_name(&deployment);
    let volume = format!("elixir-marketplace-source-{deployment}");
    let labels = HashMap::from([
        (ELIXIR_MANAGED_LABEL.to_owned(), "true".to_owned()),
        (ELIXIR_DEPLOYMENT_ID_LABEL.to_owned(), deployment.clone()),
        (
            "elixir.marketplace.volume".to_owned(),
            "quarantine-v1".to_owned(),
        ),
    ]);
    let (release, _, _) = tests::fixture()?;
    let plan = SourceQuarantinePlan::for_release(&release, None, &tests::budget())?;
    let reads = Arc::new(AtomicUsize::new(0));
    let outcome = std::panic::AssertUnwindSafe(async {
        let mut job = SourceQuarantineJob::open(runtime.clone(), &private, &deployment, &image, &plan).await?;
        let mut body = Unread(reads.clone());
        let mut receive = Box::pin(job.receive(runtime.as_ref(), QuarantinePurpose::Package, &mut body));
        timeout(Duration::from_secs(30), async {
            tokio::select! {
                result = &mut receive => { result?; bail!("native startup did not pause before handoff"); },
                _ = runtime.created.notified() => Ok::<_, anyhow::Error>(())
            }
        }).await??;
        let handle = runtime.handle.lock().unwrap().clone().context("real native identity was not created")?;
        let identity = ContainerHandle { id: handle.id.clone(), name: handle.id.clone() };
        let actual = inner.inspect(&identity).await?;
        match gate {
            Gate::Created => {
                assert!(!actual.running, "create-only writer must not execute before CID handoff");
                assert_eq!(actual.status, "created");
            }
            Gate::LostStartReply => {
                assert!(actual.running, "lost-reply test requires actual native execution");
            }
            _=>bail!("unexpected ordinary startup fixture gate"),
        }
        assert_eq!(reads.load(Ordering::SeqCst), 0);
        drop(receive);
        assert!(job.completed.is_empty());
        drop(job);
        assert!(ControllerLease::acquire(&private).is_err());
        assert!(SourceQuarantineJob::open(runtime.clone(), &private, "replacement", &image, &plan).await.is_err());
        runtime.release.notify_one();
        idle(&private).await?;
        assert!(runtime.started.load(Ordering::SeqCst), "cleanup must not mask a rejected native start as a successful round trip");
        assert!(inner.get_container_handle(&identity.id).await?.is_none());
        assert_eq!(reads.load(Ordering::SeqCst), 0);
        assert!(inner.private_file_volume_owned(&volume, &labels).await?);
        Ok::<_, anyhow::Error>(())
    }).catch_unwind().await;
    // Never leave the artificial reply gate or owned fixture resources behind,
    // including assertion/timeout failures. No production fence is reset here.
    runtime.release.notify_one();
    let joined = idle(&private).await;
    let cleanup = async {
        if let Some(handle) = inner.get_container_handle(&writer).await? {
            let actual = inner
                .describe_container_runtime_state(&handle.id)
                .await?
                .context("native launch fixture ownership unavailable")?;
            if actual.labels.get(ELIXIR_MANAGED_LABEL).map(String::as_str) != Some("true")
                || actual.labels.get(ELIXIR_DEPLOYMENT_ID_LABEL) != Some(&deployment)
                || actual
                    .labels
                    .get("elixir.marketplace.isolation")
                    .map(String::as_str)
                    != Some("quarantine-v1")
            {
                bail!("refusing foreign native launch fixture cleanup");
            }
            inner
                .remove_container(&ContainerHandle {
                    id: handle.id.clone(),
                    name: handle.id,
                })
                .await?;
        }
        if inner.private_file_volume_owned(&volume, &labels).await? {
            inner.remove_private_file_volume(&volume, &labels).await?;
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    cleanup?;
    joined?;
    match outcome {
        Ok(value) => value,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

#[tokio::test]
#[ignore = "exclusive offline actual process exit after Docker CREATE before CID; durable recovery binding/removal lost reply and foreign-name preservation; no source/SDK/VPN/grant proof"]
async fn crash_before_source_cid_binding_recovers_only_original_never_started_creation()
-> Result<()> {
    const CHILD: &str = "ELIXIR_SOURCE_CREATE_CRASH_DIRECTORY";
    const DEPLOYMENT: &str = "ELIXIR_SOURCE_CREATE_CRASH_DEPLOYMENT";
    let inner = Arc::new(DockerRuntimeManager::new(None));
    let image = std::env::var("ELIXIR_MARKETPLACE_WORKER_IMAGE")?;
    if !image
        .strip_prefix("sha256:")
        .is_some_and(crate::runtime::packet_network::immutable_id)
    {
        bail!("exact local source-worker image required");
    }
    if let Some(directory) = std::env::var_os(CHILD) {
        let private = std::path::PathBuf::from(directory);
        let deployment = std::env::var(DEPLOYMENT)?;
        let runtime = Arc::new(Delayed {
            inner,
            created: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            handle: Mutex::new(None),
            lane: private.clone(),
            gate: Gate::CrashCreated,
            started: AtomicBool::new(false),
        });
        let (release, _, _) = tests::fixture()?;
        let plan = SourceQuarantinePlan::for_release(&release, None, &tests::budget())?;
        let mut job =
            SourceQuarantineJob::open(runtime.clone(), &private, &deployment, &image, &plan)
                .await?;
        let mut input = Unread(Arc::new(AtomicUsize::new(0)));
        job.receive(runtime.as_ref(), QuarantinePurpose::Package, &mut input)
            .await?;
        bail!("source create crash point was not reached");
    }
    let root = tempfile::tempdir()?;
    let private = root.path().canonicalize()?.join("expensive");
    let deployment = format!("source-crash-{}", uuid::Uuid::new_v4().simple());
    let name = writer_name(&deployment);
    let volume = format!("elixir-marketplace-source-{deployment}");
    let labels = HashMap::from([
        (ELIXIR_MANAGED_LABEL.to_owned(), "true".to_owned()),
        (ELIXIR_DEPLOYMENT_ID_LABEL.to_owned(), deployment.clone()),
        (
            "elixir.marketplace.volume".to_owned(),
            "quarantine-v1".to_owned(),
        ),
    ]);
    let foreign_key = "elixir.marketplace.source_crash.fixture";
    let outcome=std::panic::AssertUnwindSafe(async {
        let status=timeout(Duration::from_secs(45),tokio::process::Command::new(std::env::current_exe()?)
            .args(["marketplace::quarantine::launch_tests::crash_before_source_cid_binding_recovers_only_original_never_started_creation","--exact","--ignored","--nocapture","--test-threads=1"])
            .env(CHILD,&private).env(DEPLOYMENT,&deployment).kill_on_drop(true).status()).await??;
        assert!(status.success(),"child must exit at the actual pre-CID crash point");
        assert!(ControllerLease::acquire(&private).is_err());
        let record:serde_json::Value=serde_json::from_slice(&std::fs::read(private.join("native-job.pending"))?)?;
        assert_eq!(record["target"]["type"],"source_writer_create_v2");
        let intent=serde_json::from_value(record["target"]["intent"].clone())?;
        let original=inner.get_container_handle(&name).await?.context("actual unbound source CID missing")?;
        let state=inner.inspect(&ContainerHandle{id:original.id.clone(),name:original.id.clone()}).await?;
        assert_eq!(state.status,"created");assert!(!state.running);
        let recovery=Delayed{inner:inner.clone(),created:tokio::sync::Notify::new(),release:tokio::sync::Notify::new(),
            handle:Mutex::new(Some(original.clone())),lane:private.clone(),gate:Gate::LostRecoveryRemoveReply,started:AtomicBool::new(false)};
        assert!(SourceQuarantineJob::recover_interrupted(&recovery,&private).await.is_err());
        assert!(!recovery.started.load(Ordering::SeqCst),"recovery must never START");
        assert!(inner.get_container_handle(&original.id).await?.is_none());
        assert!(ControllerLease::acquire(&private).is_err());
        let bound:serde_json::Value=serde_json::from_slice(&std::fs::read(private.join("native-job.pending"))?)?;
        assert_eq!(bound["target"]["type"],"source_writer");
        assert_eq!(bound["target"]["container_id"],original.id);
        assert_eq!(bound["operation_id"],record["operation_id"]);
        // A new foreign process may reuse the old name, but never the old CID.
        let output=timeout(Duration::from_secs(15),tokio::process::Command::new("docker")
            .args(["run","-d","--pull=never","--init=false","--name",&name,"--network=none",
                "--read-only","--cap-drop=ALL","--security-opt=no-new-privileges","--user=10004:10004",
                "--memory=32m","--memory-swap=32m","--cpus=0.01","--pids-limit=8","--log-driver=none",
                "--label",&format!("{foreign_key}={deployment}"),"--entrypoint=/bin/sleep",&image,"120"])
            .kill_on_drop(true).output()).await??;
        assert!(output.status.success(),"foreign-name fixture creation failed");
        let foreign=String::from_utf8(output.stdout)?.trim().to_owned();
        assert!(crate::runtime::packet_network::immutable_id(&foreign));assert_ne!(foreign,original.id);
        assert!(inner.inspect_unstarted_quarantine_marketplace_writer(&intent).await.is_err(),"foreign name must not match original creation");
        assert!(SourceQuarantineJob::recover_interrupted(inner.as_ref(),&private).await?);
        assert!(!SourceQuarantineJob::recover_interrupted(inner.as_ref(),&private).await?);
        let foreign_state=inner.inspect(&ContainerHandle{id:foreign.clone(),name:foreign}).await?;
        assert!(foreign_state.running,"bound-CID recovery must preserve foreign same-name process");
        drop(ControllerLease::acquire(&private)?);
        Ok::<_,anyhow::Error>(())
    }).catch_unwind().await;
    // Fixture-only cleanup, with exact ownership readback. Never reset a
    // production fence or count this cleanup attempt as recovery success.
    if let Some(h) = inner.get_container_handle(&name).await? {
        let actual = inner
            .describe_container_runtime_state(&h.id)
            .await?
            .context("crash fixture cleanup identity unavailable")?;
        let owned_source = actual.labels.get(ELIXIR_MANAGED_LABEL).map(String::as_str)
            == Some("true")
            && actual.labels.get(ELIXIR_DEPLOYMENT_ID_LABEL) == Some(&deployment)
            && actual
                .labels
                .get("elixir.marketplace.isolation")
                .map(String::as_str)
                == Some("quarantine-v1");
        let owned_foreign = actual.labels.get(foreign_key) == Some(&deployment);
        if !owned_source && !owned_foreign {
            bail!("refusing unrelated crash fixture cleanup");
        }
        inner
            .remove_container(&ContainerHandle {
                id: h.id.clone(),
                name: h.id,
            })
            .await?;
    }
    if inner.private_file_volume_owned(&volume, &labels).await? {
        inner.remove_private_file_volume(&volume, &labels).await?;
    }
    assert!(inner.get_container_handle(&name).await?.is_none());
    match outcome {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
