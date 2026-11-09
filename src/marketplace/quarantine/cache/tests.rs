//! Fault checks for DATA ownership only; these never stand in for native proof.
use super::*;
use crate::runtime::model::{ContainerRuntimeState, ContainerState};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

struct Runtime {
    label: Mutex<Option<String>>,
    present: AtomicBool,
    running: AtomicBool,
    foreign: AtomicBool,
    container: AtomicBool,
    deployment: String,
    calls: Mutex<Vec<&'static str>>,
}
impl Runtime {
    fn new(deployment: String) -> Self {
        Self {
            label: Mutex::new(None),
            present: AtomicBool::new(true),
            running: AtomicBool::new(false),
            foreign: AtomicBool::new(false),
            container: AtomicBool::new(false),
            deployment,
            calls: Mutex::default(),
        }
    }
}
fn unused<T>() -> Result<T> {
    bail!("unexpected native process in cache metadata unit test")
}
#[async_trait::async_trait]
impl RuntimeManager for Runtime {
    async fn source_cache_state(
        &self,
        name: &str,
        labels: &HashMap<String, String>,
    ) -> Result<SourceCacheState> {
        assert_eq!(
            name,
            format!("elixir-marketplace-source-{}", self.deployment)
        );
        assert_eq!(labels.get(SLOT).map(String::as_str), Some("source-v1"));
        if self.foreign.load(Ordering::SeqCst) {
            bail!("foreign cache ownership")
        }
        Ok(if self.present.load(Ordering::SeqCst) {
            SourceCacheState::Present(self.label.lock().unwrap().clone())
        } else {
            SourceCacheState::Absent
        })
    }
    async fn get_container_handle(&self, name: &str) -> Result<Option<ContainerHandle>> {
        self.calls.lock().unwrap().push("lookup");
        assert!(name == writer_name(&self.deployment) || name == "a".repeat(64));
        Ok(self
            .container
            .load(Ordering::SeqCst)
            .then(|| ContainerHandle {
                name: writer_name(&self.deployment),
                id: "a".repeat(64),
            }))
    }
    async fn describe_container_runtime_state(
        &self,
        _: &str,
    ) -> Result<Option<ContainerRuntimeState>> {
        Ok(Some(ContainerRuntimeState {
            name: writer_name(&self.deployment),
            network_mode: Some("none".into()),
            labels: HashMap::from([
                (ELIXIR_MANAGED_LABEL.into(), "true".into()),
                (ELIXIR_DEPLOYMENT_ID_LABEL.into(), self.deployment.clone()),
                (
                    "elixir.marketplace.isolation".into(),
                    "quarantine-v1".into(),
                ),
            ]),
            mounts: vec![],
            published_ports: vec![],
            security: Default::default(),
        }))
    }
    async fn remove_container(&self, _: &ContainerHandle) -> Result<()> {
        self.calls.lock().unwrap().push("remove-container");
        assert!(!self.running.load(Ordering::SeqCst));
        self.container.store(false, Ordering::SeqCst);
        Ok(())
    }
    async fn remove_private_file_volume(
        &self,
        name: &str,
        labels: &HashMap<String, String>,
    ) -> Result<()> {
        assert_eq!(
            name,
            format!("elixir-marketplace-source-{}", self.deployment)
        );
        assert_eq!(
            labels.get(ELIXIR_DEPLOYMENT_ID_LABEL),
            Some(&self.deployment)
        );
        assert!(!self.container.load(Ordering::SeqCst));
        self.calls.lock().unwrap().push("remove-volume");
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
    async fn container_logs(
        &self,
        _: &ContainerHandle,
        _: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<String> {
        unused()
    }
    async fn inspect(&self, handle: &ContainerHandle) -> Result<ContainerState> {
        assert_eq!(handle.id, "a".repeat(64));
        Ok(ContainerState {
            id: handle.id.clone(),
            name: writer_name(&self.deployment),
            status: "fixture".into(),
            running: self.running.load(Ordering::SeqCst),
            health: None,
        })
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

#[test]
fn cache_slot_is_restart_stable_but_cannot_adopt_a_replaced_lock() -> Result<()> {
    let root = tempfile::tempdir()?;
    let directory = root.path().canonicalize()?.join("lane");
    let first = ControllerLease::acquire(&directory)?;
    let original = Slot::new(&first)?.deployment;
    assert!(valid_deployment(&original));
    assert!(ControllerLease::acquire(&directory).is_err());
    drop(first);
    let second = ControllerLease::acquire(&directory)?;
    assert_eq!(Slot::new(&second)?.deployment, original);
    std::fs::rename(
        directory.join("controller.lock"),
        directory.join("old.lock"),
    )?;
    let replacement = ControllerLease::acquire(&directory)?;
    assert!(Slot::new(&second).is_err());
    assert_ne!(Slot::new(&replacement)?.deployment, original);
    Ok(())
}

#[tokio::test]
async fn compiled_cache_identity_binds_the_complete_signed_artifact_and_profile() -> Result<()> {
    let root = tempfile::tempdir()?;
    let source = super::super::build_inputs::tests::compiled_source(
        &root.path().canonicalize()?.join("source"),
    )
    .await?;
    let plan = &source.source.job.plan;
    assert!(plan.workspace.artifact.is_some());
    let image = format!("sha256:{}", "1".repeat(64));
    let original = key(plan, &image)?;
    let mut artifact = plan.clone();
    artifact
        .workspace
        .artifact
        .as_mut()
        .context("compiled fixture artifact missing")?
        .sha256 = Digest::of(b"different compiled bytes");
    artifact.source_match.workspace = artifact.workspace.clone();
    assert_ne!(original, key(&artifact, &image)?);
    let mut profile = plan.clone();
    profile.source_match.release.runtime_profile =
        elixir_marketplace::records::RuntimeProfile::SourceJs;
    assert_ne!(
        original,
        key(&profile, &image)?,
        "data identity cannot cross runtime profiles"
    );
    assert_ne!(original, key(plan, &format!("sha256:{}", "2".repeat(64)))?);
    // These are metadata/key comparisons, not source matching or execution.
    Ok(())
}

#[tokio::test]
async fn cache_hints_never_override_release_image_or_owned_cleanup() -> Result<()> {
    let root = tempfile::tempdir()?;
    let lease = ControllerLease::acquire(&root.path().canonicalize()?.join("lane"))?;
    let slot = Slot::new(&lease)?;
    let runtime = Runtime::new(slot.deployment.clone());
    let (release, _, _) = super::super::tests::fixture()?;
    let plan = SourceQuarantinePlan::for_release(&release, None, &super::super::tests::budget())?;
    let image = format!("sha256:{}", "1".repeat(64));
    *runtime.label.lock().unwrap() = Some(key(&plan, &image)?.to_string());
    assert!(
        choose(&runtime, &lease, &plan, &image)
            .await?
            .context("choice")?
            .reuse
    );
    assert!(
        runtime.calls.lock().unwrap().is_empty(),
        "matching metadata starts no process"
    );
    runtime.container.store(true, Ordering::SeqCst);
    runtime.running.store(true, Ordering::SeqCst);
    let changed_image = format!("sha256:{}", "2".repeat(64));
    assert!(
        choose(&runtime, &lease, &plan, &changed_image)
            .await
            .is_err()
    );
    assert!(runtime.present.load(Ordering::SeqCst));
    assert!(!runtime.calls.lock().unwrap().contains(&"remove-container"));
    runtime.running.store(false, Ordering::SeqCst);
    assert!(
        !choose(&runtime, &lease, &plan, &changed_image)
            .await?
            .context("choice")?
            .reuse
    );
    let calls = runtime.calls.lock().unwrap().clone();
    let removed_container = calls
        .iter()
        .position(|v| *v == "remove-container")
        .context("container removal")?;
    let removed_volume = calls
        .iter()
        .position(|v| *v == "remove-volume")
        .context("volume removal")?;
    assert!(removed_container < removed_volume);
    assert!(!runtime.present.load(Ordering::SeqCst));
    runtime.present.store(true, Ordering::SeqCst);
    runtime.foreign.store(true, Ordering::SeqCst);
    runtime.calls.lock().unwrap().clear();
    assert!(evict(&runtime, &lease).await.is_err());
    assert!(runtime.calls.lock().unwrap().is_empty());
    assert!(runtime.present.load(Ordering::SeqCst));
    Ok(())
}

#[tokio::test]
async fn cached_candidate_is_unverified_input_and_keeps_the_original_job_limits() -> Result<()> {
    let root = tempfile::tempdir()?;
    let directory = root.path().canonicalize()?.join("lane");
    let matched = super::super::tests::matched_fixture(&directory).await?;
    let mut job = matched.source.job;
    let slot = Slot::new(&job._lease)?;
    job.deployment = slot.deployment;
    job.volume.name = slot.name;
    job.volume.labels = slot.labels;
    label(&mut job.volume.labels, key(&job.plan, &job.volume.image)?);
    let before = (job.input_used, job.output_used, job.expires);
    let mut owned = Some(job);
    owned.as_mut().unwrap().provisioned = false;
    assert!(take_candidate(&mut owned).is_err());
    owned.as_mut().unwrap().provisioned = true;
    owned.as_mut().unwrap().uncertain_start = true;
    assert!(take_candidate(&mut owned).is_err());
    owned.as_mut().unwrap().uncertain_start = false;
    let disk = owned.as_ref().unwrap().plan.disk_limit;
    owned.as_mut().unwrap().plan.disk_limit = 0;
    assert!(take_candidate(&mut owned).is_err());
    owned.as_mut().unwrap().plan.disk_limit = disk;
    let source = take_candidate(&mut owned)?;
    assert!(owned.is_none());
    assert!(
        source.job.completed.is_empty(),
        "no receiver or match receipts synthesized"
    );
    assert_eq!(
        before,
        (
            source.job.input_used,
            source.job.output_used,
            source.job.expires
        )
    );
    assert!(ControllerLease::acquire(&directory).is_err());
    drop(source);
    assert!(ControllerLease::acquire(&directory).is_ok());
    Ok(())
}
