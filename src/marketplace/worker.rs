//! Offline server integration. The public network, billing, installation and
//! contribution scheduler are not activated by creating this worker.
mod protected;
pub(in crate::marketplace) use protected::IndependentAdmission;
#[cfg(all(test, unix))]
pub(in crate::marketplace) use protected::PeerPipeFixture;
pub(in crate::marketplace) use protected::Retirement;
pub(in crate::marketplace) use protected::SourceResourceWait;
pub(in crate::marketplace) use protected::WarmSearchAdmission;
#[cfg(test)]
pub(in crate::marketplace) use protected::synthetic_blob_owner;
pub use protected::{
    DiscoverySourceFact, EvidenceObservation, MetadataObservation,
    PARTICIPATION_DISCLOSURE_VERSION, ParticipatingWorker, ParticipationClient, ParticipationPhase,
    ParticipationSettings, ParticipationSnapshot, ProtectedWorker, ProtectedWorkerConfig,
    ProtectionState, PublicSourceObservation, PublicationObservation, SearchSourceObservation,
    SecurityObservation,
};
pub(in crate::marketplace) use protected::{SdkHttpAuthority, SdkHttpCall, SdkHttpClient};
use std::{path::Path, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use elixir_marketplace::{
    catalog::SearchQuery,
    evidence::EvidenceBatch,
    identity::{ExtensionId, PublicKey},
    peer::{PeerOperation, PeerOutcome, PeerResponse},
    policy::OrdinaryLimits,
    worker_control::WorkerControlResponse,
};
use tokio::time::timeout;

use super::{
    client::PrivateWorkerClient,
    lease::ControllerLease,
    runtime::{WorkerIsolation, image_is_pinned, inspect_fixed_worker},
    storage::{FixedStateVolume, FixedWorkerSpec},
};
use crate::runtime::{RuntimeManager, model::ContainerHandle};

struct WorkerProcess {
    handle: ContainerHandle,
    profile: FixedWorkerSpec,
    isolation: WorkerIsolation,
    client: PrivateWorkerClient,
    _lease: Arc<ControllerLease>,
}

pub struct OfflineWorker(WorkerProcess);

impl OfflineWorker {
    pub async fn start(
        runtime: &dyn RuntimeManager,
        controller_directory: &Path,
        deployment: &str,
        image: &str,
        issuer_keys: &[PublicKey],
        limits: &OrdinaryLimits,
    ) -> Result<Self> {
        // The stable private directory is server-owned configuration, not a
        // request field. Acquire before touching Docker, including stale-worker
        // recovery, so another live controller cannot be detached or stopped.
        let lease = ControllerLease::acquire(controller_directory)?;
        // Also bound Docker metadata/lifecycle calls, not just wire requests.
        // Cancellation may leave an owned, resource-bounded offline container;
        // the next lease holder recovers it without resetting persisted state.
        timeout(
            Duration::from_secs(180),
            Self::start_owned(runtime, lease, deployment, image, issuer_keys, limits),
        )
        .await
        .context("offline marketplace worker startup timed out")?
    }

    async fn start_owned(
        runtime: &dyn RuntimeManager,
        lease: ControllerLease,
        deployment: &str,
        image: &str,
        issuer_keys: &[PublicKey],
        limits: &OrdinaryLimits,
    ) -> Result<Self> {
        if !image_is_pinned(image) {
            bail!("marketplace worker requires an immutable image");
        }
        if runtime
            .image_labels(image)
            .await?
            .get("elixir.marketplace.worker.control")
            .map(String::as_str)
            != Some("1")
        {
            bail!("marketplace worker lacks the private evidence-control protocol");
        }
        recover_owned_worker(runtime, deployment).await?;
        let volume = FixedStateVolume::ensure(runtime, deployment, image, limits).await?;
        let profile = FixedWorkerSpec::offline(&volume, issuer_keys, limits)?;
        Ok(Self(
            WorkerProcess::start_profile(runtime, Arc::new(lease), profile).await?,
        ))
    }

    pub fn handle(&self) -> &ContainerHandle {
        &self.0.handle
    }
    pub fn profile(&self) -> &FixedWorkerSpec {
        &self.0.profile
    }
    pub fn isolation(&self) -> &WorkerIsolation {
        &self.0.isolation
    }
    pub async fn request(&mut self, operation: PeerOperation) -> Result<PeerResponse> {
        self.0.client.request(operation).await
    }
    pub async fn merge_evidence(
        &mut self,
        extension_id: ExtensionId,
        batch: EvidenceBatch,
    ) -> Result<WorkerControlResponse> {
        self.0.client.merge_evidence(extension_id, batch).await
    }
    pub async fn stop(self, runtime: &dyn RuntimeManager) -> Result<()> {
        self.0.stop(runtime).await
    }
}

pub(super) async fn recover_owned_worker(
    runtime: &dyn RuntimeManager,
    deployment: &str,
) -> Result<()> {
    let name = format!("elixir-marketplace-worker-{deployment}");
    if let Some(handle) = runtime.get_container_handle(&name).await? {
        use crate::runtime::model::{ELIXIR_DEPLOYMENT_ID_LABEL, ELIXIR_MANAGED_LABEL};
        let state = runtime
            .describe_container_runtime_state(&handle.id)
            .await?
            .context("existing marketplace worker ownership unavailable")?;
        if state.name != name
            || state.labels.get(ELIXIR_MANAGED_LABEL).map(String::as_str) != Some("true")
            || state
                .labels
                .get(ELIXIR_DEPLOYMENT_ID_LABEL)
                .map(String::as_str)
                != Some(deployment)
            || state
                .labels
                .get("elixir.marketplace.isolation")
                .map(String::as_str)
                != Some("fixed-v1")
        {
            bail!("refusing to recover an unowned marketplace worker");
        }
        // A previous parent may have died with a partially consumed frame.
        // Restart PID 1 instead of attaching a new reader to that stream.
        timeout(Duration::from_secs(20), runtime.stop_container(&handle))
            .await
            .context("stopping stale marketplace worker timed out")??;
        if timeout(Duration::from_secs(10), runtime.inspect(&handle))
            .await
            .context("stale worker stop confirmation deadline")??
            .running
        {
            bail!("stale marketplace worker did not stop");
        }
    }
    Ok(())
}

impl WorkerProcess {
    async fn start_profile(
        runtime: &dyn RuntimeManager,
        lease: Arc<ControllerLease>,
        profile: FixedWorkerSpec,
    ) -> Result<Self> {
        let spec = profile.container_spec();
        let handle = runtime.ensure_container(spec).await?;
        let stream = match runtime.attach_private_stdio(&handle, &spec.labels).await {
            Ok(stream) => stream,
            Err(error) => {
                timeout(Duration::from_secs(20), runtime.stop_container(&handle))
                    .await
                    .context("stopping unattached fixed worker timed out")??;
                return Err(error);
            }
        };
        Self::ready(runtime, lease, profile, handle, stream).await
    }

    #[cfg(unix)]
    async fn start_provider(
        runtime: &dyn RuntimeManager,
        guard: &mut super::packet::PacketGuardController,
        directory: &Path,
        image: &str,
        issuer_keys: &[PublicKey],
        health: &super::packet::ProviderConnectivity,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> Result<Self> {
        let lease = guard.provider_lease(directory)?;
        guard
            .start_provider_worker(
                runtime,
                image,
                issuer_keys,
                health,
                cancel,
                |handle, profile, stream| Self::ready(runtime, lease, profile, handle, stream),
            )
            .await
    }

    async fn ready(
        runtime: &dyn RuntimeManager,
        lease: Arc<ControllerLease>,
        profile: FixedWorkerSpec,
        handle: ContainerHandle,
        stream: crate::runtime::PrivateProcessStdio,
    ) -> Result<Self> {
        let ready = async {
            let mut client = PrivateWorkerClient::new(stream)?;
            let response = client
                .request(PeerOperation::Search {
                    query: SearchQuery {
                        text: String::new(),
                        tags: vec![],
                        limit: 1,
                    },
                    cursor: None,
                })
                .await?;
            if !matches!(response.response, PeerOutcome::Search { .. }) {
                bail!("fixed worker did not become ready");
            }
            let isolation = inspect_fixed_worker(runtime, &handle, &profile).await?;
            if runtime
                .image_labels(&profile.container_spec().image)
                .await
                .is_ok_and(|labels| {
                    labels
                        .get("elixir.marketplace.worker.compact_search")
                        .is_some_and(|value| value == "1")
                })
            {
                client.enable_compact_search();
            }
            Ok((client, isolation))
        }
        .await;
        let (client, isolation) = match ready {
            Ok(ready) => ready,
            Err(error) => {
                // The immutable returned ID, not a reusable name, is stopped.
                // Never remove/reset its volume after an uncertain startup.
                timeout(Duration::from_secs(20), runtime.stop_container(&handle))
                    .await
                    .context("stopping unavailable fixed worker timed out")??;
                return Err(error);
            }
        };
        Ok(Self {
            handle,
            profile,
            isolation,
            client,
            _lease: lease,
        })
    }

    async fn stop(self, runtime: &dyn RuntimeManager) -> Result<()> {
        // The original provider supervisor may already have removed this exact
        // journaled CID child-first. Positive immutable-ID absence is terminal;
        // do not fall back to a reusable name or stop somebody else's worker.
        if runtime
            .get_container_handle(&self.handle.id)
            .await?
            .is_none()
        {
            self.client.close().await;
            return Ok(());
        }
        let result = timeout(
            Duration::from_secs(20),
            runtime.stop_container(&self.handle),
        )
        .await
        .context("stopping fixed worker timed out")
        .and_then(|result| result);
        let result = match result {
            Ok(()) => timeout(Duration::from_secs(10), runtime.inspect(&self.handle))
                .await
                .context("fixed worker stop confirmation deadline")
                .and_then(|result| result)
                .and_then(|state| {
                    if state.running {
                        Err(anyhow::anyhow!("fixed worker did not stop"))
                    } else {
                        Ok(())
                    }
                }),
            Err(error) => Err(error),
        };
        self.client.close().await;
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{
        docker::DockerRuntimeManager,
        model::{ELIXIR_DEPLOYMENT_ID_LABEL, ELIXIR_MANAGED_LABEL},
    };
    use elixir_marketplace::admission::CandidateDescriptor;
    use elixir_marketplace::worker_control::WorkerControlOutcome;
    use futures_util::FutureExt;
    use std::collections::HashMap;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::process::Command;

    async fn docker(args: &[&str]) -> Result<String> {
        let output = timeout(
            Duration::from_secs(20),
            Command::new("docker")
                .args(args)
                .kill_on_drop(true)
                .output(),
        )
        .await??;
        if !output.status.success() {
            bail!(
                "fixture Docker operation failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(String::from_utf8(output.stdout)?.trim().to_owned())
    }

    async fn fixture() -> Result<(PublicKey, CandidateDescriptor, EvidenceBatch)> {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../crates/elixir_marketplace/worker/test/index-fixture.mjs")
            .canonicalize()?;
        let url = reqwest::Url::from_file_path(path)
            .map_err(|_| anyhow::anyhow!("invalid fixture path"))?;
        let script = format!(
            "import {{issuer,metadataFixture,supportPair}} from {}; const descriptor=metadataFixture(0),release=descriptor.head.payload.stable_release_id; process.stdout.write(JSON.stringify({{issuer,descriptor,batch:{{schema_version:1,release_id:release,snapshot_id:release,pairs:Array.from({{length:64}},(_,i)=>supportPair(descriptor,5+i)),next_cursor:null}}}}))",
            serde_json::to_string(url.as_str())?
        );
        let output = timeout(
            Duration::from_secs(5),
            Command::new("node")
                .args(["--input-type=module", "-e", &script])
                .env_clear()
                .env("PATH", std::env::var_os("PATH").unwrap_or_default())
                .kill_on_drop(true)
                .output(),
        )
        .await??;
        if !output.status.success() || output.stdout.len() > 256 * 1024 {
            bail!("public test fixture unavailable");
        }
        let value: serde_json::Value = serde_json::from_slice(&output.stdout)?;
        let key = value["issuer"]
            .as_str()
            .context("missing public fixture issuer")?
            .parse()?;
        let descriptor = CandidateDescriptor::parse(&serde_json::to_vec(&value["descriptor"])?)?;
        let batch = EvidenceBatch::parse(&serde_json::to_vec(&value["batch"])?)?;
        Ok((key, descriptor, batch))
    }

    fn search() -> PeerOperation {
        PeerOperation::Search {
            query: SearchQuery {
                text: String::new(),
                tags: vec![],
                limit: 20,
            },
            cursor: None,
        }
    }

    async fn exercise_fixed_worker(limits: OrdinaryLimits, metadata_required: bool) -> Result<()> {
        let selected = std::env::var("ELIXIR_MARKETPLACE_WORKER_IMAGE")
            .unwrap_or_else(|_| "elixir-marketplace-worker:development".to_owned());
        let image = docker(&["image", "inspect", "--format", "{{.Id}}", &selected]).await?;
        assert!(super::super::runtime::image_is_pinned(&image));
        let deployment = format!("fixture-{}", uuid::Uuid::new_v4().simple());
        let name = format!("elixir-marketplace-worker-{deployment}");
        let volume = format!("elixir-marketplace-{deployment}");
        let labels = HashMap::from([
            (ELIXIR_MANAGED_LABEL.to_owned(), "true".to_owned()),
            (ELIXIR_DEPLOYMENT_ID_LABEL.to_owned(), deployment.clone()),
            (
                "elixir.marketplace.volume".to_owned(),
                "fixed-v1".to_owned(),
            ),
        ]);
        let runtime = DockerRuntimeManager::new(None);
        let controller_root = tempfile::tempdir()?;
        let controller_directory = controller_root.path().canonicalize()?.join("controller");
        let result=std::panic::AssertUnwindSafe(async {
            let (issuer,descriptor,batch)=fixture().await?;
            let extension_id=descriptor.release.payload.extension_id.clone();
            let mut worker=OfflineWorker::start(&runtime,&controller_directory,&deployment,&image,&[issuer],&limits).await?;
            assert!(worker.isolation().fixed_catalog_enforced());
            assert_eq!(worker.profile().container_spec().network_mode.as_deref(),Some("none"));
            if metadata_required {
                let labels = &worker.profile().container_spec().labels;
                let wrong=PublicKey::from_bytes(ed25519_dalek::SigningKey::from_bytes(&[92;32]).verifying_key().to_bytes())?;
                assert!(runtime.exec_marketplace_metadata_verifier(worker.handle(),labels,&[wrong]).await.is_err());
                let mut process=runtime.exec_marketplace_metadata_verifier(worker.handle(),labels,&[issuer]).await?;
                let bytes=elixir_marketplace::canonical::to_vec(&elixir_marketplace::verifier::VerificationRequest::VerifyDescriptor {
                    schema_version:1,request_id:7,descriptor:Box::new(descriptor.clone()),
                })?;
                let (body,diagnostic,status)=timeout(Duration::from_secs(25),async {
                    process.input.write_u32(bytes.len() as u32).await?;
                    process.input.write_all(&bytes).await?;
                    process.input.shutdown().await?;
                    drop(process.input);
                    let read=async {let mut bytes=vec![];process.output.take(4097).read_to_end(&mut bytes).await?;Ok::<_,anyhow::Error>(bytes)};
                    let errors=async {let mut bytes=vec![];process.diagnostics.take().unwrap().take(1025).read_to_end(&mut bytes).await?;Ok::<_,anyhow::Error>(bytes)};
                    Ok::<_,anyhow::Error>(tokio::join!(read,errors,process.child.wait()))
                }).await??;
                let status=status?;let diagnostic=diagnostic?;
                assert!(status.success(),"native metadata verifier {status}: {}",String::from_utf8_lossy(&diagnostic));
                assert!(diagnostic.is_empty());
                let body=body?;assert!(body.len()<=4096);
                let length=u32::from_be_bytes(body[..4].try_into()?) as usize;
                assert_eq!(body.len(),length+4);
                let report:serde_json::Value=serde_json::from_slice(&body[4..])?;
                assert_eq!(report["request_id"],7);assert_eq!(report["outcome"]["type"],"metadata_gate");
                assert_eq!(report["outcome"]["metadata_only"],true);
                assert_eq!(report["outcome"]["support"]["verified_supporters"],5);
                assert!(runtime.exec_marketplace_evidence_verifier(worker.handle(),labels,&[wrong]).await.is_err());
                let mut process=runtime.exec_marketplace_evidence_verifier(worker.handle(),labels,&[issuer]).await?;
                let bytes=elixir_marketplace::canonical::to_vec(&elixir_marketplace::verifier::VerificationRequest::VerifyEvidence {
                    schema_version:1,request_id:8,extension_id:extension_id.clone(),
                    release_id:batch.release_id,batch:batch.clone(),
                })?;
                let (body,diagnostic,status)=timeout(Duration::from_secs(25),async {
                    process.input.write_u32(bytes.len() as u32).await?;
                    process.input.write_all(&bytes).await?;
                    process.input.flush().await?;
                    drop(process.input);
                    let read=async {let mut bytes=vec![];process.output.take(32*1024+1).read_to_end(&mut bytes).await?;Ok::<_,anyhow::Error>(bytes)};
                    let errors=async {let mut bytes=vec![];process.diagnostics.take().unwrap().take(1025).read_to_end(&mut bytes).await?;Ok::<_,anyhow::Error>(bytes)};
                    Ok::<_,anyhow::Error>(tokio::join!(read,errors,process.child.wait()))
                }).await??;
                assert!(status?.success());assert!(diagnostic?.is_empty());
                let body=body?;assert!(body.len()<=32*1024);
                let length=u32::from_be_bytes(body[..4].try_into()?) as usize;
                assert_eq!(body.len(),length+4);
                let report:serde_json::Value=serde_json::from_slice(&body[4..])?;
                assert_eq!(report["request_id"],8);assert_eq!(report["outcome"]["type"],"evidence_signatures");
                assert_eq!(report["outcome"]["verified"].as_array().context("missing checked facts")?.len(),64);
                assert_eq!(report["outcome"]["invalid"],0);
                inspect_fixed_worker(&runtime,worker.handle(),worker.profile()).await?;
                eprintln!("independent metadata/evidence verifier passed in the actual fixed worker at {} bytes/{} millicores; network none, no public-source or protected-owner authority",limits.memory_bytes,limits.cpu_millicores);
            }
            let response=worker.request(PeerOperation::Offer { descriptor:Box::new(descriptor.clone()) }).await?;
            assert!(matches!(response.response,PeerOutcome::Offered { .. }));
            let response=worker.request(search()).await?;
            assert!(matches!(response.response,PeerOutcome::Search { page } if page.results.len()==1));
            let merged=worker.merge_evidence(extension_id.clone(),batch.clone()).await?;
            assert!(matches!(merged.control_response,WorkerControlOutcome::EvidenceMerged { accepted:64 }));
            let repeated=worker.merge_evidence(extension_id.clone(),batch.clone()).await?;
            assert!(matches!(repeated.control_response,WorkerControlOutcome::EvidenceMerged { accepted:0 }));
            let response=worker.request(search()).await?;
            assert!(matches!(response.response,PeerOutcome::Search { page } if page.results[0].retained_support.verified_supporters==69 && page.results[0].retained_support.weighted_score==276));
            assert!(OfflineWorker::start(&runtime,&controller_directory,&deployment,&image,&[issuer],&limits).await.is_err(),
                "a second controller must not attach to or stop a live worker");
            let response=worker.request(search()).await?;
            assert!(matches!(response.response,PeerOutcome::Search { page } if page.results.len()==1));
            worker.stop(&runtime).await?;
            let mut restarted=OfflineWorker::start(&runtime,&controller_directory,&deployment,&image,&[issuer],&limits).await?;
            let response=restarted.request(search()).await?;
            assert!(matches!(response.response,PeerOutcome::Search { page } if page.results.len()==1));
            // Only the minimum gate is persisted; reconstruct the disposable
            // hot set from verified evidence instead of restoring a bare total.
            restarted.request(PeerOperation::Offer { descriptor:Box::new(descriptor) }).await?;
            let recovered=restarted.merge_evidence(extension_id,batch).await?;
            assert!(matches!(recovered.control_response,WorkerControlOutcome::EvidenceMerged { accepted } if (59..=64).contains(&accepted)));
            let response=restarted.request(search()).await?;
            assert!(matches!(response.response,PeerOutcome::Search { page } if page.results[0].retained_support.verified_supporters==69));
            // Simulate loss of the controller without an orderly stop. The
            // container may still hold a partial/EOF stream; the next owner
            // must restart PID 1 rather than attach another reader to it.
            drop(restarted);
            let mut restarted=OfflineWorker::start(&runtime,&controller_directory,&deployment,&image,&[issuer],&limits).await?;
            let response=restarted.request(search()).await?;
            assert!(matches!(response.response,PeerOutcome::Search { page } if page.results.len()==1));
            let handle=restarted.handle().clone();
            restarted.stop(&runtime).await?;
            runtime.remove_container(&handle).await?;
            // Deliberate corruption only in this freshly owned disposable
            // fixture; future provisioning must refuse it, never reset it.
            docker(&["run","--rm","--pull=never","--network=none","--read-only","--user=10004:10004","--cap-drop=ALL",
                "--security-opt=no-new-privileges","--memory=64m","--memory-swap=64m","--cpus=0.1","--pids-limit=16","--log-driver=none",
                "--mount",&format!("type=volume,source={volume},target=/state,volume-nocopy"),"--entrypoint=/usr/local/bin/node",&image,
                "-e","require('node:fs').truncateSync('/state/metadata.sqlite',0)"]).await?;
            assert!(OfflineWorker::start(&runtime,&controller_directory,&deployment,&image,&[issuer],&limits).await.is_err());
            let size=docker(&["run","--rm","--pull=never","--network=none","--read-only","--user=10004:10004","--cap-drop=ALL",
                "--memory=64m","--memory-swap=64m","--cpus=0.1","--pids-limit=16","--log-driver=none",
                "--mount",&format!("type=volume,source={volume},target=/state,volume-nocopy,readonly"),"--entrypoint=/usr/local/bin/node",&image,
                "-e","process.stdout.write(String(require('node:fs').statSync('/state/metadata.sqlite').size))"]).await?;
            assert_eq!(size,"0","failed provisioning must not rewrite corrupt existing state");
            Ok::<(),anyhow::Error>(())
        }).catch_unwind().await;
        let cleanup: Result<()> = async {
            if let Some(handle) = runtime.get_container_handle(&name).await? {
                let state = runtime
                    .describe_container_runtime_state(&name)
                    .await?
                    .context("fixture cleanup inspection unavailable")?;
                if state.labels.get(ELIXIR_DEPLOYMENT_ID_LABEL) != Some(&deployment) {
                    bail!("fixture cleanup ownership mismatch");
                }
                runtime.remove_container(&handle).await?;
            }
            runtime.remove_private_file_volume(&volume, &labels).await?;
            Ok(())
        }
        .await;
        cleanup?;
        match result {
            Ok(result) => result,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }

    #[tokio::test]
    #[ignore = "explicit local Docker fixed worker server integration"]
    async fn server_provisions_reuses_and_refuses_corrupt_fixed_storage_over_private_stdio()
    -> Result<()> {
        exercise_fixed_worker(OrdinaryLimits::default(), false).await?;
        exercise_fixed_worker(
            OrdinaryLimits {
                memory_bytes: 128 * 1024 * 1024,
                cpu_millicores: 50,
                ..Default::default()
            },
            false,
        )
        .await
    }

    #[tokio::test]
    #[ignore = "explicit isolated native metadata verifier with actual fixed worker"]
    async fn independent_metadata_verifier_runs_alongside_the_fixed_catalog_in_reserved_budget()
    -> Result<()> {
        exercise_fixed_worker(
            OrdinaryLimits {
                memory_bytes: 224 * 1024 * 1024,
                cpu_millicores: 90,
                ..Default::default()
            },
            true,
        )
        .await
    }
}
