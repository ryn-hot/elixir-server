//! Real offline-container Noise/private IPC/native receiver composition.
//! Owner watches are synthetic: NOT VPN/packet credits/public-source proof.
use super::*;
use crate::runtime::{docker::DockerRuntimeManager, model::ContainerState};
use futures_util::FutureExt;
use std::os::unix::fs::PermissionsExt;

struct RestoreDirectory(std::path::PathBuf);
impl Drop for RestoreDirectory {
    fn drop(&mut self) {
        let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o700));
    }
}

pub(in crate::marketplace) async fn docker(args: &[&str]) -> Result<String> {
    let output = timeout(
        Duration::from_secs(30),
        tokio::process::Command::new("docker")
            .args(args)
            .kill_on_drop(true)
            .output(),
    )
    .await??;
    if !output.status.success() {
        bail!(
            "peer quarantine fixture Docker operation failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}
pub(in crate::marketplace) async fn counters(name: &str) -> Result<serde_json::Value> {
    let text = docker(&[
        "exec",
        "--user=10004:10004",
        name,
        "/usr/local/bin/node",
        "-e",
        "process.stdout.write(require('node:fs').readFileSync('/state/fixture-counters.json'))",
    ])
    .await?;
    Ok(serde_json::from_str(&text)?)
}
pub(in crate::marketplace) async fn remove_peer(name: &str) -> Result<()> {
    let runtime = DockerRuntimeManager::new(None);
    let Some(handle) = runtime.get_container_handle(name).await? else {
        return Ok(());
    };
    let labels: HashMap<String, String> = serde_json::from_str(
        &docker(&["inspect", "--format", "{{json .Config.Labels}}", name]).await?,
    )?;
    if labels.get("elixir.marketplace.fixture").map(String::as_str) != Some("peer-pipe-v1")
        || labels
            .get("elixir.marketplace.fixture.instance")
            .map(String::as_str)
            != Some(name)
    {
        bail!("refusing foreign peer fixture cleanup");
    }
    runtime.stop_container(&handle).await?;
    runtime.remove_container(&handle).await
}
pub(in crate::marketplace) async fn check_profile(name: &str) -> Result<()> {
    let value: serde_json::Value = serde_json::from_str(&docker(&["inspect", name]).await?)?;
    let config = &value[0]["HostConfig"];
    assert_eq!(config["NetworkMode"], "none");
    assert_eq!(config["ReadonlyRootfs"], true);
    assert_eq!(config["Memory"], 224 * MIB);
    assert_eq!(config["MemorySwap"], 224 * MIB);
    assert_eq!(config["NanoCpus"], 90_000_000);
    assert_eq!(config["PidsLimit"], 64);
    assert_eq!(value[0]["Config"]["User"], "10004:10004");
    Ok(())
}
async fn source_fixture() -> Result<(AuthorizedRelease, Vec<u8>, Vec<u8>, String)> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../extensions/sdk/marketplace/test/fixture.mjs");
    let script = "import{pathToFileURL}from'node:url';const{createFixture}=await import(pathToFileURL(process.argv[1]));const f=createFixture({operational:true,sourceBytes:Buffer.from('export const version = 1;\\n/*'+'x'.repeat(2*1024*1024)+'*/\\n')});const{publisher,release,issuer_key,package_base64,source_proof_base64}=f;process.stdout.write(JSON.stringify({publisher,release,issuer_key,package_base64,source_proof_base64}));";
    let output = timeout(
        Duration::from_secs(15),
        tokio::process::Command::new("node")
            .args(["--input-type=module", "-e", script])
            .arg(path)
            .kill_on_drop(true)
            .output(),
    )
    .await??;
    if !output.status.success() || output.stdout.len() > 8 * 1024 * 1024 {
        bail!("large SDK fixture generation failed");
    }
    let value: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let issuer = value["issuer_key"]
        .as_str()
        .context("fixture issuer")?
        .to_owned();
    let (release, package, proof) = super::tests::decode_fixture(value)?;
    Ok((release, package, proof, issuer))
}

pub(super) struct Unavailable;
#[async_trait::async_trait]
impl RuntimeManager for Unavailable {
    async fn read_container_file(&self, _: &ContainerHandle, _: &str) -> Result<Option<Vec<u8>>> {
        bail!("receiver unavailable")
    }
    async fn copy_host_path_to_container(
        &self,
        _: &ContainerHandle,
        _: &Path,
        _: &str,
    ) -> Result<()> {
        bail!("receiver unavailable")
    }
    async fn ensure_container_directories(&self, _: &ContainerHandle, _: &[String]) -> Result<()> {
        bail!("receiver unavailable")
    }
    async fn ensure_container_directories_owned_like(
        &self,
        _: &ContainerHandle,
        _: &str,
        _: &[String],
    ) -> Result<bool> {
        bail!("receiver unavailable")
    }
    async fn ensure_network(&self, _: &str) -> Result<()> {
        bail!("receiver unavailable")
    }
    async fn ensure_container(&self, _: &ContainerSpec) -> Result<ContainerHandle> {
        bail!("receiver unavailable")
    }
    async fn get_container_handle(&self, _: &str) -> Result<Option<ContainerHandle>> {
        bail!("receiver unavailable")
    }
    async fn start_container(&self, _: &ContainerHandle) -> Result<()> {
        bail!("receiver unavailable")
    }
    async fn stop_container(&self, _: &ContainerHandle) -> Result<()> {
        bail!("receiver unavailable")
    }
    async fn rename_container(&self, _: &ContainerHandle, _: &str) -> Result<ContainerHandle> {
        bail!("receiver unavailable")
    }
    async fn remove_container(&self, _: &ContainerHandle) -> Result<()> {
        bail!("receiver unavailable")
    }
    async fn container_logs(
        &self,
        _: &ContainerHandle,
        _: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<String> {
        bail!("receiver unavailable")
    }
    async fn inspect(&self, _: &ContainerHandle) -> Result<ContainerState> {
        bail!("receiver unavailable")
    }
}

#[tokio::test]
#[ignore = "explicit offline Docker peer/private-pipe/native-quarantine composition, not VPN or install qualification"]
async fn peer_pipe_to_native_quarantine_checks_integrity_and_cleanup_without_install_authority()
-> Result<()> {
    let runtime = Arc::new(DockerRuntimeManager::new(None));
    let reference = std::env::var("ELIXIR_MARKETPLACE_WORKER_IMAGE")
        .context("explicit immutable worker image required")?;
    let image = docker(&["image", "inspect", "--format", "{{.Id}}", &reference]).await?;
    let features = runtime.image_labels(&image).await?;
    assert_eq!(
        features
            .get("elixir.marketplace.worker.blob_transport")
            .map(String::as_str),
        Some("1")
    );
    let directory = tempfile::tempdir()?;
    let root = directory.path().canonicalize()?;
    let files = root.join("public-fixtures");
    std::fs::create_dir(&files)?;
    let _restore = RestoreDirectory(files.clone());
    let (release, package, proof, issuer) = source_fixture().await?;
    for (name, bytes) in [("package.blob", &package), ("proof.blob", &proof)] {
        let path = files.join(name);
        std::fs::write(&path, bytes)?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444))?;
    }
    std::fs::set_permissions(&files, std::fs::Permissions::from_mode(0o555))?;
    let budget = SourceJobBudget {
        memory_bytes: 64 * MIB,
        cpu_millicores: 100,
        disk_bytes: 128 * MIB,
        input_bytes: 128 * MIB,
        output_bytes: 128 * MIB,
        wall_seconds: 300,
    };
    let plan = SourceQuarantinePlan::for_release(&release, None, &budget)?;
    let private = root.join("expensive");
    for mode in ["normal", "corrupt", "stall"] {
        eprintln!(
            "peer quarantine fixture mode={mode} package={} proof={}",
            package.len(),
            proof.len()
        );
        let deployment = format!("peer-{}", uuid::Uuid::new_v4().simple());
        let name = format!(
            "elixir-marketplace-peer-fixture-{}",
            uuid::Uuid::new_v4().simple()
        );
        let volume = format!("elixir-marketplace-source-{deployment}");
        let writer = writer_name(&deployment);
        let labels = HashMap::from([
            (ELIXIR_MANAGED_LABEL.to_owned(), "true".to_owned()),
            (ELIXIR_DEPLOYMENT_ID_LABEL.to_owned(), deployment.clone()),
            (
                "elixir.marketplace.volume".to_owned(),
                "quarantine-v1".to_owned(),
            ),
        ]);
        let mut fixture = None;
        let outcome=std::panic::AssertUnwindSafe(async {
            fixture=Some(super::super::worker::PeerPipeFixture::start(&image,&files,&name,&issuer,mode).await?);
            let fixture=fixture.as_ref().unwrap();check_profile(&name).await?;
            let mut job=SourceQuarantineJob::open(runtime.clone(),&private,&deployment,&image,&plan).await?;
            assert!(job.receive_peer(&Unavailable,&fixture.client,fixture.peer.clone(),QuarantinePurpose::Package).await.is_err());
            assert_eq!(counters(&name).await?["starts"],0,"failed quarantine readiness must not start a transfer");
            if mode=="stall" {
                let receive=job.receive_peer(runtime.as_ref(),&fixture.client,fixture.peer.clone(),QuarantinePurpose::Package);
                let mut receive=Box::pin(receive);
                timeout(Duration::from_secs(30),async {
                    loop {tokio::select! {
                        result=&mut receive=>{result?;bail!("stalled fixture unexpectedly completed");},
                        _=tokio::time::sleep(Duration::from_millis(200))=>{
                            if let Some(handle)=runtime.get_container_handle(&writer).await? {
                                let command=vec!["/usr/bin/od".to_owned(),"-An".into(),"-t".into(),"x1".into(),"-N3".into(),"/quarantine/package.blob".into()];
                                if let Ok(text)=runtime.exec_container_command(&handle,"10004:10004",&command).await
                                    && text.split_whitespace().collect::<String>()=="010101" {
                                    break Ok::<_,anyhow::Error>(());
                                }
                            }
                        }
                    }}
                }).await??;
                drop(receive);
                timeout(Duration::from_secs(10),async {
                    while counters(&name).await?["cancels"].as_u64().unwrap_or(0)==0 {tokio::time::sleep(Duration::from_millis(100)).await;}
                    Ok::<_,anyhow::Error>(())
                }).await??;
                assert!(job.completed.is_empty());job.cancel(runtime.as_ref()).await?;
            } else if mode=="corrupt" {
                assert!(job.receive_peer(runtime.as_ref(),&fixture.client,fixture.peer.clone(),QuarantinePurpose::Package).await.is_err());
                assert!(job.completed.is_empty());assert!(job.finish(runtime.as_ref()).await.is_err());
            } else {
                job.receive_peer(runtime.as_ref(),&fixture.client,fixture.peer.clone(),QuarantinePurpose::Package).await?;
                job.receive_peer(runtime.as_ref(),&fixture.client,fixture.peer.clone(),QuarantinePurpose::Proof).await?;
                let matched=job.finish(runtime.as_ref()).await?.match_source(runtime.as_ref()).await?;
                assert_eq!(matched.summary().release_id,release.release_id());assert_eq!(matched.summary().files,3);
                assert!(matched.require_public_source().is_err(),"offline equality is not public-source/install authority");
                assert!(matches!(matched.check_manifest()?.selection(),super::super::manifest::PublicRuntimeSelection::SourceJs {..}));
            }
            assert!(ControllerLease::acquire(&private).is_ok());
            Ok::<_,anyhow::Error>(())
        }).catch_unwind().await;
        let stopped = if let Some(fixture) = fixture {
            fixture.finish().await
        } else {
            Ok(())
        };
        let peer_cleanup = remove_peer(&name).await;
        let writer_cleanup = async {
            if let Some(handle) = runtime.get_container_handle(&writer).await? {
                let actual: HashMap<String, String> = serde_json::from_str(
                    &docker(&["inspect", "--format", "{{json .Config.Labels}}", &handle.id])
                        .await?,
                )?;
                if actual.get(ELIXIR_DEPLOYMENT_ID_LABEL) != Some(&deployment)
                    || actual.get(ELIXIR_MANAGED_LABEL).map(String::as_str) != Some("true")
                {
                    bail!("foreign writer cleanup");
                }
                runtime.stop_container(&handle).await?;
                runtime.remove_container(&handle).await?;
            }
            runtime.remove_private_file_volume(&volume, &labels).await
        }
        .await;
        match outcome {
            Ok(result) => result?,
            Err(panic) => std::panic::resume_unwind(panic),
        };
        stopped?;
        peer_cleanup?;
        writer_cleanup?;
    }
    Ok(())
}
