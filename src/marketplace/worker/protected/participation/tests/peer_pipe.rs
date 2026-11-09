//! Real private IPC/Noise in an offline container, synthetic owner watches.
//! This is NOT VPN, credit enforcement, public bootstrap or installed sharing.
use super::*;
use crate::{
    marketplace::client::PrivateWorkerClient,
    runtime::{PrivateProcessStdio, RuntimeManager, docker::DockerRuntimeManager},
};
use serde::{Deserialize, Serialize};
use std::{path::Path, process::Stdio};
use tokio::io::AsyncReadExt;

pub(in crate::marketplace) struct Fixture {
    pub client: ParticipationClient,
    pub peer: PeerKey,
    stop: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<Result<()>>,
    _status: watch::Sender<ParticipationSnapshot>,
    _protection: watch::Sender<Snapshot>,
}
impl Fixture {
    pub(in crate::marketplace) fn bind_security_database(&mut self, database: Database) {
        self.client.database = database;
    }
    pub(in crate::marketplace) fn bind_offline_source_owner(
        &mut self,
        worker: &crate::marketplace::worker::OfflineWorker,
        runtime: Arc<dyn RuntimeManager>,
        lane: &Path,
    ) -> Result<()> {
        self.client.source_jobs = Some(metrics::Source::fixture_from_offline(
            runtime,
            worker,
            self.client.database.clone(),
            lane.to_owned(),
        )?);
        Ok(())
    }
    pub(in crate::marketplace) fn defer_next_source_admission(&self) -> Result<()> {
        self.client
            .source_jobs
            .as_ref()
            .context("actual source owner missing")?
            .fixture_defer_once();
        Ok(())
    }
    pub(in crate::marketplace) async fn start(
        image: &str,
        data: &Path,
        name: &str,
        issuer: &str,
        mode: &str,
    ) -> Result<Self> {
        let features = DockerRuntimeManager::new(None).image_labels(image).await?;
        let blob_discovery = features
            .get("elixir.marketplace.worker.blob_discovery")
            .map(String::as_str)
            == Some("1");
        let sharing = mode == "sharing";
        if sharing
            && features
                .get("elixir.marketplace.worker.blob_sharing")
                .map(String::as_str)
                != Some("2")
        {
            bail!("sharing fixture requires an actual on-demand-capable image");
        }
        if mode == "providers" && !blob_discovery {
            bail!("provider fixture requires an actual discovery-capable image");
        }
        if matches!(mode, "fallback_empty" | "fallback_stale")
            && (!blob_discovery
                || features
                    .get("elixir.marketplace.worker.blob_catalog_fallback")
                    .map(String::as_str)
                    != Some("1"))
        {
            bail!("catalog fallback fixture requires the actual controller-fallback image");
        }
        let tests = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../crates/elixir_marketplace/worker/test")
            .canonicalize()?;
        let mut child = tokio::process::Command::new("docker")
            .args([
                "run",
                "--interactive",
                "--pull=never",
                "--name",
                name,
                "--label",
                "elixir.marketplace.fixture=peer-pipe-v1",
                "--label",
                &format!("elixir.marketplace.fixture.instance={name}"),
                "--network=none",
                "--read-only",
                "--user=10004:10004",
                "--cap-drop=ALL",
                "--security-opt=no-new-privileges",
                "--memory=224m",
                "--memory-swap=224m",
                "--cpus=0.09",
                "--pids-limit=64",
                "--log-driver=none",
                "--shm-size=1m",
                "--ulimit",
                "nofile=256:256",
                "--ulimit",
                "fsize=33554432:33554432",
                "--ulimit",
                "core=0:0",
                "--tmpfs",
                "/state:rw,noexec,nosuid,nodev,size=16m,uid=10004,gid=10004,mode=0700",
                "--tmpfs",
                "/tmp:rw,noexec,nosuid,nodev,size=8m,uid=10004,gid=10004,mode=0700",
                "--mount",
                &format!(
                    "type=bind,source={},target=/fixtures,readonly",
                    data.display()
                ),
                "--mount",
                &format!(
                    "type=bind,source={},target=/opt/elixir-marketplace/test,readonly",
                    tests.display()
                ),
                "--entrypoint=/usr/local/bin/node",
                image,
                "--max-old-space-size=48",
                "/opt/elixir-marketplace/test/peer-pipe-fixture.mjs",
                issuer,
                mode,
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let input = child.stdin.take().context("fixture stdin")?;
        let mut output = child.stdout.take().context("fixture stdout")?;
        let diagnostics = child.stderr.take();
        #[derive(Deserialize, Serialize)]
        #[serde(deny_unknown_fields)]
        struct Ready {
            schema_version: u32,
            peer: PeerKey,
            loopback_only: bool,
        }
        let ready = timeout(Duration::from_secs(45), async {
            let size = output.read_u32().await? as usize;
            if !(1..=4096).contains(&size) {
                bail!("fixture startup framing");
            }
            let mut bytes = vec![0; size];
            output.read_exact(&mut bytes).await?;
            let ready: Ready = elixir_marketplace::canonical::decode(&bytes, 4096)?;
            if ready.schema_version != 1 || !ready.loopback_only {
                bail!("fixture startup context");
            }
            Ok::<_, anyhow::Error>(ready)
        })
        .await
        .context("fixture startup deadline")??;
        let mut rpc = PrivateWorkerClient::new(PrivateProcessStdio {
            child,
            input,
            output,
            diagnostics,
        })?;
        let (requests, mut receiver) = mpsc::channel(1);
        let (faults, mut failures) = mpsc::channel(1);
        let (credits, _) = mpsc::channel(1);
        let (status, seen) = watch::channel(ParticipationSnapshot {
            phase: ParticipationPhase::Running,
            applied_roles: Some(NetworkRoles {
                consumer: true,
                catalog: sharing,
                routing: false,
            }),
            ..Default::default()
        });
        let (protection, observed) = watch::channel(Snapshot {
            state: ProtectionState::Ready,
            until: Some(Instant::now() + Duration::from_secs(600)),
        });
        sqlx::any::install_default_drivers();
        let client = ParticipationClient {
            blob_discovery,
            blob_sharing: sharing,
            database: Database {
                driver: crate::db::DatabaseDriver::Sqlite,
                pool: sqlx::any::AnyPoolOptions::new().connect_lazy("sqlite::memory:")?,
            },
            transfer_requests: requests.clone(),
            requests,
            status: seen,
            protection: observed,
            credits,
            source_preflight: None,
            source_jobs: None,
            security_mirror: None,
            faults,
            retirement: retirement::channel().1,
        };
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let result = tokio::select! {biased;
                _=&mut stopped=>Ok(()),
                _=failures.recv()=>Err(anyhow::anyhow!("fixture ownership failed")),
                result=async {
                    while let Some(request)=receiver.recv().await {
                        match request {
                            Request::Sharing(operation,reply)=>{
                                if !sharing {bail!("fixture has no installed sharing capability");}
                                if reply.is_closed(){continue;}
                                let response=rpc.control(operation).await?;
                                let state=match response.control_response {
                                    WorkerControlOutcome::Sharing{state}=>Ok(state),
                                    _=>Err(anyhow::anyhow!("fixture sharing operation rejected")),
                                };let _=reply.send(state);
                            }
                            Request::Providers(job,sha256,reply) => {
                                if !blob_discovery { bail!("fixture image has no provider discovery"); }
                                let response=rpc.control(WorkerControlOperation::BlobProviders {job,sha256}).await?;
                                let peers=match response.control_response {
                                    WorkerControlOutcome::BlobProviders {job:actual,sha256:observed,peers}
                                        if job==actual && sha256==observed=>Ok(peers),
                                    _=>Err(anyhow::anyhow!("fixture provider lookup rejected")),
                                };
                                let _=reply.send(LookupCompletion::replied(peers));
                            }
                            Request::Blob(operation,reply) => {
                                let cancel=matches!(operation,WorkerControlOperation::BlobCancel {..});
                                if reply.is_closed() && !cancel{continue;}
                                let response=rpc.control(operation).await?;
                                let state=match response.control_response {
                                    WorkerControlOutcome::BlobTransfer {state,..}=>Ok(state),
                                    _=>Err(anyhow::anyhow!("fixture blob operation rejected")),
                                };
                                let _=reply.send(state);
                            }
                            _=>bail!("unexpected fixture request"),
                        }
                    }
                    Ok(())
                }=>result,
            };
            rpc.close().await;
            result
        });
        Ok(Self {
            client,
            peer: ready.peer,
            stop: Some(stop),
            task,
            _status: status,
            _protection: protection,
        })
    }
    pub(in crate::marketplace) async fn finish(mut self) -> Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        timeout(Duration::from_secs(15), &mut self.task)
            .await
            .context("fixture shutdown deadline")??
    }
}
