//! Real source bytes/Git match -> real inspected source executor -> original
//! helper lifetime. Offline network=none, synthetic resource/grant admission.
//! No NativeScope constructor, fake Ready, public-source or VPN capability.
use super::*;
use crate::{
    marketplace::quarantine::{SourceMatchedQuarantine, execution::Broker},
    runtime::{
        PrivateProcessStdio,
        docker::DockerRuntimeManager,
        model::{ContainerRuntimeState, QuarantineMarketplaceVolumeSpec, SourceCacheState},
        packet_network::immutable_id,
    },
};
use futures_util::FutureExt;
use std::{
    path::PathBuf,
    sync::{Mutex, atomic::AtomicUsize},
};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Gate {
    Created,
    Started,
    Ready,
    Attached,
    Rejected,
    Material,
}

// Emergency fixture cleanup only; never interpreted as successful retirement.
// Every target has an exact CID and the original private fixture's labels.
type OwnedActors = Arc<Mutex<Vec<(ContainerHandle, HashMap<String, String>)>>>;
struct Emergency {
    actors: OwnedActors,
    volume: String,
    labels: HashMap<String, String>,
}
impl Drop for Emergency {
    fn drop(&mut self) {
        for (h, labels) in self.actors.lock().unwrap().iter().rev() {
            if let Ok(output) = std::process::Command::new("docker")
                .args(["inspect", "--format", "{{json .Config.Labels}}", &h.id])
                .output()
                && output.status.success()
                && serde_json::from_slice::<HashMap<String, String>>(&output.stdout)
                    .is_ok_and(|actual| labels.iter().all(|(k, v)| actual.get(k) == Some(v)))
            {
                let _ = std::process::Command::new("docker")
                    .args(["rm", "-f", &h.id])
                    .output();
            }
        }
        if let Ok(output) = std::process::Command::new("docker")
            .args([
                "volume",
                "inspect",
                "--format",
                "{{json .Labels}}",
                &self.volume,
            ])
            .output()
            && output.status.success()
            && serde_json::from_slice::<HashMap<String, String>>(&output.stdout)
                .is_ok_and(|actual| self.labels.iter().all(|(k, v)| actual.get(k) == Some(v)))
        {
            // No force: a still-owned/in-use volume is not cleanup success.
            let _ = std::process::Command::new("docker")
                .args(["volume", "rm", &self.volume])
                .output();
        }
    }
}
struct Runtime {
    inner: Arc<DockerRuntimeManager>,
    gate: Gate,
    lane: PathBuf,
    journal: &'static str,
    reached: tokio::sync::Notify,
    release: tokio::sync::Notify,
    helper: Mutex<Option<ContainerHandle>>,
    starts: AtomicUsize,
    owned: OwnedActors,
}
impl Runtime {
    fn phase(&self, expected: &str) -> Result<serde_json::Value> {
        self.phase_in(self.journal, expected)
    }
    fn phase_in(&self, journal: &str, expected: &str) -> Result<serde_json::Value> {
        let value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(self.lane.join(journal))?)?;
        if value["state"]["phase"] != expected {
            bail!("native fixture child phase differs");
        }
        Ok(value)
    }
    async fn material_boundary(&self) {
        // The trigger is actual private-prefix transmission, not a mutable
        // image lookup (which correctly no longer repeats for a pinned ID).
        // Observe only size, never credential/config contents.
        if self.gate == Gate::Material {
            let helper = self.helper.lock().unwrap().clone();
            if let Some(handle) = helper
                && let Ok(size) = self
                    .inner
                    .exec_container_command(
                        &handle,
                        "0:0",
                        &[
                            "/bin/busybox".into(),
                            "wc".into(),
                            "-c".into(),
                            "/gluetun/wireguard/wg0.conf".into(),
                        ],
                    )
                    .await
                && size
                    .split_whitespace()
                    .next()
                    .and_then(|v| v.parse::<usize>().ok())
                    .is_some_and(|n| n > 0)
            {
                self.reached.notify_one();
                self.release.notified().await;
            }
        }
    }
}
fn unused<T>() -> Result<T> {
    bail!("unexpected native helper fixture operation")
}

#[async_trait::async_trait]
impl RuntimeManager for Runtime {
    async fn local_image_id(&self, reference: &str) -> Result<String> {
        self.inner.local_image_id(reference).await
    }
    async fn create_application_gateway(
        &self,
        p: &crate::marketplace::application_egress::ApplicationGatewayProfile,
        attempt: crate::marketplace::application_egress::GatewayCreateAttempt,
    ) -> Result<crate::runtime::gateway_creation::GatewayCreation> {
        self.phase("creating")?;
        let result = self.inner.create_application_gateway(p, attempt).await?;
        let handle = match result {
            crate::runtime::gateway_creation::GatewayCreation::Created(handle) => handle,
            crate::runtime::gateway_creation::GatewayCreation::NotAttempted(outcome) => {
                if self.gate == Gate::Rejected {
                    self.reached.notify_one();
                    self.release.notified().await;
                }
                return Ok(
                    crate::runtime::gateway_creation::GatewayCreation::NotAttempted(outcome),
                );
            }
        };
        self.owned
            .lock()
            .unwrap()
            .push((handle.clone(), p.spec().labels.clone()));
        *self.helper.lock().unwrap() = Some(handle.clone());
        if self.gate == Gate::Created {
            self.reached.notify_one();
            self.release.notified().await;
        }
        Ok(crate::runtime::gateway_creation::GatewayCreation::Created(
            handle,
        ))
    }
    async fn start_application_gateway(
        &self,
        h: &ContainerHandle,
        p: &crate::marketplace::application_egress::ApplicationGatewayProfile,
    ) -> Result<()> {
        assert_eq!(self.phase("bound")?["state"]["container_id"], h.id);
        self.inner.start_application_gateway(h, p).await?;
        // Fixture only: an internal Docker bridge has no usable default
        // route. Supply its PRIVATE bridge gateway inside this exact namespace
        // so Gluetun can initialize; Docker's internal isolation still forbids
        // external traffic. This is not a protected-route/readiness assertion.
        let net = self
            .inner
            .inspect_packet_uplink(&p.spec().network)
            .await?
            .context("fixture uplink")?;
        if net.internal {
            let gateway = net
                .subnets
                .iter()
                .find(|s| s.gateway.is_ipv4())
                .context("fixture v4 gateway")?
                .gateway
                .to_string();
            self.inner
                .exec_container_command(
                    h,
                    "0:0",
                    &[
                        "/bin/busybox".into(),
                        "ip".into(),
                        "route".into(),
                        "replace".into(),
                        "default".into(),
                        "via".into(),
                        gateway,
                        "dev".into(),
                        "eth0".into(),
                    ],
                )
                .await?;
        }
        self.starts.fetch_add(1, Ordering::SeqCst);
        if self.gate == Gate::Started {
            self.reached.notify_one();
            self.release.notified().await;
        }
        Ok(())
    }
    async fn attach_application_gateway(
        &self,
        h: &ContainerHandle,
        p: &crate::marketplace::application_egress::ApplicationGatewayProfile,
    ) -> Result<PrivateProcessStdio> {
        let process = self.inner.attach_application_gateway(h, p).await?;
        if self.gate == Gate::Attached {
            self.reached.notify_one();
            self.release.notified().await;
        }
        Ok(process)
    }
    async fn inspect_unstarted_application_gateway(
        &self,
        p: &crate::marketplace::application_egress::ApplicationGatewayProfile,
    ) -> Result<ContainerHandle> {
        self.inner.inspect_unstarted_application_gateway(p).await
    }
    async fn stop_application_gateway(
        &self,
        h: &ContainerHandle,
        p: &crate::marketplace::application_egress::ApplicationGatewayProfile,
    ) -> Result<()> {
        self.inner.stop_application_gateway(h, p).await
    }
    async fn remove_application_gateway(
        &self,
        h: &ContainerHandle,
        p: &crate::marketplace::application_egress::ApplicationGatewayProfile,
    ) -> Result<()> {
        self.inner.remove_application_gateway(h, p).await
    }
    async fn attach_application_http(
        &self,
        h: &ContainerHandle,
        p: &ApplicationHttpProfile,
    ) -> Result<PrivateProcessStdio> {
        let process = self.inner.attach_application_http(h, p).await?;
        if self.gate == Gate::Attached {
            self.reached.notify_one();
            self.release.notified().await;
        }
        Ok(process)
    }
    async fn create_application_http(
        &self,
        p: &ApplicationHttpProfile,
        attempt: crate::marketplace::runtime::application_http::HttpCreateAttempt,
    ) -> Result<crate::runtime::http_creation::HttpCreation> {
        self.phase_in("http-helper.pending", "creating")?;
        let result = self.inner.create_application_http(p, attempt).await?;
        let h = match result {
            crate::runtime::http_creation::HttpCreation::Created(h) => h,
            crate::runtime::http_creation::HttpCreation::NotAttempted(outcome) => {
                if self.gate == Gate::Rejected {
                    self.reached.notify_one();
                    self.release.notified().await;
                }
                return Ok(crate::runtime::http_creation::HttpCreation::NotAttempted(
                    outcome,
                ));
            }
        };
        self.owned
            .lock()
            .unwrap()
            .push((h.clone(), p.spec().labels.clone()));
        if self.journal == "http-helper.pending" {
            *self.helper.lock().unwrap() = Some(h.clone());
        }
        if self.gate == Gate::Created {
            self.reached.notify_one();
            self.release.notified().await;
        }
        Ok(crate::runtime::http_creation::HttpCreation::Created(h))
    }
    async fn start_application_http(
        &self,
        h: &ContainerHandle,
        p: &ApplicationHttpProfile,
    ) -> Result<()> {
        if self.phase_in("http-helper.pending", "bound")?["state"]["container_id"] != h.id {
            bail!("native helper START preceded durable CID");
        }
        self.inner.start_application_http(h, p).await?;
        self.starts.fetch_add(1, Ordering::SeqCst);
        if self.gate == Gate::Started {
            self.reached.notify_one();
            self.release.notified().await;
        }
        Ok(())
    }
    async fn inspect_unstarted_application_http(
        &self,
        p: &ApplicationHttpProfile,
    ) -> Result<ContainerHandle> {
        self.inner.inspect_unstarted_application_http(p).await
    }
    async fn stop_application_http(
        &self,
        h: &ContainerHandle,
        p: &ApplicationHttpProfile,
    ) -> Result<()> {
        self.inner.stop_application_http(h, p).await
    }
    async fn remove_application_http(
        &self,
        h: &ContainerHandle,
        p: &ApplicationHttpProfile,
    ) -> Result<()> {
        self.inner.remove_application_http(h, p).await
    }
    async fn image_labels(&self, image: &str) -> Result<HashMap<String, String>> {
        self.inner.image_labels(image).await
    }
    async fn ensure_quarantine_marketplace_volume(
        &self,
        s: &QuarantineMarketplaceVolumeSpec,
    ) -> Result<()> {
        self.inner.ensure_quarantine_marketplace_volume(s).await
    }
    async fn create_quarantine_marketplace_writer(
        &self,
        s: &ContainerSpec,
    ) -> Result<ContainerHandle> {
        let h = self.inner.create_quarantine_marketplace_writer(s).await?;
        self.owned
            .lock()
            .unwrap()
            .push((h.clone(), s.labels.clone()));
        Ok(h)
    }
    async fn start_quarantine_marketplace_writer(
        &self,
        h: &ContainerHandle,
        s: &ContainerSpec,
        limits: &SourceJobLimits,
    ) -> Result<()> {
        self.inner
            .start_quarantine_marketplace_writer(h, s, limits)
            .await
    }
    async fn stop_quarantine_marketplace_writer(&self, h: &ContainerHandle) -> Result<()> {
        self.inner.stop_quarantine_marketplace_writer(h).await
    }
    async fn source_cache_state(
        &self,
        name: &str,
        labels: &HashMap<String, String>,
    ) -> Result<SourceCacheState> {
        self.inner.source_cache_state(name, labels).await
    }
    async fn attach_private_stdio(
        &self,
        h: &ContainerHandle,
        labels: &HashMap<String, String>,
    ) -> Result<PrivateProcessStdio> {
        self.inner.attach_private_stdio(h, labels).await
    }
    async fn confirm_quarantine_marketplace_success(
        &self,
        h: &ContainerHandle,
        labels: &HashMap<String, String>,
    ) -> Result<()> {
        self.inner
            .confirm_quarantine_marketplace_success(h, labels)
            .await
    }
    async fn private_file_volume_owned(
        &self,
        name: &str,
        labels: &HashMap<String, String>,
    ) -> Result<bool> {
        self.inner.private_file_volume_owned(name, labels).await
    }
    async fn remove_private_file_volume(
        &self,
        name: &str,
        labels: &HashMap<String, String>,
    ) -> Result<()> {
        self.inner.remove_private_file_volume(name, labels).await
    }
    async fn exec_container_command(
        &self,
        h: &ContainerHandle,
        user: &str,
        command: &[String],
    ) -> Result<String> {
        self.inner.exec_container_command(h, user, command).await
    }
    async fn describe_container_runtime_state(
        &self,
        id: &str,
    ) -> Result<Option<ContainerRuntimeState>> {
        self.inner.describe_container_runtime_state(id).await
    }
    async fn get_container_handle(&self, id: &str) -> Result<Option<ContainerHandle>> {
        self.inner.get_container_handle(id).await
    }
    async fn inspect(&self, h: &ContainerHandle) -> Result<crate::runtime::model::ContainerState> {
        let actual = self.inner.inspect(h).await?;
        self.material_boundary().await;
        Ok(actual)
    }
    async fn remove_container(&self, h: &ContainerHandle) -> Result<()> {
        self.inner.remove_container(h).await
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
struct Authority;
#[async_trait::async_trait]
impl SdkHttpAuthority for Authority {
    async fn current(&self) -> Result<()> {
        Ok(())
    } // TEST admission, no installed grant or VPN.
}
struct Scope {
    native: Option<Arc<NativeInvocation>>,
    // Import through the outer caller-facing path, not this private child.
    http: Option<crate::marketplace::quarantine::execution::HttpLaunch>,
    began: bool,
    image: String,
    runtime: Arc<Runtime>,
}
#[async_trait::async_trait]
impl Broker for Scope {
    fn native_started(&mut self, invocation: NativeInvocation) -> Result<()> {
        if self.native.is_some() {
            bail!("native fixture invoked twice");
        }
        self.native = Some(Arc::new(invocation));
        Ok(())
    }
    async fn current(&mut self) -> Result<()> {
        if let Some(native) = self.native.as_ref() {
            native.current().await?;
            if !self.began {
                self.began = true;
                // The actual source's namespace is network=none. It provides
                // ONLY a real immutable namespace ID, not protected egress.
                self.http = Some(
                    native
                        .launch_application_http(
                            "storage-offline-lifetime-fixture",
                            &self.image,
                            &native.handle.id,
                            Arc::new(Authority),
                        )
                        .await?,
                );
                self.http
                    .as_mut()
                    .context("original native helper owner")?
                    .ready()
                    .await?;
                if self.runtime.gate == Gate::Ready {
                    self.runtime.reached.notify_one();
                }
                if self.runtime.gate == Gate::Attached {
                    self.http
                        .as_mut()
                        .context("original helper I/O owner")?
                        .attach_io()
                        .await?;
                }
                self.runtime.release.notified().await;
                bail!("controlled fixture source exchange stop");
            }
        }
        Ok(())
    }
    async fn request(&mut self, _: &RuntimeJson, _: usize) -> Result<SourceSdkReply> {
        bail!("native lifetime qualification must never request an SDK effect")
    }
}

#[tokio::test]
#[ignore = "exclusive offline actual source/VM + actual unsupported helper image preflight rejection; no CREATE, synthetic grants/resources, network=none, no HTTP"]
async fn native_source_rejected_helper_image_releases_lane_without_cleanup_by_name() -> Result<()> {
    qualify(Gate::Rejected).await
}

#[tokio::test]
#[ignore = "exclusive offline real matched source/inspected VM + helper; cancel after CREATE before returned CID; synthetic grant/resources, network=none, no HTTP"]
async fn native_source_caller_loss_keeps_late_created_http_helper_owned() -> Result<()> {
    qualify(Gate::Created).await
}
#[tokio::test]
#[ignore = "exclusive offline real matched source/inspected VM + helper; cancelled waiter after actual START before reply; synthetic grant/resources, network=none, no HTTP"]
async fn native_source_cancel_waiter_joins_original_late_http_start() -> Result<()> {
    qualify(Gate::Started).await
}
#[tokio::test]
#[ignore = "exclusive offline real matched source/inspected VM + ready helper; revoke actual native scope; synthetic grant/resources, network=none, no HTTP"]
async fn native_source_scope_loss_retires_its_original_ready_http_helper() -> Result<()> {
    qualify(Gate::Ready).await
}

#[tokio::test]
#[ignore = "exclusive offline actual source/VM/helper+attach; cancelled original attach waiter must retain pipes and fence; synthetic grants/resources, network=none, NO HTTP/VPN"]
async fn native_source_cancelled_attach_joins_original_pipes_before_clearing_child_fence()
-> Result<()> {
    qualify(Gate::Attached).await
}

// Synthetic SDK text transport only. Original source/native/current checks,
// private framing, job counters and retirement below use the real controller.
struct LargeText {
    native: Option<Arc<NativeInvocation>>,
    expected: Option<elixir_marketplace::source_job::SourceMatchDescriptor>,
    maximum: usize,
    sent: usize,
    closed: bool,
    calls: usize,
    inspections: usize,
}
impl LargeText {
    async fn effect_current(&mut self) -> Result<()> {
        self.native
            .as_ref()
            .context("SDK request before actual native Ready")?
            .current()
            .await?;
        self.inspections += 1;
        Ok(())
    }
}
#[async_trait::async_trait]
impl Broker for LargeText {
    fn native_started(&mut self, native: NativeInvocation) -> Result<()> {
        if self.native.is_some() {
            bail!("large fixture native owner replaced");
        }
        native.require_invoke(
            self.expected
                .as_ref()
                .context("large fixture source owner missing")?,
        )?;
        self.native = Some(Arc::new(native));
        Ok(())
    }
    async fn current(&mut self) -> Result<()> {
        // Same framing restriction as InstalledBroker::current/check, NOT a
        // running/retired proof. Actual native inspection remains at every
        // SDK effect below, and original native exit is independently joined.
        if let Some(native) = &self.native {
            native.check()?;
        }
        Ok(())
    }
    async fn request(&mut self, raw: &RuntimeJson, _: usize) -> Result<SourceSdkReply> {
        use elixir_marketplace::source_execution::SourceSdkRequest;
        self.effect_current().await?;
        self.calls += 1;
        let value = match SourceSdkRequest::parse(raw)? {
            SourceSdkRequest::FetchOpen {
                stream_id: 1,
                request,
                maximum_body_bytes,
            } => {
                assert_eq!(maximum_body_bytes, self.maximum as u64);
                assert!(
                    matches!(request.as_ref(),SourceSdkRequest::Fetch{url,..} if url=="https://example.invalid/feed")
                );
                let until = Instant::now() + Duration::from_secs(35);
                while Instant::now() < until {
                    tokio::time::sleep(
                        Duration::from_millis(500)
                            .min(until.saturating_duration_since(Instant::now())),
                    )
                    .await;
                    self.current().await?;
                }
                serde_json::json!({"stream_id":1,"status":200,"headers":[]})
            }
            SourceSdkRequest::FetchRead { stream_id: 1 } => {
                let n = (self.maximum - self.sent).min(256 * 1024);
                assert!(n > 0);
                self.sent += n;
                serde_json::json!({"stream_id":1,"chunk":"x".repeat(n),"done":self.sent==self.maximum})
            }
            SourceSdkRequest::FetchClose { stream_id: 1 } => {
                self.closed = true;
                serde_json::json!(true)
            }
            _ => bail!("unexpected large-text fixture SDK operation"),
        };
        self.effect_current().await?;
        Ok(SourceSdkReply::Value(RuntimeJson::from_value(value)?))
    }
}

#[tokio::test]
#[ignore = "exclusive actual Rust controller/verified source/inspected native/35s SDK+48MiB text; transport/resources/grants synthetic, NO VPN/HTTP/playback/provider port proof"]
async fn native_large_feed_controller_preserves_slow_sdk_full_body_and_original_retirement()
-> Result<()> {
    let image = std::env::var("ELIXIR_MARKETPLACE_APPLICATION_HTTP_IMAGE")?;
    if !image.strip_prefix("sha256:").is_some_and(immutable_id) {
        bail!("exact large-profile worker image required");
    }
    let root = tempfile::tempdir()?;
    let lane = root.path().canonicalize()?.join("global");
    let deployment = format!("source-large-{}", uuid::Uuid::new_v4().simple());
    let volume = format!("elixir-marketplace-source-{deployment}");
    let labels = HashMap::from([
        (ELIXIR_MANAGED_LABEL.into(), "true".into()),
        (ELIXIR_DEPLOYMENT_ID_LABEL.into(), deployment.clone()),
        ("elixir.marketplace.volume".into(), "quarantine-v1".into()),
    ]);
    let owned = Arc::new(Mutex::new(Vec::new()));
    let _emergency = Emergency {
        actors: owned.clone(),
        volume: volume.clone(),
        labels: labels.clone(),
    };
    let inner = Arc::new(DockerRuntimeManager::new(None));
    let runtime = Arc::new(Runtime {
        inner: inner.clone(),
        gate: Gate::Ready,
        lane: lane.clone(),
        journal: "http-helper.pending",
        reached: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        helper: Mutex::default(),
        starts: AtomicUsize::new(0),
        owned,
    });
    let mut matched: Option<SourceMatchedQuarantine> = None;
    let mut broker = LargeText {
        native: None,
        expected: None,
        maximum: 48 * MIB as usize,
        sent: 0,
        closed: false,
        calls: 0,
        inspections: 0,
    };
    let outcome=std::panic::AssertUnwindSafe(async {
        let (release,package,proof)=crate::marketplace::quarantine::tests::operational_fixture_with_source(
            "import {fetchStream} from '@elixir/sdk/v1';export async function invoke(n){const r=await fetchStream('https://example.invalid/feed',{maximumBodyBytes:n});const text=await r.text();return {bytes:text.length,ends:text[0]+text[text.length-1]};}").await?;
        let budget=SourceJobBudget{memory_bytes:256*MIB,input_bytes:512*MIB,output_bytes:128*MIB,
            ..crate::marketplace::quarantine::tests::budget()};
        let plan=SourceQuarantinePlan::for_release(&release,None,&budget)?;
        let mut job=SourceQuarantineJob::open(runtime.clone(),&lane,&deployment,&image,&plan).await?;
        job.receive(runtime.as_ref(),QuarantinePurpose::Package,&mut package.as_slice()).await?;
        job.receive(runtime.as_ref(),QuarantinePurpose::Proof,&mut proof.as_slice()).await?;
        let source=job.finish(runtime.as_ref()).await?;
        matched=Some(source.match_source(runtime.as_ref()).await?);
        broker.expected=Some(matched.as_ref().unwrap().execution_descriptor());
        let arguments=RuntimeJson::from_value(serde_json::json!(broker.maximum))?;
        let began=Instant::now();
        let answer=matched.as_mut().unwrap().evaluate_native(SourceExecutionMode::Invoke,
            SourceExecutionLimits::large_feed(),&arguments,&mut broker).await?;
        assert!(began.elapsed()>Duration::from_secs(35));
        assert_eq!(answer.as_str(),format!("{{\"bytes\":{},\"ends\":\"xx\"}}",broker.maximum));
        assert_eq!(broker.sent,broker.maximum);assert!(broker.closed);
        assert_eq!(broker.calls,2+broker.maximum/(256*1024));
        assert_eq!(broker.inspections,2*broker.calls,"fresh native observations at every SDK effect");
        assert!(broker.native.as_ref().unwrap().check().is_err(),"original native scope must be revoked after return");
        assert!(!lane.join("native-job.pending").exists());
        assert!(runtime.helper.lock().unwrap().is_none());
        eprintln!("actual Rust controller large profile PASS: elapsed={:?},48MiB,original scope/frames/accounting/retirement; NO real HTTP/VPN/grant/resource/playback proof",began.elapsed());
        Ok::<_,anyhow::Error>(())
    }).catch_unwind().await;
    eprintln!(
        "large text fixture progress: bytes={},calls={},native_effect_inspections={}",
        broker.sent, broker.calls, broker.inspections
    );
    let cleanup = if let Some(source) = matched.as_mut() {
        source.source.discard_retained().await
    } else {
        Ok(())
    };
    drop(matched);
    drop(broker);
    cleanup?;
    if inner.private_file_volume_owned(&volume, &labels).await? {
        inner.remove_private_file_volume(&volume, &labels).await?;
    }
    drop(ControllerLease::acquire(&lane)?);
    let originals = runtime.owned.lock().unwrap().clone();
    for (h, _) in originals {
        assert!(inner.get_container_handle(&h.id).await?.is_none());
    }
    match outcome {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

mod gateway;

async fn qualify(gate: Gate) -> Result<()> {
    let image = std::env::var("ELIXIR_MARKETPLACE_APPLICATION_HTTP_IMAGE")?;
    if !image.strip_prefix("sha256:").is_some_and(immutable_id) {
        bail!("exact local native helper image required");
    }
    let root = tempfile::tempdir()?;
    let lane = root.path().canonicalize()?.join("global");
    let deployment = format!("http-life-{}", uuid::Uuid::new_v4().simple());
    let labels = HashMap::from([
        (ELIXIR_MANAGED_LABEL.into(), "true".into()),
        (ELIXIR_DEPLOYMENT_ID_LABEL.into(), deployment.clone()),
        ("elixir.marketplace.volume".into(), "quarantine-v1".into()),
    ]);
    let volume = format!("elixir-marketplace-source-{deployment}");
    let owned = Arc::new(Mutex::new(Vec::new()));
    let _emergency = Emergency {
        actors: owned.clone(),
        volume: volume.clone(),
        labels: labels.clone(),
    };
    let inner = Arc::new(DockerRuntimeManager::new(None));
    let runtime = Arc::new(Runtime {
        inner: inner.clone(),
        gate,
        lane: lane.clone(),
        journal: "http-helper.pending",
        reached: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        helper: Mutex::default(),
        starts: AtomicUsize::new(0),
        owned,
    });
    let mut broker = Scope {
        native: None,
        http: None,
        began: false,
        image: if gate == Gate::Rejected {
            let unsupported = std::env::var("ELIXIR_MARKETPLACE_UNSUPPORTED_HTTP_IMAGE")?;
            if !unsupported
                .strip_prefix("sha256:")
                .is_some_and(immutable_id)
                || unsupported == image
            {
                bail!("explicit different immutable unsupported helper image required");
            }
            unsupported
        } else {
            image.clone()
        },
        runtime: runtime.clone(),
    };
    let mut matched: Option<SourceMatchedQuarantine> = None;
    let outcome = std::panic::AssertUnwindSafe(async {
        let (release,package,proof) = crate::marketplace::quarantine::tests::operational_fixture().await?;
        // Source input reserves arguments + framing before any native start;
        // the small reception-only fixture's 1 MiB is not an invoke budget.
        let budget = SourceJobBudget { input_bytes:8*MIB, ..crate::marketplace::quarantine::tests::budget() };
        let plan = SourceQuarantinePlan::for_release(&release,None,&budget)?;
        let mut job = SourceQuarantineJob::open(runtime.clone(),&lane,&deployment,&image,&plan).await?;
        job.receive(runtime.as_ref(),QuarantinePurpose::Package,&mut package.as_slice()).await?;
        job.receive(runtime.as_ref(),QuarantinePurpose::Proof,&mut proof.as_slice()).await?;
        let source = job.finish(runtime.as_ref()).await?;
        matched = Some(source.match_source(runtime.as_ref()).await?);
        let limits = SourceExecutionLimits { profile:Default::default(),heap_bytes:16*MIB,timeout_millis:30_000,
            max_file_bytes:4*MIB,json_bytes:MIB,pending_jobs:128,sdk:None };
        let arguments = RuntimeJson::parse(b"null")?;
        let mut invoke = Box::pin(matched.as_mut().unwrap().evaluate_native(
            SourceExecutionMode::Invoke,limits,&arguments,&mut broker));
        timeout(Duration::from_secs(30),async {
            tokio::select! {
                result = &mut invoke => { result?; bail!("actual source finished before helper boundary"); },
                _ = runtime.reached.notified() => Ok::<_,anyhow::Error>(()),
            }
        }).await.context("actual source/helper boundary deadline")??;
        let helper = runtime.helper.lock().unwrap().clone();
        if gate==Gate::Rejected {
            assert!(helper.is_none(),"rejected image must never create a helper");
            runtime.phase("creating")?;
        } else {
            let h = helper.as_ref().context("actual helper CID")?;
            let actual = inner.inspect(&ContainerHandle{id:h.id.clone(),name:h.id.clone()}).await?;
            assert_eq!(actual.running,gate!=Gate::Created);
            assert_eq!(actual.status,if gate==Gate::Created {"created"}else{"running"});
        }
        drop(invoke); // Drops the ACTUAL NativeScope; no scope bit is synthesized.
        let native = broker.native.as_ref().context("actual inspected source scope")?;
        assert!(native.check().is_err());
        assert!(Fence::load(&native.lease)?.is_some());
        if gate != Gate::Ready {
            assert!(ControllerLease::acquire(&lane).is_err());
            assert!(Fence::load(&native.lease)?.unwrap().clear(&native.lease).is_err());
            assert!(timeout(Duration::from_millis(20),broker.http.as_mut().unwrap().cancel()).await.is_err());
        }
        runtime.release.notify_one();
        timeout(Duration::from_secs(40),broker.http.as_mut().unwrap().cancel()).await??;
        if let Some(helper) = helper { assert!(inner.get_container_handle(&helper.id).await?.is_none()); }
        assert!(!lane.join("http-helper.pending").exists());
        assert_eq!(runtime.starts.load(Ordering::SeqCst),usize::from(matches!(gate,Gate::Started|Gate::Ready|Gate::Attached)));
        assert!(ControllerLease::acquire(&lane).is_err(),"parent still owns the original lane");
        Ok::<_,anyhow::Error>(())
    }).catch_unwind().await;
    // Release artificial gates and join original owners even on failure.
    runtime.release.notify_one();
    let http_cleanup = if let Some(http) = broker.http.as_mut() {
        timeout(Duration::from_secs(40), http.cancel())
            .await
            .context("native fixture original helper cleanup deadline")
            .and_then(|result| result)
    } else {
        Ok(())
    };
    let source_cleanup = if let Some(source) = matched.as_mut() {
        source.source.discard_retained().await
    } else {
        Ok(())
    };
    drop(matched);
    drop(broker);
    let volume_cleanup = async {
        if inner.private_file_volume_owned(&volume, &labels).await? {
            inner.remove_private_file_volume(&volume, &labels).await?;
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    http_cleanup?;
    source_cleanup?;
    volume_cleanup?;
    let lease = ControllerLease::acquire(&lane)?;
    drop(lease);
    let originals = runtime.owned.lock().unwrap().clone();
    for (h, _) in originals {
        assert!(
            inner.get_container_handle(&h.id).await?.is_none(),
            "owned native fixture CID remains"
        );
    }
    match outcome {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
