//! TEST ONLY public-address alias inside an exact owned gateway namespace.
//! No production normalization/inspection/grants are replaced or synthesized.
use super::*;
use crate::marketplace::{
    application_egress::{
        ApplicationGatewayObservation, ApplicationGatewayProfile, GatewayCreateAttempt,
    },
    runtime::{SourceJobLimits, application_http::ApplicationHttpProfile},
};
use crate::runtime::{
    PrivateProcessStdio,
    measurement::MeasurementSpec,
    model::{
        ContainerRuntimeState, ContainerSpec, ContainerState, QuarantineMarketplaceVolumeSpec,
        SourceCacheState,
    },
};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
pub(super) struct Activity {
    pub gateways: AtomicUsize,
    pub http: AtomicUsize,
    pub buffered_http: AtomicUsize,
    pub streaming_http: AtomicUsize,
    pub source_matches: AtomicUsize,
    pub verified_invokes: AtomicUsize,
    pub image_lookups: AtomicUsize,
    times: std::sync::Mutex<std::collections::BTreeMap<&'static str, (u64, u128)>>,
}
impl Activity {
    fn record(&self, method: &'static str, started: std::time::Instant) {
        let mut times = self.times.lock().unwrap();
        // Only fixed compiled method names, no request data or growing history.
        if times.len() >= 64 && !times.contains_key(method) {
            return;
        }
        let entry = times.entry(method).or_default();
        entry.0 = entry.0.saturating_add(1);
        entry.1 = entry.1.saturating_add(started.elapsed().as_millis());
    }
    pub fn times(&self) -> std::collections::BTreeMap<&'static str, (u64, u128)> {
        self.times.lock().unwrap().clone()
    }
}

pub(super) fn runtime(
    inner: Arc<dyn RuntimeManager>,
    network: String,
    peer: String,
    owner: String,
    activity: Arc<Activity>,
    gateways: Arc<tokio::sync::Mutex<Vec<ContainerHandle>>>,
) -> Arc<dyn RuntimeManager> {
    Arc::new(ApplicationRuntime {
        inner,
        network,
        peer,
        owner,
        activity,
        gateways,
    })
}
struct ApplicationRuntime {
    inner: Arc<dyn RuntimeManager>,
    network: String,
    peer: String,
    owner: String,
    activity: Arc<Activity>,
    gateways: Arc<tokio::sync::Mutex<Vec<ContainerHandle>>>,
}
// Expand the entire impl before async_trait transforms it. Every forwarded
// method still executes the real native implementation and its actual checks.
macro_rules! forwarding {
    ($($name:ident($($argument:ident: $kind:ty),*) -> $result:ty;)*) => {
        #[async_trait::async_trait]
        impl RuntimeManager for ApplicationRuntime {
            async fn local_image_id(&self, reference: &str) -> Result<String> {
                self.activity.image_lookups.fetch_add(1, Ordering::SeqCst);
                let began = std::time::Instant::now();
                let result = self.inner.local_image_id(reference).await;
                self.activity.record("local_image_id", began);
                result
            }
            async fn start_application_gateway(&self, handle: &ContainerHandle, profile: &ApplicationGatewayProfile) -> Result<()> {
                let began = std::time::Instant::now();
                ensure!(profile.spec().network == self.network, "application fixture underlay changed");
                self.inner.start_application_gateway(handle, profile).await?;
                self.gateways.lock().await.push(handle.clone());
                // The production bridge deliberately has ICC disabled. Do not
                // weaken it merely to connect a fake VPN provider. As in the
                // original NetworkLab, use an owned point-to-point veth instead.
                // This fixed oneshot exits/removes itself BEFORE material can
                // start; it never becomes a persistent namespace holder/relay.
                let name = format!("elixir-marketplace-{}-app-link-{}", self.owner, &handle.id[..12]);
                let label = format!("{TEST_LABEL}={}", self.owner);
                let network = format!("container:{}", handle.id);
                let pid = format!("container:{}", self.peer);
                docker(&[
                    "run", "--rm", "--pull=never", "--name", &name, "--label", &label,
                    "--network", &network, "--pid", &pid, "--read-only", "--user=0:0",
                    "--cap-drop=ALL", "--cap-add=NET_ADMIN", "--security-opt=no-new-privileges",
                    "--memory=64m", "--memory-swap=64m", "--cpus=0.1", "--pids-limit=32",
                    "--log-driver=none", "--ulimit=core=0:0", "--entrypoint=/usr/bin/timeout",
                    TOOLS, "12", "/bin/sh", "-ec",
                    "ip link add app0 type veth peer name lab-app; ip link set lab-app netns 1; ip addr add 198.18.18.2/30 dev app0; ip link set app0 up",
                ], &[]).await?;
                self.inner.exec_container_command(&ContainerHandle {
                    id: self.peer.clone(), name: self.peer.clone(),
                }, "0:0", &[
                    "/usr/bin/timeout".into(), "5".into(), "/bin/sh".into(), "-ec".into(),
                    "ip addr add 198.18.18.1/30 dev lab-app; ip link set lab-app up".into(),
                ]).await?;
                // The exit owns the public endpoint alias on loopback. Route
                // to it unchanged over the owned link BEFORE Gluetun observes
                // its underlay. DNAT after route selection would leave its
                // endpoint firewall exception/source route inconsistent with
                // the actual destination. Neither the host nor the production
                // bridge's isolation is changed; material cannot race START.
                self.inner.exec_container_command(handle, "0:0", &[
                    "/bin/busybox".into(), "timeout".into(), "-s".into(), "KILL".into(), "5".into(),
                    "ip".into(), "route".into(), "replace".into(), "default".into(),
                    "via".into(), "198.18.18.1".into(), "dev".into(), "app0".into(),
                ]).await?;
                self.activity.gateways.fetch_add(1, Ordering::SeqCst);
                self.activity.record("start_application_gateway", began);
                Ok(())
            }
            async fn start_application_http(&self, handle: &ContainerHandle, profile: &ApplicationHttpProfile) -> Result<()> {
                let began = std::time::Instant::now();
                let counter = match profile.spec().command.as_slice() {
                    [mode] if mode == "--sdk-http" => &self.activity.buffered_http,
                    [mode] if mode == "--sdk-http-stream-large" => &self.activity.streaming_http,
                    _ => bail!("application fixture observed an unexpected helper mode"),
                };
                self.inner.start_application_http(handle, profile).await?;
                self.activity.http.fetch_add(1, Ordering::SeqCst);
                counter.fetch_add(1, Ordering::SeqCst);
                self.activity.record("start_application_http", began);
                // Test-only phase/cleanup evidence; no URL, credentials or
                // response data. This does not claim a successful HTTP request.
                eprintln!("actual application lab HTTP child started: {}", handle.id);
                Ok(())
            }
            async fn start_quarantine_marketplace_writer(&self, handle: &ContainerHandle, spec: &ContainerSpec, limits: &SourceJobLimits) -> Result<()> {
                let began = std::time::Instant::now();
                self.inner.start_quarantine_marketplace_writer(handle,spec,limits).await?;
                match spec.command.first().map(String::as_str) {
                    Some("--source-match") => {self.activity.source_matches.fetch_add(1,Ordering::SeqCst);},
                    Some("--source-eval-verified") => {self.activity.verified_invokes.fetch_add(1,Ordering::SeqCst);},
                    _ => {},
                }
                self.activity.record("start_quarantine_marketplace_writer",began);
                Ok(())
            }
            $(async fn $name(&self, $($argument: $kind),*) -> $result {
                let began = std::time::Instant::now();
                let result = self.inner.$name($($argument),*).await;
                self.activity.record(stringify!($name), began);
                result
            })*
        }
    }
}
forwarding! {
    image_labels(image: &str) -> Result<HashMap<String, String>>;
    create_application_gateway(profile: &ApplicationGatewayProfile, attempt: GatewayCreateAttempt) -> Result<crate::runtime::gateway_creation::GatewayCreation>;
    inspect_unstarted_application_gateway(profile: &ApplicationGatewayProfile) -> Result<ContainerHandle>;
    attach_application_gateway(handle: &ContainerHandle, profile: &ApplicationGatewayProfile) -> Result<PrivateProcessStdio>;
    observe_application_gateway(handle: &ContainerHandle, profile: &ApplicationGatewayProfile) -> Result<ApplicationGatewayObservation>;
    observe_unchanged_application_gateway(handle: &ContainerHandle, profile: &ApplicationGatewayProfile, original: &ApplicationGatewayObservation) -> Result<ApplicationGatewayObservation>;
    stop_application_gateway(handle: &ContainerHandle, profile: &ApplicationGatewayProfile) -> Result<()>;
    remove_application_gateway(handle: &ContainerHandle, profile: &ApplicationGatewayProfile) -> Result<()>;
    create_application_http(profile: &ApplicationHttpProfile, attempt: crate::marketplace::runtime::application_http::HttpCreateAttempt) -> Result<crate::runtime::http_creation::HttpCreation>;
    inspect_unstarted_application_http(profile: &ApplicationHttpProfile) -> Result<ContainerHandle>;
    attach_application_http(handle: &ContainerHandle, profile: &ApplicationHttpProfile) -> Result<PrivateProcessStdio>;
    confirm_application_http_success(handle: &ContainerHandle, profile: &ApplicationHttpProfile) -> Result<()>;
    stop_application_http(handle: &ContainerHandle, profile: &ApplicationHttpProfile) -> Result<()>;
    remove_application_http(handle: &ContainerHandle, profile: &ApplicationHttpProfile) -> Result<()>;
    create_host_measurement(spec: &MeasurementSpec) -> Result<ContainerHandle>;
    start_host_measurement(handle: &ContainerHandle, spec: &MeasurementSpec) -> Result<()>;
    confirm_host_measurement_success(handle: &ContainerHandle, spec: &MeasurementSpec) -> Result<()>;
    stop_host_measurement(handle: &ContainerHandle) -> Result<()>;
    ensure_quarantine_marketplace_volume(spec: &QuarantineMarketplaceVolumeSpec) -> Result<()>;
    create_quarantine_marketplace_writer(spec: &ContainerSpec) -> Result<ContainerHandle>;
    inspect_unstarted_quarantine_marketplace_writer(intent: &crate::marketplace::runtime::source_writer::SourceWriterIntent) -> Result<ContainerHandle>;
    stop_quarantine_marketplace_writer(handle: &ContainerHandle) -> Result<()>;
    source_cache_state(name: &str, labels: &HashMap<String, String>) -> Result<SourceCacheState>;
    attach_private_stdio(handle: &ContainerHandle, labels: &HashMap<String, String>) -> Result<PrivateProcessStdio>;
    confirm_quarantine_marketplace_success(handle: &ContainerHandle, labels: &HashMap<String, String>) -> Result<()>;
    private_file_volume_owned(name: &str, labels: &HashMap<String, String>) -> Result<bool>;
    remove_private_file_volume(name: &str, labels: &HashMap<String, String>) -> Result<()>;
    get_container_handle(name: &str) -> Result<Option<ContainerHandle>>;
    describe_container_runtime_state(name: &str) -> Result<Option<ContainerRuntimeState>>;
    exec_container_command(handle: &ContainerHandle, user: &str, command: &[String]) -> Result<String>;
    inspect(handle: &ContainerHandle) -> Result<ContainerState>;
    ensure_network(name: &str) -> Result<()>;
    ensure_container(spec: &ContainerSpec) -> Result<ContainerHandle>;
    start_container(handle: &ContainerHandle) -> Result<()>;
    stop_container(handle: &ContainerHandle) -> Result<()>;
    rename_container(handle: &ContainerHandle, name: &str) -> Result<ContainerHandle>;
    remove_container(handle: &ContainerHandle) -> Result<()>;
    container_logs(handle: &ContainerHandle, since: Option<chrono::DateTime<chrono::Utc>>) -> Result<String>;
    read_container_file(handle: &ContainerHandle, path: &str) -> Result<Option<Vec<u8>>>;
    copy_host_path_to_container(handle: &ContainerHandle, source: &Path, destination: &str) -> Result<()>;
    ensure_container_directories(handle: &ContainerHandle, paths: &[String]) -> Result<()>;
    ensure_container_directories_owned_like(handle: &ContainerHandle, reference: &str, paths: &[String]) -> Result<bool>;
}
