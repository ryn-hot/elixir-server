#[cfg(test)]
use std::collections::BTreeMap;
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    error::Error,
    fmt,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quick_xml::{Reader, events::Event};
use reqwest::{
    Client, Method, StatusCode, Url,
    header::{
        CONTENT_DISPOSITION, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue, LOCATION, REFERER,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value, json};
use sqlx::{AnyPool, Row};
use tokio::{
    fs,
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader},
    process::Command,
    time::MissedTickBehavior,
};
use uuid::Uuid;

use crate::{
    acquisition::{
        release_resolution::{
            models::{
                AcquisitionRelease, AcquisitionReleaseFile, AcquisitionReleaseJob,
                AcquisitionReleaseState, NewAcquisitionReleaseCoverage, NewAcquisitionReleaseFile,
                ReleaseConfidence, ReleaseCoverageKind, ReleaseCoverageState, ReleaseJobState,
                ReleaseJobStateUpdate,
            },
            store::{
                list_active_releases_by_route, list_release_jobs, update_release_job_state,
                upsert_release_coverage, upsert_release_file,
            },
        },
        stream_egress::{
            StreamEgressDecision, StreamHttpEgressPolicy, load_saved_stream_http_egress_policy,
        },
        subscriptions::{AcquisitionTarget, list_subscription_targets},
    },
    db::models::{MediaType, ProviderHealthState, SecretScope},
    download_broker::HTTP_STREAM_DEFAULT_LOGICAL_ID,
    extensions::store::{
        ExtensionStore, NewExtensionSourceHealthEvent, NewExtensionSourceModuleQuarantine,
    },
    http::handlers::{
        acquisition_sources::{
            ACQUISITION_STREAM_CANDIDATE_PROVIDER_CAPABILITY,
            candidate_provider_invocation_config_for_store, validate_safe_http_url,
            validate_stream_candidate_for_broker,
        },
        extensions::resolve_control_provider_transport_base_url,
    },
    media::ffprobe,
    network::protection::{
        ActiveManagedDownloaderRuntime, CloudflareWarpGatewayRuntime,
        DownloadProtectionCompileInput, DownloadProtectionProfile, GluetunOpenvpnGatewayRuntime,
        GluetunWireguardGatewayRuntime, active_managed_downloader_runtime,
    },
    orchestrator::model::ProviderEndpoint,
    runtime::{
        RuntimeManager, RuntimePaths,
        docker::DockerRuntimeManager,
        model::{
            ContainerSpec, VolumeMount, VolumeMountSourceKind, apply_container_spec_fingerprint,
        },
    },
    secrets::SecretsManager,
    state::AppState,
};

const STREAM_MATERIALIZER_INTERVAL_SECONDS: u64 = 3;
const STREAM_MATERIALIZER_BATCH_LIMIT: i64 = 50;
const STREAM_MATERIALIZER_DOWNLOAD_TIMEOUT_SECONDS: u64 = 30 * 60;
const STREAM_MATERIALIZER_REMUX_TIMEOUT_SECONDS: u64 = 8 * 60 * 60;
const STREAM_MATERIALIZER_MAX_REDIRECTS: usize = 5;
const STREAM_MATERIALIZER_PROGRESS_INTERVAL: Duration = Duration::from_secs(1);
const STREAM_MATERIALIZER_PROGRESS_BYTES: u64 = 1024 * 1024;
const STREAM_MATERIALIZER_VERSION: &str = "ess8-http-stream-materializer-v1";
const STREAM_CANDIDATE_PROVIDER_SCHEMA_VERSION: u32 = 1;
const STREAM_CANDIDATE_PROVIDER_RESOLVE_PATH: &str = "resolve";
const STREAM_CANDIDATE_RESOLVE_TIMEOUT_SECONDS: u64 = 30;
const STREAM_CANDIDATE_RESOLVE_RESPONSE_MAX_BYTES: u64 = 2 * 1024 * 1024;
const MAX_STREAM_FILE_NAME_LEN: usize = 180;
const FFMPEG_STDERR_TAIL_BYTES: usize = 16 * 1024;
const STREAM_MANIFEST_CLASSIFY_TIMEOUT_SECONDS: u64 = 30;
const STREAM_MANIFEST_CLASSIFY_MAX_BYTES: usize = 512 * 1024;
const STREAM_MANIFEST_CLASSIFY_MAX_REFERENCES: usize = 256;
const STREAM_MANIFEST_CLASSIFY_MAX_DEPTH: usize = 4;
const PROTECTED_STREAM_WORKER_IMAGE: &str = "elixir/http-stream-materializer-worker:0.1.0";
const PROTECTED_STREAM_WORKER_EXTENSION_ID: &str = "elixir.core.http-stream-materializer";
const PROTECTED_STREAM_WORKER_NETWORK: &str = "elixir_net";
const PROTECTED_STREAM_WORKER_MOUNT: &str = "/work";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HttpStreamMaterializerStats {
    pub scanned: usize,
    pub skipped: usize,
    pub claimed: usize,
    pub completed: usize,
    pub cancelled: usize,
    pub failed: usize,
    pub review_required: usize,
}

#[derive(Debug, Clone)]
struct MaterializerPaths {
    staging_root: PathBuf,
}

impl MaterializerPaths {
    fn from_state(state: &AppState) -> Self {
        let runtime_paths = RuntimePaths::from_roots(
            &state.settings.extensions.storage_root,
            &state.settings.library.local_root,
        );
        Self::from_downloads_root(PathBuf::from(runtime_paths.downloads_root))
    }

    fn from_downloads_root(downloads_root: PathBuf) -> Self {
        Self {
            staging_root: downloads_root.join("http-stream-materializer"),
        }
    }
}

#[derive(Debug, Clone)]
struct HttpStreamMaterializerConfig {
    paths: MaterializerPaths,
    batch_limit: i64,
}

impl HttpStreamMaterializerConfig {
    fn from_state(state: &AppState) -> Self {
        Self {
            paths: MaterializerPaths::from_state(state),
            batch_limit: STREAM_MATERIALIZER_BATCH_LIMIT,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StreamEgressRoute {
    policy: StreamHttpEgressPolicy,
    decision: StreamEgressDecision,
    initial_url_scheme: String,
    final_url_scheme: Option<String>,
    protected_profile_id: Option<String>,
    protected_runtime_kind: Option<String>,
    worker_runtime_id: Option<String>,
    reason: String,
    manifest_summary: Option<StreamManifestClassificationSummary>,
}

impl StreamEgressRoute {
    fn direct_https(
        policy: StreamHttpEgressPolicy,
        initial_url_scheme: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            policy,
            decision: StreamEgressDecision::DirectHttps,
            initial_url_scheme: initial_url_scheme.into(),
            final_url_scheme: None,
            protected_profile_id: None,
            protected_runtime_kind: None,
            worker_runtime_id: None,
            reason: reason.into(),
            manifest_summary: None,
        }
    }

    fn protected(
        policy: StreamHttpEgressPolicy,
        decision: StreamEgressDecision,
        initial_url_scheme: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            policy,
            decision,
            initial_url_scheme: initial_url_scheme.into(),
            final_url_scheme: None,
            protected_profile_id: None,
            protected_runtime_kind: None,
            worker_runtime_id: None,
            reason: reason.into(),
            manifest_summary: None,
        }
    }

    fn rejected(
        policy: StreamHttpEgressPolicy,
        initial_url_scheme: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            policy,
            decision: StreamEgressDecision::RejectedByPolicy,
            initial_url_scheme: initial_url_scheme.into(),
            final_url_scheme: None,
            protected_profile_id: None,
            protected_runtime_kind: None,
            worker_runtime_id: None,
            reason: reason.into(),
            manifest_summary: None,
        }
    }

    fn blocked(
        mut self,
        reason: impl Into<String>,
        profile_id: Option<String>,
        runtime_kind: Option<String>,
    ) -> Self {
        self.decision = StreamEgressDecision::BlockedProtectedEgressUnavailable;
        self.reason = reason.into();
        self.protected_profile_id = profile_id;
        self.protected_runtime_kind = runtime_kind;
        self
    }

    fn with_final_url(mut self, final_url: &str) -> Self {
        if let Ok(url) = Url::parse(final_url) {
            self.final_url_scheme = Some(url.scheme().to_string());
        }
        self
    }

    fn with_protected_runtime(
        mut self,
        profile_id: Option<String>,
        runtime_kind: Option<String>,
        worker_runtime_id: Option<String>,
    ) -> Self {
        self.protected_profile_id = profile_id;
        self.protected_runtime_kind = runtime_kind;
        self.worker_runtime_id = worker_runtime_id;
        self
    }

    fn requires_protected(&self) -> bool {
        matches!(
            self.decision,
            StreamEgressDecision::ProtectedHttp | StreamEgressDecision::ProtectedMixedManifest
        )
    }

    fn is_terminal_rejection(&self) -> bool {
        matches!(
            self.decision,
            StreamEgressDecision::BlockedProtectedEgressUnavailable
                | StreamEgressDecision::RejectedByPolicy
        )
    }

    fn route_label(&self) -> &'static str {
        self.decision.route_label()
    }

    fn runtime_json(&self) -> Value {
        json!({
            "policy": self.policy.as_str(),
            "decision": self.decision.as_str(),
            "routeLabel": self.route_label(),
            "initialUrlScheme": self.initial_url_scheme,
            "finalUrlScheme": self.final_url_scheme,
            "requiresProtected": self.requires_protected(),
            "protectedProfileId": self.protected_profile_id,
            "protectedRuntimeKind": self.protected_runtime_kind,
            "workerRuntimeId": self.worker_runtime_id,
            "reason": self.reason,
            "manifest": self.manifest_summary.as_ref().map(StreamManifestClassificationSummary::runtime_json),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StreamManifestClassificationSummary {
    inspected_manifests: usize,
    inspected_references: usize,
    http_component_kind: Option<String>,
    http_component_scheme: Option<String>,
}

impl StreamManifestClassificationSummary {
    fn runtime_json(&self) -> Value {
        json!({
            "inspectedManifests": self.inspected_manifests,
            "inspectedReferences": self.inspected_references,
            "httpComponentKind": self.http_component_kind,
            "httpComponentScheme": self.http_component_scheme,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StreamManifestClassification {
    requires_protected: bool,
    reason: String,
    summary: StreamManifestClassificationSummary,
}

#[derive(Debug, Clone)]
struct PendingDirectFileJob {
    release: AcquisitionRelease,
    job: AcquisitionReleaseJob,
    candidate: Value,
}

#[derive(Debug, Clone)]
struct DirectFileDownloadRequest {
    url: Url,
    headers: Vec<(String, String)>,
    referer: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamDeliveryType {
    DirectFile,
    Hls,
    Dash,
}

impl StreamDeliveryType {
    fn from_candidate(candidate: &Value) -> Option<Self> {
        match stream_candidate_string(candidate, "/delivery/streamType")?.as_str() {
            "direct_file" => Some(Self::DirectFile),
            "hls" => Some(Self::Hls),
            "dash" => Some(Self::Dash),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::DirectFile => "direct_file",
            Self::Hls => "hls",
            Self::Dash => "dash",
        }
    }

    fn materializer_label(self) -> &'static str {
        match self {
            Self::DirectFile => "Direct HTTP stream file",
            Self::Hls => "HLS stream",
            Self::Dash => "DASH stream",
        }
    }
}

struct DirectFileHttpResponse {
    final_url: String,
    content_length: Option<u64>,
    content_type: Option<String>,
    content_disposition: Option<String>,
    body: Box<dyn DirectFileBody + Send>,
}

#[async_trait]
trait DirectFileBody: Send {
    async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>>;
}

#[async_trait]
trait DirectFileHttpClient: Send + Sync {
    async fn open(&self, request: DirectFileDownloadRequest) -> Result<DirectFileHttpResponse>;
}

#[async_trait]
trait StreamEgressClassifier: Send + Sync {
    async fn classify_direct_file(
        &self,
        policy: StreamHttpEgressPolicy,
        request: &DirectFileDownloadRequest,
    ) -> Result<StreamEgressRoute>;

    async fn classify_remux_stream(
        &self,
        policy: StreamHttpEgressPolicy,
        request: &StreamRemuxRequest,
    ) -> Result<StreamEgressRoute>;
}

#[derive(Debug, Clone, Copy)]
struct ReqwestStreamEgressClassifier;

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
struct InitialSchemeStreamEgressClassifier;

#[derive(Debug, Clone)]
struct ProtectedDirectFileRequest {
    download: DirectFileDownloadRequest,
    partial_path: PathBuf,
}

#[derive(Debug, Clone, Default)]
struct ProtectedDirectFileResult {
    final_url: String,
    content_length: Option<u64>,
    content_type: Option<String>,
    content_disposition: Option<String>,
    downloaded_bytes: u64,
    worker_runtime_id: Option<String>,
    stderr_tail: Option<String>,
}

#[derive(Debug, Clone)]
struct ProtectedRemuxRequest {
    remux: StreamRemuxRequest,
}

#[derive(Debug, Clone, Default)]
struct ProtectedRemuxResult {
    remux: StreamRemuxResult,
    worker_runtime_id: Option<String>,
}

#[async_trait]
trait ProtectedStreamMaterializer: Send + Sync {
    async fn materialize_direct_file(
        &self,
        request: ProtectedDirectFileRequest,
    ) -> Result<ProtectedDirectFileResult>;

    async fn remux_stream(
        &self,
        request: ProtectedRemuxRequest,
        progress: &mut dyn StreamRemuxProgressSink,
    ) -> Result<ProtectedRemuxResult>;
}

#[derive(Debug, Clone, Copy)]
#[cfg(test)]
struct UnavailableProtectedStreamMaterializer;

#[derive(Clone)]
struct DockerProtectedStreamMaterializer {
    pool: AnyPool,
    secrets: Arc<SecretsManager>,
    runtime_paths: RuntimePaths,
    runtime: Arc<DockerRuntimeManager>,
    wireguard_gateway_image: String,
    worker_image: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProtectedWorkerRequestFile {
    mode: String,
    url: String,
    headers: Vec<ProtectedWorkerHeader>,
    #[serde(skip_serializing_if = "Option::is_none")]
    referer: Option<String>,
    partial_path: String,
    result_path: String,
    progress_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    duration_seconds: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProtectedWorkerHeader {
    name: String,
    value: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProtectedWorkerResultFile {
    success: bool,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    final_url: Option<String>,
    #[serde(default)]
    content_length: Option<u64>,
    #[serde(default)]
    content_type: Option<String>,
    #[serde(default)]
    content_disposition: Option<String>,
    #[serde(default)]
    output_bytes: Option<u64>,
    #[serde(default)]
    final_progress: Option<StreamRemuxProgress>,
    #[serde(default)]
    stderr_tail: Option<String>,
}

#[derive(Debug)]
struct ProtectedWorkerRunResult {
    runtime_id: String,
    result: ProtectedWorkerResultFile,
    logs_tail: Option<String>,
}

impl DockerProtectedStreamMaterializer {
    fn from_state(state: &AppState) -> Self {
        Self {
            pool: state.db_pool.clone(),
            secrets: state.secrets.clone(),
            runtime_paths: RuntimePaths::from_roots(
                &state.settings.extensions.storage_root,
                &state.settings.library.local_root,
            ),
            runtime: Arc::new(DockerRuntimeManager::new(None)),
            wireguard_gateway_image: state.settings.network.vpn.wireguard_gateway_image.clone(),
            worker_image: PROTECTED_STREAM_WORKER_IMAGE.to_string(),
        }
    }

    async fn run_worker(
        &self,
        host_partial_path: &Path,
        request: ProtectedWorkerRequestFile,
        timeout_duration: Duration,
    ) -> Result<ProtectedWorkerRunResult> {
        let job_dir = host_partial_path
            .parent()
            .ok_or_else(|| anyhow!("protected worker partial path has no parent directory"))?;
        fs::create_dir_all(job_dir).await.with_context(|| {
            format!("creating protected worker job dir '{}'", job_dir.display())
        })?;
        let runtime_id = Uuid::new_v4().to_string();
        let request_name = format!("worker-request-{runtime_id}.json");
        let result_name = format!("worker-result-{runtime_id}.json");
        let progress_name = format!("worker-progress-{runtime_id}.json");
        let request_path = job_dir.join(&request_name);
        let result_path = job_dir.join(&result_name);
        let progress_path = job_dir.join(&progress_name);
        let container_request = ProtectedWorkerRequestFile {
            result_path: format!("{PROTECTED_STREAM_WORKER_MOUNT}/{result_name}"),
            progress_path: format!("{PROTECTED_STREAM_WORKER_MOUNT}/{progress_name}"),
            ..request
        };
        let request_json = serde_json::to_vec_pretty(&container_request)
            .context("serializing protected stream worker request")?;
        fs::write(&request_path, request_json)
            .await
            .with_context(|| {
                format!(
                    "writing protected stream worker request '{}'",
                    request_path.display()
                )
            })?;

        let worker_name = format!("elixir-http-stream-worker-{}", Uuid::new_v4().simple());
        let (worker_spec, gateway_spec) = self
            .compile_protected_worker_specs(
                &worker_name,
                &runtime_id,
                job_dir,
                vec![
                    "--request".to_string(),
                    format!("{PROTECTED_STREAM_WORKER_MOUNT}/{request_name}"),
                ],
            )
            .await?;
        let mut worker_handle = None;
        let mut gateway_handle = None;
        let run_result = async {
            if let Some(gateway_spec) = gateway_spec.as_ref() {
                self.runtime.ensure_network(&gateway_spec.network).await?;
                let handle = self
                    .runtime
                    .ensure_container(gateway_spec)
                    .await
                    .with_context(|| {
                        format!(
                            "ensuring protected stream gateway container '{}'",
                            gateway_spec.name
                        )
                    })?;
                gateway_handle = Some(handle);
            }
            let handle = self
                .runtime
                .ensure_container(&worker_spec)
                .await
                .with_context(|| {
                    format!(
                        "starting protected stream worker container '{}'",
                        worker_spec.name
                    )
                })?;
            worker_handle = Some(handle.clone());
            let started = Instant::now();
            loop {
                let state = self.runtime.inspect(&handle).await.with_context(|| {
                    format!("inspecting protected stream worker '{worker_name}'")
                })?;
                if !state.running {
                    break;
                }
                if started.elapsed() > timeout_duration {
                    bail!("protected stream worker timed out");
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            let logs = self
                .runtime
                .container_logs(&handle, None)
                .await
                .ok()
                .map(|value| truncate_stream_diagnostic(&value, FFMPEG_STDERR_TAIL_BYTES));
            let bytes = fs::read(&result_path).await.with_context(|| {
                format!(
                    "reading protected stream worker result '{}'",
                    result_path.display()
                )
            })?;
            let result: ProtectedWorkerResultFile =
                serde_json::from_slice(&bytes).context("parsing protected stream worker result")?;
            if !result.success {
                bail!(
                    "protected stream worker reported failure: {}",
                    result
                        .error
                        .clone()
                        .unwrap_or_else(|| "unknown error".to_string())
                );
            }
            Ok::<ProtectedWorkerRunResult, anyhow::Error>(ProtectedWorkerRunResult {
                runtime_id: runtime_id.clone(),
                result,
                logs_tail: logs,
            })
        }
        .await;

        if let Some(handle) = worker_handle.as_ref() {
            let _ = self.runtime.remove_container(handle).await;
        }
        if let Some(handle) = gateway_handle.as_ref() {
            let _ = self.runtime.remove_container(handle).await;
        }
        let _ = fs::remove_file(request_path).await;
        let _ = fs::remove_file(result_path).await;
        let _ = fs::remove_file(progress_path).await;
        let _ = fs::remove_dir_all(self.worker_secret_runtime_root(&runtime_id)).await;
        run_result
    }

    async fn compile_protected_worker_specs(
        &self,
        worker_name: &str,
        runtime_id: &str,
        job_dir: &Path,
        command: Vec<String>,
    ) -> Result<(ContainerSpec, Option<ContainerSpec>)> {
        let mut labels = HashMap::new();
        labels.insert("elixir.instance_id".to_string(), runtime_id.to_string());
        labels.insert(
            "elixir.instance_name".to_string(),
            "HTTP stream materializer".to_string(),
        );
        labels.insert(
            "elixir.extension_id".to_string(),
            PROTECTED_STREAM_WORKER_EXTENSION_ID.to_string(),
        );
        labels.insert("elixir.managed".to_string(), "true".to_string());
        labels.insert(
            "elixir.network_role".to_string(),
            "http_stream_materializer".to_string(),
        );
        let mut app_spec = ContainerSpec {
            name: worker_name.to_string(),
            image: self.worker_image.clone(),
            network: PROTECTED_STREAM_WORKER_NETWORK.to_string(),
            network_mode: None,
            aliases: Vec::new(),
            env: Vec::new(),
            volumes: vec![VolumeMount {
                source_kind: VolumeMountSourceKind::Bind,
                host_path: job_dir.to_string_lossy().to_string(),
                container_path: PROTECTED_STREAM_WORKER_MOUNT.to_string(),
                read_only: false,
            }],
            ports: Vec::new(),
            labels: labels.clone(),
            command,
            cap_add: Vec::new(),
            devices: Vec::new(),
            sysctls: HashMap::new(),
        };
        apply_container_spec_fingerprint(&mut app_spec);

        let profile = self.active_protected_profile(runtime_id).await?;
        let compiled = profile.compile(DownloadProtectionCompileInput {
            app_container_name: worker_name,
            app_spec: &app_spec,
            base_labels: &labels,
        })?;
        Ok((compiled.protected_app_spec, compiled.gateway_spec))
    }

    async fn active_protected_profile(
        &self,
        runtime_id: &str,
    ) -> Result<DownloadProtectionProfile> {
        match active_managed_downloader_runtime(&self.pool).await? {
            ActiveManagedDownloaderRuntime::WireguardConfig {
                profile_id,
                secret_ref,
                gateway_image,
            } => {
                let config = resolve_stream_secret_value(
                    &ExtensionStore::new(&self.pool),
                    self.secrets.as_ref(),
                    Uuid::parse_str(runtime_id).unwrap_or_else(|_| Uuid::new_v4()),
                    &secret_ref,
                )
                .await?;
                let config_path = self
                    .write_worker_secret_file(runtime_id, "wireguard", "wg0.conf", &config)
                    .await?;
                Ok(DownloadProtectionProfile::wireguard_config(
                    profile_id,
                    "HTTP stream materializer WireGuard",
                    true,
                    GluetunWireguardGatewayRuntime {
                        image: gateway_image
                            .unwrap_or_else(|| self.wireguard_gateway_image.clone()),
                        config_host_path: config_path,
                    },
                ))
            }
            ActiveManagedDownloaderRuntime::OpenvpnConfig {
                profile_id,
                config_secret_ref,
                username_secret_ref,
                password_secret_ref,
                gateway_image,
            } => {
                let store = ExtensionStore::new(&self.pool);
                let instance_id = Uuid::parse_str(runtime_id).unwrap_or_else(|_| Uuid::new_v4());
                let config = resolve_stream_secret_value(
                    &store,
                    self.secrets.as_ref(),
                    instance_id,
                    &config_secret_ref,
                )
                .await?;
                let username = match username_secret_ref.as_deref() {
                    Some(secret_ref) => Some(
                        resolve_stream_secret_value(
                            &store,
                            self.secrets.as_ref(),
                            instance_id,
                            secret_ref,
                        )
                        .await?,
                    ),
                    None => None,
                };
                let password = match password_secret_ref.as_deref() {
                    Some(secret_ref) => Some(
                        resolve_stream_secret_value(
                            &store,
                            self.secrets.as_ref(),
                            instance_id,
                            secret_ref,
                        )
                        .await?,
                    ),
                    None => None,
                };
                if username.is_some() != password.is_some() {
                    bail!("OpenVPN protected stream profile has incomplete credentials");
                }
                let auth_path = if let (Some(username), Some(password)) = (username, password) {
                    Some(
                        self.write_worker_secret_file(
                            runtime_id,
                            "openvpn",
                            "auth.txt",
                            &format!("{}\n{}\n", username.trim(), password.trim()),
                        )
                        .await?,
                    )
                } else {
                    None
                };
                let rendered_config = render_stream_openvpn_config(&config, auth_path.is_some());
                let config_path = self
                    .write_worker_secret_file(
                        runtime_id,
                        "openvpn",
                        "custom.conf",
                        &rendered_config,
                    )
                    .await?;
                Ok(DownloadProtectionProfile::openvpn_config(
                    profile_id,
                    "HTTP stream materializer OpenVPN",
                    true,
                    GluetunOpenvpnGatewayRuntime {
                        image: gateway_image
                            .unwrap_or_else(|| self.wireguard_gateway_image.clone()),
                        config_host_path: config_path,
                        auth_host_path: auth_path,
                    },
                ))
            }
            ActiveManagedDownloaderRuntime::CloudflareWarp {
                profile_id,
                enrollment_id,
                identity_secret_ref,
                gateway_image,
                state_volume_name,
            } => Ok(DownloadProtectionProfile::cloudflare_warp(
                profile_id,
                "HTTP stream materializer WARP",
                true,
                CloudflareWarpGatewayRuntime {
                    image: gateway_image,
                    state_volume_name,
                    enrollment_id,
                    identity_secret_ref,
                },
            )),
            ActiveManagedDownloaderRuntime::Direct => {
                bail!("active stream egress profile is direct")
            }
            ActiveManagedDownloaderRuntime::NoStoredProfile => {
                bail!("no protected stream egress profile is configured")
            }
            ActiveManagedDownloaderRuntime::UnsupportedProtected { profile_id, kind } => {
                bail!(
                    "active stream egress profile '{}' uses unsupported runtime '{kind:?}'",
                    profile_id
                )
            }
        }
    }

    async fn write_worker_secret_file(
        &self,
        runtime_id: &str,
        scope: &str,
        name: &str,
        contents: &str,
    ) -> Result<String> {
        let root = self.worker_secret_runtime_root(runtime_id).join(scope);
        fs::create_dir_all(&root).await.with_context(|| {
            format!("creating protected stream secret dir '{}'", root.display())
        })?;
        let path = root.join(name);
        fs::write(&path, contents)
            .await
            .with_context(|| format!("writing protected stream secret '{}'", path.display()))?;
        set_stream_private_file_permissions(&path).await?;
        Ok(path.to_string_lossy().to_string())
    }

    fn worker_secret_runtime_root(&self, runtime_id: &str) -> PathBuf {
        Path::new(&self.runtime_paths.data_root)
            .join("http-stream-materializer")
            .join("protected-egress")
            .join(runtime_id)
    }
}

#[async_trait]
#[cfg(test)]
impl ProtectedStreamMaterializer for UnavailableProtectedStreamMaterializer {
    async fn materialize_direct_file(
        &self,
        _request: ProtectedDirectFileRequest,
    ) -> Result<ProtectedDirectFileResult> {
        bail!("protected stream materializer worker is unavailable")
    }

    async fn remux_stream(
        &self,
        _request: ProtectedRemuxRequest,
        _progress: &mut dyn StreamRemuxProgressSink,
    ) -> Result<ProtectedRemuxResult> {
        bail!("protected stream materializer worker is unavailable")
    }
}

#[async_trait]
impl ProtectedStreamMaterializer for DockerProtectedStreamMaterializer {
    async fn materialize_direct_file(
        &self,
        request: ProtectedDirectFileRequest,
    ) -> Result<ProtectedDirectFileResult> {
        let file_name = request
            .partial_path
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| anyhow!("protected direct-file partial path has no file name"))?;
        let worker_request = ProtectedWorkerRequestFile {
            mode: "direct_file".to_string(),
            url: request.download.url.to_string(),
            headers: request
                .download
                .headers
                .iter()
                .map(|(name, value)| ProtectedWorkerHeader {
                    name: name.clone(),
                    value: value.clone(),
                })
                .collect(),
            referer: request.download.referer.clone(),
            partial_path: format!("{PROTECTED_STREAM_WORKER_MOUNT}/{file_name}"),
            result_path: String::new(),
            progress_path: String::new(),
            stream_type: None,
            duration_seconds: None,
        };
        let run = self
            .run_worker(
                &request.partial_path,
                worker_request,
                Duration::from_secs(STREAM_MATERIALIZER_DOWNLOAD_TIMEOUT_SECONDS),
            )
            .await?;
        Ok(ProtectedDirectFileResult {
            final_url: run
                .result
                .final_url
                .unwrap_or_else(|| request.download.url.to_string()),
            content_length: run.result.content_length,
            content_type: run.result.content_type,
            content_disposition: run.result.content_disposition,
            downloaded_bytes: run.result.output_bytes.unwrap_or_default(),
            worker_runtime_id: Some(run.runtime_id),
            stderr_tail: run.result.stderr_tail.or(run.logs_tail),
        })
    }

    async fn remux_stream(
        &self,
        request: ProtectedRemuxRequest,
        progress: &mut dyn StreamRemuxProgressSink,
    ) -> Result<ProtectedRemuxResult> {
        let file_name = request
            .remux
            .partial_path
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| anyhow!("protected remux partial path has no file name"))?;
        let worker_request = ProtectedWorkerRequestFile {
            mode: "remux".to_string(),
            url: request.remux.url.to_string(),
            headers: request
                .remux
                .headers
                .iter()
                .map(|(name, value)| ProtectedWorkerHeader {
                    name: name.clone(),
                    value: value.clone(),
                })
                .collect(),
            referer: request.remux.referer.clone(),
            partial_path: format!("{PROTECTED_STREAM_WORKER_MOUNT}/{file_name}"),
            result_path: String::new(),
            progress_path: String::new(),
            stream_type: Some(request.remux.stream_type.as_str().to_string()),
            duration_seconds: request.remux.duration_seconds,
        };
        let run = self
            .run_worker(
                &request.remux.partial_path,
                worker_request,
                remux_timeout_duration(request.remux.duration_seconds),
            )
            .await?;
        if let Some(final_progress) = run.result.final_progress.clone() {
            if progress.observe(final_progress).await? == StreamRemuxControl::Cancel {
                bail!("protected stream remux was cancelled");
            }
        }
        Ok(ProtectedRemuxResult {
            remux: StreamRemuxResult {
                final_url: run
                    .result
                    .final_url
                    .or_else(|| Some(request.remux.url.to_string())),
                output_bytes: run.result.output_bytes.unwrap_or_default(),
                final_progress: run.result.final_progress,
                stderr_tail: run.result.stderr_tail.or(run.logs_tail),
            },
            worker_runtime_id: Some(run.runtime_id),
        })
    }
}

#[derive(Debug, Clone)]
struct StreamRemuxRequest {
    stream_type: StreamDeliveryType,
    url: Url,
    headers: Vec<(String, String)>,
    referer: Option<String>,
    partial_path: PathBuf,
    duration_seconds: Option<f64>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct StreamRemuxProgress {
    out_time_seconds: Option<f64>,
    out_time_raw: Option<u64>,
    speed: Option<String>,
    output_bytes: Option<u64>,
}

#[derive(Debug, Clone, Default)]
struct StreamRemuxResult {
    final_url: Option<String>,
    output_bytes: u64,
    final_progress: Option<StreamRemuxProgress>,
    stderr_tail: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamRemuxControl {
    Continue,
    Cancel,
}

#[async_trait]
trait StreamRemuxProgressSink: Send {
    async fn observe(&mut self, progress: StreamRemuxProgress) -> Result<StreamRemuxControl>;
}

#[async_trait]
trait StreamRemuxer: Send + Sync {
    async fn remux(
        &self,
        request: StreamRemuxRequest,
        progress: &mut dyn StreamRemuxProgressSink,
    ) -> Result<StreamRemuxResult>;
}

#[derive(Debug, Clone)]
struct ProbeEvidence {
    container: Option<String>,
    video_codec: Option<String>,
    audio_codec: Option<String>,
    width: Option<i32>,
    height: Option<i32>,
    duration_seconds: Option<i32>,
    streams: Vec<ProbeStreamEvidence>,
}

#[derive(Debug, Clone)]
struct ProbeStreamEvidence {
    index: Option<i32>,
    stream_type: Option<String>,
    codec: Option<String>,
    width: Option<i32>,
    height: Option<i32>,
    channels: Option<i32>,
    language: Option<String>,
    normalized_language: Option<String>,
    title: Option<String>,
    handler_name: Option<String>,
    default: bool,
    forced: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamVerificationState {
    Verified,
    ReviewRequired,
    Failed,
}

impl StreamVerificationState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Verified => "verified",
            Self::ReviewRequired => "review_required",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone)]
struct StreamVerificationDecision {
    state: StreamVerificationState,
    mismatch_class: Option<String>,
    reason: String,
    evidence: Value,
}

#[async_trait]
trait StreamFileProbe: Send + Sync {
    async fn probe(&self, path: &Path) -> Result<ProbeEvidence>;
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StreamCandidateResolveResult {
    #[serde(default)]
    delivery: Option<Value>,
    #[serde(default)]
    candidate: Option<Value>,
    #[serde(default)]
    warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StreamCandidateResolveInvocation<'a> {
    schema_version: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    candidate_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    resolve_handle: Option<&'a str>,
    candidate: &'a Value,
    provider: StreamCandidateResolveProviderContext<'a>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StreamCandidateResolveProviderContext<'a> {
    provider_id: Uuid,
    extension_id: &'a str,
    instance_id: Uuid,
    implementation: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    config: Option<Value>,
}

#[async_trait]
trait StreamCandidateLateResolver: Send + Sync {
    async fn resolve(
        &self,
        pool: &AnyPool,
        pending: &PendingDirectFileJob,
    ) -> Result<StreamCandidateResolveResult>;
}

#[derive(Debug, Clone, Default)]
struct ProviderStreamCandidateLateResolver {
    #[cfg(test)]
    base_urls: BTreeMap<Uuid, String>,
}

#[cfg(test)]
impl ProviderStreamCandidateLateResolver {
    fn with_base_url(provider_id: Uuid, base_url: String) -> Self {
        Self {
            base_urls: BTreeMap::from([(provider_id, base_url)]),
        }
    }
}

#[async_trait]
impl StreamCandidateLateResolver for ProviderStreamCandidateLateResolver {
    async fn resolve(
        &self,
        pool: &AnyPool,
        pending: &PendingDirectFileJob,
    ) -> Result<StreamCandidateResolveResult> {
        let provider_id = stream_candidate_source_provider_id(&pending.candidate)
            .or(pending.release.source_provider_id)
            .ok_or_else(|| {
                anyhow!("stream candidate is missing source provider id for late resolve")
            })?;
        let store = ExtensionStore::new(pool);
        let provider = store
            .get_provider(provider_id)
            .await?
            .ok_or_else(|| anyhow!("stream candidate provider '{provider_id}' is not installed"))?;
        if !provider
            .capability
            .eq_ignore_ascii_case(ACQUISITION_STREAM_CANDIDATE_PROVIDER_CAPABILITY)
        {
            bail!(
                "provider '{provider_id}' is not a stream candidate provider: {}",
                provider.capability
            );
        }
        if provider.health_state != ProviderHealthState::Healthy {
            bail!(
                "stream candidate provider '{provider_id}' is not healthy: {}",
                provider.health_state.as_str()
            );
        }
        let instance = store
            .get_instance(provider.instance_id)
            .await?
            .ok_or_else(|| anyhow!("stream candidate provider instance is missing"))?;
        if !instance.enabled {
            bail!("stream candidate provider instance is disabled");
        }
        let extension = store
            .get_extension(&instance.extension_id)
            .await?
            .ok_or_else(|| anyhow!("stream candidate provider extension is missing"))?;
        if !extension.enabled {
            bail!("stream candidate provider extension is disabled");
        }

        #[cfg(test)]
        let base_url = if let Some(base_url) = self.base_urls.get(&provider_id) {
            base_url.clone()
        } else {
            let endpoint_json = provider
                .endpoint_json
                .clone()
                .ok_or_else(|| anyhow!("stream candidate provider endpoint is missing"))?;
            let endpoint: ProviderEndpoint = serde_json::from_value(endpoint_json)
                .context("parsing stream candidate provider endpoint")?;
            resolve_control_provider_transport_base_url(instance.instance_id, &endpoint).await?
        };
        #[cfg(not(test))]
        let base_url = {
            let endpoint_json = provider
                .endpoint_json
                .clone()
                .ok_or_else(|| anyhow!("stream candidate provider endpoint is missing"))?;
            let endpoint: ProviderEndpoint = serde_json::from_value(endpoint_json)
                .context("parsing stream candidate provider endpoint")?;
            resolve_control_provider_transport_base_url(instance.instance_id, &endpoint).await?
        };
        let resolve_url = stream_candidate_provider_resolve_url(&base_url)?;
        let provider_config =
            candidate_provider_invocation_config_for_store(&store, &extension, &instance).await?;
        let candidate_id = stream_candidate_string(&pending.candidate, "/id");
        let resolve_handle = stream_candidate_string(&pending.candidate, "/delivery/resolveHandle");
        if candidate_id.is_none() && resolve_handle.is_none() {
            bail!("stream candidate has neither id nor delivery.resolveHandle for late resolve");
        }
        let invocation = StreamCandidateResolveInvocation {
            schema_version: STREAM_CANDIDATE_PROVIDER_SCHEMA_VERSION,
            candidate_id: candidate_id.as_deref(),
            resolve_handle: resolve_handle.as_deref(),
            candidate: &pending.candidate,
            provider: StreamCandidateResolveProviderContext {
                provider_id,
                extension_id: &extension.extension_id,
                instance_id: instance.instance_id,
                implementation: provider.implementation.as_deref(),
                config: provider_config,
            },
        };
        let client = Client::builder()
            .timeout(Duration::from_secs(
                STREAM_CANDIDATE_RESOLVE_TIMEOUT_SECONDS,
            ))
            .build()
            .context("building stream candidate resolve client")?;
        let response = client
            .post(resolve_url.clone())
            .json(&invocation)
            .send()
            .await
            .with_context(|| {
                format!("calling stream candidate provider resolve at {resolve_url}")
            })?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            bail!(
                "stream candidate provider resolve returned {status}: {}",
                truncate_stream_diagnostic(&body, 1024)
            );
        }
        parse_bounded_stream_candidate_resolve_response(response).await
    }
}

#[derive(Debug, Clone, Copy)]
enum DirectFileMaterializationOutcome {
    Completed,
    ReviewRequired,
    Cancelled,
    Failed,
}

enum StreamCandidatePreparation {
    Ready(PendingDirectFileJob),
    Failed,
}

pub async fn start_http_stream_materializer_loop(state: AppState) {
    let mut interval =
        tokio::time::interval(Duration::from_secs(STREAM_MATERIALIZER_INTERVAL_SECONDS));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let downloader = ReqwestDirectFileClient;
    let remuxer = FfmpegStreamRemuxer;
    let egress_classifier = ReqwestStreamEgressClassifier;
    let protected_materializer = DockerProtectedStreamMaterializer::from_state(&state);
    let probe = FfprobeStreamFileProbe;
    let resolver = ProviderStreamCandidateLateResolver::default();
    let config = HttpStreamMaterializerConfig::from_state(&state);

    loop {
        interval.tick().await;
        if let Err(err) = process_http_stream_materializer_once_with_all_services(
            &state.db_pool,
            &config,
            &downloader,
            &remuxer,
            &egress_classifier,
            &protected_materializer,
            &probe,
            &resolver,
        )
        .await
        {
            tracing::warn!("HTTP stream materializer pass failed: {err}");
        }
    }
}

#[allow(dead_code)]
pub async fn process_http_stream_materializer_once(
    state: &AppState,
) -> Result<HttpStreamMaterializerStats> {
    let downloader = ReqwestDirectFileClient;
    let remuxer = FfmpegStreamRemuxer;
    let egress_classifier = ReqwestStreamEgressClassifier;
    let protected_materializer = DockerProtectedStreamMaterializer::from_state(state);
    let probe = FfprobeStreamFileProbe;
    let resolver = ProviderStreamCandidateLateResolver::default();
    process_http_stream_materializer_once_with_all_services(
        &state.db_pool,
        &HttpStreamMaterializerConfig::from_state(state),
        &downloader,
        &remuxer,
        &egress_classifier,
        &protected_materializer,
        &probe,
        &resolver,
    )
    .await
}

#[cfg(test)]
async fn process_http_stream_materializer_once_with_services(
    pool: &AnyPool,
    config: &HttpStreamMaterializerConfig,
    downloader: &dyn DirectFileHttpClient,
    remuxer: &dyn StreamRemuxer,
    probe: &dyn StreamFileProbe,
    late_resolver: &dyn StreamCandidateLateResolver,
) -> Result<HttpStreamMaterializerStats> {
    let egress_classifier = InitialSchemeStreamEgressClassifier;
    let protected_materializer = UnavailableProtectedStreamMaterializer;
    process_http_stream_materializer_once_with_all_services(
        pool,
        config,
        downloader,
        remuxer,
        &egress_classifier,
        &protected_materializer,
        probe,
        late_resolver,
    )
    .await
}

async fn process_http_stream_materializer_once_with_all_services(
    pool: &AnyPool,
    config: &HttpStreamMaterializerConfig,
    downloader: &dyn DirectFileHttpClient,
    remuxer: &dyn StreamRemuxer,
    egress_classifier: &dyn StreamEgressClassifier,
    protected_materializer: &dyn ProtectedStreamMaterializer,
    probe: &dyn StreamFileProbe,
    late_resolver: &dyn StreamCandidateLateResolver,
) -> Result<HttpStreamMaterializerStats> {
    fs::create_dir_all(&config.paths.staging_root)
        .await
        .with_context(|| {
            format!(
                "creating HTTP stream materializer root '{}'",
                config.paths.staging_root.display()
            )
        })?;

    let store = ExtensionStore::new(pool);
    let egress_policy = load_saved_stream_http_egress_policy(&store)
        .await
        .context("loading stream HTTP egress policy")?;
    let pending = list_pending_direct_file_jobs(pool, config.batch_limit).await?;
    let mut stats = HttpStreamMaterializerStats {
        scanned: pending.len(),
        ..HttpStreamMaterializerStats::default()
    };

    for pending_job in pending {
        let Some(job) = claim_direct_file_job(pool, &pending_job.job).await? else {
            stats.skipped += 1;
            continue;
        };
        stats.claimed += 1;
        let pending_job = PendingDirectFileJob { job, ..pending_job };
        let pending_job =
            match refresh_stream_candidate_if_needed(pool, late_resolver, &pending_job).await? {
                StreamCandidatePreparation::Ready(pending_job) => pending_job,
                StreamCandidatePreparation::Failed => {
                    stats.failed += 1;
                    continue;
                }
            };
        let outcome = match StreamDeliveryType::from_candidate(&pending_job.candidate) {
            Some(StreamDeliveryType::DirectFile) => {
                materialize_direct_file_job(
                    pool,
                    config,
                    downloader,
                    egress_classifier,
                    protected_materializer,
                    probe,
                    &pending_job,
                    egress_policy,
                )
                .await?
            }
            Some(StreamDeliveryType::Hls | StreamDeliveryType::Dash) => {
                materialize_remux_stream_job(
                    pool,
                    config,
                    remuxer,
                    egress_classifier,
                    protected_materializer,
                    probe,
                    &pending_job,
                    egress_policy,
                )
                .await?
            }
            None => {
                stats.skipped += 1;
                continue;
            }
        };
        match outcome {
            DirectFileMaterializationOutcome::Completed => stats.completed += 1,
            DirectFileMaterializationOutcome::ReviewRequired => stats.review_required += 1,
            DirectFileMaterializationOutcome::Cancelled => stats.cancelled += 1,
            DirectFileMaterializationOutcome::Failed => stats.failed += 1,
        }
    }

    Ok(stats)
}

async fn list_pending_direct_file_jobs(
    pool: &AnyPool,
    limit: i64,
) -> Result<Vec<PendingDirectFileJob>> {
    let releases = list_active_releases_by_route(pool, HTTP_STREAM_DEFAULT_LOGICAL_ID, limit)
        .await
        .context("listing active HTTP stream releases")?;
    let mut pending = Vec::new();
    let mut seen_jobs = HashSet::new();

    for release in releases {
        let Some(candidate) = release.selected_candidate.clone() else {
            continue;
        };
        if StreamDeliveryType::from_candidate(&candidate).is_none() {
            continue;
        }
        if !stream_candidate_has_materializable_or_resolvable_delivery(&candidate) {
            continue;
        }
        let jobs = list_release_jobs(pool, release.release_id)
            .await
            .context("listing HTTP stream release jobs")?;
        for job in jobs {
            if job.route_logical_id == HTTP_STREAM_DEFAULT_LOGICAL_ID
                && job.active
                && matches!(
                    job.state,
                    ReleaseJobState::Submitted
                        | ReleaseJobState::Downloading
                        | ReleaseJobState::Materializing
                )
                && seen_jobs.insert(job.release_job_id)
            {
                pending.push(PendingDirectFileJob {
                    release: release.clone(),
                    job,
                    candidate: candidate.clone(),
                });
                break;
            }
        }
    }
    Ok(pending)
}

async fn refresh_stream_candidate_if_needed(
    pool: &AnyPool,
    late_resolver: &dyn StreamCandidateLateResolver,
    pending: &PendingDirectFileJob,
) -> Result<StreamCandidatePreparation> {
    if !stream_candidate_needs_late_resolve(&pending.candidate) {
        return Ok(StreamCandidatePreparation::Ready(pending.clone()));
    }
    let stream_type = StreamDeliveryType::from_candidate(&pending.candidate)
        .ok_or_else(|| anyhow!("stream candidate is missing delivery.streamType"))?;
    let late_resolve_reason = stream_candidate_late_resolve_reason(&pending.candidate);
    let late_resolve_provider_id = stream_candidate_source_provider_id(&pending.candidate)
        .or(pending.release.source_provider_id);
    let late_resolve_candidate_id = stream_candidate_string(&pending.candidate, "/id");
    let late_resolve_has_handle =
        stream_candidate_string(&pending.candidate, "/delivery/resolveHandle").is_some();
    let runtime = merge_runtime_object(
        base_stream_runtime(pending, stream_type, None, Vec::new(), false)?,
        json!({
            "runtimeState": "resolving",
            "lateResolve": {
                "required": true,
                "reason": late_resolve_reason,
                "providerId": late_resolve_provider_id.map(|id| id.to_string()),
                "candidateId": late_resolve_candidate_id.clone(),
                "hasResolveHandle": late_resolve_has_handle,
                "startedAt": Utc::now()
            }
        }),
    );
    update_stream_runtime(
        pool,
        &pending.release,
        &pending.job,
        AcquisitionReleaseState::Materializing,
        ReleaseJobState::Materializing,
        "Resolving HTTP stream delivery before materialization.",
        true,
        None,
        runtime.clone(),
    )
    .await?;

    let resolved = match late_resolver.resolve(pool, pending).await {
        Ok(resolved) => resolved,
        Err(err) => {
            fail_stream_job(
                pool,
                pending,
                &runtime,
                "late_resolve_failed",
                &format!("HTTP stream late resolve failed: {err}"),
                None,
            )
            .await?;
            return Ok(StreamCandidatePreparation::Failed);
        }
    };
    let (candidate, warnings) =
        match merge_late_resolved_stream_candidate(&pending.candidate, resolved) {
            Ok(candidate) => candidate,
            Err(err) => {
                fail_stream_job(
                    pool,
                    pending,
                    &runtime,
                    "late_resolve_invalid_delivery",
                    &format!("HTTP stream late resolve returned invalid delivery: {err}"),
                    None,
                )
                .await?;
                return Ok(StreamCandidatePreparation::Failed);
            }
        };
    let resolved_runtime = merge_runtime_object(
        runtime,
        json!({
            "runtimeState": "resolved",
            "sourceUrl": stream_candidate_string(&candidate, "/delivery/url")
                .map(|value| runtime_safe_stream_url(&value)),
            "headerNames": stream_candidate_header_names(&candidate),
            "refererApplied": stream_candidate_string(&candidate, "/delivery/referer").is_some(),
            "lateResolve": {
                "required": true,
                "reason": late_resolve_reason,
                "providerId": late_resolve_provider_id.map(|id| id.to_string()),
                "candidateId": late_resolve_candidate_id.clone(),
                "hasResolveHandle": late_resolve_has_handle,
                "warnings": warnings,
                "resolvedAt": Utc::now()
            }
        }),
    );
    update_stream_runtime(
        pool,
        &pending.release,
        &pending.job,
        AcquisitionReleaseState::Materializing,
        ReleaseJobState::Materializing,
        "HTTP stream delivery was resolved and validated.",
        true,
        None,
        resolved_runtime.clone(),
    )
    .await?;
    persist_resolved_stream_candidate(pool, pending.release.release_id, &candidate).await?;
    let mut release = pending.release.clone();
    release.coverage_plan = Some(merge_http_stream_runtime_evidence(
        pending.release.coverage_plan.clone(),
        resolved_runtime,
    ));
    let pending = PendingDirectFileJob {
        release,
        candidate: candidate.clone(),
        ..pending.clone()
    };
    Ok(StreamCandidatePreparation::Ready(pending))
}

async fn claim_direct_file_job(
    pool: &AnyPool,
    job: &AcquisitionReleaseJob,
) -> Result<Option<AcquisitionReleaseJob>> {
    let result = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_release_jobs
         SET state = ?,
             state_reason = ?,
             active = 1,
             started_at = COALESCE(started_at, CURRENT_TIMESTAMP),
             updated_at = CURRENT_TIMESTAMP
         WHERE release_job_id = ?
           AND active = 1
           AND state IN (?, ?, ?)",
    )
    .bind(ReleaseJobState::Materializing.as_str())
    .bind("Direct HTTP stream file is materializing through Elixir.".to_string())
    .bind(job.release_job_id.to_string())
    .bind(ReleaseJobState::Submitted.as_str())
    .bind(ReleaseJobState::Downloading.as_str())
    .bind(ReleaseJobState::Materializing.as_str())
    .execute(pool)
    .await
    .context("claiming HTTP stream materializer job")?;
    if result.rows_affected() == 0 {
        return Ok(None);
    }
    let jobs = list_release_jobs(pool, job.release_id).await?;
    Ok(jobs
        .into_iter()
        .find(|candidate| candidate.release_job_id == job.release_job_id))
}

async fn materialize_direct_file_job(
    pool: &AnyPool,
    config: &HttpStreamMaterializerConfig,
    downloader: &dyn DirectFileHttpClient,
    egress_classifier: &dyn StreamEgressClassifier,
    protected_materializer: &dyn ProtectedStreamMaterializer,
    probe: &dyn StreamFileProbe,
    pending: &PendingDirectFileJob,
    egress_policy: StreamHttpEgressPolicy,
) -> Result<DirectFileMaterializationOutcome> {
    let download_id = pending
        .job
        .download_id
        .as_deref()
        .or(pending.release.download_id.as_deref())
        .ok_or_else(|| anyhow!("HTTP stream job is missing download id"))?
        .to_string();
    let request = direct_file_download_request(&pending.candidate)?;
    let mut egress_route = egress_classifier
        .classify_direct_file(egress_policy, &request)
        .await
        .context("classifying direct-file stream egress")?;
    let header_names = request
        .headers
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();
    let referer_applied = request.referer.is_some();
    let started_at = Utc::now();
    let base_runtime = merge_runtime_object(
        existing_stream_runtime(pending),
        json!({
            "runtimeVersion": STREAM_MATERIALIZER_VERSION,
            "runtimeState": "materializing",
            "streamType": "direct_file",
            "downloadId": download_id,
            "sourceUrl": runtime_safe_stream_url(request.url.as_str()),
            "redirectPolicy": {
                "maxRedirects": STREAM_MATERIALIZER_MAX_REDIRECTS,
                "validated": true
            },
            "headerNames": header_names,
            "refererApplied": referer_applied,
            "routeLabel": egress_route.route_label(),
            "egress": egress_route.runtime_json(),
            "startedAt": started_at,
        }),
    );
    update_stream_runtime(
        pool,
        &pending.release,
        &pending.job,
        AcquisitionReleaseState::Materializing,
        ReleaseJobState::Materializing,
        "Direct HTTP stream file is materializing through Elixir.",
        true,
        None,
        base_runtime.clone(),
    )
    .await?;

    if egress_route.is_terminal_rejection() {
        fail_stream_job(
            pool,
            pending,
            &base_runtime,
            stream_egress_failure_class(&egress_route),
            &egress_route.reason,
            None,
        )
        .await?;
        return Ok(DirectFileMaterializationOutcome::Failed);
    }

    if egress_route.requires_protected() {
        return materialize_direct_file_job_via_protected_egress(
            pool,
            config,
            protected_materializer,
            probe,
            pending,
            request,
            egress_route,
            base_runtime,
        )
        .await;
    }

    let mut response = match downloader.open(request).await {
        Ok(response) => response,
        Err(err) => {
            if err.downcast_ref::<HttpStreamDowngradeRedirect>().is_some() {
                egress_route = StreamEgressRoute::protected(
                    egress_policy,
                    StreamEgressDecision::ProtectedHttp,
                    "https",
                    "direct-file HTTPS request redirects to HTTP",
                );
                let base_runtime = merge_runtime_object(
                    base_runtime,
                    json!({
                        "routeLabel": egress_route.route_label(),
                        "egress": egress_route.runtime_json(),
                    }),
                );
                if egress_policy == StreamHttpEgressPolicy::DirectOnly {
                    let rejected = StreamEgressRoute::rejected(
                        egress_policy,
                        "https",
                        "stream egress policy rejects HTTP redirect target",
                    );
                    let runtime = merge_runtime_object(
                        base_runtime,
                        json!({
                            "routeLabel": rejected.route_label(),
                            "egress": rejected.runtime_json(),
                        }),
                    );
                    fail_stream_job(
                        pool,
                        pending,
                        &runtime,
                        stream_egress_failure_class(&rejected),
                        &rejected.reason,
                        None,
                    )
                    .await?;
                    return Ok(DirectFileMaterializationOutcome::Failed);
                }
                return materialize_direct_file_job_via_protected_egress(
                    pool,
                    config,
                    protected_materializer,
                    probe,
                    pending,
                    direct_file_download_request(&pending.candidate)?,
                    egress_route,
                    base_runtime,
                )
                .await;
            }
            fail_stream_job(
                pool,
                pending,
                &base_runtime,
                "request_failed",
                &format!("Direct HTTP stream request failed: {err}"),
                None,
            )
            .await?;
            return Ok(DirectFileMaterializationOutcome::Failed);
        }
    };

    let file_name = choose_direct_file_name(&pending.candidate, &response);
    let target_dir = config
        .paths
        .staging_root
        .join(media_type_segment(pending.release.media_type))
        .join(safe_path_segment(&download_id));
    fs::create_dir_all(&target_dir)
        .await
        .with_context(|| format!("creating stream target dir '{}'", target_dir.display()))?;
    let target_path = unique_target_path(&target_dir, &file_name).await;
    let partial_path = target_path.with_extension(format!(
        "{}elixir-part",
        target_path
            .extension()
            .and_then(|value| value.to_str())
            .map(|value| format!("{value}."))
            .unwrap_or_default()
    ));

    let response_final_url = response.final_url.clone();
    let response_final_url_evidence = runtime_safe_stream_url(&response_final_url);
    let response_content_type = response.content_type.clone();
    let response_content_length = response.content_length;
    let mut runtime = merge_runtime_object(
        base_runtime.clone(),
        json!({
            "runtimeState": "downloading",
            "finalUrl": response_final_url_evidence,
            "egress": egress_route.clone().with_final_url(&response_final_url).runtime_json(),
            "contentType": response_content_type,
            "contentLength": response_content_length,
            "localPath": target_path.to_string_lossy(),
            "partialPath": partial_path.to_string_lossy(),
            "downloadedBytes": 0,
            "totalBytes": response_content_length,
            "progress": 0.0,
            "downloadRateBps": 0
        }),
    );
    if let Some(reason) = direct_file_non_media_response_reason(&response) {
        fail_stream_job(
            pool,
            pending,
            &runtime,
            "source_returned_non_media_response",
            &reason,
            None,
        )
        .await?;
        return Ok(DirectFileMaterializationOutcome::Failed);
    }
    update_stream_runtime(
        pool,
        &pending.release,
        &pending.job,
        AcquisitionReleaseState::Downloading,
        ReleaseJobState::Downloading,
        "Downloading direct HTTP stream file.",
        true,
        None,
        runtime.clone(),
    )
    .await?;

    let mut file = fs::File::create(&partial_path)
        .await
        .with_context(|| format!("creating stream partial '{}'", partial_path.display()))?;
    let mut downloaded = 0_u64;
    let mut last_update = Instant::now();
    let mut last_downloaded = 0_u64;

    loop {
        if job_cancelled(pool, pending.job.release_job_id).await? {
            drop(file);
            cleanup_partial(&partial_path, &config.paths.staging_root).await;
            mark_stream_cancelled(pool, pending, &runtime, &partial_path).await?;
            return Ok(DirectFileMaterializationOutcome::Cancelled);
        }
        let chunk = match response.body.next_chunk().await {
            Ok(chunk) => chunk,
            Err(err) => {
                drop(file);
                cleanup_partial(&partial_path, &config.paths.staging_root).await;
                fail_stream_job(
                    pool,
                    pending,
                    &runtime,
                    "read_failed",
                    &format!("Direct HTTP stream read failed: {err}"),
                    Some(&partial_path),
                )
                .await?;
                return Ok(DirectFileMaterializationOutcome::Failed);
            }
        };
        let Some(chunk) = chunk else {
            break;
        };
        if job_cancelled(pool, pending.job.release_job_id).await? {
            drop(file);
            cleanup_partial(&partial_path, &config.paths.staging_root).await;
            mark_stream_cancelled(pool, pending, &runtime, &partial_path).await?;
            return Ok(DirectFileMaterializationOutcome::Cancelled);
        }
        file.write_all(&chunk)
            .await
            .with_context(|| format!("writing stream partial '{}'", partial_path.display()))?;
        downloaded = downloaded.saturating_add(chunk.len() as u64);
        if should_persist_progress(downloaded, last_downloaded, last_update) {
            let elapsed = last_update.elapsed().as_secs_f64().max(0.001);
            let rate = ((downloaded.saturating_sub(last_downloaded)) as f64 / elapsed) as u64;
            runtime = merge_runtime_object(
                runtime,
                json!({
                    "runtimeState": "downloading",
                    "downloadedBytes": downloaded,
                    "totalBytes": response.content_length,
                    "progress": progress_fraction(downloaded, response.content_length),
                    "downloadRateBps": rate,
                    "updatedAt": Utc::now()
                }),
            );
            update_stream_runtime(
                pool,
                &pending.release,
                &pending.job,
                AcquisitionReleaseState::Downloading,
                ReleaseJobState::Downloading,
                "Downloading direct HTTP stream file.",
                true,
                None,
                runtime.clone(),
            )
            .await?;
            last_update = Instant::now();
            last_downloaded = downloaded;
        }
    }

    file.flush()
        .await
        .with_context(|| format!("flushing stream partial '{}'", partial_path.display()))?;
    drop(file);
    fs::rename(&partial_path, &target_path)
        .await
        .with_context(|| {
            format!(
                "moving stream partial '{}' to '{}'",
                partial_path.display(),
                target_path.display()
            )
        })?;

    runtime = merge_runtime_object(
        runtime,
        json!({
            "runtimeState": "probing",
            "downloadedBytes": downloaded,
            "totalBytes": response.content_length.or(Some(downloaded)),
            "progress": 1.0,
            "downloadRateBps": 0,
            "updatedAt": Utc::now()
        }),
    );
    update_stream_runtime(
        pool,
        &pending.release,
        &pending.job,
        AcquisitionReleaseState::Materializing,
        ReleaseJobState::Materializing,
        "Probing completed direct HTTP stream file before import.",
        true,
        None,
        runtime.clone(),
    )
    .await?;

    let probe_evidence = match probe.probe(&target_path).await {
        Ok(probe) => probe,
        Err(err) => {
            fail_stream_job(
                pool,
                pending,
                &runtime,
                "ffprobe_failed",
                &format!("ffprobe failed for completed direct HTTP stream file: {err}"),
                None,
            )
            .await?;
            return Ok(DirectFileMaterializationOutcome::Failed);
        }
    };

    finish_materialized_stream_job(
        pool,
        config,
        pending,
        &target_path,
        downloaded,
        response.content_length.or(Some(downloaded)),
        &probe_evidence,
        StreamDeliveryType::DirectFile,
        "ess6_direct_file_stream_candidate",
        "ess6_direct_file_materializer",
        "ess6_direct_file",
        runtime,
    )
    .await
}

enum ProtectedStreamEgressAvailability {
    Available(StreamEgressRoute),
    Blocked(StreamEgressRoute),
}

async fn resolve_protected_stream_egress(
    pool: &AnyPool,
    route: StreamEgressRoute,
) -> Result<ProtectedStreamEgressAvailability> {
    if !route.requires_protected() {
        return Ok(ProtectedStreamEgressAvailability::Available(route));
    }
    match active_managed_downloader_runtime(pool).await {
        Ok(ActiveManagedDownloaderRuntime::WireguardConfig { profile_id, .. }) => {
            Ok(ProtectedStreamEgressAvailability::Available(
                route.with_protected_runtime(
                    Some(profile_id),
                    Some("wireguard_config".to_string()),
                    None,
                ),
            ))
        }
        Ok(ActiveManagedDownloaderRuntime::OpenvpnConfig { profile_id, .. }) => {
            Ok(ProtectedStreamEgressAvailability::Available(
                route.with_protected_runtime(
                    Some(profile_id),
                    Some("openvpn_config".to_string()),
                    None,
                ),
            ))
        }
        Ok(ActiveManagedDownloaderRuntime::CloudflareWarp { profile_id, .. }) => {
            Ok(ProtectedStreamEgressAvailability::Available(
                route.with_protected_runtime(
                    Some(profile_id),
                    Some("cloudflare_warp".to_string()),
                    None,
                ),
            ))
        }
        Ok(ActiveManagedDownloaderRuntime::UnsupportedProtected { profile_id, kind }) => {
            Ok(ProtectedStreamEgressAvailability::Blocked(route.blocked(
                format!(
                    "Stream download blocked: active protected profile '{profile_id}' uses unsupported runtime '{kind:?}'."
                ),
                Some(profile_id),
                Some(format!("{kind:?}")),
            )))
        }
        Ok(ActiveManagedDownloaderRuntime::Direct) => Ok(ProtectedStreamEgressAvailability::Blocked(
            route.blocked(
                "Stream download blocked: active egress profile is direct.",
                None,
                Some("direct".to_string()),
            ),
        )),
        Ok(ActiveManagedDownloaderRuntime::NoStoredProfile) => {
            Ok(ProtectedStreamEgressAvailability::Blocked(route.blocked(
                "Stream download blocked: no protected egress profile is configured.",
                None,
                None,
            )))
        }
        Err(err) => Ok(ProtectedStreamEgressAvailability::Blocked(route.blocked(
            format!("Stream download blocked: protected egress is unavailable: {err}"),
            None,
            None,
        ))),
    }
}

fn stream_egress_failure_class(route: &StreamEgressRoute) -> &'static str {
    match route.decision {
        StreamEgressDecision::RejectedByPolicy => "stream_egress_policy_rejected",
        StreamEgressDecision::BlockedProtectedEgressUnavailable => {
            "protected_stream_egress_unavailable"
        }
        _ => "stream_egress_failed",
    }
}

fn choose_direct_file_name_from_request(
    candidate: &Value,
    request: &DirectFileDownloadRequest,
) -> String {
    filename_from_url(request.url.as_str())
        .or_else(|| stream_candidate_string(candidate, "/title"))
        .map(|value| safe_file_name(&value))
        .filter(|value| !value.is_empty())
        .map(|value| ensure_media_extension(value, None))
        .unwrap_or_else(|| ensure_media_extension("http-stream-download".to_string(), None))
}

struct StreamSecretReference {
    scope: SecretScope,
    scope_id: Option<Uuid>,
    key: String,
}

async fn resolve_stream_secret_value(
    store: &ExtensionStore<'_>,
    secrets: &SecretsManager,
    instance_id: Uuid,
    raw: &str,
) -> Result<String> {
    let reference = parse_stream_secret_reference(raw, instance_id)?;
    let secret = store
        .get_secret(reference.scope, reference.scope_id, &reference.key)
        .await?
        .ok_or_else(|| anyhow!("secret '{}' not found", reference.key))?;
    secrets
        .decrypt(&secret.value_encrypted)
        .with_context(|| format!("decrypting secret '{}'", reference.key))
}

fn parse_stream_secret_reference(raw: &str, instance_id: Uuid) -> Result<StreamSecretReference> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("secret reference must not be empty");
    }
    let parts = trimmed.split(':').collect::<Vec<_>>();
    match parts.as_slice() {
        ["instance", key] if !key.trim().is_empty() => Ok(StreamSecretReference {
            scope: SecretScope::Instance,
            scope_id: Some(instance_id),
            key: (*key).to_string(),
        }),
        ["global", key] if !key.trim().is_empty() => Ok(StreamSecretReference {
            scope: SecretScope::Global,
            scope_id: None,
            key: (*key).to_string(),
        }),
        ["provider", provider_id, key] if !key.trim().is_empty() => {
            let provider_id = Uuid::parse_str(provider_id)
                .map_err(|_| anyhow!("provider secret reference id is invalid"))?;
            Ok(StreamSecretReference {
                scope: SecretScope::Provider,
                scope_id: Some(provider_id),
                key: (*key).to_string(),
            })
        }
        _ => {
            bail!("secret reference must be instance:<key>, global:<key>, or provider:<uuid>:<key>")
        }
    }
}

fn render_stream_openvpn_config(config: &str, has_auth_file: bool) -> String {
    if !has_auth_file {
        return config.to_string();
    }
    let mut replaced = false;
    let mut lines = Vec::new();
    for line in config.lines() {
        if line.trim_start().starts_with("auth-user-pass") {
            lines.push("auth-user-pass /gluetun/auth.txt".to_string());
            replaced = true;
        } else {
            lines.push(line.to_string());
        }
    }
    if !replaced {
        lines.push("auth-user-pass /gluetun/auth.txt".to_string());
    }
    lines.join("\n") + "\n"
}

async fn set_stream_private_file_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).await?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn materialize_direct_file_job_via_protected_egress(
    pool: &AnyPool,
    config: &HttpStreamMaterializerConfig,
    protected_materializer: &dyn ProtectedStreamMaterializer,
    probe: &dyn StreamFileProbe,
    pending: &PendingDirectFileJob,
    request: DirectFileDownloadRequest,
    egress_route: StreamEgressRoute,
    base_runtime: Value,
) -> Result<DirectFileMaterializationOutcome> {
    let egress_route = match resolve_protected_stream_egress(pool, egress_route).await? {
        ProtectedStreamEgressAvailability::Available(route) => route,
        ProtectedStreamEgressAvailability::Blocked(route) => {
            let runtime = merge_runtime_object(
                base_runtime,
                json!({
                    "routeLabel": route.route_label(),
                    "egress": route.runtime_json(),
                }),
            );
            fail_stream_job(
                pool,
                pending,
                &runtime,
                stream_egress_failure_class(&route),
                &route.reason,
                None,
            )
            .await?;
            return Ok(DirectFileMaterializationOutcome::Failed);
        }
    };
    let download_id = pending
        .job
        .download_id
        .as_deref()
        .or(pending.release.download_id.as_deref())
        .ok_or_else(|| anyhow!("HTTP stream job is missing download id"))?
        .to_string();
    let file_name = choose_direct_file_name_from_request(&pending.candidate, &request);
    let target_dir = config
        .paths
        .staging_root
        .join(media_type_segment(pending.release.media_type))
        .join(safe_path_segment(&download_id));
    fs::create_dir_all(&target_dir)
        .await
        .with_context(|| format!("creating stream target dir '{}'", target_dir.display()))?;
    let target_path = unique_target_path(&target_dir, &file_name).await;
    let partial_path = target_path.with_extension(format!(
        "{}elixir-part",
        target_path
            .extension()
            .and_then(|value| value.to_str())
            .map(|value| format!("{value}."))
            .unwrap_or_default()
    ));

    let mut runtime = merge_runtime_object(
        base_runtime,
        json!({
            "runtimeState": "downloading",
            "routeLabel": egress_route.route_label(),
            "egress": egress_route.runtime_json(),
            "localPath": target_path.to_string_lossy(),
            "partialPath": partial_path.to_string_lossy(),
            "downloadedBytes": 0,
            "progress": Value::Null,
            "downloadRateBps": 0,
        }),
    );
    update_stream_runtime(
        pool,
        &pending.release,
        &pending.job,
        AcquisitionReleaseState::Downloading,
        ReleaseJobState::Downloading,
        "Downloading HTTP stream file via protected egress.",
        true,
        None,
        runtime.clone(),
    )
    .await?;

    let protected_result = match protected_materializer
        .materialize_direct_file(ProtectedDirectFileRequest {
            download: request,
            partial_path: partial_path.clone(),
        })
        .await
    {
        Ok(result) => result,
        Err(err) => {
            cleanup_partial(&partial_path, &config.paths.staging_root).await;
            fail_stream_job(
                pool,
                pending,
                &runtime,
                "protected_stream_materializer_failed",
                &format!("Protected stream materializer failed: {err}"),
                Some(&partial_path),
            )
            .await?;
            return Ok(DirectFileMaterializationOutcome::Failed);
        }
    };

    if job_cancelled(pool, pending.job.release_job_id).await? {
        cleanup_partial(&partial_path, &config.paths.staging_root).await;
        mark_stream_cancelled(pool, pending, &runtime, &partial_path).await?;
        return Ok(DirectFileMaterializationOutcome::Cancelled);
    }
    let output_bytes = fs::metadata(&partial_path)
        .await
        .with_context(|| {
            format!(
                "reading protected stream partial '{}'",
                partial_path.display()
            )
        })?
        .len();
    if output_bytes == 0 {
        cleanup_partial(&partial_path, &config.paths.staging_root).await;
        fail_stream_job(
            pool,
            pending,
            &runtime,
            "protected_stream_empty_output",
            "Protected stream materializer produced an empty output file.",
            Some(&partial_path),
        )
        .await?;
        return Ok(DirectFileMaterializationOutcome::Failed);
    }
    fs::rename(&partial_path, &target_path)
        .await
        .with_context(|| {
            format!(
                "moving protected stream partial '{}' to '{}'",
                partial_path.display(),
                target_path.display()
            )
        })?;
    let egress_route = egress_route.clone().with_protected_runtime(
        egress_route.protected_profile_id.clone(),
        egress_route.protected_runtime_kind.clone(),
        protected_result.worker_runtime_id.clone(),
    );
    let protected_final_url = protected_result.final_url.clone();
    let protected_content_type = protected_result.content_type.clone();
    let protected_content_length = protected_result.content_length;
    let protected_content_disposition = protected_result.content_disposition.clone();
    let protected_downloaded_bytes = protected_result.downloaded_bytes.max(output_bytes);
    let protected_stderr_tail = protected_result.stderr_tail.clone();
    runtime = merge_runtime_object(
        runtime,
        json!({
            "runtimeState": "probing",
            "finalUrl": runtime_safe_stream_url(&protected_final_url),
            "egress": egress_route.with_final_url(&protected_final_url).runtime_json(),
            "contentType": protected_content_type,
            "contentLength": protected_content_length,
            "contentDisposition": protected_content_disposition,
            "downloadedBytes": protected_downloaded_bytes,
            "totalBytes": protected_content_length.or(Some(output_bytes)),
            "progress": 1.0,
            "downloadRateBps": 0,
            "protectedWorker": {
                "stderrTail": protected_stderr_tail
            },
            "updatedAt": Utc::now()
        }),
    );
    update_stream_runtime(
        pool,
        &pending.release,
        &pending.job,
        AcquisitionReleaseState::Materializing,
        ReleaseJobState::Materializing,
        "Probing completed protected HTTP stream file before import.",
        true,
        None,
        runtime.clone(),
    )
    .await?;

    let probe_evidence = match probe.probe(&target_path).await {
        Ok(probe) => probe,
        Err(err) => {
            fail_stream_job(
                pool,
                pending,
                &runtime,
                "ffprobe_failed",
                &format!("ffprobe failed for completed protected HTTP stream file: {err}"),
                None,
            )
            .await?;
            return Ok(DirectFileMaterializationOutcome::Failed);
        }
    };

    finish_materialized_stream_job(
        pool,
        config,
        pending,
        &target_path,
        output_bytes,
        protected_result.content_length.or(Some(output_bytes)),
        &probe_evidence,
        StreamDeliveryType::DirectFile,
        "hse_protected_direct_file_stream_candidate",
        "hse_protected_direct_file_materializer",
        "hse_protected_direct_file",
        runtime,
    )
    .await
}

async fn materialize_remux_stream_job(
    pool: &AnyPool,
    config: &HttpStreamMaterializerConfig,
    remuxer: &dyn StreamRemuxer,
    egress_classifier: &dyn StreamEgressClassifier,
    protected_materializer: &dyn ProtectedStreamMaterializer,
    probe: &dyn StreamFileProbe,
    pending: &PendingDirectFileJob,
    egress_policy: StreamHttpEgressPolicy,
) -> Result<DirectFileMaterializationOutcome> {
    let stream_type = StreamDeliveryType::from_candidate(&pending.candidate)
        .ok_or_else(|| anyhow!("stream candidate is missing delivery.streamType"))?;
    if let Some((failure_class, message)) =
        unsupported_stream_materialization_feature(&pending.candidate, stream_type)
    {
        let runtime = base_stream_runtime(pending, stream_type, None, Vec::new(), false)?;
        update_stream_runtime(
            pool,
            &pending.release,
            &pending.job,
            AcquisitionReleaseState::Materializing,
            ReleaseJobState::Materializing,
            "HTTP stream materializer rejected an unsupported stream candidate.",
            true,
            None,
            runtime.clone(),
        )
        .await?;
        fail_stream_job(pool, pending, &runtime, failure_class, &message, None).await?;
        return Ok(DirectFileMaterializationOutcome::Failed);
    }

    let download_id = pending
        .job
        .download_id
        .as_deref()
        .or(pending.release.download_id.as_deref())
        .ok_or_else(|| anyhow!("HTTP stream job is missing download id"))?
        .to_string();
    let request = stream_delivery_request(&pending.candidate)?;
    let duration_seconds =
        stream_candidate_f64(&pending.candidate, "/targetEvidence/runtimeSeconds")
            .or_else(|| stream_candidate_f64(&pending.candidate, "/mediaEvidence/runtimeSeconds"))
            .or_else(|| stream_candidate_f64(&pending.candidate, "/mediaEvidence/durationSeconds"));
    let initial_remux_request = StreamRemuxRequest {
        stream_type,
        url: request.url.clone(),
        headers: request.headers.clone(),
        referer: request.referer.clone(),
        partial_path: PathBuf::new(),
        duration_seconds,
    };
    let egress_route = egress_classifier
        .classify_remux_stream(egress_policy, &initial_remux_request)
        .await
        .context("classifying remux stream egress")?;
    let header_names = request
        .headers
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();
    let referer_applied = request.referer.is_some();
    let started_at = Utc::now();
    let mut runtime = base_stream_runtime(
        pending,
        stream_type,
        Some(request.url.as_str()),
        header_names,
        referer_applied,
    )?;
    runtime = merge_runtime_object(
        runtime,
        json!({
            "runtimeState": "materializing",
            "routeLabel": egress_route.route_label(),
            "egress": egress_route.runtime_json(),
            "startedAt": started_at,
        }),
    );
    update_stream_runtime(
        pool,
        &pending.release,
        &pending.job,
        AcquisitionReleaseState::Materializing,
        ReleaseJobState::Materializing,
        &format!(
            "{} is preparing for ffmpeg stream-copy materialization.",
            stream_type.materializer_label()
        ),
        true,
        None,
        runtime.clone(),
    )
    .await?;

    let file_name = choose_stream_remux_file_name(&pending.candidate, stream_type);
    let target_dir = config
        .paths
        .staging_root
        .join(media_type_segment(pending.release.media_type))
        .join(safe_path_segment(&download_id));
    fs::create_dir_all(&target_dir)
        .await
        .with_context(|| format!("creating stream target dir '{}'", target_dir.display()))?;
    let target_path = unique_target_path(&target_dir, &file_name).await;
    let partial_path = target_path.with_extension(format!(
        "{}elixir-part",
        target_path
            .extension()
            .and_then(|value| value.to_str())
            .map(|value| format!("{value}."))
            .unwrap_or_default()
    ));

    runtime = merge_runtime_object(
        runtime,
        json!({
            "runtimeState": "remuxing",
            "localPath": target_path.to_string_lossy(),
            "partialPath": partial_path.to_string_lossy(),
            "durationSeconds": duration_seconds,
            "downloadedBytes": 0,
            "progress": Value::Null,
            "downloadRateBps": 0
        }),
    );
    update_stream_runtime(
        pool,
        &pending.release,
        &pending.job,
        AcquisitionReleaseState::Downloading,
        ReleaseJobState::Downloading,
        &format!(
            "{} is being stream-copied into acquisition staging.",
            stream_type.materializer_label()
        ),
        true,
        None,
        runtime.clone(),
    )
    .await?;

    let remux_request = StreamRemuxRequest {
        stream_type,
        url: request.url,
        headers: request.headers,
        referer: request.referer,
        partial_path: partial_path.clone(),
        duration_seconds,
    };
    let mut progress_sink = DbStreamRemuxProgressSink {
        pool,
        pending,
        runtime,
        partial_path: partial_path.clone(),
        duration_seconds,
    };
    let remux_result = if egress_route.is_terminal_rejection() {
        fail_stream_job(
            pool,
            pending,
            &progress_sink.runtime,
            stream_egress_failure_class(&egress_route),
            &egress_route.reason,
            None,
        )
        .await?;
        return Ok(DirectFileMaterializationOutcome::Failed);
    } else if egress_route.requires_protected() {
        match resolve_protected_stream_egress(pool, egress_route.clone()).await? {
            ProtectedStreamEgressAvailability::Available(route) => {
                progress_sink.runtime = merge_runtime_object(
                    progress_sink.runtime.clone(),
                    json!({
                        "routeLabel": route.route_label(),
                        "egress": route.runtime_json(),
                    }),
                );
                let protected = protected_materializer
                    .remux_stream(
                        ProtectedRemuxRequest {
                            remux: remux_request,
                        },
                        &mut progress_sink,
                    )
                    .await;
                match protected {
                    Ok(result) => {
                        let worker_route = route.clone().with_protected_runtime(
                            route.protected_profile_id.clone(),
                            route.protected_runtime_kind.clone(),
                            result.worker_runtime_id.clone(),
                        );
                        progress_sink.runtime = merge_runtime_object(
                            progress_sink.runtime.clone(),
                            json!({
                                "egress": worker_route.runtime_json(),
                            }),
                        );
                        Ok(result.remux)
                    }
                    Err(err) => Err(err),
                }
            }
            ProtectedStreamEgressAvailability::Blocked(route) => {
                progress_sink.runtime = merge_runtime_object(
                    progress_sink.runtime.clone(),
                    json!({
                        "routeLabel": route.route_label(),
                        "egress": route.runtime_json(),
                    }),
                );
                fail_stream_job(
                    pool,
                    pending,
                    &progress_sink.runtime,
                    stream_egress_failure_class(&route),
                    &route.reason,
                    None,
                )
                .await?;
                return Ok(DirectFileMaterializationOutcome::Failed);
            }
        }
    } else {
        remuxer.remux(remux_request, &mut progress_sink).await
    };
    let remux_result = match remux_result {
        Ok(result) => result,
        Err(err) => {
            cleanup_partial(&partial_path, &config.paths.staging_root).await;
            if job_cancelled(pool, pending.job.release_job_id).await? {
                mark_stream_cancelled(pool, pending, &progress_sink.runtime, &partial_path).await?;
                return Ok(DirectFileMaterializationOutcome::Cancelled);
            }
            fail_stream_job(
                pool,
                pending,
                &progress_sink.runtime,
                "ffmpeg_copy_failed",
                &format!(
                    "{} ffmpeg stream-copy failed: {err}",
                    stream_type.materializer_label()
                ),
                Some(&partial_path),
            )
            .await?;
            return Ok(DirectFileMaterializationOutcome::Failed);
        }
    };
    runtime = progress_sink.runtime;

    if job_cancelled(pool, pending.job.release_job_id).await? {
        cleanup_partial(&partial_path, &config.paths.staging_root).await;
        mark_stream_cancelled(pool, pending, &runtime, &partial_path).await?;
        return Ok(DirectFileMaterializationOutcome::Cancelled);
    }
    fs::rename(&partial_path, &target_path)
        .await
        .with_context(|| {
            format!(
                "moving stream remux partial '{}' to '{}'",
                partial_path.display(),
                target_path.display()
            )
        })?;

    let remux_final_url = remux_result.final_url.clone();
    let remux_final_egress = runtime_egress_with_final_url(&runtime, remux_final_url.as_deref());
    let remux_final_url_evidence = remux_final_url.as_deref().map(runtime_safe_stream_url);
    runtime = merge_runtime_object(
        runtime,
        json!({
            "runtimeState": "probing",
            "finalUrl": remux_final_url_evidence,
            "egress": remux_final_egress,
            "downloadedBytes": remux_result.output_bytes,
            "totalBytes": remux_result.output_bytes,
            "progress": 1.0,
            "downloadRateBps": 0,
            "ffmpeg": {
                "copyMode": true,
                "outputContainer": "matroska",
                "stderrTail": remux_result.stderr_tail,
                "finalProgress": remux_progress_json(remux_result.final_progress.as_ref(), duration_seconds)
            },
            "updatedAt": Utc::now()
        }),
    );
    update_stream_runtime(
        pool,
        &pending.release,
        &pending.job,
        AcquisitionReleaseState::Materializing,
        ReleaseJobState::Materializing,
        &format!(
            "Probing completed {} file before import.",
            stream_type.materializer_label()
        ),
        true,
        None,
        runtime.clone(),
    )
    .await?;

    let probe_evidence = match probe.probe(&target_path).await {
        Ok(probe) => probe,
        Err(err) => {
            fail_stream_job(
                pool,
                pending,
                &runtime,
                "ffprobe_failed",
                &format!(
                    "ffprobe failed for completed {} file: {err}",
                    stream_type.materializer_label()
                ),
                None,
            )
            .await?;
            return Ok(DirectFileMaterializationOutcome::Failed);
        }
    };

    finish_materialized_stream_job(
        pool,
        config,
        pending,
        &target_path,
        remux_result.output_bytes,
        Some(remux_result.output_bytes),
        &probe_evidence,
        stream_type,
        "ess7_hls_dash_stream_candidate",
        "ess7_hls_dash_materializer",
        "ess7_hls_dash",
        runtime,
    )
    .await
}

async fn finish_materialized_stream_job(
    pool: &AnyPool,
    config: &HttpStreamMaterializerConfig,
    pending: &PendingDirectFileJob,
    target_path: &Path,
    materialized_bytes: u64,
    total_bytes: Option<u64>,
    probe_evidence: &ProbeEvidence,
    stream_type: StreamDeliveryType,
    parser_reason: &str,
    materializer_id: &str,
    coverage_reason_prefix: &str,
    runtime: Value,
) -> Result<DirectFileMaterializationOutcome> {
    let verification = verify_materialized_stream_file(
        &pending.candidate,
        target_path,
        &config.paths.staging_root,
        materialized_bytes,
        probe_evidence,
        stream_type,
    )
    .await?;
    if verification.state == StreamVerificationState::Failed {
        cleanup_materialized_file(target_path, &config.paths.staging_root).await;
        fail_stream_job(
            pool,
            pending,
            &merge_runtime_object(
                runtime,
                json!({
                    "runtimeState": "failed",
                    "downloadedBytes": materialized_bytes,
                    "totalBytes": total_bytes,
                    "progress": 1.0,
                    "downloadRateBps": 0,
                    "probe": probe_evidence_json(probe_evidence),
                    "verification": verification.evidence,
                    "completedAt": Utc::now()
                }),
            ),
            verification
                .mismatch_class
                .as_deref()
                .unwrap_or("stream_verification_failed"),
            &verification.reason,
            None,
        )
        .await?;
        return Ok(DirectFileMaterializationOutcome::Failed);
    }

    let release_file = persist_stream_release_file(
        pool,
        pending,
        target_path,
        materialized_bytes,
        probe_evidence,
        &verification,
        stream_type,
        parser_reason,
        materializer_id,
    )
    .await?;
    let coverage_selected = persist_stream_target_coverage(
        pool,
        pending,
        &release_file,
        coverage_reason_prefix,
        &verification,
    )
    .await?;
    let completion_state = if coverage_selected {
        AcquisitionReleaseState::Completed
    } else {
        AcquisitionReleaseState::ReviewRequired
    };
    let reason = if coverage_selected {
        "HTTP stream file was materialized, verified, and mapped to the requested target."
    } else {
        verification.reason.as_str()
    };
    let runtime = merge_runtime_object(
        runtime,
        json!({
            "runtimeState": if coverage_selected { "completed" } else { "review_required" },
            "downloadedBytes": materialized_bytes,
            "totalBytes": total_bytes,
            "progress": 1.0,
            "downloadRateBps": 0,
            "releaseFileId": release_file.release_file_id,
            "targetMappingState": if coverage_selected { "selected" } else { "review_required" },
            "probe": probe_evidence_json(probe_evidence),
            "verification": verification.evidence,
            "completedAt": Utc::now()
        }),
    );
    update_stream_runtime(
        pool,
        &pending.release,
        &pending.job,
        completion_state,
        ReleaseJobState::Completed,
        reason,
        false,
        Some(Utc::now()),
        runtime,
    )
    .await?;

    if coverage_selected {
        Ok(DirectFileMaterializationOutcome::Completed)
    } else {
        Ok(DirectFileMaterializationOutcome::ReviewRequired)
    }
}

async fn verify_materialized_stream_file(
    candidate: &Value,
    target_path: &Path,
    staging_root: &Path,
    materialized_bytes: u64,
    probe: &ProbeEvidence,
    stream_type: StreamDeliveryType,
) -> Result<StreamVerificationDecision> {
    let actual_file_size = fs::metadata(target_path)
        .await
        .ok()
        .map(|metadata| metadata.len());
    let expected_runtime_seconds = expected_stream_runtime_seconds(candidate);
    let expected_resolution_height = expected_stream_resolution_height(candidate);
    let required_audio_languages = required_stream_audio_languages(candidate);
    let detected_audio_languages = probe.detected_audio_languages();
    let mut failure_reasons = Vec::new();
    let mut review_reasons = Vec::new();
    let mut failure_class: Option<&'static str> = None;
    let mut review_class: Option<&'static str> = None;

    if !path_is_under(target_path, staging_root) {
        failure_class = Some("unsafe_materialized_path");
        failure_reasons
            .push("materialized stream path is outside the approved staging root".to_string());
    }
    match actual_file_size {
        Some(0) | None => {
            failure_class.get_or_insert("probe_corrupt_output");
            failure_reasons.push("materialized stream file is missing or empty".to_string());
        }
        Some(_) => {}
    }
    if materialized_bytes == 0 {
        failure_class.get_or_insert("probe_corrupt_output");
        failure_reasons.push("materializer reported zero output bytes".to_string());
    }
    if probe.container.as_deref().is_none_or(str::is_empty) {
        failure_class.get_or_insert("probe_corrupt_output");
        failure_reasons.push("ffprobe did not report a readable container".to_string());
    }
    if probe.video_stream_count() == 0 {
        failure_class.get_or_insert("probe_corrupt_output");
        failure_reasons.push("ffprobe did not report any video streams".to_string());
    }

    if let Some(expected) = expected_runtime_seconds {
        match probe.duration_seconds {
            Some(actual) if duration_is_sane_for_target(expected, actual as f64) => {}
            Some(actual) => {
                review_class.get_or_insert("probe_target_mismatch");
                review_reasons.push(format!(
                    "ffprobe duration {actual}s is not sane for expected target runtime {:.0}s",
                    expected
                ));
            }
            None => {
                review_class.get_or_insert("probe_target_mismatch");
                review_reasons.push(format!(
                    "ffprobe did not report a duration for expected target runtime {:.0}s",
                    expected
                ));
            }
        }
    }

    if let (Some(expected), Some(actual)) = (expected_resolution_height, probe.height)
        && expected >= 720
        && actual > 0
        && (actual as f64) < ((expected as f64) * 0.70)
    {
        review_class.get_or_insert("probe_resolution_mismatch");
        review_reasons.push(format!(
            "ffprobe height {actual}p is below the expected {expected}p quality tolerance"
        ));
    }

    if !required_audio_languages.is_empty() {
        if probe.audio_stream_count() == 0 {
            review_class.get_or_insert("probe_audio_missing");
            review_reasons.push(format!(
                "ffprobe did not report any audio streams for required audio language {}",
                format_language_list(required_audio_languages.iter())
            ));
        } else if !detected_audio_languages.is_empty()
            && !required_audio_languages
                .iter()
                .any(|language| detected_audio_languages.contains(language))
        {
            review_class.get_or_insert("probe_language_mismatch");
            review_reasons.push(format!(
                "ffprobe audio languages {} do not include required or claimed language {}",
                format_language_list(detected_audio_languages.iter()),
                format_language_list(required_audio_languages.iter())
            ));
        }
    }
    if let Some(reason) =
        strict_language_preference_review_reason(candidate, &required_audio_languages, probe)
    {
        review_class.get_or_insert("probe_language_unconfirmed");
        review_reasons.push(reason);
    }

    if !failure_reasons.is_empty() {
        let reason = format!(
            "{} verification failed: {}.",
            stream_type.materializer_label(),
            failure_reasons.join("; ")
        );
        let mismatch_class = failure_class.unwrap_or("stream_verification_failed");
        return Ok(StreamVerificationDecision {
            state: StreamVerificationState::Failed,
            mismatch_class: Some(mismatch_class.to_string()),
            evidence: stream_verification_evidence(
                StreamVerificationState::Failed,
                Some(mismatch_class),
                &failure_reasons,
                candidate,
                target_path,
                staging_root,
                materialized_bytes,
                actual_file_size,
                expected_runtime_seconds,
                expected_resolution_height,
                &required_audio_languages,
                &detected_audio_languages,
                probe,
                stream_type,
            ),
            reason,
        });
    }

    if !review_reasons.is_empty() {
        let reason = format!(
            "{} verification requires review before import: {}.",
            stream_type.materializer_label(),
            review_reasons.join("; ")
        );
        let mismatch_class = review_class.unwrap_or("stream_verification_review_required");
        return Ok(StreamVerificationDecision {
            state: StreamVerificationState::ReviewRequired,
            mismatch_class: Some(mismatch_class.to_string()),
            evidence: stream_verification_evidence(
                StreamVerificationState::ReviewRequired,
                Some(mismatch_class),
                &review_reasons,
                candidate,
                target_path,
                staging_root,
                materialized_bytes,
                actual_file_size,
                expected_runtime_seconds,
                expected_resolution_height,
                &required_audio_languages,
                &detected_audio_languages,
                probe,
                stream_type,
            ),
            reason,
        });
    }

    let reasons = vec!["materialized stream passed structural ffprobe checks".to_string()];
    Ok(StreamVerificationDecision {
        state: StreamVerificationState::Verified,
        mismatch_class: None,
        reason: "HTTP stream file passed post-materialization verification.".to_string(),
        evidence: stream_verification_evidence(
            StreamVerificationState::Verified,
            None,
            &reasons,
            candidate,
            target_path,
            staging_root,
            materialized_bytes,
            actual_file_size,
            expected_runtime_seconds,
            expected_resolution_height,
            &required_audio_languages,
            &detected_audio_languages,
            probe,
            stream_type,
        ),
    })
}

fn direct_file_download_request(candidate: &Value) -> Result<DirectFileDownloadRequest> {
    stream_delivery_request(candidate)
        .with_context(|| "direct_file stream candidate delivery is invalid".to_string())
}

fn stream_candidate_has_materializable_or_resolvable_delivery(candidate: &Value) -> bool {
    stream_candidate_has_safe_delivery_url(candidate)
        || stream_candidate_string(candidate, "/delivery/resolveHandle").is_some()
}

fn stream_candidate_has_safe_delivery_url(candidate: &Value) -> bool {
    stream_candidate_string(candidate, "/delivery/url")
        .as_deref()
        .is_some_and(|url| validate_safe_http_url(url).is_ok())
}

fn stream_candidate_needs_late_resolve(candidate: &Value) -> bool {
    !stream_candidate_has_safe_delivery_url(candidate)
        || stream_candidate_bool(candidate, "/delivery/resolveRequired").unwrap_or(false)
        || stream_candidate_delivery_expired(candidate)
}

fn stream_candidate_late_resolve_reason(candidate: &Value) -> &'static str {
    if !stream_candidate_has_safe_delivery_url(candidate) {
        "missing_or_unsafe_delivery_url"
    } else if stream_candidate_bool(candidate, "/delivery/resolveRequired").unwrap_or(false) {
        "resolve_required"
    } else if stream_candidate_delivery_expired(candidate) {
        "delivery_expired"
    } else {
        "not_required"
    }
}

fn stream_candidate_delivery_expired(candidate: &Value) -> bool {
    let Some(expires_at) = stream_candidate_string(candidate, "/delivery/expiresAt") else {
        return false;
    };
    match DateTime::parse_from_rfc3339(&expires_at) {
        Ok(expires_at) => expires_at.with_timezone(&Utc) <= Utc::now(),
        Err(_) => true,
    }
}

fn stream_candidate_source_provider_id(candidate: &Value) -> Option<Uuid> {
    [
        "/sourceProviderId",
        "/raw/serverEvidence/extensionSuite/providerId",
        "/raw/serverEvidence/extensionSuite/primary/providerId",
        "/sourceSuite/providerId",
    ]
    .iter()
    .find_map(|pointer| stream_candidate_string(candidate, pointer))
    .and_then(|value| Uuid::parse_str(&value).ok())
}

fn stream_candidate_header_names(candidate: &Value) -> Vec<String> {
    candidate
        .pointer("/delivery/headers")
        .and_then(Value::as_object)
        .map(|headers| headers.keys().cloned().collect())
        .unwrap_or_default()
}

fn merge_late_resolved_stream_candidate(
    candidate: &Value,
    resolved: StreamCandidateResolveResult,
) -> Result<(Value, Vec<String>)> {
    let mut merged = candidate.clone();
    let original_stream_type = stream_candidate_string(candidate, "/delivery/streamType")
        .ok_or_else(|| anyhow!("candidate is missing delivery.streamType"))?;
    let delivery = resolved
        .delivery
        .or_else(|| {
            resolved
                .candidate
                .as_ref()
                .and_then(|candidate| candidate.get("delivery").cloned())
        })
        .ok_or_else(|| anyhow!("resolve response is missing delivery"))?;
    let delivery_object = delivery
        .as_object()
        .ok_or_else(|| anyhow!("resolve response delivery must be an object"))?;
    let mut merged_delivery = candidate
        .pointer("/delivery")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    for (key, value) in delivery_object {
        merged_delivery.insert(key.clone(), value.clone());
    }
    merged_delivery
        .entry("streamType".to_string())
        .or_insert_with(|| Value::String(original_stream_type.clone()));
    let resolved_stream_type = merged_delivery
        .get("streamType")
        .and_then(Value::as_str)
        .map(|value| value.trim().to_ascii_lowercase())
        .ok_or_else(|| anyhow!("resolved delivery is missing streamType"))?;
    if resolved_stream_type != original_stream_type.to_ascii_lowercase() {
        bail!(
            "resolved delivery changed streamType from {original_stream_type} to {resolved_stream_type}"
        );
    }
    if merged_delivery.get("url").and_then(Value::as_str).is_none() {
        bail!("resolved delivery did not include delivery.url");
    }
    merged_delivery.insert("resolveRequired".to_string(), Value::Bool(false));
    if !delivery_object.contains_key("expiresAt") {
        merged_delivery.remove("expiresAt");
    }
    merged_delivery.insert("resolvedAt".to_string(), json!(Utc::now()));
    let merged_object = merged
        .as_object_mut()
        .ok_or_else(|| anyhow!("stream candidate must be an object"))?;
    merged_object.insert("delivery".to_string(), Value::Object(merged_delivery));
    let (candidate, validation_warnings) = validate_stream_candidate_for_broker(merged)?;
    let url = stream_candidate_string(&candidate, "/delivery/url")
        .ok_or_else(|| anyhow!("validated resolved delivery is missing delivery.url"))?;
    validate_safe_http_url(&url).context("validated resolved delivery.url is unsafe")?;
    let mut warnings = resolved
        .warnings
        .into_iter()
        .map(|warning| warning.trim().to_string())
        .filter(|warning| !warning.is_empty())
        .collect::<Vec<_>>();
    warnings.extend(validation_warnings);
    Ok((candidate, warnings))
}

async fn persist_resolved_stream_candidate(
    pool: &AnyPool,
    release_id: Uuid,
    candidate: &Value,
) -> Result<()> {
    let selected_candidate_json =
        serde_json::to_string(candidate).context("serializing late-resolved stream candidate")?;
    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_releases
         SET selected_candidate_json = ?,
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = ?",
    )
    .bind(selected_candidate_json)
    .bind(release_id.to_string())
    .execute(pool)
    .await
    .context("persisting late-resolved stream candidate")?;
    Ok(())
}

fn stream_candidate_provider_resolve_url(base_url: &str) -> Result<Url> {
    let mut base = Url::parse(base_url).context("parsing stream candidate provider base URL")?;
    let mut path = base.path().trim_end_matches('/').to_string();
    if path.is_empty() {
        path.push('/');
    } else {
        path.push('/');
    }
    base.set_path(&path);
    base.join(STREAM_CANDIDATE_PROVIDER_RESOLVE_PATH)
        .context("building stream candidate provider resolve URL")
}

async fn parse_bounded_stream_candidate_resolve_response(
    response: reqwest::Response,
) -> Result<StreamCandidateResolveResult> {
    if let Some(length) = response.content_length() {
        if length > STREAM_CANDIDATE_RESOLVE_RESPONSE_MAX_BYTES {
            bail!(
                "stream candidate resolve response exceeds {} bytes",
                STREAM_CANDIDATE_RESOLVE_RESPONSE_MAX_BYTES
            );
        }
    }
    let bytes = response
        .bytes()
        .await
        .context("reading stream candidate resolve response")?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > STREAM_CANDIDATE_RESOLVE_RESPONSE_MAX_BYTES
    {
        bail!(
            "stream candidate resolve response exceeds {} bytes",
            STREAM_CANDIDATE_RESOLVE_RESPONSE_MAX_BYTES
        );
    }
    serde_json::from_slice::<StreamCandidateResolveResult>(&bytes)
        .context("parsing stream candidate resolve response")
}

fn truncate_stream_diagnostic(value: &str, max_chars: usize) -> String {
    let mut out = String::new();
    for ch in value.chars().take(max_chars) {
        out.push(ch);
    }
    if value.chars().count() > max_chars {
        out.push_str("...");
    }
    out
}

fn stream_delivery_request(candidate: &Value) -> Result<DirectFileDownloadRequest> {
    let url = stream_candidate_string(candidate, "/delivery/url")
        .ok_or_else(|| anyhow!("stream candidate is missing delivery.url"))?;
    let url = validate_safe_http_url(&url).context("delivery.url is unsafe")?;
    let headers = stream_candidate_headers(candidate)?;
    let referer = stream_candidate_string(candidate, "/delivery/referer");
    if let Some(referer) = referer.as_deref() {
        validate_safe_http_url(referer).context("delivery.referer is unsafe")?;
    }
    Ok(DirectFileDownloadRequest {
        url,
        headers,
        referer,
    })
}

fn stream_candidate_headers(candidate: &Value) -> Result<Vec<(String, String)>> {
    let Some(headers) = candidate
        .pointer("/delivery/headers")
        .and_then(Value::as_object)
    else {
        return Ok(Vec::new());
    };
    let mut output = Vec::with_capacity(headers.len());
    for (name, value) in headers {
        let value = value
            .as_str()
            .ok_or_else(|| anyhow!("delivery.headers.{name} must be a string"))?;
        HeaderName::from_bytes(name.as_bytes())
            .with_context(|| format!("delivery.headers.{name} has an invalid header name"))?;
        HeaderValue::from_str(value)
            .with_context(|| format!("delivery.headers.{name} has an invalid header value"))?;
        output.push((name.to_string(), value.to_string()));
    }
    Ok(output)
}

#[derive(Debug, Clone, Copy)]
struct ReqwestDirectFileClient;

#[derive(Debug, Clone)]
struct HttpStreamDowngradeRedirect {
    from_scheme: String,
    to_scheme: String,
}

impl fmt::Display for HttpStreamDowngradeRedirect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "stream redirect changed scheme from {} to {}; protected egress is required",
            self.from_scheme, self.to_scheme
        )
    }
}

impl Error for HttpStreamDowngradeRedirect {}

#[async_trait]
impl DirectFileHttpClient for ReqwestDirectFileClient {
    async fn open(&self, request: DirectFileDownloadRequest) -> Result<DirectFileHttpResponse> {
        validate_safe_http_url(request.url.as_str()).context("initial URL is unsafe")?;
        if request.url.scheme() == "http" {
            return Err(HttpStreamDowngradeRedirect {
                from_scheme: "http".to_string(),
                to_scheme: "http".to_string(),
            }
            .into());
        }
        let client = Client::builder()
            .timeout(Duration::from_secs(
                STREAM_MATERIALIZER_DOWNLOAD_TIMEOUT_SECONDS,
            ))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .context("building HTTP stream materializer client")?;
        let mut header_map = HeaderMap::new();
        for (name, value) in &request.headers {
            header_map.insert(
                HeaderName::from_bytes(name.as_bytes())?,
                HeaderValue::from_str(value)?,
            );
        }
        if let Some(referer) = request.referer.as_deref()
            && !header_map.contains_key(REFERER)
        {
            header_map.insert(REFERER, HeaderValue::from_str(referer)?);
        }

        let mut next_url = request.url.clone();
        let mut redirects = 0usize;
        let response = loop {
            validate_safe_http_url(next_url.as_str()).context("redirect target is unsafe")?;
            if next_url.scheme() == "http" {
                return Err(HttpStreamDowngradeRedirect {
                    from_scheme: request.url.scheme().to_string(),
                    to_scheme: next_url.scheme().to_string(),
                }
                .into());
            }
            let response = client
                .get(next_url.clone())
                .headers(header_map.clone())
                .send()
                .await
                .context("requesting direct HTTP stream file")?;
            if !response.status().is_redirection() {
                break response;
            }
            let Some(location) = response
                .headers()
                .get(LOCATION)
                .and_then(|value| value.to_str().ok())
            else {
                break response;
            };
            redirects += 1;
            if redirects > STREAM_MATERIALIZER_MAX_REDIRECTS {
                bail!("too many stream redirects");
            }
            let redirected = next_url
                .join(location)
                .with_context(|| format!("parsing stream redirect location for {next_url}"))?;
            validate_safe_http_url(redirected.as_str())
                .context("stream redirect target is unsafe")?;
            if redirected.scheme() == "http" {
                return Err(HttpStreamDowngradeRedirect {
                    from_scheme: next_url.scheme().to_string(),
                    to_scheme: redirected.scheme().to_string(),
                }
                .into());
            }
            next_url = redirected;
        };
        let status = response.status();
        if !status.is_success() {
            bail!("direct HTTP stream file returned {status}");
        }
        if status == StatusCode::NO_CONTENT {
            bail!("direct HTTP stream file returned no content");
        }
        validate_safe_http_url(response.url().as_str()).context("final URL is unsafe")?;
        let final_url = response.url().to_string();
        let content_length = response.content_length();
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let content_disposition = response
            .headers()
            .get(CONTENT_DISPOSITION)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        Ok(DirectFileHttpResponse {
            final_url,
            content_length,
            content_type,
            content_disposition,
            body: Box::new(ReqwestDirectFileBody { response }),
        })
    }
}

struct ReqwestDirectFileBody {
    response: reqwest::Response,
}

#[async_trait]
impl DirectFileBody for ReqwestDirectFileBody {
    async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>> {
        Ok(self
            .response
            .chunk()
            .await
            .context("reading direct HTTP stream body")?
            .map(|bytes| bytes.to_vec()))
    }
}

#[derive(Debug, Clone, Copy)]
struct FfmpegStreamRemuxer;

#[async_trait]
impl StreamRemuxer for FfmpegStreamRemuxer {
    async fn remux(
        &self,
        request: StreamRemuxRequest,
        progress: &mut dyn StreamRemuxProgressSink,
    ) -> Result<StreamRemuxResult> {
        let final_url = preflight_stream_manifest_url(&request)
            .await
            .with_context(|| {
                format!(
                    "preflighting {} stream manifest URL",
                    request.stream_type.as_str()
                )
            })?;
        if let Some(parent) = request.partial_path.parent() {
            fs::create_dir_all(parent)
                .await
                .with_context(|| format!("creating remux output dir '{}'", parent.display()))?;
        }

        let args = build_ffmpeg_remux_args(&request, &final_url);
        let mut child = Command::new("ffmpeg")
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("failed to spawn ffmpeg stream materializer")?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("ffmpeg progress stdout was not captured"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| anyhow!("ffmpeg stderr was not captured"))?;
        let stderr_task =
            tokio::spawn(async move { read_limited_text(stderr, FFMPEG_STDERR_TAIL_BYTES).await });
        let mut lines = BufReader::new(stdout).lines();
        let mut progress_state = FfmpegProgressState::default();
        let started = Instant::now();
        let remux_timeout = remux_timeout_duration(request.duration_seconds);
        let mut interval = tokio::time::interval(STREAM_MATERIALIZER_PROGRESS_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut exit_status = None;

        loop {
            tokio::select! {
                line = lines.next_line() => {
                    let Some(line) = line.context("reading ffmpeg progress output")? else {
                        break;
                    };
                    if let Some(observed) = progress_state.observe_line(&line, &request.partial_path).await? {
                        if progress.observe(observed).await? == StreamRemuxControl::Cancel {
                            let _ = child.kill().await;
                            let _ = child.wait().await;
                            bail!("stream remux was cancelled");
                        }
                    }
                }
                _ = interval.tick() => {
                    if started.elapsed() > remux_timeout {
                        let _ = child.kill().await;
                        let _ = child.wait().await;
                        bail!("ffmpeg stream-copy timed out");
                    }
                    if let Some(status) = child.try_wait().context("checking ffmpeg stream-copy status")? {
                        exit_status = Some(status);
                        break;
                    }
                    if let Some(observed) = progress_state.current_progress(&request.partial_path).await? {
                        if progress.observe(observed).await? == StreamRemuxControl::Cancel {
                            let _ = child.kill().await;
                            let _ = child.wait().await;
                            bail!("stream remux was cancelled");
                        }
                    }
                }
            }
        }

        let status = match exit_status {
            Some(status) => status,
            None => child
                .wait()
                .await
                .context("waiting for ffmpeg stream-copy to exit")?,
        };
        let stderr_tail = stderr_task.await.unwrap_or_else(|err| {
            Ok(format!(
                "failed to collect ffmpeg stderr from join handle: {err}"
            ))
        })?;
        if !status.success() {
            bail!(
                "ffmpeg stream-copy failed with code {:?}: {}",
                status.code(),
                stderr_tail.trim()
            );
        }
        let output_bytes = fs::metadata(&request.partial_path)
            .await
            .with_context(|| {
                format!(
                    "reading ffmpeg remux output '{}'",
                    request.partial_path.display()
                )
            })?
            .len();
        if output_bytes == 0 {
            bail!("ffmpeg stream-copy produced an empty output file");
        }
        let final_progress = progress_state
            .current_progress(&request.partial_path)
            .await?
            .map(|mut progress| {
                progress.output_bytes = Some(output_bytes);
                progress
            });
        Ok(StreamRemuxResult {
            final_url: Some(final_url),
            output_bytes,
            final_progress,
            stderr_tail: (!stderr_tail.trim().is_empty()).then(|| stderr_tail),
        })
    }
}

fn build_ffmpeg_remux_args(request: &StreamRemuxRequest, input_url: &str) -> Vec<String> {
    let mut args = vec![
        "-hide_banner".to_string(),
        "-nostdin".to_string(),
        "-y".to_string(),
        "-loglevel".to_string(),
        "warning".to_string(),
        "-reconnect".to_string(),
        "1".to_string(),
        "-reconnect_streamed".to_string(),
        "1".to_string(),
        "-reconnect_delay_max".to_string(),
        "5".to_string(),
    ];
    let header_block = ffmpeg_header_block(&request.headers);
    if !header_block.is_empty() {
        args.push("-headers".to_string());
        args.push(header_block);
    }
    if let Some(referer) = request.referer.as_deref() {
        args.push("-referer".to_string());
        args.push(referer.to_string());
    }
    args.extend([
        "-i".to_string(),
        input_url.to_string(),
        "-map".to_string(),
        "0".to_string(),
        "-c".to_string(),
        "copy".to_string(),
        "-f".to_string(),
        "matroska".to_string(),
        "-progress".to_string(),
        "pipe:1".to_string(),
        request.partial_path.to_string_lossy().to_string(),
    ]);
    args
}

fn remux_timeout_duration(duration_seconds: Option<f64>) -> Duration {
    let max = STREAM_MATERIALIZER_REMUX_TIMEOUT_SECONDS;
    let Some(duration_seconds) = duration_seconds else {
        return Duration::from_secs(max);
    };
    if duration_seconds <= 0.0 {
        return Duration::from_secs(max);
    }
    let scaled = (duration_seconds * 4.0).ceil() as u64;
    Duration::from_secs(scaled.clamp(60 * 60, max))
}

fn ffmpeg_header_block(headers: &[(String, String)]) -> String {
    headers
        .iter()
        .map(|(name, value)| format!("{}: {}\r\n", name.trim(), value.trim()))
        .collect::<String>()
}

async fn preflight_stream_manifest_url(request: &StreamRemuxRequest) -> Result<String> {
    validate_safe_http_url(request.url.as_str()).context("initial stream URL is unsafe")?;
    if request.url.scheme() != "https" {
        bail!("host stream manifest preflight requires HTTPS-only input");
    }
    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("building HTTP stream preflight client")?;
    let headers = stream_header_map(&request.headers, request.referer.as_deref())?;
    let head = send_https_stream_request_following_https_redirects(
        &client,
        Method::HEAD,
        request.url.clone(),
        headers.clone(),
        None,
    )
    .await;
    let response = match head {
        Ok(response) if response.status().is_success() => response,
        Ok(response)
            if matches!(
                response.status(),
                StatusCode::METHOD_NOT_ALLOWED | StatusCode::FORBIDDEN | StatusCode::UNAUTHORIZED
            ) =>
        {
            send_https_stream_request_following_https_redirects(
                &client,
                Method::GET,
                request.url.clone(),
                headers,
                Some(("range", "bytes=0-0")),
            )
            .await
            .context("fallback GET preflight for stream manifest URL")?
        }
        Ok(response) => bail!("stream manifest preflight returned {}", response.status()),
        Err(err) => bail!("stream manifest preflight failed: {err}"),
    };
    if !response.status().is_success() && response.status() != StatusCode::PARTIAL_CONTENT {
        bail!("stream manifest preflight returned {}", response.status());
    }
    validate_safe_http_url(response.url().as_str()).context("final stream URL is unsafe")?;
    Ok(response.url().to_string())
}

async fn send_https_stream_request_following_https_redirects(
    client: &Client,
    method: Method,
    initial_url: Url,
    headers: HeaderMap,
    extra_header: Option<(&str, &str)>,
) -> Result<reqwest::Response> {
    let mut next_url = initial_url.clone();
    for redirect_count in 0..=STREAM_MATERIALIZER_MAX_REDIRECTS {
        validate_safe_http_url(next_url.as_str()).context("stream preflight URL is unsafe")?;
        if next_url.scheme() != "https" {
            return Err(HttpStreamDowngradeRedirect {
                from_scheme: initial_url.scheme().to_string(),
                to_scheme: next_url.scheme().to_string(),
            }
            .into());
        }
        let mut request = client
            .request(method.clone(), next_url.clone())
            .headers(headers.clone());
        if let Some((name, value)) = extra_header {
            request = request.header(name, value);
        }
        let response = request
            .send()
            .await
            .context("requesting stream manifest URL")?;
        if !response.status().is_redirection() {
            return Ok(response);
        }
        let Some(location) = response
            .headers()
            .get(LOCATION)
            .and_then(|value| value.to_str().ok())
        else {
            return Ok(response);
        };
        if redirect_count == STREAM_MATERIALIZER_MAX_REDIRECTS {
            bail!("too many stream redirects");
        }
        let redirected = next_url
            .join(location)
            .with_context(|| format!("parsing stream redirect location for {next_url}"))?;
        validate_safe_http_url(redirected.as_str()).context("stream redirect target is unsafe")?;
        if redirected.scheme() != "https" {
            return Err(HttpStreamDowngradeRedirect {
                from_scheme: next_url.scheme().to_string(),
                to_scheme: redirected.scheme().to_string(),
            }
            .into());
        }
        next_url = redirected;
    }
    bail!("too many stream redirects")
}

fn stream_header_map(headers: &[(String, String)], referer: Option<&str>) -> Result<HeaderMap> {
    let mut header_map = HeaderMap::new();
    for (name, value) in headers {
        header_map.insert(
            HeaderName::from_bytes(name.as_bytes())?,
            HeaderValue::from_str(value)?,
        );
    }
    if let Some(referer) = referer
        && !header_map.contains_key(REFERER)
    {
        header_map.insert(REFERER, HeaderValue::from_str(referer)?);
    }
    Ok(header_map)
}

#[async_trait]
#[cfg(test)]
impl StreamEgressClassifier for InitialSchemeStreamEgressClassifier {
    async fn classify_direct_file(
        &self,
        policy: StreamHttpEgressPolicy,
        request: &DirectFileDownloadRequest,
    ) -> Result<StreamEgressRoute> {
        classify_initial_stream_url(policy, &request.url, StreamDeliveryType::DirectFile)
    }

    async fn classify_remux_stream(
        &self,
        policy: StreamHttpEgressPolicy,
        request: &StreamRemuxRequest,
    ) -> Result<StreamEgressRoute> {
        classify_initial_stream_url(policy, &request.url, request.stream_type)
    }
}

#[async_trait]
impl StreamEgressClassifier for ReqwestStreamEgressClassifier {
    async fn classify_direct_file(
        &self,
        policy: StreamHttpEgressPolicy,
        request: &DirectFileDownloadRequest,
    ) -> Result<StreamEgressRoute> {
        classify_initial_stream_url(policy, &request.url, StreamDeliveryType::DirectFile)
    }

    async fn classify_remux_stream(
        &self,
        policy: StreamHttpEgressPolicy,
        request: &StreamRemuxRequest,
    ) -> Result<StreamEgressRoute> {
        let mut route = classify_initial_stream_url(policy, &request.url, request.stream_type)?;
        if route.decision != StreamEgressDecision::DirectHttps
            || policy == StreamHttpEgressPolicy::AlwaysProtected
        {
            return Ok(route);
        }

        let classification = classify_https_manifest_delivery_graph(request).await?;
        route.manifest_summary = Some(classification.summary);
        if !classification.requires_protected {
            route.reason = classification.reason;
            return Ok(route);
        }

        if policy == StreamHttpEgressPolicy::DirectOnly {
            Ok(StreamEgressRoute::rejected(
                policy,
                request.url.scheme(),
                classification.reason,
            ))
        } else {
            Ok(StreamEgressRoute {
                manifest_summary: route.manifest_summary,
                ..StreamEgressRoute::protected(
                    policy,
                    StreamEgressDecision::ProtectedMixedManifest,
                    request.url.scheme(),
                    classification.reason,
                )
            })
        }
    }
}

fn classify_initial_stream_url(
    policy: StreamHttpEgressPolicy,
    url: &Url,
    stream_type: StreamDeliveryType,
) -> Result<StreamEgressRoute> {
    validate_safe_http_url(url.as_str()).context("stream delivery URL is unsafe")?;
    let scheme = url.scheme();
    if policy == StreamHttpEgressPolicy::AlwaysProtected {
        return Ok(StreamEgressRoute::protected(
            policy,
            if scheme == "http" || stream_type == StreamDeliveryType::DirectFile {
                StreamEgressDecision::ProtectedHttp
            } else {
                StreamEgressDecision::ProtectedMixedManifest
            },
            scheme,
            "stream egress policy requires protected egress",
        ));
    }

    match scheme {
        "https" => Ok(StreamEgressRoute::direct_https(
            policy,
            scheme,
            "stream delivery URL is HTTPS",
        )),
        "http" if policy == StreamHttpEgressPolicy::DirectOnly => Ok(StreamEgressRoute::rejected(
            policy,
            scheme,
            "stream egress policy rejects HTTP delivery",
        )),
        "http" => Ok(StreamEgressRoute::protected(
            policy,
            StreamEgressDecision::ProtectedHttp,
            scheme,
            match stream_type {
                StreamDeliveryType::DirectFile => "direct-file delivery URL is HTTP",
                StreamDeliveryType::Hls => "HLS manifest URL is HTTP",
                StreamDeliveryType::Dash => "DASH manifest URL is HTTP",
            },
        )),
        _ => bail!("stream delivery URL scheme must be http or https"),
    }
}

async fn classify_https_manifest_delivery_graph(
    request: &StreamRemuxRequest,
) -> Result<StreamManifestClassification> {
    if request.url.scheme() != "https" {
        bail!("HTTPS manifest graph classification requires HTTPS initial URL");
    }
    let client = Client::builder()
        .timeout(Duration::from_secs(
            STREAM_MANIFEST_CLASSIFY_TIMEOUT_SECONDS,
        ))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("building stream manifest classification client")?;
    let headers = stream_header_map(&request.headers, request.referer.as_deref())?;
    let mut queue = vec![(request.url.clone(), 0usize)];
    let mut visited = BTreeSet::new();
    let mut inspected_manifests = 0usize;
    let mut inspected_references = 0usize;

    while let Some((url, depth)) = queue.pop() {
        if !visited.insert(url.to_string()) {
            continue;
        }
        if depth > STREAM_MANIFEST_CLASSIFY_MAX_DEPTH {
            bail!("stream manifest nesting exceeds classification depth limit");
        }
        let body =
            match fetch_https_manifest_for_classification(&client, url.clone(), headers.clone())
                .await
            {
                Ok(body) => body,
                Err(err) if err.downcast_ref::<HttpStreamDowngradeRedirect>().is_some() => {
                    return Ok(StreamManifestClassification {
                        requires_protected: true,
                        reason: "stream manifest redirect downgrades to HTTP".to_string(),
                        summary: StreamManifestClassificationSummary {
                            inspected_manifests,
                            inspected_references,
                            http_component_kind: Some("manifest_redirect".to_string()),
                            http_component_scheme: Some("http".to_string()),
                        },
                    });
                }
                Err(err) => return Err(err),
            };
        inspected_manifests += 1;
        let references = match request.stream_type {
            StreamDeliveryType::Hls => hls_manifest_references(&url, &body)?,
            StreamDeliveryType::Dash => dash_manifest_references(&url, &body)?,
            StreamDeliveryType::DirectFile => Vec::new(),
        };

        for reference in references {
            inspected_references += 1;
            if inspected_references > STREAM_MANIFEST_CLASSIFY_MAX_REFERENCES {
                bail!("stream manifest reference count exceeds classification limit");
            }
            validate_safe_http_url(reference.url.as_str())
                .with_context(|| format!("stream manifest {} URL is unsafe", reference.kind))?;
            if reference.url.scheme() == "http" {
                return Ok(StreamManifestClassification {
                    requires_protected: true,
                    reason: format!("stream manifest includes HTTP {}", reference.kind),
                    summary: StreamManifestClassificationSummary {
                        inspected_manifests,
                        inspected_references,
                        http_component_kind: Some(reference.kind),
                        http_component_scheme: Some("http".to_string()),
                    },
                });
            }
            if reference.nested_manifest && depth < STREAM_MANIFEST_CLASSIFY_MAX_DEPTH {
                queue.push((reference.url, depth + 1));
            } else if reference.nested_manifest {
                bail!("stream manifest nesting exceeds classification depth limit");
            }
        }
    }

    Ok(StreamManifestClassification {
        requires_protected: false,
        reason: "stream manifest delivery graph is HTTPS-only".to_string(),
        summary: StreamManifestClassificationSummary {
            inspected_manifests,
            inspected_references,
            http_component_kind: None,
            http_component_scheme: None,
        },
    })
}

async fn fetch_https_manifest_for_classification(
    client: &Client,
    url: Url,
    headers: HeaderMap,
) -> Result<String> {
    let response = send_https_stream_request_following_https_redirects(
        client,
        Method::GET,
        url,
        headers,
        None,
    )
    .await?;
    if !response.status().is_success() {
        bail!(
            "stream manifest classification returned {}",
            response.status()
        );
    }
    let mut response = response;
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .context("reading stream manifest classification body")?
    {
        bytes.extend_from_slice(&chunk);
        ensure_stream_manifest_classification_byte_limit(bytes.len())?;
    }
    String::from_utf8(bytes).context("stream manifest is not valid UTF-8")
}

fn ensure_stream_manifest_classification_byte_limit(byte_len: usize) -> Result<()> {
    if byte_len > STREAM_MANIFEST_CLASSIFY_MAX_BYTES {
        bail!("stream manifest exceeds classification byte limit");
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct StreamManifestReference {
    kind: String,
    url: Url,
    nested_manifest: bool,
}

fn hls_manifest_references(base_url: &Url, body: &str) -> Result<Vec<StreamManifestReference>> {
    let mut references = Vec::new();
    let mut next_uri_is_playlist = false;
    for raw_line in body.lines() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        if line.starts_with("#EXT-X-STREAM-INF") {
            next_uri_is_playlist = true;
        }
        if line.starts_with('#') {
            for uri in hls_tag_uri_attributes(line) {
                let kind = hls_tag_reference_kind(line);
                references.push(manifest_reference_from_raw(base_url, &uri, kind)?);
            }
            continue;
        }
        let kind = if next_uri_is_playlist || looks_like_manifest_url(line) {
            "nested_playlist"
        } else {
            "segment"
        };
        next_uri_is_playlist = false;
        references.push(manifest_reference_from_raw(base_url, line, kind)?);
    }
    Ok(references)
}

fn hls_tag_uri_attributes(line: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut rest = line;
    while let Some(index) = rest.find("URI=") {
        rest = &rest[index + 4..];
        if let Some(stripped) = rest.strip_prefix('"') {
            if let Some(end) = stripped.find('"') {
                values.push(stripped[..end].to_string());
                rest = &stripped[end + 1..];
            } else {
                break;
            }
        } else {
            let end = rest
                .find(',')
                .or_else(|| rest.find(char::is_whitespace))
                .unwrap_or(rest.len());
            values.push(rest[..end].trim().to_string());
            rest = &rest[end..];
        }
    }
    values
}

fn hls_tag_reference_kind(line: &str) -> &'static str {
    if line.starts_with("#EXT-X-KEY") || line.starts_with("#EXT-X-SESSION-KEY") {
        "key"
    } else if line.starts_with("#EXT-X-MAP") {
        "initialization"
    } else if line.starts_with("#EXT-X-MEDIA") {
        "rendition"
    } else if line.starts_with("#EXT-X-I-FRAME-STREAM-INF") {
        "nested_playlist"
    } else {
        "uri"
    }
}

fn dash_manifest_references(base_url: &Url, body: &str) -> Result<Vec<StreamManifestReference>> {
    let mut reader = Reader::from_str(body);
    reader.trim_text(true);
    let mut buf = Vec::new();
    let mut references = Vec::new();
    let mut current_text_element: Option<Vec<u8>> = None;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(event)) => {
                let name = event.name().as_ref().to_vec();
                collect_dash_attribute_references(base_url, &mut references, &event)?;
                if matches!(
                    name.as_slice(),
                    b"BaseURL" | b"Location" | b"SegmentURL" | b"Initialization"
                ) {
                    current_text_element = Some(name);
                }
            }
            Ok(Event::Empty(event)) => {
                collect_dash_attribute_references(base_url, &mut references, &event)?;
            }
            Ok(Event::Text(text)) => {
                if let Some(name) = current_text_element.as_deref() {
                    let raw = text.unescape().context("unescaping DASH manifest text")?;
                    let raw = raw.trim();
                    if !raw.is_empty() {
                        let kind = if name == b"BaseURL" {
                            "base_url"
                        } else if name == b"Location" {
                            "nested_manifest"
                        } else {
                            "segment"
                        };
                        references.push(manifest_reference_from_raw(base_url, raw, kind)?);
                    }
                }
            }
            Ok(Event::End(_)) => {
                current_text_element = None;
            }
            Ok(Event::Eof) => break,
            Err(err) => bail!("parsing DASH manifest failed: {err}"),
            _ => {}
        }
        buf.clear();
    }
    Ok(references)
}

fn collect_dash_attribute_references(
    base_url: &Url,
    references: &mut Vec<StreamManifestReference>,
    event: &quick_xml::events::BytesStart<'_>,
) -> Result<()> {
    for attr in event.attributes().with_checks(false) {
        let attr = attr.context("reading DASH manifest attribute")?;
        let key = attr.key.as_ref();
        if !matches!(
            key,
            b"media" | b"index" | b"sourceURL" | b"initialization" | b"href"
        ) {
            continue;
        }
        let value = attr
            .unescape_value()
            .context("unescaping DASH manifest attribute")?;
        let value = value.trim();
        if value.is_empty() || value.contains('$') {
            continue;
        }
        references.push(manifest_reference_from_raw(base_url, value, "segment")?);
    }
    Ok(())
}

fn manifest_reference_from_raw(
    base_url: &Url,
    raw: &str,
    kind: &str,
) -> Result<StreamManifestReference> {
    let value = raw.trim();
    if value.is_empty()
        || value.starts_with("data:")
        || value.starts_with("urn:")
        || value.starts_with("skd:")
    {
        bail!("unsupported stream manifest URL reference scheme");
    }
    let url = if value.starts_with("//") {
        Url::parse(&format!("{}:{value}", base_url.scheme()))?
    } else if value.starts_with("http://") || value.starts_with("https://") {
        Url::parse(value)?
    } else {
        base_url.join(value)?
    };
    let nested_manifest = kind == "nested_playlist"
        || kind == "nested_manifest"
        || looks_like_manifest_url(url.path());
    Ok(StreamManifestReference {
        kind: kind.to_string(),
        url,
        nested_manifest,
    })
}

fn looks_like_manifest_url(value: &str) -> bool {
    let path = value
        .split('?')
        .next()
        .unwrap_or(value)
        .split('#')
        .next()
        .unwrap_or(value)
        .to_ascii_lowercase();
    path.ends_with(".m3u8") || path.ends_with(".mpd")
}

#[derive(Debug, Clone, Default)]
struct FfmpegProgressState {
    out_time_seconds: Option<f64>,
    out_time_raw: Option<u64>,
    speed: Option<String>,
    output_bytes: Option<u64>,
}

impl FfmpegProgressState {
    async fn observe_line(
        &mut self,
        line: &str,
        partial_path: &Path,
    ) -> Result<Option<StreamRemuxProgress>> {
        let Some((key, value)) = line.split_once('=') else {
            return Ok(None);
        };
        let key = key.trim();
        let value = value.trim();
        match key {
            "out_time_ms" | "out_time_us" => {
                if let Ok(raw) = value.parse::<u64>() {
                    self.out_time_raw = Some(raw);
                    self.out_time_seconds = Some(raw as f64 / 1_000_000.0);
                }
            }
            "out_time" => {
                self.out_time_seconds = parse_ffmpeg_out_time(value);
            }
            "speed" => {
                if !value.is_empty() && value != "N/A" {
                    self.speed = Some(value.to_string());
                }
            }
            "total_size" => {
                self.output_bytes = value.parse::<u64>().ok();
            }
            "progress" => {}
            _ => return Ok(None),
        }
        self.current_progress(partial_path).await
    }

    async fn current_progress(&self, partial_path: &Path) -> Result<Option<StreamRemuxProgress>> {
        let file_bytes = fs::metadata(partial_path)
            .await
            .ok()
            .map(|metadata| metadata.len());
        if self.out_time_seconds.is_none()
            && self.speed.is_none()
            && self.output_bytes.is_none()
            && file_bytes.is_none()
        {
            return Ok(None);
        }
        Ok(Some(StreamRemuxProgress {
            out_time_seconds: self.out_time_seconds,
            out_time_raw: self.out_time_raw,
            speed: self.speed.clone(),
            output_bytes: self.output_bytes.or(file_bytes),
        }))
    }
}

fn parse_ffmpeg_out_time(value: &str) -> Option<f64> {
    let mut parts = value.split(':');
    let hours = parts.next()?.parse::<f64>().ok()?;
    let minutes = parts.next()?.parse::<f64>().ok()?;
    let seconds = parts.next()?.parse::<f64>().ok()?;
    Some((hours * 3600.0) + (minutes * 60.0) + seconds)
}

async fn read_limited_text<R>(mut reader: R, limit: usize) -> Result<String>
where
    R: AsyncRead + Unpin,
{
    let mut bytes = Vec::new();
    reader
        .read_to_end(&mut bytes)
        .await
        .context("reading ffmpeg stderr")?;
    if bytes.len() > limit {
        bytes = bytes[bytes.len().saturating_sub(limit)..].to_vec();
    }
    Ok(String::from_utf8_lossy(&bytes).to_string())
}

#[derive(Debug, Clone, Copy)]
struct FfprobeStreamFileProbe;

#[async_trait]
impl StreamFileProbe for FfprobeStreamFileProbe {
    async fn probe(&self, path: &Path) -> Result<ProbeEvidence> {
        let path = path
            .to_str()
            .ok_or_else(|| anyhow!("stream materializer path is not valid UTF-8"))?;
        let metadata = ffprobe::probe(path).await?;
        Ok(ProbeEvidence {
            container: metadata.container,
            video_codec: metadata.video_codec,
            audio_codec: metadata.audio_codec,
            width: metadata.width,
            height: metadata.height,
            duration_seconds: metadata.duration_seconds,
            streams: metadata
                .streams
                .iter()
                .map(ProbeStreamEvidence::from_ffprobe_stream)
                .collect(),
        })
    }
}

async fn persist_stream_release_file(
    pool: &AnyPool,
    pending: &PendingDirectFileJob,
    target_path: &Path,
    downloaded: u64,
    probe: &ProbeEvidence,
    verification: &StreamVerificationDecision,
    stream_type: StreamDeliveryType,
    parser_reason: &str,
    materializer_id: &str,
) -> Result<AcquisitionReleaseFile> {
    let candidate_id = stream_candidate_string(&pending.candidate, "/id");
    let target_path_string = target_path.to_string_lossy().to_string();
    let file = upsert_release_file(
        pool,
        NewAcquisitionReleaseFile {
            release_file_id: None,
            release_id: pending.release.release_id,
            file_index: Some(0),
            file_id: pending.job.download_id.clone(),
            provider_file_id: candidate_id,
            path: target_path_string.clone(),
            basename: target_path
                .file_name()
                .and_then(|value| value.to_str())
                .map(str::to_string),
            size_bytes: u64_to_i64(downloaded),
            selectable: true,
            selected: Some(true),
            parsed_title: stream_candidate_string(
                &pending.candidate,
                "/targetEvidence/episodeTitle",
            )
            .or_else(|| stream_candidate_string(&pending.candidate, "/title")),
            parsed_season_number: stream_candidate_i32(
                &pending.candidate,
                "/targetEvidence/seasonNumber",
            ),
            parsed_episode_number: stream_candidate_i32(
                &pending.candidate,
                "/targetEvidence/episodeNumber",
            ),
            parsed_episode_end_number: None,
            parsed_absolute_episode_number: stream_candidate_i32(
                &pending.candidate,
                "/targetEvidence/absoluteEpisodeNumber",
            ),
            parsed_absolute_episode_end_number: None,
            parsed_air_date: stream_candidate_string(&pending.candidate, "/targetEvidence/airDate"),
            parsed_quality: stream_candidate_string(&pending.candidate, "/quality").or_else(|| {
                stream_candidate_string(&pending.candidate, "/mediaEvidence/resolution")
            }),
            parsed_language: stream_candidate_string(&pending.candidate, "/language"),
            parsed_release_group: None,
            parser_confidence: pending.release.confidence,
            parser_reason: Some(parser_reason.to_string()),
            raw: Some(json!({
                "source": "http_stream_materializer",
                "streamType": stream_type.as_str(),
                "candidate": pending.candidate,
                "verification": verification.evidence,
            })),
            provider_metadata: Some(json!({
                "localPath": target_path_string,
                "downloadId": pending.job.download_id,
                "streamMaterializer": {
                    "runtimeVersion": STREAM_MATERIALIZER_VERSION,
                    "streamType": stream_type.as_str(),
                    "verifiedBy": materializer_id,
                    "probe": probe_evidence_json(probe),
                    "verification": verification.evidence
                }
            })),
        },
    )
    .await?;
    Ok(file)
}

async fn persist_stream_target_coverage(
    pool: &AnyPool,
    pending: &PendingDirectFileJob,
    release_file: &AcquisitionReleaseFile,
    reason_prefix: &str,
    verification: &StreamVerificationDecision,
) -> Result<bool> {
    let Some(subscription_id) = pending.release.subscription_id else {
        return Ok(false);
    };
    let targets = list_subscription_targets(pool, subscription_id).await?;
    let Some(target) =
        match_stream_candidate_target(pending.release.media_type, &pending.candidate, &targets)
    else {
        return Ok(false);
    };
    let confidence = if verification.state == StreamVerificationState::Verified {
        pending.release.confidence
    } else {
        ReleaseConfidence::ReviewRequired
    };
    let selected = confidence == ReleaseConfidence::High
        && verification.state == StreamVerificationState::Verified;
    let reason = if selected {
        format!("{reason_prefix}_exact_target_key_verified")
    } else if let Some(mismatch_class) = verification.mismatch_class.as_deref() {
        format!("{reason_prefix}_{mismatch_class}")
    } else {
        format!("{reason_prefix}_target_review_required")
    };
    upsert_release_coverage(
        pool,
        NewAcquisitionReleaseCoverage {
            coverage_id: None,
            release_id: pending.release.release_id,
            release_file_id: Some(release_file.release_file_id),
            target_id: target.target_id,
            coverage_kind: match pending.release.media_type {
                MediaType::Movie => ReleaseCoverageKind::Movie,
                MediaType::Series | MediaType::Anime => ReleaseCoverageKind::SingleEpisode,
            },
            confidence,
            score: pending.release.score,
            reason: Some(reason),
            state: if selected {
                ReleaseCoverageState::Selected
            } else {
                ReleaseCoverageState::ReviewRequired
            },
            verified_by: Some(reason_prefix.to_string()),
        },
    )
    .await?;
    Ok(selected)
}

fn match_stream_candidate_target<'a>(
    media_type: MediaType,
    candidate: &Value,
    targets: &'a [AcquisitionTarget],
) -> Option<&'a AcquisitionTarget> {
    let target_key = stream_candidate_string(candidate, "/targetEvidence/targetKey");
    if let Some(target_key) = target_key.as_deref()
        && let Some(target) = targets
            .iter()
            .find(|target| target.target_key.eq_ignore_ascii_case(target_key))
    {
        return Some(target);
    }
    if media_type == MediaType::Movie && targets.len() == 1 {
        return targets.first();
    }
    None
}

struct DbStreamRemuxProgressSink<'a> {
    pool: &'a AnyPool,
    pending: &'a PendingDirectFileJob,
    runtime: Value,
    partial_path: PathBuf,
    duration_seconds: Option<f64>,
}

#[async_trait]
impl StreamRemuxProgressSink for DbStreamRemuxProgressSink<'_> {
    async fn observe(&mut self, progress: StreamRemuxProgress) -> Result<StreamRemuxControl> {
        if job_cancelled(self.pool, self.pending.job.release_job_id).await? {
            return Ok(StreamRemuxControl::Cancel);
        }
        let output_bytes = progress.output_bytes.or_else(|| {
            std::fs::metadata(&self.partial_path)
                .ok()
                .map(|metadata| metadata.len())
        });
        let fraction = remux_progress_fraction(progress.out_time_seconds, self.duration_seconds);
        self.runtime = merge_runtime_object(
            self.runtime.clone(),
            json!({
                "runtimeState": "remuxing",
                "downloadedBytes": output_bytes,
                "totalBytes": Value::Null,
                "progress": fraction,
                "downloadRateBps": 0,
                "ffmpeg": remux_progress_json(Some(&progress), self.duration_seconds),
                "updatedAt": Utc::now()
            }),
        );
        update_stream_runtime(
            self.pool,
            &self.pending.release,
            &self.pending.job,
            AcquisitionReleaseState::Downloading,
            ReleaseJobState::Downloading,
            "HTTP stream is being stream-copied into acquisition staging.",
            true,
            None,
            self.runtime.clone(),
        )
        .await?;
        if job_cancelled(self.pool, self.pending.job.release_job_id).await? {
            return Ok(StreamRemuxControl::Cancel);
        }
        Ok(StreamRemuxControl::Continue)
    }
}

async fn update_stream_runtime(
    pool: &AnyPool,
    release: &AcquisitionRelease,
    job: &AcquisitionReleaseJob,
    release_state: AcquisitionReleaseState,
    job_state: ReleaseJobState,
    reason: &str,
    active: bool,
    completed_at: Option<chrono::DateTime<Utc>>,
    runtime: Value,
) -> Result<()> {
    let coverage_plan = merge_http_stream_runtime_evidence(release.coverage_plan.clone(), runtime);
    update_http_stream_release_state(
        pool,
        release.release_id,
        release_state,
        reason,
        Some(coverage_plan),
    )
    .await?;
    update_release_job_state(
        pool,
        job.release_job_id,
        ReleaseJobStateUpdate {
            state: job_state,
            state_reason: Some(reason.to_string()),
            active: Some(active),
            completed_at,
            ..Default::default()
        },
    )
    .await?;
    Ok(())
}

async fn mark_stream_cancelled(
    pool: &AnyPool,
    pending: &PendingDirectFileJob,
    runtime: &Value,
    partial_path: &Path,
) -> Result<()> {
    let reason = "HTTP stream materialization was cancelled.";
    let runtime = merge_runtime_object(
        runtime.clone(),
        json!({
            "runtimeState": "cancelled",
            "partialPathRemoved": true,
            "partialPath": partial_path.to_string_lossy(),
            "completedAt": Utc::now()
        }),
    );
    update_stream_runtime(
        pool,
        &pending.release,
        &pending.job,
        AcquisitionReleaseState::Cancelled,
        ReleaseJobState::Cancelled,
        reason,
        false,
        Some(Utc::now()),
        runtime,
    )
    .await
}

async fn fail_stream_job(
    pool: &AnyPool,
    pending: &PendingDirectFileJob,
    runtime: &Value,
    failure_class: &str,
    reason: &str,
    partial_path: Option<&Path>,
) -> Result<()> {
    if let Some(partial_path) = partial_path {
        let _ = fs::remove_file(partial_path).await;
    }
    let runtime = merge_runtime_object(
        runtime.clone(),
        json!({
            "runtimeState": "failed",
            "failureClass": failure_class,
            "message": reason,
            "completedAt": Utc::now()
        }),
    );
    if let Err(err) =
        record_source_module_materialization_failure(pool, pending, failure_class, reason, &runtime)
            .await
    {
        tracing::warn!(
            release_id = %pending.release.release_id,
            failure_class,
            error = %err,
            "failed to record stream source module materialization failure"
        );
    }
    update_stream_runtime(
        pool,
        &pending.release,
        &pending.job,
        AcquisitionReleaseState::Failed,
        ReleaseJobState::Failed,
        reason,
        false,
        Some(Utc::now()),
        runtime,
    )
    .await?;
    mark_stream_release_targets_failed(pool, pending, reason).await
}

async fn record_source_module_materialization_failure(
    pool: &AnyPool,
    pending: &PendingDirectFileJob,
    failure_class: &str,
    reason: &str,
    runtime: &Value,
) -> Result<()> {
    let Some(candidate) = pending.release.selected_candidate.as_ref() else {
        return Ok(());
    };
    let Some(module_id) = stream_candidate_string(candidate, "/sourceModule/id")
        .map(|value| stable_source_module_invocation_id(&value))
    else {
        return Ok(());
    };
    let store = ExtensionStore::new(pool);
    let modules = store.list_source_modules(None, None).await?;
    let Some(module) = modules
        .iter()
        .find(|module| stream_materializer_source_module_invocation_id(module) == module_id)
    else {
        return Ok(());
    };
    let versions = store
        .list_source_module_versions(module.source_module_id)
        .await?;
    let active_version = module
        .active_version
        .as_deref()
        .and_then(|active| versions.iter().find(|version| version.version == active))
        .or_else(|| {
            versions
                .iter()
                .find(|version| version.install_state == "active")
        })
        .or_else(|| {
            versions
                .iter()
                .find(|version| version.install_state == "installed")
        });
    let hoster_domain = stream_candidate_string(candidate, "/delivery/url")
        .and_then(|url| Url::parse(&url).ok())
        .and_then(|url| url.host_str().map(str::to_string));
    let media_type = Some(pending.release.media_type.as_str().to_string());
    let source_health = source_health_state_for_materialization_failure(failure_class);
    let severity = if source_health == "broken" {
        "error"
    } else {
        "warning"
    };
    let observed_at = Utc::now();
    store
        .record_source_module_quarantine(&NewExtensionSourceModuleQuarantine {
            quarantine_id: Uuid::new_v4(),
            source_module_id: module.source_module_id,
            source_module_version_id: active_version.map(|version| version.version_id),
            instance_id: module.instance_id,
            failure_class: failure_class.to_string(),
            hoster_domain: hoster_domain.clone(),
            candidate_fingerprint: Some(pending.release.fingerprint.clone()),
            media_type: media_type.clone(),
            reason: Some(reason.to_string()),
            evidence_json: Some(json!({
                "releaseId": pending.release.release_id,
                "releaseJobId": pending.job.release_job_id,
                "downloadId": pending.job.download_id,
                "moduleId": module_id,
                "hosterDomain": hoster_domain,
                "mediaType": media_type,
                "runtime": runtime,
            })),
            expires_at: Some(observed_at + chrono::Duration::hours(6)),
        })
        .await?;
    store
        .create_source_health_event(&NewExtensionSourceHealthEvent {
            health_event_id: Uuid::new_v4(),
            source_module_id: module.source_module_id,
            event_type: "materialization_failure".to_string(),
            state: source_health.to_string(),
            severity: severity.to_string(),
            reason: Some(reason.to_string()),
            evidence_json: Some(json!({
                "releaseId": pending.release.release_id,
                "releaseJobId": pending.job.release_job_id,
                "downloadId": pending.job.download_id,
                "moduleId": module_id,
                "failureClass": failure_class,
                "candidateFingerprint": pending.release.fingerprint,
            })),
            observed_at: Some(observed_at),
        })
        .await?;
    Ok(())
}

fn source_health_state_for_materialization_failure(failure_class: &str) -> &'static str {
    match failure_class {
        "account_required" | "provider_auth_missing" => "account_required",
        "source_returned_non_media_response"
        | "hoster_resolver_missing"
        | "captcha_or_browser_required"
        | "drm_or_license_required" => "unsupported",
        "protected_stream_egress_unavailable" => "degraded",
        _ => "broken",
    }
}

fn stream_materializer_source_module_invocation_id(
    module: &crate::extensions::store::ExtensionSourceModule,
) -> String {
    module
        .metadata_json
        .as_ref()
        .and_then(|metadata| {
            metadata
                .get("nuvio")
                .or_else(|| metadata.get("cloudstream"))
        })
        .and_then(|metadata| metadata.get("moduleId"))
        .and_then(Value::as_str)
        .map(stable_source_module_invocation_id)
        .unwrap_or_else(|| {
            module
                .module_key
                .rsplit(':')
                .next()
                .map(stable_source_module_invocation_id)
                .unwrap_or_else(|| stable_source_module_invocation_id(&module.display_name))
        })
}

fn stable_source_module_invocation_id(value: &str) -> String {
    let mut output = String::new();
    let mut last_dash = false;
    for ch in value.trim().chars() {
        if ch.is_ascii_alphanumeric() {
            output.push(ch.to_ascii_lowercase());
            last_dash = false;
        } else if !last_dash && !output.is_empty() {
            output.push('-');
            last_dash = true;
        }
    }
    while output.ends_with('-') {
        output.pop();
    }
    if output.is_empty() {
        "source".to_string()
    } else {
        output
    }
}

fn direct_file_non_media_response_reason(response: &DirectFileHttpResponse) -> Option<String> {
    let content_type = response
        .content_type
        .as_deref()
        .map(normalize_content_type)?;
    if direct_file_content_type_is_non_media(&content_type) {
        Some(format!(
            "Direct HTTP stream URL returned {content_type}; this looks like a hoster, login, or error page instead of a playable media file. The scraper must resolve a direct media URL before Elixir can materialize it."
        ))
    } else {
        None
    }
}

fn normalize_content_type(value: &str) -> String {
    value
        .split(';')
        .next()
        .unwrap_or(value)
        .trim()
        .to_ascii_lowercase()
}

fn direct_file_content_type_is_non_media(content_type: &str) -> bool {
    content_type == "text/html"
        || content_type.starts_with("text/")
        || matches!(
            content_type,
            "application/json"
                | "application/problem+json"
                | "application/xml"
                | "application/xhtml+xml"
                | "application/javascript"
                | "application/x-javascript"
        )
        || content_type.ends_with("+json")
        || content_type.ends_with("+xml")
}

async fn mark_stream_release_targets_failed(
    pool: &AnyPool,
    pending: &PendingDirectFileJob,
    reason: &str,
) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_release_coverage
         SET state = ?,
             reason = ?,
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = ?",
    )
    .bind(ReleaseCoverageState::Rejected.as_str())
    .bind(reason)
    .bind(pending.release.release_id.to_string())
    .execute(pool)
    .await
    .context("marking HTTP stream release coverage failed")?;

    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_targets
         SET state = ?,
             state_reason = ?,
             selected_route_logical_id = COALESCE(selected_route_logical_id, ?),
             download_id = COALESCE(download_id, ?),
             next_search_after = NULL,
             updated_at = CURRENT_TIMESTAMP
         WHERE state NOT IN ('imported', 'excluded')
           AND target_id IN (
               SELECT target_id
               FROM acquisition_release_coverage
               WHERE release_id = ?
           )",
    )
    .bind(crate::acquisition::subscriptions::AcquisitionTargetState::Blocked.as_str())
    .bind(reason)
    .bind(pending.job.route_logical_id.as_str())
    .bind(pending.job.download_id.as_deref())
    .bind(pending.release.release_id.to_string())
    .execute(pool)
    .await
    .context("marking HTTP stream release targets failed")?;

    if let Some(subscription_id) = pending.release.subscription_id {
        sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_targets
             SET state = ?,
                 state_reason = ?,
                 selected_route_logical_id = COALESCE(selected_route_logical_id, ?),
                 download_id = COALESCE(download_id, ?),
                 next_search_after = NULL,
                 updated_at = CURRENT_TIMESTAMP
             WHERE subscription_id = ?
               AND state NOT IN ('imported', 'excluded')
               AND NOT EXISTS (
                   SELECT 1
                   FROM acquisition_release_coverage
                   WHERE release_id = ?
               )",
        )
        .bind(crate::acquisition::subscriptions::AcquisitionTargetState::Blocked.as_str())
        .bind(reason)
        .bind(pending.job.route_logical_id.as_str())
        .bind(pending.job.download_id.as_deref())
        .bind(subscription_id.to_string())
        .bind(pending.release.release_id.to_string())
        .execute(pool)
        .await
        .context("marking fallback HTTP stream subscription targets failed")?;
    }
    Ok(())
}

async fn update_http_stream_release_state(
    pool: &AnyPool,
    release_id: Uuid,
    state: AcquisitionReleaseState,
    reason: &str,
    coverage_plan: Option<Value>,
) -> Result<()> {
    let coverage_plan_json = coverage_plan
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .context("serializing HTTP stream coverage plan")?;
    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_releases
         SET state = ?,
             state_reason = ?,
             coverage_plan_json = ?,
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = ?",
    )
    .bind(state.as_str())
    .bind(reason)
    .bind(coverage_plan_json.as_deref())
    .bind(release_id.to_string())
    .execute(pool)
    .await
    .context("updating HTTP stream acquisition release state")?;
    Ok(())
}

async fn job_cancelled(pool: &AnyPool, release_job_id: Uuid) -> Result<bool> {
    let row = sqlx::query(
        "SELECT state, active
         FROM acquisition_release_jobs
         WHERE release_job_id = ?
         LIMIT 1",
    )
    .bind(release_job_id.to_string())
    .fetch_optional(pool)
    .await
    .context("checking HTTP stream materializer cancellation")?;
    let Some(row) = row else {
        return Ok(true);
    };
    let state: String = row.try_get("state")?;
    let active = row_bool(&row, "active")?;
    Ok(!active || state == ReleaseJobState::Cancelled.as_str())
}

pub(crate) async fn cleanup_http_stream_partial_from_plan(
    downloads_root: &Path,
    coverage_plan: Option<&Value>,
) {
    let Some(partial_path) = coverage_plan
        .and_then(|plan| plan.get("streamRuntime"))
        .and_then(|runtime| runtime.get("partialPath"))
        .and_then(Value::as_str)
        .map(PathBuf::from)
    else {
        return;
    };
    let paths = MaterializerPaths::from_downloads_root(downloads_root.to_path_buf());
    cleanup_partial(&partial_path, &paths.staging_root).await;
}

async fn cleanup_materialized_file(target_path: &Path, staging_root: &Path) {
    cleanup_partial(target_path, staging_root).await;
}

async fn cleanup_partial(partial_path: &Path, staging_root: &Path) {
    if !path_is_under(partial_path, staging_root) {
        tracing::warn!(
            partial_path = %partial_path.display(),
            staging_root = %staging_root.display(),
            "refusing to remove HTTP stream partial outside staging root"
        );
        return;
    }
    let _ = fs::remove_file(partial_path).await;
}

fn path_is_under(path: &Path, root: &Path) -> bool {
    let path = absolutize_path(path);
    let root = absolutize_path(root);
    path.starts_with(root)
}

fn merge_http_stream_runtime_evidence(coverage_plan: Option<Value>, runtime: Value) -> Value {
    match coverage_plan {
        Some(Value::Object(mut object)) => {
            object.insert("streamRuntime".to_string(), runtime);
            Value::Object(object)
        }
        Some(value) => json!({
            "previousCoveragePlan": value,
            "streamRuntime": runtime
        }),
        None => json!({ "streamRuntime": runtime }),
    }
}

fn merge_runtime_object(existing: Value, update: Value) -> Value {
    let mut object = existing.as_object().cloned().unwrap_or_else(JsonMap::new);
    if let Value::Object(update) = update {
        for (key, value) in update {
            object.insert(key, value);
        }
    }
    Value::Object(object)
}

fn runtime_egress_with_final_url(runtime: &Value, final_url: Option<&str>) -> Value {
    let mut egress = runtime
        .get("egress")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_else(JsonMap::new);
    if let Some(final_url) = final_url
        && let Ok(url) = Url::parse(final_url)
    {
        egress.insert(
            "finalUrlScheme".to_string(),
            Value::String(url.scheme().to_string()),
        );
    }
    Value::Object(egress)
}

fn runtime_safe_stream_url(raw: &str) -> String {
    let Ok(mut url) = Url::parse(raw) else {
        return "[redacted-stream-url]".to_string();
    };
    if url.scheme() != "http" && url.scheme() != "https" {
        return format!("{}:[redacted]", url.scheme());
    }
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.set_query(None);
    url.set_fragment(None);
    url.to_string()
}

fn existing_stream_runtime(pending: &PendingDirectFileJob) -> Value {
    pending
        .release
        .coverage_plan
        .as_ref()
        .and_then(|plan| plan.get("streamRuntime"))
        .cloned()
        .unwrap_or_else(|| json!({}))
}

fn should_persist_progress(downloaded: u64, last_downloaded: u64, last_update: Instant) -> bool {
    downloaded.saturating_sub(last_downloaded) >= STREAM_MATERIALIZER_PROGRESS_BYTES
        || last_update.elapsed() >= STREAM_MATERIALIZER_PROGRESS_INTERVAL
}

fn base_stream_runtime(
    pending: &PendingDirectFileJob,
    stream_type: StreamDeliveryType,
    source_url: Option<&str>,
    header_names: Vec<String>,
    referer_applied: bool,
) -> Result<Value> {
    let download_id = pending
        .job
        .download_id
        .as_deref()
        .or(pending.release.download_id.as_deref())
        .ok_or_else(|| anyhow!("HTTP stream job is missing download id"))?;
    Ok(merge_runtime_object(
        existing_stream_runtime(pending),
        json!({
            "runtimeVersion": STREAM_MATERIALIZER_VERSION,
            "runtimeState": "materializing",
            "streamType": stream_type.as_str(),
            "downloadId": download_id,
            "sourceUrl": source_url.map(runtime_safe_stream_url),
            "redirectPolicy": {
                "maxRedirects": STREAM_MATERIALIZER_MAX_REDIRECTS,
                "validated": true,
                "scope": if stream_type == StreamDeliveryType::DirectFile { "download" } else { "manifest-preflight" }
            },
            "headerNames": header_names,
            "refererApplied": referer_applied,
            "copyMode": true,
            "transcodeAllowed": false
        }),
    ))
}

fn progress_fraction(downloaded: u64, total: Option<u64>) -> Option<f64> {
    let total = total?;
    if total == 0 {
        return None;
    }
    Some((downloaded as f64 / total as f64).clamp(0.0, 1.0))
}

fn remux_progress_fraction(
    out_time_seconds: Option<f64>,
    duration_seconds: Option<f64>,
) -> Option<f64> {
    let out_time_seconds = out_time_seconds?;
    let duration_seconds = duration_seconds?;
    if duration_seconds <= 0.0 {
        return None;
    }
    Some((out_time_seconds / duration_seconds).clamp(0.0, 1.0))
}

fn remux_progress_json(
    progress: Option<&StreamRemuxProgress>,
    duration_seconds: Option<f64>,
) -> Value {
    let Some(progress) = progress else {
        return Value::Null;
    };
    json!({
        "outTimeSeconds": progress.out_time_seconds,
        "outTimeRaw": progress.out_time_raw,
        "speed": progress.speed,
        "outputBytes": progress.output_bytes,
        "progress": remux_progress_fraction(progress.out_time_seconds, duration_seconds)
    })
}

fn choose_direct_file_name(candidate: &Value, response: &DirectFileHttpResponse) -> String {
    filename_from_content_disposition(response.content_disposition.as_deref())
        .or_else(|| filename_from_url(&response.final_url))
        .or_else(|| stream_candidate_string(candidate, "/title"))
        .map(|value| safe_file_name(&value))
        .filter(|value| !value.is_empty())
        .map(|value| ensure_media_extension(value, response.content_type.as_deref()))
        .unwrap_or_else(|| {
            ensure_media_extension(
                "http-stream-download".to_string(),
                response.content_type.as_deref(),
            )
        })
}

fn choose_stream_remux_file_name(candidate: &Value, stream_type: StreamDeliveryType) -> String {
    let base = stream_candidate_string(candidate, "/title")
        .or_else(|| stream_candidate_string(candidate, "/targetEvidence/episodeTitle"))
        .unwrap_or_else(|| format!("http-stream-{}", stream_type.as_str()));
    let mut name = safe_file_name(&base);
    if name.is_empty() {
        name = format!("http-stream-{}", stream_type.as_str());
    }
    let path = Path::new(&name);
    match path.extension().and_then(|value| value.to_str()) {
        Some(ext) if ext.eq_ignore_ascii_case("mkv") => name,
        Some(_) => format!(
            "{}.mkv",
            path.file_stem()
                .and_then(|value| value.to_str())
                .unwrap_or("http-stream-remux")
        ),
        None => format!("{name}.mkv"),
    }
}

fn filename_from_content_disposition(value: Option<&str>) -> Option<String> {
    let value = value?;
    for part in value.split(';').map(str::trim) {
        let Some((key, raw)) = part.split_once('=') else {
            continue;
        };
        let key = key.trim().to_ascii_lowercase();
        if key != "filename" && key != "filename*" {
            continue;
        }
        let mut raw = raw.trim().trim_matches('"').to_string();
        if key == "filename*"
            && let Some((_, encoded)) = raw.rsplit_once("''")
        {
            raw = encoded.to_string();
        }
        if let Ok(decoded) = urlencoding::decode(&raw) {
            raw = decoded.to_string();
        }
        if !raw.trim().is_empty() {
            return Some(raw);
        }
    }
    None
}

fn filename_from_url(value: &str) -> Option<String> {
    let url = Url::parse(value).ok()?;
    let segment = url.path_segments().and_then(|segments| {
        segments
            .filter(|segment| !segment.trim().is_empty())
            .next_back()
    })?;
    let decoded = urlencoding::decode(segment).ok()?;
    let decoded = decoded.trim();
    (!decoded.is_empty()).then(|| decoded.to_string())
}

fn ensure_media_extension(filename: String, content_type: Option<&str>) -> String {
    if Path::new(&filename).extension().is_some() {
        return filename;
    }
    let extension = content_type
        .map(|value| {
            value
                .split(';')
                .next()
                .unwrap_or(value)
                .trim()
                .to_ascii_lowercase()
        })
        .and_then(|content_type| match content_type.as_str() {
            "video/x-matroska" | "video/matroska" | "application/x-matroska" => Some("mkv"),
            "video/mp4" | "application/mp4" => Some("mp4"),
            "video/webm" => Some("webm"),
            "video/x-msvideo" => Some("avi"),
            "video/quicktime" => Some("mov"),
            "video/mp2t" => Some("ts"),
            "video/mpeg" => Some("mpg"),
            _ => None,
        })
        .unwrap_or("mp4");
    format!("{filename}.{extension}")
}

fn safe_file_name(value: &str) -> String {
    let mut output = String::new();
    for ch in value.trim().chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | ' ') {
            output.push(ch);
        } else {
            output.push('_');
        }
        if output.len() >= MAX_STREAM_FILE_NAME_LEN {
            break;
        }
    }
    output.trim().trim_matches('.').to_string()
}

fn safe_path_segment(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string()
}

async fn unique_target_path(dir: &Path, filename: &str) -> PathBuf {
    let initial = dir.join(filename);
    if !initial.exists() {
        return initial;
    }
    let path = Path::new(filename);
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("download");
    let ext = path.extension().and_then(|value| value.to_str());
    for idx in 1..1000 {
        let candidate = match ext {
            Some(ext) if !ext.is_empty() => dir.join(format!("{stem}-{idx}.{ext}")),
            _ => dir.join(format!("{stem}-{idx}")),
        };
        if !candidate.exists() {
            return candidate;
        }
    }
    dir.join(format!("{stem}-{}", Uuid::new_v4()))
}

impl ProbeEvidence {
    fn video_stream_count(&self) -> usize {
        self.streams
            .iter()
            .filter(|stream| stream.stream_type.as_deref() == Some("video"))
            .count()
            .max(if self.video_codec.is_some() { 1 } else { 0 })
    }

    fn audio_stream_count(&self) -> usize {
        self.streams
            .iter()
            .filter(|stream| stream.stream_type.as_deref() == Some("audio"))
            .count()
            .max(if self.audio_codec.is_some() { 1 } else { 0 })
    }

    fn subtitle_stream_count(&self) -> usize {
        self.streams
            .iter()
            .filter(|stream| stream.stream_type.as_deref() == Some("subtitle"))
            .count()
    }

    fn detected_audio_languages(&self) -> Vec<String> {
        self.streams
            .iter()
            .filter(|stream| stream.stream_type.as_deref() == Some("audio"))
            .filter_map(|stream| stream.normalized_language.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }
}

impl ProbeStreamEvidence {
    fn from_ffprobe_stream(stream: &ffprobe::Stream) -> Self {
        let language = stream_language_tag(stream);
        let title = stream_text_tag(stream, "title");
        let handler_name = stream_text_tag(stream, "handler_name");
        let normalized_language = language
            .as_deref()
            .and_then(normalize_stream_language_value)
            .or_else(|| {
                title
                    .as_deref()
                    .and_then(normalize_stream_language_freeform)
            })
            .or_else(|| {
                handler_name
                    .as_deref()
                    .and_then(normalize_stream_language_freeform)
            });
        Self {
            index: stream.index,
            stream_type: stream.codec_type.clone(),
            codec: stream.codec_name.clone(),
            width: stream.width,
            height: stream.height,
            channels: stream.channels,
            language,
            normalized_language,
            title,
            handler_name,
            default: stream
                .disposition
                .as_ref()
                .and_then(|disposition| disposition.default_flag)
                .unwrap_or_default()
                == 1,
            forced: stream
                .disposition
                .as_ref()
                .and_then(|disposition| disposition.forced)
                .unwrap_or_default()
                == 1,
        }
    }
}

fn stream_verification_evidence(
    state: StreamVerificationState,
    mismatch_class: Option<&str>,
    reasons: &[String],
    candidate: &Value,
    target_path: &Path,
    staging_root: &Path,
    materialized_bytes: u64,
    actual_file_size: Option<u64>,
    expected_runtime_seconds: Option<f64>,
    expected_resolution_height: Option<i32>,
    required_audio_languages: &[String],
    detected_audio_languages: &[String],
    probe: &ProbeEvidence,
    stream_type: StreamDeliveryType,
) -> Value {
    json!({
        "phase": "ess8",
        "runtimeVersion": STREAM_MATERIALIZER_VERSION,
        "verificationState": state.as_str(),
        "mismatchClass": mismatch_class,
        "reasons": reasons,
        "streamType": stream_type.as_str(),
        "localPath": target_path.to_string_lossy(),
        "pathUnderStagingRoot": path_is_under(target_path, staging_root),
        "materializedBytes": materialized_bytes,
        "fileSizeBytes": actual_file_size,
        "targetKey": stream_candidate_string(candidate, "/targetEvidence/targetKey"),
        "targetTitle": stream_candidate_string(candidate, "/targetEvidence/episodeTitle")
            .or_else(|| stream_candidate_string(candidate, "/targetEvidence/title")),
        "expectedRuntimeSeconds": expected_runtime_seconds,
        "actualDurationSeconds": probe.duration_seconds,
        "expectedResolutionHeight": expected_resolution_height,
        "actualResolutionHeight": probe.height,
        "requiredAudioLanguages": required_audio_languages,
        "detectedAudioLanguages": detected_audio_languages,
        "videoStreamCount": probe.video_stream_count(),
        "audioStreamCount": probe.audio_stream_count(),
        "subtitleStreamCount": probe.subtitle_stream_count(),
        "probe": probe_evidence_json(probe),
    })
}

fn probe_evidence_json(probe: &ProbeEvidence) -> Value {
    json!({
        "container": probe.container,
        "videoCodec": probe.video_codec,
        "audioCodec": probe.audio_codec,
        "width": probe.width,
        "height": probe.height,
        "durationSeconds": probe.duration_seconds,
        "videoStreamCount": probe.video_stream_count(),
        "audioStreamCount": probe.audio_stream_count(),
        "subtitleStreamCount": probe.subtitle_stream_count(),
        "streams": probe
            .streams
            .iter()
            .map(probe_stream_evidence_json)
            .collect::<Vec<_>>()
    })
}

fn probe_stream_evidence_json(stream: &ProbeStreamEvidence) -> Value {
    json!({
        "index": stream.index,
        "type": stream.stream_type,
        "codec": stream.codec,
        "width": stream.width,
        "height": stream.height,
        "channels": stream.channels,
        "language": stream.language,
        "normalizedLanguage": stream.normalized_language,
        "title": stream.title,
        "handlerName": stream.handler_name,
        "default": stream.default,
        "forced": stream.forced,
    })
}

fn media_type_segment(media_type: MediaType) -> &'static str {
    match media_type {
        MediaType::Movie => "movies",
        MediaType::Series => "series",
        MediaType::Anime => "anime",
    }
}

fn stream_candidate_string(candidate: &Value, pointer: &str) -> Option<String> {
    candidate
        .pointer(pointer)
        .and_then(|value| {
            value
                .as_str()
                .map(str::to_string)
                .or_else(|| value.as_i64().map(|value| value.to_string()))
                .or_else(|| value.as_u64().map(|value| value.to_string()))
        })
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn stream_candidate_bool(candidate: &Value, pointer: &str) -> Option<bool> {
    candidate.pointer(pointer).and_then(|value| {
        value.as_bool().or_else(|| {
            value.as_str().map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "y"
                )
            })
        })
    })
}

fn stream_candidate_i32(candidate: &Value, pointer: &str) -> Option<i32> {
    candidate.pointer(pointer).and_then(|value| {
        value
            .as_i64()
            .and_then(|value| i32::try_from(value).ok())
            .or_else(|| value.as_u64().and_then(|value| i32::try_from(value).ok()))
            .or_else(|| {
                value
                    .as_str()
                    .and_then(|value| value.trim().parse::<i32>().ok())
            })
    })
}

fn stream_candidate_f64(candidate: &Value, pointer: &str) -> Option<f64> {
    candidate.pointer(pointer).and_then(|value| {
        value
            .as_f64()
            .or_else(|| value.as_i64().map(|value| value as f64))
            .or_else(|| value.as_u64().map(|value| value as f64))
            .or_else(|| {
                value
                    .as_str()
                    .and_then(|value| value.trim().parse::<f64>().ok())
            })
    })
}

fn expected_stream_runtime_seconds(candidate: &Value) -> Option<f64> {
    [
        "/targetEvidence/runtimeSeconds",
        "/targetEvidence/durationSeconds",
        "/mediaEvidence/runtimeSeconds",
        "/mediaEvidence/durationSeconds",
        "/runtimeSeconds",
        "/durationSeconds",
    ]
    .iter()
    .filter_map(|pointer| stream_candidate_f64(candidate, pointer))
    .find(|value| *value > 0.0)
}

fn expected_stream_resolution_height(candidate: &Value) -> Option<i32> {
    stream_candidate_i32(candidate, "/mediaEvidence/resolution")
        .or_else(|| stream_candidate_i32(candidate, "/mediaEvidence/height"))
        .or_else(|| stream_candidate_i32(candidate, "/delivery/resolution"))
        .or_else(|| stream_candidate_i32(candidate, "/delivery/height"))
        .or_else(|| {
            stream_candidate_string(candidate, "/quality")
                .as_deref()
                .and_then(resolution_height_from_quality)
        })
}

fn resolution_height_from_quality(value: &str) -> Option<i32> {
    let normalized = value.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return None;
    }
    if normalized.contains("2160") || normalized.contains("4k") || normalized.contains("uhd") {
        return Some(2160);
    }
    if normalized.contains("1080") || normalized.contains("fhd") {
        return Some(1080);
    }
    if normalized.contains("720") || normalized == "hd" {
        return Some(720);
    }
    if normalized.contains("576") {
        return Some(576);
    }
    if normalized.contains("480") || normalized == "sd" {
        return Some(480);
    }
    normalized
        .split(|ch: char| !ch.is_ascii_digit())
        .filter_map(|token| token.parse::<i32>().ok())
        .find(|value| matches!(*value, 2160 | 1440 | 1080 | 720 | 576 | 540 | 480 | 360))
}

fn duration_is_sane_for_target(expected_seconds: f64, actual_seconds: f64) -> bool {
    if expected_seconds <= 0.0 || actual_seconds <= 0.0 {
        return false;
    }
    let (lower_ratio, upper_ratio) = if expected_seconds < 600.0 {
        (0.35, 3.0)
    } else if expected_seconds < 2700.0 {
        (0.55, 1.75)
    } else {
        (0.65, 1.45)
    };
    let lower = (expected_seconds * lower_ratio).max(60.0);
    let upper = (expected_seconds * upper_ratio) + 300.0;
    actual_seconds >= lower && actual_seconds <= upper
}

fn required_stream_audio_languages(candidate: &Value) -> Vec<String> {
    let mut languages = BTreeSet::new();
    for pointer in [
        "/language",
        "/audioLanguage",
        "/audioLanguages",
        "/mediaEvidence/audioLanguage",
        "/mediaEvidence/audioLanguages",
        "/targetEvidence/requiredAudioLanguage",
        "/targetEvidence/requiredAudioLanguages",
        "/preferences/requiredLanguages",
        "/raw/serverEvidence/languagePreference/matchingAudioLanguages",
    ] {
        if let Some(value) = candidate.pointer(pointer) {
            for language in json_language_values(value) {
                if let Some(normalized) = normalize_stream_language_value(&language) {
                    languages.insert(normalized);
                }
            }
        }
    }
    if candidate
        .pointer("/raw/serverEvidence/languagePreference/requiresReview")
        .and_then(Value::as_bool)
        == Some(true)
    {
        if let Some(value) =
            candidate.pointer("/raw/serverEvidence/languagePreference/desiredAudioLanguages")
        {
            for language in json_language_values(value) {
                if let Some(normalized) = normalize_stream_language_value(&language) {
                    languages.insert(normalized);
                }
            }
        }
    }
    languages.into_iter().collect()
}

fn strict_language_preference_review_reason(
    candidate: &Value,
    required_audio_languages: &[String],
    probe: &ProbeEvidence,
) -> Option<String> {
    let evidence = candidate.pointer("/raw/serverEvidence/languagePreference")?;
    if evidence.get("mode").and_then(Value::as_str) != Some("require_review")
        || evidence.get("requiresReview").and_then(Value::as_bool) != Some(true)
    {
        return None;
    }
    if required_audio_languages.is_empty() {
        return Some(
            "language could not be confirmed by source evidence for a strict language preference"
                .to_string(),
        );
    }
    if probe.audio_stream_count() == 0 {
        return None;
    }
    let detected = probe.detected_audio_languages();
    if detected.is_empty() {
        return Some(format!(
            "ffprobe could not confirm desired audio language {}",
            format_language_list(required_audio_languages.iter())
        ));
    }
    if required_audio_languages
        .iter()
        .any(|language| detected.contains(language))
    {
        return None;
    }
    None
}

fn json_language_values(value: &Value) -> Vec<String> {
    if let Some(raw) = value.as_str() {
        return split_language_list(raw);
    }
    if let Some(values) = value.as_array() {
        return values
            .iter()
            .flat_map(json_language_values)
            .collect::<Vec<_>>();
    }
    if let Some(object) = value.as_object() {
        return ["language", "name", "value", "code", "id"]
            .iter()
            .filter_map(|key| object.get(*key))
            .flat_map(json_language_values)
            .collect();
    }
    Vec::new()
}

fn split_language_list(raw: &str) -> Vec<String> {
    raw.split(|ch: char| matches!(ch, ',' | ';' | '/' | '|'))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn stream_language_tag(stream: &ffprobe::Stream) -> Option<String> {
    stream_text_tag(stream, "language")
        .or_else(|| stream_text_tag(stream, "LANGUAGE"))
        .or_else(|| stream_text_tag(stream, "lang"))
}

fn stream_text_tag(stream: &ffprobe::Stream, key: &str) -> Option<String> {
    stream
        .tags
        .as_ref()
        .and_then(|tags| {
            tags.iter()
                .find(|(tag_key, _)| tag_key.eq_ignore_ascii_case(key))
                .map(|(_, value)| value.clone())
        })
        .and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        })
}

fn normalize_stream_language_freeform(raw: &str) -> Option<String> {
    normalize_stream_language_value(raw).or_else(|| {
        raw.split(|ch: char| !ch.is_ascii_alphanumeric())
            .filter_map(normalize_stream_language_name_or_three_letter)
            .next()
    })
}

fn normalize_stream_language_value(raw: &str) -> Option<String> {
    let value = raw.trim();
    if value.is_empty() {
        return None;
    }
    let normalized = value.replace('_', "-");
    let first = normalized
        .split('-')
        .find(|part| !part.trim().is_empty())?
        .trim()
        .to_ascii_lowercase();
    normalize_stream_language_token(&first)
}

fn normalize_stream_language_name_or_three_letter(token: &str) -> Option<String> {
    let token = token.trim().to_ascii_lowercase();
    if token.len() == 3 && token.chars().all(|ch| ch.is_ascii_alphabetic()) {
        return map_stream_three_letter_language(&token).map(ToString::to_string);
    }
    map_stream_language_name(&token).map(ToString::to_string)
}

fn normalize_stream_language_token(token: &str) -> Option<String> {
    let token = token.trim().to_ascii_lowercase();
    if matches!(
        token.as_str(),
        "" | "und"
            | "undefined"
            | "unknown"
            | "unk"
            | "mul"
            | "multi"
            | "dual"
            | "dual-audio"
            | "original"
            | "sub"
            | "subs"
            | "subtitle"
            | "subbed"
            | "dub"
            | "dubbed"
    ) {
        return None;
    }
    if token.len() == 2 && token.chars().all(|ch| ch.is_ascii_alphabetic()) {
        return Some(token);
    }
    if token.len() == 3 && token.chars().all(|ch| ch.is_ascii_alphabetic()) {
        return map_stream_three_letter_language(&token)
            .map(ToString::to_string)
            .or(Some(token));
    }
    map_stream_language_name(&token).map(ToString::to_string)
}

fn map_stream_three_letter_language(token: &str) -> Option<&'static str> {
    match token {
        "eng" => Some("en"),
        "spa" => Some("es"),
        "fra" | "fre" => Some("fr"),
        "deu" | "ger" => Some("de"),
        "ita" => Some("it"),
        "por" => Some("pt"),
        "nld" | "dut" => Some("nl"),
        "rus" => Some("ru"),
        "jpn" => Some("ja"),
        "kor" => Some("ko"),
        "zho" | "chi" => Some("zh"),
        "ara" => Some("ar"),
        "heb" => Some("he"),
        "hin" => Some("hi"),
        "tur" => Some("tr"),
        "pol" => Some("pl"),
        "ukr" => Some("uk"),
        "swe" => Some("sv"),
        "fin" => Some("fi"),
        "dan" => Some("da"),
        "nor" => Some("no"),
        "ron" | "rum" => Some("ro"),
        "ell" | "gre" => Some("el"),
        "ces" | "cze" => Some("cs"),
        "hun" => Some("hu"),
        "tha" => Some("th"),
        "vie" => Some("vi"),
        "ind" => Some("id"),
        "msa" | "may" => Some("ms"),
        "fas" | "per" => Some("fa"),
        "urd" => Some("ur"),
        "tam" => Some("ta"),
        "tel" => Some("te"),
        "ben" => Some("bn"),
        "mar" => Some("mr"),
        "lit" => Some("lt"),
        "lav" => Some("lv"),
        "est" => Some("et"),
        "slv" => Some("sl"),
        "slk" | "slo" => Some("sk"),
        "hrv" => Some("hr"),
        "srp" => Some("sr"),
        "bul" => Some("bg"),
        "isl" | "ice" => Some("is"),
        "gle" => Some("ga"),
        "kat" | "geo" => Some("ka"),
        "kaz" => Some("kk"),
        "tgl" => Some("tl"),
        _ => None,
    }
}

fn map_stream_language_name(token: &str) -> Option<&'static str> {
    match token {
        "english" => Some("en"),
        "spanish" | "espanol" | "castellano" => Some("es"),
        "french" => Some("fr"),
        "german" => Some("de"),
        "italian" => Some("it"),
        "portuguese" => Some("pt"),
        "russian" => Some("ru"),
        "japanese" => Some("ja"),
        "korean" => Some("ko"),
        "chinese" | "mandarin" | "cantonese" => Some("zh"),
        "dutch" => Some("nl"),
        "swedish" => Some("sv"),
        "norwegian" => Some("no"),
        "danish" => Some("da"),
        "finnish" => Some("fi"),
        "polish" => Some("pl"),
        "turkish" => Some("tr"),
        "arabic" => Some("ar"),
        "hebrew" => Some("he"),
        "greek" => Some("el"),
        "czech" => Some("cs"),
        "hungarian" => Some("hu"),
        "thai" => Some("th"),
        "vietnamese" => Some("vi"),
        "indonesian" => Some("id"),
        "malay" => Some("ms"),
        "persian" => Some("fa"),
        "hindi" => Some("hi"),
        "ukrainian" => Some("uk"),
        "romanian" => Some("ro"),
        "latino" => Some("es"),
        _ => None,
    }
}

fn format_language_list<'a>(languages: impl Iterator<Item = &'a String>) -> String {
    let values = languages.cloned().collect::<Vec<_>>();
    if values.is_empty() {
        "[]".to_string()
    } else {
        values.join(", ")
    }
}

fn unsupported_stream_materialization_feature(
    candidate: &Value,
    stream_type: StreamDeliveryType,
) -> Option<(&'static str, String)> {
    if stream_feature_is_truthy(
        candidate,
        &[
            "/delivery/browserRequired",
            "/delivery/requiresBrowser",
            "/raw/browserRequired",
            "/raw/requiresBrowser",
        ],
    ) {
        return Some((
            "browser_required",
            "HTTP stream candidate requires browser automation, which Elixir acquisition does not perform.".to_string(),
        ));
    }
    if stream_feature_is_truthy(
        candidate,
        &[
            "/delivery/captchaRequired",
            "/delivery/requiresCaptcha",
            "/raw/captchaRequired",
            "/raw/requiresCaptcha",
        ],
    ) {
        return Some((
            "browser_required",
            "HTTP stream candidate requires captcha solving, which Elixir acquisition does not perform.".to_string(),
        ));
    }
    if stream_feature_is_truthy(
        candidate,
        &[
            "/delivery/drm",
            "/delivery/drmRequired",
            "/delivery/licenseRequired",
            "/delivery/needsLicense",
            "/delivery/encrypted",
            "/delivery/licenseUrl",
            "/mediaEvidence/drm",
            "/mediaEvidence/encrypted",
            "/raw/drm",
            "/raw/requiresDrm",
            "/raw/licenseRequired",
            "/raw/licenseUrl",
            "/raw/protection",
            "/raw/encrypted",
        ],
    ) {
        return Some((
            "unsupported_drm",
            format!(
                "{} candidate requires DRM/license/encrypted stream handling, which Elixir acquisition will not bypass.",
                stream_type.materializer_label()
            ),
        ));
    }
    if stream_type == StreamDeliveryType::Dash
        && stream_feature_is_truthy(
            candidate,
            &[
                "/delivery/keySystem",
                "/delivery/contentProtection",
                "/mediaEvidence/keySystem",
                "/raw/keySystem",
                "/raw/contentProtection",
            ],
        )
    {
        return Some((
            "unsupported_drm",
            "DASH candidate includes content-protection evidence and cannot be materialized."
                .to_string(),
        ));
    }
    None
}

fn stream_feature_is_truthy(candidate: &Value, pointers: &[&str]) -> bool {
    pointers.iter().any(|pointer| {
        let Some(value) = candidate.pointer(pointer) else {
            return false;
        };
        match value {
            Value::Bool(value) => *value,
            Value::Number(value) => value.as_i64().unwrap_or_default() != 0,
            Value::String(value) => {
                let value = value.trim().to_ascii_lowercase();
                !value.is_empty()
                    && !matches!(
                        value.as_str(),
                        "false" | "0" | "no" | "none" | "clear" | "unencrypted" | "not_required"
                    )
            }
            Value::Array(values) => values.iter().any(|value| match value {
                Value::Bool(value) => *value,
                Value::String(value) => {
                    let value = value.trim().to_ascii_lowercase();
                    !value.is_empty()
                        && !matches!(
                            value.as_str(),
                            "false" | "0" | "no" | "none" | "clear" | "unencrypted"
                        )
                }
                _ => false,
            }),
            Value::Object(object) => !object.is_empty(),
            Value::Null => false,
        }
    })
}

fn u64_to_i64(value: u64) -> Option<i64> {
    i64::try_from(value).ok()
}

fn row_bool(row: &sqlx::any::AnyRow, column: &str) -> Result<bool> {
    if let Ok(value) = row.try_get::<bool, _>(column) {
        return Ok(value);
    }
    if let Ok(value) = row.try_get::<i64, _>(column) {
        return Ok(value != 0);
    }
    if let Ok(value) = row.try_get::<String, _>(column) {
        return Ok(matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "t" | "yes"
        ));
    }
    Ok(false)
}

fn absolutize_path(path: &Path) -> PathBuf {
    if path.is_absolute() {
        return path.to_path_buf();
    }
    std::env::current_dir()
        .map(|cwd| cwd.join(path))
        .unwrap_or_else(|_| path.to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        acquisition::{
            imports::{
                AcquisitionImportFileLinkState, AcquisitionImportRunState,
                get_import_run_by_release_job, list_import_file_links,
                list_import_pending_release_jobs, run_acquisition_import_iteration,
            },
            release_resolution::{
                models::{
                    NewAcquisitionRelease, NewAcquisitionReleaseJob, ReleaseKind,
                    ReleaseResolverKind,
                },
                store::{
                    get_release, get_release_by_download_id, list_release_files, upsert_release,
                    upsert_release_job,
                },
            },
            subscriptions::{
                AcquisitionMonitorPolicy, AcquisitionRoutePolicy, AcquisitionTargetState,
                NewAcquisitionSubscription, NewAcquisitionTarget, create_subscription,
                upsert_subscription_targets,
            },
        },
        config::DatabaseConfig,
        db::{
            Database,
            models::{ExtensionKind, ExtensionTrustLevel, SlotCardinality},
        },
        extensions::store::{NewExtension, NewExtensionInstance, NewProvider},
        orchestrator::planner::stable_provider_id,
    };
    use axum::{Json, Router, extract::State, http::StatusCode as AxumStatusCode, routing::post};
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use tempfile::TempDir;
    use tokio::net::TcpListener;
    use tokio::sync::Notify;

    #[derive(Clone)]
    struct FakeDirectFileClient {
        chunks: Vec<Vec<u8>>,
        content_length: Option<u64>,
        content_type: Option<String>,
        content_disposition: Option<String>,
        observed_headers: Arc<tokio::sync::Mutex<Vec<(String, String)>>>,
        observed_referer: Arc<tokio::sync::Mutex<Option<String>>>,
        block_on_second_chunk: Option<Arc<Notify>>,
    }

    #[async_trait]
    impl DirectFileHttpClient for FakeDirectFileClient {
        async fn open(&self, request: DirectFileDownloadRequest) -> Result<DirectFileHttpResponse> {
            *self.observed_headers.lock().await = request.headers.clone();
            *self.observed_referer.lock().await = request.referer.clone();
            Ok(DirectFileHttpResponse {
                final_url: request.url.to_string(),
                content_length: self.content_length,
                content_type: self.content_type.clone(),
                content_disposition: self.content_disposition.clone(),
                body: Box::new(FakeDirectFileBody {
                    chunks: self.chunks.clone(),
                    index: 0,
                    block_on_second_chunk: self.block_on_second_chunk.clone(),
                }),
            })
        }
    }

    #[derive(Clone, Default)]
    struct DowngradeRedirectDirectFileClient {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl DirectFileHttpClient for DowngradeRedirectDirectFileClient {
        async fn open(
            &self,
            _request: DirectFileDownloadRequest,
        ) -> Result<DirectFileHttpResponse> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(HttpStreamDowngradeRedirect {
                from_scheme: "https".to_string(),
                to_scheme: "http".to_string(),
            }
            .into())
        }
    }

    #[derive(Clone, Default)]
    struct FixedStreamEgressClassifier {
        direct_route: Option<StreamEgressRoute>,
        remux_route: Option<StreamEgressRoute>,
    }

    #[async_trait]
    impl StreamEgressClassifier for FixedStreamEgressClassifier {
        async fn classify_direct_file(
            &self,
            policy: StreamHttpEgressPolicy,
            request: &DirectFileDownloadRequest,
        ) -> Result<StreamEgressRoute> {
            match self.direct_route.clone() {
                Some(route) => Ok(route),
                None => classify_initial_stream_url(
                    policy,
                    &request.url,
                    StreamDeliveryType::DirectFile,
                ),
            }
        }

        async fn classify_remux_stream(
            &self,
            policy: StreamHttpEgressPolicy,
            request: &StreamRemuxRequest,
        ) -> Result<StreamEgressRoute> {
            match self.remux_route.clone() {
                Some(route) => Ok(route),
                None => classify_initial_stream_url(policy, &request.url, request.stream_type),
            }
        }
    }

    struct FakeDirectFileBody {
        chunks: Vec<Vec<u8>>,
        index: usize,
        block_on_second_chunk: Option<Arc<Notify>>,
    }

    #[async_trait]
    impl DirectFileBody for FakeDirectFileBody {
        async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>> {
            if self.index == 1
                && let Some(block) = self.block_on_second_chunk.take()
            {
                block.notified().await;
            }
            let Some(chunk) = self.chunks.get(self.index).cloned() else {
                return Ok(None);
            };
            self.index += 1;
            Ok(Some(chunk))
        }
    }

    #[derive(Clone)]
    struct FakeProbe {
        called: Arc<AtomicBool>,
        evidence: ProbeEvidence,
    }

    #[async_trait]
    impl StreamFileProbe for FakeProbe {
        async fn probe(&self, path: &Path) -> Result<ProbeEvidence> {
            self.called.store(true, Ordering::SeqCst);
            let metadata = fs::metadata(path).await?;
            if metadata.len() == 0 {
                bail!("empty file");
            }
            Ok(self.evidence.clone())
        }
    }

    #[derive(Clone, Default)]
    struct FakeStreamRemuxer {
        observed: Arc<tokio::sync::Mutex<Vec<StreamRemuxRequest>>>,
        fail_message: Option<String>,
        progress: Vec<StreamRemuxProgress>,
    }

    #[async_trait]
    impl StreamRemuxer for FakeStreamRemuxer {
        async fn remux(
            &self,
            request: StreamRemuxRequest,
            progress: &mut dyn StreamRemuxProgressSink,
        ) -> Result<StreamRemuxResult> {
            self.observed.lock().await.push(request.clone());
            if let Some(message) = self.fail_message.as_deref() {
                bail!("{message}");
            }
            fs::write(&request.partial_path, b"remuxed-stream-bytes")
                .await
                .context("writing fake remux output")?;
            let mut final_progress = None;
            for observed in &self.progress {
                final_progress = Some(observed.clone());
                if progress.observe(observed.clone()).await? == StreamRemuxControl::Cancel {
                    bail!("fake remux cancelled");
                }
            }
            let output_bytes = fs::metadata(&request.partial_path).await?.len();
            Ok(StreamRemuxResult {
                final_url: Some(request.url.to_string()),
                output_bytes,
                final_progress,
                stderr_tail: None,
            })
        }
    }

    #[derive(Clone, Default)]
    struct FakeProtectedStreamMaterializer {
        direct_calls: Arc<tokio::sync::Mutex<Vec<ProtectedDirectFileRequest>>>,
        remux_calls: Arc<tokio::sync::Mutex<Vec<ProtectedRemuxRequest>>>,
    }

    #[async_trait]
    impl ProtectedStreamMaterializer for FakeProtectedStreamMaterializer {
        async fn materialize_direct_file(
            &self,
            request: ProtectedDirectFileRequest,
        ) -> Result<ProtectedDirectFileResult> {
            self.direct_calls.lock().await.push(request.clone());
            fs::write(&request.partial_path, b"protected-video-bytes").await?;
            Ok(ProtectedDirectFileResult {
                final_url: request.download.url.to_string(),
                content_length: Some(21),
                content_type: Some("video/mp4".to_string()),
                content_disposition: None,
                downloaded_bytes: 21,
                worker_runtime_id: Some("fake-protected-worker".to_string()),
                stderr_tail: None,
            })
        }

        async fn remux_stream(
            &self,
            request: ProtectedRemuxRequest,
            progress: &mut dyn StreamRemuxProgressSink,
        ) -> Result<ProtectedRemuxResult> {
            self.remux_calls.lock().await.push(request.clone());
            fs::write(&request.remux.partial_path, b"protected-remux-bytes").await?;
            let final_progress = StreamRemuxProgress {
                out_time_seconds: request.remux.duration_seconds,
                out_time_raw: None,
                speed: Some("5.0x".to_string()),
                output_bytes: Some(21),
            };
            if progress.observe(final_progress.clone()).await? == StreamRemuxControl::Cancel {
                bail!("fake protected remux cancelled");
            }
            Ok(ProtectedRemuxResult {
                remux: StreamRemuxResult {
                    final_url: Some(request.remux.url.to_string()),
                    output_bytes: 21,
                    final_progress: Some(final_progress),
                    stderr_tail: None,
                },
                worker_runtime_id: Some("fake-protected-worker".to_string()),
            })
        }
    }

    #[derive(Clone, Default)]
    struct FakeLateResolver {
        calls: Arc<tokio::sync::Mutex<Vec<Value>>>,
        result: Option<StreamCandidateResolveResult>,
        fail_message: Option<String>,
    }

    #[async_trait]
    impl StreamCandidateLateResolver for FakeLateResolver {
        async fn resolve(
            &self,
            _pool: &AnyPool,
            pending: &PendingDirectFileJob,
        ) -> Result<StreamCandidateResolveResult> {
            self.calls.lock().await.push(pending.candidate.clone());
            if let Some(message) = self.fail_message.as_deref() {
                bail!("{message}");
            }
            self.result
                .clone()
                .ok_or_else(|| anyhow!("fake late resolver was called unexpectedly"))
        }
    }

    #[derive(Clone)]
    struct LateResolveFixtureState {
        requests: Arc<Mutex<Vec<Value>>>,
        response: Value,
    }

    async fn start_late_resolve_provider_fixture(
        response: Value,
    ) -> Result<(u16, Arc<Mutex<Vec<Value>>>, tokio::task::JoinHandle<()>)> {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let state = LateResolveFixtureState {
            requests: Arc::clone(&requests),
            response,
        };
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let app = Router::new()
            .route("/stream-provider/resolve", post(late_resolve_fixture))
            .with_state(state);
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("test server");
        });
        Ok((port, requests, handle))
    }

    async fn late_resolve_fixture(
        State(state): State<LateResolveFixtureState>,
        Json(payload): Json<Value>,
    ) -> (AxumStatusCode, Json<Value>) {
        state.requests.lock().expect("requests lock").push(payload);
        (AxumStatusCode::OK, Json(state.response.clone()))
    }

    async fn setup_db() -> Result<Database> {
        let database = Database::connect(&DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        })
        .await?;
        database.run_migrations().await?;
        Ok(database)
    }

    async fn seed_active_wireguard_stream_profile(pool: &AnyPool) -> Result<()> {
        sqlx::query(
            "INSERT INTO download_network_profiles (id, name, kind, enabled, strict, scope, provider, gateway_runtime, config_json, status, active) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind("hse-wireguard")
        .bind("HSE WireGuard")
        .bind("wireguard_config")
        .bind(true)
        .bind(true)
        .bind("managed_downloaders")
        .bind("fixture-vpn")
        .bind("gluetun_wireguard")
        .bind("{}")
        .bind("ready")
        .bind(true)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO download_network_profile_secrets (profile_id, key, secret_ref) VALUES (?, ?, ?)",
        )
        .bind("hse-wireguard")
        .bind("wireguard_config")
        .bind("global:wireguard_config")
        .execute(pool)
        .await?;
        Ok(())
    }

    fn direct_file_candidate() -> Value {
        json!({
            "candidateKind": "stream",
            "id": "fixture-source:show-1:episode-2:cdn",
            "title": "Fixture Show - S01E02 - 1080p",
            "source": "https://stream.example.test/show/s01e02",
            "sourceKind": "http_file",
            "quality": "1080p",
            "language": "jpn",
            "score": 88.0,
            "supportedRoutes": [HTTP_STREAM_DEFAULT_LOGICAL_ID],
            "defaultRoute": HTTP_STREAM_DEFAULT_LOGICAL_ID,
            "targetEvidence": {
                "mediaType": "anime",
                "targetKey": "S01E02",
                "seasonNumber": 1,
                "episodeNumber": 2,
                "absoluteEpisodeNumber": 2,
                "episodeTitle": "The First Day",
                "confidence": "high",
                "reasons": ["provider episode id matched target"]
            },
            "delivery": {
                "streamType": "direct_file",
                "url": "https://cdn.example.test/show/s01e02/file",
                "referer": "https://stream.example.test/show/s01e02",
                "headers": {
                    "user-agent": "ElixirTest/1.0",
                    "x-source-token": "redacted"
                }
            },
            "sourceModule": {
                "id": "fixture.cloudstream",
                "name": "Fixture CloudStream",
                "type": "cloudstream"
            },
            "raw": {}
        })
    }

    fn hls_candidate() -> Value {
        let mut candidate = direct_file_candidate();
        candidate["id"] = json!("fixture-source:show-1:episode-2:hls");
        candidate["source"] = json!("https://stream.example.test/show/s01e02/hls");
        candidate["sourceKind"] = json!("http_stream");
        candidate["delivery"]["streamType"] = json!("hls");
        candidate["delivery"]["url"] = json!("https://cdn.example.test/show/s01e02/master.m3u8");
        candidate["targetEvidence"]["runtimeSeconds"] = json!(1440);
        candidate
    }

    fn movie_direct_file_candidate() -> Value {
        json!({
            "candidateKind": "stream",
            "id": "fixture-cloudstream-movie-direct:northman:movie:direct",
            "title": "The Northman (2022) - 1080p",
            "source": "https://stream.example.test/movies/the-northman",
            "sourceKind": "http_file",
            "quality": "1080p",
            "language": "eng",
            "score": 94.0,
            "supportedRoutes": [HTTP_STREAM_DEFAULT_LOGICAL_ID],
            "defaultRoute": HTTP_STREAM_DEFAULT_LOGICAL_ID,
            "targetEvidence": {
                "mediaType": "movie",
                "targetKey": "movie",
                "confidence": "high",
                "reasons": ["provider movie id matched requested movie target"]
            },
            "delivery": {
                "streamType": "direct_file",
                "url": "https://cdn.example.test/movies/the-northman/file.mp4",
                "referer": "https://stream.example.test/movies/the-northman",
                "headers": {
                    "user-agent": "ElixirCloudStreamFixture/1.0"
                }
            },
            "mediaEvidence": {
                "resolution": 1080,
                "audioLanguages": ["eng"],
                "subtitleLanguages": ["eng"]
            },
            "sourceModule": {
                "id": "fixture-cloudstream-movie-direct",
                "name": "Fixture Movie Direct",
                "type": "cloudstream"
            },
            "raw": {}
        })
    }

    fn dash_drm_candidate() -> Value {
        let mut candidate = hls_candidate();
        candidate["id"] = json!("fixture-source:show-1:episode-2:dash-drm");
        candidate["delivery"]["streamType"] = json!("dash");
        candidate["delivery"]["url"] = json!("https://cdn.example.test/show/s01e02/manifest.mpd");
        candidate["delivery"]["drm"] = json!(true);
        candidate
    }

    fn verified_probe_evidence() -> ProbeEvidence {
        ProbeEvidence {
            container: Some("mov,mp4,m4a,3gp,3g2,mj2".to_string()),
            video_codec: Some("h264".to_string()),
            audio_codec: Some("aac".to_string()),
            width: Some(1920),
            height: Some(1080),
            duration_seconds: Some(1440),
            streams: vec![
                ProbeStreamEvidence {
                    index: Some(0),
                    stream_type: Some("video".to_string()),
                    codec: Some("h264".to_string()),
                    width: Some(1920),
                    height: Some(1080),
                    channels: None,
                    language: None,
                    normalized_language: None,
                    title: None,
                    handler_name: None,
                    default: true,
                    forced: false,
                },
                ProbeStreamEvidence {
                    index: Some(1),
                    stream_type: Some("audio".to_string()),
                    codec: Some("aac".to_string()),
                    width: None,
                    height: None,
                    channels: Some(2),
                    language: Some("jpn".to_string()),
                    normalized_language: Some("ja".to_string()),
                    title: Some("Japanese".to_string()),
                    handler_name: None,
                    default: true,
                    forced: false,
                },
                ProbeStreamEvidence {
                    index: Some(2),
                    stream_type: Some("subtitle".to_string()),
                    codec: Some("ass".to_string()),
                    width: None,
                    height: None,
                    channels: None,
                    language: Some("eng".to_string()),
                    normalized_language: Some("en".to_string()),
                    title: Some("English".to_string()),
                    handler_name: None,
                    default: false,
                    forced: false,
                },
            ],
        }
    }

    fn english_probe_evidence() -> ProbeEvidence {
        let mut evidence = verified_probe_evidence();
        if let Some(audio) = evidence
            .streams
            .iter_mut()
            .find(|stream| stream.stream_type.as_deref() == Some("audio"))
        {
            audio.language = Some("eng".to_string());
            audio.normalized_language = Some("en".to_string());
            audio.title = Some("English".to_string());
        }
        evidence
    }

    fn wrong_language_probe_evidence() -> ProbeEvidence {
        let mut evidence = verified_probe_evidence();
        if let Some(audio) = evidence
            .streams
            .iter_mut()
            .find(|stream| stream.stream_type.as_deref() == Some("audio"))
        {
            audio.language = Some("rus".to_string());
            audio.normalized_language = Some("ru".to_string());
            audio.title = Some("Russian".to_string());
        }
        evidence
    }

    fn wrong_duration_probe_evidence() -> ProbeEvidence {
        let mut evidence = verified_probe_evidence();
        evidence.duration_seconds = Some(120);
        evidence
    }

    fn corrupt_probe_evidence() -> ProbeEvidence {
        ProbeEvidence {
            container: None,
            video_codec: None,
            audio_codec: None,
            width: None,
            height: None,
            duration_seconds: None,
            streams: Vec::new(),
        }
    }

    #[test]
    fn lp5_stream_required_audio_reads_language_preference_match_evidence() {
        let mut candidate = direct_file_candidate();
        candidate
            .as_object_mut()
            .expect("candidate")
            .remove("language");
        candidate["raw"] = json!({
            "serverEvidence": {
                "languagePreference": {
                    "mode": "prefer",
                    "state": "match",
                    "matchingAudioLanguages": ["English"],
                    "desiredAudioLanguages": ["English"],
                    "unknownLanguageIsRejected": false
                }
            }
        });

        assert_eq!(required_stream_audio_languages(&candidate), vec!["en"]);
    }

    #[test]
    fn lp5_strict_stream_language_policy_reviews_unconfirmed_probe_language() {
        let mut candidate = direct_file_candidate();
        candidate
            .as_object_mut()
            .expect("candidate")
            .remove("language");
        candidate["raw"] = json!({
            "serverEvidence": {
                "languagePreference": {
                    "mode": "require_review",
                    "state": "unknown",
                    "requiresReview": true,
                    "desiredAudioLanguages": ["English"],
                    "unknownLanguageIsRejected": false
                }
            }
        });
        let mut probe = verified_probe_evidence();
        for stream in &mut probe.streams {
            if stream.stream_type.as_deref() == Some("audio") {
                stream.language = Some("und".to_string());
                stream.normalized_language = None;
            }
        }
        let required = required_stream_audio_languages(&candidate);

        let reason = strict_language_preference_review_reason(&candidate, &required, &probe)
            .expect("strict review reason");

        assert_eq!(required, vec!["en"]);
        assert!(reason.contains("could not confirm desired audio language en"));
    }

    fn fake_probe(evidence: ProbeEvidence) -> FakeProbe {
        FakeProbe {
            called: Arc::new(AtomicBool::new(false)),
            evidence,
        }
    }

    async fn seed_direct_file_release(pool: &AnyPool, candidate: Value) -> Result<(Uuid, String)> {
        let subscription = create_subscription(
            pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Anime,
                title: "Fixture Show".to_string(),
                year: Some(2009),
                external_ids: None,
                idempotency_key: None,
                request_mode: None,
                request_scope: None,
                scope: None,
                metadata_policy: None,
                completion_policy: None,
                monitor_policy: AcquisitionMonitorPolicy::SelectedTargets,
                route_policy: AcquisitionRoutePolicy::DebridFirst,
                source_provider_id: None,
                release_delay_seconds: Some(0),
                quality_profile: None,
                metadata_refresh_after: None,
                candidate_search_after: None,
            },
        )
        .await?;
        upsert_subscription_targets(
            pool,
            subscription.subscription_id,
            vec![NewAcquisitionTarget {
                target_key: Some("S01E02".to_string()),
                media_type: Some(MediaType::Anime),
                title: Some("The First Day".to_string()),
                season_number: Some(1),
                episode_number: Some(2),
                absolute_episode_number: Some(2),
                air_date: None,
                air_time: None,
                metadata: None,
                state: None,
                next_search_after: None,
            }],
        )
        .await?;
        let download_id = "http-stream:test-direct-file".to_string();
        let release = upsert_release(
            pool,
            NewAcquisitionRelease {
                release_id: None,
                subscription_id: Some(subscription.subscription_id),
                source_provider_id: None,
                source_extension_id: "elixir.extension_suite.stream".to_string(),
                owner_id: crate::download_broker::DEFAULT_ROUTE_OWNER_ID.to_string(),
                media_type: MediaType::Anime,
                title: "Fixture Show".to_string(),
                release_title: "Fixture Show - S01E02 - 1080p".to_string(),
                source: "https://stream.example.test/show/s01e02".to_string(),
                source_kind: stream_candidate_string(&candidate, "/sourceKind")
                    .unwrap_or_else(|| "http_file".to_string()),
                info_hash: None,
                fingerprint: "sha256:ess6directfilefixture".to_string(),
                release_kind: ReleaseKind::Single,
                resolver_kind: ReleaseResolverKind::AnimeShokoStyle,
                resolver_version: "cs13-cloudstream-anime-fixture-v1".to_string(),
                confidence: ReleaseConfidence::High,
                score: Some(88.0),
                selected_route_logical_id: Some(HTTP_STREAM_DEFAULT_LOGICAL_ID.to_string()),
                selected_provider_id: None,
                download_id: Some(download_id.clone()),
                remote_release_id: Some(download_id.clone()),
                state: AcquisitionReleaseState::Submitted,
                state_reason: Some("test stream release".to_string()),
                selected_candidate: Some(candidate.clone()),
                coverage_plan: Some(json!({
                    "source": "http_stream_broker",
                    "streamType": stream_candidate_string(&candidate, "/delivery/streamType")
                        .unwrap_or_else(|| "direct_file".to_string())
                })),
            },
        )
        .await?;
        upsert_release_job(
            pool,
            NewAcquisitionReleaseJob {
                release_job_id: None,
                release_id: release.release_id,
                route_logical_id: HTTP_STREAM_DEFAULT_LOGICAL_ID.to_string(),
                provider_id: None,
                download_id: Some(download_id.clone()),
                remote_release_id: Some(download_id.clone()),
                state: ReleaseJobState::Submitted,
                state_reason: Some("test stream job".to_string()),
                active: true,
                started_at: Some(Utc::now()),
                completed_at: None,
            },
        )
        .await?;
        Ok((release.release_id, download_id))
    }

    async fn seed_movie_stream_release(pool: &AnyPool, candidate: Value) -> Result<(Uuid, String)> {
        let subscription = create_subscription(
            pool,
            NewAcquisitionSubscription {
                media_type: MediaType::Movie,
                title: "The Northman".to_string(),
                year: Some(2022),
                external_ids: None,
                idempotency_key: None,
                request_mode: None,
                request_scope: None,
                scope: None,
                metadata_policy: None,
                completion_policy: None,
                monitor_policy: AcquisitionMonitorPolicy::SelectedTargets,
                route_policy: AcquisitionRoutePolicy::DebridFirst,
                source_provider_id: None,
                release_delay_seconds: Some(0),
                quality_profile: None,
                metadata_refresh_after: None,
                candidate_search_after: None,
            },
        )
        .await?;
        upsert_subscription_targets(
            pool,
            subscription.subscription_id,
            vec![NewAcquisitionTarget {
                target_key: Some("movie".to_string()),
                media_type: Some(MediaType::Movie),
                title: Some("The Northman".to_string()),
                season_number: None,
                episode_number: None,
                absolute_episode_number: None,
                air_date: None,
                air_time: None,
                metadata: None,
                state: None,
                next_search_after: None,
            }],
        )
        .await?;
        let download_id = "http-stream:test-movie-direct-file".to_string();
        let release = upsert_release(
            pool,
            NewAcquisitionRelease {
                release_id: None,
                subscription_id: Some(subscription.subscription_id),
                source_provider_id: None,
                source_extension_id: "elixir.extension_suite.stream".to_string(),
                owner_id: crate::download_broker::DEFAULT_ROUTE_OWNER_ID.to_string(),
                media_type: MediaType::Movie,
                title: "The Northman".to_string(),
                release_title: "The Northman (2022) - 1080p".to_string(),
                source: "https://stream.example.test/movies/the-northman".to_string(),
                source_kind: "http_file".to_string(),
                info_hash: None,
                fingerprint: "sha256:cs13moviedirectfixture".to_string(),
                release_kind: ReleaseKind::Single,
                resolver_kind: ReleaseResolverKind::Unresolved,
                resolver_version: "cs13-cloudstream-fixture-v1".to_string(),
                confidence: ReleaseConfidence::High,
                score: Some(94.0),
                selected_route_logical_id: Some(HTTP_STREAM_DEFAULT_LOGICAL_ID.to_string()),
                selected_provider_id: None,
                download_id: Some(download_id.clone()),
                remote_release_id: Some(download_id.clone()),
                state: AcquisitionReleaseState::Submitted,
                state_reason: Some("test movie stream release".to_string()),
                selected_candidate: Some(candidate.clone()),
                coverage_plan: Some(json!({
                    "source": "http_stream_broker",
                    "streamType": "direct_file"
                })),
            },
        )
        .await?;
        upsert_release_job(
            pool,
            NewAcquisitionReleaseJob {
                release_job_id: None,
                release_id: release.release_id,
                route_logical_id: HTTP_STREAM_DEFAULT_LOGICAL_ID.to_string(),
                provider_id: None,
                download_id: Some(download_id.clone()),
                remote_release_id: Some(download_id.clone()),
                state: ReleaseJobState::Submitted,
                state_reason: Some("test movie stream job".to_string()),
                active: true,
                started_at: Some(Utc::now()),
                completed_at: None,
            },
        )
        .await?;
        Ok((release.release_id, download_id))
    }

    async fn seed_late_resolve_stream_provider(pool: &AnyPool, port: u16) -> Result<Uuid> {
        let store = ExtensionStore::new(pool);
        let extension_id = "elixir.sources.ess13.late_resolve";
        store
            .upsert_extension(&NewExtension {
                extension_id: extension_id.to_string(),
                name: "ESS13 Late Resolve Provider".to_string(),
                version: "1.0.0".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: Some("Elixir Test".to_string()),
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Community,
                manifest_json: json!({
                    "id": extension_id,
                    "version": "1.0.0",
                    "kind": "module",
                    "name": "ESS13 Late Resolve Provider",
                    "provides": [{
                        "capability": ACQUISITION_STREAM_CANDIDATE_PROVIDER_CAPABILITY,
                        "slot": "default",
                        "cardinality": "many",
                        "implementation": "ess13_late_resolve_fixture",
                        "scope": {
                            "media_types": ["anime"],
                            "actions": ["search", "resolve"]
                        }
                    }],
                    "runtime": {
                        "type": "container",
                        "image": "example/ess13-late-resolve:1"
                    },
                    "control_surface": {
                        "adapter": "generic_v1",
                        "owned_settings": [
                            {
                                "id": "sourceModulesJson",
                                "label": "Source modules",
                                "type": "textarea",
                                "storage": {
                                    "type": "instance_setting",
                                    "key": "sourceModulesJson"
                                }
                            },
                            {
                                "id": "apiToken",
                                "label": "API token",
                                "type": "password",
                                "secret": true,
                                "storage": {
                                    "type": "instance_setting",
                                    "key": "apiToken"
                                }
                            }
                        ]
                    }
                }),
                package_hash: Some("ess13-late-resolve".to_string()),
                enabled: true,
            })
            .await?;
        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: extension_id.to_string(),
                instance_name: "default".to_string(),
                config_json: Some(json!({
                    "sourceModulesJson": "[{\"id\":\"fixture-source\",\"enabled\":true}]",
                    "apiToken": "must-not-cross-provider-boundary"
                })),
                enabled: true,
            })
            .await?;
        let provider_id = stable_provider_id(
            instance_id,
            ACQUISITION_STREAM_CANDIDATE_PROVIDER_CAPABILITY,
            "default",
        );
        store
            .upsert_provider(&NewProvider {
                provider_id,
                instance_id,
                capability: ACQUISITION_STREAM_CANDIDATE_PROVIDER_CAPABILITY.to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::Many,
                implementation: Some("ess13_late_resolve_fixture".to_string()),
                scope_json: Some(json!({
                    "media_types": ["anime"],
                    "actions": ["search", "resolve"]
                })),
                endpoint_json: Some(json!({
                    "scheme": "http",
                    "host": "127.0.0.1",
                    "port": port,
                    "base_path": "/stream-provider",
                    "network": null
                })),
                health_state: ProviderHealthState::Healthy,
            })
            .await?;
        Ok(provider_id)
    }

    #[tokio::test]
    async fn ess6_direct_file_materializer_applies_headers_tracks_progress_and_probes() -> Result<()>
    {
        let database = setup_db().await?;
        let temp = TempDir::new()?;
        let candidate = direct_file_candidate();
        let (release_id, download_id) = seed_direct_file_release(&database.pool, candidate).await?;
        let observed_headers = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let observed_referer = Arc::new(tokio::sync::Mutex::new(None));
        let downloader = FakeDirectFileClient {
            chunks: vec![b"video".to_vec(), b"bytes".to_vec()],
            content_length: Some(10),
            content_type: Some("video/mp4".to_string()),
            content_disposition: Some("attachment; filename=\"Fixture S01E02.mp4\"".to_string()),
            observed_headers: observed_headers.clone(),
            observed_referer: observed_referer.clone(),
            block_on_second_chunk: None,
        };
        let probe = fake_probe(verified_probe_evidence());
        let config = HttpStreamMaterializerConfig {
            paths: MaterializerPaths::from_downloads_root(temp.path().join("downloads")),
            batch_limit: 10,
        };

        let stats = process_http_stream_materializer_once_with_services(
            &database.pool,
            &config,
            &downloader,
            &FakeStreamRemuxer::default(),
            &probe,
            &FakeLateResolver::default(),
        )
        .await?;

        assert_eq!(stats.completed, 1);
        assert!(probe.called.load(Ordering::SeqCst));
        let headers = observed_headers.lock().await.clone();
        assert!(
            headers
                .iter()
                .any(|(name, value)| { name == "user-agent" && value == "ElixirTest/1.0" })
        );
        assert!(
            headers
                .iter()
                .any(|(name, value)| { name == "x-source-token" && value == "redacted" })
        );
        assert_eq!(
            observed_referer.lock().await.as_deref(),
            Some("https://stream.example.test/show/s01e02")
        );

        let release = get_release(&database.pool, release_id)
            .await?
            .expect("completed stream release");
        assert_eq!(release.state, AcquisitionReleaseState::Completed);
        let runtime = release
            .coverage_plan
            .as_ref()
            .and_then(|plan| plan.get("streamRuntime"))
            .expect("runtime evidence");
        assert_eq!(
            runtime.get("runtimeState").and_then(Value::as_str),
            Some("completed")
        );
        assert_eq!(
            runtime.get("downloadedBytes").and_then(Value::as_u64),
            Some(10)
        );
        assert_eq!(
            runtime.pointer("/probe/videoCodec").and_then(Value::as_str),
            Some("h264")
        );
        let local_path = runtime
            .get("localPath")
            .and_then(Value::as_str)
            .expect("local path");
        assert!(Path::new(local_path).is_file());

        let jobs = list_release_jobs(&database.pool, release_id).await?;
        assert_eq!(jobs[0].state, ReleaseJobState::Completed);
        assert!(!jobs[0].active);
        let files = list_release_files(&database.pool, release_id).await?;
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].selected, Some(true));
        assert_eq!(
            files[0]
                .provider_metadata
                .as_ref()
                .and_then(|value| value.get("localPath"))
                .and_then(Value::as_str),
            Some(local_path)
        );
        let coverage = crate::acquisition::release_resolution::store::list_release_coverage(
            &database.pool,
            release_id,
        )
        .await?;
        assert_eq!(coverage.len(), 1);
        assert_eq!(coverage[0].state, ReleaseCoverageState::Selected);
        assert_eq!(
            runtime
                .pointer("/verification/verificationState")
                .and_then(Value::as_str),
            Some("verified")
        );
        assert_eq!(
            files[0]
                .provider_metadata
                .as_ref()
                .and_then(|value| {
                    value.pointer("/streamMaterializer/verification/verificationState")
                })
                .and_then(Value::as_str),
            Some("verified")
        );
        let import_ready = list_import_pending_release_jobs(&database.pool, 10).await?;
        assert!(
            import_ready
                .iter()
                .any(|candidate| candidate.release.release_id == release_id)
        );

        let release_by_download_id = get_release_by_download_id(&database.pool, &download_id)
            .await?
            .expect("release by download id");
        assert_eq!(release_by_download_id.release_id, release_id);
        Ok(())
    }

    #[tokio::test]
    async fn ess6_direct_file_materializer_rejects_html_landing_page_before_probe() -> Result<()> {
        let database = setup_db().await?;
        let temp = TempDir::new()?;
        let candidate = direct_file_candidate();
        let (release_id, download_id) = seed_direct_file_release(&database.pool, candidate).await?;
        let downloader = FakeDirectFileClient {
            chunks: vec![b"<!DOCTYPE html><title>Login required</title>".to_vec()],
            content_length: Some(42),
            content_type: Some("text/html; charset=UTF-8".to_string()),
            content_disposition: None,
            observed_headers: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            observed_referer: Arc::new(tokio::sync::Mutex::new(None)),
            block_on_second_chunk: None,
        };
        let probe = fake_probe(verified_probe_evidence());
        let config = HttpStreamMaterializerConfig {
            paths: MaterializerPaths::from_downloads_root(temp.path().join("downloads")),
            batch_limit: 10,
        };

        let stats = process_http_stream_materializer_once_with_services(
            &database.pool,
            &config,
            &downloader,
            &FakeStreamRemuxer::default(),
            &probe,
            &FakeLateResolver::default(),
        )
        .await?;

        assert_eq!(stats.failed, 1);
        assert!(!probe.called.load(Ordering::SeqCst));
        let release = get_release(&database.pool, release_id)
            .await?
            .expect("failed stream release");
        assert_eq!(release.state, AcquisitionReleaseState::Failed);
        assert!(
            release
                .state_reason
                .as_deref()
                .unwrap_or_default()
                .contains("Direct HTTP stream URL returned text/html")
        );
        assert_eq!(release.download_id.as_deref(), Some(download_id.as_str()));
        let runtime = release
            .coverage_plan
            .as_ref()
            .and_then(|plan| plan.get("streamRuntime"))
            .expect("runtime evidence");
        assert_eq!(
            runtime.get("failureClass").and_then(Value::as_str),
            Some("source_returned_non_media_response")
        );
        assert_eq!(
            runtime.get("contentType").and_then(Value::as_str),
            Some("text/html; charset=UTF-8")
        );

        let jobs = list_release_jobs(&database.pool, release_id).await?;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].state, ReleaseJobState::Failed);
        assert!(
            jobs[0]
                .state_reason
                .as_deref()
                .unwrap_or_default()
                .contains("hoster, login, or error page")
        );

        let subscription_id = release
            .subscription_id
            .expect("stream release subscription id");
        let targets = list_subscription_targets(&database.pool, subscription_id).await?;
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].state, AcquisitionTargetState::Blocked);
        assert_eq!(
            targets[0].selected_route_logical_id.as_deref(),
            Some(HTTP_STREAM_DEFAULT_LOGICAL_ID)
        );
        assert_eq!(
            targets[0].download_id.as_deref(),
            Some(download_id.as_str())
        );
        assert!(
            targets[0]
                .state_reason
                .as_deref()
                .unwrap_or_default()
                .contains("Direct HTTP stream URL returned text/html")
        );
        Ok(())
    }

    #[tokio::test]
    async fn hse_direct_https_stream_records_direct_https_egress() -> Result<()> {
        let database = setup_db().await?;
        let temp = TempDir::new()?;
        let candidate = direct_file_candidate();
        let (release_id, _) = seed_direct_file_release(&database.pool, candidate).await?;
        let downloader = FakeDirectFileClient {
            chunks: vec![b"video".to_vec(), b"bytes".to_vec()],
            content_length: Some(10),
            content_type: Some("video/mp4".to_string()),
            content_disposition: None,
            observed_headers: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            observed_referer: Arc::new(tokio::sync::Mutex::new(None)),
            block_on_second_chunk: None,
        };
        let config = HttpStreamMaterializerConfig {
            paths: MaterializerPaths::from_downloads_root(temp.path().join("downloads")),
            batch_limit: 10,
        };

        let stats = process_http_stream_materializer_once_with_services(
            &database.pool,
            &config,
            &downloader,
            &FakeStreamRemuxer::default(),
            &fake_probe(verified_probe_evidence()),
            &FakeLateResolver::default(),
        )
        .await?;

        assert_eq!(stats.completed, 1);
        let release = get_release(&database.pool, release_id)
            .await?
            .expect("release");
        let runtime = release
            .coverage_plan
            .as_ref()
            .and_then(|plan| plan.get("streamRuntime"))
            .expect("runtime evidence");
        assert_eq!(
            runtime.pointer("/egress/decision").and_then(Value::as_str),
            Some("direct_https")
        );
        assert_eq!(
            runtime
                .pointer("/egress/routeLabel")
                .and_then(Value::as_str),
            Some("Direct HTTPS stream download")
        );
        Ok(())
    }

    #[tokio::test]
    async fn hse_direct_http_stream_fails_closed_without_protected_profile() -> Result<()> {
        let database = setup_db().await?;
        let temp = TempDir::new()?;
        let mut candidate = direct_file_candidate();
        candidate["delivery"]["url"] = json!("http://cdn.example.test/show/s01e02/file.mp4");
        let (release_id, _) = seed_direct_file_release(&database.pool, candidate).await?;
        let observed_headers = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let downloader = FakeDirectFileClient {
            chunks: vec![b"must-not-run".to_vec()],
            content_length: Some(12),
            content_type: Some("video/mp4".to_string()),
            content_disposition: None,
            observed_headers: observed_headers.clone(),
            observed_referer: Arc::new(tokio::sync::Mutex::new(None)),
            block_on_second_chunk: None,
        };
        let config = HttpStreamMaterializerConfig {
            paths: MaterializerPaths::from_downloads_root(temp.path().join("downloads")),
            batch_limit: 10,
        };

        let stats = process_http_stream_materializer_once_with_all_services(
            &database.pool,
            &config,
            &downloader,
            &FakeStreamRemuxer::default(),
            &InitialSchemeStreamEgressClassifier,
            &UnavailableProtectedStreamMaterializer,
            &fake_probe(verified_probe_evidence()),
            &FakeLateResolver::default(),
        )
        .await?;

        assert_eq!(stats.failed, 1);
        assert!(observed_headers.lock().await.is_empty());
        let release = get_release(&database.pool, release_id)
            .await?
            .expect("release");
        assert_eq!(release.state, AcquisitionReleaseState::Failed);
        let runtime = release
            .coverage_plan
            .as_ref()
            .and_then(|plan| plan.get("streamRuntime"))
            .expect("runtime evidence");
        assert_eq!(
            runtime.get("failureClass").and_then(Value::as_str),
            Some("protected_stream_egress_unavailable")
        );
        assert_eq!(
            runtime.pointer("/egress/decision").and_then(Value::as_str),
            Some("blocked_protected_egress_unavailable")
        );
        assert_eq!(
            runtime
                .pointer("/egress/routeLabel")
                .and_then(Value::as_str),
            Some("Stream download blocked: protected egress unavailable")
        );
        Ok(())
    }

    #[tokio::test]
    async fn hse_direct_http_stream_uses_protected_worker_with_active_profile() -> Result<()> {
        let database = setup_db().await?;
        seed_active_wireguard_stream_profile(&database.pool).await?;
        let temp = TempDir::new()?;
        let mut candidate = direct_file_candidate();
        candidate["delivery"]["url"] = json!("http://cdn.example.test/show/s01e02/file.mp4");
        let (release_id, _) = seed_direct_file_release(&database.pool, candidate).await?;
        let observed_headers = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let downloader = FakeDirectFileClient {
            chunks: vec![b"must-not-run".to_vec()],
            content_length: Some(12),
            content_type: Some("video/mp4".to_string()),
            content_disposition: None,
            observed_headers: observed_headers.clone(),
            observed_referer: Arc::new(tokio::sync::Mutex::new(None)),
            block_on_second_chunk: None,
        };
        let protected = FakeProtectedStreamMaterializer::default();
        let config = HttpStreamMaterializerConfig {
            paths: MaterializerPaths::from_downloads_root(temp.path().join("downloads")),
            batch_limit: 10,
        };

        let stats = process_http_stream_materializer_once_with_all_services(
            &database.pool,
            &config,
            &downloader,
            &FakeStreamRemuxer::default(),
            &InitialSchemeStreamEgressClassifier,
            &protected,
            &fake_probe(verified_probe_evidence()),
            &FakeLateResolver::default(),
        )
        .await?;

        assert_eq!(stats.completed, 1);
        assert!(observed_headers.lock().await.is_empty());
        assert_eq!(protected.direct_calls.lock().await.len(), 1);
        let release = get_release(&database.pool, release_id)
            .await?
            .expect("release");
        assert_eq!(release.state, AcquisitionReleaseState::Completed);
        let runtime = release
            .coverage_plan
            .as_ref()
            .and_then(|plan| plan.get("streamRuntime"))
            .expect("runtime evidence");
        assert_eq!(
            runtime.pointer("/egress/decision").and_then(Value::as_str),
            Some("protected_http")
        );
        assert_eq!(
            runtime
                .pointer("/egress/protectedProfileId")
                .and_then(Value::as_str),
            Some("hse-wireguard")
        );
        assert_eq!(
            runtime
                .pointer("/egress/workerRuntimeId")
                .and_then(Value::as_str),
            Some("fake-protected-worker")
        );
        Ok(())
    }

    #[tokio::test]
    async fn hse_https_to_http_redirect_reroutes_to_protected_worker_before_http_fetch()
    -> Result<()> {
        let database = setup_db().await?;
        seed_active_wireguard_stream_profile(&database.pool).await?;
        let temp = TempDir::new()?;
        let candidate = direct_file_candidate();
        let (release_id, _) = seed_direct_file_release(&database.pool, candidate).await?;
        let downloader = DowngradeRedirectDirectFileClient::default();
        let protected = FakeProtectedStreamMaterializer::default();
        let config = HttpStreamMaterializerConfig {
            paths: MaterializerPaths::from_downloads_root(temp.path().join("downloads")),
            batch_limit: 10,
        };

        let stats = process_http_stream_materializer_once_with_all_services(
            &database.pool,
            &config,
            &downloader,
            &FakeStreamRemuxer::default(),
            &InitialSchemeStreamEgressClassifier,
            &protected,
            &fake_probe(verified_probe_evidence()),
            &FakeLateResolver::default(),
        )
        .await?;

        assert_eq!(stats.completed, 1);
        assert_eq!(downloader.calls.load(Ordering::SeqCst), 1);
        let protected_calls = protected.direct_calls.lock().await.clone();
        assert_eq!(protected_calls.len(), 1);
        assert_eq!(
            protected_calls[0].download.url.as_str(),
            "https://cdn.example.test/show/s01e02/file"
        );
        let release = get_release(&database.pool, release_id)
            .await?
            .expect("release");
        let runtime = release
            .coverage_plan
            .as_ref()
            .and_then(|plan| plan.get("streamRuntime"))
            .expect("runtime evidence");
        assert_eq!(
            runtime.pointer("/egress/decision").and_then(Value::as_str),
            Some("protected_http")
        );
        assert_eq!(
            runtime
                .pointer("/egress/initialUrlScheme")
                .and_then(Value::as_str),
            Some("https")
        );
        Ok(())
    }

    #[tokio::test]
    async fn hse_direct_only_rejects_http_stream_without_worker() -> Result<()> {
        let database = setup_db().await?;
        ExtensionStore::new(&database.pool)
            .upsert_extension_setting(
                crate::acquisition::stream_egress::STREAM_HTTP_EGRESS_POLICY_SETTING_KEY,
                &json!("direct_only"),
            )
            .await?;
        let temp = TempDir::new()?;
        let mut candidate = direct_file_candidate();
        candidate["delivery"]["url"] = json!("http://cdn.example.test/show/s01e02/file.mp4");
        let (release_id, _) = seed_direct_file_release(&database.pool, candidate).await?;
        let protected = FakeProtectedStreamMaterializer::default();
        let config = HttpStreamMaterializerConfig {
            paths: MaterializerPaths::from_downloads_root(temp.path().join("downloads")),
            batch_limit: 10,
        };

        let stats = process_http_stream_materializer_once_with_all_services(
            &database.pool,
            &config,
            &FakeDirectFileClient {
                chunks: Vec::new(),
                content_length: None,
                content_type: None,
                content_disposition: None,
                observed_headers: Arc::new(tokio::sync::Mutex::new(Vec::new())),
                observed_referer: Arc::new(tokio::sync::Mutex::new(None)),
                block_on_second_chunk: None,
            },
            &FakeStreamRemuxer::default(),
            &InitialSchemeStreamEgressClassifier,
            &protected,
            &fake_probe(verified_probe_evidence()),
            &FakeLateResolver::default(),
        )
        .await?;

        assert_eq!(stats.failed, 1);
        assert!(protected.direct_calls.lock().await.is_empty());
        let release = get_release(&database.pool, release_id)
            .await?
            .expect("release");
        let runtime = release
            .coverage_plan
            .as_ref()
            .and_then(|plan| plan.get("streamRuntime"))
            .expect("runtime evidence");
        assert_eq!(
            runtime.get("failureClass").and_then(Value::as_str),
            Some("stream_egress_policy_rejected")
        );
        assert_eq!(
            runtime.pointer("/egress/decision").and_then(Value::as_str),
            Some("rejected_by_policy")
        );
        Ok(())
    }

    #[tokio::test]
    async fn hse_https_hls_stays_on_host_remux_when_delivery_graph_is_direct() -> Result<()> {
        let database = setup_db().await?;
        let temp = TempDir::new()?;
        let candidate = hls_candidate();
        let (release_id, _) = seed_direct_file_release(&database.pool, candidate).await?;
        let remuxer = FakeStreamRemuxer::default();
        let protected = FakeProtectedStreamMaterializer::default();
        let config = HttpStreamMaterializerConfig {
            paths: MaterializerPaths::from_downloads_root(temp.path().join("downloads")),
            batch_limit: 10,
        };

        let stats = process_http_stream_materializer_once_with_all_services(
            &database.pool,
            &config,
            &FakeDirectFileClient {
                chunks: Vec::new(),
                content_length: None,
                content_type: None,
                content_disposition: None,
                observed_headers: Arc::new(tokio::sync::Mutex::new(Vec::new())),
                observed_referer: Arc::new(tokio::sync::Mutex::new(None)),
                block_on_second_chunk: None,
            },
            &remuxer,
            &InitialSchemeStreamEgressClassifier,
            &protected,
            &fake_probe(verified_probe_evidence()),
            &FakeLateResolver::default(),
        )
        .await?;

        assert_eq!(stats.completed, 1);
        assert_eq!(remuxer.observed.lock().await.len(), 1);
        assert!(protected.remux_calls.lock().await.is_empty());
        let release = get_release(&database.pool, release_id)
            .await?
            .expect("release");
        let runtime = release
            .coverage_plan
            .as_ref()
            .and_then(|plan| plan.get("streamRuntime"))
            .expect("runtime evidence");
        assert_eq!(
            runtime.pointer("/egress/decision").and_then(Value::as_str),
            Some("direct_https")
        );
        Ok(())
    }

    #[tokio::test]
    async fn hse_mixed_hls_manifest_uses_protected_remux_worker() -> Result<()> {
        let database = setup_db().await?;
        seed_active_wireguard_stream_profile(&database.pool).await?;
        let temp = TempDir::new()?;
        let candidate = hls_candidate();
        let (release_id, _) = seed_direct_file_release(&database.pool, candidate).await?;
        let remuxer = FakeStreamRemuxer::default();
        let protected = FakeProtectedStreamMaterializer::default();
        let classifier = FixedStreamEgressClassifier {
            direct_route: None,
            remux_route: Some(StreamEgressRoute::protected(
                StreamHttpEgressPolicy::AutoHttpOnly,
                StreamEgressDecision::ProtectedMixedManifest,
                "https",
                "test manifest includes HTTP segment",
            )),
        };
        let config = HttpStreamMaterializerConfig {
            paths: MaterializerPaths::from_downloads_root(temp.path().join("downloads")),
            batch_limit: 10,
        };

        let stats = process_http_stream_materializer_once_with_all_services(
            &database.pool,
            &config,
            &FakeDirectFileClient {
                chunks: Vec::new(),
                content_length: None,
                content_type: None,
                content_disposition: None,
                observed_headers: Arc::new(tokio::sync::Mutex::new(Vec::new())),
                observed_referer: Arc::new(tokio::sync::Mutex::new(None)),
                block_on_second_chunk: None,
            },
            &remuxer,
            &classifier,
            &protected,
            &fake_probe(verified_probe_evidence()),
            &FakeLateResolver::default(),
        )
        .await?;

        assert_eq!(stats.completed, 1);
        assert!(remuxer.observed.lock().await.is_empty());
        assert_eq!(protected.remux_calls.lock().await.len(), 1);
        let release = get_release(&database.pool, release_id)
            .await?
            .expect("release");
        let runtime = release
            .coverage_plan
            .as_ref()
            .and_then(|plan| plan.get("streamRuntime"))
            .expect("runtime evidence");
        assert_eq!(
            runtime.pointer("/egress/decision").and_then(Value::as_str),
            Some("protected_mixed_manifest")
        );
        assert_eq!(
            runtime
                .pointer("/egress/workerRuntimeId")
                .and_then(Value::as_str),
            Some("fake-protected-worker")
        );
        Ok(())
    }

    #[test]
    fn hse_initial_http_manifests_require_protected_worker_route() -> Result<()> {
        for (url, stream_type, reason) in [
            (
                "http://cdn.example.test/master.m3u8",
                StreamDeliveryType::Hls,
                "HLS manifest URL is HTTP",
            ),
            (
                "http://cdn.example.test/manifest.mpd",
                StreamDeliveryType::Dash,
                "DASH manifest URL is HTTP",
            ),
        ] {
            let route = classify_initial_stream_url(
                StreamHttpEgressPolicy::AutoHttpOnly,
                &Url::parse(url)?,
                stream_type,
            )?;

            assert_eq!(route.decision, StreamEgressDecision::ProtectedHttp);
            assert_eq!(route.initial_url_scheme, "http");
            assert_eq!(route.reason, reason);
        }
        Ok(())
    }

    #[test]
    fn hse_stream_classifier_rejects_private_ip_delivery_urls() -> Result<()> {
        let err = classify_initial_stream_url(
            StreamHttpEgressPolicy::AutoHttpOnly,
            &Url::parse("http://127.0.0.1/master.m3u8")?,
            StreamDeliveryType::Hls,
        )
        .expect_err("private stream delivery IP should be rejected");

        assert!(err.to_string().contains("stream delivery URL is unsafe"));
        Ok(())
    }

    #[test]
    fn hse_stream_manifest_classification_rejects_oversized_manifests() {
        let err = ensure_stream_manifest_classification_byte_limit(
            STREAM_MANIFEST_CLASSIFY_MAX_BYTES + 1,
        )
        .expect_err("oversized manifests should be rejected");

        assert!(
            err.to_string()
                .contains("stream manifest exceeds classification byte limit")
        );
    }

    #[test]
    fn hse_hls_manifest_parser_detects_http_keys_subtitles_and_nested_playlists() -> Result<()> {
        let base = Url::parse("https://cdn.example.test/master.m3u8")?;
        let body = r#"
#EXTM3U
#EXT-X-KEY:METHOD=AES-128,URI="http://keys.example.test/key.bin"
#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="subs",URI="https://cdn.example.test/subs/en.m3u8"
#EXT-X-STREAM-INF:BANDWIDTH=800000
variant.m3u8
http://segments.example.test/seg-1.ts
"#;
        let references = hls_manifest_references(&base, body)?;

        assert!(
            references
                .iter()
                .any(|reference| reference.kind == "key" && reference.url.scheme() == "http")
        );
        assert!(references.iter().any(|reference| {
            reference.kind == "rendition" && reference.url.as_str().contains("/subs/en.m3u8")
        }));
        assert!(references.iter().any(|reference| {
            reference.kind == "nested_playlist" && reference.url.as_str().ends_with("/variant.m3u8")
        }));
        assert!(
            references
                .iter()
                .any(|reference| reference.kind == "segment" && reference.url.scheme() == "http")
        );
        Ok(())
    }

    #[test]
    fn hse_dash_manifest_parser_detects_http_base_and_segment_urls() -> Result<()> {
        let base = Url::parse("https://cdn.example.test/manifest.mpd")?;
        let body = r#"
<MPD>
  <Period>
    <BaseURL>http://segments.example.test/video/</BaseURL>
    <AdaptationSet>
      <Representation>
        <SegmentTemplate initialization="http://segments.example.test/init.m4s" />
        <SegmentList>
          <SegmentURL media="http://segments.example.test/seg-1.m4s" />
        </SegmentList>
      </Representation>
    </AdaptationSet>
  </Period>
</MPD>
"#;
        let references = dash_manifest_references(&base, body)?;

        assert!(
            references
                .iter()
                .any(|reference| reference.kind == "base_url" && reference.url.scheme() == "http")
        );
        assert!(references.iter().any(|reference| {
            reference.kind == "segment" && reference.url.as_str().ends_with("/init.m4s")
        }));
        assert!(references.iter().any(|reference| {
            reference.kind == "segment"
                && reference.url.as_str() == "http://segments.example.test/seg-1.m4s"
        }));
        Ok(())
    }

    #[tokio::test]
    async fn ess13_late_resolve_refreshes_expired_stream_candidate_before_materialization()
    -> Result<()> {
        let database = setup_db().await?;
        let temp = TempDir::new()?;
        let mut candidate = direct_file_candidate();
        candidate["delivery"]["resolveHandle"] = json!("refresh-fixture-handle");
        candidate["delivery"]["resolveRequired"] = json!(false);
        candidate["delivery"]["expiresAt"] = json!("2001-01-01T00:00:00Z");
        let (release_id, _) = seed_direct_file_release(&database.pool, candidate).await?;
        let resolved_url = "https://cdn.example.test/show/s01e02/fresh-file.mp4";
        let resolver = FakeLateResolver {
            calls: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            result: Some(StreamCandidateResolveResult {
                delivery: Some(json!({
                    "streamType": "direct_file",
                    "url": resolved_url,
                    "referer": "https://stream.example.test/show/s01e02/fresh",
                    "headers": {
                        "user-agent": "ElixirLateResolveTest/1.0"
                    }
                })),
                candidate: None,
                warnings: vec!["fixture refreshed expiring URL".to_string()],
            }),
            fail_message: None,
        };
        let downloader = FakeDirectFileClient {
            chunks: vec![b"video".to_vec(), b"bytes".to_vec()],
            content_length: Some(10),
            content_type: Some("video/mp4".to_string()),
            content_disposition: Some("attachment; filename=\"Late Resolve.mp4\"".to_string()),
            observed_headers: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            observed_referer: Arc::new(tokio::sync::Mutex::new(None)),
            block_on_second_chunk: None,
        };
        let probe = fake_probe(verified_probe_evidence());
        let config = HttpStreamMaterializerConfig {
            paths: MaterializerPaths::from_downloads_root(temp.path().join("downloads")),
            batch_limit: 10,
        };

        let stats = process_http_stream_materializer_once_with_services(
            &database.pool,
            &config,
            &downloader,
            &FakeStreamRemuxer::default(),
            &probe,
            &resolver,
        )
        .await?;

        assert_eq!(stats.completed, 1);
        let calls = resolver.calls.lock().await.clone();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            stream_candidate_string(&calls[0], "/delivery/resolveHandle").as_deref(),
            Some("refresh-fixture-handle")
        );
        let release = get_release(&database.pool, release_id)
            .await?
            .expect("late resolved release");
        assert_eq!(release.state, AcquisitionReleaseState::Completed);
        let selected = release
            .selected_candidate
            .as_ref()
            .expect("selected candidate");
        assert_eq!(
            stream_candidate_string(selected, "/delivery/url").as_deref(),
            Some(resolved_url)
        );
        assert_eq!(
            selected
                .pointer("/delivery/resolveRequired")
                .and_then(Value::as_bool),
            Some(false)
        );
        assert!(selected.pointer("/delivery/expiresAt").is_none());
        let runtime = release
            .coverage_plan
            .as_ref()
            .and_then(|plan| plan.get("streamRuntime"))
            .expect("runtime evidence");
        assert_eq!(
            runtime
                .pointer("/lateResolve/reason")
                .and_then(Value::as_str),
            Some("delivery_expired")
        );
        assert_eq!(
            runtime.get("sourceUrl").and_then(Value::as_str),
            Some(resolved_url)
        );
        assert_eq!(
            runtime
                .pointer("/lateResolve/warnings/0")
                .and_then(Value::as_str),
            Some("fixture refreshed expiring URL")
        );
        assert!(probe.called.load(Ordering::SeqCst));
        Ok(())
    }

    #[tokio::test]
    async fn ess13_provider_late_resolver_calls_registered_stream_provider_resolve() -> Result<()> {
        let database = setup_db().await?;
        let temp = TempDir::new()?;
        let resolved_url = "https://cdn.example.test/show/s01e02/provider-fresh.mp4";
        let (port, requests, _handle) = start_late_resolve_provider_fixture(json!({
            "delivery": {
                "streamType": "direct_file",
                "url": resolved_url,
                "headers": {
                    "user-agent": "ElixirProviderResolveTest/1.0"
                }
            },
            "warnings": ["provider refreshed URL"]
        }))
        .await?;
        let provider_id = seed_late_resolve_stream_provider(&database.pool, port).await?;
        let mut candidate = direct_file_candidate();
        candidate["raw"] = json!({
            "serverEvidence": {
                "extensionSuite": {
                    "providerId": provider_id.to_string(),
                    "extensionId": "elixir.sources.ess13.late_resolve",
                    "capability": ACQUISITION_STREAM_CANDIDATE_PROVIDER_CAPABILITY
                }
            }
        });
        candidate["delivery"]["resolveHandle"] = json!("provider-refresh-handle");
        candidate["delivery"]["expiresAt"] = json!("2001-01-01T00:00:00Z");
        let (release_id, _) = seed_direct_file_release(&database.pool, candidate).await?;
        let downloader = FakeDirectFileClient {
            chunks: vec![b"video".to_vec(), b"bytes".to_vec()],
            content_length: Some(10),
            content_type: Some("video/mp4".to_string()),
            content_disposition: Some("attachment; filename=\"Provider Resolve.mp4\"".to_string()),
            observed_headers: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            observed_referer: Arc::new(tokio::sync::Mutex::new(None)),
            block_on_second_chunk: None,
        };
        let probe = fake_probe(verified_probe_evidence());
        let config = HttpStreamMaterializerConfig {
            paths: MaterializerPaths::from_downloads_root(temp.path().join("downloads")),
            batch_limit: 10,
        };

        let stats = process_http_stream_materializer_once_with_services(
            &database.pool,
            &config,
            &downloader,
            &FakeStreamRemuxer::default(),
            &probe,
            &ProviderStreamCandidateLateResolver::with_base_url(
                provider_id,
                format!("http://127.0.0.1:{port}/stream-provider"),
            ),
        )
        .await?;

        assert_eq!(stats.completed, 1);
        let requests = requests.lock().expect("requests lock").clone();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].get("schemaVersion").and_then(Value::as_u64),
            Some(u64::from(STREAM_CANDIDATE_PROVIDER_SCHEMA_VERSION))
        );
        assert_eq!(
            requests[0].get("resolveHandle").and_then(Value::as_str),
            Some("provider-refresh-handle")
        );
        let provider_id_string = provider_id.to_string();
        assert_eq!(
            requests[0]
                .pointer("/provider/providerId")
                .and_then(Value::as_str),
            Some(provider_id_string.as_str())
        );
        assert!(
            requests[0]
                .pointer("/provider/config/sourceModulesJson")
                .and_then(Value::as_str)
                .is_some()
        );
        assert!(
            requests[0]
                .pointer("/provider/config/apiToken")
                .and_then(Value::as_str)
                .is_none()
        );
        let release = get_release(&database.pool, release_id)
            .await?
            .expect("provider resolved release");
        assert_eq!(release.state, AcquisitionReleaseState::Completed);
        assert_eq!(
            release
                .selected_candidate
                .as_ref()
                .and_then(|candidate| stream_candidate_string(candidate, "/delivery/url"))
                .as_deref(),
            Some(resolved_url)
        );
        Ok(())
    }

    #[tokio::test]
    async fn cs13_movie_stream_materializes_and_imports_correct_target() -> Result<()> {
        let database = setup_db().await?;
        let temp = TempDir::new()?;
        let candidate = movie_direct_file_candidate();
        let (release_id, _) = seed_movie_stream_release(&database.pool, candidate).await?;
        let downloader = FakeDirectFileClient {
            chunks: vec![b"movie".to_vec(), b"bytes".to_vec()],
            content_length: Some(10),
            content_type: Some("video/mp4".to_string()),
            content_disposition: Some("attachment; filename=\"The Northman.mp4\"".to_string()),
            observed_headers: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            observed_referer: Arc::new(tokio::sync::Mutex::new(None)),
            block_on_second_chunk: None,
        };
        let config = HttpStreamMaterializerConfig {
            paths: MaterializerPaths::from_downloads_root(temp.path().join("downloads")),
            batch_limit: 10,
        };

        let stats = process_http_stream_materializer_once_with_services(
            &database.pool,
            &config,
            &downloader,
            &FakeStreamRemuxer::default(),
            &fake_probe(english_probe_evidence()),
            &FakeLateResolver::default(),
        )
        .await?;
        assert_eq!(stats.completed, 1);

        let import_stats = run_acquisition_import_iteration(&database.pool, 10).await?;
        assert_eq!(import_stats.runs_imported, 1);
        assert_eq!(import_stats.links_imported, 1);
        let jobs = list_release_jobs(&database.pool, release_id).await?;
        let run = get_import_run_by_release_job(&database.pool, jobs[0].release_job_id)
            .await?
            .expect("movie import run");
        assert_eq!(run.state, AcquisitionImportRunState::Imported);
        let links = list_import_file_links(&database.pool, run.import_run_id).await?;
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].state, AcquisitionImportFileLinkState::Imported);
        assert!(links[0].movie_id.is_some());
        assert!(links[0].episode_id.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn cs13_expired_hls_stream_late_resolves_materializes_and_imports_episode() -> Result<()>
    {
        let database = setup_db().await?;
        let temp = TempDir::new()?;
        let mut candidate = hls_candidate();
        candidate["delivery"]["resolveHandle"] = json!("fixture-fmab-s01e02-hls-refresh");
        candidate["delivery"]["expiresAt"] = json!("2001-01-01T00:00:00Z");
        let (release_id, _) = seed_direct_file_release(&database.pool, candidate).await?;
        let resolver = FakeLateResolver {
            calls: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            result: Some(StreamCandidateResolveResult {
                delivery: Some(json!({
                    "streamType": "hls",
                    "url": "https://cdn.example.test/show/s01e02/fresh-master.m3u8",
                    "headers": {
                        "user-agent": "ElixirCloudStreamFixture/1.0"
                    }
                })),
                candidate: None,
                warnings: Vec::new(),
            }),
            fail_message: None,
        };
        let remuxer = FakeStreamRemuxer {
            observed: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            fail_message: None,
            progress: vec![StreamRemuxProgress {
                out_time_seconds: Some(1440.0),
                out_time_raw: Some(1_440_000_000),
                speed: Some("8.0x".to_string()),
                output_bytes: Some(2048),
            }],
        };
        let config = HttpStreamMaterializerConfig {
            paths: MaterializerPaths::from_downloads_root(temp.path().join("downloads")),
            batch_limit: 10,
        };

        let stats = process_http_stream_materializer_once_with_services(
            &database.pool,
            &config,
            &FakeDirectFileClient {
                chunks: Vec::new(),
                content_length: None,
                content_type: None,
                content_disposition: None,
                observed_headers: Arc::new(tokio::sync::Mutex::new(Vec::new())),
                observed_referer: Arc::new(tokio::sync::Mutex::new(None)),
                block_on_second_chunk: None,
            },
            &remuxer,
            &fake_probe(verified_probe_evidence()),
            &resolver,
        )
        .await?;
        assert_eq!(stats.completed, 1);
        assert_eq!(resolver.calls.lock().await.len(), 1);

        let import_stats = run_acquisition_import_iteration(&database.pool, 10).await?;
        assert_eq!(import_stats.runs_imported, 1);
        let jobs = list_release_jobs(&database.pool, release_id).await?;
        let run = get_import_run_by_release_job(&database.pool, jobs[0].release_job_id)
            .await?
            .expect("episode import run");
        assert_eq!(run.state, AcquisitionImportRunState::Imported);
        let links = list_import_file_links(&database.pool, run.import_run_id).await?;
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].state, AcquisitionImportFileLinkState::Imported);
        assert!(links[0].episode_id.is_some());
        assert!(links[0].movie_id.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn cs13_broken_hls_link_fails_safely_without_importing() -> Result<()> {
        let database = setup_db().await?;
        let temp = TempDir::new()?;
        let candidate = hls_candidate();
        let (release_id, _) = seed_direct_file_release(&database.pool, candidate).await?;
        let remuxer = FakeStreamRemuxer {
            observed: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            fail_message: Some("fixture HLS URL returned 404".to_string()),
            progress: Vec::new(),
        };
        let config = HttpStreamMaterializerConfig {
            paths: MaterializerPaths::from_downloads_root(temp.path().join("downloads")),
            batch_limit: 10,
        };

        let stats = process_http_stream_materializer_once_with_services(
            &database.pool,
            &config,
            &FakeDirectFileClient {
                chunks: Vec::new(),
                content_length: None,
                content_type: None,
                content_disposition: None,
                observed_headers: Arc::new(tokio::sync::Mutex::new(Vec::new())),
                observed_referer: Arc::new(tokio::sync::Mutex::new(None)),
                block_on_second_chunk: None,
            },
            &remuxer,
            &fake_probe(verified_probe_evidence()),
            &FakeLateResolver::default(),
        )
        .await?;

        assert_eq!(stats.failed, 1);
        let release = get_release(&database.pool, release_id)
            .await?
            .expect("failed release");
        assert_eq!(release.state, AcquisitionReleaseState::Failed);
        let import_ready = list_import_pending_release_jobs(&database.pool, 10).await?;
        assert!(import_ready.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn ess6_direct_file_materializer_cleans_partial_on_cancel() -> Result<()> {
        let database = setup_db().await?;
        let temp = TempDir::new()?;
        let candidate = direct_file_candidate();
        let (release_id, _) = seed_direct_file_release(&database.pool, candidate).await?;
        let release = get_release(&database.pool, release_id)
            .await?
            .expect("seeded release");
        let jobs = list_release_jobs(&database.pool, release_id).await?;
        let job_id = jobs[0].release_job_id;
        let block_second = Arc::new(Notify::new());
        let downloader = FakeDirectFileClient {
            chunks: vec![b"partial".to_vec(), b"ignored".to_vec()],
            content_length: Some(14),
            content_type: Some("video/mp4".to_string()),
            content_disposition: Some("attachment; filename=\"Cancel S01E02.mp4\"".to_string()),
            observed_headers: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            observed_referer: Arc::new(tokio::sync::Mutex::new(None)),
            block_on_second_chunk: Some(block_second.clone()),
        };
        let probe = fake_probe(verified_probe_evidence());
        let config = HttpStreamMaterializerConfig {
            paths: MaterializerPaths::from_downloads_root(temp.path().join("downloads")),
            batch_limit: 10,
        };
        let pool = database.pool.clone();
        let materializer = tokio::spawn(async move {
            process_http_stream_materializer_once_with_services(
                &pool,
                &config,
                &downloader,
                &FakeStreamRemuxer::default(),
                &probe,
                &FakeLateResolver::default(),
            )
            .await
        });

        let partial_path = wait_for_stream_partial_path(&database.pool, release_id).await?;
        update_release_job_state(
            &database.pool,
            job_id,
            ReleaseJobStateUpdate {
                state: ReleaseJobState::Cancelled,
                state_reason: Some("test cancellation".to_string()),
                active: Some(false),
                completed_at: Some(Utc::now()),
                ..Default::default()
            },
        )
        .await?;
        block_second.notify_one();

        let stats = materializer.await??;
        assert_eq!(stats.cancelled, 1);
        let release = get_release(&database.pool, release.release_id)
            .await?
            .expect("cancelled release");
        assert_eq!(release.state, AcquisitionReleaseState::Cancelled);
        let runtime = release
            .coverage_plan
            .as_ref()
            .and_then(|plan| plan.get("streamRuntime"))
            .expect("runtime evidence");
        assert_eq!(
            runtime.get("runtimeState").and_then(Value::as_str),
            Some("cancelled")
        );
        assert_eq!(
            runtime.get("partialPath").and_then(Value::as_str),
            Some(partial_path.as_str())
        );
        assert!(!Path::new(&partial_path).exists());
        Ok(())
    }

    #[tokio::test]
    async fn ess7_hls_materializer_stream_copies_tracks_progress_and_probes() -> Result<()> {
        let database = setup_db().await?;
        let temp = TempDir::new()?;
        let candidate = hls_candidate();
        let (release_id, _) = seed_direct_file_release(&database.pool, candidate).await?;
        let remuxer = FakeStreamRemuxer {
            observed: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            fail_message: None,
            progress: vec![StreamRemuxProgress {
                out_time_seconds: Some(720.0),
                out_time_raw: Some(720_000_000),
                speed: Some("4.0x".to_string()),
                output_bytes: Some(1024),
            }],
        };
        let probe = fake_probe(verified_probe_evidence());
        let config = HttpStreamMaterializerConfig {
            paths: MaterializerPaths::from_downloads_root(temp.path().join("downloads")),
            batch_limit: 10,
        };

        let stats = process_http_stream_materializer_once_with_services(
            &database.pool,
            &config,
            &FakeDirectFileClient {
                chunks: Vec::new(),
                content_length: None,
                content_type: None,
                content_disposition: None,
                observed_headers: Arc::new(tokio::sync::Mutex::new(Vec::new())),
                observed_referer: Arc::new(tokio::sync::Mutex::new(None)),
                block_on_second_chunk: None,
            },
            &remuxer,
            &probe,
            &FakeLateResolver::default(),
        )
        .await?;

        assert_eq!(stats.completed, 1);
        assert!(probe.called.load(Ordering::SeqCst));
        let observed = remuxer.observed.lock().await.clone();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].stream_type, StreamDeliveryType::Hls);
        assert_eq!(observed[0].duration_seconds, Some(1440.0));
        assert!(
            observed[0]
                .partial_path
                .to_string_lossy()
                .ends_with(".mkv.elixir-part")
        );

        let release = get_release(&database.pool, release_id)
            .await?
            .expect("completed hls stream release");
        assert_eq!(release.state, AcquisitionReleaseState::Completed);
        let runtime = release
            .coverage_plan
            .as_ref()
            .and_then(|plan| plan.get("streamRuntime"))
            .expect("runtime evidence");
        assert_eq!(
            runtime.get("runtimeState").and_then(Value::as_str),
            Some("completed")
        );
        assert_eq!(
            runtime.get("streamType").and_then(Value::as_str),
            Some("hls")
        );
        assert_eq!(
            runtime.pointer("/ffmpeg/copyMode").and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            runtime
                .pointer("/ffmpeg/finalProgress/speed")
                .and_then(Value::as_str),
            Some("4.0x")
        );
        let local_path = runtime
            .get("localPath")
            .and_then(Value::as_str)
            .expect("local path");
        assert!(local_path.ends_with(".mkv"));
        assert!(Path::new(local_path).is_file());
        let files = list_release_files(&database.pool, release_id).await?;
        assert_eq!(files.len(), 1);
        assert_eq!(
            files[0]
                .provider_metadata
                .as_ref()
                .and_then(|value| value.pointer("/streamMaterializer/streamType"))
                .and_then(Value::as_str),
            Some("hls")
        );
        Ok(())
    }

    #[tokio::test]
    async fn ess8_duration_mismatch_requires_review_before_import() -> Result<()> {
        let database = setup_db().await?;
        let temp = TempDir::new()?;
        let candidate = hls_candidate();
        let (release_id, _) = seed_direct_file_release(&database.pool, candidate).await?;
        let remuxer = FakeStreamRemuxer::default();
        let probe = fake_probe(wrong_duration_probe_evidence());
        let config = HttpStreamMaterializerConfig {
            paths: MaterializerPaths::from_downloads_root(temp.path().join("downloads")),
            batch_limit: 10,
        };

        let stats = process_http_stream_materializer_once_with_services(
            &database.pool,
            &config,
            &FakeDirectFileClient {
                chunks: Vec::new(),
                content_length: None,
                content_type: None,
                content_disposition: None,
                observed_headers: Arc::new(tokio::sync::Mutex::new(Vec::new())),
                observed_referer: Arc::new(tokio::sync::Mutex::new(None)),
                block_on_second_chunk: None,
            },
            &remuxer,
            &probe,
            &FakeLateResolver::default(),
        )
        .await?;

        assert_eq!(stats.review_required, 1);
        let release = get_release(&database.pool, release_id)
            .await?
            .expect("review stream release");
        assert_eq!(release.state, AcquisitionReleaseState::ReviewRequired);
        let runtime = release
            .coverage_plan
            .as_ref()
            .and_then(|plan| plan.get("streamRuntime"))
            .expect("runtime evidence");
        assert_eq!(
            runtime.get("runtimeState").and_then(Value::as_str),
            Some("review_required")
        );
        assert_eq!(
            runtime
                .pointer("/verification/mismatchClass")
                .and_then(Value::as_str),
            Some("probe_target_mismatch")
        );
        let coverage = crate::acquisition::release_resolution::store::list_release_coverage(
            &database.pool,
            release_id,
        )
        .await?;
        assert_eq!(coverage.len(), 1);
        assert_eq!(coverage[0].state, ReleaseCoverageState::ReviewRequired);
        let import_ready = list_import_pending_release_jobs(&database.pool, 10).await?;
        assert!(
            !import_ready
                .iter()
                .any(|candidate| candidate.release.release_id == release_id)
        );
        Ok(())
    }

    #[tokio::test]
    async fn ess8_language_mismatch_requires_review_before_import() -> Result<()> {
        let database = setup_db().await?;
        let temp = TempDir::new()?;
        let candidate = direct_file_candidate();
        let (release_id, _) = seed_direct_file_release(&database.pool, candidate).await?;
        let downloader = FakeDirectFileClient {
            chunks: vec![b"video".to_vec(), b"bytes".to_vec()],
            content_length: Some(10),
            content_type: Some("video/mp4".to_string()),
            content_disposition: Some("attachment; filename=\"Wrong Language.mp4\"".to_string()),
            observed_headers: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            observed_referer: Arc::new(tokio::sync::Mutex::new(None)),
            block_on_second_chunk: None,
        };
        let probe = fake_probe(wrong_language_probe_evidence());
        let config = HttpStreamMaterializerConfig {
            paths: MaterializerPaths::from_downloads_root(temp.path().join("downloads")),
            batch_limit: 10,
        };

        let stats = process_http_stream_materializer_once_with_services(
            &database.pool,
            &config,
            &downloader,
            &FakeStreamRemuxer::default(),
            &probe,
            &FakeLateResolver::default(),
        )
        .await?;

        assert_eq!(stats.review_required, 1);
        let release = get_release(&database.pool, release_id)
            .await?
            .expect("language review stream release");
        assert_eq!(release.state, AcquisitionReleaseState::ReviewRequired);
        let runtime = release
            .coverage_plan
            .as_ref()
            .and_then(|plan| plan.get("streamRuntime"))
            .expect("runtime evidence");
        assert_eq!(
            runtime
                .pointer("/verification/mismatchClass")
                .and_then(Value::as_str),
            Some("probe_language_mismatch")
        );
        assert_eq!(
            runtime
                .pointer("/verification/detectedAudioLanguages/0")
                .and_then(Value::as_str),
            Some("ru")
        );
        let coverage = crate::acquisition::release_resolution::store::list_release_coverage(
            &database.pool,
            release_id,
        )
        .await?;
        assert_eq!(coverage.len(), 1);
        assert_eq!(coverage[0].state, ReleaseCoverageState::ReviewRequired);
        Ok(())
    }

    #[tokio::test]
    async fn ess8_corrupt_probe_output_fails_candidate_and_removes_staged_file() -> Result<()> {
        let database = setup_db().await?;
        let temp = TempDir::new()?;
        let candidate = direct_file_candidate();
        let (release_id, _) = seed_direct_file_release(&database.pool, candidate).await?;
        let downloader = FakeDirectFileClient {
            chunks: vec![b"not".to_vec(), b"media".to_vec()],
            content_length: Some(8),
            content_type: Some("video/mp4".to_string()),
            content_disposition: Some("attachment; filename=\"Corrupt.mp4\"".to_string()),
            observed_headers: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            observed_referer: Arc::new(tokio::sync::Mutex::new(None)),
            block_on_second_chunk: None,
        };
        let probe = fake_probe(corrupt_probe_evidence());
        let config = HttpStreamMaterializerConfig {
            paths: MaterializerPaths::from_downloads_root(temp.path().join("downloads")),
            batch_limit: 10,
        };

        let stats = process_http_stream_materializer_once_with_services(
            &database.pool,
            &config,
            &downloader,
            &FakeStreamRemuxer::default(),
            &probe,
            &FakeLateResolver::default(),
        )
        .await?;

        assert_eq!(stats.failed, 1);
        let release = get_release(&database.pool, release_id)
            .await?
            .expect("failed stream release");
        assert_eq!(release.state, AcquisitionReleaseState::Failed);
        let runtime = release
            .coverage_plan
            .as_ref()
            .and_then(|plan| plan.get("streamRuntime"))
            .expect("runtime evidence");
        assert_eq!(
            runtime.get("failureClass").and_then(Value::as_str),
            Some("probe_corrupt_output")
        );
        assert_eq!(
            runtime
                .pointer("/verification/verificationState")
                .and_then(Value::as_str),
            Some("failed")
        );
        let local_path = runtime
            .get("localPath")
            .and_then(Value::as_str)
            .expect("local path");
        assert!(!Path::new(local_path).exists());
        let files = list_release_files(&database.pool, release_id).await?;
        assert!(files.is_empty());
        let coverage = crate::acquisition::release_resolution::store::list_release_coverage(
            &database.pool,
            release_id,
        )
        .await?;
        assert!(coverage.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn cs12_stream_materializer_failure_does_not_route_to_download_clients() -> Result<()> {
        let database = setup_db().await?;
        let temp = TempDir::new()?;
        let candidate = hls_candidate();
        let (release_id, _) = seed_direct_file_release(&database.pool, candidate).await?;
        let remuxer = FakeStreamRemuxer {
            observed: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            fail_message: Some("fixture stream copy failed".to_string()),
            progress: Vec::new(),
        };
        let config = HttpStreamMaterializerConfig {
            paths: MaterializerPaths::from_downloads_root(temp.path().join("downloads")),
            batch_limit: 10,
        };

        let stats = process_http_stream_materializer_once_with_services(
            &database.pool,
            &config,
            &FakeDirectFileClient {
                chunks: Vec::new(),
                content_length: None,
                content_type: None,
                content_disposition: None,
                observed_headers: Arc::new(tokio::sync::Mutex::new(Vec::new())),
                observed_referer: Arc::new(tokio::sync::Mutex::new(None)),
                block_on_second_chunk: None,
            },
            &remuxer,
            &fake_probe(verified_probe_evidence()),
            &FakeLateResolver::default(),
        )
        .await?;

        assert_eq!(stats.failed, 1);
        assert_eq!(remuxer.observed.lock().await.len(), 1);
        let release = get_release(&database.pool, release_id)
            .await?
            .expect("failed stream release");
        assert_eq!(release.state, AcquisitionReleaseState::Failed);
        assert_eq!(
            release.selected_route_logical_id.as_deref(),
            Some(HTTP_STREAM_DEFAULT_LOGICAL_ID)
        );
        assert_eq!(release.selected_provider_id, None);
        assert_eq!(
            release
                .selected_candidate
                .as_ref()
                .and_then(|candidate| stream_candidate_string(candidate, "/defaultRoute"))
                .as_deref(),
            Some(HTTP_STREAM_DEFAULT_LOGICAL_ID)
        );
        let jobs = list_release_jobs(&database.pool, release_id).await?;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].route_logical_id, HTTP_STREAM_DEFAULT_LOGICAL_ID);
        assert_ne!(
            jobs[0].route_logical_id,
            crate::download_broker::DEBRID_DEFAULT_LOGICAL_ID
        );
        assert_ne!(
            jobs[0].route_logical_id,
            crate::download_broker::TORRENT_DEFAULT_LOGICAL_ID
        );
        assert_ne!(
            jobs[0].route_logical_id,
            crate::download_broker::USENET_DEFAULT_LOGICAL_ID
        );
        let runtime = release
            .coverage_plan
            .as_ref()
            .and_then(|plan| plan.get("streamRuntime"))
            .expect("runtime evidence");
        assert_eq!(
            runtime.get("failureClass").and_then(Value::as_str),
            Some("ffmpeg_copy_failed")
        );
        Ok(())
    }

    #[tokio::test]
    async fn ess7_dash_drm_candidate_fails_before_ffmpeg() -> Result<()> {
        let database = setup_db().await?;
        let temp = TempDir::new()?;
        let candidate = dash_drm_candidate();
        let (release_id, _) = seed_direct_file_release(&database.pool, candidate).await?;
        let remuxer = FakeStreamRemuxer::default();
        let probe = fake_probe(verified_probe_evidence());
        let config = HttpStreamMaterializerConfig {
            paths: MaterializerPaths::from_downloads_root(temp.path().join("downloads")),
            batch_limit: 10,
        };

        let stats = process_http_stream_materializer_once_with_services(
            &database.pool,
            &config,
            &FakeDirectFileClient {
                chunks: Vec::new(),
                content_length: None,
                content_type: None,
                content_disposition: None,
                observed_headers: Arc::new(tokio::sync::Mutex::new(Vec::new())),
                observed_referer: Arc::new(tokio::sync::Mutex::new(None)),
                block_on_second_chunk: None,
            },
            &remuxer,
            &probe,
            &FakeLateResolver::default(),
        )
        .await?;

        assert_eq!(stats.failed, 1);
        assert!(remuxer.observed.lock().await.is_empty());
        assert!(!probe.called.load(Ordering::SeqCst));
        let release = get_release(&database.pool, release_id)
            .await?
            .expect("failed dash stream release");
        assert_eq!(release.state, AcquisitionReleaseState::Failed);
        let runtime = release
            .coverage_plan
            .as_ref()
            .and_then(|plan| plan.get("streamRuntime"))
            .expect("runtime evidence");
        assert_eq!(
            runtime.get("failureClass").and_then(Value::as_str),
            Some("unsupported_drm")
        );
        Ok(())
    }

    #[test]
    fn ess7_ffmpeg_remux_args_use_copy_without_transcode() -> Result<()> {
        let request = StreamRemuxRequest {
            stream_type: StreamDeliveryType::Hls,
            url: Url::parse("https://cdn.example.test/master.m3u8")?,
            headers: vec![("user-agent".to_string(), "ElixirTest/1.0".to_string())],
            referer: Some("https://stream.example.test".to_string()),
            partial_path: PathBuf::from("/tmp/output.mkv.elixir-part"),
            duration_seconds: Some(1200.0),
        };
        let args = build_ffmpeg_remux_args(&request, request.url.as_str());
        let args_joined = args.join(" ");
        assert!(args_joined.contains("-c copy"));
        assert!(args_joined.contains("-f matroska"));
        assert!(args_joined.contains("-progress pipe:1"));
        assert!(!args_joined.contains("libx264"));
        assert!(!args_joined.contains("aac "));
        assert!(args_joined.contains("-referer https://stream.example.test"));
        Ok(())
    }

    #[tokio::test]
    async fn ess7_ffmpeg_progress_parser_tracks_time_speed_and_bytes() -> Result<()> {
        let temp = TempDir::new()?;
        let partial = temp.path().join("progress.mkv.elixir-part");
        fs::write(&partial, b"partial").await?;
        let mut progress = FfmpegProgressState::default();
        progress
            .observe_line("out_time_ms=600000000", &partial)
            .await?;
        progress.observe_line("speed=2.5x", &partial).await?;
        progress.observe_line("total_size=8192", &partial).await?;
        let current = progress
            .current_progress(&partial)
            .await?
            .expect("progress");
        assert_eq!(current.out_time_seconds, Some(600.0));
        assert_eq!(current.speed.as_deref(), Some("2.5x"));
        assert_eq!(current.output_bytes, Some(8192));
        assert_eq!(
            remux_progress_fraction(current.out_time_seconds, Some(1200.0)),
            Some(0.5)
        );
        Ok(())
    }

    async fn wait_for_stream_partial_path(pool: &AnyPool, release_id: Uuid) -> Result<String> {
        for _ in 0..50 {
            let release = get_release(pool, release_id).await?.expect("release");
            if let Some(path) = release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.get("streamRuntime"))
                .and_then(|runtime| runtime.get("partialPath"))
                .and_then(Value::as_str)
            {
                return Ok(path.to_string());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        bail!("stream partial path was not persisted");
    }
}
