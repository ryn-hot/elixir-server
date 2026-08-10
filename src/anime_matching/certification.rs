//! Physical ALM-9 local-inference/playback coexistence certification.
//!
//! This module is release tooling compiled against the production matcher. It
//! has no HTTP route or user configuration surface. The runner starts the
//! exact packaged worker through [`LocalModelEngine`], exercises the real
//! fallback service boundary, replays a hardware-transcode command emitted by
//! playback certification, keeps an actual byte-transfer and a real Elixir API
//! endpoint active, and writes only raw observations. The independent Python
//! gate derives percentiles and decides pass/fail.

use std::{
    collections::{BTreeMap, HashSet},
    fs::File,
    io::{Read, SeekFrom},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use async_trait::async_trait;
use reqwest::{Client, StatusCode, Url, redirect::Policy};
use semver::Version;
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{Error as DeError, MapAccess, SeqAccess, Visitor},
};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    process::{Child, Command},
    task::JoinHandle,
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;

use crate::{
    anime_matching::{
        ANIME_MATCH_PROMPT_REVISION, AnimeArtifactUrlPolicy, AnimeBundleCompatibilityPolicy,
        AnimeBundleQualificationGate, AnimeDeterministicResult, AnimeExecutionBackend,
        AnimeInferenceBundleManifest, AnimeKvCacheType, AnimeMatchAssistResult, AnimeMatchEngine,
        AnimeMatchEngineOutput, AnimeMatchRequest, AnimeMatchResponse, AnimeMatchSourceMap,
        AnimeMatchingService, AnimeRuntimeArtifactManifest, AnimeRuntimeBackend,
        AnimeRuntimeProbeResult, AnimeRuntimeProfile, DeterministicMatchState, LocalModelAdmission,
        LocalModelAdmissionPhase, LocalModelEngine, LocalModelRuntimeProfile,
        LocalModelSamplingProfile, LocalModelWorkerState, PreparedAnimeMatchRequest,
        ValidatedAnimeBundle, assess_inference_memory_pressure,
        collect_inference_hardware_inventory, collect_inference_memory_pressure,
        extract_anime_runtime_for_qualification, inference_hardware_fingerprint,
        validate_anime_bundle, validate_anime_match_request,
    },
    playback::{
        certification::{
            CertificationReport, HostGpuReport, HostOsReport, artifact_tree_digest,
            collect_certification_host_identity,
        },
        hardware::collect_host_hardware_inventory,
    },
};

use super::prime::profile_probe_response_passed;

const OBSERVATION_SCHEMA_VERSION: u32 = 2;
const REQUEST_CORPUS_SCHEMA_VERSION: u32 = 1;
const IDLE_REQUESTS: usize = 10;
const COEXISTENCE_REQUESTS: usize = 50;
const STABILITY_REQUESTS: usize = 100;
const API_BASELINE_REQUESTS: usize = 20;
const MAX_JSON_BYTES: u64 = 64 * 1024 * 1024;
const API_RESPONSE_LIMIT: u64 = 256 * 1024;
const PLAYBACK_BASELINE_RUNS: usize = 2;
const PLAYBACK_BASELINE_SECONDS: f64 = 16.0;
const PLAYBACK_COEXISTENCE_SECONDS: f64 = 900.0;
const PLAYBACK_READY_TIMEOUT: Duration = Duration::from_secs(45);
const PLAYBACK_POLL: Duration = Duration::from_millis(100);
const API_SAMPLE_PAUSE: Duration = Duration::from_millis(25);
const DOWNLOAD_CHUNK_BYTES: usize = 64 * 1024;
const MAX_WORKLOAD_SAMPLES: usize = 20_000;
const DOWNLOAD_CHUNK_INTERVAL: Duration = Duration::from_millis(32);
const CERTIFICATION_DEADLINE: Duration = Duration::from_millis(1);
const WORKER_RECOVERY_TIMEOUT: Duration = Duration::from_secs(20);
const HARDWARE_REQUEST_CORPUS_BYTES: &[u8] =
    include_bytes!("fixtures/hardware-certification-requests.json");

#[derive(Debug, Clone)]
pub struct AnimeInferenceHardwareCertificationConfig {
    pub target_id: String,
    pub runtime_id: String,
    pub commit_sha: String,
    pub run_id: String,
    pub manifest_path: PathBuf,
    pub runtime_profile_path: PathBuf,
    pub model_path: PathBuf,
    pub runtime_artifact_path: PathBuf,
    pub request_corpus_path: PathBuf,
    pub playback_report_path: PathBuf,
    pub playback_command_path: PathBuf,
    pub api_url: String,
    pub output_path: PathBuf,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CertificationRequestCorpus {
    schema_version: u32,
    status: String,
    requests: Vec<AnimeMatchRequest>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PlaybackCommandArtifact {
    tool: String,
    label: String,
    args: Vec<String>,
    source: PathBuf,
    output_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct HostObservation {
    os_family: String,
    os_version: Option<String>,
    arch: String,
    gpu_vendor: Option<String>,
    gpu_model: Option<String>,
    gpu_device_id: Option<String>,
    gpu_driver_version: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct RequestObservation {
    status: &'static str,
    latency_ms: f64,
    deterministic_equivalent: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PlaybackObservation {
    active_during_all_inference: bool,
    failures: u64,
    dropped_realtime_transcodes: u64,
    rebuffer_events: u64,
    baseline_startup_p95_ms: f64,
    coexistence_startup_p95_ms: f64,
    baseline_segment_p95_ms: f64,
    coexistence_segment_p95_ms: f64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DownloadObservation {
    active_during_all_inference: bool,
    bytes_transferred: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ApiObservation {
    failures: u64,
    baseline_latency_ms: Vec<f64>,
    coexistence_latency_ms: Vec<f64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct InferenceObservation {
    idle_latency_ms: Vec<f64>,
    coexistence_requests: Vec<RequestObservation>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StabilityObservation {
    requests: Vec<RequestObservation>,
    worker_rss_bytes: Vec<u64>,
    swap_bytes: [u64; 2],
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct FallbackCheck {
    triggered: bool,
    deterministic_equivalent: bool,
    latency_ms: f64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct FallbackObservation {
    deadline: FallbackCheck,
    resource_envelope: FallbackCheck,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkerLifecycleObservation {
    crash: WorkerCrashObservation,
    deadline: WorkerDeadlineObservation,
    shutdown: WorkerShutdownObservation,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkerCrashObservation {
    original_process_id: u32,
    replacement_process_id: u32,
    original_process_reaped: bool,
    model_recovered: bool,
    exactly_one_restart: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkerDeadlineObservation {
    terminated_process_id: u32,
    replacement_process_id: u32,
    terminated_process_reaped: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkerShutdownObservation {
    terminated_process_id: u32,
    process_reaped: bool,
    engine_process_cleared: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ThroughputObservation {
    prompt_tokens_per_second: f64,
    generation_tokens_per_second: f64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct HardwareObservation {
    schema_version: u32,
    status: &'static str,
    target_id: String,
    runtime_id: String,
    commit_sha: String,
    run_id: String,
    host: HostObservation,
    runtime_profile_host_fingerprint: String,
    playback: PlaybackObservation,
    download: DownloadObservation,
    api: ApiObservation,
    inference: InferenceObservation,
    stability: StabilityObservation,
    fallback: FallbackObservation,
    worker_lifecycle: WorkerLifecycleObservation,
    throughput: ThroughputObservation,
    cpu_time_ms: f64,
    artifact_download_bytes: u64,
    peak_device_memory_bytes: Option<u64>,
    memory_pressure_warnings: u64,
    worker_crashes: u64,
    skipped_checks: Vec<String>,
}

struct PreparedRuntime {
    engine: LocalModelEngine,
    admission: Arc<ToggleAdmission>,
    _extraction: TempDir,
    profile: AnimeRuntimeProfile,
}

#[derive(Debug, Default)]
struct ToggleAdmission {
    reject_inference: AtomicBool,
}

impl LocalModelAdmission for ToggleAdmission {
    fn admit(
        &self,
        phase: LocalModelAdmissionPhase,
        _profile: &LocalModelRuntimeProfile,
    ) -> Result<()> {
        if self.reject_inference.load(Ordering::Acquire)
            && matches!(phase, LocalModelAdmissionPhase::Inference)
        {
            bail!("ALM-9 injected resource-envelope rejection")
        }
        Ok(())
    }
}

#[derive(Clone)]
struct RealWorkerDeadlineEngine {
    engine: LocalModelEngine,
}

#[async_trait]
impl AnimeMatchEngine for RealWorkerDeadlineEngine {
    async fn match_candidates(&self, request: AnimeMatchRequest) -> Result<AnimeMatchResponse> {
        Ok(self
            .engine
            .match_with_deadline_for_certification(request, CERTIFICATION_DEADLINE)
            .await?
            .response)
    }

    async fn match_candidates_with_provenance(
        &self,
        request: AnimeMatchRequest,
    ) -> Result<AnimeMatchEngineOutput> {
        self.engine
            .match_with_deadline_for_certification(request, CERTIFICATION_DEADLINE)
            .await
    }
}

#[derive(Debug)]
struct PlaybackMetrics {
    startup_ms: f64,
    segment_gaps_ms: Vec<f64>,
    exited_early: bool,
    failed: bool,
}

struct RunningPlayback {
    child: Child,
    output_dir: PathBuf,
    started: Instant,
    seen_segments: HashSet<PathBuf>,
    segment_seen_at: Vec<Instant>,
    startup_ms: Option<f64>,
}

impl RunningPlayback {
    async fn scan(&mut self) -> Result<()> {
        let mut paths = Vec::new();
        collect_segment_paths(&self.output_dir, &mut paths)?;
        paths.sort();
        for path in paths {
            if self.seen_segments.insert(path) {
                let now = Instant::now();
                if self.startup_ms.is_none() {
                    self.startup_ms = Some(duration_ms(self.started.elapsed()));
                }
                self.segment_seen_at.push(now);
            }
        }
        Ok(())
    }

    async fn wait_ready(&mut self) -> Result<()> {
        timeout(PLAYBACK_READY_TIMEOUT, async {
            loop {
                self.scan().await?;
                ensure!(
                    self.child.try_wait()?.is_none(),
                    "playback workload exited before its first segment"
                );
                if self.startup_ms.is_some() {
                    return Ok::<(), anyhow::Error>(());
                }
                sleep(PLAYBACK_POLL).await;
            }
        })
        .await
        .context("playback workload did not produce a segment before deadline")??;
        Ok(())
    }

    async fn is_alive(&mut self) -> Result<bool> {
        self.scan().await?;
        Ok(self.child.try_wait()?.is_none())
    }

    async fn finish(mut self, intentional_stop: bool) -> Result<PlaybackMetrics> {
        let status = if intentional_stop {
            let _ = self.child.start_kill();
            self.child.wait().await.ok()
        } else {
            Some(self.child.wait().await?)
        };
        self.scan().await?;
        let failed = !intentional_stop && status.is_none_or(|status| !status.success());
        let startup_ms = self
            .startup_ms
            .ok_or_else(|| anyhow!("playback workload produced no media segment"))?;
        let segment_gaps_ms = self
            .segment_seen_at
            .windows(2)
            .map(|pair| duration_ms(pair[1].duration_since(pair[0])))
            .collect::<Vec<_>>();
        ensure!(
            !segment_gaps_ms.is_empty(),
            "playback workload produced fewer than two media segments"
        );
        Ok(PlaybackMetrics {
            startup_ms,
            segment_gaps_ms,
            exited_early: !intentional_stop,
            failed,
        })
    }
}

struct ApiSampler {
    cancel: CancellationToken,
    task: JoinHandle<()>,
    latencies: Arc<Mutex<Vec<f64>>>,
    failures: Arc<AtomicU64>,
}

struct DownloadWorkload {
    cancel: CancellationToken,
    server_task: JoinHandle<()>,
    client_task: JoinHandle<()>,
    bytes: Arc<AtomicU64>,
    failed: Arc<AtomicBool>,
    output_path: PathBuf,
}

pub async fn run_anime_inference_hardware_certification(
    config: AnimeInferenceHardwareCertificationConfig,
) -> Result<()> {
    validate_external_identity(&config)?;
    let manifest: AnimeInferenceBundleManifest =
        read_strict_json(&config.manifest_path, "candidate manifest")?;
    let bundle = validate_candidate_manifest(manifest)?;
    let profile: AnimeRuntimeProfile =
        read_strict_json(&config.runtime_profile_path, "runtime profile")?;
    let requests = read_frozen_request_corpus(&config.request_corpus_path)?;
    validate_request_corpus(&requests)?;
    let playback_report: JsonValue = read_strict_json(
        &config.playback_report_path,
        "playback certification report",
    )?;
    let typed_playback_report: CertificationReport =
        serde_json::from_value(playback_report.clone())
            .context("validating typed playback certification report")?;
    validate_playback_artifact_digest(&typed_playback_report, &config.playback_report_path)?;
    let playback_command: PlaybackCommandArtifact =
        read_strict_json(&config.playback_command_path, "playback command artifact")?;
    let host = host_from_playback_report(&playback_report)?;
    validate_playback_binding(&playback_report, &config, &host)?;
    validate_playback_command_binding(
        &playback_report,
        &config.playback_report_path,
        &config.playback_command_path,
    )?;
    let api_url = validate_api_url(&config.api_url)?;

    let current_host = collect_host_hardware_inventory().await;
    let current_hardware = collect_inference_hardware_inventory(current_host.clone()).await;
    let (current_playback_os, current_playback_gpu) = collect_certification_host_identity().await;
    validate_current_host_binding(&host, &current_playback_os, &current_playback_gpu)?;
    ensure!(
        profile
            .host_fingerprint
            .eq_ignore_ascii_case(&inference_hardware_fingerprint(&current_hardware)),
        "sealed runtime profile belongs to a different current host or driver state"
    );

    let prepared = prepare_runtime(&config, bundle.manifest(), profile).await?;
    prepared
        .engine
        .prime()
        .await
        .context("priming exact candidate worker")?;
    let initial_worker = ready_worker_pid(&prepared.engine).await?;
    let priming_request = requests.requests[0].clone();
    let priming_request_id = priming_request.request_id.clone();
    let priming = prepared
        .engine
        .benchmark_match(priming_request)
        .await
        .with_context(|| format!("priming warm inference with {priming_request_id}"))?;
    ensure!(
        profile_probe_response_passed(&priming_request_id, &priming.output.response),
        "warm inference priming request returned the wrong mapping"
    );
    ensure!(
        current_worker_pid(&prepared.engine).await == Some(initial_worker),
        "warm inference priming replaced the packaged worker"
    );
    let service = AnimeMatchingService::with_engine(Arc::new(prepared.engine.clone()));

    let api_client = api_client()?;
    let mut baseline_api = Vec::with_capacity(API_BASELINE_REQUESTS);
    let mut api_failures = 0_u64;
    for _ in 0..API_BASELINE_REQUESTS {
        match sample_api(&api_client, api_url.clone()).await {
            Ok(latency) => baseline_api.push(latency),
            Err(_) => {
                api_failures += 1;
                baseline_api.push(0.0);
            }
        }
    }

    let playback_root = tempfile::Builder::new()
        .prefix("elixir-alm9-playback-")
        .tempdir()?;
    let mut baseline_startups = Vec::with_capacity(PLAYBACK_BASELINE_RUNS);
    let mut baseline_segments = Vec::new();
    for index in 0..PLAYBACK_BASELINE_RUNS {
        let output = playback_root.path().join(format!("baseline-{index}"));
        let process = start_playback(&playback_command, &output, PLAYBACK_BASELINE_SECONDS).await?;
        let metrics = finish_natural_playback(process, PLAYBACK_BASELINE_SECONDS).await?;
        ensure!(!metrics.failed, "baseline playback command failed");
        baseline_startups.push(metrics.startup_ms);
        baseline_segments.extend(metrics.segment_gaps_ms);
    }

    let mut idle_latencies = Vec::with_capacity(IDLE_REQUESTS);
    let mut prompt_tokens = 0_u64;
    let mut generated_tokens = 0_u64;
    let mut prompt_time_ms = 0_u64;
    let mut generation_time_ms = 0_u64;
    for index in 0..IDLE_REQUESTS {
        let request = requests.requests[(index + 1) % requests.requests.len()].clone();
        let measured = prepared.engine.benchmark_match(request).await?;
        idle_latencies.push(measured.elapsed_ms as f64);
        prompt_tokens = prompt_tokens.saturating_add(measured.prompt_tokens);
        generated_tokens = generated_tokens.saturating_add(measured.generated_tokens);
        prompt_time_ms = prompt_time_ms.saturating_add(measured.prompt_time_ms);
        generation_time_ms = generation_time_ms.saturating_add(measured.generation_time_ms);
    }

    let before_hardware =
        collect_inference_hardware_inventory(collect_host_hardware_inventory().await).await;
    let swap_before = system_swap_bytes().await?;
    let mut memory_pressure_warnings =
        u64::from(assess_inference_memory_pressure(&before_hardware.memory, None).under_pressure);
    let before_device_available = exact_device_available_bytes(&before_hardware);

    let api_sampler = start_api_sampler(api_client.clone(), api_url.clone());
    let download = start_download_workload(
        &config.model_path,
        &playback_root.path().join("active-model-download.partial"),
    )
    .await?;
    let coexist_output = playback_root.path().join("coexistence");
    let mut coexist_playback = start_playback(
        &playback_command,
        &coexist_output,
        PLAYBACK_COEXISTENCE_SECONDS,
    )
    .await?;
    let first_inference = run_service_request(&service, &requests.requests[0]);
    let (playback_ready, first_inference) =
        tokio::join!(coexist_playback.wait_ready(), first_inference);
    playback_ready?;
    let coexist_startup = coexist_playback
        .startup_ms
        .expect("wait_ready sets startup");
    let mut active_playback = true;
    let mut active_download =
        !download.failed.load(Ordering::Acquire) && !download.client_task.is_finished();
    let mut coexistence = Vec::with_capacity(COEXISTENCE_REQUESTS);
    coexistence.push(first_inference?);
    let mut worker_crashes = 0_u64;
    let mut expected_worker = initial_worker;
    active_playback &= coexist_playback.is_alive().await?;
    memory_pressure_warnings +=
        u64::from(collect_inference_memory_pressure(None).await.under_pressure);
    for index in 1..COEXISTENCE_REQUESTS {
        active_playback &= coexist_playback.is_alive().await?;
        active_download &=
            !download.failed.load(Ordering::Acquire) && !download.client_task.is_finished();
        coexistence.push(
            run_service_request(
                &service,
                &requests.requests[index % requests.requests.len()],
            )
            .await?,
        );
        memory_pressure_warnings +=
            u64::from(collect_inference_memory_pressure(None).await.under_pressure);
        active_playback &= coexist_playback.is_alive().await?;
        let current_worker = current_worker_pid(&prepared.engine).await;
        if current_worker != Some(expected_worker) {
            worker_crashes += 1;
            if let Some(pid) = current_worker {
                expected_worker = pid;
            }
        }
    }
    let coexist_metrics = coexist_playback.finish(true).await?;
    let api_result = stop_api_sampler(api_sampler).await;
    ensure!(
        api_result.0.len() >= API_BASELINE_REQUESTS,
        "coexistence API sampler produced fewer than {API_BASELINE_REQUESTS} real samples"
    );
    api_failures += api_result.1;
    let download_result = stop_download_workload(download).await;
    active_download &= download_result.1;

    // Deliberately crash the exact packaged worker, then make one ordinary
    // production service request. LocalModelEngine must detect that exit and
    // recover through its single-restart path without exposing a user choice.
    let crash_original_pid = ready_worker_pid(&prepared.engine).await?;
    ensure!(
        prepared
            .engine
            .crash_active_worker_for_certification()
            .await?
            == crash_original_pid,
        "worker crash certification terminated an unexpected process"
    );
    let crash_recovery = run_service_request(&service, &requests.requests[0]).await?;
    let crash_replacement_pid = ready_worker_pid(&prepared.engine).await?;
    let crash_original_reaped = wait_for_process_exit(crash_original_pid).await?;
    sleep(Duration::from_millis(250)).await;
    let crash_exactly_one_restart =
        current_worker_pid(&prepared.engine).await == Some(crash_replacement_pid);
    ensure!(
        crash_recovery.status == "model_success"
            && crash_replacement_pid != crash_original_pid
            && crash_original_reaped
            && crash_exactly_one_restart,
        "packaged worker crash did not produce exactly one successful managed restart"
    );
    expected_worker = crash_replacement_pid;

    // Exercise the real packaged worker with an intentionally shortened
    // release-only deadline. The production engine owns cancellation,
    // immediate process termination, and automatic replacement; the service
    // boundary must return the unchanged deterministic result.
    let deadline_worker_pid = expected_worker;
    let fallback_deadline = run_injected_fallback(
        AnimeMatchingService::with_engine(Arc::new(RealWorkerDeadlineEngine {
            engine: prepared.engine.clone(),
        })),
        &requests.requests[0],
    )
    .await?;
    ensure!(
        fallback_deadline.triggered && fallback_deadline.deterministic_equivalent,
        "real packaged-worker deadline did not return deterministic fallback"
    );
    let deadline_worker_reaped = wait_for_process_exit(deadline_worker_pid).await?;
    let deadline_replacement_pid =
        wait_for_replacement_worker(&prepared.engine, deadline_worker_pid).await?;
    sleep(Duration::from_millis(250)).await;
    ensure!(
        deadline_worker_reaped
            && current_worker_pid(&prepared.engine).await == Some(deadline_replacement_pid),
        "deadline worker was not reaped and replaced exactly once"
    );
    expected_worker = deadline_replacement_pid;

    prepared
        .admission
        .reject_inference
        .store(true, Ordering::Release);
    let fallback_resource = run_injected_fallback(service.clone(), &requests.requests[0]).await?;
    prepared
        .admission
        .reject_inference
        .store(false, Ordering::Release);

    let cpu_before = worker_cpu_time_ms(expected_worker).await?;
    let mut stability = Vec::with_capacity(STABILITY_REQUESTS);
    let first_rss = ready_worker_rss(&prepared.engine).await?;
    let mut rss = Vec::with_capacity(STABILITY_REQUESTS + 1);
    rss.push(first_rss);
    for index in 0..STABILITY_REQUESTS {
        stability.push(
            run_service_request(
                &service,
                &requests.requests[index % requests.requests.len()],
            )
            .await?,
        );
        memory_pressure_warnings +=
            u64::from(collect_inference_memory_pressure(None).await.under_pressure);
        rss.push(ready_worker_rss(&prepared.engine).await?);
        let current_worker = current_worker_pid(&prepared.engine).await;
        if current_worker != Some(expected_worker) {
            worker_crashes += 1;
            if let Some(pid) = current_worker {
                expected_worker = pid;
            }
        }
    }
    let cpu_after = worker_cpu_time_ms(expected_worker).await?;
    let swap_after = system_swap_bytes().await?;
    let after_hardware =
        collect_inference_hardware_inventory(collect_host_hardware_inventory().await).await;
    memory_pressure_warnings +=
        u64::from(assess_inference_memory_pressure(&after_hardware.memory, None).under_pressure);
    let after_device_available = exact_device_available_bytes(&after_hardware);
    let observed_device = before_device_available
        .zip(after_device_available)
        .map(|(before, after)| before.saturating_sub(after))
        .filter(|value| *value > 0);
    let peak_device_memory_bytes = [prepared.profile.peak_device_memory_bytes, observed_device]
        .into_iter()
        .flatten()
        .max();

    let shutdown_worker_pid = ready_worker_pid(&prepared.engine).await?;
    prepared.engine.shutdown().await;
    let shutdown_process_reaped = wait_for_process_exit(shutdown_worker_pid).await?;
    let shutdown_engine_process_cleared = prepared.engine.snapshot().await.process_id.is_none();
    ensure!(
        shutdown_process_reaped && shutdown_engine_process_cleared,
        "packaged worker remained alive or registered after engine shutdown"
    );
    let final_manifest: AnimeInferenceBundleManifest = read_strict_json(
        &config.manifest_path,
        "candidate manifest after certification",
    )?;
    let final_bundle = validate_candidate_manifest(final_manifest)?;
    ensure!(
        final_bundle
            .manifest_fingerprint()
            .eq_ignore_ascii_case(bundle.manifest_fingerprint()),
        "candidate manifest changed during hardware certification"
    );
    let final_profile: AnimeRuntimeProfile = read_strict_json(
        &config.runtime_profile_path,
        "runtime profile after certification",
    )?;
    ensure!(
        final_profile == prepared.profile,
        "sealed runtime profile changed during hardware certification"
    );
    ensure!(
        read_frozen_request_corpus(&config.request_corpus_path)? == requests,
        "hardware request corpus changed during certification"
    );
    verify_artifact(
        &config.model_path,
        &bundle.manifest().model.sha256,
        bundle.manifest().model.size_bytes,
        "model after certification",
    )?;
    let final_runtime = bundle
        .manifest()
        .runtimes
        .iter()
        .find(|runtime| runtime.artifact_key() == prepared.profile.runtime_artifact_key)
        .ok_or_else(|| anyhow!("sealed runtime disappeared from candidate manifest"))?;
    verify_artifact(
        &config.runtime_artifact_path,
        &final_runtime.sha256,
        final_runtime.size_bytes,
        "runtime artifact after certification",
    )?;
    let final_playback: JsonValue = read_strict_json(
        &config.playback_report_path,
        "playback certification report after coexistence",
    )?;
    let final_typed_playback: CertificationReport = serde_json::from_value(final_playback.clone())
        .context("validating final typed playback certification report")?;
    validate_playback_binding(&final_playback, &config, &host)?;
    validate_playback_artifact_digest(&final_typed_playback, &config.playback_report_path)?;
    ensure!(
        host_from_playback_report(&final_playback)? == host,
        "playback host identity changed during hardware certification"
    );
    let cpu_delta = cpu_after - cpu_before;
    ensure!(
        cpu_delta.is_finite() && cpu_delta > 0.0,
        "worker CPU-time measurement was not positive"
    );
    let cpu_time_ms = cpu_delta / STABILITY_REQUESTS as f64;
    let baseline_startup_p95 = percentile(&baseline_startups, 0.95)?;
    let baseline_segment_p95 = percentile(&baseline_segments, 0.95)?;
    let coexist_segment_p95 = percentile(&coexist_metrics.segment_gaps_ms, 0.95)?;
    let rebuffer_threshold = (baseline_segment_p95 * 2.0).max(12_000.0);
    let rebuffer_events = coexist_metrics
        .segment_gaps_ms
        .iter()
        .filter(|gap| **gap > rebuffer_threshold)
        .count() as u64;
    let playback_failures = u64::from(coexist_metrics.failed);
    let dropped = u64::from(!active_playback || coexist_metrics.exited_early);
    let artifact_download_bytes = file_size(&config.model_path, "model")?
        .checked_add(file_size(
            &config.runtime_artifact_path,
            "runtime artifact",
        )?)
        .ok_or_else(|| anyhow!("artifact byte count overflow"))?;

    let observation = HardwareObservation {
        schema_version: OBSERVATION_SCHEMA_VERSION,
        status: "complete",
        target_id: config.target_id,
        runtime_id: config.runtime_id,
        commit_sha: config.commit_sha,
        run_id: config.run_id,
        host,
        runtime_profile_host_fingerprint: prepared.profile.host_fingerprint.clone(),
        playback: PlaybackObservation {
            active_during_all_inference: active_playback,
            failures: playback_failures,
            dropped_realtime_transcodes: dropped,
            rebuffer_events,
            baseline_startup_p95_ms: baseline_startup_p95,
            coexistence_startup_p95_ms: coexist_startup,
            baseline_segment_p95_ms: baseline_segment_p95,
            coexistence_segment_p95_ms: coexist_segment_p95,
        },
        download: DownloadObservation {
            active_during_all_inference: active_download,
            bytes_transferred: download_result.0,
        },
        api: ApiObservation {
            failures: api_failures,
            baseline_latency_ms: baseline_api,
            coexistence_latency_ms: api_result.0,
        },
        inference: InferenceObservation {
            idle_latency_ms: idle_latencies,
            coexistence_requests: coexistence,
        },
        stability: StabilityObservation {
            requests: stability,
            worker_rss_bytes: rss,
            swap_bytes: [swap_before, swap_after],
        },
        fallback: FallbackObservation {
            deadline: fallback_deadline,
            resource_envelope: fallback_resource,
        },
        worker_lifecycle: WorkerLifecycleObservation {
            crash: WorkerCrashObservation {
                original_process_id: crash_original_pid,
                replacement_process_id: crash_replacement_pid,
                original_process_reaped: crash_original_reaped,
                model_recovered: crash_recovery.status == "model_success",
                exactly_one_restart: crash_exactly_one_restart,
            },
            deadline: WorkerDeadlineObservation {
                terminated_process_id: deadline_worker_pid,
                replacement_process_id: deadline_replacement_pid,
                terminated_process_reaped: deadline_worker_reaped,
            },
            shutdown: WorkerShutdownObservation {
                terminated_process_id: shutdown_worker_pid,
                process_reaped: shutdown_process_reaped,
                engine_process_cleared: shutdown_engine_process_cleared,
            },
        },
        throughput: ThroughputObservation {
            prompt_tokens_per_second: prompt_tokens as f64 * 1_000.0 / prompt_time_ms as f64,
            generation_tokens_per_second: generated_tokens as f64 * 1_000.0
                / generation_time_ms as f64,
        },
        cpu_time_ms,
        artifact_download_bytes,
        peak_device_memory_bytes,
        memory_pressure_warnings,
        worker_crashes,
        skipped_checks: Vec::new(),
    };
    write_new_json(&config.output_path, &observation)
}

fn validate_external_identity(config: &AnimeInferenceHardwareCertificationConfig) -> Result<()> {
    ensure!(
        !config.target_id.trim().is_empty() && !config.runtime_id.trim().is_empty(),
        "target/runtime identity is empty"
    );
    ensure!(
        config.commit_sha.len() == 40
            && config
                .commit_sha
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()),
        "commit SHA must be 40 lowercase hexadecimal characters"
    );
    ensure!(
        !config.run_id.is_empty()
            && config.run_id != "0"
            && config.run_id.bytes().all(|byte| byte.is_ascii_digit()),
        "run ID must be a positive decimal value"
    );
    ensure!(
        std::fs::symlink_metadata(&config.output_path)
            .is_err_and(|error| { error.kind() == std::io::ErrorKind::NotFound }),
        "observation output already exists or cannot be inspected"
    );
    for input in [
        &config.manifest_path,
        &config.runtime_profile_path,
        &config.model_path,
        &config.runtime_artifact_path,
        &config.request_corpus_path,
        &config.playback_report_path,
        &config.playback_command_path,
    ] {
        ensure!(
            input != &config.output_path,
            "observation output aliases an input path"
        );
    }
    Ok(())
}

fn validate_candidate_manifest(
    manifest: AnimeInferenceBundleManifest,
) -> Result<ValidatedAnimeBundle> {
    let policy = AnimeBundleCompatibilityPolicy {
        server_version: Version::parse(env!("CARGO_PKG_VERSION"))
            .context("server package version is not semantic versioning")?,
        qualification_gate: AnimeBundleQualificationGate::DevelopmentAllowUnqualified,
        artifact_url_policy: AnimeArtifactUrlPolicy::HttpsOnly,
        require_complete_platform_matrix: true,
    };
    validate_anime_bundle(manifest, &policy).context("validating strict release candidate manifest")
}

fn validate_request_corpus(corpus: &CertificationRequestCorpus) -> Result<()> {
    ensure!(
        corpus.schema_version == REQUEST_CORPUS_SCHEMA_VERSION && corpus.status == "frozen",
        "hardware request corpus is not frozen schema v1"
    );
    ensure!(
        (1..=32).contains(&corpus.requests.len()),
        "hardware request corpus must contain 1..=32 requests"
    );
    for request in &corpus.requests {
        prepared_request(request.clone())?;
    }
    Ok(())
}

fn read_frozen_request_corpus(path: &Path) -> Result<CertificationRequestCorpus> {
    let bytes = read_regular_bytes(path, "hardware request corpus")?;
    ensure!(
        bytes == HARDWARE_REQUEST_CORPUS_BYTES,
        "hardware request corpus differs byte-for-byte from the compiled frozen corpus"
    );
    let corpus = read_strict_json(path, "hardware request corpus")?;
    validate_request_corpus(&corpus)?;
    Ok(corpus)
}

fn prepared_request(request: AnimeMatchRequest) -> Result<PreparedAnimeMatchRequest<usize, usize>> {
    let mut candidates = BTreeMap::new();
    let mut files = BTreeMap::new();
    for (candidate_index, candidate) in request.candidates.iter().enumerate() {
        candidates.insert(candidate.candidate_key.clone(), candidate_index);
        for (file_index, file) in candidate.files.iter().enumerate() {
            files.insert(
                file.file_key.clone(),
                (candidate.candidate_key.clone(), file_index),
            );
        }
    }
    let prepared = PreparedAnimeMatchRequest {
        request,
        source_map: AnimeMatchSourceMap::new(candidates, files),
    };
    validate_anime_match_request(&prepared).context("validating hardware certification request")?;
    Ok(prepared)
}

async fn prepare_runtime(
    config: &AnimeInferenceHardwareCertificationConfig,
    manifest: &AnimeInferenceBundleManifest,
    profile: AnimeRuntimeProfile,
) -> Result<PreparedRuntime> {
    profile
        .validate()
        .context("validating sealed runtime profile")?;
    ensure!(
        profile.probe_result != AnimeRuntimeProbeResult::DeterministicOnly,
        "hardware certification requires a model-capable profile"
    );
    ensure!(
        profile.bundle_version == manifest.bundle_version
            && profile.model_id == manifest.model.id
            && profile.model_revision == manifest.model.revision
            && profile.worker_revision == manifest.worker_revision,
        "runtime profile and candidate manifest identities differ"
    );
    let runtimes = manifest
        .runtimes
        .iter()
        .filter(|runtime| runtime.artifact_key() == profile.runtime_artifact_key)
        .collect::<Vec<_>>();
    ensure!(
        runtimes.len() == 1,
        "runtime profile does not resolve exactly one candidate runtime"
    );
    let runtime = runtimes[0];
    ensure!(
        runtime_id(runtime) == config.runtime_id,
        "runtime ID differs from sealed profile runtime"
    );
    ensure!(
        runtime_supports_execution(runtime.backend, profile.execution_backend),
        "runtime does not support selected execution backend"
    );
    ensure!(
        certification_allows_execution(runtime.backend, profile.execution_backend),
        "sealed profile did not execute a backend certifiable for the release runtime slot"
    );
    ensure!(
        profile.kv_cache_type == manifest.runtime_policy.kv_cache_type,
        "runtime profile KV cache differs from manifest"
    );
    verify_artifact(
        &config.model_path,
        &manifest.model.sha256,
        manifest.model.size_bytes,
        "model",
    )?;
    verify_artifact(
        &config.runtime_artifact_path,
        &runtime.sha256,
        runtime.size_bytes,
        "runtime artifact",
    )?;
    let extraction = tempfile::Builder::new()
        .prefix("elixir-alm9-cert-runtime-")
        .tempdir()?;
    let runtime_root = extraction.path().join("runtime");
    let worker = extract_anime_runtime_for_qualification(
        &config.runtime_artifact_path,
        &runtime_root,
        runtime,
    )
    .await?;
    let sampling = LocalModelSamplingProfile::default();
    ensure!(
        sampling.revision == manifest.runtime_policy.sampling_profile_revision,
        "candidate sampling profile is unsupported"
    );
    let local = LocalModelRuntimeProfile {
        bundle_version: manifest.bundle_version.clone(),
        model_id: manifest.model.id.clone(),
        model_revision: manifest.model.revision.clone(),
        worker_revision: manifest.worker_revision.clone(),
        backend: profile.execution_backend.as_str().to_string(),
        profile_fingerprint: profile.profile_fingerprint.clone(),
        protocol_version: manifest.protocol_version,
        matcher_schema_version: manifest.matcher_schema_version,
        prompt_revision: ANIME_MATCH_PROMPT_REVISION.to_string(),
        worker_path: worker,
        model_path: config.model_path.clone(),
        context_tokens: manifest.model.context_tokens,
        max_output_tokens: manifest.model.max_output_tokens,
        threads: u32::from(profile.cpu_thread_count),
        batch_threads: u32::from(profile.batch_thread_count),
        gpu_layers: profile.gpu_layer_count,
        kv_cache_type: match profile.kv_cache_type {
            AnimeKvCacheType::F16 => "f16",
            AnimeKvCacheType::Q8_0 => "q8_0",
        }
        .to_string(),
        peak_rss_bytes: profile.peak_rss_bytes,
        idle_unload_seconds: manifest.runtime_policy.idle_unload_seconds,
        sampling,
    };
    local.validate_contract()?;
    let admission = Arc::new(ToggleAdmission::default());
    let engine = LocalModelEngine::new_for_probe(admission.clone())?;
    engine.activate_profile_for_probe(local).await?;
    Ok(PreparedRuntime {
        engine,
        admission,
        _extraction: extraction,
        profile,
    })
}

pub(crate) fn runtime_id(runtime: &AnimeRuntimeArtifactManifest) -> String {
    format!(
        "{}-{}-{}",
        runtime.os.as_str(),
        runtime.arch.as_str(),
        runtime.backend.as_str().replace('_', "-")
    )
}

fn runtime_supports_execution(
    runtime: AnimeRuntimeBackend,
    execution: AnimeExecutionBackend,
) -> bool {
    match execution {
        AnimeExecutionBackend::Cpu => true,
        AnimeExecutionBackend::Metal => runtime == AnimeRuntimeBackend::MetalCpu,
        AnimeExecutionBackend::Cuda => runtime == AnimeRuntimeBackend::CudaCpu,
        AnimeExecutionBackend::Hip => runtime == AnimeRuntimeBackend::HipCpu,
        AnimeExecutionBackend::Vulkan => runtime == AnimeRuntimeBackend::VulkanCpu,
    }
}

fn certification_allows_execution(
    runtime: AnimeRuntimeBackend,
    execution: AnimeExecutionBackend,
) -> bool {
    match runtime {
        AnimeRuntimeBackend::MetalCpu => {
            matches!(
                execution,
                AnimeExecutionBackend::Metal | AnimeExecutionBackend::Cpu
            )
        }
        AnimeRuntimeBackend::CudaCpu => execution == AnimeExecutionBackend::Cuda,
        AnimeRuntimeBackend::HipCpu => execution == AnimeExecutionBackend::Hip,
        AnimeRuntimeBackend::VulkanCpu => execution == AnimeExecutionBackend::Vulkan,
        AnimeRuntimeBackend::Cpu => execution == AnimeExecutionBackend::Cpu,
    }
}

async fn run_service_request(
    service: &AnimeMatchingService,
    request: &AnimeMatchRequest,
) -> Result<RequestObservation> {
    let prepared = prepared_request(request.clone())?;
    let baseline = [0x5a_u8; 32];
    let started = Instant::now();
    let outcome = service
        .match_prepared_or_fallback(
            AnimeDeterministicResult {
                value: baseline,
                state: DeterministicMatchState::Difficult,
            },
            prepared,
            |_, _, _, _| Ok([0xa5_u8; 32]),
        )
        .await;
    let used_model = outcome.provenance.result == AnimeMatchAssistResult::Matched;
    Ok(RequestObservation {
        status: if used_model {
            "model_success"
        } else {
            "deterministic_fallback"
        },
        latency_ms: duration_ms(started.elapsed()),
        deterministic_equivalent: !used_model && outcome.value == baseline,
    })
}

async fn run_injected_fallback(
    service: AnimeMatchingService,
    request: &AnimeMatchRequest,
) -> Result<FallbackCheck> {
    let measured = run_service_request(&service, request).await?;
    Ok(FallbackCheck {
        triggered: measured.status == "deterministic_fallback",
        deterministic_equivalent: measured.deterministic_equivalent,
        latency_ms: measured.latency_ms,
    })
}

fn host_from_playback_report(report: &JsonValue) -> Result<HostObservation> {
    let os = report
        .get("os")
        .and_then(JsonValue::as_object)
        .ok_or_else(|| anyhow!("playback report lacks os"))?;
    let gpu = report
        .get("gpu")
        .and_then(JsonValue::as_object)
        .ok_or_else(|| anyhow!("playback report lacks gpu"))?;
    let required = |map: &serde_json::Map<String, JsonValue>, key: &str| -> Result<String> {
        map.get(key)
            .and_then(JsonValue::as_str)
            .map(str::to_string)
            .ok_or_else(|| anyhow!("playback report lacks string {key}"))
    };
    let optional =
        |map: &serde_json::Map<String, JsonValue>, key: &str| -> Result<Option<String>> {
            match map.get(key) {
                Some(JsonValue::String(value)) => Ok(Some(value.clone())),
                Some(JsonValue::Null) => Ok(None),
                _ => bail!("playback report {key} must be string or null"),
            }
        };
    Ok(HostObservation {
        os_family: required(os, "family")?,
        os_version: optional(os, "version")?,
        arch: required(os, "arch")?,
        gpu_vendor: optional(gpu, "vendor")?,
        gpu_model: optional(gpu, "model")?,
        gpu_device_id: optional(gpu, "device_id")?,
        gpu_driver_version: optional(gpu, "driver_version")?,
    })
}

fn validate_playback_binding(
    report: &JsonValue,
    config: &AnimeInferenceHardwareCertificationConfig,
    _host: &HostObservation,
) -> Result<()> {
    ensure!(
        report.get("status").and_then(JsonValue::as_str) == Some("passed"),
        "playback certification did not pass"
    );
    ensure!(
        report.get("target_id").and_then(JsonValue::as_str) == Some(config.target_id.as_str()),
        "playback target differs"
    );
    ensure!(
        report.get("commit_sha").and_then(JsonValue::as_str) == Some(config.commit_sha.as_str()),
        "playback commit differs"
    );
    let run = report
        .get("run_id")
        .ok_or_else(|| anyhow!("playback report lacks run_id"))?;
    ensure!(
        run.as_str()
            .map(str::to_string)
            .or_else(|| run.as_u64().map(|value| value.to_string()))
            .as_deref()
            == Some(config.run_id.as_str()),
        "playback run differs"
    );
    ensure!(
        report
            .get("artifact_digest")
            .and_then(JsonValue::as_str)
            .is_some(),
        "playback report lacks artifact digest"
    );
    Ok(())
}

fn validate_playback_artifact_digest(
    report: &CertificationReport,
    report_path: &Path,
) -> Result<()> {
    ensure!(
        report_path.file_name().and_then(|name| name.to_str()) == Some("certification.json"),
        "playback report must be the canonical certification.json artifact"
    );
    let artifact_root = report_path
        .parent()
        .ok_or_else(|| anyhow!("playback report lacks an artifact root"))?;
    let declared = report
        .artifact_digest
        .as_deref()
        .ok_or_else(|| anyhow!("playback report lacks artifact digest"))?;
    let computed = artifact_tree_digest(artifact_root, report)
        .context("recomputing playback certification artifact tree digest")?;
    ensure!(
        declared.eq_ignore_ascii_case(&computed),
        "playback certification artifact tree digest does not match its retained files"
    );
    Ok(())
}

fn validate_current_host_binding(
    retained: &HostObservation,
    current_os: &HostOsReport,
    current_gpu: &HostGpuReport,
) -> Result<()> {
    ensure!(
        retained.os_family == current_os.family
            && retained.arch == current_os.arch
            && retained.os_version == current_os.version,
        "playback certification OS identity differs from the current certification host"
    );
    ensure!(
        retained.gpu_vendor == current_gpu.vendor
            && retained.gpu_model == current_gpu.model
            && retained.gpu_device_id == current_gpu.device_id
            && retained.gpu_driver_version == current_gpu.driver_version,
        "playback certification GPU/driver identity differs from the current certification host"
    );
    Ok(())
}

fn validate_playback_command_binding(
    report: &JsonValue,
    report_path: &Path,
    command_path: &Path,
) -> Result<()> {
    let artifact_root = report_path
        .parent()
        .ok_or_else(|| anyhow!("playback report lacks an artifact root"))?
        .canonicalize()?;
    let command = command_path.canonicalize()?;
    ensure!(
        command.starts_with(&artifact_root),
        "playback command is outside the bound playback artifact tree"
    );
    let relative = command.strip_prefix(&artifact_root)?;
    let components = relative
        .components()
        .map(|part| part.as_os_str().to_string_lossy().to_string())
        .collect::<Vec<_>>();
    ensure!(
        components.len() == 3 && components[0] == "cases" && components[2] == "ffmpeg-command.json",
        "playback command is not a primary certified case command"
    );
    let case_id = &components[1];
    let case_reports = report
        .pointer("/cases/case_reports")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| anyhow!("playback report lacks case reports"))?;
    let case = case_reports
        .iter()
        .find(|case| case.get("id").and_then(JsonValue::as_str) == Some(case_id))
        .ok_or_else(|| anyhow!("playback command case is absent from certification report"))?;
    ensure!(
        case.get("status").and_then(JsonValue::as_str) == Some("passed")
            && case.get("hardware_used").and_then(JsonValue::as_bool) == Some(true),
        "playback coexistence command was not produced by a passing hardware case"
    );
    ensure!(
        case.get("artifacts")
            .and_then(JsonValue::as_array)
            .is_some_and(|artifacts| artifacts
                .iter()
                .any(|value| value.as_str() == Some("ffmpeg-command.json"))),
        "playback case does not declare the supplied command artifact"
    );
    ensure!(
        case.pointer("/performance_gate/passed")
            .and_then(JsonValue::as_bool)
            == Some(true),
        "playback coexistence command lacks a passing real-time performance gate"
    );
    Ok(())
}

fn validate_api_url(raw: &str) -> Result<Url> {
    let url = Url::parse(raw).context("parsing --api-url")?;
    ensure!(
        url.scheme() == "http",
        "certification API URL must use loopback HTTP"
    );
    ensure!(
        url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none(),
        "certification API URL contains credentials, query, or fragment"
    );
    let loopback = match url.host_str() {
        Some("localhost") => true,
        Some(host) => host
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback()),
        None => false,
    };
    ensure!(
        loopback && url.port().is_some(),
        "certification API URL must name an explicit loopback port"
    );
    Ok(url)
}

fn api_client() -> Result<Client> {
    Client::builder()
        .no_proxy()
        .redirect(Policy::none())
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(5))
        .build()
        .context("building certification API client")
}

async fn sample_api(client: &Client, url: Url) -> Result<f64> {
    let started = Instant::now();
    let response = client.get(url).send().await?;
    ensure!(
        response.status() == StatusCode::OK,
        "Elixir API returned {}",
        response.status()
    );
    if let Some(length) = response.content_length() {
        ensure!(
            length <= API_RESPONSE_LIMIT,
            "Elixir API response is too large"
        );
    }
    let bytes = response.bytes().await?;
    ensure!(
        !bytes.is_empty() && bytes.len() as u64 <= API_RESPONSE_LIMIT,
        "Elixir API response size is invalid"
    );
    Ok(duration_ms(started.elapsed()))
}

fn start_api_sampler(client: Client, url: Url) -> ApiSampler {
    let cancel = CancellationToken::new();
    let task_cancel = cancel.clone();
    let latencies = Arc::new(Mutex::new(Vec::new()));
    let task_latencies = latencies.clone();
    let failures = Arc::new(AtomicU64::new(0));
    let task_failures = failures.clone();
    let task = tokio::spawn(async move {
        while !task_cancel.is_cancelled() {
            match sample_api(&client, url.clone()).await {
                Ok(value) => {
                    let mut values = task_latencies
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if values.len() < MAX_WORKLOAD_SAMPLES {
                        values.push(value);
                    }
                }
                Err(_) => {
                    task_failures.fetch_add(1, Ordering::AcqRel);
                }
            }
            sleep(API_SAMPLE_PAUSE).await;
        }
    });
    ApiSampler {
        cancel,
        task,
        latencies,
        failures,
    }
}

async fn stop_api_sampler(sampler: ApiSampler) -> (Vec<f64>, u64) {
    sampler.cancel.cancel();
    let _ = sampler.task.await;
    let values = sampler
        .latencies
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    (values, sampler.failures.load(Ordering::Acquire))
}

async fn start_download_workload(
    source_path: &Path,
    output_path: &Path,
) -> Result<DownloadWorkload> {
    ensure!(
        source_path.is_file() && !source_path.is_symlink(),
        "download workload source model is unavailable or symbolic"
    );
    ensure!(
        std::fs::symlink_metadata(output_path)
            .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound),
        "download workload output already exists or cannot be inspected"
    );
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let address = listener.local_addr()?;
    let cancel = CancellationToken::new();
    let server_cancel = cancel.clone();
    let failed = Arc::new(AtomicBool::new(false));
    let server_failed = failed.clone();
    let source_path = source_path.to_path_buf();
    let server_task = tokio::spawn(async move {
        let result = async {
            let (mut socket, _) = listener.accept().await?;
            let mut source = tokio::fs::File::open(source_path).await?;
            let mut chunk = vec![0_u8; DOWNLOAD_CHUNK_BYTES];
            while !server_cancel.is_cancelled() {
                let count = source.read(&mut chunk).await?;
                if count == 0 {
                    source.seek(SeekFrom::Start(0)).await?;
                    continue;
                }
                socket.write_all(&chunk[..count]).await?;
                sleep(DOWNLOAD_CHUNK_INTERVAL).await;
            }
            Ok::<(), std::io::Error>(())
        }
        .await;
        if result.is_err() && !server_cancel.is_cancelled() {
            server_failed.store(true, Ordering::Release);
        }
    });
    let bytes = Arc::new(AtomicU64::new(0));
    let client_bytes = bytes.clone();
    let client_cancel = cancel.clone();
    let client_failed = failed.clone();
    let output_path = output_path.to_path_buf();
    let client_output_path = output_path.clone();
    let client_task = tokio::spawn(async move {
        let result = async {
            let mut socket = TcpStream::connect(address).await?;
            let mut output = tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(client_output_path)
                .await?;
            let mut buffer = vec![0_u8; DOWNLOAD_CHUNK_BYTES];
            while !client_cancel.is_cancelled() {
                let count = socket.read(&mut buffer).await?;
                if count == 0 {
                    if client_cancel.is_cancelled() {
                        break;
                    }
                    bail!("download workload closed unexpectedly");
                }
                output.write_all(&buffer[..count]).await?;
                client_bytes.fetch_add(count as u64, Ordering::AcqRel);
            }
            output.flush().await?;
            output.sync_data().await?;
            Ok::<(), anyhow::Error>(())
        }
        .await;
        if result.is_err() && !client_cancel.is_cancelled() {
            client_failed.store(true, Ordering::Release);
        }
    });
    Ok(DownloadWorkload {
        cancel,
        server_task,
        client_task,
        bytes,
        failed,
        output_path,
    })
}

async fn stop_download_workload(workload: DownloadWorkload) -> (u64, bool) {
    workload.cancel.cancel();
    let mut client_task = workload.client_task;
    let mut server_task = workload.server_task;
    if timeout(Duration::from_secs(3), &mut client_task)
        .await
        .is_err()
    {
        client_task.abort();
    }
    if timeout(Duration::from_secs(3), &mut server_task)
        .await
        .is_err()
    {
        server_task.abort();
    }
    let bytes = workload.bytes.load(Ordering::Acquire);
    let persisted = std::fs::metadata(&workload.output_path)
        .map(|metadata| metadata.is_file() && metadata.len() == bytes)
        .unwrap_or(false);
    (
        bytes,
        bytes > 0 && persisted && !workload.failed.load(Ordering::Acquire),
    )
}

async fn start_playback(
    command: &PlaybackCommandArtifact,
    output_dir: &Path,
    seconds: f64,
) -> Result<RunningPlayback> {
    ensure!(
        command.tool == "ffmpeg" && command.label == "hardware",
        "playback command is not the certified hardware command"
    );
    ensure!(
        command.source.is_file() && !command.source.is_symlink(),
        "playback source is unavailable or symbolic"
    );
    ensure!(
        !command.args.is_empty() && command.output_dir.is_absolute(),
        "playback command metadata is invalid"
    );
    std::fs::create_dir_all(output_dir)?;
    let args = sustained_playback_args(command, output_dir, seconds)?;
    let child = Command::new("ffmpeg")
        .args(&args)
        .current_dir(output_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .context("spawning playback coexistence ffmpeg")?;
    Ok(RunningPlayback {
        child,
        output_dir: output_dir.to_path_buf(),
        started: Instant::now(),
        seen_segments: HashSet::new(),
        segment_seen_at: Vec::new(),
        startup_ms: None,
    })
}

async fn finish_natural_playback(
    mut process: RunningPlayback,
    seconds: f64,
) -> Result<PlaybackMetrics> {
    process.wait_ready().await?;
    timeout(Duration::from_secs_f64(seconds + 60.0), async {
        while process.is_alive().await? {
            sleep(PLAYBACK_POLL).await;
        }
        Ok::<(), anyhow::Error>(())
    })
    .await
    .context("baseline playback exceeded its execution deadline")??;
    process.finish(false).await
}

fn sustained_playback_args(
    command: &PlaybackCommandArtifact,
    output_dir: &Path,
    seconds: f64,
) -> Result<Vec<String>> {
    ensure!(
        seconds.is_finite() && seconds >= 4.0,
        "playback duration is invalid"
    );
    let old = command.output_dir.to_string_lossy().replace('\\', "/");
    let new = output_dir.to_string_lossy().replace('\\', "/");
    let mut args = command
        .args
        .iter()
        .map(|arg| {
            let normalized = arg.replace('\\', "/");
            if normalized == old {
                new.clone()
            } else if let Some(suffix) = normalized.strip_prefix(&(old.clone() + "/")) {
                format!("{new}/{suffix}")
            } else {
                arg.clone()
            }
        })
        .collect::<Vec<_>>();
    let duration_positions = args
        .iter()
        .enumerate()
        .filter_map(|(index, value)| (value == "-t").then_some(index))
        .collect::<Vec<_>>();
    ensure!(
        duration_positions.len() == 1 && duration_positions[0] + 1 < args.len(),
        "certified playback command must contain exactly one -t duration"
    );
    args[duration_positions[0] + 1] = seconds.to_string();
    let input = args
        .iter()
        .position(|value| value == "-i")
        .ok_or_else(|| anyhow!("certified playback command lacks -i"))?;
    args.splice(
        input..input,
        [
            "-re".to_string(),
            "-stream_loop".to_string(),
            "-1".to_string(),
        ],
    );
    Ok(args)
}

fn collect_segment_paths(root: &Path, output: &mut Vec<PathBuf>) -> Result<()> {
    if !root.exists() {
        return Ok(());
    }
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            collect_segment_paths(&path, output)?;
        } else if metadata.is_file()
            && path
                .extension()
                .and_then(|value| value.to_str())
                .is_some_and(|extension| {
                    matches!(extension.to_ascii_lowercase().as_str(), "ts" | "m4s")
                })
        {
            output.push(path);
        }
    }
    Ok(())
}

async fn ready_worker_pid(engine: &LocalModelEngine) -> Result<u32> {
    let snapshot = engine.snapshot().await;
    ensure!(
        snapshot.state == LocalModelWorkerState::Ready,
        "candidate worker is not ready"
    );
    snapshot
        .process_id
        .ok_or_else(|| anyhow!("ready candidate worker has no process ID"))
}

async fn current_worker_pid(engine: &LocalModelEngine) -> Option<u32> {
    engine.snapshot().await.process_id
}

async fn wait_for_replacement_worker(
    engine: &LocalModelEngine,
    previous_process_id: u32,
) -> Result<u32> {
    let deadline = Instant::now() + WORKER_RECOVERY_TIMEOUT;
    loop {
        let snapshot = engine.snapshot().await;
        if snapshot.state == LocalModelWorkerState::Ready
            && let Some(process_id) = snapshot.process_id
            && process_id != previous_process_id
        {
            return Ok(process_id);
        }
        ensure!(
            Instant::now() < deadline,
            "packaged worker did not recover before the certification deadline"
        );
        sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_process_exit(process_id: u32) -> Result<bool> {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        // Every supported release platform can obtain CPU time for a live
        // same-user worker. Failure after the managed child has been awaited
        // is therefore the cross-platform physical exit/reap observation.
        if worker_cpu_time_ms(process_id).await.is_err() {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
        sleep(Duration::from_millis(25)).await;
    }
}

async fn ready_worker_rss(engine: &LocalModelEngine) -> Result<u64> {
    let snapshot = engine.snapshot().await;
    ensure!(
        snapshot.state == LocalModelWorkerState::Ready,
        "candidate worker became unavailable"
    );
    snapshot
        .resident_rss_bytes
        .filter(|value| *value > 0)
        .ok_or_else(|| anyhow!("candidate worker RSS is unavailable"))
}

fn exact_device_available_bytes(
    inventory: &crate::anime_matching::InferenceHardwareInventory,
) -> Option<u64> {
    let values = inventory
        .device_memory
        .iter()
        .filter(|device| !device.available_is_estimate)
        .filter_map(|device| device.available_bytes)
        .collect::<Vec<_>>();
    (!values.is_empty()).then(|| values.into_iter().sum())
}

async fn worker_cpu_time_ms(pid: u32) -> Result<f64> {
    #[cfg(target_os = "linux")]
    {
        let stat = tokio::fs::read_to_string(format!("/proc/{pid}/stat")).await?;
        let close = stat
            .rfind(')')
            .ok_or_else(|| anyhow!("invalid worker /proc stat"))?;
        let fields = stat[close + 2..].split_whitespace().collect::<Vec<_>>();
        ensure!(fields.len() > 12, "worker /proc stat is truncated");
        let user: f64 = fields[11].parse::<u64>()? as f64;
        let system: f64 = fields[12].parse::<u64>()? as f64;
        let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
        ensure!(ticks > 0, "cannot determine host clock ticks");
        return Ok((user + system) * 1_000.0 / ticks as f64);
    }
    #[cfg(target_os = "macos")]
    {
        let output = Command::new("ps")
            .args(["-o", "cputime=", "-p", &pid.to_string()])
            .output()
            .await?;
        ensure!(output.status.success(), "ps could not read worker CPU time");
        return parse_ps_cpu_time(std::str::from_utf8(&output.stdout)?.trim());
    }
    #[cfg(target_os = "windows")]
    {
        let script = format!(
            "(Get-Process -Id {pid} -ErrorAction Stop).TotalProcessorTime.TotalMilliseconds.ToString([Globalization.CultureInfo]::InvariantCulture)"
        );
        let output = Command::new("powershell.exe")
            .args(["-NoProfile", "-NonInteractive", "-Command", &script])
            .output()
            .await?;
        ensure!(
            output.status.success(),
            "PowerShell could not read worker CPU time"
        );
        return Ok(std::str::from_utf8(&output.stdout)?.trim().parse()?);
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    bail!("worker CPU-time measurement is unsupported on this platform")
}

#[cfg(any(target_os = "macos", test))]
fn parse_ps_cpu_time(value: &str) -> Result<f64> {
    let (days, clock) = value
        .rsplit_once('-')
        .map_or((0_u64, value), |(days, clock)| {
            (days.parse().unwrap_or(u64::MAX), clock)
        });
    ensure!(days != u64::MAX, "invalid ps CPU day count");
    let fields = clock.split(':').collect::<Vec<_>>();
    ensure!((2..=3).contains(&fields.len()), "invalid ps CPU clock");
    let seconds: f64 = fields[fields.len() - 1].parse()?;
    let minutes: f64 = fields[fields.len() - 2].parse()?;
    let hours: f64 = if fields.len() == 3 {
        fields[0].parse()?
    } else {
        0.0
    };
    Ok((((days as f64 * 24.0 + hours) * 60.0 + minutes) * 60.0 + seconds) * 1_000.0)
}

async fn system_swap_bytes() -> Result<u64> {
    #[cfg(target_os = "linux")]
    {
        let text = tokio::fs::read_to_string("/proc/meminfo").await?;
        let field = |name: &str| -> Result<u64> {
            let line = text
                .lines()
                .find(|line| line.starts_with(name))
                .ok_or_else(|| anyhow!("/proc/meminfo lacks {name}"))?;
            Ok(line
                .split_whitespace()
                .nth(1)
                .ok_or_else(|| anyhow!("invalid {name}"))?
                .parse::<u64>()?
                * 1024)
        };
        return Ok(field("SwapTotal:")?.saturating_sub(field("SwapFree:")?));
    }
    #[cfg(target_os = "macos")]
    {
        let output = Command::new("sysctl")
            .args(["-n", "vm.swapusage"])
            .output()
            .await?;
        ensure!(output.status.success(), "sysctl could not read swap usage");
        let text = std::str::from_utf8(&output.stdout)?;
        let fields = text.split_whitespace().collect::<Vec<_>>();
        let used_index = fields
            .iter()
            .position(|value| *value == "used")
            .ok_or_else(|| anyhow!("vm.swapusage lacks used field"))?;
        let used = fields
            .get(used_index + 1)
            .filter(|value| **value != "=")
            .or_else(|| fields.get(used_index + 2))
            .ok_or_else(|| anyhow!("vm.swapusage used field is truncated"))?;
        return parse_human_bytes(used);
    }
    #[cfg(target_os = "windows")]
    {
        let script = "((Get-CimInstance Win32_PageFileUsage | Measure-Object CurrentUsage -Sum).Sum * 1MB).ToString([Globalization.CultureInfo]::InvariantCulture)";
        let output = Command::new("powershell.exe")
            .args(["-NoProfile", "-NonInteractive", "-Command", script])
            .output()
            .await?;
        ensure!(
            output.status.success(),
            "PowerShell could not read page-file usage"
        );
        return Ok(std::str::from_utf8(&output.stdout)?.trim().parse()?);
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    bail!("swap measurement is unsupported on this platform")
}

#[cfg(any(target_os = "macos", test))]
fn parse_human_bytes(value: &str) -> Result<u64> {
    let split = value
        .find(|character: char| !character.is_ascii_digit() && character != '.')
        .ok_or_else(|| anyhow!("swap value lacks unit"))?;
    let amount: f64 = value[..split].parse()?;
    let multiplier = match value[split..].trim().to_ascii_uppercase().as_str() {
        "B" => 1.0,
        "K" | "KB" => 1024.0,
        "M" | "MB" => 1024.0 * 1024.0,
        "G" | "GB" => 1024.0 * 1024.0 * 1024.0,
        unit => bail!("unknown swap unit {unit}"),
    };
    ensure!(
        amount.is_finite() && amount >= 0.0,
        "swap amount is invalid"
    );
    Ok((amount * multiplier).round() as u64)
}

fn percentile(values: &[f64], quantile: f64) -> Result<f64> {
    ensure!(!values.is_empty(), "percentile sample is empty");
    let mut ordered = values.to_vec();
    ensure!(
        ordered
            .iter()
            .all(|value| value.is_finite() && *value > 0.0),
        "percentile contains invalid values"
    );
    ordered.sort_by(f64::total_cmp);
    let index = ((quantile * ordered.len() as f64).ceil() as usize)
        .saturating_sub(1)
        .min(ordered.len() - 1);
    Ok(ordered[index])
}

fn duration_ms(value: Duration) -> f64 {
    value.as_secs_f64() * 1_000.0
}

fn file_size(path: &Path, label: &str) -> Result<u64> {
    let metadata = path
        .symlink_metadata()
        .with_context(|| format!("reading {label}"))?;
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink() && metadata.len() > 0,
        "{label} is not a non-empty regular file"
    );
    Ok(metadata.len())
}

pub(crate) fn verify_artifact(
    path: &Path,
    expected_sha: &str,
    expected_size: u64,
    label: &str,
) -> Result<()> {
    ensure!(
        file_size(path, label)? == expected_size,
        "{label} size differs from manifest"
    );
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    let actual = format!("sha256:{:x}", hasher.finalize());
    ensure!(
        actual.eq_ignore_ascii_case(expected_sha),
        "{label} SHA-256 differs from manifest"
    );
    Ok(())
}

fn write_new_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("observation output has no parent"))?;
    std::fs::create_dir_all(parent)?;
    let bytes = serde_json::to_vec(value)?;
    let mut options = std::fs::OpenOptions::new();
    options.create_new(true).write(true);
    let mut file = options
        .open(path)
        .with_context(|| format!("creating {}", path.display()))?;
    use std::io::Write;
    let result = (|| -> Result<()> {
        file.write_all(&bytes)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        Ok(())
    })();
    drop(file);
    if result.is_err() {
        let _ = std::fs::remove_file(path);
    }
    result
}

pub(crate) fn read_strict_json<T: for<'de> Deserialize<'de>>(
    path: &Path,
    label: &str,
) -> Result<T> {
    let metadata = path
        .symlink_metadata()
        .with_context(|| format!("reading {label}"))?;
    ensure!(
        metadata.is_file()
            && !metadata.file_type().is_symlink()
            && (1..=MAX_JSON_BYTES).contains(&metadata.len()),
        "{label} file shape or size is invalid"
    );
    let bytes = std::fs::read(path)?;
    let value = serde_json::from_slice::<StrictJsonValue>(&bytes)
        .with_context(|| format!("decoding strict {label} JSON"))?
        .0;
    serde_json::from_value(value).with_context(|| format!("validating {label}"))
}

fn read_regular_bytes(path: &Path, label: &str) -> Result<Vec<u8>> {
    let metadata = path
        .symlink_metadata()
        .with_context(|| format!("reading {label}"))?;
    ensure!(
        metadata.is_file()
            && !metadata.file_type().is_symlink()
            && (1..=MAX_JSON_BYTES).contains(&metadata.len()),
        "{label} must be a non-empty regular non-symlink file within the size limit"
    );
    std::fs::read(path).with_context(|| format!("reading {label}"))
}

struct StrictJsonValue(JsonValue);
impl<'de> Deserialize<'de> for StrictJsonValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        deserializer.deserialize_any(StrictJsonVisitor)
    }
}
struct StrictJsonVisitor;
impl<'de> Visitor<'de> for StrictJsonVisitor {
    type Value = StrictJsonValue;
    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("strict JSON without duplicate keys")
    }
    fn visit_bool<E>(self, value: bool) -> std::result::Result<Self::Value, E> {
        Ok(StrictJsonValue(JsonValue::Bool(value)))
    }
    fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E> {
        Ok(StrictJsonValue(JsonValue::Number(value.into())))
    }
    fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E> {
        Ok(StrictJsonValue(JsonValue::Number(value.into())))
    }
    fn visit_f64<E: DeError>(self, value: f64) -> std::result::Result<Self::Value, E> {
        serde_json::Number::from_f64(value)
            .map(JsonValue::Number)
            .map(StrictJsonValue)
            .ok_or_else(|| E::custom("non-finite JSON number"))
    }
    fn visit_str<E: DeError>(self, value: &str) -> std::result::Result<Self::Value, E> {
        self.visit_string(value.to_string())
    }
    fn visit_string<E>(self, value: String) -> std::result::Result<Self::Value, E> {
        Ok(StrictJsonValue(JsonValue::String(value)))
    }
    fn visit_none<E>(self) -> std::result::Result<Self::Value, E> {
        Ok(StrictJsonValue(JsonValue::Null))
    }
    fn visit_unit<E>(self) -> std::result::Result<Self::Value, E> {
        Ok(StrictJsonValue(JsonValue::Null))
    }
    fn visit_seq<A: SeqAccess<'de>>(
        self,
        mut values: A,
    ) -> std::result::Result<Self::Value, A::Error> {
        let mut output = Vec::new();
        while let Some(StrictJsonValue(value)) = values.next_element()? {
            output.push(value);
        }
        Ok(StrictJsonValue(JsonValue::Array(output)))
    }
    fn visit_map<A: MapAccess<'de>>(
        self,
        mut values: A,
    ) -> std::result::Result<Self::Value, A::Error> {
        let mut output = serde_json::Map::new();
        while let Some((key, StrictJsonValue(value))) = values.next_entry::<String, _>()? {
            if output.insert(key.clone(), value).is_some() {
                return Err(A::Error::custom(format!("duplicate JSON key {key:?}")));
            }
        }
        Ok(StrictJsonValue(JsonValue::Object(output)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bundled_hardware_request_corpus_is_strict_and_production_valid() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("requests.json");
        std::fs::write(&path, HARDWARE_REQUEST_CORPUS_BYTES)?;
        let corpus = read_frozen_request_corpus(&path)?;
        assert_eq!(corpus.requests.len(), 2);

        let mut changed = HARDWARE_REQUEST_CORPUS_BYTES.to_vec();
        changed.push(b' ');
        std::fs::write(&path, changed)?;
        assert!(read_frozen_request_corpus(&path).is_err());
        Ok(())
    }

    #[test]
    fn playback_host_binding_rejects_a_different_gpu_or_os_version() {
        let retained = HostObservation {
            os_family: "linux".into(),
            os_version: Some("Example Linux 1".into()),
            arch: "x86_64".into(),
            gpu_vendor: Some("nvidia".into()),
            gpu_model: Some("Example GPU".into()),
            gpu_device_id: Some("10de:1234".into()),
            gpu_driver_version: Some("555.1".into()),
        };
        let os = HostOsReport {
            family: retained.os_family.clone(),
            arch: retained.arch.clone(),
            version: retained.os_version.clone(),
            raw: BTreeMap::new(),
        };
        let mut gpu = HostGpuReport {
            vendor: retained.gpu_vendor.clone(),
            model: retained.gpu_model.clone(),
            device_id: retained.gpu_device_id.clone(),
            driver_version: retained.gpu_driver_version.clone(),
            raw: BTreeMap::new(),
        };
        validate_current_host_binding(&retained, &os, &gpu).unwrap();
        gpu.driver_version = Some("556.0".into());
        assert!(validate_current_host_binding(&retained, &os, &gpu).is_err());
    }

    #[test]
    fn release_runtime_slots_allow_macos_cpu_without_faking_other_accelerators() {
        assert!(certification_allows_execution(
            AnimeRuntimeBackend::MetalCpu,
            AnimeExecutionBackend::Metal
        ));
        assert!(certification_allows_execution(
            AnimeRuntimeBackend::MetalCpu,
            AnimeExecutionBackend::Cpu
        ));
        assert!(certification_allows_execution(
            AnimeRuntimeBackend::CudaCpu,
            AnimeExecutionBackend::Cuda
        ));
        assert!(!certification_allows_execution(
            AnimeRuntimeBackend::CudaCpu,
            AnimeExecutionBackend::Cpu
        ));
        assert!(certification_allows_execution(
            AnimeRuntimeBackend::HipCpu,
            AnimeExecutionBackend::Hip
        ));
        assert!(certification_allows_execution(
            AnimeRuntimeBackend::VulkanCpu,
            AnimeExecutionBackend::Vulkan
        ));
        assert!(certification_allows_execution(
            AnimeRuntimeBackend::Cpu,
            AnimeExecutionBackend::Cpu
        ));
    }

    #[test]
    fn sustained_command_rewrites_only_certified_output_and_duration() -> Result<()> {
        let artifact = PlaybackCommandArtifact {
            tool: "ffmpeg".into(),
            label: "hardware".into(),
            args: vec![
                "-hide_banner".into(),
                "-i".into(),
                "/source.mp4".into(),
                "-t".into(),
                "6".into(),
                "-hls_segment_filename".into(),
                "/old/hls/segment_%05d.ts".into(),
                "/old/hls/stream_0.m3u8".into(),
            ],
            source: PathBuf::from("/source.mp4"),
            output_dir: PathBuf::from("/old/hls"),
        };
        let args = sustained_playback_args(&artifact, Path::new("/new/hls"), 120.0)?;
        assert_eq!(&args[1..4], ["-re", "-stream_loop", "-1"]);
        assert!(args.windows(2).any(|pair| pair == ["-t", "120"]));
        assert!(args.contains(&"/new/hls/segment_%05d.ts".to_string()));
        assert!(!args.iter().any(|value| value.contains("/old/hls")));
        Ok(())
    }

    #[tokio::test]
    async fn download_workload_streams_real_source_bytes_to_disk() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let source = temporary.path().join("model.gguf");
        let output = temporary.path().join("download.partial");
        std::fs::write(&source, vec![0xa5_u8; DOWNLOAD_CHUNK_BYTES * 2])?;
        let workload = start_download_workload(&source, &output).await?;
        sleep(Duration::from_millis(150)).await;
        let (bytes, passed) = stop_download_workload(workload).await;
        assert!(passed);
        assert!(bytes >= DOWNLOAD_CHUNK_BYTES as u64);
        assert_eq!(std::fs::metadata(output)?.len(), bytes);
        Ok(())
    }

    #[test]
    fn parses_cross_platform_resource_formats() -> Result<()> {
        assert_eq!(parse_ps_cpu_time("01:02")?, 62_000.0);
        assert_eq!(parse_ps_cpu_time("1-02:03:04.5")?, 93_784_500.0);
        assert_eq!(parse_human_bytes("1.50G")?, 1_610_612_736);
        Ok(())
    }

    #[test]
    fn strict_json_rejects_duplicate_keys() {
        assert!(serde_json::from_slice::<StrictJsonValue>(br#"{"a":1,"a":2}"#).is_err());
    }
}
