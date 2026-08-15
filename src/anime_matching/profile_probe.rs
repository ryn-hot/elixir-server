//! Release-only generation of a sealed local-inference runtime profile.
//!
//! The installed product performs the same automatic probe while activating a
//! bundle. This entry point exists solely so release hardware runners can bind
//! their coexistence evidence to the exact candidate manifest, model, packaged
//! worker, runtime policy, and current hardware. It has no API route or user
//! configuration surface.

use std::{
    collections::BTreeSet,
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
};

#[cfg(unix)]
use std::fs::File;

use anyhow::{Context, Result, anyhow, bail, ensure};
use async_trait::async_trait;
use semver::Version;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::playback::hardware::{HostHardwareInventory, collect_host_hardware_inventory};

use super::{
    ANIME_MATCH_PROMPT_REVISION, ANIME_SEMANTIC_EVIDENCE_V4_PROMPT_REVISION,
    AnimeArtifactUrlPolicy, AnimeBundleCompatibilityPolicy, AnimeBundleQualificationGate,
    AnimeExecutionBackend, AnimeInferenceBundleManifest, AnimeKvCacheType, AnimeRuntimeBackend,
    AnimeRuntimeSelection, AnimeSemanticEvidenceRequest, InferenceBackend, InferenceEnvelopeProbe,
    InferenceModelEnvelope, InferenceProbeError, InferenceProbeLimits, InferenceProbeMeasurement,
    InferenceRuntimeCandidate, InferenceRuntimeProfile, LocalModelEngine, LocalModelRuntimeProfile,
    LocalModelSamplingProfile, ResolvedAnimeRuntime, RuntimeProfileIdentity, RuntimeProfilePolicy,
    ValidatedAnimeBundle, assess_inference_memory_pressure, bundle_inference_host,
    bundle_runtime_profile_from_probe, collect_inference_hardware_inventory,
    extract_anime_runtime_for_qualification, inference_hardware_fingerprint, resolve_anime_runtime,
    runtime_device_memory, runtime_profile_candidates, select_runtime_profile,
    validate_anime_bundle,
};

use super::{
    certification::{read_strict_json, runtime_id, verify_artifact},
    prime::smoke_requests,
};

#[derive(Debug, Clone)]
pub struct AnimeInferenceProfileProbeConfig {
    pub runtime_id: String,
    pub manifest_path: PathBuf,
    pub model_path: PathBuf,
    pub runtime_artifact_path: PathBuf,
    pub output_path: PathBuf,
    /// Optional release-only semantic fixtures. When present, the hardware
    /// envelope is proven through the selector contract instead of the retired
    /// direct-plan contract.
    pub semantic_probe_corpus_path: Option<PathBuf>,
    pub semantic_prompt_revision: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SemanticProbeCorpus {
    cases: Vec<SemanticProbeCase>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SemanticProbeCase {
    request: AnimeSemanticEvidenceRequest,
}

/// Generates one execution-capable, sealed profile from the production envelope
/// selector and production llama.cpp worker path. A deterministic-only result
/// is an error and no output is created.
pub async fn run_anime_inference_profile_probe(
    config: AnimeInferenceProfileProbeConfig,
) -> Result<()> {
    validate_config(&config)?;
    ensure_output_absent(&config.output_path)?;

    let manifest: AnimeInferenceBundleManifest =
        read_strict_json(&config.manifest_path, "candidate manifest")?;
    let server_version = Version::parse(env!("CARGO_PKG_VERSION"))
        .context("server package version is not semantic versioning")?;
    let policy = AnimeBundleCompatibilityPolicy {
        server_version,
        qualification_gate: AnimeBundleQualificationGate::DevelopmentAllowUnqualified,
        artifact_url_policy: AnimeArtifactUrlPolicy::HttpsOnly,
        require_complete_platform_matrix: true,
    };
    let bundle = validate_anime_bundle(manifest, &policy)
        .context("validating strict release candidate manifest")?;
    let runtime = exact_manifest_runtime(&bundle, &config.runtime_id)?.clone();

    verify_artifact(
        &config.model_path,
        &bundle.manifest().model.sha256,
        bundle.manifest().model.size_bytes,
        "candidate model",
    )?;
    verify_artifact(
        &config.runtime_artifact_path,
        &runtime.sha256,
        runtime.size_bytes,
        "candidate runtime artifact",
    )?;
    let model_path = absolute_existing_path(&config.model_path, "candidate model")?;
    let runtime_artifact_path =
        absolute_existing_path(&config.runtime_artifact_path, "candidate runtime artifact")?;
    let semantic_probe_requests = config
        .semantic_probe_corpus_path
        .as_deref()
        .map(read_semantic_probe_requests)
        .transpose()?;
    let prompt_revision = config
        .semantic_prompt_revision
        .as_deref()
        .unwrap_or(ANIME_MATCH_PROMPT_REVISION)
        .to_string();

    let host = collect_host_hardware_inventory().await;
    let inventory = collect_inference_hardware_inventory(host.clone()).await;
    let host_contract = bundle_inference_host(&host, &inventory)
        .context("converting current hardware to the anime runtime contract")?;
    let resolved = resolve_anime_runtime(&bundle, &host_contract)
        .context("resolving candidate runtimes on current hardware")?;
    let exact_selection = exact_runtime_selection(&resolved, &runtime)?;
    let probe_policy = runtime_probe_policy(&bundle, &exact_selection);
    let candidates = runtime_profile_candidates(&inventory, &probe_policy);
    ensure!(
        !candidates.is_empty(),
        "current hardware has no viable profile candidates for runtime '{}'",
        config.runtime_id
    );

    let extraction = tempfile::Builder::new()
        .prefix("elixir-alm9-profile-runtime-")
        .tempdir()
        .context("creating verified runtime extraction directory")?;
    let runtime_root = extraction.path().join("runtime");
    let worker_path =
        extract_anime_runtime_for_qualification(&runtime_artifact_path, &runtime_root, &runtime)
            .await
            .context("extracting exact candidate runtime")?;

    let probe = ReleaseEnvelopeProbe {
        bundle: &bundle,
        selection: &exact_selection,
        host: &host,
        inventory: &inventory,
        worker_path,
        model_path: model_path.clone(),
        semantic_probe_requests,
        prompt_revision,
        active_engine: Mutex::new(None),
    };
    let identity = RuntimeProfileIdentity {
        bundle_version: bundle.manifest().bundle_version.clone(),
        model_revision: bundle.manifest().model.revision.clone(),
        worker_revision: bundle.manifest().worker_revision.clone(),
        runtime_policy_revision: bundle
            .manifest()
            .runtime_policy
            .sampling_profile_revision
            .clone(),
        kv_cache_type: bundle.manifest().runtime_policy.kv_cache_type,
    };
    let selected = select_runtime_profile(
        &inventory,
        &identity,
        &candidates,
        &InferenceProbeLimits::default(),
        &probe,
    )
    .await;
    // The outer selector deadline can cancel a probe future. Always stop and
    // reap its disposable worker before inspecting or writing the result.
    probe.shutdown_active_engine().await;
    let Some(probe_profile) = selected.profile else {
        bail!(
            "no model-capable profile passed for runtime '{}': {}",
            config.runtime_id,
            probe_attempt_summary(&selected.attempts)
        );
    };
    let selected_runtime = resolved_runtime_for_profile(&exact_selection, &probe_profile)?;
    let profile = bundle_runtime_profile_from_probe(&bundle, selected_runtime, &probe_profile)
        .context("sealing selected candidate runtime profile")?;
    profile
        .validate()
        .context("validating sealed runtime profile")?;

    // Re-read immutable artifact identity and stable hardware identity after
    // the real probes. A replacement or driver/device transition invalidates
    // this run instead of producing evidence for mixed inputs.
    let final_manifest: AnimeInferenceBundleManifest =
        read_strict_json(&config.manifest_path, "candidate manifest after probe")?;
    let final_bundle = validate_anime_bundle(final_manifest, &policy)
        .context("revalidating strict candidate manifest after probe")?;
    ensure!(
        final_bundle
            .manifest_fingerprint()
            .eq_ignore_ascii_case(bundle.manifest_fingerprint()),
        "candidate manifest changed during profile probing"
    );
    verify_artifact(
        &model_path,
        &bundle.manifest().model.sha256,
        bundle.manifest().model.size_bytes,
        "candidate model after probe",
    )?;
    verify_artifact(
        &runtime_artifact_path,
        &runtime.sha256,
        runtime.size_bytes,
        "candidate runtime artifact after probe",
    )?;
    let final_host = collect_host_hardware_inventory().await;
    let final_inventory = collect_inference_hardware_inventory(final_host).await;
    ensure!(
        inference_hardware_fingerprint(&inventory)
            .eq_ignore_ascii_case(&inference_hardware_fingerprint(&final_inventory)),
        "current hardware or driver identity changed during profile probing"
    );
    ensure!(
        profile
            .host_fingerprint
            .eq_ignore_ascii_case(&inference_hardware_fingerprint(&final_inventory)),
        "sealed profile is not bound to current hardware"
    );
    ensure!(
        profile.runtime_artifact_key == runtime.artifact_key(),
        "sealed profile resolved a different runtime artifact"
    );

    write_profile_new(&config.output_path, &profile)?;
    drop(extraction);
    Ok(())
}

fn validate_config(config: &AnimeInferenceProfileProbeConfig) -> Result<()> {
    ensure!(
        !config.runtime_id.is_empty()
            && config.runtime_id.len() <= 128
            && config.runtime_id.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
            }),
        "runtime ID is invalid"
    );
    ensure!(
        config.manifest_path != config.output_path
            && config.model_path != config.output_path
            && config.runtime_artifact_path != config.output_path,
        "output path aliases an input path"
    );
    ensure!(
        config.semantic_probe_corpus_path.is_some() == config.semantic_prompt_revision.is_some(),
        "semantic probe corpus and prompt revision must be supplied together"
    );
    if let Some(revision) = config.semantic_prompt_revision.as_deref() {
        ensure!(
            revision == ANIME_MATCH_PROMPT_REVISION
                || revision == ANIME_SEMANTIC_EVIDENCE_V4_PROMPT_REVISION,
            "semantic prompt revision is unsupported"
        );
        ensure!(
            config
                .semantic_probe_corpus_path
                .as_ref()
                .is_some_and(|path| path != &config.output_path),
            "output path aliases the semantic probe corpus"
        );
    }
    Ok(())
}

fn read_semantic_probe_requests(path: &Path) -> Result<[AnimeSemanticEvidenceRequest; 2]> {
    let corpus: SemanticProbeCorpus = read_strict_json(path, "semantic probe corpus")?;
    let mut requests = corpus.cases.into_iter().map(|case| case.request);
    let priming_request = requests
        .next()
        .context("semantic probe corpus requires two cases")?;
    let request = requests
        .next()
        .context("semantic probe corpus requires two cases")?;
    ensure!(
        priming_request.request_id != request.request_id,
        "semantic probe requests must be distinct"
    );
    Ok([priming_request, request])
}

fn ensure_output_absent(path: &Path) -> Result<()> {
    match path.symlink_metadata() {
        Ok(_) => bail!("runtime profile output already exists: {}", path.display()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("checking output {}", path.display())),
    }
}

fn exact_manifest_runtime<'a>(
    bundle: &'a ValidatedAnimeBundle,
    requested_runtime_id: &str,
) -> Result<&'a super::AnimeRuntimeArtifactManifest> {
    let matches = bundle
        .manifest()
        .runtimes
        .iter()
        .filter(|candidate| runtime_id(candidate) == requested_runtime_id)
        .collect::<Vec<_>>();
    ensure!(
        matches.len() == 1,
        "runtime ID '{}' does not resolve exactly one candidate manifest runtime",
        requested_runtime_id
    );
    Ok(matches[0])
}

fn exact_runtime_selection(
    resolved: &AnimeRuntimeSelection,
    runtime: &super::AnimeRuntimeArtifactManifest,
) -> Result<AnimeRuntimeSelection> {
    let key = runtime.artifact_key();
    let candidates = resolved
        .candidates
        .iter()
        .filter(|candidate| {
            candidate.artifact.artifact_key() == key
                && certification_allows_execution(runtime.backend, candidate.execution_backend)
        })
        .cloned()
        .collect::<Vec<_>>();
    ensure!(
        !candidates.is_empty(),
        "requested runtime has no certifiable execution backend on current hardware"
    );
    Ok(AnimeRuntimeSelection { candidates })
}

fn certification_allows_execution(
    runtime: AnimeRuntimeBackend,
    execution: AnimeExecutionBackend,
) -> bool {
    match runtime {
        // The macOS artifacts are intentionally combined and there is no
        // separate CPU runtime slot. Keep the automatic Metal-first, CPU-next
        // envelope promised by the production resolver and record whichever
        // backend actually passed in the sealed profile.
        AnimeRuntimeBackend::MetalCpu => {
            matches!(
                execution,
                AnimeExecutionBackend::Metal | AnimeExecutionBackend::Cpu
            )
        }
        // These platforms have explicit CPU runtime slots in the release
        // matrix. An accelerator slot must therefore prove its named backend
        // instead of silently duplicating the CPU evidence.
        AnimeRuntimeBackend::CudaCpu => execution == AnimeExecutionBackend::Cuda,
        AnimeRuntimeBackend::HipCpu => execution == AnimeExecutionBackend::Hip,
        AnimeRuntimeBackend::VulkanCpu => execution == AnimeExecutionBackend::Vulkan,
        AnimeRuntimeBackend::Cpu => execution == AnimeExecutionBackend::Cpu,
    }
}

fn runtime_probe_policy(
    bundle: &ValidatedAnimeBundle,
    selection: &AnimeRuntimeSelection,
) -> RuntimeProfilePolicy {
    RuntimeProfilePolicy {
        certified_backends: selection
            .candidates
            .iter()
            .map(|runtime| inference_backend(runtime.execution_backend))
            .collect::<BTreeSet<_>>(),
        model: InferenceModelEnvelope {
            model_size_bytes: bundle.manifest().model.size_bytes,
            transformer_layers: bundle.manifest().model.transformer_layers,
        },
    }
}

struct ReleaseEnvelopeProbe<'a> {
    bundle: &'a ValidatedAnimeBundle,
    selection: &'a AnimeRuntimeSelection,
    host: &'a HostHardwareInventory,
    inventory: &'a super::InferenceHardwareInventory,
    worker_path: PathBuf,
    model_path: PathBuf,
    semantic_probe_requests: Option<[AnimeSemanticEvidenceRequest; 2]>,
    prompt_revision: String,
    active_engine: Mutex<Option<LocalModelEngine>>,
}

struct ReleaseProbeMeasurement {
    worker_ready: bool,
    smoke_match_passed: bool,
    load_time_ms: u64,
    warm_latency_ms: u64,
    peak_rss_bytes: Option<u64>,
}

#[async_trait]
impl InferenceEnvelopeProbe for ReleaseEnvelopeProbe<'_> {
    async fn probe(
        &self,
        candidate: &InferenceRuntimeCandidate,
    ) -> std::result::Result<InferenceProbeMeasurement, InferenceProbeError> {
        self.probe_inner(candidate)
            .await
            .map_err(|error| InferenceProbeError::Runner(bounded_error(&error)))
    }
}

impl ReleaseEnvelopeProbe<'_> {
    async fn shutdown_active_engine(&self) {
        if let Some(engine) = self.active_engine.lock().await.take() {
            engine.shutdown().await;
        }
    }

    async fn probe_inner(
        &self,
        candidate: &InferenceRuntimeCandidate,
    ) -> Result<InferenceProbeMeasurement> {
        self.shutdown_active_engine().await;
        let _runtime = resolved_runtime_for_candidate(self.selection, candidate)?;
        let profile = local_profile_for_probe(
            self.bundle,
            &self.worker_path,
            &self.model_path,
            self.inventory,
            candidate,
            &self.prompt_revision,
        )?;
        let engine = LocalModelEngine::allow_all_for_probe()?;
        engine.activate_profile_for_probe(profile).await?;
        *self.active_engine.lock().await = Some(engine.clone());

        let measured_result =
            if let Some([priming_request, request]) = self.semantic_probe_requests.as_ref() {
                engine
                    .probe_semantic(priming_request.clone(), request.clone())
                    .await
                    .map(|measured| ReleaseProbeMeasurement {
                        worker_ready: measured.worker_ready,
                        smoke_match_passed: measured.smoke_match_passed,
                        load_time_ms: measured.load_time_ms,
                        warm_latency_ms: measured.warm_latency_ms,
                        peak_rss_bytes: measured.peak_rss_bytes,
                    })
            } else {
                let [priming_request, request] = smoke_requests()?;
                engine
                    .probe(priming_request, request)
                    .await
                    .map(|measured| ReleaseProbeMeasurement {
                        worker_ready: measured.worker_ready,
                        smoke_match_passed: measured.smoke_match_passed,
                        load_time_ms: measured.load_time_ms,
                        warm_latency_ms: measured.warm_latency_ms,
                        peak_rss_bytes: measured.peak_rss_bytes,
                    })
            };
        let measured = match measured_result {
            Ok(measured) => measured,
            Err(error) => {
                engine.shutdown().await;
                self.active_engine.lock().await.take();
                return Err(error);
            }
        };
        let post_inventory = collect_inference_hardware_inventory(self.host.clone()).await;
        engine.shutdown().await;
        self.active_engine.lock().await.take();

        let memory_pressure = assess_inference_memory_pressure(&post_inventory.memory, None);
        let before_device = runtime_device_memory(
            self.inventory,
            candidate.backend,
            candidate.device_key.as_deref(),
        )
        .and_then(|device| {
            device
                .available_bytes
                .map(|bytes| (bytes, device.available_is_estimate))
        });
        let after_device = runtime_device_memory(
            &post_inventory,
            candidate.backend,
            candidate.device_key.as_deref(),
        )
        .and_then(|device| {
            device
                .available_bytes
                .map(|bytes| (bytes, device.available_is_estimate))
        });
        Ok(InferenceProbeMeasurement {
            worker_ready: measured.worker_ready,
            smoke_match_passed: measured.smoke_match_passed,
            load_time_ms: measured.load_time_ms,
            warm_latency_ms: measured.warm_latency_ms,
            peak_rss_bytes: measured.peak_rss_bytes.unwrap_or(0),
            peak_device_memory_bytes: before_device.zip(after_device).and_then(
                |((before, before_estimated), (after, after_estimated))| {
                    (!before_estimated && !after_estimated).then(|| before.saturating_sub(after))
                },
            ),
            system_available_bytes: post_inventory.memory.available_bytes,
            device_available_bytes: after_device.map(|(bytes, _)| bytes),
            memory_pressure: memory_pressure.under_pressure,
        })
    }
}

fn local_profile_for_probe(
    bundle: &ValidatedAnimeBundle,
    worker_path: &Path,
    model_path: &Path,
    inventory: &super::InferenceHardwareInventory,
    candidate: &InferenceRuntimeCandidate,
    prompt_revision: &str,
) -> Result<LocalModelRuntimeProfile> {
    let manifest = bundle.manifest();
    let sampling = LocalModelSamplingProfile::default();
    ensure!(
        sampling.revision == manifest.runtime_policy.sampling_profile_revision,
        "candidate sampling profile is unsupported by this server"
    );
    let fingerprint_payload = json!({
        "manifestFingerprint": bundle.manifest_fingerprint(),
        "bundleVersion": manifest.bundle_version,
        "modelId": manifest.model.id,
        "modelRevision": manifest.model.revision,
        "modelSha256": manifest.model.sha256,
        "workerRevision": manifest.worker_revision,
        "runtimePolicy": manifest.runtime_policy,
        "hardwareFingerprint": inference_hardware_fingerprint(inventory),
        "backend": candidate.backend,
        "deviceKey": candidate.device_key,
        "gpuLayers": candidate.gpu_layers,
        "cpuThreads": candidate.cpu_threads,
        "batchThreads": candidate.batch_threads,
        "promptRevision": prompt_revision,
    });
    let profile_fingerprint = format!(
        "sha256:{:x}",
        Sha256::digest(serde_json::to_vec(&fingerprint_payload)?)
    );
    Ok(LocalModelRuntimeProfile {
        bundle_version: manifest.bundle_version.clone(),
        model_id: manifest.model.id.clone(),
        model_revision: manifest.model.revision.clone(),
        worker_revision: manifest.worker_revision.clone(),
        backend: candidate.backend.as_str().to_string(),
        profile_fingerprint,
        protocol_version: manifest.protocol_version,
        matcher_schema_version: manifest.matcher_schema_version,
        prompt_revision: prompt_revision.to_string(),
        worker_path: worker_path.to_path_buf(),
        model_path: model_path.to_path_buf(),
        context_tokens: manifest.model.context_tokens,
        max_output_tokens: manifest.model.max_output_tokens,
        threads: candidate.cpu_threads,
        batch_threads: candidate.batch_threads,
        gpu_layers: candidate.gpu_layers,
        kv_cache_type: kv_cache_name(manifest.runtime_policy.kv_cache_type).to_string(),
        // This is only the admission estimate before the real probe replaces
        // it with observed peak RSS in the sealed profile.
        peak_rss_bytes: manifest.model.size_bytes,
        idle_unload_seconds: manifest.runtime_policy.idle_unload_seconds,
        sampling,
    })
}

fn resolved_runtime_for_candidate<'a>(
    selection: &'a AnimeRuntimeSelection,
    candidate: &InferenceRuntimeCandidate,
) -> Result<&'a ResolvedAnimeRuntime> {
    selection
        .candidates
        .iter()
        .find(|runtime| {
            runtime.execution_backend == anime_execution_backend(candidate.backend)
                && runtime.device_id == candidate.device_key
        })
        .ok_or_else(|| anyhow!("hardware probe candidate has no exact manifest runtime"))
}

fn resolved_runtime_for_profile<'a>(
    selection: &'a AnimeRuntimeSelection,
    profile: &InferenceRuntimeProfile,
) -> Result<&'a ResolvedAnimeRuntime> {
    selection
        .candidates
        .iter()
        .find(|runtime| {
            runtime.execution_backend == anime_execution_backend(profile.backend)
                && runtime.device_id == profile.device_key
        })
        .ok_or_else(|| anyhow!("selected profile has no exact manifest runtime"))
}

fn anime_execution_backend(backend: InferenceBackend) -> AnimeExecutionBackend {
    match backend {
        InferenceBackend::Metal => AnimeExecutionBackend::Metal,
        InferenceBackend::Cuda => AnimeExecutionBackend::Cuda,
        InferenceBackend::Hip => AnimeExecutionBackend::Hip,
        InferenceBackend::Vulkan => AnimeExecutionBackend::Vulkan,
        InferenceBackend::Cpu => AnimeExecutionBackend::Cpu,
    }
}

fn inference_backend(backend: AnimeExecutionBackend) -> InferenceBackend {
    match backend {
        AnimeExecutionBackend::Metal => InferenceBackend::Metal,
        AnimeExecutionBackend::Cuda => InferenceBackend::Cuda,
        AnimeExecutionBackend::Hip => InferenceBackend::Hip,
        AnimeExecutionBackend::Vulkan => InferenceBackend::Vulkan,
        AnimeExecutionBackend::Cpu => InferenceBackend::Cpu,
    }
}

fn kv_cache_name(cache: AnimeKvCacheType) -> &'static str {
    match cache {
        AnimeKvCacheType::F16 => "f16",
        AnimeKvCacheType::Q8_0 => "q8_0",
    }
}

fn probe_attempt_summary(attempts: &[super::InferenceProbeAttempt]) -> String {
    if attempts.is_empty() {
        return "no candidates were attempted".to_string();
    }
    attempts
        .iter()
        .map(|attempt| {
            let detail = attempt.detail.as_deref().or_else(|| {
                attempt.rejection.map(|reason| match reason {
                    super::InferenceProbeRejection::InvalidCandidate => "invalid_candidate",
                    super::InferenceProbeRejection::WorkerNotReady => "worker_not_ready",
                    super::InferenceProbeRejection::SmokeMatchFailed => "smoke_match_failed",
                    super::InferenceProbeRejection::LoadDeadlineExceeded => {
                        "load_deadline_exceeded"
                    }
                    super::InferenceProbeRejection::WarmDeadlineExceeded => {
                        "warm_deadline_exceeded"
                    }
                    super::InferenceProbeRejection::WorkerMemoryUnavailable => {
                        "worker_memory_unavailable"
                    }
                    super::InferenceProbeRejection::WorkerMemoryExceeded => {
                        "worker_memory_exceeded"
                    }
                    super::InferenceProbeRejection::SystemMemoryReserve => "system_memory_reserve",
                    super::InferenceProbeRejection::DeviceMemoryReserve => "device_memory_reserve",
                    super::InferenceProbeRejection::MemoryPressure => "memory_pressure",
                })
            });
            format!(
                "{}:{:?}{}",
                attempt.candidate.backend.as_str(),
                attempt.status,
                detail.map(|value| format!("({value})")).unwrap_or_default()
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn absolute_existing_path(path: &Path, label: &str) -> Result<PathBuf> {
    path.canonicalize()
        .with_context(|| format!("resolving absolute {label} path {}", path.display()))
}

fn write_profile_new(path: &Path, profile: &impl Serialize) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)
        .with_context(|| format!("creating output directory {}", parent.display()))?;
    ensure_output_absent(path)?;
    let bytes = serde_json::to_vec(profile).context("encoding sealed runtime profile")?;
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    let mut file = options
        .open(path)
        .with_context(|| format!("creating runtime profile {}", path.display()))?;
    let result = (|| -> Result<()> {
        file.write_all(&bytes)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        #[cfg(unix)]
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if let Err(error) = result {
        drop(file);
        let _ = std::fs::remove_file(path);
        return Err(error).with_context(|| format!("writing runtime profile {}", path.display()));
    }
    Ok(())
}

fn bounded_error(error: &anyhow::Error) -> String {
    const MAX_CHARS: usize = 512;
    let detail = format!("{error:#}");
    let mut characters = detail.chars();
    let bounded = characters.by_ref().take(MAX_CHARS).collect::<String>();
    if characters.next().is_some() {
        format!("{bounded}…")
    } else {
        bounded
    }
}

#[cfg(test)]
mod tests {
    use serde::ser::Error as _;

    use super::*;

    fn runtime(backend: AnimeRuntimeBackend) -> super::super::AnimeRuntimeArtifactManifest {
        use super::super::{
            AnimeDeviceClass, AnimeHostArch, AnimeHostOs, AnimeRuntimeArchiveFormat,
        };

        let (os, device_class) = match backend {
            AnimeRuntimeBackend::MetalCpu => (AnimeHostOs::Macos, Some(AnimeDeviceClass::Apple)),
            AnimeRuntimeBackend::CudaCpu => (AnimeHostOs::Linux, Some(AnimeDeviceClass::Nvidia)),
            AnimeRuntimeBackend::HipCpu => (AnimeHostOs::Linux, Some(AnimeDeviceClass::Amd)),
            AnimeRuntimeBackend::VulkanCpu => {
                (AnimeHostOs::Linux, Some(AnimeDeviceClass::AnyVulkan))
            }
            AnimeRuntimeBackend::Cpu => (AnimeHostOs::Linux, Some(AnimeDeviceClass::Cpu)),
        };
        super::super::AnimeRuntimeArtifactManifest {
            os,
            arch: AnimeHostArch::X86_64,
            device_class,
            backend,
            priority: 1,
            revision: "b9637-test".to_string(),
            minimum_os_version: "1".to_string(),
            required_cpu_features: Vec::new(),
            minimum_driver_version: None,
            minimum_device_memory_bytes: 0,
            archive_format: AnimeRuntimeArchiveFormat::Raw,
            entrypoint: "llama-server".to_string(),
            packaged_dependencies: Vec::new(),
            url: "https://downloads.elixir.test/llama-server".to_string(),
            sha256: format!("sha256:{}", "a".repeat(64)),
            size_bytes: 1,
            installed_size_bytes: 1,
        }
    }

    #[test]
    fn output_is_create_new_and_preserves_existing_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("profile.json");
        write_profile_new(&output, &json!({"profile": 1})).unwrap();
        let original = std::fs::read(&output).unwrap();
        assert!(write_profile_new(&output, &json!({"profile": 2})).is_err());
        assert_eq!(std::fs::read(&output).unwrap(), original);
    }

    #[test]
    fn serialization_failure_leaves_no_output() {
        struct Broken;
        impl Serialize for Broken {
            fn serialize<S>(&self, _serializer: S) -> std::result::Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                Err(S::Error::custom("intentional serialization failure"))
            }
        }

        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("profile.json");
        assert!(write_profile_new(&output, &Broken).is_err());
        assert!(!output.exists());
    }

    #[test]
    fn runtime_id_input_is_strict() {
        let base = AnimeInferenceProfileProbeConfig {
            runtime_id: "linux-x86_64-cuda".to_string(),
            manifest_path: "manifest.json".into(),
            model_path: "model.gguf".into(),
            runtime_artifact_path: "runtime.tar.gz".into(),
            output_path: "profile.json".into(),
            semantic_probe_corpus_path: None,
            semantic_prompt_revision: None,
        };
        validate_config(&base).unwrap();
        let mut invalid = base.clone();
        invalid.runtime_id = "linux x86_64 CUDA".to_string();
        assert!(validate_config(&invalid).is_err());

        let mut unpaired = base.clone();
        unpaired.semantic_probe_corpus_path = Some("semantic.json".into());
        assert!(validate_config(&unpaired).is_err());

        let mut semantic = base;
        semantic.semantic_probe_corpus_path = Some("semantic.json".into());
        semantic.semantic_prompt_revision = Some(ANIME_MATCH_PROMPT_REVISION.to_string());
        validate_config(&semantic).unwrap();
    }

    #[test]
    fn runtime_ids_match_release_and_qualification_slots() {
        use super::super::{AnimeHostArch, AnimeHostOs};

        let cases = [
            (
                AnimeHostOs::Macos,
                AnimeHostArch::Aarch64,
                AnimeRuntimeBackend::MetalCpu,
                "macos-aarch64-metal-cpu",
            ),
            (
                AnimeHostOs::Macos,
                AnimeHostArch::X86_64,
                AnimeRuntimeBackend::MetalCpu,
                "macos-x86_64-metal-cpu",
            ),
            (
                AnimeHostOs::Windows,
                AnimeHostArch::X86_64,
                AnimeRuntimeBackend::Cpu,
                "windows-x86_64-cpu",
            ),
            (
                AnimeHostOs::Windows,
                AnimeHostArch::X86_64,
                AnimeRuntimeBackend::CudaCpu,
                "windows-x86_64-cuda",
            ),
            (
                AnimeHostOs::Windows,
                AnimeHostArch::X86_64,
                AnimeRuntimeBackend::VulkanCpu,
                "windows-x86_64-vulkan",
            ),
            (
                AnimeHostOs::Linux,
                AnimeHostArch::X86_64,
                AnimeRuntimeBackend::Cpu,
                "linux-x86_64-cpu",
            ),
            (
                AnimeHostOs::Linux,
                AnimeHostArch::X86_64,
                AnimeRuntimeBackend::CudaCpu,
                "linux-x86_64-cuda",
            ),
            (
                AnimeHostOs::Linux,
                AnimeHostArch::X86_64,
                AnimeRuntimeBackend::HipCpu,
                "linux-x86_64-hip",
            ),
            (
                AnimeHostOs::Linux,
                AnimeHostArch::X86_64,
                AnimeRuntimeBackend::VulkanCpu,
                "linux-x86_64-vulkan",
            ),
            (
                AnimeHostOs::Linux,
                AnimeHostArch::Aarch64,
                AnimeRuntimeBackend::Cpu,
                "linux-aarch64-cpu",
            ),
        ];
        for (os, arch, backend, expected) in cases {
            let mut artifact = runtime(backend);
            artifact.os = os;
            artifact.arch = arch;
            assert_eq!(runtime_id(&artifact), expected);
            assert!(!runtime_id(&artifact).ends_with("-cuda-cpu"));
        }
    }

    #[test]
    fn release_selection_allows_only_honestly_named_backends_and_macos_cpu() {
        let cases = [
            (AnimeRuntimeBackend::MetalCpu, AnimeExecutionBackend::Metal),
            (AnimeRuntimeBackend::CudaCpu, AnimeExecutionBackend::Cuda),
            (AnimeRuntimeBackend::HipCpu, AnimeExecutionBackend::Hip),
            (
                AnimeRuntimeBackend::VulkanCpu,
                AnimeExecutionBackend::Vulkan,
            ),
            (AnimeRuntimeBackend::Cpu, AnimeExecutionBackend::Cpu),
        ];
        for (backend, expected) in cases {
            let artifact = runtime(backend);
            let accelerator_device = (expected != AnimeExecutionBackend::Cpu).then(|| {
                inference_backend(expected)
                    .llama_device_selector()
                    .to_string()
            });
            let mut candidates = vec![ResolvedAnimeRuntime {
                artifact: artifact.clone(),
                execution_backend: expected,
                device_id: accelerator_device,
            }];
            if expected != AnimeExecutionBackend::Cpu {
                candidates.push(ResolvedAnimeRuntime {
                    artifact: artifact.clone(),
                    execution_backend: AnimeExecutionBackend::Cpu,
                    device_id: None,
                });
            }
            let selection = AnimeRuntimeSelection { candidates };
            let exact = exact_runtime_selection(&selection, &artifact).unwrap();
            assert_eq!(
                exact.candidates.len(),
                usize::from(backend == AnimeRuntimeBackend::MetalCpu) + 1
            );
            assert_eq!(exact.candidates[0].execution_backend, expected);
        }

        let metal = runtime(AnimeRuntimeBackend::MetalCpu);
        let cpu_only = AnimeRuntimeSelection {
            candidates: vec![ResolvedAnimeRuntime {
                artifact: metal.clone(),
                execution_backend: AnimeExecutionBackend::Cpu,
                device_id: None,
            }],
        };
        assert_eq!(
            exact_runtime_selection(&cpu_only, &metal)
                .unwrap()
                .candidates[0]
                .execution_backend,
            AnimeExecutionBackend::Cpu
        );

        let cuda = runtime(AnimeRuntimeBackend::CudaCpu);
        let false_cuda = AnimeRuntimeSelection {
            candidates: vec![ResolvedAnimeRuntime {
                artifact: cuda.clone(),
                execution_backend: AnimeExecutionBackend::Cpu,
                device_id: None,
            }],
        };
        assert!(exact_runtime_selection(&false_cuda, &cuda).is_err());
    }
}
