//! Automatic hardware-envelope selection for the local anime matcher.
//!
//! This module deliberately reuses playback's host identity and GPU inventory.
//! It adds only the resource information needed to decide whether the qualified
//! model can run without competing with playback. Model quality is established
//! by release qualification; nothing in this module selects model intelligence.

use std::{collections::BTreeSet, path::Path, process::Stdio, time::Duration};

#[cfg(any(target_os = "linux", test))]
use std::{
    collections::BTreeMap,
    path::{Component, PathBuf},
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{process::Command, time::timeout};

#[cfg(target_os = "linux")]
use tokio::fs;

use crate::playback::hardware::{
    HostGpuInventory, HostHardwareInventory, host_hardware_fingerprint,
};

use super::bundle::{
    ANIME_RUNTIME_PROFILE_SCHEMA_VERSION, AnimeAcceleratorBackend, AnimeExecutionBackend,
    AnimeGpuVendor, AnimeHostArch, AnimeHostOs, AnimeInferenceDevice, AnimeInferenceHost,
    AnimeKvCacheType, AnimeRuntimeBackend, AnimeRuntimeProbeResult, AnimeRuntimeProfile,
    ResolvedAnimeRuntime, ValidatedAnimeBundle,
};

pub const INFERENCE_HARDWARE_SCHEMA_VERSION: u32 = 1;
pub const INFERENCE_RUNTIME_PROFILE_SCHEMA_VERSION: u32 = 2;
pub const MIN_AVAILABLE_SYSTEM_MEMORY_BYTES: u64 = 4 * 1024 * 1024 * 1024;
pub const MIN_DEVICE_MEMORY_RESERVE_BYTES: u64 = 1024 * 1024 * 1024;
pub const MAX_WORKER_RSS_BYTES: u64 = 8 * 1024 * 1024 * 1024;
pub const MAX_INFERENCE_CPU_THREADS: u32 = 4;
pub const MAX_INFERENCE_BATCH_THREADS: u32 = 8;
pub const MAX_RUNTIME_PROFILE_CANDIDATES: usize = 8;
pub const MAX_PROBE_DETAIL_BYTES: usize = 512;
const FOUR_GIB: u64 = 4 * 1024 * 1024 * 1024;
const MIB: u64 = 1024 * 1024;
const RESOURCE_COMMAND_TIMEOUT: Duration = Duration::from_secs(3);
const PROBE_LOAD_ALLOWANCE: Duration = Duration::from_secs(2 * 60);
const PROBE_PRIME_ALLOWANCE: Duration = Duration::from_secs(5 * 60);
const PROBE_REQUEST_ALLOWANCE: Duration = Duration::from_secs(30 * 60);
const PROBE_FINALIZATION_ALLOWANCE: Duration = Duration::from_secs(10);
const PROBE_SCHEDULER_JITTER_ALLOWANCE: Duration = Duration::from_secs(4);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InferenceHardwareInventory {
    pub schema_version: u32,
    pub host_fingerprint: String,
    pub os_family: String,
    pub os_arch: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub os_version: Option<String>,
    pub cpu: InferenceCpuInventory,
    pub memory: InferenceSystemMemory,
    pub device_memory: Vec<InferenceDeviceMemory>,
    pub container: InferenceContainerInventory,
    pub collected_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InferenceCpuInventory {
    pub logical_cores: u32,
    pub physical_cores: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default)]
    pub features: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InferenceSystemMemory {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub available_bytes: Option<u64>,
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_limit_bytes: Option<u64>,
}

/// A cheap, current system-memory check for local-model admission. This never
/// probes GPUs, FFmpeg, drivers, or model artifacts.
///
/// Before starting a worker, pass its profiled peak RSS as
/// `profile_rss_ceiling_bytes`. Once that worker is resident, pass `None` so
/// its already-consumed memory is not counted twice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InferenceMemoryPressureSnapshot {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub available_system_bytes: Option<u64>,
    pub minimum_reserve_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile_rss_ceiling_bytes: Option<u64>,
    pub required_available_bytes: u64,
    pub under_pressure: bool,
    pub source: String,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeviceMemoryEvidenceSource {
    NvidiaSmi,
    LinuxSysfs,
    MacosSystemProfiler,
    MacosUnifiedMemory,
    WindowsCim,
    HostInventory,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InferenceDeviceMemory {
    pub device_key: String,
    pub gpu_index: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vendor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub available_bytes: Option<u64>,
    pub available_is_estimate: bool,
    pub source: DeviceMemoryEvidenceSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InferenceContainerKind {
    None,
    Docker,
    Podman,
    Kubernetes,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InferenceContainerInventory {
    pub detected: bool,
    pub kind: InferenceContainerKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InferenceBackend {
    Metal,
    Cuda,
    Hip,
    Vulkan,
    Cpu,
}

impl InferenceBackend {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Metal => "metal",
            Self::Cuda => "cuda",
            Self::Hip => "hip",
            Self::Vulkan => "vulkan",
            Self::Cpu => "cpu",
        }
    }

    /// Accepts both the logical backend names and the combined CPU-fallback
    /// artifact names used by the bundle manifest.
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "metal" | "metal_cpu" => Some(Self::Metal),
            "cuda" | "cuda_cpu" => Some(Self::Cuda),
            "hip" | "hip_cpu" => Some(Self::Hip),
            "vulkan" | "vulkan_cpu" => Some(Self::Vulkan),
            "cpu" => Some(Self::Cpu),
            _ => None,
        }
    }

    /// Exact llama.cpp device identifier used by Elixir's V1 runtime
    /// contract. V1 deliberately pins accelerator work to backend-local
    /// device zero; it never leaves llama.cpp's multi-device distribution
    /// enabled.
    pub fn llama_device_selector(self) -> &'static str {
        match self {
            // llama.cpp's stable backend-local identifier is `MTL0` on both
            // Intel and Apple Silicon Macs. `Metal` is the backend label, not
            // a valid value for the worker's --device argument.
            Self::Metal => "MTL0",
            Self::Cuda => "CUDA0",
            Self::Hip => "ROCm0",
            Self::Vulkan => "Vulkan0",
            Self::Cpu => "none",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InferenceDeviceClass {
    Apple,
    Nvidia,
    Amd,
    Intel,
    OtherGpu,
    Cpu,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InferenceModelEnvelope {
    pub model_size_bytes: u64,
    pub transformer_layers: u32,
}

impl InferenceModelEnvelope {
    pub fn is_valid(self) -> bool {
        self.model_size_bytes > 0 && self.transformer_layers > 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeProfilePolicy {
    pub certified_backends: BTreeSet<InferenceBackend>,
    pub model: InferenceModelEnvelope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InferenceRuntimeCandidate {
    pub backend: InferenceBackend,
    pub device_class: InferenceDeviceClass,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device_key: Option<String>,
    pub gpu_layers: u32,
    pub cpu_threads: u32,
    pub batch_threads: u32,
    pub required_device_reserve_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InferenceEnvelopeOutcome {
    GpuBalanced,
    CpuBalanced,
    DeterministicOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InferenceProbeStatus {
    Passed,
    Rejected,
    TimedOut,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InferenceProbeMeasurement {
    pub worker_ready: bool,
    pub smoke_match_passed: bool,
    pub load_time_ms: u64,
    pub warm_latency_ms: u64,
    pub peak_rss_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peak_device_memory_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_available_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device_available_bytes: Option<u64>,
    pub memory_pressure: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InferenceProbeAttempt {
    pub candidate: InferenceRuntimeCandidate,
    pub status: InferenceProbeStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rejection: Option<InferenceProbeRejection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub measurement: Option<InferenceProbeMeasurement>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InferenceProbeRejection {
    InvalidCandidate,
    WorkerNotReady,
    SmokeMatchFailed,
    LoadDeadlineExceeded,
    WarmDeadlineExceeded,
    WorkerMemoryUnavailable,
    WorkerMemoryExceeded,
    SystemMemoryReserve,
    DeviceMemoryReserve,
    MemoryPressure,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InferenceProbeLimits {
    pub per_candidate_timeout: Duration,
    pub maximum_load_time: Duration,
    pub maximum_warm_latency: Duration,
    pub maximum_worker_rss_bytes: u64,
    pub minimum_available_system_bytes: u64,
    pub minimum_available_device_bytes: u64,
}

impl Default for InferenceProbeLimits {
    fn default() -> Self {
        Self {
            // The wrapper leaves room for cold readiness, internal priming,
            // one production-bounded request, post-probe inventory collection,
            // and worker teardown. Add a small scheduler-jitter allowance so
            // scheduler jitter cannot cancel cleanup.
            per_candidate_timeout: PROBE_LOAD_ALLOWANCE
                .saturating_add(PROBE_PRIME_ALLOWANCE)
                .saturating_add(PROBE_REQUEST_ALLOWANCE)
                .saturating_add(PROBE_FINALIZATION_ALLOWANCE)
                .saturating_add(PROBE_SCHEDULER_JITTER_ALLOWANCE),
            maximum_load_time: PROBE_LOAD_ALLOWANCE,
            // Correctness determines model eligibility. Latency is retained as
            // hardware-routing evidence and rejected only at the same generous
            // final failure boundary used by production matching.
            maximum_warm_latency: PROBE_REQUEST_ALLOWANCE,
            maximum_worker_rss_bytes: MAX_WORKER_RSS_BYTES,
            minimum_available_system_bytes: MIN_AVAILABLE_SYSTEM_MEMORY_BYTES,
            minimum_available_device_bytes: MIN_DEVICE_MEMORY_RESERVE_BYTES,
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum InferenceProbeError {
    #[error("probe runner failed: {0}")]
    Runner(String),
}

#[async_trait]
pub trait InferenceEnvelopeProbe: Send + Sync {
    async fn probe(
        &self,
        candidate: &InferenceRuntimeCandidate,
    ) -> std::result::Result<InferenceProbeMeasurement, InferenceProbeError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeProfileIdentity {
    pub bundle_version: String,
    pub model_revision: String,
    pub worker_revision: String,
    pub runtime_policy_revision: String,
    pub kv_cache_type: AnimeKvCacheType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeProfileHealth {
    Healthy,
    Invalidated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InferenceRuntimeProfile {
    pub schema_version: u32,
    pub bundle_version: String,
    pub model_revision: String,
    pub worker_revision: String,
    pub runtime_policy_revision: String,
    pub hardware_fingerprint: String,
    pub backend: InferenceBackend,
    pub device_class: InferenceDeviceClass,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device_key: Option<String>,
    pub gpu_layers: u32,
    pub cpu_threads: u32,
    pub batch_threads: u32,
    pub kv_cache_type: String,
    pub outcome: InferenceEnvelopeOutcome,
    pub load_time_ms: u64,
    pub warm_latency_ms: u64,
    pub peak_rss_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peak_device_memory_bytes: Option<u64>,
    pub health: RuntimeProfileHealth,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invalidation_reason: Option<String>,
    pub probed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeProfileSelection {
    pub outcome: InferenceEnvelopeOutcome,
    pub profile: Option<InferenceRuntimeProfile>,
    pub attempts: Vec<InferenceProbeAttempt>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfileCompatibilityRequirements {
    pub bundle_version: String,
    pub model_revision: String,
    pub worker_revision: String,
    pub runtime_policy_revision: String,
    pub hardware_fingerprint: String,
    pub certified_backends: BTreeSet<InferenceBackend>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeProfileInvalidationReason {
    ProfileInvalid,
    SchemaChanged,
    BundleChanged,
    ModelChanged,
    WorkerChanged,
    RuntimePolicyChanged,
    HardwareChanged,
    BackendNoLongerCertified,
    HealthCheckFailed,
}

impl RuntimeProfileInvalidationReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ProfileInvalid => "profile_invalid",
            Self::SchemaChanged => "schema_changed",
            Self::BundleChanged => "bundle_changed",
            Self::ModelChanged => "model_changed",
            Self::WorkerChanged => "worker_changed",
            Self::RuntimePolicyChanged => "runtime_policy_changed",
            Self::HardwareChanged => "hardware_changed",
            Self::BackendNoLongerCertified => "backend_no_longer_certified",
            Self::HealthCheckFailed => "health_check_failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeProfileCompatibility {
    Compatible,
    Invalid(RuntimeProfileInvalidationReason),
}

#[derive(Debug, Error)]
pub enum RuntimeProfileDecodeError {
    #[error("invalid runtime profile JSON: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum InferenceHostConversionError {
    #[error("unsupported anime inference host OS '{0}'")]
    UnsupportedOs(String),
    #[error("unsupported anime inference host architecture '{0}'")]
    UnsupportedArch(String),
    #[error("host OS version could not be determined")]
    MissingOsVersion,
    #[error("playback and inference inventories describe different hosts")]
    InventoryMismatch,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum BundleRuntimeProfileBridgeError {
    #[error("inference probe profile is invalid or unhealthy")]
    InvalidProbeProfile,
    #[error("inference probe identity does not match the validated bundle")]
    BundleIdentityMismatch,
    #[error("resolved runtime is absent from the validated bundle")]
    RuntimeAbsentFromBundle,
    #[error("resolved runtime backend does not match the inference probe")]
    RuntimeBackendMismatch,
    #[error("resolved runtime device does not match the inference probe")]
    RuntimeDeviceMismatch,
    #[error("runtime policy does not match the inference probe")]
    RuntimePolicyMismatch,
    #[error("sealing the bundle runtime profile failed: {0}")]
    Seal(String),
}

#[derive(Debug, Clone)]
struct PlatformResourceSnapshot {
    physical_cores: Option<u32>,
    model: Option<String>,
    os_version: Option<String>,
    memory: InferenceSystemMemory,
}

/// Collects only inference-specific resource data. General OS, architecture,
/// GPU, and driver identity comes from the playback inventory supplied here.
pub async fn collect_inference_hardware_inventory(
    host: HostHardwareInventory,
) -> InferenceHardwareInventory {
    let logical_cores = u32::try_from(
        std::thread::available_parallelism()
            .map(|value| value.get())
            .unwrap_or(1),
    )
    .unwrap_or(u32::MAX)
    .max(1);
    let resources = collect_platform_resources(logical_cores).await;
    let physical_cores = resources
        .physical_cores
        .unwrap_or(logical_cores)
        .clamp(1, logical_cores);
    let device_memory = collect_device_memory(&host, &resources.memory).await;
    let container = detect_container_inventory();
    let host_fingerprint = host_hardware_fingerprint(&host);

    InferenceHardwareInventory {
        schema_version: INFERENCE_HARDWARE_SCHEMA_VERSION,
        host_fingerprint,
        os_family: host.os.family.clone(),
        os_arch: host.os.arch.clone(),
        os_version: host.os.version.clone().or(resources.os_version),
        cpu: InferenceCpuInventory {
            logical_cores,
            physical_cores,
            model: resources.model,
            features: runtime_cpu_features(),
        },
        memory: resources.memory,
        device_memory,
        container,
        collected_at: Utc::now(),
    }
}

/// Reads only current system-memory availability, including an effective
/// cgroup limit on Linux. It is intentionally independent of playback's GPU
/// inventory so it is inexpensive enough to refresh for admission decisions.
pub async fn collect_current_inference_memory() -> InferenceSystemMemory {
    #[cfg(target_os = "linux")]
    return collect_linux_memory().await;
    #[cfg(target_os = "macos")]
    return collect_current_macos_memory();
    #[cfg(windows)]
    return collect_windows_memory();
    #[allow(unreachable_code)]
    InferenceSystemMemory {
        total_bytes: None,
        available_bytes: None,
        source: "unavailable".to_string(),
        container_limit_bytes: None,
    }
}

/// Evaluates a memory sample against the 4 GiB system reserve and optional
/// cold-start RSS headroom. Missing availability is treated conservatively as
/// pressure, which sends the request through the deterministic path.
pub fn assess_inference_memory_pressure(
    memory: &InferenceSystemMemory,
    profile_rss_ceiling_bytes: Option<u64>,
) -> InferenceMemoryPressureSnapshot {
    let required_available_bytes =
        MIN_AVAILABLE_SYSTEM_MEMORY_BYTES.saturating_add(profile_rss_ceiling_bytes.unwrap_or(0));
    InferenceMemoryPressureSnapshot {
        available_system_bytes: memory.available_bytes,
        minimum_reserve_bytes: MIN_AVAILABLE_SYSTEM_MEMORY_BYTES,
        profile_rss_ceiling_bytes,
        required_available_bytes,
        under_pressure: memory
            .available_bytes
            .is_none_or(|available| available < required_available_bytes),
        source: memory.source.clone(),
        observed_at: Utc::now(),
    }
}

/// Convenience wrapper for a live, system-memory-only admission snapshot.
pub async fn collect_inference_memory_pressure(
    profile_rss_ceiling_bytes: Option<u64>,
) -> InferenceMemoryPressureSnapshot {
    let memory = collect_current_inference_memory().await;
    assess_inference_memory_pressure(&memory, profile_rss_ceiling_bytes)
}

/// Converts the shared playback inventory plus inference-only memory evidence
/// into the bundle resolver's host contract. Platform backend eligibility is
/// centralized here; the bundle resolver still intersects it with signed,
/// release-certified runtime artifacts.
pub fn bundle_inference_host(
    host: &HostHardwareInventory,
    inventory: &InferenceHardwareInventory,
) -> std::result::Result<AnimeInferenceHost, InferenceHostConversionError> {
    if !host.os.family.eq_ignore_ascii_case(&inventory.os_family)
        || !architecture_matches(&host.os.arch, &inventory.os_arch)
        || host_hardware_fingerprint(host) != inventory.host_fingerprint
    {
        return Err(InferenceHostConversionError::InventoryMismatch);
    }
    let os = match inventory.os_family.to_ascii_lowercase().as_str() {
        "macos" | "darwin" => AnimeHostOs::Macos,
        "windows" => AnimeHostOs::Windows,
        "linux" => AnimeHostOs::Linux,
        _ => {
            return Err(InferenceHostConversionError::UnsupportedOs(
                inventory.os_family.clone(),
            ));
        }
    };
    let arch = match inventory.os_arch.to_ascii_lowercase().as_str() {
        "x86_64" | "amd64" => AnimeHostArch::X86_64,
        "aarch64" | "arm64" => AnimeHostArch::Aarch64,
        _ => {
            return Err(InferenceHostConversionError::UnsupportedArch(
                inventory.os_arch.clone(),
            ));
        }
    };
    let os_version = host
        .os
        .version
        .as_deref()
        .and_then(normalized_numeric_version)
        .or_else(|| {
            inventory
                .os_version
                .as_deref()
                .and_then(normalized_numeric_version)
        })
        .ok_or(InferenceHostConversionError::MissingOsVersion)?;
    let cpu_features = inventory
        .cpu
        .features
        .iter()
        .filter_map(|feature| normalized_host_cpu_feature(feature))
        .collect();
    let linux_device_access = LinuxContainerDeviceAccess::detect();
    let devices = bundle_accelerator_order(os)
        .into_iter()
        .filter_map(|backend| {
            let inference_backend = inference_backend_for_bundle_backend(backend);
            let (device, _) = preferred_device_for_backend(inventory, inference_backend)?;
            let gpu = host.gpus.get(device.gpu_index);
            let vendor = bundle_gpu_vendor(
                device
                    .vendor
                    .as_deref()
                    .or_else(|| gpu.and_then(|gpu| gpu.vendor.as_deref())),
            );
            eligible_anime_accelerator_backends(os, vendor)
                .contains(&backend)
                .then(|| AnimeInferenceDevice {
                    // Persist the exact selector passed to llama-server. The
                    // evidence resolver below is backend-local: CUDA0/ROCm0
                    // use their vendor pools, while Metal/Vulkan require an
                    // unambiguous physical adapter.
                    id: anime_backend_device_selector(backend).to_string(),
                    vendor,
                    driver_version: gpu.and_then(|gpu| gpu.driver_version.clone()),
                    available_memory_bytes: device.available_bytes.or_else(|| {
                        device
                            .total_bytes
                            .map(|total| total.saturating_sub(MIN_DEVICE_MEMORY_RESERVE_BYTES))
                    }),
                    certified_backends: [backend].into_iter().collect(),
                    exposed_to_container: !inventory.container.detected
                        || device_backend_is_exposed_in_container(
                            os,
                            device.source,
                            backend,
                            linux_device_access,
                        ),
                })
        })
        .collect();
    Ok(AnimeInferenceHost {
        os,
        arch,
        os_version: Some(os_version),
        cpu_features,
        devices,
        containerized: inventory.container.detected,
    })
}

/// Bridges a successful hardware-envelope probe into the single persisted
/// runtime-profile contract owned by the bundle store. Every identity and
/// execution field is checked before the bundle profile is sealed.
pub fn bundle_runtime_profile_from_probe(
    bundle: &ValidatedAnimeBundle,
    runtime: &ResolvedAnimeRuntime,
    profile: &InferenceRuntimeProfile,
) -> std::result::Result<AnimeRuntimeProfile, BundleRuntimeProfileBridgeError> {
    if profile.schema_version != INFERENCE_RUNTIME_PROFILE_SCHEMA_VERSION
        || profile.health != RuntimeProfileHealth::Healthy
        || profile.invalidation_reason.is_some()
        || !runtime_profile_is_well_formed(profile)
    {
        return Err(BundleRuntimeProfileBridgeError::InvalidProbeProfile);
    }
    let manifest = bundle.manifest();
    if profile.bundle_version != manifest.bundle_version
        || profile.model_revision != manifest.model.revision
        || profile.worker_revision != manifest.worker_revision
    {
        return Err(BundleRuntimeProfileBridgeError::BundleIdentityMismatch);
    }
    if !manifest
        .runtimes
        .iter()
        .any(|candidate| candidate == &runtime.artifact)
    {
        return Err(BundleRuntimeProfileBridgeError::RuntimeAbsentFromBundle);
    }
    let execution_backend = bundle_execution_backend(profile.backend);
    if execution_backend != runtime.execution_backend
        || !resolved_runtime_execution_is_valid(runtime)
    {
        return Err(BundleRuntimeProfileBridgeError::RuntimeBackendMismatch);
    }
    if profile.device_key != runtime.device_id {
        return Err(BundleRuntimeProfileBridgeError::RuntimeDeviceMismatch);
    }
    let kv_cache_type = manifest.runtime_policy.kv_cache_type;
    if profile.runtime_policy_revision != manifest.runtime_policy.sampling_profile_revision
        || profile.kv_cache_type != bundle_kv_cache_name(kv_cache_type)
    {
        return Err(BundleRuntimeProfileBridgeError::RuntimePolicyMismatch);
    }
    let probe_result = match profile.outcome {
        InferenceEnvelopeOutcome::GpuBalanced => AnimeRuntimeProbeResult::GpuBalanced,
        InferenceEnvelopeOutcome::CpuBalanced => AnimeRuntimeProbeResult::CpuBalanced,
        InferenceEnvelopeOutcome::DeterministicOnly => {
            return Err(BundleRuntimeProfileBridgeError::InvalidProbeProfile);
        }
    };
    AnimeRuntimeProfile {
        schema_version: ANIME_RUNTIME_PROFILE_SCHEMA_VERSION,
        bundle_version: manifest.bundle_version.clone(),
        model_id: manifest.model.id.clone(),
        model_revision: manifest.model.revision.clone(),
        worker_revision: manifest.worker_revision.clone(),
        runtime_artifact_key: runtime.artifact.artifact_key(),
        host_fingerprint: profile.hardware_fingerprint.clone(),
        execution_backend,
        device_id: runtime.device_id.clone(),
        gpu_layer_count: profile.gpu_layers,
        cpu_thread_count: u16::try_from(profile.cpu_threads)
            .map_err(|_| BundleRuntimeProfileBridgeError::InvalidProbeProfile)?,
        batch_thread_count: u16::try_from(profile.batch_threads)
            .map_err(|_| BundleRuntimeProfileBridgeError::InvalidProbeProfile)?,
        kv_cache_type,
        load_time_ms: profile.load_time_ms,
        warm_latency_ms: profile.warm_latency_ms,
        peak_rss_bytes: profile.peak_rss_bytes,
        peak_device_memory_bytes: profile.peak_device_memory_bytes,
        probe_result,
        probed_at: profile.probed_at.to_rfc3339(),
        profile_fingerprint: String::new(),
    }
    .seal()
    .map_err(|error| BundleRuntimeProfileBridgeError::Seal(error.to_string()))
}

fn bundle_execution_backend(backend: InferenceBackend) -> AnimeExecutionBackend {
    match backend {
        InferenceBackend::Metal => AnimeExecutionBackend::Metal,
        InferenceBackend::Cuda => AnimeExecutionBackend::Cuda,
        InferenceBackend::Hip => AnimeExecutionBackend::Hip,
        InferenceBackend::Vulkan => AnimeExecutionBackend::Vulkan,
        InferenceBackend::Cpu => AnimeExecutionBackend::Cpu,
    }
}

fn resolved_runtime_execution_is_valid(runtime: &ResolvedAnimeRuntime) -> bool {
    matches!(
        (runtime.execution_backend, runtime.artifact.backend),
        (
            AnimeExecutionBackend::Cpu,
            AnimeRuntimeBackend::Cpu | AnimeRuntimeBackend::MetalCpu
        ) | (AnimeExecutionBackend::Metal, AnimeRuntimeBackend::MetalCpu)
            | (AnimeExecutionBackend::Cuda, AnimeRuntimeBackend::CudaCpu)
            | (AnimeExecutionBackend::Hip, AnimeRuntimeBackend::HipCpu)
            | (
                AnimeExecutionBackend::Vulkan,
                AnimeRuntimeBackend::VulkanCpu
            )
    )
}

fn bundle_kv_cache_name(cache: AnimeKvCacheType) -> &'static str {
    match cache {
        AnimeKvCacheType::F16 => "f16",
        AnimeKvCacheType::Q8_0 => "q8_0",
    }
}

/// Ordered host-side accelerator policy used to populate the bundle resolver
/// and to make the preferred order directly testable.
pub fn eligible_anime_accelerator_backends(
    os: AnimeHostOs,
    vendor: AnimeGpuVendor,
) -> Vec<AnimeAcceleratorBackend> {
    match (os, vendor) {
        (AnimeHostOs::Macos, _) => vec![AnimeAcceleratorBackend::Metal],
        (AnimeHostOs::Windows, AnimeGpuVendor::Nvidia)
        | (AnimeHostOs::Linux, AnimeGpuVendor::Nvidia) => vec![
            AnimeAcceleratorBackend::Cuda,
            AnimeAcceleratorBackend::Vulkan,
        ],
        (AnimeHostOs::Linux, AnimeGpuVendor::Amd) => vec![
            AnimeAcceleratorBackend::Hip,
            AnimeAcceleratorBackend::Vulkan,
        ],
        (AnimeHostOs::Windows, AnimeGpuVendor::Amd | AnimeGpuVendor::Intel)
        | (AnimeHostOs::Linux, AnimeGpuVendor::Intel) => {
            vec![AnimeAcceleratorBackend::Vulkan]
        }
        _ => Vec::new(),
    }
}

fn architecture_matches(left: &str, right: &str) -> bool {
    matches!(
        (
            left.trim().to_ascii_lowercase().as_str(),
            right.trim().to_ascii_lowercase().as_str(),
        ),
        ("x86_64" | "amd64", "x86_64" | "amd64") | ("aarch64" | "arm64", "aarch64" | "arm64")
    )
}

fn normalized_host_cpu_feature(value: &str) -> Option<String> {
    let value = value.trim().to_ascii_lowercase().replace(['-', '.'], "_");
    (!value.is_empty()
        && value.len() <= 64
        && value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_'))
    .then_some(value)
}

fn bundle_gpu_vendor(value: Option<&str>) -> AnimeGpuVendor {
    match value
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "nvidia" => AnimeGpuVendor::Nvidia,
        "amd" => AnimeGpuVendor::Amd,
        "intel" => AnimeGpuVendor::Intel,
        "apple" => AnimeGpuVendor::Apple,
        _ => AnimeGpuVendor::Unknown,
    }
}

fn anime_backend_device_selector(backend: AnimeAcceleratorBackend) -> &'static str {
    match backend {
        AnimeAcceleratorBackend::Metal => InferenceBackend::Metal.llama_device_selector(),
        AnimeAcceleratorBackend::Cuda => InferenceBackend::Cuda.llama_device_selector(),
        AnimeAcceleratorBackend::Hip => InferenceBackend::Hip.llama_device_selector(),
        AnimeAcceleratorBackend::Vulkan => InferenceBackend::Vulkan.llama_device_selector(),
    }
}

fn inference_backend_for_bundle_backend(backend: AnimeAcceleratorBackend) -> InferenceBackend {
    match backend {
        AnimeAcceleratorBackend::Metal => InferenceBackend::Metal,
        AnimeAcceleratorBackend::Cuda => InferenceBackend::Cuda,
        AnimeAcceleratorBackend::Hip => InferenceBackend::Hip,
        AnimeAcceleratorBackend::Vulkan => InferenceBackend::Vulkan,
    }
}

fn bundle_accelerator_order(os: AnimeHostOs) -> Vec<AnimeAcceleratorBackend> {
    match os {
        AnimeHostOs::Macos => vec![AnimeAcceleratorBackend::Metal],
        AnimeHostOs::Windows => vec![
            AnimeAcceleratorBackend::Cuda,
            AnimeAcceleratorBackend::Vulkan,
        ],
        AnimeHostOs::Linux => vec![
            AnimeAcceleratorBackend::Cuda,
            AnimeAcceleratorBackend::Hip,
            AnimeAcceleratorBackend::Vulkan,
        ],
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct LinuxContainerDeviceAccess {
    nvidia_zero: bool,
    wsl_dxg: bool,
    drm_render: bool,
    kfd: bool,
}

impl LinuxContainerDeviceAccess {
    fn detect() -> Self {
        #[cfg(target_os = "linux")]
        {
            Self {
                nvidia_zero: device_node_is_accessible(Path::new("/dev/nvidia0")),
                // Docker Desktop exposes NVIDIA compute to Linux containers
                // through WSL's DirectX GPU bridge instead of the native
                // Linux /dev/nvidia* device family.
                wsl_dxg: device_node_is_accessible(Path::new("/dev/dxg")),
                drm_render: linux_render_node_is_accessible(),
                kfd: device_node_is_accessible(Path::new("/dev/kfd")),
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            Self::default()
        }
    }
}

fn device_backend_is_exposed_in_container(
    os: AnimeHostOs,
    source: DeviceMemoryEvidenceSource,
    backend: AnimeAcceleratorBackend,
    linux_access: LinuxContainerDeviceAccess,
) -> bool {
    if os == AnimeHostOs::Linux {
        return match backend {
            AnimeAcceleratorBackend::Cuda => {
                source == DeviceMemoryEvidenceSource::NvidiaSmi
                    && (linux_access.nvidia_zero || linux_access.wsl_dxg)
            }
            AnimeAcceleratorBackend::Hip => linux_access.drm_render && linux_access.kfd,
            AnimeAcceleratorBackend::Vulkan => linux_access.drm_render,
            AnimeAcceleratorBackend::Metal => false,
        };
    }
    matches!(
        source,
        DeviceMemoryEvidenceSource::NvidiaSmi
            | DeviceMemoryEvidenceSource::MacosSystemProfiler
            | DeviceMemoryEvidenceSource::MacosUnifiedMemory
            | DeviceMemoryEvidenceSource::WindowsCim
    )
}

#[cfg(target_os = "linux")]
fn linux_render_node_is_accessible() -> bool {
    std::fs::read_dir("/dev/dri").is_ok_and(|entries| {
        entries.filter_map(std::result::Result::ok).any(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with("renderD"))
                && device_node_is_accessible(&entry.path())
        })
    })
}

#[cfg(target_os = "linux")]
fn device_node_is_accessible(path: &Path) -> bool {
    use std::os::unix::fs::FileTypeExt;

    path.metadata()
        .ok()
        .is_some_and(|metadata| metadata.file_type().is_char_device())
        && std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .is_ok()
}

/// Stable inference fingerprint. Dynamic available-memory values and the
/// observation timestamp are intentionally excluded so ordinary pressure does
/// not invalidate an otherwise valid profile.
pub fn inference_hardware_fingerprint(inventory: &InferenceHardwareInventory) -> String {
    let device_memory = inventory
        .device_memory
        .iter()
        .map(|device| {
            json!({
                "deviceKey": device.device_key,
                "vendor": device.vendor,
                "model": device.model,
                "totalBytes": device.total_bytes,
                "source": device.source,
            })
        })
        .collect::<Vec<_>>();
    let payload = json!({
        "schemaVersion": INFERENCE_HARDWARE_SCHEMA_VERSION,
        "hostFingerprint": inventory.host_fingerprint,
        "osFamily": inventory.os_family,
        "osArch": inventory.os_arch,
        "osVersion": inventory.os_version,
        "cpu": {
            "logicalCores": inventory.cpu.logical_cores,
            "physicalCores": inventory.cpu.physical_cores,
            "model": inventory.cpu.model,
            "features": inventory.cpu.features,
        },
        "totalMemoryBytes": inventory.memory.total_bytes,
        "containerLimitBytes": inventory.memory.container_limit_bytes,
        "deviceMemory": device_memory,
        "container": inventory.container,
    });
    let encoded = serde_json::to_vec(&payload).unwrap_or_default();
    format!("sha256:{:x}", Sha256::digest(encoded))
}

pub fn recommended_cpu_threads(physical_cores: u32) -> u32 {
    physical_cores
        .div_ceil(2)
        .clamp(1, MAX_INFERENCE_CPU_THREADS)
}

/// Separates prompt-prefill parallelism from the generation thread count.
/// Every value returned here is an explicit probe candidate; the runtime does
/// not derive or silently override it later.
pub fn optimized_batch_threads(physical_cores: u32, cpu_threads: u32) -> u32 {
    physical_cores
        .min(cpu_threads.saturating_mul(2))
        .min(MAX_INFERENCE_BATCH_THREADS)
}

/// Produces the small, ordered probe set for one already-qualified model.
/// Backends absent from release certification are never attempted.
pub fn runtime_profile_candidates(
    inventory: &InferenceHardwareInventory,
    policy: &RuntimeProfilePolicy,
) -> Vec<InferenceRuntimeCandidate> {
    if !policy.model.is_valid()
        || inventory
            .memory
            .available_bytes
            .is_none_or(|bytes| bytes < MIN_AVAILABLE_SYSTEM_MEMORY_BYTES)
    {
        return Vec::new();
    }

    let cpu_threads = recommended_cpu_threads(inventory.cpu.physical_cores);
    let optimized_cpu_batch_threads =
        optimized_batch_threads(inventory.cpu.physical_cores, cpu_threads);
    let mut candidates = Vec::new();
    let backend_order = preferred_backend_order(inventory);
    for backend in backend_order {
        if backend == InferenceBackend::Cpu || !policy.certified_backends.contains(&backend) {
            continue;
        }
        let Some((device, device_class)) = preferred_device_for_backend(inventory, backend) else {
            continue;
        };
        let maximum_layers = conservative_gpu_layer_cap(device, policy.model);
        if maximum_layers == 0 {
            continue;
        }
        push_candidate(
            &mut candidates,
            InferenceRuntimeCandidate {
                backend,
                device_class,
                device_key: Some(backend.llama_device_selector().to_string()),
                gpu_layers: maximum_layers,
                cpu_threads,
                batch_threads: cpu_threads,
                required_device_reserve_bytes: MIN_DEVICE_MEMORY_RESERVE_BYTES,
            },
        );
        let reduced_layers = (maximum_layers / 2).max(1);
        if reduced_layers < maximum_layers {
            push_candidate(
                &mut candidates,
                InferenceRuntimeCandidate {
                    backend,
                    device_class,
                    device_key: Some(backend.llama_device_selector().to_string()),
                    gpu_layers: reduced_layers,
                    cpu_threads,
                    batch_threads: cpu_threads,
                    required_device_reserve_bytes: MIN_DEVICE_MEMORY_RESERVE_BYTES,
                },
            );
        }
    }

    if policy.certified_backends.contains(&InferenceBackend::Cpu) {
        for batch_threads in [optimized_cpu_batch_threads, cpu_threads] {
            push_candidate(
                &mut candidates,
                InferenceRuntimeCandidate {
                    backend: InferenceBackend::Cpu,
                    device_class: InferenceDeviceClass::Cpu,
                    device_key: None,
                    gpu_layers: 0,
                    cpu_threads,
                    batch_threads,
                    required_device_reserve_bytes: 0,
                },
            );
        }
        let reduced_threads = (cpu_threads / 2).max(1);
        if reduced_threads < cpu_threads {
            let optimized_reduced_batch_threads =
                optimized_batch_threads(inventory.cpu.physical_cores, reduced_threads);
            for batch_threads in [optimized_reduced_batch_threads, reduced_threads] {
                push_candidate(
                    &mut candidates,
                    InferenceRuntimeCandidate {
                        backend: InferenceBackend::Cpu,
                        device_class: InferenceDeviceClass::Cpu,
                        device_key: None,
                        gpu_layers: 0,
                        cpu_threads: reduced_threads,
                        batch_threads,
                        required_device_reserve_bytes: 0,
                    },
                );
            }
        }
    }
    candidates.truncate(MAX_RUNTIME_PROFILE_CANDIDATES);
    candidates
}

pub async fn select_runtime_profile<P: InferenceEnvelopeProbe>(
    inventory: &InferenceHardwareInventory,
    identity: &RuntimeProfileIdentity,
    candidates: &[InferenceRuntimeCandidate],
    limits: &InferenceProbeLimits,
    probe: &P,
) -> RuntimeProfileSelection {
    let mut attempts = Vec::new();
    for candidate in candidates.iter().take(MAX_RUNTIME_PROFILE_CANDIDATES) {
        if !runtime_candidate_is_well_formed(inventory, candidate) {
            attempts.push(InferenceProbeAttempt {
                candidate: candidate.clone(),
                status: InferenceProbeStatus::Rejected,
                rejection: Some(InferenceProbeRejection::InvalidCandidate),
                measurement: None,
                detail: None,
            });
            continue;
        }
        let result = timeout(limits.per_candidate_timeout, probe.probe(candidate)).await;
        let (status, measurement, rejection, detail) = match result {
            Err(_) => (
                InferenceProbeStatus::TimedOut,
                None,
                None,
                Some("hardware-envelope probe exceeded its deadline".to_string()),
            ),
            Ok(Err(error)) => (
                InferenceProbeStatus::Failed,
                None,
                None,
                Some(error.to_string()),
            ),
            Ok(Ok(measurement)) => {
                if let Some(rejection) = probe_rejection(candidate, &measurement, limits) {
                    (
                        InferenceProbeStatus::Rejected,
                        Some(measurement),
                        Some(rejection),
                        None,
                    )
                } else {
                    let outcome = if candidate.backend == InferenceBackend::Cpu {
                        InferenceEnvelopeOutcome::CpuBalanced
                    } else {
                        InferenceEnvelopeOutcome::GpuBalanced
                    };
                    let profile = runtime_profile_from_probe(
                        inventory,
                        identity,
                        candidate,
                        &measurement,
                        outcome,
                    );
                    attempts.push(InferenceProbeAttempt {
                        candidate: candidate.clone(),
                        status: InferenceProbeStatus::Passed,
                        rejection: None,
                        measurement: Some(measurement),
                        detail: None,
                    });
                    return RuntimeProfileSelection {
                        outcome,
                        profile: Some(profile),
                        attempts,
                    };
                }
            }
        };
        attempts.push(InferenceProbeAttempt {
            candidate: candidate.clone(),
            status,
            rejection,
            measurement,
            detail: detail.map(|value| bounded_detail(&value)),
        });
    }

    RuntimeProfileSelection {
        outcome: InferenceEnvelopeOutcome::DeterministicOnly,
        profile: None,
        attempts,
    }
}

fn runtime_candidate_is_well_formed(
    inventory: &InferenceHardwareInventory,
    candidate: &InferenceRuntimeCandidate,
) -> bool {
    if !(1..=MAX_INFERENCE_CPU_THREADS).contains(&candidate.cpu_threads)
        || !(candidate.cpu_threads..=MAX_INFERENCE_BATCH_THREADS).contains(&candidate.batch_threads)
        || (inventory.cpu.physical_cores > 0
            && (candidate.cpu_threads > inventory.cpu.physical_cores
                || candidate.batch_threads > inventory.cpu.physical_cores))
    {
        return false;
    }
    match candidate.backend {
        InferenceBackend::Cpu => {
            candidate.device_class == InferenceDeviceClass::Cpu
                && candidate.device_key.is_none()
                && candidate.gpu_layers == 0
                && candidate.required_device_reserve_bytes == 0
        }
        _ => {
            candidate.device_class != InferenceDeviceClass::Cpu
                && candidate.gpu_layers > 0
                && candidate.required_device_reserve_bytes >= MIN_DEVICE_MEMORY_RESERVE_BYTES
                && candidate.device_key.as_deref()
                    == Some(candidate.backend.llama_device_selector())
                && runtime_device_memory(
                    inventory,
                    candidate.backend,
                    candidate.device_key.as_deref(),
                )
                .is_some_and(|device| {
                    device_class(device.vendor.as_deref()) == candidate.device_class
                })
        }
    }
}

/// Resolves a persisted llama.cpp selector back to its physical memory
/// evidence. This is used for before/after VRAM accounting without weakening
/// the persisted selected-device identity.
pub fn runtime_device_memory<'a>(
    inventory: &'a InferenceHardwareInventory,
    backend: InferenceBackend,
    selector: Option<&str>,
) -> Option<&'a InferenceDeviceMemory> {
    if backend == InferenceBackend::Cpu || selector != Some(backend.llama_device_selector()) {
        return None;
    }
    preferred_device_for_backend(inventory, backend).map(|(device, _)| device)
}

/// Returns whether a persisted accelerated profile has stable, comparable
/// driver evidence on the same physical device selected by llama.cpp.
///
/// Unknown driver evidence is deliberately allowed into the disposable probe
/// path: the worker itself is the most reliable compatibility test in minimal
/// Linux installations and containers. It is not sufficient to reuse a cached
/// accelerated profile across a restart, however, because a driver change
/// could otherwise be invisible to the hardware fingerprint. CPU profiles do
/// not depend on accelerator driver evidence. Apple ships Metal with the OS,
/// so its normalized OS version is the comparable driver boundary.
pub fn cached_profile_driver_evidence_is_reusable(
    host: &HostHardwareInventory,
    inventory: &InferenceHardwareInventory,
    backend: AnimeExecutionBackend,
    device_id: Option<&str>,
) -> bool {
    let inference_backend = match backend {
        AnimeExecutionBackend::Cpu => return device_id.is_none(),
        AnimeExecutionBackend::Metal => InferenceBackend::Metal,
        AnimeExecutionBackend::Cuda => InferenceBackend::Cuda,
        AnimeExecutionBackend::Hip => InferenceBackend::Hip,
        AnimeExecutionBackend::Vulkan => InferenceBackend::Vulkan,
    };
    if host_hardware_fingerprint(host) != inventory.host_fingerprint {
        return false;
    }
    if device_id != Some(inference_backend.llama_device_selector()) {
        return false;
    }
    let Some((device, _)) = preferred_device_for_backend(inventory, inference_backend) else {
        return false;
    };
    if backend == AnimeExecutionBackend::Metal {
        return host
            .os
            .version
            .as_deref()
            .filter(|version| numeric_version_is_comparable(version))
            .or_else(|| {
                inventory
                    .os_version
                    .as_deref()
                    .filter(|version| numeric_version_is_comparable(version))
            })
            .is_some();
    }
    host.gpus
        .get(device.gpu_index)
        .and_then(|gpu| gpu.driver_version.as_deref())
        .is_some_and(numeric_version_is_comparable)
}

pub fn runtime_profile_compatibility(
    profile: &InferenceRuntimeProfile,
    requirements: &ProfileCompatibilityRequirements,
) -> RuntimeProfileCompatibility {
    let invalid = if !runtime_profile_is_well_formed(profile) {
        Some(RuntimeProfileInvalidationReason::ProfileInvalid)
    } else if profile.schema_version != INFERENCE_RUNTIME_PROFILE_SCHEMA_VERSION {
        Some(RuntimeProfileInvalidationReason::SchemaChanged)
    } else if profile.bundle_version != requirements.bundle_version {
        Some(RuntimeProfileInvalidationReason::BundleChanged)
    } else if profile.model_revision != requirements.model_revision {
        Some(RuntimeProfileInvalidationReason::ModelChanged)
    } else if profile.worker_revision != requirements.worker_revision {
        Some(RuntimeProfileInvalidationReason::WorkerChanged)
    } else if profile.runtime_policy_revision != requirements.runtime_policy_revision {
        Some(RuntimeProfileInvalidationReason::RuntimePolicyChanged)
    } else if profile.hardware_fingerprint != requirements.hardware_fingerprint {
        Some(RuntimeProfileInvalidationReason::HardwareChanged)
    } else if !requirements.certified_backends.contains(&profile.backend) {
        Some(RuntimeProfileInvalidationReason::BackendNoLongerCertified)
    } else if profile.health != RuntimeProfileHealth::Healthy {
        Some(RuntimeProfileInvalidationReason::HealthCheckFailed)
    } else {
        None
    };
    invalid.map_or(
        RuntimeProfileCompatibility::Compatible,
        RuntimeProfileCompatibility::Invalid,
    )
}

pub fn invalidate_runtime_profile(
    profile: &mut InferenceRuntimeProfile,
    reason: RuntimeProfileInvalidationReason,
) {
    profile.health = RuntimeProfileHealth::Invalidated;
    profile.invalidation_reason = Some(reason.as_str().to_string());
}

pub fn decode_runtime_profile(
    bytes: &[u8],
) -> std::result::Result<InferenceRuntimeProfile, RuntimeProfileDecodeError> {
    Ok(serde_json::from_slice(bytes)?)
}

pub fn decode_compatible_runtime_profile(
    bytes: &[u8],
    requirements: &ProfileCompatibilityRequirements,
) -> std::result::Result<
    std::result::Result<InferenceRuntimeProfile, RuntimeProfileInvalidationReason>,
    RuntimeProfileDecodeError,
> {
    let profile = decode_runtime_profile(bytes)?;
    match runtime_profile_compatibility(&profile, requirements) {
        RuntimeProfileCompatibility::Compatible => Ok(Ok(profile)),
        RuntimeProfileCompatibility::Invalid(reason) => Ok(Err(reason)),
    }
}

fn runtime_profile_from_probe(
    inventory: &InferenceHardwareInventory,
    identity: &RuntimeProfileIdentity,
    candidate: &InferenceRuntimeCandidate,
    measurement: &InferenceProbeMeasurement,
    outcome: InferenceEnvelopeOutcome,
) -> InferenceRuntimeProfile {
    InferenceRuntimeProfile {
        schema_version: INFERENCE_RUNTIME_PROFILE_SCHEMA_VERSION,
        bundle_version: identity.bundle_version.clone(),
        model_revision: identity.model_revision.clone(),
        worker_revision: identity.worker_revision.clone(),
        runtime_policy_revision: identity.runtime_policy_revision.clone(),
        hardware_fingerprint: inference_hardware_fingerprint(inventory),
        backend: candidate.backend,
        device_class: candidate.device_class,
        device_key: candidate.device_key.clone(),
        gpu_layers: candidate.gpu_layers,
        cpu_threads: candidate.cpu_threads,
        batch_threads: candidate.batch_threads,
        kv_cache_type: bundle_kv_cache_name(identity.kv_cache_type).to_string(),
        outcome,
        load_time_ms: measurement.load_time_ms,
        warm_latency_ms: measurement.warm_latency_ms,
        peak_rss_bytes: measurement.peak_rss_bytes,
        peak_device_memory_bytes: measurement.peak_device_memory_bytes,
        health: RuntimeProfileHealth::Healthy,
        invalidation_reason: None,
        probed_at: Utc::now(),
    }
}

fn runtime_profile_is_well_formed(profile: &InferenceRuntimeProfile) -> bool {
    let identity_is_valid = [
        profile.bundle_version.as_str(),
        profile.model_revision.as_str(),
        profile.worker_revision.as_str(),
        profile.runtime_policy_revision.as_str(),
        profile.hardware_fingerprint.as_str(),
    ]
    .into_iter()
    .all(|value| !value.trim().is_empty());
    let execution_is_valid = profile.peak_rss_bytes > 0
        && (1..=MAX_INFERENCE_CPU_THREADS).contains(&profile.cpu_threads)
        && (profile.cpu_threads..=MAX_INFERENCE_BATCH_THREADS).contains(&profile.batch_threads)
        && matches!(profile.kv_cache_type.as_str(), "f16" | "q8_0")
        && match profile.backend {
            InferenceBackend::Cpu => {
                profile.gpu_layers == 0
                    && profile.device_key.is_none()
                    && profile.device_class == InferenceDeviceClass::Cpu
                    && profile.outcome == InferenceEnvelopeOutcome::CpuBalanced
            }
            _ => {
                profile.gpu_layers > 0
                    && profile
                        .device_key
                        .as_deref()
                        .is_some_and(|key| !key.trim().is_empty())
                    && profile.device_class != InferenceDeviceClass::Cpu
                    && profile.outcome == InferenceEnvelopeOutcome::GpuBalanced
            }
        };
    identity_is_valid && execution_is_valid
}

fn probe_rejection(
    candidate: &InferenceRuntimeCandidate,
    measurement: &InferenceProbeMeasurement,
    limits: &InferenceProbeLimits,
) -> Option<InferenceProbeRejection> {
    if !measurement.worker_ready {
        return Some(InferenceProbeRejection::WorkerNotReady);
    }
    if !measurement.smoke_match_passed {
        return Some(InferenceProbeRejection::SmokeMatchFailed);
    }
    if measurement.load_time_ms
        > u64::try_from(limits.maximum_load_time.as_millis()).unwrap_or(u64::MAX)
    {
        return Some(InferenceProbeRejection::LoadDeadlineExceeded);
    }
    if measurement.warm_latency_ms
        > u64::try_from(limits.maximum_warm_latency.as_millis()).unwrap_or(u64::MAX)
    {
        return Some(InferenceProbeRejection::WarmDeadlineExceeded);
    }
    if measurement.peak_rss_bytes == 0 {
        return Some(InferenceProbeRejection::WorkerMemoryUnavailable);
    }
    if measurement.peak_rss_bytes > limits.maximum_worker_rss_bytes {
        return Some(InferenceProbeRejection::WorkerMemoryExceeded);
    }
    if measurement
        .system_available_bytes
        .is_none_or(|bytes| bytes < limits.minimum_available_system_bytes)
    {
        return Some(InferenceProbeRejection::SystemMemoryReserve);
    }
    if candidate.backend != InferenceBackend::Cpu
        && measurement
            .device_available_bytes
            .is_none_or(|bytes| bytes < limits.minimum_available_device_bytes)
    {
        return Some(InferenceProbeRejection::DeviceMemoryReserve);
    }
    measurement
        .memory_pressure
        .then_some(InferenceProbeRejection::MemoryPressure)
}

fn preferred_backend_order(inventory: &InferenceHardwareInventory) -> Vec<InferenceBackend> {
    let os = inventory.os_family.to_ascii_lowercase();
    let arch = inventory.os_arch.to_ascii_lowercase();
    let vendors = inventory
        .device_memory
        .iter()
        .filter_map(|device| device.vendor.as_deref())
        .map(str::to_ascii_lowercase)
        .collect::<BTreeSet<_>>();

    if macos_x86_64_requires_cpu(inventory) {
        return vec![InferenceBackend::Cpu];
    }
    if os == "macos" || os == "darwin" {
        return vec![InferenceBackend::Metal, InferenceBackend::Cpu];
    }
    if os == "windows" {
        if vendors.contains("nvidia") {
            return vec![
                InferenceBackend::Cuda,
                InferenceBackend::Vulkan,
                InferenceBackend::Cpu,
            ];
        }
        return vec![InferenceBackend::Vulkan, InferenceBackend::Cpu];
    }
    if os == "linux" {
        if arch == "aarch64" || arch == "arm64" {
            return if inventory.device_memory.is_empty() {
                vec![InferenceBackend::Cpu]
            } else {
                vec![InferenceBackend::Vulkan, InferenceBackend::Cpu]
            };
        }
        if vendors.contains("nvidia") {
            return vec![
                InferenceBackend::Cuda,
                InferenceBackend::Vulkan,
                InferenceBackend::Cpu,
            ];
        }
        if vendors.contains("amd") {
            return vec![
                InferenceBackend::Hip,
                InferenceBackend::Vulkan,
                InferenceBackend::Cpu,
            ];
        }
        if vendors.contains("intel") {
            return vec![InferenceBackend::Vulkan, InferenceBackend::Cpu];
        }
    }
    vec![InferenceBackend::Cpu]
}

fn preferred_device_for_backend(
    inventory: &InferenceHardwareInventory,
    backend: InferenceBackend,
) -> Option<(&InferenceDeviceMemory, InferenceDeviceClass)> {
    // A live partial-offload probe on an Intel Mac with discrete Radeon
    // graphics can wedge WindowServer and trigger the userspace watchdog. Do
    // not expose Metal as a viable device on x86_64 macOS: preventing the
    // device binding also invalidates any previously cached Metal profile.
    if backend == InferenceBackend::Metal && macos_x86_64_requires_cpu(inventory) {
        return None;
    }
    let device = match backend {
        // CUDA device order is not contractually identical to nvidia-smi order.
        // Without a worker-reported PCI/UUID binding, only a single-NVIDIA host
        // has a provable CUDA0-to-physical-memory association.
        InferenceBackend::Cuda => backend_ordinal_zero_device(inventory, "nvidia"),
        // ROCm/HIP enumeration is not guaranteed to follow DRM card order.
        // Until the worker reports a stable physical identifier, only a
        // single-AMD host has a provable ROCm0-to-VRAM association.
        InferenceBackend::Hip => backend_ordinal_zero_device(inventory, "amd"),
        // Intel Macs commonly expose both an integrated Intel adapter and one
        // discrete Radeon. llama.cpp's Metal backend selects the Radeon on
        // that topology, so bind its resource evidence to the one AMD row.
        // Multiple Radeon rows remain ambiguous without a worker-reported
        // registry identifier and therefore fail closed.
        InferenceBackend::Metal => preferred_metal_device(inventory),
        // Vulkan enumeration may reorder heterogeneous adapters and has no
        // vendor-local invariant. Retain acceleration only on a genuinely
        // single-adapter host rather than borrowing another adapter's memory.
        InferenceBackend::Vulkan => unambiguous_physical_device(inventory),
        InferenceBackend::Cpu => None,
    }?;
    Some((device, device_class(device.vendor.as_deref())))
}

fn macos_x86_64_requires_cpu(inventory: &InferenceHardwareInventory) -> bool {
    matches!(
        inventory.os_family.to_ascii_lowercase().as_str(),
        "macos" | "darwin"
    ) && matches!(
        inventory.os_arch.to_ascii_lowercase().as_str(),
        "x86_64" | "amd64"
    )
}

fn preferred_metal_device(
    inventory: &InferenceHardwareInventory,
) -> Option<&InferenceDeviceMemory> {
    if inventory.os_family.eq_ignore_ascii_case("macos")
        || inventory.os_family.eq_ignore_ascii_case("darwin")
    {
        let mut amd = inventory.device_memory.iter().filter(|device| {
            device
                .vendor
                .as_deref()
                .is_some_and(|vendor| vendor.eq_ignore_ascii_case("amd"))
        });
        let preferred = amd.next();
        if preferred.is_some() && amd.next().is_none() {
            return preferred;
        }
        if preferred.is_some() {
            return None;
        }
    }
    unambiguous_physical_device(inventory)
}

fn backend_ordinal_zero_device<'a>(
    inventory: &'a InferenceHardwareInventory,
    vendor: &str,
) -> Option<&'a InferenceDeviceMemory> {
    let mut devices = inventory
        .device_memory
        .iter()
        .filter(|device| {
            device
                .vendor
                .as_deref()
                .is_some_and(|value| value.eq_ignore_ascii_case(vendor))
        })
        .collect::<Vec<_>>();
    devices.sort_by(|left, right| {
        left.gpu_index
            .cmp(&right.gpu_index)
            .then_with(|| left.device_key.cmp(&right.device_key))
    });
    match devices.as_slice() {
        [only] => Some(*only),
        _ => None,
    }
}

fn unambiguous_physical_device(
    inventory: &InferenceHardwareInventory,
) -> Option<&InferenceDeviceMemory> {
    let first = inventory.device_memory.first()?;
    inventory
        .device_memory
        .iter()
        .all(|device| device.gpu_index == first.gpu_index && device.device_key == first.device_key)
        .then_some(first)
}

fn device_class(vendor: Option<&str>) -> InferenceDeviceClass {
    match vendor.unwrap_or_default().to_ascii_lowercase().as_str() {
        "apple" => InferenceDeviceClass::Apple,
        "nvidia" => InferenceDeviceClass::Nvidia,
        "amd" => InferenceDeviceClass::Amd,
        "intel" => InferenceDeviceClass::Intel,
        _ => InferenceDeviceClass::OtherGpu,
    }
}

fn conservative_gpu_layer_cap(
    device: &InferenceDeviceMemory,
    model: InferenceModelEnvelope,
) -> u32 {
    if !model.is_valid() {
        return 0;
    }
    let memory = device.available_bytes.or(device.total_bytes);
    let Some(memory) = memory else {
        // Unknown memory receives only a small probe. Activation still requires
        // the probe to prove the post-load 1 GiB reserve.
        return model.transformer_layers.min(8);
    };
    if memory <= MIN_DEVICE_MEMORY_RESERVE_BYTES {
        return 0;
    }
    let usable = memory
        .saturating_sub(MIN_DEVICE_MEMORY_RESERVE_BYTES)
        .saturating_mul(3)
        / 4;
    let bytes_per_layer = model
        .model_size_bytes
        .div_ceil(u64::from(model.transformer_layers))
        .max(1);
    let mut layers = u32::try_from(usable / bytes_per_layer)
        .unwrap_or(u32::MAX)
        .min(model.transformer_layers);

    // A 4 GiB discrete GPU must never be given an optimistic full-offload
    // attempt. This is the declared Intel Mac/Radeon baseline.
    if device.total_bytes.is_some_and(|bytes| bytes <= FOUR_GIB) {
        layers = layers.min((model.transformer_layers / 2).max(1));
    }
    if device.available_is_estimate {
        layers = layers.min((model.transformer_layers / 2).max(1));
    }
    layers
}

fn push_candidate(
    candidates: &mut Vec<InferenceRuntimeCandidate>,
    candidate: InferenceRuntimeCandidate,
) {
    if candidates.len() < MAX_RUNTIME_PROFILE_CANDIDATES && !candidates.contains(&candidate) {
        candidates.push(candidate);
    }
}

fn bounded_detail(value: &str) -> String {
    if value.len() <= MAX_PROBE_DETAIL_BYTES {
        return value.to_string();
    }
    let mut boundary = MAX_PROBE_DETAIL_BYTES;
    while boundary > 0 && !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    value[..boundary].to_string()
}

fn numeric_version_is_comparable(value: &str) -> bool {
    let value = value.trim();
    !value.is_empty()
        && value.len() <= 64
        && value.split('.').all(|component| {
            !component.is_empty() && component.chars().all(|ch| ch.is_ascii_digit())
        })
}

fn runtime_cpu_features() -> Vec<String> {
    let mut features = Vec::new();
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        let checks = [
            ("sse2", std::arch::is_x86_feature_detected!("sse2")),
            ("sse3", std::arch::is_x86_feature_detected!("sse3")),
            ("ssse3", std::arch::is_x86_feature_detected!("ssse3")),
            ("sse4.1", std::arch::is_x86_feature_detected!("sse4.1")),
            ("sse4.2", std::arch::is_x86_feature_detected!("sse4.2")),
            ("avx", std::arch::is_x86_feature_detected!("avx")),
            ("avx2", std::arch::is_x86_feature_detected!("avx2")),
            ("fma", std::arch::is_x86_feature_detected!("fma")),
        ];
        features.extend(
            checks
                .into_iter()
                .filter_map(|(name, present)| present.then_some(name.to_string())),
        );
    }
    #[cfg(target_arch = "aarch64")]
    features.push("neon".to_string());
    features.sort();
    features
}

async fn collect_platform_resources(logical_cores: u32) -> PlatformResourceSnapshot {
    #[cfg(target_os = "linux")]
    return collect_linux_resources(logical_cores).await;
    #[cfg(target_os = "macos")]
    return collect_macos_resources(logical_cores).await;
    #[cfg(windows)]
    return collect_windows_resources(logical_cores).await;
    #[allow(unreachable_code)]
    PlatformResourceSnapshot {
        physical_cores: Some(logical_cores),
        model: None,
        os_version: None,
        memory: InferenceSystemMemory {
            total_bytes: None,
            available_bytes: None,
            source: "unavailable".to_string(),
            container_limit_bytes: None,
        },
    }
}

#[cfg(target_os = "linux")]
async fn collect_linux_resources(logical_cores: u32) -> PlatformResourceSnapshot {
    let (memory, cpuinfo, os_release) = tokio::join!(
        collect_linux_memory(),
        fs::read_to_string("/proc/cpuinfo"),
        fs::read_to_string("/proc/sys/kernel/osrelease"),
    );
    let cpuinfo = cpuinfo.unwrap_or_default();
    PlatformResourceSnapshot {
        physical_cores: parse_linux_physical_cores(&cpuinfo).or(Some(logical_cores)),
        model: parse_linux_cpu_model(&cpuinfo),
        os_version: os_release
            .ok()
            .and_then(|value| normalized_numeric_version(&value)),
        memory,
    }
}

#[cfg(target_os = "linux")]
async fn collect_linux_memory() -> InferenceSystemMemory {
    let meminfo = fs::read_to_string("/proc/meminfo")
        .await
        .unwrap_or_default();
    let (total_bytes, host_available_bytes) = parse_linux_meminfo(&meminfo);
    let cgroup = collect_linux_cgroup_memory().await;
    let (effective_total, effective_available, container_limit) =
        effective_memory_with_cgroup(total_bytes, host_available_bytes, cgroup);
    InferenceSystemMemory {
        total_bytes: effective_total,
        available_bytes: effective_available,
        source: if container_limit.is_some() {
            "proc_meminfo+cgroup".to_string()
        } else {
            "proc_meminfo".to_string()
        },
        container_limit_bytes: container_limit,
    }
}

#[cfg(target_os = "macos")]
async fn collect_macos_resources(logical_cores: u32) -> PlatformResourceSnapshot {
    let (physical_cores, model, os_version, memory) = tokio::join!(
        run_bounded_command("sysctl", &["-n", "hw.physicalcpu"]),
        run_bounded_command("sysctl", &["-n", "machdep.cpu.brand_string"]),
        run_bounded_command("sw_vers", &["-productVersion"]),
        collect_macos_memory(),
    );
    PlatformResourceSnapshot {
        physical_cores: physical_cores
            .and_then(|value| value.trim().parse::<u32>().ok())
            .or(Some(logical_cores)),
        model: model
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty()),
        os_version: os_version.and_then(|value| normalized_numeric_version(&value)),
        memory,
    }
}

#[cfg(target_os = "macos")]
async fn collect_macos_memory() -> InferenceSystemMemory {
    let native = collect_current_macos_memory();
    if native.total_bytes.is_some() && native.available_bytes.is_some() {
        return native;
    }
    // This fallback is restricted to the install/startup inventory path. The
    // steady-state admission sampler above never spawns these commands.
    let (total, vm_stat) = tokio::join!(
        run_bounded_command("sysctl", &["-n", "hw.memsize"]),
        run_bounded_command("vm_stat", &[]),
    );
    InferenceSystemMemory {
        total_bytes: native
            .total_bytes
            .or_else(|| total.and_then(|value| value.trim().parse::<u64>().ok())),
        available_bytes: native
            .available_bytes
            .or_else(|| vm_stat.and_then(|value| parse_macos_vm_stat(&value))),
        source: "mach_host_statistics64+sysctl_fallback".to_string(),
        container_limit_bytes: None,
    }
}

#[cfg(target_os = "macos")]
fn collect_current_macos_memory() -> InferenceSystemMemory {
    InferenceSystemMemory {
        total_bytes: macos_total_memory_bytes(),
        available_bytes: macos_available_memory_bytes(),
        source: "mach_host_statistics64".to_string(),
        container_limit_bytes: None,
    }
}

#[cfg(target_os = "macos")]
fn macos_total_memory_bytes() -> Option<u64> {
    use std::{ffi::c_void, mem};

    let mut value = 0_u64;
    let mut size = mem::size_of::<u64>();
    let result = unsafe {
        libc::sysctlbyname(
            b"hw.memsize\0".as_ptr().cast(),
            (&mut value as *mut u64).cast::<c_void>(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    (result == 0 && size == mem::size_of::<u64>()).then_some(value)
}

#[cfg(target_os = "macos")]
fn macos_available_memory_bytes() -> Option<u64> {
    use std::mem;

    let mut statistics = mem::MaybeUninit::<libc::vm_statistics64_data_t>::zeroed();
    let mut count = libc::HOST_VM_INFO64_COUNT;
    // libc marks this compatibility shim deprecated in favor of the optional
    // mach2 crate. Keep the FFI surface dependency-free and confine the lint to
    // this single, non-owning send right.
    #[allow(deprecated)]
    let host = unsafe { libc::mach_host_self() };
    let result = unsafe {
        libc::host_statistics64(
            host,
            libc::HOST_VM_INFO64,
            statistics.as_mut_ptr().cast(),
            &mut count,
        )
    };
    if result != libc::KERN_SUCCESS || count < libc::HOST_VM_INFO64_COUNT {
        return None;
    }
    let statistics = unsafe { statistics.assume_init() };
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    let page_size = u64::try_from(page_size).ok().filter(|value| *value > 0)?;
    let available_pages = u64::from(statistics.free_count)
        .saturating_add(u64::from(statistics.inactive_count))
        .saturating_add(u64::from(statistics.speculative_count));
    Some(available_pages.saturating_mul(page_size))
}

#[cfg(windows)]
async fn collect_windows_resources(logical_cores: u32) -> PlatformResourceSnapshot {
    const SCRIPT: &str = "$cpu=Get-CimInstance Win32_Processor | Select-Object -First 1 Name,NumberOfCores,NumberOfLogicalProcessors; $os=Get-CimInstance Win32_OperatingSystem | Select-Object Version,TotalVisibleMemorySize,FreePhysicalMemory; [pscustomobject]@{cpu=$cpu;os=$os}|ConvertTo-Json -Compress -Depth 3";
    let value = run_bounded_command(
        "powershell",
        &["-NoProfile", "-NonInteractive", "-Command", SCRIPT],
    )
    .await
    .and_then(|raw| serde_json::from_str::<Value>(&raw).ok());
    let cpu = value.as_ref().and_then(|value| value.get("cpu"));
    let os = value.as_ref().and_then(|value| value.get("os"));
    let physical_cores = cpu
        .and_then(|value| value.get("NumberOfCores"))
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .or(Some(logical_cores));
    let model = cpu
        .and_then(|value| value.get("Name"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let total_bytes = os
        .and_then(|value| value.get("TotalVisibleMemorySize"))
        .and_then(Value::as_u64)
        .and_then(|value| value.checked_mul(1024));
    let available_bytes = os
        .and_then(|value| value.get("FreePhysicalMemory"))
        .and_then(Value::as_u64)
        .and_then(|value| value.checked_mul(1024));
    PlatformResourceSnapshot {
        physical_cores,
        model,
        os_version: os
            .and_then(|value| value.get("Version"))
            .and_then(Value::as_str)
            .and_then(normalized_numeric_version),
        memory: InferenceSystemMemory {
            total_bytes,
            available_bytes,
            source: "windows_cim".to_string(),
            container_limit_bytes: None,
        },
    }
}

#[cfg(windows)]
fn collect_windows_memory() -> InferenceSystemMemory {
    use std::mem;

    #[repr(C)]
    struct MemoryStatusEx {
        length: u32,
        memory_load: u32,
        total_physical: u64,
        available_physical: u64,
        total_page_file: u64,
        available_page_file: u64,
        total_virtual: u64,
        available_virtual: u64,
        available_extended_virtual: u64,
    }

    #[link(name = "kernel32")]
    unsafe extern "system" {
        #[link_name = "GlobalMemoryStatusEx"]
        fn global_memory_status_ex(status: *mut MemoryStatusEx) -> i32;
    }

    let mut status = MemoryStatusEx {
        length: u32::try_from(mem::size_of::<MemoryStatusEx>()).unwrap_or(u32::MAX),
        memory_load: 0,
        total_physical: 0,
        available_physical: 0,
        total_page_file: 0,
        available_page_file: 0,
        total_virtual: 0,
        available_virtual: 0,
        available_extended_virtual: 0,
    };
    let ok = unsafe { global_memory_status_ex(&mut status) } != 0;
    InferenceSystemMemory {
        total_bytes: ok.then_some(status.total_physical),
        available_bytes: ok.then_some(status.available_physical),
        source: "global_memory_status_ex".to_string(),
        container_limit_bytes: None,
    }
}

async fn run_bounded_command(program: &str, args: &[&str]) -> Option<String> {
    timeout(
        RESOURCE_COMMAND_TIMEOUT,
        Command::new(program)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output(),
    )
    .await
    .ok()?
    .ok()
    .filter(|output| output.status.success())
    .map(|output| String::from_utf8_lossy(&output.stdout).to_string())
}

#[cfg(any(target_os = "linux", test))]
fn parse_linux_meminfo(raw: &str) -> (Option<u64>, Option<u64>) {
    let values = raw
        .lines()
        .filter_map(|line| {
            let (key, value) = line.split_once(':')?;
            let kib = value.split_whitespace().next()?.parse::<u64>().ok()?;
            Some((key.trim(), kib.saturating_mul(1024)))
        })
        .collect::<BTreeMap<_, _>>();
    (
        values.get("MemTotal").copied(),
        values
            .get("MemAvailable")
            .copied()
            .or_else(|| values.get("MemFree").copied()),
    )
}

#[cfg(any(target_os = "linux", test))]
fn parse_linux_physical_cores(raw: &str) -> Option<u32> {
    let mut cores = BTreeSet::new();
    for block in raw.split("\n\n") {
        let values = block
            .lines()
            .filter_map(|line| line.split_once(':'))
            .map(|(key, value)| (key.trim(), value.trim()))
            .collect::<BTreeMap<_, _>>();
        if let (Some(physical), Some(core)) = (values.get("physical id"), values.get("core id")) {
            cores.insert(((*physical).to_string(), (*core).to_string()));
        }
    }
    (!cores.is_empty()).then(|| u32::try_from(cores.len()).unwrap_or(u32::MAX))
}

#[cfg(any(target_os = "linux", test))]
fn parse_linux_cpu_model(raw: &str) -> Option<String> {
    raw.lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(key, _)| matches!(key.trim(), "model name" | "Hardware" | "Processor"))
        .map(|(_, value)| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn normalized_numeric_version(raw: &str) -> Option<String> {
    let value = raw
        .trim()
        .chars()
        .take_while(|character| character.is_ascii_digit() || *character == '.')
        .collect::<String>();
    let value = value.trim_matches('.');
    (!value.is_empty()
        && value.len() <= 64
        && value.split('.').all(|component| {
            !component.is_empty() && component.chars().all(|c| c.is_ascii_digit())
        }))
    .then(|| value.to_string())
}

fn parse_macos_vm_stat(raw: &str) -> Option<u64> {
    let page_size = raw
        .lines()
        .next()?
        .split_whitespace()
        .find_map(|token| token.parse::<u64>().ok())?;
    let mut pages = 0_u64;
    for line in raw.lines().skip(1) {
        let (key, value) = line.split_once(':')?;
        if matches!(
            key.trim(),
            "Pages free" | "Pages inactive" | "Pages speculative"
        ) {
            let count = value.trim().trim_end_matches('.').parse::<u64>().ok()?;
            pages = pages.saturating_add(count);
        }
    }
    Some(pages.saturating_mul(page_size))
}

#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CgroupMemory {
    limit_bytes: u64,
    current_bytes: u64,
}

#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, PartialEq, Eq)]
struct CgroupMemoryPaths {
    limit: PathBuf,
    current: PathBuf,
}

#[cfg(any(target_os = "linux", test))]
#[derive(Debug)]
struct CgroupMembership<'a> {
    controllers: Vec<&'a str>,
    path: &'a str,
}

#[cfg(any(target_os = "linux", test))]
#[derive(Debug)]
struct CgroupMount {
    root: PathBuf,
    mount_point: PathBuf,
    unified: bool,
    memory_controller: bool,
}

#[cfg(target_os = "linux")]
async fn collect_linux_cgroup_memory() -> Option<CgroupMemory> {
    let (cgroup, mountinfo) = tokio::join!(
        fs::read_to_string("/proc/self/cgroup"),
        fs::read_to_string("/proc/self/mountinfo"),
    );
    if let (Ok(cgroup), Ok(mountinfo)) = (cgroup, mountinfo)
        && let Some(paths) = resolve_cgroup_memory_paths(&cgroup, &mountinfo)
        && let Some(memory) = read_cgroup_memory(&paths).await
    {
        return Some(memory);
    }

    // Compatibility fallback for old/container-minimal mounts where mountinfo
    // itself is hidden. The membership-aware path above is authoritative.
    for paths in [
        CgroupMemoryPaths {
            limit: PathBuf::from("/sys/fs/cgroup/memory.max"),
            current: PathBuf::from("/sys/fs/cgroup/memory.current"),
        },
        CgroupMemoryPaths {
            limit: PathBuf::from("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
            current: PathBuf::from("/sys/fs/cgroup/memory/memory.usage_in_bytes"),
        },
    ] {
        if let Some(memory) = read_cgroup_memory(&paths).await {
            return Some(memory);
        }
    }
    None
}

#[cfg(target_os = "linux")]
async fn read_cgroup_memory(paths: &CgroupMemoryPaths) -> Option<CgroupMemory> {
    let (limit, current) = tokio::join!(
        read_numeric_path(&paths.limit),
        read_numeric_path(&paths.current)
    );
    limit
        .zip(current)
        .map(|(limit_bytes, current_bytes)| CgroupMemory {
            limit_bytes,
            current_bytes,
        })
}

#[cfg(target_os = "linux")]
async fn read_numeric_path(path: &Path) -> Option<u64> {
    fs::read_to_string(path)
        .await
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
}

#[cfg(any(target_os = "linux", test))]
fn resolve_cgroup_memory_paths(cgroup: &str, mountinfo: &str) -> Option<CgroupMemoryPaths> {
    let memberships = parse_cgroup_memberships(cgroup);
    let mounts = parse_cgroup_mounts(mountinfo);

    for unified in [true, false] {
        let Some(membership) = memberships.iter().find(|membership| {
            if unified {
                membership.controllers.is_empty()
            } else {
                membership.controllers.contains(&"memory")
            }
        }) else {
            continue;
        };
        for mount in mounts
            .iter()
            .filter(|mount| mount.unified == unified && (unified || mount.memory_controller))
        {
            let Some(base) = resolve_cgroup_mount_path(
                &mount.root,
                &mount.mount_point,
                Path::new(membership.path),
            ) else {
                continue;
            };
            return Some(if unified {
                CgroupMemoryPaths {
                    limit: base.join("memory.max"),
                    current: base.join("memory.current"),
                }
            } else {
                CgroupMemoryPaths {
                    limit: base.join("memory.limit_in_bytes"),
                    current: base.join("memory.usage_in_bytes"),
                }
            });
        }
    }
    None
}

#[cfg(any(target_os = "linux", test))]
fn parse_cgroup_memberships(raw: &str) -> Vec<CgroupMembership<'_>> {
    raw.lines()
        .filter_map(|line| {
            let mut fields = line.splitn(3, ':');
            fields.next()?;
            let controllers = fields.next()?;
            let path = fields.next()?.trim();
            if !path.starts_with('/') {
                return None;
            }
            Some(CgroupMembership {
                controllers: controllers
                    .split(',')
                    .filter(|controller| !controller.is_empty())
                    .collect(),
                path,
            })
        })
        .collect()
}

#[cfg(any(target_os = "linux", test))]
fn parse_cgroup_mounts(raw: &str) -> Vec<CgroupMount> {
    raw.lines()
        .filter_map(|line| {
            let (before, after) = line.split_once(" - ")?;
            let fields = before.split_whitespace().collect::<Vec<_>>();
            let after = after.split_whitespace().collect::<Vec<_>>();
            if fields.len() < 6 || after.len() < 3 {
                return None;
            }
            let fs_type = after[0];
            if !matches!(fs_type, "cgroup" | "cgroup2") {
                return None;
            }
            let root = decode_mountinfo_path(fields[3])?;
            let mount_point = decode_mountinfo_path(fields[4])?;
            let memory_controller = fs_type == "cgroup"
                && after[2]
                    .split(',')
                    .chain(fields[5].split(','))
                    .any(|option| option == "memory");
            Some(CgroupMount {
                root,
                mount_point,
                unified: fs_type == "cgroup2",
                memory_controller,
            })
        })
        .collect()
}

#[cfg(any(target_os = "linux", test))]
fn decode_mountinfo_path(raw: &str) -> Option<PathBuf> {
    let bytes = raw.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\\' {
            let octal = bytes.get(index + 1..index + 4)?;
            if !octal.iter().all(|byte| matches!(byte, b'0'..=b'7')) {
                return None;
            }
            let value = u16::from(octal[0] - b'0') * 64
                + u16::from(octal[1] - b'0') * 8
                + u16::from(octal[2] - b'0');
            decoded.push(u8::try_from(value).ok()?);
            index += 4;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    let decoded = String::from_utf8(decoded).ok()?;
    normalized_absolute_path(Path::new(&decoded))
}

#[cfg(any(target_os = "linux", test))]
fn normalized_absolute_path(path: &Path) -> Option<PathBuf> {
    if !path.is_absolute() {
        return None;
    }
    let mut normalized = PathBuf::from("/");
    for component in path.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(component) => normalized.push(component),
            Component::CurDir => {}
            Component::ParentDir | Component::Prefix(_) => return None,
        }
    }
    Some(normalized)
}

#[cfg(any(target_os = "linux", test))]
fn resolve_cgroup_mount_path(root: &Path, mount_point: &Path, member: &Path) -> Option<PathBuf> {
    let root = normalized_absolute_path(root)?;
    let mount_point = normalized_absolute_path(mount_point)?;
    let member = normalized_absolute_path(member)?;
    let relative = member.strip_prefix(&root).ok()?;
    Some(mount_point.join(relative))
}

#[cfg(any(target_os = "linux", test))]
fn effective_memory_with_cgroup(
    host_total: Option<u64>,
    host_available: Option<u64>,
    cgroup: Option<CgroupMemory>,
) -> (Option<u64>, Option<u64>, Option<u64>) {
    let Some(cgroup) = cgroup else {
        return (host_total, host_available, None);
    };
    // Kernel v1 often represents "unlimited" with a huge page-aligned value.
    if cgroup.limit_bytes >= (1_u64 << 60)
        || host_total.is_some_and(|total| cgroup.limit_bytes >= total)
    {
        return (host_total, host_available, None);
    }
    let cgroup_available = cgroup.limit_bytes.saturating_sub(cgroup.current_bytes);
    (
        Some(
            host_total
                .map(|total| total.min(cgroup.limit_bytes))
                .unwrap_or(cgroup.limit_bytes),
        ),
        Some(
            host_available
                .map(|available| available.min(cgroup_available))
                .unwrap_or(cgroup_available),
        ),
        Some(cgroup.limit_bytes),
    )
}

async fn collect_device_memory(
    host: &HostHardwareInventory,
    memory: &InferenceSystemMemory,
) -> Vec<InferenceDeviceMemory> {
    let mut evidence = host
        .gpus
        .iter()
        .enumerate()
        .map(|(index, gpu)| device_memory_from_host(index, gpu, memory))
        .collect::<Vec<_>>();
    #[cfg(target_os = "linux")]
    discover_linux_exposed_devices(&mut evidence).await;
    apply_nvidia_memory_evidence(host, &mut evidence).await;
    #[cfg(target_os = "linux")]
    apply_linux_sysfs_memory_evidence(&mut evidence).await;
    #[cfg(windows)]
    apply_windows_memory_evidence(&mut evidence).await;
    ensure_apple_unified_memory_device(&mut evidence, host, memory);
    evidence
}

#[cfg(target_os = "linux")]
async fn discover_linux_exposed_devices(evidence: &mut Vec<InferenceDeviceMemory>) {
    let Ok(mut entries) = fs::read_dir("/dev/dri").await else {
        return;
    };
    let mut discovered = Vec::new();
    while let Ok(Some(entry)) = entries.next_entry().await {
        let render_node = entry.file_name().to_string_lossy().to_string();
        if !render_node.starts_with("renderD") || !device_node_is_accessible(&entry.path()) {
            continue;
        }
        let root = Path::new("/sys/class/drm")
            .join(&render_node)
            .join("device");
        let Some(vendor) = fs::read_to_string(root.join("vendor"))
            .await
            .ok()
            .and_then(|value| linux_pci_vendor_name(&value).map(str::to_string))
        else {
            // A render node alone does not identify Intel versus AMD versus a
            // third-party Vulkan implementation. /dev/kfd gates known AMD HIP
            // devices below, but is not used to guess which render node it
            // belongs to on a hybrid host.
            continue;
        };
        let device_id = fs::read_to_string(root.join("device"))
            .await
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let total = read_u64_path(&root.join("mem_info_vram_total")).await;
        let used = read_u64_path(&root.join("mem_info_vram_used")).await;
        discovered.push((render_node, vendor, device_id, total, used));
    }

    merge_linux_exposed_device_rows(evidence, discovered);
}

#[cfg(any(target_os = "linux", test))]
fn linux_pci_vendor_name(raw: &str) -> Option<&'static str> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "0x1002" | "0x1022" => Some("amd"),
        "0x8086" => Some("intel"),
        "0x10de" => Some("nvidia"),
        _ => None,
    }
}

#[cfg(any(target_os = "linux", test))]
fn merge_linux_exposed_device_rows(
    evidence: &mut Vec<InferenceDeviceMemory>,
    rows: Vec<(String, String, Option<String>, Option<u64>, Option<u64>)>,
) {
    let existing_vendors = evidence
        .iter()
        .filter_map(|device| device.vendor.clone())
        .collect::<BTreeSet<_>>();
    for (render_node, vendor, device_id, total, used) in rows {
        if existing_vendors.contains(&vendor) {
            continue;
        }
        let gpu_index = evidence.len();
        let available = total.map(|total| {
            used.map(|used| total.saturating_sub(used))
                .unwrap_or_else(|| total.saturating_sub(MIN_DEVICE_MEMORY_RESERVE_BYTES))
        });
        evidence.push(InferenceDeviceMemory {
            device_key: format!(
                "linux-drm-{render_node}-{}",
                device_id.as_deref().unwrap_or("unknown")
            ),
            gpu_index,
            vendor: Some(vendor),
            model: device_id.map(|device| format!("PCI device {device}")),
            total_bytes: total,
            available_bytes: available,
            available_is_estimate: used.is_none() && total.is_some(),
            source: if total.is_some() {
                DeviceMemoryEvidenceSource::LinuxSysfs
            } else {
                DeviceMemoryEvidenceSource::Unknown
            },
        });
    }
}

fn ensure_apple_unified_memory_device(
    evidence: &mut Vec<InferenceDeviceMemory>,
    host: &HostHardwareInventory,
    memory: &InferenceSystemMemory,
) {
    let apple_silicon = host.os.family.eq_ignore_ascii_case("macos")
        && matches!(
            host.os.arch.to_ascii_lowercase().as_str(),
            "aarch64" | "arm64"
        );
    if !apple_silicon || !evidence.is_empty() {
        return;
    }
    evidence.push(InferenceDeviceMemory {
        device_key: "apple-unified-0".to_string(),
        gpu_index: 0,
        vendor: Some("apple".to_string()),
        model: Some("Apple integrated GPU".to_string()),
        total_bytes: memory.total_bytes,
        available_bytes: memory.available_bytes,
        available_is_estimate: false,
        source: DeviceMemoryEvidenceSource::MacosUnifiedMemory,
    });
}

fn device_memory_from_host(
    index: usize,
    gpu: &HostGpuInventory,
    memory: &InferenceSystemMemory,
) -> InferenceDeviceMemory {
    let vendor = gpu.vendor.as_deref().map(str::to_ascii_lowercase);
    let mut total_bytes = find_memory_bytes_in_value(&gpu.raw);
    // Some cross-platform inventory APIs expose capacity but not free VRAM.
    // Keep a deliberately reduced estimate so a successful smoke probe can
    // still qualify conservative partial offload. Layer fitting applies its
    // own reserve again whenever `available_is_estimate` is true.
    let mut available_bytes =
        total_bytes.map(|total| total.saturating_sub(MIN_DEVICE_MEMORY_RESERVE_BYTES));
    let mut available_is_estimate = total_bytes.is_some();
    let source =
        if cfg!(target_os = "macos") && vendor.as_deref() == Some("apple") && total_bytes.is_none()
        {
            total_bytes = memory.total_bytes;
            available_bytes = memory.available_bytes;
            available_is_estimate = false;
            DeviceMemoryEvidenceSource::MacosUnifiedMemory
        } else if cfg!(target_os = "macos") && total_bytes.is_some() {
            DeviceMemoryEvidenceSource::MacosSystemProfiler
        } else if cfg!(windows) && total_bytes.is_some() {
            DeviceMemoryEvidenceSource::WindowsCim
        } else if total_bytes.is_some() {
            DeviceMemoryEvidenceSource::HostInventory
        } else {
            DeviceMemoryEvidenceSource::Unknown
        };
    InferenceDeviceMemory {
        device_key: stable_device_key(index, gpu),
        gpu_index: index,
        vendor,
        model: gpu.model.clone(),
        total_bytes,
        available_bytes,
        available_is_estimate,
        source,
    }
}

async fn apply_nvidia_memory_evidence(
    host: &HostHardwareInventory,
    evidence: &mut [InferenceDeviceMemory],
) {
    let Some(raw) = run_bounded_command(
        "nvidia-smi",
        &[
            "--query-gpu=memory.total,memory.free",
            "--format=csv,noheader,nounits",
        ],
    )
    .await
    else {
        return;
    };
    apply_nvidia_memory_rows(host, evidence, &parse_nvidia_memory_rows(&raw));
}

fn apply_nvidia_memory_rows(
    host: &HostHardwareInventory,
    evidence: &mut [InferenceDeviceMemory],
    rows: &[(u64, u64)],
) {
    let ordered_indices = host
        .gpus
        .iter()
        .enumerate()
        .filter(|(_, gpu)| gpu.vendor.as_deref() == Some("nvidia"))
        .filter_map(|(index, gpu)| host_gpu_has_nvidia_smi_ordinal(gpu).then_some(index))
        .collect::<Vec<_>>();
    let nvidia_count = host
        .gpus
        .iter()
        .filter(|gpu| gpu.vendor.as_deref() == Some("nvidia"))
        .count();
    // Both nvidia-smi queries use backend ordinal order. Refuse a partial or
    // cross-source zip: hot-plug races and platform-only inventory order must
    // never attach one GPU's free-memory value to another GPU's profile.
    if ordered_indices.len() != nvidia_count || ordered_indices.len() != rows.len() {
        return;
    }
    if ordered_indices.iter().any(|index| {
        evidence.get(*index).is_none_or(|device| {
            device.gpu_index != *index || device.vendor.as_deref() != Some("nvidia")
        })
    }) {
        return;
    }
    for (index, &(total_bytes, available_bytes)) in ordered_indices.into_iter().zip(rows.iter()) {
        let device = &mut evidence[index];
        device.total_bytes = Some(total_bytes);
        device.available_bytes = Some(available_bytes);
        device.available_is_estimate = false;
        device.source = DeviceMemoryEvidenceSource::NvidiaSmi;
    }
}

fn host_gpu_has_nvidia_smi_ordinal(gpu: &HostGpuInventory) -> bool {
    gpu.raw
        .as_object()
        .is_some_and(|raw| raw.contains_key("nvidia_smi"))
}

#[cfg(target_os = "linux")]
async fn apply_linux_sysfs_memory_evidence(evidence: &mut [InferenceDeviceMemory]) {
    let Ok(mut entries) = fs::read_dir("/sys/class/drm").await else {
        return;
    };
    let mut rows = Vec::new();
    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.starts_with("card") || name.contains('-') {
            continue;
        }
        let root = entry.path().join("device");
        let vendor = fs::read_to_string(root.join("vendor"))
            .await
            .ok()
            .and_then(|value| linux_pci_vendor_name(&value).map(str::to_string));
        let total = read_u64_path(&root.join("mem_info_vram_total")).await;
        let used = read_u64_path(&root.join("mem_info_vram_used")).await;
        if let (Some(vendor), Some(total)) = (vendor, total) {
            rows.push((vendor, total, used.map(|used| total.saturating_sub(used))));
        }
    }
    for (vendor, total, available) in rows {
        if let Some(device) = evidence.iter_mut().find(|device| {
            device.vendor.as_deref() == Some(vendor.as_str())
                && device.source != DeviceMemoryEvidenceSource::NvidiaSmi
                && device.source != DeviceMemoryEvidenceSource::LinuxSysfs
        }) {
            device.total_bytes = Some(total);
            device.available_bytes =
                available.or_else(|| Some(total.saturating_sub(MIN_DEVICE_MEMORY_RESERVE_BYTES)));
            device.available_is_estimate = available.is_none();
            device.source = DeviceMemoryEvidenceSource::LinuxSysfs;
        }
    }
}

#[cfg(target_os = "linux")]
async fn read_u64_path(path: &Path) -> Option<u64> {
    fs::read_to_string(path).await.ok()?.trim().parse().ok()
}

#[cfg(windows)]
async fn apply_windows_memory_evidence(evidence: &mut [InferenceDeviceMemory]) {
    const SCRIPT: &str = "Get-CimInstance Win32_VideoController | Select-Object Name,AdapterRAM | ConvertTo-Json -Compress";
    let Some(raw) = run_bounded_command(
        "powershell",
        &["-NoProfile", "-NonInteractive", "-Command", SCRIPT],
    )
    .await
    else {
        return;
    };
    let Ok(value) = serde_json::from_str::<Value>(&raw) else {
        return;
    };
    let rows = match value {
        Value::Array(rows) => rows,
        row => vec![row],
    };
    let mut updated = BTreeSet::new();
    for row in rows {
        let name = row.get("Name").and_then(Value::as_str);
        let total = row.get("AdapterRAM").and_then(Value::as_u64);
        let Some(total) = total else { continue };
        if let Some(device) = evidence.iter_mut().find(|device| {
            device.source != DeviceMemoryEvidenceSource::NvidiaSmi
                && !updated.contains(&device.device_key)
                && name
                    .zip(device.model.as_deref())
                    .is_some_and(|(left, right)| left.eq_ignore_ascii_case(right))
        }) {
            updated.insert(device.device_key.clone());
            device.total_bytes = Some(total);
            device.available_bytes = Some(total.saturating_sub(MIN_DEVICE_MEMORY_RESERVE_BYTES));
            device.available_is_estimate = true;
            device.source = DeviceMemoryEvidenceSource::WindowsCim;
        }
    }
}

fn parse_nvidia_memory_rows(raw: &str) -> Vec<(u64, u64)> {
    raw.lines()
        .filter_map(|line| {
            let mut parts = line.split(',').map(str::trim);
            let total = parts.next()?.parse::<u64>().ok()?.checked_mul(MIB)?;
            let free = parts.next()?.parse::<u64>().ok()?.checked_mul(MIB)?;
            Some((total, free))
        })
        .collect()
}

fn find_memory_bytes_in_value(value: &Value) -> Option<u64> {
    const KEYS: [&str; 6] = [
        "AdapterRAM",
        "spdisplays_vram",
        "spdisplays_vram_shared",
        "VRAM",
        "vram",
        "memory.total",
    ];
    match value {
        Value::Object(object) => {
            for key in KEYS {
                if let Some(bytes) = object.get(key).and_then(memory_value_bytes) {
                    return Some(bytes);
                }
            }
            object.values().find_map(find_memory_bytes_in_value)
        }
        Value::Array(values) => values.iter().find_map(find_memory_bytes_in_value),
        _ => None,
    }
}

fn memory_value_bytes(value: &Value) -> Option<u64> {
    if let Some(bytes) = value.as_u64() {
        return Some(bytes);
    }
    let raw = value.as_str()?.trim().to_ascii_lowercase();
    let number = raw
        .split_whitespace()
        .find_map(|token| token.replace(',', "").parse::<f64>().ok())?;
    let multiplier = if raw.contains("tb") {
        1024_f64.powi(4)
    } else if raw.contains("gb") {
        1024_f64.powi(3)
    } else if raw.contains("mb") {
        1024_f64.powi(2)
    } else if raw.contains("kb") {
        1024_f64
    } else {
        1.0
    };
    let bytes = number * multiplier;
    (bytes.is_finite() && bytes >= 0.0 && bytes <= u64::MAX as f64).then_some(bytes as u64)
}

fn stable_device_key(index: usize, gpu: &HostGpuInventory) -> String {
    let payload = json!({
        "index": index,
        "vendor": gpu.vendor,
        "model": gpu.model,
        "deviceId": gpu.device_id,
    });
    let digest = Sha256::digest(serde_json::to_vec(&payload).unwrap_or_default());
    let short_hash = digest[..6]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("gpu-{index}-{short_hash}")
}

fn detect_container_inventory() -> InferenceContainerInventory {
    let cgroup = std::fs::read_to_string("/proc/1/cgroup")
        .unwrap_or_default()
        .to_ascii_lowercase();
    let kubernetes =
        std::env::var_os("KUBERNETES_SERVICE_HOST").is_some() || cgroup.contains("kubepods");
    let declared = std::env::var("container").ok();
    let docker_env = Path::new("/.dockerenv").exists() || cgroup.contains("/docker/");
    let podman_env = Path::new("/run/.containerenv").exists()
        || cgroup.contains("libpod")
        || cgroup.contains("podman");
    let other_container = std::env::var_os("CONTAINER_SANDBOX_MOUNT_POINT").is_some()
        || cgroup.contains("containerd")
        || cgroup.contains("/lxc/");
    classify_container_markers(
        kubernetes,
        declared
            .as_deref()
            .or_else(|| other_container.then_some("other")),
        docker_env,
        podman_env,
    )
}

fn classify_container_markers(
    kubernetes: bool,
    declared: Option<&str>,
    docker_env: bool,
    podman_env: bool,
) -> InferenceContainerInventory {
    let kind = if kubernetes {
        InferenceContainerKind::Kubernetes
    } else if declared.is_some_and(|value| value.eq_ignore_ascii_case("podman")) || podman_env {
        InferenceContainerKind::Podman
    } else if declared.is_some_and(|value| value.eq_ignore_ascii_case("docker")) || docker_env {
        InferenceContainerKind::Docker
    } else if declared.is_some() {
        InferenceContainerKind::Other
    } else {
        InferenceContainerKind::None
    };
    InferenceContainerInventory {
        detected: kind != InferenceContainerKind::None,
        kind,
    }
}

#[cfg(test)]
mod tests {
    use super::super::bundle::{
        ANIME_BUNDLE_SCHEMA_VERSION, ANIME_INFERENCE_PROTOCOL_VERSION,
        ANIME_MATCHER_SCHEMA_VERSION, AnimeBundleCompatibilityPolicy, AnimeDeviceClass,
        AnimeInferenceBundleManifest, AnimeModelArtifactManifest, AnimeModelFormat,
        AnimeRuntimeArchiveFormat, AnimeRuntimeArtifactManifest, AnimeRuntimePolicyManifest,
        AnimeThinkingMode, validate_anime_bundle,
    };
    use super::*;
    use crate::playback::hardware::{FfmpegHardwareInventory, HostOsInventory};
    use anyhow::Result;
    use std::{collections::VecDeque, sync::Mutex};

    fn gib(value: u64) -> u64 {
        value * 1024 * 1024 * 1024
    }

    fn host(os: &str, arch: &str, gpus: Vec<HostGpuInventory>) -> HostHardwareInventory {
        HostHardwareInventory {
            os: HostOsInventory {
                family: os.to_string(),
                version: Some("test".to_string()),
                arch: arch.to_string(),
            },
            gpus,
            ffmpeg: FfmpegHardwareInventory {
                path: None,
                version: None,
                sha256: None,
                hwaccels: Vec::new(),
                encoders: Vec::new(),
                decoders: Vec::new(),
            },
        }
    }

    fn gpu(vendor: &str, model: &str, total_bytes: Option<u64>) -> HostGpuInventory {
        HostGpuInventory {
            vendor: Some(vendor.to_string()),
            model: Some(model.to_string()),
            device_id: Some(format!("{vendor}-device")),
            driver_version: Some("1".to_string()),
            raw: total_bytes
                .map(|bytes| json!({"AdapterRAM": bytes}))
                .unwrap_or(Value::Null),
        }
    }

    fn inventory(
        os: &str,
        arch: &str,
        physical_cores: u32,
        available_memory: u64,
        devices: Vec<InferenceDeviceMemory>,
    ) -> InferenceHardwareInventory {
        InferenceHardwareInventory {
            schema_version: INFERENCE_HARDWARE_SCHEMA_VERSION,
            host_fingerprint: "sha256:host".to_string(),
            os_family: os.to_string(),
            os_arch: arch.to_string(),
            os_version: Some("14.0".to_string()),
            cpu: InferenceCpuInventory {
                logical_cores: physical_cores.saturating_mul(2).max(1),
                physical_cores,
                model: Some("test cpu".to_string()),
                features: vec!["sse2".to_string()],
            },
            memory: InferenceSystemMemory {
                total_bytes: Some(gib(16)),
                available_bytes: Some(available_memory),
                source: "test".to_string(),
                container_limit_bytes: None,
            },
            device_memory: devices,
            container: InferenceContainerInventory {
                detected: false,
                kind: InferenceContainerKind::None,
            },
            collected_at: Utc::now(),
        }
    }

    fn device(vendor: &str, total: Option<u64>, available: Option<u64>) -> InferenceDeviceMemory {
        InferenceDeviceMemory {
            device_key: format!("gpu-{vendor}"),
            gpu_index: 0,
            vendor: Some(vendor.to_string()),
            model: Some(format!("{vendor} gpu")),
            total_bytes: total,
            available_bytes: available,
            available_is_estimate: available.is_none() && total.is_some(),
            source: DeviceMemoryEvidenceSource::HostInventory,
        }
    }

    fn policy(backends: &[InferenceBackend]) -> RuntimeProfilePolicy {
        RuntimeProfilePolicy {
            certified_backends: backends.iter().copied().collect(),
            model: InferenceModelEnvelope {
                model_size_bytes: 2_500_000_000,
                transformer_layers: 36,
            },
        }
    }

    fn passing_measurement() -> InferenceProbeMeasurement {
        InferenceProbeMeasurement {
            worker_ready: true,
            smoke_match_passed: true,
            load_time_ms: 4_000,
            warm_latency_ms: 400,
            peak_rss_bytes: gib(3),
            peak_device_memory_bytes: Some(gib(2)),
            system_available_bytes: Some(gib(6)),
            device_available_bytes: Some(gib(2)),
            memory_pressure: false,
        }
    }

    fn identity() -> RuntimeProfileIdentity {
        RuntimeProfileIdentity {
            bundle_version: "2026.08.1".to_string(),
            model_revision: "qwen-r1".to_string(),
            worker_revision: "llama-r1".to_string(),
            runtime_policy_revision: "policy-r1".to_string(),
            kv_cache_type: AnimeKvCacheType::F16,
        }
    }

    fn bridge_runtime(
        os: AnimeHostOs,
        arch: AnimeHostArch,
        backend: AnimeRuntimeBackend,
        device_class: Option<AnimeDeviceClass>,
    ) -> AnimeRuntimeArtifactManifest {
        let hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        AnimeRuntimeArtifactManifest {
            os,
            arch,
            device_class,
            backend,
            priority: match backend {
                AnimeRuntimeBackend::CudaCpu | AnimeRuntimeBackend::HipCpu => 10,
                AnimeRuntimeBackend::MetalCpu => 10,
                AnimeRuntimeBackend::VulkanCpu => 20,
                AnimeRuntimeBackend::Cpu => 100,
            },
            revision: format!("worker-{}", backend.as_str()),
            minimum_os_version: "1.0".to_string(),
            required_cpu_features: Vec::new(),
            minimum_driver_version: None,
            minimum_device_memory_bytes: 0,
            archive_format: AnimeRuntimeArchiveFormat::Raw,
            entrypoint: if os == AnimeHostOs::Windows {
                "llama-server.exe".to_string()
            } else {
                "llama-server".to_string()
            },
            packaged_dependencies: Vec::new(),
            url: format!("https://releases.example/{hash}"),
            sha256: hash.to_string(),
            size_bytes: 3,
            installed_size_bytes: 3,
        }
    }

    fn bridge_bundle() -> ValidatedAnimeBundle {
        let runtimes = [
            (
                AnimeHostOs::Macos,
                AnimeHostArch::Aarch64,
                AnimeRuntimeBackend::MetalCpu,
                None,
            ),
            (
                AnimeHostOs::Macos,
                AnimeHostArch::X86_64,
                AnimeRuntimeBackend::MetalCpu,
                None,
            ),
            (
                AnimeHostOs::Windows,
                AnimeHostArch::X86_64,
                AnimeRuntimeBackend::CudaCpu,
                Some(AnimeDeviceClass::Nvidia),
            ),
            (
                AnimeHostOs::Windows,
                AnimeHostArch::X86_64,
                AnimeRuntimeBackend::VulkanCpu,
                Some(AnimeDeviceClass::AnyVulkan),
            ),
            (
                AnimeHostOs::Windows,
                AnimeHostArch::X86_64,
                AnimeRuntimeBackend::Cpu,
                Some(AnimeDeviceClass::Cpu),
            ),
            (
                AnimeHostOs::Linux,
                AnimeHostArch::X86_64,
                AnimeRuntimeBackend::CudaCpu,
                Some(AnimeDeviceClass::Nvidia),
            ),
            (
                AnimeHostOs::Linux,
                AnimeHostArch::X86_64,
                AnimeRuntimeBackend::HipCpu,
                Some(AnimeDeviceClass::Amd),
            ),
            (
                AnimeHostOs::Linux,
                AnimeHostArch::X86_64,
                AnimeRuntimeBackend::VulkanCpu,
                Some(AnimeDeviceClass::AnyVulkan),
            ),
            (
                AnimeHostOs::Linux,
                AnimeHostArch::X86_64,
                AnimeRuntimeBackend::Cpu,
                Some(AnimeDeviceClass::Cpu),
            ),
            (
                AnimeHostOs::Linux,
                AnimeHostArch::Aarch64,
                AnimeRuntimeBackend::Cpu,
                Some(AnimeDeviceClass::Cpu),
            ),
        ]
        .into_iter()
        .map(|(os, arch, backend, class)| bridge_runtime(os, arch, backend, class))
        .collect();
        let hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let commit = "0123456789abcdef0123456789abcdef01234567";
        let manifest = AnimeInferenceBundleManifest {
            schema_version: ANIME_BUNDLE_SCHEMA_VERSION,
            bundle_version: "2026.08.1".to_string(),
            protocol_version: ANIME_INFERENCE_PROTOCOL_VERSION,
            matcher_schema_version: ANIME_MATCHER_SCHEMA_VERSION,
            minimum_server_version: "0.1.0".to_string(),
            worker_revision: "llama-cpp-b7000".to_string(),
            model: AnimeModelArtifactManifest {
                id: "qwen3-8b".to_string(),
                revision: "elixir-q4km-r1".to_string(),
                upstream_model_id: "Qwen/Qwen3-8B".to_string(),
                upstream_revision: commit.to_string(),
                license: "Apache-2.0".to_string(),
                format: AnimeModelFormat::Gguf,
                quantization: "Q4_K_M".to_string(),
                transformer_layers: 36,
                context_tokens: 4_096,
                max_output_tokens: 256,
                thinking_mode: AnimeThinkingMode::NonThinkingOnly,
                chat_template_revision: "qwen3-8b-elixir-v1".to_string(),
                conversion_tool_revision: commit.to_string(),
                qualification_report_fingerprint: hash.to_string(),
                url: "https://releases.example/model.gguf".to_string(),
                sha256: hash.to_string(),
                size_bytes: 5,
            },
            runtime_policy: AnimeRuntimePolicyManifest {
                sampling_profile_revision: "anime-match-v1".to_string(),
                parallel: 1,
                kv_cache_type: AnimeKvCacheType::F16,
                idle_unload_seconds: 300,
            },
            runtimes,
        };
        validate_anime_bundle(
            manifest,
            &AnimeBundleCompatibilityPolicy::development(semver::Version::new(0, 1, 0)),
        )
        .expect("valid bridge bundle")
    }

    struct FakeProbe {
        results:
            Mutex<VecDeque<std::result::Result<InferenceProbeMeasurement, InferenceProbeError>>>,
    }

    impl FakeProbe {
        fn new(
            results: Vec<std::result::Result<InferenceProbeMeasurement, InferenceProbeError>>,
        ) -> Self {
            Self {
                results: Mutex::new(results.into()),
            }
        }
    }

    #[async_trait]
    impl InferenceEnvelopeProbe for FakeProbe {
        async fn probe(
            &self,
            _candidate: &InferenceRuntimeCandidate,
        ) -> std::result::Result<InferenceProbeMeasurement, InferenceProbeError> {
            self.results
                .lock()
                .expect("fake probe lock")
                .pop_front()
                .unwrap_or_else(|| Err(InferenceProbeError::Runner("missing result".to_string())))
        }
    }

    #[test]
    fn alm6_intel_mac_radeon_is_cpu_only_without_a_metal_probe() {
        let inventory = inventory(
            "macos",
            "x86_64",
            8,
            gib(10),
            vec![device("amd", Some(gib(4)), Some(gib(4)))],
        );
        let candidates = runtime_profile_candidates(
            &inventory,
            &policy(&[InferenceBackend::Metal, InferenceBackend::Cpu]),
        );
        assert_eq!(
            candidates
                .iter()
                .map(|candidate| (
                    candidate.backend,
                    candidate.gpu_layers,
                    candidate.cpu_threads,
                    candidate.batch_threads,
                ))
                .collect::<Vec<_>>(),
            vec![
                (InferenceBackend::Cpu, 0, 4, 8),
                (InferenceBackend::Cpu, 0, 4, 4),
                (InferenceBackend::Cpu, 0, 2, 4),
                (InferenceBackend::Cpu, 0, 2, 2),
            ]
        );
        assert!(runtime_device_memory(&inventory, InferenceBackend::Metal, Some("MTL0")).is_none());
    }

    #[test]
    fn alm9_official_qwen_intel_radeon_never_schedules_metal() {
        let inventory = inventory(
            "macos",
            "x86_64",
            8,
            gib(10),
            vec![device("amd", Some(gib(4)), Some(gib(4)))],
        );
        let candidates = runtime_profile_candidates(
            &inventory,
            &RuntimeProfilePolicy {
                certified_backends: [InferenceBackend::Metal, InferenceBackend::Cpu]
                    .into_iter()
                    .collect(),
                model: InferenceModelEnvelope {
                    model_size_bytes: 1_274_396_608,
                    transformer_layers: 24,
                },
            },
        );

        assert_eq!(
            candidates
                .iter()
                .map(|candidate| (
                    candidate.backend,
                    candidate.gpu_layers,
                    candidate.cpu_threads,
                    candidate.batch_threads,
                ))
                .collect::<Vec<_>>(),
            vec![
                (InferenceBackend::Cpu, 0, 4, 8),
                (InferenceBackend::Cpu, 0, 4, 4),
                (InferenceBackend::Cpu, 0, 2, 4),
                (InferenceBackend::Cpu, 0, 2, 2),
            ]
        );
    }

    #[test]
    fn alm9_intel_mac_cpu_only_rule_accepts_platform_aliases() {
        for (os, arch) in [("macos", "amd64"), ("darwin", "x86_64")] {
            let inventory = inventory(
                os,
                arch,
                8,
                gib(10),
                vec![device("amd", Some(gib(4)), Some(gib(4)))],
            );
            let candidates = runtime_profile_candidates(
                &inventory,
                &policy(&[InferenceBackend::Metal, InferenceBackend::Cpu]),
            );

            assert!(!candidates.is_empty());
            assert!(
                candidates
                    .iter()
                    .all(|candidate| candidate.backend == InferenceBackend::Cpu)
            );
        }
    }

    #[tokio::test]
    async fn alm9_intel_mac_probe_runner_only_receives_cpu_candidates() {
        let inventory = inventory(
            "macos",
            "x86_64",
            8,
            gib(10),
            vec![device("amd", Some(gib(4)), Some(gib(4)))],
        );
        let candidates = runtime_profile_candidates(
            &inventory,
            &policy(&[InferenceBackend::Metal, InferenceBackend::Cpu]),
        );
        let probe = FakeProbe::new(vec![Ok(passing_measurement())]);

        let selected = select_runtime_profile(
            &inventory,
            &identity(),
            &candidates,
            &InferenceProbeLimits::default(),
            &probe,
        )
        .await;

        assert_eq!(selected.outcome, InferenceEnvelopeOutcome::CpuBalanced);
        assert_eq!(selected.attempts.len(), 1);
        assert_eq!(
            selected.attempts[0].candidate.backend,
            InferenceBackend::Cpu
        );
        assert_eq!(
            selected.profile.expect("CPU profile").backend,
            InferenceBackend::Cpu
        );
    }

    #[test]
    fn alm6_apple_silicon_prefers_metal_then_cpu() {
        let inventory = inventory(
            "macos",
            "aarch64",
            8,
            gib(12),
            vec![device("apple", Some(gib(16)), Some(gib(12)))],
        );
        let candidates = runtime_profile_candidates(
            &inventory,
            &policy(&[InferenceBackend::Metal, InferenceBackend::Cpu]),
        );
        assert_eq!(
            candidates.first().map(|value| value.backend),
            Some(InferenceBackend::Metal)
        );
        assert_eq!(
            candidates.last().map(|value| value.backend),
            Some(InferenceBackend::Cpu)
        );
    }

    #[test]
    fn alm6_windows_nvidia_orders_cuda_vulkan_cpu() {
        let inventory = inventory(
            "windows",
            "x86_64",
            8,
            gib(12),
            vec![device("nvidia", Some(gib(8)), Some(gib(7)))],
        );
        let candidates = runtime_profile_candidates(
            &inventory,
            &policy(&[
                InferenceBackend::Cuda,
                InferenceBackend::Vulkan,
                InferenceBackend::Cpu,
            ]),
        );
        let first_by_backend = candidates.iter().map(|candidate| candidate.backend).fold(
            Vec::new(),
            |mut values, backend| {
                if !values.contains(&backend) {
                    values.push(backend);
                }
                values
            },
        );
        assert_eq!(
            first_by_backend,
            vec![
                InferenceBackend::Cuda,
                InferenceBackend::Vulkan,
                InferenceBackend::Cpu
            ]
        );
        assert!(
            candidates
                .iter()
                .filter(|candidate| candidate.backend != InferenceBackend::Cpu)
                .all(|candidate| candidate.batch_threads == candidate.cpu_threads),
            "accelerator candidates stay conservative until a distinct value is explicitly probed"
        );
    }

    #[test]
    fn alm6_linux_amd_orders_hip_vulkan_cpu() {
        let inventory = inventory(
            "linux",
            "x86_64",
            8,
            gib(12),
            vec![device("amd", Some(gib(8)), Some(gib(7)))],
        );
        let candidates = runtime_profile_candidates(
            &inventory,
            &policy(&[
                InferenceBackend::Hip,
                InferenceBackend::Vulkan,
                InferenceBackend::Cpu,
            ]),
        );
        assert_eq!(candidates[0].backend, InferenceBackend::Hip);
        assert!(
            candidates
                .iter()
                .any(|candidate| candidate.backend == InferenceBackend::Vulkan)
        );
        assert_eq!(
            candidates.last().map(|value| value.backend),
            Some(InferenceBackend::Cpu)
        );
    }

    #[test]
    fn alm6_linux_nvidia_orders_cuda_vulkan_cpu() {
        let inventory = inventory(
            "linux",
            "x86_64",
            8,
            gib(12),
            vec![device("nvidia", Some(gib(8)), Some(gib(7)))],
        );
        let candidates = runtime_profile_candidates(
            &inventory,
            &policy(&[
                InferenceBackend::Cuda,
                InferenceBackend::Vulkan,
                InferenceBackend::Cpu,
            ]),
        );
        let first_by_backend = candidates.iter().map(|candidate| candidate.backend).fold(
            Vec::new(),
            |mut values, backend| {
                if !values.contains(&backend) {
                    values.push(backend);
                }
                values
            },
        );
        assert_eq!(
            first_by_backend,
            vec![
                InferenceBackend::Cuda,
                InferenceBackend::Vulkan,
                InferenceBackend::Cpu
            ]
        );
    }

    #[test]
    fn alm6_linux_aarch64_without_gpu_uses_cpu_only() {
        let inventory = inventory("linux", "aarch64", 4, gib(8), Vec::new());
        let candidates = runtime_profile_candidates(
            &inventory,
            &policy(&[InferenceBackend::Vulkan, InferenceBackend::Cpu]),
        );
        assert!(!candidates.is_empty());
        assert!(
            candidates
                .iter()
                .all(|candidate| candidate.backend == InferenceBackend::Cpu)
        );
    }

    #[test]
    fn alm6_windows_amd_uses_vulkan_then_cpu() {
        let inventory = inventory(
            "windows",
            "x86_64",
            8,
            gib(12),
            vec![device("amd", Some(gib(8)), Some(gib(7)))],
        );
        let candidates = runtime_profile_candidates(
            &inventory,
            &policy(&[InferenceBackend::Vulkan, InferenceBackend::Cpu]),
        );
        assert_eq!(candidates[0].backend, InferenceBackend::Vulkan);
        assert_eq!(
            candidates.last().map(|value| value.backend),
            Some(InferenceBackend::Cpu)
        );
    }

    #[test]
    fn alm6_low_system_memory_selects_deterministic_only() {
        let inventory = inventory("linux", "x86_64", 8, gib(3), Vec::new());
        assert!(
            runtime_profile_candidates(&inventory, &policy(&[InferenceBackend::Cpu])).is_empty()
        );
    }

    #[test]
    fn alm6_device_reserve_skips_gpu_but_keeps_cpu() {
        let inventory = inventory(
            "linux",
            "x86_64",
            8,
            gib(8),
            vec![device("nvidia", Some(gib(1)), Some(gib(1)))],
        );
        let candidates = runtime_profile_candidates(
            &inventory,
            &policy(&[InferenceBackend::Cuda, InferenceBackend::Cpu]),
        );
        assert!(
            candidates
                .iter()
                .all(|candidate| candidate.backend == InferenceBackend::Cpu)
        );
    }

    #[test]
    fn alm6_cpu_and_batch_threads_are_bounded_explicit_probe_values() {
        assert_eq!(recommended_cpu_threads(1), 1);
        assert_eq!(recommended_cpu_threads(4), 2);
        assert_eq!(recommended_cpu_threads(8), 4);
        assert_eq!(recommended_cpu_threads(64), 4);
        assert_eq!(optimized_batch_threads(1, 1), 1);
        assert_eq!(optimized_batch_threads(4, 2), 4);
        assert_eq!(optimized_batch_threads(6, 4), 6);
        assert_eq!(optimized_batch_threads(64, 4), 8);
    }

    #[test]
    fn alm6_multiple_same_vendor_gpus_fail_closed_without_physical_binding() -> Result<()> {
        let host = host(
            "linux",
            "x86_64",
            vec![
                gpu("nvidia", "NVIDIA first", Some(gib(4))),
                gpu("nvidia", "NVIDIA larger second", Some(gib(24))),
            ],
        );
        let mut first = device("nvidia", Some(gib(4)), Some(gib(3)));
        first.source = DeviceMemoryEvidenceSource::NvidiaSmi;
        let mut larger_second = device("nvidia", Some(gib(24)), Some(gib(23)));
        larger_second.device_key = "physical-gpu-1".to_string();
        larger_second.gpu_index = 1;
        larger_second.source = DeviceMemoryEvidenceSource::NvidiaSmi;
        let mut inventory = inventory("linux", "x86_64", 8, gib(12), vec![first, larger_second]);
        inventory.host_fingerprint = host_hardware_fingerprint(&host);

        let candidates = runtime_profile_candidates(
            &inventory,
            &policy(&[
                InferenceBackend::Cuda,
                InferenceBackend::Vulkan,
                InferenceBackend::Cpu,
            ]),
        );
        assert!(
            candidates
                .iter()
                .all(|candidate| candidate.backend == InferenceBackend::Cpu)
        );
        assert!(runtime_device_memory(&inventory, InferenceBackend::Cuda, Some("CUDA0")).is_none());
        assert!(runtime_device_memory(&inventory, InferenceBackend::Cuda, Some("CUDA1")).is_none());
        assert!(
            candidates
                .iter()
                .all(|candidate| candidate.backend != InferenceBackend::Vulkan)
        );
        let converted = bundle_inference_host(&host, &inventory)?;
        assert!(converted.devices.is_empty());
        Ok(())
    }

    #[test]
    fn alm9_intel_mac_dual_gpu_never_exposes_or_schedules_metal() -> Result<()> {
        let host = host(
            "macos",
            "x86_64",
            vec![
                gpu("intel", "Intel UHD Graphics 630", Some(gib(2))),
                gpu("amd", "AMD Radeon Pro 5500M", Some(gib(4))),
            ],
        );
        let mut integrated = device("intel", Some(gib(2)), Some(gib(1)));
        integrated.device_key = "spdisplays-0-intel-uhd-630".to_string();
        integrated.source = DeviceMemoryEvidenceSource::MacosSystemProfiler;
        let mut radeon = device("amd", Some(gib(4)), Some(gib(3)));
        radeon.device_key = "spdisplays-1-radeon-pro-5500m".to_string();
        radeon.gpu_index = 1;
        radeon.source = DeviceMemoryEvidenceSource::MacosSystemProfiler;
        let mut inventory = inventory(
            "macos",
            "x86_64",
            8,
            gib(12),
            vec![integrated, radeon.clone()],
        );
        inventory.host_fingerprint = host_hardware_fingerprint(&host);

        let candidates = runtime_profile_candidates(
            &inventory,
            &policy(&[InferenceBackend::Metal, InferenceBackend::Cpu]),
        );
        assert!(
            candidates
                .iter()
                .all(|candidate| candidate.backend == InferenceBackend::Cpu)
        );
        assert!(runtime_device_memory(&inventory, InferenceBackend::Metal, Some("MTL0")).is_none());

        let converted = bundle_inference_host(&host, &inventory)?;
        assert!(converted.devices.is_empty());
        Ok(())
    }

    #[test]
    fn alm9_intel_mac_multiple_radeons_remain_ambiguous() {
        let mut first = device("amd", Some(gib(4)), Some(gib(3)));
        first.device_key = "spdisplays-0-radeon-a".to_string();
        let mut second = device("amd", Some(gib(8)), Some(gib(7)));
        second.device_key = "spdisplays-1-radeon-b".to_string();
        second.gpu_index = 1;
        let inventory = inventory("macos", "x86_64", 8, gib(12), vec![first, second]);

        assert!(
            runtime_profile_candidates(
                &inventory,
                &policy(&[InferenceBackend::Metal, InferenceBackend::Cpu]),
            )
            .iter()
            .all(|candidate| candidate.backend == InferenceBackend::Cpu)
        );
        assert!(runtime_device_memory(&inventory, InferenceBackend::Metal, Some("MTL0")).is_none());
    }

    #[test]
    fn alm6_ambiguous_backend_ordinals_never_borrow_another_gpus_memory() {
        let mut second_nvidia = device("nvidia", Some(gib(24)), Some(gib(23)));
        second_nvidia.gpu_index = 1;
        second_nvidia.device_key = "nvidia-second".to_string();
        let nvidia_inventory = inventory(
            "linux",
            "x86_64",
            8,
            gib(12),
            vec![device("nvidia", Some(gib(4)), Some(gib(3))), second_nvidia],
        );
        assert!(
            runtime_device_memory(&nvidia_inventory, InferenceBackend::Cuda, Some("CUDA0"))
                .is_none()
        );
        assert!(
            runtime_profile_candidates(
                &nvidia_inventory,
                &policy(&[InferenceBackend::Cuda, InferenceBackend::Cpu]),
            )
            .iter()
            .all(|candidate| candidate.backend != InferenceBackend::Cuda)
        );

        let mut second_amd = device("amd", Some(gib(24)), Some(gib(23)));
        second_amd.gpu_index = 1;
        second_amd.device_key = "amd-second".to_string();
        let amd_inventory = inventory(
            "linux",
            "x86_64",
            8,
            gib(12),
            vec![device("amd", Some(gib(4)), Some(gib(3))), second_amd],
        );
        assert!(
            runtime_device_memory(&amd_inventory, InferenceBackend::Hip, Some("ROCm0")).is_none()
        );
    }

    #[test]
    fn alm6_nvidia_memory_rows_require_same_ordered_inventory_source() {
        let mut unordered_host = host(
            "linux",
            "x86_64",
            vec![gpu("nvidia", "first", None), gpu("nvidia", "second", None)],
        );
        let mut evidence = vec![device("nvidia", None, None), {
            let mut value = device("nvidia", None, None);
            value.gpu_index = 1;
            value.device_key = "second".to_string();
            value
        }];
        apply_nvidia_memory_rows(
            &unordered_host,
            &mut evidence,
            &[(gib(4), gib(3)), (gib(24), gib(23))],
        );
        assert!(evidence.iter().all(|device| device.total_bytes.is_none()));

        for gpu in &mut unordered_host.gpus {
            gpu.raw = json!({"nvidia_smi": "ordered"});
        }
        apply_nvidia_memory_rows(
            &unordered_host,
            &mut evidence,
            &[(gib(4), gib(3)), (gib(24), gib(23))],
        );
        assert_eq!(evidence[0].total_bytes, Some(gib(4)));
        assert_eq!(evidence[1].total_bytes, Some(gib(24)));
        assert!(
            evidence
                .iter()
                .all(|device| device.source == DeviceMemoryEvidenceSource::NvidiaSmi)
        );
    }

    #[test]
    fn alm6_intel_igpu_plus_nvidia_dgpu_maps_cuda0_to_nvidia_pool() -> Result<()> {
        let host = host(
            "windows",
            "x86_64",
            vec![
                gpu("intel", "Intel integrated", Some(gib(2))),
                gpu("nvidia", "NVIDIA discrete", Some(gib(12))),
            ],
        );
        let intel = device("intel", Some(gib(2)), Some(gib(1)));
        let mut nvidia = device("nvidia", Some(gib(12)), Some(gib(10)));
        nvidia.gpu_index = 1;
        let mut inventory = inventory("windows", "x86_64", 8, gib(12), vec![intel, nvidia]);
        inventory.host_fingerprint = host_hardware_fingerprint(&host);

        let candidates = runtime_profile_candidates(
            &inventory,
            &policy(&[
                InferenceBackend::Cuda,
                InferenceBackend::Vulkan,
                InferenceBackend::Cpu,
            ]),
        );
        assert!(candidates.iter().any(|candidate| {
            candidate.backend == InferenceBackend::Cuda
                && candidate.device_key.as_deref() == Some("CUDA0")
        }));
        assert!(
            candidates
                .iter()
                .all(|candidate| candidate.backend != InferenceBackend::Vulkan)
        );
        assert_eq!(
            runtime_device_memory(&inventory, InferenceBackend::Cuda, Some("CUDA0"))
                .map(|device| (device.gpu_index, device.vendor.as_deref())),
            Some((1, Some("nvidia")))
        );

        let converted = bundle_inference_host(&host, &inventory)?;
        assert_eq!(converted.devices.len(), 1);
        assert_eq!(converted.devices[0].id, "CUDA0");
        assert_eq!(converted.devices[0].vendor, AnimeGpuVendor::Nvidia);
        assert_eq!(converted.devices[0].available_memory_bytes, Some(gib(10)));
        Ok(())
    }

    #[test]
    fn alm6_intel_plus_amd_maps_rocm0_to_amd_pool_and_omits_vulkan() -> Result<()> {
        let host = host(
            "linux",
            "x86_64",
            vec![
                gpu("intel", "Intel integrated", Some(gib(2))),
                gpu("amd", "AMD discrete", Some(gib(12))),
            ],
        );
        let intel = device("intel", Some(gib(2)), Some(gib(1)));
        let mut amd = device("amd", Some(gib(12)), Some(gib(10)));
        amd.gpu_index = 1;
        let mut inventory = inventory("linux", "x86_64", 8, gib(12), vec![intel, amd]);
        inventory.host_fingerprint = host_hardware_fingerprint(&host);

        let candidates = runtime_profile_candidates(
            &inventory,
            &policy(&[
                InferenceBackend::Hip,
                InferenceBackend::Vulkan,
                InferenceBackend::Cpu,
            ]),
        );
        assert!(candidates.iter().any(|candidate| {
            candidate.backend == InferenceBackend::Hip
                && candidate.device_key.as_deref() == Some("ROCm0")
        }));
        assert!(
            candidates
                .iter()
                .all(|candidate| candidate.backend != InferenceBackend::Vulkan)
        );
        assert_eq!(
            runtime_device_memory(&inventory, InferenceBackend::Hip, Some("ROCm0"))
                .map(|device| (device.gpu_index, device.vendor.as_deref())),
            Some((1, Some("amd")))
        );

        let converted = bundle_inference_host(&host, &inventory)?;
        assert_eq!(converted.devices.len(), 1);
        assert_eq!(converted.devices[0].id, "ROCm0");
        assert_eq!(converted.devices[0].vendor, AnimeGpuVendor::Amd);
        assert_eq!(converted.devices[0].available_memory_bytes, Some(gib(10)));
        Ok(())
    }

    #[test]
    fn alm6_llama_device_selector_contract_covers_cpu_and_accelerators() {
        assert_eq!(InferenceBackend::Cpu.llama_device_selector(), "none");
        assert_eq!(InferenceBackend::Metal.llama_device_selector(), "MTL0");
        assert_eq!(InferenceBackend::Cuda.llama_device_selector(), "CUDA0");
        assert_eq!(InferenceBackend::Hip.llama_device_selector(), "ROCm0");
        assert_eq!(InferenceBackend::Vulkan.llama_device_selector(), "Vulkan0");
    }

    #[test]
    fn alm6_default_probe_wrapper_exceeds_phase_deadlines_and_cleanup() {
        let limits = InferenceProbeLimits::default();
        assert_eq!(PROBE_LOAD_ALLOWANCE, Duration::from_secs(2 * 60));
        assert_eq!(limits.maximum_load_time, PROBE_LOAD_ALLOWANCE);
        assert_eq!(PROBE_PRIME_ALLOWANCE, Duration::from_secs(5 * 60));
        assert_eq!(PROBE_REQUEST_ALLOWANCE, Duration::from_secs(30 * 60));
        assert_eq!(PROBE_FINALIZATION_ALLOWANCE, Duration::from_secs(10));
        assert_eq!(PROBE_SCHEDULER_JITTER_ALLOWANCE, Duration::from_secs(4));
        assert_eq!(limits.maximum_warm_latency, Duration::from_secs(30 * 60));
        assert_eq!(limits.per_candidate_timeout, Duration::from_secs(2_234));
        let required = limits
            .maximum_load_time
            .saturating_add(PROBE_PRIME_ALLOWANCE)
            .saturating_add(PROBE_REQUEST_ALLOWANCE)
            .saturating_add(PROBE_FINALIZATION_ALLOWANCE);
        assert_eq!(
            limits.per_candidate_timeout,
            required.saturating_add(PROBE_SCHEDULER_JITTER_ALLOWANCE)
        );
    }

    #[tokio::test]
    async fn alm6_probe_rejects_gpu_then_selects_cpu() {
        let inventory = inventory(
            "linux",
            "x86_64",
            8,
            gib(8),
            vec![device("nvidia", Some(gib(8)), Some(gib(7)))],
        );
        let gpu = InferenceRuntimeCandidate {
            backend: InferenceBackend::Cuda,
            device_class: InferenceDeviceClass::Nvidia,
            device_key: Some(InferenceBackend::Cuda.llama_device_selector().to_string()),
            gpu_layers: 24,
            cpu_threads: 4,
            batch_threads: 4,
            required_device_reserve_bytes: MIN_DEVICE_MEMORY_RESERVE_BYTES,
        };
        let cpu = InferenceRuntimeCandidate {
            backend: InferenceBackend::Cpu,
            device_class: InferenceDeviceClass::Cpu,
            device_key: None,
            gpu_layers: 0,
            cpu_threads: 4,
            batch_threads: 8,
            required_device_reserve_bytes: 0,
        };
        let mut rejected = passing_measurement();
        rejected.device_available_bytes = Some(MIB);
        let probe = FakeProbe::new(vec![Ok(rejected), Ok(passing_measurement())]);
        let selected = select_runtime_profile(
            &inventory,
            &identity(),
            &[gpu, cpu],
            &InferenceProbeLimits::default(),
            &probe,
        )
        .await;
        assert_eq!(selected.outcome, InferenceEnvelopeOutcome::CpuBalanced);
        assert_eq!(selected.attempts.len(), 2);
        assert_eq!(
            selected.attempts[0].rejection,
            Some(InferenceProbeRejection::DeviceMemoryReserve)
        );
        assert_eq!(
            selected.profile.expect("profile").backend,
            InferenceBackend::Cpu
        );
    }

    struct SlowProbe;

    #[async_trait]
    impl InferenceEnvelopeProbe for SlowProbe {
        async fn probe(
            &self,
            _candidate: &InferenceRuntimeCandidate,
        ) -> std::result::Result<InferenceProbeMeasurement, InferenceProbeError> {
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok(passing_measurement())
        }
    }

    #[tokio::test]
    async fn alm6_probe_runner_is_hard_bounded() {
        let inventory = inventory("linux", "x86_64", 8, gib(8), Vec::new());
        let candidate = InferenceRuntimeCandidate {
            backend: InferenceBackend::Cpu,
            device_class: InferenceDeviceClass::Cpu,
            device_key: None,
            gpu_layers: 0,
            cpu_threads: 4,
            batch_threads: 8,
            required_device_reserve_bytes: 0,
        };
        let mut limits = InferenceProbeLimits::default();
        limits.per_candidate_timeout = Duration::from_millis(5);
        let selected =
            select_runtime_profile(&inventory, &identity(), &[candidate], &limits, &SlowProbe)
                .await;
        assert_eq!(
            selected.outcome,
            InferenceEnvelopeOutcome::DeterministicOnly
        );
        assert_eq!(selected.attempts[0].status, InferenceProbeStatus::TimedOut);
    }

    #[test]
    fn alm6_profile_compatibility_invalidates_every_runtime_identity_change() {
        let inventory = inventory("linux", "x86_64", 8, gib(8), Vec::new());
        let candidate = InferenceRuntimeCandidate {
            backend: InferenceBackend::Cpu,
            device_class: InferenceDeviceClass::Cpu,
            device_key: None,
            gpu_layers: 0,
            cpu_threads: 4,
            batch_threads: 8,
            required_device_reserve_bytes: 0,
        };
        let profile = runtime_profile_from_probe(
            &inventory,
            &identity(),
            &candidate,
            &passing_measurement(),
            InferenceEnvelopeOutcome::CpuBalanced,
        );
        let requirements = ProfileCompatibilityRequirements {
            bundle_version: profile.bundle_version.clone(),
            model_revision: profile.model_revision.clone(),
            worker_revision: profile.worker_revision.clone(),
            runtime_policy_revision: profile.runtime_policy_revision.clone(),
            hardware_fingerprint: profile.hardware_fingerprint.clone(),
            certified_backends: [InferenceBackend::Cpu].into_iter().collect(),
        };
        assert_eq!(
            runtime_profile_compatibility(&profile, &requirements),
            RuntimeProfileCompatibility::Compatible
        );

        let mut changed = requirements.clone();
        changed.model_revision = "qwen-r2".to_string();
        assert_eq!(
            runtime_profile_compatibility(&profile, &changed),
            RuntimeProfileCompatibility::Invalid(RuntimeProfileInvalidationReason::ModelChanged)
        );
        changed = requirements.clone();
        changed.worker_revision = "llama-r2".to_string();
        assert_eq!(
            runtime_profile_compatibility(&profile, &changed),
            RuntimeProfileCompatibility::Invalid(RuntimeProfileInvalidationReason::WorkerChanged)
        );
        changed = requirements.clone();
        changed.hardware_fingerprint = "sha256:new-host".to_string();
        assert_eq!(
            runtime_profile_compatibility(&profile, &changed),
            RuntimeProfileCompatibility::Invalid(RuntimeProfileInvalidationReason::HardwareChanged)
        );
        changed = requirements.clone();
        changed.bundle_version = "2026.09.1".to_string();
        assert_eq!(
            runtime_profile_compatibility(&profile, &changed),
            RuntimeProfileCompatibility::Invalid(RuntimeProfileInvalidationReason::BundleChanged)
        );
        changed = requirements.clone();
        changed.runtime_policy_revision = "policy-r2".to_string();
        assert_eq!(
            runtime_profile_compatibility(&profile, &changed),
            RuntimeProfileCompatibility::Invalid(
                RuntimeProfileInvalidationReason::RuntimePolicyChanged
            )
        );
        changed = requirements;
        changed.certified_backends.clear();
        assert_eq!(
            runtime_profile_compatibility(&profile, &changed),
            RuntimeProfileCompatibility::Invalid(
                RuntimeProfileInvalidationReason::BackendNoLongerCertified
            )
        );
    }

    #[test]
    fn alm6_failed_health_check_invalidates_persisted_profile() -> Result<()> {
        let inventory = inventory("linux", "x86_64", 8, gib(8), Vec::new());
        let candidate = InferenceRuntimeCandidate {
            backend: InferenceBackend::Cpu,
            device_class: InferenceDeviceClass::Cpu,
            device_key: None,
            gpu_layers: 0,
            cpu_threads: 4,
            batch_threads: 8,
            required_device_reserve_bytes: 0,
        };
        let mut profile = runtime_profile_from_probe(
            &inventory,
            &identity(),
            &candidate,
            &passing_measurement(),
            InferenceEnvelopeOutcome::CpuBalanced,
        );
        invalidate_runtime_profile(
            &mut profile,
            RuntimeProfileInvalidationReason::HealthCheckFailed,
        );
        let requirements = ProfileCompatibilityRequirements {
            bundle_version: profile.bundle_version.clone(),
            model_revision: profile.model_revision.clone(),
            worker_revision: profile.worker_revision.clone(),
            runtime_policy_revision: profile.runtime_policy_revision.clone(),
            hardware_fingerprint: profile.hardware_fingerprint.clone(),
            certified_backends: [InferenceBackend::Cpu].into_iter().collect(),
        };
        let bytes = serde_json::to_vec(&profile)?;
        assert!(matches!(
            decode_compatible_runtime_profile(&bytes, &requirements)?,
            Err(RuntimeProfileInvalidationReason::HealthCheckFailed)
        ));
        Ok(())
    }

    #[test]
    fn alm6_hardware_fingerprint_ignores_dynamic_available_memory() {
        let mut first = inventory("linux", "x86_64", 8, gib(8), Vec::new());
        let first_fingerprint = inference_hardware_fingerprint(&first);
        first.memory.available_bytes = Some(gib(5));
        first.collected_at = Utc::now() + chrono::Duration::minutes(2);
        assert_eq!(first_fingerprint, inference_hardware_fingerprint(&first));
        first.memory.total_bytes = Some(gib(32));
        assert_ne!(first_fingerprint, inference_hardware_fingerprint(&first));
    }

    #[test]
    fn alm6_cached_acceleration_requires_comparable_driver_evidence() {
        let mut amd = gpu("amd", "AMD discrete", Some(gib(8)));
        amd.driver_version = None;
        let mut host = host("linux", "x86_64", vec![amd]);
        let mut inventory = inventory(
            "linux",
            "x86_64",
            8,
            gib(12),
            vec![device("amd", Some(gib(8)), Some(gib(7)))],
        );
        inventory.host_fingerprint = host_hardware_fingerprint(&host);
        assert!(!cached_profile_driver_evidence_is_reusable(
            &host,
            &inventory,
            AnimeExecutionBackend::Hip,
            Some("ROCm0"),
        ));

        host.gpus[0].driver_version = Some("Mesa 24.1.0".to_string());
        inventory.host_fingerprint = host_hardware_fingerprint(&host);
        assert!(!cached_profile_driver_evidence_is_reusable(
            &host,
            &inventory,
            AnimeExecutionBackend::Hip,
            Some("ROCm0"),
        ));

        host.gpus[0].driver_version = Some("24.1.0".to_string());
        inventory.host_fingerprint = host_hardware_fingerprint(&host);
        assert!(cached_profile_driver_evidence_is_reusable(
            &host,
            &inventory,
            AnimeExecutionBackend::Hip,
            Some("ROCm0"),
        ));
        assert!(cached_profile_driver_evidence_is_reusable(
            &host,
            &inventory,
            AnimeExecutionBackend::Cpu,
            None,
        ));
    }

    #[test]
    fn alm9_intel_mac_cached_metal_profile_is_never_reused() {
        let mut host = host(
            "macos",
            "x86_64",
            vec![gpu("amd", "AMD Radeon Pro 5500M", Some(gib(4)))],
        );
        host.os.version = Some("26.5.2".to_string());
        let mut inventory = inventory(
            "macos",
            "x86_64",
            8,
            gib(10),
            vec![device("amd", Some(gib(4)), Some(gib(3)))],
        );
        inventory.host_fingerprint = host_hardware_fingerprint(&host);

        assert!(!cached_profile_driver_evidence_is_reusable(
            &host,
            &inventory,
            AnimeExecutionBackend::Metal,
            Some("MTL0"),
        ));
        assert!(cached_profile_driver_evidence_is_reusable(
            &host,
            &inventory,
            AnimeExecutionBackend::Cpu,
            None,
        ));
    }

    #[test]
    fn alm6_linux_memory_and_cgroup_limit_are_conservative() {
        let raw =
            "MemTotal:       16777216 kB\nMemFree:        1048576 kB\nMemAvailable:   8388608 kB\n";
        let (total, available) = parse_linux_meminfo(raw);
        assert_eq!(total, Some(gib(16)));
        assert_eq!(available, Some(gib(8)));
        let (total, available, limit) = effective_memory_with_cgroup(
            total,
            available,
            Some(CgroupMemory {
                limit_bytes: gib(6),
                current_bytes: gib(3),
            }),
        );
        assert_eq!(total, Some(gib(6)));
        assert_eq!(available, Some(gib(3)));
        assert_eq!(limit, Some(gib(6)));
    }

    #[test]
    fn alm6_cgroup_v2_membership_resolves_non_root_mount_and_escapes() {
        let cgroup = "0::/tenant.slice/server scope\n";
        let mountinfo = "36 25 0:32 /tenant.slice /sys/fs/cgroup/elixir\\040root rw,nosuid,nodev,noexec,relatime - cgroup2 cgroup rw\n";
        let paths = resolve_cgroup_memory_paths(cgroup, mountinfo).expect("v2 paths");
        assert_eq!(
            paths.limit,
            PathBuf::from("/sys/fs/cgroup/elixir root/server scope/memory.max")
        );
        assert_eq!(
            paths.current,
            PathBuf::from("/sys/fs/cgroup/elixir root/server scope/memory.current")
        );
    }

    #[test]
    fn alm6_cgroup_v1_memory_controller_resolves_process_membership() {
        let cgroup = "9:cpuset:/pod-a\n8:memory,cpu:/docker/abc/workload\n";
        let mountinfo = "41 25 0:38 /docker/abc /sys/fs/cgroup/memory rw,nosuid,nodev,noexec,relatime - cgroup cgroup rw,memory\n";
        let paths = resolve_cgroup_memory_paths(cgroup, mountinfo).expect("v1 paths");
        assert_eq!(
            paths.limit,
            PathBuf::from("/sys/fs/cgroup/memory/workload/memory.limit_in_bytes")
        );
        assert_eq!(
            paths.current,
            PathBuf::from("/sys/fs/cgroup/memory/workload/memory.usage_in_bytes")
        );
    }

    #[test]
    fn alm6_cgroup_path_resolution_rejects_escape_and_wrong_mount_root() {
        assert!(
            resolve_cgroup_mount_path(
                Path::new("/tenant"),
                Path::new("/sys/fs/cgroup"),
                Path::new("/other/workload")
            )
            .is_none()
        );
        assert!(decode_mountinfo_path("/sys/fs/cgroup/../escape").is_none());
    }

    #[test]
    fn alm6_exposed_drm_rows_discover_amd_and_intel_without_unsafe_vendor_guessing() {
        let mut evidence = Vec::new();
        merge_linux_exposed_device_rows(
            &mut evidence,
            vec![
                (
                    "renderD128".to_string(),
                    "intel".to_string(),
                    Some("0x46a6".to_string()),
                    None,
                    None,
                ),
                (
                    "renderD129".to_string(),
                    "amd".to_string(),
                    Some("0x73bf".to_string()),
                    Some(gib(8)),
                    Some(gib(2)),
                ),
            ],
        );
        assert_eq!(evidence.len(), 2);
        assert_eq!(evidence[0].vendor.as_deref(), Some("intel"));
        assert_eq!(evidence[0].source, DeviceMemoryEvidenceSource::Unknown);
        assert_eq!(evidence[1].vendor.as_deref(), Some("amd"));
        assert_eq!(evidence[1].available_bytes, Some(gib(6)));
        assert_eq!(evidence[1].source, DeviceMemoryEvidenceSource::LinuxSysfs);
        assert_eq!(linux_pci_vendor_name("0x8086\n"), Some("intel"));
        assert_eq!(linux_pci_vendor_name("not-a-vendor"), None);
    }

    #[test]
    fn alm6_parsers_handle_cpu_vm_and_device_memory_evidence() {
        let cpuinfo = "processor: 0\nphysical id: 0\ncore id: 0\nmodel name: Test CPU\n\nprocessor: 1\nphysical id: 0\ncore id: 1\nmodel name: Test CPU\n";
        assert_eq!(parse_linux_physical_cores(cpuinfo), Some(2));
        assert_eq!(parse_linux_cpu_model(cpuinfo).as_deref(), Some("Test CPU"));
        let vm = "Mach Virtual Memory Statistics: (page size of 4096 bytes)\nPages free: 100.\nPages active: 20.\nPages inactive: 200.\nPages speculative: 50.\n";
        assert_eq!(parse_macos_vm_stat(vm), Some(350 * 4096));
        assert_eq!(
            parse_nvidia_memory_rows("8192, 6144\n4096, 2048\n"),
            vec![(8192 * MIB, 6144 * MIB), (4096 * MIB, 2048 * MIB)]
        );
        assert_eq!(memory_value_bytes(&json!("4 GB")), Some(gib(4)));
    }

    #[test]
    fn alm6_container_classification_is_deterministic() {
        assert_eq!(
            classify_container_markers(true, Some("docker"), true, false).kind,
            InferenceContainerKind::Kubernetes
        );
        assert_eq!(
            classify_container_markers(false, Some("podman"), true, false).kind,
            InferenceContainerKind::Podman
        );
        assert!(!classify_container_markers(false, None, false, false).detected);
    }

    #[test]
    fn alm6_host_memory_extraction_understands_existing_gpu_raw_shapes() {
        let mac = gpu("amd", "Radeon Pro 5500M", None);
        let mut mac = mac;
        mac.raw = json!({"spdisplays_vram": "4 GB"});
        let memory = InferenceSystemMemory {
            total_bytes: Some(gib(16)),
            available_bytes: Some(gib(8)),
            source: "test".to_string(),
            container_limit_bytes: None,
        };
        let extracted = device_memory_from_host(0, &mac, &memory);
        assert_eq!(extracted.total_bytes, Some(gib(4)));
        assert_eq!(extracted.available_bytes, Some(gib(3)));
        assert!(extracted.available_is_estimate);
        let _ = host("macos", "x86_64", vec![mac]);
    }

    #[test]
    fn alm6_memory_pressure_reserves_system_memory_and_cold_start_rss() {
        let mut memory = InferenceSystemMemory {
            total_bytes: Some(gib(16)),
            available_bytes: Some(gib(8)),
            source: "test".to_string(),
            container_limit_bytes: None,
        };
        let cold = assess_inference_memory_pressure(&memory, Some(gib(3)));
        assert_eq!(cold.required_available_bytes, gib(7));
        assert!(!cold.under_pressure);

        memory.available_bytes = Some(gib(6));
        assert!(assess_inference_memory_pressure(&memory, Some(gib(3))).under_pressure);
        assert!(!assess_inference_memory_pressure(&memory, None).under_pressure);

        memory.available_bytes = None;
        assert!(assess_inference_memory_pressure(&memory, None).under_pressure);
    }

    #[tokio::test]
    async fn alm6_invalid_probe_candidate_is_rejected_without_running_worker() {
        let inventory = inventory("linux", "x86_64", 8, gib(8), Vec::new());
        let invalid = InferenceRuntimeCandidate {
            backend: InferenceBackend::Cpu,
            device_class: InferenceDeviceClass::Cpu,
            device_key: None,
            gpu_layers: 0,
            cpu_threads: MAX_INFERENCE_CPU_THREADS + 1,
            batch_threads: MAX_INFERENCE_CPU_THREADS + 1,
            required_device_reserve_bytes: 0,
        };
        let probe = FakeProbe::new(Vec::new());
        let selected = select_runtime_profile(
            &inventory,
            &identity(),
            &[invalid],
            &InferenceProbeLimits::default(),
            &probe,
        )
        .await;
        assert_eq!(
            selected.outcome,
            InferenceEnvelopeOutcome::DeterministicOnly
        );
        assert_eq!(selected.attempts[0].status, InferenceProbeStatus::Rejected);
        assert_eq!(
            selected.attempts[0].rejection,
            Some(InferenceProbeRejection::InvalidCandidate)
        );
    }

    #[tokio::test]
    async fn alm6_batch_threads_must_cover_generation_and_fit_physical_cores() {
        let inventory = inventory("linux", "x86_64", 4, gib(8), Vec::new());
        for batch_threads in [3, 8, MAX_INFERENCE_BATCH_THREADS + 1] {
            let invalid = InferenceRuntimeCandidate {
                backend: InferenceBackend::Cpu,
                device_class: InferenceDeviceClass::Cpu,
                device_key: None,
                gpu_layers: 0,
                cpu_threads: 4,
                batch_threads,
                required_device_reserve_bytes: 0,
            };
            let selected = select_runtime_profile(
                &inventory,
                &identity(),
                &[invalid],
                &InferenceProbeLimits::default(),
                &FakeProbe::new(Vec::new()),
            )
            .await;
            assert_eq!(
                selected.attempts[0].rejection,
                Some(InferenceProbeRejection::InvalidCandidate),
                "batch_threads={batch_threads} must be rejected"
            );
        }
    }

    #[tokio::test]
    async fn alm6_probe_rejects_missing_peak_rss_measurement() {
        let inventory = inventory("linux", "x86_64", 8, gib(8), Vec::new());
        let candidate = InferenceRuntimeCandidate {
            backend: InferenceBackend::Cpu,
            device_class: InferenceDeviceClass::Cpu,
            device_key: None,
            gpu_layers: 0,
            cpu_threads: 4,
            batch_threads: 8,
            required_device_reserve_bytes: 0,
        };
        let mut measurement = passing_measurement();
        measurement.peak_rss_bytes = 0;
        let probe = FakeProbe::new(vec![Ok(measurement)]);
        let selected = select_runtime_profile(
            &inventory,
            &identity(),
            &[candidate],
            &InferenceProbeLimits::default(),
            &probe,
        )
        .await;
        assert_eq!(
            selected.outcome,
            InferenceEnvelopeOutcome::DeterministicOnly
        );
        assert_eq!(
            selected.attempts[0].rejection,
            Some(InferenceProbeRejection::WorkerMemoryUnavailable)
        );
    }

    #[test]
    fn alm6_apple_silicon_inventory_has_unified_memory_fallback() {
        let host = host("macos", "aarch64", Vec::new());
        let memory = InferenceSystemMemory {
            total_bytes: Some(gib(16)),
            available_bytes: Some(gib(11)),
            source: "test".to_string(),
            container_limit_bytes: None,
        };
        let mut devices = Vec::new();
        ensure_apple_unified_memory_device(&mut devices, &host, &memory);
        assert_eq!(devices.len(), 1);
        assert_eq!(devices[0].vendor.as_deref(), Some("apple"));
        assert_eq!(devices[0].available_bytes, Some(gib(11)));
        assert_eq!(
            devices[0].source,
            DeviceMemoryEvidenceSource::MacosUnifiedMemory
        );
    }

    #[test]
    fn alm6_bundle_host_conversion_fills_playback_os_version_gap() -> Result<()> {
        let mut host = host(
            "windows",
            "x86_64",
            vec![gpu("nvidia", "RTX Test", Some(gib(8)))],
        );
        host.os.version = None;
        let mut inventory = inventory(
            "windows",
            "x86_64",
            8,
            gib(12),
            vec![device("nvidia", Some(gib(8)), Some(gib(7)))],
        );
        inventory.os_version = Some("10.0.26100".to_string());
        inventory.host_fingerprint = host_hardware_fingerprint(&host);
        inventory.cpu.features = vec![
            "AVX2".to_string(),
            "sse4.1".to_string(),
            "invalid feature!".to_string(),
        ];

        let converted = bundle_inference_host(&host, &inventory)?;
        assert_eq!(converted.os, AnimeHostOs::Windows);
        assert_eq!(converted.arch, AnimeHostArch::X86_64);
        assert_eq!(converted.os_version.as_deref(), Some("10.0.26100"));
        assert_eq!(
            converted.cpu_features,
            ["avx2".to_string(), "sse4_1".to_string()]
                .into_iter()
                .collect()
        );
        assert_eq!(converted.devices.len(), 2);
        assert_eq!(converted.devices[0].id, "CUDA0");
        assert_eq!(converted.devices[0].vendor, AnimeGpuVendor::Nvidia);
        assert_eq!(converted.devices[0].available_memory_bytes, Some(gib(7)));
        assert_eq!(
            converted.devices[0].certified_backends,
            [AnimeAcceleratorBackend::Cuda].into_iter().collect()
        );
        assert_eq!(converted.devices[1].id, "Vulkan0");
        assert_eq!(
            converted.devices[1].certified_backends,
            [AnimeAcceleratorBackend::Vulkan].into_iter().collect()
        );
        Ok(())
    }

    #[test]
    fn alm6_backend_eligibility_order_is_platform_and_vendor_specific() {
        assert_eq!(
            eligible_anime_accelerator_backends(AnimeHostOs::Macos, AnimeGpuVendor::Amd),
            vec![AnimeAcceleratorBackend::Metal]
        );
        assert_eq!(
            eligible_anime_accelerator_backends(AnimeHostOs::Linux, AnimeGpuVendor::Amd),
            vec![
                AnimeAcceleratorBackend::Hip,
                AnimeAcceleratorBackend::Vulkan
            ]
        );
        assert_eq!(
            eligible_anime_accelerator_backends(AnimeHostOs::Windows, AnimeGpuVendor::Intel),
            vec![AnimeAcceleratorBackend::Vulkan]
        );
        assert!(
            eligible_anime_accelerator_backends(AnimeHostOs::Windows, AnimeGpuVendor::Unknown)
                .is_empty()
        );
    }

    #[test]
    fn alm6_linux_container_requires_backend_device_nodes_not_sysfs_alone() {
        let none = LinuxContainerDeviceAccess::default();
        assert!(!device_backend_is_exposed_in_container(
            AnimeHostOs::Linux,
            DeviceMemoryEvidenceSource::NvidiaSmi,
            AnimeAcceleratorBackend::Cuda,
            none,
        ));
        assert!(!device_backend_is_exposed_in_container(
            AnimeHostOs::Linux,
            DeviceMemoryEvidenceSource::LinuxSysfs,
            AnimeAcceleratorBackend::Vulkan,
            none,
        ));

        let native_cuda = LinuxContainerDeviceAccess {
            nvidia_zero: true,
            ..LinuxContainerDeviceAccess::default()
        };
        assert!(device_backend_is_exposed_in_container(
            AnimeHostOs::Linux,
            DeviceMemoryEvidenceSource::NvidiaSmi,
            AnimeAcceleratorBackend::Cuda,
            native_cuda,
        ));

        let wsl_cuda = LinuxContainerDeviceAccess {
            wsl_dxg: true,
            ..LinuxContainerDeviceAccess::default()
        };
        assert!(device_backend_is_exposed_in_container(
            AnimeHostOs::Linux,
            DeviceMemoryEvidenceSource::NvidiaSmi,
            AnimeAcceleratorBackend::Cuda,
            wsl_cuda,
        ));
        assert!(!device_backend_is_exposed_in_container(
            AnimeHostOs::Linux,
            DeviceMemoryEvidenceSource::LinuxSysfs,
            AnimeAcceleratorBackend::Cuda,
            wsl_cuda,
        ));

        let render_only = LinuxContainerDeviceAccess {
            drm_render: true,
            ..LinuxContainerDeviceAccess::default()
        };
        assert!(device_backend_is_exposed_in_container(
            AnimeHostOs::Linux,
            DeviceMemoryEvidenceSource::LinuxSysfs,
            AnimeAcceleratorBackend::Vulkan,
            render_only,
        ));
        assert!(!device_backend_is_exposed_in_container(
            AnimeHostOs::Linux,
            DeviceMemoryEvidenceSource::LinuxSysfs,
            AnimeAcceleratorBackend::Hip,
            render_only,
        ));

        let amd = LinuxContainerDeviceAccess {
            drm_render: true,
            kfd: true,
            ..LinuxContainerDeviceAccess::default()
        };
        assert!(device_backend_is_exposed_in_container(
            AnimeHostOs::Linux,
            DeviceMemoryEvidenceSource::LinuxSysfs,
            AnimeAcceleratorBackend::Hip,
            amd,
        ));
    }

    #[test]
    fn alm6_probe_profile_bridges_exactly_into_sealed_bundle_profile() -> Result<()> {
        let bundle = bridge_bundle();
        let artifact = bundle
            .manifest()
            .runtimes
            .iter()
            .find(|runtime| {
                runtime.os == AnimeHostOs::Linux && runtime.backend == AnimeRuntimeBackend::CudaCpu
            })
            .expect("linux CUDA runtime")
            .clone();
        let runtime = ResolvedAnimeRuntime {
            artifact,
            execution_backend: AnimeExecutionBackend::Cuda,
            device_id: Some("CUDA0".to_string()),
        };
        let inventory = inventory(
            "linux",
            "x86_64",
            8,
            gib(12),
            vec![device("nvidia", Some(gib(8)), Some(gib(7)))],
        );
        let candidate = InferenceRuntimeCandidate {
            backend: InferenceBackend::Cuda,
            device_class: InferenceDeviceClass::Nvidia,
            device_key: Some("CUDA0".to_string()),
            gpu_layers: 24,
            cpu_threads: 4,
            batch_threads: 4,
            required_device_reserve_bytes: MIN_DEVICE_MEMORY_RESERVE_BYTES,
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
        let probe_profile = runtime_profile_from_probe(
            &inventory,
            &identity,
            &candidate,
            &passing_measurement(),
            InferenceEnvelopeOutcome::GpuBalanced,
        );

        let bridged = bundle_runtime_profile_from_probe(&bundle, &runtime, &probe_profile)?;
        bridged.validate()?;
        assert_eq!(bridged.model_id, bundle.manifest().model.id);
        assert_eq!(
            bridged.runtime_artifact_key,
            runtime.artifact.artifact_key()
        );
        assert_eq!(bridged.execution_backend, AnimeExecutionBackend::Cuda);
        assert_eq!(bridged.device_id, runtime.device_id);
        assert_eq!(bridged.gpu_layer_count, 24);
        assert_eq!(bridged.cpu_thread_count, 4);
        assert_eq!(bridged.load_time_ms, probe_profile.load_time_ms);
        assert_eq!(bridged.warm_latency_ms, probe_profile.warm_latency_ms);
        assert_eq!(bridged.peak_rss_bytes, probe_profile.peak_rss_bytes);
        assert_eq!(
            bridged.peak_device_memory_bytes,
            probe_profile.peak_device_memory_bytes
        );
        assert!(!bridged.profile_fingerprint.is_empty());
        Ok(())
    }

    #[test]
    fn alm9_probe_profile_bridges_manifest_q8_0_kv_cache() -> Result<()> {
        let mut manifest = bridge_bundle().manifest().clone();
        manifest.runtime_policy.kv_cache_type = AnimeKvCacheType::Q8_0;
        let bundle = validate_anime_bundle(
            manifest,
            &AnimeBundleCompatibilityPolicy::development(semver::Version::new(0, 1, 0)),
        )?;
        let artifact = bundle
            .manifest()
            .runtimes
            .iter()
            .find(|runtime| {
                runtime.os == AnimeHostOs::Linux && runtime.backend == AnimeRuntimeBackend::CudaCpu
            })
            .expect("linux CUDA runtime")
            .clone();
        let runtime = ResolvedAnimeRuntime {
            artifact,
            execution_backend: AnimeExecutionBackend::Cuda,
            device_id: Some("CUDA0".to_string()),
        };
        let inventory = inventory(
            "linux",
            "x86_64",
            8,
            gib(12),
            vec![device("nvidia", Some(gib(8)), Some(gib(7)))],
        );
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
        let probe_profile = runtime_profile_from_probe(
            &inventory,
            &identity,
            &InferenceRuntimeCandidate {
                backend: InferenceBackend::Cuda,
                device_class: InferenceDeviceClass::Nvidia,
                device_key: Some("CUDA0".to_string()),
                gpu_layers: 24,
                cpu_threads: 4,
                batch_threads: 4,
                required_device_reserve_bytes: MIN_DEVICE_MEMORY_RESERVE_BYTES,
            },
            &passing_measurement(),
            InferenceEnvelopeOutcome::GpuBalanced,
        );

        assert_eq!(probe_profile.kv_cache_type, "q8_0");
        let bridged = bundle_runtime_profile_from_probe(&bundle, &runtime, &probe_profile)?;
        assert_eq!(bridged.kv_cache_type, AnimeKvCacheType::Q8_0);
        bridged.validate()?;
        Ok(())
    }

    #[test]
    fn alm6_probe_profile_bridge_rejects_runtime_device_mismatch() {
        let bundle = bridge_bundle();
        let artifact = bundle
            .manifest()
            .runtimes
            .iter()
            .find(|runtime| {
                runtime.os == AnimeHostOs::Linux && runtime.backend == AnimeRuntimeBackend::CudaCpu
            })
            .expect("linux CUDA runtime")
            .clone();
        let runtime = ResolvedAnimeRuntime {
            artifact,
            execution_backend: AnimeExecutionBackend::Cuda,
            device_id: Some("different-gpu".to_string()),
        };
        let inventory = inventory(
            "linux",
            "x86_64",
            8,
            gib(12),
            vec![device("nvidia", Some(gib(8)), Some(gib(7)))],
        );
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
        let profile = runtime_profile_from_probe(
            &inventory,
            &identity,
            &InferenceRuntimeCandidate {
                backend: InferenceBackend::Cuda,
                device_class: InferenceDeviceClass::Nvidia,
                device_key: Some("CUDA0".to_string()),
                gpu_layers: 24,
                cpu_threads: 4,
                batch_threads: 4,
                required_device_reserve_bytes: MIN_DEVICE_MEMORY_RESERVE_BYTES,
            },
            &passing_measurement(),
            InferenceEnvelopeOutcome::GpuBalanced,
        );
        assert_eq!(
            bundle_runtime_profile_from_probe(&bundle, &runtime, &profile),
            Err(BundleRuntimeProfileBridgeError::RuntimeDeviceMismatch)
        );
    }
}
