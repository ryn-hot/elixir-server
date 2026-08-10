//! First-party anime inference bundle contracts, platform resolution, and
//! crash-safe on-disk activation.
//!
//! This module deliberately owns artifacts rather than inference policy. It
//! validates one bundle, selects one compatible runtime plus a CPU fallback,
//! stages verified files, and atomically switches a single active descriptor.
//! Worker supervision and application lifecycle integration live elsewhere.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    ffi::CString,
    fs::{self as std_fs, File, OpenOptions},
    io::{self, Read, Write},
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use flate2::read::GzDecoder;
use futures_util::{Stream, StreamExt};
use reqwest::{Client, Url};
use semver::Version;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use tar::Archive;
use tokio::{fs as async_fs, io::AsyncWriteExt};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub const ANIME_BUNDLE_SCHEMA_VERSION: u32 = 1;
pub const ANIME_INFERENCE_PROTOCOL_VERSION: u32 = 1;
pub const ANIME_MATCHER_SCHEMA_VERSION: u32 = 1;
pub const ANIME_RUNTIME_PROFILE_SCHEMA_VERSION: u32 = 1;
const ARTIFACT_MARKER_SCHEMA_VERSION: u32 = 1;
const PENDING_ACTIVATION_SCHEMA_VERSION: u32 = 1;
const DESCRIPTOR_MAX_BYTES: u64 = 4 * 1024 * 1024;
const STAGING_RESERVE_BYTES: u64 = 64 * 1024 * 1024;
const CANCELLABLE_IO_CHUNK_BYTES: usize = 1024 * 1024;
const MAX_RUNTIME_ARTIFACTS: usize = 64;
const MAX_ARCHIVE_ENTRIES: usize = 8_192;
const MAX_PACKAGED_DEPENDENCIES: usize = 256;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnimeInferenceBundleManifest {
    pub schema_version: u32,
    pub bundle_version: String,
    pub protocol_version: u32,
    pub matcher_schema_version: u32,
    pub minimum_server_version: String,
    /// One logical pinned llama.cpp worker generation shared by every
    /// platform artifact in this bundle. Runtime `revision` remains the
    /// platform package build identity.
    pub worker_revision: String,
    pub model: AnimeModelArtifactManifest,
    pub runtime_policy: AnimeRuntimePolicyManifest,
    pub runtimes: Vec<AnimeRuntimeArtifactManifest>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnimeModelArtifactManifest {
    pub id: String,
    pub revision: String,
    pub upstream_model_id: String,
    pub upstream_revision: String,
    pub license: String,
    pub format: AnimeModelFormat,
    pub quantization: String,
    /// Transformer block count from the qualified GGUF. The local envelope
    /// uses this bound for conservative partial-offload fitting.
    pub transformer_layers: u32,
    pub context_tokens: u32,
    pub max_output_tokens: u32,
    pub thinking_mode: AnimeThinkingMode,
    pub chat_template_revision: String,
    pub conversion_tool_revision: String,
    pub qualification_report_fingerprint: String,
    pub url: String,
    pub sha256: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AnimeModelFormat {
    Gguf,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AnimeThinkingMode {
    NonThinkingOnly,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnimeRuntimePolicyManifest {
    pub sampling_profile_revision: String,
    pub parallel: u16,
    pub kv_cache_type: AnimeKvCacheType,
    pub idle_unload_seconds: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AnimeKvCacheType {
    F16,
    #[serde(rename = "q8_0")]
    Q8_0,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnimeRuntimeArtifactManifest {
    pub os: AnimeHostOs,
    pub arch: AnimeHostArch,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device_class: Option<AnimeDeviceClass>,
    pub backend: AnimeRuntimeBackend,
    pub priority: u16,
    pub revision: String,
    pub minimum_os_version: String,
    #[serde(default)]
    pub required_cpu_features: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minimum_driver_version: Option<String>,
    #[serde(default)]
    pub minimum_device_memory_bytes: u64,
    pub archive_format: AnimeRuntimeArchiveFormat,
    pub entrypoint: String,
    #[serde(default)]
    pub packaged_dependencies: Vec<String>,
    pub url: String,
    pub sha256: String,
    pub size_bytes: u64,
    pub installed_size_bytes: u64,
}

impl AnimeRuntimeArtifactManifest {
    pub fn artifact_key(&self) -> String {
        format!(
            "{}-{}-{}-{}-{}",
            self.os.as_str(),
            self.arch.as_str(),
            self.backend.as_str(),
            self.device_class
                .map(AnimeDeviceClass::as_str)
                .unwrap_or("any"),
            self.revision
        )
    }

    fn supports_cpu_execution(&self) -> bool {
        matches!(
            self.backend,
            AnimeRuntimeBackend::Cpu | AnimeRuntimeBackend::MetalCpu
        )
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "lowercase")]
pub enum AnimeHostOs {
    Macos,
    Windows,
    Linux,
}

impl AnimeHostOs {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Macos => "macos",
            Self::Windows => "windows",
            Self::Linux => "linux",
        }
    }

    pub fn current() -> Result<Self> {
        match std::env::consts::OS {
            "macos" => Ok(Self::Macos),
            "windows" => Ok(Self::Windows),
            "linux" => Ok(Self::Linux),
            other => bail!("anime inference does not support host OS '{other}'"),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum AnimeHostArch {
    X86_64,
    Aarch64,
}

impl AnimeHostArch {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::X86_64 => "x86_64",
            Self::Aarch64 => "aarch64",
        }
    }

    pub fn current() -> Result<Self> {
        match std::env::consts::ARCH {
            "x86_64" => Ok(Self::X86_64),
            "aarch64" => Ok(Self::Aarch64),
            other => bail!("anime inference does not support host architecture '{other}'"),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum AnimeDeviceClass {
    Nvidia,
    Amd,
    Intel,
    Apple,
    AnyVulkan,
    Cpu,
}

impl AnimeDeviceClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Nvidia => "nvidia",
            Self::Amd => "amd",
            Self::Intel => "intel",
            Self::Apple => "apple",
            Self::AnyVulkan => "any_vulkan",
            Self::Cpu => "cpu",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum AnimeRuntimeBackend {
    MetalCpu,
    CudaCpu,
    HipCpu,
    VulkanCpu,
    Cpu,
}

impl AnimeRuntimeBackend {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MetalCpu => "metal_cpu",
            Self::CudaCpu => "cuda_cpu",
            Self::HipCpu => "hip_cpu",
            Self::VulkanCpu => "vulkan_cpu",
            Self::Cpu => "cpu",
        }
    }

    fn accelerator(self) -> Option<AnimeAcceleratorBackend> {
        match self {
            Self::MetalCpu => Some(AnimeAcceleratorBackend::Metal),
            Self::CudaCpu => Some(AnimeAcceleratorBackend::Cuda),
            Self::HipCpu => Some(AnimeAcceleratorBackend::Hip),
            Self::VulkanCpu => Some(AnimeAcceleratorBackend::Vulkan),
            Self::Cpu => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AnimeRuntimeArchiveFormat {
    TarGz,
    Zip,
    Raw,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "lowercase")]
pub enum AnimeGpuVendor {
    Nvidia,
    Amd,
    Intel,
    Apple,
    Unknown,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "lowercase")]
pub enum AnimeAcceleratorBackend {
    Metal,
    Cuda,
    Hip,
    Vulkan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnimeInferenceDevice {
    pub id: String,
    pub vendor: AnimeGpuVendor,
    pub driver_version: Option<String>,
    pub available_memory_bytes: Option<u64>,
    pub certified_backends: BTreeSet<AnimeAcceleratorBackend>,
    /// Container hosts may inventory a physical GPU that is not actually
    /// mapped into the container. Such a device is never selected.
    pub exposed_to_container: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnimeInferenceHost {
    pub os: AnimeHostOs,
    pub arch: AnimeHostArch,
    pub os_version: Option<String>,
    pub cpu_features: BTreeSet<String>,
    pub devices: Vec<AnimeInferenceDevice>,
    pub containerized: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedAnimeRuntime {
    pub artifact: AnimeRuntimeArtifactManifest,
    pub execution_backend: AnimeExecutionBackend,
    pub device_id: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "lowercase")]
pub enum AnimeExecutionBackend {
    Metal,
    Cuda,
    Hip,
    Vulkan,
    Cpu,
}

impl AnimeExecutionBackend {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Metal => "metal",
            Self::Cuda => "cuda",
            Self::Hip => "hip",
            Self::Vulkan => "vulkan",
            Self::Cpu => "cpu",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnimeRuntimeSelection {
    /// Ordered probe chain. Certified accelerators follow the platform policy;
    /// CPU is always the final entry.
    pub candidates: Vec<ResolvedAnimeRuntime>,
}

impl AnimeRuntimeSelection {
    pub fn preferred(&self) -> &ResolvedAnimeRuntime {
        self.candidates
            .first()
            .expect("validated runtime selection is non-empty")
    }

    pub fn cpu_fallback(&self) -> &ResolvedAnimeRuntime {
        self.candidates
            .last()
            .expect("validated runtime selection is non-empty")
    }

    /// Reduces the probe chain to the single highest-priority accelerator and
    /// the mandatory CPU fallback. CPU-only selections remain a single entry.
    /// This is also the exact artifact set that should be staged/downloaded.
    pub fn preferred_with_cpu_fallback(&self) -> Self {
        let preferred = self.preferred().clone();
        let cpu = self.cpu_fallback().clone();
        let candidates = if preferred == cpu {
            vec![preferred]
        } else {
            vec![preferred, cpu]
        };
        Self { candidates }
    }

    /// Returns one isolated probe selection per runtime in policy order. The
    /// resolver guarantees that CPU is last, so callers cannot accidentally
    /// accept CPU before trying a qualified secondary accelerator.
    pub fn ordered_probe_attempts(&self) -> Vec<Self> {
        self.candidates
            .iter()
            .cloned()
            .map(|candidate| Self {
                candidates: vec![candidate],
            })
            .collect()
    }

    pub fn unique_artifacts(&self) -> Vec<&AnimeRuntimeArtifactManifest> {
        let mut seen = BTreeSet::new();
        self.candidates
            .iter()
            .map(|candidate| &candidate.artifact)
            .filter(|runtime| seen.insert(runtime.artifact_key()))
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QualifiedAnimeBundleApproval {
    pub bundle_version: String,
    /// Canonical fingerprint of the complete strict manifest. Binding the gate
    /// to only the model would leave runtime hashes outside qualification.
    pub manifest_fingerprint: String,
    pub model_sha256: String,
    pub qualification_report_fingerprint: String,
    /// Exact physical-certification bindings permitted to activate this
    /// bundle. An empty list is deliberately valid but permits no production
    /// activation; deterministic matching remains available.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub certified_runtime_profiles: Vec<QualifiedAnimeRuntimeProfileApproval>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QualifiedAnimeRuntimeProfileApproval {
    /// Stable fingerprint of the physical host/inference inventory used by
    /// both certification and runtime activation.
    pub host_fingerprint: String,
    pub runtime_artifact_key: String,
    pub runtime_artifact_sha256: String,
    pub execution_backend: AnimeExecutionBackend,
    /// Fingerprints of the passing sealed profile and its retained physical
    /// certification report. The enclosing bundle approval binds both.
    pub certified_profile_fingerprint: String,
    pub certification_report_fingerprint: String,
}

#[derive(Debug, Clone)]
pub enum AnimeBundleQualificationGate {
    Production {
        approvals: Vec<QualifiedAnimeBundleApproval>,
    },
    /// This bypass is intentionally named and must only be selected by a
    /// support/development configuration path outside this module.
    DevelopmentAllowUnqualified,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnimeArtifactUrlPolicy {
    HttpsOnly,
    DevelopmentAllowHttp,
}

#[derive(Debug, Clone)]
pub struct AnimeBundleCompatibilityPolicy {
    pub server_version: Version,
    pub qualification_gate: AnimeBundleQualificationGate,
    pub artifact_url_policy: AnimeArtifactUrlPolicy,
    pub require_complete_platform_matrix: bool,
}

impl AnimeBundleCompatibilityPolicy {
    pub fn production(
        server_version: Version,
        approvals: Vec<QualifiedAnimeBundleApproval>,
    ) -> Self {
        Self {
            server_version,
            qualification_gate: AnimeBundleQualificationGate::Production { approvals },
            artifact_url_policy: AnimeArtifactUrlPolicy::HttpsOnly,
            require_complete_platform_matrix: true,
        }
    }

    pub fn development(server_version: Version) -> Self {
        Self {
            server_version,
            qualification_gate: AnimeBundleQualificationGate::DevelopmentAllowUnqualified,
            artifact_url_policy: AnimeArtifactUrlPolicy::DevelopmentAllowHttp,
            require_complete_platform_matrix: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ValidatedAnimeBundle {
    manifest: AnimeInferenceBundleManifest,
    manifest_fingerprint: String,
    /// `None` is the explicit development bypass. Production always stores
    /// `Some`, including an empty list that activates nowhere.
    certified_runtime_profiles: Option<Vec<QualifiedAnimeRuntimeProfileApproval>>,
}

impl ValidatedAnimeBundle {
    pub fn manifest(&self) -> &AnimeInferenceBundleManifest {
        &self.manifest
    }

    pub fn manifest_fingerprint(&self) -> &str {
        &self.manifest_fingerprint
    }

    pub fn into_manifest(self) -> AnimeInferenceBundleManifest {
        self.manifest
    }

    /// Intersects ordinary platform compatibility with the physical profiles
    /// bound by the qualification approval. Production activation also
    /// requires an approved CPU fallback, so a partially certified accelerator
    /// chain cannot strand the deterministic path behind a failed worker.
    pub fn certified_runtime_selection(
        &self,
        host_fingerprint: &str,
        selection: &AnimeRuntimeSelection,
    ) -> Option<AnimeRuntimeSelection> {
        let Some(certified) = self.certified_runtime_profiles.as_ref() else {
            return Some(selection.clone());
        };
        let candidates = selection
            .candidates
            .iter()
            .filter(|candidate| {
                certified.iter().any(|approval| {
                    sha256_eq(&approval.host_fingerprint, host_fingerprint)
                        && approval.runtime_artifact_key == candidate.artifact.artifact_key()
                        && sha256_eq(
                            &approval.runtime_artifact_sha256,
                            &candidate.artifact.sha256,
                        )
                        && approval.execution_backend == candidate.execution_backend
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        candidates
            .last()
            .is_some_and(|candidate| {
                candidate.execution_backend == AnimeExecutionBackend::Cpu
                    && candidate.device_id.is_none()
                    && candidate.artifact.supports_cpu_execution()
            })
            .then_some(AnimeRuntimeSelection { candidates })
    }
}

pub fn validate_anime_bundle(
    manifest: AnimeInferenceBundleManifest,
    policy: &AnimeBundleCompatibilityPolicy,
) -> Result<ValidatedAnimeBundle> {
    ensure!(
        manifest.schema_version == ANIME_BUNDLE_SCHEMA_VERSION,
        "unsupported anime bundle schema version {}",
        manifest.schema_version
    );
    ensure!(
        manifest.protocol_version == ANIME_INFERENCE_PROTOCOL_VERSION,
        "unsupported anime inference protocol version {}",
        manifest.protocol_version
    );
    ensure!(
        manifest.matcher_schema_version == ANIME_MATCHER_SCHEMA_VERSION,
        "unsupported anime matcher schema version {}",
        manifest.matcher_schema_version
    );
    validate_release_version(&manifest.bundle_version, "bundleVersion")?;
    let minimum_server = Version::parse(&manifest.minimum_server_version)
        .context("minimumServerVersion is not valid semantic versioning")?;
    ensure!(
        policy.server_version >= minimum_server,
        "bundle requires server {}, current server is {}",
        minimum_server,
        policy.server_version
    );
    validate_component(&manifest.worker_revision, "workerRevision")?;

    validate_model_manifest(&manifest.model, policy.artifact_url_policy)?;
    validate_runtime_policy(&manifest.runtime_policy)?;
    ensure!(
        !manifest.runtimes.is_empty() && manifest.runtimes.len() <= MAX_RUNTIME_ARTIFACTS,
        "bundle must contain between 1 and {MAX_RUNTIME_ARTIFACTS} runtime artifacts"
    );

    let mut artifact_keys = HashSet::new();
    let mut compatibility_keys = HashSet::new();
    for runtime in &manifest.runtimes {
        validate_runtime_manifest(runtime, policy.artifact_url_policy)?;
        ensure!(
            artifact_keys.insert(runtime.artifact_key()),
            "duplicate runtime artifact key '{}'",
            runtime.artifact_key()
        );
        let compatibility_key = (
            runtime.os,
            runtime.arch,
            runtime.device_class,
            runtime.backend,
            runtime.priority,
        );
        ensure!(
            compatibility_keys.insert(compatibility_key),
            "ambiguous runtime entries share OS, architecture, device class, backend, and priority"
        );
    }

    if policy.require_complete_platform_matrix {
        validate_required_platform_matrix(&manifest.runtimes)?;
    }
    let encoded = serde_json::to_vec(&manifest).context("encoding validated bundle manifest")?;
    let manifest_fingerprint = sha256_prefixed(&encoded);
    let certified_runtime_profiles =
        validate_qualification(&manifest, &manifest_fingerprint, &policy.qualification_gate)?;
    Ok(ValidatedAnimeBundle {
        manifest,
        manifest_fingerprint,
        certified_runtime_profiles,
    })
}

fn validate_model_manifest(
    model: &AnimeModelArtifactManifest,
    url_policy: AnimeArtifactUrlPolicy,
) -> Result<()> {
    validate_component(&model.id, "model.id")?;
    validate_component(&model.revision, "model.revision")?;
    validate_nonempty_bounded(&model.upstream_model_id, "model.upstreamModelId", 256)?;
    validate_commit_revision(&model.upstream_revision, "model.upstreamRevision")?;
    validate_nonempty_bounded(&model.license, "model.license", 128)?;
    validate_nonempty_bounded(&model.quantization, "model.quantization", 64)?;
    ensure!(
        (1..=512).contains(&model.transformer_layers),
        "model.transformerLayers must be between 1 and 512"
    );
    ensure!(
        (1_024..=131_072).contains(&model.context_tokens),
        "model.contextTokens is outside the supported range"
    );
    ensure!(
        model.max_output_tokens > 0 && model.max_output_tokens < model.context_tokens,
        "model.maxOutputTokens must be non-zero and smaller than contextTokens"
    );
    validate_component(&model.chat_template_revision, "model.chatTemplateRevision")?;
    validate_commit_revision(
        &model.conversion_tool_revision,
        "model.conversionToolRevision",
    )?;
    validate_sha256(
        &model.qualification_report_fingerprint,
        "model.qualificationReportFingerprint",
    )?;
    validate_artifact_url(&model.url, url_policy, "model.url")?;
    validate_sha256(&model.sha256, "model.sha256")?;
    ensure!(model.size_bytes > 0, "model.sizeBytes must be non-zero");
    Ok(())
}

fn validate_runtime_policy(policy: &AnimeRuntimePolicyManifest) -> Result<()> {
    validate_component(
        &policy.sampling_profile_revision,
        "runtimePolicy.samplingProfileRevision",
    )?;
    ensure!(
        policy.parallel == 1,
        "V1 runtimePolicy.parallel must be exactly 1"
    );
    ensure!(
        (30..=86_400).contains(&policy.idle_unload_seconds),
        "runtimePolicy.idleUnloadSeconds must be between 30 and 86400"
    );
    Ok(())
}

fn validate_runtime_manifest(
    runtime: &AnimeRuntimeArtifactManifest,
    url_policy: AnimeArtifactUrlPolicy,
) -> Result<()> {
    validate_component(&runtime.revision, "runtime.revision")?;
    validate_numeric_version(&runtime.minimum_os_version, "runtime.minimumOsVersion")?;
    ensure!(
        runtime.required_cpu_features.len() <= 64,
        "runtime.requiredCpuFeatures exceeds 64 entries"
    );
    let mut features = BTreeSet::new();
    for feature in &runtime.required_cpu_features {
        let normalized = normalized_cpu_feature(feature)?;
        ensure!(
            features.insert(normalized),
            "runtime.requiredCpuFeatures contains a duplicate"
        );
    }
    if let Some(version) = runtime.minimum_driver_version.as_deref() {
        validate_numeric_version(version, "runtime.minimumDriverVersion")?;
    }
    ensure!(
        runtime.packaged_dependencies.len() <= MAX_PACKAGED_DEPENDENCIES,
        "runtime.packagedDependencies exceeds {MAX_PACKAGED_DEPENDENCIES} entries"
    );
    validate_safe_relative_path(&runtime.entrypoint, "runtime.entrypoint")?;
    let mut dependencies = BTreeSet::new();
    for dependency in &runtime.packaged_dependencies {
        validate_safe_relative_path(dependency, "runtime.packagedDependencies")?;
        ensure!(
            dependencies.insert(dependency),
            "runtime.packagedDependencies contains a duplicate"
        );
    }
    validate_artifact_url(&runtime.url, url_policy, "runtime.url")?;
    validate_sha256(&runtime.sha256, "runtime.sha256")?;
    ensure!(runtime.size_bytes > 0, "runtime.sizeBytes must be non-zero");
    ensure!(
        runtime.installed_size_bytes > 0,
        "runtime.installedSizeBytes must be non-zero"
    );
    if runtime.archive_format == AnimeRuntimeArchiveFormat::Raw {
        ensure!(
            runtime.size_bytes == runtime.installed_size_bytes,
            "raw runtime sizeBytes and installedSizeBytes must match"
        );
    }
    match runtime.backend {
        AnimeRuntimeBackend::Cpu => ensure!(
            runtime.device_class == Some(AnimeDeviceClass::Cpu),
            "CPU runtime must declare deviceClass=cpu"
        ),
        AnimeRuntimeBackend::CudaCpu => ensure!(
            runtime.device_class == Some(AnimeDeviceClass::Nvidia),
            "CUDA runtime must declare deviceClass=nvidia"
        ),
        AnimeRuntimeBackend::HipCpu => ensure!(
            runtime.device_class == Some(AnimeDeviceClass::Amd),
            "HIP runtime must declare deviceClass=amd"
        ),
        AnimeRuntimeBackend::VulkanCpu => ensure!(
            runtime.device_class == Some(AnimeDeviceClass::AnyVulkan),
            "Vulkan runtime must declare deviceClass=any_vulkan"
        ),
        AnimeRuntimeBackend::MetalCpu => ensure!(
            runtime.os == AnimeHostOs::Macos,
            "Metal runtime is only valid on macOS"
        ),
    }
    Ok(())
}

fn validate_required_platform_matrix(runtimes: &[AnimeRuntimeArtifactManifest]) -> Result<()> {
    let required = [
        (
            AnimeHostOs::Macos,
            AnimeHostArch::Aarch64,
            AnimeRuntimeBackend::MetalCpu,
        ),
        (
            AnimeHostOs::Macos,
            AnimeHostArch::X86_64,
            AnimeRuntimeBackend::MetalCpu,
        ),
        (
            AnimeHostOs::Windows,
            AnimeHostArch::X86_64,
            AnimeRuntimeBackend::CudaCpu,
        ),
        (
            AnimeHostOs::Windows,
            AnimeHostArch::X86_64,
            AnimeRuntimeBackend::VulkanCpu,
        ),
        (
            AnimeHostOs::Windows,
            AnimeHostArch::X86_64,
            AnimeRuntimeBackend::Cpu,
        ),
        (
            AnimeHostOs::Linux,
            AnimeHostArch::X86_64,
            AnimeRuntimeBackend::CudaCpu,
        ),
        (
            AnimeHostOs::Linux,
            AnimeHostArch::X86_64,
            AnimeRuntimeBackend::HipCpu,
        ),
        (
            AnimeHostOs::Linux,
            AnimeHostArch::X86_64,
            AnimeRuntimeBackend::VulkanCpu,
        ),
        (
            AnimeHostOs::Linux,
            AnimeHostArch::X86_64,
            AnimeRuntimeBackend::Cpu,
        ),
        (
            AnimeHostOs::Linux,
            AnimeHostArch::Aarch64,
            AnimeRuntimeBackend::Cpu,
        ),
    ];
    for (os, arch, backend) in required {
        ensure!(
            runtimes.iter().any(|runtime| {
                runtime.os == os && runtime.arch == arch && runtime.backend == backend
            }),
            "production bundle is missing required {}-{}-{} runtime",
            os.as_str(),
            arch.as_str(),
            backend.as_str()
        );
    }
    Ok(())
}

fn validate_qualification(
    manifest: &AnimeInferenceBundleManifest,
    manifest_fingerprint: &str,
    gate: &AnimeBundleQualificationGate,
) -> Result<Option<Vec<QualifiedAnimeRuntimeProfileApproval>>> {
    match gate {
        AnimeBundleQualificationGate::DevelopmentAllowUnqualified => Ok(None),
        AnimeBundleQualificationGate::Production { approvals } => {
            let matching = approvals
                .iter()
                .filter(|approval| {
                    approval.bundle_version == manifest.bundle_version
                        && sha256_eq(&approval.manifest_fingerprint, manifest_fingerprint)
                        && sha256_eq(&approval.model_sha256, &manifest.model.sha256)
                        && sha256_eq(
                            &approval.qualification_report_fingerprint,
                            &manifest.model.qualification_report_fingerprint,
                        )
                })
                .collect::<Vec<_>>();
            ensure!(
                !matching.is_empty(),
                "bundle is not approved by this server release's qualification gate"
            );
            let mut certified = BTreeMap::new();
            for approval in matching {
                for profile in &approval.certified_runtime_profiles {
                    validate_certified_runtime_profile(manifest, profile)?;
                    let key = (
                        normalize_sha256(&profile.host_fingerprint),
                        profile.runtime_artifact_key.clone(),
                        profile.execution_backend,
                    );
                    if let Some(existing) = certified.insert(key, profile.clone()) {
                        ensure!(
                            existing == *profile,
                            "conflicting certified runtime profiles share one activation identity"
                        );
                    }
                }
            }
            Ok(Some(certified.into_values().collect()))
        }
    }
}

fn validate_certified_runtime_profile(
    manifest: &AnimeInferenceBundleManifest,
    profile: &QualifiedAnimeRuntimeProfileApproval,
) -> Result<()> {
    validate_sha256(
        &profile.host_fingerprint,
        "certifiedRuntimeProfiles.hostFingerprint",
    )?;
    validate_component(
        &profile.runtime_artifact_key,
        "certifiedRuntimeProfiles.runtimeArtifactKey",
    )?;
    validate_sha256(
        &profile.runtime_artifact_sha256,
        "certifiedRuntimeProfiles.runtimeArtifactSha256",
    )?;
    validate_sha256(
        &profile.certified_profile_fingerprint,
        "certifiedRuntimeProfiles.certifiedProfileFingerprint",
    )?;
    validate_sha256(
        &profile.certification_report_fingerprint,
        "certifiedRuntimeProfiles.certificationReportFingerprint",
    )?;
    let runtime = manifest
        .runtimes
        .iter()
        .find(|runtime| runtime.artifact_key() == profile.runtime_artifact_key)
        .ok_or_else(|| {
            anyhow!("certified runtime profile references an absent runtime artifact")
        })?;
    ensure!(
        sha256_eq(&runtime.sha256, &profile.runtime_artifact_sha256),
        "certified runtime profile artifact SHA-256 differs from the manifest"
    );
    let supports_execution = match profile.execution_backend {
        AnimeExecutionBackend::Cpu => runtime.supports_cpu_execution(),
        AnimeExecutionBackend::Metal => runtime.backend == AnimeRuntimeBackend::MetalCpu,
        AnimeExecutionBackend::Cuda => runtime.backend == AnimeRuntimeBackend::CudaCpu,
        AnimeExecutionBackend::Hip => runtime.backend == AnimeRuntimeBackend::HipCpu,
        AnimeExecutionBackend::Vulkan => runtime.backend == AnimeRuntimeBackend::VulkanCpu,
    };
    ensure!(
        supports_execution,
        "certified runtime profile execution backend is incompatible with its artifact"
    );
    Ok(())
}

pub fn resolve_anime_runtime(
    bundle: &ValidatedAnimeBundle,
    host: &AnimeInferenceHost,
) -> Result<AnimeRuntimeSelection> {
    let compatible: Vec<&AnimeRuntimeArtifactManifest> = bundle
        .manifest
        .runtimes
        .iter()
        .filter(|runtime| base_runtime_compatible(runtime, host))
        .collect();
    ensure!(
        !compatible.is_empty(),
        "bundle has no runtime compatible with {} {}",
        host.os.as_str(),
        host.arch.as_str()
    );

    let cpu_artifact = compatible
        .iter()
        .copied()
        .filter(|runtime| runtime.supports_cpu_execution())
        .min_by_key(|runtime| cpu_fallback_rank(runtime, host.os))
        .ok_or_else(|| anyhow!("compatible bundle does not retain a CPU fallback"))?;
    let cpu_fallback = ResolvedAnimeRuntime {
        artifact: cpu_artifact.clone(),
        execution_backend: AnimeExecutionBackend::Cpu,
        device_id: None,
    };

    let mut accelerated = Vec::new();
    for runtime in compatible {
        let Some(accelerator) = runtime.backend.accelerator() else {
            continue;
        };
        for device in &host.devices {
            if host.containerized && !device.exposed_to_container {
                continue;
            }
            if !device.certified_backends.contains(&accelerator)
                || !device_matches_runtime(device, runtime)
                || !driver_compatible(device.driver_version.as_deref(), runtime)
                || !device_memory_compatible(device.available_memory_bytes, runtime)
            {
                continue;
            }
            accelerated.push((
                accelerator_rank(host, device.vendor, accelerator),
                runtime.priority,
                runtime.artifact_key(),
                ResolvedAnimeRuntime {
                    artifact: runtime.clone(),
                    execution_backend: execution_backend(accelerator),
                    device_id: Some(device.id.clone()),
                },
            ));
        }
    }
    accelerated
        .sort_by(|left, right| (&left.0, &left.1, &left.2).cmp(&(&right.0, &right.1, &right.2)));
    let mut candidates = Vec::new();
    let mut seen = BTreeSet::new();
    for (_, _, _, candidate) in accelerated {
        let key = (
            candidate.execution_backend.as_str(),
            candidate.artifact.artifact_key(),
            candidate.device_id.clone(),
        );
        if seen.insert(key) {
            candidates.push(candidate);
        }
    }
    candidates.push(cpu_fallback);
    Ok(AnimeRuntimeSelection { candidates })
}

fn base_runtime_compatible(
    runtime: &AnimeRuntimeArtifactManifest,
    host: &AnimeInferenceHost,
) -> bool {
    runtime.os == host.os
        && runtime.arch == host.arch
        && version_at_least(
            host.os_version.as_deref(),
            Some(runtime.minimum_os_version.as_str()),
        )
        && runtime.required_cpu_features.iter().all(|feature| {
            normalized_cpu_feature(feature)
                .ok()
                .is_some_and(|feature| host.cpu_features.contains(&feature))
        })
}

fn cpu_fallback_rank(runtime: &AnimeRuntimeArtifactManifest, os: AnimeHostOs) -> (u8, u16, String) {
    let class = match runtime.backend {
        AnimeRuntimeBackend::Cpu => 0,
        AnimeRuntimeBackend::MetalCpu if os == AnimeHostOs::Macos => 1,
        _ => 2,
    };
    (class, runtime.priority, runtime.artifact_key())
}

fn device_matches_runtime(
    device: &AnimeInferenceDevice,
    runtime: &AnimeRuntimeArtifactManifest,
) -> bool {
    match runtime.device_class {
        None => true,
        Some(AnimeDeviceClass::Nvidia) => device.vendor == AnimeGpuVendor::Nvidia,
        Some(AnimeDeviceClass::Amd) => device.vendor == AnimeGpuVendor::Amd,
        Some(AnimeDeviceClass::Intel) => device.vendor == AnimeGpuVendor::Intel,
        Some(AnimeDeviceClass::Apple) => device.vendor == AnimeGpuVendor::Apple,
        Some(AnimeDeviceClass::AnyVulkan) => device
            .certified_backends
            .contains(&AnimeAcceleratorBackend::Vulkan),
        Some(AnimeDeviceClass::Cpu) => false,
    }
}

fn driver_compatible(driver_version: Option<&str>, runtime: &AnimeRuntimeArtifactManifest) -> bool {
    // Vulkan driver versions are not one portable ordering domain: Windows
    // WDDM packages, Mesa, and NVIDIA all expose unrelated number schemes.
    // The signed OS floor plus disposable runtime smoke probe is authoritative.
    if runtime.backend == AnimeRuntimeBackend::VulkanCpu {
        return true;
    }
    let Some(minimum) = runtime.minimum_driver_version.as_deref() else {
        return true;
    };
    // Known numeric evidence is enforced before any worker is launched.
    // Missing or platform-specific/unparseable evidence is allowed only into
    // the disposable probe path, where the real packaged runtime proves
    // compatibility. Hardware policy separately forbids reusing an accelerated
    // cached profile when this comparable evidence is absent.
    match (
        parse_numeric_version(driver_version.unwrap_or_default()),
        parse_numeric_version(minimum),
    ) {
        (Some(mut actual), Some(mut minimum)) => {
            let length = actual.len().max(minimum.len());
            actual.resize(length, 0);
            minimum.resize(length, 0);
            actual >= minimum
        }
        _ => true,
    }
}

fn parse_numeric_version(value: &str) -> Option<Vec<u64>> {
    if value.is_empty() {
        return None;
    }
    value
        .split('.')
        .map(|part| part.parse::<u64>().ok())
        .collect()
}

fn device_memory_compatible(
    available_memory_bytes: Option<u64>,
    runtime: &AnimeRuntimeArtifactManifest,
) -> bool {
    runtime.minimum_device_memory_bytes == 0
        || available_memory_bytes
            .is_some_and(|available| available >= runtime.minimum_device_memory_bytes)
}

fn accelerator_rank(
    host: &AnimeInferenceHost,
    vendor: AnimeGpuVendor,
    backend: AnimeAcceleratorBackend,
) -> u8 {
    match (host.os, host.arch, vendor, backend) {
        (AnimeHostOs::Macos, _, _, AnimeAcceleratorBackend::Metal) => 0,
        (AnimeHostOs::Windows, _, AnimeGpuVendor::Nvidia, AnimeAcceleratorBackend::Cuda) => 0,
        (AnimeHostOs::Windows, _, _, AnimeAcceleratorBackend::Vulkan) => 1,
        (AnimeHostOs::Linux, _, AnimeGpuVendor::Nvidia, AnimeAcceleratorBackend::Cuda) => 0,
        (AnimeHostOs::Linux, _, AnimeGpuVendor::Amd, AnimeAcceleratorBackend::Hip) => 0,
        (AnimeHostOs::Linux, _, _, AnimeAcceleratorBackend::Vulkan) => 1,
        _ => 100,
    }
}

fn execution_backend(backend: AnimeAcceleratorBackend) -> AnimeExecutionBackend {
    match backend {
        AnimeAcceleratorBackend::Metal => AnimeExecutionBackend::Metal,
        AnimeAcceleratorBackend::Cuda => AnimeExecutionBackend::Cuda,
        AnimeAcceleratorBackend::Hip => AnimeExecutionBackend::Hip,
        AnimeAcceleratorBackend::Vulkan => AnimeExecutionBackend::Vulkan,
    }
}

#[derive(Debug, Clone)]
pub struct AnimeBundlePaths {
    root: PathBuf,
}

impl AnimeBundlePaths {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn active_bundle(&self) -> PathBuf {
        self.root.join("active-bundle.json")
    }

    pub fn previous_bundle(&self) -> PathBuf {
        self.root.join("previous-bundle.json")
    }

    pub fn cached_manifest(&self) -> PathBuf {
        self.root.join("bundle-manifest-cache.json")
    }

    pub fn manifests(&self) -> PathBuf {
        self.root.join("manifests").join("sha256")
    }

    pub fn pending_activation(&self) -> PathBuf {
        self.root.join("pending-activation.json")
    }

    pub fn active_runtime_profile(&self) -> PathBuf {
        self.root.join("active-runtime-profile.json")
    }

    pub fn previous_runtime_profile(&self) -> PathBuf {
        self.root.join("previous-runtime-profile.json")
    }

    pub fn models(&self) -> PathBuf {
        self.root.join("models")
    }

    pub fn runtimes(&self) -> PathBuf {
        self.root.join("runtimes")
    }

    pub fn staging(&self) -> PathBuf {
        self.root.join("staging")
    }

    fn manifest_by_fingerprint(&self, fingerprint: &str) -> Result<PathBuf> {
        validate_sha256(fingerprint, "manifest fingerprint")?;
        let normalized = normalize_sha256(fingerprint);
        let digest = normalized
            .strip_prefix("sha256:")
            .expect("normalized SHA-256 always has its prefix");
        Ok(self.manifests().join(format!("{digest}.json")))
    }

    fn model_install_root(&self, model: &AnimeModelArtifactManifest) -> PathBuf {
        // The install key commits to the complete logical model identity and
        // its verified bytes. A publisher reusing id+revision with different
        // bytes therefore receives a different immutable directory instead
        // of overwriting the artifact retained by the rollback descriptor.
        // Existing descriptors keep their stored relative path and remain
        // readable; only newly staged installations use this namespace.
        let mut hasher = Sha256::new();
        hasher.update(model.id.as_bytes());
        hasher.update([0]);
        hasher.update(model.revision.as_bytes());
        hasher.update([0]);
        hasher.update(normalize_sha256(&model.sha256).as_bytes());
        let install_key = format!("{:x}", hasher.finalize());
        self.models().join(&model.id).join(install_key)
    }

    fn runtime_install_root(&self, runtime: &AnimeRuntimeArtifactManifest) -> PathBuf {
        self.runtimes()
            .join(&runtime.revision)
            .join(runtime.artifact_key())
    }

    fn runtime_replacement_install_root(&self, runtime: &AnimeRuntimeArtifactManifest) -> PathBuf {
        let normalized = normalize_sha256(&runtime.sha256);
        let digest = normalized
            .strip_prefix("sha256:")
            .expect("normalized SHA-256 always has its prefix");
        self.runtimes().join(&runtime.revision).join(format!(
            "replacement-{}-{}",
            &digest[..16],
            Uuid::new_v4()
        ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnimeRuntimeProfile {
    pub schema_version: u32,
    pub bundle_version: String,
    pub model_id: String,
    pub model_revision: String,
    pub worker_revision: String,
    pub runtime_artifact_key: String,
    pub host_fingerprint: String,
    pub execution_backend: AnimeExecutionBackend,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device_id: Option<String>,
    pub gpu_layer_count: u32,
    pub cpu_thread_count: u16,
    pub kv_cache_type: AnimeKvCacheType,
    pub load_time_ms: u64,
    pub warm_latency_ms: u64,
    pub peak_rss_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peak_device_memory_bytes: Option<u64>,
    pub probe_result: AnimeRuntimeProbeResult,
    pub probed_at: String,
    pub profile_fingerprint: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AnimeRuntimeProbeResult {
    GpuBalanced,
    CpuBalanced,
    DeterministicOnly,
}

impl AnimeRuntimeProfile {
    pub fn seal(mut self) -> Result<Self> {
        self.profile_fingerprint.clear();
        self.validate_shape()?;
        self.profile_fingerprint = self.computed_fingerprint()?;
        Ok(self)
    }

    pub fn validate(&self) -> Result<()> {
        self.validate_shape()?;
        ensure!(
            sha256_eq(&self.profile_fingerprint, &self.computed_fingerprint()?),
            "runtime profile fingerprint does not match its content"
        );
        Ok(())
    }

    fn validate_shape(&self) -> Result<()> {
        ensure!(
            self.schema_version == ANIME_RUNTIME_PROFILE_SCHEMA_VERSION,
            "unsupported runtime profile schema version {}",
            self.schema_version
        );
        validate_release_version(&self.bundle_version, "profile.bundleVersion")?;
        validate_component(&self.model_id, "profile.modelId")?;
        validate_component(&self.model_revision, "profile.modelRevision")?;
        validate_component(&self.worker_revision, "profile.workerRevision")?;
        validate_component(&self.runtime_artifact_key, "profile.runtimeArtifactKey")?;
        validate_sha256(&self.host_fingerprint, "profile.hostFingerprint")?;
        ensure!(
            (1..=256).contains(&self.cpu_thread_count),
            "profile.cpuThreadCount must be between 1 and 256"
        );
        match self.probe_result {
            AnimeRuntimeProbeResult::GpuBalanced => {
                ensure!(
                    self.execution_backend != AnimeExecutionBackend::Cpu
                        && self.device_id.as_deref().is_some_and(|id| !id.is_empty())
                        && self.gpu_layer_count > 0,
                    "GPU-balanced profile requires an accelerator, device, and non-zero GPU layers"
                );
            }
            AnimeRuntimeProbeResult::CpuBalanced => {
                ensure!(
                    self.execution_backend == AnimeExecutionBackend::Cpu
                        && self.device_id.is_none()
                        && self.gpu_layer_count == 0,
                    "CPU-balanced profile must use CPU with no device or GPU layers"
                );
            }
            AnimeRuntimeProbeResult::DeterministicOnly => {
                ensure!(
                    self.gpu_layer_count == 0,
                    "deterministic-only profile cannot retain GPU layers"
                );
            }
        }
        DateTime::parse_from_rfc3339(&self.probed_at).context("profile.probedAt is not RFC3339")?;
        if !self.profile_fingerprint.is_empty() {
            validate_sha256(&self.profile_fingerprint, "profile.profileFingerprint")?;
        }
        Ok(())
    }

    fn computed_fingerprint(&self) -> Result<String> {
        let mut clone = self.clone();
        clone.profile_fingerprint.clear();
        let bytes = serde_json::to_vec(&clone).context("encoding runtime profile fingerprint")?;
        Ok(sha256_prefixed(&bytes))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InstalledAnimeModel {
    pub id: String,
    pub revision: String,
    pub sha256: String,
    pub size_bytes: u64,
    pub relative_file: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InstalledAnimeRuntime {
    pub artifact_key: String,
    pub revision: String,
    pub sha256: String,
    pub archive_size_bytes: u64,
    pub installed_size_bytes: u64,
    pub relative_root: String,
    pub relative_entrypoint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ActiveAnimeBundleDescriptor {
    pub schema_version: u32,
    pub manifest_fingerprint: String,
    pub bundle_version: String,
    pub protocol_version: u32,
    pub matcher_schema_version: u32,
    pub model: InstalledAnimeModel,
    pub runtimes: Vec<InstalledAnimeRuntime>,
    pub profile: AnimeRuntimeProfile,
    pub activated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CachedAnimeBundleManifest {
    schema_version: u32,
    manifest_fingerprint: String,
    manifest: AnimeInferenceBundleManifest,
    cached_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StoredAnimeBundleManifest {
    schema_version: u32,
    manifest_fingerprint: String,
    manifest: AnimeInferenceBundleManifest,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PendingAnimeBundleActivation {
    schema_version: u32,
    activation_id: String,
    active: ActiveAnimeBundleDescriptor,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    prior_active: Option<ActiveAnimeBundleDescriptor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    prior_previous: Option<ActiveAnimeBundleDescriptor>,
    created_at: String,
}

/// Exact receipt for the on-disk pending activation transaction. The receipt
/// prevents a delayed worker-success callback from clearing a newer pending
/// activation, even when both activations name identical artifact content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnimePendingActivationToken {
    activation_id: String,
    descriptor: ActiveAnimeBundleDescriptor,
}

impl AnimePendingActivationToken {
    pub fn descriptor(&self) -> &ActiveAnimeBundleDescriptor {
        &self.descriptor
    }
}

impl ActiveAnimeBundleDescriptor {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.schema_version == ANIME_BUNDLE_SCHEMA_VERSION,
            "unsupported active bundle descriptor schema"
        );
        validate_sha256(&self.manifest_fingerprint, "descriptor.manifestFingerprint")?;
        validate_release_version(&self.bundle_version, "descriptor.bundleVersion")?;
        ensure!(
            self.protocol_version == ANIME_INFERENCE_PROTOCOL_VERSION
                && self.matcher_schema_version == ANIME_MATCHER_SCHEMA_VERSION,
            "active bundle descriptor protocol is incompatible"
        );
        validate_component(&self.model.id, "descriptor.model.id")?;
        validate_component(&self.model.revision, "descriptor.model.revision")?;
        validate_sha256(&self.model.sha256, "descriptor.model.sha256")?;
        ensure!(self.model.size_bytes > 0, "descriptor model size is zero");
        validate_safe_relative_path(&self.model.relative_file, "descriptor.model.relativeFile")?;
        ensure!(
            !self.runtimes.is_empty(),
            "active descriptor contains no runtimes"
        );
        let mut keys = BTreeSet::new();
        for runtime in &self.runtimes {
            validate_component(&runtime.artifact_key, "descriptor.runtime.artifactKey")?;
            ensure!(
                keys.insert(&runtime.artifact_key),
                "active descriptor repeats a runtime artifact"
            );
            validate_component(&runtime.revision, "descriptor.runtime.revision")?;
            validate_sha256(&runtime.sha256, "descriptor.runtime.sha256")?;
            ensure!(
                runtime.archive_size_bytes > 0 && runtime.installed_size_bytes > 0,
                "descriptor runtime size is zero"
            );
            validate_safe_relative_path(&runtime.relative_root, "descriptor.runtime.relativeRoot")?;
            validate_safe_relative_path(
                &runtime.relative_entrypoint,
                "descriptor.runtime.relativeEntrypoint",
            )?;
        }
        ensure!(
            keys.contains(&self.profile.runtime_artifact_key),
            "active profile references a runtime absent from the descriptor"
        );
        ensure!(
            self.profile.bundle_version == self.bundle_version
                && self.profile.model_id == self.model.id
                && self.profile.model_revision == self.model.revision,
            "active profile identity does not match descriptor"
        );
        self.profile.validate()?;
        DateTime::parse_from_rfc3339(&self.activated_at)
            .context("descriptor.activatedAt is not RFC3339")?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InstalledArtifactMarker {
    schema_version: u32,
    kind: String,
    artifact_key: String,
    sha256: String,
    downloaded_size_bytes: u64,
    installed_size_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct ArtifactDownloadSpec {
    pub label: String,
    pub url: String,
    pub sha256: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedArtifactDownload {
    pub sha256: String,
    pub size_bytes: u64,
}

#[async_trait]
pub trait AnimeArtifactFetcher: Send + Sync {
    async fn fetch(
        &self,
        spec: &ArtifactDownloadSpec,
        destination: &Path,
    ) -> Result<VerifiedArtifactDownload>;
}

#[derive(Clone)]
pub struct ReqwestAnimeArtifactFetcher {
    client: Client,
}

impl ReqwestAnimeArtifactFetcher {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl AnimeArtifactFetcher for ReqwestAnimeArtifactFetcher {
    async fn fetch(
        &self,
        spec: &ArtifactDownloadSpec,
        destination: &Path,
    ) -> Result<VerifiedArtifactDownload> {
        validate_sha256(&spec.sha256, "download sha256")?;
        ensure!(spec.size_bytes > 0, "download size must be non-zero");
        let response = self
            .client
            .get(&spec.url)
            .send()
            .await
            .with_context(|| format!("downloading {}", spec.label))?
            .error_for_status()
            .with_context(|| format!("downloading {}", spec.label))?;
        if let Some(length) = response.content_length() {
            ensure!(
                length == spec.size_bytes,
                "{} Content-Length mismatch: expected {}, received {}",
                spec.label,
                spec.size_bytes,
                length
            );
        }
        write_verified_stream(
            response.bytes_stream(),
            destination,
            &spec.sha256,
            spec.size_bytes,
        )
        .await
        .with_context(|| format!("staging {}", spec.label))
    }
}

async fn write_verified_stream<S, B, E>(
    mut stream: S,
    destination: &Path,
    expected_sha256: &str,
    expected_size: u64,
) -> Result<VerifiedArtifactDownload>
where
    S: Stream<Item = std::result::Result<B, E>> + Unpin,
    B: AsRef<[u8]>,
    E: std::error::Error + Send + Sync + 'static,
{
    if let Some(parent) = destination.parent() {
        async_fs::create_dir_all(parent)
            .await
            .with_context(|| format!("creating artifact directory '{}'", parent.display()))?;
    }
    let file_name = destination
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow!("artifact destination has no valid file name"))?;
    let partial = destination.with_file_name(format!(".{file_name}.{}.partial", Uuid::new_v4()));
    let result = async {
        let mut output = async_fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&partial)
            .await
            .with_context(|| format!("creating partial artifact '{}'", partial.display()))?;
        let mut hasher = Sha256::new();
        let mut received = 0_u64;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context("reading artifact response stream")?;
            let bytes = chunk.as_ref();
            received = received
                .checked_add(bytes.len() as u64)
                .ok_or_else(|| anyhow!("artifact byte count overflow"))?;
            ensure!(
                received <= expected_size,
                "artifact exceeded declared size of {expected_size} bytes"
            );
            hasher.update(bytes);
            output.write_all(bytes).await.context("writing artifact")?;
        }
        output.flush().await.context("flushing artifact")?;
        output.sync_all().await.context("syncing artifact")?;
        ensure!(
            received == expected_size,
            "artifact size mismatch: expected {expected_size}, received {received}"
        );
        let actual_sha256 = format!("sha256:{:x}", hasher.finalize());
        ensure!(
            sha256_eq(&actual_sha256, expected_sha256),
            "artifact SHA-256 mismatch"
        );
        async_fs::rename(&partial, destination)
            .await
            .with_context(|| format!("committing artifact '{}'", destination.display()))?;
        Ok(VerifiedArtifactDownload {
            sha256: actual_sha256,
            size_bytes: received,
        })
    }
    .await;
    if result.is_err() {
        let _ = async_fs::remove_file(&partial).await;
    }
    result
}

pub trait AnimeDiskSpaceProbe: Send + Sync {
    fn available_bytes(&self, path: &Path) -> Result<u64>;
}

#[derive(Debug, Default)]
pub struct NativeAnimeDiskSpaceProbe;

impl AnimeDiskSpaceProbe for NativeAnimeDiskSpaceProbe {
    fn available_bytes(&self, path: &Path) -> Result<u64> {
        native_available_disk_bytes(path)
    }
}

#[derive(Clone)]
pub struct AnimeBundleStore {
    paths: AnimeBundlePaths,
    fetcher: Arc<dyn AnimeArtifactFetcher>,
    disk_space: Arc<dyn AnimeDiskSpaceProbe>,
    transaction_lock: Arc<std::sync::Mutex<()>>,
}

impl AnimeBundleStore {
    pub fn new(root: impl Into<PathBuf>, client: Client) -> Self {
        Self {
            paths: AnimeBundlePaths::new(root),
            fetcher: Arc::new(ReqwestAnimeArtifactFetcher::new(client)),
            disk_space: Arc::new(NativeAnimeDiskSpaceProbe),
            transaction_lock: Arc::new(std::sync::Mutex::new(())),
        }
    }

    pub fn with_dependencies(
        root: impl Into<PathBuf>,
        fetcher: Arc<dyn AnimeArtifactFetcher>,
        disk_space: Arc<dyn AnimeDiskSpaceProbe>,
    ) -> Self {
        Self {
            paths: AnimeBundlePaths::new(root),
            fetcher,
            disk_space,
            transaction_lock: Arc::new(std::sync::Mutex::new(())),
        }
    }

    pub fn paths(&self) -> &AnimeBundlePaths {
        &self.paths
    }

    pub async fn ensure_layout(&self) -> Result<()> {
        let models = self.paths.models();
        let runtimes = self.paths.runtimes();
        let staging = self.paths.staging();
        let manifests = self.paths.manifests();
        for path in [self.paths.root(), &models, &runtimes, &staging, &manifests] {
            async_fs::create_dir_all(path)
                .await
                .with_context(|| format!("creating anime inference path '{}'", path.display()))?;
        }
        Ok(())
    }

    pub fn load_active(&self) -> Result<Option<ActiveAnimeBundleDescriptor>> {
        self.load_descriptor(&self.paths.active_bundle())
    }

    pub fn load_previous(&self) -> Result<Option<ActiveAnimeBundleDescriptor>> {
        self.load_descriptor(&self.paths.previous_bundle())
    }

    pub fn load_cached_manifest(
        &self,
        policy: &AnimeBundleCompatibilityPolicy,
    ) -> Result<Option<ValidatedAnimeBundle>> {
        let Some(cached): Option<CachedAnimeBundleManifest> =
            read_optional_json(&self.paths.cached_manifest())?
        else {
            return Ok(None);
        };
        ensure!(
            cached.schema_version == ANIME_BUNDLE_SCHEMA_VERSION,
            "unsupported cached bundle manifest schema"
        );
        validate_sha256(&cached.manifest_fingerprint, "cached manifest fingerprint")?;
        DateTime::parse_from_rfc3339(&cached.cached_at)
            .context("cached manifest timestamp is not RFC3339")?;
        let validated = validate_anime_bundle(cached.manifest, policy)?;
        ensure!(
            sha256_eq(
                &cached.manifest_fingerprint,
                validated.manifest_fingerprint()
            ),
            "cached bundle manifest fingerprint does not match its content"
        );
        Ok(Some(validated))
    }

    pub fn cache_validated_manifest(&self, bundle: &ValidatedAnimeBundle) -> Result<()> {
        // The keyed copy is the durable source for Active/Previous. The latest
        // cache below is only a discovery/update hint and may be overwritten.
        self.persist_validated_manifest(bundle)?;
        write_atomic_json(
            &self.paths.cached_manifest(),
            &CachedAnimeBundleManifest {
                schema_version: ANIME_BUNDLE_SCHEMA_VERSION,
                manifest_fingerprint: bundle.manifest_fingerprint.clone(),
                manifest: bundle.manifest.clone(),
                cached_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            },
        )
    }

    /// Seeds an immutable, content-addressed copy of a fully validated
    /// manifest. Existing content at the fingerprint must be byte-equivalent
    /// in meaning; it is never replaced by a later latest-cache update.
    pub fn persist_validated_manifest(&self, bundle: &ValidatedAnimeBundle) -> Result<()> {
        self.seed_manifest(
            &bundle.manifest,
            &bundle.manifest_fingerprint,
            "validated bundle manifest",
        )
    }

    /// Loads a specific content-addressed manifest without consulting the
    /// mutable latest-manifest cache.
    pub fn load_manifest_by_fingerprint(
        &self,
        fingerprint: &str,
        policy: &AnimeBundleCompatibilityPolicy,
    ) -> Result<Option<ValidatedAnimeBundle>> {
        let Some(stored) = self.load_stored_manifest(fingerprint)? else {
            return Ok(None);
        };
        let validated = validate_anime_bundle(stored.manifest, policy)?;
        ensure!(
            sha256_eq(fingerprint, validated.manifest_fingerprint()),
            "stored bundle manifest fingerprint changed during validation"
        );
        Ok(Some(validated))
    }

    pub fn load_manifest_for_descriptor(
        &self,
        descriptor: &ActiveAnimeBundleDescriptor,
        policy: &AnimeBundleCompatibilityPolicy,
    ) -> Result<ValidatedAnimeBundle> {
        descriptor.validate()?;
        // Upgrade stores created before content-addressed manifests by seeding
        // only an exact-fingerprint legacy latest cache. Once seeded, all
        // subsequent loads are independent of that mutable cache.
        self.ensure_descriptor_manifest_available(descriptor)?;
        let bundle = self
            .load_manifest_by_fingerprint(&descriptor.manifest_fingerprint, policy)?
            .ok_or_else(|| {
                anyhow!(
                    "manifest {} for committed bundle {} is unavailable",
                    descriptor.manifest_fingerprint,
                    descriptor.bundle_version
                )
            })?;
        ensure!(
            bundle.manifest.bundle_version == descriptor.bundle_version
                && bundle.manifest.protocol_version == descriptor.protocol_version
                && bundle.manifest.matcher_schema_version == descriptor.matcher_schema_version
                && bundle.manifest.model.id == descriptor.model.id
                && bundle.manifest.model.revision == descriptor.model.revision
                && installed_model_matches_manifest(&descriptor.model, &bundle.manifest.model)
                && descriptor.profile.worker_revision == bundle.manifest.worker_revision,
            "stored manifest identity does not match committed bundle descriptor"
        );
        ensure!(
            descriptor.runtimes.iter().all(|installed| {
                bundle
                    .manifest
                    .runtimes
                    .iter()
                    .any(|runtime| installed_runtime_matches_manifest(installed, runtime))
            }),
            "stored manifest does not contain a committed runtime artifact"
        );
        Ok(bundle)
    }

    fn seed_manifest(
        &self,
        manifest: &AnimeInferenceBundleManifest,
        fingerprint: &str,
        source: &str,
    ) -> Result<()> {
        validate_sha256(fingerprint, "manifest fingerprint")?;
        let computed = sha256_prefixed(
            &serde_json::to_vec(manifest).context("encoding content-addressed bundle manifest")?,
        );
        ensure!(
            sha256_eq(fingerprint, &computed),
            "{source} fingerprint does not match its content"
        );
        let stored = StoredAnimeBundleManifest {
            schema_version: ANIME_BUNDLE_SCHEMA_VERSION,
            manifest_fingerprint: normalize_sha256(fingerprint),
            manifest: manifest.clone(),
        };
        let path = self.paths.manifest_by_fingerprint(fingerprint)?;
        if path.exists() {
            let existing = self
                .load_stored_manifest(fingerprint)?
                .ok_or_else(|| anyhow!("stored bundle manifest disappeared while reading"))?;
            ensure!(
                existing == stored,
                "content-addressed bundle manifest is immutable"
            );
            return Ok(());
        }
        write_atomic_json(&path, &stored)?;
        let persisted = self
            .load_stored_manifest(fingerprint)?
            .ok_or_else(|| anyhow!("stored bundle manifest disappeared after commit"))?;
        ensure!(persisted == stored, "stored bundle manifest commit changed");
        Ok(())
    }

    fn load_stored_manifest(&self, fingerprint: &str) -> Result<Option<StoredAnimeBundleManifest>> {
        validate_sha256(fingerprint, "manifest fingerprint")?;
        let path = self.paths.manifest_by_fingerprint(fingerprint)?;
        let Some(stored): Option<StoredAnimeBundleManifest> = read_optional_json(&path)? else {
            return Ok(None);
        };
        ensure!(
            stored.schema_version == ANIME_BUNDLE_SCHEMA_VERSION,
            "unsupported stored bundle manifest schema"
        );
        validate_sha256(&stored.manifest_fingerprint, "stored manifest fingerprint")?;
        let computed = sha256_prefixed(
            &serde_json::to_vec(&stored.manifest)
                .context("encoding stored bundle manifest fingerprint")?,
        );
        ensure!(
            sha256_eq(fingerprint, &stored.manifest_fingerprint)
                && sha256_eq(fingerprint, &computed),
            "stored bundle manifest fingerprint does not match its key or content"
        );
        Ok(Some(stored))
    }

    fn ensure_descriptor_manifest_available(
        &self,
        descriptor: &ActiveAnimeBundleDescriptor,
    ) -> Result<()> {
        if self
            .load_stored_manifest(&descriptor.manifest_fingerprint)?
            .is_some()
        {
            return Ok(());
        }

        // One-time migration for stores created before keyed manifests. Only
        // the exact latest-cache fingerprint can backfill a committed pointer.
        if let Ok(Some(cached)) =
            read_optional_json::<CachedAnimeBundleManifest>(&self.paths.cached_manifest())
            && cached.schema_version == ANIME_BUNDLE_SCHEMA_VERSION
            && sha256_eq(
                &cached.manifest_fingerprint,
                &descriptor.manifest_fingerprint,
            )
        {
            self.seed_manifest(
                &cached.manifest,
                &cached.manifest_fingerprint,
                "legacy cached bundle manifest",
            )?;
            return Ok(());
        }

        bail!(
            "manifest {} for committed bundle {} is unavailable offline",
            descriptor.manifest_fingerprint,
            descriptor.bundle_version
        )
    }

    fn load_pending_activation(&self) -> Result<Option<PendingAnimeBundleActivation>> {
        let Some(pending): Option<PendingAnimeBundleActivation> =
            read_optional_json(&self.paths.pending_activation())?
        else {
            return Ok(None);
        };
        ensure!(
            pending.schema_version == PENDING_ACTIVATION_SCHEMA_VERSION,
            "unsupported pending anime activation schema"
        );
        Uuid::parse_str(&pending.activation_id).context("pending activation ID is not a UUID")?;
        DateTime::parse_from_rfc3339(&pending.created_at)
            .context("pending activation timestamp is not RFC3339")?;
        pending.active.validate()?;
        self.ensure_descriptor_manifest_available(&pending.active)?;
        if let Some(prior) = pending.prior_active.as_ref() {
            prior.validate()?;
            self.ensure_descriptor_manifest_available(prior)?;
            ensure!(
                prior != &pending.active,
                "pending activation cannot name the same prior and active descriptor"
            );
        }
        if let Some(prior) = pending.prior_previous.as_ref() {
            prior.validate()?;
            self.ensure_descriptor_manifest_available(prior)?;
        }
        Ok(Some(pending))
    }

    fn write_pending_activation(&self, pending: &PendingAnimeBundleActivation) -> Result<()> {
        ensure!(
            self.load_pending_activation()?.is_none(),
            "an anime bundle activation is already pending live verification"
        );
        self.ensure_descriptor_manifest_available(&pending.active)?;
        if let Some(prior) = pending.prior_active.as_ref() {
            self.ensure_descriptor_manifest_available(prior)?;
        }
        if let Some(prior) = pending.prior_previous.as_ref() {
            self.ensure_descriptor_manifest_available(prior)?;
        }
        write_atomic_json(&self.paths.pending_activation(), pending)?;
        ensure!(
            self.load_pending_activation()?.as_ref() == Some(pending),
            "pending anime activation marker changed during commit"
        );
        Ok(())
    }

    fn clear_pending_activation_exact(
        &self,
        expected: &PendingAnimeBundleActivation,
    ) -> Result<()> {
        let observed = self.load_pending_activation()?;
        ensure!(
            observed.as_ref() == Some(expected),
            "pending anime activation changed before completion"
        );
        remove_file_if_exists(&self.paths.pending_activation())?;
        ensure!(
            self.load_pending_activation()?.is_none(),
            "pending anime activation marker remained after completion"
        );
        Ok(())
    }

    /// Returns the exact transaction receipt needed after the newly activated
    /// worker passes its live startup/probe verification.
    pub fn pending_activation_token(
        &self,
        descriptor: &ActiveAnimeBundleDescriptor,
    ) -> Result<AnimePendingActivationToken> {
        let pending = self
            .load_pending_activation()?
            .ok_or_else(|| anyhow!("no anime bundle activation is pending verification"))?;
        ensure!(
            &pending.active == descriptor,
            "pending anime activation does not match the requested descriptor"
        );
        Ok(AnimePendingActivationToken {
            activation_id: pending.activation_id,
            descriptor: pending.active,
        })
    }

    /// Completes only the exact pending transaction represented by `token`.
    /// Older callbacks cannot clear a newer activation marker.
    pub async fn complete_pending_activation(
        &self,
        token: &AnimePendingActivationToken,
    ) -> Result<()> {
        let transaction = self
            .transaction_lock
            .lock()
            .map_err(|_| anyhow!("anime bundle transaction lock is poisoned"))?;
        let pending = self
            .load_pending_activation()?
            .ok_or_else(|| anyhow!("no anime bundle activation is pending verification"))?;
        ensure!(
            pending.activation_id == token.activation_id && pending.active == token.descriptor,
            "pending anime activation changed before completion"
        );
        ensure!(
            self.load_descriptor_record(&self.paths.active_bundle())?
                .as_ref()
                == Some(&token.descriptor),
            "active anime bundle changed before pending activation completion"
        );
        self.clear_pending_activation_exact(&pending)?;

        // The descriptor that was Previous before this activation remains a
        // required rollback generation until the exact live-verification
        // token completes. Only after clearing that exact pending marker is it
        // safe to reclaim its assets. Release the pointer lock before doing
        // asynchronous filesystem work.
        let obsolete_previous = pending.prior_previous;
        let active = pending.active;
        let previous = pending.prior_active;
        drop(transaction);
        if let Some(obsolete) = obsolete_previous {
            prune_descriptor_assets(&obsolete, Some(&active), previous.as_ref(), &self.paths).await;
        }
        Ok(())
    }

    /// Reconciles a crash during activation. A marker rolls back only when the
    /// authoritative Active pointer exactly equals its pending descriptor. If
    /// Active never committed (or a later transaction already replaced it),
    /// the marker is stale and is simply cleared.
    pub async fn recover_pending_activation(&self) -> Result<Option<ActiveAnimeBundleDescriptor>> {
        let _transaction = self
            .transaction_lock
            .lock()
            .map_err(|_| anyhow!("anime bundle transaction lock is poisoned"))?;
        let Some(pending) = self.load_pending_activation()? else {
            return self.load_active();
        };
        let observed = self.load_descriptor_record(&self.paths.active_bundle())?;
        if observed.as_ref() != Some(&pending.active) {
            if observed == pending.prior_active {
                self.replace_bundle_pointer(
                    BundlePointer::Previous,
                    pending.prior_previous.as_ref(),
                )?;
            }
            self.clear_pending_activation_exact(&pending)?;
            return self.load_active();
        }

        if let Some(prior) = pending.prior_active.as_ref() {
            self.validate_descriptor_paths(prior)?;
            self.ensure_descriptor_manifest_available(prior)?;
        }
        self.replace_bundle_pointer(BundlePointer::Active, pending.prior_active.as_ref())?;

        // Active is authoritative. Re-read it after the replacement so a
        // partial rollback or post-rename fsync error cannot lead us to clear
        // the marker while the failed descriptor is still active.
        let restored = self.load_descriptor_record(&self.paths.active_bundle())?;
        ensure!(
            restored == pending.prior_active,
            "pending anime activation rollback did not restore the prior Active pointer"
        );
        self.replace_bundle_pointer(BundlePointer::Previous, pending.prior_previous.as_ref())?;
        self.clear_pending_activation_exact(&pending)?;
        schedule_descriptor_asset_prune(
            pending.active,
            pending.prior_active.clone(),
            pending.prior_previous.clone(),
            self.paths.clone(),
        );
        self.load_active()
    }

    pub fn load_active_profile(&self) -> Result<Option<AnimeRuntimeProfile>> {
        let Some(active) = self.load_active()? else {
            return Ok(None);
        };
        // `active-bundle.json` embeds the sealed profile and is the atomic
        // commit point. The standalone profile is only a convenient mirror;
        // repair it after a crash between the two descriptor writes.
        let mirror =
            read_optional_json::<AnimeRuntimeProfile>(&self.paths.active_runtime_profile())
                .ok()
                .flatten();
        if mirror.as_ref() != Some(&active.profile) {
            write_atomic_json(&self.paths.active_runtime_profile(), &active.profile)?;
        }
        Ok(Some(active.profile))
    }

    fn load_descriptor(&self, path: &Path) -> Result<Option<ActiveAnimeBundleDescriptor>> {
        let Some(descriptor) = self.load_descriptor_record(path)? else {
            return Ok(None);
        };
        self.validate_descriptor_paths(&descriptor)?;
        Ok(Some(descriptor))
    }

    fn load_descriptor_record(&self, path: &Path) -> Result<Option<ActiveAnimeBundleDescriptor>> {
        let Some(descriptor): Option<ActiveAnimeBundleDescriptor> = read_optional_json(path)?
        else {
            return Ok(None);
        };
        descriptor.validate()?;
        Ok(Some(descriptor))
    }

    fn validate_descriptor_paths(&self, descriptor: &ActiveAnimeBundleDescriptor) -> Result<()> {
        let model = self.resolve_relative(&descriptor.model.relative_file)?;
        ensure!(model.is_file(), "active model artifact is missing");
        ensure!(
            model.metadata()?.len() == descriptor.model.size_bytes,
            "active model artifact size changed"
        );
        for runtime in &descriptor.runtimes {
            let root = self.resolve_relative(&runtime.relative_root)?;
            let entrypoint = root.join(&runtime.relative_entrypoint);
            ensure_exact_regular_file(&entrypoint, "active runtime entrypoint").with_context(
                || {
                    format!(
                        "active runtime '{}' entrypoint is missing or invalid",
                        runtime.artifact_key
                    )
                },
            )?;
            let marker: InstalledArtifactMarker = read_json(&root.join(".elixir-artifact.json"))?;
            ensure!(
                marker.artifact_key == runtime.artifact_key
                    && sha256_eq(&marker.sha256, &runtime.sha256)
                    && marker.downloaded_size_bytes == runtime.archive_size_bytes
                    && marker.installed_size_bytes == runtime.installed_size_bytes,
                "active runtime '{}' marker does not match descriptor",
                runtime.artifact_key
            );
            ensure_runtime_entrypoint_executable(&entrypoint)?;
        }
        Ok(())
    }

    pub fn resolve_relative(&self, relative: &str) -> Result<PathBuf> {
        validate_safe_relative_path(relative, "stored relative path")?;
        Ok(self.paths.root().join(relative))
    }

    async fn resolve_reusable_artifacts(
        &self,
        bundle: &ValidatedAnimeBundle,
        selection: &AnimeRuntimeSelection,
        cancellation: &CancellationToken,
    ) -> Result<ReusableBundleArtifacts> {
        ensure_not_cancelled(cancellation, "resolving reusable anime artifacts")?;
        // Only structurally valid committed descriptors can authorize an exact
        // candidate path. The artifact itself is then validated independently,
        // so a broken committed model can fall through to verified replacement
        // instead of making repair impossible.
        let active = self.load_descriptor_record(&self.paths.active_bundle())?;
        let previous = self.load_descriptor_record(&self.paths.previous_bundle())?;
        let descriptors = active.iter().chain(previous.iter()).collect::<Vec<_>>();
        let mut reusable = ReusableBundleArtifacts::default();

        let expected_model_root = self.paths.model_install_root(&bundle.manifest.model);
        let expected_model_path = expected_model_root.join("model.gguf");
        for descriptor in &descriptors {
            if installed_model_matches_manifest(&descriptor.model, &bundle.manifest.model)
                && self.resolve_relative(&descriptor.model.relative_file)? == expected_model_path
            {
                let validation = self
                    .validate_reusable_model(
                        &bundle.manifest.model,
                        &expected_model_root,
                        &expected_model_path,
                        cancellation,
                    )
                    .await;
                ensure_not_cancelled(cancellation, "validating reusable anime model")?;
                if let Err(error) = validation {
                    // A descriptor authorizes considering this exact path, but
                    // never authorizes trusting corrupt bytes. Fetch once into
                    // staging; activation can atomically replace the model file.
                    tracing::warn!(
                        path = %expected_model_path.display(),
                        error = %error,
                        "installed anime model is corrupt and will be replaced"
                    );
                    reusable.replace_model = true;
                } else {
                    reusable.model = Some(ReusableModelArtifact {
                        root: expected_model_root.clone(),
                        path: expected_model_path.clone(),
                    });
                }
                break;
            }
        }

        let (runtimes, replace_runtimes) =
            self.resolve_reusable_runtimes(selection, &descriptors, cancellation)?;
        reusable.runtimes = runtimes;
        reusable.replace_runtimes = replace_runtimes;
        Ok(reusable)
    }

    fn resolve_reusable_runtimes(
        &self,
        selection: &AnimeRuntimeSelection,
        descriptors: &[&ActiveAnimeBundleDescriptor],
        cancellation: &CancellationToken,
    ) -> Result<(BTreeMap<String, ReusableRuntimeArtifact>, BTreeSet<String>)> {
        let mut runtimes = BTreeMap::new();
        let mut replace_runtimes = BTreeSet::new();
        for runtime in selection.unique_artifacts() {
            ensure_not_cancelled(cancellation, "resolving reusable anime runtimes")?;
            let artifact_key = runtime.artifact_key();
            for descriptor in descriptors {
                let Some(installed) = descriptor
                    .runtimes
                    .iter()
                    .find(|installed| installed_runtime_matches_manifest(installed, runtime))
                else {
                    continue;
                };
                let installed_root = self.resolve_relative(&installed.relative_root)?;
                let revision_root = self.paths.runtimes().join(&runtime.revision);
                if !installed_root.starts_with(&revision_root)
                    || installed.relative_entrypoint != runtime.entrypoint
                {
                    continue;
                }
                if let Err(error) = self.validate_reusable_runtime(runtime, &installed_root) {
                    tracing::warn!(
                        path = %installed_root.display(),
                        error = %error,
                        "installed anime runtime is corrupt and will be replaced"
                    );
                    replace_runtimes.insert(artifact_key.clone());
                } else {
                    runtimes.insert(
                        artifact_key.clone(),
                        ReusableRuntimeArtifact {
                            root: installed_root.clone(),
                            entrypoint: installed_root.join(&runtime.entrypoint),
                        },
                    );
                }
                break;
            }
        }
        Ok((runtimes, replace_runtimes))
    }

    async fn validate_reusable_model(
        &self,
        model: &AnimeModelArtifactManifest,
        root: &Path,
        model_path: &Path,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        self.validate_reusable_model_layout(model, root, model_path)?;
        let path = model_path.to_path_buf();
        let size = model.size_bytes;
        let sha256 = model.sha256.clone();
        let cancellation = cancellation.clone();
        tokio::task::spawn_blocking(move || {
            verify_file_sha256_cancellable(&path, size, &sha256, &cancellation)
        })
        .await
        .context("joining reusable model verification task")??;
        Ok(())
    }

    fn validate_reusable_model_layout(
        &self,
        model: &AnimeModelArtifactManifest,
        root: &Path,
        model_path: &Path,
    ) -> Result<()> {
        ensure_exact_directory(root, "installed model root")?;
        ensure_exact_regular_file(model_path, "installed model")?;
        ensure!(
            model_path.metadata()?.len() == model.size_bytes,
            "installed model size changed"
        );
        let marker: InstalledArtifactMarker = read_json(&root.join(".elixir-artifact.json"))?;
        ensure!(
            marker == model_artifact_marker(model),
            "installed model marker does not match the requested artifact"
        );
        Ok(())
    }

    fn validate_reusable_runtime(
        &self,
        runtime: &AnimeRuntimeArtifactManifest,
        root: &Path,
    ) -> Result<()> {
        ensure_exact_directory(root, "installed runtime root")?;
        let entrypoint = root.join(&runtime.entrypoint);
        ensure_exact_regular_file(&entrypoint, "installed runtime entrypoint")?;
        for dependency in &runtime.packaged_dependencies {
            ensure_exact_regular_file(
                &root.join(dependency),
                "installed runtime packaged dependency",
            )?;
        }
        let marker: InstalledArtifactMarker = read_json(&root.join(".elixir-artifact.json"))?;
        ensure!(
            marker == runtime_artifact_marker(runtime),
            "installed runtime marker does not match the requested artifact"
        );
        ensure_runtime_entrypoint_executable(&entrypoint)?;
        Ok(())
    }

    pub async fn stage_bundle(
        &self,
        bundle: &ValidatedAnimeBundle,
        selection: &AnimeRuntimeSelection,
    ) -> Result<StagedAnimeBundle> {
        self.stage_bundle_with_cancellation(bundle, selection, &CancellationToken::new())
            .await
    }

    pub async fn stage_bundle_with_cancellation(
        &self,
        bundle: &ValidatedAnimeBundle,
        selection: &AnimeRuntimeSelection,
        cancellation: &CancellationToken,
    ) -> Result<StagedAnimeBundle> {
        ensure_not_cancelled(cancellation, "staging anime bundle")?;
        self.ensure_layout().await?;
        ensure_not_cancelled(cancellation, "staging anime bundle")?;
        ensure_selection_belongs_to_bundle(bundle, selection)?;
        let reusable = self
            .resolve_reusable_artifacts(bundle, selection, cancellation)
            .await?;
        let required = required_staging_bytes(bundle, selection, &reusable)?;
        let available = self.disk_space.available_bytes(self.paths.root())?;
        ensure!(
            available >= required,
            "insufficient inference staging space: need {required} bytes, have {available} bytes"
        );

        let stage_root = self.paths.staging().join(Uuid::new_v4().to_string());
        async_fs::create_dir(&stage_root)
            .await
            .with_context(|| format!("creating staging transaction '{}'", stage_root.display()))?;
        let mut cleanup = StagingPathCleanup::new(stage_root.clone());
        let result = self
            .stage_bundle_inner(bundle, selection, reusable, stage_root, cancellation)
            .await;
        if result.is_ok() {
            // The returned StagedAnimeBundle owns cleanup from this point.
            cleanup.disarm();
        } else if remove_directory_if_exists(cleanup.path()).await.is_ok() {
            cleanup.disarm();
        }
        result
    }

    async fn stage_bundle_inner(
        &self,
        bundle: &ValidatedAnimeBundle,
        selection: &AnimeRuntimeSelection,
        reusable: ReusableBundleArtifacts,
        stage_root: PathBuf,
        cancellation: &CancellationToken,
    ) -> Result<StagedAnimeBundle> {
        ensure_not_cancelled(cancellation, "staging anime bundle")?;
        let replace_existing_model = reusable.replace_model;
        let (model_dir, model_path, model_origin) = match reusable.model {
            Some(model) => (model.root, model.path, StagedArtifactOrigin::Existing),
            None => {
                let model_dir = stage_root.join("model");
                async_fs::create_dir(&model_dir).await?;
                let model_path = model_dir.join("model.gguf");
                self.fetch_artifact_with_cancellation(
                    &ArtifactDownloadSpec {
                        label: format!("model {}", bundle.manifest.model.id),
                        url: bundle.manifest.model.url.clone(),
                        sha256: bundle.manifest.model.sha256.clone(),
                        size_bytes: bundle.manifest.model.size_bytes,
                    },
                    &model_path,
                    cancellation,
                )
                .await?;
                write_atomic_json(
                    &model_dir.join(".elixir-artifact.json"),
                    &model_artifact_marker(&bundle.manifest.model),
                )?;
                (model_dir, model_path, StagedArtifactOrigin::Downloaded)
            }
        };

        let downloads = stage_root.join("downloads");
        let runtimes_root = stage_root.join("runtimes");
        async_fs::create_dir(&downloads).await?;
        async_fs::create_dir(&runtimes_root).await?;
        let mut staged_runtimes = Vec::new();
        for runtime in selection.unique_artifacts() {
            ensure_not_cancelled(cancellation, "staging anime runtime")?;
            let artifact_key = runtime.artifact_key();
            if let Some(existing) = reusable.runtimes.get(&artifact_key) {
                staged_runtimes.push(StagedAnimeRuntime {
                    manifest: runtime.clone(),
                    root: existing.root.clone(),
                    entrypoint: existing.entrypoint.clone(),
                    install_root: existing.root.clone(),
                    origin: StagedArtifactOrigin::Existing,
                });
                continue;
            }
            let archive_path = downloads.join(format!("{artifact_key}.artifact"));
            self.fetch_artifact_with_cancellation(
                &ArtifactDownloadSpec {
                    label: format!("runtime {artifact_key}"),
                    url: runtime.url.clone(),
                    sha256: runtime.sha256.clone(),
                    size_bytes: runtime.size_bytes,
                },
                &archive_path,
                cancellation,
            )
            .await?;
            let extracted_root = runtimes_root.join(&artifact_key);
            extract_runtime_archive_with_cancellation(
                &archive_path,
                &extracted_root,
                runtime,
                cancellation,
            )
            .await?;
            write_atomic_json(
                &extracted_root.join(".elixir-artifact.json"),
                &runtime_artifact_marker(runtime),
            )?;
            staged_runtimes.push(StagedAnimeRuntime {
                manifest: runtime.clone(),
                root: extracted_root.clone(),
                entrypoint: extracted_root.join(&runtime.entrypoint),
                install_root: if reusable.replace_runtimes.contains(&artifact_key) {
                    self.paths.runtime_replacement_install_root(runtime)
                } else {
                    self.paths.runtime_install_root(runtime)
                },
                origin: StagedArtifactOrigin::Downloaded,
            });
        }

        Ok(StagedAnimeBundle {
            manifest: bundle.manifest.clone(),
            manifest_fingerprint: bundle.manifest_fingerprint.clone(),
            stage_root,
            model_dir,
            model_path,
            model_origin,
            replace_existing_model,
            runtimes: staged_runtimes,
            cleanup_armed: true,
        })
    }

    /// Adds a later accelerator fallback to an existing staging transaction.
    ///
    /// The model and mandatory CPU runtime remain owned by `staged`; only
    /// runtime artifacts that are not already present are considered here.
    /// This keeps the fallback chain lazy without ever downloading the large
    /// model (or the CPU worker) a second time after an accelerator probe fails.
    pub async fn stage_additional_runtimes_with_cancellation(
        &self,
        bundle: &ValidatedAnimeBundle,
        selection: &AnimeRuntimeSelection,
        staged: &mut StagedAnimeBundle,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        ensure_not_cancelled(cancellation, "extending staged anime runtimes")?;
        ensure_selection_belongs_to_bundle(bundle, selection)?;
        ensure!(
            staged.cleanup_armed
                && staged.stage_root.starts_with(self.paths.staging())
                && staged.manifest == bundle.manifest
                && staged.manifest_fingerprint == bundle.manifest_fingerprint,
            "staged anime bundle does not belong to this bundle transaction"
        );
        ensure_exact_directory(&staged.stage_root, "anime bundle staging root")?;

        let staged_keys = staged
            .runtimes
            .iter()
            .map(|runtime| runtime.manifest.artifact_key())
            .collect::<BTreeSet<_>>();
        let missing = selection
            .unique_artifacts()
            .into_iter()
            .filter(|runtime| !staged_keys.contains(&runtime.artifact_key()))
            .collect::<Vec<_>>();
        if missing.is_empty() {
            return Ok(());
        }

        // Runtime reuse intentionally avoids revalidating the model. The model
        // was already verified when this staging transaction was created.
        let active = self.load_descriptor_record(&self.paths.active_bundle())?;
        let previous = self.load_descriptor_record(&self.paths.previous_bundle())?;
        let descriptors = active.iter().chain(previous.iter()).collect::<Vec<_>>();
        let (reusable, replace_runtimes) =
            self.resolve_reusable_runtimes(selection, &descriptors, cancellation)?;
        let mut required = 0_u64;
        for runtime in &missing {
            if reusable.contains_key(&runtime.artifact_key()) {
                continue;
            }
            required = required
                .checked_add(runtime.size_bytes)
                .and_then(|value| value.checked_add(runtime.installed_size_bytes))
                .ok_or_else(|| anyhow!("staging space calculation overflow"))?;
        }
        if required > 0 {
            required = required
                .checked_add(STAGING_RESERVE_BYTES)
                .ok_or_else(|| anyhow!("staging space calculation overflow"))?;
            let available = self.disk_space.available_bytes(self.paths.root())?;
            ensure!(
                available >= required,
                "insufficient inference staging space: need {required} bytes, have {available} bytes"
            );
        }

        let extension_root = staged
            .stage_root
            .join("extensions")
            .join(Uuid::new_v4().to_string());
        async_fs::create_dir_all(&extension_root)
            .await
            .with_context(|| {
                format!(
                    "creating staged runtime extension '{}'",
                    extension_root.display()
                )
            })?;
        let mut cleanup = StagingPathCleanup::new(extension_root.clone());
        let downloads = extension_root.join("downloads");
        let runtimes_root = extension_root.join("runtimes");
        async_fs::create_dir(&downloads).await?;
        async_fs::create_dir(&runtimes_root).await?;

        let extension = async {
            let mut additions = Vec::with_capacity(missing.len());
            for runtime in missing {
                ensure_not_cancelled(cancellation, "staging fallback anime runtime")?;
                let artifact_key = runtime.artifact_key();
                if let Some(existing) = reusable.get(&artifact_key) {
                    additions.push(StagedAnimeRuntime {
                        manifest: runtime.clone(),
                        root: existing.root.clone(),
                        entrypoint: existing.entrypoint.clone(),
                        install_root: existing.root.clone(),
                        origin: StagedArtifactOrigin::Existing,
                    });
                    continue;
                }

                let archive_path = downloads.join(format!("{artifact_key}.artifact"));
                self.fetch_artifact_with_cancellation(
                    &ArtifactDownloadSpec {
                        label: format!("runtime {artifact_key}"),
                        url: runtime.url.clone(),
                        sha256: runtime.sha256.clone(),
                        size_bytes: runtime.size_bytes,
                    },
                    &archive_path,
                    cancellation,
                )
                .await?;
                let extracted_root = runtimes_root.join(&artifact_key);
                extract_runtime_archive_with_cancellation(
                    &archive_path,
                    &extracted_root,
                    runtime,
                    cancellation,
                )
                .await?;
                write_atomic_json(
                    &extracted_root.join(".elixir-artifact.json"),
                    &runtime_artifact_marker(runtime),
                )?;
                additions.push(StagedAnimeRuntime {
                    manifest: runtime.clone(),
                    root: extracted_root.clone(),
                    entrypoint: extracted_root.join(&runtime.entrypoint),
                    install_root: if replace_runtimes.contains(&artifact_key) {
                        self.paths.runtime_replacement_install_root(runtime)
                    } else {
                        self.paths.runtime_install_root(runtime)
                    },
                    origin: StagedArtifactOrigin::Downloaded,
                });
            }
            Ok::<_, anyhow::Error>(additions)
        }
        .await;

        match extension {
            Ok(additions) => {
                staged.runtimes.extend(additions);
                cleanup.disarm();
                Ok(())
            }
            Err(error) => {
                let mut error = error;
                if let Err(cleanup_error) = remove_directory_if_exists(cleanup.path()).await {
                    error = error.context(format!(
                        "cleaning failed staged runtime extension also failed: {cleanup_error}"
                    ));
                } else {
                    cleanup.disarm();
                }
                Err(error)
            }
        }
    }

    async fn fetch_artifact_with_cancellation(
        &self,
        spec: &ArtifactDownloadSpec,
        destination: &Path,
        cancellation: &CancellationToken,
    ) -> Result<VerifiedArtifactDownload> {
        ensure_not_cancelled(cancellation, "fetching anime artifact")?;
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                bail!("anime bundle operation cancelled while fetching {}", spec.label)
            }
            result = self.fetcher.fetch(spec, destination) => result,
        }
    }

    pub async fn activate(
        &self,
        staged: StagedAnimeBundle,
        profile: AnimeRuntimeProfile,
    ) -> Result<ActiveAnimeBundleDescriptor> {
        self.activate_with_cancellation(staged, profile, &CancellationToken::new())
            .await
    }

    pub async fn activate_with_cancellation(
        &self,
        mut staged: StagedAnimeBundle,
        profile: AnimeRuntimeProfile,
        cancellation: &CancellationToken,
    ) -> Result<ActiveAnimeBundleDescriptor> {
        let install_paths = CandidateInstallPaths::for_staged(&self.paths, &staged);
        let activation = self.activate_inner(&staged, profile, cancellation).await;

        match activation {
            Ok(descriptor) => {
                // activate_inner removes staging before its synchronous pointer
                // commit, so there is no cancellation point between a durable
                // active descriptor and returning it to the caller.
                staged.cleanup_armed = false;
                Ok(descriptor)
            }
            Err(error) => {
                let mut error = error;
                if let Err(cleanup_error) = remove_directory_if_exists(&staged.stage_root).await {
                    error = error.context(format!(
                        "cleaning failed activation staging directory also failed: {cleanup_error}"
                    ));
                } else {
                    staged.cleanup_armed = false;
                }
                if let Err(cleanup_error) = self
                    .remove_unreferenced_candidate_installs(&install_paths)
                    .await
                {
                    error = error.context(format!(
                        "cleaning unreferenced activation artifacts also failed: {cleanup_error}"
                    ));
                }
                Err(error)
            }
        }
    }

    async fn activate_inner(
        &self,
        staged: &StagedAnimeBundle,
        profile: AnimeRuntimeProfile,
        cancellation: &CancellationToken,
    ) -> Result<ActiveAnimeBundleDescriptor> {
        ensure_not_cancelled(cancellation, "activating anime bundle")?;
        validate_profile_for_staged_bundle(&staged, &profile)?;
        let profile = profile.seal()?;
        ensure!(
            profile.probe_result != AnimeRuntimeProbeResult::DeterministicOnly,
            "deterministic-only profile cannot activate a worker bundle"
        );

        // Read pointer records before installation without requiring candidate
        // artifact bytes to be healthy. This allows an exact corrupt model to
        // be repaired from a freshly verified staged copy. All pointer paths
        // are validated again after installation and before commit.
        let old_active = self.load_descriptor_record(&self.paths.active_bundle())?;
        let old_previous = self.load_previous()?;
        ensure!(
            self.load_pending_activation()?.is_none(),
            "an anime bundle activation is already pending live verification"
        );
        let installed_model = self.install_model(&staged, cancellation).await?;
        ensure_not_cancelled(cancellation, "installing anime model")?;
        let mut installed_runtimes = Vec::new();
        for runtime in &staged.runtimes {
            ensure_not_cancelled(cancellation, "installing anime runtime")?;
            installed_runtimes.push(self.install_runtime(runtime, cancellation).await?);
            ensure_not_cancelled(cancellation, "installing anime runtime")?;
        }
        let rollback_active = old_active.as_ref().and_then(|active| {
            match self.validate_descriptor_paths(active) {
                Ok(()) => Some(active.clone()),
                Err(error) => {
                    tracing::warn!(
                        bundle_version = %active.bundle_version,
                        error = %error,
                        "current anime bundle is not healthy enough to retain as rollback target"
                    );
                    None
                }
            }
        });
        let descriptor = ActiveAnimeBundleDescriptor {
            schema_version: ANIME_BUNDLE_SCHEMA_VERSION,
            manifest_fingerprint: staged.manifest_fingerprint.clone(),
            bundle_version: staged.manifest.bundle_version.clone(),
            protocol_version: staged.manifest.protocol_version,
            matcher_schema_version: staged.manifest.matcher_schema_version,
            model: installed_model,
            runtimes: installed_runtimes,
            profile,
            activated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
        };
        descriptor.validate()?;
        self.validate_descriptor_paths(&descriptor)?;

        // Staging cleanup is deliberately pre-commit. From the active pointer
        // replacement through returning the descriptor there must be no await:
        // cancellation can therefore only observe the old pointer or receive
        // the successfully committed descriptor.
        remove_directory_if_exists(&staged.stage_root)
            .await
            .context("removing anime bundle staging directory before activation commit")?;
        ensure_not_cancelled(cancellation, "committing anime bundle activation")?;

        // Serialize the marker/pointer commit and completion callbacks. No
        // await or cancellation point is permitted after the pending marker is
        // durable until the committed descriptor is returned.
        let _transaction = self
            .transaction_lock
            .lock()
            .map_err(|_| anyhow!("anime bundle transaction lock is poisoned"))?;
        ensure!(
            self.load_pending_activation()?.is_none(),
            "an anime bundle activation is already pending live verification"
        );
        ensure!(
            self.load_descriptor_record(&self.paths.active_bundle())? == old_active
                && self.load_descriptor_record(&self.paths.previous_bundle())? == old_previous,
            "anime bundle pointers changed during activation"
        );
        self.seed_manifest(
            &staged.manifest,
            &staged.manifest_fingerprint,
            "staged bundle manifest",
        )?;
        if let Some(active) = rollback_active.as_ref() {
            self.ensure_descriptor_manifest_available(active)?;
        }
        if let Some(previous) = old_previous.as_ref() {
            self.ensure_descriptor_manifest_available(previous)?;
        }
        let pending = PendingAnimeBundleActivation {
            schema_version: PENDING_ACTIVATION_SCHEMA_VERSION,
            activation_id: Uuid::new_v4().to_string(),
            active: descriptor.clone(),
            prior_active: rollback_active.clone(),
            prior_previous: old_previous.clone(),
            created_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
        };
        self.write_pending_activation(&pending)?;

        if let Err(commit_error) = self
            .replace_bundle_pointer(BundlePointer::Previous, rollback_active.as_ref())
            .and_then(|()| self.replace_bundle_pointer(BundlePointer::Active, Some(&descriptor)))
        {
            // `replace_bundle_pointer` resolves rename/fsync ambiguity by
            // reading the descriptor back. Therefore an error here means the
            // intended descriptor is not durably observable. Restore both
            // pointers to their exact pre-transaction values before returning.
            let restoration = self
                .replace_bundle_pointer(BundlePointer::Active, old_active.as_ref())
                .and_then(|()| {
                    self.replace_bundle_pointer(BundlePointer::Previous, old_previous.as_ref())
                });
            return match restoration {
                Ok(()) => match self.clear_pending_activation_exact(&pending) {
                    Ok(()) => Err(commit_error).context(
                        "anime bundle activation pointer commit failed; prior pointers restored",
                    ),
                    Err(marker_error) => Err(commit_error).context(format!(
                        "anime bundle activation pointer commit failed; prior pointers restored but the pending marker could not be cleared: {marker_error}"
                    )),
                },
                Err(restore_error) => Err(commit_error).context(format!(
                    "anime bundle activation pointer commit failed and restoring prior pointers also failed: {restore_error}; the pending marker was retained for startup recovery"
                )),
            };
        }

        // `old_previous` is retained by the durable pending marker until the
        // exact live-verification token completes. It is pruned by
        // complete_pending_activation; rollback and crash recovery restore it.
        if rollback_active.is_none()
            && let Some(obsolete) = old_active
        {
            schedule_descriptor_asset_prune(
                obsolete,
                Some(descriptor.clone()),
                None,
                self.paths.clone(),
            );
        }
        Ok(descriptor)
    }

    pub async fn rollback_to_previous(&self) -> Result<ActiveAnimeBundleDescriptor> {
        let failed_active = self.load_active()?;
        let _previous = self
            .load_previous()?
            .ok_or_else(|| anyhow!("no previous anime inference bundle is available"))?;
        let failed_active = failed_active
            .ok_or_else(|| anyhow!("no active anime inference bundle is available"))?;
        self.rollback_failed_activation(&failed_active)
            .await?
            .ok_or_else(|| anyhow!("previous anime inference bundle disappeared during rollback"))
    }

    /// Reverts a bundle whose descriptor committed but whose local worker
    /// could not be activated. Upgrades restore the exact previous descriptor;
    /// a failed first install removes the active pointer entirely. The expected
    /// descriptor prevents a delayed failure handler from rolling back a newer
    /// successful activation.
    pub async fn rollback_failed_activation(
        &self,
        failed: &ActiveAnimeBundleDescriptor,
    ) -> Result<Option<ActiveAnimeBundleDescriptor>> {
        let _transaction = self
            .transaction_lock
            .lock()
            .map_err(|_| anyhow!("anime bundle transaction lock is poisoned"))?;
        let current = self
            .load_descriptor_record(&self.paths.active_bundle())?
            .ok_or_else(|| anyhow!("no active anime inference bundle is available to roll back"))?;
        ensure!(
            &current == failed,
            "active anime inference bundle changed before failed activation rollback"
        );
        let pending = self.load_pending_activation()?;
        if let Some(pending) = pending.as_ref() {
            ensure!(
                pending.active == *failed,
                "a newer anime bundle activation is pending verification"
            );
        }
        let previous = pending
            .as_ref()
            .map(|pending| pending.prior_active.clone())
            .unwrap_or(self.load_previous()?);
        let restored_previous = pending
            .as_ref()
            .and_then(|pending| pending.prior_previous.clone());
        if let Some(previous) = previous.as_ref() {
            self.validate_descriptor_paths(previous)?;
        }

        self.replace_bundle_pointer(BundlePointer::Active, previous.as_ref())?;
        ensure!(
            self.load_descriptor_record(&self.paths.active_bundle())? == previous,
            "failed anime activation rollback did not restore the prior Active pointer"
        );
        if let Err(error) =
            self.replace_bundle_pointer(BundlePointer::Previous, restored_previous.as_ref())
        {
            // The active pointer already names the restored descriptor (or is
            // absent for a first install). Keep that authoritative result and
            // surface failure to restore the non-authoritative prior pointer.
            return Err(error).context("restoring previous bundle pointer after rollback");
        }
        if let Some(pending) = pending.as_ref() {
            self.clear_pending_activation_exact(pending)?;
        }

        // The rollback API is the transaction boundary observed by the
        // lifecycle manager. Once it returns, a failed first install must not
        // leave large unreferenced artifacts behind. Release the pointer lock
        // before asynchronous filesystem work, then finish pruning in-band.
        drop(_transaction);
        prune_descriptor_assets(
            failed,
            previous.as_ref(),
            restored_previous.as_ref(),
            &self.paths,
        )
        .await;
        Ok(previous)
    }

    pub async fn discard_staged(&self, mut staged: StagedAnimeBundle) -> Result<()> {
        ensure!(
            staged.stage_root.starts_with(self.paths.staging()),
            "refusing to discard a staging directory outside the bundle store"
        );
        remove_directory_if_exists(&staged.stage_root)
            .await
            .with_context(|| {
                format!(
                    "removing staged anime bundle '{}'",
                    staged.stage_root.display()
                )
            })?;
        staged.cleanup_armed = false;
        Ok(())
    }

    pub async fn cleanup_staging(&self) -> Result<usize> {
        self.ensure_layout().await?;
        let mut entries = async_fs::read_dir(self.paths.staging()).await?;
        let mut removed = 0;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if entry.file_type().await?.is_dir() {
                async_fs::remove_dir_all(&path).await.with_context(|| {
                    format!("removing stale inference staging path '{}'", path.display())
                })?;
                removed += 1;
            } else {
                async_fs::remove_file(&path).await.with_context(|| {
                    format!("removing stale inference staging file '{}'", path.display())
                })?;
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// Removes version-addressed model/runtime install directories that are
    /// referenced by neither the active nor previous descriptor. Call this
    /// after lifecycle cancellation: an interrupted activation can move
    /// verified artifacts out of staging before it reaches the active pointer.
    /// If either descriptor is unreadable, this fails without deleting any
    /// installs.
    pub async fn cleanup_unreferenced_installs(&self) -> Result<usize> {
        self.ensure_layout().await?;
        let retained = self.referenced_install_roots()?;
        let mut removed = prune_unreferenced_install_tree(&self.paths.models(), &retained).await?;
        removed = removed.saturating_add(
            prune_unreferenced_install_tree(&self.paths.runtimes(), &retained).await?,
        );
        Ok(removed)
    }

    fn replace_bundle_pointer(
        &self,
        pointer: BundlePointer,
        target: Option<&ActiveAnimeBundleDescriptor>,
    ) -> Result<()> {
        let descriptor_path = pointer.descriptor_path(&self.paths);
        let profile_path = pointer.profile_path(&self.paths);

        // The profile file is a repairable mirror. Write it first for the
        // normal path, but never let a mirror failure prevent committing the
        // self-contained descriptor.
        let first_mirror_error = match target {
            Some(descriptor) => write_atomic_json(&profile_path, &descriptor.profile).err(),
            None => None,
        };
        let descriptor_result = match target {
            Some(descriptor) => write_atomic_json(&descriptor_path, descriptor),
            None => remove_file_if_exists(&descriptor_path),
        };

        if let Err(error) = descriptor_result {
            // Atomic rename/remove can succeed and the following directory
            // fsync can fail. Read the pointer back before deciding whether
            // the transaction committed.
            let observed = self.load_descriptor(&descriptor_path);
            match observed {
                Ok(observed) if observed.as_ref() == target => {
                    tracing::warn!(
                        pointer = pointer.as_str(),
                        error = %error,
                        "anime bundle descriptor operation reported an error after its commit point"
                    );
                }
                Ok(_) => {
                    return Err(error).context(format!(
                        "committing {} anime bundle pointer",
                        pointer.as_str()
                    ));
                }
                Err(read_error) => {
                    return Err(error).context(format!(
                        "committing {} anime bundle pointer failed and its on-disk state could not be reconciled: {read_error}",
                        pointer.as_str()
                    ));
                }
            }
        }

        // Repair or remove the non-authoritative mirror after the descriptor
        // is known to be at the intended value. load_active_profile performs
        // the same repair after an interrupted process.
        let mirror_result = match target {
            Some(descriptor) => write_atomic_json(&profile_path, &descriptor.profile),
            None => remove_file_if_exists(&profile_path),
        };
        if let Err(second_error) = mirror_result {
            let error = first_mirror_error.map_or_else(
                || second_error.to_string(),
                |first_error| {
                    format!(
                        "initial mirror write failed: {first_error}; repair failed: {second_error}"
                    )
                },
            );
            tracing::warn!(
                pointer = pointer.as_str(),
                error = %error,
                "anime bundle descriptor committed but its profile mirror could not be repaired"
            );
        }
        Ok(())
    }

    async fn remove_unreferenced_candidate_installs(
        &self,
        candidates: &CandidateInstallPaths,
    ) -> Result<()> {
        // If either descriptor is unreadable, preserve all version-addressed
        // artifacts. They are safe to reuse only when their marker matches,
        // and retaining them is safer than deleting an ambiguously referenced
        // active worker or model.
        let retained = self.referenced_install_roots()?;

        if !retained.contains(&candidates.model_root) {
            remove_directory_if_exists(&candidates.model_root).await?;
        }
        for runtime_root in &candidates.runtime_roots {
            if !retained.contains(runtime_root) {
                remove_directory_if_exists(runtime_root).await?;
            }
        }
        Ok(())
    }

    fn referenced_install_roots(&self) -> Result<BTreeSet<PathBuf>> {
        // Structurally valid pointers are sufficient to retain their declared
        // roots. Artifact corruption must not prevent cleanup of a separate
        // cancelled repair transaction, while malformed/unsafe descriptors
        // still fail closed before any deletion.
        let active = self.load_descriptor_record(&self.paths.active_bundle())?;
        let previous = self.load_descriptor_record(&self.paths.previous_bundle())?;
        let pending = self.load_pending_activation()?;
        let mut retained = BTreeSet::new();
        for descriptor in active
            .iter()
            .chain(previous.iter())
            .chain(pending.iter().flat_map(|pending| {
                std::iter::once(&pending.active)
                    .chain(pending.prior_active.iter())
                    .chain(pending.prior_previous.iter())
            }))
        {
            retained.insert(safe_artifact_parent(
                &self.paths,
                &descriptor.model.relative_file,
                "models",
            )?);
            for runtime in &descriptor.runtimes {
                retained.insert(safe_artifact_parent(
                    &self.paths,
                    &runtime.relative_root,
                    "runtimes",
                )?);
            }
        }
        Ok(retained)
    }

    async fn install_model(
        &self,
        staged: &StagedAnimeBundle,
        cancellation: &CancellationToken,
    ) -> Result<InstalledAnimeModel> {
        ensure_not_cancelled(cancellation, "installing anime model")?;
        let final_root = self.paths.model_install_root(&staged.manifest.model);
        match staged.model_origin {
            StagedArtifactOrigin::Downloaded => {
                if final_root.exists() {
                    ensure_exact_directory(&final_root, "installed model root")?;
                    let final_model = final_root.join("model.gguf");
                    if staged.replace_existing_model {
                        replace_staged_model_file(&staged.model_path, &final_model)?;
                        write_atomic_json(
                            &final_root.join(".elixir-artifact.json"),
                            &model_artifact_marker(&staged.manifest.model),
                        )?;
                        remove_directory_if_exists(&staged.model_dir).await?;
                    } else {
                        let existing = self
                            .validate_reusable_model(
                                &staged.manifest.model,
                                &final_root,
                                &final_model,
                                cancellation,
                            )
                            .await;
                        ensure_not_cancelled(cancellation, "validating installed anime model")?;
                        if existing.is_ok() {
                            remove_directory_if_exists(&staged.model_dir).await?;
                        } else {
                            replace_staged_model_file(&staged.model_path, &final_model)?;
                            write_atomic_json(
                                &final_root.join(".elixir-artifact.json"),
                                &model_artifact_marker(&staged.manifest.model),
                            )?;
                            remove_directory_if_exists(&staged.model_dir).await?;
                        }
                    }
                } else {
                    install_directory(&staged.model_dir, &final_root).await?;
                }
            }
            StagedArtifactOrigin::Existing => {
                ensure!(
                    staged.model_dir == final_root
                        && staged.model_path == final_root.join("model.gguf"),
                    "reused model path changed during activation"
                );
                self.validate_reusable_model_layout(
                    &staged.manifest.model,
                    &staged.model_dir,
                    &staged.model_path,
                )
                .context("revalidating reused model during activation")?;
            }
        }
        // Downloaded files were stream-hashed before staging, and reused files
        // were fully hashed immediately before staging was exposed to the
        // probe. Do not scan a multi-GB GGUF a redundant second time here;
        // activation revalidates the strict path/size/marker contract instead.
        self.validate_reusable_model_layout(
            &staged.manifest.model,
            &final_root,
            &final_root.join("model.gguf"),
        )?;
        let relative_file = relative_path_string(
            self.paths.root(),
            &final_root.join("model.gguf"),
            "installed model",
        )?;
        Ok(InstalledAnimeModel {
            id: staged.manifest.model.id.clone(),
            revision: staged.manifest.model.revision.clone(),
            sha256: normalize_sha256(&staged.manifest.model.sha256),
            size_bytes: staged.manifest.model.size_bytes,
            relative_file,
        })
    }

    async fn install_runtime(
        &self,
        staged: &StagedAnimeRuntime,
        cancellation: &CancellationToken,
    ) -> Result<InstalledAnimeRuntime> {
        ensure_not_cancelled(cancellation, "installing anime runtime")?;
        let final_root = staged.install_root.clone();
        ensure!(
            final_root.starts_with(self.paths.runtimes().join(&staged.manifest.revision)),
            "staged runtime install path is outside its revision namespace"
        );
        match staged.origin {
            StagedArtifactOrigin::Downloaded => {
                install_directory(&staged.root, &final_root).await?;
            }
            StagedArtifactOrigin::Existing => {
                ensure!(
                    staged.root == final_root
                        && staged.entrypoint == final_root.join(&staged.manifest.entrypoint),
                    "reused runtime path changed during activation"
                );
                self.validate_reusable_runtime(&staged.manifest, &staged.root)
                    .context("revalidating reused runtime during activation")?;
            }
        }
        let relative_root =
            relative_path_string(self.paths.root(), &final_root, "installed runtime root")?;
        Ok(InstalledAnimeRuntime {
            artifact_key: staged.manifest.artifact_key(),
            revision: staged.manifest.revision.clone(),
            sha256: normalize_sha256(&staged.manifest.sha256),
            archive_size_bytes: staged.manifest.size_bytes,
            installed_size_bytes: staged.manifest.installed_size_bytes,
            relative_root,
            relative_entrypoint: staged.manifest.entrypoint.clone(),
        })
    }
}

#[derive(Debug, Clone, Copy)]
enum BundlePointer {
    Active,
    Previous,
}

impl BundlePointer {
    fn descriptor_path(self, paths: &AnimeBundlePaths) -> PathBuf {
        match self {
            Self::Active => paths.active_bundle(),
            Self::Previous => paths.previous_bundle(),
        }
    }

    fn profile_path(self, paths: &AnimeBundlePaths) -> PathBuf {
        match self {
            Self::Active => paths.active_runtime_profile(),
            Self::Previous => paths.previous_runtime_profile(),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Previous => "previous",
        }
    }
}

#[derive(Debug)]
struct CandidateInstallPaths {
    model_root: PathBuf,
    runtime_roots: Vec<PathBuf>,
}

impl CandidateInstallPaths {
    fn for_staged(paths: &AnimeBundlePaths, staged: &StagedAnimeBundle) -> Self {
        Self {
            model_root: paths.model_install_root(&staged.manifest.model),
            runtime_roots: staged
                .runtimes
                .iter()
                .map(|runtime| runtime.install_root.clone())
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StagedArtifactOrigin {
    Downloaded,
    Existing,
}

#[derive(Debug, Default)]
struct ReusableBundleArtifacts {
    model: Option<ReusableModelArtifact>,
    replace_model: bool,
    runtimes: BTreeMap<String, ReusableRuntimeArtifact>,
    replace_runtimes: BTreeSet<String>,
}

#[derive(Debug)]
struct ReusableModelArtifact {
    root: PathBuf,
    path: PathBuf,
}

#[derive(Debug)]
struct ReusableRuntimeArtifact {
    root: PathBuf,
    entrypoint: PathBuf,
}

#[derive(Debug)]
pub struct StagedAnimeBundle {
    manifest: AnimeInferenceBundleManifest,
    manifest_fingerprint: String,
    stage_root: PathBuf,
    model_dir: PathBuf,
    model_path: PathBuf,
    model_origin: StagedArtifactOrigin,
    replace_existing_model: bool,
    runtimes: Vec<StagedAnimeRuntime>,
    cleanup_armed: bool,
}

impl StagedAnimeBundle {
    pub fn manifest(&self) -> &AnimeInferenceBundleManifest {
        &self.manifest
    }

    pub fn stage_root(&self) -> &Path {
        &self.stage_root
    }

    pub fn model_path(&self) -> &Path {
        &self.model_path
    }

    pub fn runtimes(&self) -> &[StagedAnimeRuntime] {
        &self.runtimes
    }
}

impl Drop for StagedAnimeBundle {
    fn drop(&mut self) {
        if self.cleanup_armed {
            schedule_staging_cleanup(self.stage_root.clone());
        }
    }
}

struct StagingPathCleanup {
    path: PathBuf,
    armed: bool,
}

impl StagingPathCleanup {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for StagingPathCleanup {
    fn drop(&mut self) {
        if self.armed {
            schedule_staging_cleanup(self.path.clone());
        }
    }
}

#[derive(Debug)]
pub struct StagedAnimeRuntime {
    manifest: AnimeRuntimeArtifactManifest,
    root: PathBuf,
    entrypoint: PathBuf,
    install_root: PathBuf,
    origin: StagedArtifactOrigin,
}

impl StagedAnimeRuntime {
    pub fn manifest(&self) -> &AnimeRuntimeArtifactManifest {
        &self.manifest
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn entrypoint(&self) -> &Path {
        &self.entrypoint
    }
}

fn ensure_selection_belongs_to_bundle(
    bundle: &ValidatedAnimeBundle,
    selection: &AnimeRuntimeSelection,
) -> Result<()> {
    ensure!(
        !selection.candidates.is_empty(),
        "runtime selection contains no candidates"
    );
    for selected in &selection.candidates {
        ensure!(
            bundle
                .manifest
                .runtimes
                .iter()
                .any(|runtime| runtime == &selected.artifact),
            "runtime selection contains an artifact absent from the bundle"
        );
    }
    let cpu_fallback = selection.cpu_fallback();
    ensure!(
        cpu_fallback.execution_backend == AnimeExecutionBackend::Cpu
            && cpu_fallback.device_id.is_none()
            && cpu_fallback.artifact.supports_cpu_execution(),
        "runtime selection has no valid CPU fallback"
    );
    Ok(())
}

fn validate_profile_for_staged_bundle(
    staged: &StagedAnimeBundle,
    profile: &AnimeRuntimeProfile,
) -> Result<()> {
    ensure!(
        profile.bundle_version == staged.manifest.bundle_version
            && profile.model_id == staged.manifest.model.id
            && profile.model_revision == staged.manifest.model.revision
            && profile.worker_revision == staged.manifest.worker_revision,
        "runtime profile identity does not match staged bundle"
    );
    ensure!(
        profile.kv_cache_type == staged.manifest.runtime_policy.kv_cache_type,
        "runtime profile KV cache type does not match bundle policy"
    );
    ensure!(
        staged
            .runtimes
            .iter()
            .any(|runtime| runtime.manifest.artifact_key() == profile.runtime_artifact_key),
        "runtime profile references an artifact absent from staged bundle"
    );
    Ok(())
}

fn required_staging_bytes(
    bundle: &ValidatedAnimeBundle,
    selection: &AnimeRuntimeSelection,
    reusable: &ReusableBundleArtifacts,
) -> Result<u64> {
    let mut required = if reusable.model.is_some() {
        0
    } else {
        bundle.manifest.model.size_bytes
    };
    for runtime in selection.unique_artifacts() {
        if reusable.runtimes.contains_key(&runtime.artifact_key()) {
            continue;
        }
        required = required
            .checked_add(runtime.size_bytes)
            .and_then(|value| value.checked_add(runtime.installed_size_bytes))
            .ok_or_else(|| anyhow!("staging space calculation overflow"))?;
    }
    if required == 0 {
        Ok(0)
    } else {
        required
            .checked_add(STAGING_RESERVE_BYTES)
            .ok_or_else(|| anyhow!("staging space calculation overflow"))
    }
}

fn model_artifact_marker(model: &AnimeModelArtifactManifest) -> InstalledArtifactMarker {
    InstalledArtifactMarker {
        schema_version: ARTIFACT_MARKER_SCHEMA_VERSION,
        kind: "model".to_string(),
        artifact_key: format!("{}-{}", model.id, model.revision),
        sha256: normalize_sha256(&model.sha256),
        downloaded_size_bytes: model.size_bytes,
        installed_size_bytes: model.size_bytes,
    }
}

fn runtime_artifact_marker(runtime: &AnimeRuntimeArtifactManifest) -> InstalledArtifactMarker {
    InstalledArtifactMarker {
        schema_version: ARTIFACT_MARKER_SCHEMA_VERSION,
        kind: "runtime".to_string(),
        artifact_key: runtime.artifact_key(),
        sha256: normalize_sha256(&runtime.sha256),
        downloaded_size_bytes: runtime.size_bytes,
        installed_size_bytes: runtime.installed_size_bytes,
    }
}

fn installed_model_matches_manifest(
    installed: &InstalledAnimeModel,
    model: &AnimeModelArtifactManifest,
) -> bool {
    installed.id == model.id
        && installed.revision == model.revision
        && installed.size_bytes == model.size_bytes
        && sha256_eq(&installed.sha256, &model.sha256)
}

fn installed_runtime_matches_manifest(
    installed: &InstalledAnimeRuntime,
    runtime: &AnimeRuntimeArtifactManifest,
) -> bool {
    installed.artifact_key == runtime.artifact_key()
        && installed.revision == runtime.revision
        && installed.archive_size_bytes == runtime.size_bytes
        && installed.installed_size_bytes == runtime.installed_size_bytes
        && installed.relative_entrypoint == runtime.entrypoint
        && sha256_eq(&installed.sha256, &runtime.sha256)
}

fn ensure_exact_directory(path: &Path, label: &str) -> Result<()> {
    let metadata = std_fs::symlink_metadata(path)
        .with_context(|| format!("reading {label} '{}'", path.display()))?;
    ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "{label} is not a non-symlink directory"
    );
    Ok(())
}

fn ensure_exact_regular_file(path: &Path, label: &str) -> Result<()> {
    let metadata = std_fs::symlink_metadata(path)
        .with_context(|| format!("reading {label} '{}'", path.display()))?;
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "{label} is not a non-symlink regular file"
    );
    Ok(())
}

async fn remove_directory_if_exists(path: &Path) -> Result<()> {
    match async_fs::remove_dir_all(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => {
            Err(error).with_context(|| format!("removing inference directory '{}'", path.display()))
        }
    }
}

async fn prune_unreferenced_install_tree(
    root: &Path,
    retained: &BTreeSet<PathBuf>,
) -> Result<usize> {
    let mut parents = async_fs::read_dir(root)
        .await
        .with_context(|| format!("reading inference install root '{}'", root.display()))?;
    let mut removed = 0_usize;
    while let Some(parent) = parents.next_entry().await? {
        let parent_type = parent.file_type().await?;
        if !parent_type.is_dir() || parent_type.is_symlink() {
            continue;
        }
        let parent_path = parent.path();
        let mut installs = async_fs::read_dir(&parent_path).await.with_context(|| {
            format!(
                "reading inference install namespace '{}'",
                parent_path.display()
            )
        })?;
        while let Some(install) = installs.next_entry().await? {
            let install_type = install.file_type().await?;
            if !install_type.is_dir() || install_type.is_symlink() {
                continue;
            }
            let install_path = install.path();
            if !retained.contains(&install_path) {
                remove_directory_if_exists(&install_path).await?;
                removed = removed.saturating_add(1);
            }
        }
        drop(installs);
        let mut remaining = async_fs::read_dir(&parent_path).await?;
        if remaining.next_entry().await?.is_none() {
            // Empty namespace directories are not artifacts and do not affect
            // the returned count.
            let _ = async_fs::remove_dir(&parent_path).await;
        }
    }
    Ok(removed)
}

fn schedule_staging_cleanup(path: PathBuf) {
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        let _cleanup = runtime.spawn(async move {
            if let Err(error) = remove_directory_if_exists(&path).await {
                tracing::warn!(
                    path = %path.display(),
                    error = %error,
                    "deferred anime inference staging cleanup failed"
                );
            }
        });
    } else if let Err(error) = std_fs::remove_dir_all(&path)
        && error.kind() != io::ErrorKind::NotFound
    {
        tracing::warn!(
            path = %path.display(),
            error = %error,
            "synchronous anime inference staging cleanup failed"
        );
    }
}

async fn install_directory(staged: &Path, destination: &Path) -> Result<()> {
    let marker: InstalledArtifactMarker = read_json(&staged.join(".elixir-artifact.json"))?;
    if destination.exists() {
        let existing: InstalledArtifactMarker =
            read_json(&destination.join(".elixir-artifact.json"))?;
        ensure!(
            existing == marker,
            "installed artifact destination '{}' contains different content",
            destination.display()
        );
        async_fs::remove_dir_all(staged).await.with_context(|| {
            format!("removing duplicate staged artifact '{}'", staged.display())
        })?;
        return Ok(());
    }
    let parent = destination
        .parent()
        .ok_or_else(|| anyhow!("artifact destination has no parent"))?;
    async_fs::create_dir_all(parent).await?;
    async_fs::rename(staged, destination)
        .await
        .with_context(|| {
            format!(
                "installing staged artifact '{}' at '{}'",
                staged.display(),
                destination.display()
            )
        })?;
    Ok(())
}

fn replace_staged_model_file(staged: &Path, destination: &Path) -> Result<()> {
    ensure_exact_regular_file(staged, "verified staged model")?;
    let parent = destination
        .parent()
        .ok_or_else(|| anyhow!("installed model destination has no parent"))?;
    ensure_exact_directory(parent, "installed model root")?;
    let file_name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("installed model destination has no valid file name"))?;
    let replacement = parent.join(format!(".{file_name}.{}.replacement", Uuid::new_v4()));
    let result = (|| -> Result<()> {
        std_fs::rename(staged, &replacement).with_context(|| {
            format!(
                "moving verified replacement model '{}' into its install directory",
                staged.display()
            )
        })?;
        File::open(&replacement)?.sync_all()?;
        atomic_replace(&replacement, destination).with_context(|| {
            format!(
                "atomically replacing corrupt installed model '{}'",
                destination.display()
            )
        })?;
        sync_directory(parent)?;
        Ok(())
    })();
    if result.is_err() && replacement.exists() {
        let _ = std_fs::rename(&replacement, staged);
    }
    result
}

async fn extract_runtime_archive_with_cancellation(
    archive_path: &Path,
    destination: &Path,
    runtime: &AnimeRuntimeArtifactManifest,
    cancellation: &CancellationToken,
) -> Result<()> {
    ensure_not_cancelled(cancellation, "extracting anime runtime")?;
    let archive_path = archive_path.to_path_buf();
    let destination = destination.to_path_buf();
    let runtime = runtime.clone();
    let cancellation = cancellation.clone();
    tokio::task::spawn_blocking(move || {
        extract_runtime_archive_blocking_cancellable(
            &archive_path,
            &destination,
            &runtime,
            &cancellation,
        )
    })
    .await
    .context("joining runtime extraction task")??;
    Ok(())
}

/// Extract a release-qualification runtime with the same archive-safety,
/// installed-size, dependency, and executable checks used by bundle staging.
/// The caller owns the fresh destination and its lifetime.
pub(crate) async fn extract_anime_runtime_for_qualification(
    archive_path: &Path,
    destination: &Path,
    runtime: &AnimeRuntimeArtifactManifest,
) -> Result<PathBuf> {
    extract_runtime_archive_with_cancellation(
        archive_path,
        destination,
        runtime,
        &CancellationToken::new(),
    )
    .await?;
    let entrypoint = destination.join(&runtime.entrypoint);
    ensure_exact_regular_file(&entrypoint, "qualification runtime entrypoint")?;
    Ok(entrypoint)
}

#[cfg(test)]
fn extract_runtime_archive_blocking(
    archive_path: &Path,
    destination: &Path,
    runtime: &AnimeRuntimeArtifactManifest,
) -> Result<()> {
    extract_runtime_archive_blocking_cancellable(
        archive_path,
        destination,
        runtime,
        &CancellationToken::new(),
    )
}

fn extract_runtime_archive_blocking_cancellable(
    archive_path: &Path,
    destination: &Path,
    runtime: &AnimeRuntimeArtifactManifest,
    cancellation: &CancellationToken,
) -> Result<()> {
    ensure_not_cancelled(cancellation, "extracting anime runtime")?;
    ensure!(
        !destination.exists(),
        "runtime extraction destination already exists"
    );
    std_fs::create_dir_all(destination)?;
    let result = (|| -> Result<()> {
        let extracted_bytes = match runtime.archive_format {
            AnimeRuntimeArchiveFormat::TarGz => extract_tar_gz_cancellable(
                archive_path,
                destination,
                runtime.installed_size_bytes,
                cancellation,
            )?,
            AnimeRuntimeArchiveFormat::Zip => extract_zip_cancellable(
                archive_path,
                destination,
                runtime.installed_size_bytes,
                cancellation,
            )?,
            AnimeRuntimeArchiveFormat::Raw => {
                let output = destination.join(&runtime.entrypoint);
                if let Some(parent) = output.parent() {
                    std_fs::create_dir_all(parent)?;
                }
                let mut input = File::open(archive_path)?;
                let mut output = OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(output)?;
                let copied = copy_cancellable(&mut input, &mut output, cancellation)?;
                output.sync_all()?;
                copied
            }
        };
        ensure!(
            extracted_bytes == runtime.installed_size_bytes,
            "runtime installed size mismatch: expected {}, extracted {}",
            runtime.installed_size_bytes,
            extracted_bytes
        );
        ensure_not_cancelled(cancellation, "extracting anime runtime")?;
        let entrypoint = destination.join(&runtime.entrypoint);
        ensure!(
            entrypoint.is_file(),
            "runtime archive does not contain declared entrypoint '{}'",
            runtime.entrypoint
        );
        for dependency in &runtime.packaged_dependencies {
            ensure!(
                destination.join(dependency).is_file(),
                "runtime archive does not contain declared dependency '{dependency}'"
            );
        }
        restore_executable_permission(&entrypoint)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std_fs::remove_dir_all(destination);
    }
    result
}

fn extract_tar_gz_cancellable(
    archive_path: &Path,
    destination: &Path,
    maximum: u64,
    cancellation: &CancellationToken,
) -> Result<u64> {
    let input = File::open(archive_path)?;
    let mut archive = Archive::new(GzDecoder::new(input));
    let mut total = 0_u64;
    for (index, entry) in archive.entries()?.enumerate() {
        ensure_not_cancelled(cancellation, "extracting tar runtime entry")?;
        ensure!(
            index < MAX_ARCHIVE_ENTRIES,
            "runtime archive has too many entries"
        );
        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        validate_archive_relative_path(&path)?;
        let output = destination.join(&path);
        let entry_type = entry.header().entry_type();
        if entry_type.is_dir() {
            std_fs::create_dir_all(&output)?;
            continue;
        }
        ensure!(
            entry_type.is_file(),
            "runtime archive contains a link or unsupported entry type"
        );
        let size = entry.header().size()?;
        total = total
            .checked_add(size)
            .ok_or_else(|| anyhow!("runtime archive size overflow"))?;
        ensure!(total <= maximum, "runtime archive exceeds installed size");
        if let Some(parent) = output.parent() {
            std_fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&output)?;
        let copied = copy_cancellable(&mut entry, &mut file, cancellation)?;
        ensure!(copied == size, "runtime archive entry was truncated");
        file.sync_all()?;
    }
    Ok(total)
}

fn extract_zip_cancellable(
    archive_path: &Path,
    destination: &Path,
    maximum: u64,
    cancellation: &CancellationToken,
) -> Result<u64> {
    let input = File::open(archive_path)?;
    let mut archive = zip::ZipArchive::new(input)?;
    ensure!(
        archive.len() <= MAX_ARCHIVE_ENTRIES,
        "runtime archive has too many entries"
    );
    let mut total = 0_u64;
    for index in 0..archive.len() {
        ensure_not_cancelled(cancellation, "extracting zip runtime entry")?;
        let mut entry = archive.by_index(index)?;
        let enclosed = entry
            .enclosed_name()
            .ok_or_else(|| anyhow!("runtime zip contains an unsafe path"))?
            .to_path_buf();
        validate_archive_relative_path(&enclosed)?;
        if let Some(mode) = entry.unix_mode() {
            ensure!(
                mode & 0o170000 != 0o120000,
                "runtime zip contains a symbolic link"
            );
        }
        let output = destination.join(&enclosed);
        if entry.is_dir() {
            std_fs::create_dir_all(&output)?;
            continue;
        }
        total = total
            .checked_add(entry.size())
            .ok_or_else(|| anyhow!("runtime archive size overflow"))?;
        ensure!(total <= maximum, "runtime archive exceeds installed size");
        if let Some(parent) = output.parent() {
            std_fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&output)?;
        let copied = copy_cancellable(&mut entry, &mut file, cancellation)?;
        ensure!(copied == entry.size(), "runtime zip entry was truncated");
        file.sync_all()?;
    }
    Ok(total)
}

fn copy_cancellable<R: Read, W: Write>(
    input: &mut R,
    output: &mut W,
    cancellation: &CancellationToken,
) -> Result<u64> {
    let mut buffer = vec![0_u8; CANCELLABLE_IO_CHUNK_BYTES];
    let mut copied = 0_u64;
    loop {
        ensure_not_cancelled(cancellation, "copying anime artifact")?;
        let read = input.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        output.write_all(&buffer[..read])?;
        copied = copied
            .checked_add(read as u64)
            .ok_or_else(|| anyhow!("artifact copy byte count overflow"))?;
    }
    ensure_not_cancelled(cancellation, "copying anime artifact")?;
    Ok(copied)
}

#[cfg(unix)]
fn restore_executable_permission(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = std_fs::metadata(path)?.permissions();
    permissions.set_mode(permissions.mode() | 0o700);
    std_fs::set_permissions(path, permissions)?;
    Ok(())
}

#[cfg(not(unix))]
fn restore_executable_permission(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn ensure_runtime_entrypoint_executable(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    if std_fs::metadata(path)?.permissions().mode() & 0o111 == 0 {
        restore_executable_permission(path)?;
    }
    ensure!(
        std_fs::metadata(path)?.permissions().mode() & 0o111 != 0,
        "runtime entrypoint '{}' is not executable",
        path.display()
    );
    Ok(())
}

#[cfg(not(unix))]
fn ensure_runtime_entrypoint_executable(_path: &Path) -> Result<()> {
    Ok(())
}

async fn prune_descriptor_assets(
    obsolete: &ActiveAnimeBundleDescriptor,
    active: Option<&ActiveAnimeBundleDescriptor>,
    previous: Option<&ActiveAnimeBundleDescriptor>,
    paths: &AnimeBundlePaths,
) {
    let mut retained = BTreeSet::new();
    if let Some(active) = active {
        retained.insert(active.model.relative_file.clone());
        retained.extend(
            active
                .runtimes
                .iter()
                .map(|runtime| runtime.relative_root.clone()),
        );
    }
    if let Some(previous) = previous {
        retained.insert(previous.model.relative_file.clone());
        retained.extend(
            previous
                .runtimes
                .iter()
                .map(|runtime| runtime.relative_root.clone()),
        );
    }
    if !retained.contains(&obsolete.model.relative_file)
        && let Ok(path) = safe_artifact_parent(paths, &obsolete.model.relative_file, "models")
    {
        let _ = async_fs::remove_dir_all(path).await;
    }
    for runtime in &obsolete.runtimes {
        if !retained.contains(&runtime.relative_root)
            && let Ok(path) = safe_artifact_parent(paths, &runtime.relative_root, "runtimes")
        {
            let _ = async_fs::remove_dir_all(path).await;
        }
    }
}

fn schedule_descriptor_asset_prune(
    obsolete: ActiveAnimeBundleDescriptor,
    active: Option<ActiveAnimeBundleDescriptor>,
    previous: Option<ActiveAnimeBundleDescriptor>,
    paths: AnimeBundlePaths,
) {
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        let _cleanup = runtime.spawn(async move {
            prune_descriptor_assets(&obsolete, active.as_ref(), previous.as_ref(), &paths).await;
        });
    }
}

fn safe_artifact_parent(paths: &AnimeBundlePaths, relative: &str, kind: &str) -> Result<PathBuf> {
    let path = paths.root().join(relative);
    let expected = paths.root().join(kind);
    ensure!(
        path.starts_with(&expected),
        "artifact path is outside {kind}"
    );
    Ok(if kind == "models" {
        path.parent()
            .ok_or_else(|| anyhow!("model artifact has no parent"))?
            .to_path_buf()
    } else {
        path
    })
}

fn read_optional_json<T: DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    if !path.exists() {
        return Ok(None);
    }
    read_json(path).map(Some)
}

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let mut file = File::open(path)
        .with_context(|| format!("opening inference descriptor '{}'", path.display()))?;
    let metadata = file.metadata()?;
    ensure!(
        metadata.len() <= DESCRIPTOR_MAX_BYTES,
        "inference descriptor '{}' exceeds {} bytes",
        path.display(),
        DESCRIPTOR_MAX_BYTES
    );
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.read_to_end(&mut bytes)?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing inference descriptor '{}'", path.display()))
}

fn write_atomic_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("descriptor path has no parent"))?;
    std_fs::create_dir_all(parent)?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("descriptor path has no valid file name"))?;
    let temporary = parent.join(format!(".{file_name}.{}.tmp", Uuid::new_v4()));
    let result = (|| -> Result<()> {
        let mut bytes =
            serde_json::to_vec_pretty(value).context("encoding inference descriptor")?;
        bytes.push(b'\n');
        ensure!(
            bytes.len() as u64 <= DESCRIPTOR_MAX_BYTES,
            "encoded inference descriptor exceeds {DESCRIPTOR_MAX_BYTES} bytes"
        );
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        atomic_replace(&temporary, path)?;
        sync_directory(parent)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std_fs::remove_file(&temporary);
    }
    result
}

fn remove_file_if_exists(path: &Path) -> Result<()> {
    match std_fs::remove_file(path) {
        Ok(()) => {
            if let Some(parent) = path.parent() {
                sync_directory(parent)?;
            }
            Ok(())
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(unix)]
fn atomic_replace(source: &Path, destination: &Path) -> Result<()> {
    std_fs::rename(source, destination)?;
    Ok(())
}

#[cfg(windows)]
fn atomic_replace(source: &Path, destination: &Path) -> Result<()> {
    use std::os::windows::ffi::OsStrExt;
    const MOVEFILE_REPLACE_EXISTING: u32 = 0x1;
    const MOVEFILE_WRITE_THROUGH: u32 = 0x8;
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn MoveFileExW(existing: *const u16, replacement: *const u16, flags: u32) -> i32;
    }
    let source: Vec<u16> = source
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    let destination: Vec<u16> = destination
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    let ok = unsafe {
        MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    ensure!(
        ok != 0,
        "atomically replacing descriptor failed: {}",
        io::Error::last_os_error()
    );
    Ok(())
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn native_available_disk_bytes(path: &Path) -> Result<u64> {
    use std::os::unix::ffi::OsStrExt;
    let existing = nearest_existing_ancestor(path)?;
    let encoded = CString::new(existing.as_os_str().as_bytes())
        .context("inference data path contains a NUL byte")?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    let result = unsafe { libc::statvfs(encoded.as_ptr(), stats.as_mut_ptr()) };
    ensure!(
        result == 0,
        "checking inference disk space failed: {}",
        io::Error::last_os_error()
    );
    let stats = unsafe { stats.assume_init() };
    (stats.f_bavail as u64)
        .checked_mul(stats.f_frsize as u64)
        .ok_or_else(|| anyhow!("available disk byte count overflow"))
}

#[cfg(windows)]
fn native_available_disk_bytes(path: &Path) -> Result<u64> {
    use std::os::windows::ffi::OsStrExt;
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetDiskFreeSpaceExW(
            directory: *const u16,
            free_for_caller: *mut u64,
            total_bytes: *mut u64,
            total_free: *mut u64,
        ) -> i32;
    }
    let existing = nearest_existing_ancestor(path)?;
    let encoded: Vec<u16> = existing
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    let mut available = 0_u64;
    let ok = unsafe {
        GetDiskFreeSpaceExW(
            encoded.as_ptr(),
            &mut available,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    ensure!(
        ok != 0,
        "checking inference disk space failed: {}",
        io::Error::last_os_error()
    );
    Ok(available)
}

fn nearest_existing_ancestor(path: &Path) -> Result<PathBuf> {
    let mut candidate = path.to_path_buf();
    loop {
        if candidate.exists() {
            return Ok(candidate);
        }
        ensure!(candidate.pop(), "path has no existing ancestor");
    }
}

fn validate_artifact_url(raw: &str, policy: AnimeArtifactUrlPolicy, field: &str) -> Result<()> {
    let url = Url::parse(raw).with_context(|| format!("{field} is not a valid URL"))?;
    ensure!(
        url.username().is_empty() && url.password().is_none() && url.fragment().is_none(),
        "{field} must not contain credentials or a fragment"
    );
    let allowed = url.scheme() == "https"
        || (policy == AnimeArtifactUrlPolicy::DevelopmentAllowHttp && url.scheme() == "http");
    ensure!(allowed, "{field} must use HTTPS");
    ensure!(url.host_str().is_some(), "{field} must include a host");
    Ok(())
}

fn validate_release_version(value: &str, field: &str) -> Result<()> {
    let components: Vec<&str> = value.split('.').collect();
    ensure!(
        components.len() == 3
            && components
                .iter()
                .all(|part| !part.is_empty() && part.chars().all(|ch| ch.is_ascii_digit())),
        "{field} must contain three numeric components"
    );
    Ok(())
}

fn validate_numeric_version(value: &str, field: &str) -> Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= 64
            && value
                .split('.')
                .all(|part| !part.is_empty() && part.chars().all(|ch| ch.is_ascii_digit())),
        "{field} must be a dot-separated numeric version"
    );
    Ok(())
}

fn version_at_least(actual: Option<&str>, minimum: Option<&str>) -> bool {
    let Some(minimum) = minimum.filter(|value| !value.is_empty()) else {
        return true;
    };
    let Some(actual) = actual else {
        return false;
    };
    let parse = |value: &str| -> Option<Vec<u64>> {
        value
            .split('.')
            .map(|part| part.parse::<u64>().ok())
            .collect()
    };
    let (Some(mut actual), Some(mut minimum)) = (parse(actual), parse(minimum)) else {
        return false;
    };
    let length = actual.len().max(minimum.len());
    actual.resize(length, 0);
    minimum.resize(length, 0);
    actual >= minimum
}

fn validate_commit_revision(value: &str, field: &str) -> Result<()> {
    let value = value.strip_prefix("sha256:").unwrap_or(value);
    ensure!(
        (40..=64).contains(&value.len()) && value.chars().all(|ch| ch.is_ascii_hexdigit()),
        "{field} must be an immutable hexadecimal commit revision"
    );
    Ok(())
}

fn validate_component(value: &str, field: &str) -> Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= 128
            && value
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'))
            && value != "."
            && value != "..",
        "{field} is not a safe identifier"
    );
    Ok(())
}

fn validate_nonempty_bounded(value: &str, field: &str, maximum: usize) -> Result<()> {
    ensure!(
        !value.trim().is_empty() && value.len() <= maximum,
        "{field} must be non-empty and no longer than {maximum} bytes"
    );
    Ok(())
}

fn normalized_cpu_feature(value: &str) -> Result<String> {
    let value = value
        .trim()
        .to_ascii_lowercase()
        .replace('-', "_")
        .replace('.', "_");
    ensure!(
        !value.is_empty()
            && value.len() <= 64
            && value
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_'),
        "invalid CPU feature name"
    );
    Ok(value)
}

fn validate_safe_relative_path(value: &str, field: &str) -> Result<()> {
    ensure!(
        !value.is_empty() && value.len() <= 512 && !value.contains('\\') && !value.contains(':'),
        "{field} is not a portable relative path"
    );
    validate_archive_relative_path(Path::new(value))
        .with_context(|| format!("{field} is not a safe relative path"))
}

fn validate_archive_relative_path(path: &Path) -> Result<()> {
    ensure!(!path.as_os_str().is_empty(), "path is empty");
    ensure!(!path.is_absolute(), "path is absolute");
    let portable = path
        .to_str()
        .ok_or_else(|| anyhow!("path is not valid UTF-8"))?;
    ensure!(
        !portable.contains('\\') && !portable.contains(':'),
        "path is not portable across supported hosts"
    );
    for component in path.components() {
        ensure!(
            matches!(component, Component::Normal(_)),
            "path contains traversal or a non-normal component"
        );
    }
    Ok(())
}

fn validate_sha256(value: &str, field: &str) -> Result<()> {
    let value = value.strip_prefix("sha256:").unwrap_or(value);
    ensure!(
        value.len() == 64 && value.chars().all(|ch| ch.is_ascii_hexdigit()),
        "{field} must be a 64-digit SHA-256"
    );
    Ok(())
}

fn normalize_sha256(value: &str) -> String {
    format!(
        "sha256:{}",
        value
            .strip_prefix("sha256:")
            .unwrap_or(value)
            .to_ascii_lowercase()
    )
}

fn sha256_eq(left: &str, right: &str) -> bool {
    normalize_sha256(left) == normalize_sha256(right)
}

fn sha256_prefixed(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn verify_file_sha256_cancellable(
    path: &Path,
    expected_size: u64,
    expected_sha256: &str,
    cancellation: &CancellationToken,
) -> Result<()> {
    ensure_not_cancelled(cancellation, "hashing anime artifact")?;
    let mut file = File::open(path)
        .with_context(|| format!("opening installed artifact '{}'", path.display()))?;
    ensure!(
        file.metadata()?.len() == expected_size,
        "installed artifact '{}' size mismatch",
        path.display()
    );
    let actual = sha256_reader_cancellable(&mut file, cancellation)?;
    ensure!(
        sha256_eq(&actual, expected_sha256),
        "installed artifact '{}' SHA-256 mismatch",
        path.display()
    );
    Ok(())
}

fn sha256_reader_cancellable<R: Read>(
    input: &mut R,
    cancellation: &CancellationToken,
) -> Result<String> {
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; CANCELLABLE_IO_CHUNK_BYTES];
    loop {
        ensure_not_cancelled(cancellation, "hashing anime artifact")?;
        let read = input.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    ensure_not_cancelled(cancellation, "hashing anime artifact")?;
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

fn ensure_not_cancelled(cancellation: &CancellationToken, operation: &str) -> Result<()> {
    ensure!(
        !cancellation.is_cancelled(),
        "anime bundle operation cancelled while {operation}"
    );
    Ok(())
}

fn relative_path_string(root: &Path, path: &Path, label: &str) -> Result<String> {
    let relative = path
        .strip_prefix(root)
        .with_context(|| format!("{label} is outside inference root"))?;
    validate_archive_relative_path(relative)?;
    Ok(relative
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/"))
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Mutex};

    use futures_util::{future, stream};
    use tempfile::TempDir;
    use tokio::sync::Notify;

    use super::*;

    const HASH_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const HASH_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const HASH_C: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    const COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";

    fn runtime(
        os: AnimeHostOs,
        arch: AnimeHostArch,
        backend: AnimeRuntimeBackend,
        device_class: Option<AnimeDeviceClass>,
        hash: &str,
    ) -> AnimeRuntimeArtifactManifest {
        AnimeRuntimeArtifactManifest {
            os,
            arch,
            device_class,
            backend,
            priority: match backend {
                AnimeRuntimeBackend::CudaCpu | AnimeRuntimeBackend::HipCpu => 10,
                AnimeRuntimeBackend::VulkanCpu => 20,
                AnimeRuntimeBackend::MetalCpu => 10,
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

    fn complete_manifest() -> AnimeInferenceBundleManifest {
        AnimeInferenceBundleManifest {
            schema_version: ANIME_BUNDLE_SCHEMA_VERSION,
            bundle_version: "2026.08.1".to_string(),
            protocol_version: ANIME_INFERENCE_PROTOCOL_VERSION,
            matcher_schema_version: ANIME_MATCHER_SCHEMA_VERSION,
            minimum_server_version: "0.1.0".to_string(),
            worker_revision: "llama-cpp-b7000".to_string(),
            model: AnimeModelArtifactManifest {
                id: "qwen3-4b-instruct-2507".to_string(),
                revision: "elixir-q4km-r1".to_string(),
                upstream_model_id: "Qwen/Qwen3-4B-Instruct-2507".to_string(),
                upstream_revision: COMMIT.to_string(),
                license: "Apache-2.0".to_string(),
                format: AnimeModelFormat::Gguf,
                quantization: "Q4_K_M".to_string(),
                transformer_layers: 36,
                context_tokens: 4_096,
                max_output_tokens: 256,
                thinking_mode: AnimeThinkingMode::NonThinkingOnly,
                chat_template_revision: "qwen3-2507-elixir-v1".to_string(),
                conversion_tool_revision: COMMIT.to_string(),
                qualification_report_fingerprint: HASH_B.to_string(),
                url: "https://releases.example/model.gguf".to_string(),
                sha256: HASH_A.to_string(),
                size_bytes: 5,
            },
            runtime_policy: AnimeRuntimePolicyManifest {
                sampling_profile_revision: "anime-match-v1".to_string(),
                parallel: 1,
                kv_cache_type: AnimeKvCacheType::F16,
                idle_unload_seconds: 300,
            },
            runtimes: vec![
                runtime(
                    AnimeHostOs::Macos,
                    AnimeHostArch::Aarch64,
                    AnimeRuntimeBackend::MetalCpu,
                    None,
                    HASH_A,
                ),
                runtime(
                    AnimeHostOs::Macos,
                    AnimeHostArch::X86_64,
                    AnimeRuntimeBackend::MetalCpu,
                    None,
                    HASH_B,
                ),
                runtime(
                    AnimeHostOs::Windows,
                    AnimeHostArch::X86_64,
                    AnimeRuntimeBackend::CudaCpu,
                    Some(AnimeDeviceClass::Nvidia),
                    HASH_A,
                ),
                runtime(
                    AnimeHostOs::Windows,
                    AnimeHostArch::X86_64,
                    AnimeRuntimeBackend::VulkanCpu,
                    Some(AnimeDeviceClass::AnyVulkan),
                    HASH_B,
                ),
                runtime(
                    AnimeHostOs::Windows,
                    AnimeHostArch::X86_64,
                    AnimeRuntimeBackend::Cpu,
                    Some(AnimeDeviceClass::Cpu),
                    HASH_C,
                ),
                runtime(
                    AnimeHostOs::Linux,
                    AnimeHostArch::X86_64,
                    AnimeRuntimeBackend::CudaCpu,
                    Some(AnimeDeviceClass::Nvidia),
                    HASH_A,
                ),
                runtime(
                    AnimeHostOs::Linux,
                    AnimeHostArch::X86_64,
                    AnimeRuntimeBackend::HipCpu,
                    Some(AnimeDeviceClass::Amd),
                    HASH_B,
                ),
                runtime(
                    AnimeHostOs::Linux,
                    AnimeHostArch::X86_64,
                    AnimeRuntimeBackend::VulkanCpu,
                    Some(AnimeDeviceClass::AnyVulkan),
                    HASH_C,
                ),
                runtime(
                    AnimeHostOs::Linux,
                    AnimeHostArch::X86_64,
                    AnimeRuntimeBackend::Cpu,
                    Some(AnimeDeviceClass::Cpu),
                    HASH_A,
                ),
                runtime(
                    AnimeHostOs::Linux,
                    AnimeHostArch::Aarch64,
                    AnimeRuntimeBackend::Cpu,
                    Some(AnimeDeviceClass::Cpu),
                    HASH_B,
                ),
            ],
        }
    }

    fn approval(manifest: &AnimeInferenceBundleManifest) -> QualifiedAnimeBundleApproval {
        QualifiedAnimeBundleApproval {
            bundle_version: manifest.bundle_version.clone(),
            manifest_fingerprint: sha256_prefixed(
                &serde_json::to_vec(manifest).expect("manifest fingerprint"),
            ),
            model_sha256: manifest.model.sha256.clone(),
            qualification_report_fingerprint: manifest
                .model
                .qualification_report_fingerprint
                .clone(),
            certified_runtime_profiles: Vec::new(),
        }
    }

    fn certified_profile(
        runtime: &AnimeRuntimeArtifactManifest,
        host_fingerprint: &str,
        execution_backend: AnimeExecutionBackend,
    ) -> QualifiedAnimeRuntimeProfileApproval {
        QualifiedAnimeRuntimeProfileApproval {
            host_fingerprint: host_fingerprint.to_string(),
            runtime_artifact_key: runtime.artifact_key(),
            runtime_artifact_sha256: runtime.sha256.clone(),
            execution_backend,
            certified_profile_fingerprint: HASH_B.to_string(),
            certification_report_fingerprint: HASH_C.to_string(),
        }
    }

    fn validated(manifest: AnimeInferenceBundleManifest) -> ValidatedAnimeBundle {
        validate_anime_bundle(
            manifest,
            &AnimeBundleCompatibilityPolicy::development(Version::new(0, 1, 0)),
        )
        .expect("valid development bundle")
    }

    fn host(
        os: AnimeHostOs,
        arch: AnimeHostArch,
        vendor: Option<AnimeGpuVendor>,
        backends: &[AnimeAcceleratorBackend],
    ) -> AnimeInferenceHost {
        AnimeInferenceHost {
            os,
            arch,
            os_version: Some("14.0".to_string()),
            cpu_features: BTreeSet::new(),
            devices: vendor
                .map(|vendor| AnimeInferenceDevice {
                    id: "gpu-0".to_string(),
                    vendor,
                    driver_version: Some("10.0".to_string()),
                    available_memory_bytes: Some(8 * 1024 * 1024 * 1024),
                    certified_backends: backends.iter().copied().collect(),
                    exposed_to_container: true,
                })
                .into_iter()
                .collect(),
            containerized: false,
        }
    }

    #[test]
    fn alm6_manifest_contract_rejects_unknown_fields() {
        let mut value = serde_json::to_value(complete_manifest()).expect("manifest JSON");
        value
            .as_object_mut()
            .expect("object")
            .insert("modelPicker".to_string(), serde_json::json!(true));
        assert!(
            serde_json::from_value::<AnimeInferenceBundleManifest>(value).is_err(),
            "unknown manifest fields must fail closed"
        );
    }

    #[test]
    fn alm6_production_gate_requires_exact_qualified_bundle() {
        let manifest = complete_manifest();
        let policy = AnimeBundleCompatibilityPolicy::production(
            Version::new(0, 1, 0),
            vec![approval(&manifest)],
        );
        validate_anime_bundle(manifest.clone(), &policy).expect("approved production bundle");

        let mut wrong = approval(&manifest);
        wrong.model_sha256 = HASH_C.to_string();
        let policy = AnimeBundleCompatibilityPolicy::production(Version::new(0, 1, 0), vec![wrong]);
        assert!(validate_anime_bundle(manifest, &policy).is_err());
    }

    #[test]
    fn alm9_certified_profile_activation_requires_exact_host_runtime_and_cpu_fallback() {
        let manifest = complete_manifest();
        let windows_host = host(
            AnimeHostOs::Windows,
            AnimeHostArch::X86_64,
            Some(AnimeGpuVendor::Nvidia),
            &[AnimeAcceleratorBackend::Cuda],
        );
        let development = validated(manifest.clone());
        let compatible =
            resolve_anime_runtime(&development, &windows_host).expect("compatible runtime");
        let cuda = compatible.preferred();
        let cpu = compatible.cpu_fallback();

        let empty_policy = AnimeBundleCompatibilityPolicy::production(
            Version::new(0, 1, 0),
            vec![approval(&manifest)],
        );
        let empty = validate_anime_bundle(manifest.clone(), &empty_policy)
            .expect("qualified model with no hardware certifications remains installable");
        assert!(
            empty
                .certified_runtime_selection(HASH_A, &compatible)
                .is_none(),
            "model qualification alone must not activate a compatible host"
        );

        let mut accelerator_only = approval(&manifest);
        accelerator_only.certified_runtime_profiles = vec![certified_profile(
            &cuda.artifact,
            HASH_A,
            AnimeExecutionBackend::Cuda,
        )];
        let accelerator_only = validate_anime_bundle(
            manifest.clone(),
            &AnimeBundleCompatibilityPolicy::production(
                Version::new(0, 1, 0),
                vec![accelerator_only],
            ),
        )
        .expect("well-formed accelerator certification");
        assert!(
            accelerator_only
                .certified_runtime_selection(HASH_A, &compatible)
                .is_none(),
            "an accelerator without a certified CPU fallback must not activate"
        );

        let mut complete = approval(&manifest);
        complete.certified_runtime_profiles = vec![
            certified_profile(&cuda.artifact, HASH_A, AnimeExecutionBackend::Cuda),
            certified_profile(&cpu.artifact, HASH_A, AnimeExecutionBackend::Cpu),
        ];
        let complete = validate_anime_bundle(
            manifest,
            &AnimeBundleCompatibilityPolicy::production(Version::new(0, 1, 0), vec![complete]),
        )
        .expect("complete hardware/runtime certification");
        let selected = complete
            .certified_runtime_selection(HASH_A, &compatible)
            .expect("exact certified route");
        assert_eq!(selected.candidates.len(), 2);
        assert_eq!(
            selected.preferred().execution_backend,
            AnimeExecutionBackend::Cuda
        );
        assert_eq!(
            selected.cpu_fallback().execution_backend,
            AnimeExecutionBackend::Cpu
        );
        assert!(
            complete
                .certified_runtime_selection(HASH_B, &compatible)
                .is_none(),
            "a merely compatible but uncertified host must stay deterministic-only"
        );
    }

    #[test]
    fn alm9_certified_profile_must_bind_an_exact_manifest_runtime() {
        let manifest = complete_manifest();
        let development = validated(manifest.clone());
        let selection = resolve_anime_runtime(
            &development,
            &host(
                AnimeHostOs::Windows,
                AnimeHostArch::X86_64,
                Some(AnimeGpuVendor::Nvidia),
                &[AnimeAcceleratorBackend::Cuda],
            ),
        )
        .expect("compatible runtime");
        let mut invalid = approval(&manifest);
        let mut profile = certified_profile(
            &selection.preferred().artifact,
            HASH_A,
            AnimeExecutionBackend::Cuda,
        );
        profile.runtime_artifact_sha256 = HASH_C.to_string();
        invalid.certified_runtime_profiles.push(profile);
        let policy =
            AnimeBundleCompatibilityPolicy::production(Version::new(0, 1, 0), vec![invalid]);
        assert!(validate_anime_bundle(manifest, &policy).is_err());
    }

    #[test]
    fn alm6_production_manifest_requires_every_platform_runtime() {
        let mut manifest = complete_manifest();
        manifest.runtimes.retain(|runtime| {
            !(runtime.os == AnimeHostOs::Linux
                && runtime.arch == AnimeHostArch::Aarch64
                && runtime.backend == AnimeRuntimeBackend::Cpu)
        });
        let policy = AnimeBundleCompatibilityPolicy::production(
            Version::new(0, 1, 0),
            vec![approval(&manifest)],
        );
        assert!(validate_anime_bundle(manifest, &policy).is_err());
    }

    #[test]
    fn alm6_validated_manifest_cache_replaces_atomically_and_revalidates() -> Result<()> {
        let root = TempDir::new()?;
        let store = AnimeBundleStore::with_dependencies(
            root.path(),
            Arc::new(MemoryFetcher {
                artifacts: Arc::new(HashMap::new()),
            }),
            Arc::new(FixedDiskSpace(u64::MAX)),
        );
        let policy = AnimeBundleCompatibilityPolicy::development(Version::new(0, 1, 0));
        let first = validate_anime_bundle(complete_manifest(), &policy)?;
        store.cache_validated_manifest(&first)?;
        assert_eq!(
            store
                .load_cached_manifest(&policy)?
                .expect("cached manifest")
                .manifest()
                .bundle_version,
            "2026.08.1"
        );

        let mut second_manifest = complete_manifest();
        second_manifest.bundle_version = "2026.08.2".to_string();
        let second = validate_anime_bundle(second_manifest, &policy)?;
        store.cache_validated_manifest(&second)?;
        assert_eq!(
            store
                .load_cached_manifest(&policy)?
                .expect("replaced cache")
                .manifest()
                .bundle_version,
            "2026.08.2"
        );
        assert!(
            std_fs::read_dir(root.path())?
                .filter_map(std::result::Result::ok)
                .all(|entry| !entry.file_name().to_string_lossy().ends_with(".tmp"))
        );
        Ok(())
    }

    #[test]
    fn alm6_corrupt_manifest_cache_fails_closed() -> Result<()> {
        let root = TempDir::new()?;
        let store = AnimeBundleStore::with_dependencies(
            root.path(),
            Arc::new(MemoryFetcher {
                artifacts: Arc::new(HashMap::new()),
            }),
            Arc::new(FixedDiskSpace(u64::MAX)),
        );
        std_fs::write(store.paths().cached_manifest(), b"{not-json")?;
        let policy = AnimeBundleCompatibilityPolicy::development(Version::new(0, 1, 0));
        assert!(store.load_cached_manifest(&policy).is_err());
        Ok(())
    }

    #[test]
    fn alm6_runtime_resolution_obeys_platform_backend_order_and_cpu_fallback() {
        let bundle = validated(complete_manifest());
        let cases = [
            (
                host(
                    AnimeHostOs::Macos,
                    AnimeHostArch::X86_64,
                    Some(AnimeGpuVendor::Amd),
                    &[AnimeAcceleratorBackend::Metal],
                ),
                AnimeExecutionBackend::Metal,
            ),
            (
                host(
                    AnimeHostOs::Windows,
                    AnimeHostArch::X86_64,
                    Some(AnimeGpuVendor::Nvidia),
                    &[
                        AnimeAcceleratorBackend::Cuda,
                        AnimeAcceleratorBackend::Vulkan,
                    ],
                ),
                AnimeExecutionBackend::Cuda,
            ),
            (
                host(
                    AnimeHostOs::Windows,
                    AnimeHostArch::X86_64,
                    Some(AnimeGpuVendor::Amd),
                    &[AnimeAcceleratorBackend::Vulkan],
                ),
                AnimeExecutionBackend::Vulkan,
            ),
            (
                host(
                    AnimeHostOs::Linux,
                    AnimeHostArch::X86_64,
                    Some(AnimeGpuVendor::Nvidia),
                    &[
                        AnimeAcceleratorBackend::Cuda,
                        AnimeAcceleratorBackend::Vulkan,
                    ],
                ),
                AnimeExecutionBackend::Cuda,
            ),
            (
                host(
                    AnimeHostOs::Linux,
                    AnimeHostArch::X86_64,
                    Some(AnimeGpuVendor::Amd),
                    &[
                        AnimeAcceleratorBackend::Hip,
                        AnimeAcceleratorBackend::Vulkan,
                    ],
                ),
                AnimeExecutionBackend::Hip,
            ),
        ];
        for (host, expected) in cases {
            let selection = resolve_anime_runtime(&bundle, &host).expect("runtime selection");
            assert_eq!(selection.preferred().execution_backend, expected);
            assert_eq!(
                selection.cpu_fallback().execution_backend,
                AnimeExecutionBackend::Cpu
            );
            assert!(selection.cpu_fallback().device_id.is_none());
        }
    }

    #[test]
    fn alm6_runtime_selection_retains_complete_accelerator_fallback_chain() {
        let bundle = validated(complete_manifest());
        let cases = [
            (
                host(
                    AnimeHostOs::Windows,
                    AnimeHostArch::X86_64,
                    Some(AnimeGpuVendor::Nvidia),
                    &[
                        AnimeAcceleratorBackend::Cuda,
                        AnimeAcceleratorBackend::Vulkan,
                    ],
                ),
                vec![
                    AnimeExecutionBackend::Cuda,
                    AnimeExecutionBackend::Vulkan,
                    AnimeExecutionBackend::Cpu,
                ],
            ),
            (
                host(
                    AnimeHostOs::Linux,
                    AnimeHostArch::X86_64,
                    Some(AnimeGpuVendor::Nvidia),
                    &[
                        AnimeAcceleratorBackend::Cuda,
                        AnimeAcceleratorBackend::Vulkan,
                    ],
                ),
                vec![
                    AnimeExecutionBackend::Cuda,
                    AnimeExecutionBackend::Vulkan,
                    AnimeExecutionBackend::Cpu,
                ],
            ),
            (
                host(
                    AnimeHostOs::Linux,
                    AnimeHostArch::X86_64,
                    Some(AnimeGpuVendor::Amd),
                    &[
                        AnimeAcceleratorBackend::Hip,
                        AnimeAcceleratorBackend::Vulkan,
                    ],
                ),
                vec![
                    AnimeExecutionBackend::Hip,
                    AnimeExecutionBackend::Vulkan,
                    AnimeExecutionBackend::Cpu,
                ],
            ),
        ];
        for (host, expected) in cases {
            let selection = resolve_anime_runtime(&bundle, &host).expect("runtime selection");
            assert_eq!(
                selection
                    .candidates
                    .iter()
                    .map(|candidate| candidate.execution_backend)
                    .collect::<Vec<_>>(),
                expected
            );
            assert_eq!(
                selection
                    .ordered_probe_attempts()
                    .iter()
                    .map(|attempt| attempt.preferred().execution_backend)
                    .collect::<Vec<_>>(),
                expected
            );
        }
    }

    #[test]
    fn alm6_runtime_selection_reduces_downloads_to_preferred_plus_cpu() {
        let bundle = validated(complete_manifest());
        let selection = resolve_anime_runtime(
            &bundle,
            &host(
                AnimeHostOs::Linux,
                AnimeHostArch::X86_64,
                Some(AnimeGpuVendor::Nvidia),
                &[
                    AnimeAcceleratorBackend::Cuda,
                    AnimeAcceleratorBackend::Vulkan,
                ],
            ),
        )
        .expect("runtime selection");
        let reduced = selection.preferred_with_cpu_fallback();
        assert_eq!(reduced.candidates.len(), 2);
        assert_eq!(
            reduced.candidates[0].execution_backend,
            AnimeExecutionBackend::Cuda
        );
        assert_eq!(
            reduced.candidates[1].execution_backend,
            AnimeExecutionBackend::Cpu
        );

        let cpu_selection = resolve_anime_runtime(
            &bundle,
            &host(AnimeHostOs::Linux, AnimeHostArch::Aarch64, None, &[]),
        )
        .expect("CPU selection");
        let reduced_cpu = cpu_selection.preferred_with_cpu_fallback();
        assert_eq!(reduced_cpu.candidates.len(), 1);
        assert_eq!(
            reduced_cpu.candidates[0].execution_backend,
            AnimeExecutionBackend::Cpu
        );
    }

    #[tokio::test]
    async fn alm6_accelerator_fallback_staging_fetches_only_the_next_runtime() -> Result<()> {
        let root = TempDir::new()?;
        let model = b"model".to_vec();
        let cuda_worker = b"cuda-worker".to_vec();
        let vulkan_worker = b"vulkan-worker".to_vec();
        let cpu_worker = b"cpu-worker".to_vec();
        let mut manifest = complete_manifest();
        manifest.model.size_bytes = model.len() as u64;
        manifest.model.sha256 = sha256_prefixed(&model);

        let mut artifacts = HashMap::from([(manifest.model.url.clone(), model)]);
        for runtime in manifest.runtimes.iter_mut().filter(|runtime| {
            runtime.os == AnimeHostOs::Linux && runtime.arch == AnimeHostArch::X86_64
        }) {
            let bytes = match runtime.backend {
                AnimeRuntimeBackend::CudaCpu => cuda_worker.clone(),
                AnimeRuntimeBackend::VulkanCpu => vulkan_worker.clone(),
                AnimeRuntimeBackend::Cpu => cpu_worker.clone(),
                _ => continue,
            };
            runtime.size_bytes = bytes.len() as u64;
            runtime.installed_size_bytes = bytes.len() as u64;
            runtime.sha256 = sha256_prefixed(&bytes);
            runtime.url = format!(
                "https://releases.example/fallback-{}.raw",
                runtime.backend.as_str()
            );
            artifacts.insert(runtime.url.clone(), bytes);
        }

        let bundle = validated(manifest);
        let selection = resolve_anime_runtime(
            &bundle,
            &host(
                AnimeHostOs::Linux,
                AnimeHostArch::X86_64,
                Some(AnimeGpuVendor::Nvidia),
                &[
                    AnimeAcceleratorBackend::Cuda,
                    AnimeAcceleratorBackend::Vulkan,
                ],
            ),
        )?;
        assert_eq!(
            selection
                .candidates
                .iter()
                .map(|candidate| candidate.execution_backend)
                .collect::<Vec<_>>(),
            vec![
                AnimeExecutionBackend::Cuda,
                AnimeExecutionBackend::Vulkan,
                AnimeExecutionBackend::Cpu,
            ]
        );

        let calls = Arc::new(Mutex::new(Vec::new()));
        let store = AnimeBundleStore::with_dependencies(
            root.path(),
            Arc::new(CountingFetcher {
                artifacts: Arc::new(artifacts),
                calls: calls.clone(),
            }),
            Arc::new(FixedDiskSpace(u64::MAX)),
        );
        let initial = selection.preferred_with_cpu_fallback();
        let cpu_base = AnimeRuntimeSelection {
            candidates: vec![selection.cpu_fallback().clone()],
        };
        let mut staged = store
            .stage_bundle_with_cancellation(&bundle, &cpu_base, &CancellationToken::new())
            .await?;
        store
            .stage_additional_runtimes_with_cancellation(
                &bundle,
                &initial,
                &mut staged,
                &CancellationToken::new(),
            )
            .await?;

        let initial_calls = calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        assert_eq!(initial_calls.len(), 3);
        assert_eq!(
            initial_calls
                .iter()
                .filter(|url| *url == &bundle.manifest().model.url)
                .count(),
            1
        );
        assert!(initial_calls.contains(&selection.candidates[0].artifact.url));
        assert!(initial_calls.contains(&selection.cpu_fallback().artifact.url));
        assert!(!initial_calls.contains(&selection.candidates[1].artifact.url));

        let fallback = AnimeRuntimeSelection {
            candidates: vec![
                selection.candidates[1].clone(),
                selection.cpu_fallback().clone(),
            ],
        };
        store
            .stage_additional_runtimes_with_cancellation(
                &bundle,
                &fallback,
                &mut staged,
                &CancellationToken::new(),
            )
            .await?;
        // Re-extending is idempotent and does not refetch any artifact.
        store
            .stage_additional_runtimes_with_cancellation(
                &bundle,
                &fallback,
                &mut staged,
                &CancellationToken::new(),
            )
            .await?;

        let final_calls = calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        assert_eq!(final_calls.len(), 4);
        for expected in [
            &bundle.manifest().model.url,
            &selection.candidates[0].artifact.url,
            &selection.candidates[1].artifact.url,
            &selection.cpu_fallback().artifact.url,
        ] {
            assert_eq!(
                final_calls.iter().filter(|url| *url == expected).count(),
                1,
                "artifact was fetched more than once: {expected}"
            );
        }
        assert_eq!(staged.runtimes().len(), 3);
        store.discard_staged(staged).await?;
        Ok(())
    }

    #[tokio::test]
    async fn alm6_missing_preferred_runtime_preserves_model_and_cpu_staging() -> Result<()> {
        let root = TempDir::new()?;
        let model = b"model".to_vec();
        let cuda_worker = b"cuda-worker".to_vec();
        let vulkan_worker = b"vulkan-worker".to_vec();
        let cpu_worker = b"cpu-worker".to_vec();
        let mut manifest = complete_manifest();
        manifest.model.size_bytes = model.len() as u64;
        manifest.model.sha256 = sha256_prefixed(&model);

        let mut artifacts = HashMap::from([(manifest.model.url.clone(), model)]);
        let mut cuda_url = String::new();
        for runtime in manifest.runtimes.iter_mut().filter(|runtime| {
            runtime.os == AnimeHostOs::Linux && runtime.arch == AnimeHostArch::X86_64
        }) {
            let bytes = match runtime.backend {
                AnimeRuntimeBackend::CudaCpu => cuda_worker.clone(),
                AnimeRuntimeBackend::VulkanCpu => vulkan_worker.clone(),
                AnimeRuntimeBackend::Cpu => cpu_worker.clone(),
                _ => continue,
            };
            runtime.size_bytes = bytes.len() as u64;
            runtime.installed_size_bytes = bytes.len() as u64;
            runtime.sha256 = sha256_prefixed(&bytes);
            runtime.url = format!(
                "https://releases.example/missing-preferred-{}.raw",
                runtime.backend.as_str()
            );
            if runtime.backend == AnimeRuntimeBackend::CudaCpu {
                cuda_url = runtime.url.clone();
            } else {
                artifacts.insert(runtime.url.clone(), bytes);
            }
        }

        let bundle = validated(manifest);
        let selection = resolve_anime_runtime(
            &bundle,
            &host(
                AnimeHostOs::Linux,
                AnimeHostArch::X86_64,
                Some(AnimeGpuVendor::Nvidia),
                &[
                    AnimeAcceleratorBackend::Cuda,
                    AnimeAcceleratorBackend::Vulkan,
                ],
            ),
        )?;
        let calls = Arc::new(Mutex::new(Vec::new()));
        let store = AnimeBundleStore::with_dependencies(
            root.path(),
            Arc::new(CountingFetcher {
                artifacts: Arc::new(artifacts),
                calls: calls.clone(),
            }),
            Arc::new(FixedDiskSpace(u64::MAX)),
        );

        let cpu_base = AnimeRuntimeSelection {
            candidates: vec![selection.cpu_fallback().clone()],
        };
        let mut staged = store
            .stage_bundle_with_cancellation(&bundle, &cpu_base, &CancellationToken::new())
            .await?;
        let initial = selection.preferred_with_cpu_fallback();
        assert!(
            store
                .stage_additional_runtimes_with_cancellation(
                    &bundle,
                    &initial,
                    &mut staged,
                    &CancellationToken::new(),
                )
                .await
                .is_err()
        );

        assert!(staged.model_path().is_file());
        assert_eq!(staged.runtimes().len(), 1);
        assert_eq!(
            staged.runtimes()[0].manifest().backend,
            AnimeRuntimeBackend::Cpu
        );
        assert!(staged.runtimes()[0].entrypoint().is_file());

        let vulkan_fallback = AnimeRuntimeSelection {
            candidates: vec![
                selection.candidates[1].clone(),
                selection.cpu_fallback().clone(),
            ],
        };
        store
            .stage_additional_runtimes_with_cancellation(
                &bundle,
                &vulkan_fallback,
                &mut staged,
                &CancellationToken::new(),
            )
            .await?;
        assert_eq!(staged.runtimes().len(), 2);

        let final_calls = calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        for expected in [
            &bundle.manifest().model.url,
            &cuda_url,
            &selection.candidates[1].artifact.url,
            &selection.cpu_fallback().artifact.url,
        ] {
            assert_eq!(
                final_calls.iter().filter(|url| *url == expected).count(),
                1,
                "artifact fetch count changed: {expected}"
            );
        }
        store.discard_staged(staged).await?;
        Ok(())
    }

    #[test]
    fn alm6_unknown_driver_evidence_is_probe_eligible_but_known_old_driver_is_not() {
        let mut manifest = complete_manifest();
        let hip = manifest
            .runtimes
            .iter_mut()
            .find(|runtime| {
                runtime.os == AnimeHostOs::Linux && runtime.backend == AnimeRuntimeBackend::HipCpu
            })
            .expect("HIP runtime");
        hip.minimum_driver_version = Some("24.1.0".to_string());
        let bundle = validated(manifest);
        let mut host = host(
            AnimeHostOs::Linux,
            AnimeHostArch::X86_64,
            Some(AnimeGpuVendor::Amd),
            &[AnimeAcceleratorBackend::Hip],
        );

        host.devices[0].driver_version = None;
        assert_eq!(
            resolve_anime_runtime(&bundle, &host)
                .expect("unknown driver may be probed")
                .preferred()
                .execution_backend,
            AnimeExecutionBackend::Hip
        );
        host.devices[0].driver_version = Some("Mesa 24.1.0".to_string());
        assert_eq!(
            resolve_anime_runtime(&bundle, &host)
                .expect("uncomparable driver may be probed")
                .preferred()
                .execution_backend,
            AnimeExecutionBackend::Hip
        );
        host.devices[0].driver_version = Some("24.0.9".to_string());
        assert_eq!(
            resolve_anime_runtime(&bundle, &host)
                .expect("old driver retains CPU fallback")
                .preferred()
                .execution_backend,
            AnimeExecutionBackend::Cpu
        );
        host.devices[0].driver_version = Some("24.1.0".to_string());
        assert_eq!(
            resolve_anime_runtime(&bundle, &host)
                .expect("minimum driver")
                .preferred()
                .execution_backend,
            AnimeExecutionBackend::Hip
        );
    }

    #[test]
    fn alm6_vulkan_driver_numbers_are_never_compared_across_vendor_schemes() {
        let mut manifest = complete_manifest();
        let vulkan = manifest
            .runtimes
            .iter_mut()
            .find(|runtime| {
                runtime.os == AnimeHostOs::Windows
                    && runtime.backend == AnimeRuntimeBackend::VulkanCpu
            })
            .expect("Vulkan runtime");
        vulkan.minimum_driver_version = Some("999.0".to_string());
        let bundle = validated(manifest);
        let mut host = host(
            AnimeHostOs::Windows,
            AnimeHostArch::X86_64,
            Some(AnimeGpuVendor::Intel),
            &[AnimeAcceleratorBackend::Vulkan],
        );
        host.devices[0].driver_version = Some("1.0".to_string());
        assert_eq!(
            resolve_anime_runtime(&bundle, &host)
                .expect("Vulkan should reach disposable probing")
                .preferred()
                .execution_backend,
            AnimeExecutionBackend::Vulkan
        );
    }

    #[test]
    fn alm6_unavailable_or_unexposed_accelerator_selects_cpu() {
        let bundle = validated(complete_manifest());
        let mut host = host(
            AnimeHostOs::Windows,
            AnimeHostArch::X86_64,
            Some(AnimeGpuVendor::Nvidia),
            &[AnimeAcceleratorBackend::Cuda],
        );
        host.containerized = true;
        host.devices[0].exposed_to_container = false;
        let selection = resolve_anime_runtime(&bundle, &host).expect("CPU selection");
        assert_eq!(
            selection.preferred().execution_backend,
            AnimeExecutionBackend::Cpu
        );
    }

    #[test]
    fn alm6_cpu_features_and_device_memory_are_compatibility_gates() {
        let mut manifest = complete_manifest();
        let cuda = manifest
            .runtimes
            .iter_mut()
            .find(|runtime| {
                runtime.os == AnimeHostOs::Windows
                    && runtime.backend == AnimeRuntimeBackend::CudaCpu
            })
            .expect("CUDA runtime");
        cuda.required_cpu_features = vec!["avx2".to_string()];
        cuda.minimum_device_memory_bytes = 12 * 1024 * 1024 * 1024;
        let bundle = validated(manifest);
        let host = host(
            AnimeHostOs::Windows,
            AnimeHostArch::X86_64,
            Some(AnimeGpuVendor::Nvidia),
            &[AnimeAcceleratorBackend::Cuda],
        );
        let selection = resolve_anime_runtime(&bundle, &host).expect("fallback selection");
        assert_eq!(
            selection.preferred().execution_backend,
            AnimeExecutionBackend::Cpu
        );
    }

    #[test]
    fn alm6_cpu_feature_normalization_matches_shared_hardware_inventory() -> Result<()> {
        let mut manifest = complete_manifest();
        let cpu = manifest
            .runtimes
            .iter_mut()
            .find(|runtime| {
                runtime.os == AnimeHostOs::Linux
                    && runtime.arch == AnimeHostArch::Aarch64
                    && runtime.backend == AnimeRuntimeBackend::Cpu
            })
            .expect("Linux ARM CPU runtime");
        cpu.required_cpu_features = vec!["SSE4.1".to_string()];
        let bundle = validated(manifest);
        let mut host = host(AnimeHostOs::Linux, AnimeHostArch::Aarch64, None, &[]);
        host.cpu_features.insert("sse4_1".to_string());
        let selection = resolve_anime_runtime(&bundle, &host)?;
        assert_eq!(
            selection.preferred().execution_backend,
            AnimeExecutionBackend::Cpu
        );
        Ok(())
    }

    #[tokio::test]
    async fn alm6_streaming_download_enforces_exact_size_and_sha() -> Result<()> {
        let root = TempDir::new()?;
        let bytes = b"model".to_vec();
        let hash = sha256_prefixed(&bytes);
        let output = root.path().join("model.gguf");
        let result = write_verified_stream(
            stream::iter(vec![
                Ok::<_, io::Error>(b"mo".to_vec()),
                Ok(b"del".to_vec()),
            ]),
            &output,
            &hash,
            bytes.len() as u64,
        )
        .await?;
        assert_eq!(result.size_bytes, 5);
        assert_eq!(std_fs::read(&output)?, bytes);

        let invalid = root.path().join("invalid.gguf");
        assert!(
            write_verified_stream(
                stream::iter(vec![Ok::<_, io::Error>(b"model".to_vec())]),
                &invalid,
                HASH_A,
                5,
            )
            .await
            .is_err()
        );
        assert!(!invalid.exists());
        Ok(())
    }

    #[test]
    fn alm6_zip_extraction_rejects_traversal() -> Result<()> {
        let root = TempDir::new()?;
        let archive_path = root.path().join("runtime.zip");
        {
            let file = File::create(&archive_path)?;
            let mut zip = zip::ZipWriter::new(file);
            let options = zip::write::SimpleFileOptions::default();
            zip.start_file("../escape", options)?;
            zip.write_all(b"bad")?;
            zip.finish()?;
        }
        let mut manifest = runtime(
            AnimeHostOs::Windows,
            AnimeHostArch::X86_64,
            AnimeRuntimeBackend::Cpu,
            Some(AnimeDeviceClass::Cpu),
            HASH_A,
        );
        manifest.archive_format = AnimeRuntimeArchiveFormat::Zip;
        manifest.installed_size_bytes = 3;
        assert!(
            extract_runtime_archive_blocking(&archive_path, &root.path().join("out"), &manifest)
                .is_err()
        );
        assert!(!root.path().join("escape").exists());
        Ok(())
    }

    #[derive(Clone)]
    struct MemoryFetcher {
        artifacts: Arc<HashMap<String, Vec<u8>>>,
    }

    #[async_trait]
    impl AnimeArtifactFetcher for MemoryFetcher {
        async fn fetch(
            &self,
            spec: &ArtifactDownloadSpec,
            destination: &Path,
        ) -> Result<VerifiedArtifactDownload> {
            let bytes = self
                .artifacts
                .get(&spec.url)
                .ok_or_else(|| anyhow!("missing in-memory artifact"))?
                .clone();
            write_verified_stream(
                stream::iter(vec![Ok::<_, io::Error>(bytes)]),
                destination,
                &spec.sha256,
                spec.size_bytes,
            )
            .await
        }
    }

    #[derive(Clone)]
    struct CountingFetcher {
        artifacts: Arc<HashMap<String, Vec<u8>>>,
        calls: Arc<Mutex<Vec<String>>>,
    }

    #[derive(Clone)]
    struct BlockingFetcher {
        started: Arc<Notify>,
    }

    #[async_trait]
    impl AnimeArtifactFetcher for BlockingFetcher {
        async fn fetch(
            &self,
            _spec: &ArtifactDownloadSpec,
            destination: &Path,
        ) -> Result<VerifiedArtifactDownload> {
            std_fs::write(destination, b"partial")?;
            self.started.notify_one();
            future::pending::<Result<VerifiedArtifactDownload>>().await
        }
    }

    #[async_trait]
    impl AnimeArtifactFetcher for CountingFetcher {
        async fn fetch(
            &self,
            spec: &ArtifactDownloadSpec,
            destination: &Path,
        ) -> Result<VerifiedArtifactDownload> {
            self.calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(spec.url.clone());
            let bytes = self
                .artifacts
                .get(&spec.url)
                .ok_or_else(|| anyhow!("missing in-memory artifact"))?
                .clone();
            write_verified_stream(
                stream::iter(vec![Ok::<_, io::Error>(bytes)]),
                destination,
                &spec.sha256,
                spec.size_bytes,
            )
            .await
        }
    }

    struct FixedDiskSpace(u64);

    impl AnimeDiskSpaceProbe for FixedDiskSpace {
        fn available_bytes(&self, _path: &Path) -> Result<u64> {
            Ok(self.0)
        }
    }

    fn profile_for(
        bundle: &ValidatedAnimeBundle,
        runtime: &ResolvedAnimeRuntime,
    ) -> AnimeRuntimeProfile {
        AnimeRuntimeProfile {
            schema_version: ANIME_RUNTIME_PROFILE_SCHEMA_VERSION,
            bundle_version: bundle.manifest.bundle_version.clone(),
            model_id: bundle.manifest.model.id.clone(),
            model_revision: bundle.manifest.model.revision.clone(),
            worker_revision: bundle.manifest.worker_revision.clone(),
            runtime_artifact_key: runtime.artifact.artifact_key(),
            host_fingerprint: format!("sha256:{HASH_C}"),
            execution_backend: runtime.execution_backend,
            device_id: runtime.device_id.clone(),
            gpu_layer_count: if runtime.execution_backend == AnimeExecutionBackend::Cpu {
                0
            } else {
                12
            },
            cpu_thread_count: 4,
            kv_cache_type: bundle.manifest.runtime_policy.kv_cache_type,
            load_time_ms: 100,
            warm_latency_ms: 200,
            peak_rss_bytes: 1024,
            peak_device_memory_bytes: None,
            probe_result: if runtime.execution_backend == AnimeExecutionBackend::Cpu {
                AnimeRuntimeProbeResult::CpuBalanced
            } else {
                AnimeRuntimeProbeResult::GpuBalanced
            },
            probed_at: Utc::now().to_rfc3339(),
            profile_fingerprint: String::new(),
        }
    }

    fn raw_store_fixture(
        root: &TempDir,
        bundle_version: &str,
    ) -> Result<(
        AnimeBundleStore,
        ValidatedAnimeBundle,
        AnimeRuntimeSelection,
    )> {
        let model = b"model".to_vec();
        let worker = b"bin".to_vec();
        let mut manifest = complete_manifest();
        manifest.bundle_version = bundle_version.to_string();
        manifest.model.size_bytes = model.len() as u64;
        manifest.model.sha256 = sha256_prefixed(&model);
        let mac_runtime = manifest
            .runtimes
            .iter_mut()
            .find(|runtime| {
                runtime.os == AnimeHostOs::Macos && runtime.arch == AnimeHostArch::X86_64
            })
            .expect("Mac runtime");
        mac_runtime.size_bytes = worker.len() as u64;
        mac_runtime.installed_size_bytes = worker.len() as u64;
        mac_runtime.sha256 = sha256_prefixed(&worker);
        let bundle = validated(manifest);
        let selection = resolve_anime_runtime(
            &bundle,
            &host(AnimeHostOs::Macos, AnimeHostArch::X86_64, None, &[]),
        )?;
        let artifacts = HashMap::from([
            (bundle.manifest.model.url.clone(), model),
            (selection.cpu_fallback().artifact.url.clone(), worker),
        ]);
        let store = AnimeBundleStore::with_dependencies(
            root.path(),
            Arc::new(MemoryFetcher {
                artifacts: Arc::new(artifacts),
            }),
            Arc::new(FixedDiskSpace(u64::MAX)),
        );
        Ok((store, bundle, selection))
    }

    fn generation_store_fixture(
        root: &TempDir,
        bundle_version: &str,
        generation: u8,
    ) -> Result<(
        AnimeBundleStore,
        ValidatedAnimeBundle,
        AnimeRuntimeSelection,
    )> {
        let model = format!("model-generation-{generation}").into_bytes();
        let worker = format!("worker-generation-{generation}").into_bytes();
        let mut manifest = complete_manifest();
        manifest.bundle_version = bundle_version.to_string();
        manifest.worker_revision = format!("llama-worker-generation-{generation}");
        manifest.model.revision = format!("model-generation-{generation}");
        manifest.model.url = format!("https://releases.example/model-generation-{generation}.gguf");
        manifest.model.size_bytes = model.len() as u64;
        manifest.model.sha256 = sha256_prefixed(&model);

        let runtime = manifest
            .runtimes
            .iter_mut()
            .filter(|runtime| {
                runtime.os == AnimeHostOs::Macos
                    && runtime.arch == AnimeHostArch::X86_64
                    && runtime.supports_cpu_execution()
            })
            .min_by_key(|runtime| cpu_fallback_rank(runtime, AnimeHostOs::Macos))
            .expect("Mac CPU fallback runtime");
        runtime.revision = format!("worker-generation-{generation}");
        runtime.url = format!("https://releases.example/worker-generation-{generation}.raw");
        runtime.size_bytes = worker.len() as u64;
        runtime.installed_size_bytes = worker.len() as u64;
        runtime.sha256 = sha256_prefixed(&worker);

        let bundle = validated(manifest);
        let selection = resolve_anime_runtime(
            &bundle,
            &host(AnimeHostOs::Macos, AnimeHostArch::X86_64, None, &[]),
        )?;
        let artifacts = HashMap::from([
            (bundle.manifest.model.url.clone(), model),
            (selection.cpu_fallback().artifact.url.clone(), worker),
        ]);
        let store = AnimeBundleStore::with_dependencies(
            root.path(),
            Arc::new(MemoryFetcher {
                artifacts: Arc::new(artifacts),
            }),
            Arc::new(FixedDiskSpace(u64::MAX)),
        );
        Ok((store, bundle, selection))
    }

    async fn three_generation_pending_fixture(
        root: &TempDir,
    ) -> Result<(
        AnimeBundleStore,
        ActiveAnimeBundleDescriptor,
        ActiveAnimeBundleDescriptor,
        ActiveAnimeBundleDescriptor,
    )> {
        let (store, previous_bundle, previous_selection) =
            generation_store_fixture(root, "2026.08.1", 1)?;
        let previous = store
            .activate(
                store
                    .stage_bundle(&previous_bundle, &previous_selection)
                    .await?,
                profile_for(&previous_bundle, previous_selection.preferred()),
            )
            .await?;
        complete_activation(&store, &previous).await?;

        let (store, active_bundle, active_selection) =
            generation_store_fixture(root, "2026.08.2", 2)?;
        let active = store
            .activate(
                store
                    .stage_bundle(&active_bundle, &active_selection)
                    .await?,
                profile_for(&active_bundle, active_selection.preferred()),
            )
            .await?;
        complete_activation(&store, &active).await?;
        assert_eq!(store.load_previous()?, Some(previous.clone()));

        let (store, pending_bundle, pending_selection) =
            generation_store_fixture(root, "2026.08.3", 3)?;
        let pending = store
            .activate(
                store
                    .stage_bundle(&pending_bundle, &pending_selection)
                    .await?,
                profile_for(&pending_bundle, pending_selection.preferred()),
            )
            .await?;
        assert_eq!(store.load_active()?, Some(pending.clone()));
        assert_eq!(store.load_previous()?, Some(active.clone()));
        assert!(store.paths().pending_activation().is_file());
        Ok((store, previous, active, pending))
    }

    fn descriptor_asset_roots(
        store: &AnimeBundleStore,
        descriptor: &ActiveAnimeBundleDescriptor,
    ) -> Result<Vec<PathBuf>> {
        let mut roots = vec![safe_artifact_parent(
            store.paths(),
            &descriptor.model.relative_file,
            "models",
        )?];
        roots.extend(
            descriptor
                .runtimes
                .iter()
                .map(|runtime| {
                    safe_artifact_parent(store.paths(), &runtime.relative_root, "runtimes")
                })
                .collect::<Result<Vec<_>>>()?,
        );
        Ok(roots)
    }

    async fn complete_activation(
        store: &AnimeBundleStore,
        descriptor: &ActiveAnimeBundleDescriptor,
    ) -> Result<()> {
        let token = store.pending_activation_token(descriptor)?;
        store.complete_pending_activation(&token).await
    }

    #[tokio::test]
    async fn alm6_staging_preflights_space_before_fetching() -> Result<()> {
        let root = TempDir::new()?;
        let (_, bundle, selection) = raw_store_fixture(&root, "2026.08.1")?;
        let store = AnimeBundleStore::with_dependencies(
            root.path(),
            Arc::new(MemoryFetcher {
                artifacts: Arc::new(HashMap::new()),
            }),
            Arc::new(FixedDiskSpace(1)),
        );
        let error = store
            .stage_bundle(&bundle, &selection)
            .await
            .expect_err("insufficient disk must fail");
        assert!(
            error
                .to_string()
                .contains("insufficient inference staging space")
        );
        Ok(())
    }

    #[tokio::test]
    async fn alm6_profile_rebuild_reuses_exact_artifacts_without_refetching() -> Result<()> {
        let root = TempDir::new()?;
        let (_, bundle, selection) = raw_store_fixture(&root, "2026.08.1")?;
        let model = b"model".to_vec();
        let worker = b"bin".to_vec();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let artifacts = Arc::new(HashMap::from([
            (bundle.manifest().model.url.clone(), model),
            (selection.cpu_fallback().artifact.url.clone(), worker),
        ]));
        let store = AnimeBundleStore::with_dependencies(
            root.path(),
            Arc::new(CountingFetcher {
                artifacts,
                calls: calls.clone(),
            }),
            Arc::new(FixedDiskSpace(u64::MAX)),
        );

        let first_staged = store.stage_bundle(&bundle, &selection).await?;
        assert_eq!(first_staged.model_origin, StagedArtifactOrigin::Downloaded);
        assert!(
            first_staged
                .runtimes()
                .iter()
                .all(|runtime| runtime.origin == StagedArtifactOrigin::Downloaded)
        );
        let first = store
            .activate(first_staged, profile_for(&bundle, selection.preferred()))
            .await?;
        complete_activation(&store, &first).await?;
        assert_eq!(
            calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            2
        );

        let rebuilt = store.stage_bundle(&bundle, &selection).await?;
        assert_eq!(rebuilt.model_origin, StagedArtifactOrigin::Existing);
        assert_eq!(
            rebuilt.model_path(),
            store.resolve_relative(&first.model.relative_file)?
        );
        assert!(
            rebuilt
                .runtimes()
                .iter()
                .all(|runtime| runtime.origin == StagedArtifactOrigin::Existing)
        );
        assert_eq!(
            calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            2,
            "an exact profile rebuild must not refetch model or runtime artifacts"
        );

        let rebuilt_active = store
            .activate(rebuilt, profile_for(&bundle, selection.preferred()))
            .await?;
        assert_eq!(store.load_active()?, Some(rebuilt_active));
        assert_eq!(
            calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            2
        );
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn alm6_active_and_reusable_runtime_repairs_missing_execute_permission() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let root = TempDir::new()?;
        let (store, bundle, selection) = raw_store_fixture(&root, "2026.08.1")?;
        let staged = store.stage_bundle(&bundle, &selection).await?;
        let active = store
            .activate(staged, profile_for(&bundle, selection.preferred()))
            .await?;
        complete_activation(&store, &active).await?;
        let entrypoint = store
            .resolve_relative(&active.runtimes[0].relative_root)?
            .join(&active.runtimes[0].relative_entrypoint);

        let mut permissions = std_fs::metadata(&entrypoint)?.permissions();
        permissions.set_mode(0o600);
        std_fs::set_permissions(&entrypoint, permissions)?;
        assert!(store.load_active()?.is_some());
        assert_ne!(
            std_fs::metadata(&entrypoint)?.permissions().mode() & 0o111,
            0
        );

        let mut permissions = std_fs::metadata(&entrypoint)?.permissions();
        permissions.set_mode(0o600);
        std_fs::set_permissions(&entrypoint, permissions)?;
        let rebuilt = store.stage_bundle(&bundle, &selection).await?;
        assert!(
            rebuilt
                .runtimes()
                .iter()
                .all(|runtime| runtime.origin == StagedArtifactOrigin::Existing)
        );
        assert_ne!(
            std_fs::metadata(&entrypoint)?.permissions().mode() & 0o111,
            0
        );
        Ok(())
    }

    #[tokio::test]
    async fn alm6_reuse_refetches_and_replaces_same_size_model_tampering() -> Result<()> {
        let root = TempDir::new()?;
        let (_, bundle, selection) = raw_store_fixture(&root, "2026.08.1")?;
        let calls = Arc::new(Mutex::new(Vec::new()));
        let artifacts = Arc::new(HashMap::from([
            (bundle.manifest().model.url.clone(), b"model".to_vec()),
            (
                selection.cpu_fallback().artifact.url.clone(),
                b"bin".to_vec(),
            ),
        ]));
        let store = AnimeBundleStore::with_dependencies(
            root.path(),
            Arc::new(CountingFetcher {
                artifacts,
                calls: calls.clone(),
            }),
            Arc::new(FixedDiskSpace(u64::MAX)),
        );
        let staged = store.stage_bundle(&bundle, &selection).await?;
        let active = store
            .activate(staged, profile_for(&bundle, selection.preferred()))
            .await?;
        complete_activation(&store, &active).await?;
        std_fs::write(
            store.resolve_relative(&active.model.relative_file)?,
            b"tamp!",
        )?;

        let replacement = store.stage_bundle(&bundle, &selection).await?;
        assert_eq!(replacement.model_origin, StagedArtifactOrigin::Downloaded);
        let repaired = store
            .activate(replacement, profile_for(&bundle, selection.preferred()))
            .await?;
        assert_eq!(
            std_fs::read(store.resolve_relative(&repaired.model.relative_file)?)?,
            b"model"
        );
        assert_eq!(
            calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            3,
            "a corrupt model must be fetched exactly once and atomically replaced"
        );
        Ok(())
    }

    #[tokio::test]
    async fn alm6_reuse_refetches_corrupt_runtime_into_new_immutable_install() -> Result<()> {
        let root = TempDir::new()?;
        let (_, bundle, selection) = raw_store_fixture(&root, "2026.08.1")?;
        let calls = Arc::new(Mutex::new(Vec::new()));
        let artifacts = Arc::new(HashMap::from([
            (bundle.manifest().model.url.clone(), b"model".to_vec()),
            (
                selection.cpu_fallback().artifact.url.clone(),
                b"bin".to_vec(),
            ),
        ]));
        let store = AnimeBundleStore::with_dependencies(
            root.path(),
            Arc::new(CountingFetcher {
                artifacts,
                calls: calls.clone(),
            }),
            Arc::new(FixedDiskSpace(u64::MAX)),
        );
        let staged = store.stage_bundle(&bundle, &selection).await?;
        let active = store
            .activate(staged, profile_for(&bundle, selection.preferred()))
            .await?;
        complete_activation(&store, &active).await?;
        let corrupt_root = store.resolve_relative(&active.runtimes[0].relative_root)?;
        std_fs::remove_file(corrupt_root.join(&active.runtimes[0].relative_entrypoint))?;

        let replacement = store.stage_bundle(&bundle, &selection).await?;
        assert_eq!(replacement.model_origin, StagedArtifactOrigin::Existing);
        assert_eq!(
            replacement.runtimes[0].origin,
            StagedArtifactOrigin::Downloaded
        );
        let repaired = store
            .activate(replacement, profile_for(&bundle, selection.preferred()))
            .await?;
        let repaired_root = store.resolve_relative(&repaired.runtimes[0].relative_root)?;
        assert_ne!(repaired_root, corrupt_root);
        assert!(
            repaired_root
                .join(&repaired.runtimes[0].relative_entrypoint)
                .is_file()
        );
        assert!(store.load_previous()?.is_none());
        assert_eq!(
            calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            3,
            "only the corrupt runtime should be fetched again"
        );
        Ok(())
    }

    #[tokio::test]
    async fn alm6_staging_reuses_exact_artifacts_and_fetches_only_missing_runtime() -> Result<()> {
        let root = TempDir::new()?;
        let (_, first_bundle, first_selection) = raw_store_fixture(&root, "2026.08.1")?;
        let old_runtime = first_selection.cpu_fallback().artifact.clone();
        let new_worker = b"new".to_vec();
        let mut second_manifest = first_bundle.manifest().clone();
        second_manifest.bundle_version = "2026.08.2".to_string();
        let mut new_runtime = old_runtime.clone();
        new_runtime.priority = old_runtime.priority.saturating_sub(1);
        new_runtime.revision = "worker-metal-v2".to_string();
        new_runtime.url = "https://releases.example/worker-metal-v2".to_string();
        new_runtime.sha256 = sha256_prefixed(&new_worker);
        new_runtime.size_bytes = new_worker.len() as u64;
        new_runtime.installed_size_bytes = new_worker.len() as u64;
        second_manifest.runtimes.push(new_runtime.clone());
        let second_bundle = validated(second_manifest);
        let second_selection = AnimeRuntimeSelection {
            candidates: vec![
                ResolvedAnimeRuntime {
                    artifact: new_runtime.clone(),
                    execution_backend: AnimeExecutionBackend::Metal,
                    device_id: Some("gpu-0".to_string()),
                },
                ResolvedAnimeRuntime {
                    artifact: old_runtime.clone(),
                    execution_backend: AnimeExecutionBackend::Cpu,
                    device_id: None,
                },
            ],
        };
        let calls = Arc::new(Mutex::new(Vec::new()));
        let artifacts = Arc::new(HashMap::from([
            (first_bundle.manifest().model.url.clone(), b"model".to_vec()),
            (old_runtime.url.clone(), b"bin".to_vec()),
            (new_runtime.url.clone(), new_worker),
        ]));
        let store = AnimeBundleStore::with_dependencies(
            root.path(),
            Arc::new(CountingFetcher {
                artifacts,
                calls: calls.clone(),
            }),
            Arc::new(FixedDiskSpace(u64::MAX)),
        );

        let first_staged = store.stage_bundle(&first_bundle, &first_selection).await?;
        store
            .activate(
                first_staged,
                profile_for(&first_bundle, first_selection.preferred()),
            )
            .await?;
        let first = store.load_active()?.expect("first active bundle");
        complete_activation(&store, &first).await?;
        assert_eq!(
            calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            2
        );

        let mixed = store
            .stage_bundle(&second_bundle, &second_selection)
            .await?;
        assert_eq!(mixed.model_origin, StagedArtifactOrigin::Existing);
        assert_eq!(mixed.runtimes().len(), 2);
        assert_eq!(
            mixed
                .runtimes()
                .iter()
                .find(|runtime| runtime.manifest().artifact_key() == old_runtime.artifact_key())
                .map(|runtime| runtime.origin),
            Some(StagedArtifactOrigin::Existing)
        );
        assert_eq!(
            mixed
                .runtimes()
                .iter()
                .find(|runtime| runtime.manifest().artifact_key() == new_runtime.artifact_key())
                .map(|runtime| runtime.origin),
            Some(StagedArtifactOrigin::Downloaded)
        );
        let fetched = calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        assert_eq!(fetched.len(), 3);
        assert_eq!(
            fetched
                .iter()
                .filter(|url| *url == &first_bundle.manifest().model.url)
                .count(),
            1,
            "the exact installed model must not be fetched again"
        );
        assert_eq!(fetched.last(), Some(&new_runtime.url));

        let active = store
            .activate(
                mixed,
                profile_for(&second_bundle, second_selection.preferred()),
            )
            .await?;
        assert_eq!(active.bundle_version, "2026.08.2");
        assert_eq!(active.runtimes.len(), 2);
        assert!(store.load_previous()?.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn alm6_activation_persists_profile_and_rolls_back_atomically() -> Result<()> {
        let root = TempDir::new()?;
        let (store, first_bundle, first_selection) = raw_store_fixture(&root, "2026.08.1")?;
        let first_staged = store.stage_bundle(&first_bundle, &first_selection).await?;
        let first = store
            .activate(
                first_staged,
                profile_for(&first_bundle, first_selection.preferred()),
            )
            .await?;
        complete_activation(&store, &first).await?;
        assert_eq!(store.load_active()?, Some(first.clone()));
        assert_eq!(store.load_active_profile()?, Some(first.profile.clone()));

        let (store, second_bundle, second_selection) = raw_store_fixture(&root, "2026.08.2")?;
        let second_staged = store
            .stage_bundle(&second_bundle, &second_selection)
            .await?;
        let second = store
            .activate(
                second_staged,
                profile_for(&second_bundle, second_selection.preferred()),
            )
            .await?;
        assert_eq!(second.bundle_version, "2026.08.2");
        assert_eq!(
            store.load_previous()?.expect("previous").bundle_version,
            "2026.08.1"
        );

        let rolled_back = store.rollback_to_previous().await?;
        assert_eq!(rolled_back.bundle_version, "2026.08.1");
        assert_eq!(
            store
                .load_active_profile()?
                .expect("profile")
                .bundle_version,
            "2026.08.1"
        );
        assert!(store.load_previous()?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn alm9_reused_model_revision_preserves_exact_legacy_rollback_bytes() -> Result<()> {
        let root = TempDir::new()?;
        let (_, first_bundle, first_selection) = raw_store_fixture(&root, "2026.08.1")?;
        let mut second_manifest = first_bundle.manifest().clone();
        second_manifest.bundle_version = "2026.08.2".to_string();
        second_manifest.model.url = "https://releases.example/model-republished.gguf".to_string();
        let republished_model = b"other".to_vec();
        assert_eq!(republished_model.len(), b"model".len());
        second_manifest.model.sha256 = sha256_prefixed(&republished_model);
        second_manifest.model.size_bytes = republished_model.len() as u64;
        let second_bundle = validated(second_manifest);
        let second_selection = resolve_anime_runtime(
            &second_bundle,
            &host(AnimeHostOs::Macos, AnimeHostArch::X86_64, None, &[]),
        )?;
        let artifacts = Arc::new(HashMap::from([
            (first_bundle.manifest().model.url.clone(), b"model".to_vec()),
            (
                second_bundle.manifest().model.url.clone(),
                republished_model.clone(),
            ),
            (
                first_selection.cpu_fallback().artifact.url.clone(),
                b"bin".to_vec(),
            ),
        ]));
        let store = AnimeBundleStore::with_dependencies(
            root.path(),
            Arc::new(MemoryFetcher { artifacts }),
            Arc::new(FixedDiskSpace(u64::MAX)),
        );

        let first = store
            .activate(
                store.stage_bundle(&first_bundle, &first_selection).await?,
                profile_for(&first_bundle, first_selection.preferred()),
            )
            .await?;
        complete_activation(&store, &first).await?;

        // Simulate an on-disk descriptor produced before content-addressed
        // installs were introduced. Startup must continue to accept its safe
        // stored path, and the next activation must retain it byte-for-byte.
        let first_model_root =
            safe_artifact_parent(store.paths(), &first.model.relative_file, "models")?;
        let legacy_root = store
            .paths()
            .models()
            .join(&first.model.id)
            .join(&first.model.revision);
        std_fs::rename(&first_model_root, &legacy_root)?;
        let mut legacy_first = first.clone();
        legacy_first.model.relative_file = relative_path_string(
            store.paths().root(),
            &legacy_root.join("model.gguf"),
            "legacy installed model",
        )?;
        write_atomic_json(&store.paths().active_bundle(), &legacy_first)?;
        assert_eq!(store.load_active()?, Some(legacy_first.clone()));

        let failed = store
            .activate(
                store
                    .stage_bundle(&second_bundle, &second_selection)
                    .await?,
                profile_for(&second_bundle, second_selection.preferred()),
            )
            .await?;
        assert_eq!(failed.model.id, legacy_first.model.id);
        assert_eq!(failed.model.revision, legacy_first.model.revision);
        assert_ne!(failed.model.sha256, legacy_first.model.sha256);
        assert_ne!(failed.model.relative_file, legacy_first.model.relative_file);
        assert_eq!(std_fs::read(legacy_root.join("model.gguf"))?, b"model");
        assert_eq!(
            std_fs::read(store.resolve_relative(&failed.model.relative_file)?)?,
            republished_model
        );

        let restored = store
            .rollback_failed_activation(&failed)
            .await?
            .expect("legacy active descriptor must be restored");
        assert_eq!(restored, legacy_first);
        assert_eq!(store.load_active()?, Some(legacy_first));
        assert_eq!(std_fs::read(legacy_root.join("model.gguf"))?, b"model");
        Ok(())
    }

    #[tokio::test]
    async fn alm6_failed_first_activation_can_be_removed_transactionally() -> Result<()> {
        let root = TempDir::new()?;
        let (store, bundle, selection) = raw_store_fixture(&root, "2026.08.1")?;
        let staged = store.stage_bundle(&bundle, &selection).await?;
        let active = store
            .activate(staged, profile_for(&bundle, selection.preferred()))
            .await?;
        let model_root =
            safe_artifact_parent(store.paths(), &active.model.relative_file, "models")?;
        let runtime_roots = active
            .runtimes
            .iter()
            .map(|runtime| safe_artifact_parent(store.paths(), &runtime.relative_root, "runtimes"))
            .collect::<Result<Vec<_>>>()?;

        let restored = store.rollback_failed_activation(&active).await?;
        assert!(restored.is_none());
        assert!(store.load_active()?.is_none());
        assert!(store.load_previous()?.is_none());
        assert!(!store.paths().active_bundle().exists());
        assert!(!store.paths().active_runtime_profile().exists());
        assert!(!model_root.exists());
        assert!(runtime_roots.iter().all(|path| !path.exists()));
        Ok(())
    }

    #[tokio::test]
    async fn alm6_crash_after_first_install_commit_recovers_to_no_active_bundle() -> Result<()> {
        let root = TempDir::new()?;
        let (store, bundle, selection) = raw_store_fixture(&root, "2026.08.1")?;
        let staged = store.stage_bundle(&bundle, &selection).await?;
        store
            .activate(staged, profile_for(&bundle, selection.preferred()))
            .await?;

        assert!(store.paths().pending_activation().is_file());
        assert!(store.recover_pending_activation().await?.is_none());
        assert!(store.load_active()?.is_none());
        assert!(store.load_previous()?.is_none());
        assert!(!store.paths().pending_activation().exists());
        Ok(())
    }

    #[tokio::test]
    async fn alm6_crash_after_upgrade_commit_recovers_exact_previous_bundle() -> Result<()> {
        let root = TempDir::new()?;
        let (store, first_bundle, first_selection) = raw_store_fixture(&root, "2026.08.1")?;
        let first_staged = store.stage_bundle(&first_bundle, &first_selection).await?;
        let first = store
            .activate(
                first_staged,
                profile_for(&first_bundle, first_selection.preferred()),
            )
            .await?;
        complete_activation(&store, &first).await?;

        let (_, second_bundle, second_selection) = raw_store_fixture(&root, "2026.08.2")?;
        let second_staged = store
            .stage_bundle(&second_bundle, &second_selection)
            .await?;
        let second = store
            .activate(
                second_staged,
                profile_for(&second_bundle, second_selection.preferred()),
            )
            .await?;
        assert_eq!(store.load_active()?, Some(second));

        assert_eq!(
            store.recover_pending_activation().await?,
            Some(first.clone())
        );
        assert_eq!(store.load_active()?, Some(first));
        assert!(store.load_previous()?.is_none());
        assert!(!store.paths().pending_activation().exists());
        Ok(())
    }

    #[tokio::test]
    async fn alm6_delayed_live_failure_restores_both_prior_generations_with_assets() -> Result<()> {
        let root = TempDir::new()?;
        let (store, previous, active, pending) = three_generation_pending_fixture(&root).await?;
        let previous_assets = descriptor_asset_roots(&store, &previous)?;

        // Generic cleanup can run during shutdown/recovery. The durable
        // pending transaction must retain the second rollback generation even
        // though it is no longer named by the current pointer pair.
        store.cleanup_unreferenced_installs().await?;
        assert!(previous_assets.iter().all(|path| path.exists()));

        assert_eq!(
            store.rollback_failed_activation(&pending).await?,
            Some(active.clone())
        );
        assert_eq!(store.load_active()?, Some(active));
        assert_eq!(store.load_previous()?, Some(previous));
        assert!(previous_assets.iter().all(|path| path.exists()));
        assert!(!store.paths().pending_activation().exists());
        Ok(())
    }

    #[tokio::test]
    async fn alm6_crash_recovery_restores_both_prior_generations_with_assets() -> Result<()> {
        let root = TempDir::new()?;
        let (store, previous, active, _pending) = three_generation_pending_fixture(&root).await?;
        let previous_assets = descriptor_asset_roots(&store, &previous)?;

        store.cleanup_unreferenced_installs().await?;
        assert!(previous_assets.iter().all(|path| path.exists()));
        assert_eq!(
            store.recover_pending_activation().await?,
            Some(active.clone())
        );
        assert_eq!(store.load_active()?, Some(active));
        assert_eq!(store.load_previous()?, Some(previous));
        assert!(previous_assets.iter().all(|path| path.exists()));
        assert!(!store.paths().pending_activation().exists());
        Ok(())
    }

    #[tokio::test]
    async fn alm6_exact_completion_prunes_only_the_obsolete_prior_previous() -> Result<()> {
        let root = TempDir::new()?;
        let (store, previous, active, pending) = three_generation_pending_fixture(&root).await?;
        let previous_assets = descriptor_asset_roots(&store, &previous)?;
        let active_assets = descriptor_asset_roots(&store, &active)?;
        let pending_assets = descriptor_asset_roots(&store, &pending)?;

        complete_activation(&store, &pending).await?;

        assert!(!store.paths().pending_activation().exists());
        assert_eq!(store.load_active()?, Some(pending));
        assert_eq!(store.load_previous()?, Some(active));
        assert!(previous_assets.iter().all(|path| !path.exists()));
        assert!(active_assets.iter().all(|path| path.exists()));
        assert!(pending_assets.iter().all(|path| path.exists()));
        Ok(())
    }

    #[tokio::test]
    async fn alm6_precommit_pending_marker_is_cleared_without_rolling_active() -> Result<()> {
        let root = TempDir::new()?;
        let (store, bundle, selection) = raw_store_fixture(&root, "2026.08.1")?;
        let staged = store.stage_bundle(&bundle, &selection).await?;
        let active = store
            .activate(staged, profile_for(&bundle, selection.preferred()))
            .await?;
        complete_activation(&store, &active).await?;

        let mut uncommitted = active.clone();
        uncommitted.activated_at = "2030-01-01T00:00:00Z".to_string();
        let pending = PendingAnimeBundleActivation {
            schema_version: PENDING_ACTIVATION_SCHEMA_VERSION,
            activation_id: Uuid::new_v4().to_string(),
            active: uncommitted,
            prior_active: Some(active.clone()),
            prior_previous: None,
            created_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
        };
        write_atomic_json(&store.paths().pending_activation(), &pending)?;

        assert_eq!(
            store.recover_pending_activation().await?,
            Some(active.clone())
        );
        assert_eq!(store.load_active()?, Some(active));
        assert!(!store.paths().pending_activation().exists());
        Ok(())
    }

    #[tokio::test]
    async fn alm6_active_manifest_ignores_latest_cache_overwrite_and_corruption() -> Result<()> {
        let root = TempDir::new()?;
        let (store, bundle, selection) = raw_store_fixture(&root, "2026.08.1")?;
        let staged = store.stage_bundle(&bundle, &selection).await?;
        let active = store
            .activate(staged, profile_for(&bundle, selection.preferred()))
            .await?;
        let policy = AnimeBundleCompatibilityPolicy::development(Version::new(0, 1, 0));

        let mut latest_manifest = bundle.manifest().clone();
        latest_manifest.bundle_version = "2026.08.2".to_string();
        let latest = validate_anime_bundle(latest_manifest, &policy)?;
        store.cache_validated_manifest(&latest)?;
        assert_eq!(
            store
                .load_manifest_for_descriptor(&active, &policy)?
                .manifest()
                .bundle_version,
            "2026.08.1"
        );

        std_fs::write(store.paths().cached_manifest(), b"{corrupt-latest")?;
        assert_eq!(
            store
                .load_manifest_for_descriptor(&active, &policy)?
                .manifest_fingerprint(),
            active.manifest_fingerprint
        );
        Ok(())
    }

    #[tokio::test]
    async fn alm6_legacy_active_manifest_migrates_from_exact_latest_cache() -> Result<()> {
        let root = TempDir::new()?;
        let (store, bundle, selection) = raw_store_fixture(&root, "2026.08.1")?;
        let staged = store.stage_bundle(&bundle, &selection).await?;
        let active = store
            .activate(staged, profile_for(&bundle, selection.preferred()))
            .await?;
        let keyed = store
            .paths()
            .manifest_by_fingerprint(&active.manifest_fingerprint)?;
        std_fs::remove_file(&keyed)?;
        write_atomic_json(
            &store.paths().cached_manifest(),
            &CachedAnimeBundleManifest {
                schema_version: ANIME_BUNDLE_SCHEMA_VERSION,
                manifest_fingerprint: bundle.manifest_fingerprint().to_string(),
                manifest: bundle.manifest().clone(),
                cached_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            },
        )?;
        let policy = AnimeBundleCompatibilityPolicy::development(Version::new(0, 1, 0));

        let migrated = store.load_manifest_for_descriptor(&active, &policy)?;
        assert_eq!(
            migrated.manifest_fingerprint(),
            bundle.manifest_fingerprint()
        );
        assert!(keyed.is_file());
        Ok(())
    }

    #[tokio::test]
    async fn alm6_older_completion_cannot_clear_newer_pending_marker() -> Result<()> {
        let root = TempDir::new()?;
        let (store, bundle, selection) = raw_store_fixture(&root, "2026.08.1")?;
        let staged = store.stage_bundle(&bundle, &selection).await?;
        let active = store
            .activate(staged, profile_for(&bundle, selection.preferred()))
            .await?;
        let old_token = store.pending_activation_token(&active)?;
        let mut newer = store
            .load_pending_activation()?
            .expect("pending activation");
        newer.activation_id = Uuid::new_v4().to_string();
        write_atomic_json(&store.paths().pending_activation(), &newer)?;

        assert!(store.complete_pending_activation(&old_token).await.is_err());
        assert_eq!(store.load_pending_activation()?, Some(newer));
        assert_eq!(store.load_active()?, Some(active));
        Ok(())
    }

    #[tokio::test]
    async fn alm6_cancellable_staging_removes_partial_download_transaction() -> Result<()> {
        let root = TempDir::new()?;
        let (_, bundle, selection) = raw_store_fixture(&root, "2026.08.1")?;
        let started = Arc::new(Notify::new());
        let store = AnimeBundleStore::with_dependencies(
            root.path(),
            Arc::new(BlockingFetcher {
                started: started.clone(),
            }),
            Arc::new(FixedDiskSpace(u64::MAX)),
        );
        let cancellation = CancellationToken::new();
        let task_store = store.clone();
        let task_cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            task_store
                .stage_bundle_with_cancellation(&bundle, &selection, &task_cancellation)
                .await
        });
        started.notified().await;
        cancellation.cancel();
        let error = task.await?.expect_err("cancellation must stop staging");
        assert!(error.to_string().contains("cancelled"));
        let mut entries = async_fs::read_dir(store.paths().staging()).await?;
        assert!(entries.next_entry().await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn alm6_cancellable_activation_cleans_staging_before_install() -> Result<()> {
        let root = TempDir::new()?;
        let (store, bundle, selection) = raw_store_fixture(&root, "2026.08.1")?;
        let staged = store.stage_bundle(&bundle, &selection).await?;
        let stage_root = staged.stage_root().to_path_buf();
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let error = store
            .activate_with_cancellation(
                staged,
                profile_for(&bundle, selection.preferred()),
                &cancellation,
            )
            .await
            .expect_err("cancellation must stop activation");
        assert!(error.to_string().contains("cancelled"));
        assert!(!stage_root.exists());
        assert!(store.load_active()?.is_none());
        assert!(!store.paths().pending_activation().exists());
        Ok(())
    }

    #[test]
    fn alm6_archive_copy_checks_cancellation_between_bounded_chunks() {
        struct CancelAfterFirstRead {
            remaining: usize,
            cancellation: CancellationToken,
        }

        impl Read for CancelAfterFirstRead {
            fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
                if self.remaining == 0 {
                    return Ok(0);
                }
                let read = self.remaining.min(buffer.len());
                buffer[..read].fill(7);
                self.remaining -= read;
                self.cancellation.cancel();
                Ok(read)
            }
        }

        let cancellation = CancellationToken::new();
        let mut input = CancelAfterFirstRead {
            remaining: CANCELLABLE_IO_CHUNK_BYTES * 4,
            cancellation: cancellation.clone(),
        };
        let mut output = Vec::new();
        let error = copy_cancellable(&mut input, &mut output, &cancellation)
            .expect_err("copy must observe cancellation before its second chunk");
        assert!(error.to_string().contains("cancelled"));
        assert_eq!(output.len(), CANCELLABLE_IO_CHUNK_BYTES);
    }

    #[test]
    fn alm6_model_hash_checks_cancellation_between_bounded_chunks() {
        struct CancelAfterFirstRead {
            remaining: usize,
            cancellation: CancellationToken,
        }

        impl Read for CancelAfterFirstRead {
            fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
                if self.remaining == 0 {
                    return Ok(0);
                }
                let read = self.remaining.min(buffer.len());
                buffer[..read].fill(9);
                self.remaining -= read;
                self.cancellation.cancel();
                Ok(read)
            }
        }

        let cancellation = CancellationToken::new();
        let mut input = CancelAfterFirstRead {
            remaining: CANCELLABLE_IO_CHUNK_BYTES * 4,
            cancellation: cancellation.clone(),
        };
        let error = sha256_reader_cancellable(&mut input, &cancellation)
            .expect_err("hash must observe cancellation before its second chunk");
        assert!(error.to_string().contains("cancelled"));
        assert_eq!(input.remaining, CANCELLABLE_IO_CHUNK_BYTES * 3);
    }

    #[tokio::test]
    async fn alm6_activation_failure_removes_staging_and_partial_installs() -> Result<()> {
        let root = TempDir::new()?;
        let (store, bundle, selection) = raw_store_fixture(&root, "2026.08.1")?;
        let staged = store.stage_bundle(&bundle, &selection).await?;
        let stage_root = staged.stage_root().to_path_buf();
        let model_root = store.paths().model_install_root(&bundle.manifest().model);
        let runtime_roots = staged
            .runtimes()
            .iter()
            .map(|runtime| store.paths().runtime_install_root(runtime.manifest()))
            .collect::<Vec<_>>();
        std_fs::remove_file(staged.runtimes()[0].entrypoint())?;

        let error = store
            .activate(staged, profile_for(&bundle, selection.preferred()))
            .await
            .expect_err("missing installed entrypoint must fail activation");
        assert!(error.to_string().contains("entrypoint is missing"));
        assert!(!stage_root.exists());
        assert!(!model_root.exists());
        assert!(runtime_roots.iter().all(|path| !path.exists()));
        assert!(store.load_active()?.is_none());
        assert!(store.load_previous()?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn alm6_recovery_prunes_installs_left_by_cancelled_activation() -> Result<()> {
        let root = TempDir::new()?;
        let (store, bundle, selection) = raw_store_fixture(&root, "2026.08.1")?;
        let staged = store.stage_bundle(&bundle, &selection).await?;
        let model_root = store.paths().model_install_root(&bundle.manifest().model);
        let runtime_roots = staged
            .runtimes()
            .iter()
            .map(|runtime| store.paths().runtime_install_root(runtime.manifest()))
            .collect::<Vec<_>>();

        // Model/runtime moves occur at await boundaries before the descriptor
        // commit. This is the exact durable state left if the lifecycle future
        // is cancelled immediately afterward.
        let cancellation = CancellationToken::new();
        store.install_model(&staged, &cancellation).await?;
        for runtime in staged.runtimes() {
            store.install_runtime(runtime, &cancellation).await?;
        }
        drop(staged);
        assert!(model_root.exists());
        assert!(runtime_roots.iter().all(|path| path.exists()));

        let removed = store.cleanup_unreferenced_installs().await?;
        assert_eq!(removed, 1 + runtime_roots.len());
        assert!(!model_root.exists());
        assert!(runtime_roots.iter().all(|path| !path.exists()));
        Ok(())
    }

    #[tokio::test]
    async fn alm6_recovery_retains_active_assets_and_removes_only_orphans() -> Result<()> {
        let root = TempDir::new()?;
        let (store, bundle, selection) = raw_store_fixture(&root, "2026.08.1")?;
        let staged = store.stage_bundle(&bundle, &selection).await?;
        let active = store
            .activate(staged, profile_for(&bundle, selection.preferred()))
            .await?;
        let active_model_root =
            safe_artifact_parent(store.paths(), &active.model.relative_file, "models")?;
        let active_runtime_roots = active
            .runtimes
            .iter()
            .map(|runtime| safe_artifact_parent(store.paths(), &runtime.relative_root, "runtimes"))
            .collect::<Result<Vec<_>>>()?;
        let orphan_model = store.paths().models().join("orphan-model").join("r1");
        let orphan_runtime = store
            .paths()
            .runtimes()
            .join("orphan-rev")
            .join("orphan-runtime");
        async_fs::create_dir_all(&orphan_model).await?;
        async_fs::create_dir_all(&orphan_runtime).await?;

        assert_eq!(store.cleanup_unreferenced_installs().await?, 2);
        assert!(active_model_root.exists());
        assert!(active_runtime_roots.iter().all(|path| path.exists()));
        assert!(!orphan_model.exists());
        assert!(!orphan_runtime.exists());
        assert_eq!(store.load_active()?, Some(active));
        Ok(())
    }

    #[tokio::test]
    async fn alm6_deterministic_only_profile_cannot_activate() -> Result<()> {
        let root = TempDir::new()?;
        let (store, bundle, selection) = raw_store_fixture(&root, "2026.08.1")?;
        let staged = store.stage_bundle(&bundle, &selection).await?;
        let stage_root = staged.stage_root().to_path_buf();
        let mut profile = profile_for(&bundle, selection.preferred());
        profile.probe_result = AnimeRuntimeProbeResult::DeterministicOnly;
        let error = store
            .activate(staged, profile)
            .await
            .expect_err("deterministic-only profile must not activate");
        assert!(error.to_string().contains("deterministic-only"));
        assert!(store.load_active()?.is_none());
        assert!(!stage_root.exists());
        Ok(())
    }

    #[test]
    fn alm6_runtime_profile_fingerprint_detects_tampering() -> Result<()> {
        let bundle = validated(complete_manifest());
        let selection = resolve_anime_runtime(
            &bundle,
            &host(AnimeHostOs::Linux, AnimeHostArch::X86_64, None, &[]),
        )?;
        let mut profile = profile_for(&bundle, selection.preferred()).seal()?;
        profile.cpu_thread_count += 1;
        assert!(profile.validate().is_err());
        Ok(())
    }

    #[test]
    fn alm6_relative_paths_reject_cross_platform_escape_forms() {
        for path in [
            "../worker",
            "/worker",
            "C:/worker",
            "bin\\worker",
            "./worker",
        ] {
            assert!(
                validate_safe_relative_path(path, "fixture").is_err(),
                "{path}"
            );
        }
        assert!(validate_safe_relative_path("bin/llama-server", "fixture").is_ok());
    }

    #[test]
    fn alm6_tar_links_are_rejected() -> Result<()> {
        let root = TempDir::new()?;
        let archive_path = root.path().join("runtime.tar.gz");
        {
            let output = File::create(&archive_path)?;
            let encoder = flate2::write::GzEncoder::new(output, flate2::Compression::default());
            let mut tar = tar::Builder::new(encoder);
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(tar::EntryType::Symlink);
            header.set_size(0);
            header.set_mode(0o777);
            header.set_cksum();
            tar.append_link(&mut header, "llama-server", "../outside")?;
            tar.finish()?;
        }
        let mut manifest = runtime(
            AnimeHostOs::Linux,
            AnimeHostArch::X86_64,
            AnimeRuntimeBackend::Cpu,
            Some(AnimeDeviceClass::Cpu),
            HASH_A,
        );
        manifest.archive_format = AnimeRuntimeArchiveFormat::TarGz;
        assert!(
            extract_runtime_archive_blocking(&archive_path, &root.path().join("out"), &manifest)
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn alm6_numeric_version_comparison_is_not_lexicographic() {
        assert!(version_at_least(Some("12.10"), Some("12.9")));
        assert!(!version_at_least(Some("9.9"), Some("10.0")));
        assert!(!version_at_least(None, Some("1.0")));
    }

    #[test]
    fn alm6_raw_archive_fixture_is_consumed_without_buffering_contract_changes() -> Result<()> {
        let root = TempDir::new()?;
        let input = root.path().join("worker.raw");
        std_fs::write(&input, b"bin")?;
        let manifest = runtime(
            AnimeHostOs::Linux,
            AnimeHostArch::X86_64,
            AnimeRuntimeBackend::Cpu,
            Some(AnimeDeviceClass::Cpu),
            HASH_A,
        );
        let output = root.path().join("out");
        extract_runtime_archive_blocking(&input, &output, &manifest)?;
        assert_eq!(std_fs::read(output.join("llama-server"))?, b"bin");
        Ok(())
    }
}
