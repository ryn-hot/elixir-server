use std::{
    collections::BTreeSet,
    process::Stdio,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{AnyPool, Row};
use tokio::{process::Command, time::timeout};
use uuid::Uuid;

use crate::metrics;

const STARTUP_PROBE_TIMEOUT: Duration = Duration::from_secs(8);
const FFMPEG_CAPABILITY_TIMEOUT: Duration = Duration::from_secs(5);
pub const HARDWARE_READINESS_SCHEMA_VERSION: u32 = 1;
const MATRIX_STATUS_SUPPORTED: &str = "supported";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HardwareApi {
    VideoToolbox,
    Vaapi,
    Qsv,
    Nvenc,
    Amf,
}

impl HardwareApi {
    pub const ALL: [Self; 5] = [
        Self::VideoToolbox,
        Self::Vaapi,
        Self::Qsv,
        Self::Nvenc,
        Self::Amf,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::VideoToolbox => "videotoolbox",
            Self::Vaapi => "vaapi",
            Self::Qsv => "qsv",
            Self::Nvenc => "nvenc",
            Self::Amf => "amf",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "videotoolbox" | "video_toolbox" | "vt" => Some(Self::VideoToolbox),
            "vaapi" => Some(Self::Vaapi),
            "qsv" | "quick_sync" | "quicksync" => Some(Self::Qsv),
            "nvenc" | "cuda" => Some(Self::Nvenc),
            "amf" => Some(Self::Amf),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HardwarePreference {
    Auto,
    Off,
    Api(HardwareApi),
}

impl HardwarePreference {
    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" | "auto" => Self::Auto,
            "off" | "false" | "disabled" | "none" | "software" => Self::Off,
            value => HardwareApi::parse(value)
                .map(Self::Api)
                .unwrap_or(Self::Off),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HardwareFallbackPolicy {
    Software,
    Fail,
}

impl HardwareFallbackPolicy {
    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "fail" | "error" => Self::Fail,
            _ => Self::Software,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Software => "software",
            Self::Fail => "fail",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardwareCodecSupport {
    pub api: String,
    pub codec: String,
    pub ffmpeg_name: String,
}

impl HardwareCodecSupport {
    fn new(api: HardwareApi, codec: &str, ffmpeg_name: &str) -> Self {
        Self {
            api: api.as_str().to_string(),
            codec: codec.to_string(),
            ffmpeg_name: ffmpeg_name.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardwareStartupProbe {
    pub api: String,
    pub operation: String,
    pub ok: bool,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HardwareReadinessStatus {
    Available,
    DisabledByConfig,
    NotApplicable,
    FfmpegMissingSupport,
    DriverTooOld,
    DriverRuntimeIncompatible,
    UnsupportedGpu,
    PermissionDenied,
    DeviceBusyOrSessionLimit,
    ProbeTimeout,
    ProbeFailedUnknown,
}

impl HardwareReadinessStatus {
    pub const ALL: [Self; 11] = [
        Self::Available,
        Self::DisabledByConfig,
        Self::NotApplicable,
        Self::FfmpegMissingSupport,
        Self::DriverTooOld,
        Self::DriverRuntimeIncompatible,
        Self::UnsupportedGpu,
        Self::PermissionDenied,
        Self::DeviceBusyOrSessionLimit,
        Self::ProbeTimeout,
        Self::ProbeFailedUnknown,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::DisabledByConfig => "disabled_by_config",
            Self::NotApplicable => "not_applicable",
            Self::FfmpegMissingSupport => "ffmpeg_missing_support",
            Self::DriverTooOld => "driver_too_old",
            Self::DriverRuntimeIncompatible => "driver_runtime_incompatible",
            Self::UnsupportedGpu => "unsupported_gpu",
            Self::PermissionDenied => "permission_denied",
            Self::DeviceBusyOrSessionLimit => "device_busy_or_session_limit",
            Self::ProbeTimeout => "probe_timeout",
            Self::ProbeFailedUnknown => "probe_failed_unknown",
        }
    }

    pub fn parse(raw: &str) -> Self {
        match raw {
            "available" => Self::Available,
            "disabled_by_config" => Self::DisabledByConfig,
            "not_applicable" => Self::NotApplicable,
            "ffmpeg_missing_support" => Self::FfmpegMissingSupport,
            "driver_too_old" => Self::DriverTooOld,
            "driver_runtime_incompatible" => Self::DriverRuntimeIncompatible,
            "unsupported_gpu" => Self::UnsupportedGpu,
            "permission_denied" => Self::PermissionDenied,
            "device_busy_or_session_limit" => Self::DeviceBusyOrSessionLimit,
            "probe_timeout" => Self::ProbeTimeout,
            _ => Self::ProbeFailedUnknown,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostOsInventory {
    pub family: String,
    pub version: Option<String>,
    pub arch: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostGpuInventory {
    pub vendor: Option<String>,
    pub model: Option<String>,
    pub device_id: Option<String>,
    pub driver_version: Option<String>,
    #[serde(default)]
    pub raw: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FfmpegHardwareInventory {
    pub path: Option<String>,
    pub version: Option<String>,
    pub sha256: Option<String>,
    pub hwaccels: Vec<String>,
    pub encoders: Vec<String>,
    pub decoders: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostHardwareInventory {
    pub os: HostOsInventory,
    pub gpus: Vec<HostGpuInventory>,
    pub ffmpeg: FfmpegHardwareInventory,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardwareCodecMatrixEntry {
    pub codec: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bit_depth: Option<u8>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pixel_formats: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ffmpeg_encoder: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ffmpeg_decoder: Option<String>,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardwareFilterMatrix {
    pub hardware_scale: String,
    pub hardware_tonemap: String,
    pub subtitle_burn_in_requires_software: bool,
}

impl Default for HardwareFilterMatrix {
    fn default() -> Self {
        Self {
            hardware_scale: "unknown".to_string(),
            hardware_tonemap: "unsupported".to_string(),
            subtitle_burn_in_requires_software: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardwareCapabilityMatrix {
    pub schema_version: u32,
    pub api: String,
    pub status: HardwareReadinessStatus,
    #[serde(default)]
    pub encode: Vec<HardwareCodecMatrixEntry>,
    #[serde(default)]
    pub decode: Vec<HardwareCodecMatrixEntry>,
    #[serde(default)]
    pub filters: HardwareFilterMatrix,
}

impl HardwareCapabilityMatrix {
    pub fn empty(api: HardwareApi, status: HardwareReadinessStatus) -> Self {
        Self {
            schema_version: HARDWARE_READINESS_SCHEMA_VERSION,
            api: api.as_str().to_string(),
            status,
            encode: Vec::new(),
            decode: Vec::new(),
            filters: HardwareFilterMatrix::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardwareProbeReport {
    pub api: String,
    pub startup_probes: Vec<HardwareStartupProbe>,
    #[serde(default)]
    pub capability_probes: Vec<HardwareCapabilityProbeResult>,
    pub detection_errors: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HardwareCapabilityProbeDirection {
    Encode,
    Decode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardwareCapabilityProbeSpec {
    pub direction: HardwareCapabilityProbeDirection,
    pub codec: String,
    pub profile: Option<String>,
    pub bit_depth: Option<u8>,
    pub pixel_format: Option<String>,
    pub ffmpeg_component: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardwareCapabilityProbeResult {
    pub spec: HardwareCapabilityProbeSpec,
    pub ok: bool,
    pub status: HardwareReadinessStatus,
    pub duration_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardwareReadinessRecord {
    pub id: String,
    pub host_fingerprint: String,
    pub accelerator_id: String,
    pub api: String,
    pub os_family: String,
    pub os_version: Option<String>,
    pub arch: String,
    pub gpu_vendor: Option<String>,
    pub gpu_model: Option<String>,
    pub gpu_device_id: Option<String>,
    pub gpu_driver_version: Option<String>,
    pub ffmpeg_path: Option<String>,
    pub ffmpeg_version: Option<String>,
    pub ffmpeg_sha256: Option<String>,
    pub elixir_accel_schema_version: u32,
    pub status: HardwareReadinessStatus,
    pub status_reason: String,
    pub user_message_code: String,
    pub capabilities: HardwareCapabilityMatrix,
    pub inventory: HostHardwareInventory,
    pub probe_report: HardwareProbeReport,
    pub raw_error_excerpt: Option<String>,
    pub stale: bool,
    pub last_checked_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl HardwareReadinessRecord {
    pub fn new(
        host_fingerprint: String,
        accelerator_id: impl Into<String>,
        api: HardwareApi,
        inventory: HostHardwareInventory,
        gpu: Option<&HostGpuInventory>,
        status: HardwareReadinessStatus,
        status_reason: impl Into<String>,
        user_message_code: impl Into<String>,
        capabilities: HardwareCapabilityMatrix,
        probe_report: HardwareProbeReport,
        raw_error_excerpt: Option<String>,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4().to_string(),
            host_fingerprint,
            accelerator_id: accelerator_id.into(),
            api: api.as_str().to_string(),
            os_family: inventory.os.family.clone(),
            os_version: inventory.os.version.clone(),
            arch: inventory.os.arch.clone(),
            gpu_vendor: gpu.and_then(|gpu| gpu.vendor.clone()),
            gpu_model: gpu.and_then(|gpu| gpu.model.clone()),
            gpu_device_id: gpu.and_then(|gpu| gpu.device_id.clone()),
            gpu_driver_version: gpu.and_then(|gpu| gpu.driver_version.clone()),
            ffmpeg_path: inventory.ffmpeg.path.clone(),
            ffmpeg_version: inventory.ffmpeg.version.clone(),
            ffmpeg_sha256: inventory.ffmpeg.sha256.clone(),
            elixir_accel_schema_version: HARDWARE_READINESS_SCHEMA_VERSION,
            status,
            status_reason: status_reason.into(),
            user_message_code: user_message_code.into(),
            capabilities,
            inventory,
            probe_report,
            raw_error_excerpt,
            stale: false,
            last_checked_at: now,
            created_at: now,
            updated_at: now,
        }
    }
}

pub fn hardware_readiness_message_code(
    api: HardwareApi,
    status: HardwareReadinessStatus,
) -> &'static str {
    match (api, status) {
        (_, HardwareReadinessStatus::Available) => "hardware_acceleration_available",
        (_, HardwareReadinessStatus::DisabledByConfig) => "hardware_acceleration_disabled",
        (HardwareApi::Nvenc, HardwareReadinessStatus::DriverRuntimeIncompatible)
        | (HardwareApi::Nvenc, HardwareReadinessStatus::DriverTooOld) => {
            "nvidia_driver_update_required"
        }
        (HardwareApi::Amf, HardwareReadinessStatus::DriverRuntimeIncompatible)
        | (HardwareApi::Amf, HardwareReadinessStatus::DriverTooOld) => "amd_driver_update_required",
        (_, HardwareReadinessStatus::DriverRuntimeIncompatible)
        | (_, HardwareReadinessStatus::DriverTooOld) => "hardware_driver_update_required",
        (HardwareApi::Vaapi, HardwareReadinessStatus::PermissionDenied)
        | (HardwareApi::Qsv, HardwareReadinessStatus::PermissionDenied) => {
            "linux_render_device_permission_denied"
        }
        (_, HardwareReadinessStatus::FfmpegMissingSupport) => "ffmpeg_hardware_support_missing",
        (_, HardwareReadinessStatus::UnsupportedGpu) => "hardware_acceleration_unsupported_gpu",
        (_, HardwareReadinessStatus::PermissionDenied) => "hardware_device_permission_denied",
        (_, HardwareReadinessStatus::NotApplicable) => "hardware_acceleration_not_applicable",
        (_, HardwareReadinessStatus::DeviceBusyOrSessionLimit) => "hardware_device_busy",
        (_, HardwareReadinessStatus::ProbeTimeout) => "hardware_probe_timeout",
        (_, HardwareReadinessStatus::ProbeFailedUnknown) => "hardware_probe_failed",
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardwareCapabilities {
    pub platform: String,
    pub ffmpeg_version: Option<String>,
    pub available_apis: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capability_matrices: Vec<HardwareCapabilityMatrix>,
    pub supported_decode_codecs: Vec<HardwareCodecSupport>,
    pub supported_encode_codecs: Vec<HardwareCodecSupport>,
    pub max_sessions: Option<u32>,
    pub hdr_tone_mapping: bool,
    pub subtitle_burn_in_limitations: Vec<String>,
    pub startup_probes: Vec<HardwareStartupProbe>,
    pub detection_errors: Vec<String>,
}

impl Default for HardwareCapabilities {
    fn default() -> Self {
        Self {
            platform: current_platform(),
            ffmpeg_version: None,
            available_apis: Vec::new(),
            capability_matrices: Vec::new(),
            supported_decode_codecs: Vec::new(),
            supported_encode_codecs: Vec::new(),
            max_sessions: None,
            hdr_tone_mapping: false,
            subtitle_burn_in_limitations: vec![
                "subtitle_burn_in_requires_software_filter".to_string(),
            ],
            startup_probes: Vec::new(),
            detection_errors: Vec::new(),
        }
    }
}

impl HardwareCapabilities {
    pub fn software_only() -> Self {
        Self::default()
    }

    pub fn is_api_available(&self, api: HardwareApi) -> bool {
        self.available_apis
            .iter()
            .any(|value| value.eq_ignore_ascii_case(api.as_str()))
    }

    pub fn encode_support(&self, api: HardwareApi, codec: &str) -> Option<&HardwareCodecSupport> {
        self.supported_encode_codecs.iter().find(|support| {
            support.api.eq_ignore_ascii_case(api.as_str())
                && support.codec.eq_ignore_ascii_case(codec)
        })
    }

    pub fn decode_support(&self, api: HardwareApi, codec: &str) -> Option<&HardwareCodecSupport> {
        self.supported_decode_codecs.iter().find(|support| {
            support.api.eq_ignore_ascii_case(api.as_str())
                && support.codec.eq_ignore_ascii_case(codec)
        })
    }

    pub fn preferred_api_for_encode(&self, codec: &str) -> Option<HardwareApi> {
        HardwareApi::ALL.into_iter().find(|api| {
            self.is_api_available(*api)
                && (self
                    .supported_encode_matrix_entry(*api, codec, None, None, None)
                    .is_some()
                    || (self.capability_matrices.is_empty()
                        && self.encode_support(*api, codec).is_some()))
        })
    }

    pub fn encode_matrix_entry(
        &self,
        api: HardwareApi,
        codec: &str,
        profile: Option<&str>,
        bit_depth: Option<u8>,
        pixel_format: Option<&str>,
    ) -> Option<&HardwareCodecMatrixEntry> {
        self.matrix_entries(api, true).find(|entry| {
            entry.codec.eq_ignore_ascii_case(codec)
                && matrix_optional_matches(entry.profile.as_deref(), profile)
                && matrix_optional_u8_matches(entry.bit_depth, bit_depth)
                && matrix_pixel_format_matches(&entry.pixel_formats, pixel_format)
        })
    }

    pub fn decode_matrix_entry(
        &self,
        api: HardwareApi,
        codec: &str,
        profile: Option<&str>,
        bit_depth: Option<u8>,
        pixel_format: Option<&str>,
    ) -> Option<&HardwareCodecMatrixEntry> {
        self.matrix_entries(api, false).find(|entry| {
            entry.codec.eq_ignore_ascii_case(codec)
                && matrix_optional_matches(entry.profile.as_deref(), profile)
                && matrix_optional_u8_matches(entry.bit_depth, bit_depth)
                && matrix_pixel_format_matches(&entry.pixel_formats, pixel_format)
        })
    }

    pub fn supported_encode_matrix_entry(
        &self,
        api: HardwareApi,
        codec: &str,
        profile: Option<&str>,
        bit_depth: Option<u8>,
        pixel_format: Option<&str>,
    ) -> Option<&HardwareCodecMatrixEntry> {
        self.encode_matrix_entry(api, codec, profile, bit_depth, pixel_format)
            .filter(|entry| entry.status == MATRIX_STATUS_SUPPORTED)
    }

    pub fn supported_decode_matrix_entry(
        &self,
        api: HardwareApi,
        codec: &str,
        profile: Option<&str>,
        bit_depth: Option<u8>,
        pixel_format: Option<&str>,
    ) -> Option<&HardwareCodecMatrixEntry> {
        self.decode_matrix_entry(api, codec, profile, bit_depth, pixel_format)
            .filter(|entry| entry.status == MATRIX_STATUS_SUPPORTED)
    }

    fn matrix_entries(
        &self,
        api: HardwareApi,
        encode: bool,
    ) -> impl Iterator<Item = &HardwareCodecMatrixEntry> {
        self.capability_matrices
            .iter()
            .filter(move |matrix| matrix.api.eq_ignore_ascii_case(api.as_str()))
            .flat_map(move |matrix| {
                if encode {
                    matrix.encode.iter()
                } else {
                    matrix.decode.iter()
                }
            })
    }
}

fn matrix_optional_matches(row_value: Option<&str>, requested: Option<&str>) -> bool {
    let Some(requested) = requested.filter(|value| !value.trim().is_empty()) else {
        return true;
    };
    row_value
        .map(|value| normalized_profile(value) == normalized_profile(requested))
        .unwrap_or(true)
}

fn matrix_optional_u8_matches(row_value: Option<u8>, requested: Option<u8>) -> bool {
    requested
        .map(|requested| row_value.map(|value| value == requested).unwrap_or(true))
        .unwrap_or(true)
}

fn matrix_pixel_format_matches(row_values: &[String], requested: Option<&str>) -> bool {
    let Some(requested) = requested.filter(|value| !value.trim().is_empty()) else {
        return true;
    };
    row_values.is_empty()
        || row_values
            .iter()
            .any(|value| value.eq_ignore_ascii_case(requested))
}

fn normalized_profile(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_lowercase()
}

pub fn host_hardware_fingerprint(inventory: &HostHardwareInventory) -> String {
    let payload = json!({
        "schema_version": HARDWARE_READINESS_SCHEMA_VERSION,
        "os": inventory.os,
        "gpus": inventory.gpus,
        "ffmpeg": {
            "path": inventory.ffmpeg.path,
            "version": inventory.ffmpeg.version,
            "sha256": inventory.ffmpeg.sha256,
        }
    });
    let bytes = serde_json::to_vec(&payload).unwrap_or_default();
    let digest = Sha256::digest(bytes);
    format!("sha256:{digest:x}")
}

pub async fn upsert_hardware_readiness_record(
    pool: &AnyPool,
    record: &HardwareReadinessRecord,
) -> Result<()> {
    let capabilities_json =
        serde_json::to_string(&record.capabilities).context("serialize hardware capabilities")?;
    let inventory_json =
        serde_json::to_string(&record.inventory).context("serialize hardware inventory")?;
    let probe_report_json =
        serde_json::to_string(&record.probe_report).context("serialize hardware probe report")?;

    sqlx::query::<sqlx::Any>(
        "INSERT INTO playback_hardware_readiness (
            id,
            host_fingerprint,
            accelerator_id,
            api,
            os_family,
            os_version,
            arch,
            gpu_vendor,
            gpu_model,
            gpu_device_id,
            gpu_driver_version,
            ffmpeg_path,
            ffmpeg_version,
            ffmpeg_sha256,
            elixir_accel_schema_version,
            status,
            status_reason,
            user_message_code,
            capabilities_json,
            inventory_json,
            probe_report_json,
            raw_error_excerpt,
            stale,
            last_checked_at,
            created_at,
            updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26)
        ON CONFLICT(host_fingerprint, accelerator_id) DO UPDATE SET
            id = excluded.id,
            api = excluded.api,
            os_family = excluded.os_family,
            os_version = excluded.os_version,
            arch = excluded.arch,
            gpu_vendor = excluded.gpu_vendor,
            gpu_model = excluded.gpu_model,
            gpu_device_id = excluded.gpu_device_id,
            gpu_driver_version = excluded.gpu_driver_version,
            ffmpeg_path = excluded.ffmpeg_path,
            ffmpeg_version = excluded.ffmpeg_version,
            ffmpeg_sha256 = excluded.ffmpeg_sha256,
            elixir_accel_schema_version = excluded.elixir_accel_schema_version,
            status = excluded.status,
            status_reason = excluded.status_reason,
            user_message_code = excluded.user_message_code,
            capabilities_json = excluded.capabilities_json,
            inventory_json = excluded.inventory_json,
            probe_report_json = excluded.probe_report_json,
            raw_error_excerpt = excluded.raw_error_excerpt,
            stale = excluded.stale,
            last_checked_at = excluded.last_checked_at,
            updated_at = excluded.updated_at",
    )
    .bind(&record.id)
    .bind(&record.host_fingerprint)
    .bind(&record.accelerator_id)
    .bind(&record.api)
    .bind(&record.os_family)
    .bind(&record.os_version)
    .bind(&record.arch)
    .bind(&record.gpu_vendor)
    .bind(&record.gpu_model)
    .bind(&record.gpu_device_id)
    .bind(&record.gpu_driver_version)
    .bind(&record.ffmpeg_path)
    .bind(&record.ffmpeg_version)
    .bind(&record.ffmpeg_sha256)
    .bind(record.elixir_accel_schema_version as i64)
    .bind(record.status.as_str())
    .bind(&record.status_reason)
    .bind(&record.user_message_code)
    .bind(capabilities_json)
    .bind(inventory_json)
    .bind(probe_report_json)
    .bind(&record.raw_error_excerpt)
    .bind(if record.stale { 1_i64 } else { 0_i64 })
    .bind(record.last_checked_at.to_rfc3339())
    .bind(record.created_at.to_rfc3339())
    .bind(record.updated_at.to_rfc3339())
    .execute(pool)
    .await
    .context("upsert playback hardware readiness")?;
    Ok(())
}

pub async fn load_current_hardware_readiness_records(
    pool: &AnyPool,
    host_fingerprint: &str,
) -> Result<Vec<HardwareReadinessRecord>> {
    let rows = sqlx::query::<sqlx::Any>(
        "SELECT
            id,
            host_fingerprint,
            accelerator_id,
            api,
            os_family,
            os_version,
            arch,
            gpu_vendor,
            gpu_model,
            gpu_device_id,
            gpu_driver_version,
            ffmpeg_path,
            ffmpeg_version,
            ffmpeg_sha256,
            elixir_accel_schema_version,
            status,
            status_reason,
            user_message_code,
            capabilities_json,
            inventory_json,
            probe_report_json,
            raw_error_excerpt,
            stale,
            last_checked_at,
            created_at,
            updated_at
        FROM playback_hardware_readiness
        WHERE host_fingerprint = $1 AND stale = 0
        ORDER BY api, accelerator_id",
    )
    .bind(host_fingerprint)
    .fetch_all(pool)
    .await
    .context("load playback hardware readiness")?;

    rows.into_iter()
        .map(readiness_record_from_row)
        .collect::<Result<Vec<_>>>()
}

pub async fn mark_hardware_readiness_stale_except(
    pool: &AnyPool,
    active_host_fingerprint: &str,
) -> Result<u64> {
    let result = sqlx::query::<sqlx::Any>(
        "UPDATE playback_hardware_readiness
         SET stale = 1, updated_at = $1
         WHERE host_fingerprint <> $2 AND stale = 0",
    )
    .bind(Utc::now().to_rfc3339())
    .bind(active_host_fingerprint)
    .execute(pool)
    .await
    .context("mark stale playback hardware readiness")?;
    Ok(result.rows_affected())
}

pub async fn mark_all_hardware_readiness_stale(pool: &AnyPool) -> Result<u64> {
    let result = sqlx::query::<sqlx::Any>(
        "UPDATE playback_hardware_readiness
         SET stale = 1, updated_at = $1
         WHERE stale = 0",
    )
    .bind(Utc::now().to_rfc3339())
    .execute(pool)
    .await
    .context("mark all playback hardware readiness stale")?;
    Ok(result.rows_affected())
}

pub async fn append_hardware_readiness_event(
    pool: &AnyPool,
    readiness_id: Option<&str>,
    event_type: &str,
    status: HardwareReadinessStatus,
    message_code: &str,
    details: &Value,
) -> Result<String> {
    let id = Uuid::new_v4().to_string();
    sqlx::query::<sqlx::Any>(
        "INSERT INTO playback_hardware_readiness_events (
            id,
            readiness_id,
            event_type,
            status,
            message_code,
            details_json,
            created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(&id)
    .bind(readiness_id)
    .bind(event_type)
    .bind(status.as_str())
    .bind(message_code)
    .bind(serde_json::to_string(details).context("serialize hardware readiness event")?)
    .bind(Utc::now().to_rfc3339())
    .execute(pool)
    .await
    .context("append playback hardware readiness event")?;
    Ok(id)
}

fn readiness_record_from_row(row: sqlx::any::AnyRow) -> Result<HardwareReadinessRecord> {
    let status: String = row.try_get("status")?;
    let capabilities_json: String = row.try_get("capabilities_json")?;
    let inventory_json: String = row.try_get("inventory_json")?;
    let probe_report_json: String = row.try_get("probe_report_json")?;
    let stale_value: i64 = row.try_get("stale")?;
    let schema_version: i64 = row.try_get("elixir_accel_schema_version")?;
    Ok(HardwareReadinessRecord {
        id: row.try_get("id")?,
        host_fingerprint: row.try_get("host_fingerprint")?,
        accelerator_id: row.try_get("accelerator_id")?,
        api: row.try_get("api")?,
        os_family: row.try_get("os_family")?,
        os_version: row.try_get("os_version")?,
        arch: row.try_get("arch")?,
        gpu_vendor: row.try_get("gpu_vendor")?,
        gpu_model: row.try_get("gpu_model")?,
        gpu_device_id: row.try_get("gpu_device_id")?,
        gpu_driver_version: row.try_get("gpu_driver_version")?,
        ffmpeg_path: row.try_get("ffmpeg_path")?,
        ffmpeg_version: row.try_get("ffmpeg_version")?,
        ffmpeg_sha256: row.try_get("ffmpeg_sha256")?,
        elixir_accel_schema_version: schema_version.max(0) as u32,
        status: HardwareReadinessStatus::parse(&status),
        status_reason: row.try_get("status_reason")?,
        user_message_code: row.try_get("user_message_code")?,
        capabilities: serde_json::from_str(&capabilities_json)
            .context("parse hardware capabilities json")?,
        inventory: serde_json::from_str(&inventory_json)
            .context("parse hardware inventory json")?,
        probe_report: serde_json::from_str(&probe_report_json)
            .context("parse hardware probe report json")?,
        raw_error_excerpt: row.try_get("raw_error_excerpt")?,
        stale: stale_value != 0,
        last_checked_at: parse_db_datetime(row.try_get("last_checked_at")?)?,
        created_at: parse_db_datetime(row.try_get("created_at")?)?,
        updated_at: parse_db_datetime(row.try_get("updated_at")?)?,
    })
}

fn parse_db_datetime(raw: String) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(&raw)
        .with_context(|| format!("parse datetime {raw:?}"))?
        .with_timezone(&Utc))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HardwareDetectionConfig {
    pub preference: HardwarePreference,
}

impl Default for HardwareDetectionConfig {
    fn default() -> Self {
        Self {
            preference: HardwarePreference::Auto,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardwareProviderCandidate {
    pub id: String,
    pub api: HardwareApi,
    pub applicable: bool,
    pub reason: Option<String>,
    pub gpu_index: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardwareReadinessSnapshot {
    pub host_fingerprint: String,
    pub inventory: HostHardwareInventory,
    pub candidates: Vec<HardwareProviderCandidate>,
    pub records: Vec<HardwareReadinessRecord>,
    pub capabilities: HardwareCapabilities,
}

pub async fn detect_hardware_capabilities(
    config: &HardwareDetectionConfig,
) -> HardwareCapabilities {
    if config.preference == HardwarePreference::Off {
        return HardwareCapabilities::software_only();
    }
    detect_hardware_readiness(config).await.capabilities
}

pub async fn detect_hardware_readiness(
    config: &HardwareDetectionConfig,
) -> HardwareReadinessSnapshot {
    let inventory = collect_host_hardware_inventory().await;
    detect_hardware_readiness_for_inventory(config, inventory).await
}

pub async fn load_or_detect_hardware_capabilities(
    pool: &AnyPool,
    config: &HardwareDetectionConfig,
) -> Result<HardwareCapabilities> {
    if config.preference == HardwarePreference::Off {
        return Ok(HardwareCapabilities::software_only());
    }

    let inventory = collect_host_hardware_inventory().await;
    let host_fingerprint = host_hardware_fingerprint(&inventory);
    mark_hardware_readiness_stale_except(pool, &host_fingerprint).await?;
    let cached = load_current_hardware_readiness_records(pool, &host_fingerprint).await?;
    if !cached.is_empty() {
        for record in &cached {
            record_hardware_readiness_metrics(record);
        }
        return Ok(hardware_capabilities_from_readiness(&inventory, &cached));
    }

    let snapshot = detect_hardware_readiness_for_inventory(config, inventory).await;
    for record in &snapshot.records {
        upsert_hardware_readiness_record(pool, record).await?;
        record_hardware_readiness_metrics(record);
        append_hardware_readiness_event(
            pool,
            Some(&record.id),
            "probe_completed",
            record.status,
            &record.user_message_code,
            &json!({
                "accelerator_id": record.accelerator_id,
                "api": record.api,
                "status_reason": record.status_reason,
            }),
        )
        .await?;
    }
    Ok(snapshot.capabilities)
}

pub async fn detect_hardware_readiness_for_inventory(
    config: &HardwareDetectionConfig,
    inventory: HostHardwareInventory,
) -> HardwareReadinessSnapshot {
    if config.preference == HardwarePreference::Off {
        let host_fingerprint = host_hardware_fingerprint(&inventory);
        return HardwareReadinessSnapshot {
            host_fingerprint,
            inventory,
            candidates: Vec::new(),
            records: Vec::new(),
            capabilities: HardwareCapabilities::software_only(),
        };
    }

    let host_fingerprint = host_hardware_fingerprint(&inventory);
    let candidates = hardware_provider_candidates(&inventory, config.preference);
    let encoders: BTreeSet<String> = inventory.ffmpeg.encoders.iter().cloned().collect();
    let hwaccels: BTreeSet<String> = inventory.ffmpeg.hwaccels.iter().cloned().collect();
    let decoders: BTreeSet<String> = inventory.ffmpeg.decoders.iter().cloned().collect();
    let mut records = Vec::new();

    for candidate in candidates.iter().filter(|candidate| candidate.applicable) {
        let api = candidate.api;
        let probe = startup_probe_encoder(api, &encoders).await;
        let mut startup_probes = Vec::new();
        let mut detection_errors = Vec::new();
        let mut status = HardwareReadinessStatus::Available;
        let mut status_reason = "startup_probe_passed".to_string();
        let mut raw_error_excerpt = None;

        if let Some(probe) = probe {
            if !probe.ok {
                let detail = probe
                    .detail
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string());
                status = classify_hardware_probe_failure(api, &detail);
                status_reason = format!("startup_probe_failed:{}", status.as_str());
                raw_error_excerpt = Some(trim_error_excerpt(&detail));
                detection_errors.push(format!("{}_startup_probe_failed:{}", api.as_str(), detail));
            }
            startup_probes.push(probe);
        }

        let (capabilities, capability_probes) =
            capability_matrix_for_api(api, status, &hwaccels, &encoders, &decoders).await;
        let probe_report = HardwareProbeReport {
            api: api.as_str().to_string(),
            startup_probes,
            capability_probes,
            detection_errors,
        };
        let gpu = candidate
            .gpu_index
            .and_then(|index| inventory.gpus.get(index));
        records.push(HardwareReadinessRecord::new(
            host_fingerprint.clone(),
            candidate.id.clone(),
            api,
            inventory.clone(),
            gpu,
            status,
            status_reason,
            hardware_readiness_message_code(api, status),
            capabilities,
            probe_report,
            raw_error_excerpt,
        ));
    }

    let capabilities = hardware_capabilities_from_readiness(&inventory, &records);
    HardwareReadinessSnapshot {
        host_fingerprint,
        inventory,
        candidates,
        records,
        capabilities,
    }
}

pub async fn collect_host_hardware_inventory() -> HostHardwareInventory {
    let ffmpeg_version = run_ffmpeg_text(&["-version"], FFMPEG_CAPABILITY_TIMEOUT)
        .await
        .ok()
        .and_then(|output| output.lines().next().map(str::to_string));
    let hwaccels =
        match run_ffmpeg_text(&["-hide_banner", "-hwaccels"], FFMPEG_CAPABILITY_TIMEOUT).await {
            Ok(output) => parse_hwaccels(&output).into_iter().collect(),
            Err(_) => Vec::new(),
        };
    let encoders =
        match run_ffmpeg_text(&["-hide_banner", "-encoders"], FFMPEG_CAPABILITY_TIMEOUT).await {
            Ok(output) => parse_ffmpeg_components(&output).into_iter().collect(),
            Err(_) => Vec::new(),
        };
    let decoders =
        match run_ffmpeg_text(&["-hide_banner", "-decoders"], FFMPEG_CAPABILITY_TIMEOUT).await {
            Ok(output) => parse_ffmpeg_components(&output).into_iter().collect(),
            Err(_) => Vec::new(),
        };
    let path = resolve_command_path("ffmpeg").await;
    let sha256 = path
        .as_deref()
        .and_then(|path| std::fs::read(path).ok())
        .map(|bytes| format!("sha256:{:x}", Sha256::digest(bytes)));

    HostHardwareInventory {
        os: HostOsInventory {
            family: std::env::consts::OS.to_string(),
            version: None,
            arch: std::env::consts::ARCH.to_string(),
        },
        gpus: collect_gpu_inventory().await,
        ffmpeg: FfmpegHardwareInventory {
            path,
            version: ffmpeg_version,
            sha256,
            hwaccels,
            encoders,
            decoders,
        },
    }
}

pub fn hardware_provider_candidates(
    inventory: &HostHardwareInventory,
    preference: HardwarePreference,
) -> Vec<HardwareProviderCandidate> {
    let mut candidates = Vec::new();
    push_provider_candidate(
        &mut candidates,
        inventory,
        preference,
        "macos_videotoolbox",
        HardwareApi::VideoToolbox,
        |inventory| inventory.os.family == "macos",
        |inventory| {
            inventory
                .ffmpeg
                .hwaccels
                .iter()
                .any(|value| value == "videotoolbox")
                || inventory
                    .ffmpeg
                    .encoders
                    .iter()
                    .any(|value| value == "h264_videotoolbox")
        },
        None,
    );
    push_provider_candidate(
        &mut candidates,
        inventory,
        preference,
        "windows_nvidia_nvenc",
        HardwareApi::Nvenc,
        |inventory| inventory.os.family == "windows",
        |inventory| {
            inventory
                .ffmpeg
                .encoders
                .iter()
                .any(|value| value.ends_with("_nvenc"))
        },
        find_gpu_by_vendor(inventory, "nvidia"),
    );
    push_provider_candidate(
        &mut candidates,
        inventory,
        preference,
        "linux_nvidia_nvenc",
        HardwareApi::Nvenc,
        |inventory| inventory.os.family == "linux",
        |inventory| {
            inventory
                .ffmpeg
                .encoders
                .iter()
                .any(|value| value.ends_with("_nvenc"))
        },
        find_gpu_by_vendor(inventory, "nvidia"),
    );
    push_provider_candidate(
        &mut candidates,
        inventory,
        preference,
        "windows_amd_amf",
        HardwareApi::Amf,
        |inventory| inventory.os.family == "windows",
        |inventory| {
            inventory
                .ffmpeg
                .encoders
                .iter()
                .any(|value| value.ends_with("_amf"))
        },
        find_gpu_by_vendor(inventory, "amd"),
    );
    push_provider_candidate(
        &mut candidates,
        inventory,
        preference,
        "windows_intel_qsv",
        HardwareApi::Qsv,
        |inventory| inventory.os.family == "windows",
        |inventory| {
            inventory.ffmpeg.hwaccels.iter().any(|value| value == "qsv")
                || inventory
                    .ffmpeg
                    .encoders
                    .iter()
                    .any(|value| value.ends_with("_qsv"))
        },
        find_gpu_by_vendor(inventory, "intel"),
    );
    push_provider_candidate(
        &mut candidates,
        inventory,
        preference,
        "linux_intel_vaapi_qsv",
        HardwareApi::Vaapi,
        |inventory| inventory.os.family == "linux",
        |inventory| {
            inventory
                .ffmpeg
                .hwaccels
                .iter()
                .any(|value| value == "vaapi")
        },
        find_gpu_by_vendor(inventory, "intel"),
    );
    push_provider_candidate(
        &mut candidates,
        inventory,
        preference,
        "linux_amd_vaapi",
        HardwareApi::Vaapi,
        |inventory| inventory.os.family == "linux",
        |inventory| {
            inventory
                .ffmpeg
                .hwaccels
                .iter()
                .any(|value| value == "vaapi")
        },
        find_gpu_by_vendor(inventory, "amd"),
    );
    candidates
}

fn push_provider_candidate(
    candidates: &mut Vec<HardwareProviderCandidate>,
    inventory: &HostHardwareInventory,
    preference: HardwarePreference,
    id: &str,
    api: HardwareApi,
    os_applies: fn(&HostHardwareInventory) -> bool,
    ffmpeg_applies: fn(&HostHardwareInventory) -> bool,
    gpu_index: Option<usize>,
) {
    if !preference_allows_api(preference, api) {
        return;
    }
    let (applicable, reason) = if !os_applies(inventory) {
        (false, Some("os_not_applicable".to_string()))
    } else if api != HardwareApi::VideoToolbox && gpu_index.is_none() {
        (false, Some("gpu_vendor_not_present".to_string()))
    } else if !ffmpeg_applies(inventory) {
        (false, Some("ffmpeg_missing_support".to_string()))
    } else {
        (true, None)
    };
    candidates.push(HardwareProviderCandidate {
        id: id.to_string(),
        api,
        applicable,
        reason,
        gpu_index,
    });
}

fn preference_allows_api(preference: HardwarePreference, api: HardwareApi) -> bool {
    match preference {
        HardwarePreference::Auto => true,
        HardwarePreference::Api(selected) => selected == api,
        HardwarePreference::Off => false,
    }
}

fn find_gpu_by_vendor(inventory: &HostHardwareInventory, vendor: &str) -> Option<usize> {
    inventory.gpus.iter().position(|gpu| {
        gpu.vendor
            .as_deref()
            .is_some_and(|value| value.eq_ignore_ascii_case(vendor))
    })
}

pub fn classify_hardware_probe_failure(_api: HardwareApi, detail: &str) -> HardwareReadinessStatus {
    let lowered = detail.to_ascii_lowercase();
    if lowered.contains("cannot load cumemallocasync") {
        return HardwareReadinessStatus::DriverRuntimeIncompatible;
    }
    if lowered.contains("timed out") {
        return HardwareReadinessStatus::ProbeTimeout;
    }
    if lowered.contains("permission denied") || lowered.contains("operation not permitted") {
        return HardwareReadinessStatus::PermissionDenied;
    }
    if lowered.contains("no nvenc capable devices")
        || lowered.contains("no capable devices found")
        || lowered.contains("unsupported device")
    {
        return HardwareReadinessStatus::UnsupportedGpu;
    }
    if lowered.contains("session") && lowered.contains("limit") {
        return HardwareReadinessStatus::DeviceBusyOrSessionLimit;
    }
    HardwareReadinessStatus::ProbeFailedUnknown
}

fn trim_error_excerpt(detail: &str) -> String {
    const MAX_ERROR_EXCERPT: usize = 800;
    let collapsed = detail
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_string();
    if collapsed.len() > MAX_ERROR_EXCERPT {
        collapsed[..MAX_ERROR_EXCERPT].to_string()
    } else {
        collapsed
    }
}

fn record_hardware_readiness_metrics(record: &HardwareReadinessRecord) {
    let api = metric_label_value(Some(&record.api), "unknown");
    let os_family = metric_label_value(Some(&record.os_family), "unknown");
    let gpu_vendor = metric_label_value(record.gpu_vendor.as_deref(), "none");

    for status in HardwareReadinessStatus::ALL {
        metrics::PLAYBACK_HARDWARE_READINESS_STATUS
            .with_label_values(&[&api, status.as_str(), &os_family, &gpu_vendor])
            .set(i64::from(status == record.status));
    }
}

fn record_capability_probe_metrics(api: HardwareApi, probe: &HardwareCapabilityProbeResult) {
    let operation = hardware_probe_operation_label(probe);
    record_probe_metrics(
        api,
        &operation,
        probe.status,
        Duration::from_millis(probe.duration_ms),
        probe.ok,
    );
}

fn record_probe_metrics(
    api: HardwareApi,
    operation: &str,
    status: HardwareReadinessStatus,
    duration: Duration,
    ok: bool,
) {
    metrics::PLAYBACK_HARDWARE_PROBE_DURATION
        .with_label_values(&[api.as_str(), operation, status.as_str()])
        .observe(duration.as_secs_f64());
    if !ok {
        metrics::PLAYBACK_HARDWARE_PROBE_FAILURES
            .with_label_values(&[api.as_str(), operation, status.as_str()])
            .inc();
    }
}

fn hardware_probe_operation_label(probe: &HardwareCapabilityProbeResult) -> String {
    let direction = match probe.spec.direction {
        HardwareCapabilityProbeDirection::Encode => "encode",
        HardwareCapabilityProbeDirection::Decode => "decode",
    };
    let bit_depth = probe.spec.bit_depth.unwrap_or(0);
    format!("{direction}:{}:{bit_depth}bit", probe.spec.codec)
}

fn metric_label_value(value: Option<&str>, fallback: &str) -> String {
    let normalized = value
        .unwrap_or(fallback)
        .trim()
        .to_ascii_lowercase()
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect::<String>();
    let normalized = normalized.trim_matches('_');
    if normalized.is_empty() {
        fallback.to_string()
    } else {
        normalized.to_string()
    }
}

async fn capability_matrix_for_api(
    api: HardwareApi,
    status: HardwareReadinessStatus,
    hwaccels: &BTreeSet<String>,
    encoders: &BTreeSet<String>,
    decoders: &BTreeSet<String>,
) -> (HardwareCapabilityMatrix, Vec<HardwareCapabilityProbeResult>) {
    let mut matrix = HardwareCapabilityMatrix::empty(api, status);
    let mut capability_probes = Vec::new();
    let encode_specs = hardware_encode_probe_specs(api, encoders);
    for spec in encode_specs {
        let probe = if status == HardwareReadinessStatus::Available {
            run_encode_capability_probe(api, &spec).await
        } else {
            HardwareCapabilityProbeResult {
                spec: spec.clone(),
                ok: false,
                status,
                duration_ms: 0,
                detail: Some(status.as_str().to_string()),
            }
        };
        record_capability_probe_metrics(api, &probe);
        matrix.encode.push(HardwareCodecMatrixEntry {
            codec: spec.codec.clone(),
            profile: spec.profile.clone(),
            bit_depth: spec.bit_depth,
            pixel_formats: spec.pixel_format.iter().cloned().collect::<Vec<String>>(),
            ffmpeg_encoder: Some(spec.ffmpeg_component.clone()),
            ffmpeg_decoder: None,
            status: if probe.ok {
                MATRIX_STATUS_SUPPORTED.to_string()
            } else {
                probe.status.as_str().to_string()
            },
        });
        capability_probes.push(probe);
    }
    let decode_specs = hardware_decode_probe_specs(api, hwaccels, decoders);
    let decode_tempdir = tempfile::tempdir().ok();
    for spec in decode_specs {
        let probe = if status == HardwareReadinessStatus::Available {
            if let Some(tempdir) = decode_tempdir.as_ref() {
                run_decode_capability_probe(api, &spec, tempdir.path()).await
            } else {
                HardwareCapabilityProbeResult {
                    spec: spec.clone(),
                    ok: false,
                    status: HardwareReadinessStatus::ProbeFailedUnknown,
                    duration_ms: 0,
                    detail: Some("decode_probe_tempdir_unavailable".to_string()),
                }
            }
        } else {
            HardwareCapabilityProbeResult {
                spec: spec.clone(),
                ok: false,
                status,
                duration_ms: 0,
                detail: Some(status.as_str().to_string()),
            }
        };
        record_capability_probe_metrics(api, &probe);
        matrix.decode.push(HardwareCodecMatrixEntry {
            codec: spec.codec.clone(),
            profile: spec.profile.clone(),
            bit_depth: spec.bit_depth,
            pixel_formats: spec.pixel_format.iter().cloned().collect::<Vec<String>>(),
            ffmpeg_encoder: None,
            ffmpeg_decoder: Some(spec.ffmpeg_component.clone()),
            status: if probe.ok {
                MATRIX_STATUS_SUPPORTED.to_string()
            } else {
                probe.status.as_str().to_string()
            },
        });
        capability_probes.push(probe);
    }
    (matrix, capability_probes)
}

fn hardware_encode_probe_specs(
    api: HardwareApi,
    encoders: &BTreeSet<String>,
) -> Vec<HardwareCapabilityProbeSpec> {
    let suffix = match api {
        HardwareApi::VideoToolbox => "_videotoolbox",
        HardwareApi::Vaapi => "_vaapi",
        HardwareApi::Qsv => "_qsv",
        HardwareApi::Nvenc => "_nvenc",
        HardwareApi::Amf => "_amf",
    };
    [
        ("h264", "high", 8_u8, "yuv420p"),
        ("hevc", "main", 8_u8, "yuv420p"),
        ("hevc", "main10", 10_u8, "p010le"),
        ("av1", "main", 8_u8, "yuv420p"),
        ("av1", "main10", 10_u8, "p010le"),
    ]
    .into_iter()
    .filter_map(|(codec, profile, bit_depth, pixel_format)| {
        let encoder = match (api, codec) {
            (HardwareApi::VideoToolbox, "av1") => return None,
            (HardwareApi::VideoToolbox, _) => format!("{codec}_videotoolbox"),
            _ => format!("{codec}{suffix}"),
        };
        encoders
            .contains(&encoder)
            .then(|| HardwareCapabilityProbeSpec {
                direction: HardwareCapabilityProbeDirection::Encode,
                codec: codec.to_string(),
                profile: Some(profile.to_string()),
                bit_depth: Some(bit_depth),
                pixel_format: Some(pixel_format.to_string()),
                ffmpeg_component: encoder,
            })
    })
    .collect()
}

fn hardware_decode_probe_specs(
    api: HardwareApi,
    hwaccels: &BTreeSet<String>,
    decoders: &BTreeSet<String>,
) -> Vec<HardwareCapabilityProbeSpec> {
    let decoder_components = match api {
        HardwareApi::VideoToolbox if hwaccels.contains("videotoolbox") => {
            vec![("h264", "videotoolbox"), ("hevc", "videotoolbox")]
        }
        HardwareApi::Vaapi if hwaccels.contains("vaapi") => vec![
            ("h264", "vaapi"),
            ("hevc", "vaapi"),
            ("mpeg2video", "vaapi"),
            ("vp9", "vaapi"),
            ("av1", "vaapi"),
        ],
        HardwareApi::Qsv => [
            ("h264", "h264_qsv"),
            ("hevc", "hevc_qsv"),
            ("mpeg2video", "mpeg2_qsv"),
            ("vp9", "vp9_qsv"),
            ("av1", "av1_qsv"),
        ]
        .into_iter()
        .filter(|(_, decoder)| decoders.contains(*decoder))
        .collect(),
        HardwareApi::Nvenc if hwaccels.contains("cuda") || hwaccels.contains("nvdec") => {
            vec![("h264", "cuda"), ("hevc", "cuda"), ("av1", "cuda")]
        }
        HardwareApi::Amf if hwaccels.contains("d3d11va") => {
            vec![("h264", "d3d11va"), ("hevc", "d3d11va"), ("av1", "d3d11va")]
        }
        HardwareApi::Amf if hwaccels.contains("dxva2") => {
            vec![("h264", "dxva2"), ("hevc", "dxva2")]
        }
        _ => Vec::new(),
    };

    decoder_components
        .into_iter()
        .flat_map(|(codec, decoder)| {
            decode_profiles_for_codec(codec).map(move |profile| (profile, decoder))
        })
        .map(|(profile, decoder)| HardwareCapabilityProbeSpec {
            direction: HardwareCapabilityProbeDirection::Decode,
            codec: profile.codec.to_string(),
            profile: Some(profile.profile.to_string()),
            bit_depth: Some(profile.bit_depth),
            pixel_format: Some(profile.pixel_format.to_string()),
            ffmpeg_component: decoder.to_string(),
        })
        .collect()
}

#[derive(Debug, Clone, Copy)]
struct CodecProbeProfile {
    codec: &'static str,
    profile: &'static str,
    bit_depth: u8,
    pixel_format: &'static str,
}

fn decode_profiles_for_codec(codec: &str) -> impl Iterator<Item = CodecProbeProfile> {
    let profiles: Vec<CodecProbeProfile> = match codec {
        "h264" => vec![CodecProbeProfile {
            codec: "h264",
            profile: "high",
            bit_depth: 8,
            pixel_format: "yuv420p",
        }],
        "hevc" => vec![
            CodecProbeProfile {
                codec: "hevc",
                profile: "main",
                bit_depth: 8,
                pixel_format: "yuv420p",
            },
            CodecProbeProfile {
                codec: "hevc",
                profile: "main10",
                bit_depth: 10,
                pixel_format: "yuv420p10le",
            },
        ],
        "av1" => vec![
            CodecProbeProfile {
                codec: "av1",
                profile: "main",
                bit_depth: 8,
                pixel_format: "yuv420p",
            },
            CodecProbeProfile {
                codec: "av1",
                profile: "main10",
                bit_depth: 10,
                pixel_format: "yuv420p10le",
            },
        ],
        "vp9" => vec![CodecProbeProfile {
            codec: "vp9",
            profile: "profile0",
            bit_depth: 8,
            pixel_format: "yuv420p",
        }],
        "mpeg2video" => vec![CodecProbeProfile {
            codec: "mpeg2video",
            profile: "main",
            bit_depth: 8,
            pixel_format: "yuv420p",
        }],
        _ => Vec::new(),
    };
    profiles.into_iter()
}

async fn run_encode_capability_probe(
    api: HardwareApi,
    spec: &HardwareCapabilityProbeSpec,
) -> HardwareCapabilityProbeResult {
    let mut args = vec![
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "testsrc2=size=1280x720:rate=30:duration=0.2",
        "-frames:v",
        "1",
        "-pix_fmt",
        spec.pixel_format.as_deref().unwrap_or("yuv420p"),
        "-c:v",
        spec.ffmpeg_component.as_str(),
    ];
    if api == HardwareApi::VideoToolbox {
        args.extend(["-allow_sw", "0"]);
    }
    args.extend(["-f", "null", "-"]);

    let started = Instant::now();
    let result = run_ffmpeg_text(&args, STARTUP_PROBE_TIMEOUT).await;
    let duration_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
    match result {
        Ok(_) => HardwareCapabilityProbeResult {
            spec: spec.clone(),
            ok: true,
            status: HardwareReadinessStatus::Available,
            duration_ms,
            detail: None,
        },
        Err(err) => {
            let status = classify_hardware_probe_failure(api, &err);
            HardwareCapabilityProbeResult {
                spec: spec.clone(),
                ok: false,
                status,
                duration_ms,
                detail: Some(trim_error_excerpt(&err)),
            }
        }
    }
}

async fn run_decode_capability_probe(
    api: HardwareApi,
    spec: &HardwareCapabilityProbeSpec,
    temp_root: &std::path::Path,
) -> HardwareCapabilityProbeResult {
    let started = Instant::now();
    let fixture_path = temp_root.join(format!(
        "decode-{}-{}-{}.mkv",
        spec.codec,
        spec.bit_depth.unwrap_or(8),
        Uuid::new_v4()
    ));
    let fixture_path_string = fixture_path.to_string_lossy().to_string();
    let generate_args = software_decode_fixture_args(spec, &fixture_path_string);
    let generation = run_ffmpeg_text_owned(&generate_args, STARTUP_PROBE_TIMEOUT).await;
    if let Err(err) = generation {
        return HardwareCapabilityProbeResult {
            spec: spec.clone(),
            ok: false,
            status: HardwareReadinessStatus::ProbeFailedUnknown,
            duration_ms: started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
            detail: Some(format!(
                "decode_fixture_generation_failed:{}",
                trim_error_excerpt(&err)
            )),
        };
    }

    let decode_args = hardware_decode_probe_args(api, spec, &fixture_path_string);
    let result = run_ffmpeg_text_owned(&decode_args, STARTUP_PROBE_TIMEOUT).await;
    let _ = std::fs::remove_file(&fixture_path);
    let duration_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
    match result {
        Ok(_) => HardwareCapabilityProbeResult {
            spec: spec.clone(),
            ok: true,
            status: HardwareReadinessStatus::Available,
            duration_ms,
            detail: None,
        },
        Err(err) => {
            let status = classify_hardware_probe_failure(api, &err);
            HardwareCapabilityProbeResult {
                spec: spec.clone(),
                ok: false,
                status,
                duration_ms,
                detail: Some(trim_error_excerpt(&err)),
            }
        }
    }
}

fn software_decode_fixture_args(spec: &HardwareCapabilityProbeSpec, output: &str) -> Vec<String> {
    let mut args = vec![
        "-hide_banner".to_string(),
        "-loglevel".to_string(),
        "error".to_string(),
        "-y".to_string(),
        "-f".to_string(),
        "lavfi".to_string(),
        "-i".to_string(),
        "testsrc2=size=320x180:rate=30:duration=0.2".to_string(),
        "-frames:v".to_string(),
        "1".to_string(),
        "-pix_fmt".to_string(),
        spec.pixel_format
            .clone()
            .unwrap_or_else(|| "yuv420p".to_string()),
    ];
    match spec.codec.as_str() {
        "h264" => args.extend([
            "-c:v".to_string(),
            "libx264".to_string(),
            "-preset".to_string(),
            "ultrafast".to_string(),
            "-tune".to_string(),
            "zerolatency".to_string(),
        ]),
        "hevc" => args.extend([
            "-c:v".to_string(),
            "libx265".to_string(),
            "-preset".to_string(),
            "ultrafast".to_string(),
            "-x265-params".to_string(),
            "log-level=error".to_string(),
        ]),
        "av1" => args.extend([
            "-c:v".to_string(),
            "libaom-av1".to_string(),
            "-cpu-used".to_string(),
            "8".to_string(),
            "-row-mt".to_string(),
            "1".to_string(),
        ]),
        "vp9" => args.extend([
            "-c:v".to_string(),
            "libvpx-vp9".to_string(),
            "-deadline".to_string(),
            "realtime".to_string(),
            "-cpu-used".to_string(),
            "8".to_string(),
        ]),
        "mpeg2video" => args.extend(["-c:v".to_string(), "mpeg2video".to_string()]),
        _ => args.extend(["-c:v".to_string(), spec.codec.clone()]),
    }
    args.extend(["-an".to_string(), output.to_string()]);
    args
}

fn hardware_decode_probe_args(
    api: HardwareApi,
    spec: &HardwareCapabilityProbeSpec,
    input: &str,
) -> Vec<String> {
    let mut args = vec!["-hide_banner", "-loglevel", "error"];
    match api {
        HardwareApi::Qsv if spec.ffmpeg_component.ends_with("_qsv") => {
            args.extend(["-c:v", spec.ffmpeg_component.as_str()]);
        }
        _ => {
            args.extend(["-hwaccel", spec.ffmpeg_component.as_str()]);
        }
    }
    args.extend(["-i", input, "-an", "-frames:v", "1", "-f", "null", "-"]);
    args.into_iter().map(str::to_string).collect()
}

pub fn hardware_capabilities_from_readiness(
    inventory: &HostHardwareInventory,
    records: &[HardwareReadinessRecord],
) -> HardwareCapabilities {
    let mut capabilities = HardwareCapabilities {
        platform: current_platform(),
        ffmpeg_version: inventory.ffmpeg.version.clone(),
        available_apis: Vec::new(),
        capability_matrices: records
            .iter()
            .map(|record| record.capabilities.clone())
            .collect(),
        supported_decode_codecs: Vec::new(),
        supported_encode_codecs: Vec::new(),
        max_sessions: None,
        hdr_tone_mapping: false,
        subtitle_burn_in_limitations: vec!["subtitle_burn_in_requires_software_filter".to_string()],
        startup_probes: Vec::new(),
        detection_errors: Vec::new(),
    };
    for record in records {
        capabilities
            .startup_probes
            .extend(record.probe_report.startup_probes.clone());
        capabilities
            .detection_errors
            .extend(record.probe_report.detection_errors.clone());
        if record.status != HardwareReadinessStatus::Available {
            continue;
        }
        capabilities.available_apis.push(record.api.clone());
        for entry in &record.capabilities.encode {
            if entry.status == "supported" {
                if let Some(ffmpeg_name) = entry.ffmpeg_encoder.as_ref() {
                    capabilities
                        .supported_encode_codecs
                        .push(HardwareCodecSupport {
                            api: record.api.clone(),
                            codec: entry.codec.clone(),
                            ffmpeg_name: ffmpeg_name.clone(),
                        });
                }
            }
        }
        for entry in &record.capabilities.decode {
            if entry.status == "supported" {
                if let Some(ffmpeg_name) = entry.ffmpeg_decoder.as_ref() {
                    capabilities
                        .supported_decode_codecs
                        .push(HardwareCodecSupport {
                            api: record.api.clone(),
                            codec: entry.codec.clone(),
                            ffmpeg_name: ffmpeg_name.clone(),
                        });
                }
            }
        }
    }
    capabilities.available_apis.sort();
    capabilities.available_apis.dedup();
    capabilities
}

async fn resolve_command_path(command: &str) -> Option<String> {
    let result = if cfg!(windows) {
        run_command_text("where", &[command], Duration::from_secs(2)).await
    } else {
        run_command_text(
            "sh",
            &["-lc", &format!("command -v {}", shell_quote(command))],
            Duration::from_secs(2),
        )
        .await
    };
    result
        .ok()
        .and_then(|output| output.lines().next().map(str::trim).map(str::to_string))
        .filter(|value| !value.is_empty())
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

async fn collect_gpu_inventory() -> Vec<HostGpuInventory> {
    let mut gpus = Vec::new();
    if let Ok(output) = run_command_text(
        "nvidia-smi",
        &[
            "--query-gpu=name,driver_version,pci.device_id",
            "--format=csv,noheader",
        ],
        Duration::from_secs(3),
    )
    .await
    {
        for line in output
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
        {
            let parts = line.split(',').map(str::trim).collect::<Vec<_>>();
            gpus.push(HostGpuInventory {
                vendor: Some("nvidia".to_string()),
                model: parts.first().map(|value| (*value).to_string()),
                driver_version: parts.get(1).map(|value| (*value).to_string()),
                device_id: parts.get(2).map(|value| (*value).to_string()),
                raw: json!({"nvidia_smi": line}),
            });
        }
    }

    if gpus.is_empty() && cfg!(windows) {
        collect_windows_gpu_inventory(&mut gpus).await;
    } else if gpus.is_empty() && cfg!(target_os = "linux") {
        collect_linux_gpu_inventory(&mut gpus).await;
    } else if gpus.is_empty() && cfg!(target_os = "macos") {
        collect_macos_gpu_inventory(&mut gpus).await;
    }
    gpus
}

async fn collect_windows_gpu_inventory(gpus: &mut Vec<HostGpuInventory>) {
    let script = "Get-CimInstance Win32_VideoController | Select-Object Name,PNPDeviceID,DriverVersion | ConvertTo-Json -Compress";
    if let Ok(output) = run_command_text(
        "powershell",
        &["-NoProfile", "-Command", script],
        Duration::from_secs(4),
    )
    .await
    {
        if let Ok(value) = serde_json::from_str::<Value>(&output) {
            let entries = match value {
                Value::Array(values) => values,
                other => vec![other],
            };
            for entry in entries {
                let name = entry
                    .get("Name")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                gpus.push(HostGpuInventory {
                    vendor: infer_gpu_vendor(name.as_deref()),
                    model: name,
                    device_id: entry
                        .get("PNPDeviceID")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    driver_version: entry
                        .get("DriverVersion")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    raw: entry,
                });
            }
        }
    }
}

async fn collect_linux_gpu_inventory(gpus: &mut Vec<HostGpuInventory>) {
    if let Ok(output) = run_command_text("lspci", &["-nn"], Duration::from_secs(3)).await {
        for line in output.lines().filter(|line| {
            line.contains("VGA compatible controller")
                || line.contains("3D controller")
                || line.contains("Display controller")
        }) {
            gpus.push(HostGpuInventory {
                vendor: infer_gpu_vendor(Some(line)),
                model: Some(line.to_string()),
                device_id: extract_bracketed_device_id(line),
                driver_version: None,
                raw: json!({"lspci": line}),
            });
        }
    }
}

async fn collect_macos_gpu_inventory(gpus: &mut Vec<HostGpuInventory>) {
    if let Ok(output) = run_command_text(
        "system_profiler",
        &["SPDisplaysDataType", "-json"],
        Duration::from_secs(5),
    )
    .await
    {
        if let Ok(value) = serde_json::from_str::<Value>(&output) {
            if let Some(displays) = value.get("SPDisplaysDataType").and_then(Value::as_array) {
                for display in displays {
                    let model = display
                        .get("sppci_model")
                        .and_then(Value::as_str)
                        .map(str::to_string);
                    gpus.push(HostGpuInventory {
                        vendor: infer_gpu_vendor(model.as_deref()).or_else(|| {
                            if model
                                .as_deref()
                                .is_some_and(|value| value.to_ascii_lowercase().contains("apple"))
                            {
                                Some("apple".to_string())
                            } else {
                                None
                            }
                        }),
                        model,
                        device_id: display
                            .get("spdisplays_device-id")
                            .and_then(Value::as_str)
                            .map(str::to_string),
                        driver_version: None,
                        raw: display.clone(),
                    });
                }
            }
        }
    }
}

fn infer_gpu_vendor(raw: Option<&str>) -> Option<String> {
    let lowered = raw?.to_ascii_lowercase();
    if lowered.contains("nvidia") || lowered.contains("geforce") || lowered.contains("quadro") {
        Some("nvidia".to_string())
    } else if lowered.contains("amd")
        || lowered.contains("advanced micro devices")
        || lowered.contains("radeon")
    {
        Some("amd".to_string())
    } else if lowered.contains("intel") {
        Some("intel".to_string())
    } else if lowered.contains("apple") {
        Some("apple".to_string())
    } else {
        None
    }
}

fn extract_bracketed_device_id(line: &str) -> Option<String> {
    line.rsplit('[')
        .next()
        .and_then(|tail| tail.split(']').next())
        .filter(|value| value.contains(':'))
        .map(str::to_string)
}

async fn run_command_text(program: &str, args: &[&str], wait: Duration) -> Result<String, String> {
    let output = timeout(
        wait,
        Command::new(program)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output(),
    )
    .await
    .map_err(|_| format!("{program} timed out after {}s", wait.as_secs()))?
    .map_err(|err| format!("failed to spawn {program}: {err}"))?;

    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    } else {
        Err(format!(
            "{program} exited with {:?}: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
                .lines()
                .take(4)
                .collect::<Vec<_>>()
                .join(" | ")
        ))
    }
}

#[allow(dead_code)]
async fn detect_hardware_capabilities_v1(config: &HardwareDetectionConfig) -> HardwareCapabilities {
    if config.preference == HardwarePreference::Off {
        return HardwareCapabilities::software_only();
    }

    let platform = current_platform();
    let mut detection_errors = Vec::new();
    let ffmpeg_version = run_ffmpeg_text(&["-version"], FFMPEG_CAPABILITY_TIMEOUT)
        .await
        .ok()
        .and_then(|output| output.lines().next().map(str::to_string));
    let hwaccels =
        match run_ffmpeg_text(&["-hide_banner", "-hwaccels"], FFMPEG_CAPABILITY_TIMEOUT).await {
            Ok(output) => parse_hwaccels(&output),
            Err(err) => {
                detection_errors.push(format!("hwaccels_probe_failed:{err}"));
                BTreeSet::new()
            }
        };
    let encoders =
        match run_ffmpeg_text(&["-hide_banner", "-encoders"], FFMPEG_CAPABILITY_TIMEOUT).await {
            Ok(output) => parse_ffmpeg_components(&output),
            Err(err) => {
                detection_errors.push(format!("encoders_probe_failed:{err}"));
                BTreeSet::new()
            }
        };
    let decoders =
        match run_ffmpeg_text(&["-hide_banner", "-decoders"], FFMPEG_CAPABILITY_TIMEOUT).await {
            Ok(output) => parse_ffmpeg_components(&output),
            Err(err) => {
                detection_errors.push(format!("decoders_probe_failed:{err}"));
                BTreeSet::new()
            }
        };

    let candidate_apis = match config.preference {
        HardwarePreference::Auto => HardwareApi::ALL.to_vec(),
        HardwarePreference::Api(api) => vec![api],
        HardwarePreference::Off => Vec::new(),
    };

    let mut available_apis = Vec::new();
    let mut supported_decode_codecs = Vec::new();
    let mut supported_encode_codecs = Vec::new();
    let mut startup_probes = Vec::new();

    for api in candidate_apis {
        let configured = api_configured(api, &hwaccels, &encoders, &decoders);
        if !configured {
            continue;
        }

        let encode_probe = startup_probe_encoder(api, &encoders).await;
        let encode_startup_ok = encode_probe.as_ref().map(|probe| probe.ok).unwrap_or(true);
        if let Some(probe) = encode_probe {
            if !probe.ok {
                detection_errors.push(format!(
                    "{}_startup_probe_failed:{}",
                    api.as_str(),
                    probe.detail.as_deref().unwrap_or("unknown")
                ));
            }
            startup_probes.push(probe);
        }

        if !encode_startup_ok {
            continue;
        }

        available_apis.push(api.as_str().to_string());
        supported_encode_codecs.extend(detect_encode_codecs(api, &encoders));
        supported_decode_codecs.extend(detect_decode_codecs(api, &hwaccels, &decoders));
    }

    HardwareCapabilities {
        platform,
        ffmpeg_version,
        available_apis,
        capability_matrices: Vec::new(),
        supported_decode_codecs,
        supported_encode_codecs,
        max_sessions: None,
        hdr_tone_mapping: false,
        subtitle_burn_in_limitations: vec!["subtitle_burn_in_requires_software_filter".to_string()],
        startup_probes,
        detection_errors,
    }
}

fn current_platform() -> String {
    format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH)
}

fn api_configured(
    api: HardwareApi,
    hwaccels: &BTreeSet<String>,
    encoders: &BTreeSet<String>,
    decoders: &BTreeSet<String>,
) -> bool {
    match api {
        HardwareApi::VideoToolbox => {
            hwaccels.contains("videotoolbox") || encoders.contains("h264_videotoolbox")
        }
        HardwareApi::Vaapi => {
            hwaccels.contains("vaapi") || encoders.iter().any(|e| e.ends_with("_vaapi"))
        }
        HardwareApi::Qsv => {
            hwaccels.contains("qsv")
                || encoders.iter().any(|e| e.ends_with("_qsv"))
                || decoders.iter().any(|d| d.ends_with("_qsv"))
        }
        HardwareApi::Nvenc => encoders.iter().any(|e| e.ends_with("_nvenc")),
        HardwareApi::Amf => encoders.iter().any(|e| e.ends_with("_amf")),
    }
}

async fn startup_probe_encoder(
    api: HardwareApi,
    encoders: &BTreeSet<String>,
) -> Option<HardwareStartupProbe> {
    let encoder = match api {
        HardwareApi::VideoToolbox if encoders.contains("h264_videotoolbox") => "h264_videotoolbox",
        HardwareApi::Vaapi if encoders.contains("h264_vaapi") => "h264_vaapi",
        HardwareApi::Qsv if encoders.contains("h264_qsv") => "h264_qsv",
        HardwareApi::Nvenc if encoders.contains("h264_nvenc") => "h264_nvenc",
        HardwareApi::Amf if encoders.contains("h264_amf") => "h264_amf",
        _ => return None,
    };

    let mut args = vec![
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "testsrc=size=1280x720:rate=30:duration=0.2",
        "-frames:v",
        "1",
        "-c:v",
        encoder,
    ];
    if api == HardwareApi::VideoToolbox {
        args.extend(["-allow_sw", "0"]);
    }
    args.extend(["-f", "null", "-"]);

    let started = Instant::now();
    let result = run_ffmpeg_text(&args, STARTUP_PROBE_TIMEOUT).await;
    let duration = started.elapsed();
    let operation = format!("startup_encode:{encoder}");
    Some(match result {
        Ok(_) => {
            record_probe_metrics(
                api,
                &operation,
                HardwareReadinessStatus::Available,
                duration,
                true,
            );
            HardwareStartupProbe {
                api: api.as_str().to_string(),
                operation,
                ok: true,
                detail: None,
            }
        }
        Err(err) => {
            let status = classify_hardware_probe_failure(api, &err);
            record_probe_metrics(api, &operation, status, duration, false);
            HardwareStartupProbe {
                api: api.as_str().to_string(),
                operation,
                ok: false,
                detail: Some(err),
            }
        }
    })
}

fn detect_encode_codecs(
    api: HardwareApi,
    encoders: &BTreeSet<String>,
) -> Vec<HardwareCodecSupport> {
    match api {
        HardwareApi::VideoToolbox => [("h264", "h264_videotoolbox"), ("hevc", "hevc_videotoolbox")]
            .into_iter()
            .filter(|(_, encoder)| encoders.contains(*encoder))
            .map(|(codec, encoder)| HardwareCodecSupport::new(api, codec, encoder))
            .collect(),
        HardwareApi::Vaapi => hardware_suffix_codecs(api, encoders, "_vaapi"),
        HardwareApi::Qsv => hardware_suffix_codecs(api, encoders, "_qsv"),
        HardwareApi::Nvenc => hardware_suffix_codecs(api, encoders, "_nvenc"),
        HardwareApi::Amf => hardware_suffix_codecs(api, encoders, "_amf"),
    }
}

fn hardware_suffix_codecs(
    api: HardwareApi,
    components: &BTreeSet<String>,
    suffix: &str,
) -> Vec<HardwareCodecSupport> {
    ["h264", "hevc", "av1", "vp9", "mpeg2video"]
        .into_iter()
        .filter_map(|codec| {
            let ffmpeg_name = format!("{codec}{suffix}");
            components
                .contains(&ffmpeg_name)
                .then(|| HardwareCodecSupport::new(api, codec, &ffmpeg_name))
        })
        .collect()
}

fn detect_decode_codecs(
    api: HardwareApi,
    hwaccels: &BTreeSet<String>,
    decoders: &BTreeSet<String>,
) -> Vec<HardwareCodecSupport> {
    match api {
        HardwareApi::VideoToolbox if hwaccels.contains("videotoolbox") => ["h264", "hevc"]
            .into_iter()
            .map(|codec| HardwareCodecSupport::new(api, codec, "videotoolbox"))
            .collect(),
        HardwareApi::Vaapi if hwaccels.contains("vaapi") => {
            ["h264", "hevc", "mpeg2video", "vp9", "av1"]
                .into_iter()
                .map(|codec| HardwareCodecSupport::new(api, codec, "vaapi"))
                .collect()
        }
        HardwareApi::Qsv => hardware_suffix_codecs(api, decoders, "_qsv"),
        HardwareApi::Nvenc if hwaccels.contains("cuda") || hwaccels.contains("nvdec") => {
            ["h264", "hevc", "av1"]
                .into_iter()
                .map(|codec| HardwareCodecSupport::new(api, codec, "cuda"))
                .collect()
        }
        HardwareApi::Amf if hwaccels.contains("d3d11va") => ["h264", "hevc"]
            .into_iter()
            .map(|codec| HardwareCodecSupport::new(api, codec, "d3d11va"))
            .collect(),
        HardwareApi::Amf if hwaccels.contains("dxva2") => ["h264", "hevc"]
            .into_iter()
            .map(|codec| HardwareCodecSupport::new(api, codec, "dxva2"))
            .collect(),
        _ => Vec::new(),
    }
}

pub fn parse_hwaccels(raw: &str) -> BTreeSet<String> {
    raw.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .filter(|line| !line.starts_with("Hardware acceleration methods"))
        .map(str::to_ascii_lowercase)
        .collect()
}

pub fn parse_ffmpeg_components(raw: &str) -> BTreeSet<String> {
    raw.lines()
        .filter_map(|line| line.split_whitespace().nth(1))
        .filter(|name| !name.starts_with('='))
        .map(str::to_ascii_lowercase)
        .collect()
}

async fn run_ffmpeg_text(args: &[&str], wait: Duration) -> Result<String, String> {
    let output = timeout(
        wait,
        Command::new("ffmpeg")
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output(),
    )
    .await
    .map_err(|_| format!("ffmpeg timed out after {}s", wait.as_secs()))?
    .map_err(|err| format!("failed to spawn ffmpeg: {err}"))?;

    if output.status.success() {
        let mut text = String::from_utf8_lossy(&output.stdout).to_string();
        if text.trim().is_empty() {
            text = String::from_utf8_lossy(&output.stderr).to_string();
        }
        Ok(text)
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(format!(
            "ffmpeg exited with {:?}: {}",
            output.status.code(),
            stderr.lines().take(8).collect::<Vec<_>>().join(" | ")
        ))
    }
}

async fn run_ffmpeg_text_owned(args: &[String], wait: Duration) -> Result<String, String> {
    let output = timeout(
        wait,
        Command::new("ffmpeg")
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output(),
    )
    .await
    .map_err(|_| format!("ffmpeg timed out after {}s", wait.as_secs()))?
    .map_err(|err| format!("failed to spawn ffmpeg: {err}"))?;

    if output.status.success() {
        let mut text = String::from_utf8_lossy(&output.stdout).to_string();
        if text.trim().is_empty() {
            text = String::from_utf8_lossy(&output.stderr).to_string();
        }
        Ok(text)
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(format!(
            "ffmpeg exited with {:?}: {}",
            output.status.code(),
            stderr.lines().take(8).collect::<Vec<_>>().join(" | ")
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::DatabaseConfig,
        db::{Database, DatabaseDriver},
    };

    #[test]
    fn parses_hwaccels_and_components() {
        let hwaccels = parse_hwaccels(
            "Hardware acceleration methods:\n\
             videotoolbox\n\
             vaapi\n",
        );
        assert!(hwaccels.contains("videotoolbox"));
        assert!(hwaccels.contains("vaapi"));

        let encoders = parse_ffmpeg_components(
            " V....D h264_videotoolbox    VideoToolbox H.264 Encoder\n\
              V....D hevc_videotoolbox    VideoToolbox H.265 Encoder\n\
              V....D h264_nvenc           NVIDIA NVENC H.264 encoder\n",
        );
        assert!(encoders.contains("h264_videotoolbox"));
        assert!(encoders.contains("hevc_videotoolbox"));
        assert!(encoders.contains("h264_nvenc"));
    }

    #[test]
    fn reports_videotoolbox_codec_support_from_ffmpeg_fixtures() {
        let hwaccels = parse_hwaccels("Hardware acceleration methods:\nvideotoolbox\n");
        let encoders = parse_ffmpeg_components(
            " V....D h264_videotoolbox    VideoToolbox H.264 Encoder\n\
              V....D hevc_videotoolbox    VideoToolbox H.265 Encoder\n",
        );
        let decoders = BTreeSet::new();

        assert!(api_configured(
            HardwareApi::VideoToolbox,
            &hwaccels,
            &encoders,
            &decoders
        ));
        let encode = detect_encode_codecs(HardwareApi::VideoToolbox, &encoders);
        let decode = detect_decode_codecs(HardwareApi::VideoToolbox, &hwaccels, &decoders);

        assert!(encode.iter().any(|support| support.codec == "h264"));
        assert!(encode.iter().any(|support| support.codec == "hevc"));
        assert!(decode.iter().any(|support| support.codec == "h264"));
        assert!(decode.iter().any(|support| support.codec == "hevc"));
    }

    #[test]
    fn reports_amf_encode_with_windows_hardware_decode_from_ffmpeg_fixtures() {
        let hwaccels = parse_hwaccels("Hardware acceleration methods:\nd3d11va\ndxva2\n");
        let encoders = parse_ffmpeg_components(
            " V....D h264_amf             AMD AMF H.264 Encoder\n\
              V....D hevc_amf             AMD AMF H.265 Encoder\n",
        );
        let decoders = BTreeSet::new();

        assert!(api_configured(
            HardwareApi::Amf,
            &hwaccels,
            &encoders,
            &decoders
        ));
        let encode = detect_encode_codecs(HardwareApi::Amf, &encoders);
        let decode = detect_decode_codecs(HardwareApi::Amf, &hwaccels, &decoders);

        assert!(
            encode
                .iter()
                .any(|support| support.ffmpeg_name == "h264_amf")
        );
        assert!(
            decode
                .iter()
                .any(|support| { support.codec == "h264" && support.ffmpeg_name == "d3d11va" })
        );
        assert!(
            decode
                .iter()
                .any(|support| { support.codec == "hevc" && support.ffmpeg_name == "d3d11va" })
        );
    }

    #[test]
    fn parses_preferences_and_fallbacks() {
        assert_eq!(HardwarePreference::parse("auto"), HardwarePreference::Auto);
        assert_eq!(HardwarePreference::parse("off"), HardwarePreference::Off);
        assert_eq!(
            HardwarePreference::parse("videotoolbox"),
            HardwarePreference::Api(HardwareApi::VideoToolbox)
        );
        assert_eq!(
            HardwareFallbackPolicy::parse("fail"),
            HardwareFallbackPolicy::Fail
        );
        assert_eq!(
            HardwareFallbackPolicy::parse("software"),
            HardwareFallbackPolicy::Software
        );
    }

    #[test]
    fn provider_candidates_gate_nvenc_on_windows_amd_hosts() {
        let inventory = fixture_inventory(
            "windows",
            Some(("amd", "Radeon Pro V520")),
            &["h264_nvenc", "h264_amf", "hevc_amf"],
            &["d3d11va"],
            &[],
        );

        let candidates = hardware_provider_candidates(&inventory, HardwarePreference::Auto);
        let applicable = candidates
            .iter()
            .filter(|candidate| candidate.applicable)
            .map(|candidate| candidate.id.as_str())
            .collect::<Vec<_>>();

        assert!(applicable.contains(&"windows_amd_amf"));
        assert!(!applicable.contains(&"windows_nvidia_nvenc"));
        let nvenc = candidates
            .iter()
            .find(|candidate| candidate.id == "windows_nvidia_nvenc")
            .expect("windows nvenc candidate is tracked for diagnostics");
        assert_eq!(nvenc.reason.as_deref(), Some("gpu_vendor_not_present"));
    }

    #[test]
    fn provider_candidates_gate_to_videotoolbox_on_macos() {
        let inventory = fixture_inventory(
            "macos",
            Some(("apple", "Apple M3")),
            &["h264_videotoolbox", "hevc_videotoolbox", "h264_nvenc"],
            &["videotoolbox"],
            &[],
        );

        let candidates = hardware_provider_candidates(&inventory, HardwarePreference::Auto);
        let applicable = candidates
            .iter()
            .filter(|candidate| candidate.applicable)
            .map(|candidate| candidate.id.as_str())
            .collect::<Vec<_>>();

        assert_eq!(applicable, vec!["macos_videotoolbox"]);
    }

    #[test]
    fn classifies_nvidia_cumemallocasync_as_driver_runtime_incompatible() {
        let status = classify_hardware_probe_failure(
            HardwareApi::Nvenc,
            "ffmpeg exited with Some(-1): [h264_nvenc] Cannot load cuMemAllocAsync",
        );
        assert_eq!(status, HardwareReadinessStatus::DriverRuntimeIncompatible);
        assert_eq!(
            hardware_readiness_message_code(HardwareApi::Nvenc, status),
            "nvidia_driver_update_required"
        );
    }

    #[test]
    fn matrix_helpers_do_not_promote_unsupported_av1() {
        let capabilities = HardwareCapabilities {
            platform: "windows-x86_64".to_string(),
            ffmpeg_version: Some("ffmpeg fixture".to_string()),
            available_apis: vec!["nvenc".to_string()],
            capability_matrices: vec![HardwareCapabilityMatrix {
                schema_version: HARDWARE_READINESS_SCHEMA_VERSION,
                api: "nvenc".to_string(),
                status: HardwareReadinessStatus::Available,
                encode: vec![
                    HardwareCodecMatrixEntry {
                        codec: "h264".to_string(),
                        profile: Some("high".to_string()),
                        bit_depth: Some(8),
                        pixel_formats: vec!["yuv420p".to_string()],
                        ffmpeg_encoder: Some("h264_nvenc".to_string()),
                        ffmpeg_decoder: None,
                        status: MATRIX_STATUS_SUPPORTED.to_string(),
                    },
                    HardwareCodecMatrixEntry {
                        codec: "av1".to_string(),
                        profile: Some("main10".to_string()),
                        bit_depth: Some(10),
                        pixel_formats: vec!["p010le".to_string()],
                        ffmpeg_encoder: Some("av1_nvenc".to_string()),
                        ffmpeg_decoder: None,
                        status: HardwareReadinessStatus::UnsupportedGpu.as_str().to_string(),
                    },
                ],
                decode: Vec::new(),
                filters: HardwareFilterMatrix::default(),
            }],
            supported_decode_codecs: Vec::new(),
            supported_encode_codecs: vec![HardwareCodecSupport {
                api: "nvenc".to_string(),
                codec: "av1".to_string(),
                ffmpeg_name: "av1_nvenc".to_string(),
            }],
            max_sessions: None,
            hdr_tone_mapping: false,
            subtitle_burn_in_limitations: Vec::new(),
            startup_probes: Vec::new(),
            detection_errors: Vec::new(),
        };

        assert!(
            capabilities
                .supported_encode_matrix_entry(
                    HardwareApi::Nvenc,
                    "h264",
                    Some("High"),
                    Some(8),
                    Some("yuv420p")
                )
                .is_some()
        );
        assert!(
            capabilities
                .supported_encode_matrix_entry(
                    HardwareApi::Nvenc,
                    "av1",
                    Some("Main 10"),
                    Some(10),
                    Some("p010le")
                )
                .is_none()
        );
        assert_eq!(
            capabilities.preferred_api_for_encode("h264"),
            Some(HardwareApi::Nvenc)
        );
        assert_eq!(capabilities.preferred_api_for_encode("av1"), None);
    }

    #[test]
    fn shell_quote_handles_single_quotes() {
        assert_eq!(shell_quote("ffmpeg"), "'ffmpeg'");
        assert_eq!(shell_quote("we'ird"), "'we'\"'\"'ird'");
    }

    #[tokio::test]
    async fn unavailable_provider_matrix_never_marks_decode_supported() {
        let hwaccels = parse_hwaccels("Hardware acceleration methods:\ncuda\n");
        let encoders = parse_ffmpeg_components(
            " V....D h264_nvenc           NVIDIA NVENC H.264 encoder\n\
              V....D av1_nvenc            NVIDIA NVENC AV1 encoder\n",
        );
        let decoders = BTreeSet::new();

        let (matrix, probes) = capability_matrix_for_api(
            HardwareApi::Nvenc,
            HardwareReadinessStatus::DriverRuntimeIncompatible,
            &hwaccels,
            &encoders,
            &decoders,
        )
        .await;

        assert!(matrix.encode.iter().any(|entry| entry.codec == "h264"));
        assert!(matrix.decode.iter().any(|entry| entry.codec == "av1"));
        assert!(
            matrix
                .encode
                .iter()
                .chain(matrix.decode.iter())
                .all(|entry| entry.status != MATRIX_STATUS_SUPPORTED)
        );
        assert!(
            probes
                .iter()
                .all(|probe| probe.status == HardwareReadinessStatus::DriverRuntimeIncompatible)
        );
    }

    #[tokio::test]
    async fn readiness_records_round_trip_and_stale_in_database() -> Result<()> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        assert_eq!(database.driver, DatabaseDriver::Sqlite);
        database.run_migrations().await?;

        let inventory = fixture_inventory(
            "windows",
            Some(("nvidia", "GeForce RTX 2070")),
            &["h264_nvenc"],
            &["cuda"],
            &[],
        );
        let fingerprint = host_hardware_fingerprint(&inventory);
        let matrix = HardwareCapabilityMatrix {
            schema_version: HARDWARE_READINESS_SCHEMA_VERSION,
            api: "nvenc".to_string(),
            status: HardwareReadinessStatus::DriverRuntimeIncompatible,
            encode: vec![HardwareCodecMatrixEntry {
                codec: "h264".to_string(),
                profile: Some("high".to_string()),
                bit_depth: Some(8),
                pixel_formats: vec!["nv12".to_string()],
                ffmpeg_encoder: Some("h264_nvenc".to_string()),
                ffmpeg_decoder: None,
                status: "driver_runtime_incompatible".to_string(),
            }],
            decode: Vec::new(),
            filters: HardwareFilterMatrix::default(),
        };
        let probe_report = HardwareProbeReport {
            api: "nvenc".to_string(),
            startup_probes: vec![HardwareStartupProbe {
                api: "nvenc".to_string(),
                operation: "encode:h264_nvenc".to_string(),
                ok: false,
                detail: Some("Cannot load cuMemAllocAsync".to_string()),
            }],
            capability_probes: Vec::new(),
            detection_errors: vec!["nvenc_startup_probe_failed".to_string()],
        };
        let record = HardwareReadinessRecord::new(
            fingerprint.clone(),
            "windows_nvidia_nvenc",
            HardwareApi::Nvenc,
            inventory.clone(),
            inventory.gpus.first(),
            HardwareReadinessStatus::DriverRuntimeIncompatible,
            "startup_probe_failed:driver_runtime_incompatible",
            "nvidia_driver_update_required",
            matrix,
            probe_report,
            Some("Cannot load cuMemAllocAsync".to_string()),
        );

        upsert_hardware_readiness_record(&database.pool, &record).await?;
        let loaded = load_current_hardware_readiness_records(&database.pool, &fingerprint).await?;
        assert_eq!(loaded.len(), 1);
        assert_eq!(
            loaded[0].status,
            HardwareReadinessStatus::DriverRuntimeIncompatible
        );
        assert_eq!(loaded[0].user_message_code, "nvidia_driver_update_required");
        assert_eq!(loaded[0].gpu_driver_version.as_deref(), Some("551.86"));

        let event_id = append_hardware_readiness_event(
            &database.pool,
            Some(&loaded[0].id),
            "probe_completed",
            loaded[0].status,
            &loaded[0].user_message_code,
            &json!({"detail": "driver mismatch"}),
        )
        .await?;
        assert!(!event_id.is_empty());

        let stale_count =
            mark_hardware_readiness_stale_except(&database.pool, "sha256:new-fingerprint").await?;
        assert_eq!(stale_count, 1);
        let reloaded =
            load_current_hardware_readiness_records(&database.pool, &fingerprint).await?;
        assert!(reloaded.is_empty());
        Ok(())
    }

    fn fixture_inventory(
        os_family: &str,
        gpu: Option<(&str, &str)>,
        encoders: &[&str],
        hwaccels: &[&str],
        decoders: &[&str],
    ) -> HostHardwareInventory {
        HostHardwareInventory {
            os: HostOsInventory {
                family: os_family.to_string(),
                version: Some("test".to_string()),
                arch: "x86_64".to_string(),
            },
            gpus: gpu
                .map(|(vendor, model)| {
                    vec![HostGpuInventory {
                        vendor: Some(vendor.to_string()),
                        model: Some(model.to_string()),
                        device_id: Some("0xTEST".to_string()),
                        driver_version: Some("551.86".to_string()),
                        raw: json!({"fixture": true}),
                    }]
                })
                .unwrap_or_default(),
            ffmpeg: FfmpegHardwareInventory {
                path: Some("/usr/bin/ffmpeg".to_string()),
                version: Some("ffmpeg version test".to_string()),
                sha256: Some("sha256:test".to_string()),
                hwaccels: hwaccels.iter().map(|value| (*value).to_string()).collect(),
                encoders: encoders.iter().map(|value| (*value).to_string()).collect(),
                decoders: decoders.iter().map(|value| (*value).to_string()).collect(),
            },
        }
    }
}
