use std::{
    collections::{HashMap, HashSet},
    future::Future,
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{AnyPool, Row, TypeInfo, Value as SqlxValue, ValueRef, any::AnyRow};
use uuid::Uuid;

use crate::config::Settings;
use crate::db::models::{ExtensionInstance, Provider, SecretScope};
use crate::download_broker::{
    DEBRID_DEFAULT_LOGICAL_ID, DownloadBrokerBindingKind, DownloadBrokerRouteInventory,
    DownloadBrokerRouteUpdate, TORRENT_DEFAULT_LOGICAL_ID, USENET_DEFAULT_LOGICAL_ID,
    list_acquisition_routes, upsert_acquisition_route,
};
use crate::drivers::DownloaderTorrentPatch;
use crate::extensions::auto_managed::{is_nzbget_extension_id, is_qbittorrent_extension_id};
use crate::extensions::store::{ExtensionStore, NewSecret};
use crate::runtime::model::{
    ContainerSpec, EnvVar, VolumeMount, VolumeMountSourceKind, apply_container_spec_fingerprint,
};
use crate::secrets::SecretsManager;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DownloadProtectionMode {
    ExternalOnly,
    Direct,
    CloudflareWarp,
    WireguardConfig,
    OpenvpnConfig,
    ProviderPreset,
    DebridOnly,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DownloadNetworkProfileKind {
    ExternalOnly,
    Direct,
    CloudflareWarp,
    WireguardConfig,
    OpenvpnConfig,
    ProviderPreset,
    DebridOnly,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DownloadProtectionState {
    Direct,
    Protected,
    ExternallyManaged,
    Blocked,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DownloadTorrentReachabilityState {
    ForwardedPort,
    NoForwardedPort,
    ExternallyManaged,
    DebridOnly,
    Unknown,
    NotApplicable,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DownloadForwardedPort {
    pub port: u16,
    pub protocol: String,
    pub source: String,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DownloadTorrentReachability {
    pub state: DownloadTorrentReachabilityState,
    pub can_accept_inbound: bool,
    pub listen_port: Option<u16>,
    pub forwarded_port: Option<DownloadForwardedPort>,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DownloadProtectionCheckStatus {
    Pass,
    Warn,
    Fail,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DownloadProtectionSeverity {
    Info,
    Warning,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DownloadProtectionCheck {
    pub code: String,
    pub status: DownloadProtectionCheckStatus,
    pub severity: DownloadProtectionSeverity,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DownloadProtectionBlocker {
    pub code: String,
    pub title: String,
    pub detail: String,
    pub severity: DownloadProtectionSeverity,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DownloadProtectionProbeEvidence {
    pub status: DownloadProtectionCheckStatus,
    pub value: Option<String>,
    pub detail: String,
    pub checked_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DownloadProtectionRuntimeEvidence {
    #[serde(default)]
    pub server_public_ip: Option<DownloadProtectionProbeEvidence>,
    #[serde(default)]
    pub gateway_public_ip: Option<DownloadProtectionProbeEvidence>,
    #[serde(default)]
    pub downloader_public_ip: Option<DownloadProtectionProbeEvidence>,
    #[serde(default)]
    pub gateway_dns: Option<DownloadProtectionProbeEvidence>,
    #[serde(default)]
    pub downloader_dns: Option<DownloadProtectionProbeEvidence>,
    #[serde(default)]
    pub kill_switch: Option<DownloadProtectionProbeEvidence>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DownloadProtectionGatewayStatus {
    pub runtime: Option<String>,
    pub state: String,
    pub public_ip: Option<String>,
    pub last_checked_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManagedDownloaderPresence {
    pub qbittorrent: bool,
    pub nzbget: bool,
    pub external_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DownloadProtectionProfileSummary {
    pub id: String,
    pub name: String,
    pub kind: DownloadNetworkProfileKind,
    pub enabled: bool,
    pub strict: bool,
    pub scope: String,
    pub provider: Option<String>,
    pub gateway_runtime: Option<String>,
    pub status: DownloadProtectionState,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DownloadProtectionStatus {
    pub mode: DownloadProtectionMode,
    pub state: DownloadProtectionState,
    pub strict: bool,
    pub protected_apps: Vec<String>,
    pub server_public_ip: Option<String>,
    pub downloader_public_ip: Option<String>,
    pub gateway: Option<DownloadProtectionGatewayStatus>,
    pub torrent_reachability: DownloadTorrentReachability,
    pub managed_downloaders: ManagedDownloaderPresence,
    pub active_profile: DownloadProtectionProfileSummary,
    pub checks: Vec<DownloadProtectionCheck>,
    pub blocker: Option<DownloadProtectionBlocker>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DownloadProtectionProfilesResponse {
    pub profiles: Vec<DownloadProtectionProfileSummary>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DownloadNetworkProfileImportRequest {
    #[serde(default)]
    pub profile_id: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default = "default_true")]
    pub strict: bool,
    pub config: String,
    #[serde(default)]
    pub gateway_image: Option<String>,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub forwarded_port: Option<DownloadForwardedPort>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DownloadNetworkProfileImportResponse {
    pub profile: DownloadProtectionProfileSummary,
    pub checks: Vec<DownloadProtectionCheck>,
    pub blocker: Option<DownloadProtectionBlocker>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DownloadNetworkEventRecord {
    pub id: Uuid,
    pub profile_id: Option<String>,
    pub operation: String,
    pub status: String,
    pub evidence: serde_json::Value,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DownloadProtectionSwitchStatus {
    PreflightPassed,
    Blocked,
    Applied,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DownloadProtectionSwitchPhaseStatus {
    Pending,
    Pass,
    Fail,
    Skipped,
    Blocked,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DownloadProtectionSwitchPhase {
    pub id: String,
    pub status: DownloadProtectionSwitchPhaseStatus,
    pub detail: String,
    pub blocker: Option<DownloadProtectionBlocker>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DownloadProtectionSwitchRequest {
    pub target_profile_id: String,
    #[serde(default)]
    pub apply: bool,
    #[serde(default)]
    pub expected_active_profile_id: Option<String>,
    #[serde(default)]
    pub server_public_ip: Option<String>,
    #[serde(default)]
    pub downloader_public_ip: Option<String>,
    #[serde(default)]
    pub runtime_evidence: Option<DownloadProtectionRuntimeEvidence>,
}

impl DownloadProtectionSwitchRequest {
    fn without_pre_apply_runtime_evidence(&self) -> Self {
        let mut request = self.clone();
        request.server_public_ip = None;
        request.downloader_public_ip = None;
        request.runtime_evidence = None;
        request
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DownloadProtectionSwitchResponse {
    pub operation_id: Uuid,
    pub status: DownloadProtectionSwitchStatus,
    pub apply_requested: bool,
    pub ready_to_apply: bool,
    pub applied: bool,
    pub previous_profile: DownloadProtectionProfileSummary,
    pub target_profile: DownloadProtectionProfileSummary,
    pub checks: Vec<DownloadProtectionCheck>,
    pub phases: Vec<DownloadProtectionSwitchPhase>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_evidence: Option<DownloadProtectionRuntimeEvidence>,
    pub blocker: Option<DownloadProtectionBlocker>,
}

pub const CLOUDFLARE_WARP_PROFILE_ID: &str = "cloudflare-warp";
const FIRST_RUN_EXTERNAL_ONLY_PROFILE_ID: &str = "external-only";
const FIRST_RUN_SKIP_DOWNLOADS_PROFILE_ID: &str = "downloads-skipped";
const CLOUDFLARE_WARP_DISCLOSURE_VERSION: &str = "2026-04-29";
const CLOUDFLARE_WARP_IDENTITY_SECRET_KEY: &str = "cloudflare_warp_identity";
const DEFAULT_CLOUDFLARE_WARP_GATEWAY_IMAGE: &str =
    "caomingjun/warp:2026.3.846.0-2.12.0-bf3508b88dc075e973e8b09d078c897a414d84e8";

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CloudflareWarpDisclosure {
    pub version: String,
    pub title: String,
    pub body: String,
    pub limitations: Vec<String>,
    pub required_acceptance: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CloudflareWarpProfileRequest {
    pub accepted_disclosure: bool,
    #[serde(default)]
    pub profile_id: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CloudflareWarpEnrollmentStatus {
    pub profile_id: String,
    pub enrollment_id: String,
    pub status: String,
    pub identity_secret_ref: String,
    pub disclosure_version: String,
    pub disclosure_accepted_at: Option<DateTime<Utc>>,
    pub last_checked_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CloudflareWarpProfileResponse {
    pub profile: DownloadProtectionProfileSummary,
    pub enrollment: CloudflareWarpEnrollmentStatus,
    pub disclosure: CloudflareWarpDisclosure,
    pub checks: Vec<DownloadProtectionCheck>,
    pub blocker: Option<DownloadProtectionBlocker>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CloudflareWarpResetRequest {
    pub confirm_reset: bool,
    #[serde(default)]
    pub profile_id: Option<String>,
    #[serde(default = "default_true")]
    pub recreate: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CloudflareWarpResetResponse {
    pub profile: Option<DownloadProtectionProfileSummary>,
    pub enrollment: Option<CloudflareWarpEnrollmentStatus>,
    pub disclosure: CloudflareWarpDisclosure,
    pub reset: bool,
    pub recreated: bool,
    pub checks: Vec<DownloadProtectionCheck>,
    pub blocker: Option<DownloadProtectionBlocker>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DownloadProtectionFirstRunChoice {
    ProtectedDownloads,
    ExistingStack,
    CustomVpn,
    SkipDownloads,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DownloadProtectionFirstRunRequest {
    pub choice: DownloadProtectionFirstRunChoice,
    #[serde(default)]
    pub accepted_warp_disclosure: bool,
    #[serde(default = "default_true")]
    pub apply: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadProtectionFirstRunResponse {
    pub choice: DownloadProtectionFirstRunChoice,
    pub completed: bool,
    pub applied: bool,
    pub profile: Option<DownloadProtectionProfileSummary>,
    pub switch_result: Option<DownloadProtectionSwitchResponse>,
    pub routes: DownloadBrokerRouteInventory,
    pub checks: Vec<DownloadProtectionCheck>,
    pub blocker: Option<DownloadProtectionBlocker>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CloudflareWarpDiagnostics {
    pub profile: Option<DownloadProtectionProfileSummary>,
    pub enrollment: Option<CloudflareWarpEnrollmentStatus>,
    pub checks: Vec<DownloadProtectionCheck>,
    pub blocker: Option<DownloadProtectionBlocker>,
    pub recent_events: Vec<DownloadNetworkEventRecord>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DownloadProviderPresetCatalog {
    pub presets: Vec<DownloadProviderPreset>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DownloadProviderPreset {
    pub id: String,
    pub name: String,
    pub provider: String,
    pub profile_kinds: Vec<DownloadNetworkProfileKind>,
    pub gateway_runtimes: Vec<String>,
    pub import_methods: Vec<String>,
    pub port_forwarding: DownloadProviderPortForwarding,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DownloadProviderPortForwarding {
    Unsupported,
    Manual,
    ProviderApi,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QbittorrentListenPortSyncStatus {
    Ready,
    NotApplicable,
    Blocked,
    NoManagedQbittorrent,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct QbittorrentListenPortSyncPlan {
    pub status: QbittorrentListenPortSyncStatus,
    pub target_provider_id: Option<Uuid>,
    pub target_instance_id: Option<Uuid>,
    pub target_port: Option<u16>,
    pub capability: Option<String>,
    pub patch: Option<serde_json::Value>,
    pub requires_orchestrator: bool,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct QbittorrentListenPortSyncApplyResponse {
    pub applied: bool,
    pub plan: QbittorrentListenPortSyncPlan,
    pub notes: Vec<String>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DownloadNetworkProfile {
    pub id: String,
    pub name: String,
    pub kind: DownloadNetworkProfileKind,
    pub enabled: bool,
    pub strict: bool,
    pub scope: String,
    pub provider: Option<String>,
    pub gateway_runtime: Option<String>,
    pub config_json: serde_json::Value,
    pub status: DownloadProtectionState,
    pub active: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub last_applied_at: Option<DateTime<Utc>>,
    pub last_verified_at: Option<DateTime<Utc>>,
}

impl DownloadNetworkProfile {
    pub fn validate(&self) -> Result<()> {
        validate_download_network_profile_parts(
            &self.id,
            &self.name,
            &self.kind,
            self.strict,
            &self.scope,
            &self.config_json,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadProtectionProfile {
    pub id: String,
    pub name: String,
    pub kind: DownloadNetworkProfileKind,
    pub strict: bool,
    pub runtime: GatewayRuntime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub enum GatewayRuntime {
    None,
    GluetunWireguard(GluetunWireguardGatewayRuntime),
    GluetunOpenvpn(GluetunOpenvpnGatewayRuntime),
    CloudflareWarp(CloudflareWarpGatewayRuntime),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GluetunWireguardGatewayRuntime {
    pub image: String,
    pub config_host_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GluetunOpenvpnGatewayRuntime {
    pub image: String,
    pub config_host_path: String,
    pub auth_host_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudflareWarpGatewayRuntime {
    pub image: String,
    pub state_volume_name: String,
    pub enrollment_id: String,
    pub identity_secret_ref: String,
}

#[derive(Debug, Clone)]
pub struct DownloadProtectionCompileInput<'a> {
    pub app_container_name: &'a str,
    pub app_spec: &'a ContainerSpec,
    pub base_labels: &'a HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct CompiledDownloadProtectionProfile {
    pub gateway_spec: Option<ContainerSpec>,
    pub protected_app_spec: ContainerSpec,
}

#[derive(Debug, Clone)]
struct ManagedDownloaderInventory {
    qbittorrent_instance_ids: Vec<Uuid>,
    nzbget_instance_ids: Vec<Uuid>,
    external_count: usize,
}

#[derive(Debug, Clone)]
struct StoredDownloadNetworkProfile {
    id: String,
    name: String,
    kind: DownloadNetworkProfileKind,
    enabled: bool,
    strict: bool,
    scope: String,
    provider: Option<String>,
    gateway_runtime: Option<String>,
    config_json: serde_json::Value,
    status: DownloadProtectionState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActiveManagedDownloaderRuntime {
    NoStoredProfile,
    Direct,
    WireguardConfig {
        profile_id: String,
        secret_ref: String,
        gateway_image: Option<String>,
    },
    OpenvpnConfig {
        profile_id: String,
        config_secret_ref: String,
        username_secret_ref: Option<String>,
        password_secret_ref: Option<String>,
        gateway_image: Option<String>,
    },
    CloudflareWarp {
        profile_id: String,
        enrollment_id: String,
        identity_secret_ref: String,
        gateway_image: String,
        state_volume_name: String,
    },
    UnsupportedProtected {
        profile_id: String,
        kind: DownloadNetworkProfileKind,
    },
}

const VALID_DOWNLOAD_PROFILE_SCOPES: &[&str] = &[
    "managed_downloaders",
    "managed_downloaders_and_indexers",
    "custom",
];

#[derive(Debug, Clone)]
struct StoredWarpEnrollment {
    profile_id: String,
    enrollment_id: String,
    identity_secret_ref: String,
    status: String,
    disclosure_version: String,
    disclosure_accepted_at: Option<DateTime<Utc>>,
    last_checked_at: Option<DateTime<Utc>>,
    last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SecretCheck {
    Present,
    Missing(String),
    Empty(String),
    Unreadable(String),
    InvalidRef(String),
    Unknown(String),
}

trait GatewayRuntimeCompiler {
    fn compile(
        &self,
        profile: &DownloadProtectionProfile,
        input: DownloadProtectionCompileInput<'_>,
    ) -> Result<CompiledDownloadProtectionProfile>;
}

pub(crate) const DOWNLOAD_NETWORK_PROFILE_ID_LABEL: &str = "elixir.download_network.profile_id";
pub(crate) const DOWNLOAD_NETWORK_PROFILE_KIND_LABEL: &str = "elixir.download_network.profile_kind";
pub(crate) const DOWNLOAD_NETWORK_RUNTIME_KIND_LABEL: &str = "elixir.download_network.runtime_kind";
pub(crate) const DOWNLOAD_NETWORK_EXPOSED_PORTS_LABEL: &str =
    "elixir.download_network.exposed_ports";

fn stamp_download_network_labels(
    spec: &mut ContainerSpec,
    profile: &DownloadProtectionProfile,
    runtime_kind: &str,
    source_app_spec: &ContainerSpec,
) {
    spec.labels.insert(
        DOWNLOAD_NETWORK_PROFILE_ID_LABEL.to_string(),
        profile.id.clone(),
    );
    spec.labels.insert(
        DOWNLOAD_NETWORK_PROFILE_KIND_LABEL.to_string(),
        profile_kind_as_str(&profile.kind).to_string(),
    );
    spec.labels.insert(
        DOWNLOAD_NETWORK_RUNTIME_KIND_LABEL.to_string(),
        runtime_kind.to_string(),
    );
    spec.labels.insert(
        DOWNLOAD_NETWORK_EXPOSED_PORTS_LABEL.to_string(),
        exposed_container_ports_label(source_app_spec),
    );
    apply_container_spec_fingerprint(spec);
}

pub(crate) fn exposed_container_ports_label(spec: &ContainerSpec) -> String {
    let mut ports = spec
        .ports
        .iter()
        .map(|port| {
            format!(
                "{}/{}",
                port.container_port,
                port.protocol.as_deref().unwrap_or("tcp")
            )
        })
        .collect::<Vec<_>>();
    ports.sort();
    ports.join(",")
}

impl DownloadProtectionProfile {
    #[allow(dead_code)]
    pub fn direct(id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            kind: DownloadNetworkProfileKind::Direct,
            strict: false,
            runtime: GatewayRuntime::None,
        }
    }

    pub fn wireguard_config(
        id: impl Into<String>,
        name: impl Into<String>,
        strict: bool,
        gateway_runtime: GluetunWireguardGatewayRuntime,
    ) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            kind: DownloadNetworkProfileKind::WireguardConfig,
            strict,
            runtime: GatewayRuntime::GluetunWireguard(gateway_runtime),
        }
    }

    pub fn cloudflare_warp(
        id: impl Into<String>,
        name: impl Into<String>,
        strict: bool,
        gateway_runtime: CloudflareWarpGatewayRuntime,
    ) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            kind: DownloadNetworkProfileKind::CloudflareWarp,
            strict,
            runtime: GatewayRuntime::CloudflareWarp(gateway_runtime),
        }
    }

    pub fn openvpn_config(
        id: impl Into<String>,
        name: impl Into<String>,
        strict: bool,
        gateway_runtime: GluetunOpenvpnGatewayRuntime,
    ) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            kind: DownloadNetworkProfileKind::OpenvpnConfig,
            strict,
            runtime: GatewayRuntime::GluetunOpenvpn(gateway_runtime),
        }
    }

    pub fn compile(
        &self,
        input: DownloadProtectionCompileInput<'_>,
    ) -> Result<CompiledDownloadProtectionProfile> {
        match &self.runtime {
            GatewayRuntime::None => {
                let mut protected_app_spec = input.app_spec.clone();
                stamp_download_network_labels(
                    &mut protected_app_spec,
                    self,
                    "direct",
                    input.app_spec,
                );
                Ok(CompiledDownloadProtectionProfile {
                    gateway_spec: None,
                    protected_app_spec,
                })
            }
            GatewayRuntime::GluetunWireguard(runtime) => runtime.compile(self, input),
            GatewayRuntime::GluetunOpenvpn(runtime) => runtime.compile(self, input),
            GatewayRuntime::CloudflareWarp(runtime) => runtime.compile(self, input),
        }
    }
}

impl GatewayRuntimeCompiler for GluetunWireguardGatewayRuntime {
    fn compile(
        &self,
        profile: &DownloadProtectionProfile,
        input: DownloadProtectionCompileInput<'_>,
    ) -> Result<CompiledDownloadProtectionProfile> {
        let image = self.image.trim();
        if image.is_empty() {
            bail!(
                "download protection profile '{}' has an empty gateway image",
                profile.id
            );
        }
        let config_host_path = self.config_host_path.trim();
        if config_host_path.is_empty() {
            bail!(
                "download protection profile '{}' has an empty WireGuard config path",
                profile.id
            );
        }
        let app_container_name = input.app_container_name.trim();
        if app_container_name.is_empty() {
            bail!(
                "download protection profile '{}' has an empty app container name",
                profile.id
            );
        }

        let gateway_name = format!("{app_container_name}-vpn");
        let mut labels = input.base_labels.clone();
        labels.insert(
            "elixir.network_role".to_string(),
            "wireguard_gateway".to_string(),
        );

        let mut sysctls = HashMap::new();
        sysctls.insert(
            "net.ipv4.conf.all.src_valid_mark".to_string(),
            "1".to_string(),
        );

        let input_ports = input
            .app_spec
            .ports
            .iter()
            .map(|port| port.container_port.to_string())
            .collect::<Vec<_>>()
            .join(",");

        let mut gateway_env = vec![
            EnvVar {
                name: "VPN_SERVICE_PROVIDER".to_string(),
                value: "custom".to_string(),
            },
            EnvVar {
                name: "VPN_TYPE".to_string(),
                value: "wireguard".to_string(),
            },
            EnvVar {
                name: "WIREGUARD_CONF_FILE".to_string(),
                value: "wg0.conf".to_string(),
            },
            EnvVar {
                name: "FIREWALL_OUTBOUND_SUBNETS".to_string(),
                value: "10.0.0.0/8,172.16.0.0/12,192.168.0.0/16".to_string(),
            },
        ];
        if !input_ports.is_empty() {
            gateway_env.push(EnvVar {
                name: "FIREWALL_INPUT_PORTS".to_string(),
                value: input_ports,
            });
        }

        let mut gateway_spec = ContainerSpec {
            name: gateway_name.clone(),
            image: image.to_string(),
            network: input.app_spec.network.clone(),
            network_mode: None,
            aliases: input.app_spec.aliases.clone(),
            env: gateway_env,
            volumes: vec![VolumeMount {
                source_kind: VolumeMountSourceKind::Bind,
                host_path: config_host_path.to_string(),
                container_path: "/gluetun/wireguard/wg0.conf".to_string(),
                read_only: true,
            }],
            ports: input.app_spec.ports.clone(),
            labels,
            command: Vec::new(),
            cap_add: vec!["NET_ADMIN".to_string()],
            cap_drop: Vec::new(),
            devices: vec!["/dev/net/tun:/dev/net/tun".to_string()],
            sysctls,
            security: Default::default(),
        };

        let mut protected_app_spec = input.app_spec.clone();
        protected_app_spec.network_mode = Some(format!("container:{gateway_name}"));
        protected_app_spec.aliases.clear();
        protected_app_spec.ports.clear();
        stamp_download_network_labels(
            &mut gateway_spec,
            profile,
            "gluetun_wireguard",
            input.app_spec,
        );
        stamp_download_network_labels(
            &mut protected_app_spec,
            profile,
            "gluetun_wireguard",
            input.app_spec,
        );

        Ok(CompiledDownloadProtectionProfile {
            gateway_spec: Some(gateway_spec),
            protected_app_spec,
        })
    }
}

impl GatewayRuntimeCompiler for GluetunOpenvpnGatewayRuntime {
    fn compile(
        &self,
        profile: &DownloadProtectionProfile,
        input: DownloadProtectionCompileInput<'_>,
    ) -> Result<CompiledDownloadProtectionProfile> {
        let image = self.image.trim();
        if image.is_empty() {
            bail!(
                "download protection profile '{}' has an empty OpenVPN gateway image",
                profile.id
            );
        }
        let config_host_path = self.config_host_path.trim();
        if config_host_path.is_empty() {
            bail!(
                "download protection profile '{}' has an empty OpenVPN config path",
                profile.id
            );
        }
        let app_container_name = input.app_container_name.trim();
        if app_container_name.is_empty() {
            bail!(
                "download protection profile '{}' has an empty app container name",
                profile.id
            );
        }

        let gateway_name = format!("{app_container_name}-vpn");
        let mut labels = input.base_labels.clone();
        labels.insert(
            "elixir.network_role".to_string(),
            "openvpn_gateway".to_string(),
        );

        let input_ports = input
            .app_spec
            .ports
            .iter()
            .map(|port| port.container_port.to_string())
            .collect::<Vec<_>>()
            .join(",");

        let mut gateway_env = vec![
            EnvVar {
                name: "VPN_SERVICE_PROVIDER".to_string(),
                value: "custom".to_string(),
            },
            EnvVar {
                name: "VPN_TYPE".to_string(),
                value: "openvpn".to_string(),
            },
            EnvVar {
                name: "OPENVPN_CUSTOM_CONFIG".to_string(),
                value: "/gluetun/custom.conf".to_string(),
            },
            EnvVar {
                name: "FIREWALL_OUTBOUND_SUBNETS".to_string(),
                value: "10.0.0.0/8,172.16.0.0/12,192.168.0.0/16".to_string(),
            },
        ];
        if !input_ports.is_empty() {
            gateway_env.push(EnvVar {
                name: "FIREWALL_INPUT_PORTS".to_string(),
                value: input_ports,
            });
        }

        let mut volumes = vec![VolumeMount {
            source_kind: VolumeMountSourceKind::Bind,
            host_path: config_host_path.to_string(),
            container_path: "/gluetun/custom.conf".to_string(),
            read_only: true,
        }];
        if let Some(auth_host_path) = self
            .auth_host_path
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            volumes.push(VolumeMount {
                source_kind: VolumeMountSourceKind::Bind,
                host_path: auth_host_path.to_string(),
                container_path: "/gluetun/auth.txt".to_string(),
                read_only: true,
            });
        }

        let mut gateway_spec = ContainerSpec {
            name: gateway_name.clone(),
            image: image.to_string(),
            network: input.app_spec.network.clone(),
            network_mode: None,
            aliases: input.app_spec.aliases.clone(),
            env: gateway_env,
            volumes,
            ports: input.app_spec.ports.clone(),
            labels,
            command: Vec::new(),
            cap_add: vec!["NET_ADMIN".to_string()],
            cap_drop: Vec::new(),
            devices: vec!["/dev/net/tun:/dev/net/tun".to_string()],
            sysctls: HashMap::new(),
            security: Default::default(),
        };

        let mut protected_app_spec = input.app_spec.clone();
        protected_app_spec.network_mode = Some(format!("container:{gateway_name}"));
        protected_app_spec.aliases.clear();
        protected_app_spec.ports.clear();
        stamp_download_network_labels(
            &mut gateway_spec,
            profile,
            "gluetun_openvpn",
            input.app_spec,
        );
        stamp_download_network_labels(
            &mut protected_app_spec,
            profile,
            "gluetun_openvpn",
            input.app_spec,
        );

        Ok(CompiledDownloadProtectionProfile {
            gateway_spec: Some(gateway_spec),
            protected_app_spec,
        })
    }
}

impl GatewayRuntimeCompiler for CloudflareWarpGatewayRuntime {
    fn compile(
        &self,
        profile: &DownloadProtectionProfile,
        input: DownloadProtectionCompileInput<'_>,
    ) -> Result<CompiledDownloadProtectionProfile> {
        let image = self.image.trim();
        if image.is_empty() {
            bail!(
                "download protection profile '{}' has an empty WARP gateway image",
                profile.id
            );
        }
        let state_volume_name = self.state_volume_name.trim();
        if state_volume_name.is_empty() {
            bail!(
                "download protection profile '{}' has an empty WARP state volume",
                profile.id
            );
        }
        let enrollment_id = self.enrollment_id.trim();
        if enrollment_id.is_empty() {
            bail!(
                "download protection profile '{}' has an empty WARP enrollment id",
                profile.id
            );
        }
        let identity_secret_ref = self.identity_secret_ref.trim();
        if identity_secret_ref.is_empty() {
            bail!(
                "download protection profile '{}' has an empty WARP identity secret reference",
                profile.id
            );
        }
        let app_container_name = input.app_container_name.trim();
        if app_container_name.is_empty() {
            bail!(
                "download protection profile '{}' has an empty app container name",
                profile.id
            );
        }

        let gateway_name = format!("{app_container_name}-vpn");
        let mut labels = input.base_labels.clone();
        labels.insert(
            "elixir.network_role".to_string(),
            "warp_gateway".to_string(),
        );
        labels.insert("elixir.warp.profile_id".to_string(), profile.id.clone());
        labels.insert(
            "elixir.warp.enrollment_id".to_string(),
            enrollment_id.to_string(),
        );

        let mut sysctls = HashMap::new();
        sysctls.insert(
            "net.ipv6.conf.all.disable_ipv6".to_string(),
            "0".to_string(),
        );
        sysctls.insert(
            "net.ipv4.conf.all.src_valid_mark".to_string(),
            "1".to_string(),
        );
        sysctls.insert("net.ipv4.ip_forward".to_string(), "1".to_string());
        sysctls.insert("net.ipv6.conf.all.forwarding".to_string(), "1".to_string());
        sysctls.insert("net.ipv6.conf.all.accept_ra".to_string(), "2".to_string());

        let mut gateway_spec = ContainerSpec {
            name: gateway_name.clone(),
            image: image.to_string(),
            network: input.app_spec.network.clone(),
            network_mode: None,
            aliases: input.app_spec.aliases.clone(),
            env: vec![
                EnvVar {
                    name: "WARP_SLEEP".to_string(),
                    value: "2".to_string(),
                },
                EnvVar {
                    name: "WARP_ENABLE_NAT".to_string(),
                    value: "1".to_string(),
                },
                EnvVar {
                    name: "ELIXIR_WARP_ENROLLMENT_ID".to_string(),
                    value: enrollment_id.to_string(),
                },
                EnvVar {
                    name: "ELIXIR_WARP_IDENTITY_SECRET_REF".to_string(),
                    value: identity_secret_ref.to_string(),
                },
            ],
            volumes: vec![VolumeMount {
                source_kind: VolumeMountSourceKind::NamedVolume,
                host_path: state_volume_name.to_string(),
                container_path: "/var/lib/cloudflare-warp".to_string(),
                read_only: false,
            }],
            ports: input.app_spec.ports.clone(),
            labels,
            command: Vec::new(),
            cap_add: vec![
                "NET_ADMIN".to_string(),
                "MKNOD".to_string(),
                "AUDIT_WRITE".to_string(),
            ],
            cap_drop: Vec::new(),
            devices: vec!["/dev/net/tun:/dev/net/tun".to_string()],
            sysctls,
            security: Default::default(),
        };

        let mut protected_app_spec = input.app_spec.clone();
        protected_app_spec.network_mode = Some(format!("container:{gateway_name}"));
        protected_app_spec.aliases.clear();
        protected_app_spec.ports.clear();
        stamp_download_network_labels(&mut gateway_spec, profile, "warp_gateway", input.app_spec);
        stamp_download_network_labels(
            &mut protected_app_spec,
            profile,
            "warp_gateway",
            input.app_spec,
        );

        Ok(CompiledDownloadProtectionProfile {
            gateway_spec: Some(gateway_spec),
            protected_app_spec,
        })
    }
}

pub async fn observed_download_protection_status(
    settings: &Settings,
    pool: &AnyPool,
    secrets: &SecretsManager,
) -> Result<DownloadProtectionStatus> {
    observed_download_protection_status_with_evidence(settings, pool, secrets, None).await
}

pub async fn observed_download_protection_status_with_evidence(
    settings: &Settings,
    pool: &AnyPool,
    secrets: &SecretsManager,
    runtime_evidence: Option<&DownloadProtectionRuntimeEvidence>,
) -> Result<DownloadProtectionStatus> {
    let store = ExtensionStore::new(pool);
    let instances = store
        .list_instances(None)
        .await
        .context("listing extension instances for download protection status")?;
    let providers = store
        .list_providers(None)
        .await
        .context("listing providers for download protection status")?;
    let inventory = ManagedDownloaderInventory::from_store(&instances, &providers);

    if let Some(profile) = load_active_download_network_profile(pool).await? {
        return status_from_stored_profile(
            &profile,
            &inventory,
            pool,
            &store,
            secrets,
            runtime_evidence,
        )
        .await;
    }

    Ok(status_from_legacy_config(settings, &inventory, &store, secrets).await?)
}

pub async fn list_download_network_profiles(
    pool: &AnyPool,
) -> Result<DownloadProtectionProfilesResponse> {
    let profiles = list_stored_download_network_profiles(pool)
        .await?
        .into_iter()
        .map(|profile| profile.summary())
        .collect();
    Ok(DownloadProtectionProfilesResponse { profiles })
}

pub async fn apply_download_protection_first_run_choice_with_orchestrated_apply<
    ApplyFn,
    ApplyFut,
    EvidenceFn,
    EvidenceFut,
>(
    settings: &Settings,
    pool: &AnyPool,
    secrets: &SecretsManager,
    request: DownloadProtectionFirstRunRequest,
    apply_orchestrator: ApplyFn,
    collect_runtime_evidence: EvidenceFn,
) -> Result<DownloadProtectionFirstRunResponse>
where
    ApplyFn: FnMut() -> ApplyFut,
    ApplyFut: Future<Output = Result<()>>,
    EvidenceFn: FnOnce() -> EvidenceFut,
    EvidenceFut: Future<Output = Result<DownloadProtectionRuntimeEvidence>>,
{
    let mut checks = Vec::new();
    let mut notes = Vec::new();
    let mut profile = None;
    let mut switch_result = None;
    let mut blocker = None;
    let mut applied = false;
    let mut completed = false;

    match request.choice {
        DownloadProtectionFirstRunChoice::ProtectedDownloads => {
            let warp = ensure_cloudflare_warp_profile(
                pool,
                secrets,
                CloudflareWarpProfileRequest {
                    accepted_disclosure: request.accepted_warp_disclosure,
                    profile_id: None,
                    name: None,
                },
            )
            .await?;
            checks.extend(warp.checks.clone());
            notes.push(
                "Prepared the per-server Cloudflare WARP profile for managed downloader protection."
                    .to_string(),
            );

            upsert_default_download_routes(
                pool,
                DownloadBrokerBindingKind::ManagedProtected,
                Some(warp.profile.id.clone()),
            )
            .await?;
            notes.push(
                "Selected protected managed downloader routes for torrent and usenet acquisitions."
                    .to_string(),
            );

            let switched = switch_download_protection_profile_with_orchestrated_apply(
                settings,
                pool,
                secrets,
                DownloadProtectionSwitchRequest {
                    target_profile_id: warp.profile.id.clone(),
                    apply: request.apply,
                    expected_active_profile_id: None,
                    server_public_ip: None,
                    downloader_public_ip: None,
                    runtime_evidence: None,
                },
                apply_orchestrator,
                collect_runtime_evidence,
            )
            .await?;

            applied = switched.applied;
            completed = switched.applied && switched.blocker.is_none();
            profile = Some(switched.target_profile.clone());
            blocker = switched.blocker.clone();
            checks.extend(switched.checks.clone());
            switch_result = Some(switched);
        }
        DownloadProtectionFirstRunChoice::ExistingStack => {
            upsert_external_only_first_run_profile(
                pool,
                FIRST_RUN_EXTERNAL_ONLY_PROFILE_ID,
                "Existing Download Stack",
                &request.choice,
            )
            .await?;
            upsert_default_download_routes(pool, DownloadBrokerBindingKind::External, None).await?;
            notes.push(
                "Selected external downloader routes; Elixir will not manage downloader VPN for this stack."
                    .to_string(),
            );

            let switched = switch_download_protection_profile_with_orchestrated_apply(
                settings,
                pool,
                secrets,
                DownloadProtectionSwitchRequest {
                    target_profile_id: FIRST_RUN_EXTERNAL_ONLY_PROFILE_ID.to_string(),
                    apply: request.apply,
                    expected_active_profile_id: None,
                    server_public_ip: None,
                    downloader_public_ip: None,
                    runtime_evidence: None,
                },
                apply_orchestrator,
                || async {
                    anyhow::bail!(
                        "external-only first-run choice does not require protected runtime evidence"
                    )
                },
            )
            .await?;
            applied = switched.applied;
            completed = switched.applied && switched.blocker.is_none();
            profile = Some(switched.target_profile.clone());
            blocker = switched.blocker.clone();
            checks.extend(switched.checks.clone());
            switch_result = Some(switched);
        }
        DownloadProtectionFirstRunChoice::CustomVpn => {
            checks.push(check(
                "custom_vpn_profile_required",
                DownloadProtectionCheckStatus::Warn,
                DownloadProtectionSeverity::Warning,
                "Import a WireGuard or OpenVPN profile, or choose a provider preset, then switch managed downloader protection to that profile.",
            ));
            notes.push(
                "No downloader networking was changed. The next step is importing a provider profile."
                    .to_string(),
            );
        }
        DownloadProtectionFirstRunChoice::SkipDownloads => {
            upsert_external_only_first_run_profile(
                pool,
                FIRST_RUN_SKIP_DOWNLOADS_PROFILE_ID,
                "Downloads Skipped",
                &request.choice,
            )
            .await?;
            upsert_default_download_routes(pool, DownloadBrokerBindingKind::External, None).await?;
            notes.push(
                "Selected local-media-only setup by keeping managed downloader networking out of the default acquisition routes."
                    .to_string(),
            );

            let switched = switch_download_protection_profile_with_orchestrated_apply(
                settings,
                pool,
                secrets,
                DownloadProtectionSwitchRequest {
                    target_profile_id: FIRST_RUN_SKIP_DOWNLOADS_PROFILE_ID.to_string(),
                    apply: request.apply,
                    expected_active_profile_id: None,
                    server_public_ip: None,
                    downloader_public_ip: None,
                    runtime_evidence: None,
                },
                apply_orchestrator,
                || async {
                    anyhow::bail!(
                        "skip-downloads first-run choice does not require protected runtime evidence"
                    )
                },
            )
            .await?;
            applied = switched.applied;
            completed = switched.applied && switched.blocker.is_none();
            profile = Some(switched.target_profile.clone());
            blocker = switched.blocker.clone();
            checks.extend(switched.checks.clone());
            switch_result = Some(switched);
        }
    }

    let store = ExtensionStore::new(pool);
    let routes = list_acquisition_routes(pool, &store).await?;
    let response = DownloadProtectionFirstRunResponse {
        choice: request.choice,
        completed,
        applied,
        profile,
        switch_result,
        routes,
        checks,
        blocker,
        notes,
    };

    record_download_network_event(
        pool,
        response.profile.as_ref().map(|profile| profile.id.as_str()),
        "first_run_setup",
        if response.completed {
            "completed"
        } else if response.blocker.is_some() {
            "blocked"
        } else {
            "prepared"
        },
        &serde_json::json!({
            "choice": response.choice,
            "completed": response.completed,
            "applied": response.applied,
            "profile": response.profile,
            "checks": response.checks,
            "blocker": response.blocker,
            "notes": response.notes,
        }),
    )
    .await?;

    Ok(response)
}

struct SwitchEvaluation {
    operation_id: Uuid,
    previous_profile: DownloadProtectionProfileSummary,
    previous_stored_profile_id: Option<String>,
    target: StoredDownloadNetworkProfile,
    checks: Vec<DownloadProtectionCheck>,
    phases: Vec<DownloadProtectionSwitchPhase>,
    blocker: Option<DownloadProtectionBlocker>,
}

async fn evaluate_download_protection_switch(
    settings: &Settings,
    pool: &AnyPool,
    secrets: &SecretsManager,
    request: &DownloadProtectionSwitchRequest,
) -> Result<SwitchEvaluation> {
    let target_profile_id = request.target_profile_id.trim();
    if target_profile_id.is_empty() {
        bail!("targetProfileId is required");
    }

    let store = ExtensionStore::new(pool);
    let instances = store
        .list_instances(None)
        .await
        .context("listing extension instances for download protection switch")?;
    let providers = store
        .list_providers(None)
        .await
        .context("listing providers for download protection switch")?;
    let inventory = ManagedDownloaderInventory::from_store(&instances, &providers);
    let previous_stored_profile_id = load_active_download_network_profile(pool)
        .await?
        .map(|profile| profile.id);
    let previous_status = observed_download_protection_status(settings, pool, secrets).await?;
    let target = load_download_network_profile(pool, target_profile_id)
        .await?
        .ok_or_else(|| anyhow!("download network profile '{target_profile_id}' not found"))?;

    let mut checks = Vec::new();
    let mut phases = Vec::new();
    let mut blocker = None;

    push_profile_validation(
        &mut checks,
        &mut phases,
        &mut blocker,
        &target,
        request.expected_active_profile_id.as_deref(),
        &previous_status.active_profile.id,
    );

    if blocker.is_none() {
        push_profile_runtime_validation(
            &mut checks,
            &mut phases,
            &mut blocker,
            &target,
            &inventory,
            pool,
            &store,
            secrets,
        )
        .await?;
    }

    if blocker.is_none() {
        let evidence = request.runtime_evidence.as_ref();
        let evidence_server_public_ip = evidence
            .and_then(|evidence| successful_evidence_value(evidence.server_public_ip.as_ref()));
        let evidence_downloader_public_ip = evidence
            .and_then(|evidence| successful_evidence_value(evidence.downloader_public_ip.as_ref()));
        push_leak_gate(
            &mut checks,
            &mut phases,
            &mut blocker,
            &target,
            request
                .server_public_ip
                .as_deref()
                .or(evidence_server_public_ip.as_deref()),
            request
                .downloader_public_ip
                .as_deref()
                .or(evidence_downloader_public_ip.as_deref()),
        );
    }

    if blocker.is_none() {
        push_runtime_evidence_switch_gate(
            &mut checks,
            &mut phases,
            &mut blocker,
            &target,
            request.runtime_evidence.as_ref(),
        );
    }

    Ok(SwitchEvaluation {
        operation_id: Uuid::new_v4(),
        previous_profile: previous_status.active_profile,
        previous_stored_profile_id,
        target,
        checks,
        phases,
        blocker,
    })
}

pub async fn active_managed_downloader_runtime(
    pool: &AnyPool,
) -> Result<ActiveManagedDownloaderRuntime> {
    let Some(profile) = load_active_download_network_profile(pool).await? else {
        return Ok(ActiveManagedDownloaderRuntime::NoStoredProfile);
    };
    if !profile.enabled {
        bail!(
            "active download network profile '{}' is disabled",
            profile.id
        );
    }

    match profile.kind {
        DownloadNetworkProfileKind::ExternalOnly
        | DownloadNetworkProfileKind::Direct
        | DownloadNetworkProfileKind::DebridOnly => Ok(ActiveManagedDownloaderRuntime::Direct),
        DownloadNetworkProfileKind::WireguardConfig => {
            let secret_ref = load_profile_secret_ref(pool, &profile.id, "wireguard_config")
                .await?
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    anyhow!(
                        "active WireGuard download profile '{}' is missing wireguard_config secret reference",
                        profile.id
                    )
                })?;
            Ok(ActiveManagedDownloaderRuntime::WireguardConfig {
                profile_id: profile.id,
                secret_ref,
                gateway_image: wireguard_gateway_image_from_config(&profile.config_json),
            })
        }
        DownloadNetworkProfileKind::OpenvpnConfig => {
            let config_secret_ref = load_profile_secret_ref(pool, &profile.id, "openvpn_config")
                .await?
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    anyhow!(
                        "active OpenVPN download profile '{}' is missing openvpn_config secret reference",
                        profile.id
                    )
                })?;
            let username_secret_ref =
                load_profile_secret_ref(pool, &profile.id, "openvpn_username").await?;
            let password_secret_ref =
                load_profile_secret_ref(pool, &profile.id, "openvpn_password").await?;
            Ok(ActiveManagedDownloaderRuntime::OpenvpnConfig {
                profile_id: profile.id.clone(),
                config_secret_ref,
                username_secret_ref,
                password_secret_ref,
                gateway_image: openvpn_gateway_image_from_config(&profile.config_json),
            })
        }
        DownloadNetworkProfileKind::CloudflareWarp => {
            let enrollment = load_cloudflare_warp_enrollment(pool, &profile.id)
                .await?
                .ok_or_else(|| {
                    anyhow!(
                        "active Cloudflare WARP download profile '{}' is missing its per-server enrollment",
                        profile.id
                    )
                })?;
            Ok(ActiveManagedDownloaderRuntime::CloudflareWarp {
                profile_id: profile.id.clone(),
                enrollment_id: enrollment.enrollment_id,
                identity_secret_ref: enrollment.identity_secret_ref,
                gateway_image: warp_gateway_image_from_config(&profile.config_json),
                state_volume_name: warp_state_volume_name(&profile.id),
            })
        }
        DownloadNetworkProfileKind::ProviderPreset => {
            Ok(ActiveManagedDownloaderRuntime::UnsupportedProtected {
                profile_id: profile.id,
                kind: profile.kind,
            })
        }
    }
}

pub(crate) async fn active_download_network_profile_identity(
    pool: &AnyPool,
) -> Result<Option<(String, DownloadNetworkProfileKind)>> {
    Ok(load_active_download_network_profile(pool)
        .await?
        .map(|profile| (profile.id, profile.kind)))
}

#[allow(dead_code)]
pub async fn switch_download_protection_profile(
    settings: &Settings,
    pool: &AnyPool,
    secrets: &SecretsManager,
    request: DownloadProtectionSwitchRequest,
) -> Result<DownloadProtectionSwitchResponse> {
    let mut evaluation =
        evaluate_download_protection_switch(settings, pool, secrets, &request).await?;

    let can_apply_without_orchestrated_runtime =
        can_activate_without_orchestrated_runtime(evaluation.target.kind.clone());
    let switch_requires_orchestrated_runtime = profile_switch_requires_orchestrated_runtime(
        &evaluation.previous_profile.kind,
        &evaluation.target.kind,
    );

    if request.apply && evaluation.blocker.is_none() && switch_requires_orchestrated_runtime {
        let apply_blocker = DownloadProtectionBlocker {
            code: "profile_switch_orchestrator_apply_not_enabled".to_string(),
            title: "Download profile switch requires orchestrator".to_string(),
            detail: "The target profile passed static gates, but this switch changes managed downloader runtime topology and must flow through the deterministic orchestrator. The old profile remains active."
                .to_string(),
            severity: DownloadProtectionSeverity::Critical,
        };
        evaluation.phases.push(phase(
            "rehome_protected_apps",
            DownloadProtectionSwitchPhaseStatus::Blocked,
            &apply_blocker.detail,
            Some(apply_blocker.clone()),
        ));
        evaluation.blocker = Some(apply_blocker);
    }

    complete_remaining_switch_phases(
        &mut evaluation.phases,
        request.apply,
        switch_requires_orchestrated_runtime,
        evaluation.blocker.is_none(),
    );

    let ready_to_apply = evaluation.blocker.is_none()
        && !switch_requires_orchestrated_runtime
        && can_apply_without_orchestrated_runtime;
    let applied = request.apply && ready_to_apply;
    if applied {
        activate_download_network_profile(pool, &evaluation.target).await?;
        apply_default_routes_for_profile_switch(pool, &evaluation.target).await?;
    }

    let status = if applied {
        DownloadProtectionSwitchStatus::Applied
    } else if evaluation.blocker.is_some() {
        DownloadProtectionSwitchStatus::Blocked
    } else {
        DownloadProtectionSwitchStatus::PreflightPassed
    };

    record_download_network_event(
        pool,
        Some(evaluation.target.id.as_str()),
        if request.apply {
            "switch"
        } else {
            "switch_preflight"
        },
        switch_status_as_str(status.clone()),
        &serde_json::json!({
            "operationId": evaluation.operation_id,
            "previousProfileId": evaluation.previous_profile.id,
            "targetProfileId": evaluation.target.id,
            "checks": evaluation.checks,
            "phases": evaluation.phases,
            "blocker": evaluation.blocker,
            "applied": applied,
            "orchestrated": switch_requires_orchestrated_runtime
        }),
    )
    .await?;

    Ok(DownloadProtectionSwitchResponse {
        operation_id: evaluation.operation_id,
        status,
        apply_requested: request.apply,
        ready_to_apply,
        applied,
        previous_profile: evaluation.previous_profile,
        target_profile: evaluation.target.summary(),
        checks: evaluation.checks,
        phases: evaluation.phases,
        runtime_evidence: None,
        blocker: evaluation.blocker,
    })
}

pub async fn switch_download_protection_profile_with_orchestrated_apply<
    ApplyFn,
    ApplyFut,
    EvidenceFn,
    EvidenceFut,
>(
    settings: &Settings,
    pool: &AnyPool,
    secrets: &SecretsManager,
    request: DownloadProtectionSwitchRequest,
    mut apply_orchestrator: ApplyFn,
    collect_runtime_evidence: EvidenceFn,
) -> Result<DownloadProtectionSwitchResponse>
where
    ApplyFn: FnMut() -> ApplyFut,
    ApplyFut: Future<Output = Result<()>>,
    EvidenceFn: FnOnce() -> EvidenceFut,
    EvidenceFut: Future<Output = Result<DownloadProtectionRuntimeEvidence>>,
{
    // Current runtime evidence describes the old namespace. Protected switches
    // must verify the target namespace after the deterministic apply path.
    let evaluation_request = request.without_pre_apply_runtime_evidence();
    let mut evaluation =
        evaluate_download_protection_switch(settings, pool, secrets, &evaluation_request).await?;
    let target_requires_protected_egress =
        profile_requires_protected_egress(&evaluation.target.kind);
    let switch_requires_orchestrated_runtime = profile_switch_requires_orchestrated_runtime(
        &evaluation.previous_profile.kind,
        &evaluation.target.kind,
    );
    let can_apply_without_orchestrated_runtime =
        can_activate_without_orchestrated_runtime(evaluation.target.kind.clone());

    let mut applied = false;
    let mut runtime_evidence = None;
    let mut rollback_applied = false;
    let mut rollback_error = None;
    if request.apply && evaluation.blocker.is_none() {
        if switch_requires_orchestrated_runtime {
            activate_download_network_profile(pool, &evaluation.target).await?;
            match apply_orchestrator().await {
                Ok(()) => {
                    mark_orchestrated_switch_rehome_applied(&mut evaluation.phases);
                    if target_requires_protected_egress {
                        match collect_runtime_evidence().await {
                            Ok(evidence) => {
                                push_post_apply_runtime_evidence_gate(
                                    &mut evaluation.checks,
                                    &mut evaluation.phases,
                                    &mut evaluation.blocker,
                                    &evaluation.target,
                                    &evidence,
                                );
                                runtime_evidence = Some(evidence);
                                if evaluation.blocker.is_none() {
                                    mark_download_network_profile_verified(
                                        pool,
                                        &evaluation.target,
                                    )
                                    .await?;
                                    apply_default_routes_for_profile_switch(
                                        pool,
                                        &evaluation.target,
                                    )
                                    .await?;
                                    mark_orchestrated_switch_phases_verified(
                                        &mut evaluation.phases,
                                        &evaluation.target,
                                    );
                                    applied = true;
                                } else {
                                    mark_download_network_profile_blocked(
                                        pool,
                                        &evaluation.target.id,
                                    )
                                    .await?;
                                    let (rolled_back, err) = rollback_orchestrated_switch(
                                        pool,
                                        evaluation.previous_stored_profile_id.as_deref(),
                                        &mut apply_orchestrator,
                                    )
                                    .await;
                                    rollback_applied = rolled_back;
                                    rollback_error = err;
                                    mark_post_apply_rollback_phases(
                                        &mut evaluation.phases,
                                        evaluation.blocker.as_ref(),
                                        rollback_applied,
                                        rollback_error.as_deref(),
                                    );
                                    if let Some(err) = rollback_error.as_deref() {
                                        evaluation.blocker = Some(rollback_failed_blocker(err));
                                    }
                                }
                            }
                            Err(err) => {
                                let detail = format!(
                                    "Runtime evidence collection failed after protected downloader rehome. The previous active profile will be restored. {err}"
                                );
                                let verify_blocker = DownloadProtectionBlocker {
                                    code: "profile_switch_post_apply_verification_failed"
                                        .to_string(),
                                    title: "Protected profile verification failed".to_string(),
                                    detail: detail.clone(),
                                    severity: DownloadProtectionSeverity::Critical,
                                };
                                evaluation.checks.push(check(
                                    "post_apply_runtime_evidence",
                                    DownloadProtectionCheckStatus::Fail,
                                    DownloadProtectionSeverity::Critical,
                                    &detail,
                                ));
                                set_or_push_phase(
                                    &mut evaluation.phases,
                                    "verify_protected_apps",
                                    DownloadProtectionSwitchPhaseStatus::Blocked,
                                    &detail,
                                    Some(verify_blocker.clone()),
                                );
                                mark_download_network_profile_blocked(pool, &evaluation.target.id)
                                    .await?;
                                let (rolled_back, err) = rollback_orchestrated_switch(
                                    pool,
                                    evaluation.previous_stored_profile_id.as_deref(),
                                    &mut apply_orchestrator,
                                )
                                .await;
                                rollback_applied = rolled_back;
                                rollback_error = err;
                                mark_post_apply_rollback_phases(
                                    &mut evaluation.phases,
                                    Some(&verify_blocker),
                                    rollback_applied,
                                    rollback_error.as_deref(),
                                );
                                evaluation.blocker = if let Some(err) = rollback_error.as_deref() {
                                    Some(rollback_failed_blocker(err))
                                } else {
                                    Some(verify_blocker)
                                };
                            }
                        }
                    } else {
                        mark_download_network_profile_verified(pool, &evaluation.target).await?;
                        apply_default_routes_for_profile_switch(pool, &evaluation.target).await?;
                        mark_orchestrated_switch_phases_verified(
                            &mut evaluation.phases,
                            &evaluation.target,
                        );
                        applied = true;
                    }
                }
                Err(err) => {
                    let _ =
                        mark_download_network_profile_blocked(pool, &evaluation.target.id).await;
                    let (rolled_back, rollback_err) = rollback_orchestrated_switch(
                        pool,
                        evaluation.previous_stored_profile_id.as_deref(),
                        &mut apply_orchestrator,
                    )
                    .await;
                    rollback_applied = rolled_back;
                    rollback_error = rollback_err;
                    let apply_blocker = DownloadProtectionBlocker {
                        code: "profile_switch_orchestrator_apply_failed".to_string(),
                        title: "Download profile switch failed".to_string(),
                        detail: format!(
                            "The deterministic orchestrator failed while applying downloader networking. The previous active profile was restored. {}",
                            format_error_chain(&err)
                        ),
                        severity: DownloadProtectionSeverity::Critical,
                    };
                    evaluation.checks.push(check(
                        "orchestrator_apply",
                        DownloadProtectionCheckStatus::Fail,
                        DownloadProtectionSeverity::Critical,
                        &apply_blocker.detail,
                    ));
                    set_or_push_phase(
                        &mut evaluation.phases,
                        "rehome_protected_apps",
                        DownloadProtectionSwitchPhaseStatus::Fail,
                        &apply_blocker.detail,
                        Some(apply_blocker.clone()),
                    );
                    mark_post_apply_rollback_phases(
                        &mut evaluation.phases,
                        Some(&apply_blocker),
                        rollback_applied,
                        rollback_error.as_deref(),
                    );
                    evaluation.blocker = if let Some(err) = rollback_error.as_deref() {
                        Some(rollback_failed_blocker(err))
                    } else {
                        Some(apply_blocker)
                    };
                }
            }
        } else if can_apply_without_orchestrated_runtime {
            activate_download_network_profile(pool, &evaluation.target).await?;
            apply_default_routes_for_profile_switch(pool, &evaluation.target).await?;
            applied = true;
        }
    }

    complete_remaining_switch_phases(
        &mut evaluation.phases,
        request.apply,
        switch_requires_orchestrated_runtime,
        evaluation.blocker.is_none(),
    );

    let ready_to_apply = evaluation.blocker.is_none()
        && (switch_requires_orchestrated_runtime || can_apply_without_orchestrated_runtime);
    let status = if applied {
        DownloadProtectionSwitchStatus::Applied
    } else if evaluation.blocker.is_some() {
        DownloadProtectionSwitchStatus::Blocked
    } else {
        DownloadProtectionSwitchStatus::PreflightPassed
    };

    record_download_network_event(
        pool,
        Some(evaluation.target.id.as_str()),
        if request.apply {
            "switch"
        } else {
            "switch_preflight"
        },
        switch_status_as_str(status.clone()),
        &serde_json::json!({
            "operationId": evaluation.operation_id,
            "previousProfileId": evaluation.previous_profile.id,
            "targetProfileId": evaluation.target.id,
            "checks": evaluation.checks,
            "phases": evaluation.phases,
            "blocker": evaluation.blocker,
            "applied": applied,
            "orchestrated": switch_requires_orchestrated_runtime,
            "runtimeEvidence": runtime_evidence,
            "rollbackApplied": rollback_applied,
            "rollbackError": rollback_error
        }),
    )
    .await?;

    Ok(DownloadProtectionSwitchResponse {
        operation_id: evaluation.operation_id,
        status,
        apply_requested: request.apply,
        ready_to_apply,
        applied,
        previous_profile: evaluation.previous_profile,
        target_profile: evaluation.target.summary(),
        checks: evaluation.checks,
        phases: evaluation.phases,
        runtime_evidence,
        blocker: evaluation.blocker,
    })
}

pub fn cloudflare_warp_disclosure() -> CloudflareWarpDisclosure {
    CloudflareWarpDisclosure {
        version: CLOUDFLARE_WARP_DISCLOSURE_VERSION.to_string(),
        title: "Cloudflare WARP downloader protection".to_string(),
        body: "Cloudflare WARP is a free best-effort privacy path for managed downloader traffic. It is not an Elixir-owned VPN service, and Elixir creates a per-server identity instead of shipping shared credentials.".to_string(),
        limitations: vec![
            "WARP does not provide torrent port forwarding.".to_string(),
            "Cloudflare may change WARP availability, behavior, or policy.".to_string(),
            "Cloudflare can observe connection metadata according to its policies.".to_string(),
            "Performance depends on local routing and Cloudflare capacity.".to_string(),
            "Protected downloads must stay blocked whenever WARP health or leak checks fail.".to_string(),
        ],
        required_acceptance: "I understand that WARP is a Cloudflare-powered best-effort downloader protection mode, not an Elixir VPN service, and that protected downloads must be blocked if WARP is unavailable.".to_string(),
    }
}

pub fn download_provider_preset_catalog() -> DownloadProviderPresetCatalog {
    DownloadProviderPresetCatalog {
        presets: vec![
            provider_preset(
                "cloudflare-warp",
                "Cloudflare WARP",
                "cloudflare",
                vec![DownloadNetworkProfileKind::CloudflareWarp],
                vec!["warp_gateway"],
                vec!["one_click_enrollment"],
                DownloadProviderPortForwarding::Unsupported,
                vec![
                    "Free best-effort downloader egress privacy.",
                    "Does not provide torrent port forwarding.",
                ],
            ),
            provider_preset(
                "custom-wireguard",
                "Custom WireGuard",
                "custom",
                vec![DownloadNetworkProfileKind::WireguardConfig],
                vec!["gluetun_wireguard"],
                vec!["paste_conf", "upload_conf"],
                DownloadProviderPortForwarding::Manual,
                vec![
                    "Use this for providers that expose a standard WireGuard configuration.",
                    "Forwarded ports must be supplied by the provider and observed before qBittorrent sync.",
                ],
            ),
            provider_preset(
                "custom-openvpn",
                "Custom OpenVPN",
                "custom",
                vec![DownloadNetworkProfileKind::OpenvpnConfig],
                vec!["gluetun_openvpn"],
                vec!["upload_ovpn", "username_password"],
                DownloadProviderPortForwarding::Manual,
                vec![
                    "Use this for providers that expose OpenVPN profiles.",
                    "Username/password credentials are stored as encrypted profile secrets when required.",
                ],
            ),
            provider_preset(
                "airvpn",
                "AirVPN",
                "airvpn",
                vec![
                    DownloadNetworkProfileKind::WireguardConfig,
                    DownloadNetworkProfileKind::OpenvpnConfig,
                    DownloadNetworkProfileKind::ProviderPreset,
                ],
                vec!["gluetun_wireguard", "gluetun_openvpn"],
                vec!["provider_preset", "wireguard_conf", "openvpn_conf"],
                DownloadProviderPortForwarding::ProviderApi,
                vec!["Known to support provider-managed forwarded ports."],
            ),
            provider_preset(
                "pia",
                "Private Internet Access",
                "pia",
                vec![
                    DownloadNetworkProfileKind::OpenvpnConfig,
                    DownloadNetworkProfileKind::ProviderPreset,
                ],
                vec!["gluetun_openvpn"],
                vec!["provider_preset", "openvpn_conf"],
                DownloadProviderPortForwarding::ProviderApi,
                vec!["Forwarded port support depends on selected gateway region."],
            ),
            provider_preset(
                "proton",
                "Proton VPN",
                "proton",
                vec![
                    DownloadNetworkProfileKind::WireguardConfig,
                    DownloadNetworkProfileKind::OpenvpnConfig,
                    DownloadNetworkProfileKind::ProviderPreset,
                ],
                vec!["gluetun_wireguard", "gluetun_openvpn"],
                vec!["provider_preset", "wireguard_conf", "openvpn_conf"],
                DownloadProviderPortForwarding::ProviderApi,
                vec!["Forwarded port behavior must be verified before qBittorrent sync."],
            ),
            provider_preset(
                "mullvad",
                "Mullvad",
                "mullvad",
                vec![
                    DownloadNetworkProfileKind::WireguardConfig,
                    DownloadNetworkProfileKind::OpenvpnConfig,
                    DownloadNetworkProfileKind::ProviderPreset,
                ],
                vec!["gluetun_wireguard", "gluetun_openvpn"],
                vec!["provider_preset", "wireguard_conf", "openvpn_conf"],
                DownloadProviderPortForwarding::Unsupported,
                vec!["Current preset treats inbound torrent port forwarding as unavailable."],
            ),
            provider_preset(
                "nord",
                "NordVPN",
                "nordvpn",
                vec![
                    DownloadNetworkProfileKind::OpenvpnConfig,
                    DownloadNetworkProfileKind::ProviderPreset,
                ],
                vec!["gluetun_openvpn"],
                vec!["provider_preset", "openvpn_conf"],
                DownloadProviderPortForwarding::Unsupported,
                vec!["Use for privacy protection; no forwarded torrent port is modeled."],
            ),
            provider_preset(
                "surfshark",
                "Surfshark",
                "surfshark",
                vec![
                    DownloadNetworkProfileKind::OpenvpnConfig,
                    DownloadNetworkProfileKind::ProviderPreset,
                ],
                vec!["gluetun_openvpn"],
                vec!["provider_preset", "openvpn_conf"],
                DownloadProviderPortForwarding::Unsupported,
                vec!["Use for privacy protection; no forwarded torrent port is modeled."],
            ),
        ],
    }
}

pub async fn import_wireguard_profile(
    pool: &AnyPool,
    secrets: &SecretsManager,
    request: DownloadNetworkProfileImportRequest,
) -> Result<DownloadNetworkProfileImportResponse> {
    import_config_profile(
        pool,
        secrets,
        DownloadNetworkProfileKind::WireguardConfig,
        "Imported WireGuard",
        "wireguard_config",
        request,
    )
    .await
}

pub async fn import_openvpn_profile(
    pool: &AnyPool,
    secrets: &SecretsManager,
    request: DownloadNetworkProfileImportRequest,
) -> Result<DownloadNetworkProfileImportResponse> {
    import_config_profile(
        pool,
        secrets,
        DownloadNetworkProfileKind::OpenvpnConfig,
        "Imported OpenVPN",
        "openvpn_config",
        request,
    )
    .await
}

async fn import_config_profile(
    pool: &AnyPool,
    secrets: &SecretsManager,
    kind: DownloadNetworkProfileKind,
    default_name: &str,
    config_secret_key: &str,
    request: DownloadNetworkProfileImportRequest,
) -> Result<DownloadNetworkProfileImportResponse> {
    let name = request
        .name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(default_name)
        .to_string();
    let profile_id = request
        .profile_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(sanitize_profile_id)
        .unwrap_or_else(|| {
            let prefix = match kind {
                DownloadNetworkProfileKind::WireguardConfig => "imported-wireguard",
                DownloadNetworkProfileKind::OpenvpnConfig => "imported-openvpn",
                _ => "imported-profile",
            };
            format!("{prefix}-{}", Uuid::new_v4().simple())
        });
    let config = request.config.trim();
    if config.is_empty() {
        bail!("imported VPN config must not be empty");
    }
    if !request.strict {
        bail!("imported protected downloader profiles must use strict mode");
    }

    let mut checks = match kind {
        DownloadNetworkProfileKind::WireguardConfig => validate_wireguard_config_text(config),
        DownloadNetworkProfileKind::OpenvpnConfig => validate_openvpn_config_text(
            config,
            request.username.as_deref(),
            request.password.as_deref(),
        ),
        _ => Vec::new(),
    };
    let blocker = first_import_blocker(&checks);
    let config_secret_name = profile_import_secret_key(&profile_id, config_secret_key);
    let config_secret_ref = format!("global:{config_secret_name}");

    let store = ExtensionStore::new(pool);
    store
        .upsert_secret(&NewSecret {
            secret_id: Uuid::new_v4(),
            scope: SecretScope::Global,
            scope_id: None,
            key: config_secret_name,
            value_encrypted: secrets.encrypt(config)?,
            rotatable: true,
        })
        .await
        .context("storing imported VPN config secret")?;

    let mut config_json = serde_json::json!({
        "importMethod": "paste",
        "secretRef": config_secret_ref,
    });
    if let Some(image) = request
        .gateway_image
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        config_json["gatewayImage"] = serde_json::Value::String(image.to_string());
        match kind {
            DownloadNetworkProfileKind::WireguardConfig => {
                config_json["wireguardGatewayImage"] = serde_json::Value::String(image.to_string());
            }
            DownloadNetworkProfileKind::OpenvpnConfig => {
                config_json["openvpnGatewayImage"] = serde_json::Value::String(image.to_string());
            }
            _ => {}
        }
    }
    if let Some(forwarded_port) = request.forwarded_port.as_ref() {
        config_json["forwardedPort"] = serde_json::to_value(forwarded_port)?;
    }

    let gateway_runtime = match kind {
        DownloadNetworkProfileKind::WireguardConfig => "gluetun_wireguard",
        DownloadNetworkProfileKind::OpenvpnConfig => "gluetun_openvpn",
        _ => "",
    };
    upsert_imported_download_profile(
        pool,
        &profile_id,
        &name,
        &kind,
        request.strict,
        request.provider.as_deref(),
        gateway_runtime,
        &config_json,
        if blocker.is_some() {
            DownloadProtectionState::Blocked
        } else {
            DownloadProtectionState::Unknown
        },
    )
    .await?;
    upsert_profile_secret_ref(pool, &profile_id, config_secret_key, &config_secret_ref).await?;

    if kind == DownloadNetworkProfileKind::OpenvpnConfig {
        let username = request
            .username
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let password = request
            .password
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        if let (Some(username), Some(password)) = (username, password) {
            let username_key = profile_import_secret_key(&profile_id, "openvpn_username");
            let password_key = profile_import_secret_key(&profile_id, "openvpn_password");
            store
                .upsert_secret(&NewSecret {
                    secret_id: Uuid::new_v4(),
                    scope: SecretScope::Global,
                    scope_id: None,
                    key: username_key.clone(),
                    value_encrypted: secrets.encrypt(username)?,
                    rotatable: true,
                })
                .await
                .context("storing imported OpenVPN username")?;
            store
                .upsert_secret(&NewSecret {
                    secret_id: Uuid::new_v4(),
                    scope: SecretScope::Global,
                    scope_id: None,
                    key: password_key.clone(),
                    value_encrypted: secrets.encrypt(password)?,
                    rotatable: true,
                })
                .await
                .context("storing imported OpenVPN password")?;
            upsert_profile_secret_ref(
                pool,
                &profile_id,
                "openvpn_username",
                &format!("global:{username_key}"),
            )
            .await?;
            upsert_profile_secret_ref(
                pool,
                &profile_id,
                "openvpn_password",
                &format!("global:{password_key}"),
            )
            .await?;
            checks.push(check(
                "openvpn_auth_credentials",
                DownloadProtectionCheckStatus::Pass,
                DownloadProtectionSeverity::Info,
                "OpenVPN username/password credentials were stored as encrypted profile secrets.",
            ));
        }
    }

    record_download_network_event(
        pool,
        Some(&profile_id),
        match kind {
            DownloadNetworkProfileKind::WireguardConfig => "wireguard_profile_import",
            DownloadNetworkProfileKind::OpenvpnConfig => "openvpn_profile_import",
            _ => "download_profile_import",
        },
        if blocker.is_some() {
            "blocked"
        } else {
            "ready"
        },
        &serde_json::json!({
            "profileId": profile_id,
            "kind": profile_kind_as_str(&kind),
            "provider": request.provider,
            "checks": &checks,
            "blocker": &blocker,
        }),
    )
    .await?;

    let profile = load_download_network_profile(pool, &profile_id)
        .await?
        .ok_or_else(|| anyhow!("imported download network profile was not readable"))?;
    Ok(DownloadNetworkProfileImportResponse {
        profile: profile.summary(),
        checks,
        blocker,
    })
}

pub async fn qbittorrent_listen_port_sync_plan(
    settings: &Settings,
    pool: &AnyPool,
    secrets: &SecretsManager,
) -> Result<QbittorrentListenPortSyncPlan> {
    let status = observed_download_protection_status(settings, pool, secrets).await?;
    if !status.managed_downloaders.qbittorrent {
        return Ok(QbittorrentListenPortSyncPlan {
            status: QbittorrentListenPortSyncStatus::NoManagedQbittorrent,
            target_provider_id: None,
            target_instance_id: None,
            target_port: None,
            capability: None,
            patch: None,
            requires_orchestrator: false,
            detail: "No Elixir-managed qBittorrent instance is available for listen-port sync."
                .to_string(),
        });
    }
    if let Some(blocker) = status.blocker {
        return Ok(QbittorrentListenPortSyncPlan {
            status: QbittorrentListenPortSyncStatus::Blocked,
            target_provider_id: None,
            target_instance_id: None,
            target_port: None,
            capability: None,
            patch: None,
            requires_orchestrator: true,
            detail: format!(
                "qBittorrent listen-port sync is blocked until download protection clears '{}'.",
                blocker.code
            ),
        });
    }
    let Some(forwarded_port) = status.torrent_reachability.forwarded_port else {
        return Ok(QbittorrentListenPortSyncPlan {
            status: QbittorrentListenPortSyncStatus::NotApplicable,
            target_provider_id: None,
            target_instance_id: None,
            target_port: None,
            capability: None,
            patch: None,
            requires_orchestrator: false,
            detail: status.torrent_reachability.detail,
        });
    };

    let store = ExtensionStore::new(pool);
    let instances = store.list_instances(None).await?;
    let providers = store.list_providers(None).await?;
    let Some(target_provider) = select_managed_qbittorrent_provider(&instances, &providers) else {
        return Ok(QbittorrentListenPortSyncPlan {
            status: QbittorrentListenPortSyncStatus::Blocked,
            target_provider_id: None,
            target_instance_id: None,
            target_port: Some(forwarded_port.port),
            capability: Some("downloader.torrent".to_string()),
            patch: None,
            requires_orchestrator: true,
            detail: "qBittorrent listen-port sync requires an Elixir-managed downloader.torrent provider; run extension repair/bootstrap before applying the forwarded port.".to_string(),
        });
    };

    let patch = DownloaderTorrentPatch::SetPreferences {
        default_save_path: None,
        incomplete_path: None,
        use_incomplete: None,
        max_connections: None,
        max_connections_per_torrent: None,
        max_upload_slots: None,
        max_upload_slots_per_torrent: None,
        disk_cache_mb: None,
        disk_cache_ttl_seconds: None,
        queueing_enabled: None,
        max_active_downloads: None,
        max_active_torrents: None,
        max_active_uploads: None,
        random_port: Some(false),
        listen_port: Some(forwarded_port.port),
        upnp: Some(false),
        preallocate_all: None,
    };

    Ok(QbittorrentListenPortSyncPlan {
        status: QbittorrentListenPortSyncStatus::Ready,
        target_provider_id: Some(target_provider.provider_id),
        target_instance_id: Some(target_provider.instance_id),
        target_port: Some(forwarded_port.port),
        capability: Some("downloader.torrent".to_string()),
        patch: Some(serde_json::to_value(patch)?),
        requires_orchestrator: true,
        detail: format!(
            "qBittorrent should use forwarded {} port {}. The sync must be applied through the deterministic driver patch path.",
            forwarded_port.protocol, forwarded_port.port
        ),
    })
}

fn provider_preset(
    id: &str,
    name: &str,
    provider: &str,
    profile_kinds: Vec<DownloadNetworkProfileKind>,
    gateway_runtimes: Vec<&str>,
    import_methods: Vec<&str>,
    port_forwarding: DownloadProviderPortForwarding,
    notes: Vec<&str>,
) -> DownloadProviderPreset {
    DownloadProviderPreset {
        id: id.to_string(),
        name: name.to_string(),
        provider: provider.to_string(),
        profile_kinds,
        gateway_runtimes: gateway_runtimes.into_iter().map(str::to_string).collect(),
        import_methods: import_methods.into_iter().map(str::to_string).collect(),
        port_forwarding,
        notes: notes.into_iter().map(str::to_string).collect(),
    }
}

fn select_managed_qbittorrent_provider<'a>(
    instances: &[ExtensionInstance],
    providers: &'a [Provider],
) -> Option<&'a Provider> {
    let enabled_instances = instances
        .iter()
        .filter(|instance| instance.enabled)
        .map(|instance| (instance.instance_id, instance.extension_id.as_str()))
        .collect::<HashMap<_, _>>();

    let mut candidates = providers
        .iter()
        .filter(|provider| {
            if provider.capability != "downloader.torrent" {
                return false;
            }
            let Some(extension_id) = enabled_instances.get(&provider.instance_id) else {
                return false;
            };
            if !is_qbittorrent_extension_id(extension_id) {
                return false;
            }
            provider
                .implementation
                .as_deref()
                .map_or(true, |implementation| {
                    implementation.eq_ignore_ascii_case("qbittorrent")
                })
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        left.instance_id
            .cmp(&right.instance_id)
            .then(left.provider_id.cmp(&right.provider_id))
    });
    candidates.into_iter().next()
}

pub async fn ensure_cloudflare_warp_profile(
    pool: &AnyPool,
    secrets: &SecretsManager,
    request: CloudflareWarpProfileRequest,
) -> Result<CloudflareWarpProfileResponse> {
    if !request.accepted_disclosure {
        bail!("Cloudflare WARP disclosure must be accepted before creating a WARP profile");
    }

    let profile_id = request
        .profile_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(CLOUDFLARE_WARP_PROFILE_ID);
    let name = request
        .name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("Cloudflare WARP");

    upsert_cloudflare_warp_profile(pool, profile_id, name).await?;
    let enrollment = ensure_cloudflare_warp_enrollment(pool, secrets, profile_id).await?;
    let profile = load_download_network_profile(pool, profile_id)
        .await?
        .ok_or_else(|| {
            anyhow!("created Cloudflare WARP profile '{profile_id}' was not readable")
        })?;
    let (checks, blocker) = warp_profile_checks(&profile, Some(&enrollment));

    record_download_network_event(
        pool,
        Some(profile_id),
        "warp_profile_prepare",
        if blocker.is_some() {
            "blocked"
        } else {
            "prepared"
        },
        &serde_json::json!({
            "profileId": profile_id,
            "enrollmentId": enrollment.enrollment_id,
            "disclosureVersion": CLOUDFLARE_WARP_DISCLOSURE_VERSION,
            "checks": checks,
            "blocker": blocker
        }),
    )
    .await?;

    Ok(CloudflareWarpProfileResponse {
        profile: profile.summary(),
        enrollment: enrollment.status_response(),
        disclosure: cloudflare_warp_disclosure(),
        checks,
        blocker,
    })
}

async fn status_from_legacy_config(
    settings: &Settings,
    inventory: &ManagedDownloaderInventory,
    store: &ExtensionStore<'_>,
    secrets: &SecretsManager,
) -> Result<DownloadProtectionStatus> {
    let profile = legacy_profile_from_config(settings, inventory);
    let managed_downloaders = managed_downloader_presence(inventory);
    let mut checks = vec![managed_downloaders_check(inventory)];
    checks.push(check(
        "legacy_profile_projection",
        DownloadProtectionCheckStatus::Pass,
        DownloadProtectionSeverity::Info,
        "Legacy network.vpn settings are projected into the download protection profile model for read-only status.",
    ));
    if let Some(blocker) = profile_validation_blocker(&profile) {
        checks.push(check(
            "active_profile_valid",
            DownloadProtectionCheckStatus::Fail,
            DownloadProtectionSeverity::Critical,
            &blocker.detail,
        ));
        let torrent_reachability = torrent_reachability_for_profile(&profile, inventory);
        checks.push(torrent_reachability_check(&torrent_reachability));
        return Ok(DownloadProtectionStatus {
            mode: mode_for_profile_kind(&profile.kind),
            state: DownloadProtectionState::Blocked,
            strict: profile.strict,
            protected_apps: protected_apps_for_profile(&profile, inventory),
            server_public_ip: None,
            downloader_public_ip: None,
            gateway: None,
            torrent_reachability,
            managed_downloaders,
            active_profile: profile.summary_with_status(DownloadProtectionState::Blocked),
            checks,
            blocker: Some(blocker),
        });
    }
    checks.push(profile_validation_pass_check());

    let vpn = &settings.network.vpn;
    let legacy_protection_requested = profile.kind == DownloadNetworkProfileKind::WireguardConfig;

    if !legacy_protection_requested {
        if vpn.enabled {
            checks.push(check(
                "legacy_vpn_enabled_without_protected_apps",
                DownloadProtectionCheckStatus::Warn,
                DownloadProtectionSeverity::Warning,
                "Legacy network.vpn.enabled is true, but downloader auto-wrap settings select no protected apps.",
            ));
        }
        return Ok(direct_or_external_status(
            &profile,
            managed_downloaders,
            checks,
            inventory,
        ));
    }

    let protected_apps = protected_apps_from_legacy_config(vpn, inventory);
    checks.push(check(
        "protected_apps_selected",
        if protected_apps.is_empty() {
            DownloadProtectionCheckStatus::Warn
        } else {
            DownloadProtectionCheckStatus::Pass
        },
        if protected_apps.is_empty() {
            DownloadProtectionSeverity::Warning
        } else {
            DownloadProtectionSeverity::Info
        },
        if protected_apps.is_empty() {
            "Legacy VPN wrapping is enabled, but no managed downloader instances currently match the protected app selection."
        } else {
            "Legacy VPN wrapping selects managed downloader apps for protected egress."
        },
    ));

    let secret_check = check_wireguard_secret(
        store,
        secrets,
        &vpn.wireguard_config_secret,
        inventory.protected_instance_ids(vpn.auto_wrap_qbittorrent, vpn.auto_wrap_nzbget),
    )
    .await?;
    let mut blocker = secret_blocker(&secret_check);
    checks.push(secret_status_check(
        &secret_check,
        &vpn.wireguard_config_secret,
    ));

    if vpn.wireguard_gateway_image.trim().is_empty() {
        blocker = Some(DownloadProtectionBlocker {
            code: "wireguard_gateway_image_missing".to_string(),
            title: "WireGuard gateway image is missing".to_string(),
            detail: "Legacy VPN wrapping is enabled, but no WireGuard gateway image is configured."
                .to_string(),
            severity: DownloadProtectionSeverity::Critical,
        });
        checks.push(check(
            "wireguard_gateway_image_configured",
            DownloadProtectionCheckStatus::Fail,
            DownloadProtectionSeverity::Critical,
            "No WireGuard gateway image is configured.",
        ));
    } else {
        checks.push(check(
            "wireguard_gateway_image_configured",
            DownloadProtectionCheckStatus::Pass,
            DownloadProtectionSeverity::Info,
            "A WireGuard gateway image is configured.",
        ));
    }

    checks.push(check(
        "gateway_runtime_support",
        DownloadProtectionCheckStatus::Unknown,
        DownloadProtectionSeverity::Info,
        "Slice 1 is read-only; gateway startup, /dev/net/tun, routing, DNS, and leak checks have not been run.",
    ));

    let state = if blocker.is_some() {
        DownloadProtectionState::Blocked
    } else {
        DownloadProtectionState::Unknown
    };

    let torrent_reachability = legacy_wireguard_torrent_reachability();
    checks.push(torrent_reachability_check(&torrent_reachability));

    Ok(DownloadProtectionStatus {
        mode: mode_for_profile_kind(&profile.kind),
        state: state.clone(),
        strict: profile.strict,
        protected_apps,
        server_public_ip: None,
        downloader_public_ip: None,
        gateway: None,
        torrent_reachability,
        managed_downloaders,
        active_profile: profile.summary_with_status(state),
        checks,
        blocker,
    })
}

fn validate_download_network_profile_parts(
    id: &str,
    name: &str,
    kind: &DownloadNetworkProfileKind,
    strict: bool,
    scope: &str,
    config_json: &serde_json::Value,
) -> Result<()> {
    if id.trim().is_empty() {
        bail!("download network profile id is required");
    }
    if name.trim().is_empty() {
        bail!("download network profile name is required");
    }
    let scope = scope.trim();
    if !VALID_DOWNLOAD_PROFILE_SCOPES.contains(&scope) {
        bail!(
            "download network profile '{}' has unsupported scope '{}'",
            id,
            scope
        );
    }
    if profile_requires_protected_egress(kind) && !strict {
        bail!(
            "download network profile '{}' requires strict mode for protected downloader egress",
            id
        );
    }
    if !config_json.is_object() {
        bail!(
            "download network profile '{}' config_json must be a JSON object",
            id
        );
    }
    Ok(())
}

fn sanitize_profile_id(raw: &str) -> String {
    let mut id = String::with_capacity(raw.len());
    for ch in raw.trim().chars() {
        if ch.is_ascii_alphanumeric() {
            id.push(ch.to_ascii_lowercase());
        } else if ch == '-' || ch == '_' || ch == '.' {
            id.push(ch);
        } else if ch.is_whitespace() {
            id.push('-');
        }
    }
    let id = id
        .trim_matches(|ch| ch == '-' || ch == '_' || ch == '.')
        .to_string();
    if id.is_empty() {
        format!("profile-{}", Uuid::new_v4().simple())
    } else {
        id
    }
}

fn profile_import_secret_key(profile_id: &str, key: &str) -> String {
    let profile_id = sanitize_profile_id(profile_id)
        .replace('.', "_")
        .replace('-', "_");
    let key = sanitize_profile_id(key).replace('.', "_").replace('-', "_");
    format!("download_profile_{}_{}", profile_id, key)
}

fn validate_wireguard_config_text(config: &str) -> Vec<DownloadProtectionCheck> {
    let lower = config.to_ascii_lowercase();
    let mut checks = Vec::new();
    push_import_required_check(
        &mut checks,
        "wireguard_interface_section",
        lower.contains("[interface]"),
        "WireGuard config includes an [Interface] section.",
        "WireGuard config must include an [Interface] section.",
    );
    push_import_required_check(
        &mut checks,
        "wireguard_private_key",
        lower.contains("privatekey"),
        "WireGuard config includes a private key.",
        "WireGuard config must include PrivateKey in the [Interface] section.",
    );
    push_import_required_check(
        &mut checks,
        "wireguard_address",
        lower.contains("address"),
        "WireGuard config includes an interface address.",
        "WireGuard config must include Address in the [Interface] section.",
    );
    push_import_required_check(
        &mut checks,
        "wireguard_peer_section",
        lower.contains("[peer]"),
        "WireGuard config includes a [Peer] section.",
        "WireGuard config must include a [Peer] section.",
    );
    push_import_required_check(
        &mut checks,
        "wireguard_public_key",
        lower.contains("publickey"),
        "WireGuard config includes a peer public key.",
        "WireGuard config must include PublicKey in the [Peer] section.",
    );
    push_import_required_check(
        &mut checks,
        "wireguard_endpoint",
        lower.contains("endpoint"),
        "WireGuard config includes a peer endpoint.",
        "WireGuard config must include Endpoint in the [Peer] section.",
    );
    let routes_default =
        lower.contains("allowedips") && (lower.contains("0.0.0.0/0") || lower.contains("::/0"));
    checks.push(check(
        "wireguard_default_route",
        if routes_default {
            DownloadProtectionCheckStatus::Pass
        } else {
            DownloadProtectionCheckStatus::Warn
        },
        if routes_default {
            DownloadProtectionSeverity::Info
        } else {
            DownloadProtectionSeverity::Warning
        },
        if routes_default {
            "WireGuard AllowedIPs includes a default route for protected downloader egress."
        } else {
            "WireGuard AllowedIPs does not include a default route; import is allowed, but leak checks must pass before activation."
        },
    ));
    checks.push(check(
        "wireguard_dns",
        if lower.contains("dns") {
            DownloadProtectionCheckStatus::Pass
        } else {
            DownloadProtectionCheckStatus::Warn
        },
        if lower.contains("dns") {
            DownloadProtectionSeverity::Info
        } else {
            DownloadProtectionSeverity::Warning
        },
        if lower.contains("dns") {
            "WireGuard config includes DNS settings."
        } else {
            "WireGuard config has no DNS setting; gateway defaults will be used where possible."
        },
    ));
    let has_hook = ["preup", "postup", "predown", "postdown"]
        .iter()
        .any(|needle| lower.contains(needle));
    checks.push(check(
        "wireguard_hooks",
        if has_hook {
            DownloadProtectionCheckStatus::Fail
        } else {
            DownloadProtectionCheckStatus::Pass
        },
        if has_hook {
            DownloadProtectionSeverity::Critical
        } else {
            DownloadProtectionSeverity::Info
        },
        if has_hook {
            "WireGuard configs with PreUp/PostUp/PreDown/PostDown commands are not imported by Elixir."
        } else {
            "WireGuard config does not contain lifecycle hook commands."
        },
    ));
    checks
}

fn validate_openvpn_config_text(
    config: &str,
    username: Option<&str>,
    password: Option<&str>,
) -> Vec<DownloadProtectionCheck> {
    let lower = config.to_ascii_lowercase();
    let mut checks = Vec::new();
    push_import_required_check(
        &mut checks,
        "openvpn_client_mode",
        lower.lines().any(|line| line.trim() == "client"),
        "OpenVPN config declares client mode.",
        "OpenVPN config must include client mode.",
    );
    push_import_required_check(
        &mut checks,
        "openvpn_remote",
        lower
            .lines()
            .any(|line| line.trim_start().starts_with("remote ")),
        "OpenVPN config includes a remote endpoint.",
        "OpenVPN config must include at least one remote endpoint.",
    );
    push_import_required_check(
        &mut checks,
        "openvpn_device",
        lower
            .lines()
            .any(|line| line.trim_start().starts_with("dev ")),
        "OpenVPN config declares a tunnel device.",
        "OpenVPN config must include a dev directive.",
    );
    let uses_auth_file = lower
        .lines()
        .any(|line| line.trim_start().starts_with("auth-user-pass"));
    let has_credentials = username
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some()
        && password
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_some();
    checks.push(check(
        "openvpn_auth_credentials",
        if !uses_auth_file || has_credentials {
            DownloadProtectionCheckStatus::Pass
        } else {
            DownloadProtectionCheckStatus::Fail
        },
        if !uses_auth_file || has_credentials {
            DownloadProtectionSeverity::Info
        } else {
            DownloadProtectionSeverity::Critical
        },
        if !uses_auth_file {
            "OpenVPN config does not require a separate auth-user-pass file."
        } else if has_credentials {
            "OpenVPN username/password credentials were supplied for auth-user-pass."
        } else {
            "OpenVPN config uses auth-user-pass; provide username and password before import."
        },
    ));
    checks
}

fn push_import_required_check(
    checks: &mut Vec<DownloadProtectionCheck>,
    code: &str,
    ok: bool,
    pass_detail: &str,
    fail_detail: &str,
) {
    checks.push(check(
        code,
        if ok {
            DownloadProtectionCheckStatus::Pass
        } else {
            DownloadProtectionCheckStatus::Fail
        },
        if ok {
            DownloadProtectionSeverity::Info
        } else {
            DownloadProtectionSeverity::Critical
        },
        if ok { pass_detail } else { fail_detail },
    ));
}

fn first_import_blocker(checks: &[DownloadProtectionCheck]) -> Option<DownloadProtectionBlocker> {
    checks
        .iter()
        .find(|check| check.status == DownloadProtectionCheckStatus::Fail)
        .map(|check| DownloadProtectionBlocker {
            code: format!("{}_failed", check.code),
            title: "Imported VPN config is invalid".to_string(),
            detail: check.detail.clone(),
            severity: DownloadProtectionSeverity::Critical,
        })
}

fn profile_validation_blocker(
    profile: &StoredDownloadNetworkProfile,
) -> Option<DownloadProtectionBlocker> {
    profile
        .validate()
        .err()
        .map(|err| DownloadProtectionBlocker {
            code: "download_network_profile_invalid".to_string(),
            title: "Download network profile is invalid".to_string(),
            detail: err.to_string(),
            severity: DownloadProtectionSeverity::Critical,
        })
}

fn profile_validation_pass_check() -> DownloadProtectionCheck {
    check(
        "active_profile_valid",
        DownloadProtectionCheckStatus::Pass,
        DownloadProtectionSeverity::Info,
        "The download network profile passed static validation.",
    )
}

fn legacy_profile_from_config(
    settings: &Settings,
    inventory: &ManagedDownloaderInventory,
) -> StoredDownloadNetworkProfile {
    let vpn = &settings.network.vpn;
    if vpn.enabled && (vpn.auto_wrap_qbittorrent || vpn.auto_wrap_nzbget) {
        return StoredDownloadNetworkProfile {
            id: "legacy-wireguard".to_string(),
            name: "Legacy WireGuard".to_string(),
            kind: DownloadNetworkProfileKind::WireguardConfig,
            enabled: true,
            strict: true,
            scope: "managed_downloaders".to_string(),
            provider: Some("custom".to_string()),
            gateway_runtime: Some("gluetun_wireguard".to_string()),
            config_json: serde_json::json!({
                "legacy": {
                    "source": "network.vpn",
                    "autoWrapQbittorrent": vpn.auto_wrap_qbittorrent,
                    "autoWrapNzbget": vpn.auto_wrap_nzbget,
                    "wireguardConfigSecret": vpn.wireguard_config_secret,
                    "wireguardGatewayImage": vpn.wireguard_gateway_image
                }
            }),
            status: DownloadProtectionState::Unknown,
        };
    }

    if inventory.has_managed() {
        StoredDownloadNetworkProfile {
            id: "legacy-direct".to_string(),
            name: "Legacy Direct".to_string(),
            kind: DownloadNetworkProfileKind::Direct,
            enabled: true,
            strict: false,
            scope: "managed_downloaders".to_string(),
            provider: None,
            gateway_runtime: None,
            config_json: serde_json::json!({
                "legacy": {
                    "source": "network.vpn",
                    "vpnEnabled": vpn.enabled
                }
            }),
            status: DownloadProtectionState::Direct,
        }
    } else {
        StoredDownloadNetworkProfile {
            id: "legacy-external-only".to_string(),
            name: "External Only".to_string(),
            kind: DownloadNetworkProfileKind::ExternalOnly,
            enabled: true,
            strict: false,
            scope: "managed_downloaders".to_string(),
            provider: None,
            gateway_runtime: None,
            config_json: serde_json::json!({
                "legacy": {
                    "source": "network.vpn",
                    "vpnEnabled": vpn.enabled
                }
            }),
            status: if inventory.external_count > 0 {
                DownloadProtectionState::ExternallyManaged
            } else {
                DownloadProtectionState::Unknown
            },
        }
    }
}

fn direct_or_external_status(
    profile: &StoredDownloadNetworkProfile,
    managed_downloaders: ManagedDownloaderPresence,
    mut checks: Vec<DownloadProtectionCheck>,
    inventory: &ManagedDownloaderInventory,
) -> DownloadProtectionStatus {
    let has_managed = inventory.has_managed();
    let mode = mode_for_profile_kind(&profile.kind);
    let state = match profile.kind {
        DownloadNetworkProfileKind::ExternalOnly | DownloadNetworkProfileKind::DebridOnly => {
            if inventory.external_count > 0 {
                DownloadProtectionState::ExternallyManaged
            } else {
                DownloadProtectionState::Unknown
            }
        }
        DownloadNetworkProfileKind::Direct => DownloadProtectionState::Direct,
        _ => DownloadProtectionState::Unknown,
    };
    let torrent_reachability =
        legacy_torrent_reachability(&mode, has_managed, inventory.external_count);
    checks.push(torrent_reachability_check(&torrent_reachability));

    DownloadProtectionStatus {
        mode: mode.clone(),
        state: state.clone(),
        strict: false,
        protected_apps: Vec::new(),
        server_public_ip: None,
        downloader_public_ip: None,
        gateway: None,
        torrent_reachability,
        managed_downloaders,
        active_profile: profile.summary_with_status(state),
        checks,
        blocker: None,
    }
}

#[allow(clippy::too_many_arguments)]
fn download_protection_status(
    mode: DownloadProtectionMode,
    state: DownloadProtectionState,
    strict: bool,
    protected_apps: Vec<String>,
    torrent_reachability: DownloadTorrentReachability,
    managed_downloaders: ManagedDownloaderPresence,
    active_profile: DownloadProtectionProfileSummary,
    checks: Vec<DownloadProtectionCheck>,
    blocker: Option<DownloadProtectionBlocker>,
    profile: &StoredDownloadNetworkProfile,
    runtime_evidence: Option<&DownloadProtectionRuntimeEvidence>,
) -> DownloadProtectionStatus {
    let (server_public_ip, downloader_public_ip, gateway) =
        runtime_evidence_summary(profile, runtime_evidence);
    DownloadProtectionStatus {
        mode,
        state,
        strict,
        protected_apps,
        server_public_ip,
        downloader_public_ip,
        gateway,
        torrent_reachability,
        managed_downloaders,
        active_profile,
        checks,
        blocker,
    }
}

fn runtime_evidence_summary(
    profile: &StoredDownloadNetworkProfile,
    evidence: Option<&DownloadProtectionRuntimeEvidence>,
) -> (
    Option<String>,
    Option<String>,
    Option<DownloadProtectionGatewayStatus>,
) {
    let server_public_ip =
        evidence.and_then(|evidence| successful_evidence_value(evidence.server_public_ip.as_ref()));
    let downloader_public_ip = evidence
        .and_then(|evidence| successful_evidence_value(evidence.downloader_public_ip.as_ref()));

    let gateway = if profile_requires_protected_egress(&profile.kind) {
        let gateway_public_ip = evidence
            .and_then(|evidence| successful_evidence_value(evidence.gateway_public_ip.as_ref()));
        let last_checked_at = evidence.and_then(runtime_evidence_last_checked_at);
        let state = match evidence {
            Some(evidence)
                if evidence_status_failed(evidence.gateway_public_ip.as_ref())
                    || evidence_status_failed(evidence.gateway_dns.as_ref()) =>
            {
                "degraded"
            }
            Some(evidence)
                if evidence_status_passed(evidence.gateway_public_ip.as_ref())
                    && evidence_status_passed(evidence.gateway_dns.as_ref()) =>
            {
                "healthy"
            }
            Some(_) => "unknown",
            None => "unknown",
        };
        Some(DownloadProtectionGatewayStatus {
            runtime: profile.gateway_runtime.clone(),
            state: state.to_string(),
            public_ip: gateway_public_ip,
            last_checked_at,
        })
    } else {
        None
    };

    (server_public_ip, downloader_public_ip, gateway)
}

fn runtime_evidence_status_checks(
    profile: &StoredDownloadNetworkProfile,
    evidence: Option<&DownloadProtectionRuntimeEvidence>,
) -> (
    Vec<DownloadProtectionCheck>,
    Option<DownloadProtectionBlocker>,
) {
    let Some(evidence) = evidence else {
        return if profile_requires_protected_egress(&profile.kind) {
            (
                vec![check(
                    "leak_check",
                    DownloadProtectionCheckStatus::Unknown,
                    DownloadProtectionSeverity::Warning,
                    "No fresh downloader public-IP evidence is attached to this status request.",
                )],
                None,
            )
        } else {
            (Vec::new(), None)
        };
    };

    let mut checks = Vec::new();
    let mut blocker = None;
    checks.push(evidence_check(
        "server_public_ip",
        evidence.server_public_ip.as_ref(),
        DownloadProtectionCheckStatus::Unknown,
        DownloadProtectionSeverity::Warning,
        "Server public-IP evidence is unavailable.",
    ));

    if !profile_requires_protected_egress(&profile.kind) {
        return (checks, None);
    }

    checks.push(evidence_check(
        "gateway_public_ip",
        evidence.gateway_public_ip.as_ref(),
        DownloadProtectionCheckStatus::Fail,
        DownloadProtectionSeverity::Critical,
        "Gateway public-IP evidence is unavailable for the active protected profile.",
    ));
    checks.push(evidence_check(
        "downloader_public_ip",
        evidence.downloader_public_ip.as_ref(),
        DownloadProtectionCheckStatus::Fail,
        DownloadProtectionSeverity::Critical,
        "Downloader public-IP evidence is unavailable for the active protected profile.",
    ));
    checks.push(evidence_check(
        "gateway_dns",
        evidence.gateway_dns.as_ref(),
        DownloadProtectionCheckStatus::Fail,
        DownloadProtectionSeverity::Critical,
        "Gateway DNS evidence is unavailable for the active protected profile.",
    ));
    checks.push(evidence_check(
        "downloader_dns",
        evidence.downloader_dns.as_ref(),
        DownloadProtectionCheckStatus::Fail,
        DownloadProtectionSeverity::Critical,
        "Downloader DNS evidence is unavailable for the active protected profile.",
    ));
    checks.push(evidence_check(
        "kill_switch",
        evidence.kill_switch.as_ref(),
        DownloadProtectionCheckStatus::Fail,
        DownloadProtectionSeverity::Critical,
        "Kill-switch evidence is unavailable for the active protected profile.",
    ));

    if blocker.is_none() {
        blocker = first_runtime_evidence_blocker(evidence);
    }

    let server = successful_evidence_value(evidence.server_public_ip.as_ref());
    let downloader = successful_evidence_value(evidence.downloader_public_ip.as_ref());
    match (server, downloader) {
        (Some(server), Some(downloader)) if server == downloader => {
            let next = DownloadProtectionBlocker {
                code: "download_network_leak_detected".to_string(),
                title: "Downloader traffic is leaking".to_string(),
                detail: format!(
                    "The downloader public IP ({downloader}) matches the server public IP ({server}). Protected downloads remain blocked."
                ),
                severity: DownloadProtectionSeverity::Critical,
            };
            checks.push(check(
                "downloader_ip_differs_from_server",
                DownloadProtectionCheckStatus::Fail,
                DownloadProtectionSeverity::Critical,
                &next.detail,
            ));
            if blocker.is_none() {
                blocker = Some(next);
            }
        }
        (Some(server), Some(downloader)) => checks.push(check(
            "downloader_ip_differs_from_server",
            DownloadProtectionCheckStatus::Pass,
            DownloadProtectionSeverity::Info,
            &format!(
                "Downloader public IP ({downloader}) differs from server public IP ({server})."
            ),
        )),
        _ => {
            let next = DownloadProtectionBlocker {
                code: "download_network_leak_check_missing".to_string(),
                title: "Leak check evidence is missing".to_string(),
                detail: "Protected mode requires server and downloader public-IP evidence."
                    .to_string(),
                severity: DownloadProtectionSeverity::Critical,
            };
            checks.push(check(
                "downloader_ip_differs_from_server",
                DownloadProtectionCheckStatus::Fail,
                DownloadProtectionSeverity::Critical,
                &next.detail,
            ));
            if blocker.is_none() {
                blocker = Some(next);
            }
        }
    }

    (checks, blocker)
}

fn evidence_check(
    code: &str,
    evidence: Option<&DownloadProtectionProbeEvidence>,
    missing_status: DownloadProtectionCheckStatus,
    missing_severity: DownloadProtectionSeverity,
    missing_detail: &str,
) -> DownloadProtectionCheck {
    let Some(evidence) = evidence else {
        return check(code, missing_status, missing_severity, missing_detail);
    };
    let severity = match evidence.status {
        DownloadProtectionCheckStatus::Pass => DownloadProtectionSeverity::Info,
        DownloadProtectionCheckStatus::Warn | DownloadProtectionCheckStatus::Unknown => {
            DownloadProtectionSeverity::Warning
        }
        DownloadProtectionCheckStatus::Fail => DownloadProtectionSeverity::Critical,
    };
    check(code, evidence.status.clone(), severity, &evidence.detail)
}

fn first_runtime_evidence_blocker(
    evidence: &DownloadProtectionRuntimeEvidence,
) -> Option<DownloadProtectionBlocker> {
    for (probe, code, title) in [
        (
            evidence.gateway_public_ip.as_ref(),
            "gateway_public_ip_unavailable",
            "Gateway public IP is unavailable",
        ),
        (
            evidence.downloader_public_ip.as_ref(),
            "downloader_public_ip_unavailable",
            "Downloader public IP is unavailable",
        ),
        (
            evidence.gateway_dns.as_ref(),
            "gateway_dns_failed",
            "Gateway DNS check failed",
        ),
        (
            evidence.downloader_dns.as_ref(),
            "downloader_dns_failed",
            "Downloader DNS check failed",
        ),
        (
            evidence.kill_switch.as_ref(),
            "download_network_kill_switch_failed",
            "Downloader kill switch check failed",
        ),
    ] {
        if evidence_status_failed(probe) || probe.is_none() {
            return Some(DownloadProtectionBlocker {
                code: code.to_string(),
                title: title.to_string(),
                detail: probe
                    .map(|probe| probe.detail.clone())
                    .unwrap_or_else(|| "Required runtime evidence is missing.".to_string()),
                severity: DownloadProtectionSeverity::Critical,
            });
        }
    }
    None
}

fn runtime_evidence_last_checked_at(
    evidence: &DownloadProtectionRuntimeEvidence,
) -> Option<DateTime<Utc>> {
    [
        evidence.server_public_ip.as_ref(),
        evidence.gateway_public_ip.as_ref(),
        evidence.downloader_public_ip.as_ref(),
        evidence.gateway_dns.as_ref(),
        evidence.downloader_dns.as_ref(),
        evidence.kill_switch.as_ref(),
    ]
    .into_iter()
    .flatten()
    .map(|probe| probe.checked_at)
    .max()
}

fn successful_evidence_value(evidence: Option<&DownloadProtectionProbeEvidence>) -> Option<String> {
    evidence.and_then(|evidence| {
        if evidence.status == DownloadProtectionCheckStatus::Pass {
            evidence
                .value
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        } else {
            None
        }
    })
}

fn evidence_status_passed(evidence: Option<&DownloadProtectionProbeEvidence>) -> bool {
    evidence.is_some_and(|evidence| evidence.status == DownloadProtectionCheckStatus::Pass)
}

fn evidence_status_failed(evidence: Option<&DownloadProtectionProbeEvidence>) -> bool {
    evidence.is_some_and(|evidence| evidence.status == DownloadProtectionCheckStatus::Fail)
}

async fn status_from_stored_profile(
    profile: &StoredDownloadNetworkProfile,
    inventory: &ManagedDownloaderInventory,
    pool: &AnyPool,
    store: &ExtensionStore<'_>,
    secrets: &SecretsManager,
    runtime_evidence: Option<&DownloadProtectionRuntimeEvidence>,
) -> Result<DownloadProtectionStatus> {
    let mut checks = vec![managed_downloaders_check(inventory)];
    checks.push(check(
        "active_profile_source",
        DownloadProtectionCheckStatus::Pass,
        DownloadProtectionSeverity::Info,
        "Download protection status is sourced from the active download network profile.",
    ));
    let managed_downloaders = managed_downloader_presence(inventory);

    if let Some(blocker) = profile_validation_blocker(profile) {
        checks.push(check(
            "active_profile_valid",
            DownloadProtectionCheckStatus::Fail,
            DownloadProtectionSeverity::Critical,
            &blocker.detail,
        ));
        let torrent_reachability = torrent_reachability_for_profile(profile, inventory);
        checks.push(torrent_reachability_check(&torrent_reachability));
        return Ok(download_protection_status(
            mode_for_profile_kind(&profile.kind),
            DownloadProtectionState::Blocked,
            profile.strict,
            protected_apps_for_profile(profile, inventory),
            torrent_reachability,
            managed_downloaders,
            profile.summary_with_status(DownloadProtectionState::Blocked),
            checks,
            Some(blocker),
            profile,
            runtime_evidence,
        ));
    }
    checks.push(profile_validation_pass_check());

    if !profile.enabled {
        let blocker = DownloadProtectionBlocker {
            code: "active_profile_disabled".to_string(),
            title: "Active download profile is disabled".to_string(),
            detail: format!(
                "Download network profile '{}' is marked active but disabled.",
                profile.id
            ),
            severity: DownloadProtectionSeverity::Critical,
        };
        checks.push(check(
            "active_profile_enabled",
            DownloadProtectionCheckStatus::Fail,
            DownloadProtectionSeverity::Critical,
            &blocker.detail,
        ));
        let torrent_reachability = torrent_reachability_for_profile(profile, inventory);
        checks.push(torrent_reachability_check(&torrent_reachability));
        return Ok(download_protection_status(
            mode_for_profile_kind(&profile.kind),
            DownloadProtectionState::Blocked,
            profile.strict,
            protected_apps_for_profile(profile, inventory),
            torrent_reachability,
            managed_downloaders,
            profile.summary_with_status(DownloadProtectionState::Blocked),
            checks,
            Some(blocker),
            profile,
            runtime_evidence,
        ));
    }

    if !profile_requires_protected_egress(&profile.kind) {
        let state = match profile.kind {
            DownloadNetworkProfileKind::ExternalOnly | DownloadNetworkProfileKind::DebridOnly => {
                if inventory.external_count > 0 {
                    DownloadProtectionState::ExternallyManaged
                } else {
                    DownloadProtectionState::Unknown
                }
            }
            DownloadNetworkProfileKind::Direct => DownloadProtectionState::Direct,
            _ => DownloadProtectionState::Unknown,
        };
        let torrent_reachability = torrent_reachability_for_profile(profile, inventory);
        checks.push(torrent_reachability_check(&torrent_reachability));
        return Ok(download_protection_status(
            mode_for_profile_kind(&profile.kind),
            state.clone(),
            profile.strict,
            Vec::new(),
            torrent_reachability,
            managed_downloaders,
            profile.summary_with_status(state),
            checks,
            None,
            profile,
            runtime_evidence,
        ));
    }

    let protected_apps = protected_apps_for_profile(profile, inventory);
    checks.push(check(
        "protected_apps_selected",
        if protected_apps.is_empty() {
            DownloadProtectionCheckStatus::Fail
        } else {
            DownloadProtectionCheckStatus::Pass
        },
        if protected_apps.is_empty() {
            DownloadProtectionSeverity::Critical
        } else {
            DownloadProtectionSeverity::Info
        },
        if protected_apps.is_empty() {
            "The active protected profile has no enabled managed downloader apps to protect."
        } else {
            "The active protected profile selects managed downloader apps for protected egress."
        },
    ));

    let mut blocker = if protected_apps.is_empty() {
        Some(DownloadProtectionBlocker {
            code: "protected_downloaders_missing".to_string(),
            title: "No managed downloaders are available".to_string(),
            detail: "The active profile requires protected managed downloaders, but no enabled qBittorrent or NZBGet instances were found."
                .to_string(),
            severity: DownloadProtectionSeverity::Critical,
        })
    } else {
        None
    };

    if profile.kind == DownloadNetworkProfileKind::WireguardConfig {
        let secret_ref = load_profile_secret_ref(pool, &profile.id, "wireguard_config")
            .await?
            .unwrap_or_default();
        let secret_check = if secret_ref.trim().is_empty() {
            SecretCheck::InvalidRef(
                "wireguard_config profile secret reference is missing".to_string(),
            )
        } else {
            check_wireguard_secret(
                store,
                secrets,
                &secret_ref,
                inventory.protected_instance_ids(true, true),
            )
            .await?
        };
        if blocker.is_none() {
            blocker = secret_blocker(&secret_check);
        }
        checks.push(secret_status_check(&secret_check, &secret_ref));
    }

    if profile.kind == DownloadNetworkProfileKind::OpenvpnConfig {
        let secret_ref = load_profile_secret_ref(pool, &profile.id, "openvpn_config")
            .await?
            .unwrap_or_default();
        let secret_check = if secret_ref.trim().is_empty() {
            SecretCheck::InvalidRef(
                "openvpn_config profile secret reference is missing".to_string(),
            )
        } else {
            check_profile_secret(store, secrets, &secret_ref).await?
        };
        if blocker.is_none() {
            blocker =
                profile_secret_blocker(&secret_check, "openvpn_config_secret", "OpenVPN config");
        }
        checks.push(profile_secret_status_check(
            &secret_check,
            &secret_ref,
            "openvpn_config_secret",
            "OpenVPN config",
        ));

        let username_ref = load_profile_secret_ref(pool, &profile.id, "openvpn_username").await?;
        let password_ref = load_profile_secret_ref(pool, &profile.id, "openvpn_password").await?;
        if username_ref.is_some() != password_ref.is_some() {
            let auth_blocker = DownloadProtectionBlocker {
                code: "openvpn_auth_incomplete".to_string(),
                title: "OpenVPN credentials are incomplete".to_string(),
                detail: "The OpenVPN profile has only one of username or password configured."
                    .to_string(),
                severity: DownloadProtectionSeverity::Critical,
            };
            checks.push(check(
                "openvpn_auth_credentials",
                DownloadProtectionCheckStatus::Fail,
                DownloadProtectionSeverity::Critical,
                &auth_blocker.detail,
            ));
            if blocker.is_none() {
                blocker = Some(auth_blocker);
            }
        } else if username_ref.is_some() {
            checks.push(check(
                "openvpn_auth_credentials",
                DownloadProtectionCheckStatus::Pass,
                DownloadProtectionSeverity::Info,
                "OpenVPN username/password credentials are stored as profile secrets.",
            ));
        } else {
            checks.push(check(
                "openvpn_auth_credentials",
                DownloadProtectionCheckStatus::Warn,
                DownloadProtectionSeverity::Warning,
                "No separate OpenVPN username/password credentials are stored for this profile.",
            ));
        }
    }

    if profile.kind == DownloadNetworkProfileKind::CloudflareWarp {
        let enrollment = load_cloudflare_warp_enrollment(pool, &profile.id).await?;
        let (warp_checks, warp_blocker) = warp_profile_checks(profile, enrollment.as_ref());
        checks.extend(warp_checks);
        if blocker.is_none() {
            blocker = warp_blocker;
        }
    }

    let runtime_check = gateway_runtime_check(profile);
    let runtime_failed = matches!(&runtime_check.status, DownloadProtectionCheckStatus::Fail);
    checks.push(runtime_check);
    if blocker.is_none() && runtime_failed {
        blocker = Some(DownloadProtectionBlocker {
            code: "gateway_runtime_unsupported".to_string(),
            title: "Gateway runtime is not supported".to_string(),
            detail: "The active profile requests a gateway runtime this server cannot execute yet."
                .to_string(),
            severity: DownloadProtectionSeverity::Critical,
        });
    }

    let (evidence_checks, evidence_blocker) =
        runtime_evidence_status_checks(profile, runtime_evidence);
    checks.extend(evidence_checks);
    if blocker.is_none() {
        blocker = evidence_blocker;
    }
    let torrent_reachability = torrent_reachability_for_profile(profile, inventory);
    checks.push(torrent_reachability_check(&torrent_reachability));

    let state = if blocker.is_some() {
        DownloadProtectionState::Blocked
    } else {
        profile.status.clone()
    };

    Ok(download_protection_status(
        mode_for_profile_kind(&profile.kind),
        state.clone(),
        profile.strict,
        protected_apps,
        torrent_reachability,
        managed_downloaders,
        profile.summary_with_status(state),
        checks,
        blocker,
        profile,
        runtime_evidence,
    ))
}

fn managed_downloader_presence(
    inventory: &ManagedDownloaderInventory,
) -> ManagedDownloaderPresence {
    ManagedDownloaderPresence {
        qbittorrent: inventory.has_qbittorrent(),
        nzbget: inventory.has_nzbget(),
        external_count: inventory.external_count,
    }
}

fn managed_downloaders_check(inventory: &ManagedDownloaderInventory) -> DownloadProtectionCheck {
    check(
        "managed_downloaders_detected",
        if inventory.has_managed() {
            DownloadProtectionCheckStatus::Pass
        } else {
            DownloadProtectionCheckStatus::Warn
        },
        if inventory.has_managed() {
            DownloadProtectionSeverity::Info
        } else {
            DownloadProtectionSeverity::Warning
        },
        if inventory.has_managed() {
            "Elixir-managed downloader instances are present."
        } else if inventory.external_count > 0 {
            "Downloader providers exist, but none are Elixir-managed qBittorrent or NZBGet."
        } else {
            "No Elixir-managed qBittorrent or NZBGet instances were found."
        },
    )
}

fn legacy_wireguard_torrent_reachability() -> DownloadTorrentReachability {
    DownloadTorrentReachability {
        state: DownloadTorrentReachabilityState::Unknown,
        can_accept_inbound: false,
        listen_port: None,
        forwarded_port: None,
        detail:
            "Legacy WireGuard configuration does not expose an observed forwarded torrent port yet."
                .to_string(),
    }
}

fn legacy_torrent_reachability(
    mode: &DownloadProtectionMode,
    has_managed_downloaders: bool,
    external_count: usize,
) -> DownloadTorrentReachability {
    match mode {
        DownloadProtectionMode::Direct if has_managed_downloaders => DownloadTorrentReachability {
            state: DownloadTorrentReachabilityState::Unknown,
            can_accept_inbound: false,
            listen_port: None,
            forwarded_port: None,
            detail: "Direct managed downloader mode does not include Elixir-observed torrent port forwarding."
                .to_string(),
        },
        DownloadProtectionMode::ExternalOnly if external_count > 0 => DownloadTorrentReachability {
            state: DownloadTorrentReachabilityState::ExternallyManaged,
            can_accept_inbound: false,
            listen_port: None,
            forwarded_port: None,
            detail: "Torrent reachability is managed by the external download stack, not by Elixir."
                .to_string(),
        },
        _ => DownloadTorrentReachability {
            state: DownloadTorrentReachabilityState::NotApplicable,
            can_accept_inbound: false,
            listen_port: None,
            forwarded_port: None,
            detail: "No torrent downloader path is currently managed by Elixir.".to_string(),
        },
    }
}

fn torrent_reachability_for_profile(
    profile: &StoredDownloadNetworkProfile,
    inventory: &ManagedDownloaderInventory,
) -> DownloadTorrentReachability {
    match profile.kind {
        DownloadNetworkProfileKind::CloudflareWarp => DownloadTorrentReachability {
            state: DownloadTorrentReachabilityState::NoForwardedPort,
            can_accept_inbound: false,
            listen_port: None,
            forwarded_port: None,
            detail: "Cloudflare WARP provides downloader egress privacy but does not provide a forwarded inbound torrent port."
                .to_string(),
        },
        DownloadNetworkProfileKind::DebridOnly => DownloadTorrentReachability {
            state: DownloadTorrentReachabilityState::DebridOnly,
            can_accept_inbound: false,
            listen_port: None,
            forwarded_port: None,
            detail: "Debrid-only acquisition does not use a local torrent client that accepts inbound peers."
                .to_string(),
        },
        DownloadNetworkProfileKind::ExternalOnly => {
            if inventory.external_count > 0 {
                DownloadTorrentReachability {
                    state: DownloadTorrentReachabilityState::ExternallyManaged,
                    can_accept_inbound: false,
                    listen_port: None,
                    forwarded_port: None,
                    detail: "Torrent reachability is managed by the external download stack, not by Elixir."
                        .to_string(),
                }
            } else {
                DownloadTorrentReachability {
                    state: DownloadTorrentReachabilityState::NotApplicable,
                    can_accept_inbound: false,
                    listen_port: None,
                    forwarded_port: None,
                    detail: "No external downloader provider is registered for Elixir to observe."
                        .to_string(),
                }
            }
        }
        DownloadNetworkProfileKind::Direct => DownloadTorrentReachability {
            state: DownloadTorrentReachabilityState::Unknown,
            can_accept_inbound: false,
            listen_port: None,
            forwarded_port: None,
            detail: "Direct managed downloader mode does not include Elixir-observed torrent port forwarding."
                .to_string(),
        },
        DownloadNetworkProfileKind::WireguardConfig
        | DownloadNetworkProfileKind::OpenvpnConfig
        | DownloadNetworkProfileKind::ProviderPreset => {
            if let Some(forwarded_port) = forwarded_port_from_config(&profile.config_json) {
                return DownloadTorrentReachability {
                    state: DownloadTorrentReachabilityState::ForwardedPort,
                    can_accept_inbound: true,
                    listen_port: Some(forwarded_port.port),
                    forwarded_port: Some(forwarded_port.clone()),
                    detail: format!(
                        "Provider evidence reports forwarded {} port {} for torrent reachability.",
                        forwarded_port.protocol, forwarded_port.port
                    ),
                };
            }
            DownloadTorrentReachability {
                state: DownloadTorrentReachabilityState::Unknown,
                can_accept_inbound: false,
                listen_port: None,
                forwarded_port: None,
                detail: "This protected profile has no observed forwarded torrent port yet."
                    .to_string(),
            }
        }
    }
}

fn torrent_reachability_check(
    reachability: &DownloadTorrentReachability,
) -> DownloadProtectionCheck {
    match reachability.state {
        DownloadTorrentReachabilityState::ForwardedPort => check(
            "torrent_reachability_forwarded_port",
            DownloadProtectionCheckStatus::Pass,
            DownloadProtectionSeverity::Info,
            &reachability.detail,
        ),
        DownloadTorrentReachabilityState::NoForwardedPort => check(
            "torrent_reachability_no_forwarded_port",
            DownloadProtectionCheckStatus::Warn,
            DownloadProtectionSeverity::Warning,
            &reachability.detail,
        ),
        DownloadTorrentReachabilityState::ExternallyManaged => check(
            "torrent_reachability_externally_managed",
            DownloadProtectionCheckStatus::Unknown,
            DownloadProtectionSeverity::Info,
            &reachability.detail,
        ),
        DownloadTorrentReachabilityState::DebridOnly => check(
            "torrent_reachability_debrid_only",
            DownloadProtectionCheckStatus::Pass,
            DownloadProtectionSeverity::Info,
            &reachability.detail,
        ),
        DownloadTorrentReachabilityState::NotApplicable => check(
            "torrent_reachability_not_applicable",
            DownloadProtectionCheckStatus::Warn,
            DownloadProtectionSeverity::Warning,
            &reachability.detail,
        ),
        DownloadTorrentReachabilityState::Unknown => check(
            "torrent_reachability_unknown",
            DownloadProtectionCheckStatus::Unknown,
            DownloadProtectionSeverity::Warning,
            &reachability.detail,
        ),
    }
}

fn forwarded_port_from_config(config: &serde_json::Value) -> Option<DownloadForwardedPort> {
    let reachability =
        json_child(config, &["torrentReachability", "torrent_reachability"]).unwrap_or(config);
    let port_config = json_child(reachability, &["forwardedPort", "forwarded_port"])
        .or_else(|| json_child(reachability, &["portForwarding", "port_forwarding"]))
        .or_else(|| json_child(config, &["forwardedPort", "forwarded_port"]))?;
    let port = json_u16(json_child(
        port_config,
        &["port", "listenPort", "listen_port"],
    )?)?;
    let protocol = json_string(port_config, &["protocol"])
        .unwrap_or("tcp")
        .trim()
        .to_ascii_lowercase();
    let source = json_string(port_config, &["source", "provider"])
        .unwrap_or("profile_config")
        .trim()
        .to_string();
    let expires_at =
        json_string(port_config, &["expiresAt", "expires_at"]).and_then(parse_config_datetime);

    Some(DownloadForwardedPort {
        port,
        protocol: if protocol.is_empty() {
            "tcp".to_string()
        } else {
            protocol
        },
        source: if source.is_empty() {
            "profile_config".to_string()
        } else {
            source
        },
        expires_at,
    })
}

fn json_child<'a>(value: &'a serde_json::Value, keys: &[&str]) -> Option<&'a serde_json::Value> {
    keys.iter().find_map(|key| value.get(*key))
}

fn json_string<'a>(value: &'a serde_json::Value, keys: &[&str]) -> Option<&'a str> {
    json_child(value, keys).and_then(serde_json::Value::as_str)
}

fn json_u16(value: &serde_json::Value) -> Option<u16> {
    if let Some(raw) = value.as_u64() {
        return u16::try_from(raw).ok().filter(|port| *port > 0);
    }
    value
        .as_str()
        .and_then(|raw| raw.trim().parse::<u16>().ok())
        .filter(|port| *port > 0)
}

fn parse_config_datetime(value: &str) -> Option<DateTime<Utc>> {
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|parsed| parsed.with_timezone(&Utc))
        .ok()
        .or_else(|| parse_datetime(value, "download_network_profiles.config_json").ok())
}

fn push_profile_validation(
    checks: &mut Vec<DownloadProtectionCheck>,
    phases: &mut Vec<DownloadProtectionSwitchPhase>,
    blocker: &mut Option<DownloadProtectionBlocker>,
    target: &StoredDownloadNetworkProfile,
    expected_active_profile_id: Option<&str>,
    actual_active_profile_id: &str,
) {
    if !target.enabled {
        let next = DownloadProtectionBlocker {
            code: "target_profile_disabled".to_string(),
            title: "Target profile is disabled".to_string(),
            detail: format!(
                "Download network profile '{}' is disabled and cannot be selected.",
                target.id
            ),
            severity: DownloadProtectionSeverity::Critical,
        };
        checks.push(check(
            "target_profile_enabled",
            DownloadProtectionCheckStatus::Fail,
            DownloadProtectionSeverity::Critical,
            &next.detail,
        ));
        phases.push(phase(
            "validate_requested_profile",
            DownloadProtectionSwitchPhaseStatus::Fail,
            &next.detail,
            Some(next.clone()),
        ));
        *blocker = Some(next);
        return;
    }

    if let Some(expected) = expected_active_profile_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if expected != actual_active_profile_id {
            let next = DownloadProtectionBlocker {
                code: "active_profile_changed".to_string(),
                title: "Active profile changed".to_string(),
                detail: format!(
                    "The active profile is now '{}', not the expected '{}'. Re-read status before switching.",
                    actual_active_profile_id, expected
                ),
                severity: DownloadProtectionSeverity::Critical,
            };
            checks.push(check(
                "expected_active_profile",
                DownloadProtectionCheckStatus::Fail,
                DownloadProtectionSeverity::Critical,
                &next.detail,
            ));
            phases.push(phase(
                "validate_requested_profile",
                DownloadProtectionSwitchPhaseStatus::Fail,
                &next.detail,
                Some(next.clone()),
            ));
            *blocker = Some(next);
            return;
        }
    }

    checks.push(check(
        "target_profile_enabled",
        DownloadProtectionCheckStatus::Pass,
        DownloadProtectionSeverity::Info,
        "The requested download network profile exists and is enabled.",
    ));

    if let Some(next) = profile_validation_blocker(target) {
        checks.push(check(
            "target_profile_valid",
            DownloadProtectionCheckStatus::Fail,
            DownloadProtectionSeverity::Critical,
            &next.detail,
        ));
        phases.push(phase(
            "validate_requested_profile",
            DownloadProtectionSwitchPhaseStatus::Fail,
            &next.detail,
            Some(next.clone()),
        ));
        *blocker = Some(next);
        return;
    }

    checks.push(check(
        "target_profile_valid",
        DownloadProtectionCheckStatus::Pass,
        DownloadProtectionSeverity::Info,
        "The requested download network profile passed static validation.",
    ));
    phases.push(phase(
        "validate_requested_profile",
        DownloadProtectionSwitchPhaseStatus::Pass,
        "The requested profile passed identity and enabled-state validation.",
        None,
    ));
}

async fn push_profile_runtime_validation(
    checks: &mut Vec<DownloadProtectionCheck>,
    phases: &mut Vec<DownloadProtectionSwitchPhase>,
    blocker: &mut Option<DownloadProtectionBlocker>,
    target: &StoredDownloadNetworkProfile,
    inventory: &ManagedDownloaderInventory,
    pool: &AnyPool,
    store: &ExtensionStore<'_>,
    secrets: &SecretsManager,
) -> Result<()> {
    let protected_apps = protected_apps_for_profile(target, inventory);
    if profile_requires_protected_egress(&target.kind) && protected_apps.is_empty() {
        let next = DownloadProtectionBlocker {
            code: "protected_downloaders_missing".to_string(),
            title: "No managed downloaders are available".to_string(),
            detail: "The target profile requires protected managed downloaders, but no enabled qBittorrent or NZBGet instances were found."
                .to_string(),
            severity: DownloadProtectionSeverity::Critical,
        };
        checks.push(check(
            "protected_apps_selected",
            DownloadProtectionCheckStatus::Fail,
            DownloadProtectionSeverity::Critical,
            &next.detail,
        ));
        phases.push(phase(
            "prepare_gateway",
            DownloadProtectionSwitchPhaseStatus::Blocked,
            &next.detail,
            Some(next.clone()),
        ));
        *blocker = Some(next);
        return Ok(());
    }

    checks.push(check(
        "protected_apps_selected",
        if profile_requires_protected_egress(&target.kind) {
            DownloadProtectionCheckStatus::Pass
        } else {
            DownloadProtectionCheckStatus::Warn
        },
        if profile_requires_protected_egress(&target.kind) {
            DownloadProtectionSeverity::Info
        } else {
            DownloadProtectionSeverity::Warning
        },
        if profile_requires_protected_egress(&target.kind) {
            "The target protected profile has enabled managed downloader apps to protect."
        } else {
            "The target profile does not require protected downloader egress."
        },
    ));

    let runtime_check = gateway_runtime_check(target);
    let runtime_failed = matches!(&runtime_check.status, DownloadProtectionCheckStatus::Fail);
    if runtime_failed {
        let next = DownloadProtectionBlocker {
            code: "gateway_runtime_unsupported".to_string(),
            title: "Gateway runtime is not supported".to_string(),
            detail: runtime_check.detail.clone(),
            severity: DownloadProtectionSeverity::Critical,
        };
        checks.push(runtime_check);
        phases.push(phase(
            "prepare_gateway",
            DownloadProtectionSwitchPhaseStatus::Blocked,
            &next.detail,
            Some(next.clone()),
        ));
        *blocker = Some(next);
        return Ok(());
    }
    checks.push(runtime_check);

    if target.kind == DownloadNetworkProfileKind::WireguardConfig {
        let secret_ref = load_profile_secret_ref(pool, &target.id, "wireguard_config")
            .await?
            .unwrap_or_default();
        let secret_check = if secret_ref.trim().is_empty() {
            SecretCheck::InvalidRef(
                "wireguard_config profile secret reference is missing".to_string(),
            )
        } else {
            check_wireguard_secret(
                store,
                secrets,
                &secret_ref,
                inventory.protected_instance_ids(true, true),
            )
            .await?
        };
        if let Some(next) = secret_blocker(&secret_check) {
            checks.push(secret_status_check(&secret_check, &secret_ref));
            phases.push(phase(
                "prepare_gateway",
                DownloadProtectionSwitchPhaseStatus::Blocked,
                &next.detail,
                Some(next.clone()),
            ));
            *blocker = Some(next);
            return Ok(());
        }
        checks.push(secret_status_check(&secret_check, &secret_ref));
    }

    if target.kind == DownloadNetworkProfileKind::OpenvpnConfig {
        let secret_ref = load_profile_secret_ref(pool, &target.id, "openvpn_config")
            .await?
            .unwrap_or_default();
        let secret_check = if secret_ref.trim().is_empty() {
            SecretCheck::InvalidRef(
                "openvpn_config profile secret reference is missing".to_string(),
            )
        } else {
            check_profile_secret(store, secrets, &secret_ref).await?
        };
        if let Some(next) =
            profile_secret_blocker(&secret_check, "openvpn_config_secret", "OpenVPN config")
        {
            checks.push(profile_secret_status_check(
                &secret_check,
                &secret_ref,
                "openvpn_config_secret",
                "OpenVPN config",
            ));
            phases.push(phase(
                "prepare_gateway",
                DownloadProtectionSwitchPhaseStatus::Blocked,
                &next.detail,
                Some(next.clone()),
            ));
            *blocker = Some(next);
            return Ok(());
        }
        checks.push(profile_secret_status_check(
            &secret_check,
            &secret_ref,
            "openvpn_config_secret",
            "OpenVPN config",
        ));

        let username_ref = load_profile_secret_ref(pool, &target.id, "openvpn_username").await?;
        let password_ref = load_profile_secret_ref(pool, &target.id, "openvpn_password").await?;
        if username_ref.is_some() != password_ref.is_some() {
            let next = DownloadProtectionBlocker {
                code: "openvpn_auth_incomplete".to_string(),
                title: "OpenVPN credentials are incomplete".to_string(),
                detail: "The OpenVPN profile has only one of username or password configured."
                    .to_string(),
                severity: DownloadProtectionSeverity::Critical,
            };
            checks.push(check(
                "openvpn_auth_credentials",
                DownloadProtectionCheckStatus::Fail,
                DownloadProtectionSeverity::Critical,
                &next.detail,
            ));
            phases.push(phase(
                "prepare_gateway",
                DownloadProtectionSwitchPhaseStatus::Blocked,
                &next.detail,
                Some(next.clone()),
            ));
            *blocker = Some(next);
            return Ok(());
        }
        checks.push(check(
            "openvpn_auth_credentials",
            if username_ref.is_some() {
                DownloadProtectionCheckStatus::Pass
            } else {
                DownloadProtectionCheckStatus::Warn
            },
            if username_ref.is_some() {
                DownloadProtectionSeverity::Info
            } else {
                DownloadProtectionSeverity::Warning
            },
            if username_ref.is_some() {
                "OpenVPN username/password credentials are stored as profile secrets."
            } else {
                "No separate OpenVPN username/password credentials are stored for this profile."
            },
        ));
    }

    if target.kind == DownloadNetworkProfileKind::CloudflareWarp {
        let enrollment = load_cloudflare_warp_enrollment(pool, &target.id).await?;
        let (warp_checks, warp_blocker) = warp_profile_switch_checks(target, enrollment.as_ref());
        checks.extend(warp_checks);
        if let Some(next) = warp_blocker {
            phases.push(phase(
                "prepare_gateway",
                DownloadProtectionSwitchPhaseStatus::Blocked,
                &next.detail,
                Some(next.clone()),
            ));
            *blocker = Some(next);
            return Ok(());
        }
    }

    phases.push(phase(
        "prepare_gateway",
        if profile_requires_protected_egress(&target.kind) {
            DownloadProtectionSwitchPhaseStatus::Pending
        } else {
            DownloadProtectionSwitchPhaseStatus::Skipped
        },
        if profile_requires_protected_egress(&target.kind) {
            "Gateway preparation must be executed by the deterministic orchestrator apply path."
        } else {
            "No gateway is required for the target profile."
        },
        None,
    ));
    Ok(())
}

fn push_leak_gate(
    checks: &mut Vec<DownloadProtectionCheck>,
    phases: &mut Vec<DownloadProtectionSwitchPhase>,
    blocker: &mut Option<DownloadProtectionBlocker>,
    target: &StoredDownloadNetworkProfile,
    server_public_ip: Option<&str>,
    downloader_public_ip: Option<&str>,
) {
    if !profile_requires_protected_egress(&target.kind) {
        checks.push(check(
            "leak_check",
            DownloadProtectionCheckStatus::Warn,
            DownloadProtectionSeverity::Warning,
            "Leak checks are not required for direct or external-only profiles.",
        ));
        phases.push(phase(
            "verify_gateway",
            DownloadProtectionSwitchPhaseStatus::Skipped,
            "No protected gateway is required for the target profile.",
            None,
        ));
        return;
    }

    let server = server_public_ip
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let downloader = downloader_public_ip
        .map(str::trim)
        .filter(|value| !value.is_empty());
    match (server, downloader) {
        (Some(server), Some(downloader)) if server == downloader => {
            let next = DownloadProtectionBlocker {
                code: "download_network_leak_detected".to_string(),
                title: "Downloader traffic is leaking".to_string(),
                detail: format!(
                    "The downloader public IP ({downloader}) matches the server public IP ({server}). Protected downloads remain blocked."
                ),
                severity: DownloadProtectionSeverity::Critical,
            };
            checks.push(check(
                "leak_check",
                DownloadProtectionCheckStatus::Fail,
                DownloadProtectionSeverity::Critical,
                &next.detail,
            ));
            phases.push(phase(
                "verify_gateway",
                DownloadProtectionSwitchPhaseStatus::Fail,
                &next.detail,
                Some(next.clone()),
            ));
            *blocker = Some(next);
        }
        (Some(server), Some(downloader)) => {
            checks.push(check(
                "leak_check",
                DownloadProtectionCheckStatus::Pass,
                DownloadProtectionSeverity::Info,
                &format!(
                    "Downloader public IP ({downloader}) differs from server public IP ({server})."
                ),
            ));
            phases.push(phase(
                "verify_gateway",
                DownloadProtectionSwitchPhaseStatus::Pass,
                "Public-IP evidence does not show a downloader leak.",
                None,
            ));
        }
        _ => {
            checks.push(check(
                "leak_check",
                DownloadProtectionCheckStatus::Unknown,
                DownloadProtectionSeverity::Warning,
                "Protected profile public-IP evidence is not available before rehome; it must pass after the deterministic orchestrator applies the target profile.",
            ));
            phases.push(phase(
                "verify_gateway",
                DownloadProtectionSwitchPhaseStatus::Pending,
                "Gateway and downloader public-IP evidence will be verified after rehome.",
                None,
            ));
        }
    }
}

fn push_post_apply_runtime_evidence_gate(
    checks: &mut Vec<DownloadProtectionCheck>,
    phases: &mut Vec<DownloadProtectionSwitchPhase>,
    blocker: &mut Option<DownloadProtectionBlocker>,
    target: &StoredDownloadNetworkProfile,
    evidence: &DownloadProtectionRuntimeEvidence,
) {
    let (mut evidence_checks, evidence_blocker) =
        runtime_evidence_status_checks(target, Some(evidence));
    checks.append(&mut evidence_checks);

    if let Some(next) = evidence_blocker {
        set_or_push_phase(
            phases,
            "verify_gateway",
            DownloadProtectionSwitchPhaseStatus::Blocked,
            &next.detail,
            Some(next.clone()),
        );
        set_or_push_phase(
            phases,
            "verify_protected_apps",
            DownloadProtectionSwitchPhaseStatus::Blocked,
            &next.detail,
            Some(next.clone()),
        );
        *blocker = Some(next);
    } else {
        set_or_push_phase(
            phases,
            "verify_gateway",
            DownloadProtectionSwitchPhaseStatus::Pass,
            "Gateway public-IP and DNS evidence passed after rehome.",
            None,
        );
        set_or_push_phase(
            phases,
            "verify_protected_apps",
            DownloadProtectionSwitchPhaseStatus::Pass,
            "Downloader public-IP, DNS, and kill-switch evidence passed after rehome.",
            None,
        );
    }
}

fn push_runtime_evidence_switch_gate(
    checks: &mut Vec<DownloadProtectionCheck>,
    phases: &mut Vec<DownloadProtectionSwitchPhase>,
    blocker: &mut Option<DownloadProtectionBlocker>,
    target: &StoredDownloadNetworkProfile,
    evidence: Option<&DownloadProtectionRuntimeEvidence>,
) {
    if !profile_requires_protected_egress(&target.kind) {
        return;
    }
    let Some(evidence) = evidence else {
        return;
    };

    let mut evidence_checks = Vec::new();
    for (code, probe, missing_detail) in [
        (
            "gateway_dns",
            evidence.gateway_dns.as_ref(),
            "Gateway DNS evidence is unavailable for the protected profile switch.",
        ),
        (
            "downloader_dns",
            evidence.downloader_dns.as_ref(),
            "Downloader DNS evidence is unavailable for the protected profile switch.",
        ),
        (
            "kill_switch",
            evidence.kill_switch.as_ref(),
            "Kill-switch evidence is unavailable for the protected profile switch.",
        ),
    ] {
        evidence_checks.push(evidence_check(
            code,
            probe,
            DownloadProtectionCheckStatus::Fail,
            DownloadProtectionSeverity::Critical,
            missing_detail,
        ));
    }
    checks.extend(evidence_checks);

    if let Some(next) = first_runtime_evidence_blocker(evidence) {
        phases.push(phase(
            "verify_protected_apps",
            DownloadProtectionSwitchPhaseStatus::Blocked,
            &next.detail,
            Some(next.clone()),
        ));
        *blocker = Some(next);
    }
}

fn complete_remaining_switch_phases(
    phases: &mut Vec<DownloadProtectionSwitchPhase>,
    apply_requested: bool,
    requires_orchestrated_runtime: bool,
    can_continue: bool,
) {
    let existing = phases
        .iter()
        .map(|phase| phase.id.clone())
        .collect::<HashSet<_>>();
    for id in [
        "verify_gateway",
        "pause_managed_downloaders",
        "rehome_protected_apps",
        "verify_protected_apps",
        "resume",
        "cleanup",
    ] {
        if existing.contains(id) {
            continue;
        }
        let (status, detail) = if !can_continue {
            (
                DownloadProtectionSwitchPhaseStatus::Skipped,
                "Skipped because an earlier switch gate failed.",
            )
        } else if !apply_requested {
            (
                DownloadProtectionSwitchPhaseStatus::Skipped,
                "Skipped because this request only ran switch preflight.",
            )
        } else if !requires_orchestrated_runtime {
            (
                DownloadProtectionSwitchPhaseStatus::Skipped,
                "Skipped because the switch does not change managed downloader runtime topology.",
            )
        } else {
            (
                DownloadProtectionSwitchPhaseStatus::Pending,
                "Pending deterministic orchestrator execution.",
            )
        };
        phases.push(phase(id, status, detail, None));
    }
}

fn mark_orchestrated_switch_rehome_applied(phases: &mut Vec<DownloadProtectionSwitchPhase>) {
    set_or_push_phase(
        phases,
        "prepare_gateway",
        DownloadProtectionSwitchPhaseStatus::Pass,
        "Downloader network specs were applied by the deterministic orchestrator.",
        None,
    );
    for id in ["pause_managed_downloaders", "rehome_protected_apps"] {
        set_or_push_phase(
            phases,
            id,
            DownloadProtectionSwitchPhaseStatus::Pass,
            "Completed by the deterministic orchestrator apply path.",
            None,
        );
    }
}

fn mark_orchestrated_switch_phases_verified(
    phases: &mut Vec<DownloadProtectionSwitchPhase>,
    target: &StoredDownloadNetworkProfile,
) {
    if profile_requires_protected_egress(&target.kind) {
        set_or_push_phase(
            phases,
            "verify_gateway",
            DownloadProtectionSwitchPhaseStatus::Pass,
            "Gateway runtime evidence passed after deterministic orchestrator apply.",
            None,
        );
        set_or_push_phase(
            phases,
            "verify_protected_apps",
            DownloadProtectionSwitchPhaseStatus::Pass,
            "Protected downloader runtime evidence passed after deterministic orchestrator apply.",
            None,
        );
    } else {
        set_or_push_phase(
            phases,
            "verify_protected_apps",
            DownloadProtectionSwitchPhaseStatus::Pass,
            "Managed downloader runtime was rehomed away from protected gateway networking.",
            None,
        );
    }
    set_or_push_phase(
        phases,
        "resume",
        DownloadProtectionSwitchPhaseStatus::Pass,
        "Switch verification passed; broker submissions and managed downloads may resume.",
        None,
    );
    set_or_push_phase(
        phases,
        "cleanup",
        DownloadProtectionSwitchPhaseStatus::Pass,
        "Switch verification passed; no rollback was required.",
        None,
    );
}

fn mark_post_apply_rollback_phases(
    phases: &mut Vec<DownloadProtectionSwitchPhase>,
    blocker: Option<&DownloadProtectionBlocker>,
    rollback_applied: bool,
    rollback_error: Option<&str>,
) {
    let detail = blocker
        .map(|blocker| blocker.detail.as_str())
        .unwrap_or("Runtime verification failed after protected downloader rehome.");
    set_or_push_phase(
        phases,
        "verify_protected_apps",
        DownloadProtectionSwitchPhaseStatus::Blocked,
        detail,
        blocker.cloned(),
    );
    set_or_push_phase(
        phases,
        "resume",
        DownloadProtectionSwitchPhaseStatus::Skipped,
        "Skipped because post-apply verification failed.",
        None,
    );
    if let Some(err) = rollback_error {
        set_or_push_phase(
            phases,
            "cleanup",
            DownloadProtectionSwitchPhaseStatus::Fail,
            &format!("Post-apply verification failed, and rollback rehome failed: {err}"),
            Some(rollback_failed_blocker(err)),
        );
    } else if rollback_applied {
        set_or_push_phase(
            phases,
            "cleanup",
            DownloadProtectionSwitchPhaseStatus::Pass,
            "Post-apply verification failed; the previous active profile was restored and reapplied.",
            None,
        );
    } else {
        set_or_push_phase(
            phases,
            "cleanup",
            DownloadProtectionSwitchPhaseStatus::Pass,
            "Post-apply verification failed; the previous active profile state was restored.",
            None,
        );
    }
}

fn set_or_push_phase(
    phases: &mut Vec<DownloadProtectionSwitchPhase>,
    id: &str,
    status: DownloadProtectionSwitchPhaseStatus,
    detail: &str,
    blocker: Option<DownloadProtectionBlocker>,
) {
    if let Some(existing) = phases.iter_mut().find(|phase| phase.id == id) {
        existing.status = status;
        existing.detail = detail.to_string();
        existing.blocker = blocker;
    } else {
        phases.push(phase(id, status, detail, blocker));
    }
}

fn phase(
    id: &str,
    status: DownloadProtectionSwitchPhaseStatus,
    detail: &str,
    blocker: Option<DownloadProtectionBlocker>,
) -> DownloadProtectionSwitchPhase {
    DownloadProtectionSwitchPhase {
        id: id.to_string(),
        status,
        detail: detail.to_string(),
        blocker,
    }
}

async fn load_active_download_network_profile(
    pool: &AnyPool,
) -> Result<Option<StoredDownloadNetworkProfile>> {
    let row = sqlx::query::<sqlx::Any>(
        "SELECT id, name, kind, CAST(enabled AS INTEGER) as enabled, CAST(strict AS INTEGER) as strict, scope, CAST(provider AS TEXT) as provider, CAST(gateway_runtime AS TEXT) as gateway_runtime, CAST(config_json AS TEXT) as config_json, status, CAST(active AS INTEGER) as active FROM download_network_profiles WHERE active = TRUE ORDER BY updated_at DESC LIMIT 1",
    )
    .fetch_optional(pool)
    .await?;
    row.map(|row| map_download_network_profile(&row))
        .transpose()
}

async fn load_download_network_profile(
    pool: &AnyPool,
    profile_id: &str,
) -> Result<Option<StoredDownloadNetworkProfile>> {
    let row = sqlx::query::<sqlx::Any>(
        "SELECT id, name, kind, CAST(enabled AS INTEGER) as enabled, CAST(strict AS INTEGER) as strict, scope, CAST(provider AS TEXT) as provider, CAST(gateway_runtime AS TEXT) as gateway_runtime, CAST(config_json AS TEXT) as config_json, status, CAST(active AS INTEGER) as active FROM download_network_profiles WHERE id = ? LIMIT 1",
    )
    .bind(profile_id)
    .fetch_optional(pool)
    .await?;
    row.map(|row| map_download_network_profile(&row))
        .transpose()
}

async fn list_stored_download_network_profiles(
    pool: &AnyPool,
) -> Result<Vec<StoredDownloadNetworkProfile>> {
    let rows = sqlx::query::<sqlx::Any>(
        "SELECT id, name, kind, CAST(enabled AS INTEGER) as enabled, CAST(strict AS INTEGER) as strict, scope, CAST(provider AS TEXT) as provider, CAST(gateway_runtime AS TEXT) as gateway_runtime, CAST(config_json AS TEXT) as config_json, status, CAST(active AS INTEGER) as active FROM download_network_profiles ORDER BY active DESC, updated_at DESC, name ASC",
    )
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|row| map_download_network_profile(&row))
        .collect()
}

async fn load_profile_secret_ref(
    pool: &AnyPool,
    profile_id: &str,
    key: &str,
) -> Result<Option<String>> {
    sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT secret_ref FROM download_network_profile_secrets WHERE profile_id = ? AND key = ? LIMIT 1",
    )
    .bind(profile_id)
    .bind(key)
    .fetch_optional(pool)
    .await
    .context("loading download network profile secret reference")
}

async fn upsert_profile_secret_ref(
    pool: &AnyPool,
    profile_id: &str,
    key: &str,
    secret_ref: &str,
) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "INSERT INTO download_network_profile_secrets (profile_id, key, secret_ref) VALUES (?, ?, ?)
         ON CONFLICT(profile_id, key) DO UPDATE SET secret_ref = excluded.secret_ref",
    )
    .bind(profile_id)
    .bind(key)
    .bind(secret_ref)
    .execute(pool)
    .await
    .context("upserting download network profile secret reference")?;
    Ok(())
}

async fn upsert_imported_download_profile(
    pool: &AnyPool,
    profile_id: &str,
    name: &str,
    kind: &DownloadNetworkProfileKind,
    strict: bool,
    provider: Option<&str>,
    gateway_runtime: &str,
    config_json: &serde_json::Value,
    status: DownloadProtectionState,
) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "INSERT INTO download_network_profiles (id, name, kind, enabled, strict, scope, provider, gateway_runtime, config_json, status, active) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, FALSE)
         ON CONFLICT(id) DO UPDATE SET name = excluded.name, enabled = TRUE, strict = excluded.strict, scope = excluded.scope, provider = excluded.provider, gateway_runtime = excluded.gateway_runtime, config_json = excluded.config_json, status = excluded.status, updated_at = CURRENT_TIMESTAMP",
    )
    .bind(profile_id)
    .bind(name)
    .bind(profile_kind_as_str(kind))
    .bind(true)
    .bind(strict)
    .bind("managed_downloaders")
    .bind(provider.map(str::trim).filter(|value| !value.is_empty()))
    .bind(gateway_runtime)
    .bind(serde_json::to_string(config_json)?)
    .bind(protection_state_as_str(status))
    .execute(pool)
    .await
    .context("upserting imported download network profile")?;
    Ok(())
}

async fn upsert_external_only_first_run_profile(
    pool: &AnyPool,
    profile_id: &str,
    name: &str,
    choice: &DownloadProtectionFirstRunChoice,
) -> Result<()> {
    upsert_imported_download_profile(
        pool,
        profile_id,
        name,
        &DownloadNetworkProfileKind::ExternalOnly,
        false,
        None,
        "",
        &serde_json::json!({
            "firstRun": {
                "choice": choice,
                "managedDownloaderNetworking": "not_managed"
            }
        }),
        DownloadProtectionState::ExternallyManaged,
    )
    .await
}

async fn upsert_cloudflare_warp_profile(
    pool: &AnyPool,
    profile_id: &str,
    name: &str,
) -> Result<()> {
    let config_json = serde_json::json!({
        "cloudflareWarp": {
            "disclosureVersion": CLOUDFLARE_WARP_DISCLOSURE_VERSION,
            "runtime": "warp_gateway",
            "gatewayImage": DEFAULT_CLOUDFLARE_WARP_GATEWAY_IMAGE,
            "stateVolume": warp_state_volume_name(profile_id),
            "credentialMode": "per_server",
            "sharedCredentials": false
        }
    });
    sqlx::query::<sqlx::Any>(
        "INSERT INTO download_network_profiles (id, name, kind, enabled, strict, scope, provider, gateway_runtime, config_json, status, active) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, FALSE)
         ON CONFLICT(id) DO UPDATE SET name = excluded.name, enabled = TRUE, strict = TRUE, scope = excluded.scope, provider = excluded.provider, gateway_runtime = excluded.gateway_runtime, config_json = excluded.config_json, status = excluded.status, updated_at = CURRENT_TIMESTAMP",
    )
    .bind(profile_id)
    .bind(name)
    .bind("cloudflare_warp")
    .bind(true)
    .bind(true)
    .bind("managed_downloaders")
    .bind("cloudflare")
    .bind("warp_gateway")
    .bind(serde_json::to_string(&config_json)?)
    .bind("blocked")
    .execute(pool)
    .await
    .context("upserting Cloudflare WARP profile")?;
    Ok(())
}

async fn upsert_default_download_routes(
    pool: &AnyPool,
    binding_kind: DownloadBrokerBindingKind,
    profile_id: Option<String>,
) -> Result<()> {
    let store = ExtensionStore::new(pool);
    for logical_id in [TORRENT_DEFAULT_LOGICAL_ID, USENET_DEFAULT_LOGICAL_ID] {
        upsert_acquisition_route(
            pool,
            &store,
            logical_id,
            DownloadBrokerRouteUpdate {
                binding_kind,
                owner_id: None,
                provider_id: None,
                profile_id: profile_id.clone(),
                category: None,
                download_path: None,
                allow_shared_path: None,
                status: Some("selected".to_string()),
            },
        )
        .await
        .with_context(|| format!("storing default first-run route for '{logical_id}'"))?;
    }
    Ok(())
}

async fn apply_default_routes_for_profile_switch(
    pool: &AnyPool,
    profile: &StoredDownloadNetworkProfile,
) -> Result<()> {
    match profile.kind {
        DownloadNetworkProfileKind::CloudflareWarp
        | DownloadNetworkProfileKind::WireguardConfig
        | DownloadNetworkProfileKind::OpenvpnConfig
        | DownloadNetworkProfileKind::ProviderPreset => {
            upsert_default_download_routes(
                pool,
                DownloadBrokerBindingKind::ManagedProtected,
                Some(profile.id.clone()),
            )
            .await
        }
        DownloadNetworkProfileKind::Direct => {
            upsert_default_download_routes(pool, DownloadBrokerBindingKind::ManagedDirect, None)
                .await
        }
        DownloadNetworkProfileKind::ExternalOnly => {
            upsert_default_download_routes(pool, DownloadBrokerBindingKind::External, None).await
        }
        DownloadNetworkProfileKind::DebridOnly => {
            upsert_default_download_routes(pool, DownloadBrokerBindingKind::External, None).await?;
            let store = ExtensionStore::new(pool);
            upsert_acquisition_route(
                pool,
                &store,
                DEBRID_DEFAULT_LOGICAL_ID,
                DownloadBrokerRouteUpdate {
                    binding_kind: DownloadBrokerBindingKind::Debrid,
                    owner_id: None,
                    provider_id: None,
                    profile_id: Some(profile.id.clone()),
                    category: None,
                    download_path: None,
                    allow_shared_path: None,
                    status: Some("selected".to_string()),
                },
            )
            .await
            .context("storing default debrid-only acquisition route")?;
            Ok(())
        }
    }
}

async fn ensure_cloudflare_warp_enrollment(
    pool: &AnyPool,
    secrets: &SecretsManager,
    profile_id: &str,
) -> Result<StoredWarpEnrollment> {
    if let Some(enrollment) = load_cloudflare_warp_enrollment(pool, profile_id).await? {
        return Ok(enrollment);
    }

    let enrollment_id = Uuid::new_v4().to_string();
    let secret_key = warp_identity_secret_key(profile_id);
    let identity_secret_ref = format!("global:{secret_key}");
    let created_at = Utc::now();
    let identity_json = serde_json::json!({
        "profileId": profile_id,
        "enrollmentId": enrollment_id,
        "credentialKind": "cloudflare_warp_per_server_identity",
        "credentialSource": "elixir_local_pending_runtime",
        "sharedCredentials": false,
        "disclosureVersion": CLOUDFLARE_WARP_DISCLOSURE_VERSION,
        "createdAt": created_at.to_rfc3339()
    });
    let store = ExtensionStore::new(pool);
    store
        .upsert_secret(&NewSecret {
            secret_id: Uuid::new_v4(),
            scope: SecretScope::Global,
            scope_id: None,
            key: secret_key,
            value_encrypted: secrets.encrypt(&serde_json::to_string(&identity_json)?)?,
            rotatable: true,
        })
        .await
        .context("storing Cloudflare WARP per-server identity")?;

    sqlx::query::<sqlx::Any>(
        "INSERT INTO download_warp_enrollments (id, profile_id, enrollment_id, identity_secret_ref, status, disclosure_version) VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(profile_id)
    .bind(&enrollment_id)
    .bind(&identity_secret_ref)
    .bind("pending_runtime")
    .bind(CLOUDFLARE_WARP_DISCLOSURE_VERSION)
    .execute(pool)
    .await
    .context("creating Cloudflare WARP enrollment record")?;

    load_cloudflare_warp_enrollment(pool, profile_id)
        .await?
        .ok_or_else(|| anyhow!("created Cloudflare WARP enrollment was not readable"))
}

async fn load_cloudflare_warp_enrollment(
    pool: &AnyPool,
    profile_id: &str,
) -> Result<Option<StoredWarpEnrollment>> {
    let row = sqlx::query::<sqlx::Any>(
        "SELECT profile_id, enrollment_id, identity_secret_ref, status, disclosure_version, CAST(disclosure_accepted_at AS TEXT) as disclosure_accepted_at, CAST(last_checked_at AS TEXT) as last_checked_at, CAST(last_error AS TEXT) as last_error FROM download_warp_enrollments WHERE profile_id = ? ORDER BY created_at DESC LIMIT 1",
    )
    .bind(profile_id)
    .fetch_optional(pool)
    .await
    .context("loading Cloudflare WARP enrollment")?;
    row.map(|row| map_warp_enrollment(&row)).transpose()
}

pub async fn mark_cloudflare_warp_runtime_ready(pool: &AnyPool, profile_id: &str) -> Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query::<sqlx::Any>(
        "UPDATE download_warp_enrollments SET status = 'ready', last_checked_at = CURRENT_TIMESTAMP, last_error = NULL, updated_at = CURRENT_TIMESTAMP WHERE profile_id = ?",
    )
    .bind(profile_id)
    .execute(&mut *tx)
    .await
    .context("marking Cloudflare WARP runtime ready")?;
    sqlx::query::<sqlx::Any>(
        "UPDATE download_network_profiles SET status = 'protected', last_verified_at = CURRENT_TIMESTAMP, last_applied_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
    )
    .bind(profile_id)
    .execute(&mut *tx)
    .await
    .context("marking Cloudflare WARP profile protected")?;
    tx.commit().await?;
    Ok(())
}

pub async fn mark_cloudflare_warp_runtime_unavailable(
    pool: &AnyPool,
    profile_id: &str,
    detail: &str,
) -> Result<()> {
    let detail = detail.trim();
    let detail = if detail.is_empty() {
        "Cloudflare WARP gateway is unavailable."
    } else {
        detail
    };
    let mut tx = pool.begin().await?;
    sqlx::query::<sqlx::Any>(
        "UPDATE download_warp_enrollments SET status = 'unavailable', last_checked_at = CURRENT_TIMESTAMP, last_error = ?, updated_at = CURRENT_TIMESTAMP WHERE profile_id = ?",
    )
    .bind(detail)
    .bind(profile_id)
    .execute(&mut *tx)
    .await
    .context("marking Cloudflare WARP runtime unavailable")?;
    sqlx::query::<sqlx::Any>(
        "UPDATE download_network_profiles SET status = 'blocked', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
    )
    .bind(profile_id)
    .execute(&mut *tx)
    .await
    .context("marking Cloudflare WARP profile blocked")?;
    tx.commit().await?;
    Ok(())
}

pub async fn cloudflare_warp_diagnostics(pool: &AnyPool) -> Result<CloudflareWarpDiagnostics> {
    let profile = load_active_download_network_profile(pool)
        .await?
        .filter(|profile| profile.kind == DownloadNetworkProfileKind::CloudflareWarp)
        .or(load_download_network_profile(pool, CLOUDFLARE_WARP_PROFILE_ID).await?);
    let enrollment = if let Some(profile) = profile.as_ref() {
        load_cloudflare_warp_enrollment(pool, &profile.id).await?
    } else {
        None
    };
    let (checks, blocker) = if let Some(profile) = profile.as_ref() {
        warp_profile_checks(profile, enrollment.as_ref())
    } else {
        let blocker = DownloadProtectionBlocker {
            code: "warp_profile_missing".to_string(),
            title: "Cloudflare WARP profile is missing".to_string(),
            detail: "No Cloudflare WARP download protection profile has been created yet."
                .to_string(),
            severity: DownloadProtectionSeverity::Warning,
        };
        (
            vec![check(
                "warp_profile_present",
                DownloadProtectionCheckStatus::Warn,
                DownloadProtectionSeverity::Warning,
                &blocker.detail,
            )],
            Some(blocker),
        )
    };

    Ok(CloudflareWarpDiagnostics {
        profile: profile.map(|profile| profile.summary()),
        enrollment: enrollment.map(|enrollment| enrollment.status_response()),
        checks,
        blocker,
        recent_events: list_download_network_events(pool, 20).await?,
    })
}

pub async fn reset_cloudflare_warp_profile(
    pool: &AnyPool,
    secrets: &SecretsManager,
    request: CloudflareWarpResetRequest,
) -> Result<CloudflareWarpResetResponse> {
    if !request.confirm_reset {
        bail!("Cloudflare WARP reset requires confirmReset=true");
    }

    let profile_id = request
        .profile_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(CLOUDFLARE_WARP_PROFILE_ID);
    let secret_key = warp_identity_secret_key(profile_id);
    let store = ExtensionStore::new(pool);
    for secret in store
        .list_secrets(Some(SecretScope::Global), None, Some(&secret_key))
        .await?
    {
        store.delete_secret(secret.secret_id).await?;
    }

    sqlx::query::<sqlx::Any>("DELETE FROM download_warp_enrollments WHERE profile_id = ?")
        .bind(profile_id)
        .execute(pool)
        .await
        .context("deleting Cloudflare WARP enrollment")?;
    sqlx::query::<sqlx::Any>(
        "UPDATE download_network_profiles SET active = FALSE, status = 'blocked', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
    )
    .bind(profile_id)
    .execute(pool)
    .await
    .context("blocking reset Cloudflare WARP profile")?;

    record_download_network_event(
        pool,
        Some(profile_id),
        "warp_profile_reset",
        "reset",
        &serde_json::json!({
            "profileId": profile_id,
            "recreate": request.recreate,
            "identitySecretKey": secret_key
        }),
    )
    .await?;

    if request.recreate {
        let created = ensure_cloudflare_warp_profile(
            pool,
            secrets,
            CloudflareWarpProfileRequest {
                accepted_disclosure: true,
                profile_id: Some(profile_id.to_string()),
                name: None,
            },
        )
        .await?;
        return Ok(CloudflareWarpResetResponse {
            profile: Some(created.profile),
            enrollment: Some(created.enrollment),
            disclosure: created.disclosure,
            reset: true,
            recreated: true,
            checks: created.checks,
            blocker: created.blocker,
        });
    }

    let profile = load_download_network_profile(pool, profile_id)
        .await?
        .map(|profile| profile.summary());
    Ok(CloudflareWarpResetResponse {
        profile,
        enrollment: None,
        disclosure: cloudflare_warp_disclosure(),
        reset: true,
        recreated: false,
        checks: vec![check(
            "warp_enrollment_reset",
            DownloadProtectionCheckStatus::Warn,
            DownloadProtectionSeverity::Warning,
            "The Cloudflare WARP enrollment and local identity secret were removed. Protected WARP downloads are blocked until a new enrollment is created.",
        )],
        blocker: Some(DownloadProtectionBlocker {
            code: "warp_enrollment_missing".to_string(),
            title: "Cloudflare WARP enrollment is missing".to_string(),
            detail: "The WARP enrollment was reset and not recreated.".to_string(),
            severity: DownloadProtectionSeverity::Critical,
        }),
    })
}

async fn activate_download_network_profile(
    pool: &AnyPool,
    profile: &StoredDownloadNetworkProfile,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query::<sqlx::Any>(
        "UPDATE download_network_profiles SET active = FALSE, updated_at = CURRENT_TIMESTAMP WHERE active = TRUE",
    )
    .execute(&mut *tx)
    .await?;
    if profile_requires_protected_egress(&profile.kind) {
        sqlx::query::<sqlx::Any>(
            "UPDATE download_network_profiles SET active = TRUE, status = 'unknown', last_applied_at = CURRENT_TIMESTAMP, last_verified_at = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        )
        .bind(&profile.id)
        .execute(&mut *tx)
        .await?;
    } else {
        sqlx::query::<sqlx::Any>(
            "UPDATE download_network_profiles SET active = TRUE, status = ?, last_applied_at = CURRENT_TIMESTAMP, last_verified_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        )
        .bind(protection_state_as_str(activation_state_for_profile(profile)))
        .bind(&profile.id)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

async fn mark_download_network_profile_verified(
    pool: &AnyPool,
    profile: &StoredDownloadNetworkProfile,
) -> Result<()> {
    let state = if profile_requires_protected_egress(&profile.kind) {
        DownloadProtectionState::Protected
    } else {
        activation_state_for_profile(profile)
    };
    sqlx::query::<sqlx::Any>(
        "UPDATE download_network_profiles SET status = ?, last_verified_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
    )
    .bind(protection_state_as_str(state))
    .bind(&profile.id)
    .execute(pool)
    .await
    .context("marking download network profile verified")?;
    Ok(())
}

async fn mark_download_network_profile_blocked(pool: &AnyPool, profile_id: &str) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE download_network_profiles SET status = 'blocked', last_verified_at = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
    )
    .bind(profile_id)
    .execute(pool)
    .await
    .context("marking download network profile blocked")?;
    Ok(())
}

async fn rollback_orchestrated_switch<ApplyFn, ApplyFut>(
    pool: &AnyPool,
    previous_profile_id: Option<&str>,
    apply_orchestrator: &mut ApplyFn,
) -> (bool, Option<String>)
where
    ApplyFn: FnMut() -> ApplyFut,
    ApplyFut: Future<Output = Result<()>>,
{
    if let Err(err) = restore_active_download_network_profile(pool, previous_profile_id).await {
        return (
            false,
            Some(format!(
                "failed to restore previous active profile metadata: {err}"
            )),
        );
    }
    match apply_orchestrator().await {
        Ok(()) => (true, None),
        Err(err) => (
            false,
            Some(format!(
                "failed to reapply the previous active profile through the deterministic orchestrator: {err}"
            )),
        ),
    }
}

fn rollback_failed_blocker(detail: &str) -> DownloadProtectionBlocker {
    DownloadProtectionBlocker {
        code: "profile_switch_rollback_failed".to_string(),
        title: "Download profile rollback failed".to_string(),
        detail: detail.to_string(),
        severity: DownloadProtectionSeverity::Critical,
    }
}

fn format_error_chain(err: &anyhow::Error) -> String {
    let mut parts = Vec::new();
    for cause in err.chain() {
        let message = cause.to_string();
        if message.trim().is_empty() || parts.iter().any(|part| part == &message) {
            continue;
        }
        parts.push(message);
    }
    parts.join(": ")
}

async fn restore_active_download_network_profile(
    pool: &AnyPool,
    previous_profile_id: Option<&str>,
) -> Result<()> {
    let previous = if let Some(previous_profile_id) = previous_profile_id {
        Some(
            load_download_network_profile(pool, previous_profile_id)
                .await?
                .ok_or_else(|| {
                    anyhow!(
                        "previous download network profile '{}' no longer exists",
                        previous_profile_id
                    )
                })?,
        )
    } else {
        None
    };

    let mut tx = pool.begin().await?;
    sqlx::query::<sqlx::Any>(
        "UPDATE download_network_profiles SET active = FALSE, updated_at = CURRENT_TIMESTAMP WHERE active = TRUE",
    )
    .execute(&mut *tx)
    .await?;

    if let (Some(previous_profile_id), Some(previous)) = (previous_profile_id, previous.as_ref()) {
        sqlx::query::<sqlx::Any>(
            "UPDATE download_network_profiles SET active = TRUE, status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        )
        .bind(protection_state_as_str(activation_state_for_profile(&previous)))
        .bind(previous_profile_id)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(())
}

async fn record_download_network_event(
    pool: &AnyPool,
    profile_id: Option<&str>,
    operation: &str,
    status: &str,
    evidence: &serde_json::Value,
) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "INSERT INTO download_network_events (id, profile_id, operation, status, evidence_json, finished_at) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(profile_id)
    .bind(operation)
    .bind(status)
    .bind(serde_json::to_string(evidence)?)
    .execute(pool)
    .await
    .context("recording download network event")?;
    Ok(())
}

pub async fn list_download_network_events(
    pool: &AnyPool,
    limit: i64,
) -> Result<Vec<DownloadNetworkEventRecord>> {
    let limit = limit.clamp(1, 200);
    let rows = sqlx::query::<sqlx::Any>(
        "SELECT id, CAST(profile_id AS TEXT) as profile_id, operation, status, evidence_json, CAST(started_at AS TEXT) as started_at, CAST(finished_at AS TEXT) as finished_at FROM download_network_events ORDER BY started_at DESC LIMIT ?",
    )
    .bind(limit)
    .fetch_all(pool)
    .await
    .context("listing download network events")?;
    rows.iter().map(map_download_network_event).collect()
}

fn map_download_network_profile(row: &AnyRow) -> Result<StoredDownloadNetworkProfile> {
    let kind_raw: String = row.try_get("kind")?;
    let status_raw: String = row.try_get("status")?;
    let config_json_raw: String = row.try_get("config_json")?;
    let _active = row_get_bool(row, "active")?;
    Ok(StoredDownloadNetworkProfile {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        kind: parse_profile_kind(&kind_raw)?,
        enabled: row_get_bool(row, "enabled")?,
        strict: row_get_bool(row, "strict")?,
        scope: row.try_get("scope")?,
        provider: row_get_opt_string(row, "provider")?,
        gateway_runtime: row_get_opt_string(row, "gateway_runtime")?,
        config_json: serde_json::from_str(&config_json_raw).with_context(|| {
            format!(
                "parsing config_json for download network profile '{}'",
                row.try_get::<String, _>("id").unwrap_or_default()
            )
        })?,
        status: parse_protection_state(&status_raw),
    })
}

fn map_warp_enrollment(row: &AnyRow) -> Result<StoredWarpEnrollment> {
    Ok(StoredWarpEnrollment {
        profile_id: row.try_get("profile_id")?,
        enrollment_id: row.try_get("enrollment_id")?,
        identity_secret_ref: row.try_get("identity_secret_ref")?,
        status: row.try_get("status")?,
        disclosure_version: row.try_get("disclosure_version")?,
        disclosure_accepted_at: parse_datetime_opt(
            row_get_opt_string(row, "disclosure_accepted_at")?,
            "download_warp_enrollments.disclosure_accepted_at",
        )?,
        last_checked_at: parse_datetime_opt(
            row_get_opt_string(row, "last_checked_at")?,
            "download_warp_enrollments.last_checked_at",
        )?,
        last_error: row_get_opt_string(row, "last_error")?,
    })
}

fn map_download_network_event(row: &AnyRow) -> Result<DownloadNetworkEventRecord> {
    let id_raw: String = row.try_get("id")?;
    let evidence_raw: String = row.try_get("evidence_json")?;
    Ok(DownloadNetworkEventRecord {
        id: Uuid::parse_str(&id_raw).context("download_network_events.id is not a UUID")?,
        profile_id: row_get_opt_string(row, "profile_id")?
            .and_then(|value| (!value.trim().is_empty()).then_some(value)),
        operation: row.try_get("operation")?,
        status: row.try_get("status")?,
        evidence: serde_json::from_str(&evidence_raw)
            .context("parsing download_network_events.evidence_json")?,
        started_at: parse_datetime(
            &row.try_get::<String, _>("started_at")?,
            "download_network_events.started_at",
        )?,
        finished_at: parse_datetime_opt(
            row_get_opt_string(row, "finished_at")?,
            "download_network_events.finished_at",
        )?,
    })
}

fn row_get_opt_string(row: &AnyRow, field: &str) -> Result<Option<String>> {
    let raw = row.try_get_raw(field)?;
    if raw.type_info().name() == "NULL" {
        return Ok(None);
    }
    let value = ValueRef::to_owned(&raw).try_decode::<String>()?;
    Ok(Some(value))
}

fn row_get_bool(row: &AnyRow, field: &str) -> Result<bool> {
    if let Ok(value) = row.try_get::<bool, _>(field) {
        return Ok(value);
    }
    if let Ok(value) = row.try_get::<i64, _>(field) {
        return Ok(value != 0);
    }
    if let Ok(value) = row.try_get::<i32, _>(field) {
        return Ok(value != 0);
    }
    let value: String = row
        .try_get(field)
        .with_context(|| format!("missing {field}"))?;
    Ok(matches!(value.as_str(), "1" | "true" | "TRUE"))
}

fn parse_datetime_opt(value: Option<String>, field: &str) -> Result<Option<DateTime<Utc>>> {
    match value {
        Some(value) => Ok(Some(parse_datetime(&value, field)?)),
        None => Ok(None),
    }
}

fn parse_datetime(value: &str, field: &str) -> Result<DateTime<Utc>> {
    let value = value.trim();
    let parsed = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f"))
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S"))
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f"))
        .with_context(|| format!("invalid {field} '{value}'"))?;
    Ok(DateTime::<Utc>::from_naive_utc_and_offset(parsed, Utc))
}

fn parse_profile_kind(value: &str) -> Result<DownloadNetworkProfileKind> {
    match value.trim() {
        "external_only" => Ok(DownloadNetworkProfileKind::ExternalOnly),
        "direct" => Ok(DownloadNetworkProfileKind::Direct),
        "cloudflare_warp" => Ok(DownloadNetworkProfileKind::CloudflareWarp),
        "wireguard_config" => Ok(DownloadNetworkProfileKind::WireguardConfig),
        "openvpn_config" => Ok(DownloadNetworkProfileKind::OpenvpnConfig),
        "provider_preset" => Ok(DownloadNetworkProfileKind::ProviderPreset),
        "debrid_only" => Ok(DownloadNetworkProfileKind::DebridOnly),
        other => bail!("invalid download network profile kind '{other}'"),
    }
}

pub(crate) fn profile_kind_as_str(kind: &DownloadNetworkProfileKind) -> &'static str {
    match kind {
        DownloadNetworkProfileKind::ExternalOnly => "external_only",
        DownloadNetworkProfileKind::Direct => "direct",
        DownloadNetworkProfileKind::CloudflareWarp => "cloudflare_warp",
        DownloadNetworkProfileKind::WireguardConfig => "wireguard_config",
        DownloadNetworkProfileKind::OpenvpnConfig => "openvpn_config",
        DownloadNetworkProfileKind::ProviderPreset => "provider_preset",
        DownloadNetworkProfileKind::DebridOnly => "debrid_only",
    }
}

fn parse_protection_state(value: &str) -> DownloadProtectionState {
    match value.trim() {
        "direct" => DownloadProtectionState::Direct,
        "protected" => DownloadProtectionState::Protected,
        "externally_managed" => DownloadProtectionState::ExternallyManaged,
        "blocked" => DownloadProtectionState::Blocked,
        _ => DownloadProtectionState::Unknown,
    }
}

fn mode_for_profile_kind(kind: &DownloadNetworkProfileKind) -> DownloadProtectionMode {
    match kind {
        DownloadNetworkProfileKind::ExternalOnly => DownloadProtectionMode::ExternalOnly,
        DownloadNetworkProfileKind::Direct => DownloadProtectionMode::Direct,
        DownloadNetworkProfileKind::CloudflareWarp => DownloadProtectionMode::CloudflareWarp,
        DownloadNetworkProfileKind::WireguardConfig => DownloadProtectionMode::WireguardConfig,
        DownloadNetworkProfileKind::OpenvpnConfig => DownloadProtectionMode::OpenvpnConfig,
        DownloadNetworkProfileKind::ProviderPreset => DownloadProtectionMode::ProviderPreset,
        DownloadNetworkProfileKind::DebridOnly => DownloadProtectionMode::DebridOnly,
    }
}

fn profile_requires_protected_egress(kind: &DownloadNetworkProfileKind) -> bool {
    matches!(
        kind,
        DownloadNetworkProfileKind::CloudflareWarp
            | DownloadNetworkProfileKind::WireguardConfig
            | DownloadNetworkProfileKind::OpenvpnConfig
            | DownloadNetworkProfileKind::ProviderPreset
    )
}

fn profile_switch_requires_orchestrated_runtime(
    previous: &DownloadNetworkProfileKind,
    target: &DownloadNetworkProfileKind,
) -> bool {
    profile_requires_protected_egress(previous) || profile_requires_protected_egress(target)
}

fn can_activate_without_orchestrated_runtime(kind: DownloadNetworkProfileKind) -> bool {
    matches!(
        kind,
        DownloadNetworkProfileKind::Direct
            | DownloadNetworkProfileKind::ExternalOnly
            | DownloadNetworkProfileKind::DebridOnly
    )
}

fn protected_apps_for_profile(
    profile: &StoredDownloadNetworkProfile,
    inventory: &ManagedDownloaderInventory,
) -> Vec<String> {
    if !profile_requires_protected_egress(&profile.kind) {
        return Vec::new();
    }
    let mut apps = Vec::new();
    let scope = profile.scope.trim();
    if matches!(
        scope,
        "" | "managed_downloaders" | "managed_downloaders_and_indexers"
    ) && inventory.has_qbittorrent()
    {
        apps.push("qbittorrent".to_string());
    }
    if matches!(
        scope,
        "" | "managed_downloaders" | "managed_downloaders_and_indexers"
    ) && inventory.has_nzbget()
    {
        apps.push("nzbget".to_string());
    }
    apps
}

fn wireguard_gateway_image_from_config(config: &serde_json::Value) -> Option<String> {
    config
        .get("wireguardGatewayImage")
        .or_else(|| config.get("gatewayImage"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn openvpn_gateway_image_from_config(config: &serde_json::Value) -> Option<String> {
    config
        .get("openvpnGatewayImage")
        .or_else(|| config.get("gatewayImage"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn warp_gateway_image_from_config(config: &serde_json::Value) -> String {
    config
        .pointer("/cloudflareWarp/gatewayImage")
        .or_else(|| config.get("warpGatewayImage"))
        .or_else(|| config.get("gatewayImage"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(DEFAULT_CLOUDFLARE_WARP_GATEWAY_IMAGE)
        .to_string()
}

pub fn warp_state_volume_name(profile_id: &str) -> String {
    let suffix = profile_id
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches('_')
        .to_string();
    if suffix.is_empty() {
        "elixir_warp_state".to_string()
    } else {
        format!("elixir_warp_state_{suffix}")
    }
}

fn gateway_runtime_check(profile: &StoredDownloadNetworkProfile) -> DownloadProtectionCheck {
    if !profile_requires_protected_egress(&profile.kind) {
        return check(
            "gateway_runtime_supported",
            DownloadProtectionCheckStatus::Warn,
            DownloadProtectionSeverity::Warning,
            "No gateway runtime is required for this profile kind.",
        );
    }

    match profile.kind {
        DownloadNetworkProfileKind::WireguardConfig => {
            let runtime = profile
                .gateway_runtime
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or("gluetun_wireguard");
            if runtime == "gluetun_wireguard" {
                check(
                    "gateway_runtime_supported",
                    DownloadProtectionCheckStatus::Pass,
                    DownloadProtectionSeverity::Info,
                    "The WireGuard profile can compile through the Gluetun gateway runtime.",
                )
            } else {
                check(
                    "gateway_runtime_supported",
                    DownloadProtectionCheckStatus::Fail,
                    DownloadProtectionSeverity::Critical,
                    &format!(
                        "WireGuard profile '{}' uses unsupported gateway runtime '{}'.",
                        profile.id, runtime
                    ),
                )
            }
        }
        DownloadNetworkProfileKind::CloudflareWarp => {
            let runtime = profile
                .gateway_runtime
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or("warp_gateway");
            if runtime == "warp_gateway" {
                check(
                    "gateway_runtime_supported",
                    DownloadProtectionCheckStatus::Pass,
                    DownloadProtectionSeverity::Info,
                    "The Cloudflare WARP profile uses the dedicated WARP gateway runtime contract.",
                )
            } else {
                check(
                    "gateway_runtime_supported",
                    DownloadProtectionCheckStatus::Fail,
                    DownloadProtectionSeverity::Critical,
                    &format!(
                        "Cloudflare WARP profile '{}' uses unsupported gateway runtime '{}'.",
                        profile.id, runtime
                    ),
                )
            }
        }
        DownloadNetworkProfileKind::OpenvpnConfig => {
            let runtime = profile
                .gateway_runtime
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or("gluetun_openvpn");
            if runtime == "gluetun_openvpn" {
                check(
                    "gateway_runtime_supported",
                    DownloadProtectionCheckStatus::Pass,
                    DownloadProtectionSeverity::Info,
                    "The OpenVPN profile can compile through the Gluetun gateway runtime.",
                )
            } else {
                check(
                    "gateway_runtime_supported",
                    DownloadProtectionCheckStatus::Fail,
                    DownloadProtectionSeverity::Critical,
                    &format!(
                        "OpenVPN profile '{}' uses unsupported gateway runtime '{}'.",
                        profile.id, runtime
                    ),
                )
            }
        }
        DownloadNetworkProfileKind::ProviderPreset => check(
            "gateway_runtime_supported",
            DownloadProtectionCheckStatus::Fail,
            DownloadProtectionSeverity::Critical,
            "Provider presets must compile to a concrete gateway runtime before switching.",
        ),
        _ => check(
            "gateway_runtime_supported",
            DownloadProtectionCheckStatus::Warn,
            DownloadProtectionSeverity::Warning,
            "No gateway runtime is required for this profile kind.",
        ),
    }
}

fn warp_profile_switch_checks(
    profile: &StoredDownloadNetworkProfile,
    enrollment: Option<&StoredWarpEnrollment>,
) -> (
    Vec<DownloadProtectionCheck>,
    Option<DownloadProtectionBlocker>,
) {
    let Some(enrollment) = enrollment else {
        return warp_missing_enrollment(profile);
    };

    let (mut checks, blocker) = warp_enrollment_static_checks(enrollment);
    if blocker.is_some() {
        return (checks, blocker);
    }

    match enrollment.status.as_str() {
        "ready" => checks.push(check(
            "warp_runtime_preparable",
            DownloadProtectionCheckStatus::Pass,
            DownloadProtectionSeverity::Info,
            "The WARP gateway enrollment is already marked ready and can be re-applied by the orchestrator.",
        )),
        "pending_runtime" => checks.push(check(
            "warp_runtime_preparable",
            DownloadProtectionCheckStatus::Pass,
            DownloadProtectionSeverity::Info,
            "The WARP enrollment exists; the deterministic orchestrator will start and verify the gateway runtime during apply.",
        )),
        other => checks.push(check(
            "warp_runtime_preparable",
            DownloadProtectionCheckStatus::Warn,
            DownloadProtectionSeverity::Warning,
            &format!(
                "The WARP enrollment is currently '{}'; apply will attempt to recreate and verify the gateway runtime.",
                other
            ),
        )),
    }

    (checks, None)
}

fn warp_profile_checks(
    profile: &StoredDownloadNetworkProfile,
    enrollment: Option<&StoredWarpEnrollment>,
) -> (
    Vec<DownloadProtectionCheck>,
    Option<DownloadProtectionBlocker>,
) {
    let Some(enrollment) = enrollment else {
        return warp_missing_enrollment(profile);
    };

    let (mut checks, static_blocker) = warp_enrollment_static_checks(enrollment);
    checks.push(check(
        "warp_state_volume_configured",
        DownloadProtectionCheckStatus::Pass,
        DownloadProtectionSeverity::Info,
        &format!(
            "The WARP client state is persisted in Docker volume '{}'.",
            warp_state_volume_name(&profile.id)
        ),
    ));
    if static_blocker.is_some() {
        return (checks, static_blocker);
    }

    match enrollment.status.as_str() {
        "ready" => {
            checks.push(check(
                "warp_runtime_health",
                DownloadProtectionCheckStatus::Pass,
                DownloadProtectionSeverity::Info,
                "The WARP gateway enrollment is marked ready.",
            ));
            (checks, None)
        }
        "pending_runtime" => {
            let blocker = DownloadProtectionBlocker {
                code: "warp_runtime_pending".to_string(),
                title: "Cloudflare WARP runtime is pending".to_string(),
                detail: "A per-server WARP profile exists, but the gateway runtime has not been started and verified. Protected downloads remain blocked."
                    .to_string(),
                severity: DownloadProtectionSeverity::Critical,
            };
            checks.push(check(
                "warp_runtime_health",
                DownloadProtectionCheckStatus::Fail,
                DownloadProtectionSeverity::Critical,
                &blocker.detail,
            ));
            (checks, Some(blocker))
        }
        other => {
            let blocker = DownloadProtectionBlocker {
                code: "warp_runtime_unavailable".to_string(),
                title: "Cloudflare WARP runtime is unavailable".to_string(),
                detail: format!(
                    "The WARP enrollment status is '{}'. Protected downloads remain blocked.",
                    other
                ),
                severity: DownloadProtectionSeverity::Critical,
            };
            checks.push(check(
                "warp_runtime_health",
                DownloadProtectionCheckStatus::Fail,
                DownloadProtectionSeverity::Critical,
                &blocker.detail,
            ));
            (checks, Some(blocker))
        }
    }
}

fn warp_missing_enrollment(
    profile: &StoredDownloadNetworkProfile,
) -> (
    Vec<DownloadProtectionCheck>,
    Option<DownloadProtectionBlocker>,
) {
    let blocker = DownloadProtectionBlocker {
        code: "warp_enrollment_missing".to_string(),
        title: "Cloudflare WARP enrollment is missing".to_string(),
        detail: format!(
            "Cloudflare WARP profile '{}' has no per-server enrollment record.",
            profile.id
        ),
        severity: DownloadProtectionSeverity::Critical,
    };
    (
        vec![check(
            "warp_enrollment_present",
            DownloadProtectionCheckStatus::Fail,
            DownloadProtectionSeverity::Critical,
            &blocker.detail,
        )],
        Some(blocker),
    )
}

fn warp_enrollment_static_checks(
    enrollment: &StoredWarpEnrollment,
) -> (
    Vec<DownloadProtectionCheck>,
    Option<DownloadProtectionBlocker>,
) {
    let mut checks = Vec::new();
    checks.push(check(
        "warp_enrollment_present",
        DownloadProtectionCheckStatus::Pass,
        DownloadProtectionSeverity::Info,
        "A per-server Cloudflare WARP enrollment record exists.",
    ));
    checks.push(check(
        "warp_disclosure_accepted",
        if enrollment.disclosure_version == CLOUDFLARE_WARP_DISCLOSURE_VERSION {
            DownloadProtectionCheckStatus::Pass
        } else {
            DownloadProtectionCheckStatus::Warn
        },
        if enrollment.disclosure_version == CLOUDFLARE_WARP_DISCLOSURE_VERSION {
            DownloadProtectionSeverity::Info
        } else {
            DownloadProtectionSeverity::Warning
        },
        if enrollment.disclosure_version == CLOUDFLARE_WARP_DISCLOSURE_VERSION {
            "The current Cloudflare WARP disclosure version was accepted for this server."
        } else {
            "The Cloudflare WARP disclosure version has changed and should be accepted again before activation."
        },
    ));

    let identity_is_scoped = enrollment.identity_secret_ref.starts_with("global:");
    checks.push(check(
        "warp_identity_scoped",
        if identity_is_scoped {
            DownloadProtectionCheckStatus::Pass
        } else {
            DownloadProtectionCheckStatus::Fail
        },
        if identity_is_scoped {
            DownloadProtectionSeverity::Info
        } else {
            DownloadProtectionSeverity::Critical
        },
        if identity_is_scoped {
            "The WARP identity is stored as an encrypted per-server secret reference."
        } else {
            "The WARP identity secret reference is invalid."
        },
    ));

    let blocker = (!identity_is_scoped).then(|| DownloadProtectionBlocker {
        code: "warp_identity_secret_invalid".to_string(),
        title: "Cloudflare WARP identity is invalid".to_string(),
        detail: "The WARP identity secret reference must be scoped to this server's encrypted secret store."
            .to_string(),
        severity: DownloadProtectionSeverity::Critical,
    });
    (checks, blocker)
}

fn warp_identity_secret_key(profile_id: &str) -> String {
    if profile_id == CLOUDFLARE_WARP_PROFILE_ID {
        return CLOUDFLARE_WARP_IDENTITY_SECRET_KEY.to_string();
    }
    let suffix = profile_id
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches('_')
        .to_string();
    if suffix.is_empty() {
        CLOUDFLARE_WARP_IDENTITY_SECRET_KEY.to_string()
    } else {
        format!("{CLOUDFLARE_WARP_IDENTITY_SECRET_KEY}_{suffix}")
    }
}

fn switch_status_as_str(status: DownloadProtectionSwitchStatus) -> &'static str {
    match status {
        DownloadProtectionSwitchStatus::PreflightPassed => "preflight_passed",
        DownloadProtectionSwitchStatus::Blocked => "blocked",
        DownloadProtectionSwitchStatus::Applied => "applied",
    }
}

fn activation_state_for_profile(profile: &StoredDownloadNetworkProfile) -> DownloadProtectionState {
    match profile.kind {
        DownloadNetworkProfileKind::Direct => DownloadProtectionState::Direct,
        DownloadNetworkProfileKind::ExternalOnly | DownloadNetworkProfileKind::DebridOnly => {
            DownloadProtectionState::ExternallyManaged
        }
        _ => DownloadProtectionState::Unknown,
    }
}

fn protection_state_as_str(status: DownloadProtectionState) -> &'static str {
    match status {
        DownloadProtectionState::Direct => "direct",
        DownloadProtectionState::Protected => "protected",
        DownloadProtectionState::ExternallyManaged => "externally_managed",
        DownloadProtectionState::Blocked => "blocked",
        DownloadProtectionState::Unknown => "unknown",
    }
}

impl StoredDownloadNetworkProfile {
    fn validate(&self) -> Result<()> {
        let now = Utc::now();
        DownloadNetworkProfile {
            id: self.id.clone(),
            name: self.name.clone(),
            kind: self.kind.clone(),
            enabled: self.enabled,
            strict: self.strict,
            scope: self.scope.clone(),
            provider: self.provider.clone(),
            gateway_runtime: self.gateway_runtime.clone(),
            config_json: self.config_json.clone(),
            status: self.status.clone(),
            active: true,
            created_at: now,
            updated_at: now,
            last_applied_at: None,
            last_verified_at: None,
        }
        .validate()
    }

    fn summary(&self) -> DownloadProtectionProfileSummary {
        self.summary_with_status(self.status.clone())
    }

    fn summary_with_status(
        &self,
        status: DownloadProtectionState,
    ) -> DownloadProtectionProfileSummary {
        DownloadProtectionProfileSummary {
            id: self.id.clone(),
            name: self.name.clone(),
            kind: self.kind.clone(),
            enabled: self.enabled,
            strict: self.strict,
            scope: self.scope.clone(),
            provider: self.provider.clone(),
            gateway_runtime: self.gateway_runtime.clone(),
            status,
        }
    }
}

impl StoredWarpEnrollment {
    fn status_response(&self) -> CloudflareWarpEnrollmentStatus {
        CloudflareWarpEnrollmentStatus {
            profile_id: self.profile_id.clone(),
            enrollment_id: self.enrollment_id.clone(),
            status: self.status.clone(),
            identity_secret_ref: self.identity_secret_ref.clone(),
            disclosure_version: self.disclosure_version.clone(),
            disclosure_accepted_at: self.disclosure_accepted_at,
            last_checked_at: self.last_checked_at,
            last_error: self.last_error.clone(),
        }
    }
}

fn protected_apps_from_legacy_config(
    vpn: &crate::config::VpnConfig,
    inventory: &ManagedDownloaderInventory,
) -> Vec<String> {
    let mut apps = Vec::new();
    if vpn.auto_wrap_qbittorrent && inventory.has_qbittorrent() {
        apps.push("qbittorrent".to_string());
    }
    if vpn.auto_wrap_nzbget && inventory.has_nzbget() {
        apps.push("nzbget".to_string());
    }
    apps
}

async fn check_wireguard_secret(
    store: &ExtensionStore<'_>,
    secrets: &SecretsManager,
    secret_ref: &str,
    instance_ids: Vec<Uuid>,
) -> Result<SecretCheck> {
    let trimmed = secret_ref.trim();
    let Some((scope, rest)) = trimmed.split_once(':') else {
        return Ok(SecretCheck::InvalidRef(
            "wireguard_config_secret must use global:<key>, instance:<key>, or provider:<uuid>:<key>"
                .to_string(),
        ));
    };

    match scope {
        "global" => check_secret_value(store, secrets, SecretScope::Global, None, rest).await,
        "instance" => {
            if instance_ids.is_empty() {
                return Ok(SecretCheck::Unknown(
                    "No protected managed downloader instances exist yet, so the instance-scoped WireGuard secret cannot be checked.".to_string(),
                ));
            }
            for instance_id in instance_ids {
                match check_secret_value(store, secrets, SecretScope::Instance, Some(instance_id), rest).await? {
                    SecretCheck::Present => {}
                    other => return Ok(other),
                }
            }
            Ok(SecretCheck::Present)
        }
        "provider" => {
            let Some((provider_id, key)) = rest.split_once(':') else {
                return Ok(SecretCheck::InvalidRef(
                    "provider WireGuard secret refs must use provider:<uuid>:<key>".to_string(),
                ));
            };
            let provider_id = Uuid::parse_str(provider_id).map_err(|_| {
                anyhow!("provider WireGuard secret ref contains an invalid provider id")
            })?;
            check_secret_value(store, secrets, SecretScope::Provider, Some(provider_id), key).await
        }
        _ => Ok(SecretCheck::InvalidRef(
            "wireguard_config_secret must use global:<key>, instance:<key>, or provider:<uuid>:<key>"
                .to_string(),
        )),
    }
}

async fn check_profile_secret(
    store: &ExtensionStore<'_>,
    secrets: &SecretsManager,
    secret_ref: &str,
) -> Result<SecretCheck> {
    let trimmed = secret_ref.trim();
    let Some((scope, rest)) = trimmed.split_once(':') else {
        return Ok(SecretCheck::InvalidRef(
            "profile secret refs must use global:<key>, instance:<key>, or provider:<uuid>:<key>"
                .to_string(),
        ));
    };

    match scope {
        "global" => check_secret_value(store, secrets, SecretScope::Global, None, rest).await,
        "instance" => Ok(SecretCheck::InvalidRef(
            "network profile import secrets must not use instance scope".to_string(),
        )),
        "provider" => {
            let Some((provider_id, key)) = rest.split_once(':') else {
                return Ok(SecretCheck::InvalidRef(
                    "provider profile secret refs must use provider:<uuid>:<key>".to_string(),
                ));
            };
            let provider_id = Uuid::parse_str(provider_id).map_err(|_| {
                anyhow!("provider profile secret ref contains an invalid provider id")
            })?;
            check_secret_value(
                store,
                secrets,
                SecretScope::Provider,
                Some(provider_id),
                key,
            )
            .await
        }
        _ => Ok(SecretCheck::InvalidRef(
            "profile secret refs must use global:<key>, instance:<key>, or provider:<uuid>:<key>"
                .to_string(),
        )),
    }
}

async fn check_secret_value(
    store: &ExtensionStore<'_>,
    secrets: &SecretsManager,
    scope: SecretScope,
    scope_id: Option<Uuid>,
    key: &str,
) -> Result<SecretCheck> {
    let trimmed_key = key.trim();
    if trimmed_key.is_empty() {
        return Ok(SecretCheck::InvalidRef(
            "secret reference key is empty".to_string(),
        ));
    }
    let Some(secret) = store.get_secret(scope, scope_id, trimmed_key).await? else {
        return Ok(SecretCheck::Missing(format!(
            "{}:{} is not present",
            scope.as_str(),
            trimmed_key
        )));
    };
    match secrets.decrypt(&secret.value_encrypted) {
        Ok(value) if value.trim().is_empty() => Ok(SecretCheck::Empty(format!(
            "{}:{} decrypted to an empty value",
            scope.as_str(),
            trimmed_key
        ))),
        Ok(_) => Ok(SecretCheck::Present),
        Err(err) => Ok(SecretCheck::Unreadable(format!(
            "{}:{} could not be decrypted: {}",
            scope.as_str(),
            trimmed_key,
            err
        ))),
    }
}

fn profile_secret_status_check(
    secret_check: &SecretCheck,
    secret_ref: &str,
    code: &str,
    label: &str,
) -> DownloadProtectionCheck {
    match secret_check {
        SecretCheck::Present => check(
            code,
            DownloadProtectionCheckStatus::Pass,
            DownloadProtectionSeverity::Info,
            &format!("The configured {label} secret exists and is readable."),
        ),
        SecretCheck::Unknown(detail) => check(
            code,
            DownloadProtectionCheckStatus::Unknown,
            DownloadProtectionSeverity::Warning,
            detail,
        ),
        SecretCheck::Missing(detail) => check(
            code,
            DownloadProtectionCheckStatus::Fail,
            DownloadProtectionSeverity::Critical,
            detail,
        ),
        SecretCheck::Empty(detail) => check(
            code,
            DownloadProtectionCheckStatus::Fail,
            DownloadProtectionSeverity::Critical,
            detail,
        ),
        SecretCheck::Unreadable(detail) => check(
            code,
            DownloadProtectionCheckStatus::Fail,
            DownloadProtectionSeverity::Critical,
            detail,
        ),
        SecretCheck::InvalidRef(detail) => check(
            code,
            DownloadProtectionCheckStatus::Fail,
            DownloadProtectionSeverity::Critical,
            &format!(
                "Invalid {label} secret reference '{}': {}",
                secret_ref, detail
            ),
        ),
    }
}

fn secret_status_check(secret_check: &SecretCheck, secret_ref: &str) -> DownloadProtectionCheck {
    match secret_check {
        SecretCheck::Present => check(
            "wireguard_config_secret",
            DownloadProtectionCheckStatus::Pass,
            DownloadProtectionSeverity::Info,
            "The configured WireGuard secret exists and is readable.",
        ),
        SecretCheck::Unknown(detail) => check(
            "wireguard_config_secret",
            DownloadProtectionCheckStatus::Unknown,
            DownloadProtectionSeverity::Warning,
            detail,
        ),
        SecretCheck::Missing(detail) => check(
            "wireguard_config_secret",
            DownloadProtectionCheckStatus::Fail,
            DownloadProtectionSeverity::Critical,
            detail,
        ),
        SecretCheck::Empty(detail) => check(
            "wireguard_config_secret",
            DownloadProtectionCheckStatus::Fail,
            DownloadProtectionSeverity::Critical,
            detail,
        ),
        SecretCheck::Unreadable(detail) => check(
            "wireguard_config_secret",
            DownloadProtectionCheckStatus::Fail,
            DownloadProtectionSeverity::Critical,
            detail,
        ),
        SecretCheck::InvalidRef(detail) => check(
            "wireguard_config_secret",
            DownloadProtectionCheckStatus::Fail,
            DownloadProtectionSeverity::Critical,
            &format!(
                "Invalid WireGuard secret reference '{}': {}",
                secret_ref, detail
            ),
        ),
    }
}

fn profile_secret_blocker(
    secret_check: &SecretCheck,
    code: &str,
    label: &str,
) -> Option<DownloadProtectionBlocker> {
    match secret_check {
        SecretCheck::Present | SecretCheck::Unknown(_) => None,
        SecretCheck::Missing(detail) => Some(secret_blocker_with_detail(
            &format!("{code}_missing"),
            &format!("{label} secret is missing"),
            detail,
        )),
        SecretCheck::Empty(detail) => Some(secret_blocker_with_detail(
            &format!("{code}_empty"),
            &format!("{label} secret is empty"),
            detail,
        )),
        SecretCheck::Unreadable(detail) => Some(secret_blocker_with_detail(
            &format!("{code}_unreadable"),
            &format!("{label} secret cannot be read"),
            detail,
        )),
        SecretCheck::InvalidRef(detail) => Some(secret_blocker_with_detail(
            &format!("{code}_invalid"),
            &format!("{label} secret reference is invalid"),
            detail,
        )),
    }
}

fn secret_blocker(secret_check: &SecretCheck) -> Option<DownloadProtectionBlocker> {
    match secret_check {
        SecretCheck::Present | SecretCheck::Unknown(_) => None,
        SecretCheck::Missing(detail) => Some(secret_blocker_with_detail(
            "wireguard_config_secret_missing",
            "WireGuard config secret is missing",
            detail,
        )),
        SecretCheck::Empty(detail) => Some(secret_blocker_with_detail(
            "wireguard_config_secret_empty",
            "WireGuard config secret is empty",
            detail,
        )),
        SecretCheck::Unreadable(detail) => Some(secret_blocker_with_detail(
            "wireguard_config_secret_unreadable",
            "WireGuard config secret cannot be read",
            detail,
        )),
        SecretCheck::InvalidRef(detail) => Some(secret_blocker_with_detail(
            "wireguard_config_secret_invalid",
            "WireGuard config secret reference is invalid",
            detail,
        )),
    }
}

fn secret_blocker_with_detail(code: &str, title: &str, detail: &str) -> DownloadProtectionBlocker {
    DownloadProtectionBlocker {
        code: code.to_string(),
        title: title.to_string(),
        detail: detail.to_string(),
        severity: DownloadProtectionSeverity::Critical,
    }
}

fn check(
    code: &str,
    status: DownloadProtectionCheckStatus,
    severity: DownloadProtectionSeverity,
    detail: &str,
) -> DownloadProtectionCheck {
    DownloadProtectionCheck {
        code: code.to_string(),
        status,
        severity,
        detail: detail.to_string(),
    }
}

impl ManagedDownloaderInventory {
    fn from_store(
        instances: &[crate::db::models::ExtensionInstance],
        providers: &[crate::db::models::Provider],
    ) -> Self {
        let enabled_instances = instances
            .iter()
            .filter(|instance| instance.enabled)
            .map(|instance| (instance.instance_id, instance.extension_id.as_str()))
            .collect::<HashMap<_, _>>();
        let mut qbittorrent_instance_ids = instances
            .iter()
            .filter(|instance| {
                instance.enabled && is_qbittorrent_extension_id(&instance.extension_id)
            })
            .map(|instance| instance.instance_id)
            .collect::<Vec<_>>();
        let mut nzbget_instance_ids = instances
            .iter()
            .filter(|instance| instance.enabled && is_nzbget_extension_id(&instance.extension_id))
            .map(|instance| instance.instance_id)
            .collect::<Vec<_>>();
        let mut external_provider_ids = HashSet::new();

        for provider in providers
            .iter()
            .filter(|provider| provider.capability.starts_with("downloader."))
        {
            let instance_extension_id = enabled_instances.get(&provider.instance_id).copied();
            let is_managed_qbittorrent = provider.implementation.as_deref() == Some("qbittorrent")
                || instance_extension_id.is_some_and(is_qbittorrent_extension_id);
            let is_managed_nzbget = provider.implementation.as_deref() == Some("nzbget")
                || instance_extension_id.is_some_and(is_nzbget_extension_id);

            if is_managed_qbittorrent {
                qbittorrent_instance_ids.push(provider.instance_id);
            } else if is_managed_nzbget {
                nzbget_instance_ids.push(provider.instance_id);
            } else if instance_extension_id.is_some() {
                external_provider_ids.insert(provider.provider_id);
            }
        }

        qbittorrent_instance_ids.sort();
        qbittorrent_instance_ids.dedup();
        nzbget_instance_ids.sort();
        nzbget_instance_ids.dedup();

        Self {
            qbittorrent_instance_ids,
            nzbget_instance_ids,
            external_count: external_provider_ids.len(),
        }
    }

    fn has_qbittorrent(&self) -> bool {
        !self.qbittorrent_instance_ids.is_empty()
    }

    fn has_nzbget(&self) -> bool {
        !self.nzbget_instance_ids.is_empty()
    }

    fn has_managed(&self) -> bool {
        self.has_qbittorrent() || self.has_nzbget()
    }

    fn protected_instance_ids(&self, include_qbittorrent: bool, include_nzbget: bool) -> Vec<Uuid> {
        let mut ids = Vec::new();
        if include_qbittorrent {
            ids.extend(self.qbittorrent_instance_ids.iter().copied());
        }
        if include_nzbget {
            ids.extend(self.nzbget_instance_ids.iter().copied());
        }
        ids.sort();
        ids.dedup();
        ids
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{NetworkConfig, VpnConfig};
    use crate::db::models::{
        ExtensionKind, ExtensionTrustLevel, ProviderHealthState, SecretScope, SlotCardinality,
    };
    use crate::extensions::store::{NewExtension, NewExtensionInstance, NewProvider, NewSecret};
    use crate::orchestrator::model::ProviderEndpoint;
    use crate::runtime::model::PortMapping;
    use serde_json::json;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    fn test_settings(vpn: VpnConfig) -> Settings {
        let mut settings = Settings::default();
        settings.network = NetworkConfig {
            vpn,
            ..NetworkConfig::default()
        };
        settings
    }

    #[tokio::test]
    async fn legacy_direct_status_reports_managed_downloaders_as_direct() -> Result<()> {
        let settings = test_settings(VpnConfig::default());
        let inventory = ManagedDownloaderInventory {
            qbittorrent_instance_ids: vec![Uuid::new_v4()],
            nzbget_instance_ids: Vec::new(),
            external_count: 0,
        };
        let database = test_database().await?;
        let store = ExtensionStore::new(&database.pool);
        let secrets = SecretsManager::from_key_bytes([1u8; 32], true);

        let status = status_from_legacy_config(&settings, &inventory, &store, &secrets).await?;
        assert_eq!(status.mode, DownloadProtectionMode::Direct);
        assert_eq!(status.state, DownloadProtectionState::Direct);
        assert_eq!(status.managed_downloaders.qbittorrent, true);
        assert!(status.protected_apps.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn legacy_wireguard_missing_secret_reports_blocked() -> Result<()> {
        let mut vpn = VpnConfig::default();
        vpn.enabled = true;
        let settings = test_settings(vpn);
        let inventory = ManagedDownloaderInventory {
            qbittorrent_instance_ids: vec![Uuid::new_v4()],
            nzbget_instance_ids: Vec::new(),
            external_count: 0,
        };
        let database = test_database().await?;
        let store = ExtensionStore::new(&database.pool);
        let secrets = SecretsManager::from_key_bytes([2u8; 32], true);

        let status = status_from_legacy_config(&settings, &inventory, &store, &secrets).await?;
        assert_eq!(status.mode, DownloadProtectionMode::WireguardConfig);
        assert_eq!(status.state, DownloadProtectionState::Blocked);
        assert_eq!(
            status.blocker.as_ref().map(|blocker| blocker.code.as_str()),
            Some("wireguard_config_secret_missing")
        );
        assert_eq!(status.protected_apps, vec!["qbittorrent".to_string()]);
        Ok(())
    }

    #[test]
    fn legacy_profile_projection_maps_legacy_settings_to_profile_kinds() {
        let settings = test_settings(VpnConfig::default());
        let external = legacy_profile_from_config(
            &settings,
            &ManagedDownloaderInventory {
                qbittorrent_instance_ids: Vec::new(),
                nzbget_instance_ids: Vec::new(),
                external_count: 0,
            },
        );
        assert_eq!(external.kind, DownloadNetworkProfileKind::ExternalOnly);
        assert_eq!(external.status, DownloadProtectionState::Unknown);

        let direct = legacy_profile_from_config(
            &settings,
            &ManagedDownloaderInventory {
                qbittorrent_instance_ids: vec![Uuid::new_v4()],
                nzbget_instance_ids: Vec::new(),
                external_count: 0,
            },
        );
        assert_eq!(direct.kind, DownloadNetworkProfileKind::Direct);
        assert_eq!(direct.status, DownloadProtectionState::Direct);

        let mut vpn = VpnConfig::default();
        vpn.enabled = true;
        let protected = legacy_profile_from_config(
            &test_settings(vpn),
            &ManagedDownloaderInventory {
                qbittorrent_instance_ids: vec![Uuid::new_v4()],
                nzbget_instance_ids: Vec::new(),
                external_count: 0,
            },
        );
        assert_eq!(protected.kind, DownloadNetworkProfileKind::WireguardConfig);
        assert!(protected.strict);
        assert_eq!(
            protected.gateway_runtime.as_deref(),
            Some("gluetun_wireguard")
        );
    }

    #[test]
    fn profile_validation_rejects_invalid_scope_and_loose_protected_profiles() -> Result<()> {
        let now = Utc::now();
        let profile_model = DownloadNetworkProfile {
            id: "direct".to_string(),
            name: "Direct".to_string(),
            kind: DownloadNetworkProfileKind::Direct,
            enabled: true,
            strict: false,
            scope: "managed_downloaders".to_string(),
            provider: None,
            gateway_runtime: None,
            config_json: json!({}),
            status: DownloadProtectionState::Direct,
            active: true,
            created_at: now,
            updated_at: now,
            last_applied_at: None,
            last_verified_at: None,
        };
        profile_model.validate()?;

        let mut profile = StoredDownloadNetworkProfile {
            id: "wg".to_string(),
            name: "WireGuard".to_string(),
            kind: DownloadNetworkProfileKind::WireguardConfig,
            enabled: true,
            strict: true,
            scope: "everything".to_string(),
            provider: None,
            gateway_runtime: Some("gluetun_wireguard".to_string()),
            config_json: json!({}),
            status: DownloadProtectionState::Unknown,
        };
        let err = profile.validate().expect_err("invalid scope should fail");
        assert!(err.to_string().contains("unsupported scope"));

        profile.scope = "managed_downloaders".to_string();
        profile.strict = false;
        let err = profile
            .validate()
            .expect_err("protected profiles must be strict");
        assert!(err.to_string().contains("requires strict mode"));
        Ok(())
    }

    #[tokio::test]
    async fn stored_direct_profile_reports_direct_status() -> Result<()> {
        let settings = test_settings(VpnConfig::default());
        let database = test_database().await?;
        let secrets = SecretsManager::from_key_bytes([20u8; 32], true);
        insert_profile(
            &database.pool,
            "direct",
            "Direct",
            "direct",
            true,
            true,
            None,
        )
        .await?;

        let status =
            observed_download_protection_status(&settings, &database.pool, &secrets).await?;
        assert_eq!(status.mode, DownloadProtectionMode::Direct);
        assert_eq!(status.state, DownloadProtectionState::Direct);
        assert!(status.blocker.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn stored_external_only_profile_reports_unknown_without_external_provider() -> Result<()>
    {
        let settings = test_settings(VpnConfig::default());
        let database = test_database().await?;
        let secrets = SecretsManager::from_key_bytes([21u8; 32], true);
        insert_profile(
            &database.pool,
            "external",
            "External Only",
            "external_only",
            true,
            true,
            None,
        )
        .await?;

        let status =
            observed_download_protection_status(&settings, &database.pool, &secrets).await?;
        assert_eq!(status.mode, DownloadProtectionMode::ExternalOnly);
        assert_eq!(status.state, DownloadProtectionState::Unknown);
        assert!(status.blocker.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn stored_wireguard_profile_missing_secret_reports_blocked() -> Result<()> {
        let settings = test_settings(VpnConfig::default());
        let database = test_database().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_qbittorrent_instance(&store).await?;
        let secrets = SecretsManager::from_key_bytes([22u8; 32], true);
        insert_profile(
            &database.pool,
            "wg",
            "WireGuard",
            "wireguard_config",
            true,
            true,
            Some("gluetun_wireguard"),
        )
        .await?;
        insert_profile_secret(
            &database.pool,
            "wg",
            "wireguard_config",
            "global:wireguard_config",
        )
        .await?;

        let status =
            observed_download_protection_status(&settings, &database.pool, &secrets).await?;
        assert_eq!(status.mode, DownloadProtectionMode::WireguardConfig);
        assert_eq!(status.state, DownloadProtectionState::Blocked);
        assert_eq!(
            status.blocker.as_ref().map(|blocker| blocker.code.as_str()),
            Some("wireguard_config_secret_missing")
        );
        assert_eq!(status.protected_apps, vec!["qbittorrent".to_string()]);
        Ok(())
    }

    #[tokio::test]
    async fn runtime_evidence_blocks_active_profile_when_downloader_ip_matches_server() -> Result<()>
    {
        let settings = test_settings(VpnConfig::default());
        let database = test_database().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_qbittorrent_instance(&store).await?;
        let secrets = SecretsManager::from_key_bytes([23u8; 32], true);
        insert_global_secret(
            &store,
            &secrets,
            "wireguard_config",
            "[Interface]\nPrivateKey=x",
        )
        .await?;
        insert_profile(
            &database.pool,
            "wg",
            "WireGuard",
            "wireguard_config",
            true,
            true,
            Some("gluetun_wireguard"),
        )
        .await?;
        insert_profile_secret(
            &database.pool,
            "wg",
            "wireguard_config",
            "global:wireguard_config",
        )
        .await?;
        let evidence = DownloadProtectionRuntimeEvidence {
            server_public_ip: Some(test_evidence_pass("203.0.113.10")),
            gateway_public_ip: Some(test_evidence_pass("198.51.100.22")),
            downloader_public_ip: Some(test_evidence_pass("203.0.113.10")),
            gateway_dns: Some(test_evidence_pass("1.1.1.1")),
            downloader_dns: Some(test_evidence_pass("1.1.1.1")),
            kill_switch: Some(test_evidence_pass("container:gateway")),
        };

        let status = observed_download_protection_status_with_evidence(
            &settings,
            &database.pool,
            &secrets,
            Some(&evidence),
        )
        .await?;

        assert_eq!(status.state, DownloadProtectionState::Blocked);
        assert_eq!(
            status.blocker.as_ref().map(|blocker| blocker.code.as_str()),
            Some("download_network_leak_detected")
        );
        assert!(status.checks.iter().any(|check| {
            check.code == "downloader_ip_differs_from_server"
                && check.status == DownloadProtectionCheckStatus::Fail
        }));
        Ok(())
    }

    #[tokio::test]
    async fn switch_runtime_evidence_blocks_failed_kill_switch() -> Result<()> {
        let settings = test_settings(VpnConfig::default());
        let database = test_database().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_qbittorrent_instance(&store).await?;
        let secrets = SecretsManager::from_key_bytes([24u8; 32], true);
        insert_global_secret(
            &store,
            &secrets,
            "wireguard_config",
            "[Interface]\nPrivateKey=x",
        )
        .await?;
        insert_profile(
            &database.pool,
            "wg",
            "WireGuard",
            "wireguard_config",
            true,
            false,
            Some("gluetun_wireguard"),
        )
        .await?;
        insert_profile_secret(
            &database.pool,
            "wg",
            "wireguard_config",
            "global:wireguard_config",
        )
        .await?;
        let evidence = DownloadProtectionRuntimeEvidence {
            server_public_ip: Some(test_evidence_pass("203.0.113.10")),
            gateway_public_ip: Some(test_evidence_pass("198.51.100.22")),
            downloader_public_ip: Some(test_evidence_pass("198.51.100.22")),
            gateway_dns: Some(test_evidence_pass("1.1.1.1")),
            downloader_dns: Some(test_evidence_pass("1.1.1.1")),
            kill_switch: Some(test_evidence_fail("downloader is direct")),
        };

        let response = switch_download_protection_profile(
            &settings,
            &database.pool,
            &secrets,
            DownloadProtectionSwitchRequest {
                target_profile_id: "wg".to_string(),
                apply: true,
                expected_active_profile_id: None,
                server_public_ip: None,
                downloader_public_ip: None,
                runtime_evidence: Some(evidence),
            },
        )
        .await?;

        assert_eq!(response.status, DownloadProtectionSwitchStatus::Blocked);
        assert_eq!(
            response
                .blocker
                .as_ref()
                .map(|blocker| blocker.code.as_str()),
            Some("download_network_kill_switch_failed")
        );
        assert_eq!(active_profile_count(&database.pool).await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn switch_blocks_protected_profile_when_public_ips_match() -> Result<()> {
        let settings = test_settings(VpnConfig::default());
        let database = test_database().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_qbittorrent_instance(&store).await?;
        let secrets = SecretsManager::from_key_bytes([3u8; 32], true);
        insert_global_secret(
            &store,
            &secrets,
            "wireguard_config",
            "[Interface]\nPrivateKey=x",
        )
        .await?;
        insert_profile(
            &database.pool,
            "wg",
            "WireGuard",
            "wireguard_config",
            true,
            false,
            Some("gluetun_wireguard"),
        )
        .await?;
        insert_profile_secret(
            &database.pool,
            "wg",
            "wireguard_config",
            "global:wireguard_config",
        )
        .await?;

        let response = switch_download_protection_profile(
            &settings,
            &database.pool,
            &secrets,
            DownloadProtectionSwitchRequest {
                target_profile_id: "wg".to_string(),
                apply: true,
                expected_active_profile_id: None,
                server_public_ip: Some("203.0.113.10".to_string()),
                downloader_public_ip: Some("203.0.113.10".to_string()),
                runtime_evidence: None,
            },
        )
        .await?;

        assert_eq!(response.status, DownloadProtectionSwitchStatus::Blocked);
        assert!(!response.applied);
        assert_eq!(
            response
                .blocker
                .as_ref()
                .map(|blocker| blocker.code.as_str()),
            Some("download_network_leak_detected")
        );
        assert_eq!(active_profile_count(&database.pool).await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn switch_activates_direct_profile_without_rehome() -> Result<()> {
        let settings = test_settings(VpnConfig::default());
        let database = test_database().await?;
        let secrets = SecretsManager::from_key_bytes([4u8; 32], true);
        insert_profile(
            &database.pool,
            "direct",
            "Direct",
            "direct",
            true,
            false,
            None,
        )
        .await?;

        let response = switch_download_protection_profile(
            &settings,
            &database.pool,
            &secrets,
            DownloadProtectionSwitchRequest {
                target_profile_id: "direct".to_string(),
                apply: true,
                expected_active_profile_id: None,
                server_public_ip: None,
                downloader_public_ip: None,
                runtime_evidence: None,
            },
        )
        .await?;

        assert_eq!(response.status, DownloadProtectionSwitchStatus::Applied);
        assert!(response.applied);
        assert_eq!(active_profile_count(&database.pool).await?, 1);

        let status =
            observed_download_protection_status(&settings, &database.pool, &secrets).await?;
        assert_eq!(status.active_profile.id, "direct");
        assert_eq!(status.mode, DownloadProtectionMode::Direct);
        assert_eq!(status.state, DownloadProtectionState::Direct);
        Ok(())
    }

    #[tokio::test]
    async fn non_orchestrated_switch_from_protected_to_direct_is_blocked() -> Result<()> {
        let settings = test_settings(VpnConfig::default());
        let database = test_database().await?;
        let secrets = SecretsManager::from_key_bytes([44u8; 32], true);
        ensure_cloudflare_warp_profile(
            &database.pool,
            &secrets,
            CloudflareWarpProfileRequest {
                accepted_disclosure: true,
                profile_id: None,
                name: None,
            },
        )
        .await?;
        let warp = load_download_network_profile(&database.pool, CLOUDFLARE_WARP_PROFILE_ID)
            .await?
            .expect("warp profile");
        activate_download_network_profile(&database.pool, &warp).await?;
        insert_profile(
            &database.pool,
            "direct",
            "Direct",
            "direct",
            true,
            false,
            None,
        )
        .await?;

        let response = switch_download_protection_profile(
            &settings,
            &database.pool,
            &secrets,
            DownloadProtectionSwitchRequest {
                target_profile_id: "direct".to_string(),
                apply: true,
                expected_active_profile_id: Some(CLOUDFLARE_WARP_PROFILE_ID.to_string()),
                server_public_ip: None,
                downloader_public_ip: None,
                runtime_evidence: None,
            },
        )
        .await?;

        assert_eq!(response.status, DownloadProtectionSwitchStatus::Blocked);
        assert!(!response.applied);
        assert_eq!(
            response
                .blocker
                .as_ref()
                .map(|blocker| blocker.code.as_str()),
            Some("profile_switch_orchestrator_apply_not_enabled")
        );
        let status =
            observed_download_protection_status(&settings, &database.pool, &secrets).await?;
        assert_eq!(status.active_profile.id, CLOUDFLARE_WARP_PROFILE_ID);
        Ok(())
    }

    #[tokio::test]
    async fn protected_profile_apply_waits_for_orchestrator_path() -> Result<()> {
        let settings = test_settings(VpnConfig::default());
        let database = test_database().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_qbittorrent_instance(&store).await?;
        let secrets = SecretsManager::from_key_bytes([5u8; 32], true);
        insert_global_secret(
            &store,
            &secrets,
            "wireguard_config",
            "[Interface]\nPrivateKey=x",
        )
        .await?;
        insert_profile(
            &database.pool,
            "wg",
            "WireGuard",
            "wireguard_config",
            true,
            false,
            Some("gluetun_wireguard"),
        )
        .await?;
        insert_profile_secret(
            &database.pool,
            "wg",
            "wireguard_config",
            "global:wireguard_config",
        )
        .await?;

        let response = switch_download_protection_profile(
            &settings,
            &database.pool,
            &secrets,
            DownloadProtectionSwitchRequest {
                target_profile_id: "wg".to_string(),
                apply: true,
                expected_active_profile_id: None,
                server_public_ip: Some("203.0.113.10".to_string()),
                downloader_public_ip: Some("198.51.100.22".to_string()),
                runtime_evidence: None,
            },
        )
        .await?;

        assert_eq!(response.status, DownloadProtectionSwitchStatus::Blocked);
        assert!(!response.applied);
        assert_eq!(
            response
                .blocker
                .as_ref()
                .map(|blocker| blocker.code.as_str()),
            Some("profile_switch_orchestrator_apply_not_enabled")
        );
        assert_eq!(active_profile_count(&database.pool).await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn orchestrated_switch_activates_wireguard_profile_and_records_event() -> Result<()> {
        let settings = test_settings(VpnConfig::default());
        let database = test_database().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_qbittorrent_instance(&store).await?;
        let secrets = SecretsManager::from_key_bytes([5u8; 32], true);
        insert_global_secret(
            &store,
            &secrets,
            "wireguard_config",
            "[Interface]\nPrivateKey=x",
        )
        .await?;
        insert_profile(
            &database.pool,
            "wg",
            "WireGuard",
            "wireguard_config",
            true,
            false,
            Some("gluetun_wireguard"),
        )
        .await?;
        insert_profile_secret(
            &database.pool,
            "wg",
            "wireguard_config",
            "global:wireguard_config",
        )
        .await?;

        let response = switch_download_protection_profile_with_orchestrated_apply(
            &settings,
            &database.pool,
            &secrets,
            DownloadProtectionSwitchRequest {
                target_profile_id: "wg".to_string(),
                apply: true,
                expected_active_profile_id: None,
                server_public_ip: Some("203.0.113.10".to_string()),
                downloader_public_ip: Some("203.0.113.10".to_string()),
                runtime_evidence: Some(test_runtime_evidence_leak()),
            },
            || async { Ok(()) },
            || async { Ok(test_runtime_evidence_pass()) },
        )
        .await?;

        assert_eq!(response.status, DownloadProtectionSwitchStatus::Applied);
        assert!(response.applied);
        assert!(response.blocker.is_none());
        assert!(response.runtime_evidence.is_some());
        assert!(response.phases.iter().any(|phase| {
            phase.id == "rehome_protected_apps"
                && phase.status == DownloadProtectionSwitchPhaseStatus::Pass
        }));
        assert!(response.phases.iter().any(|phase| {
            phase.id == "verify_protected_apps"
                && phase.status == DownloadProtectionSwitchPhaseStatus::Pass
        }));
        let status =
            observed_download_protection_status(&settings, &database.pool, &secrets).await?;
        assert_eq!(status.active_profile.id, "wg");
        assert_eq!(status.state, DownloadProtectionState::Protected);

        let events = list_download_network_events(&database.pool, 10).await?;
        assert_eq!(
            events.first().map(|event| event.status.as_str()),
            Some("applied")
        );
        assert_eq!(
            events.first().and_then(|event| event.profile_id.as_deref()),
            Some("wg")
        );
        Ok(())
    }

    #[tokio::test]
    async fn orchestrated_switch_rehomes_from_warp_profile_to_direct() -> Result<()> {
        let settings = test_settings(VpnConfig::default());
        let database = test_database().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_qbittorrent_instance(&store).await?;
        let secrets = SecretsManager::from_key_bytes([31u8; 32], true);
        ensure_cloudflare_warp_profile(
            &database.pool,
            &secrets,
            CloudflareWarpProfileRequest {
                accepted_disclosure: true,
                profile_id: None,
                name: None,
            },
        )
        .await?;
        let warp = load_download_network_profile(&database.pool, CLOUDFLARE_WARP_PROFILE_ID)
            .await?
            .expect("warp profile");
        activate_download_network_profile(&database.pool, &warp).await?;
        mark_cloudflare_warp_runtime_ready(&database.pool, CLOUDFLARE_WARP_PROFILE_ID).await?;
        insert_profile(
            &database.pool,
            "direct",
            "Direct",
            "direct",
            true,
            false,
            None,
        )
        .await?;

        let apply_calls = Arc::new(AtomicUsize::new(0));
        let apply_calls_for_closure = apply_calls.clone();
        let response = switch_download_protection_profile_with_orchestrated_apply(
            &settings,
            &database.pool,
            &secrets,
            DownloadProtectionSwitchRequest {
                target_profile_id: "direct".to_string(),
                apply: true,
                expected_active_profile_id: Some(CLOUDFLARE_WARP_PROFILE_ID.to_string()),
                server_public_ip: None,
                downloader_public_ip: None,
                runtime_evidence: None,
            },
            move || {
                let apply_calls = apply_calls_for_closure.clone();
                async move {
                    apply_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
            || async {
                anyhow::bail!("direct target should not collect protected runtime evidence")
            },
        )
        .await?;

        assert_eq!(response.status, DownloadProtectionSwitchStatus::Applied);
        assert!(response.applied);
        assert_eq!(apply_calls.load(Ordering::SeqCst), 1);
        assert!(response.runtime_evidence.is_none());
        assert!(response.phases.iter().any(|phase| {
            phase.id == "rehome_protected_apps"
                && phase.status == DownloadProtectionSwitchPhaseStatus::Pass
        }));
        assert!(response.phases.iter().any(|phase| {
            phase.id == "verify_protected_apps"
                && phase.status == DownloadProtectionSwitchPhaseStatus::Pass
        }));

        let status =
            observed_download_protection_status(&settings, &database.pool, &secrets).await?;
        assert_eq!(status.active_profile.id, "direct");
        assert_eq!(status.state, DownloadProtectionState::Direct);

        let routes = list_acquisition_routes(&database.pool, &store).await?;
        for logical_id in [TORRENT_DEFAULT_LOGICAL_ID, USENET_DEFAULT_LOGICAL_ID] {
            let route = routes
                .routes
                .iter()
                .find(|route| route.logical_id == logical_id && route.owner_id == "default")
                .expect("default direct route");
            assert_eq!(route.binding_kind, DownloadBrokerBindingKind::ManagedDirect);
            assert!(route.profile_id.is_none());
        }
        Ok(())
    }

    #[tokio::test]
    async fn orchestrated_switch_rehomes_from_warp_profile_to_wireguard() -> Result<()> {
        let settings = test_settings(VpnConfig::default());
        let database = test_database().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_qbittorrent_instance(&store).await?;
        let secrets = SecretsManager::from_key_bytes([34u8; 32], true);
        ensure_cloudflare_warp_profile(
            &database.pool,
            &secrets,
            CloudflareWarpProfileRequest {
                accepted_disclosure: true,
                profile_id: None,
                name: None,
            },
        )
        .await?;
        let warp = load_download_network_profile(&database.pool, CLOUDFLARE_WARP_PROFILE_ID)
            .await?
            .expect("warp profile");
        activate_download_network_profile(&database.pool, &warp).await?;
        mark_cloudflare_warp_runtime_ready(&database.pool, CLOUDFLARE_WARP_PROFILE_ID).await?;
        insert_global_secret(
            &store,
            &secrets,
            "wireguard_config",
            "[Interface]\nPrivateKey=x",
        )
        .await?;
        insert_profile(
            &database.pool,
            "wg",
            "WireGuard",
            "wireguard_config",
            true,
            false,
            Some("gluetun_wireguard"),
        )
        .await?;
        insert_profile_secret(
            &database.pool,
            "wg",
            "wireguard_config",
            "global:wireguard_config",
        )
        .await?;

        let apply_calls = Arc::new(AtomicUsize::new(0));
        let apply_calls_for_closure = apply_calls.clone();
        let response = switch_download_protection_profile_with_orchestrated_apply(
            &settings,
            &database.pool,
            &secrets,
            DownloadProtectionSwitchRequest {
                target_profile_id: "wg".to_string(),
                apply: true,
                expected_active_profile_id: Some(CLOUDFLARE_WARP_PROFILE_ID.to_string()),
                server_public_ip: None,
                downloader_public_ip: None,
                runtime_evidence: None,
            },
            move || {
                let apply_calls = apply_calls_for_closure.clone();
                async move {
                    apply_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
            || async { Ok(test_runtime_evidence_pass()) },
        )
        .await?;

        assert_eq!(response.status, DownloadProtectionSwitchStatus::Applied);
        assert!(response.applied);
        assert_eq!(apply_calls.load(Ordering::SeqCst), 1);
        assert!(response.runtime_evidence.is_some());
        let status =
            observed_download_protection_status(&settings, &database.pool, &secrets).await?;
        assert_eq!(status.active_profile.id, "wg");
        assert_eq!(status.state, DownloadProtectionState::Protected);
        Ok(())
    }

    #[tokio::test]
    async fn debrid_only_switch_updates_routes_without_managed_downloader_rehome() -> Result<()> {
        let settings = test_settings(VpnConfig::default());
        let database = test_database().await?;
        let secrets = SecretsManager::from_key_bytes([32u8; 32], true);
        insert_profile(
            &database.pool,
            "direct",
            "Direct",
            "direct",
            true,
            true,
            None,
        )
        .await?;
        insert_profile(
            &database.pool,
            "debrid-only",
            "Debrid Only",
            "debrid_only",
            true,
            false,
            None,
        )
        .await?;

        let response = switch_download_protection_profile_with_orchestrated_apply(
            &settings,
            &database.pool,
            &secrets,
            DownloadProtectionSwitchRequest {
                target_profile_id: "debrid-only".to_string(),
                apply: true,
                expected_active_profile_id: Some("direct".to_string()),
                server_public_ip: None,
                downloader_public_ip: None,
                runtime_evidence: None,
            },
            || async { anyhow::bail!("debrid-only should not rehome managed downloaders") },
            || async { anyhow::bail!("debrid-only should not collect protected runtime evidence") },
        )
        .await?;

        assert_eq!(response.status, DownloadProtectionSwitchStatus::Applied);
        assert!(response.applied);
        let store = ExtensionStore::new(&database.pool);
        let routes = list_acquisition_routes(&database.pool, &store).await?;
        for logical_id in [TORRENT_DEFAULT_LOGICAL_ID, USENET_DEFAULT_LOGICAL_ID] {
            let route = routes
                .routes
                .iter()
                .find(|route| route.logical_id == logical_id && route.owner_id == "default")
                .expect("default non-debrid route");
            assert_eq!(route.binding_kind, DownloadBrokerBindingKind::External);
        }
        let debrid_route = routes
            .routes
            .iter()
            .find(|route| {
                route.logical_id == DEBRID_DEFAULT_LOGICAL_ID && route.owner_id == "default"
            })
            .expect("default debrid route");
        assert_eq!(debrid_route.binding_kind, DownloadBrokerBindingKind::Debrid);
        assert_eq!(debrid_route.profile_id.as_deref(), Some("debrid-only"));
        Ok(())
    }

    #[tokio::test]
    async fn failed_orchestrated_switch_restores_previous_profile() -> Result<()> {
        let settings = test_settings(VpnConfig::default());
        let database = test_database().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_qbittorrent_instance(&store).await?;
        let secrets = SecretsManager::from_key_bytes([5u8; 32], true);
        insert_global_secret(
            &store,
            &secrets,
            "wireguard_config",
            "[Interface]\nPrivateKey=x",
        )
        .await?;
        insert_profile(
            &database.pool,
            "direct",
            "Direct",
            "direct",
            true,
            true,
            None,
        )
        .await?;
        insert_profile(
            &database.pool,
            "wg",
            "WireGuard",
            "wireguard_config",
            true,
            false,
            Some("gluetun_wireguard"),
        )
        .await?;
        insert_profile_secret(
            &database.pool,
            "wg",
            "wireguard_config",
            "global:wireguard_config",
        )
        .await?;

        let apply_calls = Arc::new(AtomicUsize::new(0));
        let apply_calls_for_closure = apply_calls.clone();
        let response = switch_download_protection_profile_with_orchestrated_apply(
            &settings,
            &database.pool,
            &secrets,
            DownloadProtectionSwitchRequest {
                target_profile_id: "wg".to_string(),
                apply: true,
                expected_active_profile_id: Some("direct".to_string()),
                server_public_ip: Some("203.0.113.10".to_string()),
                downloader_public_ip: Some("198.51.100.22".to_string()),
                runtime_evidence: None,
            },
            move || {
                let apply_calls = apply_calls_for_closure.clone();
                async move {
                    if apply_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        Err(anyhow::anyhow!("simulated rehome failure"))
                    } else {
                        Ok(())
                    }
                }
            },
            || async { Ok(test_runtime_evidence_pass()) },
        )
        .await?;

        assert_eq!(response.status, DownloadProtectionSwitchStatus::Blocked);
        assert!(!response.applied);
        assert_eq!(apply_calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            response
                .blocker
                .as_ref()
                .map(|blocker| blocker.code.as_str()),
            Some("profile_switch_orchestrator_apply_failed")
        );
        let status =
            observed_download_protection_status(&settings, &database.pool, &secrets).await?;
        assert_eq!(status.active_profile.id, "direct");

        let events = list_download_network_events(&database.pool, 10).await?;
        assert_eq!(
            events.first().map(|event| event.status.as_str()),
            Some("blocked")
        );
        assert_eq!(
            events
                .first()
                .and_then(|event| event.evidence.pointer("/blocker/code"))
                .and_then(serde_json::Value::as_str),
            Some("profile_switch_orchestrator_apply_failed")
        );
        Ok(())
    }

    #[tokio::test]
    async fn post_apply_evidence_failure_restores_previous_profile_and_reapplies_orchestrator()
    -> Result<()> {
        let settings = test_settings(VpnConfig::default());
        let database = test_database().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_qbittorrent_instance(&store).await?;
        let secrets = SecretsManager::from_key_bytes([25u8; 32], true);
        insert_global_secret(
            &store,
            &secrets,
            "wireguard_config",
            "[Interface]\nPrivateKey=x",
        )
        .await?;
        insert_profile(
            &database.pool,
            "direct",
            "Direct",
            "direct",
            true,
            true,
            None,
        )
        .await?;
        insert_profile(
            &database.pool,
            "wg",
            "WireGuard",
            "wireguard_config",
            true,
            false,
            Some("gluetun_wireguard"),
        )
        .await?;
        insert_profile_secret(
            &database.pool,
            "wg",
            "wireguard_config",
            "global:wireguard_config",
        )
        .await?;

        let apply_calls = Arc::new(AtomicUsize::new(0));
        let apply_calls_for_closure = apply_calls.clone();
        let response = switch_download_protection_profile_with_orchestrated_apply(
            &settings,
            &database.pool,
            &secrets,
            DownloadProtectionSwitchRequest {
                target_profile_id: "wg".to_string(),
                apply: true,
                expected_active_profile_id: Some("direct".to_string()),
                server_public_ip: None,
                downloader_public_ip: None,
                runtime_evidence: None,
            },
            move || {
                let apply_calls = apply_calls_for_closure.clone();
                async move {
                    apply_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
            || async { Ok(test_runtime_evidence_leak()) },
        )
        .await?;

        assert_eq!(response.status, DownloadProtectionSwitchStatus::Blocked);
        assert!(!response.applied);
        assert_eq!(apply_calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            response
                .blocker
                .as_ref()
                .map(|blocker| blocker.code.as_str()),
            Some("download_network_leak_detected")
        );
        assert!(response.runtime_evidence.is_some());
        assert!(response.phases.iter().any(|phase| {
            phase.id == "verify_protected_apps"
                && phase.status == DownloadProtectionSwitchPhaseStatus::Blocked
        }));
        assert!(response.phases.iter().any(|phase| {
            phase.id == "cleanup" && phase.status == DownloadProtectionSwitchPhaseStatus::Pass
        }));

        let status =
            observed_download_protection_status(&settings, &database.pool, &secrets).await?;
        assert_eq!(status.active_profile.id, "direct");
        assert_eq!(profile_status(&database.pool, "wg").await?, "blocked");

        let events = list_download_network_events(&database.pool, 10).await?;
        let event = events.first().expect("switch event");
        assert_eq!(event.status, "blocked");
        assert_eq!(
            event
                .evidence
                .pointer("/runtimeEvidence/downloaderPublicIp/value")
                .and_then(serde_json::Value::as_str),
            Some("203.0.113.10")
        );
        assert_eq!(
            event
                .evidence
                .pointer("/rollbackApplied")
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
        Ok(())
    }

    #[tokio::test]
    async fn warp_profile_requires_disclosure_acceptance() -> Result<()> {
        let database = test_database().await?;
        let secrets = SecretsManager::from_key_bytes([6u8; 32], true);

        let err = ensure_cloudflare_warp_profile(
            &database.pool,
            &secrets,
            CloudflareWarpProfileRequest {
                accepted_disclosure: false,
                profile_id: None,
                name: None,
            },
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("disclosure must be accepted"));
        assert_eq!(active_profile_count(&database.pool).await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn warp_profile_creation_stores_per_server_identity_and_blocks_runtime() -> Result<()> {
        let database = test_database().await?;
        let store = ExtensionStore::new(&database.pool);
        let secrets = SecretsManager::from_key_bytes([7u8; 32], true);

        let response = ensure_cloudflare_warp_profile(
            &database.pool,
            &secrets,
            CloudflareWarpProfileRequest {
                accepted_disclosure: true,
                profile_id: None,
                name: None,
            },
        )
        .await?;

        assert_eq!(response.profile.id, CLOUDFLARE_WARP_PROFILE_ID);
        assert_eq!(
            response.profile.kind,
            DownloadNetworkProfileKind::CloudflareWarp
        );
        assert_eq!(
            response.enrollment.identity_secret_ref,
            "global:cloudflare_warp_identity"
        );
        assert_eq!(
            response
                .blocker
                .as_ref()
                .map(|blocker| blocker.code.as_str()),
            Some("warp_runtime_pending")
        );

        let secret = store
            .get_secret(
                SecretScope::Global,
                None,
                CLOUDFLARE_WARP_IDENTITY_SECRET_KEY,
            )
            .await?
            .expect("warp identity secret");
        let decrypted = secrets.decrypt(&secret.value_encrypted)?;
        let identity: serde_json::Value = serde_json::from_str(&decrypted)?;
        assert_eq!(
            identity
                .get("sharedCredentials")
                .and_then(serde_json::Value::as_bool),
            Some(false)
        );
        assert_eq!(
            identity
                .get("enrollmentId")
                .and_then(serde_json::Value::as_str),
            Some(response.enrollment.enrollment_id.as_str())
        );
        assert_eq!(active_profile_count(&database.pool).await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn warp_switch_prepares_runtime_through_orchestrated_apply() -> Result<()> {
        let settings = test_settings(VpnConfig::default());
        let database = test_database().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_qbittorrent_instance(&store).await?;
        let secrets = SecretsManager::from_key_bytes([8u8; 32], true);
        ensure_cloudflare_warp_profile(
            &database.pool,
            &secrets,
            CloudflareWarpProfileRequest {
                accepted_disclosure: true,
                profile_id: None,
                name: None,
            },
        )
        .await?;

        let response = switch_download_protection_profile_with_orchestrated_apply(
            &settings,
            &database.pool,
            &secrets,
            DownloadProtectionSwitchRequest {
                target_profile_id: CLOUDFLARE_WARP_PROFILE_ID.to_string(),
                apply: true,
                expected_active_profile_id: None,
                server_public_ip: Some("203.0.113.10".to_string()),
                downloader_public_ip: Some("198.51.100.22".to_string()),
                runtime_evidence: None,
            },
            || async {
                mark_cloudflare_warp_runtime_ready(&database.pool, CLOUDFLARE_WARP_PROFILE_ID).await
            },
            || async { Ok(test_runtime_evidence_pass()) },
        )
        .await?;

        assert_eq!(response.status, DownloadProtectionSwitchStatus::Applied);
        assert!(response.applied);
        assert!(response.blocker.is_none());
        assert!(response.checks.iter().any(|check| {
            check.code == "warp_runtime_preparable"
                && check.status == DownloadProtectionCheckStatus::Pass
        }));

        let status =
            observed_download_protection_status(&settings, &database.pool, &secrets).await?;
        assert_eq!(status.mode, DownloadProtectionMode::CloudflareWarp);
        assert_eq!(status.state, DownloadProtectionState::Protected);
        assert!(status.blocker.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn first_run_protected_downloads_creates_warp_routes_and_switches() -> Result<()> {
        let settings = test_settings(VpnConfig::default());
        let database = test_database().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_qbittorrent_instance(&store).await?;
        let secrets = SecretsManager::from_key_bytes([26u8; 32], true);

        let response = apply_download_protection_first_run_choice_with_orchestrated_apply(
            &settings,
            &database.pool,
            &secrets,
            DownloadProtectionFirstRunRequest {
                choice: DownloadProtectionFirstRunChoice::ProtectedDownloads,
                accepted_warp_disclosure: true,
                apply: true,
            },
            || async {
                mark_cloudflare_warp_runtime_ready(&database.pool, CLOUDFLARE_WARP_PROFILE_ID).await
            },
            || async { Ok(test_runtime_evidence_pass()) },
        )
        .await?;

        assert!(response.completed);
        assert!(response.applied);
        assert_eq!(
            response.profile.as_ref().map(|profile| profile.id.as_str()),
            Some(CLOUDFLARE_WARP_PROFILE_ID)
        );
        assert_eq!(
            response
                .switch_result
                .as_ref()
                .map(|result| result.status.clone()),
            Some(DownloadProtectionSwitchStatus::Applied)
        );
        for logical_id in [TORRENT_DEFAULT_LOGICAL_ID, USENET_DEFAULT_LOGICAL_ID] {
            let route = response
                .routes
                .routes
                .iter()
                .find(|route| route.logical_id == logical_id && route.owner_id == "default")
                .expect("default first-run route");
            assert_eq!(
                route.binding_kind,
                DownloadBrokerBindingKind::ManagedProtected
            );
            assert_eq!(
                route.profile_id.as_deref(),
                Some(CLOUDFLARE_WARP_PROFILE_ID)
            );
        }

        let status =
            observed_download_protection_status(&settings, &database.pool, &secrets).await?;
        assert_eq!(status.mode, DownloadProtectionMode::CloudflareWarp);
        assert_eq!(status.state, DownloadProtectionState::Protected);
        Ok(())
    }

    #[tokio::test]
    async fn first_run_protected_downloads_requires_warp_disclosure() -> Result<()> {
        let settings = test_settings(VpnConfig::default());
        let database = test_database().await?;
        let secrets = SecretsManager::from_key_bytes([27u8; 32], true);

        let err = apply_download_protection_first_run_choice_with_orchestrated_apply(
            &settings,
            &database.pool,
            &secrets,
            DownloadProtectionFirstRunRequest {
                choice: DownloadProtectionFirstRunChoice::ProtectedDownloads,
                accepted_warp_disclosure: false,
                apply: true,
            },
            || async { Ok(()) },
            || async { Ok(test_runtime_evidence_pass()) },
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("disclosure must be accepted"));
        assert_eq!(active_profile_count(&database.pool).await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn first_run_existing_stack_sets_external_routes_and_profile() -> Result<()> {
        let settings = test_settings(VpnConfig::default());
        let database = test_database().await?;
        let secrets = SecretsManager::from_key_bytes([28u8; 32], true);

        let response = apply_download_protection_first_run_choice_with_orchestrated_apply(
            &settings,
            &database.pool,
            &secrets,
            DownloadProtectionFirstRunRequest {
                choice: DownloadProtectionFirstRunChoice::ExistingStack,
                accepted_warp_disclosure: false,
                apply: true,
            },
            || async { Ok(()) },
            || async { Ok(test_runtime_evidence_pass()) },
        )
        .await?;

        assert!(response.completed);
        assert!(response.applied);
        assert_eq!(
            response.profile.as_ref().map(|profile| profile.id.as_str()),
            Some(FIRST_RUN_EXTERNAL_ONLY_PROFILE_ID)
        );
        assert_eq!(
            response
                .profile
                .as_ref()
                .map(|profile| profile.kind.clone()),
            Some(DownloadNetworkProfileKind::ExternalOnly)
        );
        for logical_id in [TORRENT_DEFAULT_LOGICAL_ID, USENET_DEFAULT_LOGICAL_ID] {
            let route = response
                .routes
                .routes
                .iter()
                .find(|route| route.logical_id == logical_id && route.owner_id == "default")
                .expect("default first-run route");
            assert_eq!(route.binding_kind, DownloadBrokerBindingKind::External);
            assert!(route.profile_id.is_none());
        }

        let status =
            observed_download_protection_status(&settings, &database.pool, &secrets).await?;
        assert_eq!(status.mode, DownloadProtectionMode::ExternalOnly);
        Ok(())
    }

    #[tokio::test]
    async fn first_run_existing_stack_rehomes_from_warp_through_orchestrator() -> Result<()> {
        let settings = test_settings(VpnConfig::default());
        let database = test_database().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_qbittorrent_instance(&store).await?;
        let secrets = SecretsManager::from_key_bytes([45u8; 32], true);
        ensure_cloudflare_warp_profile(
            &database.pool,
            &secrets,
            CloudflareWarpProfileRequest {
                accepted_disclosure: true,
                profile_id: None,
                name: None,
            },
        )
        .await?;
        mark_cloudflare_warp_runtime_ready(&database.pool, CLOUDFLARE_WARP_PROFILE_ID).await?;
        let warp = load_download_network_profile(&database.pool, CLOUDFLARE_WARP_PROFILE_ID)
            .await?
            .expect("warp profile");
        activate_download_network_profile(&database.pool, &warp).await?;

        let apply_calls = Arc::new(AtomicUsize::new(0));
        let apply_calls_for_closure = apply_calls.clone();
        let response = apply_download_protection_first_run_choice_with_orchestrated_apply(
            &settings,
            &database.pool,
            &secrets,
            DownloadProtectionFirstRunRequest {
                choice: DownloadProtectionFirstRunChoice::ExistingStack,
                accepted_warp_disclosure: false,
                apply: true,
            },
            move || {
                let apply_calls = apply_calls_for_closure.clone();
                async move {
                    apply_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
            || async {
                anyhow::bail!("external first-run rehome should not collect protected evidence")
            },
        )
        .await?;

        assert!(response.completed);
        assert!(response.applied);
        assert_eq!(apply_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            response
                .switch_result
                .as_ref()
                .map(|result| result.status.clone()),
            Some(DownloadProtectionSwitchStatus::Applied)
        );
        assert_eq!(
            response.profile.as_ref().map(|profile| profile.id.as_str()),
            Some(FIRST_RUN_EXTERNAL_ONLY_PROFILE_ID)
        );
        for logical_id in [TORRENT_DEFAULT_LOGICAL_ID, USENET_DEFAULT_LOGICAL_ID] {
            let route = response
                .routes
                .routes
                .iter()
                .find(|route| route.logical_id == logical_id && route.owner_id == "default")
                .expect("default first-run route");
            assert_eq!(route.binding_kind, DownloadBrokerBindingKind::External);
            assert!(route.profile_id.is_none());
        }

        let status =
            observed_download_protection_status(&settings, &database.pool, &secrets).await?;
        assert_eq!(status.active_profile.id, FIRST_RUN_EXTERNAL_ONLY_PROFILE_ID);
        assert_eq!(status.mode, DownloadProtectionMode::ExternalOnly);
        Ok(())
    }

    #[tokio::test]
    async fn first_run_skip_downloads_marks_local_media_only_profile() -> Result<()> {
        let settings = test_settings(VpnConfig::default());
        let database = test_database().await?;
        let secrets = SecretsManager::from_key_bytes([30u8; 32], true);

        let response = apply_download_protection_first_run_choice_with_orchestrated_apply(
            &settings,
            &database.pool,
            &secrets,
            DownloadProtectionFirstRunRequest {
                choice: DownloadProtectionFirstRunChoice::SkipDownloads,
                accepted_warp_disclosure: false,
                apply: true,
            },
            || async { Ok(()) },
            || async { Ok(test_runtime_evidence_pass()) },
        )
        .await?;

        assert!(response.completed);
        assert!(response.applied);
        assert_eq!(
            response.profile.as_ref().map(|profile| profile.id.as_str()),
            Some(FIRST_RUN_SKIP_DOWNLOADS_PROFILE_ID)
        );
        assert!(
            response
                .notes
                .iter()
                .any(|note| note.contains("local-media-only"))
        );
        for logical_id in [TORRENT_DEFAULT_LOGICAL_ID, USENET_DEFAULT_LOGICAL_ID] {
            let route = response
                .routes
                .routes
                .iter()
                .find(|route| route.logical_id == logical_id && route.owner_id == "default")
                .expect("default first-run route");
            assert_eq!(route.binding_kind, DownloadBrokerBindingKind::External);
        }
        Ok(())
    }

    #[tokio::test]
    async fn first_run_custom_vpn_waits_for_import_without_mutating_routes() -> Result<()> {
        let settings = test_settings(VpnConfig::default());
        let database = test_database().await?;
        let secrets = SecretsManager::from_key_bytes([29u8; 32], true);

        let response = apply_download_protection_first_run_choice_with_orchestrated_apply(
            &settings,
            &database.pool,
            &secrets,
            DownloadProtectionFirstRunRequest {
                choice: DownloadProtectionFirstRunChoice::CustomVpn,
                accepted_warp_disclosure: false,
                apply: true,
            },
            || async { Ok(()) },
            || async { Ok(test_runtime_evidence_pass()) },
        )
        .await?;

        assert!(!response.completed);
        assert!(!response.applied);
        assert!(response.profile.is_none());
        assert!(response.switch_result.is_none());
        assert!(response.blocker.is_none());
        assert!(response.checks.iter().any(|check| {
            check.code == "custom_vpn_profile_required"
                && check.status == DownloadProtectionCheckStatus::Warn
        }));
        assert_eq!(active_profile_count(&database.pool).await?, 0);
        Ok(())
    }

    #[tokio::test]
    async fn active_warp_status_reports_no_forwarded_torrent_port() -> Result<()> {
        let settings = test_settings(VpnConfig::default());
        let database = test_database().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_qbittorrent_instance(&store).await?;
        let secrets = SecretsManager::from_key_bytes([9u8; 32], true);
        ensure_cloudflare_warp_profile(
            &database.pool,
            &secrets,
            CloudflareWarpProfileRequest {
                accepted_disclosure: true,
                profile_id: None,
                name: None,
            },
        )
        .await?;
        sqlx::query::<sqlx::Any>(
            "UPDATE download_network_profiles SET active = TRUE, status = 'blocked' WHERE id = ?",
        )
        .bind(CLOUDFLARE_WARP_PROFILE_ID)
        .execute(&database.pool)
        .await?;

        let status =
            observed_download_protection_status(&settings, &database.pool, &secrets).await?;

        assert_eq!(status.mode, DownloadProtectionMode::CloudflareWarp);
        assert_eq!(
            status.torrent_reachability.state,
            DownloadTorrentReachabilityState::NoForwardedPort
        );
        assert!(!status.torrent_reachability.can_accept_inbound);
        assert!(status.torrent_reachability.forwarded_port.is_none());
        assert!(status.checks.iter().any(|check| {
            check.code == "torrent_reachability_no_forwarded_port"
                && check.status == DownloadProtectionCheckStatus::Warn
        }));
        assert_eq!(
            status.blocker.as_ref().map(|blocker| blocker.code.as_str()),
            Some("warp_runtime_pending")
        );
        Ok(())
    }

    #[tokio::test]
    async fn provider_preset_status_reports_observed_forwarded_torrent_port() -> Result<()> {
        let settings = test_settings(VpnConfig::default());
        let database = test_database().await?;
        let store = ExtensionStore::new(&database.pool);
        insert_qbittorrent_instance(&store).await?;
        let secrets = SecretsManager::from_key_bytes([10u8; 32], true);
        insert_profile_with_config(
            &database.pool,
            "preset",
            "Provider Preset",
            "provider_preset",
            true,
            true,
            Some("provider_preset"),
            json!({
                "torrentReachability": {
                    "forwardedPort": {
                        "port": 49123,
                        "protocol": "tcp",
                        "source": "provider_api"
                    }
                }
            }),
        )
        .await?;

        let status =
            observed_download_protection_status(&settings, &database.pool, &secrets).await?;

        assert_eq!(status.mode, DownloadProtectionMode::ProviderPreset);
        assert_eq!(
            status.torrent_reachability.state,
            DownloadTorrentReachabilityState::ForwardedPort
        );
        assert!(status.torrent_reachability.can_accept_inbound);
        assert_eq!(status.torrent_reachability.listen_port, Some(49123));
        let forwarded = status
            .torrent_reachability
            .forwarded_port
            .expect("forwarded port");
        assert_eq!(forwarded.port, 49123);
        assert_eq!(forwarded.source, "provider_api");
        assert!(status.checks.iter().any(|check| {
            check.code == "torrent_reachability_forwarded_port"
                && check.status == DownloadProtectionCheckStatus::Pass
        }));
        Ok(())
    }

    #[test]
    fn provider_preset_catalog_marks_warp_as_no_forwarded_port() {
        let catalog = download_provider_preset_catalog();
        let warp = catalog
            .presets
            .iter()
            .find(|preset| preset.id == "cloudflare-warp")
            .expect("warp preset");
        assert_eq!(
            warp.port_forwarding,
            DownloadProviderPortForwarding::Unsupported
        );
        assert!(
            warp.profile_kinds
                .contains(&DownloadNetworkProfileKind::CloudflareWarp)
        );

        let airvpn = catalog
            .presets
            .iter()
            .find(|preset| preset.id == "airvpn")
            .expect("airvpn preset");
        assert_eq!(
            airvpn.port_forwarding,
            DownloadProviderPortForwarding::ProviderApi
        );
    }

    #[tokio::test]
    async fn qbittorrent_listen_port_sync_plan_uses_observed_forwarded_port() -> Result<()> {
        let settings = test_settings(VpnConfig::default());
        let database = test_database().await?;
        let store = ExtensionStore::new(&database.pool);
        let instance_id = insert_qbittorrent_instance(&store).await?;
        let provider_id = insert_qbittorrent_provider(&store, instance_id).await?;
        let secrets = SecretsManager::from_key_bytes([11u8; 32], true);
        insert_global_secret(
            &store,
            &secrets,
            "wireguard_config",
            "[Interface]\nPrivateKey=x",
        )
        .await?;
        insert_profile_with_config(
            &database.pool,
            "wg-forwarded",
            "WireGuard With Forwarded Port",
            "wireguard_config",
            true,
            true,
            Some("gluetun_wireguard"),
            json!({
                "torrentReachability": {
                    "forwardedPort": {
                        "port": 51413,
                        "protocol": "tcp",
                        "source": "provider_api"
                    }
                }
            }),
        )
        .await?;
        insert_profile_secret(
            &database.pool,
            "wg-forwarded",
            "wireguard_config",
            "global:wireguard_config",
        )
        .await?;

        let plan = qbittorrent_listen_port_sync_plan(&settings, &database.pool, &secrets).await?;

        assert_eq!(plan.status, QbittorrentListenPortSyncStatus::Ready);
        assert_eq!(plan.target_provider_id, Some(provider_id));
        assert_eq!(plan.target_instance_id, Some(instance_id));
        assert_eq!(plan.target_port, Some(51413));
        assert_eq!(plan.capability.as_deref(), Some("downloader.torrent"));
        assert!(plan.requires_orchestrator);
        let patch = plan.patch.expect("driver patch");
        assert_eq!(
            patch.get("op").and_then(serde_json::Value::as_str),
            Some("set_preferences")
        );
        assert_eq!(
            patch.get("listen_port").and_then(serde_json::Value::as_u64),
            Some(51413)
        );
        assert_eq!(
            patch
                .get("random_port")
                .and_then(serde_json::Value::as_bool),
            Some(false)
        );
        Ok(())
    }

    #[tokio::test]
    async fn import_wireguard_profile_stores_config_as_profile_secret() -> Result<()> {
        let database = test_database().await?;
        let secrets = SecretsManager::from_key_bytes([12u8; 32], true);
        let config = r#"
[Interface]
PrivateKey = test-private-key
Address = 10.2.0.2/32
DNS = 1.1.1.1

[Peer]
PublicKey = test-public-key
Endpoint = 203.0.113.10:51820
AllowedIPs = 0.0.0.0/0, ::/0
"#;

        let response = import_wireguard_profile(
            &database.pool,
            &secrets,
            DownloadNetworkProfileImportRequest {
                profile_id: Some("phase6-wireguard".to_string()),
                name: Some("Phase 6 WireGuard".to_string()),
                provider: Some("custom".to_string()),
                strict: true,
                config: config.to_string(),
                gateway_image: None,
                username: None,
                password: None,
                forwarded_port: None,
            },
        )
        .await?;

        assert_eq!(response.profile.id, "phase6-wireguard");
        assert_eq!(
            response.profile.kind,
            DownloadNetworkProfileKind::WireguardConfig
        );
        assert!(response.blocker.is_none());

        let secret_ref =
            load_profile_secret_ref(&database.pool, "phase6-wireguard", "wireguard_config")
                .await?
                .expect("profile secret ref");
        assert!(secret_ref.starts_with("global:download_profile_phase6_wireguard"));
        let profiles = list_download_network_profiles(&database.pool).await?;
        assert_eq!(profiles.profiles.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn import_wireguard_profile_stores_provider_and_forwarded_port() -> Result<()> {
        let database = test_database().await?;
        let secrets = SecretsManager::from_key_bytes([14u8; 32], true);
        let config = r#"
[Interface]
PrivateKey = test-private-key
Address = 10.4.0.2/32

[Peer]
PublicKey = test-public-key
Endpoint = 203.0.113.44:51820
AllowedIPs = 0.0.0.0/0, ::/0
"#;

        import_wireguard_profile(
            &database.pool,
            &secrets,
            DownloadNetworkProfileImportRequest {
                profile_id: Some("airvpn-forwarded".to_string()),
                name: Some("AirVPN Forwarded".to_string()),
                provider: Some("airvpn".to_string()),
                strict: true,
                config: config.to_string(),
                gateway_image: None,
                username: None,
                password: None,
                forwarded_port: Some(DownloadForwardedPort {
                    port: 49152,
                    protocol: "tcp".to_string(),
                    source: "provider_api".to_string(),
                    expires_at: None,
                }),
            },
        )
        .await?;

        let profile = load_download_network_profile(&database.pool, "airvpn-forwarded")
            .await?
            .expect("imported profile");
        assert_eq!(profile.provider.as_deref(), Some("airvpn"));
        let forwarded = forwarded_port_from_config(&profile.config_json).expect("forwarded port");
        assert_eq!(forwarded.port, 49152);
        assert_eq!(forwarded.protocol, "tcp");
        assert_eq!(forwarded.source, "provider_api");
        Ok(())
    }

    #[tokio::test]
    async fn import_openvpn_profile_requires_auth_when_config_uses_auth_user_pass() -> Result<()> {
        let database = test_database().await?;
        let secrets = SecretsManager::from_key_bytes([13u8; 32], true);
        let response = import_openvpn_profile(
            &database.pool,
            &secrets,
            DownloadNetworkProfileImportRequest {
                profile_id: Some("phase6-openvpn".to_string()),
                name: Some("Phase 6 OpenVPN".to_string()),
                provider: Some("custom".to_string()),
                strict: true,
                config: "client\ndev tun\nremote 203.0.113.20 1194\nauth-user-pass\n".to_string(),
                gateway_image: None,
                username: None,
                password: None,
                forwarded_port: None,
            },
        )
        .await?;

        assert_eq!(response.profile.id, "phase6-openvpn");
        assert_eq!(response.profile.status, DownloadProtectionState::Blocked);
        assert_eq!(
            response
                .blocker
                .as_ref()
                .map(|blocker| blocker.code.as_str()),
            Some("openvpn_auth_credentials_failed")
        );
        Ok(())
    }

    #[test]
    fn direct_profile_compiler_leaves_app_spec_unwrapped() -> Result<()> {
        let app = compiler_test_app_spec();
        let profile = DownloadProtectionProfile::direct("legacy-direct", "Legacy Direct");

        let compiled = profile.compile(DownloadProtectionCompileInput {
            app_container_name: "elx-test",
            app_spec: &app,
            base_labels: &HashMap::new(),
        })?;

        assert!(compiled.gateway_spec.is_none());
        assert_eq!(compiled.protected_app_spec.network_mode, None);
        assert_eq!(compiled.protected_app_spec.aliases, app.aliases);
        assert_eq!(compiled.protected_app_spec.ports.len(), app.ports.len());
        Ok(())
    }

    #[test]
    fn wireguard_profile_compiler_renders_gateway_namespace_pair() -> Result<()> {
        let app = compiler_test_app_spec();
        let mut labels = HashMap::new();
        labels.insert("elixir.instance_id".to_string(), "instance-1".to_string());
        let profile = DownloadProtectionProfile::wireguard_config(
            "legacy-wireguard",
            "Legacy WireGuard",
            true,
            GluetunWireguardGatewayRuntime {
                image: "example/wireguard-gateway:1".to_string(),
                config_host_path: "/tmp/elixir/wg0.conf".to_string(),
            },
        );

        let compiled = profile.compile(DownloadProtectionCompileInput {
            app_container_name: "elx-test",
            app_spec: &app,
            base_labels: &labels,
        })?;

        let gateway = compiled.gateway_spec.expect("gateway spec");
        assert_eq!(gateway.name, "elx-test-vpn");
        assert_eq!(gateway.image, "example/wireguard-gateway:1");
        assert_eq!(gateway.network, "elixir_net");
        assert_eq!(gateway.network_mode, None);
        assert_eq!(gateway.aliases, app.aliases);
        assert_eq!(gateway.ports.len(), 2);
        assert_eq!(
            gateway
                .labels
                .get("elixir.network_role")
                .map(String::as_str),
            Some("wireguard_gateway")
        );
        assert_eq!(gateway.cap_add, vec!["NET_ADMIN".to_string()]);
        assert_eq!(
            gateway.devices,
            vec!["/dev/net/tun:/dev/net/tun".to_string()]
        );
        assert!(
            gateway
                .env
                .iter()
                .any(|env| { env.name == "FIREWALL_INPUT_PORTS" && env.value == "8080,9090" })
        );
        assert_eq!(gateway.volumes[0].host_path, "/tmp/elixir/wg0.conf");
        assert_eq!(
            gateway.volumes[0].container_path,
            "/gluetun/wireguard/wg0.conf"
        );
        assert!(gateway.volumes[0].read_only);

        let app = compiled.protected_app_spec;
        assert_eq!(app.network_mode.as_deref(), Some("container:elx-test-vpn"));
        assert!(app.aliases.is_empty());
        assert!(app.ports.is_empty());
        Ok(())
    }

    #[test]
    fn warp_profile_compiler_renders_gateway_namespace_pair() -> Result<()> {
        let app = compiler_test_app_spec();
        let mut labels = HashMap::new();
        labels.insert("elixir.instance_id".to_string(), "instance-1".to_string());
        let profile = DownloadProtectionProfile::cloudflare_warp(
            CLOUDFLARE_WARP_PROFILE_ID,
            "Cloudflare WARP",
            true,
            CloudflareWarpGatewayRuntime {
                image: "example/warp-gateway:1".to_string(),
                state_volume_name: warp_state_volume_name(CLOUDFLARE_WARP_PROFILE_ID),
                enrollment_id: "enrollment-1".to_string(),
                identity_secret_ref: "global:cloudflare_warp_identity".to_string(),
            },
        );

        let compiled = profile.compile(DownloadProtectionCompileInput {
            app_container_name: "elx-test",
            app_spec: &app,
            base_labels: &labels,
        })?;

        let gateway = compiled.gateway_spec.expect("gateway spec");
        assert_eq!(gateway.name, "elx-test-vpn");
        assert_eq!(gateway.image, "example/warp-gateway:1");
        assert_eq!(gateway.network_mode, None);
        assert_eq!(gateway.aliases, app.aliases);
        assert_eq!(gateway.ports.len(), 2);
        assert_eq!(
            gateway
                .labels
                .get("elixir.network_role")
                .map(String::as_str),
            Some("warp_gateway")
        );
        assert!(gateway.cap_add.iter().any(|value| value == "NET_ADMIN"));
        assert!(
            gateway
                .devices
                .iter()
                .any(|value| value == "/dev/net/tun:/dev/net/tun")
        );
        assert!(
            gateway
                .env
                .iter()
                .any(|env| { env.name == "WARP_ENABLE_NAT" && env.value == "1" })
        );
        assert_eq!(
            gateway
                .volumes
                .iter()
                .find(|volume| volume.container_path == "/var/lib/cloudflare-warp")
                .map(|volume| (&volume.source_kind, volume.host_path.as_str())),
            Some((
                &VolumeMountSourceKind::NamedVolume,
                "elixir_warp_state_cloudflare_warp"
            ))
        );

        let app = compiled.protected_app_spec;
        assert_eq!(app.network_mode.as_deref(), Some("container:elx-test-vpn"));
        assert!(app.aliases.is_empty());
        assert!(app.ports.is_empty());
        Ok(())
    }

    #[test]
    fn openvpn_profile_compiler_renders_gateway_namespace_pair() -> Result<()> {
        let app = compiler_test_app_spec();
        let profile = DownloadProtectionProfile::openvpn_config(
            "imported-openvpn",
            "Imported OpenVPN",
            true,
            GluetunOpenvpnGatewayRuntime {
                image: "example/gluetun:openvpn".to_string(),
                config_host_path: "/tmp/elixir/custom.conf".to_string(),
                auth_host_path: Some("/tmp/elixir/auth.txt".to_string()),
            },
        );

        let compiled = profile.compile(DownloadProtectionCompileInput {
            app_container_name: "elx-test",
            app_spec: &app,
            base_labels: &HashMap::new(),
        })?;

        let gateway = compiled.gateway_spec.expect("gateway spec");
        assert_eq!(gateway.name, "elx-test-vpn");
        assert_eq!(gateway.image, "example/gluetun:openvpn");
        assert_eq!(
            gateway
                .labels
                .get("elixir.network_role")
                .map(String::as_str),
            Some("openvpn_gateway")
        );
        assert!(gateway.env.iter().any(|env| {
            env.name == "OPENVPN_CUSTOM_CONFIG" && env.value == "/gluetun/custom.conf"
        }));
        assert!(gateway.volumes.iter().any(|volume| {
            volume.host_path == "/tmp/elixir/custom.conf"
                && volume.container_path == "/gluetun/custom.conf"
                && volume.read_only
        }));
        assert!(gateway.volumes.iter().any(|volume| {
            volume.host_path == "/tmp/elixir/auth.txt"
                && volume.container_path == "/gluetun/auth.txt"
                && volume.read_only
        }));

        let app = compiled.protected_app_spec;
        assert_eq!(app.network_mode.as_deref(), Some("container:elx-test-vpn"));
        assert!(app.aliases.is_empty());
        assert!(app.ports.is_empty());
        Ok(())
    }

    fn compiler_test_app_spec() -> ContainerSpec {
        ContainerSpec {
            name: "elx-test".to_string(),
            image: "example/downloader:latest".to_string(),
            network: "elixir_net".to_string(),
            network_mode: None,
            aliases: vec!["svc-test".to_string(), "elx-downloader".to_string()],
            env: Vec::new(),
            volumes: Vec::new(),
            ports: vec![
                PortMapping {
                    container_port: 8080,
                    host_port: None,
                    protocol: None,
                },
                PortMapping {
                    container_port: 9090,
                    host_port: Some(19090),
                    protocol: None,
                },
            ],
            labels: HashMap::new(),
            command: Vec::new(),
            cap_add: Vec::new(),
            cap_drop: Vec::new(),
            devices: Vec::new(),
            sysctls: HashMap::new(),
            security: Default::default(),
        }
    }

    async fn test_database() -> Result<crate::db::Database> {
        let mut settings = Settings::default();
        settings.database.url = "sqlite::memory:?cache=shared".to_string();
        settings.database.max_connections = 1;
        settings.database.connect_timeout_seconds = 5;
        sqlx::any::install_default_drivers();
        let database = crate::db::Database::connect(&settings.database).await?;
        database.run_migrations().await?;
        Ok(database)
    }

    async fn insert_qbittorrent_instance(store: &ExtensionStore<'_>) -> Result<Uuid> {
        store
            .upsert_extension(&NewExtension {
                extension_id: "elixir.modules.qbittorrent".to_string(),
                name: "qBittorrent".to_string(),
                version: "1.0.0".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: None,
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Verified,
                manifest_json: json!({}),
                package_hash: None,
                enabled: true,
            })
            .await?;
        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: "elixir.modules.qbittorrent".to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;
        Ok(instance_id)
    }

    async fn insert_qbittorrent_provider(
        store: &ExtensionStore<'_>,
        instance_id: Uuid,
    ) -> Result<Uuid> {
        let provider_id = Uuid::new_v4();
        store
            .upsert_provider(&NewProvider {
                provider_id,
                instance_id,
                capability: "downloader.torrent".to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: Some("qbittorrent".to_string()),
                scope_json: None,
                endpoint_json: Some(serde_json::to_value(ProviderEndpoint::new(
                    "http".to_string(),
                    "svc-elixir-modules-qbittorrent-default".to_string(),
                    8080,
                    None,
                    None,
                )?)?),
                health_state: ProviderHealthState::Healthy,
            })
            .await?;
        Ok(provider_id)
    }

    async fn insert_global_secret(
        store: &ExtensionStore<'_>,
        secrets: &SecretsManager,
        key: &str,
        value: &str,
    ) -> Result<()> {
        store
            .upsert_secret(&NewSecret {
                secret_id: Uuid::new_v4(),
                scope: SecretScope::Global,
                scope_id: None,
                key: key.to_string(),
                value_encrypted: secrets.encrypt(value)?,
                rotatable: true,
            })
            .await
    }

    async fn insert_profile(
        pool: &AnyPool,
        id: &str,
        name: &str,
        kind: &str,
        enabled: bool,
        active: bool,
        gateway_runtime: Option<&str>,
    ) -> Result<()> {
        insert_profile_with_config(
            pool,
            id,
            name,
            kind,
            enabled,
            active,
            gateway_runtime,
            json!({}),
        )
        .await
    }

    async fn insert_profile_with_config(
        pool: &AnyPool,
        id: &str,
        name: &str,
        kind: &str,
        enabled: bool,
        active: bool,
        gateway_runtime: Option<&str>,
        config_json: serde_json::Value,
    ) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "INSERT INTO download_network_profiles (id, name, kind, enabled, strict, scope, provider, gateway_runtime, config_json, status, active) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(id)
        .bind(name)
        .bind(kind)
        .bind(enabled)
        .bind(true)
        .bind("managed_downloaders")
        .bind(Option::<String>::None)
        .bind(gateway_runtime)
        .bind(serde_json::to_string(&config_json)?)
        .bind("unknown")
        .bind(active)
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn insert_profile_secret(
        pool: &AnyPool,
        profile_id: &str,
        key: &str,
        secret_ref: &str,
    ) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "INSERT INTO download_network_profile_secrets (profile_id, key, secret_ref) VALUES (?, ?, ?)",
        )
        .bind(profile_id)
        .bind(key)
        .bind(secret_ref)
        .execute(pool)
        .await?;
        Ok(())
    }

    fn test_evidence_pass(value: &str) -> DownloadProtectionProbeEvidence {
        DownloadProtectionProbeEvidence {
            status: DownloadProtectionCheckStatus::Pass,
            value: Some(value.to_string()),
            detail: "test evidence passed".to_string(),
            checked_at: Utc::now(),
        }
    }

    fn test_evidence_fail(detail: &str) -> DownloadProtectionProbeEvidence {
        DownloadProtectionProbeEvidence {
            status: DownloadProtectionCheckStatus::Fail,
            value: None,
            detail: detail.to_string(),
            checked_at: Utc::now(),
        }
    }

    fn test_runtime_evidence_pass() -> DownloadProtectionRuntimeEvidence {
        DownloadProtectionRuntimeEvidence {
            server_public_ip: Some(test_evidence_pass("203.0.113.10")),
            gateway_public_ip: Some(test_evidence_pass("198.51.100.22")),
            downloader_public_ip: Some(test_evidence_pass("198.51.100.22")),
            gateway_dns: Some(test_evidence_pass("1.1.1.1")),
            downloader_dns: Some(test_evidence_pass("1.1.1.1")),
            kill_switch: Some(test_evidence_pass("container:gateway")),
        }
    }

    fn test_runtime_evidence_leak() -> DownloadProtectionRuntimeEvidence {
        DownloadProtectionRuntimeEvidence {
            server_public_ip: Some(test_evidence_pass("203.0.113.10")),
            gateway_public_ip: Some(test_evidence_pass("198.51.100.22")),
            downloader_public_ip: Some(test_evidence_pass("203.0.113.10")),
            gateway_dns: Some(test_evidence_pass("1.1.1.1")),
            downloader_dns: Some(test_evidence_pass("1.1.1.1")),
            kill_switch: Some(test_evidence_pass("container:gateway")),
        }
    }

    async fn active_profile_count(pool: &AnyPool) -> Result<i64> {
        let count = sqlx::query_scalar::<sqlx::Any, i64>(
            "SELECT COUNT(*) FROM download_network_profiles WHERE active = TRUE",
        )
        .fetch_one(pool)
        .await?;
        Ok(count)
    }

    async fn profile_status(pool: &AnyPool, profile_id: &str) -> Result<String> {
        let status = sqlx::query_scalar::<sqlx::Any, String>(
            "SELECT status FROM download_network_profiles WHERE id = ?",
        )
        .bind(profile_id)
        .fetch_one(pool)
        .await?;
        Ok(status)
    }
}
