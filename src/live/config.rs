//! Validated, fail-closed configuration for the Live subsystem.

use std::{collections::HashSet, error::Error, fmt, net::IpAddr, path::Path};

use serde::{Deserialize, Serialize};

pub(crate) use crate::live_egress_common::is_public_egress_ip;

use super::crypto::validate_live_key_id;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct LiveConfig {
    pub enabled: bool,
    pub catalog_enabled: bool,
    pub playback_enabled: bool,
    pub client_direct_enabled: bool,
    pub relay_enabled: bool,
    pub remux_enabled: bool,
    pub protected_egress_enabled: bool,
    pub stremio_compat_enabled: bool,
    pub native_dash_relay_enabled: bool,
    pub low_latency_hls_enabled: bool,
    pub rtmp_remux_enabled: bool,
    pub srt_remux_enabled: bool,
    pub allow_private_lan_sources: bool,
    pub sessions: LiveSessionLimits,
    pub recovery: LiveRecoveryLimits,
    pub providers: LiveProviderLimits,
    pub relay: LiveRelayLimits,
    pub remux: LiveRemuxLimits,
    pub egress: LiveEgressConfig,
    pub crypto: LiveCryptoConfig,
}

impl Default for LiveConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            catalog_enabled: false,
            playback_enabled: false,
            client_direct_enabled: false,
            relay_enabled: false,
            remux_enabled: false,
            protected_egress_enabled: false,
            stremio_compat_enabled: false,
            native_dash_relay_enabled: false,
            low_latency_hls_enabled: false,
            rtmp_remux_enabled: false,
            srt_remux_enabled: false,
            allow_private_lan_sources: false,
            sessions: LiveSessionLimits::default(),
            recovery: LiveRecoveryLimits::default(),
            providers: LiveProviderLimits::default(),
            relay: LiveRelayLimits::default(),
            remux: LiveRemuxLimits::default(),
            egress: LiveEgressConfig::default(),
            crypto: LiveCryptoConfig::default(),
        }
    }
}

impl LiveConfig {
    pub fn validate(&self) -> Result<(), LiveConfigError> {
        require_flag(
            self.catalog_enabled,
            self.enabled,
            "catalog_enabled",
            "enabled",
        )?;
        require_flag(
            self.playback_enabled,
            self.catalog_enabled,
            "playback_enabled",
            "catalog_enabled",
        )?;
        require_flag(
            self.client_direct_enabled,
            self.playback_enabled,
            "client_direct_enabled",
            "playback_enabled",
        )?;
        require_flag(
            self.relay_enabled,
            self.playback_enabled,
            "relay_enabled",
            "playback_enabled",
        )?;
        require_flag(
            self.remux_enabled,
            self.relay_enabled,
            "remux_enabled",
            "relay_enabled",
        )?;
        require_flag(
            self.protected_egress_enabled,
            self.relay_enabled,
            "protected_egress_enabled",
            "relay_enabled",
        )?;
        require_flag(
            self.stremio_compat_enabled,
            self.catalog_enabled,
            "stremio_compat_enabled",
            "catalog_enabled",
        )?;
        for (enabled, child) in [
            (self.native_dash_relay_enabled, "native_dash_relay_enabled"),
            (self.low_latency_hls_enabled, "low_latency_hls_enabled"),
        ] {
            require_flag(enabled, self.relay_enabled, child, "relay_enabled")?;
        }
        for (enabled, child) in [
            (self.rtmp_remux_enabled, "rtmp_remux_enabled"),
            (self.srt_remux_enabled, "srt_remux_enabled"),
        ] {
            require_flag(enabled, self.remux_enabled, child, "remux_enabled")?;
        }
        if self.rtmp_remux_enabled {
            return Err(LiveConfigError::InvalidValue(
                "rtmp_remux_enabled requires an exact profile certification",
            ));
        }
        if self.srt_remux_enabled {
            return Err(LiveConfigError::InvalidValue(
                "srt_remux_enabled requires an exact platform certification",
            ));
        }
        require_flag(
            self.allow_private_lan_sources,
            self.relay_enabled,
            "allow_private_lan_sources",
            "relay_enabled",
        )?;
        if self.egress.default_mode != LiveEgressDefaultMode::Off {
            require_flag(
                true,
                self.protected_egress_enabled,
                "egress.default_mode",
                "protected_egress_enabled",
            )?;
        }
        self.egress.validate(self.protected_egress_enabled)?;

        bounded("sessions.per_user", self.sessions.per_user, 1, 32)?;
        bounded(
            "sessions.server_total",
            self.sessions.server_total,
            1,
            10_000,
        )?;
        if self.sessions.server_total < self.sessions.per_user {
            return Err(LiveConfigError::InvalidValue(
                "sessions.server_total must be at least sessions.per_user",
            ));
        }
        bounded(
            "sessions.lease_seconds",
            self.sessions.lease_seconds,
            15,
            300,
        )?;
        bounded(
            "sessions.max_lifetime_seconds",
            self.sessions.max_lifetime_seconds,
            self.sessions.lease_seconds.saturating_mul(2),
            86_400,
        )?;
        bounded(
            "sessions.startup_queue_seconds",
            self.sessions.startup_queue_seconds,
            1,
            120,
        )?;
        bounded("recovery.max_sources", self.recovery.max_sources, 1, 10)?;
        bounded(
            "recovery.max_transitions",
            self.recovery.max_transitions,
            1,
            32,
        )?;
        bounded(
            "recovery.window_seconds",
            self.recovery.window_seconds,
            60,
            3_600,
        )?;
        bounded(
            "recovery.source_cooldown_seconds",
            self.recovery.source_cooldown_seconds,
            1,
            600,
        )?;
        bounded(
            "recovery.refresh_expiry_lead_seconds",
            self.recovery.refresh_expiry_lead_seconds,
            0,
            3_600,
        )?;
        bounded(
            "providers.request_timeout_seconds",
            self.providers.request_timeout_seconds,
            1,
            60,
        )?;
        bounded(
            "providers.hard_timeout_seconds",
            self.providers.hard_timeout_seconds,
            self.providers.request_timeout_seconds,
            120,
        )?;
        bounded(
            "providers.response_bytes",
            self.providers.response_bytes,
            1_024,
            16 * 1_024 * 1_024,
        )?;
        bounded(
            "providers.concurrency_per_provider",
            self.providers.concurrency_per_provider,
            1,
            64,
        )?;
        bounded(
            "providers.concurrency_per_user",
            self.providers.concurrency_per_user,
            1,
            256,
        )?;
        bounded("relay.max_concurrent", self.relay.max_concurrent, 1, 1_000)?;
        bounded(
            "relay.per_stream_buffer_bytes",
            self.relay.per_stream_buffer_bytes,
            64 * 1_024,
            256 * 1_024 * 1_024,
        )?;
        bounded(
            "relay.aggregate_buffer_bytes",
            self.relay.aggregate_buffer_bytes,
            self.relay.per_stream_buffer_bytes,
            64 * 1_024 * 1_024 * 1_024,
        )?;
        bounded("remux.max_concurrent", self.remux.max_concurrent, 1, 128)?;
        bounded(
            "remux.temp_budget_bytes",
            self.remux.temp_budget_bytes,
            10 * 1_024 * 1_024 * 1_024,
            16 * 1_024 * 1_024 * 1_024 * 1_024,
        )?;
        if self.remux.temp_root.trim().is_empty() {
            return Err(LiveConfigError::InvalidValue(
                "remux.temp_root must not be empty",
            ));
        }
        for (label, value) in [
            ("remux.ffmpeg_binary", self.remux.ffmpeg_binary.as_str()),
            ("remux.ffprobe_binary", self.remux.ffprobe_binary.as_str()),
        ] {
            if value.trim().is_empty() || value.len() > 1_024 || value.chars().any(char::is_control)
            {
                return Err(LiveConfigError::InvalidValue(match label {
                    "remux.ffmpeg_binary" => "remux.ffmpeg_binary is invalid",
                    _ => "remux.ffprobe_binary is invalid",
                }));
            }
        }
        bounded(
            "remux.probe_timeout_seconds",
            self.remux.probe_timeout_seconds,
            1,
            60,
        )?;
        bounded(
            "remux.startup_timeout_seconds",
            self.remux.startup_timeout_seconds,
            2,
            120,
        )?;
        bounded(
            "remux.no_output_timeout_seconds",
            self.remux.no_output_timeout_seconds,
            self.remux.segment_seconds.saturating_mul(2),
            300,
        )?;
        bounded(
            "remux.graceful_stop_seconds",
            self.remux.graceful_stop_seconds,
            1,
            30,
        )?;
        bounded("remux.segment_seconds", self.remux.segment_seconds, 1, 30)?;
        bounded(
            "remux.playlist_segments",
            self.remux.playlist_segments,
            3,
            60,
        )?;
        bounded("remux.delete_threshold", self.remux.delete_threshold, 1, 10)?;
        bounded(
            "remux.stderr_ring_bytes",
            self.remux.stderr_ring_bytes,
            1_024,
            1_048_576,
        )?;
        bounded(
            "remux.minimum_free_bytes",
            self.remux.minimum_free_bytes,
            64 * 1_024 * 1_024,
            self.remux.temp_budget_bytes,
        )?;
        for (label, key_id) in [
            (
                "crypto.primary_envelope_key_id",
                self.crypto.primary_envelope_key_id.as_str(),
            ),
            (
                "crypto.primary_token_hash_key_id",
                self.crypto.primary_token_hash_key_id.as_str(),
            ),
            (
                "crypto.primary_audit_key_id",
                self.crypto.primary_audit_key_id.as_str(),
            ),
        ] {
            validate_live_key_id(key_id).map_err(|_| LiveConfigError::InvalidKeyId(label))?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct LiveSessionLimits {
    pub per_user: u32,
    pub server_total: u32,
    pub lease_seconds: u64,
    pub max_lifetime_seconds: u64,
    pub startup_queue_seconds: u64,
}

impl Default for LiveSessionLimits {
    fn default() -> Self {
        Self {
            per_user: 3,
            server_total: 100,
            lease_seconds: 90,
            max_lifetime_seconds: 43_200,
            startup_queue_seconds: 15,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct LiveRecoveryLimits {
    pub max_sources: u32,
    pub max_transitions: u32,
    pub window_seconds: u64,
    pub source_cooldown_seconds: u64,
    pub refresh_expiry_lead_seconds: u64,
}

impl Default for LiveRecoveryLimits {
    fn default() -> Self {
        Self {
            max_sources: 3,
            max_transitions: 6,
            window_seconds: 600,
            source_cooldown_seconds: 30,
            refresh_expiry_lead_seconds: 60,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct LiveProviderLimits {
    pub request_timeout_seconds: u64,
    pub hard_timeout_seconds: u64,
    pub response_bytes: u64,
    pub concurrency_per_provider: u32,
    pub concurrency_per_user: u32,
}

impl Default for LiveProviderLimits {
    fn default() -> Self {
        Self {
            request_timeout_seconds: 5,
            hard_timeout_seconds: 15,
            response_bytes: 2_097_152,
            concurrency_per_provider: 4,
            concurrency_per_user: 8,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct LiveRelayLimits {
    pub max_concurrent: u32,
    pub per_stream_buffer_bytes: u64,
    pub aggregate_buffer_bytes: u64,
}

impl Default for LiveRelayLimits {
    fn default() -> Self {
        Self {
            max_concurrent: 75,
            per_stream_buffer_bytes: 8_388_608,
            aggregate_buffer_bytes: 536_870_912,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct LiveRemuxLimits {
    pub max_concurrent: u32,
    pub temp_root: String,
    pub temp_budget_bytes: u64,
    pub ffmpeg_binary: String,
    pub ffprobe_binary: String,
    pub probe_timeout_seconds: u64,
    pub startup_timeout_seconds: u64,
    pub no_output_timeout_seconds: u64,
    pub graceful_stop_seconds: u64,
    pub segment_seconds: u64,
    pub playlist_segments: u32,
    pub delete_threshold: u32,
    pub stderr_ring_bytes: u64,
    pub minimum_free_bytes: u64,
}

impl Default for LiveRemuxLimits {
    fn default() -> Self {
        let logical_cpus = std::thread::available_parallelism()
            .map(|value| value.get())
            .unwrap_or(1);
        Self {
            max_concurrent: u32::try_from(logical_cpus.min(8)).unwrap_or(1),
            temp_root: "data/live-remux".to_string(),
            temp_budget_bytes: 10_737_418_240,
            ffmpeg_binary: "ffmpeg".to_string(),
            ffprobe_binary: "ffprobe".to_string(),
            probe_timeout_seconds: 12,
            startup_timeout_seconds: 30,
            no_output_timeout_seconds: 20,
            graceful_stop_seconds: 3,
            segment_seconds: 4,
            playlist_segments: 8,
            delete_threshold: 2,
            stderr_ring_bytes: 16_384,
            minimum_free_bytes: 536_870_912,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct LiveEgressConfig {
    pub default_mode: LiveEgressDefaultMode,
    pub default_policy_id: Option<String>,
    pub default_allow_fallback: bool,
    pub worker_image: String,
    pub network: String,
    pub control_root: String,
    pub max_concurrent: u32,
    pub startup_timeout_seconds: u64,
    pub health_timeout_seconds: u64,
    pub control_port: u16,
    pub worker_memory_mb: u64,
    pub worker_pids_limit: u64,
    pub profiles: Vec<LiveEgressProfileConfig>,
}

impl Default for LiveEgressConfig {
    fn default() -> Self {
        Self {
            default_mode: LiveEgressDefaultMode::Off,
            default_policy_id: None,
            default_allow_fallback: false,
            worker_image: "elixir-live-egress-worker:control-v2".to_string(),
            network: "elixir_live_egress".to_string(),
            control_root: "data/live-egress".to_string(),
            max_concurrent: 20,
            startup_timeout_seconds: 30,
            health_timeout_seconds: 15,
            control_port: 18_080,
            worker_memory_mb: 256,
            worker_pids_limit: 64,
            profiles: Vec::new(),
        }
    }
}

impl LiveEgressConfig {
    fn validate(&self, enabled: bool) -> Result<(), LiveConfigError> {
        bounded("egress.max_concurrent", self.max_concurrent, 1, 256)?;
        bounded(
            "egress.startup_timeout_seconds",
            self.startup_timeout_seconds,
            1,
            120,
        )?;
        bounded(
            "egress.health_timeout_seconds",
            self.health_timeout_seconds,
            1,
            60,
        )?;
        bounded("egress.control_port", self.control_port, 1_024, 65_535)?;
        bounded("egress.worker_memory_mb", self.worker_memory_mb, 64, 4_096)?;
        bounded(
            "egress.worker_pids_limit",
            self.worker_pids_limit,
            16,
            1_024,
        )?;
        if self.profiles.len() > 32 {
            return Err(LiveConfigError::OutOfBounds("egress.profiles"));
        }
        if !path_safe_component(&self.network)
            || self.worker_image.trim().is_empty()
            || self.worker_image.len() > 512
            || !safe_runtime_path(&self.control_root)
        {
            return Err(LiveConfigError::InvalidValue(
                "egress worker runtime configuration is invalid",
            ));
        }

        let mut ids = HashSet::new();
        let mut warp_state_volumes = HashSet::new();
        for profile in &self.profiles {
            profile.validate()?;
            if !ids.insert(profile.id.as_str()) {
                return Err(LiveConfigError::InvalidValue(
                    "egress profile IDs must be unique",
                ));
            }
            if profile.kind == LiveEgressProfileKind::Warp {
                let state_volume =
                    profile
                        .state_volume_name
                        .as_deref()
                        .ok_or(LiveConfigError::InvalidValue(
                            "WARP runtime identity is invalid",
                        ))?;
                if !warp_state_volumes.insert(state_volume) {
                    return Err(LiveConfigError::InvalidValue(
                        "egress WARP state volume names must be unique",
                    ));
                }
            }
        }
        if !enabled {
            if self.default_mode != LiveEgressDefaultMode::Off
                || self.default_policy_id.is_some()
                || !self.profiles.is_empty()
            {
                return Err(LiveConfigError::InvalidValue(
                    "egress profiles require protected_egress_enabled",
                ));
            }
            return Ok(());
        }
        if self.profiles.is_empty() {
            return Err(LiveConfigError::InvalidValue(
                "protected egress requires at least one profile",
            ));
        }
        match self.default_mode {
            LiveEgressDefaultMode::Off if self.default_policy_id.is_some() => {
                return Err(LiveConfigError::InvalidValue(
                    "egress.default_policy_id requires a protected default mode",
                ));
            }
            LiveEgressDefaultMode::PreferProtected | LiveEgressDefaultMode::RequireProtected => {
                let policy_id =
                    self.default_policy_id
                        .as_deref()
                        .ok_or(LiveConfigError::InvalidValue(
                            "protected egress default mode requires default_policy_id",
                        ))?;
                if !ids.contains(policy_id) {
                    return Err(LiveConfigError::InvalidValue(
                        "egress.default_policy_id does not name a configured profile",
                    ));
                }
            }
            LiveEgressDefaultMode::Off => {}
        }
        if self.default_mode != LiveEgressDefaultMode::PreferProtected
            && self.default_allow_fallback
        {
            return Err(LiveConfigError::InvalidValue(
                "egress fallback is valid only for prefer_protected",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct LiveEgressProfileConfig {
    pub id: String,
    pub name: String,
    pub kind: LiveEgressProfileKind,
    pub gateway_image: String,
    pub config_host_path: Option<String>,
    pub auth_host_path: Option<String>,
    pub state_volume_name: Option<String>,
    pub enrollment_id: Option<String>,
    pub identity_secret_ref: Option<String>,
    pub external_ip_url: String,
    pub dns_probe_host: String,
    pub expected_egress_ips: Vec<IpAddr>,
    pub selectable_by_profiles: bool,
}

impl Default for LiveEgressProfileConfig {
    fn default() -> Self {
        Self {
            id: String::new(),
            name: String::new(),
            kind: LiveEgressProfileKind::Wireguard,
            gateway_image: String::new(),
            config_host_path: None,
            auth_host_path: None,
            state_volume_name: None,
            enrollment_id: None,
            identity_secret_ref: None,
            external_ip_url: "https://api.ipify.org".to_string(),
            dns_probe_host: "example.com".to_string(),
            expected_egress_ips: Vec::new(),
            selectable_by_profiles: false,
        }
    }
}

impl LiveEgressProfileConfig {
    fn validate(&self) -> Result<(), LiveConfigError> {
        if !path_safe_component(&self.id)
            || self.name.trim().is_empty()
            || self.name.len() > 128
            || self.gateway_image.trim().is_empty()
            || self.gateway_image.len() > 512
            || !valid_https_probe(&self.external_ip_url)
            || !valid_dns_probe_host(&self.dns_probe_host)
            || self.expected_egress_ips.is_empty()
            || self.expected_egress_ips.len() > 16
            || self
                .expected_egress_ips
                .iter()
                .any(|address| !is_public_egress_ip(*address))
        {
            return Err(LiveConfigError::InvalidValue(
                "egress profile metadata or readiness proof is invalid",
            ));
        }
        match self.kind {
            LiveEgressProfileKind::Wireguard => {
                require_profile_path(self.config_host_path.as_deref())?;
                reject_present(&self.auth_host_path, "WireGuard auth path is not valid")?;
                reject_present(
                    &self.state_volume_name,
                    "WireGuard state volume is not valid",
                )?;
                reject_present(&self.enrollment_id, "WireGuard enrollment is not valid")?;
                reject_present(
                    &self.identity_secret_ref,
                    "WireGuard identity reference is not valid",
                )?;
            }
            LiveEgressProfileKind::Openvpn => {
                require_profile_path(self.config_host_path.as_deref())?;
                if self
                    .auth_host_path
                    .as_deref()
                    .is_some_and(|path| !safe_runtime_path(path))
                {
                    return Err(LiveConfigError::InvalidValue(
                        "OpenVPN auth path is invalid",
                    ));
                }
                reject_present(&self.state_volume_name, "OpenVPN state volume is not valid")?;
                reject_present(&self.enrollment_id, "OpenVPN enrollment is not valid")?;
                reject_present(
                    &self.identity_secret_ref,
                    "OpenVPN identity reference is not valid",
                )?;
            }
            LiveEgressProfileKind::Warp => {
                reject_present(&self.config_host_path, "WARP config path is not valid")?;
                reject_present(&self.auth_host_path, "WARP auth path is not valid")?;
                for value in [
                    self.state_volume_name.as_deref(),
                    self.enrollment_id.as_deref(),
                ] {
                    if !value.is_some_and(path_safe_component) {
                        return Err(LiveConfigError::InvalidValue(
                            "WARP runtime identity is invalid",
                        ));
                    }
                }
                if !self
                    .identity_secret_ref
                    .as_deref()
                    .is_some_and(valid_secret_ref)
                {
                    return Err(LiveConfigError::InvalidValue(
                        "WARP runtime identity is invalid",
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LiveEgressProfileKind {
    Warp,
    Wireguard,
    Openvpn,
}

impl Default for LiveEgressProfileKind {
    fn default() -> Self {
        Self::Wireguard
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LiveEgressDefaultMode {
    Off,
    PreferProtected,
    RequireProtected,
}

impl Default for LiveEgressDefaultMode {
    fn default() -> Self {
        Self::Off
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct LiveCryptoConfig {
    pub primary_envelope_key_id: String,
    pub primary_token_hash_key_id: String,
    pub primary_audit_key_id: String,
}

impl Default for LiveCryptoConfig {
    fn default() -> Self {
        Self {
            primary_envelope_key_id: "live-envelope-1".to_string(),
            primary_token_hash_key_id: "live-token-hash-1".to_string(),
            primary_audit_key_id: "live-audit-1".to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveConfigError {
    Dependency {
        child: &'static str,
        required: &'static str,
    },
    InvalidValue(&'static str),
    OutOfBounds(&'static str),
    InvalidKeyId(&'static str),
}

impl fmt::Display for LiveConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Dependency { child, required } => {
                write!(formatter, "live.{child} requires live.{required}")
            }
            Self::InvalidValue(message) => formatter.write_str(message),
            Self::OutOfBounds(label) => {
                write!(formatter, "live.{label} is outside its safe bound")
            }
            Self::InvalidKeyId(label) => write!(formatter, "live.{label} is not path-safe"),
        }
    }
}

impl Error for LiveConfigError {}

fn require_flag(
    child_enabled: bool,
    parent_enabled: bool,
    child: &'static str,
    required: &'static str,
) -> Result<(), LiveConfigError> {
    if child_enabled && !parent_enabled {
        return Err(LiveConfigError::Dependency { child, required });
    }
    Ok(())
}

fn bounded<T>(label: &'static str, value: T, minimum: T, maximum: T) -> Result<(), LiveConfigError>
where
    T: PartialOrd,
{
    if value < minimum || value > maximum {
        return Err(LiveConfigError::OutOfBounds(label));
    }
    Ok(())
}

fn path_safe_component(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

fn valid_secret_ref(value: &str) -> bool {
    value.split_once(':').is_some_and(|(scope, key)| {
        matches!(scope, "global" | "profile") && path_safe_component(key)
    })
}

fn safe_runtime_path(value: &str) -> bool {
    let trimmed = value.trim();
    !trimmed.is_empty()
        && trimmed.len() <= 1_024
        && !trimmed.as_bytes().contains(&0)
        && !Path::new(trimmed)
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir))
}

fn require_profile_path(value: Option<&str>) -> Result<(), LiveConfigError> {
    if !value.is_some_and(safe_runtime_path) {
        return Err(LiveConfigError::InvalidValue(
            "egress profile configuration path is invalid",
        ));
    }
    Ok(())
}

fn reject_present(value: &Option<String>, message: &'static str) -> Result<(), LiveConfigError> {
    if value.is_some() {
        return Err(LiveConfigError::InvalidValue(message));
    }
    Ok(())
}

fn valid_https_probe(value: &str) -> bool {
    reqwest::Url::parse(value).is_ok_and(|url| {
        url.scheme() == "https"
            && url.host_str().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
    })
}

fn valid_dns_probe_host(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 253
        && value.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn valid_egress_profile(kind: LiveEgressProfileKind) -> LiveEgressProfileConfig {
        let mut profile = LiveEgressProfileConfig {
            id: "live-egress-test".to_string(),
            name: "Live egress test".to_string(),
            kind,
            gateway_image: "gateway:test".to_string(),
            expected_egress_ips: vec!["1.1.1.1".parse().unwrap()],
            ..LiveEgressProfileConfig::default()
        };
        match kind {
            LiveEgressProfileKind::Wireguard => {
                profile.config_host_path = Some("/run/elixir/wireguard.conf".to_string());
            }
            LiveEgressProfileKind::Openvpn => {
                profile.config_host_path = Some("/run/elixir/openvpn.conf".to_string());
                profile.auth_host_path = Some("/run/elixir/openvpn.auth".to_string());
            }
            LiveEgressProfileKind::Warp => {
                profile.state_volume_name = Some("live-warp-state".to_string());
                profile.enrollment_id = Some("live-warp-enrollment".to_string());
                profile.identity_secret_ref = Some("global:live-warp-identity".to_string());
            }
        }
        profile
    }

    #[test]
    fn s10_live_config_defaults_are_fully_disabled_and_valid() {
        let config = LiveConfig::default();
        config.validate().unwrap();
        assert!(!config.enabled);
        assert!(!config.catalog_enabled);
        assert!(!config.playback_enabled);
        assert!(!config.client_direct_enabled);
        assert!(!config.relay_enabled);
        assert!(!config.remux_enabled);
        assert!(!config.protected_egress_enabled);
        assert!(!config.stremio_compat_enabled);
        assert!(!config.native_dash_relay_enabled);
        assert!(!config.low_latency_hls_enabled);
        assert!(!config.rtmp_remux_enabled);
        assert!(!config.srt_remux_enabled);
        assert!(!config.allow_private_lan_sources);
    }

    #[test]
    fn s10_live_config_rejects_dependency_and_limit_contradictions() {
        let mut config = LiveConfig {
            playback_enabled: true,
            ..LiveConfig::default()
        };
        assert!(matches!(
            config.validate(),
            Err(LiveConfigError::Dependency {
                child: "playback_enabled",
                required: "catalog_enabled"
            })
        ));
        config = LiveConfig::default();
        config.sessions.per_user = 101;
        config.sessions.server_total = 100;
        assert!(matches!(
            config.validate(),
            Err(LiveConfigError::OutOfBounds("sessions.per_user"))
        ));
        config = LiveConfig::default();
        config.crypto.primary_envelope_key_id = "../escape".to_string();
        assert!(matches!(
            config.validate(),
            Err(LiveConfigError::InvalidKeyId(_))
        ));
        config = LiveConfig::default();
        config.recovery.max_transitions = 0;
        assert!(matches!(
            config.validate(),
            Err(LiveConfigError::OutOfBounds("recovery.max_transitions"))
        ));
    }

    #[test]
    fn s10_live_config_deserializes_partial_sections_with_safe_defaults() {
        let config: LiveConfig = serde_json::from_value(json!({
            "enabled": true,
            "sessions": {"per_user": 2}
        }))
        .unwrap();
        assert!(config.enabled);
        assert_eq!(config.sessions.per_user, 2);
        assert_eq!(config.sessions.server_total, 100);
        assert!(!config.catalog_enabled);
        config.validate().unwrap();
    }

    #[test]
    fn m10_remux_config_is_bounded_and_uncertified_protocols_fail_closed() {
        let mut config = LiveConfig {
            enabled: true,
            catalog_enabled: true,
            playback_enabled: true,
            relay_enabled: true,
            remux_enabled: true,
            ..LiveConfig::default()
        };
        config.validate().expect("certified baseline remux config");

        config.rtmp_remux_enabled = true;
        assert_eq!(
            config.validate(),
            Err(LiveConfigError::InvalidValue(
                "rtmp_remux_enabled requires an exact profile certification"
            ))
        );
        config.rtmp_remux_enabled = false;
        config.srt_remux_enabled = true;
        assert_eq!(
            config.validate(),
            Err(LiveConfigError::InvalidValue(
                "srt_remux_enabled requires an exact platform certification"
            ))
        );

        config.srt_remux_enabled = false;
        config.remux.temp_budget_bytes = 10 * 1_024 * 1_024 * 1_024 - 1;
        assert_eq!(
            config.validate(),
            Err(LiveConfigError::OutOfBounds("remux.temp_budget_bytes"))
        );
        config.remux.temp_budget_bytes = LiveRemuxLimits::default().temp_budget_bytes;
        config.remux.no_output_timeout_seconds = config.remux.segment_seconds;
        assert_eq!(
            config.validate(),
            Err(LiveConfigError::OutOfBounds(
                "remux.no_output_timeout_seconds"
            ))
        );
    }

    #[test]
    fn n11_protected_egress_config_is_kind_specific_bounded_and_fail_closed() {
        for kind in [
            LiveEgressProfileKind::Wireguard,
            LiveEgressProfileKind::Openvpn,
            LiveEgressProfileKind::Warp,
        ] {
            let profile = valid_egress_profile(kind);
            let mut config = LiveConfig {
                enabled: true,
                catalog_enabled: true,
                playback_enabled: true,
                relay_enabled: true,
                protected_egress_enabled: true,
                ..LiveConfig::default()
            };
            config.egress.default_mode = LiveEgressDefaultMode::PreferProtected;
            config.egress.default_policy_id = Some(profile.id.clone());
            config.egress.default_allow_fallback = true;
            config.egress.profiles = vec![profile];
            config.validate().expect("valid protected egress profile");
        }

        let mut config = LiveConfig {
            enabled: true,
            catalog_enabled: true,
            playback_enabled: true,
            relay_enabled: true,
            protected_egress_enabled: true,
            ..LiveConfig::default()
        };
        config.egress.profiles = vec![valid_egress_profile(LiveEgressProfileKind::Warp)];
        config.egress.profiles[0].identity_secret_ref = Some("instance:secret".to_string());
        assert!(matches!(
            config.validate(),
            Err(LiveConfigError::InvalidValue(
                "WARP runtime identity is invalid"
            ))
        ));

        config.egress.profiles = vec![valid_egress_profile(LiveEgressProfileKind::Wireguard)];
        config.egress.profiles[0].expected_egress_ips = vec!["::ffff:127.0.0.1".parse().unwrap()];
        assert!(matches!(
            config.validate(),
            Err(LiveConfigError::InvalidValue(
                "egress profile metadata or readiness proof is invalid"
            ))
        ));

        config.egress.profiles = vec![valid_egress_profile(LiveEgressProfileKind::Wireguard)];
        config.egress.default_mode = LiveEgressDefaultMode::RequireProtected;
        config.egress.default_policy_id = Some("live-egress-test".to_string());
        config.egress.default_allow_fallback = true;
        assert_eq!(
            config.validate(),
            Err(LiveConfigError::InvalidValue(
                "egress fallback is valid only for prefer_protected"
            ))
        );
    }

    #[test]
    fn n11_protected_egress_rejects_shared_warp_state_volumes() {
        let first = valid_egress_profile(LiveEgressProfileKind::Warp);
        let mut second = first.clone();
        second.id = "live-egress-test-two".to_string();
        second.name = "Live egress test two".to_string();
        second.enrollment_id = Some("live-warp-enrollment-two".to_string());
        let mut config = LiveConfig {
            enabled: true,
            catalog_enabled: true,
            playback_enabled: true,
            relay_enabled: true,
            protected_egress_enabled: true,
            ..LiveConfig::default()
        };
        config.egress.profiles = vec![first, second];
        assert_eq!(
            config.validate(),
            Err(LiveConfigError::InvalidValue(
                "egress WARP state volume names must be unique"
            ))
        );
    }

    #[test]
    fn n11_egress_ip_policy_rejects_non_global_and_ipv4_mapped_addresses() {
        for blocked in [
            "0.0.0.0",
            "10.0.0.1",
            "100.64.0.1",
            "127.0.0.1",
            "169.254.169.254",
            "192.0.0.1",
            "192.0.2.1",
            "192.88.99.1",
            "198.18.0.1",
            "203.0.113.1",
            "::1",
            "::ffff:127.0.0.1",
            "fc00::1",
            "fe80::1",
            "fec0::1",
            "2001:db8::1",
        ] {
            assert!(
                !is_public_egress_ip(blocked.parse().unwrap()),
                "{blocked} must remain blocked"
            );
        }
        for public in ["1.1.1.1", "8.8.8.8", "2606:4700:4700::1111"] {
            assert!(
                is_public_egress_ip(public.parse().unwrap()),
                "{public} must remain usable"
            );
        }
    }
}
