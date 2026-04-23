use std::{
    collections::HashSet,
    net::SocketAddr,
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::{Context, Result};
use config::{Config, Environment as ConfigEnvironment, File};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize)]
pub struct Settings {
    #[serde(default)]
    pub environment: RunEnvironment,
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub network: NetworkConfig,
    #[serde(default)]
    pub database: DatabaseConfig,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub secrets: SecretsConfig,
    #[serde(default)]
    pub library: LibraryConfig,
    #[serde(default)]
    pub extensions: ExtensionsConfig,
    #[serde(default)]
    pub metadata: MetadataConfig,
    #[serde(default)]
    pub classifier: ClassifierConfig,
    #[serde(default)]
    pub playback: PlaybackConfig,
    #[serde(default)]
    pub telemetry: TelemetryConfig,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            environment: RunEnvironment::default(),
            server: ServerConfig::default(),
            network: NetworkConfig::default(),
            database: DatabaseConfig::default(),
            auth: AuthConfig::default(),
            secrets: SecretsConfig::default(),
            library: LibraryConfig::default(),
            extensions: ExtensionsConfig::default(),
            metadata: MetadataConfig::default(),
            classifier: ClassifierConfig::default(),
            playback: PlaybackConfig::default(),
            telemetry: TelemetryConfig::default(),
        }
    }
}

#[derive(Debug, Clone)]
struct ConfigPaths {
    base_dir: PathBuf,
    default_file: PathBuf,
    local_file: PathBuf,
}

impl ConfigPaths {
    fn new(config_dir: PathBuf) -> Self {
        let config_dir = absolutize_path(config_dir);
        let base_dir = match config_dir.file_name().and_then(|name| name.to_str()) {
            Some("config") => config_dir
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_else(|| config_dir.clone()),
            _ => config_dir.clone(),
        };
        Self {
            base_dir,
            default_file: config_dir.join("default.toml"),
            local_file: config_dir.join("local.toml"),
        }
    }
}

impl Settings {
    pub fn load() -> Result<Self> {
        let env_override = std::env::var("ELIXIR_ENV").ok().map(|v| v.to_lowercase());
        let config_paths = discover_config_paths();

        let mut builder = Config::builder()
            .set_default("environment", RunEnvironment::default().as_str())?
            .set_default("server.host", default_host())?
            .set_default("server.port", default_port())?
            .set_default("network.mdns_enabled", true)?
            .set_default("network.mdns_name", default_mdns_name())?
            .set_default("network.wan_enabled", true)?
            .set_default("network.vpn.enabled", false)?
            .set_default("network.vpn.detect_host_vpn", true)?
            .set_default(
                "network.vpn.wireguard_gateway_image",
                default_wireguard_gateway_image(),
            )?
            .set_default(
                "network.vpn.wireguard_config_secret",
                default_wireguard_config_secret(),
            )?
            .set_default("network.vpn.auto_wrap_qbittorrent", true)?
            .set_default("network.vpn.auto_wrap_nzbget", true)?
            .set_default("database.url", default_database_url())?
            .set_default(
                "auth.access_token_ttl_minutes",
                default_access_token_ttl_minutes(),
            )?
            .set_default("auth.access_token_secret", default_access_token_secret())?
            .set_default("library.local_root", default_local_root())?
            .set_default(
                "library.scan_interval_seconds",
                default_scan_interval_seconds(),
            )?
            .set_default("library.artwork_cache_dir", default_artwork_cache_dir())?
            .set_default("library.hash_dedupe_enabled", default_false())?
            .set_default("library.sonarr.enabled", default_false())?
            .set_default("extensions.registries", Vec::<String>::new())?
            .set_default("extensions.storage_root", default_extensions_root())?
            .set_default("extensions.bundled_dir", default_extensions_bundled_dir())?
            .set_default("extensions.core_extensions", default_core_extensions())?
            .set_default(
                "extensions.downloader_profile",
                default_downloader_profile().as_str(),
            )?
            .set_default("extensions.allow_unsigned", default_false())?
            .set_default("extensions.allow_directory_install", default_false())?
            .set_default(
                "extensions.docker.auto_start_runtime",
                default_extensions_docker_auto_start_runtime(),
            )?
            .set_default(
                "extensions.docker.startup_timeout_seconds",
                default_extensions_docker_startup_timeout_seconds(),
            )?
            .set_default(
                "extensions.docker.startup_poll_interval_millis",
                default_extensions_docker_startup_poll_interval_millis(),
            )?
            .set_default(
                "extensions.reconcile_interval_seconds",
                default_reconcile_interval_seconds(),
            )?
            .set_default(
                "extensions.registry_refresh_interval_seconds",
                default_registry_refresh_interval_seconds(),
            )?
            .set_default(
                "extensions.proxy_runtime_update_interval_seconds",
                default_proxy_runtime_update_interval_seconds(),
            )?
            .set_default(
                "extensions.reconcile_retry_attempts",
                default_reconcile_retry_attempts(),
            )?
            .set_default(
                "extensions.reconcile_retry_backoff_seconds",
                default_reconcile_retry_backoff_seconds(),
            )?
            .set_default(
                "extensions.reconcile_startup_settle_seconds",
                default_reconcile_startup_settle_seconds(),
            )?
            .set_default(
                "extensions.apply_lock_ttl_seconds",
                default_apply_lock_ttl_seconds(),
            )?
            .set_default("metadata.enable_tvdb", true)?
            .set_default("metadata.tvdb_base_url", default_tvdb_base_url())?
            .set_default("metadata.cinemeta_base_url", default_cinemeta_base_url())?
            .set_default("metadata.enable_cinemeta", true)?
            .set_default("metadata.enable_anilist", true)?
            .set_default("metadata.enable_aniapi", true)?
            .set_default("metadata.enable_consumet", true)?
            .set_default("metadata.request_timeout_seconds", 10)?
            .set_default("metadata.ttl_seconds", 604800)?
            .set_default("classifier.tvdb_base_url", default_tvdb_base_url())?
            .set_default("classifier.anizip_base_url", default_anizip_base_url())?
            .set_default(
                "classifier.request_timeout_seconds",
                default_classifier_timeout_seconds(),
            )?
            .set_default(
                "playback.session_ttl_seconds",
                default_session_ttl_seconds(),
            )?
            .set_default(
                "playback.cleanup_interval_seconds",
                default_cleanup_interval_seconds(),
            )?
            .set_default("playback.default_max_resolution", default_max_resolution())?
            .set_default(
                "playback.default_supported_containers",
                default_supported_containers(),
            )?
            .set_default(
                "playback.default_supported_video_codecs",
                default_supported_video_codecs(),
            )?
            .set_default(
                "playback.default_supported_audio_codecs",
                default_supported_audio_codecs(),
            )?
            .set_default(
                "playback.default_wan_max_bitrate_bps",
                default_wan_bitrate_bps(),
            )?
            .set_default(
                "playback.default_lan_max_bitrate_bps",
                default_lan_bitrate_bps(),
            )?
            .set_default("telemetry.log_directives", default_log_directives())?
            .add_source(File::from(config_paths.default_file.clone()).required(false))
            .add_source(File::from(config_paths.local_file.clone()).required(false))
            .add_source(ConfigEnvironment::with_prefix("ELIXIR").separator("__"));

        if let Some(env_value) = env_override {
            let parsed =
                RunEnvironment::from_str(&env_value).context("invalid ELIXIR_ENV value")?;
            builder = builder.set_override("environment", parsed.as_str())?;
        }

        let mut settings: Settings = builder
            .build()
            .context("unable to build configuration sources")?
            .try_deserialize()
            .context("configuration deserialization failed")?;
        settings.normalize_paths(&config_paths.base_dir)?;

        Ok(settings)
    }

    fn normalize_paths(&mut self, base_dir: &Path) -> Result<()> {
        self.database.url = normalize_database_url(&self.database.url, base_dir)
            .context("normalizing database.url")?;
        self.library.local_root = normalize_path(&self.library.local_root, base_dir);
        self.library.artwork_cache_dir = normalize_path(&self.library.artwork_cache_dir, base_dir);
        self.extensions.storage_root = normalize_path(&self.extensions.storage_root, base_dir);
        self.extensions.bundled_dir = normalize_path(&self.extensions.bundled_dir, base_dir);
        if self.metadata.tvdb_base_url.trim().is_empty() {
            self.metadata.tvdb_base_url = self.classifier.tvdb_base_url.clone();
        }
        if self
            .metadata
            .tvdb_api_key
            .as_deref()
            .map(str::trim)
            .map(str::is_empty)
            .unwrap_or(true)
        {
            self.metadata.tvdb_api_key = self.classifier.tvdb_api_key.clone();
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RunEnvironment {
    Development,
    Production,
}

impl Default for RunEnvironment {
    fn default() -> Self {
        RunEnvironment::Development
    }
}

impl RunEnvironment {
    pub fn as_str(&self) -> &'static str {
        match self {
            RunEnvironment::Development => "development",
            RunEnvironment::Production => "production",
        }
    }
}

impl FromStr for RunEnvironment {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.to_lowercase().as_str() {
            "development" | "dev" => Ok(RunEnvironment::Development),
            "production" | "prod" => Ok(RunEnvironment::Production),
            other => anyhow::bail!("unknown environment '{other}'"),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
        }
    }
}

impl ServerConfig {
    pub fn socket_addr(&self) -> Result<SocketAddr> {
        format!("{}:{}", self.host, self.port)
            .parse()
            .context("invalid server bind address")
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct NetworkConfig {
    #[serde(default = "default_true")]
    pub mdns_enabled: bool,
    #[serde(default = "default_mdns_name")]
    pub mdns_name: String,
    #[serde(default = "default_true")]
    pub wan_enabled: bool,
    #[serde(default)]
    pub vpn: VpnConfig,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            mdns_enabled: default_true(),
            mdns_name: default_mdns_name(),
            wan_enabled: default_true(),
            vpn: VpnConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct VpnConfig {
    #[serde(default = "default_false")]
    pub enabled: bool,
    #[serde(default = "default_true")]
    pub detect_host_vpn: bool,
    #[serde(default = "default_wireguard_config_secret")]
    pub wireguard_config_secret: String,
    #[serde(default = "default_true")]
    pub auto_wrap_qbittorrent: bool,
    #[serde(default = "default_true")]
    pub auto_wrap_nzbget: bool,
    #[serde(default = "default_wireguard_gateway_image")]
    pub wireguard_gateway_image: String,
}

impl Default for VpnConfig {
    fn default() -> Self {
        Self {
            enabled: default_false(),
            detect_host_vpn: default_true(),
            wireguard_config_secret: default_wireguard_config_secret(),
            auto_wrap_qbittorrent: default_true(),
            auto_wrap_nzbget: default_true(),
            wireguard_gateway_image: default_wireguard_gateway_image(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseConfig {
    #[serde(default = "default_database_url")]
    pub url: String,
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    #[serde(default = "default_connect_timeout_seconds")]
    pub connect_timeout_seconds: u64,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: default_database_url(),
            max_connections: default_max_connections(),
            connect_timeout_seconds: default_connect_timeout_seconds(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct AuthConfig {
    #[serde(default = "default_access_token_secret")]
    pub access_token_secret: String,
    #[serde(default = "default_access_token_ttl_minutes")]
    pub access_token_ttl_minutes: u64,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            access_token_secret: default_access_token_secret(),
            access_token_ttl_minutes: default_access_token_ttl_minutes(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SecretsConfig {
    #[serde(default)]
    pub master_key: Option<String>,
}

impl Default for SecretsConfig {
    fn default() -> Self {
        Self { master_key: None }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct LibraryConfig {
    #[serde(default = "default_local_root")]
    pub local_root: String,
    #[serde(default = "default_scan_interval_seconds")]
    pub scan_interval_seconds: u64,
    #[serde(default = "default_artwork_cache_dir")]
    pub artwork_cache_dir: String,
    #[serde(default)]
    pub sonarr: SonarrConfig,
    #[serde(default = "default_false")]
    pub hash_dedupe_enabled: bool,
}

impl Default for LibraryConfig {
    fn default() -> Self {
        Self {
            local_root: default_local_root(),
            scan_interval_seconds: default_scan_interval_seconds(),
            artwork_cache_dir: default_artwork_cache_dir(),
            sonarr: SonarrConfig::default(),
            hash_dedupe_enabled: default_false(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct SonarrConfig {
    #[serde(default = "default_false")]
    pub enabled: bool,
    pub base_url: Option<String>,
    pub api_key: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExtensionsConfig {
    #[serde(default)]
    pub registries: Vec<String>,
    #[serde(default = "default_extensions_root")]
    pub storage_root: String,
    #[serde(default = "default_extensions_bundled_dir")]
    pub bundled_dir: String,
    #[serde(default = "default_core_extensions")]
    pub core_extensions: Vec<String>,
    #[serde(default = "default_downloader_profile")]
    pub downloader_profile: DownloaderPerformanceProfile,
    #[serde(default = "default_false")]
    pub allow_unsigned: bool,
    #[serde(default = "default_false")]
    pub allow_directory_install: bool,
    #[serde(default)]
    pub docker: ExtensionsDockerConfig,
    #[serde(default = "default_reconcile_interval_seconds")]
    pub reconcile_interval_seconds: u64,
    #[serde(default = "default_registry_refresh_interval_seconds")]
    pub registry_refresh_interval_seconds: u64,
    #[serde(default = "default_proxy_runtime_update_interval_seconds")]
    pub proxy_runtime_update_interval_seconds: u64,
    #[serde(default = "default_reconcile_retry_attempts")]
    pub reconcile_retry_attempts: u32,
    #[serde(default = "default_reconcile_retry_backoff_seconds")]
    pub reconcile_retry_backoff_seconds: u64,
    #[serde(default = "default_reconcile_startup_settle_seconds")]
    pub reconcile_startup_settle_seconds: u64,
    #[serde(default = "default_apply_lock_ttl_seconds")]
    pub apply_lock_ttl_seconds: u64,
}

impl Default for ExtensionsConfig {
    fn default() -> Self {
        Self {
            registries: Vec::new(),
            storage_root: default_extensions_root(),
            bundled_dir: default_extensions_bundled_dir(),
            core_extensions: default_core_extensions(),
            downloader_profile: default_downloader_profile(),
            allow_unsigned: default_false(),
            allow_directory_install: default_false(),
            docker: ExtensionsDockerConfig::default(),
            reconcile_interval_seconds: default_reconcile_interval_seconds(),
            registry_refresh_interval_seconds: default_registry_refresh_interval_seconds(),
            proxy_runtime_update_interval_seconds: default_proxy_runtime_update_interval_seconds(),
            reconcile_retry_attempts: default_reconcile_retry_attempts(),
            reconcile_retry_backoff_seconds: default_reconcile_retry_backoff_seconds(),
            reconcile_startup_settle_seconds: default_reconcile_startup_settle_seconds(),
            apply_lock_ttl_seconds: default_apply_lock_ttl_seconds(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DownloaderPerformanceProfile {
    Balanced,
    Aggressive,
}

impl DownloaderPerformanceProfile {
    pub fn as_str(&self) -> &'static str {
        match self {
            DownloaderPerformanceProfile::Balanced => "balanced",
            DownloaderPerformanceProfile::Aggressive => "aggressive",
        }
    }

    pub fn from_setting_value(value: Option<&serde_json::Value>, default: Self) -> Self {
        value
            .and_then(|entry| serde_json::from_value(entry.clone()).ok())
            .unwrap_or(default)
    }
}

impl Default for DownloaderPerformanceProfile {
    fn default() -> Self {
        default_downloader_profile()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExtensionsDockerConfig {
    #[serde(
        default = "default_extensions_docker_auto_start_runtime",
        alias = "auto_start_desktop"
    )]
    pub auto_start_runtime: bool,
    #[serde(default = "default_extensions_docker_startup_timeout_seconds")]
    pub startup_timeout_seconds: u64,
    #[serde(default = "default_extensions_docker_startup_poll_interval_millis")]
    pub startup_poll_interval_millis: u64,
}

impl Default for ExtensionsDockerConfig {
    fn default() -> Self {
        Self {
            auto_start_runtime: default_extensions_docker_auto_start_runtime(),
            startup_timeout_seconds: default_extensions_docker_startup_timeout_seconds(),
            startup_poll_interval_millis: default_extensions_docker_startup_poll_interval_millis(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetadataConfig {
    #[serde(default = "default_true")]
    pub enable_tvdb: bool,
    #[serde(default = "default_tvdb_base_url")]
    pub tvdb_base_url: String,
    #[serde(default)]
    pub tvdb_api_key: Option<String>,
    #[serde(default = "default_cinemeta_base_url")]
    pub cinemeta_base_url: String,
    #[serde(default = "default_true")]
    pub enable_cinemeta: bool,
    #[serde(default = "default_true")]
    pub enable_anilist: bool,
    #[serde(default = "default_true")]
    pub enable_aniapi: bool,
    #[serde(default = "default_true")]
    pub enable_consumet: bool,
    #[serde(default = "default_request_timeout")]
    pub request_timeout_seconds: u64,
    #[serde(default = "default_ttl_seconds")]
    pub ttl_seconds: u64,
}

impl Default for MetadataConfig {
    fn default() -> Self {
        Self {
            enable_tvdb: default_true(),
            tvdb_base_url: default_tvdb_base_url(),
            tvdb_api_key: None,
            cinemeta_base_url: default_cinemeta_base_url(),
            enable_cinemeta: default_true(),
            enable_anilist: default_true(),
            enable_aniapi: default_true(),
            enable_consumet: default_true(),
            request_timeout_seconds: default_request_timeout(),
            ttl_seconds: default_ttl_seconds(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClassifierConfig {
    #[serde(default = "default_tvdb_base_url")]
    pub tvdb_base_url: String,
    #[serde(default)]
    pub tvdb_api_key: Option<String>,
    #[serde(default = "default_anizip_base_url")]
    pub anizip_base_url: String,
    #[serde(default = "default_classifier_timeout_seconds")]
    pub request_timeout_seconds: u64,
}

impl Default for ClassifierConfig {
    fn default() -> Self {
        Self {
            tvdb_base_url: default_tvdb_base_url(),
            tvdb_api_key: None,
            anizip_base_url: default_anizip_base_url(),
            request_timeout_seconds: default_classifier_timeout_seconds(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PlaybackConfig {
    #[serde(default = "default_session_ttl_seconds")]
    pub session_ttl_seconds: u64,
    #[serde(default = "default_cleanup_interval_seconds")]
    pub cleanup_interval_seconds: u64,
    #[serde(default = "default_max_resolution")]
    pub default_max_resolution: String,
    #[serde(default = "default_supported_containers")]
    pub default_supported_containers: Vec<String>,
    #[serde(default = "default_supported_video_codecs")]
    pub default_supported_video_codecs: Vec<String>,
    #[serde(default = "default_supported_audio_codecs")]
    pub default_supported_audio_codecs: Vec<String>,
    #[serde(default = "default_wan_bitrate_bps")]
    pub default_wan_max_bitrate_bps: Option<i64>,
    #[serde(default = "default_lan_bitrate_bps")]
    pub default_lan_max_bitrate_bps: Option<i64>,
    #[serde(default)]
    pub profiles: PlaybackProfiles,
}

impl Default for PlaybackConfig {
    fn default() -> Self {
        Self {
            session_ttl_seconds: default_session_ttl_seconds(),
            cleanup_interval_seconds: default_cleanup_interval_seconds(),
            default_max_resolution: default_max_resolution(),
            default_supported_containers: default_supported_containers(),
            default_supported_video_codecs: default_supported_video_codecs(),
            default_supported_audio_codecs: default_supported_audio_codecs(),
            default_wan_max_bitrate_bps: default_wan_bitrate_bps(),
            default_lan_max_bitrate_bps: default_lan_bitrate_bps(),
            profiles: PlaybackProfiles::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct PlaybackProfiles {
    pub lan: Option<PlaybackProfileOverride>,
    pub wan: Option<PlaybackProfileOverride>,
    #[serde(default)]
    pub agents: std::collections::HashMap<String, PlaybackProfileOverride>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PlaybackProfileOverride {
    pub max_resolution: Option<String>,
    pub supported_containers: Option<Vec<String>>,
    pub supported_video_codecs: Option<Vec<String>>,
    pub supported_audio_codecs: Option<Vec<String>>,
    pub max_bitrate_bps: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TelemetryConfig {
    #[serde(default = "default_log_directives")]
    pub log_directives: String,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            log_directives: default_log_directives(),
        }
    }
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}

fn default_port() -> u16 {
    44301
}

fn default_database_url() -> String {
    "sqlite://data/elixir.db".to_string()
}

fn default_access_token_secret() -> String {
    // Development-only default; should be overridden in production.
    "dev-change-me-access-secret".to_string()
}

fn default_log_directives() -> String {
    "info,elixir_server=debug,sqlx=warn,axum::rejection=info".to_string()
}

fn default_max_connections() -> u32 {
    10
}

fn default_connect_timeout_seconds() -> u64 {
    5
}

fn default_access_token_ttl_minutes() -> u64 {
    60
}

fn default_local_root() -> String {
    "./media".to_string()
}

fn default_scan_interval_seconds() -> u64 {
    600
}

fn default_artwork_cache_dir() -> String {
    "data/artwork".to_string()
}

fn default_extensions_root() -> String {
    "data/extensions".to_string()
}

fn default_extensions_bundled_dir() -> String {
    "extensions/bundled".to_string()
}

fn default_core_extensions() -> Vec<String> {
    vec![
        "elixir.modules.qbittorrent".to_string(),
        "elixir.modules.nzbget".to_string(),
    ]
}

fn default_downloader_profile() -> DownloaderPerformanceProfile {
    DownloaderPerformanceProfile::Balanced
}

fn default_reconcile_interval_seconds() -> u64 {
    60
}

fn default_registry_refresh_interval_seconds() -> u64 {
    900
}

fn default_proxy_runtime_update_interval_seconds() -> u64 {
    6 * 60 * 60
}

fn default_reconcile_retry_attempts() -> u32 {
    2
}

fn default_reconcile_retry_backoff_seconds() -> u64 {
    5
}

fn default_reconcile_startup_settle_seconds() -> u64 {
    15
}

fn default_apply_lock_ttl_seconds() -> u64 {
    300
}

fn default_extensions_docker_auto_start_runtime() -> bool {
    true
}

fn default_extensions_docker_startup_timeout_seconds() -> u64 {
    90
}

fn default_extensions_docker_startup_poll_interval_millis() -> u64 {
    1_000
}

fn default_true() -> bool {
    true
}

fn default_false() -> bool {
    false
}

fn default_request_timeout() -> u64 {
    10
}

fn default_ttl_seconds() -> u64 {
    60 * 60 * 24 * 7
}

fn default_classifier_timeout_seconds() -> u64 {
    12
}

fn default_tvdb_base_url() -> String {
    "https://api4.thetvdb.com/v4".to_string()
}

fn default_cinemeta_base_url() -> String {
    "https://v3-cinemeta.strem.io".to_string()
}

fn default_anizip_base_url() -> String {
    "https://api.ani.zip".to_string()
}

fn default_session_ttl_seconds() -> u64 {
    60 * 60 * 6 // 6 hours
}

fn default_cleanup_interval_seconds() -> u64 {
    15 * 60 // 15 minutes
}

fn default_mdns_name() -> String {
    "Elixir Server".to_string()
}

fn default_wireguard_gateway_image() -> String {
    "qmcgaw/gluetun:v3.39.0".to_string()
}

fn default_wireguard_config_secret() -> String {
    "global:wireguard_config".to_string()
}

fn default_max_resolution() -> String {
    "1080p".to_string()
}

fn default_supported_containers() -> Vec<String> {
    vec!["mp4".to_string(), "mkv".to_string()]
}

fn default_supported_video_codecs() -> Vec<String> {
    vec!["h264".to_string(), "hevc".to_string()]
}

fn default_supported_audio_codecs() -> Vec<String> {
    vec!["aac".to_string(), "ac3".to_string(), "opus".to_string()]
}

fn default_wan_bitrate_bps() -> Option<i64> {
    Some(6_000_000)
}

fn default_lan_bitrate_bps() -> Option<i64> {
    Some(20_000_000)
}

fn discover_config_paths() -> ConfigPaths {
    if let Ok(dir) = std::env::var("ELIXIR_CONFIG_DIR") {
        return ConfigPaths::new(expand_tilde_path(Path::new(dir.trim())));
    }

    if let Ok(file) = std::env::var("ELIXIR_CONFIG_FILE") {
        let file_path = expand_tilde_path(Path::new(file.trim()));
        let config_dir = file_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("config"));
        return ConfigPaths::new(config_dir);
    }

    let mut candidate_dirs = Vec::new();
    if let Ok(cwd) = std::env::current_dir() {
        candidate_dirs.extend(candidate_config_dirs(&cwd));
    }
    if let Ok(exe_path) = std::env::current_exe() {
        if let Some(exe_dir) = exe_path.parent() {
            candidate_dirs.extend(candidate_config_dirs(exe_dir));
        }
    }

    let mut seen = HashSet::new();
    candidate_dirs.retain(|dir| seen.insert(dir.clone()));

    if let Some(dir) = candidate_dirs
        .iter()
        .find(|dir| has_config_toml(dir))
        .cloned()
    {
        return ConfigPaths::new(dir);
    }

    if let Some(dir) = candidate_dirs.iter().find(|dir| dir.is_dir()).cloned() {
        return ConfigPaths::new(dir);
    }

    ConfigPaths::new(PathBuf::from("config"))
}

fn candidate_config_dirs(start: &Path) -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    for ancestor in start.ancestors() {
        dirs.push(ancestor.join("config"));
        dirs.push(ancestor.join("elixir-server").join("config"));
    }
    dirs
}

fn has_config_toml(dir: &Path) -> bool {
    dir.join("default.toml").is_file() || dir.join("local.toml").is_file()
}

fn normalize_database_url(raw: &str, base_dir: &Path) -> Result<String> {
    let lowered = raw.to_ascii_lowercase();
    if !lowered.starts_with("sqlite:") {
        return Ok(raw.to_string());
    }
    if lowered.starts_with("sqlite::memory") || lowered.starts_with("sqlite://:memory:") {
        return Ok(raw.to_string());
    }

    if let Some(rest) = raw.strip_prefix("sqlite://") {
        let (path_part, query_part) = split_query(rest);
        if path_part.is_empty() || Path::new(path_part).is_absolute() {
            return Ok(raw.to_string());
        }
        let absolute = normalize_path(path_part, base_dir);
        if let Some(query) = query_part {
            return Ok(format!("sqlite://{absolute}?{query}"));
        }
        return Ok(format!("sqlite://{absolute}"));
    }

    if let Some(rest) = raw.strip_prefix("sqlite:") {
        let (path_part, query_part) = split_query(rest);
        if path_part.is_empty()
            || path_part.starts_with(":memory:")
            || Path::new(path_part).is_absolute()
        {
            return Ok(raw.to_string());
        }
        let absolute = normalize_path(path_part, base_dir);
        if let Some(query) = query_part {
            return Ok(format!("sqlite:{absolute}?{query}"));
        }
        return Ok(format!("sqlite:{absolute}"));
    }

    Ok(raw.to_string())
}

fn split_query(input: &str) -> (&str, Option<&str>) {
    match input.split_once('?') {
        Some((path, query)) if !query.is_empty() => (path, Some(query)),
        Some((path, _)) => (path, None),
        None => (input, None),
    }
}

fn normalize_path(raw: &str, base_dir: &Path) -> String {
    let raw = raw.trim();
    if raw.is_empty() {
        return raw.to_string();
    }
    let expanded = expand_tilde_path(Path::new(raw));
    if expanded.is_absolute() {
        return expanded.to_string_lossy().to_string();
    }
    base_dir.join(expanded).to_string_lossy().to_string()
}

fn expand_tilde_path(path: &Path) -> PathBuf {
    let raw = path.to_string_lossy();
    if raw == "~" {
        if let Some(home) = home_dir() {
            return home;
        }
    } else if let Some(rest) = raw.strip_prefix("~/") {
        if let Some(home) = home_dir() {
            return home.join(rest);
        }
    }
    path.to_path_buf()
}

fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}

fn absolutize_path(path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        return path;
    }
    match std::env::current_dir() {
        Ok(cwd) => cwd.join(path),
        Err(_) => path,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::tempdir;

    #[test]
    fn normalize_paths_resolves_relative_entries() -> Result<()> {
        let base = tempdir()?;
        let mut settings = Settings::default();
        settings.database.url = "sqlite://data/elixir.db".to_string();
        settings.library.local_root = "./media".to_string();
        settings.library.artwork_cache_dir = "data/artwork".to_string();
        settings.extensions.storage_root = "data/extensions".to_string();
        settings.extensions.bundled_dir = "extensions/bundled".to_string();

        settings.normalize_paths(base.path())?;

        assert_eq!(
            settings.database.url,
            format!(
                "sqlite://{}",
                base.path().join("data/elixir.db").to_string_lossy()
            )
        );
        assert_eq!(
            PathBuf::from(&settings.library.local_root),
            base.path().join("media")
        );
        assert_eq!(
            PathBuf::from(&settings.library.artwork_cache_dir),
            base.path().join("data/artwork")
        );
        assert_eq!(
            PathBuf::from(&settings.extensions.storage_root),
            base.path().join("data/extensions")
        );
        assert_eq!(
            PathBuf::from(&settings.extensions.bundled_dir),
            base.path().join("extensions/bundled")
        );

        Ok(())
    }

    #[test]
    fn normalize_database_url_keeps_memory_url() -> Result<()> {
        let base = tempdir()?;
        let url = normalize_database_url("sqlite::memory:?cache=shared", base.path())?;
        assert_eq!(url, "sqlite::memory:?cache=shared");
        Ok(())
    }
}
