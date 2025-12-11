use std::{net::SocketAddr, str::FromStr};

use anyhow::{Context, Result};
use config::{Config, Environment as ConfigEnvironment, File};
use serde::Deserialize;

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
    pub library: LibraryConfig,
    #[serde(default)]
    pub metadata: MetadataConfig,
    #[serde(default)]
    pub playback: PlaybackConfig,
    #[serde(default)]
    pub telemetry: TelemetryConfig,
}

impl Settings {
    pub fn load() -> Result<Self> {
        let env_override = std::env::var("ELIXIR_ENV").ok().map(|v| v.to_lowercase());

        let mut builder = Config::builder()
            .set_default("environment", RunEnvironment::default().as_str())?
            .set_default("server.host", default_host())?
            .set_default("server.port", default_port())?
            .set_default("network.mdns_enabled", true)?
            .set_default("network.mdns_name", default_mdns_name())?
            .set_default("network.wan_enabled", true)?
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
            .set_default("metadata.enable_cinemeta", true)?
            .set_default("metadata.enable_wikidata", true)?
            .set_default("metadata.enable_anilist", true)?
            .set_default("metadata.enable_aniapi", true)?
            .set_default("metadata.enable_consumet", true)?
            .set_default("metadata.request_timeout_seconds", 10)?
            .set_default("metadata.ttl_seconds", 604800)?
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
            .add_source(File::with_name("config/default").required(false))
            .add_source(File::with_name("config/local").required(false))
            .add_source(ConfigEnvironment::with_prefix("ELIXIR").separator("__"));

        if let Some(env_value) = env_override {
            let parsed =
                RunEnvironment::from_str(&env_value).context("invalid ELIXIR_ENV value")?;
            builder = builder.set_override("environment", parsed.as_str())?;
        }

        let settings: Settings = builder
            .build()
            .context("unable to build configuration sources")?
            .try_deserialize()
            .context("configuration deserialization failed")?;

        Ok(settings)
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
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            mdns_enabled: default_true(),
            mdns_name: default_mdns_name(),
            wan_enabled: default_true(),
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
pub struct LibraryConfig {
    #[serde(default = "default_local_root")]
    pub local_root: String,
    #[serde(default = "default_scan_interval_seconds")]
    pub scan_interval_seconds: u64,
}

impl Default for LibraryConfig {
    fn default() -> Self {
        Self {
            local_root: default_local_root(),
            scan_interval_seconds: default_scan_interval_seconds(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetadataConfig {
    #[serde(default = "default_true")]
    pub enable_cinemeta: bool,
    #[serde(default = "default_true")]
    pub enable_wikidata: bool,
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
            enable_cinemeta: default_true(),
            enable_wikidata: default_true(),
            enable_anilist: default_true(),
            enable_aniapi: default_true(),
            enable_consumet: default_true(),
            request_timeout_seconds: default_request_timeout(),
            ttl_seconds: default_ttl_seconds(),
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
        }
    }
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
    "sqlite://elixir.db".to_string()
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

fn default_true() -> bool {
    true
}

fn default_request_timeout() -> u64 {
    10
}

fn default_ttl_seconds() -> u64 {
    60 * 60 * 24 * 7
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
