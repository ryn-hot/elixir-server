use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::types::Json;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "text")]
#[serde(rename_all = "lowercase")]
pub enum MediaType {
    Movie,
    Series,
    Anime,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "text")]
#[serde(rename_all = "lowercase")]
pub enum ScanState {
    Ok,
    Missing,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "text")]
#[serde(rename_all = "snake_case")]
pub enum PlaybackMode {
    DirectPlay,
    Transcode,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "text")]
#[serde(rename_all = "lowercase")]
pub enum PlaybackState {
    Active,
    Ended,
    Error,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "text")]
#[serde(rename_all = "lowercase")]
pub enum ExtensionKind {
    Module,
    Connector,
    Blueprint,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "text")]
#[serde(rename_all = "lowercase")]
pub enum ExtensionTrustLevel {
    Verified,
    Community,
    Untrusted,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "text")]
#[serde(rename_all = "snake_case")]
pub enum SlotCardinality {
    One,
    Many,
    ZeroOrOne,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "text")]
#[serde(rename_all = "lowercase")]
pub enum ProviderHealthState {
    Unknown,
    Healthy,
    Degraded,
    Unhealthy,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "text")]
#[serde(rename_all = "lowercase")]
pub enum BindingStatus {
    Pending,
    Applied,
    Failed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "text")]
#[serde(rename_all = "lowercase")]
pub enum OrchestratorRunStatus {
    Pending,
    Running,
    Failed,
    Completed,
    Canceled,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "text")]
#[serde(rename_all = "lowercase")]
pub enum OperationStepStatus {
    Pending,
    Running,
    Failed,
    Completed,
    Skipped,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "text")]
#[serde(rename_all = "lowercase")]
pub enum SecretScope {
    Instance,
    Provider,
    Global,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct User {
    pub id: Uuid,
    pub email: String,
    pub password_hash: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct ServerInstance {
    pub id: Uuid,
    pub user_id: Uuid,
    pub device_name: String,
    pub lan_addresses: Json<Vec<String>>,
    pub wan_direct_endpoint: Option<String>,
    pub overlay_endpoint: Option<String>,
    pub last_seen_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalIds {
    pub imdb: Option<String>,
    pub tmdb: Option<String>,
    pub tvdb: Option<String>,
    pub tvdb_series: Option<String>,
    pub tvdb_movie: Option<String>,
    pub anilist: Option<String>,
    pub anidb: Option<String>,
    pub mal: Option<String>,
    pub kitsu: Option<String>,
}

impl Default for ExternalIds {
    fn default() -> Self {
        Self {
            imdb: None,
            tmdb: None,
            tvdb: None,
            tvdb_series: None,
            tvdb_movie: None,
            anilist: None,
            anidb: None,
            mal: None,
            kitsu: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct SourceConfig {
    pub id: Uuid,
    pub server_id: Uuid,
    pub extension_id: String,
    pub config_json: Option<serde_json::Value>,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Extension {
    pub extension_id: String,
    pub name: String,
    pub version: String,
    pub kind: ExtensionKind,
    pub publisher_name: Option<String>,
    pub signing_key_id: Option<String>,
    pub trust_level: ExtensionTrustLevel,
    pub manifest_json: serde_json::Value,
    pub package_hash: Option<String>,
    pub installed_at: DateTime<Utc>,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct ExtensionInstance {
    pub instance_id: Uuid,
    pub extension_id: String,
    pub instance_name: String,
    pub config_json: Option<serde_json::Value>,
    pub runtime_version: Option<String>,
    pub rollback_version: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Provider {
    pub provider_id: Uuid,
    pub instance_id: Uuid,
    pub capability: String,
    pub slot_id: String,
    pub cardinality: SlotCardinality,
    pub implementation: Option<String>,
    pub endpoint_json: Option<serde_json::Value>,
    pub health_state: ProviderHealthState,
    pub last_healthcheck_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Binding {
    pub binding_id: Uuid,
    pub consumer_provider_id: Uuid,
    pub requires_capability: String,
    pub requires_slot_id: String,
    pub target_provider_id: Uuid,
    pub binding_params_json: Option<serde_json::Value>,
    pub status: BindingStatus,
    pub last_error: Option<String>,
    pub last_applied_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct DesiredBlueprint {
    pub desired_id: Uuid,
    pub blueprint_extension_id: String,
    pub blueprint_version: String,
    pub params_json: Option<serde_json::Value>,
    pub decisions_json: Option<serde_json::Value>,
    pub applied: bool,
    pub created_at: DateTime<Utc>,
    pub applied_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Secret {
    pub secret_id: Uuid,
    pub scope: SecretScope,
    pub scope_id: Option<Uuid>,
    pub key: String,
    pub value_encrypted: String,
    pub created_at: DateTime<Utc>,
    pub rotatable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct OrchestratorRun {
    pub run_id: Uuid,
    pub source: String,
    pub status: OrchestratorRunStatus,
    pub phase: Option<String>,
    pub plan_json: Option<serde_json::Value>,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct OperationStep {
    pub step_id: Uuid,
    pub run_id: Uuid,
    pub step_index: i32,
    pub action_type: String,
    pub action_json: Option<serde_json::Value>,
    pub status: OperationStepStatus,
    pub error: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct RuntimeLog {
    pub log_id: Uuid,
    pub instance_id: Uuid,
    pub log_uri: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct MediaItem {
    pub id: Uuid,
    pub r#type: MediaType,
    pub external_ids: Option<Json<ExternalIds>>,
    pub title: String,
    pub year: Option<i32>,
    pub season: Option<i32>,
    pub episode: Option<i32>,
    pub runtime_seconds: Option<i32>,
    pub metadata_json: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct MediaFile {
    pub id: Uuid,
    pub media_item_id: Uuid,
    pub source_config_id: Option<Uuid>,
    pub path: String,
    pub size_bytes: Option<i64>,
    pub container: Option<String>,
    pub video_codec: Option<String>,
    pub audio_codec: Option<String>,
    pub width: Option<i32>,
    pub height: Option<i32>,
    pub bitrate_bps: Option<i64>,
    pub hash: Option<String>,
    pub extension_metadata: Option<serde_json::Value>,
    pub scan_state: ScanState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Movie {
    pub id: Uuid,
    pub title: String,
    pub year: Option<i32>,
    pub external_imdb: Option<String>,
    pub external_tmdb: Option<String>,
    pub metadata_json: Option<serde_json::Value>,
    pub runtime_seconds: Option<i32>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Series {
    pub id: Uuid,
    pub title: String,
    pub year: Option<i32>,
    pub library_type: String,
    pub external_imdb: Option<String>,
    pub external_tvdb_series: Option<String>,
    pub external_anilist: Option<String>,
    pub metadata_json: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Season {
    pub id: Uuid,
    pub series_id: Uuid,
    pub season_number: i32,
    pub title: Option<String>,
    pub external_anilist: Option<String>,
    pub metadata_json: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Episode {
    pub id: Uuid,
    pub series_id: Uuid,
    pub season_id: Uuid,
    pub season_number: i32,
    pub episode_number: i32,
    pub absolute_episode_number: Option<i32>,
    pub title: Option<String>,
    pub runtime_seconds: Option<i32>,
    pub metadata_json: Option<serde_json::Value>,
    pub has_file: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct MovieFileLink {
    pub movie_id: Uuid,
    pub media_file_id: Uuid,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct EpisodeFileLink {
    pub episode_id: Uuid,
    pub media_file_id: Uuid,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct ArtworkRef {
    pub id: Uuid,
    pub owner_type: String,
    pub owner_id: Uuid,
    pub kind: String,
    pub url: String,
    pub language: Option<String>,
    pub width: Option<i32>,
    pub height: Option<i32>,
    pub provider: Option<String>,
    pub score: Option<f32>,
    pub metadata_json: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct ArtworkCache {
    pub id: Uuid,
    pub artwork_id: Uuid,
    pub local_path: String,
    pub cached_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct MediaTrack {
    pub id: Uuid,
    pub media_file_id: Uuid,
    pub track_type: String,
    pub language: Option<String>,
    pub title: Option<String>,
    pub codec: Option<String>,
    pub channels: Option<i32>,
    pub is_default: bool,
    pub is_forced: bool,
    pub stream_index: Option<i32>,
    pub metadata_json: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct ExternalSubtitle {
    pub id: Uuid,
    pub media_file_id: Uuid,
    pub path: String,
    pub language: Option<String>,
    pub title: Option<String>,
    pub format: Option<String>,
    pub is_default: bool,
    pub is_forced: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct ReviewQueueEntry {
    pub id: Uuid,
    pub media_file_id: Uuid,
    pub status: String,
    pub confidence: Option<f32>,
    pub hint_json: Option<serde_json::Value>,
    pub candidates_json: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct ClassifierOverride {
    pub id: Uuid,
    pub library_type: String,
    pub normalized_key: String,
    pub imdb_id: Option<String>,
    pub anilist_id: Option<String>,
    pub tvdb_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct AnimeEpisodeMeta {
    pub id: Uuid,
    pub season_id: Uuid,
    pub episode_number: i32,
    pub title: Option<String>,
    pub snapshot_url: Option<String>,
    pub duration_seconds: Option<i32>,
    pub raw_json: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct EpisodeProviderKey {
    pub id: Uuid,
    pub episode_id: Uuid,
    pub provider: String,
    pub provider_key: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct SeasonExternalId {
    pub id: Uuid,
    pub season_id: Uuid,
    pub provider: String,
    pub external_id: String,
    pub confidence: Option<f32>,
    pub source: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct PlaybackSession {
    pub id: Uuid,
    pub user_id: Uuid,
    pub server_id: Option<Uuid>,
    pub media_file_id: Uuid,
    pub mode: PlaybackMode,
    pub state: PlaybackState,
    pub network_type: Option<String>,
    pub logical_position_seconds: f32,
    pub duration_seconds: Option<i32>,
    pub client_capabilities: Option<serde_json::Value>,
    pub transcode_state: Option<serde_json::Value>,
    pub token: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// refresh tokens removed for simplified auth

impl ExtensionKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            ExtensionKind::Module => "module",
            ExtensionKind::Connector => "connector",
            ExtensionKind::Blueprint => "blueprint",
        }
    }
}

impl ExtensionTrustLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            ExtensionTrustLevel::Verified => "verified",
            ExtensionTrustLevel::Community => "community",
            ExtensionTrustLevel::Untrusted => "untrusted",
        }
    }
}

impl SlotCardinality {
    pub fn as_str(&self) -> &'static str {
        match self {
            SlotCardinality::One => "one",
            SlotCardinality::Many => "many",
            SlotCardinality::ZeroOrOne => "zero_or_one",
        }
    }
}

impl ProviderHealthState {
    pub fn as_str(&self) -> &'static str {
        match self {
            ProviderHealthState::Unknown => "unknown",
            ProviderHealthState::Healthy => "healthy",
            ProviderHealthState::Degraded => "degraded",
            ProviderHealthState::Unhealthy => "unhealthy",
        }
    }
}

impl BindingStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            BindingStatus::Pending => "pending",
            BindingStatus::Applied => "applied",
            BindingStatus::Failed => "failed",
        }
    }
}

impl OrchestratorRunStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            OrchestratorRunStatus::Pending => "pending",
            OrchestratorRunStatus::Running => "running",
            OrchestratorRunStatus::Failed => "failed",
            OrchestratorRunStatus::Completed => "completed",
            OrchestratorRunStatus::Canceled => "canceled",
        }
    }
}

impl OperationStepStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            OperationStepStatus::Pending => "pending",
            OperationStepStatus::Running => "running",
            OperationStepStatus::Failed => "failed",
            OperationStepStatus::Completed => "completed",
            OperationStepStatus::Skipped => "skipped",
        }
    }
}

impl SecretScope {
    pub fn as_str(&self) -> &'static str {
        match self {
            SecretScope::Instance => "instance",
            SecretScope::Provider => "provider",
            SecretScope::Global => "global",
        }
    }
}

impl std::str::FromStr for ExtensionKind {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "module" => Ok(ExtensionKind::Module),
            "connector" => Ok(ExtensionKind::Connector),
            "blueprint" => Ok(ExtensionKind::Blueprint),
            other => Err(format!("unknown extension kind '{other}'")),
        }
    }
}

impl std::str::FromStr for ExtensionTrustLevel {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "verified" => Ok(ExtensionTrustLevel::Verified),
            "community" => Ok(ExtensionTrustLevel::Community),
            "untrusted" => Ok(ExtensionTrustLevel::Untrusted),
            other => Err(format!("unknown trust level '{other}'")),
        }
    }
}

impl std::str::FromStr for SlotCardinality {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "one" => Ok(SlotCardinality::One),
            "many" => Ok(SlotCardinality::Many),
            "zero_or_one" => Ok(SlotCardinality::ZeroOrOne),
            other => Err(format!("unknown cardinality '{other}'")),
        }
    }
}

impl std::str::FromStr for ProviderHealthState {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "unknown" => Ok(ProviderHealthState::Unknown),
            "healthy" => Ok(ProviderHealthState::Healthy),
            "degraded" => Ok(ProviderHealthState::Degraded),
            "unhealthy" => Ok(ProviderHealthState::Unhealthy),
            other => Err(format!("unknown health state '{other}'")),
        }
    }
}

impl std::str::FromStr for BindingStatus {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(BindingStatus::Pending),
            "applied" => Ok(BindingStatus::Applied),
            "failed" => Ok(BindingStatus::Failed),
            other => Err(format!("unknown binding status '{other}'")),
        }
    }
}

impl std::str::FromStr for OrchestratorRunStatus {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(OrchestratorRunStatus::Pending),
            "running" => Ok(OrchestratorRunStatus::Running),
            "failed" => Ok(OrchestratorRunStatus::Failed),
            "completed" => Ok(OrchestratorRunStatus::Completed),
            "canceled" => Ok(OrchestratorRunStatus::Canceled),
            other => Err(format!("unknown orchestrator run status '{other}'")),
        }
    }
}

impl std::str::FromStr for OperationStepStatus {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(OperationStepStatus::Pending),
            "running" => Ok(OperationStepStatus::Running),
            "failed" => Ok(OperationStepStatus::Failed),
            "completed" => Ok(OperationStepStatus::Completed),
            "skipped" => Ok(OperationStepStatus::Skipped),
            other => Err(format!("unknown operation step status '{other}'")),
        }
    }
}

impl std::str::FromStr for SecretScope {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "instance" => Ok(SecretScope::Instance),
            "provider" => Ok(SecretScope::Provider),
            "global" => Ok(SecretScope::Global),
            other => Err(format!("unknown secret scope '{other}'")),
        }
    }
}
