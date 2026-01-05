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
