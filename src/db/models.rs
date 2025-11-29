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
    pub tmdb: Option<String>,
    pub imdb: Option<String>,
    pub tvdb: Option<String>,
    pub anilist: Option<String>,
    pub mal: Option<String>,
}

impl Default for ExternalIds {
    fn default() -> Self {
        Self {
            tmdb: None,
            imdb: None,
            tvdb: None,
            anilist: None,
            mal: None,
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
    pub scan_state: ScanState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct PlaybackSession {
    pub id: Uuid,
    pub user_id: Uuid,
    pub media_file_id: Uuid,
    pub mode: PlaybackMode,
    pub network_type: Option<String>,
    pub client_capabilities: Option<serde_json::Value>,
    pub transcode_state: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// refresh tokens removed for simplified auth
