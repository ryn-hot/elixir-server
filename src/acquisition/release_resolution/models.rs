use std::str::FromStr;

use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use uuid::Uuid;

use crate::db::models::MediaType;

macro_rules! string_enum {
    (
        $(#[$meta:meta])*
        pub enum $name:ident {
            $($variant:ident => $value:literal),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
        #[serde(rename_all = "snake_case")]
        pub enum $name {
            $($variant),+
        }

        impl $name {
            pub fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $value),+
                }
            }
        }

        impl FromStr for $name {
            type Err = anyhow::Error;

            fn from_str(value: &str) -> Result<Self> {
                match value.trim().to_ascii_lowercase().as_str() {
                    $($value => Ok(Self::$variant),)+
                    other => bail!("unknown {} value '{other}'", stringify!($name)),
                }
            }
        }
    };
}

string_enum! {
    pub enum ReleaseKind {
        Unknown => "unknown",
        Single => "single",
        MultiEpisode => "multi_episode",
        SeasonPack => "season_pack",
        MultiSeasonPack => "multi_season_pack",
        SeriesPack => "series_pack",
    }
}

impl Default for ReleaseKind {
    fn default() -> Self {
        Self::Unknown
    }
}

string_enum! {
    pub enum ReleaseResolverKind {
        Unresolved => "unresolved",
        MovieSingle => "movie_single",
        TvSonarrStyle => "tv_sonarr_style",
        AnimeShokoStyle => "anime_shoko_style",
    }
}

impl Default for ReleaseResolverKind {
    fn default() -> Self {
        Self::Unresolved
    }
}

string_enum! {
    pub enum ReleaseConfidence {
        High => "high",
        Medium => "medium",
        Low => "low",
        ReviewRequired => "review_required",
    }
}

impl Default for ReleaseConfidence {
    fn default() -> Self {
        Self::Low
    }
}

string_enum! {
    pub enum AcquisitionReleaseState {
        Candidate => "candidate",
        Planned => "planned",
        ReviewRequired => "review_required",
        Staging => "staging",
        Ready => "ready",
        Submitted => "submitted",
        Downloading => "downloading",
        Materializing => "materializing",
        Completed => "completed",
        Failed => "failed",
        Cancelled => "cancelled",
    }
}

impl Default for AcquisitionReleaseState {
    fn default() -> Self {
        Self::Candidate
    }
}

string_enum! {
    pub enum ReleaseCoverageKind {
        SingleEpisode => "single_episode",
        MultiEpisodeRange => "multi_episode_range",
        SeasonPack => "season_pack",
        MultiSeasonPack => "multi_season_pack",
        SeriesPack => "series_pack",
        ManualOverride => "manual_override",
        HashVerified => "hash_verified",
    }
}

string_enum! {
    pub enum ReleaseCoverageState {
        Planned => "planned",
        Selected => "selected",
        Submitted => "submitted",
        Imported => "imported",
        ReviewRequired => "review_required",
        Rejected => "rejected",
    }
}

impl Default for ReleaseCoverageState {
    fn default() -> Self {
        Self::Planned
    }
}

string_enum! {
    pub enum ReleaseJobState {
        Staging => "staging",
        Ready => "ready",
        Submitted => "submitted",
        Downloading => "downloading",
        Materializing => "materializing",
        Completed => "completed",
        Failed => "failed",
        Cancelled => "cancelled",
    }
}

impl Default for ReleaseJobState {
    fn default() -> Self {
        Self::Staging
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AcquisitionRelease {
    pub release_id: Uuid,
    pub subscription_id: Option<Uuid>,
    pub source_provider_id: Option<Uuid>,
    pub source_extension_id: String,
    pub owner_id: String,
    pub media_type: MediaType,
    pub title: String,
    pub release_title: String,
    pub source: String,
    pub source_kind: String,
    pub info_hash: Option<String>,
    pub fingerprint: String,
    pub release_kind: ReleaseKind,
    pub resolver_kind: ReleaseResolverKind,
    pub resolver_version: String,
    pub confidence: ReleaseConfidence,
    pub score: Option<f64>,
    pub selected_route_logical_id: Option<String>,
    pub selected_provider_id: Option<Uuid>,
    pub download_id: Option<String>,
    pub remote_release_id: Option<String>,
    pub state: AcquisitionReleaseState,
    pub state_reason: Option<String>,
    pub selected_candidate: Option<JsonValue>,
    pub coverage_plan: Option<JsonValue>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewAcquisitionRelease {
    pub release_id: Option<Uuid>,
    pub subscription_id: Option<Uuid>,
    pub source_provider_id: Option<Uuid>,
    pub source_extension_id: String,
    pub owner_id: String,
    pub media_type: MediaType,
    pub title: String,
    pub release_title: String,
    pub source: String,
    pub source_kind: String,
    pub info_hash: Option<String>,
    pub fingerprint: String,
    pub release_kind: ReleaseKind,
    pub resolver_kind: ReleaseResolverKind,
    pub resolver_version: String,
    pub confidence: ReleaseConfidence,
    pub score: Option<f64>,
    pub selected_route_logical_id: Option<String>,
    pub selected_provider_id: Option<Uuid>,
    pub download_id: Option<String>,
    pub remote_release_id: Option<String>,
    pub state: AcquisitionReleaseState,
    pub state_reason: Option<String>,
    pub selected_candidate: Option<JsonValue>,
    pub coverage_plan: Option<JsonValue>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AcquisitionReleaseFile {
    pub release_file_id: Uuid,
    pub release_id: Uuid,
    pub file_index: Option<i64>,
    pub file_id: Option<String>,
    pub path: String,
    pub basename: String,
    pub size_bytes: Option<i64>,
    pub selectable: bool,
    pub parsed_title: Option<String>,
    pub parsed_season_number: Option<i32>,
    pub parsed_episode_number: Option<i32>,
    pub parsed_episode_end_number: Option<i32>,
    pub parsed_absolute_episode_number: Option<i32>,
    pub parsed_absolute_episode_end_number: Option<i32>,
    pub parsed_air_date: Option<String>,
    pub parsed_quality: Option<String>,
    pub parsed_language: Option<String>,
    pub parsed_release_group: Option<String>,
    pub parser_confidence: ReleaseConfidence,
    pub parser_reason: Option<String>,
    pub raw: Option<JsonValue>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewAcquisitionReleaseFile {
    pub release_file_id: Option<Uuid>,
    pub release_id: Uuid,
    pub file_index: Option<i64>,
    pub file_id: Option<String>,
    pub path: String,
    pub basename: Option<String>,
    pub size_bytes: Option<i64>,
    pub selectable: bool,
    pub parsed_title: Option<String>,
    pub parsed_season_number: Option<i32>,
    pub parsed_episode_number: Option<i32>,
    pub parsed_episode_end_number: Option<i32>,
    pub parsed_absolute_episode_number: Option<i32>,
    pub parsed_absolute_episode_end_number: Option<i32>,
    pub parsed_air_date: Option<String>,
    pub parsed_quality: Option<String>,
    pub parsed_language: Option<String>,
    pub parsed_release_group: Option<String>,
    pub parser_confidence: ReleaseConfidence,
    pub parser_reason: Option<String>,
    pub raw: Option<JsonValue>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AcquisitionReleaseCoverage {
    pub coverage_id: Uuid,
    pub release_id: Uuid,
    pub release_file_id: Option<Uuid>,
    pub target_id: Uuid,
    pub coverage_kind: ReleaseCoverageKind,
    pub confidence: ReleaseConfidence,
    pub score: Option<f64>,
    pub reason: Option<String>,
    pub state: ReleaseCoverageState,
    pub verified_by: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewAcquisitionReleaseCoverage {
    pub coverage_id: Option<Uuid>,
    pub release_id: Uuid,
    pub release_file_id: Option<Uuid>,
    pub target_id: Uuid,
    pub coverage_kind: ReleaseCoverageKind,
    pub confidence: ReleaseConfidence,
    pub score: Option<f64>,
    pub reason: Option<String>,
    pub state: ReleaseCoverageState,
    pub verified_by: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AcquisitionReleaseJob {
    pub release_job_id: Uuid,
    pub release_id: Uuid,
    pub route_logical_id: String,
    pub provider_id: Option<Uuid>,
    pub download_id: Option<String>,
    pub remote_release_id: Option<String>,
    pub state: ReleaseJobState,
    pub state_reason: Option<String>,
    pub active: bool,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewAcquisitionReleaseJob {
    pub release_job_id: Option<Uuid>,
    pub release_id: Uuid,
    pub route_logical_id: String,
    pub provider_id: Option<Uuid>,
    pub download_id: Option<String>,
    pub remote_release_id: Option<String>,
    pub state: ReleaseJobState,
    pub state_reason: Option<String>,
    pub active: bool,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default)]
pub struct ReleaseJobStateUpdate {
    pub state: ReleaseJobState,
    pub state_reason: Option<String>,
    pub active: Option<bool>,
    pub download_id: Option<String>,
    pub remote_release_id: Option<String>,
    pub completed_at: Option<DateTime<Utc>>,
}

string_enum! {
    pub enum AnimeFileHashStatus {
        Pending => "pending",
        Hashed => "hashed",
        Invalidated => "invalidated",
        Failed => "failed",
    }
}

string_enum! {
    pub enum AniDbFileLookupStatus {
        Pending => "pending",
        Hit => "hit",
        NoSuchFile => "no_such_file",
        Banned => "banned",
        TransportFailed => "transport_failed",
        Disabled => "disabled",
    }
}

string_enum! {
    pub enum AnimeEpisodeType {
        Normal => "normal",
        Special => "special",
        Credits => "credits",
        Trailer => "trailer",
        Parody => "parody",
        Other => "other",
        Movie => "movie",
    }
}

string_enum! {
    pub enum AnimeMatchOutcome {
        Planned => "planned",
        Verified => "verified",
        Mismatch => "mismatch",
        NoMatch => "no_match",
        Deferred => "deferred",
        Rejected => "rejected",
    }
}

string_enum! {
    pub enum AnimeMismatchState {
        Open => "open",
        Resolved => "resolved",
        Ignored => "ignored",
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AcquisitionAnimeGraphSnapshot {
    pub graph_snapshot_id: Uuid,
    pub subscription_id: Option<Uuid>,
    pub owner_id: String,
    pub media_type: MediaType,
    pub anilist_root_id: Option<i64>,
    pub anilist_season_id: Option<i64>,
    pub anilist_status: Option<String>,
    pub anilist_next_airing_at: Option<DateTime<Utc>>,
    pub tvdb_series_id: Option<i64>,
    pub anidb_anime_id: Option<i64>,
    pub fingerprint: String,
    pub graph: JsonValue,
    pub aliases: JsonValue,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewAcquisitionAnimeGraphSnapshot {
    pub graph_snapshot_id: Option<Uuid>,
    pub subscription_id: Option<Uuid>,
    pub owner_id: String,
    pub media_type: MediaType,
    pub anilist_root_id: Option<i64>,
    pub anilist_season_id: Option<i64>,
    pub anilist_status: Option<String>,
    pub anilist_next_airing_at: Option<DateTime<Utc>>,
    pub tvdb_series_id: Option<i64>,
    pub anidb_anime_id: Option<i64>,
    pub fingerprint: String,
    pub graph: JsonValue,
    pub aliases: JsonValue,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AcquisitionAnimeCandidateParse {
    pub candidate_parse_id: Uuid,
    pub release_id: Uuid,
    pub source_provider_id: Option<Uuid>,
    pub source_candidate_id: Option<String>,
    pub release_title: String,
    pub normalized_title: Option<String>,
    pub parsed: JsonValue,
    pub confidence: ReleaseConfidence,
    pub review_reasons: JsonValue,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewAcquisitionAnimeCandidateParse {
    pub candidate_parse_id: Option<Uuid>,
    pub release_id: Uuid,
    pub source_provider_id: Option<Uuid>,
    pub source_candidate_id: Option<String>,
    pub release_title: String,
    pub normalized_title: Option<String>,
    pub parsed: JsonValue,
    pub confidence: ReleaseConfidence,
    pub review_reasons: JsonValue,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AcquisitionFileHash {
    pub file_hash_id: Uuid,
    pub release_file_id: Option<Uuid>,
    pub local_file_id: Option<String>,
    pub file_path: String,
    pub size_bytes: i64,
    pub mtime_fingerprint: Option<String>,
    pub ed2k: Option<String>,
    pub crc32: Option<String>,
    pub hash_status: AnimeFileHashStatus,
    pub hash_computed_at: Option<DateTime<Utc>>,
    pub hash_invalidated_at: Option<DateTime<Utc>>,
    pub filename_history: JsonValue,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewAcquisitionFileHash {
    pub file_hash_id: Option<Uuid>,
    pub release_file_id: Option<Uuid>,
    pub local_file_id: Option<String>,
    pub file_path: String,
    pub size_bytes: i64,
    pub mtime_fingerprint: Option<String>,
    pub ed2k: Option<String>,
    pub crc32: Option<String>,
    pub hash_status: AnimeFileHashStatus,
    pub hash_computed_at: Option<DateTime<Utc>>,
    pub hash_invalidated_at: Option<DateTime<Utc>>,
    pub filename_history: JsonValue,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AcquisitionAniDbFileCache {
    pub lookup_key: String,
    pub ed2k: String,
    pub size_bytes: i64,
    pub lookup_status: AniDbFileLookupStatus,
    pub anidb_file_id: Option<i64>,
    pub anidb_anime_id: Option<i64>,
    pub anidb_episode_ids: JsonValue,
    pub anidb_group_id: Option<i64>,
    pub anidb_group_name: Option<String>,
    pub anidb_group_short_name: Option<String>,
    pub anidb_version: Option<i64>,
    pub anidb_source: Option<String>,
    pub anidb_quality: Option<String>,
    pub anidb_audio_languages: JsonValue,
    pub anidb_subtitle_languages: JsonValue,
    pub anidb_state_flags: JsonValue,
    pub anidb_original_filename: Option<String>,
    pub released_at: Option<DateTime<Utc>>,
    pub raw_response: Option<String>,
    pub positive_cached_at: Option<DateTime<Utc>>,
    pub negative_cached_until: Option<DateTime<Utc>>,
    pub last_lookup_attempt_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewAcquisitionAniDbFileCache {
    pub lookup_key: String,
    pub ed2k: String,
    pub size_bytes: i64,
    pub lookup_status: AniDbFileLookupStatus,
    pub anidb_file_id: Option<i64>,
    pub anidb_anime_id: Option<i64>,
    pub anidb_episode_ids: JsonValue,
    pub anidb_group_id: Option<i64>,
    pub anidb_group_name: Option<String>,
    pub anidb_group_short_name: Option<String>,
    pub anidb_version: Option<i64>,
    pub anidb_source: Option<String>,
    pub anidb_quality: Option<String>,
    pub anidb_audio_languages: JsonValue,
    pub anidb_subtitle_languages: JsonValue,
    pub anidb_state_flags: JsonValue,
    pub anidb_original_filename: Option<String>,
    pub released_at: Option<DateTime<Utc>>,
    pub raw_response: Option<String>,
    pub positive_cached_at: Option<DateTime<Utc>>,
    pub negative_cached_until: Option<DateTime<Utc>>,
    pub last_lookup_attempt_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AcquisitionAniDbFileXref {
    pub xref_id: Uuid,
    pub lookup_key: String,
    pub release_file_id: Option<Uuid>,
    pub anidb_file_id: Option<i64>,
    pub anidb_anime_id: i64,
    pub anidb_episode_id: i64,
    pub episode_type: AnimeEpisodeType,
    pub percentage_start: i64,
    pub percentage_end: i64,
    pub episode_order: i64,
    pub provider: String,
    pub confidence: ReleaseConfidence,
    pub is_manual_override: bool,
    pub created_from_release_id: Option<Uuid>,
    pub created_from_target_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewAcquisitionAniDbFileXref {
    pub xref_id: Option<Uuid>,
    pub lookup_key: String,
    pub release_file_id: Option<Uuid>,
    pub anidb_file_id: Option<i64>,
    pub anidb_anime_id: i64,
    pub anidb_episode_id: i64,
    pub episode_type: AnimeEpisodeType,
    pub percentage_start: i64,
    pub percentage_end: i64,
    pub episode_order: i64,
    pub provider: String,
    pub confidence: ReleaseConfidence,
    pub is_manual_override: bool,
    pub created_from_release_id: Option<Uuid>,
    pub created_from_target_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AcquisitionAnimeMatchAttempt {
    pub match_attempt_id: Uuid,
    pub release_id: Option<Uuid>,
    pub release_file_id: Option<Uuid>,
    pub attempted_providers: JsonValue,
    pub selected_provider: Option<String>,
    pub ed2k: Option<String>,
    pub size_bytes: Option<i64>,
    pub candidate_fingerprint: Option<String>,
    pub planned_targets: JsonValue,
    pub verified_targets: JsonValue,
    pub outcome: AnimeMatchOutcome,
    pub rejection_reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewAcquisitionAnimeMatchAttempt {
    pub match_attempt_id: Option<Uuid>,
    pub release_id: Option<Uuid>,
    pub release_file_id: Option<Uuid>,
    pub attempted_providers: JsonValue,
    pub selected_provider: Option<String>,
    pub ed2k: Option<String>,
    pub size_bytes: Option<i64>,
    pub candidate_fingerprint: Option<String>,
    pub planned_targets: JsonValue,
    pub verified_targets: JsonValue,
    pub outcome: AnimeMatchOutcome,
    pub rejection_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AcquisitionAnimeIdentityMismatch {
    pub mismatch_id: Uuid,
    pub release_id: Option<Uuid>,
    pub release_file_id: Option<Uuid>,
    pub target_id: Option<Uuid>,
    pub planned_target: JsonValue,
    pub verified_identity: JsonValue,
    pub provider: String,
    pub confidence: ReleaseConfidence,
    pub state: AnimeMismatchState,
    pub reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewAcquisitionAnimeIdentityMismatch {
    pub mismatch_id: Option<Uuid>,
    pub release_id: Option<Uuid>,
    pub release_file_id: Option<Uuid>,
    pub target_id: Option<Uuid>,
    pub planned_target: JsonValue,
    pub verified_identity: JsonValue,
    pub provider: String,
    pub confidence: ReleaseConfidence,
    pub state: AnimeMismatchState,
    pub reason: Option<String>,
}
