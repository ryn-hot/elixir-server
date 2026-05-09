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
