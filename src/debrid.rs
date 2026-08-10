use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use once_cell::sync::Lazy;
use reqwest::header::RANGE;
use reqwest::{Client, Method, StatusCode, Url};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{Row, TypeInfo, Value as SqlxValue, ValueRef};
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

use crate::acquisition::{
    anime_matching::{
        acquisition_anime_deterministic_state, acquisition_anime_match_batch_input,
        assess_acquisition_anime_model_audio_profile, assess_acquisition_anime_provider_file_audio,
        bind_exact_single_anime_provider_file,
        model_derived_anime_coverage_plans_with_file_selection_support,
    },
    episode_state::sync_library_episode_acquisition_state_for_target,
    language_policy::LanguagePreferenceAssessmentState,
    release_resolution::{
        anime::{
            AnimeCandidateInput, AnimeCandidateScoringContext, AnimeCandidateTarget,
            AnimeCoverageOptions, AnimeFileCoveragePlan, AnimeReleaseFileInput, AnimeScopedAlias,
            anime_parser_diagnostics, parse_anime_release_title,
            plan_anime_file_coverage_with_options, score_anime_candidate,
        },
        fingerprint::{
            ReleaseFingerprintInput, build_release_fingerprint, extract_magnet_info_hash,
        },
        hashing::{HashFileJob, queue_anime_hash_file},
        models::{
            AcquisitionRelease, AcquisitionReleaseCoverage, AcquisitionReleaseFile,
            AcquisitionReleaseState, NewAcquisitionRelease, NewAcquisitionReleaseCoverage,
            NewAcquisitionReleaseFile, NewAcquisitionReleaseJob, ReleaseConfidence,
            ReleaseCoverageKind, ReleaseCoverageState, ReleaseJobState, ReleaseKind,
            ReleaseResolverKind,
        },
        movie::{
            MOVIE_RADARR_STYLE_RESOLVER_VERSION, MovieReleaseFileSelectionInput,
            select_movie_main_file,
        },
        movie_radarr_parser::MovieRadarrStyleParser,
        review_candidates::SYNTHETIC_SOURCE_CANDIDATE_FILE_ID,
        store::{
            get_release, get_release_by_download_id, list_release_coverage, list_release_files,
            update_release_coverage_review_state, upsert_release, upsert_release_coverage,
            upsert_release_file, upsert_release_job,
        },
        tv::{TvCoverageOptions, TvReleaseFileInput, TvSonarrStyleResolver, TvTarget},
    },
    subscriptions::{
        AcquisitionTargetState, AcquisitionTargetStateUpdate, get_subscription, get_target,
        list_subscription_targets, reset_target_for_candidate_retry, update_target_state,
    },
};
use crate::anime_matching::{
    ANIME_MATCH_SCHEMA_VERSION, AnimeDeterministicResult, AnimeMatchAssistProvenance,
    AnimeMatchAssistResult, AnimeMatchAssistSource, AnimeMatchAudioProfile,
    AnimeMatchFallbackReason, AnimeMatchingService, DeterministicMatchState,
};
use crate::db::models::{
    ExtensionKind, ExtensionTrustLevel, MediaType, ProviderHealthState, ProviderReadinessPhase,
    SecretScope, SlotCardinality,
};
use crate::download_broker::{
    DEBRID_DEFAULT_LOGICAL_ID, DEFAULT_ROUTE_OWNER_ID, DownloadBrokerProviderKind,
    DownloadBrokerRole, list_acquisition_routes, list_logical_downloaders,
};
use crate::extensions::store::{
    ExtensionStore, NewExtension, NewExtensionInstance, NewProvider, NewSecret,
};
use crate::http::handlers::acquisition_sources::{AcquisitionCandidate, AcquisitionCandidateFile};
use crate::orchestrator::model::ProviderEndpoint;
use crate::orchestrator::planner::stable_provider_id;
use crate::runtime::RuntimePaths;
use crate::secrets::SecretsManager;
use crate::state::AppState;

#[allow(dead_code)]
pub const DEBRID_EXTENSION_ID: &str = "elixir.modules.debrid";
pub const LEGACY_REAL_DEBRID_EXTENSION_ID: &str = "elixir.modules.real_debrid";
#[allow(dead_code)]
pub const REAL_DEBRID_EXTENSION_ID: &str = LEGACY_REAL_DEBRID_EXTENSION_ID;
pub const REAL_DEBRID_IMPLEMENTATION: &str = "real_debrid";
pub const REAL_DEBRID_TOKEN_SECRET_KEY: &str = "real_debrid_api_token";
pub const DEBRID_REAL_DEBRID_TOKEN_SECRET_KEY: &str = "debrid.real_debrid.api_token";
pub const DEBRID_TORBOX_TOKEN_SECRET_KEY: &str = "debrid.torbox.api_token";
pub const DEBRID_ALL_DEBRID_TOKEN_SECRET_KEY: &str = "debrid.all_debrid.api_token";
pub const DEBRID_PREMIUMIZE_TOKEN_SECRET_KEY: &str = "debrid.premiumize.api_token";

const REAL_DEBRID_API_BASE: &str = "https://api.real-debrid.com/rest/1.0";
#[allow(dead_code)]
const TORBOX_API_BASE: &str = "https://api.torbox.app/v1/api";
#[allow(dead_code)]
const ALL_DEBRID_API_BASE: &str = "https://api.alldebrid.com/v4";
#[allow(dead_code)]
const PREMIUMIZE_API_BASE: &str = "https://www.premiumize.me/api";
const REAL_DEBRID_POLL_INTERVAL_SECONDS: u64 = 20;
pub const DEFAULT_DEBRID_CONCURRENT_DOWNLOADS: i64 = 1;
pub const MIN_DEBRID_CONCURRENT_DOWNLOADS: i64 = 1;
pub const MAX_DEBRID_CONCURRENT_DOWNLOADS: i64 = 16;
const REAL_DEBRID_USER_AGENT: &str = "Elixir/0.1 Real-Debrid";
const TORBOX_USER_AGENT: &str = "Elixir/0.1 TorBox";
const ALL_DEBRID_USER_AGENT: &str = "Elixir/0.1 AllDebrid";
const MAX_DOWNLOAD_FILE_NAME_LEN: usize = 180;
const DEBRID_SELECTION_POLICY_VERSION: &str = "rr4f-deterministic-selection-v1";
const ANIME_DEBRID_CANDIDATE_RETRY_SECONDS: i64 = 30;
const DEBRID_ACTIVE_SERVICE_CONFIG_KEY: &str = "activeService";
pub const DEBRID_CONCURRENT_DOWNLOADS_CONFIG_KEY: &str = "maxConcurrentDownloads";
const TORBOX_CREATE_TORRENT_MINUTE_LIMIT: usize = 10;
const TORBOX_CREATE_TORRENT_HOUR_LIMIT: usize = 60;
const TORBOX_CREATE_TORRENT_MINUTE_WINDOW: Duration = Duration::from_secs(60);
const TORBOX_CREATE_TORRENT_HOUR_WINDOW: Duration = Duration::from_secs(60 * 60);

static TORBOX_CREATE_TORRENT_LIMITERS: Lazy<
    Mutex<HashMap<String, TorBoxCreateTorrentRateLimiter>>,
> = Lazy::new(|| Mutex::new(HashMap::new()));
static PREMIUMIZE_DIRECTDL_RELEASES: Lazy<Mutex<HashMap<String, PremiumizeDirectDlSnapshot>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DebridServiceKind {
    RealDebrid,
    TorBox,
    AllDebrid,
    Premiumize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DebridRouteAttempt {
    pub provider_id: Uuid,
    pub instance_id: Uuid,
    pub implementation: String,
    pub display_name: String,
    pub health_state: ProviderHealthState,
    pub attempt_key: String,
}

#[allow(dead_code)]
impl DebridServiceKind {
    pub const ALL: [Self; 4] = [
        Self::RealDebrid,
        Self::TorBox,
        Self::AllDebrid,
        Self::Premiumize,
    ];

    pub fn implementation_id(self) -> &'static str {
        match self {
            Self::RealDebrid => REAL_DEBRID_IMPLEMENTATION,
            Self::TorBox => "torbox",
            Self::AllDebrid => "all_debrid",
            Self::Premiumize => "premiumize",
        }
    }

    pub fn display_name(self) -> &'static str {
        match self {
            Self::RealDebrid => "Real-Debrid",
            Self::TorBox => "TorBox",
            Self::AllDebrid => "AllDebrid",
            Self::Premiumize => "Premiumize",
        }
    }

    pub fn secret_key(self) -> &'static str {
        match self {
            Self::RealDebrid => DEBRID_REAL_DEBRID_TOKEN_SECRET_KEY,
            Self::TorBox => DEBRID_TORBOX_TOKEN_SECRET_KEY,
            Self::AllDebrid => DEBRID_ALL_DEBRID_TOKEN_SECRET_KEY,
            Self::Premiumize => DEBRID_PREMIUMIZE_TOKEN_SECRET_KEY,
        }
    }

    pub fn legacy_secret_key(self) -> Option<&'static str> {
        match self {
            Self::RealDebrid => Some(REAL_DEBRID_TOKEN_SECRET_KEY),
            Self::TorBox | Self::AllDebrid | Self::Premiumize => None,
        }
    }

    pub fn secret_keys_for_read(self) -> Vec<&'static str> {
        let mut keys = vec![self.secret_key()];
        if let Some(legacy_key) = self.legacy_secret_key()
            && legacy_key != self.secret_key()
        {
            keys.push(legacy_key);
        }
        keys
    }

    pub fn api_base_url(self) -> &'static str {
        match self {
            Self::RealDebrid => REAL_DEBRID_API_BASE,
            Self::TorBox => TORBOX_API_BASE,
            Self::AllDebrid => ALL_DEBRID_API_BASE,
            Self::Premiumize => PREMIUMIZE_API_BASE,
        }
    }

    pub fn docs_url(self) -> &'static str {
        match self {
            Self::RealDebrid => "https://app.real-debrid.com/",
            Self::TorBox => {
                "https://www.postman.com/torbox/torbox-api/documentation/b6l9hbv/main-api"
            }
            Self::AllDebrid => "https://docs.alldebrid.com/",
            Self::Premiumize => "https://www.premiumize.me/api",
        }
    }

    pub fn from_implementation_id(value: &str) -> Result<Self> {
        Self::from_str(value)
    }
}

impl FromStr for DebridServiceKind {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        let normalized = value.trim().to_ascii_lowercase().replace(['-', ' '], "_");
        match normalized.as_str() {
            "real_debrid" | "realdebrid" | "rd" => Ok(Self::RealDebrid),
            "torbox" | "tor_box" | "tb" => Ok(Self::TorBox),
            "all_debrid" | "alldebrid" | "ad" => Ok(Self::AllDebrid),
            "premiumize" | "pm" => Ok(Self::Premiumize),
            _ => bail!("unsupported debrid service '{value}'"),
        }
    }
}

impl fmt::Display for DebridServiceKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.implementation_id())
    }
}

pub fn active_debrid_service_from_config(config_json: Option<&Value>) -> Result<DebridServiceKind> {
    let Some(config) = config_json else {
        return Ok(DebridServiceKind::RealDebrid);
    };
    let Some(value) = config
        .get(DEBRID_ACTIVE_SERVICE_CONFIG_KEY)
        .or_else(|| config.get("active_service"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(DebridServiceKind::RealDebrid);
    };
    DebridServiceKind::from_str(value)
}

pub fn debrid_concurrent_downloads_from_config(config_json: Option<&Value>) -> i64 {
    let Some(config) = config_json else {
        return DEFAULT_DEBRID_CONCURRENT_DOWNLOADS;
    };
    config
        .get(DEBRID_CONCURRENT_DOWNLOADS_CONFIG_KEY)
        .or_else(|| config.get("concurrentDownloads"))
        .or_else(|| config.get("concurrencyCap"))
        .and_then(|value| {
            value
                .as_i64()
                .or_else(|| value.as_u64().and_then(|number| i64::try_from(number).ok()))
                .or_else(|| {
                    value
                        .as_f64()
                        .filter(|number| number.fract() == 0.0)
                        .map(|number| number as i64)
                })
                .or_else(|| {
                    value
                        .as_str()
                        .map(str::trim)
                        .filter(|text| !text.is_empty())
                        .and_then(|text| text.parse::<i64>().ok())
                })
        })
        .map(normalize_debrid_concurrent_downloads)
        .unwrap_or(DEFAULT_DEBRID_CONCURRENT_DOWNLOADS)
}

pub fn normalize_debrid_concurrent_downloads(value: i64) -> i64 {
    value.clamp(
        MIN_DEBRID_CONCURRENT_DOWNLOADS,
        MAX_DEBRID_CONCURRENT_DOWNLOADS,
    )
}

pub fn validate_debrid_concurrent_downloads(value: i64) -> Result<i64> {
    if !(MIN_DEBRID_CONCURRENT_DOWNLOADS..=MAX_DEBRID_CONCURRENT_DOWNLOADS).contains(&value) {
        bail!(
            "Debrid concurrent downloads must be between {MIN_DEBRID_CONCURRENT_DOWNLOADS} and {MAX_DEBRID_CONCURRENT_DOWNLOADS}"
        );
    }
    Ok(value)
}

pub fn is_debrid_extension_id(value: &str) -> bool {
    value.trim().eq_ignore_ascii_case(DEBRID_EXTENSION_ID)
        || value
            .trim()
            .eq_ignore_ascii_case(LEGACY_REAL_DEBRID_EXTENSION_ID)
}

#[derive(Debug, Clone)]
pub struct DebridBrokerProgressItem {
    pub id: String,
    pub name: Option<String>,
    pub state: Option<String>,
    pub category: Option<String>,
    pub local_path: Option<String>,
    pub progress: Option<f64>,
    pub downloaded_bytes: Option<u64>,
    pub total_bytes: Option<u64>,
    pub remaining_bytes: Option<u64>,
    pub download_rate_bps: Option<u64>,
    pub debrid: Option<DebridBrokerProgressEvidence>,
}

#[derive(Debug, Clone)]
pub struct DebridBrokerProgressEvidence {
    pub provider_name: Option<String>,
    pub provider_implementation: Option<String>,
    pub provider_capabilities: Option<Value>,
    pub provider_status: Option<Value>,
    pub remote_status: Option<String>,
    pub selection_mode: Option<String>,
    pub selected_file_count: usize,
    pub skipped_file_count: usize,
    pub review_reasons: Vec<String>,
    pub failure_class: Option<String>,
    pub last_error: Option<String>,
    pub fallback_state: String,
}

#[derive(Debug, Clone)]
pub struct DebridJobStatus {
    pub job_id: Uuid,
    pub status: String,
    pub remote_status: Option<String>,
    pub source_kind: String,
    pub release_id: Option<Uuid>,
    pub failure_class: Option<String>,
    pub last_error: Option<String>,
    pub selection_error: Option<String>,
}

impl DebridJobStatus {
    pub fn is_failed(&self) -> bool {
        self.failure_class.is_some()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DebridFailureClass {
    ProviderAuthMissing,
    ProviderAccountRestricted,
    ProviderAccountLimitReached,
    ProviderUnavailable,
    ProviderUnsupported,
    RateLimited,
    TooManyActiveDownloads,
    QuotaExhausted,
    MagnetRejected,
    InvalidSource,
    ContentBlocked,
    NotFoundExpired,
    NoSeeds,
    ProviderStalled,
    StagingTimeout,
    FileListUnavailable,
    SelectionFailed,
    TransferFailed,
    UnrestrictFailed,
    MaterializerFailed,
    ProviderDeleteFailed,
    Unknown,
}

impl DebridFailureClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ProviderAuthMissing => "provider_auth_missing",
            Self::ProviderAccountRestricted => "provider_account_restricted",
            Self::ProviderAccountLimitReached => "provider_account_limit_reached",
            Self::ProviderUnavailable => "provider_unavailable",
            Self::ProviderUnsupported => "provider_unsupported",
            Self::RateLimited => "rate_limited",
            Self::TooManyActiveDownloads => "too_many_active_downloads",
            Self::QuotaExhausted => "quota_exhausted",
            Self::MagnetRejected => "magnet_rejected",
            Self::InvalidSource => "invalid_source",
            Self::ContentBlocked => "content_blocked",
            Self::NotFoundExpired => "not_found_or_expired",
            Self::NoSeeds => "no_seeds",
            Self::ProviderStalled => "provider_stalled",
            Self::StagingTimeout => "staging_timeout",
            Self::FileListUnavailable => "file_list_unavailable",
            Self::SelectionFailed => "selection_failed",
            Self::TransferFailed => "transfer_failed",
            Self::UnrestrictFailed => "unrestrict_failed",
            Self::MaterializerFailed => "materializer_failed",
            Self::ProviderDeleteFailed => "provider_delete_failed",
            Self::Unknown => "unknown",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "provider_auth_missing" | "unauthorized" | "auth_failed" => {
                Some(Self::ProviderAuthMissing)
            }
            "provider_account_restricted" | "account_restricted" | "permission_denied" => {
                Some(Self::ProviderAccountRestricted)
            }
            "provider_account_limit_reached" | "account_limit_reached" => {
                Some(Self::ProviderAccountLimitReached)
            }
            "provider_unavailable" => Some(Self::ProviderUnavailable),
            "provider_unsupported" => Some(Self::ProviderUnsupported),
            "rate_limited" | "rate_limit_reached" => Some(Self::RateLimited),
            "too_many_active_downloads" => Some(Self::TooManyActiveDownloads),
            "quota_exhausted" | "traffic_exhausted" | "fair_usage_limit" => {
                Some(Self::QuotaExhausted)
            }
            "magnet_rejected" => Some(Self::MagnetRejected),
            "invalid_source" | "invalid_magnet_or_torrent" => Some(Self::InvalidSource),
            "content_blocked" | "infringing_or_filtered" => Some(Self::ContentBlocked),
            "not_found_or_expired" | "not_found" | "expired" => Some(Self::NotFoundExpired),
            "no_seeds" => Some(Self::NoSeeds),
            "provider_stalled" => Some(Self::ProviderStalled),
            "staging_timeout" => Some(Self::StagingTimeout),
            "file_list_unavailable" => Some(Self::FileListUnavailable),
            "selection_failed" => Some(Self::SelectionFailed),
            "transfer_failed" => Some(Self::TransferFailed),
            "unrestrict_failed" => Some(Self::UnrestrictFailed),
            "materializer_failed" => Some(Self::MaterializerFailed),
            "provider_delete_failed" => Some(Self::ProviderDeleteFailed),
            "unknown" => Some(Self::Unknown),
            _ => None,
        }
    }

    pub fn response_policy(self) -> DebridFailureResponsePolicy {
        match self {
            Self::ProviderAuthMissing
            | Self::ProviderAccountRestricted
            | Self::ProviderAccountLimitReached
            | Self::QuotaExhausted => DebridFailureResponsePolicy::AccountActionRequired,
            Self::RateLimited | Self::TooManyActiveDownloads | Self::ProviderUnavailable => {
                DebridFailureResponsePolicy::RetryProviderLater
            }
            Self::NoSeeds
            | Self::ProviderStalled
            | Self::StagingTimeout
            | Self::FileListUnavailable
            | Self::MagnetRejected
            | Self::InvalidSource
            | Self::ContentBlocked
            | Self::NotFoundExpired
            | Self::SelectionFailed
            | Self::TransferFailed => DebridFailureResponsePolicy::TryAlternateRouteOrCandidate,
            Self::ProviderUnsupported => DebridFailureResponsePolicy::ProviderUnsupported,
            Self::UnrestrictFailed | Self::MaterializerFailed => {
                DebridFailureResponsePolicy::RetryOrReview
            }
            Self::ProviderDeleteFailed | Self::Unknown => DebridFailureResponsePolicy::Review,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DebridFailureResponsePolicy {
    TryAlternateRouteOrCandidate,
    RetryProviderLater,
    AccountActionRequired,
    ProviderUnsupported,
    RetryOrReview,
    Review,
}

impl DebridFailureResponsePolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TryAlternateRouteOrCandidate => "try_alternate_route_or_candidate",
            Self::RetryProviderLater => "retry_provider_later",
            Self::AccountActionRequired => "account_action_required",
            Self::ProviderUnsupported => "provider_unsupported",
            Self::RetryOrReview => "retry_or_review",
            Self::Review => "review",
        }
    }
}

#[derive(Debug, Clone)]
pub struct DebridSubmitOptions<'a> {
    pub owner_id: &'a str,
    pub category: Option<&'a str>,
    pub name: Option<&'a str>,
    pub paused: bool,
    pub release_context: Option<DebridReleaseSubmitContext>,
}

#[derive(Debug, Clone)]
pub struct DebridReleaseSubmitContext {
    pub subscription_id: Option<Uuid>,
    pub source_provider_id: Option<Uuid>,
    pub source_extension_id: String,
    pub media_type: MediaType,
    pub title: String,
    pub release_title: String,
    pub info_hash: Option<String>,
    pub fingerprint: Option<String>,
    pub score: Option<f64>,
    pub selected_candidate: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DebridFileSelectionMode {
    BeforeTransfer,
    AfterTransfer,
    Unsupported,
    ProviderSpecific(String),
}

impl DebridFileSelectionMode {
    pub fn as_persistence_value(&self) -> &str {
        match self {
            Self::BeforeTransfer => "before_transfer",
            Self::AfterTransfer => "after_transfer",
            Self::Unsupported => "unsupported",
            Self::ProviderSpecific(value) => value.as_str(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[allow(dead_code)]
pub enum DebridReleaseStatus {
    Staging,
    WaitingFiles,
    Selected,
    Transferring,
    Downloaded,
    Materializing,
    Completed,
    ReviewRequired,
    Failed,
    Cancelled,
}

#[allow(dead_code)]
impl DebridReleaseStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Staging => "staging",
            Self::WaitingFiles => "waiting_files",
            Self::Selected => "selected",
            Self::Transferring => "transferring",
            Self::Downloaded => "downloaded",
            Self::Materializing => "materializing",
            Self::Completed => "completed",
            Self::ReviewRequired => "review_required",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DebridProviderCapabilities {
    pub supports_magnet_submit: bool,
    pub supports_hoster_unrestrict: bool,
    pub supports_file_listing: bool,
    pub supports_file_selection: bool,
    pub supports_cache_check: bool,
    pub supports_delete: bool,
    pub supports_progress: bool,
    pub file_selection_mode: DebridFileSelectionMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct DebridAccount {
    pub provider_implementation: String,
    pub account_id: Option<String>,
    pub username: Option<String>,
    pub raw: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct DebridRemoteRelease {
    pub provider_implementation: String,
    pub remote_release_id: String,
    pub display_name: Option<String>,
    pub status: DebridReleaseStatus,
    pub raw_status: Option<String>,
    pub raw: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct DebridRemoteFile {
    pub provider_file_id: String,
    pub file_index: Option<i64>,
    pub path: String,
    pub basename: String,
    pub size_bytes: Option<u64>,
    pub selectable: bool,
    pub selected: Option<bool>,
    pub raw: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct DebridReleaseProgress {
    pub status: DebridReleaseStatus,
    pub progress: Option<f64>,
    pub downloaded_bytes: Option<u64>,
    pub total_bytes: Option<u64>,
    pub download_rate_bps: Option<u64>,
    pub raw: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct DebridResolvedLink {
    pub provider_file_id: Option<String>,
    pub url: String,
    pub filename: Option<String>,
    pub size_bytes: Option<u64>,
    pub raw: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct DebridFileSelection {
    pub mode: DebridFileSelectionMode,
    pub selected_file_ids: Vec<String>,
    pub skipped_file_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct DebridReleaseInspection {
    pub release: DebridRemoteRelease,
    pub capabilities: DebridProviderCapabilities,
    pub files: Vec<DebridRemoteFile>,
    pub links: Vec<DebridResolvedLink>,
    pub progress: Option<DebridReleaseProgress>,
    pub selection: Option<DebridFileSelection>,
    pub raw: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[allow(dead_code)]
pub enum DebridProviderErrorKind {
    Unauthorized,
    NotFound,
    RateLimited,
    SelectionUnsupported,
    Temporary,
    Permanent,
    Unknown,
}

#[allow(dead_code)]
impl DebridProviderErrorKind {
    pub fn failure_class(self) -> DebridFailureClass {
        match self {
            Self::Unauthorized => DebridFailureClass::ProviderAuthMissing,
            Self::NotFound => DebridFailureClass::NotFoundExpired,
            Self::RateLimited => DebridFailureClass::RateLimited,
            Self::Temporary => DebridFailureClass::ProviderUnavailable,
            Self::SelectionUnsupported => DebridFailureClass::ProviderUnsupported,
            Self::Permanent => DebridFailureClass::InvalidSource,
            Self::Unknown => DebridFailureClass::Unknown,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct DebridProviderError {
    pub kind: DebridProviderErrorKind,
    pub provider_code: Option<String>,
    pub message: String,
}

#[allow(dead_code)]
impl DebridProviderError {
    pub fn failure_class(&self) -> DebridFailureClass {
        self.kind.failure_class()
    }
}

#[async_trait]
#[allow(dead_code)]
pub trait DebridProviderAdapter: Send + Sync {
    fn implementation(&self) -> &str;

    fn capabilities(&self) -> DebridProviderCapabilities;

    async fn test_account(&self) -> Result<DebridAccount>;

    async fn submit_magnet(&self, magnet: &str) -> Result<DebridRemoteRelease>;

    async fn inspect_release(&self, remote_release_id: &str) -> Result<DebridReleaseInspection>;

    async fn select_files(
        &self,
        remote_release_id: &str,
        selected_file_ids: &[String],
    ) -> Result<DebridReleaseInspection>;

    async fn list_links(&self, remote_release_id: &str) -> Result<Vec<DebridResolvedLink>>;

    async fn unrestrict_hoster(&self, link: &str) -> Result<DebridResolvedLink>;

    async fn refresh_progress(&self, remote_release_id: &str) -> Result<DebridReleaseProgress>;

    async fn delete_release(&self, remote_release_id: &str) -> Result<bool>;
}

#[derive(Debug, Clone)]
struct DebridDownloadJob {
    job_id: Uuid,
    provider_id: Uuid,
    instance_id: Uuid,
    owner_id: String,
    source: String,
    source_kind: String,
    category: Option<String>,
    display_name: Option<String>,
    remote_torrent_id: Option<String>,
    remote_download_id: Option<String>,
    provider_implementation: Option<String>,
    remote_release_id: Option<String>,
    remote_release_status: Option<String>,
    provider_capabilities: Option<Value>,
    provider_status: Option<Value>,
    selection_mode: Option<String>,
    selected_file_ids: Vec<String>,
    skipped_file_ids: Vec<String>,
    selection_error: Option<String>,
    release_id: Option<Uuid>,
    status: String,
    local_path: Option<String>,
    links: Vec<String>,
    progress: Option<f64>,
    downloaded_bytes: Option<u64>,
    total_bytes: Option<u64>,
    download_rate_bps: Option<u64>,
    last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RealDebridUser {
    pub username: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RealDebridAddResponse {
    id: String,
    #[serde(default)]
    uri: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RealDebridTorrent {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    filename: Option<String>,
    #[serde(default)]
    bytes: Option<u64>,
    #[serde(default)]
    original_bytes: Option<u64>,
    #[serde(default)]
    progress: Option<f64>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    links: Vec<String>,
    #[serde(default)]
    files: Vec<RealDebridTorrentFile>,
    #[serde(default)]
    speed: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RealDebridTorrentFile {
    #[serde(default)]
    id: Value,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    bytes: Option<u64>,
    #[serde(default)]
    selected: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RealDebridUnrestrictedLink {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    filename: Option<String>,
    #[serde(default)]
    filesize: Option<u64>,
    #[serde(default)]
    download: Option<String>,
}

#[derive(Clone)]
pub struct RealDebridClient {
    http: Client,
    base_url: String,
    token: String,
}

impl RealDebridClient {
    pub fn new(token: impl Into<String>) -> Result<Self> {
        Self::with_base_url(token, REAL_DEBRID_API_BASE)
    }

    fn with_base_url(token: impl Into<String>, base_url: impl Into<String>) -> Result<Self> {
        let token = token.into();
        let provider_name = DebridServiceKind::RealDebrid.display_name();
        if token.trim().is_empty() {
            bail!("{provider_name} API token is required");
        }
        Ok(Self {
            http: Client::builder()
                .user_agent(REAL_DEBRID_USER_AGENT)
                .timeout(Duration::from_secs(30))
                .build()
                .with_context(|| format!("building {provider_name} HTTP client"))?,
            base_url: base_url.into().trim_end_matches('/').to_string(),
            token,
        })
    }

    pub async fn user(&self) -> Result<RealDebridUser> {
        self.request_json(Method::GET, "user", &[]).await
    }

    async fn add_magnet(&self, magnet: &str) -> Result<RealDebridAddResponse> {
        self.request_json(Method::POST, "torrents/addMagnet", &[("magnet", magnet)])
            .await
    }

    async fn select_files(&self, id: &str, files: &str) -> Result<()> {
        self.request_empty(
            Method::POST,
            &format!("torrents/selectFiles/{}", path_segment(id)),
            &[("files", files)],
        )
        .await
    }

    async fn torrent_info(&self, id: &str) -> Result<RealDebridTorrent> {
        self.request_json(
            Method::GET,
            &format!("torrents/info/{}", path_segment(id)),
            &[],
        )
        .await
    }

    async fn delete_torrent(&self, id: &str) -> Result<bool> {
        match self
            .request_empty_status(
                Method::DELETE,
                &format!("torrents/delete/{}", path_segment(id)),
                &[],
            )
            .await
        {
            Ok(status) => Ok(status.is_success() || status == StatusCode::NOT_FOUND),
            Err(err) if err.to_string().contains("404") => Ok(false),
            Err(err) => Err(err),
        }
    }

    async fn unrestrict_link(&self, link: &str) -> Result<RealDebridUnrestrictedLink> {
        self.request_json(Method::POST, "unrestrict/link", &[("link", link)])
            .await
    }

    async fn request_json<T: for<'de> Deserialize<'de>>(
        &self,
        method: Method,
        path: &str,
        form: &[(&str, &str)],
    ) -> Result<T> {
        let response = self.request(method, path, form).await?;
        let status = response.status();
        let provider_name = DebridServiceKind::RealDebrid.display_name();
        let body = response
            .text()
            .await
            .with_context(|| format!("reading {provider_name} response body"))?;
        if !status.is_success() {
            bail!(
                "{provider_name} API returned {status}: {}",
                redacted_body(&body)
            );
        }
        serde_json::from_str(&body).with_context(|| format!("parsing {provider_name} response"))
    }

    async fn request_empty(&self, method: Method, path: &str, form: &[(&str, &str)]) -> Result<()> {
        let status = self.request_empty_status(method, path, form).await?;
        if status.is_success() || status == StatusCode::ACCEPTED {
            Ok(())
        } else {
            bail!(
                "{} API returned {status}",
                DebridServiceKind::RealDebrid.display_name()
            )
        }
    }

    async fn request_empty_status(
        &self,
        method: Method,
        path: &str,
        form: &[(&str, &str)],
    ) -> Result<StatusCode> {
        let response = self.request(method, path, form).await?;
        let status = response.status();
        if !status.is_success() && status != StatusCode::ACCEPTED {
            let body = response.text().await.unwrap_or_default();
            bail!(
                "{} API returned {status}: {}",
                DebridServiceKind::RealDebrid.display_name(),
                redacted_body(&body)
            );
        }
        Ok(status)
    }

    async fn request(
        &self,
        method: Method,
        path: &str,
        form: &[(&str, &str)],
    ) -> Result<reqwest::Response> {
        let url = format!("{}/{}", self.base_url, path.trim_start_matches('/'));
        let mut request = self
            .http
            .request(method, url)
            .bearer_auth(self.token.trim());
        if !form.is_empty() {
            request = request.form(form);
        }
        request.send().await.with_context(|| {
            format!(
                "calling {} API",
                DebridServiceKind::RealDebrid.display_name()
            )
        })
    }
}

#[async_trait]
impl DebridProviderAdapter for RealDebridClient {
    fn implementation(&self) -> &str {
        REAL_DEBRID_IMPLEMENTATION
    }

    fn capabilities(&self) -> DebridProviderCapabilities {
        real_debrid_capabilities()
    }

    async fn test_account(&self) -> Result<DebridAccount> {
        let user = self.user().await?;
        Ok(DebridAccount {
            provider_implementation: self.implementation().to_string(),
            account_id: None,
            username: Some(user.username.clone()),
            raw: Some(serde_json::to_value(user)?),
        })
    }

    async fn submit_magnet(&self, magnet: &str) -> Result<DebridRemoteRelease> {
        let added = self.add_magnet(magnet).await?;
        Ok(DebridRemoteRelease {
            provider_implementation: self.implementation().to_string(),
            remote_release_id: added.id.clone(),
            display_name: None,
            status: DebridReleaseStatus::Staging,
            raw_status: Some("add_magnet_submitted".to_string()),
            raw: Some(serde_json::to_value(added)?),
        })
    }

    async fn inspect_release(&self, remote_release_id: &str) -> Result<DebridReleaseInspection> {
        let torrent = self.torrent_info(remote_release_id).await?;
        real_debrid_torrent_to_inspection(remote_release_id, torrent)
    }

    async fn select_files(
        &self,
        remote_release_id: &str,
        selected_file_ids: &[String],
    ) -> Result<DebridReleaseInspection> {
        if selected_file_ids.is_empty() {
            bail!(
                "{} file selection requires at least one file id",
                DebridServiceKind::RealDebrid.display_name()
            );
        }
        let files = selected_file_ids.join(",");
        RealDebridClient::select_files(self, remote_release_id, &files).await?;
        match self.inspect_release(remote_release_id).await {
            Ok(inspection) => Ok(inspection),
            Err(_) => Ok(DebridReleaseInspection {
                release: DebridRemoteRelease {
                    provider_implementation: self.implementation().to_string(),
                    remote_release_id: remote_release_id.to_string(),
                    display_name: None,
                    status: DebridReleaseStatus::Selected,
                    raw_status: Some("selected".to_string()),
                    raw: None,
                },
                capabilities: self.capabilities(),
                files: Vec::new(),
                links: Vec::new(),
                progress: Some(DebridReleaseProgress {
                    status: DebridReleaseStatus::Selected,
                    progress: None,
                    downloaded_bytes: None,
                    total_bytes: None,
                    download_rate_bps: None,
                    raw: None,
                }),
                selection: Some(DebridFileSelection {
                    mode: DebridFileSelectionMode::BeforeTransfer,
                    selected_file_ids: selected_file_ids.to_vec(),
                    skipped_file_ids: Vec::new(),
                }),
                raw: None,
            }),
        }
    }

    async fn list_links(&self, remote_release_id: &str) -> Result<Vec<DebridResolvedLink>> {
        Ok(self.inspect_release(remote_release_id).await?.links)
    }

    async fn unrestrict_hoster(&self, link: &str) -> Result<DebridResolvedLink> {
        let unrestricted = self.unrestrict_link(link).await?;
        let Some(download_url) = unrestricted.download.as_deref() else {
            bail!(
                "{} did not return a downloadable link",
                DebridServiceKind::RealDebrid.display_name()
            );
        };
        Ok(DebridResolvedLink {
            provider_file_id: unrestricted.id.clone(),
            url: download_url.to_string(),
            filename: unrestricted.filename.clone(),
            size_bytes: unrestricted.filesize,
            raw: Some(serde_json::to_value(unrestricted)?),
        })
    }

    async fn refresh_progress(&self, remote_release_id: &str) -> Result<DebridReleaseProgress> {
        let torrent = self.torrent_info(remote_release_id).await?;
        Ok(real_debrid_torrent_progress(&torrent))
    }

    async fn delete_release(&self, remote_release_id: &str) -> Result<bool> {
        self.delete_torrent(remote_release_id).await
    }
}

#[derive(Clone)]
pub struct TorBoxClient {
    http: Client,
    base_url: String,
    token: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct TorBoxEnvelope {
    #[serde(default)]
    success: Option<bool>,
    #[serde(default)]
    detail: Option<Value>,
    #[serde(default)]
    error: Option<Value>,
    #[serde(default)]
    data: Option<Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct TorBoxCreateTorrentResponse {
    #[serde(default)]
    hash: Option<String>,
    #[serde(default)]
    torrent_id: Value,
    #[serde(default)]
    auth_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct TorBoxTorrent {
    #[serde(default)]
    id: Value,
    #[serde(default)]
    auth_id: Option<String>,
    #[serde(default)]
    hash: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    size: Option<u64>,
    #[serde(default)]
    download_state: Option<String>,
    #[serde(default)]
    progress: Option<f64>,
    #[serde(default)]
    download_speed: Option<u64>,
    #[serde(default)]
    total_downloaded: Option<u64>,
    #[serde(default)]
    download_finished: Option<bool>,
    #[serde(default)]
    download_present: Option<bool>,
    #[serde(default)]
    cached: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_torbox_torrent_files")]
    files: Vec<TorBoxTorrentFile>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct TorBoxTorrentFile {
    #[serde(default)]
    id: Value,
    #[serde(default)]
    hash: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    short_name: Option<String>,
    #[serde(default)]
    absolute_path: Option<String>,
    #[serde(default)]
    size: Option<u64>,
    #[serde(default)]
    zipped: Option<bool>,
    #[serde(default)]
    infected: Option<bool>,
    #[serde(default)]
    mimetype: Option<String>,
}

fn deserialize_torbox_torrent_files<'de, D>(
    deserializer: D,
) -> std::result::Result<Vec<TorBoxTorrentFile>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    match value {
        Value::Array(items) => items
            .into_iter()
            .map(serde_json::from_value)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(serde::de::Error::custom),
        Value::Null => Ok(Vec::new()),
        other => Err(serde::de::Error::custom(format!(
            "expected TorBox files array or null, got {other}"
        ))),
    }
}

#[derive(Debug, Clone)]
struct TorBoxCreateTorrentRateLimiter {
    minute_started_at: Instant,
    minute_count: usize,
    hour_started_at: Instant,
    hour_count: usize,
    backoff_until: Option<Instant>,
}

impl TorBoxCreateTorrentRateLimiter {
    fn new(now: Instant) -> Self {
        Self {
            minute_started_at: now,
            minute_count: 0,
            hour_started_at: now,
            hour_count: 0,
            backoff_until: None,
        }
    }

    fn try_acquire(&mut self, now: Instant) -> Result<()> {
        if let Some(backoff_until) = self.backoff_until {
            if now < backoff_until {
                return Err(torbox_rate_limit_error(backoff_until.duration_since(now)));
            }
            self.backoff_until = None;
        }

        if now.duration_since(self.minute_started_at) >= TORBOX_CREATE_TORRENT_MINUTE_WINDOW {
            self.minute_started_at = now;
            self.minute_count = 0;
        }
        if now.duration_since(self.hour_started_at) >= TORBOX_CREATE_TORRENT_HOUR_WINDOW {
            self.hour_started_at = now;
            self.hour_count = 0;
        }

        if self.minute_count >= TORBOX_CREATE_TORRENT_MINUTE_LIMIT {
            let retry_after =
                TORBOX_CREATE_TORRENT_MINUTE_WINDOW - now.duration_since(self.minute_started_at);
            self.backoff_until = Some(now + retry_after);
            return Err(torbox_rate_limit_error(retry_after));
        }
        if self.hour_count >= TORBOX_CREATE_TORRENT_HOUR_LIMIT {
            let retry_after =
                TORBOX_CREATE_TORRENT_HOUR_WINDOW - now.duration_since(self.hour_started_at);
            self.backoff_until = Some(now + retry_after);
            return Err(torbox_rate_limit_error(retry_after));
        }

        self.minute_count += 1;
        self.hour_count += 1;
        Ok(())
    }
}

impl TorBoxClient {
    pub fn new(token: impl Into<String>) -> Result<Self> {
        Self::with_base_url(token, TORBOX_API_BASE)
    }

    fn with_base_url(token: impl Into<String>, base_url: impl Into<String>) -> Result<Self> {
        let token = token.into();
        let provider_name = DebridServiceKind::TorBox.display_name();
        if token.trim().is_empty() {
            bail!("{provider_name} API token is required");
        }
        Ok(Self {
            http: Client::builder()
                .user_agent(TORBOX_USER_AGENT)
                .timeout(Duration::from_secs(30))
                .build()
                .with_context(|| format!("building {provider_name} HTTP client"))?,
            base_url: base_url.into().trim_end_matches('/').to_string(),
            token,
        })
    }

    async fn user(&self) -> Result<Value> {
        self.request_value(Method::GET, "user/me", &[("settings", "false")], &[])
            .await
    }

    fn check_create_torrent_rate_limit(&self) -> Result<()> {
        let key = torbox_create_torrent_limiter_key(&self.base_url, &self.token);
        let mut limiters = TORBOX_CREATE_TORRENT_LIMITERS
            .lock()
            .map_err(|_| anyhow!("TorBox create torrent rate limiter lock is poisoned"))?;
        let now = Instant::now();
        limiters
            .entry(key)
            .or_insert_with(|| TorBoxCreateTorrentRateLimiter::new(now))
            .try_acquire(now)
    }

    async fn create_torrent(&self, magnet: &str) -> Result<TorBoxCreateTorrentResponse> {
        self.check_create_torrent_rate_limit()?;
        let form = reqwest::multipart::Form::new()
            .text("magnet", magnet.to_string())
            .text("allow_zip", "false".to_string());
        let value = self
            .request_value_multipart(Method::POST, "torrents/createtorrent", &[], form)
            .await?;
        serde_json::from_value(value)
            .with_context(|| format!("parsing {} create torrent response", self.provider_name()))
    }

    async fn check_cached_hash(&self, hash: &str) -> Result<Option<Value>> {
        let hash = hash.trim();
        if hash.is_empty() {
            return Ok(None);
        }
        let value = self
            .request_value(
                Method::GET,
                "torrents/checkcached",
                &[("hash", hash), ("format", "object"), ("list_files", "true")],
                &[],
            )
            .await?;
        Ok(torbox_cache_entry_for_hash(&value, hash))
    }

    async fn torrent_by_id(&self, remote_release_id: &str) -> Result<TorBoxTorrent> {
        let torrent_id = remote_release_id.trim();
        if torrent_id.is_empty() {
            bail!("{} torrent id is required", self.provider_name());
        }
        let value = self
            .request_value(
                Method::GET,
                "torrents/mylist",
                &[("id", torrent_id), ("bypass_cache", "true")],
                &[],
            )
            .await?;
        torbox_torrent_from_mylist_value(&value, torrent_id)
    }

    async fn request_download_link(
        &self,
        torrent_id: &str,
        file: Option<&DebridRemoteFile>,
    ) -> Result<DebridResolvedLink> {
        let _provider_url = self
            .request_provider_download_url(
                torrent_id,
                file.map(|file| file.provider_file_id.as_str()),
            )
            .await?;
        let stored_url = torbox_internal_download_url(torrent_id, file)?;
        let file_id = file.map(|file| file.provider_file_id.clone());
        Ok(DebridResolvedLink {
            provider_file_id: file_id.clone(),
            url: stored_url.clone(),
            filename: file.map(|file| file.basename.clone()),
            size_bytes: file.and_then(|file| file.size_bytes),
            raw: Some(json!({
                "torrentId": torrent_id,
                "fileId": file_id,
                "storedUrl": stored_url,
                "providerUrlRedacted": true
            })),
        })
    }

    async fn request_provider_download_url(
        &self,
        torrent_id: &str,
        file_id: Option<&str>,
    ) -> Result<String> {
        let mut query = vec![
            ("token", self.token.trim()),
            ("torrent_id", torrent_id.trim()),
            ("redirect", "false"),
            ("append_name", "true"),
        ];
        if let Some(file_id) = file_id {
            query.push(("file_id", file_id));
        } else {
            query.push(("zip_link", "true"));
        }
        let value = self
            .request_value(Method::GET, "torrents/requestdl", &query, &[])
            .await?;
        let url = value
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("{} did not return a download URL", self.provider_name()))?;
        Ok(url.to_string())
    }

    async fn request_download_links(
        &self,
        torrent_id: &str,
        files: &[DebridRemoteFile],
        selected_file_ids: &[String],
    ) -> Result<Vec<DebridResolvedLink>> {
        let selected = selected_file_ids.iter().cloned().collect::<HashSet<_>>();
        let mut links = Vec::new();
        for file in files {
            if selected.contains(&file.provider_file_id) {
                links.push(self.request_download_link(torrent_id, Some(file)).await?);
            }
        }
        Ok(links)
    }

    async fn control_torrent(&self, torrent_id: &str, operation: &str) -> Result<()> {
        let torrent_id = torbox_torrent_id_json_value(torrent_id)?;
        let body = json!({
            "torrent_id": torrent_id,
            "operation": operation,
            "all": false
        });
        let _ = self
            .request_value_json(Method::POST, "torrents/controltorrent", &[], &body)
            .await?;
        Ok(())
    }

    fn provider_name(&self) -> &'static str {
        DebridServiceKind::TorBox.display_name()
    }

    async fn request_value(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
        form: &[(&str, &str)],
    ) -> Result<Value> {
        let response = self.request(method, path, query, form).await?;
        let status = response.status();
        let body = response.text().await.with_context(|| {
            format!(
                "reading {} response body",
                DebridServiceKind::TorBox.display_name()
            )
        })?;
        torbox_response_value(status, &body, &self.token)
    }

    async fn request_value_json(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
        body: &Value,
    ) -> Result<Value> {
        let response = self.request_json(method, path, query, body).await?;
        let status = response.status();
        let body = response.text().await.with_context(|| {
            format!(
                "reading {} response body",
                DebridServiceKind::TorBox.display_name()
            )
        })?;
        torbox_response_value(status, &body, &self.token)
    }

    async fn request_value_multipart(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
        form: reqwest::multipart::Form,
    ) -> Result<Value> {
        let response = self.request_multipart(method, path, query, form).await?;
        let status = response.status();
        let body = response.text().await.with_context(|| {
            format!(
                "reading {} response body",
                DebridServiceKind::TorBox.display_name()
            )
        })?;
        torbox_response_value(status, &body, &self.token)
    }

    async fn request(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
        form: &[(&str, &str)],
    ) -> Result<reqwest::Response> {
        let mut url = Url::parse(&format!(
            "{}/{}",
            self.base_url,
            path.trim_start_matches('/')
        ))
        .with_context(|| format!("building TorBox API URL for {path}"))?;
        if !query.is_empty() {
            url.query_pairs_mut().extend_pairs(query.iter().copied());
        }
        let mut request = self
            .http
            .request(method, url)
            .bearer_auth(self.token.trim());
        if !form.is_empty() {
            request = request.form(form);
        }
        request
            .send()
            .await
            .with_context(|| format!("calling {} API", DebridServiceKind::TorBox.display_name()))
    }

    async fn request_json(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
        body: &Value,
    ) -> Result<reqwest::Response> {
        let mut url = Url::parse(&format!(
            "{}/{}",
            self.base_url,
            path.trim_start_matches('/')
        ))
        .with_context(|| format!("building TorBox API URL for {path}"))?;
        if !query.is_empty() {
            url.query_pairs_mut().extend_pairs(query.iter().copied());
        }
        self.http
            .request(method, url)
            .bearer_auth(self.token.trim())
            .json(body)
            .send()
            .await
            .with_context(|| format!("calling {} API", DebridServiceKind::TorBox.display_name()))
    }

    async fn request_multipart(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
        form: reqwest::multipart::Form,
    ) -> Result<reqwest::Response> {
        let mut url = Url::parse(&format!(
            "{}/{}",
            self.base_url,
            path.trim_start_matches('/')
        ))
        .with_context(|| format!("building TorBox API URL for {path}"))?;
        if !query.is_empty() {
            url.query_pairs_mut().extend_pairs(query.iter().copied());
        }
        self.http
            .request(method, url)
            .bearer_auth(self.token.trim())
            .multipart(form)
            .send()
            .await
            .with_context(|| format!("calling {} API", DebridServiceKind::TorBox.display_name()))
    }
}

#[async_trait]
impl DebridProviderAdapter for TorBoxClient {
    fn implementation(&self) -> &str {
        DebridServiceKind::TorBox.implementation_id()
    }

    fn capabilities(&self) -> DebridProviderCapabilities {
        torbox_lifecycle_capabilities()
    }

    async fn test_account(&self) -> Result<DebridAccount> {
        let user = self.user().await?;
        Ok(DebridAccount {
            provider_implementation: self.implementation().to_string(),
            account_id: torbox_user_string(&user, "id"),
            username: torbox_user_string(&user, "username")
                .or_else(|| torbox_user_string(&user, "email")),
            raw: Some(user),
        })
    }

    async fn submit_magnet(&self, magnet: &str) -> Result<DebridRemoteRelease> {
        let cache = match extract_magnet_info_hash(magnet) {
            Some(hash) => self.check_cached_hash(&hash).await?,
            None => None,
        };
        let created = self.create_torrent(magnet).await?;
        let remote_release_id = torbox_id_string(&created.torrent_id).ok_or_else(|| {
            anyhow!(
                "{} did not return a torrent id",
                DebridServiceKind::TorBox.display_name()
            )
        })?;
        let raw_status = if cache.is_some() {
            "submitted_cached"
        } else {
            "submitted_uncached_or_unknown"
        };
        Ok(DebridRemoteRelease {
            provider_implementation: self.implementation().to_string(),
            remote_release_id,
            display_name: created.hash.clone(),
            status: DebridReleaseStatus::Staging,
            raw_status: Some(raw_status.to_string()),
            raw: Some(json!({
                "create": created,
                "cache": cache
            })),
        })
    }

    async fn inspect_release(&self, remote_release_id: &str) -> Result<DebridReleaseInspection> {
        let torrent = self.torrent_by_id(remote_release_id).await?;
        torbox_torrent_to_inspection(&torrent, Vec::new(), None)
    }

    async fn select_files(
        &self,
        remote_release_id: &str,
        selected_file_ids: &[String],
    ) -> Result<DebridReleaseInspection> {
        if selected_file_ids.is_empty() {
            bail!(
                "{} file selection requires at least one file id",
                DebridServiceKind::TorBox.display_name()
            );
        }
        let torrent = self.torrent_by_id(remote_release_id).await?;
        let files = torbox_torrent_files(&torrent)?;
        let selected = selected_file_ids.iter().cloned().collect::<HashSet<_>>();
        let unknown = selected
            .iter()
            .filter(|file_id| !files.iter().any(|file| file.provider_file_id == **file_id))
            .cloned()
            .collect::<Vec<_>>();
        if !unknown.is_empty() {
            bail!(
                "{} file selection referenced unknown file ids: {}",
                DebridServiceKind::TorBox.display_name(),
                unknown.join(",")
            );
        }
        let links = if torbox_torrent_status(&torrent) == DebridReleaseStatus::Downloaded {
            self.request_download_links(remote_release_id, &files, selected_file_ids)
                .await?
        } else {
            Vec::new()
        };
        torbox_torrent_to_inspection(&torrent, links, Some(selected_file_ids))
    }

    async fn list_links(&self, remote_release_id: &str) -> Result<Vec<DebridResolvedLink>> {
        let torrent = self.torrent_by_id(remote_release_id).await?;
        if torbox_torrent_status(&torrent) != DebridReleaseStatus::Downloaded {
            return Ok(Vec::new());
        }
        let files = torbox_torrent_files(&torrent)?;
        if files.is_empty() {
            return Ok(vec![
                self.request_download_link(remote_release_id, None).await?,
            ]);
        }
        let selected_file_ids = files
            .iter()
            .map(|file| file.provider_file_id.clone())
            .collect::<Vec<_>>();
        self.request_download_links(remote_release_id, &files, &selected_file_ids)
            .await
    }

    async fn unrestrict_hoster(&self, link: &str) -> Result<DebridResolvedLink> {
        let link = link.trim();
        if link.is_empty() {
            bail!(
                "{} download link is empty",
                DebridServiceKind::TorBox.display_name()
            );
        }
        if let Some(reference) = torbox_internal_download_ref(link)? {
            let url = self
                .request_provider_download_url(&reference.torrent_id, reference.file_id.as_deref())
                .await?;
            return Ok(DebridResolvedLink {
                provider_file_id: reference.file_id,
                url,
                filename: reference.filename,
                size_bytes: reference.size_bytes,
                raw: Some(json!({
                    "torrentId": reference.torrent_id,
                    "storedUrl": link,
                    "providerUrlRedacted": true
                })),
            });
        }
        Ok(DebridResolvedLink {
            provider_file_id: None,
            url: link.to_string(),
            filename: filename_from_url_path(link),
            size_bytes: None,
            raw: Some(json!({ "direct": true })),
        })
    }

    async fn refresh_progress(&self, remote_release_id: &str) -> Result<DebridReleaseProgress> {
        let torrent = self.torrent_by_id(remote_release_id).await?;
        Ok(torbox_torrent_progress(&torrent))
    }

    async fn delete_release(&self, remote_release_id: &str) -> Result<bool> {
        match self.control_torrent(remote_release_id, "delete").await {
            Ok(()) => Ok(true),
            Err(err) if torbox_error_is_not_found(&err) => Ok(false),
            Err(err) => Err(err),
        }
    }
}

pub struct AllDebridClient {
    http: Client,
    base_url: String,
    token: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct AllDebridEnvelope {
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    data: Option<Value>,
    #[serde(default)]
    error: Option<Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct AllDebridUploadResponse {
    #[serde(default)]
    magnets: Vec<AllDebridUploadedMagnet>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct AllDebridUploadedMagnet {
    #[serde(default)]
    magnet: Option<String>,
    #[serde(default)]
    hash: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    size: Option<u64>,
    #[serde(default)]
    ready: Option<bool>,
    #[serde(default)]
    id: Value,
    #[serde(default)]
    error: Option<Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct AllDebridStatusResponse {
    #[serde(default, deserialize_with = "deserialize_all_debrid_status_magnets")]
    magnets: Vec<AllDebridMagnetStatus>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct AllDebridMagnetStatus {
    #[serde(default)]
    id: Value,
    #[serde(default)]
    filename: Option<String>,
    #[serde(default)]
    size: Option<u64>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default, rename = "statusCode")]
    status_code: Option<i64>,
    #[serde(default)]
    downloaded: Option<u64>,
    #[serde(default, rename = "downloadSpeed")]
    download_speed: Option<u64>,
    #[serde(default)]
    seeders: Option<u64>,
    #[serde(default)]
    files: Vec<Value>,
}

fn deserialize_all_debrid_status_magnets<'de, D>(
    deserializer: D,
) -> std::result::Result<Vec<AllDebridMagnetStatus>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    match value {
        Value::Array(items) => items
            .into_iter()
            .map(serde_json::from_value)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(serde::de::Error::custom),
        Value::Object(_) => serde_json::from_value(value)
            .map(|item| vec![item])
            .map_err(serde::de::Error::custom),
        Value::Null => Ok(Vec::new()),
        other => Err(serde::de::Error::custom(format!(
            "expected AllDebrid magnets array or object, got {other}"
        ))),
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct AllDebridFilesResponse {
    #[serde(default)]
    magnets: Vec<AllDebridFilesMagnet>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct AllDebridFilesMagnet {
    #[serde(default)]
    id: Value,
    #[serde(default)]
    files: Vec<Value>,
    #[serde(default)]
    error: Option<Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct AllDebridUnlockedLink {
    #[serde(default)]
    link: Option<String>,
    #[serde(default)]
    filename: Option<String>,
    #[serde(default)]
    filesize: Option<u64>,
    #[serde(default)]
    delayed: Option<Value>,
    #[serde(default)]
    id: Option<Value>,
    #[serde(default)]
    host: Option<String>,
    #[serde(default, rename = "hostDomain")]
    host_domain: Option<String>,
}

impl AllDebridClient {
    pub fn new(token: impl Into<String>) -> Result<Self> {
        Self::with_base_url(token, ALL_DEBRID_API_BASE)
    }

    fn with_base_url(token: impl Into<String>, base_url: impl Into<String>) -> Result<Self> {
        let token = token.into();
        let provider_name = DebridServiceKind::AllDebrid.display_name();
        if token.trim().is_empty() {
            bail!("{provider_name} API token is required");
        }
        Ok(Self {
            http: Client::builder()
                .user_agent(ALL_DEBRID_USER_AGENT)
                .timeout(Duration::from_secs(30))
                .build()
                .with_context(|| format!("building {provider_name} HTTP client"))?,
            base_url: base_url.into().trim_end_matches('/').to_string(),
            token,
        })
    }

    async fn user(&self) -> Result<Value> {
        let data = self.request_value(Method::GET, "user", &[], &[]).await?;
        Ok(data.get("user").cloned().unwrap_or(data))
    }

    async fn upload_magnet(&self, magnet: &str) -> Result<AllDebridUploadedMagnet> {
        let value = self
            .request_value(Method::POST, "magnet/upload", &[], &[("magnets[]", magnet)])
            .await?;
        let response: AllDebridUploadResponse = serde_json::from_value(value)
            .with_context(|| format!("parsing {} magnet upload response", self.provider_name()))?;
        let uploaded = response.magnets.into_iter().next().ok_or_else(|| {
            anyhow!(
                "{} did not return a magnet upload result",
                self.provider_name()
            )
        })?;
        if let Some(error) = uploaded.error.as_ref() {
            return Err(all_debrid_error_value_to_anyhow(
                error,
                StatusCode::BAD_REQUEST,
                &self.token,
            ));
        }
        Ok(uploaded)
    }

    async fn magnet_status(&self, remote_release_id: &str) -> Result<AllDebridMagnetStatus> {
        let remote_release_id = remote_release_id.trim();
        if remote_release_id.is_empty() {
            bail!("{} magnet id is required", self.provider_name());
        }
        let value = self
            .request_value(
                Method::POST,
                "v4.1/magnet/status",
                &[],
                &[("id", remote_release_id)],
            )
            .await?;
        let response: AllDebridStatusResponse = serde_json::from_value(value)
            .with_context(|| format!("parsing {} magnet status response", self.provider_name()))?;
        all_debrid_status_for_id(response.magnets, remote_release_id)
    }

    async fn magnet_files(&self, remote_release_id: &str) -> Result<Vec<DebridRemoteFile>> {
        let remote_release_id = remote_release_id.trim();
        if remote_release_id.is_empty() {
            bail!("{} magnet id is required", self.provider_name());
        }
        let value = self
            .request_value(
                Method::POST,
                "magnet/files",
                &[],
                &[("id[]", remote_release_id)],
            )
            .await?;
        let response: AllDebridFilesResponse = serde_json::from_value(value)
            .with_context(|| format!("parsing {} magnet files response", self.provider_name()))?;
        let magnet = all_debrid_files_for_id(response.magnets, remote_release_id)?;
        all_debrid_flatten_file_nodes(&magnet.files)
    }

    async fn unlock_link(
        &self,
        link: &str,
        provider_file_id: Option<&str>,
        fallback_filename: Option<&str>,
        fallback_size: Option<u64>,
    ) -> Result<DebridResolvedLink> {
        let link = link.trim();
        if link.is_empty() {
            bail!(
                "{} link unlock requires a non-empty link",
                self.provider_name()
            );
        }
        let value = self
            .request_value(Method::POST, "link/unlock", &[], &[("link", link)])
            .await?;
        let unlocked: AllDebridUnlockedLink = serde_json::from_value(value)
            .with_context(|| format!("parsing {} link unlock response", self.provider_name()))?;
        all_debrid_unlocked_link_to_resolved(
            unlocked,
            provider_file_id,
            fallback_filename,
            fallback_size,
        )
    }

    async fn delete_magnet(&self, remote_release_id: &str) -> Result<()> {
        let remote_release_id = remote_release_id.trim();
        if remote_release_id.is_empty() {
            bail!("{} magnet id is required", self.provider_name());
        }
        let _ = self
            .request_value(
                Method::POST,
                "magnet/delete",
                &[],
                &[("id", remote_release_id)],
            )
            .await?;
        Ok(())
    }

    fn provider_name(&self) -> &'static str {
        DebridServiceKind::AllDebrid.display_name()
    }

    async fn request_value(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
        form: &[(&str, &str)],
    ) -> Result<Value> {
        let response = self.request(method, path, query, form).await?;
        let status = response.status();
        let body = response.text().await.with_context(|| {
            format!(
                "reading {} response body",
                DebridServiceKind::AllDebrid.display_name()
            )
        })?;
        all_debrid_response_value(status, &body, &self.token)
    }

    async fn request(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
        form: &[(&str, &str)],
    ) -> Result<reqwest::Response> {
        let mut url = Url::parse(&format!(
            "{}/{}",
            self.base_url,
            path.trim_start_matches('/')
        ))
        .with_context(|| format!("building AllDebrid API URL for {path}"))?;
        if path.trim_start_matches('/').starts_with("v4.1/") {
            url = Url::parse(&self.base_url)
                .with_context(|| format!("building AllDebrid API base URL for {path}"))?;
            url.set_path(&format!("/{}", path.trim_start_matches('/')));
        }
        if !query.is_empty() {
            url.query_pairs_mut().extend_pairs(query.iter().copied());
        }
        let mut request = self
            .http
            .request(method, url)
            .bearer_auth(self.token.trim());
        if !form.is_empty() {
            request = request.form(form);
        }
        request.send().await.with_context(|| {
            format!(
                "calling {} API",
                DebridServiceKind::AllDebrid.display_name()
            )
        })
    }
}

#[async_trait]
impl DebridProviderAdapter for AllDebridClient {
    fn implementation(&self) -> &str {
        DebridServiceKind::AllDebrid.implementation_id()
    }

    fn capabilities(&self) -> DebridProviderCapabilities {
        all_debrid_lifecycle_capabilities()
    }

    async fn test_account(&self) -> Result<DebridAccount> {
        let user = self.user().await?;
        Ok(DebridAccount {
            provider_implementation: self.implementation().to_string(),
            account_id: all_debrid_user_string(&user, &["id", "userId", "uid"]),
            username: all_debrid_user_string(&user, &["username", "email", "name"]),
            raw: Some(user),
        })
    }

    async fn submit_magnet(&self, magnet: &str) -> Result<DebridRemoteRelease> {
        let uploaded = self.upload_magnet(magnet).await?;
        all_debrid_uploaded_magnet_to_release(&uploaded)
    }

    async fn inspect_release(&self, remote_release_id: &str) -> Result<DebridReleaseInspection> {
        let status = self.magnet_status(remote_release_id).await?;
        let files =
            if all_debrid_status_to_release_status(&status) == DebridReleaseStatus::Downloaded {
                self.magnet_files(remote_release_id).await?
            } else {
                all_debrid_flatten_file_nodes(&status.files)?
            };
        all_debrid_status_to_inspection(status, files, Vec::new(), None)
    }

    async fn select_files(
        &self,
        remote_release_id: &str,
        selected_file_ids: &[String],
    ) -> Result<DebridReleaseInspection> {
        if selected_file_ids.is_empty() {
            bail!(
                "{} file selection requires at least one file id",
                self.provider_name()
            );
        }
        let status = self.magnet_status(remote_release_id).await?;
        let files =
            if all_debrid_status_to_release_status(&status) == DebridReleaseStatus::Downloaded {
                self.magnet_files(remote_release_id).await?
            } else {
                all_debrid_flatten_file_nodes(&status.files)?
            };
        let selected = selected_file_ids.iter().cloned().collect::<HashSet<_>>();
        let unknown = selected
            .iter()
            .filter(|file_id| !files.iter().any(|file| file.provider_file_id == **file_id))
            .cloned()
            .collect::<Vec<_>>();
        if !unknown.is_empty() {
            bail!(
                "{} file selection referenced unknown file ids: {}",
                self.provider_name(),
                unknown.join(",")
            );
        }

        let mut links = Vec::new();
        if all_debrid_status_to_release_status(&status) == DebridReleaseStatus::Downloaded {
            for file in &files {
                if selected.contains(&file.provider_file_id) {
                    if let Some(link) = all_debrid_file_link(file) {
                        links.push(
                            self.unlock_link(
                                &link,
                                Some(&file.provider_file_id),
                                Some(&file.basename),
                                file.size_bytes,
                            )
                            .await?,
                        );
                    }
                }
            }
        }
        all_debrid_status_to_inspection(status, files, links, Some(selected_file_ids))
    }

    async fn list_links(&self, remote_release_id: &str) -> Result<Vec<DebridResolvedLink>> {
        let status = self.magnet_status(remote_release_id).await?;
        if all_debrid_status_to_release_status(&status) != DebridReleaseStatus::Downloaded {
            return Ok(Vec::new());
        }
        let files = self.magnet_files(remote_release_id).await?;
        let mut links = Vec::new();
        for file in &files {
            if !file.selectable {
                continue;
            }
            if let Some(link) = all_debrid_file_link(file) {
                links.push(
                    self.unlock_link(
                        &link,
                        Some(&file.provider_file_id),
                        Some(&file.basename),
                        file.size_bytes,
                    )
                    .await?,
                );
            }
        }
        Ok(links)
    }

    async fn unrestrict_hoster(&self, link: &str) -> Result<DebridResolvedLink> {
        if all_debrid_is_unlocked_download_url(link, &self.base_url) {
            let link = link.trim();
            return Ok(DebridResolvedLink {
                provider_file_id: None,
                url: link.to_string(),
                filename: filename_from_url_path(link),
                size_bytes: None,
                raw: Some(json!({ "direct": true, "provider": "all_debrid" })),
            });
        }
        self.unlock_link(link, None, filename_from_url_path(link).as_deref(), None)
            .await
    }

    async fn refresh_progress(&self, remote_release_id: &str) -> Result<DebridReleaseProgress> {
        let status = self.magnet_status(remote_release_id).await?;
        Ok(all_debrid_status_to_progress(&status))
    }

    async fn delete_release(&self, remote_release_id: &str) -> Result<bool> {
        match self.delete_magnet(remote_release_id).await {
            Ok(()) => Ok(true),
            Err(err) if all_debrid_error_is_not_found(&err) => Ok(false),
            Err(err) => Err(err),
        }
    }
}

#[derive(Clone)]
pub struct PremiumizeClient {
    http: Client,
    base_url: String,
    token: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct PremiumizeEnvelope {
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    message: Option<String>,
}

#[derive(Debug, Clone)]
struct PremiumizeDirectDlSnapshot {
    release: DebridRemoteRelease,
    files: Vec<DebridRemoteFile>,
    links: Vec<DebridResolvedLink>,
    raw: Value,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct PremiumizeTransfer {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    progress: Option<f64>,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    folder_id: Option<String>,
    #[serde(default)]
    file_id: Option<String>,
}

impl PremiumizeClient {
    pub fn new(token: impl Into<String>) -> Result<Self> {
        Self::with_base_url(token, PREMIUMIZE_API_BASE)
    }

    fn with_base_url(token: impl Into<String>, base_url: impl Into<String>) -> Result<Self> {
        let token = token.into();
        let provider_name = DebridServiceKind::Premiumize.display_name();
        if token.trim().is_empty() {
            bail!("{provider_name} API token is required");
        }
        Ok(Self {
            http: Client::builder()
                .user_agent(format!("Elixir/0.1 {}", provider_name))
                .timeout(Duration::from_secs(30))
                .build()
                .with_context(|| format!("building {provider_name} HTTP client"))?,
            base_url: base_url.into().trim_end_matches('/').to_string(),
            token,
        })
    }

    async fn account_info(&self) -> Result<Value> {
        self.request_value(Method::GET, "account/info", &[], &[])
            .await
    }

    async fn directdl_snapshot(
        &self,
        source: &str,
        source_kind: &str,
    ) -> Result<PremiumizeDirectDlSnapshot> {
        let source = source.trim();
        if source.is_empty() {
            bail!(
                "{} directdl requires a non-empty source",
                DebridServiceKind::Premiumize.display_name()
            );
        }
        let value = self
            .request_value(Method::POST, "transfer/directdl", &[], &[("src", source)])
            .await?;
        let snapshot = premiumize_directdl_snapshot(source, source_kind, value, &self.token)?;
        premiumize_cache_directdl_snapshot(snapshot.clone())?;
        Ok(snapshot)
    }

    fn directdl_snapshot_by_id(
        &self,
        remote_release_id: &str,
    ) -> Result<PremiumizeDirectDlSnapshot> {
        premiumize_directdl_snapshot_by_id(remote_release_id)
    }

    async fn create_transfer(&self, source: &str) -> Result<DebridRemoteRelease> {
        let source = source.trim();
        if source.is_empty() {
            bail!(
                "{} transfer/create requires a non-empty source",
                DebridServiceKind::Premiumize.display_name()
            );
        }
        let value = self
            .request_value(Method::POST, "transfer/create", &[], &[("src", source)])
            .await?;
        if premiumize_value_key_string(&value, "type")
            .map(|value| value.eq_ignore_ascii_case("container"))
            .unwrap_or(false)
        {
            return Err(premiumize_error_to_anyhow(DebridProviderError {
                kind: DebridProviderErrorKind::Permanent,
                provider_code: Some("unsupported_container".to_string()),
                message: "Premiumize transfer/create returned a container response; Elixir does not fan out container links yet".to_string(),
            }));
        }
        let remote_release_id = premiumize_value_key_string(&value, "id").ok_or_else(|| {
            premiumize_error_to_anyhow(DebridProviderError {
                kind: DebridProviderErrorKind::Permanent,
                provider_code: Some("invalid_request".to_string()),
                message: "Premiumize transfer/create did not return a transfer id".to_string(),
            })
        })?;
        Ok(DebridRemoteRelease {
            provider_implementation: self.implementation().to_string(),
            remote_release_id,
            display_name: premiumize_value_key_string(&value, "name"),
            status: DebridReleaseStatus::Staging,
            raw_status: Some("transfer_created".to_string()),
            raw: Some(value),
        })
    }

    async fn transfer_by_id(&self, remote_release_id: &str) -> Result<PremiumizeTransfer> {
        let remote_release_id = remote_release_id.trim();
        if remote_release_id.is_empty() {
            bail!(
                "{} transfer id is required",
                DebridServiceKind::Premiumize.display_name()
            );
        }
        let value = self
            .request_value(Method::GET, "transfer/list", &[], &[])
            .await?;
        let transfers = value
            .get("transfers")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                premiumize_error_to_anyhow(DebridProviderError {
                    kind: DebridProviderErrorKind::Permanent,
                    provider_code: Some("invalid_request".to_string()),
                    message: "Premiumize transfer/list response did not include transfers[]"
                        .to_string(),
                })
            })?;
        let transfer = transfers
            .iter()
            .find(|transfer| {
                premiumize_value_key_string(transfer, "id")
                    .map(|id| id == remote_release_id)
                    .unwrap_or(false)
            })
            .cloned()
            .ok_or_else(|| {
                premiumize_error_to_anyhow(DebridProviderError {
                    kind: DebridProviderErrorKind::NotFound,
                    provider_code: Some("not_found".to_string()),
                    message: format!("Premiumize transfer '{remote_release_id}' was not found"),
                })
            })?;
        serde_json::from_value(transfer)
            .with_context(|| format!("parsing Premiumize transfer '{remote_release_id}'"))
    }

    async fn item_details(&self, file_id: &str) -> Result<Value> {
        let file_id = file_id.trim();
        if file_id.is_empty() {
            bail!(
                "{} item/details requires a file id",
                DebridServiceKind::Premiumize.display_name()
            );
        }
        self.request_value(Method::GET, "item/details", &[("id", file_id)], &[])
            .await
    }

    async fn folder_list(&self, folder_id: &str) -> Result<Value> {
        let folder_id = folder_id.trim();
        if folder_id.is_empty() {
            bail!(
                "{} folder/list requires a folder id",
                DebridServiceKind::Premiumize.display_name()
            );
        }
        self.request_value(Method::GET, "folder/list", &[("id", folder_id)], &[])
            .await
    }

    async fn cloud_files_for_transfer(
        &self,
        transfer: &PremiumizeTransfer,
    ) -> Result<Vec<DebridRemoteFile>> {
        if premiumize_transfer_status(transfer) != DebridReleaseStatus::Downloaded {
            return Ok(Vec::new());
        }
        if let Some(file_id) = transfer.file_id.as_deref().and_then(non_empty) {
            let details = self.item_details(file_id).await?;
            return premiumize_cloud_files_from_item_details(&details, &self.token);
        }
        if let Some(folder_id) = transfer.folder_id.as_deref().and_then(non_empty) {
            let mut files = Vec::new();
            let mut visited = HashSet::new();
            self.flatten_cloud_folder(folder_id, &mut Vec::new(), &mut visited, &mut files)
                .await?;
            if files.is_empty() {
                return Err(premiumize_error_to_anyhow(DebridProviderError {
                    kind: DebridProviderErrorKind::NotFound,
                    provider_code: Some("not_found".to_string()),
                    message: format!(
                        "Premiumize transfer '{}' finished without resolvable cloud files",
                        transfer.id.as_deref().unwrap_or("unknown")
                    ),
                }));
            }
            return Ok(files);
        }
        Err(premiumize_error_to_anyhow(DebridProviderError {
            kind: DebridProviderErrorKind::NotFound,
            provider_code: Some("not_found".to_string()),
            message: format!(
                "Premiumize transfer '{}' finished without file_id or folder_id",
                transfer.id.as_deref().unwrap_or("unknown")
            ),
        }))
    }

    async fn flatten_cloud_folder(
        &self,
        folder_id: &str,
        path: &mut Vec<String>,
        visited: &mut HashSet<String>,
        files: &mut Vec<DebridRemoteFile>,
    ) -> Result<()> {
        let folder_id = folder_id.trim();
        if !visited.insert(folder_id.to_string()) {
            return Ok(());
        }
        let folder = self.folder_list(folder_id).await?;
        let content = folder
            .get("content")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                premiumize_error_to_anyhow(DebridProviderError {
                    kind: DebridProviderErrorKind::Permanent,
                    provider_code: Some("invalid_request".to_string()),
                    message: format!(
                        "Premiumize folder/list response for '{folder_id}' did not include content[]"
                    ),
                })
            })?;
        for entry in content {
            let entry_type = premiumize_value_key_string(entry, "type")
                .unwrap_or_default()
                .to_ascii_lowercase();
            let name =
                premiumize_value_key_string(entry, "name").unwrap_or_else(|| "unnamed".to_string());
            match entry_type.as_str() {
                "folder" => {
                    if let Some(child_id) = premiumize_value_key_string(entry, "id") {
                        path.push(name);
                        Box::pin(self.flatten_cloud_folder(&child_id, path, visited, files))
                            .await?;
                        path.pop();
                    }
                }
                "file" => {
                    if let Some(file) = premiumize_cloud_file_from_folder_entry(
                        entry,
                        path,
                        files.len(),
                        &self.token,
                    )? {
                        files.push(file);
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    async fn delete_transfer(&self, remote_release_id: &str) -> Result<()> {
        let remote_release_id = remote_release_id.trim();
        if remote_release_id.is_empty() {
            bail!(
                "{} transfer id is required",
                DebridServiceKind::Premiumize.display_name()
            );
        }
        let _ = self
            .request_value(
                Method::POST,
                "transfer/delete",
                &[],
                &[("id", remote_release_id)],
            )
            .await?;
        Ok(())
    }

    async fn request_value(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
        form: &[(&str, &str)],
    ) -> Result<Value> {
        let response = self.request(method, path, query, form).await?;
        let status = response.status();
        let body = response.text().await.with_context(|| {
            format!(
                "reading {} response body",
                DebridServiceKind::Premiumize.display_name()
            )
        })?;
        premiumize_response_value(status, &body, &self.token)
    }

    async fn request(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
        form: &[(&str, &str)],
    ) -> Result<reqwest::Response> {
        let mut url = Url::parse(&format!(
            "{}/{}",
            self.base_url,
            path.trim_start_matches('/')
        ))
        .with_context(|| format!("building Premiumize API URL for {path}"))?;
        if !query.is_empty() {
            url.query_pairs_mut().extend_pairs(query.iter().copied());
        }
        let mut request = self
            .http
            .request(method, url)
            .bearer_auth(self.token.trim());
        if !form.is_empty() {
            request = request.form(form);
        }
        request.send().await.with_context(|| {
            format!(
                "calling {} API",
                DebridServiceKind::Premiumize.display_name()
            )
        })
    }
}

#[async_trait]
impl DebridProviderAdapter for PremiumizeClient {
    fn implementation(&self) -> &str {
        DebridServiceKind::Premiumize.implementation_id()
    }

    fn capabilities(&self) -> DebridProviderCapabilities {
        premiumize_lifecycle_capabilities()
    }

    async fn test_account(&self) -> Result<DebridAccount> {
        let account = self.account_info().await?;
        Ok(DebridAccount {
            provider_implementation: self.implementation().to_string(),
            account_id: premiumize_value_key_string(&account, "customer_id"),
            username: None,
            raw: Some(account),
        })
    }

    async fn submit_magnet(&self, magnet: &str) -> Result<DebridRemoteRelease> {
        match self.directdl_snapshot(magnet, "magnet").await {
            Ok(snapshot) => Ok(snapshot.release),
            Err(err) if premiumize_directdl_error_should_queue_transfer(&err) => {
                self.create_transfer(magnet).await
            }
            Err(err) => {
                let message = err.to_string();
                if premiumize_error_message_is_rejected_source(&message) {
                    Err(anyhow!("magnet rejected by Premiumize directdl: {message}"))
                } else {
                    Err(err)
                }
            }
        }
    }

    async fn inspect_release(&self, remote_release_id: &str) -> Result<DebridReleaseInspection> {
        if is_premiumize_directdl_release_id(remote_release_id) {
            let snapshot = self.directdl_snapshot_by_id(remote_release_id)?;
            return premiumize_directdl_inspection(&snapshot, None);
        }
        let transfer = self.transfer_by_id(remote_release_id).await?;
        let files = self.cloud_files_for_transfer(&transfer).await?;
        premiumize_transfer_to_inspection(&transfer, files, Vec::new(), None)
    }

    async fn select_files(
        &self,
        remote_release_id: &str,
        selected_file_ids: &[String],
    ) -> Result<DebridReleaseInspection> {
        if selected_file_ids.is_empty() {
            bail!(
                "{} file selection requires at least one file id",
                DebridServiceKind::Premiumize.display_name()
            );
        }
        if is_premiumize_directdl_release_id(remote_release_id) {
            let snapshot = self.directdl_snapshot_by_id(remote_release_id)?;
            let selected = premiumize_selected_file_ids(&snapshot.files, selected_file_ids)?;
            return premiumize_directdl_inspection(&snapshot, Some(&selected));
        }

        let transfer = self.transfer_by_id(remote_release_id).await?;
        let files = self.cloud_files_for_transfer(&transfer).await?;
        let selected = if premiumize_transfer_status(&transfer) == DebridReleaseStatus::Downloaded {
            premiumize_selected_file_ids(&files, selected_file_ids)?
        } else {
            selected_file_ids.to_vec()
        };
        let links = if premiumize_transfer_status(&transfer) == DebridReleaseStatus::Downloaded {
            premiumize_cloud_selected_links(&files, &selected)
        } else {
            Vec::new()
        };
        premiumize_transfer_to_inspection(&transfer, files, links, Some(&selected))
    }

    async fn list_links(&self, remote_release_id: &str) -> Result<Vec<DebridResolvedLink>> {
        if is_premiumize_directdl_release_id(remote_release_id) {
            let snapshot = self.directdl_snapshot_by_id(remote_release_id)?;
            return Ok(premiumize_directdl_selectable_links(&snapshot));
        }
        let transfer = self.transfer_by_id(remote_release_id).await?;
        if premiumize_transfer_status(&transfer) != DebridReleaseStatus::Downloaded {
            return Ok(Vec::new());
        }
        let files = self.cloud_files_for_transfer(&transfer).await?;
        Ok(files
            .iter()
            .filter(|file| file.selectable)
            .filter_map(premiumize_cloud_file_link)
            .collect())
    }

    async fn unrestrict_hoster(&self, link: &str) -> Result<DebridResolvedLink> {
        if premiumize_is_direct_download_url(link, &self.base_url) {
            let link = link.trim();
            return Ok(DebridResolvedLink {
                provider_file_id: None,
                url: link.to_string(),
                filename: filename_from_url_path(link),
                size_bytes: None,
                raw: Some(json!({ "direct": true, "provider": "premiumize" })),
            });
        }
        let snapshot = self.directdl_snapshot(link, "hoster").await?;
        let links = premiumize_directdl_selectable_links(&snapshot);
        if links.len() != 1 {
            bail!(
                "{} directdl hoster unrestrict expected exactly one file, got {}",
                DebridServiceKind::Premiumize.display_name(),
                links.len()
            );
        }
        Ok(links.into_iter().next().expect("one Premiumize link"))
    }

    async fn refresh_progress(&self, remote_release_id: &str) -> Result<DebridReleaseProgress> {
        if is_premiumize_directdl_release_id(remote_release_id) {
            let snapshot = self.directdl_snapshot_by_id(remote_release_id)?;
            return Ok(premiumize_directdl_progress(&snapshot));
        }
        let transfer = self.transfer_by_id(remote_release_id).await?;
        Ok(premiumize_transfer_progress(&transfer))
    }

    async fn delete_release(&self, remote_release_id: &str) -> Result<bool> {
        if is_premiumize_directdl_release_id(remote_release_id) {
            premiumize_remove_directdl_snapshot(remote_release_id)?;
            return Ok(false);
        }
        match self.delete_transfer(remote_release_id).await {
            Ok(()) => Ok(true),
            Err(err) if premiumize_error_is_not_found(&err) => Ok(false),
            Err(err) => Err(err),
        }
    }
}

pub struct DebridAdapterFactory<'a> {
    secrets: &'a SecretsManager,
}

impl<'a> DebridAdapterFactory<'a> {
    pub fn new(secrets: &'a SecretsManager) -> Self {
        Self { secrets }
    }

    pub fn from_state(state: &'a AppState) -> Self {
        Self::new(&state.secrets)
    }

    #[allow(dead_code)]
    pub async fn adapter_for_active_service(
        &self,
        store: &ExtensionStore<'_>,
        instance_id: Uuid,
    ) -> Result<Box<dyn DebridProviderAdapter>> {
        let service = active_debrid_service_for_instance(store, instance_id).await?;
        self.adapter_for_service(store, instance_id, service).await
    }

    pub async fn adapter_for_provider_implementation(
        &self,
        store: &ExtensionStore<'_>,
        instance_id: Uuid,
        provider_implementation: Option<&str>,
    ) -> Result<Box<dyn DebridProviderAdapter>> {
        let service =
            debrid_service_for_provider_or_instance(store, instance_id, provider_implementation)
                .await?;
        self.adapter_for_service(store, instance_id, service).await
    }

    pub async fn adapter_for_job_implementation(
        &self,
        store: &ExtensionStore<'_>,
        instance_id: Uuid,
        provider_implementation: Option<&str>,
    ) -> Result<Box<dyn DebridProviderAdapter>> {
        let service = provider_implementation
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(DebridServiceKind::from_implementation_id)
            .unwrap_or(Ok(DebridServiceKind::RealDebrid))?;
        self.adapter_for_service(store, instance_id, service).await
    }

    pub async fn adapter_for_service(
        &self,
        store: &ExtensionStore<'_>,
        instance_id: Uuid,
        service: DebridServiceKind,
    ) -> Result<Box<dyn DebridProviderAdapter>> {
        let token =
            debrid_token_for_instance_with_secrets(self.secrets, store, instance_id, service)
                .await?;
        match service {
            DebridServiceKind::RealDebrid => {
                #[cfg(test)]
                if let Some(base_url) =
                    test_real_debrid_api_base_url_for_instance(store, instance_id).await?
                {
                    return Ok(Box::new(RealDebridClient::with_base_url(token, base_url)?)
                        as Box<dyn DebridProviderAdapter>);
                }
                Ok(Box::new(RealDebridClient::new(token)?) as Box<dyn DebridProviderAdapter>)
            }
            DebridServiceKind::TorBox => {
                #[cfg(test)]
                if let Some(base_url) =
                    test_torbox_api_base_url_for_instance(store, instance_id).await?
                {
                    return Ok(Box::new(TorBoxClient::with_base_url(token, base_url)?)
                        as Box<dyn DebridProviderAdapter>);
                }
                Ok(Box::new(TorBoxClient::new(token)?) as Box<dyn DebridProviderAdapter>)
            }
            DebridServiceKind::AllDebrid => {
                #[cfg(test)]
                if let Some(base_url) =
                    test_all_debrid_api_base_url_for_instance(store, instance_id).await?
                {
                    return Ok(Box::new(AllDebridClient::with_base_url(token, base_url)?)
                        as Box<dyn DebridProviderAdapter>);
                }
                Ok(Box::new(AllDebridClient::new(token)?) as Box<dyn DebridProviderAdapter>)
            }
            DebridServiceKind::Premiumize => {
                #[cfg(test)]
                if let Some(base_url) =
                    test_premiumize_api_base_url_for_instance(store, instance_id).await?
                {
                    return Ok(Box::new(PremiumizeClient::with_base_url(token, base_url)?)
                        as Box<dyn DebridProviderAdapter>);
                }
                Ok(Box::new(PremiumizeClient::new(token)?) as Box<dyn DebridProviderAdapter>)
            }
        }
    }
}

#[cfg(test)]
async fn test_real_debrid_api_base_url_for_instance(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
) -> Result<Option<String>> {
    let Some(instance) = store.get_instance(instance_id).await? else {
        return Ok(None);
    };
    Ok(instance
        .config_json
        .as_ref()
        .and_then(|config| config.get("testRealDebridApiBaseUrl"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string))
}

#[cfg(test)]
async fn test_torbox_api_base_url_for_instance(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
) -> Result<Option<String>> {
    let Some(instance) = store.get_instance(instance_id).await? else {
        return Ok(None);
    };
    Ok(instance
        .config_json
        .as_ref()
        .and_then(|config| config.get("testTorBoxApiBaseUrl"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string))
}

#[cfg(test)]
async fn test_all_debrid_api_base_url_for_instance(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
) -> Result<Option<String>> {
    let Some(instance) = store.get_instance(instance_id).await? else {
        return Ok(None);
    };
    Ok(instance
        .config_json
        .as_ref()
        .and_then(|config| config.get("testAllDebridApiBaseUrl"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string))
}

#[cfg(test)]
async fn test_premiumize_api_base_url_for_instance(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
) -> Result<Option<String>> {
    let Some(instance) = store.get_instance(instance_id).await? else {
        return Ok(None);
    };
    Ok(instance
        .config_json
        .as_ref()
        .and_then(|config| config.get("testPremiumizeApiBaseUrl"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string))
}

#[cfg(test)]
#[derive(Clone)]
struct UnsupportedDebridAdapter {
    service: DebridServiceKind,
}

#[cfg(test)]
#[async_trait]
impl DebridProviderAdapter for UnsupportedDebridAdapter {
    fn implementation(&self) -> &str {
        self.service.implementation_id()
    }

    fn capabilities(&self) -> DebridProviderCapabilities {
        unsupported_debrid_capabilities()
    }

    async fn test_account(&self) -> Result<DebridAccount> {
        Err(provider_unsupported_error(
            self.service,
            "account validation",
        ))
    }

    async fn submit_magnet(&self, _magnet: &str) -> Result<DebridRemoteRelease> {
        Err(provider_unsupported_error(
            self.service,
            "magnet submission",
        ))
    }

    async fn inspect_release(&self, _remote_release_id: &str) -> Result<DebridReleaseInspection> {
        Err(provider_unsupported_error(
            self.service,
            "release inspection",
        ))
    }

    async fn select_files(
        &self,
        _remote_release_id: &str,
        _selected_file_ids: &[String],
    ) -> Result<DebridReleaseInspection> {
        Err(provider_unsupported_error(self.service, "file selection"))
    }

    async fn list_links(&self, _remote_release_id: &str) -> Result<Vec<DebridResolvedLink>> {
        Err(provider_unsupported_error(self.service, "link listing"))
    }

    async fn unrestrict_hoster(&self, _link: &str) -> Result<DebridResolvedLink> {
        Err(provider_unsupported_error(
            self.service,
            "hoster unrestrict",
        ))
    }

    async fn refresh_progress(&self, _remote_release_id: &str) -> Result<DebridReleaseProgress> {
        Err(provider_unsupported_error(self.service, "progress refresh"))
    }

    async fn delete_release(&self, _remote_release_id: &str) -> Result<bool> {
        Err(provider_unsupported_error(self.service, "remote delete"))
    }
}

#[cfg(test)]
fn unsupported_debrid_capabilities() -> DebridProviderCapabilities {
    DebridProviderCapabilities {
        supports_magnet_submit: false,
        supports_hoster_unrestrict: false,
        supports_file_listing: false,
        supports_file_selection: false,
        supports_cache_check: false,
        supports_delete: false,
        supports_progress: false,
        file_selection_mode: DebridFileSelectionMode::Unsupported,
    }
}

fn premiumize_directdl_capabilities() -> DebridProviderCapabilities {
    DebridProviderCapabilities {
        supports_magnet_submit: true,
        supports_hoster_unrestrict: true,
        supports_file_listing: true,
        supports_file_selection: true,
        supports_cache_check: false,
        supports_delete: false,
        supports_progress: true,
        file_selection_mode: DebridFileSelectionMode::AfterTransfer,
    }
}

fn premiumize_lifecycle_capabilities() -> DebridProviderCapabilities {
    DebridProviderCapabilities {
        supports_magnet_submit: true,
        supports_hoster_unrestrict: true,
        supports_file_listing: true,
        supports_file_selection: true,
        supports_cache_check: false,
        supports_delete: true,
        supports_progress: true,
        file_selection_mode: DebridFileSelectionMode::AfterTransfer,
    }
}

fn all_debrid_lifecycle_capabilities() -> DebridProviderCapabilities {
    DebridProviderCapabilities {
        supports_magnet_submit: true,
        supports_hoster_unrestrict: true,
        supports_file_listing: true,
        supports_file_selection: true,
        supports_cache_check: false,
        supports_delete: true,
        supports_progress: true,
        file_selection_mode: DebridFileSelectionMode::AfterTransfer,
    }
}

fn torbox_lifecycle_capabilities() -> DebridProviderCapabilities {
    DebridProviderCapabilities {
        supports_magnet_submit: true,
        supports_hoster_unrestrict: false,
        supports_file_listing: true,
        supports_file_selection: true,
        supports_cache_check: true,
        supports_delete: true,
        supports_progress: true,
        file_selection_mode: DebridFileSelectionMode::AfterTransfer,
    }
}

#[cfg(test)]
fn provider_unsupported_error(service: DebridServiceKind, operation: &str) -> anyhow::Error {
    anyhow!(
        "provider unsupported: {} native adapter is not implemented yet for {operation}",
        service.display_name()
    )
}

fn premiumize_response_value(status: StatusCode, body: &str, token: &str) -> Result<Value> {
    let parsed = match serde_json::from_str::<Value>(body) {
        Ok(parsed) => parsed,
        Err(_) => {
            let error = DebridProviderError {
                kind: premiumize_error_kind(status, None, body),
                provider_code: Some(status.as_u16().to_string()),
                message: redacted_body_with_secret(body, token),
            };
            return Err(premiumize_error_to_anyhow(error));
        }
    };
    let envelope = serde_json::from_value::<PremiumizeEnvelope>(parsed.clone()).ok();
    let api_status = envelope
        .as_ref()
        .and_then(|envelope| envelope.status.as_deref())
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if status.is_success() && api_status == "success" {
        return Ok(parsed);
    }

    let provider_code = envelope
        .as_ref()
        .and_then(|envelope| envelope.code.clone())
        .or_else(|| premiumize_value_key_string(&parsed, "code"))
        .or_else(|| Some(status.as_u16().to_string()));
    let message = envelope
        .as_ref()
        .and_then(|envelope| envelope.message.clone())
        .or_else(|| premiumize_value_key_string(&parsed, "message"))
        .unwrap_or_else(|| redacted_body_with_secret(body, token));
    let error = DebridProviderError {
        kind: premiumize_error_kind(status, provider_code.as_deref(), &message),
        provider_code: provider_code.map(|code| redacted_body_with_secret(&code, token)),
        message: redacted_body_with_secret(&message, token),
    };
    Err(premiumize_error_to_anyhow(error))
}

fn premiumize_value_key_string(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(premiumize_value_message)
}

fn premiumize_value_message(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => non_empty(value).map(str::to_string),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        Value::Object(object) => object
            .get("message")
            .or_else(|| object.get("detail"))
            .or_else(|| object.get("error"))
            .and_then(premiumize_value_message)
            .or_else(|| Some(value.to_string())),
        Value::Array(_) => Some(value.to_string()),
        Value::Null => None,
    }
}

fn premiumize_error_kind(
    status: StatusCode,
    provider_code: Option<&str>,
    message: &str,
) -> DebridProviderErrorKind {
    let code = provider_code
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let lower = message.to_ascii_lowercase();
    if matches!(status.as_u16(), 401 | 403)
        || code == "authentication_failed"
        || code == "permission_denied"
        || lower.contains("api key")
        || lower.contains("apikey")
        || lower.contains("auth")
        || lower.contains("unauthorized")
        || lower.contains("forbidden")
        || lower.contains("permission denied")
    {
        DebridProviderErrorKind::Unauthorized
    } else if code == "rate_limit_reached"
        || code == "account_limit_reached"
        || code == "service_limit_reached"
        || lower.contains("rate limit")
        || lower.contains("too many requests")
        || lower.contains("fair-use")
        || lower.contains("booster")
        || lower.contains("account limit")
        || lower.contains("service limit")
    {
        DebridProviderErrorKind::RateLimited
    } else if status == StatusCode::NOT_FOUND || code == "not_found" {
        DebridProviderErrorKind::NotFound
    } else if status.is_server_error()
        || code == "unknown_error"
        || code == "service_down"
        || code == "semi_permanent_error"
        || code == "link_generation_failed"
        || code == "transient_error"
        || lower.contains("temporar")
        || lower.contains("unavailable")
        || lower.contains("service down")
        || lower.contains("try again")
        || lower.contains("timeout")
    {
        DebridProviderErrorKind::Temporary
    } else if code == "service_unsupported"
        || code == "invalid_request"
        || code == "permanent_error"
        || lower.contains("unsupported")
        || lower.contains("invalid")
        || lower.contains("malformed")
    {
        DebridProviderErrorKind::Permanent
    } else {
        DebridProviderErrorKind::Unknown
    }
}

fn premiumize_error_to_anyhow(error: DebridProviderError) -> anyhow::Error {
    anyhow!(
        "Premiumize API {}{}: {}",
        premiumize_error_kind_label(error.kind),
        error
            .provider_code
            .as_ref()
            .map(|code| format!(" ({code})"))
            .unwrap_or_default(),
        error.message
    )
}

fn premiumize_error_kind_label(kind: DebridProviderErrorKind) -> &'static str {
    match kind {
        DebridProviderErrorKind::Unauthorized => "auth error",
        DebridProviderErrorKind::NotFound => "not found",
        DebridProviderErrorKind::RateLimited => "provider unavailable",
        DebridProviderErrorKind::SelectionUnsupported => "selection unsupported",
        DebridProviderErrorKind::Temporary => "temporary error",
        DebridProviderErrorKind::Permanent => "request rejected",
        DebridProviderErrorKind::Unknown => "error",
    }
}

fn premiumize_directdl_snapshot(
    source: &str,
    source_kind: &str,
    value: Value,
    token: &str,
) -> Result<PremiumizeDirectDlSnapshot> {
    let content = value
        .get("content")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            premiumize_error_to_anyhow(DebridProviderError {
                kind: DebridProviderErrorKind::Permanent,
                provider_code: Some("invalid_request".to_string()),
                message: "Premiumize directdl response did not include a content array".to_string(),
            })
        })?;
    if content.is_empty() {
        return Err(premiumize_error_to_anyhow(DebridProviderError {
            kind: DebridProviderErrorKind::Permanent,
            provider_code: Some("empty_content".to_string()),
            message: format!("Premiumize directdl returned no content for {source_kind} source"),
        }));
    }

    let mut files = Vec::new();
    let mut links = Vec::new();
    for (index, entry) in content.iter().enumerate() {
        let link = premiumize_value_key_string(entry, "link");
        if let Some(link) = link.as_deref()
            && !token.trim().is_empty()
            && link.contains(token.trim())
        {
            return Err(premiumize_error_to_anyhow(DebridProviderError {
                kind: DebridProviderErrorKind::Permanent,
                provider_code: Some("token_leak".to_string()),
                message: "Premiumize directdl returned a URL containing the API token".to_string(),
            }));
        }
        let path = premiumize_value_key_string(entry, "path")
            .or_else(|| link.as_deref().and_then(filename_from_url_path))
            .unwrap_or_else(|| format!("premiumize-directdl-file-{}", index.saturating_add(1)));
        let basename = basename_from_remote_path(&path);
        let size_bytes = entry.get("size").and_then(Value::as_u64);
        let provider_file_id =
            premiumize_directdl_provider_file_id(index, &path, size_bytes, link.as_deref());
        let selectable = link.is_some() && premiumize_directdl_path_is_selectable(&path);
        files.push(DebridRemoteFile {
            provider_file_id: provider_file_id.clone(),
            file_index: Some(index as i64),
            path: path.clone(),
            basename: basename.clone(),
            size_bytes,
            selectable,
            selected: None,
            raw: Some(entry.clone()),
        });
        if let Some(link) = link {
            links.push(DebridResolvedLink {
                provider_file_id: Some(provider_file_id),
                url: link,
                filename: Some(basename),
                size_bytes,
                raw: Some(json!({
                    "provider": "premiumize",
                    "directdl": true,
                    "content": entry
                })),
            });
        }
    }

    if links.is_empty() {
        return Err(premiumize_error_to_anyhow(DebridProviderError {
            kind: DebridProviderErrorKind::Temporary,
            provider_code: Some("link_generation_failed".to_string()),
            message: format!(
                "Premiumize directdl returned {} content entries without downloadable links",
                files.len()
            ),
        }));
    }

    let source_hash = short_sha256_hex(source.as_bytes(), 16);
    let fingerprint = premiumize_directdl_content_fingerprint(&files, &links);
    let remote_release_id = format!("pm-directdl-{source_hash}-{fingerprint}");
    let display_name = if files.len() == 1 {
        files.first().map(|file| file.basename.clone())
    } else {
        premiumize_directdl_common_root(&files)
            .or_else(|| Some(format!("Premiumize directdl pack ({})", files.len())))
    };
    let release_raw = json!({
        "directdl": true,
        "sourceKind": source_kind,
        "sourceHash": source_hash,
        "contentFingerprint": fingerprint,
        "contentCount": files.len()
    });
    Ok(PremiumizeDirectDlSnapshot {
        release: DebridRemoteRelease {
            provider_implementation: DebridServiceKind::Premiumize
                .implementation_id()
                .to_string(),
            remote_release_id,
            display_name,
            status: DebridReleaseStatus::Downloaded,
            raw_status: Some("directdl_ready".to_string()),
            raw: Some(release_raw),
        },
        files,
        links,
        raw: value,
    })
}

fn premiumize_transfer_to_inspection(
    transfer: &PremiumizeTransfer,
    mut files: Vec<DebridRemoteFile>,
    links: Vec<DebridResolvedLink>,
    selected_file_ids: Option<&[String]>,
) -> Result<DebridReleaseInspection> {
    let selected_file_ids = selected_file_ids
        .map(|ids| ids.iter().cloned().collect::<HashSet<_>>())
        .unwrap_or_default();
    if !selected_file_ids.is_empty() {
        for file in &mut files {
            file.selected = Some(selected_file_ids.contains(&file.provider_file_id));
        }
    }
    let mut selected_file_ids = selected_file_ids.into_iter().collect::<Vec<_>>();
    selected_file_ids.sort();
    let mut skipped_file_ids = if selected_file_ids.is_empty() {
        Vec::new()
    } else {
        files
            .iter()
            .filter(|file| !selected_file_ids.contains(&file.provider_file_id))
            .map(|file| file.provider_file_id.clone())
            .collect::<Vec<_>>()
    };
    skipped_file_ids.sort();
    let progress = premiumize_transfer_progress(transfer);
    let provider_status = premiumize_transfer_provider_status(transfer, progress.status);
    let raw = json!({
        "transfer": transfer,
        "providerStatus": provider_status,
    });
    let remote_release_id = transfer
        .id
        .clone()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "unknown-premiumize-transfer".to_string());
    Ok(DebridReleaseInspection {
        release: DebridRemoteRelease {
            provider_implementation: DebridServiceKind::Premiumize
                .implementation_id()
                .to_string(),
            remote_release_id,
            display_name: transfer.name.clone(),
            status: progress.status,
            raw_status: Some(premiumize_transfer_raw_status(transfer)),
            raw: Some(raw.clone()),
        },
        capabilities: premiumize_lifecycle_capabilities(),
        files,
        links,
        progress: Some(progress),
        selection: Some(DebridFileSelection {
            mode: DebridFileSelectionMode::AfterTransfer,
            selected_file_ids,
            skipped_file_ids,
        }),
        raw: Some(raw),
    })
}

fn premiumize_transfer_provider_status(
    transfer: &PremiumizeTransfer,
    release_status: DebridReleaseStatus,
) -> Value {
    let raw_status = transfer
        .status
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let message = transfer
        .message
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let raw_status_string = premiumize_transfer_raw_status(transfer);
    let provider_failure_class = if release_status == DebridReleaseStatus::Failed {
        classify_debrid_failure("failed", raw_status, message, None)
            .filter(|failure_class| *failure_class != DebridFailureClass::Unknown)
    } else {
        None
    };
    let message_lower = message.unwrap_or_default().to_ascii_lowercase();
    let no_seeds = provider_failure_class == Some(DebridFailureClass::NoSeeds)
        || message_lower.contains("no seed")
        || message_lower.contains("no peer");
    let not_cached = release_status == DebridReleaseStatus::Failed
        && no_seeds
        && (message_lower.contains("not cached")
            || message_lower.contains("uncached")
            || message_lower.contains("cache miss"));
    json!({
        "providerImplementation": DebridServiceKind::Premiumize.implementation_id(),
        "providerName": DebridServiceKind::Premiumize.display_name(),
        "status": release_status.as_str(),
        "providerState": raw_status,
        "rawStatus": raw_status_string,
        "providerFailureClass": provider_failure_class.map(DebridFailureClass::as_str),
        "retryable": provider_failure_class
            .map(|failure_class| failure_class.response_policy() == DebridFailureResponsePolicy::TryAlternateRouteOrCandidate)
            .unwrap_or(false),
        "cached": if not_cached { Some(false) } else { None },
        "notCached": not_cached,
        "noSeeds": no_seeds,
        "progress": transfer.progress,
        "message": premiumize_transfer_user_message(
            message,
            provider_failure_class,
            not_cached,
        ),
    })
}

fn premiumize_transfer_user_message(
    message: Option<&str>,
    failure_class: Option<DebridFailureClass>,
    not_cached: bool,
) -> Option<String> {
    match failure_class {
        Some(DebridFailureClass::NoSeeds) if not_cached => Some(
            "Premiumize accepted this transfer, but it is not cached and has no peers.".to_string(),
        ),
        Some(failure_class) => Some(format!(
            "Premiumize reported {}{}.",
            failure_class.as_str(),
            message
                .map(|message| format!(" ({message})"))
                .unwrap_or_default()
        )),
        None => message.map(str::to_string),
    }
}

fn premiumize_transfer_progress(transfer: &PremiumizeTransfer) -> DebridReleaseProgress {
    DebridReleaseProgress {
        status: premiumize_transfer_status(transfer),
        progress: premiumize_transfer_progress_fraction(transfer),
        downloaded_bytes: None,
        total_bytes: None,
        download_rate_bps: None,
        raw: Some(serde_json::to_value(transfer).unwrap_or_else(|_| json!({}))),
    }
}

fn premiumize_transfer_status(transfer: &PremiumizeTransfer) -> DebridReleaseStatus {
    match transfer
        .status
        .as_deref()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "finished" | "seeding" => DebridReleaseStatus::Downloaded,
        "running" => DebridReleaseStatus::Transferring,
        "queued" => DebridReleaseStatus::Staging,
        "error" => DebridReleaseStatus::Failed,
        "deleted" | "cancelled" | "canceled" => DebridReleaseStatus::Cancelled,
        _ => DebridReleaseStatus::Staging,
    }
}

fn premiumize_transfer_progress_fraction(transfer: &PremiumizeTransfer) -> Option<f64> {
    match premiumize_transfer_status(transfer) {
        DebridReleaseStatus::Downloaded => Some(1.0),
        DebridReleaseStatus::Failed => Some(0.0),
        _ => transfer.progress.map(|value| value.clamp(0.0, 1.0)),
    }
}

fn premiumize_transfer_raw_status(transfer: &PremiumizeTransfer) -> String {
    let status = transfer
        .status
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("unknown");
    match transfer
        .message
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(message) => format!("{status}: {message}"),
        None => status.to_string(),
    }
}

fn premiumize_cloud_files_from_item_details(
    value: &Value,
    token: &str,
) -> Result<Vec<DebridRemoteFile>> {
    let file = premiumize_cloud_file_from_item_value(value, 0, token)?.ok_or_else(|| {
        premiumize_error_to_anyhow(DebridProviderError {
            kind: DebridProviderErrorKind::NotFound,
            provider_code: Some("not_found".to_string()),
            message: "Premiumize item/details did not return a downloadable file".to_string(),
        })
    })?;
    Ok(vec![file])
}

fn premiumize_cloud_file_from_item_value(
    value: &Value,
    index: usize,
    token: &str,
) -> Result<Option<DebridRemoteFile>> {
    let id = premiumize_value_key_string(value, "id").ok_or_else(|| {
        premiumize_error_to_anyhow(DebridProviderError {
            kind: DebridProviderErrorKind::Permanent,
            provider_code: Some("invalid_request".to_string()),
            message: "Premiumize cloud file did not include id".to_string(),
        })
    })?;
    let link = premiumize_value_key_string(value, "link");
    let Some(link) = link else {
        return Ok(None);
    };
    premiumize_validate_download_link(&link, token)?;
    let name = premiumize_value_key_string(value, "name")
        .or_else(|| filename_from_url_path(&link))
        .unwrap_or_else(|| format!("premiumize-cloud-file-{}", index.saturating_add(1)));
    let path = premiumize_value_key_string(value, "path").unwrap_or_else(|| name.clone());
    let size_bytes = value.get("size").and_then(Value::as_u64);
    Ok(Some(DebridRemoteFile {
        provider_file_id: id,
        file_index: Some(index as i64),
        path: path.clone(),
        basename: basename_from_remote_path(&path),
        size_bytes,
        selectable: premiumize_directdl_path_is_selectable(&path),
        selected: None,
        raw: Some(json!({
            "provider": "premiumize",
            "cloud": true,
            "id": premiumize_value_key_string(value, "id"),
            "name": name,
            "path": path,
            "size": size_bytes,
            "mime_type": premiumize_value_key_string(value, "mime_type"),
            "folder_id": premiumize_value_key_string(value, "folder_id"),
            "link": link
        })),
    }))
}

fn premiumize_cloud_file_from_folder_entry(
    value: &Value,
    parent_path: &[String],
    index: usize,
    token: &str,
) -> Result<Option<DebridRemoteFile>> {
    let id = premiumize_value_key_string(value, "id").ok_or_else(|| {
        premiumize_error_to_anyhow(DebridProviderError {
            kind: DebridProviderErrorKind::Permanent,
            provider_code: Some("invalid_request".to_string()),
            message: "Premiumize folder file did not include id".to_string(),
        })
    })?;
    let link = premiumize_value_key_string(value, "link");
    let Some(link) = link else {
        return Ok(None);
    };
    premiumize_validate_download_link(&link, token)?;
    let name = premiumize_value_key_string(value, "name")
        .or_else(|| filename_from_url_path(&link))
        .unwrap_or_else(|| format!("premiumize-cloud-file-{}", index.saturating_add(1)));
    let mut path_parts = parent_path.to_vec();
    path_parts.push(name.clone());
    let path = path_parts.join("/");
    let size_bytes = value.get("size").and_then(Value::as_u64);
    Ok(Some(DebridRemoteFile {
        provider_file_id: id,
        file_index: Some(index as i64),
        path: path.clone(),
        basename: name.clone(),
        size_bytes,
        selectable: premiumize_directdl_path_is_selectable(&path),
        selected: None,
        raw: Some(json!({
            "provider": "premiumize",
            "cloud": true,
            "id": premiumize_value_key_string(value, "id"),
            "name": name,
            "path": path,
            "size": size_bytes,
            "mime_type": premiumize_value_key_string(value, "mime_type"),
            "link": link
        })),
    }))
}

fn premiumize_cloud_selected_links(
    files: &[DebridRemoteFile],
    selected_file_ids: &[String],
) -> Vec<DebridResolvedLink> {
    let selected = selected_file_ids.iter().cloned().collect::<HashSet<_>>();
    files
        .iter()
        .filter(|file| selected.contains(&file.provider_file_id))
        .filter_map(premiumize_cloud_file_link)
        .collect()
}

fn premiumize_cloud_file_link(file: &DebridRemoteFile) -> Option<DebridResolvedLink> {
    let link = file
        .raw
        .as_ref()
        .and_then(|raw| raw.get("link"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    Some(DebridResolvedLink {
        provider_file_id: Some(file.provider_file_id.clone()),
        url: link.to_string(),
        filename: Some(file.basename.clone()),
        size_bytes: file.size_bytes,
        raw: Some(json!({
            "provider": "premiumize",
            "cloud": true,
            "fileId": file.provider_file_id,
            "path": file.path
        })),
    })
}

fn premiumize_validate_download_link(link: &str, token: &str) -> Result<()> {
    if !token.trim().is_empty() && link.contains(token.trim()) {
        return Err(premiumize_error_to_anyhow(DebridProviderError {
            kind: DebridProviderErrorKind::Permanent,
            provider_code: Some("token_leak".to_string()),
            message: "Premiumize returned a URL containing the API token".to_string(),
        }));
    }
    Ok(())
}

fn premiumize_error_is_not_found(err: &anyhow::Error) -> bool {
    let message = err.to_string().to_ascii_lowercase();
    message.contains("not found") || message.contains("not_found") || message.contains("404")
}

fn premiumize_directdl_error_should_queue_transfer(err: &anyhow::Error) -> bool {
    let message = err.to_string().to_ascii_lowercase();
    (message.contains("link_generation_failed")
        || message.contains("transient_error")
        || message.contains("returned") && message.contains("without downloadable links"))
        && !message.contains("service_unsupported")
        && !message.contains("account_limit_reached")
        && !message.contains("service_limit_reached")
        && !message.contains("rate_limit_reached")
        && !message.contains("authentication_failed")
}

fn premiumize_cache_directdl_snapshot(snapshot: PremiumizeDirectDlSnapshot) -> Result<()> {
    let mut releases = PREMIUMIZE_DIRECTDL_RELEASES
        .lock()
        .map_err(|_| anyhow!("Premiumize directdl cache lock is poisoned"))?;
    releases.insert(snapshot.release.remote_release_id.clone(), snapshot);
    Ok(())
}

fn premiumize_directdl_snapshot_by_id(
    remote_release_id: &str,
) -> Result<PremiumizeDirectDlSnapshot> {
    let remote_release_id = remote_release_id.trim();
    if remote_release_id.is_empty() {
        bail!("Premiumize directdl release id is required");
    }
    let releases = PREMIUMIZE_DIRECTDL_RELEASES
        .lock()
        .map_err(|_| anyhow!("Premiumize directdl cache lock is poisoned"))?;
    releases.get(remote_release_id).cloned().ok_or_else(|| {
        premiumize_error_to_anyhow(DebridProviderError {
            kind: DebridProviderErrorKind::NotFound,
            provider_code: Some("not_found".to_string()),
            message: format!("Premiumize directdl release '{remote_release_id}' is not cached"),
        })
    })
}

fn premiumize_remove_directdl_snapshot(remote_release_id: &str) -> Result<()> {
    let mut releases = PREMIUMIZE_DIRECTDL_RELEASES
        .lock()
        .map_err(|_| anyhow!("Premiumize directdl cache lock is poisoned"))?;
    releases.remove(remote_release_id.trim());
    Ok(())
}

fn is_premiumize_directdl_release_id(remote_release_id: &str) -> bool {
    remote_release_id
        .trim()
        .to_ascii_lowercase()
        .starts_with("pm-directdl-")
}

fn premiumize_directdl_inspection(
    snapshot: &PremiumizeDirectDlSnapshot,
    selected_file_ids: Option<&[String]>,
) -> Result<DebridReleaseInspection> {
    let selected = selected_file_ids
        .map(|ids| ids.iter().cloned().collect::<HashSet<_>>())
        .unwrap_or_default();
    let mut files = snapshot.files.clone();
    if !selected.is_empty() {
        for file in &mut files {
            file.selected = Some(selected.contains(&file.provider_file_id));
        }
    }
    let links = if selected.is_empty() {
        Vec::new()
    } else {
        snapshot
            .links
            .iter()
            .filter(|link| {
                link.provider_file_id
                    .as_ref()
                    .map(|file_id| selected.contains(file_id))
                    .unwrap_or(false)
            })
            .cloned()
            .collect::<Vec<_>>()
    };
    let selection = if selected.is_empty() {
        None
    } else {
        let mut selected_file_ids = selected.into_iter().collect::<Vec<_>>();
        selected_file_ids.sort();
        let mut skipped_file_ids = files
            .iter()
            .filter(|file| !selected_file_ids.contains(&file.provider_file_id))
            .map(|file| file.provider_file_id.clone())
            .collect::<Vec<_>>();
        skipped_file_ids.sort();
        Some(DebridFileSelection {
            mode: DebridFileSelectionMode::AfterTransfer,
            selected_file_ids,
            skipped_file_ids,
        })
    };
    Ok(DebridReleaseInspection {
        release: snapshot.release.clone(),
        capabilities: premiumize_directdl_capabilities(),
        files,
        links,
        progress: Some(premiumize_directdl_progress(snapshot)),
        selection,
        raw: Some(snapshot.raw.clone()),
    })
}

fn premiumize_selected_file_ids(
    files: &[DebridRemoteFile],
    selected_file_ids: &[String],
) -> Result<Vec<String>> {
    let selectable = files
        .iter()
        .filter(|file| file.selectable)
        .map(|file| file.provider_file_id.clone())
        .collect::<BTreeSet<_>>();
    if selected_file_ids.iter().any(|file_id| file_id == "all") {
        return Ok(selectable.into_iter().collect());
    }
    let known = files
        .iter()
        .map(|file| file.provider_file_id.clone())
        .collect::<HashSet<_>>();
    let mut selected = BTreeSet::new();
    let mut unknown = Vec::new();
    let mut non_selectable = Vec::new();
    for file_id in selected_file_ids {
        if !known.contains(file_id) {
            unknown.push(file_id.clone());
        } else if !selectable.contains(file_id) {
            non_selectable.push(file_id.clone());
        } else {
            selected.insert(file_id.clone());
        }
    }
    if !unknown.is_empty() {
        bail!(
            "{} file selection referenced unknown file ids: {}",
            DebridServiceKind::Premiumize.display_name(),
            unknown.join(",")
        );
    }
    if !non_selectable.is_empty() {
        bail!(
            "{} file selection referenced non-selectable file ids: {}",
            DebridServiceKind::Premiumize.display_name(),
            non_selectable.join(",")
        );
    }
    if selected.is_empty() {
        bail!(
            "{} file selection did not include any selectable files",
            DebridServiceKind::Premiumize.display_name()
        );
    }
    Ok(selected.into_iter().collect())
}

fn premiumize_directdl_selectable_links(
    snapshot: &PremiumizeDirectDlSnapshot,
) -> Vec<DebridResolvedLink> {
    let selectable = snapshot
        .files
        .iter()
        .filter(|file| file.selectable)
        .map(|file| file.provider_file_id.clone())
        .collect::<HashSet<_>>();
    snapshot
        .links
        .iter()
        .filter(|link| {
            link.provider_file_id
                .as_ref()
                .map(|file_id| selectable.contains(file_id))
                .unwrap_or(false)
        })
        .cloned()
        .collect()
}

fn premiumize_directdl_progress(snapshot: &PremiumizeDirectDlSnapshot) -> DebridReleaseProgress {
    let total_bytes = snapshot
        .files
        .iter()
        .filter_map(|file| file.size_bytes)
        .reduce(|sum, value| sum.saturating_add(value));
    DebridReleaseProgress {
        status: DebridReleaseStatus::Downloaded,
        progress: Some(1.0),
        downloaded_bytes: total_bytes,
        total_bytes,
        download_rate_bps: Some(0),
        raw: Some(json!({
            "directdl": true,
            "contentCount": snapshot.files.len()
        })),
    }
}

fn premiumize_directdl_provider_file_id(
    index: usize,
    path: &str,
    size_bytes: Option<u64>,
    link: Option<&str>,
) -> String {
    let fingerprint = format!(
        "{}\n{}\n{}",
        path,
        size_bytes
            .map(|value| value.to_string())
            .unwrap_or_default(),
        link.unwrap_or_default()
    );
    format!(
        "pm-file-{:04}-{}",
        index.saturating_add(1),
        short_sha256_hex(fingerprint.as_bytes(), 8)
    )
}

fn premiumize_directdl_content_fingerprint(
    files: &[DebridRemoteFile],
    links: &[DebridResolvedLink],
) -> String {
    let links_by_id = links
        .iter()
        .filter_map(|link| Some((link.provider_file_id.as_ref()?, link.url.as_str())))
        .collect::<HashMap<_, _>>();
    let mut rows = files
        .iter()
        .map(|file| {
            format!(
                "{}:{}:{}:{}",
                file.provider_file_id,
                file.path,
                file.size_bytes
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
                links_by_id
                    .get(&file.provider_file_id)
                    .copied()
                    .unwrap_or_default()
            )
        })
        .collect::<Vec<_>>();
    rows.sort();
    short_sha256_hex(rows.join("\n").as_bytes(), 16)
}

fn premiumize_directdl_common_root(files: &[DebridRemoteFile]) -> Option<String> {
    let mut roots = files
        .iter()
        .filter_map(|file| {
            file.path
                .split('/')
                .next()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        })
        .collect::<Vec<_>>();
    roots.sort();
    roots.dedup();
    if roots.len() == 1 { roots.pop() } else { None }
}

fn premiumize_directdl_path_is_selectable(path: &str) -> bool {
    is_debrid_media_file(path)
        && !is_debrid_non_selectable_path(path)
        && !is_debrid_sample_or_extra_file(path)
}

fn premiumize_error_message_is_rejected_source(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("service_unsupported")
        || lower.contains("unsupported")
        || lower.contains("invalid_request")
        || lower.contains("invalid magnet")
        || lower.contains("bad magnet")
}

fn premiumize_is_direct_download_url(link: &str, api_base_url: &str) -> bool {
    let Ok(url) = Url::parse(link.trim()) else {
        return false;
    };
    if url.scheme() != "http" && url.scheme() != "https" {
        return false;
    }
    let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
    if host.contains("premiumize") || host.contains("energycdn") {
        return !url.path().starts_with("/api/");
    }
    #[cfg(test)]
    {
        if let Ok(api_base) = Url::parse(api_base_url) {
            return url.scheme() == api_base.scheme()
                && url.host_str() == api_base.host_str()
                && url.port_or_known_default() == api_base.port_or_known_default()
                && url.path().starts_with("/download/");
        }
    }
    #[cfg(not(test))]
    let _ = api_base_url;
    false
}

fn short_sha256_hex(input: &[u8], len: usize) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input);
    let hash = format!("{:x}", hasher.finalize());
    hash.chars().take(len).collect()
}

fn all_debrid_response_value(status: StatusCode, body: &str, token: &str) -> Result<Value> {
    let parsed = match serde_json::from_str::<Value>(body) {
        Ok(parsed) => parsed,
        Err(_) => {
            let error = DebridProviderError {
                kind: all_debrid_error_kind(status, None, body),
                provider_code: Some(status.as_u16().to_string()),
                message: redacted_body_with_secret(body, token),
            };
            return Err(all_debrid_error_to_anyhow(error));
        }
    };
    let envelope = serde_json::from_value::<AllDebridEnvelope>(parsed.clone()).ok();
    let api_status = envelope
        .as_ref()
        .and_then(|envelope| envelope.status.as_deref())
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if status.is_success() && api_status == "success" {
        if let Some(data) = envelope.and_then(|envelope| envelope.data) {
            return Ok(data);
        }
        return Ok(parsed);
    }

    let (provider_code, message) =
        all_debrid_error_details(body, envelope.as_ref(), &parsed, token);
    let error = DebridProviderError {
        kind: all_debrid_error_kind(status, provider_code.as_deref(), &message),
        provider_code: provider_code.or_else(|| Some(status.as_u16().to_string())),
        message,
    };
    Err(all_debrid_error_to_anyhow(error))
}

fn all_debrid_error_details(
    body: &str,
    envelope: Option<&AllDebridEnvelope>,
    parsed: &Value,
    token: &str,
) -> (Option<String>, String) {
    let error_value = envelope
        .and_then(|envelope| envelope.error.as_ref())
        .or_else(|| parsed.get("error"));
    let provider_code = error_value
        .and_then(|error| all_debrid_value_key_string(error, "code"))
        .or_else(|| all_debrid_value_key_string(parsed, "code"));
    let detail = error_value
        .and_then(|error| all_debrid_value_key_string(error, "message"))
        .or_else(|| error_value.and_then(all_debrid_value_message))
        .or_else(|| all_debrid_value_key_string(parsed, "message"))
        .unwrap_or_else(|| redacted_body_with_secret(body, token));
    (
        provider_code.map(|code| redacted_body_with_secret(&code, token)),
        redacted_body_with_secret(&detail, token),
    )
}

fn all_debrid_value_key_string(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(all_debrid_value_message)
}

fn all_debrid_value_message(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => non_empty(value).map(str::to_string),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        Value::Object(object) => object
            .get("message")
            .or_else(|| object.get("detail"))
            .or_else(|| object.get("error"))
            .and_then(all_debrid_value_message)
            .or_else(|| Some(value.to_string())),
        Value::Array(_) => Some(value.to_string()),
        Value::Null => None,
    }
}

fn all_debrid_error_kind(
    status: StatusCode,
    provider_code: Option<&str>,
    message: &str,
) -> DebridProviderErrorKind {
    let code = provider_code
        .unwrap_or_default()
        .trim()
        .to_ascii_uppercase();
    let lower = message.to_ascii_lowercase();
    if matches!(status.as_u16(), 401 | 403)
        || code.starts_with("AUTH_")
        || lower.contains("auth")
        || lower.contains("apikey")
        || lower.contains("api key")
        || lower.contains("unauthorized")
        || lower.contains("forbidden")
        || lower.contains("banned")
        || lower.contains("blocked")
    {
        DebridProviderErrorKind::Unauthorized
    } else if status == StatusCode::TOO_MANY_REQUESTS
        || code.contains("RATE")
        || code.contains("LIMIT")
        || code.contains("TOO_MANY")
        || lower.contains("rate limit")
        || lower.contains("ratelimit")
        || lower.contains("too many requests")
        || lower.contains("too many active")
        || lower.contains("capacity")
    {
        DebridProviderErrorKind::RateLimited
    } else if status == StatusCode::NOT_FOUND
        || code == "404"
        || code.contains("NOT_FOUND")
        || lower.contains("not found")
    {
        DebridProviderErrorKind::NotFound
    } else if status.is_server_error()
        || lower.contains("temporar")
        || lower.contains("unavailable")
        || lower.contains("timeout")
    {
        DebridProviderErrorKind::Temporary
    } else if status == StatusCode::BAD_REQUEST
        || code.contains("MAGNET")
        || code.contains("LINK")
        || lower.contains("invalid")
        || lower.contains("unsupported")
        || lower.contains("dead")
    {
        DebridProviderErrorKind::Permanent
    } else {
        DebridProviderErrorKind::Unknown
    }
}

fn all_debrid_error_to_anyhow(error: DebridProviderError) -> anyhow::Error {
    anyhow!(
        "AllDebrid API {}{}: {}",
        all_debrid_error_kind_label(error.kind),
        error
            .provider_code
            .as_ref()
            .map(|code| format!(" ({code})"))
            .unwrap_or_default(),
        error.message
    )
}

fn all_debrid_error_kind_label(kind: DebridProviderErrorKind) -> &'static str {
    match kind {
        DebridProviderErrorKind::Unauthorized => "auth error",
        DebridProviderErrorKind::NotFound => "not found",
        DebridProviderErrorKind::RateLimited => "rate limit",
        DebridProviderErrorKind::SelectionUnsupported => "selection unsupported",
        DebridProviderErrorKind::Temporary => "temporary error",
        DebridProviderErrorKind::Permanent => "request rejected",
        DebridProviderErrorKind::Unknown => "error",
    }
}

fn all_debrid_user_string(user: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| user.get(*key).and_then(all_debrid_value_message))
}

fn all_debrid_uploaded_magnet_to_release(
    uploaded: &AllDebridUploadedMagnet,
) -> Result<DebridRemoteRelease> {
    let remote_release_id = all_debrid_id_string(&uploaded.id)
        .ok_or_else(|| anyhow!("AllDebrid did not return a magnet id"))?;
    Ok(DebridRemoteRelease {
        provider_implementation: DebridServiceKind::AllDebrid.implementation_id().to_string(),
        remote_release_id,
        display_name: uploaded.name.clone().or_else(|| uploaded.hash.clone()),
        status: if uploaded.ready == Some(true) {
            DebridReleaseStatus::Downloaded
        } else {
            DebridReleaseStatus::Staging
        },
        raw_status: Some(
            if uploaded.ready == Some(true) {
                "submitted_ready"
            } else {
                "submitted_pending"
            }
            .to_string(),
        ),
        raw: Some(serde_json::to_value(uploaded)?),
    })
}

fn all_debrid_status_for_id(
    magnets: Vec<AllDebridMagnetStatus>,
    remote_release_id: &str,
) -> Result<AllDebridMagnetStatus> {
    magnets
        .into_iter()
        .find(|magnet| {
            all_debrid_id_string(&magnet.id)
                .map(|id| id == remote_release_id)
                .unwrap_or(false)
        })
        .ok_or_else(|| {
            all_debrid_error_to_anyhow(DebridProviderError {
                kind: DebridProviderErrorKind::NotFound,
                provider_code: Some(StatusCode::NOT_FOUND.as_u16().to_string()),
                message: format!("AllDebrid magnet '{remote_release_id}' was not found"),
            })
        })
}

fn all_debrid_files_for_id(
    magnets: Vec<AllDebridFilesMagnet>,
    remote_release_id: &str,
) -> Result<AllDebridFilesMagnet> {
    let magnet = magnets
        .into_iter()
        .find(|magnet| {
            all_debrid_id_string(&magnet.id)
                .map(|id| id == remote_release_id)
                .unwrap_or(false)
        })
        .ok_or_else(|| {
            all_debrid_error_to_anyhow(DebridProviderError {
                kind: DebridProviderErrorKind::NotFound,
                provider_code: Some(StatusCode::NOT_FOUND.as_u16().to_string()),
                message: format!("AllDebrid magnet '{remote_release_id}' files were not found"),
            })
        })?;
    if let Some(error) = magnet.error.as_ref() {
        return Err(all_debrid_error_value_to_anyhow(
            error,
            StatusCode::BAD_REQUEST,
            "",
        ));
    }
    Ok(magnet)
}

fn all_debrid_status_to_inspection(
    status: AllDebridMagnetStatus,
    mut files: Vec<DebridRemoteFile>,
    links: Vec<DebridResolvedLink>,
    selected_file_ids: Option<&[String]>,
) -> Result<DebridReleaseInspection> {
    let selected_file_ids = selected_file_ids
        .map(|ids| ids.iter().cloned().collect::<HashSet<_>>())
        .unwrap_or_default();
    if !selected_file_ids.is_empty() {
        for file in &mut files {
            file.selected = Some(selected_file_ids.contains(&file.provider_file_id));
        }
    }
    let mut skipped_file_ids = files
        .iter()
        .filter(|file| !selected_file_ids.contains(&file.provider_file_id))
        .map(|file| file.provider_file_id.clone())
        .collect::<Vec<_>>();
    skipped_file_ids.sort();
    let mut selected_file_ids = selected_file_ids.into_iter().collect::<Vec<_>>();
    selected_file_ids.sort();
    let progress = all_debrid_status_to_progress(&status);
    let provider_status = all_debrid_magnet_provider_status(&status, progress.status);
    let raw = json!({
        "magnet": status,
        "providerStatus": provider_status,
    });
    Ok(DebridReleaseInspection {
        release: DebridRemoteRelease {
            provider_implementation: DebridServiceKind::AllDebrid.implementation_id().to_string(),
            remote_release_id: all_debrid_id_string(&status.id)
                .unwrap_or_else(|| "unknown-alldebrid-magnet".to_string()),
            display_name: status.filename.clone(),
            status: progress.status,
            raw_status: status.status.clone(),
            raw: Some(raw.clone()),
        },
        capabilities: all_debrid_lifecycle_capabilities(),
        files,
        links,
        progress: Some(progress),
        selection: Some(DebridFileSelection {
            mode: DebridFileSelectionMode::AfterTransfer,
            selected_file_ids,
            skipped_file_ids,
        }),
        raw: Some(raw),
    })
}

fn all_debrid_status_to_progress(status: &AllDebridMagnetStatus) -> DebridReleaseProgress {
    let release_status = all_debrid_status_to_release_status(status);
    let progress = match release_status {
        DebridReleaseStatus::Downloaded => Some(1.0),
        _ => progress_fraction(status.downloaded, status.size),
    };
    DebridReleaseProgress {
        status: release_status,
        progress,
        downloaded_bytes: status.downloaded.or_else(|| {
            progress.map(|progress| (progress * status.size.unwrap_or(0) as f64) as u64)
        }),
        total_bytes: status.size,
        download_rate_bps: status.download_speed,
        raw: serde_json::to_value(status).ok(),
    }
}

fn all_debrid_status_to_release_status(status: &AllDebridMagnetStatus) -> DebridReleaseStatus {
    match status.status_code {
        Some(4) => DebridReleaseStatus::Downloaded,
        Some(5..=15) => DebridReleaseStatus::Failed,
        Some(0) | Some(1) | Some(2) | Some(3) => DebridReleaseStatus::Transferring,
        _ => match status
            .status
            .as_deref()
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "ready" | "finished" | "downloaded" => DebridReleaseStatus::Downloaded,
            value if value.contains("error") || value.contains("fail") => {
                DebridReleaseStatus::Failed
            }
            value if value.contains("download") || value.contains("process") => {
                DebridReleaseStatus::Transferring
            }
            _ => DebridReleaseStatus::Staging,
        },
    }
}

fn all_debrid_magnet_provider_status(
    status: &AllDebridMagnetStatus,
    release_status: DebridReleaseStatus,
) -> Value {
    let status_code = status.status_code;
    let raw_status = status
        .status
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let provider_failure_class = all_debrid_status_failure_class(status_code, raw_status);
    let no_seeds = provider_failure_class == Some(DebridFailureClass::NoSeeds);
    let file_list_unavailable =
        status.files.is_empty() && release_status != DebridReleaseStatus::Downloaded;
    let not_cached = matches!(release_status, DebridReleaseStatus::Failed)
        && (no_seeds || file_list_unavailable || matches!(status_code, Some(7 | 10 | 15)));
    let message =
        all_debrid_magnet_user_message(status_code, raw_status, provider_failure_class, not_cached);
    json!({
        "providerImplementation": DebridServiceKind::AllDebrid.implementation_id(),
        "providerName": DebridServiceKind::AllDebrid.display_name(),
        "status": release_status.as_str(),
        "providerState": raw_status,
        "rawStatus": raw_status,
        "providerStatusCode": status_code,
        "providerFailureClass": provider_failure_class.map(DebridFailureClass::as_str),
        "retryable": provider_failure_class
            .map(|failure_class| failure_class.response_policy() == DebridFailureResponsePolicy::TryAlternateRouteOrCandidate)
            .unwrap_or(false),
        "cached": if not_cached { Some(false) } else { None },
        "notCached": not_cached,
        "fileCount": status.files.len(),
        "fileListUnavailable": file_list_unavailable,
        "providerStalled": provider_failure_class == Some(DebridFailureClass::ProviderStalled),
        "noSeeds": no_seeds,
        "progress": all_debrid_status_to_progress(status).progress,
        "downloadedBytes": status.downloaded,
        "totalBytes": status.size,
        "downloadRateBps": status.download_speed,
        "seeders": status.seeders,
        "message": message,
    })
}

fn all_debrid_status_failure_class(
    status_code: Option<i64>,
    raw_status: Option<&str>,
) -> Option<DebridFailureClass> {
    match status_code {
        Some(5 | 12 | 13) => Some(DebridFailureClass::TransferFailed),
        Some(6 | 9) => Some(DebridFailureClass::ProviderUnavailable),
        Some(7 | 10) => Some(DebridFailureClass::StagingTimeout),
        Some(8) => Some(DebridFailureClass::InvalidSource),
        Some(11) => Some(DebridFailureClass::NotFoundExpired),
        Some(14) => Some(DebridFailureClass::ProviderStalled),
        Some(15) => Some(DebridFailureClass::NoSeeds),
        _ => {
            let raw_status = raw_status.unwrap_or_default().to_ascii_lowercase();
            if raw_status.contains("no peer") || raw_status.contains("no seed") {
                Some(DebridFailureClass::NoSeeds)
            } else if raw_status.contains("not downloaded") || raw_status.contains("took more") {
                Some(DebridFailureClass::StagingTimeout)
            } else if raw_status.contains("tracker") {
                Some(DebridFailureClass::ProviderStalled)
            } else if raw_status.contains("deleted") || raw_status.contains("removed") {
                Some(DebridFailureClass::NotFoundExpired)
            } else if raw_status.contains("too big") {
                Some(DebridFailureClass::InvalidSource)
            } else if raw_status.contains("fail") || raw_status.contains("error") {
                Some(DebridFailureClass::TransferFailed)
            } else {
                None
            }
        }
    }
}

fn all_debrid_magnet_user_message(
    status_code: Option<i64>,
    raw_status: Option<&str>,
    failure_class: Option<DebridFailureClass>,
    not_cached: bool,
) -> Option<String> {
    match failure_class {
        Some(DebridFailureClass::NoSeeds) if not_cached => {
            Some("AllDebrid accepted this magnet, but it is not cached and has no peers.".to_string())
        }
        Some(DebridFailureClass::StagingTimeout) if not_cached => {
            Some("AllDebrid accepted this magnet, but provider staging timed out before it became cached.".to_string())
        }
        Some(DebridFailureClass::ProviderStalled) if not_cached => {
            Some("AllDebrid accepted this magnet, but the provider transfer is stalled.".to_string())
        }
        Some(failure_class) => Some(format!(
            "AllDebrid reported {}{}{}.",
            failure_class.as_str(),
            status_code
                .map(|code| format!(" for statusCode {code}"))
                .unwrap_or_default(),
            raw_status
                .map(|status| format!(" ({status})"))
                .unwrap_or_default()
        )),
        None => None,
    }
}

fn all_debrid_flatten_file_nodes(nodes: &[Value]) -> Result<Vec<DebridRemoteFile>> {
    let mut files = Vec::new();
    let mut path = Vec::new();
    for node in nodes {
        all_debrid_flatten_file_node(node, &mut path, &mut files)?;
    }
    Ok(files)
}

fn all_debrid_flatten_file_node(
    node: &Value,
    path: &mut Vec<String>,
    files: &mut Vec<DebridRemoteFile>,
) -> Result<()> {
    let name = node
        .get("n")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("unnamed");
    if let Some(children) = node.get("e").and_then(Value::as_array) {
        path.push(name.to_string());
        for child in children {
            all_debrid_flatten_file_node(child, path, files)?;
        }
        path.pop();
        return Ok(());
    }

    let mut full_path = path.clone();
    full_path.push(name.to_string());
    let full_path = full_path.join("/");
    let provider_file_id = format!("{}", files.len().saturating_add(1));
    let link = node
        .get("l")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let selectable = link.is_some() && !is_debrid_non_selectable_path(&full_path);
    files.push(DebridRemoteFile {
        provider_file_id,
        file_index: Some(files.len() as i64),
        path: full_path,
        basename: name.to_string(),
        size_bytes: node.get("s").and_then(Value::as_u64),
        selectable,
        selected: None,
        raw: Some(node.clone()),
    });
    Ok(())
}

fn is_debrid_non_selectable_path(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    let basename = basename_from_remote_path(path).to_ascii_lowercase();
    basename.starts_with("sample")
        || basename.contains(".sample.")
        || lower.contains("/sample/")
        || lower.ends_with(".rar")
        || lower.ends_with(".r00")
        || lower.ends_with(".zip")
        || lower.ends_with(".7z")
}

fn all_debrid_file_link(file: &DebridRemoteFile) -> Option<String> {
    file.raw
        .as_ref()
        .and_then(|raw| raw.get("l"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn all_debrid_is_unlocked_download_url(link: &str, api_base_url: &str) -> bool {
    let Ok(url) = Url::parse(link.trim()) else {
        return false;
    };
    if url.scheme() != "http" && url.scheme() != "https" {
        return false;
    }
    #[cfg(not(test))]
    let _ = api_base_url;
    let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
    if host.ends_with(".debrid.it")
        || host == "debrid.it"
        || host.ends_with(".alldeb.ovh")
        || host == "alldeb.ovh"
    {
        return true;
    }

    #[cfg(test)]
    {
        if let Ok(api_base) = Url::parse(api_base_url) {
            if url.scheme() == api_base.scheme()
                && url.host_str() == api_base.host_str()
                && url.port_or_known_default() == api_base.port_or_known_default()
                && url.path().starts_with("/download/")
            {
                return true;
            }
        }
    }

    false
}

fn all_debrid_unlocked_link_to_resolved(
    unlocked: AllDebridUnlockedLink,
    provider_file_id: Option<&str>,
    fallback_filename: Option<&str>,
    fallback_size: Option<u64>,
) -> Result<DebridResolvedLink> {
    let url = unlocked
        .link
        .clone()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            if let Some(delayed) = unlocked.delayed.as_ref() {
                anyhow!("AllDebrid link unlock returned delayed id {delayed}")
            } else {
                anyhow!("AllDebrid link unlock did not return a downloadable link")
            }
        })?;
    Ok(DebridResolvedLink {
        provider_file_id: provider_file_id.map(str::to_string),
        url: url.clone(),
        filename: unlocked
            .filename
            .clone()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| fallback_filename.map(str::to_string))
            .or_else(|| filename_from_url_path(&url)),
        size_bytes: unlocked.filesize.or(fallback_size),
        raw: Some(serde_json::to_value(unlocked)?),
    })
}

fn all_debrid_error_value_to_anyhow(
    value: &Value,
    status: StatusCode,
    token: &str,
) -> anyhow::Error {
    let provider_code = all_debrid_value_key_string(value, "code");
    let message = all_debrid_value_key_string(value, "message")
        .or_else(|| all_debrid_value_message(value))
        .unwrap_or_else(|| value.to_string());
    all_debrid_error_to_anyhow(DebridProviderError {
        kind: all_debrid_error_kind(status, provider_code.as_deref(), &message),
        provider_code,
        message: redacted_body_with_secret(&message, token),
    })
}

fn all_debrid_id_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => non_empty(value).map(str::to_string),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn all_debrid_error_is_not_found(err: &anyhow::Error) -> bool {
    let message = err.to_string().to_ascii_lowercase();
    message.contains("not found")
        || message.contains("404")
        || message.contains("magnet_invalid_id")
        || message.contains("invalid id")
}

fn torbox_response_value(status: StatusCode, body: &str, token: &str) -> Result<Value> {
    let parsed = serde_json::from_str::<Value>(body).unwrap_or_else(|_| Value::String(body.into()));
    let envelope = serde_json::from_value::<TorBoxEnvelope>(parsed.clone()).ok();
    let success = envelope.as_ref().and_then(|envelope| envelope.success);
    if status.is_success() && success != Some(false) {
        if let Some(data) = envelope.and_then(|envelope| envelope.data) {
            return Ok(data);
        }
        return Ok(parsed);
    }

    let message = torbox_error_message(body, envelope.as_ref(), token);
    let error = DebridProviderError {
        kind: torbox_error_kind(status, &message),
        provider_code: Some(status.as_u16().to_string()),
        message,
    };
    Err(torbox_error_to_anyhow(error))
}

fn torbox_error_message(body: &str, envelope: Option<&TorBoxEnvelope>, token: &str) -> String {
    let detail = envelope
        .and_then(|envelope| envelope.detail.as_ref())
        .and_then(torbox_value_message)
        .or_else(|| {
            envelope
                .and_then(|envelope| envelope.error.as_ref())
                .and_then(torbox_value_message)
        });
    redacted_body_with_secret(
        &detail.unwrap_or_else(|| redacted_body_with_secret(body, token)),
        token,
    )
}

fn torbox_value_message(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Object(object) => object
            .get("message")
            .or_else(|| object.get("detail"))
            .or_else(|| object.get("error"))
            .and_then(torbox_value_message)
            .or_else(|| Some(value.to_string())),
        Value::Null => None,
        _ => Some(value.to_string()),
    }
}

fn torbox_error_kind(status: StatusCode, message: &str) -> DebridProviderErrorKind {
    let lower = message.to_ascii_lowercase();
    if matches!(status.as_u16(), 401 | 403)
        || lower.contains("unauthorized")
        || lower.contains("forbidden")
        || lower.contains("invalid api")
        || lower.contains("api token")
        || lower.contains("auth")
    {
        DebridProviderErrorKind::Unauthorized
    } else if status == StatusCode::TOO_MANY_REQUESTS
        || lower.contains("rate limit")
        || lower.contains("ratelimit")
        || lower.contains("too many requests")
    {
        DebridProviderErrorKind::RateLimited
    } else if status == StatusCode::NOT_FOUND {
        DebridProviderErrorKind::NotFound
    } else if status.is_server_error()
        || lower.contains("temporar")
        || lower.contains("unavailable")
        || lower.contains("timeout")
    {
        DebridProviderErrorKind::Temporary
    } else if status == StatusCode::BAD_REQUEST || lower.contains("invalid") {
        DebridProviderErrorKind::Permanent
    } else {
        DebridProviderErrorKind::Unknown
    }
}

fn torbox_error_to_anyhow(error: DebridProviderError) -> anyhow::Error {
    anyhow!(
        "TorBox API {}{}: {}",
        torbox_error_kind_label(error.kind),
        error
            .provider_code
            .as_ref()
            .map(|code| format!(" ({code})"))
            .unwrap_or_default(),
        error.message
    )
}

fn torbox_error_kind_label(kind: DebridProviderErrorKind) -> &'static str {
    match kind {
        DebridProviderErrorKind::Unauthorized => "auth error",
        DebridProviderErrorKind::NotFound => "not found",
        DebridProviderErrorKind::RateLimited => "rate limit",
        DebridProviderErrorKind::SelectionUnsupported => "selection unsupported",
        DebridProviderErrorKind::Temporary => "temporary error",
        DebridProviderErrorKind::Permanent => "request rejected",
        DebridProviderErrorKind::Unknown => "error",
    }
}

fn redacted_body_with_secret(body: &str, secret: &str) -> String {
    let body = redacted_body(body);
    let secret = secret.trim();
    if secret.is_empty() {
        body
    } else {
        body.replace(secret, "[redacted]")
    }
}

fn torbox_user_string(user: &Value, key: &str) -> Option<String> {
    let value = user.get(key)?;
    match value {
        Value::String(value) => non_empty(value).map(str::to_string),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn torbox_cache_entry_for_hash(value: &Value, hash: &str) -> Option<Value> {
    let normalized_hash = hash.trim().to_ascii_lowercase();
    match value {
        Value::Object(map) => map
            .iter()
            .find(|(key, _)| key.trim().eq_ignore_ascii_case(&normalized_hash))
            .map(|(_, value)| value.clone())
            .or_else(|| {
                map.values()
                    .find(|entry| {
                        entry
                            .get("hash")
                            .and_then(Value::as_str)
                            .map(|value| value.trim().eq_ignore_ascii_case(&normalized_hash))
                            .unwrap_or(false)
                    })
                    .cloned()
            }),
        Value::Array(entries) => entries
            .iter()
            .find(|entry| {
                entry
                    .get("hash")
                    .and_then(Value::as_str)
                    .map(|value| value.trim().eq_ignore_ascii_case(&normalized_hash))
                    .unwrap_or(false)
            })
            .cloned(),
        _ => None,
    }
}

fn torbox_torrent_from_mylist_value(
    value: &Value,
    remote_release_id: &str,
) -> Result<TorBoxTorrent> {
    let selected = match value {
        Value::Array(items) => items
            .iter()
            .find(|item| {
                item.get("id")
                    .and_then(torbox_id_string)
                    .map(|id| id == remote_release_id)
                    .unwrap_or(false)
            })
            .or_else(|| items.first())
            .cloned(),
        Value::Object(_) => Some(value.clone()),
        _ => None,
    }
    .ok_or_else(|| {
        torbox_error_to_anyhow(DebridProviderError {
            kind: DebridProviderErrorKind::NotFound,
            provider_code: Some(StatusCode::NOT_FOUND.as_u16().to_string()),
            message: format!("TorBox torrent '{remote_release_id}' was not found"),
        })
    })?;
    serde_json::from_value(selected)
        .with_context(|| format!("parsing TorBox torrent '{remote_release_id}'"))
}

fn torbox_torrent_to_inspection(
    torrent: &TorBoxTorrent,
    links: Vec<DebridResolvedLink>,
    selected_file_ids: Option<&[String]>,
) -> Result<DebridReleaseInspection> {
    let mut files = torbox_torrent_files(torrent)?;
    let selected_file_ids = selected_file_ids
        .map(|ids| ids.iter().cloned().collect::<HashSet<_>>())
        .unwrap_or_default();
    if !selected_file_ids.is_empty() {
        for file in &mut files {
            file.selected = Some(selected_file_ids.contains(&file.provider_file_id));
        }
    }
    let mut skipped_file_ids = files
        .iter()
        .filter(|file| !selected_file_ids.contains(&file.provider_file_id))
        .map(|file| file.provider_file_id.clone())
        .collect::<Vec<_>>();
    skipped_file_ids.sort();
    let mut selected_file_ids = selected_file_ids.into_iter().collect::<Vec<_>>();
    selected_file_ids.sort();
    let status = torbox_torrent_status(torrent);
    let provider_status = torbox_torrent_provider_status(torrent, status);
    let raw = json!({
        "torrent": torrent,
        "providerStatus": provider_status,
    });
    Ok(DebridReleaseInspection {
        release: DebridRemoteRelease {
            provider_implementation: DebridServiceKind::TorBox.implementation_id().to_string(),
            remote_release_id: torbox_id_string(&torrent.id)
                .unwrap_or_else(|| "unknown-torbox-torrent".to_string()),
            display_name: torrent.name.clone().or_else(|| torrent.hash.clone()),
            status,
            raw_status: torrent.download_state.clone(),
            raw: Some(raw.clone()),
        },
        capabilities: torbox_lifecycle_capabilities(),
        files,
        links,
        progress: Some(torbox_torrent_progress(torrent)),
        selection: Some(DebridFileSelection {
            mode: DebridFileSelectionMode::AfterTransfer,
            selected_file_ids,
            skipped_file_ids,
        }),
        raw: Some(raw),
    })
}

fn torbox_torrent_files(torrent: &TorBoxTorrent) -> Result<Vec<DebridRemoteFile>> {
    torrent
        .files
        .iter()
        .enumerate()
        .map(|(index, file)| {
            let provider_file_id =
                torbox_id_string(&file.id).unwrap_or_else(|| (index.saturating_add(1)).to_string());
            let path = file
                .absolute_path
                .clone()
                .or_else(|| file.name.clone())
                .or_else(|| file.short_name.clone())
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| format!("torbox-file-{provider_file_id}"));
            let selectable = file.infected != Some(true) && file.zipped != Some(true);
            Ok(DebridRemoteFile {
                provider_file_id,
                file_index: Some(index as i64),
                basename: file
                    .short_name
                    .clone()
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or_else(|| basename_from_remote_path(&path)),
                path,
                size_bytes: file.size,
                selectable,
                selected: None,
                raw: Some(serde_json::to_value(file)?),
            })
        })
        .collect()
}

fn torbox_torrent_progress(torrent: &TorBoxTorrent) -> DebridReleaseProgress {
    let total = torrent.size;
    let progress = torbox_progress_fraction(torrent.progress, torrent.total_downloaded, total);
    let status = torbox_torrent_status(torrent);
    DebridReleaseProgress {
        status,
        progress,
        downloaded_bytes: torrent
            .total_downloaded
            .or_else(|| progress.map(|progress| (progress * total.unwrap_or(0) as f64) as u64)),
        total_bytes: total,
        download_rate_bps: torrent.download_speed,
        raw: Some(json!({
            "torrent": torrent,
            "providerStatus": torbox_torrent_provider_status(torrent, status),
        })),
    }
}

fn torbox_torrent_status(torrent: &TorBoxTorrent) -> DebridReleaseStatus {
    if torrent.cached == Some(true)
        || torrent.download_finished == Some(true)
        || torrent
            .download_state
            .as_deref()
            .map(|status| status.eq_ignore_ascii_case("cached"))
            .unwrap_or(false)
    {
        return DebridReleaseStatus::Downloaded;
    }
    match torrent
        .download_state
        .as_deref()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "downloading" | "uploading" | "completed" => DebridReleaseStatus::Transferring,
        "metadl" | "checkingresumedata" | "queued" | "paused" => DebridReleaseStatus::Staging,
        "stalled (no seeds)" | "error" | "failed" | "missing_files" | "virus" => {
            DebridReleaseStatus::Failed
        }
        "cancelled" | "canceled" => DebridReleaseStatus::Cancelled,
        _ => DebridReleaseStatus::Staging,
    }
}

fn torbox_torrent_provider_status(torrent: &TorBoxTorrent, status: DebridReleaseStatus) -> Value {
    let raw_status = torrent
        .download_state
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let raw_status_lower = raw_status.unwrap_or_default().to_ascii_lowercase();
    let no_seeds = raw_status_lower.contains("no seeds");
    let provider_stalled = raw_status_lower.contains("stalled");
    let not_cached = torrent.cached == Some(false);
    let file_list_unavailable =
        torrent.files.is_empty() && status != DebridReleaseStatus::Downloaded;
    let provider_failure_class = if no_seeds {
        Some("no_seeds")
    } else if provider_stalled {
        Some("provider_stalled")
    } else if file_list_unavailable {
        Some("file_list_unavailable")
    } else {
        None
    };
    let message = torbox_torrent_user_message(
        no_seeds,
        provider_stalled,
        not_cached,
        file_list_unavailable,
    );
    json!({
        "providerImplementation": DebridServiceKind::TorBox.implementation_id(),
        "providerName": DebridServiceKind::TorBox.display_name(),
        "status": status.as_str(),
        "providerState": raw_status,
        "rawStatus": raw_status,
        "providerFailureClass": provider_failure_class,
        "retryable": matches!(provider_failure_class, Some("no_seeds" | "provider_stalled" | "file_list_unavailable")),
        "cached": torrent.cached,
        "notCached": not_cached,
        "downloadPresent": torrent.download_present,
        "downloadFinished": torrent.download_finished,
        "filesAvailable": !torrent.files.is_empty(),
        "fileCount": torrent.files.len(),
        "fileListUnavailable": file_list_unavailable,
        "providerStalled": provider_stalled,
        "noSeeds": no_seeds,
        "progress": torbox_progress_fraction(torrent.progress, torrent.total_downloaded, torrent.size),
        "downloadedBytes": torrent.total_downloaded,
        "totalBytes": torrent.size,
        "downloadRateBps": torrent.download_speed,
        "message": message,
    })
}

fn torbox_torrent_user_message(
    no_seeds: bool,
    provider_stalled: bool,
    not_cached: bool,
    file_list_unavailable: bool,
) -> Option<&'static str> {
    if no_seeds && not_cached {
        Some("TorBox accepted this torrent, but it is not cached and has no seeds.")
    } else if no_seeds {
        Some("TorBox accepted this torrent, but the transfer has no seeds.")
    } else if provider_stalled && not_cached {
        Some(
            "TorBox accepted this torrent, but it is not cached and the provider transfer is stalled.",
        )
    } else if provider_stalled {
        Some("TorBox accepted this torrent, but the provider transfer is stalled.")
    } else if file_list_unavailable && not_cached {
        Some(
            "TorBox accepted this torrent, but it is not cached and no file list is available yet.",
        )
    } else {
        None
    }
}

fn torbox_progress_fraction(
    progress: Option<f64>,
    downloaded_bytes: Option<u64>,
    total_bytes: Option<u64>,
) -> Option<f64> {
    if let Some(progress) = progress {
        return Some(if progress > 1.0 {
            (progress / 100.0).clamp(0.0, 1.0)
        } else {
            progress.clamp(0.0, 1.0)
        });
    }
    progress_fraction(downloaded_bytes, total_bytes)
}

fn torbox_id_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => non_empty(value).map(str::to_string),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn torbox_torrent_id_json_value(value: &str) -> Result<Value> {
    let value = value.trim();
    if value.is_empty() {
        bail!(
            "{} torrent id is required",
            DebridServiceKind::TorBox.display_name()
        );
    }
    Ok(value
        .parse::<i64>()
        .map(Value::from)
        .unwrap_or_else(|_| Value::String(value.to_string())))
}

fn torbox_error_is_not_found(err: &anyhow::Error) -> bool {
    let message = err.to_string().to_ascii_lowercase();
    message.contains("not found") || message.contains("404")
}

struct TorBoxInternalDownloadRef {
    torrent_id: String,
    file_id: Option<String>,
    filename: Option<String>,
    size_bytes: Option<u64>,
}

fn torbox_internal_download_url(
    torrent_id: &str,
    file: Option<&DebridRemoteFile>,
) -> Result<String> {
    let mut url = Url::parse("elixir-debrid://torbox/download")
        .context("building internal TorBox download reference")?;
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("torrent_id", torrent_id.trim());
        if let Some(file) = file {
            query.append_pair("file_id", file.provider_file_id.as_str());
            query.append_pair("filename", file.basename.as_str());
            if let Some(size) = file.size_bytes {
                query.append_pair("size", &size.to_string());
            }
        } else {
            query.append_pair("zip_link", "true");
        }
    }
    Ok(url.to_string())
}

fn torbox_internal_download_ref(link: &str) -> Result<Option<TorBoxInternalDownloadRef>> {
    let Ok(url) = Url::parse(link.trim()) else {
        return Ok(None);
    };
    if url.scheme() != "elixir-debrid"
        || url.host_str() != Some("torbox")
        || url.path() != "/download"
    {
        return Ok(None);
    }
    let mut torrent_id = None::<String>;
    let mut file_id = None::<String>;
    let mut filename = None::<String>;
    let mut size_bytes = None::<u64>;
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "torrent_id" => torrent_id = non_empty(value.as_ref()).map(str::to_string),
            "file_id" => file_id = non_empty(value.as_ref()).map(str::to_string),
            "filename" => filename = non_empty(value.as_ref()).map(str::to_string),
            "size" => {
                if let Some(value) = non_empty(value.as_ref()) {
                    size_bytes = Some(value.parse::<u64>().with_context(|| {
                        format!("parsing internal TorBox download size '{value}'")
                    })?);
                }
            }
            _ => {}
        }
    }
    let torrent_id =
        torrent_id.context("internal TorBox download reference is missing torrent_id")?;
    Ok(Some(TorBoxInternalDownloadRef {
        torrent_id,
        file_id,
        filename,
        size_bytes,
    }))
}

fn torbox_create_torrent_limiter_key(base_url: &str, token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(base_url.trim().as_bytes());
    hasher.update([0]);
    hasher.update(token.trim().as_bytes());
    format!("{:x}", hasher.finalize())
}

fn torbox_rate_limit_error(retry_after: Duration) -> anyhow::Error {
    anyhow!(
        "TorBox API rate limit: createtorrent request deferred for {} seconds",
        retry_after.as_secs().max(1)
    )
}

pub async fn ensure_debrid_builtin(state: &AppState) -> Result<()> {
    let store = ExtensionStore::new(&state.db_pool);
    ensure_debrid_builtin_records(&state.db_pool, &store).await
}

async fn ensure_debrid_builtin_records(
    pool: &sqlx::AnyPool,
    store: &ExtensionStore<'_>,
) -> Result<()> {
    let existing = store.get_extension(DEBRID_EXTENSION_ID).await?;
    let legacy = store.get_extension(LEGACY_REAL_DEBRID_EXTENSION_ID).await?;
    let enabled = existing
        .as_ref()
        .map(|item| item.enabled)
        .or_else(|| legacy.as_ref().map(|item| item.enabled))
        .unwrap_or(true);
    store
        .upsert_extension(&NewExtension {
            extension_id: DEBRID_EXTENSION_ID.to_string(),
            name: "Debrid".to_string(),
            version: "0.1.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: Some("Elixir".to_string()),
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Verified,
            manifest_json: debrid_manifest_json(),
            package_hash: None,
            enabled,
        })
        .await?;

    migrate_legacy_real_debrid_extension(pool).await?;

    let mut instances = store.list_instances(Some(DEBRID_EXTENSION_ID)).await?;
    if instances.is_empty() {
        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: DEBRID_EXTENSION_ID.to_string(),
                instance_name: "default".to_string(),
                config_json: Some(default_debrid_instance_config()),
                enabled: true,
            })
            .await?;
        instances = store.list_instances(Some(DEBRID_EXTENSION_ID)).await?;
    }

    ensure_debrid_instance_config_defaults(store, &instances).await?;
    instances = store.list_instances(Some(DEBRID_EXTENSION_ID)).await?;

    let Some(instance) = instances
        .into_iter()
        .filter(|instance| instance.enabled)
        .min_by_key(|instance| {
            (
                !instance.instance_name.eq_ignore_ascii_case("default"),
                instance.instance_name.clone(),
            )
        })
    else {
        return Ok(());
    };
    reconcile_debrid_provider_for_instance(pool, store, instance.instance_id).await?;
    Ok(())
}

pub async fn reconcile_debrid_provider_for_instance(
    pool: &sqlx::AnyPool,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
) -> Result<Uuid> {
    let service = active_debrid_service_for_instance(store, instance_id).await?;
    let concurrent_downloads = debrid_concurrent_downloads_for_instance(pool, instance_id).await?;
    let provider_id = store
        .list_providers(Some(instance_id))
        .await?
        .into_iter()
        .find(|provider| provider.capability == "debrid.resolver" && provider.slot_id == "default")
        .map(|provider| provider.provider_id)
        .unwrap_or_else(|| stable_provider_id(instance_id, "debrid.resolver", "default"));
    disable_non_active_debrid_resolver_providers(pool, instance_id).await?;
    let endpoint = debrid_service_endpoint(service)?;
    let has_token = debrid_secret_exists_for_instance(store, instance_id, service).await?;
    store
        .upsert_provider(&NewProvider {
            provider_id,
            instance_id,
            capability: "debrid.resolver".to_string(),
            slot_id: "default".to_string(),
            cardinality: SlotCardinality::One,
            implementation: Some(service.implementation_id().to_string()),
            scope_json: Some(json!({
                "download_broker": {
                    "enabled": true,
                    "provider_kind": "debrid",
                    "logical_id": DEBRID_DEFAULT_LOGICAL_ID,
                    "activeService": service.implementation_id(),
                    "maxConcurrentDownloads": concurrent_downloads
                }
            })),
            endpoint_json: Some(serde_json::to_value(endpoint)?),
            health_state: if has_token {
                ProviderHealthState::Healthy
            } else {
                ProviderHealthState::Unknown
            },
        })
        .await?;
    let readiness_detail = if has_token {
        format!("{} account token is present.", service.display_name())
    } else {
        format!(
            "Add a {} account to enable debrid acquisition.",
            service.display_name()
        )
    };
    store
        .upsert_provider_readiness(
            provider_id,
            if has_token {
                ProviderReadinessPhase::DriverReady
            } else {
                ProviderReadinessPhase::Unknown
            },
            Some(&readiness_detail),
        )
        .await?;
    Ok(provider_id)
}

fn debrid_service_endpoint(service: DebridServiceKind) -> Result<ProviderEndpoint> {
    let url = Url::parse(service.api_base_url())
        .with_context(|| format!("parsing {} API base URL", service.display_name()))?;
    let scheme = url.scheme().to_string();
    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("{} API base URL is missing a host", service.display_name()))?
        .to_string();
    let port = url
        .port_or_known_default()
        .ok_or_else(|| anyhow!("{} API base URL is missing a port", service.display_name()))?;
    let base_path = if url.path().trim().is_empty() {
        "/".to_string()
    } else {
        url.path().to_string()
    };
    ProviderEndpoint::new(scheme, host, port, Some(base_path), None)
}

async fn migrate_legacy_real_debrid_extension(pool: &sqlx::AnyPool) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE extension_instances
         SET extension_id = $1, updated_at = CURRENT_TIMESTAMP
         WHERE extension_id = $2",
    )
    .bind(DEBRID_EXTENSION_ID)
    .bind(LEGACY_REAL_DEBRID_EXTENSION_ID)
    .execute(pool)
    .await
    .context("migrating legacy Real-Debrid instances to canonical Debrid extension id")?;

    sqlx::query::<sqlx::Any>("DELETE FROM extensions WHERE extension_id = $1")
        .bind(LEGACY_REAL_DEBRID_EXTENSION_ID)
        .execute(pool)
        .await
        .context("removing legacy Real-Debrid extension row after Debrid migration")?;
    Ok(())
}

async fn disable_non_active_debrid_resolver_providers(
    pool: &sqlx::AnyPool,
    active_instance_id: Uuid,
) -> Result<()> {
    let disabled_scope = serde_json::to_string(&json!({
        "download_broker": {
            "enabled": false,
            "provider_kind": "debrid",
            "logical_id": DEBRID_DEFAULT_LOGICAL_ID,
            "inactive": true
        }
    }))?;
    sqlx::query::<sqlx::Any>(
        "UPDATE providers
         SET scope_json = $1,
             health_state = $2,
             updated_at = CURRENT_TIMESTAMP
         WHERE capability = 'debrid.resolver'
           AND slot_id = 'default'
           AND instance_id <> $3",
    )
    .bind(disabled_scope)
    .bind(ProviderHealthState::Unknown.as_str())
    .bind(active_instance_id.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

async fn ensure_debrid_instance_config_defaults(
    store: &ExtensionStore<'_>,
    instances: &[crate::db::models::ExtensionInstance],
) -> Result<()> {
    for instance in instances {
        let normalized = normalized_debrid_instance_config(instance.config_json.clone());
        if instance.config_json.as_ref() != Some(&normalized) {
            store
                .update_instance_config(instance.instance_id, Some(&normalized))
                .await?;
        }
        migrate_debrid_instance_secret_defaults(store, instance.instance_id).await?;
    }
    Ok(())
}

fn normalized_debrid_instance_config(config_json: Option<Value>) -> Value {
    let mut config = match config_json {
        Some(Value::Object(map)) => map,
        _ => serde_json::Map::new(),
    };
    let active_service = config
        .get(DEBRID_ACTIVE_SERVICE_CONFIG_KEY)
        .or_else(|| config.get("active_service"))
        .and_then(Value::as_str)
        .and_then(|value| DebridServiceKind::from_str(value).ok())
        .unwrap_or(DebridServiceKind::RealDebrid)
        .implementation_id();
    config
        .entry("materialize".to_string())
        .or_insert_with(|| json!(true));
    let concurrent_downloads =
        debrid_concurrent_downloads_from_config(Some(&Value::Object(config.clone())));
    config.insert(
        DEBRID_CONCURRENT_DOWNLOADS_CONFIG_KEY.to_string(),
        json!(concurrent_downloads),
    );
    config.insert(
        DEBRID_ACTIVE_SERVICE_CONFIG_KEY.to_string(),
        json!(active_service),
    );
    config.entry("serviceOrder".to_string()).or_insert_with(|| {
        json!(
            DebridServiceKind::ALL
                .into_iter()
                .map(DebridServiceKind::implementation_id)
                .collect::<Vec<_>>()
        )
    });
    Value::Object(config)
}

fn default_debrid_instance_config() -> Value {
    normalized_debrid_instance_config(None)
}

async fn migrate_debrid_instance_secret_defaults(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
) -> Result<()> {
    if store
        .get_secret(
            SecretScope::Instance,
            Some(instance_id),
            DEBRID_REAL_DEBRID_TOKEN_SECRET_KEY,
        )
        .await?
        .is_some()
    {
        return Ok(());
    }

    let Some(legacy) = store
        .get_secret(
            SecretScope::Instance,
            Some(instance_id),
            REAL_DEBRID_TOKEN_SECRET_KEY,
        )
        .await?
    else {
        return Ok(());
    };

    store
        .upsert_secret(&NewSecret {
            secret_id: Uuid::new_v4(),
            scope: SecretScope::Instance,
            scope_id: Some(instance_id),
            key: DEBRID_REAL_DEBRID_TOKEN_SECRET_KEY.to_string(),
            value_encrypted: legacy.value_encrypted,
            rotatable: legacy.rotatable,
        })
        .await?;
    Ok(())
}

#[allow(dead_code)]
pub async fn ensure_real_debrid_builtin(state: &AppState) -> Result<()> {
    ensure_debrid_builtin(state).await
}

pub async fn start_debrid_materializer_loop(state: AppState) {
    let mut interval =
        tokio::time::interval(Duration::from_secs(REAL_DEBRID_POLL_INTERVAL_SECONDS));
    loop {
        interval.tick().await;
        if let Err(err) = process_debrid_jobs_once(&state).await {
            tracing::warn!("Debrid materializer pass failed: {err}");
        }
    }
}

pub async fn active_debrid_service_for_instance(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
) -> Result<DebridServiceKind> {
    let instance = store
        .get_instance(instance_id)
        .await?
        .ok_or_else(|| anyhow!("debrid extension instance '{instance_id}' does not exist"))?;
    active_debrid_service_from_config(instance.config_json.as_ref())
}

pub async fn debrid_concurrent_downloads_for_instance(
    pool: &sqlx::AnyPool,
    instance_id: Uuid,
) -> Result<i64> {
    let raw_config = sqlx::query_scalar::<sqlx::Any, Option<String>>(
        "SELECT CAST(config_json AS TEXT) AS config_json
         FROM extension_instances
         WHERE instance_id = $1
         LIMIT 1",
    )
    .bind(instance_id.to_string())
    .fetch_optional(pool)
    .await
    .context("loading Debrid instance concurrency cap")?
    .flatten();
    let parsed_config = raw_config
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok());
    Ok(debrid_concurrent_downloads_from_config(
        parsed_config.as_ref(),
    ))
}

pub async fn active_debrid_concurrent_downloads(pool: &sqlx::AnyPool) -> Result<i64> {
    let raw_config = sqlx::query_scalar::<sqlx::Any, Option<String>>(
        "SELECT CAST(config_json AS TEXT) AS config_json
         FROM extension_instances
         WHERE (extension_id = $1 OR extension_id = $2)
           AND enabled = $3
         ORDER BY CASE WHEN LOWER(instance_name) = 'default' THEN 0 ELSE 1 END,
                  instance_name ASC
         LIMIT 1",
    )
    .bind(DEBRID_EXTENSION_ID)
    .bind(LEGACY_REAL_DEBRID_EXTENSION_ID)
    .bind(true)
    .fetch_optional(pool)
    .await
    .context("loading active Debrid concurrency cap")?
    .flatten();
    let parsed_config = raw_config
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok());
    Ok(debrid_concurrent_downloads_from_config(
        parsed_config.as_ref(),
    ))
}

#[allow(dead_code)]
pub async fn debrid_token_for_instance(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    service: DebridServiceKind,
) -> Result<String> {
    debrid_token_for_instance_with_secrets(&state.secrets, store, instance_id, service).await
}

async fn debrid_token_for_instance_with_secrets(
    secrets: &SecretsManager,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    service: DebridServiceKind,
) -> Result<String> {
    for secret_key in service.secret_keys_for_read() {
        if let Some(secret) = store
            .get_secret(SecretScope::Instance, Some(instance_id), secret_key)
            .await?
        {
            return secrets
                .decrypt(&secret.value_encrypted)
                .with_context(|| format!("decrypting {} API token", service.display_name()));
        }
    }
    bail!("{} API token is not configured", service.display_name())
}

pub async fn debrid_secret_exists_for_instance(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    service: DebridServiceKind,
) -> Result<bool> {
    for secret_key in service.secret_keys_for_read() {
        if store
            .get_secret(SecretScope::Instance, Some(instance_id), secret_key)
            .await?
            .is_some()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub async fn real_debrid_token_for_instance(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
) -> Result<String> {
    debrid_token_for_instance(state, store, instance_id, DebridServiceKind::RealDebrid).await
}

pub async fn list_eligible_debrid_route_attempts(
    pool: &sqlx::AnyPool,
    store: &ExtensionStore<'_>,
    owner_id: Option<&str>,
) -> Result<Vec<DebridRouteAttempt>> {
    let owner_id = normalize_debrid_route_owner_id(owner_id);
    let routes = list_acquisition_routes(pool, store).await?;
    let Some(route) = routes
        .routes
        .into_iter()
        .find(|route| route.logical_id == DEBRID_DEFAULT_LOGICAL_ID && route.owner_id == owner_id)
    else {
        return Ok(Vec::new());
    };

    let inventory = list_logical_downloaders(store).await?;
    let records_by_id = inventory
        .downloaders
        .into_iter()
        .filter(|record| {
            record.role == DownloadBrokerRole::DebridResolver
                && record.logical_id == DEBRID_DEFAULT_LOGICAL_ID
                && record.provider_kind == DownloadBrokerProviderKind::Debrid
        })
        .map(|record| (record.provider_id, record))
        .collect::<HashMap<_, _>>();

    let mut provider_ids = Vec::new();
    if let Some(provider_id) = route.selected_provider_id {
        provider_ids.push(provider_id);
    }
    let mut fallback_provider_ids = route
        .candidates
        .iter()
        .filter(|candidate| candidate.provider_kind == DownloadBrokerProviderKind::Debrid)
        .filter_map(|candidate| {
            (Some(candidate.provider_id) != route.selected_provider_id)
                .then_some(candidate.provider_id)
        })
        .collect::<Vec<_>>();
    fallback_provider_ids.sort();
    fallback_provider_ids.dedup();
    provider_ids.extend(fallback_provider_ids);

    let mut attempts = Vec::new();
    let mut seen_attempts = HashSet::new();
    for provider_id in provider_ids {
        let Some(record) = records_by_id.get(&provider_id) else {
            continue;
        };
        if record.health_state == ProviderHealthState::Unhealthy {
            continue;
        }
        if !is_builtin_debrid_extension_id(&record.extension_id) {
            continue;
        }
        let Some(instance) = store.get_instance(record.instance_id).await? else {
            continue;
        };
        if !instance.enabled
            || instance.extension_id != record.extension_id
            || !is_builtin_debrid_extension_id(&instance.extension_id)
        {
            continue;
        }
        let active_service = active_debrid_service_from_config(instance.config_json.as_ref())?;
        for service in
            debrid_route_attempt_service_order(instance.config_json.as_ref(), active_service)
        {
            if !debrid_secret_exists_for_instance(store, record.instance_id, service).await? {
                continue;
            }
            let attempt_key = debrid_route_attempt_key(provider_id, service);
            if !seen_attempts.insert(attempt_key.clone()) {
                continue;
            }
            attempts.push(DebridRouteAttempt {
                provider_id,
                instance_id: record.instance_id,
                implementation: service.implementation_id().to_string(),
                display_name: service.display_name().to_string(),
                health_state: record.health_state,
                attempt_key,
            });
        }
    }

    Ok(attempts)
}

pub fn debrid_route_attempt_key(provider_id: Uuid, service: DebridServiceKind) -> String {
    format!("debrid:{provider_id}:{}", service.implementation_id())
}

fn normalize_debrid_route_owner_id(owner_id: Option<&str>) -> String {
    owner_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(DEFAULT_ROUTE_OWNER_ID)
        .to_string()
}

fn is_builtin_debrid_extension_id(extension_id: &str) -> bool {
    extension_id == DEBRID_EXTENSION_ID || extension_id == LEGACY_REAL_DEBRID_EXTENSION_ID
}

fn debrid_route_attempt_service_order(
    config_json: Option<&Value>,
    active_service: DebridServiceKind,
) -> Vec<DebridServiceKind> {
    let mut ordered = Vec::new();
    push_unique_debrid_service(&mut ordered, active_service);
    if let Some(values) = config_json
        .and_then(|config| config.get("serviceOrder"))
        .or_else(|| config_json.and_then(|config| config.get("service_order")))
        .and_then(Value::as_array)
    {
        for value in values {
            if let Some(service) = value
                .as_str()
                .and_then(|raw| DebridServiceKind::from_implementation_id(raw).ok())
            {
                push_unique_debrid_service(&mut ordered, service);
            }
        }
    }
    for service in DebridServiceKind::ALL {
        push_unique_debrid_service(&mut ordered, service);
    }
    ordered
}

fn push_unique_debrid_service(ordered: &mut Vec<DebridServiceKind>, service: DebridServiceKind) {
    if !ordered.contains(&service) {
        ordered.push(service);
    }
}

pub async fn test_debrid_service_account(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    service: DebridServiceKind,
) -> Result<DebridAccount> {
    let factory = DebridAdapterFactory::from_state(state);
    factory
        .adapter_for_service(store, instance_id, service)
        .await?
        .test_account()
        .await
}

pub async fn test_debrid_account(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
) -> Result<DebridAccount> {
    let service = active_debrid_service_for_instance(store, instance_id).await?;
    test_debrid_service_account(state, store, instance_id, service).await
}

#[allow(dead_code)]
pub async fn test_real_debrid_account(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
) -> Result<RealDebridUser> {
    let account =
        test_debrid_service_account(state, store, instance_id, DebridServiceKind::RealDebrid)
            .await?;
    Ok(RealDebridUser {
        username: account.username.unwrap_or_default(),
    })
}

#[allow(dead_code)]
pub async fn submit_real_debrid(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider_id: Uuid,
    instance_id: Uuid,
    source: &str,
    options: DebridSubmitOptions<'_>,
) -> Result<Uuid> {
    submit_debrid(
        state,
        store,
        provider_id,
        instance_id,
        Some(REAL_DEBRID_IMPLEMENTATION),
        source,
        options,
    )
    .await
}

pub async fn submit_debrid(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider_id: Uuid,
    instance_id: Uuid,
    provider_implementation: Option<&str>,
    source: &str,
    options: DebridSubmitOptions<'_>,
) -> Result<Uuid> {
    let factory = DebridAdapterFactory::from_state(state);
    let adapter = factory
        .adapter_for_provider_implementation(store, instance_id, provider_implementation)
        .await?;
    let anime_matching = state.anime_inference.matching_service();
    submit_debrid_with_adapter_and_anime_matching(
        &state.db_pool,
        provider_id,
        instance_id,
        source,
        options,
        &anime_matching,
        &*adapter,
    )
    .await
}

async fn debrid_service_for_provider_or_instance(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    provider_implementation: Option<&str>,
) -> Result<DebridServiceKind> {
    match provider_implementation
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(implementation) => DebridServiceKind::from_implementation_id(implementation),
        None => active_debrid_service_for_instance(store, instance_id).await,
    }
}

async fn submit_debrid_with_adapter<A: DebridProviderAdapter + ?Sized>(
    pool: &sqlx::AnyPool,
    provider_id: Uuid,
    instance_id: Uuid,
    source: &str,
    options: DebridSubmitOptions<'_>,
    adapter: &A,
) -> Result<Uuid> {
    let anime_matching = AnimeMatchingService::disabled();
    submit_debrid_with_adapter_and_anime_matching(
        pool,
        provider_id,
        instance_id,
        source,
        options,
        &anime_matching,
        adapter,
    )
    .await
}

async fn submit_debrid_with_adapter_and_anime_matching<A: DebridProviderAdapter + ?Sized>(
    pool: &sqlx::AnyPool,
    provider_id: Uuid,
    instance_id: Uuid,
    source: &str,
    options: DebridSubmitOptions<'_>,
    anime_matching: &AnimeMatchingService,
    adapter: &A,
) -> Result<Uuid> {
    let source_kind = debrid_source_kind(source)?;
    let defer_anime_refinement = options
        .release_context
        .as_ref()
        .is_some_and(debrid_submit_context_bookkeeping_pending);
    if !options.paused {
        let cap = debrid_concurrent_downloads_for_instance(pool, instance_id).await?;
        let active_jobs = count_active_debrid_jobs_for_instance(pool, instance_id).await?;
        if active_jobs >= cap {
            bail!("Debrid route capacity reached: active Debrid jobs {active_jobs}/{cap}");
        }
    }
    let job_id = Uuid::new_v4();
    let mut remote_torrent_id = None;
    let mut remote_download_id = None;
    let mut remote_release_id = None;
    let mut remote_release_status = None;
    let mut links = Vec::new();
    let mut status = if options.paused {
        "paused".to_string()
    } else {
        DebridReleaseStatus::Staging.as_str().to_string()
    };
    let provider_capabilities = adapter.capabilities();
    let initial_coverage_plan = Some(merge_debrid_provider_provenance(
        None,
        provider_id,
        adapter.implementation(),
        &provider_capabilities,
        None,
        Some(&status),
        source_kind,
        None,
    ));
    let mut release = upsert_debrid_acquisition_release(
        pool,
        provider_id,
        source,
        source_kind,
        &options,
        None,
        None,
        AcquisitionReleaseState::Staging,
        Some("Debrid release staged before provider submission."),
        ReleaseShape::default(),
        initial_coverage_plan,
    )
    .await?;

    if !options.paused {
        match source_kind {
            "magnet" => {
                let submitted = match adapter.submit_magnet(source).await {
                    Ok(submitted) => submitted,
                    Err(err) => {
                        record_debrid_release_failure_without_job(
                            pool,
                            release.as_ref(),
                            job_id,
                            provider_id,
                            adapter,
                            &provider_capabilities,
                            source_kind,
                            &err,
                        )
                        .await?;
                        return Err(err);
                    }
                };
                remote_torrent_id = Some(submitted.remote_release_id.clone());
                remote_release_id = Some(submitted.remote_release_id.clone());
                remote_release_status = Some(submitted.status.as_str().to_string());
                status = debrid_status_to_job_status(submitted.status);
                if let Some(existing) = release.as_ref()
                    && let Some(updated) = upsert_debrid_acquisition_release(
                        pool,
                        provider_id,
                        source,
                        source_kind,
                        &options,
                        Some(&submitted.remote_release_id),
                        Some(&job_id.to_string()),
                        acquisition_state_for_debrid_status(submitted.status),
                        Some("Debrid release submitted and awaiting provider inspection."),
                        ReleaseShape::default(),
                        Some(merge_debrid_provider_provenance(
                            existing.coverage_plan.clone(),
                            provider_id,
                            adapter.implementation(),
                            &provider_capabilities,
                            Some(&submitted.remote_release_id),
                            Some(submitted.status.as_str()),
                            source_kind,
                            Some(job_id),
                        )),
                    )
                    .await?
                {
                    release = Some(updated);
                }
            }
            "hoster" => {
                let unrestricted = match adapter.unrestrict_hoster(source).await {
                    Ok(unrestricted) => unrestricted,
                    Err(err) => {
                        record_debrid_release_failure_without_job(
                            pool,
                            release.as_ref(),
                            job_id,
                            provider_id,
                            adapter,
                            &provider_capabilities,
                            source_kind,
                            &err,
                        )
                        .await?;
                        return Err(err);
                    }
                };
                if let Some(id) = unrestricted.provider_file_id {
                    remote_download_id = Some(id.clone());
                    remote_release_id = Some(id);
                }
                if !unrestricted.url.trim().is_empty() {
                    links.push(source.to_string());
                    status = debrid_status_to_job_status(DebridReleaseStatus::Downloaded);
                    remote_release_status =
                        Some(DebridReleaseStatus::Downloaded.as_str().to_string());
                } else {
                    status = "failed".to_string();
                    remote_release_status = Some(DebridReleaseStatus::Failed.as_str().to_string());
                }
            }
            other => bail!("unsupported debrid source kind '{other}'"),
        }
    }
    let remote_release_id = remote_release_id.or_else(|| {
        remote_torrent_id
            .clone()
            .or_else(|| remote_download_id.clone())
    });
    let remote_release_status = remote_release_status.or_else(|| Some(status.clone()));
    let deferred_anime_provider_failure = defer_anime_refinement
        && remote_release_status.as_deref() == Some(DebridReleaseStatus::Failed.as_str());
    if deferred_anime_provider_failure {
        // Keep the failed provider result discoverable while acquisition owns
        // the submission barrier. Inspection/automatic retry begins only
        // after the writer completes or the barrier recovery deadline passes.
        status = "anime_retry_pending".to_string();
    }

    insert_debrid_job(
        pool,
        &DebridDownloadJob {
            job_id,
            provider_id,
            instance_id,
            owner_id: normalized_owner_id(options.owner_id),
            source: source.to_string(),
            source_kind: source_kind.to_string(),
            category: options.category.and_then(non_empty).map(str::to_string),
            display_name: options.name.and_then(non_empty).map(str::to_string),
            remote_torrent_id,
            remote_download_id,
            provider_implementation: Some(adapter.implementation().to_string()),
            remote_release_id: remote_release_id.clone(),
            remote_release_status: remote_release_status.clone(),
            provider_capabilities: Some(json!(provider_capabilities.clone())),
            provider_status: None,
            selection_mode: Some(
                provider_capabilities
                    .file_selection_mode
                    .as_persistence_value()
                    .to_string(),
            ),
            selected_file_ids: Vec::new(),
            skipped_file_ids: Vec::new(),
            selection_error: None,
            release_id: release.as_ref().map(|release| release.release_id),
            status,
            local_path: None,
            links,
            progress: Some(0.0),
            downloaded_bytes: Some(0),
            total_bytes: None,
            download_rate_bps: None,
            last_error: None,
        },
    )
    .await?;
    if let Some(existing) = release.as_ref()
        && let Some(updated) = upsert_debrid_acquisition_release(
            pool,
            provider_id,
            source,
            source_kind,
            &options,
            remote_release_id.as_deref(),
            Some(&job_id.to_string()),
            if deferred_anime_provider_failure {
                AcquisitionReleaseState::Staging
            } else {
                acquisition_state_for_job_status(remote_release_status.as_deref())
            },
            Some("Debrid job recorded with provider provenance."),
            release_shape_from_release(existing),
            Some(merge_debrid_provider_provenance(
                existing.coverage_plan.clone(),
                provider_id,
                adapter.implementation(),
                &provider_capabilities,
                remote_release_id.as_deref(),
                remote_release_status.as_deref(),
                source_kind,
                Some(job_id),
            )),
        )
        .await?
    {
        release = Some(updated);
        upsert_debrid_release_job(
            pool,
            release.as_ref().expect("release should exist"),
            provider_id,
            job_id,
            remote_release_id.as_deref(),
            if deferred_anime_provider_failure {
                ReleaseJobState::Staging
            } else {
                release_job_state_for_job_status(remote_release_status.as_deref())
            },
            "Debrid job recorded with provider provenance.",
        )
        .await?;
    }
    if !options.paused
        && !defer_anime_refinement
        && source_kind == "magnet"
        && let Some(remote_release_id) = remote_release_id.as_deref()
    {
        let staged_result = async {
            let inspection = adapter.inspect_release(remote_release_id).await?;
            if !update_debrid_job_from_inspection(pool, job_id, &inspection).await? {
                return Ok(());
            }
            if consume_failed_anime_debrid_inspection(pool, adapter, job_id, &inspection).await? {
                return Ok(());
            }
            cleanup_uncached_no_seed_release(pool, adapter, job_id, &inspection).await?;
            if matches!(
                inspection.release.status,
                DebridReleaseStatus::WaitingFiles | DebridReleaseStatus::Downloaded
            ) && !inspection.files.is_empty()
                && let Some(existing) = release.as_ref()
            {
                let refinement = persist_debrid_file_list_and_refine_coverage(
                    pool,
                    existing,
                    &options,
                    &inspection,
                    anime_matching,
                )
                .await?;
                let coverage_plan = Some(merge_debrid_provider_provenance(
                    refinement.coverage_plan.clone(),
                    provider_id,
                    adapter.implementation(),
                    &provider_capabilities,
                    Some(&inspection.release.remote_release_id),
                    Some(inspection.release.status.as_str()),
                    source_kind,
                    Some(job_id),
                ));
                let updated = if existing.media_type == MediaType::Anime {
                    commit_anime_debrid_refinement_if_owned(
                        pool,
                        existing,
                        provider_id,
                        job_id,
                        &inspection,
                        &refinement,
                        coverage_plan,
                    )
                    .await?
                } else {
                    let updated = upsert_debrid_acquisition_release(
                        pool,
                        provider_id,
                        source,
                        source_kind,
                        &options,
                        Some(&inspection.release.remote_release_id),
                        Some(&job_id.to_string()),
                        refinement.state,
                        refinement.state_reason.as_deref(),
                        refinement.shape.clone(),
                        coverage_plan,
                    )
                    .await?;
                    if let Some(updated) = updated.as_ref() {
                        upsert_debrid_release_job(
                            pool,
                            updated,
                            provider_id,
                            job_id,
                            Some(&inspection.release.remote_release_id),
                            refinement.job_state,
                            refinement
                                .job_state_reason
                                .as_deref()
                                .unwrap_or("Debrid release inspected and staged."),
                        )
                        .await?;
                    }
                    updated
                };
                if let Some(updated) = updated {
                    if let Some(automatic_retry) = refinement.automatic_retry.as_ref() {
                        stage_anime_debrid_retry_disposition(pool, job_id, automatic_retry).await?;
                    } else if refinement.apply_file_selection_policy {
                        let _ = apply_debrid_file_selection_policy(
                            pool,
                            adapter,
                            job_id,
                            &updated,
                            &inspection,
                            false,
                        )
                        .await?;
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        }
        .await;
        if let Err(err) = staged_result {
            if let Some(existing) = release
                .as_ref()
                .filter(|release| release.media_type == MediaType::Anime)
            {
                let retry = anime_debrid_runtime_error_retry_disposition(
                    pool,
                    job_id,
                    existing,
                    "anime_debrid_staging_error",
                    &err,
                )
                .await?;
                stage_anime_debrid_retry_disposition(pool, job_id, &retry).await?;
                persist_anime_debrid_retry_with_adapter(
                    pool,
                    adapter,
                    job_id,
                    existing,
                    remote_release_id,
                    adapter.implementation(),
                    &retry,
                )
                .await?;
            } else {
                mark_debrid_job_status(pool, job_id, "failed", Some(&err.to_string())).await?;
            }
            return Err(err);
        }
    }
    Ok(job_id)
}

fn debrid_submit_context_bookkeeping_pending(context: &DebridReleaseSubmitContext) -> bool {
    context.media_type == MediaType::Anime
        && context
            .selected_candidate
            .as_ref()
            .and_then(|candidate| candidate.pointer("/submissionBookkeeping/status"))
            .and_then(Value::as_str)
            == Some("pending")
}

fn debrid_release_bookkeeping_pending(release: &AcquisitionRelease) -> bool {
    release
        .selected_candidate
        .as_ref()
        .and_then(|candidate| candidate.pointer("/submissionBookkeeping/status"))
        .and_then(Value::as_str)
        == Some("pending")
        || release
            .coverage_plan
            .as_ref()
            .and_then(|plan| plan.pointer("/submissionBookkeeping/status"))
            .and_then(Value::as_str)
            == Some("pending")
}

#[derive(Debug, Clone)]
struct ReleaseShape {
    release_kind: ReleaseKind,
    resolver_kind: ReleaseResolverKind,
    resolver_version: String,
    confidence: ReleaseConfidence,
}

impl Default for ReleaseShape {
    fn default() -> Self {
        Self {
            release_kind: ReleaseKind::Unknown,
            resolver_kind: ReleaseResolverKind::Unresolved,
            resolver_version: "rr4-debrid-staging-v1".to_string(),
            confidence: ReleaseConfidence::Low,
        }
    }
}

#[derive(Debug, Clone)]
struct DebridCoverageRefinement {
    shape: ReleaseShape,
    state: AcquisitionReleaseState,
    state_reason: Option<String>,
    job_state: ReleaseJobState,
    job_state_reason: Option<String>,
    coverage_plan: Option<Value>,
    /// Difficult anime remains entirely automatic. The provider selection
    /// policy is only run once deterministic coverage or a validated local
    /// model result has proved an exact file set.
    apply_file_selection_policy: bool,
    /// An unresolved anime file map rejects only this release and returns the
    /// scoped targets to the normal acquisition scheduler. It is never a
    /// user-facing review state.
    automatic_retry: Option<AnimeDebridAutomaticRetry>,
    /// Anime coverage is computed without touching shared release rows. The
    /// worker commits these entries together with the exact-attempt ownership
    /// check after any local-model call has completed.
    anime_coverage_entries: Vec<AnimeDebridCoverageWrite>,
}

#[derive(Debug, Clone)]
struct AnimeDebridCoverageWrite {
    target_id: Uuid,
    provider_file_id: String,
    coverage_kind: ReleaseCoverageKind,
    confidence: ReleaseConfidence,
    score: Option<f64>,
    reason: String,
    state: ReleaseCoverageState,
    verified_by: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AnimeDebridAutomaticRetry {
    target_ids: Vec<Uuid>,
    reason_code: String,
    suppress_automatic_rediscovery: bool,
    coverage_plan: Option<Value>,
}

fn release_shape_from_release(release: &AcquisitionRelease) -> ReleaseShape {
    ReleaseShape {
        release_kind: release.release_kind,
        resolver_kind: release.resolver_kind,
        resolver_version: release.resolver_version.clone(),
        confidence: release.confidence,
    }
}

async fn upsert_debrid_acquisition_release(
    pool: &sqlx::AnyPool,
    provider_id: Uuid,
    source: &str,
    source_kind: &str,
    options: &DebridSubmitOptions<'_>,
    remote_release_id: Option<&str>,
    download_id: Option<&str>,
    state: AcquisitionReleaseState,
    state_reason: Option<&str>,
    shape: ReleaseShape,
    coverage_plan: Option<Value>,
) -> Result<Option<AcquisitionRelease>> {
    let Some(context) = options.release_context.as_ref() else {
        return Ok(None);
    };
    let fingerprint = context.fingerprint.clone().unwrap_or_else(|| {
        build_release_fingerprint(&ReleaseFingerprintInput {
            source_kind,
            source,
            info_hash: context.info_hash.as_deref(),
            release_title: &context.release_title,
            size_bytes: selected_candidate_u64(context.selected_candidate.as_ref(), "sizeBytes"),
            source_provider_id: context.source_provider_id,
        })
    });
    let info_hash = context
        .info_hash
        .clone()
        .or_else(|| extract_magnet_info_hash(source));
    let release = upsert_release(
        pool,
        NewAcquisitionRelease {
            release_id: None,
            subscription_id: context.subscription_id,
            source_provider_id: context.source_provider_id,
            source_extension_id: context.source_extension_id.clone(),
            owner_id: DEFAULT_ROUTE_OWNER_ID.to_string(),
            media_type: context.media_type,
            title: context.title.clone(),
            release_title: context.release_title.clone(),
            source: source.to_string(),
            source_kind: source_kind.to_string(),
            info_hash,
            fingerprint,
            release_kind: shape.release_kind,
            resolver_kind: shape.resolver_kind,
            resolver_version: shape.resolver_version,
            confidence: shape.confidence,
            score: context
                .score
                .or_else(|| selected_candidate_f64(context.selected_candidate.as_ref(), "score")),
            selected_route_logical_id: Some(DEBRID_DEFAULT_LOGICAL_ID.to_string()),
            selected_provider_id: Some(provider_id),
            download_id: download_id.map(str::to_string),
            remote_release_id: remote_release_id.map(str::to_string),
            state,
            state_reason: state_reason.map(str::to_string),
            selected_candidate: context.selected_candidate.clone(),
            coverage_plan,
        },
    )
    .await?;
    Ok(Some(release))
}

async fn upsert_debrid_release_job(
    pool: &sqlx::AnyPool,
    release: &AcquisitionRelease,
    provider_id: Uuid,
    job_id: Uuid,
    remote_release_id: Option<&str>,
    state: ReleaseJobState,
    reason: &str,
) -> Result<()> {
    let terminal = matches!(
        state,
        ReleaseJobState::Completed | ReleaseJobState::Failed | ReleaseJobState::Cancelled
    );
    upsert_release_job(
        pool,
        NewAcquisitionReleaseJob {
            release_job_id: None,
            release_id: release.release_id,
            route_logical_id: DEBRID_DEFAULT_LOGICAL_ID.to_string(),
            provider_id: Some(provider_id),
            download_id: Some(job_id.to_string()),
            remote_release_id: remote_release_id.map(str::to_string),
            state,
            state_reason: Some(reason.to_string()),
            active: !terminal,
            started_at: Some(chrono::Utc::now()),
            completed_at: terminal.then(chrono::Utc::now),
        },
    )
    .await?;
    Ok(())
}

async fn persist_debrid_file_list_and_refine_coverage(
    pool: &sqlx::AnyPool,
    release: &AcquisitionRelease,
    options: &DebridSubmitOptions<'_>,
    inspection: &DebridReleaseInspection,
    anime_matching: &AnimeMatchingService,
) -> Result<DebridCoverageRefinement> {
    // Anime may spend measurable time in the local model. Do not write its
    // provider files or coverage until the exact attempt is revalidated and
    // committed transactionally after inference.
    let file_ids = if release.media_type == MediaType::Anime {
        HashMap::new()
    } else {
        persist_debrid_release_files(pool, release, &inspection.files).await?
    };
    let base = refinement_from_debrid_status(inspection.release.status);
    let Some(subscription_id) = release.subscription_id else {
        if release.media_type == MediaType::Anime {
            let coverage_plan = json!({
                "source": "debrid_provider_file_list",
                "providerImplementation": inspection.release.provider_implementation,
                "remoteReleaseId": inspection.release.remote_release_id,
                "files": inspection.files.len(),
                "automaticResolution": {
                    "status": "pending",
                    "reason": "missing_subscription_context",
                    "retryDisposition": "retryable"
                }
            });
            return Ok(DebridCoverageRefinement {
                coverage_plan: Some(coverage_plan.clone()),
                state: AcquisitionReleaseState::Staging,
                state_reason: Some(
                    "Anime file matching is pending automatic subscription-context recovery."
                        .to_string(),
                ),
                job_state: ReleaseJobState::Staging,
                job_state_reason: Some(
                    "Anime file matching will retry automatically when context is available."
                        .to_string(),
                ),
                apply_file_selection_policy: false,
                automatic_retry: Some(AnimeDebridAutomaticRetry {
                    target_ids: Vec::new(),
                    reason_code: "anime_debrid_missing_subscription_context".to_string(),
                    suppress_automatic_rediscovery: false,
                    coverage_plan: Some(coverage_plan),
                }),
                anime_coverage_entries: Vec::new(),
                ..base
            });
        }
        return Ok(DebridCoverageRefinement {
            coverage_plan: Some(json!({
                "source": "debrid_provider_file_list",
                "providerImplementation": inspection.release.provider_implementation,
                "remoteReleaseId": inspection.release.remote_release_id,
                "files": inspection.files.len(),
                "reviewReasons": ["missing_subscription_context"]
            })),
            state: AcquisitionReleaseState::ReviewRequired,
            state_reason: Some(
                "Debrid file list staged without subscription target context.".to_string(),
            ),
            apply_file_selection_policy: true,
            automatic_retry: None,
            anime_coverage_entries: Vec::new(),
            ..base
        });
    };

    let targets = list_subscription_targets(pool, subscription_id).await?;
    match release.media_type {
        MediaType::Series => {
            refine_tv_debrid_coverage(pool, release, inspection, &targets, &file_ids).await
        }
        MediaType::Anime => {
            let subscription = get_subscription(pool, subscription_id).await?;
            let existing_coverage = list_release_coverage(pool, release.release_id).await?;
            let bound_target_ids = match release.download_id.as_deref() {
                Some(download_id) => target_ids_for_download_id(pool, download_id).await?,
                None => Vec::new(),
            };
            let scoped_targets = debrid_release_scoped_targets(
                release,
                &targets,
                &existing_coverage,
                &bound_target_ids,
            );
            refine_anime_debrid_coverage(
                release,
                options,
                inspection,
                subscription.as_ref(),
                &scoped_targets,
                anime_matching,
            )
            .await
        }
        MediaType::Movie => {
            refine_movie_debrid_coverage(pool, release, inspection, &targets, &file_ids).await
        }
    }
}

async fn persist_debrid_release_files(
    pool: &sqlx::AnyPool,
    release: &AcquisitionRelease,
    files: &[DebridRemoteFile],
) -> Result<HashMap<String, Uuid>> {
    let mut file_ids = HashMap::new();
    for file in files {
        let parse_name = if file.basename.trim().is_empty() {
            file.path.as_str()
        } else {
            file.basename.as_str()
        };
        let parsed = parsed_file_metadata(release.media_type, parse_name);
        let release_file = upsert_release_file(
            pool,
            NewAcquisitionReleaseFile {
                release_file_id: None,
                release_id: release.release_id,
                file_index: file.file_index,
                file_id: Some(file.provider_file_id.clone()),
                provider_file_id: Some(file.provider_file_id.clone()),
                path: file.path.clone(),
                basename: Some(file.basename.clone()),
                size_bytes: file.size_bytes.and_then(u64_to_i64),
                selectable: file.selectable,
                selected: file.selected,
                parsed_title: parsed.title,
                parsed_season_number: parsed.season_number,
                parsed_episode_number: parsed.episode_number,
                parsed_episode_end_number: parsed.episode_end_number,
                parsed_absolute_episode_number: parsed.absolute_episode_number,
                parsed_absolute_episode_end_number: parsed.absolute_episode_end_number,
                parsed_air_date: parsed.air_date,
                parsed_quality: parsed.quality,
                parsed_language: parsed.language,
                parsed_release_group: parsed.release_group,
                parser_confidence: parsed.confidence,
                parser_reason: parsed.reason,
                raw: file.raw.clone(),
                provider_metadata: Some(json!({
                    "providerFileId": file.provider_file_id,
                    "fileIndex": file.file_index,
                    "selectable": file.selectable,
                    "selected": file.selected,
                    "sizeBytes": file.size_bytes
                })),
            },
        )
        .await?;
        file_ids.insert(file.provider_file_id.clone(), release_file.release_file_id);
    }
    Ok(file_ids)
}

async fn commit_anime_debrid_refinement_if_owned(
    pool: &sqlx::AnyPool,
    release: &AcquisitionRelease,
    provider_id: Uuid,
    job_id: Uuid,
    inspection: &DebridReleaseInspection,
    refinement: &DebridCoverageRefinement,
    coverage_plan: Option<Value>,
) -> Result<Option<AcquisitionRelease>> {
    debug_assert_eq!(release.media_type, MediaType::Anime);
    let release_id = release.release_id.to_string();
    let job_id = job_id.to_string();
    let provider_id = provider_id.to_string();
    let remote_release_id = inspection.release.remote_release_id.trim();
    if remote_release_id.is_empty() {
        return Ok(None);
    }
    let mut transaction = pool.begin().await?;

    // This row lock and compare-and-set is the commit point for an anime
    // provider-file resolution. A delayed worker for attempt A cannot write
    // files, coverage, or state after the scheduler has rebound the release
    // to attempt B.
    let ownership = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_releases
         SET updated_at = updated_at
         WHERE release_id = $1
           AND media_type = 'anime'
           AND download_id = $2
           AND selected_route_logical_id = $3
           AND selected_provider_id = $4
           AND remote_release_id = $5
           AND state IN ('staging', 'ready', 'submitted', 'downloading', 'materializing')
           AND EXISTS (
               SELECT 1
               FROM acquisition_release_jobs j
               WHERE j.release_id = acquisition_releases.release_id
                 AND j.download_id = $2
                 AND j.route_logical_id = $3
                 AND j.provider_id = $4
                 AND j.remote_release_id = $5
                 AND j.active = 1
                 AND j.state IN ('staging', 'ready', 'submitted', 'downloading', 'materializing')
           )
           AND EXISTS (
               SELECT 1
               FROM debrid_download_jobs d
               WHERE d.job_id = $2
                 AND d.release_id = acquisition_releases.release_id
                 AND d.provider_id = $4
                 AND COALESCE(d.remote_release_id, d.remote_torrent_id, '') = $5
                 AND d.status NOT IN ('completed', 'failed', 'cancelled', 'paused', 'review_required', 'materializing', 'anime_retry_pending')
           )",
    )
    .bind(&release_id)
    .bind(&job_id)
    .bind(DEBRID_DEFAULT_LOGICAL_ID)
    .bind(&provider_id)
    .bind(remote_release_id)
    .execute(&mut *transaction)
    .await
    .context("claiming exact anime Debrid refinement commit")?;
    if ownership.rows_affected() != 1 {
        transaction.rollback().await?;
        return Ok(None);
    }

    let file_ids = persist_anime_debrid_files_in_transaction(
        &mut transaction,
        release.release_id,
        &inspection.files,
    )
    .await?;
    for entry in &refinement.anime_coverage_entries {
        let Some(release_file_id) = file_ids.get(&entry.provider_file_id).copied() else {
            transaction.rollback().await?;
            bail!(
                "anime Debrid coverage references unavailable provider file '{}'",
                entry.provider_file_id
            );
        };
        upsert_anime_debrid_coverage_in_transaction(
            &mut transaction,
            release.release_id,
            release_file_id,
            entry,
        )
        .await?;
    }

    let coverage_plan_json = coverage_plan
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .context("serializing exact anime Debrid refinement coverage")?;
    let release_update = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_releases
         SET release_kind = $1,
             resolver_kind = $2,
             resolver_version = $3,
             confidence = $4,
             state = $5,
             state_reason = $6,
             coverage_plan_json = $7,
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = $8
           AND download_id = $9
           AND selected_route_logical_id = $10
           AND selected_provider_id = $11
           AND remote_release_id = $12",
    )
    .bind(refinement.shape.release_kind.as_str())
    .bind(refinement.shape.resolver_kind.as_str())
    .bind(&refinement.shape.resolver_version)
    .bind(refinement.shape.confidence.as_str())
    .bind(refinement.state.as_str())
    .bind(refinement.state_reason.as_deref())
    .bind(coverage_plan_json.as_deref())
    .bind(&release_id)
    .bind(&job_id)
    .bind(DEBRID_DEFAULT_LOGICAL_ID)
    .bind(&provider_id)
    .bind(remote_release_id)
    .execute(&mut *transaction)
    .await
    .context("committing exact anime Debrid release refinement")?;
    if release_update.rows_affected() != 1 {
        transaction.rollback().await?;
        return Ok(None);
    }

    let terminal = matches!(
        refinement.job_state,
        ReleaseJobState::Completed | ReleaseJobState::Failed | ReleaseJobState::Cancelled
    );
    let release_job_update = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_release_jobs
         SET state = $1,
             state_reason = $2,
             active = $3,
             completed_at = CASE WHEN $3 = 0 THEN COALESCE(completed_at, CURRENT_TIMESTAMP) ELSE completed_at END,
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = $4
           AND download_id = $5
           AND route_logical_id = $6
           AND provider_id = $7
           AND remote_release_id = $8
           AND active = 1",
    )
    .bind(refinement.job_state.as_str())
    .bind(
        refinement
            .job_state_reason
            .as_deref()
            .unwrap_or("Debrid release inspected and staged."),
    )
    .bind(if terminal { 0_i64 } else { 1_i64 })
    .bind(&release_id)
    .bind(&job_id)
    .bind(DEBRID_DEFAULT_LOGICAL_ID)
    .bind(&provider_id)
    .bind(remote_release_id)
    .execute(&mut *transaction)
    .await
    .context("committing exact anime Debrid release-job refinement")?;
    if release_job_update.rows_affected() != 1 {
        transaction.rollback().await?;
        return Ok(None);
    }
    transaction.commit().await?;
    get_release(pool, release.release_id).await
}

async fn anime_debrid_attempt_is_current(
    pool: &sqlx::AnyPool,
    release: &AcquisitionRelease,
    provider_id: Uuid,
    job_id: Uuid,
    remote_release_id: &str,
) -> Result<bool> {
    if release.media_type != MediaType::Anime {
        return Ok(true);
    }
    let remote_release_id = remote_release_id.trim();
    if remote_release_id.is_empty() {
        return Ok(false);
    }
    let count = sqlx::query_scalar::<sqlx::Any, i64>(
        "SELECT COUNT(*)
         FROM acquisition_releases r
         JOIN debrid_download_jobs d
           ON d.release_id = r.release_id
          AND d.job_id = $2
          AND d.provider_id = $4
         WHERE r.release_id = $1
           AND r.media_type = 'anime'
           AND r.download_id = $2
           AND r.selected_route_logical_id = $3
           AND r.selected_provider_id = $4
           AND r.remote_release_id = $5
           AND r.state IN ('staging', 'ready', 'submitted', 'downloading', 'materializing')
           AND COALESCE(d.remote_release_id, d.remote_torrent_id, '') = $5
           AND d.status NOT IN ('completed', 'failed', 'cancelled', 'paused', 'review_required', 'materializing', 'anime_retry_pending')
           AND EXISTS (
               SELECT 1
               FROM acquisition_release_jobs j
               WHERE j.release_id = r.release_id
                 AND j.download_id = $2
                 AND j.route_logical_id = $3
                 AND j.provider_id = $4
                 AND j.remote_release_id = $5
                 AND j.active = 1
                 AND j.state IN ('staging', 'ready', 'submitted', 'downloading', 'materializing')
           )",
    )
    .bind(release.release_id.to_string())
    .bind(job_id.to_string())
    .bind(DEBRID_DEFAULT_LOGICAL_ID)
    .bind(provider_id.to_string())
    .bind(remote_release_id)
    .fetch_one(pool)
    .await
    .context("checking exact anime Debrid worker ownership")?;
    Ok(count == 1)
}

async fn persist_anime_debrid_files_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Any>,
    release_id: Uuid,
    files: &[DebridRemoteFile],
) -> Result<HashMap<String, Uuid>> {
    let release_id = release_id.to_string();
    let mut file_ids = HashMap::new();
    for file in files {
        let existing = sqlx::query_scalar::<sqlx::Any, String>(
            "SELECT release_file_id
             FROM acquisition_release_files
             WHERE release_id = $1 AND provider_file_id = $2
             LIMIT 1",
        )
        .bind(&release_id)
        .bind(&file.provider_file_id)
        .fetch_optional(&mut **transaction)
        .await?;
        let release_file_id = existing
            .as_deref()
            .and_then(|value| Uuid::parse_str(value).ok())
            .unwrap_or_else(Uuid::new_v4);
        let parse_name = if file.basename.trim().is_empty() {
            file.path.as_str()
        } else {
            file.basename.as_str()
        };
        let parsed = parsed_file_metadata(MediaType::Anime, parse_name);
        let raw_json = file
            .raw
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .context("serializing anime Debrid provider file")?;
        let provider_metadata_json = serde_json::to_string(&json!({
            "providerFileId": file.provider_file_id,
            "fileIndex": file.file_index,
            "selectable": file.selectable,
            "selected": file.selected,
            "sizeBytes": file.size_bytes
        }))?;
        if existing.is_some() {
            sqlx::query::<sqlx::Any>(
                "UPDATE acquisition_release_files
                 SET file_index = $1, file_id = $2, provider_file_id = $3,
                     path = $4, basename = $5, size_bytes = $6, selectable = $7,
                     selected = $8, parsed_title = $9, parsed_season_number = $10,
                     parsed_episode_number = $11, parsed_episode_end_number = $12,
                     parsed_absolute_episode_number = $13,
                     parsed_absolute_episode_end_number = $14, parsed_air_date = $15,
                     parsed_quality = $16, parsed_language = $17,
                     parsed_release_group = $18, parser_confidence = $19,
                     parser_reason = $20, raw_json = $21,
                     provider_metadata_json = $22, updated_at = CURRENT_TIMESTAMP
                 WHERE release_file_id = $23 AND release_id = $24",
            )
            .bind(file.file_index)
            .bind(&file.provider_file_id)
            .bind(&file.provider_file_id)
            .bind(file.path.trim())
            .bind(&file.basename)
            .bind(file.size_bytes.and_then(u64_to_i64))
            .bind(if file.selectable { 1_i64 } else { 0_i64 })
            .bind(file.selected.map(|value| if value { 1_i64 } else { 0_i64 }))
            .bind(parsed.title.as_deref())
            .bind(parsed.season_number)
            .bind(parsed.episode_number)
            .bind(parsed.episode_end_number)
            .bind(parsed.absolute_episode_number)
            .bind(parsed.absolute_episode_end_number)
            .bind(parsed.air_date.as_deref())
            .bind(parsed.quality.as_deref())
            .bind(parsed.language.as_deref())
            .bind(parsed.release_group.as_deref())
            .bind(parsed.confidence.as_str())
            .bind(parsed.reason.as_deref())
            .bind(raw_json.as_deref())
            .bind(&provider_metadata_json)
            .bind(release_file_id.to_string())
            .bind(&release_id)
            .execute(&mut **transaction)
            .await
            .context("updating exact anime Debrid provider file")?;
        } else {
            sqlx::query::<sqlx::Any>(
                "INSERT INTO acquisition_release_files (
                    release_file_id, release_id, file_index, file_id, provider_file_id,
                    path, basename, size_bytes, selectable, selected, parsed_title,
                    parsed_season_number, parsed_episode_number, parsed_episode_end_number,
                    parsed_absolute_episode_number, parsed_absolute_episode_end_number,
                    parsed_air_date, parsed_quality, parsed_language, parsed_release_group,
                    parser_confidence, parser_reason, raw_json, provider_metadata_json
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                           $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)",
            )
            .bind(release_file_id.to_string())
            .bind(&release_id)
            .bind(file.file_index)
            .bind(&file.provider_file_id)
            .bind(&file.provider_file_id)
            .bind(file.path.trim())
            .bind(&file.basename)
            .bind(file.size_bytes.and_then(u64_to_i64))
            .bind(if file.selectable { 1_i64 } else { 0_i64 })
            .bind(file.selected.map(|value| if value { 1_i64 } else { 0_i64 }))
            .bind(parsed.title.as_deref())
            .bind(parsed.season_number)
            .bind(parsed.episode_number)
            .bind(parsed.episode_end_number)
            .bind(parsed.absolute_episode_number)
            .bind(parsed.absolute_episode_end_number)
            .bind(parsed.air_date.as_deref())
            .bind(parsed.quality.as_deref())
            .bind(parsed.language.as_deref())
            .bind(parsed.release_group.as_deref())
            .bind(parsed.confidence.as_str())
            .bind(parsed.reason.as_deref())
            .bind(raw_json.as_deref())
            .bind(&provider_metadata_json)
            .execute(&mut **transaction)
            .await
            .context("creating exact anime Debrid provider file")?;
        }
        file_ids.insert(file.provider_file_id.clone(), release_file_id);
    }
    Ok(file_ids)
}

async fn upsert_anime_debrid_coverage_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Any>,
    release_id: Uuid,
    release_file_id: Uuid,
    entry: &AnimeDebridCoverageWrite,
) -> Result<()> {
    let release_id = release_id.to_string();
    let release_file_id = release_file_id.to_string();
    let target_id = entry.target_id.to_string();
    let existing = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT coverage_id
         FROM acquisition_release_coverage
         WHERE release_id = $1 AND target_id = $2 AND release_file_id = $3
         LIMIT 1",
    )
    .bind(&release_id)
    .bind(&target_id)
    .bind(&release_file_id)
    .fetch_optional(&mut **transaction)
    .await?;
    if let Some(coverage_id) = existing {
        sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_release_coverage
             SET coverage_kind = $1, confidence = $2, score = $3, reason = $4,
                 state = $5, verified_by = $6, updated_at = CURRENT_TIMESTAMP
             WHERE coverage_id = $7 AND release_id = $8",
        )
        .bind(entry.coverage_kind.as_str())
        .bind(entry.confidence.as_str())
        .bind(entry.score)
        .bind(&entry.reason)
        .bind(entry.state.as_str())
        .bind(&entry.verified_by)
        .bind(coverage_id)
        .bind(&release_id)
        .execute(&mut **transaction)
        .await?;
    } else {
        sqlx::query::<sqlx::Any>(
            "INSERT INTO acquisition_release_coverage (
                coverage_id, release_id, release_file_id, target_id, coverage_kind,
                confidence, score, reason, state, verified_by
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(&release_id)
        .bind(&release_file_id)
        .bind(&target_id)
        .bind(entry.coverage_kind.as_str())
        .bind(entry.confidence.as_str())
        .bind(entry.score)
        .bind(&entry.reason)
        .bind(entry.state.as_str())
        .bind(&entry.verified_by)
        .execute(&mut **transaction)
        .await?;
    }
    Ok(())
}

fn debrid_release_scoped_targets(
    release: &AcquisitionRelease,
    targets: &[crate::acquisition::subscriptions::AcquisitionTarget],
    existing_coverage: &[AcquisitionReleaseCoverage],
    bound_target_ids: &[Uuid],
) -> Vec<crate::acquisition::subscriptions::AcquisitionTarget> {
    let request_scope = release.selected_candidate.as_ref().and_then(|candidate| {
        candidate
            .get("requestScopeEvidence")
            .or_else(|| candidate.get("request_scope_evidence"))
    });
    if let Some(scope) = request_scope {
        let target_ids_value = scope.get("targetIds").or_else(|| scope.get("target_ids"));
        let target_keys_value = scope.get("targetKeys").or_else(|| scope.get("target_keys"));
        let target_ids_present = target_ids_value.is_some();
        let target_keys_present = target_keys_value.is_some();
        if !target_ids_present && !target_keys_present {
            return Vec::new();
        }
        let mut target_ids = BTreeSet::new();
        if let Some(ids) = target_ids_value {
            let Some(ids) = ids.as_array() else {
                return Vec::new();
            };
            if ids.is_empty() {
                return Vec::new();
            }
            for value in ids {
                let Some(value) = value.as_str() else {
                    return Vec::new();
                };
                let Ok(target_id) = Uuid::parse_str(value.trim()) else {
                    return Vec::new();
                };
                if !targets.iter().any(|target| target.target_id == target_id) {
                    return Vec::new();
                }
                target_ids.insert(target_id);
            }
        }
        let mut keyed_ids = BTreeSet::new();
        if let Some(keys) = target_keys_value {
            let Some(keys) = keys.as_array() else {
                return Vec::new();
            };
            if keys.is_empty() {
                return Vec::new();
            }
            for value in keys {
                let Some(key) = value.as_str().map(str::trim).filter(|key| !key.is_empty()) else {
                    return Vec::new();
                };
                let matching = targets
                    .iter()
                    .filter(|target| target.target_key == key)
                    .map(|target| target.target_id)
                    .collect::<BTreeSet<_>>();
                if matching.len() != 1 {
                    return Vec::new();
                }
                keyed_ids.extend(matching);
            }
        }
        if target_ids_present && target_keys_present && target_ids != keyed_ids {
            return Vec::new();
        }
        let scoped_ids = if target_ids_present {
            target_ids
        } else {
            keyed_ids
        };
        let covered_ids = existing_coverage
            .iter()
            .filter(|coverage| coverage.state != ReleaseCoverageState::Rejected)
            .map(|coverage| coverage.target_id)
            .collect::<BTreeSet<_>>();
        let bound_ids = bound_target_ids.iter().copied().collect::<BTreeSet<_>>();
        if (!covered_ids.is_empty() && covered_ids != scoped_ids)
            || (!bound_ids.is_empty() && bound_ids != scoped_ids)
        {
            return Vec::new();
        }
        return targets
            .iter()
            .filter(|target| scoped_ids.contains(&target.target_id))
            .cloned()
            .collect();
    }

    let covered_target_ids = existing_coverage
        .iter()
        .filter(|coverage| coverage.state != ReleaseCoverageState::Rejected)
        .map(|coverage| coverage.target_id)
        .collect::<BTreeSet<_>>();
    if !covered_target_ids.is_empty() {
        return targets
            .iter()
            .filter(|target| covered_target_ids.contains(&target.target_id))
            .cloned()
            .collect();
    }

    let bound_target_ids = bound_target_ids.iter().copied().collect::<BTreeSet<_>>();
    if !bound_target_ids.is_empty() {
        return targets
            .iter()
            .filter(|target| bound_target_ids.contains(&target.target_id))
            .cloned()
            .collect();
    }

    if targets.len() == 1 {
        return targets.to_vec();
    }
    Vec::new()
}

#[derive(Debug, Clone)]
struct ParsedReleaseFileMetadata {
    title: Option<String>,
    season_number: Option<i32>,
    episode_number: Option<i32>,
    episode_end_number: Option<i32>,
    absolute_episode_number: Option<i32>,
    absolute_episode_end_number: Option<i32>,
    air_date: Option<String>,
    quality: Option<String>,
    language: Option<String>,
    release_group: Option<String>,
    confidence: ReleaseConfidence,
    reason: Option<String>,
}

fn parsed_file_metadata(media_type: MediaType, path: &str) -> ParsedReleaseFileMetadata {
    match media_type {
        MediaType::Series => {
            let parsed = TvSonarrStyleResolver.parse_file(path);
            let has_air_date = parsed.air_date.is_some();
            ParsedReleaseFileMetadata {
                title: parsed.normalized_series_title,
                season_number: parsed.season_number,
                episode_number: parsed.episode_numbers.first().copied(),
                episode_end_number: parsed.episode_numbers.last().copied(),
                absolute_episode_number: parsed.anime_absolute_hints.first().copied(),
                absolute_episode_end_number: parsed.anime_absolute_hints.last().copied(),
                air_date: parsed.air_date,
                quality: parsed
                    .quality
                    .resolution
                    .map(|resolution| format!("{resolution:?}")),
                language: parsed.modifiers.languages.first().cloned(),
                release_group: parsed.release_group,
                confidence: if parsed.season_number.is_some()
                    && (!parsed.episode_numbers.is_empty() || has_air_date)
                {
                    ReleaseConfidence::High
                } else {
                    ReleaseConfidence::ReviewRequired
                },
                reason: None,
            }
        }
        MediaType::Anime => {
            let parsed = parse_anime_release_title(path);
            ParsedReleaseFileMetadata {
                title: parsed.series_title,
                season_number: parsed.season_number,
                episode_number: parsed.episode_start_number,
                episode_end_number: parsed.episode_end_number,
                absolute_episode_number: parsed.absolute_episode_numbers.first().copied(),
                absolute_episode_end_number: parsed.absolute_episode_numbers.last().copied(),
                air_date: None,
                quality: parsed.quality.resolution,
                language: parsed
                    .subtitle_languages
                    .first()
                    .cloned()
                    .or_else(|| parsed.audio_languages.first().cloned()),
                release_group: parsed.release_group,
                confidence: parsed.confidence,
                reason: (!parsed.review_reasons.is_empty())
                    .then(|| parsed.review_reasons.join(",")),
            }
        }
        MediaType::Movie => {
            let parsed = MovieRadarrStyleParser.parse_path(path);
            ParsedReleaseFileMetadata {
                title: parsed
                    .as_ref()
                    .and_then(|parsed| parsed.primary_movie_title().map(ToString::to_string)),
                season_number: None,
                episode_number: None,
                episode_end_number: None,
                absolute_episode_number: None,
                absolute_episode_end_number: None,
                air_date: None,
                quality: parsed
                    .as_ref()
                    .and_then(|parsed| parsed.quality.quality.clone()),
                language: parsed
                    .as_ref()
                    .and_then(|parsed| parsed.languages.first().cloned()),
                release_group: parsed
                    .as_ref()
                    .and_then(|parsed| parsed.release_group.clone()),
                confidence: parsed
                    .as_ref()
                    .map(|_| ReleaseConfidence::High)
                    .unwrap_or(ReleaseConfidence::ReviewRequired),
                reason: parsed
                    .is_none()
                    .then(|| "movie_file_path_unparseable".to_string()),
            }
        }
    }
}

async fn refine_movie_debrid_coverage(
    pool: &sqlx::AnyPool,
    release: &AcquisitionRelease,
    inspection: &DebridReleaseInspection,
    targets: &[crate::acquisition::subscriptions::AcquisitionTarget],
    file_ids: &HashMap<String, Uuid>,
) -> Result<DebridCoverageRefinement> {
    let file_inputs = inspection
        .files
        .iter()
        .map(|file| MovieReleaseFileSelectionInput {
            file_id: file.provider_file_id.clone(),
            path: file.path.clone(),
            size_bytes: file.size_bytes.and_then(u64_to_i64),
            selectable: file.selectable,
        })
        .collect::<Vec<_>>();
    let selection = select_movie_main_file(&file_inputs);
    let mut review_reasons = selection.review_reasons.clone();
    if targets.is_empty() {
        review_reasons.push("missing_movie_target".to_string());
    }
    review_reasons.sort();
    review_reasons.dedup();

    let confidence = if review_reasons.is_empty() {
        ReleaseConfidence::High
    } else {
        ReleaseConfidence::ReviewRequired
    };

    let selected_file_id = (confidence == ReleaseConfidence::High)
        .then(|| selection.selected_file_id.clone())
        .flatten();

    if let Some(target) = targets.first() {
        upsert_release_coverage(
            pool,
            NewAcquisitionReleaseCoverage {
                coverage_id: None,
                release_id: release.release_id,
                release_file_id: selected_file_id
                    .as_ref()
                    .and_then(|file_id| file_ids.get(file_id).copied()),
                target_id: target.target_id,
                coverage_kind: ReleaseCoverageKind::Movie,
                confidence,
                score: Some(1.0),
                reason: Some(if confidence == ReleaseConfidence::High {
                    "rrm5_debrid_movie_main_file".to_string()
                } else {
                    "rrm5_debrid_movie_file_list_review".to_string()
                }),
                state: if confidence == ReleaseConfidence::High {
                    ReleaseCoverageState::Planned
                } else {
                    ReleaseCoverageState::ReviewRequired
                },
                verified_by: Some("rrm5_debrid_movie_file_list".to_string()),
            },
        )
        .await?;
    }

    Ok(refinement_from_plan(
        ReleaseShape {
            release_kind: if confidence == ReleaseConfidence::High {
                ReleaseKind::Single
            } else {
                ReleaseKind::Unknown
            },
            resolver_kind: ReleaseResolverKind::MovieRadarrStyle,
            resolver_version: MOVIE_RADARR_STYLE_RESOLVER_VERSION.to_string(),
            confidence,
        },
        json!({
            "source": "debrid_provider_file_list",
            "providerImplementation": inspection.release.provider_implementation,
            "remoteReleaseId": inspection.release.remote_release_id,
            "movie": {
                "confidence": confidence,
                "coverageKind": ReleaseCoverageKind::Movie,
                "fileSelection": selection,
                "selectedFileId": selected_file_id,
                "mainCandidateCount": selection.main_candidate_count
            },
            "reviewReasons": review_reasons
        }),
        review_reasons,
        inspection.release.status,
    ))
}

async fn refine_tv_debrid_coverage(
    pool: &sqlx::AnyPool,
    release: &AcquisitionRelease,
    inspection: &DebridReleaseInspection,
    targets: &[crate::acquisition::subscriptions::AcquisitionTarget],
    file_ids: &HashMap<String, Uuid>,
) -> Result<DebridCoverageRefinement> {
    let resolver = TvSonarrStyleResolver;
    let parsed = resolver.parse_title(&release.release_title);
    let tv_targets = targets
        .iter()
        .filter_map(|target| {
            Some(TvTarget {
                target_id: target.target_id,
                target_key: target.target_key.clone(),
                season_number: target.season_number?,
                episode_number: target.episode_number?,
                air_date: target.air_date.clone(),
            })
        })
        .collect::<Vec<_>>();
    let files = inspection
        .files
        .iter()
        .map(|file| TvReleaseFileInput {
            file_id: file.provider_file_id.clone(),
            path: if file.basename.trim().is_empty() {
                file.path.clone()
            } else {
                file.basename.clone()
            },
            size_bytes: file.size_bytes.and_then(u64_to_i64),
            selectable: file.selectable,
        })
        .collect::<Vec<_>>();
    let plan = resolver.plan_coverage(
        &parsed,
        &tv_targets,
        &files,
        TvCoverageOptions {
            allow_partial_pack: false,
            file_selection_supported: inspection.capabilities.supports_file_selection,
        },
    );
    for entry in &plan.entries {
        upsert_release_coverage(
            pool,
            NewAcquisitionReleaseCoverage {
                coverage_id: None,
                release_id: release.release_id,
                release_file_id: entry
                    .release_file_id
                    .as_ref()
                    .and_then(|file_id| file_ids.get(file_id))
                    .copied(),
                target_id: entry.target_id,
                coverage_kind: entry.coverage_kind,
                confidence: plan.confidence,
                score: None,
                reason: Some("rr4e_tv_file_list_refinement".to_string()),
                state: entry.state,
                verified_by: Some("rr4e_tv_file_list".to_string()),
            },
        )
        .await?;
    }
    let review_reasons = plan
        .rejection_reasons
        .iter()
        .map(|reason| reason.as_str().to_string())
        .collect::<Vec<_>>();
    Ok(refinement_from_plan(
        ReleaseShape {
            release_kind: plan.release_kind,
            resolver_kind: plan.resolver_kind,
            resolver_version: plan.resolver_version.clone(),
            confidence: plan.confidence,
        },
        json!({
            "source": "debrid_provider_file_list",
            "providerImplementation": inspection.release.provider_implementation,
            "remoteReleaseId": inspection.release.remote_release_id,
            "tv": plan,
            "reviewReasons": review_reasons
        }),
        review_reasons,
        inspection.release.status,
    ))
}

async fn refine_anime_debrid_coverage(
    release: &AcquisitionRelease,
    options: &DebridSubmitOptions<'_>,
    inspection: &DebridReleaseInspection,
    subscription: Option<&crate::acquisition::subscriptions::AcquisitionSubscription>,
    targets: &[crate::acquisition::subscriptions::AcquisitionTarget],
    anime_matching: &AnimeMatchingService,
) -> Result<DebridCoverageRefinement> {
    let context = anime_scoring_context_from_release(release, targets);
    let selected_candidate = options
        .release_context
        .as_ref()
        .and_then(|context| context.selected_candidate.as_ref())
        .or(release.selected_candidate.as_ref());
    let candidate = AnimeCandidateInput {
        title: release.release_title.clone(),
        source_kind: release.source_kind.clone(),
        quality: selected_candidate_string(selected_candidate, "quality"),
        size_bytes: selected_candidate_u64(selected_candidate, "sizeBytes"),
        seeders: selected_candidate_u64(selected_candidate, "seeders")
            .and_then(|value| u32::try_from(value).ok()),
        cached_debrid: selected_candidate_bool(selected_candidate, "cachedDebrid"),
        rank: selected_candidate_u64(selected_candidate, "rank")
            .and_then(|value| u32::try_from(value).ok()),
        source_score: selected_candidate_f64(selected_candidate, "score"),
        supported_routes: selected_candidate_string_vec(selected_candidate, "supportedRoutes"),
        default_route: selected_candidate_string(selected_candidate, "defaultRoute"),
    };
    let files = inspection
        .files
        .iter()
        .map(|file| AnimeReleaseFileInput {
            file_key: file.provider_file_id.clone(),
            file_id: Some(file.provider_file_id.clone()),
            file_index: file.file_index,
            path: file.path.clone(),
            size_bytes: file.size_bytes.and_then(u64_to_i64),
            selectable: file.selectable,
        })
        .collect::<Vec<_>>();
    let deterministic_plan = plan_anime_file_coverage_with_options(
        &context,
        &candidate,
        &files,
        AnimeCoverageOptions {
            file_selection_supported: inspection.capabilities.supports_file_selection,
        },
    );
    let deterministic_plan =
        bind_exact_single_anime_provider_file(deterministic_plan, &context, &candidate, &files);
    let (deterministic_required_audio_satisfied, deterministic_audio_hard_mismatch) = subscription
        .map(|subscription| {
            let candidate = acquisition_candidate_from_debrid_file_list(
                release,
                selected_candidate,
                inspection,
            );
            let (assessment, satisfied) = assess_acquisition_anime_provider_file_audio(
                subscription,
                &candidate,
                &deterministic_plan.selected_file_keys,
            );
            (
                satisfied,
                !satisfied && assessment.state == LanguagePreferenceAssessmentState::Mismatch,
            )
        })
        .unwrap_or((true, false));
    let deterministic_state = debrid_anime_deterministic_state(
        &deterministic_plan,
        &files,
        &inspection.capabilities,
        targets,
    );
    let deterministic = ResolvedAnimeDebridCoverage {
        plan: deterministic_plan,
        model_audio_profile: None,
        model_audio_assessment: None,
        required_audio_satisfied: deterministic_required_audio_satisfied,
    };

    let (resolved, match_assist) = if deterministic_state == DeterministicMatchState::Definitive
        && deterministic_required_audio_satisfied
    {
        (
            deterministic,
            AnimeMatchAssistProvenance {
                source: AnimeMatchAssistSource::DeterministicFastPath,
                result: AnimeMatchAssistResult::Definitive,
                matcher_schema_version: ANIME_MATCH_SCHEMA_VERSION,
                request_fingerprint: None,
                reason: None,
                detail: None,
                runtime: None,
                latency_ms: 0,
            },
        )
    } else if let Some(subscription) = subscription {
        let acquisition_candidate =
            acquisition_candidate_from_debrid_file_list(release, selected_candidate, inspection);
        let acquisition_candidates = vec![acquisition_candidate];
        match acquisition_anime_match_batch_input(
            format!(
                "debrid-real-files:{}:{}",
                release.release_id, inspection.release.remote_release_id
            ),
            subscription,
            targets,
            &context,
            &acquisition_candidates,
        ) {
            Ok(batch_input) => {
                let context_for_model = context.clone();
                let files_for_validation = files.clone();
                let capabilities_for_validation = inspection.capabilities.clone();
                let file_selection_supported = inspection.capabilities.supports_file_selection;
                let subscription_for_model = subscription.clone();
                let targets_for_validation = targets.to_vec();
                let outcome = anime_matching
                    .match_or_fallback(
                        AnimeDeterministicResult {
                            value: deterministic,
                            state: DeterministicMatchState::Difficult,
                        },
                        batch_input,
                        move |_deterministic, request, matches, source_map| {
                            let mut plans =
                                model_derived_anime_coverage_plans_with_file_selection_support(
                                request,
                                &context_for_model,
                                &acquisition_candidates,
                                file_selection_supported,
                                matches,
                                source_map,
                            )?;
                            if plans.len() != 1 || plans[0].candidate_index != 0 {
                                bail!(
                                    "Debrid anime model response must resolve the inspected release exactly once"
                                );
                            }
                            let model = plans.remove(0);
                            let (audio_assessment, required_audio_satisfied) =
                                assess_acquisition_anime_model_audio_profile(
                                    &subscription_for_model,
                                    model.audio_profile,
                                );
                            if !required_audio_satisfied {
                                bail!(
                                    "Debrid anime model mapping did not satisfy the required audio policy"
                                );
                            }
                            if !anime_plan_ready_for_automatic_selection(
                                &model.plan,
                                &files_for_validation,
                                &capabilities_for_validation,
                                &targets_for_validation,
                            ) {
                                bail!(
                                    "Debrid anime model mapping did not prove an automatically selectable file set"
                                );
                            }
                            Ok(ResolvedAnimeDebridCoverage {
                                plan: model.plan,
                                model_audio_profile: Some(model.audio_profile),
                                model_audio_assessment: Some(json!({
                                    "state": audio_assessment.state.as_str(),
                                    "scoreDelta": audio_assessment.score_delta,
                                    "matchingAudio": audio_assessment.matching_audio,
                                    "matchingSubtitles": audio_assessment.matching_subtitles,
                                    "matchingProfiles": audio_assessment.matching_profiles,
                                    "desiredAudio": audio_assessment.desired_audio,
                                    "desiredSubtitles": audio_assessment.desired_subtitles,
                                    "desiredProfiles": audio_assessment.desired_profiles,
                                    "evidenceAudio": audio_assessment.evidence_audio,
                                    "evidenceSubtitles": audio_assessment.evidence_subtitles,
                                    "evidenceProfiles": audio_assessment.evidence_profiles,
                                    "requiredPreferenceSatisfied": required_audio_satisfied
                                })),
                                required_audio_satisfied,
                            })
                        },
                    )
                    .await;
                (outcome.value, outcome.provenance)
            }
            Err(error) => (
                deterministic,
                anime_match_invalid_request_fallback(error.to_string()),
            ),
        }
    } else {
        (
            deterministic,
            anime_match_invalid_request_fallback(
                "anime subscription was unavailable after provider file inspection".to_string(),
            ),
        )
    };

    // The model result is fully reference- and coverage-validated above. No
    // coverage row is written before this point, so invalid output cannot
    // partially replace the deterministic state.
    let ResolvedAnimeDebridCoverage {
        plan,
        model_audio_profile,
        model_audio_assessment,
        required_audio_satisfied,
    } = resolved;
    let used_model = match_assist.result == AnimeMatchAssistResult::Matched;
    let ready_for_selection = required_audio_satisfied
        && anime_plan_ready_for_automatic_selection(
            &plan,
            &files,
            &inspection.capabilities,
            targets,
        );
    let targets_by_key = targets
        .iter()
        .map(|target| (target.target_key.clone(), target.target_id))
        .collect::<HashMap<_, _>>();
    let mut anime_coverage_entries = Vec::new();
    if ready_for_selection {
        for entry in &plan.entries {
            let target_id = targets_by_key
                .get(&entry.target_key)
                .copied()
                .ok_or_else(|| {
                    anyhow!(
                        "Debrid anime mapping references unknown scoped target '{}'",
                        entry.target_key
                    )
                })?;
            let provider_file_id = entry.release_file_key.as_ref().cloned().ok_or_else(|| {
                anyhow!(
                    "Debrid anime mapping for '{}' has no available provider file",
                    entry.target_key
                )
            })?;
            anime_coverage_entries.push(AnimeDebridCoverageWrite {
                target_id,
                provider_file_id,
                coverage_kind: entry.coverage_kind,
                confidence: entry.confidence,
                score: entry.score,
                reason: entry.reason.clone(),
                state: entry.state,
                verified_by: if used_model {
                    "alm7_debrid_local_model_file_list".to_string()
                } else {
                    "rr4e_anime_file_list".to_string()
                },
            });
        }
    }
    let mut review_reasons = plan.review_reasons.clone();
    review_reasons.extend(plan.rejection_reasons.clone());
    review_reasons.sort();
    review_reasons.dedup();
    let score = score_anime_candidate(&context, &candidate);
    let diagnostics = anime_parser_diagnostics(&context, &score, Some(&plan));
    let anime_plan_evidence =
        serde_json::to_value(&plan).context("serializing anime Debrid file coverage evidence")?;
    let anime_plan_evidence = if ready_for_selection {
        anime_plan_evidence
    } else {
        sanitize_anime_automatic_resolution_evidence(anime_plan_evidence)
    };
    let diagnostics = if ready_for_selection {
        diagnostics
    } else {
        sanitize_anime_automatic_resolution_evidence(diagnostics)
    };
    let request_scope_evidence = selected_candidate.and_then(|candidate| {
        candidate
            .get("requestScopeEvidence")
            .or_else(|| candidate.get("request_scope_evidence"))
            .cloned()
    });
    let suppress_automatic_rediscovery = deterministic_audio_hard_mismatch
        || anime_debrid_retry_suppresses_rediscovery(&match_assist);
    let mapping_diagnostics = sanitize_anime_automatic_resolution_evidence(json!(review_reasons));
    let coverage_plan = json!({
        "source": "debrid_provider_file_list",
        "providerImplementation": inspection.release.provider_implementation,
        "remoteReleaseId": inspection.release.remote_release_id,
        "anime": anime_plan_evidence,
        "diagnostics": diagnostics,
        "animeMatchAssist": match_assist,
        "modelAudioProfile": model_audio_profile,
        "modelAudioAssessment": model_audio_assessment,
        "requestScopeEvidence": request_scope_evidence,
        "automaticResolution": {
            "status": if ready_for_selection { "resolved" } else { "pending" },
            "selectionPolicyReady": ready_for_selection,
            "requiredAudioSatisfied": required_audio_satisfied,
            "scopedTargetKeys": targets.iter().map(|target| target.target_key.clone()).collect::<Vec<_>>()
        },
        "mappingDiagnostics": mapping_diagnostics
    });
    let shape = ReleaseShape {
        release_kind: plan.release_kind,
        resolver_kind: plan.resolver_kind,
        resolver_version: plan.resolver_version.clone(),
        confidence: plan.confidence,
    };
    if ready_for_selection {
        let mut refinement =
            refinement_from_plan(shape, coverage_plan, Vec::new(), inspection.release.status);
        refinement.anime_coverage_entries = anime_coverage_entries;
        return Ok(refinement);
    }

    Ok(DebridCoverageRefinement {
        shape,
        state: AcquisitionReleaseState::Staging,
        state_reason: Some(
            "Anime file matching retained the deterministic fallback and will retry automatically."
                .to_string(),
        ),
        job_state: ReleaseJobState::Staging,
        job_state_reason: Some(
            "Anime provider-file resolution is pending an automatic retry.".to_string(),
        ),
        coverage_plan: Some(coverage_plan.clone()),
        apply_file_selection_policy: false,
        automatic_retry: Some(AnimeDebridAutomaticRetry {
            target_ids: targets
                .iter()
                .filter(|target| {
                    !matches!(
                        target.state,
                        AcquisitionTargetState::Imported | AcquisitionTargetState::Excluded
                    )
                })
                .map(|target| target.target_id)
                .collect(),
            reason_code: "anime_debrid_file_mapping_unresolved".to_string(),
            suppress_automatic_rediscovery,
            coverage_plan: Some(coverage_plan),
        }),
        anime_coverage_entries: Vec::new(),
    })
}

#[derive(Debug)]
struct ResolvedAnimeDebridCoverage {
    plan: AnimeFileCoveragePlan,
    model_audio_profile: Option<AnimeMatchAudioProfile>,
    model_audio_assessment: Option<Value>,
    required_audio_satisfied: bool,
}

fn debrid_anime_deterministic_state(
    plan: &AnimeFileCoveragePlan,
    files: &[AnimeReleaseFileInput],
    capabilities: &DebridProviderCapabilities,
    targets: &[crate::acquisition::subscriptions::AcquisitionTarget],
) -> DeterministicMatchState {
    if acquisition_anime_deterministic_state(plan) == DeterministicMatchState::Definitive
        && anime_plan_ready_for_automatic_selection(plan, files, capabilities, targets)
    {
        DeterministicMatchState::Definitive
    } else {
        DeterministicMatchState::Difficult
    }
}

fn anime_plan_ready_for_automatic_selection(
    plan: &AnimeFileCoveragePlan,
    files: &[AnimeReleaseFileInput],
    capabilities: &DebridProviderCapabilities,
    targets: &[crate::acquisition::subscriptions::AcquisitionTarget],
) -> bool {
    let wanted_target_keys = targets
        .iter()
        .map(|target| target.target_key.as_str())
        .collect::<BTreeSet<_>>();
    let planned_target_keys = plan
        .entries
        .iter()
        .map(|entry| entry.target_key.as_str())
        .collect::<BTreeSet<_>>();
    if plan.confidence != ReleaseConfidence::High
        || plan.entries.is_empty()
        || wanted_target_keys.is_empty()
        || wanted_target_keys.len() != targets.len()
        || wanted_target_keys != planned_target_keys
        || planned_target_keys.len() != plan.entries.len()
        || !plan.review_reasons.is_empty()
        || !plan.rejection_reasons.is_empty()
        || plan.entries.iter().any(|entry| {
            entry.confidence != ReleaseConfidence::High
                || matches!(
                    entry.state,
                    ReleaseCoverageState::ReviewRequired | ReleaseCoverageState::Rejected
                )
        })
    {
        return false;
    }

    let selectable_media = files
        .iter()
        .filter(|file| {
            file.selectable
                && is_debrid_media_file(&file.path)
                && !is_debrid_sample_or_extra_file(&file.path)
        })
        .map(|file| file.file_key.as_str())
        .collect::<BTreeSet<_>>();
    if selectable_media.is_empty() {
        return false;
    }
    let selected = plan
        .selected_file_keys
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if selected.len() != plan.selected_file_keys.len() || !selected.is_subset(&selectable_media) {
        return false;
    }
    if selected.is_empty() {
        return false;
    }
    if !capabilities.supports_file_selection && selected != selectable_media {
        return false;
    }
    let mapped_files = plan
        .entries
        .iter()
        .filter_map(|entry| entry.release_file_key.as_deref())
        .collect::<BTreeSet<_>>();
    mapped_files == selected
        && plan
            .entries
            .iter()
            .all(|entry| entry.release_file_key.is_some())
}

fn anime_match_invalid_request_fallback(detail: String) -> AnimeMatchAssistProvenance {
    AnimeMatchAssistProvenance {
        source: AnimeMatchAssistSource::DeterministicFallback,
        result: AnimeMatchAssistResult::Fallback,
        matcher_schema_version: ANIME_MATCH_SCHEMA_VERSION,
        request_fingerprint: None,
        reason: Some(AnimeMatchFallbackReason::InvalidRequest),
        detail: Some(detail),
        runtime: None,
        latency_ms: 0,
    }
}

fn anime_debrid_retry_suppresses_rediscovery(match_assist: &AnimeMatchAssistProvenance) -> bool {
    match_assist.result != AnimeMatchAssistResult::Fallback
}

fn sanitize_anime_automatic_resolution_evidence(value: Value) -> Value {
    match value {
        Value::Object(object) => Value::Object(
            object
                .into_iter()
                .map(|(key, value)| {
                    let key = key
                        .replace("Review", "Unresolved")
                        .replace("review", "unresolved");
                    (key, sanitize_anime_automatic_resolution_evidence(value))
                })
                .collect(),
        ),
        Value::Array(values) => Value::Array(
            values
                .into_iter()
                .map(sanitize_anime_automatic_resolution_evidence)
                .collect(),
        ),
        Value::String(value) => Value::String(
            value
                .replace("review_required", "unresolved")
                .replace("requires_review", "unresolved")
                .replace("requires review", "unresolved")
                .replace("review required", "automatic retry required")
                .replace("waiting for review", "waiting for automatic retry")
                .replace("_review", "_unresolved")
                .replace("Review", "Unresolved")
                .replace("review", "unresolved"),
        ),
        value => value,
    }
}

fn acquisition_candidate_from_debrid_file_list(
    release: &AcquisitionRelease,
    selected_candidate: Option<&Value>,
    inspection: &DebridReleaseInspection,
) -> AcquisitionCandidate {
    let mut supported_routes = selected_candidate_string_vec(selected_candidate, "supportedRoutes");
    if supported_routes.is_empty() {
        supported_routes.push(DEBRID_DEFAULT_LOGICAL_ID.to_string());
    }
    AcquisitionCandidate {
        id: selected_candidate_string(selected_candidate, "id"),
        title: release.release_title.clone(),
        source: release.source.clone(),
        source_kind: release.source_kind.clone(),
        info_hash: release.info_hash.clone(),
        file_index: selected_candidate_u64(selected_candidate, "fileIndex")
            .and_then(|value| i64::try_from(value).ok()),
        quality: selected_candidate_string(selected_candidate, "quality"),
        size_bytes: selected_candidate_u64(selected_candidate, "sizeBytes"),
        seeders: selected_candidate_u64(selected_candidate, "seeders")
            .and_then(|value| u32::try_from(value).ok()),
        language: selected_candidate_string(selected_candidate, "language"),
        cached_debrid: selected_candidate_bool(selected_candidate, "cachedDebrid"),
        rank: selected_candidate_u64(selected_candidate, "rank")
            .and_then(|value| u32::try_from(value).ok()),
        score: release
            .score
            .or_else(|| selected_candidate_f64(selected_candidate, "score")),
        score_badges: Vec::new(),
        files: inspection
            .files
            .iter()
            .map(|file| AcquisitionCandidateFile {
                file_id: Some(file.provider_file_id.clone()),
                file_index: file.file_index,
                path: file.path.clone(),
                size_bytes: file.size_bytes,
                selectable: Some(file.selectable),
            })
            .collect(),
        supported_routes,
        default_route: selected_candidate_string(selected_candidate, "defaultRoute")
            .or_else(|| Some(DEBRID_DEFAULT_LOGICAL_ID.to_string())),
        raw: selected_candidate
            .and_then(|candidate| candidate.get("raw"))
            .cloned()
            .or_else(|| selected_candidate.cloned()),
    }
}

fn anime_scoring_context_from_release(
    release: &AcquisitionRelease,
    targets: &[crate::acquisition::subscriptions::AcquisitionTarget],
) -> AnimeCandidateScoringContext {
    let mut aliases = Vec::new();
    let mut scoped_aliases = BTreeMap::new();
    let mut graph_fingerprints = BTreeSet::new();
    push_unique_alias(&mut aliases, &release.title);
    for target in targets {
        push_unique_alias(&mut aliases, &target.title);
        if let Some(metadata) = target.metadata.as_ref() {
            if let Some(fingerprint) = metadata_json_string(Some(metadata), "graphFingerprint") {
                graph_fingerprints.insert(fingerprint);
            }
            for key in ["aliases", "titles", "anilistTitles"] {
                if let Some(values) = metadata.get(key).and_then(Value::as_array) {
                    for value in values.iter().filter_map(Value::as_str) {
                        push_unique_alias(&mut aliases, value);
                    }
                }
            }
            for key in ["scopedAliases", "scoped_aliases"] {
                let Some(values) = metadata.get(key).and_then(Value::as_array) else {
                    continue;
                };
                for value in values {
                    let Ok(alias) = serde_json::from_value::<AnimeScopedAlias>(value.clone())
                    else {
                        continue;
                    };
                    let display = alias.display.trim();
                    if display.is_empty()
                        || (alias.season_number.is_none() && alias.anilist_season_id.is_none())
                    {
                        continue;
                    }
                    let key = format!(
                        "{}:{}:{}:{}",
                        display.to_ascii_lowercase(),
                        alias.source,
                        alias.season_number.unwrap_or_default(),
                        alias.anilist_season_id.as_deref().unwrap_or_default()
                    );
                    scoped_aliases.entry(key).or_insert(alias);
                }
            }
        }
    }
    let graph_fingerprint = match graph_fingerprints.len() {
        1 => graph_fingerprints.first().cloned(),
        0 => release
            .coverage_plan
            .as_ref()
            .and_then(find_anime_graph_fingerprint),
        _ => None,
    };
    AnimeCandidateScoringContext {
        graph_fingerprint,
        aliases,
        scoped_aliases: scoped_aliases.into_values().collect(),
        targets: targets
            .iter()
            .map(|target| {
                let metadata = target.metadata.as_ref();
                AnimeCandidateTarget {
                    target_key: target.target_key.clone(),
                    canonical_key: metadata_json_string(metadata, "targetCanonicalKey"),
                    title: target.title.clone(),
                    season_number: target.season_number,
                    anilist_season_id: metadata_json_string(metadata, "anilistSeasonId"),
                    episode_number: target.episode_number,
                    absolute_episode_number: target.absolute_episode_number,
                    tvdb_episode_id: metadata_json_string(metadata, "tvdbEpisodeId"),
                    anidb_episode_id: metadata_json_string(metadata, "anidbEpisodeId"),
                }
            })
            .collect(),
    }
}

fn find_anime_graph_fingerprint(value: &Value) -> Option<String> {
    for key in ["graphFingerprint", "graph_fingerprint"] {
        if let Some(fingerprint) = json_scalar_string(value.get(key)) {
            return Some(fingerprint);
        }
    }
    for key in [
        "anime",
        "animeCoveragePlan",
        "coveragePlan",
        "previousCoveragePlan",
        "debridCoveragePlan",
    ] {
        if let Some(fingerprint) = value.get(key).and_then(find_anime_graph_fingerprint) {
            return Some(fingerprint);
        }
    }
    None
}

fn push_unique_alias(aliases: &mut Vec<String>, value: &str) {
    let trimmed = value.trim();
    if !trimmed.is_empty() && !aliases.iter().any(|alias| alias == trimmed) {
        aliases.push(trimmed.to_string());
    }
}

fn refinement_from_plan(
    shape: ReleaseShape,
    coverage_plan: Value,
    review_reasons: Vec<String>,
    status: DebridReleaseStatus,
) -> DebridCoverageRefinement {
    if shape.confidence == ReleaseConfidence::ReviewRequired || !review_reasons.is_empty() {
        DebridCoverageRefinement {
            shape,
            state: AcquisitionReleaseState::ReviewRequired,
            state_reason: Some(format!(
                "Debrid file list requires review: {}",
                review_reasons.join(",")
            )),
            job_state: ReleaseJobState::Staging,
            job_state_reason: Some("Debrid file selection is waiting for review.".to_string()),
            coverage_plan: Some(coverage_plan),
            apply_file_selection_policy: true,
            automatic_retry: None,
            anime_coverage_entries: Vec::new(),
        }
    } else {
        DebridCoverageRefinement {
            shape,
            state: acquisition_ready_state_for_debrid_status(status),
            state_reason: Some(
                "Debrid file list coverage resolved with high confidence.".to_string(),
            ),
            job_state: release_job_state_for_debrid_status(status),
            job_state_reason: Some(
                "Debrid file list coverage resolved with high confidence.".to_string(),
            ),
            coverage_plan: Some(coverage_plan),
            apply_file_selection_policy: true,
            automatic_retry: None,
            anime_coverage_entries: Vec::new(),
        }
    }
}

fn refinement_from_debrid_status(status: DebridReleaseStatus) -> DebridCoverageRefinement {
    DebridCoverageRefinement {
        shape: ReleaseShape::default(),
        state: acquisition_state_for_debrid_status(status),
        state_reason: Some("Debrid release staged.".to_string()),
        job_state: release_job_state_for_debrid_status(status),
        job_state_reason: Some("Debrid release staged.".to_string()),
        coverage_plan: None,
        apply_file_selection_policy: true,
        automatic_retry: None,
        anime_coverage_entries: Vec::new(),
    }
}

fn release_job_state_for_debrid_status(status: DebridReleaseStatus) -> ReleaseJobState {
    match status {
        DebridReleaseStatus::Downloaded
        | DebridReleaseStatus::Selected
        | DebridReleaseStatus::Transferring => ReleaseJobState::Downloading,
        DebridReleaseStatus::Materializing => ReleaseJobState::Materializing,
        DebridReleaseStatus::Completed => ReleaseJobState::Completed,
        DebridReleaseStatus::Failed => ReleaseJobState::Failed,
        DebridReleaseStatus::Cancelled => ReleaseJobState::Cancelled,
        _ => ReleaseJobState::Staging,
    }
}

fn release_job_state_for_job_status(status: Option<&str>) -> ReleaseJobState {
    match status.unwrap_or_default() {
        "debrid_downloaded" | "debrid_downloading" | "rd_downloaded" | "rd_downloading" => {
            ReleaseJobState::Downloading
        }
        "materializing" => ReleaseJobState::Materializing,
        "completed" => ReleaseJobState::Completed,
        "failed" => ReleaseJobState::Failed,
        "cancelled" => ReleaseJobState::Cancelled,
        _ => ReleaseJobState::Staging,
    }
}

fn acquisition_state_for_debrid_status(status: DebridReleaseStatus) -> AcquisitionReleaseState {
    match status {
        DebridReleaseStatus::Downloaded
        | DebridReleaseStatus::Selected
        | DebridReleaseStatus::Transferring => AcquisitionReleaseState::Downloading,
        DebridReleaseStatus::Materializing => AcquisitionReleaseState::Materializing,
        DebridReleaseStatus::Completed => AcquisitionReleaseState::Completed,
        DebridReleaseStatus::ReviewRequired => AcquisitionReleaseState::ReviewRequired,
        DebridReleaseStatus::Failed => AcquisitionReleaseState::Failed,
        DebridReleaseStatus::Cancelled => AcquisitionReleaseState::Cancelled,
        _ => AcquisitionReleaseState::Staging,
    }
}

fn acquisition_ready_state_for_debrid_status(
    status: DebridReleaseStatus,
) -> AcquisitionReleaseState {
    match status {
        DebridReleaseStatus::Downloaded
        | DebridReleaseStatus::Selected
        | DebridReleaseStatus::Transferring => AcquisitionReleaseState::Downloading,
        DebridReleaseStatus::Failed => AcquisitionReleaseState::Failed,
        DebridReleaseStatus::Cancelled => AcquisitionReleaseState::Cancelled,
        DebridReleaseStatus::Completed => AcquisitionReleaseState::Completed,
        _ => AcquisitionReleaseState::Ready,
    }
}

fn acquisition_state_for_job_status(status: Option<&str>) -> AcquisitionReleaseState {
    match status.unwrap_or_default() {
        "debrid_downloaded" | "debrid_downloading" | "rd_downloaded" | "rd_downloading" => {
            AcquisitionReleaseState::Downloading
        }
        "materializing" => AcquisitionReleaseState::Materializing,
        "completed" => AcquisitionReleaseState::Completed,
        "failed" => AcquisitionReleaseState::Failed,
        "cancelled" => AcquisitionReleaseState::Cancelled,
        _ => AcquisitionReleaseState::Staging,
    }
}

fn metadata_json_string(metadata: Option<&Value>, key: &str) -> Option<String> {
    json_scalar_string(metadata?.get(key))
}

fn json_scalar_string(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
    .map(|value| value.trim().to_string())
    .filter(|value| !value.is_empty())
}

fn selected_candidate_string(candidate: Option<&Value>, key: &str) -> Option<String> {
    candidate?
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn selected_candidate_string_vec(candidate: Option<&Value>, key: &str) -> Vec<String> {
    candidate
        .and_then(|candidate| candidate.get(key))
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn selected_candidate_u64(candidate: Option<&Value>, key: &str) -> Option<u64> {
    candidate?.get(key).and_then(Value::as_u64)
}

fn selected_candidate_f64(candidate: Option<&Value>, key: &str) -> Option<f64> {
    candidate?.get(key).and_then(Value::as_f64)
}

fn selected_candidate_bool(candidate: Option<&Value>, key: &str) -> Option<bool> {
    candidate?.get(key).and_then(Value::as_bool)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DebridSelectionDecisionStatus {
    Approved,
    ReviewRequired,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DebridFileSelectionDecision {
    status: DebridSelectionDecisionStatus,
    selected_file_ids: Vec<String>,
    skipped_file_ids: Vec<String>,
    provider_selection_ids: Vec<String>,
    target_file_selections: Vec<DebridTargetFileSelection>,
    review_reasons: Vec<String>,
    policy_version: String,
    coverage_fingerprint: String,
    select_all: bool,
    select_all_approved: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DebridTargetFileSelection {
    target_id: Uuid,
    provider_file_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DebridCoverageTarget {
    target_id: Uuid,
    season_number: Option<i32>,
    episode_number: Option<i32>,
    episode_end_number: Option<i32>,
    absolute_episode_number: Option<i32>,
    absolute_episode_end_number: Option<i32>,
}

impl DebridFileSelectionDecision {
    fn is_approved(&self) -> bool {
        self.status == DebridSelectionDecisionStatus::Approved
    }

    fn with_inferred_target_file_selections(
        mut self,
        release: &AcquisitionRelease,
        files: &[AcquisitionReleaseFile],
        coverage: &[AcquisitionReleaseCoverage],
    ) -> Self {
        if !self.is_approved() || !self.target_file_selections.is_empty() {
            return self;
        }
        self.target_file_selections =
            infer_debrid_target_file_selections(release, files, coverage, &self.selected_file_ids);
        self
    }
}

async fn apply_debrid_file_selection_policy<A: DebridProviderAdapter + ?Sized>(
    pool: &sqlx::AnyPool,
    adapter: &A,
    job_id: Uuid,
    release: &AcquisitionRelease,
    inspection: &DebridReleaseInspection,
    allow_automatic_retry: bool,
) -> Result<Option<DebridReleaseInspection>> {
    if release.media_type == MediaType::Anime {
        let Some(provider_id) = release.selected_provider_id else {
            mark_stale_anime_debrid_provider_job(
                pool,
                job_id,
                "Anime Debrid selection lost its provider ownership.",
            )
            .await?;
            return Ok(None);
        };
        if !anime_debrid_attempt_is_current(
            pool,
            release,
            provider_id,
            job_id,
            &inspection.release.remote_release_id,
        )
        .await?
        {
            mark_stale_anime_debrid_provider_job(
                pool,
                job_id,
                "Superseded by a newer anime Debrid attempt.",
            )
            .await?;
            return Ok(None);
        }
    }
    let files = list_release_files(pool, release.release_id).await?;
    let coverage = list_release_coverage(pool, release.release_id).await?;
    let decision = decide_debrid_file_selection(release, &files, &coverage, inspection);
    if release.media_type == MediaType::Anime && !decision.is_approved() {
        let retry = AnimeDebridAutomaticRetry {
            target_ids: coverage.iter().map(|entry| entry.target_id).collect(),
            reason_code: "anime_debrid_file_selection_unapproved".to_string(),
            suppress_automatic_rediscovery: true,
            coverage_plan: release.coverage_plan.clone(),
        };
        if allow_automatic_retry {
            if !stage_anime_debrid_retry_disposition(pool, job_id, &retry).await? {
                mark_stale_anime_debrid_provider_job(
                    pool,
                    job_id,
                    "Superseded while staging anime Debrid selection retry.",
                )
                .await?;
                return Ok(None);
            }
            persist_anime_debrid_retry_with_adapter(
                pool,
                adapter,
                job_id,
                release,
                &inspection.release.remote_release_id,
                &inspection.release.provider_implementation,
                &retry,
            )
            .await?;
            return Ok(None);
        }
        if !stage_anime_debrid_retry_disposition(pool, job_id, &retry).await? {
            mark_stale_anime_debrid_provider_job(
                pool,
                job_id,
                "Superseded while staging anime Debrid selection retry.",
            )
            .await?;
        }
        return Ok(None);
    }
    if !persist_debrid_selection_decision(pool, job_id, release, &files, &coverage, &decision)
        .await?
    {
        mark_stale_anime_debrid_provider_job(
            pool,
            job_id,
            "Superseded while committing anime Debrid selection intent.",
        )
        .await?;
        return Ok(None);
    }
    if !decision.is_approved() {
        return Ok(None);
    }
    if !inspection.capabilities.supports_file_selection {
        if release.media_type == MediaType::Anime {
            if !mark_debrid_selection_applied(pool, release, job_id, inspection).await? {
                mark_stale_anime_debrid_provider_job(
                    pool,
                    job_id,
                    "Superseded while applying implicit anime Debrid selection.",
                )
                .await?;
                return Ok(None);
            }
            return Ok(Some(inspection.clone()));
        }
        update_release_state(
            pool,
            release.release_id,
            acquisition_state_for_debrid_status(inspection.release.status),
            "Debrid provider requires no explicit selection for the exact mapped file set.",
            None,
        )
        .await?;
        update_debrid_release_job_selection_state(
            pool,
            release.release_id,
            job_id,
            release_job_state_for_debrid_status(inspection.release.status),
            "Debrid provider requires no explicit selection for the exact mapped file set.",
        )
        .await?;
        return Ok(Some(inspection.clone()));
    }

    let selected = adapter
        .select_files(
            &inspection.release.remote_release_id,
            &decision.provider_selection_ids,
        )
        .await
        .with_context(|| {
            format!(
                "selecting debrid files for remote release '{}'",
                inspection.release.remote_release_id
            )
        })?;
    if release.media_type == MediaType::Anime
        && let Some(provider_id) = release.selected_provider_id
        && !anime_debrid_attempt_is_current(
            pool,
            release,
            provider_id,
            job_id,
            &inspection.release.remote_release_id,
        )
        .await?
    {
        mark_stale_anime_debrid_provider_job(
            pool,
            job_id,
            "Superseded during anime Debrid provider-file selection.",
        )
        .await?;
        return Ok(None);
    }
    if !update_debrid_job_from_inspection(pool, job_id, &selected).await? {
        return Ok(None);
    }
    if consume_failed_anime_debrid_inspection(pool, adapter, job_id, &selected).await? {
        return Ok(None);
    }
    if release.media_type != MediaType::Anime {
        persist_debrid_release_files(pool, release, &selected.files).await?;
    }
    if !mark_debrid_selection_applied(pool, release, job_id, &selected).await? {
        mark_stale_anime_debrid_provider_job(
            pool,
            job_id,
            "Superseded while committing anime Debrid provider selection.",
        )
        .await?;
        return Ok(None);
    }
    Ok(Some(selected))
}

async fn persist_anime_debrid_retry_with_adapter<A: DebridProviderAdapter + ?Sized>(
    pool: &sqlx::AnyPool,
    adapter: &A,
    job_id: Uuid,
    release: &AcquisitionRelease,
    remote_release_id: &str,
    provider_implementation: &str,
    retry: &AnimeDebridAutomaticRetry,
) -> Result<()> {
    let remote_release_id = remote_release_id.trim();
    let now = chrono::Utc::now();
    let reason =
        "Anime files could not be mapped safely. Elixir will try the next release automatically.";
    let initial_claim = claim_anime_debrid_retry_attempt(
        pool,
        job_id,
        release,
        remote_release_id,
        retry,
        json!({
            "status": "pending",
            "deleted": false,
            "policyVersion": "alm7-debrid-owned-cleanup-v1"
        }),
        now,
        reason,
    )
    .await?;
    if matches!(initial_claim, AnimeDebridRetryClaim::LostOwnership) {
        finish_anime_debrid_retry_claim(pool, job_id, release, initial_claim, now, reason).await?;
        return Ok(());
    }

    // Cleanup is authorized only after the exact attempt is durably claimed.
    // The target remains bound to this failed attempt until cleanup evidence
    // is persisted, so the scheduler cannot create a normal replacement in
    // the external-call window.
    let cleanup = if remote_release_id.is_empty() {
        json!({
            "status": "not_applicable",
            "deleted": false,
            "reason": "missing_remote_release_id"
        })
    } else if anime_debrid_remote_release_is_shared_with_active_attempt(
        pool,
        job_id,
        remote_release_id,
    )
    .await?
    {
        json!({
            "status": "retained_shared_active_attempt",
            "deleted": false,
            "reason": "remote_release_id_is_owned_by_another_active_attempt"
        })
    } else {
        match adapter.delete_release(remote_release_id).await {
            Ok(deleted) => json!({
                "status": if deleted { "deleted" } else { "already_absent" },
                "deleted": deleted
            }),
            Err(error) => {
                tracing::warn!(
                    debrid_job_id = %job_id,
                    remote_release_id,
                    provider_implementation,
                    "debrid anime automatic-retry cleanup failed: {error}"
                );
                json!({
                    "status": "delete_failed",
                    "deleted": false,
                    "error": error.to_string()
                })
            }
        }
    };
    let final_claim = claim_anime_debrid_retry_attempt(
        pool,
        job_id,
        release,
        remote_release_id,
        retry,
        sanitize_anime_automatic_resolution_evidence(cleanup),
        chrono::Utc::now(),
        reason,
    )
    .await?;
    finish_anime_debrid_retry_claim(pool, job_id, release, final_claim, now, reason).await
}

async fn anime_debrid_remote_release_is_shared_with_active_attempt(
    pool: &sqlx::AnyPool,
    job_id: Uuid,
    remote_release_id: &str,
) -> Result<bool> {
    let Some(job) = load_debrid_job(pool, job_id).await? else {
        return Ok(true);
    };
    let active_provider_jobs = sqlx::query_scalar::<sqlx::Any, i64>(
        "SELECT COUNT(*)
         FROM debrid_download_jobs
         WHERE job_id <> $1
           AND provider_id = $2
           AND instance_id = $3
           AND status NOT IN ('completed', 'failed', 'cancelled')
           AND (remote_release_id = $4 OR remote_torrent_id = $4)",
    )
    .bind(job_id.to_string())
    .bind(job.provider_id.to_string())
    .bind(job.instance_id.to_string())
    .bind(remote_release_id)
    .fetch_one(pool)
    .await
    .context("checking shared active Debrid provider cleanup ownership")?;
    if active_provider_jobs > 0 {
        return Ok(true);
    }
    let active_release_jobs = sqlx::query_scalar::<sqlx::Any, i64>(
        "SELECT COUNT(*)
         FROM acquisition_release_jobs
         WHERE active = 1
           AND provider_id = $1
           AND COALESCE(download_id, '') <> $2
           AND remote_release_id = $3",
    )
    .bind(job.provider_id.to_string())
    .bind(job_id.to_string())
    .bind(remote_release_id)
    .fetch_one(pool)
    .await
    .context("checking shared active Debrid release-job cleanup ownership")?;
    Ok(active_release_jobs > 0)
}

async fn anime_debrid_runtime_error_retry_disposition(
    pool: &sqlx::AnyPool,
    job_id: Uuid,
    release: &AcquisitionRelease,
    reason_code: &str,
    error: &anyhow::Error,
) -> Result<AnimeDebridAutomaticRetry> {
    debug_assert_eq!(release.media_type, MediaType::Anime);
    let job_download_id = job_id.to_string();
    let mut bound_target_ids = BTreeSet::new();
    if let Some(download_id) = release.download_id.as_deref() {
        bound_target_ids.extend(target_ids_for_download_id(pool, download_id).await?);
    }
    if release.download_id.as_deref() != Some(job_download_id.as_str()) {
        bound_target_ids.extend(target_ids_for_download_id(pool, &job_download_id).await?);
    }

    let target_ids = if let Some(subscription_id) = release.subscription_id {
        let targets = list_subscription_targets(pool, subscription_id).await?;
        let coverage = list_release_coverage(pool, release.release_id).await?;
        debrid_release_scoped_targets(
            release,
            &targets,
            &coverage,
            &bound_target_ids.iter().copied().collect::<Vec<_>>(),
        )
        .into_iter()
        .filter(|target| {
            !matches!(
                target.state,
                AcquisitionTargetState::Imported | AcquisitionTargetState::Excluded
            )
        })
        .map(|target| target.target_id)
        .collect::<Vec<_>>()
    } else {
        let mut target_ids = Vec::new();
        for target_id in bound_target_ids {
            if get_target(pool, target_id).await?.is_some_and(|target| {
                !matches!(
                    target.state,
                    AcquisitionTargetState::Imported | AcquisitionTargetState::Excluded
                )
            }) {
                target_ids.push(target_id);
            }
        }
        target_ids
    };
    let error_evidence = sanitize_anime_automatic_resolution_evidence(json!({
        "status": "retryable",
        "reason": reason_code,
        "message": error.to_string(),
        "retryDisposition": "automatic",
        "policyVersion": "alm7-debrid-anime-runtime-retry-v1"
    }));
    let coverage_plan = merge_debrid_evidence_object(
        release.coverage_plan.clone(),
        "automaticResolutionError",
        error_evidence,
    );
    Ok(AnimeDebridAutomaticRetry {
        target_ids,
        reason_code: reason_code.to_string(),
        suppress_automatic_rediscovery: false,
        coverage_plan: Some(coverage_plan),
    })
}

async fn persist_anime_debrid_retry(
    pool: &sqlx::AnyPool,
    job_id: Uuid,
    release: &AcquisitionRelease,
    remote_release_id: &str,
    retry: &AnimeDebridAutomaticRetry,
    cleanup: Value,
) -> Result<()> {
    debug_assert_eq!(release.media_type, MediaType::Anime);
    let now = chrono::Utc::now();
    let reason =
        "Anime files could not be mapped safely. Elixir will try the next release automatically.";

    let claim = claim_anime_debrid_retry_attempt(
        pool,
        job_id,
        release,
        remote_release_id,
        retry,
        cleanup,
        now,
        reason,
    )
    .await?;
    finish_anime_debrid_retry_claim(pool, job_id, release, claim, now, reason).await
}

async fn finish_anime_debrid_retry_claim(
    pool: &sqlx::AnyPool,
    job_id: Uuid,
    release: &AcquisitionRelease,
    claim: AnimeDebridRetryClaim,
    now: chrono::DateTime<chrono::Utc>,
    reason: &str,
) -> Result<()> {
    let AnimeDebridRetryClaim::Owned {
        target_ids,
        provider_id,
    } = claim
    else {
        tracing::info!(
            debrid_job_id = %job_id,
            release_id = %release.release_id,
            "ignored stale anime Debrid retry after release ownership moved to a newer attempt"
        );
        // Job A is still ours to terminalize even though the shared release is
        // not. Consuming its durable marker prevents the materializer from
        // selecting the stale job forever and repeatedly deleting remote A.
        mark_anime_debrid_retry_job_failed(pool, job_id, reason).await?;
        return Ok(());
    };

    for target_id in target_ids {
        let retry_after = now
            + chrono::Duration::seconds(
                ANIME_DEBRID_CANDIDATE_RETRY_SECONDS + i64::from(target_id.as_bytes()[0] % 15),
            );
        reset_anime_debrid_target_for_candidate_retry_if_owned(
            pool,
            target_id,
            job_id,
            provider_id,
            reason,
            retry_after,
        )
        .await?;
    }

    // Terminalize the provider job last. Until this write succeeds, the
    // durable disposition remains visible to the materializer and every step
    // above is safe to replay after a crash. This is an automatic routing
    // outcome, so bypass generic failure classification and its review policy.
    mark_anime_debrid_retry_job_failed(pool, job_id, reason).await?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum AnimeDebridRetryClaim {
    /// The exact attempt owns the release, including idempotent replays of an
    /// already-claimed failure. Only these target IDs may be reset.
    Owned {
        target_ids: BTreeSet<Uuid>,
        provider_id: Uuid,
    },
    /// A newer attempt owns the release. The delayed disposition must not
    /// mutate that release, its coverage, or its targets.
    LostOwnership,
}

async fn claim_anime_debrid_retry_attempt(
    pool: &sqlx::AnyPool,
    job_id: Uuid,
    release: &AcquisitionRelease,
    remote_release_id: &str,
    retry: &AnimeDebridAutomaticRetry,
    cleanup: Value,
    now: chrono::DateTime<chrono::Utc>,
    reason: &str,
) -> Result<AnimeDebridRetryClaim> {
    let Some(job) = load_debrid_job(pool, job_id).await? else {
        return Ok(AnimeDebridRetryClaim::LostOwnership);
    };
    if job.release_id != Some(release.release_id) {
        return Ok(AnimeDebridRetryClaim::LostOwnership);
    }

    let job_id_string = job_id.to_string();
    let provider_id_string = job.provider_id.to_string();
    let release_id_string = release.release_id.to_string();
    let mut transaction = pool.begin().await?;

    // This no-op CAS acquires the release row before evidence is read. It
    // prevents a concurrent attempt from rebinding the release between the
    // ownership check and the terminal failure write on both SQLite and
    // PostgreSQL.
    let ownership_lock = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_releases
         SET updated_at = updated_at
         WHERE release_id = $1
           AND media_type = 'anime'
           AND download_id = $2
           AND selected_route_logical_id = $3
           AND selected_provider_id = $4
           AND state IN ('staging', 'ready', 'submitted', 'downloading', 'materializing')
           AND EXISTS (
               SELECT 1
               FROM acquisition_release_jobs j
               WHERE j.release_id = acquisition_releases.release_id
                 AND j.download_id = $2
                 AND j.route_logical_id = $3
                 AND j.provider_id = $4
                 AND j.active = 1
                 AND j.state IN ('staging', 'ready', 'submitted', 'downloading', 'materializing')
           )",
    )
    .bind(&release_id_string)
    .bind(&job_id_string)
    .bind(DEBRID_DEFAULT_LOGICAL_ID)
    .bind(&provider_id_string)
    .execute(&mut *transaction)
    .await
    .context("claiming exact anime Debrid retry attempt")?;

    if ownership_lock.rows_affected() == 1 {
        let target_ids = anime_debrid_retry_target_ids_for_attempt(
            &mut transaction,
            job_id,
            job.provider_id,
            retry,
            None,
        )
        .await?;
        if !lock_anime_debrid_retry_targets_for_attempt(
            &mut transaction,
            &target_ids,
            job_id,
            job.provider_id,
        )
        .await?
        {
            transaction.rollback().await?;
            return Ok(AnimeDebridRetryClaim::LostOwnership);
        }
        let current_coverage_plan =
            anime_debrid_retry_coverage_plan_in_transaction(&mut transaction, release.release_id)
                .await?
                .or_else(|| release.coverage_plan.clone());
        let current_coverage_plan =
            merge_debrid_coverage_plans(current_coverage_plan, retry.coverage_plan.clone());
        let coverage_plan = merge_anime_debrid_retry_evidence(
            current_coverage_plan,
            job_id,
            remote_release_id,
            retry,
            &target_ids,
            cleanup,
            now,
        );
        let coverage_plan_json = serde_json::to_string(&coverage_plan)
            .context("serializing claimed anime Debrid retry evidence")?;

        let release_job_update = sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_release_jobs
             SET state = 'failed',
                 state_reason = $1,
                 active = 0,
                 completed_at = COALESCE(completed_at, CURRENT_TIMESTAMP),
                 updated_at = CURRENT_TIMESTAMP
             WHERE release_id = $2
               AND download_id = $3
               AND route_logical_id = $4
               AND provider_id = $5
               AND active = 1
               AND state IN ('staging', 'ready', 'submitted', 'downloading', 'materializing')",
        )
        .bind(reason)
        .bind(&release_id_string)
        .bind(&job_id_string)
        .bind(DEBRID_DEFAULT_LOGICAL_ID)
        .bind(&provider_id_string)
        .execute(&mut *transaction)
        .await
        .context("terminalizing claimed anime Debrid release job")?;
        if release_job_update.rows_affected() != 1 {
            transaction.rollback().await?;
            return Ok(AnimeDebridRetryClaim::LostOwnership);
        }

        sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_release_files
             SET selected = 0, updated_at = CURRENT_TIMESTAMP
             WHERE release_id = $1",
        )
        .bind(&release_id_string)
        .execute(&mut *transaction)
        .await
        .context("clearing files for claimed anime Debrid retry")?;
        sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_release_coverage
             SET state = 'rejected',
                 reason = $1,
                 verified_by = 'alm7_debrid_automatic_retry',
                 updated_at = CURRENT_TIMESTAMP
             WHERE release_id = $2",
        )
        .bind(&retry.reason_code)
        .bind(&release_id_string)
        .execute(&mut *transaction)
        .await
        .context("rejecting coverage for claimed anime Debrid retry")?;
        let release_update = sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_releases
             SET state = 'failed',
                 state_reason = $1,
                 coverage_plan_json = $2,
                 updated_at = CURRENT_TIMESTAMP
             WHERE release_id = $3
               AND download_id = $4
               AND selected_route_logical_id = $5
               AND selected_provider_id = $6
               AND state IN ('staging', 'ready', 'submitted', 'downloading', 'materializing')",
        )
        .bind(reason)
        .bind(coverage_plan_json)
        .bind(&release_id_string)
        .bind(&job_id_string)
        .bind(DEBRID_DEFAULT_LOGICAL_ID)
        .bind(&provider_id_string)
        .execute(&mut *transaction)
        .await
        .context("failing claimed anime Debrid release")?;
        if release_update.rows_affected() != 1 {
            transaction.rollback().await?;
            return Ok(AnimeDebridRetryClaim::LostOwnership);
        }
        transaction.commit().await?;
        return Ok(AnimeDebridRetryClaim::Owned {
            target_ids,
            provider_id: job.provider_id,
        });
    }
    transaction.rollback().await?;

    claim_anime_debrid_retry_replay(
        pool,
        &job,
        release,
        remote_release_id,
        retry,
        cleanup,
        now,
        reason,
    )
    .await
}

async fn claim_anime_debrid_retry_replay(
    pool: &sqlx::AnyPool,
    job: &DebridDownloadJob,
    release: &AcquisitionRelease,
    remote_release_id: &str,
    retry: &AnimeDebridAutomaticRetry,
    cleanup: Value,
    now: chrono::DateTime<chrono::Utc>,
    reason: &str,
) -> Result<AnimeDebridRetryClaim> {
    let job_id_string = job.job_id.to_string();
    let provider_id_string = job.provider_id.to_string();
    let release_id_string = release.release_id.to_string();
    let mut transaction = pool.begin().await?;
    let replay_lock = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_releases
         SET updated_at = updated_at
         WHERE release_id = $1
           AND media_type = 'anime'
           AND download_id = $2
           AND selected_route_logical_id = $3
           AND selected_provider_id = $4
           AND state = 'failed'",
    )
    .bind(&release_id_string)
    .bind(&job_id_string)
    .bind(DEBRID_DEFAULT_LOGICAL_ID)
    .bind(&provider_id_string)
    .execute(&mut *transaction)
    .await
    .context("locking replayed anime Debrid retry")?;
    if replay_lock.rows_affected() != 1 {
        transaction.rollback().await?;
        return Ok(AnimeDebridRetryClaim::LostOwnership);
    }

    let current_coverage_plan =
        anime_debrid_retry_coverage_plan_in_transaction(&mut transaction, release.release_id)
            .await?;
    if anime_debrid_retry_evidence_job_id(current_coverage_plan.as_ref()) != Some(job.job_id) {
        transaction.rollback().await?;
        return Ok(AnimeDebridRetryClaim::LostOwnership);
    }
    let matching_release_jobs = sqlx::query_scalar::<sqlx::Any, i64>(
        "SELECT COUNT(*)
         FROM acquisition_release_jobs
         WHERE release_id = $1
           AND download_id = $2
           AND route_logical_id = $3
           AND provider_id = $4
           AND (
               (active = 0 AND state = 'failed')
               OR (active = 1 AND state IN ('staging', 'ready', 'submitted', 'downloading', 'materializing'))
           )",
    )
    .bind(&release_id_string)
    .bind(&job_id_string)
    .bind(DEBRID_DEFAULT_LOGICAL_ID)
    .bind(&provider_id_string)
    .fetch_one(&mut *transaction)
    .await
    .context("verifying replayed anime Debrid release job")?;
    if matching_release_jobs != 1 {
        transaction.rollback().await?;
        return Ok(AnimeDebridRetryClaim::LostOwnership);
    }

    let target_ids = anime_debrid_retry_target_ids_for_attempt(
        &mut transaction,
        job.job_id,
        job.provider_id,
        retry,
        current_coverage_plan.as_ref(),
    )
    .await?;
    if !lock_anime_debrid_retry_targets_for_attempt(
        &mut transaction,
        &target_ids,
        job.job_id,
        job.provider_id,
    )
    .await?
    {
        transaction.rollback().await?;
        return Ok(AnimeDebridRetryClaim::LostOwnership);
    }
    let current_coverage_plan =
        merge_debrid_coverage_plans(current_coverage_plan, retry.coverage_plan.clone());
    let coverage_plan = merge_anime_debrid_retry_evidence(
        current_coverage_plan,
        job.job_id,
        remote_release_id,
        retry,
        &target_ids,
        cleanup,
        now,
    );
    let coverage_plan_json = serde_json::to_string(&coverage_plan)
        .context("serializing replayed anime Debrid retry evidence")?;
    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_release_jobs
         SET state = 'failed',
             state_reason = $1,
             active = 0,
             completed_at = COALESCE(completed_at, CURRENT_TIMESTAMP),
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = $2
           AND download_id = $3
           AND route_logical_id = $4
           AND provider_id = $5",
    )
    .bind(reason)
    .bind(&release_id_string)
    .bind(&job_id_string)
    .bind(DEBRID_DEFAULT_LOGICAL_ID)
    .bind(&provider_id_string)
    .execute(&mut *transaction)
    .await
    .context("reconciling replayed anime Debrid release job")?;
    let release_update = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_releases
         SET state_reason = $1,
             coverage_plan_json = $2,
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = $3
           AND download_id = $4
           AND selected_route_logical_id = $5
           AND selected_provider_id = $6
           AND state = 'failed'",
    )
    .bind(reason)
    .bind(coverage_plan_json)
    .bind(&release_id_string)
    .bind(&job_id_string)
    .bind(DEBRID_DEFAULT_LOGICAL_ID)
    .bind(&provider_id_string)
    .execute(&mut *transaction)
    .await
    .context("reconciling replayed anime Debrid retry evidence")?;
    if release_update.rows_affected() != 1 {
        transaction.rollback().await?;
        return Ok(AnimeDebridRetryClaim::LostOwnership);
    }
    transaction.commit().await?;
    Ok(AnimeDebridRetryClaim::Owned {
        target_ids,
        provider_id: job.provider_id,
    })
}

async fn anime_debrid_retry_coverage_plan_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Any>,
    release_id: Uuid,
) -> Result<Option<Value>> {
    let raw = sqlx::query_scalar::<sqlx::Any, Option<String>>(
        "SELECT CAST(coverage_plan_json AS TEXT)
         FROM acquisition_releases
         WHERE release_id = $1",
    )
    .bind(release_id.to_string())
    .fetch_optional(&mut **transaction)
    .await
    .context("loading anime Debrid retry evidence")?
    .flatten();
    raw.map(|raw| {
        serde_json::from_str(&raw).context("parsing acquisition release coverage plan JSON")
    })
    .transpose()
}

async fn anime_debrid_retry_target_ids_for_attempt(
    transaction: &mut sqlx::Transaction<'_, sqlx::Any>,
    job_id: Uuid,
    provider_id: Uuid,
    retry: &AnimeDebridAutomaticRetry,
    existing_coverage_plan: Option<&Value>,
) -> Result<BTreeSet<Uuid>> {
    let mut target_ids = retry.target_ids.iter().copied().collect::<BTreeSet<_>>();
    target_ids.extend(anime_debrid_retry_evidence_target_ids(
        existing_coverage_plan,
    ));
    let rows = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT target_id
         FROM acquisition_targets
         WHERE download_id = $1
           AND selected_route_logical_id = $2
           AND selected_provider_id = $3
           AND state NOT IN ('imported', 'excluded')",
    )
    .bind(job_id.to_string())
    .bind(DEBRID_DEFAULT_LOGICAL_ID)
    .bind(provider_id.to_string())
    .fetch_all(&mut **transaction)
    .await
    .context("loading exact anime Debrid retry targets")?;
    for target_id in rows {
        target_ids.insert(
            Uuid::parse_str(&target_id)
                .with_context(|| format!("acquisition target id '{target_id}' is invalid"))?,
        );
    }
    Ok(target_ids)
}

async fn lock_anime_debrid_retry_targets_for_attempt(
    transaction: &mut sqlx::Transaction<'_, sqlx::Any>,
    target_ids: &BTreeSet<Uuid>,
    job_id: Uuid,
    provider_id: Uuid,
) -> Result<bool> {
    for target_id in target_ids {
        let lock = sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_targets
             SET updated_at = updated_at
             WHERE target_id = $1
               AND (
                   state IN ('imported', 'excluded')
                   OR (
                       download_id = $2
                       AND selected_route_logical_id = $3
                       AND selected_provider_id = $4
                   )
                   OR (
                       state = 'pending'
                       AND download_id IS NULL
                       AND selected_route_logical_id IS NULL
                       AND selected_provider_id IS NULL
                   )
               )",
        )
        .bind(target_id.to_string())
        .bind(job_id.to_string())
        .bind(DEBRID_DEFAULT_LOGICAL_ID)
        .bind(provider_id.to_string())
        .execute(&mut **transaction)
        .await
        .context("locking exact anime Debrid retry target")?;
        if lock.rows_affected() == 1 {
            continue;
        }
        let exists = sqlx::query_scalar::<sqlx::Any, i64>(
            "SELECT COUNT(*) FROM acquisition_targets WHERE target_id = $1",
        )
        .bind(target_id.to_string())
        .fetch_one(&mut **transaction)
        .await
        .context("checking anime Debrid retry target ownership")?;
        if exists != 0 {
            return Ok(false);
        }
    }
    Ok(true)
}

fn anime_debrid_retry_evidence_job_id(coverage_plan: Option<&Value>) -> Option<Uuid> {
    coverage_plan?
        .pointer("/automaticRetry/jobId")?
        .as_str()
        .and_then(|value| Uuid::parse_str(value).ok())
}

fn anime_debrid_retry_evidence_target_ids(coverage_plan: Option<&Value>) -> BTreeSet<Uuid> {
    coverage_plan
        .and_then(|plan| plan.pointer("/automaticRetry/targetIds"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .filter_map(|value| Uuid::parse_str(value).ok())
        .collect()
}

async fn reset_anime_debrid_target_for_candidate_retry_if_owned(
    pool: &sqlx::AnyPool,
    target_id: Uuid,
    job_id: Uuid,
    expected_provider_id: Uuid,
    reason: &str,
    retry_after: chrono::DateTime<chrono::Utc>,
) -> Result<bool> {
    let result = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_targets
         SET state = 'pending',
             state_reason = $1,
             selected_provider_id = NULL,
             selected_route_logical_id = NULL,
             selected_candidate_json = NULL,
             download_id = NULL,
             next_search_after = $2,
             updated_at = CURRENT_TIMESTAMP
         WHERE target_id = $3
           AND (
               (
                   download_id = $4
                   AND selected_route_logical_id = $5
                   AND selected_provider_id = $6
               )
               OR (
                   state = 'pending'
                   AND download_id IS NULL
                   AND selected_route_logical_id IS NULL
                   AND selected_provider_id IS NULL
               )
           )
           AND state NOT IN ('imported', 'excluded')",
    )
    .bind(reason)
    .bind(retry_after.format("%Y-%m-%d %H:%M:%S").to_string())
    .bind(target_id.to_string())
    .bind(job_id.to_string())
    .bind(DEBRID_DEFAULT_LOGICAL_ID)
    .bind(expected_provider_id.to_string())
    .execute(pool)
    .await
    .context("resetting exact anime Debrid retry target")?;
    if result.rows_affected() != 1 {
        return Ok(false);
    }
    if let Some(target) = get_target(pool, target_id).await? {
        sync_library_episode_acquisition_state_for_target(pool, &target).await?;
    }
    Ok(true)
}

async fn stage_anime_debrid_retry_disposition(
    pool: &sqlx::AnyPool,
    job_id: Uuid,
    retry: &AnimeDebridAutomaticRetry,
) -> Result<bool> {
    let Some(job) = load_debrid_job(pool, job_id).await? else {
        bail!("Debrid job disappeared while staging anime automatic retry");
    };
    let Some(release_id) = job.release_id else {
        return Ok(false);
    };
    let job_id = job_id.to_string();
    let release_id = release_id.to_string();
    let provider_id = job.provider_id.to_string();
    let mut transaction = pool.begin().await?;
    let ownership = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_releases
         SET updated_at = updated_at
         WHERE release_id = $1
           AND media_type = 'anime'
           AND download_id = $2
           AND selected_route_logical_id = $3
           AND selected_provider_id = $4
           AND state IN ('staging', 'ready', 'submitted', 'downloading', 'materializing')
           AND EXISTS (
               SELECT 1 FROM acquisition_release_jobs j
               WHERE j.release_id = acquisition_releases.release_id
                 AND j.download_id = $2
                 AND j.route_logical_id = $3
                 AND j.provider_id = $4
                 AND j.active = 1
                 AND j.state IN ('staging', 'ready', 'submitted', 'downloading', 'materializing')
           )
           AND EXISTS (
               SELECT 1 FROM debrid_download_jobs d
               WHERE d.job_id = $2
                 AND d.release_id = acquisition_releases.release_id
                 AND d.provider_id = $4
                 AND d.status NOT IN ('completed', 'failed', 'cancelled', 'paused', 'review_required')
           )",
    )
    .bind(&release_id)
    .bind(&job_id)
    .bind(DEBRID_DEFAULT_LOGICAL_ID)
    .bind(&provider_id)
    .execute(&mut *transaction)
    .await
    .context("claiming exact anime Debrid retry staging attempt")?;
    if ownership.rows_affected() != 1 {
        transaction.rollback().await?;
        return Ok(false);
    }
    let Some(snapshot) = sqlx::query::<sqlx::Any>(
        "SELECT status, COALESCE(provider_status_json, '') AS provider_status_json
         FROM debrid_download_jobs
         WHERE job_id = $1 AND release_id = $2 AND provider_id = $3",
    )
    .bind(&job_id)
    .bind(&release_id)
    .bind(&provider_id)
    .fetch_optional(&mut *transaction)
    .await?
    else {
        transaction.rollback().await?;
        return Ok(false);
    };
    let expected_status: String = snapshot.try_get("status")?;
    let expected_provider_status: String = snapshot.try_get("provider_status_json")?;
    let current_provider_status = if expected_provider_status.trim().is_empty() {
        None
    } else {
        serde_json::from_str(&expected_provider_status).ok()
    };
    let provider_status = merge_debrid_evidence_object(
        current_provider_status,
        "animeAutomaticRetry",
        serde_json::to_value(retry).context("serializing anime Debrid retry disposition")?,
    );
    let update = sqlx::query::<sqlx::Any>(
        "UPDATE debrid_download_jobs
         SET status = 'anime_retry_pending',
             provider_status_json = $1,
             updated_at = CURRENT_TIMESTAMP
         WHERE job_id = $2
           AND release_id = $3
           AND provider_id = $4
           AND status = $5
           AND COALESCE(provider_status_json, '') = $6",
    )
    .bind(serde_json::to_string(&provider_status)?)
    .bind(&job_id)
    .bind(&release_id)
    .bind(&provider_id)
    .bind(&expected_status)
    .bind(&expected_provider_status)
    .execute(&mut *transaction)
    .await?;
    if update.rows_affected() != 1 {
        transaction.rollback().await?;
        return Ok(false);
    }
    transaction.commit().await?;
    Ok(true)
}

fn anime_debrid_retry_disposition_from_job(
    job: &DebridDownloadJob,
) -> Option<AnimeDebridAutomaticRetry> {
    serde_json::from_value(
        job.provider_status
            .as_ref()?
            .get("animeAutomaticRetry")?
            .clone(),
    )
    .ok()
}

async fn mark_anime_debrid_retry_job_failed(
    pool: &sqlx::AnyPool,
    job_id: Uuid,
    reason: &str,
) -> Result<()> {
    let Some(job) = load_debrid_job(pool, job_id).await? else {
        return Ok(());
    };
    let mut provider_status = match job.provider_status {
        Some(Value::Object(object)) => object,
        Some(previous) => {
            serde_json::Map::from_iter([("previousProviderStatus".to_string(), previous)])
        }
        None => serde_json::Map::new(),
    };
    let consumed_retry = provider_status.remove("animeAutomaticRetry");
    if let Some(retry) = consumed_retry {
        provider_status.insert(
            "animeAutomaticRetryConsumed".to_string(),
            json!({
                "status": "consumed",
                "retry": retry,
                "consumedAt": chrono::Utc::now(),
                "policyVersion": "alm7-debrid-anime-retry-consumption-v1"
            }),
        );
    }
    let provider_status_json = serde_json::to_string(&Value::Object(provider_status))
        .context("serializing consumed anime Debrid retry disposition")?;
    sqlx::query::<sqlx::Any>(
        "UPDATE debrid_download_jobs
         SET status = 'failed',
             remote_release_status = 'failed',
             last_error = $1,
             selection_error = NULL,
             provider_status_json = $2,
             updated_at = CURRENT_TIMESTAMP
         WHERE job_id = $3",
    )
    .bind(reason)
    .bind(provider_status_json)
    .bind(job_id.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

async fn mark_stale_anime_debrid_provider_job(
    pool: &sqlx::AnyPool,
    job_id: Uuid,
    reason: &str,
) -> Result<()> {
    // Only the superseded provider-attempt row is ours to terminalize. The
    // shared release, targets, files, coverage, and remote object belong to
    // the newer exact attempt and are intentionally untouched.
    sqlx::query::<sqlx::Any>(
        "UPDATE debrid_download_jobs
         SET status = 'failed', last_error = $1, updated_at = CURRENT_TIMESTAMP
         WHERE job_id = $2
           AND status NOT IN ('completed', 'failed', 'cancelled')",
    )
    .bind(reason)
    .bind(job_id.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

fn merge_anime_debrid_retry_evidence(
    coverage_plan: Option<Value>,
    job_id: Uuid,
    remote_release_id: &str,
    retry: &AnimeDebridAutomaticRetry,
    target_ids: &BTreeSet<Uuid>,
    cleanup: Value,
    now: chrono::DateTime<chrono::Utc>,
) -> Value {
    let coverage_plan = merge_debrid_evidence_object(
        coverage_plan,
        "automaticRetry",
        json!({
            "status": "scheduled",
            "reason": retry.reason_code,
            "jobId": job_id,
            "remoteReleaseId": remote_release_id,
            "targetIds": target_ids,
            "retryDelaySeconds": ANIME_DEBRID_CANDIDATE_RETRY_SECONDS,
            "scheduledAt": now,
            "policyVersion": "alm7-debrid-anime-retry-v1",
            "providerCleanup": cleanup
        }),
    );
    merge_debrid_evidence_object(
        Some(coverage_plan),
        "retrySuppression",
        json!({
            "status": if retry.suppress_automatic_rediscovery { "rejected" } else { "retryable" },
            "suppressAutomaticRediscovery": retry.suppress_automatic_rediscovery,
            "reason": retry.reason_code,
            "failedAt": now,
        }),
    )
}

fn decide_debrid_file_selection(
    release: &AcquisitionRelease,
    files: &[AcquisitionReleaseFile],
    coverage: &[AcquisitionReleaseCoverage],
    inspection: &DebridReleaseInspection,
) -> DebridFileSelectionDecision {
    if release.media_type != MediaType::Anime
        && let Some(decision) =
            approved_debrid_user_override(release, files, &inspection.capabilities)
    {
        return decision.with_inferred_target_file_selections(release, files, coverage);
    }

    let mut review_reasons = BTreeSet::new();
    let capabilities = &inspection.capabilities;
    if release.confidence != ReleaseConfidence::High {
        review_reasons.insert("coverage_not_high_confidence".to_string());
    }
    let selectable_files = files
        .iter()
        .filter(|file| file.selectable)
        .collect::<Vec<_>>();
    let selectable_media_files = selectable_files
        .iter()
        .copied()
        .filter(|file| {
            is_debrid_media_file(&file.path) && !is_debrid_sample_or_extra_file(&file.path)
        })
        .collect::<Vec<_>>();

    let safe_single_without_file_list =
        files.is_empty() && matches!(release.release_kind, ReleaseKind::Single);
    if files.is_empty() && !safe_single_without_file_list {
        review_reasons.insert("missing_file_list".to_string());
    }
    if !files.is_empty() && selectable_media_files.is_empty() {
        review_reasons.insert("no_media_files".to_string());
    }

    let files_by_release_file_id = files
        .iter()
        .map(|file| (file.release_file_id, file))
        .collect::<HashMap<_, _>>();
    let mut selected_file_ids = coverage
        .iter()
        .filter(|coverage| coverage.confidence == ReleaseConfidence::High)
        .filter(|coverage| coverage.state != ReleaseCoverageState::Rejected)
        .filter_map(|coverage| coverage.release_file_id)
        .filter_map(|release_file_id| files_by_release_file_id.get(&release_file_id))
        .filter(|file| file.selectable)
        .filter_map(|file| {
            file.provider_file_id
                .clone()
                .or_else(|| file.file_id.clone())
        })
        .collect::<BTreeSet<_>>();

    let mut target_file_selections = Vec::new();
    if selected_file_ids.is_empty() && release.confidence == ReleaseConfidence::High {
        let targets = debrid_targets_from_coverage_plan(release, coverage);
        let fallback = select_debrid_files_for_targets(release, &selectable_media_files, &targets);
        selected_file_ids.extend(fallback.selected_file_ids);
        target_file_selections.extend(fallback.target_file_selections);
        review_reasons.extend(fallback.review_reasons);
    }

    if selected_file_ids.is_empty()
        && matches!(release.release_kind, ReleaseKind::Single)
        && selectable_media_files.len() == 1
        && release.confidence == ReleaseConfidence::High
        && let Some(file_id) = selectable_media_files[0]
            .provider_file_id
            .clone()
            .or_else(|| selectable_media_files[0].file_id.clone())
    {
        selected_file_ids.insert(file_id);
    }

    if safe_single_without_file_list && release.confidence == ReleaseConfidence::High {
        selected_file_ids.insert("all".to_string());
    }

    let selectable_media_ids = selectable_media_files
        .iter()
        .filter_map(|file| {
            file.provider_file_id
                .clone()
                .or_else(|| file.file_id.clone())
        })
        .collect::<BTreeSet<_>>();
    if !capabilities.supports_file_selection && selected_file_ids != selectable_media_ids {
        review_reasons.insert("file_selection_unsupported".to_string());
    }
    let missing_wanted_media = selectable_media_ids
        .difference(&selected_file_ids)
        .cloned()
        .collect::<Vec<_>>();
    if !missing_wanted_media.is_empty()
        && matches!(
            release.release_kind,
            ReleaseKind::MultiEpisode
                | ReleaseKind::SeasonPack
                | ReleaseKind::MultiSeasonPack
                | ReleaseKind::SeriesPack
        )
        && !release_allows_scoped_overfetch_skip(release)
    {
        review_reasons.insert("file_list_does_not_cover_all_selectable_media".to_string());
    }

    if selected_file_ids.is_empty() {
        review_reasons.insert("no_selected_files".to_string());
    }

    let selected_file_ids = selected_file_ids.into_iter().collect::<Vec<_>>();
    let selected_set = selected_file_ids.iter().cloned().collect::<HashSet<_>>();
    let selected_btree = selected_file_ids.iter().cloned().collect::<BTreeSet<_>>();
    let mut skipped_file_ids = files
        .iter()
        .filter_map(|file| {
            file.provider_file_id
                .clone()
                .or_else(|| file.file_id.clone())
        })
        .filter(|file_id| !selected_set.contains(file_id))
        .collect::<Vec<_>>();
    skipped_file_ids.sort();
    skipped_file_ids.dedup();

    let select_all = safe_single_without_file_list && selected_set.contains("all");
    let select_all_approved =
        select_all || (!selectable_media_ids.is_empty() && selectable_media_ids == selected_btree);
    let provider_selection_ids = if select_all {
        vec!["all".to_string()]
    } else {
        selected_file_ids.clone()
    };
    let review_reasons = review_reasons.into_iter().collect::<Vec<_>>();
    let status = if review_reasons.is_empty() {
        DebridSelectionDecisionStatus::Approved
    } else {
        DebridSelectionDecisionStatus::ReviewRequired
    };
    DebridFileSelectionDecision {
        status,
        selected_file_ids,
        skipped_file_ids,
        provider_selection_ids,
        target_file_selections,
        policy_version: DEBRID_SELECTION_POLICY_VERSION.to_string(),
        coverage_fingerprint: debrid_coverage_fingerprint(release, files, coverage),
        review_reasons,
        select_all,
        select_all_approved,
    }
}

fn release_allows_scoped_overfetch_skip(release: &AcquisitionRelease) -> bool {
    if release.media_type == MediaType::Anime
        && release.coverage_plan.as_ref().is_some_and(|plan| {
            plan.pointer("/automaticResolution/selectionPolicyReady")
                .and_then(Value::as_bool)
                == Some(true)
        })
    {
        return true;
    }
    let Some(evidence) = release
        .coverage_plan
        .as_ref()
        .and_then(|plan| find_request_scope_evidence(plan, 0))
    else {
        return false;
    };
    let request_mode = evidence
        .get("requestMode")
        .or_else(|| evidence.get("request_mode"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    if request_mode != "one_shot" {
        return false;
    }
    let target_count = evidence
        .get("targetCount")
        .or_else(|| evidence.get("target_count"))
        .and_then(Value::as_u64)
        .unwrap_or_default();
    target_count > 0
}

fn find_request_scope_evidence(value: &Value, depth: usize) -> Option<&Value> {
    if depth > 4 {
        return None;
    }
    if let Some(evidence) = value
        .get("requestScopeEvidence")
        .or_else(|| value.get("request_scope_evidence"))
        && evidence.is_object()
    {
        return Some(evidence);
    }
    for key in [
        "tv",
        "anime",
        "tvCoveragePlan",
        "animeCoveragePlan",
        "coveragePlan",
        "previousCoveragePlan",
    ] {
        if let Some(nested) = value.get(key)
            && let Some(evidence) = find_request_scope_evidence(nested, depth + 1)
        {
            return Some(evidence);
        }
    }
    None
}

fn debrid_targets_from_coverage_plan(
    release: &AcquisitionRelease,
    coverage: &[AcquisitionReleaseCoverage],
) -> Vec<DebridCoverageTarget> {
    let covered_target_ids = coverage
        .iter()
        .filter(|coverage| coverage.confidence == ReleaseConfidence::High)
        .filter(|coverage| coverage.state != ReleaseCoverageState::Rejected)
        .map(|coverage| coverage.target_id)
        .collect::<BTreeSet<_>>();
    if covered_target_ids.is_empty() {
        return Vec::new();
    }

    let scoped_targets = debrid_scope_targets(release.coverage_plan.as_ref());
    let scoped_by_id = scoped_targets
        .iter()
        .map(|(_, target)| (target.target_id, *target))
        .collect::<HashMap<_, _>>();
    let scoped_by_key = scoped_targets
        .iter()
        .filter_map(|(target_key, target)| {
            target_key
                .as_ref()
                .map(|target_key| (target_key.clone(), *target))
        })
        .collect::<HashMap<_, _>>();

    let mut targets = Vec::new();
    let mut seen = HashSet::new();
    for entry in debrid_coverage_plan_entries(release.coverage_plan.as_ref()) {
        let target_key = coverage_plan_string(entry, &["targetKey", "target_key"]);
        let target = debrid_target_from_coverage_plan_entry(entry)
            .or_else(|| {
                target_key
                    .as_ref()
                    .and_then(|target_key| scoped_by_key.get(target_key).copied())
            })
            .or_else(|| {
                let target_id = single_covered_target_id(&covered_target_ids)?;
                let mut target = debrid_target_from_coverage_entry_values(target_id, entry);
                if let Some(target_key) = target_key.as_deref()
                    && let Some(from_key) = debrid_target_from_key(target_id, target_key)
                {
                    target = merge_debrid_coverage_target(target, from_key);
                }
                Some(target)
            })
            .map(|target| {
                scoped_by_id
                    .get(&target.target_id)
                    .copied()
                    .map(|fallback| merge_debrid_coverage_target(target, fallback))
                    .unwrap_or(target)
            });
        let Some(target) = target else {
            continue;
        };
        if covered_target_ids.contains(&target.target_id) && seen.insert(target.target_id) {
            targets.push(target);
        }
    }

    for (_, target) in scoped_targets {
        if covered_target_ids.contains(&target.target_id) && seen.insert(target.target_id) {
            targets.push(target);
        }
    }

    targets
}

fn debrid_coverage_plan_entries(coverage_plan: Option<&Value>) -> Vec<&Value> {
    let mut entries = Vec::new();
    if let Some(value) = coverage_plan {
        collect_debrid_coverage_plan_entries(value, &mut entries, 0);
    }
    entries
}

fn collect_debrid_coverage_plan_entries<'a>(
    value: &'a Value,
    entries: &mut Vec<&'a Value>,
    depth: usize,
) {
    if depth > 3 {
        return;
    }

    if let Some(array) = value.get("entries").and_then(Value::as_array) {
        entries.extend(array.iter());
    }

    for key in [
        "tv",
        "anime",
        "tvCoveragePlan",
        "animeCoveragePlan",
        "coveragePlan",
        "previousCoveragePlan",
    ] {
        if let Some(nested) = value.get(key) {
            collect_debrid_coverage_plan_entries(nested, entries, depth + 1);
        }
    }
}

fn debrid_target_from_coverage_plan_entry(entry: &Value) -> Option<DebridCoverageTarget> {
    let target_id = entry
        .get("targetId")
        .or_else(|| entry.get("target_id"))
        .and_then(Value::as_str)
        .and_then(|value| Uuid::parse_str(value).ok())?;
    Some(debrid_target_from_coverage_entry_values(target_id, entry))
}

fn debrid_target_from_coverage_entry_values(
    target_id: Uuid,
    entry: &Value,
) -> DebridCoverageTarget {
    let mut target = DebridCoverageTarget {
        target_id,
        season_number: coverage_plan_i32(entry, &["seasonNumber", "season_number"]),
        episode_number: coverage_plan_i32(entry, &["episodeNumber", "episode_number"]),
        episode_end_number: coverage_plan_i32(entry, &["episodeEndNumber", "episode_end_number"]),
        absolute_episode_number: coverage_plan_i32(
            entry,
            &["absoluteEpisodeNumber", "absolute_episode_number"],
        ),
        absolute_episode_end_number: coverage_plan_i32(
            entry,
            &["absoluteEpisodeEndNumber", "absolute_episode_end_number"],
        ),
    };
    if let Some(target_key) = coverage_plan_string(entry, &["targetKey", "target_key"])
        && let Some(from_key) = debrid_target_from_key(target_id, &target_key)
    {
        target = merge_debrid_coverage_target(target, from_key);
    }
    target
}

fn debrid_scope_targets(
    coverage_plan: Option<&Value>,
) -> Vec<(Option<String>, DebridCoverageTarget)> {
    let Some(evidence) = coverage_plan.and_then(|plan| find_request_scope_evidence(plan, 0)) else {
        return Vec::new();
    };

    let target_ids = json_string_array(
        evidence
            .get("targetIds")
            .or_else(|| evidence.get("target_ids")),
    );
    let target_keys = json_string_array(
        evidence
            .get("targetKeys")
            .or_else(|| evidence.get("target_keys")),
    );
    let season_numbers = json_i32_array(
        evidence
            .get("seasonNumbers")
            .or_else(|| evidence.get("season_numbers")),
    );
    let episode_numbers = json_i32_array(
        evidence
            .get("episodeNumbers")
            .or_else(|| evidence.get("episode_numbers")),
    );
    let episode_end_numbers = json_i32_array(
        evidence
            .get("episodeEndNumbers")
            .or_else(|| evidence.get("episode_end_numbers")),
    );
    let absolute_episode_numbers = json_i32_array(
        evidence
            .get("absoluteEpisodeNumbers")
            .or_else(|| evidence.get("absolute_episode_numbers")),
    );
    let absolute_episode_end_numbers = json_i32_array(
        evidence
            .get("absoluteEpisodeEndNumbers")
            .or_else(|| evidence.get("absolute_episode_end_numbers")),
    );

    target_ids
        .iter()
        .enumerate()
        .filter_map(|(index, target_id)| {
            let target_id = Uuid::parse_str(target_id).ok()?;
            let target_key = target_keys.get(index).cloned();
            let target_metadata = target_key.as_ref().and_then(|target_key| {
                evidence
                    .get("targets")
                    .and_then(|targets| targets.get(target_key))
            });
            let mut target = DebridCoverageTarget {
                target_id,
                season_number: target_metadata
                    .and_then(|value| coverage_plan_i32(value, &["seasonNumber", "season_number"]))
                    .or_else(|| indexed_or_single_i32(&season_numbers, index)),
                episode_number: target_metadata
                    .and_then(|value| {
                        coverage_plan_i32(value, &["episodeNumber", "episode_number"])
                    })
                    .or_else(|| indexed_or_single_i32(&episode_numbers, index)),
                episode_end_number: target_metadata
                    .and_then(|value| {
                        coverage_plan_i32(value, &["episodeEndNumber", "episode_end_number"])
                    })
                    .or_else(|| indexed_or_single_i32(&episode_end_numbers, index)),
                absolute_episode_number: target_metadata
                    .and_then(|value| {
                        coverage_plan_i32(
                            value,
                            &["absoluteEpisodeNumber", "absolute_episode_number"],
                        )
                    })
                    .or_else(|| indexed_or_single_i32(&absolute_episode_numbers, index)),
                absolute_episode_end_number: target_metadata
                    .and_then(|value| {
                        coverage_plan_i32(
                            value,
                            &["absoluteEpisodeEndNumber", "absolute_episode_end_number"],
                        )
                    })
                    .or_else(|| indexed_or_single_i32(&absolute_episode_end_numbers, index)),
            };
            if let Some(target_key) = target_key.as_deref()
                && let Some(from_key) = debrid_target_from_key(target_id, target_key)
            {
                target = merge_debrid_coverage_target(target, from_key);
            }
            Some((target_key, target))
        })
        .collect()
}

fn single_covered_target_id(target_ids: &BTreeSet<Uuid>) -> Option<Uuid> {
    (target_ids.len() == 1)
        .then(|| target_ids.iter().next().copied())
        .flatten()
}

fn debrid_target_from_key(target_id: Uuid, target_key: &str) -> Option<DebridCoverageTarget> {
    let trimmed = target_key.trim();
    if trimmed.len() == 6
        && trimmed.as_bytes()[0].eq_ignore_ascii_case(&b'S')
        && trimmed.as_bytes()[3].eq_ignore_ascii_case(&b'E')
    {
        let season = trimmed.get(1..3)?.parse::<i32>().ok()?;
        let episode = trimmed.get(4..6)?.parse::<i32>().ok()?;
        return Some(DebridCoverageTarget {
            target_id,
            season_number: Some(season),
            episode_number: Some(episode),
            episode_end_number: Some(episode),
            absolute_episode_number: None,
            absolute_episode_end_number: None,
        });
    }
    if trimmed.len() == 5 && trimmed.as_bytes()[0].eq_ignore_ascii_case(&b'A') {
        let absolute = trimmed.get(1..5)?.parse::<i32>().ok()?;
        return Some(DebridCoverageTarget {
            target_id,
            season_number: None,
            episode_number: None,
            episode_end_number: None,
            absolute_episode_number: Some(absolute),
            absolute_episode_end_number: Some(absolute),
        });
    }
    None
}

fn merge_debrid_coverage_target(
    target: DebridCoverageTarget,
    fallback: DebridCoverageTarget,
) -> DebridCoverageTarget {
    DebridCoverageTarget {
        target_id: target.target_id,
        season_number: target.season_number.or(fallback.season_number),
        episode_number: target.episode_number.or(fallback.episode_number),
        episode_end_number: target.episode_end_number.or(fallback.episode_end_number),
        absolute_episode_number: target
            .absolute_episode_number
            .or(fallback.absolute_episode_number),
        absolute_episode_end_number: target
            .absolute_episode_end_number
            .or(fallback.absolute_episode_end_number),
    }
}

fn coverage_plan_i32(entry: &Value, keys: &[&str]) -> Option<i32> {
    keys.iter()
        .find_map(|key| entry.get(*key))
        .and_then(Value::as_i64)
        .and_then(|value| i32::try_from(value).ok())
}

fn coverage_plan_string(entry: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| entry.get(*key))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn json_i32_array(value: Option<&Value>) -> Vec<i32> {
    value
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_i64)
                .filter_map(|value| i32::try_from(value).ok())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn indexed_or_single_i32(values: &[i32], index: usize) -> Option<i32> {
    if values.len() == 1 {
        values.first().copied()
    } else {
        values.get(index).copied()
    }
}

#[derive(Debug, Clone, Default)]
struct DebridTargetSelectionFallback {
    selected_file_ids: BTreeSet<String>,
    target_file_selections: Vec<DebridTargetFileSelection>,
    review_reasons: Vec<String>,
}

fn select_debrid_files_for_targets(
    release: &AcquisitionRelease,
    files: &[&AcquisitionReleaseFile],
    targets: &[DebridCoverageTarget],
) -> DebridTargetSelectionFallback {
    let mut selection = DebridTargetSelectionFallback::default();
    let mut review_reasons = BTreeSet::new();
    for target in targets {
        let episode_matches = files
            .iter()
            .copied()
            .filter(|file| debrid_file_matches_target(file, target))
            .collect::<Vec<_>>();
        if episode_matches.is_empty() {
            continue;
        }
        let title_matches = episode_matches
            .iter()
            .copied()
            .filter(|file| debrid_file_title_compatible(release, file))
            .collect::<Vec<_>>();
        let selected = if title_matches.len() == 1 {
            title_matches[0]
        } else if title_matches.is_empty() && episode_matches.len() == 1 {
            episode_matches[0]
        } else {
            review_reasons.insert("ambiguous_target_file_match".to_string());
            continue;
        };
        let Some(provider_file_id) = selected
            .provider_file_id
            .clone()
            .or_else(|| selected.file_id.clone())
        else {
            review_reasons.insert("matched_file_missing_provider_file_id".to_string());
            continue;
        };
        selection.selected_file_ids.insert(provider_file_id.clone());
        selection
            .target_file_selections
            .push(DebridTargetFileSelection {
                target_id: target.target_id,
                provider_file_id,
            });
    }
    selection.review_reasons = review_reasons.into_iter().collect();
    selection
        .target_file_selections
        .sort_by(|left, right| left.target_id.cmp(&right.target_id));
    selection
}

fn infer_debrid_target_file_selections(
    release: &AcquisitionRelease,
    files: &[AcquisitionReleaseFile],
    coverage: &[AcquisitionReleaseCoverage],
    selected_file_ids: &[String],
) -> Vec<DebridTargetFileSelection> {
    let selected_ids = selected_file_ids.iter().cloned().collect::<HashSet<_>>();
    if selected_ids.is_empty() || selected_ids.contains("all") {
        return Vec::new();
    }

    let selected_media_files = files
        .iter()
        .filter(|file| file.selectable)
        .filter(|file| {
            is_debrid_media_file(&file.path) && !is_debrid_sample_or_extra_file(&file.path)
        })
        .filter(|file| {
            file.provider_file_id
                .as_ref()
                .or(file.file_id.as_ref())
                .is_some_and(|file_id| selected_ids.contains(file_id))
        })
        .collect::<Vec<_>>();
    if selected_media_files.is_empty() {
        return Vec::new();
    }

    let targets = debrid_targets_from_coverage_plan(release, coverage);
    let inferred = select_debrid_files_for_targets(release, &selected_media_files, &targets)
        .target_file_selections;
    if !inferred.is_empty() {
        return inferred;
    }

    let unresolved_coverage = coverage
        .iter()
        .filter(|entry| entry.confidence == ReleaseConfidence::High)
        .filter(|entry| entry.state != ReleaseCoverageState::Rejected)
        .filter(|entry| entry.release_file_id.is_none())
        .collect::<Vec<_>>();
    if unresolved_coverage.len() == 1 && selected_media_files.len() == 1 {
        let Some(provider_file_id) = selected_media_files[0]
            .provider_file_id
            .clone()
            .or_else(|| selected_media_files[0].file_id.clone())
        else {
            return Vec::new();
        };
        return vec![DebridTargetFileSelection {
            target_id: unresolved_coverage[0].target_id,
            provider_file_id,
        }];
    }

    Vec::new()
}

fn debrid_file_title_compatible(
    release: &AcquisitionRelease,
    file: &AcquisitionReleaseFile,
) -> bool {
    if release.media_type != MediaType::Anime {
        return true;
    }
    let expected = parse_anime_release_title(&release.release_title)
        .series_title
        .or_else(|| Some(release.title.clone()))
        .map(|title| normalized_debrid_title(&title))
        .unwrap_or_default();
    let actual = file
        .parsed_title
        .as_ref()
        .map(|title| normalized_debrid_title(title))
        .unwrap_or_default();
    expected.is_empty() || actual.is_empty() || expected == actual
}

fn normalized_debrid_title(value: &str) -> String {
    value
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn debrid_file_matches_target(
    file: &AcquisitionReleaseFile,
    target: &DebridCoverageTarget,
) -> bool {
    if let Some(target_absolute) = target.absolute_episode_number {
        if let Some(file_absolute) = file.parsed_absolute_episode_number {
            let file_end = file
                .parsed_absolute_episode_end_number
                .unwrap_or(file_absolute);
            let target_end = target
                .absolute_episode_end_number
                .unwrap_or(target_absolute);
            if ranges_overlap(file_absolute, file_end, target_absolute, target_end) {
                return true;
            }
        }
    }

    let Some(target_episode) = target.episode_number else {
        return false;
    };
    if let Some(target_season) = target.season_number
        && file.parsed_season_number != Some(target_season)
    {
        return false;
    }
    let Some(file_episode) = file.parsed_episode_number else {
        return false;
    };
    let file_end = file.parsed_episode_end_number.unwrap_or(file_episode);
    let target_end = target.episode_end_number.unwrap_or(target_episode);
    ranges_overlap(file_episode, file_end, target_episode, target_end)
}

fn ranges_overlap(left_start: i32, left_end: i32, right_start: i32, right_end: i32) -> bool {
    left_start <= right_end && right_start <= left_end
}

fn approved_debrid_user_override(
    release: &AcquisitionRelease,
    files: &[AcquisitionReleaseFile],
    capabilities: &DebridProviderCapabilities,
) -> Option<DebridFileSelectionDecision> {
    if !capabilities.supports_file_selection {
        return None;
    }
    let policy = release.coverage_plan.as_ref().and_then(|value| {
        value
            .get("manualReview")
            .or_else(|| value.get("priorityPolicy"))
    })?;
    if policy.get("status").and_then(Value::as_str) != Some("approved")
        || policy.get("userApproved").and_then(Value::as_bool) != Some(true)
    {
        return None;
    }

    let selected_release_file_ids = resolve_approved_debrid_file_ids(
        json_string_array(policy.get("selectedReleaseFileIds")),
        files,
    );
    let selected_file_ids = if selected_release_file_ids.is_empty() {
        resolve_approved_debrid_file_ids(json_string_array(policy.get("selectedFileIds")), files)
    } else {
        selected_release_file_ids
    };
    if selected_file_ids.is_empty() {
        return None;
    }
    let selected_set = selected_file_ids.iter().cloned().collect::<BTreeSet<_>>();
    let skipped_release_file_ids = resolve_approved_debrid_file_ids(
        json_string_array(policy.get("skippedReleaseFileIds")),
        files,
    );
    let mut skipped_file_ids = if skipped_release_file_ids.is_empty() {
        resolve_approved_debrid_file_ids(json_string_array(policy.get("skippedFileIds")), files)
    } else {
        skipped_release_file_ids
    };
    skipped_file_ids.retain(|file_id| !selected_set.contains(file_id));
    let known_provider_ids = files
        .iter()
        .filter_map(|file| {
            file.provider_file_id
                .clone()
                .or_else(|| file.file_id.clone())
        })
        .collect::<BTreeSet<_>>();
    let mut review_reasons = BTreeSet::new();
    for file_id in &selected_file_ids {
        if file_id != "all" && !known_provider_ids.contains(file_id) {
            review_reasons.insert(format!("approved_file_id_not_found:{file_id}"));
        }
    }
    let select_all = selected_file_ids.iter().any(|value| value == "all");
    let select_all_approved =
        select_all || (!known_provider_ids.is_empty() && known_provider_ids == selected_set);
    let status = if review_reasons.is_empty() {
        DebridSelectionDecisionStatus::Approved
    } else {
        DebridSelectionDecisionStatus::ReviewRequired
    };
    let provider_selection_ids = if select_all {
        vec!["all".to_string()]
    } else {
        selected_file_ids.clone()
    };
    Some(DebridFileSelectionDecision {
        status,
        selected_file_ids,
        skipped_file_ids,
        provider_selection_ids,
        target_file_selections: Vec::new(),
        review_reasons: review_reasons.into_iter().collect(),
        policy_version: policy
            .get("policyVersion")
            .and_then(Value::as_str)
            .unwrap_or("rr7a-manual-review-v1")
            .to_string(),
        coverage_fingerprint: policy
            .get("coverageFingerprint")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| debrid_coverage_fingerprint(release, files, &[])),
        select_all,
        select_all_approved,
    })
}

fn resolve_approved_debrid_file_ids(
    file_ids: Vec<String>,
    files: &[AcquisitionReleaseFile],
) -> Vec<String> {
    let release_file_aliases = synthetic_source_candidate_release_file_aliases(files);
    file_ids
        .into_iter()
        .flat_map(|file_id| {
            if file_id == SYNTHETIC_SOURCE_CANDIDATE_FILE_ID
                && let Some(provider_file_id) =
                    matching_provider_file_id_for_synthetic_source_candidate(files)
            {
                return vec![provider_file_id];
            }
            if let Ok(release_file_id) = Uuid::parse_str(file_id.trim()) {
                let effective_release_file_id = release_file_aliases
                    .get(&release_file_id)
                    .copied()
                    .unwrap_or(release_file_id);
                if let Some(provider_file_id) = files
                    .iter()
                    .find(|file| file.release_file_id == effective_release_file_id)
                    .and_then(|file| {
                        file.provider_file_id
                            .clone()
                            .or_else(|| file.file_id.clone())
                    })
                    .filter(|value| !value.trim().is_empty())
                {
                    return vec![provider_file_id];
                }
            }
            vec![file_id]
        })
        .collect()
}

fn matching_provider_file_id_for_synthetic_source_candidate(
    files: &[AcquisitionReleaseFile],
) -> Option<String> {
    let synthetic = files
        .iter()
        .find(|file| is_synthetic_source_candidate_file(file))?;
    let synthetic_basename = normalized_debrid_review_basename(&synthetic.basename);
    if synthetic_basename.is_empty() {
        return None;
    }
    let matches = files
        .iter()
        .filter(|file| !is_synthetic_source_candidate_file(file))
        .filter(|file| file.selectable)
        .filter_map(|file| {
            let provider_file_id = file
                .provider_file_id
                .clone()
                .or_else(|| file.file_id.clone())?;
            (!provider_file_id.is_empty()
                && provider_file_id != SYNTHETIC_SOURCE_CANDIDATE_FILE_ID
                && normalized_debrid_review_basename(&file.basename) == synthetic_basename)
                .then_some(provider_file_id)
        })
        .collect::<Vec<_>>();
    if matches.len() == 1 {
        matches.into_iter().next()
    } else {
        None
    }
}

fn synthetic_source_candidate_release_file_aliases(
    files: &[AcquisitionReleaseFile],
) -> BTreeMap<Uuid, Uuid> {
    let mut aliases = BTreeMap::new();
    for synthetic in files
        .iter()
        .filter(|file| is_synthetic_source_candidate_file(file))
    {
        let synthetic_basename = normalized_debrid_review_basename(&synthetic.basename);
        if synthetic_basename.is_empty() {
            continue;
        }
        let matches = files
            .iter()
            .filter(|file| !is_synthetic_source_candidate_file(file))
            .filter(|file| file.selectable)
            .filter(|file| {
                file.provider_file_id
                    .as_deref()
                    .or(file.file_id.as_deref())
                    .is_some_and(|provider_file_id| {
                        !provider_file_id.is_empty()
                            && provider_file_id != SYNTHETIC_SOURCE_CANDIDATE_FILE_ID
                    })
            })
            .filter(|file| normalized_debrid_review_basename(&file.basename) == synthetic_basename)
            .map(|file| file.release_file_id)
            .collect::<Vec<_>>();
        if matches.len() == 1 {
            aliases.insert(synthetic.release_file_id, matches[0]);
        }
    }
    aliases
}

fn is_synthetic_source_candidate_file(file: &AcquisitionReleaseFile) -> bool {
    file.file_id.as_deref() == Some(SYNTHETIC_SOURCE_CANDIDATE_FILE_ID)
        || file.provider_file_id.as_deref() == Some(SYNTHETIC_SOURCE_CANDIDATE_FILE_ID)
        || file
            .raw
            .as_ref()
            .and_then(|value| value.get("source"))
            .and_then(Value::as_str)
            == Some("manual_review_source_candidate")
}

fn normalized_debrid_review_basename(value: &str) -> String {
    value
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or(value)
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn json_string_array(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

async fn persist_debrid_selection_decision(
    pool: &sqlx::AnyPool,
    job_id: Uuid,
    release: &AcquisitionRelease,
    files: &[AcquisitionReleaseFile],
    coverage: &[AcquisitionReleaseCoverage],
    decision: &DebridFileSelectionDecision,
) -> Result<bool> {
    let selected_ids = decision
        .selected_file_ids
        .iter()
        .cloned()
        .collect::<HashSet<_>>();
    let file_updates = files
        .iter()
        .map(|file| {
            let provider_id = file.provider_file_id.as_ref().or(file.file_id.as_ref());
            (
                file.release_file_id,
                provider_id
                    .map(|file_id| selected_ids.contains(file_id))
                    .unwrap_or(false),
            )
        })
        .collect::<Vec<_>>();
    let file_aliases = synthetic_source_candidate_release_file_aliases(files);
    let selected_provider_file_to_release_file = files
        .iter()
        .filter_map(|file| {
            let provider_id = file.provider_file_id.as_ref().or(file.file_id.as_ref())?;
            selected_ids
                .contains(provider_id)
                .then_some((provider_id.clone(), file.release_file_id))
        })
        .collect::<HashMap<_, _>>();
    let target_selection_by_target_id = decision
        .target_file_selections
        .iter()
        .filter_map(|selection| {
            selected_provider_file_to_release_file
                .get(&selection.provider_file_id)
                .copied()
                .map(|release_file_id| (selection.target_id, release_file_id))
        })
        .collect::<HashMap<_, _>>();
    let selected_target_ids = coverage
        .iter()
        .filter_map(|entry| {
            let release_file_id = entry.release_file_id.and_then(|release_file_id| {
                file_aliases
                    .get(&release_file_id)
                    .copied()
                    .or(Some(release_file_id))
            })?;
            let file = files
                .iter()
                .find(|file| file.release_file_id == release_file_id)?;
            let provider_id = file.provider_file_id.as_ref().or(file.file_id.as_ref())?;
            (decision.is_approved() && selected_ids.contains(provider_id))
                .then_some(entry.target_id)
        })
        .chain(target_selection_by_target_id.keys().copied())
        .collect::<HashSet<_>>();
    let coverage_updates = coverage
        .iter()
        .map(|entry| {
            let release_file_id = entry
                .release_file_id
                .and_then(|release_file_id| {
                    file_aliases
                        .get(&release_file_id)
                        .copied()
                        .or(Some(release_file_id))
                })
                .or_else(|| target_selection_by_target_id.get(&entry.target_id).copied());
            let selected = release_file_id
                .and_then(|release_file_id| {
                    files
                        .iter()
                        .find(|file| file.release_file_id == release_file_id)
                })
                .and_then(|file| file.provider_file_id.as_ref().or(file.file_id.as_ref()))
                .map(|file_id| selected_ids.contains(file_id))
                .unwrap_or(false);
            NewAcquisitionReleaseCoverage {
                coverage_id: Some(entry.coverage_id),
                release_id: entry.release_id,
                release_file_id,
                target_id: entry.target_id,
                coverage_kind: entry.coverage_kind,
                confidence: entry.confidence,
                score: entry.score,
                reason: entry.reason.clone(),
                state: if decision.is_approved() && selected {
                    ReleaseCoverageState::Selected
                } else if decision.is_approved()
                    && entry.release_file_id.is_none()
                    && selected_target_ids.contains(&entry.target_id)
                {
                    ReleaseCoverageState::Rejected
                } else if decision.is_approved() {
                    entry.state
                } else {
                    ReleaseCoverageState::ReviewRequired
                },
                verified_by: entry.verified_by.clone(),
            }
        })
        .collect::<Vec<_>>();
    let state = if decision.is_approved() {
        AcquisitionReleaseState::Ready
    } else {
        AcquisitionReleaseState::ReviewRequired
    };
    let reason = if decision.is_approved() {
        "RR-4F deterministic file selection approved."
    } else {
        "RR-4F deterministic file selection requires review."
    };
    if release.media_type == MediaType::Anime {
        return persist_anime_debrid_selection_intent_if_owned(
            pool,
            job_id,
            release,
            &file_updates,
            &coverage_updates,
            decision,
            state,
            reason,
        )
        .await;
    }

    update_debrid_job_selection_decision(pool, job_id, decision).await?;
    for (release_file_id, selected) in file_updates {
        update_release_file_selected(pool, release_file_id, selected).await?;
    }
    for update in coverage_updates {
        upsert_release_coverage(pool, update).await?;
    }
    update_debrid_release_selection_evidence(pool, release, state, reason, decision).await?;
    update_debrid_release_job_selection_state(
        pool,
        release.release_id,
        job_id,
        if decision.is_approved() {
            ReleaseJobState::Ready
        } else {
            ReleaseJobState::Staging
        },
        reason,
    )
    .await?;
    Ok(true)
}

async fn persist_anime_debrid_selection_intent_if_owned(
    pool: &sqlx::AnyPool,
    job_id: Uuid,
    release: &AcquisitionRelease,
    file_updates: &[(Uuid, bool)],
    coverage_updates: &[NewAcquisitionReleaseCoverage],
    decision: &DebridFileSelectionDecision,
    state: AcquisitionReleaseState,
    reason: &str,
) -> Result<bool> {
    debug_assert_eq!(release.media_type, MediaType::Anime);
    let Some(provider_id) = release.selected_provider_id else {
        return Ok(false);
    };
    let Some(remote_release_id) = release.remote_release_id.as_deref() else {
        return Ok(false);
    };
    let release_id = release.release_id.to_string();
    let job_id = job_id.to_string();
    let provider_id = provider_id.to_string();
    let mut transaction = pool.begin().await?;
    let ownership = lock_anime_debrid_selection_attempt(
        &mut transaction,
        &release_id,
        &job_id,
        &provider_id,
        remote_release_id,
    )
    .await?;
    if !ownership {
        transaction.rollback().await?;
        return Ok(false);
    }

    let selected = serde_json::to_string(&decision.selected_file_ids)?;
    let skipped = serde_json::to_string(&decision.skipped_file_ids)?;
    let error = (!decision.is_approved()).then(|| decision.review_reasons.join(","));
    let job_update = sqlx::query::<sqlx::Any>(
        "UPDATE debrid_download_jobs
         SET selected_file_ids_json = $1,
             skipped_file_ids_json = $2,
             selection_error = $3,
             status = CASE WHEN $4 THEN status ELSE 'review_required' END,
             remote_release_status = CASE WHEN $4 THEN remote_release_status ELSE 'review_required' END,
             updated_at = CURRENT_TIMESTAMP
         WHERE job_id = $5
           AND release_id = $6
           AND provider_id = $7
           AND COALESCE(remote_release_id, remote_torrent_id, '') = $8
           AND status NOT IN ('completed', 'failed', 'cancelled', 'paused', 'review_required', 'materializing', 'anime_retry_pending')",
    )
    .bind(selected)
    .bind(skipped)
    .bind(error.as_deref())
    .bind(decision.is_approved())
    .bind(&job_id)
    .bind(&release_id)
    .bind(&provider_id)
    .bind(remote_release_id)
    .execute(&mut *transaction)
    .await?;
    if job_update.rows_affected() != 1 {
        transaction.rollback().await?;
        return Ok(false);
    }
    for (release_file_id, selected) in file_updates {
        let update = sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_release_files
             SET selected = $1, updated_at = CURRENT_TIMESTAMP
             WHERE release_file_id = $2 AND release_id = $3",
        )
        .bind(if *selected { 1_i64 } else { 0_i64 })
        .bind(release_file_id.to_string())
        .bind(&release_id)
        .execute(&mut *transaction)
        .await?;
        if update.rows_affected() != 1 {
            transaction.rollback().await?;
            return Ok(false);
        }
    }
    for update in coverage_updates {
        let Some(coverage_id) = update.coverage_id else {
            transaction.rollback().await?;
            return Ok(false);
        };
        let coverage_update = sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_release_coverage
             SET release_file_id = $1, coverage_kind = $2, confidence = $3,
                 score = $4, reason = $5, state = $6, verified_by = $7,
                 updated_at = CURRENT_TIMESTAMP
             WHERE coverage_id = $8 AND release_id = $9",
        )
        .bind(update.release_file_id.map(|value| value.to_string()))
        .bind(update.coverage_kind.as_str())
        .bind(update.confidence.as_str())
        .bind(update.score)
        .bind(update.reason.as_deref())
        .bind(update.state.as_str())
        .bind(update.verified_by.as_deref())
        .bind(coverage_id.to_string())
        .bind(&release_id)
        .execute(&mut *transaction)
        .await?;
        if coverage_update.rows_affected() != 1 {
            transaction.rollback().await?;
            return Ok(false);
        }
    }
    let coverage_plan = merge_selection_policy_evidence(release.coverage_plan.clone(), decision);
    let coverage_plan_json = serde_json::to_string(&coverage_plan)?;
    let release_update = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_releases
         SET state = $1, state_reason = $2, coverage_plan_json = $3,
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = $4
           AND download_id = $5
           AND selected_route_logical_id = $6
           AND selected_provider_id = $7
           AND remote_release_id = $8
           AND state IN ('staging', 'ready', 'submitted', 'downloading', 'materializing')",
    )
    .bind(state.as_str())
    .bind(reason)
    .bind(coverage_plan_json)
    .bind(&release_id)
    .bind(&job_id)
    .bind(DEBRID_DEFAULT_LOGICAL_ID)
    .bind(&provider_id)
    .bind(remote_release_id)
    .execute(&mut *transaction)
    .await?;
    if release_update.rows_affected() != 1 {
        transaction.rollback().await?;
        return Ok(false);
    }
    let release_job_update = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_release_jobs
         SET state = $1, state_reason = $2, active = 1,
             completed_at = NULL, updated_at = CURRENT_TIMESTAMP
         WHERE release_id = $3
           AND download_id = $4
           AND route_logical_id = $5
           AND provider_id = $6
           AND remote_release_id = $7
           AND active = 1",
    )
    .bind(if decision.is_approved() {
        ReleaseJobState::Ready.as_str()
    } else {
        ReleaseJobState::Staging.as_str()
    })
    .bind(reason)
    .bind(&release_id)
    .bind(&job_id)
    .bind(DEBRID_DEFAULT_LOGICAL_ID)
    .bind(&provider_id)
    .bind(remote_release_id)
    .execute(&mut *transaction)
    .await?;
    if release_job_update.rows_affected() != 1 {
        transaction.rollback().await?;
        return Ok(false);
    }
    transaction.commit().await?;
    Ok(true)
}

async fn lock_anime_debrid_selection_attempt(
    transaction: &mut sqlx::Transaction<'_, sqlx::Any>,
    release_id: &str,
    job_id: &str,
    provider_id: &str,
    remote_release_id: &str,
) -> Result<bool> {
    let lock = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_releases
         SET updated_at = updated_at
         WHERE release_id = $1
           AND media_type = 'anime'
           AND download_id = $2
           AND selected_route_logical_id = $3
           AND selected_provider_id = $4
           AND remote_release_id = $5
           AND state IN ('staging', 'ready', 'submitted', 'downloading', 'materializing')
           AND EXISTS (
               SELECT 1 FROM acquisition_release_jobs j
               WHERE j.release_id = acquisition_releases.release_id
                 AND j.download_id = $2
                 AND j.route_logical_id = $3
                 AND j.provider_id = $4
                 AND j.remote_release_id = $5
                 AND j.active = 1
                 AND j.state IN ('staging', 'ready', 'submitted', 'downloading', 'materializing')
           )
           AND EXISTS (
               SELECT 1 FROM debrid_download_jobs d
               WHERE d.job_id = $2
                 AND d.release_id = acquisition_releases.release_id
                 AND d.provider_id = $4
                 AND COALESCE(d.remote_release_id, d.remote_torrent_id, '') = $5
                 AND d.status NOT IN ('completed', 'failed', 'cancelled', 'paused', 'review_required', 'materializing', 'anime_retry_pending')
           )",
    )
    .bind(release_id)
    .bind(job_id)
    .bind(DEBRID_DEFAULT_LOGICAL_ID)
    .bind(provider_id)
    .bind(remote_release_id)
    .execute(&mut **transaction)
    .await
    .context("locking exact anime Debrid selection attempt")?;
    Ok(lock.rows_affected() == 1)
}

async fn mark_debrid_selection_applied(
    pool: &sqlx::AnyPool,
    release: &AcquisitionRelease,
    job_id: Uuid,
    inspection: &DebridReleaseInspection,
) -> Result<bool> {
    if release.media_type == MediaType::Anime {
        let Some(provider_id) = release.selected_provider_id else {
            return Ok(false);
        };
        let release_id = release.release_id.to_string();
        let job_id = job_id.to_string();
        let provider_id = provider_id.to_string();
        let remote_release_id = inspection.release.remote_release_id.trim();
        let mut transaction = pool.begin().await?;
        if remote_release_id.is_empty()
            || !lock_anime_debrid_selection_attempt(
                &mut transaction,
                &release_id,
                &job_id,
                &provider_id,
                remote_release_id,
            )
            .await?
        {
            transaction.rollback().await?;
            return Ok(false);
        }
        persist_anime_debrid_files_in_transaction(
            &mut transaction,
            release.release_id,
            &inspection.files,
        )
        .await?;
        let release_update = sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_releases
             SET state = $1, state_reason = $2, updated_at = CURRENT_TIMESTAMP
             WHERE release_id = $3
               AND download_id = $4
               AND selected_route_logical_id = $5
               AND selected_provider_id = $6
               AND remote_release_id = $7",
        )
        .bind(acquisition_state_for_debrid_status(inspection.release.status).as_str())
        .bind("Debrid provider accepted deterministic file selection.")
        .bind(&release_id)
        .bind(&job_id)
        .bind(DEBRID_DEFAULT_LOGICAL_ID)
        .bind(&provider_id)
        .bind(remote_release_id)
        .execute(&mut *transaction)
        .await?;
        if release_update.rows_affected() != 1 {
            transaction.rollback().await?;
            return Ok(false);
        }
        let state = release_job_state_for_debrid_status(inspection.release.status);
        let terminal = matches!(
            state,
            ReleaseJobState::Completed | ReleaseJobState::Failed | ReleaseJobState::Cancelled
        );
        let release_job_update = sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_release_jobs
             SET state = $1, state_reason = $2, active = $3,
                 completed_at = CASE WHEN $3 = 0 THEN COALESCE(completed_at, CURRENT_TIMESTAMP) ELSE completed_at END,
                 updated_at = CURRENT_TIMESTAMP
             WHERE release_id = $4
               AND download_id = $5
               AND route_logical_id = $6
               AND provider_id = $7
               AND remote_release_id = $8
               AND active = 1",
        )
        .bind(state.as_str())
        .bind("Debrid provider accepted deterministic file selection.")
        .bind(if terminal { 0_i64 } else { 1_i64 })
        .bind(&release_id)
        .bind(&job_id)
        .bind(DEBRID_DEFAULT_LOGICAL_ID)
        .bind(&provider_id)
        .bind(remote_release_id)
        .execute(&mut *transaction)
        .await?;
        if release_job_update.rows_affected() != 1 {
            transaction.rollback().await?;
            return Ok(false);
        }
        transaction.commit().await?;
        return Ok(true);
    }
    update_release_state(
        pool,
        release.release_id,
        acquisition_state_for_debrid_status(inspection.release.status),
        "Debrid provider accepted deterministic file selection.",
        None,
    )
    .await?;
    update_debrid_release_job_selection_state(
        pool,
        release.release_id,
        job_id,
        release_job_state_for_debrid_status(inspection.release.status),
        "Debrid provider accepted deterministic file selection.",
    )
    .await?;
    Ok(true)
}

async fn update_debrid_job_selection_decision(
    pool: &sqlx::AnyPool,
    job_id: Uuid,
    decision: &DebridFileSelectionDecision,
) -> Result<()> {
    let selected = serde_json::to_string(&decision.selected_file_ids)?;
    let skipped = serde_json::to_string(&decision.skipped_file_ids)?;
    let error = (!decision.is_approved()).then(|| decision.review_reasons.join(","));
    sqlx::query::<sqlx::Any>(
        "UPDATE debrid_download_jobs
         SET selected_file_ids_json = $1,
             skipped_file_ids_json = $2,
             selection_error = $3,
             status = CASE WHEN $4 THEN status ELSE 'review_required' END,
             remote_release_status = CASE WHEN $5 THEN remote_release_status ELSE 'review_required' END,
             updated_at = CURRENT_TIMESTAMP
         WHERE job_id = $6",
    )
    .bind(selected)
    .bind(skipped)
    .bind(error.as_deref())
    .bind(decision.is_approved())
    .bind(decision.is_approved())
    .bind(job_id.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

async fn update_release_file_selected(
    pool: &sqlx::AnyPool,
    release_file_id: Uuid,
    selected: bool,
) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_release_files
         SET selected = $1, updated_at = CURRENT_TIMESTAMP
         WHERE release_file_id = $2",
    )
    .bind(if selected { 1_i64 } else { 0_i64 })
    .bind(release_file_id.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

async fn update_debrid_release_selection_evidence(
    pool: &sqlx::AnyPool,
    release: &AcquisitionRelease,
    state: AcquisitionReleaseState,
    reason: &str,
    decision: &DebridFileSelectionDecision,
) -> Result<()> {
    let coverage_plan = merge_selection_policy_evidence(release.coverage_plan.clone(), decision);
    update_release_state(pool, release.release_id, state, reason, Some(coverage_plan)).await
}

async fn update_release_state(
    pool: &sqlx::AnyPool,
    release_id: Uuid,
    state: AcquisitionReleaseState,
    reason: &str,
    coverage_plan: Option<Value>,
) -> Result<()> {
    let coverage_plan_json = coverage_plan
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .context("serializing debrid selection evidence")?;
    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_releases
         SET state = $1,
             state_reason = $2,
             coverage_plan_json = COALESCE($3, coverage_plan_json),
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = $4",
    )
    .bind(state.as_str())
    .bind(reason)
    .bind(coverage_plan_json.as_deref())
    .bind(release_id.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

async fn update_debrid_release_job_selection_state(
    pool: &sqlx::AnyPool,
    release_id: Uuid,
    job_id: Uuid,
    state: ReleaseJobState,
    reason: &str,
) -> Result<()> {
    let terminal = matches!(
        state,
        ReleaseJobState::Completed | ReleaseJobState::Failed | ReleaseJobState::Cancelled
    );
    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_release_jobs
         SET state = $1,
             state_reason = $2,
             active = $3,
             completed_at = CASE WHEN $4 THEN COALESCE(completed_at, CURRENT_TIMESTAMP) ELSE completed_at END,
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = $5
           AND download_id = $6",
    )
    .bind(state.as_str())
    .bind(reason)
    .bind(!terminal)
    .bind(terminal)
    .bind(release_id.to_string())
    .bind(job_id.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

fn merge_selection_policy_evidence(
    mut coverage_plan: Option<Value>,
    decision: &DebridFileSelectionDecision,
) -> Value {
    let evidence = json!({
        "policyVersion": decision.policy_version,
        "status": if decision.is_approved() { "approved" } else { "review_required" },
        "selectedFileIds": decision.selected_file_ids,
        "skippedFileIds": decision.skipped_file_ids,
        "providerSelectionIds": decision.provider_selection_ids,
        "targetFileSelections": decision
            .target_file_selections
            .iter()
            .map(|selection| json!({
                "targetId": selection.target_id,
                "providerFileId": selection.provider_file_id,
            }))
            .collect::<Vec<_>>(),
        "selectAll": decision.select_all,
        "selectAllApproved": decision.select_all_approved,
        "coverageFingerprint": decision.coverage_fingerprint,
        "reviewReasons": decision.review_reasons,
    });
    match coverage_plan.take() {
        Some(Value::Object(mut object)) => {
            object.insert("selectionPolicy".to_string(), evidence);
            Value::Object(object)
        }
        Some(value) => json!({
            "previousCoveragePlan": value,
            "selectionPolicy": evidence
        }),
        None => json!({
            "selectionPolicy": evidence
        }),
    }
}

fn merge_debrid_failure_evidence(coverage_plan: Option<Value>, job: &DebridDownloadJob) -> Value {
    let failure_class = classify_debrid_job_failure(job).unwrap_or(DebridFailureClass::Unknown);
    let evidence = json!({
        "status": "failed",
        "failureClass": failure_class.as_str(),
        "responsePolicy": failure_class.response_policy().as_str(),
        "message": job
            .last_error
            .as_deref()
            .or(job.selection_error.as_deref())
            .unwrap_or("Debrid provider reported a failed release."),
        "jobId": job.job_id,
        "providerId": job.provider_id,
        "providerImplementation": job.provider_implementation,
        "remoteReleaseId": job.remote_release_id,
        "remoteStatus": job.remote_release_status,
        "sourceKind": job.source_kind,
        "fallbackState": debrid_fallback_state(job, Some(failure_class)),
    });
    let coverage_plan = merge_debrid_evidence_object(coverage_plan, "debridFailure", evidence);
    if debrid_failure_suppresses_automatic_rediscovery(failure_class) {
        merge_debrid_evidence_object(
            Some(coverage_plan),
            "retrySuppression",
            json!({
                "status": "rejected",
                "suppressAutomaticRediscovery": true,
                "reason": failure_class.as_str(),
                "responsePolicy": failure_class.response_policy().as_str(),
                "message": job
                    .last_error
                    .as_deref()
                    .or(job.selection_error.as_deref())
                    .unwrap_or("Debrid provider reported a failed release."),
                "jobId": job.job_id,
                "providerId": job.provider_id,
                "providerImplementation": job.provider_implementation,
                "remoteReleaseId": job.remote_release_id,
                "remoteStatus": job.remote_release_status,
            }),
        )
    } else {
        coverage_plan
    }
}

fn debrid_failure_suppresses_automatic_rediscovery(failure_class: DebridFailureClass) -> bool {
    matches!(
        failure_class,
        DebridFailureClass::NoSeeds
            | DebridFailureClass::ProviderStalled
            | DebridFailureClass::StagingTimeout
            | DebridFailureClass::FileListUnavailable
            | DebridFailureClass::MagnetRejected
            | DebridFailureClass::InvalidSource
            | DebridFailureClass::ContentBlocked
            | DebridFailureClass::NotFoundExpired
            | DebridFailureClass::SelectionFailed
    )
}

fn debrid_provider_provenance_evidence(
    provider_id: Uuid,
    implementation: &str,
    capabilities: &DebridProviderCapabilities,
    remote_release_id: Option<&str>,
    remote_status: Option<&str>,
    source_kind: &str,
    job_id: Option<Uuid>,
) -> Value {
    json!({
        "providerId": provider_id,
        "providerImplementation": implementation,
        "providerName": debrid_provider_display_name(implementation),
        "providerCapabilities": capabilities,
        "remoteReleaseId": remote_release_id,
        "remoteStatus": remote_status,
        "sourceKind": source_kind,
        "jobId": job_id,
    })
}

fn merge_debrid_provider_provenance(
    coverage_plan: Option<Value>,
    provider_id: Uuid,
    implementation: &str,
    capabilities: &DebridProviderCapabilities,
    remote_release_id: Option<&str>,
    remote_status: Option<&str>,
    source_kind: &str,
    job_id: Option<Uuid>,
) -> Value {
    merge_debrid_evidence_object(
        coverage_plan,
        "debridProvider",
        debrid_provider_provenance_evidence(
            provider_id,
            implementation,
            capabilities,
            remote_release_id,
            remote_status,
            source_kind,
            job_id,
        ),
    )
}

fn merge_debrid_coverage_plans(existing: Option<Value>, update: Option<Value>) -> Option<Value> {
    match (existing, update) {
        (Some(Value::Object(mut existing)), Some(Value::Object(update))) => {
            for (key, value) in update {
                existing.insert(key, value);
            }
            Some(Value::Object(existing))
        }
        (Some(existing), Some(update)) => Some(json!({
            "previousCoveragePlan": existing,
            "debridCoveragePlan": update
        })),
        (Some(existing), None) => Some(existing),
        (None, Some(update)) => Some(update),
        (None, None) => None,
    }
}

fn merge_debrid_evidence_object(
    mut coverage_plan: Option<Value>,
    key: &str,
    evidence: Value,
) -> Value {
    match coverage_plan.take() {
        Some(Value::Object(mut object)) => {
            object.insert(key.to_string(), evidence);
            Value::Object(object)
        }
        Some(value) => json!({
            "previousCoveragePlan": value,
            key: evidence
        }),
        None => json!({
            key: evidence
        }),
    }
}

fn debrid_coverage_fingerprint(
    release: &AcquisitionRelease,
    files: &[AcquisitionReleaseFile],
    coverage: &[AcquisitionReleaseCoverage],
) -> String {
    let mut rows = Vec::new();
    rows.push(format!(
        "release:{}:{}:{}",
        release.release_id,
        release.release_kind.as_str(),
        release.confidence.as_str()
    ));
    for file in files {
        rows.push(format!(
            "file:{}:{}:{}:{}:{}",
            file.release_file_id,
            file.provider_file_id.as_deref().unwrap_or_default(),
            file.path,
            file.selectable,
            file.size_bytes.unwrap_or_default()
        ));
    }
    for entry in coverage {
        rows.push(format!(
            "coverage:{}:{}:{}:{}:{}",
            entry.coverage_id,
            entry
                .release_file_id
                .map(|id| id.to_string())
                .unwrap_or_default(),
            entry.target_id,
            entry.coverage_kind.as_str(),
            entry.confidence.as_str()
        ));
    }
    rows.sort();
    let mut hasher = Sha256::new();
    hasher.update(rows.join("\n").as_bytes());
    format!("{:x}", hasher.finalize())
}

fn selected_link_urls_from_inspection(inspection: &DebridReleaseInspection) -> Vec<String> {
    selected_links_from_inspection(inspection)
        .into_iter()
        .map(|link| link.url.clone())
        .collect()
}

fn selected_links_from_inspection(
    inspection: &DebridReleaseInspection,
) -> Vec<&DebridResolvedLink> {
    let selected = inspection
        .selection
        .as_ref()
        .map(|selection| {
            selection
                .selected_file_ids
                .iter()
                .cloned()
                .collect::<HashSet<_>>()
        })
        .unwrap_or_default();
    if selected.is_empty() {
        return if inspection.files.is_empty() {
            inspection.links.iter().collect()
        } else {
            Vec::new()
        };
    }
    let mut mapped = inspection
        .links
        .iter()
        .filter(|link| {
            link.provider_file_id
                .as_ref()
                .map(|file_id| selected.contains(file_id))
                .unwrap_or(false)
        })
        .collect::<Vec<_>>();
    if mapped.is_empty() && inspection.links.len() <= selected.len() {
        mapped = inspection.links.iter().collect();
    }
    mapped
}

fn is_debrid_media_file(path: &str) -> bool {
    let lower = path.trim().to_ascii_lowercase();
    [
        ".mkv", ".mp4", ".m4v", ".avi", ".mov", ".wmv", ".ts", ".m2ts", ".webm",
    ]
    .iter()
    .any(|extension| lower.ends_with(extension))
}

fn is_debrid_sample_or_extra_file(path: &str) -> bool {
    let lower = path.replace('\\', "/").to_ascii_lowercase();
    let basename = lower.rsplit('/').next().unwrap_or(&lower);
    basename.contains("sample")
        || basename.contains("trailer")
        || basename.contains("extra")
        || lower.contains("/sample")
        || lower.contains("/extras/")
}

fn debrid_progress_evidence_for_job(job: &DebridDownloadJob) -> DebridBrokerProgressEvidence {
    let failure_class = classify_debrid_job_failure(job);
    DebridBrokerProgressEvidence {
        provider_name: job
            .provider_implementation
            .as_deref()
            .map(debrid_provider_display_name),
        provider_implementation: job.provider_implementation.clone(),
        provider_capabilities: job.provider_capabilities.clone(),
        provider_status: job.provider_status.clone(),
        remote_status: job.remote_release_status.clone(),
        selection_mode: job.selection_mode.clone(),
        selected_file_count: job.selected_file_ids.len(),
        skipped_file_count: job.skipped_file_ids.len(),
        review_reasons: debrid_review_reasons(job.selection_error.as_deref()),
        failure_class: failure_class.map(|class| class.as_str().to_string()),
        last_error: job.last_error.clone(),
        fallback_state: debrid_fallback_state(job, failure_class),
    }
}

fn debrid_provider_display_name(implementation: &str) -> String {
    match DebridServiceKind::from_implementation_id(implementation) {
        Ok(service) => service.display_name().to_string(),
        Err(_) => implementation
            .split(['_', '-'])
            .filter(|part| !part.is_empty())
            .map(|part| {
                let mut chars = part.chars();
                chars
                    .next()
                    .map(|first| first.to_uppercase().collect::<String>() + chars.as_str())
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>()
            .join(" "),
    }
}

fn debrid_review_reasons(selection_error: Option<&str>) -> Vec<String> {
    selection_error
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect()
}

fn debrid_fallback_state(
    job: &DebridDownloadJob,
    failure_class: Option<DebridFailureClass>,
) -> String {
    if job.status == "review_required" {
        "not_attempted_review_required".to_string()
    } else if let Some(failure_class) = failure_class {
        debrid_fallback_state_for_source_kind(&job.source_kind, failure_class)
    } else {
        "not_needed".to_string()
    }
}

fn debrid_fallback_state_for_source_kind(
    source_kind: &str,
    failure_class: DebridFailureClass,
) -> String {
    match failure_class.response_policy() {
        DebridFailureResponsePolicy::TryAlternateRouteOrCandidate
            if source_kind.eq_ignore_ascii_case("magnet") =>
        {
            "eligible_if_candidate_supports_torrent_route".to_string()
        }
        DebridFailureResponsePolicy::TryAlternateRouteOrCandidate => {
            "try_next_candidate".to_string()
        }
        DebridFailureResponsePolicy::RetryProviderLater => "retry_provider_later".to_string(),
        DebridFailureResponsePolicy::AccountActionRequired => {
            "blocked_account_action_required".to_string()
        }
        DebridFailureResponsePolicy::ProviderUnsupported => {
            "blocked_provider_unsupported".to_string()
        }
        DebridFailureResponsePolicy::RetryOrReview => "retry_or_review".to_string(),
        DebridFailureResponsePolicy::Review => "review_required".to_string(),
    }
}

fn classify_debrid_job_failure(job: &DebridDownloadJob) -> Option<DebridFailureClass> {
    classify_debrid_failure(
        &job.status,
        job.remote_release_status.as_deref(),
        job.last_error.as_deref(),
        job.selection_error.as_deref(),
    )
}

fn classify_debrid_failure(
    status: &str,
    remote_status: Option<&str>,
    last_error: Option<&str>,
    selection_error: Option<&str>,
) -> Option<DebridFailureClass> {
    let status = status.trim().to_ascii_lowercase();
    let remote_status = remote_status
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let message = [
        last_error.unwrap_or_default(),
        selection_error.unwrap_or_default(),
        remote_status.as_str(),
        status.as_str(),
    ]
    .join(" ")
    .to_ascii_lowercase();

    let failed = matches!(
        status.as_str(),
        "failed" | "error" | "dead" | "virus" | "magnet_error"
    ) || matches!(
        remote_status.as_str(),
        "failed" | "error" | "dead" | "virus" | "magnet_error"
    );
    if !failed {
        return None;
    }

    if real_debrid_error_code_in(&message, &[28, 35])
        || message.contains("infringing file")
        || message.contains("infringing_file")
        || message.contains("file not allowed")
        || message.contains("file_not_allowed")
        || message.contains("content blocked")
        || message.contains("dmca")
        || message.contains("copyright")
    {
        Some(DebridFailureClass::ContentBlocked)
    } else if real_debrid_error_code_in(&message, &[21])
        || message.contains("too many active downloads")
        || message.contains("too many active")
        || message.contains("maximum allowed active")
        || message.contains("link_too_many_downloads")
        || message.contains("magnet_too_many_active")
        || message.contains("magnet_too_many")
    {
        Some(DebridFailureClass::TooManyActiveDownloads)
    } else if message.contains("account_limit_reached")
        || message.contains("service_limit_reached")
        || message.contains("account limit")
        || message.contains("service limit")
        || message.contains("link_host_limit_reached")
    {
        Some(DebridFailureClass::ProviderAccountLimitReached)
    } else if real_debrid_error_code_in(&message, &[18, 23, 36])
        || message.contains("traffic exhausted")
        || message.contains("fair usage limit")
        || message.contains("fair-use")
        || message.contains("fairuse")
        || message.contains("quota")
        || message.contains("free_trial_limit_reached")
        || message.contains("insufficient_balance")
    {
        Some(DebridFailureClass::QuotaExhausted)
    } else if real_debrid_error_code_in(&message, &[9, 10, 11, 12, 13, 14, 15, 20, 22])
        || message.contains("not premium")
        || message.contains("must be premium")
        || message.contains("free users")
        || message.contains("magnet_must_be_premium")
        || message.contains("magnet_no_server")
        || message.contains("must_be_premium")
        || message.contains("no_server")
        || message.contains("auth_blocked")
        || message.contains("auth_user_banned")
        || message.contains("account_invalid")
        || message.contains("permission_denied")
        || message.contains("account locked")
        || message.contains("account restricted")
        || message.contains("ip address not allowed")
    {
        Some(DebridFailureClass::ProviderAccountRestricted)
    } else if real_debrid_error_code_in(&message, &[8])
        || message.contains("auth_missing_apikey")
        || message.contains("auth_bad_apikey")
        || message.contains("authentication_failed")
        || message.contains("api token")
        || message.contains("apikey")
        || message.contains("api key")
        || message.contains("unauthorized")
        || message.contains("forbidden")
        || message.contains("bad token")
        || message.contains("invalid token")
        || message.contains("401")
        || message.contains("403")
    {
        Some(DebridFailureClass::ProviderAuthMissing)
    } else if real_debrid_error_code_in(&message, &[37])
        || message.contains("native adapter")
        || message.contains("provider unsupported")
        || message.contains("unsupported provider")
        || message.contains("unsupported_container")
    {
        Some(DebridFailureClass::ProviderUnsupported)
    } else if real_debrid_error_code_in(&message, &[5, 34])
        || message.contains("rate limit")
        || message.contains("ratelimit")
        || message.contains("too many requests")
        || message.contains("slow down")
        || message.contains("429")
        || message.contains("rate_limit_reached")
    {
        Some(DebridFailureClass::RateLimited)
    } else if real_debrid_error_code_in(&message, &[7, 24])
        || all_debrid_status_code_in(&message, &[11])
        || message.contains("file unavailable")
        || message.contains("magnet_invalid_id")
        || message.contains("magnet_links_removed")
        || message.contains("link_down")
        || message.contains("delayed_invalid_id")
        || message.contains("not_found")
        || message.contains("not found")
        || message.contains("expired")
        || message.contains("not_found_or_expired")
    {
        Some(DebridFailureClass::NotFoundExpired)
    } else if real_debrid_error_code_in(&message, &[-1, 6, 17, 19, 25])
        || message.contains("provider unavailable")
        || message.contains("maintenance")
        || message.contains("service_down")
        || message.contains("semi_permanent_error")
        || message.contains("link_generation_failed")
        || message.contains("transient_error")
        || message.contains("link_host_unavailable")
        || message.contains("link_host_full")
        || message.contains("link_temporary_unavailable")
        || message.contains("magnet_internal_error")
        || message.contains("unknown_error")
        || message.contains("service unavailable")
        || message.contains("temporar")
        || message.contains("503")
        || message.contains("502")
        || message.contains("504")
        || message.contains("500")
    {
        Some(DebridFailureClass::ProviderUnavailable)
    } else if all_debrid_status_code_in(&message, &[7, 10])
        || message.contains("magnet_cant_bootstrap")
        || message.contains("magnet_took_too_long")
        || message.contains("not downloaded in 20 min")
        || message.contains("download took more than 72h")
        || message.contains("timed out")
        || message.contains("timeout")
    {
        Some(DebridFailureClass::StagingTimeout)
    } else if message.contains("selecting debrid files")
        || message.contains("selectfiles")
        || message.contains("file selection")
        || message.contains("selection failed")
        || message.contains("action already done")
        || real_debrid_error_code_in(&message, &[31])
    {
        Some(DebridFailureClass::SelectionFailed)
    } else if message.contains("unrestrict") {
        Some(DebridFailureClass::UnrestrictFailed)
    } else if message.contains("materializ")
        || message.contains("download returned")
        || message.contains("downloading debrid url")
        || message.contains("writing debrid download")
    {
        Some(DebridFailureClass::MaterializerFailed)
    } else if message.contains("delete") {
        Some(DebridFailureClass::ProviderDeleteFailed)
    } else if message.contains("no_seeds")
        || message.contains("no seeds")
        || message.contains("0 seeds")
        || message.contains("no seeders")
        || message.contains("low seeders")
        || message.contains("no peer")
        || message.contains("no peers")
        || message.contains("file not available - no peer")
        || message.contains("torrent as dead")
        || all_debrid_status_code_in(&message, &[15])
    {
        Some(DebridFailureClass::NoSeeds)
    } else if message.contains("provider_stalled")
        || message.contains("stalled")
        || message.contains("no progress")
        || message.contains("error while contacting tracker")
        || all_debrid_status_code_in(&message, &[14])
    {
        Some(DebridFailureClass::ProviderStalled)
    } else if message.contains("file list")
        || message.contains("no files")
        || message.contains("torrent info")
        || message.contains("not cached")
    {
        Some(DebridFailureClass::FileListUnavailable)
    } else if real_debrid_error_code_in(&message, &[1, 2, 16, 26, 29, 30, 32])
        || all_debrid_status_code_in(&message, &[8])
        || message.contains("magnet_error")
        || message.contains("magnet_invalid")
        || message.contains("magnet_invalid_file")
        || message.contains("magnet rejected")
        || message.contains("invalid magnet")
        || message.contains("bad magnet")
        || message.contains("bad_link")
        || message.contains("link_is_missing")
        || message.contains("link_pass_protected")
        || message.contains("magnet_no_uri")
        || message.contains("magnet_too_large")
        || message.contains("magnet_magnet_too_big")
        || message.contains("magnet_too_big")
        || message.contains("service_unsupported")
        || message.contains("service unsupported")
        || message.contains("link_host_not_supported")
        || message.contains("link_not_supported")
        || message.contains("redirector_not_supported")
        || message.contains("unsupported hoster")
        || message.contains("unsupported_hoster")
        || message.contains("torrent file invalid")
        || message.contains("torrent invalid")
        || message.contains("invalid torrent")
        || message.contains("invalid_request")
        || message.contains("permanent_error")
    {
        if real_debrid_error_code_in(&message, &[1, 2, 16, 26, 29, 30, 32])
            || all_debrid_status_code_in(&message, &[8])
            || message.contains("service_unsupported")
            || message.contains("service unsupported")
            || message.contains("link_host_not_supported")
            || message.contains("link_not_supported")
            || message.contains("redirector_not_supported")
            || message.contains("unsupported hoster")
            || message.contains("unsupported_hoster")
            || message.contains("magnet")
            || message.contains("torrent")
            || message.contains("invalid_request")
        {
            Some(DebridFailureClass::InvalidSource)
        } else {
            Some(DebridFailureClass::MagnetRejected)
        }
    } else if real_debrid_error_code_in(&message, &[27])
        || all_debrid_status_code_in(&message, &[5, 12, 13])
        || message.contains("magnet_file_upload_failed")
        || message.contains("magnet_upload_failed")
        || message.contains("link_error")
        || message.contains("redirector_error")
        || message.contains("download_failed")
        || message.contains("connection")
        || message.contains("connect")
        || message.contains("network")
    {
        if message.contains("connection")
            || message.contains("connect")
            || message.contains("network")
        {
            Some(DebridFailureClass::ProviderUnavailable)
        } else {
            Some(DebridFailureClass::TransferFailed)
        }
    } else if message.contains("transfer")
        || message.contains("downloading")
        || message.contains("download failed")
    {
        Some(DebridFailureClass::TransferFailed)
    } else {
        Some(DebridFailureClass::Unknown)
    }
}

fn real_debrid_error_code_in(message: &str, codes: &[i32]) -> bool {
    numeric_code_after_keys(message, &["error_code"], codes)
}

fn all_debrid_status_code_in(message: &str, codes: &[i32]) -> bool {
    numeric_code_after_keys(
        message,
        &["statuscode", "status_code", "providerstatuscode"],
        codes,
    )
}

fn numeric_code_after_keys(message: &str, keys: &[&str], codes: &[i32]) -> bool {
    keys.iter()
        .any(|key| numeric_codes_after_key(message, key).any(|code| codes.contains(&code)))
}

fn numeric_codes_after_key<'a>(message: &'a str, key: &'a str) -> impl Iterator<Item = i32> + 'a {
    let mut search_from = 0usize;
    std::iter::from_fn(move || {
        while search_from < message.len() {
            let relative = message[search_from..].find(key)?;
            let start = search_from + relative + key.len();
            search_from = start;
            let tail = &message[start..];
            let mut number = String::new();
            let mut seen_separator = false;
            for character in tail.chars().take(32) {
                if character == '-' && number.is_empty() {
                    number.push(character);
                    seen_separator = true;
                    continue;
                }
                if character.is_ascii_digit() {
                    number.push(character);
                    seen_separator = true;
                    continue;
                }
                if number.chars().any(|value| value.is_ascii_digit()) {
                    break;
                }
                if matches!(
                    character,
                    ':' | '=' | '"' | '\'' | ' ' | '\t' | '\n' | '\r' | '{' | '['
                ) {
                    seen_separator = true;
                    continue;
                }
                if seen_separator {
                    break;
                }
            }
            if let Ok(value) = number.parse::<i32>() {
                return Some(value);
            }
        }
        None
    })
}

pub async fn load_debrid_progress(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider_id: Uuid,
    instance_id: Uuid,
) -> Result<Vec<DebridBrokerProgressItem>> {
    let _ = refresh_debrid_remote_state(state, store, provider_id, instance_id).await;
    let jobs = list_debrid_jobs_for_provider(&state.db_pool, provider_id).await?;
    Ok(jobs
        .into_iter()
        .map(|job| DebridBrokerProgressItem {
            id: job.job_id.to_string(),
            name: job
                .display_name
                .clone()
                .or_else(|| file_name_from_path(job.local_path.as_deref()))
                .or_else(|| Some(job.source.clone())),
            state: Some(job.status.clone()),
            category: job.category.clone(),
            local_path: job.local_path.clone(),
            progress: job.progress,
            downloaded_bytes: job.downloaded_bytes,
            total_bytes: job.total_bytes,
            remaining_bytes: remaining_bytes(job.downloaded_bytes, job.total_bytes),
            download_rate_bps: job.download_rate_bps,
            debrid: Some(debrid_progress_evidence_for_job(&job)),
        })
        .collect())
}

#[allow(dead_code)]
pub async fn load_real_debrid_progress(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider_id: Uuid,
    instance_id: Uuid,
) -> Result<Vec<DebridBrokerProgressItem>> {
    load_debrid_progress(state, store, provider_id, instance_id).await
}

pub async fn get_debrid_job_status(
    pool: &sqlx::AnyPool,
    job_id: Uuid,
) -> Result<Option<DebridJobStatus>> {
    let Some(job) = load_debrid_job(pool, job_id).await? else {
        return Ok(None);
    };
    Ok(Some(DebridJobStatus {
        job_id,
        status: job.status.clone(),
        remote_status: job.remote_release_status.clone(),
        source_kind: job.source_kind.clone(),
        release_id: job.release_id,
        failure_class: classify_debrid_job_failure(&job).map(|class| class.as_str().to_string()),
        last_error: job.last_error.clone(),
        selection_error: job.selection_error.clone(),
    }))
}

pub async fn cancel_debrid_job(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider_id: Uuid,
    _instance_id: Uuid,
    download_id: &str,
) -> Result<bool> {
    let Some(job) = find_debrid_job(&state.db_pool, provider_id, download_id).await? else {
        return Ok(false);
    };
    let mut cancel_error = None;
    if let Some(remote_release_id) = job
        .remote_release_id
        .as_deref()
        .or(job.remote_torrent_id.as_deref())
    {
        let factory = DebridAdapterFactory::from_state(state);
        match factory
            .adapter_for_job_implementation(
                store,
                job.instance_id,
                job.provider_implementation.as_deref(),
            )
            .await
        {
            Ok(adapter) => {
                if let Err(err) = adapter.delete_release(remote_release_id).await {
                    cancel_error = Some(err.to_string());
                }
            }
            Err(err) => {
                cancel_error = Some(err.to_string());
            }
        }
    }
    mark_debrid_job_status(
        &state.db_pool,
        job.job_id,
        "cancelled",
        cancel_error.as_deref(),
    )
    .await?;
    Ok(true)
}

#[allow(dead_code)]
pub async fn cancel_real_debrid_job(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider_id: Uuid,
    instance_id: Uuid,
    download_id: &str,
) -> Result<bool> {
    cancel_debrid_job(state, store, provider_id, instance_id, download_id).await
}

async fn process_debrid_jobs_once(state: &AppState) -> Result<()> {
    let cap = active_debrid_concurrent_downloads(&state.db_pool).await?;
    let jobs = list_active_debrid_jobs(&state.db_pool, cap).await?;
    if jobs.is_empty() {
        return Ok(());
    }
    let mut handles = Vec::with_capacity(jobs.len());
    for job in jobs {
        let worker_state = state.clone();
        handles.push(tokio::spawn(async move {
            let store = ExtensionStore::new(&worker_state.db_pool);
            let paths = RuntimePaths::from_roots(
                &worker_state.settings.extensions.storage_root,
                &worker_state.settings.library.local_root,
            );
            let result = process_debrid_job(&worker_state, &store, &paths, job.clone()).await;
            handle_debrid_job_processing_result(&worker_state, &store, &job, result).await?;
            Ok::<(), anyhow::Error>(())
        }));
    }
    for handle in handles {
        handle
            .await
            .map_err(|err| anyhow!("Debrid materializer worker panicked: {err}"))??;
    }
    Ok(())
}

async fn handle_debrid_job_processing_result(
    state: &AppState,
    store: &ExtensionStore<'_>,
    job: &DebridDownloadJob,
    result: Result<()>,
) -> Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(error) => handle_debrid_job_processing_error(state, store, job, &error).await,
    }
}

async fn handle_debrid_job_processing_error(
    state: &AppState,
    store: &ExtensionStore<'_>,
    job: &DebridDownloadJob,
    error: &anyhow::Error,
) -> Result<()> {
    let release = match job.release_id {
        Some(release_id) => get_release(&state.db_pool, release_id).await?,
        None => None,
    };
    let Some(release) = release.filter(|release| release.media_type == MediaType::Anime) else {
        return mark_debrid_job_status(
            &state.db_pool,
            job.job_id,
            "failed",
            Some(&error.to_string()),
        )
        .await;
    };
    if debrid_release_bookkeeping_pending(&release)
        && chrono::Utc::now()
            .signed_duration_since(release.updated_at)
            .num_seconds()
            .max(0)
            < ANIME_DEBRID_CANDIDATE_RETRY_SECONDS
    {
        // The acquisition writer still owns this fresh half-commit. A worker
        // or adapter error must not race it into a terminal retry state.
        return Ok(());
    }

    let retry = anime_debrid_runtime_error_retry_disposition(
        &state.db_pool,
        job.job_id,
        &release,
        "anime_debrid_worker_error",
        error,
    )
    .await?;
    stage_anime_debrid_retry_disposition(&state.db_pool, job.job_id, &retry).await?;
    let remote_release_id = job
        .remote_release_id
        .as_deref()
        .or(job.remote_torrent_id.as_deref())
        .unwrap_or_default();
    let provider_implementation = job
        .provider_implementation
        .as_deref()
        .unwrap_or("unavailable");
    let factory = DebridAdapterFactory::from_state(state);
    match factory
        .adapter_for_job_implementation(
            store,
            job.instance_id,
            job.provider_implementation.as_deref(),
        )
        .await
    {
        Ok(adapter) => {
            persist_anime_debrid_retry_with_adapter(
                &state.db_pool,
                &*adapter,
                job.job_id,
                &release,
                remote_release_id,
                provider_implementation,
                &retry,
            )
            .await
        }
        Err(adapter_error) => {
            tracing::warn!(
                debrid_job_id = %job.job_id,
                provider_implementation,
                "recovering anime Debrid worker failure without remote cleanup: {adapter_error}"
            );
            let cleanup = sanitize_anime_automatic_resolution_evidence(json!({
                "status": "adapter_unavailable",
                "deleted": false,
                "error": adapter_error.to_string()
            }));
            persist_anime_debrid_retry(
                &state.db_pool,
                job.job_id,
                &release,
                remote_release_id,
                &retry,
                cleanup,
            )
            .await
        }
    }
}

async fn process_debrid_job(
    state: &AppState,
    store: &ExtensionStore<'_>,
    paths: &RuntimePaths,
    job: DebridDownloadJob,
) -> Result<()> {
    if job.status == "paused" || job.status == "cancelled" {
        return Ok(());
    }
    let mut job = job;
    let factory = DebridAdapterFactory::from_state(state);
    if let Some(release_id) = job.release_id
        && let Some(release) = get_release(&state.db_pool, release_id).await?
        && release.media_type == MediaType::Anime
        && debrid_release_bookkeeping_pending(&release)
    {
        let bookkeeping_age = chrono::Utc::now()
            .signed_duration_since(release.updated_at)
            .num_seconds()
            .max(0);
        if bookkeeping_age < ANIME_DEBRID_CANDIDATE_RETRY_SECONDS {
            return Ok(());
        }
        let error = anyhow!(
            "anime Debrid submission bookkeeping did not complete before its recovery deadline"
        );
        let retry = anime_debrid_runtime_error_retry_disposition(
            &state.db_pool,
            job.job_id,
            &release,
            "anime_debrid_submission_bookkeeping_timeout",
            &error,
        )
        .await?;
        stage_anime_debrid_retry_disposition(&state.db_pool, job.job_id, &retry).await?;
        let remote_release_id = job
            .remote_release_id
            .as_deref()
            .or(job.remote_torrent_id.as_deref())
            .unwrap_or_default();
        let provider_implementation = job
            .provider_implementation
            .as_deref()
            .unwrap_or("unavailable");
        match factory
            .adapter_for_job_implementation(
                store,
                job.instance_id,
                job.provider_implementation.as_deref(),
            )
            .await
        {
            Ok(adapter) => {
                persist_anime_debrid_retry_with_adapter(
                    &state.db_pool,
                    &*adapter,
                    job.job_id,
                    &release,
                    remote_release_id,
                    provider_implementation,
                    &retry,
                )
                .await?;
            }
            Err(adapter_error) => {
                persist_anime_debrid_retry(
                    &state.db_pool,
                    job.job_id,
                    &release,
                    remote_release_id,
                    &retry,
                    sanitize_anime_automatic_resolution_evidence(json!({
                        "status": "adapter_unavailable",
                        "deleted": false,
                        "error": adapter_error.to_string()
                    })),
                )
                .await?;
            }
        }
        return Ok(());
    }
    if stage_deferred_anime_debrid_provider_failure_if_ready(&state.db_pool, &job).await? {
        job = load_debrid_job(&state.db_pool, job.job_id)
            .await?
            .ok_or_else(|| anyhow!("Debrid job disappeared while staging provider failure"))?;
    }
    if let Some(automatic_retry) = anime_debrid_retry_disposition_from_job(&job)
        && let Some(release_id) = job.release_id
        && let Some(release) = get_release(&state.db_pool, release_id).await?
        && release.media_type == MediaType::Anime
    {
        let remote_release_id = job
            .remote_release_id
            .clone()
            .or_else(|| job.remote_torrent_id.clone())
            .unwrap_or_default();
        let provider_implementation = job
            .provider_implementation
            .clone()
            .unwrap_or_else(|| "unavailable".to_string());
        match factory
            .adapter_for_job_implementation(
                store,
                job.instance_id,
                job.provider_implementation.as_deref(),
            )
            .await
        {
            Ok(adapter) => {
                persist_anime_debrid_retry_with_adapter(
                    &state.db_pool,
                    &*adapter,
                    job.job_id,
                    &release,
                    &remote_release_id,
                    &provider_implementation,
                    &automatic_retry,
                )
                .await?;
            }
            Err(error) => {
                tracing::warn!(
                    debrid_job_id = %job.job_id,
                    provider_implementation,
                    "consuming staged anime Debrid retry without remote cleanup: {error}"
                );
                persist_anime_debrid_retry(
                    &state.db_pool,
                    job.job_id,
                    &release,
                    &remote_release_id,
                    &automatic_retry,
                    json!({
                        "status": "adapter_unavailable",
                        "deleted": false,
                        "error": error.to_string()
                    }),
                )
                .await?;
            }
        }
        return Ok(());
    }
    let adapter = factory
        .adapter_for_job_implementation(
            store,
            job.instance_id,
            job.provider_implementation.as_deref(),
        )
        .await?;
    let remote_torrent_release_id = job.remote_torrent_id.clone().or_else(|| {
        (job.source_kind == "magnet")
            .then(|| job.remote_release_id.clone())
            .flatten()
    });
    let can_materialize_premiumize_directdl_without_refresh = job
        .provider_implementation
        .as_deref()
        .map(|implementation| {
            DebridServiceKind::Premiumize
                .implementation_id()
                .eq_ignore_ascii_case(implementation)
        })
        .unwrap_or(false)
        && job
            .remote_release_id
            .as_deref()
            .map(is_premiumize_directdl_release_id)
            .unwrap_or(false)
        && !job.links.is_empty();
    if !can_materialize_premiumize_directdl_without_refresh
        && let Some(remote_release_id) = remote_torrent_release_id
    {
        let inspection = adapter.inspect_release(&remote_release_id).await?;
        if !update_debrid_job_from_inspection(&state.db_pool, job.job_id, &inspection).await? {
            return Ok(());
        }
        if consume_failed_anime_debrid_inspection(
            &state.db_pool,
            &*adapter,
            job.job_id,
            &inspection,
        )
        .await?
        {
            return Ok(());
        }
        cleanup_uncached_no_seed_release(&state.db_pool, &*adapter, job.job_id, &inspection)
            .await?;
        job = load_debrid_job(&state.db_pool, job.job_id)
            .await?
            .ok_or_else(|| anyhow!("Debrid job disappeared during refresh"))?;
        if let Some(release_id) = job.release_id
            && let Some(release) = get_release(&state.db_pool, release_id).await?
            && release.media_type == MediaType::Anime
            && !anime_debrid_attempt_is_current(
                &state.db_pool,
                &release,
                job.provider_id,
                job.job_id,
                &inspection.release.remote_release_id,
            )
            .await?
        {
            mark_stale_anime_debrid_provider_job(
                &state.db_pool,
                job.job_id,
                "Superseded by a newer anime Debrid attempt.",
            )
            .await?;
            return Ok(());
        }
        if let Some(release_id) = job.release_id
            && let Some(release) = get_release(&state.db_pool, release_id).await?
            && replay_ready_anime_debrid_provider_selection(
                &state.db_pool,
                &*adapter,
                &job,
                &release,
                &inspection,
            )
            .await?
        {
            // Ready is durable provider intent. Reconciliation or replay owns
            // this tick; a later inspection advances transfer/materialization.
            return Ok(());
        }
        if matches!(
            inspection.release.status,
            DebridReleaseStatus::WaitingFiles | DebridReleaseStatus::Downloaded
        ) && job.source_kind == "magnet"
            && !inspection.files.is_empty()
            && let Some(release_id) = job.release_id
            && let Some(release) = crate::acquisition::release_resolution::store::get_release(
                &state.db_pool,
                release_id,
            )
            .await?
            && debrid_provider_selection_needs_application(&job.selected_file_ids, release.state)
        {
            let release_context = DebridReleaseSubmitContext {
                subscription_id: release.subscription_id,
                source_provider_id: release.source_provider_id.or(Some(job.provider_id)),
                source_extension_id: release.source_extension_id.clone(),
                media_type: release.media_type,
                title: release.title.clone(),
                release_title: release.release_title.clone(),
                info_hash: release.info_hash.clone(),
                fingerprint: Some(release.fingerprint.clone()),
                score: release.score,
                selected_candidate: release.selected_candidate.clone(),
            };
            let options = DebridSubmitOptions {
                owner_id: &job.owner_id,
                category: job.category.as_deref(),
                name: job.display_name.as_deref(),
                paused: false,
                release_context: Some(release_context),
            };
            let provider_capabilities = adapter.capabilities();
            let anime_matching = state.anime_inference.matching_service();
            let refinement = persist_debrid_file_list_and_refine_coverage(
                &state.db_pool,
                &release,
                &options,
                &inspection,
                &anime_matching,
            )
            .await?;
            let refinement_coverage_plan = merge_debrid_coverage_plans(
                release.coverage_plan.clone(),
                refinement.coverage_plan.clone(),
            );
            let automatic_retry = refinement.automatic_retry.clone();
            let coverage_plan = Some(merge_debrid_provider_provenance(
                refinement_coverage_plan,
                job.provider_id,
                adapter.implementation(),
                &provider_capabilities,
                Some(&inspection.release.remote_release_id),
                Some(inspection.release.status.as_str()),
                &job.source_kind,
                Some(job.job_id),
            ));
            let updated_release = if release.media_type == MediaType::Anime {
                let Some(updated) = commit_anime_debrid_refinement_if_owned(
                    &state.db_pool,
                    &release,
                    job.provider_id,
                    job.job_id,
                    &inspection,
                    &refinement,
                    coverage_plan,
                )
                .await?
                else {
                    mark_stale_anime_debrid_provider_job(
                        &state.db_pool,
                        job.job_id,
                        "Superseded by a newer anime Debrid attempt.",
                    )
                    .await?;
                    return Ok(());
                };
                updated
            } else {
                let updated = upsert_debrid_acquisition_release(
                    &state.db_pool,
                    job.provider_id,
                    &job.source,
                    &job.source_kind,
                    &options,
                    Some(&inspection.release.remote_release_id),
                    Some(&job.job_id.to_string()),
                    refinement.state,
                    refinement.state_reason.as_deref(),
                    refinement.shape.clone(),
                    coverage_plan,
                )
                .await?
                .unwrap_or(release);
                upsert_debrid_release_job(
                    &state.db_pool,
                    &updated,
                    job.provider_id,
                    job.job_id,
                    Some(&inspection.release.remote_release_id),
                    refinement.job_state,
                    refinement
                        .job_state_reason
                        .as_deref()
                        .unwrap_or("Debrid release inspected and staged."),
                )
                .await?;
                updated
            };
            if let Some(automatic_retry) = automatic_retry.as_ref() {
                stage_anime_debrid_retry_disposition(&state.db_pool, job.job_id, automatic_retry)
                    .await?;
                persist_anime_debrid_retry_with_adapter(
                    &state.db_pool,
                    &*adapter,
                    job.job_id,
                    &updated_release,
                    &inspection.release.remote_release_id,
                    &inspection.release.provider_implementation,
                    automatic_retry,
                )
                .await?;
                return Ok(());
            }
            if refinement.apply_file_selection_policy {
                let _ = apply_debrid_file_selection_policy(
                    &state.db_pool,
                    &*adapter,
                    job.job_id,
                    &updated_release,
                    &inspection,
                    true,
                )
                .await?;
            }
            job = load_debrid_job(&state.db_pool, job.job_id)
                .await?
                .ok_or_else(|| anyhow!("Debrid job disappeared during selection"))?;
            if job.status == "review_required" {
                return Ok(());
            }
        }
        if inspection.release.status == DebridReleaseStatus::WaitingFiles {
            return Ok(());
        }
        if inspection.release.status != DebridReleaseStatus::Downloaded {
            return Ok(());
        }
        if job.links.is_empty() && !inspection.links.is_empty() {
            let links = selected_link_urls_from_inspection(&inspection);
            update_debrid_job_links(&state.db_pool, job.job_id, &links).await?;
            job.links = links;
        }
    }
    if job.links.is_empty() {
        return Ok(());
    }
    if job.source_kind == "magnet" && job.selected_file_ids.is_empty() {
        return Ok(());
    }

    materialize_debrid_links(state, &*adapter, paths, &job).await
}

async fn stage_deferred_anime_debrid_provider_failure_if_ready(
    pool: &sqlx::AnyPool,
    job: &DebridDownloadJob,
) -> Result<bool> {
    if job.status != "anime_retry_pending"
        || anime_debrid_retry_disposition_from_job(job).is_some()
        || job.remote_release_status.as_deref() != Some(DebridReleaseStatus::Failed.as_str())
    {
        return Ok(false);
    }
    let Some(release_id) = job.release_id else {
        return Ok(false);
    };
    let Some(release) = get_release(pool, release_id).await? else {
        return Ok(false);
    };
    if release.media_type != MediaType::Anime || debrid_release_bookkeeping_pending(&release) {
        return Ok(false);
    }

    // The provider can reject a magnet synchronously while the acquisition
    // writer still owns its bookkeeping barrier. Once that barrier closes,
    // turn the already-durable provider result into the normal automatic
    // retry marker without requiring another provider inspection.
    let error = anyhow!("Debrid provider rejected the anime release during submission");
    let retry = anime_debrid_runtime_error_retry_disposition(
        pool,
        job.job_id,
        &release,
        "anime_debrid_provider_failed",
        &error,
    )
    .await?;
    stage_anime_debrid_retry_disposition(pool, job.job_id, &retry).await
}

/// `Ready` is the durable provider-selection intent. The selected IDs are
/// persisted before the remote call, so a restart must replay them until the
/// provider response advances the release away from `Ready`.
fn debrid_provider_selection_needs_application(
    selected_file_ids: &[String],
    release_state: AcquisitionReleaseState,
) -> bool {
    selected_file_ids.is_empty() || release_state == AcquisitionReleaseState::Ready
}

fn debrid_inspection_confirms_provider_selection_applied(
    inspection: &DebridReleaseInspection,
    expected_file_ids: &[String],
) -> bool {
    if expected_file_ids.is_empty() {
        return false;
    }
    if matches!(
        inspection.release.status,
        DebridReleaseStatus::Selected
            | DebridReleaseStatus::Transferring
            | DebridReleaseStatus::Downloaded
            | DebridReleaseStatus::Materializing
            | DebridReleaseStatus::Completed
    ) {
        return true;
    }

    let expected = expected_file_ids
        .iter()
        .map(|file_id| file_id.trim())
        .filter(|file_id| !file_id.is_empty())
        .collect::<BTreeSet<_>>();
    let observed = inspection
        .selection
        .as_ref()
        .into_iter()
        .flat_map(|selection| selection.selected_file_ids.iter())
        .map(|file_id| file_id.trim())
        .filter(|file_id| !file_id.is_empty())
        .chain(
            inspection
                .files
                .iter()
                .filter(|file| file.selected == Some(true))
                .map(|file| file.provider_file_id.trim())
                .filter(|file_id| !file_id.is_empty()),
        )
        .collect::<BTreeSet<_>>();
    !expected.is_empty() && observed == expected
}

/// Reconcile or replay an exact persisted anime selection before recomputing
/// coverage. The provider call is outside the database transaction, so exact
/// attempt ownership is checked both before and after it; the final applied
/// state is committed through the existing owner-CAS transaction.
async fn replay_ready_anime_debrid_provider_selection<A: DebridProviderAdapter + ?Sized>(
    pool: &sqlx::AnyPool,
    adapter: &A,
    job: &DebridDownloadJob,
    release: &AcquisitionRelease,
    inspection: &DebridReleaseInspection,
) -> Result<bool> {
    if release.media_type != MediaType::Anime
        || release.state != AcquisitionReleaseState::Ready
        || job.selected_file_ids.is_empty()
    {
        return Ok(false);
    }
    let Some(provider_id) = release.selected_provider_id else {
        mark_stale_anime_debrid_provider_job(
            pool,
            job.job_id,
            "Anime Debrid Ready selection lost its provider ownership.",
        )
        .await?;
        return Ok(true);
    };
    let remote_release_id = inspection.release.remote_release_id.trim();
    if remote_release_id.is_empty()
        || !anime_debrid_attempt_is_current(
            pool,
            release,
            provider_id,
            job.job_id,
            remote_release_id,
        )
        .await?
    {
        mark_stale_anime_debrid_provider_job(
            pool,
            job.job_id,
            "Superseded before replaying anime Debrid provider selection.",
        )
        .await?;
        return Ok(true);
    }

    if debrid_inspection_confirms_provider_selection_applied(inspection, &job.selected_file_ids) {
        if !mark_debrid_selection_applied(pool, release, job.job_id, inspection).await? {
            mark_stale_anime_debrid_provider_job(
                pool,
                job.job_id,
                "Superseded while reconciling anime Debrid provider selection.",
            )
            .await?;
        }
        return Ok(true);
    }
    if !inspection.capabilities.supports_file_selection {
        return Ok(false);
    }

    let selected = adapter
        .select_files(remote_release_id, &job.selected_file_ids)
        .await
        .with_context(|| {
            format!(
                "replaying persisted anime Debrid selection for remote release '{remote_release_id}'"
            )
        })?;
    if !anime_debrid_attempt_is_current(pool, release, provider_id, job.job_id, remote_release_id)
        .await?
    {
        mark_stale_anime_debrid_provider_job(
            pool,
            job.job_id,
            "Superseded during anime Debrid provider-selection replay.",
        )
        .await?;
        return Ok(true);
    }
    if !update_debrid_job_from_inspection(pool, job.job_id, &selected).await? {
        return Ok(true);
    }
    if consume_failed_anime_debrid_inspection(pool, adapter, job.job_id, &selected).await? {
        return Ok(true);
    }
    if !mark_debrid_selection_applied(pool, release, job.job_id, &selected).await? {
        mark_stale_anime_debrid_provider_job(
            pool,
            job.job_id,
            "Superseded while committing replayed anime Debrid provider selection.",
        )
        .await?;
    }
    Ok(true)
}

async fn materialize_debrid_links(
    state: &AppState,
    adapter: &dyn DebridProviderAdapter,
    paths: &RuntimePaths,
    job: &DebridDownloadJob,
) -> Result<()> {
    let anime_release = match job.release_id {
        Some(release_id) => get_release(&state.db_pool, release_id)
            .await?
            .filter(|release| release.media_type == MediaType::Anime),
        None => None,
    };
    if anime_release.is_some() {
        if !transition_anime_debrid_runtime_if_owned(
            &state.db_pool,
            job.job_id,
            AnimeDebridRuntimeTransition::Materializing,
            None,
        )
        .await?
        {
            return Ok(());
        }
    } else {
        mark_debrid_job_status(&state.db_pool, job.job_id, "materializing", None).await?;
    }
    let mut target_dir = Path::new(&paths.downloads_root).join(
        job.category
            .as_deref()
            .map(safe_path_segment)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "debrid".to_string()),
    );
    if job.links.len() > 1 {
        let pack_dir = job
            .display_name
            .as_deref()
            .map(safe_path_segment)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| job.job_id.to_string());
        target_dir = target_dir.join(pack_dir);
    }
    tokio::fs::create_dir_all(&target_dir)
        .await
        .with_context(|| format!("creating debrid download dir '{}'", target_dir.display()))?;

    let mut completed_paths = Vec::new();
    for link in &job.links {
        let unrestricted = adapter.unrestrict_hoster(link).await?;
        let download_url = unrestricted.url.as_str();
        let filename = unrestricted
            .filename
            .as_deref()
            .or(job.display_name.as_deref())
            .map(safe_file_name)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| format!("debrid-{}.bin", job.job_id));
        let target_path = unique_target_path(&target_dir, &filename).await;
        download_url_to_file(
            &state.db_pool,
            job.job_id,
            &target_path,
            download_url,
            unrestricted.size_bytes,
        )
        .await?;
        completed_paths.push(target_path);
    }
    let local_path = if completed_paths.len() == 1 {
        completed_paths
            .first()
            .map(|path| path.to_string_lossy().to_string())
    } else {
        Some(target_dir.to_string_lossy().to_string())
    };
    if let Err(err) =
        queue_anime_hashes_for_completed_debrid_paths(state, job, &completed_paths).await
    {
        tracing::warn!(
            debrid_job_id = %job.job_id,
            "queueing anime hash jobs failed: {err}"
        );
    }
    if !mark_debrid_job_completed(&state.db_pool, job.job_id, local_path.as_deref()).await? {
        return Ok(());
    }
    Ok(())
}

async fn queue_anime_hashes_for_completed_debrid_paths(
    state: &AppState,
    job: &DebridDownloadJob,
    completed_paths: &[PathBuf],
) -> Result<()> {
    let Some(release) = get_release_by_download_id(&state.db_pool, &job.job_id.to_string()).await?
    else {
        return Ok(());
    };
    if release.media_type != MediaType::Anime {
        return Ok(());
    }
    let release_files = list_release_files(&state.db_pool, release.release_id).await?;
    for (index, path) in completed_paths.iter().enumerate() {
        let release_file_id =
            match_completed_release_file(path, &release_files, completed_paths.len())
                .map(|file| file.release_file_id);
        let basename = path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("file");
        let local_file_id = release_file_id
            .map(|id| format!("release-file:{id}"))
            .unwrap_or_else(|| format!("debrid:{}:{index}:{basename}", job.job_id));
        queue_anime_hash_file(
            &state.db_pool,
            HashFileJob {
                release_file_id,
                local_file_id: Some(local_file_id),
                file_path: path.clone(),
                force_rehash: false,
            },
        )
        .await?;
    }
    Ok(())
}

fn match_completed_release_file<'a>(
    completed_path: &Path,
    release_files: &'a [AcquisitionReleaseFile],
    completed_count: usize,
) -> Option<&'a AcquisitionReleaseFile> {
    let basename = completed_path
        .file_name()
        .and_then(|value| value.to_str())?;
    release_files
        .iter()
        .find(|file| {
            file.basename == basename
                || Path::new(&file.path)
                    .file_name()
                    .and_then(|value| value.to_str())
                    == Some(basename)
        })
        .or_else(|| (completed_count == 1 && release_files.len() == 1).then(|| &release_files[0]))
}

async fn download_url_to_file(
    pool: &sqlx::AnyPool,
    job_id: Uuid,
    target_path: &Path,
    url: &str,
    expected_size: Option<u64>,
) -> Result<()> {
    let client = Client::builder()
        .user_agent(REAL_DEBRID_USER_AGENT)
        .timeout(Duration::from_secs(30 * 60))
        .build()
        .context("building debrid materializer HTTP client")?;

    if let Some(parent) = target_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let tmp_path = target_path.with_extension("elixir-part");
    let resume_from = tokio::fs::metadata(&tmp_path)
        .await
        .ok()
        .filter(|metadata| metadata.is_file())
        .map(|metadata| metadata.len())
        .unwrap_or(0);

    let mut request = client.get(url);
    if resume_from > 0 {
        request = request.header(RANGE, format!("bytes={resume_from}-"));
    }
    let mut response = request
        .send()
        .await
        .context("requesting debrid provider download")?;
    let status = response.status();
    if status == StatusCode::RANGE_NOT_SATISFIABLE
        && expected_size.is_some_and(|size| resume_from >= size)
    {
        tokio::fs::rename(&tmp_path, target_path)
            .await
            .with_context(|| {
                format!(
                    "moving completed debrid partial '{}' to '{}'",
                    tmp_path.display(),
                    target_path.display()
                )
            })?;
        update_debrid_job_download_progress(pool, job_id, resume_from, expected_size, Some(0))
            .await?;
        update_debrid_job_local_path(pool, job_id, &target_path.to_string_lossy()).await?;
        return Ok(());
    }
    if !status.is_success() {
        bail!("Debrid provider download returned {status}");
    }
    let append_existing = resume_from > 0 && status == StatusCode::PARTIAL_CONTENT;
    let total = expected_size.or_else(|| {
        response.content_length().map(|content_length| {
            if append_existing {
                resume_from.saturating_add(content_length)
            } else {
                content_length
            }
        })
    });
    let mut file = if append_existing {
        tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&tmp_path)
            .await
            .with_context(|| format!("opening '{}' for resume", tmp_path.display()))?
    } else {
        tokio::fs::File::create(&tmp_path)
            .await
            .with_context(|| format!("creating '{}'", tmp_path.display()))?
    };
    let mut downloaded = if append_existing { resume_from } else { 0 };
    let mut last_update = Instant::now();
    let mut last_downloaded = downloaded;
    if downloaded > 0 {
        update_debrid_job_download_progress(pool, job_id, downloaded, total, Some(0)).await?;
    }
    while let Some(chunk) = response.chunk().await.context("reading debrid download")? {
        file.write_all(&chunk).await?;
        downloaded = downloaded.saturating_add(chunk.len() as u64);
        if last_update.elapsed() >= Duration::from_secs(1) {
            let elapsed = last_update.elapsed().as_secs_f64().max(0.001);
            let rate = ((downloaded.saturating_sub(last_downloaded)) as f64 / elapsed) as u64;
            update_debrid_job_download_progress(pool, job_id, downloaded, total, Some(rate))
                .await?;
            last_update = Instant::now();
            last_downloaded = downloaded;
        }
    }
    file.flush().await?;
    drop(file);
    tokio::fs::rename(&tmp_path, target_path)
        .await
        .with_context(|| {
            format!(
                "moving debrid download '{}' to '{}'",
                tmp_path.display(),
                target_path.display()
            )
        })?;
    update_debrid_job_download_progress(pool, job_id, downloaded, total, Some(0)).await?;
    update_debrid_job_local_path(pool, job_id, &target_path.to_string_lossy()).await?;
    Ok(())
}

async fn refresh_debrid_remote_state(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider_id: Uuid,
    _instance_id: Uuid,
) -> Result<()> {
    let jobs = list_refreshable_debrid_jobs(&state.db_pool, provider_id).await?;
    let factory = DebridAdapterFactory::from_state(state);
    for job in jobs {
        if let Some(release_id) = job.release_id
            && let Some(release) = get_release(&state.db_pool, release_id).await?
            && release.media_type == MediaType::Anime
            && debrid_release_bookkeeping_pending(&release)
        {
            // Acquisition still owns the submit-to-bind window. The worker
            // performs bounded timeout recovery; status polling is read-only
            // until that durable barrier closes.
            continue;
        }
        let adapter = match factory
            .adapter_for_job_implementation(
                store,
                job.instance_id,
                job.provider_implementation.as_deref(),
            )
            .await
        {
            Ok(adapter) => adapter,
            Err(err) => {
                handle_debrid_refresh_error(&state.db_pool, &job, &err).await?;
                continue;
            }
        };
        let remote_torrent_release_id = job.remote_torrent_id.as_deref().or_else(|| {
            (job.source_kind == "magnet")
                .then_some(job.remote_release_id.as_deref())
                .flatten()
        });
        if let Some(remote_release_id) = remote_torrent_release_id {
            match adapter.inspect_release(remote_release_id).await {
                Ok(inspection) => {
                    if !update_debrid_job_from_inspection(&state.db_pool, job.job_id, &inspection)
                        .await?
                    {
                        continue;
                    }
                    if consume_failed_anime_debrid_inspection(
                        &state.db_pool,
                        &*adapter,
                        job.job_id,
                        &inspection,
                    )
                    .await?
                    {
                        continue;
                    }
                    cleanup_uncached_no_seed_release(
                        &state.db_pool,
                        &*adapter,
                        job.job_id,
                        &inspection,
                    )
                    .await?;
                }
                Err(err) => {
                    handle_debrid_refresh_error(&state.db_pool, &job, &err).await?;
                }
            }
        }
    }
    Ok(())
}

async fn handle_debrid_refresh_error(
    pool: &sqlx::AnyPool,
    job: &DebridDownloadJob,
    error: &anyhow::Error,
) -> Result<()> {
    let message = error.to_string();
    match classify_debrid_failure("failed", Some("failed"), Some(&message), None) {
        Some(DebridFailureClass::ProviderAuthMissing | DebridFailureClass::ProviderUnsupported) => {
            mark_debrid_job_status(pool, job.job_id, "failed", Some(&message)).await?;
        }
        _ => {
            update_debrid_job_error(pool, job.job_id, &message).await?;
        }
    }
    Ok(())
}

async fn insert_debrid_job(pool: &sqlx::AnyPool, job: &DebridDownloadJob) -> Result<()> {
    let links_json = serde_json::to_string(&job.links)?;
    let provider_capabilities_json = json_value_to_string(job.provider_capabilities.as_ref())?;
    let provider_status_json = json_value_to_string(job.provider_status.as_ref())?;
    let selected_file_ids_json = serde_json::to_string(&job.selected_file_ids)?;
    let skipped_file_ids_json = serde_json::to_string(&job.skipped_file_ids)?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO debrid_download_jobs (
            job_id, provider_id, instance_id, owner_id, source, source_kind, category,
            display_name, remote_torrent_id, remote_download_id, status, local_path,
            links_json, progress, downloaded_bytes, total_bytes, download_rate_bps, last_error,
            provider_implementation, remote_release_id, remote_release_status,
            provider_capabilities_json, provider_status_json, selection_mode,
            selected_file_ids_json, skipped_file_ids_json, selection_error, release_id
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28)",
    )
    .bind(job.job_id.to_string())
    .bind(job.provider_id.to_string())
    .bind(job.instance_id.to_string())
    .bind(&job.owner_id)
    .bind(&job.source)
    .bind(&job.source_kind)
    .bind(job.category.as_deref())
    .bind(job.display_name.as_deref())
    .bind(job.remote_torrent_id.as_deref())
    .bind(job.remote_download_id.as_deref())
    .bind(&job.status)
    .bind(job.local_path.as_deref())
    .bind(links_json)
    .bind(job.progress)
    .bind(job.downloaded_bytes.and_then(u64_to_i64))
    .bind(job.total_bytes.and_then(u64_to_i64))
    .bind(job.download_rate_bps.and_then(u64_to_i64))
    .bind(job.last_error.as_deref())
    .bind(job.provider_implementation.as_deref())
    .bind(job.remote_release_id.as_deref())
    .bind(job.remote_release_status.as_deref())
    .bind(provider_capabilities_json.as_deref())
    .bind(provider_status_json.as_deref())
    .bind(job.selection_mode.as_deref())
    .bind(selected_file_ids_json)
    .bind(skipped_file_ids_json)
    .bind(job.selection_error.as_deref())
    .bind(job.release_id.map(|value| value.to_string()))
    .execute(pool)
    .await?;
    Ok(())
}

macro_rules! debrid_job_columns {
    () => {
        "job_id,
provider_id,
instance_id,
owner_id,
source,
source_kind,
COALESCE(CAST(category AS TEXT), '') as category,
COALESCE(CAST(display_name AS TEXT), '') as display_name,
COALESCE(CAST(remote_torrent_id AS TEXT), '') as remote_torrent_id,
COALESCE(CAST(remote_download_id AS TEXT), '') as remote_download_id,
COALESCE(CAST(provider_implementation AS TEXT), '') as provider_implementation,
COALESCE(CAST(remote_release_id AS TEXT), '') as remote_release_id,
COALESCE(CAST(remote_release_status AS TEXT), '') as remote_release_status,
COALESCE(CAST(provider_capabilities_json AS TEXT), '') as provider_capabilities_json,
COALESCE(CAST(provider_status_json AS TEXT), '') as provider_status_json,
COALESCE(CAST(selection_mode AS TEXT), '') as selection_mode,
COALESCE(CAST(selected_file_ids_json AS TEXT), '[]') as selected_file_ids_json,
COALESCE(CAST(skipped_file_ids_json AS TEXT), '[]') as skipped_file_ids_json,
COALESCE(CAST(selection_error AS TEXT), '') as selection_error,
COALESCE(CAST(release_id AS TEXT), '') as release_id,
status,
COALESCE(CAST(local_path AS TEXT), '') as local_path,
links_json,
progress,
downloaded_bytes,
total_bytes,
download_rate_bps,
COALESCE(CAST(last_error AS TEXT), '') as last_error"
    };
}

async fn list_debrid_jobs_for_provider(
    pool: &sqlx::AnyPool,
    provider_id: Uuid,
) -> Result<Vec<DebridDownloadJob>> {
    let rows = sqlx::query(concat!(
        "SELECT ",
        debrid_job_columns!(),
        " FROM debrid_download_jobs
         WHERE provider_id = $1
         ORDER BY updated_at DESC
         LIMIT 100"
    ))
    .bind(provider_id.to_string())
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(|row| map_debrid_job(&row)).collect()
}

async fn list_active_debrid_jobs(
    pool: &sqlx::AnyPool,
    limit: i64,
) -> Result<Vec<DebridDownloadJob>> {
    let rows = sqlx::query(concat!(
        "SELECT ",
        debrid_job_columns!(),
        " FROM debrid_download_jobs
         WHERE (
               status NOT IN ('completed', 'failed', 'cancelled', 'paused', 'review_required', 'materializing')
               OR (
                   status = 'failed'
                   AND COALESCE(CAST(provider_status_json AS TEXT), '') LIKE '%\"animeAutomaticRetry\"%'
               )
               OR (
                   status = 'review_required'
                   AND COALESCE(selection_error, '') LIKE '%no_selected_files%'
               )
         )
         ORDER BY updated_at ASC
         LIMIT $1"
    ))
    .bind(limit)
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(|row| map_debrid_job(&row)).collect()
}

async fn count_active_debrid_jobs_for_instance(
    pool: &sqlx::AnyPool,
    instance_id: Uuid,
) -> Result<i64> {
    let count = sqlx::query_scalar::<sqlx::Any, i64>(
        "SELECT COUNT(*)
         FROM debrid_download_jobs
         WHERE instance_id = $1
           AND status NOT IN ('completed', 'failed', 'cancelled', 'paused', 'review_required')",
    )
    .bind(instance_id.to_string())
    .fetch_one(pool)
    .await
    .context("counting active Debrid jobs for instance")?;
    Ok(count)
}

async fn list_refreshable_debrid_jobs(
    pool: &sqlx::AnyPool,
    provider_id: Uuid,
) -> Result<Vec<DebridDownloadJob>> {
    let rows = sqlx::query(concat!(
        "SELECT ",
        debrid_job_columns!(),
        " FROM debrid_download_jobs
         WHERE provider_id = $1
           AND (remote_torrent_id IS NOT NULL OR remote_release_id IS NOT NULL)
           AND status NOT IN ('completed', 'failed', 'cancelled', 'paused', 'review_required', 'materializing', 'anime_retry_pending')
         ORDER BY updated_at DESC
         LIMIT 50"
    ))
    .bind(provider_id.to_string())
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(|row| map_debrid_job(&row)).collect()
}

async fn find_debrid_job(
    pool: &sqlx::AnyPool,
    provider_id: Uuid,
    download_id: &str,
) -> Result<Option<DebridDownloadJob>> {
    let row = sqlx::query(concat!(
        "SELECT ",
        debrid_job_columns!(),
        " FROM debrid_download_jobs
         WHERE provider_id = $1
           AND (job_id = $2 OR remote_torrent_id = $3 OR remote_download_id = $4 OR remote_release_id = $5)
         LIMIT 1"
    ))
    .bind(provider_id.to_string())
    .bind(download_id)
    .bind(download_id)
    .bind(download_id)
    .bind(download_id)
    .fetch_optional(pool)
    .await?;
    row.map(|row| map_debrid_job(&row)).transpose()
}

async fn load_debrid_job(pool: &sqlx::AnyPool, job_id: Uuid) -> Result<Option<DebridDownloadJob>> {
    let row = sqlx::query(concat!(
        "SELECT ",
        debrid_job_columns!(),
        " FROM debrid_download_jobs
         WHERE job_id = $1
         LIMIT 1"
    ))
    .bind(job_id.to_string())
    .fetch_optional(pool)
    .await?;
    row.map(|row| map_debrid_job(&row)).transpose()
}

fn debrid_provider_status_from_inspection(inspection: &DebridReleaseInspection) -> Value {
    let provider_status = inspection
        .raw
        .as_ref()
        .and_then(|raw| raw.get("providerStatus"))
        .or_else(|| {
            inspection
                .release
                .raw
                .as_ref()
                .and_then(|raw| raw.get("providerStatus"))
        })
        .or_else(|| {
            inspection
                .progress
                .as_ref()
                .and_then(|progress| progress.raw.as_ref())
                .and_then(|raw| raw.get("providerStatus"))
        });
    if let Some(provider_status) = provider_status {
        return provider_status.clone();
    }
    json!({
        "providerImplementation": inspection.release.provider_implementation.clone(),
        "providerName": debrid_provider_display_name(&inspection.release.provider_implementation),
        "status": inspection.release.status.as_str(),
        "providerState": inspection.release.raw_status.clone(),
        "rawStatus": inspection.release.raw_status.clone(),
        "remoteReleaseId": inspection.release.remote_release_id.clone(),
        "progress": inspection.progress.as_ref().and_then(|progress| progress.progress),
        "downloadedBytes": inspection.progress.as_ref().and_then(|progress| progress.downloaded_bytes),
        "totalBytes": inspection.progress.as_ref().and_then(|progress| progress.total_bytes),
        "downloadRateBps": inspection.progress.as_ref().and_then(|progress| progress.download_rate_bps),
        "fileCount": inspection.files.len(),
    })
}

fn debrid_failure_message_from_inspection(inspection: &DebridReleaseInspection) -> Option<String> {
    if inspection.release.status != DebridReleaseStatus::Failed {
        return None;
    }
    let provider_status = debrid_provider_status_from_inspection(inspection);
    if let Some(message) = provider_status
        .get("message")
        .and_then(Value::as_str)
        .and_then(non_empty)
    {
        return Some(message.to_string());
    }
    let provider_name = debrid_provider_display_name(&inspection.release.provider_implementation);
    let raw_status = inspection.release.raw_status.as_deref().and_then(non_empty);
    Some(match raw_status {
        Some(raw_status) => {
            format!("{provider_name} reported failed provider state: {raw_status}.")
        }
        None => format!("{provider_name} reported a failed release."),
    })
}

async fn cleanup_uncached_no_seed_release(
    pool: &sqlx::AnyPool,
    adapter: &(impl DebridProviderAdapter + ?Sized),
    job_id: Uuid,
    inspection: &DebridReleaseInspection,
) -> Result<()> {
    if !should_cleanup_uncached_no_seed_release(inspection) {
        return Ok(());
    }
    let remote_release_id = inspection.release.remote_release_id.trim();
    if remote_release_id.is_empty() {
        return Ok(());
    }
    let provider_status = debrid_provider_status_from_inspection(inspection);
    let mut evidence = json!({
        "providerImplementation": inspection.release.provider_implementation,
        "providerName": debrid_provider_display_name(&inspection.release.provider_implementation),
        "remoteReleaseId": remote_release_id,
        "reason": "uncached_no_seeds",
        "providerFailureClass": provider_status.get("providerFailureClass").cloned(),
        "notCached": provider_status.get("notCached").and_then(Value::as_bool).unwrap_or(false),
        "noSeeds": provider_status.get("noSeeds").and_then(Value::as_bool).unwrap_or(false),
        "attemptedAt": chrono::Utc::now().to_rfc3339(),
    });
    match adapter.delete_release(remote_release_id).await {
        Ok(true) => {
            evidence["status"] = json!("deleted");
            evidence["deleted"] = json!(true);
        }
        Ok(false) => {
            evidence["status"] = json!("already_absent");
            evidence["deleted"] = json!(false);
        }
        Err(err) => {
            evidence["status"] = json!("delete_failed");
            evidence["deleted"] = json!(false);
            evidence["error"] = json!(err.to_string());
            tracing::warn!(
                debrid_job_id = %job_id,
                remote_release_id,
                provider_implementation = %inspection.release.provider_implementation,
                "debrid uncached no-seeds cleanup failed: {err}"
            );
        }
    }
    persist_debrid_provider_cleanup_evidence(pool, job_id, evidence).await
}

fn should_cleanup_uncached_no_seed_release(inspection: &DebridReleaseInspection) -> bool {
    if inspection.release.status != DebridReleaseStatus::Failed {
        return false;
    }
    let provider_status = debrid_provider_status_from_inspection(inspection);
    let no_seeds = provider_status
        .get("noSeeds")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        && provider_status
            .get("providerFailureClass")
            .and_then(Value::as_str)
            .map(|value| value.eq_ignore_ascii_case(DebridFailureClass::NoSeeds.as_str()))
            .unwrap_or(false);
    let not_cached = provider_status
        .get("notCached")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || provider_status
            .get("cached")
            .and_then(Value::as_bool)
            .is_some_and(|cached| !cached);
    no_seeds && not_cached
}

async fn persist_debrid_provider_cleanup_evidence(
    pool: &sqlx::AnyPool,
    job_id: Uuid,
    evidence: Value,
) -> Result<()> {
    let Some(job) = load_debrid_job(pool, job_id).await? else {
        return Ok(());
    };
    let provider_status = merge_debrid_evidence_object(
        job.provider_status.clone(),
        "providerCleanup",
        evidence.clone(),
    );
    let provider_status_json = serde_json::to_string(&provider_status)
        .context("serializing debrid provider cleanup evidence")?;
    sqlx::query::<sqlx::Any>(
        "UPDATE debrid_download_jobs
         SET provider_status_json = $1, updated_at = CURRENT_TIMESTAMP
         WHERE job_id = $2",
    )
    .bind(provider_status_json)
    .bind(job_id.to_string())
    .execute(pool)
    .await?;

    let Some(release_id) = job.release_id else {
        return Ok(());
    };
    let Some(release) = get_release(pool, release_id).await? else {
        return Ok(());
    };
    let coverage_plan = merge_debrid_evidence_object(
        release.coverage_plan.clone(),
        "debridProviderCleanup",
        evidence,
    );
    update_release_state(
        pool,
        release.release_id,
        release.state,
        release
            .state_reason
            .as_deref()
            .unwrap_or("Debrid provider cleanup evidence updated."),
        Some(coverage_plan),
    )
    .await
}

async fn update_debrid_job_from_inspection(
    pool: &sqlx::AnyPool,
    job_id: Uuid,
    inspection: &DebridReleaseInspection,
) -> Result<bool> {
    // Inspection is an optimistic state transition, not an unconditional
    // snapshot write. The worker and the status endpoint may inspect the same
    // provider release concurrently; a response that started before an anime
    // retry was claimed must never resurrect that terminal/pending attempt.
    let Some(snapshot) = sqlx::query::<sqlx::Any>(
        "SELECT status,
                COALESCE(remote_release_status, '') AS remote_release_status,
                COALESCE(provider_status_json, '') AS provider_status_json,
                COALESCE(selected_file_ids_json, '') AS selected_file_ids_json,
                COALESCE(skipped_file_ids_json, '') AS skipped_file_ids_json,
                COALESCE(selection_error, '') AS selection_error,
                COALESCE(release_id, '') AS release_id
         FROM debrid_download_jobs
         WHERE job_id = $1",
    )
    .bind(job_id.to_string())
    .fetch_optional(pool)
    .await?
    else {
        return Ok(false);
    };
    let expected_status: String = snapshot.try_get("status")?;
    let expected_remote_status: String = snapshot.try_get("remote_release_status")?;
    let expected_provider_status: String = snapshot.try_get("provider_status_json")?;
    let expected_selected_file_ids: String = snapshot.try_get("selected_file_ids_json")?;
    let expected_skipped_file_ids: String = snapshot.try_get("skipped_file_ids_json")?;
    let expected_selection_error: String = snapshot.try_get("selection_error")?;
    let expected_release_id: String = snapshot.try_get("release_id")?;
    if matches!(
        expected_status.as_str(),
        "completed"
            | "failed"
            | "cancelled"
            | "paused"
            | "review_required"
            | "materializing"
            | "anime_retry_pending"
    ) {
        return Ok(false);
    }
    if debrid_remote_status_progress_rank(&expected_remote_status)
        .zip(debrid_remote_status_progress_rank(
            inspection.release.status.as_str(),
        ))
        .is_some_and(|(current, incoming)| incoming < current)
    {
        return Ok(false);
    }

    let failed_job = if inspection.release.status == DebridReleaseStatus::Failed {
        load_debrid_job(pool, job_id).await?
    } else {
        None
    };
    let failed_release = match failed_job.as_ref().and_then(|job| job.release_id) {
        Some(release_id) => get_release(pool, release_id).await?,
        None => None,
    };
    let anime_retry = if let Some(release) = failed_release
        .as_ref()
        .filter(|release| release.media_type == MediaType::Anime)
    {
        let error = anyhow!(
            "{}",
            debrid_failure_message_from_inspection(inspection).unwrap_or_else(|| {
                "Debrid provider reported a failed anime release.".to_string()
            })
        );
        Some(
            anime_debrid_runtime_error_retry_disposition(
                pool,
                job_id,
                release,
                "anime_debrid_provider_failed",
                &error,
            )
            .await?,
        )
    } else {
        None
    };
    // Failure state and the automatic disposition are one durable write. A
    // restart can therefore either retry the inspection or consume this
    // marker; it can never observe a terminal anime job with no retry intent.
    let status = if anime_retry.is_some() {
        "anime_retry_pending".to_string()
    } else {
        debrid_status_to_job_status(inspection.release.status)
    };
    let links = selected_link_urls_from_inspection(inspection);
    let links_json = serde_json::to_string(&links)?;
    let provider_capabilities_json = serde_json::to_string(&inspection.capabilities)?;
    let mut provider_status = debrid_provider_status_from_inspection(inspection);
    if let Some(retry) = anime_retry.as_ref() {
        provider_status = merge_debrid_evidence_object(
            Some(provider_status),
            "animeAutomaticRetry",
            serde_json::to_value(retry)
                .context("serializing atomic anime Debrid retry disposition")?,
        );
    }
    let provider_status_json = serde_json::to_string(&provider_status)?;
    let failure_message = debrid_failure_message_from_inspection(inspection);
    let (selected_file_ids, skipped_file_ids) = inspection
        .selection
        .as_ref()
        .filter(|selection| !selection.selected_file_ids.is_empty())
        .map(|selection| {
            (
                selection.selected_file_ids.clone(),
                selection.skipped_file_ids.clone(),
            )
        })
        .unwrap_or_default();
    let selected_file_ids_json = serde_json::to_string(&selected_file_ids)?;
    let skipped_file_ids_json = serde_json::to_string(&skipped_file_ids)?;
    let progress = inspection.progress.as_ref();
    let update = sqlx::query::<sqlx::Any>(
        "UPDATE debrid_download_jobs
         SET status = $1, remote_release_status = $2, display_name = COALESCE(display_name, $3),
             links_json = CASE WHEN $4 != '[]' THEN $5 ELSE links_json END,
             progress = $6, downloaded_bytes = $7, total_bytes = $8, download_rate_bps = $9,
             provider_implementation = $10,
             remote_release_id = COALESCE(remote_release_id, $11),
             provider_capabilities_json = $12,
             provider_status_json = $13,
             last_error = $14,
             selection_mode = $15,
             selected_file_ids_json = CASE WHEN $16 != '[]' THEN $17 ELSE selected_file_ids_json END,
             skipped_file_ids_json = CASE WHEN $18 != '[]' THEN $19 ELSE skipped_file_ids_json END,
             updated_at = CURRENT_TIMESTAMP
         WHERE job_id = $20
           AND status = $21
           AND COALESCE(remote_release_status, '') = $22
           AND COALESCE(provider_status_json, '') = $23
           AND COALESCE(selected_file_ids_json, '') = $24
           AND COALESCE(skipped_file_ids_json, '') = $25
           AND COALESCE(selection_error, '') = $26
           AND COALESCE(release_id, '') = $27",
    )
    .bind(&status)
    .bind(inspection.release.status.as_str())
    .bind(inspection.release.display_name.as_deref())
    .bind(&links_json)
    .bind(&links_json)
    .bind(progress.and_then(|progress| progress.progress))
    .bind(
        progress
            .and_then(|progress| progress.downloaded_bytes)
            .and_then(u64_to_i64),
    )
    .bind(
        progress
            .and_then(|progress| progress.total_bytes)
            .and_then(u64_to_i64),
    )
    .bind(
        progress
            .and_then(|progress| progress.download_rate_bps)
            .and_then(u64_to_i64),
    )
    .bind(&inspection.release.provider_implementation)
    .bind(&inspection.release.remote_release_id)
    .bind(provider_capabilities_json)
    .bind(provider_status_json)
    .bind(failure_message.as_deref())
    .bind(
        inspection
            .capabilities
            .file_selection_mode
            .as_persistence_value(),
    )
    .bind(&selected_file_ids_json)
    .bind(&selected_file_ids_json)
    .bind(&skipped_file_ids_json)
    .bind(&skipped_file_ids_json)
    .bind(job_id.to_string())
    .bind(&expected_status)
    .bind(&expected_remote_status)
    .bind(&expected_provider_status)
    .bind(&expected_selected_file_ids)
    .bind(&expected_skipped_file_ids)
    .bind(&expected_selection_error)
    .bind(&expected_release_id)
    .execute(pool)
    .await?;
    if update.rows_affected() != 1 {
        return Ok(false);
    }
    if inspection.release.status == DebridReleaseStatus::Failed
        && anime_retry.is_none()
        && let Some(job) = load_debrid_job(pool, job_id).await?
    {
        record_debrid_release_failure_evidence(pool, &job).await?;
    }
    Ok(true)
}

fn debrid_remote_status_progress_rank(status: &str) -> Option<u8> {
    match status.trim().to_ascii_lowercase().as_str() {
        "staging" => Some(0),
        "waiting_files" => Some(1),
        "selected" => Some(2),
        "transferring" => Some(3),
        "downloaded" => Some(4),
        "materializing" => Some(5),
        "completed" => Some(6),
        _ => None,
    }
}

async fn consume_failed_anime_debrid_inspection<A: DebridProviderAdapter + ?Sized>(
    pool: &sqlx::AnyPool,
    adapter: &A,
    job_id: Uuid,
    inspection: &DebridReleaseInspection,
) -> Result<bool> {
    if inspection.release.status != DebridReleaseStatus::Failed {
        return Ok(false);
    }
    let Some(job) = load_debrid_job(pool, job_id).await? else {
        return Ok(false);
    };
    let Some(release_id) = job.release_id else {
        return Ok(false);
    };
    let Some(release) = get_release(pool, release_id).await? else {
        return Ok(false);
    };
    if release.media_type != MediaType::Anime {
        return Ok(false);
    }
    let retry = if let Some(retry) = anime_debrid_retry_disposition_from_job(&job) {
        retry
    } else {
        // Legacy/direct callers may reach the consumer without the atomic
        // inspection writer. Stage first so a crash before persistence is
        // still recoverable by the materializer.
        let error = anyhow!(
            "{}",
            debrid_failure_message_from_inspection(inspection)
                .unwrap_or_else(|| "Debrid provider reported a failed anime release.".to_string())
        );
        let retry = anime_debrid_runtime_error_retry_disposition(
            pool,
            job_id,
            &release,
            "anime_debrid_provider_failed",
            &error,
        )
        .await?;
        stage_anime_debrid_retry_disposition(pool, job_id, &retry).await?;
        retry
    };
    persist_anime_debrid_retry_with_adapter(
        pool,
        adapter,
        job_id,
        &release,
        &inspection.release.remote_release_id,
        &inspection.release.provider_implementation,
        &retry,
    )
    .await?;
    Ok(true)
}

async fn update_debrid_job_links(
    pool: &sqlx::AnyPool,
    job_id: Uuid,
    links: &[String],
) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE debrid_download_jobs SET links_json = $1, updated_at = CURRENT_TIMESTAMP WHERE job_id = $2",
    )
    .bind(serde_json::to_string(links)?)
    .bind(job_id.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

async fn update_debrid_job_download_progress(
    pool: &sqlx::AnyPool,
    job_id: Uuid,
    downloaded: u64,
    total: Option<u64>,
    rate: Option<u64>,
) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE debrid_download_jobs
         SET status = 'materializing', downloaded_bytes = $1, total_bytes = COALESCE($2, total_bytes),
             progress = $3, download_rate_bps = $4, updated_at = CURRENT_TIMESTAMP
         WHERE job_id = $5 AND status = 'materializing'",
    )
    .bind(u64_to_i64(downloaded))
    .bind(total.and_then(u64_to_i64))
    .bind(progress_fraction(Some(downloaded), total))
    .bind(rate.and_then(u64_to_i64))
    .bind(job_id.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

async fn update_debrid_job_local_path(
    pool: &sqlx::AnyPool,
    job_id: Uuid,
    path: &str,
) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE debrid_download_jobs
         SET local_path = $1, updated_at = CURRENT_TIMESTAMP
         WHERE job_id = $2 AND status = 'materializing'",
    )
    .bind(path)
    .bind(job_id.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

async fn mark_debrid_job_status(
    pool: &sqlx::AnyPool,
    job_id: Uuid,
    status: &str,
    error: Option<&str>,
) -> Result<()> {
    if status == "failed"
        && let Some(job) = load_debrid_job(pool, job_id).await?
        && let Some(release_id) = job.release_id
        && let Some(release) = get_release(pool, release_id)
            .await?
            .filter(|release| release.media_type == MediaType::Anime)
    {
        let failure = anyhow!(
            "{}",
            error.unwrap_or("Debrid provider reported an anime runtime failure.")
        );
        let retry = anime_debrid_runtime_error_retry_disposition(
            pool,
            job_id,
            &release,
            "anime_debrid_runtime_failure",
            &failure,
        )
        .await?;
        if !stage_anime_debrid_retry_disposition(pool, job_id, &retry).await? {
            return Ok(());
        }
        persist_anime_debrid_retry(
            pool,
            job_id,
            &release,
            job.remote_release_id
                .as_deref()
                .or(job.remote_torrent_id.as_deref())
                .unwrap_or_default(),
            &retry,
            sanitize_anime_automatic_resolution_evidence(json!({
                "status": "not_attempted",
                "deleted": false,
                "reason": "provider_adapter_not_available_in_failure_writer"
            })),
        )
        .await?;
        return Ok(());
    }

    sqlx::query::<sqlx::Any>(
        "UPDATE debrid_download_jobs
         SET status = $1, remote_release_status = $2, last_error = $3, updated_at = CURRENT_TIMESTAMP
         WHERE job_id = $4",
    )
    .bind(status)
    .bind(status)
    .bind(error)
    .bind(job_id.to_string())
    .execute(pool)
    .await?;
    if let Some(job) = load_debrid_job(pool, job_id).await? {
        match status {
            "failed" => {
                record_debrid_release_failure_evidence(pool, &job).await?;
            }
            "materializing" => {
                sync_debrid_release_runtime_state(
                    pool,
                    &job,
                    AcquisitionReleaseState::Materializing,
                    ReleaseJobState::Materializing,
                    "Debrid materializer is downloading selected files.",
                    Some(ReleaseCoverageState::Submitted),
                    Some(AcquisitionTargetState::Submitted),
                )
                .await?;
            }
            _ => {}
        }
    }
    Ok(())
}

async fn update_debrid_job_error(pool: &sqlx::AnyPool, job_id: Uuid, error: &str) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE debrid_download_jobs SET last_error = $1, updated_at = CURRENT_TIMESTAMP WHERE job_id = $2",
    )
    .bind(error)
    .bind(job_id.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

async fn record_debrid_release_failure_evidence(
    pool: &sqlx::AnyPool,
    job: &DebridDownloadJob,
) -> Result<()> {
    let Some(release_id) = job.release_id else {
        return Ok(());
    };
    let Some(release) = get_release(pool, release_id).await? else {
        return Ok(());
    };
    let failure_class = classify_debrid_job_failure(job).unwrap_or(DebridFailureClass::Unknown);
    let message = job
        .last_error
        .as_deref()
        .or(job.selection_error.as_deref())
        .unwrap_or("Debrid provider reported a failed release.");
    let coverage_plan = merge_debrid_failure_evidence(release.coverage_plan.clone(), job);
    update_release_state(
        pool,
        release_id,
        AcquisitionReleaseState::Failed,
        &format!("Debrid failure [{}]: {}", failure_class.as_str(), message),
        Some(coverage_plan),
    )
    .await?;
    update_debrid_release_job_selection_state(
        pool,
        release_id,
        job.job_id,
        ReleaseJobState::Failed,
        &format!("Debrid failure [{}]: {}", failure_class.as_str(), message),
    )
    .await?;
    Ok(())
}

async fn record_debrid_release_failure_without_job(
    pool: &sqlx::AnyPool,
    release: Option<&AcquisitionRelease>,
    job_id: Uuid,
    provider_id: Uuid,
    adapter: &(impl DebridProviderAdapter + ?Sized),
    capabilities: &DebridProviderCapabilities,
    source_kind: &str,
    error: &anyhow::Error,
) -> Result<()> {
    let Some(release) = release else {
        return Ok(());
    };
    if release.media_type == MediaType::Anime {
        let retry = anime_debrid_runtime_error_retry_disposition(
            pool,
            job_id,
            release,
            "anime_debrid_provider_submit_error",
            error,
        )
        .await?;
        let now = chrono::Utc::now();
        let target_ids = retry.target_ids.iter().copied().collect::<BTreeSet<_>>();
        let coverage_plan = merge_anime_debrid_retry_evidence(
            merge_debrid_coverage_plans(release.coverage_plan.clone(), retry.coverage_plan.clone()),
            job_id,
            "",
            &retry,
            &target_ids,
            json!({
                "status": "not_applicable",
                "deleted": false,
                "reason": "provider_submission_failed_before_remote_creation"
            }),
            now,
        );
        let reason =
            "Anime provider submission failed. Elixir will try another release automatically.";
        update_release_state(
            pool,
            release.release_id,
            AcquisitionReleaseState::Failed,
            reason,
            Some(coverage_plan),
        )
        .await?;
        for target_id in target_ids {
            let retry_after = now
                + chrono::Duration::seconds(
                    ANIME_DEBRID_CANDIDATE_RETRY_SECONDS + i64::from(target_id.as_bytes()[0] % 15),
                );
            reset_target_for_candidate_retry(pool, target_id, reason.to_string(), retry_after)
                .await?;
        }
        upsert_debrid_release_job(
            pool,
            release,
            provider_id,
            job_id,
            None,
            ReleaseJobState::Failed,
            reason,
        )
        .await?;
        return Ok(());
    }
    let message = error.to_string();
    let failure_class = classify_debrid_failure("failed", Some("failed"), Some(&message), None)
        .unwrap_or(DebridFailureClass::Unknown);
    let evidence = json!({
        "status": "failed",
        "failureClass": failure_class.as_str(),
        "responsePolicy": failure_class.response_policy().as_str(),
        "message": message,
        "fallbackState": debrid_fallback_state_for_source_kind(source_kind, failure_class),
        "stage": "provider_submit",
        "providerId": provider_id,
        "providerImplementation": adapter.implementation(),
        "providerName": debrid_provider_display_name(adapter.implementation()),
        "providerCapabilities": capabilities,
        "sourceKind": source_kind,
    });
    let coverage_plan = merge_debrid_evidence_object(
        Some(merge_debrid_provider_provenance(
            release.coverage_plan.clone(),
            provider_id,
            adapter.implementation(),
            capabilities,
            None,
            Some(DebridReleaseStatus::Failed.as_str()),
            source_kind,
            None,
        )),
        "debridFailure",
        evidence,
    );
    update_release_state(
        pool,
        release.release_id,
        AcquisitionReleaseState::Failed,
        &format!("Debrid failure [{}]: {}", failure_class.as_str(), error),
        Some(coverage_plan),
    )
    .await
}

async fn mark_debrid_job_completed(
    pool: &sqlx::AnyPool,
    job_id: Uuid,
    local_path: Option<&str>,
) -> Result<bool> {
    if let Some(job) = load_debrid_job(pool, job_id).await?
        && let Some(release_id) = job.release_id
        && get_release(pool, release_id)
            .await?
            .is_some_and(|release| release.media_type == MediaType::Anime)
    {
        return transition_anime_debrid_runtime_if_owned(
            pool,
            job_id,
            AnimeDebridRuntimeTransition::Completed,
            local_path,
        )
        .await;
    }
    sqlx::query::<sqlx::Any>(
        "UPDATE debrid_download_jobs
         SET status = 'completed', local_path = COALESCE($1, local_path), progress = 1.0,
             download_rate_bps = 0, completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
         WHERE job_id = $2",
    )
    .bind(local_path)
    .bind(job_id.to_string())
    .execute(pool)
    .await?;
    if let Some(job) = load_debrid_job(pool, job_id).await? {
        sync_debrid_release_runtime_state(
            pool,
            &job,
            AcquisitionReleaseState::Completed,
            ReleaseJobState::Completed,
            "Debrid materializer completed selected files.",
            Some(ReleaseCoverageState::Submitted),
            Some(AcquisitionTargetState::Submitted),
        )
        .await?;
    }
    Ok(true)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AnimeDebridRuntimeTransition {
    Materializing,
    Completed,
}

async fn transition_anime_debrid_runtime_if_owned(
    pool: &sqlx::AnyPool,
    job_id: Uuid,
    transition: AnimeDebridRuntimeTransition,
    local_path: Option<&str>,
) -> Result<bool> {
    let Some(job) = load_debrid_job(pool, job_id).await? else {
        return Ok(false);
    };
    let Some(release_id) = job.release_id else {
        return Ok(false);
    };
    let Some(release) = get_release(pool, release_id).await? else {
        return Ok(false);
    };
    if release.media_type != MediaType::Anime {
        return Ok(false);
    }
    let Some(remote_release_id) = job
        .remote_release_id
        .as_deref()
        .or(job.remote_torrent_id.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(false);
    };
    let release_id = release_id.to_string();
    let job_id = job_id.to_string();
    let provider_id = job.provider_id.to_string();
    let mut transaction = pool.begin().await?;
    let expected_job_predicate = match transition {
        AnimeDebridRuntimeTransition::Materializing => {
            "d.status NOT IN ('completed', 'failed', 'cancelled', 'paused', 'review_required', 'materializing', 'anime_retry_pending')"
        }
        AnimeDebridRuntimeTransition::Completed => "d.status = 'materializing'",
    };
    let ownership_sql = format!(
        "UPDATE acquisition_releases
         SET updated_at = updated_at
         WHERE release_id = $1
           AND media_type = 'anime'
           AND download_id = $2
           AND selected_route_logical_id = $3
           AND selected_provider_id = $4
           AND remote_release_id = $5
           AND state IN ('staging', 'ready', 'submitted', 'downloading', 'materializing')
           AND EXISTS (
               SELECT 1 FROM acquisition_release_jobs j
               WHERE j.release_id = acquisition_releases.release_id
                 AND j.download_id = $2
                 AND j.route_logical_id = $3
                 AND j.provider_id = $4
                 AND j.remote_release_id = $5
                 AND j.active = 1
                 AND j.state IN ('staging', 'ready', 'submitted', 'downloading', 'materializing')
           )
           AND EXISTS (
               SELECT 1 FROM debrid_download_jobs d
               WHERE d.job_id = $2
                 AND d.release_id = acquisition_releases.release_id
                 AND d.provider_id = $4
                 AND COALESCE(d.remote_release_id, d.remote_torrent_id, '') = $5
                 AND {expected_job_predicate}
           )"
    );
    let ownership = sqlx::query::<sqlx::Any>(&ownership_sql)
        .bind(&release_id)
        .bind(&job_id)
        .bind(DEBRID_DEFAULT_LOGICAL_ID)
        .bind(&provider_id)
        .bind(remote_release_id)
        .execute(&mut *transaction)
        .await
        .context("claiming exact anime Debrid materializer transition")?;
    if ownership.rows_affected() != 1 {
        transaction.rollback().await?;
        return Ok(false);
    }

    let job_update = match transition {
        AnimeDebridRuntimeTransition::Materializing => sqlx::query::<sqlx::Any>(
            "UPDATE debrid_download_jobs
             SET status = 'materializing', remote_release_status = 'materializing',
                 last_error = NULL, updated_at = CURRENT_TIMESTAMP
             WHERE job_id = $1 AND release_id = $2 AND provider_id = $3
               AND COALESCE(remote_release_id, remote_torrent_id, '') = $4
               AND status NOT IN ('completed', 'failed', 'cancelled', 'paused', 'review_required', 'materializing', 'anime_retry_pending')",
        )
        .bind(&job_id)
        .bind(&release_id)
        .bind(&provider_id)
        .bind(remote_release_id)
        .execute(&mut *transaction)
        .await?,
        AnimeDebridRuntimeTransition::Completed => sqlx::query::<sqlx::Any>(
            "UPDATE debrid_download_jobs
             SET status = 'completed', remote_release_status = 'completed',
                 local_path = COALESCE($1, local_path), progress = 1.0,
                 download_rate_bps = 0, completed_at = CURRENT_TIMESTAMP,
                 updated_at = CURRENT_TIMESTAMP
             WHERE job_id = $2 AND release_id = $3 AND provider_id = $4
               AND COALESCE(remote_release_id, remote_torrent_id, '') = $5
               AND status = 'materializing'",
        )
        .bind(local_path)
        .bind(&job_id)
        .bind(&release_id)
        .bind(&provider_id)
        .bind(remote_release_id)
        .execute(&mut *transaction)
        .await?,
    };
    if job_update.rows_affected() != 1 {
        transaction.rollback().await?;
        return Ok(false);
    }

    let (release_state, release_job_state, reason, terminal) = match transition {
        AnimeDebridRuntimeTransition::Materializing => (
            AcquisitionReleaseState::Materializing,
            ReleaseJobState::Materializing,
            "Debrid materializer is downloading selected files.",
            false,
        ),
        AnimeDebridRuntimeTransition::Completed => (
            AcquisitionReleaseState::Completed,
            ReleaseJobState::Completed,
            "Debrid materializer completed selected files.",
            true,
        ),
    };
    let runtime_evidence = json!({
        "status": match transition {
            AnimeDebridRuntimeTransition::Materializing => "materializing",
            AnimeDebridRuntimeTransition::Completed => "completed",
        },
        "remoteStatus": match transition {
            AnimeDebridRuntimeTransition::Materializing => "materializing",
            AnimeDebridRuntimeTransition::Completed => "completed",
        },
        "providerImplementation": job.provider_implementation,
        "remoteReleaseId": remote_release_id,
        "sourceKind": job.source_kind,
        "progress": if terminal { Some(1.0) } else { job.progress },
        "downloadedBytes": job.downloaded_bytes,
        "totalBytes": job.total_bytes,
        "downloadRateBps": if terminal { Some(0_u64) } else { job.download_rate_bps },
        "localPath": local_path.or(job.local_path.as_deref()),
        "selectedFileCount": job.selected_file_ids.len(),
        "skippedFileCount": job.skipped_file_ids.len(),
        "updatedAt": chrono::Utc::now().to_rfc3339(),
    });
    let coverage_plan = merge_debrid_evidence_object(
        release.coverage_plan.clone(),
        "debridRuntime",
        runtime_evidence,
    );
    let release_update = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_releases
         SET state = $1, state_reason = $2, coverage_plan_json = $3,
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = $4 AND download_id = $5
           AND selected_route_logical_id = $6 AND selected_provider_id = $7
           AND remote_release_id = $8",
    )
    .bind(release_state.as_str())
    .bind(reason)
    .bind(serde_json::to_string(&coverage_plan)?)
    .bind(&release_id)
    .bind(&job_id)
    .bind(DEBRID_DEFAULT_LOGICAL_ID)
    .bind(&provider_id)
    .bind(remote_release_id)
    .execute(&mut *transaction)
    .await?;
    if release_update.rows_affected() != 1 {
        transaction.rollback().await?;
        return Ok(false);
    }
    let release_job_update = sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_release_jobs
         SET state = $1, state_reason = $2, active = $3,
             completed_at = CASE WHEN $3 = 0 THEN COALESCE(completed_at, CURRENT_TIMESTAMP) ELSE completed_at END,
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = $4 AND download_id = $5
           AND route_logical_id = $6 AND provider_id = $7
           AND remote_release_id = $8 AND active = 1",
    )
    .bind(release_job_state.as_str())
    .bind(reason)
    .bind(if terminal { 0_i64 } else { 1_i64 })
    .bind(&release_id)
    .bind(&job_id)
    .bind(DEBRID_DEFAULT_LOGICAL_ID)
    .bind(&provider_id)
    .bind(remote_release_id)
    .execute(&mut *transaction)
    .await?;
    if release_job_update.rows_affected() != 1 {
        transaction.rollback().await?;
        return Ok(false);
    }
    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_release_coverage
         SET state = 'submitted', reason = $1, verified_by = 'debrid_materializer',
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = $2 AND release_file_id IS NOT NULL AND state <> 'rejected'",
    )
    .bind(reason)
    .bind(&release_id)
    .execute(&mut *transaction)
    .await?;
    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_targets
         SET state = 'submitted', state_reason = $1, updated_at = CURRENT_TIMESTAMP
         WHERE download_id = $2 AND state NOT IN ('imported', 'excluded')",
    )
    .bind(reason)
    .bind(&job_id)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(true)
}

async fn sync_debrid_release_runtime_state(
    pool: &sqlx::AnyPool,
    job: &DebridDownloadJob,
    release_state: AcquisitionReleaseState,
    job_state: ReleaseJobState,
    reason: &str,
    coverage_state: Option<ReleaseCoverageState>,
    target_state: Option<AcquisitionTargetState>,
) -> Result<()> {
    let Some(release_id) = job.release_id else {
        return Ok(());
    };
    let Some(release) = get_release(pool, release_id).await? else {
        return Ok(());
    };
    let runtime_evidence = json!({
        "status": job.status,
        "remoteStatus": job.remote_release_status,
        "providerImplementation": job.provider_implementation,
        "remoteReleaseId": job.remote_release_id,
        "sourceKind": job.source_kind,
        "progress": job.progress,
        "downloadedBytes": job.downloaded_bytes,
        "totalBytes": job.total_bytes,
        "downloadRateBps": job.download_rate_bps,
        "localPath": job.local_path,
        "selectedFileCount": job.selected_file_ids.len(),
        "skippedFileCount": job.skipped_file_ids.len(),
        "updatedAt": chrono::Utc::now().to_rfc3339(),
    });
    update_release_state(
        pool,
        release_id,
        release_state,
        reason,
        Some(merge_debrid_evidence_object(
            release.coverage_plan.clone(),
            "debridRuntime",
            runtime_evidence,
        )),
    )
    .await?;
    update_debrid_release_job_selection_state(pool, release_id, job.job_id, job_state, reason)
        .await?;

    let coverage = list_release_coverage(pool, release_id).await?;
    let mut target_ids = BTreeSet::new();
    if let Some(coverage_state) = coverage_state {
        for entry in &coverage {
            target_ids.insert(entry.target_id);
            if !should_update_debrid_runtime_coverage(entry, coverage_state) {
                continue;
            }
            update_release_coverage_review_state(
                pool,
                entry.coverage_id,
                coverage_state,
                Some(reason.to_string()),
                Some("debrid_materializer".to_string()),
            )
            .await?;
        }
    } else {
        for entry in &coverage {
            target_ids.insert(entry.target_id);
        }
    }

    if let Some(download_id) = release
        .download_id
        .clone()
        .or_else(|| Some(job.job_id.to_string()))
    {
        for target_id in target_ids_for_download_id(pool, &download_id).await? {
            target_ids.insert(target_id);
        }
    }

    let Some(target_state) = target_state else {
        return Ok(());
    };
    for target_id in target_ids {
        update_target_state(
            pool,
            target_id,
            AcquisitionTargetStateUpdate {
                state: target_state,
                state_reason: Some(reason.to_string()),
                selected_provider_id: release
                    .selected_provider_id
                    .or(release.source_provider_id)
                    .or(Some(job.provider_id)),
                selected_route_logical_id: release.selected_route_logical_id.clone(),
                selected_candidate: release.selected_candidate.clone(),
                download_id: release
                    .download_id
                    .clone()
                    .or_else(|| Some(job.job_id.to_string())),
                next_search_after: None,
                increment_search_attempts: false,
                ..Default::default()
            },
        )
        .await?;
    }
    Ok(())
}

fn should_update_debrid_runtime_coverage(
    entry: &AcquisitionReleaseCoverage,
    next_state: ReleaseCoverageState,
) -> bool {
    if entry.state == ReleaseCoverageState::Rejected {
        return false;
    }
    if entry.release_file_id.is_none()
        && matches!(
            next_state,
            ReleaseCoverageState::Submitted | ReleaseCoverageState::Imported
        )
    {
        return false;
    }
    true
}

async fn target_ids_for_download_id(pool: &sqlx::AnyPool, download_id: &str) -> Result<Vec<Uuid>> {
    let rows = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT target_id
         FROM acquisition_targets
         WHERE download_id = $1",
    )
    .bind(download_id)
    .fetch_all(pool)
    .await
    .context("loading acquisition targets by download id")?;
    rows.into_iter()
        .map(|target_id| {
            Uuid::parse_str(&target_id)
                .with_context(|| format!("acquisition target id '{target_id}' is invalid"))
        })
        .collect()
}

fn map_debrid_job(row: &sqlx::any::AnyRow) -> Result<DebridDownloadJob> {
    let job_id_raw: String = row.try_get("job_id")?;
    let provider_id_raw: String = row.try_get("provider_id")?;
    let instance_id_raw: String = row.try_get("instance_id")?;
    let links_raw: String = row.try_get("links_json")?;
    let provider_capabilities_raw =
        empty_string_to_none(row.try_get::<String, _>("provider_capabilities_json")?);
    let provider_status_raw =
        empty_string_to_none(row.try_get::<String, _>("provider_status_json")?);
    let release_id = empty_string_to_none(row.try_get::<String, _>("release_id")?)
        .map(|value| Uuid::parse_str(&value).context("debrid release_id is invalid"))
        .transpose()?;
    Ok(DebridDownloadJob {
        job_id: Uuid::parse_str(&job_id_raw).context("debrid job_id is invalid")?,
        provider_id: Uuid::parse_str(&provider_id_raw).context("debrid provider_id is invalid")?,
        instance_id: Uuid::parse_str(&instance_id_raw).context("debrid instance_id is invalid")?,
        owner_id: row.try_get("owner_id")?,
        source: row.try_get("source")?,
        source_kind: row.try_get("source_kind")?,
        category: empty_string_to_none(row.try_get::<String, _>("category")?),
        display_name: empty_string_to_none(row.try_get::<String, _>("display_name")?),
        remote_torrent_id: empty_string_to_none(row.try_get::<String, _>("remote_torrent_id")?),
        remote_download_id: empty_string_to_none(row.try_get::<String, _>("remote_download_id")?),
        provider_implementation: empty_string_to_none(
            row.try_get::<String, _>("provider_implementation")?,
        ),
        remote_release_id: empty_string_to_none(row.try_get::<String, _>("remote_release_id")?),
        remote_release_status: empty_string_to_none(
            row.try_get::<String, _>("remote_release_status")?,
        ),
        provider_capabilities: provider_capabilities_raw
            .map(|value| serde_json::from_str(&value).context("parsing provider capabilities"))
            .transpose()?,
        provider_status: provider_status_raw
            .map(|value| serde_json::from_str(&value).context("parsing provider status"))
            .transpose()?,
        selection_mode: empty_string_to_none(row.try_get::<String, _>("selection_mode")?),
        selected_file_ids: parse_string_vec(row.try_get("selected_file_ids_json")?),
        skipped_file_ids: parse_string_vec(row.try_get("skipped_file_ids_json")?),
        selection_error: empty_string_to_none(row.try_get::<String, _>("selection_error")?),
        release_id,
        status: row.try_get("status")?,
        local_path: empty_string_to_none(row.try_get::<String, _>("local_path")?),
        links: serde_json::from_str(&links_raw).unwrap_or_default(),
        progress: row_get_f64_opt(row, "progress")?,
        downloaded_bytes: row_get_i64_opt(row, "downloaded_bytes")?.and_then(i64_to_u64),
        total_bytes: row_get_i64_opt(row, "total_bytes")?.and_then(i64_to_u64),
        download_rate_bps: row_get_i64_opt(row, "download_rate_bps")?.and_then(i64_to_u64),
        last_error: empty_string_to_none(row.try_get::<String, _>("last_error")?),
    })
}

fn real_debrid_capabilities() -> DebridProviderCapabilities {
    DebridProviderCapabilities {
        supports_magnet_submit: true,
        supports_hoster_unrestrict: true,
        supports_file_listing: true,
        supports_file_selection: true,
        supports_cache_check: false,
        supports_delete: true,
        supports_progress: true,
        file_selection_mode: DebridFileSelectionMode::BeforeTransfer,
    }
}

fn debrid_manifest_json() -> Value {
    json!({
        "id": DEBRID_EXTENSION_ID,
        "version": "0.1.0",
        "kind": "module",
        "name": "Debrid",
        "description": "Native debrid acquisition provider for direct HTTPS debrid downloads.",
        "publisher": { "name": "Elixir" },
        "trust": "verified",
        "permissions": ["network.egress"],
        "runtime": {
            "type": "internal"
        },
        "provides": [{
            "capability": "debrid.resolver",
            "slot": "default",
            "cardinality": "one",
            "implementation": REAL_DEBRID_IMPLEMENTATION,
            "scope": {
                "download_broker": {
                    "enabled": true,
                    "provider_kind": "debrid",
                    "logical_id": DEBRID_DEFAULT_LOGICAL_ID,
                    "capabilities": {
                        "magnetSubmit": true,
                        "hosterUnrestrict": true,
                        "fileListing": true,
                        "fileSelection": true,
                        "delete": true,
                        "progress": true,
                        "fileSelectionMode": "before_transfer"
                    }
                }
            },
            "endpoint": {
                "type": "http",
                "scheme": "https",
                "host": "api.real-debrid.com",
                "port": 443,
                "base_path": "/rest/1.0"
            },
            "healthcheck": {
                "type": "http",
                "path": "/user"
            }
        }],
        "requires": [],
        "control_surface": {
            "adapter": "generic_v1",
            "owned_settings": [{
                "id": "apiToken",
                "label": "API token",
                "description": "Debrid API token used by Elixir to resolve and materialize direct HTTPS debrid downloads.",
                "type": "password",
                "required": true,
                "secret": true,
                "ownership": "managed",
                "storage": {
                    "type": "instance_secret",
                    "key": DEBRID_REAL_DEBRID_TOKEN_SECRET_KEY
                }
            }],
            "native_only": [{
                "id": "streaming",
                "title": "Streaming",
                "description": "This pass implements local downloads only. Debrid streaming remains reserved for a future playback integration."
            }]
        }
    })
}

#[allow(dead_code)]
fn real_debrid_manifest_json() -> Value {
    debrid_manifest_json()
}

#[allow(dead_code)]
pub fn is_real_debrid_implementation(value: Option<&str>) -> bool {
    value
        .map(str::trim)
        .and_then(|value| DebridServiceKind::from_implementation_id(value).ok())
        .map(|service| service == DebridServiceKind::RealDebrid)
        .unwrap_or(false)
}

pub fn is_debrid_service_implementation(value: Option<&str>) -> bool {
    value
        .map(str::trim)
        .and_then(|value| DebridServiceKind::from_implementation_id(value).ok())
        .is_some()
}

pub fn debrid_source_kind(source: &str) -> Result<&'static str> {
    let lowered = source.trim().to_ascii_lowercase();
    if lowered.starts_with("magnet:") {
        Ok("magnet")
    } else if lowered.starts_with("http://") || lowered.starts_with("https://") {
        Ok("hoster")
    } else {
        bail!("debrid source must be a magnet, http, or https link")
    }
}

fn real_debrid_torrent_to_inspection(
    remote_release_id: &str,
    torrent: RealDebridTorrent,
) -> Result<DebridReleaseInspection> {
    let files = real_debrid_torrent_files(&torrent)?;
    let selected_file_ids = files
        .iter()
        .filter(|file| file.selected.unwrap_or(false))
        .map(|file| file.provider_file_id.clone())
        .collect::<Vec<_>>();
    let skipped_file_ids = files
        .iter()
        .filter(|file| !file.selected.unwrap_or(false))
        .map(|file| file.provider_file_id.clone())
        .collect::<Vec<_>>();
    let links = torrent
        .links
        .iter()
        .enumerate()
        .map(|(index, link)| DebridResolvedLink {
            provider_file_id: selected_file_ids.get(index).cloned(),
            url: link.clone(),
            filename: None,
            size_bytes: None,
            raw: Some(json!({ "index": index, "link": link })),
        })
        .collect::<Vec<_>>();
    let status = real_debrid_status_to_debrid_status(torrent.status.as_deref());
    let provider_status = real_debrid_torrent_provider_status(&torrent, status);
    let raw = json!({
        "torrent": torrent,
        "providerStatus": provider_status,
    });
    Ok(DebridReleaseInspection {
        release: DebridRemoteRelease {
            provider_implementation: REAL_DEBRID_IMPLEMENTATION.to_string(),
            remote_release_id: torrent
                .id
                .clone()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| remote_release_id.to_string()),
            display_name: torrent.filename.clone(),
            status,
            raw_status: torrent.status.clone(),
            raw: Some(raw.clone()),
        },
        capabilities: real_debrid_capabilities(),
        files,
        links,
        progress: Some(real_debrid_torrent_progress(&torrent)),
        selection: Some(DebridFileSelection {
            mode: DebridFileSelectionMode::BeforeTransfer,
            selected_file_ids,
            skipped_file_ids,
        }),
        raw: Some(raw),
    })
}

fn real_debrid_torrent_provider_status(
    torrent: &RealDebridTorrent,
    release_status: DebridReleaseStatus,
) -> Value {
    let raw_status = torrent
        .status
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let raw_status_lower = raw_status.unwrap_or_default().to_ascii_lowercase();
    let provider_failure_class = if release_status == DebridReleaseStatus::Failed {
        real_debrid_torrent_status_failure_class(raw_status_lower.as_str()).or_else(|| {
            classify_debrid_failure("failed", raw_status, None, None)
                .filter(|failure_class| *failure_class != DebridFailureClass::Unknown)
        })
    } else {
        None
    };
    let no_seeds = provider_failure_class == Some(DebridFailureClass::NoSeeds);
    let file_list_unavailable =
        torrent.files.is_empty() && release_status != DebridReleaseStatus::Downloaded;
    json!({
        "providerImplementation": DebridServiceKind::RealDebrid.implementation_id(),
        "providerName": DebridServiceKind::RealDebrid.display_name(),
        "status": release_status.as_str(),
        "providerState": raw_status,
        "rawStatus": raw_status,
        "providerFailureClass": provider_failure_class.map(DebridFailureClass::as_str),
        "retryable": provider_failure_class
            .map(|failure_class| failure_class.response_policy() == DebridFailureResponsePolicy::TryAlternateRouteOrCandidate)
            .unwrap_or(false),
        "cached": Option::<bool>::None,
        "notCached": false,
        "fileCount": torrent.files.len(),
        "fileListUnavailable": file_list_unavailable,
        "providerStalled": false,
        "noSeeds": no_seeds,
        "progress": torrent.progress.map(|value| (value / 100.0).clamp(0.0, 1.0)),
        "downloadedBytes": progress_downloaded_bytes(torrent.progress, torrent.bytes.or(torrent.original_bytes)),
        "totalBytes": torrent.bytes.or(torrent.original_bytes),
        "downloadRateBps": torrent.speed,
        "message": real_debrid_torrent_status_user_message(
            raw_status,
            provider_failure_class,
        ),
    })
}

fn real_debrid_torrent_status_failure_class(raw_status: &str) -> Option<DebridFailureClass> {
    match raw_status {
        "dead" => Some(DebridFailureClass::NoSeeds),
        "virus" => Some(DebridFailureClass::ContentBlocked),
        "magnet_error" | "error" => Some(DebridFailureClass::InvalidSource),
        _ => None,
    }
}

fn real_debrid_torrent_status_user_message(
    raw_status: Option<&str>,
    failure_class: Option<DebridFailureClass>,
) -> Option<String> {
    match failure_class {
        Some(DebridFailureClass::NoSeeds) => {
            Some("Real-Debrid reported this torrent as dead.".to_string())
        }
        Some(failure_class) => Some(format!(
            "Real-Debrid reported {}{}.",
            failure_class.as_str(),
            raw_status
                .map(|status| format!(" ({status})"))
                .unwrap_or_default()
        )),
        None => None,
    }
}

fn real_debrid_torrent_files(torrent: &RealDebridTorrent) -> Result<Vec<DebridRemoteFile>> {
    torrent
        .files
        .iter()
        .enumerate()
        .map(|(index, file)| {
            let provider_file_id = real_debrid_file_id(&file.id)
                .unwrap_or_else(|| (index.saturating_add(1)).to_string());
            let path = file
                .path
                .clone()
                .filter(|value| !value.trim().is_empty())
                .or_else(|| torrent.filename.clone())
                .unwrap_or_else(|| format!("debrid-file-{provider_file_id}"));
            Ok(DebridRemoteFile {
                provider_file_id,
                file_index: Some(index as i64),
                basename: basename_from_remote_path(&path),
                path,
                size_bytes: file.bytes,
                selectable: true,
                selected: real_debrid_selected_value(&file.selected),
                raw: Some(serde_json::to_value(file)?),
            })
        })
        .collect()
}

fn real_debrid_torrent_progress(torrent: &RealDebridTorrent) -> DebridReleaseProgress {
    let total = torrent.bytes.or(torrent.original_bytes);
    DebridReleaseProgress {
        status: real_debrid_status_to_debrid_status(torrent.status.as_deref()),
        progress: torrent
            .progress
            .map(|value| (value / 100.0).clamp(0.0, 1.0)),
        downloaded_bytes: progress_downloaded_bytes(torrent.progress, total),
        total_bytes: total,
        download_rate_bps: torrent.speed,
        raw: serde_json::to_value(torrent).ok(),
    }
}

fn real_debrid_file_id(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => non_empty(value).map(str::to_string),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn real_debrid_selected_value(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(value) => Some(*value),
        Value::Number(value) => value.as_i64().map(|value| value != 0),
        Value::String(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" => Some(true),
            "0" | "false" | "no" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn basename_from_remote_path(path: &str) -> String {
    Path::new(path)
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(path)
        .trim_matches('/')
        .to_string()
}

fn real_debrid_status_to_debrid_status(status: Option<&str>) -> DebridReleaseStatus {
    match status
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "downloaded" => DebridReleaseStatus::Downloaded,
        "downloading" | "compressing" | "uploading" => DebridReleaseStatus::Transferring,
        "waiting_files_selection" => DebridReleaseStatus::WaitingFiles,
        "queued" | "magnet_conversion" => DebridReleaseStatus::Staging,
        "magnet_error" | "error" | "virus" | "dead" => DebridReleaseStatus::Failed,
        "cancelled" => DebridReleaseStatus::Cancelled,
        _ => DebridReleaseStatus::Staging,
    }
}

fn debrid_status_to_job_status(status: DebridReleaseStatus) -> String {
    match status {
        DebridReleaseStatus::Downloaded => "debrid_downloaded",
        DebridReleaseStatus::Selected | DebridReleaseStatus::Transferring => "debrid_downloading",
        DebridReleaseStatus::WaitingFiles => "waiting_files_selection",
        DebridReleaseStatus::Staging => "submitted",
        DebridReleaseStatus::Materializing => "materializing",
        DebridReleaseStatus::Completed => "completed",
        DebridReleaseStatus::ReviewRequired => "review_required",
        DebridReleaseStatus::Failed => "failed",
        DebridReleaseStatus::Cancelled => "cancelled",
    }
    .to_string()
}

#[cfg(test)]
fn real_debrid_status_to_job_status(status: Option<&str>) -> String {
    debrid_status_to_job_status(real_debrid_status_to_debrid_status(status))
}

fn progress_downloaded_bytes(progress: Option<f64>, total: Option<u64>) -> Option<u64> {
    let total = total?;
    let progress = progress?;
    Some(((progress.clamp(0.0, 100.0) / 100.0) * total as f64) as u64)
}

fn progress_fraction(downloaded: Option<u64>, total: Option<u64>) -> Option<f64> {
    let total = total?;
    if total == 0 {
        return None;
    }
    Some((downloaded.unwrap_or(0) as f64 / total as f64).clamp(0.0, 1.0))
}

fn remaining_bytes(downloaded: Option<u64>, total: Option<u64>) -> Option<u64> {
    Some(total?.saturating_sub(downloaded.unwrap_or(0)))
}

fn normalized_owner_id(owner_id: &str) -> String {
    owner_id
        .trim()
        .is_empty()
        .then_some(DEFAULT_ROUTE_OWNER_ID)
        .unwrap_or_else(|| owner_id.trim())
        .to_string()
}

fn non_empty(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then_some(trimmed)
}

fn safe_path_segment(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string()
}

fn safe_file_name(value: &str) -> String {
    let mut output = String::new();
    for ch in value.trim().chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | ' ') {
            output.push(ch);
        } else {
            output.push('_');
        }
        if output.len() >= MAX_DOWNLOAD_FILE_NAME_LEN {
            break;
        }
    }
    let output = output.trim().trim_matches('.').to_string();
    if output.is_empty() {
        "debrid-download.bin".to_string()
    } else {
        output
    }
}

async fn unique_target_path(dir: &Path, filename: &str) -> PathBuf {
    let initial = dir.join(filename);
    if !initial.exists() {
        return initial;
    }
    let path = Path::new(filename);
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("download");
    let ext = path.extension().and_then(|value| value.to_str());
    for idx in 1..1000 {
        let candidate = match ext {
            Some(ext) if !ext.is_empty() => dir.join(format!("{stem}-{idx}.{ext}")),
            _ => dir.join(format!("{stem}-{idx}")),
        };
        if !candidate.exists() {
            return candidate;
        }
    }
    dir.join(format!("{stem}-{}", Uuid::new_v4()))
}

fn file_name_from_path(value: Option<&str>) -> Option<String> {
    value
        .and_then(|path| Path::new(path).file_name())
        .and_then(|value| value.to_str())
        .map(str::to_string)
}

fn filename_from_url_path(value: &str) -> Option<String> {
    let url = Url::parse(value).ok()?;
    let filename = url
        .path_segments()
        .and_then(|mut segments| segments.next_back())
        .and_then(non_empty)?;
    Some(filename.to_string())
}

fn path_segment(value: &str) -> String {
    urlencoding::encode(value).into_owned()
}

fn redacted_body(body: &str) -> String {
    let trimmed = body.trim();
    let mut chars = trimmed.chars();
    let short = chars.by_ref().take(400).collect::<String>();
    if chars.next().is_some() {
        format!("{short}...")
    } else {
        trimmed.to_string()
    }
}

fn empty_string_to_none(value: String) -> Option<String> {
    value
        .trim()
        .is_empty()
        .then_some(None)
        .unwrap_or_else(|| Some(value))
}

fn json_value_to_string(value: Option<&Value>) -> Result<Option<String>> {
    value
        .map(serde_json::to_string)
        .transpose()
        .context("serializing JSON value")
}

fn parse_string_vec(value: String) -> Vec<String> {
    serde_json::from_str::<Vec<String>>(&value).unwrap_or_default()
}

fn row_get_i64_opt(row: &sqlx::any::AnyRow, field: &str) -> Result<Option<i64>> {
    let raw = row.try_get_raw(field)?;
    if raw.type_info().name() == "NULL" {
        return Ok(None);
    }
    if let Ok(value) = row.try_get::<i64, _>(field) {
        return Ok(Some(value));
    }
    if let Ok(value) = row.try_get::<i32, _>(field) {
        return Ok(Some(value as i64));
    }
    let value = ValueRef::to_owned(&raw).try_decode::<String>()?;
    Ok(Some(value.parse::<i64>().with_context(|| {
        format!("invalid integer value for {field}: {value}")
    })?))
}

fn row_get_f64_opt(row: &sqlx::any::AnyRow, field: &str) -> Result<Option<f64>> {
    let raw = row.try_get_raw(field)?;
    if raw.type_info().name() == "NULL" {
        return Ok(None);
    }
    if let Ok(value) = row.try_get::<f64, _>(field) {
        return Ok(Some(value));
    }
    if let Ok(value) = row.try_get::<f32, _>(field) {
        return Ok(Some(value as f64));
    }
    let value = ValueRef::to_owned(&raw).try_decode::<String>()?;
    Ok(Some(value.parse::<f64>().with_context(|| {
        format!("invalid float value for {field}: {value}")
    })?))
}

fn u64_to_i64(value: u64) -> Option<i64> {
    i64::try_from(value).ok()
}

fn i64_to_u64(value: i64) -> Option<u64> {
    u64::try_from(value).ok()
}

#[cfg(test)]
mod live_validation;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acquisition::release_resolution::models::{
        ReleaseCoverageKind, ReleaseCoverageState,
    };
    use crate::{
        artwork::ArtworkService,
        auth::AuthService,
        config::{DatabaseConfig, Settings},
        db::Database,
        extensions::ExtensionManager,
        library::LinkerService,
        metadata::MetadataService,
    };
    use axum::{
        Json, Router,
        body::Bytes,
        extract::{Form, Path as AxumPath, Query, State},
        http::{HeaderMap, StatusCode as HttpStatusCode},
        response::IntoResponse,
        routing::{delete as axum_delete, get, post},
    };
    use chrono::Utc;
    use std::{
        collections::HashMap,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering as AtomicOrdering},
        },
    };
    use tokio::{net::TcpListener, sync::oneshot};

    #[test]
    fn debrid_service_kind_metadata_is_stable() -> Result<()> {
        let rows = [
            (
                DebridServiceKind::RealDebrid,
                "real_debrid",
                "Real-Debrid",
                DEBRID_REAL_DEBRID_TOKEN_SECRET_KEY,
                REAL_DEBRID_API_BASE,
                "https://app.real-debrid.com/",
            ),
            (
                DebridServiceKind::TorBox,
                "torbox",
                "TorBox",
                DEBRID_TORBOX_TOKEN_SECRET_KEY,
                TORBOX_API_BASE,
                "https://www.postman.com/torbox/torbox-api/documentation/b6l9hbv/main-api",
            ),
            (
                DebridServiceKind::AllDebrid,
                "all_debrid",
                "AllDebrid",
                DEBRID_ALL_DEBRID_TOKEN_SECRET_KEY,
                ALL_DEBRID_API_BASE,
                "https://docs.alldebrid.com/",
            ),
            (
                DebridServiceKind::Premiumize,
                "premiumize",
                "Premiumize",
                DEBRID_PREMIUMIZE_TOKEN_SECRET_KEY,
                PREMIUMIZE_API_BASE,
                "https://www.premiumize.me/api",
            ),
        ];
        assert_eq!(DebridServiceKind::ALL.len(), rows.len());
        for (service, implementation, display, secret_key, api_base, docs_url) in rows {
            assert_eq!(service.implementation_id(), implementation);
            assert_eq!(service.to_string(), implementation);
            assert_eq!(service.display_name(), display);
            assert_eq!(service.secret_key(), secret_key);
            assert_eq!(service.api_base_url(), api_base);
            assert_eq!(service.docs_url(), docs_url);
        }
        assert_eq!(
            DebridServiceKind::RealDebrid.secret_keys_for_read(),
            vec![
                DEBRID_REAL_DEBRID_TOKEN_SECRET_KEY,
                REAL_DEBRID_TOKEN_SECRET_KEY
            ]
        );
        Ok(())
    }

    #[test]
    fn debrid_service_kind_parses_implementation_aliases() -> Result<()> {
        assert_eq!(
            DebridServiceKind::from_str("real-debrid")?,
            DebridServiceKind::RealDebrid
        );
        assert_eq!(
            DebridServiceKind::from_str("REAL_DEBRID")?,
            DebridServiceKind::RealDebrid
        );
        assert_eq!(
            DebridServiceKind::from_str("alldebrid")?,
            DebridServiceKind::AllDebrid
        );
        assert_eq!(
            DebridServiceKind::from_str("all debrid")?,
            DebridServiceKind::AllDebrid
        );
        assert_eq!(
            DebridServiceKind::from_str("torbox")?,
            DebridServiceKind::TorBox
        );
        assert_eq!(
            DebridServiceKind::from_str("premiumize")?,
            DebridServiceKind::Premiumize
        );
        assert!(DebridServiceKind::from_str("unknown_debrid").is_err());
        Ok(())
    }

    #[test]
    fn active_debrid_service_config_parses_with_legacy_default() -> Result<()> {
        assert_eq!(
            active_debrid_service_from_config(None)?,
            DebridServiceKind::RealDebrid
        );
        assert_eq!(
            active_debrid_service_from_config(Some(&json!({ "materialize": true })))?,
            DebridServiceKind::RealDebrid
        );
        assert_eq!(
            active_debrid_service_from_config(Some(&json!({ "activeService": "torbox" })))?,
            DebridServiceKind::TorBox
        );
        assert_eq!(
            active_debrid_service_from_config(Some(&json!({ "active_service": "all-debrid" })))?,
            DebridServiceKind::AllDebrid
        );
        assert!(
            active_debrid_service_from_config(Some(&json!({
                "activeService": "unsupported"
            })))
            .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn debrid_adapter_factory_active_service_uses_configured_service() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        let secrets = SecretsManager::from_key_bytes([31u8; 32], false);
        let instance_id = setup_debrid_factory_instance(
            &database.pool,
            &store,
            json!({ "activeService": "torbox" }),
        )
        .await?;
        save_debrid_token(
            &secrets,
            &store,
            instance_id,
            DebridServiceKind::TorBox,
            "tb-token",
        )
        .await?;

        let factory = DebridAdapterFactory::new(&secrets);
        let adapter = factory
            .adapter_for_active_service(&store, instance_id)
            .await?;

        assert_eq!(adapter.implementation(), "torbox");
        assert_eq!(adapter.capabilities(), torbox_lifecycle_capabilities());
        Ok(())
    }

    #[tokio::test]
    async fn premiumize_adapter_validates_account_and_redacts_token_errors() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        let secrets = SecretsManager::from_key_bytes([41u8; 32], false);
        let (base_url, shutdown) = start_mock_premiumize_server().await?;
        let instance_id = setup_debrid_factory_instance(
            &database.pool,
            &store,
            json!({
                "activeService": "premiumize",
                "testPremiumizeApiBaseUrl": base_url
            }),
        )
        .await?;
        save_debrid_token(
            &secrets,
            &store,
            instance_id,
            DebridServiceKind::Premiumize,
            "good-token",
        )
        .await?;

        let adapter = DebridAdapterFactory::new(&secrets)
            .adapter_for_active_service(&store, instance_id)
            .await?;
        assert_eq!(adapter.implementation(), "premiumize");
        assert_eq!(adapter.capabilities(), premiumize_lifecycle_capabilities());
        let account = adapter.test_account().await?;
        assert_eq!(account.provider_implementation, "premiumize");
        assert_eq!(account.account_id.as_deref(), Some("pm-customer-123"));
        assert_eq!(account.username, None);
        assert_eq!(
            account
                .raw
                .as_ref()
                .and_then(|raw| raw.get("limit_used"))
                .and_then(Value::as_f64),
            Some(0.42)
        );

        save_debrid_token(
            &secrets,
            &store,
            instance_id,
            DebridServiceKind::Premiumize,
            "echo-token",
        )
        .await?;
        let adapter = DebridAdapterFactory::new(&secrets)
            .adapter_for_active_service(&store, instance_id)
            .await?;
        let err = adapter
            .test_account()
            .await
            .expect_err("echo token should fail auth");
        let message = err.to_string();
        assert!(message.contains("Premiumize API auth error"));
        assert!(message.contains("authentication_failed"));
        assert!(!message.contains("echo-token"));
        assert!(message.contains("[redacted]"));
        assert_eq!(
            classify_debrid_failure("failed", None, Some(&message), None),
            Some(DebridFailureClass::ProviderAuthMissing)
        );

        let _ = shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn premiumize_adapter_classifies_provider_limits() -> Result<()> {
        let (base_url, shutdown) = start_mock_premiumize_server().await?;
        let adapter = PremiumizeClient::with_base_url("rate-limit-token", base_url.clone())?;
        let err = adapter
            .test_account()
            .await
            .expect_err("rate-limited Premiumize account check should fail");
        let message = err.to_string();
        assert!(message.contains("Premiumize API provider unavailable"));
        assert!(message.contains("rate_limit_reached"));
        assert_eq!(
            classify_debrid_failure("failed", None, Some(&message), None),
            Some(DebridFailureClass::RateLimited)
        );

        let adapter = PremiumizeClient::with_base_url("account-limit-token", base_url)?;
        let err = adapter
            .test_account()
            .await
            .expect_err("account-limited Premiumize account check should fail");
        let message = err.to_string();
        assert!(message.contains("Premiumize API provider unavailable"));
        assert!(message.contains("account_limit_reached"));
        assert_eq!(
            classify_debrid_failure("failed", None, Some(&message), None),
            Some(DebridFailureClass::ProviderAccountLimitReached)
        );

        let _ = shutdown.send(());
        Ok(())
    }

    #[test]
    fn premiumize_response_envelope_parsing_is_stable() -> Result<()> {
        let value = premiumize_response_value(
            StatusCode::OK,
            r#"{"status":"success","customer_id":"pm-7","limit_used":0.1}"#,
            "secret-token",
        )?;
        assert_eq!(
            premiumize_value_key_string(&value, "customer_id").as_deref(),
            Some("pm-7")
        );

        let err = premiumize_response_value(
            StatusCode::OK,
            r#"{"status":"error","code":"authentication_failed","message":"Invalid secret-token"}"#,
            "secret-token",
        )
        .expect_err("Premiumize error envelope should fail");
        let message = err.to_string();
        assert!(message.contains("Premiumize API auth error"));
        assert!(message.contains("authentication_failed"));
        assert!(!message.contains("secret-token"));
        assert!(message.contains("[redacted]"));

        let err = premiumize_response_value(
            StatusCode::INTERNAL_SERVER_ERROR,
            r#"{"status":"error","code":"unknown_error","message":"Temporary outage"}"#,
            "secret-token",
        )
        .expect_err("Premiumize server error envelope should fail");
        assert!(err.to_string().contains("Premiumize API temporary error"));

        let err =
            premiumize_response_value(StatusCode::OK, r#"{"customer_id":"pm-7"}"#, "secret-token")
                .expect_err("Premiumize responses without status are provider failures");
        assert!(err.to_string().contains("Premiumize API error"));
        Ok(())
    }

    #[tokio::test]
    async fn premiumize_directdl_maps_magnet_lifecycle_to_generic_contract() -> Result<()> {
        let (base_url, state, shutdown) = start_mock_premiumize_directdl_server().await?;
        let adapter = PremiumizeClient::with_base_url("good-token", base_url)?;
        assert_eq!(adapter.implementation(), "premiumize");
        let capabilities = adapter.capabilities();
        assert!(capabilities.supports_magnet_submit);
        assert!(capabilities.supports_hoster_unrestrict);
        assert!(capabilities.supports_file_listing);
        assert!(capabilities.supports_file_selection);
        assert!(!capabilities.supports_cache_check);
        assert!(capabilities.supports_delete);
        assert!(capabilities.supports_progress);
        assert_eq!(
            capabilities.file_selection_mode,
            DebridFileSelectionMode::AfterTransfer
        );

        let magnet = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567";
        let release = adapter.submit_magnet(magnet).await?;
        assert!(release.remote_release_id.starts_with("pm-directdl-"));
        assert_eq!(release.status, DebridReleaseStatus::Downloaded);
        assert_eq!(release.raw_status.as_deref(), Some("directdl_ready"));
        assert_eq!(release.display_name.as_deref(), Some("Show"));
        assert_eq!(state.directdl_sources.lock().unwrap().as_slice(), [magnet]);

        let inspection = adapter.inspect_release(&release.remote_release_id).await?;
        assert_eq!(inspection.release.status, DebridReleaseStatus::Downloaded);
        assert_eq!(inspection.files.len(), 6);
        assert!(inspection.links.is_empty());
        assert_eq!(inspection.files[0].path, "Show/Season 01/Show.S01E01.mkv");
        assert_eq!(inspection.files[0].basename, "Show.S01E01.mkv");
        assert_eq!(inspection.files[0].size_bytes, Some(2048));
        assert!(inspection.files[0].selectable);
        assert!(inspection.files[1].selectable);
        assert!(
            !inspection.files[2].selectable,
            "sample should not be selectable"
        );
        assert!(
            !inspection.files[3].selectable,
            "extra should not be selectable"
        );
        assert!(
            !inspection.files[4].selectable,
            "archive should not be selectable"
        );
        assert!(
            !inspection.files[5].selectable,
            "text file should not be selectable"
        );
        assert_eq!(
            inspection
                .progress
                .as_ref()
                .and_then(|progress| progress.progress),
            Some(1.0)
        );
        assert_eq!(
            inspection
                .progress
                .as_ref()
                .and_then(|progress| progress.total_bytes),
            Some(7104)
        );

        let links = adapter.list_links(&release.remote_release_id).await?;
        assert_eq!(links.len(), 2);
        assert_eq!(links[0].filename.as_deref(), Some("Show.S01E01.mkv"));
        assert_eq!(links[1].filename.as_deref(), Some("Show.S01E02.mkv"));

        let selected_id = inspection.files[0].provider_file_id.clone();
        let selected = adapter
            .select_files(
                &release.remote_release_id,
                std::slice::from_ref(&selected_id),
            )
            .await?;
        assert_eq!(selected.links.len(), 1);
        assert_eq!(
            selected.links[0].provider_file_id.as_deref(),
            Some(selected_id.as_str())
        );
        assert_eq!(
            selected
                .selection
                .as_ref()
                .map(|selection| selection.selected_file_ids.as_slice()),
            Some(&[selected_id][..])
        );
        assert!(selected.links[0].url.ends_with("/download/Show.S01E01.mkv"));

        let progress = adapter.refresh_progress(&release.remote_release_id).await?;
        assert_eq!(progress.status, DebridReleaseStatus::Downloaded);
        assert_eq!(progress.progress, Some(1.0));
        assert!(!adapter.delete_release(&release.remote_release_id).await?);
        assert!(
            adapter
                .inspect_release(&release.remote_release_id)
                .await
                .is_err(),
            "directdl delete removes only the local synthetic snapshot"
        );

        let _ = shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn premiumize_directdl_unlocks_single_hoster_and_rejects_ambiguous_hoster() -> Result<()>
    {
        let (base_url, _state, shutdown) = start_mock_premiumize_directdl_server().await?;
        let adapter = PremiumizeClient::with_base_url("good-token", base_url)?;

        let unrestricted = adapter
            .unrestrict_hoster("https://hoster.test/single-hoster")
            .await?;
        assert_eq!(
            unrestricted.filename.as_deref(),
            Some("Movie.2024.1080p.mkv")
        );
        assert_eq!(unrestricted.size_bytes, Some(8192));
        assert!(unrestricted.url.ends_with("/download/Movie.2024.1080p.mkv"));

        let direct = adapter.unrestrict_hoster(&unrestricted.url).await?;
        assert_eq!(direct.url, unrestricted.url);
        assert_eq!(direct.filename.as_deref(), Some("Movie.2024.1080p.mkv"));

        let err = adapter
            .unrestrict_hoster("https://hoster.test/multi-file")
            .await
            .expect_err("multi-file directdl hoster unlock should require selection path");
        assert!(
            err.to_string().contains("expected exactly one file"),
            "{err}"
        );

        let _ = shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn premiumize_directdl_classifies_provider_and_source_errors() -> Result<()> {
        let (base_url, _state, shutdown) = start_mock_premiumize_directdl_server().await?;
        let adapter = PremiumizeClient::with_base_url("good-token", base_url)?;

        let rejected = adapter
            .submit_magnet("magnet:?xt=urn:btih:bad-magnet")
            .await
            .expect_err("unsupported Premiumize source should fail");
        assert!(rejected.to_string().contains("service_unsupported"));
        assert_eq!(
            classify_debrid_failure("failed", None, Some(&rejected.to_string()), None),
            Some(DebridFailureClass::InvalidSource)
        );

        let service_down = adapter
            .submit_magnet("magnet:?xt=urn:btih:service-down")
            .await
            .expect_err("Premiumize service-down directdl should fail");
        assert!(service_down.to_string().contains("service_down"));
        assert_eq!(
            classify_debrid_failure("failed", None, Some(&service_down.to_string()), None),
            Some(DebridFailureClass::ProviderUnavailable)
        );

        let account_limit = adapter
            .submit_magnet("magnet:?xt=urn:btih:account-limit")
            .await
            .expect_err("Premiumize account limit should fail");
        assert!(account_limit.to_string().contains("account_limit_reached"));
        assert_eq!(
            classify_debrid_failure("failed", None, Some(&account_limit.to_string()), None),
            Some(DebridFailureClass::ProviderAccountLimitReached)
        );

        let no_content = adapter
            .submit_magnet("magnet:?xt=urn:btih:no-content")
            .await
            .expect_err("empty Premiumize directdl content should fail");
        assert!(no_content.to_string().contains("empty_content"));

        let fallback = adapter
            .submit_magnet("magnet:?xt=urn:btih:no-link")
            .await
            .expect("Premiumize directdl link-generation failure should queue a transfer");
        assert_eq!(fallback.remote_release_id, "pm-transfer-folder");
        assert_eq!(fallback.status, DebridReleaseStatus::Staging);

        let _ = shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn premiumize_transfer_create_fallback_resolves_folder_files_and_delete() -> Result<()> {
        let (base_url, state, shutdown) = start_mock_premiumize_directdl_server().await?;
        let adapter = PremiumizeClient::with_base_url("good-token", base_url)?;
        let magnet = "magnet:?xt=urn:btih:queue-fallback";

        let release = adapter.submit_magnet(magnet).await?;
        assert_eq!(release.remote_release_id, "pm-transfer-folder");
        assert_eq!(release.status, DebridReleaseStatus::Staging);
        assert_eq!(release.raw_status.as_deref(), Some("transfer_created"));
        assert_eq!(state.directdl_sources.lock().unwrap().as_slice(), [magnet]);
        assert_eq!(state.created_transfers.lock().unwrap().as_slice(), [magnet]);

        let inspection = adapter.inspect_release(&release.remote_release_id).await?;
        assert_eq!(inspection.release.status, DebridReleaseStatus::Downloaded);
        assert_eq!(inspection.release.raw_status.as_deref(), Some("finished"));
        assert_eq!(inspection.files.len(), 4);
        assert_eq!(inspection.files[0].provider_file_id, "cloud-ep1");
        assert_eq!(inspection.files[0].path, "Show.S01E01.mkv");
        assert!(inspection.files[0].selectable);
        assert!(!inspection.files[1].selectable, "sample must be skipped");
        assert!(!inspection.files[2].selectable, "archive must be skipped");
        assert_eq!(inspection.files[3].provider_file_id, "cloud-ep2");
        assert_eq!(inspection.files[3].path, "Season 02/Show.S02E01.mkv");
        assert!(inspection.files[3].selectable);
        assert!(
            !inspection.files[0]
                .raw
                .as_ref()
                .unwrap()
                .as_object()
                .unwrap()
                .contains_key("directlink"),
            "deprecated cloud fields must not drive normalized raw evidence"
        );

        let links = adapter.list_links(&release.remote_release_id).await?;
        assert_eq!(links.len(), 2);
        assert_eq!(links[0].provider_file_id.as_deref(), Some("cloud-ep1"));
        assert!(links[0].url.ends_with("/download/Show.S01E01.mkv"));
        assert_eq!(links[1].provider_file_id.as_deref(), Some("cloud-ep2"));

        let selected = adapter
            .select_files(&release.remote_release_id, &["cloud-ep2".to_string()])
            .await?;
        assert_eq!(selected.links.len(), 1);
        assert_eq!(
            selected.links[0].provider_file_id.as_deref(),
            Some("cloud-ep2")
        );
        assert_eq!(
            selected
                .selection
                .as_ref()
                .map(|selection| selection.skipped_file_ids.as_slice()),
            Some(
                &[
                    "cloud-archive".to_string(),
                    "cloud-ep1".to_string(),
                    "cloud-sample".to_string()
                ][..]
            )
        );

        assert!(adapter.delete_release(&release.remote_release_id).await?);
        assert_eq!(
            state.deleted_transfers.lock().unwrap().as_slice(),
            ["pm-transfer-folder"]
        );
        assert!(!adapter.delete_release("pm-transfer-missing").await?);

        let container = adapter
            .submit_magnet("magnet:?xt=urn:btih:container-transfer")
            .await
            .expect_err("container fan-out is intentionally unsupported");
        assert!(container.to_string().contains("unsupported_container"));
        assert_eq!(
            classify_debrid_failure("failed", None, Some(&container.to_string()), None),
            Some(DebridFailureClass::ProviderUnsupported)
        );

        let _ = shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn premiumize_transfer_progress_and_single_file_resolution() -> Result<()> {
        let (base_url, _state, shutdown) = start_mock_premiumize_directdl_server().await?;
        let adapter = PremiumizeClient::with_base_url("good-token", base_url)?;

        let progress_release = adapter
            .submit_magnet("magnet:?xt=urn:btih:progress-transfer")
            .await?;
        assert_eq!(progress_release.remote_release_id, "pm-transfer-progress");

        let queued = adapter
            .refresh_progress(&progress_release.remote_release_id)
            .await?;
        assert_eq!(queued.status, DebridReleaseStatus::Staging);
        assert_eq!(queued.progress, Some(0.0));

        let running = adapter
            .refresh_progress(&progress_release.remote_release_id)
            .await?;
        assert_eq!(running.status, DebridReleaseStatus::Transferring);
        assert_eq!(running.progress, Some(0.42));

        let finished = adapter
            .inspect_release(&progress_release.remote_release_id)
            .await?;
        assert_eq!(finished.release.status, DebridReleaseStatus::Downloaded);
        assert_eq!(finished.files.len(), 1);
        assert_eq!(finished.files[0].provider_file_id, "file-progress");
        assert_eq!(finished.links.len(), 0);

        let single_release = adapter
            .submit_magnet("magnet:?xt=urn:btih:single-file-transfer")
            .await?;
        assert_eq!(single_release.remote_release_id, "pm-transfer-file");
        let single = adapter
            .inspect_release(&single_release.remote_release_id)
            .await?;
        assert_eq!(single.release.status, DebridReleaseStatus::Downloaded);
        assert_eq!(single.files.len(), 1);
        assert_eq!(single.files[0].provider_file_id, "file-movie");
        assert_eq!(single.files[0].basename, "Movie.2024.1080p.mkv");
        assert!(single.files[0].selectable);
        assert!(
            !single.files[0]
                .raw
                .as_ref()
                .unwrap()
                .as_object()
                .unwrap()
                .contains_key("stream_link"),
            "deprecated item/details fields must not drive normalized raw evidence"
        );
        let selected = adapter
            .select_files(&single_release.remote_release_id, &["all".to_string()])
            .await?;
        assert_eq!(selected.links.len(), 1);
        assert!(
            selected.links[0]
                .url
                .ends_with("/download/Movie.2024.1080p.mkv")
        );

        let _ = shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn premiumize_transfer_error_status_maps_to_provider_failure() -> Result<()> {
        let (base_url, _state, shutdown) = start_mock_premiumize_directdl_server().await?;
        let adapter = PremiumizeClient::with_base_url("good-token", base_url)?;

        let inspection = adapter.inspect_release("pm-transfer-error").await?;
        assert_eq!(inspection.release.status, DebridReleaseStatus::Failed);
        assert_eq!(
            inspection.release.raw_status.as_deref(),
            Some("error: service_down: target unavailable")
        );
        assert_eq!(
            classify_debrid_failure(
                "failed",
                inspection.release.raw_status.as_deref(),
                None,
                None
            ),
            Some(DebridFailureClass::ProviderUnavailable)
        );

        let _ = shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn premiumize_submission_materializes_selected_directdl_pack_after_active_service_switch()
    -> Result<()> {
        let state = setup_debrid_test_state().await?;
        let store = ExtensionStore::new(&state.db_pool);
        let (base_url, premiumize_state, shutdown) =
            start_mock_premiumize_directdl_server().await?;
        let instance_id = setup_debrid_factory_instance(
            &state.db_pool,
            &store,
            json!({
                "activeService": "premiumize",
                "materialize": true,
                "testPremiumizeApiBaseUrl": base_url.clone()
            }),
        )
        .await?;
        save_debrid_token(
            state.secrets.as_ref(),
            &store,
            instance_id,
            DebridServiceKind::Premiumize,
            "good-token",
        )
        .await?;
        let provider_id =
            reconcile_debrid_provider_for_instance(&state.db_pool, &store, instance_id).await?;
        let provider = store
            .list_providers(Some(instance_id))
            .await?
            .into_iter()
            .find(|provider| provider.provider_id == provider_id)
            .context("Premiumize default debrid provider should exist")?;
        assert_eq!(provider.implementation.as_deref(), Some("premiumize"));

        let subscription_id = create_series_subscription_with_targets(&state.db_pool).await?;
        let source = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567";
        let job_id = submit_debrid(
            &state,
            &store,
            provider_id,
            instance_id,
            None,
            source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("series"),
                name: Some("Show.S01.1080p.WEB-DL"),
                paused: false,
                release_context: Some(DebridReleaseSubmitContext {
                    subscription_id: Some(subscription_id),
                    source_provider_id: Some(provider_id),
                    source_extension_id: "test.source".to_string(),
                    media_type: MediaType::Series,
                    title: "Show".to_string(),
                    release_title: "Show.S01.1080p.WEB-DL".to_string(),
                    info_hash: Some("0123456789abcdef0123456789abcdef01234567".to_string()),
                    fingerprint: Some("premiumize-dp7d-directdl-pack".to_string()),
                    score: Some(99.0),
                    selected_candidate: Some(json!({
                        "title": "Show.S01.1080p.WEB-DL",
                        "source": source,
                        "sourceKind": "magnet",
                        "cachedDebrid": true,
                        "supportedRoutes": ["acquisition.debrid.default"],
                        "defaultRoute": "acquisition.debrid.default"
                    })),
                }),
            },
        )
        .await?;

        assert_eq!(
            premiumize_state.directdl_sources.lock().unwrap().as_slice(),
            [source]
        );
        let job = load_debrid_job(&state.db_pool, job_id)
            .await?
            .context("Premiumize job should load after submit")?;
        assert_eq!(job.provider_implementation.as_deref(), Some("premiumize"));
        assert!(
            job.remote_release_id
                .as_deref()
                .map(is_premiumize_directdl_release_id)
                .unwrap_or(false)
        );
        assert_eq!(
            job.status, "debrid_downloaded",
            "Premiumize directdl pack should auto-select; selection_error={:?}",
            job.selection_error
        );
        assert_eq!(job.selected_file_ids.len(), 2);
        assert_eq!(job.skipped_file_ids.len(), 4);
        assert_eq!(job.links.len(), 2);

        let progress = load_debrid_progress(&state, &store, provider_id, instance_id).await?;
        assert_eq!(progress.len(), 1);
        let evidence = progress[0]
            .debrid
            .as_ref()
            .context("Premiumize progress evidence should exist")?;
        assert_eq!(progress[0].state.as_deref(), Some("debrid_downloaded"));
        assert_eq!(evidence.provider_name.as_deref(), Some("Premiumize"));
        assert_eq!(
            evidence.provider_implementation.as_deref(),
            Some("premiumize")
        );
        assert_eq!(evidence.selected_file_count, 2);
        assert_eq!(evidence.skipped_file_count, 4);
        assert_eq!(evidence.fallback_state, "not_needed");

        store
            .update_instance_config(
                instance_id,
                Some(&normalized_debrid_instance_config(Some(json!({
                    "activeService": "real_debrid",
                    "materialize": true,
                    "testPremiumizeApiBaseUrl": base_url.clone()
                })))),
            )
            .await?;

        process_debrid_jobs_once(&state).await?;

        let job = load_debrid_job(&state.db_pool, job_id)
            .await?
            .context("materialized Premiumize job should load")?;
        assert_eq!(job.status, "completed");
        assert_eq!(job.progress, Some(1.0));
        assert_eq!(job.provider_implementation.as_deref(), Some("premiumize"));
        let local_path = PathBuf::from(
            job.local_path
                .as_deref()
                .context("Premiumize pack materialization should store a local path")?,
        );
        assert!(local_path.is_dir());
        let first = local_path.join("Show.S01E01.mkv");
        let second = local_path.join("Show.S01E02.mkv");
        assert_eq!(
            tokio::fs::read_to_string(&first).await?,
            "premiumize-Show.S01E01.mkv"
        );
        assert_eq!(
            tokio::fs::read_to_string(&second).await?,
            "premiumize-Show.S01E02.mkv"
        );

        let release = get_release_by_download_id(&state.db_pool, &job_id.to_string())
            .await?
            .context("Premiumize acquisition release should load")?;
        assert_eq!(release.state, AcquisitionReleaseState::Completed);
        assert_eq!(release.release_kind, ReleaseKind::SeasonPack);
        assert_eq!(release.resolver_kind, ReleaseResolverKind::TvSonarrStyle);
        assert_eq!(release.confidence, ReleaseConfidence::High);
        assert_eq!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.get("debridRuntime"))
                .and_then(|runtime| runtime.get("providerImplementation"))
                .and_then(Value::as_str),
            Some("premiumize")
        );
        let release_jobs = crate::acquisition::release_resolution::store::list_release_jobs(
            &state.db_pool,
            release.release_id,
        )
        .await?;
        assert_eq!(release_jobs.len(), 1);
        assert_eq!(release_jobs[0].state, ReleaseJobState::Completed);
        assert!(!release_jobs[0].active);
        let coverage = list_release_coverage(&state.db_pool, release.release_id).await?;
        assert_eq!(coverage.len(), 2);
        assert!(
            coverage
                .iter()
                .all(|entry| entry.state == ReleaseCoverageState::Submitted)
        );

        let _ = shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn premiumize_transfer_create_pending_later_materializes_after_finish() -> Result<()> {
        let state = setup_debrid_test_state().await?;
        let store = ExtensionStore::new(&state.db_pool);
        let (base_url, premiumize_state, shutdown) =
            start_mock_premiumize_directdl_server().await?;
        let instance_id = setup_debrid_factory_instance(
            &state.db_pool,
            &store,
            json!({
                "activeService": "premiumize",
                "materialize": true,
                "testPremiumizeApiBaseUrl": base_url.clone()
            }),
        )
        .await?;
        save_debrid_token(
            state.secrets.as_ref(),
            &store,
            instance_id,
            DebridServiceKind::Premiumize,
            "good-token",
        )
        .await?;
        let provider_id =
            reconcile_debrid_provider_for_instance(&state.db_pool, &store, instance_id).await?;
        let subscription_id =
            create_movie_subscription_with_target(&state.db_pool, "Progress Show").await?;
        let source = "magnet:?xt=urn:btih:progress-transfer";

        let job_id = submit_debrid(
            &state,
            &store,
            provider_id,
            instance_id,
            None,
            source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("movies"),
                name: Some("Progress.Show.S01E01.1080p.WEB-DL"),
                paused: false,
                release_context: Some(DebridReleaseSubmitContext {
                    subscription_id: Some(subscription_id),
                    source_provider_id: Some(provider_id),
                    source_extension_id: "test.source".to_string(),
                    media_type: MediaType::Movie,
                    title: "Progress Show".to_string(),
                    release_title: "Progress.Show.S01E01.1080p.WEB-DL".to_string(),
                    info_hash: Some("progress-transfer".to_string()),
                    fingerprint: Some("premiumize-dp7d-pending-transfer".to_string()),
                    score: Some(91.0),
                    selected_candidate: Some(json!({
                        "title": "Progress.Show.S01E01.1080p.WEB-DL",
                        "source": source,
                        "sourceKind": "magnet",
                        "cachedDebrid": false,
                        "supportedRoutes": ["acquisition.debrid.default"],
                        "defaultRoute": "acquisition.debrid.default"
                    })),
                }),
            },
        )
        .await?;

        let job = load_debrid_job(&state.db_pool, job_id)
            .await?
            .context("queued Premiumize transfer job should load")?;
        assert_eq!(job.provider_implementation.as_deref(), Some("premiumize"));
        assert_eq!(
            job.remote_release_id.as_deref(),
            Some("pm-transfer-progress")
        );
        assert_eq!(job.status, "submitted");
        assert!(job.links.is_empty());
        assert!(job.selected_file_ids.is_empty());
        assert_eq!(
            premiumize_state
                .created_transfers
                .lock()
                .unwrap()
                .as_slice(),
            [source]
        );

        process_debrid_jobs_once(&state).await?;
        let job = load_debrid_job(&state.db_pool, job_id)
            .await?
            .context("running Premiumize transfer job should load")?;
        assert_eq!(job.status, "debrid_downloading");
        assert_eq!(job.progress, Some(0.42));
        assert!(job.links.is_empty());

        process_debrid_jobs_once(&state).await?;
        let job = load_debrid_job(&state.db_pool, job_id)
            .await?
            .context("completed Premiumize transfer job should load")?;
        assert_eq!(job.status, "completed");
        assert_eq!(job.progress, Some(1.0));
        assert_eq!(job.selected_file_ids, vec!["file-progress".to_string()]);
        let local_path = job
            .local_path
            .as_deref()
            .context("materialized Premiumize pending transfer should have local path")?;
        let contents = tokio::fs::read_to_string(local_path).await?;
        assert_eq!(contents, "premiumize-Progress.Show.S01E01.mkv");
        assert!(
            *premiumize_state.transfer_list_calls.lock().unwrap() >= 3,
            "pending transfer should poll until Premiumize exposes a finished file"
        );

        let release = get_release_by_download_id(&state.db_pool, &job_id.to_string())
            .await?
            .context("Premiumize pending transfer release should load")?;
        assert_eq!(release.state, AcquisitionReleaseState::Completed);
        assert_eq!(release.release_kind, ReleaseKind::Single);
        assert_eq!(release.resolver_kind, ReleaseResolverKind::MovieRadarrStyle);
        assert_eq!(release.confidence, ReleaseConfidence::High);

        let _ = shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn premiumize_hoster_materializes_through_generic_factory() -> Result<()> {
        let state = setup_debrid_test_state().await?;
        let store = ExtensionStore::new(&state.db_pool);
        let (base_url, premiumize_state, shutdown) =
            start_mock_premiumize_directdl_server().await?;
        let instance_id = setup_debrid_factory_instance(
            &state.db_pool,
            &store,
            json!({
                "activeService": "premiumize",
                "materialize": true,
                "testPremiumizeApiBaseUrl": base_url.clone()
            }),
        )
        .await?;
        save_debrid_token(
            state.secrets.as_ref(),
            &store,
            instance_id,
            DebridServiceKind::Premiumize,
            "good-token",
        )
        .await?;
        let provider_id =
            reconcile_debrid_provider_for_instance(&state.db_pool, &store, instance_id).await?;
        let source = "https://hoster.test/single-hoster";

        let job_id = submit_debrid(
            &state,
            &store,
            provider_id,
            instance_id,
            None,
            source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("movies"),
                name: Some("Movie.2024.1080p.WEB-DL"),
                paused: false,
                release_context: None,
            },
        )
        .await?;
        let job = load_debrid_job(&state.db_pool, job_id)
            .await?
            .context("hoster Premiumize job should load")?;
        assert_eq!(job.source_kind, "hoster");
        assert_eq!(job.status, "debrid_downloaded");
        assert_eq!(job.links, vec![source.to_string()]);
        assert_eq!(
            premiumize_state.directdl_sources.lock().unwrap().as_slice(),
            [source]
        );

        process_debrid_jobs_once(&state).await?;

        let job = load_debrid_job(&state.db_pool, job_id)
            .await?
            .context("materialized hoster Premiumize job should load")?;
        assert_eq!(job.status, "completed");
        assert_eq!(job.progress, Some(1.0));
        let local_path = job
            .local_path
            .as_deref()
            .context("materialized Premiumize job should have local path")?;
        let contents = tokio::fs::read_to_string(local_path).await?;
        assert_eq!(contents, "premiumize-Movie.2024.1080p.mkv");
        assert_eq!(
            premiumize_state.directdl_sources.lock().unwrap().as_slice(),
            [source, source]
        );

        let _ = shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn premiumize_submit_failures_record_fallback_evidence() -> Result<()> {
        let state = setup_debrid_test_state().await?;
        let store = ExtensionStore::new(&state.db_pool);
        let (base_url, _premiumize_state, shutdown) =
            start_mock_premiumize_directdl_server().await?;
        let instance_id = setup_debrid_factory_instance(
            &state.db_pool,
            &store,
            json!({
                "activeService": "premiumize",
                "materialize": true,
                "testPremiumizeApiBaseUrl": base_url.clone()
            }),
        )
        .await?;
        save_debrid_token(
            state.secrets.as_ref(),
            &store,
            instance_id,
            DebridServiceKind::Premiumize,
            "good-token",
        )
        .await?;
        let provider_id =
            reconcile_debrid_provider_for_instance(&state.db_pool, &store, instance_id).await?;
        let subscription_id = create_series_subscription_with_targets(&state.db_pool).await?;

        for (source, fingerprint, expected_failure, expected_fallback_state) in [
            (
                "magnet:?xt=urn:btih:account-limit",
                "premiumize-dp7d-account-limit",
                "provider_account_limit_reached",
                "blocked_account_action_required",
            ),
            (
                "magnet:?xt=urn:btih:service-down",
                "premiumize-dp7d-service-down",
                "provider_unavailable",
                "retry_provider_later",
            ),
            (
                "magnet:?xt=urn:btih:bad-magnet",
                "premiumize-dp7d-bad-magnet",
                "invalid_source",
                "eligible_if_candidate_supports_torrent_route",
            ),
        ] {
            let err = submit_debrid(
                &state,
                &store,
                provider_id,
                instance_id,
                None,
                source,
                DebridSubmitOptions {
                    owner_id: "test.source",
                    category: Some("series"),
                    name: Some("Show.S01.1080p.WEB-DL"),
                    paused: false,
                    release_context: Some(DebridReleaseSubmitContext {
                        subscription_id: Some(subscription_id),
                        source_provider_id: Some(provider_id),
                        source_extension_id: "test.source".to_string(),
                        media_type: MediaType::Series,
                        title: "Show".to_string(),
                        release_title: "Show.S01.1080p.WEB-DL".to_string(),
                        info_hash: None,
                        fingerprint: Some(fingerprint.to_string()),
                        score: Some(88.0),
                        selected_candidate: Some(json!({
                            "title": "Show.S01.1080p.WEB-DL",
                            "source": source,
                            "sourceKind": "magnet",
                            "supportedRoutes": [
                                "acquisition.debrid.default",
                                "acquisition.torrent.default"
                            ],
                            "defaultRoute": "acquisition.debrid.default"
                        })),
                    }),
                },
            )
            .await
            .expect_err("Premiumize submit failure should be recorded");
            assert!(
                err.to_string().contains("Premiumize API")
                    || err.to_string().contains("magnet rejected by Premiumize"),
                "{err}"
            );

            let release =
                crate::acquisition::release_resolution::store::get_release_by_fingerprint(
                    &state.db_pool,
                    DEFAULT_ROUTE_OWNER_ID,
                    "test.source",
                    fingerprint,
                )
                .await?
                .with_context(|| {
                    format!("failed Premiumize release '{fingerprint}' should persist")
                })?;
            assert_eq!(release.state, AcquisitionReleaseState::Failed);
            assert_eq!(
                release
                    .coverage_plan
                    .as_ref()
                    .and_then(|plan| plan.get("debridFailure"))
                    .and_then(|failure| failure.get("failureClass"))
                    .and_then(Value::as_str),
                Some(expected_failure)
            );
            assert_eq!(
                release
                    .coverage_plan
                    .as_ref()
                    .and_then(|plan| plan.get("debridFailure"))
                    .and_then(|failure| failure.get("fallbackState"))
                    .and_then(Value::as_str),
                Some(expected_fallback_state)
            );
            assert_eq!(
                release
                    .coverage_plan
                    .as_ref()
                    .and_then(|plan| plan.get("debridProvider"))
                    .and_then(|provider| provider.get("providerImplementation"))
                    .and_then(Value::as_str),
                Some("premiumize")
            );
        }

        save_debrid_token(
            state.secrets.as_ref(),
            &store,
            instance_id,
            DebridServiceKind::Premiumize,
            "echo-token",
        )
        .await?;
        let auth_fingerprint = "premiumize-dp7d-invalid-token";
        let auth_source = "magnet:?xt=urn:btih:invalid-token-source";
        let err = submit_debrid(
            &state,
            &store,
            provider_id,
            instance_id,
            None,
            auth_source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("series"),
                name: Some("Show.S01.1080p.WEB-DL"),
                paused: false,
                release_context: Some(DebridReleaseSubmitContext {
                    subscription_id: Some(subscription_id),
                    source_provider_id: Some(provider_id),
                    source_extension_id: "test.source".to_string(),
                    media_type: MediaType::Series,
                    title: "Show".to_string(),
                    release_title: "Show.S01.1080p.WEB-DL".to_string(),
                    info_hash: None,
                    fingerprint: Some(auth_fingerprint.to_string()),
                    score: Some(88.0),
                    selected_candidate: Some(json!({
                        "title": "Show.S01.1080p.WEB-DL",
                        "source": auth_source,
                        "sourceKind": "magnet",
                        "supportedRoutes": ["acquisition.debrid.default"],
                        "defaultRoute": "acquisition.debrid.default"
                    })),
                }),
            },
        )
        .await
        .expect_err("invalid Premiumize token should fail provider submission");
        assert!(err.to_string().contains("authentication_failed"));
        assert!(!err.to_string().contains("echo-token"));
        let release = crate::acquisition::release_resolution::store::get_release_by_fingerprint(
            &state.db_pool,
            DEFAULT_ROUTE_OWNER_ID,
            "test.source",
            auth_fingerprint,
        )
        .await?
        .context("invalid token Premiumize release should persist")?;
        assert_eq!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.get("debridFailure"))
                .and_then(|failure| failure.get("failureClass"))
                .and_then(Value::as_str),
            Some("provider_auth_missing")
        );

        let _ = shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn torbox_adapter_validates_account_and_redacts_token_errors() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        let secrets = SecretsManager::from_key_bytes([35u8; 32], false);
        let (base_url, shutdown) = start_mock_torbox_server().await?;
        let instance_id = setup_debrid_factory_instance(
            &database.pool,
            &store,
            json!({
                "activeService": "torbox",
                "testTorBoxApiBaseUrl": base_url
            }),
        )
        .await?;
        save_debrid_token(
            &secrets,
            &store,
            instance_id,
            DebridServiceKind::TorBox,
            "good-token",
        )
        .await?;

        let adapter = DebridAdapterFactory::new(&secrets)
            .adapter_for_active_service(&store, instance_id)
            .await?;
        let account = adapter.test_account().await?;

        assert_eq!(account.provider_implementation, "torbox");
        assert_eq!(account.account_id.as_deref(), Some("12345"));
        assert_eq!(account.username.as_deref(), Some("torbox-user"));

        save_debrid_token(
            &secrets,
            &store,
            instance_id,
            DebridServiceKind::TorBox,
            "echo-token",
        )
        .await?;
        let adapter = DebridAdapterFactory::new(&secrets)
            .adapter_for_active_service(&store, instance_id)
            .await?;
        let err = adapter
            .test_account()
            .await
            .expect_err("echo token should fail auth");
        let message = err.to_string();
        assert!(message.contains("TorBox API auth error"));
        assert!(!message.contains("echo-token"));
        assert!(message.contains("[redacted]"));
        assert_eq!(
            classify_debrid_failure("failed", None, Some(&message), None),
            Some(DebridFailureClass::ProviderAuthMissing)
        );

        let _ = shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn torbox_adapter_classifies_provider_rate_limits() -> Result<()> {
        let (base_url, shutdown) = start_mock_torbox_server().await?;
        let adapter = TorBoxClient::with_base_url("rate-limit-token", base_url)?;

        let err = adapter
            .test_account()
            .await
            .expect_err("rate-limited TorBox account check should fail");
        let message = err.to_string();

        assert!(message.contains("TorBox API rate limit"));
        assert_eq!(
            classify_debrid_failure("failed", None, Some(&message), None),
            Some(DebridFailureClass::RateLimited)
        );

        let _ = shutdown.send(());
        Ok(())
    }

    #[test]
    fn torbox_create_torrent_rate_limiter_is_deterministic() -> Result<()> {
        let start = Instant::now();
        let mut limiter = TorBoxCreateTorrentRateLimiter::new(start);
        for _ in 0..TORBOX_CREATE_TORRENT_MINUTE_LIMIT {
            limiter.try_acquire(start)?;
        }
        let err = limiter
            .try_acquire(start)
            .expect_err("minute limit should reject the eleventh create request");
        let message = err.to_string();
        assert!(message.contains("TorBox API rate limit"));
        assert_eq!(
            classify_debrid_failure("failed", None, Some(&message), None),
            Some(DebridFailureClass::RateLimited)
        );
        limiter.try_acquire(start + TORBOX_CREATE_TORRENT_MINUTE_WINDOW)?;

        let start = Instant::now();
        let mut limiter = TorBoxCreateTorrentRateLimiter::new(start);
        for index in 0..TORBOX_CREATE_TORRENT_HOUR_LIMIT {
            let now = start + Duration::from_secs((index as u64 / 10) * 61);
            limiter.try_acquire(now)?;
        }
        let err = limiter
            .try_acquire(start + Duration::from_secs(10 * 61))
            .expect_err("hour limit should reject the sixty-first create request");
        assert!(err.to_string().contains("TorBox API rate limit"));
        limiter.try_acquire(start + TORBOX_CREATE_TORRENT_HOUR_WINDOW)?;

        let client = TorBoxClient::with_base_url(
            format!("limiter-token-{}", Uuid::new_v4()),
            "https://torbox.test/v1/api",
        )?;
        for _ in 0..TORBOX_CREATE_TORRENT_MINUTE_LIMIT {
            client.check_create_torrent_rate_limit()?;
        }
        let err = client
            .check_create_torrent_rate_limit()
            .expect_err("client guard should share the same deterministic limit");
        assert!(err.to_string().contains("TorBox API rate limit"));
        Ok(())
    }

    #[test]
    fn torbox_response_envelope_parsing_is_stable() -> Result<()> {
        let value = torbox_response_value(
            StatusCode::OK,
            r#"{"success":true,"detail":"ok","data":{"id":7,"username":"tb"}}"#,
            "secret-token",
        )?;
        assert_eq!(torbox_user_string(&value, "id").as_deref(), Some("7"));
        assert_eq!(
            torbox_user_string(&value, "username").as_deref(),
            Some("tb")
        );

        let err = torbox_response_value(
            StatusCode::OK,
            r#"{"success":false,"detail":"Invalid API token secret-token"}"#,
            "secret-token",
        )
        .expect_err("success false envelope should fail");
        let message = err.to_string();
        assert!(message.contains("TorBox API auth error"));
        assert!(!message.contains("secret-token"));
        assert!(message.contains("[redacted]"));
        Ok(())
    }

    #[tokio::test]
    async fn all_debrid_adapter_validates_account_and_redacts_token_errors() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        let secrets = SecretsManager::from_key_bytes([37u8; 32], false);
        let (base_url, shutdown) = start_mock_all_debrid_server().await?;
        let instance_id = setup_debrid_factory_instance(
            &database.pool,
            &store,
            json!({
                "activeService": "all_debrid",
                "testAllDebridApiBaseUrl": base_url
            }),
        )
        .await?;
        save_debrid_token(
            &secrets,
            &store,
            instance_id,
            DebridServiceKind::AllDebrid,
            "good-token",
        )
        .await?;

        let adapter = DebridAdapterFactory::new(&secrets)
            .adapter_for_active_service(&store, instance_id)
            .await?;
        assert_eq!(adapter.implementation(), "all_debrid");
        assert_eq!(adapter.capabilities(), all_debrid_lifecycle_capabilities());
        let account = adapter.test_account().await?;
        assert_eq!(account.provider_implementation, "all_debrid");
        assert_eq!(account.account_id.as_deref(), Some("67890"));
        assert_eq!(account.username.as_deref(), Some("alldebrid-user"));

        save_debrid_token(
            &secrets,
            &store,
            instance_id,
            DebridServiceKind::AllDebrid,
            "echo-token",
        )
        .await?;
        let adapter = DebridAdapterFactory::new(&secrets)
            .adapter_for_active_service(&store, instance_id)
            .await?;
        let err = adapter
            .test_account()
            .await
            .expect_err("echo token should fail auth");
        let message = err.to_string();
        assert!(message.contains("AllDebrid API auth error"));
        assert!(message.contains("AUTH_BAD_APIKEY"));
        assert!(!message.contains("echo-token"));
        assert!(message.contains("[redacted]"));
        assert_eq!(
            classify_debrid_failure("failed", None, Some(&message), None),
            Some(DebridFailureClass::ProviderAuthMissing)
        );

        let _ = shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn all_debrid_adapter_classifies_provider_rate_limits() -> Result<()> {
        let (base_url, shutdown) = start_mock_all_debrid_server().await?;
        let adapter = AllDebridClient::with_base_url("rate-limit-token", base_url)?;

        let err = adapter
            .test_account()
            .await
            .expect_err("rate-limited AllDebrid account check should fail");
        let message = err.to_string();

        assert!(message.contains("AllDebrid API rate limit"));
        assert_eq!(
            classify_debrid_failure("failed", None, Some(&message), None),
            Some(DebridFailureClass::RateLimited)
        );

        let _ = shutdown.send(());
        Ok(())
    }

    #[test]
    fn all_debrid_response_envelope_parsing_is_stable() -> Result<()> {
        let value = all_debrid_response_value(
            StatusCode::OK,
            r#"{"status":"success","data":{"user":{"id":7,"username":"ad"}}}"#,
            "secret-token",
        )?;
        let user = value
            .get("user")
            .context("user payload should be returned")?;
        assert_eq!(all_debrid_user_string(user, &["id"]).as_deref(), Some("7"));
        assert_eq!(
            all_debrid_user_string(user, &["username"]).as_deref(),
            Some("ad")
        );

        let err = all_debrid_response_value(
            StatusCode::OK,
            r#"{"status":"error","error":{"code":"AUTH_BAD_APIKEY","message":"Invalid secret-token"}}"#,
            "secret-token",
        )
        .expect_err("AllDebrid error envelope should fail");
        let message = err.to_string();
        assert!(message.contains("AllDebrid API auth error"));
        assert!(message.contains("AUTH_BAD_APIKEY"));
        assert!(!message.contains("secret-token"));
        assert!(message.contains("[redacted]"));

        let err = all_debrid_response_value(
            StatusCode::OK,
            r#"{"data":{"user":{"id":7}}}"#,
            "secret-token",
        )
        .expect_err("AllDebrid responses without status are provider failures");
        assert!(err.to_string().contains("AllDebrid API error"));
        Ok(())
    }

    #[tokio::test]
    async fn all_debrid_adapter_maps_magnet_lifecycle_to_generic_contract() -> Result<()> {
        let (base_url, state, shutdown) = start_mock_all_debrid_lifecycle_server().await?;
        let adapter = AllDebridClient::with_base_url("good-token", base_url)?;

        let capabilities = adapter.capabilities();
        assert!(capabilities.supports_magnet_submit);
        assert!(capabilities.supports_hoster_unrestrict);
        assert!(capabilities.supports_file_listing);
        assert!(capabilities.supports_file_selection);
        assert!(capabilities.supports_delete);
        assert!(capabilities.supports_progress);
        assert_eq!(
            capabilities.file_selection_mode,
            DebridFileSelectionMode::AfterTransfer
        );

        let magnet = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567";
        let release = adapter.submit_magnet(magnet).await?;
        assert_eq!(release.remote_release_id, "88");
        assert_eq!(release.status, DebridReleaseStatus::Downloaded);
        assert_eq!(release.raw_status.as_deref(), Some("submitted_ready"));
        assert_eq!(state.added_magnets.lock().unwrap().as_slice(), [magnet]);

        let rejected = adapter
            .submit_magnet("bad-magnet")
            .await
            .expect_err("invalid AllDebrid magnet should fail");
        assert!(rejected.to_string().contains("MAGNET_INVALID_URI"));
        assert_eq!(
            classify_debrid_failure("failed", None, Some(&rejected.to_string()), None),
            Some(DebridFailureClass::InvalidSource)
        );

        let inspection = adapter.inspect_release("88").await?;
        assert_eq!(inspection.release.status, DebridReleaseStatus::Downloaded);
        assert_eq!(inspection.files.len(), 4);
        assert_eq!(inspection.files[0].provider_file_id, "1");
        assert_eq!(inspection.files[0].path, "Show/Season 01/Show.S01E01.mkv");
        assert_eq!(inspection.files[0].basename, "Show.S01E01.mkv");
        assert_eq!(inspection.files[0].size_bytes, Some(2048));
        assert!(inspection.files[0].selectable);
        assert!(!inspection.files[2].selectable);
        assert!(!inspection.files[3].selectable);
        assert_eq!(
            inspection
                .progress
                .as_ref()
                .and_then(|progress| progress.progress),
            Some(1.0)
        );
        assert!(inspection.links.is_empty());

        let selected = adapter.select_files("88", &["1".to_string()]).await?;
        assert_eq!(
            selected
                .selection
                .as_ref()
                .map(|selection| selection.selected_file_ids.as_slice()),
            Some(&["1".to_string()][..])
        );
        assert_eq!(
            selected
                .selection
                .as_ref()
                .map(|selection| selection.skipped_file_ids.as_slice()),
            Some(&["2".to_string(), "3".to_string(), "4".to_string()][..])
        );
        assert_eq!(selected.links.len(), 1);
        assert_eq!(selected.links[0].provider_file_id.as_deref(), Some("1"));
        assert_eq!(
            selected.links[0].filename.as_deref(),
            Some("Show.S01E01.mkv")
        );
        assert!(selected.links[0].url.ends_with("/download/Show.S01E01.mkv"));
        assert_eq!(
            state.unlocked_links.lock().unwrap().as_slice(),
            ["https://alldebrid.com/f/episode-1"]
        );

        let links = adapter.list_links("88").await?;
        assert_eq!(links.len(), 2);
        assert_eq!(links[0].provider_file_id.as_deref(), Some("1"));
        assert_eq!(links[1].provider_file_id.as_deref(), Some("2"));
        assert_eq!(
            state.unlocked_links.lock().unwrap().as_slice(),
            [
                "https://alldebrid.com/f/episode-1",
                "https://alldebrid.com/f/episode-1",
                "https://alldebrid.com/f/episode-2"
            ]
        );

        let unrestricted = adapter
            .unrestrict_hoster("https://hoster.test/direct-file.mkv")
            .await?;
        assert!(unrestricted.url.ends_with("/download/direct-file.mkv"));
        assert_eq!(unrestricted.filename.as_deref(), Some("direct-file.mkv"));

        *state.ready.lock().unwrap() = false;
        let progress = adapter.refresh_progress("88").await?;
        assert_eq!(progress.status, DebridReleaseStatus::Transferring);
        assert_eq!(progress.progress, Some(1720.0 / 4096.0));
        assert_eq!(progress.downloaded_bytes, Some(1720));
        assert_eq!(progress.total_bytes, Some(4096));
        assert_eq!(progress.download_rate_bps, Some(2048));
        *state.ready.lock().unwrap() = true;

        assert!(adapter.delete_release("88").await?);
        assert_eq!(state.deleted_releases.lock().unwrap().as_slice(), ["88"]);
        assert!(!adapter.delete_release("404").await?);

        let _ = shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn torbox_adapter_maps_torrent_lifecycle_to_generic_contract() -> Result<()> {
        let (base_url, state, shutdown) = start_mock_torbox_lifecycle_server().await?;
        let adapter = TorBoxClient::with_base_url("good-token", base_url)?;

        let capabilities = adapter.capabilities();
        assert!(capabilities.supports_magnet_submit);
        assert!(capabilities.supports_file_listing);
        assert!(capabilities.supports_file_selection);
        assert!(capabilities.supports_cache_check);
        assert!(capabilities.supports_delete);
        assert!(capabilities.supports_progress);
        assert_eq!(
            capabilities.file_selection_mode,
            DebridFileSelectionMode::AfterTransfer
        );

        let magnet = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567";
        let release = adapter.submit_magnet(magnet).await?;
        assert_eq!(release.remote_release_id, "77");
        assert_eq!(release.status, DebridReleaseStatus::Staging);
        assert_eq!(release.raw_status.as_deref(), Some("submitted_cached"));
        assert_eq!(state.added_magnets.lock().unwrap().as_slice(), [magnet]);

        let inspection = adapter.inspect_release("77").await?;
        assert_eq!(inspection.release.status, DebridReleaseStatus::Downloaded);
        assert_eq!(inspection.files.len(), 2);
        assert_eq!(inspection.files[0].provider_file_id, "10");
        assert_eq!(inspection.files[0].path, "Show/Season 01/Show.S01E01.mkv");
        assert_eq!(inspection.files[0].basename, "Show.S01E01.mkv");
        assert_eq!(inspection.files[0].size_bytes, Some(2048));
        assert_eq!(inspection.files[0].selectable, true);
        assert_eq!(
            inspection
                .progress
                .as_ref()
                .and_then(|progress| progress.progress),
            Some(1.0)
        );
        assert!(inspection.links.is_empty());

        let selected = adapter.select_files("77", &["10".to_string()]).await?;
        assert_eq!(
            selected
                .selection
                .as_ref()
                .map(|selection| selection.selected_file_ids.as_slice()),
            Some(&["10".to_string()][..])
        );
        assert_eq!(
            selected
                .selection
                .as_ref()
                .map(|selection| selection.skipped_file_ids.as_slice()),
            Some(&["11".to_string()][..])
        );
        assert_eq!(selected.links.len(), 1);
        assert_eq!(selected.links[0].provider_file_id.as_deref(), Some("10"));
        assert!(
            selected.links[0]
                .url
                .starts_with("elixir-debrid://torbox/download?")
        );
        assert!(!selected.links[0].url.contains("good-token"));
        assert_eq!(
            selected.links[0].filename.as_deref(),
            Some("Show.S01E01.mkv")
        );
        assert_eq!(
            state.requested_downloads.lock().unwrap().as_slice(),
            ["77:10"]
        );

        let links = adapter.list_links("77").await?;
        assert_eq!(links.len(), 2);
        assert_eq!(links[0].provider_file_id.as_deref(), Some("10"));
        assert_eq!(links[1].provider_file_id.as_deref(), Some("11"));

        let internal_unrestricted = adapter.unrestrict_hoster(&selected.links[0].url).await?;
        assert!(
            internal_unrestricted
                .url
                .ends_with("/api/download/77/Show.S01E01.mkv")
        );
        assert_eq!(
            internal_unrestricted.filename.as_deref(),
            Some("Show.S01E01.mkv")
        );

        let unrestricted = adapter
            .unrestrict_hoster("https://download.torbox.test/77/Show.S01E01.mkv")
            .await?;
        assert_eq!(
            unrestricted.url,
            "https://download.torbox.test/77/Show.S01E01.mkv"
        );
        assert_eq!(unrestricted.filename.as_deref(), Some("Show.S01E01.mkv"));

        *state.ready.lock().unwrap() = false;
        let progress = adapter.refresh_progress("77").await?;
        assert_eq!(progress.status, DebridReleaseStatus::Transferring);
        assert_eq!(progress.progress, Some(0.42));
        assert_eq!(progress.downloaded_bytes, Some(1720));
        assert_eq!(progress.total_bytes, Some(4096));
        *state.ready.lock().unwrap() = true;

        assert!(adapter.delete_release("77").await?);
        assert_eq!(state.deleted_releases.lock().unwrap().as_slice(), ["77"]);
        assert!(!adapter.delete_release("404").await?);

        let _ = shutdown.send(());
        Ok(())
    }

    #[test]
    fn torbox_stalled_torrent_with_null_files_is_provider_failure_not_parse_failure() -> Result<()>
    {
        let torrent: TorBoxTorrent = serde_json::from_value(json!({
            "id": 30589564,
            "hash": "29638f38523bf01f688fd524ffd2c21b17ea3792",
            "name": "Show.S03E02.2160p.WEB-DL.mkv",
            "size": 9_948_770_661_u64,
            "download_state": "stalled (no seeds)",
            "progress": 0.999157,
            "download_speed": 0,
            "total_downloaded": 9_940_390_158_u64,
            "download_finished": false,
            "download_present": false,
            "cached": false,
            "files": null
        }))?;

        let inspection = torbox_torrent_to_inspection(&torrent, Vec::new(), None)?;

        assert_eq!(inspection.release.status, DebridReleaseStatus::Failed);
        assert_eq!(
            inspection.release.raw_status.as_deref(),
            Some("stalled (no seeds)")
        );
        assert!(inspection.files.is_empty());
        assert_eq!(
            inspection
                .progress
                .as_ref()
                .and_then(|progress| progress.progress),
            Some(0.999157)
        );
        let provider_status = debrid_provider_status_from_inspection(&inspection);
        assert_eq!(
            provider_status.get("providerState").and_then(Value::as_str),
            Some("stalled (no seeds)")
        );
        assert_eq!(
            provider_status
                .get("providerFailureClass")
                .and_then(Value::as_str),
            Some("no_seeds")
        );
        assert_eq!(
            provider_status.get("cached").and_then(Value::as_bool),
            Some(false)
        );
        assert_eq!(
            provider_status
                .get("downloadPresent")
                .and_then(Value::as_bool),
            Some(false)
        );
        assert_eq!(
            provider_status.get("noSeeds").and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            provider_status.get("message").and_then(Value::as_str),
            Some("TorBox accepted this torrent, but it is not cached and has no seeds.")
        );
        assert_eq!(
            classify_debrid_failure(
                "failed",
                Some("failed"),
                debrid_failure_message_from_inspection(&inspection).as_deref(),
                None,
            ),
            Some(DebridFailureClass::NoSeeds)
        );
        Ok(())
    }

    #[tokio::test]
    async fn torbox_stalled_no_seed_inspection_persists_retryable_failure_evidence() -> Result<()> {
        let database = setup_db().await?;
        let (provider_id, instance_id) = create_provider_refs(&database.pool).await?;
        let subscription_id = create_series_subscription_with_targets(&database.pool).await?;
        // This test owns the provider-failure transition. Do not let the
        // submit fixture first enter the terminal selection-review state for
        // an unrelated synthetic file list; terminal jobs intentionally
        // reject later inspection snapshots.
        let adapter = FakeDebridAdapter::with_files(Vec::new());
        let source = "magnet:?xt=urn:btih:29638f38523bf01f688fd524ffd2c21b17ea3792";
        let job_id = submit_debrid_with_adapter(
            &database.pool,
            provider_id,
            instance_id,
            source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("series"),
                name: Some("Show.S03E02.2160p.WEB-DL"),
                paused: false,
                release_context: Some(DebridReleaseSubmitContext {
                    subscription_id: Some(subscription_id),
                    source_provider_id: Some(provider_id),
                    source_extension_id: "test.source".to_string(),
                    media_type: MediaType::Series,
                    title: "Show".to_string(),
                    release_title: "Show.S03E02.2160p.WEB-DL".to_string(),
                    info_hash: Some("29638f38523bf01f688fd524ffd2c21b17ea3792".to_string()),
                    fingerprint: Some("torbox-stalled-no-seeds-evidence".to_string()),
                    score: Some(91.0),
                    selected_candidate: Some(json!({
                        "title": "Show.S03E02.2160p.WEB-DL",
                        "source": source,
                        "sourceKind": "magnet",
                        "supportedRoutes": [
                            "acquisition.debrid.default",
                            "acquisition.torrent.default"
                        ],
                        "defaultRoute": "acquisition.debrid.default"
                    })),
                }),
            },
            &adapter,
        )
        .await?;
        sqlx::query::<sqlx::Any>(
            "UPDATE debrid_download_jobs SET remote_release_id = NULL WHERE job_id = $1",
        )
        .bind(job_id.to_string())
        .execute(&database.pool)
        .await?;

        let torrent: TorBoxTorrent = serde_json::from_value(json!({
            "id": 30589564,
            "hash": "29638f38523bf01f688fd524ffd2c21b17ea3792",
            "name": "Show.S03E02.2160p.WEB-DL.mkv",
            "size": 9_948_770_661_u64,
            "download_state": "stalled (no seeds)",
            "progress": 0.999157,
            "download_speed": 0,
            "total_downloaded": 9_940_390_158_u64,
            "download_finished": false,
            "download_present": false,
            "cached": false,
            "files": null
        }))?;
        let inspection = torbox_torrent_to_inspection(&torrent, Vec::new(), None)?;

        update_debrid_job_from_inspection(&database.pool, job_id, &inspection).await?;

        let job = load_debrid_job(&database.pool, job_id)
            .await?
            .context("TorBox stalled job should load")?;
        assert_eq!(job.status, "failed");
        assert_eq!(job.provider_implementation.as_deref(), Some("torbox"));
        assert_eq!(job.remote_release_id.as_deref(), Some("30589564"));
        assert_eq!(
            job.last_error.as_deref(),
            Some("TorBox accepted this torrent, but it is not cached and has no seeds.")
        );
        assert_eq!(
            classify_debrid_job_failure(&job),
            Some(DebridFailureClass::NoSeeds)
        );
        assert_eq!(job.progress, Some(0.999157));
        assert!(job.downloaded_bytes.is_some());
        assert!(job.total_bytes.is_some());
        let provider_status = job
            .provider_status
            .as_ref()
            .context("provider status evidence should persist")?;
        assert_eq!(
            provider_status.get("providerState").and_then(Value::as_str),
            Some("stalled (no seeds)")
        );
        assert_eq!(
            provider_status
                .get("providerFailureClass")
                .and_then(Value::as_str),
            Some("no_seeds")
        );
        assert_eq!(
            provider_status.get("notCached").and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            provider_status
                .get("downloadPresent")
                .and_then(Value::as_bool),
            Some(false)
        );
        assert_eq!(
            provider_status
                .get("downloadedBytes")
                .and_then(Value::as_u64),
            Some(9_940_390_158)
        );
        assert_eq!(
            provider_status.get("totalBytes").and_then(Value::as_u64),
            Some(9_948_770_661)
        );

        let status = get_debrid_job_status(&database.pool, job_id)
            .await?
            .context("debrid job status should load")?;
        assert!(status.is_failed());
        assert_eq!(status.failure_class.as_deref(), Some("no_seeds"));
        assert_eq!(
            status.last_error.as_deref(),
            Some("TorBox accepted this torrent, but it is not cached and has no seeds.")
        );

        let evidence = debrid_progress_evidence_for_job(&job);
        assert_eq!(evidence.provider_name.as_deref(), Some("TorBox"));
        assert_eq!(
            evidence
                .provider_status
                .as_ref()
                .and_then(|status| status.get("noSeeds"))
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(evidence.failure_class.as_deref(), Some("no_seeds"));
        assert_eq!(
            evidence.fallback_state,
            "eligible_if_candidate_supports_torrent_route"
        );

        let release = get_release(
            &database.pool,
            job.release_id
                .context("job should be linked to a release")?,
        )
        .await?
        .context("failed TorBox release should load")?;
        assert_eq!(release.state, AcquisitionReleaseState::Failed);
        let failure = release
            .coverage_plan
            .as_ref()
            .and_then(|plan| plan.get("debridFailure"))
            .context("debrid failure evidence should persist")?;
        assert_eq!(
            failure.get("failureClass").and_then(Value::as_str),
            Some("no_seeds")
        );
        assert_eq!(
            failure.get("message").and_then(Value::as_str),
            Some("TorBox accepted this torrent, but it is not cached and has no seeds.")
        );
        assert_eq!(
            failure.get("fallbackState").and_then(Value::as_str),
            Some("eligible_if_candidate_supports_torrent_route")
        );
        let retry_suppression = release
            .coverage_plan
            .as_ref()
            .and_then(|plan| plan.get("retrySuppression"))
            .context("retry suppression evidence should persist")?;
        assert_eq!(
            retry_suppression.get("status").and_then(Value::as_str),
            Some("rejected")
        );
        assert_eq!(
            retry_suppression
                .get("suppressAutomaticRediscovery")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            retry_suppression.get("reason").and_then(Value::as_str),
            Some("no_seeds")
        );
        Ok(())
    }

    #[tokio::test]
    async fn debrid_adapter_factory_persisted_job_implementation_overrides_active_service()
    -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        let secrets = SecretsManager::from_key_bytes([32u8; 32], false);
        let instance_id = setup_debrid_factory_instance(
            &database.pool,
            &store,
            json!({ "activeService": "premiumize" }),
        )
        .await?;
        save_debrid_token(
            &secrets,
            &store,
            instance_id,
            DebridServiceKind::RealDebrid,
            "rd-token",
        )
        .await?;

        let factory = DebridAdapterFactory::new(&secrets);
        let adapter = factory
            .adapter_for_job_implementation(&store, instance_id, Some("real_debrid"))
            .await?;

        assert_eq!(adapter.implementation(), REAL_DEBRID_IMPLEMENTATION);
        Ok(())
    }

    #[tokio::test]
    async fn debrid_adapter_factory_missing_token_fails_closed() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        let secrets = SecretsManager::from_key_bytes([33u8; 32], false);
        let instance_id =
            setup_debrid_factory_instance(&database.pool, &store, default_debrid_instance_config())
                .await?;

        let factory = DebridAdapterFactory::new(&secrets);
        let err = match factory
            .adapter_for_active_service(&store, instance_id)
            .await
        {
            Ok(_) => bail!("missing active debrid token should fail closed"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("API token is not configured"));
        assert_eq!(
            classify_debrid_failure("failed", None, Some(&err.to_string()), None),
            Some(DebridFailureClass::ProviderAuthMissing)
        );
        Ok(())
    }

    #[test]
    fn debrid_provider_error_kind_maps_to_failure_classes() {
        assert_eq!(
            DebridProviderErrorKind::Unauthorized.failure_class(),
            DebridFailureClass::ProviderAuthMissing
        );
        assert_eq!(
            DebridProviderErrorKind::RateLimited.failure_class(),
            DebridFailureClass::RateLimited
        );
        assert_eq!(
            DebridProviderErrorKind::SelectionUnsupported.failure_class(),
            DebridFailureClass::ProviderUnsupported
        );
        assert_eq!(
            DebridProviderErrorKind::Permanent.failure_class(),
            DebridFailureClass::InvalidSource
        );
        let error = DebridProviderError {
            kind: DebridProviderErrorKind::Temporary,
            provider_code: Some("429".to_string()),
            message: "rate limit".to_string(),
        };
        assert_eq!(
            error.failure_class(),
            DebridFailureClass::ProviderUnavailable
        );
    }

    #[test]
    fn legacy_real_debrid_compatibility_helpers_still_work() {
        assert_eq!(REAL_DEBRID_EXTENSION_ID, LEGACY_REAL_DEBRID_EXTENSION_ID);
        assert!(is_debrid_extension_id(DEBRID_EXTENSION_ID));
        assert!(is_debrid_extension_id(LEGACY_REAL_DEBRID_EXTENSION_ID));
        assert!(is_real_debrid_implementation(Some("real_debrid")));
        assert!(is_real_debrid_implementation(Some("Real-Debrid")));
        assert!(is_debrid_service_implementation(Some("premiumize")));
        assert!(!is_real_debrid_implementation(Some("premiumize")));
        assert!(!is_debrid_service_implementation(Some("not_debrid")));
    }

    #[tokio::test]
    async fn debrid_builtin_fresh_install_creates_canonical_extension() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);

        ensure_debrid_builtin_records(&database.pool, &store).await?;

        let extension = store
            .get_extension(DEBRID_EXTENSION_ID)
            .await?
            .context("canonical Debrid extension should exist")?;
        assert_eq!(extension.name, "Debrid");
        assert_eq!(extension.manifest_json["id"], DEBRID_EXTENSION_ID);
        assert_eq!(extension.manifest_json["name"], "Debrid");
        let manifest: crate::extensions::manifest::ExtensionManifest =
            serde_json::from_value(extension.manifest_json.clone())?;
        manifest.validate()?;
        assert_eq!(
            manifest
                .runtime
                .as_ref()
                .map(|runtime| runtime.r#type.as_str()),
            Some("internal")
        );
        assert!(
            store
                .get_extension(LEGACY_REAL_DEBRID_EXTENSION_ID)
                .await?
                .is_none()
        );

        let instances = store.list_instances(Some(DEBRID_EXTENSION_ID)).await?;
        assert_eq!(instances.len(), 1);
        let instance = &instances[0];
        assert_eq!(instance.instance_name, "default");
        let config = instance
            .config_json
            .as_ref()
            .context("default debrid instance config should exist")?;
        assert_eq!(
            config.get("activeService").and_then(Value::as_str),
            Some(REAL_DEBRID_IMPLEMENTATION)
        );
        assert_eq!(
            config.get("materialize").and_then(Value::as_bool),
            Some(true)
        );

        let providers = store.list_providers(Some(instance.instance_id)).await?;
        assert_eq!(providers.len(), 1);
        let provider = &providers[0];
        assert_eq!(provider.capability, "debrid.resolver");
        assert_eq!(provider.slot_id, "default");
        assert_eq!(
            provider.implementation.as_deref(),
            Some(REAL_DEBRID_IMPLEMENTATION)
        );
        assert_eq!(provider.health_state, ProviderHealthState::Unknown);
        assert_eq!(
            provider
                .scope_json
                .as_ref()
                .and_then(|scope| scope.pointer("/download_broker/logical_id"))
                .and_then(Value::as_str),
            Some(DEBRID_DEFAULT_LOGICAL_ID)
        );

        Ok(())
    }

    #[tokio::test]
    async fn debrid_builtin_migrates_legacy_real_debrid_extension_records() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        let instance_id = Uuid::new_v4();
        let legacy_provider_id = Uuid::new_v4();

        store
            .upsert_extension(&NewExtension {
                extension_id: LEGACY_REAL_DEBRID_EXTENSION_ID.to_string(),
                name: "Real-Debrid".to_string(),
                version: "0.1.0".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: Some("Elixir".to_string()),
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Verified,
                manifest_json: json!({ "id": LEGACY_REAL_DEBRID_EXTENSION_ID }),
                package_hash: None,
                enabled: false,
            })
            .await
            .context("seeding legacy Real-Debrid extension")?;
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: LEGACY_REAL_DEBRID_EXTENSION_ID.to_string(),
                instance_name: "default".to_string(),
                config_json: Some(json!({ "materialize": true })),
                enabled: true,
            })
            .await
            .context("seeding legacy Real-Debrid instance")?;
        store
            .upsert_provider(&NewProvider {
                provider_id: legacy_provider_id,
                instance_id,
                capability: "debrid.resolver".to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: Some(REAL_DEBRID_IMPLEMENTATION.to_string()),
                scope_json: Some(json!({
                    "download_broker": {
                        "enabled": true,
                        "provider_kind": "debrid",
                        "logical_id": DEBRID_DEFAULT_LOGICAL_ID
                    }
                })),
                endpoint_json: None,
                health_state: ProviderHealthState::Healthy,
            })
            .await
            .context("seeding legacy Real-Debrid provider")?;

        ensure_debrid_builtin_records(&database.pool, &store).await?;

        assert!(
            store
                .get_extension(LEGACY_REAL_DEBRID_EXTENSION_ID)
                .await?
                .is_none()
        );
        let extension = store
            .get_extension(DEBRID_EXTENSION_ID)
            .await?
            .context("canonical Debrid extension should exist")?;
        assert!(!extension.enabled);

        let migrated = store
            .get_instance(instance_id)
            .await?
            .context("legacy instance should be migrated")?;
        assert_eq!(migrated.extension_id, DEBRID_EXTENSION_ID);
        assert_eq!(
            migrated
                .config_json
                .as_ref()
                .and_then(|config| config.get("activeService"))
                .and_then(Value::as_str),
            Some(REAL_DEBRID_IMPLEMENTATION)
        );

        let providers = store.list_providers(Some(instance_id)).await?;
        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].provider_id, legacy_provider_id);
        assert_eq!(
            providers[0].implementation.as_deref(),
            Some(REAL_DEBRID_IMPLEMENTATION)
        );
        Ok(())
    }

    #[tokio::test]
    async fn debrid_builtin_copies_legacy_real_debrid_secret_to_canonical_key() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        let secrets = SecretsManager::from_key_bytes([7u8; 32], false);
        let instance_id = Uuid::new_v4();

        store
            .upsert_extension(&NewExtension {
                extension_id: LEGACY_REAL_DEBRID_EXTENSION_ID.to_string(),
                name: "Real-Debrid".to_string(),
                version: "0.1.0".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: Some("Elixir".to_string()),
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Verified,
                manifest_json: json!({ "id": LEGACY_REAL_DEBRID_EXTENSION_ID }),
                package_hash: None,
                enabled: true,
            })
            .await?;
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: LEGACY_REAL_DEBRID_EXTENSION_ID.to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;
        store
            .upsert_secret(&NewSecret {
                secret_id: Uuid::new_v4(),
                scope: SecretScope::Instance,
                scope_id: Some(instance_id),
                key: REAL_DEBRID_TOKEN_SECRET_KEY.to_string(),
                value_encrypted: secrets.encrypt("legacy-token")?,
                rotatable: true,
            })
            .await?;

        ensure_debrid_builtin_records(&database.pool, &store).await?;

        assert!(
            store
                .get_secret(
                    SecretScope::Instance,
                    Some(instance_id),
                    REAL_DEBRID_TOKEN_SECRET_KEY,
                )
                .await?
                .is_some()
        );
        assert!(
            store
                .get_secret(
                    SecretScope::Instance,
                    Some(instance_id),
                    DEBRID_REAL_DEBRID_TOKEN_SECRET_KEY,
                )
                .await?
                .is_some()
        );
        assert!(
            debrid_secret_exists_for_instance(&store, instance_id, DebridServiceKind::RealDebrid)
                .await?
        );
        assert_eq!(
            debrid_token_for_instance_with_secrets(
                &secrets,
                &store,
                instance_id,
                DebridServiceKind::RealDebrid,
            )
            .await?,
            "legacy-token"
        );
        Ok(())
    }

    #[tokio::test]
    async fn debrid_builtin_legacy_migration_is_idempotent_and_preserves_bindings() -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        let secrets = SecretsManager::from_key_bytes([7u8; 32], false);
        let instance_id = Uuid::new_v4();
        let legacy_provider_id = Uuid::new_v4();
        let route_binding_id = Uuid::new_v4();
        let endpoint = ProviderEndpoint::new(
            "https".to_string(),
            "api.real-debrid.com".to_string(),
            443,
            Some("/rest/1.0".to_string()),
            None,
        )?;

        store
            .upsert_extension(&NewExtension {
                extension_id: LEGACY_REAL_DEBRID_EXTENSION_ID.to_string(),
                name: "Real-Debrid".to_string(),
                version: "0.1.0".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: Some("Elixir".to_string()),
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Verified,
                manifest_json: json!({ "id": LEGACY_REAL_DEBRID_EXTENSION_ID }),
                package_hash: None,
                enabled: true,
            })
            .await?;
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: LEGACY_REAL_DEBRID_EXTENSION_ID.to_string(),
                instance_name: "default".to_string(),
                config_json: Some(json!({ "materialize": true })),
                enabled: true,
            })
            .await?;
        store
            .upsert_provider(&NewProvider {
                provider_id: legacy_provider_id,
                instance_id,
                capability: "debrid.resolver".to_string(),
                slot_id: "default".to_string(),
                cardinality: SlotCardinality::One,
                implementation: Some(REAL_DEBRID_IMPLEMENTATION.to_string()),
                scope_json: Some(json!({
                    "download_broker": {
                        "enabled": true,
                        "provider_kind": "debrid",
                        "logical_id": DEBRID_DEFAULT_LOGICAL_ID
                    }
                })),
                endpoint_json: Some(serde_json::to_value(endpoint)?),
                health_state: ProviderHealthState::Healthy,
            })
            .await?;
        store
            .upsert_secret(&NewSecret {
                secret_id: Uuid::new_v4(),
                scope: SecretScope::Instance,
                scope_id: Some(instance_id),
                key: REAL_DEBRID_TOKEN_SECRET_KEY.to_string(),
                value_encrypted: secrets.encrypt("legacy-token")?,
                rotatable: true,
            })
            .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO download_provider_bindings
             (id, logical_role, owner_id, binding_kind, provider_id, profile_id, category,
              download_path, allow_shared_path, status)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(route_binding_id.to_string())
        .bind(DEBRID_DEFAULT_LOGICAL_ID)
        .bind(DEFAULT_ROUTE_OWNER_ID)
        .bind("debrid")
        .bind(legacy_provider_id.to_string())
        .bind(Option::<String>::None)
        .bind("debrid")
        .bind("/downloads/debrid")
        .bind(0_i64)
        .bind("configured")
        .execute(&database.pool)
        .await?;

        ensure_debrid_builtin_records(&database.pool, &store).await?;
        ensure_debrid_builtin_records(&database.pool, &store).await?;

        assert!(
            store
                .get_extension(LEGACY_REAL_DEBRID_EXTENSION_ID)
                .await?
                .is_none()
        );
        let canonical = store
            .get_extension(DEBRID_EXTENSION_ID)
            .await?
            .context("canonical Debrid extension should exist")?;
        assert!(canonical.enabled);

        let instances = store.list_instances(Some(DEBRID_EXTENSION_ID)).await?;
        assert_eq!(instances.len(), 1);
        let migrated = &instances[0];
        assert_eq!(migrated.instance_id, instance_id);
        assert_eq!(migrated.extension_id, DEBRID_EXTENSION_ID);
        assert_eq!(
            migrated
                .config_json
                .as_ref()
                .and_then(|config| config.get("activeService"))
                .and_then(Value::as_str),
            Some(REAL_DEBRID_IMPLEMENTATION)
        );

        let providers = store.list_providers(Some(instance_id)).await?;
        assert_eq!(providers.len(), 1);
        let provider = &providers[0];
        assert_eq!(provider.provider_id, legacy_provider_id);
        assert_eq!(
            provider.implementation.as_deref(),
            Some(REAL_DEBRID_IMPLEMENTATION)
        );
        assert_eq!(provider.health_state, ProviderHealthState::Healthy);
        assert_eq!(
            provider
                .scope_json
                .as_ref()
                .and_then(|scope| scope.pointer("/download_broker/logical_id"))
                .and_then(Value::as_str),
            Some(DEBRID_DEFAULT_LOGICAL_ID)
        );

        let routes = crate::download_broker::list_acquisition_routes(&database.pool, &store)
            .await?
            .routes;
        let debrid_route = routes
            .iter()
            .find(|route| {
                route.logical_id == DEBRID_DEFAULT_LOGICAL_ID
                    && route.owner_id == DEFAULT_ROUTE_OWNER_ID
            })
            .context("default Debrid route should exist")?;
        assert_eq!(debrid_route.provider_id, Some(legacy_provider_id));
        assert_eq!(debrid_route.selected_provider_id, Some(legacy_provider_id));
        assert!(debrid_route.blocker.is_none());

        for key in [
            REAL_DEBRID_TOKEN_SECRET_KEY,
            DEBRID_REAL_DEBRID_TOKEN_SECRET_KEY,
        ] {
            let secret = store
                .get_secret(SecretScope::Instance, Some(instance_id), key)
                .await?
                .with_context(|| format!("{key} should be preserved after migration"))?;
            assert_eq!(secrets.decrypt(&secret.value_encrypted)?, "legacy-token");
        }
        assert_eq!(
            debrid_token_for_instance_with_secrets(
                &secrets,
                &store,
                instance_id,
                DebridServiceKind::RealDebrid,
            )
            .await?,
            "legacy-token"
        );
        Ok(())
    }

    #[tokio::test]
    async fn debrid_builtin_disables_duplicate_default_debrid_providers_without_deleting_them()
    -> Result<()> {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        let active_instance_id = Uuid::new_v4();
        let inactive_instance_id = Uuid::new_v4();
        let active_provider_id =
            stable_provider_id(active_instance_id, "debrid.resolver", "default");
        let inactive_provider_id = Uuid::new_v4();

        store
            .upsert_extension(&NewExtension {
                extension_id: DEBRID_EXTENSION_ID.to_string(),
                name: "Debrid".to_string(),
                version: "0.1.0".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: Some("Elixir".to_string()),
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Verified,
                manifest_json: debrid_manifest_json(),
                package_hash: None,
                enabled: true,
            })
            .await?;
        for (instance_id, instance_name) in [
            (active_instance_id, "default"),
            (inactive_instance_id, "secondary"),
        ] {
            store
                .create_instance(&NewExtensionInstance {
                    instance_id,
                    extension_id: DEBRID_EXTENSION_ID.to_string(),
                    instance_name: instance_name.to_string(),
                    config_json: Some(default_debrid_instance_config()),
                    enabled: true,
                })
                .await?;
        }
        for (provider_id, instance_id) in [
            (active_provider_id, active_instance_id),
            (inactive_provider_id, inactive_instance_id),
        ] {
            store
                .upsert_provider(&NewProvider {
                    provider_id,
                    instance_id,
                    capability: "debrid.resolver".to_string(),
                    slot_id: "default".to_string(),
                    cardinality: SlotCardinality::One,
                    implementation: Some(REAL_DEBRID_IMPLEMENTATION.to_string()),
                    scope_json: Some(json!({
                        "download_broker": {
                            "enabled": true,
                            "provider_kind": "debrid",
                            "logical_id": DEBRID_DEFAULT_LOGICAL_ID
                        }
                    })),
                    endpoint_json: None,
                    health_state: ProviderHealthState::Healthy,
                })
                .await?;
        }

        ensure_debrid_builtin_records(&database.pool, &store).await?;

        let active_provider = store
            .get_provider(active_provider_id)
            .await?
            .context("active provider should remain")?;
        assert_eq!(active_provider.instance_id, active_instance_id);
        assert_eq!(
            active_provider
                .scope_json
                .as_ref()
                .and_then(|scope| scope.pointer("/download_broker/enabled"))
                .and_then(Value::as_bool),
            Some(true)
        );

        let inactive_provider = store
            .get_provider(inactive_provider_id)
            .await?
            .context("inactive provider should be preserved")?;
        assert_eq!(inactive_provider.instance_id, inactive_instance_id);
        assert_eq!(inactive_provider.health_state, ProviderHealthState::Unknown);
        assert_eq!(
            inactive_provider
                .scope_json
                .as_ref()
                .and_then(|scope| scope.pointer("/download_broker/enabled"))
                .and_then(Value::as_bool),
            Some(false)
        );
        Ok(())
    }

    #[derive(Clone)]
    struct FakeDebridAdapter {
        state: Arc<Mutex<FakeDebridState>>,
        fail_submit: bool,
        force_failed_submit_status: bool,
        fail_inspect: bool,
        force_failed_inspection: bool,
        fail_select: bool,
        fail_unrestrict: bool,
        fail_delete: bool,
        template_files: Vec<DebridRemoteFile>,
    }

    #[derive(Default)]
    struct FakeDebridState {
        next_id: u64,
        releases: HashMap<String, FakeDebridRelease>,
        deleted_release_ids: Vec<String>,
    }

    #[derive(Clone)]
    struct FakeDebridRelease {
        release: DebridRemoteRelease,
        files: Vec<DebridRemoteFile>,
        selected_file_ids: Vec<String>,
    }

    #[derive(Clone, Copy)]
    enum FakeAnimeMatchBehavior {
        MatchFirst,
        MatchSubbed,
        EngineError,
        EngineTimeout,
        Empty,
        InvalidOutput,
        UnknownFile,
    }

    #[derive(Clone)]
    struct FakeAnimeMatchEngine {
        behavior: FakeAnimeMatchBehavior,
        calls: Arc<AtomicUsize>,
        observed_paths: Arc<Mutex<Vec<String>>>,
    }

    impl FakeAnimeMatchEngine {
        fn new(behavior: FakeAnimeMatchBehavior) -> Self {
            Self {
                behavior,
                calls: Arc::new(AtomicUsize::new(0)),
                observed_paths: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn service(&self) -> AnimeMatchingService {
            AnimeMatchingService::with_engine(Arc::new(self.clone()))
        }
    }

    #[async_trait::async_trait]
    impl crate::anime_matching::AnimeMatchEngine for FakeAnimeMatchEngine {
        async fn match_candidates(
            &self,
            request: crate::anime_matching::AnimeMatchRequest,
        ) -> Result<crate::anime_matching::AnimeMatchResponse> {
            self.calls.fetch_add(1, AtomicOrdering::SeqCst);
            *self.observed_paths.lock().unwrap() = request
                .candidates
                .iter()
                .flat_map(|candidate| candidate.files.iter().map(|file| file.path.clone()))
                .collect();
            if matches!(self.behavior, FakeAnimeMatchBehavior::EngineError) {
                bail!("local anime worker unavailable");
            }
            if matches!(self.behavior, FakeAnimeMatchBehavior::EngineTimeout) {
                bail!("local anime worker request timed out");
            }
            let candidate = request
                .candidates
                .first()
                .context("fake matcher requires one candidate")?;
            let target_key = request
                .target
                .wanted_target_keys
                .first()
                .context("fake matcher requires one target")?
                .clone();
            if matches!(self.behavior, FakeAnimeMatchBehavior::Empty) {
                return Ok(crate::anime_matching::AnimeMatchResponse {
                    schema_version: ANIME_MATCH_SCHEMA_VERSION,
                    matches: Vec::new(),
                });
            }
            let selected_file_key = if matches!(self.behavior, FakeAnimeMatchBehavior::UnknownFile)
            {
                "candidate-0-file-unknown".to_string()
            } else {
                candidate
                    .files
                    .first()
                    .context("fake matcher requires one real file")?
                    .file_key
                    .clone()
            };
            Ok(crate::anime_matching::AnimeMatchResponse {
                schema_version: if matches!(self.behavior, FakeAnimeMatchBehavior::InvalidOutput) {
                    ANIME_MATCH_SCHEMA_VERSION + 1
                } else {
                    ANIME_MATCH_SCHEMA_VERSION
                },
                matches: vec![crate::anime_matching::AnimeCandidateMatch {
                    candidate_key: candidate.candidate_key.clone(),
                    matched_target_keys: vec![target_key],
                    audio_profile: if matches!(self.behavior, FakeAnimeMatchBehavior::MatchSubbed) {
                        AnimeMatchAudioProfile::Subbed
                    } else {
                        AnimeMatchAudioProfile::DualAudio
                    },
                    selected_file_keys: Some(vec![selected_file_key]),
                }],
            })
        }
    }

    fn fake_debrid_series_files() -> Vec<DebridRemoteFile> {
        vec![
            DebridRemoteFile {
                provider_file_id: "file-1".to_string(),
                file_index: Some(0),
                path: "Show/Season 01/Show.S01E01.mkv".to_string(),
                basename: "Show.S01E01.mkv".to_string(),
                size_bytes: Some(1024),
                selectable: true,
                selected: Some(false),
                raw: None,
            },
            DebridRemoteFile {
                provider_file_id: "file-2".to_string(),
                file_index: Some(1),
                path: "Show/Season 01/Show.S01E02.mkv".to_string(),
                basename: "Show.S01E02.mkv".to_string(),
                size_bytes: Some(1024),
                selectable: true,
                selected: Some(false),
                raw: None,
            },
        ]
    }

    fn fake_ready_anime_file() -> DebridRemoteFile {
        DebridRemoteFile {
            provider_file_id: "ready-anime-file-1".to_string(),
            file_index: Some(1),
            path: "Ready Replay Anime/Ready.Replay.Anime.S01E01.1080p.WEB-DL.mkv".to_string(),
            basename: "Ready.Replay.Anime.S01E01.1080p.WEB-DL.mkv".to_string(),
            size_bytes: Some(2_048),
            selectable: true,
            selected: Some(false),
            raw: None,
        }
    }

    impl FakeDebridAdapter {
        fn new() -> Self {
            Self {
                state: Arc::new(Mutex::new(FakeDebridState::default())),
                fail_submit: false,
                force_failed_submit_status: false,
                fail_inspect: false,
                force_failed_inspection: false,
                fail_select: false,
                fail_unrestrict: false,
                fail_delete: false,
                template_files: fake_debrid_series_files(),
            }
        }

        fn failing_inspect() -> Self {
            Self {
                state: Arc::new(Mutex::new(FakeDebridState::default())),
                fail_submit: false,
                force_failed_submit_status: false,
                fail_inspect: true,
                force_failed_inspection: false,
                fail_select: false,
                fail_unrestrict: false,
                fail_delete: false,
                template_files: fake_debrid_series_files(),
            }
        }

        fn failing_select() -> Self {
            Self {
                state: Arc::new(Mutex::new(FakeDebridState::default())),
                fail_submit: false,
                force_failed_submit_status: false,
                fail_inspect: false,
                force_failed_inspection: false,
                fail_select: true,
                fail_unrestrict: false,
                fail_delete: false,
                template_files: fake_debrid_series_files(),
            }
        }

        fn with_files(files: Vec<DebridRemoteFile>) -> Self {
            Self {
                state: Arc::new(Mutex::new(FakeDebridState::default())),
                fail_submit: false,
                force_failed_submit_status: false,
                fail_inspect: false,
                force_failed_inspection: false,
                fail_select: false,
                fail_unrestrict: false,
                fail_delete: false,
                template_files: files,
            }
        }

        fn failing_submit() -> Self {
            Self {
                fail_submit: true,
                ..Self::new()
            }
        }

        fn failed_submit_status() -> Self {
            Self {
                force_failed_submit_status: true,
                ..Self::new()
            }
        }

        fn failed_inspection() -> Self {
            Self {
                force_failed_inspection: true,
                ..Self::new()
            }
        }

        fn failing_delete() -> Self {
            Self {
                fail_delete: true,
                ..Self::failed_inspection()
            }
        }

        fn failing_unrestrict() -> Self {
            Self {
                fail_unrestrict: true,
                ..Self::new()
            }
        }

        fn inspection(
            &self,
            release: &FakeDebridRelease,
            status: DebridReleaseStatus,
        ) -> DebridReleaseInspection {
            let selected = release.selected_file_ids.clone();
            let skipped = release
                .files
                .iter()
                .filter_map(|file| {
                    (!selected
                        .iter()
                        .any(|selected| selected == &file.provider_file_id))
                    .then(|| file.provider_file_id.clone())
                })
                .collect::<Vec<_>>();
            let mut remote = release.release.clone();
            remote.status = status;
            DebridReleaseInspection {
                release: remote,
                capabilities: self.capabilities(),
                files: release
                    .files
                    .iter()
                    .map(|file| DebridRemoteFile {
                        selected: Some(
                            selected
                                .iter()
                                .any(|selected| selected == &file.provider_file_id),
                        ),
                        ..file.clone()
                    })
                    .collect(),
                links: selected
                    .iter()
                    .map(|file_id| DebridResolvedLink {
                        provider_file_id: Some(file_id.clone()),
                        url: format!("https://fake-debrid.test/download/{file_id}"),
                        filename: Some(format!("{file_id}.mkv")),
                        size_bytes: Some(1024),
                        raw: None,
                    })
                    .collect(),
                progress: Some(DebridReleaseProgress {
                    status,
                    progress: if selected.is_empty() {
                        Some(0.0)
                    } else {
                        Some(1.0)
                    },
                    downloaded_bytes: if selected.is_empty() {
                        Some(0)
                    } else {
                        Some(1024)
                    },
                    total_bytes: Some(1024),
                    download_rate_bps: Some(0),
                    raw: None,
                }),
                selection: Some(DebridFileSelection {
                    mode: DebridFileSelectionMode::BeforeTransfer,
                    selected_file_ids: selected,
                    skipped_file_ids: skipped,
                }),
                raw: None,
            }
        }
    }

    async fn setup_db() -> Result<Database> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        Ok(database)
    }

    async fn setup_debrid_test_state() -> Result<AppState> {
        let mut settings = Settings::default();
        let test_root = std::env::temp_dir().join(format!("elixir-debrid-test-{}", Uuid::new_v4()));
        settings.extensions.storage_root =
            test_root.join("extensions").to_string_lossy().to_string();
        settings.library.local_root = test_root.join("media").to_string_lossy().to_string();
        settings.database = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&settings.database).await?;
        database.run_migrations().await?;
        let auth_service = AuthService::new(settings.auth.clone())?;
        let metadata = MetadataService::new(settings.metadata.clone())?;
        let linkers = LinkerService::new(settings.classifier.clone())?;
        let artwork = ArtworkService::new(
            settings.library.artwork_cache_dir.clone(),
            settings.metadata.request_timeout_seconds,
        )?;
        Ok(AppState::new(
            settings,
            database,
            auth_service,
            ExtensionManager::new(),
            metadata,
            linkers,
            artwork,
            SecretsManager::from_key_bytes([44u8; 32], true),
        ))
    }

    async fn setup_debrid_factory_instance(
        pool: &sqlx::AnyPool,
        store: &ExtensionStore<'_>,
        config: Value,
    ) -> Result<Uuid> {
        ensure_debrid_builtin_records(pool, store).await?;
        let instance = store
            .list_instances(Some(DEBRID_EXTENSION_ID))
            .await?
            .into_iter()
            .next()
            .context("Debrid default instance should exist")?;
        store
            .update_instance_config(
                instance.instance_id,
                Some(&normalized_debrid_instance_config(Some(config))),
            )
            .await?;
        Ok(instance.instance_id)
    }

    async fn save_debrid_token(
        secrets: &SecretsManager,
        store: &ExtensionStore<'_>,
        instance_id: Uuid,
        service: DebridServiceKind,
        token: &str,
    ) -> Result<()> {
        store
            .upsert_secret(&NewSecret {
                secret_id: Uuid::new_v4(),
                scope: SecretScope::Instance,
                scope_id: Some(instance_id),
                key: service.secret_key().to_string(),
                value_encrypted: secrets.encrypt(token)?,
                rotatable: false,
            })
            .await?;
        Ok(())
    }

    async fn default_debrid_provider_id(
        store: &ExtensionStore<'_>,
        instance_id: Uuid,
    ) -> Result<Uuid> {
        store
            .list_providers(Some(instance_id))
            .await?
            .into_iter()
            .find(|provider| {
                provider.capability == "debrid.resolver" && provider.slot_id == "default"
            })
            .map(|provider| provider.provider_id)
            .context("default debrid provider should exist")
    }

    #[tokio::test]
    async fn dfu2_debrid_route_attempts_start_with_active_service_then_service_order() -> Result<()>
    {
        let state = setup_debrid_test_state().await?;
        let store = ExtensionStore::new(&state.db_pool);
        let instance_id = setup_debrid_factory_instance(
            &state.db_pool,
            &store,
            json!({
                "activeService": "torbox",
                "serviceOrder": ["premiumize", "real_debrid", "torbox", "all_debrid"]
            }),
        )
        .await?;
        for (service, token) in [
            (DebridServiceKind::TorBox, "tb-token"),
            (DebridServiceKind::Premiumize, "pm-token"),
            (DebridServiceKind::RealDebrid, "rd-token"),
        ] {
            save_debrid_token(state.secrets.as_ref(), &store, instance_id, service, token).await?;
        }
        let provider_id =
            reconcile_debrid_provider_for_instance(&state.db_pool, &store, instance_id).await?;
        let route_before = crate::download_broker::list_acquisition_routes(&state.db_pool, &store)
            .await?
            .routes
            .into_iter()
            .find(|route| {
                route.logical_id == DEBRID_DEFAULT_LOGICAL_ID
                    && route.owner_id == DEFAULT_ROUTE_OWNER_ID
            })
            .context("default debrid route should exist")?;

        let attempts = list_eligible_debrid_route_attempts(&state.db_pool, &store, None).await?;

        assert_eq!(
            attempts
                .iter()
                .map(|attempt| attempt.implementation.as_str())
                .collect::<Vec<_>>(),
            vec!["torbox", "premiumize", "real_debrid"]
        );
        assert_eq!(
            attempts
                .iter()
                .map(|attempt| attempt.display_name.as_str())
                .collect::<Vec<_>>(),
            vec!["TorBox", "Premiumize", "Real-Debrid"]
        );
        assert!(attempts.iter().all(|attempt| {
            attempt.provider_id == provider_id
                && attempt.instance_id == instance_id
                && attempt.health_state == ProviderHealthState::Healthy
        }));
        assert_eq!(
            attempts
                .iter()
                .map(|attempt| attempt.attempt_key.as_str())
                .collect::<Vec<_>>(),
            vec![
                format!("debrid:{provider_id}:torbox"),
                format!("debrid:{provider_id}:premiumize"),
                format!("debrid:{provider_id}:real_debrid")
            ]
        );

        let instance_after = store
            .get_instance(instance_id)
            .await?
            .context("debrid instance should still exist")?;
        assert_eq!(
            instance_after
                .config_json
                .as_ref()
                .and_then(|config| config.get("activeService"))
                .and_then(Value::as_str),
            Some("torbox")
        );
        let route_after = crate::download_broker::list_acquisition_routes(&state.db_pool, &store)
            .await?
            .routes
            .into_iter()
            .find(|route| {
                route.logical_id == DEBRID_DEFAULT_LOGICAL_ID
                    && route.owner_id == DEFAULT_ROUTE_OWNER_ID
            })
            .context("default debrid route should still exist")?;
        assert_eq!(
            route_after.selected_provider_id,
            route_before.selected_provider_id
        );
        assert_eq!(route_after.selected_provider_id, Some(provider_id));
        Ok(())
    }

    #[tokio::test]
    async fn dfu2_debrid_route_attempts_append_tokened_services_missing_from_service_order()
    -> Result<()> {
        let state = setup_debrid_test_state().await?;
        let store = ExtensionStore::new(&state.db_pool);
        let instance_id = setup_debrid_factory_instance(
            &state.db_pool,
            &store,
            json!({
                "activeService": "torbox",
                "serviceOrder": ["torbox", "real_debrid"]
            }),
        )
        .await?;
        for service in DebridServiceKind::ALL {
            save_debrid_token(
                state.secrets.as_ref(),
                &store,
                instance_id,
                service,
                "token",
            )
            .await?;
        }
        let provider_id =
            reconcile_debrid_provider_for_instance(&state.db_pool, &store, instance_id).await?;

        let attempts = list_eligible_debrid_route_attempts(&state.db_pool, &store, None).await?;

        assert_eq!(
            attempts
                .iter()
                .map(|attempt| attempt.implementation.as_str())
                .collect::<Vec<_>>(),
            vec!["torbox", "real_debrid", "all_debrid", "premiumize"]
        );
        assert_eq!(
            attempts
                .iter()
                .map(|attempt| attempt.attempt_key.as_str())
                .collect::<Vec<_>>(),
            vec![
                format!("debrid:{provider_id}:torbox"),
                format!("debrid:{provider_id}:real_debrid"),
                format!("debrid:{provider_id}:all_debrid"),
                format!("debrid:{provider_id}:premiumize")
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn dfu2_debrid_route_attempts_skip_unhealthy_provider() -> Result<()> {
        let state = setup_debrid_test_state().await?;
        let store = ExtensionStore::new(&state.db_pool);
        let instance_id = setup_debrid_factory_instance(
            &state.db_pool,
            &store,
            json!({
                "activeService": "torbox",
                "serviceOrder": ["torbox", "real_debrid"]
            }),
        )
        .await?;
        for service in [DebridServiceKind::TorBox, DebridServiceKind::RealDebrid] {
            save_debrid_token(
                state.secrets.as_ref(),
                &store,
                instance_id,
                service,
                "token",
            )
            .await?;
        }
        let provider_id =
            reconcile_debrid_provider_for_instance(&state.db_pool, &store, instance_id).await?;
        store
            .update_provider_health(provider_id, ProviderHealthState::Unhealthy)
            .await?;

        let attempts = list_eligible_debrid_route_attempts(&state.db_pool, &store, None).await?;

        assert!(attempts.is_empty());
        Ok(())
    }

    async fn insert_lifecycle_debrid_job(
        pool: &sqlx::AnyPool,
        provider_id: Uuid,
        instance_id: Uuid,
        service: DebridServiceKind,
        status: &str,
    ) -> Result<Uuid> {
        let job_id = Uuid::new_v4();
        insert_debrid_job(
            pool,
            &DebridDownloadJob {
                job_id,
                provider_id,
                instance_id,
                owner_id: "test.source".to_string(),
                source: "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567".to_string(),
                source_kind: "magnet".to_string(),
                category: Some("series".to_string()),
                display_name: Some("Show.S01E01.1080p.WEB-DL".to_string()),
                remote_torrent_id: Some("remote-release-1".to_string()),
                remote_download_id: None,
                provider_implementation: Some(service.implementation_id().to_string()),
                remote_release_id: Some("remote-release-1".to_string()),
                remote_release_status: Some(DebridReleaseStatus::Staging.as_str().to_string()),
                provider_capabilities: Some(json!(unsupported_debrid_capabilities())),
                provider_status: None,
                selection_mode: Some(
                    DebridFileSelectionMode::Unsupported
                        .as_persistence_value()
                        .to_string(),
                ),
                selected_file_ids: Vec::new(),
                skipped_file_ids: Vec::new(),
                selection_error: None,
                release_id: None,
                status: status.to_string(),
                local_path: None,
                links: Vec::new(),
                progress: Some(0.0),
                downloaded_bytes: Some(0),
                total_bytes: None,
                download_rate_bps: None,
                last_error: None,
            },
        )
        .await?;
        Ok(job_id)
    }

    async fn update_lifecycle_debrid_job_remote_id(
        pool: &sqlx::AnyPool,
        job_id: Uuid,
        remote_release_id: &str,
    ) -> Result<()> {
        sqlx::query::<sqlx::Any>(
            "UPDATE debrid_download_jobs
             SET remote_torrent_id = $1, remote_release_id = $2, updated_at = CURRENT_TIMESTAMP
             WHERE job_id = $3",
        )
        .bind(remote_release_id)
        .bind(remote_release_id)
        .bind(job_id.to_string())
        .execute(pool)
        .await?;
        Ok(())
    }

    async fn create_provider_refs(pool: &sqlx::AnyPool) -> Result<(Uuid, Uuid)> {
        let instance_id = Uuid::new_v4();
        let provider_id = Uuid::new_v4();
        let extension_id = format!("test.debrid.{instance_id}");
        sqlx::query::<sqlx::Any>(
            "INSERT INTO extensions (
                extension_id, name, version, kind, trust_level, manifest_json, enabled
             ) VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(&extension_id)
        .bind("Test Debrid")
        .bind("0.1.0")
        .bind("module")
        .bind("verified")
        .bind("{}")
        .bind(true)
        .execute(pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO extension_instances (
                instance_id, extension_id, instance_name, config_json, enabled
             ) VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(instance_id.to_string())
        .bind(&extension_id)
        .bind("default")
        .bind("{}")
        .bind(true)
        .execute(pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO providers (
                provider_id, instance_id, capability, slot_id, cardinality, implementation
             ) VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(provider_id.to_string())
        .bind(instance_id.to_string())
        .bind("debrid.resolver")
        .bind("default")
        .bind("one")
        .bind("test_debrid")
        .execute(pool)
        .await?;
        Ok((provider_id, instance_id))
    }

    async fn create_series_subscription_with_targets(pool: &sqlx::AnyPool) -> Result<Uuid> {
        let subscription_id = Uuid::new_v4();
        sqlx::query::<sqlx::Any>(
            "INSERT INTO acquisition_subscriptions (
                subscription_id, media_type, title, normalized_title, monitor_policy,
                route_policy, release_delay_seconds, metadata_refresh_after,
                candidate_search_after, status, active
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, $8, $9)",
        )
        .bind(subscription_id.to_string())
        .bind("series")
        .bind("Show")
        .bind("show")
        .bind("all_missing")
        .bind("debrid_first")
        .bind(0_i64)
        .bind("active")
        .bind(true)
        .execute(pool)
        .await?;
        for (key, episode) in [("S01E01", 1_i32), ("S01E02", 2_i32)] {
            sqlx::query::<sqlx::Any>(
                "INSERT INTO acquisition_targets (
                    target_id, subscription_id, target_key, media_type, title,
                    season_number, episode_number, state
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            )
            .bind(Uuid::new_v4().to_string())
            .bind(subscription_id.to_string())
            .bind(key)
            .bind("series")
            .bind("Show")
            .bind(1_i32)
            .bind(episode)
            .bind("pending")
            .execute(pool)
            .await?;
        }
        Ok(subscription_id)
    }

    async fn create_anime_subscription_with_target(
        pool: &sqlx::AnyPool,
        title: &str,
        target_title: &str,
        target_key: &str,
        season_number: i32,
        episode_number: i32,
        absolute_episode_number: i32,
    ) -> Result<Uuid> {
        let subscription_id = Uuid::new_v4();
        sqlx::query::<sqlx::Any>(
            "INSERT INTO acquisition_subscriptions (
                subscription_id, media_type, title, normalized_title, request_scope,
                monitor_policy, route_policy, release_delay_seconds, metadata_refresh_after,
                candidate_search_after, status, active
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, $9, $10)",
        )
        .bind(subscription_id.to_string())
        .bind("anime")
        .bind(title)
        .bind(title.trim().to_ascii_lowercase())
        .bind("episode")
        .bind("all_missing")
        .bind("debrid_first")
        .bind(0_i64)
        .bind("active")
        .bind(true)
        .execute(pool)
        .await?;
        let anilist_season_id = format!("test-anilist-{season_number}");
        sqlx::query::<sqlx::Any>(
            "INSERT INTO acquisition_targets (
                target_id, subscription_id, target_key, media_type, title,
                season_number, episode_number, absolute_episode_number, metadata_json, state
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(subscription_id.to_string())
        .bind(target_key)
        .bind("anime")
        .bind(target_title)
        .bind(season_number)
        .bind(episode_number)
        .bind(absolute_episode_number)
        .bind(
            json!({
                "graphFingerprint": format!("alm7-test:{subscription_id}"),
                "targetCanonicalKey": format!("anilist:{anilist_season_id}:{episode_number}"),
                "anilistSeasonId": anilist_season_id,
                "aliases": [title, target_title],
                "scopedAliases": [{
                    "display": target_title,
                    "source": "anilist_season_title",
                    "language": "en",
                    "seasonNumber": season_number,
                    "anilistSeasonId": format!("test-anilist-{season_number}")
                }]
            })
            .to_string(),
        )
        .bind("pending")
        .execute(pool)
        .await?;
        Ok(subscription_id)
    }

    #[derive(Debug, Clone)]
    struct OwnedAnimeDebridSelectionFixture {
        job_id: Uuid,
        release_id: Uuid,
        target_id: Uuid,
        remote_release_id: String,
        provider_file_id: String,
    }

    async fn setup_owned_ready_anime_debrid_selection(
        pool: &sqlx::AnyPool,
        adapter: &FakeDebridAdapter,
        fingerprint: &str,
    ) -> Result<OwnedAnimeDebridSelectionFixture> {
        let (provider_id, instance_id) = create_provider_refs(pool).await?;
        let subscription_id = create_anime_subscription_with_target(
            pool,
            "Ready Replay Anime",
            "Ready Replay Anime",
            "S01E01",
            1,
            1,
            1,
        )
        .await?;
        let target = list_subscription_targets(pool, subscription_id)
            .await?
            .into_iter()
            .next()
            .context("owned Ready anime target")?;
        let source = "magnet:?xt=urn:btih:7777777777777777777777777777777777777777&dn=Ready.Replay.Anime.S01E01";
        let job_id = submit_debrid_with_adapter(
            pool,
            provider_id,
            instance_id,
            source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("anime"),
                name: Some("Ready Replay Anime S01E01"),
                paused: false,
                release_context: Some(DebridReleaseSubmitContext {
                    subscription_id: Some(subscription_id),
                    source_provider_id: Some(provider_id),
                    source_extension_id: "test.source".to_string(),
                    media_type: MediaType::Anime,
                    title: "Ready Replay Anime".to_string(),
                    release_title: "Ready.Replay.Anime.S01E01.1080p.WEB-DL".to_string(),
                    info_hash: Some("7777777777777777777777777777777777777777".to_string()),
                    fingerprint: Some(fingerprint.to_string()),
                    score: Some(100.0),
                    selected_candidate: Some(json!({
                        "title": "Ready.Replay.Anime.S01E01.1080p.WEB-DL",
                        "source": source,
                        "sourceKind": "magnet",
                        "submissionBookkeeping": { "status": "pending" }
                    })),
                }),
            },
            adapter,
        )
        .await?;
        let job = load_debrid_job(pool, job_id)
            .await?
            .context("owned Ready anime Debrid job")?;
        let release_id = job.release_id.context("owned Ready anime release id")?;
        let release = get_release(pool, release_id)
            .await?
            .context("owned Ready anime release")?;
        let remote_file = adapter
            .template_files
            .first()
            .context("owned Ready anime fixture requires one provider file")?;
        let mut file = test_release_file(
            release.release_id,
            &remote_file.provider_file_id,
            &remote_file.path,
            true,
        );
        file.file_index = remote_file.file_index;
        file.size_bytes = remote_file.size_bytes.and_then(u64_to_i64);
        file.parsed_title = Some("Ready Replay Anime".to_string());
        file.parsed_season_number = Some(1);
        file.parsed_episode_number = Some(1);
        file.parsed_episode_end_number = Some(1);
        file.parsed_absolute_episode_number = Some(1);
        file.parsed_absolute_episode_end_number = Some(1);
        let file = insert_test_release_file(pool, &file).await?;
        let coverage = upsert_release_coverage(
            pool,
            NewAcquisitionReleaseCoverage {
                coverage_id: None,
                release_id: release.release_id,
                release_file_id: None,
                target_id: target.target_id,
                coverage_kind: ReleaseCoverageKind::SingleEpisode,
                confidence: ReleaseConfidence::High,
                score: Some(100.0),
                reason: Some("owned Ready anime exact mapping".to_string()),
                state: ReleaseCoverageState::Submitted,
                verified_by: Some("alm7_owned_ready_anime_fixture".to_string()),
            },
        )
        .await?;
        update_target_state(
            pool,
            target.target_id,
            AcquisitionTargetStateUpdate {
                state: AcquisitionTargetState::Submitted,
                state_reason: Some("owned Ready anime attempt submitted".to_string()),
                selected_provider_id: Some(provider_id),
                selected_route_logical_id: Some(DEBRID_DEFAULT_LOGICAL_ID.to_string()),
                selected_candidate: Some(json!({ "fingerprint": fingerprint })),
                download_id: Some(job_id.to_string()),
                ..Default::default()
            },
        )
        .await?;
        let decision = DebridFileSelectionDecision {
            status: DebridSelectionDecisionStatus::Approved,
            selected_file_ids: vec![remote_file.provider_file_id.clone()],
            skipped_file_ids: Vec::new(),
            provider_selection_ids: vec![remote_file.provider_file_id.clone()],
            target_file_selections: vec![DebridTargetFileSelection {
                target_id: target.target_id,
                provider_file_id: remote_file.provider_file_id.clone(),
            }],
            review_reasons: Vec::new(),
            policy_version: DEBRID_SELECTION_POLICY_VERSION.to_string(),
            coverage_fingerprint: format!("sha256:{fingerprint}"),
            select_all: false,
            select_all_approved: true,
        };
        assert!(
            persist_debrid_selection_decision(
                pool,
                job_id,
                &release,
                std::slice::from_ref(&file),
                std::slice::from_ref(&coverage),
                &decision,
            )
            .await?,
            "owned Ready anime selection intent must commit"
        );
        // Simulate the acquisition writer completing its internal submission
        // barrier before the worker replays provider selection.
        sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_releases
             SET selected_candidate_json = $1, updated_at = CURRENT_TIMESTAMP
             WHERE release_id = $2",
        )
        .bind(
            json!({
                "title": "Ready.Replay.Anime.S01E01.1080p.WEB-DL",
                "source": source,
                "sourceKind": "magnet"
            })
            .to_string(),
        )
        .bind(release.release_id.to_string())
        .execute(pool)
        .await?;

        let ready = get_release(pool, release.release_id)
            .await?
            .context("persisted Ready anime release")?;
        assert_eq!(ready.state, AcquisitionReleaseState::Ready);
        Ok(OwnedAnimeDebridSelectionFixture {
            job_id,
            release_id: release.release_id,
            target_id: target.target_id,
            remote_release_id: job
                .remote_release_id
                .context("owned Ready anime remote release id")?,
            provider_file_id: remote_file.provider_file_id.clone(),
        })
    }

    fn anime_automatic_evidence_has_review_semantics(value: &Value) -> bool {
        match value {
            Value::Object(object) => object.iter().any(|(key, value)| {
                key.to_ascii_lowercase().contains("review")
                    || anime_automatic_evidence_has_review_semantics(value)
            }),
            Value::Array(values) => values
                .iter()
                .any(anime_automatic_evidence_has_review_semantics),
            Value::String(value) => {
                let normalized = value.trim().to_ascii_lowercase();
                normalized == "review"
                    || normalized == "review_required"
                    || normalized.contains("requires_review")
                    || normalized.contains("requires review")
                    || normalized.contains("_review")
                    || normalized.contains("waiting for review")
            }
            _ => false,
        }
    }

    fn anime_evidence_has_nonempty_review_outcome(value: &Value) -> bool {
        fn has_content(value: &Value) -> bool {
            match value {
                Value::Null => false,
                Value::Bool(value) => *value,
                Value::Number(_) => true,
                Value::String(value) => !value.trim().is_empty(),
                Value::Array(values) => values.iter().any(has_content),
                Value::Object(object) => object.values().any(has_content),
            }
        }

        match value {
            Value::Object(object) => object.iter().any(|(key, value)| {
                let normalized_key = key.trim().to_ascii_lowercase();
                let is_review_outcome_key = matches!(
                    normalized_key.as_str(),
                    "review"
                        | "reviewreason"
                        | "reviewreasons"
                        | "review_reason"
                        | "review_reasons"
                        | "reviewrequired"
                        | "review_required"
                        | "requiresreview"
                        | "requires_review"
                );
                (is_review_outcome_key && has_content(value))
                    || anime_evidence_has_nonempty_review_outcome(value)
            }),
            Value::Array(values) => values
                .iter()
                .any(anime_evidence_has_nonempty_review_outcome),
            Value::String(value) => {
                let normalized = value.trim().to_ascii_lowercase();
                normalized == "review"
                    || normalized == "review_required"
                    || normalized.contains("requires_review")
                    || normalized.contains("requires review")
                    || normalized.contains("waiting for review")
            }
            _ => false,
        }
    }

    async fn assert_no_anime_review_lane_artifacts(
        pool: &sqlx::AnyPool,
        release: &AcquisitionRelease,
    ) -> Result<()> {
        let subscription_id = release
            .subscription_id
            .context("anime automatic-retry subscription")?;
        let review_releases = sqlx::query_scalar::<sqlx::Any, i64>(
            "SELECT COUNT(*)
             FROM acquisition_releases
             WHERE subscription_id = $1
               AND state = 'review_required'",
        )
        .bind(subscription_id.to_string())
        .fetch_one(pool)
        .await?;
        assert_eq!(
            review_releases, 0,
            "automatic anime retry must not create a review release artifact"
        );

        let review_audits = sqlx::query_scalar::<sqlx::Any, i64>(
            "SELECT COUNT(*)
             FROM acquisition_audit_events
             WHERE subscription_id = $1
               AND (
                   event_type IN (
                       'review_candidate_created',
                       'inspect_requested',
                       'manual_approval',
                       'manual_rejection'
                   )
                   OR state = 'review_required'
               )",
        )
        .bind(subscription_id.to_string())
        .fetch_one(pool)
        .await?;
        assert_eq!(
            review_audits, 0,
            "automatic anime retry must not emit a review-lane audit event"
        );
        Ok(())
    }

    async fn create_movie_subscription_with_target(
        pool: &sqlx::AnyPool,
        title: &str,
    ) -> Result<Uuid> {
        let subscription_id = Uuid::new_v4();
        sqlx::query::<sqlx::Any>(
            "INSERT INTO acquisition_subscriptions (
                subscription_id, media_type, title, normalized_title, monitor_policy,
                route_policy, release_delay_seconds, metadata_refresh_after,
                candidate_search_after, status, active
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, $8, $9)",
        )
        .bind(subscription_id.to_string())
        .bind("movie")
        .bind(title)
        .bind(title.trim().to_ascii_lowercase())
        .bind("all_missing")
        .bind("debrid_first")
        .bind(0_i64)
        .bind("active")
        .bind(true)
        .execute(pool)
        .await?;
        sqlx::query::<sqlx::Any>(
            "INSERT INTO acquisition_targets (
                target_id, subscription_id, target_key, media_type, title, state
             ) VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(subscription_id.to_string())
        .bind("movie")
        .bind("movie")
        .bind(title)
        .bind("pending")
        .execute(pool)
        .await?;
        Ok(subscription_id)
    }

    #[derive(Clone, Default)]
    struct MockRealDebridState {
        added_magnets: Arc<Mutex<Vec<String>>>,
        selected_files: Arc<Mutex<Vec<String>>>,
        deleted_releases: Arc<Mutex<Vec<String>>>,
    }

    #[derive(Clone, Default)]
    struct MockTorBoxState {
        added_magnets: Arc<Mutex<Vec<String>>>,
        requested_downloads: Arc<Mutex<Vec<String>>>,
        deleted_releases: Arc<Mutex<Vec<String>>>,
        ready: Arc<Mutex<bool>>,
        failed_no_seeds: Arc<Mutex<bool>>,
    }

    #[derive(Clone, Default)]
    struct MockAllDebridState {
        added_magnets: Arc<Mutex<Vec<String>>>,
        unlocked_links: Arc<Mutex<Vec<String>>>,
        deleted_releases: Arc<Mutex<Vec<String>>>,
        ready: Arc<Mutex<bool>>,
    }

    #[derive(Clone, Default)]
    struct MockPremiumizeState {
        directdl_sources: Arc<Mutex<Vec<String>>>,
        created_transfers: Arc<Mutex<Vec<String>>>,
        deleted_transfers: Arc<Mutex<Vec<String>>>,
        transfer_list_calls: Arc<Mutex<usize>>,
    }

    async fn start_mock_real_debrid_server()
    -> Result<(String, MockRealDebridState, oneshot::Sender<()>)> {
        let state = MockRealDebridState::default();
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let app = Router::new()
            .route("/user", get(mock_real_debrid_user))
            .route("/torrents/addMagnet", post(mock_real_debrid_add_magnet))
            .route("/torrents/info/:id", get(mock_real_debrid_torrent_info))
            .route(
                "/torrents/selectFiles/:id",
                post(mock_real_debrid_select_files),
            )
            .route("/torrents/delete/:id", axum_delete(mock_real_debrid_delete))
            .route("/unrestrict/link", post(mock_real_debrid_unrestrict))
            .route("/download/:name", get(mock_real_debrid_download))
            .with_state(state.clone());
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });
        Ok((format!("http://{address}"), state, shutdown_tx))
    }

    async fn start_mock_torbox_server() -> Result<(String, oneshot::Sender<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let app = Router::new().route("/api/user/me", get(mock_torbox_user));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });
        Ok((format!("http://{address}/api"), shutdown_tx))
    }

    async fn start_mock_all_debrid_server() -> Result<(String, oneshot::Sender<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let app = Router::new().route("/v4/user", get(mock_all_debrid_user));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });
        Ok((format!("http://{address}/v4"), shutdown_tx))
    }

    async fn start_mock_premiumize_server() -> Result<(String, oneshot::Sender<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let app = Router::new().route("/api/account/info", get(mock_premiumize_account_info));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });
        Ok((format!("http://{address}/api"), shutdown_tx))
    }

    async fn start_mock_premiumize_directdl_server()
    -> Result<(String, MockPremiumizeState, oneshot::Sender<()>)> {
        let state = MockPremiumizeState::default();
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let app = Router::new()
            .route("/api/account/info", get(mock_premiumize_account_info))
            .route("/api/transfer/directdl", post(mock_premiumize_directdl))
            .route(
                "/api/transfer/create",
                post(mock_premiumize_transfer_create),
            )
            .route("/api/transfer/list", get(mock_premiumize_transfer_list))
            .route(
                "/api/transfer/delete",
                post(mock_premiumize_transfer_delete),
            )
            .route("/api/item/details", get(mock_premiumize_item_details))
            .route("/api/folder/list", get(mock_premiumize_folder_list))
            .route("/download/:name", get(mock_premiumize_download))
            .with_state(state.clone());
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });
        Ok((format!("http://{address}/api"), state, shutdown_tx))
    }

    async fn start_mock_all_debrid_lifecycle_server()
    -> Result<(String, MockAllDebridState, oneshot::Sender<()>)> {
        let state = MockAllDebridState::default();
        *state.ready.lock().unwrap() = true;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let app = Router::new()
            .route("/v4/user", get(mock_all_debrid_user))
            .route("/v4/magnet/upload", post(mock_all_debrid_upload))
            .route("/v4.1/magnet/status", post(mock_all_debrid_status))
            .route("/v4/magnet/files", post(mock_all_debrid_files))
            .route("/v4/link/unlock", post(mock_all_debrid_unlock))
            .route("/v4/magnet/delete", post(mock_all_debrid_delete))
            .route("/download/:name", get(mock_all_debrid_download))
            .with_state(state.clone());
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });
        Ok((format!("http://{address}/v4"), state, shutdown_tx))
    }

    async fn start_mock_torbox_lifecycle_server()
    -> Result<(String, MockTorBoxState, oneshot::Sender<()>)> {
        let state = MockTorBoxState::default();
        *state.ready.lock().unwrap() = true;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let app = Router::new()
            .route("/api/user/me", get(mock_torbox_user))
            .route("/api/torrents/checkcached", get(mock_torbox_check_cached))
            .route(
                "/api/torrents/createtorrent",
                post(mock_torbox_create_torrent),
            )
            .route("/api/torrents/mylist", get(mock_torbox_mylist))
            .route("/api/torrents/requestdl", get(mock_torbox_request_download))
            .route(
                "/api/torrents/controltorrent",
                post(mock_torbox_control_torrent),
            )
            .route(
                "/api/download/:torrent_id/:file_name",
                get(mock_torbox_download),
            )
            .with_state(state.clone());
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });
        Ok((format!("http://{address}/api"), state, shutdown_tx))
    }

    async fn mock_torbox_user(headers: HeaderMap) -> impl IntoResponse {
        let authorization = headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        if authorization == "Bearer good-token" {
            return Json(json!({
                "success": true,
                "detail": "User data retrieved successfully.",
                "data": {
                    "id": 12345,
                    "username": "torbox-user",
                    "email": "torbox@example.test",
                    "plan": 2
                }
            }))
            .into_response();
        }
        if authorization == "Bearer rate-limit-token" {
            return (
                HttpStatusCode::TOO_MANY_REQUESTS,
                Json(json!({
                    "success": false,
                    "detail": "Too many requests. Please respect the rate limit."
                })),
            )
                .into_response();
        }
        if authorization == "Bearer echo-token" {
            return (
                HttpStatusCode::FORBIDDEN,
                Json(json!({
                    "success": false,
                    "detail": "Invalid API token echo-token"
                })),
            )
                .into_response();
        }
        (
            HttpStatusCode::FORBIDDEN,
            Json(json!({
                "success": false,
                "detail": "Invalid API token."
            })),
        )
            .into_response()
    }

    async fn mock_premiumize_account_info(headers: HeaderMap) -> impl IntoResponse {
        let authorization = headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        if authorization == "Bearer good-token" {
            return Json(json!({
                "status": "success",
                "customer_id": "pm-customer-123",
                "premium_until": 1799999999_i64,
                "limit_used": 0.42,
                "booster_points": 3
            }))
            .into_response();
        }
        if authorization == "Bearer rate-limit-token" {
            return Json(json!({
                "status": "error",
                "code": "rate_limit_reached",
                "message": "API rate limit reached"
            }))
            .into_response();
        }
        if authorization == "Bearer account-limit-token" {
            return Json(json!({
                "status": "error",
                "code": "account_limit_reached",
                "message": "Your fair-use points are exhausted"
            }))
            .into_response();
        }
        if authorization == "Bearer echo-token" {
            return Json(json!({
                "status": "error",
                "code": "authentication_failed",
                "message": "API key echo-token is invalid"
            }))
            .into_response();
        }
        Json(json!({
            "status": "error",
            "code": "authentication_failed",
            "message": "API key is missing, invalid, or expired"
        }))
        .into_response()
    }

    async fn mock_premiumize_directdl(
        State(state): State<MockPremiumizeState>,
        headers: HeaderMap,
        Form(form): Form<HashMap<String, String>>,
    ) -> impl IntoResponse {
        if let Some(response) = mock_premiumize_auth_failure(&headers) {
            return response;
        }
        let src = form.get("src").cloned().unwrap_or_default();
        state.directdl_sources.lock().unwrap().push(src.clone());
        if src.contains("bad-magnet") {
            return Json(json!({
                "status": "error",
                "code": "service_unsupported",
                "message": "The submitted source is unsupported"
            }))
            .into_response();
        }
        if src.contains("service-down") {
            return Json(json!({
                "status": "error",
                "code": "service_down",
                "message": "The target service is unreachable right now"
            }))
            .into_response();
        }
        if src.contains("queue-fallback")
            || src.contains("single-file-transfer")
            || src.contains("progress-transfer")
            || src.contains("container-transfer")
        {
            return Json(json!({
                "status": "error",
                "code": "link_generation_failed",
                "message": "There was an error generating the link right now"
            }))
            .into_response();
        }
        if src.contains("account-limit") {
            return Json(json!({
                "status": "error",
                "code": "account_limit_reached",
                "message": "Your fair-use points are exhausted"
            }))
            .into_response();
        }
        if src.contains("no-content") {
            return Json(json!({
                "status": "success",
                "content": []
            }))
            .into_response();
        }
        if src.contains("no-link") {
            return Json(json!({
                "status": "success",
                "content": [{
                    "path": "Broken/Show.S01E01.mkv",
                    "size": 1234
                }]
            }))
            .into_response();
        }

        let host = headers
            .get("host")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("127.0.0.1");
        let download_url = |name: &str| format!("http://{host}/download/{name}");
        if src.contains("single-hoster") || src.contains("movie") {
            return Json(json!({
                "status": "success",
                "location": download_url("Movie.2024.1080p.mkv"),
                "filename": "Movie.2024.1080p.mkv",
                "filesize": 8192,
                "content": [{
                    "path": "Movie.2024.1080p.mkv",
                    "size": 8192,
                    "link": download_url("Movie.2024.1080p.mkv"),
                    "stream_link": download_url("ignored-stream-link.mkv"),
                    "transcode_status": "good_as_is"
                }]
            }))
            .into_response();
        }

        Json(json!({
            "status": "success",
            "location": download_url("legacy-location-ignored.mkv"),
            "filename": "legacy-filename-ignored.mkv",
            "filesize": 1,
            "content": [
                {
                    "path": "Show/Season 01/Show.S01E01.mkv",
                    "size": 2048,
                    "link": download_url("Show.S01E01.mkv")
                },
                {
                    "path": "Show/Season 01/Show.S01E02.mkv",
                    "size": 4096,
                    "link": download_url("Show.S01E02.mkv")
                },
                {
                    "path": "Show/Season 01/Sample.mkv",
                    "size": 128,
                    "link": download_url("Sample.mkv")
                },
                {
                    "path": "Show/Season 01/Extras/trailer.mp4",
                    "size": 256,
                    "link": download_url("trailer.mp4")
                },
                {
                    "path": "Show/Season 01/archive.rar",
                    "size": 512,
                    "link": download_url("archive.rar")
                },
                {
                    "path": "Show/Season 01/notes.txt",
                    "size": 64,
                    "link": download_url("notes.txt")
                }
            ]
        }))
        .into_response()
    }

    async fn mock_premiumize_transfer_create(
        State(state): State<MockPremiumizeState>,
        headers: HeaderMap,
        Form(form): Form<HashMap<String, String>>,
    ) -> impl IntoResponse {
        if let Some(response) = mock_premiumize_auth_failure(&headers) {
            return response;
        }
        let src = form.get("src").cloned().unwrap_or_default();
        state.created_transfers.lock().unwrap().push(src.clone());
        if src.contains("container-transfer") {
            return Json(json!({
                "status": "success",
                "type": "container",
                "content": ["https://example.test/one", "https://example.test/two"]
            }))
            .into_response();
        }
        let id = if src.contains("single-file-transfer") {
            "pm-transfer-file"
        } else if src.contains("progress-transfer") {
            "pm-transfer-progress"
        } else {
            "pm-transfer-folder"
        };
        Json(json!({
            "status": "success",
            "id": id,
            "name": "Premiumize Queued Transfer"
        }))
        .into_response()
    }

    async fn mock_premiumize_transfer_list(
        State(state): State<MockPremiumizeState>,
        headers: HeaderMap,
    ) -> impl IntoResponse {
        if let Some(response) = mock_premiumize_auth_failure(&headers) {
            return response;
        }
        let mut calls = state.transfer_list_calls.lock().unwrap();
        *calls = calls.saturating_add(1);
        let progress_status = match *calls {
            1 => json!({
                "id": "pm-transfer-progress",
                "name": "Progress.Show.S01E01.mkv",
                "status": "queued",
                "progress": 0.0,
                "message": "Queued",
                "folder_id": null,
                "file_id": null
            }),
            2 => json!({
                "id": "pm-transfer-progress",
                "name": "Progress.Show.S01E01.mkv",
                "status": "running",
                "progress": 0.42,
                "message": "Downloading from server",
                "folder_id": null,
                "file_id": null
            }),
            _ => json!({
                "id": "pm-transfer-progress",
                "name": "Progress.Show.S01E01.mkv",
                "status": "finished",
                "progress": 1.0,
                "message": "",
                "folder_id": null,
                "file_id": "file-progress"
            }),
        };
        Json(json!({
            "status": "success",
            "transfers": [
                progress_status,
                {
                    "id": "pm-transfer-folder",
                    "name": "Show.S01.Pack",
                    "status": "finished",
                    "progress": 1.0,
                    "message": "",
                    "folder_id": "folder-root",
                    "file_id": null
                },
                {
                    "id": "pm-transfer-file",
                    "name": "Movie.2024.1080p.mkv",
                    "status": "finished",
                    "progress": 1.0,
                    "message": "",
                    "folder_id": "movies",
                    "file_id": "file-movie"
                },
                {
                    "id": "pm-transfer-error",
                    "name": "Broken.Release",
                    "status": "error",
                    "progress": 0.0,
                    "message": "service_down: target unavailable",
                    "folder_id": null,
                    "file_id": null
                }
            ]
        }))
        .into_response()
    }

    async fn mock_premiumize_transfer_delete(
        State(state): State<MockPremiumizeState>,
        headers: HeaderMap,
        Form(form): Form<HashMap<String, String>>,
    ) -> impl IntoResponse {
        if let Some(response) = mock_premiumize_auth_failure(&headers) {
            return response;
        }
        let id = form.get("id").cloned().unwrap_or_default();
        if id == "pm-transfer-missing" {
            return Json(json!({
                "status": "error",
                "code": "not_found",
                "message": "Transfer not found"
            }))
            .into_response();
        }
        state.deleted_transfers.lock().unwrap().push(id);
        Json(json!({ "status": "success" })).into_response()
    }

    async fn mock_premiumize_item_details(
        headers: HeaderMap,
        Query(query): Query<HashMap<String, String>>,
    ) -> impl IntoResponse {
        if let Some(response) = mock_premiumize_auth_failure(&headers) {
            return response;
        }
        let id = query.get("id").map(String::as_str).unwrap_or_default();
        let host = headers
            .get("host")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("127.0.0.1");
        let download_url = |name: &str| format!("http://{host}/download/{name}");
        match id {
            "file-movie" => Json(json!({
                "status": "success",
                "id": "file-movie",
                "name": "Movie.2024.1080p.mkv",
                "size": 8192,
                "created_at": 1700000000_i64,
                "folder_id": "movies",
                "mime_type": "video/x-matroska",
                "link": download_url("Movie.2024.1080p.mkv"),
                "directlink": download_url("ignored-directlink.mkv"),
                "stream_link": download_url("ignored-stream-link.mkv"),
                "transcode_status": "good_as_is"
            }))
            .into_response(),
            "file-progress" => Json(json!({
                "status": "success",
                "id": "file-progress",
                "name": "Progress.Show.S01E01.mkv",
                "size": 2048,
                "created_at": 1700000001_i64,
                "folder_id": "progress",
                "mime_type": "video/x-matroska",
                "link": download_url("Progress.Show.S01E01.mkv")
            }))
            .into_response(),
            _ => Json(json!({
                "status": "error",
                "code": "not_found",
                "message": "Item not found"
            }))
            .into_response(),
        }
    }

    async fn mock_premiumize_folder_list(
        headers: HeaderMap,
        Query(query): Query<HashMap<String, String>>,
    ) -> impl IntoResponse {
        if let Some(response) = mock_premiumize_auth_failure(&headers) {
            return response;
        }
        let id = query.get("id").map(String::as_str).unwrap_or_default();
        let host = headers
            .get("host")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("127.0.0.1");
        let download_url = |name: &str| format!("http://{host}/download/{name}");
        match id {
            "folder-root" => Json(json!({
                "status": "success",
                "name": "Show",
                "folder_id": "folder-root",
                "content": [
                    {
                        "id": "cloud-ep1",
                        "name": "Show.S01E01.mkv",
                        "type": "file",
                        "created_at": 1700000000_i64,
                        "size": 2048,
                        "mime_type": "video/x-matroska",
                        "link": download_url("Show.S01E01.mkv"),
                        "directlink": download_url("ignored-directlink.mkv"),
                        "stream_link": download_url("ignored-stream-link.mkv"),
                        "transcode_status": "good_as_is"
                    },
                    {
                        "id": "cloud-sample",
                        "name": "Sample.mkv",
                        "type": "file",
                        "created_at": 1700000001_i64,
                        "size": 128,
                        "mime_type": "video/x-matroska",
                        "link": download_url("Sample.mkv")
                    },
                    {
                        "id": "cloud-archive",
                        "name": "extras.zip",
                        "type": "file",
                        "created_at": 1700000002_i64,
                        "size": 512,
                        "mime_type": "application/zip",
                        "link": download_url("extras.zip")
                    },
                    {
                        "id": "folder-s02",
                        "name": "Season 02",
                        "type": "folder",
                        "created_at": 1700000003_i64
                    }
                ]
            }))
            .into_response(),
            "folder-s02" => Json(json!({
                "status": "success",
                "name": "Season 02",
                "folder_id": "folder-s02",
                "content": [{
                    "id": "cloud-ep2",
                    "name": "Show.S02E01.mkv",
                    "type": "file",
                    "created_at": 1700000004_i64,
                    "size": 4096,
                    "mime_type": "video/x-matroska",
                    "link": download_url("Show.S02E01.mkv")
                }]
            }))
            .into_response(),
            _ => Json(json!({
                "status": "error",
                "code": "not_found",
                "message": "Folder not found"
            }))
            .into_response(),
        }
    }

    fn mock_premiumize_auth_failure(headers: &HeaderMap) -> Option<axum::response::Response> {
        let authorization = headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        match authorization {
            "Bearer good-token" => None,
            "Bearer rate-limit-token" => Some(
                Json(json!({
                    "status": "error",
                    "code": "rate_limit_reached",
                    "message": "API rate limit reached"
                }))
                .into_response(),
            ),
            "Bearer account-limit-token" => Some(
                Json(json!({
                    "status": "error",
                    "code": "account_limit_reached",
                    "message": "Your fair-use points are exhausted"
                }))
                .into_response(),
            ),
            "Bearer echo-token" => Some(
                Json(json!({
                    "status": "error",
                    "code": "authentication_failed",
                    "message": "API key echo-token is invalid"
                }))
                .into_response(),
            ),
            _ => Some(
                Json(json!({
                    "status": "error",
                    "code": "authentication_failed",
                    "message": "API key is missing, invalid, or expired"
                }))
                .into_response(),
            ),
        }
    }

    async fn mock_premiumize_download(AxumPath(name): AxumPath<String>) -> impl IntoResponse {
        Bytes::from(format!("premiumize-{name}"))
    }

    async fn mock_all_debrid_user(headers: HeaderMap) -> impl IntoResponse {
        let authorization = headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        if authorization == "Bearer good-token" {
            return Json(json!({
                "status": "success",
                "data": {
                    "user": {
                        "id": 67890,
                        "username": "alldebrid-user",
                        "email": "alldebrid@example.test",
                        "isPremium": true
                    }
                }
            }))
            .into_response();
        }
        if authorization == "Bearer rate-limit-token" {
            return (
                HttpStatusCode::TOO_MANY_REQUESTS,
                Json(json!({
                    "status": "error",
                    "error": {
                        "code": "RATE_LIMIT",
                        "message": "Too many requests. Please respect the rate limit."
                    }
                })),
            )
                .into_response();
        }
        if authorization == "Bearer echo-token" {
            return (
                HttpStatusCode::FORBIDDEN,
                Json(json!({
                    "status": "error",
                    "error": {
                        "code": "AUTH_BAD_APIKEY",
                        "message": "The auth apikey echo-token is invalid"
                    }
                })),
            )
                .into_response();
        }
        (
            HttpStatusCode::UNAUTHORIZED,
            Json(json!({
                "status": "error",
                "error": {
                    "code": "AUTH_BAD_APIKEY",
                    "message": "The auth apikey is invalid"
                }
            })),
        )
            .into_response()
    }

    async fn mock_all_debrid_upload(
        State(state): State<MockAllDebridState>,
        headers: HeaderMap,
        Form(form): Form<HashMap<String, String>>,
    ) -> impl IntoResponse {
        if let Some(response) = mock_all_debrid_auth_failure(&headers) {
            return response;
        }
        let magnet = form
            .get("magnets[]")
            .or_else(|| form.get("magnets"))
            .cloned()
            .unwrap_or_default();
        state.added_magnets.lock().unwrap().push(magnet.clone());
        if magnet.contains("bad") {
            return Json(json!({
                "status": "success",
                "data": {
                    "magnets": [{
                        "magnet": magnet,
                        "error": {
                            "code": "MAGNET_INVALID_URI",
                            "message": "Magnet is not valid"
                        }
                    }]
                }
            }))
            .into_response();
        }
        Json(json!({
            "status": "success",
            "data": {
                "magnets": [{
                    "magnet": magnet,
                    "hash": "0123456789abcdef0123456789abcdef01234567",
                    "name": "Show.S01.PACK",
                    "size": 4096,
                    "ready": *state.ready.lock().unwrap(),
                    "id": 88
                }]
            }
        }))
        .into_response()
    }

    async fn mock_all_debrid_status(
        State(state): State<MockAllDebridState>,
        headers: HeaderMap,
        Form(form): Form<HashMap<String, String>>,
    ) -> impl IntoResponse {
        if let Some(response) = mock_all_debrid_auth_failure(&headers) {
            return response;
        }
        let id = form.get("id").cloned().unwrap_or_else(|| "88".to_string());
        if id == "404" {
            return Json(json!({
                "status": "success",
                "data": { "magnets": [] }
            }))
            .into_response();
        }
        let ready = *state.ready.lock().unwrap();
        Json(json!({
            "status": "success",
            "data": {
                "magnets": [{
                    "id": id,
                    "filename": "Show.S01.PACK",
                    "size": 4096,
                    "status": if ready { "Ready" } else { "Downloading" },
                    "statusCode": if ready { 4 } else { 1 },
                    "downloaded": if ready { 4096 } else { 1720 },
                    "seeders": 7,
                    "downloadSpeed": if ready { 0 } else { 2048 }
                }]
            }
        }))
        .into_response()
    }

    async fn mock_all_debrid_files(
        State(_state): State<MockAllDebridState>,
        headers: HeaderMap,
        Form(form): Form<HashMap<String, String>>,
    ) -> impl IntoResponse {
        if let Some(response) = mock_all_debrid_auth_failure(&headers) {
            return response;
        }
        let id = form
            .get("id[]")
            .or_else(|| form.get("id"))
            .cloned()
            .unwrap_or_else(|| "88".to_string());
        if id == "404" {
            return Json(json!({
                "status": "success",
                "data": {
                    "magnets": [{
                        "id": id,
                        "error": {
                            "code": "MAGNET_INVALID_ID",
                            "message": "This magnet ID does not exists or is invalid"
                        }
                    }]
                }
            }))
            .into_response();
        }
        Json(json!({
            "status": "success",
            "data": {
                "magnets": [{
                    "id": id,
                    "files": [
                        {
                            "n": "Show",
                            "e": [
                                {
                                    "n": "Season 01",
                                    "e": [
                                        {
                                            "n": "Show.S01E01.mkv",
                                            "s": 2048,
                                            "l": "https://alldebrid.com/f/episode-1"
                                        },
                                        {
                                            "n": "Show.S01E02.mkv",
                                            "s": 2048,
                                            "l": "https://alldebrid.com/f/episode-2"
                                        },
                                        {
                                            "n": "sample.mkv",
                                            "s": 64,
                                            "l": "https://alldebrid.com/f/sample"
                                        }
                                    ]
                                }
                            ]
                        },
                        {
                            "n": "extras.zip",
                            "s": 128,
                            "l": "https://alldebrid.com/f/extras"
                        }
                    ]
                }]
            }
        }))
        .into_response()
    }

    async fn mock_all_debrid_unlock(
        State(state): State<MockAllDebridState>,
        headers: HeaderMap,
        Form(form): Form<HashMap<String, String>>,
    ) -> impl IntoResponse {
        if let Some(response) = mock_all_debrid_auth_failure(&headers) {
            return response;
        }
        let link = form.get("link").cloned().unwrap_or_default();
        state.unlocked_links.lock().unwrap().push(link.clone());
        if link.contains("dead") {
            return (
                HttpStatusCode::BAD_REQUEST,
                Json(json!({
                    "status": "error",
                    "error": {
                        "code": "LINK_DOWN",
                        "message": "This link is not available on the file hoster website"
                    }
                })),
            )
                .into_response();
        }
        let filename = if link.contains("episode-2") {
            "Show.S01E02.mkv"
        } else if link.contains("episode-1") {
            "Show.S01E01.mkv"
        } else {
            "direct-file.mkv"
        };
        let host = headers
            .get("host")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("mock-alldebrid.test");
        Json(json!({
            "status": "success",
            "data": {
                "link": format!("http://{host}/download/{filename}"),
                "filename": filename,
                "filesize": if filename.contains("E02") { 2048 } else { 1024 },
                "host": "alldebrid",
                "id": "mock-generation"
            }
        }))
        .into_response()
    }

    async fn mock_all_debrid_delete(
        State(state): State<MockAllDebridState>,
        headers: HeaderMap,
        Form(form): Form<HashMap<String, String>>,
    ) -> impl IntoResponse {
        if let Some(response) = mock_all_debrid_auth_failure(&headers) {
            return response;
        }
        let id = form.get("id").cloned().unwrap_or_default();
        if id == "404" {
            return (
                HttpStatusCode::BAD_REQUEST,
                Json(json!({
                    "status": "error",
                    "error": {
                        "code": "MAGNET_INVALID_ID",
                        "message": "Magnet ID is invalid"
                    }
                })),
            )
                .into_response();
        }
        state.deleted_releases.lock().unwrap().push(id);
        Json(json!({
            "status": "success",
            "data": {
                "message": "Magnet was successfully deleted"
            }
        }))
        .into_response()
    }

    fn mock_all_debrid_auth_failure(headers: &HeaderMap) -> Option<axum::response::Response> {
        let authorization = headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        if authorization == "Bearer good-token" {
            return None;
        }
        if authorization == "Bearer rate-limit-token" {
            return Some(
                (
                    HttpStatusCode::TOO_MANY_REQUESTS,
                    Json(json!({
                        "status": "error",
                        "error": {
                            "code": "RATE_LIMIT",
                            "message": "Too many requests. Please respect the rate limit."
                        }
                    })),
                )
                    .into_response(),
            );
        }
        Some(
            (
                HttpStatusCode::FORBIDDEN,
                Json(json!({
                    "status": "error",
                    "error": {
                        "code": "AUTH_BAD_APIKEY",
                        "message": "The auth apikey is invalid"
                    }
                })),
            )
                .into_response(),
        )
    }

    async fn mock_all_debrid_download(AxumPath(name): AxumPath<String>) -> impl IntoResponse {
        format!("mock-alldebrid-download-{name}")
    }

    async fn mock_torbox_check_cached(
        Query(query): Query<HashMap<String, String>>,
    ) -> impl IntoResponse {
        let hash = query
            .get("hash")
            .cloned()
            .unwrap_or_else(|| "0123456789abcdef0123456789abcdef01234567".to_string());
        Json(json!({
            "success": true,
            "detail": "Torrent cache status retrieved successfully.",
            "data": {
                hash.clone(): {
                    "name": "Show.S01.PACK",
                    "size": 4096,
                    "hash": hash,
                    "files": [
                        {
                            "id": 99,
                            "name": "cache-only-order/Show.S01E01.mkv",
                            "short_name": "Show.S01E01.mkv",
                            "size": 2048,
                            "mimetype": "video/x-matroska"
                        }
                    ]
                }
            }
        }))
    }

    async fn mock_torbox_create_torrent(
        State(state): State<MockTorBoxState>,
        headers: HeaderMap,
        body: Bytes,
    ) -> impl IntoResponse {
        let authorization = headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        if authorization != "Bearer good-token" {
            if authorization == "Bearer rate-limit-token" {
                return (
                    HttpStatusCode::TOO_MANY_REQUESTS,
                    Json(json!({
                        "success": false,
                        "detail": "Too many createtorrent requests. Please respect the rate limit."
                    })),
                )
                    .into_response();
            }
            return (
                HttpStatusCode::FORBIDDEN,
                Json(json!({
                    "success": false,
                    "detail": "Invalid API token."
                })),
            )
                .into_response();
        }
        let body = String::from_utf8_lossy(&body);
        let magnet = extract_mock_body_value(&body, "magnet")
            .or_else(|| extract_mock_magnet(&body))
            .unwrap_or_default();
        if magnet.is_empty() {
            return (
                HttpStatusCode::BAD_REQUEST,
                Json(json!({
                    "success": false,
                    "detail": "A magnet link is required."
                })),
            )
                .into_response();
        }
        state.added_magnets.lock().unwrap().push(magnet);
        Json(json!({
            "success": true,
            "detail": "Torrent Added Successfully",
            "data": {
                "hash": "0123456789abcdef0123456789abcdef01234567",
                "torrent_id": 77,
                "auth_id": "auth-77"
            }
        }))
        .into_response()
    }

    async fn mock_torbox_mylist(
        State(state): State<MockTorBoxState>,
        Query(query): Query<HashMap<String, String>>,
    ) -> impl IntoResponse {
        if query.get("id").map(String::as_str) == Some("404") {
            return (
                HttpStatusCode::NOT_FOUND,
                Json(json!({
                    "success": false,
                    "detail": "Torrent not found."
                })),
            )
                .into_response();
        }
        let ready = *state.ready.lock().unwrap();
        let failed_no_seeds = *state.failed_no_seeds.lock().unwrap();
        if failed_no_seeds {
            return Json(json!({
                "success": true,
                "detail": "Torrent list retrieved successfully.",
                "data": {
                    "id": 77,
                    "auth_id": "auth-77",
                    "hash": "0123456789abcdef0123456789abcdef01234567",
                    "name": "Show.S01.PACK",
                    "size": 4096,
                    "download_state": "stalled (no seeds)",
                    "progress": 0,
                    "download_speed": 0,
                    "total_downloaded": 0,
                    "download_finished": false,
                    "download_present": false,
                    "cached": false,
                    "files": null
                }
            }))
            .into_response();
        }
        Json(json!({
            "success": true,
            "detail": "Torrent list retrieved successfully.",
            "data": {
                "id": 77,
                "auth_id": "auth-77",
                "hash": "0123456789abcdef0123456789abcdef01234567",
                "name": "Show.S01.PACK",
                "size": 4096,
                "download_state": if ready { "cached" } else { "downloading" },
                "progress": if ready { 100 } else { 42 },
                "download_speed": if ready { 0 } else { 512000 },
                "total_downloaded": if ready { 4096 } else { 1720 },
                "download_finished": ready,
                "download_present": ready,
                "cached": ready,
                "files": [
                    {
                        "id": 10,
                        "hash": "file-hash-10",
                        "name": "Show/Season 01/Show.S01E01.mkv",
                        "short_name": "Show.S01E01.mkv",
                        "absolute_path": "Show/Season 01/Show.S01E01.mkv",
                        "size": 2048,
                        "zipped": false,
                        "infected": false,
                        "mimetype": "video/x-matroska"
                    },
                    {
                        "id": 11,
                        "hash": "file-hash-11",
                        "name": "Show/Season 01/Show.S01E02.mkv",
                        "short_name": "Show.S01E02.mkv",
                        "absolute_path": "Show/Season 01/Show.S01E02.mkv",
                        "size": 2048,
                        "zipped": false,
                        "infected": false,
                        "mimetype": "video/x-matroska"
                    }
                ]
            }
        }))
        .into_response()
    }

    async fn mock_torbox_request_download(
        State(state): State<MockTorBoxState>,
        headers: HeaderMap,
        Query(query): Query<HashMap<String, String>>,
    ) -> impl IntoResponse {
        if query.get("token").map(String::as_str) != Some("good-token") {
            return (
                HttpStatusCode::FORBIDDEN,
                Json(json!({
                    "success": false,
                    "detail": "Invalid API token."
                })),
            )
                .into_response();
        }
        let torrent_id = query.get("torrent_id").cloned().unwrap_or_default();
        let file_id = query
            .get("file_id")
            .cloned()
            .unwrap_or_else(|| "zip".to_string());
        state
            .requested_downloads
            .lock()
            .unwrap()
            .push(format!("{torrent_id}:{file_id}"));
        let host = headers
            .get("host")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("127.0.0.1");
        let file_name = match file_id.as_str() {
            "10" => "Show.S01E01.mkv",
            "11" => "Show.S01E02.mkv",
            _ => "Show.S01.PACK.zip",
        };
        Json(json!({
            "success": true,
            "detail": "Torrent download requested successfully.",
            "data": format!("http://{host}/api/download/{torrent_id}/{file_name}")
        }))
        .into_response()
    }

    async fn mock_torbox_download(
        AxumPath((_torrent_id, file_name)): AxumPath<(String, String)>,
    ) -> impl IntoResponse {
        (
            [("content-type", "application/octet-stream")],
            format!("mock-torbox-download-{file_name}"),
        )
    }

    async fn mock_torbox_control_torrent(
        State(state): State<MockTorBoxState>,
        Json(body): Json<Value>,
    ) -> impl IntoResponse {
        let torrent_id = body
            .get("torrent_id")
            .and_then(torbox_id_string)
            .unwrap_or_default();
        if torrent_id == "404" {
            return (
                HttpStatusCode::NOT_FOUND,
                Json(json!({
                    "success": false,
                    "detail": "Torrent not found."
                })),
            )
                .into_response();
        }
        let operation = body
            .get("operation")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if operation == "delete" {
            state.deleted_releases.lock().unwrap().push(torrent_id);
        }
        Json(json!({
            "success": true,
            "detail": "Torrent operationd successfully.",
            "data": null
        }))
        .into_response()
    }

    fn extract_mock_body_value(body: &str, field_name: &str) -> Option<String> {
        let marker = format!("name=\"{field_name}\"");
        let after_marker = body.split(&marker).nth(1)?;
        let value = after_marker.split("\r\n\r\n").nth(1)?;
        value
            .split("\r\n")
            .next()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    }

    fn extract_mock_magnet(body: &str) -> Option<String> {
        let start = body.find("magnet:?")?;
        let value = &body[start..];
        value
            .split(['\r', '\n', '"', '&'])
            .next()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    }

    async fn mock_real_debrid_user() -> impl IntoResponse {
        Json(json!({ "username": "rd-user" }))
    }

    async fn mock_real_debrid_add_magnet(
        State(state): State<MockRealDebridState>,
        Form(form): Form<HashMap<String, String>>,
    ) -> impl IntoResponse {
        if let Some(magnet) = form.get("magnet") {
            state.added_magnets.lock().unwrap().push(magnet.clone());
        }
        Json(
            json!({ "id": "rd-torrent-1", "uri": "https://real-debrid.test/torrents/rd-torrent-1" }),
        )
    }

    async fn mock_real_debrid_torrent_info(
        State(state): State<MockRealDebridState>,
        AxumPath(id): AxumPath<String>,
    ) -> impl IntoResponse {
        if id == "provider-error" {
            return (
                HttpStatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "error": "temporary" })),
            )
                .into_response();
        }
        let selected = state.selected_files.lock().unwrap().clone();
        let has_selection = !selected.is_empty();
        let files = [
            ("1", "/Show/Season 01/Show.S01E01.mkv"),
            ("2", "/sample.txt"),
        ]
        .into_iter()
        .map(|(file_id, path)| {
            json!({
                "id": file_id.parse::<i64>().unwrap(),
                "path": path,
                "bytes": if file_id == "1" { 2048 } else { 128 },
                "selected": selected.iter().any(|selected| selected == file_id) as i32
            })
        })
        .collect::<Vec<_>>();
        Json(json!({
            "id": id,
            "filename": "Show.S01.PACK",
            "bytes": 2176,
            "original_bytes": 2176,
            "progress": if has_selection { 100.0 } else { 0.0 },
            "status": if has_selection { "downloaded" } else { "waiting_files_selection" },
            "files": files,
            "links": if has_selection { json!(["https://real-debrid.test/link/1"]) } else { json!([]) },
            "speed": 0
        }))
        .into_response()
    }

    async fn mock_real_debrid_select_files(
        State(state): State<MockRealDebridState>,
        AxumPath(id): AxumPath<String>,
        Form(form): Form<HashMap<String, String>>,
    ) -> impl IntoResponse {
        if id == "provider-error" {
            return (
                HttpStatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "error": "temporary" })),
            )
                .into_response();
        }
        let selected = form
            .get("files")
            .map(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        *state.selected_files.lock().unwrap() = selected;
        HttpStatusCode::NO_CONTENT.into_response()
    }

    async fn mock_real_debrid_delete(
        State(state): State<MockRealDebridState>,
        AxumPath(id): AxumPath<String>,
    ) -> impl IntoResponse {
        state.deleted_releases.lock().unwrap().push(id);
        HttpStatusCode::NO_CONTENT
    }

    async fn mock_real_debrid_unrestrict(
        Form(form): Form<HashMap<String, String>>,
    ) -> impl IntoResponse {
        let link = form.get("link").cloned().unwrap_or_default();
        if link.contains("provider-error") {
            return (
                HttpStatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "error": "temporary" })),
            )
                .into_response();
        }
        let download = if link.contains("/download/") {
            link.clone()
        } else {
            "https://download.real-debrid.test/Show.S01E01.mkv".to_string()
        };
        Json(json!({
            "id": "unrestricted-1",
            "filename": "Show.S01E01.mkv",
            "filesize": 2048,
            "download": download
        }))
        .into_response()
    }

    async fn mock_real_debrid_download(AxumPath(_name): AxumPath<String>) -> impl IntoResponse {
        (
            [("content-type", "application/octet-stream")],
            "mock-real-debrid-download",
        )
    }

    #[async_trait::async_trait]
    impl DebridProviderAdapter for FakeDebridAdapter {
        fn implementation(&self) -> &str {
            "fake_debrid"
        }

        fn capabilities(&self) -> DebridProviderCapabilities {
            DebridProviderCapabilities {
                supports_magnet_submit: true,
                supports_hoster_unrestrict: true,
                supports_file_listing: true,
                supports_file_selection: true,
                supports_cache_check: true,
                supports_delete: true,
                supports_progress: true,
                file_selection_mode: DebridFileSelectionMode::BeforeTransfer,
            }
        }

        async fn test_account(&self) -> Result<DebridAccount> {
            Ok(DebridAccount {
                provider_implementation: self.implementation().to_string(),
                account_id: Some("fake-account".to_string()),
                username: Some("tester".to_string()),
                raw: Some(json!({ "ok": true })),
            })
        }

        async fn submit_magnet(&self, magnet: &str) -> Result<DebridRemoteRelease> {
            if self.fail_submit {
                bail!("fake provider submission failed with retry_or_review");
            }
            let mut state = self.state.lock().unwrap();
            state.next_id += 1;
            let remote_release_id = format!("fake-release-{}", state.next_id);
            let release = DebridRemoteRelease {
                provider_implementation: self.implementation().to_string(),
                remote_release_id: remote_release_id.clone(),
                display_name: Some(magnet.to_string()),
                status: if self.force_failed_submit_status {
                    DebridReleaseStatus::Failed
                } else {
                    DebridReleaseStatus::WaitingFiles
                },
                raw_status: Some(
                    if self.force_failed_submit_status {
                        "provider_failed"
                    } else {
                        "waiting_files"
                    }
                    .to_string(),
                ),
                raw: None,
            };
            state.releases.insert(
                remote_release_id.clone(),
                FakeDebridRelease {
                    release: release.clone(),
                    files: self.template_files.clone(),
                    selected_file_ids: Vec::new(),
                },
            );
            Ok(release)
        }

        async fn inspect_release(
            &self,
            remote_release_id: &str,
        ) -> Result<DebridReleaseInspection> {
            if self.fail_inspect {
                bail!("fake provider inspection failed with review_required");
            }
            let state = self.state.lock().unwrap();
            let release = state
                .releases
                .get(remote_release_id)
                .ok_or_else(|| anyhow!("fake release not found"))?;
            let status = if self.force_failed_inspection {
                DebridReleaseStatus::Failed
            } else if release.selected_file_ids.is_empty() {
                DebridReleaseStatus::WaitingFiles
            } else {
                DebridReleaseStatus::Selected
            };
            Ok(self.inspection(release, status))
        }

        async fn select_files(
            &self,
            remote_release_id: &str,
            selected_file_ids: &[String],
        ) -> Result<DebridReleaseInspection> {
            if self.fail_select {
                bail!("selecting debrid files failed");
            }
            let mut state = self.state.lock().unwrap();
            let release = state
                .releases
                .get_mut(remote_release_id)
                .ok_or_else(|| anyhow!("fake release not found"))?;
            release.selected_file_ids = selected_file_ids.to_vec();
            Ok(self.inspection(release, DebridReleaseStatus::Selected))
        }

        async fn list_links(&self, remote_release_id: &str) -> Result<Vec<DebridResolvedLink>> {
            Ok(self.inspect_release(remote_release_id).await?.links)
        }

        async fn unrestrict_hoster(&self, link: &str) -> Result<DebridResolvedLink> {
            if self.fail_unrestrict {
                bail!("fake provider unrestrict failed during materialization");
            }
            Ok(DebridResolvedLink {
                provider_file_id: None,
                url: link.to_string(),
                filename: Some("hoster-file.bin".to_string()),
                size_bytes: Some(2048),
                raw: None,
            })
        }

        async fn refresh_progress(&self, remote_release_id: &str) -> Result<DebridReleaseProgress> {
            self.inspect_release(remote_release_id)
                .await?
                .progress
                .ok_or_else(|| anyhow!("fake progress missing"))
        }

        async fn delete_release(&self, remote_release_id: &str) -> Result<bool> {
            if self.fail_delete {
                bail!("fake provider cleanup failed with review_required");
            }
            let mut state = self.state.lock().unwrap();
            state
                .deleted_release_ids
                .push(remote_release_id.to_string());
            Ok(state.releases.remove(remote_release_id).is_some())
        }
    }

    #[tokio::test]
    async fn debrid_submit_enforces_single_active_job_cap() -> Result<()> {
        let database = setup_db().await?;
        let (provider_id, instance_id) = create_provider_refs(&database.pool).await?;
        let adapter = FakeDebridAdapter::new();
        let first_source = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567";
        let second_source = "magnet:?xt=urn:btih:89abcdef012345670123456789abcdef01234567";

        let first_job_id = submit_debrid_with_adapter(
            &database.pool,
            provider_id,
            instance_id,
            first_source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("series"),
                name: Some("Show.S01E01.1080p.WEB-DL"),
                paused: false,
                release_context: None,
            },
            &adapter,
        )
        .await?;

        let err = submit_debrid_with_adapter(
            &database.pool,
            provider_id,
            instance_id,
            second_source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("series"),
                name: Some("Show.S01E02.1080p.WEB-DL"),
                paused: false,
                release_context: None,
            },
            &adapter,
        )
        .await
        .expect_err("second active Debrid submit should hit route capacity");
        assert!(err.to_string().contains("Debrid route capacity reached"));

        mark_debrid_job_status(&database.pool, first_job_id, "completed", None).await?;
        let second_job_id = submit_debrid_with_adapter(
            &database.pool,
            provider_id,
            instance_id,
            second_source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("series"),
                name: Some("Show.S01E02.1080p.WEB-DL"),
                paused: false,
                release_context: None,
            },
            &adapter,
        )
        .await?;
        assert_ne!(first_job_id, second_job_id);
        Ok(())
    }

    #[tokio::test]
    async fn debrid_submit_respects_configured_active_job_cap() -> Result<()> {
        let database = setup_db().await?;
        let (provider_id, instance_id) = create_provider_refs(&database.pool).await?;
        sqlx::query::<sqlx::Any>(
            "UPDATE extension_instances
             SET config_json = $1
             WHERE instance_id = $2",
        )
        .bind(json!({ "maxConcurrentDownloads": 2 }).to_string())
        .bind(instance_id.to_string())
        .execute(&database.pool)
        .await?;

        let adapter = FakeDebridAdapter::new();
        let first_source = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567";
        let second_source = "magnet:?xt=urn:btih:89abcdef012345670123456789abcdef01234567";
        let third_source = "magnet:?xt=urn:btih:fedcba98765432100123456789abcdef01234567";

        submit_debrid_with_adapter(
            &database.pool,
            provider_id,
            instance_id,
            first_source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("series"),
                name: Some("Show.S01E01.1080p.WEB-DL"),
                paused: false,
                release_context: None,
            },
            &adapter,
        )
        .await?;
        submit_debrid_with_adapter(
            &database.pool,
            provider_id,
            instance_id,
            second_source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("series"),
                name: Some("Show.S01E02.1080p.WEB-DL"),
                paused: false,
                release_context: None,
            },
            &adapter,
        )
        .await?;

        let err = submit_debrid_with_adapter(
            &database.pool,
            provider_id,
            instance_id,
            third_source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("series"),
                name: Some("Show.S01E03.1080p.WEB-DL"),
                paused: false,
                release_context: None,
            },
            &adapter,
        )
        .await
        .expect_err("third active Debrid submit should hit configured capacity");
        assert!(
            err.to_string()
                .contains("Debrid route capacity reached: active Debrid jobs 2/2")
        );
        Ok(())
    }

    fn test_debrid_release(
        release_kind: ReleaseKind,
        confidence: ReleaseConfidence,
    ) -> AcquisitionRelease {
        let now = Utc::now();
        AcquisitionRelease {
            release_id: Uuid::new_v4(),
            subscription_id: Some(Uuid::new_v4()),
            source_provider_id: Some(Uuid::new_v4()),
            source_extension_id: "test.source".to_string(),
            owner_id: "test.source".to_string(),
            media_type: MediaType::Series,
            title: "Show".to_string(),
            release_title: "Show.S01.1080p.WEB-DL".to_string(),
            source: "magnet:?xt=urn:btih:0123456789abcdef".to_string(),
            source_kind: "magnet".to_string(),
            info_hash: Some("0123456789abcdef".to_string()),
            fingerprint: "test-fingerprint".to_string(),
            release_kind,
            resolver_kind: ReleaseResolverKind::TvSonarrStyle,
            resolver_version: "test".to_string(),
            confidence,
            score: Some(99.0),
            selected_route_logical_id: Some(DEBRID_DEFAULT_LOGICAL_ID.to_string()),
            selected_provider_id: Some(Uuid::new_v4()),
            download_id: None,
            remote_release_id: Some("fake-release-1".to_string()),
            state: AcquisitionReleaseState::Staging,
            state_reason: None,
            selected_candidate: None,
            coverage_plan: None,
            created_at: now,
            updated_at: now,
        }
    }

    async fn insert_test_release(
        pool: &sqlx::AnyPool,
        release: &AcquisitionRelease,
    ) -> Result<AcquisitionRelease> {
        upsert_release(
            pool,
            NewAcquisitionRelease {
                release_id: Some(release.release_id),
                subscription_id: release.subscription_id,
                source_provider_id: release.source_provider_id,
                source_extension_id: release.source_extension_id.clone(),
                owner_id: release.owner_id.clone(),
                media_type: release.media_type,
                title: release.title.clone(),
                release_title: release.release_title.clone(),
                source: release.source.clone(),
                source_kind: release.source_kind.clone(),
                info_hash: release.info_hash.clone(),
                fingerprint: release.fingerprint.clone(),
                release_kind: release.release_kind,
                resolver_kind: release.resolver_kind,
                resolver_version: release.resolver_version.clone(),
                confidence: release.confidence,
                score: release.score,
                selected_route_logical_id: release.selected_route_logical_id.clone(),
                selected_provider_id: release.selected_provider_id,
                download_id: release.download_id.clone(),
                remote_release_id: release.remote_release_id.clone(),
                state: release.state,
                state_reason: release.state_reason.clone(),
                selected_candidate: release.selected_candidate.clone(),
                coverage_plan: release.coverage_plan.clone(),
            },
        )
        .await
    }

    fn test_release_file(
        release_id: Uuid,
        provider_file_id: &str,
        path: &str,
        selectable: bool,
    ) -> AcquisitionReleaseFile {
        let now = Utc::now();
        AcquisitionReleaseFile {
            release_file_id: Uuid::new_v4(),
            release_id,
            file_index: None,
            file_id: Some(provider_file_id.to_string()),
            provider_file_id: Some(provider_file_id.to_string()),
            path: path.to_string(),
            basename: path.rsplit('/').next().unwrap_or(path).to_string(),
            size_bytes: Some(1024),
            selectable,
            selected: None,
            parsed_title: Some("Show".to_string()),
            parsed_season_number: Some(1),
            parsed_episode_number: None,
            parsed_episode_end_number: None,
            parsed_absolute_episode_number: None,
            parsed_absolute_episode_end_number: None,
            parsed_air_date: None,
            parsed_quality: Some("WEB-DL-1080p".to_string()),
            parsed_language: None,
            parsed_release_group: None,
            parser_confidence: ReleaseConfidence::High,
            parser_reason: None,
            raw: None,
            provider_metadata: None,
            created_at: now,
            updated_at: now,
        }
    }

    async fn insert_test_release_file(
        pool: &sqlx::AnyPool,
        file: &AcquisitionReleaseFile,
    ) -> Result<AcquisitionReleaseFile> {
        upsert_release_file(
            pool,
            NewAcquisitionReleaseFile {
                release_file_id: Some(file.release_file_id),
                release_id: file.release_id,
                file_index: file.file_index,
                file_id: file.file_id.clone(),
                provider_file_id: file.provider_file_id.clone(),
                path: file.path.clone(),
                basename: Some(file.basename.clone()),
                size_bytes: file.size_bytes,
                selectable: file.selectable,
                selected: file.selected,
                parsed_title: file.parsed_title.clone(),
                parsed_season_number: file.parsed_season_number,
                parsed_episode_number: file.parsed_episode_number,
                parsed_episode_end_number: file.parsed_episode_end_number,
                parsed_absolute_episode_number: file.parsed_absolute_episode_number,
                parsed_absolute_episode_end_number: file.parsed_absolute_episode_end_number,
                parsed_air_date: file.parsed_air_date.clone(),
                parsed_quality: file.parsed_quality.clone(),
                parsed_language: file.parsed_language.clone(),
                parsed_release_group: file.parsed_release_group.clone(),
                parser_confidence: file.parser_confidence,
                parser_reason: file.parser_reason.clone(),
                raw: file.raw.clone(),
                provider_metadata: file.provider_metadata.clone(),
            },
        )
        .await
    }

    fn test_coverage(release_id: Uuid, release_file_id: Uuid) -> AcquisitionReleaseCoverage {
        let now = Utc::now();
        AcquisitionReleaseCoverage {
            coverage_id: Uuid::new_v4(),
            release_id,
            release_file_id: Some(release_file_id),
            target_id: Uuid::new_v4(),
            coverage_kind: ReleaseCoverageKind::SingleEpisode,
            confidence: ReleaseConfidence::High,
            score: Some(100.0),
            reason: Some("test".to_string()),
            state: ReleaseCoverageState::Planned,
            verified_by: Some("test".to_string()),
            created_at: now,
            updated_at: now,
        }
    }

    #[test]
    fn debrid_runtime_coverage_update_does_not_resurrect_rejected_or_placeholder_rows() {
        let release_id = Uuid::new_v4();
        let release_file_id = Uuid::new_v4();

        let concrete = test_coverage(release_id, release_file_id);
        assert!(should_update_debrid_runtime_coverage(
            &concrete,
            ReleaseCoverageState::Submitted
        ));

        let mut rejected = test_coverage(release_id, release_file_id);
        rejected.state = ReleaseCoverageState::Rejected;
        assert!(!should_update_debrid_runtime_coverage(
            &rejected,
            ReleaseCoverageState::Submitted
        ));

        let mut placeholder = test_coverage(release_id, release_file_id);
        placeholder.release_file_id = None;
        placeholder.state = ReleaseCoverageState::ReviewRequired;
        assert!(!should_update_debrid_runtime_coverage(
            &placeholder,
            ReleaseCoverageState::Submitted
        ));
        assert!(should_update_debrid_runtime_coverage(
            &placeholder,
            ReleaseCoverageState::ReviewRequired
        ));
    }

    fn test_debrid_inspection(
        supports_file_selection: bool,
        files: Vec<DebridRemoteFile>,
        links: Vec<DebridResolvedLink>,
        selection: Option<DebridFileSelection>,
    ) -> DebridReleaseInspection {
        DebridReleaseInspection {
            release: DebridRemoteRelease {
                provider_implementation: "test_debrid".to_string(),
                remote_release_id: "fake-release-1".to_string(),
                display_name: Some("Show.S01.1080p.WEB-DL".to_string()),
                status: DebridReleaseStatus::WaitingFiles,
                raw_status: Some("waiting_files".to_string()),
                raw: None,
            },
            capabilities: DebridProviderCapabilities {
                supports_magnet_submit: true,
                supports_hoster_unrestrict: true,
                supports_file_listing: true,
                supports_file_selection,
                supports_cache_check: true,
                supports_delete: true,
                supports_progress: true,
                file_selection_mode: if supports_file_selection {
                    DebridFileSelectionMode::BeforeTransfer
                } else {
                    DebridFileSelectionMode::Unsupported
                },
            },
            files,
            links,
            progress: None,
            selection,
            raw: None,
        }
    }

    #[test]
    fn debrid_selection_policy_approves_exact_pack_file_ids() {
        let release = test_debrid_release(ReleaseKind::SeasonPack, ReleaseConfidence::High);
        let first = test_release_file(
            release.release_id,
            "file-1",
            "Show/Season 01/Show.S01E01.mkv",
            true,
        );
        let second = test_release_file(
            release.release_id,
            "file-2",
            "Show/Season 01/Show.S01E02.mkv",
            true,
        );
        let files = vec![first.clone(), second.clone()];
        let coverage = vec![
            test_coverage(release.release_id, first.release_file_id),
            test_coverage(release.release_id, second.release_file_id),
        ];
        let inspection = test_debrid_inspection(true, Vec::new(), Vec::new(), None);

        let decision = decide_debrid_file_selection(&release, &files, &coverage, &inspection);

        assert_eq!(decision.status, DebridSelectionDecisionStatus::Approved);
        assert_eq!(
            decision.provider_selection_ids,
            vec!["file-1".to_string(), "file-2".to_string()]
        );
        assert!(decision.skipped_file_ids.is_empty());
        assert!(!decision.select_all);
        assert!(decision.select_all_approved);
    }

    #[test]
    fn download_broker_debrid_movie_selection_uses_movie_coverage_main_file() {
        let mut release = test_debrid_release(ReleaseKind::Single, ReleaseConfidence::High);
        release.media_type = MediaType::Movie;
        release.resolver_kind = ReleaseResolverKind::MovieRadarrStyle;
        let main = test_release_file(
            release.release_id,
            "file-main",
            "Movie.2026.1080p.BluRay/Movie.2026.1080p.BluRay-GROUP.mkv",
            true,
        );
        let extra = test_release_file(
            release.release_id,
            "file-extra",
            "Movie.2026.1080p.BluRay/Movie.2026.Commentary.Track.mkv",
            true,
        );
        let files = vec![main.clone(), extra];
        let mut coverage = test_coverage(release.release_id, main.release_file_id);
        coverage.coverage_kind = ReleaseCoverageKind::Movie;
        let inspection = test_debrid_inspection(true, Vec::new(), Vec::new(), None);

        let decision = decide_debrid_file_selection(&release, &files, &[coverage], &inspection);

        assert_eq!(decision.status, DebridSelectionDecisionStatus::Approved);
        assert_eq!(
            decision.provider_selection_ids,
            vec!["file-main".to_string()]
        );
        assert_eq!(decision.skipped_file_ids, vec!["file-extra".to_string()]);
        assert!(decision.review_reasons.is_empty());
    }

    #[test]
    fn debrid_selection_policy_maps_post_transfer_pack_file_to_single_episode_target() {
        let mut release = test_debrid_release(ReleaseKind::Single, ReleaseConfidence::High);
        let target_id = Uuid::new_v4();
        release.coverage_plan = Some(json!({
            "source": "debrid_provider_file_list",
            "remoteReleaseId": "remote-pack-1",
            "tv": {
                "entries": [{
                    "targetId": target_id.to_string(),
                    "targetKey": "S01E02",
                    "coverageKind": "single_episode",
                    "seasonNumber": 1,
                    "episodeNumber": 2,
                    "releaseFileId": null,
                    "state": "planned"
                }],
                "releaseKind": "single",
                "confidence": "high"
            },
            "debridProvider": {
                "providerImplementation": "torbox",
                "remoteStatus": "downloaded"
            }
        }));
        let mut first = test_release_file(
            release.release_id,
            "file-1",
            "Show/Show.S01E01.1080p.WEB-DL.mkv",
            true,
        );
        first.parsed_episode_number = Some(1);
        first.parsed_episode_end_number = Some(1);
        let mut second = test_release_file(
            release.release_id,
            "file-2",
            "Show/Show.S01E02.1080p.WEB-DL.mkv",
            true,
        );
        second.parsed_episode_number = Some(2);
        second.parsed_episode_end_number = Some(2);
        let mut third = test_release_file(
            release.release_id,
            "file-3",
            "Show/Show.S01E03.1080p.WEB-DL.mkv",
            true,
        );
        third.parsed_episode_number = Some(3);
        third.parsed_episode_end_number = Some(3);
        let files = vec![first, second, third];
        let now = Utc::now();
        let coverage = vec![AcquisitionReleaseCoverage {
            coverage_id: Uuid::new_v4(),
            release_id: release.release_id,
            release_file_id: None,
            target_id,
            coverage_kind: ReleaseCoverageKind::SingleEpisode,
            confidence: ReleaseConfidence::High,
            score: Some(100.0),
            reason: Some("TV Sonarr-style S01E02".to_string()),
            state: ReleaseCoverageState::Submitted,
            verified_by: Some("test".to_string()),
            created_at: now,
            updated_at: now,
        }];
        let inspection = test_debrid_inspection(true, Vec::new(), Vec::new(), None);

        let decision = decide_debrid_file_selection(&release, &files, &coverage, &inspection);

        assert_eq!(decision.status, DebridSelectionDecisionStatus::Approved);
        assert_eq!(decision.provider_selection_ids, vec!["file-2".to_string()]);
        assert_eq!(
            decision.skipped_file_ids,
            vec!["file-1".to_string(), "file-3".to_string()]
        );
        assert!(decision.review_reasons.is_empty());
    }

    #[test]
    fn debrid_selection_policy_maps_anime_target_key_scope_to_single_pack_file() {
        let mut release = test_debrid_release(ReleaseKind::Single, ReleaseConfidence::High);
        release.media_type = MediaType::Anime;
        release.resolver_kind = ReleaseResolverKind::AnimeShokoStyle;
        release.release_title =
            "Fullmetal Alchemist - Brotherhood - S01E01 - Fullmetal Alchemist Bluray-1080p Remux.mkv"
                .to_string();
        let target_id = Uuid::new_v4();
        release.coverage_plan = Some(json!({
            "resolverKind": "anime_shoko_style",
            "releaseKind": "single",
            "confidence": "high",
            "requiresFileList": false,
            "requiresFileSelection": false,
            "entries": [{
                "targetKey": "S01E01",
                "canonicalKey": null,
                "releaseFileKey": null,
                "fileId": null,
                "fileIndex": null,
                "path": null,
                "coverageKind": "single_episode",
                "confidence": "high",
                "reason": "sonarr_season_episode",
                "state": "planned"
            }],
            "requestScopeEvidence": {
                "requestMode": "one_shot",
                "requestScope": "range",
                "targetCount": 1,
                "targetIds": [target_id.to_string()],
                "targetKeys": ["S01E01"],
                "targets": {
                    "S01E01": {
                        "seasonNumber": 1,
                        "episodeNumber": 1,
                        "absoluteEpisodeNumber": 1,
                        "title": "Fullmetal Alchemist"
                    }
                }
            }
        }));

        let mut brotherhood = test_release_file(
            release.release_id,
            "42",
            "/completed/hash/Full Metal Alchemist Brotherhood (1-64)/Fullmetal Alchemist - Brotherhood - S01E01 - Fullmetal Alchemist.mkv",
            true,
        );
        brotherhood.parsed_title = Some("Fullmetal Alchemist - Brotherhood".to_string());
        brotherhood.parsed_episode_number = Some(1);
        brotherhood.parsed_episode_end_number = Some(1);
        brotherhood.parsed_absolute_episode_number = None;
        brotherhood.parsed_absolute_episode_end_number = None;

        let mut original_series_same_episode = test_release_file(
            release.release_id,
            "99",
            "/completed/hash/Full Metal Alchemist (2003)/Fullmetal Alchemist - S01E01.mkv",
            true,
        );
        original_series_same_episode.parsed_title = Some("Fullmetal Alchemist".to_string());
        original_series_same_episode.parsed_episode_number = Some(1);
        original_series_same_episode.parsed_episode_end_number = Some(1);
        original_series_same_episode.parsed_absolute_episode_number = None;
        original_series_same_episode.parsed_absolute_episode_end_number = None;

        let mut next_episode = test_release_file(
            release.release_id,
            "31",
            "/completed/hash/Full Metal Alchemist Brotherhood (1-64)/Fullmetal Alchemist - Brotherhood - S01E02 - The First Day.mkv",
            true,
        );
        next_episode.parsed_title = Some("Fullmetal Alchemist - Brotherhood".to_string());
        next_episode.parsed_episode_number = Some(2);
        next_episode.parsed_episode_end_number = Some(2);
        next_episode.parsed_absolute_episode_number = None;
        next_episode.parsed_absolute_episode_end_number = None;

        let now = Utc::now();
        let coverage = vec![AcquisitionReleaseCoverage {
            coverage_id: Uuid::new_v4(),
            release_id: release.release_id,
            release_file_id: None,
            target_id,
            coverage_kind: ReleaseCoverageKind::SingleEpisode,
            confidence: ReleaseConfidence::High,
            score: Some(112.0),
            reason: Some("sonarr_season_episode".to_string()),
            state: ReleaseCoverageState::Submitted,
            verified_by: Some("rr3f_file_list".to_string()),
            created_at: now,
            updated_at: now,
        }];
        let inspection = test_debrid_inspection(true, Vec::new(), Vec::new(), None);

        let decision = decide_debrid_file_selection(
            &release,
            &[brotherhood, original_series_same_episode, next_episode],
            &coverage,
            &inspection,
        );

        assert_eq!(decision.status, DebridSelectionDecisionStatus::Approved);
        assert_eq!(decision.provider_selection_ids, vec!["42".to_string()]);
        assert_eq!(
            decision.skipped_file_ids,
            vec!["31".to_string(), "99".to_string()]
        );
        assert_eq!(
            decision.target_file_selections,
            vec![DebridTargetFileSelection {
                target_id,
                provider_file_id: "42".to_string(),
            }]
        );
        assert!(decision.review_reasons.is_empty());
    }

    #[tokio::test]
    async fn debrid_selection_persistence_attaches_fallback_file_to_coverage() -> Result<()> {
        let database = setup_db().await?;
        let (provider_id, instance_id) = create_provider_refs(&database.pool).await?;
        let subscription_id = create_anime_subscription_with_target(
            &database.pool,
            "Fullmetal Alchemist Brotherhood",
            "Fullmetal Alchemist Brotherhood",
            "S01E01",
            1,
            1,
            1,
        )
        .await?;
        let target_id = list_subscription_targets(&database.pool, subscription_id)
            .await?
            .into_iter()
            .next()
            .context("loading test S01E01 anime target")?
            .target_id;
        let source = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&dn=Fullmetal.Alchemist.Brotherhood.S01E01";
        let adapter = FakeDebridAdapter::new();
        let job_id = submit_debrid_with_adapter(
            &database.pool,
            provider_id,
            instance_id,
            source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("anime"),
                name: Some("Fullmetal Alchemist Brotherhood S01E01"),
                paused: false,
                release_context: Some(DebridReleaseSubmitContext {
                    subscription_id: Some(subscription_id),
                    source_provider_id: Some(provider_id),
                    source_extension_id: "test.source".to_string(),
                    media_type: MediaType::Anime,
                    title: "Fullmetal Alchemist Brotherhood".to_string(),
                    release_title: "Fullmetal Alchemist - Brotherhood - S01E01 - Fullmetal Alchemist Bluray-1080p Remux.mkv".to_string(),
                    info_hash: Some(
                        "0123456789abcdef0123456789abcdef01234567".to_string(),
                    ),
                    fingerprint: Some(format!(
                        "debrid-fallback-file-persistence-{}",
                        Uuid::new_v4()
                    )),
                    score: Some(112.0),
                    selected_candidate: Some(json!({
                        "submissionBookkeeping": { "status": "pending" }
                    })),
                }),
            },
            &adapter,
        )
        .await?;
        let job = load_debrid_job(&database.pool, job_id)
            .await?
            .context("owned anime Debrid job")?;
        let release = get_release(
            &database.pool,
            job.release_id.context("owned anime Debrid release id")?,
        )
        .await?
        .context("owned anime Debrid release")?;
        let mut file = test_release_file(
            release.release_id,
            "42",
            "/completed/hash/Full Metal Alchemist Brotherhood (1-64)/Fullmetal Alchemist - Brotherhood - S01E01 - Fullmetal Alchemist.mkv",
            true,
        );
        file.parsed_title = Some("Fullmetal Alchemist - Brotherhood".to_string());
        file.parsed_episode_number = Some(1);
        file.parsed_episode_end_number = Some(1);
        let file = insert_test_release_file(&database.pool, &file).await?;
        let coverage = upsert_release_coverage(
            &database.pool,
            NewAcquisitionReleaseCoverage {
                coverage_id: None,
                release_id: release.release_id,
                release_file_id: None,
                target_id,
                coverage_kind: ReleaseCoverageKind::SingleEpisode,
                confidence: ReleaseConfidence::High,
                score: Some(112.0),
                reason: Some("sonarr_season_episode".to_string()),
                state: ReleaseCoverageState::Submitted,
                verified_by: Some("rr3f_file_list".to_string()),
            },
        )
        .await?;
        let decision = DebridFileSelectionDecision {
            status: DebridSelectionDecisionStatus::Approved,
            selected_file_ids: vec!["42".to_string()],
            skipped_file_ids: Vec::new(),
            provider_selection_ids: vec!["42".to_string()],
            target_file_selections: vec![DebridTargetFileSelection {
                target_id,
                provider_file_id: "42".to_string(),
            }],
            review_reasons: Vec::new(),
            policy_version: DEBRID_SELECTION_POLICY_VERSION.to_string(),
            coverage_fingerprint: "sha256:test".to_string(),
            select_all: false,
            select_all_approved: true,
        };

        assert!(
            persist_debrid_selection_decision(
                &database.pool,
                job_id,
                &release,
                std::slice::from_ref(&file),
                std::slice::from_ref(&coverage),
                &decision,
            )
            .await?,
            "the exact active anime Debrid attempt must own the atomic selection commit"
        );

        let updated = list_release_coverage(&database.pool, release.release_id).await?;
        assert_eq!(updated.len(), 1);
        assert_eq!(updated[0].target_id, target_id);
        assert_eq!(updated[0].release_file_id, Some(file.release_file_id));
        assert_eq!(updated[0].state, ReleaseCoverageState::Selected);
        let files = list_release_files(&database.pool, release.release_id).await?;
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].selected, Some(true));
        Ok(())
    }

    #[test]
    fn debrid_selection_policy_reviews_unsupported_file_selection() {
        let release = test_debrid_release(ReleaseKind::SeasonPack, ReleaseConfidence::High);
        let file = test_release_file(
            release.release_id,
            "file-1",
            "Show/Season 01/Show.S01E01.mkv",
            true,
        );
        let coverage = vec![test_coverage(release.release_id, file.release_file_id)];
        let inspection = test_debrid_inspection(false, Vec::new(), Vec::new(), None);

        let all_media_decision = decide_debrid_file_selection(
            &release,
            std::slice::from_ref(&file),
            &coverage,
            &inspection,
        );
        assert_eq!(
            all_media_decision.status,
            DebridSelectionDecisionStatus::Approved,
            "an exact all-media mapping needs no provider selection operation"
        );

        let unselected = test_release_file(
            release.release_id,
            "file-2",
            "Show/Season 01/Show.S01E02.mkv",
            true,
        );
        let files = vec![file, unselected];
        let decision = decide_debrid_file_selection(&release, &files, &coverage, &inspection);

        assert_eq!(
            decision.status,
            DebridSelectionDecisionStatus::ReviewRequired
        );
        assert!(
            decision
                .review_reasons
                .iter()
                .any(|reason| reason == "file_selection_unsupported")
        );
    }

    #[test]
    fn debrid_selection_policy_reviews_pack_without_file_list() {
        let release = test_debrid_release(ReleaseKind::SeasonPack, ReleaseConfidence::High);
        let inspection = test_debrid_inspection(true, Vec::new(), Vec::new(), None);

        let decision = decide_debrid_file_selection(&release, &[], &[], &inspection);

        assert_eq!(
            decision.status,
            DebridSelectionDecisionStatus::ReviewRequired
        );
        assert!(
            decision
                .review_reasons
                .iter()
                .any(|reason| reason == "missing_file_list")
        );
    }

    #[test]
    fn debrid_selection_policy_reviews_pack_with_uncovered_media() {
        let release = test_debrid_release(ReleaseKind::SeasonPack, ReleaseConfidence::High);
        let covered = test_release_file(
            release.release_id,
            "file-1",
            "Show/Season 01/Show.S01E01.mkv",
            true,
        );
        let uncovered = test_release_file(
            release.release_id,
            "file-2",
            "Show/Season 01/Show.S01E02.mkv",
            true,
        );
        let files = vec![covered.clone(), uncovered];
        let coverage = vec![test_coverage(release.release_id, covered.release_file_id)];
        let inspection = test_debrid_inspection(true, Vec::new(), Vec::new(), None);

        let decision = decide_debrid_file_selection(&release, &files, &coverage, &inspection);

        assert_eq!(
            decision.status,
            DebridSelectionDecisionStatus::ReviewRequired
        );
        assert_eq!(decision.provider_selection_ids, vec!["file-1".to_string()]);
        assert!(
            decision
                .review_reasons
                .iter()
                .any(|reason| reason == "file_list_does_not_cover_all_selectable_media")
        );
    }

    #[test]
    fn osr4_debrid_selection_skips_out_of_scope_files_for_one_shot_pack() {
        let mut release = test_debrid_release(ReleaseKind::SeasonPack, ReleaseConfidence::High);
        release.coverage_plan = Some(json!({
            "resolverKind": "tv_sonarr_style",
            "releaseKind": "season_pack",
            "confidence": "high",
            "entries": [{
                "targetId": Uuid::new_v4().to_string(),
                "targetKey": "S01E01",
                "seasonNumber": 1,
                "episodeNumber": 1,
                "releaseFileId": "file-1",
                "coverageKind": "season_pack",
                "state": "submitted"
            }],
            "requestScopeEvidence": {
                "requestMode": "one_shot",
                "requestScope": "episode",
                "targetCount": 1,
                "targetKeys": ["S01E01"]
            }
        }));
        let covered = test_release_file(
            release.release_id,
            "file-1",
            "Show/Season 01/Show.S01E01.mkv",
            true,
        );
        let out_of_scope = test_release_file(
            release.release_id,
            "file-2",
            "Show/Season 01/Show.S01E02.mkv",
            true,
        );
        let files = vec![covered.clone(), out_of_scope];
        let coverage = vec![test_coverage(release.release_id, covered.release_file_id)];
        let inspection = test_debrid_inspection(true, Vec::new(), Vec::new(), None);

        let decision = decide_debrid_file_selection(&release, &files, &coverage, &inspection);

        assert_eq!(decision.status, DebridSelectionDecisionStatus::Approved);
        assert_eq!(decision.provider_selection_ids, vec!["file-1".to_string()]);
        assert_eq!(decision.skipped_file_ids, vec!["file-2".to_string()]);
        assert!(decision.review_reasons.is_empty());
    }

    #[test]
    fn debrid_selection_policy_honors_user_approved_file_override() {
        let mut release =
            test_debrid_release(ReleaseKind::SeasonPack, ReleaseConfidence::ReviewRequired);
        release.coverage_plan = Some(json!({
            "manualReview": {
                "status": "approved",
                "userApproved": true,
                "selectedFileIds": ["file-1"],
                "skippedFileIds": ["file-2"],
                "coverageFingerprint": "sha256:user-approved-debrid"
            }
        }));
        let selected = test_release_file(
            release.release_id,
            "file-1",
            "Show/Season 01/Show.S01E01.mkv",
            true,
        );
        let skipped = test_release_file(
            release.release_id,
            "file-2",
            "Show/Season 01/Show.S01E02.mkv",
            true,
        );
        let files = vec![selected.clone(), skipped];
        let coverage = vec![test_coverage(release.release_id, selected.release_file_id)];
        let inspection = test_debrid_inspection(true, Vec::new(), Vec::new(), None);

        let decision = decide_debrid_file_selection(&release, &files, &coverage, &inspection);

        assert_eq!(decision.status, DebridSelectionDecisionStatus::Approved);
        assert_eq!(decision.provider_selection_ids, vec!["file-1".to_string()]);
        assert_eq!(decision.skipped_file_ids, vec!["file-2".to_string()]);
        assert_eq!(decision.coverage_fingerprint, "sha256:user-approved-debrid");
        assert!(decision.review_reasons.is_empty());
    }

    #[test]
    fn alm7_debrid_selection_policy_ignores_legacy_anime_user_override() {
        let mut release =
            test_debrid_release(ReleaseKind::SeasonPack, ReleaseConfidence::ReviewRequired);
        release.media_type = MediaType::Anime;
        release.resolver_kind = ReleaseResolverKind::AnimeShokoStyle;
        release.release_title =
            "Fullmetal Alchemist - Brotherhood - S01E01 - Fullmetal Alchemist Bluray-1080p Remux.mkv"
                .to_string();
        let target_id = Uuid::new_v4();
        release.coverage_plan = Some(json!({
            "manualReview": {
                "status": "approved",
                "userApproved": true,
                "selectedFileIds": ["42"],
                "skippedFileIds": ["31", "99"],
                "coverageFingerprint": "sha256:user-approved-fmab"
            },
            "animeCoveragePlan": {
                "resolverKind": "anime_shoko_style",
                "releaseKind": "single",
                "confidence": "high",
                "entries": [{
                    "targetKey": "S01E01",
                    "coverageKind": "single_episode",
                    "confidence": "high",
                    "reason": "sonarr_season_episode",
                    "state": "planned"
                }],
                "requestScopeEvidence": {
                    "requestMode": "one_shot",
                    "requestScope": "range",
                    "targetCount": 1,
                    "targetIds": [target_id.to_string()],
                    "targetKeys": ["S01E01"],
                    "targets": {
                        "S01E01": {
                            "seasonNumber": 1,
                            "episodeNumber": 1,
                            "absoluteEpisodeNumber": 1,
                            "title": "Fullmetal Alchemist"
                        }
                    }
                }
            }
        }));

        let mut brotherhood = test_release_file(
            release.release_id,
            "42",
            "/completed/hash/Full Metal Alchemist Brotherhood (1-64)/Fullmetal Alchemist - Brotherhood - S01E01 - Fullmetal Alchemist.mkv",
            true,
        );
        brotherhood.parsed_title = Some("Fullmetal Alchemist - Brotherhood".to_string());
        brotherhood.parsed_episode_number = Some(1);
        brotherhood.parsed_episode_end_number = Some(1);
        let mut original_series_same_episode = test_release_file(
            release.release_id,
            "99",
            "/completed/hash/Full Metal Alchemist (2003)/Fullmetal Alchemist - S01E01.mkv",
            true,
        );
        original_series_same_episode.parsed_title = Some("Fullmetal Alchemist".to_string());
        original_series_same_episode.parsed_episode_number = Some(1);
        original_series_same_episode.parsed_episode_end_number = Some(1);
        let mut next_episode = test_release_file(
            release.release_id,
            "31",
            "/completed/hash/Full Metal Alchemist Brotherhood (1-64)/Fullmetal Alchemist - Brotherhood - S01E02 - The First Day.mkv",
            true,
        );
        next_episode.parsed_title = Some("Fullmetal Alchemist - Brotherhood".to_string());
        next_episode.parsed_episode_number = Some(2);
        next_episode.parsed_episode_end_number = Some(2);
        let coverage = vec![AcquisitionReleaseCoverage {
            coverage_id: Uuid::new_v4(),
            release_id: release.release_id,
            release_file_id: None,
            target_id,
            coverage_kind: ReleaseCoverageKind::SingleEpisode,
            confidence: ReleaseConfidence::High,
            score: Some(112.0),
            reason: Some("sonarr_season_episode".to_string()),
            state: ReleaseCoverageState::Submitted,
            verified_by: Some("rr3f_file_list".to_string()),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }];
        let files = vec![brotherhood, original_series_same_episode, next_episode];
        let inspection = test_debrid_inspection(true, Vec::new(), Vec::new(), None);

        let decision = decide_debrid_file_selection(&release, &files, &coverage, &inspection);

        assert_eq!(
            decision.status,
            DebridSelectionDecisionStatus::ReviewRequired
        );
        assert!(decision.provider_selection_ids.is_empty());
        assert!(decision.target_file_selections.is_empty());
        assert!(
            decision
                .review_reasons
                .contains(&"coverage_not_high_confidence".to_string())
        );
        assert_ne!(decision.coverage_fingerprint, "sha256:user-approved-fmab");
    }

    #[test]
    fn debrid_selection_policy_translates_synthetic_review_file_to_provider_file() {
        let mut release =
            test_debrid_release(ReleaseKind::SeasonPack, ReleaseConfidence::ReviewRequired);
        release.coverage_plan = Some(json!({
            "manualReview": {
                "status": "approved",
                "userApproved": true,
                "selectedFileIds": [SYNTHETIC_SOURCE_CANDIDATE_FILE_ID],
                "skippedFileIds": ["6"],
                "coverageFingerprint": "sha256:user-approved-synthetic"
            }
        }));
        let mut synthetic = test_release_file(
            release.release_id,
            SYNTHETIC_SOURCE_CANDIDATE_FILE_ID,
            "Star Wars Clone Wars [2003] Volume 01.mkv",
            true,
        );
        synthetic.raw = Some(json!({
            "source": "manual_review_source_candidate",
            "synthetic": true
        }));
        let provider = test_release_file(
            release.release_id,
            "6",
            "/completed/hash/Star Wars Clone Wars [2003] Volume 01.mkv",
            true,
        );
        let files = vec![synthetic, provider];
        let inspection = test_debrid_inspection(true, Vec::new(), Vec::new(), None);

        let decision = decide_debrid_file_selection(&release, &files, &[], &inspection);

        assert_eq!(decision.status, DebridSelectionDecisionStatus::Approved);
        assert_eq!(decision.selected_file_ids, vec!["6".to_string()]);
        assert_eq!(decision.provider_selection_ids, vec!["6".to_string()]);
        assert!(decision.skipped_file_ids.is_empty());
        assert_eq!(
            decision.coverage_fingerprint,
            "sha256:user-approved-synthetic"
        );
        assert!(decision.review_reasons.is_empty());
    }

    #[test]
    fn debrid_selection_policy_prefers_reviewed_release_file_alias_over_numeric_provider_collision()
    {
        let mut release =
            test_debrid_release(ReleaseKind::SeasonPack, ReleaseConfidence::ReviewRequired);
        let mut synthetic = test_release_file(
            release.release_id,
            "source-preview",
            "[Erai-raws] Fullmetal Alchemist - Brotherhood - 01 [1080p NF WEB-DL AVC AAC][MultiSub][C99DA55C].mkv",
            true,
        );
        synthetic.file_index = Some(0);
        synthetic.file_id = None;
        synthetic.provider_file_id = None;
        synthetic.raw = Some(json!({
            "source": "manual_review_source_candidate",
            "synthetic": true
        }));
        let provider_episode_one = test_release_file(
            release.release_id,
            "62",
            "/completed/hash/[Erai-raws] Fullmetal Alchemist - Brotherhood - 01 [1080p NF WEB-DL AVC AAC][MultiSub][C99DA55C].mkv",
            true,
        );
        let provider_id_zero_is_episode_thirty_six = test_release_file(
            release.release_id,
            "0",
            "/completed/hash/[Erai-raws] Fullmetal Alchemist - Brotherhood - 36 [1080p NF WEB-DL AVC AAC][MultiSub][37B0B992].mkv",
            true,
        );
        release.coverage_plan = Some(json!({
            "manualReview": {
                "status": "approved",
                "userApproved": true,
                "selectedReleaseFileIds": [synthetic.release_file_id.to_string()],
                "selectedFileIds": ["0"],
                "skippedFileIds": [],
                "coverageFingerprint": "sha256:user-approved-fma"
            }
        }));
        let files = vec![
            synthetic,
            provider_episode_one,
            provider_id_zero_is_episode_thirty_six,
        ];
        let inspection = test_debrid_inspection(true, Vec::new(), Vec::new(), None);

        let decision = decide_debrid_file_selection(&release, &files, &[], &inspection);

        assert_eq!(decision.status, DebridSelectionDecisionStatus::Approved);
        assert_eq!(decision.selected_file_ids, vec!["62".to_string()]);
        assert_eq!(decision.provider_selection_ids, vec!["62".to_string()]);
        assert!(!decision.selected_file_ids.iter().any(|id| id == "0"));
        assert!(decision.review_reasons.is_empty());
    }

    #[test]
    fn debrid_synthetic_review_alias_maps_to_unique_provider_release_file() {
        let release =
            test_debrid_release(ReleaseKind::SeasonPack, ReleaseConfidence::ReviewRequired);
        let mut synthetic = test_release_file(
            release.release_id,
            SYNTHETIC_SOURCE_CANDIDATE_FILE_ID,
            "Star Wars Clone Wars [2003] Volume 01.mkv",
            true,
        );
        synthetic.raw = Some(json!({
            "source": "manual_review_source_candidate",
            "synthetic": true
        }));
        let provider = test_release_file(
            release.release_id,
            "6",
            "/completed/hash/Star Wars Clone Wars [2003] Volume 01.mkv",
            true,
        );

        let aliases =
            synthetic_source_candidate_release_file_aliases(&[synthetic.clone(), provider.clone()]);

        assert_eq!(
            aliases.get(&synthetic.release_file_id),
            Some(&provider.release_file_id)
        );
    }

    #[test]
    fn debrid_coverage_plan_merge_preserves_manual_review_evidence() {
        let merged = merge_debrid_coverage_plans(
            Some(json!({
                "manualReview": {
                    "status": "approved",
                    "userApproved": true,
                    "selectedFileIds": [SYNTHETIC_SOURCE_CANDIDATE_FILE_ID]
                },
                "priorityPolicy": {
                    "status": "approved",
                    "userApproved": true
                }
            })),
            Some(json!({
                "tv": {
                    "confidence": "review_required",
                    "rejectionReasons": ["unknown_numbering"]
                }
            })),
        )
        .expect("merged plan");

        assert_eq!(
            merged
                .pointer("/manualReview/status")
                .and_then(Value::as_str),
            Some("approved")
        );
        assert_eq!(
            merged.pointer("/tv/confidence").and_then(Value::as_str),
            Some("review_required")
        );
    }

    #[test]
    fn debrid_selection_policy_allows_safe_single_without_file_list() {
        let release = test_debrid_release(ReleaseKind::Single, ReleaseConfidence::High);
        let inspection = test_debrid_inspection(true, Vec::new(), Vec::new(), None);

        let decision = decide_debrid_file_selection(&release, &[], &[], &inspection);

        assert_eq!(decision.status, DebridSelectionDecisionStatus::Approved);
        assert_eq!(decision.provider_selection_ids, vec!["all".to_string()]);
        assert!(decision.select_all);
        assert!(decision.select_all_approved);
    }

    #[test]
    fn debrid_selected_links_ignore_skipped_provider_files() {
        let inspection = test_debrid_inspection(
            true,
            Vec::new(),
            vec![
                DebridResolvedLink {
                    provider_file_id: Some("file-1".to_string()),
                    url: "https://debrid.test/file-1".to_string(),
                    filename: Some("Show.S01E01.mkv".to_string()),
                    size_bytes: Some(1024),
                    raw: None,
                },
                DebridResolvedLink {
                    provider_file_id: Some("file-2".to_string()),
                    url: "https://debrid.test/file-2".to_string(),
                    filename: Some("Show.S01E02.mkv".to_string()),
                    size_bytes: Some(1024),
                    raw: None,
                },
            ],
            Some(DebridFileSelection {
                mode: DebridFileSelectionMode::BeforeTransfer,
                selected_file_ids: vec!["file-1".to_string()],
                skipped_file_ids: vec!["file-2".to_string()],
            }),
        );

        assert_eq!(
            selected_link_urls_from_inspection(&inspection),
            vec!["https://debrid.test/file-1".to_string()]
        );
    }

    #[test]
    fn classifies_debrid_sources() {
        assert_eq!(
            debrid_source_kind("magnet:?xt=urn:btih:abc").unwrap(),
            "magnet"
        );
        assert_eq!(
            debrid_source_kind("https://example.test/file").unwrap(),
            "hoster"
        );
        assert!(debrid_source_kind("ftp://example.test/file").is_err());
    }

    #[test]
    fn maps_real_debrid_status_to_local_status() {
        assert_eq!(
            real_debrid_status_to_job_status(Some("waiting_files_selection")),
            "waiting_files_selection"
        );
        assert_eq!(
            real_debrid_status_to_job_status(Some("downloaded")),
            "debrid_downloaded"
        );
        assert_eq!(
            release_job_state_for_job_status(Some("rd_downloaded")),
            ReleaseJobState::Downloading
        );
        assert_eq!(
            real_debrid_status_to_job_status(Some("magnet_error")),
            "failed"
        );
    }

    #[test]
    fn real_debrid_adapter_advertises_generic_contract_capabilities() {
        let capabilities = real_debrid_capabilities();

        assert!(capabilities.supports_magnet_submit);
        assert!(capabilities.supports_hoster_unrestrict);
        assert!(capabilities.supports_file_listing);
        assert!(capabilities.supports_file_selection);
        assert!(!capabilities.supports_cache_check);
        assert!(capabilities.supports_delete);
        assert!(capabilities.supports_progress);
        assert_eq!(
            capabilities.file_selection_mode,
            DebridFileSelectionMode::BeforeTransfer
        );
    }

    #[test]
    fn real_debrid_adapter_token_error_uses_provider_display_name() -> Result<()> {
        let err = match RealDebridClient::new("") {
            Ok(_) => bail!("empty token should fail"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains(DebridServiceKind::RealDebrid.display_name())
        );
        Ok(())
    }

    #[test]
    fn classifies_debrid_failures_without_treating_review_as_failure() {
        assert_eq!(
            classify_debrid_failure(
                "failed",
                Some("failed"),
                Some("selecting debrid files for remote release failed"),
                None,
            ),
            Some(DebridFailureClass::SelectionFailed)
        );
        assert_eq!(
            classify_debrid_failure(
                "failed",
                Some("magnet_error"),
                Some("Real-Debrid magnet rejected the source"),
                None,
            ),
            Some(DebridFailureClass::InvalidSource)
        );
        assert_eq!(
            classify_debrid_failure(
                "review_required",
                Some("review_required"),
                None,
                Some("file_selection_unsupported"),
            ),
            None
        );
    }

    #[test]
    fn classifies_documented_debrid_provider_errors_to_response_policies() {
        let cases = [
            (
                r#"Real-Debrid API returned 401: {"error":"Bad token","error_code":8}"#,
                DebridFailureClass::ProviderAuthMissing,
                DebridFailureResponsePolicy::AccountActionRequired,
            ),
            (
                r#"Real-Debrid API returned 403: {"error":"infringing_file","error_code":35}"#,
                DebridFailureClass::ContentBlocked,
                DebridFailureResponsePolicy::TryAlternateRouteOrCandidate,
            ),
            (
                r#"Real-Debrid API returned 403: {"error":"traffic exhausted","error_code":23}"#,
                DebridFailureClass::QuotaExhausted,
                DebridFailureResponsePolicy::AccountActionRequired,
            ),
            (
                r#"Real-Debrid API returned 429: {"error":"Slow down","error_code":5}"#,
                DebridFailureClass::RateLimited,
                DebridFailureResponsePolicy::RetryProviderLater,
            ),
            (
                r#"Real-Debrid API returned 503: {"error":"Service unavailable","error_code":25}"#,
                DebridFailureClass::ProviderUnavailable,
                DebridFailureResponsePolicy::RetryProviderLater,
            ),
            (
                r#"Real-Debrid API returned 503: {"error":"File unavailable","error_code":24}"#,
                DebridFailureClass::NotFoundExpired,
                DebridFailureResponsePolicy::TryAlternateRouteOrCandidate,
            ),
            (
                r#"Real-Debrid API returned 400: {"error":"Torrent file invalid","error_code":30}"#,
                DebridFailureClass::InvalidSource,
                DebridFailureResponsePolicy::TryAlternateRouteOrCandidate,
            ),
            (
                r#"Real-Debrid API returned 400: {"error":"Unsupported hoster","error_code":16}"#,
                DebridFailureClass::InvalidSource,
                DebridFailureResponsePolicy::TryAlternateRouteOrCandidate,
            ),
            (
                "AllDebrid API request rejected (MAGNET_INVALID_URI): Magnet is not valid",
                DebridFailureClass::InvalidSource,
                DebridFailureResponsePolicy::TryAlternateRouteOrCandidate,
            ),
            (
                "AllDebrid API request rejected (MAGNET_MUST_BE_PREMIUM): You must be premium",
                DebridFailureClass::ProviderAccountRestricted,
                DebridFailureResponsePolicy::AccountActionRequired,
            ),
            (
                "AllDebrid API request rejected (MAGNET_TOO_MANY_ACTIVE): maximum allowed active magnets",
                DebridFailureClass::TooManyActiveDownloads,
                DebridFailureResponsePolicy::RetryProviderLater,
            ),
            (
                "AllDebrid API request rejected (MAGNET_NO_SERVER): Server are not allowed to use this feature",
                DebridFailureClass::ProviderAccountRestricted,
                DebridFailureResponsePolicy::AccountActionRequired,
            ),
            (
                "AllDebrid API request rejected (LINK_HOST_FULL): All servers are full for this host, please retry later",
                DebridFailureClass::ProviderUnavailable,
                DebridFailureResponsePolicy::RetryProviderLater,
            ),
            (
                "AllDebrid API request rejected (MAGNET_CANT_BOOTSTRAP): Not downloaded in 20 min",
                DebridFailureClass::StagingTimeout,
                DebridFailureResponsePolicy::TryAlternateRouteOrCandidate,
            ),
            (
                "AllDebrid status providerStatusCode: 15 File not available - no peer",
                DebridFailureClass::NoSeeds,
                DebridFailureResponsePolicy::TryAlternateRouteOrCandidate,
            ),
            (
                "AllDebrid API request rejected (MAGNET_LINKS_REMOVED): Removed from hoster website",
                DebridFailureClass::NotFoundExpired,
                DebridFailureResponsePolicy::TryAlternateRouteOrCandidate,
            ),
            (
                "Premiumize API provider unavailable (rate_limit_reached): too many API requests",
                DebridFailureClass::RateLimited,
                DebridFailureResponsePolicy::RetryProviderLater,
            ),
            (
                "Premiumize API provider unavailable (account_limit_reached): fair-use points exhausted",
                DebridFailureClass::ProviderAccountLimitReached,
                DebridFailureResponsePolicy::AccountActionRequired,
            ),
            (
                "Premiumize API rejected request (invalid_request): missing malformed parameter",
                DebridFailureClass::InvalidSource,
                DebridFailureResponsePolicy::TryAlternateRouteOrCandidate,
            ),
            (
                "Premiumize API rejected request (service_unsupported): link points to a service we cannot process",
                DebridFailureClass::InvalidSource,
                DebridFailureResponsePolicy::TryAlternateRouteOrCandidate,
            ),
            (
                "Premiumize API temporary error (transient_error): try again",
                DebridFailureClass::ProviderUnavailable,
                DebridFailureResponsePolicy::RetryProviderLater,
            ),
            (
                "TorBox status: Stalled (No seeds)",
                DebridFailureClass::NoSeeds,
                DebridFailureResponsePolicy::TryAlternateRouteOrCandidate,
            ),
            (
                "TorBox API rate limit (429): too many requests",
                DebridFailureClass::RateLimited,
                DebridFailureResponsePolicy::RetryProviderLater,
            ),
        ];

        for (message, expected_class, expected_policy) in cases {
            let failure_class =
                classify_debrid_failure("failed", Some("failed"), Some(message), None);
            assert_eq!(failure_class, Some(expected_class), "{message}");
            assert_eq!(
                expected_class.response_policy(),
                expected_policy,
                "{message}"
            );
        }
    }

    #[test]
    fn all_debrid_no_peer_status_uses_torbox_strength_cleanup_evidence() -> Result<()> {
        let status = AllDebridMagnetStatus {
            id: json!(77),
            filename: Some("Show.S01E01.1080p.WEB-DL".to_string()),
            size: Some(1024),
            status: Some("File not available - no peer".to_string()),
            status_code: Some(15),
            downloaded: Some(0),
            download_speed: Some(0),
            seeders: Some(0),
            files: Vec::new(),
        };
        let inspection = all_debrid_status_to_inspection(status, Vec::new(), Vec::new(), None)?;
        assert_eq!(inspection.release.status, DebridReleaseStatus::Failed);

        let provider_status = debrid_provider_status_from_inspection(&inspection);
        assert_eq!(
            provider_status
                .get("providerImplementation")
                .and_then(Value::as_str),
            Some("all_debrid")
        );
        assert_eq!(
            provider_status
                .get("providerFailureClass")
                .and_then(Value::as_str),
            Some("no_seeds")
        );
        assert_eq!(
            provider_status.get("notCached").and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            provider_status.get("noSeeds").and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            provider_status.get("message").and_then(Value::as_str),
            Some("AllDebrid accepted this magnet, but it is not cached and has no peers.")
        );
        assert!(should_cleanup_uncached_no_seed_release(&inspection));
        assert_eq!(
            classify_debrid_failure(
                "failed",
                Some("failed"),
                debrid_failure_message_from_inspection(&inspection).as_deref(),
                None,
            ),
            Some(DebridFailureClass::NoSeeds)
        );
        Ok(())
    }

    #[test]
    fn real_debrid_dead_torrent_routes_without_uncached_cleanup() -> Result<()> {
        let torrent = RealDebridTorrent {
            id: Some("rd-dead-1".to_string()),
            filename: Some("Show.S01E01.1080p.WEB-DL".to_string()),
            bytes: Some(1024),
            original_bytes: Some(1024),
            progress: Some(0.0),
            status: Some("dead".to_string()),
            links: Vec::new(),
            files: Vec::new(),
            speed: Some(0),
        };
        let inspection = real_debrid_torrent_to_inspection("rd-dead-1", torrent)?;
        assert_eq!(inspection.release.status, DebridReleaseStatus::Failed);

        let provider_status = debrid_provider_status_from_inspection(&inspection);
        assert_eq!(
            provider_status
                .get("providerImplementation")
                .and_then(Value::as_str),
            Some(REAL_DEBRID_IMPLEMENTATION)
        );
        assert_eq!(
            provider_status
                .get("providerFailureClass")
                .and_then(Value::as_str),
            Some("no_seeds")
        );
        assert_eq!(
            provider_status.get("noSeeds").and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            provider_status.get("notCached").and_then(Value::as_bool),
            Some(false)
        );
        assert!(!should_cleanup_uncached_no_seed_release(&inspection));
        assert_eq!(
            classify_debrid_failure(
                "failed",
                Some("failed"),
                debrid_failure_message_from_inspection(&inspection).as_deref(),
                None,
            ),
            Some(DebridFailureClass::NoSeeds)
        );
        Ok(())
    }

    #[test]
    fn premiumize_uncached_no_peer_transfer_uses_cleanup_evidence() -> Result<()> {
        let transfer = PremiumizeTransfer {
            id: Some("pm-transfer-1".to_string()),
            name: Some("Show.S01E01.1080p.WEB-DL".to_string()),
            status: Some("error".to_string()),
            progress: Some(0.0),
            message: Some("Torrent is not cached and has no peers".to_string()),
            folder_id: None,
            file_id: None,
        };
        let inspection =
            premiumize_transfer_to_inspection(&transfer, Vec::new(), Vec::new(), None)?;
        assert_eq!(inspection.release.status, DebridReleaseStatus::Failed);

        let provider_status = debrid_provider_status_from_inspection(&inspection);
        assert_eq!(
            provider_status
                .get("providerImplementation")
                .and_then(Value::as_str),
            Some("premiumize")
        );
        assert_eq!(
            provider_status
                .get("providerFailureClass")
                .and_then(Value::as_str),
            Some("no_seeds")
        );
        assert_eq!(
            provider_status.get("notCached").and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            provider_status.get("noSeeds").and_then(Value::as_bool),
            Some(true)
        );
        assert!(should_cleanup_uncached_no_seed_release(&inspection));
        assert_eq!(
            classify_debrid_failure(
                "failed",
                Some("failed"),
                debrid_failure_message_from_inspection(&inspection).as_deref(),
                None,
            ),
            Some(DebridFailureClass::NoSeeds)
        );
        Ok(())
    }

    #[test]
    fn debrid_progress_evidence_surfaces_review_failure_and_fallback_state() {
        let job = DebridDownloadJob {
            job_id: Uuid::new_v4(),
            provider_id: Uuid::new_v4(),
            instance_id: Uuid::new_v4(),
            owner_id: "test.source".to_string(),
            source: "magnet:?xt=urn:btih:0123456789abcdef".to_string(),
            source_kind: "magnet".to_string(),
            category: Some("series".to_string()),
            display_name: Some("Show.S01.PACK".to_string()),
            remote_torrent_id: Some("remote-1".to_string()),
            remote_download_id: None,
            provider_implementation: Some(REAL_DEBRID_IMPLEMENTATION.to_string()),
            remote_release_id: Some("remote-1".to_string()),
            remote_release_status: Some("failed".to_string()),
            provider_capabilities: Some(json!({
                "supportsFileSelection": true,
                "fileSelectionMode": "before_transfer"
            })),
            provider_status: Some(json!({
                "providerImplementation": REAL_DEBRID_IMPLEMENTATION,
                "status": "failed"
            })),
            selection_mode: Some("before_transfer".to_string()),
            selected_file_ids: vec!["1".to_string()],
            skipped_file_ids: vec!["2".to_string(), "3".to_string()],
            selection_error: Some("file_selection_unsupported,missing_file_list".to_string()),
            release_id: Some(Uuid::new_v4()),
            status: "failed".to_string(),
            local_path: None,
            links: Vec::new(),
            progress: Some(0.0),
            downloaded_bytes: Some(0),
            total_bytes: Some(1024),
            download_rate_bps: None,
            last_error: Some("selecting debrid files failed".to_string()),
        };

        let evidence = debrid_progress_evidence_for_job(&job);

        assert_eq!(evidence.provider_name.as_deref(), Some("Real-Debrid"));
        assert_eq!(evidence.selected_file_count, 1);
        assert_eq!(evidence.skipped_file_count, 2);
        assert_eq!(evidence.failure_class.as_deref(), Some("selection_failed"));
        assert_eq!(
            evidence.review_reasons,
            vec![
                "file_selection_unsupported".to_string(),
                "missing_file_list".to_string()
            ]
        );
        assert_eq!(
            evidence.fallback_state,
            "eligible_if_candidate_supports_torrent_route"
        );
    }

    #[test]
    fn debrid_progress_evidence_uses_selected_service_display_names() {
        for service in DebridServiceKind::ALL {
            let job = DebridDownloadJob {
                job_id: Uuid::new_v4(),
                provider_id: Uuid::new_v4(),
                instance_id: Uuid::new_v4(),
                owner_id: "test.source".to_string(),
                source: "magnet:?xt=urn:btih:0123456789abcdef".to_string(),
                source_kind: "magnet".to_string(),
                category: Some("series".to_string()),
                display_name: Some("Show.S01.PACK".to_string()),
                remote_torrent_id: Some("remote-1".to_string()),
                remote_download_id: None,
                provider_implementation: Some(service.implementation_id().to_string()),
                remote_release_id: Some("remote-1".to_string()),
                remote_release_status: Some("downloading".to_string()),
                provider_capabilities: None,
                provider_status: None,
                selection_mode: Some("before_transfer".to_string()),
                selected_file_ids: vec!["1".to_string()],
                skipped_file_ids: Vec::new(),
                selection_error: None,
                release_id: Some(Uuid::new_v4()),
                status: "downloading".to_string(),
                local_path: None,
                links: Vec::new(),
                progress: Some(0.25),
                downloaded_bytes: Some(256),
                total_bytes: Some(1024),
                download_rate_bps: Some(128),
                last_error: None,
            };

            let evidence = debrid_progress_evidence_for_job(&job);

            assert_eq!(
                evidence.provider_name.as_deref(),
                Some(service.display_name())
            );
            assert_eq!(
                evidence.provider_implementation.as_deref(),
                Some(service.implementation_id())
            );
            assert_eq!(evidence.fallback_state, "not_needed");
        }
    }

    #[tokio::test]
    async fn generic_debrid_adapter_contract_round_trips_fake_release() -> Result<()> {
        let adapter = FakeDebridAdapter::new();
        let account = adapter.test_account().await?;
        assert_eq!(account.provider_implementation, "fake_debrid");
        assert!(adapter.capabilities().supports_file_selection);

        let release = adapter
            .submit_magnet("magnet:?xt=urn:btih:0123456789abcdef")
            .await?;
        assert_eq!(release.status, DebridReleaseStatus::WaitingFiles);

        let inspection = adapter.inspect_release(&release.remote_release_id).await?;
        assert_eq!(inspection.files.len(), 2);
        assert_eq!(
            inspection
                .selection
                .as_ref()
                .map(|selection| selection.skipped_file_ids.len()),
            Some(2)
        );

        let selected = adapter
            .select_files(&release.remote_release_id, &["file-1".to_string()])
            .await?;
        assert_eq!(
            selected
                .selection
                .as_ref()
                .map(|selection| selection.selected_file_ids.as_slice()),
            Some(&["file-1".to_string()][..])
        );
        assert_eq!(
            selected
                .files
                .iter()
                .find(|file| file.provider_file_id == "file-1")
                .and_then(|file| file.selected),
            Some(true)
        );

        let links = adapter.list_links(&release.remote_release_id).await?;
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].provider_file_id.as_deref(), Some("file-1"));

        let hoster = adapter
            .unrestrict_hoster("https://example.test/file")
            .await?;
        assert_eq!(hoster.url, "https://example.test/file");

        let progress = adapter.refresh_progress(&release.remote_release_id).await?;
        assert_eq!(progress.status, DebridReleaseStatus::Selected);

        assert!(adapter.delete_release(&release.remote_release_id).await?);
        assert!(
            adapter
                .inspect_release(&release.remote_release_id)
                .await
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn real_debrid_adapter_maps_http_to_generic_contract() -> Result<()> {
        let (base_url, state, shutdown) = start_mock_real_debrid_server().await?;
        let adapter = RealDebridClient::with_base_url("test-token", base_url)?;

        let account = adapter.test_account().await?;
        assert_eq!(account.provider_implementation, REAL_DEBRID_IMPLEMENTATION);
        assert_eq!(account.username.as_deref(), Some("rd-user"));

        let submitted = adapter
            .submit_magnet("magnet:?xt=urn:btih:0123456789abcdef")
            .await?;
        assert_eq!(submitted.remote_release_id, "rd-torrent-1");
        assert_eq!(
            state.added_magnets.lock().unwrap().as_slice(),
            ["magnet:?xt=urn:btih:0123456789abcdef"]
        );

        let inspection = adapter.inspect_release("rd-torrent-1").await?;
        assert_eq!(inspection.release.status, DebridReleaseStatus::WaitingFiles);
        assert_eq!(inspection.files.len(), 2);
        assert_eq!(inspection.files[0].provider_file_id, "1");
        assert_eq!(inspection.files[0].basename, "Show.S01E01.mkv");
        assert_eq!(inspection.files[0].size_bytes, Some(2048));
        assert_eq!(inspection.files[0].selected, Some(false));

        let selected =
            DebridProviderAdapter::select_files(&adapter, "rd-torrent-1", &["1".to_string()])
                .await?;
        assert_eq!(state.selected_files.lock().unwrap().as_slice(), ["1"]);
        assert_eq!(selected.release.status, DebridReleaseStatus::Downloaded);
        assert_eq!(
            selected
                .selection
                .as_ref()
                .map(|selection| selection.selected_file_ids.as_slice()),
            Some(&["1".to_string()][..])
        );

        let links = adapter.list_links("rd-torrent-1").await?;
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].provider_file_id.as_deref(), Some("1"));
        assert_eq!(links[0].url, "https://real-debrid.test/link/1");

        let unrestricted = adapter
            .unrestrict_hoster("https://real-debrid.test/link/1")
            .await?;
        assert_eq!(
            unrestricted.provider_file_id.as_deref(),
            Some("unrestricted-1")
        );
        assert_eq!(
            unrestricted.url,
            "https://download.real-debrid.test/Show.S01E01.mkv"
        );

        let progress = adapter.refresh_progress("rd-torrent-1").await?;
        assert_eq!(progress.status, DebridReleaseStatus::Downloaded);
        assert_eq!(progress.progress, Some(1.0));

        assert!(adapter.delete_release("rd-torrent-1").await?);
        assert_eq!(
            state.deleted_releases.lock().unwrap().as_slice(),
            ["rd-torrent-1"]
        );

        let err = adapter.inspect_release("provider-error").await.unwrap_err();
        assert!(err.to_string().contains("503"));
        assert!(err.to_string().contains("temporary"));

        let _ = shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn real_debrid_submission_lifecycle_runs_through_generic_factory() -> Result<()> {
        let state = setup_debrid_test_state().await?;
        let store = ExtensionStore::new(&state.db_pool);
        let (base_url, mock_state, shutdown) = start_mock_real_debrid_server().await?;
        let instance_id = setup_debrid_factory_instance(
            &state.db_pool,
            &store,
            json!({
                "activeService": "real_debrid",
                "materialize": true,
                "testRealDebridApiBaseUrl": base_url.clone()
            }),
        )
        .await?;
        save_debrid_token(
            state.secrets.as_ref(),
            &store,
            instance_id,
            DebridServiceKind::RealDebrid,
            "rd-token",
        )
        .await?;
        let provider_id = default_debrid_provider_id(&store, instance_id).await?;
        let source = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567";

        let job_id = submit_debrid(
            &state,
            &store,
            provider_id,
            instance_id,
            None,
            source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("series"),
                name: Some("Show.S01.PACK"),
                paused: false,
                release_context: None,
            },
        )
        .await?;

        assert_eq!(
            mock_state.added_magnets.lock().unwrap().as_slice(),
            [source]
        );
        let job = load_debrid_job(&state.db_pool, job_id)
            .await?
            .context("Real-Debrid job should be persisted")?;
        assert_eq!(
            job.provider_implementation.as_deref(),
            Some(REAL_DEBRID_IMPLEMENTATION)
        );
        assert_eq!(job.remote_release_id.as_deref(), Some("rd-torrent-1"));
        assert_eq!(job.status, "waiting_files_selection");

        let adapter = DebridAdapterFactory::from_state(&state)
            .adapter_for_job_implementation(
                &store,
                instance_id,
                job.provider_implementation.as_deref(),
            )
            .await?;
        let selected = adapter
            .select_files("rd-torrent-1", &["1".to_string()])
            .await?;
        update_debrid_job_from_inspection(&state.db_pool, job_id, &selected).await?;

        assert_eq!(mock_state.selected_files.lock().unwrap().as_slice(), ["1"]);
        let job = load_debrid_job(&state.db_pool, job_id)
            .await?
            .context("selected Real-Debrid job should load")?;
        assert_eq!(job.status, "debrid_downloaded");
        assert_eq!(job.links, vec!["https://real-debrid.test/link/1"]);
        assert_eq!(job.selected_file_ids, vec!["1".to_string()]);

        store
            .update_instance_config(
                instance_id,
                Some(&normalized_debrid_instance_config(Some(json!({
                    "activeService": "premiumize",
                    "materialize": true,
                    "testRealDebridApiBaseUrl": base_url.clone()
                })))),
            )
            .await?;
        let progress = load_debrid_progress(&state, &store, provider_id, instance_id).await?;
        assert_eq!(progress.len(), 1);
        assert_eq!(progress[0].state.as_deref(), Some("debrid_downloaded"));
        assert_eq!(
            progress[0]
                .debrid
                .as_ref()
                .and_then(|evidence| evidence.provider_implementation.as_deref()),
            Some(REAL_DEBRID_IMPLEMENTATION)
        );

        sqlx::query::<sqlx::Any>(
            "UPDATE debrid_download_jobs SET provider_implementation = NULL WHERE job_id = $1",
        )
        .bind(job_id.to_string())
        .execute(&state.db_pool)
        .await?;
        let progress = load_debrid_progress(&state, &store, provider_id, instance_id).await?;
        assert_eq!(
            progress[0]
                .debrid
                .as_ref()
                .and_then(|evidence| evidence.provider_implementation.as_deref()),
            Some(REAL_DEBRID_IMPLEMENTATION)
        );
        let job = load_debrid_job(&state.db_pool, job_id)
            .await?
            .context("legacy Real-Debrid job should be refreshed")?;
        assert_eq!(
            job.provider_implementation.as_deref(),
            Some(REAL_DEBRID_IMPLEMENTATION)
        );

        assert!(
            cancel_debrid_job(
                &state,
                &store,
                provider_id,
                instance_id,
                &job_id.to_string()
            )
            .await?
        );
        assert_eq!(
            mock_state.deleted_releases.lock().unwrap().as_slice(),
            ["rd-torrent-1"]
        );
        let job = load_debrid_job(&state.db_pool, job_id)
            .await?
            .context("cancelled Real-Debrid job should load")?;
        assert_eq!(job.status, "cancelled");

        let _ = shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn active_service_switch_only_affects_future_debrid_submissions() -> Result<()> {
        let state = setup_debrid_test_state().await?;
        let store = ExtensionStore::new(&state.db_pool);
        let (rd_base_url, _rd_state, rd_shutdown) = start_mock_real_debrid_server().await?;
        let (torbox_base_url, torbox_state, torbox_shutdown) =
            start_mock_torbox_lifecycle_server().await?;
        let (all_debrid_base_url, all_debrid_state, all_debrid_shutdown) =
            start_mock_all_debrid_lifecycle_server().await?;
        let (premiumize_base_url, premiumize_state, premiumize_shutdown) =
            start_mock_premiumize_directdl_server().await?;
        let instance_id = setup_debrid_factory_instance(
            &state.db_pool,
            &store,
            json!({
                "activeService": "real_debrid",
                "materialize": true,
                "testRealDebridApiBaseUrl": rd_base_url.clone(),
                "testTorBoxApiBaseUrl": torbox_base_url.clone(),
                "testAllDebridApiBaseUrl": all_debrid_base_url.clone(),
                "testPremiumizeApiBaseUrl": premiumize_base_url.clone(),
                "maxConcurrentDownloads": 4
            }),
        )
        .await?;
        for (service, token) in [
            (DebridServiceKind::RealDebrid, "good-token"),
            (DebridServiceKind::TorBox, "good-token"),
            (DebridServiceKind::AllDebrid, "good-token"),
            (DebridServiceKind::Premiumize, "good-token"),
        ] {
            save_debrid_token(state.secrets.as_ref(), &store, instance_id, service, token).await?;
        }
        let provider_id =
            reconcile_debrid_provider_for_instance(&state.db_pool, &store, instance_id).await?;
        let subscription_id = create_series_subscription_with_targets(&state.db_pool).await?;
        let source = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567";

        let old_job_id = submit_debrid(
            &state,
            &store,
            provider_id,
            instance_id,
            Some(REAL_DEBRID_IMPLEMENTATION),
            source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("series"),
                name: Some("Show.S01.RealDebrid.1080p.WEB-DL"),
                paused: true,
                release_context: Some(DebridReleaseSubmitContext {
                    subscription_id: Some(subscription_id),
                    source_provider_id: Some(provider_id),
                    source_extension_id: "test.source".to_string(),
                    media_type: MediaType::Series,
                    title: "Show".to_string(),
                    release_title: "Show.S01.RealDebrid.1080p.WEB-DL".to_string(),
                    info_hash: Some("0123456789abcdef0123456789abcdef01234567".to_string()),
                    fingerprint: Some("dp9b-old-real-debrid-job".to_string()),
                    score: Some(100.0),
                    selected_candidate: Some(json!({
                        "title": "Show.S01.RealDebrid.1080p.WEB-DL",
                        "source": source,
                        "sourceKind": "magnet"
                    })),
                }),
            },
        )
        .await?;
        let old_job = load_debrid_job(&state.db_pool, old_job_id)
            .await?
            .context("old Real-Debrid job should load")?;
        assert_eq!(
            old_job.provider_implementation.as_deref(),
            Some(REAL_DEBRID_IMPLEMENTATION)
        );
        assert_eq!(old_job.status, "paused");

        for (service, fingerprint, release_title) in [
            (
                DebridServiceKind::TorBox,
                "dp9b-new-torbox-job",
                "Show.S01.TorBox.1080p.WEB-DL",
            ),
            (
                DebridServiceKind::AllDebrid,
                "dp9b-new-all-debrid-job",
                "Show.S01.AllDebrid.1080p.WEB-DL",
            ),
            (
                DebridServiceKind::Premiumize,
                "dp9b-new-premiumize-job",
                "Show.S01.Premiumize.1080p.WEB-DL",
            ),
        ] {
            store
                .update_instance_config(
                    instance_id,
                    Some(&normalized_debrid_instance_config(Some(json!({
                        "activeService": service.implementation_id(),
                        "materialize": true,
                        "testRealDebridApiBaseUrl": rd_base_url.clone(),
                        "testTorBoxApiBaseUrl": torbox_base_url.clone(),
                        "testAllDebridApiBaseUrl": all_debrid_base_url.clone(),
                        "testPremiumizeApiBaseUrl": premiumize_base_url.clone(),
                        "maxConcurrentDownloads": 4
                    })))),
                )
                .await?;
            let active_provider_id =
                reconcile_debrid_provider_for_instance(&state.db_pool, &store, instance_id).await?;
            assert_eq!(
                active_provider_id, provider_id,
                "active service switches should not replace the canonical provider id"
            );
            let provider = store
                .get_provider(provider_id)
                .await?
                .context("canonical Debrid provider should remain")?;
            assert_eq!(
                provider.implementation.as_deref(),
                Some(service.implementation_id())
            );

            let job_id = submit_debrid(
                &state,
                &store,
                provider_id,
                instance_id,
                provider.implementation.as_deref(),
                source,
                DebridSubmitOptions {
                    owner_id: "test.source",
                    category: Some("series"),
                    name: Some(release_title),
                    paused: false,
                    release_context: Some(DebridReleaseSubmitContext {
                        subscription_id: Some(subscription_id),
                        source_provider_id: Some(provider_id),
                        source_extension_id: "test.source".to_string(),
                        media_type: MediaType::Series,
                        title: "Show".to_string(),
                        release_title: release_title.to_string(),
                        info_hash: Some("0123456789abcdef0123456789abcdef01234567".to_string()),
                        fingerprint: Some(fingerprint.to_string()),
                        score: Some(99.0),
                        selected_candidate: Some(json!({
                            "title": release_title,
                            "source": source,
                            "sourceKind": "magnet",
                            "cachedDebrid": true
                        })),
                    }),
                },
            )
            .await?;
            let new_job = load_debrid_job(&state.db_pool, job_id)
                .await?
                .with_context(|| format!("{} job should load", service.display_name()))?;
            assert_eq!(
                new_job.provider_id,
                provider_id,
                "future {} submissions should keep the canonical provider id",
                service.display_name()
            );
            assert_eq!(
                new_job.provider_implementation.as_deref(),
                Some(service.implementation_id()),
                "future submissions should capture the active service at submit time"
            );
            let release = get_release_by_download_id(&state.db_pool, &job_id.to_string())
                .await?
                .with_context(|| {
                    format!("{} acquisition release should load", service.display_name())
                })?;
            assert_eq!(release.selected_provider_id, Some(provider_id));
            assert_eq!(
                release
                    .coverage_plan
                    .as_ref()
                    .and_then(|plan| plan.get("debridProvider"))
                    .and_then(|provider| provider.get("providerImplementation"))
                    .and_then(Value::as_str),
                Some(service.implementation_id())
            );

            let old_job = load_debrid_job(&state.db_pool, old_job_id)
                .await?
                .context("old Real-Debrid job should remain readable")?;
            assert_eq!(
                old_job.provider_implementation.as_deref(),
                Some(REAL_DEBRID_IMPLEMENTATION),
                "switching active service must not rewrite historical jobs"
            );
            let old_release = get_release_by_download_id(&state.db_pool, &old_job_id.to_string())
                .await?
                .context("old Real-Debrid acquisition release should remain readable")?;
            assert_eq!(
                old_release
                    .coverage_plan
                    .as_ref()
                    .and_then(|plan| plan.get("debridProvider"))
                    .and_then(|provider| provider.get("providerImplementation"))
                    .and_then(Value::as_str),
                Some(REAL_DEBRID_IMPLEMENTATION),
                "switching active service must not rewrite historical release provenance"
            );
        }

        assert_eq!(
            torbox_state.added_magnets.lock().unwrap().as_slice(),
            [source]
        );
        assert_eq!(
            all_debrid_state.added_magnets.lock().unwrap().as_slice(),
            [source]
        );
        assert_eq!(
            premiumize_state.directdl_sources.lock().unwrap().as_slice(),
            [source]
        );

        let _ = rd_shutdown.send(());
        let _ = torbox_shutdown.send(());
        let _ = all_debrid_shutdown.send(());
        let _ = premiumize_shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn torbox_submission_materializes_selected_pack_after_active_service_switch() -> Result<()>
    {
        let state = setup_debrid_test_state().await?;
        let store = ExtensionStore::new(&state.db_pool);
        let (base_url, mock_state, shutdown) = start_mock_torbox_lifecycle_server().await?;
        let instance_id = setup_debrid_factory_instance(
            &state.db_pool,
            &store,
            json!({
                "activeService": "torbox",
                "materialize": true,
                "testTorBoxApiBaseUrl": base_url.clone()
            }),
        )
        .await?;
        save_debrid_token(
            state.secrets.as_ref(),
            &store,
            instance_id,
            DebridServiceKind::TorBox,
            "good-token",
        )
        .await?;
        let provider_id =
            reconcile_debrid_provider_for_instance(&state.db_pool, &store, instance_id).await?;
        let provider = store
            .list_providers(Some(instance_id))
            .await?
            .into_iter()
            .find(|provider| provider.provider_id == provider_id)
            .context("TorBox default debrid provider should exist")?;
        assert_eq!(provider.implementation.as_deref(), Some("torbox"));

        let subscription_id = create_series_subscription_with_targets(&state.db_pool).await?;
        let source = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567";
        let job_id = submit_debrid(
            &state,
            &store,
            provider_id,
            instance_id,
            None,
            source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("series"),
                name: Some("Show.S01.1080p.WEB-DL"),
                paused: false,
                release_context: Some(DebridReleaseSubmitContext {
                    subscription_id: Some(subscription_id),
                    source_provider_id: Some(provider_id),
                    source_extension_id: "test.source".to_string(),
                    media_type: MediaType::Series,
                    title: "Show".to_string(),
                    release_title: "Show.S01.1080p.WEB-DL".to_string(),
                    info_hash: Some("0123456789abcdef0123456789abcdef01234567".to_string()),
                    fingerprint: Some("torbox-dp5c-pack-materializer".to_string()),
                    score: Some(99.0),
                    selected_candidate: Some(json!({
                        "title": "Show.S01.1080p.WEB-DL",
                        "source": source,
                        "sourceKind": "magnet",
                        "cachedDebrid": true,
                        "supportedRoutes": ["acquisition.debrid.default"],
                        "defaultRoute": "acquisition.debrid.default"
                    })),
                }),
            },
        )
        .await?;

        assert_eq!(
            mock_state.added_magnets.lock().unwrap().as_slice(),
            [source]
        );
        let job = load_debrid_job(&state.db_pool, job_id)
            .await?
            .context("TorBox job should load after submit")?;
        assert_eq!(job.provider_implementation.as_deref(), Some("torbox"));
        assert_eq!(job.remote_release_id.as_deref(), Some("77"));
        assert_eq!(job.status, "debrid_downloaded");
        assert_eq!(
            job.selected_file_ids,
            vec!["10".to_string(), "11".to_string()]
        );
        assert!(job.skipped_file_ids.is_empty());
        assert_eq!(job.links.len(), 2);
        assert_eq!(
            mock_state.requested_downloads.lock().unwrap().as_slice(),
            ["77:10", "77:11"]
        );

        let progress = load_debrid_progress(&state, &store, provider_id, instance_id).await?;
        assert_eq!(progress.len(), 1);
        let evidence = progress[0]
            .debrid
            .as_ref()
            .context("TorBox progress evidence should exist")?;
        assert_eq!(progress[0].state.as_deref(), Some("debrid_downloaded"));
        assert_eq!(evidence.provider_name.as_deref(), Some("TorBox"));
        assert_eq!(evidence.provider_implementation.as_deref(), Some("torbox"));
        assert_eq!(evidence.selected_file_count, 2);
        assert_eq!(evidence.fallback_state, "not_needed");

        store
            .update_instance_config(
                instance_id,
                Some(&normalized_debrid_instance_config(Some(json!({
                    "activeService": "real_debrid",
                    "materialize": true,
                    "testTorBoxApiBaseUrl": base_url.clone()
                })))),
            )
            .await?;

        process_debrid_jobs_once(&state).await?;

        let job = load_debrid_job(&state.db_pool, job_id)
            .await?
            .context("materialized TorBox job should load")?;
        assert_eq!(job.status, "completed");
        assert_eq!(job.progress, Some(1.0));
        assert_eq!(job.provider_implementation.as_deref(), Some("torbox"));
        let local_path = PathBuf::from(
            job.local_path
                .as_deref()
                .context("TorBox pack materialization should store a local path")?,
        );
        assert!(local_path.is_dir());
        let first = local_path.join("Show.S01E01.mkv");
        let second = local_path.join("Show.S01E02.mkv");
        assert_eq!(
            tokio::fs::read_to_string(&first).await?,
            "mock-torbox-download-Show.S01E01.mkv"
        );
        assert_eq!(
            tokio::fs::read_to_string(&second).await?,
            "mock-torbox-download-Show.S01E02.mkv"
        );

        let release = get_release_by_download_id(&state.db_pool, &job_id.to_string())
            .await?
            .context("TorBox acquisition release should load")?;
        assert_eq!(release.state, AcquisitionReleaseState::Completed);
        assert_eq!(release.release_kind, ReleaseKind::SeasonPack);
        assert_eq!(release.resolver_kind, ReleaseResolverKind::TvSonarrStyle);
        assert_eq!(release.confidence, ReleaseConfidence::High);
        assert_eq!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.get("debridRuntime"))
                .and_then(|runtime| runtime.get("providerImplementation"))
                .and_then(Value::as_str),
            Some("torbox")
        );
        let release_jobs = crate::acquisition::release_resolution::store::list_release_jobs(
            &state.db_pool,
            release.release_id,
        )
        .await?;
        assert_eq!(release_jobs.len(), 1);
        assert_eq!(release_jobs[0].state, ReleaseJobState::Completed);
        assert!(!release_jobs[0].active);
        let coverage = list_release_coverage(&state.db_pool, release.release_id).await?;
        assert_eq!(coverage.len(), 2);
        assert!(
            coverage
                .iter()
                .all(|entry| entry.state == ReleaseCoverageState::Submitted)
        );

        let _ = shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn torbox_cancel_uses_persisted_provider_after_active_service_switch() -> Result<()> {
        let state = setup_debrid_test_state().await?;
        let store = ExtensionStore::new(&state.db_pool);
        let (base_url, mock_state, shutdown) = start_mock_torbox_lifecycle_server().await?;
        let instance_id = setup_debrid_factory_instance(
            &state.db_pool,
            &store,
            json!({
                "activeService": "torbox",
                "materialize": true,
                "testTorBoxApiBaseUrl": base_url.clone()
            }),
        )
        .await?;
        save_debrid_token(
            state.secrets.as_ref(),
            &store,
            instance_id,
            DebridServiceKind::TorBox,
            "good-token",
        )
        .await?;
        let provider_id =
            reconcile_debrid_provider_for_instance(&state.db_pool, &store, instance_id).await?;
        let source = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567";
        let job_id = submit_debrid(
            &state,
            &store,
            provider_id,
            instance_id,
            None,
            source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("series"),
                name: Some("Show.S01.PACK"),
                paused: false,
                release_context: None,
            },
        )
        .await?;

        store
            .update_instance_config(
                instance_id,
                Some(&normalized_debrid_instance_config(Some(json!({
                    "activeService": "real_debrid",
                    "materialize": true,
                    "testTorBoxApiBaseUrl": base_url.clone()
                })))),
            )
            .await?;
        let cancelled = cancel_debrid_job(
            &state,
            &store,
            provider_id,
            instance_id,
            &job_id.to_string(),
        )
        .await?;

        assert!(cancelled);
        assert_eq!(
            mock_state.deleted_releases.lock().unwrap().as_slice(),
            ["77"]
        );
        let job = load_debrid_job(&state.db_pool, job_id)
            .await?
            .context("cancelled TorBox job should load")?;
        assert_eq!(job.status, "cancelled");
        assert_eq!(job.provider_implementation.as_deref(), Some("torbox"));
        assert!(job.last_error.is_none());

        let _ = shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn torbox_uncached_no_seed_failure_deletes_remote_release() -> Result<()> {
        let state = setup_debrid_test_state().await?;
        let store = ExtensionStore::new(&state.db_pool);
        let (base_url, mock_state, shutdown) = start_mock_torbox_lifecycle_server().await?;
        *mock_state.failed_no_seeds.lock().unwrap() = true;
        let instance_id = setup_debrid_factory_instance(
            &state.db_pool,
            &store,
            json!({
                "activeService": "torbox",
                "materialize": true,
                "testTorBoxApiBaseUrl": base_url.clone()
            }),
        )
        .await?;
        save_debrid_token(
            state.secrets.as_ref(),
            &store,
            instance_id,
            DebridServiceKind::TorBox,
            "good-token",
        )
        .await?;
        let provider_id =
            reconcile_debrid_provider_for_instance(&state.db_pool, &store, instance_id).await?;
        let subscription_id = create_series_subscription_with_targets(&state.db_pool).await?;
        let source = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567";

        let job_id = submit_debrid(
            &state,
            &store,
            provider_id,
            instance_id,
            None,
            source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("series"),
                name: Some("Show.S01.PACK"),
                paused: false,
                release_context: Some(DebridReleaseSubmitContext {
                    subscription_id: Some(subscription_id),
                    source_provider_id: Some(provider_id),
                    source_extension_id: "test.source".to_string(),
                    media_type: MediaType::Series,
                    title: "Show".to_string(),
                    release_title: "Show.S01.PACK".to_string(),
                    info_hash: Some("0123456789abcdef0123456789abcdef01234567".to_string()),
                    fingerprint: Some("torbox-uncached-no-seed-cleanup".to_string()),
                    score: Some(99.0),
                    selected_candidate: Some(json!({
                        "title": "Show.S01.PACK",
                        "source": source,
                        "sourceKind": "magnet",
                        "cachedDebrid": false
                    })),
                }),
            },
        )
        .await?;

        assert_eq!(
            mock_state.deleted_releases.lock().unwrap().as_slice(),
            ["77"]
        );
        let job = load_debrid_job(&state.db_pool, job_id)
            .await?
            .context("failed TorBox job should load")?;
        assert_eq!(job.status, "failed");
        assert_eq!(job.provider_implementation.as_deref(), Some("torbox"));
        assert_eq!(job.remote_release_id.as_deref(), Some("77"));
        assert_eq!(
            classify_debrid_job_failure(&job),
            Some(DebridFailureClass::NoSeeds)
        );
        let provider_cleanup = job
            .provider_status
            .as_ref()
            .and_then(|status| status.get("providerCleanup"))
            .context("job provider cleanup evidence should persist")?;
        assert_eq!(
            provider_cleanup.get("status").and_then(Value::as_str),
            Some("deleted")
        );
        assert_eq!(
            provider_cleanup.get("notCached").and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            provider_cleanup.get("noSeeds").and_then(Value::as_bool),
            Some(true)
        );

        let release = get_release_by_download_id(&state.db_pool, &job_id.to_string())
            .await?
            .context("failed TorBox acquisition release should load")?;
        let release_cleanup = release
            .coverage_plan
            .as_ref()
            .and_then(|plan| plan.get("debridProviderCleanup"))
            .context("release cleanup evidence should persist")?;
        assert_eq!(
            release_cleanup.get("status").and_then(Value::as_str),
            Some("deleted")
        );
        assert_eq!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.get("debridFailure"))
                .and_then(|failure| failure.get("failureClass"))
                .and_then(Value::as_str),
            Some("no_seeds")
        );

        let _ = shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn torbox_submit_rate_limit_records_fallback_evidence() -> Result<()> {
        let state = setup_debrid_test_state().await?;
        let store = ExtensionStore::new(&state.db_pool);
        let (base_url, _mock_state, shutdown) = start_mock_torbox_lifecycle_server().await?;
        let instance_id = setup_debrid_factory_instance(
            &state.db_pool,
            &store,
            json!({
                "activeService": "torbox",
                "materialize": true,
                "testTorBoxApiBaseUrl": base_url.clone()
            }),
        )
        .await?;
        save_debrid_token(
            state.secrets.as_ref(),
            &store,
            instance_id,
            DebridServiceKind::TorBox,
            "rate-limit-token",
        )
        .await?;
        let provider_id =
            reconcile_debrid_provider_for_instance(&state.db_pool, &store, instance_id).await?;
        let subscription_id = create_series_subscription_with_targets(&state.db_pool).await?;
        let source = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567";
        let err = submit_debrid(
            &state,
            &store,
            provider_id,
            instance_id,
            None,
            source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("series"),
                name: Some("Show.S01.1080p.WEB-DL"),
                paused: false,
                release_context: Some(DebridReleaseSubmitContext {
                    subscription_id: Some(subscription_id),
                    source_provider_id: Some(provider_id),
                    source_extension_id: "test.source".to_string(),
                    media_type: MediaType::Series,
                    title: "Show".to_string(),
                    release_title: "Show.S01.1080p.WEB-DL".to_string(),
                    info_hash: Some("0123456789abcdef0123456789abcdef01234567".to_string()),
                    fingerprint: Some("torbox-dp5c-rate-limit".to_string()),
                    score: Some(99.0),
                    selected_candidate: Some(json!({
                        "title": "Show.S01.1080p.WEB-DL",
                        "source": source,
                        "sourceKind": "magnet",
                        "supportedRoutes": [
                            "acquisition.debrid.default",
                            "acquisition.torrent.default"
                        ],
                        "defaultRoute": "acquisition.debrid.default"
                    })),
                }),
            },
        )
        .await
        .expect_err("TorBox create rate limit should fail provider submission");
        assert!(err.to_string().contains("TorBox API rate limit"));

        let release = crate::acquisition::release_resolution::store::get_release_by_fingerprint(
            &state.db_pool,
            DEFAULT_ROUTE_OWNER_ID,
            "test.source",
            "torbox-dp5c-rate-limit",
        )
        .await?
        .context("failed TorBox release should be persisted for fallback evidence")?;
        assert_eq!(release.state, AcquisitionReleaseState::Failed);
        assert_eq!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.get("debridFailure"))
                .and_then(|failure| failure.get("failureClass"))
                .and_then(Value::as_str),
            Some("rate_limited")
        );
        assert_eq!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.get("debridFailure"))
                .and_then(|failure| failure.get("responsePolicy"))
                .and_then(Value::as_str),
            Some("retry_provider_later")
        );
        assert_eq!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.get("debridFailure"))
                .and_then(|failure| failure.get("fallbackState"))
                .and_then(Value::as_str),
            Some("retry_provider_later")
        );
        assert_eq!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.get("debridProvider"))
                .and_then(|provider| provider.get("providerImplementation"))
                .and_then(Value::as_str),
            Some("torbox")
        );

        let _ = shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn all_debrid_submission_materializes_selected_pack_after_active_service_switch()
    -> Result<()> {
        let state = setup_debrid_test_state().await?;
        let store = ExtensionStore::new(&state.db_pool);
        let (base_url, mock_state, shutdown) = start_mock_all_debrid_lifecycle_server().await?;
        let instance_id = setup_debrid_factory_instance(
            &state.db_pool,
            &store,
            json!({
                "activeService": "all_debrid",
                "materialize": true,
                "testAllDebridApiBaseUrl": base_url.clone()
            }),
        )
        .await?;
        save_debrid_token(
            state.secrets.as_ref(),
            &store,
            instance_id,
            DebridServiceKind::AllDebrid,
            "good-token",
        )
        .await?;
        let provider_id =
            reconcile_debrid_provider_for_instance(&state.db_pool, &store, instance_id).await?;
        let provider = store
            .list_providers(Some(instance_id))
            .await?
            .into_iter()
            .find(|provider| provider.provider_id == provider_id)
            .context("AllDebrid default debrid provider should exist")?;
        assert_eq!(provider.implementation.as_deref(), Some("all_debrid"));

        let subscription_id = create_series_subscription_with_targets(&state.db_pool).await?;
        let source = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567";
        let job_id = submit_debrid(
            &state,
            &store,
            provider_id,
            instance_id,
            None,
            source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("series"),
                name: Some("Show.S01.1080p.WEB-DL"),
                paused: false,
                release_context: Some(DebridReleaseSubmitContext {
                    subscription_id: Some(subscription_id),
                    source_provider_id: Some(provider_id),
                    source_extension_id: "test.source".to_string(),
                    media_type: MediaType::Series,
                    title: "Show".to_string(),
                    release_title: "Show.S01.1080p.WEB-DL".to_string(),
                    info_hash: Some("0123456789abcdef0123456789abcdef01234567".to_string()),
                    fingerprint: Some("alldebrid-dp6c-pack-materializer".to_string()),
                    score: Some(99.0),
                    selected_candidate: Some(json!({
                        "title": "Show.S01.1080p.WEB-DL",
                        "source": source,
                        "sourceKind": "magnet",
                        "cachedDebrid": true,
                        "supportedRoutes": ["acquisition.debrid.default"],
                        "defaultRoute": "acquisition.debrid.default"
                    })),
                }),
            },
        )
        .await?;

        assert_eq!(
            mock_state.added_magnets.lock().unwrap().as_slice(),
            [source]
        );
        let job = load_debrid_job(&state.db_pool, job_id)
            .await?
            .context("AllDebrid job should load after submit")?;
        assert_eq!(job.provider_implementation.as_deref(), Some("all_debrid"));
        assert_eq!(job.remote_release_id.as_deref(), Some("88"));
        assert_eq!(job.status, "debrid_downloaded");
        assert_eq!(
            job.selected_file_ids,
            vec!["1".to_string(), "2".to_string()]
        );
        assert_eq!(job.skipped_file_ids, vec!["3".to_string(), "4".to_string()]);
        assert_eq!(job.links.len(), 2);
        assert_eq!(
            mock_state.unlocked_links.lock().unwrap().as_slice(),
            [
                "https://alldebrid.com/f/episode-1",
                "https://alldebrid.com/f/episode-2"
            ]
        );

        let progress = load_debrid_progress(&state, &store, provider_id, instance_id).await?;
        assert_eq!(progress.len(), 1);
        let evidence = progress[0]
            .debrid
            .as_ref()
            .context("AllDebrid progress evidence should exist")?;
        assert_eq!(progress[0].state.as_deref(), Some("debrid_downloaded"));
        assert_eq!(evidence.provider_name.as_deref(), Some("AllDebrid"));
        assert_eq!(
            evidence.provider_implementation.as_deref(),
            Some("all_debrid")
        );
        assert_eq!(evidence.selected_file_count, 2);
        assert_eq!(evidence.skipped_file_count, 2);
        assert_eq!(evidence.fallback_state, "not_needed");

        store
            .update_instance_config(
                instance_id,
                Some(&normalized_debrid_instance_config(Some(json!({
                    "activeService": "real_debrid",
                    "materialize": true,
                    "testAllDebridApiBaseUrl": base_url.clone()
                })))),
            )
            .await?;

        process_debrid_jobs_once(&state).await?;

        let job = load_debrid_job(&state.db_pool, job_id)
            .await?
            .context("materialized AllDebrid job should load")?;
        assert_eq!(job.status, "completed");
        assert_eq!(job.progress, Some(1.0));
        assert_eq!(job.provider_implementation.as_deref(), Some("all_debrid"));
        let local_path = PathBuf::from(
            job.local_path
                .as_deref()
                .context("AllDebrid pack materialization should store a local path")?,
        );
        assert!(local_path.is_dir());
        let first = local_path.join("Show.S01E01.mkv");
        let second = local_path.join("Show.S01E02.mkv");
        assert_eq!(
            tokio::fs::read_to_string(&first).await?,
            "mock-alldebrid-download-Show.S01E01.mkv"
        );
        assert_eq!(
            tokio::fs::read_to_string(&second).await?,
            "mock-alldebrid-download-Show.S01E02.mkv"
        );
        assert_eq!(
            mock_state.unlocked_links.lock().unwrap().as_slice(),
            [
                "https://alldebrid.com/f/episode-1",
                "https://alldebrid.com/f/episode-2"
            ]
        );

        let release = get_release_by_download_id(&state.db_pool, &job_id.to_string())
            .await?
            .context("AllDebrid acquisition release should load")?;
        assert_eq!(release.state, AcquisitionReleaseState::Completed);
        assert_eq!(release.release_kind, ReleaseKind::SeasonPack);
        assert_eq!(release.resolver_kind, ReleaseResolverKind::TvSonarrStyle);
        assert_eq!(release.confidence, ReleaseConfidence::High);
        assert_eq!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.get("debridRuntime"))
                .and_then(|runtime| runtime.get("providerImplementation"))
                .and_then(Value::as_str),
            Some("all_debrid")
        );
        let release_jobs = crate::acquisition::release_resolution::store::list_release_jobs(
            &state.db_pool,
            release.release_id,
        )
        .await?;
        assert_eq!(release_jobs.len(), 1);
        assert_eq!(release_jobs[0].state, ReleaseJobState::Completed);
        assert!(!release_jobs[0].active);
        let coverage = list_release_coverage(&state.db_pool, release.release_id).await?;
        assert_eq!(coverage.len(), 2);
        assert!(
            coverage
                .iter()
                .all(|entry| entry.state == ReleaseCoverageState::Submitted)
        );

        let _ = shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn all_debrid_hoster_materializes_through_generic_factory() -> Result<()> {
        let state = setup_debrid_test_state().await?;
        let store = ExtensionStore::new(&state.db_pool);
        let (base_url, mock_state, shutdown) = start_mock_all_debrid_lifecycle_server().await?;
        let instance_id = setup_debrid_factory_instance(
            &state.db_pool,
            &store,
            json!({
                "activeService": "all_debrid",
                "materialize": true,
                "testAllDebridApiBaseUrl": base_url.clone()
            }),
        )
        .await?;
        save_debrid_token(
            state.secrets.as_ref(),
            &store,
            instance_id,
            DebridServiceKind::AllDebrid,
            "good-token",
        )
        .await?;
        let provider_id =
            reconcile_debrid_provider_for_instance(&state.db_pool, &store, instance_id).await?;
        let source = "https://hoster.test/direct-file.mkv";

        let job_id = submit_debrid(
            &state,
            &store,
            provider_id,
            instance_id,
            None,
            source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("series"),
                name: Some("Show.S01E01.1080p.WEB-DL"),
                paused: false,
                release_context: None,
            },
        )
        .await?;
        let job = load_debrid_job(&state.db_pool, job_id)
            .await?
            .context("hoster AllDebrid job should load")?;
        assert_eq!(job.source_kind, "hoster");
        assert_eq!(job.status, "debrid_downloaded");
        assert_eq!(job.links, vec![source.to_string()]);
        assert_eq!(
            mock_state.unlocked_links.lock().unwrap().as_slice(),
            [source]
        );

        process_debrid_jobs_once(&state).await?;

        let job = load_debrid_job(&state.db_pool, job_id)
            .await?
            .context("materialized hoster AllDebrid job should load")?;
        assert_eq!(job.status, "completed");
        assert_eq!(job.progress, Some(1.0));
        let local_path = job
            .local_path
            .as_deref()
            .context("materialized AllDebrid job should have local path")?;
        let contents = tokio::fs::read_to_string(local_path).await?;
        assert_eq!(contents, "mock-alldebrid-download-direct-file.mkv");
        assert_eq!(
            mock_state.unlocked_links.lock().unwrap().as_slice(),
            [source, source]
        );

        let _ = shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn all_debrid_cancel_uses_persisted_provider_after_active_service_switch() -> Result<()> {
        let state = setup_debrid_test_state().await?;
        let store = ExtensionStore::new(&state.db_pool);
        let (base_url, mock_state, shutdown) = start_mock_all_debrid_lifecycle_server().await?;
        let instance_id = setup_debrid_factory_instance(
            &state.db_pool,
            &store,
            json!({
                "activeService": "all_debrid",
                "materialize": true,
                "testAllDebridApiBaseUrl": base_url.clone()
            }),
        )
        .await?;
        save_debrid_token(
            state.secrets.as_ref(),
            &store,
            instance_id,
            DebridServiceKind::AllDebrid,
            "good-token",
        )
        .await?;
        let provider_id =
            reconcile_debrid_provider_for_instance(&state.db_pool, &store, instance_id).await?;
        let source = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567";
        let job_id = submit_debrid(
            &state,
            &store,
            provider_id,
            instance_id,
            None,
            source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("series"),
                name: Some("Show.S01.PACK"),
                paused: false,
                release_context: None,
            },
        )
        .await?;

        store
            .update_instance_config(
                instance_id,
                Some(&normalized_debrid_instance_config(Some(json!({
                    "activeService": "real_debrid",
                    "materialize": true,
                    "testAllDebridApiBaseUrl": base_url.clone()
                })))),
            )
            .await?;
        let cancelled = cancel_debrid_job(
            &state,
            &store,
            provider_id,
            instance_id,
            &job_id.to_string(),
        )
        .await?;

        assert!(cancelled);
        assert_eq!(
            mock_state.deleted_releases.lock().unwrap().as_slice(),
            ["88"]
        );
        let job = load_debrid_job(&state.db_pool, job_id)
            .await?
            .context("cancelled AllDebrid job should load")?;
        assert_eq!(job.status, "cancelled");
        assert_eq!(job.provider_implementation.as_deref(), Some("all_debrid"));
        assert!(job.last_error.is_none());

        let _ = shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn all_debrid_submit_rate_limit_records_fallback_evidence() -> Result<()> {
        let state = setup_debrid_test_state().await?;
        let store = ExtensionStore::new(&state.db_pool);
        let (base_url, _mock_state, shutdown) = start_mock_all_debrid_lifecycle_server().await?;
        let instance_id = setup_debrid_factory_instance(
            &state.db_pool,
            &store,
            json!({
                "activeService": "all_debrid",
                "materialize": true,
                "testAllDebridApiBaseUrl": base_url.clone()
            }),
        )
        .await?;
        save_debrid_token(
            state.secrets.as_ref(),
            &store,
            instance_id,
            DebridServiceKind::AllDebrid,
            "rate-limit-token",
        )
        .await?;
        let provider_id =
            reconcile_debrid_provider_for_instance(&state.db_pool, &store, instance_id).await?;
        let subscription_id = create_series_subscription_with_targets(&state.db_pool).await?;
        let source = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567";
        let err = submit_debrid(
            &state,
            &store,
            provider_id,
            instance_id,
            None,
            source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("series"),
                name: Some("Show.S01.1080p.WEB-DL"),
                paused: false,
                release_context: Some(DebridReleaseSubmitContext {
                    subscription_id: Some(subscription_id),
                    source_provider_id: Some(provider_id),
                    source_extension_id: "test.source".to_string(),
                    media_type: MediaType::Series,
                    title: "Show".to_string(),
                    release_title: "Show.S01.1080p.WEB-DL".to_string(),
                    info_hash: Some("0123456789abcdef0123456789abcdef01234567".to_string()),
                    fingerprint: Some("alldebrid-dp6c-rate-limit".to_string()),
                    score: Some(99.0),
                    selected_candidate: Some(json!({
                        "title": "Show.S01.1080p.WEB-DL",
                        "source": source,
                        "sourceKind": "magnet",
                        "supportedRoutes": [
                            "acquisition.debrid.default",
                            "acquisition.torrent.default"
                        ],
                        "defaultRoute": "acquisition.debrid.default"
                    })),
                }),
            },
        )
        .await
        .expect_err("AllDebrid upload rate limit should fail provider submission");
        assert!(err.to_string().contains("AllDebrid API rate limit"));

        let release = crate::acquisition::release_resolution::store::get_release_by_fingerprint(
            &state.db_pool,
            DEFAULT_ROUTE_OWNER_ID,
            "test.source",
            "alldebrid-dp6c-rate-limit",
        )
        .await?
        .context("failed AllDebrid release should be persisted for fallback evidence")?;
        assert_eq!(release.state, AcquisitionReleaseState::Failed);
        assert_eq!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.get("debridFailure"))
                .and_then(|failure| failure.get("failureClass"))
                .and_then(Value::as_str),
            Some("rate_limited")
        );
        assert_eq!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.get("debridFailure"))
                .and_then(|failure| failure.get("responsePolicy"))
                .and_then(Value::as_str),
            Some("retry_provider_later")
        );
        assert_eq!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.get("debridFailure"))
                .and_then(|failure| failure.get("fallbackState"))
                .and_then(Value::as_str),
            Some("retry_provider_later")
        );
        assert_eq!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.get("debridProvider"))
                .and_then(|provider| provider.get("providerImplementation"))
                .and_then(Value::as_str),
            Some("all_debrid")
        );

        let _ = shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn real_debrid_hoster_materializes_through_generic_factory() -> Result<()> {
        let state = setup_debrid_test_state().await?;
        let store = ExtensionStore::new(&state.db_pool);
        let (base_url, _mock_state, shutdown) = start_mock_real_debrid_server().await?;
        let instance_id = setup_debrid_factory_instance(
            &state.db_pool,
            &store,
            json!({
                "activeService": "real_debrid",
                "materialize": true,
                "testRealDebridApiBaseUrl": base_url.clone()
            }),
        )
        .await?;
        save_debrid_token(
            state.secrets.as_ref(),
            &store,
            instance_id,
            DebridServiceKind::RealDebrid,
            "rd-token",
        )
        .await?;
        let provider_id = default_debrid_provider_id(&store, instance_id).await?;
        let source = format!("{base_url}/download/Show.S01E01.mkv");

        let job_id = submit_debrid(
            &state,
            &store,
            provider_id,
            instance_id,
            None,
            &source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("series"),
                name: Some("Show.S01E01.1080p.WEB-DL"),
                paused: false,
                release_context: None,
            },
        )
        .await?;
        let job = load_debrid_job(&state.db_pool, job_id)
            .await?
            .context("hoster Real-Debrid job should load")?;
        assert_eq!(job.source_kind, "hoster");
        assert_eq!(job.status, "debrid_downloaded");
        assert_eq!(job.links, vec![source.clone()]);

        process_debrid_jobs_once(&state).await?;

        let job = load_debrid_job(&state.db_pool, job_id)
            .await?
            .context("materialized hoster Real-Debrid job should load")?;
        assert_eq!(job.status, "completed");
        assert_eq!(job.progress, Some(1.0));
        let local_path = job
            .local_path
            .as_deref()
            .context("materialized Real-Debrid job should have local path")?;
        let contents = tokio::fs::read_to_string(local_path).await?;
        assert_eq!(contents, "mock-real-debrid-download");

        let _ = shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn generic_debrid_job_persistence_keeps_legacy_aliases_and_metadata() -> Result<()> {
        let database = setup_db().await?;
        let (provider_id, instance_id) = create_provider_refs(&database.pool).await?;
        let legacy_job_id = Uuid::new_v4();
        sqlx::query::<sqlx::Any>(
            "INSERT INTO debrid_download_jobs (
                job_id, provider_id, instance_id, owner_id, source, source_kind,
                remote_torrent_id, status, links_json
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(legacy_job_id.to_string())
        .bind(provider_id.to_string())
        .bind(instance_id.to_string())
        .bind("default")
        .bind("magnet:?xt=urn:btih:legacy")
        .bind("magnet")
        .bind("rd-legacy-torrent")
        .bind("submitted")
        .bind("[]")
        .execute(&database.pool)
        .await?;

        let legacy = load_debrid_job(&database.pool, legacy_job_id)
            .await?
            .context("legacy debrid job should load")?;
        assert_eq!(
            legacy.remote_torrent_id.as_deref(),
            Some("rd-legacy-torrent")
        );
        assert_eq!(legacy.remote_release_id, None);

        let generic_job_id = Uuid::new_v4();
        insert_debrid_job(
            &database.pool,
            &DebridDownloadJob {
                job_id: generic_job_id,
                provider_id,
                instance_id,
                owner_id: "default".to_string(),
                source: "magnet:?xt=urn:btih:generic".to_string(),
                source_kind: "magnet".to_string(),
                category: Some("series".to_string()),
                display_name: Some("Show.S01.PACK".to_string()),
                remote_torrent_id: None,
                remote_download_id: None,
                provider_implementation: Some("test_debrid".to_string()),
                remote_release_id: Some("generic-release-1".to_string()),
                remote_release_status: Some("waiting_files".to_string()),
                provider_capabilities: Some(json!({
                    "supportsFileSelection": true,
                    "fileSelectionMode": "before_transfer"
                })),
                provider_status: Some(json!({
                    "providerImplementation": "test_debrid",
                    "status": "waiting_files"
                })),
                selection_mode: Some("before_transfer".to_string()),
                selected_file_ids: vec!["file-1".to_string()],
                skipped_file_ids: vec!["file-2".to_string()],
                selection_error: None,
                release_id: None,
                status: "waiting_files_selection".to_string(),
                local_path: None,
                links: vec!["https://example.test/file-1".to_string()],
                progress: Some(0.0),
                downloaded_bytes: Some(0),
                total_bytes: Some(2048),
                download_rate_bps: None,
                last_error: None,
            },
        )
        .await?;
        let generic = load_debrid_job(&database.pool, generic_job_id)
            .await?
            .context("generic debrid job should load")?;
        assert_eq!(
            generic.provider_implementation.as_deref(),
            Some("test_debrid")
        );
        assert_eq!(
            generic.remote_release_id.as_deref(),
            Some("generic-release-1")
        );
        assert_eq!(generic.selected_file_ids, vec!["file-1".to_string()]);
        assert_eq!(generic.skipped_file_ids, vec!["file-2".to_string()]);
        assert_eq!(
            generic.provider_capabilities,
            Some(json!({
                "supportsFileSelection": true,
                "fileSelectionMode": "before_transfer"
            }))
        );
        assert_eq!(
            generic.provider_status,
            Some(json!({
                "providerImplementation": "test_debrid",
                "status": "waiting_files"
            }))
        );
        Ok(())
    }

    #[tokio::test]
    async fn alm7_anime_staging_error_recovers_without_review_failure_evidence() -> Result<()> {
        let database = setup_db().await?;
        let (provider_id, instance_id) = create_provider_refs(&database.pool).await?;
        let subscription_id = create_anime_subscription_with_target(
            &database.pool,
            "Tokyo Ghoul",
            "Tokyo Ghoul Root A",
            "S02E01",
            2,
            1,
            13,
        )
        .await?;
        let adapter = FakeDebridAdapter::failing_inspect();
        let source = "magnet:?xt=urn:btih:abababababababababababababababababababab";
        let fingerprint = "alm7-anime-staging-error";

        let error = submit_debrid_with_adapter_and_anime_matching(
            &database.pool,
            provider_id,
            instance_id,
            source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("anime"),
                name: Some("[Group] Tokyo Ghoul Root A - 01"),
                paused: false,
                release_context: Some(DebridReleaseSubmitContext {
                    subscription_id: Some(subscription_id),
                    source_provider_id: Some(provider_id),
                    source_extension_id: "test.source".to_string(),
                    media_type: MediaType::Anime,
                    title: "Tokyo Ghoul".to_string(),
                    release_title: "[Group] Tokyo Ghoul Root A - 01".to_string(),
                    info_hash: None,
                    fingerprint: Some(fingerprint.to_string()),
                    score: Some(95.0),
                    selected_candidate: Some(json!({
                        "title": "[Group] Tokyo Ghoul Root A - 01",
                        "source": source,
                        "sourceKind": "magnet"
                    })),
                }),
            },
            &AnimeMatchingService::disabled(),
            &adapter,
        )
        .await
        .expect_err("provider inspection should fail");
        assert!(error.to_string().contains("review_required"));

        let release = crate::acquisition::release_resolution::store::get_release_by_fingerprint(
            &database.pool,
            DEFAULT_ROUTE_OWNER_ID,
            "test.source",
            fingerprint,
        )
        .await?
        .context("anime staging-error release should persist")?;
        assert_eq!(release.state, AcquisitionReleaseState::Failed);
        let coverage_plan = release
            .coverage_plan
            .as_ref()
            .context("anime staging-error evidence should persist")?;
        assert_eq!(
            coverage_plan
                .pointer("/automaticResolutionError/status")
                .and_then(Value::as_str),
            Some("retryable")
        );
        assert_eq!(
            coverage_plan
                .pointer("/automaticRetry/status")
                .and_then(Value::as_str),
            Some("scheduled")
        );
        assert!(
            coverage_plan
                .pointer("/debridFailure/responsePolicy")
                .is_none()
        );
        assert!(
            coverage_plan
                .pointer("/debridFailure/fallbackState")
                .is_none()
        );
        assert!(!anime_automatic_evidence_has_review_semantics(
            coverage_plan
        ));

        let job_id = release
            .download_id
            .as_deref()
            .context("anime staging-error job id")?
            .parse::<Uuid>()?;
        let job = load_debrid_job(&database.pool, job_id)
            .await?
            .context("anime staging-error job should persist")?;
        assert_eq!(job.status, "failed");
        assert_ne!(job.status, "review_required");
        let target = list_subscription_targets(&database.pool, subscription_id)
            .await?
            .into_iter()
            .next()
            .context("anime staging-error target should persist")?;
        assert_eq!(target.state, AcquisitionTargetState::Pending);
        assert!(target.next_search_after.is_some());
        assert!(adapter.state.lock().unwrap().releases.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn staged_debrid_submit_selects_exact_file_ids_without_select_all() -> Result<()> {
        let database = setup_db().await?;
        let (provider_id, instance_id) = create_provider_refs(&database.pool).await?;
        let subscription_id = create_series_subscription_with_targets(&database.pool).await?;
        let adapter = FakeDebridAdapter::new();
        let source = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567";
        let job_id = submit_debrid_with_adapter(
            &database.pool,
            provider_id,
            instance_id,
            source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("series"),
                name: Some("Show.S01.1080p.WEB-DL"),
                paused: false,
                release_context: Some(DebridReleaseSubmitContext {
                    subscription_id: Some(subscription_id),
                    source_provider_id: Some(provider_id),
                    source_extension_id: "test.source".to_string(),
                    media_type: MediaType::Series,
                    title: "Show".to_string(),
                    release_title: "Show.S01.1080p.WEB-DL".to_string(),
                    info_hash: None,
                    fingerprint: None,
                    score: Some(99.0),
                    selected_candidate: Some(json!({
                        "title": "Show.S01.1080p.WEB-DL",
                        "source": source,
                        "sourceKind": "magnet",
                        "supportedRoutes": ["acquisition.debrid.default"]
                    })),
                }),
            },
            &adapter,
        )
        .await?;

        let job = load_debrid_job(&database.pool, job_id)
            .await?
            .context("staged debrid job should load")?;
        assert_eq!(job.provider_implementation.as_deref(), Some("fake_debrid"));
        assert_eq!(
            job.provider_capabilities
                .as_ref()
                .and_then(|capabilities| capabilities.get("supportsFileSelection"))
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(job.status, "debrid_downloading");
        assert_eq!(
            job.selected_file_ids,
            vec!["file-1".to_string(), "file-2".to_string()]
        );
        assert!(job.skipped_file_ids.is_empty());
        assert_eq!(job.links.len(), 2);
        assert!(job.release_id.is_some());

        let release = crate::acquisition::release_resolution::store::get_release(
            &database.pool,
            job.release_id.unwrap(),
        )
        .await?
        .context("staged acquisition release should load")?;
        assert_eq!(release.state, AcquisitionReleaseState::Downloading);
        assert_eq!(release.release_kind, ReleaseKind::SeasonPack);
        assert_eq!(release.resolver_kind, ReleaseResolverKind::TvSonarrStyle);
        assert_eq!(release.confidence, ReleaseConfidence::High);
        assert_eq!(release.remote_release_id.as_deref(), Some("fake-release-1"));
        let job_id_string = job_id.to_string();
        assert_eq!(release.download_id.as_deref(), Some(job_id_string.as_str()));
        let provider_evidence = release
            .coverage_plan
            .as_ref()
            .and_then(|plan| plan.get("debridProvider"))
            .context("debrid provider provenance should be persisted")?;
        assert_eq!(
            provider_evidence
                .get("providerImplementation")
                .and_then(Value::as_str),
            Some("fake_debrid")
        );
        assert_eq!(
            provider_evidence
                .get("providerName")
                .and_then(Value::as_str),
            Some("Fake Debrid")
        );
        assert_eq!(
            provider_evidence
                .get("remoteReleaseId")
                .and_then(Value::as_str),
            Some("fake-release-1")
        );
        assert_eq!(
            provider_evidence.get("jobId").and_then(Value::as_str),
            Some(job_id_string.as_str())
        );

        let files = list_release_files(&database.pool, release.release_id).await?;
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].provider_file_id.as_deref(), Some("file-1"));
        assert_eq!(files[0].selected, Some(true));
        assert_eq!(files[1].provider_file_id.as_deref(), Some("file-2"));
        assert_eq!(files[1].selected, Some(true));

        let coverage = crate::acquisition::release_resolution::store::list_release_coverage(
            &database.pool,
            release.release_id,
        )
        .await?;
        assert_eq!(coverage.len(), 2);
        assert!(
            coverage
                .iter()
                .all(|entry| entry.state == ReleaseCoverageState::Selected)
        );
        let selection_policy = release
            .coverage_plan
            .as_ref()
            .and_then(|plan| plan.get("selectionPolicy"))
            .context("selection policy evidence should be persisted")?;
        assert_eq!(
            selection_policy.get("status").and_then(Value::as_str),
            Some("approved")
        );
        assert_eq!(
            selection_policy
                .get("providerSelectionIds")
                .and_then(Value::as_array)
                .map(|values| values.len()),
            Some(2)
        );
        assert_eq!(
            selection_policy.get("selectAll").and_then(Value::as_bool),
            Some(false)
        );

        let fake_state = adapter.state.lock().unwrap();
        let fake_release = fake_state
            .releases
            .get("fake-release-1")
            .context("fake release should still exist")?;
        assert_eq!(
            fake_release.selected_file_ids,
            vec!["file-1".to_string(), "file-2".to_string()]
        );
        Ok(())
    }

    #[tokio::test]
    async fn alm7_anime_provider_submit_failure_retries_without_review_evidence() -> Result<()> {
        let database = setup_db().await?;
        let (provider_id, instance_id) = create_provider_refs(&database.pool).await?;
        let subscription_id = create_anime_subscription_with_target(
            &database.pool,
            "Submit Failure Anime",
            "Submit Failure Anime",
            "S01E01",
            1,
            1,
            1,
        )
        .await?;
        let targets = list_subscription_targets(&database.pool, subscription_id).await?;
        let source = "magnet:?xt=urn:btih:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let fingerprint = "alm7-anime-submit-failure";
        let result = submit_debrid_with_adapter_and_anime_matching(
            &database.pool,
            provider_id,
            instance_id,
            source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("anime"),
                name: Some("Submit.Failure.Anime.01"),
                paused: false,
                release_context: Some(DebridReleaseSubmitContext {
                    subscription_id: Some(subscription_id),
                    source_provider_id: Some(provider_id),
                    source_extension_id: "test.source".to_string(),
                    media_type: MediaType::Anime,
                    title: "Submit Failure Anime".to_string(),
                    release_title: "Submit.Failure.Anime.01".to_string(),
                    info_hash: Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string()),
                    fingerprint: Some(fingerprint.to_string()),
                    score: Some(90.0),
                    selected_candidate: Some(json!({
                        "title": "Submit.Failure.Anime.01",
                        "source": source,
                        "sourceKind": "magnet",
                        "requestScopeEvidence": {
                            "targetIds": [targets[0].target_id],
                            "targetKeys": [targets[0].target_key.clone()]
                        }
                    })),
                }),
            },
            &AnimeMatchingService::disabled(),
            &FakeDebridAdapter::failing_submit(),
        )
        .await;
        assert!(result.is_err());
        let release = crate::acquisition::release_resolution::store::get_release_by_fingerprint(
            &database.pool,
            DEFAULT_ROUTE_OWNER_ID,
            "test.source",
            fingerprint,
        )
        .await?
        .context("anime provider-submit failure release")?;
        assert_eq!(release.state, AcquisitionReleaseState::Failed);
        let coverage_plan = release
            .coverage_plan
            .as_ref()
            .context("automatic retry evidence")?;
        assert!(!anime_automatic_evidence_has_review_semantics(
            coverage_plan
        ));
        assert_eq!(
            coverage_plan
                .pointer("/automaticRetry/reason")
                .and_then(Value::as_str),
            Some("anime_debrid_provider_submit_error")
        );
        let jobs = crate::acquisition::release_resolution::store::list_release_jobs(
            &database.pool,
            release.release_id,
        )
        .await?;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].state, ReleaseJobState::Failed);
        assert!(!jobs[0].active);
        let target = get_target(&database.pool, targets[0].target_id)
            .await?
            .context("automatic retry target")?;
        assert_eq!(target.state, AcquisitionTargetState::Pending);
        assert!(target.next_search_after.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn alm7_debrid_bookkeeping_barrier_and_failed_inspection_survive_restart() -> Result<()> {
        let database = setup_db().await?;
        let (provider_id, instance_id) = create_provider_refs(&database.pool).await?;
        let subscription_id = create_anime_subscription_with_target(
            &database.pool,
            "Tokyo Ghoul",
            "Tokyo Ghoul Root A",
            "S02E01",
            2,
            1,
            13,
        )
        .await?;
        let target = list_subscription_targets(&database.pool, subscription_id)
            .await?
            .into_iter()
            .next()
            .context("bookkeeping-barrier target")?;
        let adapter = FakeDebridAdapter::with_files(vec![DebridRemoteFile {
            provider_file_id: "actual-file-42".to_string(),
            file_index: Some(42),
            path: "Tokyo Ghoul Root A/[Group] TGRA - 13.mkv".to_string(),
            basename: "[Group] TGRA - 13.mkv".to_string(),
            size_bytes: Some(2_048),
            selectable: true,
            selected: Some(false),
            raw: None,
        }]);
        let engine = FakeAnimeMatchEngine::new(FakeAnimeMatchBehavior::MatchFirst);
        let source = "magnet:?xt=urn:btih:7777777777777777777777777777777777777777";
        let job_id = submit_debrid_with_adapter_and_anime_matching(
            &database.pool,
            provider_id,
            instance_id,
            source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("anime"),
                name: Some("[Group] TGRA - 13"),
                paused: false,
                release_context: Some(DebridReleaseSubmitContext {
                    subscription_id: Some(subscription_id),
                    source_provider_id: Some(provider_id),
                    source_extension_id: "test.source".to_string(),
                    media_type: MediaType::Anime,
                    title: "Tokyo Ghoul".to_string(),
                    release_title: "[Group] TGRA - 13".to_string(),
                    info_hash: None,
                    fingerprint: Some(format!("alm7-debrid-bookkeeping-{}", Uuid::new_v4())),
                    score: Some(95.0),
                    selected_candidate: Some(json!({
                        "title": "[Group] TGRA - 13",
                        "source": source,
                        "sourceKind": "magnet",
                        "submissionBookkeeping": {
                            "status": "pending",
                            "policyVersion": "acquisition-broker-bookkeeping-v1"
                        },
                        "requestScopeEvidence": {
                            "targetIds": [target.target_id],
                            "targetKeys": [target.target_key.clone()]
                        }
                    })),
                }),
            },
            &engine.service(),
            &adapter,
        )
        .await?;

        let staged_job = load_debrid_job(&database.pool, job_id)
            .await?
            .context("bookkeeping-barrier job")?;
        assert_eq!(engine.calls.load(AtomicOrdering::SeqCst), 0);
        assert!(staged_job.selected_file_ids.is_empty());
        let remote_release_id = staged_job
            .remote_release_id
            .clone()
            .context("bookkeeping-barrier remote release")?;
        assert!(
            adapter
                .state
                .lock()
                .unwrap()
                .releases
                .get(&remote_release_id)
                .context("fake remote release")?
                .selected_file_ids
                .is_empty()
        );

        // Simulate a provider failure and process death immediately after the
        // inspection write, before the retry consumer runs.
        let mut failed_inspection = adapter.inspect_release(&remote_release_id).await?;
        failed_inspection.release.status = DebridReleaseStatus::Failed;
        failed_inspection.release.raw_status = Some("provider_failed_after_submit".to_string());
        update_debrid_job_from_inspection(&database.pool, job_id, &failed_inspection).await?;
        let pending_job = load_debrid_job(&database.pool, job_id)
            .await?
            .context("atomic retry-pending job")?;
        assert_eq!(pending_job.status, "anime_retry_pending");
        let retry = anime_debrid_retry_disposition_from_job(&pending_job)
            .context("atomic failed-inspection retry marker")?;
        assert!(
            list_active_debrid_jobs(&database.pool, 10)
                .await?
                .iter()
                .any(|job| job.job_id == job_id),
            "a restart must rediscover the durable retry-pending job"
        );

        let release = get_release(
            &database.pool,
            pending_job.release_id.context("retry-pending release id")?,
        )
        .await?
        .context("retry-pending release")?;
        persist_anime_debrid_retry_with_adapter(
            &database.pool,
            &adapter,
            job_id,
            &release,
            &remote_release_id,
            &failed_inspection.release.provider_implementation,
            &retry,
        )
        .await?;

        let consumed_job = load_debrid_job(&database.pool, job_id)
            .await?
            .context("consumed retry job")?;
        assert_eq!(consumed_job.status, "failed");
        assert!(anime_debrid_retry_disposition_from_job(&consumed_job).is_none());
        assert_eq!(
            consumed_job
                .provider_status
                .as_ref()
                .and_then(|status| status.pointer("/animeAutomaticRetryConsumed/status"))
                .and_then(Value::as_str),
            Some("consumed")
        );
        assert!(
            list_active_debrid_jobs(&database.pool, 10)
                .await?
                .iter()
                .all(|job| job.job_id != job_id),
            "consumed failed jobs must not wake forever"
        );
        let failed_release = get_release(&database.pool, release.release_id)
            .await?
            .context("failed automatic-retry release")?;
        assert_eq!(failed_release.state, AcquisitionReleaseState::Failed);
        assert_ne!(
            failed_release.state,
            AcquisitionReleaseState::ReviewRequired
        );
        let target = get_target(&database.pool, target.target_id)
            .await?
            .context("automatic retry target")?;
        assert_eq!(target.state, AcquisitionTargetState::Pending);
        Ok(())
    }

    #[tokio::test]
    async fn alm7_debrid_failed_submit_status_waits_for_bookkeeping_then_retries() -> Result<()> {
        let database = setup_db().await?;
        let (provider_id, instance_id) = create_provider_refs(&database.pool).await?;
        let subscription_id = create_anime_subscription_with_target(
            &database.pool,
            "Provider Failed Anime",
            "Provider Failed Anime",
            "S01E01",
            1,
            1,
            1,
        )
        .await?;
        let target = list_subscription_targets(&database.pool, subscription_id)
            .await?
            .into_iter()
            .next()
            .context("failed-submit target")?;
        let adapter = FakeDebridAdapter::failed_submit_status();
        let engine = FakeAnimeMatchEngine::new(FakeAnimeMatchBehavior::MatchFirst);
        let source = "magnet:?xt=urn:btih:8888888888888888888888888888888888888888";
        let job_id = submit_debrid_with_adapter_and_anime_matching(
            &database.pool,
            provider_id,
            instance_id,
            source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("anime"),
                name: Some("Provider.Failed.Anime.01"),
                paused: false,
                release_context: Some(DebridReleaseSubmitContext {
                    subscription_id: Some(subscription_id),
                    source_provider_id: Some(provider_id),
                    source_extension_id: "test.source".to_string(),
                    media_type: MediaType::Anime,
                    title: "Provider Failed Anime".to_string(),
                    release_title: "Provider.Failed.Anime.01".to_string(),
                    info_hash: None,
                    fingerprint: Some(format!("alm7-failed-submit-{}", Uuid::new_v4())),
                    score: Some(90.0),
                    selected_candidate: Some(json!({
                        "title": "Provider.Failed.Anime.01",
                        "source": source,
                        "sourceKind": "magnet",
                        "submissionBookkeeping": {
                            "status": "pending",
                            "policyVersion": "acquisition-broker-bookkeeping-v1"
                        },
                        "requestScopeEvidence": {
                            "targetIds": [target.target_id],
                            "targetKeys": [target.target_key.clone()]
                        }
                    })),
                }),
            },
            &engine.service(),
            &adapter,
        )
        .await?;

        let job = load_debrid_job(&database.pool, job_id)
            .await?
            .context("failed-submit job")?;
        assert_eq!(job.status, "anime_retry_pending");
        assert_eq!(job.remote_release_status.as_deref(), Some("failed"));
        assert!(anime_debrid_retry_disposition_from_job(&job).is_none());
        let release = get_release(
            &database.pool,
            job.release_id.context("failed-submit release id")?,
        )
        .await?
        .context("failed-submit release")?;
        assert_eq!(release.state, AcquisitionReleaseState::Staging);
        assert!(debrid_release_bookkeeping_pending(&release));
        assert_eq!(engine.calls.load(AtomicOrdering::SeqCst), 0);
        assert!(
            !stage_deferred_anime_debrid_provider_failure_if_ready(&database.pool, &job).await?
        );

        let mut selected_candidate = release
            .selected_candidate
            .clone()
            .context("failed-submit candidate evidence")?;
        selected_candidate
            .as_object_mut()
            .context("failed-submit candidate object")?
            .insert(
                "submissionBookkeeping".to_string(),
                json!({
                    "status": "complete",
                    "policyVersion": "acquisition-broker-bookkeeping-v1"
                }),
            );
        sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_releases
             SET selected_candidate_json = $1, updated_at = CURRENT_TIMESTAMP
             WHERE release_id = $2",
        )
        .bind(serde_json::to_string(&selected_candidate)?)
        .bind(release.release_id.to_string())
        .execute(&database.pool)
        .await?;

        assert!(stage_deferred_anime_debrid_provider_failure_if_ready(&database.pool, &job).await?);
        let marked = load_debrid_job(&database.pool, job_id)
            .await?
            .context("marked failed-submit job")?;
        let retry = anime_debrid_retry_disposition_from_job(&marked)
            .context("failed-submit automatic retry marker")?;
        let release = get_release(&database.pool, release.release_id)
            .await?
            .context("completed-bookkeeping failed-submit release")?;
        let remote_release_id = marked
            .remote_release_id
            .as_deref()
            .context("failed-submit remote release")?;
        persist_anime_debrid_retry_with_adapter(
            &database.pool,
            &adapter,
            job_id,
            &release,
            remote_release_id,
            adapter.implementation(),
            &retry,
        )
        .await?;

        let consumed = load_debrid_job(&database.pool, job_id)
            .await?
            .context("consumed failed-submit job")?;
        assert_eq!(consumed.status, "failed");
        assert!(anime_debrid_retry_disposition_from_job(&consumed).is_none());
        assert!(adapter.state.lock().unwrap().releases.is_empty());
        let failed_release = get_release(&database.pool, release.release_id)
            .await?
            .context("terminal failed-submit release")?;
        assert_eq!(failed_release.state, AcquisitionReleaseState::Failed);
        assert_ne!(
            failed_release.state,
            AcquisitionReleaseState::ReviewRequired
        );
        let target = get_target(&database.pool, target.target_id)
            .await?
            .context("failed-submit retry target")?;
        assert_eq!(target.state, AcquisitionTargetState::Pending);
        assert!(target.next_search_after.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn alm7_anime_provider_failed_status_cleans_up_and_never_requests_review() -> Result<()> {
        for (index, adapter) in [
            FakeDebridAdapter::failed_inspection(),
            FakeDebridAdapter::failing_delete(),
        ]
        .into_iter()
        .enumerate()
        {
            let database = setup_db().await?;
            let (provider_id, instance_id) = create_provider_refs(&database.pool).await?;
            let subscription_id = create_anime_subscription_with_target(
                &database.pool,
                "Provider Failed Anime",
                "Provider Failed Anime",
                "S01E01",
                1,
                1,
                1,
            )
            .await?;
            let targets = list_subscription_targets(&database.pool, subscription_id).await?;
            let source = format!("magnet:?xt=urn:btih:{:040x}", 0xbb_u128 + index as u128);
            let fingerprint = format!("alm7-anime-provider-failed-{index}");
            let job_id = submit_debrid_with_adapter_and_anime_matching(
                &database.pool,
                provider_id,
                instance_id,
                &source,
                DebridSubmitOptions {
                    owner_id: "test.source",
                    category: Some("anime"),
                    name: Some("Provider.Failed.Anime.01"),
                    paused: false,
                    release_context: Some(DebridReleaseSubmitContext {
                        subscription_id: Some(subscription_id),
                        source_provider_id: Some(provider_id),
                        source_extension_id: "test.source".to_string(),
                        media_type: MediaType::Anime,
                        title: "Provider Failed Anime".to_string(),
                        release_title: "Provider.Failed.Anime.01".to_string(),
                        info_hash: None,
                        fingerprint: Some(fingerprint),
                        score: Some(90.0),
                        selected_candidate: Some(json!({
                            "title": "Provider.Failed.Anime.01",
                            "source": source,
                            "sourceKind": "magnet",
                            "requestScopeEvidence": {
                                "targetIds": [targets[0].target_id],
                                "targetKeys": [targets[0].target_key.clone()]
                            }
                        })),
                    }),
                },
                &AnimeMatchingService::disabled(),
                &adapter,
            )
            .await?;
            let job = load_debrid_job(&database.pool, job_id)
                .await?
                .context("provider-failed anime job")?;
            assert_eq!(job.status, "failed");
            let release = get_release(
                &database.pool,
                job.release_id.context("provider-failed anime release id")?,
            )
            .await?
            .context("provider-failed anime release")?;
            assert_eq!(release.state, AcquisitionReleaseState::Failed);
            let coverage_plan = release
                .coverage_plan
                .as_ref()
                .context("provider-failed automatic retry evidence")?;
            assert!(!anime_automatic_evidence_has_review_semantics(
                coverage_plan
            ));
            assert_eq!(
                coverage_plan
                    .pointer("/automaticRetry/reason")
                    .and_then(Value::as_str),
                Some("anime_debrid_provider_failed")
            );
            let cleanup_status = coverage_plan
                .pointer("/automaticRetry/providerCleanup/status")
                .and_then(Value::as_str);
            assert_eq!(
                cleanup_status,
                Some(if index == 0 {
                    "deleted"
                } else {
                    "delete_failed"
                })
            );
            let target = get_target(&database.pool, targets[0].target_id)
                .await?
                .context("provider-failed retry target")?;
            assert_eq!(target.state, AcquisitionTargetState::Pending);
        }
        Ok(())
    }

    #[tokio::test]
    async fn alm7_debrid_real_file_model_match_selects_exact_provider_file_automatically()
    -> Result<()> {
        let database = setup_db().await?;
        let (provider_id, instance_id) = create_provider_refs(&database.pool).await?;
        let subscription_id = create_anime_subscription_with_target(
            &database.pool,
            "Tokyo Ghoul",
            "Tokyo Ghoul Root A",
            "S02E01",
            2,
            1,
            13,
        )
        .await?;
        sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_subscriptions SET quality_profile_json = $1 WHERE subscription_id = $2",
        )
        .bind(
            json!({
                "animeAudioPreference": {
                    "mode": "require_dub_review",
                    "language": "en"
                }
            })
            .to_string(),
        )
        .bind(subscription_id.to_string())
        .execute(&database.pool)
        .await?;
        let actual_path = "Tokyo Ghoul Root A/[Group] TGRA - 13 [Dual Audio].mkv";
        let adapter = FakeDebridAdapter::with_files(vec![DebridRemoteFile {
            provider_file_id: "actual-file-42".to_string(),
            file_index: Some(42),
            path: actual_path.to_string(),
            basename: "[Group] TGRA - 13 [Dual Audio].mkv".to_string(),
            size_bytes: Some(2_048),
            selectable: true,
            selected: Some(false),
            raw: None,
        }]);
        let engine = FakeAnimeMatchEngine::new(FakeAnimeMatchBehavior::MatchFirst);
        let service = engine.service();
        let source = "magnet:?xt=urn:btih:1111111111111111111111111111111111111111";

        let job_id = submit_debrid_with_adapter_and_anime_matching(
            &database.pool,
            provider_id,
            instance_id,
            source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("anime"),
                name: Some("[Group] TGRA - 13 [Dual Audio]"),
                paused: false,
                release_context: Some(DebridReleaseSubmitContext {
                    subscription_id: Some(subscription_id),
                    source_provider_id: Some(provider_id),
                    source_extension_id: "test.source".to_string(),
                    media_type: MediaType::Anime,
                    title: "Tokyo Ghoul".to_string(),
                    release_title: "[Group] TGRA - 13 [Dual Audio]".to_string(),
                    info_hash: None,
                    fingerprint: Some("alm7-model-success".to_string()),
                    score: Some(95.0),
                    selected_candidate: Some(json!({
                        "title": "[Group] TGRA - 13 [Dual Audio]",
                        "source": source,
                        "sourceKind": "magnet",
                        "supportedRoutes": [DEBRID_DEFAULT_LOGICAL_ID],
                        "defaultRoute": DEBRID_DEFAULT_LOGICAL_ID
                    })),
                }),
            },
            &service,
            &adapter,
        )
        .await?;

        assert_eq!(engine.calls.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(
            engine.observed_paths.lock().unwrap().as_slice(),
            [actual_path]
        );
        let job = load_debrid_job(&database.pool, job_id)
            .await?
            .context("ALM-7 model-assisted Debrid job should load")?;
        assert_eq!(job.selected_file_ids, vec!["actual-file-42".to_string()]);
        assert_eq!(job.status, "debrid_downloading");
        assert_ne!(job.status, "review_required");

        let release = get_release(&database.pool, job.release_id.context("release id")?)
            .await?
            .context("ALM-7 model-assisted release should load")?;
        assert_eq!(release.state, AcquisitionReleaseState::Downloading);
        assert_eq!(release.confidence, ReleaseConfidence::High);
        assert_eq!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.pointer("/animeMatchAssist/source"))
                .and_then(Value::as_str),
            Some("local_model")
        );
        assert_eq!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.pointer("/modelAudioProfile"))
                .and_then(Value::as_str),
            Some("dual_audio")
        );
        assert_eq!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.pointer("/modelAudioAssessment/state"))
                .and_then(Value::as_str),
            Some("match")
        );
        assert_eq!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|plan| {
                    plan.pointer("/modelAudioAssessment/requiredPreferenceSatisfied")
                })
                .and_then(Value::as_bool),
            Some(true)
        );
        let coverage = list_release_coverage(&database.pool, release.release_id).await?;
        assert_eq!(coverage.len(), 1);
        assert_eq!(coverage[0].state, ReleaseCoverageState::Selected);
        assert_eq!(
            coverage[0].verified_by.as_deref(),
            Some("alm7_debrid_local_model_file_list")
        );
        assert_eq!(
            adapter
                .state
                .lock()
                .unwrap()
                .releases
                .get("fake-release-1")
                .context("fake release")?
                .selected_file_ids,
            vec!["actual-file-42".to_string()]
        );
        assert!(
            transition_anime_debrid_runtime_if_owned(
                &database.pool,
                job_id,
                AnimeDebridRuntimeTransition::Materializing,
                None,
            )
            .await?,
            "the current exact attempt must own the materialization handoff"
        );
        assert!(
            transition_anime_debrid_runtime_if_owned(
                &database.pool,
                job_id,
                AnimeDebridRuntimeTransition::Completed,
                Some("/library/Tokyo Ghoul Root A/S02E01.mkv"),
            )
            .await?,
            "the current materializing attempt must own completion"
        );
        let completed_job = load_debrid_job(&database.pool, job_id)
            .await?
            .context("completed ALM-7 Debrid job should load")?;
        assert_eq!(completed_job.status, "completed");
        assert_eq!(completed_job.progress, Some(1.0));
        assert_eq!(
            completed_job.local_path.as_deref(),
            Some("/library/Tokyo Ghoul Root A/S02E01.mkv")
        );
        let completed_release = get_release(
            &database.pool,
            completed_job.release_id.context("completed release id")?,
        )
        .await?
        .context("completed ALM-7 release should load")?;
        assert_eq!(completed_release.state, AcquisitionReleaseState::Completed);
        let completed_download_id = completed_job.job_id.to_string();
        let completed_release_job =
            crate::acquisition::release_resolution::store::list_release_jobs(
                &database.pool,
                completed_release.release_id,
            )
            .await?
            .into_iter()
            .find(|release_job| {
                release_job.download_id.as_deref() == Some(completed_download_id.as_str())
            })
            .context("completed ALM-7 release job should load")?;
        assert_eq!(completed_release_job.state, ReleaseJobState::Completed);
        Ok(())
    }

    #[tokio::test]
    async fn alm7_debrid_scopes_targets_and_requires_exact_plan_coverage() -> Result<()> {
        let database = setup_db().await?;
        let subscription_id = create_series_subscription_with_targets(&database.pool).await?;
        let mut targets = list_subscription_targets(&database.pool, subscription_id).await?;
        assert_eq!(targets.len(), 2);
        let release_id = Uuid::new_v4();
        let mut covered = test_coverage(release_id, Uuid::new_v4());
        covered.target_id = targets[0].target_id;
        let mut rejected = test_coverage(release_id, Uuid::new_v4());
        rejected.target_id = targets[1].target_id;
        rejected.state = ReleaseCoverageState::Rejected;
        let mut release = test_debrid_release(ReleaseKind::Single, ReleaseConfidence::High);

        let scoped =
            debrid_release_scoped_targets(&release, &targets, &[covered, rejected.clone()], &[]);
        assert_eq!(scoped.len(), 1);
        assert_eq!(scoped[0].target_id, targets[0].target_id);
        assert_eq!(
            debrid_release_scoped_targets(&release, &targets, &[rejected.clone()], &[]).len(),
            0,
            "rejected coverage must not broaden a future retry to the whole subscription"
        );
        release.selected_candidate = Some(json!({
            "requestScopeEvidence": {
                "targetIds": [targets[1].target_id],
                "targetKeys": [targets[1].target_key.clone()]
            }
        }));
        let evidence_scoped =
            debrid_release_scoped_targets(&release, &targets, &[rejected.clone()], &[]);
        assert_eq!(
            evidence_scoped
                .iter()
                .map(|target| target.target_id)
                .collect::<Vec<_>>(),
            vec![targets[1].target_id]
        );
        release.selected_candidate = Some(json!({
            "requestScopeEvidence": {
                "targetIds": [targets[0].target_id, targets[1].target_id],
                "targetKeys": [targets[0].target_key.clone()]
            }
        }));
        assert!(
            debrid_release_scoped_targets(&release, &targets, &[], &[]).is_empty(),
            "partially overlapping Debrid ID/key scope must fail closed"
        );
        release.selected_candidate = Some(json!({
            "requestScopeEvidence": {
                "targetIds": [targets[1].target_id],
                "targetKeys": [targets[1].target_key.clone()]
            }
        }));
        let mut outside_scope = test_coverage(release_id, Uuid::new_v4());
        outside_scope.target_id = targets[0].target_id;
        assert!(
            debrid_release_scoped_targets(&release, &targets, &[outside_scope], &[]).is_empty(),
            "coverage outside authoritative Debrid request scope must fail closed"
        );
        release.selected_candidate = None;
        let bound_scoped = debrid_release_scoped_targets(
            &release,
            &targets,
            &[rejected.clone()],
            &[targets[0].target_id],
        );
        assert_eq!(bound_scoped.len(), 1);
        assert_eq!(bound_scoped[0].target_id, targets[0].target_id);
        assert_eq!(
            debrid_release_scoped_targets(&release, &targets[..1], &[rejected], &[]).len(),
            1,
            "a sole subscription target is the only safe context-free fallback"
        );

        let selected_file_key = "provider-file-1".to_string();
        let plan = AnimeFileCoveragePlan {
            resolver_kind: ReleaseResolverKind::AnimeShokoStyle,
            resolver_version: "alm7-test".to_string(),
            release_kind: ReleaseKind::Single,
            confidence: ReleaseConfidence::High,
            requires_file_list: false,
            requires_file_selection: false,
            selected_file_keys: vec![selected_file_key.clone()],
            entries: vec![
                crate::acquisition::release_resolution::anime::AnimeFileCoverageEntry {
                    target_key: targets[0].target_key.clone(),
                    canonical_key: None,
                    release_file_key: Some(selected_file_key.clone()),
                    file_id: Some(selected_file_key.clone()),
                    file_index: Some(1),
                    path: Some("Show.S01E01.mkv".to_string()),
                    coverage_kind: ReleaseCoverageKind::SingleEpisode,
                    confidence: ReleaseConfidence::High,
                    score: Some(100.0),
                    reason: "alm7_test".to_string(),
                    state: ReleaseCoverageState::Planned,
                },
            ],
            review_reasons: Vec::new(),
            rejection_reasons: Vec::new(),
        };
        let files = vec![AnimeReleaseFileInput {
            file_key: selected_file_key.clone(),
            file_id: Some(selected_file_key),
            file_index: Some(1),
            path: "Show.S01E01.mkv".to_string(),
            size_bytes: Some(2_048),
            selectable: true,
        }];
        let capabilities = test_debrid_inspection(true, Vec::new(), Vec::new(), None).capabilities;
        assert!(!anime_plan_ready_for_automatic_selection(
            &plan,
            &files,
            &capabilities,
            &targets,
        ));
        assert!(anime_plan_ready_for_automatic_selection(
            &plan,
            &files,
            &capabilities,
            &scoped,
        ));
        let mut implicit_selection_capabilities = capabilities.clone();
        implicit_selection_capabilities.supports_file_selection = false;
        assert!(
            anime_plan_ready_for_automatic_selection(
                &plan,
                &files,
                &implicit_selection_capabilities,
                &scoped,
            ),
            "an exact one-media-file mapping needs no provider selection operation"
        );
        let mut overfetching_files = files.clone();
        overfetching_files.push(AnimeReleaseFileInput {
            file_key: "provider-file-2".to_string(),
            file_id: Some("provider-file-2".to_string()),
            file_index: Some(2),
            path: "Show.S01E02.mkv".to_string(),
            size_bytes: Some(2_048),
            selectable: true,
        });
        assert!(
            !anime_plan_ready_for_automatic_selection(
                &plan,
                &overfetching_files,
                &implicit_selection_capabilities,
                &scoped,
            ),
            "excluding provider media still requires file-selection capability"
        );

        targets[0].metadata = Some(json!({ "graphFingerprint": "graph-a" }));
        targets[1].metadata = Some(json!({ "graphFingerprint": "graph-b" }));
        assert!(
            anime_scoring_context_from_release(&release, &targets)
                .graph_fingerprint
                .is_none(),
            "conflicting scoped graph identities must fail closed"
        );
        Ok(())
    }

    #[tokio::test]
    async fn alm7_debrid_model_failure_keeps_exact_deterministic_fallback_automatic() -> Result<()>
    {
        let database = setup_db().await?;
        let (provider_id, instance_id) = create_provider_refs(&database.pool).await?;
        let subscription_id = create_anime_subscription_with_target(
            &database.pool,
            "Tokyo Ghoul",
            "Tokyo Ghoul Root A",
            "S02E01",
            2,
            1,
            13,
        )
        .await?;
        let actual_path = "Tokyo Ghoul Root A/[Group] TGRA - 13.mkv";
        let adapter = FakeDebridAdapter::with_files(vec![DebridRemoteFile {
            provider_file_id: "actual-file-42".to_string(),
            file_index: Some(42),
            path: actual_path.to_string(),
            basename: "[Group] TGRA - 13.mkv".to_string(),
            size_bytes: Some(2_048),
            selectable: true,
            selected: Some(false),
            raw: None,
        }]);
        let engine = FakeAnimeMatchEngine::new(FakeAnimeMatchBehavior::EngineError);
        let service = engine.service();
        let source = "magnet:?xt=urn:btih:2222222222222222222222222222222222222222";

        let job_id = submit_debrid_with_adapter_and_anime_matching(
            &database.pool,
            provider_id,
            instance_id,
            source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("anime"),
                name: Some("[Group] TGRA - 13"),
                paused: false,
                release_context: Some(DebridReleaseSubmitContext {
                    subscription_id: Some(subscription_id),
                    source_provider_id: Some(provider_id),
                    source_extension_id: "test.source".to_string(),
                    media_type: MediaType::Anime,
                    title: "Tokyo Ghoul".to_string(),
                    release_title: "[Group] TGRA - 13".to_string(),
                    info_hash: None,
                    fingerprint: Some("alm7-model-failure".to_string()),
                    score: Some(95.0),
                    selected_candidate: Some(json!({
                        "title": "[Group] TGRA - 13",
                        "source": source,
                        "sourceKind": "magnet"
                    })),
                }),
            },
            &service,
            &adapter,
        )
        .await?;

        assert_eq!(engine.calls.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(
            engine.observed_paths.lock().unwrap().as_slice(),
            [actual_path]
        );
        let job = load_debrid_job(&database.pool, job_id)
            .await?
            .context("ALM-7 fallback Debrid job should load")?;
        let automatic_retry = anime_debrid_retry_disposition_from_job(&job)
            .context("ALM-7 immediate submit should stage its automatic retry disposition")?;
        assert!(!automatic_retry.suppress_automatic_rediscovery);
        stage_anime_debrid_retry_disposition(&database.pool, job_id, &automatic_retry).await?;
        let restaged_job = load_debrid_job(&database.pool, job_id)
            .await?
            .context("ALM-7 restaged Debrid job")?;
        assert_eq!(
            anime_debrid_retry_disposition_from_job(&restaged_job),
            Some(automatic_retry.clone()),
            "the durable immediate-submit disposition must be idempotent"
        );
        assert!(job.selected_file_ids.is_empty());
        assert_ne!(job.status, "review_required");
        assert!(job.selection_error.is_none());

        let release = get_release(&database.pool, job.release_id.context("release id")?)
            .await?
            .context("ALM-7 fallback release should load")?;
        assert_eq!(release.state, AcquisitionReleaseState::Staging);
        assert_ne!(release.state, AcquisitionReleaseState::ReviewRequired);
        assert!(
            !release
                .state_reason
                .as_deref()
                .unwrap_or_default()
                .to_ascii_lowercase()
                .contains("review")
        );
        assert_eq!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.pointer("/animeMatchAssist/source"))
                .and_then(Value::as_str),
            Some("deterministic_fallback")
        );
        assert_eq!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.pointer("/animeMatchAssist/reason"))
                .and_then(Value::as_str),
            Some("engine_error")
        );
        assert_eq!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.pointer("/automaticResolution/status"))
                .and_then(Value::as_str),
            Some("pending")
        );
        assert!(
            adapter
                .state
                .lock()
                .unwrap()
                .releases
                .get("fake-release-1")
                .context("fake release")?
                .selected_file_ids
                .is_empty()
        );

        // The synchronous submit path intentionally leaves the release staged
        // until automation has persisted its submission bookkeeping. The
        // materializer then rejects this one unsafe candidate and returns the
        // scoped target to the normal scheduler instead of invoking the model
        // again on every Debrid poll.
        let inspection = adapter.inspect_release("fake-release-1").await?;
        let target = list_subscription_targets(&database.pool, subscription_id)
            .await?
            .into_iter()
            .next()
            .context("ALM-7 target")?;
        update_target_state(
            &database.pool,
            target.target_id,
            AcquisitionTargetStateUpdate {
                state: AcquisitionTargetState::Submitted,
                state_reason: Some("test Debrid submission".to_string()),
                selected_provider_id: Some(provider_id),
                selected_route_logical_id: Some(DEBRID_DEFAULT_LOGICAL_ID.to_string()),
                selected_candidate: Some(json!({ "title": "[Group] TGRA - 13" })),
                download_id: Some(job_id.to_string()),
                import_event_id: None,
                next_search_after: None,
                increment_search_attempts: false,
            },
        )
        .await?;
        let retry_started_at = chrono::Utc::now();
        persist_anime_debrid_retry_with_adapter(
            &database.pool,
            &adapter,
            job_id,
            &release,
            &inspection.release.remote_release_id,
            &inspection.release.provider_implementation,
            &automatic_retry,
        )
        .await?;

        let job = load_debrid_job(&database.pool, job_id)
            .await?
            .context("ALM-7 retried Debrid job")?;
        assert_eq!(job.status, "failed");
        assert_ne!(job.status, "review_required");
        let release = get_release(&database.pool, release.release_id)
            .await?
            .context("ALM-7 rejected release")?;
        assert_eq!(release.state, AcquisitionReleaseState::Failed);
        assert_ne!(release.state, AcquisitionReleaseState::ReviewRequired);
        assert_eq!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.pointer("/automaticRetry/status"))
                .and_then(Value::as_str),
            Some("scheduled")
        );
        assert_eq!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|plan| { plan.pointer("/retrySuppression/suppressAutomaticRediscovery") })
                .and_then(Value::as_bool),
            Some(false)
        );
        assert_eq!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.pointer("/retrySuppression/status"))
                .and_then(Value::as_str),
            Some("retryable")
        );
        assert_eq!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.pointer("/automaticRetry/providerCleanup/status"))
                .and_then(Value::as_str),
            Some("deleted")
        );
        let target = list_subscription_targets(&database.pool, subscription_id)
            .await?
            .into_iter()
            .next()
            .context("ALM-7 reset target")?;
        assert_eq!(target.state, AcquisitionTargetState::Pending);
        assert!(
            target
                .next_search_after
                .is_some_and(|retry_at| retry_at > retry_started_at)
        );
        assert!(target.selected_provider_id.is_none());
        assert!(target.selected_route_logical_id.is_none());
        assert!(target.selected_candidate.is_none());
        assert!(target.download_id.is_none());
        assert!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.pointer("/debridFailure/responsePolicy"))
                .is_none()
        );
        assert!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.pointer("/debridFailure/fallbackState"))
                .is_none()
        );
        assert_eq!(engine.calls.load(AtomicOrdering::SeqCst), 1);
        assert!(
            !adapter
                .state
                .lock()
                .unwrap()
                .releases
                .contains_key("fake-release-1")
        );

        // A crash may replay the disposition after remote cleanup or target
        // reset. Every persistence step remains idempotent, and terminalizing
        // the provider job last makes such a replay safe.
        persist_anime_debrid_retry(
            &database.pool,
            job_id,
            &release,
            &inspection.release.remote_release_id,
            &automatic_retry,
            json!({
                "status": "adapter_unavailable",
                "deleted": false,
                "error": "simulated provider removal after restart"
            }),
        )
        .await?;
        let replayed_release = get_release(&database.pool, release.release_id)
            .await?
            .context("ALM-7 replayed release")?;
        assert_eq!(replayed_release.state, AcquisitionReleaseState::Failed);
        assert_eq!(
            replayed_release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.pointer("/automaticRetry/providerCleanup/status"))
                .and_then(Value::as_str),
            Some("adapter_unavailable")
        );
        assert_eq!(engine.calls.load(AtomicOrdering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn alm7_debrid_fallback_matrix_persists_one_zero_review_retry_decision() -> Result<()> {
        let database = setup_db().await?;
        let (provider_id, instance_id) = create_provider_refs(&database.pool).await?;
        let subscription_id = create_anime_subscription_with_target(
            &database.pool,
            "Tokyo Ghoul",
            "Tokyo Ghoul Root A",
            "S02E01",
            2,
            1,
            13,
        )
        .await?;
        let cases = [
            (
                "disabled",
                None,
                AnimeMatchFallbackReason::EngineUnavailable,
            ),
            (
                "timeout",
                Some(FakeAnimeMatchBehavior::EngineTimeout),
                AnimeMatchFallbackReason::EngineError,
            ),
            (
                "empty",
                Some(FakeAnimeMatchBehavior::Empty),
                AnimeMatchFallbackReason::EmptyModelMatches,
            ),
            (
                "invalid_output",
                Some(FakeAnimeMatchBehavior::InvalidOutput),
                AnimeMatchFallbackReason::InvalidModelResponse,
            ),
            (
                "invalid_reference",
                Some(FakeAnimeMatchBehavior::UnknownFile),
                AnimeMatchFallbackReason::InvalidModelResponse,
            ),
        ];

        for (index, (label, behavior, expected_reason)) in cases.into_iter().enumerate() {
            let adapter = FakeDebridAdapter::with_files(vec![DebridRemoteFile {
                provider_file_id: "actual-file-42".to_string(),
                file_index: Some(42),
                path: "Tokyo Ghoul Root A/[Group] TGRA - 13.mkv".to_string(),
                basename: "[Group] TGRA - 13.mkv".to_string(),
                size_bytes: Some(2_048),
                selectable: true,
                selected: Some(false),
                raw: None,
            }]);
            let engine = behavior.map(FakeAnimeMatchEngine::new);
            let service = engine
                .as_ref()
                .map(FakeAnimeMatchEngine::service)
                .unwrap_or_else(AnimeMatchingService::disabled);
            let info_hash = format!("{:040x}", index + 10);
            let source = format!("magnet:?xt=urn:btih:{info_hash}");
            let job_id = submit_debrid_with_adapter_and_anime_matching(
                &database.pool,
                provider_id,
                instance_id,
                &source,
                DebridSubmitOptions {
                    owner_id: "test.source",
                    category: Some("anime"),
                    name: Some("[Group] TGRA - 13"),
                    paused: false,
                    release_context: Some(DebridReleaseSubmitContext {
                        subscription_id: Some(subscription_id),
                        source_provider_id: Some(provider_id),
                        source_extension_id: "test.source".to_string(),
                        media_type: MediaType::Anime,
                        title: "Tokyo Ghoul".to_string(),
                        release_title: "[Group] TGRA - 13".to_string(),
                        info_hash: Some(info_hash),
                        fingerprint: Some(format!("alm7-fallback-{label}")),
                        score: Some(95.0),
                        selected_candidate: Some(json!({
                            "title": "[Group] TGRA - 13",
                            "source": source.clone(),
                            "sourceKind": "magnet"
                        })),
                    }),
                },
                &service,
                &adapter,
            )
            .await?;

            if let Some(engine) = engine.as_ref() {
                assert_eq!(
                    engine.calls.load(AtomicOrdering::SeqCst),
                    1,
                    "{label} should invoke the model exactly once"
                );
            }
            let job = load_debrid_job(&database.pool, job_id)
                .await?
                .with_context(|| format!("{label} Debrid job"))?;
            let retry = anime_debrid_retry_disposition_from_job(&job)
                .with_context(|| format!("{label} durable retry disposition"))?;
            assert!(!retry.suppress_automatic_rediscovery, "{label}");
            let release = get_release(&database.pool, job.release_id.context("release id")?)
                .await?
                .with_context(|| format!("{label} release"))?;
            assert_eq!(release.state, AcquisitionReleaseState::Staging, "{label}");
            assert_ne!(
                release.state,
                AcquisitionReleaseState::ReviewRequired,
                "{label}"
            );
            let release_plan = release.coverage_plan.as_ref().context("release plan")?;
            let retry_plan = retry.coverage_plan.as_ref().context("retry plan")?;
            let expected_reason = serde_json::to_value(expected_reason)?;
            assert_eq!(
                release_plan
                    .pointer("/animeMatchAssist/reason")
                    .and_then(Value::as_str),
                expected_reason.as_str(),
                "{label}"
            );
            assert_eq!(
                retry_plan.pointer("/anime"),
                release_plan.pointer("/anime"),
                "{label} must durably preserve the exact deterministic fallback"
            );
            assert!(!anime_automatic_evidence_has_review_semantics(release_plan));
            assert!(!anime_automatic_evidence_has_review_semantics(retry_plan));
            assert!(
                list_release_coverage(&database.pool, release.release_id)
                    .await?
                    .iter()
                    .all(|entry| entry.state != ReleaseCoverageState::ReviewRequired),
                "{label}"
            );

            let remote_release_id = job
                .remote_release_id
                .as_deref()
                .context("remote release id")?;
            persist_anime_debrid_retry_with_adapter(
                &database.pool,
                &adapter,
                job_id,
                &release,
                remote_release_id,
                adapter.implementation(),
                &retry,
            )
            .await?;
            let release = get_release(&database.pool, release.release_id)
                .await?
                .context("terminal retry release")?;
            assert_eq!(release.state, AcquisitionReleaseState::Failed, "{label}");
            assert_eq!(
                release
                    .coverage_plan
                    .as_ref()
                    .and_then(|plan| plan.pointer("/retrySuppression/status"))
                    .and_then(Value::as_str),
                Some("retryable"),
                "{label}"
            );
            assert_eq!(
                release
                    .coverage_plan
                    .as_ref()
                    .and_then(|plan| {
                        plan.pointer("/retrySuppression/suppressAutomaticRediscovery")
                    })
                    .and_then(Value::as_bool),
                Some(false),
                "{label}"
            );
            assert!(!anime_automatic_evidence_has_review_semantics(
                release.coverage_plan.as_ref().context("terminal plan")?
            ));
            assert_eq!(
                engine
                    .as_ref()
                    .map(|engine| engine.calls.load(AtomicOrdering::SeqCst))
                    .unwrap_or(0),
                usize::from(behavior.is_some()),
                "{label} must not invoke the model again while consuming retry"
            );
        }
        Ok(())
    }

    #[test]
    fn alm7_debrid_retry_suppression_distinguishes_runtime_from_candidate_failures() {
        let provenance = |reason| AnimeMatchAssistProvenance {
            source: AnimeMatchAssistSource::DeterministicFallback,
            result: AnimeMatchAssistResult::Fallback,
            matcher_schema_version: ANIME_MATCH_SCHEMA_VERSION,
            request_fingerprint: None,
            reason: Some(reason),
            detail: None,
            runtime: None,
            latency_ms: 0,
        };
        for reason in [
            AnimeMatchFallbackReason::EngineUnavailable,
            AnimeMatchFallbackReason::EngineError,
            AnimeMatchFallbackReason::InvalidRequest,
            AnimeMatchFallbackReason::InvalidModelResponse,
            AnimeMatchFallbackReason::EmptyModelMatches,
            AnimeMatchFallbackReason::CoverageValidationFailed,
        ] {
            assert!(!anime_debrid_retry_suppresses_rediscovery(&provenance(
                reason
            )));
        }
        assert!(anime_debrid_retry_suppresses_rediscovery(
            &AnimeMatchAssistProvenance {
                source: AnimeMatchAssistSource::LocalModel,
                result: AnimeMatchAssistResult::Matched,
                matcher_schema_version: ANIME_MATCH_SCHEMA_VERSION,
                request_fingerprint: None,
                reason: None,
                detail: None,
                runtime: None,
                latency_ms: 0,
            }
        ));
    }

    #[tokio::test]
    async fn alm7_debrid_ready_selection_replays_provider_application_after_restart() -> Result<()>
    {
        let selected = vec!["provider-file-42".to_string()];
        assert!(debrid_provider_selection_needs_application(
            &selected,
            AcquisitionReleaseState::Ready,
        ));
        assert!(!debrid_provider_selection_needs_application(
            &selected,
            AcquisitionReleaseState::Staging,
        ));
        assert!(debrid_provider_selection_needs_application(
            &[],
            AcquisitionReleaseState::Staging,
        ));

        let mut inspection = test_debrid_inspection(
            true,
            Vec::new(),
            Vec::new(),
            Some(DebridFileSelection {
                mode: DebridFileSelectionMode::BeforeTransfer,
                selected_file_ids: selected.clone(),
                skipped_file_ids: Vec::new(),
            }),
        );
        assert!(debrid_inspection_confirms_provider_selection_applied(
            &inspection,
            &selected,
        ));
        inspection
            .selection
            .as_mut()
            .expect("selection")
            .selected_file_ids = vec!["different-file".to_string()];
        assert!(!debrid_inspection_confirms_provider_selection_applied(
            &inspection,
            &selected,
        ));
        inspection.selection = None;
        inspection.release.status = DebridReleaseStatus::Transferring;
        assert!(debrid_inspection_confirms_provider_selection_applied(
            &inspection,
            &selected,
        ));

        let database = setup_db().await?;
        let adapter = FakeDebridAdapter::with_files(vec![fake_ready_anime_file()]);
        let fixture = setup_owned_ready_anime_debrid_selection(
            &database.pool,
            &adapter,
            "alm7-ready-selection-provider-replay",
        )
        .await?;
        let job = load_debrid_job(&database.pool, fixture.job_id)
            .await?
            .context("persisted Ready replay job")?;
        let release = get_release(&database.pool, fixture.release_id)
            .await?
            .context("persisted Ready replay release")?;
        let inspection = adapter.inspect_release(&fixture.remote_release_id).await?;
        assert_eq!(inspection.release.status, DebridReleaseStatus::WaitingFiles);
        assert!(
            inspection
                .selection
                .as_ref()
                .is_none_or(|selection| selection.selected_file_ids.is_empty())
        );

        assert!(
            replay_ready_anime_debrid_provider_selection(
                &database.pool,
                &adapter,
                &job,
                &release,
                &inspection,
            )
            .await?
        );

        let fake_state = adapter.state.lock().unwrap();
        let provider_release = fake_state
            .releases
            .get(&fixture.remote_release_id)
            .context("replayed provider release")?;
        assert_eq!(
            provider_release.selected_file_ids,
            vec![fixture.provider_file_id.clone()],
            "the worker must issue the persisted exact provider selection"
        );
        drop(fake_state);
        let applied_release = get_release(&database.pool, fixture.release_id)
            .await?
            .context("provider-applied anime release")?;
        assert_eq!(applied_release.state, AcquisitionReleaseState::Downloading);
        assert_eq!(
            applied_release.state_reason.as_deref(),
            Some("Debrid provider accepted deterministic file selection.")
        );
        let applied_job = load_debrid_job(&database.pool, fixture.job_id)
            .await?
            .context("provider-applied anime job")?;
        assert_eq!(applied_job.status, "debrid_downloading");
        assert_ne!(applied_job.status, "review_required");
        let release_jobs = crate::acquisition::release_resolution::store::list_release_jobs(
            &database.pool,
            fixture.release_id,
        )
        .await?;
        assert_eq!(release_jobs.len(), 1);
        assert_eq!(release_jobs[0].state, ReleaseJobState::Downloading);
        assert!(release_jobs[0].active);
        Ok(())
    }

    #[tokio::test]
    async fn alm7_anime_ready_select_files_failure_retries_without_review() -> Result<()> {
        let state = setup_debrid_test_state().await?;
        let store = ExtensionStore::new(&state.db_pool);
        let mut adapter = FakeDebridAdapter::with_files(vec![fake_ready_anime_file()]);
        adapter.fail_select = true;
        let fixture = setup_owned_ready_anime_debrid_selection(
            &state.db_pool,
            &adapter,
            "alm7-ready-select-files-failure",
        )
        .await?;
        let job = load_debrid_job(&state.db_pool, fixture.job_id)
            .await?
            .context("Ready selection-failure job")?;
        let release = get_release(&state.db_pool, fixture.release_id)
            .await?
            .context("Ready selection-failure release")?;
        let inspection = adapter.inspect_release(&fixture.remote_release_id).await?;
        let error = replay_ready_anime_debrid_provider_selection(
            &state.db_pool,
            &adapter,
            &job,
            &release,
            &inspection,
        )
        .await
        .expect_err("provider selection replay should fail");
        assert!(
            error
                .to_string()
                .contains("replaying persisted anime Debrid selection")
        );

        handle_debrid_job_processing_result(&state, &store, &job, Err(error)).await?;

        let failed_job = load_debrid_job(&state.db_pool, fixture.job_id)
            .await?
            .context("automatic selection-failure job")?;
        assert_eq!(failed_job.status, "failed");
        assert_ne!(failed_job.status, "review_required");
        assert!(failed_job.selection_error.is_none());
        let failed_release = get_release(&state.db_pool, fixture.release_id)
            .await?
            .context("automatic selection-failure release")?;
        assert_eq!(failed_release.state, AcquisitionReleaseState::Failed);
        let coverage_plan = failed_release
            .coverage_plan
            .as_ref()
            .context("automatic selection-failure evidence")?;
        assert_eq!(
            coverage_plan
                .pointer("/automaticResolutionError/reason")
                .and_then(Value::as_str),
            Some("anime_debrid_worker_error")
        );
        assert_eq!(
            coverage_plan
                .pointer("/automaticRetry/status")
                .and_then(Value::as_str),
            Some("scheduled")
        );
        assert!(!anime_evidence_has_nonempty_review_outcome(coverage_plan));
        assert!(
            !failed_release
                .state_reason
                .as_deref()
                .unwrap_or_default()
                .to_ascii_lowercase()
                .contains("review")
        );
        assert_no_anime_review_lane_artifacts(&state.db_pool, &failed_release).await?;
        let release_jobs = crate::acquisition::release_resolution::store::list_release_jobs(
            &state.db_pool,
            fixture.release_id,
        )
        .await?;
        assert_eq!(release_jobs.len(), 1);
        assert_eq!(release_jobs[0].state, ReleaseJobState::Failed);
        assert!(!release_jobs[0].active);
        assert!(
            !release_jobs[0]
                .state_reason
                .as_deref()
                .unwrap_or_default()
                .to_ascii_lowercase()
                .contains("review")
        );
        let target = get_target(&state.db_pool, fixture.target_id)
            .await?
            .context("automatic selection-failure target")?;
        assert_eq!(target.state, AcquisitionTargetState::Pending);
        assert!(target.download_id.is_none());
        assert!(target.next_search_after.is_some());
        let coverage = list_release_coverage(&state.db_pool, fixture.release_id).await?;
        assert!(
            coverage
                .iter()
                .all(|entry| entry.state == ReleaseCoverageState::Rejected)
        );
        Ok(())
    }

    #[tokio::test]
    async fn alm7_anime_materialization_error_handler_retries_without_review() -> Result<()> {
        let state = setup_debrid_test_state().await?;
        let store = ExtensionStore::new(&state.db_pool);
        let selection_adapter = FakeDebridAdapter::with_files(vec![fake_ready_anime_file()]);
        let fixture = setup_owned_ready_anime_debrid_selection(
            &state.db_pool,
            &selection_adapter,
            "alm7-materialization-processing-failure",
        )
        .await?;
        let ready_job = load_debrid_job(&state.db_pool, fixture.job_id)
            .await?
            .context("materialization Ready job")?;
        let ready_release = get_release(&state.db_pool, fixture.release_id)
            .await?
            .context("materialization Ready release")?;
        let inspection = selection_adapter
            .inspect_release(&fixture.remote_release_id)
            .await?;
        assert!(
            replay_ready_anime_debrid_provider_selection(
                &state.db_pool,
                &selection_adapter,
                &ready_job,
                &ready_release,
                &inspection,
            )
            .await?
        );
        let job = load_debrid_job(&state.db_pool, fixture.job_id)
            .await?
            .context("provider-selected materialization job")?;
        assert_eq!(job.status, "debrid_downloading");
        assert!(!job.links.is_empty());
        let paths = RuntimePaths::from_roots(
            &state.settings.extensions.storage_root,
            &state.settings.library.local_root,
        );
        let materialization_adapter = FakeDebridAdapter::failing_unrestrict();
        let error = materialize_debrid_links(&state, &materialization_adapter, &paths, &job)
            .await
            .expect_err("provider unrestrict should fail during materialization");
        assert!(error.to_string().contains("unrestrict failed"));

        handle_debrid_job_processing_result(&state, &store, &job, Err(error)).await?;

        let failed_job = load_debrid_job(&state.db_pool, fixture.job_id)
            .await?
            .context("automatic materialization-failure job")?;
        assert_eq!(failed_job.status, "failed");
        assert_ne!(failed_job.status, "review_required");
        let failed_release = get_release(&state.db_pool, fixture.release_id)
            .await?
            .context("automatic materialization-failure release")?;
        assert_eq!(failed_release.state, AcquisitionReleaseState::Failed);
        let coverage_plan = failed_release
            .coverage_plan
            .as_ref()
            .context("automatic materialization-failure evidence")?;
        assert_eq!(
            coverage_plan
                .pointer("/automaticResolutionError/reason")
                .and_then(Value::as_str),
            Some("anime_debrid_worker_error")
        );
        assert_eq!(
            coverage_plan
                .pointer("/automaticRetry/status")
                .and_then(Value::as_str),
            Some("scheduled")
        );
        assert!(!anime_evidence_has_nonempty_review_outcome(coverage_plan));
        assert!(
            !failed_release
                .state_reason
                .as_deref()
                .unwrap_or_default()
                .to_ascii_lowercase()
                .contains("review")
        );
        assert_no_anime_review_lane_artifacts(&state.db_pool, &failed_release).await?;
        let release_jobs = crate::acquisition::release_resolution::store::list_release_jobs(
            &state.db_pool,
            fixture.release_id,
        )
        .await?;
        assert_eq!(release_jobs.len(), 1);
        assert_eq!(release_jobs[0].state, ReleaseJobState::Failed);
        assert!(!release_jobs[0].active);
        assert!(
            !release_jobs[0]
                .state_reason
                .as_deref()
                .unwrap_or_default()
                .to_ascii_lowercase()
                .contains("review")
        );
        let target = get_target(&state.db_pool, fixture.target_id)
            .await?
            .context("automatic materialization-failure target")?;
        assert_eq!(target.state, AcquisitionTargetState::Pending);
        assert!(target.download_id.is_none());
        assert!(target.next_search_after.is_some());
        let coverage = list_release_coverage(&state.db_pool, fixture.release_id).await?;
        assert!(
            coverage
                .iter()
                .all(|entry| entry.state == ReleaseCoverageState::Rejected)
        );
        Ok(())
    }

    #[tokio::test]
    async fn alm7_debrid_stale_inspection_cannot_resurrect_retry_or_regress_provider_state()
    -> Result<()> {
        let database = setup_db().await?;
        let (provider_id, instance_id) = create_provider_refs(&database.pool).await?;
        let job_id = Uuid::new_v4();
        insert_debrid_job(
            &database.pool,
            &DebridDownloadJob {
                job_id,
                provider_id,
                instance_id,
                owner_id: "test.source".to_string(),
                source: "magnet:?xt=urn:btih:9999999999999999999999999999999999999999".to_string(),
                source_kind: "magnet".to_string(),
                category: Some("anime".to_string()),
                display_name: Some("Stale Inspection Anime".to_string()),
                remote_torrent_id: Some("fake-release-1".to_string()),
                remote_download_id: None,
                provider_implementation: Some("fake_debrid".to_string()),
                remote_release_id: Some("fake-release-1".to_string()),
                remote_release_status: Some("failed".to_string()),
                provider_capabilities: None,
                provider_status: Some(json!({
                    "animeAutomaticRetryConsumed": { "status": "consumed" },
                    "sentinel": "must-survive-stale-inspection"
                })),
                selection_mode: Some("before_transfer".to_string()),
                selected_file_ids: Vec::new(),
                skipped_file_ids: Vec::new(),
                selection_error: None,
                release_id: None,
                status: "anime_retry_pending".to_string(),
                local_path: None,
                links: Vec::new(),
                progress: Some(0.0),
                downloaded_bytes: Some(0),
                total_bytes: None,
                download_rate_bps: None,
                last_error: None,
            },
        )
        .await?;
        let stale = test_debrid_inspection(true, Vec::new(), Vec::new(), None);
        assert!(!update_debrid_job_from_inspection(&database.pool, job_id, &stale).await?);
        let preserved = load_debrid_job(&database.pool, job_id)
            .await?
            .context("preserved retry-pending job")?;
        assert_eq!(preserved.status, "anime_retry_pending");
        assert_eq!(
            preserved
                .provider_status
                .as_ref()
                .and_then(|status| status.get("sentinel"))
                .and_then(Value::as_str),
            Some("must-survive-stale-inspection")
        );

        sqlx::query::<sqlx::Any>(
            "UPDATE debrid_download_jobs
             SET status = 'debrid_downloaded', remote_release_status = 'downloaded'
             WHERE job_id = $1",
        )
        .bind(job_id.to_string())
        .execute(&database.pool)
        .await?;
        assert!(!update_debrid_job_from_inspection(&database.pool, job_id, &stale).await?);
        let preserved = load_debrid_job(&database.pool, job_id)
            .await?
            .context("non-regressed downloaded job")?;
        assert_eq!(
            preserved.remote_release_status.as_deref(),
            Some("downloaded")
        );
        assert_eq!(preserved.status, "debrid_downloaded");
        Ok(())
    }

    #[tokio::test]
    async fn alm7_debrid_delayed_retry_does_not_clobber_newer_attempt() -> Result<()> {
        let database = setup_db().await?;
        let (provider_id, instance_id) = create_provider_refs(&database.pool).await?;
        let subscription_id = create_anime_subscription_with_target(
            &database.pool,
            "Tokyo Ghoul",
            "Tokyo Ghoul Root A",
            "S02E01",
            2,
            1,
            13,
        )
        .await?;
        let target = list_subscription_targets(&database.pool, subscription_id)
            .await?
            .into_iter()
            .next()
            .context("stale-retry target")?;
        let job_a_id = Uuid::new_v4();
        let job_b_id = Uuid::new_v4();
        let job_a_string = job_a_id.to_string();
        let job_b_string = job_b_id.to_string();

        let mut release_a = test_debrid_release(ReleaseKind::Single, ReleaseConfidence::High);
        release_a.subscription_id = Some(subscription_id);
        release_a.source_provider_id = Some(provider_id);
        release_a.media_type = MediaType::Anime;
        release_a.title = "Tokyo Ghoul".to_string();
        release_a.release_title = "[Group] Tokyo Ghoul Root A - 01".to_string();
        release_a.fingerprint = format!("alm7-debrid-attempt-claim-{}", Uuid::new_v4());
        release_a.selected_provider_id = Some(provider_id);
        release_a.download_id = Some(job_a_string.clone());
        release_a.remote_release_id = Some("remote-a".to_string());
        release_a.state = AcquisitionReleaseState::Submitted;
        release_a.coverage_plan = Some(json!({ "attempt": "a" }));
        let release_a = insert_test_release(&database.pool, &release_a).await?;

        let job_a = DebridDownloadJob {
            job_id: job_a_id,
            provider_id,
            instance_id,
            owner_id: "test.source".to_string(),
            source: release_a.source.clone(),
            source_kind: "magnet".to_string(),
            category: Some("anime".to_string()),
            display_name: Some(release_a.release_title.clone()),
            remote_torrent_id: Some("remote-a".to_string()),
            remote_download_id: None,
            provider_implementation: Some("fake_debrid".to_string()),
            remote_release_id: Some("remote-a".to_string()),
            remote_release_status: Some("submitted".to_string()),
            provider_capabilities: None,
            provider_status: None,
            selection_mode: Some("before_transfer".to_string()),
            selected_file_ids: vec!["file-a".to_string()],
            skipped_file_ids: Vec::new(),
            selection_error: None,
            release_id: Some(release_a.release_id),
            status: "submitted".to_string(),
            local_path: None,
            links: Vec::new(),
            progress: Some(0.0),
            downloaded_bytes: Some(0),
            total_bytes: Some(2_048),
            download_rate_bps: None,
            last_error: None,
        };
        insert_debrid_job(&database.pool, &job_a).await?;
        upsert_debrid_release_job(
            &database.pool,
            &release_a,
            provider_id,
            job_a_id,
            Some("remote-a"),
            ReleaseJobState::Submitted,
            "attempt A submitted",
        )
        .await?;
        update_target_state(
            &database.pool,
            target.target_id,
            AcquisitionTargetStateUpdate {
                state: AcquisitionTargetState::Submitted,
                state_reason: Some("attempt A submitted".to_string()),
                selected_provider_id: Some(provider_id),
                selected_route_logical_id: Some(DEBRID_DEFAULT_LOGICAL_ID.to_string()),
                selected_candidate: Some(json!({ "attempt": "a" })),
                download_id: Some(job_a_string.clone()),
                ..Default::default()
            },
        )
        .await?;
        let mut file = test_release_file(
            release_a.release_id,
            "file-a",
            "Tokyo Ghoul Root A/[Group] Tokyo Ghoul Root A - 01.mkv",
            true,
        );
        file.selected = Some(true);
        let file = insert_test_release_file(&database.pool, &file).await?;
        let coverage = upsert_release_coverage(
            &database.pool,
            NewAcquisitionReleaseCoverage {
                coverage_id: None,
                release_id: release_a.release_id,
                release_file_id: Some(file.release_file_id),
                target_id: target.target_id,
                coverage_kind: ReleaseCoverageKind::SingleEpisode,
                confidence: ReleaseConfidence::High,
                score: Some(100.0),
                reason: Some("attempt A exact mapping".to_string()),
                state: ReleaseCoverageState::Selected,
                verified_by: Some("attempt_a".to_string()),
            },
        )
        .await?;
        let retry = AnimeDebridAutomaticRetry {
            target_ids: vec![target.target_id],
            reason_code: "anime_debrid_delayed_attempt_a".to_string(),
            suppress_automatic_rediscovery: false,
            coverage_plan: Some(json!({ "attempt": "a-retry" })),
        };
        stage_anime_debrid_retry_disposition(&database.pool, job_a_id, &retry).await?;

        // The scheduler completed a newer bind before delayed job A resumed.
        // Every shared record now belongs to B.
        sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_release_jobs
             SET state = 'cancelled', active = 0, completed_at = CURRENT_TIMESTAMP
             WHERE release_id = $1 AND download_id = $2",
        )
        .bind(release_a.release_id.to_string())
        .bind(&job_a_string)
        .execute(&database.pool)
        .await?;
        let attempt_b_plan = json!({
            "attempt": "b",
            "sentinel": "must-survive-stale-attempt-a"
        });
        sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_releases
             SET download_id = $1,
                 remote_release_id = 'remote-b',
                 state = 'submitted',
                 state_reason = 'attempt B submitted',
                 coverage_plan_json = $2,
                 updated_at = CURRENT_TIMESTAMP
             WHERE release_id = $3",
        )
        .bind(&job_b_string)
        .bind(attempt_b_plan.to_string())
        .bind(release_a.release_id.to_string())
        .execute(&database.pool)
        .await?;
        let release_b = get_release(&database.pool, release_a.release_id)
            .await?
            .context("attempt B release")?;
        upsert_debrid_release_job(
            &database.pool,
            &release_b,
            provider_id,
            job_b_id,
            Some("remote-b"),
            ReleaseJobState::Submitted,
            "attempt B submitted",
        )
        .await?;
        let mut job_b = job_a.clone();
        job_b.job_id = job_b_id;
        job_b.remote_torrent_id = Some("remote-b".to_string());
        job_b.remote_release_id = Some("remote-b".to_string());
        job_b.provider_status = Some(json!({ "attempt": "b" }));
        insert_debrid_job(&database.pool, &job_b).await?;
        update_target_state(
            &database.pool,
            target.target_id,
            AcquisitionTargetStateUpdate {
                state: AcquisitionTargetState::Submitted,
                state_reason: Some("attempt B submitted".to_string()),
                selected_provider_id: Some(provider_id),
                selected_route_logical_id: Some(DEBRID_DEFAULT_LOGICAL_ID.to_string()),
                selected_candidate: Some(json!({ "attempt": "b" })),
                download_id: Some(job_b_string.clone()),
                ..Default::default()
            },
        )
        .await?;
        update_release_file_selected(&database.pool, file.release_file_id, true).await?;
        update_release_coverage_review_state(
            &database.pool,
            coverage.coverage_id,
            ReleaseCoverageState::Selected,
            Some("attempt B exact mapping".to_string()),
            Some("attempt_b".to_string()),
        )
        .await?;

        let mut stale_inspection = test_debrid_inspection(
            true,
            vec![DebridRemoteFile {
                provider_file_id: "stale-file-a".to_string(),
                file_index: Some(99),
                path: "Tokyo Ghoul Root A/Stale.Attempt.A.01.mkv".to_string(),
                basename: "Stale.Attempt.A.01.mkv".to_string(),
                size_bytes: Some(4_096),
                selectable: true,
                selected: Some(true),
                raw: None,
            }],
            Vec::new(),
            None,
        );
        stale_inspection.release.remote_release_id = "remote-a".to_string();
        let mut stale_refinement = refinement_from_debrid_status(DebridReleaseStatus::WaitingFiles);
        stale_refinement.state = AcquisitionReleaseState::Ready;
        stale_refinement.coverage_plan = Some(json!({ "attempt": "stale-a-refinement" }));
        assert!(
            commit_anime_debrid_refinement_if_owned(
                &database.pool,
                &release_a,
                provider_id,
                job_a_id,
                &stale_inspection,
                &stale_refinement,
                stale_refinement.coverage_plan.clone(),
            )
            .await?
            .is_none(),
            "stale attempt A must not commit provider files or release state over B"
        );
        let stale_decision = DebridFileSelectionDecision {
            status: DebridSelectionDecisionStatus::Approved,
            selected_file_ids: vec!["file-a".to_string()],
            skipped_file_ids: Vec::new(),
            provider_selection_ids: vec!["file-a".to_string()],
            target_file_selections: vec![DebridTargetFileSelection {
                target_id: target.target_id,
                provider_file_id: "file-a".to_string(),
            }],
            review_reasons: Vec::new(),
            policy_version: DEBRID_SELECTION_POLICY_VERSION.to_string(),
            coverage_fingerprint: "sha256:stale-a".to_string(),
            select_all: false,
            select_all_approved: true,
        };
        assert!(
            !persist_debrid_selection_decision(
                &database.pool,
                job_a_id,
                &release_a,
                std::slice::from_ref(&file),
                std::slice::from_ref(&coverage),
                &stale_decision,
            )
            .await?,
            "stale attempt A must not resurrect its selection intent"
        );
        stale_inspection.release.status = DebridReleaseStatus::Selected;
        assert!(
            !mark_debrid_selection_applied(
                &database.pool,
                &release_a,
                job_a_id,
                &stale_inspection,
            )
            .await?,
            "stale attempt A must not apply provider selection over B"
        );
        assert!(
            !transition_anime_debrid_runtime_if_owned(
                &database.pool,
                job_a_id,
                AnimeDebridRuntimeTransition::Materializing,
                None,
            )
            .await?,
            "stale attempt A must not enter materialization over B"
        );

        let stale_adapter = FakeDebridAdapter::new();
        persist_anime_debrid_retry_with_adapter(
            &database.pool,
            &stale_adapter,
            job_a_id,
            &release_a,
            "remote-a",
            stale_adapter.implementation(),
            &retry,
        )
        .await?;
        assert!(
            stale_adapter
                .state
                .lock()
                .unwrap()
                .deleted_release_ids
                .is_empty(),
            "ownership must be claimed before any stale remote delete"
        );

        persist_anime_debrid_retry(
            &database.pool,
            job_a_id,
            &release_a,
            "remote-a",
            &retry,
            json!({ "status": "deleted_late", "deleted": true }),
        )
        .await?;

        let release_after = get_release(&database.pool, release_a.release_id)
            .await?
            .context("release after stale retry")?;
        assert_eq!(release_after.state, AcquisitionReleaseState::Submitted);
        assert_eq!(
            release_after.download_id.as_deref(),
            Some(job_b_string.as_str())
        );
        assert_eq!(release_after.coverage_plan, Some(attempt_b_plan));
        let target_after = get_target(&database.pool, target.target_id)
            .await?
            .context("target after stale retry")?;
        assert_eq!(target_after.state, AcquisitionTargetState::Submitted);
        assert_eq!(
            target_after.download_id.as_deref(),
            Some(job_b_string.as_str())
        );
        assert_eq!(
            target_after
                .selected_candidate
                .as_ref()
                .and_then(|candidate| candidate.get("attempt"))
                .and_then(Value::as_str),
            Some("b")
        );
        let files_after = list_release_files(&database.pool, release_a.release_id).await?;
        assert_eq!(files_after.len(), 1);
        assert_eq!(files_after[0].selected, Some(true));
        let coverage_after = list_release_coverage(&database.pool, release_a.release_id).await?;
        assert_eq!(coverage_after.len(), 1);
        assert_eq!(coverage_after[0].state, ReleaseCoverageState::Selected);
        assert_eq!(coverage_after[0].verified_by.as_deref(), Some("attempt_b"));
        let release_jobs = crate::acquisition::release_resolution::store::list_release_jobs(
            &database.pool,
            release_a.release_id,
        )
        .await?;
        let release_job_b = release_jobs
            .iter()
            .find(|job| job.download_id.as_deref() == Some(job_b_string.as_str()))
            .context("attempt B release job")?;
        assert!(release_job_b.active);
        assert_eq!(release_job_b.state, ReleaseJobState::Submitted);
        let provider_job_b = load_debrid_job(&database.pool, job_b_id)
            .await?
            .context("attempt B provider job")?;
        assert_eq!(provider_job_b.status, "submitted");
        assert_eq!(
            provider_job_b.provider_status,
            Some(json!({ "attempt": "b" }))
        );
        let provider_job_a = load_debrid_job(&database.pool, job_a_id)
            .await?
            .context("attempt A provider job")?;
        assert_eq!(provider_job_a.status, "failed");
        assert!(anime_debrid_retry_disposition_from_job(&provider_job_a).is_none());
        assert_eq!(
            provider_job_a
                .provider_status
                .as_ref()
                .and_then(|status| status.pointer("/animeAutomaticRetryConsumed/status"))
                .and_then(Value::as_str),
            Some("consumed")
        );
        Ok(())
    }

    #[tokio::test]
    async fn alm7_debrid_required_audio_mismatch_stays_automatic_without_selection() -> Result<()> {
        let database = setup_db().await?;
        let (provider_id, _) = create_provider_refs(&database.pool).await?;
        let subscription_id = create_anime_subscription_with_target(
            &database.pool,
            "Tokyo Ghoul",
            "Tokyo Ghoul Root A",
            "S02E01",
            2,
            1,
            13,
        )
        .await?;
        sqlx::query::<sqlx::Any>(
            "UPDATE acquisition_subscriptions SET quality_profile_json = $1 WHERE subscription_id = $2",
        )
        .bind(
            json!({
                "animeAudioPreference": {
                    "mode": "require_dub_review",
                    "language": "en"
                }
            })
            .to_string(),
        )
        .bind(subscription_id.to_string())
        .execute(&database.pool)
        .await?;

        let mut release = test_debrid_release(ReleaseKind::Unknown, ReleaseConfidence::Low);
        release.subscription_id = Some(subscription_id);
        release.source_provider_id = Some(provider_id);
        release.selected_provider_id = Some(provider_id);
        release.media_type = MediaType::Anime;
        release.title = "Tokyo Ghoul".to_string();
        release.release_title = "[Group] TGRA - 13 [Subbed]".to_string();
        release.resolver_kind = ReleaseResolverKind::AnimeShokoStyle;
        release.selected_candidate = Some(json!({
            "title": release.release_title.clone(),
            "source": release.source.clone(),
            "sourceKind": "magnet"
        }));
        let release = insert_test_release(&database.pool, &release).await?;
        let inspection = test_debrid_inspection(
            true,
            vec![DebridRemoteFile {
                provider_file_id: "actual-file-42".to_string(),
                file_index: Some(42),
                path: "Tokyo Ghoul Root A/[Group] TGRA - 13 [Subbed].mkv".to_string(),
                basename: "[Group] TGRA - 13 [Subbed].mkv".to_string(),
                size_bytes: Some(2_048),
                selectable: true,
                selected: Some(false),
                raw: None,
            }],
            Vec::new(),
            None,
        );
        let engine = FakeAnimeMatchEngine::new(FakeAnimeMatchBehavior::MatchSubbed);
        let service = engine.service();
        let options = DebridSubmitOptions {
            owner_id: "test.source",
            category: Some("anime"),
            name: Some("[Group] TGRA - 13 [Subbed]"),
            paused: false,
            release_context: None,
        };

        let refinement = persist_debrid_file_list_and_refine_coverage(
            &database.pool,
            &release,
            &options,
            &inspection,
            &service,
        )
        .await?;

        assert_eq!(engine.calls.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(refinement.state, AcquisitionReleaseState::Staging);
        assert_eq!(refinement.job_state, ReleaseJobState::Staging);
        assert!(!refinement.apply_file_selection_policy);
        assert!(
            refinement
                .automatic_retry
                .as_ref()
                .is_some_and(|retry| retry.suppress_automatic_rediscovery)
        );
        assert!(
            !refinement
                .state_reason
                .as_deref()
                .unwrap_or_default()
                .to_ascii_lowercase()
                .contains("review")
        );
        assert_eq!(
            refinement
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.pointer("/modelAudioProfile"))
                .and_then(Value::as_str),
            None
        );
        assert_eq!(
            refinement
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.pointer("/modelAudioAssessment/state"))
                .and_then(Value::as_str),
            None
        );
        assert_eq!(
            refinement
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.pointer("/animeMatchAssist/result"))
                .and_then(Value::as_str),
            Some("fallback")
        );
        assert_eq!(
            refinement
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.pointer("/animeMatchAssist/reason"))
                .and_then(Value::as_str),
            Some("coverage_validation_failed")
        );
        assert_eq!(
            refinement
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.pointer("/automaticResolution/requiredAudioSatisfied"))
                .and_then(Value::as_bool),
            Some(false)
        );
        Ok(())
    }

    #[tokio::test]
    async fn alm7_debrid_invalid_model_file_reference_is_rejected_before_coverage_override()
    -> Result<()> {
        let database = setup_db().await?;
        let (provider_id, instance_id) = create_provider_refs(&database.pool).await?;
        let subscription_id = create_anime_subscription_with_target(
            &database.pool,
            "Tokyo Ghoul",
            "Tokyo Ghoul Root A",
            "S02E01",
            2,
            1,
            13,
        )
        .await?;
        let adapter = FakeDebridAdapter::with_files(vec![DebridRemoteFile {
            provider_file_id: "actual-file-42".to_string(),
            file_index: Some(42),
            path: "Tokyo Ghoul Root A/[Group] TGRA - 13.mkv".to_string(),
            basename: "[Group] TGRA - 13.mkv".to_string(),
            size_bytes: Some(2_048),
            selectable: true,
            selected: Some(false),
            raw: None,
        }]);
        let engine = FakeAnimeMatchEngine::new(FakeAnimeMatchBehavior::UnknownFile);
        let service = engine.service();
        let source = "magnet:?xt=urn:btih:3333333333333333333333333333333333333333";

        let job_id = submit_debrid_with_adapter_and_anime_matching(
            &database.pool,
            provider_id,
            instance_id,
            source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("anime"),
                name: Some("[Group] TGRA - 13"),
                paused: false,
                release_context: Some(DebridReleaseSubmitContext {
                    subscription_id: Some(subscription_id),
                    source_provider_id: Some(provider_id),
                    source_extension_id: "test.source".to_string(),
                    media_type: MediaType::Anime,
                    title: "Tokyo Ghoul".to_string(),
                    release_title: "[Group] TGRA - 13".to_string(),
                    info_hash: None,
                    fingerprint: Some("alm7-invalid-model-file".to_string()),
                    score: Some(95.0),
                    selected_candidate: Some(json!({
                        "title": "[Group] TGRA - 13",
                        "source": source,
                        "sourceKind": "magnet"
                    })),
                }),
            },
            &service,
            &adapter,
        )
        .await?;

        let job = load_debrid_job(&database.pool, job_id)
            .await?
            .context("ALM-7 invalid-model Debrid job should load")?;
        assert!(job.selected_file_ids.is_empty());
        assert_ne!(job.status, "review_required");
        let release = get_release(&database.pool, job.release_id.context("release id")?)
            .await?
            .context("ALM-7 invalid-model release should load")?;
        assert_eq!(release.state, AcquisitionReleaseState::Staging);
        assert_eq!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.pointer("/animeMatchAssist/reason"))
                .and_then(Value::as_str),
            Some("invalid_model_response")
        );
        assert!(
            list_release_coverage(&database.pool, release.release_id)
                .await?
                .iter()
                .all(|entry| entry.verified_by.as_deref()
                    != Some("alm7_debrid_local_model_file_list"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn alm7_debrid_definitive_anime_uses_zero_model_calls() -> Result<()> {
        let database = setup_db().await?;
        let (provider_id, instance_id) = create_provider_refs(&database.pool).await?;
        let subscription_id = create_anime_subscription_with_target(
            &database.pool,
            "Naruto",
            "Naruto",
            "S01E01",
            1,
            1,
            1,
        )
        .await?;
        let adapter = FakeDebridAdapter::with_files(vec![DebridRemoteFile {
            provider_file_id: "naruto-1".to_string(),
            file_index: Some(1),
            path: "Naruto/Naruto.S01E01.1080p.WEB-DL.mkv".to_string(),
            basename: "Naruto.S01E01.1080p.WEB-DL.mkv".to_string(),
            size_bytes: Some(2_048),
            selectable: true,
            selected: Some(false),
            raw: None,
        }]);
        let engine = FakeAnimeMatchEngine::new(FakeAnimeMatchBehavior::EngineError);
        let service = engine.service();
        let source = "magnet:?xt=urn:btih:4444444444444444444444444444444444444444";

        let job_id = submit_debrid_with_adapter_and_anime_matching(
            &database.pool,
            provider_id,
            instance_id,
            source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("anime"),
                name: Some("Naruto.S01E01.1080p.WEB-DL"),
                paused: false,
                release_context: Some(DebridReleaseSubmitContext {
                    subscription_id: Some(subscription_id),
                    source_provider_id: Some(provider_id),
                    source_extension_id: "test.source".to_string(),
                    media_type: MediaType::Anime,
                    title: "Naruto".to_string(),
                    release_title: "Naruto.S01E01.1080p.WEB-DL".to_string(),
                    info_hash: None,
                    fingerprint: Some("alm7-deterministic-fast-path".to_string()),
                    score: Some(95.0),
                    selected_candidate: Some(json!({
                        "title": "Naruto.S01E01.1080p.WEB-DL",
                        "source": source,
                        "sourceKind": "magnet"
                    })),
                }),
            },
            &service,
            &adapter,
        )
        .await?;

        assert_eq!(engine.calls.load(AtomicOrdering::SeqCst), 0);
        let job = load_debrid_job(&database.pool, job_id)
            .await?
            .context("ALM-7 deterministic Debrid job should load")?;
        assert_eq!(job.selected_file_ids, vec!["naruto-1".to_string()]);
        let release = get_release(&database.pool, job.release_id.context("release id")?)
            .await?
            .context("ALM-7 deterministic release should load")?;
        assert_eq!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.pointer("/animeMatchAssist/source"))
                .and_then(Value::as_str),
            Some("deterministic_fast_path")
        );
        Ok(())
    }

    #[tokio::test]
    async fn existing_debrid_job_keeps_provider_identity_after_active_service_switch() -> Result<()>
    {
        let database = setup_db().await?;
        let store = ExtensionStore::new(&database.pool);
        let (provider_id, instance_id) = create_provider_refs(&database.pool).await?;
        let adapter = FakeDebridAdapter::new();
        let source = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567";
        let job_id = submit_debrid_with_adapter(
            &database.pool,
            provider_id,
            instance_id,
            source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("series"),
                name: Some("Show.S01.1080p.WEB-DL"),
                paused: true,
                release_context: Some(DebridReleaseSubmitContext {
                    subscription_id: None,
                    source_provider_id: Some(provider_id),
                    source_extension_id: "test.source".to_string(),
                    media_type: MediaType::Series,
                    title: "Show".to_string(),
                    release_title: "Show.S01.1080p.WEB-DL".to_string(),
                    info_hash: None,
                    fingerprint: Some("provider-switch-pinned-job".to_string()),
                    score: Some(99.0),
                    selected_candidate: Some(json!({
                        "title": "Show.S01.1080p.WEB-DL",
                        "source": source,
                        "sourceKind": "magnet"
                    })),
                }),
            },
            &adapter,
        )
        .await?;

        store
            .update_instance_config(
                instance_id,
                Some(&json!({
                    "activeService": "torbox",
                    "materialize": true
                })),
            )
            .await?;

        let job = load_debrid_job(&database.pool, job_id)
            .await?
            .context("debrid job should load after active service switch")?;
        assert_eq!(job.provider_implementation.as_deref(), Some("fake_debrid"));
        assert_eq!(job.status, "paused");

        let release = crate::acquisition::release_resolution::store::get_release_by_fingerprint(
            &database.pool,
            DEFAULT_ROUTE_OWNER_ID,
            "test.source",
            "provider-switch-pinned-job",
        )
        .await?
        .context("release should remain readable")?;
        assert_eq!(
            release.download_id.as_deref(),
            Some(job_id.to_string().as_str())
        );
        assert_eq!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.get("debridProvider"))
                .and_then(|provider| provider.get("providerImplementation"))
                .and_then(Value::as_str),
            Some("fake_debrid")
        );
        Ok(())
    }

    #[tokio::test]
    async fn debrid_progress_refresh_dispatches_by_persisted_provider() -> Result<()> {
        let state = setup_debrid_test_state().await?;
        let store = ExtensionStore::new(&state.db_pool);
        let (provider_id, instance_id) = create_provider_refs(&state.db_pool).await?;
        let (base_url, _premiumize_state, shutdown) =
            start_mock_premiumize_directdl_server().await?;
        store
            .update_instance_config(
                instance_id,
                Some(&json!({
                    "activeService": "real_debrid",
                    "materialize": true,
                    "testPremiumizeApiBaseUrl": base_url
                })),
            )
            .await?;
        save_debrid_token(
            state.secrets.as_ref(),
            &store,
            instance_id,
            DebridServiceKind::Premiumize,
            "good-token",
        )
        .await?;
        let job_id = insert_lifecycle_debrid_job(
            &state.db_pool,
            provider_id,
            instance_id,
            DebridServiceKind::Premiumize,
            "submitted",
        )
        .await?;
        update_lifecycle_debrid_job_remote_id(&state.db_pool, job_id, "pm-transfer-file").await?;

        let progress = load_debrid_progress(&state, &store, provider_id, instance_id).await?;
        assert_eq!(progress.len(), 1);
        let evidence = progress[0]
            .debrid
            .as_ref()
            .context("debrid progress evidence should be present")?;
        assert_eq!(evidence.provider_name.as_deref(), Some("Premiumize"));
        assert_eq!(
            evidence.provider_implementation.as_deref(),
            Some("premiumize")
        );
        assert_eq!(evidence.failure_class, None);

        let job = load_debrid_job(&state.db_pool, job_id)
            .await?
            .context("debrid job should load after refresh")?;
        assert_eq!(job.status, "debrid_downloaded");
        assert_eq!(
            job.remote_release_status.as_deref(),
            Some(DebridReleaseStatus::Downloaded.as_str())
        );
        assert_eq!(job.last_error, None);
        let _ = shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn debrid_materializer_loop_dispatches_by_persisted_provider() -> Result<()> {
        let state = setup_debrid_test_state().await?;
        let store = ExtensionStore::new(&state.db_pool);
        let (provider_id, instance_id) = create_provider_refs(&state.db_pool).await?;
        let (base_url, _premiumize_state, shutdown) =
            start_mock_premiumize_directdl_server().await?;
        store
            .update_instance_config(
                instance_id,
                Some(&json!({
                    "activeService": "real_debrid",
                    "materialize": true,
                    "testPremiumizeApiBaseUrl": base_url
                })),
            )
            .await?;
        save_debrid_token(
            state.secrets.as_ref(),
            &store,
            instance_id,
            DebridServiceKind::Premiumize,
            "good-token",
        )
        .await?;
        let job_id = insert_lifecycle_debrid_job(
            &state.db_pool,
            provider_id,
            instance_id,
            DebridServiceKind::Premiumize,
            "submitted",
        )
        .await?;
        update_lifecycle_debrid_job_remote_id(&state.db_pool, job_id, "pm-transfer-file").await?;

        process_debrid_jobs_once(&state).await?;

        let job = load_debrid_job(&state.db_pool, job_id)
            .await?
            .context("debrid job should load after materializer pass")?;
        assert_eq!(job.status, "debrid_downloaded");
        assert_eq!(
            job.remote_release_status.as_deref(),
            Some(DebridReleaseStatus::Downloaded.as_str())
        );
        assert_eq!(classify_debrid_job_failure(&job), None);
        assert_eq!(job.last_error, None);
        let _ = shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn debrid_cancel_dispatches_remote_delete_by_persisted_provider() -> Result<()> {
        let state = setup_debrid_test_state().await?;
        let store = ExtensionStore::new(&state.db_pool);
        let (provider_id, instance_id) = create_provider_refs(&state.db_pool).await?;
        let (base_url, premiumize_state, shutdown) =
            start_mock_premiumize_directdl_server().await?;
        store
            .update_instance_config(
                instance_id,
                Some(&json!({
                    "activeService": "real_debrid",
                    "materialize": true,
                    "testPremiumizeApiBaseUrl": base_url
                })),
            )
            .await?;
        save_debrid_token(
            state.secrets.as_ref(),
            &store,
            instance_id,
            DebridServiceKind::Premiumize,
            "good-token",
        )
        .await?;
        let job_id = insert_lifecycle_debrid_job(
            &state.db_pool,
            provider_id,
            instance_id,
            DebridServiceKind::Premiumize,
            "submitted",
        )
        .await?;
        update_lifecycle_debrid_job_remote_id(&state.db_pool, job_id, "pm-transfer-file").await?;

        let cancelled = cancel_debrid_job(
            &state,
            &store,
            provider_id,
            instance_id,
            &job_id.to_string(),
        )
        .await?;

        assert!(cancelled);
        let job = load_debrid_job(&state.db_pool, job_id)
            .await?
            .context("cancelled debrid job should load")?;
        assert_eq!(job.status, "cancelled");
        assert_eq!(job.last_error, None);
        assert_eq!(
            premiumize_state
                .deleted_transfers
                .lock()
                .unwrap()
                .as_slice(),
            ["pm-transfer-file"]
        );
        let _ = shutdown.send(());
        Ok(())
    }

    #[tokio::test]
    async fn debrid_materializer_syncs_release_job_and_target_progress() -> Result<()> {
        let database = setup_db().await?;
        let (provider_id, instance_id) = create_provider_refs(&database.pool).await?;
        let subscription_id = create_series_subscription_with_targets(&database.pool).await?;
        let adapter = FakeDebridAdapter::new();
        let source = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567";
        let job_id = submit_debrid_with_adapter(
            &database.pool,
            provider_id,
            instance_id,
            source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("series"),
                name: Some("Show.S01.1080p.WEB-DL"),
                paused: false,
                release_context: Some(DebridReleaseSubmitContext {
                    subscription_id: Some(subscription_id),
                    source_provider_id: Some(provider_id),
                    source_extension_id: "test.source".to_string(),
                    media_type: MediaType::Series,
                    title: "Show".to_string(),
                    release_title: "Show.S01.1080p.WEB-DL".to_string(),
                    info_hash: None,
                    fingerprint: None,
                    score: Some(99.0),
                    selected_candidate: Some(json!({
                        "title": "Show.S01.1080p.WEB-DL",
                        "source": source,
                        "sourceKind": "magnet",
                        "supportedRoutes": ["acquisition.debrid.default"]
                    })),
                }),
            },
            &adapter,
        )
        .await?;

        let release_id = load_debrid_job(&database.pool, job_id)
            .await?
            .context("debrid job should load")?
            .release_id
            .context("debrid job should be linked to a release")?;
        mark_debrid_job_status(&database.pool, job_id, "materializing", None).await?;

        let release =
            crate::acquisition::release_resolution::store::get_release(&database.pool, release_id)
                .await?
                .context("release should load")?;
        assert_eq!(release.state, AcquisitionReleaseState::Materializing);
        assert_eq!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.get("debridRuntime"))
                .and_then(|runtime| runtime.get("status"))
                .and_then(Value::as_str),
            Some("materializing")
        );
        let jobs = crate::acquisition::release_resolution::store::list_release_jobs(
            &database.pool,
            release_id,
        )
        .await?;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].state, ReleaseJobState::Materializing);
        assert!(jobs[0].active);
        let submitted_targets: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)
             FROM acquisition_targets
             WHERE subscription_id = $1
               AND state = 'submitted'",
        )
        .bind(subscription_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(submitted_targets, 2);

        mark_debrid_job_completed(&database.pool, job_id, Some("/downloads/debrid/Show.S01"))
            .await?;
        let release =
            crate::acquisition::release_resolution::store::get_release(&database.pool, release_id)
                .await?
                .context("completed release should load")?;
        assert_eq!(release.state, AcquisitionReleaseState::Completed);
        assert_eq!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.get("debridRuntime"))
                .and_then(|runtime| runtime.get("status"))
                .and_then(Value::as_str),
            Some("completed")
        );
        assert_eq!(
            release
                .coverage_plan
                .as_ref()
                .and_then(|plan| plan.get("debridRuntime"))
                .and_then(|runtime| runtime.get("localPath"))
                .and_then(Value::as_str),
            Some("/downloads/debrid/Show.S01")
        );
        let jobs = crate::acquisition::release_resolution::store::list_release_jobs(
            &database.pool,
            release_id,
        )
        .await?;
        assert_eq!(jobs[0].state, ReleaseJobState::Completed);
        assert!(!jobs[0].active);
        assert!(jobs[0].completed_at.is_some());

        let coverage = crate::acquisition::release_resolution::store::list_release_coverage(
            &database.pool,
            release_id,
        )
        .await?;
        assert!(
            coverage
                .iter()
                .all(|entry| entry.state == ReleaseCoverageState::Submitted)
        );
        let imported_targets: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)
             FROM acquisition_targets
             WHERE subscription_id = $1
               AND state = 'imported'",
        )
        .bind(subscription_id.to_string())
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(imported_targets, 0);
        Ok(())
    }

    #[tokio::test]
    async fn debrid_materializer_does_not_requeue_in_flight_jobs() -> Result<()> {
        let database = setup_db().await?;
        let (provider_id, instance_id) = create_provider_refs(&database.pool).await?;
        let adapter = FakeDebridAdapter::new();
        let job_id = submit_debrid_with_adapter(
            &database.pool,
            provider_id,
            instance_id,
            "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("series"),
                name: Some("Show.S01E01.1080p.WEB-DL"),
                paused: false,
                release_context: None,
            },
            &adapter,
        )
        .await?;

        mark_debrid_job_status(&database.pool, job_id, "materializing", None).await?;

        let active = list_active_debrid_jobs(&database.pool, 10).await?;
        assert!(
            active.iter().all(|job| job.job_id != job_id),
            "materializing jobs must not be started again by a later materializer tick"
        );
        let refreshable = list_refreshable_debrid_jobs(&database.pool, provider_id).await?;
        assert!(
            refreshable.iter().all(|job| job.job_id != job_id),
            "UI/remote refresh must not overwrite local materializer byte progress"
        );

        mark_debrid_job_status(&database.pool, job_id, "debrid_downloaded", None).await?;
        let active = list_active_debrid_jobs(&database.pool, 10).await?;
        assert!(
            active.iter().any(|job| job.job_id == job_id),
            "downloaded debrid jobs should still be materialized"
        );
        Ok(())
    }

    #[tokio::test]
    async fn debrid_materializer_requeues_no_selected_files_review_jobs() -> Result<()> {
        let database = setup_db().await?;
        let (provider_id, instance_id) = create_provider_refs(&database.pool).await?;
        let adapter = FakeDebridAdapter::new();
        let job_id = submit_debrid_with_adapter(
            &database.pool,
            provider_id,
            instance_id,
            "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("series"),
                name: Some("Show.S01E01.1080p.WEB-DL"),
                paused: false,
                release_context: None,
            },
            &adapter,
        )
        .await?;

        mark_debrid_job_status(&database.pool, job_id, "review_required", None).await?;
        sqlx::query::<sqlx::Any>(
            "UPDATE debrid_download_jobs
             SET selection_error = 'no_selected_files'
             WHERE job_id = $1",
        )
        .bind(job_id.to_string())
        .execute(&database.pool)
        .await?;
        let active = list_active_debrid_jobs(&database.pool, 10).await?;
        assert!(
            active.iter().any(|job| job.job_id == job_id),
            "no_selected_files review jobs should be retried after selector fixes"
        );

        sqlx::query::<sqlx::Any>(
            "UPDATE debrid_download_jobs
             SET selection_error = 'ambiguous_target_file_match'
             WHERE job_id = $1",
        )
        .bind(job_id.to_string())
        .execute(&database.pool)
        .await?;
        let active = list_active_debrid_jobs(&database.pool, 10).await?;
        assert!(
            active.iter().all(|job| job.job_id != job_id),
            "ambiguous review jobs must remain parked for manual review"
        );
        Ok(())
    }

    #[tokio::test]
    async fn staged_debrid_submit_records_selection_failure_evidence() -> Result<()> {
        let database = setup_db().await?;
        let (provider_id, instance_id) = create_provider_refs(&database.pool).await?;
        let subscription_id = create_series_subscription_with_targets(&database.pool).await?;
        let adapter = FakeDebridAdapter::failing_select();
        let source = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567";

        let err = submit_debrid_with_adapter(
            &database.pool,
            provider_id,
            instance_id,
            source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("series"),
                name: Some("Show.S01.1080p.WEB-DL"),
                paused: false,
                release_context: Some(DebridReleaseSubmitContext {
                    subscription_id: Some(subscription_id),
                    source_provider_id: Some(provider_id),
                    source_extension_id: "test.source".to_string(),
                    media_type: MediaType::Series,
                    title: "Show".to_string(),
                    release_title: "Show.S01.1080p.WEB-DL".to_string(),
                    info_hash: None,
                    fingerprint: None,
                    score: Some(99.0),
                    selected_candidate: Some(json!({
                        "title": "Show.S01.1080p.WEB-DL",
                        "source": source,
                        "sourceKind": "magnet",
                        "supportedRoutes": [
                            "acquisition.debrid.default",
                            "acquisition.torrent.default"
                        ]
                    })),
                }),
            },
            &adapter,
        )
        .await
        .unwrap_err();
        let error_message = err.to_string();
        assert!(
            error_message.contains("selecting debrid files for remote release"),
            "{error_message}"
        );

        let jobs = list_debrid_jobs_for_provider(&database.pool, provider_id).await?;
        assert_eq!(jobs.len(), 1);
        let job = &jobs[0];
        assert_eq!(job.status, "failed");
        assert!(
            job.last_error
                .as_deref()
                .unwrap_or_default()
                .contains("selecting debrid files for remote release")
        );

        let status = get_debrid_job_status(&database.pool, job.job_id)
            .await?
            .context("debrid job status should load")?;
        assert!(status.is_failed());
        assert_eq!(status.failure_class.as_deref(), Some("selection_failed"));
        assert_eq!(status.source_kind, "magnet");

        let release = get_release(
            &database.pool,
            job.release_id.context("job should be linked to release")?,
        )
        .await?
        .context("failed debrid release should load")?;
        assert_eq!(release.state, AcquisitionReleaseState::Failed);
        let failure = release
            .coverage_plan
            .as_ref()
            .and_then(|plan| plan.get("debridFailure"))
            .context("debrid failure evidence should be persisted")?;
        assert_eq!(
            failure.get("failureClass").and_then(Value::as_str),
            Some("selection_failed")
        );
        assert_eq!(
            failure.get("fallbackState").and_then(Value::as_str),
            Some("eligible_if_candidate_supports_torrent_route")
        );
        Ok(())
    }

    #[tokio::test]
    async fn debrid_submit_failure_without_job_records_provider_provenance() -> Result<()> {
        let database = setup_db().await?;
        let (provider_id, instance_id) = create_provider_refs(&database.pool).await?;
        let adapter = UnsupportedDebridAdapter {
            service: DebridServiceKind::TorBox,
        };
        let source = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567";

        let err = submit_debrid_with_adapter(
            &database.pool,
            provider_id,
            instance_id,
            source,
            DebridSubmitOptions {
                owner_id: "test.source",
                category: Some("series"),
                name: Some("Show.S01E01.1080p.WEB-DL"),
                paused: false,
                release_context: Some(DebridReleaseSubmitContext {
                    subscription_id: None,
                    source_provider_id: Some(provider_id),
                    source_extension_id: "test.source".to_string(),
                    media_type: MediaType::Series,
                    title: "Show".to_string(),
                    release_title: "Show.S01E01.1080p.WEB-DL".to_string(),
                    info_hash: None,
                    fingerprint: Some("torbox-submit-unsupported".to_string()),
                    score: Some(80.0),
                    selected_candidate: Some(json!({
                        "title": "Show.S01E01.1080p.WEB-DL",
                        "source": source,
                        "sourceKind": "magnet",
                        "supportedRoutes": ["acquisition.debrid.default"]
                    })),
                }),
            },
            &adapter,
        )
        .await
        .expect_err("unsupported provider should fail submit before job creation");
        assert!(err.to_string().contains("provider unsupported: TorBox"));

        let jobs = list_debrid_jobs_for_provider(&database.pool, provider_id).await?;
        assert!(jobs.is_empty());
        let release = crate::acquisition::release_resolution::store::get_release_by_fingerprint(
            &database.pool,
            DEFAULT_ROUTE_OWNER_ID,
            "test.source",
            "torbox-submit-unsupported",
        )
        .await?
        .context("failed release should remain queryable")?;
        assert_eq!(release.state, AcquisitionReleaseState::Failed);
        let provider = release
            .coverage_plan
            .as_ref()
            .and_then(|plan| plan.get("debridProvider"))
            .context("provider provenance should be recorded on failed submit")?;
        assert_eq!(
            provider
                .get("providerImplementation")
                .and_then(Value::as_str),
            Some("torbox")
        );
        assert_eq!(
            provider.get("providerName").and_then(Value::as_str),
            Some("TorBox")
        );
        assert_eq!(
            provider
                .get("providerCapabilities")
                .and_then(|capabilities| capabilities.get("supportsMagnetSubmit"))
                .and_then(Value::as_bool),
            Some(false)
        );
        let failure = release
            .coverage_plan
            .as_ref()
            .and_then(|plan| plan.get("debridFailure"))
            .context("failure evidence should be recorded on failed submit")?;
        assert_eq!(
            failure.get("failureClass").and_then(Value::as_str),
            Some("provider_unsupported")
        );
        assert_eq!(
            failure
                .get("providerImplementation")
                .and_then(Value::as_str),
            Some("torbox")
        );
        assert_eq!(
            failure.get("stage").and_then(Value::as_str),
            Some("provider_submit")
        );
        Ok(())
    }

    #[test]
    fn sanitizes_download_paths() {
        assert_eq!(safe_path_segment("TV Shows/../x"), "TV-Shows-..-x");
        assert_eq!(safe_file_name("../Movie: 2024.mkv"), "_Movie_ 2024.mkv");
    }
}
