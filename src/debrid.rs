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

use crate::acquisition::release_resolution::{
    anime::{
        AnimeCandidateInput, AnimeCandidateScoringContext, AnimeCandidateTarget,
        AnimeReleaseFileInput, parse_anime_release_title, plan_anime_file_coverage,
    },
    fingerprint::{ReleaseFingerprintInput, build_release_fingerprint, extract_magnet_info_hash},
    hashing::{HashFileJob, queue_anime_hash_file},
    models::{
        AcquisitionRelease, AcquisitionReleaseCoverage, AcquisitionReleaseFile,
        AcquisitionReleaseState, NewAcquisitionRelease, NewAcquisitionReleaseCoverage,
        NewAcquisitionReleaseFile, NewAcquisitionReleaseJob, ReleaseConfidence,
        ReleaseCoverageKind, ReleaseCoverageState, ReleaseJobState, ReleaseKind,
        ReleaseResolverKind,
    },
    review_candidates::SYNTHETIC_SOURCE_CANDIDATE_FILE_ID,
    store::{
        get_release, get_release_by_download_id, list_release_coverage, list_release_files,
        update_release_coverage_review_state, upsert_release, upsert_release_coverage,
        upsert_release_file, upsert_release_job,
    },
    tv::{TvCoverageOptions, TvReleaseFileInput, TvSonarrStyleResolver, TvTarget},
};
use crate::acquisition::subscriptions::{
    AcquisitionTargetState, AcquisitionTargetStateUpdate, list_subscription_targets,
    update_target_state,
};
use crate::db::models::{
    ExtensionKind, ExtensionTrustLevel, MediaType, ProviderHealthState, ProviderReadinessPhase,
    SecretScope, SlotCardinality,
};
use crate::download_broker::{DEBRID_DEFAULT_LOGICAL_ID, DEFAULT_ROUTE_OWNER_ID};
use crate::extensions::store::{
    ExtensionStore, NewExtension, NewExtensionInstance, NewProvider, NewSecret,
};
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
            raw: Some(serde_json::to_value(transfer)?),
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
        raw: Some(serde_json::to_value(transfer)?),
    })
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
    Ok(DebridReleaseInspection {
        release: DebridRemoteRelease {
            provider_implementation: DebridServiceKind::AllDebrid.implementation_id().to_string(),
            remote_release_id: all_debrid_id_string(&status.id)
                .unwrap_or_else(|| "unknown-alldebrid-magnet".to_string()),
            display_name: status.filename.clone(),
            status: progress.status,
            raw_status: status.status.clone(),
            raw: Some(serde_json::to_value(&status)?),
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
        raw: Some(serde_json::to_value(status)?),
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
         SET extension_id = ?, updated_at = CURRENT_TIMESTAMP
         WHERE extension_id = ?",
    )
    .bind(DEBRID_EXTENSION_ID)
    .bind(LEGACY_REAL_DEBRID_EXTENSION_ID)
    .execute(pool)
    .await
    .context("migrating legacy Real-Debrid instances to canonical Debrid extension id")?;

    sqlx::query::<sqlx::Any>("DELETE FROM extensions WHERE extension_id = ?")
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
         SET scope_json = ?,
             health_state = ?,
             updated_at = CURRENT_TIMESTAMP
         WHERE capability = 'debrid.resolver'
           AND slot_id = 'default'
           AND instance_id <> ?",
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
         WHERE instance_id = ?
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
         WHERE (extension_id = ? OR extension_id = ?)
           AND enabled = ?
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
    submit_debrid_with_adapter(
        &state.db_pool,
        provider_id,
        instance_id,
        source,
        options,
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
    let source_kind = debrid_source_kind(source)?;
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
            acquisition_state_for_job_status(remote_release_status.as_deref()),
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
            release_job_state_for_job_status(remote_release_status.as_deref()),
            "Debrid job recorded with provider provenance.",
        )
        .await?;
    }
    if !options.paused
        && source_kind == "magnet"
        && let Some(remote_release_id) = remote_release_id.as_deref()
    {
        let staged_result = async {
            let inspection = adapter.inspect_release(remote_release_id).await?;
            update_debrid_job_from_inspection(pool, job_id, &inspection).await?;
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
                )
                .await?;
                if let Some(updated) = upsert_debrid_acquisition_release(
                    pool,
                    provider_id,
                    source,
                    source_kind,
                    &options,
                    Some(&inspection.release.remote_release_id),
                    Some(&job_id.to_string()),
                    refinement.state,
                    refinement.state_reason.as_deref(),
                    refinement.shape,
                    Some(merge_debrid_provider_provenance(
                        refinement.coverage_plan,
                        provider_id,
                        adapter.implementation(),
                        &provider_capabilities,
                        Some(&inspection.release.remote_release_id),
                        Some(inspection.release.status.as_str()),
                        source_kind,
                        Some(job_id),
                    )),
                )
                .await?
                {
                    upsert_debrid_release_job(
                        pool,
                        &updated,
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
                    let _ = apply_debrid_file_selection_policy(
                        pool,
                        adapter,
                        job_id,
                        &updated,
                        &inspection,
                    )
                    .await?;
                }
            }
            Ok::<(), anyhow::Error>(())
        }
        .await;
        if let Err(err) = staged_result {
            mark_debrid_job_status(pool, job_id, "failed", Some(&err.to_string())).await?;
            return Err(err);
        }
    }
    Ok(job_id)
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
            active: true,
            started_at: Some(chrono::Utc::now()),
            completed_at: None,
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
) -> Result<DebridCoverageRefinement> {
    let file_ids = persist_debrid_release_files(pool, release, &inspection.files).await?;
    let base = refinement_from_debrid_status(inspection.release.status);
    let Some(subscription_id) = release.subscription_id else {
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
            ..base
        });
    };

    let targets = list_subscription_targets(pool, subscription_id).await?;
    match release.media_type {
        MediaType::Series => {
            refine_tv_debrid_coverage(pool, release, inspection, &targets, &file_ids).await
        }
        MediaType::Anime => {
            refine_anime_debrid_coverage(pool, release, options, inspection, &targets, &file_ids)
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
        MediaType::Movie => ParsedReleaseFileMetadata {
            title: None,
            season_number: None,
            episode_number: None,
            episode_end_number: None,
            absolute_episode_number: None,
            absolute_episode_end_number: None,
            air_date: None,
            quality: None,
            language: None,
            release_group: None,
            confidence: ReleaseConfidence::Low,
            reason: Some("movie_file_parser_pending".to_string()),
        },
    }
}

async fn refine_movie_debrid_coverage(
    pool: &sqlx::AnyPool,
    release: &AcquisitionRelease,
    inspection: &DebridReleaseInspection,
    targets: &[crate::acquisition::subscriptions::AcquisitionTarget],
    file_ids: &HashMap<String, Uuid>,
) -> Result<DebridCoverageRefinement> {
    let media_files = inspection
        .files
        .iter()
        .filter(|file| {
            file.selectable
                && is_debrid_media_file(&file.path)
                && !is_debrid_sample_or_extra_file(&file.path)
        })
        .collect::<Vec<_>>();
    let mut review_reasons = Vec::new();
    if targets.is_empty() {
        review_reasons.push("missing_movie_target".to_string());
    }
    if media_files.is_empty() {
        review_reasons.push("no_media_files".to_string());
    }
    if media_files.len() > 1 {
        review_reasons.push("movie_multi_file_policy_pending".to_string());
    }
    review_reasons.sort();
    review_reasons.dedup();

    let confidence = if review_reasons.is_empty() {
        ReleaseConfidence::High
    } else {
        ReleaseConfidence::ReviewRequired
    };

    if let (Some(target), Some(file)) = (targets.first(), media_files.first()) {
        upsert_release_coverage(
            pool,
            NewAcquisitionReleaseCoverage {
                coverage_id: None,
                release_id: release.release_id,
                release_file_id: file_ids.get(&file.provider_file_id).copied(),
                target_id: target.target_id,
                coverage_kind: ReleaseCoverageKind::SingleEpisode,
                confidence,
                score: Some(1.0),
                reason: Some("rr10c_debrid_movie_single_file".to_string()),
                state: ReleaseCoverageState::Planned,
                verified_by: Some("rr10c_debrid_movie_file_list".to_string()),
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
            resolver_kind: ReleaseResolverKind::MovieSingle,
            resolver_version: DEBRID_SELECTION_POLICY_VERSION.to_string(),
            confidence,
        },
        json!({
            "source": "debrid_provider_file_list",
            "providerImplementation": inspection.release.provider_implementation,
            "remoteReleaseId": inspection.release.remote_release_id,
            "movie": {
                "confidence": confidence,
                "mediaFileCount": media_files.len()
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
    pool: &sqlx::AnyPool,
    release: &AcquisitionRelease,
    options: &DebridSubmitOptions<'_>,
    inspection: &DebridReleaseInspection,
    targets: &[crate::acquisition::subscriptions::AcquisitionTarget],
    file_ids: &HashMap<String, Uuid>,
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
    let plan = plan_anime_file_coverage(&context, &candidate, &files);
    let targets_by_key = targets
        .iter()
        .map(|target| (target.target_key.clone(), target.target_id))
        .collect::<HashMap<_, _>>();
    for entry in &plan.entries {
        let Some(target_id) = targets_by_key.get(&entry.target_key).copied() else {
            continue;
        };
        upsert_release_coverage(
            pool,
            NewAcquisitionReleaseCoverage {
                coverage_id: None,
                release_id: release.release_id,
                release_file_id: entry
                    .release_file_key
                    .as_ref()
                    .and_then(|file_id| file_ids.get(file_id))
                    .copied(),
                target_id,
                coverage_kind: entry.coverage_kind,
                confidence: entry.confidence,
                score: entry.score,
                reason: Some(entry.reason.clone()),
                state: entry.state,
                verified_by: Some("rr4e_anime_file_list".to_string()),
            },
        )
        .await?;
    }
    let mut review_reasons = plan.review_reasons.clone();
    review_reasons.extend(plan.rejection_reasons.clone());
    review_reasons.sort();
    review_reasons.dedup();
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
            "anime": plan,
            "reviewReasons": review_reasons
        }),
        review_reasons,
        inspection.release.status,
    ))
}

fn anime_scoring_context_from_release(
    release: &AcquisitionRelease,
    targets: &[crate::acquisition::subscriptions::AcquisitionTarget],
) -> AnimeCandidateScoringContext {
    let mut aliases = Vec::new();
    push_unique_alias(&mut aliases, &release.title);
    for target in targets {
        push_unique_alias(&mut aliases, &target.title);
        if let Some(metadata) = target.metadata.as_ref() {
            for key in ["aliases", "titles", "anilistTitles"] {
                if let Some(values) = metadata.get(key).and_then(Value::as_array) {
                    for value in values.iter().filter_map(Value::as_str) {
                        push_unique_alias(&mut aliases, value);
                    }
                }
            }
        }
    }
    AnimeCandidateScoringContext {
        graph_fingerprint: release
            .coverage_plan
            .as_ref()
            .and_then(|value| value.get("graphFingerprint"))
            .and_then(Value::as_str)
            .map(str::to_string),
        aliases,
        targets: targets
            .iter()
            .map(|target| {
                let metadata = target.metadata.as_ref();
                AnimeCandidateTarget {
                    target_key: target.target_key.clone(),
                    canonical_key: metadata_json_string(metadata, "targetCanonicalKey"),
                    title: target.title.clone(),
                    season_number: target.season_number,
                    episode_number: target.episode_number,
                    absolute_episode_number: target.absolute_episode_number,
                    tvdb_episode_id: metadata_json_string(metadata, "tvdbEpisodeId"),
                    anidb_episode_id: metadata_json_string(metadata, "anidbEpisodeId"),
                }
            })
            .collect(),
    }
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
    metadata?
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
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
    review_reasons: Vec<String>,
    policy_version: String,
    coverage_fingerprint: String,
    select_all: bool,
    select_all_approved: bool,
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
}

async fn apply_debrid_file_selection_policy<A: DebridProviderAdapter + ?Sized>(
    pool: &sqlx::AnyPool,
    adapter: &A,
    job_id: Uuid,
    release: &AcquisitionRelease,
    inspection: &DebridReleaseInspection,
) -> Result<Option<DebridReleaseInspection>> {
    let files = list_release_files(pool, release.release_id).await?;
    let coverage = list_release_coverage(pool, release.release_id).await?;
    let decision = decide_debrid_file_selection(release, &files, &coverage, inspection);
    persist_debrid_selection_decision(pool, job_id, release, &files, &coverage, &decision).await?;
    if !decision.is_approved() {
        return Ok(None);
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
    update_debrid_job_from_inspection(pool, job_id, &selected).await?;
    persist_debrid_release_files(pool, release, &selected.files).await?;
    mark_debrid_selection_applied(pool, release, job_id, &selected).await?;
    Ok(Some(selected))
}

fn decide_debrid_file_selection(
    release: &AcquisitionRelease,
    files: &[AcquisitionReleaseFile],
    coverage: &[AcquisitionReleaseCoverage],
    inspection: &DebridReleaseInspection,
) -> DebridFileSelectionDecision {
    if let Some(decision) = approved_debrid_user_override(release, files, &inspection.capabilities)
    {
        return decision;
    }

    let mut review_reasons = BTreeSet::new();
    let capabilities = &inspection.capabilities;
    if release.confidence != ReleaseConfidence::High {
        review_reasons.insert("coverage_not_high_confidence".to_string());
    }
    if !capabilities.supports_file_selection {
        review_reasons.insert("file_selection_unsupported".to_string());
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

    if selected_file_ids.is_empty() && release.confidence == ReleaseConfidence::High {
        let targets = debrid_targets_from_coverage_plan(release, coverage);
        for file in &selectable_media_files {
            if targets
                .iter()
                .any(|target| debrid_file_matches_target(file, target))
                && let Some(file_id) = file
                    .provider_file_id
                    .clone()
                    .or_else(|| file.file_id.clone())
            {
                selected_file_ids.insert(file_id);
            }
        }
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
        policy_version: DEBRID_SELECTION_POLICY_VERSION.to_string(),
        coverage_fingerprint: debrid_coverage_fingerprint(release, files, coverage),
        review_reasons,
        select_all,
        select_all_approved,
    }
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

    debrid_coverage_plan_entries(release.coverage_plan.as_ref())
        .into_iter()
        .filter_map(debrid_target_from_coverage_plan_entry)
        .filter(|target| covered_target_ids.contains(&target.target_id))
        .collect::<Vec<_>>()
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
    Some(DebridCoverageTarget {
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
    })
}

fn coverage_plan_i32(entry: &Value, keys: &[&str]) -> Option<i32> {
    keys.iter()
        .find_map(|key| entry.get(*key))
        .and_then(Value::as_i64)
        .and_then(|value| i32::try_from(value).ok())
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

    let selected_file_ids =
        resolve_approved_debrid_file_ids(json_string_array(policy.get("selectedFileIds")), files);
    if selected_file_ids.is_empty() {
        return None;
    }
    let selected_set = selected_file_ids.iter().cloned().collect::<BTreeSet<_>>();
    let mut skipped_file_ids =
        resolve_approved_debrid_file_ids(json_string_array(policy.get("skippedFileIds")), files);
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
    file_ids
        .into_iter()
        .flat_map(|file_id| {
            if file_id == SYNTHETIC_SOURCE_CANDIDATE_FILE_ID
                && let Some(provider_file_id) =
                    matching_provider_file_id_for_synthetic_source_candidate(files)
            {
                return vec![provider_file_id];
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
) -> Result<()> {
    update_debrid_job_selection_decision(pool, job_id, decision).await?;
    let selected_ids = decision
        .selected_file_ids
        .iter()
        .cloned()
        .collect::<HashSet<_>>();
    for file in files {
        let provider_id = file.provider_file_id.as_ref().or(file.file_id.as_ref());
        update_release_file_selected(
            pool,
            file.release_file_id,
            provider_id
                .map(|file_id| selected_ids.contains(file_id))
                .unwrap_or(false),
        )
        .await?;
    }
    let file_aliases = synthetic_source_candidate_release_file_aliases(files);
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
        .collect::<HashSet<_>>();
    for entry in coverage {
        let release_file_id = entry.release_file_id.and_then(|release_file_id| {
            file_aliases
                .get(&release_file_id)
                .copied()
                .or(Some(release_file_id))
        });
        let selected = release_file_id
            .and_then(|release_file_id| {
                files
                    .iter()
                    .find(|file| file.release_file_id == release_file_id)
            })
            .and_then(|file| file.provider_file_id.as_ref().or(file.file_id.as_ref()))
            .map(|file_id| selected_ids.contains(file_id))
            .unwrap_or(false);
        upsert_release_coverage(
            pool,
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
            },
        )
        .await?;
    }
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
    Ok(())
}

async fn mark_debrid_selection_applied(
    pool: &sqlx::AnyPool,
    release: &AcquisitionRelease,
    job_id: Uuid,
    inspection: &DebridReleaseInspection,
) -> Result<()> {
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
    Ok(())
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
         SET selected_file_ids_json = ?,
             skipped_file_ids_json = ?,
             selection_error = ?,
             status = CASE WHEN ? THEN status ELSE 'review_required' END,
             remote_release_status = CASE WHEN ? THEN remote_release_status ELSE 'review_required' END,
             updated_at = CURRENT_TIMESTAMP
         WHERE job_id = ?",
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
         SET selected = ?, updated_at = CURRENT_TIMESTAMP
         WHERE release_file_id = ?",
    )
    .bind(selected)
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
         SET state = ?,
             state_reason = ?,
             coverage_plan_json = COALESCE(?, coverage_plan_json),
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = ?",
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
         SET state = ?,
             state_reason = ?,
             active = ?,
             completed_at = CASE WHEN ? THEN COALESCE(completed_at, CURRENT_TIMESTAMP) ELSE completed_at END,
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = ?
           AND download_id = ?",
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

    if message.contains("error_code\":35")
        || message.contains("error_code:35")
        || message.contains("infringing file")
        || message.contains("infringing_file")
        || message.contains("file not allowed")
        || message.contains("file_not_allowed")
        || message.contains("content blocked")
        || message.contains("dmca")
        || message.contains("copyright")
    {
        Some(DebridFailureClass::ContentBlocked)
    } else if message.contains("error_code\":21")
        || message.contains("error_code:21")
        || message.contains("too many active downloads")
        || message.contains("too many active")
        || message.contains("maximum allowed active")
        || message.contains("magnet_too_many_active")
    {
        Some(DebridFailureClass::TooManyActiveDownloads)
    } else if message.contains("account_limit_reached")
        || message.contains("service_limit_reached")
        || message.contains("account limit")
        || message.contains("service limit")
    {
        Some(DebridFailureClass::ProviderAccountLimitReached)
    } else if message.contains("error_code\":23")
        || message.contains("error_code:23")
        || message.contains("error_code\":36")
        || message.contains("error_code:36")
        || message.contains("traffic exhausted")
        || message.contains("fair usage limit")
        || message.contains("fair-use")
        || message.contains("fairuse")
        || message.contains("quota")
    {
        Some(DebridFailureClass::QuotaExhausted)
    } else if message.contains("not premium")
        || message.contains("must be premium")
        || message.contains("free users")
        || message.contains("magnet_must_be_premium")
        || message.contains("magnet_no_server")
        || message.contains("permission_denied")
        || message.contains("account locked")
        || message.contains("account restricted")
    {
        Some(DebridFailureClass::ProviderAccountRestricted)
    } else if message.contains("api token")
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
    } else if message.contains("native adapter")
        || message.contains("provider unsupported")
        || message.contains("unsupported provider")
        || message.contains("unsupported_container")
    {
        Some(DebridFailureClass::ProviderUnsupported)
    } else if message.contains("rate limit")
        || message.contains("ratelimit")
        || message.contains("too many requests")
        || message.contains("slow down")
        || message.contains("429")
        || message.contains("rate_limit_reached")
    {
        Some(DebridFailureClass::RateLimited)
    } else if message.contains("provider unavailable")
        || message.contains("service_down")
        || message.contains("semi_permanent_error")
        || message.contains("link_generation_failed")
        || message.contains("transient_error")
        || message.contains("service unavailable")
        || message.contains("temporar")
        || message.contains("unknown_error")
        || message.contains("503")
        || message.contains("502")
        || message.contains("504")
        || message.contains("500")
    {
        Some(DebridFailureClass::ProviderUnavailable)
    } else if message.contains("timed out") || message.contains("timeout") {
        Some(DebridFailureClass::StagingTimeout)
    } else if message.contains("selecting debrid files")
        || message.contains("selectfiles")
        || message.contains("file selection")
        || message.contains("selection failed")
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
    {
        Some(DebridFailureClass::NoSeeds)
    } else if message.contains("provider_stalled")
        || message.contains("stalled")
        || message.contains("no progress")
    {
        Some(DebridFailureClass::ProviderStalled)
    } else if message.contains("file list")
        || message.contains("no files")
        || message.contains("torrent info")
        || message.contains("magnet_invalid_id")
        || message.contains("not_found")
        || message.contains("not found")
        || message.contains("not cached")
        || message.contains("expired")
        || message.contains("not_found_or_expired")
    {
        if message.contains("not found")
            || message.contains("not_found")
            || message.contains("expired")
        {
            Some(DebridFailureClass::NotFoundExpired)
        } else {
            Some(DebridFailureClass::FileListUnavailable)
        }
    } else if message.contains("magnet_error")
        || message.contains("magnet_invalid")
        || message.contains("magnet_invalid_file")
        || message.contains("magnet rejected")
        || message.contains("invalid magnet")
        || message.contains("bad magnet")
        || message.contains("torrent file invalid")
        || message.contains("torrent invalid")
        || message.contains("invalid torrent")
        || message.contains("error_code\":30")
        || message.contains("error_code:30")
        || message.contains("error_code\":29")
        || message.contains("error_code:29")
        || message.contains("service_unsupported")
        || message.contains("service unsupported")
        || message.contains("unsupported hoster")
        || message.contains("unsupported_hoster")
        || message.contains("invalid_request")
        || message.contains("permanent_error")
    {
        if message.contains("magnet")
            || message.contains("torrent")
            || message.contains("invalid_request")
        {
            Some(DebridFailureClass::InvalidSource)
        } else {
            Some(DebridFailureClass::MagnetRejected)
        }
    } else if message.contains("connection")
        || message.contains("connect")
        || message.contains("network")
    {
        Some(DebridFailureClass::ProviderUnavailable)
    } else if message.contains("transfer")
        || message.contains("downloading")
        || message.contains("download failed")
    {
        Some(DebridFailureClass::TransferFailed)
    } else {
        Some(DebridFailureClass::Unknown)
    }
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
            if let Err(err) = process_debrid_job(&worker_state, &store, &paths, job.clone()).await {
                mark_debrid_job_status(
                    &worker_state.db_pool,
                    job.job_id,
                    "failed",
                    Some(&err.to_string()),
                )
                .await?;
            }
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

async fn process_debrid_job(
    state: &AppState,
    store: &ExtensionStore<'_>,
    paths: &RuntimePaths,
    job: DebridDownloadJob,
) -> Result<()> {
    if job.status == "paused" || job.status == "cancelled" {
        return Ok(());
    }
    let factory = DebridAdapterFactory::from_state(state);
    let adapter = factory
        .adapter_for_job_implementation(
            store,
            job.instance_id,
            job.provider_implementation.as_deref(),
        )
        .await?;
    let mut job = job;
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
        update_debrid_job_from_inspection(&state.db_pool, job.job_id, &inspection).await?;
        job = load_debrid_job(&state.db_pool, job.job_id)
            .await?
            .ok_or_else(|| anyhow!("Debrid job disappeared during refresh"))?;
        if matches!(
            inspection.release.status,
            DebridReleaseStatus::WaitingFiles | DebridReleaseStatus::Downloaded
        ) && job.source_kind == "magnet"
            && job.selected_file_ids.is_empty()
            && !inspection.files.is_empty()
            && let Some(release_id) = job.release_id
            && let Some(release) = crate::acquisition::release_resolution::store::get_release(
                &state.db_pool,
                release_id,
            )
            .await?
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
            let refinement = persist_debrid_file_list_and_refine_coverage(
                &state.db_pool,
                &release,
                &options,
                &inspection,
            )
            .await?;
            let refinement_state = refinement.state;
            let refinement_state_reason = refinement.state_reason.clone();
            let refinement_shape = refinement.shape.clone();
            let refinement_coverage_plan = merge_debrid_coverage_plans(
                release.coverage_plan.clone(),
                refinement.coverage_plan.clone(),
            );
            let refinement_job_state = refinement.job_state;
            let refinement_job_state_reason = refinement.job_state_reason.clone();
            let updated_release = upsert_debrid_acquisition_release(
                &state.db_pool,
                job.provider_id,
                &job.source,
                &job.source_kind,
                &options,
                Some(&inspection.release.remote_release_id),
                Some(&job.job_id.to_string()),
                refinement_state,
                refinement_state_reason.as_deref(),
                refinement_shape,
                Some(merge_debrid_provider_provenance(
                    refinement_coverage_plan,
                    job.provider_id,
                    adapter.implementation(),
                    &provider_capabilities,
                    Some(&inspection.release.remote_release_id),
                    Some(inspection.release.status.as_str()),
                    &job.source_kind,
                    Some(job.job_id),
                )),
            )
            .await?
            .unwrap_or(release);
            upsert_debrid_release_job(
                &state.db_pool,
                &updated_release,
                job.provider_id,
                job.job_id,
                Some(&inspection.release.remote_release_id),
                refinement_job_state,
                refinement_job_state_reason
                    .as_deref()
                    .unwrap_or("Debrid release inspected and staged."),
            )
            .await?;
            let _ = apply_debrid_file_selection_policy(
                &state.db_pool,
                &*adapter,
                job.job_id,
                &updated_release,
                &inspection,
            )
            .await?;
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

async fn materialize_debrid_links(
    state: &AppState,
    adapter: &dyn DebridProviderAdapter,
    paths: &RuntimePaths,
    job: &DebridDownloadJob,
) -> Result<()> {
    mark_debrid_job_status(&state.db_pool, job.job_id, "materializing", None).await?;
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
    mark_debrid_job_completed(&state.db_pool, job.job_id, local_path.as_deref()).await?;
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
                    update_debrid_job_from_inspection(&state.db_pool, job.job_id, &inspection)
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
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
         WHERE provider_id = ?
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
         WHERE status NOT IN ('completed', 'failed', 'cancelled', 'paused', 'review_required', 'materializing')
         ORDER BY updated_at ASC
         LIMIT ?"
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
         WHERE instance_id = ?
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
         WHERE provider_id = ?
           AND (remote_torrent_id IS NOT NULL OR remote_release_id IS NOT NULL)
           AND status NOT IN ('completed', 'failed', 'cancelled', 'paused', 'review_required', 'materializing')
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
         WHERE provider_id = ?
           AND (job_id = ? OR remote_torrent_id = ? OR remote_download_id = ? OR remote_release_id = ?)
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
         WHERE job_id = ?
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

async fn update_debrid_job_from_inspection(
    pool: &sqlx::AnyPool,
    job_id: Uuid,
    inspection: &DebridReleaseInspection,
) -> Result<()> {
    let status = debrid_status_to_job_status(inspection.release.status);
    let links = selected_link_urls_from_inspection(inspection);
    let links_json = serde_json::to_string(&links)?;
    let provider_capabilities_json = serde_json::to_string(&inspection.capabilities)?;
    let provider_status = debrid_provider_status_from_inspection(inspection);
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
    sqlx::query::<sqlx::Any>(
        "UPDATE debrid_download_jobs
         SET status = ?, remote_release_status = ?, display_name = COALESCE(display_name, ?),
             links_json = CASE WHEN ? != '[]' THEN ? ELSE links_json END,
             progress = ?, downloaded_bytes = ?, total_bytes = ?, download_rate_bps = ?,
             provider_implementation = ?,
             remote_release_id = COALESCE(remote_release_id, ?),
             provider_capabilities_json = ?,
             provider_status_json = ?,
             last_error = ?,
             selection_mode = ?,
             selected_file_ids_json = CASE WHEN ? != '[]' THEN ? ELSE selected_file_ids_json END,
             skipped_file_ids_json = CASE WHEN ? != '[]' THEN ? ELSE skipped_file_ids_json END,
             updated_at = CURRENT_TIMESTAMP
         WHERE job_id = ?",
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
    .execute(pool)
    .await?;
    if inspection.release.status == DebridReleaseStatus::Failed
        && let Some(job) = load_debrid_job(pool, job_id).await?
    {
        record_debrid_release_failure_evidence(pool, &job).await?;
    }
    Ok(())
}

async fn update_debrid_job_links(
    pool: &sqlx::AnyPool,
    job_id: Uuid,
    links: &[String],
) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE debrid_download_jobs SET links_json = ?, updated_at = CURRENT_TIMESTAMP WHERE job_id = ?",
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
         SET status = 'materializing', downloaded_bytes = ?, total_bytes = COALESCE(?, total_bytes),
             progress = ?, download_rate_bps = ?, updated_at = CURRENT_TIMESTAMP
         WHERE job_id = ?",
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
        "UPDATE debrid_download_jobs SET local_path = ?, updated_at = CURRENT_TIMESTAMP WHERE job_id = ?",
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
    sqlx::query::<sqlx::Any>(
        "UPDATE debrid_download_jobs
         SET status = ?, remote_release_status = ?, last_error = ?, updated_at = CURRENT_TIMESTAMP
         WHERE job_id = ?",
    )
    .bind(status)
    .bind(status)
    .bind(error)
    .bind(job_id.to_string())
    .execute(pool)
    .await?;
    if let Some(job) = load_debrid_job(pool, job_id).await? {
        match status {
            "failed" => record_debrid_release_failure_evidence(pool, &job).await?,
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
        "UPDATE debrid_download_jobs SET last_error = ?, updated_at = CURRENT_TIMESTAMP WHERE job_id = ?",
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
    provider_id: Uuid,
    adapter: &(impl DebridProviderAdapter + ?Sized),
    capabilities: &DebridProviderCapabilities,
    source_kind: &str,
    error: &anyhow::Error,
) -> Result<()> {
    let Some(release) = release else {
        return Ok(());
    };
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
) -> Result<()> {
    sqlx::query::<sqlx::Any>(
        "UPDATE debrid_download_jobs
         SET status = 'completed', local_path = COALESCE(?, local_path), progress = 1.0,
             download_rate_bps = 0, completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
         WHERE job_id = ?",
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
    Ok(())
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
         WHERE download_id = ?",
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
            raw: Some(serde_json::to_value(&torrent)?),
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
        raw: Some(serde_json::to_value(torrent)?),
    })
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
        sync::{Arc, Mutex},
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
        assert_eq!(release.resolver_kind, ReleaseResolverKind::MovieSingle);
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
        let adapter = FakeDebridAdapter::new();
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
            "UPDATE debrid_download_jobs SET remote_release_id = NULL WHERE job_id = ?",
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
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
        fail_select: bool,
    }

    #[derive(Default)]
    struct FakeDebridState {
        next_id: u64,
        releases: HashMap<String, FakeDebridRelease>,
    }

    #[derive(Clone)]
    struct FakeDebridRelease {
        release: DebridRemoteRelease,
        files: Vec<DebridRemoteFile>,
        selected_file_ids: Vec<String>,
    }

    impl FakeDebridAdapter {
        fn new() -> Self {
            Self {
                state: Arc::new(Mutex::new(FakeDebridState::default())),
                fail_select: false,
            }
        }

        fn failing_select() -> Self {
            Self {
                state: Arc::new(Mutex::new(FakeDebridState::default())),
                fail_select: true,
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
             SET remote_torrent_id = ?, remote_release_id = ?, updated_at = CURRENT_TIMESTAMP
             WHERE job_id = ?",
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
             ) VALUES (?, ?, ?, ?, ?, ?, ?)",
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
             ) VALUES (?, ?, ?, ?, ?)",
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
             ) VALUES (?, ?, ?, ?, ?, ?)",
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
             ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?, ?)",
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
                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
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
             ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?, ?)",
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
             ) VALUES (?, ?, ?, ?, ?, ?)",
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
            let mut state = self.state.lock().unwrap();
            state.next_id += 1;
            let remote_release_id = format!("fake-release-{}", state.next_id);
            let release = DebridRemoteRelease {
                provider_implementation: self.implementation().to_string(),
                remote_release_id: remote_release_id.clone(),
                display_name: Some(magnet.to_string()),
                status: DebridReleaseStatus::WaitingFiles,
                raw_status: Some("waiting_files".to_string()),
                raw: None,
            };
            state.releases.insert(
                remote_release_id.clone(),
                FakeDebridRelease {
                    release: release.clone(),
                    files: vec![
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
                    ],
                    selected_file_ids: Vec::new(),
                },
            );
            Ok(release)
        }

        async fn inspect_release(
            &self,
            remote_release_id: &str,
        ) -> Result<DebridReleaseInspection> {
            let state = self.state.lock().unwrap();
            let release = state
                .releases
                .get(remote_release_id)
                .ok_or_else(|| anyhow!("fake release not found"))?;
            let status = if release.selected_file_ids.is_empty() {
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
            Ok(self
                .state
                .lock()
                .unwrap()
                .releases
                .remove(remote_release_id)
                .is_some())
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
             SET config_json = ?
             WHERE instance_id = ?",
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
    fn debrid_selection_policy_reviews_unsupported_file_selection() {
        let release = test_debrid_release(ReleaseKind::SeasonPack, ReleaseConfidence::High);
        let file = test_release_file(
            release.release_id,
            "file-1",
            "Show/Season 01/Show.S01E01.mkv",
            true,
        );
        let files = vec![file.clone()];
        let coverage = vec![test_coverage(release.release_id, file.release_file_id)];
        let inspection = test_debrid_inspection(false, Vec::new(), Vec::new(), None);

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
            "UPDATE debrid_download_jobs SET provider_implementation = NULL WHERE job_id = ?",
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
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
             WHERE subscription_id = ?
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
             WHERE subscription_id = ?
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
