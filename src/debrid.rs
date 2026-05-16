use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use reqwest::{Client, Method, StatusCode};
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
        ReleaseCoverageState, ReleaseJobState, ReleaseKind, ReleaseResolverKind,
    },
    store::{
        get_release, get_release_by_download_id, list_release_coverage, list_release_files,
        upsert_release, upsert_release_coverage, upsert_release_file, upsert_release_job,
    },
    tv::{TvCoverageOptions, TvReleaseFileInput, TvSonarrStyleResolver, TvTarget},
};
use crate::acquisition::subscriptions::list_subscription_targets;
use crate::db::models::{
    ExtensionKind, ExtensionTrustLevel, MediaType, ProviderHealthState, ProviderReadinessPhase,
    SecretScope, SlotCardinality,
};
use crate::download_broker::{DEBRID_DEFAULT_LOGICAL_ID, DEFAULT_ROUTE_OWNER_ID};
use crate::extensions::store::{ExtensionStore, NewExtension, NewExtensionInstance, NewProvider};
use crate::orchestrator::model::ProviderEndpoint;
use crate::orchestrator::planner::stable_provider_id;
use crate::runtime::RuntimePaths;
use crate::state::AppState;

pub const REAL_DEBRID_EXTENSION_ID: &str = "elixir.modules.real_debrid";
pub const REAL_DEBRID_IMPLEMENTATION: &str = "real_debrid";
pub const REAL_DEBRID_TOKEN_SECRET_KEY: &str = "real_debrid_api_token";

const REAL_DEBRID_API_BASE: &str = "https://api.real-debrid.com/rest/1.0";
const REAL_DEBRID_POLL_INTERVAL_SECONDS: u64 = 20;
const REAL_DEBRID_USER_AGENT: &str = "Elixir/0.1 Real-Debrid";
const MAX_DOWNLOAD_FILE_NAME_LEN: usize = 180;
const DEBRID_SELECTION_POLICY_VERSION: &str = "rr4f-deterministic-selection-v1";

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
    ProviderUnavailable,
    ProviderUnsupported,
    MagnetRejected,
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
            Self::ProviderUnavailable => "provider_unavailable",
            Self::ProviderUnsupported => "provider_unsupported",
            Self::MagnetRejected => "magnet_rejected",
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct DebridProviderError {
    pub kind: DebridProviderErrorKind,
    pub provider_code: Option<String>,
    pub message: String,
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
        if token.trim().is_empty() {
            bail!("Real-Debrid API token is required");
        }
        Ok(Self {
            http: Client::builder()
                .user_agent(REAL_DEBRID_USER_AGENT)
                .timeout(Duration::from_secs(30))
                .build()
                .context("building Real-Debrid HTTP client")?,
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
        let body = response.text().await.context("reading Real-Debrid body")?;
        if !status.is_success() {
            bail!(
                "Real-Debrid API returned {status}: {}",
                redacted_body(&body)
            );
        }
        serde_json::from_str(&body).context("parsing Real-Debrid response")
    }

    async fn request_empty(&self, method: Method, path: &str, form: &[(&str, &str)]) -> Result<()> {
        let status = self.request_empty_status(method, path, form).await?;
        if status.is_success() || status == StatusCode::ACCEPTED {
            Ok(())
        } else {
            bail!("Real-Debrid API returned {status}")
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
                "Real-Debrid API returned {status}: {}",
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
        request.send().await.context("calling Real-Debrid API")
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
            bail!("Real-Debrid file selection requires at least one file id");
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
            bail!("Real-Debrid did not return a downloadable link");
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

pub async fn ensure_real_debrid_builtin(state: &AppState) -> Result<()> {
    let store = ExtensionStore::new(&state.db_pool);
    let existing = store.get_extension(REAL_DEBRID_EXTENSION_ID).await?;
    let enabled = existing.as_ref().map(|item| item.enabled).unwrap_or(true);
    store
        .upsert_extension(&NewExtension {
            extension_id: REAL_DEBRID_EXTENSION_ID.to_string(),
            name: "Real-Debrid".to_string(),
            version: "0.1.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: Some("Elixir".to_string()),
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Verified,
            manifest_json: real_debrid_manifest_json(),
            package_hash: None,
            enabled,
        })
        .await?;

    let mut instances = store.list_instances(Some(REAL_DEBRID_EXTENSION_ID)).await?;
    if instances.is_empty() {
        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: REAL_DEBRID_EXTENSION_ID.to_string(),
                instance_name: "default".to_string(),
                config_json: Some(json!({ "materialize": true })),
                enabled: true,
            })
            .await?;
        instances = store.list_instances(Some(REAL_DEBRID_EXTENSION_ID)).await?;
    }

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
    let provider_id = stable_provider_id(instance.instance_id, "debrid.resolver", "default");
    let endpoint = ProviderEndpoint::new(
        "https".to_string(),
        "api.real-debrid.com".to_string(),
        443,
        Some("/rest/1.0".to_string()),
        None,
    )?;
    let has_token = real_debrid_token_for_instance(state, &store, instance.instance_id)
        .await
        .is_ok();
    store
        .upsert_provider(&NewProvider {
            provider_id,
            instance_id: instance.instance_id,
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
            health_state: if has_token {
                ProviderHealthState::Healthy
            } else {
                ProviderHealthState::Unknown
            },
        })
        .await?;
    store
        .upsert_provider_readiness(
            provider_id,
            if has_token {
                ProviderReadinessPhase::DriverReady
            } else {
                ProviderReadinessPhase::Unknown
            },
            if has_token {
                Some("Real-Debrid API token is present.")
            } else {
                Some("Add a Real-Debrid API token to enable debrid acquisition.")
            },
        )
        .await?;
    Ok(())
}

pub async fn start_debrid_materializer_loop(state: AppState) {
    let mut interval =
        tokio::time::interval(Duration::from_secs(REAL_DEBRID_POLL_INTERVAL_SECONDS));
    loop {
        interval.tick().await;
        if let Err(err) = process_debrid_jobs_once(&state).await {
            tracing::warn!("Real-Debrid materializer pass failed: {err}");
        }
    }
}

pub async fn real_debrid_token_for_instance(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
) -> Result<String> {
    let secret = store
        .get_secret(
            SecretScope::Instance,
            Some(instance_id),
            REAL_DEBRID_TOKEN_SECRET_KEY,
        )
        .await?
        .ok_or_else(|| anyhow!("Real-Debrid API token is not configured"))?;
    state
        .secrets
        .decrypt(&secret.value_encrypted)
        .context("decrypting Real-Debrid API token")
}

pub async fn test_real_debrid_account(
    state: &AppState,
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
) -> Result<RealDebridUser> {
    let token = real_debrid_token_for_instance(state, store, instance_id).await?;
    let account = RealDebridClient::new(token)?.test_account().await?;
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
    if !is_real_debrid_implementation(provider_implementation) {
        bail!("the selected debrid provider does not have a native adapter yet");
    }
    let token = real_debrid_token_for_instance(state, store, instance_id).await?;
    let adapter = RealDebridClient::new(token)?;
    submit_debrid_with_adapter(
        &state.db_pool,
        provider_id,
        instance_id,
        source,
        options,
        &adapter,
    )
    .await
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
        None,
    )
    .await?;

    if !options.paused {
        match source_kind {
            "magnet" => {
                let submitted = match adapter.submit_magnet(source).await {
                    Ok(submitted) => submitted,
                    Err(err) => {
                        record_debrid_release_failure_without_job(pool, release.as_ref(), &err)
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
                        existing.coverage_plan.clone(),
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
                        record_debrid_release_failure_without_job(pool, release.as_ref(), &err)
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
                    status = "rd_downloaded".to_string();
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
            existing.coverage_plan.clone(),
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
            if let Some(existing) = release.as_ref() {
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
                    refinement.coverage_plan,
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
        MediaType::Movie => Ok(DebridCoverageRefinement {
            coverage_plan: Some(json!({
                "source": "debrid_provider_file_list",
                "providerImplementation": inspection.release.provider_implementation,
                "remoteReleaseId": inspection.release.remote_release_id,
                "files": inspection.files.len(),
                "reviewReasons": ["movie_file_selection_policy_pending"]
            })),
            state: AcquisitionReleaseState::ReviewRequired,
            state_reason: Some("Movie debrid file-list policy is pending RR-4F.".to_string()),
            ..base
        }),
    }
}

async fn persist_debrid_release_files(
    pool: &sqlx::AnyPool,
    release: &AcquisitionRelease,
    files: &[DebridRemoteFile],
) -> Result<HashMap<String, Uuid>> {
    let mut file_ids = HashMap::new();
    for file in files {
        let parsed = parsed_file_metadata(release.media_type, &file.path);
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
            path: file.path.clone(),
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
        "rd_downloaded" | "rd_downloading" => ReleaseJobState::Downloading,
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
        "rd_downloaded" | "rd_downloading" => AcquisitionReleaseState::Downloading,
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
        .filter_map(|coverage| coverage.release_file_id)
        .filter_map(|release_file_id| files_by_release_file_id.get(&release_file_id))
        .filter(|file| file.selectable)
        .filter_map(|file| {
            file.provider_file_id
                .clone()
                .or_else(|| file.file_id.clone())
        })
        .collect::<BTreeSet<_>>();

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

    let selected_file_ids = json_string_array(policy.get("selectedFileIds"));
    if selected_file_ids.is_empty() {
        return None;
    }
    let skipped_file_ids = json_string_array(policy.get("skippedFileIds"));
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
    let selected_set = selected_file_ids.iter().cloned().collect::<BTreeSet<_>>();
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
    for entry in coverage {
        let selected = entry
            .release_file_id
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
                release_file_id: entry.release_file_id,
                target_id: entry.target_id,
                coverage_kind: entry.coverage_kind,
                confidence: entry.confidence,
                score: entry.score,
                reason: entry.reason.clone(),
                state: if decision.is_approved() && selected {
                    ReleaseCoverageState::Selected
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
    sqlx::query::<sqlx::Any>(
        "UPDATE acquisition_release_jobs
         SET state = ?,
             state_reason = ?,
             active = ?,
             updated_at = CURRENT_TIMESTAMP
         WHERE release_id = ?
           AND download_id = ?",
    )
    .bind(state.as_str())
    .bind(reason)
    .bind(!matches!(
        state,
        ReleaseJobState::Completed | ReleaseJobState::Failed | ReleaseJobState::Cancelled
    ))
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
    merge_debrid_evidence_object(coverage_plan, "debridFailure", evidence)
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
    match implementation {
        REAL_DEBRID_IMPLEMENTATION => "Real-Debrid".to_string(),
        "all_debrid" | "alldebrid" => "AllDebrid".to_string(),
        "torbox" => "TorBox".to_string(),
        "premiumize" => "Premiumize".to_string(),
        other => other
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
    } else if failure_class.is_some() && job.source_kind == "magnet" {
        "eligible_if_candidate_supports_torrent_route".to_string()
    } else if failure_class.is_some() {
        "not_available_for_hoster_source".to_string()
    } else {
        "not_needed".to_string()
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

    if message.contains("api token")
        || message.contains("unauthorized")
        || message.contains("forbidden")
        || message.contains("401")
        || message.contains("403")
    {
        Some(DebridFailureClass::ProviderAuthMissing)
    } else if message.contains("native adapter")
        || message.contains("provider unsupported")
        || message.contains("unsupported provider")
    {
        Some(DebridFailureClass::ProviderUnsupported)
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
    } else if message.contains("file list")
        || message.contains("no files")
        || message.contains("torrent info")
    {
        Some(DebridFailureClass::FileListUnavailable)
    } else if message.contains("magnet_error")
        || message.contains("magnet rejected")
        || message.contains("invalid magnet")
        || message.contains("bad magnet")
    {
        Some(DebridFailureClass::MagnetRejected)
    } else if message.contains("503")
        || message.contains("502")
        || message.contains("504")
        || message.contains("service unavailable")
        || message.contains("connection")
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

pub async fn load_real_debrid_progress(
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

pub async fn cancel_real_debrid_job(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider_id: Uuid,
    instance_id: Uuid,
    download_id: &str,
) -> Result<bool> {
    let Some(job) = find_debrid_job(&state.db_pool, provider_id, download_id).await? else {
        return Ok(false);
    };
    if let Some(remote_release_id) = job
        .remote_release_id
        .as_deref()
        .or(job.remote_torrent_id.as_deref())
    {
        if let Ok(token) = real_debrid_token_for_instance(state, store, instance_id).await {
            let _ = RealDebridClient::new(token)?
                .delete_release(remote_release_id)
                .await;
        }
    }
    mark_debrid_job_status(&state.db_pool, job.job_id, "cancelled", None).await?;
    Ok(true)
}

async fn process_debrid_jobs_once(state: &AppState) -> Result<()> {
    let jobs = list_active_debrid_jobs(&state.db_pool, 8).await?;
    if jobs.is_empty() {
        return Ok(());
    }
    let store = ExtensionStore::new(&state.db_pool);
    let paths = RuntimePaths::from_roots(
        &state.settings.extensions.storage_root,
        &state.settings.library.local_root,
    );
    for job in jobs {
        if let Err(err) = process_debrid_job(state, &store, &paths, job.clone()).await {
            mark_debrid_job_status(&state.db_pool, job.job_id, "failed", Some(&err.to_string()))
                .await?;
        }
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
    let token = real_debrid_token_for_instance(state, store, job.instance_id).await?;
    let adapter = RealDebridClient::new(token)?;
    let mut job = job;
    let remote_torrent_release_id = job.remote_torrent_id.clone().or_else(|| {
        (job.source_kind == "magnet")
            .then(|| job.remote_release_id.clone())
            .flatten()
    });
    if let Some(remote_release_id) = remote_torrent_release_id {
        let inspection = adapter.inspect_release(&remote_release_id).await?;
        update_debrid_job_from_inspection(&state.db_pool, job.job_id, &inspection).await?;
        job = load_debrid_job(&state.db_pool, job.job_id)
            .await?
            .ok_or_else(|| anyhow!("Real-Debrid job disappeared during refresh"))?;
        if inspection.release.status == DebridReleaseStatus::WaitingFiles {
            if let Some(release_id) = job.release_id
                && let Some(release) = crate::acquisition::release_resolution::store::get_release(
                    &state.db_pool,
                    release_id,
                )
                .await?
            {
                let options = DebridSubmitOptions {
                    owner_id: &job.owner_id,
                    category: job.category.as_deref(),
                    name: job.display_name.as_deref(),
                    paused: false,
                    release_context: None,
                };
                let _ = persist_debrid_file_list_and_refine_coverage(
                    &state.db_pool,
                    &release,
                    &options,
                    &inspection,
                )
                .await?;
                let _ = apply_debrid_file_selection_policy(
                    &state.db_pool,
                    &adapter,
                    job.job_id,
                    &release,
                    &inspection,
                )
                .await?;
            }
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

    materialize_debrid_links(state, &adapter, paths, &job).await
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
    let mut response = client
        .get(url)
        .send()
        .await
        .context("requesting Real-Debrid download")?;
    let status = response.status();
    if !status.is_success() {
        bail!("Real-Debrid download returned {status}");
    }
    let total = expected_size.or_else(|| response.content_length());
    if let Some(parent) = target_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let tmp_path = target_path.with_extension("elixir-part");
    let mut file = tokio::fs::File::create(&tmp_path)
        .await
        .with_context(|| format!("creating '{}'", tmp_path.display()))?;
    let mut downloaded = 0_u64;
    let mut last_update = Instant::now();
    let mut last_downloaded = 0_u64;
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
    instance_id: Uuid,
) -> Result<()> {
    let token = real_debrid_token_for_instance(state, store, instance_id).await?;
    let adapter = RealDebridClient::new(token)?;
    let jobs = list_refreshable_debrid_jobs(&state.db_pool, provider_id).await?;
    for job in jobs {
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
                    update_debrid_job_error(&state.db_pool, job.job_id, &err.to_string()).await?;
                }
            }
        }
    }
    Ok(())
}

async fn insert_debrid_job(pool: &sqlx::AnyPool, job: &DebridDownloadJob) -> Result<()> {
    let links_json = serde_json::to_string(&job.links)?;
    let provider_capabilities_json = json_value_to_string(job.provider_capabilities.as_ref())?;
    let selected_file_ids_json = serde_json::to_string(&job.selected_file_ids)?;
    let skipped_file_ids_json = serde_json::to_string(&job.skipped_file_ids)?;
    sqlx::query::<sqlx::Any>(
        "INSERT INTO debrid_download_jobs (
            job_id, provider_id, instance_id, owner_id, source, source_kind, category,
            display_name, remote_torrent_id, remote_download_id, status, local_path,
            links_json, progress, downloaded_bytes, total_bytes, download_rate_bps, last_error,
            provider_implementation, remote_release_id, remote_release_status,
            provider_capabilities_json, selection_mode, selected_file_ids_json,
            skipped_file_ids_json, selection_error, release_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
         WHERE status NOT IN ('completed', 'failed', 'cancelled', 'paused', 'review_required')
         ORDER BY updated_at ASC
         LIMIT ?"
    ))
    .bind(limit)
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(|row| map_debrid_job(&row)).collect()
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
           AND status NOT IN ('completed', 'failed', 'cancelled', 'review_required')
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

async fn update_debrid_job_from_inspection(
    pool: &sqlx::AnyPool,
    job_id: Uuid,
    inspection: &DebridReleaseInspection,
) -> Result<()> {
    let status = debrid_status_to_job_status(inspection.release.status);
    let links = selected_link_urls_from_inspection(inspection);
    let links_json = serde_json::to_string(&links)?;
    let provider_capabilities_json = serde_json::to_string(&inspection.capabilities)?;
    let (selected_file_ids, skipped_file_ids) = inspection
        .selection
        .as_ref()
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
             selection_mode = ?,
             selected_file_ids_json = ?,
             skipped_file_ids_json = ?,
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
    .bind(
        inspection
            .capabilities
            .file_selection_mode
            .as_persistence_value(),
    )
    .bind(selected_file_ids_json)
    .bind(skipped_file_ids_json)
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
    if status == "failed"
        && let Some(job) = load_debrid_job(pool, job_id).await?
    {
        record_debrid_release_failure_evidence(pool, &job).await?;
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
        "message": message,
        "fallbackState": "eligible_if_candidate_supports_torrent_route",
        "stage": "provider_submit",
    });
    let coverage_plan =
        merge_debrid_evidence_object(release.coverage_plan.clone(), "debridFailure", evidence);
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
    Ok(())
}

fn map_debrid_job(row: &sqlx::any::AnyRow) -> Result<DebridDownloadJob> {
    let job_id_raw: String = row.try_get("job_id")?;
    let provider_id_raw: String = row.try_get("provider_id")?;
    let instance_id_raw: String = row.try_get("instance_id")?;
    let links_raw: String = row.try_get("links_json")?;
    let provider_capabilities_raw =
        empty_string_to_none(row.try_get::<String, _>("provider_capabilities_json")?);
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

fn real_debrid_manifest_json() -> Value {
    json!({
        "id": REAL_DEBRID_EXTENSION_ID,
        "version": "0.1.0",
        "kind": "module",
        "name": "Real-Debrid",
        "description": "Native Real-Debrid acquisition provider for high-speed debrid downloads.",
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
                "description": "Real-Debrid API token used by Elixir to resolve and materialize debrid downloads.",
                "type": "password",
                "required": true,
                "secret": true,
                "ownership": "managed",
                "storage": {
                    "type": "instance_secret",
                    "key": REAL_DEBRID_TOKEN_SECRET_KEY
                }
            }],
            "native_only": [{
                "id": "streaming",
                "title": "Streaming",
                "description": "This pass implements local downloads only. Real-Debrid streaming remains reserved for a future playback integration."
            }]
        }
    })
}

pub fn is_real_debrid_implementation(value: Option<&str>) -> bool {
    value
        .map(str::trim)
        .map(|value| value.eq_ignore_ascii_case(REAL_DEBRID_IMPLEMENTATION))
        .unwrap_or(false)
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
        DebridReleaseStatus::Downloaded => "rd_downloaded",
        DebridReleaseStatus::Selected | DebridReleaseStatus::Transferring => "rd_downloading",
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
mod tests {
    use super::*;
    use crate::acquisition::release_resolution::models::{
        ReleaseCoverageKind, ReleaseCoverageState,
    };
    use crate::{config::DatabaseConfig, db::Database};
    use axum::{
        Json, Router,
        extract::{Form, Path as AxumPath, State},
        http::StatusCode as HttpStatusCode,
        response::IntoResponse,
        routing::{delete as axum_delete, get, post},
    };
    use chrono::Utc;
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };
    use tokio::{net::TcpListener, sync::oneshot};

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

    #[derive(Clone, Default)]
    struct MockRealDebridState {
        added_magnets: Arc<Mutex<Vec<String>>>,
        selected_files: Arc<Mutex<Vec<String>>>,
        deleted_releases: Arc<Mutex<Vec<String>>>,
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
        Json(json!({
            "id": "unrestricted-1",
            "filename": "Show.S01E01.mkv",
            "filesize": 2048,
            "download": "https://download.real-debrid.test/Show.S01E01.mkv"
        }))
        .into_response()
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
            "rd_downloaded"
        );
        assert_eq!(
            real_debrid_status_to_job_status(Some("magnet_error")),
            "failed"
        );
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
            Some(DebridFailureClass::MagnetRejected)
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
        assert_eq!(job.status, "rd_downloading");
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

    #[test]
    fn sanitizes_download_paths() {
        assert_eq!(safe_path_segment("TV Shows/../x"), "TV-Shows-..-x");
        assert_eq!(safe_file_name("../Movie: 2024.mkv"), "_Movie_ 2024.mkv");
    }
}
