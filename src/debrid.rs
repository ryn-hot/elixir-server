use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use reqwest::{Client, Method, StatusCode};
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::Row;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

use crate::db::models::{
    ExtensionKind, ExtensionTrustLevel, ProviderHealthState, ProviderReadinessPhase, SecretScope,
    SlotCardinality,
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

#[derive(Debug, Clone)]
pub struct DebridBrokerProgressItem {
    pub id: String,
    pub name: Option<String>,
    pub state: Option<String>,
    pub category: Option<String>,
    pub progress: Option<f64>,
    pub downloaded_bytes: Option<u64>,
    pub total_bytes: Option<u64>,
    pub remaining_bytes: Option<u64>,
    pub download_rate_bps: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct DebridSubmitOptions<'a> {
    pub owner_id: &'a str,
    pub category: Option<&'a str>,
    pub name: Option<&'a str>,
    pub paused: bool,
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
    status: String,
    local_path: Option<String>,
    links: Vec<String>,
    progress: Option<f64>,
    downloaded_bytes: Option<u64>,
    total_bytes: Option<u64>,
    download_rate_bps: Option<u64>,
    last_error: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct RealDebridUser {
    pub username: String,
}

#[derive(Debug, Deserialize)]
struct RealDebridAddResponse {
    id: String,
    #[serde(default)]
    uri: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RealDebridTorrent {
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
    speed: Option<u64>,
}

#[derive(Debug, Deserialize)]
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
    RealDebridClient::new(token)?.user().await
}

pub async fn submit_real_debrid(
    state: &AppState,
    store: &ExtensionStore<'_>,
    provider_id: Uuid,
    instance_id: Uuid,
    source: &str,
    options: DebridSubmitOptions<'_>,
) -> Result<Uuid> {
    let source_kind = debrid_source_kind(source)?;
    let job_id = Uuid::new_v4();
    let token = real_debrid_token_for_instance(state, store, instance_id).await?;
    let client = RealDebridClient::new(token)?;
    let mut remote_torrent_id = None;
    let mut remote_download_id = None;
    let mut links = Vec::new();
    let mut status = if options.paused {
        "paused".to_string()
    } else {
        "submitted".to_string()
    };

    if !options.paused {
        match source_kind {
            "magnet" => {
                let added = client.add_magnet(source).await?;
                let _ = added.uri.as_deref();
                remote_torrent_id = Some(added.id.clone());
                status = "waiting_files_selection".to_string();
                if let Ok(info) = client.torrent_info(&added.id).await {
                    status = real_debrid_status_to_job_status(info.status.as_deref());
                    if can_select_all_files(info.status.as_deref()) {
                        match client.select_files(&added.id, "all").await {
                            Ok(()) => status = "rd_downloading".to_string(),
                            Err(err) => {
                                tracing::debug!(
                                    remote_torrent_id = %added.id,
                                    "Real-Debrid select all deferred: {err}"
                                );
                            }
                        }
                    }
                }
            }
            "hoster" => {
                let unrestricted = client.unrestrict_link(source).await?;
                if let Some(id) = unrestricted.id {
                    remote_download_id = Some(id);
                }
                if unrestricted.download.is_some() {
                    links.push(source.to_string());
                    status = "rd_downloaded".to_string();
                } else {
                    status = "failed".to_string();
                }
            }
            other => bail!("unsupported debrid source kind '{other}'"),
        }
    }

    insert_debrid_job(
        &state.db_pool,
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
    Ok(job_id)
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
            state: Some(job.status),
            category: job.category,
            progress: job.progress,
            downloaded_bytes: job.downloaded_bytes,
            total_bytes: job.total_bytes,
            remaining_bytes: remaining_bytes(job.downloaded_bytes, job.total_bytes),
            download_rate_bps: job.download_rate_bps,
        })
        .collect())
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
    if let Some(remote_torrent_id) = job.remote_torrent_id.as_deref() {
        if let Ok(token) = real_debrid_token_for_instance(state, store, instance_id).await {
            let _ = RealDebridClient::new(token)?
                .delete_torrent(remote_torrent_id)
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
    let client = RealDebridClient::new(token)?;
    let mut job = job;
    if let Some(remote_torrent_id) = job.remote_torrent_id.clone() {
        let info = client.torrent_info(&remote_torrent_id).await?;
        update_debrid_job_from_torrent(&state.db_pool, job.job_id, &info).await?;
        job = load_debrid_job(&state.db_pool, job.job_id)
            .await?
            .ok_or_else(|| anyhow!("Real-Debrid job disappeared during refresh"))?;
        if can_select_all_files(info.status.as_deref()) {
            client.select_files(&remote_torrent_id, "all").await?;
            mark_debrid_job_status(&state.db_pool, job.job_id, "rd_downloading", None).await?;
            return Ok(());
        }
        if !matches!(info.status.as_deref(), Some("downloaded")) {
            return Ok(());
        }
        if job.links.is_empty() && !info.links.is_empty() {
            update_debrid_job_links(&state.db_pool, job.job_id, &info.links).await?;
            job.links = info.links;
        }
    }
    if job.links.is_empty() {
        return Ok(());
    }

    materialize_debrid_links(state, &client, paths, &job).await
}

async fn materialize_debrid_links(
    state: &AppState,
    client: &RealDebridClient,
    paths: &RuntimePaths,
    job: &DebridDownloadJob,
) -> Result<()> {
    mark_debrid_job_status(&state.db_pool, job.job_id, "materializing", None).await?;
    let target_dir = Path::new(&paths.downloads_root).join(
        job.category
            .as_deref()
            .map(safe_path_segment)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "debrid".to_string()),
    );
    tokio::fs::create_dir_all(&target_dir)
        .await
        .with_context(|| format!("creating debrid download dir '{}'", target_dir.display()))?;

    let mut completed_paths = Vec::new();
    for link in &job.links {
        let unrestricted = client.unrestrict_link(link).await?;
        let Some(download_url) = unrestricted.download.as_deref() else {
            bail!("Real-Debrid did not return a downloadable link");
        };
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
            unrestricted.filesize,
        )
        .await?;
        completed_paths.push(target_path);
    }
    let local_path = completed_paths
        .first()
        .map(|path| path.to_string_lossy().to_string());
    mark_debrid_job_completed(&state.db_pool, job.job_id, local_path.as_deref()).await?;
    Ok(())
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
    let client = RealDebridClient::new(token)?;
    let jobs = list_refreshable_debrid_jobs(&state.db_pool, provider_id).await?;
    for job in jobs {
        if let Some(remote_torrent_id) = job.remote_torrent_id.as_deref() {
            match client.torrent_info(remote_torrent_id).await {
                Ok(info) => {
                    update_debrid_job_from_torrent(&state.db_pool, job.job_id, &info).await?;
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
    sqlx::query::<sqlx::Any>(
        "INSERT INTO debrid_download_jobs (
            job_id, provider_id, instance_id, owner_id, source, source_kind, category,
            display_name, remote_torrent_id, remote_download_id, status, local_path,
            links_json, progress, downloaded_bytes, total_bytes, download_rate_bps, last_error
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
    .execute(pool)
    .await?;
    Ok(())
}

async fn list_debrid_jobs_for_provider(
    pool: &sqlx::AnyPool,
    provider_id: Uuid,
) -> Result<Vec<DebridDownloadJob>> {
    let rows = sqlx::query(
        "SELECT job_id, provider_id, instance_id, owner_id, source, source_kind,
            COALESCE(CAST(category AS TEXT), '') as category,
            COALESCE(CAST(display_name AS TEXT), '') as display_name,
            COALESCE(CAST(remote_torrent_id AS TEXT), '') as remote_torrent_id,
            COALESCE(CAST(remote_download_id AS TEXT), '') as remote_download_id,
            status, COALESCE(CAST(local_path AS TEXT), '') as local_path, links_json, progress,
            downloaded_bytes, total_bytes, download_rate_bps, COALESCE(CAST(last_error AS TEXT), '') as last_error
         FROM debrid_download_jobs
         WHERE provider_id = ?
         ORDER BY updated_at DESC
         LIMIT 100",
    )
    .bind(provider_id.to_string())
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(|row| map_debrid_job(&row)).collect()
}

async fn list_active_debrid_jobs(
    pool: &sqlx::AnyPool,
    limit: i64,
) -> Result<Vec<DebridDownloadJob>> {
    let rows = sqlx::query(
        "SELECT job_id, provider_id, instance_id, owner_id, source, source_kind,
            COALESCE(CAST(category AS TEXT), '') as category,
            COALESCE(CAST(display_name AS TEXT), '') as display_name,
            COALESCE(CAST(remote_torrent_id AS TEXT), '') as remote_torrent_id,
            COALESCE(CAST(remote_download_id AS TEXT), '') as remote_download_id,
            status, COALESCE(CAST(local_path AS TEXT), '') as local_path, links_json, progress,
            downloaded_bytes, total_bytes, download_rate_bps, COALESCE(CAST(last_error AS TEXT), '') as last_error
         FROM debrid_download_jobs
         WHERE status NOT IN ('completed', 'failed', 'cancelled', 'paused')
         ORDER BY updated_at ASC
         LIMIT ?",
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(|row| map_debrid_job(&row)).collect()
}

async fn list_refreshable_debrid_jobs(
    pool: &sqlx::AnyPool,
    provider_id: Uuid,
) -> Result<Vec<DebridDownloadJob>> {
    let rows = sqlx::query(
        "SELECT job_id, provider_id, instance_id, owner_id, source, source_kind,
            COALESCE(CAST(category AS TEXT), '') as category,
            COALESCE(CAST(display_name AS TEXT), '') as display_name,
            COALESCE(CAST(remote_torrent_id AS TEXT), '') as remote_torrent_id,
            COALESCE(CAST(remote_download_id AS TEXT), '') as remote_download_id,
            status, COALESCE(CAST(local_path AS TEXT), '') as local_path, links_json, progress,
            downloaded_bytes, total_bytes, download_rate_bps, COALESCE(CAST(last_error AS TEXT), '') as last_error
         FROM debrid_download_jobs
         WHERE provider_id = ?
           AND remote_torrent_id IS NOT NULL
           AND status NOT IN ('completed', 'failed', 'cancelled')
         ORDER BY updated_at DESC
         LIMIT 50",
    )
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
    let row = sqlx::query(
        "SELECT job_id, provider_id, instance_id, owner_id, source, source_kind,
            COALESCE(CAST(category AS TEXT), '') as category,
            COALESCE(CAST(display_name AS TEXT), '') as display_name,
            COALESCE(CAST(remote_torrent_id AS TEXT), '') as remote_torrent_id,
            COALESCE(CAST(remote_download_id AS TEXT), '') as remote_download_id,
            status, COALESCE(CAST(local_path AS TEXT), '') as local_path, links_json, progress,
            downloaded_bytes, total_bytes, download_rate_bps, COALESCE(CAST(last_error AS TEXT), '') as last_error
         FROM debrid_download_jobs
         WHERE provider_id = ? AND (job_id = ? OR remote_torrent_id = ? OR remote_download_id = ?)
         LIMIT 1",
    )
    .bind(provider_id.to_string())
    .bind(download_id)
    .bind(download_id)
    .bind(download_id)
    .fetch_optional(pool)
    .await?;
    row.map(|row| map_debrid_job(&row)).transpose()
}

async fn load_debrid_job(pool: &sqlx::AnyPool, job_id: Uuid) -> Result<Option<DebridDownloadJob>> {
    let row = sqlx::query(
        "SELECT job_id, provider_id, instance_id, owner_id, source, source_kind,
            COALESCE(CAST(category AS TEXT), '') as category,
            COALESCE(CAST(display_name AS TEXT), '') as display_name,
            COALESCE(CAST(remote_torrent_id AS TEXT), '') as remote_torrent_id,
            COALESCE(CAST(remote_download_id AS TEXT), '') as remote_download_id,
            status, COALESCE(CAST(local_path AS TEXT), '') as local_path, links_json, progress,
            downloaded_bytes, total_bytes, download_rate_bps, COALESCE(CAST(last_error AS TEXT), '') as last_error
         FROM debrid_download_jobs
         WHERE job_id = ?
         LIMIT 1",
    )
    .bind(job_id.to_string())
    .fetch_optional(pool)
    .await?;
    row.map(|row| map_debrid_job(&row)).transpose()
}

async fn update_debrid_job_from_torrent(
    pool: &sqlx::AnyPool,
    job_id: Uuid,
    torrent: &RealDebridTorrent,
) -> Result<()> {
    let status = real_debrid_status_to_job_status(torrent.status.as_deref());
    let total = torrent.bytes.or(torrent.original_bytes);
    let downloaded = progress_downloaded_bytes(torrent.progress, total);
    let links_json = serde_json::to_string(&torrent.links)?;
    sqlx::query::<sqlx::Any>(
        "UPDATE debrid_download_jobs
         SET status = ?, display_name = COALESCE(display_name, ?), links_json = CASE WHEN ? != '[]' THEN ? ELSE links_json END,
             progress = ?, downloaded_bytes = ?, total_bytes = ?, download_rate_bps = ?, updated_at = CURRENT_TIMESTAMP
         WHERE job_id = ?",
    )
    .bind(status)
    .bind(torrent.filename.as_deref())
    .bind(&links_json)
    .bind(&links_json)
    .bind(torrent.progress.map(|value| (value / 100.0).clamp(0.0, 1.0)))
    .bind(downloaded.and_then(u64_to_i64))
    .bind(total.and_then(u64_to_i64))
    .bind(torrent.speed.and_then(u64_to_i64))
    .bind(job_id.to_string())
    .execute(pool)
    .await?;
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
         SET status = ?, last_error = ?, updated_at = CURRENT_TIMESTAMP
         WHERE job_id = ?",
    )
    .bind(status)
    .bind(error)
    .bind(job_id.to_string())
    .execute(pool)
    .await?;
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
        status: row.try_get("status")?,
        local_path: empty_string_to_none(row.try_get::<String, _>("local_path")?),
        links: serde_json::from_str(&links_raw).unwrap_or_default(),
        progress: row.try_get::<Option<f64>, _>("progress")?,
        downloaded_bytes: row
            .try_get::<Option<i64>, _>("downloaded_bytes")?
            .and_then(i64_to_u64),
        total_bytes: row
            .try_get::<Option<i64>, _>("total_bytes")?
            .and_then(i64_to_u64),
        download_rate_bps: row
            .try_get::<Option<i64>, _>("download_rate_bps")?
            .and_then(i64_to_u64),
        last_error: empty_string_to_none(row.try_get::<String, _>("last_error")?),
    })
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
                    "logical_id": DEBRID_DEFAULT_LOGICAL_ID
                }
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

fn real_debrid_status_to_job_status(status: Option<&str>) -> String {
    match status
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "downloaded" => "rd_downloaded",
        "downloading" | "compressing" | "uploading" => "rd_downloading",
        "waiting_files_selection" => "waiting_files_selection",
        "queued" | "magnet_conversion" => "submitted",
        "magnet_error" | "error" | "virus" | "dead" => "failed",
        _ => "submitted",
    }
    .to_string()
}

fn can_select_all_files(status: Option<&str>) -> bool {
    matches!(
        status
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "waiting_files_selection"
    )
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

fn u64_to_i64(value: u64) -> Option<i64> {
    i64::try_from(value).ok()
}

fn i64_to_u64(value: i64) -> Option<u64> {
    u64::try_from(value).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn sanitizes_download_paths() {
        assert_eq!(safe_path_segment("TV Shows/../x"), "TV-Shows-..-x");
        assert_eq!(safe_file_name("../Movie: 2024.mkv"), "_Movie_ 2024.mkv");
    }
}
