use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use reqwest::header::{ACCEPT, COOKIE, HOST, HeaderMap, HeaderValue, SET_COOKIE, USER_AGENT};
use reqwest::{Client, Method, StatusCode, Url};
use serde::Deserialize;
use serde_json::Value;
use tokio::fs;
use tokio::process::Command;
use tokio::time::sleep;
use uuid::Uuid;

use crate::drivers::patches::{DownloadCategorySpec, DownloaderTorrentPatch};
use crate::drivers::{
    ActivitySnapshot, ApplyResult, CapabilityDriver, DriverCtx, DriverPatch, StateSnapshot,
};

#[derive(Debug, Default)]
pub struct DownloaderTorrentDriver;

impl DownloaderTorrentDriver {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl CapabilityDriver for DownloaderTorrentDriver {
    fn capability(&self) -> &'static str {
        "downloader.torrent"
    }

    async fn read_state(&self, ctx: DriverCtx) -> Result<StateSnapshot> {
        let implementation = ctx
            .implementation
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("provider implementation is required"))?;
        if implementation != "qbittorrent" {
            bail!(
                "downloader.torrent implementation '{}' is not supported",
                implementation
            );
        }

        let config = QbittorrentDriverConfig::from_ctx(&ctx)?;
        let endpoint_url = ctx.endpoint.canonical_url()?;
        let transport_url = ctx.canonical_url()?;
        let transport_override = if transport_url != endpoint_url {
            Some(transport_url)
        } else {
            None
        };
        let client = QbittorrentClient::from_config(
            config,
            endpoint_url,
            transport_override,
            ctx.instance_id,
        )
        .await?;
        let transfer_info = client.transfer_info().await?;
        let torrents = client.torrents_info().await?;
        let activity = summarize_qbittorrent_activity(&transfer_info, &torrents);
        let summary = summarize_qbittorrent_state(&activity);

        Ok(StateSnapshot {
            summary,
            activity: Some(activity),
        })
    }

    async fn apply_patch(&self, ctx: DriverCtx, patch: DriverPatch) -> Result<ApplyResult> {
        let patch = match patch {
            DriverPatch::DownloaderTorrent(patch) => patch,
            _ => bail!("downloader.torrent patch mismatch"),
        };
        let implementation = ctx
            .implementation
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("provider implementation is required"))?;
        if implementation != "qbittorrent" {
            bail!(
                "downloader.torrent implementation '{}' is not supported",
                implementation
            );
        }

        patch.validate()?;

        let config = QbittorrentDriverConfig::from_ctx(&ctx)?;
        let endpoint_url = ctx.endpoint.canonical_url()?;
        let transport_url = ctx.canonical_url()?;
        let transport_override = if transport_url != endpoint_url {
            Some(transport_url)
        } else {
            None
        };
        let client = match QbittorrentClient::from_config(
            config,
            endpoint_url,
            transport_override,
            ctx.instance_id,
        )
        .await
        {
            Ok(client) => client,
            Err(err) => return Err(err),
        };

        let patch_result = match patch {
            DownloaderTorrentPatch::SetCategories { categories } => {
                client.upsert_categories(&categories).await
            }
            DownloaderTorrentPatch::SetPreferences {
                default_save_path,
                incomplete_path,
                use_incomplete,
                max_connections,
                max_connections_per_torrent,
                max_upload_slots,
                max_upload_slots_per_torrent,
                disk_cache_mb,
                disk_cache_ttl_seconds,
                queueing_enabled,
                max_active_downloads,
                max_active_torrents,
                max_active_uploads,
                random_port,
                listen_port,
                upnp,
                preallocate_all,
            } => {
                client
                    .set_preferences(
                        default_save_path,
                        incomplete_path,
                        use_incomplete,
                        max_connections,
                        max_connections_per_torrent,
                        max_upload_slots,
                        max_upload_slots_per_torrent,
                        disk_cache_mb,
                        disk_cache_ttl_seconds,
                        queueing_enabled,
                        max_active_downloads,
                        max_active_torrents,
                        max_active_uploads,
                        random_port,
                        listen_port,
                        upnp,
                        preallocate_all,
                    )
                    .await
            }
        };
        if let Err(err) = patch_result {
            return Err(err);
        }

        Ok(ApplyResult::applied())
    }
}

#[derive(Debug, Deserialize)]
struct QbittorrentTransferInfo {
    #[serde(default)]
    connection_status: Option<String>,
    #[serde(default)]
    dl_info_speed: Option<u64>,
    #[serde(default)]
    up_info_speed: Option<u64>,
    #[serde(default)]
    dl_info_data: Option<u64>,
    #[serde(default)]
    up_info_data: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct QbittorrentTorrentInfo {
    #[serde(default)]
    state: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct QbittorrentDriverConfig {
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    password: Option<String>,
    #[serde(default)]
    base_url: Option<String>,
    #[serde(default)]
    api_version: Option<String>,
}

impl QbittorrentDriverConfig {
    fn from_ctx(ctx: &DriverCtx) -> Result<Self> {
        let config = if let Some(raw) = ctx.instance_config.as_ref() {
            serde_json::from_value(raw.clone()).context("parsing qBittorrent driver config")?
        } else {
            QbittorrentDriverConfig::default()
        };
        let username = config
            .username
            .clone()
            .or_else(|| ctx.secret("qbittorrent_username").map(str::to_string))
            .or_else(|| ctx.secret("username").map(str::to_string))
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| anyhow::anyhow!("qbittorrent username is required"))?;
        let password = config
            .password
            .clone()
            .or_else(|| ctx.secret("qbittorrent_password").map(str::to_string))
            .or_else(|| ctx.secret("password").map(str::to_string))
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| anyhow::anyhow!("qbittorrent password is required"))?;
        Ok(QbittorrentDriverConfig {
            username: Some(username),
            password: Some(password),
            base_url: config.base_url,
            api_version: config.api_version,
        })
    }
}

struct QbittorrentClient {
    client: Client,
    endpoint_root: Url,
    transport_root: Url,
    api_base: Url,
    cookie: String,
    host_header: Option<String>,
}

const QBITTORRENT_BOOTSTRAP_USER: &str = "admin";
const QBITTORRENT_BOOTSTRAP_PASS: &str = "adminadmin";
const QBITTORRENT_AUTOGEN_PREFIX: &str = "elixir_";

pub(crate) async fn bootstrap_qbittorrent_session_cookie(
    endpoint_url: &str,
    transport_url: Option<&str>,
    instance_id: Uuid,
    username: &str,
    password: &str,
) -> Result<String> {
    let client = QbittorrentClient::from_config(
        QbittorrentDriverConfig {
            username: Some(username.to_string()),
            password: Some(password.to_string()),
            base_url: None,
            api_version: Some("v2".to_string()),
        },
        endpoint_url.to_string(),
        transport_url.map(str::to_string),
        instance_id,
    )
    .await?;
    Ok(client.cookie)
}

impl QbittorrentClient {
    async fn from_config(
        config: QbittorrentDriverConfig,
        endpoint_url: String,
        transport_url: Option<String>,
        instance_id: Uuid,
    ) -> Result<Self> {
        let username = config
            .username
            .ok_or_else(|| anyhow::anyhow!("qbittorrent username is required"))?;
        let password = config
            .password
            .ok_or_else(|| anyhow::anyhow!("qbittorrent password is required"))?;
        let endpoint_root = resolve_base_url(config.base_url.as_deref(), &endpoint_url)?;
        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static("Elixir/1.0"));
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
        let builder = Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .default_headers(headers);
        let transport_root = transport_root(&endpoint_root, transport_url.as_deref())?;
        let host_header = host_header_for_transport(&endpoint_root, &transport_root);

        let client = builder
            .build()
            .context("building qbittorrent http client")?;
        let mut client = Self {
            client,
            endpoint_root,
            transport_root,
            api_base: Url::parse("http://127.0.0.1/").expect("fallback url"),
            cookie: String::new(),
            host_header,
        };

        if let Some(version) = config.api_version.as_deref() {
            client.set_api_version(version)?;
        } else {
            client.set_api_version("v2")?;
        }
        client
            .login_with_bootstrap(instance_id, &username, &password)
            .await?;
        Ok(client)
    }

    fn set_api_version(&mut self, version: &str) -> Result<()> {
        let version = normalize_version(version)?;
        self.api_base = build_api_base(&self.transport_root, version)?;
        Ok(())
    }

    async fn login_with_bootstrap(
        &mut self,
        instance_id: Uuid,
        username: &str,
        password: &str,
    ) -> Result<()> {
        self.refresh_transport_port(instance_id).await?;
        if self.try_login(username, password).await? {
            return Ok(());
        }
        if !username.starts_with(QBITTORRENT_AUTOGEN_PREFIX) {
            bail!("qbittorrent auth rejected for configured credentials");
        }
        reset_qbittorrent_webui_auth(instance_id).await?;

        // qBittorrent needs a short warm-up after restart before auth/login is ready.
        let mut bootstrapped = false;
        let mut tried_default = false;
        let mut last_temp_attempt: Option<String> = None;
        for _ in 0..20 {
            self.refresh_transport_port(instance_id).await?;

            if let Some(temp_password) = lookup_qbittorrent_temporary_password(instance_id).await? {
                if last_temp_attempt.as_deref() != Some(temp_password.as_str()) {
                    last_temp_attempt = Some(temp_password.clone());
                    if self
                        .try_login(QBITTORRENT_BOOTSTRAP_USER, &temp_password)
                        .await?
                    {
                        bootstrapped = true;
                        break;
                    }
                }
            }

            if !tried_default {
                tried_default = true;
                if self
                    .try_login(QBITTORRENT_BOOTSTRAP_USER, QBITTORRENT_BOOTSTRAP_PASS)
                    .await?
                {
                    bootstrapped = true;
                    break;
                }
            }

            sleep(Duration::from_millis(500)).await;
        }
        if !bootstrapped {
            bail!("qbittorrent auth rejected for configured credentials and bootstrap credentials");
        }
        self.set_webui_credentials(username, password).await?;
        if self.try_login(username, password).await? {
            return Ok(());
        }
        bail!("qbittorrent auth rejected after bootstrap reset");
    }

    async fn refresh_transport_port(&mut self, instance_id: Uuid) -> Result<()> {
        if self.host_header.is_none() {
            return Ok(());
        }
        let container_port = self.endpoint_root.port_or_known_default().unwrap_or(80);
        let Some(container_name) = find_container_name_for_instance(instance_id).await? else {
            return Ok(());
        };
        let Some(host_port) = lookup_container_host_port(&container_name, container_port).await?
        else {
            return Ok(());
        };
        let current_port = self
            .transport_root
            .port_or_known_default()
            .unwrap_or(container_port);
        if current_port == host_port {
            return Ok(());
        }
        self.transport_root
            .set_port(Some(host_port))
            .map_err(|_| anyhow::anyhow!("invalid qbittorrent transport host port"))?;
        self.set_api_version("v2")?;
        Ok(())
    }

    async fn try_login(&mut self, username: &str, password: &str) -> Result<bool> {
        let url = self
            .api_base
            .join("auth/login")
            .context("building qbittorrent login url")?;
        let resp = match self
            .client
            .post(url)
            .with_optional_host_header(self.host_header.as_deref())?
            .form(&[("username", username), ("password", password)])
            .send()
            .await
        {
            Ok(resp) => resp,
            // During bootstrap/recovery qBittorrent can briefly refuse/close connections.
            Err(_) => return Ok(false),
        };
        let status = resp.status();
        let cookie_header = resp
            .headers()
            .get(SET_COOKIE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        let body = resp
            .text()
            .await
            .context("reading qbittorrent auth response")?;
        if !status.is_success() {
            return Ok(false);
        }
        if body.trim() != "Ok." {
            return Ok(false);
        }
        let cookie =
            cookie_header.ok_or_else(|| anyhow::anyhow!("qbittorrent auth cookie missing"))?;
        self.cookie = cookie;
        Ok(true)
    }

    fn api_url(&self, path: &str) -> Result<Url> {
        self.api_base
            .join(path)
            .context("building qbittorrent api url")
    }

    fn authed_request(&self, method: Method, path: &str) -> Result<reqwest::RequestBuilder> {
        let url = self.api_url(path)?;
        Ok(self
            .client
            .request(method, url)
            .with_optional_host_header(self.host_header.as_deref())?
            .header(COOKIE, self.cookie.clone()))
    }

    async fn request_json_value(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
    ) -> Result<Value> {
        let mut request = self.authed_request(method.clone(), path)?;
        if let Some(body) = body {
            request = request.json(body);
        }
        let resp = request
            .send()
            .await
            .with_context(|| format!("{} {path}", method.as_str()))?;
        let status = resp.status();
        let bytes = resp
            .bytes()
            .await
            .with_context(|| format!("reading {} {path} response", method.as_str()))?;
        if !status.is_success() {
            let detail = describe_error_body(&bytes);
            if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
                bail!("qbittorrent auth rejected ({status}): {detail}");
            }
            bail!(
                "qbittorrent {} {path} failed ({status}): {detail}",
                method.as_str()
            );
        }
        if bytes.is_empty() {
            bail!(
                "qbittorrent {} {path} returned empty response",
                method.as_str()
            );
        }
        serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing {} {path} response", method.as_str()))
    }

    async fn request_form(&self, path: &str, fields: &HashMap<String, String>) -> Result<()> {
        let request = self.authed_request(Method::POST, path)?;
        let resp = request
            .form(fields)
            .send()
            .await
            .with_context(|| format!("POST {path}"))?;
        let status = resp.status();
        let body = resp
            .bytes()
            .await
            .with_context(|| format!("reading POST {path} response"))?;
        if !status.is_success() {
            let detail = describe_error_body(&body);
            if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
                bail!("qbittorrent auth rejected ({status}): {detail}");
            }
            bail!("qbittorrent POST {path} failed ({status}): {detail}");
        }
        Ok(())
    }

    async fn get_categories(&self) -> Result<Value> {
        self.request_json_value(Method::GET, "torrents/categories", None)
            .await
    }

    async fn transfer_info(&self) -> Result<QbittorrentTransferInfo> {
        let value = self
            .request_json_value(Method::GET, "transfer/info", None)
            .await?;
        serde_json::from_value(value).context("parsing qbittorrent transfer info")
    }

    async fn torrents_info(&self) -> Result<Vec<QbittorrentTorrentInfo>> {
        let value = self
            .request_json_value(Method::GET, "torrents/info", None)
            .await?;
        serde_json::from_value(value).context("parsing qbittorrent torrents info")
    }

    async fn upsert_categories(&self, categories: &[DownloadCategorySpec]) -> Result<()> {
        if categories.is_empty() {
            return Ok(());
        }
        let existing = self.get_categories().await?;
        let existing_map = existing.as_object().cloned().unwrap_or_default();

        for category in categories {
            let entry = existing_map.get(&category.name);
            if let Some(entry) = entry {
                if let Some(save_path) = category.save_path.as_ref() {
                    let existing_path = entry
                        .get("savePath")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    if existing_path == save_path.as_str() {
                        continue;
                    }
                    self.edit_category(&category.name, Some(save_path)).await?;
                }
                continue;
            }
            self.create_category(&category.name, category.save_path.as_deref())
                .await?;
        }
        Ok(())
    }

    async fn create_category(&self, name: &str, save_path: Option<&str>) -> Result<()> {
        let mut fields = HashMap::new();
        fields.insert("category".to_string(), name.to_string());
        if let Some(save_path) = save_path {
            fields.insert("savePath".to_string(), save_path.to_string());
        }
        self.request_form("torrents/createCategory", &fields).await
    }

    async fn edit_category(&self, name: &str, save_path: Option<&str>) -> Result<()> {
        let mut fields = HashMap::new();
        fields.insert("category".to_string(), name.to_string());
        if let Some(save_path) = save_path {
            fields.insert("savePath".to_string(), save_path.to_string());
        }
        self.request_form("torrents/editCategory", &fields).await
    }

    async fn set_preferences(
        &self,
        default_save_path: Option<String>,
        incomplete_path: Option<String>,
        use_incomplete: Option<bool>,
        max_connections: Option<u64>,
        max_connections_per_torrent: Option<u64>,
        max_upload_slots: Option<u64>,
        max_upload_slots_per_torrent: Option<u64>,
        disk_cache_mb: Option<u64>,
        disk_cache_ttl_seconds: Option<u64>,
        queueing_enabled: Option<bool>,
        max_active_downloads: Option<u64>,
        max_active_torrents: Option<u64>,
        max_active_uploads: Option<u64>,
        random_port: Option<bool>,
        listen_port: Option<u16>,
        upnp: Option<bool>,
        preallocate_all: Option<bool>,
    ) -> Result<()> {
        let mut prefs = serde_json::Map::new();
        if let Some(path) = default_save_path {
            prefs.insert("save_path".to_string(), Value::String(path));
        }
        if let Some(path) = incomplete_path {
            prefs.insert("temp_path".to_string(), Value::String(path));
        }
        if let Some(flag) = use_incomplete {
            prefs.insert("temp_path_enabled".to_string(), Value::Bool(flag));
        }
        insert_u64_pref(&mut prefs, "max_connec", max_connections);
        insert_u64_pref(
            &mut prefs,
            "max_connec_per_torrent",
            max_connections_per_torrent,
        );
        insert_u64_pref(&mut prefs, "max_uploads", max_upload_slots);
        insert_u64_pref(
            &mut prefs,
            "max_uploads_per_torrent",
            max_upload_slots_per_torrent,
        );
        insert_u64_pref(&mut prefs, "disk_cache", disk_cache_mb);
        insert_u64_pref(&mut prefs, "disk_cache_ttl", disk_cache_ttl_seconds);
        insert_bool_pref(&mut prefs, "queueing_enabled", queueing_enabled);
        insert_u64_pref(&mut prefs, "max_active_downloads", max_active_downloads);
        insert_u64_pref(&mut prefs, "max_active_torrents", max_active_torrents);
        insert_u64_pref(&mut prefs, "max_active_uploads", max_active_uploads);
        insert_bool_pref(&mut prefs, "random_port", random_port);
        if let Some(port) = listen_port {
            prefs.insert(
                "listen_port".to_string(),
                Value::Number(serde_json::Number::from(port)),
            );
        }
        insert_bool_pref(&mut prefs, "upnp", upnp);
        insert_bool_pref(&mut prefs, "preallocate_all", preallocate_all);
        if prefs.is_empty() {
            return Ok(());
        }
        let payload = Value::Object(prefs);
        let mut fields = HashMap::new();
        fields.insert("json".to_string(), payload.to_string());
        self.request_form("app/setPreferences", &fields).await
    }

    async fn set_webui_credentials(&self, username: &str, password: &str) -> Result<()> {
        let mut prefs = serde_json::Map::new();
        prefs.insert(
            "web_ui_username".to_string(),
            Value::String(username.to_string()),
        );
        prefs.insert(
            "web_ui_password".to_string(),
            Value::String(password.to_string()),
        );
        let payload = Value::Object(prefs);
        let mut fields = HashMap::new();
        fields.insert("json".to_string(), payload.to_string());
        self.request_form("app/setPreferences", &fields).await
    }
}

fn insert_u64_pref(prefs: &mut serde_json::Map<String, Value>, key: &str, value: Option<u64>) {
    if let Some(value) = value {
        prefs.insert(
            key.to_string(),
            Value::Number(serde_json::Number::from(value)),
        );
    }
}

fn insert_bool_pref(prefs: &mut serde_json::Map<String, Value>, key: &str, value: Option<bool>) {
    if let Some(value) = value {
        prefs.insert(key.to_string(), Value::Bool(value));
    }
}

trait HostHeaderExt {
    fn with_optional_host_header(self, host_header: Option<&str>) -> Result<Self>
    where
        Self: Sized;
}

impl HostHeaderExt for reqwest::RequestBuilder {
    fn with_optional_host_header(self, host_header: Option<&str>) -> Result<Self> {
        if let Some(value) = host_header {
            let header = HeaderValue::from_str(value)
                .with_context(|| format!("invalid Host header value '{value}'"))?;
            Ok(self.header(HOST, header))
        } else {
            Ok(self)
        }
    }
}

async fn lookup_qbittorrent_temporary_password(instance_id: Uuid) -> Result<Option<String>> {
    let Some(container_name) = find_container_name_for_instance(instance_id).await? else {
        return Ok(None);
    };

    let logs_output = run_docker_command(&["logs", "--tail", "500", &container_name]).await?;
    if let Some(password) = extract_qbittorrent_temporary_password(&logs_output.combined()) {
        return Ok(Some(password));
    }
    lookup_qbittorrent_temporary_password_from_mount(&container_name).await
}

#[derive(Debug)]
struct DockerCommandOutput {
    stdout: String,
    stderr: String,
}

impl DockerCommandOutput {
    fn combined(&self) -> String {
        if self.stderr.trim().is_empty() {
            return self.stdout.clone();
        }
        if self.stdout.trim().is_empty() {
            return self.stderr.clone();
        }
        format!("{}\n{}", self.stdout, self.stderr)
    }
}

async fn run_docker_command(args: &[&str]) -> Result<DockerCommandOutput> {
    let output = Command::new("docker")
        .args(args)
        .output()
        .await
        .with_context(|| format!("running docker {}", args.join(" ")))?;
    if !output.status.success() {
        bail!(
            "docker {} failed with status {:?}",
            args.join(" "),
            output.status.code()
        );
    }
    Ok(DockerCommandOutput {
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
    })
}

fn extract_qbittorrent_temporary_password(logs: &str) -> Option<String> {
    let marker = "temporary password is provided for this session:";
    for line in logs.lines().rev() {
        let normalized = line.to_ascii_lowercase();
        let Some(index) = normalized.find(marker) else {
            continue;
        };
        let value = line[index + marker.len()..].trim();
        if !value.is_empty() {
            return Some(value.to_string());
        }
    }
    None
}

async fn reset_qbittorrent_webui_auth(instance_id: Uuid) -> Result<()> {
    let container_name = find_container_name_for_instance(instance_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("qbittorrent container not found for instance"))?;

    // Stop first so qBittorrent does not overwrite config on shutdown after we patch it.
    let _ = run_docker_command(&["stop", &container_name]).await;

    let config_root = match find_container_config_root(&container_name).await? {
        Some(path) => path,
        None => {
            let _ = run_docker_command(&["start", &container_name]).await;
            bail!("qbittorrent /config mount not found for recovery");
        }
    };
    let config_path = config_root.join("qBittorrent").join("qBittorrent.conf");
    let content = match fs::read_to_string(&config_path).await {
        Ok(content) => content,
        Err(err) => {
            let _ = run_docker_command(&["start", &container_name]).await;
            return Err(err).with_context(|| format!("reading {}", config_path.display()));
        }
    };
    let (rewritten, changed) = strip_qbittorrent_webui_auth_fields(&content);
    if changed {
        if let Err(err) = fs::write(&config_path, rewritten).await {
            let _ = run_docker_command(&["start", &container_name]).await;
            return Err(err).with_context(|| format!("writing {}", config_path.display()));
        }
    }

    run_docker_command(&["start", &container_name]).await?;
    Ok(())
}

async fn lookup_qbittorrent_temporary_password_from_mount(
    container_name: &str,
) -> Result<Option<String>> {
    let Some(config_root) = find_container_config_root(container_name).await? else {
        return Ok(None);
    };
    let candidates = [
        config_root
            .join("qBittorrent")
            .join("logs")
            .join("qbittorrent.log"),
        config_root
            .join("qBittorrent")
            .join("logs")
            .join("qBittorrent.log"),
    ];
    for path in candidates {
        let content = match fs::read_to_string(&path).await {
            Ok(content) => content,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err).with_context(|| format!("reading {}", path.display())),
        };
        if let Some(password) = extract_qbittorrent_temporary_password(&content) {
            return Ok(Some(password));
        }
    }
    Ok(None)
}

async fn find_container_config_root(container_name: &str) -> Result<Option<std::path::PathBuf>> {
    let inspect_output = run_docker_command(&[
        "inspect",
        "--format",
        "{{range .Mounts}}{{if eq .Destination \"/config\"}}{{.Source}}{{end}}{{end}}",
        container_name,
    ])
    .await?;
    let config_root = inspect_output.stdout.trim();
    if config_root.is_empty() {
        return Ok(None);
    }
    Ok(Some(std::path::PathBuf::from(config_root)))
}

async fn find_container_name_for_instance(instance_id: Uuid) -> Result<Option<String>> {
    let ps_output = run_docker_command(&[
        "ps",
        "-a",
        "--filter",
        &format!("label=elixir.instance_id={instance_id}"),
        "--format",
        "{{.Names}}",
    ])
    .await?;
    let container = ps_output
        .stdout
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(str::to_string);
    Ok(container)
}

async fn lookup_container_host_port(
    container_name: &str,
    container_port: u16,
) -> Result<Option<u16>> {
    let port_output =
        run_docker_command(&["port", container_name, &format!("{container_port}/tcp")]).await?;
    let value = port_output
        .stdout
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty());
    let Some(value) = value else {
        return Ok(None);
    };
    let host_port = value
        .rsplit(':')
        .next()
        .and_then(|raw| raw.trim().parse::<u16>().ok());
    Ok(host_port)
}

fn strip_qbittorrent_webui_auth_fields(content: &str) -> (String, bool) {
    let mut lines = Vec::new();
    for line in content.lines() {
        let trimmed = line.trim_start();
        let should_remove = trimmed.starts_with("WebUI\\Username=")
            || trimmed.starts_with("WebUI\\Password_PBKDF2=")
            || trimmed.starts_with("WebUI\\Password_ha1=")
            || trimmed.starts_with("WebUI\\Password=")
            || trimmed.starts_with("WebUI\\MaxAuthenticationFailCount=")
            || trimmed.starts_with("WebUI\\BanDuration=");
        if should_remove {
            continue;
        }
        lines.push(line);
    }
    lines.push("WebUI\\MaxAuthenticationFailCount=0");
    lines.push("WebUI\\BanDuration=0");
    let mut rewritten = lines.join("\n");
    if content.ends_with('\n') {
        rewritten.push('\n');
    }
    (rewritten, true)
}

fn normalize_root_url(base_url: &str) -> Result<Url> {
    let mut url = Url::parse(base_url).context("parsing qbittorrent base_url")?;
    let mut path = url.path().trim_end_matches('/').to_string();
    if path.ends_with("/api/v2") {
        path = path.trim_end_matches("/api/v2").to_string();
    }
    if path.is_empty() {
        path = "/".to_string();
    }
    url.set_path(&path);
    Ok(url)
}

fn resolve_base_url(config_base_url: Option<&str>, endpoint_url: &str) -> Result<Url> {
    let endpoint = normalize_root_url(endpoint_url)?;
    if let Some(base_url) = config_base_url {
        let base = normalize_root_url(base_url)?;
        ensure_same_origin(&base, &endpoint)?;
        return Ok(base);
    }
    Ok(endpoint)
}

fn transport_root(endpoint: &Url, transport_override: Option<&str>) -> Result<Url> {
    let Some(transport_override) = transport_override else {
        return Ok(endpoint.clone());
    };
    let transport = normalize_root_url(transport_override)?;
    let mut merged = endpoint.clone();
    merged
        .set_scheme(transport.scheme())
        .map_err(|_| anyhow::anyhow!("invalid transport scheme '{}'", transport.scheme()))?;
    merged
        .set_host(transport.host_str())
        .map_err(|_| anyhow::anyhow!("invalid transport host"))?;
    merged
        .set_port(transport.port())
        .map_err(|_| anyhow::anyhow!("invalid transport port"))?;
    Ok(merged)
}

fn host_header_for_transport(endpoint: &Url, transport: &Url) -> Option<String> {
    let endpoint_host = endpoint.host_str()?;
    let endpoint_port = endpoint.port_or_known_default().unwrap_or(80);
    let transport_host = transport.host_str()?;
    let transport_port = transport.port_or_known_default().unwrap_or(80);
    if endpoint_host.eq_ignore_ascii_case(transport_host) && endpoint_port == transport_port {
        return None;
    }
    Some(format!("{endpoint_host}:{endpoint_port}"))
}

fn ensure_same_origin(candidate: &Url, endpoint: &Url) -> Result<()> {
    let candidate_host = candidate
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("qbittorrent base_url host is missing"))?;
    let endpoint_host = endpoint
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("qbittorrent endpoint host is missing"))?;
    let candidate_port = candidate.port_or_known_default().unwrap_or(80);
    let endpoint_port = endpoint.port_or_known_default().unwrap_or(80);
    if candidate.scheme() != endpoint.scheme()
        || candidate_host != endpoint_host
        || candidate_port != endpoint_port
    {
        bail!("qbittorrent base_url must match provider endpoint scheme/host/port");
    }
    Ok(())
}

fn normalize_version(version: &str) -> Result<&'static str> {
    let trimmed = version.trim().to_ascii_lowercase();
    match trimmed.as_str() {
        "v2" | "2" => Ok("v2"),
        _ => bail!("unsupported qbittorrent api version '{version}'"),
    }
}

fn summarize_qbittorrent_activity(
    transfer: &QbittorrentTransferInfo,
    torrents: &[QbittorrentTorrentInfo],
) -> ActivitySnapshot {
    let mut active_items = 0u64;
    let mut queued_items = 0u64;
    let mut error_items = 0u64;

    for torrent in torrents {
        let state = torrent
            .state
            .as_deref()
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        if state.is_empty() {
            continue;
        }
        if is_qbittorrent_error_state(&state) {
            error_items += 1;
        }
        if state.contains("queued") {
            queued_items += 1;
        }
        if is_qbittorrent_active_state(&state) {
            active_items += 1;
        }
    }

    ActivitySnapshot {
        status: transfer.connection_status.clone(),
        download_rate_bps: transfer.dl_info_speed,
        upload_rate_bps: transfer.up_info_speed,
        active_items: Some(active_items),
        queued_items: Some(queued_items),
        error_items: Some(error_items),
        post_process_items: None,
        downloaded_bytes: transfer.dl_info_data,
        uploaded_bytes: transfer.up_info_data,
    }
}

fn summarize_qbittorrent_state(activity: &ActivitySnapshot) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(status) = activity
        .status
        .as_deref()
        .filter(|status| !status.trim().is_empty())
    {
        parts.push(status.to_string());
    }
    if let Some(active) = activity.active_items.filter(|count| *count > 0) {
        parts.push(format!("{active} active"));
    }
    if let Some(errors) = activity.error_items.filter(|count| *count > 0) {
        parts.push(format!(
            "{errors} issue{}",
            if errors == 1 { "" } else { "s" }
        ));
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" · "))
    }
}

fn is_qbittorrent_active_state(state: &str) -> bool {
    matches!(
        state,
        "uploading"
            | "stalledup"
            | "checkingup"
            | "forcedup"
            | "allocating"
            | "downloading"
            | "metadl"
            | "stalleddl"
            | "forceddl"
            | "checkingdl"
            | "checkingresume"
            | "moving"
    )
}

fn is_qbittorrent_error_state(state: &str) -> bool {
    state == "error" || state == "missingfiles"
}

fn build_api_base(root: &Url, version: &str) -> Result<Url> {
    let version = normalize_version(version)?;
    let root_path = root.path().trim_end_matches('/');
    let api_path = if root_path.is_empty() || root_path == "/" {
        format!("/api/{version}")
    } else {
        format!("{root_path}/api/{version}")
    };
    let mut api_base = root.clone();
    api_base.set_path(&format!("{}/", api_path.trim_end_matches('/')));
    Ok(api_base)
}

fn describe_error_body(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return "empty response".to_string();
    }
    if let Ok(value) = serde_json::from_slice::<Value>(bytes) {
        if let Some(message) = extract_error_message(&value) {
            return message;
        }
        return value.to_string();
    }
    let text = String::from_utf8_lossy(bytes);
    let trimmed = text.trim();
    if trimmed.is_empty() {
        "empty response".to_string()
    } else {
        trimmed.to_string()
    }
}

fn extract_error_message(value: &Value) -> Option<String> {
    let message_keys = ["message", "errorMessage", "error", "detail", "description"];
    for key in message_keys {
        if let Some(message) = value.get(key).and_then(Value::as_str) {
            if !message.trim().is_empty() {
                return Some(message.to_string());
            }
        }
    }
    let list_keys = ["errors", "validationErrors", "validationFailures"];
    for key in list_keys {
        if let Some(entries) = value.get(key) {
            return Some(entries.to_string());
        }
    }
    None
}

fn is_qbittorrent_auth_error(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        let message = cause.to_string().to_ascii_lowercase();
        message.contains("qbittorrent auth rejected")
            || message.contains("auth/login")
            || message.contains("bootstrap credentials")
            || message.contains("after bootstrap reset")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::drivers::CapabilityDriver;
    use axum::{
        Json, Router,
        http::{HeaderValue, header::SET_COOKIE},
        response::{IntoResponse, Response},
        routing::{get, post},
    };
    use std::collections::HashMap;

    #[test]
    fn extracts_latest_temporary_password_from_logs() {
        let logs = r#"
The WebUI administrator password was not set. A temporary password is provided for this session: oldPass123
some other line
The WebUI administrator password was not set. A temporary password is provided for this session: NewPass456
"#;
        let password = extract_qbittorrent_temporary_password(logs);
        assert_eq!(password.as_deref(), Some("NewPass456"));
    }

    #[test]
    fn detects_qbittorrent_auth_errors_from_error_chain() {
        let err = anyhow::anyhow!("qbittorrent auth rejected for configured credentials");
        assert!(is_qbittorrent_auth_error(&err));

        let nested = Err::<(), _>(anyhow::anyhow!("wrapper"))
            .context("auth/login failed")
            .unwrap_err();
        assert!(is_qbittorrent_auth_error(&nested));

        let other = anyhow::anyhow!("network timeout");
        assert!(!is_qbittorrent_auth_error(&other));
    }

    #[test]
    fn strips_webui_auth_fields_from_qbittorrent_conf() {
        let input = "\
[Preferences]
WebUI\\Address=*
WebUI\\Username=elixir_old
WebUI\\Password_PBKDF2=@ByteArray(foo:bar)
WebUI\\ServerDomains=*
";
        let (output, changed) = strip_qbittorrent_webui_auth_fields(input);
        assert!(changed);
        assert!(!output.contains("WebUI\\Username="));
        assert!(!output.contains("WebUI\\Password_PBKDF2="));
        assert!(output.contains("WebUI\\Address=*"));
        assert!(output.contains("WebUI\\ServerDomains=*"));
        assert!(output.contains("WebUI\\MaxAuthenticationFailCount=0"));
        assert!(output.contains("WebUI\\BanDuration=0"));
    }

    #[test]
    fn host_header_is_set_when_transport_differs() -> Result<()> {
        let endpoint = Url::parse("http://svc-elixir-modules-qbittorrent-default:8080/")?;
        let transport = Url::parse("http://127.0.0.1:33042/")?;
        let header = host_header_for_transport(&endpoint, &transport);
        assert_eq!(
            header.as_deref(),
            Some("svc-elixir-modules-qbittorrent-default:8080")
        );
        Ok(())
    }

    #[test]
    fn transport_root_swaps_host_and_port_keeps_path() -> Result<()> {
        let endpoint = Url::parse("http://svc-elixir-modules-qbittorrent-default:8080/base/")?;
        let merged = transport_root(&endpoint, Some("http://127.0.0.1:33042"))?;
        assert_eq!(merged.as_str(), "http://127.0.0.1:33042/base/");
        Ok(())
    }

    #[tokio::test]
    async fn read_state_reports_qbittorrent_live_telemetry() -> Result<()> {
        async fn login() -> Response {
            (
                [(SET_COOKIE, HeaderValue::from_static("SID=test; HttpOnly"))],
                "Ok.",
            )
                .into_response()
        }

        async fn transfer() -> Json<Value> {
            Json(serde_json::json!({
                "connection_status": "connected",
                "dl_info_speed": 5242880u64,
                "up_info_speed": 262144u64,
                "dl_info_data": 1073741824u64,
                "up_info_data": 134217728u64
            }))
        }

        async fn torrents() -> Json<Value> {
            Json(serde_json::json!([
                { "state": "downloading" },
                { "state": "stalledDL" },
                { "state": "queuedDL" },
                { "state": "error" }
            ]))
        }

        let app = Router::new()
            .route("/api/v2/auth/login", post(login))
            .route("/api/v2/transfer/info", get(transfer))
            .route("/api/v2/torrents/info", get(torrents));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("mock qbittorrent server");
        });

        let endpoint = crate::orchestrator::model::ProviderEndpoint::new(
            "http".to_string(),
            "svc-qbittorrent-default".to_string(),
            addr.port(),
            None,
            Some("elixir_net".to_string()),
        )?;
        let ctx = DriverCtx::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "downloader.torrent".to_string(),
            endpoint,
            Some(format!("http://127.0.0.1:{}", addr.port())),
            Some("qbittorrent".to_string()),
            Some(serde_json::json!({
                "username": "admin",
                "password": "adminadmin"
            })),
            HashMap::new(),
        );

        let snapshot = DownloaderTorrentDriver::new().read_state(ctx).await?;
        let activity = snapshot.activity.expect("activity");
        assert_eq!(activity.status.as_deref(), Some("connected"));
        assert_eq!(activity.download_rate_bps, Some(5_242_880));
        assert_eq!(activity.upload_rate_bps, Some(262_144));
        assert_eq!(activity.active_items, Some(2));
        assert_eq!(activity.queued_items, Some(1));
        assert_eq!(activity.error_items, Some(1));
        assert_eq!(activity.downloaded_bytes, Some(1_073_741_824));
        assert_eq!(activity.uploaded_bytes, Some(134_217_728));
        assert_eq!(
            snapshot.summary.as_deref(),
            Some("connected · 2 active · 1 issue")
        );
        Ok(())
    }
}
