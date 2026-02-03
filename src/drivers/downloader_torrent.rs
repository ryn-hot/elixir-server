use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use reqwest::header::{ACCEPT, COOKIE, HeaderMap, HeaderValue, SET_COOKIE, USER_AGENT};
use reqwest::{Client, Method, StatusCode, Url};
use serde::Deserialize;
use serde_json::Value;

use crate::drivers::{ApplyResult, CapabilityDriver, DriverCtx, DriverPatch, StateSnapshot};
use crate::drivers::patches::{DownloadCategorySpec, DownloaderTorrentPatch};

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

    async fn read_state(&self, _ctx: DriverCtx) -> Result<StateSnapshot> {
        Ok(StateSnapshot {
            summary: Some("downloader.torrent driver is not implemented".to_string()),
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
        let client = QbittorrentClient::from_config(config, ctx.canonical_url()?).await?;

        match patch {
            DownloaderTorrentPatch::SetCategories { categories } => {
                client.upsert_categories(&categories).await?;
            }
            DownloaderTorrentPatch::SetPreferences {
                default_save_path,
                incomplete_path,
                use_incomplete,
            } => {
                client
                    .set_preferences(default_save_path, incomplete_path, use_incomplete)
                    .await?;
            }
        }

        Ok(ApplyResult::applied())
    }
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
    root: Url,
    api_base: Url,
    cookie: String,
}

impl QbittorrentClient {
    async fn from_config(
        config: QbittorrentDriverConfig,
        endpoint_url: String,
    ) -> Result<Self> {
        let username = config
            .username
            .ok_or_else(|| anyhow::anyhow!("qbittorrent username is required"))?;
        let password = config
            .password
            .ok_or_else(|| anyhow::anyhow!("qbittorrent password is required"))?;
        let root = resolve_base_url(config.base_url.as_deref(), &endpoint_url)?;
        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static("Elixir/1.0"));
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .default_headers(headers)
            .build()
            .context("building qbittorrent http client")?;
        let mut client = Self {
            client,
            root,
            api_base: Url::parse("http://127.0.0.1/").expect("fallback url"),
            cookie: String::new(),
        };

        if let Some(version) = config.api_version.as_deref() {
            client.set_api_version(version)?;
        } else {
            client.set_api_version("v2")?;
        }
        client.login(&username, &password).await?;
        Ok(client)
    }

    fn set_api_version(&mut self, version: &str) -> Result<()> {
        let version = normalize_version(version)?;
        self.api_base = build_api_base(&self.root, version)?;
        Ok(())
    }

    async fn login(&mut self, username: &str, password: &str) -> Result<()> {
        let url = self
            .api_base
            .join("auth/login")
            .context("building qbittorrent login url")?;
        let resp = self
            .client
            .post(url)
            .form(&[("username", username), ("password", password)])
            .send()
            .await
            .context("qbittorrent auth/login request")?;
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
            bail!(
                "qbittorrent auth failed ({}): {}",
                status,
                body.trim()
            );
        }
        if body.trim() != "Ok." {
            bail!("qbittorrent auth rejected: {}", body.trim());
        }
        let cookie = cookie_header.ok_or_else(|| anyhow::anyhow!("qbittorrent auth cookie missing"))?;
        self.cookie = cookie;
        Ok(())
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
            bail!("qbittorrent {} {path} failed ({status}): {detail}", method.as_str());
        }
        if bytes.is_empty() {
            bail!("qbittorrent {} {path} returned empty response", method.as_str());
        }
        serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing {} {path} response", method.as_str()))
    }

    async fn request_form(
        &self,
        path: &str,
        fields: &HashMap<String, String>,
    ) -> Result<()> {
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
        self.request_form("torrents/createCategory", &fields)
            .await
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
        if prefs.is_empty() {
            return Ok(());
        }
        let payload = Value::Object(prefs);
        let mut fields = HashMap::new();
        fields.insert("json".to_string(), payload.to_string());
        self.request_form("app/setPreferences", &fields).await
    }
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
