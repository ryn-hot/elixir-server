use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use reqwest::{
    Client, Method, StatusCode,
    header::{HeaderMap, HeaderValue, ACCEPT, USER_AGENT},
};
use reqwest::Url;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{Value, json};
use tracing::warn;

use crate::drivers::patches::{DownloaderSpec, MediaManagerMoviesPatch, RootFolderSpec};
use crate::drivers::{ApplyResult, CapabilityDriver, DriverCtx, DriverPatch, StateSnapshot};

pub struct MediaManagerMoviesDriver;

impl MediaManagerMoviesDriver {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl CapabilityDriver for MediaManagerMoviesDriver {
    fn capability(&self) -> &'static str {
        "media.manager.movies"
    }

    async fn read_state(&self, _ctx: DriverCtx) -> Result<StateSnapshot> {
        Ok(StateSnapshot { summary: None })
    }

    async fn apply_patch(&self, ctx: DriverCtx, patch: DriverPatch) -> Result<ApplyResult> {
        let patch = match patch {
            DriverPatch::MediaManagerMovies(patch) => patch,
            other => bail!("media.manager.movies driver received unsupported patch {other:?}"),
        };

        let endpoint_url = ctx.canonical_url()?;
        let config = RadarrDriverConfig::from_ctx(&ctx)?;
        let client = RadarrClient::from_config(config, endpoint_url).await?;

        match patch {
            MediaManagerMoviesPatch::SetDownloaders { downloaders } => {
                client.upsert_downloaders(&downloaders).await?;
            }
            MediaManagerMoviesPatch::SetRootFolders { roots } => {
                client.ensure_root_folders(&roots).await?;
            }
            MediaManagerMoviesPatch::SetTags { tags } => {
                let _ = client.ensure_tags(&tags).await?;
            }
        }

        Ok(ApplyResult::applied())
    }
}

#[derive(Debug, Deserialize, Default)]
struct RadarrDriverConfig {
    #[serde(default)]
    api_key: Option<String>,
    #[serde(default)]
    base_url: Option<String>,
    #[serde(default)]
    api_version: Option<String>,
}

impl RadarrDriverConfig {
    fn from_ctx(ctx: &DriverCtx) -> Result<Self> {
        let config = if let Some(raw) = ctx.instance_config.as_ref() {
            serde_json::from_value(raw.clone()).context("parsing radarr driver config")?
        } else {
            RadarrDriverConfig::default()
        };
        let api_key = config
            .api_key
            .clone()
            .or_else(|| ctx.secret("radarr_api_key").map(str::to_string))
            .or_else(|| ctx.secret("api_key").map(str::to_string))
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| anyhow::anyhow!("radarr api_key is required"))?;
        Ok(RadarrDriverConfig {
            api_key: Some(api_key),
            base_url: config.base_url,
            api_version: config.api_version,
        })
    }
}

struct RadarrClient {
    client: Client,
    root: Url,
    api_base: Url,
    api_version: &'static str,
}

impl RadarrClient {
    async fn from_config(config: RadarrDriverConfig, endpoint_url: String) -> Result<Self> {
        let api_key = config
            .api_key
            .ok_or_else(|| anyhow::anyhow!("radarr api_key is required"))?;
        let root = resolve_base_url(config.base_url.as_deref(), &endpoint_url)?;
        let mut headers = HeaderMap::new();
        headers.insert(
            "X-Api-Key",
            HeaderValue::from_str(&api_key).context("invalid radarr api key header")?,
        );
        headers.insert(USER_AGENT, HeaderValue::from_static("Elixir/1.0"));
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .default_headers(headers)
            .build()
            .context("building radarr http client")?;

        let mut client = Self {
            client,
            root,
            api_base: Url::parse("http://127.0.0.1/").expect("fallback url"),
            api_version: "v3",
        };

        if let Some(version) = config.api_version.as_deref() {
            client.set_api_version(version)?;
        } else {
            client.detect_api_version().await?;
        }
        Ok(client)
    }

    async fn detect_api_version(&mut self) -> Result<()> {
        if self.probe_api("v3").await? {
            self.set_api_version("v3")?;
            return Ok(());
        }
        if self.probe_api("v4").await? {
            self.set_api_version("v4")?;
            return Ok(());
        }
        bail!("radarr api version could not be detected");
    }

    async fn probe_api(&self, version: &str) -> Result<bool> {
        let url = build_api_url(&self.root, version, "system/status")?;
        let resp = self
            .client
            .get(url)
            .send()
            .await
            .context("probing radarr api")?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(false);
        }
        if resp.status() == StatusCode::UNAUTHORIZED {
            bail!("radarr api key is invalid");
        }
        if !resp.status().is_success() {
            bail!("radarr api probe failed with {}", resp.status());
        }
        Ok(true)
    }

    fn set_api_version(&mut self, version: &str) -> Result<()> {
        let version = normalize_version(version)?;
        self.api_base = build_api_base(&self.root, version)?;
        self.api_version = version;
        Ok(())
    }

    fn api_url(&self, path: &str) -> Result<Url> {
        let trimmed = path.trim_start_matches('/');
        self.api_base
            .join(trimmed)
            .context("building radarr api url")
    }

    async fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let value = self
            .request_json_value(Method::GET, path, None)
            .await?;
        serde_json::from_value(value)
            .with_context(|| format!("parsing GET {path} response"))
    }

    async fn post_json(&self, path: &str, body: &Value) -> Result<Value> {
        self.request_json_value(Method::POST, path, Some(body))
            .await
    }

    async fn put_json(&self, path: &str, body: &Value) -> Result<Value> {
        self.request_json_value(Method::PUT, path, Some(body))
            .await
    }

    async fn request_json_value(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
    ) -> Result<Value> {
        let url = self.api_url(path)?;
        let mut request = self.client.request(method.clone(), url);
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
                bail!("radarr api key rejected ({status}): {detail}");
            }
            bail!("radarr {} {path} failed ({status}): {detail}", method.as_str());
        }
        if bytes.is_empty() {
            bail!("radarr {} {path} returned empty response", method.as_str());
        }
        let value: Value =
            serde_json::from_slice(&bytes).context("parsing radarr json response")?;
        Ok(value)
    }

    async fn ensure_root_folders(&self, roots: &[RootFolderSpec]) -> Result<()> {
        if roots.is_empty() {
            return Ok(());
        }
        let existing = self.get_json::<Vec<Value>>("rootfolder").await?;
        for root in roots {
            if find_by_path(&existing, &root.path).is_some() {
                continue;
            }
            let body = json!({ "path": root.path });
            self.post_json("rootfolder", &body).await?;
        }
        Ok(())
    }

    async fn upsert_downloaders(&self, downloaders: &[DownloaderSpec]) -> Result<()> {
        if downloaders.is_empty() {
            return Ok(());
        }
        let schema = self.get_json::<Vec<Value>>("downloadclient/schema").await?;
        let existing = self.get_json::<Vec<Value>>("downloadclient").await?;

        for downloader in downloaders {
            let tags = self.ensure_tags(&downloader.tags).await?;
            let schema_item = find_schema(&schema, &downloader.r#type)?;
            let existing_item = find_by_name(&existing, &downloader.name);
            let mut target = match existing_item.clone() {
                Some(existing) => existing,
                None => schema_item,
            };
            let enabled = downloader.enabled.unwrap_or(true);
            set_enabled(&mut target, enabled)?;
            set_string(&mut target, "name", downloader.name.clone())?;
            set_array_i64(&mut target, "tags", &tags)?;
            ensure_schema_fields(&mut target, &downloader.r#type)?;

            let fields = target
                .get_mut("fields")
                .and_then(Value::as_array_mut)
                .ok_or_else(|| anyhow::anyhow!("download client fields missing"))?;
            apply_downloader_fields(fields, downloader)?;

            if let Some(existing_item) = existing_item {
                if target == existing_item {
                    continue;
                }
            }

            if let Some(id) = target.get("id").and_then(Value::as_i64) {
                let path = format!("downloadclient/{id}");
                self.put_json(&path, &target).await?;
            } else {
                remove_readonly_fields(&mut target);
                self.post_json("downloadclient", &target).await?;
            }
        }
        Ok(())
    }

    async fn ensure_tags(&self, tags: &[String]) -> Result<Vec<i64>> {
        if tags.is_empty() {
            return Ok(Vec::new());
        }
        let existing = self.get_json::<Vec<Value>>("tag").await?;
        let mut by_name: HashMap<String, i64> = HashMap::new();
        for tag in &existing {
            if let (Some(name), Some(id)) = (
                tag.get("label").and_then(Value::as_str),
                tag.get("id").and_then(Value::as_i64),
            ) {
                by_name.insert(normalize_name(name), id);
            }
        }
        let mut tag_ids = Vec::new();
        let mut seen = HashSet::new();
        for tag in tags {
            let normalized = normalize_name(tag);
            if !seen.insert(normalized.clone()) {
                continue;
            }
            if let Some(id) = by_name.get(&normalized) {
                tag_ids.push(*id);
                continue;
            }
            let created = self
                .post_json("tag", &json!({ "label": tag }))
                .await?;
            if let Some(id) = created.get("id").and_then(Value::as_i64) {
                by_name.insert(normalized, id);
                tag_ids.push(id);
            } else {
                bail!("tag creation did not return id");
            }
        }
        Ok(tag_ids)
    }
}

fn normalize_version(value: &str) -> Result<&'static str> {
    let value = value.trim().to_ascii_lowercase();
    match value.as_str() {
        "v3" | "3" => Ok("v3"),
        "v4" | "4" => Ok("v4"),
        _ => bail!("unsupported radarr api version '{}'", value),
    }
}

fn resolve_base_url(config: Option<&str>, endpoint_url: &str) -> Result<Url> {
    let url = config.unwrap_or(endpoint_url);
    Url::parse(url).context("parsing radarr base url")
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

fn build_api_url(root: &Url, version: &str, path: &str) -> Result<Url> {
    let api_base = build_api_base(root, version)?;
    api_base
        .join(path)
        .context("building radarr api url")
}

fn describe_error_body(body: &[u8]) -> String {
    if let Ok(value) = std::str::from_utf8(body) {
        if !value.trim().is_empty() {
            return value.trim().to_string();
        }
    }
    format!("{} bytes", body.len())
}

fn normalize_name(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace(' ', "")
}

fn find_schema(items: &[Value], implementation: &str) -> Result<Value> {
    let needle = normalize_name(implementation);
    let schema = items.iter().find_map(|value| {
        let implementation_value = value
            .get("implementation")
            .and_then(Value::as_str)
            .or_else(|| value.get("implementationName").and_then(Value::as_str))?;
        if normalize_name(implementation_value) == needle {
            Some(value.clone())
        } else {
            None
        }
    });
    schema.ok_or_else(|| anyhow::anyhow!("implementation '{}' not found", implementation))
}

fn find_by_name(items: &[Value], name: &str) -> Option<Value> {
    let needle = normalize_name(name);
    items.iter().find_map(|value| {
        let current = value.get("name").and_then(Value::as_str)?;
        if normalize_name(current) == needle {
            Some(value.clone())
        } else {
            None
        }
    })
}

fn find_by_path(items: &[Value], path: &str) -> Option<Value> {
    let needle = normalize_name(path);
    items.iter().find_map(|value| {
        let current = value.get("path").and_then(Value::as_str)?;
        if normalize_name(current) == needle {
            Some(value.clone())
        } else {
            None
        }
    })
}

fn ensure_schema_fields(target: &mut Value, implementation: &str) -> Result<()> {
    if let Some(map) = target.as_object_mut() {
        if !map.contains_key("implementation") {
            map.insert(
                "implementation".to_string(),
                Value::String(implementation.to_string()),
            );
        }
        if !map.contains_key("configContract") {
            map.insert(
                "configContract".to_string(),
                Value::String(format!("{}Settings", implementation)),
            );
        }
    }
    Ok(())
}

fn set_enabled(target: &mut Value, enabled: bool) -> Result<()> {
    if let Some(obj) = target.as_object_mut() {
        if obj.contains_key("enable") {
            obj.insert("enable".to_string(), Value::Bool(enabled));
            return Ok(());
        }
        obj.insert("enabled".to_string(), Value::Bool(enabled));
        return Ok(());
    }
    bail!("download client payload must be an object");
}

fn set_string(target: &mut Value, field: &str, value: String) -> Result<()> {
    if let Some(obj) = target.as_object_mut() {
        obj.insert(field.to_string(), Value::String(value));
        return Ok(());
    }
    bail!("payload must be an object");
}

fn set_array_i64(target: &mut Value, field: &str, values: &[i64]) -> Result<()> {
    if let Some(obj) = target.as_object_mut() {
        let values = values
            .iter()
            .map(|value| Value::Number((*value).into()))
            .collect();
        obj.insert(field.to_string(), Value::Array(values));
        return Ok(());
    }
    bail!("payload must be an object");
}

fn remove_readonly_fields(target: &mut Value) {
    if let Some(map) = target.as_object_mut() {
        map.remove("id");
        map.remove("warnings");
        map.remove("validationFailures");
    }
}

fn apply_downloader_fields(fields: &mut Vec<Value>, spec: &DownloaderSpec) -> Result<()> {
    apply_url_fields(fields, &spec.url)?;
    if let Some(api_key) = spec.api_key.as_ref() {
        set_field_value_optional(fields, "apiKey", Value::String(api_key.clone()))?;
    }
    if let Some(category) = spec.category.as_ref() {
        if !set_field_value_optional(fields, "category", Value::String(category.clone()))?
            && !set_field_value_optional(fields, "tvCategory", Value::String(category.clone()))?
        {
            warn!("download client category field not found");
        }
    }
    for (key, value) in &spec.settings {
        if !set_field_value_optional(fields, key, value.clone())? {
            warn!("download client field '{}' not found in schema", key);
        }
    }
    Ok(())
}

fn apply_url_fields(fields: &mut Vec<Value>, url: &str) -> Result<()> {
    let parsed = Url::parse(url).context("parsing downloader url")?;
    let url_value = Value::String(url.to_string());
    let has_base = set_field_value_optional(fields, "baseUrl", url_value.clone())?;
    let has_url = set_field_value_optional(fields, "url", url_value.clone())?;
    let host = parsed
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("downloader url host missing"))?;
    let port = parsed.port_or_known_default().unwrap_or(80);

    if !has_base && !has_url {
        set_field_value_optional(fields, "host", Value::String(host.to_string()))?;
        set_field_value_optional(fields, "port", Value::Number(port.into()))?;
        if parsed.scheme() == "https" {
            set_field_value_optional(fields, "useSsl", Value::Bool(true))?;
        }
    }
    Ok(())
}

fn set_field_value_optional(
    fields: &mut [Value],
    name: &str,
    value: Value,
) -> Result<bool> {
    for field in fields.iter_mut() {
        let field_name = field.get("name").and_then(Value::as_str);
        if field_name == Some(name) {
            if let Some(obj) = field.as_object_mut() {
                obj.insert("value".to_string(), value);
                return Ok(true);
            }
        }
    }
    Ok(false)
}
