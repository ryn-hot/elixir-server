use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use reqwest::header::{ACCEPT, HeaderMap, HeaderValue, USER_AGENT};
use reqwest::{Client, Method, StatusCode, Url};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{Value, json};
use tracing::warn;

use crate::drivers::{ApplyResult, CapabilityDriver, DriverCtx, DriverPatch, StateSnapshot};
use crate::drivers::patches::{IndexerRegistryPatch, IndexerSpec};

#[derive(Debug, Default)]
pub struct IndexerRegistryDriver;

impl IndexerRegistryDriver {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl CapabilityDriver for IndexerRegistryDriver {
    fn capability(&self) -> &'static str {
        "indexer.registry"
    }

    async fn read_state(&self, _ctx: DriverCtx) -> Result<StateSnapshot> {
        Ok(StateSnapshot {
            summary: Some("indexer.registry driver is not implemented".to_string()),
        })
    }

    async fn apply_patch(&self, ctx: DriverCtx, patch: DriverPatch) -> Result<ApplyResult> {
        let patch = match patch {
            DriverPatch::IndexerRegistry(patch) => patch,
            _ => bail!("indexer.registry patch mismatch"),
        };
        let implementation = ctx
            .implementation
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("provider implementation is required"))?;
        if implementation != "prowlarr" {
            bail!(
                "indexer.registry implementation '{}' is not supported",
                implementation
            );
        }

        patch.validate()?;

        let config = ProwlarrDriverConfig::from_ctx(&ctx)?;
        let client = ProwlarrClient::from_config(config, ctx.canonical_url()?).await?;

        match patch {
            IndexerRegistryPatch::RegisterIndexers { indexers } => {
                client.upsert_indexers(&indexers).await?;
            }
        }

        Ok(ApplyResult::applied())
    }
}

#[derive(Debug, Deserialize, Default)]
struct ProwlarrDriverConfig {
    #[serde(default)]
    api_key: Option<String>,
    #[serde(default)]
    base_url: Option<String>,
    #[serde(default)]
    api_version: Option<String>,
}

impl ProwlarrDriverConfig {
    fn from_ctx(ctx: &DriverCtx) -> Result<Self> {
        let config = if let Some(raw) = ctx.instance_config.as_ref() {
            serde_json::from_value(raw.clone()).context("parsing Prowlarr driver config")?
        } else {
            ProwlarrDriverConfig::default()
        };
        let api_key = config
            .api_key
            .clone()
            .or_else(|| ctx.secret("prowlarr_api_key").map(str::to_string))
            .or_else(|| ctx.secret("api_key").map(str::to_string))
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| anyhow::anyhow!("prowlarr api_key is required"))?;
        Ok(ProwlarrDriverConfig {
            api_key: Some(api_key),
            base_url: config.base_url,
            api_version: config.api_version,
        })
    }
}

struct ProwlarrClient {
    client: Client,
    root: Url,
    api_base: Url,
    api_version: &'static str,
}

impl ProwlarrClient {
    async fn from_config(config: ProwlarrDriverConfig, endpoint_url: String) -> Result<Self> {
        let api_key = config
            .api_key
            .ok_or_else(|| anyhow::anyhow!("prowlarr api_key is required"))?;
        let root = resolve_base_url(config.base_url.as_deref(), &endpoint_url)?;
        let mut headers = HeaderMap::new();
        headers.insert(
            "X-Api-Key",
            HeaderValue::from_str(&api_key).context("invalid prowlarr api key header")?,
        );
        headers.insert(USER_AGENT, HeaderValue::from_static("Elixir/1.0"));
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .default_headers(headers)
            .build()
            .context("building prowlarr http client")?;
        let mut client = Self {
            client,
            root,
            api_base: Url::parse("http://127.0.0.1/").expect("fallback url"),
            api_version: "v1",
        };

        if let Some(version) = config.api_version.as_deref() {
            client.set_api_version(version)?;
        } else {
            client.set_api_version("v1")?;
        }
        Ok(client)
    }

    fn set_api_version(&mut self, version: &str) -> Result<()> {
        let version = normalize_version(version)?;
        self.api_base = build_api_base(&self.root, version)?;
        self.api_version = version;
        Ok(())
    }

    fn api_url(&self, path: &str) -> Result<Url> {
        self.api_base
            .join(path)
            .context("building prowlarr api url")
    }

    async fn request_json<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
    ) -> Result<T> {
        let value = self.request_json_value(method, path, body).await?;
        serde_json::from_value(value).context("parsing prowlarr response")
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
                bail!("prowlarr api key rejected ({status}): {detail}");
            }
            bail!("prowlarr {} {path} failed ({status}): {detail}", method.as_str());
        }
        if bytes.is_empty() {
            bail!("prowlarr {} {path} returned empty response", method.as_str());
        }
        serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing {} {path} response", method.as_str()))
    }

    async fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        self.request_json(Method::GET, path, None).await
    }

    async fn post_json(&self, path: &str, body: &Value) -> Result<Value> {
        self.request_json_value(Method::POST, path, Some(body)).await
    }

    async fn put_json(&self, path: &str, body: &Value) -> Result<Value> {
        self.request_json_value(Method::PUT, path, Some(body)).await
    }

    async fn upsert_indexers(&self, indexers: &[IndexerSpec]) -> Result<()> {
        if indexers.is_empty() {
            return Ok(());
        }
        let schema = self.get_json::<Vec<Value>>("indexer/schema").await?;
        let existing = self.get_json::<Vec<Value>>("indexer").await?;

        for indexer in indexers {
            let tags = self.ensure_tags(&indexer.tags).await?;
            let schema_item = find_schema(&schema, &indexer.implementation)?;
            let existing_item = find_by_name(&existing, &indexer.name);
            let mut target = match existing_item.clone() {
                Some(existing) => existing,
                None => schema_item,
            };
            let enabled = indexer.enabled.unwrap_or(true);
            set_enabled(&mut target, enabled)?;
            set_string(&mut target, "name", indexer.name.clone())?;
            set_array_i64(&mut target, "tags", &tags)?;
            ensure_schema_fields(&mut target, &indexer.implementation)?;

            let fields = target
                .get_mut("fields")
                .and_then(Value::as_array_mut)
                .ok_or_else(|| anyhow::anyhow!("indexer fields missing"))?;
            apply_indexer_fields(fields, indexer)?;

            if let Some(existing_item) = existing_item {
                if target == existing_item {
                    continue;
                }
            }

            if let Some(id) = target.get("id").and_then(Value::as_i64) {
                let path = format!("indexer/{id}");
                self.put_json(&path, &target).await?;
            } else {
                remove_readonly_fields(&mut target);
                self.post_json("indexer", &target).await?;
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

fn normalize_root_url(base_url: &str) -> Result<Url> {
    let mut url = Url::parse(base_url).context("parsing prowlarr base_url")?;
    let mut path = url.path().trim_end_matches('/').to_string();
    if path.ends_with("/api/v1") {
        path = path.trim_end_matches("/api/v1").to_string();
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
        .ok_or_else(|| anyhow::anyhow!("prowlarr base_url host is missing"))?;
    let endpoint_host = endpoint
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("prowlarr endpoint host is missing"))?;
    let candidate_port = candidate.port_or_known_default().unwrap_or(80);
    let endpoint_port = endpoint.port_or_known_default().unwrap_or(80);
    if candidate.scheme() != endpoint.scheme()
        || candidate_host != endpoint_host
        || candidate_port != endpoint_port
    {
        bail!("prowlarr base_url must match provider endpoint scheme/host/port");
    }
    Ok(())
}

fn normalize_version(version: &str) -> Result<&'static str> {
    let trimmed = version.trim().to_ascii_lowercase();
    match trimmed.as_str() {
        "v1" | "1" => Ok("v1"),
        _ => bail!("unsupported prowlarr api version '{version}'"),
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

fn normalize_name(value: &str) -> String {
    value.trim().to_ascii_lowercase()
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

fn remove_readonly_fields(target: &mut Value) {
    if let Some(map) = target.as_object_mut() {
        map.remove("id");
        map.remove("warnings");
        map.remove("validationFailures");
    }
}

fn apply_indexer_fields(fields: &mut Vec<Value>, spec: &IndexerSpec) -> Result<()> {
    let url_value = Value::String(spec.url.clone());
    if !set_field_value_optional(fields, "baseUrl", url_value.clone())?
        && !set_field_value_optional(fields, "url", url_value.clone())?
    {
        bail!("indexer requires baseUrl or url field");
    }
    if let Some(api_key) = spec.api_key.as_ref() {
        set_field_value_optional(fields, "apiKey", Value::String(api_key.clone()))?;
    }
    if !spec.categories.is_empty() {
        let categories = parse_int_list(&spec.categories)?;
        if !set_field_value_optional(fields, "categories", Value::Array(categories))? {
            warn!("indexer categories field not found");
        }
    }
    for (key, value) in &spec.settings {
        if !set_field_value_optional(fields, key, value.clone())? {
            warn!("indexer field '{}' not found in schema", key);
        }
    }
    Ok(())
}

fn set_field_value_optional(fields: &mut [Value], name: &str, value: Value) -> Result<bool> {
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

fn set_enabled(target: &mut Value, enabled: bool) -> Result<()> {
    if let Some(obj) = target.as_object_mut() {
        if obj.contains_key("enable") {
            obj.insert("enable".to_string(), Value::Bool(enabled));
            return Ok(());
        }
        obj.insert("enabled".to_string(), Value::Bool(enabled));
        return Ok(());
    }
    bail!("expected object for enabled update");
}

fn set_string(target: &mut Value, field: &str, value: String) -> Result<()> {
    if let Some(obj) = target.as_object_mut() {
        obj.insert(field.to_string(), Value::String(value));
        return Ok(());
    }
    bail!("expected object for field '{}'", field);
}

fn set_array_i64(target: &mut Value, field: &str, values: &[i64]) -> Result<()> {
    if let Some(obj) = target.as_object_mut() {
        let array = values.iter().map(|value| Value::Number((*value).into())).collect();
        obj.insert(field.to_string(), Value::Array(array));
        return Ok(());
    }
    bail!("expected object for field '{}'", field);
}

fn parse_int_list(values: &[String]) -> Result<Vec<Value>> {
    let mut parsed = Vec::new();
    for value in values {
        let num = value
            .trim()
            .parse::<i64>()
            .with_context(|| format!("invalid category '{}'", value))?;
        parsed.push(Value::Number(num.into()));
    }
    Ok(parsed)
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
