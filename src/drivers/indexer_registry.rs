use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use reqwest::header::{ACCEPT, HeaderMap, HeaderValue, USER_AGENT};
use reqwest::{Client, Method, StatusCode, Url};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tracing::debug;
use crate::drivers::patches::{AppSpec, IndexerRegistryPatch, IndexerSpec};
use crate::drivers::{ApplyResult, CapabilityDriver, DriverCtx, DriverPatch, StateSnapshot};

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

    async fn read_state(&self, ctx: DriverCtx) -> Result<StateSnapshot> {
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

        let config = ProwlarrDriverConfig::from_ctx(&ctx)?;
        let client = ProwlarrClient::from_config(config, ctx.canonical_url()?).await?;
        let status = client.get_json::<Value>("system/status").await?;
        let version = status
            .get("version")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let indexers = client.get_json::<Vec<Value>>("indexer").await?;
        let summary = format!("Prowlarr v{} · {} indexers", version, indexers.len());

        Ok(StateSnapshot {
            summary: Some(summary),
            activity: None,
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
        let provider_endpoint_url = ctx.endpoint.canonical_url()?;

        match patch {
            IndexerRegistryPatch::RegisterIndexers { indexers } => {
                client.upsert_indexers(&indexers).await?;
            }
            IndexerRegistryPatch::RegisterApp { app } => {
                client.upsert_apps(&[app], &provider_endpoint_url).await?;
            }
            IndexerRegistryPatch::RegisterApps { apps } => {
                client.upsert_apps(&apps, &provider_endpoint_url).await?;
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
            bail!(
                "prowlarr {} {path} failed ({status}): {detail}",
                method.as_str()
            );
        }
        if bytes.is_empty() {
            bail!(
                "prowlarr {} {path} returned empty response",
                method.as_str()
            );
        }
        serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing {} {path} response", method.as_str()))
    }

    async fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        self.request_json(Method::GET, path, None).await
    }

    async fn post_json(&self, path: &str, body: &Value) -> Result<Value> {
        self.request_json_value(Method::POST, path, Some(body))
            .await
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
        let default_app_profile_id = self.resolve_default_app_profile_id().await?;
        let mut unsupported: Vec<(String, String)> = Vec::new();
        let mut realized = 0usize;

        for indexer in indexers {
            let existing_item = find_by_name(&existing, &indexer.name);
            let schema_item = find_schema_optional(&schema, &indexer.implementation);
            let Some(schema_or_existing) = schema_item.clone().or_else(|| existing_item.clone()) else {
                unsupported.push((indexer.name.clone(), indexer.implementation.clone()));
                continue;
            };
            let tags = self.ensure_tags(&indexer.tags).await?;
            let mut target = match existing_item.clone() {
                Some(existing) => existing,
                None => schema_or_existing,
            };
            let enabled = indexer.enabled.unwrap_or(true);
            set_enabled(&mut target, enabled)?;
            set_string(&mut target, "name", indexer.name.clone())?;
            set_array_i64(&mut target, "tags", &tags)?;
            ensure_schema_fields(&mut target, &indexer.implementation)?;
            apply_indexer_defaults(&mut target, default_app_profile_id)?;

            let fields = target
                .get_mut("fields")
                .and_then(Value::as_array_mut)
                .ok_or_else(|| anyhow::anyhow!("indexer fields missing"))?;
            apply_indexer_fields(fields, indexer)?;

            if let Some(existing_item) = existing_item {
                if target == existing_item {
                    realized += 1;
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
            realized += 1;
        }
        if !unsupported.is_empty() {
            bail!(
                "prowlarr schema does not support requested indexers: {}",
                unsupported
                    .iter()
                    .map(|(name, implementation)| format!("{name} ({implementation})"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        if realized == 0 {
            bail!("prowlarr did not apply any requested indexers");
        }
        Ok(())
    }

    async fn upsert_apps(&self, apps: &[AppSpec], prowlarr_url: &str) -> Result<()> {
        if apps.is_empty() {
            return Ok(());
        }
        let schema = self.get_json::<Vec<Value>>("applications/schema").await?;
        let existing = self.get_json::<Vec<Value>>("applications").await?;

        for app in apps {
            let tags = if app.tags.is_empty() {
                Vec::new()
            } else {
                self.ensure_tags(&app.tags).await?
            };
            let schema_item = find_schema(&schema, &app.implementation)?;
            let existing_item = find_app_by_identity(&existing, app);
            let mut target = match existing_item.clone() {
                Some(existing) => existing,
                None => schema_item,
            };
            let enabled = app.enabled.unwrap_or(true);
            set_enabled(&mut target, enabled)?;
            set_string(&mut target, "name", app.name.clone())?;
            if !tags.is_empty() {
                set_array_i64(&mut target, "tags", &tags)?;
            }
            ensure_schema_fields(&mut target, &app.implementation)?;

            let fields = target
                .get_mut("fields")
                .and_then(Value::as_array_mut)
                .ok_or_else(|| anyhow::anyhow!("app fields missing"))?;
            apply_app_fields(fields, app, prowlarr_url)?;

            if let Some(existing_item) = existing_item {
                if target == existing_item {
                    continue;
                }
            }

            if let Some(id) = target.get("id").and_then(Value::as_i64) {
                let path = format!("applications/{id}");
                self.put_json(&path, &target).await?;
            } else {
                remove_readonly_fields(&mut target);
                self.post_json("applications", &target).await?;
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
            let created = self.post_json("tag", &json!({ "label": tag })).await?;
            if let Some(id) = created.get("id").and_then(Value::as_i64) {
                by_name.insert(normalized, id);
                tag_ids.push(id);
            } else {
                bail!("tag creation did not return id");
            }
        }
        Ok(tag_ids)
    }

    async fn resolve_default_app_profile_id(&self) -> Result<Option<i64>> {
        let profiles = self.get_json::<Vec<Value>>("appProfile").await?;
        if profiles.is_empty() {
            return Ok(None);
        }

        if let Some(id) = profiles.iter().find_map(|profile| {
            let name = profile.get("name").and_then(Value::as_str)?;
            let id = profile.get("id").and_then(Value::as_i64)?;
            if name.trim().eq_ignore_ascii_case("standard") && id > 0 {
                Some(id)
            } else {
                None
            }
        }) {
            return Ok(Some(id));
        }

        Ok(profiles
            .iter()
            .filter_map(|profile| profile.get("id").and_then(Value::as_i64))
            .find(|id| *id > 0))
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

fn find_app_by_identity(items: &[Value], app: &AppSpec) -> Option<Value> {
    let target_name = normalize_name(&app.name);
    let target_impl = normalize_name(&app.implementation);
    let target_url = normalize_url(&app.url);

    items.iter().find_map(|item| {
        let name = item.get("name").and_then(Value::as_str)?;
        if normalize_name(name) != target_name {
            return None;
        }

        let implementation = item
            .get("implementation")
            .and_then(Value::as_str)
            .or_else(|| item.get("implementationName").and_then(Value::as_str))
            .or_else(|| item.get("configContract").and_then(Value::as_str))
            .map(normalize_name)
            .unwrap_or_default();
        if implementation != target_impl && !implementation.contains(&target_impl) {
            return None;
        }

        let Some(existing_url) = app_url_from_item(item) else {
            return None;
        };
        if normalize_url(&existing_url) != target_url {
            return None;
        }

        Some(item.clone())
    })
}

fn app_url_from_item(item: &Value) -> Option<String> {
    let fields = item.get("fields")?.as_array()?;
    for key in ["baseUrl", "url", "serverUrl", "prowlarrUrl"] {
        let value = field_string_value(fields, key)?;
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    None
}

fn field_string_value(fields: &[Value], key: &str) -> Option<String> {
    for field in fields {
        let name = field.get("name").and_then(Value::as_str)?;
        if !name.eq_ignore_ascii_case(key) {
            continue;
        }
        let value = field.get("value")?;
        if let Some(text) = value.as_str() {
            return Some(text.to_string());
        }
        if value.is_number() {
            return Some(value.to_string());
        }
    }
    None
}

fn normalize_url(value: &str) -> String {
    let trimmed = value.trim();
    if let Ok(url) = Url::parse(trimmed) {
        let mut normalized = format!(
            "{}://{}",
            url.scheme().to_ascii_lowercase(),
            url.host_str().unwrap_or("").to_ascii_lowercase()
        );
        if let Some(port) = url.port_or_known_default() {
            normalized.push(':');
            normalized.push_str(&port.to_string());
        }
        let path = url.path().trim_end_matches('/');
        if !path.is_empty() {
            normalized.push_str(path);
        }
        return normalized;
    }
    trimmed.to_ascii_lowercase()
}

fn find_schema(items: &[Value], implementation: &str) -> Result<Value> {
    find_schema_optional(items, implementation)
        .ok_or_else(|| anyhow::anyhow!("implementation '{}' not found", implementation))
}

fn find_schema_optional(items: &[Value], implementation: &str) -> Option<Value> {
    let needle = normalize_name(implementation);
    let exact = items.iter().find_map(|value| {
        let matches = value
            .get("implementation")
            .and_then(Value::as_str)
            .into_iter()
            .chain(value.get("implementationName").and_then(Value::as_str))
            .chain(value.get("name").and_then(Value::as_str))
            .any(|candidate| normalize_name(candidate) == needle);
        if matches { Some(value.clone()) } else { None }
    });
    if exact.is_some() {
        return exact;
    }

    // Fallback for minor naming drift between manifests and live schema
    // (for example short-name vs implementationName) while remaining deterministic.
    let fuzzy: Vec<Value> = items
        .iter()
        .filter_map(|value| {
            let matches = value
                .get("implementation")
                .and_then(Value::as_str)
                .into_iter()
                .chain(value.get("implementationName").and_then(Value::as_str))
                .chain(value.get("name").and_then(Value::as_str))
                .map(normalize_name)
                .any(|candidate| candidate.starts_with(&needle) || needle.starts_with(&candidate));
            if matches { Some(value.clone()) } else { None }
        })
        .collect();

    if fuzzy.len() == 1 {
        return fuzzy.into_iter().next();
    }
    None
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

fn apply_indexer_defaults(target: &mut Value, default_app_profile_id: Option<i64>) -> Result<()> {
    let Some(map) = target.as_object_mut() else {
        bail!("expected object for indexer defaults");
    };

    if map.contains_key("appProfileId") {
        let current = map.get("appProfileId").and_then(Value::as_i64).unwrap_or(0);
        if current <= 0 {
            let app_profile_id = default_app_profile_id.ok_or_else(|| {
                anyhow::anyhow!(
                    "prowlarr requires an app profile for this indexer, but none are available"
                )
            })?;
            map.insert(
                "appProfileId".to_string(),
                Value::Number(app_profile_id.into()),
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
            debug!("indexer categories field not present in schema; skipping categories");
        }
    }
    for (key, value) in &spec.settings {
        if !set_field_value_optional(fields, key, value.clone())? {
            debug!("indexer field '{}' not present in schema; skipping", key);
        }
    }
    Ok(())
}

fn apply_app_fields(fields: &mut Vec<Value>, spec: &AppSpec, prowlarr_url: &str) -> Result<()> {
    let prowlarr_url = normalize_root_url(prowlarr_url)?.to_string();
    set_field_value_optional(
        fields,
        "prowlarrUrl",
        Value::String(prowlarr_url.to_string()),
    )?;

    let url_value = Value::String(spec.url.clone());
    if !set_field_value_optional(fields, "baseUrl", url_value.clone())?
        && !set_field_value_optional(fields, "url", url_value.clone())?
    {
        bail!("app requires baseUrl or url field");
    }
    if let Some(api_key) = spec.api_key.as_ref() {
        set_field_value_optional(fields, "apiKey", Value::String(api_key.clone()))?;
    }
    if !spec.categories.is_empty() {
        let categories = parse_int_list(&spec.categories)?;
        if !set_field_value_optional(fields, "syncCategories", Value::Array(categories))? {
            debug!("app syncCategories field not present in schema; skipping categories");
        }
    }
    for (key, value) in &spec.settings {
        if !set_field_value_optional(fields, key, value.clone())? {
            debug!("app field '{}' not present in schema; skipping", key);
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
        let array = values
            .iter()
            .map(|value| Value::Number((*value).into()))
            .collect();
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, routing::{get, post}};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;

    #[test]
    fn register_app_identity_matches_name_type_and_url() {
        let app = AppSpec {
            name: "Sonarr".to_string(),
            implementation: "Sonarr".to_string(),
            url: "http://sonarr:8989".to_string(),
            api_key: Some("abc".to_string()),
            categories: vec![],
            tags: vec![],
            enabled: Some(true),
            settings: HashMap::new(),
        };
        let existing = vec![json!({
            "id": 5,
            "name": "sonarr",
            "implementation": "sonarr",
            "fields": [
                { "name": "baseUrl", "value": "http://sonarr:8989/" }
            ]
        })];

        let matched = find_app_by_identity(&existing, &app);
        assert!(matched.is_some(), "app identity should match");
    }

    #[test]
    fn register_app_identity_rejects_url_mismatch() {
        let app = AppSpec {
            name: "Radarr".to_string(),
            implementation: "Radarr".to_string(),
            url: "http://radarr:7878".to_string(),
            api_key: Some("abc".to_string()),
            categories: vec![],
            tags: vec![],
            enabled: Some(true),
            settings: HashMap::new(),
        };
        let existing = vec![json!({
            "id": 8,
            "name": "radarr",
            "implementation": "radarr",
            "fields": [
                { "name": "baseUrl", "value": "http://radarr:8888/" }
            ]
        })];

        let matched = find_app_by_identity(&existing, &app);
        assert!(
            matched.is_none(),
            "app identity should require matching URL"
        );
    }

    #[test]
    fn find_schema_optional_returns_none_when_implementation_missing() {
        let schema = vec![json!({
            "implementation": "Newznab"
        })];

        let found = find_schema_optional(&schema, "Nyaa");
        assert!(
            found.is_none(),
            "missing implementation should be skipped by optional lookup"
        );
    }

    #[test]
    fn find_schema_optional_matches_single_fuzzy_candidate() {
        let schema = vec![
            json!({"implementation": "NyaaSi"}),
            json!({"implementation": "Newznab"}),
        ];

        let found = find_schema_optional(&schema, "Nyaa");
        assert!(found.is_some(), "expected fuzzy implementation match");
        assert_eq!(
            found
                .as_ref()
                .and_then(|value| value.get("implementation"))
                .and_then(Value::as_str),
            Some("NyaaSi")
        );
    }

    #[test]
    fn find_schema_returns_error_when_implementation_missing() {
        let schema = vec![json!({
            "implementation": "Newznab"
        })];

        let err = find_schema(&schema, "Nyaa").expect_err("expected missing implementation error");
        let message = err.to_string();
        assert!(
            message.contains("implementation 'Nyaa' not found"),
            "unexpected error message: {message}"
        );
    }

    #[test]
    fn apply_app_fields_sets_prowlarr_url_when_supported() {
        let app = AppSpec {
            name: "Radarr".to_string(),
            implementation: "Radarr".to_string(),
            url: "http://elx-radarr:7878".to_string(),
            api_key: Some("abc".to_string()),
            categories: vec!["2000".to_string()],
            tags: vec![],
            enabled: Some(true),
            settings: HashMap::new(),
        };
        let mut fields = vec![
            json!({"name": "prowlarrUrl", "value": ""}),
            json!({"name": "baseUrl", "value": ""}),
            json!({"name": "apiKey", "value": ""}),
            json!({"name": "syncCategories", "value": []}),
        ];

        apply_app_fields(&mut fields, &app, "http://svc-prowlarr:9696/").unwrap();

        let prowlarr_url = field_string_value(&fields, "prowlarrUrl").unwrap();
        assert_eq!(prowlarr_url, "http://svc-prowlarr:9696/");
        let base_url = field_string_value(&fields, "baseUrl").unwrap();
        assert_eq!(base_url, "http://elx-radarr:7878");
    }

    #[test]
    fn find_schema_optional_returns_none_for_removed_public_indexers() {
        let schema = vec![
            json!({"implementation": "Anidex"}),
            json!({"implementation": "TorrentsCSV"}),
        ];

        assert!(find_schema_optional(&schema, "Nyaa").is_none());
        assert!(find_schema_optional(&schema, "EZTV").is_none());
    }

    #[tokio::test]
    async fn upsert_indexers_fails_when_requested_implementation_is_missing_from_schema() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind test server");
        let addr = listener.local_addr().expect("local addr");

        let app = Router::new()
            .route(
                "/api/v1/indexer/schema",
                get(|| async { Json(json!([{ "implementation": "Anidex", "fields": [] }])) }),
            )
            .route("/api/v1/indexer", get(|| async { Json(json!([])) }))
            .route("/api/v1/indexer", post(|| async { Json(json!({ "id": 1 })) }));

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });

        let client = ProwlarrClient::from_config(
            ProwlarrDriverConfig {
                api_key: Some("test".to_string()),
                base_url: None,
                api_version: None,
            },
            format!("http://127.0.0.1:{}/", addr.port()),
        )
        .await
        .expect("build client");

        let err = client
            .upsert_indexers(&[IndexerSpec {
                name: "Nyaa".to_string(),
                implementation: "Nyaa".to_string(),
                url: "https://nyaa.si/".to_string(),
                auth: crate::drivers::patches::IndexerAuthSpec {
                    requires_account: Some(false),
                    required_fields: Vec::new(),
                },
                api_key: None,
                categories: Vec::new(),
                tags: Vec::new(),
                enabled: Some(true),
                settings: HashMap::new(),
            }])
            .await
            .expect_err("unsupported schema implementation should fail");

        let _ = shutdown_tx.send(());

        assert!(
            err.to_string()
                .contains("prowlarr schema does not support requested indexers"),
            "unexpected error: {err:#}"
        );
    }

    #[tokio::test]
    async fn upsert_indexers_assigns_default_app_profile_id_when_required() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind test server");
        let addr = listener.local_addr().expect("local addr");
        let captured = Arc::new(Mutex::new(None::<Value>));

        let app = Router::new()
            .route(
                "/api/v1/indexer/schema",
                get(|| async {
                    Json(json!([{
                        "name": "AnimeTosho",
                        "implementation": "Torznab",
                        "appProfileId": 0,
                        "fields": [
                            { "name": "baseUrl", "value": "" },
                            { "name": "apiPath", "value": "/api" }
                        ]
                    }]))
                }),
            )
            .route(
                "/api/v1/appProfile",
                get(|| async { Json(json!([{ "name": "Standard", "id": 1 }])) }),
            )
            .route("/api/v1/tag", get(|| async { Json(json!([])) }))
            .route("/api/v1/tag", post(|| async { Json(json!({ "id": 77 })) }))
            .route("/api/v1/indexer", get(|| async { Json(json!([])) }))
            .route(
                "/api/v1/indexer",
                post({
                    let captured = Arc::clone(&captured);
                    move |Json(body): Json<Value>| {
                        let captured = Arc::clone(&captured);
                        async move {
                            *captured.lock().expect("capture payload") = Some(body);
                            Json(json!({ "id": 1 }))
                        }
                    }
                }),
            );

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });

        let client = ProwlarrClient::from_config(
            ProwlarrDriverConfig {
                api_key: Some("test".to_string()),
                base_url: None,
                api_version: None,
            },
            format!("http://127.0.0.1:{}/", addr.port()),
        )
        .await
        .expect("build client");

        client
            .upsert_indexers(&[IndexerSpec {
                name: "AnimeTosho".to_string(),
                implementation: "Torznab".to_string(),
                url: "https://feed.animetosho.org".to_string(),
                auth: crate::drivers::patches::IndexerAuthSpec {
                    requires_account: Some(false),
                    required_fields: Vec::new(),
                },
                api_key: None,
                categories: Vec::new(),
                tags: vec!["public".to_string()],
                enabled: Some(true),
                settings: HashMap::new(),
            }])
            .await
            .expect("indexer creation should succeed");

        let _ = shutdown_tx.send(());

        let body = captured
            .lock()
            .expect("captured payload")
            .clone()
            .expect("indexer create payload");
        assert_eq!(body.get("appProfileId").and_then(Value::as_i64), Some(1));
    }
}
