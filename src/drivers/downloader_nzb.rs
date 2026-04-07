use std::collections::{BTreeMap, HashSet};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use reqwest::header::{ACCEPT, HeaderMap, HeaderValue, USER_AGENT};
use reqwest::{Client, Method, StatusCode, Url};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::drivers::patches::{DownloadCategorySpec, DownloaderNzbPatch};
use crate::drivers::{
    ActivitySnapshot, ApplyResult, CapabilityDriver, DriverCtx, DriverPatch, StateSnapshot,
};

#[derive(Debug, Default)]
pub struct DownloaderNzbDriver;

impl DownloaderNzbDriver {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl CapabilityDriver for DownloaderNzbDriver {
    fn capability(&self) -> &'static str {
        "downloader.nzb"
    }

    async fn read_state(&self, ctx: DriverCtx) -> Result<StateSnapshot> {
        let implementation = ctx
            .implementation
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("provider implementation is required"))?;
        if !implementation.eq_ignore_ascii_case("nzbget") {
            bail!(
                "downloader.nzb implementation '{}' is not supported",
                implementation
            );
        }
        let client =
            NzbgetClient::from_config(NzbgetDriverConfig::from_ctx(&ctx)?, ctx.canonical_url()?)
                .await?;
        let version = client.version().await?;
        let status = client.status().await?;
        let groups = client.list_groups().await?;
        let activity = summarize_nzbget_activity(&status, &groups);
        Ok(StateSnapshot {
            summary: Some(summarize_nzbget_state(&version, &activity)),
            activity: Some(activity),
        })
    }

    async fn apply_patch(&self, ctx: DriverCtx, patch: DriverPatch) -> Result<ApplyResult> {
        let patch = match patch {
            DriverPatch::DownloaderNzb(patch) => patch,
            _ => bail!("downloader.nzb patch mismatch"),
        };
        let implementation = ctx
            .implementation
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("provider implementation is required"))?;
        if !implementation.eq_ignore_ascii_case("nzbget") {
            bail!(
                "downloader.nzb implementation '{}' is not supported",
                implementation
            );
        }

        patch.validate()?;

        let config = NzbgetDriverConfig::from_ctx(&ctx)?;
        let client = NzbgetClient::from_config(config, ctx.canonical_url()?).await?;

        match patch {
            DownloaderNzbPatch::SetCategories { categories } => {
                client.upsert_categories(&categories).await?;
            }
            DownloaderNzbPatch::SetPreferences {
                default_save_path,
                incomplete_path,
                use_incomplete,
                server_connections,
                article_retries,
                article_timeout_seconds,
                article_cache_mb,
                direct_write,
                write_buffer_kb,
                continue_partial,
                par_check,
                par_scan,
                par_quick,
                par_repair,
                par_rename,
                par_pause_queue,
                par_threads,
                unpack,
                unpack_pause_queue,
                download_rate_kib,
            } => {
                client
                    .set_preferences(
                        default_save_path,
                        incomplete_path,
                        use_incomplete,
                        server_connections,
                        article_retries,
                        article_timeout_seconds,
                        article_cache_mb,
                        direct_write,
                        write_buffer_kb,
                        continue_partial,
                        par_check,
                        par_scan,
                        par_quick,
                        par_repair,
                        par_rename,
                        par_pause_queue,
                        par_threads,
                        unpack,
                        unpack_pause_queue,
                        download_rate_kib,
                    )
                    .await?;
            }
        }

        Ok(ApplyResult::applied())
    }
}

#[derive(Debug, Deserialize)]
struct NzbgetStatus {
    #[serde(rename = "DownloadRate", default)]
    download_rate: Option<u64>,
    #[serde(rename = "DownloadedSizeLo", default)]
    downloaded_size_lo: Option<u64>,
    #[serde(rename = "DownloadedSizeHi", default)]
    downloaded_size_hi: Option<u64>,
    #[serde(rename = "PostJobCount", default)]
    post_job_count: Option<u64>,
    #[serde(rename = "ServerStandBy", default)]
    server_stand_by: Option<bool>,
    #[serde(rename = "DownloadPaused", default)]
    download_paused: Option<bool>,
    #[serde(rename = "PostPaused", default)]
    post_paused: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct NzbgetGroup {
    #[serde(rename = "Status", default)]
    status: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct NzbgetDriverConfig {
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    password: Option<String>,
    #[serde(default)]
    base_url: Option<String>,
}

impl NzbgetDriverConfig {
    fn from_ctx(ctx: &DriverCtx) -> Result<Self> {
        let config = if let Some(raw) = ctx.instance_config.as_ref() {
            serde_json::from_value(raw.clone()).context("parsing nzbget driver config")?
        } else {
            NzbgetDriverConfig::default()
        };
        let username = config
            .username
            .clone()
            .or_else(|| ctx.secret("nzbget_username").map(str::to_string))
            .or_else(|| ctx.secret("username").map(str::to_string))
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| anyhow::anyhow!("nzbget username is required"))?;
        let password = config
            .password
            .clone()
            .or_else(|| ctx.secret("nzbget_password").map(str::to_string))
            .or_else(|| ctx.secret("password").map(str::to_string))
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| anyhow::anyhow!("nzbget password is required"))?;
        Ok(NzbgetDriverConfig {
            username: Some(username),
            password: Some(password),
            base_url: config.base_url,
        })
    }
}

struct NzbgetClient {
    client: Client,
    root: Url,
    username: String,
    password: String,
}

impl NzbgetClient {
    async fn from_config(config: NzbgetDriverConfig, endpoint_url: String) -> Result<Self> {
        let username = config
            .username
            .ok_or_else(|| anyhow::anyhow!("nzbget username is required"))?;
        let password = config
            .password
            .ok_or_else(|| anyhow::anyhow!("nzbget password is required"))?;
        let root = resolve_base_url(config.base_url.as_deref(), &endpoint_url)?;
        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static("Elixir/1.0"));
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .default_headers(headers)
            .build()
            .context("building nzbget http client")?;

        let client = Self {
            client,
            root,
            username,
            password,
        };
        let _ = client.version().await?;
        Ok(client)
    }

    async fn version(&self) -> Result<String> {
        let value = self.rpc("version", Value::Array(Vec::new())).await?;
        match value {
            Value::String(version) if !version.trim().is_empty() => Ok(version),
            other => bail!("nzbget version returned unexpected payload: {other}"),
        }
    }

    async fn status(&self) -> Result<NzbgetStatus> {
        let value = self.rpc("status", Value::Array(Vec::new())).await?;
        serde_json::from_value(value).context("parsing nzbget status response")
    }

    async fn list_groups(&self) -> Result<Vec<NzbgetGroup>> {
        let value = self.rpc("listgroups", json!([0])).await?;
        serde_json::from_value(value).context("parsing nzbget listgroups response")
    }

    async fn read_config(&self) -> Result<Vec<NzbgetConfigItem>> {
        let value = self.rpc("config", Value::Array(Vec::new())).await?;
        serde_json::from_value(value).context("parsing nzbget config response")
    }

    async fn save_config(&self, updates: Vec<NzbgetConfigUpdate>) -> Result<()> {
        if updates.is_empty() {
            return Ok(());
        }
        let result = self
            .rpc("saveconfig", json!([updates]))
            .await
            .context("saving nzbget config")?;
        if !rpc_success(&result) {
            bail!("nzbget saveconfig returned unexpected payload: {result}");
        }
        let reload = self
            .rpc("reload", Value::Array(Vec::new()))
            .await
            .context("reloading nzbget config")?;
        if !rpc_success(&reload) {
            bail!("nzbget reload returned unexpected payload: {reload}");
        }
        Ok(())
    }

    async fn set_preferences(
        &self,
        default_save_path: Option<String>,
        incomplete_path: Option<String>,
        use_incomplete: Option<bool>,
        server_connections: Option<u64>,
        article_retries: Option<u64>,
        article_timeout_seconds: Option<u64>,
        article_cache_mb: Option<u64>,
        direct_write: Option<bool>,
        write_buffer_kb: Option<u64>,
        continue_partial: Option<bool>,
        par_check: Option<String>,
        par_scan: Option<String>,
        par_quick: Option<bool>,
        par_repair: Option<bool>,
        par_rename: Option<bool>,
        par_pause_queue: Option<bool>,
        par_threads: Option<u64>,
        unpack: Option<bool>,
        unpack_pause_queue: Option<bool>,
        download_rate_kib: Option<u64>,
    ) -> Result<()> {
        let config = self.read_config().await?;
        let mut updates = Vec::new();
        if let Some(path) = default_save_path {
            updates.push(NzbgetConfigUpdate::new("DestDir", path));
        }
        match use_incomplete {
            Some(false) => updates.push(NzbgetConfigUpdate::new("InterDir", "")),
            _ => {
                if let Some(path) = incomplete_path {
                    updates.push(NzbgetConfigUpdate::new("InterDir", path));
                }
            }
        }
        if let Some(connections) = server_connections {
            updates.extend(server_connection_updates(&config, connections));
        }
        push_numeric_update(&mut updates, "ArticleRetries", article_retries);
        push_numeric_update(&mut updates, "ArticleTimeout", article_timeout_seconds);
        push_numeric_update(&mut updates, "ArticleCache", article_cache_mb);
        push_bool_update(&mut updates, "DirectWrite", direct_write);
        push_numeric_update(&mut updates, "WriteBuffer", write_buffer_kb);
        push_bool_update(&mut updates, "ContinuePartial", continue_partial);
        push_string_update(&mut updates, "ParCheck", par_check);
        push_string_update(&mut updates, "ParScan", par_scan);
        push_bool_update(&mut updates, "ParQuick", par_quick);
        push_bool_update(&mut updates, "ParRepair", par_repair);
        push_bool_update(&mut updates, "ParRename", par_rename);
        push_bool_update(&mut updates, "ParPauseQueue", par_pause_queue);
        push_numeric_update(&mut updates, "ParThreads", par_threads);
        push_bool_update(&mut updates, "Unpack", unpack);
        push_bool_update(&mut updates, "UnpackPauseQueue", unpack_pause_queue);
        push_numeric_update(&mut updates, "DownloadRate", download_rate_kib);
        self.save_config(updates).await
    }

    async fn upsert_categories(&self, categories: &[DownloadCategorySpec]) -> Result<()> {
        if categories.is_empty() {
            return Ok(());
        }
        let config = self.read_config().await?;
        let mut slots = category_slots(&config);
        if slots.is_empty() {
            for index in 1..=15 {
                slots.insert(index, CategorySlot::default());
            }
        }

        let mut used_slots = HashSet::new();
        let mut updates = Vec::new();
        for category in categories {
            let desired = normalize_name(&category.name);
            let selected_slot = slots
                .iter()
                .find_map(|(slot, current)| {
                    if normalize_name(&current.name) == desired {
                        Some(*slot)
                    } else {
                        None
                    }
                })
                .or_else(|| {
                    slots.iter().find_map(|(slot, current)| {
                        if used_slots.contains(slot) {
                            None
                        } else if current.name.trim().is_empty() {
                            Some(*slot)
                        } else {
                            None
                        }
                    })
                })
                .ok_or_else(|| anyhow::anyhow!("no free nzbget category slots available"))?;
            used_slots.insert(selected_slot);

            let current = slots.entry(selected_slot).or_default();
            if current.name.trim() != category.name.trim() {
                updates.push(NzbgetConfigUpdate::new(
                    format!("Category{selected_slot}.Name"),
                    category.name.clone(),
                ));
                current.name = category.name.clone();
            }
            if let Some(save_path) = category.save_path.as_ref() {
                if current.dest_dir.trim() != save_path.trim() {
                    updates.push(NzbgetConfigUpdate::new(
                        format!("Category{selected_slot}.DestDir"),
                        save_path.clone(),
                    ));
                    current.dest_dir = save_path.clone();
                }
            }
        }

        self.save_config(updates).await
    }

    async fn rpc(&self, method: &str, params: Value) -> Result<Value> {
        let mut url = self.root.clone();
        url.set_path("/jsonrpc");
        let response = self
            .client
            .request(Method::POST, url)
            .basic_auth(&self.username, Some(&self.password))
            .json(&json!({
                "version": "1.1",
                "method": method,
                "params": params,
                "id": 1
            }))
            .send()
            .await
            .with_context(|| format!("nzbget {method}"))?;
        let status = response.status();
        let body = response
            .bytes()
            .await
            .with_context(|| format!("reading nzbget {method} response"))?;
        if !status.is_success() {
            let detail = describe_error_body(&body);
            if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
                bail!("nzbget auth rejected ({status}): {detail}");
            }
            bail!("nzbget {method} failed ({status}): {detail}");
        }
        let payload: Value =
            serde_json::from_slice(&body).context("parsing nzbget json response")?;
        if let Some(error) = payload.get("error").filter(|value| !value.is_null()) {
            bail!("nzbget {method} returned error: {error}");
        }
        payload
            .get("result")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("nzbget {method} response missing result"))
    }
}

#[derive(Debug, Clone, Deserialize)]
struct NzbgetConfigItem {
    #[serde(alias = "Name")]
    name: String,
    #[serde(alias = "Value")]
    value: String,
}

#[derive(Debug, Clone, Serialize)]
struct NzbgetConfigUpdate {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Value")]
    value: String,
}

impl NzbgetConfigUpdate {
    fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct CategorySlot {
    name: String,
    dest_dir: String,
}

fn category_slots(config: &[NzbgetConfigItem]) -> BTreeMap<u32, CategorySlot> {
    let mut slots = BTreeMap::new();
    for item in config {
        let Some((slot, field)) = parse_category_option(&item.name) else {
            continue;
        };
        let entry = slots.entry(slot).or_insert_with(CategorySlot::default);
        match field {
            "Name" => entry.name = item.value.clone(),
            "DestDir" => entry.dest_dir = item.value.clone(),
            _ => {}
        }
    }
    slots
}

fn server_connection_updates(
    config: &[NzbgetConfigItem],
    target_connections: u64,
) -> Vec<NzbgetConfigUpdate> {
    let mut hosts: BTreeMap<u32, String> = BTreeMap::new();
    let mut enabled: BTreeMap<u32, bool> = BTreeMap::new();
    let mut updates = Vec::new();
    for item in config {
        let Some((slot, field)) = parse_server_option(&item.name) else {
            continue;
        };
        match field {
            "Host" => {
                hosts.insert(slot, item.value.trim().to_string());
            }
            "Active" => {
                enabled.insert(slot, parse_nzb_bool(&item.value));
            }
            _ => {}
        }
    }
    for (slot, host) in hosts {
        if host.is_empty() {
            continue;
        }
        if enabled.get(&slot).copied() == Some(false) {
            continue;
        }
        updates.push(NzbgetConfigUpdate::new(
            format!("Server{slot}.Connections"),
            target_connections.to_string(),
        ));
    }
    updates
}

fn parse_server_option(name: &str) -> Option<(u32, &str)> {
    let suffix = name.strip_prefix("Server")?;
    let (slot, field) = suffix.split_once('.')?;
    let slot = slot.parse::<u32>().ok()?;
    Some((slot, field))
}

fn push_numeric_update(updates: &mut Vec<NzbgetConfigUpdate>, key: &str, value: Option<u64>) {
    if let Some(value) = value {
        updates.push(NzbgetConfigUpdate::new(key, value.to_string()));
    }
}

fn push_bool_update(updates: &mut Vec<NzbgetConfigUpdate>, key: &str, value: Option<bool>) {
    if let Some(value) = value {
        updates.push(NzbgetConfigUpdate::new(
            key,
            if value { "yes" } else { "no" },
        ));
    }
}

fn push_string_update(updates: &mut Vec<NzbgetConfigUpdate>, key: &str, value: Option<String>) {
    if let Some(value) = value {
        updates.push(NzbgetConfigUpdate::new(key, value));
    }
}

fn parse_nzb_bool(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "yes" | "true" | "1" | "on"
    )
}

fn parse_category_option(name: &str) -> Option<(u32, &str)> {
    let rest = name.strip_prefix("Category")?;
    let (slot, field) = rest.split_once('.')?;
    let slot = slot.parse::<u32>().ok()?;
    Some((slot, field))
}

fn normalize_name(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn rpc_success(value: &Value) -> bool {
    match value {
        Value::Bool(ok) => *ok,
        Value::Number(number) => number.as_u64() == Some(1),
        Value::Null => true,
        Value::String(text) => {
            matches!(
                text.trim().to_ascii_lowercase().as_str(),
                "true" | "ok" | "1"
            )
        }
        _ => false,
    }
}

fn resolve_base_url(config_base_url: Option<&str>, endpoint_url: &str) -> Result<Url> {
    let url = config_base_url.unwrap_or(endpoint_url);
    Url::parse(url).context("parsing nzbget base url")
}

fn summarize_nzbget_activity(status: &NzbgetStatus, groups: &[NzbgetGroup]) -> ActivitySnapshot {
    let mut active_items = 0u64;
    let mut queued_items = 0u64;
    let mut error_items = 0u64;

    for group in groups {
        let state = group
            .status
            .as_deref()
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        if state.is_empty() {
            continue;
        }
        if is_nzbget_error_state(&state) {
            error_items += 1;
        }
        if is_nzbget_active_state(&state) {
            active_items += 1;
        } else if is_nzbget_queued_state(&state) {
            queued_items += 1;
        }
    }

    ActivitySnapshot {
        status: Some(nzbget_status_label(status)),
        download_rate_bps: status.download_rate,
        upload_rate_bps: None,
        active_items: Some(active_items),
        queued_items: Some(queued_items),
        error_items: Some(error_items),
        post_process_items: status.post_job_count,
        downloaded_bytes: combine_u64_parts(status.downloaded_size_hi, status.downloaded_size_lo),
        uploaded_bytes: None,
    }
}

fn summarize_nzbget_state(version: &str, activity: &ActivitySnapshot) -> String {
    let mut parts = vec![format!("NZBGet {version}")];
    if let Some(status) = activity
        .status
        .as_deref()
        .filter(|status| !status.trim().is_empty())
    {
        parts.push(status.to_string());
    }
    if let Some(errors) = activity.error_items.filter(|count| *count > 0) {
        parts.push(format!(
            "{errors} issue{}",
            if errors == 1 { "" } else { "s" }
        ));
    }
    parts.join(" · ")
}

fn nzbget_status_label(status: &NzbgetStatus) -> String {
    if status.download_paused.unwrap_or(false) {
        "paused".to_string()
    } else if status.post_paused.unwrap_or(false) {
        "post paused".to_string()
    } else if status.server_stand_by.unwrap_or(false) {
        "idle".to_string()
    } else {
        "downloading".to_string()
    }
}

fn is_nzbget_active_state(state: &str) -> bool {
    matches!(
        state,
        "downloading"
            | "fetching"
            | "checking"
            | "repairing"
            | "extracting"
            | "moving"
            | "running"
            | "processing"
    )
}

fn is_nzbget_queued_state(state: &str) -> bool {
    matches!(state, "queued" | "paused")
}

fn is_nzbget_error_state(state: &str) -> bool {
    state.contains("failure") || state.contains("warning")
}

fn combine_u64_parts(hi: Option<u64>, lo: Option<u64>) -> Option<u64> {
    match (hi, lo) {
        (Some(hi), Some(lo)) => Some((hi << 32) | lo),
        (Some(hi), None) => Some(hi << 32),
        (None, Some(lo)) => Some(lo),
        (None, None) => None,
    }
}

fn describe_error_body(body: &[u8]) -> String {
    if let Ok(value) = std::str::from_utf8(body) {
        if !value.trim().is_empty() {
            return value.trim().to_string();
        }
    }
    "<empty response>".to_string()
}

#[cfg(test)]
mod tests {
    use super::{
        CategorySlot, DownloaderNzbDriver, DriverCtx, NzbgetConfigItem, category_slots,
        parse_category_option, rpc_success,
    };

    use crate::drivers::CapabilityDriver;
    use anyhow::Result;
    use axum::{Json, Router, routing::post};
    use serde_json::{Value, json};
    use std::collections::HashMap;
    use uuid::Uuid;

    #[test]
    fn parses_category_slots() {
        let config = vec![
            NzbgetConfigItem {
                name: "Category1.Name".to_string(),
                value: "movies".to_string(),
            },
            NzbgetConfigItem {
                name: "Category1.DestDir".to_string(),
                value: "/downloads/movies".to_string(),
            },
            NzbgetConfigItem {
                name: "Category2.Name".to_string(),
                value: "tv".to_string(),
            },
        ];
        let slots = category_slots(&config);
        assert_eq!(
            slots.get(&1).map(|slot| (&slot.name, &slot.dest_dir)),
            Some((&"movies".to_string(), &"/downloads/movies".to_string()))
        );
        assert_eq!(
            slots.get(&2),
            Some(&CategorySlot {
                name: "tv".to_string(),
                dest_dir: String::new(),
            })
        );
    }

    #[test]
    fn parses_category_option_names() {
        assert_eq!(parse_category_option("Category1.Name"), Some((1, "Name")));
        assert_eq!(
            parse_category_option("Category5.DestDir"),
            Some((5, "DestDir"))
        );
        assert_eq!(parse_category_option("MainDir"), None);
    }

    #[test]
    fn rpc_success_accepts_common_shapes() {
        assert!(rpc_success(&serde_json::json!(true)));
        assert!(rpc_success(&serde_json::json!(1)));
        assert!(rpc_success(&serde_json::json!("ok")));
        assert!(!rpc_success(&serde_json::json!(false)));
    }

    #[tokio::test]
    async fn read_state_reports_nzbget_live_telemetry() -> Result<()> {
        async fn rpc(Json(body): Json<Value>) -> Json<Value> {
            let method = body
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let result = match method {
                "version" => json!("24.3"),
                "status" => json!({
                    "DownloadRate": 3145728u64,
                    "DownloadedSizeLo": 268435456u64,
                    "DownloadedSizeHi": 0u64,
                    "PostJobCount": 2u64,
                    "ServerStandBy": false,
                    "DownloadPaused": false,
                    "PostPaused": false
                }),
                "listgroups" => json!([
                    { "Status": "DOWNLOADING" },
                    { "Status": "QUEUED" },
                    { "Status": "WARNING" }
                ]),
                other => json!({ "unexpected": other }),
            };
            Json(json!({
                "version": "1.1",
                "result": result,
                "error": Value::Null,
                "id": 1
            }))
        }

        let app = Router::new().route("/jsonrpc", post(rpc));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("mock nzbget server");
        });

        let endpoint = crate::orchestrator::model::ProviderEndpoint::new(
            "http".to_string(),
            "svc-nzbget-default".to_string(),
            addr.port(),
            None,
            Some("elixir_net".to_string()),
        )?;
        let ctx = DriverCtx::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "downloader.nzb".to_string(),
            endpoint,
            Some(format!("http://127.0.0.1:{}", addr.port())),
            Some("nzbget".to_string()),
            Some(json!({
                "username": "elixir",
                "password": "secret"
            })),
            HashMap::new(),
        );

        let snapshot = DownloaderNzbDriver::new().read_state(ctx).await?;
        let activity = snapshot.activity.expect("activity");
        assert_eq!(activity.status.as_deref(), Some("downloading"));
        assert_eq!(activity.download_rate_bps, Some(3_145_728));
        assert_eq!(activity.active_items, Some(1));
        assert_eq!(activity.queued_items, Some(1));
        assert_eq!(activity.error_items, Some(1));
        assert_eq!(activity.post_process_items, Some(2));
        assert_eq!(activity.downloaded_bytes, Some(268_435_456));
        assert!(activity.upload_rate_bps.is_none());
        assert_eq!(
            snapshot.summary.as_deref(),
            Some("NZBGet 24.3 · downloading · 1 issue")
        );
        Ok(())
    }
}
