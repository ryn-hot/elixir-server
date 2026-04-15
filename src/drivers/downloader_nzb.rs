use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use reqwest::header::{ACCEPT, HeaderMap, HeaderValue, USER_AGENT};
use reqwest::{Client, Method, StatusCode, Url};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::drivers::patches::{DownloadCategorySpec, DownloaderNzbPatch};
use crate::drivers::{
    ActivitySnapshot, ApplyResult, CapabilityDriver, DriverCtx, DriverPatch, StateSnapshot,
};
use tokio::fs;
use tokio::time::{Duration, sleep};

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
        let managed_config_path = config.managed_config_path();

        match patch {
            DownloaderNzbPatch::SetCategories { categories } => {
                if let Some(path) = managed_config_path.as_deref() {
                    upsert_categories_in_file(path, &categories).await?;
                } else {
                    let client = NzbgetClient::from_config(config, ctx.canonical_url()?).await?;
                    let preserved_server_updates = persisted_server_inventory_updates(&ctx, None)?;
                    let managed_path_updates = live_managed_path_updates(&ctx);
                    client
                        .upsert_categories(
                            &categories,
                            preserved_server_updates,
                            managed_path_updates,
                        )
                        .await?;
                }
            }
            DownloaderNzbPatch::SetPreferences {
                main_dir,
                default_save_path,
                incomplete_path,
                nzb_dir,
                queue_dir,
                temp_dir,
                script_dir,
                log_file,
                web_dir,
                config_template,
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
                let preserved_server_updates =
                    persisted_server_inventory_updates(&ctx, server_connections)?;
                let managed_path_updates = live_managed_path_updates(&ctx);
                let managed_category_updates = managed_default_category_updates(&ctx);
                if let Some(path) = managed_config_path.as_deref() {
                    set_preferences_in_file(
                        path,
                        main_dir,
                        default_save_path,
                        incomplete_path,
                        nzb_dir,
                        queue_dir,
                        temp_dir,
                        script_dir,
                        log_file,
                        web_dir,
                        config_template,
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
                        preserved_server_updates,
                    )
                    .await?;
                } else {
                    let client = NzbgetClient::from_config(config, ctx.canonical_url()?).await?;
                    client
                        .set_preferences(
                            main_dir,
                            default_save_path,
                            incomplete_path,
                            nzb_dir,
                            queue_dir,
                            temp_dir,
                            script_dir,
                            log_file,
                            web_dir,
                            config_template,
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
                            managed_path_updates,
                            managed_category_updates,
                            preserved_server_updates,
                        )
                        .await?;
                }
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
    #[serde(default)]
    runtime: NzbgetDriverRuntimeConfig,
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
            runtime: config.runtime,
        })
    }

    fn managed_config_path(&self) -> Option<PathBuf> {
        self.runtime
            .config_dir
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(PathBuf::from)
            .map(|path| path.join("nzbget.conf"))
    }
}

#[derive(Debug, Deserialize, Default)]
struct NzbgetDriverRuntimeConfig {
    #[serde(default)]
    config_dir: Option<String>,
}

struct NzbgetClient {
    client: Client,
    root: Url,
    username: String,
    password: String,
}

impl NzbgetClient {
    const READY_RETRY_ATTEMPTS: usize = 8;
    const READY_RETRY_DELAY_MS: u64 = 500;
    const CONFIG_CONVERGENCE_RETRY_ATTEMPTS: usize = 20;
    const CONFIG_CONVERGENCE_RETRY_DELAY_MS: u64 = 500;

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
        let _ = client
            .wait_until_ready(
                Self::READY_RETRY_ATTEMPTS,
                Duration::from_millis(Self::READY_RETRY_DELAY_MS),
            )
            .await?;
        Ok(client)
    }

    async fn wait_until_ready(&self, attempts: usize, delay: Duration) -> Result<String> {
        let attempts = attempts.max(1);
        let mut last_error = None;
        for attempt in 0..attempts {
            match self.version().await {
                Ok(version) => return Ok(version),
                Err(err) => {
                    let detail = err.to_string();
                    if detail.contains("auth rejected") {
                        return Err(err);
                    }
                    last_error = Some(err);
                }
            }
            if attempt + 1 < attempts {
                sleep(delay).await;
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("nzbget version")))
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
        let _ = self
            .wait_until_ready(10, Duration::from_millis(500))
            .await
            .context("waiting for nzbget to come back after reload")?;
        self.wait_until_config_reflects(
            &updates,
            Self::CONFIG_CONVERGENCE_RETRY_ATTEMPTS,
            Duration::from_millis(Self::CONFIG_CONVERGENCE_RETRY_DELAY_MS),
        )
        .await
        .context("waiting for nzbget config to reflect saved updates")?;
        Ok(())
    }

    async fn wait_until_config_reflects(
        &self,
        updates: &[NzbgetConfigUpdate],
        attempts: usize,
        delay: Duration,
    ) -> Result<()> {
        let expected = normalized_expected_updates(updates);
        if expected.is_empty() {
            return Ok(());
        }

        let attempts = attempts.max(1);
        let mut last_error = None;
        for attempt in 0..attempts {
            match self.read_config().await {
                Ok(config) => {
                    if config_reflects_updates(&config, &expected) {
                        return Ok(());
                    }
                    last_error = Some(anyhow!(
                        "nzbget config has not converged yet for keys: {}",
                        expected.keys().cloned().collect::<Vec<_>>().join(", ")
                    ));
                }
                Err(err) => {
                    let detail = err.to_string();
                    if detail.contains("auth rejected") {
                        return Err(err);
                    }
                    last_error = Some(err);
                }
            }
            if attempt + 1 < attempts {
                sleep(delay).await;
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow!("nzbget config did not converge after save")))
    }

    async fn set_preferences(
        &self,
        main_dir: Option<String>,
        default_save_path: Option<String>,
        incomplete_path: Option<String>,
        nzb_dir: Option<String>,
        queue_dir: Option<String>,
        temp_dir: Option<String>,
        script_dir: Option<String>,
        log_file: Option<String>,
        web_dir: Option<String>,
        config_template: Option<String>,
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
        managed_path_updates: Vec<NzbgetConfigUpdate>,
        managed_category_updates: Vec<NzbgetConfigUpdate>,
        preserved_server_updates: Vec<NzbgetConfigUpdate>,
    ) -> Result<()> {
        let config = self.read_config().await?;
        let mut updates = Vec::new();
        let uses_managed_main_dir = matches!(main_dir.as_deref(), Some("/config"));
        push_string_update(&mut updates, "MainDir", main_dir);
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
        push_string_update(&mut updates, "NzbDir", nzb_dir);
        push_string_update(&mut updates, "QueueDir", queue_dir);
        push_string_update(&mut updates, "TempDir", temp_dir);
        push_string_update(&mut updates, "ScriptDir", script_dir);
        push_string_update(&mut updates, "LogFile", log_file);
        push_string_update(&mut updates, "WebDir", web_dir);
        push_string_update(&mut updates, "ConfigTemplate", config_template);
        if uses_managed_main_dir {
            updates.push(NzbgetConfigUpdate::new("LockFile", "/config/nzbget.lock"));
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
        updates.extend(managed_path_updates);
        updates.extend(preserved_category_updates(&config));
        updates.extend(managed_category_updates);
        updates.extend(preserved_server_updates);
        self.save_config(updates).await
    }

    async fn upsert_categories(
        &self,
        categories: &[DownloadCategorySpec],
        preserved_server_updates: Vec<NzbgetConfigUpdate>,
        managed_path_updates: Vec<NzbgetConfigUpdate>,
    ) -> Result<()> {
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

        updates.extend(managed_path_updates);
        updates.extend(preserved_server_updates);
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

#[derive(Debug, Clone, Deserialize, Default)]
struct PersistedNzbgetServerEntry {
    slot: u32,
    #[serde(default)]
    name: String,
    #[serde(default)]
    host: String,
    #[serde(default)]
    port: Option<u16>,
    #[serde(default)]
    encryption: bool,
    #[serde(default)]
    active: bool,
    #[serde(default)]
    level: u64,
    #[serde(default)]
    connections: Option<u64>,
    #[serde(default)]
    cert_verification: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct PersistedNzbgetDriverInventoryConfig {
    #[serde(default)]
    server_inventory: Vec<PersistedNzbgetServerEntry>,
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

fn preserved_category_updates(config: &[NzbgetConfigItem]) -> Vec<NzbgetConfigUpdate> {
    let mut updates = Vec::new();
    for (slot, category) in category_slots(config) {
        if category.name.trim().is_empty() {
            continue;
        }
        updates.push(NzbgetConfigUpdate::new(
            format!("Category{slot}.Name"),
            category.name,
        ));
        if !category.dest_dir.trim().is_empty() {
            updates.push(NzbgetConfigUpdate::new(
                format!("Category{slot}.DestDir"),
                category.dest_dir,
            ));
        }
    }
    updates
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

fn persisted_server_inventory_updates(
    ctx: &DriverCtx,
    server_connections_override: Option<u64>,
) -> Result<Vec<NzbgetConfigUpdate>> {
    let Some(value) = ctx.instance_config.as_ref() else {
        return Ok(Vec::new());
    };
    let config: PersistedNzbgetDriverInventoryConfig =
        serde_json::from_value(value.clone()).context("parsing nzbget persisted inventory")?;
    let mut updates = Vec::new();
    for server in config.server_inventory {
        if !persisted_nzbget_server_is_configured(&server) {
            continue;
        }
        let username_key = format!("nzbget.server.{}.username", server.slot);
        let password_key = format!("nzbget.server.{}.password", server.slot);
        let username = ctx.secret(&username_key).ok_or_else(|| {
            anyhow!(
                "missing persisted nzbget server username secret for slot {}",
                server.slot
            )
        })?;
        let password = ctx.secret(&password_key).ok_or_else(|| {
            anyhow!(
                "missing persisted nzbget server password secret for slot {}",
                server.slot
            )
        })?;
        updates.extend(persisted_server_config_updates(
            &server,
            username,
            password,
            server_connections_override,
        ));
    }
    Ok(updates)
}

fn persisted_nzbget_server_is_configured(server: &PersistedNzbgetServerEntry) -> bool {
    server.slot > 0 && !server.host.trim().is_empty()
}

fn persisted_server_config_updates(
    server: &PersistedNzbgetServerEntry,
    username: &str,
    password: &str,
    server_connections_override: Option<u64>,
) -> Vec<NzbgetConfigUpdate> {
    let connections = server_connections_override
        .or(server.connections)
        .unwrap_or(20);
    let cert_verification = server
        .cert_verification
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("strict");
    vec![
        NzbgetConfigUpdate::new(
            format!("Server{}.Active", server.slot),
            if server.active { "yes" } else { "no" },
        ),
        NzbgetConfigUpdate::new(format!("Server{}.Name", server.slot), server.name.clone()),
        NzbgetConfigUpdate::new(
            format!("Server{}.Level", server.slot),
            server.level.to_string(),
        ),
        NzbgetConfigUpdate::new(format!("Server{}.Host", server.slot), server.host.clone()),
        NzbgetConfigUpdate::new(
            format!("Server{}.Encryption", server.slot),
            if server.encryption { "yes" } else { "no" },
        ),
        NzbgetConfigUpdate::new(
            format!("Server{}.Port", server.slot),
            server.port.unwrap_or(563).to_string(),
        ),
        NzbgetConfigUpdate::new(
            format!("Server{}.Username", server.slot),
            username.trim().to_string(),
        ),
        NzbgetConfigUpdate::new(
            format!("Server{}.Password", server.slot),
            password.trim().to_string(),
        ),
        NzbgetConfigUpdate::new(
            format!("Server{}.Connections", server.slot),
            connections.to_string(),
        ),
        NzbgetConfigUpdate::new(
            format!("Server{}.CertVerification", server.slot),
            cert_verification.to_string(),
        ),
    ]
}

fn live_managed_path_updates(ctx: &DriverCtx) -> Vec<NzbgetConfigUpdate> {
    if !nzbget_managed_defaults_enabled(ctx) {
        return Vec::new();
    }
    vec![
        NzbgetConfigUpdate::new("MainDir", "/config"),
        NzbgetConfigUpdate::new("DestDir", "/downloads"),
        NzbgetConfigUpdate::new("InterDir", "/runtime/incomplete"),
        NzbgetConfigUpdate::new("NzbDir", "/runtime/nzb"),
        NzbgetConfigUpdate::new("QueueDir", "/runtime/queue"),
        NzbgetConfigUpdate::new("TempDir", "/runtime/tmp"),
        NzbgetConfigUpdate::new("ScriptDir", "/config/scripts"),
        NzbgetConfigUpdate::new("LogFile", "/config/nzbget.log"),
        NzbgetConfigUpdate::new("WebDir", "/app/nzbget/webui"),
        NzbgetConfigUpdate::new("ConfigTemplate", "/app/nzbget/webui/nzbget.conf.template"),
        NzbgetConfigUpdate::new("LockFile", "/config/nzbget.lock"),
    ]
}

fn managed_default_category_updates(ctx: &DriverCtx) -> Vec<NzbgetConfigUpdate> {
    if !nzbget_managed_defaults_enabled(ctx) {
        return Vec::new();
    }
    vec![
        NzbgetConfigUpdate::new("Category1.Name", "tv"),
        NzbgetConfigUpdate::new("Category1.DestDir", "/downloads/tv"),
        NzbgetConfigUpdate::new("Category2.Name", "anime"),
        NzbgetConfigUpdate::new("Category2.DestDir", "/downloads/anime"),
        NzbgetConfigUpdate::new("Category3.Name", "movies"),
        NzbgetConfigUpdate::new("Category3.DestDir", "/downloads/movies"),
    ]
}

fn nzbget_managed_defaults_enabled(ctx: &DriverCtx) -> bool {
    ctx.instance_config
        .as_ref()
        .and_then(|value| value.get("managed_defaults"))
        .is_some()
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

fn normalized_expected_updates(updates: &[NzbgetConfigUpdate]) -> BTreeMap<String, String> {
    let mut expected = BTreeMap::new();
    for update in updates {
        if should_skip_config_convergence_check(&update.name) {
            continue;
        }
        expected.insert(update.name.clone(), update.value.clone());
    }
    expected
}

fn should_skip_config_convergence_check(name: &str) -> bool {
    name.eq_ignore_ascii_case("ControlPassword")
        || name.ends_with(".Password")
        || name.eq_ignore_ascii_case("ServerPassword")
}

fn config_reflects_updates(
    config: &[NzbgetConfigItem],
    expected: &BTreeMap<String, String>,
) -> bool {
    if expected.is_empty() {
        return true;
    }
    let mut actual = BTreeMap::new();
    for item in config {
        actual.insert(item.name.as_str(), item.value.as_str());
    }
    expected
        .iter()
        .all(|(name, value)| actual.get(name.as_str()).copied() == Some(value.as_str()))
}

fn parse_nzb_bool(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "yes" | "true" | "1" | "on"
    )
}

async fn upsert_categories_in_file(
    config_path: &Path,
    categories: &[DownloadCategorySpec],
) -> Result<()> {
    if categories.is_empty() {
        return Ok(());
    }
    let mut config = read_managed_config(config_path).await?;
    let mut slots = category_slots(&config.items);
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

    write_config_updates(&mut config, updates).await
}

#[allow(clippy::too_many_arguments)]
async fn set_preferences_in_file(
    config_path: &Path,
    main_dir: Option<String>,
    default_save_path: Option<String>,
    incomplete_path: Option<String>,
    nzb_dir: Option<String>,
    queue_dir: Option<String>,
    temp_dir: Option<String>,
    script_dir: Option<String>,
    log_file: Option<String>,
    web_dir: Option<String>,
    config_template: Option<String>,
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
    preserved_server_updates: Vec<NzbgetConfigUpdate>,
) -> Result<()> {
    let mut config = read_managed_config(config_path).await?;
    let main_dir = canonicalize_nzbget_managed_path("MainDir", main_dir);
    let nzb_dir = canonicalize_nzbget_managed_path("NzbDir", nzb_dir);
    let queue_dir = canonicalize_nzbget_managed_path("QueueDir", queue_dir);
    let temp_dir = canonicalize_nzbget_managed_path("TempDir", temp_dir);
    let script_dir = canonicalize_nzbget_managed_path("ScriptDir", script_dir);
    let log_file = canonicalize_nzbget_managed_path("LogFile", log_file);
    let web_dir = canonicalize_nzbget_managed_path("WebDir", web_dir);
    let config_template = canonicalize_nzbget_managed_path("ConfigTemplate", config_template);
    let mut updates = Vec::new();
    push_string_update(&mut updates, "MainDir", main_dir);
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
    push_string_update(&mut updates, "NzbDir", nzb_dir);
    push_string_update(&mut updates, "QueueDir", queue_dir);
    push_string_update(&mut updates, "TempDir", temp_dir);
    push_string_update(&mut updates, "ScriptDir", script_dir);
    push_string_update(&mut updates, "LogFile", log_file);
    push_string_update(&mut updates, "WebDir", web_dir);
    push_string_update(&mut updates, "ConfigTemplate", config_template);
    if let Some(connections) = server_connections {
        updates.extend(server_connection_updates(&config.items, connections));
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
    updates.push(NzbgetConfigUpdate::new("LockFile", "/config/nzbget.lock"));
    updates.extend(preserved_server_updates);

    write_config_updates(&mut config, updates).await
}

fn canonicalize_nzbget_managed_path(key: &str, value: Option<String>) -> Option<String> {
    match (key, value) {
        ("MainDir", Some(_)) => Some("/config".to_string()),
        ("NzbDir", Some(_)) => Some("/config/nzb".to_string()),
        ("QueueDir", Some(_)) => Some("/config/queue".to_string()),
        ("TempDir", Some(_)) => Some("/config/tmp".to_string()),
        ("ScriptDir", Some(_)) => Some("/config/scripts".to_string()),
        ("LogFile", Some(_)) => Some("/config/nzbget.log".to_string()),
        ("WebDir", Some(_)) => Some("/app/nzbget/webui".to_string()),
        ("ConfigTemplate", Some(_)) => Some("/app/nzbget/webui/nzbget.conf.template".to_string()),
        (_, other) => other,
    }
}

struct ManagedConfigFile {
    path: PathBuf,
    text: String,
    items: Vec<NzbgetConfigItem>,
}

async fn read_managed_config(config_path: &Path) -> Result<ManagedConfigFile> {
    let text = fs::read_to_string(config_path)
        .await
        .with_context(|| format!("reading nzbget config {}", config_path.display()))?;
    let items = parse_config_items(&text);
    Ok(ManagedConfigFile {
        path: config_path.to_path_buf(),
        text,
        items,
    })
}

async fn write_config_updates(
    config: &mut ManagedConfigFile,
    updates: Vec<NzbgetConfigUpdate>,
) -> Result<()> {
    if updates.is_empty() {
        return Ok(());
    }
    let rendered = apply_updates_to_config_text(&config.text, &updates);
    if rendered == config.text {
        return Ok(());
    }
    fs::write(&config.path, rendered.as_bytes())
        .await
        .with_context(|| format!("writing nzbget config {}", config.path.display()))?;
    config.text = rendered;
    config.items = parse_config_items(&config.text);
    Ok(())
}

fn parse_config_items(text: &str) -> Vec<NzbgetConfigItem> {
    text.lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                return None;
            }
            let (name, value) = trimmed.split_once('=')?;
            Some(NzbgetConfigItem {
                name: name.trim().to_string(),
                value: value.trim().to_string(),
            })
        })
        .collect()
}

fn apply_updates_to_config_text(text: &str, updates: &[NzbgetConfigUpdate]) -> String {
    let mut rendered = Vec::new();
    let mut seen = HashSet::new();
    let update_map = updates
        .iter()
        .map(|update| (update.name.as_str(), update.value.as_str()))
        .collect::<BTreeMap<_, _>>();

    for line in text.lines() {
        let trimmed = line.trim_start();
        if !trimmed.starts_with('#') {
            if let Some((name, _)) = line.split_once('=') {
                let key = name.trim();
                if let Some(value) = update_map.get(key) {
                    if seen.insert(key.to_string()) {
                        rendered.push(format!("{key}={value}"));
                    }
                    continue;
                }
            }
        }
        rendered.push(line.to_string());
    }

    for update in updates {
        if seen.insert(update.name.clone()) {
            rendered.push(format!("{}={}", update.name, update.value));
        }
    }

    let mut output = rendered.join("\n");
    output.push('\n');
    output
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
        CategorySlot, DownloaderNzbDriver, DriverCtx, NzbgetClient, NzbgetConfigItem,
        NzbgetDriverConfig, NzbgetDriverRuntimeConfig, category_slots, parse_category_option,
        rpc_success,
    };

    use crate::drivers::CapabilityDriver;
    use crate::drivers::patches::DownloadCategorySpec;
    use anyhow::Result;
    use axum::{Json, Router, response::IntoResponse, routing::post};
    use serde_json::{Value, json};
    use std::collections::HashMap;
    use std::fs;
    use std::sync::{Arc, Mutex};
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

    #[tokio::test]
    async fn apply_patch_updates_managed_config_file_for_local_nzbget() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let config_dir = temp.path().join("config");
        fs::create_dir_all(&config_dir)?;
        let config_path = config_dir.join("nzbget.conf");
        fs::write(
            &config_path,
            "# base\nDestDir=/downloads\nInterDir=/downloads/.incomplete\n",
        )?;

        let endpoint = crate::orchestrator::model::ProviderEndpoint::new(
            "http".to_string(),
            "elx-nzbget".to_string(),
            6789,
            None,
            Some("elixir_net".to_string()),
        )?;
        let mut secrets = HashMap::new();
        secrets.insert("nzbget.server.1.username".to_string(), "reader".to_string());
        secrets.insert(
            "nzbget.server.1.password".to_string(),
            "provider-secret".to_string(),
        );
        let ctx = DriverCtx::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "downloader.nzb".to_string(),
            endpoint,
            Some("http://127.0.0.1:6789".to_string()),
            Some("nzbget".to_string()),
            Some(json!({
                "username": "elixir",
                "password": "secret",
                "server_inventory": [{
                    "slot": 1,
                    "name": "XSNews",
                    "host": "news.xsnews.nl",
                    "port": 563,
                    "encryption": true,
                    "active": true,
                    "level": 0,
                    "connections": 20,
                    "cert_verification": "strict"
                }],
                "runtime": {
                    "config_dir": config_dir.to_string_lossy()
                }
            })),
            secrets,
        );

        let driver = DownloaderNzbDriver::new();
        driver
            .apply_patch(
                ctx.clone(),
                crate::drivers::DriverPatch::DownloaderNzb(
                    crate::drivers::DownloaderNzbPatch::SetPreferences {
                        main_dir: Some("/downloads".to_string()),
                        default_save_path: Some("/downloads".to_string()),
                        incomplete_path: Some("/downloads/.incomplete".to_string()),
                        nzb_dir: Some("/downloads/.nzb".to_string()),
                        queue_dir: Some("/downloads/.queue".to_string()),
                        temp_dir: Some("/downloads/.tmp".to_string()),
                        script_dir: Some("/config/scripts".to_string()),
                        log_file: Some("/config/nzbget.log".to_string()),
                        web_dir: Some("/app/nzbget/webui".to_string()),
                        config_template: Some("/app/nzbget/webui/nzbget.conf.template".to_string()),
                        use_incomplete: Some(true),
                        server_connections: None,
                        article_retries: Some(3),
                        article_timeout_seconds: Some(60),
                        article_cache_mb: Some(200),
                        direct_write: Some(true),
                        write_buffer_kb: Some(1024),
                        continue_partial: Some(true),
                        par_check: Some("auto".to_string()),
                        par_scan: Some("auto".to_string()),
                        par_quick: Some(true),
                        par_repair: Some(true),
                        par_rename: Some(true),
                        par_pause_queue: Some(true),
                        par_threads: Some(2),
                        unpack: Some(true),
                        unpack_pause_queue: Some(true),
                        download_rate_kib: Some(0),
                    },
                ),
            )
            .await?;
        driver
            .apply_patch(
                ctx,
                crate::drivers::DriverPatch::DownloaderNzb(
                    crate::drivers::DownloaderNzbPatch::SetCategories {
                        categories: vec![
                            DownloadCategorySpec {
                                name: "tv".to_string(),
                                save_path: Some("/downloads/tv".to_string()),
                            },
                            DownloadCategorySpec {
                                name: "movies".to_string(),
                                save_path: Some("/downloads/movies".to_string()),
                            },
                        ],
                    },
                ),
            )
            .await?;

        let rendered = fs::read_to_string(config_path)?;
        assert!(rendered.contains("MainDir=/config"));
        assert!(rendered.contains("DestDir=/downloads"));
        assert!(rendered.contains("InterDir=/downloads/.incomplete"));
        assert!(rendered.contains("NzbDir=/config/nzb"));
        assert!(rendered.contains("QueueDir=/config/queue"));
        assert!(rendered.contains("TempDir=/config/tmp"));
        assert!(rendered.contains("ScriptDir=/config/scripts"));
        assert!(rendered.contains("LogFile=/config/nzbget.log"));
        assert!(rendered.contains("WebDir=/app/nzbget/webui"));
        assert!(rendered.contains("ConfigTemplate=/app/nzbget/webui/nzbget.conf.template"));
        assert!(rendered.contains("LockFile=/config/nzbget.lock"));
        assert!(rendered.contains("Category1.Name=tv"));
        assert!(rendered.contains("Category1.DestDir=/downloads/tv"));
        assert!(rendered.contains("Category2.Name=movies"));
        assert!(rendered.contains("Category2.DestDir=/downloads/movies"));
        assert!(rendered.contains("Server1.Active=yes"));
        assert!(rendered.contains("Server1.Name=XSNews"));
        assert!(rendered.contains("Server1.Level=0"));
        assert!(rendered.contains("Server1.Host=news.xsnews.nl"));
        assert!(rendered.contains("Server1.Encryption=yes"));
        assert!(rendered.contains("Server1.Port=563"));
        assert!(rendered.contains("Server1.Username=reader"));
        assert!(rendered.contains("Server1.Password=provider-secret"));
        Ok(())
    }

    #[tokio::test]
    async fn from_config_retries_nzbget_version_after_reload_gap() -> Result<()> {
        #[derive(Clone, Default)]
        struct RpcState {
            version_failures_remaining: Arc<Mutex<usize>>,
        }

        async fn rpc(
            axum::extract::State(state): axum::extract::State<RpcState>,
            Json(body): Json<Value>,
        ) -> impl axum::response::IntoResponse {
            let method = body
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if method == "version" {
                let mut failures = state.version_failures_remaining.lock().unwrap();
                if *failures > 0 {
                    *failures -= 1;
                    return (
                        axum::http::StatusCode::SERVICE_UNAVAILABLE,
                        Json(json!({ "error": "reloading" })),
                    )
                        .into_response();
                }
            }
            (
                axum::http::StatusCode::OK,
                Json(json!({
                    "version": "1.1",
                    "result": match method {
                        "version" => json!("26.1"),
                        _ => Value::Null,
                    },
                    "error": Value::Null,
                    "id": 1
                })),
            )
                .into_response()
        }

        let state = RpcState {
            version_failures_remaining: Arc::new(Mutex::new(2)),
        };
        let app = Router::new()
            .route("/jsonrpc", post(rpc))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("mock nzbget server");
        });

        let client: NzbgetClient = NzbgetClient::from_config(
            NzbgetDriverConfig {
                username: Some("elixir".to_string()),
                password: Some("secret".to_string()),
                base_url: None,
                runtime: NzbgetDriverRuntimeConfig::default(),
            },
            format!("http://127.0.0.1:{}", addr.port()),
        )
        .await?;

        assert_eq!(client.version().await?, "26.1");
        assert_eq!(
            *state.version_failures_remaining.lock().unwrap(),
            0,
            "expected readiness retries to absorb the reload gap"
        );
        Ok(())
    }

    #[tokio::test]
    async fn apply_patch_preserves_persisted_server_inventory_for_live_nzbget() -> Result<()> {
        #[derive(Clone, Default)]
        struct RpcState {
            saved_updates: Arc<Mutex<Vec<(String, String)>>>,
            config_items: Arc<Mutex<Vec<(String, String)>>>,
        }

        async fn rpc(
            axum::extract::State(state): axum::extract::State<RpcState>,
            Json(body): Json<Value>,
        ) -> impl axum::response::IntoResponse {
            let method = body
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if method == "saveconfig" {
                let updates = body
                    .get("params")
                    .and_then(Value::as_array)
                    .and_then(|params| params.first())
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                let mut saved = state.saved_updates.lock().unwrap();
                saved.clear();
                for update in updates {
                    let Some(name) = update.get("Name").and_then(Value::as_str) else {
                        continue;
                    };
                    let Some(value) = update.get("Value").and_then(Value::as_str) else {
                        continue;
                    };
                    saved.push((name.to_string(), value.to_string()));
                }
                *state.config_items.lock().unwrap() = saved.clone();
            }

            let result = match method {
                "version" => json!("26.1"),
                "config" => {
                    let items = state
                        .config_items
                        .lock()
                        .unwrap()
                        .iter()
                        .map(|(name, value)| json!({ "Name": name, "Value": value }))
                        .collect::<Vec<_>>();
                    json!(items)
                }
                "saveconfig" | "reload" => json!(true),
                other => panic!("unexpected nzbget rpc method {other}"),
            };
            (
                axum::http::StatusCode::OK,
                Json(json!({
                    "version": "1.1",
                    "result": result,
                    "error": Value::Null,
                    "id": 1
                })),
            )
                .into_response()
        }

        let state = RpcState {
            saved_updates: Arc::new(Mutex::new(Vec::new())),
            config_items: Arc::new(Mutex::new(vec![
                ("Category1.Name".to_string(), "tv".to_string()),
                ("Category1.DestDir".to_string(), "/downloads/tv".to_string()),
            ])),
        };
        let app = Router::new()
            .route("/jsonrpc", post(rpc))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("mock nzbget server");
        });

        let endpoint = crate::orchestrator::model::ProviderEndpoint::new(
            "http".to_string(),
            "elx-nzbget".to_string(),
            6789,
            None,
            Some("elixir_net".to_string()),
        )?;
        let mut secrets = HashMap::new();
        secrets.insert("nzbget.server.1.username".to_string(), "reader".to_string());
        secrets.insert(
            "nzbget.server.1.password".to_string(),
            "provider-secret".to_string(),
        );
        let ctx = DriverCtx::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "downloader.nzb".to_string(),
            endpoint,
            Some(format!("http://127.0.0.1:{}", addr.port())),
            Some("nzbget".to_string()),
            Some(json!({
                "username": "elixir",
                "password": "secret",
                "server_inventory": [{
                    "slot": 1,
                    "name": "XSNews",
                    "host": "news.xsnews.nl",
                    "port": 563,
                    "encryption": true,
                    "active": true,
                    "level": 0,
                    "connections": 20,
                    "cert_verification": "strict"
                }]
            })),
            secrets,
        );

        let driver = DownloaderNzbDriver::new();
        driver
            .apply_patch(
                ctx,
                crate::drivers::DriverPatch::DownloaderNzb(
                    crate::drivers::DownloaderNzbPatch::SetPreferences {
                        main_dir: Some("/config".to_string()),
                        default_save_path: Some("/downloads".to_string()),
                        incomplete_path: Some("/runtime/incomplete".to_string()),
                        nzb_dir: Some("/runtime/nzb".to_string()),
                        queue_dir: Some("/runtime/queue".to_string()),
                        temp_dir: Some("/runtime/tmp".to_string()),
                        script_dir: Some("/config/scripts".to_string()),
                        log_file: Some("/config/nzbget.log".to_string()),
                        web_dir: Some("/app/nzbget/webui".to_string()),
                        config_template: Some("/app/nzbget/webui/nzbget.conf.template".to_string()),
                        use_incomplete: Some(true),
                        server_connections: Some(32),
                        article_retries: Some(4),
                        article_timeout_seconds: Some(60),
                        article_cache_mb: Some(384),
                        direct_write: Some(true),
                        write_buffer_kb: Some(2048),
                        continue_partial: Some(true),
                        par_check: Some("auto".to_string()),
                        par_scan: Some("auto".to_string()),
                        par_quick: Some(true),
                        par_repair: Some(true),
                        par_rename: Some(true),
                        par_pause_queue: Some(true),
                        par_threads: Some(4),
                        unpack: Some(true),
                        unpack_pause_queue: Some(true),
                        download_rate_kib: Some(0),
                    },
                ),
            )
            .await?;

        let saved = state.saved_updates.lock().unwrap().clone();
        assert!(saved.contains(&("Server1.Active".to_string(), "yes".to_string())));
        assert!(saved.contains(&("Server1.Name".to_string(), "XSNews".to_string())));
        assert!(saved.contains(&("Server1.Host".to_string(), "news.xsnews.nl".to_string())));
        assert!(saved.contains(&("Server1.Port".to_string(), "563".to_string())));
        assert!(saved.contains(&("Server1.Username".to_string(), "reader".to_string())));
        assert!(saved.contains(&(
            "Server1.Password".to_string(),
            "provider-secret".to_string()
        )));
        assert!(saved.contains(&("Server1.Connections".to_string(), "32".to_string())));
        assert!(saved.contains(&("Category1.Name".to_string(), "tv".to_string())));
        assert!(saved.contains(&("Category1.DestDir".to_string(), "/downloads/tv".to_string())));
        Ok(())
    }

    #[tokio::test]
    async fn apply_patch_set_categories_preserves_managed_paths_and_server_inventory() -> Result<()>
    {
        #[derive(Clone, Default)]
        struct RpcState {
            saved_updates: Arc<Mutex<Vec<(String, String)>>>,
            config_items: Arc<Mutex<Vec<(String, String)>>>,
        }

        async fn rpc(
            axum::extract::State(state): axum::extract::State<RpcState>,
            Json(body): Json<Value>,
        ) -> impl axum::response::IntoResponse {
            let method = body
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if method == "saveconfig" {
                let updates = body
                    .get("params")
                    .and_then(Value::as_array)
                    .and_then(|params| params.first())
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                let mut saved = state.saved_updates.lock().unwrap();
                saved.clear();
                for update in updates {
                    let Some(name) = update.get("Name").and_then(Value::as_str) else {
                        continue;
                    };
                    let Some(value) = update.get("Value").and_then(Value::as_str) else {
                        continue;
                    };
                    saved.push((name.to_string(), value.to_string()));
                }
                *state.config_items.lock().unwrap() = saved.clone();
            }

            let result = match method {
                "version" => json!("26.1"),
                "config" => {
                    let items = state
                        .config_items
                        .lock()
                        .unwrap()
                        .iter()
                        .map(|(name, value)| json!({ "Name": name, "Value": value }))
                        .collect::<Vec<_>>();
                    json!(items)
                }
                "saveconfig" | "reload" => json!(true),
                other => panic!("unexpected nzbget rpc method {other}"),
            };
            (
                axum::http::StatusCode::OK,
                Json(json!({
                    "version": "1.1",
                    "result": result,
                    "error": Value::Null,
                    "id": 1
                })),
            )
                .into_response()
        }

        let state = RpcState {
            saved_updates: Arc::new(Mutex::new(Vec::new())),
            config_items: Arc::new(Mutex::new(vec![
                ("MainDir".to_string(), "/root/downloads".to_string()),
                ("DestDir".to_string(), "/root/downloads/dst".to_string()),
                ("NzbDir".to_string(), "/root/downloads/nzb".to_string()),
                ("QueueDir".to_string(), "/root/downloads/queue".to_string()),
                ("TempDir".to_string(), "/root/downloads/tmp".to_string()),
                (
                    "ScriptDir".to_string(),
                    "/root/downloads/scripts".to_string(),
                ),
                (
                    "LogFile".to_string(),
                    "/root/downloads/nzbget.log".to_string(),
                ),
                (
                    "LockFile".to_string(),
                    "/root/downloads/nzbget.lock".to_string(),
                ),
            ])),
        };
        let app = Router::new()
            .route("/jsonrpc", post(rpc))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("mock nzbget server");
        });

        let endpoint = crate::orchestrator::model::ProviderEndpoint::new(
            "http".to_string(),
            "elx-nzbget".to_string(),
            6789,
            None,
            Some("elixir_net".to_string()),
        )?;
        let mut secrets = HashMap::new();
        secrets.insert("nzbget.server.1.username".to_string(), "reader".to_string());
        secrets.insert(
            "nzbget.server.1.password".to_string(),
            "provider-secret".to_string(),
        );
        let ctx = DriverCtx::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "downloader.nzb".to_string(),
            endpoint,
            Some(format!("http://127.0.0.1:{}", addr.port())),
            Some("nzbget".to_string()),
            Some(json!({
                "username": "elixir",
                "password": "secret",
                "managed_defaults": {
                    "nzbget_performance_profile_version": "aggressive-v4"
                },
                "server_inventory": [{
                    "slot": 1,
                    "name": "XSNews",
                    "host": "news.xsnews.nl",
                    "port": 563,
                    "encryption": true,
                    "active": true,
                    "level": 0,
                    "connections": 20,
                    "cert_verification": "strict"
                }]
            })),
            secrets,
        );

        let driver = DownloaderNzbDriver::new();
        driver
            .apply_patch(
                ctx,
                crate::drivers::DriverPatch::DownloaderNzb(
                    crate::drivers::DownloaderNzbPatch::SetCategories {
                        categories: vec![
                            DownloadCategorySpec {
                                name: "tv".to_string(),
                                save_path: Some("/downloads/tv".to_string()),
                            },
                            DownloadCategorySpec {
                                name: "movies".to_string(),
                                save_path: Some("/downloads/movies".to_string()),
                            },
                        ],
                    },
                ),
            )
            .await?;

        let saved = state.saved_updates.lock().unwrap().clone();
        assert!(saved.contains(&("MainDir".to_string(), "/config".to_string())));
        assert!(saved.contains(&("DestDir".to_string(), "/downloads".to_string())));
        assert!(saved.contains(&("LockFile".to_string(), "/config/nzbget.lock".to_string())));
        assert!(saved.contains(&("Category1.Name".to_string(), "tv".to_string())));
        assert!(saved.contains(&("Category1.DestDir".to_string(), "/downloads/tv".to_string())));
        assert!(saved.contains(&("Category2.Name".to_string(), "movies".to_string())));
        assert!(saved.contains(&(
            "Category2.DestDir".to_string(),
            "/downloads/movies".to_string()
        )));
        assert!(saved.contains(&("Server1.Host".to_string(), "news.xsnews.nl".to_string())));
        assert!(saved.contains(&("Server1.Username".to_string(), "reader".to_string())));
        Ok(())
    }

    #[tokio::test]
    async fn apply_patch_waits_for_live_config_convergence_after_reload() -> Result<()> {
        #[derive(Clone, Default)]
        struct RpcState {
            saved_updates: Arc<Mutex<Vec<(String, String)>>>,
            current_config_items: Arc<Mutex<Vec<(String, String)>>>,
            pending_config_items: Arc<Mutex<Vec<(String, String)>>>,
            stale_config_polls_remaining: Arc<Mutex<usize>>,
        }

        async fn rpc(
            axum::extract::State(state): axum::extract::State<RpcState>,
            Json(body): Json<Value>,
        ) -> impl axum::response::IntoResponse {
            let method = body
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if method == "saveconfig" {
                let updates = body
                    .get("params")
                    .and_then(Value::as_array)
                    .and_then(|params| params.first())
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                let mut saved = state.saved_updates.lock().unwrap();
                saved.clear();
                for update in updates {
                    let Some(name) = update.get("Name").and_then(Value::as_str) else {
                        continue;
                    };
                    let Some(value) = update.get("Value").and_then(Value::as_str) else {
                        continue;
                    };
                    saved.push((name.to_string(), value.to_string()));
                }
                *state.pending_config_items.lock().unwrap() = saved.clone();
                *state.stale_config_polls_remaining.lock().unwrap() = 2;
            }

            let result = match method {
                "version" => json!("26.1"),
                "config" => {
                    let mut remaining = state.stale_config_polls_remaining.lock().unwrap();
                    if *remaining == 0 {
                        let pending = state.pending_config_items.lock().unwrap().clone();
                        *state.current_config_items.lock().unwrap() = pending;
                    } else {
                        *remaining -= 1;
                    }
                    let items = state
                        .current_config_items
                        .lock()
                        .unwrap()
                        .iter()
                        .map(|(name, value)| json!({ "Name": name, "Value": value }))
                        .collect::<Vec<_>>();
                    json!(items)
                }
                "saveconfig" | "reload" => json!(true),
                other => panic!("unexpected nzbget rpc method {other}"),
            };
            (
                axum::http::StatusCode::OK,
                Json(json!({
                    "version": "1.1",
                    "result": result,
                    "error": Value::Null,
                    "id": 1
                })),
            )
                .into_response()
        }

        let state = RpcState {
            saved_updates: Arc::new(Mutex::new(Vec::new())),
            current_config_items: Arc::new(Mutex::new(vec![
                ("MainDir".to_string(), "/config".to_string()),
                ("DestDir".to_string(), "/downloads".to_string()),
                ("InterDir".to_string(), "/runtime/incomplete".to_string()),
                ("NzbDir".to_string(), "/runtime/nzb".to_string()),
                ("QueueDir".to_string(), "/runtime/queue".to_string()),
                ("TempDir".to_string(), "/runtime/tmp".to_string()),
                ("Category1.Name".to_string(), "old-tv".to_string()),
                (
                    "Category1.DestDir".to_string(),
                    "/downloads/old-tv".to_string(),
                ),
            ])),
            pending_config_items: Arc::new(Mutex::new(Vec::new())),
            stale_config_polls_remaining: Arc::new(Mutex::new(0)),
        };
        let app = Router::new()
            .route("/jsonrpc", post(rpc))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("mock nzbget server");
        });

        let endpoint = crate::orchestrator::model::ProviderEndpoint::new(
            "http".to_string(),
            "elx-nzbget".to_string(),
            6789,
            None,
            Some("elixir_net".to_string()),
        )?;
        let mut secrets = HashMap::new();
        secrets.insert("nzbget.server.1.username".to_string(), "reader".to_string());
        secrets.insert(
            "nzbget.server.1.password".to_string(),
            "provider-secret".to_string(),
        );
        let ctx = DriverCtx::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "downloader.nzb".to_string(),
            endpoint,
            Some(format!("http://127.0.0.1:{}", addr.port())),
            Some("nzbget".to_string()),
            Some(json!({
                "username": "elixir",
                "password": "secret",
                "managed_defaults": {
                    "nzbget_performance_profile_version": "aggressive-v4"
                },
                "server_inventory": [{
                    "slot": 1,
                    "name": "XSNews",
                    "host": "news.xsnews.nl",
                    "port": 563,
                    "encryption": true,
                    "active": true,
                    "level": 0,
                    "connections": 20,
                    "cert_verification": "strict"
                }]
            })),
            secrets,
        );

        let driver = DownloaderNzbDriver::new();
        driver
            .apply_patch(
                ctx,
                crate::drivers::DriverPatch::DownloaderNzb(
                    crate::drivers::DownloaderNzbPatch::SetCategories {
                        categories: vec![
                            DownloadCategorySpec {
                                name: "tv".to_string(),
                                save_path: Some("/downloads/tv".to_string()),
                            },
                            DownloadCategorySpec {
                                name: "anime".to_string(),
                                save_path: Some("/downloads/anime".to_string()),
                            },
                        ],
                    },
                ),
            )
            .await?;

        assert_eq!(
            *state.stale_config_polls_remaining.lock().unwrap(),
            0,
            "expected config convergence polling to wait through stale config reads"
        );
        let current = state.current_config_items.lock().unwrap().clone();
        assert!(current.contains(&("Category1.Name".to_string(), "tv".to_string())));
        assert!(current.contains(&("Category1.DestDir".to_string(), "/downloads/tv".to_string())));
        assert!(current.contains(&("Category2.Name".to_string(), "anime".to_string())));
        assert!(current.contains(&(
            "Category2.DestDir".to_string(),
            "/downloads/anime".to_string()
        )));
        Ok(())
    }

    #[tokio::test]
    async fn apply_patch_set_preferences_preserves_managed_defaults_on_live_nzbget() -> Result<()> {
        #[derive(Clone, Default)]
        struct RpcState {
            saved_updates: Arc<Mutex<Vec<(String, String)>>>,
            config_items: Arc<Mutex<Vec<(String, String)>>>,
        }

        async fn rpc(
            axum::extract::State(state): axum::extract::State<RpcState>,
            Json(body): Json<Value>,
        ) -> impl axum::response::IntoResponse {
            let method = body
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if method == "saveconfig" {
                let updates = body
                    .get("params")
                    .and_then(Value::as_array)
                    .and_then(|params| params.first())
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                let mut saved = state.saved_updates.lock().unwrap();
                saved.clear();
                for update in updates {
                    let Some(name) = update.get("Name").and_then(Value::as_str) else {
                        continue;
                    };
                    let Some(value) = update.get("Value").and_then(Value::as_str) else {
                        continue;
                    };
                    saved.push((name.to_string(), value.to_string()));
                }
                *state.config_items.lock().unwrap() = saved.clone();
            }

            let result = match method {
                "version" => json!("26.1"),
                "config" => {
                    let items = state
                        .config_items
                        .lock()
                        .unwrap()
                        .iter()
                        .map(|(name, value)| json!({ "Name": name, "Value": value }))
                        .collect::<Vec<_>>();
                    json!(items)
                }
                "saveconfig" | "reload" => json!(true),
                other => panic!("unexpected nzbget rpc method {other}"),
            };
            (
                axum::http::StatusCode::OK,
                Json(json!({
                    "version": "1.1",
                    "result": result,
                    "error": Value::Null,
                    "id": 1
                })),
            )
                .into_response()
        }

        let state = RpcState {
            saved_updates: Arc::new(Mutex::new(Vec::new())),
            config_items: Arc::new(Mutex::new(vec![
                ("DestDir".to_string(), "/downloads".to_string()),
                ("InterDir".to_string(), "/runtime/incomplete".to_string()),
            ])),
        };
        let app = Router::new()
            .route("/jsonrpc", post(rpc))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("mock nzbget server");
        });

        let endpoint = crate::orchestrator::model::ProviderEndpoint::new(
            "http".to_string(),
            "elx-nzbget".to_string(),
            6789,
            None,
            Some("elixir_net".to_string()),
        )?;
        let mut secrets = HashMap::new();
        secrets.insert("nzbget.server.1.username".to_string(), "reader".to_string());
        secrets.insert(
            "nzbget.server.1.password".to_string(),
            "provider-secret".to_string(),
        );
        let ctx = DriverCtx::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "downloader.nzb".to_string(),
            endpoint,
            Some(format!("http://127.0.0.1:{}", addr.port())),
            Some("nzbget".to_string()),
            Some(json!({
                "username": "elixir",
                "password": "secret",
                "managed_defaults": {
                    "nzbget_performance_profile_version": "aggressive-v4"
                },
                "server_inventory": [{
                    "slot": 1,
                    "name": "XSNews",
                    "host": "news.xsnews.nl",
                    "port": 563,
                    "encryption": true,
                    "active": true,
                    "level": 0,
                    "connections": 20,
                    "cert_verification": "strict"
                }]
            })),
            secrets,
        );

        let driver = DownloaderNzbDriver::new();
        driver
            .apply_patch(
                ctx,
                crate::drivers::DriverPatch::DownloaderNzb(
                    crate::drivers::DownloaderNzbPatch::SetPreferences {
                        main_dir: None,
                        default_save_path: Some("/downloads".to_string()),
                        incomplete_path: Some("/runtime/incomplete".to_string()),
                        nzb_dir: Some("/runtime/nzb".to_string()),
                        queue_dir: Some("/runtime/queue".to_string()),
                        temp_dir: Some("/runtime/tmp".to_string()),
                        script_dir: None,
                        log_file: None,
                        web_dir: None,
                        config_template: None,
                        use_incomplete: Some(true),
                        server_connections: Some(32),
                        article_retries: None,
                        article_timeout_seconds: None,
                        article_cache_mb: None,
                        direct_write: None,
                        write_buffer_kb: None,
                        continue_partial: None,
                        par_check: None,
                        par_scan: None,
                        par_quick: None,
                        par_repair: None,
                        par_rename: None,
                        par_pause_queue: None,
                        par_threads: None,
                        unpack: None,
                        unpack_pause_queue: None,
                        download_rate_kib: None,
                    },
                ),
            )
            .await?;

        let saved = state.saved_updates.lock().unwrap().clone();
        assert!(saved.contains(&("MainDir".to_string(), "/config".to_string())));
        assert!(saved.contains(&("ScriptDir".to_string(), "/config/scripts".to_string())));
        assert!(saved.contains(&("LogFile".to_string(), "/config/nzbget.log".to_string())));
        assert!(saved.contains(&("LockFile".to_string(), "/config/nzbget.lock".to_string())));
        assert!(saved.contains(&("Category1.Name".to_string(), "tv".to_string())));
        assert!(saved.contains(&("Category2.Name".to_string(), "anime".to_string())));
        assert!(saved.contains(&("Category3.Name".to_string(), "movies".to_string())));
        Ok(())
    }
}
