use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use reqwest::{Client, StatusCode, Url};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{Value, json};
use tracing::warn;

use crate::drivers::{ApplyResult, CapabilityDriver, DriverCtx, DriverPatch, StateSnapshot};
use crate::drivers::patches::{
    CustomFormatSpec, DownloaderSpec, IndexerSpec, LanguageProfileSpec, MediaManagerTvPatch,
    QualityProfileSpec, ReleaseProfileSpec, RootFolderSpec, SeriesTypeDefaultsSpec, WebhookSpec,
};

#[derive(Debug, Default)]
pub struct MediaManagerTvDriver;

impl MediaManagerTvDriver {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl CapabilityDriver for MediaManagerTvDriver {
    fn capability(&self) -> &'static str {
        "media.manager.tv"
    }

    async fn read_state(&self, _ctx: DriverCtx) -> Result<StateSnapshot> {
        Ok(StateSnapshot {
            summary: Some("media.manager.tv driver is not implemented".to_string()),
        })
    }

    async fn apply_patch(&self, ctx: DriverCtx, patch: DriverPatch) -> Result<ApplyResult> {
        let patch = match patch {
            DriverPatch::MediaManagerTv(patch) => patch,
        };
        let implementation = ctx
            .implementation
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("provider implementation is required"))?;
        if implementation != "sonarr" {
            bail!(
                "media.manager.tv implementation '{}' is not supported",
                implementation
            );
        }

        patch.validate()?;

        let config = SonarrDriverConfig::from_ctx(&ctx)?;
        let client = SonarrClient::from_config(config, ctx.canonical_url()?).await?;

        match patch {
            MediaManagerTvPatch::SetIndexerRegistry { indexers } => {
                client.upsert_indexers(&indexers).await?;
            }
            MediaManagerTvPatch::SetDownloaders { downloaders } => {
                client.upsert_downloaders(&downloaders).await?;
            }
            MediaManagerTvPatch::SetRootFolders { roots } => {
                client.ensure_root_folders(&roots).await?;
            }
            MediaManagerTvPatch::SetQualityProfiles { profiles } => {
                client.upsert_quality_profiles(&profiles).await?;
            }
            MediaManagerTvPatch::SetLanguageProfiles { profiles } => {
                client.upsert_language_profiles(&profiles).await?;
            }
            MediaManagerTvPatch::SetSeriesTypeDefaults { defaults } => {
                client.apply_series_defaults(&defaults).await?;
            }
            MediaManagerTvPatch::SetTags { tags } => {
                let _ = client.ensure_tags(&tags).await?;
            }
            MediaManagerTvPatch::AssignTags { series_ids, tags } => {
                client.assign_tags(&series_ids, &tags).await?;
            }
            MediaManagerTvPatch::SetWebhooks { webhooks } => {
                client.upsert_webhooks(&webhooks).await?;
            }
            MediaManagerTvPatch::SetCustomFormats {
                formats,
                release_profiles,
            } => {
                client.upsert_custom_formats(&formats).await?;
                client.upsert_release_profiles(&release_profiles).await?;
                client
                    .apply_custom_format_scores(&formats)
                    .await?;
            }
            MediaManagerTvPatch::SetAuxServiceEndpoint { url } => {
                bail!("aux service endpoint is not supported for Sonarr ({})", url);
            }
        }

        Ok(ApplyResult::applied())
    }
}

#[derive(Debug, Deserialize, Default)]
struct SonarrDriverConfig {
    #[serde(default)]
    api_key: Option<String>,
    #[serde(default)]
    base_url: Option<String>,
    #[serde(default)]
    api_version: Option<String>,
}

impl SonarrDriverConfig {
    fn from_ctx(ctx: &DriverCtx) -> Result<Self> {
        let config = if let Some(raw) = ctx.instance_config.as_ref() {
            serde_json::from_value(raw.clone()).context("parsing Sonarr driver config")?
        } else {
            SonarrDriverConfig::default()
        };
        let api_key = config
            .api_key
            .clone()
            .or_else(|| ctx.secret("sonarr_api_key").map(str::to_string))
            .or_else(|| ctx.secret("api_key").map(str::to_string))
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| anyhow::anyhow!("sonarr api_key is required"))?;
        Ok(SonarrDriverConfig {
            api_key: Some(api_key),
            base_url: config.base_url,
            api_version: config.api_version,
        })
    }
}

struct SonarrClient {
    client: Client,
    root: Url,
    api_base: Url,
    api_key: String,
}

impl SonarrClient {
    async fn from_config(config: SonarrDriverConfig, endpoint_url: String) -> Result<Self> {
        let base_url = config.base_url.unwrap_or(endpoint_url);
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .build()
            .context("building sonarr http client")?;
        let root = normalize_root_url(&base_url)?;
        let mut client = Self {
            client,
            root,
            api_base: Url::parse("http://127.0.0.1/").expect("fallback url"),
            api_key: config
                .api_key
                .ok_or_else(|| anyhow::anyhow!("sonarr api_key is required"))?,
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
        bail!("sonarr api version could not be detected");
    }

    async fn probe_api(&self, version: &str) -> Result<bool> {
        let url = build_api_url(&self.root, version, "system/status")?;
        let resp = self
            .client
            .get(url)
            .header("X-Api-Key", &self.api_key)
            .send()
            .await
            .context("probing sonarr api")?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(false);
        }
        if resp.status() == StatusCode::UNAUTHORIZED {
            bail!("sonarr api key is invalid");
        }
        if !resp.status().is_success() {
            bail!("sonarr api probe failed with {}", resp.status());
        }
        Ok(true)
    }

    fn set_api_version(&mut self, version: &str) -> Result<()> {
        let version = normalize_version(version)?;
        self.api_base = build_api_base(&self.root, version)?;
        Ok(())
    }

    fn api_url(&self, path: &str) -> Result<Url> {
        let trimmed = path.trim_start_matches('/');
        self.api_base
            .join(trimmed)
            .context("building sonarr api url")
    }

    async fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let url = self.api_url(path)?;
        let resp = self
            .client
            .get(url)
            .header("X-Api-Key", &self.api_key)
            .send()
            .await
            .with_context(|| format!("GET {path}"))?;
        let resp = resp
            .error_for_status()
            .with_context(|| format!("GET {path} failed"))?;
        resp.json::<T>()
            .await
            .with_context(|| format!("parsing GET {path} response"))
    }

    async fn post_json(&self, path: &str, body: &Value) -> Result<Value> {
        let url = self.api_url(path)?;
        let resp = self
            .client
            .post(url)
            .header("X-Api-Key", &self.api_key)
            .json(body)
            .send()
            .await
            .with_context(|| format!("POST {path}"))?;
        let resp = resp
            .error_for_status()
            .with_context(|| format!("POST {path} failed"))?;
        resp.json::<Value>()
            .await
            .with_context(|| format!("parsing POST {path} response"))
    }

    async fn put_json(&self, path: &str, body: &Value) -> Result<Value> {
        let url = self.api_url(path)?;
        let resp = self
            .client
            .put(url)
            .header("X-Api-Key", &self.api_key)
            .json(body)
            .send()
            .await
            .with_context(|| format!("PUT {path}"))?;
        let resp = resp
            .error_for_status()
            .with_context(|| format!("PUT {path} failed"))?;
        resp.json::<Value>()
            .await
            .with_context(|| format!("parsing PUT {path} response"))
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
            let mut target = match find_by_name(&existing, &indexer.name) {
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

    async fn upsert_downloaders(&self, downloaders: &[DownloaderSpec]) -> Result<()> {
        if downloaders.is_empty() {
            return Ok(());
        }
        let schema = self.get_json::<Vec<Value>>("downloadclient/schema").await?;
        let existing = self.get_json::<Vec<Value>>("downloadclient").await?;

        for downloader in downloaders {
            let tags = self.ensure_tags(&downloader.tags).await?;
            let schema_item = find_schema(&schema, &downloader.r#type)?;
            let mut target = match find_by_name(&existing, &downloader.name) {
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

    async fn upsert_quality_profiles(&self, profiles: &[QualityProfileSpec]) -> Result<()> {
        if profiles.is_empty() {
            return Ok(());
        }
        let existing = self.get_json::<Vec<Value>>("qualityprofile").await?;
        let template = existing
            .get(0)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("quality profile template missing"))?;

        for profile in profiles {
            let mut target = match find_by_name(&existing, &profile.name) {
                Some(existing) => existing,
                None => template.clone(),
            };
            if target.get("id").is_none() {
                target.as_object_mut().map(|obj| obj.remove("id"));
            }
            set_string(&mut target, "name", profile.name.clone())?;
            if let Some(upgrade) = profile.upgrade_allowed {
                set_bool(&mut target, "upgradeAllowed", upgrade)?;
            }

            let allowed = normalize_set(&profile.allowed);
            let cutoff_name = profile.cutoff.as_deref().map(normalize_name);
            let cutoff_id = apply_quality_items(&mut target, &allowed, cutoff_name.as_deref())?;
            if let Some(cutoff_id) = cutoff_id {
                set_i64(&mut target, "cutoff", cutoff_id)?;
            } else if profile.cutoff.is_some() {
                bail!("quality cutoff '{}' not found", profile.cutoff.as_deref().unwrap_or(""));
            }

            if let Some(id) = target.get("id").and_then(Value::as_i64) {
                let path = format!("qualityprofile/{id}");
                self.put_json(&path, &target).await?;
            } else {
                remove_readonly_fields(&mut target);
                self.post_json("qualityprofile", &target).await?;
            }
        }
        Ok(())
    }

    async fn upsert_language_profiles(&self, profiles: &[LanguageProfileSpec]) -> Result<()> {
        if profiles.is_empty() {
            return Ok(());
        }
        let existing = self
            .get_json::<Vec<Value>>("languageprofile")
            .await
            .context("fetching language profiles")?;
        let template = existing
            .get(0)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("language profile template missing"))?;

        for profile in profiles {
            let mut target = match find_by_name(&existing, &profile.name) {
                Some(existing) => existing,
                None => template.clone(),
            };
            set_string(&mut target, "name", profile.name.clone())?;
            let allowed = normalize_set(&profile.languages);
            let cutoff_name = profile.cutoff.as_deref().map(normalize_name);
            let cutoff_id = apply_language_items(&mut target, &allowed, cutoff_name.as_deref())?;
            if let Some(cutoff_id) = cutoff_id {
                set_i64(&mut target, "cutoff", cutoff_id)?;
            } else if profile.cutoff.is_some() {
                bail!("language cutoff '{}' not found", profile.cutoff.as_deref().unwrap_or(""));
            }

            if let Some(id) = target.get("id").and_then(Value::as_i64) {
                let path = format!("languageprofile/{id}");
                self.put_json(&path, &target).await?;
            } else {
                remove_readonly_fields(&mut target);
                self.post_json("languageprofile", &target).await?;
            }
        }
        Ok(())
    }

    async fn apply_series_defaults(&self, defaults: &SeriesTypeDefaultsSpec) -> Result<()> {
        let quality_profiles = self.get_json::<Vec<Value>>("qualityprofile").await?;
        let quality_id = match defaults.quality_profile.as_deref() {
            Some(name) => Some(find_id_by_name(&quality_profiles, name)?),
            None => None,
        };

        let language_profiles = if defaults.language_profile.is_some() {
            Some(self.get_json::<Vec<Value>>("languageprofile").await?)
        } else {
            None
        };
        let language_id = match defaults.language_profile.as_deref() {
            Some(name) => {
                let profiles = language_profiles
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("language profiles not available"))?;
                Some(find_id_by_name(profiles, name)?)
            }
            None => None,
        };

        let tags = if defaults.tags.is_empty() {
            Vec::new()
        } else {
            self.ensure_tags(&defaults.tags).await?
        };

        let series_list = self.get_json::<Vec<Value>>("series").await?;
        let target_type = normalize_name(&defaults.series_type);
        for series in series_list {
            let series_type = series
                .get("seriesType")
                .and_then(Value::as_str)
                .map(normalize_name)
                .unwrap_or_default();
            if series_type != target_type {
                continue;
            }
            let mut updated = series.clone();
            if let Some(quality_id) = quality_id {
                set_i64(&mut updated, "qualityProfileId", quality_id)?;
            }
            if let Some(language_id) = language_id {
                set_i64(&mut updated, "languageProfileId", language_id)?;
            }
            set_bool(&mut updated, "seasonFolder", defaults.season_folder)?;
            if !tags.is_empty() {
                merge_tags(&mut updated, &tags)?;
            }
            let id = updated
                .get("id")
                .and_then(Value::as_i64)
                .ok_or_else(|| anyhow::anyhow!("series id missing"))?;
            let path = format!("series/{id}");
            self.put_json(&path, &updated).await?;
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

    async fn assign_tags(&self, series_ids: &[i64], tags: &[String]) -> Result<()> {
        if series_ids.is_empty() {
            return Ok(());
        }
        let tag_ids = self.ensure_tags(tags).await?;
        for series_id in series_ids {
            let path = format!("series/{series_id}");
            let series = self.get_json::<Value>(&path).await?;
            let mut updated = series.clone();
            merge_tags(&mut updated, &tag_ids)?;
            self.put_json(&path, &updated).await?;
        }
        Ok(())
    }

    async fn upsert_webhooks(&self, webhooks: &[WebhookSpec]) -> Result<()> {
        if webhooks.is_empty() {
            return Ok(());
        }
        let schema = self.get_json::<Vec<Value>>("notification/schema").await?;
        let existing = self.get_json::<Vec<Value>>("notification").await?;
        let schema_item = schema
            .iter()
            .find(|value| is_webhook_schema(value))
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("webhook schema not found"))?;

        for webhook in webhooks {
            let mut target = match find_by_name(&existing, &webhook.name) {
                Some(existing) => existing,
                None => schema_item.clone(),
            };
            let enabled = webhook.enabled.unwrap_or(true);
            set_enabled(&mut target, enabled)?;
            set_string(&mut target, "name", webhook.name.clone())?;
            apply_notification_events(&mut target, &webhook.events)?;

            let fields = target
                .get_mut("fields")
                .and_then(Value::as_array_mut)
                .ok_or_else(|| anyhow::anyhow!("notification fields missing"))?;
            set_field_value_optional(fields, "url", Value::String(webhook.url.clone()))?;

            if let Some(id) = target.get("id").and_then(Value::as_i64) {
                let path = format!("notification/{id}");
                self.put_json(&path, &target).await?;
            } else {
                remove_readonly_fields(&mut target);
                self.post_json("notification", &target).await?;
            }
        }
        Ok(())
    }

    async fn upsert_custom_formats(&self, formats: &[CustomFormatSpec]) -> Result<()> {
        if formats.is_empty() {
            return Ok(());
        }
        let existing = self.get_json::<Vec<Value>>("customformat").await?;
        for format in formats {
            let mut target = match find_by_name(&existing, &format.name) {
                Some(existing) => existing,
                None => json!({}),
            };
            set_string(&mut target, "name", format.name.clone())?;
            set_bool(&mut target, "includeCustomFormatWhenRenaming", false)?;
            let specs = build_custom_format_specs(format)?;
            target
                .as_object_mut()
                .ok_or_else(|| anyhow::anyhow!("custom format object missing"))?
                .insert("specifications".to_string(), Value::Array(specs));

            if let Some(id) = target.get("id").and_then(Value::as_i64) {
                let path = format!("customformat/{id}");
                self.put_json(&path, &target).await?;
            } else {
                remove_readonly_fields(&mut target);
                self.post_json("customformat", &target).await?;
            }
        }
        Ok(())
    }

    async fn upsert_release_profiles(&self, profiles: &[ReleaseProfileSpec]) -> Result<()> {
        if profiles.is_empty() {
            return Ok(());
        }
        let existing = self.get_json::<Vec<Value>>("releaseprofile").await?;
        for profile in profiles {
            let mut target = match find_by_name(&existing, &profile.name) {
                Some(existing) => existing,
                None => json!({}),
            };
            set_string(&mut target, "name", profile.name.clone())?;
            set_string(
                &mut target,
                "required",
                join_lines(&profile.required),
            )?;
            set_string(&mut target, "ignored", join_lines(&profile.ignored))?;
            set_string(
                &mut target,
                "preferred",
                join_lines(&profile.preferred),
            )?;

            if let Some(id) = target.get("id").and_then(Value::as_i64) {
                let path = format!("releaseprofile/{id}");
                self.put_json(&path, &target).await?;
            } else {
                remove_readonly_fields(&mut target);
                self.post_json("releaseprofile", &target).await?;
            }
        }
        Ok(())
    }

    async fn apply_custom_format_scores(&self, formats: &[CustomFormatSpec]) -> Result<()> {
        let scored: Vec<&CustomFormatSpec> = formats
            .iter()
            .filter(|format| format.score.is_some())
            .collect();
        if scored.is_empty() {
            return Ok(());
        }
        let custom_formats = self.get_json::<Vec<Value>>("customformat").await?;
        let mut score_map = HashMap::new();
        for format in scored {
            let id = find_id_by_name(&custom_formats, &format.name)?;
            score_map.insert(id, format.score.unwrap_or(0));
        }

        let quality_profiles = self.get_json::<Vec<Value>>("qualityprofile").await?;
        for profile in quality_profiles {
            let mut updated = profile.clone();
            apply_format_items(&mut updated, &score_map)?;
            let id = updated
                .get("id")
                .and_then(Value::as_i64)
                .ok_or_else(|| anyhow::anyhow!("quality profile id missing"))?;
            let path = format!("qualityprofile/{id}");
            self.put_json(&path, &updated).await?;
        }
        Ok(())
    }
}

fn normalize_root_url(base_url: &str) -> Result<Url> {
    let mut url = Url::parse(base_url).context("parsing sonarr base_url")?;
    let mut path = url.path().trim_end_matches('/').to_string();
    for suffix in ["/api/v3", "/api/v4"] {
        if path.ends_with(suffix) {
            path = path.trim_end_matches(suffix).to_string();
        }
    }
    if path.is_empty() {
        path = "/".to_string();
    }
    url.set_path(&path);
    Ok(url)
}

fn normalize_version(version: &str) -> Result<&'static str> {
    let trimmed = version.trim().to_ascii_lowercase();
    match trimmed.as_str() {
        "v3" | "3" => Ok("v3"),
        "v4" | "4" => Ok("v4"),
        _ => bail!("unsupported sonarr api version '{version}'"),
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

fn build_api_url(root: &Url, version: &str, path: &str) -> Result<Url> {
    let api_base = build_api_base(root, version)?;
    api_base
        .join(path)
        .context("building sonarr api url")
}

fn normalize_name(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn normalize_set(values: &[String]) -> HashSet<String> {
    values.iter().map(|value| normalize_name(value)).collect()
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
    let needle = path.trim();
    items.iter().find_map(|value| {
        let current = value.get("path").and_then(Value::as_str)?;
        if current == needle {
            Some(value.clone())
        } else {
            None
        }
    })
}

fn find_id_by_name(items: &[Value], name: &str) -> Result<i64> {
    let needle = normalize_name(name);
    let id = items.iter().find_map(|value| {
        let current = value.get("name").and_then(Value::as_str)?;
        if normalize_name(current) == needle {
            value.get("id").and_then(Value::as_i64)
        } else {
            None
        }
    });
    id.ok_or_else(|| anyhow::anyhow!("entry '{}' not found", name))
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

fn set_bool(target: &mut Value, field: &str, value: bool) -> Result<()> {
    if let Some(obj) = target.as_object_mut() {
        obj.insert(field.to_string(), Value::Bool(value));
        return Ok(());
    }
    bail!("expected object for field '{}'", field);
}

fn set_i64(target: &mut Value, field: &str, value: i64) -> Result<()> {
    if let Some(obj) = target.as_object_mut() {
        obj.insert(field.to_string(), Value::Number(value.into()));
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

fn apply_quality_items(
    profile: &mut Value,
    allowed: &HashSet<String>,
    cutoff: Option<&str>,
) -> Result<Option<i64>> {
    let items = profile
        .get_mut("items")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| anyhow::anyhow!("quality profile items missing"))?;
    update_quality_items(items, allowed, cutoff)
}

fn update_quality_items(
    items: &mut [Value],
    allowed: &HashSet<String>,
    cutoff: Option<&str>,
) -> Result<Option<i64>> {
    let mut cutoff_id = None;
    for item in items {
        if let Some(quality) = item.get("quality") {
            let name = quality.get("name").and_then(Value::as_str);
            let id = quality.get("id").and_then(Value::as_i64);
            if let Some(name) = name {
                let normalized = normalize_name(name);
                let is_allowed = allowed.contains(&normalized);
                if let Some(obj) = item.as_object_mut() {
                    obj.insert("allowed".to_string(), Value::Bool(is_allowed));
                }
                if let Some(cutoff_name) = cutoff {
                    if normalized == normalize_name(cutoff_name) {
                        cutoff_id = id;
                    }
                }
            }
        }
        if let Some(children) = item.get_mut("items").and_then(Value::as_array_mut) {
            let nested_cutoff = update_quality_items(children, allowed, cutoff)?;
            if cutoff_id.is_none() {
                cutoff_id = nested_cutoff;
            }
        }
    }
    Ok(cutoff_id)
}

fn apply_language_items(
    profile: &mut Value,
    allowed: &HashSet<String>,
    cutoff: Option<&str>,
) -> Result<Option<i64>> {
    let languages = profile
        .get_mut("languages")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| anyhow::anyhow!("language profile languages missing"))?;
    let cutoff_name = cutoff;
    let mut cutoff_id = None;
    for entry in languages {
        let name = entry
            .get("name")
            .and_then(Value::as_str)
            .or_else(|| entry.get("language").and_then(|lang| lang.get("name")).and_then(Value::as_str));
        let id = entry
            .get("id")
            .and_then(Value::as_i64)
            .or_else(|| entry.get("language").and_then(|lang| lang.get("id")).and_then(Value::as_i64));
        if let Some(name) = name {
            let normalized = normalize_name(name);
            let is_allowed = allowed.contains(&normalized);
            if let Some(obj) = entry.as_object_mut() {
                obj.insert("allowed".to_string(), Value::Bool(is_allowed));
            }
            if let Some(cutoff_name) = cutoff_name {
                if normalized == normalize_name(cutoff_name) {
                    cutoff_id = id;
                }
            }
        }
    }
    Ok(cutoff_id)
}

fn merge_tags(target: &mut Value, tags: &[i64]) -> Result<()> {
    let existing = target
        .get("tags")
        .and_then(Value::as_array)
        .map(|array| {
            array
                .iter()
                .filter_map(Value::as_i64)
                .collect::<HashSet<i64>>()
        })
        .unwrap_or_default();
    let mut merged: HashSet<i64> = existing;
    for tag in tags {
        merged.insert(*tag);
    }
    let mut array = Vec::new();
    for tag in merged {
        array.push(Value::Number(tag.into()));
    }
    if let Some(obj) = target.as_object_mut() {
        obj.insert("tags".to_string(), Value::Array(array));
        return Ok(());
    }
    bail!("expected object for tag merge");
}

fn is_webhook_schema(value: &Value) -> bool {
    let implementation = value
        .get("implementation")
        .and_then(Value::as_str)
        .or_else(|| value.get("implementationName").and_then(Value::as_str))
        .unwrap_or_default();
    normalize_name(implementation) == "webhook"
}

fn apply_notification_events(target: &mut Value, events: &[String]) -> Result<()> {
    let enabled = normalize_set(events);
    let mapping = [
        ("grab", "onGrab"),
        ("download", "onDownload"),
        ("upgrade", "onUpgrade"),
        ("rename", "onRename"),
        ("series_add", "onSeriesAdd"),
        ("series_delete", "onSeriesDelete"),
        ("episode_file_delete", "onEpisodeFileDelete"),
        (
            "episode_file_delete_for_upgrade",
            "onEpisodeFileDeleteForUpgrade",
        ),
        ("health_issue", "onHealthIssue"),
        ("application_update", "onApplicationUpdate"),
    ];
    for (event, field) in mapping {
        let value = enabled.contains(&normalize_name(event));
        if let Some(obj) = target.as_object_mut() {
            obj.insert(field.to_string(), Value::Bool(value));
        }
    }
    Ok(())
}

fn build_custom_format_specs(format: &CustomFormatSpec) -> Result<Vec<Value>> {
    let mut specs = Vec::new();
    for pattern in &format.include {
        specs.push(custom_format_spec(pattern, false)?);
    }
    for pattern in &format.exclude {
        specs.push(custom_format_spec(pattern, true)?);
    }
    Ok(specs)
}

fn custom_format_spec(pattern: &str, negate: bool) -> Result<Value> {
    if pattern.trim().is_empty() {
        bail!("custom format pattern is required");
    }
    Ok(json!({
        "name": "Release Title",
        "implementation": "ReleaseTitleSpecification",
        "negate": negate,
        "required": true,
        "fields": [
            { "name": "value", "value": pattern }
        ]
    }))
}

fn join_lines(values: &[String]) -> String {
    values.join("\n")
}

fn apply_format_items(profile: &mut Value, scores: &HashMap<i64, i32>) -> Result<()> {
    let needs_insert = !profile.get("formatItems").is_some();
    if needs_insert {
        if let Some(obj) = profile.as_object_mut() {
            obj.insert("formatItems".to_string(), Value::Array(Vec::new()));
        }
    }
    let items = profile
        .get_mut("formatItems")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| anyhow::anyhow!("formatItems should be array"))?;

    for (format_id, score) in scores {
        let mut found = false;
        for item in items.iter_mut() {
            let current_id = item
                .get("format")
                .and_then(|format| format.get("id"))
                .and_then(Value::as_i64)
                .or_else(|| item.get("formatId").and_then(Value::as_i64));
            if current_id == Some(*format_id) {
                if let Some(obj) = item.as_object_mut() {
                    obj.insert("score".to_string(), Value::Number((*score).into()));
                }
                found = true;
                break;
            }
        }
        if !found {
            items.push(json!({
                "format": { "id": format_id },
                "score": score,
            }));
        }
    }
    Ok(())
}
