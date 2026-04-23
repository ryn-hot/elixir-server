use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use reqwest::header::{ACCEPT, HeaderMap, HeaderValue, USER_AGENT};
use reqwest::{Client, Method, StatusCode, Url};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tracing::debug;

use crate::drivers::media_manager_support::{
    MANAGED_MEDIA_TAG, lookup_terms_for_add_request, select_lookup_item,
};
use crate::drivers::patches::{
    CustomFormatSpec, DownloaderSpec, IndexerSpec, LanguageProfileSpec, MediaManagerTvPatch,
    QualityProfileSpec, ReleaseProfileSpec, RootFolderSpec, SeriesTypeDefaultsSpec, WebhookSpec,
    normalize_custom_format_specifications,
};
use crate::drivers::{
    AddMediaRequest, AddMediaResult, ApplyResult, CapabilityDriver, DriftEvaluation, DriverCtx,
    DriverPatch, PatchSemantics, PatchSideEffect, StateSnapshot, build_sonarr_quality_policy_plan,
    is_elixir_managed_sonarr_quality_profile,
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

    fn patch_semantics(&self, patch: &DriverPatch) -> PatchSemantics {
        match patch {
            DriverPatch::MediaManagerTv(MediaManagerTvPatch::ApplyQualityPolicyPreset {
                ..
            }) => PatchSemantics::desired_change_only(PatchSideEffect::LiveApiWrite),
            DriverPatch::MediaManagerTv(_) => {
                PatchSemantics::periodic_safe(PatchSideEffect::LiveApiWrite)
            }
            _ => PatchSemantics::periodic_safe(PatchSideEffect::LiveApiWrite),
        }
    }

    async fn evaluate_patch(&self, ctx: DriverCtx, patch: DriverPatch) -> Result<DriftEvaluation> {
        let patch = match patch {
            DriverPatch::MediaManagerTv(patch) => patch,
            _ => bail!("media.manager.tv patch mismatch"),
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

        let config = SonarrDriverConfig::from_ctx(&ctx)?;
        let client = SonarrClient::from_config(config, ctx.canonical_url()?).await?;

        match patch {
            MediaManagerTvPatch::SetIndexerRegistry { indexers } => Ok(drift_evaluation(
                "Sonarr indexers",
                client.evaluate_indexers(&indexers).await?,
            )),
            MediaManagerTvPatch::SetDownloaders { downloaders } => Ok(drift_evaluation(
                "Sonarr download clients",
                client.evaluate_downloaders(&downloaders).await?,
            )),
            MediaManagerTvPatch::SetRootFolders { roots } => Ok(drift_evaluation(
                "Sonarr root folders",
                client.evaluate_root_folders(&roots).await?,
            )),
            MediaManagerTvPatch::SetQualityProfiles { profiles } => Ok(drift_evaluation(
                "Sonarr quality profiles",
                client.evaluate_quality_profiles(&profiles).await?,
            )),
            MediaManagerTvPatch::SetLanguageProfiles { profiles } => Ok(drift_evaluation(
                "Sonarr language profiles",
                client.evaluate_language_profiles(&profiles).await?,
            )),
            MediaManagerTvPatch::SetSeriesTypeDefaults { defaults } => Ok(drift_evaluation(
                "Sonarr series defaults",
                client.evaluate_series_defaults(&defaults).await?,
            )),
            MediaManagerTvPatch::SetTags { tags } => Ok(drift_evaluation(
                "Sonarr tags",
                client.evaluate_tags(&tags).await?,
            )),
            MediaManagerTvPatch::AssignTags { series_ids, tags } => Ok(drift_evaluation(
                "Sonarr series tags",
                client.evaluate_assign_tags(&series_ids, &tags).await?,
            )),
            MediaManagerTvPatch::SetWebhooks { webhooks } => Ok(drift_evaluation(
                "Sonarr webhooks",
                client.evaluate_webhooks(&webhooks).await?,
            )),
            MediaManagerTvPatch::SetCustomFormats {
                formats,
                release_profiles,
            } => {
                let mut drift = client.evaluate_custom_formats(&formats).await?;
                drift.extend(client.evaluate_release_profiles(&release_profiles).await?);
                drift.extend(client.evaluate_custom_format_scores(&formats).await?);
                Ok(drift_evaluation(
                    "Sonarr custom formats",
                    dedup_strings(drift),
                ))
            }
            MediaManagerTvPatch::ApplyQualityPolicyPreset { policy } => {
                let plan = build_sonarr_quality_policy_plan(&policy)?;
                let mut drift = client
                    .evaluate_quality_profiles(std::slice::from_ref(&plan.quality_profile))
                    .await?;
                drift.extend(client.evaluate_custom_formats(&plan.custom_formats).await?);
                drift.extend(
                    client
                        .evaluate_exact_custom_format_scores(
                            &plan.quality_profile.name,
                            &plan.custom_formats,
                        )
                        .await?,
                );
                Ok(drift_evaluation(
                    "Sonarr quality policy preset",
                    dedup_strings(drift),
                ))
            }
            MediaManagerTvPatch::SetAuxServiceEndpoint { url } => {
                bail!("aux service endpoint is not supported for Sonarr ({})", url);
            }
        }
    }

    async fn read_state(&self, _ctx: DriverCtx) -> Result<StateSnapshot> {
        Ok(StateSnapshot {
            summary: Some("media.manager.tv driver is not implemented".to_string()),
            activity: None,
        })
    }

    async fn apply_patch(&self, ctx: DriverCtx, patch: DriverPatch) -> Result<ApplyResult> {
        let patch = match patch {
            DriverPatch::MediaManagerTv(patch) => patch,
            _ => bail!("media.manager.tv patch mismatch"),
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
                client.apply_custom_format_scores(&formats).await?;
            }
            MediaManagerTvPatch::ApplyQualityPolicyPreset { policy } => {
                let plan = build_sonarr_quality_policy_plan(&policy)?;
                client
                    .upsert_quality_profiles(std::slice::from_ref(&plan.quality_profile))
                    .await?;
                client.upsert_custom_formats(&plan.custom_formats).await?;
                client
                    .apply_exact_custom_format_scores(
                        &plan.quality_profile.name,
                        &plan.custom_formats,
                    )
                    .await?;
            }
            MediaManagerTvPatch::SetAuxServiceEndpoint { url } => {
                bail!("aux service endpoint is not supported for Sonarr ({})", url);
            }
        }

        Ok(ApplyResult::applied())
    }

    async fn add_media(&self, ctx: DriverCtx, request: AddMediaRequest) -> Result<AddMediaResult> {
        let implementation = ctx
            .implementation
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("provider implementation is required"))?;
        if implementation != "sonarr" {
            bail!(
                "media.manager.tv implementation '{}' does not support add_media",
                implementation
            );
        }

        let config = SonarrDriverConfig::from_ctx(&ctx)?;
        let client = SonarrClient::from_config(config, ctx.canonical_url()?).await?;
        client.add_series(&request).await
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
    api_version: &'static str,
    product_major: Option<u32>,
}

impl SonarrClient {
    async fn from_config(config: SonarrDriverConfig, endpoint_url: String) -> Result<Self> {
        let api_key = config
            .api_key
            .ok_or_else(|| anyhow::anyhow!("sonarr api_key is required"))?;
        let root = resolve_base_url(config.base_url.as_deref(), &endpoint_url)?;
        let mut headers = HeaderMap::new();
        headers.insert(
            "X-Api-Key",
            HeaderValue::from_str(&api_key).context("invalid sonarr api key header")?,
        );
        headers.insert(USER_AGENT, HeaderValue::from_static("Elixir/1.0"));
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .default_headers(headers)
            .build()
            .context("building sonarr http client")?;
        let mut client = Self {
            client,
            root,
            api_base: Url::parse("http://127.0.0.1/").expect("fallback url"),
            api_version: "v3",
            product_major: None,
        };

        if let Some(version) = config.api_version.as_deref() {
            client.set_api_version(version)?;
        } else {
            client.detect_api_version().await?;
        }
        client.load_product_major().await?;
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
        self.api_version = version;
        Ok(())
    }

    async fn load_product_major(&mut self) -> Result<()> {
        let status = self.get_json::<Value>("system/status").await?;
        self.product_major = parse_sonarr_major(&status);
        Ok(())
    }

    fn api_url(&self, path: &str) -> Result<Url> {
        let trimmed = path.trim_start_matches('/');
        self.api_base
            .join(trimmed)
            .context("building sonarr api url")
    }

    async fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let value = self.request_json_value(Method::GET, path, None).await?;
        serde_json::from_value(value).with_context(|| format!("parsing GET {path} response"))
    }

    async fn post_json(&self, path: &str, body: &Value) -> Result<Value> {
        self.request_json_value(Method::POST, path, Some(body))
            .await
    }

    async fn put_json(&self, path: &str, body: &Value) -> Result<Value> {
        self.request_json_value(Method::PUT, path, Some(body)).await
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
                bail!("sonarr api key rejected ({status}): {detail}");
            }
            bail!(
                "sonarr {} {path} failed ({status}): {detail}",
                method.as_str()
            );
        }
        if bytes.is_empty() {
            bail!("sonarr {} {path} returned empty response", method.as_str());
        }
        serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing {} {path} response", method.as_str()))
    }

    async fn add_series(&self, request: &AddMediaRequest) -> Result<AddMediaResult> {
        let lookup_terms = lookup_terms_for_add_request(request, "sonarr");
        debug!(
            lookup_terms = ?lookup_terms,
            media_type = ?request.media_type,
            title = %request.title,
            year = request.year,
            "adding media through sonarr driver"
        );

        let items = self
            .lookup_candidates(&lookup_terms, "series/lookup")
            .await?;
        let mut selected = select_lookup_item(&items, request)
            .ok_or_else(|| anyhow::anyhow!("unable to resolve title in manager lookup"))?;

        let quality_profile_id = match request.options.quality_profile_id {
            Some(value) => value,
            None => self.preferred_quality_profile_id().await?,
        };
        let root_folder_path = match request.options.root_folder_path.as_deref() {
            Some(path) if !path.trim().is_empty() => path.trim().to_string(),
            _ => self.first_path("rootfolder").await?,
        };
        let managed_tag_ids = self.ensure_tags(&[MANAGED_MEDIA_TAG.to_string()]).await?;

        if let Some(payload) = selected.as_object_mut() {
            payload.insert(
                "qualityProfileId".to_string(),
                Value::Number(quality_profile_id.into()),
            );
            payload.insert(
                "rootFolderPath".to_string(),
                Value::String(root_folder_path),
            );
            payload.insert(
                "monitored".to_string(),
                Value::Bool(request.options.monitor),
            );
            payload.insert("seasonFolder".to_string(), Value::Bool(true));
            payload.insert(
                "addOptions".to_string(),
                json!({
                    "searchForMissingEpisodes": request.options.search,
                    "monitor": if request.options.monitor { "all" } else { "none" }
                }),
            );
            if !managed_tag_ids.is_empty() {
                let _ = merge_tags(&mut selected, &managed_tag_ids)?;
            }
        } else {
            bail!("series payload must be an object");
        }

        let created = self.post_json("series", &selected).await?;
        Ok(AddMediaResult {
            manager_item_id: created
                .get("id")
                .and_then(Value::as_i64)
                .map(|value| value.to_string()),
        })
    }

    async fn lookup_candidates(&self, queries: &[String], path: &str) -> Result<Vec<Value>> {
        let mut last_items = Vec::new();
        for query in queries {
            let url = self.lookup_url(path, query)?;
            let resp = self
                .client
                .get(url)
                .send()
                .await
                .with_context(|| format!("GET {}?term={query}", path))?;
            let status = resp.status();
            if status == StatusCode::NOT_FOUND {
                continue;
            }
            let bytes = resp
                .bytes()
                .await
                .with_context(|| format!("reading GET {path} response"))?;
            if !status.is_success() {
                let detail = describe_error_body(&bytes);
                bail!("{path} failed ({status}): {detail}");
            }
            let items: Vec<Value> = serde_json::from_slice(&bytes)
                .with_context(|| format!("parsing GET {path} response"))?;
            if !items.is_empty() {
                return Ok(items);
            }
            last_items = items;
        }
        Ok(last_items)
    }

    fn lookup_url(&self, path: &str, term: &str) -> Result<Url> {
        let mut url = self.api_url(path)?;
        url.query_pairs_mut().append_pair("term", term);
        Ok(url)
    }

    async fn first_id(&self, path: &str) -> Result<i64> {
        let items = self.get_json::<Vec<Value>>(path).await?;
        items
            .first()
            .and_then(|item| item.get("id"))
            .and_then(Value::as_i64)
            .ok_or_else(|| anyhow::anyhow!("unable to determine {}", path))
    }

    async fn preferred_quality_profile_id(&self) -> Result<i64> {
        let profiles = self.get_json::<Vec<Value>>("qualityprofile").await?;
        if let Some(id) = profiles.iter().find_map(|profile| {
            let name = profile.get("name").and_then(Value::as_str)?;
            if is_elixir_managed_sonarr_quality_profile(name) {
                profile.get("id").and_then(Value::as_i64)
            } else {
                None
            }
        }) {
            return Ok(id);
        }
        profiles
            .first()
            .and_then(|item| item.get("id"))
            .and_then(Value::as_i64)
            .ok_or_else(|| anyhow::anyhow!("unable to determine qualityprofile"))
    }

    async fn first_path(&self, path: &str) -> Result<String> {
        let items = self.get_json::<Vec<Value>>(path).await?;
        items
            .first()
            .and_then(|item| item.get("path"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string())
            .ok_or_else(|| anyhow::anyhow!("unable to determine {}", path))
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
            let existing_item = find_by_name(&existing, &profile.name);
            let mut target = match existing_item.clone() {
                Some(existing) => existing,
                None => template.clone(),
            };
            if existing_item.is_none() {
                target.as_object_mut().map(|obj| obj.remove("id"));
            }
            set_string(&mut target, "name", profile.name.clone())?;
            if let Some(upgrade) = profile.upgrade_allowed {
                set_bool(&mut target, "upgradeAllowed", upgrade)?;
            }
            apply_quality_profile_score_fields(&mut target, profile)?;

            let allowed = normalize_set(&profile.allowed);
            let cutoff_name = profile.cutoff.as_deref().map(normalize_name);
            let cutoff_id = apply_quality_items(&mut target, &allowed, cutoff_name.as_deref())?;
            if let Some(cutoff_id) = cutoff_id {
                set_i64(&mut target, "cutoff", cutoff_id)?;
            } else if profile.cutoff.is_some() {
                bail!(
                    "quality cutoff '{}' not found",
                    profile.cutoff.as_deref().unwrap_or("")
                );
            }

            if let Some(existing_item) = existing_item {
                if target == existing_item {
                    continue;
                }
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
        self.ensure_sonarr_v3("language profiles")?;
        let existing = self
            .get_json::<Vec<Value>>("languageprofile")
            .await
            .context("fetching language profiles")?;
        let template = existing
            .get(0)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("language profile template missing"))?;

        for profile in profiles {
            let existing_item = find_by_name(&existing, &profile.name);
            let mut target = match existing_item.clone() {
                Some(existing) => existing,
                None => template.clone(),
            };
            if existing_item.is_none() {
                target.as_object_mut().map(|obj| obj.remove("id"));
            }
            set_string(&mut target, "name", profile.name.clone())?;
            let allowed = normalize_set(&profile.languages);
            let cutoff_name = profile.cutoff.as_deref().map(normalize_name);
            let cutoff_id = apply_language_items(&mut target, &allowed, cutoff_name.as_deref())?;
            if let Some(cutoff_id) = cutoff_id {
                set_i64(&mut target, "cutoff", cutoff_id)?;
            } else if profile.cutoff.is_some() {
                bail!(
                    "language cutoff '{}' not found",
                    profile.cutoff.as_deref().unwrap_or("")
                );
            }

            if let Some(existing_item) = existing_item {
                if target == existing_item {
                    continue;
                }
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

        if defaults.language_profile.is_some() {
            self.ensure_sonarr_v3("language profiles")?;
        }
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
                let _ = merge_tags(&mut updated, &tags)?;
            }
            let id = updated
                .get("id")
                .and_then(Value::as_i64)
                .ok_or_else(|| anyhow::anyhow!("series id missing"))?;
            if updated == series {
                continue;
            }
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

    async fn resolve_existing_tag_ids(&self, tags: &[String]) -> Result<(Vec<i64>, Vec<String>)> {
        if tags.is_empty() {
            return Ok((Vec::new(), Vec::new()));
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
        let mut missing = Vec::new();
        let mut seen = HashSet::new();
        for tag in tags {
            let normalized = normalize_name(tag);
            if !seen.insert(normalized.clone()) {
                continue;
            }
            if let Some(id) = by_name.get(&normalized) {
                tag_ids.push(*id);
            } else {
                missing.push(tag.clone());
            }
        }
        Ok((tag_ids, missing))
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
            let changed = merge_tags(&mut updated, &tag_ids)?;
            if !changed {
                continue;
            }
            self.put_json(&path, &updated).await?;
        }
        Ok(())
    }

    async fn evaluate_indexers(&self, indexers: &[IndexerSpec]) -> Result<Vec<String>> {
        if indexers.is_empty() {
            return Ok(Vec::new());
        }
        let schema = self.get_json::<Vec<Value>>("indexer/schema").await?;
        let existing = self.get_json::<Vec<Value>>("indexer").await?;
        let mut drift = Vec::new();

        for indexer in indexers {
            let (tags, missing_tags) = self.resolve_existing_tag_ids(&indexer.tags).await?;
            drift.extend(missing_tags.into_iter().map(|tag| format!("tag:{tag}")));

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

            if existing_item.as_ref() != Some(&target) {
                drift.push(indexer.name.clone());
            }
        }
        Ok(dedup_strings(drift))
    }

    async fn evaluate_downloaders(&self, downloaders: &[DownloaderSpec]) -> Result<Vec<String>> {
        if downloaders.is_empty() {
            return Ok(Vec::new());
        }
        let schema = self.get_json::<Vec<Value>>("downloadclient/schema").await?;
        let existing = self.get_json::<Vec<Value>>("downloadclient").await?;
        let mut drift = Vec::new();

        for downloader in downloaders {
            let (tags, missing_tags) = self.resolve_existing_tag_ids(&downloader.tags).await?;
            drift.extend(missing_tags.into_iter().map(|tag| format!("tag:{tag}")));

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

            if existing_item.as_ref() != Some(&target) {
                drift.push(downloader.name.clone());
            }
        }
        Ok(dedup_strings(drift))
    }

    async fn evaluate_root_folders(&self, roots: &[RootFolderSpec]) -> Result<Vec<String>> {
        if roots.is_empty() {
            return Ok(Vec::new());
        }
        let existing = self.get_json::<Vec<Value>>("rootfolder").await?;
        let mut drift = Vec::new();
        for root in roots {
            if find_by_path(&existing, &root.path).is_none() {
                drift.push(root.path.clone());
            }
        }
        Ok(drift)
    }

    async fn evaluate_quality_profiles(
        &self,
        profiles: &[QualityProfileSpec],
    ) -> Result<Vec<String>> {
        if profiles.is_empty() {
            return Ok(Vec::new());
        }
        let existing = self.get_json::<Vec<Value>>("qualityprofile").await?;
        let template = existing
            .first()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("quality profile template missing"))?;
        let mut drift = Vec::new();

        for profile in profiles {
            let existing_item = find_by_name(&existing, &profile.name);
            let mut target = match existing_item.clone() {
                Some(existing) => existing,
                None => template.clone(),
            };
            if existing_item.is_none() {
                target.as_object_mut().map(|obj| obj.remove("id"));
            }
            set_string(&mut target, "name", profile.name.clone())?;
            if let Some(upgrade) = profile.upgrade_allowed {
                set_bool(&mut target, "upgradeAllowed", upgrade)?;
            }
            apply_quality_profile_score_fields(&mut target, profile)?;

            let allowed = normalize_set(&profile.allowed);
            let cutoff_name = profile.cutoff.as_deref().map(normalize_name);
            let cutoff_id = apply_quality_items(&mut target, &allowed, cutoff_name.as_deref())?;
            if let Some(cutoff_id) = cutoff_id {
                set_i64(&mut target, "cutoff", cutoff_id)?;
            } else if profile.cutoff.is_some() {
                bail!(
                    "quality cutoff '{}' not found",
                    profile.cutoff.as_deref().unwrap_or("")
                );
            }

            if existing_item.as_ref() != Some(&target) {
                drift.push(profile.name.clone());
            }
        }
        Ok(dedup_strings(drift))
    }

    async fn evaluate_language_profiles(
        &self,
        profiles: &[LanguageProfileSpec],
    ) -> Result<Vec<String>> {
        if profiles.is_empty() {
            return Ok(Vec::new());
        }
        self.ensure_sonarr_v3("language profiles")?;
        let existing = self
            .get_json::<Vec<Value>>("languageprofile")
            .await
            .context("fetching language profiles")?;
        let template = existing
            .first()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("language profile template missing"))?;
        let mut drift = Vec::new();

        for profile in profiles {
            let existing_item = find_by_name(&existing, &profile.name);
            let mut target = match existing_item.clone() {
                Some(existing) => existing,
                None => template.clone(),
            };
            if existing_item.is_none() {
                target.as_object_mut().map(|obj| obj.remove("id"));
            }
            set_string(&mut target, "name", profile.name.clone())?;
            let allowed = normalize_set(&profile.languages);
            let cutoff_name = profile.cutoff.as_deref().map(normalize_name);
            let cutoff_id = apply_language_items(&mut target, &allowed, cutoff_name.as_deref())?;
            if let Some(cutoff_id) = cutoff_id {
                set_i64(&mut target, "cutoff", cutoff_id)?;
            } else if profile.cutoff.is_some() {
                bail!(
                    "language cutoff '{}' not found",
                    profile.cutoff.as_deref().unwrap_or("")
                );
            }

            if existing_item.as_ref() != Some(&target) {
                drift.push(profile.name.clone());
            }
        }
        Ok(dedup_strings(drift))
    }

    async fn evaluate_series_defaults(
        &self,
        defaults: &SeriesTypeDefaultsSpec,
    ) -> Result<Vec<String>> {
        let quality_profiles = self.get_json::<Vec<Value>>("qualityprofile").await?;
        let quality_id = match defaults.quality_profile.as_deref() {
            Some(name) => Some(find_id_by_name(&quality_profiles, name)?),
            None => None,
        };

        if defaults.language_profile.is_some() {
            self.ensure_sonarr_v3("language profiles")?;
        }
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

        let (tags, missing_tags) = self.resolve_existing_tag_ids(&defaults.tags).await?;
        let mut drift = missing_tags
            .into_iter()
            .map(|tag| format!("tag:{tag}"))
            .collect::<Vec<_>>();

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
                let _ = merge_tags(&mut updated, &tags)?;
            }
            if updated != series {
                let label = series
                    .get("title")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .unwrap_or_else(|| defaults.series_type.clone());
                drift.push(label);
            }
        }
        Ok(dedup_strings(drift))
    }

    async fn evaluate_tags(&self, tags: &[String]) -> Result<Vec<String>> {
        let (_ids, missing) = self.resolve_existing_tag_ids(tags).await?;
        Ok(missing)
    }

    async fn evaluate_assign_tags(
        &self,
        series_ids: &[i64],
        tags: &[String],
    ) -> Result<Vec<String>> {
        if series_ids.is_empty() {
            return Ok(Vec::new());
        }
        let (tag_ids, missing_tags) = self.resolve_existing_tag_ids(tags).await?;
        let mut drift = missing_tags
            .into_iter()
            .map(|tag| format!("tag:{tag}"))
            .collect::<Vec<_>>();
        let series_list = self.get_json::<Vec<Value>>("series").await?;
        let by_id = series_list
            .into_iter()
            .filter_map(|series| {
                series
                    .get("id")
                    .and_then(Value::as_i64)
                    .map(|id| (id, series))
            })
            .collect::<HashMap<_, _>>();
        for series_id in series_ids {
            let Some(series) = by_id.get(series_id) else {
                drift.push(format!("series:{series_id}"));
                continue;
            };
            let mut updated = series.clone();
            let changed = merge_tags(&mut updated, &tag_ids)?;
            if changed {
                let label = series
                    .get("title")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .unwrap_or_else(|| format!("series:{series_id}"));
                drift.push(label);
            }
        }
        Ok(dedup_strings(drift))
    }

    async fn evaluate_webhooks(&self, webhooks: &[WebhookSpec]) -> Result<Vec<String>> {
        if webhooks.is_empty() {
            return Ok(Vec::new());
        }
        let schema = self.get_json::<Vec<Value>>("notification/schema").await?;
        let existing = self.get_json::<Vec<Value>>("notification").await?;
        let schema_item = schema
            .iter()
            .find(|value| is_webhook_schema(value))
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("webhook schema not found"))?;
        let mut drift = Vec::new();

        for webhook in webhooks {
            let existing_item = find_by_name(&existing, &webhook.name);
            let mut target = match existing_item.clone() {
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

            if existing_item.as_ref() != Some(&target) {
                drift.push(webhook.name.clone());
            }
        }
        Ok(dedup_strings(drift))
    }

    async fn evaluate_custom_formats(&self, formats: &[CustomFormatSpec]) -> Result<Vec<String>> {
        if formats.is_empty() {
            return Ok(Vec::new());
        }
        let existing = self.get_json::<Vec<Value>>("customformat").await?;
        let mut drift = Vec::new();
        for format in formats {
            let existing_item = find_by_name(&existing, &format.name);
            let mut target = match existing_item.clone() {
                Some(existing) => existing,
                None => json!({}),
            };
            set_string(&mut target, "name", format.name.clone())?;
            set_bool(
                &mut target,
                "includeCustomFormatWhenRenaming",
                format.include_custom_format_when_renaming.unwrap_or(false),
            )?;
            let specs = build_custom_format_specs(format)?;
            target
                .as_object_mut()
                .ok_or_else(|| anyhow::anyhow!("custom format object missing"))?
                .insert("specifications".to_string(), Value::Array(specs));

            if existing_item.as_ref() != Some(&target) {
                drift.push(format.name.clone());
            }
        }
        Ok(dedup_strings(drift))
    }

    async fn evaluate_release_profiles(
        &self,
        profiles: &[ReleaseProfileSpec],
    ) -> Result<Vec<String>> {
        if profiles.is_empty() {
            return Ok(Vec::new());
        }
        self.ensure_sonarr_v3("release profiles")?;
        let existing = self.get_json::<Vec<Value>>("releaseprofile").await?;
        let mut drift = Vec::new();
        for profile in profiles {
            let existing_item = find_by_name(&existing, &profile.name);
            let mut target = match existing_item.clone() {
                Some(existing) => existing,
                None => json!({}),
            };
            set_string(&mut target, "name", profile.name.clone())?;
            set_string(&mut target, "required", join_lines(&profile.required))?;
            set_string(&mut target, "ignored", join_lines(&profile.ignored))?;
            set_string(&mut target, "preferred", join_lines(&profile.preferred))?;

            if existing_item.as_ref() != Some(&target) {
                drift.push(profile.name.clone());
            }
        }
        Ok(dedup_strings(drift))
    }

    async fn evaluate_custom_format_scores(
        &self,
        formats: &[CustomFormatSpec],
    ) -> Result<Vec<String>> {
        let scored: Vec<&CustomFormatSpec> = formats
            .iter()
            .filter(|format| format.score.is_some())
            .collect();
        if scored.is_empty() {
            return Ok(Vec::new());
        }
        let custom_formats = self.get_json::<Vec<Value>>("customformat").await?;
        let mut score_map = HashMap::new();
        let mut drift = Vec::new();
        for format in scored {
            if let Ok(id) = find_id_by_name(&custom_formats, &format.name) {
                score_map.insert(id, format.score.unwrap_or(0));
            } else {
                drift.push(format.name.clone());
            }
        }

        let quality_profiles = self.get_json::<Vec<Value>>("qualityprofile").await?;
        for profile in quality_profiles {
            let mut updated = profile.clone();
            let changed = apply_format_items(&mut updated, &score_map)?;
            if changed {
                let label = profile
                    .get("name")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .unwrap_or_else(|| "qualityprofile".to_string());
                drift.push(label);
            }
        }
        Ok(dedup_strings(drift))
    }

    async fn evaluate_exact_custom_format_scores(
        &self,
        profile_name: &str,
        formats: &[CustomFormatSpec],
    ) -> Result<Vec<String>> {
        let custom_formats = self.get_json::<Vec<Value>>("customformat").await?;
        let Some(expected) = build_named_score_map_if_present(formats, &custom_formats)? else {
            return Ok(vec![profile_name.to_string()]);
        };
        let quality_profiles = self.get_json::<Vec<Value>>("qualityprofile").await?;
        let Some(profile) = find_by_name(&quality_profiles, profile_name) else {
            return Ok(vec![profile_name.to_string()]);
        };
        let actual = extract_named_format_scores(&profile);
        if actual == expected {
            return Ok(Vec::new());
        }
        Ok(vec![profile_name.to_string()])
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
            let existing_item = find_by_name(&existing, &webhook.name);
            let mut target = match existing_item.clone() {
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

            if let Some(existing_item) = existing_item {
                if target == existing_item {
                    continue;
                }
            }

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
            let existing_item = find_by_name(&existing, &format.name);
            let mut target = match existing_item.clone() {
                Some(existing) => existing,
                None => json!({}),
            };
            set_string(&mut target, "name", format.name.clone())?;
            set_bool(
                &mut target,
                "includeCustomFormatWhenRenaming",
                format.include_custom_format_when_renaming.unwrap_or(false),
            )?;
            let specs = build_custom_format_specs(format)?;
            target
                .as_object_mut()
                .ok_or_else(|| anyhow::anyhow!("custom format object missing"))?
                .insert("specifications".to_string(), Value::Array(specs));

            if let Some(existing_item) = existing_item {
                if target == existing_item {
                    continue;
                }
            }

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
        self.ensure_sonarr_v3("release profiles")?;
        let existing = self.get_json::<Vec<Value>>("releaseprofile").await?;
        for profile in profiles {
            let existing_item = find_by_name(&existing, &profile.name);
            let mut target = match existing_item.clone() {
                Some(existing) => existing,
                None => json!({}),
            };
            set_string(&mut target, "name", profile.name.clone())?;
            set_string(&mut target, "required", join_lines(&profile.required))?;
            set_string(&mut target, "ignored", join_lines(&profile.ignored))?;
            set_string(&mut target, "preferred", join_lines(&profile.preferred))?;

            if let Some(existing_item) = existing_item {
                if target == existing_item {
                    continue;
                }
            }

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
            let changed = apply_format_items(&mut updated, &score_map)?;
            let id = updated
                .get("id")
                .and_then(Value::as_i64)
                .ok_or_else(|| anyhow::anyhow!("quality profile id missing"))?;
            if !changed {
                continue;
            }
            let path = format!("qualityprofile/{id}");
            self.put_json(&path, &updated).await?;
        }
        Ok(())
    }

    async fn apply_exact_custom_format_scores(
        &self,
        profile_name: &str,
        formats: &[CustomFormatSpec],
    ) -> Result<()> {
        let custom_formats = self.get_json::<Vec<Value>>("customformat").await?;
        let expected = build_named_score_map(formats, &custom_formats)?;
        let quality_profiles = self.get_json::<Vec<Value>>("qualityprofile").await?;
        let profile = find_by_name(&quality_profiles, profile_name)
            .ok_or_else(|| anyhow::anyhow!("quality profile '{}' not found", profile_name))?;
        let mut updated = profile.clone();
        set_exact_format_items(&mut updated, &expected)?;
        if updated == profile {
            return Ok(());
        }
        let id = updated
            .get("id")
            .and_then(Value::as_i64)
            .ok_or_else(|| anyhow::anyhow!("quality profile id missing"))?;
        let path = format!("qualityprofile/{id}");
        self.put_json(&path, &updated).await?;
        Ok(())
    }

    fn ensure_sonarr_v3(&self, feature: &str) -> Result<()> {
        ensure_sonarr_v3(self.product_major, self.api_version, feature)
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
        .ok_or_else(|| anyhow::anyhow!("sonarr base_url host is missing"))?;
    let endpoint_host = endpoint
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("sonarr endpoint host is missing"))?;
    let candidate_port = candidate.port_or_known_default().unwrap_or(80);
    let endpoint_port = endpoint.port_or_known_default().unwrap_or(80);
    if candidate.scheme() != endpoint.scheme()
        || candidate_host != endpoint_host
        || candidate_port != endpoint_port
    {
        bail!("sonarr base_url must match provider endpoint scheme/host/port");
    }
    Ok(())
}

fn normalize_version(version: &str) -> Result<&'static str> {
    let trimmed = version.trim().to_ascii_lowercase();
    match trimmed.as_str() {
        "v3" | "3" => Ok("v3"),
        "v4" | "4" => Ok("v4"),
        _ => bail!("unsupported sonarr api version '{version}'"),
    }
}

fn ensure_sonarr_v3(product_major: Option<u32>, api_version: &str, feature: &str) -> Result<()> {
    if let Some(major) = product_major {
        if major >= 4 {
            bail!("sonarr {feature} require Sonarr v3, detected v{major}");
        }
        return Ok(());
    }
    if api_version == "v4" {
        bail!("sonarr {feature} require Sonarr v3, detected api v4");
    }
    Ok(())
}

fn parse_sonarr_major(status: &Value) -> Option<u32> {
    let version = status.get("version").and_then(Value::as_str)?;
    let major = version.split('.').next()?.trim();
    major.parse().ok()
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
    api_base.join(path).context("building sonarr api url")
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

fn apply_downloader_fields(fields: &mut Vec<Value>, spec: &DownloaderSpec) -> Result<()> {
    apply_url_fields(fields, &spec.url)?;
    if let Some(api_key) = spec.api_key.as_ref() {
        set_field_value_optional(fields, "apiKey", Value::String(api_key.clone()))?;
    }
    if let Some(category) = spec.category.as_ref() {
        if !set_field_value_optional(fields, "category", Value::String(category.clone()))?
            && !set_field_value_optional(fields, "tvCategory", Value::String(category.clone()))?
        {
            debug!(
                "download client category is unsupported by schema for type '{}'; skipping",
                spec.r#type
            );
        }
    }
    for (key, value) in &spec.settings {
        if !set_field_value_optional(fields, key, value.clone())? {
            debug!(
                "download client field '{}' not present in schema; skipping",
                key
            );
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
        let array = values
            .iter()
            .map(|value| Value::Number((*value).into()))
            .collect();
        obj.insert(field.to_string(), Value::Array(array));
        return Ok(());
    }
    bail!("expected object for field '{}'", field);
}

fn apply_quality_profile_score_fields(
    target: &mut Value,
    profile: &QualityProfileSpec,
) -> Result<()> {
    if let Some(value) = profile.min_format_score {
        set_i64(target, "minFormatScore", i64::from(value))?;
    }
    if let Some(value) = profile.min_upgrade_format_score {
        set_i64(target, "minUpgradeFormatScore", i64::from(value))?;
    }
    if let Some(value) = profile.cutoff_format_score {
        set_i64(target, "cutoffFormatScore", i64::from(value))?;
    }
    Ok(())
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
        let mut nested_cutoff = None;
        let mut group_has_allowed_child = None;
        if let Some(children) = item.get_mut("items").and_then(Value::as_array_mut) {
            nested_cutoff = update_quality_items(children, allowed, cutoff)?;
            group_has_allowed_child = Some(
                children
                    .iter()
                    .any(|child| child.get("allowed").and_then(Value::as_bool).unwrap_or(false)),
            );
        }

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

        let group_name = item
            .get("name")
            .and_then(Value::as_str)
            .map(str::to_string);
        let group_id = item.get("id").and_then(Value::as_i64);
        if let Some(group_name) = group_name {
            if let Some(has_allowed_child) = group_has_allowed_child {
                if let Some(obj) = item.as_object_mut() {
                    obj.insert("allowed".to_string(), Value::Bool(has_allowed_child));
                }
            }
            if let Some(cutoff_name) = cutoff {
                if normalize_name(&group_name) == normalize_name(cutoff_name) {
                    cutoff_id = group_id;
                }
            }
        }

        if cutoff_id.is_none() {
            cutoff_id = nested_cutoff;
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
        let name = entry.get("name").and_then(Value::as_str).or_else(|| {
            entry
                .get("language")
                .and_then(|lang| lang.get("name"))
                .and_then(Value::as_str)
        });
        let id = entry.get("id").and_then(Value::as_i64).or_else(|| {
            entry
                .get("language")
                .and_then(|lang| lang.get("id"))
                .and_then(Value::as_i64)
        });
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

fn merge_tags(target: &mut Value, tags: &[i64]) -> Result<bool> {
    let existing = target
        .get("tags")
        .and_then(Value::as_array)
        .map(|array| array.iter().filter_map(Value::as_i64).collect::<Vec<i64>>())
        .unwrap_or_default();
    let mut seen: HashSet<i64> = existing.iter().copied().collect();
    let mut merged = existing.clone();
    let mut changed = false;
    for tag in tags {
        if seen.insert(*tag) {
            merged.push(*tag);
            changed = true;
        }
    }
    if !changed {
        return Ok(false);
    }
    let mut array = Vec::new();
    for tag in merged {
        array.push(Value::Number(tag.into()));
    }
    if let Some(obj) = target.as_object_mut() {
        obj.insert("tags".to_string(), Value::Array(array));
        return Ok(true);
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
    if !format.specifications.is_empty() {
        return normalize_custom_format_specifications(&format.specifications);
    }
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

fn apply_format_items(profile: &mut Value, scores: &HashMap<i64, i32>) -> Result<bool> {
    let mut changed = false;
    let needs_insert = !profile.get("formatItems").is_some();
    if needs_insert {
        if let Some(obj) = profile.as_object_mut() {
            obj.insert("formatItems".to_string(), Value::Array(Vec::new()));
            changed = true;
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
                .and_then(|format| format.as_i64().or_else(|| format.get("id").and_then(Value::as_i64)))
                .or_else(|| item.get("formatId").and_then(Value::as_i64));
            if current_id == Some(*format_id) {
                let current_score = item.get("score").and_then(Value::as_i64);
                if let Some(obj) = item.as_object_mut() {
                    if current_score != Some(i64::from(*score)) {
                        obj.insert("score".to_string(), Value::Number((*score).into()));
                        changed = true;
                    }
                }
                found = true;
                break;
            }
        }
        if !found {
            items.push(json!({
                "format": format_id,
                "score": score,
            }));
            changed = true;
        }
    }
    Ok(changed)
}

fn build_named_score_map(
    formats: &[CustomFormatSpec],
    custom_formats: &[Value],
) -> Result<HashMap<String, i32>> {
    let mut score_map = HashMap::new();
    for format in formats.iter().filter(|format| format.score.is_some()) {
        let id = find_id_by_name(custom_formats, &format.name)?;
        score_map.insert(id.to_string(), format.score.unwrap_or_default());
    }
    Ok(score_map)
}

fn build_named_score_map_if_present(
    formats: &[CustomFormatSpec],
    custom_formats: &[Value],
) -> Result<Option<HashMap<String, i32>>> {
    let mut score_map = HashMap::new();
    for format in formats.iter().filter(|format| format.score.is_some()) {
        let Ok(id) = find_id_by_name(custom_formats, &format.name) else {
            return Ok(None);
        };
        score_map.insert(id.to_string(), format.score.unwrap_or_default());
    }
    Ok(Some(score_map))
}

fn extract_named_format_scores(profile: &Value) -> HashMap<String, i32> {
    profile
        .get("formatItems")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|item| {
            let id = item
                .get("format")
                .and_then(|format| format.as_i64().or_else(|| format.get("id").and_then(Value::as_i64)))
                .or_else(|| item.get("formatId").and_then(Value::as_i64))?;
            let score = item.get("score").and_then(Value::as_i64)?;
            Some((id.to_string(), score as i32))
        })
        .collect()
}

fn set_exact_format_items(profile: &mut Value, scores: &HashMap<String, i32>) -> Result<()> {
    let mut items = scores
        .iter()
        .map(|(format_id, score)| {
            json!({
                "format": format_id.parse::<i64>().unwrap_or_default(),
                "score": score,
            })
        })
        .collect::<Vec<_>>();
    items.sort_by(|left, right| {
        let left_id = left
            .get("format")
            .and_then(|format| format.as_i64().or_else(|| format.get("id").and_then(Value::as_i64)))
            .unwrap_or_default();
        let right_id = right
            .get("format")
            .and_then(|format| format.as_i64().or_else(|| format.get("id").and_then(Value::as_i64)))
            .unwrap_or_default();
        left_id.cmp(&right_id)
    });
    if let Some(obj) = profile.as_object_mut() {
        obj.insert("formatItems".to_string(), Value::Array(items));
        return Ok(());
    }
    bail!("quality profile must be an object");
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

fn drift_evaluation(subject: &str, drift: Vec<String>) -> DriftEvaluation {
    if drift.is_empty() {
        DriftEvaluation::in_sync()
    } else {
        DriftEvaluation::drifted(format!("{subject} require repair: {}", drift.join(", ")))
    }
}

fn dedup_strings(values: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::new();
    for value in values {
        let key = normalize_name(&value);
        if seen.insert(key) {
            deduped.push(value);
        }
    }
    deduped
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    #[test]
    fn resolve_base_url_requires_same_origin() {
        let result = resolve_base_url(Some("http://other-host:8989"), "http://svc:8989/");
        assert!(result.is_err());

        let resolved =
            resolve_base_url(Some("http://svc:8989/sonarr"), "http://svc:8989/").unwrap();
        assert_eq!(resolved.path(), "/sonarr");
    }

    #[test]
    fn merge_tags_preserves_order_and_detects_change() {
        let mut target = json!({ "tags": [2, 1] });
        let changed = merge_tags(&mut target, &[1, 3]).expect("merge tags");
        assert!(changed);
        assert_eq!(target.get("tags"), Some(&json!([2, 1, 3])));

        let changed = merge_tags(&mut target, &[3]).expect("merge tags");
        assert!(!changed);
    }

    #[test]
    fn apply_format_items_reports_changes() {
        let mut profile = json!({
            "formatItems": [
                { "formatId": 1, "score": 10 }
            ]
        });
        let mut scores = HashMap::new();
        scores.insert(1, 10);
        let changed = apply_format_items(&mut profile, &scores).expect("apply scores");
        assert!(!changed);

        scores.insert(1, 20);
        let changed = apply_format_items(&mut profile, &scores).expect("apply scores");
        assert!(changed);

        scores.insert(2, 5);
        let changed = apply_format_items(&mut profile, &scores).expect("apply scores");
        assert!(changed);
        assert_eq!(profile["formatItems"][1]["format"], Value::Number(2.into()));
    }

    #[test]
    fn ensure_sonarr_v3_reports_mismatch() {
        let err = ensure_sonarr_v3(Some(4), "v3", "release profiles").unwrap_err();
        assert!(
            err.to_string()
                .contains("release profiles require Sonarr v3"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn parse_sonarr_major_handles_version() {
        let status = json!({ "version": "4.0.1.1234" });
        assert_eq!(parse_sonarr_major(&status), Some(4));
    }

    #[test]
    fn quality_policy_score_map_evaluation_treats_missing_formats_as_drift() {
        let formats = vec![CustomFormatSpec {
            name: "AV1".to_string(),
            include_custom_format_when_renaming: None,
            score: Some(1500),
            include: Vec::new(),
            exclude: Vec::new(),
            specifications: Vec::new(),
        }];

        let present = build_named_score_map_if_present(
            &formats,
            &[json!({ "id": 7, "name": "AV1" })],
        )
        .expect("score map should build");
        assert_eq!(
            present,
            Some(HashMap::from([(String::from("7"), 1500)]))
        );

        let missing = build_named_score_map_if_present(&formats, &[])
            .expect("missing formats should not error");
        assert_eq!(missing, None);
    }

    #[test]
    fn apply_quality_items_uses_group_cutoff_for_sonarr_web_profiles() {
        let mut profile = json!({
            "items": [
                {
                    "name": "WEB 1080p",
                    "id": 1002,
                    "allowed": true,
                    "items": [
                        {
                            "quality": { "id": 15, "name": "WEBRip-1080p" },
                            "items": [],
                            "allowed": true
                        },
                        {
                            "quality": { "id": 3, "name": "WEBDL-1080p" },
                            "items": [],
                            "allowed": true
                        }
                    ]
                }
            ]
        });

        let cutoff_id = apply_quality_items(
            &mut profile,
            &HashSet::from([
                normalize_name("WEBRip-1080p"),
                normalize_name("WEBDL-1080p"),
            ]),
            Some("WEB 1080p"),
        )
        .expect("apply quality items");

        assert_eq!(cutoff_id, Some(1002));
        assert_eq!(profile["items"][0]["allowed"], Value::Bool(true));
        assert_eq!(profile["items"][0]["items"][0]["allowed"], Value::Bool(true));
        assert_eq!(profile["items"][0]["items"][1]["allowed"], Value::Bool(true));
    }
}

#[cfg(all(test, feature = "docker-sonarr-tests"))]
mod docker_tests {
    use super::*;

    use anyhow::{Context, Result, bail};
    use quick_xml::Reader;
    use quick_xml::events::Event;
    use tempfile::TempDir;
    use tokio::fs;
    use tokio::process::Command;
    use tokio::time::{Duration, Instant, sleep};
    use uuid::Uuid;

    const SONARR_IMAGE_DEFAULT: &str = "lscr.io/linuxserver/sonarr:4.0.0";
    const SONARR_PORT: u16 = 8989;

    struct DockerContainer {
        name: String,
    }

    impl Drop for DockerContainer {
        fn drop(&mut self) {
            let _ = std::process::Command::new("docker")
                .args(["rm", "-f", &self.name])
                .output();
        }
    }

    async fn docker_output(args: &[&str]) -> Result<String> {
        let output = Command::new("docker")
            .args(args)
            .output()
            .await
            .with_context(|| format!("running docker {}", args.join(" ")))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!("docker {} failed: {}", args.join(" "), stderr.trim());
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    async fn ensure_docker_available() -> Result<()> {
        let output = Command::new("docker")
            .arg("info")
            .output()
            .await
            .context("checking docker availability")?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!("docker is not available: {}", stderr.trim());
        }
        Ok(())
    }

    fn sonarr_image() -> String {
        std::env::var("SONARR_IMAGE").unwrap_or_else(|_| SONARR_IMAGE_DEFAULT.to_string())
    }

    async fn start_sonarr_container(config_dir: &TempDir) -> Result<DockerContainer> {
        let name = format!("elixir-sonarr-{}", Uuid::new_v4());
        let image = sonarr_image();
        let config_path = config_dir
            .path()
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("config dir path is invalid"))?;
        docker_output(&[
            "run",
            "-d",
            "--rm",
            "--name",
            &name,
            "-p",
            &SONARR_PORT.to_string(),
            "-v",
            &format!("{config_path}:/config"),
            "-e",
            "TZ=UTC",
            &image,
        ])
        .await?;
        Ok(DockerContainer { name })
    }

    async fn container_host_port(name: &str) -> Result<u16> {
        let output = docker_output(&["port", name, &format!("{SONARR_PORT}/tcp")]).await?;
        let line = output
            .lines()
            .next()
            .ok_or_else(|| anyhow::anyhow!("docker port output empty"))?;
        let port_str = line
            .rsplit(':')
            .next()
            .ok_or_else(|| anyhow::anyhow!("docker port output invalid"))?;
        port_str
            .parse::<u16>()
            .with_context(|| format!("invalid host port '{}'", port_str))
    }

    async fn wait_for_api_key(config_dir: &TempDir) -> Result<String> {
        let path = config_dir.path().join("config.xml");
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            if let Ok(content) = fs::read_to_string(&path).await {
                if let Some(key) = parse_sonarr_api_key(&content)? {
                    return Ok(key);
                }
            }
            if Instant::now() > deadline {
                bail!("timed out waiting for sonarr config.xml api key");
            }
            sleep(Duration::from_secs(2)).await;
        }
    }

    fn parse_sonarr_api_key(xml: &str) -> Result<Option<String>> {
        let mut reader = Reader::from_str(xml);
        reader.trim_text(true);
        let mut buf = Vec::new();
        let mut in_key = false;
        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Start(event)) => {
                    if event.name().as_ref() == b"ApiKey" {
                        in_key = true;
                    }
                }
                Ok(Event::End(event)) => {
                    if event.name().as_ref() == b"ApiKey" {
                        in_key = false;
                    }
                }
                Ok(Event::Text(event)) if in_key => {
                    let value = event.unescape().context("decoding ApiKey")?;
                    return Ok(Some(value.to_string()));
                }
                Ok(Event::Eof) => break,
                Err(err) => return Err(err.into()),
                _ => {}
            }
            buf.clear();
        }
        Ok(None)
    }

    async fn wait_for_system_status(base_url: &str, api_key: &str) -> Result<Value> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .context("building readiness http client")?;
        let deadline = Instant::now() + Duration::from_secs(120);
        let urls = [
            format!("{base_url}/api/v3/system/status"),
            format!("{base_url}/api/v4/system/status"),
        ];
        loop {
            for url in &urls {
                if let Ok(resp) = client.get(url).header("X-Api-Key", api_key).send().await {
                    if resp.status().is_success() {
                        let value = resp.json::<Value>().await?;
                        return Ok(value);
                    }
                }
            }
            if Instant::now() > deadline {
                bail!("timed out waiting for sonarr api readiness");
            }
            sleep(Duration::from_secs(2)).await;
        }
    }

    #[tokio::test]
    async fn sonarr_v4_guard_messages() -> Result<()> {
        ensure_docker_available().await?;
        let config_dir = TempDir::new().context("creating temp config dir")?;
        let container = start_sonarr_container(&config_dir).await?;
        let api_key = wait_for_api_key(&config_dir).await?;
        let host_port = container_host_port(&container.name).await?;
        let base_url = format!("http://127.0.0.1:{host_port}");
        let status = wait_for_system_status(&base_url, &api_key).await?;
        let major = parse_sonarr_major(&status).unwrap_or(0);
        if major < 4 {
            bail!("expected Sonarr v4+, detected v{major}");
        }

        let config = SonarrDriverConfig {
            api_key: Some(api_key),
            base_url: Some(base_url.clone()),
            api_version: None,
        };
        let client = SonarrClient::from_config(config, base_url.clone()).await?;
        assert_eq!(client.product_major, Some(major));

        let language_profiles = vec![LanguageProfileSpec {
            name: "English".to_string(),
            languages: vec!["English".to_string()],
            cutoff: Some("English".to_string()),
        }];
        let err = client
            .upsert_language_profiles(&language_profiles)
            .await
            .unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("require Sonarr v3"),
            "unexpected error: {message}"
        );
        assert!(
            message.contains(&format!("v{major}")),
            "unexpected version in error: {message}"
        );

        let release_profiles = vec![ReleaseProfileSpec {
            name: "x265".to_string(),
            required: vec!["x265".to_string()],
            ignored: Vec::new(),
            preferred: Vec::new(),
        }];
        let err = client
            .upsert_release_profiles(&release_profiles)
            .await
            .unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("require Sonarr v3"),
            "unexpected error: {message}"
        );
        assert!(
            message.contains(&format!("v{major}")),
            "unexpected version in error: {message}"
        );
        Ok(())
    }
}
