use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use reqwest::Url;
use reqwest::{
    Client, Method, StatusCode,
    header::{ACCEPT, HeaderMap, HeaderValue, USER_AGENT},
};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tracing::debug;

use crate::drivers::media_manager_support::{
    MANAGED_MEDIA_TAG, lookup_terms_for_add_request, select_lookup_item,
};
use crate::drivers::patches::{
    CustomFormatSpec, DownloaderSpec, IndexerSpec, MediaManagerMoviesPatch, QualityProfileSpec,
    RootFolderSpec, normalize_custom_format_specifications,
};
use crate::drivers::{
    AddMediaRequest, AddMediaResult, ApplyResult, CapabilityDriver, DriftEvaluation, DriverCtx,
    DriverPatch, PatchSemantics, PatchSideEffect, StateSnapshot, build_radarr_quality_policy_plan,
    is_elixir_managed_radarr_quality_profile,
};

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

    fn patch_semantics(&self, patch: &DriverPatch) -> PatchSemantics {
        match patch {
            DriverPatch::MediaManagerMovies(
                MediaManagerMoviesPatch::ApplyQualityPolicyPreset { .. },
            ) => PatchSemantics::desired_change_only(PatchSideEffect::LiveApiWrite),
            DriverPatch::MediaManagerMovies(_) => {
                PatchSemantics::periodic_safe(PatchSideEffect::LiveApiWrite)
            }
            _ => PatchSemantics::periodic_safe(PatchSideEffect::LiveApiWrite),
        }
    }

    async fn evaluate_patch(&self, ctx: DriverCtx, patch: DriverPatch) -> Result<DriftEvaluation> {
        let patch = match patch {
            DriverPatch::MediaManagerMovies(patch) => patch,
            _ => bail!("media.manager.movies patch mismatch"),
        };

        let endpoint_url = ctx.canonical_url()?;
        let config = RadarrDriverConfig::from_ctx(&ctx)?;
        let client = RadarrClient::from_config(config, endpoint_url).await?;

        match patch {
            MediaManagerMoviesPatch::SetIndexerRegistry { indexers } => Ok(drift_evaluation(
                "Radarr indexers",
                client.evaluate_indexers(&indexers).await?,
            )),
            MediaManagerMoviesPatch::SetDownloaders { downloaders } => Ok(drift_evaluation(
                "Radarr download clients",
                client.evaluate_downloaders(&downloaders).await?,
            )),
            MediaManagerMoviesPatch::SetRootFolders { roots } => Ok(drift_evaluation(
                "Radarr root folders",
                client.evaluate_root_folders(&roots).await?,
            )),
            MediaManagerMoviesPatch::SetTags { tags } => Ok(drift_evaluation(
                "Radarr tags",
                client.evaluate_tags(&tags).await?,
            )),
            MediaManagerMoviesPatch::ApplyQualityPolicyPreset { policy } => {
                let plan = build_radarr_quality_policy_plan(&policy)?;
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
                    "Radarr quality policy preset",
                    dedup_strings(drift),
                ))
            }
        }
    }

    async fn read_state(&self, _ctx: DriverCtx) -> Result<StateSnapshot> {
        Ok(StateSnapshot {
            summary: None,
            activity: None,
        })
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
            MediaManagerMoviesPatch::SetIndexerRegistry { indexers } => {
                client.upsert_indexers(&indexers).await?;
            }
            MediaManagerMoviesPatch::SetDownloaders { downloaders } => {
                client.upsert_downloaders(&downloaders).await?;
            }
            MediaManagerMoviesPatch::SetRootFolders { roots } => {
                client.ensure_root_folders(&roots).await?;
            }
            MediaManagerMoviesPatch::SetTags { tags } => {
                let _ = client.ensure_tags(&tags).await?;
            }
            MediaManagerMoviesPatch::ApplyQualityPolicyPreset { policy } => {
                let plan = build_radarr_quality_policy_plan(&policy)?;
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
        }

        Ok(ApplyResult::applied())
    }

    async fn add_media(&self, ctx: DriverCtx, request: AddMediaRequest) -> Result<AddMediaResult> {
        let implementation = ctx
            .implementation
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("provider implementation is required"))?;
        if implementation != "radarr" {
            bail!(
                "media.manager.movies implementation '{}' does not support add_media",
                implementation
            );
        }

        let endpoint_url = ctx.canonical_url()?;
        let config = RadarrDriverConfig::from_ctx(&ctx)?;
        let client = RadarrClient::from_config(config, endpoint_url).await?;
        client.add_movie(&request).await
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
                bail!("radarr api key rejected ({status}): {detail}");
            }
            bail!(
                "radarr {} {path} failed ({status}): {detail}",
                method.as_str()
            );
        }
        if bytes.is_empty() {
            bail!("radarr {} {path} returned empty response", method.as_str());
        }
        let value: Value =
            serde_json::from_slice(&bytes).context("parsing radarr json response")?;
        Ok(value)
    }

    async fn add_movie(&self, request: &AddMediaRequest) -> Result<AddMediaResult> {
        let lookup_terms = lookup_terms_for_add_request(request, "radarr");
        debug!(
            lookup_terms = ?lookup_terms,
            title = %request.title,
            year = request.year,
            "adding media through radarr driver"
        );

        let items = self
            .lookup_candidates(&lookup_terms, "movie/lookup")
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
            payload.insert(
                "addOptions".to_string(),
                json!({
                    "searchForMovie": request.options.search
                }),
            );
            if !managed_tag_ids.is_empty() {
                let _ = merge_tags(&mut selected, &managed_tag_ids)?;
            }
        } else {
            bail!("movie payload must be an object");
        }

        let created = self.post_json("movie", &selected).await?;
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
            if is_elixir_managed_radarr_quality_profile(name) {
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

    async fn upsert_quality_profiles(&self, profiles: &[QualityProfileSpec]) -> Result<()> {
        if profiles.is_empty() {
            return Ok(());
        }
        let existing = self.get_json::<Vec<Value>>("qualityprofile").await?;
        let template = existing
            .first()
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

    async fn evaluate_tags(&self, tags: &[String]) -> Result<Vec<String>> {
        let (_ids, missing) = self.resolve_existing_tag_ids(tags).await?;
        Ok(missing)
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
    api_base.join(path).context("building radarr api url")
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

fn set_bool(target: &mut Value, field: &str, value: bool) -> Result<()> {
    if let Some(obj) = target.as_object_mut() {
        obj.insert(field.to_string(), Value::Bool(value));
        return Ok(());
    }
    bail!("payload must be an object");
}

fn set_i64(target: &mut Value, field: &str, value: i64) -> Result<()> {
    if let Some(obj) = target.as_object_mut() {
        obj.insert(field.to_string(), Value::Number(value.into()));
        return Ok(());
    }
    bail!("payload must be an object");
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

fn normalize_set(values: &[String]) -> HashSet<String> {
    values.iter().map(|value| normalize_name(value)).collect()
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
        let category_value = Value::String(category.clone());
        let has_category = set_field_value_optional(fields, "category", category_value.clone())?
            || set_field_value_optional(fields, "movieCategory", category_value.clone())?
            || set_field_value_optional(fields, "tvCategory", category_value)?;
        if !has_category {
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

fn find_id_by_name(items: &[Value], name: &str) -> Result<i64> {
    find_by_name(items, name)
        .and_then(|item| item.get("id").and_then(Value::as_i64))
        .ok_or_else(|| anyhow::anyhow!("'{}' not found", name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    fn value_for_field<'a>(fields: &'a [Value], name: &str) -> Option<&'a Value> {
        for field in fields {
            if field.get("name").and_then(Value::as_str) == Some(name) {
                return field.get("value");
            }
        }
        None
    }

    #[test]
    fn apply_downloader_fields_sets_base_url_category_and_api_key() -> Result<()> {
        let mut fields = vec![
            json!({"name": "baseUrl", "value": ""}),
            json!({"name": "apiKey", "value": ""}),
            json!({"name": "category", "value": ""}),
            json!({"name": "priority", "value": 0}),
        ];
        let mut settings = HashMap::new();
        settings.insert("priority".to_string(), json!(42));
        let spec = DownloaderSpec {
            name: "qBittorrent".to_string(),
            r#type: "qbittorrent".to_string(),
            url: "http://elx-qbittorrent:8080".to_string(),
            api_key: Some("secret-key".to_string()),
            category: Some("movies".to_string()),
            tags: Vec::new(),
            enabled: Some(true),
            settings,
        };

        apply_downloader_fields(&mut fields, &spec)?;

        assert_eq!(
            value_for_field(&fields, "baseUrl").cloned(),
            Some(json!("http://elx-qbittorrent:8080"))
        );
        assert_eq!(
            value_for_field(&fields, "apiKey").cloned(),
            Some(json!("secret-key"))
        );
        assert_eq!(
            value_for_field(&fields, "category").cloned(),
            Some(json!("movies"))
        );
        assert_eq!(
            value_for_field(&fields, "priority").cloned(),
            Some(json!(42))
        );

        Ok(())
    }

    #[test]
    fn quality_policy_score_map_evaluation_treats_missing_formats_as_drift() -> Result<()> {
        let formats = vec![CustomFormatSpec {
            name: "AV1".to_string(),
            include_custom_format_when_renaming: None,
            score: Some(1500),
            include: Vec::new(),
            exclude: Vec::new(),
            specifications: Vec::new(),
        }];

        let present =
            build_named_score_map_if_present(&formats, &[json!({ "id": 11, "name": "AV1" })])?;
        assert_eq!(
            present,
            Some(HashMap::from([(String::from("11"), 1500)]))
        );

        let missing = build_named_score_map_if_present(&formats, &[])?;
        assert_eq!(missing, None);

        Ok(())
    }

    #[test]
    fn apply_downloader_fields_sets_host_port_and_ssl_when_url_fields_missing() -> Result<()> {
        let mut fields = vec![
            json!({"name": "host", "value": ""}),
            json!({"name": "port", "value": 0}),
            json!({"name": "useSsl", "value": false}),
            json!({"name": "tvCategory", "value": ""}),
        ];
        let spec = DownloaderSpec {
            name: "qBittorrent".to_string(),
            r#type: "qbittorrent".to_string(),
            url: "https://elx-qbittorrent:9443".to_string(),
            api_key: None,
            category: Some("movies".to_string()),
            tags: Vec::new(),
            enabled: Some(true),
            settings: HashMap::new(),
        };

        apply_downloader_fields(&mut fields, &spec)?;

        assert_eq!(
            value_for_field(&fields, "host").cloned(),
            Some(json!("elx-qbittorrent"))
        );
        assert_eq!(value_for_field(&fields, "port").cloned(), Some(json!(9443)));
        assert_eq!(
            value_for_field(&fields, "useSsl").cloned(),
            Some(json!(true))
        );
        assert_eq!(
            value_for_field(&fields, "tvCategory").cloned(),
            Some(json!("movies"))
        );

        Ok(())
    }

    #[test]
    fn apply_downloader_fields_sets_movie_category_when_present() -> Result<()> {
        let mut fields = vec![
            json!({"name": "baseUrl", "value": ""}),
            json!({"name": "movieCategory", "value": ""}),
        ];
        let spec = DownloaderSpec {
            name: "qBittorrent".to_string(),
            r#type: "qbittorrent".to_string(),
            url: "http://elx-qbittorrent:8080".to_string(),
            api_key: None,
            category: Some("movies".to_string()),
            tags: Vec::new(),
            enabled: Some(true),
            settings: HashMap::new(),
        };

        apply_downloader_fields(&mut fields, &spec)?;

        assert_eq!(
            value_for_field(&fields, "movieCategory").cloned(),
            Some(json!("movies"))
        );

        Ok(())
    }

    #[test]
    fn merge_tags_preserves_existing_order_and_avoids_duplicates() -> Result<()> {
        let mut payload = json!({ "tags": [2, 1] });
        let changed = merge_tags(&mut payload, &[1, 3])?;
        assert!(changed);
        assert_eq!(payload.get("tags"), Some(&json!([2, 1, 3])));

        let changed = merge_tags(&mut payload, &[3])?;
        assert!(!changed);

        Ok(())
    }
}
