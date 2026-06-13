#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::fs;
use std::net::IpAddr;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use chrono::Utc;
use reqwest::header::{ETAG, HeaderMap, LAST_MODIFIED};
use reqwest::{Client, Url, redirect::Policy};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::extensions::store::{
    ExtensionStore, NewExtensionSourceModule, NewExtensionSourceModuleVersion,
    NewExtensionSourceRegistry, NewExtensionSourceReplacementRecommendation,
};

pub const CLOUDSTREAM_COMPAT_EXTENSION_ID: &str = "elixir.sources.cloudstream_compat";
pub const CLOUDSTREAM_RECOMMENDED_SOURCE_PACK_ID: &str =
    "elixir.sourcepacks.cloudstream.recommended";
pub const CLOUDSTREAM_RECOMMENDED_REGISTRY_KEY: &str = "cloudstream.recommended";
pub const CLOUDSTREAM_RECOMMENDED_SOURCE_PACK_PATH: &str =
    "source-packs/cloudstream-recommended.json";

const DEFAULT_FETCH_TIMEOUT: Duration = Duration::from_secs(12);
const DEFAULT_MAX_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
const DEFAULT_MAX_PLUGIN_LISTS: usize = 32;
const DEFAULT_MAX_PLUGINS: usize = 2_000;
const DEFAULT_MAX_REDIRECTS: usize = 5;
const BUNDLED_CLOUDSTREAM_RECOMMENDED_SOURCE_PACK: &str = include_str!(
    "../../../extensions/marketplace/cloudstream-compat-provider/source-packs/cloudstream-recommended.json"
);

#[derive(Debug, Clone)]
pub struct CloudStreamRegistryFetchConfig {
    pub timeout: Duration,
    pub max_redirects: usize,
    pub max_response_bytes: usize,
    pub max_plugin_lists: usize,
    pub max_plugins: usize,
    pub allow_private_hosts: bool,
}

impl Default for CloudStreamRegistryFetchConfig {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_FETCH_TIMEOUT,
            max_redirects: DEFAULT_MAX_REDIRECTS,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            max_plugin_lists: DEFAULT_MAX_PLUGIN_LISTS,
            max_plugins: DEFAULT_MAX_PLUGINS,
            allow_private_hosts: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloudStreamRegistryKind {
    RepoJson,
    PluginsJson,
}

impl CloudStreamRegistryKind {
    pub fn from_registry_type(registry_type: &str) -> Result<Self> {
        match registry_type.trim() {
            "cloudstream_repo_json" => Ok(Self::RepoJson),
            "cloudstream_plugins_json" => Ok(Self::PluginsJson),
            other => bail!("unsupported CloudStream registry type '{other}' for CS-2 parser"),
        }
    }

    pub fn as_registry_type(self) -> &'static str {
        match self {
            Self::RepoJson => "cloudstream_repo_json",
            Self::PluginsJson => "cloudstream_plugins_json",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CloudStreamRepositoryDescriptor {
    pub name: Option<String>,
    pub description: Option<String>,
    pub manifest_version: Option<String>,
    pub plugin_lists: Vec<String>,
    pub source_url: String,
    #[serde(default)]
    pub raw: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CloudStreamPluginListDescriptor {
    pub source_url: String,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub plugin_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CloudStreamSourceModuleDescriptor {
    pub module_id: String,
    pub display_name: String,
    pub internal_name: Option<String>,
    pub plugin_package: Option<String>,
    pub version: String,
    pub artifact_url: Option<String>,
    pub artifact_sha256: Option<String>,
    pub signature: Option<String>,
    pub media_types: Vec<String>,
    pub language_tags: Vec<String>,
    pub region_tags: Vec<String>,
    pub source_domains: Vec<String>,
    pub account_required: bool,
    pub unsupported: bool,
    pub unsupported_reason: Option<String>,
    pub status: Option<i64>,
    pub api_version: Option<String>,
    pub plugin_list_url: String,
    pub repository_url: Option<String>,
    pub raw: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CloudStreamRegistrySnapshot {
    pub registry_kind: String,
    pub source_url: String,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub repository: Option<CloudStreamRepositoryDescriptor>,
    pub plugin_lists: Vec<CloudStreamPluginListDescriptor>,
    pub modules: Vec<CloudStreamSourceModuleDescriptor>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct CloudStreamRegistryStoreInput {
    pub registry_id: Uuid,
    pub instance_id: Uuid,
    pub registry_key: String,
    pub registry_type: String,
    pub trust_class: String,
    pub display_name: Option<String>,
    pub url: Option<String>,
    pub enabled: bool,
    pub auto_refresh: bool,
    pub trusted_for_executable_updates: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CloudStreamRegistryPersistSummary {
    pub registries: usize,
    pub modules: usize,
    pub versions: usize,
    pub disabled_modules: usize,
    pub unsupported_modules: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CloudStreamRecommendedPackMigrationSummary {
    pub instances_seen: usize,
    pub migrated_instances: usize,
    pub skipped_existing_instances: usize,
    pub registries: usize,
    pub modules: usize,
    pub versions: usize,
    pub disabled_modules: usize,
    pub unsupported_modules: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CloudStreamSourcePackManifest {
    pub schema_version: u32,
    pub source_pack_id: String,
    pub name: String,
    pub version: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub registry_key: Option<String>,
    #[serde(default)]
    pub trust_class: Option<String>,
    #[serde(default)]
    pub enabled_by_default: bool,
    #[serde(default)]
    pub trusted_for_executable_updates: bool,
    #[serde(default)]
    pub update_manifest: Option<CloudStreamSourcePackUpdateManifest>,
    pub modules: Vec<Value>,
    #[serde(default)]
    pub replacement_recommendations: Vec<CloudStreamSourcePackReplacementRecommendation>,
    #[serde(default)]
    pub raw: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CloudStreamSourcePackUpdateManifest {
    pub url: Option<String>,
    pub signature: Option<CloudStreamSourcePackSignaturePolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CloudStreamSourcePackSignaturePolicy {
    pub algorithm: String,
    pub canonicalization: String,
    pub publisher_key_id: String,
    #[serde(default)]
    pub signature: Option<String>,
    #[serde(default)]
    pub signed_at: Option<String>,
    #[serde(default)]
    pub required_for_remote_updates: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CloudStreamSourcePackReplacementRecommendation {
    pub recommendation_key: String,
    pub action: String,
    pub source_module_id: String,
    #[serde(default)]
    pub replacement_module_id: Option<String>,
    #[serde(default)]
    pub recommended_version: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default = "default_true")]
    pub active: bool,
    #[serde(default)]
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone)]
struct FetchedText {
    url: String,
    text: String,
    etag: Option<String>,
    last_modified: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawRepoJson {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default, alias = "manifest_version")]
    manifest_version: Option<Value>,
    #[serde(default, alias = "plugin_lists")]
    plugin_lists: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawPluginEntry {
    #[serde(default, alias = "moduleId", alias = "id")]
    module_id: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default, alias = "internal_name")]
    internal_name: Option<String>,
    #[serde(default, alias = "plugin_package", alias = "packageName")]
    plugin_package: Option<String>,
    #[serde(default)]
    version: Option<Value>,
    #[serde(default, alias = "jar_url")]
    jar_url: Option<String>,
    #[serde(default)]
    url: Option<String>,
    #[serde(default, alias = "jar_hash")]
    jar_hash: Option<String>,
    #[serde(default, alias = "file_hash")]
    file_hash: Option<String>,
    #[serde(default)]
    signature: Option<String>,
    #[serde(default, alias = "jar_signature")]
    jar_signature: Option<String>,
    #[serde(default, alias = "api_version")]
    api_version: Option<Value>,
    #[serde(default, alias = "repository_url")]
    repository_url: Option<String>,
    #[serde(default, alias = "icon_url")]
    icon_url: Option<String>,
    #[serde(default, alias = "tv_types")]
    tv_types: Option<Value>,
    #[serde(default, alias = "media_types")]
    media_types: Option<Value>,
    #[serde(default)]
    status: Option<Value>,
    #[serde(default, alias = "requires_account", alias = "accountRequired")]
    requires_account: Option<bool>,
    #[serde(default, alias = "drm_required")]
    drm_required: Option<bool>,
    #[serde(default, alias = "captcha_required")]
    captcha_required: Option<bool>,
    #[serde(default, alias = "browser_required")]
    browser_required: Option<bool>,
    #[serde(default)]
    language: Option<Value>,
    #[serde(default, alias = "languages", alias = "language_tags")]
    language_tags: Option<Value>,
    #[serde(default)]
    region: Option<Value>,
    #[serde(default, alias = "regions", alias = "region_tags")]
    region_tags: Option<Value>,
    #[serde(
        default,
        alias = "sourceDomains",
        alias = "source_domains",
        alias = "domains"
    )]
    source_domains: Option<Value>,
    #[serde(default, alias = "mainUrl", alias = "main_url")]
    main_url: Option<String>,
    #[serde(default, alias = "baseUrl", alias = "base_url")]
    base_url: Option<String>,
}

pub struct CloudStreamRegistryClient {
    client: Client,
    config: CloudStreamRegistryFetchConfig,
}

impl CloudStreamRegistryClient {
    pub fn new(config: CloudStreamRegistryFetchConfig) -> Result<Self> {
        let client = Client::builder()
            .timeout(config.timeout)
            .redirect(Policy::limited(config.max_redirects))
            .build()
            .context("building CloudStream registry client")?;
        Ok(Self { client, config })
    }

    pub async fn fetch_registry(
        &self,
        registry_type: &str,
        url: &str,
    ) -> Result<CloudStreamRegistrySnapshot> {
        let kind = CloudStreamRegistryKind::from_registry_type(registry_type)?;
        let source_url = normalize_http_url(url, None, &self.config)
            .with_context(|| format!("validating CloudStream registry URL {url}"))?;
        match kind {
            CloudStreamRegistryKind::RepoJson => self.fetch_repo_json(&source_url).await,
            CloudStreamRegistryKind::PluginsJson => self.fetch_plugins_json(&source_url).await,
        }
    }

    async fn fetch_repo_json(&self, url: &str) -> Result<CloudStreamRegistrySnapshot> {
        let repo_fetch = self.fetch_text(url).await?;
        let repository = parse_repo_json(&repo_fetch.text, &repo_fetch.url, &self.config)?;
        let mut plugin_lists = Vec::new();
        let mut modules = Vec::new();
        let mut warnings = Vec::new();
        for plugin_list_url in &repository.plugin_lists {
            let plugin_fetch = self.fetch_text(plugin_list_url).await?;
            let parsed = parse_plugins_json(
                &plugin_fetch.text,
                &plugin_fetch.url,
                &self.config,
                &mut warnings,
            )?;
            plugin_lists.push(CloudStreamPluginListDescriptor {
                source_url: plugin_fetch.url,
                etag: plugin_fetch.etag,
                last_modified: plugin_fetch.last_modified,
                plugin_count: parsed.len(),
            });
            modules.extend(parsed);
        }
        let modules = dedupe_modules(modules, &mut warnings);
        Ok(CloudStreamRegistrySnapshot {
            registry_kind: CloudStreamRegistryKind::RepoJson
                .as_registry_type()
                .to_string(),
            source_url: repo_fetch.url,
            etag: repo_fetch.etag,
            last_modified: repo_fetch.last_modified,
            repository: Some(repository),
            plugin_lists,
            modules,
            warnings,
        })
    }

    async fn fetch_plugins_json(&self, url: &str) -> Result<CloudStreamRegistrySnapshot> {
        let plugin_fetch = self.fetch_text(url).await?;
        let mut warnings = Vec::new();
        let modules = parse_plugins_json(
            &plugin_fetch.text,
            &plugin_fetch.url,
            &self.config,
            &mut warnings,
        )?;
        let modules = dedupe_modules(modules, &mut warnings);
        Ok(CloudStreamRegistrySnapshot {
            registry_kind: CloudStreamRegistryKind::PluginsJson
                .as_registry_type()
                .to_string(),
            source_url: plugin_fetch.url.clone(),
            etag: plugin_fetch.etag.clone(),
            last_modified: plugin_fetch.last_modified.clone(),
            repository: None,
            plugin_lists: vec![CloudStreamPluginListDescriptor {
                source_url: plugin_fetch.url,
                etag: plugin_fetch.etag,
                last_modified: plugin_fetch.last_modified,
                plugin_count: modules.len(),
            }],
            modules,
            warnings,
        })
    }

    async fn fetch_text(&self, url: &str) -> Result<FetchedText> {
        let normalized = normalize_http_url(url, None, &self.config)?;
        let mut response = self
            .client
            .get(&normalized)
            .send()
            .await
            .with_context(|| format!("fetching CloudStream registry document {normalized}"))?;
        if !response.status().is_success() {
            bail!(
                "CloudStream registry document {} returned {}",
                normalized,
                response.status()
            );
        }
        if let Some(content_length) = response.content_length() {
            if content_length as usize > self.config.max_response_bytes {
                bail!(
                    "CloudStream registry document {} is too large: {} bytes exceeds {} bytes",
                    normalized,
                    content_length,
                    self.config.max_response_bytes
                );
            }
        }
        let final_url = response.url().to_string();
        let etag = header_text(response.headers(), ETAG.as_str());
        let last_modified = header_text(response.headers(), LAST_MODIFIED.as_str());
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .with_context(|| format!("reading CloudStream registry document {final_url}"))?
        {
            if bytes.len().saturating_add(chunk.len()) > self.config.max_response_bytes {
                bail!(
                    "CloudStream registry document {} exceeded {} bytes",
                    final_url,
                    self.config.max_response_bytes
                );
            }
            bytes.extend_from_slice(&chunk);
        }
        let text = String::from_utf8(bytes)
            .with_context(|| format!("CloudStream registry document {final_url} is not UTF-8"))?;
        Ok(FetchedText {
            url: final_url,
            text,
            etag,
            last_modified,
        })
    }
}

pub fn parse_repo_json(
    text: &str,
    source_url: &str,
    config: &CloudStreamRegistryFetchConfig,
) -> Result<CloudStreamRepositoryDescriptor> {
    let value: Value = serde_json::from_str(text).context("parsing CloudStream repo.json")?;
    if !value.is_object() {
        bail!("CloudStream repo.json must be a JSON object");
    }
    let raw: RawRepoJson =
        serde_json::from_value(value.clone()).context("normalizing CloudStream repo.json")?;
    let Some(plugin_lists_value) = raw.plugin_lists.as_array() else {
        bail!("CloudStream repo.json pluginLists must be an array");
    };
    if plugin_lists_value.len() > config.max_plugin_lists {
        bail!(
            "CloudStream repo.json contains {} plugin lists, exceeding limit {}",
            plugin_lists_value.len(),
            config.max_plugin_lists
        );
    }
    let mut plugin_lists = Vec::with_capacity(plugin_lists_value.len());
    for (index, entry) in plugin_lists_value.iter().enumerate() {
        let Some(entry) = entry.as_str().map(str::trim).filter(|s| !s.is_empty()) else {
            bail!("CloudStream repo.json pluginLists[{index}] must be a non-empty string");
        };
        let normalized = normalize_http_url(entry, Some(source_url), config)
            .with_context(|| format!("validating CloudStream plugin list URL {entry}"))?;
        plugin_lists.push(normalized);
    }
    Ok(CloudStreamRepositoryDescriptor {
        name: trim_option(raw.name),
        description: trim_option(raw.description),
        manifest_version: raw.manifest_version.as_ref().and_then(value_to_string),
        plugin_lists,
        source_url: source_url.to_string(),
        raw: value,
    })
}

pub fn parse_plugins_json(
    text: &str,
    plugin_list_url: &str,
    config: &CloudStreamRegistryFetchConfig,
    warnings: &mut Vec<String>,
) -> Result<Vec<CloudStreamSourceModuleDescriptor>> {
    let value: Value = serde_json::from_str(text).context("parsing CloudStream plugins.json")?;
    let Some(entries) = value.as_array() else {
        bail!("CloudStream plugins.json must be a JSON array");
    };
    if entries.len() > config.max_plugins {
        bail!(
            "CloudStream plugins.json contains {} plugins, exceeding limit {}",
            entries.len(),
            config.max_plugins
        );
    }
    let mut modules = Vec::with_capacity(entries.len());
    for (index, entry) in entries.iter().enumerate() {
        let module = normalize_plugin_entry(entry.clone(), index, plugin_list_url, config)
            .with_context(|| format!("normalizing CloudStream plugins.json entry {index}"))?;
        match module {
            Some(module) => modules.push(module),
            None => warnings.push(format!(
                "cloudstream_plugins_json:{}: skipped empty plugin entry {}",
                plugin_list_url, index
            )),
        }
    }
    Ok(modules)
}

pub fn parse_cloudstream_source_pack_manifest(
    text: &str,
    source_url: &str,
    config: &CloudStreamRegistryFetchConfig,
) -> Result<CloudStreamSourcePackManifest> {
    let value: Value =
        serde_json::from_str(text).context("parsing CloudStream source-pack manifest")?;
    if !value.is_object() {
        bail!("CloudStream source-pack manifest must be a JSON object");
    }
    let mut pack: CloudStreamSourcePackManifest = serde_json::from_value(value.clone())
        .context("normalizing CloudStream source-pack manifest")?;
    pack.raw = value;
    if pack.schema_version != 1 {
        bail!(
            "unsupported CloudStream source-pack schema version {}",
            pack.schema_version
        );
    }
    ensure_non_empty(&pack.source_pack_id, "sourcePackId")?;
    ensure_non_empty(&pack.name, "name")?;
    ensure_non_empty(&pack.version, "version")?;
    if pack.modules.len() > config.max_plugins {
        bail!(
            "CloudStream source-pack contains {} modules, exceeding limit {}",
            pack.modules.len(),
            config.max_plugins
        );
    }
    if let Some(trust_class) = pack.trust_class.as_deref() {
        match trust_class.trim() {
            "curated" | "maintainer_known" | "custom" => {}
            other => bail!("unsupported CloudStream source-pack trust class '{other}'"),
        }
    }
    if let Some(update_manifest) = pack.update_manifest.as_ref() {
        if let Some(url) = update_manifest.url.as_deref() {
            normalize_http_url(url, Some(source_url), config)
                .with_context(|| format!("validating source-pack update manifest URL {url}"))?;
        }
        if let Some(signature) = update_manifest.signature.as_ref() {
            ensure_non_empty(&signature.algorithm, "updateManifest.signature.algorithm")?;
            ensure_non_empty(
                &signature.canonicalization,
                "updateManifest.signature.canonicalization",
            )?;
            ensure_non_empty(
                &signature.publisher_key_id,
                "updateManifest.signature.publisherKeyId",
            )?;
        }
    }
    for recommendation in &pack.replacement_recommendations {
        ensure_non_empty(
            &recommendation.recommendation_key,
            "replacementRecommendations.recommendationKey",
        )?;
        ensure_non_empty(
            &recommendation.source_module_id,
            "replacementRecommendations.sourceModuleId",
        )?;
        match recommendation.action.trim() {
            "replace" => {
                if recommendation
                    .replacement_module_id
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .is_none()
                {
                    bail!("replace source-pack recommendations must include replacementModuleId");
                }
            }
            "disable" | "pin" | "none" => {}
            other => bail!("unsupported source-pack recommendation action '{other}'"),
        }
    }
    Ok(pack)
}

pub async fn seed_cloudstream_recommended_source_pack_for_instance(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    installed_package_dir: Option<&Path>,
) -> Result<CloudStreamRegistryPersistSummary> {
    let manifest_text = read_recommended_source_pack_manifest(installed_package_dir)?;
    let config = CloudStreamRegistryFetchConfig::default();
    let pack = parse_cloudstream_source_pack_manifest(
        &manifest_text,
        "https://elixir.media/source-packs/cloudstream/recommended.json",
        &config,
    )?;
    if pack.source_pack_id != CLOUDSTREAM_RECOMMENDED_SOURCE_PACK_ID {
        bail!(
            "bundled CloudStream source pack id '{}' does not match expected '{}'",
            pack.source_pack_id,
            CLOUDSTREAM_RECOMMENDED_SOURCE_PACK_ID
        );
    }
    persist_cloudstream_source_pack_manifest(
        store,
        instance_id,
        &pack,
        "bundled://elixir/cloudstream/recommended",
        &config,
    )
    .await
}

pub async fn migrate_cloudstream_recommended_source_pack_for_installed_instances(
    store: &ExtensionStore<'_>,
    installed_package_dir: Option<&Path>,
) -> Result<CloudStreamRecommendedPackMigrationSummary> {
    if store
        .get_extension(CLOUDSTREAM_COMPAT_EXTENSION_ID)
        .await?
        .is_none()
    {
        return Ok(CloudStreamRecommendedPackMigrationSummary::default());
    }

    let instances = store
        .list_instances(Some(CLOUDSTREAM_COMPAT_EXTENSION_ID))
        .await?;
    let mut summary = CloudStreamRecommendedPackMigrationSummary {
        instances_seen: instances.len(),
        ..CloudStreamRecommendedPackMigrationSummary::default()
    };

    for instance in instances {
        let registries = store
            .list_source_registries(Some(instance.instance_id))
            .await?;
        if registries
            .iter()
            .any(|registry| registry.registry_key == CLOUDSTREAM_RECOMMENDED_REGISTRY_KEY)
        {
            summary.skipped_existing_instances += 1;
            continue;
        }

        let seed = seed_cloudstream_recommended_source_pack_for_instance(
            store,
            instance.instance_id,
            installed_package_dir,
        )
        .await
        .with_context(|| {
            format!(
                "migrating CloudStream recommended source pack for instance {}",
                instance.instance_id
            )
        })?;
        summary.migrated_instances += 1;
        summary.registries += seed.registries;
        summary.modules += seed.modules;
        summary.versions += seed.versions;
        summary.disabled_modules += seed.disabled_modules;
        summary.unsupported_modules += seed.unsupported_modules;
    }

    Ok(summary)
}

pub async fn persist_cloudstream_source_pack_manifest(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    pack: &CloudStreamSourcePackManifest,
    source_url: &str,
    config: &CloudStreamRegistryFetchConfig,
) -> Result<CloudStreamRegistryPersistSummary> {
    if pack.schema_version != 1 {
        bail!(
            "unsupported CloudStream source-pack schema version {}",
            pack.schema_version
        );
    }
    let registry_key = pack
        .registry_key
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(CLOUDSTREAM_RECOMMENDED_REGISTRY_KEY);
    let trust_class = pack
        .trust_class
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("curated");
    let registry_id = deterministic_uuid(&format!(
        "elixir:cloudstream:source-pack-registry:{instance_id}:{registry_key}"
    ));
    store
        .upsert_source_registry(&NewExtensionSourceRegistry {
            registry_id,
            instance_id,
            registry_key: registry_key.to_string(),
            registry_type: "elixir_curated_cloudstream_pack".to_string(),
            trust_class: trust_class.to_string(),
            display_name: pack.name.clone(),
            url: pack
                .update_manifest
                .as_ref()
                .and_then(|manifest| manifest.url.clone()),
            enabled: true,
            auto_refresh: true,
            trusted_for_executable_updates: pack.trusted_for_executable_updates,
            etag: None,
            last_modified: None,
            metadata_json: Some(json!({
                "cloudstreamSourcePack": {
                    "sourcePackId": pack.source_pack_id,
                    "version": pack.version,
                    "description": pack.description,
                    "sourceUrl": source_url,
                    "updateManifest": pack.update_manifest,
                    "replacementRecommendations": pack.replacement_recommendations,
                    "raw": pack.raw,
                }
            })),
        })
        .await?;
    store
        .record_source_registry_fetch(registry_id, "success", None, None, None)
        .await?;

    let mut warnings = Vec::new();
    let mut modules = Vec::with_capacity(pack.modules.len());
    for (index, raw_module) in pack.modules.iter().enumerate() {
        let module = normalize_plugin_entry(raw_module.clone(), index, source_url, config)
            .with_context(|| {
                format!(
                    "normalizing CloudStream source-pack module {}",
                    index.saturating_add(1)
                )
            })?;
        match module {
            Some(module) => modules.push(module),
            None => warnings.push(format!(
                "cloudstream_source_pack:{}: skipped empty module entry {}",
                pack.source_pack_id, index
            )),
        }
    }
    let modules = dedupe_modules(modules, &mut warnings);
    let existing_modules = store.list_source_modules(Some(instance_id), None).await?;
    let existing_by_key: HashMap<String, _> = existing_modules
        .into_iter()
        .map(|module| (module.module_key.clone(), module))
        .collect();
    let recommendation_key_by_module = recommendation_keys_by_module(registry_key, pack);
    let now = Utc::now();
    let mut summary = CloudStreamRegistryPersistSummary {
        registries: 1,
        ..Default::default()
    };

    for module in &modules {
        let module_key = source_module_key(registry_key, &module.module_id);
        let source_module_id = deterministic_uuid(&format!(
            "elixir:cloudstream:source-pack-module:{instance_id}:{module_key}"
        ));
        let existing = existing_by_key.get(&module_key);
        let default_enabled =
            pack.enabled_by_default && !module.unsupported && !module.account_required;
        let module_enabled = if module.unsupported || module.account_required {
            false
        } else {
            existing
                .map(|module| module.enabled)
                .unwrap_or(default_enabled)
        };
        let installed = existing
            .map(|module| module.installed)
            .unwrap_or(default_enabled);
        let can_activate = installed && !module.unsupported && !module.account_required;
        let pinned_version = existing.and_then(|module| module.pinned_version.clone());
        let active_version = if can_activate {
            pinned_version
                .clone()
                .or_else(|| Some(module.version.clone()))
        } else {
            None
        };
        let previous_active_version = existing.and_then(|module| module.active_version.clone());
        let rollback_version = match (&previous_active_version, &active_version) {
            (Some(previous), Some(active)) if previous != active => Some(previous.clone()),
            _ => existing.and_then(|module| module.rollback_version.clone()),
        };
        let health_state = source_pack_module_health_state(module, module_enabled);
        if !module_enabled {
            summary.disabled_modules += 1;
        }
        if module.unsupported {
            summary.unsupported_modules += 1;
        }
        store
            .upsert_source_module(&NewExtensionSourceModule {
                source_module_id,
                instance_id,
                registry_id,
                module_key: module_key.clone(),
                display_name: module.display_name.clone(),
                ecosystem: "cloudstream".to_string(),
                plugin_package: module.plugin_package.clone(),
                active_version: active_version.clone(),
                rollback_version: rollback_version.clone(),
                media_types_json: Some(json!(module.media_types)),
                language_tags_json: Some(json!(module.language_tags)),
                region_tags_json: Some(json!(module.region_tags)),
                source_domains_json: Some(json!(module.source_domains)),
                account_required: module.account_required,
                unsupported: module.unsupported,
                unsupported_reason: module.unsupported_reason.clone(),
                enabled: module_enabled,
                installed,
                pinned_version,
                health_state: health_state.to_string(),
                replacement_recommendation_key: recommendation_key_by_module
                    .get(&module_key)
                    .cloned()
                    .or_else(|| {
                        existing.and_then(|module| module.replacement_recommendation_key.clone())
                    }),
                last_error: module.unsupported_reason.clone(),
                metadata_json: Some(json!({
                    "cloudstream": module,
                    "sourcePackId": pack.source_pack_id,
                    "sourcePackVersion": pack.version,
                    "registryKey": registry_key,
                    "sourcePackWarnings": warnings,
                })),
            })
            .await?;
        summary.modules += 1;

        let existing_versions = if existing.is_some() {
            store.list_source_module_versions(source_module_id).await?
        } else {
            Vec::new()
        };
        if previous_active_version.as_deref() != active_version.as_deref() {
            if let Some(previous_active) = previous_active_version.as_deref() {
                if let Some(previous_version) = existing_versions
                    .iter()
                    .find(|version| version.version == previous_active)
                {
                    store
                        .set_source_module_version_state(
                            previous_version.version_id,
                            "installed",
                            &previous_version.smoke_status,
                            previous_version.smoke_error.as_deref(),
                        )
                        .await?;
                }
            }
        }
        let rollback_of_version_id =
            previous_active_version
                .as_deref()
                .and_then(|previous_active| {
                    if active_version.as_deref() == Some(previous_active) {
                        None
                    } else {
                        existing_versions
                            .iter()
                            .find(|version| version.version == previous_active)
                            .map(|version| version.version_id)
                    }
                });
        let version_id = deterministic_uuid(&format!(
            "elixir:cloudstream:source-pack-module-version:{source_module_id}:{}:{}",
            module.version,
            module.artifact_url.as_deref().unwrap_or("")
        ));
        let install_state = if active_version.as_deref() == Some(module.version.as_str()) {
            "active"
        } else if installed {
            "installed"
        } else {
            "available"
        };
        store
            .upsert_source_module_version(&NewExtensionSourceModuleVersion {
                version_id,
                source_module_id,
                version: module.version.clone(),
                artifact_url: module.artifact_url.clone(),
                artifact_sha256: module.artifact_sha256.clone(),
                signature: module.signature.clone(),
                install_state: install_state.to_string(),
                smoke_status: "unknown".to_string(),
                smoke_error: None,
                rollback_of_version_id,
                installed_at: (install_state == "active" || install_state == "installed")
                    .then_some(now),
                activated_at: (install_state == "active").then_some(now),
                metadata_json: Some(json!({
                    "cloudstream": {
                        "apiVersion": module.api_version,
                        "status": module.status,
                        "repositoryUrl": module.repository_url,
                        "pluginListUrl": module.plugin_list_url,
                        "raw": module.raw,
                    },
                    "sourcePackId": pack.source_pack_id,
                    "sourcePackVersion": pack.version,
                })),
            })
            .await?;
        summary.versions += 1;
    }

    persist_source_pack_replacement_recommendations(
        store,
        instance_id,
        registry_id,
        registry_key,
        pack,
    )
    .await?;
    Ok(summary)
}

pub async fn apply_cloudstream_source_replacement_recommendation(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    recommendation_key: &str,
) -> Result<bool> {
    let recommendation_key = recommendation_key.trim();
    if recommendation_key.is_empty() {
        bail!("recommendation key must not be empty");
    }
    let modules = store.list_source_modules(Some(instance_id), None).await?;
    let module_ids: HashSet<Uuid> = modules
        .iter()
        .map(|module| module.source_module_id)
        .collect();
    let replacement_by_id: HashMap<Uuid, _> = modules
        .iter()
        .map(|module| (module.source_module_id, module))
        .collect();
    let recommendations = store
        .list_source_replacement_recommendations(None, true)
        .await?;
    let Some(recommendation) = recommendations.into_iter().find(|recommendation| {
        recommendation.recommendation_key == recommendation_key
            && module_ids.contains(&recommendation.source_module_id)
    }) else {
        return Ok(false);
    };
    match recommendation.action.as_str() {
        "replace" => {
            let Some(replacement_source_module_id) = recommendation.replacement_source_module_id
            else {
                bail!("replace recommendation '{recommendation_key}' has no replacement module");
            };
            let Some(replacement) = replacement_by_id.get(&replacement_source_module_id) else {
                bail!(
                    "replace recommendation '{recommendation_key}' references missing replacement module"
                );
            };
            if replacement.unsupported {
                bail!(
                    "replace recommendation '{}' references unsupported replacement module '{}'",
                    recommendation_key,
                    replacement.display_name
                );
            }
            store
                .set_source_module_enabled_state(
                    recommendation.source_module_id,
                    false,
                    "disabled",
                    recommendation.reason.as_deref(),
                )
                .await?;
            store
                .set_source_module_enabled_state(
                    replacement_source_module_id,
                    true,
                    "available",
                    None,
                )
                .await?;
        }
        "disable" => {
            store
                .set_source_module_enabled_state(
                    recommendation.source_module_id,
                    false,
                    "disabled",
                    recommendation.reason.as_deref(),
                )
                .await?;
        }
        "pin" => {
            if let Some(version) = recommendation.recommended_version.as_deref() {
                store
                    .set_source_module_active_version(
                        recommendation.source_module_id,
                        Some(version),
                        None,
                    )
                    .await?;
            }
        }
        "none" => {}
        other => bail!("unsupported source replacement action '{other}'"),
    }
    store
        .mark_source_replacement_recommendation_applied(recommendation.recommendation_id)
        .await?;
    Ok(true)
}

pub async fn persist_cloudstream_registry_snapshot(
    store: &ExtensionStore<'_>,
    input: &CloudStreamRegistryStoreInput,
    snapshot: &CloudStreamRegistrySnapshot,
) -> Result<CloudStreamRegistryPersistSummary> {
    let registry_type = CloudStreamRegistryKind::from_registry_type(&input.registry_type)?;
    if registry_type.as_registry_type() != snapshot.registry_kind {
        bail!(
            "CloudStream snapshot kind '{}' does not match registry type '{}'",
            snapshot.registry_kind,
            input.registry_type
        );
    }
    let display_name = input
        .display_name
        .as_deref()
        .or_else(|| {
            snapshot
                .repository
                .as_ref()
                .and_then(|repo| repo.name.as_deref())
        })
        .unwrap_or(input.registry_key.as_str());
    store
        .upsert_source_registry(&NewExtensionSourceRegistry {
            registry_id: input.registry_id,
            instance_id: input.instance_id,
            registry_key: input.registry_key.clone(),
            registry_type: input.registry_type.clone(),
            trust_class: input.trust_class.clone(),
            display_name: display_name.to_string(),
            url: input
                .url
                .clone()
                .or_else(|| Some(snapshot.source_url.clone())),
            enabled: input.enabled,
            auto_refresh: input.auto_refresh,
            trusted_for_executable_updates: input.trusted_for_executable_updates,
            etag: snapshot.etag.clone(),
            last_modified: snapshot.last_modified.clone(),
            metadata_json: Some(json!({
                "cloudstream": {
                    "sourceUrl": snapshot.source_url,
                    "repository": snapshot.repository,
                    "pluginLists": snapshot.plugin_lists,
                    "warnings": snapshot.warnings,
                }
            })),
        })
        .await?;
    store
        .record_source_registry_fetch(
            input.registry_id,
            "success",
            None,
            snapshot.etag.as_deref(),
            snapshot.last_modified.as_deref(),
        )
        .await?;

    let mut summary = CloudStreamRegistryPersistSummary {
        registries: 1,
        ..Default::default()
    };
    for module in &snapshot.modules {
        let module_key = source_module_key(&input.registry_key, &module.module_id);
        let source_module_id = deterministic_uuid(&format!(
            "elixir:cloudstream:module:{}:{module_key}",
            input.instance_id
        ));
        let module_enabled = input.enabled && !module.unsupported && input.trust_class != "custom";
        let health_state = if module.unsupported {
            "unsupported"
        } else if module_enabled {
            "available"
        } else {
            "disabled"
        };
        if !module_enabled {
            summary.disabled_modules += 1;
        }
        if module.unsupported {
            summary.unsupported_modules += 1;
        }
        store
            .upsert_source_module(&NewExtensionSourceModule {
                source_module_id,
                instance_id: input.instance_id,
                registry_id: input.registry_id,
                module_key: module_key.clone(),
                display_name: module.display_name.clone(),
                ecosystem: "cloudstream".to_string(),
                plugin_package: module.plugin_package.clone(),
                active_version: None,
                rollback_version: None,
                media_types_json: Some(json!(module.media_types)),
                language_tags_json: Some(json!(module.language_tags)),
                region_tags_json: Some(json!(module.region_tags)),
                source_domains_json: Some(json!(module.source_domains)),
                account_required: module.account_required,
                unsupported: module.unsupported,
                unsupported_reason: module.unsupported_reason.clone(),
                enabled: module_enabled,
                installed: false,
                pinned_version: None,
                health_state: health_state.to_string(),
                replacement_recommendation_key: None,
                last_error: module.unsupported_reason.clone(),
                metadata_json: Some(json!({
                    "cloudstream": module,
                    "registryKey": input.registry_key,
                    "pluginListUrl": module.plugin_list_url,
                })),
            })
            .await?;
        summary.modules += 1;

        let version_id = deterministic_uuid(&format!(
            "elixir:cloudstream:module-version:{source_module_id}:{}:{}",
            module.version,
            module.artifact_url.as_deref().unwrap_or("")
        ));
        store
            .upsert_source_module_version(&NewExtensionSourceModuleVersion {
                version_id,
                source_module_id,
                version: module.version.clone(),
                artifact_url: module.artifact_url.clone(),
                artifact_sha256: module.artifact_sha256.clone(),
                signature: module.signature.clone(),
                install_state: "available".to_string(),
                smoke_status: "unknown".to_string(),
                smoke_error: None,
                rollback_of_version_id: None,
                installed_at: None,
                activated_at: None,
                metadata_json: Some(json!({
                    "cloudstream": {
                        "apiVersion": module.api_version,
                        "status": module.status,
                        "repositoryUrl": module.repository_url,
                        "pluginListUrl": module.plugin_list_url,
                        "raw": module.raw,
                    }
                })),
            })
            .await?;
        summary.versions += 1;
    }
    Ok(summary)
}

fn read_recommended_source_pack_manifest(installed_package_dir: Option<&Path>) -> Result<String> {
    if let Some(package_dir) = installed_package_dir {
        let path = package_dir.join(CLOUDSTREAM_RECOMMENDED_SOURCE_PACK_PATH);
        if path.exists() {
            return fs::read_to_string(&path)
                .with_context(|| format!("reading CloudStream source pack {}", path.display()));
        }
    }
    Ok(BUNDLED_CLOUDSTREAM_RECOMMENDED_SOURCE_PACK.to_string())
}

fn source_pack_module_health_state(
    module: &CloudStreamSourceModuleDescriptor,
    enabled: bool,
) -> &'static str {
    if module.unsupported {
        "unsupported"
    } else if module.account_required {
        "account_required"
    } else if enabled {
        "available"
    } else {
        "disabled"
    }
}

fn recommendation_keys_by_module(
    registry_key: &str,
    pack: &CloudStreamSourcePackManifest,
) -> HashMap<String, String> {
    pack.replacement_recommendations
        .iter()
        .filter(|recommendation| recommendation.active)
        .map(|recommendation| {
            (
                source_module_key(registry_key, &recommendation.source_module_id),
                recommendation.recommendation_key.clone(),
            )
        })
        .collect()
}

async fn persist_source_pack_replacement_recommendations(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    registry_id: Uuid,
    registry_key: &str,
    pack: &CloudStreamSourcePackManifest,
) -> Result<()> {
    if pack.replacement_recommendations.is_empty() {
        return Ok(());
    }
    let modules = store
        .list_source_modules(Some(instance_id), Some(registry_id))
        .await?;
    let module_by_key: HashMap<String, _> = modules
        .iter()
        .map(|module| (module.module_key.clone(), module))
        .collect();
    for recommendation in &pack.replacement_recommendations {
        let action = recommendation.action.trim().to_ascii_lowercase();
        let source_key = source_module_key(registry_key, &recommendation.source_module_id);
        let Some(source_module) = module_by_key.get(&source_key) else {
            bail!(
                "source-pack recommendation '{}' references missing source module '{}'",
                recommendation.recommendation_key,
                recommendation.source_module_id
            );
        };
        let replacement_source_module_id = if action == "replace" {
            let replacement_module_id = recommendation
                .replacement_module_id
                .as_deref()
                .context("replace recommendation missing replacementModuleId")?;
            let replacement_module_key = source_module_key(registry_key, replacement_module_id);
            let Some(replacement_module) = module_by_key.get(&replacement_module_key) else {
                bail!(
                    "source-pack recommendation '{}' references missing replacement module '{}'",
                    recommendation.recommendation_key,
                    replacement_module_id
                );
            };
            Some(replacement_module.source_module_id)
        } else {
            None
        };
        let recommendation_id = deterministic_uuid(&format!(
            "elixir:cloudstream:source-pack-recommendation:{}:{}",
            source_module.source_module_id, recommendation.recommendation_key
        ));
        store
            .upsert_source_replacement_recommendation(
                &NewExtensionSourceReplacementRecommendation {
                    recommendation_id,
                    source_module_id: source_module.source_module_id,
                    replacement_source_module_id,
                    replacement_registry_id: Some(registry_id),
                    recommendation_key: recommendation.recommendation_key.clone(),
                    action,
                    recommended_version: recommendation.recommended_version.clone(),
                    reason: recommendation.reason.clone(),
                    metadata_json: Some(json!({
                        "cloudstreamSourcePack": {
                            "sourcePackId": pack.source_pack_id,
                            "sourcePackVersion": pack.version,
                            "recommendation": recommendation,
                            "metadata": recommendation.metadata,
                        }
                    })),
                    active: recommendation.active,
                },
            )
            .await?;
        if recommendation.active {
            store
                .set_source_module_replacement_recommendation_key(
                    source_module.source_module_id,
                    Some(&recommendation.recommendation_key),
                )
                .await?;
        }
    }
    Ok(())
}

fn normalize_plugin_entry(
    value: Value,
    index: usize,
    plugin_list_url: &str,
    config: &CloudStreamRegistryFetchConfig,
) -> Result<Option<CloudStreamSourceModuleDescriptor>> {
    if value.is_null() {
        return Ok(None);
    }
    if !value.is_object() {
        bail!("CloudStream plugin entry must be an object");
    }
    let raw: RawPluginEntry =
        serde_json::from_value(value.clone()).context("deserializing CloudStream plugin entry")?;
    let display_name = first_non_empty([raw.name.as_deref(), raw.internal_name.as_deref()])
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("CloudStream Source {}", index + 1));
    let internal_name = trim_option(raw.internal_name.clone());
    let plugin_package = trim_option(raw.plugin_package.clone()).or_else(|| internal_name.clone());
    let module_seed = first_non_empty([
        raw.module_id.as_deref(),
        plugin_package.as_deref(),
        Some(display_name.as_str()),
    ])
    .unwrap_or(display_name.as_str());
    let module_id = stable_text_id(module_seed);
    let status = raw.status.as_ref().and_then(value_to_i64);
    let version = raw
        .version
        .as_ref()
        .and_then(value_to_string)
        .unwrap_or_else(|| "0".to_string());
    let api_version = raw.api_version.as_ref().and_then(value_to_string);
    let artifact_url = first_non_empty([raw.jar_url.as_deref(), raw.url.as_deref()])
        .map(|url| normalize_http_url(url, Some(plugin_list_url), config));
    let mut unsupported_reasons = Vec::new();
    let artifact_url = match artifact_url {
        Some(Ok(url)) => Some(url),
        Some(Err(err)) => {
            unsupported_reasons.push(format!("unsafe artifact URL: {err}"));
            None
        }
        None => None,
    };
    let media_types =
        normalize_media_types(first_value(raw.media_types.as_ref(), raw.tv_types.as_ref()));
    if media_types.is_empty() {
        unsupported_reasons.push("no supported CloudStream media types".to_string());
    }
    if let Some(status) = status {
        if status != 1 {
            unsupported_reasons.push(format!("CloudStream plugin status is {status}"));
        }
    }
    if raw.drm_required.unwrap_or(false) {
        unsupported_reasons.push("DRM-protected source module".to_string());
    }
    if raw.captcha_required.unwrap_or(false) {
        unsupported_reasons.push("captcha-required source module".to_string());
    }
    if raw.browser_required.unwrap_or(false) {
        unsupported_reasons.push("browser-automation source module".to_string());
    }
    let source_domains = normalize_source_domains(&raw, config);
    let language_tags = normalize_string_tags(first_value(
        raw.language_tags.as_ref(),
        raw.language.as_ref(),
    ));
    let region_tags =
        normalize_string_tags(first_value(raw.region_tags.as_ref(), raw.region.as_ref()));
    let unsupported = !unsupported_reasons.is_empty();
    Ok(Some(CloudStreamSourceModuleDescriptor {
        module_id,
        display_name,
        internal_name: plugin_package.clone(),
        plugin_package,
        version,
        artifact_url,
        artifact_sha256: first_non_empty([raw.jar_hash.as_deref(), raw.file_hash.as_deref()])
            .map(normalize_checksum),
        signature: first_non_empty([raw.jar_signature.as_deref(), raw.signature.as_deref()])
            .map(ToOwned::to_owned),
        media_types,
        language_tags,
        region_tags,
        source_domains,
        account_required: raw.requires_account.unwrap_or(false),
        unsupported,
        unsupported_reason: if unsupported {
            Some(unsupported_reasons.join("; "))
        } else {
            None
        },
        status,
        api_version,
        plugin_list_url: plugin_list_url.to_string(),
        repository_url: raw.repository_url.clone(),
        raw: value,
    }))
}

fn dedupe_modules(
    modules: Vec<CloudStreamSourceModuleDescriptor>,
    warnings: &mut Vec<String>,
) -> Vec<CloudStreamSourceModuleDescriptor> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::with_capacity(modules.len());
    for module in modules {
        let key = module.module_id.to_ascii_lowercase();
        if !seen.insert(key.clone()) {
            warnings.push(format!(
                "cloudstream_plugins_json:{}: duplicate source module id '{}' ignored",
                module.plugin_list_url, module.module_id
            ));
            continue;
        }
        deduped.push(module);
    }
    deduped.sort_by(|left, right| left.module_id.cmp(&right.module_id));
    deduped
}

fn normalize_http_url(
    input: &str,
    base_url: Option<&str>,
    config: &CloudStreamRegistryFetchConfig,
) -> Result<String> {
    let input = input.trim();
    if input.is_empty() {
        bail!("URL is empty");
    }
    let url = match Url::parse(input) {
        Ok(url) => url,
        Err(err) => {
            let Some(base_url) = base_url else {
                return Err(err).context("parsing URL");
            };
            let base = Url::parse(base_url).context("parsing base URL")?;
            base.join(input)
                .with_context(|| format!("resolving relative URL {input} against {base_url}"))?
        }
    };
    validate_safe_http_url(&url, config.allow_private_hosts)?;
    Ok(url.to_string())
}

fn validate_safe_http_url(url: &Url, allow_private_hosts: bool) -> Result<()> {
    match url.scheme() {
        "http" | "https" => {}
        scheme => bail!("URL scheme '{scheme}' is not allowed"),
    }
    if !url.username().is_empty() || url.password().is_some() {
        bail!("URL credentials are not allowed");
    }
    let Some(host) = url
        .host_str()
        .map(str::trim)
        .filter(|host| !host.is_empty())
    else {
        bail!("URL host is required");
    };
    if allow_private_hosts {
        return Ok(());
    }
    let lower = host
        .trim_matches('[')
        .trim_matches(']')
        .to_ascii_lowercase();
    if lower == "localhost" || lower.ends_with(".localhost") || lower.ends_with(".local") {
        bail!("private or local host '{host}' is not allowed");
    }
    if let Ok(ip) = lower.parse::<IpAddr>() {
        match ip {
            IpAddr::V4(ip) => {
                if ip.is_private()
                    || ip.is_loopback()
                    || ip.is_link_local()
                    || ip.is_broadcast()
                    || ip.is_documentation()
                    || ip.octets()[0] == 0
                {
                    bail!("private or local IP address '{ip}' is not allowed");
                }
            }
            IpAddr::V6(ip) => {
                if ip.is_loopback()
                    || ip.is_unique_local()
                    || ip.is_unicast_link_local()
                    || ip.is_unspecified()
                    || (ip.segments()[0] & 0xffc0) == 0xfe80
                {
                    bail!("private or local IP address '{ip}' is not allowed");
                }
            }
        }
    }
    Ok(())
}

fn normalize_media_types(value: Option<&Value>) -> Vec<String> {
    let mut output = Vec::new();
    for tag in normalize_string_tags(value) {
        let lower = tag.to_ascii_lowercase();
        let mapped = if lower.contains("anime") || lower == "ova" {
            Some("anime")
        } else if lower.contains("movie") || lower == "film" {
            Some("movie")
        } else if lower.contains("tv")
            || lower.contains("series")
            || lower.contains("drama")
            || lower.contains("cartoon")
            || lower.contains("documentary")
        {
            Some("tv")
        } else {
            None
        };
        if let Some(mapped) = mapped {
            push_unique(&mut output, mapped.to_string());
        }
    }
    output
}

fn normalize_source_domains(
    raw: &RawPluginEntry,
    config: &CloudStreamRegistryFetchConfig,
) -> Vec<String> {
    let mut domains = normalize_string_tags(raw.source_domains.as_ref());
    for maybe_url in [raw.main_url.as_deref(), raw.base_url.as_deref()]
        .into_iter()
        .flatten()
    {
        if let Some(host) = safe_host_from_url(maybe_url, config) {
            push_unique(&mut domains, host);
        }
    }
    if let Some(icon_url) = raw.icon_url.as_deref() {
        if let Ok(url) = Url::parse(icon_url) {
            for (key, value) in url.query_pairs() {
                if key.eq_ignore_ascii_case("domain") {
                    let domain = value.trim().trim_start_matches("www.").to_ascii_lowercase();
                    if !domain.is_empty() {
                        push_unique(&mut domains, domain);
                    }
                }
            }
        }
    }
    domains.sort();
    domains
}

fn safe_host_from_url(url: &str, config: &CloudStreamRegistryFetchConfig) -> Option<String> {
    let normalized = normalize_http_url(url, None, config).ok()?;
    let parsed = Url::parse(&normalized).ok()?;
    parsed
        .host_str()
        .map(|host| host.trim_start_matches("www.").to_ascii_lowercase())
}

fn normalize_string_tags(value: Option<&Value>) -> Vec<String> {
    let mut output = Vec::new();
    match value {
        Some(Value::Array(items)) => {
            for item in items {
                if let Some(text) = value_to_string(item) {
                    push_unique(&mut output, text);
                }
            }
        }
        Some(value) => {
            if let Some(text) = value_to_string(value) {
                push_unique(&mut output, text);
            }
        }
        None => {}
    }
    output
}

fn first_value<'a>(left: Option<&'a Value>, right: Option<&'a Value>) -> Option<&'a Value> {
    left.or(right)
}

fn first_non_empty<'a, I>(values: I) -> Option<&'a str>
where
    I: IntoIterator<Item = Option<&'a str>>,
{
    values
        .into_iter()
        .flatten()
        .map(str::trim)
        .find(|value| !value.is_empty())
}

fn trim_option(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => {
            let trimmed = value.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_string())
        }
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

fn value_to_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number
            .as_i64()
            .or_else(|| number.as_u64().and_then(|value| i64::try_from(value).ok())),
        Value::String(value) => value.trim().parse::<i64>().ok(),
        _ => None,
    }
}

fn normalize_checksum(value: &str) -> String {
    let trimmed = value.trim();
    trimmed
        .strip_prefix("sha256-")
        .unwrap_or(trimmed)
        .to_string()
}

fn stable_text_id(value: &str) -> String {
    let mut output = String::new();
    let mut last_dash = false;
    for ch in value.trim().chars() {
        if ch.is_ascii_alphanumeric() {
            output.push(ch.to_ascii_lowercase());
            last_dash = false;
        } else if !last_dash && !output.is_empty() {
            output.push('-');
            last_dash = true;
        }
    }
    while output.ends_with('-') {
        output.pop();
    }
    if output.is_empty() {
        "source".to_string()
    } else {
        output
    }
}

fn source_module_key(registry_key: &str, module_id: &str) -> String {
    format!(
        "cloudstream:{}:{}",
        stable_text_id(registry_key),
        stable_text_id(module_id)
    )
}

fn deterministic_uuid(seed: &str) -> Uuid {
    Uuid::new_v5(&Uuid::NAMESPACE_URL, seed.as_bytes())
}

fn push_unique(values: &mut Vec<String>, value: String) {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return;
    }
    if !values
        .iter()
        .any(|existing| existing.eq_ignore_ascii_case(trimmed))
    {
        values.push(trimmed.to_string());
    }
}

fn default_true() -> bool {
    true
}

fn ensure_non_empty(value: &str, field: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{field} must not be empty");
    }
    Ok(())
}

fn header_text(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DatabaseConfig;
    use crate::db::Database;
    use crate::db::models::{ExtensionKind, ExtensionTrustLevel};
    use crate::extensions::store::{NewExtension, NewExtensionInstance};
    use axum::Router;
    use axum::body::Body;
    use axum::http::{HeaderValue, Response, StatusCode};
    use axum::routing::get;
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;

    const CS13_FIXTURE_REPO_JSON: &str = include_str!(
        "../../../extensions/marketplace/cloudstream-compat-provider/fixtures/cloudstream-fixture-repo.json"
    );
    const CS13_FIXTURE_PLUGINS_JSON: &str = include_str!(
        "../../../extensions/marketplace/cloudstream-compat-provider/fixtures/cloudstream-fixture-plugins.json"
    );
    const CS13_FIXTURE_SOURCE_PACK_JSON: &str = include_str!(
        "../../../extensions/marketplace/cloudstream-compat-provider/fixtures/cloudstream-fixture-source-pack.json"
    );

    fn test_config() -> CloudStreamRegistryFetchConfig {
        CloudStreamRegistryFetchConfig {
            allow_private_hosts: true,
            max_response_bytes: 4096,
            max_plugins: 16,
            max_plugin_lists: 4,
            ..CloudStreamRegistryFetchConfig::default()
        }
    }

    #[test]
    fn cs2_rejects_malformed_repo_json() {
        let err = parse_repo_json(
            r#"{"name":"broken"}"#,
            "https://repo.example/repo.json",
            &test_config(),
        )
        .expect_err("missing pluginLists must fail");
        assert!(err.to_string().contains("pluginLists"));
    }

    #[test]
    fn cs2_dedupes_duplicate_module_ids() -> Result<()> {
        let mut warnings = Vec::new();
        let modules = parse_plugins_json(
            r#"[
                {
                    "internalName": "FixtureProvider",
                    "name": "Fixture",
                    "version": 7,
                    "jarUrl": "https://repo.example/fixture.jar",
                    "jarHash": "sha256-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "status": 1,
                    "tvTypes": ["Movie", "TvSeries"]
                },
                {
                    "internalName": "FixtureProvider",
                    "name": "Fixture Mirror",
                    "version": 8,
                    "jarUrl": "https://repo.example/fixture2.jar",
                    "status": 1,
                    "tvTypes": ["Movie"]
                }
            ]"#,
            "https://repo.example/plugins.json",
            &test_config(),
            &mut warnings,
        )?;
        let modules = dedupe_modules(modules, &mut warnings);
        assert_eq!(modules.len(), 1);
        assert_eq!(modules[0].module_id, "fixtureprovider");
        assert_eq!(
            modules[0].artifact_sha256.as_deref(),
            Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        );
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("duplicate source module id"))
        );
        Ok(())
    }

    #[test]
    fn cs2_rejects_unsafe_registry_url() {
        let err = normalize_http_url(
            "file:///tmp/plugins.json",
            None,
            &CloudStreamRegistryFetchConfig::default(),
        )
        .expect_err("file URLs must be rejected");
        assert!(err.to_string().contains("scheme"));

        let err = normalize_http_url(
            "http://127.0.0.1/plugins.json",
            None,
            &CloudStreamRegistryFetchConfig::default(),
        )
        .expect_err("loopback URLs must be rejected by default");
        assert!(err.to_string().contains("private") || err.to_string().contains("local"));
    }

    #[tokio::test]
    async fn cs2_fetches_valid_repo_json_and_follows_plugin_lists() -> Result<()> {
        let (base_url, shutdown) = start_cloudstream_fixture_server(false).await?;
        let client = CloudStreamRegistryClient::new(test_config())?;
        let snapshot = client
            .fetch_registry("cloudstream_repo_json", &format!("{base_url}/repo.json"))
            .await?;
        let _ = shutdown.send(());

        assert_eq!(snapshot.registry_kind, "cloudstream_repo_json");
        assert_eq!(snapshot.plugin_lists.len(), 1);
        assert_eq!(snapshot.modules.len(), 2);
        let anime = snapshot
            .modules
            .iter()
            .find(|module| module.module_id == "animealpha")
            .expect("anime source module");
        assert_eq!(anime.media_types, vec!["anime"]);
        assert_eq!(
            anime.artifact_sha256.as_deref(),
            Some("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
        );
        assert_eq!(anime.source_domains, vec!["anime.example"]);
        Ok(())
    }

    #[tokio::test]
    async fn cs2_fetches_direct_plugins_json() -> Result<()> {
        let (base_url, shutdown) = start_cloudstream_fixture_server(false).await?;
        let client = CloudStreamRegistryClient::new(test_config())?;
        let snapshot = client
            .fetch_registry(
                "cloudstream_plugins_json",
                &format!("{base_url}/plugins.json"),
            )
            .await?;
        let _ = shutdown.send(());

        assert_eq!(snapshot.registry_kind, "cloudstream_plugins_json");
        assert!(snapshot.repository.is_none());
        assert_eq!(snapshot.modules.len(), 2);
        assert!(
            snapshot
                .modules
                .iter()
                .any(|module| module.module_id == "moviebox")
        );
        Ok(())
    }

    #[tokio::test]
    async fn cs2_rejects_oversized_response_before_parse() -> Result<()> {
        let (base_url, shutdown) = start_cloudstream_fixture_server(true).await?;
        let client = CloudStreamRegistryClient::new(CloudStreamRegistryFetchConfig {
            allow_private_hosts: true,
            max_response_bytes: 64,
            ..CloudStreamRegistryFetchConfig::default()
        })?;
        let err = client
            .fetch_registry(
                "cloudstream_plugins_json",
                &format!("{base_url}/oversized.json"),
            )
            .await
            .expect_err("oversized response must fail");
        let _ = shutdown.send(());
        assert!(err.to_string().contains("too large") || err.to_string().contains("exceeded"));
        Ok(())
    }

    #[tokio::test]
    async fn cs2_persists_normalized_plugin_metadata_into_source_module_records() -> Result<()> {
        let (database, instance_id) = create_cloudstream_test_database().await?;
        let store = ExtensionStore::new(&database.pool);

        let mut warnings = Vec::new();
        let modules = parse_plugins_json(
            fixture_plugins_json(),
            "https://repo.example/plugins.json",
            &test_config(),
            &mut warnings,
        )?;
        let snapshot = CloudStreamRegistrySnapshot {
            registry_kind: "cloudstream_plugins_json".to_string(),
            source_url: "https://repo.example/plugins.json".to_string(),
            etag: Some("\"plugins-etag\"".to_string()),
            last_modified: Some("Tue, 09 Jun 2026 18:00:00 GMT".to_string()),
            repository: None,
            plugin_lists: vec![CloudStreamPluginListDescriptor {
                source_url: "https://repo.example/plugins.json".to_string(),
                etag: None,
                last_modified: None,
                plugin_count: modules.len(),
            }],
            modules: dedupe_modules(modules, &mut warnings),
            warnings,
        };
        let registry_id = Uuid::new_v4();
        let summary = persist_cloudstream_registry_snapshot(
            &store,
            &CloudStreamRegistryStoreInput {
                registry_id,
                instance_id,
                registry_key: "cloudstream.fixture".to_string(),
                registry_type: "cloudstream_plugins_json".to_string(),
                trust_class: "maintainer_known".to_string(),
                display_name: Some("Fixture Sources".to_string()),
                url: Some("https://repo.example/plugins.json".to_string()),
                enabled: true,
                auto_refresh: true,
                trusted_for_executable_updates: false,
            },
            &snapshot,
        )
        .await?;
        assert_eq!(summary.modules, 2);
        assert_eq!(summary.versions, 2);

        let registries = store.list_source_registries(Some(instance_id)).await?;
        assert_eq!(registries.len(), 1);
        assert_eq!(registries[0].last_fetch_status, "success");
        assert_eq!(registries[0].etag.as_deref(), Some("\"plugins-etag\""));

        let modules = store
            .list_source_modules(Some(instance_id), Some(registry_id))
            .await?;
        assert_eq!(modules.len(), 2);
        let movie = modules
            .iter()
            .find(|module| module.module_key.ends_with(":moviebox"))
            .expect("movie module");
        assert_eq!(movie.health_state, "available");
        assert_eq!(movie.plugin_package.as_deref(), Some("MovieBox"));
        assert_eq!(
            movie.media_types_json.as_ref(),
            Some(&json!(["movie", "tv"]))
        );

        let versions = store
            .list_source_module_versions(movie.source_module_id)
            .await?;
        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].version, "12");
        assert_eq!(
            versions[0].artifact_sha256.as_deref(),
            Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        );
        Ok(())
    }

    #[test]
    fn cs3_parses_bundled_recommended_source_pack_manifest() -> Result<()> {
        let pack = parse_cloudstream_source_pack_manifest(
            BUNDLED_CLOUDSTREAM_RECOMMENDED_SOURCE_PACK,
            "https://elixir.media/source-packs/cloudstream/recommended.json",
            &test_config(),
        )?;
        assert_eq!(pack.source_pack_id, CLOUDSTREAM_RECOMMENDED_SOURCE_PACK_ID);
        assert_eq!(
            pack.registry_key.as_deref(),
            Some(CLOUDSTREAM_RECOMMENDED_REGISTRY_KEY)
        );
        assert!(pack.enabled_by_default);
        assert!(pack.trusted_for_executable_updates);
        assert_eq!(pack.modules.len(), 3);
        let signature = pack
            .update_manifest
            .as_ref()
            .and_then(|manifest| manifest.signature.as_ref())
            .expect("signature policy");
        assert!(signature.required_for_remote_updates);
        assert_eq!(signature.algorithm, "ed25519-detached-sha256");
        Ok(())
    }

    #[tokio::test]
    async fn cs3_install_seed_creates_enabled_recommended_source_records() -> Result<()> {
        let (database, instance_id) = create_cloudstream_test_database().await?;
        let store = ExtensionStore::new(&database.pool);
        let summary =
            seed_cloudstream_recommended_source_pack_for_instance(&store, instance_id, None)
                .await?;
        assert_eq!(summary.registries, 1);
        assert_eq!(summary.modules, 3);
        assert_eq!(summary.versions, 3);
        assert_eq!(summary.disabled_modules, 0);

        let registries = store.list_source_registries(Some(instance_id)).await?;
        assert_eq!(registries.len(), 1);
        assert_eq!(
            registries[0].registry_type,
            "elixir_curated_cloudstream_pack"
        );
        assert_eq!(registries[0].trust_class, "curated");
        assert!(registries[0].trusted_for_executable_updates);
        assert_eq!(registries[0].last_fetch_status, "success");

        let modules = store
            .list_source_modules(Some(instance_id), Some(registries[0].registry_id))
            .await?;
        assert_eq!(modules.len(), 3);
        assert!(modules.iter().all(|module| module.enabled));
        assert!(modules.iter().all(|module| module.installed));
        assert!(modules.iter().all(|module| module.active_version.is_some()));
        let archive = modules
            .iter()
            .find(|module| module.module_key.ends_with(":internet-archive"))
            .expect("internet archive module");
        assert_eq!(
            archive.media_types_json.as_ref(),
            Some(&json!(["movie", "tv"]))
        );
        let versions = store
            .list_source_module_versions(archive.source_module_id)
            .await?;
        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].install_state, "active");
        Ok(())
    }

    #[tokio::test]
    async fn cs3_source_pack_update_retains_previous_working_version() -> Result<()> {
        let (database, instance_id) = create_cloudstream_test_database().await?;
        let store = ExtensionStore::new(&database.pool);
        let config = test_config();
        let mut pack = parse_cloudstream_source_pack_manifest(
            BUNDLED_CLOUDSTREAM_RECOMMENDED_SOURCE_PACK,
            "https://elixir.media/source-packs/cloudstream/recommended.json",
            &config,
        )?;
        persist_cloudstream_source_pack_manifest(
            &store,
            instance_id,
            &pack,
            "bundled://elixir/cloudstream/recommended",
            &config,
        )
        .await?;

        pack.version = "2026.6.10".to_string();
        let module = pack.modules[0]
            .as_object_mut()
            .expect("source-pack module object");
        module.insert("version".to_string(), json!(2));
        module.insert(
            "jarHash".to_string(),
            json!("sha256-1111111111111111111111111111111111111111111111111111111111111111"),
        );
        persist_cloudstream_source_pack_manifest(
            &store,
            instance_id,
            &pack,
            "bundled://elixir/cloudstream/recommended",
            &config,
        )
        .await?;

        let modules = store.list_source_modules(Some(instance_id), None).await?;
        let module = modules
            .iter()
            .find(|module| module.module_key.ends_with(":internet-archive"))
            .expect("updated module");
        assert_eq!(module.active_version.as_deref(), Some("2"));
        assert_eq!(module.rollback_version.as_deref(), Some("1"));
        let versions = store
            .list_source_module_versions(module.source_module_id)
            .await?;
        assert_eq!(versions.len(), 2);
        let old = versions
            .iter()
            .find(|version| version.version == "1")
            .expect("old version");
        let new = versions
            .iter()
            .find(|version| version.version == "2")
            .expect("new version");
        assert_eq!(old.install_state, "installed");
        assert_eq!(new.install_state, "active");
        assert_eq!(new.rollback_of_version_id, Some(old.version_id));
        Ok(())
    }

    #[tokio::test]
    async fn cs3_replacement_recommendation_disables_broken_module_and_enables_replacement()
    -> Result<()> {
        let (database, instance_id) = create_cloudstream_test_database().await?;
        let store = ExtensionStore::new(&database.pool);
        let config = test_config();
        let pack = parse_cloudstream_source_pack_manifest(
            fixture_source_pack_with_replacement(),
            "https://repo.example/source-pack.json",
            &config,
        )?;
        persist_cloudstream_source_pack_manifest(
            &store,
            instance_id,
            &pack,
            "https://repo.example/source-pack.json",
            &config,
        )
        .await?;

        let modules = store.list_source_modules(Some(instance_id), None).await?;
        let alpha = modules
            .iter()
            .find(|module| module.module_key.ends_with(":alpha"))
            .expect("alpha module");
        store
            .set_source_module_enabled_state(
                alpha.source_module_id,
                true,
                "broken",
                Some("maintainer marked source dead"),
            )
            .await?;

        let applied = apply_cloudstream_source_replacement_recommendation(
            &store,
            instance_id,
            "replace-alpha-with-beta",
        )
        .await?;
        assert!(applied);

        let modules = store.list_source_modules(Some(instance_id), None).await?;
        let alpha = modules
            .iter()
            .find(|module| module.module_key.ends_with(":alpha"))
            .expect("alpha module");
        let beta = modules
            .iter()
            .find(|module| module.module_key.ends_with(":beta"))
            .expect("beta module");
        assert!(!alpha.enabled);
        assert_eq!(alpha.health_state, "disabled");
        assert!(beta.enabled);
        assert_eq!(beta.health_state, "available");
        let active = store
            .list_source_replacement_recommendations(Some(alpha.source_module_id), true)
            .await?;
        assert!(active.is_empty());
        Ok(())
    }

    #[test]
    fn cs13_parses_fixture_repo_plugins_and_curated_source_pack() -> Result<()> {
        let config = test_config();
        let repo = parse_repo_json(
            CS13_FIXTURE_REPO_JSON,
            "https://fixtures.elixir.media/cloudstream/repo.json",
            &config,
        )?;
        assert_eq!(
            repo.plugin_lists,
            vec![
                "https://fixtures.elixir.media/cloudstream/cloudstream-fixture-plugins.json"
                    .to_string()
            ]
        );

        let mut warnings = Vec::new();
        let modules = parse_plugins_json(
            CS13_FIXTURE_PLUGINS_JSON,
            &repo.plugin_lists[0],
            &config,
            &mut warnings,
        )?;
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(modules.len(), 4);
        assert!(modules.iter().any(|module| {
            module.module_id == "fixture-movie-direct"
                && module.media_types == vec!["movie".to_string()]
        }));
        assert!(modules.iter().any(|module| {
            module.module_id == "fixture-tv-dash" && module.media_types == vec!["tv".to_string()]
        }));
        assert!(modules.iter().any(|module| {
            module.module_id == "fixture-anime-hls"
                && module.media_types == vec!["anime".to_string()]
        }));
        let drm = modules
            .iter()
            .find(|module| module.module_id == "fixture-drm-unsupported")
            .expect("drm fixture module");
        assert!(drm.unsupported);
        assert!(
            drm.unsupported_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("DRM-protected"))
        );

        let pack = parse_cloudstream_source_pack_manifest(
            CS13_FIXTURE_SOURCE_PACK_JSON,
            "https://fixtures.elixir.media/cloudstream/source-pack.json",
            &config,
        )?;
        assert_eq!(
            pack.source_pack_id,
            "elixir.sourcepacks.cloudstream.cs13.fixture"
        );
        assert_eq!(pack.modules.len(), 4);
        assert_eq!(pack.replacement_recommendations.len(), 1);
        assert!(pack.enabled_by_default);
        assert!(pack.trusted_for_executable_updates);
        Ok(())
    }

    #[tokio::test]
    async fn cs13_fixture_curated_pack_persists_active_and_unsupported_modules() -> Result<()> {
        let (database, instance_id) = create_cloudstream_test_database().await?;
        let store = ExtensionStore::new(&database.pool);
        let config = test_config();
        let pack = parse_cloudstream_source_pack_manifest(
            CS13_FIXTURE_SOURCE_PACK_JSON,
            "https://fixtures.elixir.media/cloudstream/source-pack.json",
            &config,
        )?;

        let summary = persist_cloudstream_source_pack_manifest(
            &store,
            instance_id,
            &pack,
            "bundled://elixir/cloudstream/cs13-fixture",
            &config,
        )
        .await?;

        assert_eq!(summary.registries, 1);
        assert_eq!(summary.modules, 4);
        assert_eq!(summary.versions, 4);
        assert_eq!(summary.disabled_modules, 1);
        assert_eq!(summary.unsupported_modules, 1);

        let modules = store.list_source_modules(Some(instance_id), None).await?;
        let active_modules = modules
            .iter()
            .filter(|module| {
                module.module_key.ends_with(":fixture-movie-direct")
                    || module.module_key.ends_with(":fixture-tv-dash")
                    || module.module_key.ends_with(":fixture-anime-hls")
            })
            .collect::<Vec<_>>();
        assert_eq!(active_modules.len(), 3);
        assert!(active_modules.iter().all(|module| module.enabled));
        assert!(active_modules.iter().all(|module| module.installed));
        assert!(
            active_modules
                .iter()
                .all(|module| module.active_version.as_deref() == Some("1.0.0"))
        );

        let drm = modules
            .iter()
            .find(|module| module.module_key.ends_with(":fixture-drm-unsupported"))
            .expect("drm fixture module");
        assert!(drm.unsupported);
        assert!(!drm.enabled);
        assert!(!drm.installed);
        assert_eq!(drm.active_version, None);
        assert_eq!(drm.health_state, "unsupported");
        assert_eq!(
            drm.replacement_recommendation_key.as_deref(),
            Some("disable-fixture-drm")
        );
        let recommendations = store
            .list_source_replacement_recommendations(Some(drm.source_module_id), true)
            .await?;
        assert_eq!(recommendations.len(), 1);
        assert_eq!(recommendations[0].action, "disable");
        Ok(())
    }

    #[tokio::test]
    async fn cs14_migrates_existing_cloudstream_instance_without_touching_static_bridge_config()
    -> Result<()> {
        let (database, instance_id) = create_cloudstream_test_database().await?;
        let store = ExtensionStore::new(&database.pool);
        let legacy_config = json!({
            "sourceModulesJson": "[{\"id\":\"legacy-static\",\"adapter\":\"static_fixture_v1\",\"enabled\":true}]",
            "resultLimit": 12
        });
        store
            .update_instance_config(instance_id, Some(&legacy_config))
            .await?;

        let summary =
            migrate_cloudstream_recommended_source_pack_for_installed_instances(&store, None)
                .await?;

        assert_eq!(summary.instances_seen, 1);
        assert_eq!(summary.migrated_instances, 1);
        assert_eq!(summary.skipped_existing_instances, 0);
        assert_eq!(summary.registries, 1);
        assert_eq!(summary.modules, 3);
        assert_eq!(summary.versions, 3);

        let registries = store.list_source_registries(Some(instance_id)).await?;
        assert_eq!(registries.len(), 1);
        assert_eq!(
            registries[0].registry_key,
            CLOUDSTREAM_RECOMMENDED_REGISTRY_KEY
        );
        assert!(registries[0].enabled);
        let modules = store.list_source_modules(Some(instance_id), None).await?;
        assert_eq!(modules.len(), 3);
        assert!(modules.iter().all(|module| module.enabled));
        assert!(modules.iter().all(|module| module.installed));

        let instance = store.get_instance(instance_id).await?.expect("instance");
        assert_eq!(instance.config_json.as_ref(), Some(&legacy_config));
        Ok(())
    }

    #[tokio::test]
    async fn cs14_migration_is_idempotent_and_does_not_reenable_disabled_recommended_pack()
    -> Result<()> {
        let (database, instance_id) = create_cloudstream_test_database().await?;
        let store = ExtensionStore::new(&database.pool);
        let first =
            migrate_cloudstream_recommended_source_pack_for_installed_instances(&store, None)
                .await?;
        assert_eq!(first.migrated_instances, 1);
        let registry = store
            .list_source_registries(Some(instance_id))
            .await?
            .into_iter()
            .find(|registry| registry.registry_key == CLOUDSTREAM_RECOMMENDED_REGISTRY_KEY)
            .expect("recommended registry");
        store
            .set_source_registry_enabled_state(registry.registry_id, false, false)
            .await?;

        let second =
            migrate_cloudstream_recommended_source_pack_for_installed_instances(&store, None)
                .await?;

        assert_eq!(second.instances_seen, 1);
        assert_eq!(second.migrated_instances, 0);
        assert_eq!(second.skipped_existing_instances, 1);
        assert_eq!(second.registries, 0);
        assert_eq!(second.modules, 0);
        let registry = store
            .list_source_registries(Some(instance_id))
            .await?
            .into_iter()
            .find(|registry| registry.registry_key == CLOUDSTREAM_RECOMMENDED_REGISTRY_KEY)
            .expect("recommended registry");
        assert!(!registry.enabled);
        assert!(!registry.auto_refresh);
        Ok(())
    }

    async fn start_cloudstream_fixture_server(
        include_oversized: bool,
    ) -> Result<(String, oneshot::Sender<()>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let base_url = format!("http://{addr}");
        let repo_base = base_url.clone();
        let app = Router::new()
            .route(
                "/repo.json",
                get(move || {
                    let repo_base = repo_base.clone();
                    async move {
                        let body = json!({
                            "name": "Fixture CloudStream Repo",
                            "description": "Fixture repository",
                            "manifestVersion": 1,
                            "pluginLists": [format!("{repo_base}/plugins.json")]
                        });
                        Response::builder()
                            .header(ETAG, HeaderValue::from_static("\"repo-etag\""))
                            .body(Body::from(body.to_string()))
                            .expect("response")
                    }
                }),
            )
            .route(
                "/plugins.json",
                get(|| async {
                    Response::builder()
                        .header(ETAG, HeaderValue::from_static("\"plugins-etag\""))
                        .body(Body::from(fixture_plugins_json()))
                        .expect("response")
                }),
            )
            .route(
                "/oversized.json",
                get(move || async move {
                    if include_oversized {
                        Response::builder()
                            .status(StatusCode::OK)
                            .header("content-length", "1024")
                            .body(Body::from("x".repeat(1024)))
                            .expect("response")
                    } else {
                        Response::builder()
                            .status(StatusCode::NOT_FOUND)
                            .body(Body::empty())
                            .expect("response")
                    }
                }),
            );
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });
        Ok((base_url, shutdown_tx))
    }

    async fn create_cloudstream_test_database() -> Result<(Database, Uuid)> {
        let database = Database::connect(&DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        })
        .await?;
        database.run_migrations().await?;
        let store = ExtensionStore::new(&database.pool);
        store
            .upsert_extension(&NewExtension {
                extension_id: CLOUDSTREAM_COMPAT_EXTENSION_ID.to_string(),
                name: "CloudStream Compat".to_string(),
                version: "0.1.0".to_string(),
                kind: ExtensionKind::Module,
                publisher_name: Some("Elixir".to_string()),
                signing_key_id: None,
                trust_level: ExtensionTrustLevel::Community,
                manifest_json: json!({"id": CLOUDSTREAM_COMPAT_EXTENSION_ID}),
                package_hash: None,
                enabled: true,
            })
            .await?;
        let instance_id = Uuid::new_v4();
        store
            .create_instance(&NewExtensionInstance {
                instance_id,
                extension_id: CLOUDSTREAM_COMPAT_EXTENSION_ID.to_string(),
                instance_name: "default".to_string(),
                config_json: None,
                enabled: true,
            })
            .await?;
        Ok((database, instance_id))
    }

    fn fixture_plugins_json() -> &'static str {
        r#"[
            {
                "internalName": "MovieBox",
                "name": "Movie Box",
                "version": 12,
                "jarUrl": "https://repo.example/moviebox.jar",
                "url": "https://repo.example/moviebox.cs3",
                "jarHash": "sha256-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "fileHash": "sha256-ignored",
                "status": 1,
                "apiVersion": 6,
                "repositoryUrl": "https://github.com/example/repo",
                "iconUrl": "https://www.google.com/s2/favicons?domain=movies.example&sz=%size%",
                "tvTypes": ["Movie", "TvSeries"]
            },
            {
                "internalName": "AnimeAlpha",
                "name": "Anime Alpha",
                "version": "2.3.4",
                "jarUrl": "https://repo.example/anime.jar",
                "jarHash": "sha256-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "status": 1,
                "apiVersion": "6",
                "tvTypes": ["Anime", "OVA"],
                "sourceDomains": ["anime.example"]
            }
        ]"#
    }

    fn fixture_source_pack_with_replacement() -> &'static str {
        r#"{
            "schemaVersion": 1,
            "sourcePackId": "elixir.sourcepacks.cloudstream.fixture",
            "name": "Fixture Source Pack",
            "version": "1.0.0",
            "registryKey": "cloudstream.fixture",
            "trustClass": "curated",
            "enabledByDefault": true,
            "trustedForExecutableUpdates": true,
            "updateManifest": {
                "url": null,
                "signature": {
                    "algorithm": "ed25519-detached-sha256",
                    "canonicalization": "jcs",
                    "publisherKeyId": "ed25519:test",
                    "signature": null,
                    "requiredForRemoteUpdates": true
                }
            },
            "modules": [
                {
                    "moduleId": "alpha",
                    "internalName": "AlphaProvider",
                    "name": "Alpha",
                    "version": "1.0.0",
                    "jarUrl": "https://repo.example/alpha.jar",
                    "jarHash": "sha256-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "status": 1,
                    "mediaTypes": ["Movie"],
                    "sourceDomains": ["alpha.example"]
                },
                {
                    "moduleId": "beta",
                    "internalName": "BetaProvider",
                    "name": "Beta",
                    "version": "1.0.0",
                    "jarUrl": "https://repo.example/beta.jar",
                    "jarHash": "sha256-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "status": 1,
                    "mediaTypes": ["Movie"],
                    "sourceDomains": ["beta.example"]
                }
            ],
            "replacementRecommendations": [
                {
                    "recommendationKey": "replace-alpha-with-beta",
                    "action": "replace",
                    "sourceModuleId": "alpha",
                    "replacementModuleId": "beta",
                    "reason": "maintainer marked Alpha as broken"
                }
            ]
        }"#
    }
}
