#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use chrono::Utc;
use reqwest::header::{ETAG, LAST_MODIFIED, LOCATION, USER_AGENT};
use reqwest::{Client, Url, redirect::Policy};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::extensions::source_artifacts::install_source_module_artifact;
use crate::extensions::source_network_safety::{
    validate_public_source_ip, validate_source_url_dns,
};
use crate::extensions::store::{
    ExtensionSourceRegistry, ExtensionStore, NewExtensionSourceModule,
    NewExtensionSourceModuleVersion, NewExtensionSourceRegistry,
    NewExtensionSourceReplacementRecommendation,
};

pub const PRISM_EXTENSION_ID: &str = "elixir.sources.prism";
pub const LEGACY_NUVIO_COMPAT_EXTENSION_ID: &str = "elixir.sources.nuvio_compat";
pub const PRISM_RECOMMENDED_SOURCE_PACK_ID: &str = "elixir.sourcepacks.prism.recommended";
pub const PRISM_RECOMMENDED_REGISTRY_KEY: &str = "prism.recommended";
pub const PRISM_RECOMMENDED_SOURCE_PACK_PATH: &str = "source-packs/prism-recommended.json";
const PRISM_SOURCE_REGISTRY_TOMBSTONES_CONFIG_KEY: &str = "sourceRegistryTombstones";

pub fn is_prism_extension_id(extension_id: &str) -> bool {
    matches!(
        extension_id,
        PRISM_EXTENSION_ID | LEGACY_NUVIO_COMPAT_EXTENSION_ID
    )
}

const DEFAULT_FETCH_TIMEOUT: Duration = Duration::from_secs(12);
const DEFAULT_MAX_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
const DEFAULT_MAX_SCRAPERS: usize = 2_000;
const DEFAULT_MAX_REDIRECTS: usize = 5;
const GITHUB_PROVIDER_DISCOVERY_DIRS: &[&str] = &["providers", "src/providers"];
const GITHUB_PROVIDER_DISCOVERY_USER_AGENT: &str = "Elixir-Prism-Nuvio-Compat";
const BUNDLED_PRISM_RECOMMENDED_SOURCE_PACK: &str = include_str!(
    "../../../extensions/marketplace/prism-source-provider/source-packs/prism-recommended.json"
);

#[derive(Debug, Clone)]
pub struct NuvioRegistryFetchConfig {
    pub timeout: Duration,
    pub max_redirects: usize,
    pub max_response_bytes: usize,
    pub max_scrapers: usize,
    pub allow_private_hosts: bool,
}

impl Default for NuvioRegistryFetchConfig {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_FETCH_TIMEOUT,
            max_redirects: DEFAULT_MAX_REDIRECTS,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            max_scrapers: DEFAULT_MAX_SCRAPERS,
            allow_private_hosts: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NuvioRegistryKind {
    ManifestJson,
}

impl NuvioRegistryKind {
    pub fn from_registry_type(registry_type: &str) -> Result<Self> {
        match registry_type.trim() {
            "nuvio_manifest_json" => Ok(Self::ManifestJson),
            other => bail!("unsupported Nuvio registry type '{other}'"),
        }
    }

    pub fn as_registry_type(self) -> &'static str {
        match self {
            Self::ManifestJson => "nuvio_manifest_json",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NuvioRepositoryDescriptor {
    pub name: Option<String>,
    pub version: Option<String>,
    pub description: Option<String>,
    pub source_url: String,
    #[serde(default)]
    pub raw: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NuvioSourceModuleDescriptor {
    pub module_id: String,
    pub display_name: String,
    pub version: String,
    pub artifact_url: Option<String>,
    pub artifact_sha256: Option<String>,
    pub media_types: Vec<String>,
    pub language_tags: Vec<String>,
    pub formats: Vec<String>,
    pub source_domains: Vec<String>,
    pub author: Option<String>,
    pub description: Option<String>,
    pub logo_url: Option<String>,
    pub has_settings: bool,
    pub account_required: bool,
    pub disabled: bool,
    pub unsupported: bool,
    pub unsupported_reason: Option<String>,
    pub manifest_url: String,
    pub raw: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NuvioRegistrySnapshot {
    pub registry_kind: String,
    pub source_url: String,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub repository: NuvioRepositoryDescriptor,
    pub modules: Vec<NuvioSourceModuleDescriptor>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct NuvioRegistryStoreInput {
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
pub struct NuvioRegistryPersistSummary {
    pub registries: usize,
    pub modules: usize,
    pub versions: usize,
    pub disabled_modules: usize,
    pub unsupported_modules: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PrismRecommendedPackMigrationSummary {
    pub instances_seen: usize,
    pub migrated_instances: usize,
    pub skipped_existing_instances: usize,
    pub registries: usize,
    pub modules: usize,
    pub versions: usize,
    pub disabled_modules: usize,
    pub unsupported_modules: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PrismSourcePackPolicy {
    recommended_pack_auto_enable: bool,
    recommended_pack_executable_updates: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrismSourcePackManifest {
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
    pub update_manifest: Option<PrismSourcePackUpdateManifest>,
    #[serde(default)]
    pub maintainer_known_repositories: Vec<PrismSourcePackRepository>,
    pub modules: Vec<Value>,
    #[serde(default)]
    pub replacement_recommendations: Vec<PrismSourcePackReplacementRecommendation>,
    #[serde(default)]
    pub raw: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrismSourcePackRepository {
    pub registry_key: String,
    pub display_name: String,
    #[serde(default = "default_nuvio_manifest_registry_type")]
    pub registry_type: String,
    pub url: String,
    #[serde(default)]
    pub trust_class: Option<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_true")]
    pub auto_refresh: bool,
    #[serde(default)]
    pub trusted_for_executable_updates: bool,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrismSourcePackUpdateManifest {
    pub url: Option<String>,
    pub signature: Option<PrismSourcePackSignaturePolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrismSourcePackSignaturePolicy {
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
pub struct PrismSourcePackReplacementRecommendation {
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

#[derive(Debug, Clone)]
struct GitHubRawRepositoryContext {
    owner: String,
    repository: String,
    reference: String,
    root_path: String,
}

#[derive(Debug, Deserialize)]
struct GitHubContentEntry {
    name: String,
    path: String,
    #[serde(rename = "type")]
    entry_type: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawManifest {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    version: Option<Value>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default, alias = "providers", alias = "modules", alias = "sources")]
    scrapers: Vec<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawScraper {
    #[serde(default, alias = "moduleId", alias = "providerId")]
    id: Option<Value>,
    #[serde(default, alias = "displayName")]
    name: Option<Value>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    version: Option<Value>,
    #[serde(default)]
    author: Option<Value>,
    #[serde(default, alias = "mediaTypes", alias = "types")]
    supported_types: Option<Value>,
    #[serde(
        default,
        alias = "file",
        alias = "path",
        alias = "script",
        alias = "url"
    )]
    filename: Option<Value>,
    #[serde(
        default,
        alias = "artifactUrl",
        alias = "artifact_url",
        alias = "sourceUrl"
    )]
    artifact_url: Option<Value>,
    #[serde(
        default,
        alias = "sha256",
        alias = "artifactSha256",
        alias = "artifact_sha256"
    )]
    hash: Option<Value>,
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(
        default,
        alias = "contentLanguages",
        alias = "languages",
        alias = "language"
    )]
    content_language: Option<Value>,
    #[serde(default)]
    formats: Option<Value>,
    #[serde(default, alias = "logoUrl", alias = "icon")]
    logo: Option<Value>,
    #[serde(default, alias = "sourceDomains", alias = "domains")]
    source_domains: Option<Value>,
    #[serde(default)]
    has_settings: Option<bool>,
    #[serde(default, alias = "requiresAccount", alias = "accountRequired")]
    requires_account: Option<bool>,
    #[serde(default)]
    browser_required: Option<bool>,
    #[serde(default)]
    captcha_required: Option<bool>,
    #[serde(default)]
    drm_required: Option<bool>,
}

pub struct NuvioRegistryClient {
    client: Client,
    config: NuvioRegistryFetchConfig,
}

impl NuvioRegistryClient {
    pub fn new(config: NuvioRegistryFetchConfig) -> Result<Self> {
        let client = Client::builder()
            .timeout(config.timeout)
            .redirect(Policy::none())
            .build()
            .context("building Nuvio registry HTTP client")?;
        Ok(Self { client, config })
    }

    pub async fn fetch_registry(
        &self,
        registry_type: &str,
        url: &str,
    ) -> Result<NuvioRegistrySnapshot> {
        let kind = NuvioRegistryKind::from_registry_type(registry_type)?;
        match kind {
            NuvioRegistryKind::ManifestJson => self.fetch_manifest_json(url).await,
        }
    }

    async fn fetch_manifest_json(&self, url: &str) -> Result<NuvioRegistrySnapshot> {
        let registry_url = normalize_nuvio_manifest_url(url, &self.config)?;
        let fetched = self.fetch_text(&registry_url, None).await?;
        let mut snapshot = parse_nuvio_manifest_json(&fetched.text, &fetched.url, &self.config)?;
        self.enrich_manifest_snapshot_from_provider_files(&mut snapshot)
            .await?;
        snapshot.etag = fetched.etag;
        snapshot.last_modified = fetched.last_modified;
        Ok(snapshot)
    }

    async fn enrich_manifest_snapshot_from_provider_files(
        &self,
        snapshot: &mut NuvioRegistrySnapshot,
    ) -> Result<()> {
        let Some(context) = github_raw_repository_context(&snapshot.source_url) else {
            return Ok(());
        };
        let mut provider_entries = Vec::new();
        for provider_dir in GITHUB_PROVIDER_DISCOVERY_DIRS {
            match self
                .fetch_github_provider_directory(&context, provider_dir)
                .await
            {
                Ok(mut entries) => provider_entries.append(&mut entries),
                Err(err) => snapshot.warnings.push(format!(
                    "nuvio_manifest_json:{}: provider-file discovery skipped for {}: {err}",
                    snapshot.source_url, provider_dir
                )),
            }
        }
        if provider_entries.is_empty() {
            return Ok(());
        }
        let mut modules = std::mem::take(&mut snapshot.modules);
        synthesize_missing_provider_file_modules(
            &mut modules,
            provider_entries,
            &snapshot.source_url,
            snapshot.repository.version.as_deref(),
            &self.config,
            &mut snapshot.warnings,
        )?;
        snapshot.modules = dedupe_modules(modules, &mut snapshot.warnings);
        Ok(())
    }

    async fn fetch_github_provider_directory(
        &self,
        context: &GitHubRawRepositoryContext,
        provider_dir: &str,
    ) -> Result<Vec<GitHubContentEntry>> {
        let dir_path = join_url_path(&context.root_path, provider_dir);
        let mut url = Url::parse(&format!(
            "https://api.github.com/repos/{}/{}/contents/{}",
            context.owner, context.repository, dir_path
        ))
        .context("building GitHub provider directory URL")?;
        url.query_pairs_mut().append_pair("ref", &context.reference);
        validate_safe_http_url(&url, self.config.allow_private_hosts)?;
        let entries = self.fetch_github_provider_directory_url(url).await?;
        Ok(entries
            .into_iter()
            .filter(|entry| {
                entry.entry_type == "file"
                    && entry.name.ends_with(".js")
                    && !entry.name.starts_with('.')
                    && !entry.name.to_ascii_lowercase().contains("extractor")
            })
            .collect())
    }

    async fn fetch_github_provider_directory_url(
        &self,
        url: Url,
    ) -> Result<Vec<GitHubContentEntry>> {
        let mut next_url = url;
        let mut redirects = 0usize;
        let response = loop {
            validate_safe_http_url(&next_url, self.config.allow_private_hosts)?;
            validate_source_url_dns(
                &next_url,
                self.config.allow_private_hosts,
                "GitHub provider directory",
            )
            .await?;
            let response = self
                .client
                .get(next_url.clone())
                .header(USER_AGENT, GITHUB_PROVIDER_DISCOVERY_USER_AGENT)
                .send()
                .await
                .with_context(|| format!("fetching GitHub provider directory {next_url}"))?;
            if !response.status().is_redirection() {
                break response;
            }
            let Some(location) = response
                .headers()
                .get(LOCATION)
                .and_then(|value| value.to_str().ok())
            else {
                bail!("GitHub provider directory redirect response missing Location header");
            };
            redirects += 1;
            if redirects > self.config.max_redirects {
                bail!("too many GitHub provider directory redirects");
            }
            next_url = checked_nuvio_registry_redirect_target(
                &next_url,
                location,
                self.config.allow_private_hosts,
            )?;
        };
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(Vec::new());
        }
        let final_url = response.url().to_string();
        validate_safe_http_url(response.url(), self.config.allow_private_hosts)?;
        let response = response.error_for_status().with_context(|| {
            format!("GitHub provider directory {final_url} returned an error status")
        })?;
        if let Some(length) = response.content_length() {
            if length > self.config.max_response_bytes as u64 {
                bail!(
                    "GitHub provider directory {} is too large: {} bytes exceeds {} bytes",
                    final_url,
                    length,
                    self.config.max_response_bytes
                );
            }
        }
        let bytes = response
            .bytes()
            .await
            .with_context(|| format!("reading GitHub provider directory {final_url}"))?;
        if bytes.len() > self.config.max_response_bytes {
            bail!(
                "GitHub provider directory {} exceeded {} bytes",
                final_url,
                self.config.max_response_bytes
            );
        }
        serde_json::from_slice::<Vec<GitHubContentEntry>>(&bytes)
            .with_context(|| format!("parsing GitHub provider directory {final_url}"))
    }

    async fn fetch_text(&self, url: &str, base_url: Option<&str>) -> Result<FetchedText> {
        let normalized = normalize_http_url(url, base_url, &self.config)?;
        let mut next_url = Url::parse(&normalized).context("parsing normalized Nuvio URL")?;
        let mut redirects = 0usize;
        let response = loop {
            validate_safe_http_url(&next_url, self.config.allow_private_hosts)?;
            validate_source_url_dns(&next_url, self.config.allow_private_hosts, "Nuvio registry")
                .await?;
            let response = self
                .client
                .get(next_url.clone())
                .send()
                .await
                .with_context(|| format!("fetching Nuvio registry document {next_url}"))?;
            if !response.status().is_redirection() {
                break response;
            }
            let Some(location) = response
                .headers()
                .get(LOCATION)
                .and_then(|value| value.to_str().ok())
            else {
                bail!("Nuvio registry redirect response missing Location header");
            };
            redirects += 1;
            if redirects > self.config.max_redirects {
                bail!("too many Nuvio registry redirects");
            }
            next_url = checked_nuvio_registry_redirect_target(
                &next_url,
                location,
                self.config.allow_private_hosts,
            )?;
        };
        let final_url = response.url().to_string();
        validate_safe_http_url(response.url(), self.config.allow_private_hosts)?;
        let headers = response.headers().clone();
        let response = response.error_for_status().with_context(|| {
            format!(
                "Nuvio registry document {} returned an error status",
                final_url
            )
        })?;
        if let Some(length) = response.content_length() {
            if length > self.config.max_response_bytes as u64 {
                bail!(
                    "Nuvio registry document {} is too large: {} bytes exceeds {} bytes",
                    final_url,
                    length,
                    self.config.max_response_bytes
                );
            }
        }
        let bytes = response
            .bytes()
            .await
            .with_context(|| format!("reading Nuvio registry document {final_url}"))?;
        if bytes.len() > self.config.max_response_bytes {
            bail!(
                "Nuvio registry document {} exceeded {} bytes",
                final_url,
                self.config.max_response_bytes
            );
        }
        let text = String::from_utf8(bytes.to_vec())
            .with_context(|| format!("Nuvio registry document {final_url} is not UTF-8"))?;
        Ok(FetchedText {
            url: final_url,
            text,
            etag: headers
                .get(ETAG)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string),
            last_modified: headers
                .get(LAST_MODIFIED)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string),
        })
    }
}

fn checked_nuvio_registry_redirect_target(
    current_url: &Url,
    location: &str,
    allow_private_hosts: bool,
) -> Result<Url> {
    let redirected = current_url
        .join(location)
        .with_context(|| format!("parsing Nuvio registry redirect location for {current_url}"))?;
    validate_safe_http_url(&redirected, allow_private_hosts)
        .context("blocked unsafe Nuvio registry redirect target")?;
    Ok(redirected)
}

pub fn parse_nuvio_manifest_json(
    text: &str,
    source_url: &str,
    config: &NuvioRegistryFetchConfig,
) -> Result<NuvioRegistrySnapshot> {
    let value: Value = serde_json::from_str(text).context("parsing Nuvio manifest.json")?;
    let raw: RawManifest = match &value {
        Value::Object(_) => {
            serde_json::from_value(value.clone()).context("normalizing Nuvio manifest.json")?
        }
        Value::Array(scrapers) => RawManifest {
            name: None,
            version: None,
            description: None,
            scrapers: scrapers.clone(),
        },
        _ => bail!("Nuvio manifest.json must be a JSON object or scraper array"),
    };
    if raw.scrapers.len() > config.max_scrapers {
        bail!(
            "Nuvio manifest.json contains {} scrapers, exceeding limit {}",
            raw.scrapers.len(),
            config.max_scrapers
        );
    }
    let mut warnings = Vec::new();
    let mut modules = Vec::new();
    for (index, scraper) in raw.scrapers.into_iter().enumerate() {
        match normalize_scraper_entry(scraper, index, source_url, config) {
            Ok(Some(module)) => modules.push(module),
            Ok(None) => warnings.push(format!(
                "nuvio_manifest_json:{source_url}: skipped empty scraper entry {index}"
            )),
            Err(err) => warnings.push(format!(
                "nuvio_manifest_json:{source_url}: scraper entry {index} rejected: {err}"
            )),
        }
    }
    let modules = dedupe_modules(modules, &mut warnings);
    Ok(NuvioRegistrySnapshot {
        registry_kind: NuvioRegistryKind::ManifestJson
            .as_registry_type()
            .to_string(),
        source_url: source_url.to_string(),
        etag: None,
        last_modified: None,
        repository: NuvioRepositoryDescriptor {
            name: raw.name,
            version: raw.version.as_ref().and_then(value_to_string),
            description: raw.description,
            source_url: source_url.to_string(),
            raw: value,
        },
        modules,
        warnings,
    })
}

pub fn parse_prism_source_pack_manifest(
    text: &str,
    source_url: &str,
    config: &NuvioRegistryFetchConfig,
) -> Result<PrismSourcePackManifest> {
    let value: Value = serde_json::from_str(text).context("parsing Prism source-pack manifest")?;
    if !value.is_object() {
        bail!("Prism source-pack manifest must be a JSON object");
    }
    let mut pack: PrismSourcePackManifest =
        serde_json::from_value(value.clone()).context("normalizing Prism source-pack manifest")?;
    pack.raw = value;
    if pack.schema_version != 1 {
        bail!(
            "unsupported Prism source-pack schema version {}",
            pack.schema_version
        );
    }
    ensure_non_empty(&pack.source_pack_id, "sourcePackId")?;
    ensure_non_empty(&pack.name, "name")?;
    ensure_non_empty(&pack.version, "version")?;
    if pack.modules.len() > config.max_scrapers {
        bail!(
            "Prism source-pack contains {} modules, exceeding limit {}",
            pack.modules.len(),
            config.max_scrapers
        );
    }
    if let Some(trust_class) = pack.trust_class.as_deref() {
        match trust_class.trim() {
            "curated" | "maintainer_known" | "custom" => {}
            other => bail!("unsupported Prism source-pack trust class '{other}'"),
        }
    }
    if let Some(update_manifest) = pack.update_manifest.as_ref() {
        if let Some(url) = update_manifest.url.as_deref() {
            normalize_http_url(url, Some(source_url), config).with_context(|| {
                format!("validating Prism source-pack update manifest URL {url}")
            })?;
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
    for repository in &pack.maintainer_known_repositories {
        ensure_non_empty(
            &repository.registry_key,
            "maintainerKnownRepositories.registryKey",
        )?;
        ensure_non_empty(
            &repository.display_name,
            "maintainerKnownRepositories.displayName",
        )?;
        ensure_non_empty(&repository.url, "maintainerKnownRepositories.url")?;
        NuvioRegistryKind::from_registry_type(&repository.registry_type).with_context(|| {
            format!(
                "validating Prism maintainer-known repository '{}'",
                repository.registry_key
            )
        })?;
        normalize_http_url(&repository.url, Some(source_url), config).with_context(|| {
            format!(
                "validating Prism maintainer-known repository URL {}",
                repository.url
            )
        })?;
        if let Some(trust_class) = repository.trust_class.as_deref() {
            match trust_class.trim() {
                "maintainer_known" | "custom" => {}
                other => {
                    bail!("unsupported Prism maintainer-known repository trust class '{other}'")
                }
            }
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

pub async fn record_prism_source_registry_tombstone(
    store: &ExtensionStore<'_>,
    registry: &ExtensionSourceRegistry,
    reason: &str,
) -> Result<()> {
    let registry_key = registry.registry_key.trim();
    if registry_key.is_empty() {
        return Ok(());
    }
    update_prism_source_registry_tombstones(store, registry.instance_id, |tombstones| {
        tombstones.insert(
            registry_key.to_string(),
            json!({
                "registryId": registry.registry_id,
                "registryKey": registry.registry_key,
                "registryType": registry.registry_type,
                "trustClass": registry.trust_class,
                "displayName": registry.display_name,
                "url": registry.url,
                "reason": reason,
                "removedAt": Utc::now(),
            }),
        );
    })
    .await
}

async fn clear_prism_source_registry_tombstones(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    registry_keys: &[String],
) -> Result<()> {
    if registry_keys.is_empty() {
        return Ok(());
    }
    update_prism_source_registry_tombstones(store, instance_id, |tombstones| {
        for registry_key in registry_keys {
            tombstones.remove(registry_key);
        }
    })
    .await
}

async fn update_prism_source_registry_tombstones(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    update: impl FnOnce(&mut serde_json::Map<String, Value>),
) -> Result<()> {
    let instance = store
        .get_instance(instance_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Prism instance {instance_id} was not found"))?;
    let mut config = instance
        .config_json
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
    let mut tombstones = config
        .get(PRISM_SOURCE_REGISTRY_TOMBSTONES_CONFIG_KEY)
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    update(&mut tombstones);
    if tombstones.is_empty() {
        config.remove(PRISM_SOURCE_REGISTRY_TOMBSTONES_CONFIG_KEY);
    } else {
        config.insert(
            PRISM_SOURCE_REGISTRY_TOMBSTONES_CONFIG_KEY.to_string(),
            Value::Object(tombstones),
        );
    }
    let config_value = Value::Object(config);
    store
        .update_instance_config(instance_id, Some(&config_value))
        .await
}

async fn prism_source_registry_tombstone_keys(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
) -> Result<HashSet<String>> {
    Ok(store
        .get_instance(instance_id)
        .await?
        .and_then(|instance| instance.config_json)
        .and_then(|config| {
            config
                .get(PRISM_SOURCE_REGISTRY_TOMBSTONES_CONFIG_KEY)
                .and_then(Value::as_object)
                .map(|tombstones| tombstones.keys().cloned().collect::<HashSet<_>>())
        })
        .unwrap_or_default())
}

fn prism_source_pack_registry_keys(pack: &PrismSourcePackManifest) -> Vec<String> {
    let mut keys = vec![
        pack.registry_key
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or(PRISM_RECOMMENDED_REGISTRY_KEY)
            .to_string(),
    ];
    keys.extend(
        pack.maintainer_known_repositories
            .iter()
            .map(|repository| repository.registry_key.trim())
            .filter(|registry_key| !registry_key.is_empty())
            .map(str::to_string),
    );
    keys.sort();
    keys.dedup();
    keys
}

pub async fn seed_prism_recommended_source_pack_for_instance(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    installed_package_dir: Option<&Path>,
    storage_root: Option<&str>,
) -> Result<NuvioRegistryPersistSummary> {
    seed_prism_recommended_source_pack_for_instance_inner(
        store,
        instance_id,
        installed_package_dir,
        storage_root,
        false,
    )
    .await
}

pub async fn restore_prism_recommended_source_pack_for_instance(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    installed_package_dir: Option<&Path>,
    storage_root: Option<&str>,
) -> Result<NuvioRegistryPersistSummary> {
    seed_prism_recommended_source_pack_for_instance_inner(
        store,
        instance_id,
        installed_package_dir,
        storage_root,
        true,
    )
    .await
}

async fn seed_prism_recommended_source_pack_for_instance_inner(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    installed_package_dir: Option<&Path>,
    storage_root: Option<&str>,
    restore_tombstoned: bool,
) -> Result<NuvioRegistryPersistSummary> {
    let manifest_text = read_prism_recommended_source_pack_manifest(installed_package_dir)?;
    let config = NuvioRegistryFetchConfig::default();
    let pack = parse_prism_source_pack_manifest(
        &manifest_text,
        "https://elixir.media/source-packs/prism/recommended.json",
        &config,
    )?;
    if pack.source_pack_id != PRISM_RECOMMENDED_SOURCE_PACK_ID {
        bail!(
            "bundled Prism source pack id '{}' does not match expected '{}'",
            pack.source_pack_id,
            PRISM_RECOMMENDED_SOURCE_PACK_ID
        );
    }
    let registry_keys = prism_source_pack_registry_keys(&pack);
    if restore_tombstoned {
        clear_prism_source_registry_tombstones(store, instance_id, &registry_keys).await?;
    }
    let tombstones = prism_source_registry_tombstone_keys(store, instance_id).await?;
    let registry_key = pack
        .registry_key
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(PRISM_RECOMMENDED_REGISTRY_KEY);
    if tombstones.contains(registry_key) {
        return Ok(NuvioRegistryPersistSummary::default());
    }
    let summary = persist_prism_source_pack_manifest(
        store,
        instance_id,
        &pack,
        "bundled://elixir/prism/recommended",
        &config,
        &tombstones,
    )
    .await?;
    let policy = prism_source_pack_policy(store, instance_id).await?;
    if policy.recommended_pack_executable_updates {
        if let Some(storage_root) = storage_root {
            install_prism_recommended_source_pack_artifacts(store, instance_id, storage_root)
                .await?;
        }
    }
    Ok(summary)
}

pub async fn migrate_prism_recommended_source_pack_for_installed_instances(
    store: &ExtensionStore<'_>,
    installed_package_dir: Option<&Path>,
    storage_root: Option<&str>,
) -> Result<PrismRecommendedPackMigrationSummary> {
    if store.get_extension(PRISM_EXTENSION_ID).await?.is_none() {
        return Ok(PrismRecommendedPackMigrationSummary::default());
    }
    let manifest_text = read_prism_recommended_source_pack_manifest(installed_package_dir)?;
    let config = NuvioRegistryFetchConfig::default();
    let desired_pack = parse_prism_source_pack_manifest(
        &manifest_text,
        "https://elixir.media/source-packs/prism/recommended.json",
        &config,
    )?;

    let instances = store.list_instances(Some(PRISM_EXTENSION_ID)).await?;
    let mut summary = PrismRecommendedPackMigrationSummary {
        instances_seen: instances.len(),
        ..PrismRecommendedPackMigrationSummary::default()
    };

    for instance in instances {
        let registries = store
            .list_source_registries(Some(instance.instance_id))
            .await?;
        let existing_recommended = registries
            .iter()
            .find(|registry| registry.registry_key == PRISM_RECOMMENDED_REGISTRY_KEY);
        if existing_recommended.is_none() && !registries.is_empty() {
            summary.skipped_existing_instances += 1;
            continue;
        }
        if existing_recommended
            .and_then(prism_registry_source_pack_version)
            .is_some_and(|version| version == desired_pack.version.as_str())
        {
            summary.skipped_existing_instances += 1;
            continue;
        }

        let seed = seed_prism_recommended_source_pack_for_instance(
            store,
            instance.instance_id,
            installed_package_dir,
            storage_root,
        )
        .await
        .with_context(|| {
            format!(
                "migrating Prism recommended source pack for instance {}",
                instance.instance_id
            )
        })?;
        if seed.registries == 0 && seed.modules == 0 && seed.versions == 0 {
            summary.skipped_existing_instances += 1;
            continue;
        }
        summary.migrated_instances += 1;
        summary.registries += seed.registries;
        summary.modules += seed.modules;
        summary.versions += seed.versions;
        summary.disabled_modules += seed.disabled_modules;
        summary.unsupported_modules += seed.unsupported_modules;
    }

    Ok(summary)
}

pub async fn persist_prism_source_pack_manifest(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    pack: &PrismSourcePackManifest,
    source_url: &str,
    config: &NuvioRegistryFetchConfig,
    tombstoned_registry_keys: &HashSet<String>,
) -> Result<NuvioRegistryPersistSummary> {
    if pack.schema_version != 1 {
        bail!(
            "unsupported Prism source-pack schema version {}",
            pack.schema_version
        );
    }
    let registry_key = pack
        .registry_key
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(PRISM_RECOMMENDED_REGISTRY_KEY);
    let trust_class = pack
        .trust_class
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("curated");
    let registry_id = deterministic_uuid(&format!(
        "elixir:prism:source-pack-registry:{instance_id}:{registry_key}"
    ));
    store
        .upsert_source_registry(&NewExtensionSourceRegistry {
            registry_id,
            instance_id,
            registry_key: registry_key.to_string(),
            registry_type: "elixir_curated_nuvio_pack".to_string(),
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
                "prismSourcePack": {
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

    let known_registry_count = persist_prism_source_pack_repositories(
        store,
        instance_id,
        pack,
        source_url,
        config,
        tombstoned_registry_keys,
    )
    .await?;
    let mut warnings = Vec::new();
    let mut modules = Vec::with_capacity(pack.modules.len());
    for (index, raw_module) in pack.modules.iter().enumerate() {
        let module = normalize_scraper_entry(raw_module.clone(), index, source_url, config)
            .with_context(|| {
                format!(
                    "normalizing Prism source-pack module {}",
                    index.saturating_add(1)
                )
            })?;
        match module {
            Some(module) => modules.push(module),
            None => warnings.push(format!(
                "prism_source_pack:{}: skipped empty module entry {}",
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
    let source_pack_policy = prism_source_pack_policy(store, instance_id).await?;
    let recommendation_key_by_module = prism_recommendation_keys_by_module(registry_key, pack);
    let now = Utc::now();
    let mut summary = NuvioRegistryPersistSummary {
        registries: 1 + known_registry_count,
        ..Default::default()
    };

    for module in &modules {
        let module_key = source_module_key(registry_key, &module.module_id);
        let source_module_id = deterministic_uuid(&format!(
            "elixir:prism:source-pack-module:{instance_id}:{module_key}"
        ));
        let existing = existing_by_key.get(&module_key);
        let hash_pinned = module
            .artifact_sha256
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_some();
        let default_enabled = pack.enabled_by_default
            && source_pack_policy.recommended_pack_auto_enable
            && hash_pinned
            && !module.unsupported
            && !module.account_required
            && !module.disabled;
        let module_enabled = if module.unsupported || module.account_required || module.disabled {
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
        let health_state =
            prism_source_pack_module_health_state(module, module_enabled, hash_pinned);
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
                ecosystem: "nuvio".to_string(),
                plugin_package: Some(module.module_id.clone()),
                active_version: active_version.clone(),
                rollback_version: rollback_version.clone(),
                media_types_json: Some(json!(module.media_types)),
                language_tags_json: Some(json!(module.language_tags)),
                region_tags_json: None,
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
                last_error: if hash_pinned {
                    module.unsupported_reason.clone()
                } else {
                    Some("recommended executable artifact is not hash-pinned".to_string())
                },
                metadata_json: Some(json!({
                    "nuvio": {
                        "moduleId": module.module_id,
                        "adapter": "nuvio_js_v1",
                        "hasSettings": module.has_settings,
                        "author": module.author,
                        "description": module.description,
                        "formats": module.formats,
                        "logoUrl": module.logo_url,
                        "manifestUrl": module.manifest_url,
                        "raw": module.raw,
                    },
                    "prismSourcePack": {
                        "sourcePackId": pack.source_pack_id,
                        "version": pack.version,
                    },
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
            "elixir:prism:source-pack-module-version:{source_module_id}:{}:{}",
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
                signature: None,
                install_state: install_state.to_string(),
                smoke_status: "unknown".to_string(),
                smoke_error: None,
                rollback_of_version_id,
                installed_at: (install_state == "active" || install_state == "installed")
                    .then_some(now),
                activated_at: (install_state == "active").then_some(now),
                metadata_json: Some(json!({
                    "artifact": {
                        "kind": "javascript",
                        "filename": module.raw.get("filename").cloned(),
                    },
                    "nuvio": {
                        "moduleId": module.module_id,
                        "adapter": "nuvio_js_v1",
                        "manifestUrl": module.manifest_url,
                        "raw": module.raw,
                    },
                    "prismSourcePack": {
                        "sourcePackId": pack.source_pack_id,
                        "version": pack.version,
                    },
                    "sourcePackId": pack.source_pack_id,
                    "sourcePackVersion": pack.version,
                })),
            })
            .await?;
        summary.versions += 1;
    }

    persist_prism_source_pack_replacement_recommendations(
        store,
        instance_id,
        registry_id,
        registry_key,
        pack,
    )
    .await?;
    Ok(summary)
}

pub async fn persist_nuvio_registry_snapshot(
    store: &ExtensionStore<'_>,
    input: &NuvioRegistryStoreInput,
    snapshot: &NuvioRegistrySnapshot,
) -> Result<NuvioRegistryPersistSummary> {
    let registry_type = NuvioRegistryKind::from_registry_type(&input.registry_type)?;
    if registry_type.as_registry_type() != snapshot.registry_kind {
        bail!(
            "Nuvio snapshot kind '{}' does not match registry type '{}'",
            snapshot.registry_kind,
            input.registry_type
        );
    }
    let display_name = input
        .display_name
        .as_deref()
        .or(snapshot.repository.name.as_deref())
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
                "nuvio": {
                    "sourceUrl": snapshot.source_url,
                    "repository": snapshot.repository,
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

    let mut summary = NuvioRegistryPersistSummary {
        registries: 1,
        ..Default::default()
    };
    for module in &snapshot.modules {
        let module_key = source_module_key(&input.registry_key, &module.module_id);
        let source_module_id = deterministic_uuid(&format!(
            "elixir:nuvio:module:{}:{module_key}",
            input.instance_id
        ));
        let module_enabled = false;
        let health_state = if module.unsupported {
            "unsupported"
        } else if module.account_required {
            "account_required"
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
                ecosystem: "nuvio".to_string(),
                plugin_package: Some(module.module_id.clone()),
                active_version: None,
                rollback_version: None,
                media_types_json: Some(json!(module.media_types)),
                language_tags_json: Some(json!(module.language_tags)),
                region_tags_json: None,
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
                    "nuvio": {
                        "moduleId": module.module_id,
                        "adapter": "nuvio_js_v1",
                        "hasSettings": module.has_settings,
                        "author": module.author,
                        "description": module.description,
                        "formats": module.formats,
                        "logoUrl": module.logo_url,
                        "manifestUrl": module.manifest_url,
                        "raw": module.raw,
                    },
                    "registryKey": input.registry_key,
                })),
            })
            .await?;
        summary.modules += 1;

        let version_id = deterministic_uuid(&format!(
            "elixir:nuvio:module-version:{source_module_id}:{}:{}",
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
                signature: None,
                install_state: "available".to_string(),
                smoke_status: "unknown".to_string(),
                smoke_error: None,
                rollback_of_version_id: None,
                installed_at: None,
                activated_at: None,
                metadata_json: Some(json!({
                    "artifact": {
                        "kind": "javascript",
                        "filename": module.raw.get("filename").cloned(),
                    },
                    "nuvio": {
                        "moduleId": module.module_id,
                        "adapter": "nuvio_js_v1",
                        "manifestUrl": module.manifest_url,
                        "raw": module.raw,
                    }
                })),
            })
            .await?;
        summary.versions += 1;
    }
    Ok(summary)
}

async fn install_prism_recommended_source_pack_artifacts(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    storage_root: &str,
) -> Result<()> {
    let registries = store.list_source_registries(Some(instance_id)).await?;
    let Some(registry) = registries
        .iter()
        .find(|registry| registry.registry_key == PRISM_RECOMMENDED_REGISTRY_KEY)
    else {
        return Ok(());
    };
    let modules = store
        .list_source_modules(Some(instance_id), Some(registry.registry_id))
        .await?;
    for module in modules
        .into_iter()
        .filter(|module| module.enabled && module.installed && !module.unsupported)
    {
        let versions = store
            .list_source_module_versions(module.source_module_id)
            .await?;
        let selected_version = module
            .active_version
            .as_deref()
            .or(module.pinned_version.as_deref())
            .and_then(|version| {
                versions
                    .iter()
                    .find(|candidate| candidate.version == version)
            })
            .or_else(|| versions.first());
        let Some(version) = selected_version else {
            store
                .set_source_module_enabled_state(
                    module.source_module_id,
                    false,
                    "degraded",
                    Some("recommended source has no installable version"),
                )
                .await?;
            continue;
        };
        if version.artifact_sha256.is_none() {
            store
                .set_source_module_enabled_state(
                    module.source_module_id,
                    false,
                    "disabled",
                    Some("recommended source is not hash-pinned"),
                )
                .await?;
            continue;
        }
        if let Err(err) =
            install_source_module_artifact(store, storage_root, &module, version).await
        {
            let reason = err.to_string();
            store
                .set_source_module_enabled_state(
                    module.source_module_id,
                    false,
                    "degraded",
                    Some(&reason),
                )
                .await?;
            tracing::warn!(
                module = %module.display_name,
                error = %reason,
                "Prism recommended source artifact install failed"
            );
        }
    }
    Ok(())
}

async fn persist_prism_source_pack_repositories(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    pack: &PrismSourcePackManifest,
    source_url: &str,
    config: &NuvioRegistryFetchConfig,
    tombstoned_registry_keys: &HashSet<String>,
) -> Result<usize> {
    let mut count = 0usize;
    for repository in &pack.maintainer_known_repositories {
        let registry_key = repository.registry_key.trim().to_string();
        if tombstoned_registry_keys.contains(&registry_key) {
            continue;
        }
        let registry_id = deterministic_uuid(&format!(
            "elixir:prism:source-pack-known-registry:{instance_id}:{registry_key}"
        ));
        let trust_class = repository
            .trust_class
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("maintainer_known");
        let normalized_url = normalize_http_url(&repository.url, Some(source_url), config)?;
        store
            .upsert_source_registry(&NewExtensionSourceRegistry {
                registry_id,
                instance_id,
                registry_key,
                registry_type: repository.registry_type.clone(),
                trust_class: trust_class.to_string(),
                display_name: repository.display_name.clone(),
                url: Some(normalized_url),
                enabled: repository.enabled,
                auto_refresh: repository.auto_refresh,
                trusted_for_executable_updates: repository.trusted_for_executable_updates,
                etag: None,
                last_modified: None,
                metadata_json: Some(json!({
                    "prismSourcePack": {
                        "sourcePackId": pack.source_pack_id,
                        "version": pack.version,
                        "sourceUrl": source_url,
                        "knownRepository": repository,
                    },
                    "description": repository.description,
                    "metadata": repository.metadata,
                })),
            })
            .await?;
        count += 1;
    }
    Ok(count)
}

fn read_prism_recommended_source_pack_manifest(
    installed_package_dir: Option<&Path>,
) -> Result<String> {
    if let Some(package_dir) = installed_package_dir {
        let path = package_dir.join(PRISM_RECOMMENDED_SOURCE_PACK_PATH);
        if path.exists() {
            return fs::read_to_string(&path)
                .with_context(|| format!("reading Prism source pack {}", path.display()));
        }
    }
    Ok(BUNDLED_PRISM_RECOMMENDED_SOURCE_PACK.to_string())
}

fn prism_registry_source_pack_version(
    registry: &crate::extensions::store::ExtensionSourceRegistry,
) -> Option<String> {
    registry
        .metadata_json
        .as_ref()
        .and_then(|metadata| metadata.get("prismSourcePack"))
        .and_then(|pack| pack.get("version"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

async fn prism_source_pack_policy(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
) -> Result<PrismSourcePackPolicy> {
    let policy = store
        .get_instance(instance_id)
        .await?
        .and_then(|instance| instance.config_json)
        .and_then(|config| config.get("sourcePackPolicy").cloned())
        .and_then(|policy| policy.as_object().cloned())
        .unwrap_or_default();
    Ok(PrismSourcePackPolicy {
        recommended_pack_auto_enable: source_policy_bool(
            &policy,
            "recommendedPackAutoEnable",
            Some("curatedExecutableUpdates"),
            true,
        ),
        recommended_pack_executable_updates: source_policy_bool(
            &policy,
            "recommendedPackExecutableUpdates",
            Some("curatedBrokenModuleReplacement"),
            true,
        ),
    })
}

fn source_policy_bool(
    policy: &serde_json::Map<String, Value>,
    key: &str,
    legacy_key: Option<&str>,
    default: bool,
) -> bool {
    policy
        .get(key)
        .and_then(Value::as_bool)
        .or_else(|| legacy_key.and_then(|key| policy.get(key).and_then(Value::as_bool)))
        .unwrap_or(default)
}

fn prism_source_pack_module_health_state(
    module: &NuvioSourceModuleDescriptor,
    enabled: bool,
    hash_pinned: bool,
) -> &'static str {
    if module.unsupported {
        "unsupported"
    } else if module.account_required {
        "account_required"
    } else if !hash_pinned {
        "disabled"
    } else if enabled {
        "available"
    } else {
        "disabled"
    }
}

fn prism_recommendation_keys_by_module(
    registry_key: &str,
    pack: &PrismSourcePackManifest,
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

async fn persist_prism_source_pack_replacement_recommendations(
    store: &ExtensionStore<'_>,
    instance_id: Uuid,
    registry_id: Uuid,
    registry_key: &str,
    pack: &PrismSourcePackManifest,
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
            "elixir:prism:source-pack-recommendation:{}:{}",
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
                        "prismSourcePack": {
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

fn normalize_scraper_entry(
    value: Value,
    index: usize,
    source_url: &str,
    config: &NuvioRegistryFetchConfig,
) -> Result<Option<NuvioSourceModuleDescriptor>> {
    if !value.is_object() {
        bail!("Nuvio scraper entry must be an object");
    }
    let raw: RawScraper =
        serde_json::from_value(value.clone()).context("deserializing Nuvio scraper entry")?;
    let id = first_non_empty_owned([
        raw.id.as_ref().and_then(value_to_string),
        raw.name.as_ref().and_then(value_to_string),
    ])
    .unwrap_or_else(|| format!("nuvio-source-{}", index + 1));
    let display_name = first_non_empty_owned([
        raw.name.as_ref().and_then(value_to_string),
        Some(id.clone()),
    ])
    .unwrap_or_else(|| "Nuvio Source".to_string());
    let version = raw
        .version
        .as_ref()
        .and_then(value_to_string)
        .unwrap_or_else(|| "0.0.0".to_string());
    let artifact_url = first_non_empty_owned([
        raw.artifact_url.as_ref().and_then(value_to_string),
        raw.filename.as_ref().and_then(value_to_string),
    ])
    .map(|url| normalize_http_url(&url, Some(source_url), config))
    .transpose()?;
    let media_types = normalize_media_types(raw.supported_types.as_ref());
    let language_tags = normalize_string_tags(raw.content_language.as_ref())
        .into_iter()
        .map(normalize_language_tag)
        .filter(|tag| !tag.is_empty())
        .collect::<Vec<_>>();
    let formats = normalize_string_tags(raw.formats.as_ref());
    let mut source_domains = normalize_string_tags(raw.source_domains.as_ref());
    let logo_url = raw.logo.as_ref().and_then(value_to_string);
    for candidate in [artifact_url.as_deref(), logo_url.as_deref()]
        .into_iter()
        .flatten()
    {
        if let Some(host) = safe_host_from_url(candidate, config) {
            push_unique(&mut source_domains, host);
        }
    }
    source_domains.sort();
    let mut unsupported_reasons = Vec::new();
    if media_types.is_empty() {
        unsupported_reasons.push("no supported Nuvio media types".to_string());
    }
    if artifact_url.is_none() {
        unsupported_reasons.push("no JavaScript artifact URL or filename".to_string());
    }
    if raw.browser_required.unwrap_or(false) {
        unsupported_reasons.push("browser automation required".to_string());
    }
    if raw.captcha_required.unwrap_or(false) {
        unsupported_reasons.push("captcha required".to_string());
    }
    if raw.drm_required.unwrap_or(false) {
        unsupported_reasons.push("DRM-protected streams are not supported".to_string());
    }
    let disabled = !raw.enabled.unwrap_or(true);
    Ok(Some(NuvioSourceModuleDescriptor {
        module_id: stable_text_id(&id),
        display_name,
        version,
        artifact_url,
        artifact_sha256: raw
            .hash
            .as_ref()
            .and_then(value_to_string)
            .map(|hash| normalize_checksum(&hash)),
        media_types,
        language_tags,
        formats,
        source_domains,
        author: raw.author.as_ref().and_then(value_to_string),
        description: raw.description,
        logo_url,
        has_settings: raw.has_settings.unwrap_or(false),
        account_required: raw.requires_account.unwrap_or(false),
        disabled,
        unsupported: !unsupported_reasons.is_empty(),
        unsupported_reason: (!unsupported_reasons.is_empty())
            .then(|| unsupported_reasons.join(", ")),
        manifest_url: source_url.to_string(),
        raw: value,
    }))
}

fn dedupe_modules(
    modules: Vec<NuvioSourceModuleDescriptor>,
    warnings: &mut Vec<String>,
) -> Vec<NuvioSourceModuleDescriptor> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::with_capacity(modules.len());
    for module in modules {
        let key = module.module_id.to_ascii_lowercase();
        if !seen.insert(key.clone()) {
            warnings.push(format!(
                "nuvio_manifest_json:{}: duplicate source module id '{}' ignored",
                module.manifest_url, module.module_id
            ));
            continue;
        }
        deduped.push(module);
    }
    deduped.sort_by(|left, right| left.module_id.cmp(&right.module_id));
    deduped
}

fn synthesize_missing_provider_file_modules(
    modules: &mut Vec<NuvioSourceModuleDescriptor>,
    provider_entries: Vec<GitHubContentEntry>,
    source_url: &str,
    repository_version: Option<&str>,
    config: &NuvioRegistryFetchConfig,
    warnings: &mut Vec<String>,
) -> Result<()> {
    let mut existing_ids = modules
        .iter()
        .map(|module| module.module_id.to_ascii_lowercase())
        .collect::<HashSet<_>>();
    let mut seen_paths = HashSet::new();
    for entry in provider_entries {
        if !seen_paths.insert(entry.path.to_ascii_lowercase()) {
            continue;
        }
        let Some(provider_id) = provider_id_from_js_path(&entry.path) else {
            continue;
        };
        let module_id = stable_text_id(&provider_id);
        if !existing_ids.insert(module_id.to_ascii_lowercase()) {
            continue;
        }
        if modules.len() >= config.max_scrapers {
            bail!(
                "Nuvio provider-file discovery exceeded scraper limit {}",
                config.max_scrapers
            );
        }
        let supported_types = infer_provider_file_media_types(&provider_id);
        let raw = json!({
            "id": provider_id,
            "name": humanize_provider_id(&provider_id),
            "description": "Discovered from a repository provider file because manifest.json does not list this scraper.",
            "version": repository_version.unwrap_or("0.0.0"),
            "supportedTypes": supported_types,
            "filename": entry.path,
            "enabled": false,
            "formats": ["mp4", "mkv", "m3u8"],
            "contentLanguage": ["en"],
            "xPrismDiscoveredFromProviderFile": true,
        });
        match normalize_scraper_entry(raw, modules.len(), source_url, config) {
            Ok(Some(module)) => modules.push(module),
            Ok(None) => warnings.push(format!(
                "nuvio_manifest_json:{source_url}: skipped discovered provider file {}",
                entry.path
            )),
            Err(err) => warnings.push(format!(
                "nuvio_manifest_json:{source_url}: discovered provider file {} rejected: {err}",
                entry.path
            )),
        }
    }
    Ok(())
}

fn provider_id_from_js_path(path: &str) -> Option<String> {
    let file = path.rsplit('/').next()?.trim();
    let stem = file.strip_suffix(".js")?.trim();
    if stem.is_empty() {
        return None;
    }
    let lower = stem.to_ascii_lowercase();
    if matches!(
        lower.as_str(),
        "index" | "test" | "server" | "build" | "extractor" | "http" | "utils" | "common"
    ) {
        return None;
    }
    Some(stem.to_string())
}

fn infer_provider_file_media_types(provider_id: &str) -> Vec<String> {
    let lower = provider_id.to_ascii_lowercase();
    if lower.contains("anime")
        || lower.contains("anichi")
        || lower.contains("hianime")
        || lower.contains("kickassanime")
        || lower.contains("donghua")
        || lower.contains("toon")
        || lower.contains("cartoon")
        || lower.contains("onepace")
        || lower.contains("tokusatsu")
        || lower.contains("tokuzilla")
    {
        vec!["anime".to_string(), "tv".to_string(), "movie".to_string()]
    } else {
        vec!["movie".to_string(), "tv".to_string()]
    }
}

fn humanize_provider_id(provider_id: &str) -> String {
    let mut words = Vec::new();
    let mut current = String::new();
    for ch in provider_id.chars() {
        if ch.is_ascii_alphanumeric() {
            current.push(ch);
        } else if !current.is_empty() {
            words.push(current.clone());
            current.clear();
        }
    }
    if !current.is_empty() {
        words.push(current);
    }
    if words.is_empty() {
        return provider_id.to_string();
    }
    words
        .into_iter()
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                Some(first) => {
                    let mut out = first.to_ascii_uppercase().to_string();
                    out.push_str(chars.as_str());
                    out
                }
                None => word,
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn normalize_nuvio_manifest_url(input: &str, config: &NuvioRegistryFetchConfig) -> Result<String> {
    let normalized = normalize_http_url(input, None, config)?;
    let mut url = Url::parse(&normalized).context("parsing normalized Nuvio registry URL")?;
    let Some(host) = url.host_str() else {
        return Ok(normalized);
    };
    if host.eq_ignore_ascii_case("github.com") {
        return github_page_url_to_raw_manifest_url(&url, config).unwrap_or(Ok(normalized));
    }
    if !host.eq_ignore_ascii_case("raw.githubusercontent.com") {
        return Ok(normalized);
    }
    let path = url.path();
    if path.ends_with(".json") {
        return Ok(normalized);
    }
    let manifest_path = format!("{}/manifest.json", path.trim_end_matches('/'));
    url.set_path(&manifest_path);
    Ok(url.to_string())
}

fn github_page_url_to_raw_manifest_url(
    url: &Url,
    config: &NuvioRegistryFetchConfig,
) -> Option<Result<String>> {
    let segments = url.path_segments()?.collect::<Vec<_>>();
    if segments.len() < 2 {
        return None;
    }
    let owner = segments[0];
    let repository = segments[1];
    if owner.is_empty() || repository.is_empty() {
        return None;
    }
    let (reference, root_path, is_file) = match segments.get(2).copied() {
        Some("blob") => {
            if segments.len() < 5 {
                return None;
            }
            (segments[3], segments[4..].join("/"), true)
        }
        Some("tree") => {
            if segments.len() < 4 {
                return None;
            }
            (
                segments[3],
                segments.get(4..).unwrap_or_default().join("/"),
                false,
            )
        }
        Some(_) | None => ("main", String::new(), false),
    };
    let manifest_path = if is_file {
        root_path
    } else {
        join_url_path(&root_path, "manifest.json")
    };
    let raw = format!(
        "https://raw.githubusercontent.com/{owner}/{repository}/refs/heads/{reference}/{manifest_path}"
    );
    Some(normalize_http_url(&raw, None, config))
}

fn github_raw_repository_context(source_url: &str) -> Option<GitHubRawRepositoryContext> {
    let url = Url::parse(source_url).ok()?;
    let host = url.host_str()?;
    if !host.eq_ignore_ascii_case("raw.githubusercontent.com") {
        return None;
    }
    let segments = url.path_segments()?.collect::<Vec<_>>();
    if segments.len() < 4 {
        return None;
    }
    let owner = segments[0].to_string();
    let repository = segments[1].to_string();
    let (reference, file_index) =
        if segments.get(2) == Some(&"refs") && segments.get(3) == Some(&"heads") {
            if segments.len() < 6 {
                return None;
            }
            (segments[4].to_string(), 5usize)
        } else {
            (segments[2].to_string(), 3usize)
        };
    let file_path = segments[file_index..].join("/");
    if !file_path.ends_with("manifest.json") {
        return None;
    }
    let root_path = file_path
        .strip_suffix("manifest.json")
        .unwrap_or("")
        .trim_matches('/')
        .to_string();
    Some(GitHubRawRepositoryContext {
        owner,
        repository,
        reference,
        root_path,
    })
}

fn join_url_path(left: &str, right: &str) -> String {
    match (left.trim_matches('/'), right.trim_matches('/')) {
        ("", right) => right.to_string(),
        (left, "") => left.to_string(),
        (left, right) => format!("{left}/{right}"),
    }
}

fn normalize_http_url(
    input: &str,
    base_url: Option<&str>,
    config: &NuvioRegistryFetchConfig,
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
    if let Ok(ip) = lower.parse() {
        validate_public_source_ip(ip, "Nuvio registry")?;
    }
    Ok(())
}

fn safe_host_from_url(url: &str, config: &NuvioRegistryFetchConfig) -> Option<String> {
    let normalized = normalize_http_url(url, None, config).ok()?;
    let parsed = Url::parse(&normalized).ok()?;
    parsed
        .host_str()
        .map(|host| host.trim_start_matches("www.").to_ascii_lowercase())
}

fn normalize_media_types(value: Option<&Value>) -> Vec<String> {
    let mut output = Vec::new();
    for tag in normalize_string_tags(value) {
        let normalized = match tag.trim().to_ascii_lowercase().as_str() {
            "movie" | "movies" | "film" => "movie",
            "tv" | "show" | "shows" | "series" | "episode" | "episodes" => "tv",
            "anime" => "anime",
            _ => continue,
        };
        push_unique(&mut output, normalized.to_string());
    }
    output
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

fn normalize_language_tag(value: String) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "eng" | "english" => "en".to_string(),
        "jpn" | "japanese" | "ja-jp" => "ja".to_string(),
        "hin" | "hindi" => "hi".to_string(),
        other => other.to_string(),
    }
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

fn first_non_empty_owned<I>(values: I) -> Option<String>
where
    I: IntoIterator<Item = Option<String>>,
{
    values
        .into_iter()
        .flatten()
        .map(|value| value.trim().to_string())
        .find(|value| !value.is_empty())
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

fn normalize_checksum(value: &str) -> String {
    value
        .trim()
        .strip_prefix("sha256-")
        .or_else(|| value.trim().strip_prefix("sha256:"))
        .unwrap_or(value.trim())
        .to_ascii_lowercase()
}

fn push_unique(values: &mut Vec<String>, value: String) {
    let value = value.trim().to_string();
    if value.is_empty() {
        return;
    }
    if !values
        .iter()
        .any(|existing| existing.eq_ignore_ascii_case(&value))
    {
        values.push(value);
    }
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
        "nuvio:{}:{}",
        stable_text_id(registry_key),
        stable_text_id(module_id)
    )
}

fn deterministic_uuid(key: &str) -> Uuid {
    Uuid::new_v5(&Uuid::NAMESPACE_URL, key.as_bytes())
}

fn default_true() -> bool {
    true
}

fn default_nuvio_manifest_registry_type() -> String {
    "nuvio_manifest_json".to_string()
}

fn ensure_non_empty(value: &str, field: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{field} must not be empty");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    #[test]
    fn nuvio_registry_redirect_target_allows_public_relative_redirects() -> Result<()> {
        let current = Url::parse("https://raw.example.test/repos/manifest.json")?;
        let redirected =
            checked_nuvio_registry_redirect_target(&current, "../next/manifest.json", false)?;
        assert_eq!(
            redirected.as_str(),
            "https://raw.example.test/next/manifest.json"
        );
        Ok(())
    }

    #[test]
    fn nuvio_registry_redirect_target_rejects_private_destinations() {
        let current = Url::parse("https://raw.example.test/repos/manifest.json").unwrap();
        let err = checked_nuvio_registry_redirect_target(
            &current,
            "http://10.0.0.5/manifest.json",
            false,
        )
        .expect_err("private registry redirect should be rejected");
        assert!(
            err.to_string()
                .contains("blocked unsafe Nuvio registry redirect target")
        );
    }

    #[test]
    fn parses_nuvio_manifest_and_resolves_relative_artifacts() -> Result<()> {
        let snapshot = parse_nuvio_manifest_json(
            r#"{
                "name": "Fixture Repo",
                "version": "1.0.0",
                "scrapers": [{
                    "id": "MoviesDrive",
                    "name": "MoviesDrive",
                    "version": "1.1.1",
                    "supportedTypes": ["movie", "tv"],
                    "filename": "src/providers/moviesdrive.js",
                    "enabled": true,
                    "formats": ["mp4", "m3u8"],
                    "contentLanguage": ["English"]
                }]
            }"#,
            "https://raw.githubusercontent.com/example/repo/main/manifest.json",
            &NuvioRegistryFetchConfig::default(),
        )?;
        assert_eq!(snapshot.modules.len(), 1);
        let module = &snapshot.modules[0];
        assert_eq!(module.module_id, "moviesdrive");
        assert_eq!(module.media_types, vec!["movie", "tv"]);
        assert_eq!(module.language_tags, vec!["en"]);
        assert_eq!(
            module.artifact_url.as_deref(),
            Some(
                "https://raw.githubusercontent.com/example/repo/main/src/providers/moviesdrive.js"
            )
        );
        assert!(!module.unsupported);
        Ok(())
    }

    #[test]
    fn parses_top_level_array_nuvio_manifest() -> Result<()> {
        let snapshot = parse_nuvio_manifest_json(
            r#"[{
                "id": "ArrayProvider",
                "name": "Array Provider",
                "version": "2.0.0",
                "mediaTypes": ["movie"],
                "path": "providers/array-provider.js"
            }]"#,
            "https://raw.githubusercontent.com/example/repo/main/manifest.json",
            &NuvioRegistryFetchConfig::default(),
        )?;
        assert_eq!(snapshot.modules.len(), 1);
        let module = &snapshot.modules[0];
        assert_eq!(module.module_id, "arrayprovider");
        assert_eq!(module.display_name, "Array Provider");
        assert_eq!(module.media_types, vec!["movie"]);
        assert_eq!(
            module.artifact_url.as_deref(),
            Some("https://raw.githubusercontent.com/example/repo/main/providers/array-provider.js")
        );
        Ok(())
    }

    #[test]
    fn normalizes_raw_github_repo_root_to_manifest_json() -> Result<()> {
        let normalized = normalize_nuvio_manifest_url(
            "https://raw.githubusercontent.com/phisher98/phisher-nuvio-providers/refs/heads/main/",
            &NuvioRegistryFetchConfig::default(),
        )?;
        assert_eq!(
            normalized,
            "https://raw.githubusercontent.com/phisher98/phisher-nuvio-providers/refs/heads/main/manifest.json"
        );
        Ok(())
    }

    #[test]
    fn normalizes_github_page_urls_to_raw_manifest_json() -> Result<()> {
        let config = NuvioRegistryFetchConfig::default();
        let root = normalize_nuvio_manifest_url(
            "https://github.com/phisher98/phisher-nuvio-providers",
            &config,
        )?;
        assert_eq!(
            root,
            "https://raw.githubusercontent.com/phisher98/phisher-nuvio-providers/refs/heads/main/manifest.json"
        );

        let tree = normalize_nuvio_manifest_url(
            "https://github.com/phisher98/phisher-nuvio-providers/tree/main",
            &config,
        )?;
        assert_eq!(
            tree,
            "https://raw.githubusercontent.com/phisher98/phisher-nuvio-providers/refs/heads/main/manifest.json"
        );

        let blob = normalize_nuvio_manifest_url(
            "https://github.com/phisher98/phisher-nuvio-providers/blob/main/manifest.json",
            &config,
        )?;
        assert_eq!(
            blob,
            "https://raw.githubusercontent.com/phisher98/phisher-nuvio-providers/refs/heads/main/manifest.json"
        );
        Ok(())
    }

    #[test]
    fn extracts_raw_github_repository_context() {
        let context = github_raw_repository_context(
            "https://raw.githubusercontent.com/phisher98/phisher-nuvio-providers/refs/heads/main/manifest.json",
        )
        .expect("raw GitHub manifest should be recognized");
        assert_eq!(context.owner, "phisher98");
        assert_eq!(context.repository, "phisher-nuvio-providers");
        assert_eq!(context.reference, "main");
        assert_eq!(context.root_path, "");
    }

    #[tokio::test]
    async fn nuvio_github_provider_directory_follows_checked_redirects() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut socket, _) = listener.accept().await?;
                let mut buffer = vec![0u8; 2048];
                let read = socket.read(&mut buffer).await?;
                let request = String::from_utf8_lossy(&buffer[..read]);
                if request.starts_with("GET /start ") {
                    socket
                        .write_all(
                            b"HTTP/1.1 302 Found\r\nLocation: /final\r\nContent-Length: 0\r\n\r\n",
                        )
                        .await?;
                } else {
                    let body =
                        r#"[{"name":"allwish.js","path":"providers/allwish.js","type":"file"}]"#;
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    socket.write_all(response.as_bytes()).await?;
                }
            }
            Ok::<(), anyhow::Error>(())
        });

        let client = NuvioRegistryClient::new(NuvioRegistryFetchConfig {
            allow_private_hosts: true,
            max_response_bytes: 4096,
            ..NuvioRegistryFetchConfig::default()
        })?;
        let entries = client
            .fetch_github_provider_directory_url(Url::parse(&format!(
                "http://127.0.0.1:{port}/start"
            ))?)
            .await?;

        server.await??;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "allwish.js");
        assert_eq!(entries[0].path, "providers/allwish.js");
        Ok(())
    }

    #[tokio::test]
    async fn nuvio_github_provider_directory_rejects_oversized_response() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let mut buffer = vec![0u8; 1024];
            let _ = socket.read(&mut buffer).await?;
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 128\r\n\r\n[]",
                )
                .await?;
            Ok::<(), anyhow::Error>(())
        });

        let client = NuvioRegistryClient::new(NuvioRegistryFetchConfig {
            allow_private_hosts: true,
            max_response_bytes: 16,
            ..NuvioRegistryFetchConfig::default()
        })?;
        let err = client
            .fetch_github_provider_directory_url(Url::parse(&format!(
                "http://127.0.0.1:{port}/oversized"
            ))?)
            .await
            .expect_err("oversized GitHub directory response should fail");

        server.await??;
        assert!(err.to_string().contains("too large"));
        Ok(())
    }

    #[test]
    fn synthesizes_missing_provider_file_modules_without_duplicates() -> Result<()> {
        let config = NuvioRegistryFetchConfig::default();
        let mut modules = vec![
            normalize_scraper_entry(
                json!({
                    "id": "MoviesDrive",
                    "name": "MoviesDrive",
                    "version": "1.1.1",
                    "supportedTypes": ["movie", "tv"],
                    "filename": "src/providers/moviesdrive.js",
                    "enabled": true
                }),
                0,
                "https://raw.githubusercontent.com/phisher98/phisher-nuvio-providers/refs/heads/main/manifest.json",
                &config,
            )?
            .expect("fixture module should normalize"),
        ];
        let mut warnings = Vec::new();
        synthesize_missing_provider_file_modules(
            &mut modules,
            vec![
                GitHubContentEntry {
                    name: "moviesdrive.js".to_string(),
                    path: "src/providers/moviesdrive.js".to_string(),
                    entry_type: "file".to_string(),
                },
                GitHubContentEntry {
                    name: "cinefreak.js".to_string(),
                    path: "src/providers/cinefreak.js".to_string(),
                    entry_type: "file".to_string(),
                },
            ],
            "https://raw.githubusercontent.com/phisher98/phisher-nuvio-providers/refs/heads/main/manifest.json",
            Some("1.0.0"),
            &config,
            &mut warnings,
        )?;
        assert_eq!(modules.len(), 2);
        let cinefreak = modules
            .iter()
            .find(|module| module.module_id == "cinefreak")
            .expect("cinefreak should be synthesized");
        assert_eq!(cinefreak.display_name, "Cinefreak");
        assert_eq!(cinefreak.media_types, vec!["movie", "tv"]);
        assert_eq!(
            cinefreak.artifact_url.as_deref(),
            Some(
                "https://raw.githubusercontent.com/phisher98/phisher-nuvio-providers/refs/heads/main/src/providers/cinefreak.js"
            )
        );
        assert!(warnings.is_empty());
        Ok(())
    }

    #[test]
    fn rejects_private_registry_urls() {
        let err = normalize_http_url(
            "http://127.0.0.1/manifest.json",
            None,
            &NuvioRegistryFetchConfig::default(),
        )
        .expect_err("private URL should be rejected");
        assert!(err.to_string().contains("private") || err.to_string().contains("local"));
    }

    #[test]
    fn parses_bundled_prism_recommended_source_pack() -> Result<()> {
        let pack = parse_prism_source_pack_manifest(
            BUNDLED_PRISM_RECOMMENDED_SOURCE_PACK,
            "https://elixir.media/source-packs/prism/recommended.json",
            &NuvioRegistryFetchConfig::default(),
        )?;
        assert_eq!(pack.source_pack_id, PRISM_RECOMMENDED_SOURCE_PACK_ID);
        assert_eq!(
            pack.registry_key.as_deref(),
            Some(PRISM_RECOMMENDED_REGISTRY_KEY)
        );
        assert!(pack.enabled_by_default);
        assert!(pack.trusted_for_executable_updates);
        assert_eq!(pack.maintainer_known_repositories.len(), 1);
        let known_repo = &pack.maintainer_known_repositories[0];
        assert_eq!(known_repo.registry_key, "prism.repo.phisher.nuvio");
        assert_eq!(known_repo.registry_type, "nuvio_manifest_json");
        assert_eq!(known_repo.trust_class.as_deref(), Some("maintainer_known"));
        assert!(!known_repo.trusted_for_executable_updates);
        assert!(!pack.modules.is_empty());
        for module in &pack.modules {
            let normalized = normalize_scraper_entry(
                module.clone(),
                0,
                "https://elixir.media/source-packs/prism/recommended.json",
                &NuvioRegistryFetchConfig::default(),
            )?
            .expect("recommended module should normalize");
            assert!(normalized.artifact_url.is_some());
            assert!(
                normalized
                    .artifact_sha256
                    .as_deref()
                    .is_some_and(|hash| hash.len() == 64)
            );
            assert!(!normalized.media_types.is_empty());
        }
        Ok(())
    }
}
