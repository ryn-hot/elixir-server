use std::collections::HashMap;

use anyhow::{Context, Result, anyhow, bail};
use reqwest::Url;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub enum DriverPatch {
    MediaManagerTv(MediaManagerTvPatch),
    IndexerRegistry(IndexerRegistryPatch),
    DownloaderTorrent(DownloaderTorrentPatch),
}

impl DriverPatch {
    pub fn capability(&self) -> &'static str {
        match self {
            DriverPatch::MediaManagerTv(_) => "media.manager.tv",
            DriverPatch::IndexerRegistry(_) => "indexer.registry",
            DriverPatch::DownloaderTorrent(_) => "downloader.torrent",
        }
    }

    pub fn validate(&self) -> Result<()> {
        match self {
            DriverPatch::MediaManagerTv(patch) => patch.validate(),
            DriverPatch::IndexerRegistry(patch) => patch.validate(),
            DriverPatch::DownloaderTorrent(patch) => patch.validate(),
        }
    }

    pub fn from_manifest(capability: &str, patch: serde_json::Value) -> Result<Self> {
        match capability {
            "media.manager.tv" => {
                let patch: MediaManagerTvPatch =
                    serde_json::from_value(patch).context("parsing media.manager.tv patch")?;
                Ok(DriverPatch::MediaManagerTv(patch))
            }
            "indexer.registry" => {
                let patch: IndexerRegistryPatch =
                    serde_json::from_value(patch).context("parsing indexer.registry patch")?;
                Ok(DriverPatch::IndexerRegistry(patch))
            }
            "downloader.torrent" => {
                let patch: DownloaderTorrentPatch =
                    serde_json::from_value(patch).context("parsing downloader.torrent patch")?;
                Ok(DriverPatch::DownloaderTorrent(patch))
            }
            _ => bail!("no driver patch type registered for capability '{capability}'"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum MediaManagerTvPatch {
    SetIndexerRegistry {
        indexers: Vec<IndexerSpec>,
    },
    SetDownloaders {
        downloaders: Vec<DownloaderSpec>,
    },
    SetRootFolders {
        roots: Vec<RootFolderSpec>,
    },
    SetQualityProfiles {
        profiles: Vec<QualityProfileSpec>,
    },
    SetLanguageProfiles {
        profiles: Vec<LanguageProfileSpec>,
    },
    SetSeriesTypeDefaults {
        defaults: SeriesTypeDefaultsSpec,
    },
    SetTags {
        tags: Vec<String>,
    },
    AssignTags {
        series_ids: Vec<i64>,
        tags: Vec<String>,
    },
    SetWebhooks {
        webhooks: Vec<WebhookSpec>,
    },
    SetCustomFormats {
        #[serde(default)]
        formats: Vec<CustomFormatSpec>,
        #[serde(default)]
        release_profiles: Vec<ReleaseProfileSpec>,
    },
    SetAuxServiceEndpoint {
        url: String,
    },
}

impl MediaManagerTvPatch {
    pub fn validate(&self) -> Result<()> {
        match self {
            MediaManagerTvPatch::SetIndexerRegistry { indexers } => {
                ensure_non_empty_list(indexers, "indexers")?;
                for indexer in indexers {
                    indexer.validate()?;
                }
                Ok(())
            }
            MediaManagerTvPatch::SetDownloaders { downloaders } => {
                ensure_non_empty_list(downloaders, "downloaders")?;
                for downloader in downloaders {
                    downloader.validate()?;
                }
                Ok(())
            }
            MediaManagerTvPatch::SetRootFolders { roots } => {
                ensure_non_empty_list(roots, "roots")?;
                for root in roots {
                    root.validate()?;
                }
                Ok(())
            }
            MediaManagerTvPatch::SetQualityProfiles { profiles } => {
                ensure_non_empty_list(profiles, "profiles")?;
                for profile in profiles {
                    profile.validate()?;
                }
                Ok(())
            }
            MediaManagerTvPatch::SetLanguageProfiles { profiles } => {
                ensure_non_empty_list(profiles, "profiles")?;
                for profile in profiles {
                    profile.validate()?;
                }
                Ok(())
            }
            MediaManagerTvPatch::SetSeriesTypeDefaults { defaults } => defaults.validate(),
            MediaManagerTvPatch::SetTags { tags } => validate_tags(tags, "tags"),
            MediaManagerTvPatch::AssignTags { series_ids, tags } => {
                ensure_non_empty_list(series_ids, "series_ids")?;
                validate_tags(tags, "tags")
            }
            MediaManagerTvPatch::SetWebhooks { webhooks } => {
                ensure_non_empty_list(webhooks, "webhooks")?;
                for webhook in webhooks {
                    webhook.validate()?;
                }
                Ok(())
            }
            MediaManagerTvPatch::SetCustomFormats {
                formats,
                release_profiles,
            } => {
                if formats.is_empty() && release_profiles.is_empty() {
                    bail!("custom formats or release profiles are required");
                }
                for format in formats {
                    format.validate()?;
                }
                for profile in release_profiles {
                    profile.validate()?;
                }
                Ok(())
            }
            MediaManagerTvPatch::SetAuxServiceEndpoint { url } => validate_url(url),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum IndexerRegistryPatch {
    RegisterIndexers {
        indexers: Vec<IndexerSpec>,
    },
}

impl IndexerRegistryPatch {
    pub fn validate(&self) -> Result<()> {
        match self {
            IndexerRegistryPatch::RegisterIndexers { indexers } => {
                ensure_non_empty_list(indexers, "indexers")?;
                for indexer in indexers {
                    indexer.validate()?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum DownloaderTorrentPatch {
    SetCategories {
        categories: Vec<DownloadCategorySpec>,
    },
    SetPreferences {
        #[serde(default)]
        default_save_path: Option<String>,
        #[serde(default)]
        incomplete_path: Option<String>,
        #[serde(default)]
        use_incomplete: Option<bool>,
    },
}

impl DownloaderTorrentPatch {
    pub fn validate(&self) -> Result<()> {
        match self {
            DownloaderTorrentPatch::SetCategories { categories } => {
                ensure_non_empty_list(categories, "categories")?;
                for category in categories {
                    category.validate()?;
                }
                Ok(())
            }
            DownloaderTorrentPatch::SetPreferences {
                default_save_path,
                incomplete_path,
                use_incomplete,
            } => {
                if default_save_path.is_none()
                    && incomplete_path.is_none()
                    && use_incomplete.is_none()
                {
                    bail!("preferences must include at least one value");
                }
                ensure_optional_non_empty(
                    default_save_path.as_deref(),
                    "default_save_path",
                )?;
                ensure_optional_non_empty(incomplete_path.as_deref(), "incomplete_path")?;
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexerSpec {
    pub name: String,
    pub implementation: String,
    pub url: String,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub categories: Vec<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub settings: HashMap<String, serde_json::Value>,
}

impl IndexerSpec {
    fn validate(&self) -> Result<()> {
        ensure_non_empty(&self.name, "indexer.name")?;
        ensure_non_empty(&self.implementation, "indexer.implementation")?;
        validate_url(&self.url)?;
        ensure_optional_non_empty(self.api_key.as_deref(), "indexer.api_key")?;
        validate_tags(&self.categories, "indexer.categories")?;
        validate_tags(&self.tags, "indexer.tags")?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloaderSpec {
    pub name: String,
    #[serde(rename = "type")]
    pub r#type: String,
    pub url: String,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub category: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub settings: HashMap<String, serde_json::Value>,
}

impl DownloaderSpec {
    fn validate(&self) -> Result<()> {
        ensure_non_empty(&self.name, "downloader.name")?;
        ensure_non_empty(&self.r#type, "downloader.type")?;
        validate_url(&self.url)?;
        ensure_optional_non_empty(self.api_key.as_deref(), "downloader.api_key")?;
        ensure_optional_non_empty(self.category.as_deref(), "downloader.category")?;
        validate_tags(&self.tags, "downloader.tags")?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadCategorySpec {
    pub name: String,
    #[serde(default)]
    pub save_path: Option<String>,
}

impl DownloadCategorySpec {
    fn validate(&self) -> Result<()> {
        ensure_non_empty(&self.name, "category.name")?;
        ensure_optional_non_empty(self.save_path.as_deref(), "category.save_path")?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RootFolderSpec {
    pub path: String,
    #[serde(default)]
    pub default: bool,
}

impl RootFolderSpec {
    fn validate(&self) -> Result<()> {
        ensure_non_empty(&self.path, "root_folder.path")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityProfileSpec {
    pub name: String,
    #[serde(default)]
    pub cutoff: Option<String>,
    #[serde(default)]
    pub allowed: Vec<String>,
    #[serde(default)]
    pub upgrade_allowed: Option<bool>,
}

impl QualityProfileSpec {
    fn validate(&self) -> Result<()> {
        ensure_non_empty(&self.name, "quality_profile.name")?;
        ensure_optional_non_empty(self.cutoff.as_deref(), "quality_profile.cutoff")?;
        ensure_non_empty_list(&self.allowed, "quality_profile.allowed")?;
        validate_tags(&self.allowed, "quality_profile.allowed")?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LanguageProfileSpec {
    pub name: String,
    #[serde(default)]
    pub languages: Vec<String>,
    #[serde(default)]
    pub cutoff: Option<String>,
}

impl LanguageProfileSpec {
    fn validate(&self) -> Result<()> {
        ensure_non_empty(&self.name, "language_profile.name")?;
        ensure_non_empty_list(&self.languages, "language_profile.languages")?;
        validate_tags(&self.languages, "language_profile.languages")?;
        ensure_optional_non_empty(self.cutoff.as_deref(), "language_profile.cutoff")?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesTypeDefaultsSpec {
    pub series_type: String,
    #[serde(default)]
    pub season_folder: bool,
    #[serde(default)]
    pub quality_profile: Option<String>,
    #[serde(default)]
    pub language_profile: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
}

impl SeriesTypeDefaultsSpec {
    fn validate(&self) -> Result<()> {
        ensure_non_empty(&self.series_type, "series_type")?;
        let normalized = self.series_type.trim().to_ascii_lowercase();
        if normalized != "standard" && normalized != "anime" && normalized != "daily" {
            bail!(
                "series_type must be one of 'standard', 'anime', or 'daily'"
            );
        }
        ensure_optional_non_empty(self.quality_profile.as_deref(), "quality_profile")?;
        ensure_optional_non_empty(self.language_profile.as_deref(), "language_profile")?;
        validate_tags(&self.tags, "series_type_defaults.tags")?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookSpec {
    pub name: String,
    pub url: String,
    #[serde(default)]
    pub events: Vec<String>,
    #[serde(default)]
    pub enabled: Option<bool>,
}

impl WebhookSpec {
    fn validate(&self) -> Result<()> {
        ensure_non_empty(&self.name, "webhook.name")?;
        validate_url(&self.url)?;
        ensure_non_empty_list(&self.events, "webhook.events")?;
        validate_tags(&self.events, "webhook.events")?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomFormatSpec {
    pub name: String,
    #[serde(default)]
    pub include: Vec<String>,
    #[serde(default)]
    pub exclude: Vec<String>,
    #[serde(default)]
    pub score: Option<i32>,
}

impl CustomFormatSpec {
    fn validate(&self) -> Result<()> {
        ensure_non_empty(&self.name, "custom_format.name")?;
        if self.include.is_empty() && self.exclude.is_empty() {
            bail!("custom_format include or exclude entries are required");
        }
        validate_tags(&self.include, "custom_format.include")?;
        validate_tags(&self.exclude, "custom_format.exclude")?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseProfileSpec {
    pub name: String,
    #[serde(default)]
    pub required: Vec<String>,
    #[serde(default)]
    pub ignored: Vec<String>,
    #[serde(default)]
    pub preferred: Vec<String>,
}

impl ReleaseProfileSpec {
    fn validate(&self) -> Result<()> {
        ensure_non_empty(&self.name, "release_profile.name")?;
        if self.required.is_empty() && self.ignored.is_empty() && self.preferred.is_empty() {
            bail!("release_profile requires at least one rule");
        }
        validate_tags(&self.required, "release_profile.required")?;
        validate_tags(&self.ignored, "release_profile.ignored")?;
        validate_tags(&self.preferred, "release_profile.preferred")?;
        Ok(())
    }
}

fn validate_url(value: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("url is required");
    }
    let parsed = Url::parse(value).context("parsing url")?;
    match parsed.scheme() {
        "http" | "https" => {}
        scheme => bail!("unsupported url scheme '{scheme}'"),
    }
    let host = parsed
        .host_str()
        .ok_or_else(|| anyhow!("url host is required"))?;
    validate_host(host)?;
    Ok(())
}

fn validate_host(host: &str) -> Result<()> {
    let trimmed = host.trim();
    if trimmed.is_empty() {
        bail!("host is required");
    }
    let lowered = trimmed.to_ascii_lowercase();
    if matches!(
        lowered.as_str(),
        "localhost" | "127.0.0.1" | "0.0.0.0" | "::1" | "host.docker.internal"
    ) {
        bail!("host '{}' is not allowed", trimmed);
    }
    Ok(())
}

fn ensure_non_empty(value: &str, field: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{field} is required");
    }
    Ok(())
}

fn ensure_optional_non_empty(value: Option<&str>, field: &str) -> Result<()> {
    if let Some(value) = value {
        ensure_non_empty(value, field)?;
    }
    Ok(())
}

fn ensure_non_empty_list<T>(values: &[T], field: &str) -> Result<()> {
    if values.is_empty() {
        bail!("{field} is required");
    }
    Ok(())
}

fn validate_tags(values: &[String], field: &str) -> Result<()> {
    for value in values {
        ensure_non_empty(value, field)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn media_manager_tv_patch_requires_url() {
        let patch = MediaManagerTvPatch::SetIndexerRegistry {
            indexers: vec![IndexerSpec {
                name: "test".to_string(),
                implementation: "torznab".to_string(),
                url: "".to_string(),
                api_key: None,
                categories: Vec::new(),
                tags: Vec::new(),
                enabled: None,
                settings: HashMap::new(),
            }],
        };
        assert!(patch.validate().is_err());
    }

    #[test]
    fn media_manager_tv_patch_rejects_localhost() {
        let patch = MediaManagerTvPatch::SetWebhooks {
            webhooks: vec![WebhookSpec {
                name: "test".to_string(),
                url: "http://localhost:8989".to_string(),
                events: vec!["grab".to_string()],
                enabled: None,
            }],
        };
        assert!(patch.validate().is_err());
    }

    #[test]
    fn indexer_registry_patch_requires_indexers() {
        let patch = IndexerRegistryPatch::RegisterIndexers { indexers: Vec::new() };
        assert!(patch.validate().is_err());
    }

    #[test]
    fn downloader_torrent_patch_requires_categories() {
        let patch = DownloaderTorrentPatch::SetCategories { categories: Vec::new() };
        assert!(patch.validate().is_err());
    }

    #[test]
    fn downloader_torrent_patch_requires_preferences() {
        let patch = DownloaderTorrentPatch::SetPreferences {
            default_save_path: None,
            incomplete_path: None,
            use_incomplete: None,
        };
        assert!(patch.validate().is_err());
    }
}
