use std::collections::HashMap;

use anyhow::{Context, Result, anyhow, bail};
use reqwest::Url;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub enum DriverPatch {
    MediaManagerTv(MediaManagerTvPatch),
    MediaManagerMovies(MediaManagerMoviesPatch),
    IndexerRegistry(IndexerRegistryPatch),
    DownloaderTorrent(DownloaderTorrentPatch),
    DownloaderNzb(DownloaderNzbPatch),
}

impl DriverPatch {
    pub fn capability(&self) -> &'static str {
        match self {
            DriverPatch::MediaManagerTv(_) => "media.manager.tv",
            DriverPatch::MediaManagerMovies(_) => "media.manager.movies",
            DriverPatch::IndexerRegistry(_) => "indexer.registry",
            DriverPatch::DownloaderTorrent(_) => "downloader.torrent",
            DriverPatch::DownloaderNzb(_) => "downloader.nzb",
        }
    }

    pub fn validate(&self) -> Result<()> {
        match self {
            DriverPatch::MediaManagerTv(patch) => patch.validate(),
            DriverPatch::MediaManagerMovies(patch) => patch.validate(),
            DriverPatch::IndexerRegistry(patch) => patch.validate(),
            DriverPatch::DownloaderTorrent(patch) => patch.validate(),
            DriverPatch::DownloaderNzb(patch) => patch.validate(),
        }
    }

    pub fn from_manifest(capability: &str, patch: serde_json::Value) -> Result<Self> {
        match capability {
            "media.manager.tv" => {
                let patch: MediaManagerTvPatch =
                    serde_json::from_value(patch).context("parsing media.manager.tv patch")?;
                Ok(DriverPatch::MediaManagerTv(patch))
            }
            "media.manager.movies" => {
                let patch: MediaManagerMoviesPatch =
                    serde_json::from_value(patch).context("parsing media.manager.movies patch")?;
                Ok(DriverPatch::MediaManagerMovies(patch))
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
            "downloader.nzb" => {
                let patch: DownloaderNzbPatch =
                    serde_json::from_value(patch).context("parsing downloader.nzb patch")?;
                Ok(DriverPatch::DownloaderNzb(patch))
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
pub enum MediaManagerMoviesPatch {
    SetIndexerRegistry { indexers: Vec<IndexerSpec> },
    SetDownloaders { downloaders: Vec<DownloaderSpec> },
    SetRootFolders { roots: Vec<RootFolderSpec> },
    SetTags { tags: Vec<String> },
}

impl MediaManagerMoviesPatch {
    pub fn validate(&self) -> Result<()> {
        match self {
            MediaManagerMoviesPatch::SetIndexerRegistry { indexers } => {
                ensure_non_empty_list(indexers, "indexers")?;
                for indexer in indexers {
                    indexer.validate()?;
                }
                Ok(())
            }
            MediaManagerMoviesPatch::SetDownloaders { downloaders } => {
                ensure_non_empty_list(downloaders, "downloaders")?;
                for downloader in downloaders {
                    downloader.validate()?;
                }
                Ok(())
            }
            MediaManagerMoviesPatch::SetRootFolders { roots } => {
                ensure_non_empty_list(roots, "roots")?;
                for root in roots {
                    root.validate()?;
                }
                Ok(())
            }
            MediaManagerMoviesPatch::SetTags { tags } => {
                ensure_non_empty_list(tags, "tags")?;
                validate_tags(tags, "tags")?;
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum IndexerRegistryPatch {
    RegisterIndexers { indexers: Vec<IndexerSpec> },
    RegisterApp { app: AppSpec },
    RegisterApps { apps: Vec<AppSpec> },
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
            IndexerRegistryPatch::RegisterApp { app } => app.validate(),
            IndexerRegistryPatch::RegisterApps { apps } => {
                ensure_non_empty_list(apps, "apps")?;
                for app in apps {
                    app.validate()?;
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
        #[serde(default)]
        max_connections: Option<u64>,
        #[serde(default)]
        max_connections_per_torrent: Option<u64>,
        #[serde(default)]
        max_upload_slots: Option<u64>,
        #[serde(default)]
        max_upload_slots_per_torrent: Option<u64>,
        #[serde(default)]
        disk_cache_mb: Option<u64>,
        #[serde(default)]
        disk_cache_ttl_seconds: Option<u64>,
        #[serde(default)]
        queueing_enabled: Option<bool>,
        #[serde(default)]
        max_active_downloads: Option<u64>,
        #[serde(default)]
        max_active_torrents: Option<u64>,
        #[serde(default)]
        max_active_uploads: Option<u64>,
        #[serde(default)]
        random_port: Option<bool>,
        #[serde(default)]
        listen_port: Option<u16>,
        #[serde(default)]
        upnp: Option<bool>,
        #[serde(default)]
        preallocate_all: Option<bool>,
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
                max_connections,
                max_connections_per_torrent,
                max_upload_slots,
                max_upload_slots_per_torrent,
                disk_cache_mb,
                disk_cache_ttl_seconds,
                queueing_enabled,
                max_active_downloads,
                max_active_torrents,
                max_active_uploads,
                random_port,
                listen_port,
                upnp,
                preallocate_all,
            } => {
                if default_save_path.is_none()
                    && incomplete_path.is_none()
                    && use_incomplete.is_none()
                    && max_connections.is_none()
                    && max_connections_per_torrent.is_none()
                    && max_upload_slots.is_none()
                    && max_upload_slots_per_torrent.is_none()
                    && disk_cache_mb.is_none()
                    && disk_cache_ttl_seconds.is_none()
                    && queueing_enabled.is_none()
                    && max_active_downloads.is_none()
                    && max_active_torrents.is_none()
                    && max_active_uploads.is_none()
                    && random_port.is_none()
                    && listen_port.is_none()
                    && upnp.is_none()
                    && preallocate_all.is_none()
                {
                    bail!("preferences must include at least one value");
                }
                ensure_optional_non_empty(default_save_path.as_deref(), "default_save_path")?;
                ensure_optional_non_empty(incomplete_path.as_deref(), "incomplete_path")?;
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum DownloaderNzbPatch {
    SetCategories {
        categories: Vec<DownloadCategorySpec>,
    },
    SetPreferences {
        #[serde(default)]
        main_dir: Option<String>,
        #[serde(default)]
        default_save_path: Option<String>,
        #[serde(default)]
        incomplete_path: Option<String>,
        #[serde(default)]
        nzb_dir: Option<String>,
        #[serde(default)]
        queue_dir: Option<String>,
        #[serde(default)]
        temp_dir: Option<String>,
        #[serde(default)]
        script_dir: Option<String>,
        #[serde(default)]
        log_file: Option<String>,
        #[serde(default)]
        web_dir: Option<String>,
        #[serde(default)]
        config_template: Option<String>,
        #[serde(default)]
        use_incomplete: Option<bool>,
        #[serde(default)]
        server_connections: Option<u64>,
        #[serde(default)]
        article_retries: Option<u64>,
        #[serde(default)]
        article_timeout_seconds: Option<u64>,
        #[serde(default)]
        article_cache_mb: Option<u64>,
        #[serde(default)]
        direct_write: Option<bool>,
        #[serde(default)]
        write_buffer_kb: Option<u64>,
        #[serde(default)]
        continue_partial: Option<bool>,
        #[serde(default)]
        par_check: Option<String>,
        #[serde(default)]
        par_scan: Option<String>,
        #[serde(default)]
        par_quick: Option<bool>,
        #[serde(default)]
        par_repair: Option<bool>,
        #[serde(default)]
        par_rename: Option<bool>,
        #[serde(default)]
        par_pause_queue: Option<bool>,
        #[serde(default)]
        par_threads: Option<u64>,
        #[serde(default)]
        unpack: Option<bool>,
        #[serde(default)]
        unpack_pause_queue: Option<bool>,
        #[serde(default)]
        download_rate_kib: Option<u64>,
    },
}

impl DownloaderNzbPatch {
    pub fn validate(&self) -> Result<()> {
        match self {
            DownloaderNzbPatch::SetCategories { categories } => {
                ensure_non_empty_list(categories, "categories")?;
                for category in categories {
                    category.validate()?;
                }
                Ok(())
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
                if main_dir.is_none()
                    && default_save_path.is_none()
                    && incomplete_path.is_none()
                    && nzb_dir.is_none()
                    && queue_dir.is_none()
                    && temp_dir.is_none()
                    && script_dir.is_none()
                    && log_file.is_none()
                    && web_dir.is_none()
                    && config_template.is_none()
                    && use_incomplete.is_none()
                    && server_connections.is_none()
                    && article_retries.is_none()
                    && article_timeout_seconds.is_none()
                    && article_cache_mb.is_none()
                    && direct_write.is_none()
                    && write_buffer_kb.is_none()
                    && continue_partial.is_none()
                    && par_check.is_none()
                    && par_scan.is_none()
                    && par_quick.is_none()
                    && par_repair.is_none()
                    && par_rename.is_none()
                    && par_pause_queue.is_none()
                    && par_threads.is_none()
                    && unpack.is_none()
                    && unpack_pause_queue.is_none()
                    && download_rate_kib.is_none()
                {
                    bail!("preferences must include at least one value");
                }
                ensure_optional_non_empty(main_dir.as_deref(), "main_dir")?;
                ensure_optional_non_empty(default_save_path.as_deref(), "default_save_path")?;
                ensure_optional_non_empty(incomplete_path.as_deref(), "incomplete_path")?;
                ensure_optional_non_empty(nzb_dir.as_deref(), "nzb_dir")?;
                ensure_optional_non_empty(queue_dir.as_deref(), "queue_dir")?;
                ensure_optional_non_empty(temp_dir.as_deref(), "temp_dir")?;
                ensure_optional_non_empty(script_dir.as_deref(), "script_dir")?;
                ensure_optional_non_empty(log_file.as_deref(), "log_file")?;
                ensure_optional_non_empty(web_dir.as_deref(), "web_dir")?;
                ensure_optional_non_empty(config_template.as_deref(), "config_template")?;
                ensure_optional_non_empty(par_check.as_deref(), "par_check")?;
                ensure_optional_non_empty(par_scan.as_deref(), "par_scan")?;
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
    pub auth: IndexerAuthSpec,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppSpec {
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

impl AppSpec {
    fn validate(&self) -> Result<()> {
        ensure_non_empty(&self.name, "app.name")?;
        ensure_non_empty(&self.implementation, "app.implementation")?;
        validate_url(&self.url)?;
        ensure_optional_non_empty(self.api_key.as_deref(), "app.api_key")?;
        validate_tags(&self.categories, "app.categories")?;
        validate_tags(&self.tags, "app.tags")?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IndexerAuthSpec {
    pub requires_account: Option<bool>,
    #[serde(default)]
    pub required_fields: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexerCredentialField {
    Username,
    Password,
    ApiKey,
}

impl IndexerSpec {
    fn validate(&self) -> Result<()> {
        ensure_non_empty(&self.name, "indexer.name")?;
        ensure_non_empty(&self.implementation, "indexer.implementation")?;
        validate_url(&self.url)?;
        ensure_optional_non_empty(self.api_key.as_deref(), "indexer.api_key")?;
        self.validate_auth()?;
        validate_tags(&self.categories, "indexer.categories")?;
        validate_tags(&self.tags, "indexer.tags")?;
        Ok(())
    }

    pub fn credential_fields(&self) -> Result<Vec<IndexerCredentialField>> {
        let requires_account = self
            .auth
            .requires_account
            .ok_or_else(|| anyhow!("indexer.auth.requires_account is required"))?;
        if !requires_account {
            return Ok(Vec::new());
        }
        let mut fields = if self.auth.required_fields.is_empty() {
            vec!["username", "password"]
        } else {
            self.auth
                .required_fields
                .iter()
                .map(String::as_str)
                .collect()
        };
        let mut out = Vec::new();
        for field in &mut fields {
            let parsed = IndexerCredentialField::from_str(field)?;
            if !out.contains(&parsed) {
                out.push(parsed);
            }
        }
        Ok(out)
    }

    pub fn credential_secret_key(&self, field: IndexerCredentialField) -> String {
        let slug = slugify(&self.name);
        format!("indexer.{slug}.{}", field.as_key())
    }

    fn validate_auth(&self) -> Result<()> {
        let requires_account = self
            .auth
            .requires_account
            .ok_or_else(|| anyhow!("indexer.auth.requires_account is required"))?;
        if !requires_account && !self.auth.required_fields.is_empty() {
            bail!("indexer.auth.required_fields is only allowed when requires_account is true");
        }
        if requires_account {
            let fields = if self.auth.required_fields.is_empty() {
                vec!["username", "password"]
            } else {
                self.auth
                    .required_fields
                    .iter()
                    .map(String::as_str)
                    .collect()
            };
            for field in fields {
                let _ = IndexerCredentialField::from_str(field)?;
            }
        }
        Ok(())
    }
}

impl IndexerCredentialField {
    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "username" => Ok(IndexerCredentialField::Username),
            "password" => Ok(IndexerCredentialField::Password),
            "api_key" | "apikey" => Ok(IndexerCredentialField::ApiKey),
            _ => bail!("unsupported indexer auth field '{}'", value),
        }
    }

    fn as_key(&self) -> &'static str {
        match self {
            IndexerCredentialField::Username => "username",
            IndexerCredentialField::Password => "password",
            IndexerCredentialField::ApiKey => "api_key",
        }
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
            bail!("series_type must be one of 'standard', 'anime', or 'daily'");
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

fn slugify(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('-');
        }
    }
    while out.contains("--") {
        out = out.replace("--", "-");
    }
    out.trim_matches('-').to_string()
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
                auth: IndexerAuthSpec {
                    requires_account: Some(false),
                    required_fields: Vec::new(),
                },
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
        let patch = IndexerRegistryPatch::RegisterIndexers {
            indexers: Vec::new(),
        };
        assert!(patch.validate().is_err());
    }

    #[test]
    fn indexer_registry_patch_requires_apps() {
        let patch = IndexerRegistryPatch::RegisterApps { apps: Vec::new() };
        assert!(patch.validate().is_err());
    }

    #[test]
    fn indexer_registry_patch_register_app_requires_url() {
        let patch = IndexerRegistryPatch::RegisterApp {
            app: AppSpec {
                name: "sonarr".to_string(),
                implementation: "sonarr".to_string(),
                url: "".to_string(),
                api_key: None,
                categories: Vec::new(),
                tags: Vec::new(),
                enabled: None,
                settings: HashMap::new(),
            },
        };
        assert!(patch.validate().is_err());
    }

    #[test]
    fn media_manager_movies_patch_requires_valid_indexer_registry() {
        let patch = MediaManagerMoviesPatch::SetIndexerRegistry {
            indexers: vec![IndexerSpec {
                name: "bad".to_string(),
                implementation: "torznab".to_string(),
                url: "http://localhost:9696".to_string(),
                auth: IndexerAuthSpec {
                    requires_account: Some(false),
                    required_fields: Vec::new(),
                },
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
    fn indexer_requires_auth_tag() {
        let patch = MediaManagerTvPatch::SetIndexerRegistry {
            indexers: vec![IndexerSpec {
                name: "test".to_string(),
                implementation: "torznab".to_string(),
                url: "https://example.invalid".to_string(),
                auth: IndexerAuthSpec {
                    requires_account: None,
                    required_fields: Vec::new(),
                },
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
    fn downloader_torrent_patch_requires_categories() {
        let patch = DownloaderTorrentPatch::SetCategories {
            categories: Vec::new(),
        };
        assert!(patch.validate().is_err());
    }

    #[test]
    fn downloader_torrent_patch_requires_preferences() {
        let patch = DownloaderTorrentPatch::SetPreferences {
            default_save_path: None,
            incomplete_path: None,
            use_incomplete: None,
            max_connections: None,
            max_connections_per_torrent: None,
            max_upload_slots: None,
            max_upload_slots_per_torrent: None,
            disk_cache_mb: None,
            disk_cache_ttl_seconds: None,
            queueing_enabled: None,
            max_active_downloads: None,
            max_active_torrents: None,
            max_active_uploads: None,
            random_port: None,
            listen_port: None,
            upnp: None,
            preallocate_all: None,
        };
        assert!(patch.validate().is_err());
    }

    #[test]
    fn downloader_nzb_patch_requires_categories() {
        let patch = DownloaderNzbPatch::SetCategories {
            categories: Vec::new(),
        };
        assert!(patch.validate().is_err());
    }

    #[test]
    fn downloader_nzb_patch_requires_preferences() {
        let patch = DownloaderNzbPatch::SetPreferences {
            main_dir: None,
            default_save_path: None,
            incomplete_path: None,
            nzb_dir: None,
            queue_dir: None,
            temp_dir: None,
            script_dir: None,
            log_file: None,
            web_dir: None,
            config_template: None,
            use_incomplete: None,
            server_connections: None,
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
        };
        assert!(patch.validate().is_err());
    }

    #[test]
    fn downloader_torrent_patch_accepts_performance_profile() {
        let patch = DownloaderTorrentPatch::SetPreferences {
            default_save_path: None,
            incomplete_path: None,
            use_incomplete: None,
            max_connections: Some(500),
            max_connections_per_torrent: Some(100),
            max_upload_slots: Some(20),
            max_upload_slots_per_torrent: Some(8),
            disk_cache_mb: Some(512),
            disk_cache_ttl_seconds: Some(60),
            queueing_enabled: Some(false),
            max_active_downloads: Some(50),
            max_active_torrents: Some(100),
            max_active_uploads: Some(20),
            random_port: Some(false),
            listen_port: Some(51413),
            upnp: Some(false),
            preallocate_all: Some(false),
        };
        assert!(patch.validate().is_ok());
    }

    #[test]
    fn downloader_nzb_patch_accepts_performance_profile() {
        let patch = DownloaderNzbPatch::SetPreferences {
            main_dir: Some("/downloads".to_string()),
            default_save_path: None,
            incomplete_path: None,
            nzb_dir: Some("/downloads/.nzb".to_string()),
            queue_dir: Some("/downloads/.queue".to_string()),
            temp_dir: Some("/downloads/.tmp".to_string()),
            script_dir: Some("/config/scripts".to_string()),
            log_file: Some("/config/nzbget.log".to_string()),
            web_dir: Some("/app/nzbget/webui".to_string()),
            config_template: Some("/app/nzbget/webui/nzbget.conf.template".to_string()),
            use_incomplete: None,
            server_connections: Some(20),
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
            par_threads: Some(4),
            unpack: Some(true),
            unpack_pause_queue: Some(true),
            download_rate_kib: Some(0),
        };
        assert!(patch.validate().is_ok());
    }
}
