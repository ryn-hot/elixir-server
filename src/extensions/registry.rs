use std::collections::HashSet;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::fs;
use tracing::warn;

use crate::db::models::ExtensionTrustLevel;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryIndex {
    pub registry_version: u32,
    #[serde(default)]
    pub extensions: Vec<RegistryEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryEntry {
    pub id: String,
    pub version: String,
    pub download_url: String,
    #[serde(default)]
    pub package_path: Option<String>,
    #[serde(default)]
    pub sha256: Option<String>,
    #[serde(default)]
    pub signature: Option<String>,
    #[serde(default)]
    pub publisher_key_id: Option<String>,
    #[serde(default)]
    pub trust: Option<ExtensionTrustLevel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryFetchError {
    pub url: String,
    pub error: String,
    #[serde(default)]
    pub occurred_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryCache {
    pub fetched_at: DateTime<Utc>,
    #[serde(default)]
    pub last_success_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_error: Option<RegistryFetchError>,
    #[serde(default)]
    pub registry_urls: Vec<String>,
    #[serde(default)]
    pub registry_errors: Vec<RegistryFetchError>,
    pub index: RegistryIndex,
}

pub struct RegistryCacheStore {
    cache_dir: PathBuf,
    cache_file: PathBuf,
}

impl RegistryCacheStore {
    pub fn new(cache_dir: PathBuf) -> Self {
        let cache_file = cache_dir.join("merged.json");
        Self {
            cache_dir,
            cache_file,
        }
    }

    pub async fn load(&self) -> Result<Option<RegistryCache>> {
        let contents = match fs::read_to_string(&self.cache_file).await {
            Ok(contents) => contents,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        if contents.trim().is_empty() {
            return Ok(None);
        }
        let cache =
            serde_json::from_str::<RegistryCache>(&contents).context("parsing registry cache")?;
        Ok(Some(cache))
    }

    pub async fn save(&self, cache: &RegistryCache) -> Result<()> {
        fs::create_dir_all(&self.cache_dir)
            .await
            .with_context(|| format!("creating {}", self.cache_dir.display()))?;
        let payload = serde_json::to_vec_pretty(cache).context("serializing registry cache")?;
        let tmp_path = self.cache_file.with_extension("tmp");
        fs::write(&tmp_path, &payload)
            .await
            .with_context(|| format!("writing {}", tmp_path.display()))?;
        fs::rename(&tmp_path, &self.cache_file)
            .await
            .with_context(|| format!("persisting {}", self.cache_file.display()))?;
        Ok(())
    }
}

pub struct RegistryClient {
    client: Client,
}

impl RegistryClient {
    pub fn new(timeout: Duration) -> Result<Self> {
        let client = Client::builder()
            .timeout(timeout)
            .build()
            .context("building registry client")?;
        Ok(Self { client })
    }

    pub async fn fetch(&self, url: &str) -> Result<RegistryIndex> {
        let resp = self
            .client
            .get(url)
            .send()
            .await
            .with_context(|| format!("fetching registry index from {url}"))?;
        if !resp.status().is_success() {
            anyhow::bail!("registry {url} returned {}", resp.status());
        }
        resp.json::<RegistryIndex>()
            .await
            .with_context(|| format!("parsing registry index from {url}"))
    }
}

pub async fn fetch_registries(urls: &[String], timeout: Duration) -> Result<Vec<RegistryIndex>> {
    let client = RegistryClient::new(timeout)?;
    let mut results = Vec::new();
    for url in urls {
        results.push(client.fetch(url).await?);
    }
    Ok(results)
}

pub async fn refresh_registry_cache(
    urls: &[String],
    timeout: Duration,
    cache_store: &RegistryCacheStore,
) -> Result<RegistryCache> {
    let previous = cache_store.load().await?;
    let previous_matches = previous
        .as_ref()
        .map(|cache| cache.registry_urls == urls)
        .unwrap_or(false);
    let mut last_success_at = if previous_matches {
        previous.as_ref().and_then(|cache| cache.last_success_at)
    } else {
        None
    };
    let mut last_error = if previous_matches {
        previous.as_ref().and_then(|cache| cache.last_error.clone())
    } else {
        None
    };

    if urls.is_empty() {
        let cache = RegistryCache {
            fetched_at: Utc::now(),
            last_success_at: None,
            last_error: None,
            registry_urls: Vec::new(),
            registry_errors: Vec::new(),
            index: RegistryIndex {
                registry_version: 1,
                extensions: Vec::new(),
            },
        };
        cache_store.save(&cache).await?;
        return Ok(cache);
    }

    let client = RegistryClient::new(timeout)?;
    let mut indexes = Vec::new();
    let mut errors = Vec::new();
    let now = Utc::now();
    for url in urls {
        match client.fetch(url).await {
            Ok(index) => indexes.push(index),
            Err(err) => errors.push(RegistryFetchError {
                url: url.clone(),
                error: err.to_string(),
                occurred_at: Some(now),
            }),
        }
    }
    let had_success = !indexes.is_empty();
    let merged = merge_indexes(indexes);
    if had_success {
        last_success_at = Some(now);
    }
    if !errors.is_empty() {
        last_error = errors.last().cloned();
    }

    let cache = RegistryCache {
        fetched_at: now,
        last_success_at,
        last_error,
        registry_urls: urls.to_vec(),
        registry_errors: errors,
        index: merged,
    };
    cache_store.save(&cache).await?;
    Ok(cache)
}

pub async fn start_registry_refresh_loop(
    registries: Vec<String>,
    storage_root: String,
    interval: Duration,
) {
    if interval.is_zero() || registries.is_empty() {
        return;
    }

    let cache_store = RegistryCacheStore::new(PathBuf::from(storage_root).join("registry-cache"));
    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;
        if let Err(err) =
            refresh_registry_cache(&registries, Duration::from_secs(10), &cache_store).await
        {
            warn!("registry refresh failed: {err}");
        }
    }
}

pub fn merge_indexes(indexes: Vec<RegistryIndex>) -> RegistryIndex {
    let mut seen = HashSet::new();
    let mut merged = Vec::new();

    for index in indexes {
        for entry in index.extensions {
            let key = format!("{}:{}", entry.id, entry.version);
            if seen.insert(key) {
                merged.push(entry);
            }
        }
    }

    RegistryIndex {
        registry_version: 1,
        extensions: merged,
    }
}
