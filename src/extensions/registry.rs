use std::collections::HashSet;
use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};

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
    pub sha256: Option<String>,
    #[serde(default)]
    pub signature: Option<String>,
    #[serde(default)]
    pub publisher_key_id: Option<String>,
    #[serde(default)]
    pub trust: Option<ExtensionTrustLevel>,
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

pub async fn fetch_registries(
    urls: &[String],
    timeout: Duration,
) -> Result<Vec<RegistryIndex>> {
    let client = RegistryClient::new(timeout)?;
    let mut results = Vec::new();
    for url in urls {
        results.push(client.fetch(url).await?);
    }
    Ok(results)
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
