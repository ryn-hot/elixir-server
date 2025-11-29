mod provider_types;
mod providers;

use anyhow::Result;
use reqwest::Client;

use crate::{config::MetadataConfig, db::models::MediaType, extensions::MediaIdentity};

pub use provider_types::MetadataResult;

#[derive(Clone)]
pub struct MetadataService {
    client: Client,
    pub config: MetadataConfig,
}

impl MetadataService {
    pub fn new(config: MetadataConfig) -> Result<Self> {
        let client = Client::builder()
            .user_agent("ElixirMediaServer/0.1 (+https://example.com)")
            .timeout(std::time::Duration::from_secs(
                config.request_timeout_seconds,
            ))
            .build()?;

        Ok(Self { client, config })
    }

    pub fn ttl_seconds(&self) -> u64 {
        self.config.ttl_seconds
    }

    pub async fn fetch_metadata(&self, identity: &MediaIdentity) -> Result<Option<MetadataResult>> {
        match identity.r#type {
            MediaType::Anime => {
                if self.config.enable_anilist {
                    if let Some(meta) = providers::anilist::fetch(&self.client, identity).await? {
                        return Ok(Some(meta));
                    }
                }
                if self.config.enable_aniapi {
                    if let Some(meta) = providers::aniapi::fetch(&self.client, identity).await? {
                        return Ok(Some(meta));
                    }
                }
                if self.config.enable_consumet {
                    if let Some(meta) = providers::consumet::fetch(&self.client, identity).await? {
                        return Ok(Some(meta));
                    }
                }
            }
            _ => {
                if self.config.enable_cinemeta {
                    if let Some(meta) = providers::cinemeta::fetch(&self.client, identity).await? {
                        return Ok(Some(meta));
                    }
                }
                if self.config.enable_wikidata {
                    if let Some(meta) = providers::wikidata::fetch(&self.client, identity).await? {
                        return Ok(Some(meta));
                    }
                }
            }
        }

        Ok(None)
    }
}
