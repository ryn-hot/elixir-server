mod provider_types;
mod providers;

use anyhow::Result;
use reqwest::Client;

use crate::{
    config::MetadataConfig,
    db::models::MediaType,
    extensions::{ExternalIds, MediaIdentity},
};

pub use provider_types::DiscoveryResult;
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

    pub async fn discovery_search(
        &self,
        query: &str,
        r#type: Option<MediaType>,
    ) -> Result<Vec<DiscoveryResult>> {
        let media_kind = match r#type.unwrap_or(MediaType::Movie) {
            MediaType::Movie => "movie",
            _ => "series",
        };

        // Try Cinemeta first.
        if self.config.enable_cinemeta {
            if let Ok(results) = providers::cinemeta::search(&self.client, query, media_kind).await
            {
                if !results.is_empty() {
                    let mapped = results
                        .into_iter()
                        .map(|meta| {
                            let title = meta
                                .rest
                                .get("name")
                                .or_else(|| meta.rest.get("title"))
                                .and_then(serde_json::Value::as_str)
                                .unwrap_or_else(|| query)
                                .to_string();
                            let year = meta
                                .rest
                                .get("year")
                                .and_then(serde_json::Value::as_i64)
                                .map(|v| v as i32);
                            DiscoveryResult {
                                title,
                                r#type: r#type.unwrap_or(MediaType::Movie),
                                year,
                                external_ids: Some(ExternalIds {
                                    imdb: meta.imdb_id.clone(),
                                    ..Default::default()
                                }),
                                description: meta.description.clone(),
                            }
                        })
                        .collect();
                    return Ok(mapped);
                }
            }
        }

        // Fallback to Wikidata simple search.
        if self.config.enable_wikidata {
            if let Ok(Some(meta)) = providers::wikidata::fetch(
                &self.client,
                &MediaIdentity {
                    r#type: r#type.unwrap_or(MediaType::Movie),
                    external_ids: ExternalIds::default(),
                    title: query.to_string(),
                    year: None,
                    season: None,
                    episode: None,
                },
            )
            .await
            {
                return Ok(vec![DiscoveryResult {
                    title: meta
                        .metadata_json
                        .get("label")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or(query)
                        .to_string(),
                    r#type: r#type.unwrap_or(MediaType::Movie),
                    year: None,
                    external_ids: None,
                    description: meta.description,
                }]);
            }
        }

        Ok(Vec::new())
    }
}
