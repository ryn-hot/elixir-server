use anyhow::Result;
use reqwest::Client;
use serde::Serialize;

use crate::{
    db::models::MediaType,
    extensions::{ExternalIds, MediaIdentity},
    metadata::provider_types::MetadataResult,
};

pub mod cinemeta {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    struct CineMetaResponse {
        meta: Option<CineMetaItem>,
    }

    #[derive(Debug, Deserialize, Serialize)]
    pub struct CineMetaItem {
        #[serde(rename = "imdb_id")]
        pub imdb_id: Option<String>,
        pub runtime: Option<i32>, // minutes
        pub description: Option<String>,
        pub genres: Option<Vec<String>>,
        #[serde(flatten)]
        pub rest: serde_json::Value,
    }

    pub async fn fetch(
        client: &Client,
        identity: &MediaIdentity,
    ) -> Result<Option<MetadataResult>> {
        let media_kind = match identity.r#type {
            MediaType::Movie => "movie",
            MediaType::Series | MediaType::Anime => "series",
        };

        // Try by imdb id if present.
        if let Some(imdb) = identity.external_ids.imdb.as_ref() {
            let url = format!(
                "https://v3-cinemeta.strem.io/meta/{}/{}.json",
                media_kind, imdb
            );
            if let Ok(res) = client.get(&url).send().await {
                if res.status().is_success() {
                    if let Ok(body) = res.json::<CineMetaResponse>().await {
                        if let Some(meta) = body.meta {
                            let runtime_seconds = meta.runtime.map(|m| m * 60);
                            let mut external_ids = ExternalIds::default();
                            external_ids.imdb = meta.imdb_id.clone();
                            let json = serde_json::to_value(&meta)?;
                            return Ok(Some(MetadataResult {
                                metadata_json: json,
                                runtime_seconds,
                                external_ids: Some(external_ids),
                                description: meta.description.clone(),
                                genres: meta.genres.clone(),
                            }));
                        }
                    }
                }
            }
        }

        // Fallback to search by title.
        let query = urlencoding::encode(&identity.title);
        let url = format!(
            "https://v3-cinemeta.strem.io/meta/{}/search={}.json",
            media_kind, query
        );
        let res = client.get(&url).send().await?;
        if !res.status().is_success() {
            return Ok(None);
        }
        #[derive(Debug, Deserialize)]
        struct SearchResp {
            metas: Option<Vec<CineMetaItem>>,
        }
        let body: SearchResp = res.json().await?;
        if let Some(first) = body.metas.and_then(|mut m| m.pop()) {
            let runtime_seconds = first.runtime.map(|m| m * 60);
            let json = serde_json::to_value(&first)?;
            return Ok(Some(MetadataResult {
                metadata_json: json,
                runtime_seconds,
                external_ids: None,
                description: first.description.clone(),
                genres: first.genres.clone(),
            }));
        }
        Ok(None)
    }

    pub async fn search(
        client: &Client,
        query: &str,
        media_kind: &str,
    ) -> Result<Vec<CineMetaItem>> {
        #[derive(Debug, Deserialize)]
        struct SearchResp {
            metas: Option<Vec<CineMetaItem>>,
        }
        let encoded = urlencoding::encode(query);
        let url = format!(
            "https://v3-cinemeta.strem.io/meta/{}/search={}.json",
            media_kind, encoded
        );
        let res = client.get(&url).send().await?;
        if !res.status().is_success() {
            return Ok(Vec::new());
        }
        let body: SearchResp = res.json().await?;
        Ok(body.metas.unwrap_or_default())
    }
}

pub mod wikidata {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    struct SearchResult {
        search: Option<Vec<SearchItem>>,
    }

    #[derive(Debug, Deserialize, Serialize, Clone)]
    struct SearchItem {
        id: String,
        label: Option<String>,
        description: Option<String>,
    }

    pub async fn fetch(
        client: &Client,
        identity: &MediaIdentity,
    ) -> Result<Option<MetadataResult>> {
        let url = format!(
            "https://www.wikidata.org/w/api.php?action=wbsearchentities&language=en&format=json&search={}",
            urlencoding::encode(&identity.title)
        );
        let res = client.get(&url).send().await?;
        if !res.status().is_success() {
            return Ok(None);
        }
        let body: SearchResult = res.json().await?;
        if let Some(first) = body.search.and_then(|mut s| s.pop()) {
            let json = serde_json::to_value(first.clone())?;
            return Ok(Some(MetadataResult {
                metadata_json: json,
                runtime_seconds: None,
                external_ids: None,
                description: first.description,
                genres: None,
            }));
        }
        Ok(None)
    }
}

pub mod anilist {
    use super::*;
    use serde::Deserialize;

    pub async fn fetch(
        client: &Client,
        identity: &MediaIdentity,
    ) -> Result<Option<MetadataResult>> {
        let query = r#"
            query ($search: String) {
              Media(search: $search, type: ANIME) {
                id
                title { romaji english native }
                episodes
                duration
                format
                description
                genres
              }
            }
        "#;

        #[derive(Serialize)]
        struct Variables<'a> {
            search: &'a str,
        }

        #[derive(Deserialize)]
        struct MediaResp {
            data: Option<MediaData>,
        }
        #[derive(Deserialize)]
        struct MediaData {
            Media: Option<MediaItem>,
        }
        #[derive(Deserialize, Serialize)]
        struct MediaItem {
            id: i32,
            title: Option<serde_json::Value>,
            episodes: Option<i32>,
            duration: Option<i32>,
            format: Option<String>,
            description: Option<String>,
            genres: Option<Vec<String>>,
        }

        let res = client
            .post("https://graphql.anilist.co")
            .json(&serde_json::json!({ "query": query, "variables": Variables { search: &identity.title } }))
            .send()
            .await?;

        if !res.status().is_success() {
            return Ok(None);
        }
        let body: MediaResp = res.json().await?;
        if let Some(media) = body.data.and_then(|d| d.Media) {
            let runtime_seconds = media
                .duration
                .and_then(|d| media.episodes.map(|e| d * 60 * e).or(Some(d * 60)));
            let json = serde_json::to_value(&media)?;
            return Ok(Some(MetadataResult {
                metadata_json: json,
                runtime_seconds,
                external_ids: Some(ExternalIds {
                    anilist: Some(media.id.to_string()),
                    ..Default::default()
                }),
                description: media.description.clone(),
                genres: media.genres.clone(),
            }));
        }
        Ok(None)
    }
}

pub mod aniapi {
    use super::*;
    use serde::Deserialize;

    #[derive(Deserialize, Serialize)]
    struct AniApiAnime {
        id: i64,
        titles: serde_json::Value,
        episodes_count: Option<i32>,
        episode_duration: Option<i32>,
        description: Option<String>,
        genres: Option<Vec<String>>,
    }

    #[derive(Deserialize)]
    struct AniApiResponse {
        data: Option<Vec<AniApiAnime>>,
    }

    pub async fn fetch(
        client: &Client,
        identity: &MediaIdentity,
    ) -> Result<Option<MetadataResult>> {
        let url = format!(
            "https://api.aniapi.com/v1/anime?title={}",
            urlencoding::encode(&identity.title)
        );
        let res = client.get(&url).send().await?;
        if !res.status().is_success() {
            return Ok(None);
        }
        let body: AniApiResponse = res.json().await?;
        if let Some(first) = body.data.and_then(|mut d| d.pop()) {
            let runtime_seconds = first.episode_duration.and_then(|dur| {
                first
                    .episodes_count
                    .map(|e| dur * 60 * e)
                    .or(Some(dur * 60))
            });
            let json = serde_json::to_value(&first)?;
            return Ok(Some(MetadataResult {
                metadata_json: json,
                runtime_seconds,
                external_ids: None,
                description: first.description.clone(),
                genres: first.genres.clone(),
            }));
        }
        Ok(None)
    }
}

pub mod consumet {
    use super::*;

    pub async fn fetch(
        _client: &Client,
        _identity: &MediaIdentity,
    ) -> Result<Option<MetadataResult>> {
        // Placeholder: Consumet has multiple providers; skip actual call to avoid instability.
        Ok(None)
    }
}
