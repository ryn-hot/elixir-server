use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{config::ClassifierConfig, extensions::ExternalIds};

const TVDB_TOKEN_TTL_SECONDS: u64 = 60 * 60 * 12;

pub struct LinkerService {
    client: Client,
    config: ClassifierConfig,
    tvdb_token: Mutex<Option<TvdbToken>>,
}

#[derive(Clone)]
struct TvdbToken {
    token: String,
    expires_at: Instant,
}

impl LinkerService {
    pub fn new(config: ClassifierConfig) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(config.request_timeout_seconds))
            .build()
            .context("building classifier http client")?;

        Ok(Self {
            client,
            config,
            tvdb_token: Mutex::new(None),
        })
    }

    pub async fn link_tvdb_series_by_imdb(&self, imdb_id: &str) -> Result<Option<String>> {
        if imdb_id.trim().is_empty() {
            return Ok(None);
        }
        let resp: Option<TvdbRemoteIdResponse> = self
            .tvdb_get_json(&format!("/search/remoteid/{}", imdb_id), &[])
            .await?;
        let Some(resp) = resp else {
            return Ok(None);
        };
        let Some(results) = resp.data else {
            return Ok(None);
        };
        for result in results {
            if let Some(series) = result.series {
                return Ok(Some(series.id.to_string()));
            }
        }
        Ok(None)
    }

    pub async fn fetch_tvdb_season_episodes(
        &self,
        tvdb_series_id: &str,
        season_number: i32,
    ) -> Result<Vec<TvdbEpisodeRecord>> {
        if tvdb_series_id.trim().is_empty() {
            return Ok(Vec::new());
        }
        let resp: Option<TvdbSeriesEpisodesResponse> = self
            .tvdb_get_json(
                &format!("/series/{}/episodes/default", tvdb_series_id),
                &[
                    ("page", "0".to_string()),
                    ("season", season_number.to_string()),
                ],
            )
            .await?;
        let Some(resp) = resp else {
            return Ok(Vec::new());
        };
        let Some(data) = resp.data else {
            return Ok(Vec::new());
        };
        let episodes = data
            .episodes
            .unwrap_or_default()
            .into_iter()
            .map(|episode| {
                let raw = serde_json::to_value(&episode).unwrap_or_default();
                TvdbEpisodeRecord {
                    tvdb_episode_id: Some(episode.id.to_string()),
                    season_number: episode.season_number,
                    episode_number: episode.number,
                    absolute_number: episode.absolute_number,
                    title: episode.name,
                    overview: episode.overview,
                    runtime_minutes: episode.runtime,
                    image: episode.image,
                    raw,
                }
            })
            .collect();
        Ok(episodes)
    }

    pub async fn fetch_anizip_mapping(&self, anilist_id: &str) -> Result<Option<AniZipMapping>> {
        if anilist_id.trim().is_empty() {
            return Ok(None);
        }
        let base = self.config.anizip_base_url.trim_end_matches('/');
        if base.is_empty() {
            return Ok(None);
        }
        let url = format!("{}/mappings?anilist_id={}", base, anilist_id);
        let resp = self.client.get(&url).send().await?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            anyhow::bail!("ani.zip returned {}", resp.status());
        }
        let payload: AniZipResponse = resp.json().await?;

        let ids = ExternalIds {
            imdb: payload.mappings.imdb_id.clone(),
            tmdb: payload
                .mappings
                .themoviedb_id
                .map(|id| id.to_string()),
            tvdb_series: payload.mappings.thetvdb_id.map(|id| id.to_string()),
            anilist: payload.mappings.anilist_id.map(|id| id.to_string()),
            anidb: payload.mappings.anidb_id.map(|id| id.to_string()),
            mal: payload.mappings.mal_id.map(|id| id.to_string()),
            kitsu: payload.mappings.kitsu_id.map(|id| id.to_string()),
            ..Default::default()
        };

        let mut episodes = Vec::new();
        for (_key, raw) in payload.episodes.unwrap_or_default() {
            let parsed: AniZipEpisode = serde_json::from_value(raw.clone()).unwrap_or_default();
            let title = select_title(parsed.title.as_ref());
            episodes.push(AniZipEpisodeRecord {
                season_number: parsed.season_number,
                episode_number: parsed.episode_number,
                absolute_episode_number: parsed.absolute_episode_number,
                title,
                overview: parsed.overview.clone(),
                runtime_minutes: parsed.runtime.or(parsed.length),
                image: parsed.image.clone(),
                tvdb_id: parsed.tvdb_id.map(|id| id.to_string()),
                anidb_eid: parsed.anidb_eid.map(|id| id.to_string()),
                raw,
            });
        }

        Ok(Some(AniZipMapping {
            ids,
            episodes,
            images: payload.images.unwrap_or_default(),
            titles: payload.titles.unwrap_or_default(),
        }))
    }

    async fn tvdb_get_json<T: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<Option<T>> {
        let Some(token) = self.tvdb_token().await? else {
            return Ok(None);
        };
        let base = self.config.tvdb_base_url.trim_end_matches('/');
        if base.is_empty() {
            return Ok(None);
        }
        let url = format!("{}{}", base, path);

        let mut resp = self
            .client
            .get(&url)
            .bearer_auth(&token)
            .query(query)
            .send()
            .await?;

        if resp.status() == StatusCode::UNAUTHORIZED {
            self.clear_tvdb_token().await;
            if let Some(token) = self.tvdb_token().await? {
                resp = self
                    .client
                    .get(&url)
                    .bearer_auth(&token)
                    .query(query)
                    .send()
                    .await?;
            }
        }

        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            anyhow::bail!("tvdb returned {}", resp.status());
        }
        let parsed = resp.json::<T>().await?;
        Ok(Some(parsed))
    }

    async fn tvdb_token(&self) -> Result<Option<String>> {
        let api_key = match self.config.tvdb_api_key.as_deref() {
            Some(key) if !key.trim().is_empty() => key.trim(),
            _ => return Ok(None),
        };

        {
            let guard = self.tvdb_token.lock().await;
            if let Some(token) = guard.as_ref() {
                if Instant::now() < token.expires_at {
                    return Ok(Some(token.token.clone()));
                }
            }
        }

        let base = self.config.tvdb_base_url.trim_end_matches('/');
        if base.is_empty() {
            return Ok(None);
        }
        let url = format!("{}/login", base);
        let resp = self
            .client
            .post(&url)
            .json(&serde_json::json!({ "apikey": api_key }))
            .send()
            .await?;
        if !resp.status().is_success() {
            anyhow::bail!("tvdb login failed {}", resp.status());
        }
        let body: TvdbLoginResponse = resp.json().await?;
        let token = body
            .data
            .and_then(|d| d.token)
            .ok_or_else(|| anyhow::anyhow!("tvdb login response missing token"))?;
        let token = TvdbToken {
            token: token.clone(),
            expires_at: Instant::now() + Duration::from_secs(TVDB_TOKEN_TTL_SECONDS),
        };
        let token_value = token.token.clone();
        let mut guard = self.tvdb_token.lock().await;
        *guard = Some(token);
        Ok(Some(token_value))
    }

    async fn clear_tvdb_token(&self) {
        let mut guard = self.tvdb_token.lock().await;
        *guard = None;
    }
}

#[derive(Debug, Clone)]
pub struct TvdbEpisodeRecord {
    pub tvdb_episode_id: Option<String>,
    pub season_number: Option<i32>,
    pub episode_number: Option<i32>,
    pub absolute_number: Option<i32>,
    pub title: Option<String>,
    pub overview: Option<String>,
    pub runtime_minutes: Option<i32>,
    pub image: Option<String>,
    pub raw: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct AniZipMapping {
    pub ids: ExternalIds,
    pub episodes: Vec<AniZipEpisodeRecord>,
    pub images: Vec<AniZipImage>,
    pub titles: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct AniZipEpisodeRecord {
    pub season_number: Option<i32>,
    pub episode_number: Option<i32>,
    pub absolute_episode_number: Option<i32>,
    pub title: Option<String>,
    pub overview: Option<String>,
    pub runtime_minutes: Option<i32>,
    pub image: Option<String>,
    pub tvdb_id: Option<String>,
    pub anidb_eid: Option<String>,
    pub raw: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AniZipImage {
    #[serde(rename = "coverType")]
    pub cover_type: Option<String>,
    pub url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TvdbLoginResponse {
    data: Option<TvdbLoginData>,
}

#[derive(Debug, Deserialize)]
struct TvdbLoginData {
    token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TvdbRemoteIdResponse {
    data: Option<Vec<TvdbRemoteIdResult>>,
}

#[derive(Debug, Deserialize)]
struct TvdbRemoteIdResult {
    series: Option<TvdbSeriesBaseRecord>,
}

#[derive(Debug, Deserialize)]
struct TvdbSeriesBaseRecord {
    id: i64,
}

#[derive(Debug, Deserialize)]
struct TvdbSeriesEpisodesResponse {
    data: Option<TvdbSeriesEpisodesData>,
}

#[derive(Debug, Deserialize)]
struct TvdbSeriesEpisodesData {
    episodes: Option<Vec<TvdbEpisodeBaseRecord>>,
}

#[derive(Debug, Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct TvdbEpisodeBaseRecord {
    id: i64,
    season_number: Option<i32>,
    number: Option<i32>,
    absolute_number: Option<i32>,
    name: Option<String>,
    overview: Option<String>,
    runtime: Option<i32>,
    image: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AniZipEpisode {
    season_number: Option<i32>,
    episode_number: Option<i32>,
    absolute_episode_number: Option<i32>,
    title: Option<HashMap<String, String>>,
    runtime: Option<i32>,
    length: Option<i32>,
    overview: Option<String>,
    image: Option<String>,
    tvdb_id: Option<i64>,
    anidb_eid: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct AniZipResponse {
    mappings: AniZipMappings,
    episodes: Option<HashMap<String, serde_json::Value>>,
    images: Option<Vec<AniZipImage>>,
    titles: Option<HashMap<String, String>>,
}

#[derive(Debug, Deserialize)]
struct AniZipMappings {
    #[serde(rename = "animeplanet_id")]
    animeplanet_id: Option<i64>,
    #[serde(rename = "kitsu_id")]
    kitsu_id: Option<i64>,
    #[serde(rename = "mal_id")]
    mal_id: Option<i64>,
    #[serde(rename = "anilist_id")]
    anilist_id: Option<i64>,
    #[serde(rename = "anisearch_id")]
    anisearch_id: Option<i64>,
    #[serde(rename = "anidb_id")]
    anidb_id: Option<i64>,
    #[serde(rename = "thetvdb_id")]
    thetvdb_id: Option<i64>,
    #[serde(rename = "themoviedb_id")]
    themoviedb_id: Option<i64>,
    #[serde(rename = "imdb_id")]
    imdb_id: Option<String>,
}

fn select_title(title_map: Option<&HashMap<String, String>>) -> Option<String> {
    let map = title_map?;
    if let Some(en) = map.get("en") {
        return Some(en.clone());
    }
    if let Some(romaji) = map.get("x-jat") {
        return Some(romaji.clone());
    }
    map.values().next().cloned()
}
