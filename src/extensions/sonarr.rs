use std::{collections::HashMap, collections::HashSet, path::Path};

use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sqlx::{AnyPool, Row};

use crate::{
    db::models::MediaType,
    extensions::{ExternalIds, FileDescriptor, FileSource, MediaFileCandidate, MediaIdentity},
};

#[derive(Clone)]
pub struct SonarrSource {
    client: Client,
    base_url: String,
    api_key: String,
    source_config_id: Option<uuid::Uuid>,
}

#[derive(Debug, Deserialize)]
pub struct SonarrSourceConfig {
    pub base_url: String,
    pub api_key: String,
}

impl SonarrSource {
    pub fn new(
        base_url: String,
        api_key: String,
        source_config_id: Option<uuid::Uuid>,
    ) -> Result<Self> {
        let base = base_url.trim_end_matches('/').to_string();
        if base.is_empty() {
            anyhow::bail!("sonarr base_url is required");
        }
        if api_key.trim().is_empty() {
            anyhow::bail!("sonarr api_key is required");
        }

        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .context("building sonarr client")?;

        Ok(Self {
            client,
            base_url: base,
            api_key,
            source_config_id,
        })
    }

    fn url(&self, path: &str) -> String {
        format!("{}/api/v3{}", self.base_url, path)
    }

    async fn fetch_series(&self) -> Result<Vec<SonarrSeries>> {
        let resp = self
            .client
            .get(self.url("/series"))
            .header("X-Api-Key", &self.api_key)
            .send()
            .await
            .context("requesting sonarr series")?;
        if !resp.status().is_success() {
            anyhow::bail!("sonarr /series returned {}", resp.status());
        }
        resp.json::<Vec<SonarrSeries>>()
            .await
            .context("parsing sonarr series response")
    }

    async fn fetch_episodes(&self, series_id: i64) -> Result<Vec<SonarrEpisode>> {
        let resp = self
            .client
            .get(self.url("/episode"))
            .query(&[("seriesId", series_id)])
            .header("X-Api-Key", &self.api_key)
            .send()
            .await
            .with_context(|| format!("requesting episodes for series {series_id}"))?;
        if !resp.status().is_success() {
            anyhow::bail!(
                "sonarr /episode for series {} returned {}",
                series_id,
                resp.status()
            );
        }
        resp.json::<Vec<SonarrEpisode>>()
            .await
            .with_context(|| format!("parsing episodes for series {series_id}"))
    }

    async fn fetch_episode_files(&self, series_id: i64) -> Result<Vec<SonarrEpisodeFile>> {
        let resp = self
            .client
            .get(self.url("/episodefile"))
            .query(&[("seriesId", series_id)])
            .header("X-Api-Key", &self.api_key)
            .send()
            .await
            .with_context(|| format!("requesting episode files for series {series_id}"))?;
        if !resp.status().is_success() {
            anyhow::bail!(
                "sonarr /episodefile for series {} returned {}",
                series_id,
                resp.status()
            );
        }
        resp.json::<Vec<SonarrEpisodeFile>>()
            .await
            .with_context(|| format!("parsing episode files for series {series_id}"))
    }
}

pub async fn load_sonarr_sources(
    pool: &AnyPool,
    fallback: &crate::config::SonarrConfig,
) -> Result<Vec<SonarrSource>> {
    let mut sources = Vec::new();
    let mut seen = HashSet::new();

    let rows = sqlx::query("SELECT id, config_json FROM source_configs WHERE extension_id = 'elixir.sonarr' AND enabled = TRUE")
        .fetch_all(pool)
        .await
        .unwrap_or_default();

    for row in rows {
        let raw: Option<String> = row.try_get("config_json").ok();
        let config_id: Option<String> = row.try_get("id").ok();
        let config_uuid = config_id
            .as_deref()
            .and_then(|s| uuid::Uuid::parse_str(s).ok());
        if let Some(cfg_str) = raw {
            if let Ok(cfg) = serde_json::from_str::<SonarrSourceConfig>(&cfg_str) {
                if seen.insert(cfg.base_url.clone()) {
                    if let Ok(src) = SonarrSource::new(cfg.base_url, cfg.api_key, config_uuid) {
                        sources.push(src);
                    }
                }
            }
        }
    }

    if sources.is_empty() && fallback.enabled {
        if let (Some(base), Some(key)) = (&fallback.base_url, &fallback.api_key) {
            if seen.insert(base.clone()) {
                if let Ok(src) = SonarrSource::new(base.clone(), key.clone(), None) {
                    sources.push(src);
                }
            }
        }
    }

    Ok(sources)
}

#[async_trait::async_trait]
impl FileSource for SonarrSource {
    async fn scan(
        &self,
        _since: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Vec<MediaFileCandidate>> {
        let mut candidates = Vec::new();
        let series_list = self.fetch_series().await?;
        let mut seen_paths: HashSet<String> = HashSet::new();

        for series in series_list {
            let episodes = self.fetch_episodes(series.id).await?;
            let files = self.fetch_episode_files(series.id).await?;
            let file_map: HashMap<i64, SonarrEpisodeFile> =
                files.into_iter().map(|f| (f.id, f)).collect();

            for episode in episodes {
                if let Some(file_id) = episode.episode_file_id {
                    if let Some(file) = file_map.get(&file_id) {
                        // Multi-episode files point to the same path; ingest once.
                        if seen_paths.contains(&file.path) {
                            continue;
                        }
                        if let Some(mut candidate) = make_candidate(&series, &episode, file) {
                            candidate.source_config_id = self.source_config_id;
                            seen_paths.insert(file.path.clone());
                            candidates.push(candidate);
                        }
                    }
                }
            }
        }

        Ok(candidates)
    }
}

fn make_candidate(
    series: &SonarrSeries,
    episode: &SonarrEpisode,
    file: &SonarrEpisodeFile,
) -> Option<MediaFileCandidate> {
    if file.path.is_empty() {
        return None;
    }

    let identity = MediaIdentity {
        r#type: MediaType::Series,
        external_ids: ExternalIds {
            tmdb: None,
            imdb: series.imdb_id.clone(),
            tvdb: series.tvdb_id.map(|id| id.to_string()),
            tvdb_series: series.tvdb_id.map(|id| id.to_string()),
            tvdb_movie: None,
            anilist: None,
            anidb: None,
            mal: None,
            kitsu: None,
        },
        title: series.title.clone(),
        year: series.year,
        season: Some(episode.season_number),
        episode: Some(episode.episode_number),
    };

    let (video_codec, audio_codec) = file
        .media_info
        .as_ref()
        .map(|mi| {
            (
                mi.video_codec.as_ref().map(|v| v.to_ascii_lowercase()),
                mi.audio_codec.as_ref().map(|v| v.to_ascii_lowercase()),
            )
        })
        .unwrap_or((None, None));

    let descriptor = FileDescriptor {
        path: file.path.clone(),
        size_bytes: file.size,
        hash: None,
        container: container_from_path(&file.path),
        video_codec,
        audio_codec,
    };

    let mut extension_metadata = HashMap::new();
    let mut sonarr_meta = serde_json::Map::new();
    if let Some(q) = &file.quality {
        if let Ok(val) = serde_json::to_value(q) {
            sonarr_meta.insert("quality".to_string(), val);
        }
    }
    if let Some(mi) = &file.media_info {
        if let Ok(val) = serde_json::to_value(mi) {
            sonarr_meta.insert("mediaInfo".to_string(), val);
        }
    }
    if !sonarr_meta.is_empty() {
        extension_metadata.insert("sonarr".to_string(), serde_json::Value::Object(sonarr_meta));
    }

    Some(MediaFileCandidate {
        identity,
        files: vec![descriptor],
        extension_metadata,
        source_config_id: None,
    })
}

fn container_from_path(path: &str) -> Option<String> {
    Path::new(path)
        .extension()
        .and_then(|e| e.to_str())
        .map(|s| s.to_ascii_lowercase())
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SonarrSeries {
    id: i64,
    title: String,
    tvdb_id: Option<i64>,
    imdb_id: Option<String>,
    year: Option<i32>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SonarrEpisode {
    id: i64,
    season_number: i32,
    episode_number: i32,
    episode_file_id: Option<i64>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct SonarrEpisodeFile {
    id: i64,
    path: String,
    size: Option<i64>,
    media_info: Option<SonarrMediaInfo>,
    quality: Option<SonarrQualityInfo>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct SonarrMediaInfo {
    video_codec: Option<String>,
    audio_codec: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct SonarrQualityInfo {
    quality: Option<SonarrQuality>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct SonarrQuality {
    name: Option<String>,
    source: Option<String>,
    resolution: Option<i32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_candidate_with_series_and_episode_fields() {
        let series = SonarrSeries {
            id: 1,
            title: "Example Show".to_string(),
            tvdb_id: Some(12345),
            imdb_id: Some("tt123".to_string()),
            year: Some(2024),
        };
        let episode = SonarrEpisode {
            id: 10,
            season_number: 1,
            episode_number: 2,
            episode_file_id: Some(99),
        };
        let file = SonarrEpisodeFile {
            id: 99,
            path: "/media/Example.Show.S01E02.mkv".to_string(),
            size: Some(1024),
            media_info: Some(SonarrMediaInfo {
                video_codec: Some("H264".to_string()),
                audio_codec: Some("AAC".to_string()),
            }),
            quality: Some(SonarrQualityInfo {
                quality: Some(SonarrQuality {
                    name: Some("HD-1080p".to_string()),
                    source: Some("Web".to_string()),
                    resolution: Some(1080),
                }),
            }),
        };

        let candidate = make_candidate(&series, &episode, &file).expect("candidate");
        assert_eq!(candidate.identity.title, "Example Show");
        assert_eq!(candidate.identity.season, Some(1));
        assert_eq!(candidate.identity.episode, Some(2));
        assert_eq!(
            candidate.identity.external_ids.tvdb.as_deref(),
            Some("12345")
        );
        assert_eq!(candidate.files.len(), 1);
        assert_eq!(candidate.files[0].container.as_deref(), Some("mkv"));
        assert_eq!(candidate.files[0].video_codec.as_deref(), Some("h264"));
        assert_eq!(candidate.files[0].audio_codec.as_deref(), Some("aac"));
        assert!(candidate.extension_metadata.contains_key("sonarr"));
    }
}
