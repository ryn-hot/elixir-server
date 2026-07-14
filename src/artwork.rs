use std::path::{Path, PathBuf};

use anyhow::Result;
use reqwest::Client;
use serde_json::Value;
use sqlx::AnyPool;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tracing::debug;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtworkKind {
    Poster,
    Backdrop,
    Banner,
    Thumbnail,
}

impl ArtworkKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            ArtworkKind::Poster => "poster",
            ArtworkKind::Backdrop => "backdrop",
            ArtworkKind::Banner => "banner",
            ArtworkKind::Thumbnail => "thumbnail",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "poster" => Some(ArtworkKind::Poster),
            "backdrop" => Some(ArtworkKind::Backdrop),
            "banner" => Some(ArtworkKind::Banner),
            "thumbnail" => Some(ArtworkKind::Thumbnail),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ArtworkCandidate {
    pub kind: ArtworkKind,
    pub url: String,
    pub language: Option<String>,
    pub width: Option<i32>,
    pub height: Option<i32>,
    pub provider: Option<String>,
    pub score: Option<f32>,
    pub metadata_json: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct StoredArtwork {
    pub id: Uuid,
    pub kind: ArtworkKind,
    pub url: String,
    pub provider: Option<String>,
    pub score: Option<f32>,
    pub width: Option<i32>,
    pub height: Option<i32>,
}

#[derive(Clone)]
pub struct ArtworkService {
    client: Client,
    cache_dir: PathBuf,
}

impl ArtworkService {
    pub fn new(cache_dir: impl Into<PathBuf>, timeout_seconds: u64) -> Result<Self> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(timeout_seconds))
            .build()?;
        Ok(Self {
            client,
            cache_dir: cache_dir.into(),
        })
    }

    pub fn cache_dir(&self) -> &Path {
        &self.cache_dir
    }

    pub async fn upsert_refs(
        &self,
        pool: &AnyPool,
        owner_type: &str,
        owner_id: Uuid,
        refs: &[ArtworkCandidate],
    ) -> Result<Vec<StoredArtwork>> {
        let mut stored = Vec::new();
        for candidate in refs {
            let Some(url) = normalize_artwork_url(&candidate.url) else {
                continue;
            };
            let existing = sqlx::query_scalar::<sqlx::Any, String>(
                "SELECT id FROM artwork_refs WHERE owner_type = $1 AND owner_id = $2 AND kind = $3 AND url = $4 LIMIT 1",
            )
            .bind(owner_type)
            .bind(owner_id.to_string())
            .bind(candidate.kind.as_str())
            .bind(&url)
            .fetch_optional(pool)
            .await?;

            let metadata_json = candidate
                .metadata_json
                .as_ref()
                .and_then(|value| serde_json::to_string(value).ok());

            let id = if let Some(id_str) = existing {
                sqlx::query::<sqlx::Any>(
                    "UPDATE artwork_refs SET language = COALESCE($1, language), width = COALESCE($2, width), height = COALESCE($3, height), provider = COALESCE($4, provider), score = COALESCE($5, score), metadata_json = COALESCE($6, metadata_json), updated_at = CURRENT_TIMESTAMP WHERE id = $7",
                )
                .bind(candidate.language.as_deref())
                .bind(candidate.width)
                .bind(candidate.height)
                .bind(candidate.provider.as_deref())
                .bind(candidate.score)
                .bind(metadata_json)
                .bind(&id_str)
                .execute(pool)
                .await?;
                Uuid::parse_str(&id_str)?
            } else {
                let id = Uuid::new_v4();
                sqlx::query::<sqlx::Any>(
                    "INSERT INTO artwork_refs (id, owner_type, owner_id, kind, url, language, width, height, provider, score, metadata_json, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                )
                .bind(id.to_string())
                .bind(owner_type)
                .bind(owner_id.to_string())
                .bind(candidate.kind.as_str())
                .bind(&url)
                .bind(candidate.language.as_deref())
                .bind(candidate.width)
                .bind(candidate.height)
                .bind(candidate.provider.as_deref())
                .bind(candidate.score)
                .bind(metadata_json)
                .execute(pool)
                .await?;
                id
            };

            stored.push(StoredArtwork {
                id,
                kind: candidate.kind,
                url,
                provider: candidate.provider.clone(),
                score: candidate.score,
                width: candidate.width,
                height: candidate.height,
            });
        }

        Ok(stored)
    }

    pub async fn cache_primary(
        &self,
        pool: &AnyPool,
        refs: &[StoredArtwork],
        provider_priority: &[&str],
    ) -> Result<()> {
        for kind in [
            ArtworkKind::Poster,
            ArtworkKind::Banner,
            ArtworkKind::Backdrop,
            ArtworkKind::Thumbnail,
        ] {
            let Some(primary) = select_primary(kind, refs, provider_priority) else {
                continue;
            };
            if let Err(err) = self.cache_artwork(pool, &primary).await {
                debug!("artwork cache failed for {}: {err}", primary.url);
            }
        }
        Ok(())
    }

    pub async fn ensure_cached(
        &self,
        pool: &AnyPool,
        artwork_id: Uuid,
        kind: ArtworkKind,
        url: &str,
    ) -> Result<Option<String>> {
        let existing = sqlx::query_scalar::<sqlx::Any, String>(
            "SELECT local_path FROM artwork_cache WHERE artwork_id = $1 LIMIT 1",
        )
        .bind(artwork_id.to_string())
        .fetch_optional(pool)
        .await?;

        if existing.is_some() {
            return Ok(existing);
        }

        let artwork = StoredArtwork {
            id: artwork_id,
            kind,
            url: url.to_string(),
            provider: None,
            score: None,
            width: None,
            height: None,
        };

        self.cache_artwork(pool, &artwork).await
    }

    async fn cache_artwork(
        &self,
        pool: &AnyPool,
        artwork: &StoredArtwork,
    ) -> Result<Option<String>> {
        let existing = sqlx::query_scalar::<sqlx::Any, String>(
            "SELECT local_path FROM artwork_cache WHERE artwork_id = $1 LIMIT 1",
        )
        .bind(artwork.id.to_string())
        .fetch_optional(pool)
        .await?;

        if let Some(path) = existing {
            return Ok(Some(path));
        }

        let resp = self.client.get(&artwork.url).send().await?;
        if !resp.status().is_success() {
            return Ok(None);
        }
        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(|value| value.to_string());
        let bytes = resp.bytes().await?;

        fs::create_dir_all(&self.cache_dir).await?;

        let ext = extension_from_content_type(content_type.as_deref())
            .or_else(|| extension_from_url(&artwork.url))
            .unwrap_or("jpg");
        let file_name = format!("{}.{}", artwork.id, ext);
        let path = self.cache_dir.join(file_name);

        let mut file = fs::File::create(&path).await?;
        file.write_all(&bytes).await?;

        sqlx::query::<sqlx::Any>(
            "INSERT INTO artwork_cache (id, artwork_id, local_path, cached_at) VALUES ($1, $2, $3, CURRENT_TIMESTAMP)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(artwork.id.to_string())
        .bind(path.to_string_lossy().to_string())
        .execute(pool)
        .await?;

        Ok(Some(path.to_string_lossy().to_string()))
    }
}

pub fn select_primary(
    kind: ArtworkKind,
    refs: &[StoredArtwork],
    provider_priority: &[&str],
) -> Option<StoredArtwork> {
    let mut candidates: Vec<&StoredArtwork> = refs.iter().filter(|r| r.kind == kind).collect();
    candidates.sort_by(|a, b| {
        let rank_a = provider_rank(a.provider.as_deref(), provider_priority);
        let rank_b = provider_rank(b.provider.as_deref(), provider_priority);
        if rank_a != rank_b {
            return rank_a.cmp(&rank_b);
        }
        let score_a = a.score.unwrap_or(0.0);
        let score_b = b.score.unwrap_or(0.0);
        if (score_a - score_b).abs() > f32::EPSILON {
            return score_b
                .partial_cmp(&score_a)
                .unwrap_or(std::cmp::Ordering::Equal);
        }
        let area_a = a.width.unwrap_or(0) * a.height.unwrap_or(0);
        let area_b = b.width.unwrap_or(0) * b.height.unwrap_or(0);
        area_b.cmp(&area_a)
    });
    candidates.first().map(|item| (*item).clone())
}

fn provider_rank(provider: Option<&str>, priority: &[&str]) -> usize {
    let Some(provider) = provider else {
        return priority.len();
    };
    priority
        .iter()
        .position(|p| p.eq_ignore_ascii_case(provider))
        .unwrap_or(priority.len())
}

fn normalize_artwork_url(url: &str) -> Option<String> {
    let trimmed = url.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        return Some(trimmed.to_string());
    }
    if trimmed.starts_with("//") {
        return Some(format!("https:{}", trimmed));
    }
    if trimmed.starts_with('/') {
        return Some(format!("https://artworks.thetvdb.com{}", trimmed));
    }
    None
}

fn extension_from_content_type(content_type: Option<&str>) -> Option<&'static str> {
    let content_type = content_type?;
    if content_type.contains("image/jpeg") {
        return Some("jpg");
    }
    if content_type.contains("image/png") {
        return Some("png");
    }
    if content_type.contains("image/webp") {
        return Some("webp");
    }
    if content_type.contains("image/gif") {
        return Some("gif");
    }
    None
}

fn extension_from_url(url: &str) -> Option<&str> {
    let path = url.split('?').next().unwrap_or(url);
    let file_name = path.rsplit('/').next().unwrap_or(path);
    let ext = file_name.rsplit('.').next()?;
    if ext.len() > 1 && ext.len() <= 5 && ext.chars().all(|c| c.is_ascii_alphanumeric()) {
        Some(ext)
    } else {
        None
    }
}

pub fn extract_anilist_artwork(meta: &Value) -> Vec<ArtworkCandidate> {
    let mut refs = Vec::new();
    let cover = meta.get("coverImage");
    if let Some(cover) = cover {
        for key in ["extraLarge", "large", "medium"] {
            if let Some(url) = cover.get(key).and_then(Value::as_str) {
                refs.push(ArtworkCandidate {
                    kind: ArtworkKind::Poster,
                    url: url.to_string(),
                    language: None,
                    width: None,
                    height: None,
                    provider: Some("anilist".to_string()),
                    score: None,
                    metadata_json: None,
                });
            }
        }
    }
    if let Some(url) = meta.get("bannerImage").and_then(Value::as_str) {
        refs.push(ArtworkCandidate {
            kind: ArtworkKind::Banner,
            url: url.to_string(),
            language: None,
            width: None,
            height: None,
            provider: Some("anilist".to_string()),
            score: None,
            metadata_json: None,
        });
    }
    refs
}

pub fn extract_cinemeta_artwork(meta: &Value) -> Vec<ArtworkCandidate> {
    let mut refs = Vec::new();
    if let Some(url) = meta.get("poster").and_then(Value::as_str) {
        refs.push(ArtworkCandidate {
            kind: ArtworkKind::Poster,
            url: url.to_string(),
            language: None,
            width: None,
            height: None,
            provider: Some("cinemeta".to_string()),
            score: None,
            metadata_json: None,
        });
    }
    if let Some(url) = meta.get("background").and_then(Value::as_str) {
        refs.push(ArtworkCandidate {
            kind: ArtworkKind::Backdrop,
            url: url.to_string(),
            language: None,
            width: None,
            height: None,
            provider: Some("cinemeta".to_string()),
            score: None,
            metadata_json: None,
        });
    }
    refs
}

pub fn extract_tvdb_entity_artwork(meta: &Value) -> Vec<ArtworkCandidate> {
    let mut refs = Vec::new();
    if let Some(url) = meta.get("image").and_then(Value::as_str) {
        refs.push(ArtworkCandidate {
            kind: ArtworkKind::Poster,
            url: url.to_string(),
            language: None,
            width: None,
            height: None,
            provider: Some("tvdb".to_string()),
            score: None,
            metadata_json: None,
        });
    }
    if let Some(url) = meta.get("banner").and_then(Value::as_str) {
        refs.push(ArtworkCandidate {
            kind: ArtworkKind::Banner,
            url: url.to_string(),
            language: None,
            width: None,
            height: None,
            provider: Some("tvdb".to_string()),
            score: None,
            metadata_json: None,
        });
    }
    refs
}

pub fn extract_tvdb_series_artwork(meta: &Value) -> Vec<ArtworkCandidate> {
    extract_tvdb_entity_artwork(meta)
}

#[derive(Debug, Clone)]
pub struct TvdbArtworkEntry {
    pub kind: ArtworkKind,
    pub url: String,
    pub language: Option<String>,
    pub width: Option<i32>,
    pub height: Option<i32>,
    pub score: Option<f32>,
    pub season_number: Option<i32>,
}

pub fn extract_tvdb_artworks(value: &Value) -> Vec<TvdbArtworkEntry> {
    let mut refs = Vec::new();
    let candidates = if let Some(arr) = value.as_array() {
        Some(arr)
    } else if let Some(arr) = value.get("artworks").and_then(Value::as_array) {
        Some(arr)
    } else if let Some(arr) = value.get("data").and_then(Value::as_array) {
        Some(arr)
    } else if let Some(arr) = value
        .get("data")
        .and_then(|v| v.get("artworks"))
        .and_then(Value::as_array)
    {
        Some(arr)
    } else {
        None
    };

    let Some(candidates) = candidates else {
        return refs;
    };

    for item in candidates {
        let url = item
            .get("image")
            .or_else(|| item.get("imageUrl"))
            .or_else(|| item.get("url"))
            .and_then(Value::as_str);
        let Some(url) = url else {
            continue;
        };
        if is_unsupported_tvdb_artwork_path(url) || is_unsupported_tvdb_artwork_type(item) {
            continue;
        }
        let kind = tvdb_artwork_kind(item);
        let Some(kind) = kind else {
            continue;
        };
        let language = item
            .get("language")
            .or_else(|| item.get("languageCode"))
            .and_then(Value::as_str)
            .map(|value| value.to_string());
        let score = item
            .get("score")
            .and_then(Value::as_f64)
            .map(|value| value as f32);
        let width = item
            .get("width")
            .and_then(Value::as_i64)
            .and_then(|value| i32::try_from(value).ok());
        let height = item
            .get("height")
            .and_then(Value::as_i64)
            .and_then(|value| i32::try_from(value).ok());
        let season_number = item
            .get("seasonNumber")
            .or_else(|| item.get("season"))
            .and_then(Value::as_i64)
            .map(|value| value as i32);

        refs.push(TvdbArtworkEntry {
            kind,
            url: url.to_string(),
            language,
            width,
            height,
            score,
            season_number,
        });
    }

    refs
}

fn tvdb_artwork_kind(item: &Value) -> Option<ArtworkKind> {
    item.get("typeName")
        .or_else(|| item.get("imageType"))
        .or_else(|| item.get("keyType"))
        .or_else(|| item.get("type"))
        .and_then(Value::as_str)
        .and_then(map_tvdb_kind)
        .or_else(|| {
            item.get("type")
                .and_then(Value::as_i64)
                .and_then(map_tvdb_numeric_kind)
        })
        .or_else(|| infer_tvdb_artwork_kind_from_dimensions(item))
}

fn map_tvdb_numeric_kind(value: i64) -> Option<ArtworkKind> {
    match value {
        14 => Some(ArtworkKind::Poster),
        15 => Some(ArtworkKind::Backdrop),
        13 | 24 | 25 => None,
        _ => None,
    }
}

fn is_unsupported_tvdb_artwork_path(url: &str) -> bool {
    let normalized = url.to_ascii_lowercase();
    normalized.contains("/clearart/")
        || normalized.contains("/clearlogo/")
        || normalized.contains("/actor/")
        || normalized.contains("/person/")
}

fn is_unsupported_tvdb_artwork_type(item: &Value) -> bool {
    matches!(item.get("type").and_then(Value::as_i64), Some(13 | 24 | 25))
}

fn infer_tvdb_artwork_kind_from_dimensions(item: &Value) -> Option<ArtworkKind> {
    let width = item.get("width").and_then(Value::as_f64)?;
    let height = item.get("height").and_then(Value::as_f64)?;
    if width <= 0.0 || height <= 0.0 {
        return None;
    }
    let ratio = width / height;
    if ratio >= 4.0 {
        Some(ArtworkKind::Banner)
    } else if ratio >= 1.35 {
        Some(ArtworkKind::Backdrop)
    } else if ratio < 1.0 {
        Some(ArtworkKind::Poster)
    } else {
        None
    }
}

fn map_tvdb_kind(value: &str) -> Option<ArtworkKind> {
    let normalized = value.to_lowercase();
    if normalized.contains("poster") {
        Some(ArtworkKind::Poster)
    } else if normalized.contains("banner") {
        Some(ArtworkKind::Banner)
    } else if normalized.contains("fanart") || normalized.contains("background") {
        Some(ArtworkKind::Backdrop)
    } else if normalized.contains("thumb") {
        Some(ArtworkKind::Thumbnail)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn tvdb_artwork_extraction_skips_clearart_clearlogo_and_people_images() {
        let value = json!({
            "artworks": [
                {
                    "image": "https://artworks.thetvdb.com/banners/v4/movie/330/clearart/6124a45419fa7.png",
                    "type": 24,
                    "width": 1000,
                    "height": 562,
                    "language": "eng",
                    "score": 100001
                },
                {
                    "image": "https://artworks.thetvdb.com/banners/v4/movie/330/clearlogo/6124a42d9a938.png",
                    "type": 25,
                    "width": 800,
                    "height": 310,
                    "language": "eng",
                    "score": 100001
                },
                {
                    "image": "https://artworks.thetvdb.com/banners/v4/actor/525247/photo/6075f14031529.jpg",
                    "type": 13,
                    "width": 300,
                    "height": 450,
                    "score": 0
                },
                {
                    "image": "https://artworks.thetvdb.com/banners/v4/movie/330/backgrounds/664a76d83adf3.jpg",
                    "type": 15,
                    "width": 1920,
                    "height": 1080,
                    "score": 100000
                }
            ]
        });

        let refs = extract_tvdb_artworks(&value);

        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].kind, ArtworkKind::Backdrop);
        assert_eq!(refs[0].width, Some(1920));
        assert_eq!(refs[0].height, Some(1080));
    }
}
