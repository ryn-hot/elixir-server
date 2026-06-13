use std::{collections::HashMap, fs, path::Path};

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use globwalk::GlobWalkerBuilder;
use serde::{Deserialize, Serialize};
use sqlx::AnyPool;
use tokio::fs as tokio_fs;
use tokio::io::AsyncReadExt;
use tracing::debug;
use uuid::Uuid;

use crate::{config::SonarrConfig, db::models::MediaType, extensions::sonarr::load_sonarr_sources};
use elixir_classifier::HintParser;
use elixir_classifier::hint::general_parser::GeneralParser;
use elixir_classifier::hint::{FileInput, LibraryType};

pub mod auto_managed;
pub mod cloudstream_registry;
pub mod managed_paths;
pub mod manifest;
pub mod nuvio_registry;
pub mod package;
pub mod permissions;
pub mod registry;
pub mod required_secrets;
pub mod source_artifacts;
pub mod store;
pub mod updater;

mod sonarr;

#[derive(Debug, Clone, Deserialize)]
pub struct FileSourceManifest {
    pub id: String,
    pub name: String,
    pub version: String,
    #[serde(default)]
    pub capabilities: FileSourceCapabilities,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct FileSourceCapabilities {
    #[serde(default)]
    pub file_source: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MediaIdentity {
    pub r#type: MediaType,
    pub external_ids: ExternalIds,
    pub title: String,
    pub year: Option<i32>,
    pub season: Option<i32>,
    pub episode: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ExternalIds {
    pub imdb: Option<String>,
    pub tmdb: Option<String>,
    pub tvdb: Option<String>,
    pub tvdb_series: Option<String>,
    pub tvdb_movie: Option<String>,
    pub anilist: Option<String>,
    pub anidb: Option<String>,
    pub mal: Option<String>,
    pub kitsu: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileDescriptor {
    pub path: String,
    pub size_bytes: Option<i64>,
    pub hash: Option<String>,
    pub container: Option<String>,
    pub video_codec: Option<String>,
    pub audio_codec: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MediaFileCandidate {
    pub identity: MediaIdentity,
    pub files: Vec<FileDescriptor>,
    #[serde(default)]
    pub extension_metadata: HashMap<String, serde_json::Value>,
    #[serde(default)]
    pub source_config_id: Option<Uuid>,
}

#[async_trait]
pub trait FileSource: Send + Sync {
    async fn scan(&self, since: Option<DateTime<Utc>>) -> Result<Vec<MediaFileCandidate>>;
}

pub struct ExtensionManager {
    file_sources: Vec<RegisteredFileSource>,
}

struct RegisteredFileSource {
    manifest: FileSourceManifest,
    source: Box<dyn FileSource>,
}

impl ExtensionManager {
    pub fn new() -> Self {
        Self {
            file_sources: Vec::new(),
        }
    }

    pub fn register_file_source(
        &mut self,
        manifest: FileSourceManifest,
        source: Box<dyn FileSource>,
    ) {
        self.file_sources
            .push(RegisteredFileSource { manifest, source });
    }

    pub async fn load_from_dir(path: &str, local_root: &str, hash_files: bool) -> Result<Self> {
        let mut manager = Self::new();
        if let Ok(entries) = fs::read_dir(path) {
            for entry in entries {
                let entry = entry?;
                let manifest_path = entry.path().join("manifest.json");
                if manifest_path.exists() {
                    let manifest =
                        read_file_source_manifest(&manifest_path.to_string_lossy()).await?;
                    if manifest.capabilities.file_source {
                        // Placeholder: actual per-extension implementation should be loaded here.
                        // For now we register a no-op source to keep the manager aware of the manifest.
                        manager.register_file_source(manifest.clone(), Box::new(NullFileSource));
                    }
                }
            }
        }
        // Always include built-in localfolder source (reads from env/config later).
        manager.register_file_source(
            FileSourceManifest {
                id: "elixir.localfolder".to_string(),
                name: "Local Folder".to_string(),
                version: "0.0.1".to_string(),
                capabilities: FileSourceCapabilities { file_source: true },
            },
            Box::new(LocalFolderSource {
                root_path: local_root.to_string(),
                hash_files,
            }),
        );

        Ok(manager)
    }

    pub async fn scan_all(&self, since: Option<DateTime<Utc>>) -> Result<Vec<MediaFileCandidate>> {
        let mut results = Vec::new();
        for reg in &self.file_sources {
            if reg.manifest.capabilities.file_source {
                let mut entries = reg.source.scan(since).await?;
                results.append(&mut entries);
            }
        }
        Ok(results)
    }

    pub async fn scan_all_with_db(
        &self,
        pool: &AnyPool,
        sonarr_config: &SonarrConfig,
        since: Option<DateTime<Utc>>,
    ) -> Result<Vec<MediaFileCandidate>> {
        let mut results = self.scan_all(since).await?;
        let sonarr_sources = load_sonarr_sources(pool, sonarr_config).await?;
        for source in sonarr_sources {
            let mut entries = source.scan(since).await?;
            results.append(&mut entries);
        }
        Ok(results)
    }
}

async fn read_file_source_manifest(path: &str) -> Result<FileSourceManifest> {
    let mut file = tokio_fs::File::open(path)
        .await
        .with_context(|| format!("opening manifest at {path}"))?;
    let mut buf = String::new();
    file.read_to_string(&mut buf).await?;
    let manifest: FileSourceManifest =
        serde_json::from_str(&buf).context("parsing extension manifest")?;
    Ok(manifest)
}

struct NullFileSource;

#[async_trait]
impl FileSource for NullFileSource {
    async fn scan(&self, _since: Option<DateTime<Utc>>) -> Result<Vec<MediaFileCandidate>> {
        Ok(vec![])
    }
}

/// Built-in local folder file source. Reads from a configured root path.
pub struct LocalFolderSource {
    pub root_path: String,
    pub hash_files: bool,
}

#[async_trait]
impl FileSource for LocalFolderSource {
    async fn scan(&self, since: Option<DateTime<Utc>>) -> Result<Vec<MediaFileCandidate>> {
        if !Path::new(&self.root_path).exists() {
            return Ok(vec![]);
        }
        let mut candidates = Vec::new();
        let walker = GlobWalkerBuilder::from_patterns(&self.root_path, &["**/*.mkv", "**/*.mp4"])
            .follow_links(false)
            .case_insensitive(true)
            .build()?
            .into_iter()
            .filter_map(Result::ok);

        for entry in walker {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            if let Some(since_ts) = since {
                if let Ok(meta) = path.metadata() {
                    if let Ok(modified) = meta.modified() {
                        let modified: DateTime<Utc> = modified.into();
                        if modified < since_ts {
                            continue;
                        }
                    }
                }
            }
            if let Some(candidate) = build_candidate_from_path(path, self.hash_files).await {
                debug!("found media candidate at {}", candidate.files[0].path);
                candidates.push(candidate);
            }
        }
        Ok(candidates)
    }
}

async fn build_candidate_from_path(path: &Path, hash_files: bool) -> Option<MediaFileCandidate> {
    let file_name = path.file_stem()?.to_string_lossy().to_string();
    let mut parts = file_name.split(['.', ' ']).collect::<Vec<_>>();
    if parts.is_empty() {
        return None;
    }

    let mut title = parts.join(" ");
    let mut year = extract_year(&title);
    if year.is_some() {
        title = strip_year_suffix(&title);
    } else if let Some(last) = parts.last() {
        if let Ok(y) = last.parse::<i32>() {
            year = Some(y);
            parts.pop();
            title = parts.join(" ");
        }
    }

    let (season, episode) = parse_season_episode(&file_name);
    let media_type = if season.is_some() || episode.is_some() {
        MediaType::Series
    } else {
        MediaType::Movie
    };
    let mut cleaned_title =
        derive_clean_title(&file_name, media_type).unwrap_or_else(|| title.trim().to_string());
    if media_type == MediaType::Movie {
        if let Some((folder_title, folder_year)) = movie_folder_identity_from_parent(path) {
            cleaned_title = folder_title;
            year = folder_year.or(year);
        }
    }

    let identity = MediaIdentity {
        r#type: media_type,
        external_ids: ExternalIds::default(),
        title: cleaned_title,
        year,
        season,
        episode,
    };

    let hash = if hash_files {
        compute_hash(&path.to_string_lossy()).await
    } else {
        None
    };

    let desc = FileDescriptor {
        path: path.to_string_lossy().to_string(),
        size_bytes: path.metadata().ok().map(|m| m.len() as i64),
        hash,
        container: path.extension().map(|e| e.to_string_lossy().to_string()),
        video_codec: None,
        audio_codec: None,
    };

    Some(MediaFileCandidate {
        identity,
        files: vec![desc],
        extension_metadata: HashMap::new(),
        source_config_id: None,
    })
}

fn extract_year(title: &str) -> Option<i32> {
    if let Some(start) = title.find('(') {
        if let Some(end) = title[start + 1..].find(')') {
            let year_str = &title[start + 1..start + 1 + end];
            if let Ok(y) = year_str.parse::<i32>() {
                return Some(y);
            }
        }
    }
    None
}

fn strip_year_suffix(title: &str) -> String {
    if let Some(idx) = title.rfind('(') {
        return title[..idx].trim().to_string();
    }
    title.to_string()
}

fn movie_folder_identity_from_parent(path: &Path) -> Option<(String, Option<i32>)> {
    let folder = path.parent()?.file_name()?.to_string_lossy();
    let year = extract_year(&folder)?;
    let title = strip_year_suffix(&folder);
    if title.trim().is_empty() {
        return None;
    }
    Some((title, Some(year)))
}

fn parse_season_episode(name: &str) -> (Option<i32>, Option<i32>) {
    let upper = name.to_ascii_uppercase();
    if let Some((season, episode)) = parse_sxxeyy(&upper) {
        return (Some(season), Some(episode));
    }
    if let Some((season, episode)) = parse_season_episode_words(&upper) {
        return (Some(season), Some(episode));
    }
    if let Some((season, episode)) = parse_x_pattern(&upper) {
        return (Some(season), Some(episode));
    }
    (None, None)
}

fn derive_clean_title(file_name: &str, media_type: MediaType) -> Option<String> {
    let library_type = match media_type {
        MediaType::Movie => LibraryType::Movie,
        MediaType::Series => LibraryType::Series,
        MediaType::Anime => LibraryType::Anime,
    };
    let mut input = FileInput::new(file_name.to_string());
    input.file_name = Some(file_name.to_string());
    input.library_type_hint = Some(library_type);
    let parser = GeneralParser::default();
    parser
        .parse(&input)
        .into_iter()
        .next()
        .map(|hint| hint.title)
        .filter(|title: &String| !title.trim().is_empty())
}

fn parse_sxxeyy(value: &str) -> Option<(i32, i32)> {
    let bytes = value.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'S' {
            i += 1;
            continue;
        }
        let mut j = i + 1;
        let mut season = String::new();
        while j < bytes.len() && bytes[j].is_ascii_digit() {
            season.push(bytes[j] as char);
            j += 1;
            if season.len() >= 3 {
                break;
            }
        }
        if season.is_empty() {
            i += 1;
            continue;
        }

        let mut k = j;
        while k < bytes.len() {
            let ch = bytes[k];
            if ch == b'E' {
                break;
            }
            if ch.is_ascii_alphanumeric() && !is_separator(ch) {
                break;
            }
            k += 1;
        }
        if k >= bytes.len() || bytes[k] != b'E' {
            i += 1;
            continue;
        }

        let mut l = k + 1;
        let mut episode = String::new();
        while l < bytes.len() && bytes[l].is_ascii_digit() {
            episode.push(bytes[l] as char);
            l += 1;
            if episode.len() >= 3 {
                break;
            }
        }
        if episode.is_empty() {
            i += 1;
            continue;
        }

        let s_num = season.parse::<i32>().ok()?;
        let e_num = episode.parse::<i32>().ok()?;
        return Some((s_num, e_num));
    }
    None
}

fn parse_season_episode_words(value: &str) -> Option<(i32, i32)> {
    let season_idx = value.find("SEASON")?;
    let after = &value[season_idx + "SEASON".len()..];
    let (season, season_end) = parse_number_after(after)?;
    let rest = &after[season_end..];
    let ep_idx = rest.find("EPISODE").or_else(|| rest.find("EP"))?;
    let ep_label_len = if rest[ep_idx..].starts_with("EPISODE") {
        "EPISODE".len()
    } else {
        "EP".len()
    };
    let after_ep = &rest[ep_idx + ep_label_len..];
    let (episode, _) = parse_number_after(after_ep)?;
    Some((season, episode))
}

fn parse_x_pattern(value: &str) -> Option<(i32, i32)> {
    let bytes = value.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if !bytes[i].is_ascii_digit() {
            i += 1;
            continue;
        }
        let mut j = i;
        let mut season = String::new();
        while j < bytes.len() && bytes[j].is_ascii_digit() {
            season.push(bytes[j] as char);
            j += 1;
            if season.len() >= 3 {
                break;
            }
        }
        if season.is_empty() || j >= bytes.len() || bytes[j] != b'X' {
            i += 1;
            continue;
        }
        let mut k = j + 1;
        let mut episode = String::new();
        while k < bytes.len() && bytes[k].is_ascii_digit() {
            episode.push(bytes[k] as char);
            k += 1;
            if episode.len() >= 3 {
                break;
            }
        }
        if episode.is_empty() {
            i += 1;
            continue;
        }
        let s_num = season.parse::<i32>().ok()?;
        let e_num = episode.parse::<i32>().ok()?;
        return Some((s_num, e_num));
    }
    None
}

fn parse_number_after(value: &str) -> Option<(i32, usize)> {
    let bytes = value.as_bytes();
    let mut i = 0;
    while i < bytes.len() && !bytes[i].is_ascii_digit() {
        i += 1;
    }
    if i >= bytes.len() {
        return None;
    }
    let mut digits = String::new();
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        digits.push(bytes[i] as char);
        i += 1;
        if digits.len() >= 3 {
            break;
        }
    }
    let num = digits.parse::<i32>().ok()?;
    Some((num, i))
}

fn is_separator(byte: u8) -> bool {
    matches!(byte, b'.' | b'_' | b'-' | b' ' | b'(' | b')' | b'[' | b']')
}

async fn compute_hash(path: &str) -> Option<String> {
    use tokio::io::AsyncReadExt;
    let file = tokio::fs::File::open(path).await.ok()?;
    let mut reader = tokio::io::BufReader::new(file);
    let mut hasher = blake3::Hasher::new();
    let mut buf = [0u8; 8192];
    let mut read = 0usize;
    loop {
        match reader.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => {
                hasher.update(&buf[..n]);
                read += n;
                if read > 10 * 1024 * 1024 {
                    break;
                }
            }
            Err(_) => return None,
        }
    }
    Some(hasher.finalize().to_hex().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use tokio::fs::File;
    use tokio::io::AsyncWriteExt;

    #[test]
    fn parse_season_episode_detects_patterns() {
        let (season, episode) =
            parse_season_episode("Solo.Leveling.S02E02.I.Suppose.You.Arent.Aware");
        assert_eq!(season, Some(2));
        assert_eq!(episode, Some(2));

        let (season, episode) = parse_season_episode("Show.Name.1x02.1080p");
        assert_eq!(season, Some(1));
        assert_eq!(episode, Some(2));

        let (season, episode) = parse_season_episode("Show Name Season 3 Episode 4");
        assert_eq!(season, Some(3));
        assert_eq!(episode, Some(4));

        let (season, episode) = parse_season_episode("Movie.Title.2024");
        assert_eq!(season, None);
        assert_eq!(episode, None);
    }

    #[tokio::test]
    async fn local_folder_scans_media_files() -> Result<()> {
        let dir = tempdir()?;
        let file_path = dir.path().join("Example.2024.mkv");
        let mut f = File::create(&file_path).await?;
        f.write_all(b"dummy").await?;

        let source = LocalFolderSource {
            root_path: dir.path().to_string_lossy().to_string(),
            hash_files: false,
        };

        let items = source.scan(None).await?;
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].identity.title, "Example");
        assert_eq!(items[0].identity.year, Some(2024));

        Ok(())
    }

    #[tokio::test]
    async fn local_folder_prefers_movie_folder_identity_for_release_names() -> Result<()> {
        let dir = tempdir()?;
        let movie_dir = dir.path().join("Casino Royale (2006)");
        tokio::fs::create_dir_all(&movie_dir).await?;
        let file_path = movie_dir.join("Casino Royale 2006 BluRay 1080p DDP 5 1 x264-hallowed.mkv");
        let mut f = File::create(&file_path).await?;
        f.write_all(b"dummy").await?;

        let source = LocalFolderSource {
            root_path: dir.path().to_string_lossy().to_string(),
            hash_files: false,
        };

        let items = source.scan(None).await?;
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].identity.r#type, MediaType::Movie);
        assert_eq!(items[0].identity.title, "Casino Royale");
        assert_eq!(items[0].identity.year, Some(2006));

        Ok(())
    }
}

pub fn make_identity_key(identity: &MediaIdentity) -> String {
    if let Some(imdb) = &identity.external_ids.imdb {
        format!("imdb:{imdb}")
    } else if let Some(tmdb) = &identity.external_ids.tmdb {
        format!("tmdb:{tmdb}")
    } else if let Some(tvdb) = &identity.external_ids.tvdb_series {
        format!("tvdb_series:{tvdb}")
    } else if let Some(tvdb) = &identity.external_ids.tvdb_movie {
        format!("tvdb_movie:{tvdb}")
    } else if let Some(anilist) = &identity.external_ids.anilist {
        format!("anilist:{anilist}")
    } else if let Some(mal) = &identity.external_ids.mal {
        format!("mal:{mal}")
    } else if let Some(anidb) = &identity.external_ids.anidb {
        format!("anidb:{anidb}")
    } else if let Some(kitsu) = &identity.external_ids.kitsu {
        format!("kitsu:{kitsu}")
    } else if let Some(tvdb) = &identity.external_ids.tvdb {
        format!("tvdb:{tvdb}")
    } else {
        format!(
            "{}:{}:{}",
            identity.r#type.as_str(),
            identity.title.to_lowercase(),
            identity.year.unwrap_or_default()
        )
    }
}

// Helper to convert MediaType to str for keying
impl MediaType {
    pub fn as_str(&self) -> &'static str {
        match self {
            MediaType::Movie => "movie",
            MediaType::Series => "series",
            MediaType::Anime => "anime",
        }
    }
}
