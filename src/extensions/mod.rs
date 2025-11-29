use std::{collections::HashMap, fs, path::Path};

use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use globwalk::GlobWalkerBuilder;
use serde::{Deserialize, Serialize};
use tokio::fs as tokio_fs;
use tokio::io::AsyncReadExt;
use tracing::debug;

use crate::db::models::MediaType;

#[derive(Debug, Clone, Deserialize)]
pub struct ExtensionManifest {
    pub id: String,
    pub name: String,
    pub version: String,
    #[serde(default)]
    pub capabilities: ExtensionCapabilities,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ExtensionCapabilities {
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ExternalIds {
    pub tmdb: Option<String>,
    pub imdb: Option<String>,
    pub tvdb: Option<String>,
    pub anilist: Option<String>,
    pub mal: Option<String>,
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
}

#[async_trait]
pub trait FileSource: Send + Sync {
    async fn scan(&self, since: Option<DateTime<Utc>>) -> Result<Vec<MediaFileCandidate>>;
}

pub struct ExtensionManager {
    file_sources: Vec<RegisteredFileSource>,
}

struct RegisteredFileSource {
    manifest: ExtensionManifest,
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
        manifest: ExtensionManifest,
        source: Box<dyn FileSource>,
    ) {
        self.file_sources
            .push(RegisteredFileSource { manifest, source });
    }

    pub async fn load_from_dir(path: &str, local_root: &str) -> Result<Self> {
        let mut manager = Self::new();
        if let Ok(entries) = fs::read_dir(path) {
            for entry in entries {
                let entry = entry?;
                let manifest_path = entry.path().join("manifest.json");
                if manifest_path.exists() {
                    let manifest = read_manifest(&manifest_path.to_string_lossy()).await?;
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
            ExtensionManifest {
                id: "elixir.localfolder".to_string(),
                name: "Local Folder".to_string(),
                version: "0.0.1".to_string(),
                capabilities: ExtensionCapabilities { file_source: true },
            },
            Box::new(LocalFolderSource {
                root_path: local_root.to_string(),
            }),
        );
        Ok(manager)
    }

    pub async fn scan_all(&self) -> Result<Vec<MediaFileCandidate>> {
        let mut results = Vec::new();
        for reg in &self.file_sources {
            if reg.manifest.capabilities.file_source {
                let mut entries = reg.source.scan(None).await?;
                results.append(&mut entries);
            }
        }
        Ok(results)
    }
}

async fn read_manifest(path: &str) -> Result<ExtensionManifest> {
    let mut file = tokio_fs::File::open(path)
        .await
        .with_context(|| format!("opening manifest at {path}"))?;
    let mut buf = String::new();
    file.read_to_string(&mut buf).await?;
    let manifest: ExtensionManifest =
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
}

#[async_trait]
impl FileSource for LocalFolderSource {
    async fn scan(&self, _since: Option<DateTime<Utc>>) -> Result<Vec<MediaFileCandidate>> {
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
            if let Some(candidate) = build_candidate_from_path(path) {
                debug!("found media candidate at {}", candidate.files[0].path);
                candidates.push(candidate);
            }
        }
        Ok(candidates)
    }
}

fn build_candidate_from_path(path: &Path) -> Option<MediaFileCandidate> {
    let file_name = path.file_stem()?.to_string_lossy().to_string();
    let mut parts = file_name.split(['.', ' ']).collect::<Vec<_>>();
    if parts.is_empty() {
        return None;
    }

    // crude parse: "Title (Year)" or "Title.Year"
    let mut title = parts.join(" ");
    let mut year = None;
    if let Some(last) = parts.last() {
        if let Ok(y) = last.parse::<i32>() {
            year = Some(y);
            parts.pop();
            title = parts.join(" ");
        }
    }

    let identity = MediaIdentity {
        r#type: MediaType::Movie,
        external_ids: ExternalIds::default(),
        title: title.trim().to_string(),
        year,
        season: None,
        episode: None,
    };

    let desc = FileDescriptor {
        path: path.to_string_lossy().to_string(),
        size_bytes: path.metadata().ok().map(|m| m.len() as i64),
        hash: None,
        container: path.extension().map(|e| e.to_string_lossy().to_string()),
        video_codec: None,
        audio_codec: None,
    };

    Some(MediaFileCandidate {
        identity,
        files: vec![desc],
        extension_metadata: HashMap::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use tokio::fs::File;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn local_folder_scans_media_files() -> Result<()> {
        let dir = tempdir()?;
        let file_path = dir.path().join("Example.2024.mkv");
        let mut f = File::create(&file_path).await?;
        f.write_all(b"dummy").await?;

        let source = LocalFolderSource {
            root_path: dir.path().to_string_lossy().to_string(),
        };

        let items = source.scan(None).await?;
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].identity.title, "Example");
        assert_eq!(items[0].identity.year, Some(2024));

        Ok(())
    }
}

pub fn make_identity_key(identity: &MediaIdentity) -> String {
    if let Some(tmdb) = &identity.external_ids.tmdb {
        format!("tmdb:{tmdb}")
    } else if let Some(imdb) = &identity.external_ids.imdb {
        format!("imdb:{imdb}")
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
