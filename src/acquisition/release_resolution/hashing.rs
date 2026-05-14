use std::{
    collections::BTreeSet,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    time::{Duration, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use crc32fast::Hasher as Crc32Hasher;
use md4::{Digest as Md4DigestTrait, Md4};
use serde_json::{Value as JsonValue, json};
use sqlx::AnyPool;
use tokio::time::MissedTickBehavior;
use uuid::Uuid;

use crate::{
    acquisition::release_resolution::{
        models::{AcquisitionFileHash, AnimeFileHashStatus, NewAcquisitionFileHash},
        store::{
            get_file_hash_by_ed2k_size, get_file_hash_by_local_file_id, get_file_hash_by_path,
            list_file_hash_work, upsert_file_hash,
        },
    },
    state::AppState,
};

pub const ANIME_HASH_WORKER_VERSION: &str = "rr3g-local-file-hash-v0";
pub const ED2K_CHUNK_SIZE: usize = 9_728_000;
const DEFAULT_HASH_POLL_INTERVAL_SECONDS: u64 = 30;
const DEFAULT_HASH_BATCH_SIZE: usize = 4;
const DEFAULT_HASH_CONCURRENCY: usize = 1;
const DEFAULT_READ_BUFFER_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone)]
pub struct AnimeHashWorkerConfig {
    pub poll_interval: Duration,
    pub batch_size: usize,
    pub max_concurrency: usize,
    pub read_buffer_bytes: usize,
}

impl Default for AnimeHashWorkerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(DEFAULT_HASH_POLL_INTERVAL_SECONDS),
            batch_size: DEFAULT_HASH_BATCH_SIZE,
            max_concurrency: DEFAULT_HASH_CONCURRENCY,
            read_buffer_bytes: DEFAULT_READ_BUFFER_BYTES,
        }
    }
}

#[derive(Debug, Clone)]
pub struct HashFileJob {
    pub release_file_id: Option<Uuid>,
    pub local_file_id: Option<String>,
    pub file_path: PathBuf,
    pub force_rehash: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashFileAction {
    Queued,
    Reused,
    Hashed,
    Failed,
}

#[derive(Debug, Clone)]
pub struct HashFileOutcome {
    pub action: HashFileAction,
    pub file_hash: AcquisitionFileHash,
    pub duplicate_of_file_hash_id: Option<Uuid>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AnimeHashWorkerStats {
    pub scanned: usize,
    pub hashed: usize,
    pub failed: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalFileDigest {
    pub size_bytes: i64,
    pub mtime_fingerprint: Option<String>,
    pub ed2k: String,
    pub crc32: String,
}

pub async fn start_anime_hash_worker_loop(state: AppState) {
    let config = AnimeHashWorkerConfig::default();
    let mut interval = tokio::time::interval(config.poll_interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        match run_anime_hash_worker_pass(&state.db_pool, config.clone()).await {
            Ok(stats) if stats.scanned > 0 => {
                tracing::debug!(
                    scanned = stats.scanned,
                    hashed = stats.hashed,
                    failed = stats.failed,
                    version = ANIME_HASH_WORKER_VERSION,
                    "processed anime hash worker pass"
                );
            }
            Ok(_) => {}
            Err(err) => tracing::warn!("anime hash worker pass failed: {err}"),
        }
    }
}

pub async fn queue_anime_hash_file(pool: &AnyPool, job: HashFileJob) -> Result<HashFileOutcome> {
    let metadata = tokio::fs::metadata(&job.file_path)
        .await
        .with_context(|| format!("reading metadata for '{}'", job.file_path.display()))?;
    if !metadata.is_file() {
        bail!(
            "hash target '{}' is not a regular file",
            job.file_path.display()
        );
    }
    let size_bytes =
        i64::try_from(metadata.len()).context("hash target size does not fit in i64")?;
    let mtime_fingerprint = metadata_fingerprint(&metadata);
    let path = normalized_path_string(&job.file_path);
    let existing = existing_hash_record(pool, job.local_file_id.as_deref(), &path).await?;
    let now = Utc::now();
    let filename_history = filename_history_with_observation(existing.as_ref(), &path, Some(now));
    let release_file_id = job
        .release_file_id
        .or_else(|| existing.as_ref().and_then(|hash| hash.release_file_id));
    let local_file_id = job
        .local_file_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .or_else(|| {
            existing
                .as_ref()
                .and_then(|hash| hash.local_file_id.clone())
        });

    if let Some(existing) = existing.as_ref()
        && !job.force_rehash
        && existing.hash_status == AnimeFileHashStatus::Hashed
        && existing.size_bytes == size_bytes
        && existing.mtime_fingerprint == mtime_fingerprint
        && existing.ed2k.is_some()
        && existing.crc32.is_some()
    {
        let file_hash = upsert_file_hash(
            pool,
            NewAcquisitionFileHash {
                file_hash_id: Some(existing.file_hash_id),
                release_file_id,
                local_file_id,
                file_path: path,
                size_bytes,
                mtime_fingerprint,
                ed2k: existing.ed2k.clone(),
                crc32: existing.crc32.clone(),
                hash_status: AnimeFileHashStatus::Hashed,
                hash_computed_at: existing.hash_computed_at,
                hash_invalidated_at: existing.hash_invalidated_at,
                filename_history,
            },
        )
        .await?;
        return Ok(HashFileOutcome {
            action: HashFileAction::Reused,
            file_hash,
            duplicate_of_file_hash_id: None,
            error: None,
        });
    }

    let changed = existing.as_ref().is_some_and(|existing| {
        existing.size_bytes != size_bytes || existing.mtime_fingerprint != mtime_fingerprint
    });
    let hash_status = if existing.is_some() && (changed || job.force_rehash) {
        AnimeFileHashStatus::Invalidated
    } else {
        AnimeFileHashStatus::Pending
    };
    let hash_invalidated_at = if hash_status == AnimeFileHashStatus::Invalidated {
        Some(now)
    } else {
        existing.as_ref().and_then(|hash| hash.hash_invalidated_at)
    };
    let file_hash = upsert_file_hash(
        pool,
        NewAcquisitionFileHash {
            file_hash_id: existing.as_ref().map(|hash| hash.file_hash_id),
            release_file_id,
            local_file_id,
            file_path: path,
            size_bytes,
            mtime_fingerprint,
            ed2k: None,
            crc32: None,
            hash_status,
            hash_computed_at: None,
            hash_invalidated_at,
            filename_history,
        },
    )
    .await?;
    Ok(HashFileOutcome {
        action: HashFileAction::Queued,
        file_hash,
        duplicate_of_file_hash_id: None,
        error: None,
    })
}

pub async fn run_anime_hash_worker_pass(
    pool: &AnyPool,
    config: AnimeHashWorkerConfig,
) -> Result<AnimeHashWorkerStats> {
    let batch_size = config.batch_size.max(1).min(128);
    let jobs = list_file_hash_work(pool, i64::try_from(batch_size).unwrap_or(128)).await?;
    let mut stats = AnimeHashWorkerStats {
        scanned: jobs.len(),
        ..Default::default()
    };
    let max_concurrency = config.max_concurrency.max(1).min(batch_size);
    for chunk in jobs.chunks(max_concurrency) {
        let mut handles = Vec::with_capacity(chunk.len());
        for file_hash in chunk.iter().cloned() {
            let pool = pool.clone();
            let config = config.clone();
            handles.push(tokio::spawn(async move {
                hash_existing_file_hash(&pool, file_hash, &config).await
            }));
        }
        for handle in handles {
            match handle
                .await
                .context("joining anime hash worker task")??
                .action
            {
                HashFileAction::Hashed | HashFileAction::Reused => stats.hashed += 1,
                HashFileAction::Failed => stats.failed += 1,
                HashFileAction::Queued => {}
            }
        }
    }
    Ok(stats)
}

pub async fn hash_existing_file_hash(
    pool: &AnyPool,
    existing: AcquisitionFileHash,
    config: &AnimeHashWorkerConfig,
) -> Result<HashFileOutcome> {
    let path = existing.file_path.clone();
    let digest = match hash_local_file(PathBuf::from(&path), config.read_buffer_bytes).await {
        Ok(digest) => digest,
        Err(err) => {
            let error = err.to_string();
            let failed = upsert_file_hash(
                pool,
                NewAcquisitionFileHash {
                    file_hash_id: Some(existing.file_hash_id),
                    release_file_id: existing.release_file_id,
                    local_file_id: existing.local_file_id.clone(),
                    file_path: existing.file_path.clone(),
                    size_bytes: existing.size_bytes,
                    mtime_fingerprint: existing.mtime_fingerprint.clone(),
                    ed2k: existing.ed2k.clone(),
                    crc32: existing.crc32.clone(),
                    hash_status: AnimeFileHashStatus::Failed,
                    hash_computed_at: existing.hash_computed_at,
                    hash_invalidated_at: existing.hash_invalidated_at.or_else(|| Some(Utc::now())),
                    filename_history: filename_history_with_observation(
                        Some(&existing),
                        &path,
                        Some(Utc::now()),
                    ),
                },
            )
            .await?;
            return Ok(HashFileOutcome {
                action: HashFileAction::Failed,
                file_hash: failed,
                duplicate_of_file_hash_id: None,
                error: Some(error),
            });
        }
    };

    let duplicate = get_file_hash_by_ed2k_size(pool, &digest.ed2k, digest.size_bytes)
        .await?
        .and_then(|hash| (hash.file_hash_id != existing.file_hash_id).then_some(hash.file_hash_id));
    let now = Utc::now();
    let file_hash = upsert_file_hash(
        pool,
        NewAcquisitionFileHash {
            file_hash_id: Some(existing.file_hash_id),
            release_file_id: existing.release_file_id,
            local_file_id: existing.local_file_id.clone(),
            file_path: path.clone(),
            size_bytes: digest.size_bytes,
            mtime_fingerprint: digest.mtime_fingerprint,
            ed2k: Some(digest.ed2k),
            crc32: Some(digest.crc32),
            hash_status: AnimeFileHashStatus::Hashed,
            hash_computed_at: Some(now),
            hash_invalidated_at: existing.hash_invalidated_at,
            filename_history: filename_history_with_observation(Some(&existing), &path, Some(now)),
        },
    )
    .await?;
    Ok(HashFileOutcome {
        action: HashFileAction::Hashed,
        file_hash,
        duplicate_of_file_hash_id: duplicate,
        error: None,
    })
}

pub async fn hash_local_file(path: PathBuf, read_buffer_bytes: usize) -> Result<LocalFileDigest> {
    tokio::task::spawn_blocking(move || {
        let buffer_size = read_buffer_bytes.max(8 * 1024);
        hash_local_file_blocking(&path, buffer_size, ED2K_CHUNK_SIZE)
            .with_context(|| format!("hashing '{}'", path.display()))
    })
    .await
    .context("joining local hash task")?
}

fn hash_local_file_blocking(
    path: &Path,
    read_buffer_bytes: usize,
    ed2k_chunk_size: usize,
) -> Result<LocalFileDigest> {
    let before = std::fs::metadata(path)?;
    if !before.is_file() {
        bail!("hash target '{}' is not a regular file", path.display());
    }
    let mut file = File::open(path)?;
    let (size_bytes, ed2k, crc32) = hash_reader(&mut file, ed2k_chunk_size, read_buffer_bytes)?;
    let after = std::fs::metadata(path)?;
    let before_fingerprint = metadata_fingerprint(&before);
    let after_fingerprint = metadata_fingerprint(&after);
    if before.len() != after.len() || before_fingerprint != after_fingerprint {
        bail!("file changed while hashing '{}'", path.display());
    }
    Ok(LocalFileDigest {
        size_bytes,
        mtime_fingerprint: after_fingerprint,
        ed2k,
        crc32,
    })
}

fn hash_reader<R: Read>(
    reader: &mut R,
    ed2k_chunk_size: usize,
    read_buffer_bytes: usize,
) -> Result<(i64, String, String)> {
    if ed2k_chunk_size == 0 {
        bail!("ED2K chunk size cannot be zero");
    }
    let mut crc32 = Crc32Hasher::new();
    let mut chunk_hasher = Md4::new();
    let mut chunk_len = 0_usize;
    let mut total_len = 0_u64;
    let mut chunk_digests = Vec::<[u8; 16]>::new();
    let mut buffer = vec![0_u8; read_buffer_bytes.max(1)];

    loop {
        let remaining_in_chunk = ed2k_chunk_size - chunk_len;
        let read_len = remaining_in_chunk.min(buffer.len());
        let bytes_read = reader.read(&mut buffer[..read_len])?;
        if bytes_read == 0 {
            break;
        }
        let bytes = &buffer[..bytes_read];
        crc32.update(bytes);
        chunk_hasher.update(bytes);
        chunk_len += bytes_read;
        total_len = total_len.saturating_add(bytes_read as u64);
        if chunk_len == ed2k_chunk_size {
            chunk_digests.push(finalize_md4(std::mem::take(&mut chunk_hasher)));
            chunk_hasher = Md4::new();
            chunk_len = 0;
        }
    }

    if total_len == 0 || chunk_len > 0 {
        chunk_digests.push(finalize_md4(chunk_hasher));
    }
    let ed2k_bytes = if chunk_digests.len() == 1 {
        chunk_digests[0]
    } else {
        let mut root = Md4::new();
        for digest in &chunk_digests {
            root.update(digest);
        }
        finalize_md4(root)
    };
    let size_bytes = i64::try_from(total_len).context("hashed file size does not fit in i64")?;
    Ok((
        size_bytes,
        hex_lower(&ed2k_bytes),
        format!("{:08X}", crc32.finalize()),
    ))
}

async fn existing_hash_record(
    pool: &AnyPool,
    local_file_id: Option<&str>,
    path: &str,
) -> Result<Option<AcquisitionFileHash>> {
    if let Some(local_file_id) = local_file_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if let Some(hash) = get_file_hash_by_local_file_id(pool, local_file_id).await? {
            return Ok(Some(hash));
        }
    }
    get_file_hash_by_path(pool, path).await
}

fn filename_history_with_observation(
    existing: Option<&AcquisitionFileHash>,
    path: &str,
    observed_at: Option<DateTime<Utc>>,
) -> JsonValue {
    let mut values = existing
        .and_then(|hash| hash.filename_history.as_array().cloned())
        .unwrap_or_default();
    let basename = Path::new(path)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(path)
        .to_string();
    let mut seen = values
        .iter()
        .filter_map(history_identity)
        .collect::<BTreeSet<_>>();
    let key = format!("{path}\n{basename}");
    if seen.insert(key) {
        values.push(json!({
            "path": path,
            "basename": basename,
            "observedAt": observed_at.unwrap_or_else(Utc::now),
        }));
    }
    JsonValue::Array(values)
}

fn history_identity(value: &JsonValue) -> Option<String> {
    if let Some(text) = value.as_str() {
        return Some(format!("\n{text}"));
    }
    let path = value.get("path").and_then(JsonValue::as_str).unwrap_or("");
    let basename = value
        .get("basename")
        .and_then(JsonValue::as_str)
        .unwrap_or("");
    (!path.is_empty() || !basename.is_empty()).then(|| format!("{path}\n{basename}"))
}

fn metadata_fingerprint(metadata: &std::fs::Metadata) -> Option<String> {
    let modified = metadata.modified().ok()?;
    let duration = modified.duration_since(UNIX_EPOCH).ok()?;
    Some(format!(
        "{}:{}:{}",
        metadata.len(),
        duration.as_secs(),
        duration.subsec_nanos()
    ))
}

fn normalized_path_string(path: &Path) -> String {
    path.to_string_lossy().trim().to_string()
}

fn finalize_md4(hasher: Md4) -> [u8; 16] {
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..]);
    bytes
}

fn hex_lower(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::DatabaseConfig, db::Database};
    use serde_json::Value as JsonValue;
    use tempfile::tempdir;
    use tokio::fs;

    async fn setup_db() -> Result<Database> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;
        Ok(database)
    }

    #[test]
    fn ed2k_single_chunk_matches_md4_known_vector() -> Result<()> {
        let input = b"The quick brown fox jumps over the lazy dog";
        let (_, ed2k, crc32) = hash_reader(&mut std::io::Cursor::new(input), ED2K_CHUNK_SIZE, 8)?;
        assert_eq!(ed2k, "1bee69a46ba811185c194762abaeae90");
        assert_eq!(crc32, "414FA339");
        Ok(())
    }

    #[test]
    fn ed2k_multi_chunk_hashes_md4_of_chunk_hashes() -> Result<()> {
        let input = b"abcdefghij";
        let (_, ed2k, _) = hash_reader(&mut std::io::Cursor::new(input), 4, 3)?;

        let mut root = Md4::new();
        for chunk in input.chunks(4) {
            let mut md4 = Md4::new();
            md4.update(chunk);
            root.update(finalize_md4(md4));
        }
        assert_eq!(ed2k, hex_lower(&finalize_md4(root)));
        Ok(())
    }

    #[tokio::test]
    async fn hash_worker_queues_reuses_and_invalidates_changed_files() -> Result<()> {
        let database = setup_db().await?;
        let dir = tempdir()?;
        let file_path = dir.path().join("Anime Series - 01.mkv");
        fs::write(&file_path, b"episode-one").await?;

        let queued = queue_anime_hash_file(
            &database.pool,
            HashFileJob {
                release_file_id: None,
                local_file_id: Some("local-anime-1".to_string()),
                file_path: file_path.clone(),
                force_rehash: false,
            },
        )
        .await?;
        assert_eq!(queued.file_hash.hash_status, AnimeFileHashStatus::Pending);

        let stats =
            run_anime_hash_worker_pass(&database.pool, AnimeHashWorkerConfig::default()).await?;
        assert_eq!(stats.scanned, 1);
        assert_eq!(stats.hashed, 1);
        let hashed = get_file_hash_by_path(&database.pool, &normalized_path_string(&file_path))
            .await?
            .expect("hashed file");
        assert_eq!(hashed.hash_status, AnimeFileHashStatus::Hashed);
        assert!(hashed.ed2k.is_some());
        assert_eq!(hashed.crc32.as_deref(), Some("066E0EFF"));

        let reused = queue_anime_hash_file(
            &database.pool,
            HashFileJob {
                release_file_id: None,
                local_file_id: Some("local-anime-1".to_string()),
                file_path: file_path.clone(),
                force_rehash: false,
            },
        )
        .await?;
        assert_eq!(reused.action, HashFileAction::Reused);
        assert_eq!(reused.file_hash.file_hash_id, hashed.file_hash_id);
        assert!(list_file_hash_work(&database.pool, 10).await?.is_empty());

        fs::write(&file_path, b"episode-one-v2").await?;
        let invalidated = queue_anime_hash_file(
            &database.pool,
            HashFileJob {
                release_file_id: None,
                local_file_id: Some("local-anime-1".to_string()),
                file_path: file_path.clone(),
                force_rehash: false,
            },
        )
        .await?;
        assert_eq!(
            invalidated.file_hash.hash_status,
            AnimeFileHashStatus::Invalidated
        );
        assert!(invalidated.file_hash.hash_invalidated_at.is_some());

        let stats =
            run_anime_hash_worker_pass(&database.pool, AnimeHashWorkerConfig::default()).await?;
        assert_eq!(stats.scanned, 1);
        assert_eq!(stats.hashed, 1);
        let rehashed = get_file_hash_by_local_file_id(&database.pool, "local-anime-1")
            .await?
            .expect("rehashed file");
        assert_eq!(rehashed.hash_status, AnimeFileHashStatus::Hashed);
        assert_eq!(rehashed.size_bytes, 14);
        assert!(rehashed.hash_invalidated_at.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn filename_history_tracks_moves_by_local_file_id() -> Result<()> {
        let database = setup_db().await?;
        let dir = tempdir()?;
        let first_path = dir.path().join("Anime Series - 01.mkv");
        let second_path = dir.path().join("Anime Series - 01v2.mkv");
        fs::write(&first_path, b"same-file").await?;
        queue_anime_hash_file(
            &database.pool,
            HashFileJob {
                release_file_id: None,
                local_file_id: Some("stable-local-file".to_string()),
                file_path: first_path.clone(),
                force_rehash: false,
            },
        )
        .await?;
        run_anime_hash_worker_pass(&database.pool, AnimeHashWorkerConfig::default()).await?;

        fs::rename(&first_path, &second_path).await?;
        let moved = queue_anime_hash_file(
            &database.pool,
            HashFileJob {
                release_file_id: None,
                local_file_id: Some("stable-local-file".to_string()),
                file_path: second_path.clone(),
                force_rehash: false,
            },
        )
        .await?;
        let fetched = get_file_hash_by_local_file_id(&database.pool, "stable-local-file")
            .await?
            .expect("moved file hash");
        assert_eq!(moved.file_hash.file_hash_id, fetched.file_hash_id);
        assert_eq!(fetched.file_path, normalized_path_string(&second_path));
        assert!(history_contains_basename(
            &fetched.filename_history,
            "Anime Series - 01.mkv"
        ));
        assert!(history_contains_basename(
            &fetched.filename_history,
            "Anime Series - 01v2.mkv"
        ));
        Ok(())
    }

    fn history_contains_basename(history: &JsonValue, basename: &str) -> bool {
        history.as_array().is_some_and(|items| {
            items.iter().any(|item| {
                item.as_str() == Some(basename)
                    || item.get("basename").and_then(JsonValue::as_str) == Some(basename)
            })
        })
    }
}
