use std::collections::HashSet;

use anyhow::Result;
use chrono::Utc;
use sqlx::{AnyPool, Row};
use uuid::Uuid;

use crate::extensions::ExternalIds;
use crate::{
    db::models::MediaType,
    extensions::{FileDescriptor, MediaFileCandidate, MediaIdentity},
    media::ffprobe,
    metadata::{MetadataResult, MetadataService},
    state::AppState,
};

pub async fn run_full_scan(pool: &AnyPool, candidates: Vec<MediaFileCandidate>) -> Result<()> {
    run_full_scan_with_metadata(pool, None, candidates, false).await
}

pub async fn run_full_scan_with_metadata(
    pool: &AnyPool,
    metadata: Option<&MetadataService>,
    candidates: Vec<MediaFileCandidate>,
    force_metadata: bool,
) -> Result<()> {
    let mut seen_paths: HashSet<String> = HashSet::new();

    for candidate in candidates {
        let meta = if let Some(service) = metadata {
            let should_refresh = should_refresh_metadata(
                pool,
                &candidate.identity,
                service.ttl_seconds(),
                force_metadata,
            )
            .await?;
            if should_refresh {
                service
                    .fetch_metadata(&candidate.identity)
                    .await
                    .ok()
                    .flatten()
            } else {
                None
            }
        } else {
            None
        };
        let media_item_id = upsert_media_item(pool, &candidate.identity, meta.as_ref()).await?;
        for file in candidate.files {
            seen_paths.insert(file.path.clone());
            upsert_media_file(pool, media_item_id, &file).await?;
        }
    }

    // Mark missing
    let existing_paths: Vec<String> = sqlx::query_scalar::<sqlx::Any, String>(
        "SELECT path FROM media_files WHERE scan_state = 'ok'",
    )
    .fetch_all(pool)
    .await?;

    for path in existing_paths {
        if !seen_paths.contains(&path) {
            sqlx::query::<sqlx::Any>(
                "UPDATE media_files SET scan_state = 'missing' WHERE path = ?",
            )
            .bind(path)
            .execute(pool)
            .await?;
        }
    }

    Ok(())
}

fn merge_external_ids(base: &ExternalIds, incoming: Option<ExternalIds>) -> ExternalIds {
    if let Some(incoming) = incoming {
        ExternalIds {
            tmdb: base.tmdb.clone().or(incoming.tmdb),
            imdb: base.imdb.clone().or(incoming.imdb),
            tvdb: base.tvdb.clone().or(incoming.tvdb),
            anilist: base.anilist.clone().or(incoming.anilist),
            mal: base.mal.clone().or(incoming.mal),
        }
    } else {
        base.clone()
    }
}

async fn should_refresh_metadata(
    pool: &AnyPool,
    identity: &MediaIdentity,
    ttl_seconds: u64,
    force: bool,
) -> Result<bool> {
    if force {
        return Ok(true);
    }

    let existing = sqlx::query::<sqlx::Any>(
        "SELECT metadata_json, updated_at FROM media_items WHERE type = ? AND title = ? AND (year IS ? OR year = ?) LIMIT 1",
    )
    .bind(identity.r#type.as_str())
    .bind(&identity.title)
    .bind(identity.year)
    .fetch_optional(pool)
    .await?;

    if let Some(row) = existing {
        let meta: Option<String> = row.try_get("metadata_json").ok();
        if meta.is_none() {
            return Ok(true);
        }
        let updated: Option<String> = row.try_get("updated_at").ok();
        if let Some(updated) = updated {
            if let Ok(parsed) = updated.parse::<chrono::NaiveDateTime>() {
                let updated_ts = chrono::DateTime::<Utc>::from_naive_utc_and_offset(parsed, Utc);
                let age = Utc::now() - updated_ts;
                return Ok(age.num_seconds() as u64 > ttl_seconds);
            }
        }
        return Ok(false);
    }

    Ok(true)
}

pub async fn run_extension_scan(state: &AppState, force_metadata: bool) -> Result<()> {
    let candidates = state.extensions.scan_all().await?;
    run_full_scan_with_metadata(
        &state.db_pool,
        Some(&state.metadata),
        candidates,
        force_metadata,
    )
    .await?;
    Ok(())
}

pub async fn start_periodic_scan(state: AppState, interval_seconds: u64) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_seconds));
    loop {
        interval.tick().await;
        if let Err(err) = run_extension_scan(&state, false).await {
            tracing::warn!("periodic scan failed: {err}");
        }
    }
}

async fn upsert_media_item(
    pool: &AnyPool,
    identity: &MediaIdentity,
    meta: Option<&MetadataResult>,
) -> Result<Uuid> {
    // Try to find by type/title/year as a simple uniqueness approximation.
    let existing = sqlx::query::<sqlx::Any>(
        "SELECT id FROM media_items WHERE type = ? AND title = ? AND (year IS ? OR year = ?) LIMIT 1",
    )
    .bind(identity.r#type.as_str())
    .bind(&identity.title)
    .bind(identity.year)
    .fetch_optional(pool)
    .await?;

    if let Some(row) = existing {
        let id_str: String = row.get(0);
        let id = Uuid::parse_str(&id_str)?;
        sqlx::query::<sqlx::Any>(
            "UPDATE media_items SET external_ids = ?, season = ?, episode = ?, metadata_json = COALESCE(?, metadata_json), runtime_seconds = COALESCE(?, runtime_seconds), updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        )
        .bind(serde_json::to_string(&merge_external_ids(
            &identity.external_ids,
            meta.and_then(|m| m.external_ids.clone()),
        ))?)
        .bind(identity.season)
        .bind(identity.episode)
        .bind(meta.and_then(|m| serde_json::to_string(&m.metadata_json).ok()))
        .bind(meta.and_then(|m| m.runtime_seconds))
        .bind(id_str)
        .execute(pool)
        .await?;
        return Ok(id);
    }

    let id = Uuid::new_v4();
    sqlx::query::<sqlx::Any>("INSERT INTO media_items (id, type, external_ids, title, year, season, episode, metadata_json, runtime_seconds, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)")
        .bind(id.to_string())
        .bind(identity.r#type.as_str())
        .bind(serde_json::to_string(&merge_external_ids(
            &identity.external_ids,
            meta.and_then(|m| m.external_ids.clone()),
        ))?)
        .bind(&identity.title)
        .bind(identity.year)
        .bind(identity.season)
        .bind(identity.episode)
        .bind(meta.and_then(|m| serde_json::to_string(&m.metadata_json).ok()))
        .bind(meta.and_then(|m| m.runtime_seconds))
        .execute(pool)
        .await?;
    Ok(id)
}

async fn upsert_media_file(
    pool: &AnyPool,
    media_item_id: Uuid,
    file: &FileDescriptor,
) -> Result<()> {
    let metadata = ffprobe::probe(&file.path).await.unwrap_or_default();

    let existing = sqlx::query::<sqlx::Any>("SELECT id FROM media_files WHERE path = ? LIMIT 1")
        .bind(&file.path)
        .fetch_optional(pool)
        .await?;

    if let Some(row) = existing {
        let id_str: String = row.get(0);
        sqlx::query::<sqlx::Any>("UPDATE media_files SET size_bytes = ?, container = ?, video_codec = ?, audio_codec = ?, width = COALESCE(?, width), height = COALESCE(?, height), bitrate_bps = COALESCE(?, bitrate_bps), updated_at = CURRENT_TIMESTAMP, scan_state = 'ok' WHERE id = ?")
            .bind(file.size_bytes)
            .bind(metadata.container.as_ref().or(file.container.as_ref()))
            .bind(metadata.video_codec.as_ref().or(file.video_codec.as_ref()))
            .bind(metadata.audio_codec.as_ref().or(file.audio_codec.as_ref()))
            .bind(metadata.width)
            .bind(metadata.height)
            .bind(metadata.bitrate_bps)
            .bind(&id_str)
            .execute(pool)
            .await?;
        return Ok(());
    }

    let id = Uuid::new_v4();
    sqlx::query::<sqlx::Any>("INSERT INTO media_files (id, media_item_id, path, size_bytes, container, video_codec, audio_codec, width, height, bitrate_bps, scan_state, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ok', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)")
        .bind(id.to_string())
        .bind(media_item_id.to_string())
        .bind(&file.path)
        .bind(file.size_bytes)
        .bind(metadata.container.as_ref().or(file.container.as_ref()))
        .bind(metadata.video_codec.as_ref().or(file.video_codec.as_ref()))
        .bind(metadata.audio_codec.as_ref().or(file.audio_codec.as_ref()))
        .bind(metadata.width)
        .bind(metadata.height)
        .bind(metadata.bitrate_bps)
        .execute(pool)
        .await?;

    if let Some(duration) = metadata.duration_seconds {
        sqlx::query::<sqlx::Any>("UPDATE media_items SET runtime_seconds = COALESCE(runtime_seconds, ?), updated_at = CURRENT_TIMESTAMP WHERE id = ?")
            .bind(duration)
            .bind(media_item_id.to_string())
            .execute(pool)
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::DatabaseConfig,
        db::Database,
        extensions::{ExternalIds as ExtIds, FileDescriptor as FD, MediaIdentity},
    };
    use std::collections::HashMap;

    fn sample_identity() -> MediaIdentity {
        MediaIdentity {
            r#type: MediaType::Movie,
            external_ids: ExtIds {
                tmdb: Some("123".to_string()),
                imdb: None,
                tvdb: None,
                anilist: None,
                mal: None,
            },
            title: "Test Movie".to_string(),
            year: Some(2024),
            season: None,
            episode: None,
        }
    }

    #[tokio::test]
    async fn upsert_and_mark_missing() -> Result<()> {
        let config = DatabaseConfig {
            url: "sqlite::memory:?cache=shared".to_string(),
            max_connections: 1,
            connect_timeout_seconds: 5,
        };
        let database = Database::connect(&config).await?;
        database.run_migrations().await?;

        // Initial scan with one file
        let candidates = vec![MediaFileCandidate {
            identity: sample_identity(),
            files: vec![FD {
                path: "/media/movie.mkv".to_string(),
                size_bytes: Some(1024),
                hash: None,
                container: Some("mkv".to_string()),
                video_codec: Some("h264".to_string()),
                audio_codec: Some("aac".to_string()),
            }],
            extension_metadata: HashMap::new(),
        }];
        run_full_scan(&database.pool, candidates).await?;

        let (count_ok,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM media_files WHERE scan_state = 'ok'")
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(count_ok, 1);

        // Second scan with no files should mark missing
        run_full_scan(&database.pool, Vec::new()).await?;
        let (count_missing,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM media_files WHERE scan_state = 'missing'")
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(count_missing, 1);

        Ok(())
    }
}
