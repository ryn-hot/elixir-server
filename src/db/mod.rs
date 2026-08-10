pub mod models;
mod postgres_migrations;

use std::{path::Path, time::Duration};

use anyhow::{Context, Result, bail};
use sqlx::{AnyPool, any::AnyPoolOptions, migrate::Migrator};
use tokio::fs;

use crate::config::DatabaseConfig;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseDriver {
    Postgres,
    Sqlite,
}

impl DatabaseDriver {
    pub fn from_url(url: &str) -> Result<Self> {
        let lowered = url.to_lowercase();
        if lowered.starts_with("postgres://") || lowered.starts_with("postgresql://") {
            Ok(DatabaseDriver::Postgres)
        } else if lowered.starts_with("sqlite:") {
            Ok(DatabaseDriver::Sqlite)
        } else {
            bail!(
                "unsupported database driver (expected postgres:// or sqlite://): {}",
                url
            );
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            DatabaseDriver::Postgres => "postgres",
            DatabaseDriver::Sqlite => "sqlite",
        }
    }
}

pub(crate) fn numbered_bind_list(first: usize, count: usize) -> String {
    assert!(first > 0, "SQL bind parameters are one-indexed");
    (first..first + count)
        .map(|position| format!("${position}"))
        .collect::<Vec<_>>()
        .join(", ")
}

#[derive(Clone)]
pub struct Database {
    pub driver: DatabaseDriver,
    pub pool: AnyPool,
}

impl Database {
    pub async fn connect(config: &DatabaseConfig) -> Result<Self> {
        let driver = DatabaseDriver::from_url(&config.url)?;
        if matches!(driver, DatabaseDriver::Sqlite) {
            ensure_sqlite_file_ready(&config.url).await?;
        }

        // Enable default Any drivers (Postgres + SQLite). This is a no-op if already installed.
        sqlx::any::install_default_drivers();

        let max_connections = effective_pool_max_connections(driver, config.max_connections);
        if max_connections != config.max_connections {
            tracing::warn!(
                configured = config.max_connections,
                effective = max_connections,
                "PostgreSQL reserves a second database connection for cross-process library identity coordination"
            );
        }
        let pool = AnyPoolOptions::new()
            .max_connections(max_connections)
            .min_connections(1)
            .acquire_timeout(Duration::from_secs(config.connect_timeout_seconds))
            .connect(&config.url)
            .await
            .with_context(|| format!("failed to connect to database {}", config.url))?;

        if matches!(driver, DatabaseDriver::Sqlite) {
            // Enforce foreign keys when using SQLite to avoid silent integrity issues.
            sqlx::query("PRAGMA foreign_keys = ON;")
                .execute(&pool)
                .await
                .context("failed to enable SQLite foreign_keys pragma")?;
        }

        Ok(Self { driver, pool })
    }

    pub async fn run_migrations(&self) -> Result<()> {
        match self.driver {
            DatabaseDriver::Postgres => postgres_migrations::migrator()?.run(&self.pool).await,
            DatabaseDriver::Sqlite => MIGRATOR.run(&self.pool).await,
        }
        .context("database migrations failed")?;
        backfill_media_interaction_state(&self.pool).await?;
        Ok(())
    }
}

pub(crate) fn effective_pool_max_connections(driver: DatabaseDriver, configured: u32) -> u32 {
    match driver {
        // ALM-8 holds one PostgreSQL connection for its transaction-scoped
        // advisory lock while scan/import/repair work uses the pool. Silently
        // disabling that coordinator at one connection would make a supported
        // configuration race across server processes, so reserve the minimum
        // automatically instead of creating a user-facing setup decision.
        DatabaseDriver::Postgres => configured.max(2),
        DatabaseDriver::Sqlite => configured,
    }
}

async fn backfill_media_interaction_state(pool: &AnyPool) -> Result<()> {
    sqlx::query(
        "UPDATE playback_sessions
         SET selected_item_type = 'movie',
             selected_item_id = (
                 SELECT movie_id
                 FROM movie_files
                 WHERE movie_files.media_file_id = playback_sessions.media_file_id
                 LIMIT 1
             )
         WHERE selected_item_id IS NULL
           AND (
               SELECT COUNT(*)
               FROM movie_files
               WHERE movie_files.media_file_id = playback_sessions.media_file_id
           ) = 1
           AND NOT EXISTS (
               SELECT 1
               FROM episode_files
               WHERE episode_files.media_file_id = playback_sessions.media_file_id
           )",
    )
    .execute(pool)
    .await
    .context("backfilling movie playback session item context")?;

    sqlx::query(
        "UPDATE playback_sessions
         SET selected_item_type = 'episode',
             selected_item_id = (
                 SELECT episode_id
                 FROM episode_files
                 WHERE episode_files.media_file_id = playback_sessions.media_file_id
                 LIMIT 1
             ),
             selected_episode_id = (
                 SELECT episode_id
                 FROM episode_files
                 WHERE episode_files.media_file_id = playback_sessions.media_file_id
                 LIMIT 1
             ),
             selected_series_id = (
                 SELECT episodes.series_id
                 FROM episode_files
                 JOIN episodes ON episodes.id = episode_files.episode_id
                 WHERE episode_files.media_file_id = playback_sessions.media_file_id
                 LIMIT 1
             ),
             selected_season_id = (
                 SELECT episodes.season_id
                 FROM episode_files
                 JOIN episodes ON episodes.id = episode_files.episode_id
                 WHERE episode_files.media_file_id = playback_sessions.media_file_id
                 LIMIT 1
             )
         WHERE selected_item_id IS NULL
           AND (
               SELECT COUNT(*)
               FROM episode_files
               WHERE episode_files.media_file_id = playback_sessions.media_file_id
           ) = 1
           AND NOT EXISTS (
               SELECT 1
               FROM movie_files
               WHERE movie_files.media_file_id = playback_sessions.media_file_id
           )",
    )
    .execute(pool)
    .await
    .context("backfilling episode playback session item context")?;

    sqlx::query(
        "WITH session_candidates AS (
             SELECT
                 ps.id AS session_id,
                 ps.user_id AS user_id,
                 ps.media_file_id AS media_file_id,
                 'movie' AS item_type,
                 mf.movie_id AS item_id,
                 NULL AS series_id,
                 NULL AS season_id,
                 CASE
                     WHEN ps.logical_position_seconds > 0 THEN ps.logical_position_seconds
                     ELSE 0
                 END AS position_seconds,
                 COALESCE(ps.duration_seconds, m.runtime_seconds, 0) AS duration_seconds,
                 ps.created_at AS created_at,
                 ps.updated_at AS updated_at,
                 600 AS remaining_threshold_seconds
             FROM playback_sessions ps
             JOIN movie_files mf ON mf.media_file_id = ps.media_file_id
             JOIN movies m ON m.id = mf.movie_id
             WHERE (
                 SELECT COUNT(*)
                 FROM movie_files
                 WHERE movie_files.media_file_id = ps.media_file_id
             ) = 1
               AND NOT EXISTS (
                   SELECT 1
                   FROM episode_files
                   WHERE episode_files.media_file_id = ps.media_file_id
               )
             UNION ALL
             SELECT
                 ps.id AS session_id,
                 ps.user_id AS user_id,
                 ps.media_file_id AS media_file_id,
                 'episode' AS item_type,
                 ef.episode_id AS item_id,
                 e.series_id AS series_id,
                 e.season_id AS season_id,
                 CASE
                     WHEN ps.logical_position_seconds > 0 THEN ps.logical_position_seconds
                     ELSE 0
                 END AS position_seconds,
                 COALESCE(ps.duration_seconds, e.runtime_seconds, 0) AS duration_seconds,
                 ps.created_at AS created_at,
                 ps.updated_at AS updated_at,
                 180 AS remaining_threshold_seconds
             FROM playback_sessions ps
             JOIN episode_files ef ON ef.media_file_id = ps.media_file_id
             JOIN episodes e ON e.id = ef.episode_id
             WHERE (
                 SELECT COUNT(*)
                 FROM episode_files
                 WHERE episode_files.media_file_id = ps.media_file_id
             ) = 1
               AND NOT EXISTS (
                   SELECT 1
                   FROM movie_files
                   WHERE movie_files.media_file_id = ps.media_file_id
               )
         ),
         classified_sessions AS (
             SELECT
                 *,
                 CASE
                     WHEN duration_seconds > 0
                          AND (
                              position_seconds >= duration_seconds * 0.9
                              OR duration_seconds - position_seconds <= remaining_threshold_seconds
                          )
                     THEN 1
                     ELSE 0
                 END AS completed
             FROM session_candidates
         ),
         latest_sessions AS (
             SELECT
                 *,
                 ROW_NUMBER() OVER (
                     PARTITION BY user_id, item_type, item_id
                     ORDER BY updated_at DESC, created_at DESC, session_id DESC
                 ) AS row_number
             FROM classified_sessions
         ),
         session_rollups AS (
             SELECT
                 user_id,
                 item_type,
                 item_id,
                 SUM(completed) AS completed_count,
                 MAX(CASE WHEN completed = 1 THEN updated_at ELSE NULL END) AS last_watched_at
             FROM classified_sessions
             GROUP BY user_id, item_type, item_id
         )
         INSERT INTO user_media_state (
             user_id,
             item_type,
             item_id,
             media_file_id,
             series_id,
             season_id,
             resume_seconds,
             duration_seconds,
             watched,
             watched_at,
             play_count,
             last_played_at,
             last_session_id,
             state_source
         )
         SELECT
             latest.user_id,
             latest.item_type,
             latest.item_id,
             latest.media_file_id,
             latest.series_id,
             latest.season_id,
             CASE
                 WHEN rollup.completed_count > 0 THEN 0
                 WHEN latest.position_seconds > 0 THEN latest.position_seconds
                 ELSE 0
             END,
             CASE
                 WHEN latest.duration_seconds > 0 THEN latest.duration_seconds
                 ELSE NULL
             END,
             CASE
                 WHEN rollup.completed_count > 0 THEN TRUE
                 ELSE FALSE
             END,
             rollup.last_watched_at,
             rollup.completed_count,
             latest.updated_at,
             latest.session_id,
             'migration'
         FROM latest_sessions latest
         JOIN session_rollups rollup
           ON rollup.user_id = latest.user_id
          AND rollup.item_type = latest.item_type
          AND rollup.item_id = latest.item_id
         WHERE latest.row_number = 1
         ON CONFLICT(user_id, item_type, item_id) DO UPDATE SET
             media_file_id = excluded.media_file_id,
             series_id = excluded.series_id,
             season_id = excluded.season_id,
             resume_seconds = excluded.resume_seconds,
             duration_seconds = excluded.duration_seconds,
             watched = excluded.watched,
             watched_at = excluded.watched_at,
             play_count = excluded.play_count,
             last_played_at = excluded.last_played_at,
             last_session_id = excluded.last_session_id,
             state_source = excluded.state_source,
             updated_at = CURRENT_TIMESTAMP
         WHERE user_media_state.state_source = 'migration'",
    )
    .execute(pool)
    .await
    .context("backfilling user media state from playback sessions")?;

    Ok(())
}

async fn ensure_sqlite_file_ready(url: &str) -> Result<()> {
    let Some(path) = sqlite_file_path(url) else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .with_context(|| format!("creating sqlite directory {}", parent.display()))?;
    }
    fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await
        .with_context(|| format!("creating sqlite database file {}", path.display()))?;
    Ok(())
}

fn sqlite_file_path(url: &str) -> Option<std::path::PathBuf> {
    let lowered = url.to_ascii_lowercase();
    if lowered.starts_with("sqlite::memory") || lowered.starts_with("sqlite://:memory:") {
        return None;
    }
    let rest = url
        .strip_prefix("sqlite://")
        .or_else(|| url.strip_prefix("sqlite:"))?;
    let path_part = rest.split('?').next().unwrap_or(rest).trim();
    if path_part.is_empty() || path_part.starts_with(":memory:") {
        return None;
    }
    let path = Path::new(path_part);
    if path.is_absolute() {
        Some(path.to_path_buf())
    } else {
        Some(std::path::PathBuf::from(path_part))
    }
}

#[cfg(test)]
mod tests;
