pub mod models;

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

#[derive(Clone)]
pub struct Database {
    pub driver: DatabaseDriver,
    pub pool: AnyPool,
}

impl Database {
    pub async fn connect(config: &DatabaseConfig) -> Result<Self> {
        let driver = DatabaseDriver::from_url(&config.url)?;
        if matches!(driver, DatabaseDriver::Sqlite) {
            ensure_sqlite_parent_dir(&config.url).await?;
        }

        // Enable default Any drivers (Postgres + SQLite). This is a no-op if already installed.
        sqlx::any::install_default_drivers();

        let pool = AnyPoolOptions::new()
            .max_connections(config.max_connections)
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
        MIGRATOR
            .run(&self.pool)
            .await
            .context("database migrations failed")
    }
}

async fn ensure_sqlite_parent_dir(url: &str) -> Result<()> {
    let Some(path) = sqlite_file_path(url) else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .with_context(|| format!("creating sqlite directory {}", parent.display()))?;
    }
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
