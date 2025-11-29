pub mod models;

use std::time::Duration;

use anyhow::{Context, Result, bail};
use sqlx::{AnyPool, any::AnyPoolOptions, migrate::Migrator};

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

#[cfg(test)]
mod tests;
