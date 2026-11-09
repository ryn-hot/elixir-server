//! Durability is checked on the actual connection used to commit authority.
use crate::db::{Database, DatabaseDriver};
use anyhow::{Result, bail};
use sqlx::{Any, Row, pool::PoolConnection};

pub(super) async fn connection(database: &Database) -> Result<PoolConnection<Any>> {
    let mut connection = database.pool.acquire().await?;
    match database.driver {
        DatabaseDriver::Sqlite => {
            sqlx::query("PRAGMA foreign_keys=ON")
                .execute(&mut *connection)
                .await?;
            sqlx::query("PRAGMA main.synchronous=EXTRA")
                .execute(&mut *connection)
                .await?;
            sqlx::query("PRAGMA fullfsync=ON")
                .execute(&mut *connection)
                .await?;
            sqlx::query("PRAGMA checkpoint_fullfsync=ON")
                .execute(&mut *connection)
                .await?;
            let synchronous: i64 = sqlx::query("PRAGMA main.synchronous")
                .fetch_one(&mut *connection)
                .await?
                .try_get(0)?;
            let journal: String = sqlx::query("PRAGMA main.journal_mode")
                .fetch_one(&mut *connection)
                .await?
                .try_get(0)?;
            let foreign_keys: i64 = sqlx::query("PRAGMA foreign_keys")
                .fetch_one(&mut *connection)
                .await?
                .try_get(0)?;
            if synchronous != 3
                || !matches!(journal.as_str(), "delete" | "truncate" | "persist" | "wal")
            {
                bail!("marketplace authority requires durable SQLite storage");
            }
            if foreign_keys != 1 {
                bail!("marketplace authority requires foreign key enforcement");
            }
        }
        DatabaseDriver::Postgres => {
            let row = sqlx::query("SELECT current_setting('fsync') AS fsync, current_setting('full_page_writes') AS full_page_writes")
                .fetch_one(&mut *connection).await?;
            if row.try_get::<String, _>("fsync")? != "on"
                || row.try_get::<String, _>("full_page_writes")? != "on"
            {
                bail!("marketplace authority requires durable PostgreSQL storage");
            }
        }
    }
    Ok(connection)
}
