//! Actual application connection and bundled SQLite/Any compatibility.
use super::Database;
use crate::config::DatabaseConfig;
use anyhow::Result;
use sqlx::Row;

#[tokio::test]
async fn patched_sqlite_keeps_boolean_null_integer_and_reopen_semantics() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("sqlite-security.sqlite");
    let configuration = DatabaseConfig {
        url: format!("sqlite://{}", path.display()),
        max_connections: 1,
        ..DatabaseConfig::default()
    };
    let database = Database::connect(&configuration).await?;
    let version: String = sqlx::query_scalar("SELECT sqlite_version()")
        .fetch_one(&database.pool)
        .await?;
    let version = semver::Version::parse(&version)?;
    assert!(
        version >= semver::Version::new(3, 54, 0),
        "patched native SQLite must actually be linked"
    );
    sqlx::query("CREATE TABLE native_types(id INTEGER PRIMARY KEY, flag BOOLEAN, wide BIGINT NOT NULL, bytes BLOB NOT NULL, text_value TEXT NOT NULL)")
        .execute(&database.pool).await?;
    for (id, flag, wide) in [
        (1_i64, Some(false), i64::MIN),
        (2, Some(true), i64::MAX),
        (3, None, 9_007_199_254_740_991),
    ] {
        sqlx::query("INSERT INTO native_types VALUES($1,$2,$3,$4,$5)")
            .bind(id)
            .bind(flag)
            .bind(wide)
            .bind(vec![0_u8, 1, 255])
            .bind("synthetic-日本語")
            .execute(&database.pool)
            .await?;
    }
    database.pool.close().await;
    let database = Database::connect(&configuration).await?;
    let rows = sqlx::query("SELECT * FROM native_types ORDER BY id")
        .fetch_all(&database.pool)
        .await?;
    assert_eq!(rows.len(), 3);
    for (row, (flag, wide)) in rows.iter().zip([
        (Some(false), i64::MIN),
        (Some(true), i64::MAX),
        (None, 9_007_199_254_740_991),
    ]) {
        assert_eq!(row.try_get::<Option<bool>, _>("flag")?, flag);
        assert_eq!(row.try_get::<i64, _>("wide")?, wide);
        assert_eq!(row.try_get::<Vec<u8>, _>("bytes")?, [0, 1, 255]);
        assert_eq!(row.try_get::<String, _>("text_value")?, "synthetic-日本語");
    }
    assert_eq!(
        sqlx::query_scalar::<_, String>("PRAGMA integrity_check")
            .fetch_one(&database.pool)
            .await?,
        "ok"
    );
    database.pool.close().await;
    Ok(())
}
