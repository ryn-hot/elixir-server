use anyhow::Result;
use tempfile::tempdir;
use uuid::Uuid;

use crate::{
    config::DatabaseConfig,
    db::{Database, DatabaseDriver},
};

#[tokio::test]
async fn migrations_apply_and_basic_inserts() -> Result<()> {
    let config = DatabaseConfig {
        url: "sqlite::memory:?cache=shared".to_string(),
        max_connections: 1,
        connect_timeout_seconds: 5,
    };

    let database = Database::connect(&config).await?;
    assert_eq!(database.driver, DatabaseDriver::Sqlite);

    database.run_migrations().await?;

    let user_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES (?1, ?2, ?3)")
        .bind(user_id.to_string())
        .bind("test@example.com")
        .bind("hashed")
        .execute(&database.pool)
        .await?;

    let server_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO server_instances (id, user_id, device_name, lan_addresses) VALUES (?1, ?2, ?3, ?4)",
    )
    .bind(server_id.to_string())
    .bind(user_id.to_string())
    .bind("Test Device")
    .bind(r#"["127.0.0.1:1234"]"#)
    .execute(&database.pool)
    .await?;

    let source_config_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO source_configs (id, server_id, extension_id, config_json, enabled) VALUES (?1, ?2, ?3, ?4, ?5)",
    )
    .bind(source_config_id.to_string())
    .bind(server_id.to_string())
    .bind("elixir.localfolder")
    .bind(r#"{"root_path":"/media"}"#)
    .bind(true)
    .execute(&database.pool)
    .await?;

    let media_item_id = Uuid::new_v4();
    sqlx::query("INSERT INTO media_items (id, type, title, external_ids) VALUES (?1, ?2, ?3, ?4)")
        .bind(media_item_id.to_string())
        .bind("movie")
        .bind("Test Movie")
        .bind(r#"{"tmdb":"123"}"#)
        .execute(&database.pool)
        .await?;

    let media_file_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO media_files (id, media_item_id, source_config_id, path, size_bytes, scan_state) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
    )
    .bind(media_file_id.to_string())
    .bind(media_item_id.to_string())
    .bind(source_config_id.to_string())
    .bind("/media/test.mkv")
    .bind(1024_i64)
    .bind("ok")
    .execute(&database.pool)
    .await?;

    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM media_files")
        .fetch_one(&database.pool)
        .await?;
    assert_eq!(count, 1);

    Ok(())
}

#[tokio::test]
async fn sqlite_connect_creates_missing_file_on_disk() -> Result<()> {
    let temp = tempdir()?;
    let db_path = temp.path().join("fresh").join("elixir.db");
    let config = DatabaseConfig {
        url: format!("sqlite://{}", db_path.display()),
        max_connections: 1,
        connect_timeout_seconds: 5,
    };

    assert!(
        !db_path.exists(),
        "expected missing sqlite file before connect"
    );

    let database = Database::connect(&config).await?;
    assert_eq!(database.driver, DatabaseDriver::Sqlite);
    assert!(db_path.exists(), "expected sqlite file to be created");

    database.run_migrations().await?;
    Ok(())
}

#[tokio::test]
async fn migrations_create_provider_readiness_table() -> Result<()> {
    let config = DatabaseConfig {
        url: "sqlite::memory:?cache=shared".to_string(),
        max_connections: 1,
        connect_timeout_seconds: 5,
    };

    let database = Database::connect(&config).await?;
    database.run_migrations().await?;

    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM provider_readiness")
        .fetch_one(&database.pool)
        .await?;
    assert_eq!(count, 0);

    let applied_versions =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM _sqlx_migrations WHERE version = 24")
            .fetch_one(&database.pool)
            .await?;
    assert_eq!(applied_versions, 1);

    Ok(())
}

#[tokio::test]
async fn migrations_create_cloudstream_source_registry_tables() -> Result<()> {
    let config = DatabaseConfig {
        url: "sqlite::memory:?cache=shared".to_string(),
        max_connections: 1,
        connect_timeout_seconds: 5,
    };

    let database = Database::connect(&config).await?;
    database.run_migrations().await?;

    let applied_versions =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM _sqlx_migrations WHERE version = 41")
            .fetch_one(&database.pool)
            .await?;
    assert_eq!(applied_versions, 1);

    for table in [
        "extension_source_registries",
        "extension_source_modules",
        "extension_source_module_versions",
        "extension_source_health_events",
        "extension_source_replacement_recommendations",
        "extension_source_module_certifications",
        "extension_source_module_quarantines",
        "extension_source_certification_jobs",
    ] {
        let count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
        )
        .bind(table)
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(count, 1, "missing migration table {table}");
    }

    Ok(())
}
