use anyhow::Result;
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
