use std::borrow::Cow;

use anyhow::Result;
use tempfile::tempdir;
use uuid::Uuid;

use crate::{
    config::DatabaseConfig,
    db::{Database, DatabaseDriver},
};

fn migrator_through(version: i64) -> sqlx::migrate::Migrator {
    sqlx::migrate::Migrator {
        migrations: Cow::Owned(
            super::MIGRATOR
                .iter()
                .filter(|migration| migration.version <= version)
                .cloned()
                .collect(),
        ),
        ignore_missing: false,
        locking: true,
    }
}

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

#[tokio::test]
async fn migrations_create_playback_hardware_readiness_tables() -> Result<()> {
    let config = DatabaseConfig {
        url: "sqlite::memory:?cache=shared".to_string(),
        max_connections: 1,
        connect_timeout_seconds: 5,
    };

    let database = Database::connect(&config).await?;
    database.run_migrations().await?;

    let applied_versions =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM _sqlx_migrations WHERE version = 46")
            .fetch_one(&database.pool)
            .await?;
    assert_eq!(applied_versions, 1);

    for table in [
        "playback_hardware_readiness",
        "playback_hardware_readiness_events",
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

#[tokio::test]
async fn migrations_create_playback_performance_envelope_table() -> Result<()> {
    let config = DatabaseConfig {
        url: "sqlite::memory:?cache=shared".to_string(),
        max_connections: 1,
        connect_timeout_seconds: 5,
    };

    let database = Database::connect(&config).await?;
    database.run_migrations().await?;

    let applied_versions =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM _sqlx_migrations WHERE version = 48")
            .fetch_one(&database.pool)
            .await?;
    assert_eq!(applied_versions, 1);

    let count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
    )
    .bind("playback_performance_envelopes")
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(count, 1, "missing playback_performance_envelopes");

    Ok(())
}

#[tokio::test]
async fn migrations_create_remote_playback_policy_session_columns() -> Result<()> {
    let config = DatabaseConfig {
        url: "sqlite::memory:?cache=shared".to_string(),
        max_connections: 1,
        connect_timeout_seconds: 5,
    };

    let database = Database::connect(&config).await?;
    database.run_migrations().await?;

    let applied_versions =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM _sqlx_migrations WHERE version = 49")
            .fetch_one(&database.pool)
            .await?;
    assert_eq!(applied_versions, 1);

    for column in ["token_expires_at", "share_id", "remote_policy_json"] {
        let count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM pragma_table_info('playback_sessions') WHERE name = ?",
        )
        .bind(column)
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(count, 1, "missing playback_sessions.{column}");
    }

    Ok(())
}

#[tokio::test]
async fn migrations_create_media_interaction_tables() -> Result<()> {
    let config = DatabaseConfig {
        url: "sqlite::memory:?cache=shared".to_string(),
        max_connections: 1,
        connect_timeout_seconds: 5,
    };

    let database = Database::connect(&config).await?;
    database.run_migrations().await?;

    let applied_versions =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM _sqlx_migrations WHERE version = 50")
            .fetch_one(&database.pool)
            .await?;
    assert_eq!(applied_versions, 1);

    for column in [
        "selected_item_type",
        "selected_item_id",
        "selected_series_id",
        "selected_season_id",
        "selected_episode_id",
        "playback_context_json",
        "last_progress_at",
    ] {
        let count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM pragma_table_info('playback_sessions') WHERE name = ?",
        )
        .bind(column)
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(count, 1, "missing playback_sessions.{column}");
    }

    for table in [
        "user_media_state",
        "playback_progress_events",
        "media_file_fingerprints",
        "media_segment_candidates",
        "media_segments",
        "media_segment_jobs",
        "media_segment_provider_cache",
        "media_segment_provider_certifications",
        "media_interaction_library_provider_settings",
        "media_segment_provider_rate_limits",
        "user_playback_preferences",
        "user_autoplay_sessions",
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

#[tokio::test]
async fn media_interaction_backfill_projects_movie_and_episode_state() -> Result<()> {
    let config = DatabaseConfig {
        url: "sqlite::memory:?cache=shared".to_string(),
        max_connections: 1,
        connect_timeout_seconds: 5,
    };

    let database = Database::connect(&config).await?;
    database.run_migrations().await?;

    let (user_id, server_id, source_config_id) = seed_backfill_owner(&database.pool).await?;

    let movie_id = Uuid::new_v4().to_string();
    let movie_file_id = seed_backfill_media_file(
        &database.pool,
        &source_config_id,
        "movie",
        "Backfill Movie",
        "/media/backfill-movie.mkv",
    )
    .await?;
    sqlx::query("INSERT INTO movies (id, title, runtime_seconds) VALUES (?, ?, ?)")
        .bind(&movie_id)
        .bind("Backfill Movie")
        .bind(7200_i64)
        .execute(&database.pool)
        .await?;
    sqlx::query("INSERT INTO movie_files (movie_id, media_file_id) VALUES (?, ?)")
        .bind(&movie_id)
        .bind(&movie_file_id)
        .execute(&database.pool)
        .await?;

    let old_movie_session = Uuid::new_v4().to_string();
    let latest_movie_session = Uuid::new_v4().to_string();
    seed_backfill_playback_session(
        &database.pool,
        &old_movie_session,
        &user_id,
        &server_id,
        &movie_file_id,
        300.0,
        7200,
        "2024-01-01 00:00:00",
    )
    .await?;
    seed_backfill_playback_session(
        &database.pool,
        &latest_movie_session,
        &user_id,
        &server_id,
        &movie_file_id,
        1200.0,
        7200,
        "2024-01-02 00:00:00",
    )
    .await?;

    let series_id = Uuid::new_v4().to_string();
    let season_id = Uuid::new_v4().to_string();
    let episode_id = Uuid::new_v4().to_string();
    let episode_file_id = seed_backfill_media_file(
        &database.pool,
        &source_config_id,
        "episode",
        "Backfill Episode",
        "/media/backfill-episode.mkv",
    )
    .await?;
    sqlx::query("INSERT INTO series (id, title, library_type) VALUES (?, ?, ?)")
        .bind(&series_id)
        .bind("Backfill Series")
        .bind("tv")
        .execute(&database.pool)
        .await?;
    sqlx::query("INSERT INTO seasons (id, series_id, season_number, title) VALUES (?, ?, ?, ?)")
        .bind(&season_id)
        .bind(&series_id)
        .bind(1_i64)
        .bind("Season 1")
        .execute(&database.pool)
        .await?;
    sqlx::query(
        "INSERT INTO episodes
            (id, series_id, season_id, season_number, episode_number, title, runtime_seconds)
         VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&episode_id)
    .bind(&series_id)
    .bind(&season_id)
    .bind(1_i64)
    .bind(1_i64)
    .bind("Episode 1")
    .bind(1800_i64)
    .execute(&database.pool)
    .await?;
    sqlx::query("INSERT INTO episode_files (episode_id, media_file_id) VALUES (?, ?)")
        .bind(&episode_id)
        .bind(&episode_file_id)
        .execute(&database.pool)
        .await?;

    let episode_session = Uuid::new_v4().to_string();
    seed_backfill_playback_session(
        &database.pool,
        &episode_session,
        &user_id,
        &server_id,
        &episode_file_id,
        1700.0,
        1800,
        "2024-02-01 00:00:00",
    )
    .await?;

    super::backfill_media_interaction_state(&database.pool).await?;

    let movie_state: (f64, i64, i64, String, String) = sqlx::query_as(
        "SELECT resume_seconds,
                CASE WHEN watched THEN 1 ELSE 0 END,
                play_count,
                last_session_id,
                state_source
         FROM user_media_state
         WHERE user_id = ? AND item_type = 'movie' AND item_id = ?",
    )
    .bind(&user_id)
    .bind(&movie_id)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(
        movie_state,
        (1200.0, 0, 0, latest_movie_session, "migration".to_string())
    );

    let episode_state: (f64, i64, i64, String, String, String) = sqlx::query_as(
        "SELECT resume_seconds,
                CASE WHEN watched THEN 1 ELSE 0 END,
                play_count,
                series_id,
                season_id,
                last_session_id
         FROM user_media_state
         WHERE user_id = ? AND item_type = 'episode' AND item_id = ?",
    )
    .bind(&user_id)
    .bind(&episode_id)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(
        episode_state,
        (
            0.0,
            1,
            1,
            series_id.clone(),
            season_id.clone(),
            episode_session
        )
    );

    let selected_episode_context: (String, String, String, String) = sqlx::query_as(
        "SELECT selected_item_type,
                selected_item_id,
                selected_series_id,
                selected_season_id
         FROM playback_sessions
         WHERE id = ?",
    )
    .bind(&episode_state.5)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(
        selected_episode_context,
        ("episode".to_string(), episode_id, series_id, season_id)
    );

    Ok(())
}

#[tokio::test]
async fn media_interaction_backfill_is_conservative_and_idempotent() -> Result<()> {
    let config = DatabaseConfig {
        url: "sqlite::memory:?cache=shared".to_string(),
        max_connections: 1,
        connect_timeout_seconds: 5,
    };

    let database = Database::connect(&config).await?;
    database.run_migrations().await?;

    let (user_id, server_id, source_config_id) = seed_backfill_owner(&database.pool).await?;
    let ambiguous_file_id = seed_backfill_media_file(
        &database.pool,
        &source_config_id,
        "movie",
        "Ambiguous File",
        "/media/ambiguous.mkv",
    )
    .await?;
    let unlinked_file_id = seed_backfill_media_file(
        &database.pool,
        &source_config_id,
        "movie",
        "Unlinked File",
        "/media/unlinked.mkv",
    )
    .await?;

    let ambiguous_movie_id = Uuid::new_v4().to_string();
    let ambiguous_series_id = Uuid::new_v4().to_string();
    let ambiguous_season_id = Uuid::new_v4().to_string();
    let ambiguous_episode_id = Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO movies (id, title, runtime_seconds) VALUES (?, ?, ?)")
        .bind(&ambiguous_movie_id)
        .bind("Ambiguous Movie")
        .bind(3600_i64)
        .execute(&database.pool)
        .await?;
    sqlx::query("INSERT INTO movie_files (movie_id, media_file_id) VALUES (?, ?)")
        .bind(&ambiguous_movie_id)
        .bind(&ambiguous_file_id)
        .execute(&database.pool)
        .await?;
    sqlx::query("INSERT INTO series (id, title, library_type) VALUES (?, ?, ?)")
        .bind(&ambiguous_series_id)
        .bind("Ambiguous Series")
        .bind("tv")
        .execute(&database.pool)
        .await?;
    sqlx::query("INSERT INTO seasons (id, series_id, season_number, title) VALUES (?, ?, ?, ?)")
        .bind(&ambiguous_season_id)
        .bind(&ambiguous_series_id)
        .bind(1_i64)
        .bind("Season 1")
        .execute(&database.pool)
        .await?;
    sqlx::query(
        "INSERT INTO episodes
            (id, series_id, season_id, season_number, episode_number, title, runtime_seconds)
         VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&ambiguous_episode_id)
    .bind(&ambiguous_series_id)
    .bind(&ambiguous_season_id)
    .bind(1_i64)
    .bind(1_i64)
    .bind("Episode 1")
    .bind(1800_i64)
    .execute(&database.pool)
    .await?;
    sqlx::query("INSERT INTO episode_files (episode_id, media_file_id) VALUES (?, ?)")
        .bind(&ambiguous_episode_id)
        .bind(&ambiguous_file_id)
        .execute(&database.pool)
        .await?;

    let ambiguous_session = Uuid::new_v4().to_string();
    let unlinked_session = Uuid::new_v4().to_string();
    seed_backfill_playback_session(
        &database.pool,
        &ambiguous_session,
        &user_id,
        &server_id,
        &ambiguous_file_id,
        3500.0,
        3600,
        "2024-03-01 00:00:00",
    )
    .await?;
    seed_backfill_playback_session(
        &database.pool,
        &unlinked_session,
        &user_id,
        &server_id,
        &unlinked_file_id,
        3500.0,
        3600,
        "2024-03-01 00:01:00",
    )
    .await?;

    super::backfill_media_interaction_state(&database.pool).await?;
    super::backfill_media_interaction_state(&database.pool).await?;

    let conservative_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM user_media_state
         WHERE user_id = ?",
    )
    .bind(&user_id)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(conservative_count, 0);

    for session_id in [&ambiguous_session, &unlinked_session] {
        let selected_item_id: String = sqlx::query_scalar(
            "SELECT COALESCE(selected_item_id, '') FROM playback_sessions WHERE id = ?",
        )
        .bind(session_id)
        .fetch_one(&database.pool)
        .await?;
        assert!(selected_item_id.is_empty());
    }

    let movie_id = Uuid::new_v4().to_string();
    let movie_file_id = seed_backfill_media_file(
        &database.pool,
        &source_config_id,
        "movie",
        "Manual Preserve Movie",
        "/media/manual-preserve.mkv",
    )
    .await?;
    sqlx::query("INSERT INTO movies (id, title, runtime_seconds) VALUES (?, ?, ?)")
        .bind(&movie_id)
        .bind("Manual Preserve Movie")
        .bind(7200_i64)
        .execute(&database.pool)
        .await?;
    sqlx::query("INSERT INTO movie_files (movie_id, media_file_id) VALUES (?, ?)")
        .bind(&movie_id)
        .bind(&movie_file_id)
        .execute(&database.pool)
        .await?;

    let movie_session = Uuid::new_v4().to_string();
    seed_backfill_playback_session(
        &database.pool,
        &movie_session,
        &user_id,
        &server_id,
        &movie_file_id,
        1200.0,
        7200,
        "2024-04-01 00:00:00",
    )
    .await?;

    super::backfill_media_interaction_state(&database.pool).await?;
    super::backfill_media_interaction_state(&database.pool).await?;

    let movie_state_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM user_media_state
         WHERE user_id = ? AND item_type = 'movie' AND item_id = ?",
    )
    .bind(&user_id)
    .bind(&movie_id)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(movie_state_count, 1);

    sqlx::query(
        "UPDATE user_media_state
         SET resume_seconds = 42,
             watched = FALSE,
             state_source = 'manual'
         WHERE user_id = ? AND item_type = 'movie' AND item_id = ?",
    )
    .bind(&user_id)
    .bind(&movie_id)
    .execute(&database.pool)
    .await?;

    let later_movie_session = Uuid::new_v4().to_string();
    seed_backfill_playback_session(
        &database.pool,
        &later_movie_session,
        &user_id,
        &server_id,
        &movie_file_id,
        7100.0,
        7200,
        "2024-04-02 00:00:00",
    )
    .await?;
    super::backfill_media_interaction_state(&database.pool).await?;

    let manual_state: (f64, i64, String) = sqlx::query_as(
        "SELECT resume_seconds,
                CASE WHEN watched THEN 1 ELSE 0 END,
                state_source
         FROM user_media_state
         WHERE user_id = ? AND item_type = 'movie' AND item_id = ?",
    )
    .bind(&user_id)
    .bind(&movie_id)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(manual_state, (42.0, 0, "manual".to_string()));

    Ok(())
}

#[tokio::test]
async fn copied_pre_midm_sqlite_upgrade_projects_media_interaction_state() -> Result<()> {
    let temp = tempdir()?;
    let db_path = temp.path().join("copied-pre-midm.db");
    let config = DatabaseConfig {
        url: format!("sqlite://{}", db_path.display()),
        max_connections: 1,
        connect_timeout_seconds: 5,
    };

    let pre_upgrade = Database::connect(&config).await?;
    assert_eq!(pre_upgrade.driver, DatabaseDriver::Sqlite);
    migrator_through(49).run(&pre_upgrade.pool).await?;
    let version_50_before: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations WHERE version = 50")
            .fetch_one(&pre_upgrade.pool)
            .await?;
    assert_eq!(version_50_before, 0);
    let selected_item_column_before: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM pragma_table_info('playback_sessions')
         WHERE name = 'selected_item_id'",
    )
    .fetch_one(&pre_upgrade.pool)
    .await?;
    assert_eq!(selected_item_column_before, 0);

    let (user_id, server_id, source_config_id) = seed_backfill_owner(&pre_upgrade.pool).await?;

    let movie_id = Uuid::new_v4().to_string();
    let movie_file_id = seed_backfill_media_file(
        &pre_upgrade.pool,
        &source_config_id,
        "movie",
        "Copied Upgrade Movie",
        "/media/copied-upgrade-movie.mkv",
    )
    .await?;
    sqlx::query("INSERT INTO movies (id, title, runtime_seconds) VALUES (?, ?, ?)")
        .bind(&movie_id)
        .bind("Copied Upgrade Movie")
        .bind(3600_i64)
        .execute(&pre_upgrade.pool)
        .await?;
    sqlx::query("INSERT INTO movie_files (movie_id, media_file_id) VALUES (?, ?)")
        .bind(&movie_id)
        .bind(&movie_file_id)
        .execute(&pre_upgrade.pool)
        .await?;
    let older_movie_session = Uuid::new_v4().to_string();
    let latest_movie_session = Uuid::new_v4().to_string();
    seed_backfill_playback_session(
        &pre_upgrade.pool,
        &older_movie_session,
        &user_id,
        &server_id,
        &movie_file_id,
        300.0,
        3600,
        "2024-05-01 00:00:00",
    )
    .await?;
    seed_backfill_playback_session(
        &pre_upgrade.pool,
        &latest_movie_session,
        &user_id,
        &server_id,
        &movie_file_id,
        1200.0,
        3600,
        "2024-05-02 00:00:00",
    )
    .await?;

    let series_id = Uuid::new_v4().to_string();
    let season_id = Uuid::new_v4().to_string();
    let episode_id = Uuid::new_v4().to_string();
    let episode_file_id = seed_backfill_media_file(
        &pre_upgrade.pool,
        &source_config_id,
        "episode",
        "Copied Upgrade Episode",
        "/media/copied-upgrade-episode.mkv",
    )
    .await?;
    sqlx::query("INSERT INTO series (id, title, library_type) VALUES (?, ?, ?)")
        .bind(&series_id)
        .bind("Copied Upgrade Show")
        .bind("tv")
        .execute(&pre_upgrade.pool)
        .await?;
    sqlx::query("INSERT INTO seasons (id, series_id, season_number, title) VALUES (?, ?, ?, ?)")
        .bind(&season_id)
        .bind(&series_id)
        .bind(1_i64)
        .bind("Season 1")
        .execute(&pre_upgrade.pool)
        .await?;
    sqlx::query(
        "INSERT INTO episodes
            (id, series_id, season_id, season_number, episode_number, title, runtime_seconds)
         VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&episode_id)
    .bind(&series_id)
    .bind(&season_id)
    .bind(1_i64)
    .bind(1_i64)
    .bind("Pilot")
    .bind(1800_i64)
    .execute(&pre_upgrade.pool)
    .await?;
    sqlx::query("INSERT INTO episode_files (episode_id, media_file_id) VALUES (?, ?)")
        .bind(&episode_id)
        .bind(&episode_file_id)
        .execute(&pre_upgrade.pool)
        .await?;
    let episode_session = Uuid::new_v4().to_string();
    seed_backfill_playback_session(
        &pre_upgrade.pool,
        &episode_session,
        &user_id,
        &server_id,
        &episode_file_id,
        1700.0,
        1800,
        "2024-05-03 00:00:00",
    )
    .await?;

    let ambiguous_file_id = seed_backfill_media_file(
        &pre_upgrade.pool,
        &source_config_id,
        "movie",
        "Copied Ambiguous",
        "/media/copied-ambiguous.mkv",
    )
    .await?;
    let ambiguous_movie_id = Uuid::new_v4().to_string();
    let ambiguous_episode_id = Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO movies (id, title, runtime_seconds) VALUES (?, ?, ?)")
        .bind(&ambiguous_movie_id)
        .bind("Copied Ambiguous Movie")
        .bind(3600_i64)
        .execute(&pre_upgrade.pool)
        .await?;
    sqlx::query("INSERT INTO movie_files (movie_id, media_file_id) VALUES (?, ?)")
        .bind(&ambiguous_movie_id)
        .bind(&ambiguous_file_id)
        .execute(&pre_upgrade.pool)
        .await?;
    sqlx::query(
        "INSERT INTO episodes
            (id, series_id, season_id, season_number, episode_number, title, runtime_seconds)
         VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&ambiguous_episode_id)
    .bind(&series_id)
    .bind(&season_id)
    .bind(1_i64)
    .bind(2_i64)
    .bind("Ambiguous Episode")
    .bind(1800_i64)
    .execute(&pre_upgrade.pool)
    .await?;
    sqlx::query("INSERT INTO episode_files (episode_id, media_file_id) VALUES (?, ?)")
        .bind(&ambiguous_episode_id)
        .bind(&ambiguous_file_id)
        .execute(&pre_upgrade.pool)
        .await?;
    let ambiguous_session = Uuid::new_v4().to_string();
    seed_backfill_playback_session(
        &pre_upgrade.pool,
        &ambiguous_session,
        &user_id,
        &server_id,
        &ambiguous_file_id,
        900.0,
        1800,
        "2024-05-04 00:00:00",
    )
    .await?;

    pre_upgrade.pool.close().await;

    let copied = Database::connect(&config).await?;
    copied.run_migrations().await?;
    copied.run_migrations().await?;

    let version_50_after: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations WHERE version = 50")
            .fetch_one(&copied.pool)
            .await?;
    assert_eq!(version_50_after, 1);

    let state_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM user_media_state
         WHERE user_id = ?",
    )
    .bind(&user_id)
    .fetch_one(&copied.pool)
    .await?;
    assert_eq!(
        state_count, 2,
        "copied pre-MIDM DB should backfill only unambiguous movie and episode rows"
    );

    let movie_state: (f64, i64, i64, String, String) = sqlx::query_as(
        "SELECT resume_seconds,
                CASE WHEN watched THEN 1 ELSE 0 END,
                play_count,
                last_session_id,
                state_source
         FROM user_media_state
         WHERE user_id = ? AND item_type = 'movie' AND item_id = ?",
    )
    .bind(&user_id)
    .bind(&movie_id)
    .fetch_one(&copied.pool)
    .await?;
    assert_eq!(
        movie_state,
        (
            1200.0,
            0,
            0,
            latest_movie_session.clone(),
            "migration".to_string()
        )
    );

    let episode_state: (f64, i64, i64, String, String, String) = sqlx::query_as(
        "SELECT resume_seconds,
                CASE WHEN watched THEN 1 ELSE 0 END,
                play_count,
                series_id,
                season_id,
                last_session_id
         FROM user_media_state
         WHERE user_id = ? AND item_type = 'episode' AND item_id = ?",
    )
    .bind(&user_id)
    .bind(&episode_id)
    .fetch_one(&copied.pool)
    .await?;
    assert_eq!(
        episode_state,
        (
            0.0,
            1,
            1,
            series_id.clone(),
            season_id.clone(),
            episode_session
        )
    );

    let ambiguous_selected_item_id: String = sqlx::query_scalar(
        "SELECT COALESCE(selected_item_id, '') FROM playback_sessions WHERE id = ?",
    )
    .bind(&ambiguous_session)
    .fetch_one(&copied.pool)
    .await?;
    assert!(
        ambiguous_selected_item_id.is_empty(),
        "ambiguous copied playback history must remain session-only"
    );

    let movie_selected_item_id: String =
        sqlx::query_scalar("SELECT selected_item_id FROM playback_sessions WHERE id = ?")
            .bind(&latest_movie_session)
            .fetch_one(&copied.pool)
            .await?;
    assert_eq!(movie_selected_item_id, movie_id);

    let episode_selected_context: (String, String, String, String) = sqlx::query_as(
        "SELECT selected_item_type,
                selected_item_id,
                selected_series_id,
                selected_season_id
         FROM playback_sessions
         WHERE id = ?",
    )
    .bind(&episode_state.5)
    .fetch_one(&copied.pool)
    .await?;
    assert_eq!(
        episode_selected_context,
        ("episode".to_string(), episode_id, series_id, season_id)
    );

    let no_extra_rows_after_rerun: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM user_media_state
         WHERE user_id = ?",
    )
    .bind(&user_id)
    .fetch_one(&copied.pool)
    .await?;
    assert_eq!(no_extra_rows_after_rerun, 2);

    Ok(())
}

async fn seed_backfill_owner(pool: &sqlx::AnyPool) -> Result<(String, String, String)> {
    let user_id = Uuid::new_v4().to_string();
    let server_id = Uuid::new_v4().to_string();
    let source_config_id = Uuid::new_v4().to_string();

    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES (?, ?, ?)")
        .bind(&user_id)
        .bind(format!("{user_id}@example.com"))
        .bind("hashed")
        .execute(pool)
        .await?;
    sqlx::query(
        "INSERT INTO server_instances (id, user_id, device_name, lan_addresses)
         VALUES (?, ?, ?, ?)",
    )
    .bind(&server_id)
    .bind(&user_id)
    .bind("Backfill Test")
    .bind(r#"["127.0.0.1:1234"]"#)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO source_configs (id, server_id, extension_id, config_json, enabled)
         VALUES (?, ?, ?, ?, ?)",
    )
    .bind(&source_config_id)
    .bind(&server_id)
    .bind("elixir.localfolder")
    .bind(r#"{"root_path":"/media"}"#)
    .bind(true)
    .execute(pool)
    .await?;

    Ok((user_id, server_id, source_config_id))
}
async fn seed_backfill_media_file(
    pool: &sqlx::AnyPool,
    source_config_id: &str,
    item_type: &str,
    title: &str,
    path: &str,
) -> Result<String> {
    let media_item_id = Uuid::new_v4().to_string();
    let media_file_id = Uuid::new_v4().to_string();

    sqlx::query("INSERT INTO media_items (id, type, title, external_ids) VALUES (?, ?, ?, ?)")
        .bind(&media_item_id)
        .bind(item_type)
        .bind(title)
        .bind("{}")
        .execute(pool)
        .await?;
    sqlx::query(
        "INSERT INTO media_files
            (id, media_item_id, source_config_id, path, size_bytes, scan_state)
         VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(&media_file_id)
    .bind(&media_item_id)
    .bind(source_config_id)
    .bind(path)
    .bind(1024_i64)
    .bind("ok")
    .execute(pool)
    .await?;

    Ok(media_file_id)
}

async fn seed_backfill_playback_session(
    pool: &sqlx::AnyPool,
    session_id: &str,
    user_id: &str,
    server_id: &str,
    media_file_id: &str,
    position_seconds: f64,
    duration_seconds: i64,
    updated_at: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO playback_sessions
            (id, user_id, server_id, media_file_id, mode, state, network_type,
             logical_position_seconds, duration_seconds, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(session_id)
    .bind(user_id)
    .bind(server_id)
    .bind(media_file_id)
    .bind("direct_stream")
    .bind("ended")
    .bind("lan")
    .bind(position_seconds)
    .bind(duration_seconds)
    .bind(updated_at)
    .bind(updated_at)
    .execute(pool)
    .await?;

    Ok(())
}
