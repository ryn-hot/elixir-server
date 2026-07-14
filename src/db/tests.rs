use std::{borrow::Cow, time::Duration};

use anyhow::Result;
use chrono::Utc;
use sqlx::Row;
use tempfile::tempdir;
use uuid::Uuid;

use crate::{
    acquisition::{
        imports::{
            AcquisitionImportFileLinkState, AcquisitionImportRunState,
            NewAcquisitionImportFileLink, NewAcquisitionImportRun, create_or_get_import_run,
            get_import_run_by_release_job, list_import_file_links,
            list_import_file_links_by_release, list_import_runs_by_release,
            upsert_import_file_link,
        },
        release_resolution::{
            models::{
                AcquisitionReleaseState, NewAcquisitionRelease, NewAcquisitionReleaseFile,
                NewAcquisitionReleaseJob, ReleaseConfidence, ReleaseJobState, ReleaseKind,
                ReleaseResolverKind,
            },
            store::{
                ReleaseListFilter, get_release_by_download_id, list_active_releases_by_route,
                list_releases, update_release_file_selection, upsert_release, upsert_release_file,
                upsert_release_job,
            },
        },
    },
    auth::{
        AuthService,
        sessions::{AuthSessionError, LoginContext},
    },
    config::{AuthConfig, DatabaseConfig},
    db::{
        Database, DatabaseDriver,
        models::{ExtensionKind, ExtensionTrustLevel, MediaType, SecretScope},
    },
    extensions::store::{
        ExtensionStore, NewDesiredBlueprint, NewExtension, NewExtensionInstance,
        NewExtensionSourceModule, NewExtensionSourceRegistry, NewSecret,
    },
    playback::plan::{
        PlaybackPerformanceConfidence, PlaybackPerformanceDecision, PlaybackPerformanceEnvelope,
        PlaybackSupportDecision,
    },
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

fn postgres_migrator_through(version: i64) -> Result<sqlx::migrate::Migrator> {
    let migrator = super::postgres_migrations::migrator()?;
    Ok(sqlx::migrate::Migrator {
        migrations: Cow::Owned(
            migrator
                .iter()
                .filter(|migration| migration.version <= version)
                .cloned()
                .collect(),
        ),
        ignore_missing: migrator.ignore_missing,
        locking: migrator.locking,
    })
}

#[test]
fn numbered_bind_lists_are_postgres_and_sqlite_compatible() {
    assert_eq!(super::numbered_bind_list(1, 0), "");
    assert_eq!(super::numbered_bind_list(1, 3), "$1, $2, $3");
    assert_eq!(super::numbered_bind_list(4, 2), "$4, $5");
}

#[tokio::test]
async fn s10_live_streaming_sqlite_migration_is_additive_and_enforces_guards() -> Result<()> {
    let database = Database::connect(&DatabaseConfig {
        url: format!(
            "sqlite:file:s10-live-migration-{}?mode=memory&cache=shared",
            Uuid::new_v4()
        ),
        max_connections: 2,
        connect_timeout_seconds: 5,
    })
    .await?;
    migrator_through(55).run(&database.pool).await?;
    let playback_columns_before: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT name, type, pk
         FROM pragma_table_info('playback_sessions')
         ORDER BY cid",
    )
    .fetch_all(&database.pool)
    .await?;

    migrator_through(56).run(&database.pool).await?;
    let playback_columns_after: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT name, type, pk
         FROM pragma_table_info('playback_sessions')
         ORDER BY cid",
    )
    .fetch_all(&database.pool)
    .await?;
    assert_eq!(playback_columns_after, playback_columns_before);
    let live_table_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM sqlite_master
         WHERE type = 'table' AND name IN (
             'live_provider_cache',
             'live_provider_grants',
             'live_provider_admin_state',
             'live_provider_destination_rules',
             'live_admin_audit_events',
             'live_admin_audit_chain_heads',
             'live_key_rotation_state',
             'live_playback_sessions',
             'live_session_idempotency',
             'live_control_server_leases',
             'live_egress_bindings'
         )",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(live_table_count, 11);
    let trigger_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master
         WHERE type = 'trigger' AND name LIKE 'trg_live_%'",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(trigger_count, 6);
    let lease: (String, i64, Option<String>) = sqlx::query_as(
        "SELECT lease_name, fencing_token, CAST(owner_instance_id AS TEXT)
         FROM live_control_server_leases",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(lease, ("live-control-v1".to_string(), 0, None));
    assert!(
        sqlx::query("DELETE FROM live_control_server_leases")
            .execute(&database.pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query(
            "UPDATE live_control_server_leases SET fencing_token = 2
             WHERE lease_name = 'live-control-v1'",
        )
        .execute(&database.pool)
        .await
        .is_err()
    );

    let user_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
        .bind(user_id.to_string())
        .bind("s10-migration@example.test")
        .bind("hashed")
        .execute(&database.pool)
        .await?;
    let owner = crate::auth::home_profiles::HomeProfileRepository::new(&database.pool)
        .ensure_owner_home(user_id)
        .await?;
    let extension_id = "elixir.test.s10-live-provider";
    let instance_id = Uuid::new_v4();
    let provider_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO extensions
            (extension_id, name, version, kind, trust_level, manifest_json, enabled)
         VALUES ($1, 'S10 Live Provider', '1.0.0', 'module', 'verified', '{}', TRUE)",
    )
    .bind(extension_id)
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "INSERT INTO extension_instances
            (instance_id, extension_id, instance_name, enabled)
         VALUES ($1, $2, 'default', TRUE)",
    )
    .bind(instance_id.to_string())
    .bind(extension_id)
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "INSERT INTO providers
            (provider_id, instance_id, capability, slot_id, cardinality, health_state)
         VALUES ($1, $2, 'live.catalog_provider/v1', 'default', 'one', 'healthy')",
    )
    .bind(provider_id.to_string())
    .bind(instance_id.to_string())
    .execute(&database.pool)
    .await?;
    let rule_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO live_provider_destination_rules (
            id, home_id, provider_id, scheme, normalized_host, port, exact_path,
            network_scope, allow_fetch, created_by_user_id, created_by_actor_snapshot
         ) VALUES ($1, $2, $3, 'https', 'origin.example.test', 443, '/live.m3u8',
                   'public', TRUE, $4, '{}')",
    )
    .bind(rule_id.to_string())
    .bind(owner.home.id.to_string())
    .bind(provider_id.to_string())
    .bind(user_id.to_string())
    .execute(&database.pool)
    .await?;
    assert!(
        sqlx::query(
            "UPDATE live_provider_destination_rules
             SET allow_credentials = TRUE
             WHERE id = $1",
        )
        .bind(rule_id.to_string())
        .execute(&database.pool)
        .await
        .is_err()
    );
    sqlx::query(
        "UPDATE live_provider_destination_rules
         SET allow_credentials = TRUE, revision = revision + 1
         WHERE id = $1",
    )
    .bind(rule_id.to_string())
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "UPDATE live_provider_destination_rules
         SET exact_path = '/replacement.m3u8', revision = revision + 1
         WHERE id = $1",
    )
    .bind(rule_id.to_string())
    .execute(&database.pool)
    .await?;
    assert!(
        sqlx::query(
            "INSERT INTO live_provider_destination_rules (
                id, home_id, provider_id, scheme, normalized_host, port, exact_path,
                network_scope, allow_fetch, allow_credentials, created_by_actor_snapshot
             ) VALUES ($1, $2, $3, 'rtmp', 'origin.example.test', 1935, '/live',
                       'public', TRUE, TRUE, '{}')",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(owner.home.id.to_string())
        .bind(provider_id.to_string())
        .execute(&database.pool)
        .await
        .is_err()
    );

    let audit_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO live_admin_audit_events (
            id, home_id, action, target_type, target_id, actor_user_id,
            actor_snapshot_json, after_json, audit_key_id, record_hash, retain_until
         ) VALUES ($1, $2, 'rule_created', 'destination_rule', $3, $4,
                   '{}', '{}', 'audit-1', $5, '2099-01-01T00:00:00Z')",
    )
    .bind(audit_id.to_string())
    .bind(owner.home.id.to_string())
    .bind(rule_id.to_string())
    .bind(user_id.to_string())
    .bind("a".repeat(64))
    .execute(&database.pool)
    .await?;
    assert!(
        sqlx::query("UPDATE live_admin_audit_events SET action = 'changed' WHERE id = $1")
            .bind(audit_id.to_string())
            .execute(&database.pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM live_admin_audit_events WHERE id = $1")
            .bind(audit_id.to_string())
            .execute(&database.pool)
            .await
            .is_err()
    );
    let migration_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations WHERE version = 56")
            .fetch_one(&database.pool)
            .await?;
    assert_eq!(migration_count, 1);
    Ok(())
}

#[tokio::test]
async fn s10_live_streaming_postgres_migration_and_lease_when_configured() -> Result<()> {
    let Ok(url) = std::env::var("ELIXIR_TEST_POSTGRES_EMPTY_DATABASE_URL") else {
        return Ok(());
    };
    let config = DatabaseConfig {
        url,
        max_connections: 4,
        connect_timeout_seconds: 5,
    };
    let database = Database::connect(&config).await?;
    assert_eq!(database.driver, DatabaseDriver::Postgres);
    postgres_migrator_through(55)?.run(&database.pool).await?;
    postgres_migrator_through(56)?.run(&database.pool).await?;
    let table_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.tables
         WHERE table_schema = CURRENT_SCHEMA()
           AND table_name LIKE 'live_%'",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(table_count, 10);
    let repository = crate::live::lease::ControlLeaseRepository::new(
        database.pool.clone(),
        Duration::from_secs(30),
    );
    let lease = repository.acquire(Uuid::new_v4()).await?;
    assert_eq!(lease.fencing_token, 1);
    assert!(repository.release(&lease).await?);
    assert!(
        sqlx::query("DELETE FROM live_control_server_leases")
            .execute(&database.pool)
            .await
            .is_err()
    );
    database.pool.close().await;
    let database = Database::connect(&config).await?;
    database.run_migrations().await?;
    let migration_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations WHERE version = 56")
            .fetch_one(&database.pool)
            .await?;
    assert_eq!(migration_count, 1);
    Ok(())
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
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
        .bind(user_id.to_string())
        .bind("test@example.com")
        .bind("hashed")
        .execute(&database.pool)
        .await?;

    let server_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO server_instances (id, user_id, device_name, lan_addresses) VALUES ($1, $2, $3, $4)",
    )
    .bind(server_id.to_string())
    .bind(user_id.to_string())
    .bind("Test Device")
    .bind(r#"["127.0.0.1:1234"]"#)
    .execute(&database.pool)
    .await?;

    let source_config_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO source_configs (id, server_id, extension_id, config_json, enabled) VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(source_config_id.to_string())
    .bind(server_id.to_string())
    .bind("elixir.localfolder")
    .bind(r#"{"root_path":"/media"}"#)
    .bind(true)
    .execute(&database.pool)
    .await?;

    let media_item_id = Uuid::new_v4();
    sqlx::query("INSERT INTO media_items (id, type, title, external_ids) VALUES ($1, $2, $3, $4)")
        .bind(media_item_id.to_string())
        .bind("movie")
        .bind("Test Movie")
        .bind(r#"{"tmdb":"123"}"#)
        .execute(&database.pool)
        .await?;

    let media_file_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO media_files (id, media_item_id, source_config_id, path, size_bytes, scan_state) VALUES ($1, $2, $3, $4, $5, $6)",
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
async fn portable_repository_matrix_runs_on_sqlite() -> Result<()> {
    let config = DatabaseConfig {
        url: "sqlite::memory:?cache=shared".to_string(),
        max_connections: 1,
        connect_timeout_seconds: 5,
    };
    let database = Database::connect(&config).await?;
    database.run_migrations().await?;
    exercise_portable_repository_matrix(&database).await
}

async fn exercise_portable_repository_matrix(database: &Database) -> Result<()> {
    exercise_any_scalar_bind_matrix(database).await?;

    let null_row = sqlx::query(
        "SELECT
            CAST(NULL AS TEXT) AS optional_text,
            CAST(NULL AS BIGINT) AS optional_integer,
            CAST(NULL AS BOOLEAN) AS optional_boolean,
            CAST(CASE WHEN TRUE THEN 1 ELSE 0 END AS BIGINT) AS boolean_true,
            CAST(CASE WHEN FALSE THEN 1 ELSE 0 END AS BIGINT) AS boolean_false",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(
        null_row.try_get::<Option<String>, _>("optional_text")?,
        None
    );
    assert_eq!(
        null_row.try_get::<Option<i64>, _>("optional_integer")?,
        None
    );
    assert_eq!(
        null_row.try_get::<Option<bool>, _>("optional_boolean")?,
        None
    );
    assert_eq!(null_row.try_get::<i64, _>("boolean_true")?, 1);
    assert_eq!(null_row.try_get::<i64, _>("boolean_false")?, 0);

    let store = ExtensionStore::new(&database.pool);
    let suffix = Uuid::new_v4();
    let extension_id = format!("elixir.test.portability.{suffix}");
    store
        .upsert_extension(&NewExtension {
            extension_id: extension_id.clone(),
            name: "Portability Matrix".to_string(),
            version: "1.0.0".to_string(),
            kind: ExtensionKind::Module,
            publisher_name: None,
            signing_key_id: None,
            trust_level: ExtensionTrustLevel::Verified,
            manifest_json: serde_json::json!({"id": extension_id}),
            package_hash: None,
            enabled: true,
        })
        .await?;
    let installed = store
        .get_extension(&extension_id)
        .await?
        .expect("portable extension was not persisted");
    assert!(installed.enabled);
    assert!(store.list_extensions().await?.iter().any(|item| {
        item.extension_id == extension_id && item.publisher_name.is_none() && item.enabled
    }));
    store.set_extension_enabled(&extension_id, false).await?;
    assert!(!store.get_extension(&extension_id).await?.unwrap().enabled);

    let instance_id = Uuid::new_v4();
    store
        .create_instance(&NewExtensionInstance {
            instance_id,
            extension_id: extension_id.clone(),
            instance_name: "default".to_string(),
            config_json: None,
            enabled: true,
        })
        .await?;
    store.rename_instance(instance_id, "primary").await?;
    store
        .update_instance_runtime_version(instance_id, "1.0.0", None)
        .await?;
    let instance = store
        .get_instance(instance_id)
        .await?
        .expect("portable extension instance was not persisted");
    assert_eq!(instance.instance_name, "primary");
    assert_eq!(instance.runtime_version.as_deref(), Some("1.0.0"));
    assert!(instance.rollback_version.is_none());
    assert_eq!(store.list_instances(Some(&extension_id)).await?.len(), 1);

    let desired_id = Uuid::new_v4();
    store
        .create_desired_blueprint(&NewDesiredBlueprint {
            desired_id,
            blueprint_extension_id: extension_id.clone(),
            blueprint_version: "1.0.0".to_string(),
            params_json: None,
        })
        .await?;
    assert!(
        store
            .list_desired_blueprints(Some(false))
            .await?
            .iter()
            .any(|item| item.desired_id == desired_id)
    );
    store.mark_desired_applied(desired_id, true).await?;
    assert!(
        store
            .list_desired_blueprints(Some(true))
            .await?
            .iter()
            .any(|item| item.desired_id == desired_id)
    );

    let instance_secret_id = Uuid::new_v4();
    store
        .upsert_secret(&NewSecret {
            secret_id: instance_secret_id,
            scope: SecretScope::Instance,
            scope_id: Some(instance_id),
            key: "token".to_string(),
            value_encrypted: "ciphertext".to_string(),
            rotatable: true,
        })
        .await?;
    let filtered_secrets = store
        .list_secrets(
            Some(SecretScope::Instance),
            Some(instance_id),
            Some("token"),
        )
        .await?;
    assert_eq!(filtered_secrets.len(), 1);
    assert!(filtered_secrets[0].rotatable);

    let global_secret_id = Uuid::new_v4();
    store
        .upsert_secret(&NewSecret {
            secret_id: global_secret_id,
            scope: SecretScope::Global,
            scope_id: None,
            key: format!("global-{suffix}"),
            value_encrypted: "global-ciphertext".to_string(),
            rotatable: false,
        })
        .await?;
    assert!(
        store
            .get_secret(SecretScope::Global, None, &format!("global-{suffix}"))
            .await?
            .is_some()
    );

    let registry_id = Uuid::new_v4();
    store
        .upsert_source_registry(&NewExtensionSourceRegistry {
            registry_id,
            instance_id,
            registry_key: format!("registry-{suffix}"),
            registry_type: "nuvio_manifest_json".to_string(),
            trust_class: "custom".to_string(),
            display_name: "Portable Registry".to_string(),
            url: None,
            enabled: true,
            auto_refresh: false,
            trusted_for_executable_updates: false,
            etag: None,
            last_modified: None,
            metadata_json: None,
        })
        .await?;
    store
        .record_source_registry_fetch(registry_id, "success", None, None, None)
        .await?;
    let registries = store.list_source_registries(Some(instance_id)).await?;
    assert_eq!(registries.len(), 1);
    assert!(registries[0].enabled);
    assert!(!registries[0].auto_refresh);
    assert!(registries[0].last_fetched_at.is_some());

    let source_module_id = Uuid::new_v4();
    store
        .upsert_source_module(&NewExtensionSourceModule {
            source_module_id,
            instance_id,
            registry_id,
            module_key: format!("module-{suffix}"),
            display_name: "Portable Source".to_string(),
            ecosystem: "nuvio".to_string(),
            plugin_package: None,
            active_version: None,
            rollback_version: None,
            media_types_json: Some(serde_json::json!(["movie", "series"])),
            language_tags_json: None,
            region_tags_json: None,
            source_domains_json: None,
            account_required: false,
            unsupported: false,
            unsupported_reason: None,
            enabled: true,
            installed: false,
            pinned_version: None,
            health_state: "available".to_string(),
            replacement_recommendation_key: None,
            last_error: None,
            metadata_json: None,
        })
        .await?;
    store
        .set_source_modules_enabled_for_registry(
            registry_id,
            false,
            "disabled",
            Some("matrix-disabled"),
        )
        .await?;
    let modules = store
        .list_source_modules(Some(instance_id), Some(registry_id))
        .await?;
    assert_eq!(modules.len(), 1);
    assert!(!modules[0].enabled);
    assert_eq!(modules[0].health_state, "disabled");

    let missing_episode_id = Uuid::new_v4().to_string();
    assert!(
        crate::acquisition::episode_state::list_library_episode_acquisition_projections(
            &database.pool,
            &[missing_episode_id],
        )
        .await?
        .is_empty()
    );

    let download_id = format!("download-{suffix}");
    let route_id = format!("route-{suffix}");
    let release = upsert_release(
        &database.pool,
        NewAcquisitionRelease {
            release_id: None,
            subscription_id: None,
            source_provider_id: None,
            source_extension_id: extension_id.clone(),
            owner_id: "portability-matrix".to_string(),
            media_type: MediaType::Movie,
            title: "Portable Release".to_string(),
            release_title: "Portable.Release.2026.1080p".to_string(),
            source: "https://example.test/portable-release".to_string(),
            source_kind: "http".to_string(),
            info_hash: None,
            fingerprint: format!("fingerprint-{suffix}"),
            release_kind: ReleaseKind::Single,
            resolver_kind: ReleaseResolverKind::MovieSingle,
            resolver_version: "portability-v1".to_string(),
            confidence: ReleaseConfidence::High,
            score: Some(98.5),
            selected_route_logical_id: Some(route_id.clone()),
            selected_provider_id: None,
            download_id: Some(download_id.clone()),
            remote_release_id: None,
            state: AcquisitionReleaseState::Ready,
            state_reason: None,
            selected_candidate: Some(serde_json::json!({"portable": true})),
            coverage_plan: None,
        },
    )
    .await?;
    assert_eq!(release.download_id.as_deref(), Some(download_id.as_str()));
    assert_eq!(
        get_release_by_download_id(&database.pool, &download_id)
            .await?
            .map(|item| item.release_id),
        Some(release.release_id)
    );
    assert!(
        list_releases(
            &database.pool,
            ReleaseListFilter {
                state: Some(AcquisitionReleaseState::Ready),
                limit: Some(5),
                ..Default::default()
            },
        )
        .await?
        .iter()
        .any(|item| item.release_id == release.release_id)
    );
    assert!(
        list_active_releases_by_route(&database.pool, &route_id, 5)
            .await?
            .iter()
            .any(|item| item.release_id == release.release_id)
    );

    let release_file = upsert_release_file(
        &database.pool,
        NewAcquisitionReleaseFile {
            release_file_id: None,
            release_id: release.release_id,
            file_index: Some(0),
            file_id: None,
            provider_file_id: None,
            path: "/portable/release.mkv".to_string(),
            basename: None,
            size_bytes: Some(1_024),
            selectable: true,
            selected: Some(true),
            parsed_title: None,
            parsed_season_number: None,
            parsed_episode_number: None,
            parsed_episode_end_number: None,
            parsed_absolute_episode_number: None,
            parsed_absolute_episode_end_number: None,
            parsed_air_date: None,
            parsed_quality: None,
            parsed_language: None,
            parsed_release_group: None,
            parser_confidence: ReleaseConfidence::High,
            parser_reason: None,
            raw: None,
            provider_metadata: None,
        },
    )
    .await?;
    assert!(release_file.selectable);
    assert_eq!(release_file.selected, Some(true));
    assert_eq!(
        update_release_file_selection(&database.pool, release_file.release_file_id, Some(false),)
            .await?
            .and_then(|item| item.selected),
        Some(false)
    );

    let release_job = upsert_release_job(
        &database.pool,
        NewAcquisitionReleaseJob {
            release_job_id: None,
            release_id: release.release_id,
            route_logical_id: route_id.clone(),
            provider_id: None,
            download_id: Some(download_id.clone()),
            remote_release_id: None,
            state: ReleaseJobState::Ready,
            state_reason: None,
            active: true,
            started_at: Some(chrono::Utc::now()),
            completed_at: None,
        },
    )
    .await?;
    assert!(release_job.active);
    assert!(release_job.started_at.is_some());

    let (import_run, created) = create_or_get_import_run(
        &database.pool,
        NewAcquisitionImportRun {
            import_run_id: None,
            release_id: release.release_id,
            release_job_id: release_job.release_job_id,
            route_logical_id: route_id,
            provider_id: None,
            download_id: Some(download_id),
            remote_release_id: None,
            state: AcquisitionImportRunState::Pending,
            state_reason: None,
            mismatch_class: None,
            retry_count: 0,
            provenance: Some(serde_json::json!({"source": "portability_matrix"})),
            started_at: Some(chrono::Utc::now()),
            completed_at: None,
        },
    )
    .await?;
    assert!(created);
    assert_eq!(
        get_import_run_by_release_job(&database.pool, release_job.release_job_id)
            .await?
            .map(|item| item.import_run_id),
        Some(import_run.import_run_id)
    );
    assert!(
        list_import_runs_by_release(&database.pool, release.release_id)
            .await?
            .iter()
            .any(|item| item.import_run_id == import_run.import_run_id)
    );
    let import_link = upsert_import_file_link(
        &database.pool,
        NewAcquisitionImportFileLink {
            import_link_id: None,
            import_run_id: import_run.import_run_id,
            release_id: release.release_id,
            release_file_id: Some(release_file.release_file_id),
            target_id: None,
            local_path: None,
            media_file_id: None,
            movie_id: None,
            episode_id: None,
            state: AcquisitionImportFileLinkState::Pending,
            state_reason: None,
            verification_state: None,
            mismatch_class: None,
            evidence: Some(serde_json::json!({"portable": true})),
        },
    )
    .await?;
    assert_eq!(
        import_link.release_file_id,
        Some(release_file.release_file_id)
    );
    assert!(import_link.target_id.is_none());
    assert!(
        list_import_file_links(&database.pool, import_run.import_run_id)
            .await?
            .iter()
            .any(|item| item.import_link_id == import_link.import_link_id)
    );
    assert!(
        list_import_file_links_by_release(&database.pool, release.release_id)
            .await?
            .iter()
            .any(|item| item.import_link_id == import_link.import_link_id)
    );

    let host_fingerprint = format!("host-{suffix}");
    let performance_envelope = PlaybackPerformanceEnvelope {
        id: format!("envelope-{suffix}"),
        host_fingerprint: host_fingerprint.clone(),
        os_family: "linux".to_string(),
        os_version: None,
        gpu_vendor: None,
        gpu_model: None,
        gpu_driver_version: None,
        hardware_api: None,
        ffmpeg_path: Some("ffmpeg".to_string()),
        ffmpeg_version: None,
        ffmpeg_sha256: None,
        elixir_version: Some(env!("CARGO_PKG_VERSION").to_string()),
        workload_class_id: "video:h264:1080p:h264:720p".to_string(),
        pipeline_signature: "decode:software|encode:libx264".to_string(),
        support_decision: PlaybackSupportDecision::Supported,
        performance_decision: PlaybackPerformanceDecision::RealtimeSafe,
        confidence: PlaybackPerformanceConfidence::LiveObserved,
        p50_realtime_factor_millis: Some(1_400),
        p95_realtime_factor_millis: Some(1_100),
        startup_latency_ms: Some(250),
        first_segment_latency_ms: Some(500),
        failure_count: 0,
        sample_count: 3,
        invalidation_fingerprint: format!("invalidation-{suffix}"),
        last_observed_at: Some("2026-07-11T00:00:00Z".to_string()),
        reasons: vec!["portable_repository_matrix".to_string()],
        warnings: Vec::new(),
        remediation_codes: Vec::new(),
    };
    crate::playback::performance::upsert_playback_performance_envelope(
        &database.pool,
        &performance_envelope,
    )
    .await?;
    let loaded_envelopes = crate::playback::performance::load_playback_performance_envelopes(
        &database.pool,
        &host_fingerprint,
    )
    .await?;
    assert_eq!(loaded_envelopes.len(), 1);
    assert_eq!(
        loaded_envelopes[0].pipeline_signature,
        performance_envelope.pipeline_signature
    );
    assert_eq!(
        loaded_envelopes[0].last_observed_at,
        performance_envelope.last_observed_at
    );

    Ok(())
}

async fn exercise_any_scalar_bind_matrix(database: &Database) -> Result<()> {
    let sql = match database.driver {
        DatabaseDriver::Sqlite => {
            "SELECT
                CAST(CASE WHEN CAST($1 AS BOOLEAN) THEN 1 ELSE 0 END AS BIGINT) AS boolean_value,
                CAST(CAST($2 AS SMALLINT) AS BIGINT) AS smallint_value,
                CAST(CAST($3 AS INTEGER) AS BIGINT) AS integer_value,
                CAST($4 AS TEXT) AS bigint_value,
                CAST($5 AS DOUBLE PRECISION) AS real_value,
                CAST($6 AS DOUBLE PRECISION) AS double_value,
                CAST($7 AS TEXT) AS text_value,
                $8 AS blob_value,
                CAST($9 AS TEXT) AS timestamp_value,
                CAST($10 AS TEXT) AS null_value"
        }
        DatabaseDriver::Postgres => {
            "SELECT
                CAST(CASE WHEN CAST($1 AS BOOLEAN) THEN 1 ELSE 0 END AS BIGINT) AS boolean_value,
                CAST(CAST($2 AS SMALLINT) AS BIGINT) AS smallint_value,
                CAST(CAST($3 AS INTEGER) AS BIGINT) AS integer_value,
                CAST($4 AS TEXT) AS bigint_value,
                CAST($5 AS DOUBLE PRECISION) AS real_value,
                CAST($6 AS DOUBLE PRECISION) AS double_value,
                CAST($7 AS TEXT) AS text_value,
                CAST($8 AS BYTEA) AS blob_value,
                CAST(CAST($9 AS TIMESTAMP) AS TEXT) AS timestamp_value,
                CAST($10 AS TEXT) AS null_value"
        }
    };
    let blob = vec![0_u8, 1, 0x7f, 0x80, 0xff];
    let row = sqlx::query(sql)
        .bind(true)
        .bind(-12_i16)
        .bind(34_567_i32)
        .bind(9_876_543_210_i64)
        .bind(1.25_f32)
        .bind(-98.5_f64)
        .bind("portable text")
        .bind(blob.clone())
        .bind("2026-07-11 12:34:56")
        .bind(Option::<String>::None)
        .fetch_one(&database.pool)
        .await?;

    assert_eq!(row.try_get::<i64, _>("boolean_value")?, 1);
    assert_eq!(row.try_get::<i64, _>("smallint_value")?, -12);
    assert_eq!(row.try_get::<i64, _>("integer_value")?, 34_567);
    assert_eq!(row.try_get::<String, _>("bigint_value")?, "9876543210");
    assert!((row.try_get::<f64, _>("real_value")? - 1.25).abs() < f64::EPSILON);
    assert!((row.try_get::<f64, _>("double_value")? + 98.5).abs() < f64::EPSILON);
    assert_eq!(row.try_get::<String, _>("text_value")?, "portable text");
    assert_eq!(row.try_get::<Vec<u8>, _>("blob_value")?, blob);
    assert_eq!(
        row.try_get::<String, _>("timestamp_value")?,
        "2026-07-11 12:34:56"
    );
    assert_eq!(row.try_get::<Option<String>, _>("null_value")?, None);

    if database.driver == DatabaseDriver::Postgres {
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let decoded: f64 = sqlx::query_scalar("SELECT CAST($1 AS DOUBLE PRECISION)")
                .bind(value)
                .fetch_one(&database.pool)
                .await?;
            if value.is_nan() {
                assert!(decoded.is_nan());
            } else {
                assert_eq!(decoded, value);
            }
        }
    }

    Ok(())
}

#[test]
fn auth_home_profiles_migration_is_reserved_immediately_after_0052() {
    let versions: Vec<i64> = super::MIGRATOR
        .iter()
        .map(|migration| migration.version)
        .collect();
    let positions: Vec<usize> = versions
        .iter()
        .enumerate()
        .filter_map(|(position, version)| (*version == 53).then_some(position))
        .collect();

    assert_eq!(
        positions.len(),
        1,
        "migration 0053 must appear exactly once"
    );
    let position = positions[0];
    assert!(position > 0, "migration 0053 must have a predecessor");
    assert_eq!(versions[position - 1], 52);
    assert!(versions.windows(2).all(|pair| pair[0] < pair[1]));
}

#[test]
fn auth_sessions_migration_is_reserved_immediately_after_0053() {
    let versions: Vec<i64> = super::MIGRATOR
        .iter()
        .map(|migration| migration.version)
        .collect();
    let positions: Vec<usize> = versions
        .iter()
        .enumerate()
        .filter_map(|(position, version)| (*version == 54).then_some(position))
        .collect();

    assert_eq!(
        positions.len(),
        1,
        "migration 0054 must appear exactly once"
    );
    let position = positions[0];
    assert!(position > 0, "migration 0054 must have a predecessor");
    assert_eq!(versions[position - 1], 53);
    assert!(versions.windows(2).all(|pair| pair[0] < pair[1]));
}

#[test]
fn a12_principal_capabilities_migration_is_reserved_immediately_after_0054() {
    let versions: Vec<i64> = super::MIGRATOR
        .iter()
        .map(|migration| migration.version)
        .collect();
    let positions: Vec<usize> = versions
        .iter()
        .enumerate()
        .filter_map(|(position, version)| (*version == 55).then_some(position))
        .collect();

    assert_eq!(
        positions.len(),
        1,
        "migration 0055 must appear exactly once"
    );
    let position = positions[0];
    assert!(position > 0, "migration 0055 must have a predecessor");
    assert_eq!(versions[position - 1], 54);
    assert!(versions.windows(2).all(|pair| pair[0] < pair[1]));
}

#[tokio::test]
async fn a12_principal_capabilities_migration_backfills_owner_authorization_state() -> Result<()> {
    let database = Database::connect(&DatabaseConfig {
        url: "sqlite::memory:?cache=shared".to_string(),
        max_connections: 1,
        connect_timeout_seconds: 5,
    })
    .await?;
    migrator_through(52).run(&database.pool).await?;
    let user_id = Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, 'hashed')")
        .bind(&user_id)
        .bind("a12-owner@example.test")
        .execute(&database.pool)
        .await?;
    migrator_through(54).run(&database.pool).await?;
    migrator_through(55).run(&database.pool).await?;

    let sections: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM library_sections WHERE home_id = $1")
            .bind(&user_id)
            .fetch_one(&database.pool)
            .await?;
    assert_eq!(sections, 3);
    let grants: (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT COUNT(*),
                SUM(CASE WHEN can_view THEN 1 ELSE 0 END),
                SUM(CASE WHEN can_play THEN 1 ELSE 0 END),
                SUM(CASE WHEN can_download THEN 1 ELSE 0 END)
         FROM library_grants WHERE profile_id = $1",
    )
    .bind(&user_id)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(grants, (3, 3, 3, 3));
    let revision: i64 = sqlx::query_scalar(
        "SELECT revision FROM profile_authorization_revisions WHERE profile_id = $1",
    )
    .bind(&user_id)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(revision, 1);
    let registry: (String, i64) =
        sqlx::query_as("SELECT singleton_key, revision FROM authorization_revocation_registry")
            .fetch_one(&database.pool)
            .await?;
    assert_eq!(registry.0, "authorization-revocation-v1");
    assert_eq!(registry.1, 1);

    migrator_through(55).run(&database.pool).await?;
    let stable_counts: (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT COUNT(*) FROM library_sections WHERE home_id = $1),
            (SELECT COUNT(*) FROM library_grants WHERE profile_id = $2),
            (SELECT COUNT(*) FROM profile_authorization_revisions WHERE profile_id = $3),
            (SELECT COUNT(*) FROM authorization_revocation_registry)",
    )
    .bind(&user_id)
    .bind(&user_id)
    .bind(&user_id)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(stable_counts, (3, 3, 1, 1));
    Ok(())
}

#[tokio::test]
async fn auth_sessions_migration_rolls_back_and_retries_after_late_failure() -> Result<()> {
    let database = Database::connect(&DatabaseConfig {
        url: format!(
            "sqlite:file:auth-session-migration-{}?mode=memory&cache=shared",
            Uuid::new_v4()
        ),
        max_connections: 1,
        connect_timeout_seconds: 5,
    })
    .await?;
    migrator_through(53).run(&database.pool).await?;
    sqlx::query("CREATE TABLE refresh_tokens (id TEXT PRIMARY KEY)")
        .execute(&database.pool)
        .await?;

    let error = migrator_through(54)
        .run(&database.pool)
        .await
        .expect_err("migration 0054 should fail on the malformed late table");
    assert!(
        error.to_string().contains("session_id") || error.to_string().contains("no such column"),
        "unexpected migration failure: {error:#}"
    );
    let migration_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations WHERE version = 54")
            .fetch_one(&database.pool)
            .await?;
    assert_eq!(migration_count, 0);
    let account_sessions_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM sqlite_master
         WHERE type = 'table' AND name = 'account_sessions'",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(account_sessions_count, 0);

    sqlx::query("DROP TABLE refresh_tokens")
        .execute(&database.pool)
        .await?;
    migrator_through(54).run(&database.pool).await?;
    for table in ["account_sessions", "refresh_tokens"] {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = $1",
        )
        .bind(table)
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(count, 1, "missing auth session table {table}");
    }
    Ok(())
}

#[tokio::test]
async fn auth_sessions_schema_enforces_security_invariants_and_cascades() -> Result<()> {
    let database = Database::connect(&DatabaseConfig {
        url: format!(
            "sqlite:file:auth-session-constraints-{}?mode=memory&cache=shared",
            Uuid::new_v4()
        ),
        max_connections: 2,
        connect_timeout_seconds: 5,
    })
    .await?;
    database.run_migrations().await?;
    let user_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
        .bind(user_id.to_string())
        .bind("auth-constraints@example.test")
        .bind("hashed")
        .execute(&database.pool)
        .await?;
    let auth = AuthService::new(AuthConfig {
        access_token_secret: "constraint-access-secret-000000000000000000000".to_string(),
        refresh_token_secret: Some("constraint-refresh-secret-0000000000000000000".to_string()),
        csrf_secret: Some("constraint-csrf-secret-0000000000000000000000".to_string()),
        ..AuthConfig::default()
    })?;
    let tokens = auth
        .issue_login_tokens(&database.pool, user_id, LoginContext::default())
        .await?;

    assert!(
        sqlx::query("UPDATE account_sessions SET csrf_revision = 0 WHERE id = $1")
            .bind(tokens.session_id.to_string())
            .execute(&database.pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("UPDATE account_sessions SET revoked_at = CURRENT_TIMESTAMP WHERE id = $1")
            .bind(tokens.session_id.to_string())
            .execute(&database.pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query(
            "INSERT INTO refresh_tokens (
                id, session_id, token_hash, token_family, expires_at
             ) VALUES ($1, $2, 'too-short', $3, $4)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(tokens.session_id.to_string())
        .bind(Uuid::new_v4().to_string())
        .bind((Utc::now() + chrono::Duration::hours(1)).to_rfc3339())
        .execute(&database.pool)
        .await
        .is_err()
    );

    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(user_id.to_string())
        .execute(&database.pool)
        .await?;
    let counts: (i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT COUNT(*) FROM account_sessions),
            (SELECT COUNT(*) FROM refresh_tokens)",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(counts, (0, 0));
    Ok(())
}

#[tokio::test]
async fn auth_home_profiles_migration_backfills_legacy_accounts_deterministically() -> Result<()> {
    let config = DatabaseConfig {
        url: "sqlite::memory:?cache=shared".to_string(),
        max_connections: 1,
        connect_timeout_seconds: 5,
    };
    let database = Database::connect(&config).await?;
    migrator_through(52).run(&database.pool).await?;

    let alice_id = Uuid::new_v4().to_string();
    let fallback_id = Uuid::new_v4().to_string();
    let malformed_id = Uuid::new_v4().to_string();
    for (id, email) in [
        (alice_id.as_str(), "alice@example.com"),
        (fallback_id.as_str(), "   "),
        (malformed_id.as_str(), "local-only"),
    ] {
        sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
            .bind(id)
            .bind(email)
            .bind("hashed")
            .execute(&database.pool)
            .await?;
    }

    let alice_server_id = Uuid::new_v4().to_string();
    sqlx::query(
        "INSERT INTO server_instances (id, user_id, device_name, lan_addresses)
         VALUES ($1, $2, $3, $4)",
    )
    .bind(&alice_server_id)
    .bind(&alice_id)
    .bind("Alice Server")
    .bind("[]")
    .execute(&database.pool)
    .await?;

    migrator_through(53).run(&database.pool).await?;

    let alice_home: (String, String, String) =
        sqlx::query_as("SELECT id, owner_user_id, name FROM homes WHERE owner_user_id = $1")
            .bind(&alice_id)
            .fetch_one(&database.pool)
            .await?;
    assert_eq!(
        alice_home,
        (
            alice_id.clone(),
            alice_id.clone(),
            "alice@example.com's Home".to_string(),
        )
    );

    let alice_membership: (String, String, String, String, String) = sqlx::query_as(
        "SELECT id, home_id, user_id, role, status
         FROM home_members
         WHERE home_id = $1 AND user_id = $2",
    )
    .bind(&alice_id)
    .bind(&alice_id)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(
        alice_membership,
        (
            alice_id.clone(),
            alice_id.clone(),
            alice_id.clone(),
            "owner".to_string(),
            "active".to_string(),
        )
    );

    let alice_profile: (String, String, String, String, String, i64) = sqlx::query_as(
        "SELECT id,
                home_id,
                user_id,
                profile_type,
                display_name,
                CAST(is_default AS INTEGER)
         FROM profiles
         WHERE home_id = $1 AND user_id = $2",
    )
    .bind(&alice_id)
    .bind(&alice_id)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(
        alice_profile,
        (
            alice_id.clone(),
            alice_id.clone(),
            alice_id.clone(),
            "account".to_string(),
            "alice".to_string(),
            1,
        )
    );

    let fallback_home_name: String =
        sqlx::query_scalar("SELECT name FROM homes WHERE owner_user_id = $1")
            .bind(&fallback_id)
            .fetch_one(&database.pool)
            .await?;
    let fallback_profile_name: String =
        sqlx::query_scalar("SELECT display_name FROM profiles WHERE home_id = $1")
            .bind(&fallback_id)
            .fetch_one(&database.pool)
            .await?;
    assert_eq!(fallback_home_name, "Elixir Home");
    assert_eq!(fallback_profile_name, "Owner");

    let malformed_home_name: String =
        sqlx::query_scalar("SELECT name FROM homes WHERE owner_user_id = $1")
            .bind(&malformed_id)
            .fetch_one(&database.pool)
            .await?;
    let malformed_profile_name: String =
        sqlx::query_scalar("SELECT display_name FROM profiles WHERE home_id = $1")
            .bind(&malformed_id)
            .fetch_one(&database.pool)
            .await?;
    assert_eq!(malformed_home_name, "local-only's Home");
    assert_eq!(malformed_profile_name, "Owner");

    let server_home_id: String =
        sqlx::query_scalar("SELECT home_id FROM server_instances WHERE id = $1")
            .bind(&alice_server_id)
            .fetch_one(&database.pool)
            .await?;
    assert_eq!(server_home_id, alice_id);

    let migration_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations WHERE version = 53")
            .fetch_one(&database.pool)
            .await?;
    assert_eq!(migration_count, 1);
    assert!(
        sqlx::query("PRAGMA foreign_key_check")
            .fetch_optional(&database.pool)
            .await?
            .is_none()
    );

    migrator_through(53).run(&database.pool).await?;
    for table in ["homes", "home_members", "profiles"] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(&database.pool)
            .await?;
        assert_eq!(count, 3, "rerunning migrations changed {table}");
    }

    Ok(())
}

#[tokio::test]
async fn auth_home_profiles_constraints_and_foreign_keys_are_enforced() -> Result<()> {
    let config = DatabaseConfig {
        url: "sqlite::memory:?cache=shared".to_string(),
        max_connections: 1,
        connect_timeout_seconds: 5,
    };
    let database = Database::connect(&config).await?;
    migrator_through(52).run(&database.pool).await?;

    let alice_id = Uuid::new_v4().to_string();
    let bob_id = Uuid::new_v4().to_string();
    for (id, email) in [
        (alice_id.as_str(), "alice@example.com"),
        (bob_id.as_str(), "bob@example.com"),
    ] {
        sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
            .bind(id)
            .bind(email)
            .bind("hashed")
            .execute(&database.pool)
            .await?;
    }
    let server_id = Uuid::new_v4().to_string();
    sqlx::query(
        "INSERT INTO server_instances (id, user_id, device_name, lan_addresses)
         VALUES ($1, $2, $3, $4)",
    )
    .bind(&server_id)
    .bind(&alice_id)
    .bind("Alice Server")
    .bind("[]")
    .execute(&database.pool)
    .await?;
    migrator_through(53).run(&database.pool).await?;

    assert!(
        sqlx::query(
            "INSERT INTO home_members (id, home_id, user_id, role, status)
             VALUES ($1, $2, $3, 'owner', 'suspended')",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(&alice_id)
        .bind(&bob_id)
        .execute(&database.pool)
        .await
        .is_err(),
        "an owner must be active"
    );
    assert!(
        sqlx::query(
            "INSERT INTO home_members (id, home_id, user_id, role, status)
             VALUES ($1, $2, $3, 'owner', 'active')",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(&alice_id)
        .bind(&bob_id)
        .execute(&database.pool)
        .await
        .is_err(),
        "a home must not have two active owners"
    );

    let bob_membership_id = Uuid::new_v4().to_string();
    sqlx::query(
        "INSERT INTO home_members (id, home_id, user_id, role, status)
         VALUES ($1, $2, $3, 'viewer', 'invited')",
    )
    .bind(&bob_membership_id)
    .bind(&alice_id)
    .bind(&bob_id)
    .execute(&database.pool)
    .await?;
    assert!(
        sqlx::query(
            "INSERT INTO home_members (id, home_id, user_id, role, status)
             VALUES ($1, $2, $3, 'viewer', 'active')",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(&alice_id)
        .bind(&bob_id)
        .execute(&database.pool)
        .await
        .is_err(),
        "a user must have at most one membership per home"
    );

    assert!(
        sqlx::query(
            "INSERT INTO home_members (id, home_id, user_id, role, status)
             VALUES ($1, $2, $3, 'viewer', 'active')",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(Uuid::new_v4().to_string())
        .bind(&bob_id)
        .execute(&database.pool)
        .await
        .is_err(),
        "memberships must reference an existing home"
    );

    assert!(
        sqlx::query(
            "INSERT INTO profiles
                (id, home_id, user_id, profile_type, display_name, is_default)
             VALUES ($1, $2, NULL, 'account', 'Invalid Account', FALSE)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(&alice_id)
        .execute(&database.pool)
        .await
        .is_err(),
        "account profiles must reference an account"
    );
    assert!(
        sqlx::query(
            "INSERT INTO profiles
                (id, home_id, user_id, profile_type, display_name, is_default)
             VALUES ($1, $2, $3, 'managed', 'Invalid Managed', FALSE)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(&alice_id)
        .bind(&bob_id)
        .execute(&database.pool)
        .await
        .is_err(),
        "managed profiles must not reference a sign-in account"
    );
    assert!(
        sqlx::query(
            "INSERT INTO profiles
                (id, home_id, user_id, profile_type, display_name, is_default)
             VALUES ($1, $2, $3, 'account', 'Alice Duplicate', FALSE)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(&alice_id)
        .bind(&alice_id)
        .execute(&database.pool)
        .await
        .is_err(),
        "an account must have at most one profile in a home"
    );
    assert!(
        sqlx::query(
            "INSERT INTO profiles
                (id, home_id, user_id, profile_type, display_name, is_default)
             VALUES ($1, $2, NULL, 'managed', 'Second Default', TRUE)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(&alice_id)
        .execute(&database.pool)
        .await
        .is_err(),
        "a home must have at most one default profile"
    );
    assert!(
        sqlx::query(
            "INSERT INTO profiles
                (id, home_id, user_id, profile_type, display_name, is_default)
             VALUES ($1, $2, NULL, 'managed', 'Invalid Boolean', 2)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(&alice_id)
        .execute(&database.pool)
        .await
        .is_err(),
        "SQLite must reject non-boolean default-profile values"
    );

    let managed_profile_id = Uuid::new_v4().to_string();
    sqlx::query(
        "INSERT INTO profiles
            (id, home_id, user_id, profile_type, display_name, is_default)
         VALUES ($1, $2, NULL, 'managed', 'Guest', FALSE)",
    )
    .bind(&managed_profile_id)
    .bind(&alice_id)
    .execute(&database.pool)
    .await?;

    assert!(
        sqlx::query("PRAGMA foreign_key_check")
            .fetch_optional(&database.pool)
            .await?
            .is_none()
    );

    sqlx::query("DELETE FROM homes WHERE id = $1")
        .bind(&alice_id)
        .execute(&database.pool)
        .await?;
    let server_home_id: String = sqlx::query_scalar(
        "SELECT COALESCE(CAST(home_id AS TEXT), '')
         FROM server_instances
         WHERE id = $1",
    )
    .bind(&server_id)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(server_home_id, "");
    for table in ["home_members", "profiles"] {
        let count: i64 =
            sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table} WHERE home_id = $1"))
                .bind(&alice_id)
                .fetch_one(&database.pool)
                .await?;
        assert_eq!(count, 0, "deleting a home did not cascade to {table}");
    }
    let bob_home_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM homes WHERE id = $1")
        .bind(&bob_id)
        .fetch_one(&database.pool)
        .await?;
    assert_eq!(bob_home_count, 1);

    Ok(())
}

#[tokio::test]
async fn auth_home_profiles_migration_rolls_back_all_changes_after_late_failure() -> Result<()> {
    let config = DatabaseConfig {
        url: "sqlite::memory:?cache=shared".to_string(),
        max_connections: 1,
        connect_timeout_seconds: 5,
    };
    let database = Database::connect(&config).await?;
    migrator_through(52).run(&database.pool).await?;

    sqlx::query("ALTER TABLE server_instances ADD COLUMN home_id TEXT")
        .execute(&database.pool)
        .await?;
    let error = migrator_through(53)
        .run(&database.pool)
        .await
        .expect_err("migration 0053 should fail on the duplicate late column");
    assert!(
        error
            .to_string()
            .to_lowercase()
            .contains("duplicate column"),
        "unexpected migration failure: {error:#}"
    );

    let migration_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations WHERE version = 53")
            .fetch_one(&database.pool)
            .await?;
    assert_eq!(migration_count, 0);
    let new_table_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM sqlite_master
         WHERE type = 'table' AND name IN ('homes', 'home_members', 'profiles')",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(new_table_count, 0);
    let new_index_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM sqlite_master
         WHERE type = 'index'
           AND name IN (
               'idx_homes_owner',
               'idx_home_members_user',
               'idx_profiles_home',
               'idx_profiles_user',
               'idx_home_members_active_owner',
               'idx_profiles_account_user',
               'idx_profiles_default'
           )",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(new_index_count, 0);
    let home_id_column_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM pragma_table_info('server_instances')
         WHERE name = 'home_id'",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(home_id_column_count, 1, "the pre-existing column was lost");

    Ok(())
}

#[tokio::test]
async fn auth_home_profiles_postgres_upgrade_and_rollback_when_configured() -> Result<()> {
    let Ok(url) = std::env::var("ELIXIR_TEST_POSTGRES_EMPTY_DATABASE_URL") else {
        return Ok(());
    };
    let config = DatabaseConfig {
        url,
        max_connections: 2,
        connect_timeout_seconds: 5,
    };
    let database = Database::connect(&config).await?;
    assert_eq!(database.driver, DatabaseDriver::Postgres);
    postgres_migrator_through(52)?.run(&database.pool).await?;

    let legacy_user_id = Uuid::new_v4();
    let legacy_server_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
        .bind(legacy_user_id.to_string())
        .bind("postgres.legacy@example.com")
        .bind("hashed")
        .execute(&database.pool)
        .await?;
    sqlx::query(
        "INSERT INTO server_instances (id, user_id, device_name, lan_addresses)
         VALUES ($1, $2, $3, $4)",
    )
    .bind(legacy_server_id.to_string())
    .bind(legacy_user_id.to_string())
    .bind("Postgres Legacy Server")
    .bind("[]")
    .execute(&database.pool)
    .await?;

    sqlx::query("ALTER TABLE server_instances ADD COLUMN home_id TEXT")
        .execute(&database.pool)
        .await?;
    assert!(
        postgres_migrator_through(53)?
            .run(&database.pool)
            .await
            .is_err()
    );
    let migration_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations WHERE version = 53")
            .fetch_one(&database.pool)
            .await?;
    assert_eq!(migration_count, 0);
    let rolled_back_table_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM information_schema.tables
         WHERE table_schema = CURRENT_SCHEMA()
           AND table_name IN ('homes', 'home_members', 'profiles')",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(rolled_back_table_count, 0);

    database.pool.close().await;
    let database = Database::connect(&config).await?;
    sqlx::query("ALTER TABLE server_instances DROP COLUMN home_id")
        .execute(&database.pool)
        .await?;
    let retry_migrator = postgres_migrator_through(53)?;
    tokio::time::timeout(Duration::from_secs(10), retry_migrator.run(&database.pool)).await??;

    let legacy_profile: (String, String, i64) = sqlx::query_as(
        "SELECT id,
                display_name,
                CAST(CASE WHEN is_default THEN 1 ELSE 0 END AS BIGINT)
         FROM profiles
         WHERE home_id = $1 AND user_id = $2",
    )
    .bind(legacy_user_id.to_string())
    .bind(legacy_user_id.to_string())
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(
        legacy_profile,
        (legacy_user_id.to_string(), "postgres.legacy".to_string(), 1,)
    );
    let legacy_server_home_id: String =
        sqlx::query_scalar("SELECT home_id FROM server_instances WHERE id = $1")
            .bind(legacy_server_id.to_string())
            .fetch_one(&database.pool)
            .await?;
    assert_eq!(legacy_server_home_id, legacy_user_id.to_string());

    let repository = crate::auth::home_profiles::HomeProfileRepository::new(&database.pool);
    let legacy_bootstrap = repository.ensure_owner_home(legacy_user_id).await?;
    assert_eq!(legacy_bootstrap.home.id, legacy_user_id);
    let legacy_home_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM homes WHERE owner_user_id = $1")
            .bind(legacy_user_id.to_string())
            .fetch_one(&database.pool)
            .await?;
    assert_eq!(legacy_home_count, 1);

    let runtime_user_id = Uuid::new_v4();
    let runtime_server_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
        .bind(runtime_user_id.to_string())
        .bind("postgres.runtime@example.com")
        .bind("hashed")
        .execute(&database.pool)
        .await?;
    sqlx::query(
        "INSERT INTO server_instances (id, user_id, device_name, lan_addresses)
         VALUES ($1, $2, $3, $4)",
    )
    .bind(runtime_server_id.to_string())
    .bind(runtime_user_id.to_string())
    .bind("Postgres Runtime Server")
    .bind("[]")
    .execute(&database.pool)
    .await?;

    let first = repository.ensure_owner_home(runtime_user_id).await?;
    let second = repository.ensure_owner_home(runtime_user_id).await?;
    assert_eq!(first, second);
    assert_eq!(first.profile.display_name, "postgres.runtime");
    let runtime_server_home_id: String =
        sqlx::query_scalar("SELECT home_id FROM server_instances WHERE id = $1")
            .bind(runtime_server_id.to_string())
            .fetch_one(&database.pool)
            .await?;
    assert_eq!(runtime_server_home_id, first.home.id.to_string());

    database.run_migrations().await?;
    let applied_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations")
        .fetch_one(&database.pool)
        .await?;
    assert_eq!(applied_count, 55);
    exercise_portable_repository_matrix(&database).await?;

    Ok(())
}

#[tokio::test]
async fn auth_sessions_postgres_upgrade_rollback_restart_and_concurrency_when_configured()
-> Result<()> {
    let Ok(url) = std::env::var("ELIXIR_TEST_POSTGRES_EMPTY_DATABASE_URL") else {
        return Ok(());
    };
    let config = DatabaseConfig {
        url,
        max_connections: 4,
        connect_timeout_seconds: 5,
    };
    let database = Database::connect(&config).await?;
    assert_eq!(database.driver, DatabaseDriver::Postgres);
    postgres_migrator_through(53)?.run(&database.pool).await?;
    sqlx::query("CREATE TABLE refresh_tokens (id TEXT PRIMARY KEY)")
        .execute(&database.pool)
        .await?;
    assert!(
        postgres_migrator_through(54)?
            .run(&database.pool)
            .await
            .is_err()
    );
    let migration_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations WHERE version = 54")
            .fetch_one(&database.pool)
            .await?;
    assert_eq!(migration_count, 0);
    let account_sessions_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM information_schema.tables
         WHERE table_schema = CURRENT_SCHEMA() AND table_name = 'account_sessions'",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(account_sessions_count, 0);

    sqlx::query("DROP TABLE refresh_tokens")
        .execute(&database.pool)
        .await?;
    tokio::time::timeout(
        Duration::from_secs(10),
        postgres_migrator_through(54)?.run(&database.pool),
    )
    .await??;

    let user_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
        .bind(user_id.to_string())
        .bind("postgres.sessions@example.test")
        .bind("hashed")
        .execute(&database.pool)
        .await?;
    let auth = AuthService::new(AuthConfig {
        access_token_secret: "postgres-access-secret-0000000000000000000000000000".to_string(),
        refresh_token_secret: Some(
            "postgres-refresh-secret-000000000000000000000000000".to_string(),
        ),
        csrf_secret: Some("postgres-csrf-secret-00000000000000000000000000000".to_string()),
        ..AuthConfig::default()
    })?;
    let tokens = auth
        .issue_login_tokens(
            &database.pool,
            user_id,
            LoginContext {
                remember_device: true,
                ..LoginContext::default()
            },
        )
        .await?;
    let token = tokens.refresh_token.expose_secret().to_string();
    let first = auth.refresh_session(&database.pool, &token, LoginContext::default());
    let second = auth.refresh_session(&database.pool, &token, LoginContext::default());
    let (first, second) = tokio::join!(first, second);
    let outcomes = [first, second];
    assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|result| matches!(result, Err(AuthSessionError::RefreshTokenReused)))
            .count(),
        1
    );
    let active: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM account_sessions WHERE id = $1 AND revoked_at IS NULL",
    )
    .bind(tokens.session_id.to_string())
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(active, 0);

    database.pool.close().await;
    let database = Database::connect(&config).await?;
    database.run_migrations().await?;
    let applied_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations")
        .fetch_one(&database.pool)
        .await?;
    assert_eq!(applied_count, 55);
    let revoked_reason: String =
        sqlx::query_scalar("SELECT revoked_reason FROM account_sessions WHERE id = $1")
            .bind(tokens.session_id.to_string())
            .fetch_one(&database.pool)
            .await?;
    assert_eq!(revoked_reason, "refresh_token_reuse");
    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(user_id.to_string())
        .execute(&database.pool)
        .await?;
    let cascade_counts: (i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT COUNT(*) FROM account_sessions),
            (SELECT COUNT(*) FROM refresh_tokens)",
    )
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(cascade_counts, (0, 0));
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
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = $1",
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
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = $1",
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
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = $1",
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
            "SELECT COUNT(*) FROM pragma_table_info('playback_sessions') WHERE name = $1",
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
            "SELECT COUNT(*) FROM pragma_table_info('playback_sessions') WHERE name = $1",
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
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = $1",
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
    sqlx::query("INSERT INTO movies (id, title, runtime_seconds) VALUES ($1, $2, $3)")
        .bind(&movie_id)
        .bind("Backfill Movie")
        .bind(7200_i64)
        .execute(&database.pool)
        .await?;
    sqlx::query("INSERT INTO movie_files (movie_id, media_file_id) VALUES ($1, $2)")
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
    sqlx::query("INSERT INTO series (id, title, library_type) VALUES ($1, $2, $3)")
        .bind(&series_id)
        .bind("Backfill Series")
        .bind("tv")
        .execute(&database.pool)
        .await?;
    sqlx::query(
        "INSERT INTO seasons (id, series_id, season_number, title) VALUES ($1, $2, $3, $4)",
    )
    .bind(&season_id)
    .bind(&series_id)
    .bind(1_i64)
    .bind("Season 1")
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "INSERT INTO episodes
            (id, series_id, season_id, season_number, episode_number, title, runtime_seconds)
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
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
    sqlx::query("INSERT INTO episode_files (episode_id, media_file_id) VALUES ($1, $2)")
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
         WHERE user_id = $1 AND item_type = 'movie' AND item_id = $2",
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
         WHERE user_id = $1 AND item_type = 'episode' AND item_id = $2",
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
         WHERE id = $1",
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
    sqlx::query("INSERT INTO movies (id, title, runtime_seconds) VALUES ($1, $2, $3)")
        .bind(&ambiguous_movie_id)
        .bind("Ambiguous Movie")
        .bind(3600_i64)
        .execute(&database.pool)
        .await?;
    sqlx::query("INSERT INTO movie_files (movie_id, media_file_id) VALUES ($1, $2)")
        .bind(&ambiguous_movie_id)
        .bind(&ambiguous_file_id)
        .execute(&database.pool)
        .await?;
    sqlx::query("INSERT INTO series (id, title, library_type) VALUES ($1, $2, $3)")
        .bind(&ambiguous_series_id)
        .bind("Ambiguous Series")
        .bind("tv")
        .execute(&database.pool)
        .await?;
    sqlx::query(
        "INSERT INTO seasons (id, series_id, season_number, title) VALUES ($1, $2, $3, $4)",
    )
    .bind(&ambiguous_season_id)
    .bind(&ambiguous_series_id)
    .bind(1_i64)
    .bind("Season 1")
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "INSERT INTO episodes
            (id, series_id, season_id, season_number, episode_number, title, runtime_seconds)
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
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
    sqlx::query("INSERT INTO episode_files (episode_id, media_file_id) VALUES ($1, $2)")
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
         WHERE user_id = $1",
    )
    .bind(&user_id)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(conservative_count, 0);

    for session_id in [&ambiguous_session, &unlinked_session] {
        let selected_item_id: String = sqlx::query_scalar(
            "SELECT COALESCE(selected_item_id, '') FROM playback_sessions WHERE id = $1",
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
    sqlx::query("INSERT INTO movies (id, title, runtime_seconds) VALUES ($1, $2, $3)")
        .bind(&movie_id)
        .bind("Manual Preserve Movie")
        .bind(7200_i64)
        .execute(&database.pool)
        .await?;
    sqlx::query("INSERT INTO movie_files (movie_id, media_file_id) VALUES ($1, $2)")
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
         WHERE user_id = $1 AND item_type = 'movie' AND item_id = $2",
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
         WHERE user_id = $1 AND item_type = 'movie' AND item_id = $2",
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
         WHERE user_id = $1 AND item_type = 'movie' AND item_id = $2",
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
    sqlx::query("INSERT INTO movies (id, title, runtime_seconds) VALUES ($1, $2, $3)")
        .bind(&movie_id)
        .bind("Copied Upgrade Movie")
        .bind(3600_i64)
        .execute(&pre_upgrade.pool)
        .await?;
    sqlx::query("INSERT INTO movie_files (movie_id, media_file_id) VALUES ($1, $2)")
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
    sqlx::query("INSERT INTO series (id, title, library_type) VALUES ($1, $2, $3)")
        .bind(&series_id)
        .bind("Copied Upgrade Show")
        .bind("tv")
        .execute(&pre_upgrade.pool)
        .await?;
    sqlx::query(
        "INSERT INTO seasons (id, series_id, season_number, title) VALUES ($1, $2, $3, $4)",
    )
    .bind(&season_id)
    .bind(&series_id)
    .bind(1_i64)
    .bind("Season 1")
    .execute(&pre_upgrade.pool)
    .await?;
    sqlx::query(
        "INSERT INTO episodes
            (id, series_id, season_id, season_number, episode_number, title, runtime_seconds)
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
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
    sqlx::query("INSERT INTO episode_files (episode_id, media_file_id) VALUES ($1, $2)")
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
    sqlx::query("INSERT INTO movies (id, title, runtime_seconds) VALUES ($1, $2, $3)")
        .bind(&ambiguous_movie_id)
        .bind("Copied Ambiguous Movie")
        .bind(3600_i64)
        .execute(&pre_upgrade.pool)
        .await?;
    sqlx::query("INSERT INTO movie_files (movie_id, media_file_id) VALUES ($1, $2)")
        .bind(&ambiguous_movie_id)
        .bind(&ambiguous_file_id)
        .execute(&pre_upgrade.pool)
        .await?;
    sqlx::query(
        "INSERT INTO episodes
            (id, series_id, season_id, season_number, episode_number, title, runtime_seconds)
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
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
    sqlx::query("INSERT INTO episode_files (episode_id, media_file_id) VALUES ($1, $2)")
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
         WHERE user_id = $1",
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
         WHERE user_id = $1 AND item_type = 'movie' AND item_id = $2",
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
         WHERE user_id = $1 AND item_type = 'episode' AND item_id = $2",
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
        "SELECT COALESCE(selected_item_id, '') FROM playback_sessions WHERE id = $1",
    )
    .bind(&ambiguous_session)
    .fetch_one(&copied.pool)
    .await?;
    assert!(
        ambiguous_selected_item_id.is_empty(),
        "ambiguous copied playback history must remain session-only"
    );

    let movie_selected_item_id: String =
        sqlx::query_scalar("SELECT selected_item_id FROM playback_sessions WHERE id = $1")
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
         WHERE id = $1",
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
         WHERE user_id = $1",
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

    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
        .bind(&user_id)
        .bind(format!("{user_id}@example.com"))
        .bind("hashed")
        .execute(pool)
        .await?;
    sqlx::query(
        "INSERT INTO server_instances (id, user_id, device_name, lan_addresses)
         VALUES ($1, $2, $3, $4)",
    )
    .bind(&server_id)
    .bind(&user_id)
    .bind("Backfill Test")
    .bind(r#"["127.0.0.1:1234"]"#)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO source_configs (id, server_id, extension_id, config_json, enabled)
         VALUES ($1, $2, $3, $4, $5)",
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

    sqlx::query("INSERT INTO media_items (id, type, title, external_ids) VALUES ($1, $2, $3, $4)")
        .bind(&media_item_id)
        .bind(item_type)
        .bind(title)
        .bind("{}")
        .execute(pool)
        .await?;
    sqlx::query(
        "INSERT INTO media_files
            (id, media_item_id, source_config_id, path, size_bytes, scan_state)
         VALUES ($1, $2, $3, $4, $5, $6)",
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
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
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
