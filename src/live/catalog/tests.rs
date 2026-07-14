use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::Result;
use chrono::{TimeZone, Utc};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    auth::{
        home_profiles::{HomeRole, ProfileType},
        revocation::AuthorizationRevocationNotifier,
    },
    config::DatabaseConfig,
    db::Database,
    live::{
        contract::{
            ArtworkSource, CacheHint, CatalogPage, CatalogPageRequest, CatalogSet, Fact,
            FilterValue, LiveItem, LiveItemStatus, LiveItemType, MetaRequest,
        },
        crypto::{LiveCrypto, LiveMasterKey},
        provider::tests::{NativeFixture, build_client, seed_provider, test_database},
    },
};

use super::{
    cache::{CacheFreshness, CacheKey, CacheOperation, CatalogCacheRepository, CatalogCacheValue},
    circuit::{CircuitAdmission, ProviderCircuitBreakers},
    coalesce::{CatalogRequestCoalescer, CoalescedLoadError},
    grants::{LiveProviderAccess, LiveProviderGrantError, LiveProviderGrantRepository},
    service::{LiveCatalogAccessContext, LiveCatalogService},
};

struct Fixture {
    database: Database,
    owner_id: Uuid,
    home_id: Uuid,
    profile_id: Uuid,
    provider_id: Uuid,
}

async fn sqlite_fixture() -> Result<Fixture> {
    let database = Database::connect(&DatabaseConfig {
        url: format!(
            "sqlite:file:s12-live-catalog-{}?mode=memory&cache=shared",
            Uuid::new_v4()
        ),
        max_connections: 8,
        connect_timeout_seconds: 5,
    })
    .await?;
    database.run_migrations().await?;
    seed_fixture(database).await
}

async fn seed_fixture(database: Database) -> Result<Fixture> {
    let owner_id = Uuid::new_v4();
    let member_id = Uuid::new_v4();
    let home_id = Uuid::new_v4();
    let profile_id = Uuid::new_v4();
    let extension_id = format!("dev.elixir.s12.{}", Uuid::new_v4().simple());
    let instance_id = Uuid::new_v4();
    let provider_id = Uuid::new_v4();
    for (id, email) in [
        (owner_id, format!("{owner_id}@example.invalid")),
        (member_id, format!("{member_id}@example.invalid")),
    ] {
        sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
            .bind(id.to_string())
            .bind(email)
            .bind("test-hash")
            .execute(&database.pool)
            .await?;
    }
    sqlx::query("INSERT INTO homes (id, owner_user_id, name) VALUES ($1, $2, $3)")
        .bind(home_id.to_string())
        .bind(owner_id.to_string())
        .bind("S12 Home")
        .execute(&database.pool)
        .await?;
    for (id, user_id, role) in [
        (Uuid::new_v4(), owner_id, "owner"),
        (Uuid::new_v4(), member_id, "manager"),
    ] {
        sqlx::query(
            "INSERT INTO home_members (id, home_id, user_id, role, status)
             VALUES ($1, $2, $3, $4, 'active')",
        )
        .bind(id.to_string())
        .bind(home_id.to_string())
        .bind(user_id.to_string())
        .bind(role)
        .execute(&database.pool)
        .await?;
    }
    sqlx::query(
        "INSERT INTO profiles (id, home_id, user_id, profile_type, display_name, is_default)
         VALUES ($1, $2, $3, 'account', 'Manager', FALSE)",
    )
    .bind(profile_id.to_string())
    .bind(home_id.to_string())
    .bind(member_id.to_string())
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "INSERT INTO profile_authorization_revisions (profile_id, home_id, revision)
         VALUES ($1, $2, 1)",
    )
    .bind(profile_id.to_string())
    .bind(home_id.to_string())
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "INSERT INTO extensions (extension_id, name, version, kind, trust_level, manifest_json)
         VALUES ($1, 'S12', '1.0.0', 'connector', 'verified', '{}')",
    )
    .bind(&extension_id)
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "INSERT INTO extension_instances (instance_id, extension_id, instance_name)
         VALUES ($1, $2, 'default')",
    )
    .bind(instance_id.to_string())
    .bind(&extension_id)
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "INSERT INTO providers (provider_id, instance_id, capability, cardinality)
         VALUES ($1, $2, 'live.catalog_provider/v1', 'many')",
    )
    .bind(provider_id.to_string())
    .bind(instance_id.to_string())
    .execute(&database.pool)
    .await?;
    Ok(Fixture {
        database,
        owner_id,
        home_id,
        profile_id,
        provider_id,
    })
}

async fn seed_owner_profile(database: &Database) -> Result<(Uuid, Uuid, Uuid)> {
    let user_id = Uuid::new_v4();
    let home_id = Uuid::new_v4();
    let profile_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, $3)")
        .bind(user_id.to_string())
        .bind(format!("{user_id}@example.invalid"))
        .bind("test-hash")
        .execute(&database.pool)
        .await?;
    sqlx::query("INSERT INTO homes (id, owner_user_id, name) VALUES ($1, $2, 'Owner Home')")
        .bind(home_id.to_string())
        .bind(user_id.to_string())
        .execute(&database.pool)
        .await?;
    sqlx::query(
        "INSERT INTO home_members (id, home_id, user_id, role, status)
         VALUES ($1, $2, $3, 'owner', 'active')",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(home_id.to_string())
    .bind(user_id.to_string())
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "INSERT INTO profiles (id, home_id, user_id, profile_type, display_name, is_default)
         VALUES ($1, $2, $3, 'account', 'Owner', TRUE)",
    )
    .bind(profile_id.to_string())
    .bind(home_id.to_string())
    .bind(user_id.to_string())
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "INSERT INTO profile_authorization_revisions (profile_id, home_id, revision)
         VALUES ($1, $2, 1)",
    )
    .bind(profile_id.to_string())
    .bind(home_id.to_string())
    .execute(&database.pool)
    .await?;
    Ok((user_id, home_id, profile_id))
}

fn crypto() -> Result<Arc<LiveCrypto>> {
    Ok(Arc::new(LiveCrypto::new(
        "s12-key",
        [LiveMasterKey::new("s12-key", [12u8; 32])?],
    )?))
}

fn cache_value() -> CatalogCacheValue {
    CatalogCacheValue::Catalog(CatalogPage {
        items: vec![LiveItem {
            id: "event-1".to_string(),
            item_type: LiveItemType::Event,
            title: "Final".to_string(),
            subtitle: None,
            description: None,
            status: LiveItemStatus::Live,
            starts_at: None,
            ends_at: None,
            poster: Some(ArtworkSource::new(
                "https://artwork.example.invalid/private/poster.jpg".to_string(),
            )),
            background: None,
            logo: None,
            categories: vec!["sport".to_string()],
            badges: vec!["live".to_string()],
            facts: vec![Fact {
                label: "Round".to_string(),
                value: "Final".to_string(),
            }],
        }],
        next_cursor: None,
        cache: CacheHint {
            max_age_seconds: 10,
            stale_while_revalidate_seconds: 20,
            etag: Some("fixture-etag".to_string()),
        },
        diagnostics: Vec::new(),
    })
}

#[tokio::test]
async fn s12_cache_encrypts_artwork_and_survives_restart_fresh_stale_and_expiry() -> Result<()> {
    let fixture = sqlite_fixture().await?;
    let crypto = crypto()?;
    let key = CacheKey::for_test("live-cache-v1:s12-cache-test");
    let now = Utc.with_ymd_and_hms(2026, 7, 12, 12, 0, 0).unwrap();
    let repository = CatalogCacheRepository::new(fixture.database.pool.clone(), crypto.clone());
    repository
        .put(&key, fixture.provider_id, &cache_value(), now)
        .await?;
    let persisted: String =
        sqlx::query_scalar("SELECT payload_json FROM live_provider_cache WHERE cache_key = $1")
            .bind(key.as_str())
            .fetch_one(&fixture.database.pool)
            .await?;
    assert!(!persisted.contains("artwork.example.invalid"));
    assert!(persisted.contains("elx-live:v1:"));

    let restarted = CatalogCacheRepository::new(fixture.database.pool.clone(), crypto);
    let fresh = restarted.get(&key, now).await?.expect("fresh cache row");
    assert_eq!(fresh.freshness, CacheFreshness::Fresh);
    let CatalogCacheValue::Catalog(page) = fresh.value.as_ref() else {
        panic!("catalog cache value");
    };
    assert_eq!(
        page.items[0].poster.as_ref().map(ArtworkSource::expose),
        Some("https://artwork.example.invalid/private/poster.jpg")
    );
    assert_eq!(
        restarted
            .get(&key, now + chrono::Duration::seconds(11))
            .await?
            .expect("stale cache row")
            .freshness,
        CacheFreshness::Stale
    );
    assert!(
        restarted
            .get(&key, now + chrono::Duration::seconds(31))
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn s12_cache_tampering_fails_closed() -> Result<()> {
    let fixture = sqlite_fixture().await?;
    let repository = CatalogCacheRepository::new(fixture.database.pool.clone(), crypto()?);
    let key = CacheKey::for_test("live-cache-v1:s12-tamper-test");
    let now = Utc::now();
    repository
        .put(&key, fixture.provider_id, &cache_value(), now)
        .await?;
    sqlx::query(
        "UPDATE live_provider_cache
         SET payload_json = REPLACE(payload_json, 'elx-live:v1:', 'elx-live:v1:X')
         WHERE cache_key = $1",
    )
    .bind(key.as_str())
    .execute(&fixture.database.pool)
    .await?;
    assert!(repository.get(&key, now).await.is_err());
    Ok(())
}

#[tokio::test]
async fn s12_visibility_and_grant_contraction_are_revisioned_and_transactional() -> Result<()> {
    let fixture = sqlite_fixture().await?;
    let repository = LiveProviderGrantRepository::new(fixture.database.pool.clone());
    assert!(
        repository
            .visibility(
                fixture.home_id,
                fixture.profile_id,
                HomeRole::Owner,
                ProfileType::Account,
                fixture.provider_id,
                LiveProviderAccess::Browse,
            )
            .await?
            .allowed
    );
    assert!(
        !repository
            .visibility(
                fixture.home_id,
                fixture.profile_id,
                HomeRole::Manager,
                ProfileType::Account,
                fixture.provider_id,
                LiveProviderAccess::Browse,
            )
            .await?
            .allowed
    );

    let browse = repository
        .set_grant(
            fixture.owner_id,
            r#"{"role":"owner"}"#,
            fixture.profile_id,
            fixture.provider_id,
            true,
            false,
            Some(1),
            None,
        )
        .await?;
    assert_eq!(browse.authorization_revision, 2);
    assert!(
        repository
            .visibility(
                fixture.home_id,
                fixture.profile_id,
                HomeRole::Manager,
                ProfileType::Account,
                fixture.provider_id,
                LiveProviderAccess::Browse,
            )
            .await?
            .allowed
    );
    assert!(
        !repository
            .visibility(
                fixture.home_id,
                fixture.profile_id,
                HomeRole::Manager,
                ProfileType::Account,
                fixture.provider_id,
                LiveProviderAccess::Play,
            )
            .await?
            .allowed
    );

    let play = repository
        .set_grant(
            fixture.owner_id,
            r#"{"role":"different-snapshot"}"#,
            fixture.profile_id,
            fixture.provider_id,
            true,
            true,
            Some(2),
            None,
        )
        .await?;
    assert_eq!(play.authorization_revision, 3);
    assert!(play.revocation_event_id.is_none());
    let snapshot: String = sqlx::query_scalar(
        "SELECT created_by_actor_snapshot FROM live_provider_grants WHERE profile_id = $1",
    )
    .bind(fixture.profile_id.to_string())
    .fetch_one(&fixture.database.pool)
    .await?;
    assert_eq!(snapshot, r#"{"role":"owner"}"#);

    let notifier = AuthorizationRevocationNotifier::new();
    let mut notifications = notifier.subscribe();
    let contraction = repository
        .set_grant(
            fixture.owner_id,
            r#"{"role":"owner"}"#,
            fixture.profile_id,
            fixture.provider_id,
            true,
            false,
            Some(3),
            Some(&notifier),
        )
        .await?;
    let event_id = contraction.revocation_event_id.expect("contraction event");
    assert_eq!(notifications.recv().await?, event_id);
    let outbox_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM authorization_revocation_outbox
         WHERE id = $1 AND event_type = 'provider_grant_revoked'
           AND profile_id = $2 AND provider_id = $3",
    )
    .bind(event_id.to_string())
    .bind(fixture.profile_id.to_string())
    .bind(fixture.provider_id.to_string())
    .fetch_one(&fixture.database.pool)
    .await?;
    assert_eq!(outbox_count, 1);
    Ok(())
}

#[tokio::test]
async fn s12_concurrent_grant_writers_use_authorization_revision_cas() -> Result<()> {
    let fixture = sqlite_fixture().await?;
    let repository = LiveProviderGrantRepository::new(fixture.database.pool.clone());
    repository
        .set_grant(
            fixture.owner_id,
            r#"{"role":"owner"}"#,
            fixture.profile_id,
            fixture.provider_id,
            true,
            false,
            Some(1),
            None,
        )
        .await?;
    let first = repository.clone();
    let second = repository.clone();
    let arguments = (fixture.owner_id, fixture.profile_id, fixture.provider_id);
    let (first, second) = tokio::join!(
        first.set_grant(
            arguments.0,
            r#"{"writer":1}"#,
            arguments.1,
            arguments.2,
            true,
            true,
            Some(2),
            None,
        ),
        second.set_grant(
            arguments.0,
            r#"{"writer":2}"#,
            arguments.1,
            arguments.2,
            false,
            false,
            Some(2),
            None,
        )
    );
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    assert!(matches!(
        first.as_ref().err().or(second.as_ref().err()),
        Some(LiveProviderGrantError::RevisionChanged)
    ));
    let revision: i64 = sqlx::query_scalar(
        "SELECT revision FROM profile_authorization_revisions WHERE profile_id = $1",
    )
    .bind(fixture.profile_id.to_string())
    .fetch_one(&fixture.database.pool)
    .await?;
    assert_eq!(revision, 3);
    Ok(())
}

fn coalesced_value() -> Arc<CatalogCacheValue> {
    Arc::new(CatalogCacheValue::Catalogs(CatalogSet {
        catalogs: Vec::new(),
        cache: CacheHint {
            max_age_seconds: 30,
            stale_while_revalidate_seconds: 30,
            etag: None,
        },
    }))
}

#[tokio::test]
async fn s12_coalescing_runs_one_loader_and_waiters_cancel_independently() -> Result<()> {
    let coalescer = CatalogRequestCoalescer::default();
    let key = CacheKey::for_test("live-cache-v1:s12-coalescing-test");
    let calls = Arc::new(AtomicUsize::new(0));
    let cancelled_waiter = CancellationToken::new();
    let first_cancel = cancelled_waiter.clone();
    let first_coalescer = coalescer.clone();
    let first_key = key.clone();
    let first_calls = calls.clone();
    let first = tokio::spawn(async move {
        first_coalescer
            .run(first_key, &first_cancel, move |_| async move {
                first_calls.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(40)).await;
                Ok(coalesced_value())
            })
            .await
    });
    tokio::time::sleep(Duration::from_millis(5)).await;
    let second_calls = calls.clone();
    let second = tokio::spawn(async move {
        let active_waiter = CancellationToken::new();
        coalescer
            .run(key, &active_waiter, move |_| async move {
                second_calls.fetch_add(1, Ordering::SeqCst);
                Ok(coalesced_value())
            })
            .await
    });
    cancelled_waiter.cancel();
    assert_eq!(first.await?, Err(CoalescedLoadError::Cancelled));
    assert!(second.await?.is_ok());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn s12_last_coalesced_waiter_cancels_upstream() -> Result<()> {
    let coalescer = CatalogRequestCoalescer::default();
    let cancellation = CancellationToken::new();
    let waiter_cancel = cancellation.clone();
    let (upstream_sender, upstream_receiver) = tokio::sync::oneshot::channel();
    let task_coalescer = coalescer.clone();
    let task = tokio::spawn(async move {
        task_coalescer
            .run(
                CacheKey::for_test("live-cache-v1:s12-last-waiter"),
                &waiter_cancel,
                move |upstream| async move {
                    upstream.cancelled().await;
                    let _ = upstream_sender.send(());
                    Err(CoalescedLoadError::Cancelled)
                },
            )
            .await
    });
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert_eq!(coalescer.active_flights(), 1);
    cancellation.cancel();
    assert_eq!(task.await?, Err(CoalescedLoadError::Cancelled));
    tokio::time::timeout(Duration::from_secs(1), upstream_receiver).await??;
    Ok(())
}

#[tokio::test]
async fn s12_circuit_breaker_is_scoped_and_allows_one_half_open_probe() {
    let provider = Uuid::new_v4();
    let circuits = ProviderCircuitBreakers::new(2, Duration::from_millis(20), 8);
    assert_eq!(
        circuits.admit(provider, CacheOperation::Catalog).await,
        CircuitAdmission::Allowed
    );
    circuits
        .record_failure(provider, CacheOperation::Catalog)
        .await;
    assert_eq!(
        circuits.admit(provider, CacheOperation::Catalog).await,
        CircuitAdmission::Allowed
    );
    circuits
        .record_failure(provider, CacheOperation::Catalog)
        .await;
    assert_eq!(
        circuits.admit(provider, CacheOperation::Catalog).await,
        CircuitAdmission::Open
    );
    assert_eq!(
        circuits.admit(provider, CacheOperation::Meta).await,
        CircuitAdmission::Allowed
    );
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(
        circuits.admit(provider, CacheOperation::Catalog).await,
        CircuitAdmission::Probe
    );
    assert_eq!(
        circuits.admit(provider, CacheOperation::Catalog).await,
        CircuitAdmission::Open
    );
    circuits
        .record_success(provider, CacheOperation::Catalog)
        .await;
    assert_eq!(
        circuits.admit(provider, CacheOperation::Catalog).await,
        CircuitAdmission::Allowed
    );
}

#[tokio::test]
async fn s12_catalog_service_aggregates_partial_provider_results_and_serves_pages() -> Result<()> {
    let fixture = NativeFixture::start().await?;
    let database = test_database().await?;
    let (_, healthy_provider) =
        seed_provider(&database, fixture.port(), serde_json::json!({})).await?;
    let (_, failing_provider) = seed_provider(
        &database,
        fixture.port(),
        serde_json::json!({"fixtureFault": "provider_error"}),
    )
    .await?;
    let (user_id, home_id, profile_id) = seed_owner_profile(&database).await?;
    let service = LiveCatalogService::new(
        database.pool.clone(),
        crypto()?,
        build_client(&database, None),
    );
    let context = LiveCatalogAccessContext {
        user_id,
        home_id,
        profile_id,
        role: HomeRole::Owner,
        profile_type: ProfileType::Account,
        authorization_revision: 1,
        can_browse_live: true,
        locale: "en-US".to_string(),
        timezone: "America/Chicago".to_string(),
        now: Utc.with_ymd_and_hms(2026, 7, 12, 12, 0, 0).unwrap(),
    };
    let cancellation = CancellationToken::new();
    let catalogs = service.catalogs(&context, &cancellation).await?;
    assert_eq!(catalogs.providers.len(), 1);
    assert_eq!(catalogs.providers[0].provider_id, healthy_provider);
    assert_eq!(catalogs.providers[0].catalogs.catalogs.len(), 2);
    assert_eq!(catalogs.errors.len(), 1);
    assert_eq!(catalogs.errors[0].provider_id, failing_provider);
    assert_eq!(catalogs.errors[0].code, "provider_reported_failure");

    let page = service
        .catalog(
            &context,
            healthy_provider,
            CatalogPageRequest {
                catalog_id: "events".to_string(),
                cursor: None,
                limit: 2,
                filters: std::collections::BTreeMap::from([(
                    "category".to_string(),
                    FilterValue::Multiple(vec!["sports".to_string()]),
                )]),
            },
            &cancellation,
        )
        .await?;
    assert_eq!(page.page.items.len(), 2);
    assert_eq!(page.freshness, CacheFreshness::Fresh);
    let cached = service
        .catalog(
            &context,
            healthy_provider,
            CatalogPageRequest {
                catalog_id: "events".to_string(),
                cursor: None,
                limit: 2,
                filters: std::collections::BTreeMap::from([(
                    "category".to_string(),
                    FilterValue::Multiple(vec!["sports".to_string()]),
                )]),
            },
            &cancellation,
        )
        .await?;
    assert_eq!(cached.page, page.page);
    let metadata = service
        .meta(
            &context,
            healthy_provider,
            MetaRequest {
                item_id: "event-live".to_string(),
            },
            &cancellation,
        )
        .await?;
    assert!(!metadata.metadata.streams.is_empty());
    fixture.stop().await?;
    Ok(())
}

#[tokio::test]
async fn s12_postgres_cache_restart_and_concurrent_grants_when_configured() -> Result<()> {
    let Ok(url) = std::env::var("ELIXIR_TEST_POSTGRES_EMPTY_DATABASE_URL") else {
        return Ok(());
    };
    let config = DatabaseConfig {
        url,
        max_connections: 8,
        connect_timeout_seconds: 5,
    };
    let database = Database::connect(&config).await?;
    database.run_migrations().await?;
    let fixture = seed_fixture(database).await?;
    let repository = LiveProviderGrantRepository::new(fixture.database.pool.clone());
    repository
        .set_grant(
            fixture.owner_id,
            r#"{"role":"owner"}"#,
            fixture.profile_id,
            fixture.provider_id,
            true,
            false,
            Some(1),
            None,
        )
        .await?;
    let first = repository.clone();
    let second = repository.clone();
    let (first, second) = tokio::join!(
        first.set_grant(
            fixture.owner_id,
            r#"{"writer":1}"#,
            fixture.profile_id,
            fixture.provider_id,
            true,
            true,
            Some(2),
            None,
        ),
        second.set_grant(
            fixture.owner_id,
            r#"{"writer":2}"#,
            fixture.profile_id,
            fixture.provider_id,
            false,
            false,
            Some(2),
            None,
        )
    );
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    assert!(matches!(
        first.as_ref().err().or(second.as_ref().err()),
        Some(LiveProviderGrantError::RevisionChanged)
    ));

    let key = CacheKey::for_test("live-cache-v1:s12-postgres-restart");
    let now = Utc::now();
    CatalogCacheRepository::new(fixture.database.pool.clone(), crypto()?)
        .put(&key, fixture.provider_id, &cache_value(), now)
        .await?;
    fixture.database.pool.close().await;
    let restarted = Database::connect(&config).await?;
    restarted.run_migrations().await?;
    let entry = CatalogCacheRepository::new(restarted.pool.clone(), crypto()?)
        .get(&key, now)
        .await?
        .expect("PostgreSQL cache survives reconnect");
    assert_eq!(entry.freshness, CacheFreshness::Fresh);
    Ok(())
}
