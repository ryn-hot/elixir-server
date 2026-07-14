use std::sync::Arc;

use anyhow::Result;
use chrono::{Duration, TimeZone, Utc};
use sqlx::Row;
use uuid::Uuid;

use crate::{
    config::DatabaseConfig,
    db::Database,
    live::{
        config::LiveSessionLimits,
        crypto::{LiveCrypto, LiveMasterKey, SecretBytes},
    },
};

use super::{
    DeliveryMode, IdempotencyRequest, LiveSessionRepository, NewSession, RecoveryAction,
    SessionOwner, SessionProtocol, SessionRecoveryFailure, SessionRecoveryReplacement,
    SessionRepositoryError, SessionState, TerminalReason,
};

pub(crate) struct Fixture {
    pub(crate) database: Database,
    pub(crate) owner: SessionOwner,
    pub(crate) now: chrono::DateTime<Utc>,
}

pub(crate) async fn sqlite_fixture() -> Result<Fixture> {
    let database = Database::connect(&DatabaseConfig {
        url: format!(
            "sqlite:file:p10-live-session-{}?mode=memory&cache=shared",
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
    let now = Utc.with_ymd_and_hms(2026, 7, 12, 18, 0, 0).unwrap();
    let user_id = Uuid::new_v4();
    let home_id = Uuid::new_v4();
    let profile_id = Uuid::new_v4();
    let account_session_id = Uuid::new_v4();
    let instance_id = Uuid::new_v4();
    let provider_id = Uuid::new_v4();
    let extension_id = format!("dev.elixir.p10.{}", Uuid::new_v4().simple());

    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, 'test-hash')")
        .bind(user_id.to_string())
        .bind(format!("{user_id}@example.invalid"))
        .execute(&database.pool)
        .await?;
    sqlx::query("INSERT INTO homes (id, owner_user_id, name) VALUES ($1, $2, 'P10 Home')")
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
        "INSERT INTO account_sessions (
            id, user_id, home_id, active_profile_id, expires_at
         ) VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(account_session_id.to_string())
    .bind(user_id.to_string())
    .bind(home_id.to_string())
    .bind(profile_id.to_string())
    .bind((now + Duration::days(31)).to_rfc3339())
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "INSERT INTO extensions (extension_id, name, version, kind, trust_level, manifest_json)
         VALUES ($1, 'P10', '1.0.0', 'connector', 'verified', '{}')",
    )
    .bind(&extension_id)
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "INSERT INTO extension_instances (instance_id, extension_id, instance_name)
         VALUES ($1, $2, 'default')",
    )
    .bind(instance_id.to_string())
    .bind(extension_id)
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
    sqlx::query(
        "UPDATE live_control_server_leases
         SET owner_instance_id = 'p10-a', fencing_token = 1,
             acquired_at = $1, heartbeat_at = $1, expires_at = $2
         WHERE lease_name = 'live-control-v1' AND fencing_token = 0",
    )
    .bind(now.to_rfc3339())
    .bind((now + Duration::days(30)).to_rfc3339())
    .execute(&database.pool)
    .await?;

    Ok(Fixture {
        database,
        owner: SessionOwner {
            user_id,
            home_id,
            profile_id,
            account_session_id,
            provider_id,
        },
        now,
    })
}

fn crypto(key_id: &str, material: u8) -> Result<Arc<LiveCrypto>> {
    Ok(Arc::new(LiveCrypto::new(
        key_id,
        [LiveMasterKey::new(key_id, [material; 32])?],
    )?))
}

fn limits() -> LiveSessionLimits {
    LiveSessionLimits {
        per_user: 3,
        server_total: 10,
        lease_seconds: 90,
        max_lifetime_seconds: 180,
        startup_queue_seconds: 15,
    }
}

pub(crate) fn repository(fixture: &Fixture) -> Result<LiveSessionRepository> {
    Ok(LiveSessionRepository::new(
        fixture.database.pool.clone(),
        crypto("p10-key", 10)?,
        limits(),
    ))
}

fn new_session(owner: SessionOwner, now: chrono::DateTime<Utc>) -> NewSession {
    NewSession {
        owner,
        item_key: SecretBytes::from_utf8("provider-item-secret".to_string()),
        stream_option_key: SecretBytes::from_utf8("provider-stream-secret".to_string()),
        item_snapshot: SecretBytes::from_utf8("{\"title\":\"Championship\"}".to_string()),
        descriptor: SecretBytes::from_utf8(
            "{\"url\":\"https://upstream.invalid/live?token=secret\"}".to_string(),
        ),
        delivery_mode: DeliveryMode::ServerRelay,
        protocol: SessionProtocol::Hls,
        source_index: 0,
        control_fencing_token: 1,
        now,
    }
}

fn idempotency(key: &str, request: &str) -> IdempotencyRequest {
    IdempotencyRequest {
        key: SecretBytes::from_utf8(key.to_string()),
        request_identity: SecretBytes::from_utf8(request.to_string()),
    }
}

#[tokio::test]
async fn p10_create_replays_exact_request_and_persists_no_plaintext_secret() -> Result<()> {
    let fixture = sqlite_fixture().await?;
    let repository = repository(&fixture)?;
    let first = repository
        .create(
            new_session(fixture.owner, fixture.now),
            Some(idempotency("create-one", "request-a")),
        )
        .await?;
    assert!(!first.replayed);
    let first_token = first.token.expose_secret().to_string();
    let replay = repository
        .create(
            new_session(fixture.owner, fixture.now),
            Some(idempotency("create-one", "request-a")),
        )
        .await?;
    assert!(replay.replayed);
    assert_eq!(replay.session.id, first.session.id);
    assert_eq!(replay.token.expose_secret(), first_token);
    assert!(matches!(
        repository
            .create(
                new_session(fixture.owner, fixture.now),
                Some(idempotency("create-one", "different-request")),
            )
            .await,
        Err(SessionRepositoryError::IdempotencyConflict)
    ));

    let row = sqlx::query(
        "SELECT item_key_hash, stream_option_key_hash, token_hash,
                encrypted_item_snapshot, encrypted_descriptor
         FROM live_playback_sessions WHERE id = $1",
    )
    .bind(first.session.id.to_string())
    .fetch_one(&fixture.database.pool)
    .await?;
    let durable = [
        row.try_get::<String, _>("item_key_hash")?,
        row.try_get::<String, _>("stream_option_key_hash")?,
        row.try_get::<String, _>("token_hash")?,
        row.try_get::<String, _>("encrypted_item_snapshot")?,
        row.try_get::<String, _>("encrypted_descriptor")?,
    ]
    .join("\n");
    for secret in [
        "provider-item-secret",
        "provider-stream-secret",
        "Championship",
        "upstream.invalid",
        first_token.as_str(),
        "create-one",
    ] {
        assert!(!durable.contains(secret));
    }
    assert!(durable.contains("elx-live-token-hash:v1:"));
    assert!(durable.contains("elx-live:v1:"));
    let secrets = repository
        .decrypt_secrets(fixture.owner, first.session.id)
        .await?;
    assert_eq!(
        secrets.descriptor.expose_secret(),
        b"{\"url\":\"https://upstream.invalid/live?token=secret\"}"
    );
    assert_eq!(
        repository
            .verify_delivery_token(first.session.id, &first_token, fixture.now)
            .await?
            .id,
        first.session.id
    );
    Ok(())
}

#[tokio::test]
async fn p10_state_cas_heartbeat_and_fence_takeover_are_strict() -> Result<()> {
    let fixture = sqlite_fixture().await?;
    let repository = repository(&fixture)?;
    let grant = repository
        .create(new_session(fixture.owner, fixture.now), None)
        .await?;
    let planning = repository
        .transition(
            fixture.owner,
            grant.session.id,
            1,
            1,
            SessionState::Planning,
            fixture.now,
        )
        .await?;
    assert_eq!(planning.session.revision, 2);
    assert!(matches!(
        repository
            .transition(
                fixture.owner,
                grant.session.id,
                1,
                1,
                SessionState::Ready,
                fixture.now,
            )
            .await,
        Err(SessionRepositoryError::RevisionChanged)
    ));
    assert!(matches!(
        repository
            .transition(
                fixture.owner,
                grant.session.id,
                2,
                1,
                SessionState::Playing,
                fixture.now,
            )
            .await,
        Err(SessionRepositoryError::InvalidTransition)
    ));
    let heartbeat = repository
        .heartbeat(
            fixture.owner,
            grant.session.id,
            2,
            1,
            fixture.now + Duration::seconds(80),
        )
        .await?;
    assert_eq!(
        heartbeat.session.expires_at,
        fixture.now + Duration::seconds(170)
    );
    assert!(
        heartbeat
            .session
            .needs_rollover(fixture.now + Duration::seconds(100), 90)
    );

    sqlx::query(
        "UPDATE live_control_server_leases
         SET owner_instance_id = 'p10-b', fencing_token = 2,
             acquired_at = $1, heartbeat_at = $1, expires_at = $2
         WHERE lease_name = 'live-control-v1' AND fencing_token = 1",
    )
    .bind((fixture.now + Duration::seconds(101)).to_rfc3339())
    .bind((fixture.now + Duration::days(30)).to_rfc3339())
    .execute(&fixture.database.pool)
    .await?;
    assert!(matches!(
        repository
            .heartbeat(
                fixture.owner,
                grant.session.id,
                3,
                1,
                fixture.now + Duration::seconds(101),
            )
            .await,
        Err(SessionRepositoryError::FenceLost)
    ));
    let adopted = repository
        .adopt_control_fence(
            fixture.owner,
            grant.session.id,
            3,
            1,
            2,
            fixture.now + Duration::seconds(101),
        )
        .await?;
    assert_eq!(adopted.session.control_fencing_token, 2);
    assert_eq!(adopted.session.revision, 4);
    Ok(())
}

#[tokio::test]
async fn p10_token_rotation_updates_replay_and_terminal_cleanup_destroys_secrets() -> Result<()> {
    let fixture = sqlite_fixture().await?;
    let repository = repository(&fixture)?;
    let grant = repository
        .create(
            new_session(fixture.owner, fixture.now),
            Some(idempotency("rotate-one", "request-a")),
        )
        .await?;
    let old_token = grant.token.expose_secret().to_string();
    let rotated = repository
        .rotate_delivery_token(fixture.owner, grant.session.id, 1, 1, fixture.now)
        .await?;
    assert_ne!(rotated.token.expose_secret(), old_token);
    let replay = repository
        .create(
            new_session(fixture.owner, fixture.now),
            Some(idempotency("rotate-one", "request-a")),
        )
        .await?;
    assert_eq!(replay.token.expose_secret(), rotated.token.expose_secret());
    assert!(
        repository
            .verify_delivery_token(grant.session.id, &old_token, fixture.now)
            .await
            .is_err()
    );
    repository
        .verify_delivery_token(grant.session.id, rotated.token.expose_secret(), fixture.now)
        .await?;

    sqlx::query("UPDATE live_playback_sessions SET remux_job_id = 'job-secret' WHERE id = $1")
        .bind(grant.session.id.to_string())
        .execute(&fixture.database.pool)
        .await?;
    let ended = repository
        .terminate(
            fixture.owner,
            grant.session.id,
            2,
            1,
            TerminalReason::ended(),
            fixture.now + Duration::seconds(1),
        )
        .await?;
    assert_eq!(ended.session.state, SessionState::Ended);
    assert!(ended.session.remux_job_id.is_none());
    assert!(matches!(
        repository
            .decrypt_secrets(fixture.owner, grant.session.id)
            .await,
        Err(SessionRepositoryError::Expired)
    ));
    assert!(
        repository
            .verify_delivery_token(
                grant.session.id,
                rotated.token.expose_secret(),
                fixture.now + Duration::seconds(1),
            )
            .await
            .is_err()
    );
    let replay_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM live_session_idempotency WHERE session_id = $1")
            .bind(grant.session.id.to_string())
            .fetch_one(&fixture.database.pool)
            .await?;
    assert_eq!(replay_count, 0);
    repository
        .terminate(
            fixture.owner,
            grant.session.id,
            1,
            1,
            TerminalReason::ended(),
            fixture.now + Duration::seconds(2),
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn r20_recovery_atomically_replaces_descriptor_token_replay_and_rejects_late_results()
-> Result<()> {
    let fixture = sqlite_fixture().await?;
    let repository = repository(&fixture)?;
    let grant = repository
        .create(
            new_session(fixture.owner, fixture.now),
            Some(idempotency("r20-recovery", "request-a")),
        )
        .await?;
    let old_token = grant.token.expose_secret().to_string();
    let planning = repository
        .transition(
            fixture.owner,
            grant.session.id,
            1,
            1,
            SessionState::Planning,
            fixture.now,
        )
        .await?;
    let ready = repository
        .transition(
            fixture.owner,
            grant.session.id,
            planning.session.revision,
            1,
            SessionState::Ready,
            fixture.now,
        )
        .await?;
    let replacement_secret = "{\"generation\":2,\"url\":\"https://new.invalid/live?secret=new\"}";
    let replaced = repository
        .replace_for_recovery(SessionRecoveryReplacement {
            owner: fixture.owner,
            session_id: grant.session.id,
            expected_revision: ready.session.revision,
            control_fencing_token: 1,
            descriptor: SecretBytes::from_utf8(replacement_secret.to_string()),
            delivery_mode: DeliveryMode::ServerRelay,
            protocol: SessionProtocol::HttpProgressive,
            source_index: 1,
            action: RecoveryAction::Refresh,
            now: fixture.now + Duration::seconds(1),
        })
        .await?;
    assert_eq!(replaced.session.revision, 5);
    assert_eq!(replaced.session.token_revision, 2);
    assert_eq!(replaced.session.refresh_count, 1);
    assert_eq!(replaced.session.failover_count, 0);
    assert_eq!(replaced.session.source_index, 1);
    assert_eq!(replaced.session.protocol, SessionProtocol::HttpProgressive);
    assert_eq!(replaced.session.state, SessionState::Ready);
    assert_ne!(replaced.token.expose_secret(), old_token);
    assert!(
        repository
            .verify_delivery_token(grant.session.id, &old_token, fixture.now)
            .await
            .is_err()
    );
    repository
        .verify_delivery_token(
            grant.session.id,
            replaced.token.expose_secret(),
            fixture.now + Duration::seconds(1),
        )
        .await?;
    let replay = repository
        .lookup_idempotency(
            fixture.owner,
            &idempotency("r20-recovery", "request-a"),
            fixture.now + Duration::seconds(1),
        )
        .await?
        .expect("unexpired replay");
    assert_eq!(replay.token.expose_secret(), replaced.token.expose_secret());
    assert_eq!(
        repository
            .decrypt_secrets(fixture.owner, grant.session.id)
            .await?
            .descriptor
            .expose_secret(),
        replacement_secret.as_bytes()
    );

    let failure_secret = "{\"generation\":2,\"recovery\":\"failed\"}";
    let failed = repository
        .record_recovery_failure(SessionRecoveryFailure {
            owner: fixture.owner,
            session_id: grant.session.id,
            expected_revision: replaced.session.revision,
            control_fencing_token: 1,
            descriptor: SecretBytes::from_utf8(failure_secret.to_string()),
            action: RecoveryAction::Failover,
            now: fixture.now + Duration::seconds(2),
        })
        .await?;
    assert_eq!(failed.session.revision, 7);
    assert_eq!(failed.session.state, SessionState::Ready);
    assert_eq!(failed.session.token_revision, 2);
    assert_eq!(failed.session.refresh_count, 1);
    assert_eq!(failed.session.failover_count, 1);
    repository
        .verify_delivery_token(
            grant.session.id,
            replaced.token.expose_secret(),
            fixture.now + Duration::seconds(2),
        )
        .await?;
    assert_eq!(
        repository
            .decrypt_secrets(fixture.owner, grant.session.id)
            .await?
            .descriptor
            .expose_secret(),
        failure_secret.as_bytes()
    );

    let stale = repository
        .replace_for_recovery(SessionRecoveryReplacement {
            owner: fixture.owner,
            session_id: grant.session.id,
            expected_revision: replaced.session.revision,
            control_fencing_token: 1,
            descriptor: SecretBytes::from_utf8("late-secret".to_string()),
            delivery_mode: DeliveryMode::ServerRelay,
            protocol: SessionProtocol::Hls,
            source_index: 0,
            action: RecoveryAction::Failover,
            now: fixture.now + Duration::seconds(2),
        })
        .await;
    assert!(matches!(
        stale,
        Err(SessionRepositoryError::RevisionChanged)
    ));

    let ended = repository
        .terminate(
            fixture.owner,
            grant.session.id,
            failed.session.revision,
            1,
            TerminalReason::ended(),
            fixture.now + Duration::seconds(3),
        )
        .await?;
    assert_eq!(ended.session.state, SessionState::Ended);
    let late_after_end = repository
        .record_recovery_failure(SessionRecoveryFailure {
            owner: fixture.owner,
            session_id: grant.session.id,
            expected_revision: ended.previous_revision,
            control_fencing_token: 1,
            descriptor: SecretBytes::from_utf8("late-failure-secret".to_string()),
            action: RecoveryAction::Refresh,
            now: fixture.now + Duration::seconds(4),
        })
        .await;
    assert!(matches!(
        late_after_end,
        Err(SessionRepositoryError::Expired | SessionRepositoryError::RevisionChanged)
    ));
    let durable: String =
        sqlx::query_scalar("SELECT encrypted_descriptor FROM live_playback_sessions WHERE id = $1")
            .bind(grant.session.id.to_string())
            .fetch_one(&fixture.database.pool)
            .await?;
    assert!(!durable.contains("late-secret"));
    assert!(!durable.contains("late-failure-secret"));
    Ok(())
}

#[tokio::test]
async fn p10_concurrent_duplicate_coalesces_before_capacity_is_consumed() -> Result<()> {
    let fixture = sqlite_fixture().await?;
    let mut one_limit = limits();
    one_limit.per_user = 1;
    let repository = Arc::new(LiveSessionRepository::new(
        fixture.database.pool.clone(),
        crypto("p10-key", 10)?,
        one_limit,
    ));
    let first_repository = Arc::clone(&repository);
    let second_repository = Arc::clone(&repository);
    let owner = fixture.owner;
    let now = fixture.now;
    let first = tokio::spawn(async move {
        first_repository
            .create(
                new_session(owner, now),
                Some(idempotency("concurrent", "same-request")),
            )
            .await
    });
    let second = tokio::spawn(async move {
        second_repository
            .create(
                new_session(owner, now),
                Some(idempotency("concurrent", "same-request")),
            )
            .await
    });
    let first = first.await??;
    let second = second.await??;
    assert_eq!(first.session.id, second.session.id);
    assert_ne!(first.replayed, second.replayed);
    assert!(matches!(
        repository
            .create(
                new_session(fixture.owner, fixture.now),
                Some(idempotency("another", "another-request")),
            )
            .await,
        Err(SessionRepositoryError::Capacity)
    ));
    Ok(())
}

#[tokio::test]
async fn p10_cleanup_is_bounded_and_retains_terminal_diagnostics_for_seven_days() -> Result<()> {
    let fixture = sqlite_fixture().await?;
    let repository = repository(&fixture)?;
    let grant = repository
        .create(new_session(fixture.owner, fixture.now), None)
        .await?;
    repository
        .terminate(
            fixture.owner,
            grant.session.id,
            1,
            1,
            TerminalReason::ended(),
            fixture.now,
        )
        .await?;
    let expiring = repository
        .create(
            new_session(fixture.owner, fixture.now + Duration::seconds(1)),
            None,
        )
        .await?;
    let expiry_cleanup = repository
        .cleanup(fixture.now + Duration::seconds(182), 1, 10)
        .await?;
    assert_eq!(expiry_cleanup.expired_sessions, 1);
    assert_eq!(
        repository
            .get_owned(fixture.owner, expiring.session.id)
            .await?
            .expect("expired diagnostic row")
            .state,
        SessionState::Expired
    );
    let early = repository
        .cleanup(fixture.now + Duration::days(6), 1, 10)
        .await?;
    assert_eq!(early.purged_terminal_sessions, 0);
    assert!(
        repository
            .get_owned(fixture.owner, grant.session.id)
            .await?
            .is_some()
    );
    let late = repository
        .cleanup(fixture.now + Duration::days(8), 1, 10)
        .await?;
    assert_eq!(late.purged_terminal_sessions, 2);
    assert!(
        repository
            .get_owned(fixture.owner, grant.session.id)
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn p10_envelope_rotation_reencrypts_and_token_key_rotation_terminates() -> Result<()> {
    let fixture = sqlite_fixture().await?;
    let old_crypto = crypto("old", 1)?;
    let old_repository = LiveSessionRepository::new(
        fixture.database.pool.clone(),
        Arc::clone(&old_crypto),
        limits(),
    );
    let grant = old_repository
        .create(
            new_session(fixture.owner, fixture.now),
            Some(idempotency("key-rotation", "request")),
        )
        .await?;
    let envelope_rotating = Arc::new(LiveCrypto::new_with_domain_keys(
        "new",
        [
            LiveMasterKey::new("old", [1; 32])?,
            LiveMasterKey::new("new", [2; 32])?,
        ],
        "old",
        [LiveMasterKey::new("old", [1; 32])?],
    )?);
    let envelope_repository =
        LiveSessionRepository::new(fixture.database.pool.clone(), envelope_rotating, limits());
    let report = envelope_repository
        .rotate_encryption_keys(fixture.now, 1, 10)
        .await?;
    assert_eq!(report.reencrypted_sessions, 1);
    assert_eq!(report.reencrypted_replays, 1);
    envelope_repository
        .decrypt_secrets(fixture.owner, grant.session.id)
        .await?;

    let token_rotating = Arc::new(LiveCrypto::new_with_domain_keys(
        "new",
        [
            LiveMasterKey::new("old", [1; 32])?,
            LiveMasterKey::new("new", [2; 32])?,
        ],
        "new",
        [
            LiveMasterKey::new("old", [1; 32])?,
            LiveMasterKey::new("new", [2; 32])?,
        ],
    )?);
    let token_repository =
        LiveSessionRepository::new(fixture.database.pool.clone(), token_rotating, limits());
    let report = token_repository
        .rotate_encryption_keys(fixture.now, 1, 10)
        .await?;
    assert_eq!(report.terminated_for_token_key_rotation, 1);
    assert_eq!(
        token_repository
            .get_owned(fixture.owner, grant.session.id)
            .await?
            .expect("retained terminal row")
            .state,
        SessionState::Failed
    );
    Ok(())
}

#[tokio::test]
async fn p10_postgres_session_lifecycle_when_configured() -> Result<()> {
    let Ok(url) = std::env::var("ELIXIR_TEST_POSTGRES_EMPTY_DATABASE_URL") else {
        return Ok(());
    };
    let database = Database::connect(&DatabaseConfig {
        url,
        max_connections: 8,
        connect_timeout_seconds: 5,
    })
    .await?;
    database.run_migrations().await?;
    let fixture = seed_fixture(database).await?;
    let repository = repository(&fixture)?;
    let created = repository
        .create(
            new_session(fixture.owner, fixture.now),
            Some(idempotency("postgres", "request")),
        )
        .await?;
    let replay = repository
        .create(
            new_session(fixture.owner, fixture.now),
            Some(idempotency("postgres", "request")),
        )
        .await?;
    assert_eq!(created.session.id, replay.session.id);
    let rotated = repository
        .rotate_delivery_token(fixture.owner, created.session.id, 1, 1, fixture.now)
        .await?;
    repository
        .terminate(
            fixture.owner,
            created.session.id,
            rotated.session.revision,
            1,
            TerminalReason::ended(),
            fixture.now,
        )
        .await?;
    Ok(())
}
