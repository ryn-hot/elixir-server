use std::sync::Arc;

use anyhow::Result;
use chrono::{Duration, Utc};
use elixir_server::{
    auth::home_profiles::{HomeProfileRepository, HomeRole},
    config::DatabaseConfig,
    db::Database,
    live::{
        admin::{ActorSnapshot, LiveAuditChain, LiveAuditKey},
        egress::{
            EffectiveEgressPolicy, EgressPolicyMode, EgressPolicyRepository,
            EgressPolicyRepositoryError, EgressPolicySource, PolicyScope,
        },
        metrics,
        session::{DeliveryMode, SessionOwner, SessionProtocol, SessionRecord, SessionState},
    },
};
use prometheus::{Encoder, Registry, TextEncoder};
use uuid::Uuid;

#[test]
fn o10_live_metrics_register_with_bounded_privacy_safe_labels() -> Result<()> {
    let registry = Registry::new();
    metrics::register(&registry);
    metrics::PROVIDER_REQUESTS
        .with_label_values(&["health", "success"])
        .inc_by(0);
    metrics::PROVIDER_REQUEST_DURATION
        .with_label_values(&["health", "success"])
        .observe(0.0);
    metrics::PROVIDER_CONTRACT_FAILURES
        .with_label_values(&["invalid_shape"])
        .inc_by(0);
    metrics::CATALOG_CACHE
        .with_label_values(&["fresh"])
        .inc_by(0);
    metrics::SESSIONS_ACTIVE
        .with_label_values(&["server_relay", "hls", "server_default"])
        .set(0);
    metrics::SESSIONS_STARTED
        .with_label_values(&["server_relay", "hls", "started"])
        .inc_by(0);
    metrics::SESSION_START_DURATION
        .with_label_values(&["server_relay"])
        .observe(0.0);
    metrics::RELAY_REQUESTS
        .with_label_values(&["manifest", "success"])
        .inc_by(0);
    metrics::RELAY_UPSTREAM_BYTES
        .with_label_values(&["manifest"])
        .inc_by(0);
    metrics::RELAY_CLIENT_BYTES
        .with_label_values(&["manifest"])
        .inc_by(0);
    metrics::REMUX_JOBS_ACTIVE
        .with_label_values(&["mpeg_ts_copy"])
        .set(0);
    metrics::REMUX_JOBS
        .with_label_values(&["mpeg_ts_copy", "completed"])
        .inc_by(0);
    metrics::RECONNECTS
        .with_label_values(&["transport", "succeeded"])
        .inc_by(0);
    metrics::REFRESHES
        .with_label_values(&["expiry_threshold", "succeeded"])
        .inc_by(0);
    metrics::FAILOVERS
        .with_label_values(&["transport", "succeeded"])
        .inc_by(0);
    metrics::EGRESS_BINDINGS_ACTIVE
        .with_label_values(&["wireguard"])
        .set(0);
    metrics::CLEANUP
        .with_label_values(&["session", "completed"])
        .inc_by(0);
    metrics::ADMISSION_REJECTIONS
        .with_label_values(&["relay", "capacity_exhausted"])
        .inc_by(0);

    let families = registry.gather();
    let mut encoded = Vec::new();
    TextEncoder::new().encode(&families, &mut encoded)?;
    let encoded = String::from_utf8(encoded)?;
    for name in [
        "live_provider_requests_total",
        "live_provider_request_duration_seconds",
        "live_provider_contract_failures_total",
        "live_catalog_cache_total",
        "live_sessions_active",
        "live_sessions_started_total",
        "live_session_start_duration_seconds",
        "live_relay_requests_total",
        "live_relay_upstream_bytes_total",
        "live_relay_client_bytes_total",
        "live_remux_jobs_active",
        "live_remux_jobs_total",
        "live_reconnects_total",
        "live_refreshes_total",
        "live_failovers_total",
        "live_egress_bindings_active",
        "live_cleanup_total",
        "live_admission_rejections_total",
    ] {
        assert!(
            encoded.contains(&format!("# HELP {name} ")),
            "missing {name}"
        );
    }
    for forbidden in [
        "user_id=",
        "session_id=",
        "provider_id=",
        "item_id=",
        "title=",
        "url=",
        "host=",
        "query=",
        "source_label=",
        "extension_id=",
    ] {
        assert!(!encoded.contains(forbidden), "forbidden label {forbidden}");
    }
    Ok(())
}

#[tokio::test]
async fn n11_repository_cas_scope_and_audit_are_atomic() -> Result<()> {
    let database = Database::connect(&DatabaseConfig {
        url: format!(
            "sqlite:file:/tmp/n11-live-egress-{}?mode=memory&cache=shared",
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
        .bind(format!("{user_id}@n11.example"))
        .bind("hashed")
        .execute(&database.pool)
        .await?;
    let owner = HomeProfileRepository::new(&database.pool)
        .ensure_owner_home(user_id)
        .await?;
    let actor = ActorSnapshot::new(user_id, "N11 Owner", HomeRole::Owner)?;
    let audit = Arc::new(LiveAuditChain::new(LiveAuditKey::new(
        "n11-audit-test",
        [83_u8; 32],
    )?));
    let repository = EgressPolicyRepository::new(database.pool.clone());

    let created = repository
        .upsert_audited(
            owner.home.id,
            PolicyScope::ServerDefault,
            EgressPolicyMode::PreferProtected,
            Some("live-egress-test"),
            true,
            0,
            &actor,
            &audit,
            Utc::now(),
        )
        .await?;
    assert_eq!(created.revision, 1);
    assert!(created.allow_fallback);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM live_admin_audit_events
             WHERE action = 'egress_policy_set'"
        )
        .fetch_one(&database.pool)
        .await?,
        1
    );

    sqlx::query(
        "CREATE TRIGGER n11_reject_egress_audit
         BEFORE INSERT ON live_admin_audit_events
         BEGIN
             SELECT RAISE(ABORT, 'injected egress audit failure');
         END",
    )
    .execute(&database.pool)
    .await?;
    let failed = repository
        .upsert_audited(
            owner.home.id,
            PolicyScope::ServerDefault,
            EgressPolicyMode::RequireProtected,
            Some("live-egress-test"),
            false,
            1,
            &actor,
            &audit,
            Utc::now(),
        )
        .await;
    assert!(matches!(failed, Err(EgressPolicyRepositoryError::Audit(_))));
    let unchanged = repository
        .assignments_for_home(owner.home.id)
        .await?
        .pop()
        .expect("server assignment");
    assert_eq!(unchanged.revision, 1);
    assert_eq!(unchanged.mode, EgressPolicyMode::PreferProtected);

    sqlx::query("DROP TRIGGER n11_reject_egress_audit")
        .execute(&database.pool)
        .await?;
    let updated = repository
        .upsert_audited(
            owner.home.id,
            PolicyScope::ServerDefault,
            EgressPolicyMode::RequireProtected,
            Some("live-egress-test"),
            false,
            1,
            &actor,
            &audit,
            Utc::now(),
        )
        .await?;
    assert_eq!(updated.revision, 2);
    assert_eq!(updated.mode, EgressPolicyMode::RequireProtected);

    let stale = repository
        .upsert(
            owner.home.id,
            PolicyScope::ServerDefault,
            EgressPolicyMode::Off,
            None,
            false,
            1,
            user_id,
            Utc::now(),
        )
        .await;
    assert!(matches!(
        stale,
        Err(EgressPolicyRepositoryError::RevisionChanged)
    ));
    let foreign_profile = repository
        .upsert(
            owner.home.id,
            PolicyScope::Profile(Uuid::new_v4()),
            EgressPolicyMode::Off,
            None,
            false,
            0,
            user_id,
            Utc::now(),
        )
        .await;
    assert!(matches!(
        foreign_profile,
        Err(EgressPolicyRepositoryError::ScopeForbidden)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM live_admin_audit_events
             WHERE action = 'egress_policy_set'"
        )
        .fetch_one(&database.pool)
        .await?,
        2
    );
    Ok(())
}

#[tokio::test]
async fn n11_direct_fallback_audit_is_atomic_and_fail_closed() -> Result<()> {
    let database = Database::connect(&DatabaseConfig {
        url: format!(
            "sqlite:file:/tmp/n11-live-fallback-{}?mode=memory&cache=shared",
            Uuid::new_v4()
        ),
        max_connections: 1,
        connect_timeout_seconds: 5,
    })
    .await?;
    database.run_migrations().await?;
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&database.pool)
        .await?;

    let now = Utc::now();
    let owner = SessionOwner {
        user_id: Uuid::new_v4(),
        home_id: Uuid::new_v4(),
        profile_id: Uuid::new_v4(),
        account_session_id: Uuid::new_v4(),
        provider_id: Uuid::new_v4(),
    };
    let session_id = Uuid::new_v4();
    let binding_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO live_playback_sessions (
            id, user_id, home_id, profile_id, account_session_id, provider_id,
            item_key_hash, stream_option_key_hash, encrypted_item_snapshot,
            delivery_mode, protocol, state, revision, token_revision,
            control_fencing_token, token_hash, encrypted_descriptor, source_index,
            egress_binding_id, expires_at, hard_expires_at
         ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9,
            'server_relay', 'hls', 'provisioning_egress', 3, 1,
            9, $10, $11, 0, $12, $13, $14
         )",
    )
    .bind(session_id.to_string())
    .bind(owner.user_id.to_string())
    .bind(owner.home_id.to_string())
    .bind(owner.profile_id.to_string())
    .bind(owner.account_session_id.to_string())
    .bind(owner.provider_id.to_string())
    .bind("i".repeat(32))
    .bind("s".repeat(32))
    .bind("elx-live:v1:item")
    .bind("elx-live-token-hash:v1:test")
    .bind("elx-live:v1:descriptor")
    .bind(binding_id.to_string())
    .bind((now + Duration::minutes(2)).to_rfc3339())
    .bind((now + Duration::hours(1)).to_rfc3339())
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "INSERT INTO live_egress_bindings (
            id, session_id, policy_id, mode, gateway_container_name,
            worker_container_name, state, control_fencing_token, policy_revision
         ) VALUES ($1, $2, 'live-egress-test', 'wireguard', $3, $4,
                   'provisioning', 9, 1)",
    )
    .bind(binding_id.to_string())
    .bind(session_id.to_string())
    .bind("elixir-live-test-vpn")
    .bind("elixir-live-test-worker")
    .execute(&database.pool)
    .await?;
    let session = SessionRecord {
        id: session_id,
        owner,
        delivery_mode: DeliveryMode::ServerRelay,
        protocol: SessionProtocol::Hls,
        state: SessionState::ProvisioningEgress,
        revision: 3,
        token_revision: 1,
        control_fencing_token: 9,
        source_index: 0,
        failover_count: 0,
        refresh_count: 0,
        egress_binding_id: Some(binding_id),
        remux_job_id: None,
        created_at: now,
        last_heartbeat_at: now,
        expires_at: now + Duration::minutes(2),
        hard_expires_at: now + Duration::hours(1),
        ended_at: None,
        error_code: None,
        error_detail_redacted: None,
    };
    let policy = EffectiveEgressPolicy {
        mode: EgressPolicyMode::PreferProtected,
        policy_id: Some("live-egress-test".to_string()),
        allow_fallback: true,
        revision: 1,
        source: EgressPolicySource::ServerAssignment,
    };
    let actor = ActorSnapshot::new(owner.user_id, "N11 Viewer", HomeRole::Viewer)?;
    let audit = LiveAuditChain::new(LiveAuditKey::new("n11-fallback", [91_u8; 32])?);
    let repository = EgressPolicyRepository::new(database.pool.clone());

    repository
        .mark_binding_failed(
            binding_id,
            &session,
            &policy,
            &actor,
            &audit,
            "runtime_readiness_failed",
            now,
        )
        .await?;
    metrics::refresh_database_gauges(&database.pool).await?;
    assert_eq!(
        metrics::SESSIONS_ACTIVE
            .with_label_values(&["server_relay", "hls", "direct_fallback"])
            .get(),
        1
    );
    assert_eq!(
        metrics::EGRESS_BINDINGS_ACTIVE
            .with_label_values(&["wireguard"])
            .get(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM live_egress_bindings WHERE id = $1")
            .bind(binding_id.to_string())
            .fetch_one(&database.pool)
            .await?,
        "failed"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM live_admin_audit_events
             WHERE action = 'egress_direct_fallback' AND target_id = $1"
        )
        .bind(session_id.to_string())
        .fetch_one(&database.pool)
        .await?,
        1
    );

    sqlx::query(
        "UPDATE live_egress_bindings SET state = 'provisioning',
             failure_reason_redacted = NULL, released_at = NULL WHERE id = $1",
    )
    .bind(binding_id.to_string())
    .execute(&database.pool)
    .await?;
    sqlx::query("UPDATE live_playback_sessions SET egress_binding_id = $1 WHERE id = $2")
        .bind(binding_id.to_string())
        .bind(session_id.to_string())
        .execute(&database.pool)
        .await?;
    sqlx::query(
        "CREATE TRIGGER n11_reject_fallback_audit
         BEFORE INSERT ON live_admin_audit_events
         BEGIN
             SELECT RAISE(ABORT, 'injected fallback audit failure');
         END",
    )
    .execute(&database.pool)
    .await?;
    let failed = repository
        .mark_binding_failed(
            binding_id,
            &session,
            &policy,
            &actor,
            &audit,
            "runtime_readiness_failed",
            now + Duration::seconds(1),
        )
        .await;
    assert!(matches!(failed, Err(EgressPolicyRepositoryError::Audit(_))));
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT state FROM live_egress_bindings WHERE id = $1")
            .bind(binding_id.to_string())
            .fetch_one(&database.pool)
            .await?,
        "provisioning"
    );
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT egress_binding_id FROM live_playback_sessions WHERE id = $1"
        )
        .bind(session_id.to_string())
        .fetch_one(&database.pool)
        .await?,
        Some(binding_id.to_string())
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM live_admin_audit_events
             WHERE action = 'egress_direct_fallback'"
        )
        .fetch_one(&database.pool)
        .await?,
        1
    );
    Ok(())
}
