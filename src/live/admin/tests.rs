use std::sync::Arc;

use anyhow::Result;
use chrono::Utc;
use sqlx::Row;
use uuid::Uuid;

use crate::{
    auth::home_profiles::{HomeProfileRepository, HomeRole},
    config::DatabaseConfig,
    db::Database,
    live::admin::{
        ActorSnapshot, DestinationNetworkScope, DestinationRuleInput, DestinationRulePolicy,
        LiveAuditChain, LiveAuditKey, LiveDestinationRuleError, LiveDestinationRuleRepository,
    },
    live::catalog::{LiveProviderGrantError, LiveProviderGrantRepository},
};

#[tokio::test]
async fn o11_destination_rule_crud_is_normalized_revisioned_audited_and_deletable() -> Result<()> {
    let fixture = AdminFixture::new().await?;
    let created = fixture
        .repository
        .create(
            fixture.home_id,
            fixture.provider_id,
            1,
            &fixture.actor,
            rule_input(
                "HTTPS",
                "ExAmple.COM.",
                443,
                "/sports/./league/../live.m3u8",
            ),
            Utc::now(),
        )
        .await?;
    assert_eq!(created.provider_revision, 2);
    assert_eq!(created.revision, 1);
    assert!(!created.terminate_provider_sessions);
    assert_eq!(created.audit.record_hash.len(), 64);
    let rule = created.rule.as_ref().expect("created rule");
    assert_eq!(rule.scheme, "https");
    assert_eq!(rule.host, "example.com");
    assert_eq!(rule.path, "/sports/live.m3u8");

    let stale = fixture
        .repository
        .create(
            fixture.home_id,
            fixture.provider_id,
            1,
            &fixture.actor,
            rule_input("https", "other.example", 443, "/live"),
            Utc::now(),
        )
        .await
        .expect_err("stale provider revision");
    assert!(matches!(stale, LiveDestinationRuleError::RevisionChanged));

    let updated = fixture
        .repository
        .update(
            fixture.home_id,
            fixture.provider_id,
            created.rule_id,
            1,
            &fixture.actor,
            rule_input("https", "media.example", 8443, "/replacement.m3u8"),
            Utc::now(),
        )
        .await?;
    assert_eq!(updated.revision, 2);
    assert_eq!(updated.provider_revision, 3);
    assert!(updated.terminate_provider_sessions);
    assert!(updated.revocation_event.is_some());
    assert_eq!(
        updated.rule.as_ref().expect("updated rule").host,
        "media.example"
    );

    let listed = fixture
        .repository
        .list(fixture.home_id, fixture.provider_id)
        .await?;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].revision, 2);

    let deleted = fixture
        .repository
        .delete(
            fixture.home_id,
            fixture.provider_id,
            created.rule_id,
            2,
            &fixture.actor,
            Utc::now(),
        )
        .await?;
    assert!(deleted.deleted);
    assert_eq!(deleted.revision, 3);
    assert_eq!(deleted.provider_revision, 4);
    assert!(deleted.rule.is_none());
    assert!(deleted.terminate_provider_sessions);
    assert!(deleted.revocation_event.is_some());
    assert!(
        fixture
            .repository
            .list(fixture.home_id, fixture.provider_id)
            .await?
            .is_empty()
    );

    let audits = sqlx::query(
        "SELECT previous_hash, record_hash FROM live_admin_audit_events
         WHERE home_id = $1 ORDER BY occurred_at, id",
    )
    .bind(fixture.home_id.to_string())
    .fetch_all(&fixture.database.pool)
    .await?;
    assert_eq!(audits.len(), 3);
    for row in &audits {
        let hash: String = row.try_get("record_hash")?;
        assert_eq!(hash.len(), 64);
        assert!(hash.bytes().all(|byte| byte.is_ascii_hexdigit()));
    }
    let chained: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM live_admin_audit_events WHERE previous_hash IS NOT NULL",
    )
    .fetch_one(&fixture.database.pool)
    .await?;
    assert_eq!(chained, 2);
    let revocations: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM authorization_revocation_outbox
         WHERE event_type = 'provider_policy_changed' AND provider_id = $1",
    )
    .bind(fixture.provider_id.to_string())
    .fetch_one(&fixture.database.pool)
    .await?;
    assert_eq!(revocations, 2);
    Ok(())
}

#[tokio::test]
async fn o11_destination_rules_reject_collision_unsafe_permissions_and_uncertified_modes()
-> Result<()> {
    let fixture = AdminFixture::new().await?;
    fixture
        .repository
        .create(
            fixture.home_id,
            fixture.provider_id,
            1,
            &fixture.actor,
            rule_input("https", "EXAMPLE.com.", 443, "/live"),
            Utc::now(),
        )
        .await?;
    let collision = fixture
        .repository
        .create(
            fixture.home_id,
            fixture.provider_id,
            2,
            &fixture.actor,
            rule_input("https", "example.com", 443, "/live"),
            Utc::now(),
        )
        .await
        .expect_err("normalized collision");
    assert!(matches!(collision, LiveDestinationRuleError::Conflict));

    let mut credentials = rule_input("http", "public.example", 80, "/live");
    credentials.allow_credentials = true;
    assert!(matches!(
        fixture
            .repository
            .create(
                fixture.home_id,
                fixture.provider_id,
                2,
                &fixture.actor,
                credentials,
                Utc::now(),
            )
            .await,
        Err(LiveDestinationRuleError::InvalidInput)
    ));

    let mut private = rule_input("https", "nas.internal", 443, "/live");
    private.network_scope = DestinationNetworkScope::PrivateLan;
    assert!(matches!(
        fixture
            .repository
            .create(
                fixture.home_id,
                fixture.provider_id,
                2,
                &fixture.actor,
                private,
                Utc::now(),
            )
            .await,
        Err(LiveDestinationRuleError::Forbidden)
    ));

    assert!(matches!(
        fixture
            .repository
            .create(
                fixture.home_id,
                fixture.provider_id,
                2,
                &fixture.actor,
                rule_input("rtmp", "rtmp.example", 1935, "/live"),
                Utc::now(),
            )
            .await,
        Err(LiveDestinationRuleError::Forbidden)
    ));
    Ok(())
}

#[tokio::test]
async fn o11_destination_rule_audit_failure_rolls_back_policy_and_revision() -> Result<()> {
    let fixture = AdminFixture::new().await?;
    sqlx::query(
        "CREATE TRIGGER o11_reject_audit_insert
         BEFORE INSERT ON live_admin_audit_events
         BEGIN
             SELECT RAISE(ABORT, 'injected audit failure');
         END",
    )
    .execute(&fixture.database.pool)
    .await?;
    let error = fixture
        .repository
        .create(
            fixture.home_id,
            fixture.provider_id,
            1,
            &fixture.actor,
            rule_input("https", "rollback.example", 443, "/live"),
            Utc::now(),
        )
        .await
        .expect_err("audit failure must fail the policy transaction");
    assert!(matches!(error, LiveDestinationRuleError::Audit(_)));
    let rules: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM live_provider_destination_rules")
        .fetch_one(&fixture.database.pool)
        .await?;
    let states: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM live_provider_admin_state")
        .fetch_one(&fixture.database.pool)
        .await?;
    let heads: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM live_admin_audit_chain_heads")
        .fetch_one(&fixture.database.pool)
        .await?;
    let revocations: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM authorization_revocation_outbox")
            .fetch_one(&fixture.database.pool)
            .await?;
    assert_eq!((rules, states, heads, revocations), (0, 0, 0, 0));
    sqlx::query("DROP TRIGGER o11_reject_audit_insert")
        .execute(&fixture.database.pool)
        .await?;
    Ok(())
}

#[tokio::test]
async fn o11_provider_grants_are_revisioned_audited_and_revoked_transactionally() -> Result<()> {
    let fixture = AdminFixture::new().await?;
    let repository = LiveProviderGrantRepository::new(fixture.database.pool.clone());
    let granted = repository
        .set_grant_audited(
            &fixture.actor,
            fixture.profile_id,
            fixture.provider_id,
            true,
            true,
            1,
            None,
            &fixture.audit,
        )
        .await?;
    assert_eq!(granted.revision, 2);
    assert!(granted.can_browse && granted.can_play);
    assert_eq!(granted.audit.action, "provider_grant_set");
    assert!(granted.revocation_event_id.is_none());

    let stale = repository
        .set_grant_audited(
            &fixture.actor,
            fixture.profile_id,
            fixture.provider_id,
            true,
            false,
            1,
            None,
            &fixture.audit,
        )
        .await
        .expect_err("stale authorization revision");
    assert!(matches!(stale, LiveProviderGrantError::RevisionChanged));

    let revoked = repository
        .set_grant_audited(
            &fixture.actor,
            fixture.profile_id,
            fixture.provider_id,
            false,
            false,
            2,
            None,
            &fixture.audit,
        )
        .await?;
    assert_eq!(revoked.revision, 3);
    assert!(!revoked.can_browse && !revoked.can_play);
    assert_eq!(revoked.audit.action, "provider_grant_revoke");
    assert!(revoked.revocation_event_id.is_some());
    let serialized = serde_json::to_value(&revoked)?;
    assert!(serialized.get("revocationEventId").is_none());
    let counts: (i64, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT COUNT(*) FROM live_provider_grants),
            (SELECT COUNT(*) FROM live_admin_audit_events
             WHERE target_type = 'provider_grant'),
            (SELECT COUNT(*) FROM authorization_revocation_outbox
             WHERE event_type = 'provider_grant_revoked')",
    )
    .fetch_one(&fixture.database.pool)
    .await?;
    assert_eq!(counts, (0, 2, 1));
    Ok(())
}

#[tokio::test]
async fn o11_provider_grant_audit_failure_rolls_back_grant_revision_and_revocation() -> Result<()> {
    let fixture = AdminFixture::new().await?;
    sqlx::query(
        "CREATE TRIGGER o11_reject_grant_audit_insert
         BEFORE INSERT ON live_admin_audit_events
         BEGIN
             SELECT RAISE(ABORT, 'injected grant audit failure');
         END",
    )
    .execute(&fixture.database.pool)
    .await?;
    let error = LiveProviderGrantRepository::new(fixture.database.pool.clone())
        .set_grant_audited(
            &fixture.actor,
            fixture.profile_id,
            fixture.provider_id,
            true,
            true,
            1,
            None,
            &fixture.audit,
        )
        .await
        .expect_err("grant audit failure");
    assert!(matches!(error, LiveProviderGrantError::Audit(_)));
    let state: (i64, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT COUNT(*) FROM live_provider_grants),
            (SELECT revision FROM profile_authorization_revisions WHERE profile_id = $1),
            (SELECT COUNT(*) FROM authorization_revocation_outbox)",
    )
    .bind(fixture.profile_id.to_string())
    .fetch_one(&fixture.database.pool)
    .await?;
    assert_eq!(state, (0, 1, 0));
    Ok(())
}

struct AdminFixture {
    database: Database,
    home_id: Uuid,
    profile_id: Uuid,
    provider_id: Uuid,
    actor: ActorSnapshot,
    audit: Arc<LiveAuditChain>,
    repository: LiveDestinationRuleRepository,
}

impl AdminFixture {
    async fn new() -> Result<Self> {
        let database = Database::connect(&DatabaseConfig {
            url: format!(
                "sqlite:file:/tmp/o11-live-admin-{}?mode=memory&cache=shared",
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
            .bind(format!("{user_id}@o11.example"))
            .bind("hashed")
            .execute(&database.pool)
            .await?;
        let owner = HomeProfileRepository::new(&database.pool)
            .ensure_owner_home(user_id)
            .await?;
        let extension_id = format!("elixir.test.o11.{user_id}");
        let instance_id = Uuid::new_v4();
        let provider_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO extensions
                (extension_id, name, version, kind, trust_level, manifest_json, enabled)
             VALUES ($1, 'O11 Provider', '1.0.0', 'module', 'verified', '{}', TRUE)",
        )
        .bind(&extension_id)
        .execute(&database.pool)
        .await?;
        sqlx::query(
            "INSERT INTO extension_instances
                (instance_id, extension_id, instance_name, enabled)
             VALUES ($1, $2, 'default', TRUE)",
        )
        .bind(instance_id.to_string())
        .bind(&extension_id)
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
        let actor = ActorSnapshot::new(user_id, "O11 Owner", HomeRole::Owner)?;
        let audit = Arc::new(LiveAuditChain::new(LiveAuditKey::new(
            "audit-test-1",
            [91u8; 32],
        )?));
        let repository = LiveDestinationRuleRepository::new(
            database.pool.clone(),
            audit.clone(),
            DestinationRulePolicy::default(),
        );
        Ok(Self {
            database,
            home_id: owner.home.id,
            profile_id: owner.profile.id,
            provider_id,
            actor,
            audit,
            repository,
        })
    }
}

fn rule_input(scheme: &str, host: &str, port: u16, path: &str) -> DestinationRuleInput {
    DestinationRuleInput {
        scheme: scheme.to_string(),
        host: host.to_string(),
        port,
        path: path.to_string(),
        network_scope: DestinationNetworkScope::Public,
        allow_fetch: true,
        allow_credentials: false,
        allow_client_disclosure: false,
    }
}
