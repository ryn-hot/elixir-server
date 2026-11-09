//! Actual signed source/archive, SQL/encryption/effects. Active enrollment and
//! native ownership are TEST-ONLY fixtures, not activation or runtime proofs.
use super::super::tests::Fixture;
use super::*;
use crate::marketplace::grant_store::{sdk::InstalledScope, tests as grant_tests};
use crate::{config::DatabaseConfig, marketplace::security_state};
use ed25519_dalek::SigningKey;
use elixir_marketplace::source_execution::{RuntimeJson, SourceSdkReply};
use elixir_marketplace::{
    records::{Revocation, SignedRecord},
    security_job::SecurityRecords,
};
use serde_json::{Value, json};
use std::time::Duration;

const MIB: u64 = 1024 * 1024;
pub(in crate::marketplace::grant_store::installation) mod local_http;

#[tokio::test]
async fn installed_http_rechecks_grant_native_restrictions_and_instance_without_mutating_state()
-> Result<()> {
    let f = Fixture::new().await?;
    let instance = enroll(&f).await?;
    let held = binding(&f, instance).await?;
    let before = snapshot(&f, instance).await?;
    assert!(super::super::super::sdk::authorize_installed_http(&f.db, &held, || Ok(())).await?);
    assert!(
        super::super::super::sdk::authorize_installed_http(&f.db, &held, || bail!(
            "fixture native owner lost"
        ))
        .await
        .is_err()
    );
    assert_eq!(before, snapshot(&f, instance).await?);
    sqlx::query("UPDATE extension_instances SET enabled=FALSE WHERE instance_id=$1")
        .bind(instance.to_string())
        .execute(&f.db.pool)
        .await?;
    assert!(
        super::super::super::sdk::authorize_installed_http(&f.db, &held, || Ok(()))
            .await
            .is_err()
    );
    assert_eq!(before, snapshot(&f, instance).await?);
    Ok(())
}
#[tokio::test]
async fn installed_http_never_inherits_network_from_a_later_broader_approval() -> Result<()> {
    let mut f = Fixture::without_network().await?;
    let instance = enroll(&f).await?;
    let held = binding(&f, instance).await?;
    assert!(
        !held
            .review()
            .scope()
            .permissions()
            .iter()
            .any(|p| p == "network.egress")
    );
    assert!(!super::super::super::sdk::authorize_installed_http(&f.db, &held, || Ok(())).await?);
    f.upgrade("1.1.0", None).await?;
    assert!(
        f.material()?
            .review
            .scope()
            .permissions()
            .iter()
            .any(|p| p == "network.egress")
    );
    assert!(!super::super::super::sdk::authorize_installed_http(&f.db, &held, || Ok(())).await?);
    Ok(())
}
#[tokio::test]
async fn revoked_installed_http_grant_cannot_be_restored_by_reusing_a_binding() -> Result<()> {
    let f = Fixture::new().await?;
    let instance = enroll(&f).await?;
    let held = binding(&f, instance).await?;
    assert!(super::super::super::sdk::authorize_installed_http(&f.db, &held, || Ok(())).await?);
    super::super::super::revoke_as(
        &f.db,
        &f.actor,
        &held.extension,
        1,
        Uuid::new_v4(),
        Utc::now(),
    )
    .await?;
    assert!(
        super::super::super::sdk::authorize_installed_http(&f.db, &held, || Ok(()))
            .await
            .is_err()
    );
    Ok(())
}
pub(in crate::marketplace::grant_store::installation) fn limit() -> InstallBudget {
    InstallBudget {
        entries: 4,
        bytes: 4 * MIB,
    }
}
fn manager() -> SecretsManager {
    SecretsManager::from_key_bytes([57; 32], false)
}

/// Synthetic commit, deliberately confined to tests. Production activation
/// cannot move a staged capsule without its separate native/state gates.
pub(in crate::marketplace::grant_store::installation) async fn enroll(f: &Fixture) -> Result<Uuid> {
    let staged = f.stage(Uuid::new_v4(), 0, limit()).await?;
    let mut connection = crate::marketplace::durability::connection(&f.db).await?;
    let mut tx = connection.begin().await?;
    synchronous(&mut tx, f.db.driver).await?;
    let ledger = store::lock_budget(&mut tx, limit()).await?;
    let old = store::load(&mut tx, f.db.driver, staged.extension())
        .await?
        .unwrap();
    let journal = Journal {
        choices: None,
        configured_instance: None,
        schema_version: 1,
        extension: old.journal.extension.clone(),
        home: old.journal.home,
        revision: old.journal.revision + 1,
        mutation: Uuid::new_v4(),
        request: Digest::of(b"synthetic installed authority fixture"),
        approved_by: old.journal.approved_by,
        active: old.journal.pending.clone(),
        previous: None,
        pending: None,
        cancelled: None,
        activation: None,
    };
    store::write(&mut tx, ledger, Some(&old), journal, old.floor, limit()).await?;
    sqlx::query("UPDATE extensions SET enabled=TRUE WHERE extension_id=$1")
        .bind(staged.extension().as_str())
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE extension_instances SET enabled=TRUE WHERE instance_id=$1")
        .bind(staged.instance().to_string())
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(staged.instance())
}
pub(in crate::marketplace::grant_store::installation) async fn binding(
    f: &Fixture,
    instance: Uuid,
) -> Result<InstalledBinding> {
    InstalledBinding::load(
        &f.db,
        f.actor.home,
        &f.descriptor.release.payload.extension_id,
        instance,
        limit(),
    )
    .await
}
/// Fixture-only journal replacement; not a production enrollment operation.
pub(in crate::marketplace::grant_store::installation) async fn change_journal(
    f: &Fixture,
    edit: impl FnOnce(&mut Journal) -> Result<()>,
) -> Result<()> {
    let mut tx = f.db.pool.begin().await?;
    synchronous(&mut tx, f.db.driver).await?;
    let id = &f.descriptor.release.payload.extension_id;
    crate::marketplace::preferences::require_home_binding(&mut tx, f.actor.home, f.db.driver)
        .await?;
    store::lock_grant_binding(&mut tx, f.db.driver, f.actor.home, id).await?;
    let ledger = store::lock_budget(&mut tx, limit()).await?;
    let old = store::load(&mut tx, f.db.driver, id).await?.unwrap();
    let mut journal: Journal =
        canonical::decode(&canonical::to_vec(&old.journal)?, record::MAX_RECORD_BYTES)?;
    journal.revision += 1;
    journal.mutation = Uuid::new_v4();
    edit(&mut journal)?;
    store::write(&mut tx, ledger, Some(&old), journal, old.floor, limit()).await?;
    tx.commit().await?;
    Ok(())
}
async fn call(f: &Fixture, binding: &InstalledBinding, request: Value) -> Result<SourceSdkReply> {
    InstalledScope {
        database: &f.db,
        secrets: &manager(),
        binding,
    }
    .execute(&RuntimeJson::from_value(request)?, 64 * 1024, || Ok(()))
    .await
}
fn value(reply: SourceSdkReply) -> Result<Value> {
    let SourceSdkReply::Value(raw) = reply else {
        bail!("expected installed fixture value");
    };
    Ok(serde_json::from_str(raw.as_str())?)
}
async fn state_set(
    f: &Fixture,
    binding: &InstalledBinding,
    content: &str,
) -> Result<SourceSdkReply> {
    call(
        f,
        binding,
        json!({"operation":"state_set","key":"cursor","value":content}),
    )
    .await
}
async fn state_get(f: &Fixture, binding: &InstalledBinding) -> Result<Value> {
    value(call(f, binding, json!({"operation":"state_get","key":"cursor"})).await?)
}
type Snapshot = (Option<(i64, i64, i64)>, Vec<(String, i64, String, i64)>);
async fn snapshot(f: &Fixture, instance: Uuid) -> Result<Snapshot> {
    Ok((sqlx::query_as("SELECT revision,key_count,encoded_bytes FROM marketplace_instance_sdk_state WHERE instance_id=$1")
        .bind(instance.to_string()).fetch_optional(&f.db.pool).await?,
        sqlx::query_as("SELECT key,revision,value_encrypted,encoded_bytes FROM marketplace_instance_sdk_values WHERE instance_id=$1 ORDER BY key")
        .bind(instance.to_string()).fetch_all(&f.db.pool).await?))
}
async fn approve(f: &Fixture, review: &GrantReview, expected: i64) -> Result<CommittedOwnerGrant> {
    super::super::super::approve_review_with_binding(
        &f.db,
        &f.actor,
        review,
        &super::super::super::ApprovalSelection {
            review_id: review.review_id()?,
            expected_revision: expected,
            mutation_id: Uuid::new_v4(),
            automatic_updates: false,
        },
        Utc::now(),
        true,
    )
    .await
}

#[tokio::test]
async fn pending_or_source_prepared_metadata_cannot_mint_an_active_binding() -> Result<()> {
    let f = Fixture::new().await?;
    let staged = f.stage(Uuid::new_v4(), 0, limit()).await?;
    assert!(binding(&f, staged.instance()).await.is_err());
    // Enabled flags alone still cannot turn pending data into an installation.
    sqlx::query("UPDATE extensions SET enabled=TRUE")
        .execute(&f.db.pool)
        .await?;
    sqlx::query("UPDATE extension_instances SET enabled=TRUE")
        .execute(&f.db.pool)
        .await?;
    assert!(binding(&f, staged.instance()).await.is_err());
    assert!(binding(&f, Uuid::new_v4()).await.is_err());
    change_journal(&f, |journal| {
        journal.revision = 4;
        journal.activation = Some(record::ActivationFence {
            attempt: Uuid::new_v4(),
            staged_revision: 1,
            grant_revision: 1,
            plan: Digest::of(b"synthetic completed source probe"),
            phase: record::ActivationPhase::SourcePrepared,
            state_copy: None,
        });
        Ok(())
    })
    .await?;
    assert!(
        binding(&f, staged.instance()).await.is_err(),
        "even source-prepared plus enabled flags is not an active capsule"
    );
    Ok(())
}

#[tokio::test]
async fn cached_effect_binding_cannot_adopt_replaced_enrollment_even_for_the_same_release_and_instance()
-> Result<()> {
    let f = Fixture::new().await?;
    let instance = enroll(&f).await?;
    let held = binding(&f, instance).await?;
    approve(&f, held.review(), 1).await?;
    change_journal(&f, |journal| {
        journal.active.as_mut().unwrap().grant_revision = 2;
        Ok(())
    })
    .await?;
    // The replacement is independently coherent. A check that looked only at
    // extension/release/instance or the latest approval would silently adopt it.
    let replacement = binding(&f, instance).await?;
    replacement.current(&f.db).await?;
    assert!(held.current(&f.db).await.is_err());
    let before = snapshot(&f, instance).await?;
    assert!(
        state_set(&f, &held, "old enrollment must not write")
            .await
            .is_err()
    );
    assert_eq!(snapshot(&f, instance).await?, before);
    Ok(())
}

#[tokio::test]
async fn installed_local_effects_survive_offline_discovery_session_expiry_rotation_and_restart()
-> Result<()> {
    let mut f = Fixture::new().await?;
    let instance = enroll(&f).await?;
    let held = binding(&f, instance).await?;
    state_set(&f, &held, "retained-progress").await?;
    (f.meta_owner)(false);
    (f.public_owner)(false);
    sqlx::query("UPDATE account_sessions SET revoked_at=CURRENT_TIMESTAMP,revoked_reason='installed fixture browser logout'")
        .execute(&f.db.pool).await?;
    let mut d = f.descriptor.clone();
    d.publisher.payload.sequence += 1;
    d.publisher.payload.release_keys = vec![elixir_marketplace::identity::PublicKey::from_bytes(
        SigningKey::from_bytes(&[4; 32]).verifying_key().to_bytes(),
    )?];
    d.publisher = SignedRecord::sign(d.publisher.payload, None, &SigningKey::from_bytes(&[1; 32]))?;
    let publisher = Digest::of(d.publisher.to_bytes()?);
    d.head.payload.publisher_state_id = publisher;
    d.head.payload.sequence = 0;
    d.head.payload.stable_release_id = Digest::of(b"a newer release, not this installed source");
    d.head = SignedRecord::sign(
        d.head.payload,
        Some(publisher),
        &SigningKey::from_bytes(&[4; 32]),
    )?;
    assert!(
        security_state::fixture_observe_signed(
            &f.db,
            SecurityRecords {
                schema_version: 1,
                extension_id: d.release.payload.extension_id.clone(),
                publisher: d.publisher,
                head: Some(d.head),
                revocations: vec![],
            }
        )
        .await?
    );
    held.current(&f.db).await?;
    assert_eq!(state_get(&f, &held).await?, json!("retained-progress"));
    f.db.pool.close().await;
    f.db = Database::connect(&DatabaseConfig {
        url: format!(
            "sqlite://{}",
            f.root.path().join("install.sqlite").display()
        ),
        max_connections: 4,
        connect_timeout_seconds: 5,
    })
    .await?;
    let restored = binding(&f, instance).await?;
    state_set(&f, &restored, "after-restart").await?;
    assert_eq!(state_get(&f, &restored).await?, json!("after-restart"));
    let raw = snapshot(&f, instance).await?;
    assert!(!raw.1[0].2.contains("after-restart"));
    Ok(())
}

#[tokio::test]
async fn manual_broader_approval_never_expands_installed_scope_and_reapproval_never_resurrects_it()
-> Result<()> {
    let f = Fixture::new().await?;
    let instance = enroll(&f).await?;
    let held = binding(&f, instance).await?;
    let original = held.review.clone();
    let mut raw = serde_json::to_value(&original)?;
    raw["release_id"] = json!(Digest::of(b"a different manually approved release"));
    raw["manifest_sha256"] = json!(Digest::of(b"different manifest"));
    raw["scope"]["runtime"]["memory_bytes"] = json!(256 * MIB);
    raw["scope"]["secret_keys"]
        .as_array_mut()
        .unwrap()
        .push(json!("zz_new_secret"));
    let broader: GrantReview = serde_json::from_value(raw)?;
    let grant = approve(&f, &broader, 1).await?;
    assert!(!grant.automatic_updates());
    assert!(
        !grant.covers(&original)?,
        "enrollment consent stays distinct from retained scope"
    );
    grant_tests::secret(
        &f.db,
        "instance",
        Some(instance),
        "zz_new_secret",
        "new-only",
        &manager(),
    )
    .await?;
    assert!(matches!(
        call(
            &f,
            &held,
            json!({"operation":"secret","key":"zz_new_secret"})
        )
        .await?,
        SourceSdkReply::Denied
    ));
    state_set(&f, &held, "old-original-scope").await?;
    super::super::super::revoke_as(
        &f.db,
        &f.actor,
        &held.extension,
        2,
        Uuid::new_v4(),
        Utc::now(),
    )
    .await?;
    approve(&f, &broader, 3).await?;
    let before = snapshot(&f, instance).await?;
    assert!(held.current(&f.db).await.is_err());
    assert!(state_set(&f, &held, "must-not-resurrect").await.is_err());
    assert!(binding(&f, instance).await.is_err());
    assert_eq!(snapshot(&f, instance).await?, before);
    Ok(())
}

#[tokio::test]
async fn installed_scope_does_not_adopt_a_narrower_grant_or_an_unauthorized_secret() -> Result<()> {
    let f = Fixture::new().await?;
    let instance = enroll(&f).await?;
    let held = binding(&f, instance).await?;
    grant_tests::secret(
        &f.db,
        "global",
        None,
        "source_api",
        "not-global",
        &manager(),
    )
    .await?;
    assert_eq!(
        value(call(&f, &held, json!({"operation":"secret","key":"source_api"})).await?)?,
        Value::Null
    );
    grant_tests::secret(
        &f.db,
        "instance",
        Some(instance),
        "source_api",
        "own-only",
        &manager(),
    )
    .await?;
    assert_eq!(
        value(call(&f, &held, json!({"operation":"secret","key":"source_api"})).await?)?,
        json!("own-only")
    );
    let mut raw = serde_json::to_value(held.review())?;
    raw["scope"]["runtime"]["memory_bytes"] = json!(64 * MIB);
    let narrower: GrantReview = serde_json::from_value(raw)?;
    approve(&f, &narrower, 1).await?;
    assert!(held.current(&f.db).await.is_err());
    assert!(state_set(&f, &held, "outside-current-scope").await.is_err());
    Ok(())
}

#[tokio::test]
async fn installed_effect_reply_overflow_and_lost_native_owner_roll_back_without_state_repair()
-> Result<()> {
    let f = Fixture::new().await?;
    let instance = enroll(&f).await?;
    let held = binding(&f, instance).await?;
    let manager = manager();
    let scope = InstalledScope {
        database: &f.db,
        secrets: &manager,
        binding: &held,
    };
    let request = RuntimeJson::from_value(
        json!({"operation":"state_set","key":"cursor","value":"rollback"}),
    )?;
    let before = snapshot(&f, instance).await?;
    for maximum in [
        0,
        3,
        elixir_marketplace::source_execution::MAX_JSON_BYTES + 1,
    ] {
        assert!(scope.execute(&request, maximum, || Ok(())).await.is_err());
        assert_eq!(snapshot(&f, instance).await?, before);
    }
    let calls = std::sync::atomic::AtomicUsize::new(0);
    assert!(
        scope
            .execute(&request, 1024, || {
                if calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) >= 2 {
                    bail!("synthetic native owner lost before commit");
                }
                Ok(())
            })
            .await
            .is_err()
    );
    assert_eq!(snapshot(&f, instance).await?, before);
    Ok(())
}

#[tokio::test]
async fn every_effect_rechecks_durable_source_grant_security_home_and_accounting() -> Result<()> {
    for fault in [
        "grant",
        "epoch",
        "grant_json",
        "home",
        "journal",
        "journal_hash",
        "budget",
        "metadata",
        "disabled",
        "instance",
        "state",
        "state_home",
        "security",
        "withdrawal",
    ] {
        let f = Fixture::new().await?;
        let instance = enroll(&f).await?;
        let held = binding(&f, instance).await?;
        state_set(&f, &held, "original").await?;
        match fault {
            "grant" => {
                sqlx::query("UPDATE marketplace_extension_grants SET enabled=FALSE")
                    .execute(&f.db.pool)
                    .await?;
            }
            "epoch" => {
                sqlx::query("UPDATE marketplace_extension_grants SET revision=revision+2,revocation_revision=revision+1").execute(&f.db.pool).await?;
            }
            "grant_json" => {
                sqlx::query("UPDATE marketplace_extension_grants SET review_json='{}'")
                    .execute(&f.db.pool)
                    .await?;
            }
            "home" => {
                sqlx::query("UPDATE marketplace_preferences SET revision=0,home_id=NULL,preferences_json='',mutation_id=NULL,approved_by_user_id=NULL").execute(&f.db.pool).await?;
            }
            "journal" => {
                sqlx::query("DELETE FROM marketplace_installations")
                    .execute(&f.db.pool)
                    .await?;
            }
            "journal_hash" => {
                sqlx::query("UPDATE marketplace_installations SET record_sha256=$1")
                    .bind(Digest::of(b"corrupt").to_string())
                    .execute(&f.db.pool)
                    .await?;
            }
            "budget" => {
                sqlx::query(
                    "UPDATE marketplace_installation_budget SET encoded_bytes=encoded_bytes+1",
                )
                .execute(&f.db.pool)
                .await?;
            }
            "metadata" => {
                sqlx::query("UPDATE extensions SET manifest_json='{}'")
                    .execute(&f.db.pool)
                    .await?;
            }
            "disabled" => {
                sqlx::query("UPDATE extensions SET enabled=FALSE")
                    .execute(&f.db.pool)
                    .await?;
            }
            "instance" => {
                sqlx::query("UPDATE extension_instances SET enabled=FALSE")
                    .execute(&f.db.pool)
                    .await?;
            }
            "state" => {
                sqlx::query("DELETE FROM marketplace_instance_sdk_state")
                    .execute(&f.db.pool)
                    .await?;
            }
            "state_home" => {
                let foreign = grant_tests::owner(&f.db).await?;
                sqlx::query("UPDATE marketplace_instance_sdk_state SET home_id=$1")
                    .bind(foreign.home.to_string())
                    .execute(&f.db.pool)
                    .await?;
            }
            "security" => {
                sqlx::query("UPDATE marketplace_security_budget SET blocked=1")
                    .execute(&f.db.pool)
                    .await?;
            }
            "withdrawal" => {
                let d = &f.descriptor;
                let revoked = SignedRecord::sign(
                    Revocation {
                        root_key: d.publisher.payload.root_key,
                        extension_id: d.release.payload.extension_id.clone(),
                        release_ids: vec![held.review.release_id()],
                        release_keys: vec![],
                        reason: "installed fixture withdrawal".into(),
                    },
                    None,
                    &SigningKey::from_bytes(&[1; 32]),
                )?;
                assert!(
                    security_state::fixture_observe_signed(
                        &f.db,
                        SecurityRecords {
                            schema_version: 1,
                            extension_id: held.extension.clone(),
                            publisher: d.publisher.clone(),
                            head: Some(d.head.clone()),
                            revocations: vec![revoked]
                        }
                    )
                    .await?
                );
            }
            _ => unreachable!(),
        }
        let before = snapshot(&f, instance).await?;
        assert!(held.current(&f.db).await.is_err(), "{fault}");
        assert!(
            state_set(&f, &held, "must-not-write").await.is_err(),
            "{fault}"
        );
        assert_eq!(snapshot(&f, instance).await?, before, "{fault}");
    }
    Ok(())
}

#[tokio::test]
#[ignore = "explicit fresh isolated loopback PostgreSQL installed authority/effect locks and restart; synthetic active/native enrollment"]
async fn postgres_installed_effects_hold_revocation_and_allow_independent_instances() -> Result<()>
{
    let url = std::env::var("ELIXIR_TEST_MARKETPLACE_STANDING_POSTGRES_URL")?;
    let parsed = reqwest::Url::parse(&url)?;
    if !matches!(parsed.scheme(), "postgres" | "postgresql")
        || !matches!(
            parsed.host_str(),
            Some("127.0.0.1" | "localhost" | "[::1]" | "::1")
        )
        || !parsed.path().starts_with("/elixir_marketplace_standing_")
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        bail!("fresh dedicated loopback standing fixture required");
    }
    let db = Database::connect(&DatabaseConfig {
        url: url.clone(),
        max_connections: 8,
        connect_timeout_seconds: 5,
    })
    .await?;
    let tables:i64=sqlx::query_scalar("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog','information_schema')").fetch_one(&db.pool).await?;
    if tables != 0 || db.driver != DatabaseDriver::Postgres {
        bail!("empty PostgreSQL fixture required; no reset");
    }
    let mut f = Fixture::with_database(tempfile::tempdir()?, db.clone()).await?;
    let first = enroll(&f).await?;
    let first_binding = binding(&f, first).await?;
    let second = Fixture::with_database_and_actor(
        tempfile::tempdir()?,
        db.clone(),
        Some(f.actor.clone()),
        "independent-fixture",
    )
    .await?;
    let second_instance = enroll(&second).await?;
    let second_binding = binding(&second, second_instance).await?;
    let mut reader = db.pool.begin().await?;
    first_binding
        .require_in_transaction(&mut reader, db.driver)
        .await?;
    tokio::time::timeout(
        Duration::from_secs(5),
        state_set(&second, &second_binding, "independent"),
    )
    .await??;
    let writer_db = db.clone();
    let actor = f.actor.clone();
    let id = first_binding.extension.clone();
    let writer = tokio::spawn(async move {
        super::super::super::revoke_as(&writer_db, &actor, &id, 1, Uuid::new_v4(), Utc::now()).await
    });
    // Observe an actual blocked writer, not just a scheduling timeout.
    tokio::time::timeout(Duration::from_secs(5),async {
        loop {
            let n:i64=sqlx::query_scalar("SELECT COUNT(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND cardinality(pg_blocking_pids(pid))>0")
                .fetch_one(&db.pool).await?;
            if n>0 {return Ok::<_,anyhow::Error>(());}
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await??;
    assert!(!writer.is_finished());
    reader.rollback().await?;
    tokio::time::timeout(Duration::from_secs(5), writer).await???;
    assert!(state_set(&f, &first_binding, "revoked").await.is_err());
    assert_eq!(
        state_get(&second, &second_binding).await?,
        json!("independent")
    );
    drop(second);
    db.pool.close().await;
    f.db = Database::connect(&DatabaseConfig {
        url,
        max_connections: 6,
        connect_timeout_seconds: 5,
    })
    .await?;
    assert!(binding(&f, first).await.is_err());
    f.db.pool.close().await;
    Ok(())
}
