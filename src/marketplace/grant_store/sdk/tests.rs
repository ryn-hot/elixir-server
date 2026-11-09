//! Actual disposable SQL/encryption/owner approvals. Source/runtime ownership
//! is synthetic here; these are not public-source or installed runtime proofs.
use super::*;
use crate::{config::DatabaseConfig, marketplace::grant_store::tests as grant_tests};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

struct Fixture {
    grant: CommittedOwnerGrant,
    review: GrantReview,
    instance: Uuid,
    manager: SecretsManager,
    actor: Actor,
}
async fn fixture(db: &Database, quota: u64) -> Result<Fixture> {
    let actor = grant_tests::owner(db).await?;
    let prefs = String::from_utf8(canonical::to_vec(
        &super::super::super::preferences::Preferences::default(),
    )?)?;
    sqlx::query("UPDATE marketplace_preferences SET revision=1,home_id=$1,preferences_json=$2,mutation_id=$3,approved_by_user_id=$4 WHERE singleton=1")
        .bind(actor.home.to_string()).bind(prefs).bind(Uuid::new_v4().to_string()).bind(actor.user.to_string()).execute(&db.pool).await?;
    let mut value = serde_json::to_value(super::super::super::grants::fixture_review()?)?;
    value["scope"]["runtime"]["state_bytes"] = json!(quota);
    let review: GrantReview = serde_json::from_value(value)?;
    let grant = approve_review(
        db,
        &actor,
        &review,
        &grant_tests::selection(&review, 0)?,
        Utc::now(),
    )
    .await?;
    sqlx::query("INSERT INTO extensions (extension_id,name,version,kind,trust_level,manifest_json,enabled) VALUES ($1,'SDK fixture','1.0.0','module','community','{}',TRUE)")
        .bind(grant.extension_id.as_str()).execute(&db.pool).await?;
    let instance = Uuid::new_v4();
    initialize(db, &grant, &review, instance, "sdk", || Ok(())).await?;
    sqlx::query("UPDATE extension_instances SET enabled=TRUE WHERE instance_id=$1")
        .bind(instance.to_string())
        .execute(&db.pool)
        .await?;
    Ok(Fixture {
        grant,
        review,
        instance,
        manager: SecretsManager::from_key_bytes([48; 32], false),
        actor,
    })
}
async fn call(db: &Database, f: &Fixture, request: Value) -> Result<SourceSdkReply> {
    execute(
        db,
        &f.manager,
        &f.grant,
        &f.review,
        f.instance,
        &RuntimeJson::from_value(request)?,
        || Ok(()),
    )
    .await
}
fn value(reply: SourceSdkReply) -> Result<Value> {
    let SourceSdkReply::Value(json) = reply else {
        bail!("expected successful SDK fixture reply");
    };
    Ok(serde_json::from_str(json.as_str())?)
}
async fn set(db: &Database, f: &Fixture, key: &str, v: Value) -> Result<SourceSdkReply> {
    call(db, f, json!({"operation":"state_set","key":key,"value":v})).await
}
async fn get(db: &Database, f: &Fixture, key: &str) -> Result<Value> {
    value(call(db, f, json!({"operation":"state_get","key":key})).await?)
}
async fn counters(db: &Database, f: &Fixture) -> Result<(i64, i64, i64)> {
    let row=sqlx::query("SELECT revision,key_count,encoded_bytes FROM marketplace_instance_sdk_state WHERE instance_id=$1").bind(f.instance.to_string()).fetch_one(&db.pool).await?;
    Ok((
        row.try_get("revision")?,
        row.try_get("key_count")?,
        row.try_get("encoded_bytes")?,
    ))
}

#[tokio::test]
async fn owner_invocation_effects_recheck_session_and_roll_back_when_the_reply_will_not_fit()
-> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = grant_tests::connect(&dir.path().join("invocation.sqlite")).await?;
    let f = fixture(&db, 1024 * 1024).await?;
    authorize_invocation(&db, &f.actor, &f.grant, &f.review, f.instance).await?;
    let scope = LocalScope {
        database: &db,
        secrets: &f.manager,
        grant: &f.grant,
        requested: &f.review,
        instance: f.instance,
    };
    let request = RuntimeJson::from_value(
        json!({"operation":"state_set","key":"cursor","value":"must-rollback"}),
    )?;
    let before = counters(&db, &f).await?;
    for maximum in [0, elixir_marketplace::source_execution::MAX_JSON_BYTES + 1] {
        assert!(
            scope
                .execute(
                    &request,
                    Some(InvocationAuthority {
                        actor: &f.actor,
                        maximum_reply_bytes: maximum
                    }),
                    || Ok(())
                )
                .await
                .is_err()
        );
        assert_eq!(counters(&db, &f).await?, before);
    }
    let foreign = grant_tests::owner(&db).await?;
    assert!(
        scope
            .execute(
                &request,
                Some(InvocationAuthority {
                    actor: &foreign,
                    maximum_reply_bytes: 1024
                }),
                || Ok(())
            )
            .await
            .is_err()
    );
    assert_eq!(counters(&db, &f).await?, before);
    assert!(
        scope
            .execute(
                &request,
                Some(InvocationAuthority {
                    actor: &f.actor,
                    maximum_reply_bytes: 3
                }),
                || Ok(())
            )
            .await
            .is_err()
    );
    assert_eq!(counters(&db, &f).await?, before);
    assert_eq!(get(&db, &f, "cursor").await?, Value::Null);
    assert_eq!(
        value(
            scope
                .execute(
                    &request,
                    Some(InvocationAuthority {
                        actor: &f.actor,
                        maximum_reply_bytes: 4
                    }),
                    || Ok(())
                )
                .await?
        )?,
        json!(true)
    );
    let committed = counters(&db, &f).await?;
    sqlx::query("UPDATE account_sessions SET revoked_at=CURRENT_TIMESTAMP,revoked_reason='SDK invocation fixture' WHERE id=$1")
        .bind(f.actor.session.to_string())
        .execute(&db.pool)
        .await?;
    assert!(
        authorize_invocation(&db, &f.actor, &f.grant, &f.review, f.instance)
            .await
            .is_err()
    );
    assert!(
        scope
            .execute(
                &request,
                Some(InvocationAuthority {
                    actor: &f.actor,
                    maximum_reply_bytes: 1024
                }),
                || Ok(())
            )
            .await
            .is_err()
    );
    assert_eq!(counters(&db, &f).await?, committed);
    let mut expired = f.actor.clone();
    expired.expires = Utc::now() - chrono::Duration::seconds(1);
    assert!(
        authorize_invocation(&db, &expired, &f.grant, &f.review, f.instance)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn sdk_local_secrets_are_current_instance_only_and_never_plaintext_or_global_fallback()
-> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = grant_tests::connect(&dir.path().join("sdk.sqlite")).await?;
    let f = fixture(&db, 0).await?;
    grant_tests::secret(
        &db,
        "global",
        None,
        "source_api",
        "never-global",
        &f.manager,
    )
    .await?;
    assert_eq!(
        value(call(&db, &f, json!({"operation":"secret","key":"source_api"})).await?)?,
        Value::Null
    );
    grant_tests::secret(
        &db,
        "instance",
        Some(f.instance),
        "source_api",
        "own-instance-only",
        &f.manager,
    )
    .await?;
    assert_eq!(
        value(call(&db, &f, json!({"operation":"secret","key":"source_api"})).await?)?,
        json!("own-instance-only")
    );
    assert!(matches!(
        call(&db, &f, json!({"operation":"secret","key":"not_approved"})).await?,
        SourceSdkReply::Denied
    ));
    assert!(
        call(
            &db,
            &f,
            json!({"operation":"secret","key":"source_api","instance_id":Uuid::new_v4()})
        )
        .await
        .is_err()
    );
    assert!(matches!(call(&db,&f,json!({"operation":"fetch","url":"https://example.invalid","method":"GET","headers":[],"body":null})).await?,SourceSdkReply::Denied));
    sqlx::query("UPDATE secrets SET value_encrypted='plaintext-must-not-leak' WHERE scope='instance' AND scope_id=$1").bind(f.instance.to_string()).execute(&db.pool).await?;
    let raw = RuntimeJson::from_value(json!({"operation":"secret","key":"source_api"}))?;
    assert!(
        execute(
            &db,
            &SecretsManager::from_key_bytes([48; 32], true),
            &f.grant,
            &f.review,
            f.instance,
            &raw,
            || Ok(())
        )
        .await
        .is_err()
    );
    Ok(())
}
async fn persistence(db: &Database, f: &Fixture) -> Result<()> {
    assert_eq!(get(db, f, "cursor").await?, Value::Null);
    assert_eq!(
        value(
            set(
                db,
                f,
                "cursor",
                json!({"fraction":0.75,"token":"private-value-not-plaintext"})
            )
            .await?
        )?,
        json!(true)
    );
    let first = counters(db, f).await?;
    assert_eq!((first.0, first.1), (2, 1));
    assert_eq!(
        value(
            set(
                db,
                f,
                "cursor",
                json!({"fraction":0.75,"token":"private-value-not-plaintext"})
            )
            .await?
        )?,
        json!(true)
    );
    assert_eq!(counters(db, f).await?, first);
    let encrypted: String = sqlx::query_scalar(
        "SELECT value_encrypted FROM marketplace_instance_sdk_values WHERE instance_id=$1",
    )
    .bind(f.instance.to_string())
    .fetch_one(&db.pool)
    .await?;
    assert!(SecretsManager::is_encrypted(&encrypted));
    assert!(!encrypted.contains("private-value-not-plaintext"));
    assert_eq!(
        get(db, f, "cursor").await?,
        json!({"fraction":0.75,"token":"private-value-not-plaintext"})
    );
    Ok(())
}
#[tokio::test]
async fn sdk_state_persists_encrypted_replaces_without_history_and_cascades_on_instance_delete()
-> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("sdk.sqlite");
    let db = grant_tests::connect(&path).await?;
    let f = fixture(&db, 1024 * 1024).await?;
    persistence(&db, &f).await?;
    db.pool.close().await;
    let db = grant_tests::connect(&path).await?;
    assert_eq!(get(&db, &f, "cursor").await?["fraction"], json!(0.75));
    for i in 0..20 {
        assert_eq!(value(set(&db, &f, "cursor", json!(i)).await?)?, json!(true));
    }
    assert_eq!(counters(&db, &f).await?.1, 1);
    assert_eq!(
        value(call(&db, &f, json!({"operation":"state_delete","key":"cursor"})).await?)?,
        json!(true)
    );
    assert_eq!(
        value(call(&db, &f, json!({"operation":"state_delete","key":"cursor"})).await?)?,
        json!(false)
    );
    assert_eq!(counters(&db, &f).await?.1, 0);
    set(&db, &f, "again", json!("value")).await?;
    sqlx::query("DELETE FROM extension_instances WHERE instance_id=$1")
        .bind(f.instance.to_string())
        .execute(&db.pool)
        .await?;
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_instance_sdk_values")
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(count, 0);
    assert!(get(&db, &f, "again").await.is_err());
    Ok(())
}
#[tokio::test]
async fn sdk_state_quota_and_key_count_are_hard_limits_without_partial_counter_changes()
-> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = grant_tests::connect(&dir.path().join("sdk.sqlite")).await?;
    let f = fixture(&db, 1024 * 1024).await?;
    let mut accepted = 0;
    loop {
        let before = counters(&db, &f).await?;
        match set(
            &db,
            &f,
            &format!("key{accepted}"),
            json!("x".repeat(60 * 1024)),
        )
        .await?
        {
            SourceSdkReply::Denied => {
                assert_eq!(counters(&db, &f).await?, before);
                break;
            }
            reply => {
                assert_eq!(value(reply)?, json!(true));
                accepted += 1;
                assert!(accepted <= 128);
            }
        }
    }
    assert!(accepted > 1 && accepted < 128);
    call(&db, &f, json!({"operation":"state_delete","key":"key0"})).await?;
    assert_eq!(
        value(set(&db, &f, "replacement", json!("x".repeat(60 * 1024))).await?)?,
        json!(true)
    );
    assert_eq!(counters(&db, &f).await?.1, accepted);
    // A separately approved larger quota reaches the independent 128-key cap.
    let mut json = serde_json::to_value(&f.review)?;
    json["scope"]["runtime"]["state_bytes"] = json!(2 * 1024 * 1024);
    let review: GrantReview = serde_json::from_value(json)?;
    let grant = approve_review(
        &db,
        &f.actor,
        &review,
        &grant_tests::selection(&review, 1)?,
        Utc::now(),
    )
    .await?;
    let second = Uuid::new_v4();
    initialize(&db, &grant, &review, second, "second", || Ok(())).await?;
    sqlx::query("UPDATE extension_instances SET enabled=TRUE WHERE instance_id=$1")
        .bind(second.to_string())
        .execute(&db.pool)
        .await?;
    let f = Fixture {
        grant,
        review,
        instance: second,
        manager: f.manager,
        actor: f.actor,
    };
    for key in 0..128 {
        assert_eq!(
            value(set(&db, &f, &format!("key{key}"), json!(null)).await?)?,
            json!(true)
        );
    }
    let before = counters(&db, &f).await?;
    assert!(matches!(
        set(&db, &f, "overflow", json!(null)).await?,
        SourceSdkReply::Denied
    ));
    assert_eq!(counters(&db, &f).await?, before);
    Ok(())
}
#[tokio::test]
async fn sdk_missing_corrupt_or_cross_key_state_is_not_repaired_or_overwritten() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = grant_tests::connect(&dir.path().join("sdk.sqlite")).await?;
    let f = fixture(&db, 1024 * 1024).await?;
    set(&db, &f, "first", json!("sensitive")).await?;
    set(&db, &f, "second", json!("different")).await?;
    let row=sqlx::query("SELECT value_encrypted,encoded_bytes,revision FROM marketplace_instance_sdk_values WHERE instance_id=$1 AND key='first'").bind(f.instance.to_string()).fetch_one(&db.pool).await?;
    let cipher: String = row.try_get("value_encrypted")?;
    let old: i64 = row.try_get("encoded_bytes")?;
    let revision: i64 = row.try_get("revision")?;
    let second:i64=sqlx::query_scalar("SELECT encoded_bytes FROM marketplace_instance_sdk_values WHERE instance_id=$1 AND key='second'").bind(f.instance.to_string()).fetch_one(&db.pool).await?;
    sqlx::query("UPDATE marketplace_instance_sdk_values SET value_encrypted=$1,encoded_bytes=$2,revision=$3 WHERE instance_id=$4 AND key='second'").bind(cipher).bind(old).bind(revision).bind(f.instance.to_string()).execute(&db.pool).await?;
    sqlx::query("UPDATE marketplace_instance_sdk_state SET encoded_bytes=encoded_bytes+$1 WHERE instance_id=$2").bind(old-second).bind(f.instance.to_string()).execute(&db.pool).await?;
    assert!(get(&db, &f, "second").await.is_err());
    assert!(set(&db, &f, "second", json!("overwrite")).await.is_err());
    sqlx::query("DELETE FROM marketplace_instance_sdk_state WHERE instance_id=$1")
        .bind(f.instance.to_string())
        .execute(&db.pool)
        .await?;
    assert!(get(&db, &f, "first").await.is_err());
    assert!(
        initialize(&db, &f.grant, &f.review, f.instance, "sdk", || Ok(()))
            .await
            .is_err()
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_instance_sdk_state")
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(count, 0);
    Ok(())
}
#[tokio::test]
async fn sdk_disabled_unapproved_foreign_and_stale_contexts_cannot_read_or_write() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = grant_tests::connect(&dir.path().join("sdk.sqlite")).await?;
    let f = fixture(&db, 0).await?;
    assert!(matches!(
        set(&db, &f, "state", json!(1)).await?,
        SourceSdkReply::Denied
    ));
    sqlx::query("UPDATE extension_instances SET enabled=FALSE WHERE instance_id=$1")
        .bind(f.instance.to_string())
        .execute(&db.pool)
        .await?;
    assert!(
        call(&db, &f, json!({"operation":"secret","key":"source_api"}))
            .await
            .is_err()
    );
    sqlx::query("UPDATE extension_instances SET enabled=TRUE WHERE instance_id=$1")
        .bind(f.instance.to_string())
        .execute(&db.pool)
        .await?;
    let raw = RuntimeJson::from_value(json!({"operation":"secret","key":"source_api"}))?;
    assert!(
        execute(
            &db,
            &f.manager,
            &f.grant,
            &f.review,
            Uuid::new_v4(),
            &raw,
            || Ok(())
        )
        .await
        .is_err()
    );
    sqlx::query(
        "UPDATE marketplace_extension_grants SET revision=revision+1 WHERE extension_id=$1",
    )
    .bind(f.grant.extension_id.as_str())
    .execute(&db.pool)
    .await?;
    assert!(
        call(&db, &f, json!({"operation":"secret","key":"source_api"}))
            .await
            .is_err()
    );
    Ok(())
}
#[tokio::test]
async fn sdk_initialize_is_disabled_atomic_and_never_creates_state_from_requests() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = grant_tests::connect(&dir.path().join("sdk.sqlite")).await?;
    let f = fixture(&db, 1024 * 1024).await?;
    let id = Uuid::new_v4();
    let calls = std::cell::Cell::new(0);
    assert!(
        initialize(&db, &f.grant, &f.review, id, "cancelled", || {
            calls.set(calls.get() + 1);
            if calls.get() == 2 {
                bail!("synthetic source lost before commit");
            }
            Ok(())
        })
        .await
        .is_err()
    );
    let absent: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM extension_instances WHERE instance_id=$1")
            .bind(id.to_string())
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(absent, 0);
    initialize(&db, &f.grant, &f.review, id, "created", || Ok(())).await?;
    let enabled:i64=sqlx::query_scalar("SELECT CAST(CASE WHEN enabled THEN 1 ELSE 0 END AS BIGINT) FROM extension_instances WHERE instance_id=$1").bind(id.to_string()).fetch_one(&db.pool).await?;
    assert_eq!(enabled, 0);
    assert!(
        initialize(&db, &f.grant, &f.review, id, "created", || Ok(()))
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn orphan_configured_metadata_blocks_foreground_initialization_effects_and_generic_secret_writes()
-> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = grant_tests::connect(&dir.path().join("metadata-orphan.sqlite")).await?;
    let f = fixture(&db, 1024 * 1024).await?; // actual fresh foreground initializer
    sqlx::query("INSERT INTO marketplace_installation_metadata(extension_id,home_id,metadata_sha256) VALUES($1,$2,$3)")
        .bind(f.grant.extension_id.as_str()).bind(f.grant.home_id.to_string()).bind(elixir_marketplace::identity::Digest::of(b"orphan marker").to_string()).execute(&db.pool).await?;
    let fresh = Uuid::new_v4();
    assert!(
        initialize(&db, &f.grant, &f.review, fresh, "must not adopt", || Ok(()))
            .await
            .is_err()
    );
    assert!(
        call(&db, &f, json!({"operation":"state_get","key":"cursor"}))
            .await
            .is_err()
    );
    let store = crate::extensions::store::ExtensionStore::new(&db.pool);
    let secret = crate::extensions::store::NewSecret {
        secret_id: Uuid::new_v4(),
        scope: crate::db::models::SecretScope::Instance,
        scope_id: Some(f.instance),
        key: "control".into(),
        value_encrypted: f.manager.encrypt("private value")?,
        rotatable: false,
    };
    assert!(store.upsert_secret(&secret).await.is_err());
    sqlx::query("DELETE FROM marketplace_installation_metadata")
        .execute(&db.pool)
        .await?;
    store.upsert_secret(&secret).await?;
    initialize(
        &db,
        &f.grant,
        &f.review,
        fresh,
        "actual foreground control",
        || Ok(()),
    )
    .await?;
    Ok(())
}
async fn concurrent(db: &Database, other: &Database, f: &Fixture) -> Result<()> {
    let a = set(db, f, "one", json!(1));
    let b = set(other, f, "two", json!(2));
    let (a, b) = tokio::join!(a, b);
    assert_eq!(value(a?)?, json!(true));
    assert_eq!(value(b?)?, json!(true));
    assert_eq!(counters(db, f).await?.1, 2);
    let mut tx = db.pool.begin().await?;
    sqlx::query("UPDATE marketplace_extension_grants SET enabled=FALSE,revision=revision+1 WHERE extension_id=$1").bind(f.grant.extension_id.as_str()).execute(&mut *tx).await?;
    let mut pending = Box::pin(set(other, f, "after_revoke", json!(3)));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(40), &mut pending)
            .await
            .is_err()
    );
    tx.commit().await?;
    assert!(pending.await.is_err());
    assert_eq!(counters(db, f).await?.1, 2);
    Ok(())
}
#[tokio::test]
async fn sdk_independent_sqlite_connections_serialize_counters_and_revocation() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("sdk.sqlite");
    let db = grant_tests::connect(&path).await?;
    let other = grant_tests::connect(&path).await?;
    let f = fixture(&db, 1024 * 1024).await?;
    concurrent(&db, &other, &f).await
}
#[tokio::test]
async fn sdk_lost_source_rolls_back_effects_and_foreign_home_binding_stops_access() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = grant_tests::connect(&dir.path().join("sdk.sqlite")).await?;
    let f = fixture(&db, 1024 * 1024).await?;
    let valid = Arc::new(AtomicBool::new(true));
    let raw =
        RuntimeJson::from_value(json!({"operation":"state_set","key":"cursor","value":0.75}))?;
    let checks = std::cell::Cell::new(0);
    let validity = valid.clone();
    assert!(
        execute(
            &db,
            &f.manager,
            &f.grant,
            &f.review,
            f.instance,
            &raw,
            || {
                checks.set(checks.get() + 1);
                if checks.get() == 3 {
                    validity.store(false, Ordering::SeqCst);
                }
                if !validity.load(Ordering::SeqCst) {
                    bail!("synthetic source expired");
                }
                Ok(())
            }
        )
        .await
        .is_err()
    );
    assert_eq!(counters(&db, &f).await?, (1, 0, 0));
    let foreign = grant_tests::owner(&db).await?;
    sqlx::query("UPDATE marketplace_preferences SET home_id=$1 WHERE singleton=1")
        .bind(foreign.home.to_string())
        .execute(&db.pool)
        .await?;
    assert!(get(&db, &f, "cursor").await.is_err());
    Ok(())
}
async fn delete_former_home(db: &Database, f: &Fixture) -> Result<()> {
    let foreign = grant_tests::owner(db).await?;
    // The settings binding deliberately prevents deleting the active home.
    // After an explicit rebind, old private SDK data follows the deleted home.
    sqlx::query("UPDATE marketplace_preferences SET home_id=$1 WHERE singleton=1")
        .bind(foreign.home.to_string())
        .execute(&db.pool)
        .await?;
    sqlx::query("DELETE FROM homes WHERE id=$1")
        .bind(f.actor.home.to_string())
        .execute(&db.pool)
        .await?;
    for table in [
        "marketplace_instance_sdk_state",
        "marketplace_instance_sdk_values",
    ] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(&db.pool)
            .await?;
        assert_eq!(count, 0, "deleted home retained SDK data");
    }
    assert!(get(db, f, "one").await.is_err());
    Ok(())
}
#[tokio::test]
async fn sdk_deleting_a_rebound_home_cascades_private_state_without_blocking_deletion() -> Result<()>
{
    let dir = tempfile::tempdir()?;
    let db = grant_tests::connect(&dir.path().join("sdk.sqlite")).await?;
    let f = fixture(&db, 1024 * 1024).await?;
    set(&db, &f, "one", json!("private fixture value")).await?;
    delete_former_home(&db, &f).await
}
async fn postgres_scope_fences(db: &Database, other: &Database, f: &Fixture) -> Result<()> {
    let second = Uuid::new_v4();
    initialize(db, &f.grant, &f.review, second, "parallel", || Ok(())).await?;
    sqlx::query("UPDATE extension_instances SET enabled=TRUE WHERE instance_id=$1")
        .bind(second.to_string())
        .execute(&db.pool)
        .await?;
    let mut held = db.pool.begin().await?;
    lock_grant(&mut held, &f.grant, &f.review, DatabaseDriver::Postgres).await?;
    lock_instance(&mut held, &f.grant, f.instance, DatabaseDriver::Postgres).await?;
    // Same home/grant/extension, distinct instance: no global SDK serialization.
    let raw = RuntimeJson::from_value(json!({"operation":"state_set","key":"parallel","value":1}))?;
    let reply = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        execute(other, &f.manager, &f.grant, &f.review, second, &raw, || {
            Ok(())
        }),
    )
    .await
    .context("unrelated SDK instance blocked behind shared authority")??;
    assert_eq!(value(reply)?, json!(true));
    held.rollback().await?;
    for (query, id) in [
        (
            "UPDATE marketplace_preferences SET home_id=home_id WHERE singleton=1 AND home_id=$1",
            f.actor.home.to_string(),
        ),
        (
            "UPDATE marketplace_extension_grants SET updated_at=updated_at WHERE extension_id=$1",
            f.grant.extension_id.to_string(),
        ),
        (
            "UPDATE extensions SET installed_at=installed_at WHERE extension_id=$1",
            f.grant.extension_id.to_string(),
        ),
        (
            "UPDATE extension_instances SET updated_at=updated_at WHERE instance_id=$1",
            f.instance.to_string(),
        ),
    ] {
        let mut held = db.pool.begin().await?;
        lock_grant(&mut held, &f.grant, &f.review, DatabaseDriver::Postgres).await?;
        lock_instance(&mut held, &f.grant, f.instance, DatabaseDriver::Postgres).await?;
        let mut pending = Box::pin(sqlx::query(query).bind(id).execute(&other.pool));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(40), &mut pending)
                .await
                .is_err(),
            "authority mutation bypassed SDK transaction fence"
        );
        held.rollback().await?;
        assert_eq!(pending.await?.rows_affected(), 1);
    }
    Ok(())
}
#[tokio::test]
#[ignore = "explicit fresh loopback PostgreSQL SDK state/secret transaction integration"]
async fn sdk_postgres_transactions_restart_and_revocation_when_configured() -> Result<()> {
    let url = std::env::var("ELIXIR_MARKETPLACE_TEST_POSTGRES_SDK_URL")
        .context("requires a dedicated fresh loopback SDK test database")?;
    let parsed = reqwest::Url::parse(&url)?;
    anyhow::ensure!(
        matches!(parsed.scheme(), "postgres" | "postgresql")
            && parsed.host_str() == Some("127.0.0.1")
            && parsed.path() == "/elixir_marketplace_sdk_test",
        "requires dedicated loopback SDK test database"
    );
    let config = DatabaseConfig {
        url,
        max_connections: 3,
        connect_timeout_seconds: 5,
    };
    let db = Database::connect(&config).await?;
    let tables: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM pg_catalog.pg_tables WHERE schemaname='public'")
            .fetch_one(&db.pool)
            .await?;
    anyhow::ensure!(tables == 0, "SDK test database must be empty");
    db.run_migrations().await?;
    let f = fixture(&db, 1024 * 1024).await?;
    persistence(&db, &f).await?;
    // Keep concurrency assertions independent of the preceding cursor key.
    call(&db, &f, json!({"operation":"state_delete","key":"cursor"})).await?;
    let other = Database::connect(&config).await?;
    postgres_scope_fences(&db, &other, &f).await?;
    concurrent(&db, &other, &f).await?;
    other.pool.close().await;
    db.pool.close().await;
    let restarted = Database::connect(&config).await?;
    restarted.run_migrations().await?;
    assert_eq!(counters(&restarted, &f).await?.1, 2);
    assert!(get(&restarted, &f, "one").await.is_err());
    delete_former_home(&restarted, &f).await?;
    restarted.pool.close().await;
    Ok(())
}
