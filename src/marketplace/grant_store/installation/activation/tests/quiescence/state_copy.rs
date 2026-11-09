//! Actual SQL/encryption/controller ownership. ACTIVE enrollment/probe are
//! synthetic fixtures; the separate native test covers original-call cleanup.
use super::super::super::state_copy;
use super::*;
use crate::marketplace::grant_store::installation::runtime::{
    InstalledController, QuiescentRuntime,
};
use crate::{
    db::models::SecretScope,
    extensions::store::{ExtensionStore, NewSecret},
};

pub(super) async fn open_owner(
    f: &Fixture,
    manager: SecretsManager,
) -> Result<InstalledController> {
    InstalledController::open(
        f.db.clone(),
        manager,
        controller_config(f, &format!("sha256:{}", "1".repeat(64)), "copy-fixture")?,
    )
    .await
}
pub(super) async fn copy(
    f: &Fixture,
    staged: &StagedInstallation,
    mutation: Uuid,
    plan: &SourceActivationPlan,
    guard: &QuiescentRuntime<'_>,
) -> Result<ActivationReference> {
    let mut step = step(f, staged, mutation, plan)?;
    step.database = guard.database();
    state_copy::copy_checked(step, guard).await
}
async fn secrets(f: &Fixture, scope: Uuid) -> Result<Vec<(String, String, String, i64)>> {
    Ok(sqlx::query_as("SELECT secret_id,key,value_encrypted,CAST(CASE WHEN rotatable THEN 1 ELSE 0 END AS BIGINT) FROM secrets WHERE scope='instance' AND scope_id=$1 ORDER BY key")
        .bind(scope.to_string()).fetch_all(&f.db.pool).await?)
}
async fn add_secret(f: &Fixture, instance: Uuid, key: &str, value: &str) -> Result<()> {
    sqlx::query("INSERT INTO secrets(secret_id,scope,scope_id,key,value_encrypted,rotatable) VALUES($1,'instance',$2,$3,$4,TRUE)")
        .bind(Uuid::new_v4().to_string()).bind(instance.to_string()).bind(key).bind(manager().encrypt(value)?).execute(&f.db.pool).await?;
    Ok(())
}
async fn copied_value(f: &Fixture, instance: Uuid, key: &str) -> Result<Value> {
    let cipher:String=sqlx::query_scalar("SELECT value_encrypted FROM marketplace_instance_sdk_values WHERE instance_id=$1 AND key=$2")
        .bind(instance.to_string()).bind(key).fetch_one(&f.db.pool).await?;
    let plain = manager().decrypt(&cipher)?;
    let v: Value = serde_json::from_str(&plain)?;
    assert_eq!(v["home_id"], json!(f.actor.home));
    assert_eq!(v["instance_id"], json!(instance));
    assert_eq!(
        v["extension_id"],
        json!(f.descriptor.release.payload.extension_id)
    );
    assert_eq!(v["key"], json!(key));
    assert_eq!(v["revision"], json!(2));
    Ok(v["value"].clone())
}
pub(super) async fn legacy_roundtrip(db: &Database) -> Result<()> {
    let store = ExtensionStore::new(&db.pool);
    for (scope, scope_id) in [
        (SecretScope::Instance, Some(Uuid::new_v4())),
        (SecretScope::Provider, Some(Uuid::new_v4())),
        (SecretScope::Global, None),
    ] {
        let id = Uuid::new_v4();
        let key = format!("copy-roundtrip-{}", Uuid::new_v4().simple());
        let mut data = NewSecret {
            secret_id: id,
            scope,
            scope_id,
            key: key.clone(),
            value_encrypted: manager().encrypt("legacy")?,
            rotatable: false,
        };
        store.upsert_secret(&data).await?;
        if scope_id.is_some() {
            data.value_encrypted = manager().encrypt("replaced")?;
            store.upsert_secret(&data).await?;
        }
        assert_eq!(
            manager().decrypt(
                &store
                    .get_secret(scope, scope_id, &key)
                    .await?
                    .unwrap()
                    .value_encrypted
            )?,
            if scope_id.is_some() {
                "replaced"
            } else {
                "legacy"
            }
        );
        store
            .update_secret(id, &manager().encrypt("updated")?, true)
            .await?;
        store.delete_secret(id).await?;
        store.delete_secret(id).await?;
        store.upsert_secret(&data).await?;
        store.delete_secrets_by_scope(scope, scope_id).await?;
        assert!(store.get_secret(scope, scope_id, &key).await?.is_none());
    }
    Ok(())
}

#[tokio::test]
async fn empty_first_install_copy_is_disabled_idempotent_and_offline_abortable() -> Result<()> {
    let f = Fixture::new().await?;
    let mut owner = open_owner(&f, manager()).await?;
    let (staged, plan, mutation, prepared) = source_prepared(&f, 0).await?;
    let fenced = quiesce_checked(step(&f, &staged, mutation, &plan)?, &prepared).await?;
    let scope = super::super::super::quiescence::observe_checked(
        step(&f, &staged, mutation, &plan)?,
        &fenced,
    )
    .await?;
    let guard = owner.quiesce(scope).await?;
    // No arbitrary DB is accepted even if it describes the same rows.
    assert!(
        state_copy::copy_checked(step(&f, &staged, mutation, &plan)?, &guard)
            .await
            .is_err()
    );
    let copied = copy(&f, &staged, mutation, &plan, &guard).await?;
    assert_eq!(copied.revision(), 6);
    assert_eq!(
        sdk_snapshot(&f, staged.instance()).await?,
        (2, 0, 0, vec![])
    );
    let before = snapshot(&f).await?;
    assert_eq!(
        copy(&f, &staged, mutation, &plan, &guard).await?.revision(),
        6
    );
    assert_eq!(before, snapshot(&f).await?);
    match inspect_as(&f.db, f.actor.clone(), staged.extension(), limit()).await? {
        ActivationStatus::StateCopied(r) => assert_eq!(r.attempt(), copied.attempt()),
        _ => bail!("lost copy phase"),
    }
    (f.meta_owner)(false);
    (f.public_owner)(false);
    let mutation = Uuid::new_v4();
    let resumed = resume(
        &f.db,
        OwnerResumeIntent::fixture_for_actor(f.actor.clone(), &copied, mutation)?,
        limit(),
    )
    .await?;
    assert_eq!(resumed.revision(), 7);
    assert_eq!(
        sdk_snapshot(&f, staged.instance()).await?,
        (1, 0, 0, vec![])
    );
    let before = snapshot(&f).await?;
    resume(
        &f.db,
        OwnerResumeIntent::fixture_for_actor(f.actor.clone(), &copied, mutation)?,
        limit(),
    )
    .await?;
    assert_eq!(before, snapshot(&f).await?);
    unused(&f).await
}

#[tokio::test]
async fn encrypted_fork_rebinds_every_envelope_copies_only_declared_secrets_and_survives_reopen()
-> Result<()> {
    let mut f = Fixture::new().await?;
    let mut owner = open_owner(&f, manager()).await?;
    let instance = installed::enroll(&f).await?;
    let binding = installed::binding(&f, instance).await?;
    for i in 0..20 {
        effect(&f,&binding,json!({"operation":"state_set","key":format!("key.{i:03}"),"value":{"n":i,"unicode":"🎞️"}})).await?;
    }
    add_secret(&f, instance, "source_api", "private-api").await?;
    add_secret(&f, instance, "not_declared", "do-not-copy").await?;
    let original = sdk_snapshot(&f, instance).await?;
    let original_secrets = secrets(&f, instance).await?;
    f.upgrade("2.0.0", None).await?;
    let (staged, plan, mutation, prepared) = source_prepared(&f, 2).await?;
    let empty = sdk_snapshot(&f, staged.instance()).await?;
    let fenced = quiesce_checked(step(&f, &staged, mutation, &plan)?, &prepared).await?;
    let scope = super::super::super::quiescence::observe_checked(
        step(&f, &staged, mutation, &plan)?,
        &fenced,
    )
    .await?;
    let guard = owner.quiesce(scope).await?;
    let copied = copy(&f, &staged, mutation, &plan, &guard).await?;
    let candidate = sdk_snapshot(&f, staged.instance()).await?;
    assert_eq!(candidate.1, 20);
    for (i, ((key, cipher, _), (_, old_cipher, _))) in
        candidate.3.iter().zip(&original.3).enumerate()
    {
        assert_ne!(cipher, old_cipher);
        assert_eq!(
            copied_value(&f, staged.instance(), key).await?,
            json!({"n":i,"unicode":"🎞️"})
        );
    }
    let copied_secrets = secrets(&f, staged.instance()).await?;
    assert_eq!(copied_secrets.len(), 1);
    assert_eq!(copied_secrets[0].1, "source_api");
    assert_eq!(manager().decrypt(&copied_secrets[0].2)?, "private-api");
    let old = original_secrets
        .iter()
        .find(|s| s.1 == "source_api")
        .unwrap();
    assert_ne!(copied_secrets[0].0, old.0);
    assert_ne!(copied_secrets[0].2, old.2);
    assert_eq!(original, sdk_snapshot(&f, instance).await?);
    assert_eq!(original_secrets, secrets(&f, instance).await?);
    assert!(binding.current(&f.db).await.is_err());
    assert!(installed::binding(&f, staged.instance()).await.is_err());
    assert!(matches!(
        prepare_checked(step(&f, &staged, mutation, &plan)?).await?,
        PreparedOrRecovery::Recovery(_)
    ));
    drop(owner);
    reopen(&mut f).await?;
    let scope = super::super::super::quiescence::observe_checked(
        step(&f, &staged, mutation, &plan)?,
        &copied,
    )
    .await?;
    let mut owner = open_owner(&f, manager()).await?;
    let guard = owner.quiesce(scope).await?;
    let before = snapshot(&f).await?;
    assert_eq!(
        copy(&f, &staged, mutation, &plan, &guard).await?.revision(),
        copied.revision()
    );
    assert_eq!(before, snapshot(&f).await?);
    (f.meta_owner)(false);
    (f.public_owner)(false);
    sqlx::query("UPDATE marketplace_extension_grants SET review_json='{}'")
        .execute(&f.db.pool)
        .await?;
    resume(
        &f.db,
        OwnerResumeIntent::fixture_for_actor(f.actor.clone(), &copied, Uuid::new_v4())?,
        limit(),
    )
    .await?;
    assert_eq!(original, sdk_snapshot(&f, instance).await?);
    assert_eq!(original_secrets, secrets(&f, instance).await?);
    assert_eq!(empty, sdk_snapshot(&f, staged.instance()).await?);
    assert!(secrets(&f, staged.instance()).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn wrong_key_or_corrupt_envelope_rolls_back_all_partial_copies() -> Result<()> {
    for corrupt in [false, true] {
        let mut f = Fixture::new().await?;
        let mut owner = open_owner(
            &f,
            if corrupt {
                manager()
            } else {
                SecretsManager::from_key_bytes([58; 32], false)
            },
        )
        .await?;
        let instance = installed::enroll(&f).await?;
        let binding = installed::binding(&f, instance).await?;
        for key in ["a", "z"] {
            effect(
                &f,
                &binding,
                json!({"operation":"state_set","key":key,"value":"source"}),
            )
            .await?;
        }
        if corrupt {
            let cipher:String=sqlx::query_scalar("SELECT value_encrypted FROM marketplace_instance_sdk_values WHERE instance_id=$1 AND key='z'").bind(instance.to_string()).fetch_one(&f.db.pool).await?;
            let mut v: Value = serde_json::from_str(&manager().decrypt(&cipher)?)?;
            v["instance_id"] = json!(Uuid::new_v4());
            let new = manager().encrypt(&serde_json::to_string(&v)?)?;
            // Same UUID width preserves counters; authenticated envelope differs.
            assert_eq!(new.len(), cipher.len());
            sqlx::query("UPDATE marketplace_instance_sdk_values SET value_encrypted=$1 WHERE instance_id=$2 AND key='z'").bind(new).bind(instance.to_string()).execute(&f.db.pool).await?;
        }
        f.upgrade("2.0.0", None).await?;
        let (staged, plan, mutation, prepared) = source_prepared(&f, 2).await?;
        let fenced = quiesce_checked(step(&f, &staged, mutation, &plan)?, &prepared).await?;
        let scope = super::super::super::quiescence::observe_checked(
            step(&f, &staged, mutation, &plan)?,
            &fenced,
        )
        .await?;
        let guard = owner.quiesce(scope).await?;
        let before = snapshot(&f).await?;
        let old = sdk_snapshot(&f, instance).await?;
        let target = sdk_snapshot(&f, staged.instance()).await?;
        assert!(copy(&f, &staged, mutation, &plan, &guard).await.is_err());
        assert_eq!(before, snapshot(&f).await?);
        assert_eq!(old, sdk_snapshot(&f, instance).await?);
        assert_eq!(target, sdk_snapshot(&f, staged.instance()).await?);
    }
    Ok(())
}

#[tokio::test]
async fn any_source_candidate_or_secret_change_prevents_retry_and_destructive_abort() -> Result<()>
{
    for change in ["source", "candidate", "secret"] {
        let mut f = Fixture::new().await?;
        let mut owner = open_owner(&f, manager()).await?;
        let instance = installed::enroll(&f).await?;
        let binding = installed::binding(&f, instance).await?;
        effect(
            &f,
            &binding,
            json!({"operation":"state_set","key":"cursor","value":"source"}),
        )
        .await?;
        add_secret(&f, instance, "source_api", "private-api").await?;
        f.upgrade("2.0.0", None).await?;
        let (staged, plan, mutation, prepared) = source_prepared(&f, 2).await?;
        let fenced = quiesce_checked(step(&f, &staged, mutation, &plan)?, &prepared).await?;
        let scope = super::super::super::quiescence::observe_checked(
            step(&f, &staged, mutation, &plan)?,
            &fenced,
        )
        .await?;
        let guard = owner.quiesce(scope).await?;
        let copied = copy(&f, &staged, mutation, &plan, &guard).await?;
        match change {
            "secret" => {
                sqlx::query(
                    "UPDATE secrets SET rotatable=FALSE WHERE scope='instance' AND scope_id=$1",
                )
                .bind(staged.instance().to_string())
                .execute(&f.db.pool)
                .await?;
            }
            _ => {
                sqlx::query("UPDATE marketplace_instance_sdk_state SET revision=revision+1 WHERE instance_id=$1").bind(if change=="source" {instance} else {staged.instance()}.to_string()).execute(&f.db.pool).await?;
            }
        }
        let before = snapshot(&f).await?;
        let old = sdk_snapshot(&f, instance).await?;
        let target = sdk_snapshot(&f, staged.instance()).await?;
        let secret = secrets(&f, staged.instance()).await?;
        assert!(copy(&f, &staged, mutation, &plan, &guard).await.is_err());
        assert!(
            resume(
                &f.db,
                OwnerResumeIntent::fixture_for_actor(f.actor.clone(), &copied, Uuid::new_v4())?,
                limit()
            )
            .await
            .is_err()
        );
        assert_eq!(before, snapshot(&f).await?);
        assert_eq!(old, sdk_snapshot(&f, instance).await?);
        assert_eq!(target, sdk_snapshot(&f, staged.instance()).await?);
        assert_eq!(secret, secrets(&f, staged.instance()).await?);
    }
    Ok(())
}

#[tokio::test]
async fn source_owner_and_grant_are_fresh_and_safe_resume_has_reserved_global_capacity()
-> Result<()> {
    let f = Fixture::new().await?;
    let mut owner = open_owner(&f, manager()).await?;
    let (staged, plan, mutation, prepared) = source_prepared(&f, 0).await?;
    let fenced = quiesce_checked(step(&f, &staged, mutation, &plan)?, &prepared).await?;
    let scope = super::super::super::quiescence::observe_checked(
        step(&f, &staged, mutation, &plan)?,
        &fenced,
    )
    .await?;
    let guard = owner.quiesce(scope).await?;
    let before = snapshot(&f).await?;
    (f.public_owner)(false);
    assert!(copy(&f, &staged, mutation, &plan, &guard).await.is_err());
    (f.public_owner)(true);
    (f.meta_owner)(false);
    assert!(copy(&f, &staged, mutation, &plan, &guard).await.is_err());
    (f.meta_owner)(true);
    let mut wrong = step(&f, &staged, mutation, &plan)?;
    wrong.database = guard.database();
    wrong.intent = Arc::new(OwnerActivationIntent::fixture_for_actor(
        Actor {
            expires: Utc::now() - chrono::Duration::seconds(1),
            ..f.actor.clone()
        },
        &staged,
        &f.material()?.review,
        f.revision,
        mutation,
    )?);
    assert!(state_copy::copy_checked(wrong, &guard).await.is_err());
    assert_eq!(before, snapshot(&f).await?);
    sqlx::query("UPDATE marketplace_installation_budget SET revision=$1")
        .bind(MAX_REVISION - 4096)
        .execute(&f.db.pool)
        .await?;
    assert!(copy(&f, &staged, mutation, &plan, &guard).await.is_err());
    assert_eq!(
        sdk_snapshot(&f, staged.instance()).await?,
        (1, 0, 0, vec![])
    );
    // Even the last bounded revision is usable by an already-fenced abort.
    sqlx::query("UPDATE marketplace_installation_budget SET revision=$1")
        .bind(MAX_REVISION - 1)
        .execute(&f.db.pool)
        .await?;
    let mutation = Uuid::new_v4();
    resume(
        &f.db,
        OwnerResumeIntent::fixture_for_actor(f.actor.clone(), &fenced, mutation)?,
        limit(),
    )
    .await?;
    resume(
        &f.db,
        OwnerResumeIntent::fixture_for_actor(f.actor.clone(), &fenced, mutation)?,
        limit(),
    )
    .await?;
    assert_eq!(snapshot(&f).await?.3, MAX_REVISION);
    unused(&f).await
}

#[tokio::test]
async fn legacy_secret_writers_cannot_mutate_installed_or_journal_only_namespaces() -> Result<()> {
    for missing in ["none", "journal", "root", "downgraded_root"] {
        let f = Fixture::new().await?;
        let instance = installed::enroll(&f).await?;
        add_secret(&f, instance, "source_api", "keep").await?;
        if matches!(missing, "journal" | "downgraded_root") {
            sqlx::query("DELETE FROM marketplace_installations")
                .execute(&f.db.pool)
                .await?;
        }
        if missing == "root" {
            sqlx::query("DELETE FROM marketplace_instance_sdk_state")
                .execute(&f.db.pool)
                .await?;
        }
        if missing == "downgraded_root" {
            sqlx::query("UPDATE marketplace_instance_sdk_state SET purpose='foreground' WHERE instance_id=$1")
                .bind(instance.to_string()).execute(&f.db.pool).await?;
        }
        let before = secrets(&f, instance).await?;
        let store = ExtensionStore::new(&f.db.pool);
        assert!(
            store
                .upsert_secret(&NewSecret {
                    secret_id: Uuid::new_v4(),
                    scope: SecretScope::Instance,
                    scope_id: Some(instance),
                    key: "new".into(),
                    value_encrypted: manager().encrypt("forbidden")?,
                    rotatable: false
                })
                .await
                .is_err()
        );
        let id = before[0].0.parse()?;
        for key in ["source_api", "new_create_only"] {
            assert!(
                store
                    .insert_secret_if_absent(&NewSecret {
                        secret_id: Uuid::new_v4(),
                        scope: SecretScope::Instance,
                        scope_id: Some(instance),
                        key: key.into(),
                        value_encrypted: manager().encrypt("forbidden")?,
                        rotatable: false,
                    })
                    .await
                    .is_err(),
                "create-only must preserve installed and pending namespace guards"
            );
        }
        assert!(
            store
                .update_secret(id, &manager().encrypt("forbidden")?, false)
                .await
                .is_err()
        );
        assert!(store.delete_secret(id).await.is_err());
        assert!(
            store
                .delete_secrets_by_scope(SecretScope::Instance, Some(instance))
                .await
                .is_err()
        );
        assert_eq!(before, secrets(&f, instance).await?);
    }
    let f = Fixture::new().await?;
    legacy_roundtrip(&f.db).await?;
    // Create a genuinely fresh foreground namespace with the actual helper;
    // relabeling a formerly installed root must not erase its owner marker.
    let material = f.material()?;
    let grant =
        crate::marketplace::grant_store::load(&f.db, material.review.scope().extension_id())
            .await?
            .context("foreground control grant")?;
    let mut tx = f.db.pool.begin().await?;
    crate::marketplace::grant_store::installation::store::prepare_extension(
        &mut tx, &material, None,
    )
    .await?;
    tx.commit().await?;
    let instance = Uuid::new_v4();
    crate::marketplace::grant_store::sdk::initialize(
        &f.db,
        &grant,
        &material.review,
        instance,
        "fresh foreground secret control",
        || Ok(()),
    )
    .await?;
    let store = ExtensionStore::new(&f.db.pool);
    let data = NewSecret {
        secret_id: Uuid::new_v4(),
        scope: SecretScope::Instance,
        scope_id: Some(instance),
        key: "source_api".into(),
        value_encrypted: manager().encrypt("foreground")?,
        rotatable: false,
    };
    store.upsert_secret(&data).await?;
    store
        .update_secret(data.secret_id, &manager().encrypt("changed")?, true)
        .await?;
    store
        .delete_secrets_by_scope(data.scope, data.scope_id)
        .await?;
    assert!(secrets(&f, instance).await?.is_empty());
    Ok(())
}
