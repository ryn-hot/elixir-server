//! Real archive/crypto/SQL barrier. Initial ACTIVE enrollment and source-probe
//! completion are synthetic fixtures, NEVER production/native activation proof.
use super::super::quiescence::{OwnerResumeIntent, quiesce_checked, resume};
use super::*;
use crate::marketplace::grant_store::{
    installation::standing::{self, tests as installed},
    sdk::InstalledScope,
};
use elixir_marketplace::source_execution::{RuntimeJson, SourceSdkReply};
use serde_json::{Value, json};
use std::time::Duration;

mod candidate;
mod state_copy;

async fn source_prepared(
    f: &Fixture,
    expected: i64,
) -> Result<(
    StagedInstallation,
    SourceActivationPlan,
    Uuid,
    ActivationReference,
)> {
    source_prepared_with_plan(f, expected, plan(f)?).await
}
async fn source_prepared_with_plan(
    f: &Fixture,
    expected: i64,
    plan: SourceActivationPlan,
) -> Result<(
    StagedInstallation,
    SourceActivationPlan,
    Uuid,
    ActivationReference,
)> {
    let staged = f.stage(Uuid::new_v4(), expected, limit()).await?;
    let mutation = Uuid::new_v4();
    let prepared = prepared(f, &staged, mutation, &plan).await?;
    let start = match claim_checked(step(f, &staged, mutation, &plan)?, &prepared).await? {
        StartOrRecovery::Start(start) => start,
        _ => bail!("original fixture claim required"),
    };
    let reference = source_prepared::retain(f, &start.native_check(), &plan, limit()).await?;
    Ok((staged, plan, mutation, reference))
}

/// Initial probe completion remains synthetic. The separate runtime fixture
/// rehashes actual retained bytes and exercises REAL candidate native hooks.
pub(in crate::marketplace::grant_store::installation) async fn candidate_for_runtime(
    f: &Fixture,
    owner: &mut super::super::super::runtime::InstalledController,
    image: &str,
    budget: &SourceJobBudget,
    expected: i64,
) -> Result<(StagedInstallation, super::super::candidate::CandidateClaim)> {
    let material = f.material()?;
    let plan = SourceActivationPlan::new(
        material.authority.verify()?.release(),
        &material.review,
        image,
        budget,
    )?;
    let (staged, plan, mutation, prepared) = source_prepared_with_plan(f, expected, plan).await?;
    let fenced = quiesce_checked(step(f, &staged, mutation, &plan)?, &prepared).await?;
    let scope =
        super::super::quiescence::observe_checked(step(f, &staged, mutation, &plan)?, &fenced)
            .await?;
    let guard = owner.quiesce(scope).await?;
    let copied = state_copy::copy(f, &staged, mutation, &plan, &guard).await?;
    let mut checked = step(f, &staged, mutation, &plan)?;
    checked.database = guard.database();
    match super::super::candidate::claim_checked(checked, &guard, &copied).await? {
        super::super::candidate::CandidateOrRecovery::Claim(claim) => Ok((staged, claim)),
        _ => bail!("original candidate runtime fixture claim missing"),
    }
}
fn manager() -> SecretsManager {
    SecretsManager::from_key_bytes([57; 32], false)
}
fn controller_config(
    f: &Fixture,
    image: &str,
    deployment: &str,
) -> Result<super::super::super::runtime::InstalledRuntime> {
    use crate::{
        marketplace::archive::ArchiveBudget, playback::PlaybackJobManager,
        runtime::docker::DockerRuntimeManager,
    };
    Ok(super::super::super::runtime::InstalledRuntime {
        runtime: Arc::new(DockerRuntimeManager::new(None)),
        playback: Arc::new(PlaybackJobManager::new(f.db.pool.clone(), None)),
        local_http: None,
        application_http: None,
        compiled_toolchains: None,
        lane: f.root.path().canonicalize()?.join("expensive"),
        archive: f.root.path().canonicalize()?.join("archive"),
        deployment: deployment.into(),
        image: image.into(),
        source_budget: SourceJobBudget {
            memory_bytes: 64 * MIB,
            cpu_millicores: 100,
            disk_bytes: 128 * MIB,
            input_bytes: 64 * MIB,
            output_bytes: 64 * MIB,
            wall_seconds: 300,
        },
        retention: ArchiveBudget {
            bytes: 128 * MIB,
            releases: 4,
        },
    })
}
async fn effect(
    f: &Fixture,
    binding: &standing::InstalledBinding,
    request: Value,
) -> Result<SourceSdkReply> {
    InstalledScope {
        database: &f.db,
        secrets: &manager(),
        binding,
    }
    .execute(&RuntimeJson::from_value(request)?, 65536, || Ok(()))
    .await
}
async fn sdk_snapshot(
    f: &Fixture,
    instance: Uuid,
) -> Result<(i64, i64, i64, Vec<(String, String, i64)>)> {
    let head:(i64,i64,i64)=sqlx::query_as("SELECT revision,key_count,encoded_bytes FROM marketplace_instance_sdk_state WHERE instance_id=$1")
        .bind(instance.to_string()).fetch_one(&f.db.pool).await?;
    let values=sqlx::query_as("SELECT key,value_encrypted,revision FROM marketplace_instance_sdk_values WHERE instance_id=$1 ORDER BY key")
        .bind(instance.to_string()).fetch_all(&f.db.pool).await?;
    Ok((head.0, head.1, head.2, values))
}
fn altered(r: &ActivationReference) -> ActivationReference {
    ActivationReference {
        configured_instance: r.configured_instance,
        extension: r.extension.clone(),
        release: r.release,
        instance: r.instance,
        revision: r.revision,
        attempt: r.attempt,
        plan: r.plan,
    }
}

#[tokio::test]
async fn barrier_is_exact_durable_retriable_and_offline_abort_never_enables_a_candidate()
-> Result<()> {
    let mut f = Fixture::new().await?;
    let (staged, plan, mutation, prepared) = source_prepared(&f, 0).await?;
    let fenced = quiesce_checked(step(&f, &staged, mutation, &plan)?, &prepared).await?;
    assert_eq!(fenced.revision(), 5);
    let before = snapshot(&f).await?;
    let retry = quiesce_checked(step(&f, &staged, mutation, &plan)?, &prepared).await?;
    assert_eq!(retry.revision(), fenced.revision());
    assert_eq!(before, snapshot(&f).await?);
    let claim = prepare_checked(step(&f, &staged, mutation, &plan)?).await?;
    assert!(matches!(claim,PreparedOrRecovery::Recovery(r) if r.revision()==5));
    reopen(&mut f).await?;
    match inspect_as(&f.db, f.actor.clone(), staged.extension(), limit()).await? {
        ActivationStatus::Quiescing(r) => assert_eq!(r.attempt(), fenced.attempt()),
        _ => bail!("lost durable quiescence barrier"),
    }
    (f.meta_owner)(false);
    (f.public_owner)(false);
    sqlx::query("UPDATE marketplace_extension_grants SET review_json='{}'")
        .execute(&f.db.pool)
        .await?;
    let resume_mutation = Uuid::new_v4();
    let returned = resume(
        &f.db,
        OwnerResumeIntent::fixture_for_actor(f.actor.clone(), &fenced, resume_mutation)?,
        limit(),
    )
    .await?;
    assert_eq!(returned.revision(), 6);
    assert_eq!(returned.instance(), staged.instance());
    let after = snapshot(&f).await?;
    assert_eq!(
        resume(
            &f.db,
            OwnerResumeIntent::fixture_for_actor(f.actor.clone(), &fenced, resume_mutation)?,
            limit()
        )
        .await?
        .revision(),
        6
    );
    assert_eq!(after, snapshot(&f).await?);
    let mut wrong = altered(&fenced);
    wrong.plan = Digest::of(b"other plan");
    assert!(
        resume(
            &f.db,
            OwnerResumeIntent::fixture_for_actor(f.actor.clone(), &wrong, resume_mutation)?,
            limit()
        )
        .await
        .is_err()
    );
    assert_eq!(after, snapshot(&f).await?);
    unused(&f).await
}

#[tokio::test]
async fn barrier_fences_every_old_effect_and_abort_preserves_both_generations_without_copying()
-> Result<()> {
    let mut f = Fixture::new().await?;
    let instance = installed::enroll(&f).await?;
    let binding = installed::binding(&f, instance).await?;
    effect(
        &f,
        &binding,
        json!({"operation":"state_set","key":"cursor","value":"original"}),
    )
    .await?;
    let state = sdk_snapshot(&f, instance).await?;
    f.upgrade("2.0.0", None).await?;
    let (staged, plan, mutation, prepared) = source_prepared(&f, 2).await?;
    binding.current(&f.db).await?;
    let candidate = sdk_snapshot(&f, staged.instance()).await?;
    let fenced = quiesce_checked(step(&f, &staged, mutation, &plan)?, &prepared).await?;
    assert!(binding.current(&f.db).await.is_err());
    for request in [
        json!({"operation":"state_set","key":"cursor","value":"forbidden"}),
        json!({"operation":"state_delete","key":"cursor"}),
        json!({"operation":"secret","key":"source_api"}),
    ] {
        assert!(effect(&f, &binding, request).await.is_err());
    }
    assert!(installed::binding(&f, staged.instance()).await.is_err());
    assert_eq!(sdk_snapshot(&f, instance).await?, state);
    assert_eq!(sdk_snapshot(&f, staged.instance()).await?, candidate);
    (f.meta_owner)(false);
    (f.public_owner)(false);
    resume(
        &f.db,
        OwnerResumeIntent::fixture_for_actor(f.actor.clone(), &fenced, Uuid::new_v4())?,
        limit(),
    )
    .await?;
    binding.current(&f.db).await?;
    assert_eq!(sdk_snapshot(&f, instance).await?, state);
    assert_eq!(sdk_snapshot(&f, staged.instance()).await?, candidate);
    let version: String =
        sqlx::query_scalar("SELECT version FROM extensions WHERE extension_id=$1")
            .bind(staged.extension().as_str())
            .fetch_one(&f.db.pool)
            .await?;
    assert_eq!(version, "1.0.0");
    let enabled:i64=sqlx::query_scalar("SELECT CAST(CASE WHEN enabled THEN 1 ELSE 0 END AS BIGINT) FROM extension_instances WHERE instance_id=$1")
        .bind(staged.instance().to_string()).fetch_one(&f.db.pool).await?;
    assert_eq!(enabled, 0);
    assert_eq!(f.counts().await?, (1, 2, 1));
    Ok(())
}

#[tokio::test]
async fn changed_inputs_and_current_authority_cannot_partially_quiesce_or_repair_a_journal()
-> Result<()> {
    for fault in [
        "phase",
        "reference_revision",
        "attempt",
        "plan",
        "public",
        "metadata",
        "grant",
        "session",
        "quota",
    ] {
        let f = Fixture::new().await?;
        let (staged, plan, mutation, mut prepared) = source_prepared(&f, 0).await?;
        let mut budget = limit();
        let mut action = step(&f, &staged, mutation, &plan)?;
        match fault {
            "phase" => {
                let mut tx = f.db.pool.begin().await?;
                let ledger = store::lock_budget(&mut tx, budget).await?;
                let old = store::load(&mut tx, f.db.driver, staged.extension())
                    .await?
                    .unwrap();
                let mut journal: Journal =
                    canonical::decode(&canonical::to_vec(&old.journal)?, record::MAX_RECORD_BYTES)?;
                journal.activation.as_mut().unwrap().phase = ActivationPhase::Starting;
                journal.revision -= 1;
                store::write(&mut tx, ledger, Some(&old), journal, old.floor, budget).await?;
                tx.commit().await?;
            }
            "reference_revision" => prepared.revision -= 1,
            "attempt" => prepared.attempt = Uuid::new_v4(),
            "plan" => prepared.plan = Digest::of(b"wrong quiescence plan"),
            "public" => (f.public_owner)(false),
            "metadata" => (f.meta_owner)(false),
            "grant" => {
                sqlx::query("UPDATE marketplace_extension_grants SET enabled=FALSE")
                    .execute(&f.db.pool)
                    .await?;
            }
            "session" => {
                sqlx::query("UPDATE account_sessions SET revoked_at=CURRENT_TIMESTAMP,revoked_reason='quiescence fixture'")
                    .execute(&f.db.pool)
                    .await?;
            }
            "quota" => budget.entries = 0,
            _ => unreachable!(),
        }
        let before = snapshot(&f).await?;
        action.budget = budget;
        assert!(quiesce_checked(action, &prepared).await.is_err(), "{fault}");
        assert_eq!(snapshot(&f).await?, before, "{fault}");
        unused(&f).await?;
    }
    Ok(())
}

#[tokio::test]
async fn abort_refuses_changed_or_used_candidate_state_and_cannot_erase_another_attempt()
-> Result<()> {
    for fault in [
        "not_quiescing",
        "state",
        "enabled",
        "secret",
        "reference",
        "attempt",
        "session",
    ] {
        let f = Fixture::new().await?;
        let (staged, plan, mutation, prepared) = source_prepared(&f, 0).await?;
        let mut r = if fault == "not_quiescing" {
            prepared
        } else {
            quiesce_checked(step(&f, &staged, mutation, &plan)?, &prepared).await?
        };
        match fault {
            "state" => {
                sqlx::query("INSERT INTO marketplace_instance_sdk_values(instance_id,key,revision,value_encrypted,encoded_bytes) VALUES($1,'unexpected',1,'opaque',6)")
                .bind(staged.instance().to_string()).execute(&f.db.pool).await?;
            }
            "enabled" => {
                sqlx::query("UPDATE extension_instances SET enabled=TRUE WHERE instance_id=$1")
                    .bind(staged.instance().to_string())
                    .execute(&f.db.pool)
                    .await?;
            }
            "secret" => {
                crate::marketplace::grant_store::tests::secret(
                    &f.db,
                    "instance",
                    Some(staged.instance()),
                    "source_api",
                    "unexpected",
                    &manager(),
                )
                .await?;
            }
            "reference" => r.revision -= 1,
            "attempt" => r.attempt = Uuid::new_v4(),
            "session" => {
                sqlx::query("UPDATE account_sessions SET revoked_at=CURRENT_TIMESTAMP,revoked_reason='quiescence fixture'")
                    .execute(&f.db.pool)
                    .await?;
            }
            "not_quiescing" => {}
            _ => unreachable!(),
        }
        let before = snapshot(&f).await?;
        assert!(
            resume(
                &f.db,
                OwnerResumeIntent::fixture_for_actor(f.actor.clone(), &r, Uuid::new_v4())?,
                limit()
            )
            .await
            .is_err(),
            "{fault}"
        );
        assert_eq!(before, snapshot(&f).await?, "{fault}");
    }
    Ok(())
}

#[tokio::test]
async fn offline_abort_cannot_resurrect_an_old_generation_after_grant_revocation() -> Result<()> {
    let mut f = Fixture::new().await?;
    let instance = installed::enroll(&f).await?;
    let binding = installed::binding(&f, instance).await?;
    f.upgrade("2.0.0", None).await?;
    let (staged, plan, mutation, prepared) = source_prepared(&f, 2).await?;
    let r = quiesce_checked(step(&f, &staged, mutation, &plan)?, &prepared).await?;
    crate::marketplace::grant_store::revoke_as(
        &f.db,
        &f.actor,
        staged.extension(),
        2,
        Uuid::new_v4(),
        Utc::now(),
    )
    .await?;
    sqlx::query("UPDATE marketplace_extension_grants SET review_json='{}'")
        .execute(&f.db.pool)
        .await?;
    (f.meta_owner)(false);
    (f.public_owner)(false);
    resume(
        &f.db,
        OwnerResumeIntent::fixture_for_actor(f.actor.clone(), &r, Uuid::new_v4())?,
        limit(),
    )
    .await?;
    assert!(binding.current(&f.db).await.is_err());
    assert!(
        effect(
            &f,
            &binding,
            json!({"operation":"state_set","key":"cursor","value":"revived"})
        )
        .await
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn quiescing_phase_is_wire_bounded_and_requires_its_own_atomic_revision() -> Result<()> {
    let f = Fixture::new().await?;
    let (_, _, _, _) = source_prepared(&f, 0).await?;
    let mut journal: Journal =
        canonical::decode(snapshot(&f).await?.0.as_bytes(), record::MAX_RECORD_BYTES)?;
    journal.activation.as_mut().unwrap().phase = ActivationPhase::Quiescing;
    assert!(journal.validate().is_err());
    journal.revision += 1;
    journal.validate()?;
    let mut data = serde_json::to_value(&journal)?;
    data["activation"]["stopped"] = json!(true);
    assert!(
        canonical::decode::<Journal>(&canonical::to_vec(&data)?, record::MAX_RECORD_BYTES).is_err()
    );
    unused(&f).await
}

#[tokio::test]
async fn foreground_owner_code_cannot_adopt_enrolled_state_even_with_a_missing_journal()
-> Result<()> {
    use crate::marketplace::grant_store::sdk::LocalScope;
    for fault in [
        "enrolled",
        "quiescing",
        "missing_journal",
        "changed_purpose",
        "corrupt_journal",
    ] {
        let mut f = Fixture::new().await?;
        let instance = installed::enroll(&f).await?;
        let binding = installed::binding(&f, instance).await?;
        effect(
            &f,
            &binding,
            json!({"operation":"state_set","key":"cursor","value":"protected"}),
        )
        .await?;
        let grant = f.upgrade("2.0.0", None).await?;
        let requested = f.material()?.review;
        if fault == "quiescing" {
            let (staged, plan, mutation, prepared) = source_prepared(&f, 2).await?;
            quiesce_checked(step(&f, &staged, mutation, &plan)?, &prepared).await?;
        }
        match fault {
            "missing_journal" => {
                sqlx::query("DELETE FROM marketplace_installations")
                    .execute(&f.db.pool)
                    .await?;
            }
            "changed_purpose" => {
                sqlx::query("UPDATE marketplace_instance_sdk_state SET purpose='foreground'")
                    .execute(&f.db.pool)
                    .await?;
            }
            "corrupt_journal" => {
                sqlx::query("UPDATE marketplace_installations SET record_sha256=$1")
                    .bind(Digest::of(b"corrupt quiescence fixture journal").to_string())
                    .execute(&f.db.pool)
                    .await?;
            }
            _ => {}
        }
        let before = sdk_snapshot(&f, instance).await?;
        for raw in [
            json!({"operation":"state_set","key":"cursor","value":"bypass"}),
            json!({"operation":"secret","key":"source_api"}),
        ] {
            assert!(
                LocalScope {
                    database: &f.db,
                    secrets: &manager(),
                    grant: &grant,
                    requested: &requested,
                    instance
                }
                .execute(&RuntimeJson::from_value(raw)?, None, || Ok(()))
                .await
                .is_err(),
                "{fault}"
            );
        }
        assert!(
            crate::marketplace::grant_store::sdk::initialize(
                &f.db,
                &grant,
                &requested,
                Uuid::new_v4(),
                "alternate foreground",
                || Ok(())
            )
            .await
            .is_err(),
            "{fault}"
        );
        assert_eq!(sdk_snapshot(&f, instance).await?, before, "{fault}");
    }
    Ok(())
}

#[tokio::test]
async fn namespace_migration_preserves_existing_ciphertext_and_defaults_to_installed_restrictions()
-> Result<()> {
    let f = Fixture::new().await?;
    let instance = installed::enroll(&f).await?;
    let binding = installed::binding(&f, instance).await?;
    effect(
        &f,
        &binding,
        json!({"operation":"state_set","key":"cursor","value":{"migration":"preserve"}}),
    )
    .await?;
    let before = sdk_snapshot(&f, instance).await?;
    // Recreate the actual populated v66 table shape without changing any
    // authority, encrypted entries, counters or FK relationships.
    sqlx::query("DROP INDEX marketplace_sdk_namespace_extension_purpose")
        .execute(&f.db.pool)
        .await?;
    sqlx::query("ALTER TABLE marketplace_instance_sdk_state DROP COLUMN purpose")
        .execute(&f.db.pool)
        .await?;
    let columns = sqlx::query("PRAGMA table_info(marketplace_instance_sdk_state)")
        .fetch_all(&f.db.pool)
        .await?;
    assert!(
        !columns.iter().any(|row| row
            .try_get::<String, _>("name")
            .is_ok_and(|name| name == "purpose")),
        "fixture must actually have the populated v66 schema before upgrade"
    );
    // Restore ONLY this private fixture's matching migration ledger, then use
    // the actual production migrator rather than Any's ad-hoc multi-SQL path.
    assert_eq!(
        sqlx::query("DELETE FROM _sqlx_migrations WHERE version=67")
            .execute(&f.db.pool)
            .await?
            .rows_affected(),
        1
    );
    f.db.run_migrations().await?;
    let purpose: String = sqlx::query_scalar(
        "SELECT purpose FROM marketplace_instance_sdk_state WHERE instance_id=$1",
    )
    .bind(instance.to_string())
    .fetch_one(&f.db.pool)
    .await?;
    assert_eq!(purpose, "installed");
    assert_eq!(before, sdk_snapshot(&f, instance).await?);
    binding.current(&f.db).await?;
    let SourceSdkReply::Value(value) = effect(
        &f,
        &binding,
        json!({"operation":"state_get","key":"cursor"}),
    )
    .await?
    else {
        bail!("migrated state unavailable");
    };
    assert_eq!(
        serde_json::from_str::<Value>(value.as_str())?,
        json!({"migration":"preserve"})
    );
    let plans=sqlx::query("EXPLAIN QUERY PLAN SELECT 1 FROM marketplace_instance_sdk_state WHERE extension_id=$1 AND purpose='installed'")
        .bind(f.descriptor.release.payload.extension_id.as_str()).fetch_all(&f.db.pool).await?;
    assert!(plans.iter().any(|row| {
        row.try_get::<String, _>("detail")
            .is_ok_and(|s| s.contains("marketplace_sdk_namespace_extension_purpose"))
    }));
    Ok(())
}

#[tokio::test]
async fn cutover_observation_is_read_only_exact_and_rechecks_source_owner_grant_and_unused_candidate()
-> Result<()> {
    use super::super::quiescence::observe_checked;
    for fault in [
        "none",
        "metadata",
        "public",
        "grant",
        "session",
        "reference",
        "candidate",
    ] {
        let f = Fixture::new().await?;
        let (staged, plan, mutation, prepared) = source_prepared(&f, 0).await?;
        assert!(
            observe_checked(step(&f, &staged, mutation, &plan)?, &prepared)
                .await
                .is_err(),
            "a probe/phase is not quiescence"
        );
        let fenced = quiesce_checked(step(&f, &staged, mutation, &plan)?, &prepared).await?;
        let mut expected = altered(&fenced);
        let checked = step(&f, &staged, mutation, &plan)?;
        match fault {
            "metadata" => (f.meta_owner)(false),
            "public" => (f.public_owner)(false),
            "grant" => {
                sqlx::query("UPDATE marketplace_extension_grants SET enabled=FALSE")
                    .execute(&f.db.pool)
                    .await?;
            }
            "session" => {
                sqlx::query("UPDATE account_sessions SET revoked_at=CURRENT_TIMESTAMP,revoked_reason='cutover fixture logout'").execute(&f.db.pool).await?;
            }
            "reference" => expected.revision += 1,
            "candidate" => {
                sqlx::query("UPDATE extension_instances SET enabled=TRUE WHERE instance_id=$1")
                    .bind(staged.instance().to_string())
                    .execute(&f.db.pool)
                    .await?;
            }
            _ => {}
        }
        let before = snapshot(&f).await?;
        let result = observe_checked(checked, &expected).await;
        if fault == "none" {
            result?.current(&f.db).await?;
        } else {
            assert!(result.is_err(), "{fault}");
        }
        assert_eq!(before, snapshot(&f).await?, "{fault}");
    }
    Ok(())
}

#[tokio::test]
async fn cutover_guard_rechecks_exact_generation_and_cannot_outlive_an_independent_abort()
-> Result<()> {
    use super::super::super::runtime::InstalledController;
    use super::super::quiescence::observe_checked;
    let mut f = Fixture::new().await?;
    let mut owner = InstalledController::open(
        f.db.clone(),
        manager(),
        controller_config(&f, &format!("sha256:{}", "1".repeat(64)), "guard-fixture")?,
    )
    .await?;
    let old = installed::enroll(&f).await?;
    f.upgrade("2.0.0", None).await?;
    let (staged, plan, mutation, prepared) = source_prepared(&f, 2).await?;
    let fenced = quiesce_checked(step(&f, &staged, mutation, &plan)?, &prepared).await?;
    let scope = observe_checked(step(&f, &staged, mutation, &plan)?, &fenced).await?;
    let guard = owner.quiesce(scope).await?;
    let mut tx = f.db.pool.begin().await?;
    let mut current = store::load(&mut tx, f.db.driver, staged.extension())
        .await?
        .unwrap();
    guard.require(&mut tx, f.db.driver, &current).await?;
    // Coordinate equality cannot hide a changed original active capsule.
    current.journal.active.as_mut().unwrap().grant_revision += 1;
    assert!(guard.require(&mut tx, f.db.driver, &current).await.is_err());
    tx.rollback().await?;
    let before = sdk_snapshot(&f, old).await?;
    resume(
        &f.db,
        OwnerResumeIntent::fixture_for_actor(f.actor.clone(), &fenced, Uuid::new_v4())?,
        limit(),
    )
    .await?;
    let mut tx = f.db.pool.begin().await?;
    let current = store::load(&mut tx, f.db.driver, staged.extension())
        .await?
        .unwrap();
    assert!(
        guard.require(&mut tx, f.db.driver, &current).await.is_err(),
        "a saved borrow is not a reusable barrier/activation permit"
    );
    tx.rollback().await?;
    drop(guard);
    assert!(
        observe_checked(step(&f, &staged, mutation, &plan)?, &fenced)
            .await
            .is_err()
    );
    installed::binding(&f, old).await?;
    assert_eq!(before, sdk_snapshot(&f, old).await?);
    assert!(installed::binding(&f, staged.instance()).await.is_err());
    Ok(())
}

#[tokio::test]
async fn saved_cutover_coordinates_allow_owned_offline_cleanup_but_never_restore_a_revoked_grant()
-> Result<()> {
    use super::super::super::runtime::InstalledController;
    use super::super::quiescence::observe_checked;
    let mut f = Fixture::new().await?;
    let mut owner = InstalledController::open(
        f.db.clone(),
        manager(),
        controller_config(&f, &format!("sha256:{}", "1".repeat(64)), "offline-guard")?,
    )
    .await?;
    let old = installed::enroll(&f).await?;
    f.upgrade("2.0.0", None).await?;
    let (staged, plan, mutation, prepared) = source_prepared(&f, 2).await?;
    let fenced = quiesce_checked(step(&f, &staged, mutation, &plan)?, &prepared).await?;
    let scope = observe_checked(step(&f, &staged, mutation, &plan)?, &fenced).await?;
    let checked = step(&f, &staged, mutation, &plan)?;
    (f.meta_owner)(false);
    (f.public_owner)(false);
    sqlx::query("UPDATE marketplace_extension_grants SET enabled=FALSE,revocation_revision=revision,review_json='{}'").execute(&f.db.pool).await?;
    assert!(observe_checked(checked, &fenced).await.is_err());
    let guard = owner.quiesce(scope).await?;
    let mut tx = f.db.pool.begin().await?;
    let current = store::load(&mut tx, f.db.driver, staged.extension())
        .await?
        .unwrap();
    guard.require(&mut tx, f.db.driver, &current).await?;
    tx.rollback().await?;
    drop(guard);
    resume(
        &f.db,
        OwnerResumeIntent::fixture_for_actor(f.actor.clone(), &fenced, Uuid::new_v4())?,
        limit(),
    )
    .await?;
    assert!(installed::binding(&f, old).await.is_err());
    assert!(installed::binding(&f, staged.instance()).await.is_err());
    Ok(())
}

#[tokio::test]
#[ignore = "explicit fresh isolated loopback PostgreSQL quiescence/SDK transaction barrier and restart; synthetic ACTIVE/probe completion"]
async fn postgres_quiescence_waits_for_the_original_effect_transaction_and_survives_restart()
-> Result<()> {
    let url = std::env::var("ELIXIR_TEST_MARKETPLACE_QUIESCENCE_POSTGRES_URL")?;
    let db = fresh_postgres(&url).await?;
    // The vendored Any adapter's SQLite widening must NOT reinterpret PG INT4
    // binary values as eight-byte integers or break existing NULL handling.
    for value in [
        i64::MIN,
        i32::MIN as i64 - 1,
        0,
        i32::MAX as i64 + 1,
        MAX_REVISION,
        i64::MAX,
    ] {
        let actual: i64 = sqlx::query_scalar("SELECT CAST($1 AS BIGINT)")
            .bind(value)
            .fetch_one(&db.pool)
            .await?;
        assert_eq!(actual, value);
    }
    let small: i32 = sqlx::query_scalar("SELECT CAST(2147483647 AS INTEGER)")
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(small, i32::MAX);
    let null: Option<i64> = sqlx::query_scalar("SELECT CAST(NULL AS BIGINT)")
        .fetch_one(&db.pool)
        .await?;
    assert!(null.is_none());
    let mut fixture = Fixture::with_database(tempfile::tempdir()?, db.clone()).await?;
    let initial_controller = super::super::super::runtime::InstalledController::open(
        db.clone(),
        manager(),
        controller_config(
            &fixture,
            &format!("sha256:{}", "1".repeat(64)),
            "postgres-cutover-init",
        )?,
    )
    .await?;
    let instance = installed::enroll(&fixture).await?;
    let binding = installed::binding(&fixture, instance).await?;
    effect(
        &fixture,
        &binding,
        json!({"operation":"state_set","key":"cursor","value":"original"}),
    )
    .await?;
    fixture.upgrade("2.0.0", None).await?;
    let (staged, plan, mutation, prepared) = source_prepared(&fixture, 2).await?;
    let f = Arc::new(fixture);
    let staged = Arc::new(staged);
    let plan = Arc::new(plan);
    let prepared = Arc::new(prepared);
    let mut reader = db.pool.begin().await?;
    binding
        .require_in_transaction(&mut reader, db.driver)
        .await?;
    let writer = tokio::spawn({
        let f = f.clone();
        let staged = staged.clone();
        let plan = plan.clone();
        let prepared = prepared.clone();
        async move { quiesce_checked(step(&f, &staged, mutation, &plan)?, &prepared).await }
    });
    tokio::time::timeout(Duration::from_secs(5),async {
        loop {
            let blocked:i64=sqlx::query_scalar("SELECT COUNT(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND cardinality(pg_blocking_pids(pid))>0")
                .fetch_one(&db.pool).await?;
            if blocked>0 { return Ok::<_,anyhow::Error>(()); }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await??;
    assert!(!writer.is_finished());
    reader.rollback().await?;
    let fenced = tokio::time::timeout(Duration::from_secs(5), writer).await???;
    assert!(binding.current(&db).await.is_err());
    let scope =
        super::super::quiescence::observe_checked(step(&f, &staged, mutation, &plan)?, &fenced)
            .await?;
    drop(initial_controller);
    db.pool.close().await;
    let reopened = Database::connect(&DatabaseConfig {
        url,
        max_connections: 6,
        connect_timeout_seconds: 5,
    })
    .await?;
    assert!(binding.current(&reopened).await.is_err());
    match inspect_as(&reopened, f.actor.clone(), staged.extension(), limit()).await? {
        ActivationStatus::Quiescing(r) => assert_eq!(r.revision(), fenced.revision()),
        _ => bail!("quiescence lost after restart"),
    }
    let mut controller = super::super::super::runtime::InstalledController::open(
        reopened.clone(),
        manager(),
        controller_config(
            &f,
            &format!("sha256:{}", "1".repeat(64)),
            "postgres-cutover",
        )?,
    )
    .await?;
    let guard = controller.quiesce(scope).await?;
    let mut tx = reopened.pool.begin().await?;
    let current = store::load(&mut tx, reopened.driver, staged.extension())
        .await?
        .unwrap();
    guard.require(&mut tx, reopened.driver, &current).await?;
    tx.rollback().await?;
    let copied = state_copy::copy(&f, &staged, mutation, &plan, &guard).await?;
    let copied_head: (i64, i64) = sqlx::query_as(
        "SELECT revision,key_count FROM marketplace_instance_sdk_state WHERE instance_id=$1",
    )
    .bind(staged.instance().to_string())
    .fetch_one(&reopened.pool)
    .await?;
    assert_eq!(copied_head, (2, 1));
    state_copy::legacy_roundtrip(&reopened).await?;
    drop(guard);
    (f.meta_owner)(false);
    (f.public_owner)(false);
    resume(
        &reopened,
        OwnerResumeIntent::fixture_for_actor(f.actor.clone(), &copied, Uuid::new_v4())?,
        limit(),
    )
    .await?;
    binding.current(&reopened).await?;
    let discarded:(i64,i64,i64)=sqlx::query_as("SELECT revision,key_count,encoded_bytes FROM marketplace_instance_sdk_state WHERE instance_id=$1")
        .bind(staged.instance().to_string()).fetch_one(&reopened.pool).await?;
    assert_eq!(discarded, (1, 0, 0));
    let value = InstalledScope {
        database: &reopened,
        secrets: &manager(),
        binding: &binding,
    }
    .execute(
        &RuntimeJson::from_value(json!({"operation":"state_get","key":"cursor"}))?,
        65536,
        || Ok(()),
    )
    .await?;
    let SourceSdkReply::Value(value) = value else {
        bail!("original state unavailable after abort");
    };
    assert_eq!(
        serde_json::from_str::<Value>(value.as_str())?,
        json!("original")
    );
    reopened.pool.close().await;
    Ok(())
}

#[tokio::test]
#[ignore = "explicit offline Docker old-generation native call crosses real quiescence/abort; synthetic ACTIVE enrollment and candidate probe completion"]
async fn native_quiescence_stops_the_actual_old_call_without_promoting_or_mutating_the_candidate()
-> Result<()> {
    use crate::{
        marketplace::{
            archive::ArchiveBudget,
            grant_store::installation::runtime::{InstalledController, InstalledRuntime},
            lease::ControllerLease,
        },
        playback::PlaybackJobManager,
        runtime::{RuntimeManager, docker::DockerRuntimeManager, model::ContainerHandle},
    };
    use elixir_marketplace::source_execution::{SourceExecutionLimits, SourceSdkLimits};
    use futures_util::FutureExt;
    let image = std::env::var("ELIXIR_MARKETPLACE_WORKER_IMAGE")?;
    if !crate::marketplace::runtime::image_is_pinned(&image) {
        bail!("explicit immutable worker image required");
    }
    let mut f=Fixture::with_source(Some(r#"import {state} from '@elixir/sdk/v1';
export async function invoke(a) { await state.set('cursor',a.value); if(a.loop){for(;;){}} return {version:1,value:await state.get('cursor')}; }"#)).await?;
    drop(
        InstalledController::open(
            f.db.clone(),
            manager(),
            controller_config(&f, &image, "cutover-bootstrap")?,
        )
        .await?,
    );
    let instance = installed::enroll(&f).await?;
    f.upgrade(
        "2.0.0",
        Some("export async function invoke(){return {version:2}}\n"),
    )
    .await?;
    let (staged, plan, mutation, prepared) = source_prepared(&f, 2).await?;
    let runtime = Arc::new(DockerRuntimeManager::new(None));
    let deployment = format!("cutover-{}", Uuid::new_v4().simple());
    let lane = f.root.path().canonicalize()?.join("expensive");
    let limits = SourceExecutionLimits {
        profile: Default::default(),
        heap_bytes: 16 * MIB,
        timeout_millis: 30000,
        max_file_bytes: 4 * MIB,
        json_bytes: MIB,
        pending_jobs: 128,
        sdk: Some(SourceSdkLimits {
            calls: 4,
            request_bytes: MIB,
            response_bytes: MIB,
        }),
    };
    let config = || -> Result<InstalledRuntime> {
        Ok(InstalledRuntime {
            runtime: runtime.clone(),
            playback: Arc::new(PlaybackJobManager::new(f.db.pool.clone(), None)),
            local_http: None,
            application_http: None,
            compiled_toolchains: None,
            lane: lane.clone(),
            archive: f.root.path().canonicalize()?.join("archive"),
            deployment: deployment.clone(),
            image: image.clone(),
            source_budget: SourceJobBudget {
                memory_bytes: 64 * MIB,
                cpu_millicores: 100,
                disk_bytes: 128 * MIB,
                input_bytes: 64 * MIB,
                output_bytes: 64 * MIB,
                wall_seconds: 300,
            },
            retention: ArchiveBudget {
                bytes: 128 * MIB,
                releases: 4,
            },
        })
    };
    let mut call = Some(InstalledController::open(f.db.clone(), manager(), config()?).await?);
    let result = std::panic::AssertUnwindSafe(async {
        call.as_mut().unwrap().begin(
            installed::binding(&f, instance).await?,
            limits.clone(),
            RuntimeJson::from_value(json!({"value":"old-started","loop":true}))?,
        )?;
        tokio::time::timeout(Duration::from_secs(280), async {
            loop {
                let count: i64 = sqlx::query_scalar(
                    "SELECT key_count FROM marketplace_instance_sdk_state WHERE instance_id=$1",
                )
                .bind(instance.to_string())
                .fetch_one(&f.db.pool)
                .await?;
                if count == 1 {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await??;
        let writer = runtime
            .get_container_handle(&format!("elixir-marketplace-source-writer-{deployment}"))
            .await?
            .context("old native writer is missing")?;
        let identity = ContainerHandle {
            id: writer.id.clone(),
            name: writer.id.clone(),
        };
        assert!(
            runtime
                .inspect(&identity)
                .await
                .context("inspect actual live old native identity before quiescence")?
                .running,
            "old interpreter must actually be live before the barrier"
        );
        let original = sdk_snapshot(&f, instance).await?;
        let candidate = sdk_snapshot(&f, staged.instance()).await?;
        let fenced = quiesce_checked(step(&f, &staged, mutation, &plan)?, &prepared).await?;
        let scope =
            super::super::quiescence::observe_checked(step(&f, &staged, mutation, &plan)?, &fenced)
                .await?;
        // The controller still owns the actual live old call here. Its scope
        // cannot be handed off before the SAME task confirms native disposal.
        let guard = tokio::time::timeout(
            Duration::from_secs(45),
            call.as_mut().unwrap().quiesce(scope),
        )
        .await??;
        let mut tx = f.db.pool.begin().await?;
        let current = store::load(&mut tx, f.db.driver, staged.extension())
            .await?
            .unwrap();
        guard.require(&mut tx, f.db.driver, &current).await?;
        tx.rollback().await?;
        let copied = state_copy::copy(&f, &staged, mutation, &plan, &guard).await?;
        assert_eq!(sdk_snapshot(&f, staged.instance()).await?.1, 1);
        drop(guard);
        assert!(
            call.as_mut().unwrap().finish().await.is_err(),
            "quiescence cannot expose the cancelled source result"
        );
        assert!(
            // Use the same positive exact-ID absence lookup as source-job
            // retirement. The configuration inspection API deliberately
            // returns an error, not None, for a missing immutable ID.
            runtime
                .get_container_handle(&writer.id)
                .await
                .context("confirm retirement of the actual immutable old native identity")?
                .is_none(),
            "actual immutable old native ID must be retired"
        );
        let held = ControllerLease::acquire(&lane)?;
        assert!(crate::marketplace::lease::activity::Fence::load(&held)?.is_none());
        drop(held);
        assert_eq!(original, sdk_snapshot(&f, instance).await?);
        assert_eq!(sdk_snapshot(&f, staged.instance()).await?.0, 2);
        assert!(installed::binding(&f, staged.instance()).await.is_err());
        (f.meta_owner)(false);
        (f.public_owner)(false);
        resume(
            &f.db,
            OwnerResumeIntent::fixture_for_actor(f.actor.clone(), &copied, Uuid::new_v4())?,
            limit(),
        )
        .await?;
        call.as_mut().unwrap().begin(
            installed::binding(&f, instance).await?,
            limits,
            RuntimeJson::from_value(json!({"value":"resumed"}))?,
        )?;
        let value = call.as_mut().unwrap().finish().await?;
        assert_eq!(
            serde_json::from_str::<Value>(value.as_str())?,
            json!({"version":1,"value":"resumed"})
        );
        assert_eq!(candidate, sdk_snapshot(&f, staged.instance()).await?);
        Ok::<_, anyhow::Error>(())
    })
    .catch_unwind()
    .await;
    if let Some(call) = call.as_mut() {
        call.cancel().await?;
    }
    let held = ControllerLease::acquire(&lane)?;
    assert!(crate::marketplace::lease::activity::Fence::load(&held)?.is_none());
    drop(held);
    match result {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
