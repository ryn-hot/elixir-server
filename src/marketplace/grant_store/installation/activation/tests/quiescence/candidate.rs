//! Real SQL/encryption/controller ownership; source-probe/ACTIVE enrollment
//! and direct SDK-current closures are synthetic component fixtures. These
//! tests do NOT qualify native candidate execution, retirement or activation.
use super::super::super::candidate::{self as candidate_api, CandidateClaim, CandidateOrRecovery};
use super::*;
use crate::marketplace::grant_store::{
    installation::runtime::{InstalledController, QuiescentRuntime},
    sdk::CandidateScope,
};
use std::cell::Cell;

async fn claim(
    f: &Fixture,
    staged: &StagedInstallation,
    mutation: Uuid,
    plan: &SourceActivationPlan,
    guard: &QuiescentRuntime<'_>,
    copied: &ActivationReference,
) -> Result<CandidateOrRecovery> {
    let mut checked = step(f, staged, mutation, plan)?;
    checked.database = guard.database();
    candidate_api::claim_checked(checked, guard, copied).await
}
async fn start(
    f: &Fixture,
    owner: &mut InstalledController,
    expected: i64,
) -> Result<(StagedInstallation, CandidateClaim, ActivationReference)> {
    let (staged, plan, mutation, prepared) = source_prepared(f, expected).await?;
    let fenced = quiesce_checked(step(f, &staged, mutation, &plan)?, &prepared).await?;
    let scope = super::super::super::quiescence::observe_checked(
        step(f, &staged, mutation, &plan)?,
        &fenced,
    )
    .await?;
    let guard = owner.quiesce(scope).await?;
    let copied = state_copy::copy(f, &staged, mutation, &plan, &guard).await?;
    let CandidateOrRecovery::Claim(claim) =
        claim(f, &staged, mutation, &plan, &guard, &copied).await?
    else {
        bail!("fresh candidate claim missing");
    };
    Ok((staged, claim, copied))
}
async fn call(f: &Fixture, claim: &CandidateClaim, request: Value) -> Result<SourceSdkReply> {
    CandidateScope {
        database: &f.db,
        secrets: &manager(),
        binding: claim.binding(),
    }
    .fixture_execute(&RuntimeJson::from_value(request)?, 65536, || Ok(()))
    .await
}
fn value(reply: SourceSdkReply) -> Result<Value> {
    let SourceSdkReply::Value(raw) = reply else {
        bail!("expected candidate value");
    };
    Ok(serde_json::from_str(raw.as_str())?)
}

#[tokio::test]
async fn candidate_claim_is_one_way_exact_disabled_and_never_recreated_by_replay_or_status()
-> Result<()> {
    let mut f = Fixture::new().await?;
    let mut owner = state_copy::open_owner(&f, manager()).await?;
    let (staged, plan, mutation, prepared) = source_prepared(&f, 0).await?;
    let fenced = quiesce_checked(step(&f, &staged, mutation, &plan)?, &prepared).await?;
    let scope = super::super::super::quiescence::observe_checked(
        step(&f, &staged, mutation, &plan)?,
        &fenced,
    )
    .await?;
    let guard = owner.quiesce(scope).await?;
    let copied = state_copy::copy(&f, &staged, mutation, &plan, &guard).await?;
    let before = snapshot(&f).await?;
    // Equal-looking records do not let a caller select another DB handle.
    assert!(
        candidate_api::claim_checked(step(&f, &staged, mutation, &plan)?, &guard, &copied)
            .await
            .is_err()
    );
    for field in ["revision", "attempt", "instance", "plan", "release"] {
        let mut wrong = altered(&copied);
        match field {
            "revision" => wrong.revision -= 1,
            "attempt" => wrong.attempt = Uuid::new_v4(),
            "instance" => wrong.instance = Uuid::new_v4(),
            "plan" => wrong.plan = Digest::of(b"wrong plan"),
            _ => wrong.release = Digest::of(b"wrong release"),
        }
        assert!(
            claim(&f, &staged, mutation, &plan, &guard, &wrong)
                .await
                .is_err()
        );
        assert_eq!(before, snapshot(&f).await?);
    }
    let CandidateOrRecovery::Claim(original) =
        claim(&f, &staged, mutation, &plan, &guard, &copied).await?
    else {
        bail!("first claim missing");
    };
    assert_eq!(
        serde_json::from_str::<Value>(original.context().arguments()?.as_str())?,
        json!({
            "schema_version":1,"from_version":null,"to_version":"1.0.0",
        })
    );
    let saved = snapshot(&f).await?;
    let status = match inspect_as(&f.db, f.actor.clone(), staged.extension(), limit()).await? {
        ActivationStatus::CandidateStarting(reference) => reference,
        _ => bail!("candidate phase lost"),
    };
    assert_eq!(status.revision(), 7);
    for _ in 0..2 {
        assert!(
            matches!(claim(&f, &staged, mutation, &plan, &guard, &copied).await?,
            CandidateOrRecovery::Recovery(r) if r.revision() == 7 && r.attempt() == copied.attempt())
        );
        assert_eq!(saved, snapshot(&f).await?);
    }
    assert!(matches!(
        prepare_checked(step(&f, &staged, mutation, &plan)?).await?,
        PreparedOrRecovery::Recovery(_)
    ));
    assert!(
        state_copy::copy(&f, &staged, mutation, &plan, &guard)
            .await
            .is_err()
    );
    for reference in [&copied, &status] {
        assert!(
            resume(
                &f.db,
                OwnerResumeIntent::fixture_for_actor(f.actor.clone(), reference, Uuid::new_v4(),)?,
                limit()
            )
            .await
            .is_err()
        );
    }
    assert_eq!(saved, snapshot(&f).await?);
    drop(original);
    drop(guard);
    drop(owner);
    reopen(&mut f).await?;
    assert!(
        matches!(inspect_as(&f.db, f.actor.clone(), staged.extension(), limit()).await?,
        ActivationStatus::CandidateStarting(r) if r.revision() == 7)
    );
    assert!(
        matches!(prepare_checked(step(&f, &staged, mutation, &plan)?).await?,
        PreparedOrRecovery::Recovery(r) if r.revision() == 7)
    );
    assert_eq!(saved, snapshot(&f).await?);
    unused(&f).await
}

#[tokio::test]
async fn candidate_effects_mutate_only_the_disabled_copy_and_keep_active_execution_fenced()
-> Result<()> {
    let mut f = Fixture::new().await?;
    let mut owner = state_copy::open_owner(&f, manager()).await?;
    let old = installed::enroll(&f).await?;
    let active = installed::binding(&f, old).await?;
    effect(
        &f,
        &active,
        json!({"operation":"state_set","key":"cursor","value":"original"}),
    )
    .await?;
    sqlx::query("INSERT INTO secrets(secret_id,scope,scope_id,key,value_encrypted,rotatable) VALUES($1,'instance',$2,'source_api',$3,TRUE)")
        .bind(Uuid::new_v4().to_string()).bind(old.to_string()).bind(manager().encrypt("scoped-secret")?).execute(&f.db.pool).await?;
    let original = sdk_snapshot(&f, old).await?;
    f.upgrade("2.0.0", None).await?;
    let (staged, claim, _) = start(&f, &mut owner, 2).await?;
    assert_eq!(
        serde_json::from_str::<Value>(claim.context().arguments()?.as_str())?,
        json!({
            "schema_version":1,"from_version":"1.0.0","to_version":"2.0.0",
        })
    );
    assert_eq!(
        value(call(&f, &claim, json!({"operation":"state_get","key":"cursor"})).await?)?,
        json!("original")
    );
    call(
        &f,
        &claim,
        json!({"operation":"state_set","key":"cursor","value":"migrated"}),
    )
    .await?;
    assert_eq!(
        value(call(&f, &claim, json!({"operation":"state_get","key":"cursor"})).await?)?,
        json!("migrated")
    );
    assert_eq!(
        value(call(&f, &claim, json!({"operation":"secret","key":"source_api"})).await?)?,
        json!("scoped-secret")
    );
    assert!(matches!(
        call(
            &f,
            &claim,
            json!({"operation":"secret","key":"global_password"})
        )
        .await?,
        SourceSdkReply::Denied
    ));
    assert!(matches!(call(&f, &claim, json!({"operation":"fetch","url":"https://example.com/","method":"GET","headers":[],"body":null})).await?, SourceSdkReply::Denied));
    assert_eq!(original, sdk_snapshot(&f, old).await?);
    assert!(active.current(&f.db).await.is_err());
    assert!(installed::binding(&f, staged.instance()).await.is_err());
    assert!(
        effect(
            &f,
            &active,
            json!({"operation":"state_set","key":"cursor","value":"late old write"})
        )
        .await
        .is_err()
    );
    assert_eq!(sdk_snapshot(&f, staged.instance()).await?.0, 3);
    let enabled:i64=sqlx::query_scalar("SELECT CAST(CASE WHEN enabled THEN 1 ELSE 0 END AS BIGINT) FROM extension_instances WHERE instance_id=$1")
        .bind(staged.instance().to_string()).fetch_one(&f.db.pool).await?;
    assert_eq!(enabled, 0);
    Ok(())
}

#[tokio::test]
async fn candidate_effect_rechecks_cancellation_and_reply_limits_before_commit() -> Result<()> {
    let f = Fixture::new().await?;
    let mut owner = state_copy::open_owner(&f, manager()).await?;
    let (staged, claim, _) = start(&f, &mut owner, 0).await?;
    let initial = sdk_snapshot(&f, staged.instance()).await?;
    let request = RuntimeJson::from_value(
        json!({"operation":"state_set","key":"cursor","value":"no commit"}),
    )?;
    let scope = CandidateScope {
        database: &f.db,
        secrets: &manager(),
        binding: claim.binding(),
    };
    for fail_at in [1, 2, 3] {
        let calls = Cell::new(0);
        assert!(
            scope
                .fixture_execute(&request, 65536, || {
                    calls.set(calls.get() + 1);
                    if calls.get() == fail_at {
                        bail!("cancelled fixture");
                    }
                    Ok(())
                })
                .await
                .is_err()
        );
        assert_eq!(initial, sdk_snapshot(&f, staged.instance()).await?);
    }
    assert!(scope.fixture_execute(&request, 3, || Ok(())).await.is_err());
    assert_eq!(initial, sdk_snapshot(&f, staged.instance()).await?);
    assert!(call(&f, &claim, json!({"operation":"state_set","key":"cursor","value":"foreign","instance_id":Uuid::new_v4()})).await.is_err());
    assert_eq!(initial, sdk_snapshot(&f, staged.instance()).await?);
    call(
        &f,
        &claim,
        json!({"operation":"state_set","key":"cursor","value":"valid"}),
    )
    .await?;
    call(
        &f,
        &claim,
        json!({"operation":"state_delete","key":"cursor"}),
    )
    .await?;
    assert_eq!(sdk_snapshot(&f, staged.instance()).await?.1, 0);
    Ok(())
}

#[tokio::test]
async fn candidate_effect_rejects_revoked_owner_grant_and_changed_disabled_namespace() -> Result<()>
{
    for fault in [
        "grant",
        "grant_revision",
        "session",
        "authorization",
        "home",
        "enabled",
        "purpose",
        "counter",
        "secret",
        "withdrawal",
    ] {
        let f = Fixture::new().await?;
        let mut owner = state_copy::open_owner(&f, manager()).await?;
        let (staged, claim, _) = start(&f, &mut owner, 0).await?;
        match fault {
            "grant" => {
                sqlx::query("UPDATE marketplace_extension_grants SET enabled=FALSE")
                    .execute(&f.db.pool)
                    .await?;
            }
            "grant_revision" => {
                sqlx::query("UPDATE marketplace_extension_grants SET revision=revision+1")
                    .execute(&f.db.pool)
                    .await?;
            }
            "session" => {
                sqlx::query("UPDATE account_sessions SET revoked_at=CURRENT_TIMESTAMP,revoked_reason='candidate fixture'").execute(&f.db.pool).await?;
            }
            "authorization" => {
                sqlx::query("UPDATE profile_authorization_revisions SET revision=revision+1")
                    .execute(&f.db.pool)
                    .await?;
            }
            "home" => {
                let foreign = crate::marketplace::grant_store::tests::owner(&f.db).await?;
                sqlx::query("UPDATE marketplace_preferences SET home_id=$1")
                    .bind(foreign.home.to_string())
                    .execute(&f.db.pool)
                    .await?;
            }
            "enabled" => {
                sqlx::query("UPDATE extension_instances SET enabled=TRUE WHERE instance_id=$1")
                    .bind(staged.instance().to_string())
                    .execute(&f.db.pool)
                    .await?;
            }
            "purpose" => {
                sqlx::query("UPDATE marketplace_instance_sdk_state SET purpose='foreground'")
                    .execute(&f.db.pool)
                    .await?;
            }
            "counter" => {
                sqlx::query("UPDATE marketplace_instance_sdk_state SET key_count=1")
                    .execute(&f.db.pool)
                    .await?;
            }
            "withdrawal" => {
                use elixir_marketplace::{
                    records::{Revocation, SignedRecord},
                    security_job::SecurityRecords,
                };
                let d = &f.descriptor;
                let revoked = SignedRecord::sign(
                    Revocation {
                        root_key: d.publisher.payload.root_key,
                        extension_id: d.release.payload.extension_id.clone(),
                        release_ids: vec![claim.binding().review().release_id()],
                        release_keys: vec![],
                        reason: "candidate fixture withdrawal".into(),
                    },
                    None,
                    &ed25519_dalek::SigningKey::from_bytes(&[1; 32]),
                )?;
                assert!(
                    crate::marketplace::security_state::fixture_observe_signed(
                        &f.db,
                        SecurityRecords {
                            schema_version: 1,
                            extension_id: d.release.payload.extension_id.clone(),
                            publisher: d.publisher.clone(),
                            head: Some(d.head.clone()),
                            revocations: vec![revoked]
                        }
                    )
                    .await?
                );
            }
            _ => {
                sqlx::query("INSERT INTO secrets(secret_id,scope,scope_id,key,value_encrypted,rotatable) VALUES($1,'instance',$2,'source_api',$3,TRUE)")
                .bind(Uuid::new_v4().to_string()).bind(staged.instance().to_string()).bind(manager().encrypt("unexpected")?).execute(&f.db.pool).await?;
            }
        }
        let before = sdk_snapshot(&f, staged.instance()).await?;
        assert!(
            call(
                &f,
                &claim,
                json!({"operation":"state_set","key":"cursor","value":"forbidden"})
            )
            .await
            .is_err(),
            "fault {fault}"
        );
        assert_eq!(before, sdk_snapshot(&f, staged.instance()).await?);
    }
    Ok(())
}

#[tokio::test]
async fn candidate_claim_checks_fresh_source_owner_and_global_completion_capacity() -> Result<()> {
    let f = Fixture::new().await?;
    let mut owner = state_copy::open_owner(&f, manager()).await?;
    let (staged, plan, mutation, prepared) = source_prepared(&f, 0).await?;
    let fenced = quiesce_checked(step(&f, &staged, mutation, &plan)?, &prepared).await?;
    let scope = super::super::super::quiescence::observe_checked(
        step(&f, &staged, mutation, &plan)?,
        &fenced,
    )
    .await?;
    let guard = owner.quiesce(scope).await?;
    let copied = state_copy::copy(&f, &staged, mutation, &plan, &guard).await?;
    let before = snapshot(&f).await?;
    (f.public_owner)(false);
    assert!(
        claim(&f, &staged, mutation, &plan, &guard, &copied)
            .await
            .is_err()
    );
    (f.public_owner)(true);
    (f.meta_owner)(false);
    assert!(
        claim(&f, &staged, mutation, &plan, &guard, &copied)
            .await
            .is_err()
    );
    (f.meta_owner)(true);
    assert_eq!(before, snapshot(&f).await?);
    sqlx::query("UPDATE marketplace_installation_budget SET revision=$1")
        .bind(MAX_REVISION - 4096)
        .execute(&f.db.pool)
        .await?;
    let before = snapshot(&f).await?;
    assert!(
        claim(&f, &staged, mutation, &plan, &guard, &copied)
            .await
            .is_err()
    );
    assert_eq!(before, snapshot(&f).await?);
    // No code claim was committed, so the original authenticated untouched-
    // copy abort remains available from its separately reserved capacity.
    resume(
        &f.db,
        OwnerResumeIntent::fixture_for_actor(f.actor.clone(), &copied, Uuid::new_v4())?,
        limit(),
    )
    .await?;
    unused(&f).await
}

#[tokio::test]
#[ignore = "explicit fresh isolated loopback PostgreSQL candidate local-effect/revocation/restart checks; synthetic native boundaries"]
async fn postgres_candidate_effects_hold_current_authority_and_preserve_the_one_way_fence()
-> Result<()> {
    let url = std::env::var("ELIXIR_TEST_MARKETPLACE_CANDIDATE_POSTGRES_URL")?;
    let db = fresh_postgres(&url).await?;
    let mut f = Fixture::with_database(tempfile::tempdir()?, db.clone()).await?;
    let mut owner = state_copy::open_owner(&f, manager()).await?;
    let (staged, claim, _) = start(&f, &mut owner, 0).await?;
    call(
        &f,
        &claim,
        json!({"operation":"state_set","key":"cursor","value":"candidate"}),
    )
    .await?;
    let before = sdk_snapshot(&f, staged.instance()).await?;
    let mut effect = db.pool.begin().await?;
    claim
        .binding()
        .require_in_transaction(&mut effect, db.driver, Utc::now())
        .await?;
    let revoke = tokio::spawn({
        let db = db.clone();
        let actor = f.actor.clone();
        let id = staged.extension().clone();
        async move {
            crate::marketplace::grant_store::revoke_as(
                &db,
                &actor,
                &id,
                1,
                Uuid::new_v4(),
                Utc::now(),
            )
            .await
        }
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let blocked:i64=sqlx::query_scalar("SELECT COUNT(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND cardinality(pg_blocking_pids(pid))>0")
                .fetch_one(&db.pool).await?;
            if blocked > 0 { return Ok::<_, anyhow::Error>(()); }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await??;
    assert!(!revoke.is_finished());
    effect.rollback().await?;
    tokio::time::timeout(Duration::from_secs(5), revoke).await???;
    assert!(
        call(
            &f,
            &claim,
            json!({"operation":"state_set","key":"cursor","value":"after revoke"})
        )
        .await
        .is_err()
    );
    assert_eq!(before, sdk_snapshot(&f, staged.instance()).await?);
    drop(owner);
    db.pool.close().await;
    f.db = Database::connect(&DatabaseConfig {
        url,
        max_connections: 6,
        connect_timeout_seconds: 5,
    })
    .await?;
    assert!(
        matches!(inspect_as(&f.db, f.actor.clone(), staged.extension(), limit()).await?,
        ActivationStatus::CandidateStarting(r) if r.revision() == 7)
    );
    assert_eq!(before, sdk_snapshot(&f, staged.instance()).await?);
    unused(&f).await
}
