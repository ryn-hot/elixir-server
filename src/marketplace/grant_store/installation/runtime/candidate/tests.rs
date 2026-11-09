//! SQL/crypto/controller checks use synthetic initial probe/ACTIVE enrollment.
//! The explicitly ignored native matrix additionally runs real lifecycle hooks
//! and confirmed cleanup. No activation commit is synthesized by these tests.
use super::super::super::{
    activation::tests::quiescence::candidate_for_runtime, standing::tests as installed,
    tests::Fixture,
};
use super::super::tests::{assert_disposed, configuration, limits};
use super::*;
use serde_json::{Value, json};

fn manager() -> SecretsManager {
    SecretsManager::from_key_bytes([57; 32], false)
}
fn image() -> String {
    format!("sha256:{}", "1".repeat(64))
}
async fn setup(
    f: &Fixture,
) -> Result<(
    InstalledController,
    InstalledRuntime,
    super::super::super::StagedInstallation,
    Option<CandidateClaim>,
)> {
    let config = configuration(f, &image(), "candidate-component")?;
    let mut owner = InstalledController::open(f.db.clone(), manager(), config.clone()).await?;
    let (staged, claim) =
        candidate_for_runtime(f, &mut owner, &config.image, &config.source_budget, 0).await?;
    Ok((owner, config, staged, Some(claim)))
}
async fn head(f: &Fixture, instance: Uuid) -> Result<(i64, i64, i64)> {
    Ok(sqlx::query_as("SELECT revision,key_count,encoded_bytes FROM marketplace_instance_sdk_state WHERE instance_id=$1")
        .bind(instance.to_string()).fetch_one(&f.db.pool).await?)
}
async fn value(f: &Fixture, instance: Uuid, key: &str) -> Result<Value> {
    let cipher:String=sqlx::query_scalar("SELECT value_encrypted FROM marketplace_instance_sdk_values WHERE instance_id=$1 AND key=$2")
        .bind(instance.to_string()).bind(key).fetch_one(&f.db.pool).await?;
    let plain = manager().decrypt(&cipher)?;
    let envelope: Value = serde_json::from_str(&plain)?;
    assert_eq!(envelope["instance_id"], json!(instance));
    assert_eq!(envelope["home_id"], json!(f.actor.home));
    Ok(envelope["value"].clone())
}

async fn partial_retired(
    f: &Fixture,
) -> Result<(
    InstalledController,
    super::super::super::StagedInstallation,
    RetiredCandidate,
)> {
    let (mut owner, config, staged, mut claim) = setup(f).await?;
    // Explicit SQL component fixture for partial migration, NOT native SDK
    // authorization. Real native cases below independently exercise that path.
    crate::marketplace::grant_store::sdk::CandidateScope {
        database: &f.db,
        secrets: &manager(),
        binding: claim.as_ref().unwrap().binding(),
    }
    .fixture_execute(
        &RuntimeJson::from_value(json!({"operation":"state_set","key":"cursor","value":4}))?,
        65536,
        || Ok(()),
    )
    .await?;
    let lane = ControllerLease::acquire(&config.lane)?;
    owner.begin_candidate(&mut claim, limits())?;
    let retired = owner.finish_candidate().await?;
    assert!(retired.migration_result().is_err());
    drop(lane);
    Ok((owner, staged, retired))
}

#[tokio::test]
async fn retired_discard_is_fresh_authenticated_offline_and_idempotent_after_revocation()
-> Result<()> {
    use super::super::super::activation::retirement::{OwnerDiscardIntent, discard};
    let f = Fixture::new().await?;
    let (_owner, staged, retired) = partial_retired(&f).await?;
    (f.public_owner)(false);
    (f.meta_owner)(false);
    sqlx::query("UPDATE marketplace_extension_grants SET enabled=FALSE,review_json='{}',revision=revision+1,revocation_revision=revision+1")
        .execute(&f.db.pool).await?;
    sqlx::query("UPDATE account_sessions SET csrf_revision=csrf_revision+1")
        .execute(&f.db.pool)
        .await?;
    let fresh = Actor {
        csrf_revision: f.actor.csrf_revision + 1,
        ..f.actor.clone()
    };
    let mutation = Uuid::new_v4();
    assert!(
        discard(
            &retired,
            OwnerDiscardIntent::fixture(f.actor.clone(), mutation)?
        )
        .await
        .is_err()
    );
    assert_eq!(value(&f, staged.instance(), "cursor").await?, json!(4));
    let first = discard(
        &retired,
        OwnerDiscardIntent::fixture(fresh.clone(), mutation)?,
    )
    .await?;
    assert_eq!(head(&f, staged.instance()).await?, (1, 0, 0));
    let before:(String,i64)=sqlx::query_as("SELECT i.record_json,b.revision FROM marketplace_installations i CROSS JOIN marketplace_installation_budget b")
        .fetch_one(&f.db.pool).await?;
    let again = discard(
        &retired,
        OwnerDiscardIntent::fixture(fresh.clone(), mutation)?,
    )
    .await?;
    assert_eq!(again.revision(), first.revision());
    let after:(String,i64)=sqlx::query_as("SELECT i.record_json,b.revision FROM marketplace_installations i CROSS JOIN marketplace_installation_budget b")
        .fetch_one(&f.db.pool).await?;
    assert_eq!(before, after);
    assert!(
        discard(
            &retired,
            OwnerDiscardIntent::fixture(fresh, Uuid::new_v4())?
        )
        .await
        .is_err()
    );
    assert!(installed::binding(&f, staged.instance()).await.is_err());
    Ok(())
}

#[tokio::test]
async fn retired_discard_refuses_bad_ciphertext_and_unauthorized_or_changed_state_without_deletion()
-> Result<()> {
    use super::super::super::activation::retirement::{OwnerDiscardIntent, discard};
    for fault in ["envelope", "secrets", "metadata", "owner", "session"] {
        let f = Fixture::new().await?;
        let (_owner, staged, retired) = partial_retired(&f).await?;
        match fault {
            "envelope" => {
                let cipher:String=sqlx::query_scalar("SELECT value_encrypted FROM marketplace_instance_sdk_values WHERE instance_id=$1")
                    .bind(staged.instance().to_string()).fetch_one(&f.db.pool).await?;
                let mut plain: Value = serde_json::from_str(&manager().decrypt(&cipher)?)?;
                plain["home_id"] = json!(Uuid::new_v4());
                let bad = manager().encrypt(&serde_json::to_string(&plain)?)?;
                assert_eq!(bad.len(), cipher.len());
                sqlx::query("UPDATE marketplace_instance_sdk_values SET value_encrypted=$1 WHERE instance_id=$2")
                    .bind(bad).bind(staged.instance().to_string()).execute(&f.db.pool).await?;
            }
            "secrets" => {
                sqlx::query("INSERT INTO secrets(secret_id,scope,scope_id,key,value_encrypted,rotatable) VALUES($1,'instance',$2,'source_api',$3,FALSE)")
                .bind(Uuid::new_v4().to_string()).bind(staged.instance().to_string()).bind(manager().encrypt("unexpected")?).execute(&f.db.pool).await?;
            }
            "metadata" => {
                sqlx::query("UPDATE extension_instances SET config_json='{}' WHERE instance_id=$1")
                    .bind(staged.instance().to_string())
                    .execute(&f.db.pool)
                    .await?;
            }
            "owner" => {
                sqlx::query("UPDATE marketplace_installed_runtime_owner SET binding_sha256=$1")
                    .bind(Digest::of(b"wrong lineage").to_string())
                    .execute(&f.db.pool)
                    .await?;
            }
            _ => {
                sqlx::query("UPDATE account_sessions SET revoked_at=CURRENT_TIMESTAMP,revoked_reason='discard fixture'")
                .execute(&f.db.pool).await?;
            }
        }
        let before = head(&f, staged.instance()).await?;
        let journal: String =
            sqlx::query_scalar("SELECT record_json FROM marketplace_installations")
                .fetch_one(&f.db.pool)
                .await?;
        assert!(
            discard(
                &retired,
                OwnerDiscardIntent::fixture(f.actor.clone(), Uuid::new_v4())?
            )
            .await
            .is_err(),
            "fault {fault}"
        );
        assert_eq!(before, head(&f, staged.instance()).await?);
        let after: String = sqlx::query_scalar("SELECT record_json FROM marketplace_installations")
            .fetch_one(&f.db.pool)
            .await?;
        assert_eq!(journal, after);
    }
    Ok(())
}

#[tokio::test]
async fn retired_discard_keeps_its_proof_when_a_waiter_cancels_and_uses_reserved_final_capacity()
-> Result<()> {
    use super::super::super::activation::retirement::{OwnerDiscardIntent, discard};
    let f = Fixture::new().await?;
    let (_owner, staged, retired) = partial_retired(&f).await?;
    let before = head(&f, staged.instance()).await?;
    let mut blocker = f.db.pool.begin().await?;
    sqlx::query("UPDATE marketplace_installation_budget SET revision=revision WHERE singleton=1")
        .execute(&mut *blocker)
        .await?;
    let mutation = Uuid::new_v4();
    assert!(
        timeout(
            Duration::from_millis(10),
            discard(
                &retired,
                OwnerDiscardIntent::fixture(f.actor.clone(), mutation)?
            )
        )
        .await
        .is_err()
    );
    blocker.rollback().await?;
    assert_eq!(before, head(&f, staged.instance()).await?);
    sqlx::query("UPDATE marketplace_installation_budget SET revision=$1")
        .bind(MAX_REVISION - 1)
        .execute(&f.db.pool)
        .await?;
    discard(
        &retired,
        OwnerDiscardIntent::fixture(f.actor.clone(), mutation)?,
    )
    .await?;
    assert_eq!(head(&f, staged.instance()).await?, (1, 0, 0));
    let last: i64 = sqlx::query_scalar("SELECT revision FROM marketplace_installation_budget")
        .fetch_one(&f.db.pool)
        .await?;
    assert_eq!(last, MAX_REVISION);
    discard(
        &retired,
        OwnerDiscardIntent::fixture(f.actor.clone(), mutation)?,
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn candidate_preflight_keeps_the_original_claim_and_never_uses_a_different_plan() -> Result<()>
{
    let f = Fixture::new().await?;
    let (mut owner, config, staged, mut claim) = setup(&f).await?;
    let initial = head(&f, staged.instance()).await?;
    let mut invalid = limits();
    invalid.sdk.as_mut().unwrap().calls = 513;
    assert!(owner.begin_candidate(&mut claim, invalid).is_err());
    assert!(claim.is_some());
    assert!(
        claim
            .as_ref()
            .unwrap()
            .require_configuration(&format!("sha256:{}", "2".repeat(64)), &config.source_budget)
            .is_err()
    );
    assert_eq!(head(&f, staged.instance()).await?, initial);
    // Hold the real global lane so this component check cannot create native
    // work even if the spawned task wins a scheduling race with cancellation.
    let lane = ControllerLease::acquire(&config.lane)?;
    owner.begin_candidate(&mut claim, limits())?;
    assert!(claim.is_none());
    assert!(owner.begin_candidate(&mut claim, limits()).is_err());
    owner.cancel().await?;
    let retired = owner.finish_candidate().await?;
    assert!(retired.migration_result().is_err());
    assert!(owner.finish_candidate().await.is_err());
    assert_eq!(head(&f, staged.instance()).await?, initial);
    assert!(crate::marketplace::lease::activity::Fence::load(&lane)?.is_none());
    Ok(())
}

#[tokio::test]
async fn candidate_cancelled_waiter_keeps_the_original_handle_and_joined_result_keeps_the_owner()
-> Result<()> {
    let f = Fixture::new().await?;
    let (owner, config, _, mut claim) = setup(&f).await?;
    let runtime_owner = owner.retained_owner()?;
    let lane = ControllerLease::acquire(&config.lane)?;
    let (stop, stopped) = watch::channel(false);
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn({
        let database = f.db.clone();
        let config = config.clone();
        let original = claim.take().unwrap();
        async move {
            let _ = released.await;
            run_candidate(
                database,
                manager(),
                original,
                config,
                limits(),
                RunControl {
                    stop: stopped,
                    owner: Some(runtime_owner.lease()),
                },
                runtime_owner,
            )
            .await
        }
    });
    let mut call = CandidateCall {
        stop,
        task: Some(task),
        result: None,
        join_failed: false,
    };
    assert!(
        timeout(Duration::from_millis(10), call.join())
            .await
            .is_err()
    );
    assert!(call.task.is_some());
    drop(owner);
    assert!(ControllerLease::acquire(&config.lane.join("installed-controller")).is_err());
    assert!(
        InstalledController::open(f.db.clone(), manager(), config.clone())
            .await
            .is_err()
    );
    assert!(
        timeout(Duration::from_millis(10), call.cancel())
            .await
            .is_err()
    );
    release.send(()).unwrap();
    call.join().await?;
    let retired = call.take()?;
    assert!(retired.migration_result().is_err());
    assert!(call.take().is_err());
    assert!(ControllerLease::acquire(&config.lane.join("installed-controller")).is_err());
    assert!(
        InstalledController::open(f.db.clone(), manager(), config.clone())
            .await
            .is_err(),
        "a joined result still retains the original registry owner through final audit"
    );
    drop(lane);
    drop(retired);
    drop(InstalledController::open(f.db.clone(), manager(), config).await?);
    Ok(())
}

#[tokio::test]
async fn candidate_panicked_join_never_returns_retirement_or_replaces_the_owned_slot() -> Result<()>
{
    let f = Fixture::new().await?;
    let (mut owner, _, _, _) = setup(&f).await?;
    let (stop, _) = watch::channel(false);
    let task = tokio::spawn(async { panic!("synthetic original candidate task panic") });
    // Tests may construct this pending owner; no retirement result is forged.
    owner.candidate_fixture(CandidateCall {
        stop,
        task: Some(task),
        result: None,
        join_failed: false,
    });
    assert!(owner.finish_candidate().await.is_err());
    assert!(owner.cancel().await.is_err());
    assert!(owner.begin_candidate(&mut None, limits()).is_err());
    Ok(())
}

#[tokio::test]
async fn candidate_checks_database_owner_lineage_before_any_native_work() -> Result<()> {
    let f = Fixture::new().await?;
    let (mut owner, config, staged, mut claim) = setup(&f).await?;
    let initial = head(&f, staged.instance()).await?;
    sqlx::query("UPDATE marketplace_installed_runtime_owner SET binding_sha256=$1")
        .bind(Digest::of(b"replaced owner lineage").to_string())
        .execute(&f.db.pool)
        .await?;
    owner.begin_candidate(&mut claim, limits())?;
    let retired = owner.finish_candidate().await?;
    assert!(retired.migration_result().is_err());
    let mut tx = f.db.pool.begin().await?;
    assert!(retired.require_owner(&mut tx, f.db.driver).await.is_err());
    tx.rollback().await?;
    assert_eq!(head(&f, staged.instance()).await?, initial);
    let lane = ControllerLease::acquire(&config.lane)?;
    assert!(crate::marketplace::lease::activity::Fence::load(&lane)?.is_none());
    Ok(())
}

#[tokio::test]
#[ignore = "explicit offline native lifecycle/COW/encrypted SDK/owned cancellation and failure retirement; initial ACTIVE/probe enrollment synthetic"]
async fn native_candidate_lifecycle_joins_actual_retirement_without_activation_or_replay()
-> Result<()> {
    let pinned = std::env::var("ELIXIR_MARKETPLACE_WORKER_IMAGE")?;
    if !crate::marketplace::runtime::image_is_pinned(&pinned) {
        bail!("explicit immutable image required");
    }
    native_cases(&pinned, &["ready", "throw", "cancel", "revoke"], None).await
}

#[tokio::test]
#[ignore = "explicit fresh loopback PostgreSQL plus real native lifecycle/owner-scoped effects/audit/discard; initial ACTIVE/probe enrollment synthetic"]
async fn postgres_native_candidate_owner_effects_and_authenticated_discard() -> Result<()> {
    let pinned = std::env::var("ELIXIR_MARKETPLACE_WORKER_IMAGE")?;
    if !crate::marketplace::runtime::image_is_pinned(&pinned) {
        bail!("explicit immutable image required");
    }
    let url = std::env::var("ELIXIR_TEST_MARKETPLACE_RETIRED_POSTGRES_URL")?;
    let db = super::super::super::activation::tests::fresh_postgres(&url).await?;
    native_cases(&pinned, &["ready"], Some(db)).await
}

async fn native_cases(pinned: &str, cases: &[&str], database: Option<Database>) -> Result<()> {
    if database.is_some() && cases.len() != 1 {
        bail!("one fresh PostgreSQL database per native case");
    }
    for case in cases.iter().copied() {
        eprintln!("Native candidate lifecycle case={case}");
        let mut f = if let Some(db) = &database {
            Fixture::with_database(tempfile::tempdir()?, db.clone()).await?
        } else {
            Fixture::new().await?
        };
        let config = configuration(&f, pinned, "candidate-native-fixture")?;
        let mut owner = InstalledController::open(f.db.clone(), manager(), config.clone()).await?;
        let old = installed::enroll(&f).await?;
        let binding = installed::binding(&f, old).await?;
        crate::marketplace::grant_store::sdk::InstalledScope {
            database: &f.db,
            secrets: &manager(),
            binding: &binding,
        }
        .execute(
            &RuntimeJson::from_value(json!({"operation":"state_set","key":"cursor","value":3}))?,
            65536,
            || Ok(()),
        )
        .await?;
        let old_cipher:String=sqlx::query_scalar("SELECT value_encrypted FROM marketplace_instance_sdk_values WHERE instance_id=$1 AND key='cursor'")
            .bind(old.to_string()).fetch_one(&f.db.pool).await?;
        let source = match case {
            "ready" => {
                "import {state} from '@elixir/sdk/v1'; export function invoke(){throw Error('ordinary invoke must not run')} export async function migrate(c){if(c.from_version!=='1.0.0'||c.to_version!=='2.0.0')throw Error('wrong coordinates'); await state.set('cursor',(await state.get('cursor'))+1)} export async function checkReady(c){return (await state.get('cursor'))===4&&c.to_version==='2.0.0'}"
            }
            "throw" => {
                "import {state} from '@elixir/sdk/v1'; export function invoke(){throw Error('ordinary invoke must not run')} export async function migrate(){await state.set('cursor',(await state.get('cursor'))+1);throw Error('migration failed')} export function checkReady(){return true}"
            }
            _ => {
                "import {state} from '@elixir/sdk/v1'; export function invoke(){throw Error('ordinary invoke must not run')} export async function migrate(){await state.set('cursor',(await state.get('cursor'))+1);for(;;){}} export function checkReady(){return true}"
            }
        };
        f.upgrade("2.0.0", Some(source)).await?;
        let (staged, claim) =
            candidate_for_runtime(&f, &mut owner, &config.image, &config.source_budget, 2).await?;
        let deployment = claim.deployment();
        let mut original = Some(claim);
        owner.begin_candidate(&mut original, limits())?;
        assert!(original.is_none());
        if matches!(case, "cancel" | "revoke") {
            timeout(Duration::from_secs(180), async {
                loop {
                    if head(&f, staged.instance()).await?.0 > 2 {
                        return Ok::<_, anyhow::Error>(());
                    }
                    sleep(Duration::from_millis(50)).await;
                }
            })
            .await??;
            assert!(
                timeout(Duration::from_millis(10), owner.finish_candidate())
                    .await
                    .is_err()
            );
            if case == "revoke" {
                sqlx::query("UPDATE marketplace_extension_grants SET enabled=FALSE")
                    .execute(&f.db.pool)
                    .await?;
            } else {
                owner.cancel().await?;
            }
        }
        let retired = timeout(Duration::from_secs(180), owner.finish_candidate()).await??;
        assert_eq!(retired.migration_result().is_ok(), case == "ready");
        assert!(owner.finish_candidate().await.is_err());
        assert_eq!(
            value(&f, staged.instance(), "cursor").await?,
            json!(4),
            "never replay partial migration"
        );
        assert_eq!(value(&f, old, "cursor").await?, json!(3));
        let unchanged:String=sqlx::query_scalar("SELECT value_encrypted FROM marketplace_instance_sdk_values WHERE instance_id=$1 AND key='cursor'")
            .bind(old.to_string()).fetch_one(&f.db.pool).await?;
        assert_eq!(old_cipher, unchanged);
        let disabled:i64=sqlx::query_scalar("SELECT CAST(CASE WHEN enabled THEN 1 ELSE 0 END AS BIGINT) FROM extension_instances WHERE instance_id=$1")
            .bind(staged.instance().to_string()).fetch_one(&f.db.pool).await?;
        assert_eq!(disabled, 0);
        assert!(binding.current(&f.db).await.is_err());
        let journal: String =
            sqlx::query_scalar("SELECT record_json FROM marketplace_installations")
                .fetch_one(&f.db.pool)
                .await?;
        assert_eq!(
            serde_json::from_str::<Value>(&journal)?["activation"]["phase"],
            json!("candidate_starting")
        );
        assert_disposed(&f, pinned, &deployment).await?;
        // Rotate actual session authority: the captured claim actor is stale,
        // but a new authenticated owner context must still be able to discard.
        sqlx::query("UPDATE account_sessions SET csrf_revision=csrf_revision+1")
            .execute(&f.db.pool)
            .await?;
        let fresh = Actor {
            csrf_revision: f.actor.csrf_revision + 1,
            ..f.actor.clone()
        };
        if case == "revoke" {
            sqlx::query("UPDATE marketplace_extension_grants SET review_json='{}'")
                .execute(&f.db.pool)
                .await?;
        }
        (f.public_owner)(false);
        (f.meta_owner)(false);
        let mutation = Uuid::new_v4();
        let discarded = super::super::super::activation::retirement::discard(
            &retired,
            super::super::super::activation::retirement::OwnerDiscardIntent::fixture(
                fresh.clone(),
                mutation,
            )?,
        )
        .await?;
        assert_eq!(head(&f, staged.instance()).await?, (1, 0, 0));
        assert_eq!(value(&f, old, "cursor").await?, json!(3));
        let repeated = super::super::super::activation::retirement::discard(
            &retired,
            super::super::super::activation::retirement::OwnerDiscardIntent::fixture(
                fresh, mutation,
            )?,
        )
        .await?;
        assert_eq!(repeated.revision(), discarded.revision());
        drop(retired);
        eprintln!("Native candidate lifecycle case={case} passed; no activation commit");
    }
    Ok(())
}
