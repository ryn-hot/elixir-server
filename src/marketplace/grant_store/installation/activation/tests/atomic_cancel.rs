//! SQL/crypto and actual task ownership, with synthetic discovery/probe and a
//! deliberately blocked native lane. NOT native execution/disposal evidence;
//! preparation/native_tests independently qualifies actual native cancellation.
use super::*;
use crate::marketplace::{
    grant_store::{
        installation::{
            InstallTarget,
            runtime::{
                InstalledController, RetiredCandidate,
                tests::{configuration, limits},
            },
        },
        sdk,
    },
    lease::ControllerLease,
};
use elixir_marketplace::source_execution::RuntimeJson;
use retirement::{OwnerRetiredCancelIntent, cancel};
use serde_json::json;

fn manager() -> SecretsManager {
    SecretsManager::from_key_bytes([57; 32], false)
}
async fn setup(f: &Fixture) -> Result<(InstalledController, StagedInstallation, RetiredCandidate)> {
    let config = configuration(f, IMAGE, "atomic-cancel-component")?;
    let mut controller = InstalledController::open(f.db.clone(), manager(), config.clone()).await?;
    let staged = f
        .stage_configured(
            InstallTarget::New {
                name: "Atomic A".into(),
            },
            Uuid::new_v4(),
            0,
            limit(),
        )
        .await?;
    let claim = configured_runtime::claim_with_credentials(
        f,
        &mut controller,
        &config,
        &staged,
        Some(vec![CredentialChange::set(
            "source_api",
            "A-private",
            false,
        )]),
    )
    .await?;
    sdk::CandidateScope {
        database: &f.db,
        secrets: &manager(),
        binding: claim.binding(),
    }
    .fixture_execute(
        &RuntimeJson::from_value(json!({"operation":"state_set","key":"cursor","value":17}))?,
        65536,
        || Ok(()),
    )
    .await?;
    let held = ControllerLease::acquire(&config.lane)?;
    let mut slot = Some(claim);
    controller.begin_candidate(&mut slot, limits())?;
    assert!(slot.is_none());
    let retired = controller.finish_candidate().await?;
    assert!(retired.migration_result().is_err());
    drop(held);
    Ok((controller, staged, retired))
}
async fn record(f: &Fixture, owner: Uuid) -> Result<String> {
    Ok(
        sqlx::query_scalar(
            "SELECT record_json FROM marketplace_installations WHERE instance_id=$1",
        )
        .bind(owner.to_string())
        .fetch_one(&f.db.pool)
        .await?,
    )
}
async fn counts(f: &Fixture, ns: Uuid) -> Result<(i64, i64, i64)> {
    Ok(sqlx::query_as("SELECT CAST((SELECT COUNT(*) FROM marketplace_sdk_namespaces WHERE namespace_id=$1) AS BIGINT),CAST((SELECT COUNT(*) FROM marketplace_instance_sdk_state WHERE instance_id=$1) AS BIGINT),CAST((SELECT COUNT(*) FROM marketplace_generation_secrets WHERE namespace_id=$1) AS BIGINT)")
        .bind(ns.to_string()).fetch_one(&f.db.pool).await?)
}
fn input(p: &CurrentPrincipal, mutation: Uuid) -> Result<OwnerRetiredCancelIntent> {
    OwnerRetiredCancelIntent::new(p, mutation)
}

#[tokio::test]
async fn atomic_retired_cancel_is_offline_exact_final_capacity_and_preserves_other_data()
-> Result<()> {
    let f = Fixture::new().await?;
    let (_controller, a, retired) = setup(&f).await?;
    let owner = a.configured_instance().context("configured owner")?;
    let b = f
        .stage_configured(
            InstallTarget::New {
                name: "Atomic B".into(),
            },
            Uuid::new_v4(),
            0,
            limit(),
        )
        .await?;
    let b_owner = b.configured_instance().context("B owner")?;
    let before_b = record(&f, b_owner).await?;
    sqlx::query("UPDATE extension_instances SET config_json=$1 WHERE instance_id=$2")
        .bind(r#"{"owner":"preserved"}"#)
        .bind(owner.to_string())
        .execute(&f.db.pool)
        .await?;
    let legacy = manager().encrypt("legacy-preserved")?;
    sqlx::query("INSERT INTO secrets(secret_id,scope,scope_id,key,value_encrypted,rotatable) VALUES($1,'instance',$2,'source_api',$3,FALSE)")
        .bind(Uuid::new_v4().to_string()).bind(owner.to_string()).bind(&legacy).execute(&f.db.pool).await?;
    let before_owner:(String,Option<String>,String)=sqlx::query_as("SELECT instance_name,config_json,extension_id FROM extension_instances WHERE instance_id=$1")
        .bind(owner.to_string()).fetch_one(&f.db.pool).await?;
    let (first, _, _) = counts(&f, a.instance()).await?;
    assert_eq!(first, 1);
    (f.meta_owner)(false);
    (f.public_owner)(false);
    sqlx::query("UPDATE marketplace_extension_grants SET enabled=FALSE,review_json='{}',revision=revision+1,revocation_revision=revision+1")
        .execute(&f.db.pool).await?;
    sqlx::query("UPDATE marketplace_installation_budget SET revision=$1 WHERE singleton=1")
        .bind(record::MAX_REVISION - 1)
        .execute(&f.db.pool)
        .await?;
    let mut caps = crate::authz::CapabilitySet::all();
    caps.remove(crate::authz::Capability::SecretsManage);
    let p = CurrentPrincipal::fixture_owner(
        f.actor.user,
        f.actor.home,
        f.actor.profile,
        f.actor.session,
        f.actor.authorization_revision,
        f.actor.expires,
        caps,
    );
    let mutation = Uuid::new_v4();
    let cancelled = cancel(&retired, input(&p, mutation)?).await?;
    let after = record(&f, owner).await?;
    let value: serde_json::Value = serde_json::from_str(&after)?;
    assert_eq!(value["revision"], json!(cancelled.revision()));
    assert!(value["pending"].is_null());
    assert!(value.get("activation").is_none());
    assert!(value["cancelled"].is_object());
    assert_eq!(counts(&f, a.instance()).await?, (0, 0, 0));
    assert_eq!(counts(&f, b.instance()).await?, (1, 1, 0));
    assert_eq!(record(&f, b_owner).await?, before_b);
    let after_owner:(String,Option<String>,String)=sqlx::query_as("SELECT instance_name,config_json,extension_id FROM extension_instances WHERE instance_id=$1")
        .bind(owner.to_string()).fetch_one(&f.db.pool).await?;
    assert_eq!(after_owner, before_owner);
    let cipher:String=sqlx::query_scalar("SELECT value_encrypted FROM secrets WHERE scope='instance' AND scope_id=$1 AND key='source_api'")
        .bind(owner.to_string()).fetch_one(&f.db.pool).await?;
    assert_eq!(cipher, legacy);
    assert_eq!(
        cancel(&retired, input(&p, mutation)?).await?.revision(),
        cancelled.revision()
    );
    let revision: i64 = sqlx::query_scalar(
        "SELECT revision FROM marketplace_installation_budget WHERE singleton=1",
    )
    .fetch_one(&f.db.pool)
    .await?;
    assert_eq!(revision, record::MAX_REVISION);
    assert_eq!(record(&f, owner).await?, after);
    assert!(cancel(&retired, input(&p, Uuid::new_v4())?).await.is_err());
    assert!(f.archive.release_id() == a.release());
    Ok(())
}

#[tokio::test]
async fn atomic_retired_cancel_requires_new_owner_authority_and_never_restores_stale_revision()
-> Result<()> {
    let f = Fixture::new().await?;
    let (_controller, a, retired) = setup(&f).await?;
    let owner = a.configured_instance().context("owner")?;
    let before = record(&f, owner).await?;
    let stale = configured_runtime::principal(&f);
    sqlx::query(
        "UPDATE profile_authorization_revisions SET revision=revision+1 WHERE profile_id=$1",
    )
    .bind(stale.profile_id.to_string())
    .execute(&f.db.pool)
    .await?;
    let mutation = Uuid::new_v4();
    assert!(cancel(&retired, input(&stale, mutation)?).await.is_err());
    assert_eq!(record(&f, owner).await?, before);
    let mut fresh = stale.clone();
    fresh.capability_revision += 1;
    let mut admin = fresh.clone();
    admin.role = HomeRole::Admin;
    assert!(input(&admin, mutation).is_err());
    let mut query = fresh.clone();
    query.transport = crate::http::auth::AccountAuthTransport::Query;
    assert!(input(&query, mutation).is_err());
    cancel(&retired, input(&fresh, mutation)?).await?;
    assert_eq!(counts(&f, a.instance()).await?, (0, 0, 0));
    let revision: String = sqlx::query_scalar(
        "SELECT CAST(revision AS TEXT) FROM profile_authorization_revisions WHERE profile_id=$1",
    )
    .bind(stale.profile_id.to_string())
    .fetch_one(&f.db.pool)
    .await?;
    assert_eq!(revision.parse::<i64>()?, fresh.capability_revision);
    Ok(())
}

#[tokio::test]
async fn atomic_retired_cancel_rolls_back_namespace_deletion_if_final_write_is_impossible()
-> Result<()> {
    let f = Fixture::new().await?;
    let (_controller, a, retired) = setup(&f).await?;
    let owner = a.configured_instance().context("owner")?;
    let before = record(&f, owner).await?;
    let before_counts = counts(&f, a.instance()).await?;
    let cipher:String=sqlx::query_scalar("SELECT value_encrypted FROM marketplace_instance_sdk_values WHERE instance_id=$1 AND key='cursor'").bind(a.instance().to_string()).fetch_one(&f.db.pool).await?;
    sqlx::query("UPDATE marketplace_installation_budget SET revision=$1 WHERE singleton=1")
        .bind(record::MAX_REVISION)
        .execute(&f.db.pool)
        .await?;
    assert!(
        cancel(
            &retired,
            input(&configured_runtime::principal(&f), Uuid::new_v4())?
        )
        .await
        .is_err()
    );
    assert_eq!(record(&f, owner).await?, before);
    assert_eq!(counts(&f, a.instance()).await?, before_counts);
    let after:String=sqlx::query_scalar("SELECT value_encrypted FROM marketplace_instance_sdk_values WHERE instance_id=$1 AND key='cursor'").bind(a.instance().to_string()).fetch_one(&f.db.pool).await?;
    assert_eq!(after, cipher);
    Ok(())
}

#[tokio::test]
async fn atomic_retired_cancel_refuses_unaudited_ciphertext_without_deleting_anything() -> Result<()>
{
    let f = Fixture::new().await?;
    let (_controller, a, retired) = setup(&f).await?;
    let owner = a.configured_instance().context("owner")?;
    let cipher:String=sqlx::query_scalar("SELECT value_encrypted FROM marketplace_instance_sdk_values WHERE instance_id=$1 AND key='cursor'").bind(a.instance().to_string()).fetch_one(&f.db.pool).await?;
    let mut envelope: serde_json::Value = serde_json::from_str(&manager().decrypt(&cipher)?)?;
    envelope["home_id"] = json!(Uuid::new_v4());
    let corrupt = manager().encrypt(&serde_json::to_string(&envelope)?)?;
    assert_eq!(corrupt.len(), cipher.len());
    sqlx::query("UPDATE marketplace_instance_sdk_values SET value_encrypted=$1 WHERE instance_id=$2 AND key='cursor'").bind(&corrupt).bind(a.instance().to_string()).execute(&f.db.pool).await?;
    let before = record(&f, owner).await?;
    assert!(
        cancel(
            &retired,
            input(&configured_runtime::principal(&f), Uuid::new_v4())?
        )
        .await
        .is_err()
    );
    assert_eq!(record(&f, owner).await?, before);
    assert_eq!(counts(&f, a.instance()).await?, (1, 1, 1));
    let after:String=sqlx::query_scalar("SELECT value_encrypted FROM marketplace_instance_sdk_values WHERE instance_id=$1 AND key='cursor'").bind(a.instance().to_string()).fetch_one(&f.db.pool).await?;
    assert_eq!(after, corrupt);
    Ok(())
}

#[tokio::test]
async fn atomic_retired_cancel_keeps_original_proof_after_a_cancelled_sql_waiter() -> Result<()> {
    let f = Fixture::new().await?;
    let (_controller, a, retired) = setup(&f).await?;
    let owner = a.configured_instance().context("owner")?;
    let before = record(&f, owner).await?;
    let before_counts = counts(&f, a.instance()).await?;
    let mut blocker = f.db.pool.begin().await?;
    sqlx::query("UPDATE marketplace_installation_budget SET revision=revision WHERE singleton=1")
        .execute(&mut *blocker)
        .await?;
    let p = configured_runtime::principal(&f);
    let mutation = Uuid::new_v4();
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(10),
            cancel(&retired, input(&p, mutation)?)
        )
        .await
        .is_err()
    );
    blocker.rollback().await?;
    assert_eq!(record(&f, owner).await?, before);
    assert_eq!(counts(&f, a.instance()).await?, before_counts);
    let done = cancel(&retired, input(&p, mutation)?).await?;
    assert_eq!(
        cancel(&retired, input(&p, mutation)?).await?.revision(),
        done.revision()
    );
    assert_eq!(counts(&f, a.instance()).await?, (0, 0, 0));
    Ok(())
}
