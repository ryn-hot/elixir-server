//! Real SQL/crypto and original claim/controller ownership. The initial source
//! probe/discovery boundary is synthetic; these tests claim no native execution.
use super::*;
use crate::{
    extensions::store::ExtensionStore,
    marketplace::{
        grant_store::{
            installation::{
                InstallTarget,
                runtime::{
                    InstalledController, UnstartedCandidate,
                    tests::{configuration, limits},
                },
            },
            sdk,
        },
        lease::ControllerLease,
    },
};
use elixir_marketplace::source_execution::RuntimeJson;
use serde_json::json;

fn manager() -> SecretsManager {
    SecretsManager::from_key_bytes([57; 32], false)
}
async fn setup(
    f: &Fixture,
    mismatch: bool,
) -> Result<(
    InstalledController,
    StagedInstallation,
    candidate::CandidateClaim,
)> {
    let config = configuration(f, IMAGE, "unstarted-candidate")?;
    let mut owner = InstalledController::open(f.db.clone(), manager(), config.clone()).await?;
    let staged = f
        .stage_configured(
            InstallTarget::New {
                name: "Original A".into(),
            },
            Uuid::new_v4(),
            0,
            limit(),
        )
        .await?;
    let mut claimed = config.clone();
    if mismatch {
        claimed.source_budget.cpu_millicores -= 1;
    }
    let claim = configured_runtime::claim_with_credentials(
        f,
        &mut owner,
        &claimed,
        &staged,
        Some(vec![CredentialChange::set(
            "source_api",
            "A-private",
            false,
        )]),
    )
    .await?;
    Ok((owner, staged, claim))
}
async fn sealed(
    f: &Fixture,
) -> Result<(InstalledController, StagedInstallation, UnstartedCandidate)> {
    let (owner, staged, claim) = setup(f, false).await?;
    let mut slot = Some(claim);
    let proof = owner.seal_unstarted_candidate(&mut slot)?;
    assert!(slot.is_none());
    assert!(owner.seal_unstarted_candidate(&mut slot).is_err());
    Ok((owner, staged, proof))
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
async fn counts(f: &Fixture, namespace: Uuid) -> Result<(i64, i64, i64)> {
    Ok(sqlx::query_as("SELECT CAST((SELECT COUNT(*) FROM marketplace_sdk_namespaces WHERE namespace_id=$1) AS BIGINT),CAST((SELECT COUNT(*) FROM marketplace_instance_sdk_state WHERE instance_id=$1) AS BIGINT),CAST((SELECT COUNT(*) FROM marketplace_generation_secrets WHERE namespace_id=$1) AS BIGINT)")
        .bind(namespace.to_string()).fetch_one(&f.db.pool).await?)
}

#[tokio::test]
async fn sealed_original_candidate_cancels_failed_preflight_offline_at_final_capacity_with_fresh_owner()
-> Result<()> {
    let f = Fixture::new().await?;
    let (mut controller, a, claim) = setup(&f, true).await?;
    let owner = a.configured_instance().context("owner")?;
    let mut slot = Some(claim);
    let error = controller
        .begin_candidate(&mut slot, limits())
        .err()
        .context("different plan accepted")?;
    assert!(
        format!("{error:#}").contains("original claimed plan"),
        "{error:#}"
    );
    assert!(
        slot.is_some(),
        "pure rejection must preserve the original claim"
    );
    let proof = controller.seal_unstarted_candidate(&mut slot)?;
    assert!(slot.is_none());
    assert!(controller.begin_candidate(&mut slot, limits()).is_err());
    let b = f
        .stage_configured(
            InstallTarget::New {
                name: "Original B".into(),
            },
            Uuid::new_v4(),
            0,
            limit(),
        )
        .await?;
    let b_owner = b.configured_instance().context("B")?;
    let before_b = record(&f, b_owner).await?;
    let store = ExtensionStore::new(&f.db.pool);
    store.set_instance_enabled(owner, false).await?;
    store
        .update_instance_config(owner, Some(&json!({"owner":"keep"})))
        .await?;
    let before_owner:(String,Option<String>,String) = sqlx::query_as("SELECT instance_name,config_json,extension_id FROM extension_instances WHERE instance_id=$1")
        .bind(owner.to_string()).fetch_one(&f.db.pool).await?;
    sqlx::query(
        "UPDATE profile_authorization_revisions SET revision=revision+1 WHERE profile_id=$1",
    )
    .bind(f.actor.profile.to_string())
    .execute(&f.db.pool)
    .await?;
    let mutation = Uuid::new_v4();
    assert!(
        proof
            .cancel(&configured_runtime::principal(&f), mutation)
            .await
            .is_err()
    );
    assert_eq!(counts(&f, a.instance()).await?, (1, 1, 1));
    let mut caps = crate::authz::CapabilitySet::all();
    caps.remove(crate::authz::Capability::SecretsManage);
    let fresh = CurrentPrincipal::fixture_owner(
        f.actor.user,
        f.actor.home,
        f.actor.profile,
        f.actor.session,
        f.actor.authorization_revision + 1,
        f.actor.expires,
        caps,
    );
    (f.meta_owner)(false);
    (f.public_owner)(false);
    sqlx::query("UPDATE marketplace_extension_grants SET enabled=FALSE,review_json='{}',revision=revision+1,revocation_revision=revision+1")
        .execute(&f.db.pool).await?;
    sqlx::query("UPDATE marketplace_installation_budget SET revision=$1 WHERE singleton=1")
        .bind(MAX_REVISION - 1)
        .execute(&f.db.pool)
        .await?;
    let cancelled = proof.cancel(&fresh, mutation).await?;
    assert_eq!(cancelled.revision(), proof.reference().revision() + 1);
    assert_eq!(
        proof.cancel(&fresh, mutation).await?.revision(),
        cancelled.revision()
    );
    assert!(proof.cancel(&fresh, Uuid::new_v4()).await.is_err());
    assert_eq!(counts(&f, a.instance()).await?, (0, 0, 0));
    assert_eq!(counts(&f, b.instance()).await?, (1, 1, 0));
    assert_eq!(record(&f, b_owner).await?, before_b);
    let after_owner:(String,Option<String>,String) = sqlx::query_as("SELECT instance_name,config_json,extension_id FROM extension_instances WHERE instance_id=$1")
        .bind(owner.to_string()).fetch_one(&f.db.pool).await?;
    assert_eq!(before_owner, after_owner);
    let revision: String = sqlx::query_scalar(
        "SELECT CAST(revision AS TEXT) FROM marketplace_installation_budget WHERE singleton=1",
    )
    .fetch_one(&f.db.pool)
    .await?;
    assert_eq!(revision, MAX_REVISION.to_string());
    Ok(())
}

#[tokio::test]
async fn sealed_original_candidate_refuses_used_copy_corrupt_secrets_changed_journal_or_owner()
-> Result<()> {
    for fault in ["used", "secret", "journal", "owner"] {
        let f = Fixture::new().await?;
        let (controller, a, claim) = setup(&f, false).await?;
        let owner = a.configured_instance().context("owner")?;
        if fault == "used" {
            sdk::CandidateScope {
                database: &f.db,
                secrets: &manager(),
                binding: claim.binding(),
            }
            .fixture_execute(
                &RuntimeJson::from_value(
                    json!({"operation":"state_set","key":"cursor","value":17}),
                )?,
                65536,
                || Ok(()),
            )
            .await?;
        }
        let proof = controller.seal_unstarted_candidate(&mut Some(claim))?;
        match fault {
            "secret" => {
                sqlx::query("UPDATE marketplace_generation_secrets SET value_encrypted='!' || SUBSTR(value_encrypted,2) WHERE namespace_id=$1")
                .bind(a.instance().to_string()).execute(&f.db.pool).await?;
            }
            "journal" => {
                let mut value: serde_json::Value = serde_json::from_str(&record(&f, owner).await?)?;
                value["mutation"] = json!(Uuid::new_v4());
                let bytes = canonical::to_vec(&value)?;
                sqlx::query("UPDATE marketplace_installations SET record_json=$1,record_sha256=$2 WHERE instance_id=$3")
                    .bind(String::from_utf8(bytes.clone())?).bind(Digest::of(&bytes).to_string()).bind(owner.to_string()).execute(&f.db.pool).await?;
            }
            "owner" => {
                sqlx::query("UPDATE marketplace_installed_runtime_owner SET binding_sha256=$1")
                    .bind(Digest::of(b"wrong-runtime-lineage").to_string())
                    .execute(&f.db.pool)
                    .await?;
            }
            _ => {}
        }
        let before = record(&f, owner).await?;
        assert!(
            proof
                .cancel(&configured_runtime::principal(&f), Uuid::new_v4())
                .await
                .is_err(),
            "{fault}"
        );
        assert_eq!(record(&f, owner).await?, before, "{fault}");
        assert_eq!(counts(&f, a.instance()).await?, (1, 1, 1), "{fault}");
    }
    Ok(())
}

#[tokio::test]
async fn sealed_original_candidate_rolls_back_namespace_deletion_and_keeps_its_proof() -> Result<()>
{
    let f = Fixture::new().await?;
    let (_controller, a, proof) = sealed(&f).await?;
    let owner = a.configured_instance().context("owner")?;
    let before = record(&f, owner).await?;
    sqlx::query("CREATE TRIGGER fail_unstarted_commit BEFORE UPDATE ON marketplace_installations BEGIN SELECT RAISE(ABORT,'fixture cancellation failure'); END")
        .execute(&f.db.pool).await?;
    let principal = configured_runtime::principal(&f);
    let mutation = Uuid::new_v4();
    assert!(proof.cancel(&principal, mutation).await.is_err());
    assert_eq!(record(&f, owner).await?, before);
    assert_eq!(counts(&f, a.instance()).await?, (1, 1, 1));
    sqlx::query("DROP TRIGGER fail_unstarted_commit")
        .execute(&f.db.pool)
        .await?;
    proof.cancel(&principal, mutation).await?;
    assert_eq!(counts(&f, a.instance()).await?, (0, 0, 0));
    Ok(())
}

#[tokio::test]
async fn sealed_original_candidate_survives_a_cancelled_sql_waiter() -> Result<()> {
    let f = Fixture::new().await?;
    let (_controller, a, proof) = sealed(&f).await?;
    let owner = a.configured_instance().context("owner")?;
    let before = record(&f, owner).await?;
    let mut blocker = f.db.pool.begin().await?;
    sqlx::query("UPDATE marketplace_installation_budget SET revision=revision WHERE singleton=1")
        .execute(&mut *blocker)
        .await?;
    let principal = configured_runtime::principal(&f);
    let mutation = Uuid::new_v4();
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(10),
            proof.cancel(&principal, mutation)
        )
        .await
        .is_err()
    );
    blocker.rollback().await?;
    assert_eq!(record(&f, owner).await?, before);
    let first = proof.cancel(&principal, mutation).await?;
    assert_eq!(
        proof.cancel(&principal, mutation).await?.revision(),
        first.revision()
    );
    assert_eq!(counts(&f, a.instance()).await?, (0, 0, 0));
    Ok(())
}

#[tokio::test]
async fn handed_off_candidate_cannot_be_sealed_from_an_empty_slot_or_a_free_lane() -> Result<()> {
    let f = Fixture::new().await?;
    let (mut controller, a, claim) = setup(&f, false).await?;
    let config = configuration(&f, IMAGE, "unstarted-candidate")?;
    // This component fixture blocks admission, rather than claiming a real
    // interpreter execution or native retirement qualification.
    let lane = ControllerLease::acquire(&config.lane)?;
    let mut slot = Some(claim);
    controller.begin_candidate(&mut slot, limits())?;
    assert!(slot.is_none());
    assert!(controller.seal_unstarted_candidate(&mut slot).is_err());
    controller.cancel().await?;
    let retired = controller.finish_candidate().await?;
    assert!(retired.migration_result().is_err());
    drop(lane);
    assert!(controller.seal_unstarted_candidate(&mut slot).is_err());
    assert_eq!(counts(&f, a.instance()).await?, (1, 1, 1));
    Ok(())
}
