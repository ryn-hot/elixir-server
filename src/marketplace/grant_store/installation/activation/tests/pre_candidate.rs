//! Real SQL/crypto and original transition ownership. Initial source probing
//! is synthetic here; preparation/native_tests exercises the actual handoff.
use super::super::{pre_candidate::PreCandidateReceipt, quiescence as cutover};
use super::*;
use crate::{
    extensions::store::ExtensionStore,
    marketplace::grant_store::installation::{
        InstallTarget,
        runtime::{InstalledController, tests::configuration},
    },
};
use serde_json::json;

fn manager() -> SecretsManager {
    SecretsManager::from_key_bytes([57; 32], false)
}
struct Setup {
    owner: InstalledController,
    staged: StagedInstallation,
    plan: SourceActivationPlan,
    intent: Arc<OwnerActivationIntent>,
    prepared: ActivationReference,
    receipt: Box<PreCandidateReceipt>,
}
fn checked<'a>(
    f: &'a Fixture,
    plan: &'a SourceActivationPlan,
    intent: &Arc<OwnerActivationIntent>,
    database: &'a Database,
) -> Result<Step<'a>> {
    Ok(Step {
        database,
        metadata: &f.metadata,
        public: &f.public,
        material: f.material()?,
        intent: intent.clone(),
        plan,
        budget: limit(),
    })
}
async fn setup(f: &Fixture) -> Result<Setup> {
    let config = configuration(f, IMAGE, "pre-candidate")?;
    let owner = InstalledController::open(f.db.clone(), manager(), config.clone()).await?;
    let staged = f
        .stage_configured(
            InstallTarget::New { name: "A".into() },
            Uuid::new_v4(),
            0,
            limit(),
        )
        .await?;
    let material = f.material()?;
    let plan = SourceActivationPlan::new(
        material.authority.verify()?.release(),
        &material.review,
        &config.image,
        &config.source_budget,
    )?;
    let intent = Arc::new(OwnerActivationIntent::new_with_credentials(
        &configured_runtime::principal(f),
        &staged,
        &material.review,
        f.revision,
        Uuid::new_v4(),
        &manager(),
        vec![CredentialChange::set("source_api", "A-private", false)],
    )?);
    let prepared = match prepare_checked(checked(f, &plan, &intent, &f.db)?).await? {
        PreparedOrRecovery::Prepared(prepared) => prepared,
        _ => bail!("original preparation required"),
    };
    let start = match claim_checked(checked(f, &plan, &intent, &f.db)?, &prepared).await? {
        StartOrRecovery::Start(start) => start,
        _ => bail!("original source claim required"),
    };
    let prepared = source_prepared::retain(f, &start.native_check(), &plan, limit()).await?;
    let receipt = cutover::quiesce_retained(checked(f, &plan, &intent, &f.db)?, &prepared)
        .await?
        .original()?;
    Ok(Setup {
        owner,
        staged,
        plan,
        intent,
        prepared,
        receipt,
    })
}
async fn copy_original(f: &Fixture, s: &mut Setup) -> Result<Box<PreCandidateReceipt>> {
    let scope = cutover::observe_checked(
        checked(f, &s.plan, &s.intent, &f.db)?,
        s.receipt.reference(),
    )
    .await?;
    let guard = s.owner.quiesce(scope).await?;
    let copied =
        state_copy::copy_retained(checked(f, &s.plan, &s.intent, guard.database())?, &guard)
            .await?
            .original()?;
    Ok(std::mem::replace(&mut s.receipt, copied))
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
async fn original_preclaim_receipts_cancel_offline_with_fresh_owner_at_final_capacity() -> Result<()>
{
    for copied in [false, true] {
        let f = Fixture::new().await?;
        let mut s = setup(&f).await?;
        if copied {
            let _original_quiescence = copy_original(&f, &mut s).await?;
        }
        let id = s.staged.configured_instance().context("owner")?;
        let b = f
            .stage_configured(
                InstallTarget::New { name: "B".into() },
                Uuid::new_v4(),
                0,
                limit(),
            )
            .await?;
        let b_before = record(&f, b.configured_instance().context("B")?).await?;
        let store = ExtensionStore::new(&f.db.pool);
        store.set_instance_enabled(id, false).await?;
        store
            .update_instance_config(id, Some(&json!({"owner":"keep"})))
            .await?;
        let before:(String,Option<String>,String,i64)=sqlx::query_as("SELECT instance_name,config_json,extension_id,CAST(CASE WHEN enabled THEN 1 ELSE 0 END AS BIGINT) FROM extension_instances WHERE instance_id=$1")
            .bind(id.to_string()).fetch_one(&f.db.pool).await?;
        sqlx::query(
            "UPDATE profile_authorization_revisions SET revision=revision+1 WHERE profile_id=$1",
        )
        .bind(f.actor.profile.to_string())
        .execute(&f.db.pool)
        .await?;
        let mutation = Uuid::new_v4();
        assert!(
            s.receipt
                .cancel(&configured_runtime::principal(&f), mutation)
                .await
                .is_err()
        );
        let mut capabilities = crate::authz::CapabilitySet::all();
        capabilities.remove(crate::authz::Capability::SecretsManage);
        let fresh = CurrentPrincipal::fixture_owner(
            f.actor.user,
            f.actor.home,
            f.actor.profile,
            f.actor.session,
            f.actor.authorization_revision + 1,
            f.actor.expires,
            capabilities,
        );
        (f.meta_owner)(false);
        (f.public_owner)(false);
        sqlx::query("UPDATE marketplace_extension_grants SET enabled=FALSE,review_json='{}',revision=revision+1,revocation_revision=revision+1")
            .execute(&f.db.pool).await?;
        sqlx::query("UPDATE marketplace_installation_budget SET revision=$1 WHERE singleton=1")
            .bind(MAX_REVISION - 1)
            .execute(&f.db.pool)
            .await?;
        let cancelled = s.receipt.cancel(&fresh, mutation).await?;
        assert_eq!(cancelled.revision(), s.receipt.reference().revision() + 1);
        assert_eq!(
            s.receipt.cancel(&fresh, mutation).await?.revision(),
            cancelled.revision()
        );
        assert!(s.receipt.cancel(&fresh, Uuid::new_v4()).await.is_err());
        assert_eq!(counts(&f, s.staged.instance()).await?, (0, 0, 0));
        assert_eq!(counts(&f, b.instance()).await?, (1, 1, 0));
        assert_eq!(
            record(&f, b.configured_instance().context("B")?).await?,
            b_before
        );
        let after:(String,Option<String>,String,i64)=sqlx::query_as("SELECT instance_name,config_json,extension_id,CAST(CASE WHEN enabled THEN 1 ELSE 0 END AS BIGINT) FROM extension_instances WHERE instance_id=$1")
            .bind(id.to_string()).fetch_one(&f.db.pool).await?;
        assert_eq!(after, before);
    }
    Ok(())
}

#[tokio::test]
async fn retries_are_coordinates_only_and_old_receipts_cannot_cancel_later_transitions()
-> Result<()> {
    let f = Fixture::new().await?;
    let mut s = setup(&f).await?;
    assert!(
        cutover::quiesce_retained(checked(&f, &s.plan, &s.intent, &f.db)?, &s.prepared)
            .await?
            .original()
            .is_err()
    );
    let old = copy_original(&f, &mut s).await?;
    let principal = configured_runtime::principal(&f);
    assert!(old.cancel(&principal, Uuid::new_v4()).await.is_err());
    let scope = cutover::observe_checked(
        checked(&f, &s.plan, &s.intent, &f.db)?,
        s.receipt.reference(),
    )
    .await?;
    let guard = s.owner.quiesce(scope).await?;
    assert!(
        state_copy::copy_retained(checked(&f, &s.plan, &s.intent, guard.database())?, &guard)
            .await?
            .original()
            .is_err()
    );
    let claim = match candidate::claim_checked(
        checked(&f, &s.plan, &s.intent, guard.database())?,
        &guard,
        s.receipt.reference(),
    )
    .await?
    {
        candidate::CandidateOrRecovery::Claim(claim) => claim,
        _ => bail!("original candidate claim required"),
    };
    drop(guard);
    assert!(s.receipt.cancel(&principal, Uuid::new_v4()).await.is_err());
    let proof = s.owner.seal_unstarted_candidate(&mut Some(claim))?;
    proof.cancel(&principal, Uuid::new_v4()).await?;
    Ok(())
}

#[tokio::test]
async fn original_preclaim_receipt_refuses_changed_journal_used_copy_or_corrupt_ciphertext()
-> Result<()> {
    for fault in ["journal", "used", "cipher"] {
        let f = Fixture::new().await?;
        let mut s = setup(&f).await?;
        let _old = copy_original(&f, &mut s).await?;
        let id = s.staged.configured_instance().context("owner")?;
        match fault {
            "journal" => {
                let mut value: serde_json::Value = serde_json::from_str(&record(&f, id).await?)?;
                value["mutation"] = json!(Uuid::new_v4());
                let bytes = canonical::to_vec(&value)?;
                sqlx::query("UPDATE marketplace_installations SET record_json=$1,record_sha256=$2 WHERE instance_id=$3")
                    .bind(String::from_utf8(bytes.clone())?).bind(Digest::of(&bytes).to_string()).bind(id.to_string()).execute(&f.db.pool).await?;
            }
            "used" => {
                sqlx::query("UPDATE marketplace_instance_sdk_state SET revision=revision+1 WHERE instance_id=$1")
                    .bind(s.staged.instance().to_string()).execute(&f.db.pool).await?;
            }
            "cipher" => {
                sqlx::query("UPDATE marketplace_generation_secrets SET value_encrypted='!' || SUBSTR(value_encrypted,2) WHERE namespace_id=$1")
                    .bind(s.staged.instance().to_string()).execute(&f.db.pool).await?;
            }
            _ => unreachable!(),
        }
        let before = record(&f, id).await?;
        assert!(
            s.receipt
                .cancel(&configured_runtime::principal(&f), Uuid::new_v4())
                .await
                .is_err(),
            "{fault}"
        );
        assert_eq!(record(&f, id).await?, before, "{fault}");
        assert_eq!(counts(&f, s.staged.instance()).await?, (1, 1, 1), "{fault}");
    }
    Ok(())
}

#[tokio::test]
async fn preclaim_namespace_deletion_and_tombstone_rollback_together() -> Result<()> {
    let f = Fixture::new().await?;
    let mut s = setup(&f).await?;
    let _old = copy_original(&f, &mut s).await?;
    let id = s.staged.configured_instance().context("owner")?;
    let before = record(&f, id).await?;
    sqlx::query("CREATE TRIGGER preclaim_cancel_abort BEFORE UPDATE ON marketplace_installations BEGIN SELECT RAISE(ABORT,'explicit preclaim journal fault'); END")
        .execute(&f.db.pool).await?;
    let principal = configured_runtime::principal(&f);
    let mutation = Uuid::new_v4();
    assert!(s.receipt.cancel(&principal, mutation).await.is_err());
    assert_eq!(record(&f, id).await?, before);
    assert_eq!(counts(&f, s.staged.instance()).await?, (1, 1, 1));
    sqlx::query("DROP TRIGGER preclaim_cancel_abort")
        .execute(&f.db.pool)
        .await?;
    s.receipt.cancel(&principal, mutation).await?;
    assert_eq!(counts(&f, s.staged.instance()).await?, (0, 0, 0));
    Ok(())
}

#[tokio::test]
async fn cancelled_preclaim_cleanup_waiter_retains_the_original_receipt() -> Result<()> {
    let f = Fixture::new().await?;
    let mut s = setup(&f).await?;
    let _old = copy_original(&f, &mut s).await?;
    let id = s.staged.configured_instance().context("owner")?;
    let before = record(&f, id).await?;
    let mut blocker = f.db.pool.begin().await?;
    sqlx::query("UPDATE marketplace_installation_budget SET revision=revision WHERE singleton=1")
        .execute(&mut *blocker)
        .await?;
    let principal = configured_runtime::principal(&f);
    let mutation = Uuid::new_v4();
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(50),
            s.receipt.cancel(&principal, mutation)
        )
        .await
        .is_err()
    );
    blocker.rollback().await?;
    assert_eq!(record(&f, id).await?, before);
    let first = s.receipt.cancel(&principal, mutation).await?;
    assert_eq!(
        s.receipt.cancel(&principal, mutation).await?.revision(),
        first.revision()
    );
    assert_eq!(counts(&f, s.staged.instance()).await?, (0, 0, 0));
    Ok(())
}
