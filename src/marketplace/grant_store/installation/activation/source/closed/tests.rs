//! Actual signed source/SQL/control writes. Native disposal is explicitly
//! synthetic here; real source-owner failure/disposal is a separate native test.
use super::*;
use crate::extensions::store::ExtensionStore;
use crate::marketplace::grant_store::installation::{InstallTarget, tests::Fixture};

fn budget() -> InstallBudget {
    InstallBudget {
        entries: 4,
        bytes: 4 * 1024 * 1024,
    }
}
async fn claim(f: &Fixture) -> Result<(ActivationStart, NativeStartCheck, SourceActivationPlan)> {
    let staged = f
        .stage_configured(
            InstallTarget::New {
                name: "Original closed source".into(),
            },
            Uuid::new_v4(),
            0,
            budget(),
        )
        .await?;
    let material = f.material()?;
    let plan = SourceActivationPlan::new(
        material.authority.verify()?.release(),
        &material.review,
        "sha256:5d1db50d7282d14d23179c2ff687d1afe9ce94b5335588935cb506b84b6b4fa0",
        &crate::marketplace::quarantine::SourceJobBudget {
            memory_bytes: 64 * 1024 * 1024,
            cpu_millicores: 100,
            disk_bytes: 128 * 1024 * 1024,
            input_bytes: 16 * 1024 * 1024,
            output_bytes: 64 * 1024 * 1024,
            wall_seconds: 120,
        },
    )?;
    let intent = Arc::new(OwnerActivationIntent::for_actor(
        f.actor.clone(),
        &staged,
        &material.review,
        f.revision,
        Uuid::new_v4(),
    )?);
    let prepared = match prepare_checked(step(f, &intent, &plan)?).await? {
        PreparedOrRecovery::Prepared(p) => p,
        _ => bail!("original prepare required"),
    };
    let start = match claim_checked(step(f, &intent, &plan)?, &prepared).await? {
        StartOrRecovery::Start(s) => s,
        _ => bail!("original start required"),
    };
    let check = start.native_check();
    Ok((start, check, plan))
}
fn step<'a>(
    f: &'a Fixture,
    intent: &Arc<OwnerActivationIntent>,
    plan: &'a SourceActivationPlan,
) -> Result<Step<'a>> {
    Ok(Step {
        database: &f.db,
        metadata: &f.metadata,
        public: &f.public,
        material: f.material()?,
        intent: intent.clone(),
        plan,
        budget: budget(),
    })
}
async fn record(f: &Fixture) -> Result<String> {
    Ok(
        sqlx::query_scalar("SELECT record_json FROM marketplace_installations")
            .fetch_one(&f.db.pool)
            .await?,
    )
}
async fn prepared(
    f: &Fixture,
    check: &NativeStartCheck,
    plan: &SourceActivationPlan,
) -> Result<ActivationReference> {
    match step(f, &check.intent, plan)?
        .transition(Action::Retain(&DisposedProbe::fixture(check)))
        .await?
    {
        Transition::SourcePrepared(r) => Ok(r),
        _ => bail!("retention required"),
    }
}
async fn remaining(f: &Fixture) -> Result<i64> {
    Ok(sqlx::query_scalar("SELECT CAST((SELECT COUNT(*) FROM marketplace_sdk_namespaces)+(SELECT COUNT(*) FROM marketplace_instance_sdk_state)+(SELECT COUNT(*) FROM marketplace_instance_sdk_values)+(SELECT COUNT(*) FROM marketplace_generation_secrets) AS BIGINT)").fetch_one(&f.db.pool).await?)
}

#[tokio::test]
async fn original_disposed_source_cleans_changed_choices_offline_at_final_capacity_with_fresh_owner()
-> Result<()> {
    let f = Fixture::new().await?;
    let (start, _, _) = claim(&f).await?;
    let source = DisposedSource::fixture(f.db.clone(), start, budget());
    let owner = source.reference().configured_instance.context("owner")?;
    ExtensionStore::new(&f.db.pool)
        .set_instance_enabled(owner, false)
        .await?;
    ExtensionStore::new(&f.db.pool)
        .update_instance_config(owner, Some(&serde_json::json!({"keep":true})))
        .await?;
    (f.meta_owner)(false);
    (f.public_owner)(false);
    sqlx::query(
        "UPDATE profile_authorization_revisions SET revision=revision+1 WHERE profile_id=$1",
    )
    .bind(f.actor.profile.to_string())
    .execute(&f.db.pool)
    .await?;
    let mutation = Uuid::new_v4();
    assert!(
        source
            .cancel(&f.actor.fixture_principal(), mutation)
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
    sqlx::query("UPDATE marketplace_extension_grants SET enabled=FALSE,review_json='{}',revision=revision+1,revocation_revision=revision+1").execute(&f.db.pool).await?;
    sqlx::query("UPDATE marketplace_installation_budget SET revision=$1 WHERE singleton=1")
        .bind(MAX_REVISION - 1)
        .execute(&f.db.pool)
        .await?;
    assert_eq!(source.cancel(&fresh, mutation).await?.revision(), 4);
    assert_eq!(source.cancel(&fresh, mutation).await?.revision(), 4);
    assert_eq!(remaining(&f).await?, 0);
    let settings:(String,i64)=sqlx::query_as("SELECT config_json,CAST(CASE WHEN enabled THEN 1 ELSE 0 END AS BIGINT) FROM extension_instances WHERE instance_id=$1").bind(owner.to_string()).fetch_one(&f.db.pool).await?;
    assert_eq!(settings, ("{\"keep\":true}".into(), 0));
    Ok(())
}

#[tokio::test]
async fn disposed_original_reconciles_exact_committed_retention_without_reissuing_a_capability()
-> Result<()> {
    let f = Fixture::new().await?;
    let (start, check, plan) = claim(&f).await?;
    let source = DisposedSource::fixture(f.db.clone(), start, budget());
    // Deliberately ignore the returned retention acknowledgement. Only the
    // original fixture disposal owner, not its observed status, authorizes SQL.
    prepared(&f, &check, &plan).await?;
    ExtensionStore::new(&f.db.pool)
        .set_extension_enabled(source.reference().extension.as_str(), false)
        .await?;
    (f.meta_owner)(false);
    (f.public_owner)(false);
    let mutation = Uuid::new_v4();
    assert_eq!(
        source
            .cancel(&f.actor.fixture_principal(), mutation)
            .await?
            .revision(),
        5
    );
    assert_eq!(
        source
            .cancel(&f.actor.fixture_principal(), mutation)
            .await?
            .revision(),
        5
    );
    assert_eq!(remaining(&f).await?, 0);
    Ok(())
}

#[tokio::test]
async fn original_disposal_cannot_cancel_a_changed_starting_journal_even_with_matching_coordinates()
-> Result<()> {
    let f = Fixture::new().await?;
    let (start, _, _) = claim(&f).await?;
    let source = DisposedSource::fixture(f.db.clone(), start, budget());
    let mut journal: Journal =
        canonical::decode(record(&f).await?.as_bytes(), record::MAX_RECORD_BYTES)?;
    journal.mutation = Uuid::new_v4();
    let bytes = canonical::to_vec(&journal)?;
    sqlx::query("UPDATE marketplace_installations SET record_json=$1,record_sha256=$2")
        .bind(String::from_utf8(bytes.clone())?)
        .bind(Digest::of(&bytes).to_string())
        .execute(&f.db.pool)
        .await?;
    let before = record(&f).await?;
    let error = source
        .cancel(&f.actor.fixture_principal(), Uuid::new_v4())
        .await
        .err()
        .context("changed journal accepted")?;
    assert!(
        format!("{error:#}").contains("exact original starting journal"),
        "{error:#}"
    );
    assert_eq!(record(&f).await?, before);
    assert_eq!(remaining(&f).await?, 2);
    Ok(())
}

#[tokio::test]
async fn disposed_source_cannot_reach_past_the_original_retention_boundary() -> Result<()> {
    let f = Fixture::new().await?;
    let (start, check, plan) = claim(&f).await?;
    let source = DisposedSource::fixture(f.db.clone(), start, budget());
    let retained = prepared(&f, &check, &plan).await?;
    quiescence::quiesce_checked(step(&f, &check.intent, &plan)?, &retained).await?;
    let before = record(&f).await?;
    assert!(
        source
            .cancel(&f.actor.fixture_principal(), Uuid::new_v4())
            .await
            .is_err()
    );
    assert_eq!(record(&f).await?, before);
    assert_eq!(remaining(&f).await?, 2);
    Ok(())
}

#[tokio::test]
async fn disposed_source_cleanup_rolls_back_deletion_and_retains_proof_for_exact_retry()
-> Result<()> {
    let f = Fixture::new().await?;
    let (start, _, _) = claim(&f).await?;
    let source = DisposedSource::fixture(f.db.clone(), start, budget());
    let mutation = Uuid::new_v4();
    let before = record(&f).await?;
    sqlx::query("CREATE TRIGGER fail_closed_source_update BEFORE UPDATE ON marketplace_installations BEGIN SELECT RAISE(ABORT,'closed source rollback fixture'); END").execute(&f.db.pool).await?;
    assert!(
        source
            .cancel(&f.actor.fixture_principal(), mutation)
            .await
            .is_err()
    );
    assert_eq!(remaining(&f).await?, 2);
    assert_eq!(record(&f).await?, before);
    sqlx::query("DROP TRIGGER fail_closed_source_update")
        .execute(&f.db.pool)
        .await?;
    assert_eq!(
        source
            .cancel(&f.actor.fixture_principal(), mutation)
            .await?
            .revision(),
        4
    );
    assert_eq!(remaining(&f).await?, 0);
    Ok(())
}

#[tokio::test]
async fn disposed_source_refuses_nonempty_staged_state_without_deleting_private_data() -> Result<()>
{
    let f = Fixture::new().await?;
    let (start, _, _) = claim(&f).await?;
    let source = DisposedSource::fixture(f.db.clone(), start, budget());
    sqlx::query("UPDATE marketplace_instance_sdk_state SET revision=revision+1")
        .execute(&f.db.pool)
        .await?;
    let before = record(&f).await?;
    assert!(
        source
            .cancel(&f.actor.fixture_principal(), Uuid::new_v4())
            .await
            .is_err()
    );
    assert_eq!(remaining(&f).await?, 2);
    assert_eq!(record(&f).await?, before);
    Ok(())
}
