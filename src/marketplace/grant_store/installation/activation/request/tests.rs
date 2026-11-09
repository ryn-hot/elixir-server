//! Real one-use SQL claims and cancellation transactions, synthetic discovery
//! observations. No native execution, disposal or public-network evidence.
use super::*;
use crate::marketplace::grant_store::installation::{InstallTarget, tests::Fixture};

const MIB: u64 = 1024 * 1024;
fn budget() -> InstallBudget {
    InstallBudget {
        entries: 4,
        bytes: 4 * MIB,
    }
}
async fn stage(f: &Fixture) -> Result<StagedInstallation> {
    let mutation = Uuid::new_v4();
    f.stage_configured(
        InstallTarget::New {
            name: format!("Unstarted source {mutation}"),
        },
        mutation,
        0,
        budget(),
    )
    .await
}
async fn sealed(
    f: &Fixture,
    staged: &StagedInstallation,
) -> Result<(UnstartedSourceStart, NativeStartCheck)> {
    let material = f.material()?;
    let plan = SourceActivationPlan::new(
        material.authority.verify()?.release(),
        &material.review,
        "sha256:5d1db50d7282d14d23179c2ff687d1afe9ce94b5335588935cb506b84b6b4fa0",
        &crate::marketplace::quarantine::SourceJobBudget {
            memory_bytes: 64 * MIB,
            cpu_millicores: 100,
            disk_bytes: 128 * MIB,
            input_bytes: 16 * MIB,
            output_bytes: 64 * MIB,
            wall_seconds: 120,
        },
    )?;
    let intent = Arc::new(OwnerActivationIntent::for_actor(
        f.actor.clone(),
        staged,
        &material.review,
        f.revision,
        Uuid::new_v4(),
    )?);
    let step = || -> Result<Step<'_>> {
        Ok(Step {
            database: &f.db,
            metadata: &f.metadata,
            public: &f.public,
            material: f.material()?,
            intent: intent.clone(),
            plan: &plan,
            budget: budget(),
        })
    };
    let prepared = match prepare_checked(step()?).await? {
        PreparedOrRecovery::Prepared(prepared) => prepared,
        _ => bail!("fresh preparation required"),
    };
    let start = match claim_checked(step()?, &prepared).await? {
        StartOrRecovery::Start(start) => start,
        _ => bail!("original start required"),
    };
    let check = start.native_check();
    // A prepared/status reference cannot cancel Starting; only consuming the
    // actual original capability below can construct the sealed owner.
    assert!(
        cancellation::cancel(
            &f.db,
            cancellation::OwnerCancelIntent::prepared(
                &f.actor.fixture_principal(),
                &prepared,
                Uuid::new_v4()
            )?,
            budget()
        )
        .await
        .is_err()
    );
    assert!(matches!(
        claim_checked(step()?, &prepared).await?,
        StartOrRecovery::Recovery(_)
    ));
    Ok((UnstartedSourceStart { start }, check))
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
    Ok(sqlx::query_as("SELECT CAST((SELECT COUNT(*) FROM marketplace_sdk_namespaces WHERE namespace_id=$1) AS BIGINT),CAST((SELECT COUNT(*) FROM marketplace_instance_sdk_state WHERE instance_id=$1) AS BIGINT),CAST((SELECT COUNT(*) FROM marketplace_instance_sdk_values WHERE instance_id=$1) AS BIGINT)")
        .bind(namespace.to_string()).fetch_one(&f.db.pool).await?)
}
async fn cancel(
    f: &Fixture,
    p: &CurrentPrincipal,
    proof: &UnstartedSourceStart,
    mutation: Uuid,
) -> Result<cancellation::CancelledInstallation> {
    cancellation::cancel(
        &f.db,
        cancellation::OwnerCancelIntent::unstarted_source(p, proof, mutation)?,
        budget(),
    )
    .await
}
fn principal(f: &Fixture, revision: i64) -> CurrentPrincipal {
    let mut caps = crate::authz::CapabilitySet::all();
    caps.remove(crate::authz::Capability::SecretsManage);
    CurrentPrincipal::fixture_owner(
        f.actor.user,
        f.actor.home,
        f.actor.profile,
        f.actor.session,
        revision,
        f.actor.expires,
        caps,
    )
}

#[tokio::test]
async fn sealed_original_source_start_cancels_offline_at_final_capacity_without_other_data_loss()
-> Result<()> {
    let f = Fixture::new().await?;
    let a = stage(&f).await?;
    let (proof, _read_only_check) = sealed(&f, &a).await?;
    let b = stage(&f).await?;
    let b_before = record(&f, b.configured_instance().context("B owner")?).await?;
    let owner = a.configured_instance().context("owner")?;
    sqlx::query("UPDATE extension_instances SET config_json=$1 WHERE instance_id=$2")
        .bind(r#"{"owner":"preserved"}"#)
        .bind(owner.to_string())
        .execute(&f.db.pool)
        .await?;
    let owner_before:(String,Option<String>,String)=sqlx::query_as("SELECT instance_name,config_json,extension_id FROM extension_instances WHERE instance_id=$1")
        .bind(owner.to_string()).fetch_one(&f.db.pool).await?;
    (f.meta_owner)(false);
    (f.public_owner)(false);
    sqlx::query("UPDATE marketplace_extension_grants SET enabled=FALSE,review_json='{}',revision=revision+1,revocation_revision=revision+1")
        .execute(&f.db.pool).await?;
    sqlx::query("UPDATE marketplace_installation_budget SET revision=$1 WHERE singleton=1")
        .bind(MAX_REVISION - 1)
        .execute(&f.db.pool)
        .await?;
    let p = principal(&f, f.actor.authorization_revision);
    let mutation = Uuid::new_v4();
    let cancelled = cancel(&f, &p, &proof, mutation).await?;
    assert_eq!(cancelled.revision(), proof.reference().revision() + 1);
    assert_eq!(
        cancel(&f, &p, &proof, mutation).await?.revision(),
        cancelled.revision()
    );
    assert!(cancel(&f, &p, &proof, Uuid::new_v4()).await.is_err());
    assert_eq!(counts(&f, a.instance()).await?, (0, 0, 0));
    assert_eq!(counts(&f, b.instance()).await?, (1, 1, 0));
    assert_eq!(
        record(&f, b.configured_instance().context("B owner")?).await?,
        b_before
    );
    let owner_after:(String,Option<String>,String)=sqlx::query_as("SELECT instance_name,config_json,extension_id FROM extension_instances WHERE instance_id=$1")
        .bind(owner.to_string()).fetch_one(&f.db.pool).await?;
    assert_eq!(owner_after, owner_before);
    let value: serde_json::Value = serde_json::from_str(&record(&f, owner).await?)?;
    assert!(
        value["pending"].is_null()
            && value["activation"].is_null()
            && value["cancelled"].is_object()
    );
    let ledger: i64 = sqlx::query_scalar(
        "SELECT revision FROM marketplace_installation_budget WHERE singleton=1",
    )
    .fetch_one(&f.db.pool)
    .await?;
    assert_eq!(ledger, MAX_REVISION);
    Ok(())
}

#[tokio::test]
async fn sealed_original_source_start_requires_fresh_owner_without_rewinding_authority()
-> Result<()> {
    let f = Fixture::new().await?;
    let staged = stage(&f).await?;
    let (proof, _) = sealed(&f, &staged).await?;
    let owner = staged.configured_instance().context("owner")?;
    let before = record(&f, owner).await?;
    sqlx::query(
        "UPDATE profile_authorization_revisions SET revision=revision+1 WHERE profile_id=$1",
    )
    .bind(f.actor.profile.to_string())
    .execute(&f.db.pool)
    .await?;
    assert!(
        cancel(
            &f,
            &principal(&f, f.actor.authorization_revision),
            &proof,
            Uuid::new_v4()
        )
        .await
        .is_err()
    );
    assert_eq!(record(&f, owner).await?, before);
    assert_eq!(counts(&f, staged.instance()).await?, (1, 1, 0));
    let fresh = principal(&f, f.actor.authorization_revision + 1);
    assert_eq!(
        cancel(&f, &fresh, &proof, Uuid::new_v4()).await?.revision(),
        4
    );
    let revision: i64 = sqlx::query_scalar(
        "SELECT revision FROM profile_authorization_revisions WHERE profile_id=$1",
    )
    .bind(f.actor.profile.to_string())
    .fetch_one(&f.db.pool)
    .await?;
    assert_eq!(revision, f.actor.authorization_revision + 1);
    Ok(())
}

#[tokio::test]
async fn sealed_original_source_start_rolls_back_namespace_deletion_if_final_write_fails()
-> Result<()> {
    let f = Fixture::new().await?;
    let staged = stage(&f).await?;
    let (proof, _) = sealed(&f, &staged).await?;
    let owner = staged.configured_instance().context("owner")?;
    let before = record(&f, owner).await?;
    sqlx::query("UPDATE marketplace_installation_budget SET revision=$1 WHERE singleton=1")
        .bind(MAX_REVISION)
        .execute(&f.db.pool)
        .await?;
    assert!(
        cancel(
            &f,
            &principal(&f, f.actor.authorization_revision),
            &proof,
            Uuid::new_v4()
        )
        .await
        .is_err()
    );
    assert_eq!(record(&f, owner).await?, before);
    assert_eq!(counts(&f, staged.instance()).await?, (1, 1, 0));
    Ok(())
}

#[tokio::test]
async fn sealed_original_source_start_refuses_used_or_corrupt_staged_state() -> Result<()> {
    let f = Fixture::new().await?;
    let staged = stage(&f).await?;
    let (proof, _) = sealed(&f, &staged).await?;
    sqlx::query(
        "UPDATE marketplace_instance_sdk_state SET revision=revision+1 WHERE instance_id=$1",
    )
    .bind(staged.instance().to_string())
    .execute(&f.db.pool)
    .await?;
    let owner = staged.configured_instance().context("owner")?;
    let before = record(&f, owner).await?;
    assert!(
        cancel(
            &f,
            &principal(&f, f.actor.authorization_revision),
            &proof,
            Uuid::new_v4()
        )
        .await
        .is_err()
    );
    assert_eq!(record(&f, owner).await?, before);
    assert_eq!(counts(&f, staged.instance()).await?, (1, 1, 0));
    Ok(())
}

#[tokio::test]
async fn sealed_original_source_start_survives_a_cancelled_sql_waiter() -> Result<()> {
    let f = Fixture::new().await?;
    let staged = stage(&f).await?;
    let (proof, _) = sealed(&f, &staged).await?;
    let owner = staged.configured_instance().context("owner")?;
    let before = record(&f, owner).await?;
    let mut blocker = f.db.pool.begin().await?;
    sqlx::query("UPDATE marketplace_installation_budget SET revision=revision WHERE singleton=1")
        .execute(&mut *blocker)
        .await?;
    let p = principal(&f, f.actor.authorization_revision);
    let mutation = Uuid::new_v4();
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(10),
            cancel(&f, &p, &proof, mutation)
        )
        .await
        .is_err()
    );
    blocker.rollback().await?;
    assert_eq!(record(&f, owner).await?, before);
    assert_eq!(counts(&f, staged.instance()).await?, (1, 1, 0));
    let receipt = cancel(&f, &p, &proof, mutation).await?;
    assert_eq!(
        cancel(&f, &p, &proof, mutation).await?.revision(),
        receipt.revision()
    );
    Ok(())
}
