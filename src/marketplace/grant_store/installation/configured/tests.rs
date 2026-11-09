//! Actual signed archives and journal transactions; discovery owners remain
//! synthetic. These tests do NOT qualify runtime/activation or public access.
use super::super::cancellation::{self, OwnerCancelIntent};
use super::super::tests::{Fixture, budget};
use super::*;

fn intent(
    f: &Fixture,
    target: InstallTarget,
    mutation: Uuid,
    expected: i64,
) -> Result<OwnerInstallIntent> {
    let material = f.material()?;
    let mut intent = OwnerInstallIntent::for_actor(
        f.actor.clone(),
        &material.review,
        f.revision,
        expected,
        mutation,
    )?;
    intent.configured = Some(Target::new(
        &intent,
        target,
        material.review.scope().extension_id(),
    )?);
    Ok(intent)
}
async fn stage(
    f: &Fixture,
    target: InstallTarget,
    mutation: Uuid,
    expected: i64,
    limit: InstallBudget,
) -> Result<StagedInstallation> {
    stage_checked(
        &f.db,
        &f.metadata,
        &f.public,
        f.material()?,
        intent(f, target, mutation, expected)?,
        limit,
    )
    .await
}
fn new(name: &str) -> InstallTarget {
    InstallTarget::New { name: name.into() }
}
fn existing(receipt: &StagedInstallation) -> Result<InstallTarget> {
    Ok(InstallTarget::Existing {
        instance: receipt.configured_instance.context("configured receipt")?,
    })
}
async fn cancel(f: &Fixture, receipt: &StagedInstallation, mutation: Uuid) -> Result<i64> {
    Ok(cancellation::cancel(
        &f.db,
        OwnerCancelIntent::for_actor(f.actor.clone(), receipt, mutation)?,
        budget(),
    )
    .await?
    .revision())
}
async fn record(f: &Fixture, receipt: &StagedInstallation) -> Result<Stored> {
    let mut tx = f.db.pool.begin().await?;
    let stored = store::load_configured(
        &mut tx,
        f.db.driver,
        receipt.extension(),
        receipt.configured_instance,
    )
    .await?
    .context("configured record missing")?;
    verify(&mut tx, f.db.driver, &stored.journal).await?;
    tx.rollback().await?;
    Ok(stored)
}

async fn independent_instances(f: &mut Fixture) -> Result<()> {
    let mutation = Uuid::new_v4();
    let a = stage(f, new("Family"), mutation, 0, budget()).await?;
    let retried = stage(f, new("Family"), mutation, 0, budget()).await?;
    assert_eq!(a.configured_instance, retried.configured_instance);
    assert_eq!(a.instance(), retried.instance());
    assert!(
        stage(f, new("Changed name"), mutation, 0, budget())
            .await
            .is_err()
    );
    let b = stage(f, new("Kids"), Uuid::new_v4(), 0, budget()).await?;
    let owner_a = a.configured_instance.context("owner A")?;
    let owner_b = b.configured_instance.context("owner B")?;
    assert_ne!(owner_a, owner_b);
    assert_ne!(owner_a, a.instance());
    assert_ne!(owner_b, b.instance());
    assert_ne!(a.instance(), b.instance());
    assert_eq!(f.counts().await?, (1, 2, 2));
    let old_b = record(f, &b).await?;
    let old_b_json = canonical::to_vec(&old_b.journal)?;
    let old_a = record(f, &a).await?;
    assert_eq!(
        old_a.journal.pending.as_ref().unwrap().source.id()?,
        old_b.journal.pending.as_ref().unwrap().source.id()?,
        "code archive is shared, not private state"
    );
    let mut tx = f.db.pool.begin().await?;
    assert!(
        store::load(&mut tx, f.db.driver, a.extension())
            .await?
            .is_none(),
        "legacy readers must not pick an arbitrary configured instance"
    );
    tx.rollback().await?;
    let actor_mutation = Uuid::new_v4();
    assert_eq!(cancel(f, &a, actor_mutation).await?, 2);
    assert_eq!(cancel(f, &a, actor_mutation).await?, 2);
    let owner_exists: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM extension_instances WHERE instance_id=$1")
            .bind(owner_a.to_string())
            .fetch_one(&f.db.pool)
            .await?;
    assert_eq!(
        owner_exists, 1,
        "cancellation never deletes the configured owner"
    );
    f.upgrade("2.0.0", None).await?;
    let updated = stage(f, existing(&a)?, Uuid::new_v4(), 2, budget()).await?;
    assert_eq!(updated.configured_instance, Some(owner_a));
    assert_ne!(updated.instance(), a.instance());
    assert_eq!(
        canonical::to_vec(&record(f, &b).await?.journal)?,
        old_b_json,
        "other instance journal remains byte-identical under new desired metadata"
    );
    let desired: String =
        sqlx::query_scalar("SELECT version FROM extensions WHERE extension_id=$1")
            .bind(a.extension().as_str())
            .fetch_one(&f.db.pool)
            .await?;
    assert_eq!(desired, "2.0.0");
    let a2 = record(f, &updated).await?;
    assert_eq!(a2.journal.configured_instance, Some(owner_a));
    assert_eq!(a2.journal.schema_version, 2);
    assert_eq!(
        a2.journal.pending.as_ref().unwrap().instance,
        updated.instance()
    );
    activation::OwnerActivationIntent::fixture_for_actor(
        f.actor.clone(),
        &updated,
        &f.material()?.review,
        f.revision,
        Uuid::new_v4(),
    )?;
    assert!(
        standing::InstalledBinding::load_configured(
            &f.db,
            f.actor.home,
            updated.extension(),
            owner_a,
            budget()
        )
        .await
        .is_err(),
        "an intent or staged record alone never enables configured execution"
    );
    assert_eq!(
        cancel(f, &b, Uuid::new_v4()).await?,
        2,
        "cleanup does not adopt latest desired source or require old discovery"
    );
    assert_eq!(f.counts().await?, (1, 2, 2));
    Ok(())
}

#[tokio::test]
async fn two_configured_instances_share_code_but_have_independent_journals_namespaces_and_cancellation()
-> Result<()> {
    independent_instances(&mut Fixture::new().await?).await
}

#[tokio::test]
async fn unused_namespace_cancellation_preserves_configured_settings_name_and_legacy_secret_ciphertext()
-> Result<()> {
    let f = Fixture::new().await?;
    let a = stage(&f, new("Keep my name"), Uuid::new_v4(), 0, budget()).await?;
    let owner = a.configured_instance.context("owner")?;
    sqlx::query("UPDATE extension_instances SET config_json=$1 WHERE instance_id=$2")
        .bind(r#"{"keep":true}"#)
        .bind(owner.to_string())
        .execute(&f.db.pool)
        .await?;
    let cipher = crate::secrets::SecretsManager::from_key_bytes([115; 32], false)
        .encrypt("preserved secret")?;
    sqlx::query("INSERT INTO secrets(secret_id,scope,scope_id,key,value_encrypted,rotatable) VALUES($1,'instance',$2,'source_api',$3,FALSE)")
        .bind(Uuid::new_v4().to_string()).bind(owner.to_string()).bind(&cipher).execute(&f.db.pool).await?;
    cancel(&f, &a, Uuid::new_v4()).await?;
    let b = stage(&f, existing(&a)?, Uuid::new_v4(), 2, budget()).await?;
    assert_eq!(b.configured_instance, Some(owner));
    let settings: (String, String) = sqlx::query_as(
        "SELECT instance_name,config_json FROM extension_instances WHERE instance_id=$1",
    )
    .bind(owner.to_string())
    .fetch_one(&f.db.pool)
    .await?;
    assert_eq!(settings, ("Keep my name".into(), r#"{"keep":true}"#.into()));
    let after: String = sqlx::query_scalar(
        "SELECT value_encrypted FROM secrets WHERE scope='instance' AND scope_id=$1",
    )
    .bind(owner.to_string())
    .fetch_one(&f.db.pool)
    .await?;
    assert_eq!(after, cipher);
    Ok(())
}

#[tokio::test]
async fn failed_second_instance_allocation_rolls_back_desired_metadata_owner_and_namespace()
-> Result<()> {
    let mut f = Fixture::new().await?;
    let limit = InstallBudget {
        entries: 1,
        ..budget()
    };
    let a = stage(&f, new("Original"), Uuid::new_v4(), 0, limit).await?;
    let before = canonical::to_vec(&record(&f, &a).await?.journal)?;
    f.upgrade("2.0.0", None).await?;
    assert!(
        stage(&f, new("Over budget"), Uuid::new_v4(), 0, limit)
            .await
            .is_err()
    );
    assert_eq!(f.counts().await?, (1, 1, 1));
    assert_eq!(canonical::to_vec(&record(&f, &a).await?.journal)?, before);
    let namespaces: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM marketplace_sdk_namespaces")
        .fetch_one(&f.db.pool)
        .await?;
    assert_eq!(namespaces, 1);
    let version: String = sqlx::query_scalar("SELECT version FROM extensions")
        .fetch_one(&f.db.pool)
        .await?;
    assert_eq!(version, "1.0.0");
    Ok(())
}

#[tokio::test]
async fn configured_staging_never_adopts_legacy_journals_or_untracked_operational_instances()
-> Result<()> {
    let f = Fixture::new().await?;
    f.stage(Uuid::new_v4(), 0, budget()).await?;
    assert!(
        stage(&f, new("Not a conversion"), Uuid::new_v4(), 0, budget())
            .await
            .is_err()
    );
    assert_eq!(f.counts().await?, (1, 1, 1));
    let other = Fixture::new().await?;
    let a = stage(&other, new("Configured"), Uuid::new_v4(), 0, budget()).await?;
    assert!(other.stage(Uuid::new_v4(), 0, budget()).await.is_err());
    let untracked = Uuid::new_v4();
    sqlx::query("INSERT INTO extension_instances(instance_id,extension_id,instance_name,enabled) VALUES($1,$2,'not adopted',FALSE)")
        .bind(untracked.to_string()).bind(a.extension().as_str()).execute(&other.db.pool).await?;
    assert!(
        stage(
            &other,
            InstallTarget::Existing {
                instance: untracked
            },
            Uuid::new_v4(),
            0,
            budget()
        )
        .await
        .is_err()
    );
    assert_eq!(other.counts().await?, (1, 2, 1));
    Ok(())
}

#[tokio::test]
async fn concurrent_exact_new_instance_actions_allocate_one_stable_owner_and_one_namespace()
-> Result<()> {
    let f = Fixture::new().await?;
    let mutation = Uuid::new_v4();
    let (a, b) = tokio::try_join!(
        stage(&f, new("Same action"), mutation, 0, budget()),
        stage(&f, new("Same action"), mutation, 0, budget())
    )?;
    assert_eq!(a.configured_instance, b.configured_instance);
    assert_eq!(a.instance(), b.instance());
    assert_eq!(f.counts().await?, (1, 1, 1));
    Ok(())
}

#[tokio::test]
#[ignore = "explicit fresh empty loopback PostgreSQL configured-journal staging/cancel/reopen; no native activation"]
async fn postgres_configured_journals_are_independent_under_shared_desired_metadata() -> Result<()>
{
    let url = std::env::var("ELIXIR_TEST_MARKETPLACE_INSTANCE_POSTGRES_URL")?;
    let parsed = reqwest::Url::parse(&url)?;
    anyhow::ensure!(
        matches!(parsed.scheme(), "postgres" | "postgresql")
            && matches!(
                parsed.host_str(),
                Some("127.0.0.1" | "localhost" | "[::1]" | "::1")
            )
            && parsed.query().is_none()
            && parsed.fragment().is_none()
            && parsed.path().starts_with("/elixir_marketplace_instances_"),
        "dedicated empty loopback fixture required"
    );
    let configuration = crate::config::DatabaseConfig {
        url,
        max_connections: 4,
        ..Default::default()
    };
    let db = Database::connect(&configuration).await?;
    anyhow::ensure!(db.driver == DatabaseDriver::Postgres, "PostgreSQL required");
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog','information_schema')").fetch_one(&db.pool).await?;
    anyhow::ensure!(count == 0, "fixture must be empty; no reset");
    let mut f = Fixture::with_database(tempfile::tempdir()?, db).await?;
    independent_instances(&mut f).await?;
    f.db.pool.close().await;
    f.db = Database::connect(&configuration).await?;
    let owners: Vec<String> = sqlx::query_scalar("SELECT instance_id FROM marketplace_installations WHERE instance_id IS NOT NULL ORDER BY instance_id")
        .fetch_all(&f.db.pool).await?;
    assert_eq!(owners.len(), 2);
    for owner in owners {
        let mut tx = f.db.pool.begin().await?;
        let stored = store::load_configured(
            &mut tx,
            f.db.driver,
            f.material()?.review.scope().extension_id(),
            Some(owner.parse()?),
        )
        .await?
        .context("reopened record")?;
        verify(&mut tx, f.db.driver, &stored.journal).await?;
        tx.rollback().await?;
    }
    Ok(())
}
